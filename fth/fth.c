//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/*
 * File:   fth.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008-2009, Schooner Information Technology, Inc.
 */

/**
 * @brief Featherweight threads main modules
 */

#define FTH_SPIN_CALLER      // Give the addr of the caller in spin locks

/*
 * Disable thread-q access function inlining.  FTH_THREADQ_NO_INLINE is always
 * marginally faster.
 */

#define FTH_THREADQ_NO_INLINE

#define POLLER_SCHED    0
#define LOW_PRIO_SCHED  0

#include <sched.h>
#include <sys/resource.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <stdio.h>

#ifdef VALGRIND
#include "valgrind/valgrind.h"
#endif

#include "platform/alloc_stack.h"
#include "platform/attr.h"
#include "platform/defs.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#include "platform/platform.h"

#include "fth.h"

#include "platform/assert.h"
#include "platform/shmem_global.h"

#undef   max
#define  max(a, b)     ((a) > (b) ? (a) : (b))
#define  pf(...)       printf(__VA_ARGS__); fflush(NULL)
#define array_size(a)  (sizeof(a) / sizeof((a)[0]))

/*
 *  These variables are largely read-only.  They are initialized here in an
 *  attempt to make them contiguous in memory.
 */

fth_t *                   fth             = NULL;
__thread int              totalScheds     = 1;  // this is never changed
__thread int              curSchedNum     = 0;  // this is never changed

uint64_t                  fthReverses     = 0;
uint64_t                  fthFloatMax     = 0;
uint64_t                  fthFloatStats[FTH_MAX_SCHEDS];

uint64_t fthTscTicksPerMicro = 3000; // this is an initial guess

__thread fthThread_t *selfFthread = NULL;

struct fthConfig fthConfig = {
    .idleMode = FTH_IDLE_SPIN,
    .busyPollInterval = 1,
    .affinityMode = FTH_AFFINITY_DEFAULT
};

static inline void *
atomic_xchgp(void * volatile *location, void *value)
{
    asm volatile("lock; xchgq %0,%1"
        : "=r" (value)
        : "m" (*location), "0" (value)
        : "memory");
    return value;
}

static struct plat_attr_uthread_specific *
fthUthreadGetter(void)
{
    fthThread_t *thread = fthSelf();
    return thread ? thread->local : NULL;
}

void fthInitMultiQ(int numArgs, ...)
{
    uint64_t tscBefore;
    uint64_t tscAfter;
    uint64_t endNs;
    uint64_t nowNs;
    struct timespec nowTs;

    fth = plat_alloc(sizeof(fth_t));
    plat_assert(fth);
    fth->allHead             = NULL;
    fth->allTail             = NULL;
    fth->nthrds              = 0;
    pthread_mutex_init(&(fth->list_mutex), NULL);

    /*  These are used to gate fthread starting until at least one
     *  scheduler starts.
     */
    pthread_mutex_init(&(fth->sched_mutex), NULL);
    pthread_cond_init(&(fth->sched_condvar), NULL);
    fth->sched_started       = 0;
    fth->sched_thrds_waiting = 0;

    /*  These are used to synchronize the "killing" of fthreads
     *  and schedulers.
     */
    fth->kill = 0;
    pthread_mutex_init(&(fth->kill_mutex), NULL);
    pthread_cond_init(&(fth->kill_condvar), NULL);

    tscBefore = rdtsc();
    clock_gettime(CLOCK_REALTIME, &nowTs);
    nowNs = (uint64_t)nowTs.tv_sec * PLAT_BILLION + nowTs.tv_nsec;
    endNs = nowNs + (uint64_t)FTH_TIME_CALIBRATION_MICROS * PLAT_THOUSAND;
    do {
        clock_gettime(CLOCK_REALTIME, &nowTs);
        nowNs = (uint64_t)nowTs.tv_sec * PLAT_BILLION + nowTs.tv_nsec;
    } while (nowNs < endNs);
    tscAfter = rdtsc();

    fthTscTicksPerMicro = (tscAfter - tscBefore) / FTH_TIME_CALIBRATION_MICROS;

    plat_attr_uthread_getter_set(&fthUthreadGetter);
}

/**
 * @brief fthSchedulerThread - this pthread becomes an FTH scheduler
 *
 * On termination, plat_shmem_pthread_done() is called to cleanup
 * threads.  See #fthScheduler for details.
 *
 * @param prio <IN> Realtime priority (1-99) or 0 if not a realtime thread
 */
void fthSchedulerPthread(int prio)
{
    /* start any fthreads that were waiting for a scheduler to start */

    pthread_mutex_lock(&(fth->sched_mutex));
    if (!fth->sched_started) {
	fth->sched_started = 1;
	pthread_cond_broadcast(&(fth->sched_condvar));
    }
    pthread_mutex_unlock(&(fth->sched_mutex));

    pthread_mutex_lock(&(fth->kill_mutex));
    while (1) {
	if (fth->kill) {
	    (fth->kill)--;
	    break;
	} else {
	    pthread_cond_wait(&(fth->kill_condvar), &(fth->kill_mutex));
	}
    }
    pthread_mutex_unlock(&(fth->kill_mutex));
}

/**
 * @brief called to stop FTH (test only)
 */
void fthKill(int kill)
{
    fthThread_t  *fthrd;
    fthThread_t  *fthrd_self;
    fthThread_t  *fthrd_next;

    pthread_mutex_lock(&(fth->kill_mutex));
    // kill all fthreads (other than myself!)
    fthrd_self = fthSelf();
    for (fthrd = fth->allHead; fthrd != NULL; fthrd = fthrd_next) {
        fthrd_next = fthrd->next;
	if (fthrd != fthrd_self) {
	    pthread_cancel(fthrd->pthread);
	    plat_free(fthrd);
	}
    }
    // kill scheduler pthreads
    // xxxzzz should I just do a broadcast here?
    for (;fth->kill > 0; (fth->kill)--) {
	pthread_cond_signal(&(fth->kill_condvar));
    }
    pthread_mutex_unlock(&(fth->kill_mutex));
}

/**
 * @brief return my fthThread
 */
inline fthThread_t *fthSelf(void)
{
    // get my fthread value from the fth mapping table 
    return selfFthread;
}

/**
 * @brief return global id
 */

void * fthId(void)
{
   return (void *) fthSelf();
}

/*
 * Return an unique id.
 */
uint64_t fth_uid(void)
{
    return (selfFthread->id);
}

static void *pthread_func_wrapper(void *arg)
{
    uint64_t       rv;
    fthThread_t   *fthrd = (fthThread_t *) arg;

    selfFthread = fthrd;

    /* wait until at least one scheduler is running */

    pthread_mutex_lock(&(fth->sched_mutex));
    while (1) {
	if (fth->sched_started) {
	    break;
	} else {
	    (fth->sched_thrds_waiting)++;
	    pthread_cond_wait(&(fth->sched_condvar), &(fth->sched_mutex));
	    (fth->sched_thrds_waiting)--;
	}
    }
    pthread_mutex_unlock(&(fth->sched_mutex));

    rv = fthWait();

    // call fthread function
    fthrd->startfn(rv);

    pthread_exit(NULL);
}

extern int getProperty_Int(const char *key, const int defaultVal);

fthThread_t *fthSpawnPthread(int shutdown_thread)
{
    fthThread_t           *fthrd;

    cpu_set_t              mask;
    int i;

    
    int async_thread = 0;
    if ( getProperty_Int( "ASYNC_DELETE_CONTAINERS", 0) == 1 ) {
        async_thread = 1;
    }

	/* 
	 * Already thread state initialized, return error if it is
	 * not shutdown thread
	 */
	if (!async_thread && selfFthread && !shutdown_thread) {
        if (NULL != selfFthread) {
            plat_log_msg(160216,
                    PLAT_LOG_CAT_SDF_NAMING,
                    PLAT_LOG_LEVEL_ERROR,
                    "Old fthSpawnPthread: ID=%d TID=%ld\n", selfFthread->id, (uint64_t)pthread_self());
        }
		return NULL;
	}

    for (i = 0; i < 32; ++i) {
		CPU_SET(i, &mask);
    }
    // sched_setaffinity(0, sizeof(mask), &mask);

    fthrd = NULL;

    if ((fthrd = plat_alloc(sizeof(fthThread_t))) == NULL) {
		return NULL;
	}
    plat_assert(fthrd);
    pthread_mutex_init(&(fthrd->mutex), NULL);
    fthrd->id         = fth->nthrds;
	fthrd->is_waiting = 0;
	fthrd->do_resume  = 0;
	fthrd->pthread    = pthread_self();
	fthrd->startfn    = NULL;
	fthrd->next       = NULL;
	fthrd->prev       = NULL;

	/*  Used for Drew's screwy fthread-local state stuff in
	 *  platform/attr.[ch].
	 */
	fthrd->local      = plat_attr_uthread_alloc();

	pthread_cond_init(&(fthrd->condvar), NULL);

	pthread_mutex_lock(&(fth->list_mutex));
	if (fth->allHead == NULL) {
	    fth->allHead = fthrd;
	    fth->allTail = fthrd;
	} else {
	    fthrd->next = fth->allHead;
	    fth->allHead->prev = fthrd;
	    fth->allHead = fthrd;
	}
	(fth->nthrds)++;
	pthread_mutex_unlock(&(fth->list_mutex));

    selfFthread = fthrd;
    //printf("Fresh fthSpawnPthread: ID=%d TID=%u\n", selfFthread->id, pthread_self());

    return fthrd;
}

void
fthReleasePthread()
{
	fthThread_t         *fthrd = fthSelf();
	
	/* Thread sstate already freed, return */
	if (fthrd == NULL) {
		return;
	}

	pthread_mutex_lock(&(fth->list_mutex));
	if (fthrd->next) {
		fthrd->next->prev = fthrd->prev;
		plat_assert(fth->allTail != fthrd);
	}
	if (fthrd->prev) {
		fthrd->prev->next = fthrd->next;
		plat_assert(fth->allHead != fthrd);
	}
	if (fth->allHead == fthrd) {
		fth->allHead = fthrd->next;
	}
	if (fth->allTail == fthrd) {
		fth->allTail = fthrd->prev;
	}
	(fth->nthrds)--;
	pthread_mutex_unlock(&(fth->list_mutex));
	plat_attr_uthread_free(fthrd->local);
	plat_free(fthrd);

	selfFthread = NULL;
}
/**
 * @brief Spawn a new thread.  Thread is not dispatchable until resumed.
 *
 * @param startRoutine(arg) <IN> called when thread dispatches
 * @param minStackSize <IN> Minimum size of stack to allocate.
 * @return fthThread structure pointer
 */
fthThread_t *fthSpawn(void (*startRoutine)(uint64_t), long minStackSize)
{
    int                    rc;
    fthThread_t           *fthrd;
    pthread_t              pthrd;
    pthread_attr_t         attr;

    cpu_set_t              mask;
    int i;

    for (i = 0; i < 32; ++i) {
	CPU_SET(i, &mask);
    }
    // sched_setaffinity(0, sizeof(mask), &mask);

    fthrd = NULL;

    fthrd = plat_alloc(sizeof(fthThread_t));
    plat_assert(fthrd);
    pthread_mutex_init(&(fthrd->mutex), NULL);
    pthread_mutex_lock(&(fthrd->mutex));
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    rc = pthread_create(&pthrd, &attr, pthread_func_wrapper, (void *) fthrd);
    if (rc == 0) {
        fthrd->id         = fth->nthrds;
	fthrd->is_waiting = 0;
	fthrd->do_resume  = 0;
	fthrd->pthread    = pthrd;
	fthrd->startfn    = startRoutine;
	fthrd->next       = NULL;
	fthrd->prev       = NULL;

	/*  Used for Drew's screwy fthread-local state stuff in
	 *  platform/attr.[ch].
	 */
	fthrd->local      = plat_attr_uthread_alloc();

	pthread_cond_init(&(fthrd->condvar), NULL);

	if (fth->allHead == NULL) {
	    fth->allHead = fthrd;
	    fth->allTail = fthrd;
	} else {
	    fthrd->next = fth->allHead;
	    fth->allHead->prev = fthrd;
	    fth->allHead = fthrd;
	}
	(fth->nthrds)++;
	pthread_mutex_unlock(&(fthrd->mutex));
    } else {
	pthread_mutex_unlock(&(fthrd->mutex));
	plat_free(fthrd);
    }
    pthread_attr_destroy(&attr);
    return fthrd;
}

fthThread_t *fthMakeFloating(fthThread_t *thread)
{
   // purposefully empty
   return thread;
}

/**
 * @brief Give up processor, go undispatchable.
 *
 * @return Value specified by caller to fthResume.
 */
uint64_t fthWait(void)
{
    fthThread_t *thread = fthSelf();
    uint64_t     rv;

    pthread_mutex_lock(&(thread->mutex));
    while (1) {
	if (thread->do_resume) {
	    break;
	} else {
	    thread->is_waiting = 1;
	    pthread_cond_wait(&(thread->condvar), &(thread->mutex));
	}
    }
    rv = thread->rv_wait;
    thread->is_waiting = 0;
    thread->do_resume= 0;
    pthread_mutex_unlock(&(thread->mutex));
    return rv;
}

/**
 * @brief Make thread dispatchable
 *
 * The thread should have called fthWait (which made the thread undispatchable)
 *
 * @parameter thread <IN> Thread to make dispatchable
 * @parameter rv <IN> Value that the thread sees as the RV from fthWait
 */
static __inline__ void fthResumeInternal(fthThread_t *thread, uint64_t rv)
{
    pthread_mutex_lock(&(thread->mutex));
    thread->rv_wait   = rv;
    thread->do_resume = 1;
    if (thread->is_waiting) {
	pthread_cond_signal(&(thread->condvar));
    }
    pthread_mutex_unlock(&(thread->mutex));
}

/**
 * @brief Make thread dispatchable
 *
 * The thread should have called fthWait (which made the thread undispatchable)
 *
 * @parameter thread <IN> Thread to make dispatchable
 * @parameter rv <IN> Value that the thread sees as the RV from fthWait
 */
void
fthResume(fthThread_t *thread, uint64_t rv)
{
    fthResumeInternal(thread, rv);
}

/** 
 * @brief - Cross-thread (but not cross-process) safe resume for fth thread
 *
 * @param <IN> thread pointer
 * @param <IN> argument to pass to thread (return val from fthWait call)
 */     
            
void XResume(struct fthThread *thread, uint64_t arg)
{           
    fthResumeInternal(thread, arg);
}       
    
/**
 * @brief Give up processor but remain dispatchable
 *
 * @param count <IN> For non-negative values the thread is skipped count
 * times.  Negative values put fthSelf() on the low priority queue which
 * is only checked when no regular priority threads are eligible for
 * dispatch with the thread in question skipped -count - 1 times.
 */
void fthYield(int count)
{
    pthread_yield();
}

/**
 * @brief sleep
 *
 * @param nanoSecs <IN>  nonseconds to sleep (from now)
 */

void fthNanoSleep(uint64_t nanoSecs)
{
    usleep(nanoSecs/1000);
}

/**
 * @brief Return accumulated FTH thread run time in microseconds
 *
 * @return accumulated FTH thread run time
 */
uint64_t fthGetTotalThreadRunTime(void)
{
    return 0;
}

/**
 * @brief Return accumulated FTH scheduler dispatch time in microseconds
 *
 * @return accumulated FTH dispatch time
 */
uint64_t fthGetSchedulerDispatchTime(void)
{
    return 0;
}

/**
 * @brief Return accumulated FTH scheduler low prio dispatch time in
 *        microseconds
 *
 * @return accumulated low prio FTH dispatch time
 */
uint64_t fthGetSchedulerLowPrioDispatchTime(void)
{
    return 0;
}

/**
 * @brief Return accumulated number of thread dispatches
 *
 * @return Total number of times threads were dispatched
 */

uint64_t fthGetSchedulerNumDispatches(void)
{
    return 0;
}

/**
 * @brief Return accumulated number of low-prio thread dispathes
 *
 * @return Total number of times low-prio threads were dispatched
 */
uint64_t fthGetSchedulerNumLowPrioDispatches(void)
{
    return 0;
}

/**
 * @brief Return average dispatch time in nanoseconds
 *
 * @return Dispatch time divided by number of dispatches times tsc ticks per
 *         nanosecond
 */
uint64_t fthGetSchedulerAvgDispatchNanosec(void)
{
    return 0;
}

/**
 * @brief Return accumulated FTH scheduler idle time in microseconds
 *
 * @return idle time
 */
uint64_t fthGetSchedulerIdleTime(void)
{
    return 0;
}

/**
 * @brief Return voluntary context switches
 *
 * @return context switches
 */
uint64_t fthGetVoluntarySwitchCount(void)
{
    return 0;
}

/**
 * @brief Return involuntary context switches
 *
 * @return context switches
 */
uint64_t fthGetInvoluntarySwitchCount(void)
{
    return 0;
}

/**
 * @brief Return the estimated tsc ticks per microsecond
 *
 * @return tstTicksPerMicro
 */
uint64_t fthGetTscTicksPerMicro(void)
{
    return fthTscTicksPerMicro;
}

/**
 * @brief return fth structure pointer
 */
inline fth_t *fthBase(void)
{
    return fth;
}

