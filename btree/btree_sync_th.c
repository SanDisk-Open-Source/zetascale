/*
 * File:   btSync.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008-2009, Schooner Information Technology, Inc.
 */

/**
 * @brief Featherweight threads main modules
 */

#define btSync_SPIN_CALLER      // Give the addr of the caller in spin locks

/*
 * Disable thread-q access function inlining.  btSync_THREADQ_NO_INLINE is always
 * marginally faster.
 */

#define btSync_THREADQ_NO_INLINE

#define POLLER_SCHED    0
#define LOW_PRIO_SCHED  0

#include <sched.h>
#include <sys/resource.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include "btree_raw_internal.h"
#include "btree_sync_th.h"
#if 0
#include "valgrind/valgrind.h"

#include "platform/alloc_stack.h"
#include "platform/attr.h"
#include "platform/defs.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#include "platform/platform.h"


#include "platform/assert.h"
#include "platform/shmem_global.h"
#endif
#undef   max
#define  max(a, b)     ((a) > (b) ? (a) : (b))
#define  pf(...)       printf(__VA_ARGS__); fflush(NULL)
#define array_size(a)  (sizeof(a) / sizeof((a)[0]))

#ifdef _OPTIMIZE
#undef assert
#define assert(a)
#endif

/*
 *  These variables are largely read-only.  They are initialized here in an
 *  attempt to make them contiguous in memory.
 */

__thread int              totalScheds     = 1;  // this is never changed
__thread int              curSchedNum     = 0;  // this is never changed

uint64_t                  btSyncReverses     = 0;
uint64_t                  btSyncFloatMax     = 0;

uint64_t btSyncTscTicksPerMicro = 3000; // this is an initial guess

#if 0
struct btSyncConfig btSyncConfig = {
    .idleMode = btSync_IDLE_SPIN,
    .busyPollInterval = 1,
    .affinityMode = btSync_AFFINITY_DEFAULT
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
btSyncUthreadGetter(void)
{
    btSyncThread_t *thread = btSyncSelf();
    return thread ? thread->local : NULL;
}

void btSyncInitMultiQ(int numArgs, ...)
{
    uint64_t tscBefore;
    uint64_t tscAfter;
    uint64_t endNs;
    uint64_t nowNs;
    struct timespec nowTs;

    btSync = plat_alloc(sizeof(btSync_t));
    assert(btSync);
    btSync->allHead             = NULL;
    btSync->allTail             = NULL;
    btSync->nthrds              = 0;
    pthread_mutex_init(&(btSync->list_mutex), NULL);

    /*  These are used to gate btSyncread starting until at least one
     *  scheduler starts.
     */
    pthread_mutex_init(&(btSync->sched_mutex), NULL);
    pthread_cond_init(&(btSync->sched_condvar), NULL);
    btSync->sched_started       = 0;
    btSync->sched_thrds_waiting = 0;

    /*  These are used to synchronize the "killing" of btSyncreads
     *  and schedulers.
     */
    btSync->kill = 0;
    pthread_mutex_init(&(btSync->kill_mutex), NULL);
    pthread_cond_init(&(btSync->kill_condvar), NULL);

    tscBefore = rdtsc();
    clock_gettime(CLOCK_REALTIME, &nowTs);
    nowNs = (uint64_t)nowTs.tv_sec * PLAT_BILLION + nowTs.tv_nsec;
    endNs = nowNs + (uint64_t)btSync_TIME_CALIBRATION_MICROS * PLAT_THOUSAND;
    do {
        clock_gettime(CLOCK_REALTIME, &nowTs);
        nowNs = (uint64_t)nowTs.tv_sec * PLAT_BILLION + nowTs.tv_nsec;
    } while (nowNs < endNs);
    tscAfter = rdtsc();

    btSyncTscTicksPerMicro = (tscAfter - tscBefore) / btSync_TIME_CALIBRATION_MICROS;

    plat_attr_uthread_getter_set(&btSyncUthreadGetter);
}

/**
 * @brief btSyncSchedulerThread - this pthread becomes an btSync scheduler
 *
 * On termination, plat_shmem_pthread_done() is called to cleanup
 * threads.  See #btSyncScheduler for details.
 *
 * @param prio <IN> Realtime priority (1-99) or 0 if not a realtime thread
 */
void btSyncSchedulerPthread(int prio)
{
    /* start any btSyncreads that were waiting for a scheduler to start */

    pthread_mutex_lock(&(btSync->sched_mutex));
    if (!btSync->sched_started) {
	btSync->sched_started = 1;
	pthread_cond_broadcast(&(btSync->sched_condvar));
    }
    pthread_mutex_unlock(&(btSync->sched_mutex));

    pthread_mutex_lock(&(btSync->kill_mutex));
    while (1) {
	if (btSync->kill) {
	    (btSync->kill)--;
	    break;
	} else {
	    pthread_cond_wait(&(btSync->kill_condvar), &(btSync->kill_mutex));
	}
    }
    pthread_mutex_unlock(&(btSync->kill_mutex));
}

/**
 * @brief called to stop btSync (test only)
 */
void btSyncKill(int kill)
{
    btSyncThread_t  *btSyncrd;
    btSyncThread_t  *btSyncrd_self;
    btSyncThread_t  *btSyncrd_next;

    pthread_mutex_lock(&(btSync->kill_mutex));
    // kill all btSyncreads (other than myself!)
    btSyncrd_self = btSyncSelf();
    for (btSyncrd = btSync->allHead; btSyncrd != NULL; btSyncrd = btSyncrd_next) {
        btSyncrd_next = btSyncrd->next;
	if (btSyncrd != btSyncrd_self) {
	    pthread_cancel(btSyncrd->pthread);
	    plat_free(btSyncrd);
	}
    }
    // kill scheduler pthreads
    // xxxzzz should I just do a broadcast here?
    for (;btSync->kill > 0; (btSync->kill)--) {
	pthread_cond_signal(&(btSync->kill_condvar));
    }
    pthread_mutex_unlock(&(btSync->kill_mutex));
}

/**
 * @brief return my btSyncThread
 */
inline btSyncThread_t *btSyncSelf(void)
{
    // get my btSyncread value from the btSync mapping table 
    return selfbtSyncread;
}

/**
 * @brief return global id
 */

void * btSyncId(void)
{
   return (void *) btSyncSelf();
}

/*
 * Return an unique id.
 */
uint64_t btSync_uid(void)
{
    return (selfbtSyncread->id);
}
#endif

uint64_t btSyncWait(btSyncThread_t *thread);
	
static void *pthread_func_wrapper(void *arg)
{
    uint64_t       rv;
    btSyncThread_t   *btSyncrd = (btSyncThread_t *) arg;

#if 0
    selfbtSyncread = btSyncrd;

    /* wait until at least one scheduler is running */

    pthread_mutex_lock(&(btSync->sched_mutex));
    while (1) {
	if (btSync->sched_started) {
	    break;
	} else {
	    (btSync->sched_thrds_waiting)++;
	    pthread_cond_wait(&(btSync->sched_condvar), &(btSync->sched_mutex));
	    (btSync->sched_thrds_waiting)--;
	}
    }
    pthread_mutex_unlock(&(btSync->sched_mutex));
#endif
    rv = btSyncWait(btSyncrd);

    // call btSyncread function
    btSyncrd->startfn(rv);

    pthread_exit(NULL);
}

btSyncThread_t *btSyncSpawnPthread(int shutdown_thread)
{
    btSyncThread_t           *btSyncrd;

    cpu_set_t              mask;
    int i;


    for (i = 0; i < 32; ++i) {
		CPU_SET(i, &mask);
    }
    // sched_setaffinity(0, sizeof(mask), &mask);

    btSyncrd = NULL;

    if ((btSyncrd = malloc(sizeof(btSyncThread_t))) == NULL) {
		return NULL;
	}
    assert(btSyncrd);
    pthread_mutex_init(&(btSyncrd->mutex), NULL);
    btSyncrd->id         = 0;
	btSyncrd->is_waiting = 0;
	btSyncrd->do_resume  = 0;
	btSyncrd->pthread    = pthread_self();
	btSyncrd->startfn    = NULL;
	btSyncrd->next       = NULL;
	btSyncrd->prev       = NULL;

	/*  Used for Drew's screwy btSyncread-local state stuff in
	 *  platform/attr.[ch].
	 */
//	btSyncrd->local      = plat_attr_uthread_alloc();

	pthread_cond_init(&(btSyncrd->condvar), NULL);
#if 0
	pthread_mutex_lock(&(btSync->list_mutex));
	if (btSync->allHead == NULL) {
	    btSync->allHead = btSyncrd;
	    btSync->allTail = btSyncrd;
	} else {
	    btSyncrd->next = btSync->allHead;
	    btSync->allHead->prev = btSyncrd;
	    btSync->allHead = btSyncrd;
	}
	pthread_mutex_unlock(&(btSync->list_mutex));

    selfbtSyncread = btSyncrd;
#endif

    return btSyncrd;
}

void
btSyncReleasePthread()
{
#if 0
	btSyncThread_t         *btSyncrd = btSyncSelf();
	
	/* Thread sstate already freed, return */
	if (btSyncrd == NULL) {
		return;
	}

	pthread_mutex_lock(&(btSync->list_mutex));
	if (btSyncrd->next) {
		btSyncrd->next->prev = btSyncrd->prev;
		assert(btSync->allTail != btSyncrd);
	}
	if (btSyncrd->prev) {
		btSyncrd->prev->next = btSyncrd->next;
		assert(btSync->allHead != btSyncrd);
	}
	if (btSync->allHead == btSyncrd) {
		btSync->allHead = btSyncrd->next;
	}
	if (btSync->allTail == btSyncrd) {
		btSync->allTail = btSyncrd->prev;
	}
	pthread_mutex_unlock(&(btSync->list_mutex));
//	plat_attr_uthread_free(btSyncrd->local);
	plat_free(btSyncrd);
#endif
}
/**
 * @brief Spawn a new thread.  Thread is not dispatchable until resumed.
 *
 * @param startRoutine(arg) <IN> called when thread dispatches
 * @param minStackSize <IN> Minimum size of stack to allocate.
 * @return btSyncThread structure pointer
 */
btSyncThread_t *btSyncSpawn(btree_raw_t *btree, int index, void (*startRoutine)(uint64_t))
{
    int                    rc;
    btSyncThread_t           *btSyncrd;
    pthread_t              pthrd;
    pthread_attr_t         attr;
	btree_raw_t				*bt = (btree_raw_t *)btree;

    cpu_set_t              mask;
    int i;

	
    for (i = 0; i < 32; ++i) {
	CPU_SET(i, &mask);
    }
    // sched_setaffinity(0, sizeof(mask), &mask);

    btSyncrd = NULL;

    btSyncrd = (btSyncThread_t *)malloc(sizeof(btSyncThread_t));
    assert(btSyncrd);
    pthread_mutex_init(&(btSyncrd->mutex), NULL);
    pthread_mutex_lock(&(btSyncrd->mutex));
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    rc = pthread_create(&pthrd, &attr, pthread_func_wrapper, (void *) btSyncrd);
    if (rc == 0) {
		bt->syncthread[index] = btSyncrd;
	btSyncrd->id         = index;
	btSyncrd->is_waiting = 0;
	btSyncrd->do_resume  = 0;
	btSyncrd->pthread    = pthrd;
	btSyncrd->startfn    = startRoutine;
	btSyncrd->next       = NULL;
	btSyncrd->prev       = NULL;

	/*  Used for Drew's screwy btSyncread-local state stuff in
	 *  platform/attr.[ch].
	 */
	//btSyncrd->local      = plat_attr_uthread_alloc();

	pthread_cond_init(&(btSyncrd->condvar), NULL);
#if 0
	if (btSync->allHead == NULL) {
	    btSync->allHead = btSyncrd;
	    btSync->allTail = btSyncrd;
	} else {
	    btSyncrd->next = btSync->allHead;
	    btSync->allHead->prev = btSyncrd;
	    btSync->allHead = btSyncrd;
	}
#endif
	pthread_mutex_unlock(&(btSyncrd->mutex));
    } else {
	pthread_mutex_unlock(&(btSyncrd->mutex));
	free(btSyncrd);
    }
    pthread_attr_destroy(&attr);
    return btSyncrd;
}
#if 0
btSyncThread_t *btSyncMakeFloating(btSyncThread_t *thread)
{
   // purposefully empty
   return thread;
}
#endif

/**
 * @brief Give up processor, go undispatchable.
 *
 * @return Value specified by caller to btSyncResume.
 */
uint64_t btSyncWait(btSyncThread_t *thread)
{
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
 * The thread should have called btSyncWait (which made the thread undispatchable)
 *
 * @parameter thread <IN> Thread to make dispatchable
 * @parameter rv <IN> Value that the thread sees as the RV from btSyncWait
 */
static __inline__ void btSyncResumeInternal(btSyncThread_t *thread, uint64_t rv)
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
 * The thread should have called btSyncWait (which made the thread undispatchable)
 *
 * @parameter thread <IN> Thread to make dispatchable
 * @parameter rv <IN> Value that the thread sees as the RV from btSyncWait
 */
void
btSyncResume(btSyncThread_t *thread, uint64_t rv)
{
    btSyncResumeInternal(thread, rv);
}

/** 
 * @brief - Cross-thread (but not cross-process) safe resume for btSync thread
 *
 * @param <IN> thread pointer
 * @param <IN> argument to pass to thread (return val from btSyncWait call)
 */     
#if 0       
void XResume(struct btSyncThread *thread, uint64_t arg)
{           
    btSyncResumeInternal(thread, arg);
}       
    
/**
 * @brief Give up processor but remain dispatchable
 *
 * @param count <IN> For non-negative values the thread is skipped count
 * times.  Negative values put btSyncSelf() on the low priority queue which
 * is only checked when no regular priority threads are eligible for
 * dispatch with the thread in question skipped -count - 1 times.
 */
void btSyncYield(int count)
{
    pthread_yield();
}

/**
 * @brief sleep
 *
 * @param nanoSecs <IN>  nonseconds to sleep (from now)
 */

void btSyncNanoSleep(uint64_t nanoSecs)
{
    usleep(nanoSecs/1000);
}

/**
 * @brief Return accumulated btSync thread run time in microseconds
 *
 * @return accumulated btSync thread run time
 */
uint64_t btSyncGetTotalThreadRunTime(void)
{
    return 0;
}

/**
 * @brief Return accumulated btSync scheduler dispatch time in microseconds
 *
 * @return accumulated btSync dispatch time
 */
uint64_t btSyncGetSchedulerDispatchTime(void)
{
    return 0;
}

/**
 * @brief Return accumulated btSync scheduler low prio dispatch time in
 *        microseconds
 *
 * @return accumulated low prio btSync dispatch time
 */
uint64_t btSyncGetSchedulerLowPrioDispatchTime(void)
{
    return 0;
}

/**
 * @brief Return accumulated number of thread dispatches
 *
 * @return Total number of times threads were dispatched
 */

uint64_t btSyncGetSchedulerNumDispatches(void)
{
    return 0;
}

/**
 * @brief Return accumulated number of low-prio thread dispathes
 *
 * @return Total number of times low-prio threads were dispatched
 */
uint64_t btSyncGetSchedulerNumLowPrioDispatches(void)
{
    return 0;
}

/**
 * @brief Return average dispatch time in nanoseconds
 *
 * @return Dispatch time divided by number of dispatches times tsc ticks per
 *         nanosecond
 */
uint64_t btSyncGetSchedulerAvgDispatchNanosec(void)
{
    return 0;
}

/**
 * @brief Return accumulated btSync scheduler idle time in microseconds
 *
 * @return idle time
 */
uint64_t btSyncGetSchedulerIdleTime(void)
{
    return 0;
}

/**
 * @brief Return voluntary context switches
 *
 * @return context switches
 */
uint64_t btSyncGetVoluntarySwitchCount(void)
{
    return 0;
}

/**
 * @brief Return involuntary context switches
 *
 * @return context switches
 */
uint64_t btSyncGetInvoluntarySwitchCount(void)
{
    return 0;
}

/**
 * @brief Return the estimated tsc ticks per microsecond
 *
 * @return tstTicksPerMicro
 */
uint64_t btSyncGetTscTicksPerMicro(void)
{
    return btSyncTscTicksPerMicro;
}

/**
 * @brief return btSync structure pointer
 */
inline btSync_t *btSyncBase(void)
{
    return btSync;
}
#endif

