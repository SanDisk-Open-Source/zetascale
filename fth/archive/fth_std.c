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
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fth.c,v 1.2 2009/03/19 23:42:58 jbertoni Exp jbertoni $
 */

/**
 * @brief Featherweight threads main modules
 */

#define FTH_SPIN_CALLER                      // Give the addr of the caller in spin locks
#define HI_PRIO_MEMQ                         // MEMQ (op post) threads are scheduled first


/*
 * Disable thread-q access function inlining
 *
 * FTH_THREADQ_NO_INLINE is always marginally faster without FTH_FAST_SCHEDULER
 * (a significant fraction of a percent on flashTestCase3).  With
 * FTH_FAST_SCHEDULER the minimum run time is marginally better (.3%) with
 * it off but the variability is higher.
 */
#define FTH_THREADQ_NO_INLINE

/*
 * Fast-path running the next eligible thread
 *
 * This nets a 6.5% reduction in runtime on flashTestCase3 and shows
 * a 30% speedup in fthScheduler and its children in gprof.
 *
 * XXX: drew 2008-08-15 It breaks two cases in fth_c_2_test although I have not
 * yet determined whether the test is wrong or the change is incomplete.
 */
#define FTH_FAST_SCHEDULER

#include <sched.h>
#include <sys/resource.h>

#include <pthread.h>
#include <unistd.h>
#include "valgrind/valgrind.h"

#include <fcntl.h>
#include <sys/mman.h>

#include "platform/alloc_stack.h"
#include "platform/attr.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#include "platform/platform.h"

#include "fthSchedType.h"
#include "fth.h"

#include "fthWaitEl.h"
#include "fthSparseLock.h"
#include "fthThread.h"
#include "fthSched.h"
#include "fthTrace.h"

#ifdef VECTOR_SCHED
#define FTH_DISPATCHQ_C
#include "fthDispatchQ.h"
#endif

#ifdef FTH_IDLE_CONTROL
#include "fthIdleControl.h"
#endif

XLIST_IMPL(allThreads, fthThread, nextAll);
XLIST_IMPL(XResume, fthThread, resumeNext);

#include "sdfappcommon/XList.h"
#include "sdfappcommon/XMbox.h"
#include "sdfappcommon/XResume.h"

#include "platform/assert.h"
#include "platform/shmem_global.h"

#include <stdio.h>

#define RESUME_SHMEM 0                       // Use non-shmem version

#define pf(...) printf(__VA_ARGS__); fflush(NULL)

extern fth_t *fth;
fth_t *fth = NULL;

static pthread_once_t pthreadOnce = PTHREAD_ONCE_INIT;
static ptofThreadPtrs_t ptofResumePtrs;


volatile void *volatile floatingThread;

static int fthDebug = 0;
fthSpinLock_t floatingSpin;
volatile void *volatile floatingQueue;
#ifdef MULTIQ_SCHED
static int previousFloating = 0;
static int nextFloating = 0;
#endif

int      floatingCount    = 0;
int      floatingStops[4] = { 0 };
uint64_t fthFloatMax      = 0;
uint64_t fthFloatStats[FTH_MAX_SCHEDS] = { 0 };

static __thread fthSched_t *sched = NULL;

__thread int curSchedNum = -1;

#ifdef MULTIQ_SCHED
// Convenience thread-local pointers to "this" scheduler's
// queues. The idea is to minimize sharing access to the
// global fth structure to the extent possible.
static __thread fthThreadQ_lll_t * myEligibleQ;
static __thread fthThreadQ_lll_t * myLowPrioQ;
static __thread fthThreadQ_lll_t * myMutexQ;
//static __thread fthThreadQ_lll_t * myMemQ;
static __thread fthThreadQ_lll_t * mySleepQ;
//static __thread fthSpinLock_t    * mySleepSpin;
//static __thread fthWaitQ_lll_t   * myFreeWait;
int totalScheds=0;

#endif // MULTIQ_SCHED

#ifdef FTH_TIME_STATS
static __thread uint64_t threadSuspendTimeStamp;
static __thread uint64_t threadResumeTimeStamp;
static __thread uint64_t dispatchTimeStamp;

#ifdef FTH_TIME_MIN_MAX
static uint64_t minFromDispatch = 0x7fffffffffffffff;
static uint64_t maxFromDispatch = 0;
static uint64_t minToDispatch = 0x7fffffffffffffff;
static uint64_t maxToDispatch = 0;
#endif
#endif

uint64_t fthTscTicksPerMicro = 3000;  // 3 GHZ is a good guess to start

struct fthConfig fthConfig = {
    .idleMode = FTH_IDLE_SPIN,
#ifdef FTH_FAST_SCHEDULER
    .busyPollInterval = 10,
#else
    .busyPollInterval = 1,
#endif
    .affinityMode = FTH_AFFINITY_DEFAULT
};


void *heapBase = NULL;
void *heapNext = NULL;

fthSpinLock_t heapSpin;

static inline void *atomic_xchgp(volatile void * volatile *location, void *value)
{
    asm volatile("lock; xchgq %0,%1"
        : "=r" (value)
        : "m" (*location), "0" (value)
        : "memory");
    return value;
}

#ifdef MALLOC

void mallocInit(void) {
    FTH_SPIN_INIT(&heapSpin);
    int tempFD = plat_open("/dev/physmem", O_RDWR);
    if ((heapBase = mmap(0, 0x100000000, PROT_READ | PROT_WRITE, MAP_SHARED, tempFD, (off_t) 0)) == MAP_FAILED) {
        printf("mmap failed!\n");
        close(tempFD);
        plat_exit(1);
    }

    heapNext = heapBase;
}

#define CACHELINE_SIZE 64

void *malloc(size_t size) {
    size = (size + (CACHELINE_SIZE - 1)) & ~(CACHELINE_SIZE - 1);
    FTH_SPIN_LOCK(&heapSpin);
    void *ptr = heapNext;
    if (ptr == NULL) {
        mallocInit();
        ptr = heapNext;
    }
    heapNext += size;
    FTH_SPIN_UNLOCK(&heapSpin);
    return (ptr);
}

void free(void *ptr) {
}

#endif

static struct plat_attr_uthread_specific *
fthUthreadGetter() {
    fthThread_t *thread = fthSelf();
    return (thread ? thread->local : NULL);
}

/**
 * @brief FTH initialization
 *
 * fthInit must be called before any fthThreads are created or started,
 *
 */

/**
 * @brief Initialize CPU info fields
 */
static void
fthInitCpuInfo() {
    int status;
    int sched;
    int cpu;
    int partnerCPU;
    uint32_t partnerCPUs;
    uint32_t printCPUs;

    plat_assert(fth);

    memset(&fth->cpuInfo, 0, sizeof (fth->cpuInfo));

    switch (fth->config.affinityMode) {
    case FTH_AFFINITY_PER_THREAD:
        fth->cpuInfo.CPUs = fth->config.affinityCores;
        break;
    case FTH_AFFINITY_DEFAULT:
        status = sched_getaffinity(0, sizeof(cpu_set_t), &fth->cpuInfo.CPUs);
        if (status == -1) {
            plat_log_msg(20869, PLAT_LOG_CAT_FTH,
                         PLAT_LOG_LEVEL_WARN, "Can't get CPU affinity: %s",
                         plat_strerror(errno));
        }
        break;
    }

    for (sched = 0, cpu = 0, printCPUs = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (CPU_ISSET(cpu, &fth->cpuInfo.CPUs)) {
            printCPUs |= 1 << cpu;
            if (sched < FTH_MAX_SCHEDS) {
                fth->cpuInfo.schedToCPU[sched] = cpu;
                fth->cpuInfo.CPUToSched[cpu] |= 1 << sched;
            }
            ++sched;
        }
    }
    fth->cpuInfo.numCPUs = sched;

    if (sched > FTH_MAX_SCHEDS) {
        plat_log_msg(20870, PLAT_LOG_CAT_FTH,
                     PLAT_LOG_LEVEL_WARN,
                     "%d CPUs available not all usable with FTH_MAX_SCHEDS %d",
                     sched, FTH_MAX_SCHEDS);
        sched = FTH_MAX_SCHEDS;
    }

    // Partner schedulers are those on all CPUs sharing the largest common
    // cache.
    for (sched = 0; sched < fth->cpuInfo.numCPUs; ++sched) {
        cpu = fth->cpuInfo.schedToCPU[sched];
        status = plat_get_cpu_cache_peers(&partnerCPUs, cpu);
        plat_assert_always(!status);

        for (partnerCPU = 0; partnerCPU < (sizeof (partnerCPUs) * CHAR_BIT);
             ++partnerCPU) {
            if (partnerCPUs & (1 << partnerCPU)) {
                fth->cpuInfo.schedToPartners[sched] |=
                    fth->cpuInfo.CPUToSched[partnerCPU];
            }
        }
    }

    for (; sched < FTH_MAX_SCHEDS; ++sched) {
        fth->cpuInfo.schedToCPU[sched] = FTH_CPU_INVALID;
    }

    plat_log_msg(20871, PLAT_LOG_CAT_FTH,
                 PLAT_LOG_LEVEL_DEBUG, "fth using up to %d cores from set 0x%x",
                 fth->cpuInfo.numCPUs, printCPUs);
}

#ifdef MULTIQ_SCHED
static void
initSchedQs(int totalScheds) {
    int schedNum;
    for(schedNum=0; schedNum < totalScheds; schedNum++) {
        fthThreadQ_lll_init(&fth->eligibleQ[schedNum]);
        fthFloatMax = totalScheds - 1;
        /*
          fthThreadQ_lll_init(&fth->lowPrioQ[schedNum]);
          FTH_SPIN_INIT(&fth->sleepSpin[schedNum]);
          fthWaitQ_lll_init(&fth->freeWait[schedNum]);
        */
    }
}

volatile int currOpenSched = 0;

#define NUM_FLASH_THREADS 0
#define FLASH_EXCLUSIVE_THREADS 0

int nextOpenSched(long stackSize) {

    plat_assert_always(totalScheds > 0);
    if(totalScheds == 1) {
        return 0;
    }
    int ret=currOpenSched%(fth->totalScheds-FLASH_EXCLUSIVE_THREADS) + FLASH_EXCLUSIVE_THREADS; // reserve 0 for flash and don't run stuff on 1 to prevent L2 flushing
#if (NUM_FLASH_THREADS > 0)
    if(stackSize%1024 == (1024 - FTH_HINT_FLASH)) {
            ret = currOpenSched%NUM_FLASH_THREADS;
        }
#endif

    if (PLAT_UNLIKELY(fthDebug)) {
        printf("on scheduler number %d \n", ret); fflush(NULL);
    }
    currOpenSched++;
    return ret;
}

void
fthInitMultiQ(int numArgs, ...) {
    int inTotalScheds = 1;
    va_list argp;
    va_start(argp, numArgs);

    if(numArgs > 0) {
        inTotalScheds = va_arg(argp, int);
    }
    plat_assert_always(inTotalScheds <= FTH_MAX_SCHEDS && inTotalScheds > 0);
    /*CA: unfortunately  fthPthreadInit  has to be a (void) routine so we
      use an ugly global to pass the totalScheds info.
      The assert catches multiple fthInits. */
    if(__sync_lock_test_and_set(&totalScheds, inTotalScheds) == 0) { // This is the first call
        // Let pthread make the overall init happen exactly once
        pthread_once(&pthreadOnce, &fthPthreadInit);
    }
}
#else
void
fthInit(void) {
    // Let pthread make the overall init happen exactly once
    pthread_once(&pthreadOnce, &fthPthreadInit);
}
#endif // MULTIQ_SCHED

int schedNum = 0;

/**
 * @brief Set affinity for calling scheduler
 *
 * Includes sched->partnerMask
 */
static void
fthSetAffinity() {
    int cpu;
    cpu_set_t cpuSet;

    cpu = fth->cpuInfo.schedToCPU[sched->schedNum];
    if (cpu != FTH_CPU_INVALID) {
        CPU_ZERO(&cpuSet);
        CPU_SET(cpu, &cpuSet);
        if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuSet) == -1) {
            plat_log_msg(20872, PLAT_LOG_CAT_FTH,
                         PLAT_LOG_LEVEL_WARN,
                         "Failed to set scheduler %d affinity: %s",
                         sched->schedNum, plat_strerror(errno));
        } else {
            sched->partnerMask = fth->cpuInfo.schedToPartners[sched->schedNum];
            plat_log_msg(20873, PLAT_LOG_CAT_FTH,
                         PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "fth scheduler %d affinity to core %d partners %x",
                         sched->schedNum, cpu, sched->partnerMask);
        }
    } else if (sched->schedNum == fth->cpuInfo.numCPUs) {
        plat_log_msg(20874, PLAT_LOG_CAT_FTH,
                     PLAT_LOG_LEVEL_WARN,
                     "Too few cores to set affinity on sched >= %d",
                     fth->cpuInfo.numCPUs);
    }
}


/**
 * @brief fthSchedulerThread - this pthread becomes an FTH scheduler
 *
 * On termination, plat_shmem_pthread_done() is called to cleanup
 * thraeds.  See #fthScheduler for details.
 *
 * @param prio <IN> Realtime priority (1-99) or 0 if not a realtime thread
 */
void fthSchedulerPthread(int prio) {
    struct sched_param sp;
    int rc;

    plat_assert(fth != NULL);

    // Schedulers have a stack
    sched= FTH_MALLOC(sizeof(fthSched_t));
    sched->dispatch.stackSize = FTH_STACK_ALIGN(4096);
    sched->dispatch.stack = plat_alloc_stack(sched->dispatch.stackSize);
    char *end = (char *) sched->dispatch.stack + sched->dispatch.stackSize;
    sched->dispatch.stackId = VALGRIND_STACK_REGISTER(sched->dispatch.stack, end);
    plat_assert_always(sched->dispatch.stack);
    sched->dispatch.startRoutine = &fthScheduler;
    sched->dispatch.sched = NULL;                    // No scheduler/cur yet

    // Leave space for sched structure and round down to 16 byte alignment
    // for x86_64.
    end = (char *)sched->dispatch.stack + sched->dispatch.stackSize - sizeof (fthSched_t);
    end = (char *)((long)end & ~0xf);

    // Make space for "return" address since entry is by jmp not call
    // thus ensuring the correct offset from the required alignment for
    // GCC's alignment preservation.  Otherwise printf("%g") will segfault.
    end -= sizeof (void *);

    // Make return address NULL so GDB stack back traces terminate
    *(void **)end = NULL;

#ifdef fthSetjmpLongjmp
    fthStackSwitch(sched, (uint64_t)end);
#endif

#ifdef fthAsmDispatch
    // Hack the longjump buffer
    sched->dispatch.pc = (uint64_t) NULL;
    sched->dispatch.rsp = (uint64_t)end;
#endif

    sched->dispatch.arg = (uint64_t) sched;  // Self pointer is arg

#ifdef FTH_TIME_STATS
    sched->totalThreadRunTime = 0;
    sched->schedulerIdleTime = 0;
    sched->schedulerDispatchTime = 0;
    sched->schedulerLowPrioDispatchTime = 0;
    sched->schedulerNumDispatches = 1;       // Avoid divide by zero
    sched->schedulerNumLowPrioDispatches = 1; // Avoid divide by zero
#endif

    if (prio != 0) {
        // Set the scheduling policies
        sp.sched_priority = prio;
        rc = sched_setscheduler(0, SCHED_RR, &sp);
    }

    sched->schedNum = __sync_fetch_and_add(&schedNum, 1);
    curSchedNum = sched->schedNum;
    if (curSchedNum >= FTH_MAX_SCHEDS) {
        plat_log_msg(20875, PLAT_LOG_CAT_FTH,
                     PLAT_LOG_LEVEL_FATAL,
                     "Trying to start scheduler %d >= FTH_MAX_SCHEDS",
                     curSchedNum);
        plat_abort();
    }

    sched->prevDispatchPrio = 1;

    sched->schedMask = 1 << sched->schedNum;


    // XXX: drew 2009-02-11 The documentation has no mention that affinity
    // is handled on a per-thread basis.  The kernel implementation does a
    // find_process_by_pid(); which I'd guess translates into a kernel
    // thread.
    fthSetAffinity(sched);


    fth->scheds[sched->schedNum] = sched;

    // Init the wait element lists
    sched->freeWaitCount = 0;
    sched->freeSparseCount = 0;
    fthWaitQ_lll_init(&sched->freeWait);
    fthSparseQ_lll_init(&sched->freeSparse);

    #ifdef FTH_INSTR_LOCK
    {
        /* per-scheduler state for lock tracing facility */
        sched->locktrace_data = (fthLockTraceData_t *) plat_alloc(sizeof(fthLockTraceData_t));
        sched->locktrace_data->schedNum       = sched->schedNum;
        sched->locktrace_data->n_trace_recs   = 0;
        sched->locktrace_data->n_lock_trace   = 0;
    }
    #endif // FTH_INSTR_LOCK


    // Put initial values into the time adjuster
    sched->prevTimeTsc = rdtsc();
    clock_gettime(CLOCK_REALTIME, &sched->prevTimeSpec);

#ifdef MULTIQ_SCHED
    myEligibleQ = &fth->eligibleQ[sched->schedNum];
    myLowPrioQ =  &fth->lowPrioQ;
    myMutexQ   =  &fth->mutexQ;
    //    myMemQ     =  &fth->memQ;
    mySleepQ   =  &fth->sleepQ;
#endif

#ifdef fthSetjmpLongjmp
    longjmp(sched->env, 1);                  // Never returns
#endif
#ifdef fthAsmDispatch
    fthScheduler((uint64_t) sched);
#endif

}

/**
 * @brief called to stop FTH (test only)
 */
void fthKill(int kill) {
    fth_t *fth = fthBase();
    fth->kill = kill;
    asm volatile("mfence");

#ifdef FTH_IDLE_CONTROL
    if (fth->idleControlPtr) {
        fthIdleControlPokeLocal(fth->idleControlPtr, kill);
    }
#endif
}

/**
 * @brief Init FTH structure - Pthread ensures that this gets called just once
 *
 * FIXME: drew 2008-04-22
 *
 * 1.  This assumes a single heavy weight process is receiving ptof mail boxes.  The correct fix is to hang the the
 *     mailbox structure off plat_process and associate ptof XMbox with specific target processes.
 *
 * 2.  This assumes fail-stop behavior since we combine shared memory allocation with initialization.
 */
void fthPthreadInit(void) {
    uint64_t existing;
    ptofMboxPtrs_t *ptofMboxPtr;

#ifdef ENABLE_FTH_TRACE
    fthSetTraceLevel(getenv("FTH_TRACE_LEVEL"));
#endif

    // XXX There needs to be a complementary shutdown function so that
    // programmatic leak detection does not break

    // General allocation and init
    fth = FTH_MALLOC(sizeof(fth_t));
    memset(fth, 0, sizeof (*fth));

    fth->config = fthConfig;

    fthInitCpuInfo();

#ifdef MULTIQ_SCHED
    fth->totalScheds = totalScheds;
    /*CA: Each scheduler has it's own queue.
      We preemptively init all the Q's for each of
      totalScheds schedulers right upfront to avoid
      races later*/
    initSchedQs(totalScheds);
    //    fthThreadQ_lll_init(&fth->memQ);
    fthThreadQ_lll_init(&fth->lowPrioQ);

#else

#ifdef VECTOR_SCHED
    fthDispatchQ_lll_init(&fth->eligibleQ);
    fthDispatchQ_lll_init(&fth->lowPrioQ);
    fth->idleSchedMask = 0;
#else
    fthThreadQ_lll_init(&fth->eligibleQ);
    fthThreadQ_lll_init(&fth->lowPrioQ);
#endif

    FTH_SPIN_INIT(&fth->qCheckSpin);
#endif

    fth->memQCount = 0;                      // No mem queues to check yet
    for (int i = 0; i < FTH_MEMQ_MAX; i++) {
        fth->memTest[i] = NULL;
        fth->memQ[i] = NULL;
    }

    fthThreadQ_lll_init(&fth->mutexQ);
    fthThreadQ_lll_init(&fth->sleepQ);

    // Init the global free element lists
    fthWaitQ_lll_init(&fth->freeWait);
#ifndef TEMP_WAIT_EL
    fth->waitEls = NULL;
#endif
    fthSparseQ_lll_init(&fth->freeSparse);

    fth->allHead = NULL;
    fth->allTail = NULL;

    FTH_SPIN_INIT(&fth->sleepSpin);

    FTH_SPIN_INIT(&fth->allQSpin);

    fth->kill = 0;

    // Init the SHMEM globals

    // Allocate the PTOF global pointer area

    // Allocate the PTOF global pointer area completely before assigning
    ptofMboxPtrs_sp_t ptofMboxShmem = ptofMboxPtrs_sp_alloc();
    plat_assert(!ptofMboxPtrs_sp_is_null(ptofMboxShmem));
    fth->ptofMboxPtr = NULL;
    ptofMboxPtr = ptofMboxPtrs_sp_rwref(&fth->ptofMboxPtr, ptofMboxShmem);
    // Init the head and tail
    ptofMboxPtr->headShmem = XMboxEl_sp_null;
    ptofMboxPtr->tailShmem = XMboxEl_sp_null;

    existing = shmem_global_set(SHMEM_GLOBAL_PTOF_MBOX_PTRS,
                                ptofMboxShmem.base.int_base);
    // The value was already initialized

    // FIXME: If we didn't assert and had some other guarantee we weren't directing messages to the wrong process
    // we'd want to ptofPtrs_sp_free(ptofPtr) and assign existing.
    plat_assert(!existing);

    fth->ptofMboxPtr = ptofMboxPtr;

#ifdef FTH_IDLE_CONTROL
    fth->idleControlPtr = NULL;

    fthIdleControl_sp_t fthIdleControlShmem;
    // Allocate the idle control (if enabled)
    switch (fth->config.idleMode) {
    case FTH_IDLE_SPIN:
        break;
    case FTH_IDLE_CONDVAR:
        fthIdleControlShmem = fthIdleControlAlloc();
        plat_assert(!fthIdleControl_sp_is_null(fthIdleControlShmem));
        fthIdleControl_sp_rwref(&fth->idleControlPtr, fthIdleControlShmem);

        // XXX: See SHMEM_GLOBAL_PTOF_MBOX_PTRS & function header comments
        existing = shmem_global_set(SHMEM_GLOBAL_IDLE_CONTROL,
                                    fthIdleControlShmem.base.int_base);
        plat_assert(!existing);
        break;
    }
#endif

    // We do not use the shmem version for now because we have no shmem-based fthThread ptrs
#if RESUME_SHMEM
    // Allocate the PTOF global resume pointers
    ptofThreadPtrs_sp_t ptofResumeShmem = ptofThreadPtrs_sp_alloc();
    plat_assert(!ptofThreadPtrs_sp_is_null(ptofResumeShmem));
    ptofResumePtr = ptofThreadPtrs_sp_rwref(&fth->ptofResumePtr, ptofResumeShmem);
    // Init the head and tail
    ptofResumePtr->headShmem = fthThread_sp_null;
    ptofResumePtr->tailShmem = fthThread_sp_null;
    existing = shmem_global_set(SHMEM_GLOBAL_PTOF_RESUME_PTRS,
                                ptofResumeShmem.base.int_base);
    // The value was already initialized

    // FIXME: If we didn't assert and had some other guarantee we weren't directing messages to the wrong process
    // we'd want to ptofPtrs_sp_free(ptofPtr) and assign existing.
    plat_assert(!existing);


    fth->ptofResumePtr = ptofResumePtr;

#else
    // Non-shmem version
    ptofResumePtrs.head = NULL;
    ptofResumePtrs.tail = NULL;

#endif
    plat_attr_uthread_getter_set(&fthUthreadGetter);
}

/**
 * @brief return my fthThread
 */
inline fthThread_t *fthSelf(void) {
    fthSched_t *my_sched;

    my_sched = sched;

    if (my_sched == NULL) {
        return NULL;                         // If not an FTH thread...
    }
    return (my_sched->dispatch.thread);         // Saved here
}

/**
 * @brief return global id
 */

void * fthId(void) {
#ifdef VECTOR_SCHED
    fthThread_t *self = fthSelf();
    if (self->vector != NULL) {
        return (self->vector);
    }
    return (self);
#else
   return fthSelf();
#endif
}

/**
 * @brief return a non-zero ID to use when setting locks.  This is for debug and recovery.
 */
inline uint64_t fthLockID(void) {
    if (sched != NULL) {                     // If found a key then from and FTH thread
        return ((uint64_t) sched->dispatch.thread);    // Saved here
    }

    return (getpid());                        // Just use the process ID
}

/**
 * @brief return fth structure pointer
 */
inline fth_t *fthBase(void) {
    return (fth);
}

/**
 * @brief return fth resume pointer
 */
inline struct ptofThreadPtrs *fthResumePtrs(void) {
    return (&ptofResumePtrs);
}

#ifdef FTH_IDLE_CONTROL
inline struct fthIdleControl *fthIdleControlPtr(void) {
    return (fth->idleControlPtr);
}
#endif

/*
 * @brief Dummy routine that starts every thread
 */
void fthDummy(fthThread_t *thread) {
#ifdef fthSetjmpLongjmp
    if (sigsetjmp(thread->env, 0) == 0) {
        return;                              // Just setting up the stack
    }
#endif

    // This is where the thread is actually entered after the stack switch junk
#ifdef FTH_TIME_STATS
    threadResumeTimeStamp = rdtsc();
#endif

    thread->state = 'R';
    thread->dispatch.startRoutine(thread->dispatch.arg);

    thread->state = 'K';                     // This thread is dead
    VALGRIND_STACK_DEREGISTER(thread->dispatch.stackId);

#ifdef FTH_TIME_STATS
    threadSuspendTimeStamp = rdtsc();
#endif

#ifdef fthSetjmpLongjmp
    if (sigsetjmp(thread->env, 0) == 0) {
        siglongjmp(thread->dispatch.sched->env, 1);
    }
#endif
#ifdef fthAsmDispatch
    fthToScheduler(thread, 0);               // Give up the CPU forever
#endif
}

/**
 * @brief Spawn a new thread.  Thread is not dispatchable until resumed.
 *
 * @param startRoutine(arg) <IN> called when thread dispatches
 * @param minStackSize <IN> Minimum size of stack to allocate.
 * @return fthThread structure pointer
 */
fthThread_t *fthSpawn(void (*startRoutine)(uint64_t), long minStackSize) {
    char *end;
    // Allocate the basic structures for the thread
    fthThread_t *thread = FTH_MALLOC(sizeof(fthThread_t));
    thread->dispatch.stackSize = FTH_STACK_ALIGN(minStackSize);
    thread->dispatch.stack = plat_alloc_stack(thread->dispatch.stackSize);
    end = (char *) thread->dispatch.stack + thread->dispatch.stackSize;
    thread->dispatch.stackId = VALGRIND_STACK_REGISTER(thread->dispatch.stack, end);
    plat_assert_always(thread->dispatch.stack);
    thread->dispatch.startRoutine = startRoutine;
    thread->state = 'N';                     // Virgin thread
    thread->dispatch.sched = NULL;           // No scheduler/cur yet
#ifdef MULTIQ_SCHED
    thread->schedNum = nextOpenSched(minStackSize);  //CA: Assign it to a scheduler.
#else
    thread->schedNum = -1;
#endif
#ifndef TEMP_WAIT_EL
    thread->waitElCount = 0;
#endif
    thread->spin = 0;
    thread->dispatchable = 0;
    thread->yieldCount = 0;
    thread->defaultYield = 0;
    thread->nextAll = NULL;
    thread->dispatch.floating = 0;

#ifdef VECTOR_SCHED
    thread->nextAll = NULL;
    thread->vector = NULL;
    thread->vectorSchedNum = -1;
    thread->vectorWait = 0;
    thread->vectorOrdinal = -1;

    // For now, all threads are dispatchable on all schedulers
    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        thread->dispatchMap.thread[i] = thread;
    }
    thread->dispatchMap.schedPrefMask = 0xffffffffffffffff; // Any scheduler to start
#endif

    allThreads_xlist_enqueue(&fth->allHead, &fth->allTail, thread);

    // XXX: Thread struct isn't actually stored on the stack, so why make
    // space for it?

    // Leave space for thread structure and round down to 16 byte alignment
    // for x86_64.
    end = (char *)thread->dispatch.stack + thread->dispatch.stackSize - sizeof (fthThread_t);
    end = (char *)((long)end & ~0xf);

    // Make space for "return" address since entry is by jmp not call
    // thus ensuring the correct offset from the required alignment for
    // GCC's alignment preservation.  Otherwise printf("%g") will segfault.
    end -= sizeof (void *);

    // Make return address NULL so GDB stack back traces terminate
    *(void **)end = NULL;

#ifdef fthSetjmpLongjmp
    fthStackSwitch(thread, (uint64_t)end);
#endif

#ifdef fthAsmDispatch
    // Hack the longjump buffer
    thread->dispatch.pc = (uint64_t) &fthDummy;
    thread->dispatch.rsp = (uint64_t)end;
#endif
    thread->local = plat_attr_uthread_alloc();
    plat_assert_always(thread->local);

    return (thread);
}

fthThread_t *fthMakeFloating(fthThread_t *thread) {
   plat_assert_always(floatingThread == NULL);
#ifdef floating_thread
   fthThread_t *old;

   thread->dispatch.floating = -1;
   old = atomic_xchgp(&floatingThread, thread);
   plat_assert_always(old == NULL);
#endif
   return thread;
}

#ifdef VECTOR_SCHED
/**
 * @brief Create a thread vector and spawn the associated threads.
 *
 * @param startRoutine(arg) <IN> called when thread dispatches
 * @param minStackSize <IN> Minimum size of stack to allocate.
 * @param schedMask <IN> Mask of scehdulers to spawn threads for or zero (all scheds)
 * @return fthThreadVector address
 */
fthThreadVector_t *fthVectorSpawn(void (*startRoutine)(uint64_t), long minStackSize,
                                  uint64_t schedMask) {

    fthThreadVector_t *vector = FTH_MALLOC(sizeof(fthThreadVector_t)); // Allocated the vector
    if (schedMask == 0) {
        schedMask = 0xffffffffffffffff;      // All scehdulers
    }
    vector->waitCount = 0;                   // Init
    vector->threadCount = 0;
    vector->map.schedPrefMask = 0xffffffffffffffff; // Any scheduler works for vectors

    // Spawn and assign a thread for each scheduler in the mask
    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if ((1 << i) & schedMask) {          // If assigning this one
            vector->map.thread[i] = fthSpawn(startRoutine, minStackSize);
            vector->map.thread[i]->vector = vector;
            vector->map.thread[i]->vectorSchedNum = i;
            vector->map.thread[i]->vectorOrdinal = vector->threadCount;
            vector->threadCount++;
        } else {
            vector->map.thread[i] = NULL;
        }
    }

    plat_assert(vector->threadCount != 0);

    return (vector);
}

/**
 * @brief Start all of the threads in a vector
 * @param vector <IN> Thread vector pointer
 * @param arg <IN> Argument to pass
 */
void fthVectorStart(fthThreadVector_t *vector, uint64_t arg) {

    // Just resume all non-null map entries
    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (vector->map.thread[i] != NULL) {
            fthResume(vector->map.thread[i], arg);
        }
    }
}
#endif

/**
 * @brief Make thread dispatchable
 *
 * The thread should have called fthWait (which made the thread undispatchable)
 *
 * @parameter thread <IN> Thread to make dispatchable
 * @parameter rv <IN> Value that the thread sees as the RV from fthWait
 */
static __inline__ void
fthResumeInternal (fthThread_t *thread, uint64_t rv) {
    FTH_SPIN_LOCK(&thread->spin);
    plat_assert(thread->dispatchable == 0);
    // Check the state and atomically change it to make sure that the thread
    // is never put on the XResume queue twice.  CDS logic is needed because
    // XResume cannot do a spin lock.
    char threadState;
    do {
        threadState = thread->state;
        plat_assert((threadState == 'R') || (threadState == 'W') || (threadState == 'N'));
    } while (!__sync_bool_compare_and_swap(&thread->state, threadState, 'd'));
    thread->dispatch.arg = rv;               // Save as return parameter

#ifdef MULTIQ_SCHED
    if (thread->dispatch.floating) {
       plat_assert_always(thread == floatingThread);
       plat_assert_always(floatingQueue == NULL);
       floatingQueue = thread;
    } else {
        fthThreadQ_push(&fth->eligibleQ[thread->schedNum], thread);
    }
#else /* ! MULTIQ_SCHED */

    // XResume_xlist_enqueue(&ptofResumePtrs.head, &ptofResumePtrs.tail, thread);

#ifdef VECTOR_SCHED
    fthDispatchQ_push(&fth->eligibleQ, &thread->dispatchMap);
#else
    fthThreadQ_push(&fth->eligibleQ, thread);
#endif
#endif // MULTIQ_SCHED

    thread->dispatchable = 1;                // Mark as on dispatch queue
    thread->state = 'D';                     // Now dispatchable
    FTH_SPIN_UNLOCK(&thread->spin);
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
fthResume(fthThread_t *thread, uint64_t rv) {
    fthResumeInternal(thread, rv);

#ifdef FTH_IDLE_CONTROL
    if (fth->idleControlPtr) {
        fthIdleControlPokeLocal(fth->idleControlPtr, 1);
    }
#endif
}

void
fthPrintSwitchStats(void) {
            /*  print out the thread dispatch/switch stats for all fthreads */
//            if(sched->schedNum == 0) {
                fthThread_t *curTh = fth->allHead;
                int totalDispatches = 0;
                int totalSwitches  =  0;
                while(curTh) {
                    totalDispatches += curTh->dispatchCount;
                    totalSwitches += curTh->switchCount;
                    curTh = curTh->nextAll;
                }
                printf("\nTotal Dispatches = %d , Total Scheduler Switches = %d \n", totalDispatches, totalSwitches);
//            }

            return;
}

#ifdef VECTOR_SCHED
#ifdef AFFINITY_QUEUE_SCAN

// This is the maximum "lookahead" on the eligible Q.  If a scheduler is looking
// beyond this point then some other scheduler(s) are backed up.  If enough stuff
// runs between dispatches of a given thread then it has lost its cache entries
// anyways so there is no point in any affinity.

#define MAX_AFFINITY_DEPTH 10

static __inline__ fthDispatchQ_t *
eligibleQAffinityScan(fthSched_t *sched) {

    // Search the eligible Q for a good affinity match
    fthDispatchQ_t *great = NULL;             // Great (but not perfect) match
    fthDispatchQ_t *ok = NULL;               // OK (but not great) match

    fthDispatchQ_spinLock(&fth->eligibleQ);
    fthDispatchQ_t *map = fth->eligibleQ.head;
    if (map == NULL) {                       // Check for nothing to dispatch
        fthDispatchQ_spinUnlock(&fth->eligibleQ);
        return (NULL);                        // Nothing on this queue
    }

    for (int i = 0; i < MAX_AFFINITY_DEPTH; i++) {
        if (map->schedPrefMask & sched->schedMask) { // If it prefers this scheduler
            great = map;                     // This is the one
            break;                           // Look no further
        }

        if ((great == NULL) &&               // If no good match yet
            (map->schedPrefMask & fth->idleSchedMask) == 0) { // and preferred is not idle
            // Since the preferred is not idle we might steal it for this CPU

            if (map->schedPrefMask & sched->partnerMask) { // If preferred to a partner
                // The preferred CPU is a partner (probably shared L2)
                great = map;                 // This is probably the one
            } else if (ok == NULL) {         // If no OK match yet
                ok = map;                    // This one is OK but not great
            }
        }

        map = fthDispatchQ_next(map);        // Try the next one
        if (map == NULL) {                   // Check for the end
            break;
        }
    }

    if (great == NULL) {                     // If failed to find any affinity
        if (ok == NULL) {                    // If not even anything OK
            fthDispatchQ_spinUnlock(&fth->eligibleQ); // Unlock
            return (NULL);                   // Nothing to dispatch
        }
        great = ok;                          // Use the OK match instead
    }

    // We now have a great match
    fthDispatchQ_remove_nospin(&fth->eligibleQ, great); // Remove
    fthDispatchQ_spinUnlock(&fth->eligibleQ); // Unlock

    return (great);                          // Use this one

}

#endif  // AFFINITY_QUEUE_SCAN
#endif  // VECTOR_SCHED

#ifndef MULTIQ_SCHED
#   ifdef FTH_FAST_SCHEDULER
/**
 * @brief Run the first eligible thread except 1/config.busyPollInterval times
 *
 * This gets higher throughput on thread dispatching at the expense of
 * higher latency on some workloads with memory and ptof mailbox waits
 * by reducing the number of instructions executed in the normal case
 * and resulting instruction cache effects.
 *
 * It gets called by fthScheduler after the previous thread has been dealt
 * with but before looking at the queues.
 *
 * @param sched <IN> Scheduler state
 * @param schedStartTimeStamp <IN> tsc time stamp at which scheduler resumed
 * control from previous thread (long jump to scheduler loop)
 * @param idleStartTimesStampPtr <OUT> We only consider the system idle once
 * we've polled for a regular thread and failed to find one. This causes 
 */
static __inline__ void
fthFastScheduler(fthSched_t *sched, uint64_t schedStartTimeStamp,
                 uint64_t *idleStartTimeStampPtr) {
    fthThread_t *cur;
    /*
     * How many iterations remain for this scheduler where we only
     * dispatch from the eligible queue.
     */
    static __thread int noPollRemain = 0;

    uint64_t idleStartTimeStamp = *idleStartTimeStampPtr;

    if (PLAT_LIKELY(noPollRemain > 0)) {
        --noPollRemain;

        // Check for an eligible thread
        while (1) {
#   ifdef FTH_TIME_STATS
            dispatchTimeStamp = rdtsc();     // Remember all but the last time around loop
#   endif // def FTH_TIME_STATS
#   ifdef VECTOR_SCHED
#       ifdef AFFINITY_QUEUE_SCAN
            fthDispatchQ_t *map = eligibleQAffinityScan(sched);
#       else // def AFFINITY_QUEUE_SCAN
            fthDispatchQ_t *map = fthDispatchQ_shift_precheck(&fth->eligibleQ);
#       endif //else def AFFINITY_QUEUE_SCAN
            if (PLAT_UNLIKELY(map == NULL)) {
                cur = NULL;
                break;
            }
            cur = map->thread[curSchedNum];
#   else // def VECTOR_SCHED
            cur = fthThreadQ_shift_precheck(&fth->eligibleQ);
            if (PLAT_UNLIKELY(cur == NULL)) {
                break;
            }
#   endif // else def VECTOR_SCHED
            // Make sure that the thread isn't just about to be suspended
            if (PLAT_UNLIKELY(cur->dispatch.sched != NULL)) {
            } else if (PLAT_UNLIKELY(cur->yieldCount != 0)) {
                cur->yieldCount--;
            } else {
#   ifdef VECTOR_SCHED
                map->schedPrefMask = sched->schedMask;
#   endif // def VECTOR_SCHED
                sched->prevDispatchPrio = 1;
                break;
            }
#   ifdef VECTOR_SCHED
            fthDispatchQ_push(&fth->eligibleQ, map); // Push back
#   else // def VECTOR_SCHED
            fthThreadQ_push(&fth->eligibleQ, cur); // Push back
#   endif // else def VECTOR_SCHED
        }

        if (PLAT_LIKELY(cur != NULL)) {
            if (PLAT_UNLIKELY(fthDebug)) {
                printf("Scheduler %p dispatching %p\n", sched, cur); fflush(NULL);
            }

            // This scheduler is not idle any more
#   ifdef VECTOR_SCHED
            (void) __sync_fetch_and_and(&fth->idleSchedMask, ~sched->schedMask);
#   endif
            FTH_SPIN_LOCK(&cur->spin);
            cur->dispatchable = 0;           // No longer on the eligble Q
            sched->dispatch.thread = cur;    // Use this for the current thread
            cur->dispatch.sched = sched;     // Remember your mama

            if (cur->schedNum != sched->schedNum) {
                cur->switchCount++;
            }

            cur->schedNum = sched->schedNum;
            cur->schedMask = sched->schedMask;
            cur->dispatchCount++;
#   ifdef ENABLE_FTH_TRACE
            traceBuffer = sched->traceBuffer;
#   endif // def ENABLE_FTH_TRACE

            FTH_SPIN_UNLOCK(&cur->spin);

#   ifdef FTH_TIME_STATS
            if (idleStartTimeStamp) {
                sched->schedulerIdleTime += dispatchTimeStamp -
                    idleStartTimeStamp;
            }
            if (sched->prevDispatchPrio) {
                sched->schedulerNumDispatches++;
            } else {
                sched->schedulerNumLowPrioDispatches++;
            }
#       ifdef FTH_TIME_MIN_MAX
            if (dispatchTimeStamp - schedStartTimeStamp < minToDispatch) {
                minToDispatch = dispatchTimeStamp - schedStartTimeStamp;
            }
            if (dispatchTimeStamp - schedStartTimeStamp > maxToDispatch) {
                maxToDispatch = dispatchTimeStamp - schedStartTimeStamp;
            }
#       endif // def FTH_TIME_MIN_MAX
#   endif // def FTH_TIME_STATS
            asm __volatile__("mfence":::"memory"); // Make sure all is seen
#   ifdef fthSetjmpLongjmp
            longjmp(cur->env, (uint64_t) cur);
#   endif // def fthSetjmpLongJmp
#   ifdef fthAsmDispatch
            fthDispatch(cur);                // Never return
#   endif // def fthAsmDispatch
            asm __volatile__("":::"memory");
        } else if (!idleStartTimeStamp) {
            *idleStartTimeStampPtr = schedStartTimeStamp;
        }
    } else {
        // Fall through to normal polling loop
        noPollRemain = fth->config.busyPollInterval;
    }
}
#   endif // FTH_FAST_SCHEDULER
#endif // ndef MULTIQ_SCHED

/**
 * @brief Scheduler main
 *
 * With fthSetjmpLongjmp defined, pthread_exit() will be called on termination
 * with a preceeding call to plat_shmem_pthread_done() to cleanup local
 * allocation pools.
 *
 * For consistency thread local arenas are also cleaned up in the
 * fthAsmDispatch case.
 */
void fthScheduler(uint64_t arg) {
    fthSched_t *sched = (fthSched_t *) arg;  // Type cheat for parameter passing
    int schedNum = sched->schedNum;

#ifdef NO_FAST_SCHED
    static int skipPolling = 0;
#endif // def NO_FAST_SCHED

#ifdef ENABLE_FTH_TRACE
    fthTraceSchedInit();
#endif // def ENABLE_FTH_TRACE
#ifdef ENABLE_FTH_RCU
    fthRcuSchedInit();
#endif // def ENABLE_FTH_RCU

#ifdef FTH_TIME_STATS
    threadSuspendTimeStamp = rdtsc();        // Init
#endif // def FTH_TIME_STATS

#ifdef FTH_IDLE_CONTROL
    if (fth->idleControlPtr) {
        fthIdleControlAttachLocal(fth->idleControlPtr);
    }
#endif // def FTH_IDLE_CONTROL

    asm __volatile__("":::"memory");         // Things could have changed
#ifdef fthSetjmpLongjmp
    sigsetjmp(sched->env, 0);
#endif
#ifdef fthAsmDispatch
    fthSaveSchedulerContext(sched);          // Save this point for fthToScheduler
#endif
    asm __volatile__("mfence":::"memory");   // Things could have changed

    // This is like Bill Murray in "Ground Hog Day":
    // the scheduler finds itself here every time it wakes up.

#ifdef VECTOR_SCHED
    (void) __sync_fetch_and_or(&fth->idleSchedMask, sched->schedMask);
#endif // def VECTOR_SCHED

#ifdef FTH_TIME_STATS
    uint64_t idleStartTimeStamp;            // Idle start (0 for not idle)
    uint64_t schedStartTimeStamp;           // Loop start

    idleStartTimeStamp = 0;
    schedStartTimeStamp = rdtsc();
    if (sched->prevDispatchPrio) {
        sched->schedulerDispatchTime += schedStartTimeStamp -
            threadSuspendTimeStamp;
    } else {
        sched->schedulerLowPrioDispatchTime += schedStartTimeStamp -
            threadSuspendTimeStamp;
    }

#   ifdef FTH_TIME_MIN_MAX
    if (schedStartTimeStamp - threadSuspendTimeStamp < minFromDispatch) {
        minFromDispatch = schedStartTimeStamp - threadSuspendTimeStamp;
    }
    if (schedStartTimeStamp - threadSuspendTimeStamp > maxFromDispatch) {
        maxFromDispatch = schedStartTimeStamp - threadSuspendTimeStamp;
    }
#   endif  // FTH_TIME_MIN_MAX
#endif  // FTH_TIME_STATS

    if (PLAT_UNLIKELY(fthDebug)) {
        printf("Back to scheduler %p\n", sched); fflush(NULL);
    }

    fthThread_t *cur = sched->dispatch.thread; // This field is used for the current thread
    sched->dispatch.thread = NULL;
    if (PLAT_LIKELY(cur != NULL)) {          // If just finished dispatch
        plat_assert(cur->dispatch.floating || cur->dispatch.sched == sched);
        cur->dispatch.sched = NULL;          // No longer running
        if (PLAT_UNLIKELY(cur->state == 'K')) { // if killed
            // Clean up storage
            plat_free_stack(cur->dispatch.stack);
            plat_attr_uthread_free(cur->local);
            FTH_SPIN_LOCK(&fth->allQSpin);   // Protect the allQ for deletes
            fthThread_t *prev = fth->allHead; // Prime the pump

            // Find the previous
            if (fth->allHead == cur) {       // It is the head
                prev = NULL;
            } else {
                while (prev->nextAll != cur) {
                    prev = prev->nextAll;
                }
            }

            // Replace the tail with the prev. This is a NOP if cur is not the tail
            (void) __sync_bool_compare_and_swap(&fth->allTail, cur, prev);

            // Now swap out the head or previous next pointer.  This is delayed until
            // after the tail is changed in case (a) cur was the tail and (b) a new
            // one got chained on just before we swapped out the cur.
            if (prev == NULL) {
                (void) __sync_bool_compare_and_swap(&fth->allHead, cur, cur->nextAll);
            } else {
                prev->nextAll = cur->nextAll; // Splice it out
            }

            FTH_SPIN_UNLOCK(&fth->allQSpin);

            FTH_FREE(cur, sizeof(fthThread_t));

        }
    }

#ifndef MULTIQ_SCHED
#   ifdef FTH_FAST_SCHEDULER
    fthFastScheduler(sched, schedStartTimeStamp, &idleStartTimeStamp);
#   endif // def FTH_FAST_SCHEDULER
#endif // ndef MULTIQ_SCHED
    while (1) {                              // Loop until we find something to do
#ifdef FTH_TIME_STATS
        dispatchTimeStamp = rdtsc();         // Remember all but the last time around loop
#endif // def FTH_TIME_STATS

#ifdef FTH_IDLE_CONTROL
        struct timespec sleepUntil;
        struct timespec *sleepUntilPtr = NULL;
        int pokeCount = 0;
#endif // def FTH_IDLE_CONTROL


#ifdef MULTIQ_SCHED
#   define POLLER_SCHED 0
        if (curSchedNum == POLLER_SCHED)
#else // def MULTIQ_SCHED
#   ifdef NO_FAST_SCHED
        if (PLAT_UNLIKELY((skipPolling-- <= 0) && FTH_SPIN_TRY(&fth->qCheckSpin))) // Try to be the queue check scheduler
#   else // def NO_FAST_SCHED
       if (PLAT_UNLIKELY(FTH_SPIN_TRY(&fth->qCheckSpin))) // Try to be the queue check scheduler
#   endif // else def NO_FAST_SCHED
#endif // else def MULTIQ_SCHED
           {

#ifdef NO_FAST_SCHED
               skipPolling = fth->config.busyPollInterval; // Refresh polling counter
#endif // def NO_FAST_SCHED

#ifdef FTH_IDLE_CONTROL
               /*
                * From the previous iteration.  Kick any other potentially
                * slumbering threads.
                */
               if (pokeCount > 0 && fth->idleControlPtr) {
                   fthIdleControlPokeLocal(fth->idleControlPtr, pokeCount);
                   pokeCount = 0;
               }
#endif // def FTH_IDLE_CONTROL

               // Process the memory queue

#if 0                                 // was MULTIQ_SCHED
               if (PLAT_UNLIKELY(fth->memQ.head == NULL)) goto  MUTEX_POLL;

               fthThreadQ_spinLock(&fth->memQ); // Lock the mem queue
               cur = fthThreadQ_head(&fth->memQ);
               while (PLAT_LIKELY(cur != NULL)) {
                   int timeout = 0;
                   if (PLAT_UNLIKELY((*(cur->memWait)) != 0 || timeout)) { // Check for mem nonzero
                       fthThread_t *dispatchable = cur;
                       cur = fthThreadQ_next(cur);
                       fthThreadQ_remove_nospin(&fth->memQ, dispatchable);
                       fthThreadQ_push(&fth->eligibleQ[dispatchable->schedNum], dispatchable);
#   ifdef VECTOR_SCHED
                       fthDispatchQ_push(&fth->eligibleQ, &dispatchable->dispatchMap);
#   else // def VECTOR_SCHED
                       fthThreadQ_push(&fth->eligibleQ, dispatchable);
#   endif // else def VECTOR_SCHED

#   ifdef FTH_IDLE_CONTROL
                       /* Wake up additional schedulers */
                       ++pokeCount;
#   endif // def FTH_IDLE_CONTROL
                   } else {                  // Not yet
                       cur = fthThreadQ_next(cur);
                   }
               }

               fthThreadQ_spinUnlock(&fth->memQ); // Done walking the queue

#else // 0,  was MULTIQ_SCHED)
#   ifdef FLASH_SIM_LATENCY
               if (fth->nextSimCompletion && rdtsc() >= fth->nextSimCompletion) {
                   flashSimCompleteOps(fth->dev);
               }
#   endif // def FLASH_SIM_LATENCY
               for (int qNum = 0; qNum < fth->memQCount; qNum++) {
                   uint64_t rv;
                   if ((fth->memTest[qNum] != NULL) && ((rv = *(fth->memTest[qNum])) != 0)) {
                       cur = fth->memQ[qNum];
                       fth->memTest[qNum] = NULL;
                       cur->dispatch.arg = rv;
#   ifdef MULTIQ_SCHED
                       if (cur->dispatch.floating) {
                           plat_assert_always(cur == floatingThread);
                           plat_assert_always(floatingQueue == NULL);
                           floatingQueue = cur;
                       } else {
                           fthThreadQ_push(&fth->eligibleQ[0], cur);
                       }
#   else // def MULTIQ_SCHED
#       ifdef VECTOR_SCHED
                       fthDispatchQ_push(&fth->eligibleQ, &cur->dispatchMap);
#       else /* def VECTOR_SCHED */
#           ifdef HI_PRIO_MEMQ
                       fthThreadQ_unshift(&fth->eligibleQ, cur); // Onto the front
#           else /* def HI_PRIO_MEMQ */
                       fthThreadQ_push(&fth->eligibleQ, cur); // Onto the back
#           endif /* else def HI_PRIO_MEMQ */
#       endif /* else VECTOR_SCHED */
#   endif // else def MULTIQ_SCHED

#   ifdef FTH_IDLE_CONTROL
                       /* Wake up additional schedulers */
                       ++pokeCount;
#   endif // def FTH_IDLE_CONTROL

                   }
               }

#endif  // else 0 was MULTIQ_SCHED


               // Process the mutex queue
#ifdef MULTIQ_SCHED
               //                  MUTEX_POLL:
               if(fth->mutexQ.head == NULL) goto  SLEEP_POLL;
#endif  // def MULTIQ_SCHED
               fthThreadQ_spinLock(&fth->mutexQ);
               cur = fthThreadQ_head(&fth->mutexQ);
               while (PLAT_UNLIKELY(cur != NULL)) {
                   if (pthread_mutex_trylock(cur->mutexWait) != 0) {
                       fthThread_t *dispatchable = cur;
                       cur = fthThreadQ_next(cur);
                       fthThreadQ_remove_nospin(&fth->mutexQ, dispatchable);
#ifdef MULTIQ_SCHED
                       if (dispatchable->dispatch.floating) {
			   plat_assert_always(dispatchable == floatingThread);
                           plat_assert_always(floatingQueue == NULL);
                           floatingQueue = dispatchable;
                       } else {
                           fthThreadQ_push(&fth->eligibleQ[dispatchable->schedNum], dispatchable);
                       }
#elif defined(VECTOR_SCHED) // def MULTIQ_SCHED
                       fthDispatchQ_push(&fth->eligibleQ, &dispatchable->dispatchMap);
#else  // defined(VECTOR_SCHED)
                       fthThreadQ_push(&fth->eligibleQ, dispatchable);
#endif // else defined(VECTOR_SCHED)

#ifdef FTH_IDLE_CONTROL
                       /* Wake up additional schedulers */
                       ++pokeCount;
#endif // def FTH_IDLE_CONTROL
                   } else {                  // Not yet
                       cur = fthThreadQ_next(cur);
                   }
               }

               fthThreadQ_spinUnlock(&fth->mutexQ); // Done walking the queue

#ifdef MULTIQ_SCHED
             SLEEP_POLL:
#endif // def MULTIQ_SCHED
               // Check to see if it is time to wake up sleeping threads
               if (PLAT_UNLIKELY(fth->sleepQ.head != NULL)) { // No point if noone home
                   if (FTH_SPIN_TRY(&fth->sleepSpin)) {
                       // Find threads to resume
                       uint64_t now = rdtsc(); // Refresh for accuracy
                       if (PLAT_UNLIKELY((now - sched->prevTimeTsc) > 10000000000)) { // Every 3-5 seconds
                           // Time to adjust the tsc ticks per nanosec
                           struct timespec nowSpec;
                           clock_gettime(CLOCK_REALTIME, &nowSpec);

                           uint64_t elapsedMicro = (nowSpec.tv_nsec - sched->prevTimeSpec.tv_nsec)/1000 +
                               (nowSpec.tv_sec - sched->prevTimeSpec.tv_sec) * 1000000;

                           // Take the average of this value and the current guess so things dont jump
                           fthTscTicksPerMicro = (fthTscTicksPerMicro + ((now - sched->prevTimeTsc) / elapsedMicro)) >> 1;
                           // Save the new values for next time
                           sched->prevTimeTsc = now;
                           sched->prevTimeSpec.tv_sec = nowSpec.tv_sec;
                           sched->prevTimeSpec.tv_nsec = nowSpec.tv_nsec;
                       }

                       fthThread_t *st = fth->sleepQ.head;
                       while ((st != NULL) && (st->sleep < now)) {
		   st = fthThreadQ_shift_precheck(&fth->sleepQ); // Must be the head
		   fthResumeInternal(st, 0);
#ifdef FTH_IDLE_CONTROL
		   ++pokeCount;
#endif // def FTH_IDLE_CONTROL
		   st = fth->sleepQ.head;

	       }
#ifdef FTH_IDLE_CONTROL
	       if (st != NULL && fth->idleControlPtr) {
		   uint64_t deltaNs = (st->sleep - sched->prevTimeTsc)
		       / (fthTscTicksPerMicro / 1000);

		   sleepUntil = sched->prevTimeSpec;
		   sleepUntil.tv_nsec += deltaNs;
		   sleepUntil.tv_sec += sleepUntil.tv_nsec / 1000000000;
		   sleepUntil.tv_nsec %= 1000000000;
		   sleepUntilPtr = &sleepUntil;
	       }
#endif // def FTH_IDLE_CONTROL

	       FTH_SPIN_UNLOCK(&fth->sleepSpin);
	   }
       }

       //
       // Check the PtofMbox global chain for new mail
       // Mail always gets pushed on here and then peeled off by the scheduler.  This
       // avoids a bunch of cross-thread race conditions
       while (1) {
	   XMboxEl_sp_t elShmem = XMboxEl_xlist_dequeue(&fth->ptofMboxPtr->headShmem,
							&fth->ptofMboxPtr->tailShmem);

	   if (PLAT_LIKELY(XMboxEl_sp_is_null(elShmem))) { // Check if anything there
	       break;                // Done for now
	   }

	   // Something chained off head - post the head
	   XMboxEl_t *el = XMboxEl_sp_rwref(&el, elShmem);
	   ptofMbox_t *mb = ptofMbox_sp_rwref(&mb, el->ptofMboxShmem);

	   // Must lock the mailbox to avoid a race where we enqueue just as a waiter arrives
	   FTH_SPIN_LOCK(&mb->spin);
	   fthThread_t *mbWaiter = fthThreadQ_shift_precheck(&mb->threadQ);
	   if (mbWaiter == NULL) {   // If no thread waiting
	       // Just queue up the mail
	       XMboxEl_xlist_enqueue(&mb->mailHeadShmem, &mb->mailTailShmem, elShmem);

	   } else {                  // Start the top wiating thread
	       fthResumeInternal(mbWaiter, el->mailShmem.base.int_base); // Kick him off
#ifdef FTH_IDLE_CONTROL
	       ++pokeCount;
#endif // def FTH_IDLE_CONTROL

	       // Release the old reference and the XMbox element
	       XMboxEl_sp_rwrelease(&el); // Release the old one
	       XMboxEl_sp_free(elShmem);

	   }
	   (void) __sync_fetch_and_add(&mb->pending, -1); // Decrement pending count
	   FTH_SPIN_UNLOCK(&mb->spin);

	   asm __volatile__("mfence":::"memory"); // Make sure all is seen
	   ptofMbox_sp_rwrelease(&mb); // Done with reference

       }


       //
       // Check the PtoF dispatch queue.
       // Threads get placed on this queue when they are waiting for an asynchronous
       // event or when a PThread needs to post the thread.  This
       // avoids a bunch of cross-thread race conditions
       while (1) {
#if RESUME_SHMEM
	   fthThread_sp_t *threadShmem = XResume_xlist_dequeue(&fth->ptofResumePtrs->headShmem,
							       &fth->ptofResumePtrs->tailShmem);
	   fthThread_t *thread = fthThread_sp_rwref(&thread, threadShmem);
#else // RESUME_SHMEM
	   fthThread_t *thread = XResume_xlist_dequeue(&ptofResumePtrs.head,
						       &ptofResumePtrs.tail);
#endif // else RESUME_SHMEM
	   if (PLAT_LIKELY(thread == NULL)) {
	       break;
	   }

	   // This is like resume but the arg is already in thread->arg
	   FTH_SPIN_LOCK(&thread->spin);
	   plat_assert(thread->dispatchable == 0);
	   plat_assert(thread->state == 'd');
#ifdef MULTIQ_SCHED
	   if (thread->dispatch.floating) {
	       plat_assert_always(thread == floatingThread);
               plat_assert_always(floatingQueue == NULL);
               floatingQueue = thread;
	   } else {
	       fthThreadQ_push(&fth->eligibleQ[thread->schedNum], thread);
	   }
#elif defined(VECTOR_SCHED) // def MULTIQ_SCHED
	   fthDispatchQ_push(&fth->eligibleQ, &thread->dispatchMap);
#else // defined(VECTOR_SCHED)
	   fthThreadQ_push(&fth->eligibleQ, thread);
#endif // else defined(VECTOR_SCHED)

	   thread->dispatchable = 1; // Mark as on dispatch queue
	   thread->state = 'D';      // Now dispatchable
	   FTH_SPIN_UNLOCK(&thread->spin);

#if RESUME_SHMEM
	   fthThread_sp_release(&thread);
	   fthThread_sp_free(threadShmem);
#endif // RESUME_SHMEM

#ifdef FTH_IDLE_CONTROL
	   ++pokeCount;
#endif // def FTH_IDLE_CONTROL
       }


#ifdef FTH_IDLE_CONTROL
	/* We'll run the first thread on this scheduler */
	if (pokeCount > 1 && fth->idleControlPtr) {
	    fthIdleControlPokeLocal(fth->idleControlPtr, pokeCount - 1);
	}
#endif // def FTH_IDLE_CONTROL

#ifndef MULTIQ_SCHED
	FTH_SPIN_UNLOCK(&fth->qCheckSpin);
#endif // ndef MULTIQ_SCHED

    }
#ifdef MULTIQ_SCHED
#       define LOW_PRIO_SCHED 0
// Check for doing a dispatch of a floating (global) thread.
        if (previousFloating != sched->schedNum && floatingQueue != NULL)
        {
            cur = atomic_xchgp(&floatingQueue, NULL);

	    if (cur == NULL) {
                ;
            } else if (cur->dispatch.sched != NULL) {
                floatingQueue = cur;
            } else {
                plat_assert_always(cur == floatingThread);
                plat_assert_always(floatingQueue == NULL);
                previousFloating = sched->schedNum;
                nextFloating = sched->schedNum + 1;

                if (nextFloating > fthFloatMax) {
                    nextFloating = 0;
                }

                fthFloatStats[sched->schedNum]++;
                goto dispatch;
            }
        }

        // Check for an eligible thread
        while (1) {
            cur = myEligibleQ->head;
            if (PLAT_UNLIKELY(cur == NULL)) {
                break;
            }
            // Make sure that the thread isn't just about to be suspended
            if (PLAT_UNLIKELY(cur->dispatch.sched != NULL)) {
            } else if (PLAT_UNLIKELY(cur->yieldCount != 0)) {
                cur->yieldCount--;
            } else {
                cur = fthThreadQ_shift_precheck(myEligibleQ);
                sched->prevDispatchPrio = 1;
                break;
            }
            // move the head to the back of the queue
            cur = fthThreadQ_shift_precheck(myEligibleQ);
            fthThreadQ_push(myEligibleQ, cur); // Push back
#   ifdef FTH_TIME_STATS
            dispatchTimeStamp = rdtsc();     // Remember all but the last time around loop
#   endif // def FTH_TIME_STATS
        }
#   ifdef FTH_TIME_STATS
        if (PLAT_UNLIKELY(cur == NULL && !idleStartTimeStamp)) {
            idleStartTimeStamp = schedStartTimeStamp;
        }
#   endif // def FTH_TIME_STATS
        if (PLAT_UNLIKELY(cur == NULL) &&  (PLAT_UNLIKELY(schedNum == LOW_PRIO_SCHED))) { // If none, try the low prio Q
            while (1) {
                cur = myLowPrioQ->head;
                if (PLAT_UNLIKELY(cur == NULL)) {
                    break;
                }
                // Make sure that the thread isn't just about to be suspended
                if (PLAT_UNLIKELY(cur->dispatch.sched != NULL)) {
                } else if (PLAT_UNLIKELY(cur->yieldCount != 0)) {
                    cur->yieldCount--;
                } else {
                    cur = fthThreadQ_shift_precheck(myLowPrioQ);
                    sched->prevDispatchPrio = 0;
                    break;
                }
                // move the head to the back of the queue
                cur = fthThreadQ_shift_precheck(myLowPrioQ);
                fthThreadQ_push(myLowPrioQ, cur); // Push back
#   ifdef FTH_TIME_STATS
                dispatchTimeStamp = rdtsc(); // Remember all but the last time around loop
#   endif // def FTH_TIME_STATS
            }
        }
#else // def MULTIQ_SCHED
        // Check for an eligible thread
        while (1) {
#   ifdef VECTOR_SCHED
#       ifdef AFFINITY_QUEUE_SCAN
            fthDispatchQ_t *map = eligibleQAffinityScan(sched);
#       else // def AFFINITY_QUEUE_SCN
            fthDispatchQ_t *map = fthDispatchQ_shift_precheck(&fth->eligibleQ);
#       endif  // else def AFFINITY_QUEUE_SCAN
            if (PLAT_UNLIKELY(map == NULL)) {
                cur = NULL;
                break;
            }
            cur = map->thread[curSchedNum];
#   else // def VECTOR_SCHED
            cur = fthThreadQ_shift_precheck(&fth->eligibleQ);
            if (PLAT_UNLIKELY(cur == NULL)) {
                break;
            }
#   endif // else def VECTOR_SCHED
            // Make sure that the thread isn't just about to be suspended
            if (PLAT_UNLIKELY(cur->dispatch.sched != NULL)) {
            } else if (PLAT_UNLIKELY(cur->yieldCount != 0)) {
                cur->yieldCount--;
            } else {
                sched->prevDispatchPrio = 1;
#   ifdef VECTOR_SCHED
                map->schedPrefMask = sched->schedMask;
#   endif // def VECTOR_SCHED
                break;
            }
#   ifdef VECTOR_SCHED
            fthDispatchQ_push(&fth->eligibleQ, map); // Push back
#   else // def VECTOR_SCHED
            fthThreadQ_push(&fth->eligibleQ, cur); // Push back
#   endif // else def VECTOR_SCHED
        }

        if (PLAT_UNLIKELY(cur == NULL)) {    // If none, try the low prio Q
            if (!idleStartTimeStamp) {
                idleStartTimeStamp = schedStartTimeStamp;
            }

#   ifdef NO_FAST_SCHED
            skipPolling = 0;                 // No high prio threads so poll next time
#   endif // def NO_FAST_SCHED
            while (1) {
#   ifdef VECTOR_SCHED
                fthDispatchQ_t *map = fthDispatchQ_shift_precheck(&fth->lowPrioQ);
                if (PLAT_UNLIKELY(map == NULL)) {
                    cur = NULL;
                    break;
                }
                cur = map->thread[curSchedNum];
#   else // def VECTOR_SCHED
                cur = fthThreadQ_shift_precheck(&fth->lowPrioQ);
                if (PLAT_UNLIKELY(cur == NULL)) {
                    break;
                }
#   endif // else def VECTOR_SCHED
                // Make sure that the thread isn't just about to be suspended
                if (PLAT_UNLIKELY(cur->dispatch.sched != NULL)) {
                } else if (PLAT_UNLIKELY(cur->yieldCount != 0)) {
                    cur->yieldCount--;
                } else {
                    sched->prevDispatchPrio = 0;
#   ifdef VECTOR_SCHED
                    map->schedPrefMask = sched->schedMask;
#   endif // def VECTOR_SCHED
                    break;
                }
#   ifdef VECTOR_SCHED
                fthDispatchQ_push(&fth->lowPrioQ, map); // Push back
#   else // def VECTOR_SCHED
                fthThreadQ_push(&fth->lowPrioQ, cur); // Push back
#   endif // else def VECTOR_SCHED
            }
        }
#endif  // else def MULTIQ_SCHED
        if (PLAT_LIKELY(cur != NULL)) {

#ifdef MULTIQ_SCHED
dispatch:
#endif
            //
            // New thread to run - switch
            //
            if (PLAT_UNLIKELY(fthDebug)) {
                printf("Scheduler %p dispatching %p\n", sched, cur); fflush(NULL);
            }

#ifdef VECTOR_SCHED
            // This scheduler is not idle any more
            (void) __sync_fetch_and_and(&fth->idleSchedMask, ~sched->schedMask);
#endif

            FTH_SPIN_LOCK(&cur->spin);
            cur->dispatchable = 0;           // No longer on the eligible queue
            sched->dispatch.thread = cur;    // Use this for the current thread
            cur->dispatch.sched = sched;     // Remember your mama

            if (cur->schedNum != schedNum) {
                cur->switchCount++;
            }

#ifndef MULTIQ_SCHED
            cur->schedNum = schedNum;
            cur->schedMask = sched->schedMask;
            cur->dispatchCount++;
#ifdef ENABLE_FTH_TRACE
            traceBuffer = sched->traceBuffer;
#endif // def ENABLE_FTH_TRACE
#endif // ndef MULTIQ_SCHED
            FTH_SPIN_UNLOCK(&cur->spin);

#ifdef FTH_TIME_STATS
            if (idleStartTimeStamp) {
                sched->schedulerIdleTime += dispatchTimeStamp -
                    idleStartTimeStamp;
            }
            if (sched->prevDispatchPrio) {
                sched->schedulerNumDispatches++;
            } else {
                sched->schedulerNumLowPrioDispatches++;
            }
            // XXX this is considered idle time above, but dispatch time below
#   ifdef FTH_TIME_MIN_MAX
            if (dispatchTimeStamp - schedStartTimeStamp < minToDispatch) {
                minToDispatch = dispatchTimeStamp - schedStartTimeStamp;
            }
            if (dispatchTimeStamp - schedStartTimeStamp > maxToDispatch) {
                maxToDispatch = dispatchTimeStamp - schedStartTimeStamp;
            }
#   endif // def FTH_TIME_MIN_MAX
#endif // def FTH_TIME_STATS
            asm __volatile__("mfence":::"memory"); // Make sure all is seen
#ifdef fthSetjmpLongjmp
            longjmp(cur->env, (uint64_t) cur);
#endif // def fthSetjmpLongjmp
#ifdef fthAsmDispatch
            fthDispatch(cur);                // Never return
#endif // de ffthAsmDispatch
            asm __volatile__("":::"memory");

            //
            // We should never get here
            //
        }

        if (fth->kill) {                     // Have we been killed?


#   ifdef VECTOR_SCHED
            // This scheduler is not idle any more
            (void) __sync_fetch_and_and(&fth->idleSchedMask, ~sched->schedMask);
#   endif

#   ifdef FTH_IDLE_CONTROL
            fthIdleControlDetachLocal(fth->idleControlPtr);
#   endif


#   ifdef fthSetjmpLongjmp
            // XXX: drew 2009-02-19 I don't know why this shouldn't be
            // handled by the same race-condition avoidance code as the
            // fthAsmDispatch case
            plat_shmem_pthread_done();

            pthread_exit(NULL);              // No returns
#   endif // def fthSetjmpLongJmp
#   ifdef fthAsmDispatch
            // There is a race here but we don't care since this is for testing
            if (__sync_fetch_and_sub(&fth->kill, 1) > 0) {
                plat_shmem_pthread_done();
                return;
            }
#   endif // def fthAsmDispatch
        }

#   ifdef FTH_RCU
        fthRcuUpdate();
#   endif // def FTH_RCU

#   ifdef FTH_IDLE_CONTROL
        if (fth->idleControlPtr) {
            // Note that this is unreachable when we have any memory
            // waits.
            // This scheduler is not idle any more
#       ifdef VECTOR_SCHED
            (void) __sync_fetch_and_and(&fth->idleSchedMask, ~sched->schedMask);
#       endif // def VECTOR_SCHED

            fthIdleControlWaitLocal(fth->idleControlPtr, sleepUntilPtr,
                                    fthThreadQ_head(&fth->memQ) ? 1 : 0);
#       ifdef VECTOR_SCHED
            (void) __sync_fetch_and_or(&fth->idleSchedMask, sched->schedMask);
#       endif // def VECTOR_SCHED
        } else
#   endif // def FTH_IDLE_CONTROL
        {
            // Release malloc waiters in case we have more mem now
            while (1) {
                fthThread_t *thread = fthThreadQ_shift(&fth->mallocWaitThreads);
                if (thread == NULL) {
                    break;
                }
                fthResumeInternal(thread, 0); // Restart
            }


            // Spin a while to avoid locking out others on the linked list
            //            for (int i = 0; i < 10; i++) {
            //              asm("rep; nop");             // Tell processor to cool heels
            //        }

        }
    }

}

/**
 * @brief Give up processor, go undispatchable.
 *
 * @return Value specified by caller to fthResume.
 */
uint64_t fthWait() {
#ifdef FTH_RCU
    fthRcuUpdate();
#endif
    fthThread_t *thread = fthSelf();
    // The thread could be taken off of whatever wait Q that it is currently on (i.e.,
    // mbox wait) and put on the dispatch Q at this point.  So, the state may have been
    // changed from 'R' to 'd' or even 'D'.  Or maybe the caller has set a special state.
    // Either way, we only replace the 'R' state.
    (void) __sync_bool_compare_and_swap(&thread->state, 'R', 'W'); // Only replace R state
#ifdef FTH_TIME_STATS
    threadSuspendTimeStamp = rdtsc();
    thread->runTime += threadSuspendTimeStamp - threadResumeTimeStamp;
    sched->totalThreadRunTime += threadSuspendTimeStamp - threadResumeTimeStamp;
#endif
    sched->prevDispatchPrio = 1;             // High prio if waiting
#ifdef fthSetjmpLongjmp
    if (sigsetjmp(thread->env, 0) == 0) {
        siglongjmp(thread->dispatch.sched->env, 1);
    }
#endif
#ifdef fthAsmDispatch
    fthToScheduler(thread, 0);                  // Go away
#endif
    thread->state = 'R';                     // Running again

#ifdef FTH_TIME_STATS
    threadResumeTimeStamp = rdtsc();
    sched->schedulerDispatchTime += threadResumeTimeStamp - dispatchTimeStamp;
#endif

    return (thread->dispatch.arg);
}

/**
 * @brief Give up processor but remain dispatchable
 *
 * @param count <IN> For non-negative values the thread is skipped count
 * times.  Negative values put fthSelf() on the low priority queue which
 * is only checked when no regular priority threads are eligible for
 * dispatch with the thread in question skipped -count - 1 times.
 */
void fthYield(int count) {
#ifdef FTH_RCU
    fthRcuUpdate();
#endif
    fthThread_t *thread = fthSelf();

#ifdef FTH_TIME_STATS
    threadSuspendTimeStamp = rdtsc();
    thread->runTime += threadSuspendTimeStamp - threadResumeTimeStamp;
    sched->totalThreadRunTime += threadSuspendTimeStamp - threadResumeTimeStamp;
#endif

    FTH_SPIN_LOCK(&thread->spin);
    plat_assert(thread->dispatchable == 0);
    if (count >= 0) {
        sched->prevDispatchPrio = 1;
        thread->yieldCount = count;
#ifdef MULTIQ_SCHED
        if (! thread->dispatch.floating) {
            fthThreadQ_push(&fth->eligibleQ[thread->schedNum], thread); // Reschedule
        }
#else /* ! MULTIQ_SCHED */
#ifdef VECTOR_SCHED
        fthDispatchQ_push(&fth->eligibleQ, &thread->dispatchMap);
#else
        fthThreadQ_push(&fth->eligibleQ, thread);
#endif
#endif /* MULTIQ_SCHED */

    } else {
        sched->prevDispatchPrio = 0;
        thread->yieldCount = -1 - count;
#ifdef VECTOR_SCHED
        fthDispatchQ_push(&fth->lowPrioQ, &thread->dispatchMap);
#else
        if (! thread->dispatch.floating) {
           fthThreadQ_push(&fth->lowPrioQ, thread);
        }
#endif
    }
    thread->dispatchable = 1;                // Mark as dispatchable
    FTH_SPIN_UNLOCK(&thread->spin);

#ifdef fthSetjmpLongjmp
    if (sigsetjmp(thread->env, 0) == 0) {
        siglongjmp(thread->dispatch.sched->env, 1);
    }
#endif
#ifdef fthAsmDispatch
    fthToScheduler(thread, 1);               // Go away
#endif
    thread->state = 'R';                     // Running again

#ifdef FTH_TIME_STATS
    threadResumeTimeStamp = rdtsc();
    if (count >= 0) {
        sched->schedulerDispatchTime += threadResumeTimeStamp - dispatchTimeStamp;
    } else {
        sched->schedulerLowPrioDispatchTime += threadResumeTimeStamp - dispatchTimeStamp;
    }
#endif

}

#ifdef VECTOR_SCHED
/**
 * @brief Wait until all threads in a vector are yielding
 *
 * The last thread in the vector starts the yield
 *
 * @param vector <IN> Thread vector pointer
 * @param count <IN> yield count - should be the same for each thread
 *
 *
 */
void fthVectorYield(int count) {

    fthThread_t *thread = fthSelf();

    FTH_SPIN_LOCK(&thread->spin);
    plat_assert(thread->dispatchable == 0);

    thread->vectorWait = 1;                  // Part of a vector

#ifdef FTH_TIME_STATS
    if (count >= 0) {
        sched->prevDispatchPrio = 1;
        thread->yieldCount = count;          // Should be the same for each thread
    } else {
        sched->prevDispatchPrio = 0;
        thread->yieldCount = -1 - count;     // Should be the same for each thread
    }
    thread->dispatchable = 1;                // Mark as dispatchable
    FTH_SPIN_UNLOCK(&thread->spin);

    threadSuspendTimeStamp = rdtsc();
    thread->runTime += threadSuspendTimeStamp - threadResumeTimeStamp;
    sched->totalThreadRunTime += threadSuspendTimeStamp - threadResumeTimeStamp;
#endif

    if (__sync_add_and_fetch(&thread->vector->waitCount, 1) == thread->vector->threadCount) {
        // Last to yield - start wait
        if (count >= 0) {
#ifdef VECTOR_SCHED
            fthDispatchQ_push(&fth->eligibleQ, &thread->dispatchMap);
#else
            if (! thread->floating) {
                fthThreadQ_push(&fth->eligibleQ, thread);
            }
#endif
        } else {
#ifdef VECTOR_SCHED
            fthDispatchQ_push(&fth->lowPrioQ, &thread->dispatchMap);
#else
            if (! thread->floating) {
                fthThreadQ_push(&fth->eligibleQ, thread);
            }
#endif
        }
    }

    // Now control is passed back to the scheduler.  If this is not the last thread in the
    // vector then it is not currently eligible for redispatch.  That has to wait until all
    // of the threads in the vector have yielded.

#ifdef fthSetjmpLongjmp
    if (sigsetjmp(thread->env, 0) == 0) {
        siglongjmp(sched->env, 1);
    }
#endif
#ifdef fthAsmDispatch
    fthToScheduler(thread, 0);                  // Go away
#endif

    // The yield is over and exactly one of the threads in the vector is running again
    (void) __sync_sub_and_fetch(&thread->vector->waitCount, 1); // One less waiting

    thread->state = 'R';                     // Running again
    thread->vectorWait = 0;                  // Not in the vector for now

#ifdef FTH_TIME_STATS
    threadResumeTimeStamp = rdtsc();
    if (count >= 0) {
        sched->schedulerDispatchTime += threadResumeTimeStamp - dispatchTimeStamp;
    } else {
        sched->schedulerLowPrioDispatchTime += threadResumeTimeStamp - dispatchTimeStamp;
    }
#endif

}



/**
 * @brief Wait until all threads in a vector are waiting
 *
 * The last thread in the vector gets a non-zero return
 *
 * @param vector <IN> Thread vector pointer
 *
 * @return TRUE if this is the last waiter for the vector
 */
int fthVectorQueue(fthThreadVector_t *vector) {

    return (__sync_add_and_fetch(&vector->waitCount, 1) == vector->threadCount);
}

#endif


/**
 * @brief Wait element allocation.
 *
 * @return free wait element which caller must return with call to fthFreeWaitEl
 */
fthWaitEl_t *fthGetWaitEl() {
    fthWaitEl_t *rv = NULL;
    if (sched != NULL) {
        rv = fthWaitQ_shift_nospin(&sched->freeWait);
    }

    if (rv != NULL) {                        // Check if we got one
        sched->freeWaitCount--;
    } else {
        rv = fthWaitQ_shift_precheck(&fth->freeWait); // Try the global queue
    }

    while (rv == NULL) {                     // Check if we got one
        rv = FTH_MALLOC(sizeof(fthWaitEl_t)); // Allocate a new one
        if (rv != NULL) {                    // If malloc worked
            rv->pool = 1;                    // This is a pool element
#ifndef TEMP_WAIT_EL
            rv->list = fth->waitEls;
            fth->waitEls = rv;
#endif
            break;
        }

        // Malloc failed - wait
        fthThreadQ_push(&fth->mallocWaitThreads, fthSelf());
        (void) fthWait();                     // Sleep for a while
    }

    fthWaitQ_el_init(rv);                    // Init the linked list element
    rv->thread = (void *) fthLockID();       // Remember the thread or PID
#ifndef TEMP_WAIT_EL
    rv->caller = (void *) __builtin_return_address(1);
#endif
    return (rv);
}

/**
 * @brief Wait element release
 *
 * @param waitEl <IN> Wait element previously returned by fthGetWaitEl
 */
void fthFreeWaitEl(fthWaitEl_t *waitEl) {
    plat_assert(waitEl->pool == 1);
#ifndef TEMP_WAIT_EL
    waitEl->caller = NULL;
#endif
    if ((sched != NULL) && (sched->freeWaitCount < SCHED_MAX_FREE_WAIT)) {
        fthWaitQ_push_nospin(&sched->freeWait, waitEl);
        sched->freeWaitCount++;
    } else {                                 // Put onto global free list
        fthWaitQ_push(&fth->freeWait, waitEl);
    }
}

/**
 * @brief Sparse lock (not table) allocation.
 *
 * @return free lock element which caller must return with call to fth
 */
fthSparseLock_t *fthGetSparseLock() {
    fthSparseLock_t *rv = fthSparseQ_shift_nospin(&sched->freeSparse);

    if (rv != NULL) {                        // Check if we got one
        sched->freeSparseCount--;
    } else {
        rv = fthSparseQ_shift(&fth->freeSparse); // Try the global queue
    }

    if (rv == NULL) {                        // Check if we got one
        rv = FTH_MALLOC(sizeof(fthSparseLock_t)); // Allocate
    }

    fthSparseQ_el_init(rv);
    fthLockInit(&rv->lock);                  // Init the embedded FTH lock
    rv->useCount = 0;
    return (rv);
}

/**
 * @brief sparse lockrelease
 *
 * @param sl <IN> sparse lock previously returned by fthGetSparseLock
 */
void fthFreeSparseLock(fthSparseLock_t *sl) {
    if (sched->freeSparseCount < SCHED_MAX_FREE_SPARSE) {
        fthSparseQ_push_nospin(&sched->freeSparse, sl);
        sched->freeSparseCount++;
    } else {
        fthSparseQ_push(&fth->freeSparse, sl);
    }
}

/**
 * @brief sleep
 *
 * @param nanoSecs <IN>  nonseconds to sleep (from now)
 */
void fthNanoSleep(uint64_t nanoSecs) {
    fthThread_t *self = fthSelf();

    self->sleep = (nanoSecs * fthTscTicksPerMicro / 1000) + rdtsc();

    // Insert into the sleep Q at the appopriate place
    FTH_SPIN_LOCK(&fth->sleepSpin);
    fthThread_t *st = fth->sleepQ.head;
    fthThread_t *prevST = NULL;
    while ((st != NULL) && (st->sleep < self->sleep)) {
        prevST = st;
        st = st->threadQ.next;
    }

    fthThreadQ_insert_nospin(&fth->sleepQ, prevST, self);

    FTH_SPIN_UNLOCK(&fth->sleepSpin);

    (void) fthWait();

}

/**
 * @brief get an estimate of the current time of day
 *
 * Fills in timeval structure just like gettimeofday.  For the timezone info
 * just call gettimeofday itself since this doesn't change.
 *
 * @param tv <IN> system timeval
 *
 */
void fthGetTimeOfDay(struct timeval *tv) {
    tv->tv_usec = sched->prevTimeSpec.tv_nsec/1000 + (rdtsc() - sched->prevTimeTsc)/fthTscTicksPerMicro;
    tv->tv_sec = sched->prevTimeSpec.tv_sec;
    if (tv->tv_usec >= 1000000) {
        tv->tv_sec += (int) tv->tv_usec/1000000;
        tv->tv_usec -= ((int) (tv->tv_usec/1000000)) * 100000;
    }
}

/**
 * @brief get an estimate of the current time in seconds
 *
 * Returns just the time in seconds like time
 *
 */
uint64_t fthTime(void) {
    uint64_t usec = sched->prevTimeSpec.tv_nsec/1000 + (rdtsc() - sched->prevTimeTsc)/fthTscTicksPerMicro;
    uint64_t sec = sched->prevTimeSpec.tv_sec;
    if (usec >= 1000000) {
        sec += (int) usec/1000000;
    }
    return (sec);
}

/**
 * @brief get total run time for this thread
 *
 * @param thread <IN> thread pointer or null (self)
 *
 * @return - thread run time as of last wait (does not include current time quanta)
 *
 */
uint64_t fthThreadRunTime(fthThread_t *thread) {
    if (thread == NULL) {
        thread = fthSelf();
    }
#ifdef FTH_TIME_STATS
    return (thread->runTime / fthTscTicksPerMicro);
#else
    return 0;
#endif
}

/**
 * @brief Return accumulated FTH thread run time in microseconds
 *
 * @return accumulated FTH thread run time
 */
uint64_t fthGetTotalThreadRunTime(void) {
#ifdef FTH_TIME_STATS
    uint64_t fthTotalThreadRunTime = 0;
    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (fth->scheds[i] != NULL) {
            fthTotalThreadRunTime += fth->scheds[i]->totalThreadRunTime;
        }
    }
    return (fthTotalThreadRunTime / fthTscTicksPerMicro);
#else
    return 0;
#endif
}

/**
 * @brief Return accumulated FTH scheduler dispatch time in microseconds
 *
 * @return accumulated FTH dispatch time
 */
uint64_t fthGetSchedulerDispatchTime(void) {
#ifdef FTH_TIME_STATS
    uint64_t fthSchedulerDispatchTime = 0;
    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (fth->scheds[i] != NULL) {
            fthSchedulerDispatchTime += fth->scheds[i]->schedulerDispatchTime;
        }
    }
    return (fthSchedulerDispatchTime / fthTscTicksPerMicro);
#else
    return 0;
#endif
}

/**
 * @brief Return accumulated FTH scheduler low prio dispatch time in microseconds
 *
 * @return accumulated low prio FTH dispatch time
 */
uint64_t fthGetSchedulerLowPrioDispatchTime(void) {
#ifdef FTH_TIME_STATS
    uint64_t fthSchedulerLowPrioDispatchTime = 0;
    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (fth->scheds[i] != NULL) {
            fthSchedulerLowPrioDispatchTime += fth->scheds[i]->schedulerLowPrioDispatchTime;
        }
    }
    return (fthSchedulerLowPrioDispatchTime / fthTscTicksPerMicro);
#else
    return 0;
#endif
}

/**
 * @brief Return accumulated number of thread dispathes
 *
 * @return Total number of times threads were dispatched
 */
uint64_t fthGetSchedulerNumDispatches(void) {
#ifdef FTH_TIME_STATS
    uint64_t fthSchedulerNumDispatches = 0;
    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (fth->scheds[i] != NULL) {
            fthSchedulerNumDispatches += fth->scheds[i]->schedulerNumDispatches;
        }
    }
    return (fthSchedulerNumDispatches);
#else
    return 0;
#endif
}

/**
 * @brief Return accumulated number of low-prio thread dispathes
 *
 * @return Total number of times low-prio threads were dispatched
 */
uint64_t fthGetSchedulerNumLowPrioDispatches(void) {
#ifdef FTH_TIME_STATS
    uint64_t fthSchedulerNumLowPrioDispatches = 0;
    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (fth->scheds[i] != NULL) {
            fthSchedulerNumLowPrioDispatches += fth->scheds[i]->schedulerNumLowPrioDispatches;
        }
    }
    return (fthSchedulerNumLowPrioDispatches);
#else
    return 0;
#endif
}

/**
 * @brief Return average dispatch time in nanoseconds
 *
 * @return Dispatch time divided by number of dispatches times tsc ticks per nanosecond
 */
uint64_t fthGetSchedulerAvgDispatchNanosec(void) {
#ifdef FTH_TIME_STATS
    return (fthGetSchedulerDispatchTime() * 1000 / fthGetSchedulerNumDispatches() / fthTscTicksPerMicro);
#else
    return 0;
#endif
}

/**
 * @brief Return accumulated FTH scheduler idle time in microseconds
 *
 * @return idle time
 */
uint64_t fthGetSchedulerIdleTime(void) {
#ifdef FTH_TIME_STATS
    uint64_t fthSchedulerIdleTime = 0;
    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (fth->scheds[i] != NULL) {
            fthSchedulerIdleTime += fth->scheds[i]->schedulerIdleTime;
        }
    }
    return (fthSchedulerIdleTime / fthTscTicksPerMicro);
#else
    return 0;
#endif
}

/**
 * @brief Return voluntary context switches
 *
 * @return context switches
 */
uint64_t fthGetVoluntarySwitchCount(void) {
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    return (usage.ru_nvcsw);
}

/**
 * @brief Return involuntary context switches
 *
 * @return context switches
 */
uint64_t fthGetInvoluntarySwitchCount(void) {
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    return (usage.ru_nivcsw);
}

/**
 * @brief Return the estimated tsc ticks per microsecond
 *
 * @return tstTicksPerMicro
 */
uint64_t fthGetTscTicksPerMicro(void) {
    return (fthTscTicksPerMicro);
}

long fthGetDefaultStackSize() {
    // Should probably set this some how more interesting.  3 * 4096
    // proves insufficient for the system vfprintf in some cases.
    return (4096 * 5);
}

#ifdef FTH_INSTR_LOCK

/**
 * @brief Get a pointer to the per-scheduler lock tracing data structure
 *
 * @return Pointer to lock trace data for this scheduler
 */
fthLockTraceData_t *fthGetLockTraceData()
{
    if (sched == NULL) {
        return (NULL);
    } else {
        return (sched->locktrace_data);
    }
}

#endif // FTH_INSTR_LOCK
