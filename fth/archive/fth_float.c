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

#include "valgrind/valgrind.h"

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

XLIST_IMPL(allThreads, fthThread, nextAll);
XLIST_IMPL(XResume, fthThread, resumeNext);

#include "sdfappcommon/XList.h"
#include "sdfappcommon/XMbox.h"
#include "sdfappcommon/XResume.h"
#include "platform/assert.h"
#include "platform/shmem_global.h"

#undef   max
#define  max(a, b)     ((a) > (b) ? (a) : (b))
#define  pf(...)       printf(__VA_ARGS__); fflush(NULL)
#define array_size(a)  (sizeof(a) / sizeof((a)[0]))

typedef struct
{
   uint64_t  pad[8];
} fth_line_t;

/*
 *  These variables are largely read-only.
 */

fth_t *                   fth = NULL;
static pthread_once_t     pthreadOnce = PTHREAD_ONCE_INIT;
static ptofThreadPtrs_t   ptofResumePtrs;
void *volatile            floatingThread;
uint64_t                  fthFloatMax;
int                       schedNum;
volatile int              currOpenSched;
int                       totalScheds;
static int                fthDebug;

/*
 *  These variables are used to schedule the floating thread, and will ping
 *  between caches.
 */

static fth_line_t       pad1;
static int              previousFloating;
static int              nextFloating;
void * volatile         floatingQueue;
static fth_line_t       pad2;
uint64_t                fthFloatStats[FTH_MAX_SCHEDS];
static fth_line_t       pad3;

/*
 *  These variables are thread-specific.
 */

static __thread fthSched_t *  sched = NULL;
__thread int                  curSchedNum = -1;

// Convenient thread-local pointers to "this" scheduler's
// queues. The idea is to minimize sharing access to the
// global fth structure to the extent possible.

static __thread fthThreadQ_lll_t *  myEligibleQ;
static __thread fthThreadQ_lll_t *  myLowPrioQ;
static __thread fthThreadQ_lll_t *  myMutexQ;
static __thread fthThreadQ_lll_t *  mySleepQ;

#ifdef FTH_TIME_STATS
static __thread uint64_t  threadSuspendTimeStamp;
static __thread uint64_t  threadResumeTimeStamp;
static __thread uint64_t  dispatchTimeStamp;

static uint64_t           minFromDispatch = 0x7fffffffffffffff;
static uint64_t           maxFromDispatch = 0;
static uint64_t           minToDispatch = 0x7fffffffffffffff;
static uint64_t           maxToDispatch = 0;
#endif

uint64_t fthTscTicksPerMicro = 3000;  // 3 GHZ is a good guess to start

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
fthUthreadGetter(void) {
    fthThread_t *thread = fthSelf();
    return thread ? thread->local : NULL;
}

/**
 * @brief Initialize CPU info fields
 */

static void
fthInitCpuInfo(void) {
    int status;
    int sched;
    int cpu;
    int partnerCPU;
    uint32_t partnerCPUs;
    uint32_t printCPUs;

    plat_assert(fth);

    memset(&fth->cpuInfo, 0, sizeof (fth->cpuInfo));

    memset(&pad1, 0, sizeof(pad1));  // keep gcc happy.
    memset(&pad2, 0, sizeof(pad2));
    memset(&pad3, 0, sizeof(pad3));

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

    for (sched = 0; sched < fth->cpuInfo.numCPUs; ++sched) {
        cpu = fth->cpuInfo.schedToCPU[sched];
        status = plat_get_cpu_cache_peers(&partnerCPUs, cpu);
        plat_assert_always(!status);

        for (partnerCPU = 0; partnerCPU < (sizeof (partnerCPUs) * CHAR_BIT);
             ++partnerCPU) {

            if (partnerCPUs & (1 << partnerCPU)) {
                fth->cpuInfo.schedToPartners[sched] |= (1 << partnerCPU);
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

static void
initSchedQs(int totalScheds) {
    int schedNum;

    for(schedNum = 0; schedNum < totalScheds; schedNum++) {
        fthThreadQ_lll_init(&fth->eligibleQ[schedNum]);
        fthFloatMax = totalScheds - 1;
    }
}

int nextOpenSched(long stackSize) {
    plat_assert_always(totalScheds > 0);

    if (totalScheds == 1) {
        return 0;
    }

    int ret = currOpenSched % fth->totalScheds;

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

    if (numArgs > 0) {
        inTotalScheds = va_arg(argp, int);
    }

    plat_assert_always(inTotalScheds <= FTH_MAX_SCHEDS && inTotalScheds > 0);

    /*
     *  CA: unfortunately  fthPthreadInit  has to be a (void) routine so we
     *  use an ugly global to pass the totalScheds info.
     *  The assert catches multiple fthInits.
      */

    if (__sync_lock_test_and_set(&totalScheds, inTotalScheds) == 0) {
        // Let pthread make the overall init happen exactly once
        pthread_once(&pthreadOnce, &fthPthreadInit);
    }
}

/**
 * @brief Set affinity for calling scheduler
 *
 * Includes sched->partnerMask
 */
static void
fthSetAffinity(void) {
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
 * threads.  See #fthScheduler for details.
 *
 * @param prio <IN> Realtime priority (1-99) or 0 if not a realtime thread
 */
void fthSchedulerPthread(int prio) {
    struct sched_param sp;
    int rc;

    plat_assert(fth != NULL);

    // Schedulers have a stack
    sched = FTH_MALLOC(sizeof(fthSched_t));
    sched->dispatch.stackSize = FTH_STACK_ALIGN(4096);
    sched->dispatch.stack = plat_alloc_stack(sched->dispatch.stackSize);
    char *end = (char *) sched->dispatch.stack + sched->dispatch.stackSize;
    sched->dispatch.stackId =
         VALGRIND_STACK_REGISTER(sched->dispatch.stack, end);
    plat_assert_always(sched->dispatch.stack);
    sched->dispatch.startRoutine = &fthScheduler;
    sched->dispatch.sched = NULL;                    // No scheduler yet

    // Leave space for sched structure and round down to 16 byte alignment
    // for x86_64.

    end  = (char *) sched->dispatch.stack;
    end += sched->dispatch.stackSize - sizeof (fthSched_t);
    end  = (char *) ((uintptr_t) end & ~0xf);

    // Make space for "return" address since entry is by jmp not call
    // thus ensuring the correct offset from the required alignment for
    // GCC's alignment preservation.  Otherwise printf("%g") will segfault.
    end -= sizeof (void *);

    // Make return address NULL so GDB stack back traces terminate
    *(void **)end = NULL;

    // Hack the longjmp buffer
    sched->dispatch.pc = (uint64_t) NULL;
    sched->dispatch.rsp = (uint64_t)end;
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

    fthSetAffinity();
    fth->scheds[sched->schedNum] = sched;

    // Init the wait element lists
    sched->freeWaitCount = 0;
    sched->freeSparseCount = 0;
    fthWaitQ_lll_init(&sched->freeWait);
    fthSparseQ_lll_init(&sched->freeSparse);

#ifdef FTH_INSTR_LOCK
    /* per-scheduler state for lock tracing facility */
    sched->locktrace_data =
         (fthLockTraceData_t *) plat_alloc(sizeof(fthLockTraceData_t));
    sched->locktrace_data->schedNum       = sched->schedNum;
    sched->locktrace_data->n_trace_recs   = 0;
    sched->locktrace_data->n_lock_trace   = 0;
#endif

    // Put initial values into the time adjuster
    sched->prevTimeTsc = rdtsc();
    clock_gettime(CLOCK_REALTIME, &sched->prevTimeSpec);

    myEligibleQ = &fth->eligibleQ[sched->schedNum];
#ifdef multi_low
    myLowPrioQ =  &fth->lowPrioQ[sched->schedNum];
#else
    myLowPrioQ =  &fth->lowPrioQ;
#endif
    myMutexQ   =  &fth->mutexQ;
    mySleepQ   =  &fth->sleepQ;

    fthScheduler((uint64_t) sched);
}

/**
 * @brief called to stop FTH (test only)
 */
void fthKill(int kill) {
    fth_t *fth = fthBase();
    fth->kill = kill;
    asm volatile("mfence");
}

/**
 * @brief Init FTH structure - Pthread ensures that this gets called just once
 *
 * FIXME: drew 2008-04-22
 *
 * 1.  This assumes a single heavy weight process is receiving ptof mail boxes.
 *     The correct fix is to hang the the mailbox structure off plat_process
 *     and associate ptof XMbox with specific target processes.
 *
 * 2.  This assumes fail-stop behavior since we combine shared memory
 *     allocation with initialization.
 */
void fthPthreadInit(void) {
    uint64_t existing;
    ptofMboxPtrs_t *ptofMboxPtr;

    // XXX There needs to be a complementary shutdown function so that
    // programmatic leak detection does not break

    // General allocation and init
    fth = FTH_MALLOC(sizeof(fth_t));
    memset(fth, 0, sizeof (*fth));
    fth->config = fthConfig;
    fthInitCpuInfo();
    fth->totalScheds = totalScheds;

    /*
     *  CA: Each scheduler has its own queue.
     *  We preemptively init all the queues for each of
     *  totalScheds schedulers now to avoid
     *  races later
     */

    initSchedQs(totalScheds);
#ifdef multi_low
    for (int i = 0; i < array_size(fth->lowPrioQ); i++) {
        fthThreadQ_lll_init(&fth->lowPrioQ[i]);
    }
#else
    fthThreadQ_lll_init(&fth->lowPrioQ);
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
    fth->waitEls = NULL;
    fthSparseQ_lll_init(&fth->freeSparse);

    fth->allHead = NULL;
    fth->allTail = NULL;

    FTH_SPIN_INIT(&fth->sleepSpin);
    FTH_SPIN_INIT(&fth->allQSpin);

    fth->kill = 0;

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
    plat_assert(!existing);
    fth->ptofMboxPtr = ptofMboxPtr;

    // We do not use the shmem version for now because we have no shmem-based
    // fthThread ptrs
    ptofResumePtrs.head = NULL;
    ptofResumePtrs.tail = NULL;

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

    return my_sched->dispatch.thread;         // Saved here
}

/**
 * @brief return global id
 */

void * fthId(void) {
   return fthSelf();
}

/**
 * @brief return a non-zero ID to use when setting locks.  This is for debug
 *        and recovery.
 */
inline uint64_t fthLockID(void) {
    if (sched != NULL) {
        return (uint64_t) sched->dispatch.thread;    // Saved here
    }

    return getpid();                        // Just use the process ID
}

/**
 * @brief return fth structure pointer
 */
inline fth_t *fthBase(void) {
    return fth;
}

/**
 * @brief return fth resume pointer
 */
inline struct ptofThreadPtrs *fthResumePtrs(void) {
    return &ptofResumePtrs;
}

/*
 * @brief Dummy routine that starts every thread
 */
void fthDummy(fthThread_t *thread) {
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
    fthToScheduler(thread, 0);               // Give up the CPU forever
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
    thread->dispatch.stackId =
        VALGRIND_STACK_REGISTER(thread->dispatch.stack, end);
    plat_assert_always(thread->dispatch.stack);
    thread->dispatch.startRoutine = startRoutine;
    thread->state = 'N';                     // Virgin thread
    thread->dispatch.sched = NULL;           // No scheduler yet
    thread->schedNum = nextOpenSched(minStackSize);
    thread->waitElCount = 0;
    thread->spin = 0;
    thread->dispatchable = 0;
    thread->yieldCount = 0;
    thread->defaultYield = 0;
    thread->nextAll = NULL;
    thread->dispatch.floating = 0;

    allThreads_xlist_enqueue(&fth->allHead, &fth->allTail, thread);

    // XXX: Thread struct isn't actually stored on the stack, so why make
    // space for it?

    // Leave space for thread structure and round down to 16 byte alignment
    // for x86_64.
    end  = (char *) thread->dispatch.stack;
    end += thread->dispatch.stackSize - sizeof (fthThread_t);
    end  = (char *) ((uintptr_t) end & ~0xf);

    // Make space for "return" address since entry is by jmp not call
    // thus ensuring the correct offset from the required alignment for
    // GCC's alignment preservation.  Otherwise printf("%g") will segfault.
    end -= sizeof (void *);

    // Make return address NULL so GDB stack back traces terminate
    *(void **)end = NULL;

    // Hack the longjump buffer
    thread->dispatch.pc = (uint64_t) &fthDummy;
    thread->dispatch.rsp = (uint64_t)end;
    thread->local = plat_attr_uthread_alloc();
    plat_assert_always(thread->local);

    return thread;
}

fthThread_t *fthMakeFloating(fthThread_t *thread) {
   fthThread_t *old;

   plat_assert_always(floatingThread == NULL);
   thread->dispatch.floating = -1;
   old = atomic_xchgp(&floatingThread, thread);
   plat_assert_always(old == NULL);
   return thread;
}

/**
 * @brief Make thread dispatchable
 *
 * The thread should have called fthWait (which made the thread undispatchable)
 *
 * @parameter thread <IN> Thread to make dispatchable
 * @parameter rv <IN> Value that the thread sees as the RV from fthWait
 */
static  void fthResumeInternal(fthThread_t *thread, uint64_t rv)
{
    FTH_SPIN_LOCK(&thread->spin);
    plat_assert(thread->dispatchable == 0);
    // Check the state and atomically change it to make sure that the thread
    // is never put on the XResume queue twice.  CDS logic is needed because
    // XResume cannot do a spin lock.
    char threadState;

    do {
        threadState = thread->state;
        plat_assert(threadState == 'R' || threadState == 'W'
                    || threadState == 'N');
    } while (!__sync_bool_compare_and_swap(&thread->state, threadState, 'd'));

    thread->dispatch.arg = rv;               // Save as return parameter

    if (thread->dispatch.floating) {
       plat_assert_always(thread == floatingThread);
       plat_assert_always(floatingQueue == NULL);
       floatingQueue = thread;
    } else {
        fthThreadQ_push(&fth->eligibleQ[thread->schedNum], thread);
    }

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
}

void
fthPrintSwitchStats(void) {
    /*  print the thread dispatch and switch stats for all fthreads */
    fthThread_t *curTh = fth->allHead;
    int totalDispatches = 0;
    int totalSwitches  =  0;

    while (curTh) {
        totalDispatches += curTh->dispatchCount;
        totalSwitches += curTh->switchCount;
        curTh = curTh->nextAll;
    }

    printf("\nTotal Dispatches = %d , Total Scheduler Switches = %d \n",
           totalDispatches, totalSwitches);
    return;
}

static inline void fthRun(fthThread_t *cur, uint64_t idleStartTimeStamp,
                          uint64_t schedStartTimeStamp)
{
    //
    // New thread to run - switch
    //

    if (PLAT_UNLIKELY(fthDebug)) {
        printf("Scheduler %p dispatching %p\n", sched, cur); fflush(NULL);
    }

    FTH_SPIN_LOCK(&cur->spin);
    cur->dispatchable = 0;           // No longer on the eligible queue
    sched->dispatch.thread = cur;    // Use this for the current thread
    cur->dispatch.sched = sched;     // Remember your mama

    if (cur->schedNum != schedNum) {
        cur->switchCount++;
    }

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

    if (dispatchTimeStamp - schedStartTimeStamp < minToDispatch) {
        minToDispatch = dispatchTimeStamp - schedStartTimeStamp;
    }

    if (dispatchTimeStamp - schedStartTimeStamp > maxToDispatch) {
        maxToDispatch = dispatchTimeStamp - schedStartTimeStamp;
    }
#endif

    asm __volatile__("mfence":::"memory"); // Make sure all is seen
    fthDispatch(cur);                // Never return
    asm __volatile__("":::"memory");

    //
    // We should never get here
    //
}

static inline void fthAdjustClock(uint64_t now)
{
   // Time to adjust the tsc ticks per nanosecond
    struct timespec nowSpec;
    uint64_t elapsedMicro;

    clock_gettime(CLOCK_REALTIME, &nowSpec);

    elapsedMicro = (nowSpec.tv_nsec - sched->prevTimeSpec.tv_nsec) / 1000 +
           (nowSpec.tv_sec - sched->prevTimeSpec.tv_sec) * 1000000;

    // Take the average of this value and the current guess so time doesn't jump
    fthTscTicksPerMicro += (now - sched->prevTimeTsc) / elapsedMicro;
    fthTscTicksPerMicro /= 2;

    // Save the new values for next time
    sched->prevTimeTsc = now;
    sched->prevTimeSpec.tv_sec = nowSpec.tv_sec;
    sched->prevTimeSpec.tv_nsec = nowSpec.tv_nsec;
}

static void fthProcessQueues(void)
{
    fthThread_t *cur;

    // Process the memory queue

    for (int qNum = 0; qNum < fth->memQCount; qNum++) {
        uint64_t rv;

        if (fth->memTest[qNum] != NULL && (rv = *(fth->memTest[qNum])) != 0) {
            cur = fth->memQ[qNum];
            fth->memTest[qNum] = NULL;
            cur->dispatch.arg = rv;

            if (cur->dispatch.floating) {
                    plat_assert_always(cur == floatingThread);
                plat_assert_always(floatingQueue == NULL);
                floatingQueue = cur;
            } else {
                fthThreadQ_push(&fth->eligibleQ[0], cur);
            }
        }
    }

    // Process the mutex queue

    if (fth->mutexQ.head != NULL) {
        fthThreadQ_spinLock(&fth->mutexQ);
        cur = fthThreadQ_head(&fth->mutexQ);
    
        while (PLAT_UNLIKELY(cur != NULL)) {
           if (pthread_mutex_trylock(cur->mutexWait) != 0) {
               fthThread_t *dispatchable = cur;
               cur = fthThreadQ_next(cur);
               fthThreadQ_remove_nospin(&fth->mutexQ, dispatchable);
    
               if (dispatchable->dispatch.floating) {
                   plat_assert_always(dispatchable == floatingThread);
                   plat_assert_always(floatingQueue == NULL);
                   floatingQueue = dispatchable;
               } else {
                   fthThreadQ_push(&fth->eligibleQ[dispatchable->schedNum],
                                   dispatchable);
               }
           } else {                  // Not yet
               cur = fthThreadQ_next(cur);
           }
        }
    
        fthThreadQ_spinUnlock(&fth->mutexQ); // Done walking the queue
    }

    // Check to see whether it is time to wake up sleeping threads

    if (fth->sleepQ.head != NULL && FTH_SPIN_TRY(&fth->sleepSpin)) {
        uint64_t now = rdtsc(); // Refresh for accuracy

        if (PLAT_UNLIKELY((now - sched->prevTimeTsc) > 10000000000)) {
            fthAdjustClock(now);
        }

        fthThread_t *st = fth->sleepQ.head;

        while (st != NULL && st->sleep < now) {
            st = fthThreadQ_shift_precheck(&fth->sleepQ); // Must be the head
            fthResumeInternal(st, 0);
            st = fth->sleepQ.head;
        }

        FTH_SPIN_UNLOCK(&fth->sleepSpin);
    }

    //
    // Check the PtofMbox global chain for new mail.
    // Mail always is pushed on this chain and then dequeued by the scheduler.
    // This approach avoids a bunch of cross-thread race conditions.

    while (1) {
        XMboxEl_sp_t elShmem =
            XMboxEl_xlist_dequeue(&fth->ptofMboxPtr->headShmem,
                                  &fth->ptofMboxPtr->tailShmem);

        if (PLAT_LIKELY(XMboxEl_sp_is_null(elShmem))) {
            break;                // Done for now
        }

        // Something chained off head - post the head
        XMboxEl_t *el = XMboxEl_sp_rwref(&el, elShmem);
        ptofMbox_t *mb = ptofMbox_sp_rwref(&mb, el->ptofMboxShmem);

        // Must lock the mailbox to avoid a race where we enqueue just
        // as a waiter arrives

        FTH_SPIN_LOCK(&mb->spin);
        fthThread_t *mbWaiter = fthThreadQ_shift_precheck(&mb->threadQ);

        if (mbWaiter == NULL) {   // If no thread waiting
            // Just queue up the mail
            XMboxEl_xlist_enqueue(&mb->mailHeadShmem, &mb->mailTailShmem,
                                  elShmem);
        } else {                  // Start the top waiting thread
            fthResumeInternal(mbWaiter, el->mailShmem.base.int_base);
            // Release the old reference and the XMbox element
            XMboxEl_sp_rwrelease(&el); // Release the old one
            XMboxEl_sp_free(elShmem);
        }

        __sync_fetch_and_add(&mb->pending, -1); // Decrement pending count
        FTH_SPIN_UNLOCK(&mb->spin);

        asm __volatile__("mfence":::"memory"); // Make sure all is seen
        ptofMbox_sp_rwrelease(&mb); // Done with reference
    }

    //
    // Check the PtoF dispatch queue.  Threads get placed on this queue
    // when they are waiting for an asynchronous event or when a
    // PThread needs to post the thread.  This avoids a bunch of
    // cross-thread race conditions.

    while (1) {
        fthThread_t *thread =
            XResume_xlist_dequeue(&ptofResumePtrs.head,
                                  &ptofResumePtrs.tail);

        if (PLAT_LIKELY(thread == NULL)) {
            break;
        }

        // This is like resume but the arg is already in thread->arg
        FTH_SPIN_LOCK(&thread->spin);
        plat_assert(thread->dispatchable == 0);
        plat_assert(thread->state == 'd');

        if (thread->dispatch.floating) {
            plat_assert_always(thread == floatingThread);
            plat_assert_always(floatingQueue == NULL);
            floatingQueue = thread;
        } else {
            fthThreadQ_push(&fth->eligibleQ[thread->schedNum], thread);
        }

        thread->dispatchable = 1; // Mark as on dispatch queue
        thread->state = 'D';      // Now dispatchable
        FTH_SPIN_UNLOCK(&thread->spin);
    }
}

static inline void fthKilled(fthThread_t *cur)
{
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
    __sync_bool_compare_and_swap(&fth->allTail, cur, prev);

    // Now swap out the head or previous next pointer.  This is delayed until
    // after the tail is changed in case (a) cur was the tail and (b) a new
    // one got chained on just before we swapped out the cur.
    if (prev == NULL) {
        __sync_bool_compare_and_swap(&fth->allHead, cur, cur->nextAll);
    } else {
        prev->nextAll = cur->nextAll; // Splice it out
    }

    FTH_SPIN_UNLOCK(&fth->allQSpin);
    FTH_FREE(cur, sizeof(fthThread_t));
}

/*
 *  Look for a low-priority thread to schedule.
 */

static fthThread_t *fthProcessLowPrio(void)
{
    fthThread_t *cur;

    while (1) {
        cur = myLowPrioQ->head;

        if (PLAT_UNLIKELY(cur == NULL)) {
            break;
        }

        // Make sure that the thread isn't just about to be suspended
        if (PLAT_UNLIKELY(cur->dispatch.sched != NULL)) {
            ;
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
#ifdef FTH_TIME_STATS
        // Remember all but the last time around loop
        dispatchTimeStamp = rdtsc();
#endif
    }

    return cur;
}

/**
 * @brief Scheduler main
 *
 */

void fthScheduler(uint64_t arg)
{
    int schedNum = sched->schedNum;
    fthSched_t *sched = (fthSched_t *) arg;  // Type cheat for parameter passing

#ifdef FTH_TIME_STATS
    threadSuspendTimeStamp = rdtsc();      // Init
#endif

    asm __volatile__("":::"memory");       // Things could have changed
    fthSaveSchedulerContext(sched);        // Save this point for fthToScheduler
    asm __volatile__("mfence":::"memory"); // Things could have changed

    // This is like Bill Murray in "Ground Hog Day": the scheduler finds itself
    // here every time it wakes up.

#ifdef FTH_TIME_STATS
    uint64_t idleStartTimeStamp;           // Idle start (0 for not idle)
    uint64_t schedStartTimeStamp;          // Loop start

    idleStartTimeStamp = 0;
    schedStartTimeStamp = rdtsc();

    if (sched->prevDispatchPrio) {
        sched->schedulerDispatchTime += schedStartTimeStamp -
            threadSuspendTimeStamp;
    } else {
        sched->schedulerLowPrioDispatchTime += schedStartTimeStamp -
            threadSuspendTimeStamp;
    }

    if (schedStartTimeStamp - threadSuspendTimeStamp < minFromDispatch) {
        minFromDispatch = schedStartTimeStamp - threadSuspendTimeStamp;
    }

    if (schedStartTimeStamp - threadSuspendTimeStamp > maxFromDispatch) {
        maxFromDispatch = schedStartTimeStamp - threadSuspendTimeStamp;
    }
#endif

    if (PLAT_UNLIKELY(fthDebug)) {
        printf("Back to scheduler %p\n", sched); fflush(NULL);
    }

    fthThread_t *cur = sched->dispatch.thread;
    sched->dispatch.thread = NULL;

    if (PLAT_LIKELY(cur != NULL)) {          // If just finished dispatch
        plat_assert(cur->dispatch.floating || cur->dispatch.sched == sched);
        cur->dispatch.sched = NULL;          // No longer running

        if (PLAT_UNLIKELY(cur->state == 'K')) { // if killed
            fthKilled(cur);
        }
    }

    /*
     *  This is the start of the main scheduler loop.  The loop continues until
     *  a thread is dispatched.  Once a thread finishes its time slice, the
     *  scheduler is re-entered above.
     */

    cur = NULL; /* no thread to dispatch yet */

    while (1) {
#ifdef FTH_TIME_STATS
        dispatchTimeStamp = rdtsc();
#endif

        if (curSchedNum == POLLER_SCHED) {
            fthProcessQueues();
        }

        // Check for doing a dispatch of a floating (global) thread.

        if (previousFloating != schedNum && floatingQueue != NULL) {
            cur = atomic_xchgp(&floatingQueue, NULL);

            if (cur == NULL) {
                ;
            } else if (cur->dispatch.sched != NULL) {
                floatingQueue = cur;
                cur = NULL;
            } else {
                plat_assert_always(cur == floatingThread);
                plat_assert_always(floatingQueue == NULL);
                previousFloating = schedNum;
                nextFloating = schedNum + 1;

                if (nextFloating > fthFloatMax) {
                    nextFloating = 0;
                }

                fthFloatStats[schedNum]++;
            }
        }

        // Check for an eligible thread

        while (cur == NULL) {
            cur = myEligibleQ->head;

            if (PLAT_UNLIKELY(cur == NULL)) {
                break;
            }

            // Make sure that the thread isn't just about to be suspended
            if (PLAT_UNLIKELY(cur->dispatch.sched != NULL)) {
                ;
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
            cur = NULL;
#ifdef FTH_TIME_STATS
            dispatchTimeStamp = rdtsc();
#endif
        }

#ifdef FTH_TIME_STATS
        if (PLAT_UNLIKELY(cur == NULL && !idleStartTimeStamp)) {
            idleStartTimeStamp = schedStartTimeStamp;
        }
#endif

#ifdef multi_low
        if (PLAT_UNLIKELY(cur == NULL)) {
            cur = fthProcessLowPrio();
        }
#else
        if (PLAT_UNLIKELY(cur == NULL) && schedNum == LOW_PRIO_SCHED) {
            cur = fthProcessLowPrio();
        }
#endif

        /*
         *  If a thread was found, run it!
         */

        if (PLAT_LIKELY(cur != NULL)) {
            fthRun(cur, idleStartTimeStamp, schedStartTimeStamp);
            /* fthRun does not return */
        }

        /*
         *  Shut down this scheduler thread entirely if that's been requested.
         */

        if (fth->kill && __sync_fetch_and_sub(&fth->kill, 1) > 0) {
            plat_shmem_pthread_done();
            return;
        }

        // Release malloc waiters in case we have free memory now
        while (1) {
            fthThread_t *thread = fthThreadQ_shift(&fth->mallocWaitThreads);

            if (thread == NULL) {
                break;
            }

            fthResumeInternal(thread, 0); // Restart
        }
    }
}

/**
 * @brief Give up processor, go undispatchable.
 *
 * @return Value specified by caller to fthResume.
 */
uint64_t fthWait(void) {
    fthThread_t *thread = fthSelf();
    // The thread could be taken off of whatever wait Q that it is currently on
    // (i.e., mbox wait) and put on the dispatch Q at this point.  So, the
    // state may have been changed from 'R' to 'd' or even 'D'.  Or maybe the
    // caller has set a special state.  Either way, we only replace the 'R'
    // state.

    __sync_bool_compare_and_swap(&thread->state, 'R', 'W');

#ifdef FTH_TIME_STATS
    threadSuspendTimeStamp = rdtsc();
    thread->runTime += threadSuspendTimeStamp - threadResumeTimeStamp;
    sched->totalThreadRunTime += threadSuspendTimeStamp - threadResumeTimeStamp;
#endif
    sched->prevDispatchPrio = 1;             // High prio if waiting
    fthToScheduler(thread, 0);                  // Go away
    thread->state = 'R';                     // Running again

#ifdef FTH_TIME_STATS
    threadResumeTimeStamp = rdtsc();
    sched->schedulerDispatchTime += threadResumeTimeStamp - dispatchTimeStamp;
#endif

    return thread->dispatch.arg;
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

        if (! thread->dispatch.floating) {
            fthThreadQ_push(&fth->eligibleQ[thread->schedNum], thread);
        }
    } else {
        sched->prevDispatchPrio = 0;
        thread->yieldCount = -1 - count;

        if (! thread->dispatch.floating) {
#ifdef multi_low
           fthThreadQ_push(&fth->lowPrioQ[thread->schedNum], thread);
#else
           fthThreadQ_push(&fth->lowPrioQ, thread);
#endif
        }
    }

    thread->dispatchable = 1;                // Mark as dispatchable
    FTH_SPIN_UNLOCK(&thread->spin);

    fthToScheduler(thread, 1);               // Go away
    thread->state = 'R';                     // Running again

#ifdef FTH_TIME_STATS
    threadResumeTimeStamp = rdtsc();

    if (count >= 0) {
        sched->schedulerDispatchTime +=
            threadResumeTimeStamp - dispatchTimeStamp;
    } else {
        sched->schedulerLowPrioDispatchTime +=
            threadResumeTimeStamp - dispatchTimeStamp;
    }
#endif
}

/**
 * @brief Wait element allocation.
 *
 * @return free wait element which caller must return with call to fthFreeWaitEl
 */
fthWaitEl_t *fthGetWaitEl(void) {
    fthWaitEl_t *rv = NULL;

    if (sched != NULL) {
        rv = fthWaitQ_shift_nospin(&sched->freeWait);
    }

    if (rv != NULL) {                        // Check whether we got one
        sched->freeWaitCount--;
    } else {
        rv = fthWaitQ_shift_precheck(&fth->freeWait); // Try the global queue
    }

    while (rv == NULL) {                     // Check whether we got one
        rv = FTH_MALLOC(sizeof(fthWaitEl_t)); // Allocate a new one

        if (rv != NULL) {                    // If malloc worked
            rv->pool = 1;                    // This is a pool element
            rv->list = fth->waitEls;
            fth->waitEls = rv;
            break;
        }

        // Malloc failed - wait
        fthThreadQ_push(&fth->mallocWaitThreads, fthSelf());
        fthWait();                     // Sleep for a while
    }

    fthWaitQ_el_init(rv);                    // Init the linked list element
    rv->thread = (void *) fthLockID();       // Remember the thread or PID
    rv->caller = (void *) __builtin_return_address(1);
    return rv;
}

/**
 * @brief Wait element release
 *
 * @param waitEl <IN> Wait element previously returned by fthGetWaitEl
 */
void fthFreeWaitEl(fthWaitEl_t *waitEl) {
    plat_assert(waitEl->pool == 1);
    waitEl->caller = NULL;

    if (sched != NULL && sched->freeWaitCount < SCHED_MAX_FREE_WAIT) {
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
fthSparseLock_t *fthGetSparseLock(void) {
    fthSparseLock_t *rv = fthSparseQ_shift_nospin(&sched->freeSparse);

    if (rv != NULL) {                        // Check whether we got one
        sched->freeSparseCount--;
    } else {
        rv = fthSparseQ_shift(&fth->freeSparse); // Try the global queue
    }

    if (rv == NULL) {                        // Check whether we got one
        rv = FTH_MALLOC(sizeof(fthSparseLock_t)); // Allocate
    }

    fthSparseQ_el_init(rv);
    fthLockInit(&rv->lock);                  // Init the embedded FTH lock
    rv->useCount = 0;
    return rv;
}

/**
 * @brief sparse lock release
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

    // Insert into the sleep Q at the appropriate place
    FTH_SPIN_LOCK(&fth->sleepSpin);
    fthThread_t *st = fth->sleepQ.head;
    fthThread_t *prevST = NULL;

    while (st != NULL && st->sleep < self->sleep) {
        prevST = st;
        st = st->threadQ.next;
    }

    fthThreadQ_insert_nospin(&fth->sleepQ, prevST, self);
    FTH_SPIN_UNLOCK(&fth->sleepSpin);
    fthWait();
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
    tv->tv_usec  = sched->prevTimeSpec.tv_nsec / 1000;
    tv->tv_usec += (rdtsc() - sched->prevTimeTsc) / fthTscTicksPerMicro;
    tv->tv_sec   = sched->prevTimeSpec.tv_sec;

    if (tv->tv_usec >= 1000000) {
        tv->tv_sec  += (int) tv->tv_usec / 1000000;
        tv->tv_usec %= 100000;
    }
}

/**
 * @brief get an estimate of the current time in seconds
 *
 * Returns just the time in seconds like time
 *
 */
uint64_t fthTime(void) {
    uint64_t usec;
    uint64_t sec;

    usec  = sched->prevTimeSpec.tv_nsec / 1000;
    usec += (rdtsc() - sched->prevTimeTsc) / fthTscTicksPerMicro;
    sec   = sched->prevTimeSpec.tv_sec;

    if (usec >= 1000000) {
        sec += (int) usec / 1000000;
    }

    return sec;
}

/**
 * @brief get total run time for this thread
 *
 * @param thread <IN> thread pointer or null (self)
 *
 * @return - thread run time as of last wait (does not include current time
 *           quanta)
 *
 */
uint64_t fthThreadRunTime(fthThread_t *thread) {
    if (thread == NULL) {
        thread = fthSelf();
    }

    return thread->runTime / fthTscTicksPerMicro;
}

/**
 * @brief Return accumulated FTH thread run time in microseconds
 *
 * @return accumulated FTH thread run time
 */
uint64_t fthGetTotalThreadRunTime(void) {
    uint64_t fthTotalThreadRunTime = 0;

    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (fth->scheds[i] != NULL) {
            fthTotalThreadRunTime += fth->scheds[i]->totalThreadRunTime;
        }
    }

    return fthTotalThreadRunTime / fthTscTicksPerMicro;
}

/**
 * @brief Return accumulated FTH scheduler dispatch time in microseconds
 *
 * @return accumulated FTH dispatch time
 */
uint64_t fthGetSchedulerDispatchTime(void) {
    uint64_t fthSchedulerDispatchTime = 0;

    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (fth->scheds[i] != NULL) {
            fthSchedulerDispatchTime += fth->scheds[i]->schedulerDispatchTime;
        }
    }

    return fthSchedulerDispatchTime / fthTscTicksPerMicro;
}

/**
 * @brief Return accumulated FTH scheduler low prio dispatch time in
 *        microseconds
 *
 * @return accumulated low prio FTH dispatch time
 */
uint64_t fthGetSchedulerLowPrioDispatchTime(void) {
    uint64_t fthSchedulerLowPrioDispatchTime = 0;

    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (fth->scheds[i] != NULL) {
            fthSchedulerLowPrioDispatchTime +=
                fth->scheds[i]->schedulerLowPrioDispatchTime;
        }
    }

    return fthSchedulerLowPrioDispatchTime / fthTscTicksPerMicro;
}

/**
 * @brief Return accumulated number of thread dispatches
 *
 * @return Total number of times threads were dispatched
 */

uint64_t fthGetSchedulerNumDispatches(void) {
    uint64_t fthSchedulerNumDispatches = 0;

    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (fth->scheds[i] != NULL) {
            fthSchedulerNumDispatches += fth->scheds[i]->schedulerNumDispatches;
        }
    }

    return max(fthSchedulerNumDispatches, 1);
}

/**
 * @brief Return accumulated number of low-prio thread dispathes
 *
 * @return Total number of times low-prio threads were dispatched
 */
uint64_t fthGetSchedulerNumLowPrioDispatches(void) {
    uint64_t fthSchedulerNumLowPrioDispatches = 0;

    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (fth->scheds[i] != NULL) {
            fthSchedulerNumLowPrioDispatches +=
                fth->scheds[i]->schedulerNumLowPrioDispatches;
        }
    }

    return fthSchedulerNumLowPrioDispatches;
}

/**
 * @brief Return average dispatch time in nanoseconds
 *
 * @return Dispatch time divided by number of dispatches times tsc ticks per
 *         nanosecond
 */
uint64_t fthGetSchedulerAvgDispatchNanosec(void) {
    return fthGetSchedulerDispatchTime() * 1000 /
               fthGetSchedulerNumDispatches() / fthTscTicksPerMicro;
}

/**
 * @brief Return accumulated FTH scheduler idle time in microseconds
 *
 * @return idle time
 */
uint64_t fthGetSchedulerIdleTime(void) {
    uint64_t fthSchedulerIdleTime = 0;

    for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
        if (fth->scheds[i] != NULL) {
            fthSchedulerIdleTime += fth->scheds[i]->schedulerIdleTime;
        }
    }

    return fthSchedulerIdleTime / fthTscTicksPerMicro;
}

/**
 * @brief Return voluntary context switches
 *
 * @return context switches
 */
uint64_t fthGetVoluntarySwitchCount(void) {
    struct rusage usage;

    getrusage(RUSAGE_SELF, &usage);
    return usage.ru_nvcsw;
}

/**
 * @brief Return involuntary context switches
 *
 * @return context switches
 */
uint64_t fthGetInvoluntarySwitchCount(void) {
    struct rusage usage;

    getrusage(RUSAGE_SELF, &usage);
    return usage.ru_nivcsw;
}

/**
 * @brief Return the estimated tsc ticks per microsecond
 *
 * @return tstTicksPerMicro
 */
uint64_t fthGetTscTicksPerMicro(void) {
    return fthTscTicksPerMicro;
}

long fthGetDefaultStackSize(void) {
    // 3 * 4096 proves to be insufficient for vfprintf in some cases.
    return 4096 * 5;
}

#ifdef FTH_INSTR_LOCK

/**
 * @brief Get a pointer to the per-scheduler lock tracing data structure
 *
 * @return Pointer to lock trace data for this scheduler
 */
fthLockTraceData_t *fthGetLockTraceData(void)
{
    if (sched == NULL) {
        return NULL;
    }

    return sched->locktrace_data;
}

#endif
