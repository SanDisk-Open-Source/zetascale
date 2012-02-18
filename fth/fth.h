/*
 * File:   fth.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fth.h 396 2008-02-29 22:55:43Z jim $
 */

//
//  Featherweight threading structures
//

#ifndef _FTH_H
#define _FTH_H

#include <sys/time.h>
#include <sched.h>
#include <time.h>
#include <stdio.h>

#include "platform/defs.h"
#include "platform/types.h"
#include "platform/stdlib.h"

#define FTH_TIME_STATS                       // Collect time stats
#define FTH_TIME_MIN_MAX

#include "fthSpinLock.h"
#include "fthIdleControl.h"
#include "fthLock.h"
#include "fthWaitEl.h"
#include "fthSchedType.h"
#include "fthSparseLock.h"
#include "fthThreadQ.h"
#include "fthMem.h"
#include "fthThread.h"
#include "sdfappcommon/XList.h"

XLIST_H(allThreads, fthThread, nextAll);

#include "sdfappcommon/XMbox.h"
#include "sdfappcommon/XResume.h"

#ifndef __x86_64__
#error "Only works with x86 64-bit processors - may need to cross compile"
#endif

extern __thread uint64_t fthTscTicksPerMicro;

extern __thread int curSchedNum;

enum {
    FTH_TIME_CALIBRATION_MICROS = 100 * PLAT_THOUSAND
};

// #define FTH_IDLE_CONTROL                  // Idle control of pthread (unused)

#define FTH_MALLOC(size) plat_alloc_arena(size, PLAT_SHMEM_ARENA_FTH)
#define FTH_FREE(ptr, size) plat_free(ptr)

#define FTH_STACK_ALIGNMENT 4096
#define FTH_STACK_ALIGN(size) ((size + FTH_STACK_ALIGNMENT) & (- FTH_STACK_ALIGNMENT))

/* item(caps, lower, description) */
#define FTH_IDLE_MODE_ITEMS() \
    item(SPIN, spin, "spin loop on inbound queues")                          \
    item(CONDVAR, condvar, "block on pthread condition variable")

enum fthIdleMode {
#define item(caps, lower, description) FTH_IDLE_ ## caps,
    FTH_IDLE_MODE_ITEMS()
#undef item
};

enum fthAffinityMode {
    FTH_AFFINITY_DEFAULT,
    FTH_AFFINITY_PER_THREAD
};

// @brief Clock time source
enum fthClockMode {
    // @brief Real time
    FTH_CLOCK_REAL,
    /* 
     * @brief elapsed CPU time
     *
     * It's painful to debug real time problems like replication lease 
     * issues when the clock advances but the debugger is stopped.  Allow
     * fth to get its time base from ellapsed CPU time to work around that.
     */
    FTH_CLOCK_FAKE
};

struct fthConfig {
    // @brief Idle behavior
    enum fthIdleMode idleMode;

    // @brief interval to check queues when busy
    int busyPollInterval; 

    // @brief how to interpret affinityCores
    enum fthAffinityMode affinityMode;

    // @brief list of CPUs to use for thread affinity
    cpu_set_t affinityCores;

    // @brief which time source to use
    enum fthClockMode clockMode;;
};

extern struct fthConfig fthConfig;

enum {
    FTH_CPU_INVALID = -1                     // No valid CPU assigned
};

// @brief CPU/cache configuration used for cache/numa affinity
struct fthCpuInfo {
    cpu_set_t CPUs;                          // Cpus to use 
    int numCPUs;                             // Total number of CPUs

    int schedToCPU[FTH_MAX_SCHEDS];          // Scheduler to CPU map
};

// Global threading (scheduler) structure
typedef struct fth {
    struct fthConfig config;                 // Frozen configuration

    struct fthCpuInfo cpuInfo;               // CPU information for init

    volatile int kill;                       // Set when FTH should go away

    struct fthSched *scheds[FTH_MAX_SCHEDS];    //  all schedulers accessible from fth.

    int totalScheds;                    /* Total exptected number of fth schedulers
                                           for this process. The needs to be set in 
                                           the fthInitMultiQ() call */
                                           
    fthThreadQ_lll_t eligibleQ[FTH_MAX_SCHEDS];   // Eligible thread queues
#ifdef multi_low
    fthThreadQ_lll_t lowPrioQ[FTH_MAX_SCHEDS];    // Lw priority eligbile Q
#else
    fthThreadQ_lll_t lowPrioQ;               // Lw priority eligbile Q
#endif
    fthSpinLock_t sleepSpin;                 // Lock the sleep Q for searching
    
  //    fthThreadQ_lll_t memQ;                   // Actual sleep Q

    uint32_t memQCount;                      // Number of mem queues to check
    fthThread_t *memQ[FTH_MEMQ_MAX+1];       // Mem wait thread queue    
    uint64_t *memTest[FTH_MEMQ_MAX+1];       // Mem wait test
    
    fthThreadQ_lll_t sleepQ;                 // Actual sleep Q
    fthThreadQ_lll_t mutexQ;                 // Mutex wait thread queue
    ptofMboxPtrs_t *ptofMboxPtr;             // Xlist head, tail ptrs for ptof Mbox
    struct fthThread *allHead, *allTail;     // Chain of all threads for debug
    fthSpinLock_t allQSpin;                  // CD logic to add, spin to delete

    fthWaitQ_lll_t freeWait;                 // Free wait list elements
#ifndef TEMP_WAIT_EL        
    struct fthWaitEl *waitEls;
#endif    
    fthSparseQ_lll_t freeSparse;             // Free sparse locks
    
    fthThreadQ_lll_t mallocWaitThreads;      // Out-of-space waiters
    
#ifdef FLASH_SIM_LATENCY
    flashDev_t *dev;
    uint64_t nextSimCompletion;
#endif

#ifdef FTH_IDLE_CONTROL    
    struct fthIdleControl *idleControlPtr;   // Hook for idle behavior
#endif
} fth_t;

//
// Mailbox (threads consume data)
struct fthMailbox {
    fthThreadQ_lll_t waitQ;                  // Waiting threads
    fthWaitQ_lll_t dataQ;                    // Data waiting to serve
} fthMailbox_t;

extern int schedNum;
extern fth_t *fth;

// Add a common routine to get the hardware time-stamp counter
static __inline__ uint64_t rdtsc(void)
{
    uint32_t u, l;
    asm volatile("rdtsc" : "=a" (l), "=d" (u)); 
    return (((uint64_t) u << 32) | l); 
}


//  is misaligned stack size for spawn  by
// the FTH_HINT_X tells the multiq scheduler 
// something about where the spawn is coming from
#define  FTH_HINT_FLASH 1


#ifdef AIO_TRACE
    /* stuff for tracing aio access times */

#define AIO_TRACE_WRITE_FLAG            (1<<0)
#define AIO_TRACE_SCHED_MISMATCH_FLAG   (1<<1)
#define AIO_TRACE_ERR_FLAG              (1<<2)

typedef struct aio_trace_rec {
    uint64_t       t_start;
    uint64_t       t_end;
    uint64_t       fth;
    int            fd;
    uint32_t       size;
    uint64_t       submitted:32;
    uint64_t       flags:8;
    uint64_t       nsched:8;
    uint64_t       spare:16;
} aio_trace_rec_t;

    /* number of aio trace events to buffer before dumping them to a file */
#define FTH_AIO_TRACE_LEN     100000
// #define FTH_AIO_TRACE_LEN     1000

    /* aio trace structure that is kept per scheduler */
typedef struct aio_trace_sched_state {
    FILE              *f;
    uint32_t           schedNum;       // ID of my scheduler
    uint64_t           n_trace_recs;   // Total number of trace 
                                       // records for this scheduler
    uint64_t           n_aio_trace;    // Next free entry in aio_trace[]
    aio_trace_rec_t    aio_trace[FTH_AIO_TRACE_LEN]; // AIO trace buffer
} aio_trace_sched_state_t;

#endif // AIO_TRACE

// Routines

#ifdef AIO_TRACE
extern aio_trace_sched_state_t *fthGetAIOTraceData();
extern void fthLogAIOTraceRec(aio_trace_sched_state_t *patd, aio_trace_rec_t *ptr);
#endif // AIO_TRACE


void fthInitMultiQ(int numArgs, ...);
#define fthInit()  fthInitMultiQ(0)

void fthSchedulerPthread(int prio);
void fthKill(int kill);
void fthPthreadInit(void);
inline fthThread_t *fthSelf(void);
inline uint64_t fthLockID(void);
inline fth_t *fthBase(void);
inline struct ptofThreadPtrs *fthResumePtrs(void);
void
fthPrintSwitchStats(void);
#ifdef FTH_IDLE_CONTROL
inline struct fthIdleControl *fthIdleControlPtr(void);
#endif

fthThread_t *fthSpawn(void (*startRoutine)(uint64_t), long minStackSize);
void fthResume(fthThread_t *thread, uint64_t rv);
uint64_t fthWait(void);
void fthYield(int count);
void *fthId(void);
uint64_t fth_uid(void);

fthWaitEl_t *fthGetWaitEl(void);
void fthFreeWaitEl(fthWaitEl_t *waitEl);
fthSparseLock_t *fthGetSparseLock(void);
void fthFreeSparseLock(fthSparseLock_t *sl);
void fthNanoSleep(uint64_t nanoSecs);

void fthScheduler(uint64_t sched);
fthThread_t *fthMakeFloating(fthThread_t *);

#ifdef ENABLE_FTH_RCU
void fthRcuSchedInit (void);
void fthRcuUpdate (void);
void fthRcuDeferFree (void *x);
#endif

extern uint64_t fthFloatStats[FTH_MAX_SCHEDS];
extern uint64_t fthFloatMax;
extern uint64_t fthReverses;

void fthGetTimeOfDay(struct timeval *tv);
/**
 * @brief Return purely monotonic time.
 *
 * Note that this will not advance when gettimeofday() is going backwards.
 */
void fthGetTimeMonotonic(struct timeval *tv);
uint64_t fthTime(void);
    
uint64_t fthThreadRunTime(fthThread_t *thread);
uint64_t fthGetSchedulerNumDispatches(void);
uint64_t fthGetSchedulerNumLowPrioDispatches(void);
uint64_t fthGetSchedulerAvgDispatchNanosec(void);
uint64_t fthGetSchedulerIdleTime(void);
uint64_t fthGetTotalThreadRunTime(void);
uint64_t fthGetSchedulerDispatchTime(void);
uint64_t fthGetSchedulerLowPrioDispatchTime(void);
uint64_t fthGetVoluntarySwitchCount(void);
uint64_t fthGetInvoluntarySwitchCount(void);
uint64_t fthGetTscTicksPerMicro(void);

// @brief Return default stack size
long fthGetDefaultStackSize();

#endif
