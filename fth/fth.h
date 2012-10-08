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
#include <pthread.h>

#include "platform/defs.h"
#include "platform/types.h"
#include "platform/stdlib.h"

#define FTH_TIME_STATS                       // Collect time stats
#define FTH_TIME_MIN_MAX

#include "fthSpinLock.h"
#include "fthLock.h"

typedef struct fthThread {
    uint32_t          id;
    pthread_t         pthread;
    void            (*startfn)(uint64_t arg);
    pthread_mutex_t   mutex;
    pthread_cond_t    condvar;
    uint64_t          rv_wait;
    uint32_t          is_waiting;
    uint32_t          do_resume;
    struct fthThread *next;
    struct fthThread *prev;

    /*  For Drew's screwy per-fthread local state used by
     *  platform/attr.[ch] and his replication code.
     */
    struct plat_attr_uthread_specific *local; // Uthread local storage

} fthThread_t;

extern uint64_t fthTscTicksPerMicro;
extern __thread int curSchedNum;
extern __thread int totalScheds;

enum {
    FTH_TIME_CALIBRATION_MICROS = 100 * PLAT_THOUSAND
};

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

#define FTH_MAX_SCHEDS    1

// @brief CPU/cache configuration used for cache/numa affinity
struct fthCpuInfo {
    cpu_set_t CPUs;                          // Cpus to use 
    int numCPUs;                             // Total number of CPUs

    int schedToCPU[FTH_MAX_SCHEDS];          // Scheduler to CPU map
};

// Global threading (scheduler) structure
typedef struct fth {
    struct fthConfig   config;                // Frozen configuration
    struct fthCpuInfo  cpuInfo;               // CPU information for init
    struct fthThread  *allHead, *allTail;     // Chain of all threads for debug
    uint32_t           nthrds;

    /*  These are used to keep new fthread pthreads from running 
     *  until at least one scheduler is running.
     */

    uint32_t           sched_started;
    uint32_t           sched_thrds_waiting;
    pthread_mutex_t    sched_mutex;
    pthread_cond_t     sched_condvar;

    /*  These are used to synchronize the killing of fthreads
     *  and their schedulers.
     */

    uint32_t           kill;
    pthread_mutex_t    kill_mutex;
    pthread_cond_t     kill_condvar;
} fth_t;

extern int schedNum;
extern fth_t *fth;

// Add a common routine to get the hardware time-stamp counter
static __inline__ uint64_t rdtsc(void)
{
    uint32_t u, l;
    asm volatile("rdtsc" : "=a" (l), "=d" (u)); 
    return (((uint64_t) u << 32) | l); 
}


// Routines

void fthInitMultiQ(int numArgs, ...);
#define fthInit()  fthInitMultiQ(0)

void fthSchedulerPthread(int prio);
void fthKill(int kill);
inline fth_t *fthBase(void);
inline fthThread_t *fthSelf(void);

fthThread_t *fthSpawn(void (*startRoutine)(uint64_t), long minStackSize);
fthThread_t *fthSpawnPthread();
void fthResume(fthThread_t *thread, uint64_t rv);
void XResume(struct fthThread *thread, uint64_t arg);
uint64_t fthWait(void);
void fthYield(int count);
void *fthId(void);
uint64_t fth_uid(void);

void fthNanoSleep(uint64_t nanoSecs);

void fthScheduler(uint64_t sched);
fthThread_t *fthMakeFloating(fthThread_t *);

extern uint64_t fthFloatStats[FTH_MAX_SCHEDS];
extern uint64_t fthFloatMax;
extern uint64_t fthReverses;

void fthGetTimeOfDay(struct timeval *tv);
/**
 * @brief Return purely monotonic time.
 *
 * Note that this will not advance when gettimeofday() is going backwards.
 */
// void fthGetTimeMonotonic(struct timeval *tv);
// uint64_t fthTime(void);
    
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
// long fthGetDefaultStackSize();

#endif
