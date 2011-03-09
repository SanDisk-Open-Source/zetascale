/*
 * File:   fthThread.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthThread.h 396 2008-02-29 22:55:43Z jim $
 */

// Resolve include order issue. fthThreadQ.h needs to have the fthThread 
// defined so that its inline definitions can be resolved.
#include "fthThreadQ.h"

#ifndef _FTH_THREAD_H
#define _FTH_THREAD_H

#include <setjmp.h>
#include <pthread.h>

#include "platform/types.h"

#include "fthSpinLock.h"
#include "fthSchedType.h"
#include "fthDispatch.h"

#include "sdfappcommon/XList.h"
#include "sdfappcommon/XResume.h"

//
// Per-thread structure
typedef struct fthThread {

    fthDispatchArea_t dispatch;              // ***ORDER***
    int32_t switchCount;                     // how many times schedulers got switched
    int32_t dispatchCount;                   // total number of times this fth got dispatched

    char dispatchable;                       // Set when on the dispatch queue

    union {
        uint64_t *memWait;                   // Location for mem wait
        pthread_mutex_t *mutexWait;          // Cross wait
        uint64_t sleep;                      // tsc ticks to sleep until
    };

#ifdef FTH_MEM_WAIT_TIMEOUT
    struct timeval memWaitStartTime;         // Time at which mem wait started
#endif

    fthSpinLock_t spin;                      // Lock on this thread struct
    //    int spinCount;                           // Count of spin locks held

    int yieldCount;                          // Count of times around eligible list
    int defaultYield;                        // Default value on yield

    uint32_t schedMask;                      // Mask of most recent dispatch
    int schedNum;                            // Number of current scheduler
    void * condVar;

#ifdef FTH_TIME_STATS
    uint64_t runTime;                        // Total run time for this thread
#endif    

    union {
        fthThreadQ_lll_el_t threadQ;         // Next, prev for LLL routines
        //fthThread_sp_t nextShmem;            // Next for SHMEM-based XLists
        struct fthThread *resumeNext;        // For cross-thread resumes
    };

    struct fthThread *nextAll;               // Chain of all FTH threads for debug

#ifndef TEMP_WAIT_EL    
    uint64_t waitElCount;
#endif    


    struct plat_attr_uthread_specific *local; // Uthread local storage

    char state;                              // Process state:
                                             //   "N" - New
                                             //   "M" - Mem wait (on dispatch queue)
                                             //   "X" - Mutex wait (on dipatch queue)
                                             //   "R" - Running
                                             //   "D" - Dispatchable - not currently running
                                             //   "d" - On XResume list pending "D"
                                             //   "K" - Killed
                                             //   "W" - Waiting
                                             //   "!" - Scheduler thread
                                             //   "#" - Dummy mail thread


    uint64_t id;                             // Unique id
} fthThread_t;

#endif
