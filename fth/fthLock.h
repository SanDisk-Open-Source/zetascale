/*
 * File:   fthLock.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthLock.h 396 2008-02-29 22:55:43Z jim $
 */

//
// Schooner locks for FTH-type threads
//

#include "platform/defs.h"
#include "fthSpinLock.h"
#include "fthWaitEl.h"

#ifndef _FTH_LOCK_H
#define _FTH_LOCK_H

#ifdef FTH_INSTR_LOCK
    /* stuff for tracing lock wait times */

#define FTH_INSTR_LOCK_BT_SIZE       4
    /* number of lock trace events to buffer before dumping them to a file */
// #define FTH_INSTR_LOCK_TRACE_LEN     1000000
#define FTH_INSTR_LOCK_TRACE_LEN     100000
// #define FTH_INSTR_LOCK_TRACE_LEN     1000

    /* maximum trace record size */
#define FTH_INSTR_LOCK_MAX_REC_SIZE    (32 + (8*FTH_INSTR_LOCK_BT_SIZE))

    /* lock trace record types */
#define FTH_INSTR_LOCK_END_REC             0
#define FTH_INSTR_LOCK_LOCK_RD_REC         1
#define FTH_INSTR_LOCK_LOCK_WR_REC         2
#define FTH_INSTR_LOCK_N_RECS              3

    /* lock trace structure that is kept per scheduler */
typedef struct fthLockTraceData {
    uint32_t      schedNum;       // ID of my scheduler
    uint64_t      n_trace_recs;   // Total number of trace records for this scheduler
    uint64_t      n_lock_trace;   // Next free entry in lock_trace[]
    uint64_t      lock_trace[FTH_INSTR_LOCK_TRACE_LEN]; // lock trace buffer
} fthLockTraceData_t;

#endif // FTH_INSTR_LOCK

/** @brief The lock data structure for sleep-type read-write locks */
typedef struct fthLock {
    fthSpinLock_t spin;                      // Protects the more complex lock
    int writeLock;                           // Count of write locks held (0 or 1)
    int readLock;                            // Count of read locks held
    void *writer;                            // thread holding the write lock

    fthWaitQ_lll_t waitQ;                    // WaitQ elements of threads waiting for lock
    fthWaitQ_lll_t holdQ;                    // WaitQ elements of threads holding the lock
    const char * name;                       // file and line number, for now
    const char * func;                       // __PRETTY_FUNCTION__
} fthLock_t;

#ifdef FTH_INSTR_LOCK

extern fthLockTraceData_t *fthGetLockTraceData();
extern void fthDumpLockTrace(fthLockTraceData_t *pltd);

#endif // FTH_INSTR_LOCK

__BEGIN_DECLS
// Routines - must be here becuase of wait El definition
/**
 * @brief Initialize lock
 * @param lock <OUT> lock to initialize
 */
void fthLockInitName(struct fthLock *lock, const char *name, const char *f);

#define fth__s(s) #s
#define fth_s(s) fth__s(s)

#define fthLockInit(lock) \
	  fthLockInitName((lock), __FILE__ ":" fth_s(__LINE__), __PRETTY_FUNCTION__)

/**
 * @brief Lock.
 *
 * For fth<->fth locking.  Use #fthXLock/#pthreadXLock or #fthMutexLock for
 * pthread<->fth locking.
 *
 * @param lock <IN> lock to acquire
 * @param write <IN> 0 for read lock, non-zero for write
 * @return fthWaitEl_t * to pass to fthUnlock.
 */
fthWaitEl_t *fthLock(struct fthLock *lock, int write, fthWaitEl_t *waitEl);
fthWaitEl_t *fthTryLock(struct fthLock *lock, int write, fthWaitEl_t *waitEl);

/**
 * @brief Release lock
 *
 * @param waitEl <IN> waiter returned by fthLock or fthTryLock.
 */ 
void fthUnlock(fthWaitEl_t *waitEl);
void fthDemoteLock(fthWaitEl_t *lockEl);
__END_DECLS

#endif
