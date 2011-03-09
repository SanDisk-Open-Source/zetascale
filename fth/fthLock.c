/*
 * File:   fthLock.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 */

/**
 * @brief FTH internal locks.
 */

#define FTH_SPIN_CALLER

#include <execinfo.h>
#include <stdio.h>
#include "fthSpinLock.h"
#include "fth.h"
#include "fthLock.h"

#ifdef FTH_INSTR_LOCK

    /* add 8B of lock trace data to the lock trace array */
#define FTH_INSTR_LOCK_LOG(pltd, x) {\
    pltd->lock_trace[pltd->n_lock_trace] = (uint64_t) x;\
    (pltd->n_lock_trace)++;\
    (pltd->n_trace_recs)++;\
}

    /* add an 8B end record to the lock trace array */
#define FTH_INSTR_LOCK_ENDREC(pltd) {\
    pltd->lock_trace[pltd->n_lock_trace] = (uint64_t) FTH_INSTR_LOCK_END_REC;\
    if (pltd->n_lock_trace >= (FTH_INSTR_LOCK_TRACE_LEN - (2*(FTH_INSTR_LOCK_MAX_REC_SIZE+1)))) {\
        fthDumpLockTrace(pltd);\
	pltd->n_lock_trace = 0;\
    }\
}

/**
 * @brief Dump lock trace data to a file.
 *
 * @param pltd - pointer to the lock trace data structure.
 */
void fthDumpLockTrace(fthLockTraceData_t *pltd)
{
    char        fname[20];
    FILE       *f;

    (void) sprintf(fname, "LT/lt_%d_%"PRIu64, pltd->schedNum, pltd->n_trace_recs);
    if ((f = fopen(fname, "w")) == NULL) {
        (void) fprintf(stderr, "ERROR: Could not open lock trace dump file '%s'", fname);
	return;
    }

    if (fwrite((const void *) pltd->lock_trace, sizeof(uint64_t), pltd->n_lock_trace, f) != pltd->n_lock_trace) {
        (void) fprintf(stderr, "ERROR: Problem dumping lock trace dump file '%s'", fname);
    }


    if (fclose(f) != 0) {
        (void) fprintf(stderr, "ERROR: Could not close lock trace dump file '%s'", fname);
    }
}

#endif // FTH_INSTR_LOCK

/**
 * @brief Init lock data structure.
 *
 * @param lock - fthLock pointer
 */
void fthLockInitName(fthLock_t *lock, const char *name, const char * f) {
    lock->readLock = 0;
    lock->writeLock = 0;

    fthWaitQ_lll_init(&lock->waitQ);
    fthWaitQ_lll_init(&lock->holdQ);
    FTH_SPIN_INIT(&lock->spin);
    lock->name = name;
    lock->func = f;
}

/**
 * @brief Release FTH lock
 *
 * @param lockEl <IN> fthWaitEl structure pointer returned by fthLock call
 */
void fthUnlock(fthWaitEl_t *lockEl) {
    fthLock_t *lock = lockEl->lock;
    FTH_SPIN_LOCK(&lock->spin);

    if (lockEl->write) {                     // Check lock type we have
        plat_assert(lock->writeLock == 1);
        plat_assert(lock->readLock == 0);
        lock->writeLock = 0;                 // If held then this thread must have it
        lock->writer = NULL;
    } else {
        lock->readLock--;                    // Release read lock
        plat_assert(lock->writeLock == 0);
        plat_assert(lock->readLock >= 0);
    }

    // Delete this element from the chain
    fthWaitQ_remove_nospin(&lock->holdQ, lockEl);

    if (lockEl->pool) {                      // If this is a pool element
        fthFreeWaitEl(lockEl);               // Put back on the free list
    }

    // See if any other threads are ready
    while (1) {                              // Process each one
        fthWaitEl_t *wait = lock->waitQ.head; // Prospective locker
        if (wait == NULL) {
            break;                           // No more locks to grant
        }
        
        if (wait->write) {                   // If a write lock requested
            if (lock->readLock != 0) {       // If read lock held
                break;                       // Top waiter can't go so we quit
            }
            
            lock->writeLock++;               // Set the lock
            lock->writer = wait->thread;
        } else {                             // Read lock requested
            // Read locks are always eligble since we just released
            lock->readLock++;                // Increment

        }

        // Remove the element from the wait Q
        lock->waitQ.head = lock->waitQ.head->waitQEl.next; // Next on list
        if (lock->waitQ.head == NULL) {
            lock->waitQ.tail = NULL;         // All gone
        } else {
            lock->waitQ.head->waitQEl.prev = NULL; // New head of list
        }

        fthWaitQ_push_nospin(&lock->holdQ, wait); // Add to the lock held-list
        
        fthResume(wait->thread, 0);          // This guy can go

        if (wait->write) {
            break;                           // Once we see a write, we are done
        }

        // The head of the list was popped off so we can try the new head.
        
    }

    FTH_SPIN_UNLOCK(&lock->spin);            // Release lock lock
}
/**
 * @brief Release WRITE lock and atomically aquire a READ lock
 *
 * @param lockEl <IN> fthWaitEl structure pointer returned by fthLock call
 */
void fthDemoteLock(fthWaitEl_t *lockEl) {
    fthLock_t *lock = lockEl->lock;
    FTH_SPIN_LOCK(&lock->spin);

    plat_assert(lockEl->write);

    plat_assert(lock->writeLock == 1);
    plat_assert(lock->readLock == 0);
    
    lock->writeLock = 0;              // If held then this thread must have it
    lock->writer = NULL;
    lock->readLock++;                 // Now a read lock
    lockEl->write = 0;

    // See if any other threads are ready
    while (1) {                              // Process each one
        fthWaitEl_t *wait = lock->waitQ.head; // Prospective locker
        if (wait == NULL) {
            break;                           // No more locks to grant
        }
        
        if (wait->write) {                   // If a write lock requested
            break;                           // Top waiter can't go so we quit
        } else {                             // Read lock requested
            // Read locks are always eligble since we just demoted
            lock->readLock++;                // Increment
        }

        // Remove the element from the wait Q
        lock->waitQ.head = lock->waitQ.head->waitQEl.next; // Next on list
        if (lock->waitQ.head == NULL) {
            lock->waitQ.tail = NULL;         // All gone
        } else {
            lock->waitQ.head->waitQEl.prev = NULL; // New head of list
        }

        fthWaitQ_push_nospin(&lock->holdQ, wait);   // Add to the lock held-list
        
        fthResume(wait->thread, 0);          // This guy can go

        // The head of the list was popped off so we can try the new head.  Note
        // that we have unlocked the queue so the head could change on us.
        
    }

    FTH_SPIN_UNLOCK(&lock->spin);            // Release lock lock
}

/**
 * @brief Wait for FTH lock
 *
 * @param lock <IN> FTH lock
 * @param write <IN> Nonzero for write lock, zero for read lock.
 * @parame waitEl <IN> Wait element to use or NULL (allocate one)
 * @return fthWaitEl data structure to use in call to fthUnlock
 */
fthWaitEl_t *fthLock(fthLock_t *lock, int write, fthWaitEl_t *waitEl) {
    fthThread_t *self = fthSelf();
    plat_assert(self);

    if (waitEl == NULL) {                    // If none passed in
        waitEl = fthGetWaitEl();
    }

    waitEl->lock = lock;                     // Init the lock el fields
    waitEl->write = write;
    waitEl->thread = self;

    // See if we can get the lock
    FTH_SPIN_LOCK(&lock->spin);              // Lock the lock structure

    if ((write && (lock->readLock != 0)) ||  // If cannot get lock
        (lock->writeLock != 0)) {

	#ifdef FTH_INSTR_LOCK
	    uint64_t      t, t2, dt;
	    fthLockTraceData_t *pltd1, *pltd;
	    int           i, nptrs;
	    void         *bt[FTH_INSTR_LOCK_BT_SIZE + 1];
	#endif // FTH_INSTR_LOCK

        fthWaitQ_push_nospin(&lock->waitQ, waitEl); // Add element to wait queue
        
        FTH_SPIN_UNLOCK(&lock->spin);

	#ifdef FTH_INSTR_LOCK
	{
	    /* this lock request must wait, so remember when I started waiting */

	    pltd1 = fthGetLockTraceData();
	    t = rdtsc();
	}
	#endif // FTH_INSTR_LOCK

        fthWait();                           // Wait for another thread to wake us up

	#ifdef FTH_INSTR_LOCK
	{
	    /* stash lock wait stats into the lock trace array */

	    pltd = fthGetLockTraceData();
            if (pltd1 == pltd) {
		/*  I can only get good time data if I wake up on the same
		 *  scheduler that I went to sleep on.  The lock trace data
		 *  pointers (pltd1, pltd) are unique to each scheduler.
		 */
	        t2 = rdtsc();

		    if (t2 < t) {
		        fprintf(stderr, "t2=%"PRIu64" > t=%"PRIu64" for lock %p!", t2, t, lock);
		    }
		#ifdef notdef
		#endif

		dt = t2 - t;
	    } else {
	        dt = 0;
	    }


            if (dt > 0) {
	        /*  Collect the backtrace for this lock call so I
		 *  can figure out who did the locking.
		 */
		nptrs = backtrace(bt, FTH_INSTR_LOCK_BT_SIZE + 1);
                if (nptrs > FTH_INSTR_LOCK_BT_SIZE) {
		    nptrs = FTH_INSTR_LOCK_BT_SIZE;
		}

		#ifdef notdef
		    (void) fprintf(stderr, "nptrs=%d: ", nptrs);
		    for (i=0; i<nptrs; i++) {
			(void) fprintf(stderr, "%p, ", bt[i]);
		    }
		    (void) fprintf(stderr, "\n");
		#endif

                /*  Lock records are of the form:
		 *
		 *     uint64_t  rec_type;    // read lock or write lock
		 *     uint64_t  wait_time;   // in rdtsc tics
		 *     uint64_t  plock;       // lock pointer
		 *     uint64_t  fth_self;    // fth thread pointer
		 *     uint64_t  bt[BT_SIZE]; // backtrace pointers
		 */

		if (write) {
		    FTH_INSTR_LOCK_LOG(pltd, FTH_INSTR_LOCK_LOCK_WR_REC);
		} else {
		    FTH_INSTR_LOCK_LOG(pltd, FTH_INSTR_LOCK_LOCK_RD_REC);
		}
		FTH_INSTR_LOCK_LOG(pltd, dt);
		FTH_INSTR_LOCK_LOG(pltd, (uint64_t) lock);
		FTH_INSTR_LOCK_LOG(pltd, (uint64_t) self);
		for (i=1; i<=nptrs; i++) {
		    FTH_INSTR_LOCK_LOG(pltd, (uint64_t) bt[i]);
		}
		for (i=nptrs+1; i<=FTH_INSTR_LOCK_BT_SIZE; i++) {
		    FTH_INSTR_LOCK_LOG(pltd, 0);
		}

		/*  Always dump an end record so trace readers can identify
		 *  the end of a trace file.
		 *  (We don't know if this will be the last record in trace file.)
		 */
		FTH_INSTR_LOCK_ENDREC(pltd);
	    }
	}
	#endif // FTH_INSTR_LOCK

        // When we wake up we will have the lock and be on the hold queue
        
    } else {
        if (write) {                         // Increment the appropriate counter
            lock->writeLock = 1;
            lock->writer = self;
        } else {
            lock->readLock++;
        }
        fthWaitQ_push_nospin(&lock->holdQ, waitEl); // Add element to hold queue
        FTH_SPIN_UNLOCK(&lock->spin);
    }
    
    // Got the lock either instantly or by waiting for it
    return (waitEl);

}

/**
 * @brief Try to obtain FTH lock
 *
 * @param lock <IN> FTH lock
 * @param write <IN> Nonzero for write lock, zero for read lock.
 * @param waitEl <IN> Wait element to use or NULL (allocate)
 * @return fthWaitEl data structure to use in call to fthUnlock or null
 */
fthWaitEl_t *fthTryLock(fthLock_t *lock, int write, fthWaitEl_t *waitEl) {
    fthThread_t *self = fthSelf();
    plat_assert(self);

    // See if we can get the lock
    FTH_SPIN_LOCK(&lock->spin);              // Lock the lock structure
    if ((write && (lock->readLock != 0)) ||  // If cannot get lock
        (lock->writeLock != 0)) {
        FTH_SPIN_UNLOCK(&lock->spin);
        return (NULL);                       // Try failed   
    }

    if (write) {                             // Increment the appropriate counter
        lock->writeLock = 1;
        lock->writer = self;
    } else {
        lock->readLock++;
    }

    if (waitEl == NULL) {                    // If none provided
        waitEl = fthGetWaitEl();             // Allocate oone
    }

    waitEl->lock = lock;                         // Init the lock el fields
    waitEl->write = write;
    waitEl->thread = self;

    fthWaitQ_push_nospin(&lock->holdQ, waitEl); // Add element to hold queue
    FTH_SPIN_UNLOCK(&lock->spin);
    return (waitEl);
}

