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
 * File:   fthSparseLock.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthSparseLock.c 396 2008-02-29 22:55:43Z jim $
 */

/**
 * @brief sparse FTH internal locks.
 */

#define FTH_SPIN_CALLER

#define FTH_SPARSE_LOCK_C

#include "fth.h"
#include "fthLock.h"

#include "fthSpinLock.h"
#include "fthSparseLock.h"

/*
 * @brief - sparse lock table init
 *
 * @param st - table to init
 * @param numBuckets - number of buckets allocated (power of 2)
 */
void fthSparseLockTableInit(fthSparseLockTable_t *st, int numBuckets) {
    st->numBuckets = numBuckets;
    for (int i = 0; i < numBuckets; i++) {
        FTH_SPIN_INIT(&st->buckets[i].spin);
        fthSparseQ_lll_init(&st->buckets[i].bucket);
    }
}

/*
 * @brief - sparse lock wait routine
 *
 * @param lockTable - lock table
 * @param key - key of lock;  also used for hashing
 * @param write - nonzero if write lock requested
 * @param waitEl <IN> wait element or NULL (allocate)
 *
 * @return wait element to use on release
 */
fthWaitEl_t *fthSparseLock(fthSparseLockTable_t *lt, uint64_t key, int write, fthWaitEl_t *waitEl) {
    int hash = key & (lt->numBuckets-1);
    FTH_SPIN_LOCK(&lt->buckets[hash].spin);  // One at a time

    // Walk the chain for a key match
    struct fthSparseLock *prevPtr = NULL;    // Point at the chain head
    struct fthSparseLock *sl = lt->buckets[hash].bucket.head;
    while ((sl != NULL) && (sl->key < key)) { // Search for the key
        prevPtr = sl;
        sl = sl->sparseQEl.next;
    }

    if ((sl == NULL) || (sl->key != key)) {  // If no match
        // Allocate a sparse lock
        sl = fthGetSparseLock();
        fthSparseQ_insert_nospin(&lt->buckets[hash].bucket, prevPtr, sl);
        sl->key = key;
    }

    sl->useCount++;                          // Remember users

    FTH_SPIN_UNLOCK(&lt->buckets[hash].spin);

    fthWaitEl_t *rv = fthLock(&sl->lock, write, waitEl); // Get/wait for the underlying lock
    rv->sparseLock = sl;

    return(rv);
    
}

/*
 * @brief - sparse lock try routine
 *
 * @param lockTable - lock table
 * @param key - key of lock;  also used for hashing
 * @param write - nonzero if write lock requested
 * @param waitEl <IN> wait element or NULL (allocate)
 *
 * @return Wait element to use on release or NULL if already locked
 */
fthWaitEl_t *fthTrySparseLock(fthSparseLockTable_t *lt, uint64_t key, int write, fthWaitEl_t *waitEl) {
    int hash = key & (lt->numBuckets-1);
    FTH_SPIN_LOCK(&lt->buckets[hash].spin);  // One at a time

    // Walk the chain for a key match
    struct fthSparseLock *prevPtr = NULL;    // Point at the chain head
    struct fthSparseLock *sl = lt->buckets[hash].bucket.head;
    while ((sl != NULL) && (sl->key < key)) { // Search for the key
        prevPtr = sl;
        sl = sl->sparseQEl.next;
    }

    if ((sl == NULL) || (sl->key != key)) {  // If no match
        // Allocate a sparse lock
        sl = fthGetSparseLock();
        fthSparseQ_insert_nospin(&lt->buckets[hash].bucket, prevPtr, sl);
        sl->key = key;
    }

    fthWaitEl_t *rv = fthTryLock(&sl->lock, write, waitEl); // Get/wait for the underlying lock
    if (rv != NULL) {
        rv->sparseLock = sl;
        sl->useCount++;                      // Remember users
    } else if (sl->useCount == 0) {          // Created just for this and it failed
        fthSparseQ_remove_nospin(&lt->buckets[hash].bucket, sl); // Off the bucket
        fthFreeSparseLock(sl);
    }


    FTH_SPIN_UNLOCK(&lt->buckets[hash].spin);


    return(rv);
    
}

/*
 * @brief - sparse unlock routine
 *
 * @param lockTable - lock table
 * @param - wait element to release
 */
void fthSparseUnlock(fthSparseLockTable_t *lt, fthWaitEl_t *wait) {
    fthSparseLock_t *sl = wait->sparseLock;
    int hash = sl->key & (lt->numBuckets-1);
    FTH_SPIN_LOCK(&lt->buckets[hash].spin);  // One at a time
    wait->lock = &sl->lock;                  // Replace underlying lock pointer
    fthUnlock(wait);                         // Release the underlying lock

    sl->useCount--;                          // Decrement use count
    if (sl->useCount == 0) {                 // Last one out - free the SL
        fthSparseQ_remove(&lt->buckets[hash].bucket, sl); // Off the bucket
        fthFreeSparseLock(sl);
    }
    FTH_SPIN_UNLOCK(&lt->buckets[hash].spin);

    
}
