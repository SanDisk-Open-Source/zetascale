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
 * File:   fthSparseLock.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthSparseLock.h 396 2008-02-29 22:55:43Z jim $
 */

//
// Schooner sparse locks for FTH-type threads
//

#ifndef _FTH_SPARSE_LOCK_H
#define _FTH_SPARSE_LOCK_H

#include "platform/defs.h"

struct fthSparseLock;

#include "fthSpinLock.h"

#include "fthWaitEl.h"
#include "fthLock.h"

#undef LLL_NAME
#undef LLL_EL_TYPE
#undef LLL_EL_FIELD
#undef LLL_INLINE

#define LLL_NAME(suffix) fthSparseQ ## suffix
#define LLL_EL_TYPE struct fthSparseLock
#define LLL_EL_FIELD sparseQEl

#include "fthlll.h"


// Individual lock elements
typedef struct fthSparseLock {
    int useCount;                            // Number of users
    fthSparseQ_lll_el_t sparseQEl;
    uint64_t key;                            // Key used to ID the lock
    fthLock_t lock;                          // Lock held for this key
} fthSparseLock_t;
    

typedef struct fthSparseLockBucket {
    fthSpinLock_t spin;
    fthSparseQ_lll_t bucket;
} fthSparseLockBucket_t;

#define FTH_SPARSE_LOCK_TABLE_SIZE(numBuckets) (sizeof(fthSparseLock_t) + (sizeof(fthSparseLockBucket_t) * numBuckets))

//    The lock data structure for sleep-type locks
typedef struct fthSparseLockTable {
    int numBuckets;                          // Number of buckets to hash into
    fthSparseLockBucket_t buckets[0];        // Buckets of sparse locks
} fthSparseLockTable_t;

#undef LLL_INLINE

#ifdef FTH_SPARSE_LOCK_C
#define LLL_INLINE
#else
#define LLL_INLINE PLAT_EXTERN_INLINE
#endif

#include "fthlll_c.h"

// Routines
void fthSparseLockTableInit(fthSparseLockTable_t *st, int numBuckets);
fthWaitEl_t *fthSparseLock(fthSparseLockTable_t *lt, uint64_t key, int write, fthWaitEl_t *waitEl);
fthWaitEl_t *fthTrySparseLock(fthSparseLockTable_t *lt, uint64_t key, int write, fthWaitEl_t *waitEl);
void fthSparseUnlock(fthSparseLockTable_t *lt, fthWaitEl_t *wait);

#include "fthSparseLock.h"                   // Yes, recurse again (resets LLL_xxx)

#endif
