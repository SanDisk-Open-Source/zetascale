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

#ifndef _FTH_LOCK_H
#define _FTH_LOCK_H

/** @brief The lock data structure for sleep-type read-write locks */
typedef struct fthLock {
    pthread_mutex_t     mutex;
    int locks;
#ifdef DEBUG_BUILD
    void *locking_func;
    pthread_t owner_tid;
    void *last_unlock_func;
#endif 
} fthLock_t;

typedef fthLock_t fthWaitEl_t;

__BEGIN_DECLS
// Routines - must be here because of wait El definition
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
