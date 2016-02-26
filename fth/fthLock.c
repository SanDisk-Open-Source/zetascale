/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthLock.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
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

/**
 * @brief Init lock data structure.
 *
 * @param lock - fthLock pointer
 */
void fthLockInitName(fthLock_t *lock, const char *name, const char * f) 
{
    pthread_mutex_init(&(lock->mutex), NULL);
#ifdef DEBUG_BUILD
    lock->owner_tid = -1; //Invalid tid 0xfffffffff..
    lock->locking_func = NULL;
#endif 
    lock->locks = 0;
}

/**
 * @brief Release FTH lock
 *
 * @param lockEl <IN> fthWaitEl structure pointer returned by fthLock call
 */
void fthUnlock(fthWaitEl_t *lockEl) 
{
    plat_assert(lockEl->locks == 1);
#ifdef DEBUG_BUILD
    plat_assert(lockEl->locking_func != NULL);
//  plat_assert(pthread_self() == lockEl->owner_tid); //lock and unlock from same thread

    lockEl->owner_tid = -1;
    lockEl->locking_func = NULL;
    lockEl->last_unlock_func =  __builtin_return_address(0);
#endif 
    lockEl->locks--;

    pthread_mutex_unlock(&(lockEl->mutex));
}

/**
 * @brief Release WRITE lock and atomically aquire a READ lock
 *
 * @param lockEl <IN> fthWaitEl structure pointer returned by fthLock call
 */
void fthDemoteLock(fthWaitEl_t *lockEl) 
{
    // purposefully empty
}

/**
 * @brief Wait for FTH lock
 *
 * @param lock <IN> FTH lock
 * @param write <IN> Nonzero for write lock, zero for read lock.
 * @parame waitEl <IN> Wait element to use or NULL (allocate one)
 * @return fthWaitEl data structure to use in call to fthUnlock
 */
fthWaitEl_t *fthLock(fthLock_t *lock, int write, fthWaitEl_t *waitEl) 
{
    pthread_mutex_lock(&(lock->mutex));
    plat_assert(lock->locks == 0);
    lock->locks++;
  
#ifdef DEBUG_BUILD 
    lock->owner_tid = pthread_self();
    plat_assert(lock->locking_func == NULL);
    lock->locking_func = __builtin_return_address(0);
    lock->last_unlock_func = NULL;
#endif 
    
    return lock;
}

/**
 * @brief Try to obtain FTH lock
 *
 * @param lock <IN> FTH lock
 * @param write <IN> Nonzero for write lock, zero for read lock.
 * @param waitEl <IN> Wait element to use or NULL (allocate)
 * @return fthWaitEl data structure to use in call to fthUnlock or null
 */
fthWaitEl_t *fthTryLock(fthLock_t *lock, int write, fthWaitEl_t *waitEl) 
{
    fthWaitEl_t  *rv;

    if (pthread_mutex_trylock(&(lock->mutex)) == 0) {
	plat_assert(lock->locks == 0);
#ifdef DEBUG_BUILD
        plat_assert(lock->locking_func == NULL);
	lock->locking_func = __builtin_return_address(0);
	lock->last_unlock_func = NULL;
#endif 
	lock->locks++;
        rv = lock;
    } else {
        rv = NULL;
    }
    return rv;
}

