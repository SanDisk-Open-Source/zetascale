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

/**
 * @brief Init lock data structure.
 *
 * @param lock - fthLock pointer
 */
void fthLockInitName(fthLock_t *lock, const char *name, const char * f) 
{
    pthread_mutex_init(&(lock->mutex), NULL);
}

/**
 * @brief Release FTH lock
 *
 * @param lockEl <IN> fthWaitEl structure pointer returned by fthLock call
 */
void fthUnlock(fthWaitEl_t *lockEl) 
{
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
        rv = lock;
    } else {
        rv = NULL;
    }
    return rv;
}

