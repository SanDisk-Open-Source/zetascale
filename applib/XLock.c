/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   XLock.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: XLock.c 396 2008-02-29 22:55:43Z jim $
 */


/**
 * @brief Cross-thread locking - this is the PTHREAD-callable version
 */

#include <pthread.h>

#include "platform/assert.h"

#include "sdfappcommon/XLock.h"
#include "XLock.h"

/**
 * @brief Get or wait for cross-thread lock (pthread callable only)
 *
 * @param cross <IN> Pointer to cross lock data structure
 * @param write <IN> Nonzero for write lock, zero for read lock
 */
void XLock(XLock_t *cross, int write) {
    // Increment the FTH lock to block FTH threads
    (void) __sync_fetch_and_add(&cross->fthLock, 1); // Increment FTH lock

    // Get and release the Q lock for queueing purposes only
    plat_assert_rc(pthread_rwlock_rdlock(&cross->qLock));
    plat_assert_rc(pthread_rwlock_unlock(&cross->qLock));

    // Release the FTH lock
    (void)__sync_fetch_and_sub(&cross->fthLock, 1);

    // Get the real lock (or queue for it)
    if (write) {
        plat_assert_rc(pthread_rwlock_wrlock(&cross->lock));
    } else {
        plat_assert_rc(pthread_rwlock_rdlock(&cross->lock));
    }
}

/**
 * @brief Try for cross-thread lock (pthread callable only)
 *
 * @param cross <IN> Pointer to cross lock data structure
 * @param write <IN> Nonzero for write lock, zero for read lock
 * @return int 0=OK; not zero=could not get lock
 */
int XTryLock(XLock_t *cross, int write) {
    // Increment the FTH lock to block FTH threads
    (void) __sync_fetch_and_add(&cross->fthLock, 1); // Increment FTH lock

    // Get and release the Q lock for queueing purposes only
    if (pthread_rwlock_tryrdlock(&cross->qLock) != 0) {
        (void) __sync_fetch_and_add(&cross->fthLock, -1);
        return 1;
    }
    plat_assert_rc(pthread_rwlock_unlock(&cross->qLock));

    // Release the FTH lock
    (void) __sync_fetch_and_sub(&cross->fthLock, 1);

    // Get the real lock (or queue for it)
    int rv;
    if (write) {
        rv = pthread_rwlock_trywrlock(&cross->lock);
    } else {
        rv = pthread_rwlock_tryrdlock(&cross->lock);
    }
    
    return (rv);
}

/**
 * @brief Release cross lock
 *
 * @param cross <IN> Pointer to cross lock data structure
 */
void XUnlock(XLock_t *cross) {
    plat_assert_rc(pthread_rwlock_unlock(&cross->lock));
}
