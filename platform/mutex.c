/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   $HeadURL
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mutex.c 342 2008-02-23 05:58:08Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include "platform/mutex.h"

int
plat_mutex_lock(plat_mutex_t *mutex) {
    return (pthread_mutex_lock(mutex));
}

int
plat_mutex_init(plat_mutex_t *mutex) {
    int ret;

    pthread_mutexattr_t attr;
    ret = pthread_mutexattr_init(&attr);
    if (!ret) {
        ret = pthread_mutexattr_setpshared(&attr, 1);
    }
    if (!ret) {
        ret = pthread_mutex_init(mutex, &attr);
    }

    return (ret);
}

int
plat_mutex_unlock(plat_mutex_t *mutex) {
    return (pthread_mutex_unlock(mutex));
}
