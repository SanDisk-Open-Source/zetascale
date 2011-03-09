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
