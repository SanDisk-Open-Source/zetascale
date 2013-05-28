
/*
 * File:   sdf/platform/rwlock.c[
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: rwlock.c 342 2008-02-23 05:58:08Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include "platform/rwlock.h"

int
plat_rwlock_init(plat_rwlock_t *rwlock) {
    int ret;

    pthread_rwlockattr_t attr;
    ret = pthread_rwlockattr_init(&attr);

    if (!ret) {
        ret = pthread_rwlockattr_setpshared(&attr, 1);
    }

    if (!ret) {
        ret = pthread_rwlock_init(rwlock, &attr);
    }

    return (ret);
}

int
plat_rwlock_destroy(plat_rwlock_t *rwlock) {
    return (pthread_rwlock_destroy(rwlock));
}

int
plat_rwlock_rdlock(plat_rwlock_t *rwlock) {
    return (pthread_rwlock_rdlock(rwlock));
}

int
plat_rwlock_tryrdlock(plat_rwlock_t *rwlock) {
    return (pthread_rwlock_tryrdlock(rwlock));
}

int
plat_rwlock_wrlock(plat_rwlock_t *rwlock) {
    return (pthread_rwlock_wrlock(rwlock));
}

int
plat_rwlock_trywrlock(plat_rwlock_t *rwlock) {
    return (pthread_rwlock_trywrlock(rwlock));
}

int
plat_rwlock_unlock(plat_rwlock_t *rwlock) {
    return (pthread_rwlock_unlock(rwlock));
}
