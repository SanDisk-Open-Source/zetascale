#ifndef PLATFORM_RWLOCK_H
#define PLATFORM_RWLOCK_H  1

/*
 * File:   rwlock.h
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: rwlock.h 342 2008-02-23 05:58:08Z drew $
 */

/*
 * Read-write lock wrapper 
 */
#ifndef notyet
#include <pthread.h>

#define PLAT_RWLOCK_INITIALIZER PTHREAD_RWLOCK_INITIALIZER

typedef pthread_rwlock_t plat_rwlock_t;

#endif

int plat_rwlock_init(plat_rwlock_t *rwlock);
int plat_rwlock_destroy(plat_rwlock_t *rwlock);
int plat_rwlock_rdlock(plat_rwlock_t *rwlock);
int plat_rwlock_tryrdlock(plat_rwlock_t *rwlock);
int plat_rwlock_wrlock(plat_rwlock_t *rwlock);
int plat_rwlock_trywrlock(plat_rwlock_t *rwlock);
int plat_rwlock_unlock(plat_rwlock_t *rwlock);

#endif /* ndef PLATFORM_RWLOCK_H */
