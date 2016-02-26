/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_SPIN_RW_H
#define PLATFORM_SPIN_RW_H  1

/*
 * File:   spin_rw.h
 * Author: Wei,Li
 *
 * Created on July 15, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 */

/*
 * Spin Read-write wrapper
 */
#include <limits.h>

#include "platform/defs.h"

typedef int plat_spin_rwlock_t;

enum {
    RWSPIN_LOCKED_WRITE  = INT_MAX,
    RWSPIN_INIT = 0
};

#define plat_spin_rw_init(rwlock) \
    do {                                                                       \
        (rwlock) = RWSPIN_INIT;                                                \
    } while (0)

#define plat_spin_rw_rdlock(rwlock) \
    do {                                                                       \
        while (PLAT_UNLIKELY(__sync_add_and_fetch(&rwlock, 1) <= 0)) {         \
            (void) __sync_sub_and_fetch(&rwlock, 1);                           \
        }                                                                      \
    } while (0)

#define plat_spin_rw_wrlock(rwlock) \
    do {                                                                       \
    } while (PLAT_UNLIKELY(__sync_bool_compare_and_swap(&rwlock, 0,            \
                                                        -RWSPIN_LOCKED_WRITE) == 0))

#define plat_spin_rw_rdunlock(rwlock) \
    (void) __sync_sub_and_fetch(&rwlock, 1)

#define plat_spin_rw_wrunlock(rwlock)\
    (void) __sync_add_and_fetch(&rwlock, RWSPIN_LOCKED_WRITE)

#endif /* ndef PLATFORM_SPIN_RW_H */
