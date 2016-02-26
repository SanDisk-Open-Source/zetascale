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
