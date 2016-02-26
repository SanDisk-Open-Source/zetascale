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
 * File:   fthSpinLock.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthSpinLock.h 396 2008-02-29 22:55:43Z jim $
 */

//
// Spin lock
//

#ifndef _FTH_SPIN_LOCK_H
#define _FTH_SPIN_LOCK_H

#include "platform/defs.h"
#include "utils/caller.h"

#ifndef FTH_SPIN_LOCK_BACKOFF_DELAY
#define FTH_SPIN_LOCK_BACKOFF_DELAY 1
//#define FTH_SPIN_LOCK_BACKOFF_DELAY 6500
#endif

// Initialize to unlocked state
#define FTH_SPIN_INIT(lock) *lock = 0

// Spin locks are pointers to the locking pth thread (or other thread type) for debug purposes
typedef void * fthSpinLock_t;

//
// Define simple spinlock operations.  The parameter is a pointer to the spinlock
//
#if defined(FTH_SPIN_LOCK_FAKE)
#define FTH_SPIN_LOCK(l)
#elif 0
#define FTH_SPIN_LOCK(l)                                              \
    while (PLAT_UNLIKELY(__sync_lock_test_and_set(l, (void *) 1) == (void *) 1)) {}
#elif defined(FTH_SPIN_CALLER)
#define FTH_SPIN_LOCK(l)                                              \
{                                                                     \
    void *spinVal = (void *) __builtin_return_address(0);             \
    do {                                 /* Intel recommended */      \
        while (PLAT_UNLIKELY(*l != 0)) {                              \
            __asm__ __volatile__("rep;nop" ::: "memory");             \
        }                                                             \
    } while (PLAT_UNLIKELY(__sync_val_compare_and_swap(l, (void *) 0, spinVal) != (void *) 0)); \
}
#elif 1
#define FTH_SPIN_LOCK(l)                                              \
{                                                                     \
    void *spinVal = caller();                                         \
    do {                                 /* Intel recommended */      \
        while (PLAT_UNLIKELY(*l != 0)) {                              \
            __asm__ __volatile__("rep;nop" ::: "memory");             \
        }                                                             \
    } while (PLAT_UNLIKELY(__sync_val_compare_and_swap(l, (void *) 0, spinVal) != (void *) 0)); \
}
#elif 0
#define FTH_SPIN_LOCK(l)                                              \
    while (PLAT_UNLIKELY(__sync_val_compare_and_swap(l, (void *) 0, (void *) __builtin_return_address(0)) != (void *) 0)) { \
        int _i_; \
        for (_i_ = 0; _i_ < FTH_SPIN_LOCK_BACKOFF_DELAY; ++_i_) \
            __asm__ __volatile__("rep;nop" ::: "memory");                   \
    }
#else
#define FTH_SPIN_LOCK(l) \
    while (PLAT_UNLIKELY(__sync_val_compare_and_swap(l, (void *) 0, (void *) fthLockID()) != (void *) 0)) { \
        __asm__ __volatile__("rep;nop" ::: "memory"); \
    }
#endif

//    {fthThread_t *self = fthSelf(); if (self!= NULL) {self->spinCount++;}}

#if defined(FTH_SPIN_LOCK_FAKE) 
#define FTH_SPIN_TRY(l) (1)
#elif 0
#define FTH_SPIN_TRY(l)                                              \
    (__sync_lock_test_and_set(l, (void *) 1) == (void *) 0)
#elif defined(FTH_SPIN_CALLER)
#define FTH_SPIN_TRY(l)                                               \
    (__sync_val_compare_and_swap(l, (void *) 0, (void *) __builtin_return_address(0)) == (void *) 0)
#elif 1 
#define FTH_SPIN_TRY(l)                                               \
    (__sync_val_compare_and_swap(l, (void *) 0, caller()) == (void *) 0)
#else
#define FTH_SPIN_TRY(l)                                               \
    (__sync_val_compare_and_swap(l, (void *) 0, (void *) fthLockID()) == (void *) 0)
#endif

#if defined(FTH_SPIN_LOCK_FAKE)
#define FTH_SPIN_UNLOCK(l)
#elif 0
#define FTH_SPIN_UNLOCK(l) { *(l) = (void *) 0;}
#else
#define FTH_SPIN_UNLOCK(l) { asm volatile ("sfence":::"memory"); *(l) = (void *) 0;}
#endif

#if 0
// XXX: drew 2008-09-17 This creates link and include ordering problems.  
//
// Probably better to move fthSelf()'s prototype to its own location or
// duplicate here if we switch to a lock implementation using it
#include "fth.h"
#endif


//    fthThread_t *self = fthSelf(); if (self!= NULL) {self->spinCount--;}}

#endif
