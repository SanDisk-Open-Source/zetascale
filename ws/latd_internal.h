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

/**********************************************************************
 *
 *  latd_internal.h   8/29/16   Brian O'Krafka   
 *
 *  Lookaside map for tracking the most current copies of data
 *  that are in destaging buffers that haven't completed writing
 *  to storage.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 **********************************************************************/

#ifndef _LATD_INTERNAL_H
#define _LATD_INTERNAL_H

#include "wshash.h"
#include "latd.h"

#define RWLOCK                pthread_rwlock_t
#define RWLOCK_INIT(rwlk)     pthread_rwlock_init(rwlk, NULL)
#define RW_RD_LOCK(rwlk)      pthread_rwlock_rdlock(rwlk)
#define RW_WR_LOCK(rwlk)      pthread_rwlock_wrlock(rwlk)
#define RW_RD_TRYLOCK(rwlk)   pthread_rwlock_tryrdlock(rwlk)
#define RW_WR_TRYLOCK(rwlk)   pthread_rwlock_trywrlock(rwlk)
#define RW_RD_UNLOCK(rwlk)    pthread_rwlock_unlock(rwlk)
#define RW_WR_UNLOCK(rwlk)    pthread_rwlock_unlock(rwlk)

struct latd;

typedef struct latd_link {
    uint64_t            cguid;
    char                key[MAX_KEY_LEN];
    uint32_t            key_len;
    struct latd_link   *next;
    struct latd_link   *prev;
    struct latd        *table;
    uint32_t            n_free;
    uint32_t            n_bucket;
    char                data[0];
} latd_link_t;

typedef struct latd {
    uint32_t       n_buckets;
    uint32_t       datasize;
    RWLOCK        *locks;
    long          *lock_thrds;
    latd_link_t  **buckets;
    uint32_t       n_free_lists;
    RWLOCK        *free_list_locks;
    long          *free_lock_thrds;
    latd_link_t  **free_lists;
    uint32_t       n_links_total;
    uint32_t       n_links_used;
} latd_t;

#endif
