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
 * File:   shmem_global.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: shmem_global.c 396 2008-02-29 22:55:43Z jim $
 */

#include "platform/assert.h"
#include "platform/attr.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/types.h"

#include "private/shmem_internal.h"

#include "shmem_global.h"

enum set_mode {
    SET,
    RESET
};

#ifdef PLAT_SHMEM_FAKE
static uint64_t shmem_globals[SHMEM_GLOBAL_COUNT] = { };
#endif


static  uint64_t
set_internal(enum plat_shmem_global_id globalID, uint64_t val,
             enum set_mode mode);
    
uint64_t
shmem_global_get(enum plat_shmem_global_id globalID) {
#ifdef PLAT_SHMEM_FAKE
    return (shmem_globals[globalID]);
#else
    shmem_header_sp_t shared_header = {};
    struct shmem_header *local_header = NULL;
    shmem_admin_sp_t shared_admin = shmem_admin_sp_null;
    struct shmem_admin *local_admin = NULL;

    plat_assert(globalID < SHMEM_GLOBAL_COUNT);

    /* First real segment */
    shared_header.base = plat_shmem_first_segment();
    shmem_header_sp_rwref(&local_header, shared_header);
    plat_assert(local_header);
    plat_assert(local_header->magic == SHMEM_HEADER_MAGIC);

    shared_admin = local_header->admin;
    shmem_header_sp_rwrelease(&local_header);
    
    shmem_admin_sp_rwref(&local_admin, shared_admin);
    plat_assert_imply(!shmem_admin_sp_is_null(shared_admin), local_admin);
    
    plat_assert(local_admin->magic == SHMEM_ADMIN_MAGIC);

    uint64_t rv = local_admin->shmem_globals[globalID];

    shmem_admin_sp_rwrelease(&local_admin);

    return (rv);
#endif /* else def PLAT_SHMEM_FAKE */
}

uint64_t
shmem_global_set(enum plat_shmem_global_id globalId, uint64_t val) {
    return (set_internal(globalId, val, SET));
}

uint64_t
shmem_global_reset(enum plat_shmem_global_id globalId, uint64_t val) {
    return (set_internal(globalId, val, RESET));
}

/** @brief Set or reset global value, returning old value */
static uint64_t
set_internal(enum plat_shmem_global_id globalID, uint64_t val,
             enum set_mode mode) {
#ifndef PLAT_SHMEM_FAKE
    shmem_header_sp_t shared_header = {};
    struct shmem_header *local_header = NULL;
    shmem_admin_sp_t shared_admin = shmem_admin_sp_null;
    struct shmem_admin *local_admin = NULL;
#endif
    uint64_t ret;
    uint64_t last;

    plat_assert(globalID < SHMEM_GLOBAL_COUNT);

#ifndef PLAT_SHMEM_FAKE
    /* First real segment */
    shared_header.base = plat_shmem_first_segment();
    shmem_header_sp_rwref(&local_header, shared_header);
    plat_assert(local_header);
    plat_assert(local_header->magic == SHMEM_HEADER_MAGIC);

    shared_admin = local_header->admin;
    shmem_header_sp_rwrelease(&local_header);
    
    shmem_admin_sp_rwref(&local_admin, shared_admin);
    plat_assert_imply(!shmem_admin_sp_is_null(shared_admin), local_admin);
    
    plat_assert(local_admin->magic == SHMEM_ADMIN_MAGIC);
#endif /* ndef PLAT_SHMEM_FAKE */

    ret = 0;
    do {
        last = ret;
        ret = __sync_val_compare_and_swap(
#ifdef PLAT_SHMEM_FAKE
                                          &shmem_globals[globalID],
#else
                                          &local_admin->shmem_globals[globalID],
#endif
                                          last, val);
    } while (mode == RESET && last != ret);

#ifndef PLAT_SHMEM_FAKE
    shmem_admin_sp_rwrelease(&local_admin);
#endif

    return (ret);
}
