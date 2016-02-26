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
 * File:   appbuf_pool.c
 * Author: Brian O'Krafka
 *
 * Created on March 3, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: appbuf_pool.c 308 2008-02-20 22:34:58Z tomr $
 */

#define _APPBUF_POOL_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "platform/stdlib.h"
#include "platform/shmem.h"
#include "platform/assert.h"
#include "platform/stats.h"
#include "platform/shmem_arena.h"
#include "platform/alloc.h"

#include "appbuf_pool.h"
#include "appbuf_pool_internal.h"

/* returns: 0 if successful, non-zero otherwise */
int init_app_buf_pool(SDF_appBufState_t **ppabs, SDF_appBufProps_t *pprops)
{
    SDF_appBufState_t *pabs;

    // pabs = plat_alloc(sizeof(SDF_appBufState_t));
    // pabs = plat_alloc_arena(sizeof(SDF_appBufState_t), PLAT_SHMEM_ARENA_CACHE_THREAD);
    pabs = plat_alloc_arena(sizeof(SDF_appBufState_t), PLAT_SHMEM_ARENA_CACHE_READ_BUF_THREAD);
    if (pabs == NULL) {
	plat_log_msg(21081, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
		     "plat_alloc failed!");
        plat_abort();
    }
    *ppabs = pabs;

    pabs->props = *pprops;
    pabs->nget  = 0;
    pabs->nfree = 0;
    return(0);
}

void destroy_app_buf_pool(struct SDF_appBufState *pabs)
{
    plat_free(pabs);
}

/* returns: non-NULL if successful, NULL otherwise */
void *get_app_buf(SDF_appBufState_t *pabs, uint64_t size)
{
    // return(plat_alloc(size));
    // return(plat_alloc_arena(size, PLAT_SHMEM_ARENA_CACHE_THREAD));
    (pabs->nget)++;
    return(plat_alloc_arena(size, PLAT_SHMEM_ARENA_CACHE_READ_BUF_THREAD));
}

/* returns: 0 if successful, non-zero otherwise */
int free_app_buf(SDF_appBufState_t *pabs, void *pbuf)
{
    (pabs->nfree)++;
    plat_free(pbuf);
    return(0);
}

void get_app_buf_pool_stats(struct SDF_appBufState *pabs, uint64_t *pnget, uint64_t *pnfree)
{
    *pnget  = pabs->nget;
    *pnfree = pabs->nfree;
}




