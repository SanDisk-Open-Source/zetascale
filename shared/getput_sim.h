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
 * File:   getput_sim.h
 * Author: Darpan Dinker
 *
 * Created on March 4, 2008, 1:12 PM
 * * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: getput_sim.h 1519 2008-06-09 19:10:28Z tomr $
 */

#ifndef _GETPUT_SIM_H
#define _GETPUT_SIM_H

#ifdef __cplusplus
extern "C" {
#endif

#include "container.h"

struct shard *
get_shard(SDF_internal_ctxt_t *pai, local_SDF_CONTAINER lc);

SDF_size_t
flashBlockRead(struct flashDev *dev, void *buff, SDF_blocknum_t blockNum, SDF_size_t blockSize);

int
flashBlockWrite(struct flashDev *dev, char *buf, SDF_blocknum_t blockNum, SDF_size_t blockSize);

SDF_status_t
get_object(struct shard *shard, SDF_key_t key, SDF_CACHE_OBJ *dest, SDF_size_t *size, SDF_operation_t *opid);

SDF_status_t
get_block(SDF_CONTAINER c, struct shard *shard, SDF_key_t key, SDF_CACHE_OBJ *dest, SDF_size_t *size,
          SDF_operation_t *opid);

SDF_status_t
put(SDF_internal_ctxt_t *pai, SDF_CONTAINER c, SDF_key_t key, void *pbuf, SDF_size_t size, SDF_operation_t *opid);

SDF_status_t
get(SDF_internal_ctxt_t *pai, SDF_CONTAINER c, SDF_key_t key, SDF_CACHE_OBJ *dest, SDF_size_t *size, SDF_operation_t *opid);

SDF_boolean_t
object_exists(SDF_internal_ctxt_t *pai, SDF_CONTAINER c, SDF_key_t key);

SDF_boolean_t
is_block_container(SDF_CONTAINER c);

struct objDesc *
enumerate_container(struct shard *shard, struct objDesc *prevObj, int *hashIndex, char **key, uint32_t *len);

#ifdef __cplusplus
}
#endif

#endif /* _GETPUT_SIM_H */
