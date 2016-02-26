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
 * File:   shard_compute.c
 * Author: chetan
 *
 * Created on  Nov 3, 2008, 2:46 PM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id$
 */

#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include "platform/string.h"
#include "platform/stdlib.h"
#include "platform/logging.h"
#include "cmc.h"
#include "container.h"
#include "container_props.h"
#include "protocol/action/recovery.h"
#include  "shared/shard_compute.h"
#include  "common/sdftypes.h"

#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_NAMING
#define LOG_TRC PLAT_LOG_LEVEL_TRACE
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_WARN PLAT_LOG_LEVEL_WARN
#define LOG_FATAL PLAT_LOG_LEVEL_FATAL

extern SDF_cmc_t *theCMC; // Global CMC object

extern SDF_status_t
name_service_get_meta(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, SDF_container_meta_t *meta);

SDF_shardid_t
shard_compute_get_shard(SDF_cguid_t cguid, const char *objkey,
                        SDF_shardid_t first_shard, uint32_t shard_count)
{
    SDF_shardid_t shard = SDF_SHARDID_INVALID;
    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_TRC;
    size_t  key_len;

    plat_log_msg(21630, LOG_CAT, LOG_TRC, "%lu",
		 cguid);
    
    if (cguid == CMC_CGUID) {
	shard = theCMC->meta.shard;
	status = SDF_SUCCESS;
/* The objects are distributed over shards here.
   shards are distributed over flash devices in home node code. 
   (or wherever else shardCreate is done
*/
    } else {
        key_len =  strnlen(objkey, 256);
        /* XXX:  objkey should probably be defined unsiged too */
        shard = first_shard +
                hashck((unsigned char *) objkey, key_len, 0, cguid) %
                shard_count;
    }

    plat_log_msg(21613, LOG_CAT, log_level, "%lu - %lu - %s",
		 cguid, shard, SDF_Status_Strings[status]);
    
    return (shard);
}


inline flashDev_t *
get_flashdev_from_shardid(flashDev_t *flash_dev[], 
                          SDF_shardid_t shardid,
                          uint32_t flash_dev_count)
{
    int index;
    index = (((shardid << SDF_SHARD_CGUID_BITS) >> SDF_SHARD_CGUID_BITS) % flash_dev_count);

        // TEMP DEBUG STUFF ONLY. FOR flash_dev_count == 2
    plat_assert(index == 0 || index == 1);

    return flash_dev[index];
}

/**
 * @brief Get a list of all shardid's for the given container.
 *  
 * @doc see declaration in shard_compute.h
 */
SDF_status_t
get_container_shards(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, 
                     SDF_shardid_t shardids[], uint32_t max_shardids,
                     uint32_t *shard_count)
{
    SDF_shardid_t first_shard;
    SDF_status_t  rc;
    SDF_container_meta_t meta;
    
    memset(&meta, 0, sizeof(meta));
    
    plat_assert(pai); 
    plat_assert(shardids); 
    plat_assert(shard_count);
    
    
    rc = name_service_get_meta(pai, cguid, &meta);
    if (rc == SDF_SUCCESS) {
        first_shard = meta.shard;
        *shard_count = meta.properties.shard.num_shards;
    } else {
        return rc;
    }
    
    plat_assert(*shard_count <= max_shardids);
    
    for (int ii=0; ii< *shard_count; ii++) {
        shardids[ii] = first_shard + ii;
        plat_assert((shardids[ii] & (~SDF_SHARD_ID_MASK)) <= SDF_SHARD_ID_MAX);
    }
    return SDF_SUCCESS;
}
