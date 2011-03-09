/*
 * File:   shard_compute.h
 * Author: chetan
 *
 * Created on  Nov 3, 2008, 2:46 PM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id$
 */
#ifndef _SHARD_COMPUTE_H
#define _SHARD_COMPUTE_H 1

#include "platform/string.h"
#include "container_props.h"
#include "common/sdftypes.h"
#include "container_meta.h"
#include "container.h"

__BEGIN_DECLS

#define SDF_SHARD_ID_MASK 0xffffffffff000000

#define CGUID_FROM_SHARDID(SHARDID)           \
    ((SHARDID) & SDF_SHARD_ID_MASK)

#define SDF_SHARD_ID_BITS 24
#define SDF_SHARD_CGUID_BITS (64 - SDF_SHARD_ID_BITS)

#define SDF_SHARD_DEFAULT_SHARD_COUNT 1

#define SDF_CONTAINER_ID_MAX (((uint64_t)1<< (64 - SDF_SHARD_ID_BITS))-1)
#define  SDF_SHARD_ID_MAX ((1<< SDF_SHARD_ID_BITS)-1)
 
/**
 * @brief get a shardid and vnode number given  objkey and cguid
 *
 */
SDF_status_t
getShardVnodeFromObj(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, const char *objkey, uint32_t num_shards, vnode_t *vnode, SDF_shardid_t *shardid);
/**
 * @brief get a shardid only given  objkey, cguid and shard_count
 */

SDF_shardid_t
shard_compute_get_shard(SDF_cguid_t cguid, const char *objkey, 
                        SDF_shardid_t first_shard, uint32_t shard_count);

/* Work around a lack of flashDev_t definition in flash.h */
inline flashDev_t *
get_flashdev_from_shardid(flashDev_t *flash_dev[], 
                          SDF_shardid_t shardid,
                          uint32_t flash_dev_count);
/**
 * @brief Get a list of all shardid's for the given container.
 *
 * @param cguid <IN> Container guid.
 * @param  shardids <OUT>    ptr to caller allocated empty array for shardid's
 * @param  max_shardids <IN> size of the preallocated shardid's array
 * @param  shard_count  <OUT> number of shardid's in the container. These
 *                            will be stuffed into the first shard_count elements 
 *                            of shardids array
 * @return SDF status.
 */
SDF_status_t
get_container_shards(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, 
                     SDF_shardid_t shardids[], uint32_t max_shardids,
                     uint32_t *shard_count);

#endif // _SHARD_COMPUTE_H 
