/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   container_meta.h
 * Author: DO
 *
 * Created on January 15, 2008, 2:46 PM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: container_meta.h 9219 2009-09-14 02:12:19Z briano $
 *
 */

#ifndef _CONTAINER_META_H
#define _CONTAINER_META_H

#ifdef __cplusplus
extern "C" {
#endif

#include "platform/stdlib.h"
#include "common/sdftypes.h"
#include "container.h"
#include "container_props.h"
#include "api/zs.h"

__BEGIN_DECLS

enum {
    SDF_CONTAINER_META_VERSION = 0x2
};

typedef struct {
    SDF_shardid_t sguid;
    SDF_objectid_t oguid;
} SDF_container_cntrs_t;

typedef struct SDF_container_meta {

    /*
     * @brief Type of metadata (to differentiate from cguid map, state, etc.)
     */
    SDF_meta_t type;

    /*
     * @brief Metadata version number.
     */
    uint32_t version;

    /*
     * @brief Container guid.
     */
    SDF_cguid_t cguid;

    /*
     * @brief Container properties.
     */
    SDF_container_props_t properties;		// FIXME: remove
    ZS_container_props_t zs_properties;

    /** @brief Time at which objects in container are to be flushed */
    SDF_time_t   flush_time;
    /** @brief Time at which flush_time was set */
    SDF_time_t   flush_set_time;

    /** @brief Is container stopped? */
    SDF_boolean_t   stopflag;

    /**
     * @brief Flash shard id (all nodes)
     *
     * XXX: drew 2008-10-15 This should become the first shard-id of the
     * set, with the number either included in the container meta-data
     * or programatically determined by the sharding API from the replication
     * properties.
     */
    SDF_shardid_t shard;

    /**
     * @brief total number of shards comprising
     * this container
     */

//    uint32_t   shard_count; 

    /**
     * @brief Flash meta-data shard id
     *
     * XXX: This should be programatically determined.
     */
    SDF_shardid_t meta_shard;

    /** @brief Number of shards created on each node */
    int shards_per_node;

    /** @brief Number of nodes used for a single single */
    int nodes_per_replica;

    /**
     * @brief First pnode containing first shard's first replica
     *
     * As of 2008-10-27, the replication code assumes shard replicas
     * are distributed according to chained declustering;
     * so shard id shard + 1 would have replicas starting at
     * node (node + 1) % nnode.  Its first replica would be at node
     * (node + 1 + 1) % nnode.  Etc.
     *
     * A similar scheme can be applied to shard location. A shard offset
     * can live at node (node + (shard_offset %  nodes_per_replica)) % nnode,
     * and its device can be (shard_offset / nodes_per_replica) % num
     * devices.
     *
     * XXX: drew 2008-10-14 This may go away entirely when we get the sharding
     * API in and can programatically determine this.  We may or may not want
     * first node and current home-node available, with current home node not
     * persisted.
     */
    uint32_t node;

    /** 
     * @brief First meta-data node.
     *
     * XXX: drew 2008-11-04 This is just here to get test coverage on 
     * switch-over etc. by storing the meta-data on an always available
     * non-data node until we have Paxos-based replication on the meta-data.
     */
    uint32_t meta_node;

    /** @brief Which VIP group group this container is associated with */
    int inter_node_vip_group_group_id;

    SDF_container_cntrs_t counters;

    /*
    ** Adding container name for recovery blob support...
    */
    char cname[MAX_CNAME_SIZE + 1];

	SDF_boolean_t delete_in_progress;
    uint64_t flags;
} SDF_container_meta_t;


/*
 * @brief Structure used by flash to store container metadata.
 *  This used to contain more than just the metadata...leaving it in case we add stuff..
 */
typedef struct SDF_container_meta_blob {

    /*
     * @brief Blob container metadata version number.
     */
    uint64_t version;

    // Container metadata
    SDF_container_meta_t meta;

} SDF_container_meta_blob_t;


// ==============================================================

/**
 * @brief Get the versions for SDF_container_meta_t and SDF_container_meta_blob_t
 *
 * Into a provided buffer, print a string that shows the versions of
 * the SDF_container_meta_t and SDF_container_meta_blob_t structures.
 *
 * @param bufp <IN> Pointer to a character buffer.
 * @param lenp <IN> Length of the character buffer.
 *
 * @return 0 if successful, -ENOSPC if not.
 */
int container_meta_report_version( char **bufp, int *lenp);

/**
 * @brief Create container metadata object.
 *
 * This object simply contains the name of the container and pointers
 * to the properties and  shard table objects.
 *
 * @param name <IN> Container name.
 * @param props <IN> Pointer to the container properties object.
 *
 * XXX Sharding will change from being a single-per container object
 * to a scheme reference and specifics.
 *
 * @param shard <IN> Shard id
 * @return Pointer to the metadata object.
 */
SDF_container_meta_t *
container_meta_create(const char *name, SDF_container_props_t props,
                      SDF_cguid_t cguid, SDF_shardid_t shard);

/**
 * @brief Destroy container properties.
 *
 * @param meta <IN> Pointer to the metadata object.
 * @return SDF status.
 */
SDF_status_t
container_meta_destroy(SDF_container_meta_t *meta);
#if 0
/**
 * @brief Get the container name from the metadata object.
 *
 * @param meta <IN> Pointer to the metadata object.
 * @return Container name.
 */
char *
container_meta_get_name(SDF_container_meta_t *meta);

/**
 * @brief Set the container name in the metadata object.
 *
 * @param meta <IN> Pointer to the metadata object.
 * @param name <IN> Container name.
 * @return SDF status.
 */
SDF_status_t
container_meta_set_name(SDF_container_meta_t *meta, const char *name);
#endif
/**
 * @brief Get the container properties from the metadata object.
 *
 * @param meta <IN> Pointer to the metadata object.
 * @param props <IN> Pointer to the container properties object.
 * @return SDF status.
 */
SDF_status_t
container_meta_get_props(SDF_container_meta_t *meta, SDF_container_props_t *props);

/**
 * @brief Set the container properties in the metadata object.
 *
 * @param meta <IN> Pointer to the metadata object.
 * @param props <IN> Pointer to the container properties object.
 * @return SDF status.
 */
SDF_status_t
container_meta_set_props(SDF_container_meta_t *meta, SDF_container_props_t props);

/**
 * @brief Get the container shard mapping table from the metadata object.
 *
 * @param meta <IN> Pointer to the metadata object.
 * @return Shard, #SDF_SHARDID_INVALID on failure
 */
SDF_shardid_t
container_meta_get_shard(SDF_container_meta_t *meta);

/**
 * @brief Set the container shard mapping table in the metadata object.
 *
 * @param meta <IN> Pointer to the metadata object.
 * @return shard <IN> Pointer to the container shard mapping table object.
 * @return SDF_status_t
 */
SDF_status_t
container_meta_set_shard(SDF_container_meta_t *meta, SDF_shardid_t shard);

/**
 * @brief Get the home container shard mapping table from the metadata object.
 *
 * @param meta <IN> Pointer to the metadata object.
 * @return Shard, > #SDF_SHARDID_LIMIT on failure
 */
SDF_shardid_t
container_meta_get_shard(SDF_container_meta_t *meta);

/**
 * @brief Get the home vnode from the container metadata.
 *
 * @param meta <IN> Pointer to the metadata object.
 * @return Container home vnode.
 */
uint32_t
container_meta_get_vnode(SDF_container_meta_t *meta);


__END_DECLS

#ifdef __cplusplus
}
#endif

#endif /* _CONTAINER_META_H */
