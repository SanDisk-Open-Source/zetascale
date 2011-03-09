/*
 * File:   shard_meta.h
 * Author: drew
 *
 * Created on June 16, 2008, 2:46 PM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shard_meta.h 8689 2009-08-11 18:34:40Z drew $
 */

/**
 * Per-shard meta-data.  Used for shard creation.
 */

#ifndef _SHARD_META_H
#define _SHARD_META_H

#include "common/sdf_properties.h"
#include "common/sdftypes.h"

enum SDF_shard_type {
    SDF_SHARD_TYPE_BLOCK,
    SDF_SHARD_TYPE_OBJECT,
    SDF_SHARD_TYPE_LOG
};

enum SDF_shard_persistence {
    SDF_SHARD_PERSISTENCE_YES,
    SDF_SHARD_PERSISTENCE_NO
};

enum SDF_shard_eviction {
    SDF_SHARD_EVICTION_CACHE,
    SDF_SHARD_EVICTION_STORE
};

/** @brief Shard meta-data */
struct SDF_shard_meta {
    /* XXX: Best bet is to make this a starting number and add a count */
    SDF_shardid_t sguid;

    /**
     * @brief Shard-id used by replication meta-data
     *
     * Interesting (performant, fault tolerant) replication implementations
     * on top of Schooner flash will be simplest to implement when we have some
     * meta-data stored on separate shards from the user data with a different
     * set of sequence numbers and coalescing properties.
     *
     * This also makes it easier to separate user data from ours (we could have
     * a special "internal" key bit) and have different replication counts for
     * meta-data (must have 3 potentially optomistic copies to avoid
     * split-brain) and data.
     *
     * XXX: It would be nice if this was opaque, but as long as container
     * creation is happening here we'll live with it.
     */
    SDF_shardid_t sguid_meta;

    enum SDF_shard_type type;
    enum SDF_shard_persistence persistence;
    enum SDF_shard_eviction eviction;
    uint64_t quota;
    uint32_t num_objs;

    /*
     * XXX: Everything above here is a pass-through to the flash layer,
     * so it should probably split out.
     *
     * Maybe everything below goes into the data payload for the create
     * shard message.
     */

    /** @brief How replication is performed */
    SDF_replication_props_t replication_props;

    /**
     * @brief Home node for replication
     *
     * XXX: drew 2008-10-14 Long term, we'll have the sharding API to
     * calculate things for us.  Short term we punt the first node around
     * and arrive at the others via chained declustering.
     *
     * Replicas on (first_node + replica number) % num nodes for all
     * replica numbers 0..replication_props.num_replicas - 1 for data,
     * 0..replication_props.num_meta_replicas - 1 for consensus replicated
     * meta-data.
     */
    vnode_t first_node;

#ifndef notyet
    /**
     * @brief Meta-data node.
     *
     * XXX: drew 2008-11-04 This is just here to get test coverage on
     * switch-over etc. by storing the meta-data on an always available
     * non-data node until we have Paxos-based replication on the meta-data.
     */
    vnode_t first_meta_node;
#endif

    /** @brief Which VIP group group this container is associated with */
    int inter_node_vip_group_group_id;
};

#endif /* _SHARD_META_H */
