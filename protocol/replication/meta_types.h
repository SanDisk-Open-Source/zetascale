#ifndef REPLICATION_META_TYPES_H
#define REPLICATION_META_TYPES_H 1

/*
 * File:   sdf/protocol/replication/meta_types.h
 *
 * Author: drew
 *
 * Created on October 23, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: meta_types.h 7540 2009-05-22 03:14:46Z drew $
 */

/**
 * Encapsulate the replication layer meta-data, defining all types here
 * so we can avoid include order problems with meta.h defining manipulator
 * functions.
 *
 * This defines the meta-data interface between the replication code
 * and action/home/flash modules.
 */
#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif /* SDFAPI */

enum {
    /**
     * @brief Maximum number of replicas
     *
     * 5 is enough to tolerate 2-node failures in consensus replicated
     * meta-data
     */
    SDF_REPLICATION_MAX_REPLICAS = 5
};

/**
 * @brief Configuration information about a shard needed for operations.
 *
 * Currently we stick this in the meta-data for every operation, but it's
 * not entropy and we'd be better off reading it once and caching it every
 * place where we need it.
 */
struct sdf_replication_shard_meta {
    /** @brief Replication type */
    SDF_replication_t type;

    /** @brief Number of data copies */
    uint32_t nreplica;

    /** @brief Total number of meta-data replicas */
    uint32_t nmeta;

    /**
     * @brief Where the request should be routed to.
     *
     * This can be different from the shard meta-data first node.  Due to
     * asynchrony it may not be current and the home node may redirect.
     */
    SDF_vnode_t current_home_node;

    /**
     * @brief Node IDs for replication
     *
     * The first nreplica are for data copies; meta-data is currently
     * replicated to those out to nmeta total.
     */
    SDF_vnode_t pnodes[SDF_REPLICATION_MAX_REPLICAS];

    /**
     * @brief Meta-data node.
     *
     * XXX: drew 2008-11-04 This is just here to get test coverage on
     * switch-over etc. by storing the meta-data on an always available
     * non-data node until we have Paxos-based replication on the meta-data.
     *
     * XXX: drew 2009-05-18 This becomes an array for the Paxos flavor,
     * unless we move to the CMC/replication meta merge and extract that
     * from local state which is more likely.
     *
     * It's the node used for all meta-data within the container.
     */
    SDF_vnode_t meta_pnode;

    /**
     * @brief Meta-data shard id
     *
     * We store meta-data separately from data so that we can have
     * our own garbage collection and meta-data stream (for example sequence
     * numbers) that don't affect the data stream, so we can have different
     * availability characteristics, and to avoid key collisions (although
     * an internal bit would suffice for this)
     */
    SDF_shardid_t meta_shardid;
};

enum {
    /* Invalid ltime */
    SDF_REPLICATION_LTIME_INVALID = 0
};

/**
 * @brief Shard ltime.
 *
 *  #SDF_REPLICATION_LTIME_INVALID be used as an out-of-band value.
 */
typedef uint32_t sdf_replication_ltime_t;

/**
 * @brief Fields required in each operation for replication
 *
 * Replication works as a distributed state machine where the inputs are
 * operations like "set object=value."  All copies of the state machine
 * execute the operations in the same sequence although subsequent operations
 * can be pipelined.
 */
struct sdf_replication_op_meta {
    /**
     * @brief Shard's logical time
     *
     * Operations which causaly result from stale data must be fenced.  For
     * instance, a client might lock an object, read its current value '1' and
     * write back '2'.  The lock gets broken due to a failure and the value
     * incremented a few times to '4'.  The original operation must not be
     * allowed to complete and reset the value.
     *
     * This increments on every home node change or restart.
     *
     * Replication clients which are forwarding external client
     * requests should also fence stale IOs, with a simple strategy
     * being to terminate TCP connections when the ltime changes.
     */
    sdf_replication_ltime_t shard_ltime;

    /**
     * @brief Operation GUID
     *
     * Recovery is simplest in when we can uniquely identify each opeartion
     * feeding into the state machine, even where multiple copies are
     * optimistically executing with different inputs.
     *
     * For example, for the 100th command client A could propose "key A=1"
     * with guid A1000 while client B proposes "key B=2" with guid B2000.
     * Ensuring that all copies of the state machine end up with command 100 =
     * guid A1000 is sufficient to insure they match.
     *
     * It may be reasonable to merge this with the Tag command although
     * that would require tags to be unique across reboots and mean that
     * SDF API operations could not result in multiple replication operations.
     */
    uint64_t guid;

    /**
     * @brief Operation sequence number.
     *
     * Replication works as a distributed state machine where inputs
     * are operations like "set object=value".  Using a set of sequence
     * numbers means that commands can be applied in a consistent order even
     * if the messaging layer or scheduling result in out of order delivery.
     * For example, given a delete of key A and write of key A we can guarantee
     * that the same order is observed on all replicas.  Sequence numbers
     * can associates operations with multi-Paxos instance numbers for
     * consensus based replication.  Temporal retrieval to replace
     * IOs lost during transient outages is also possible by sequence
     * number.
     *
     * Consecutive assignment of sequence numbers does not imply
     * serialization.
     *
     * The same sequence number is propogated to the flash layer and
     * used to implement its recovery scheme where last write wins and
     * only the newest version is retained on garbage collection
     * (coallescing)
     *
     * XXX: drew 2009-05-21 We've added a sequence number field to the
     * protocol message header and should probably remove this.
     */
    uint64_t seqno;

    /**
     * @brief High water mark of sequence numbers synced to all replicas
     *
     * Includes unavailable replicas.  This is used as the upper bound
     * on tombstone garbage collection, because proceeding beyond this
     * can result in dead key, value versions on off-line replicas being
     * re-animated when the replicas come back on line.
     *
     * When no replicas are down, this will be the same as
     * #last_consecutive_decided_seqno.
     */
    uint64_t last_consecutive_synced_seqno;

    /**
     * @brief High water mark of agreed sequence numbers
     *
     * Once we get to consensus based replication, data can be partitioned
     * into definitely decided and potentially undecided values.  Flash
     * coalescing can only run up to this threshold.
     */
    uint64_t last_consecutive_decided_seqno;

    /**
     * @brief static shard configuration
     *
     * Replication parameters.
     *
     * XXX: drew 2008-12-12 As a micro-optimization for message space, this
     * should be specified in a separate open message because it's not
     * going to change unless we implement certain types of dynamic
     * membership (like adding or moving shard replicas to different vnodes)
     */
    struct sdf_replication_shard_meta shard_meta;
};

#endif /* ndef REPLICATION_META_TYPES_H */
