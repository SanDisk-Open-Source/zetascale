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

#ifndef REPLICATION_COPY_REPLICATOR_INTERNAL_H
#define REPLICATION_COPY_REPLICATOR_INTERNAL_H 1

/*
 * File:   sdf/protocol/replication/copy_replicator_internal.h
 *
 * Author: drew
 *
 * Created on November 13, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: copy_replicator_internal.h 9332 2009-09-18 22:14:39Z drew $
 */

/**
 * Internal structures and functions for copy replicator.
 *
 * XXX: drew 2009-09-11 This is getting out of hand and needs to split
 * into smaller pieces
 */

#include "platform/assert.h"
#include "platform/closure.h"
#include "platform/defs.h"

#include "common/sdftypes.h"
#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif

/* For SDF_REPLICATION_MAX_REPLICAS */
#include "protocol/replication/meta_types.h"

/* For MAX_CNAME_SIZE */
#include "shared/container.h"

/** @brief #cr_create_meta_shard callback */
PLAT_CLOSURE1(cr_create_meta_shard_cb, SDF_status_t, status)

struct SDF_shard_meta;

struct sdf_replicator_config;

enum {
    /** @brief First valid sequence number */
#ifndef notyet
    /* Recovery never fucking completes without this */
    CR_FIRST_VALID_SEQNO = 0
#else
    CR_FIRST_VALID_SEQNO = 1
#endif
};

struct cr_paxos_meta {
    /** @brief Ballot most significant */
    uint64_t ballot_number;

    /** @brief Ballot least significant */
    SDF_vnode_t ballot_node;

    /** @brief Multi-Paxos instance number */
    uint64_t instance;

    /** @brief Guid of operation */
    uint64_t op_guid;
};

struct cr_persistent_header {
    /** @brief magic number for this structure */
    plat_magic_t magic;

    /** @brief Size of this structure */
    uint32_t len;

    /** @brief Version of this structure */
    uint32_t version;
};

enum {
    /* SHARD */
    CR_SHARD_META_MAGIC = 0x52414853,
    CR_SHARD_META_VERSION = 1
};

enum {
    /** @brief Out of band value for no current home node */
    CR_HOME_NODE_NONE = SDF_ILLEGAL_PNODE
};

/**
 * Persistent meta-data
 *
 * This has a lot of information in common with various other
 * schooner structures (notably shard_meta from shared/shard_meta.h
 * and SDF_replication_props_t from common/sdf_properties.h) which
 * can change independantly.
 */
struct cr_persistent_shard_meta {
    struct cr_persistent_header header;

    /* Static configuration section */

    /* @brief Container guid.  */
    SDF_cguid_t cguid;

    /** @brief Container name */
    char cname[MAX_CNAME_SIZE + 1];

    /** @brief Data shard */
    SDF_shardid_t sguid;

    /**
     * @brief Shardids are stored in separate cr_persistent_vip_meta
     *
     * This implies sguid == SDF_SHARDID_INVALID;
     */
    unsigned sguid_multiple : 1;

    /** @brief Meta-data shard (this shard) */
    SDF_shardid_t sguid_meta;

    /** @brief Which VIP group group this container is associated with */
    int inter_node_vip_group_group_id;

    /**
     * @brief Group ID being mapped in
     *
     * Separate state is maintained for each sguid, intra_node_vip_group_id
     * tuple each generating a callback because the lease expiration time
     * may be different for each.
     */
    int intra_node_vip_group_id;

    /** @brief Replication type */
    SDF_replication_t type;

    /** @brief Number of data replicas */
    uint32_t num_replicas;

    /** @brief Numb meta-data replicas */
    uint32_t num_meta_replicas;

    /** @brief Meta-data pnodes */
    SDF_vnode_t meta_pnodes[SDF_REPLICATION_MAX_REPLICAS];

#if 0
    /*
     * XXX: drew 2008-11-14 I think this is unnecessary, because the
     * outstanding window implies that everything within the sequence
     * number range [0, highest_observed - oustanding_window] is synced
     * with all replicas that were authoritative at the time of the
     * crash.
     */

    /**
     * @brief Increment which sequence number is synced back to persistent
     * shard meta-data.
     *
     * A smaller increment trades shorter recovery time for more
     * overhead from meta-data operations.
     */
    uint64_t seqno_sync_increment;
#endif

    /* Things which may become dynamic */

    /**
     * @brief Number of outstanding operations allowed
     *
     * Note that changes here require the existing window to close when
     * this is reduced, and recovery has to go far enough that the
     * rollback window based on the observed high-water sequence number
     * and old outstanding_window has been correctly stored in a
     * #cr_persistent_shard_range.
     */
    uint32_t outstanding_window;

    /* Dynamic operational state */

    /**
     * @brief Current home-node
     *
     * #CR_HOME_NODE_NONE when there is no current home node.
     * The copy_replicator on the node containing the lowest numbered
     * replica in the #CR_REPLICA_STATE_AUTHORITATIVE state shall
     * attempt to become the new home node.
     */
    SDF_vnode_t current_home_node;

    /**
     * @brief Previous home node
     *
     * The last real other home node.  Where current_home_node is
     * CR_HOME_NODE_NONE this is the last node to hold a lease.  When
     * current_home_node is valid this is the node which held the lease
     * before the transition to CR_HOME_NODE_NONE.
     *
     * CR_HOME_NODE_NONE when there is no valid value as on initial
     * creation.
     */
    SDF_vnode_t last_home_node;

    /** @brief Node on which this was written */
    SDF_vnode_t write_node;

    /**
     * @brief Lease on home node, relative to current time
     *
     * So that we don't have to execute consensus on every operation
     * we grant a lease to the current home node.  To avoid problems
     * with real time going backwards we make the time relative.
     *
     * The node which last became home node accepts requests until
     * when it started the request which acquired the lease.  Other
     * nodes do not become home node until they observe the lease time
     * out.
     *
     * This differs from the flavor in #sdf_replicator_config
     * in that it can change between invocations; this is the
     * last value.
     */
    uint64_t lease_usecs;

    /**
     * @brief Leases remain until liveness changes for test
     *
     * When debugging recovery state problems lease timeouts are inconvienient
     * because they change the system state. When sdf_replicator_config,
     * lease_usecs is set to this value leases will never time out and
     * all attempts to acquire a lease will succeed as long as the old
     * home node is dead according to the liveness subsystem and the new
     * meta-data is causally related to the previous version with a correct
     * ltime.
     */
    unsigned lease_liveness : 1;

    /**
     * @brief Shard Lamport clock
     *
     * This increments when the home-node changes so that stale
     * requests can be fenced.  It does not advance on other meta-data
     * changes such as lease renewals or recovery progress.
     */
    sdf_replication_ltime_t ltime;

    /**
     * @brief Shard meta version number
     *
     * Increments on every shard meta write regardless of whether the
     * shard ltime has advanced.  This is used to fence stale updates
     * and guarantee that modified versions of the shard meta-data are
     * causally related to the most recent version in persistent storage.
     */
    sdf_replication_ltime_t shard_meta_seqno;
};

/**
 * @brief Persistent replica states
 * item(caps, lower)
 */
#define CR_PERSISTENT_REPLICA_STATE_ITEMS() \
    /**                                                                        \
     * @brief Dead replica which needs deleting                                \
     *                                                                         \
     * With high churn, we run out of space on the authoritative replicas      \
     * maintaining tombstones and aren't gaining much in terms of time         \
     * to recovery.  Once this has happend we should declare the replica       \
     * dead.                                                                   \
     *                                                                         \
     * When the replica comes back on line, we'll delete it.                   \
     */                                                                        \
    item(CR_REPLICA_DEAD, dead)                                                \
    /**                                                                        \
     * @brief Dead replica which is deleting                                   \
     *                                                                         \
     * On completion, the replica can be set to stale.                         \
     */                                                                        \
    item(CR_REPLICA_DELETING, deleting)                                        \
    /**                                                                        \
     * @brief Stale replica that's not accessable                              \
     *                                                                         \
     * When stale replicas exist authoritative replicas' tombstone             \
     * retention window cannot advance.                                        \
     */                                                                        \
    item(CR_REPLICA_STALE, stale)                                              \
    /**                                                                        \
     * @brief Authoritative replica which can be used                          \
     *                                                                         \
     * This implies correctness, but using the replica resolves ambiguity      \
     * which causes other AUTHORITATVE replcase to become stale.               \
     */                                                                        \
    item(CR_REPLICA_STATE_AUTHORITATIVE, authoritative)                        \
    /**                                                                        \
     * @brief Synchronized                                                     \
     *                                                                         \
     * All replicas have completely identical content.  Other replicas         \
     * in the SYNCHRONIZED state can remain authoritative until writes         \
     * are performed.                                                          \
     */                                                                        \
    item(CR_REPLICA_STATE_SYNCHRONIZED, synchronized)


enum cr_persistent_replica_state {
#define item(caps, lower) caps,
    CR_PERSISTENT_REPLICA_STATE_ITEMS()
#undef item
};

enum {
    /* REPL */
    CR_SHARD_REPLICA_META_MAGIC = 0x4c504552,
    CR_SHARD_REPLICA_META_VERSION = 1
};


/** @brief Persistent shard replica meta-data */
struct cr_persistent_shard_replica_meta {
    struct cr_persistent_header header;

    /** @brief Data pnode */
    SDF_vnode_t pnode;

    /** @brief Replica state summary */
    enum cr_persistent_replica_state state;

    /** @brief Number of ranges */
    int nrange;
};

#define CR_REPLICA_RANGE_TYPE_ITEMS() \
    /**                                                                        \
     * @brief Range needs to be rolled back.                                   \
     *                                                                         \
     * The subset of IOs which reach each replica can be different when a      \
     * simulataneous failure occurs.                                           \
     *                                                                         \
     * For example :$                                                          \
     * op #    replica A      replica B                                        \
     * 1       write X = 1                                                     \
     * 2                      write Y = 2                                      \
     *                                                                         \
     * If replica A is made authoritative, the write Y = 2 must be rolled      \
     * back by an idempotent put of Y's current value.                         \
     *                                                                         \
     * The cr_persistent_shard_range checkpoint fields may be valid.           \
     */                                                                        \
    item(CR_REPLICA_RANGE_UNDO)                                                \
    /**                                                                        \
     * @brief Range needs to be synced mutually with other replicas            \
     *                                                                         \
     * With simultaneous failures mutual redo brings replicas into             \
     * agreement without causing some to become non-authoritative or           \
     * an advance in current sequence numbers that would be more               \
     * difficult to address following a subsequent failure.                    \
     */                                                                        \
    item(CR_REPLICA_RANGE_MUTUAL_REDO)                                         \
    /**                                                                        \
     * @brief Range needs resync                                               \
     *                                                                         \
     * The recovery code iterates over all objects (including tombstones       \
     * for deleted objects) on a replica which is synchronized for this        \
     * range and puts or deletes as appropriate                                \
     *                                                                         \
     * The cr_persistent_shard_range checkpoint fields may be valid.           \
     */                                                                        \
    item(CR_REPLICA_RANGE_REDO)                                                \
    /**                                                                        \
     * @brief Range is still active                                            \
     *                                                                         \
     * Legal transitions following a crash are for a mutual roll-forward       \
     * or a roll-back and then roll-forward off a single authoritative         \
     * copy; the later being preferred for commonality with the recovery       \
     * followin an outage.                                                     \
     *                                                                         \
     * The cr_persistent_shard_range checkpoint fields are never valid.        \
     */                                                                        \
    item(CR_REPLICA_RANGE_ACTIVE)                                              \
    /**                                                                        \
     * @brief Range is completely synchronized                                 \
     *                                                                         \
     * The cr_persistent_shard_range checkpoint fields are never valid.        \
     */                                                                        \
    item(CR_REPLICA_RANGE_SYNCED)

/** @brief Synchronized state of a range within a replica */
enum cr_replica_range_type {
#define item(caps) caps,
    CR_REPLICA_RANGE_TYPE_ITEMS()
#undef item
};

enum {
    /** @brief A range which is still open with unknown ending */
    CR_SHARD_RANGE_OPEN = UINT64_MAX
};

enum {
    /* RANG */
    CR_SHARD_RANGE_MAGIC = 0x474e4152,
    CR_SHARD_RANGE_VERSION = 1
};

/** @brief Persistent shard range description */
struct cr_persistent_shard_range {
    struct cr_persistent_header header;

    /** @brief Starting sequence number */
    uint64_t start;

    /** @brief Length of range */
    uint64_t len;

    /** @brief State of range */
    enum cr_replica_range_type range_type;

    /** @brief Checkpoint is valid */
    int checkpoint_valid;

    /** @brief Replica index for retrieval */
    uint32_t checkpoint_replica;

    /** @brief Checkpoint of retrieval */
    uint64_t checkpoint[2];
};

/** @brief In-core shard meta-data */
struct cr_shard_meta {
    /** @brief Persistent subset of data */
    struct cr_persistent_shard_meta persistent;

    /** @brief Replicas */
    struct cr_shard_replica_meta *replicas[SDF_REPLICATION_MAX_REPLICAS];

    /**
     * @brief Shards within a VIP group
     *
     * XXX: drew 2009-09-11 This is an expedient measure to support
     * multiple containers associated with a single intra-node vip
     * group, the issue being that copy_replicator is built around
     * the cr_shard abstraction and replicator_meta_storage
     * around rms_shard each of which assume complete ownership
     * of a lease.
     *
     * We need to split the lease/vip group out, run an acquisition
     * state machine on that in the copy replicator code, and associate
     * multiple shard level objects with that.
     *
     * For now we just have an annex to associate multiple shards
     * and their state with the top level structure which we're
     * using a subset of for v1.
     */
    struct cr_vip_meta *vip_meta;
};

/** @Brief In-core per-shard meta-data */
struct cr_shard_replica_meta {
    struct cr_persistent_shard_replica_meta persistent;

    struct cr_persistent_shard_range *ranges;
};

enum {
    /* VIPG */
    CR_VIP_META_MAGIC = 0x47504956,
    CR_VIP_META_VERSION = 1
};

#define CR_VIP_META_SHARD_STATE_ITEMS() \
    /** @brief Vip group should live here */                                   \
    item(CR_VMSS_PREFERRED, preferred)                                         \
    item(CR_VMSS_UNRECOVERED, unrecovered)                                     \
    item(CR_VMSS_RECOVERED, recovered)

enum cr_vip_meta_shard_state {
#define item(caps, lower) caps,
    CR_VIP_META_SHARD_STATE_ITEMS()
#undef item
};

struct cr_vip_meta_shard {
    SDF_shardid_t sguid;
    enum cr_vip_meta_shard_state state;
};

/*
 * @brief List of shard ids associated with a piece of meta-data
 *
 * XXX: drew 2009-09-02 This is probably a stop gap for V1 simple
 * replication association of multiple local shards to a single
 * intra-node VIP group.  Long term we probably want to split the
 * lease information out.
 */
struct cr_persistent_vip_meta {
    struct cr_persistent_header header;

    /** @brief Size of sguid */
    int num_shards;

    /** @brief Data shard ids */
    struct cr_vip_meta_shard shards[];
};

/**
 * @brief Thin wrapper around cr_persistent_vip_meta
 *
 * Allows growth of shard id state without changing address.
 */
struct cr_vip_meta {
    struct cr_persistent_vip_meta *persistent;
};

__BEGIN_DECLS

/**
 * @brief Create cr_shard_meta structure for a new shard
 *
 * By default, all replicas are marked CR_REPLICA_STATE_AUTHORITATIVE with
 * one CR_REPLICA_RANGE_ACTIVE range.  Meta-data is created with no current
 * home node or lease.
 *
 * @param out <OUT> On success, a pointer to a #cr_shard_meta
 * structure freeable with #cr_shard_meta_free is stored here.
 * @param config <IN> Replicator configuration
 * @param shard_meta <IN> Shard meta-data from creation request
 *
 */
SDF_status_t
cr_shard_meta_create(struct cr_shard_meta **out,
                     const struct sdf_replicator_config *config,
                     const struct SDF_shard_meta *shard_meta);

/**
 * @brief Recursively free in-core shard meta-data
 *
 * @param shard_meta <IN> free this
 */
void cr_shard_meta_free(struct cr_shard_meta *shard_meta);

/**
 * @brief Make a deep copy of the cr_shard_meta structure
 */
struct cr_shard_meta *cr_shard_meta_dup(const struct cr_shard_meta *cr_shard_meta);

/**
 * @brief Compare lhs to rhs
 *
 * Perform a deep comparison between lhs and rhs, with an output
 * like memcmp.
 *
 * @return < 0 if *lhs is less than *rhs, 0 when *lhs equals *rhs,
 * and 1 when lhs is greater than *rhs.
 */
int cr_shard_meta_cmp(const struct cr_shard_meta *lhs,
                      const struct cr_shard_meta *rhs);

/**
 * @brief Marshal peristant/network shard meta-data
 *
 * @param out <OUT> On success, a pointer to a buffer allocated
 * via #plat_alloc is stored here.
 * @param out_len <OUT> On success, the total length of the output
 * buffer including user specified buffer_offset is stored here.
 * @param buffer_offset <IN> Offset of actual data in out buffer so that
 * the user can store a header for network transmission in the same
 * space.
 * @param in <IN> #cr_shard_meta object which remains owned by the user
 */
SDF_status_t cr_shard_meta_marshal(void **out, size_t *out_len,
                                   size_t buffer_offset,
                                   const struct cr_shard_meta *in);

/**
 * @brief Unmarshal persistent/network shard meta-data
 *
 * @param out <OUT> On success, a pointer to a #cr_shard_meta
 * structure freeable with #cr_shard_meta_free is stored here.
 * @param in <IN> Buffer which is unmarshalled
 * @param in_len <IN> Length of in buffer
 * @return SDF_SUCCESS on success, otherwise on error
 */
SDF_status_t cr_shard_meta_unmarshal(struct cr_shard_meta **out,
                                     const void *in, size_t in_len);

/**
 * @brief Return human and machine friendly string
 *
 * @param  shard_meta <IN> shard meta data to dump
 * @return pthread-local static buffer
 */
char *cr_shard_meta_to_string(const struct cr_shard_meta *shard_meta);

/**
 * @brief Replace shards ids
 *
 * Used when the local node assumes ownership of an intra node VIP group
 * and is replacing the shardids.
 *
 * @param shard_meta <INOUT> shard replica meta-data being manipulated
 * @param ids <IN> new IDs. Caller retains ownership
 * @return SDF_SUCCESS on success; the only failures should be memory
 *  allocation
 */
SDF_status_t cr_shard_meta_replace_vip_meta(struct cr_shard_meta *shard_meta,
                                            const struct cr_vip_meta *meta);


/**
 * @brief Return pnodes based on replication meta-data
 *
 * Currently this does chained declustering
 *
 * @param config <IN> Configuration
 * @param dest <OUT> All pnodes
 * @param num_ptr <OUT> N pointer for number of replicas.  While this
 * duplicates information in rep_props, it gets filled in correctly
 * regardless of whether replication is turned on.
 * @param num_ptr_meta <OUT> N pointer for number of meta-data replicas.
 * This is also correct regardless of whether replication is turned on.
 */
void cr_get_pnodes(const struct sdf_replicator_config *config,
                   const SDF_replication_props_t *rep_props, vnode_t first_node,
                   SDF_vnode_t dest[SDF_REPLICATION_MAX_REPLICAS],
                   uint32_t *num_ptr, uint32_t *num_ptr_meta);


/**
 * @brief Create meta-data shards
 *
 * @param config <IN> Configuration
 * @param shard_meta <IN> Shard meta-data
 * @param cb <IN> applied on completion
 */
void cr_create_meta_shard(const struct sdf_replicator_config *config,
                          const struct cr_shard_meta *shard_meta,
                          cr_create_meta_shard_cb_t cb);

/**
 * @brief Add additional range to end of replica meta-data
 *
 * @param meta <INOUT> shard replica meta-data being manipulated which is a
 * sub-object of #cr_shard_meta.
 * @param range_type <IN> type of new range
 * @param start <IN> start of new range
 * @param seqno_len <IN> length of new range
 */
void cr_shard_replica_meta_add_range(struct cr_shard_replica_meta *meta,
                                     enum cr_replica_range_type range_type,
                                     uint64_t start, uint64_t seqno_len);


/**
 * @brief Concatenate adjacent ranges of same type producing canonical form
 *
 * @param meta <INOUT> shard replica meta-data beng manipulated.  Is probably
 * a sub-object of #cr_shard_meta.
 */
void cr_shard_replica_meta_make_canonical(struct cr_shard_replica_meta *meta);

/**
 * @brief Convert range type to a string
 *
 * @return Constant string which won't change
 */
const char *cr_replica_range_type_to_string(enum cr_replica_range_type
                                            range_type);

/**
 * @brief Convert the replica state to a string
 *
 * @return Constant string which won't change
 */

const char *cr_persistent_replica_state_to_string(enum cr_persistent_replica_state
                                                  replica_state);

/** @brief Allocate a #cr_vip_meta */
struct cr_vip_meta *cr_vip_meta_alloc();

/** @brief Make a deep copy of #cr_vip_meta */
struct cr_vip_meta *cr_vip_meta_dup(const struct cr_vip_meta *meta);

/** @brief Free a #cr_vip_meta */
void cr_vip_meta_free(struct cr_vip_meta *ids);

/** @brief Set sguid state, adding to #cr_vip_meta when sguid does not exist */
SDF_status_t cr_vip_meta_set_shard_state(struct cr_vip_meta *meta,
                                         SDF_shardid_t sguid,
                                         enum cr_vip_meta_shard_state state);

/**
 * @brief Set all shard states to state
 *
 * Useful for switch-over where all foreign shards transition to
 * the CR_VMSS_UNCRECOVERED state.
 */
void
cr_vip_meta_set_all_shard_state(struct cr_vip_meta *meta,
                                enum cr_vip_meta_shard_state state);

/** @brief Remove sguid from #cr_vip_meta */
SDF_status_t cr_vip_meta_remove(struct cr_vip_meta *ids, SDF_shardid_t sguid);

/** @brief Return non-zero when #cr_vip_meta contains sguid */
int cr_vip_meta_contains(struct cr_vip_meta *ids, SDF_shardid_t sguid);

/** @brief Return whether the vip group is completely recovered */
int cr_vip_meta_is_recovered(struct cr_vip_meta *meta);

/**
 * @brief Compare lhs to rhs
 *
 * Perform a deep comparison between lhs and rhs, with an output
 * like memcmp.
 *
 * @return < 0 if *lhs is less than *rhs, 0 when *lhs equals *rhs,
 * and 1 when lhs is greater than *rhs.
 */
int cr_vip_meta_cmp(const struct cr_vip_meta *lhs,
                    const struct cr_vip_meta *rhs);

/**
 * @brief Marshal peristant/network shard ids
 *
 * @param out <OUT> On success, a pointer to a buffer allocated
 * via #plat_alloc is stored here.
 * @param out_len <OUT> On success, the total length of the output
 * buffer including user specified buffer_offset is stored here.
 * @param buffer_offset <IN> Offset of actual data in out buffer so that
 * the user can store a header for network transmission in the same
 * space.
 */
SDF_status_t cr_vip_meta_marshal(void **out, size_t *out_len,
                                 size_t buffer_offset,
                                 const struct cr_vip_meta *in);

/**
 * @brief Unmarshal persistent/network vip meta data
 *
 * @param out <OUT> On success, a pointer to a #cr_vip_meta
 * structure freeable with #cr_vip_meta_free is stored here.
 * @param inout <IN> Pointer of data to unmarshal, updated to point
 * at end
 * @param inout_len <IN> Length of in buffer
 * @return SDF_SUCCESS on success, otherwise on error
 */
SDF_status_t cr_vip_meta_unmarshal(struct cr_vip_meta **out, const void **inout,
                                   size_t inout_len);

/**
 * @brief Return human and machine friendly string
 *
 * @param  meta <IN> vip meta data to dump
 * @return pthread-local static buffer
 */
char *cr_vip_meta_to_string(const struct cr_vip_meta *meta);

const char *cr_vip_meta_shard_state_to_string(enum cr_vip_meta_shard_state state);

__END_DECLS

#endif /* ndef REPLICATION_COPY_REPLICATOR_INTERNAL_H */
