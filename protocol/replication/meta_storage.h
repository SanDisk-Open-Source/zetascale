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

#ifndef REPLICATION_META_STORAGE_H
#define REPLICATION_META_STORAGE_H 1
#include "meta_types.h"
#include "replicator.h"

/*
 * File:   sdf/protocol/replication/meta_storage.h
 *
 * Author: drew
 *
 * Created on December 11, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: meta_storage.h 8819 2009-08-20 10:43:07Z drew $
 */

/**
 * The replication_meta_storage interface provides a black box for
 * storing shard meta data (reference #cr_shard_meta in
 * copy_replicator_internal).
 *
 * To avoid the split brain problem, leases are granted to the
 * node modifying the meta-data on updates in which the subsystem
 * guarantees that another node will not become master for the given
 * time window.
 *
 * Implementation specifics vary depending on the shard type, with a
 * single node being used (like a quorum disc) for
 * #SDF_REPLICATION_META_SUPER_NODE and Paxos based scheme for
 * #SDF_REPLICATION_META_CONSENSUS.
 *
 * XXX: drew 2009-05-15 We want to merge the subset of the CMC meta-data
 * we're actually using with the replication meta-data so we can get
 * away from problems with that code dating to the assumptions that it
 * was a singleton for large conainers with many shards (not good for
 * availability)
 *
 * The implications are that
 * - The replication code no longer owns the meta_storage code.
 *
 * - We may want more than one listener for update callbacks
 */

#include "platform/closure.h"
#include "platform/defs.h"
#include "platform/time.h"

#include "protocol/replication/replicator.h"

struct cr_shard_meta;
struct replicator_meta_storage;
struct rms_lease_status;

/** @brief Shutdown callback */
PLAT_CLOSURE(rms_shutdown_cb);

/**
 * @brief Callback for meta-data updates
 *
 * @param status <IN> SDF_SUCCESS on success; otherwise implies a failure.
 * The conflicting shard meta is returned in shard meta, NULL if none is
 * available.
 *
 * @param shard_meta <IN> Receiver owns the result and shall free with
 * #cr_shard_meta_free.  Where an operation was requested, if known the
 * current meta-data can be returned.
 *
 * @param lease_expires <IN> A lower bound on when the lease expires in
 * absolute local time according to the #sdf_replicator_api gettime
 * function.  Network propagation delays have been accounted for.
 */
PLAT_CLOSURE3(rms_shard_meta_cb, SDF_status_t, status,
              struct cr_shard_meta *, shard_meta,
              struct timeval, lease_expires);

/**
 * @brief Callback for meta-data delete
 *
 * @param <IN> status SDF_SUCCESS on success
 */
PLAT_CLOSURE1(rms_delete_shard_meta_cb, SDF_status_t, status);

#ifdef notyet
/*
 * XXX: drew 2008-12-12 For theoretical performance reasons we should just be
 * sending the basic information which needs no marshaling and is common
 * to all roles (separate action node, potential home node, current
 * home node).
 *
 * However, that means that the copy_replicator client needs to be applying
 * its own updates to the cr_shard_meta structure.  In practice, even
 * with a fairly short timeout (hundreds of miliseconds) with a reasonable
 * number of containers the overhead should not be significant compared to
 * IOPs on the order of 500K/sec. With large number of containers we
 * want to implement the originally proposed vnode scheme and use a ligher
 * weight API.
 *
 * If we go with this simplification, there are additional callbacks
 * and argument lists change.
 *
 * This is mostly here because it illustrates information actually
 * needed for various tasks.
 */
struct rms_lease_status {
    /*
     * Provided so that routing information is propagated to
     * action nodes.
     */

    /**
     * @brief Shard-level meta-data, including ltime and current home
     * node
     */
    struct sdf_replication_shard_meta shard_meta;

    /**
     * @brief Lease on home node (relative)
     *
     * When zero, no lease exists and the current_home_node is the
     * node which last held the lease.
     *
     * The lease holder should interpret the lease granted relative
     * to the time it sent the request.  Consumers can base their
     * timing relative to response receipt; if they're wrong this
     * may just result in a retry due to #SDF_STALE_LTIME.
     *
     * So that we don't have to execute consensus on every operation
     * we grant a lease to the current home node.  To avoid problems
     * with real time going backwards we make the time relative.
     *
     * The node which last became home node accepts requests until
     * when it started the request which acquired the lease
     * (Paxos Accept!).  Other nodes do not become the home node
     * until when they observed the change plus the delta.
     *
     * This may be as long as twice the lease, where a new node
     * starts the clock counting from when it starts the sequence
     * to become master.  Observing Accepted messages would limit
     * this to one lease delay.
     *
     * This differs from the flavor in #sdf_replicator_config
     * in that it can change between invocations; this is the
     * last value.
     */
    uint64_t lease_usecs;

    /*
     * Provided so that action nodes can generate IOs appropriate for the
     * current epoch.
     */

    /**
     * @brief Shard Lamport clock
     *
     * This increments when the home-node changes so that stale
     * requests can be fenced.  It must be provided so that it can propagate
     * to the action node code.
     */
    sdf_replication_ltime_t ltime;

    /*
     * Provided so that nodes which may become the next home node
     * know when their state may have changed.
     */

    /**
     * @brief Shard meta version number
     *
     * Increments on every shard meta write, regardless of whether the
     * shard ltime has advanced.  This is used to sanity check that
     * a modified version of the shard meta-data is causally related
     * to the most recent version in persistent storage.
     *
     * It does not update when the lease is extended.
     */
    sdf_replication_ltime_t shard_meta_seqno;
};

/**
 * @brief lease info update
 *
 * @param <IN> status SDF_SUCCESS on success; otherwise implies a failure
 * with NULL lease status.
 *
 * @param <IN> lease status, receiver should free with
 * #rms_lease_status_free.
 */
PLAT_CLOSURE1(rms_lease_update_cb, struct rms_lease_status *, lease_status);

/**
 * @brief #rms_put_shard_meta completion callback
 *
 * @param <IN> status SDF_SUCCESS on success; otherwise implies a failure
 * with NULL lease status.
 *
 * @param <IN> lease status, receiver should free with
 * #rms_lease_status_free.
 */
PLAT_CLOSURE2(rms_put_shard_meta_cb, SDF_status_t, status,
              struct rms_lease_status *, lease_status);


#endif

__BEGIN_DECLS

/**
 * @brief Construct replicator_meta_storage
 *
 * The replicator_meta_storage system is started in an
 *
 * @param config <IN> configuration.  This structure is copied.  The
 * replicator_meta_storage interface may use replication_peer_service
 * for internal requests.
 *
 * @param api <IN> interface to the real system (provided by either
 * #sdf_replicator_adapter or the test environment)
 *
 * @param update_cb <IN> applied when an update is detected for
 * a shard whether remote or local
 *
 * XXX: drew 2009-08-13 Due to implementation side effects, the user's
 * callback may be applied with non-successful status and no shard_meta
 * There's an XXX to fix this in meta_storage.c.
 *
 * @return replicator_meta_storage (stop and free with #rms_shutdown)
 */

struct replicator_meta_storage *
replicator_meta_storage_alloc(const struct sdf_replicator_config *config,
                              struct sdf_replicator_api *api,
                              rms_shard_meta_cb_t update_cb);


/** @brief Start replicator meta storage   */
void rms_start(struct replicator_meta_storage *storage);


/**
 * @brief Shutdown and free the replicator meta storage
 *
 * @param rms <IN> replicator meta storage to shutdown
 * @param shutdown_complete <IN> invoked when there is no activity left
 */
void rms_shutdown(struct replicator_meta_storage *rms,
                  rms_shutdown_cb_t shutdown_complete);

/**
 * @brief Create shard meta-data
 *
 * XXX: drew 2009-05-05 To acccomodate the difference between super node,
 * Paxos, and a hypothetical LDAP meta-storage scheme we need to handle
 * meta-shard creation at this level instead of the copy_replicator.
 *
 * For now existing meta-data shard replicas are a pre-condition although
 * in the future we'll probably move this code.
 *
 * Expected failures:
 * SDF_CONTAINER_EXISTS: The container already exists.  This does not
 * apply for distributed meta-data as used in SDF_REPLICATION_V1_2_WAY and
 * SDF_REPLICATION_V1_N_PLUS_1.
 *
 * For distributed meta-data, possible failures are
 * SDF_LEASE_EXISTS: Another node holds a lease
 *
 * @param rms <IN> replicator meta storage
 *
 * @param cr_shard_meta <IN> shard meta-data to put.  The requested lease
 * time may be ignored.  The caller retains ownership of the cr_shard_meta
 * structure.  The structure will not be referenced by the replicator
 * meta storage code after #rms_put_shard_meta returns.
 *
 * @param cb <IN> applied on completion.
 */
void rms_create_shard_meta(struct replicator_meta_storage *rms,
                           const struct cr_shard_meta *cr_shard_meta,
                           rms_shard_meta_cb_t cb);

/**
 * @brief Get shard meta-data
 *
 * @param rms <IN> replicator meta storage
 *
 * @param sguid <IN> data shard GUID or #SDF_SHARDID_VIP_GROUP_OFFSET +
 * intra-node vip group id.
 *
 * XXX: drew 2009-08-09 Overloading the shard guid is an expedient step
 * to getting the state machines, lease management, and meta-data propagation
 * code working for v1 which should be revisited after v1.
 *
 * @param shard_meta <IN> subset of shard meta-data to get. This is used instead
 * of the sguid by itself so that the "client" side doesn't have to maintain
 * any sort of routing hash.  This originates from the CMC replication
 * properties, etc. and is propagated through the action->home node messages
 * so that a CMC lookup is not needed on each operation.
 *
 * XXX: drew 2009-05-11 We need to get rid of the CMC for reliability and
 * to avoid issues in the async case.  #shard_meta will go away.
 *
 * @param cb <IN> applied on completion
 */
void rms_get_shard_meta(struct replicator_meta_storage *rms,
                        SDF_shardid_t sguid,
                        const struct sdf_replication_shard_meta *shard_meta,
                        rms_shard_meta_cb_t cb);

/**
 * @brief Put shard meta-data
 *
 * Expected failures:
 * SDF_LEASE_EXISTS: Another node holds a lease
 *
 * The following do not apply to distributed meta-data as used in
 * SDF_REPLICATION_V1_2_WAY and SDF_REPLICATION_V1_N_PLUS_1.
 *
 * SDF_BAD_META_SEQNO: The data put does not have a sequence number 1 greater
 * than what's already there, implying that they are not causally related
 * (does not apply to distributed meta-data)
 *
 * SDF_BAD_LTIME: Either the home node must match or the ltime must
 * advance by one.
 *
 * @param rms <IN> replicator meta storage
 *
 * @param cr_shard_meta <IN> shard meta-data to put.  The requested lease
 * time may be ignored.  The caller retains ownership of the cr_shard_meta
 * structure.  The structure will not be referenced by the replicator
 * meta storage code after #rms_put_shard_meta
 *
 * @param cb <IN> applied on completion.
 */
void rms_put_shard_meta(struct replicator_meta_storage *rms,
                        const struct cr_shard_meta *cr_shard_meta,
                        rms_shard_meta_cb_t cb);

#if 0
/**
 * @brief Renew lease
 *
 * XXX: I don't know that there's a good argument for having a
 * separate API for lease renewal except as
 *
 * @param rms <IN> replicator meta storage
 *
 * @param cr_shard_meta <IN> Current shard meta-data
 *
 * @param cb <IN> applied on completion with length of lease, etc.
 */
void rms_renew_lease(struct replicator_meta_storage *rms,
                     const struct cr_shard_meta *cr_shard_meta,
                     rms_shard_meta_cb_t);
#endif /* ndef notyet */

/**
 * @brief Delete shard meta
 *
 * @param rms <IN> replicator meta storage
 *
 * @param cr_shard_meta <IN> Current shard meta-data
 *
 * @param cb <IN> applied on completion.   On success a NULL pointer
 * will be returned for the current meta-data.
 */
void rms_delete_shard_meta(struct replicator_meta_storage *rms,
                           const struct cr_shard_meta *cr_shard_meta,
                           rms_shard_meta_cb_t);

/**
 * @brief Receive message from peer
 */
void
rms_receive_msg(struct replicator_meta_storage *rms,
                struct sdf_msg_wrapper *msg_wrapper);

/**
 * @brief Indicate node is live
 * @param pnode <IN> Node which was alive
 */
void rms_node_live(struct replicator_meta_storage *rms, vnode_t pnode);

/**
 * @brief Indicate that a node should be treated as dead
 *
 * @param pnode <IN> Node which is dead.
 */
void rms_node_dead(struct replicator_meta_storage *rms, vnode_t pnode);

__END_DECLS

#endif /* ndef REPLICATION_STORAGE_H */
