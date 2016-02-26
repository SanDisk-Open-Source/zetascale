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

#ifndef REPLICATION_TEST_NODE_H
#define REPLICATION_TEST_NODE_H 1

/*
 * File:   sdf/protocol/replication/tests/replication_test_node.h
 *
 * Author: drew
 *
 * Created on November 3, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_node.h 12947 2010-04-16 02:05:47Z drew $
 */
#include "sys/queue.h"
#include "platform/mbox_scheduler.h"
#include "platform/closure.h"
#include "platform/defs.h"
#include "test_flash.h"
#include "test_api.h"

#include "protocol/replication/replicator.h"

struct replication_test_config;
struct replication_test_node;

/** @brief shutdown callback asynchronized */
PLAT_CLOSURE(replication_test_node_shutdown_async_cb);

/** @brief crash callback */
PLAT_CLOSURE1(replication_test_node_crash_async_cb, SDF_status_t, status);

__BEGIN_DECLS

/**
 * @brief Create simulated node.
 *
 * The node is comprised of replicator (instantiated on restart) and
 * flash (instantiated on startup) potentially with queues for outbound
 * traffic.
 */
struct replication_test_node *
replication_test_node_alloc(const struct replication_test_config *test_config,
                            struct replication_test_framework *test_framework,
                            const replication_test_api_t *api, vnode_t node);


/**
 * @brief Indicate that the node is live
 *
 * For test_config.new_liveness, this is a message ping delivery.
 *
 * Without test_config.new_liveness, this is an immediate indication
 * that the node is dead.
 *
 * XXX: drew 2010-04-07 Rename to #rtn_receive_liveness_ping for less
 * confusion once we delete the historic liveness behavior that does
 * not deal with asynchrony.
 *
 * @param test_node <IN> An initialized replication_test_node
 * @param src_node <IN> Originating "live" node
 * @param src_epoch <IN> Monotonically increasing each time src_node
 * restarts.  Note that this is different from #rtn_node_dead's
 * dest_epoch.
 */
void rtn_node_live(struct replication_test_node *test_node, vnode_t node_id,
                   int64_t src_epoch);

/**
 * @brief Indicate that the node is dead
 *
 * For test_config.new_liveness, this is close of connection
 *
 * Without test_config.new_liveness, this is an immediate indication
 * that the node is dead.
 *
 * XXX: drew 2010-04-07 Rename to #rtn_receive_connection_close for less
 * confusion once we delete the historic liveness behavior that does not
 * deal with asynchrony.
 *
 * @param test_node <IN> An initialized replication_test_node
 * @param src_node <IN> Node closing the conenction because it thought
 * #dest_node was dead.
 * @param dest_epoch <IN> Monotonically increasing each time #test_node
 * restarts.  Note this is different from #rtn_node_live since it is being
 * used to fence socket closes which happened before a #test_node
 * restart
 */
void rtn_node_dead(struct replication_test_node *test_node, vnode_t src_node,
                   int64_t dest_epoch);

/** @brief Return non-zero when node is live */
int rtn_node_is_live(struct replication_test_node *test_node);

/** @brief Return non-zero when node is live */
 int rtn_node_is_live(struct replication_test_node *test_node);

/** @brief Turn on network of specified node */
void rtn_start_network(struct replication_test_node *test_node,
                       rtfw_void_cb_t cb);

/** @brief Trun off network of specified node */
void rtn_shutdown_network(struct replication_test_node *test_node,
                          rtfw_void_cb_t cb);

/** @brief Return non-zero when node network is live */
int rtn_node_is_network_up(struct replication_test_node *test_node);

/**
 * @brief Shutdown and free simulated node
 */
void
rtn_shutdown_async(struct replication_test_node *test_node,
                   replication_test_node_shutdown_async_cb_t cb);

/**
 * @brief Start or restart node operation.
 *
 * @param user_cb <IN>  Applied on completion with SDF_SUCCESS for success,
 * other on failure (ex: node already started)
 */
void
rtn_start(struct replication_test_node *test_node,
          replication_test_framework_cb_t user_cb);

/**
 * @brief Crash simulated flash asynchronously
 *
 * This is a fan-out on #rtf_crash_async and replicator shutdown
 *
 * @param cb <IN> Applied when the "crash" completes.  Status
 * returns include SDF_SUCCESS where the node crashed,
 * SDF_NODE_DEAD where the node is already dead,
 * SDF_SHUTDOWN where it's shutting down, and SDF_BUSY
 * where a shutdown is already pending.
 */
void
rtn_crash_async(struct replication_test_node *test_node,
                replication_test_node_crash_async_cb_t cb);

/**
 * @brief Receive message.
 *
 * One reference count of msg_wrapper is consumed
 *
 * Depending on service, this indirects to either flash (SDF_FLSH) or
 * the replicator (SDF_REPLICATOR, SDF_REPLICATOR_PEER, etc.)
 */
void
rtn_receive_msg(struct replication_test_node *test_node,
                struct sdf_msg_wrapper *msg_wrapper);

/**
 * @brief Send message
 */
void
rtn_send_msg(struct replication_test_node *test_node,
             struct sdf_msg_wrapper *msg_wrapper,
             struct sdf_fth_mbx *ar_mbx);

/**
 * @brief Get op_meta
 * @param op_meta <OUT> op_meta pointer
 */
SDF_status_t
rtn_get_op_meta(struct replication_test_node *test_node,
                const struct SDF_container_meta *container_meta,
                SDF_shardid_t shard, struct sdf_replication_op_meta *op_meta);

/* RT_TYPE_REPLICATOR functions */

/**
 * @brief Set closure applied on notification callback events
 *
 * @param test_node <IN> An initialized replication_test_node with
 * test_type RT_TYPE_REPLICATOR.
 *
 * @param cb <IN> attached via the #sdf_replicator_add_notifier.  Note
 * that the user must hand-shake the callback before receiving an additional
 * notification and that shutdown/crashes block on pending notifications.
 * This replaces all previously specified closures.
 * rtfw_replicator_notification_cb_null may be specified to
 * route messages into the aether.
 */
void
rtn_set_replicator_notification_cb(struct replication_test_node *test_node,
                                   rtfw_replicator_notification_cb_t cb);

/**
 * @brief Process a command asynchronously
 *
 * @param test_node <IN> An initialized replication_test_node with
 * test_type RT_TYPE_REPLICATOR.
 * @param shard <IN> shard being operated on
 * @param command <IN> command being executed; caller can free after
 * return.
 * @param cb <IN> closure applied on completion
 */
void
rtn_command_async(struct replication_test_node *test_node, SDF_shardid_t shard,
                  const char *command, sdf_replicator_command_cb_t cb);



/* RT_TYPE_META_STORAGE functions */

/**
 * @brief Set function called when replication shard meta data propagates
 *
 * @param test_node <IN> An initialized replication_test_node with
 * test_type RT_TYPE_META_STORAGE.
 *
 * @param update_cb <IN> applied when the node is live and an update is
 * detected for a shard whether remote or local.  This replaces all previously
 * specified closures.  rtfw_shard_meta_cb_null may be specified to
 * route messages into the aether.
 */
void rtn_set_meta_storage_cb(struct replication_test_node *test_node,
                             rtfw_shard_meta_cb_t update_cb);

/**
 * @brief Create shard meta-data
 *
 * @param test_node <IN> An initialized replication_test_node with
 * test_type RT_TEST_TYPE_META_STORAGE.
 *
 * @param cr_shard_meta <IN> shard meta-data to put.  The requested lease
 * time is ignored.
 *
 * @param cb <IN> applied on completion. See #rtfw_shard_meta_cb
 * for ownership of closure shard_meta argument.
 */
void rtn_create_shard_meta(struct replication_test_node *test_node,
                           const struct cr_shard_meta *cr_shard_meta_arg,
                           rtfw_shard_meta_cb_t cb);

/**
 * @brief Get shard meta-data
 *
 * @param test_node <IN> An initialized replication_test_node with
 * test_type RT_TEST_TYPE_META_STORAGE.
 *
 * @param sguid <IN> data shard GUID
 *
 * @param shard_meta <IN> subset of shard meta-data to get. This is used instead
 * of the sguid by itself so that the "client" side doesn't have to maintain
 * any sort of routing hash.  The caller may free shard_meta immediately.
 *
 * @param cb <IN> applied on completion. See #rtfw_shard_meta_cb
 * for ownership of closure shard_meta argument.
 */
void rtn_get_shard_meta(struct replication_test_node *test_node,
                        SDF_shardid_t sguid,
                        const struct sdf_replication_shard_meta *shard_meta_arg,
                        rtfw_shard_meta_cb_t cb);


/**
 * @brief Put shard meta-data
 *
 * @param test_node <IN> An initialized replication_test_node with
 * test_type RT_TEST_TYPE_META_STORAGE.
 *
 * @param cr_shard_meta <IN> shard meta-data to put.  The requested lease
 * time is ignored.  The shard meta-data being put must be causaly related
 * to the current meta data, meaning that its shard_meta_seqno must be exactly
 * one higher than the last version (otherwise the callback status argument
 * will be SDF_STALE_META_SEQNO).  The caller may immediately free
 * cr_shard_meta.
 *
 * @param cb <IN> applied on completion. See #rtfw_shard_meta_cb
 * for ownership of closure shard_meta argument.
 */
void rtn_put_shard_meta(struct replication_test_node *test_node,
                        const struct cr_shard_meta *cr_shard_meta_arg,
                        rtfw_shard_meta_cb_t cb);

/**
 * @brief Delete shard meta
 *
 * @param test_node <IN> An initialized replication_test_node with
 * test_type RT_TEST_TYPE_META_STORAGE.
 *
 * @param cr_shard_meta <IN> Current shard meta-data.  The caller may
 * immediately free #cr_shard_meta.
 *
 * @param meta_out <OUT> On success, NULL is stored here.  On failure,
 * conflicting meta-data is stored here with the caller owning.
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
void rtn_delete_shard_meta(struct replication_test_node *test_node,
                           const struct cr_shard_meta *cr_shard_meta_arg,
                           rtfw_shard_meta_cb_t user_cb);

__END_DECLS
#endif /* ndef REPLICATION_TEST_NODE_H */
