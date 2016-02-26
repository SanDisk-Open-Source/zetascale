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

#ifndef REPLICATION_RPC_H
#define REPLICATION_RPC_H 1

/*
 * File:   sdf/protocol/replication/rpc.h
 *
 * Author: drew
 *
 * Created on May 22, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: rpc.h 8439 2009-07-21 14:28:07Z jmoilanen $
 */

/*
 * Thin wrappers for message based APIs.  The input is a
 * sdf_replicator_send_msg_cb_t which can easily be used
 * to wrap the original sdfmsg code, test environment, etc.
 */

#include "common/sdftypes.h"
#include "platform/closure.h"

/**
 * @brief Callback for #rr_get_msg_by_cursor_async
 *
 * @param wrapper <IN> sdf_msg_wrapper with one reference count
 * for the callee.
 */
PLAT_CLOSURE1(rr_get_msg_by_cursor_cb,
              struct sdf_msg_wrapper *, wrapper);

/**
 * @brief Callback for #rr_get_seqno_async
 *
 * @param seqno <IN> get seqno to use for action
 */
PLAT_CLOSURE2(rr_get_seqno_cb, SDF_status_t, status, uint64_t, seqno);

/** @brief Apply to free buffer. */
PLAT_CLOSURE2(rr_read_data_free_cb,
              const void *, data, size_t, data_len);

/**
 * @brief Callback for #rr_get_last_seqno_async
 *
 * @param status <IN> SDF_SUCCESS on success, otherwise on failure
 * @param seqno <IN> highest sequence number seen of write acknowledged to
 * user.
 */
PLAT_CLOSURE2(rr_last_seqno_cb, SDF_status_t, status, uint64_t, seqno);

/**
 * @brief Callback for #rr_get_iteration_cursors_async
 *
 * @param status <IN> SDF_SUCCESS on success, otherwise on failure
 * @param output <IN> iteration cursors, NULL on EOF, free with
 * #rr_get_iteration_cursors_free
 */
PLAT_CLOSURE2(rr_get_iteration_cursors_cb,
              SDF_status_t, status,
              struct flashGetIterationOutput *, output);

/**
 * @brief Replication test get-by-cursor op completion callback
 *
 * data should be freed by the free callback argument.
 */
PLAT_CLOSURE9(rr_get_by_cursor_cb,
              SDF_status_t, status, const void *, data, size_t, data_len,
              char *, key, int,  key_len, SDF_time_t, exptime,
              SDF_time_t, createtime, uint64_t, seqno,
              rr_read_data_free_cb_t, free_cb);

/**
 * @brief Get last sequence number asynchronously
 *
 * @param node <IN> Destination node for request
 *
 * @param sguid <IN> data shard GUID
 *
 * @param cb <IN> applied on completion.
 */
void rr_get_last_seqno_async(sdf_replicator_send_msg_cb_t send_closure,
                             vnode_t src_node, vnode_t dest_node, SDF_shardid_t sguid,
                             rr_last_seqno_cb_t cb);

/**
 * @brief Get last sequence number synchronously
 *
 * This is a thin wrapper around #rr_get_last_seqno_async
 *
 * @param src_node <IN> Source node for request
 *
 * @param dest_node <IN> Destination node for request
 *
 * @param pseqno <OUT> The sequence number retrieved is stored here.
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t rr_get_last_seqno_sync(sdf_replicator_send_msg_cb_t send_closure,
                                    vnode_t src_node, vnode_t dest_node,
				    SDF_shardid_t sguid, uint64_t *pseqno);

/**
 * @brief Get iteration cursors asynchronously
 *
 * @param src_node <IN> Source node
 *
 * @param dest_node <IN> Destination node
 *
 * @param shard <IN> data shard GUID
 *
 * @param seqno_start <IN> start of sequence number range, inclusive
 *
 * @param seqno_len   <IN> max number of cursors to return at a time
 *
 * @param seqno_max   <IN> end of sequence number range, inclusive
 *
 * @param cb <IN> applied on completion.
 */
void rr_get_iteration_cursors_async(sdf_replicator_send_msg_cb_t send_closure,
                                    vnode_t src_node, vnode_t dest_node,
				    SDF_shardid_t shard, 
                                    uint64_t seqno_start, uint64_t seqno_len,
                                    uint64_t seqno_max,
                                    const void *cursor, int cursor_size,
                                    rr_get_iteration_cursors_cb_t cb);

struct SDF_action_state;

SDF_status_t
rr_start_replicating_simple(SDF_context_t ctxt, SDF_cguid_t cguid,
                            vnode_t src_node,
                            vnode_t dest_node,
                            SDF_shardid_t shard,
                            void * data,
                            size_t *data_len);

SDF_status_t
rr_get_iteration_cursors_simple(vnode_t src_node, vnode_t dest_node,
                                SDF_shardid_t shard, uint64_t seqno_start,
                                uint64_t seqno_len, uint64_t seqno_max,
                                const void *cursor, int cursor_size,
                                struct flashGetIterationOutput **out);

SDF_status_t
rr_get_by_cursor_simple(SDF_context_t ctxt, SDF_shardid_t shard, vnode_t src_node,
                        vnode_t dest_node, void *cursor,
                        size_t cursor_len, SDF_cguid_t cguid,
                        char **key, int max_key_len, int *key_len,
                        SDF_time_t *exptime, SDF_time_t *createtime,
                        uint64_t *seqno, void **data, size_t *data_len);


/**
 * @brief Get iteration cursors synchronously
 *
 * This is a thin wrapper around #rr_get_iteration_cursors_async
 *
 * @param src_node <IN> Source node
 *
 * @param dest_node <IN> Destination node
 *
 * @param sguid <IN> data shard GUID
 *
 * @param seqno_start <IN> start of sequence number range, inclusive
 *
 * @param seqno_len   <IN> max number of cursors to return at a time
 *
 * @param seqno_max   <IN> end of sequence number range, inclusive
 *
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t rr_get_iteration_cursors_sync(sdf_replicator_send_msg_cb_t send_closure,
                                           vnode_t src_node, vnode_t dest_node,
					   SDF_shardid_t shard,
                                           uint64_t seqno_start,
                                           uint64_t seqno_len,
                                           uint64_t seqno_max,
                                           const void *cursor, int cursor_size,
                                           struct flashGetIterationOutput **output);


/**
 * @brief Get by cursor asynchronously
 *
 * @param rr <IN> Replicator structure
 *
 * @param sguid <IN> data shard GUID
 *
 * @param src_node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param dest_node <IN> Node from which this request is going
 *
 *
 * @param cursor     <IN> Opaque cursor data
 * @param cursor_len <IN> Cursor length
 *
 * @param cb <IN> applied on completion.
 */
void rr_get_by_cursor_async(struct sdf_replicator *rr,
                            SDF_container_meta_t *cmeta,
                            sdf_replicator_send_msg_cb_t send_closure,
                            SDF_shardid_t shard, vnode_t src_node,
			    vnode_t dest_node, const void *cursor,
			    size_t cursor_len,
                            rr_get_by_cursor_cb_t cb);

/**
 * @brief Get by cursor synchronously
 *
 * This is a thin wrapper around #rr_get_iteration_cursors_async
 *
 * @param rr <IN> Replicator structure
 *
 * @param sguid <IN> data shard GUID
 *
 * @param src_node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param dest_node <IN> Node from which this request is going
 *
 *
 * @param cursor      <IN> Opaque cursor data
 * @param cursor_len  <IN> Cursor length
 * @param max_key_len <OUT> Maximum key length
 *
 * @param key         <OUT> Key (points to buffer of length max_key_len provided by caller)
 * @param key_len     <OUT> Key length
 * @param exptime     <OUT> Expiry time
 * @param createtime  <OUT> Create time
 * @param seqno       <OUT> Sequence number
 * @param data        <OUT> Data, must free with free_cb.
 * @param data_len    <OUT> Data length
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t rr_get_by_cursor_sync(struct sdf_replicator *rr,
                                   SDF_container_meta_t *cmeta,
                                   sdf_replicator_send_msg_cb_t send_closure,
                                   SDF_shardid_t shard, vnode_t src_node,
				   vnode_t dest_node, void *cursor,
				   size_t cursor_len, char *key,
				   int max_key_len, int *key_len,
                                   SDF_time_t *exptime, SDF_time_t *createtime,
                                   uint64_t *seqno,
                                   void **data, size_t *data_len,
                                   rr_read_data_free_cb_t *free_cb);


/**
 * @brief Get iteration cursors for #rr_get_by_cursor
 *
 * This is somewhat expedient for the current replication implementation
 * where we're often
 *
 * @param send_closure <IN> closure for sending a message
 *
 * @param src_node <IN> Source node
 *
 * @param dest_node <IN> Destination node
 *
 * @param sguid <IN> data shard GUID
 *
 * @param cb <IN> applied on completion.
 */
void rr_get_msg_by_cursor_async(sdf_replicator_send_msg_cb_t send_closure,
                                vnode_t src_node, vnode_t dest_node, SDF_shardid_t shard,
                                void *cursor, int cursor_len,
                                rr_get_msg_by_cursor_cb_t cb);


uint64_t rr_get_seqno_rpc(struct sdf_replicator *replicator,
                          SDF_vnode_t mynode, struct shard *shard);

/*
 *
 *   Drew's original interface stuff
 *
 */
#ifdef notdef

/**
 * @brief Callback for #rr_get_last_seqno_async
 *
 * @param status <IN> SDF_SUCCESS on success, otherwise on failure
 * @param seqno <IN> highest sequence number seen of write acknowledged to
 * user.
 */
PLAT_CLOSURE2(rr_last_seqno_cb, SDF_status_t, status, uint64_t, seqno)

/**
 * @brief Callback for #rr_get_iteration_cursors_async
 *
 * @param status <IN> SDF_SUCCESS on success, otherwise on failure
 * @param output <IN> iteration cursors, NULL on EOF, free with
 * #rr_get_iteration_cursors_free
 */
PLAT_CLOSURE2(rr_get_iteration_cursors_cb,
              SDF_status_t, status,
              struct flashGetIterationOutput *, output);

/**
 * @brief Get last sequence number asynchronously
 *
 * @param send_closure <IN> closure for sending a message
 *
 * @param node <IN> Destination node
 *
 * @param sguid <IN> data shard GUID
 *
 * @param cb <IN> applied on completion.
 */
void rr_get_last_seqno_async(sdf_send_msg_cb_t send_closure,
                             vnode_t node, SDF_shardid_t sguid,
                             rr_last_seqno_cb_t cb);

/**
 * @brief Get iteration cursors for #rr_get_by_cursor
 *
 * @param send_closure <IN> closure for sending a message
 *
 * @param node <IN> Destination node
 *
 * @param sguid <IN> data shard GUID
 *
 * @param cb <IN> applied on completion.
 */
void rr_get_iteration_cursors_async(sdf_send_msg_cb_t send_closure,
                                    vnode_t node, SDF_shardid_t shard,
                                    uint64_t seqno_start, uint64_t seqno_len,
                                    uint64_t seqno_max,
                                    const void *resumeCursorDataIn,
                                    int resumeCursorLenIn,
                                    rr_get_iteration_cursors_cb_t cb);


#endif   // end of Drew's original interface stuff

/** @brief free return from #rr_get_iteration_cursors_async */
void rr_get_iteration_cursors_free(struct flashGetIterationOutput *out);

#endif /* def REPLICATION_RPC_H */
