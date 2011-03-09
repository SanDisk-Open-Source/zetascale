/*
 * File:   rpc.c
 * Author: Brian O'Krafka
 *
 * Created on May 23, 2009
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: rpc.c 9646 2009-05-22 05:18:20Z lzwei $
 *
 */

#include "sys/queue.h"

#include "platform/assert.h"
#include "platform/closure.h"
#include "platform/logging.h"
#include "platform/mbox_scheduler.h"
#include "platform/stdlib.h"

#include "fth/fth.h"

#include "common/sdftypes.h"
#include "protocol/protocol_common.h"
/* XXX: Move to protocol/flash.h? to resolve link order */
#include "protocol/home/home_flash.h"
/* XXX: Move to protocol/util.h? to resolve link order */
#include "protocol/home/home_util.h"
#include "protocol/replication/replicator.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_msg_types.h"
#include "sdfmsg/sdf_msg_wrapper.h"

#include "shared/container_meta.h"
#include "shared/shard_meta.h"
#include "ssd/ssd_local.h"

#include "protocol/replication/replicator_adapter.h"

#include "meta_types.h"
#include "copy_replicator.h"
#include "copy_replicator_internal.h"
#include "rpc.h"

#include <sys/time.h>

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "seqno");

#define LOG_ID          PLAT_LOG_ID_INITIAL
#define LOG_DBG         PLAT_LOG_LEVEL_DEBUG
#define LOG_DIAG        PLAT_LOG_LEVEL_DIAGNOSTIC
#define LOG_INFO        PLAT_LOG_LEVEL_INFO
#define LOG_ERR         PLAT_LOG_LEVEL_ERROR
#define LOG_WARN        PLAT_LOG_LEVEL_WARN
#define LOG_TRACE       PLAT_LOG_LEVEL_TRACE
#define LOG_FATAL       PLAT_LOG_LEVEL_FATAL

struct rr_request_state {
    /** @brief response callback for last seqno */
    rr_last_seqno_cb_t last_seqno_closure;

    /** @brief response callback for get seqno */
    rr_get_seqno_cb_t get_seqno_closure;

    /** @brief response callback for get cursors */
    rr_get_iteration_cursors_cb_t get_iteration_cursors_closure;

    /** @brief response callback for get by cursor */
    rr_get_by_cursor_cb_t get_by_cursor_closure;

    /** @brief response callback for get msg by cursor */
    rr_get_msg_by_cursor_cb_t get_msg_by_cursor_closure;

    /** @brief dynamically allocated mbx structure */
    struct sdf_fth_mbx *mbx;

    /** @brief timeout usecs */
    int64_t timeout_usecs;
};

struct rr_last_seqno_sync_state {
    /** @brief Response mbx */
    fthMbox_t mbox;

    /** @brief Enumerate all the response closure fields */
    SDF_status_t status;
    uint64_t     seqno;
};

struct rr_get_seqno_sync_state {
    /** @brief Response mbx */
    fthMbox_t mbox;

    /** @brief Enumerate all the response closure fields */
    SDF_status_t status;
    uint64_t     seqno;
};

struct rr_get_iteration_cursors_sync_state {
    /** @brief Response mbx */
    fthMbox_t mbox;

    /** @brief Enumerate all the response closure fields */
    SDF_status_t status;

    struct flashGetIterationOutput *out;
};

struct rr_get_by_cursor_sync_state {
    /** @brief Response mbx */
    fthMbox_t mbox;

    /** @brief Enumerate all the response closure fields */
    SDF_status_t status;

    char              *key;
    int                key_len;
    int                max_key_len;
    SDF_time_t         exptime;
    SDF_time_t         createtime;
    uint64_t           seqno;
    void              *data;
    size_t             data_len;
    rr_read_data_free_cb_t free_cb;
};

/**
 *  @brief Process framework user operation response for last_seqno
 */
static void
rr_last_seqno_response(struct plat_closure_scheduler *context, void *env,
                       struct sdf_msg_wrapper *response);

/**
 *  @brief Process framework user operation response for get cursors
 */
static void
rr_get_iteration_cursors_response(struct plat_closure_scheduler *context,
                                  void *env, struct sdf_msg_wrapper *response);

/**
 *  @brief Process framework user operation response for get by cursor
 */
static void
rr_get_by_cursor_response(struct plat_closure_scheduler *context, void *env,
                          struct sdf_msg_wrapper *response);

static void
rr_get_msg_by_cursor_response(struct plat_closure_scheduler *context, void *env,
                              struct sdf_msg_wrapper *response);

static void
rr_last_seqno_sync_cb(plat_closure_scheduler_t *context, void *env,
                      SDF_status_t status, uint64_t seqno);

static void
rr_get_iteration_cursors_sync_cb(plat_closure_scheduler_t *context,
                                 void *env, SDF_status_t status,
                                 struct flashGetIterationOutput *out);

static void
rr_get_by_cursor_sync_cb(plat_closure_scheduler_t *context, void *env,
                         SDF_status_t status, const void * data,
                         size_t data_len, char *key, int key_len,
                         SDF_time_t exptime, SDF_time_t createtime,
                         uint64_t seqno, rr_read_data_free_cb_t free_cb);

static void
rr_msg_free(plat_closure_scheduler_t *context, void *env, struct sdf_msg *msg);

void
rr_read_free(plat_closure_scheduler_t *context, void *env, const void *data,
             size_t data_len);

static void
rr_req_state_free(struct rr_request_state *request_state);

static void
rr_send_msg(plat_closure_scheduler_t *context, void *env,
            struct sdf_msg_wrapper *msg_wrapper,
            struct sdf_fth_mbx *ar_mbx,
            SDF_status_t *out);

void
rr_get_last_seqno_async(sdf_replicator_send_msg_cb_t send_closure,
                        vnode_t src_node, vnode_t dest_node, SDF_shardid_t shard,
                        rr_last_seqno_cb_t cb)
{
    SDF_protocol_msg_t *pm = NULL;
    struct sdf_msg *msg = NULL;
    struct sdf_msg_wrapper *wrapper = NULL;
    sdf_msg_wrapper_free_local_t local_free;
    uint32_t msg_len;
    struct rr_request_state *request_state;
    sdf_msg_recv_wrapper_t recv_cb;


    msg = sdf_msg_calloc(sizeof(*msg) + sizeof(SDF_protocol_msg_t));
    plat_assert(msg);
    msg_len = sizeof(*msg) + sizeof(SDF_protocol_msg_t);

    msg->msg_len = msg_len;

    pm = (SDF_protocol_msg_t *)msg->msg_payload;
    pm->msgtype = HFGLS;
    pm->shard = shard;

    /* send a start message to framework */

    plat_calloc_struct(&request_state);
    plat_assert(request_state);
    request_state->last_seqno_closure = cb;
    request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_ONLY_ERROR;

    recv_cb =
        sdf_msg_recv_wrapper_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                    &rr_last_seqno_response, request_state),
    request_state->mbx =
        sdf_fth_mbx_resp_closure_alloc(recv_cb,
                                       /* XXX: release arg goes away */
                                       SACK_REL_YES,
                                       request_state->timeout_usecs);
    plat_assert(request_state->mbx);

    /* Wrapper as a sdf_msg_wrapper */
    local_free =
        sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                          &rr_msg_free, NULL);
    wrapper =
        sdf_msg_wrapper_local_alloc(msg, local_free, SMW_MUTABLE_FIRST,
                                    SMW_TYPE_REQUEST,
                                    src_node /* src */, SDF_RESPONSES,
                                    dest_node /* dest */, SDF_FLSH,
                                    FLSH_REQUEST, NULL /* not a response */);
    plat_assert(wrapper);
    /* send to dest node */
    plat_closure_apply(sdf_replicator_send_msg_cb, &send_closure, wrapper,
                       request_state->mbx, NULL);
}

SDF_status_t
rr_get_last_seqno_sync(sdf_replicator_send_msg_cb_t send_closure, vnode_t src_node,
                       vnode_t dest_node, SDF_shardid_t sguid, uint64_t *pseqno)
{
    SDF_status_t ret;
    struct rr_last_seqno_sync_state *state;
    plat_alloc_struct(&state);
    plat_assert(state);
    rr_last_seqno_cb_t cb;

    fthMboxInit(&state->mbox);
    cb = rr_last_seqno_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                 &rr_last_seqno_sync_cb, state);
    rr_get_last_seqno_async(send_closure, src_node, dest_node, sguid, cb);

    fthMboxWait(&state->mbox);
    if (state->status == SDF_SUCCESS) {
        *pseqno = state->seqno;
    } else {
        plat_log_msg(21473, LOG_CAT, LOG_FATAL,
                     "get last sequence number failed");
        ret = SDF_FAILURE;
    }
    ret = state->status;
    plat_free(state);
    return (ret);
}

void
rr_get_iteration_cursors_async(sdf_replicator_send_msg_cb_t send_closure,
                               vnode_t src_node, vnode_t dest_node, SDF_shardid_t shard,
                               uint64_t seqno_start, uint64_t seqno_len,
                               uint64_t seqno_max,
                               const void *cursor, int cursor_size,
                               rr_get_iteration_cursors_cb_t cb)
{
    SDF_protocol_msg_t *pm = NULL;
    struct sdf_msg *response_msg = NULL;
    struct sdf_msg_wrapper *wrapper = NULL;
    sdf_msg_wrapper_free_local_t local_free;
    uint32_t msg_len;
    struct rr_request_state *request_state;
    sdf_msg_recv_wrapper_t recv_cb;

    msg_len = sizeof(*response_msg) + sizeof(SDF_protocol_msg_t) + cursor_size;
    response_msg = sdf_msg_calloc(msg_len);

    response_msg->msg_len = msg_len;

    pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
    pm->msgtype   = HFGIC;
    pm->shard     = shard;
    pm->seqno     = seqno_start;
    pm->seqno_len = seqno_len;
    pm->seqno_max = seqno_max;
    pm->data_size = cursor_size;
    if (cursor) {
        memcpy((char *)pm + sizeof(SDF_protocol_msg_t), cursor, cursor_size);
    }

    /* send a start message to framework */
    plat_calloc_struct(&request_state);
    plat_assert(request_state);
    request_state->get_iteration_cursors_closure = cb;
    request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_ONLY_ERROR;

    recv_cb =
        sdf_msg_recv_wrapper_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                    &rr_get_iteration_cursors_response,
                                    request_state),

    request_state->mbx =
        sdf_fth_mbx_resp_closure_alloc(recv_cb,
                                       /* XXX: release arg goes away */
                                       SACK_REL_YES,
                                       request_state->timeout_usecs);
    plat_assert(request_state->mbx);

    /* Wrapper as a sdf_msg_wrapper */
    local_free =
        sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                          &rr_msg_free, NULL);
    wrapper =
        sdf_msg_wrapper_local_alloc(response_msg, local_free, SMW_MUTABLE_FIRST,
                                    SMW_TYPE_REQUEST,
                                    src_node /* src */, SDF_RESPONSES,
                                    dest_node /* dest */, SDF_FLSH /* dest */,
                                    FLSH_REQUEST, NULL /* not a response */);
    plat_assert(wrapper);
    /* send to dest node */
    plat_closure_apply(sdf_replicator_send_msg_cb, &send_closure, wrapper,
                       request_state->mbx, NULL);
}

SDF_status_t
rr_get_iteration_cursors_sync(sdf_replicator_send_msg_cb_t send_closure,
                              vnode_t src_node, vnode_t dest_node,
                              SDF_shardid_t shard, uint64_t seqno_start,
                              uint64_t seqno_len, uint64_t seqno_max,
                              const void *cursor, int cursor_size,
                              struct flashGetIterationOutput **out)
{
    SDF_status_t ret;
    struct rr_get_iteration_cursors_sync_state *state;
    plat_alloc_struct(&state);
    plat_assert(state);
    rr_get_iteration_cursors_cb_t cb;

    fthMboxInit(&state->mbox);

    cb = rr_get_iteration_cursors_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                            &rr_get_iteration_cursors_sync_cb,
                                            state);
    rr_get_iteration_cursors_async(send_closure, src_node, dest_node, shard, seqno_start,
                                   seqno_len, seqno_max, cursor, cursor_size,
                                   cb);

    fthMboxWait(&state->mbox);
    *out = state->out;
    ret = state->status;
    if (ret != SDF_SUCCESS) {
        plat_log_msg(21474, LOG_CAT, LOG_WARN,
                     "get cursors failed");
    }
    plat_free(state);
    return (ret);
}

void
rr_get_iteration_cursors_free(struct flashGetIterationOutput *out) {
    plat_free(out);
}

/**
 * @brief Get by cursor asynchronously
 *
 * @param rr <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param sguid <IN> data shard GUID
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param cursor     <IN> Opaque cursor data
 * @param cursor_len <IN> Cursor length
 *
 * @param cb <IN> applied on completion.
 */
void
rr_get_by_cursor_async(struct sdf_replicator *rr,
                       SDF_container_meta_t *cmeta,
                       sdf_replicator_send_msg_cb_t send_closure,
                       SDF_shardid_t shard, vnode_t src_node,
                       vnode_t dest_node, const void *cursor,
                       size_t cursor_len, rr_get_by_cursor_cb_t cb)
{
    SDF_status_t status = SDF_FALSE;
    int failed = 0;
    SDF_protocol_msg_t *pm = NULL;
    struct sdf_msg *response_msg = NULL;
    struct sdf_msg_wrapper *wrapper = NULL;
    sdf_msg_wrapper_free_local_t local_free;
    uint32_t msg_len;
    struct rr_request_state *request_state;

    msg_len = sizeof(*response_msg) + sizeof(SDF_protocol_msg_t) + cursor_len;
    response_msg = sdf_msg_calloc(msg_len);

    response_msg->msg_len = msg_len;

    pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
    pm->msgtype   = HFGBC;
    pm->shard     = shard;
    pm->data_size = cursor_len;
    memcpy((char *)pm + sizeof(SDF_protocol_msg_t), cursor, cursor_len);

    if (SDF_SUCCESS != (*rr->get_op_meta_fn)(rr, cmeta, shard, &pm->op_meta)) {
        plat_log_msg(21475, LOG_CAT, LOG_FATAL,
                     "get op_meta failed");
        failed = 1;
    } else {
        /* send a start message to framework */
        if (plat_calloc_struct(&request_state)) {
            request_state->get_by_cursor_closure = cb;
            request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_NONE;

            request_state->mbx =
            sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                                       &rr_get_by_cursor_response,
                                                                       request_state),
                                           /* XXX: release arg goes away */
                                           SACK_REL_YES,
                                           request_state->timeout_usecs);

            if (!request_state->mbx) {
                status = SDF_FAILURE_MEMORY_ALLOC;
            }
        } else {
            status = SDF_FAILURE_MEMORY_ALLOC;
        }
        /* Wrapper as a sdf_msg_wrapper */
        local_free = sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                       &rr_msg_free, NULL);
        wrapper =
            sdf_msg_wrapper_local_alloc(response_msg, local_free,
                                        SMW_MUTABLE_FIRST,
                                        SMW_TYPE_REQUEST,
                                        src_node /* src */, SDF_RESPONSES,
                                        dest_node /* dest */, SDF_FLSH /* dest */,
                                        FLSH_REQUEST, NULL /* not a response */);
        /* send to dest node */
        plat_closure_apply(sdf_replicator_send_msg_cb, &send_closure, wrapper,
                           request_state->mbx, &status);
    }

    /*
     * XXX: drew 2009-08-28 It's not smart to replace a meaningful error
     * status with the never insightful SDF_FAILURE.
     */

    if (failed) {
        /* set a response to sync_functions */
        rr_read_data_free_cb_t free_cb =
            rr_read_data_free_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                        &rr_read_free, NULL);
        plat_closure_apply(rr_get_by_cursor_cb, &cb,
                           SDF_FAILURE, NULL, 0, NULL, 0, 0, 0,
                           SDF_SEQUENCE_NO_INVALID,
                           free_cb);
    }
}

/**
 * @brief Get by cursor asynchronously
 *
 * @param rr <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param sguid <IN> data shard GUID
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param cursor     <IN> Opaque cursor data
 * @param cursor_len <IN> Cursor length
 *
 * @param cb <IN> applied on completion.
 */
void rr_get_msg_by_cursor_async(sdf_replicator_send_msg_cb_t send_closure,
                                vnode_t src_node, vnode_t dest_node,
                                SDF_shardid_t shard, void *cursor,
                                int cursor_len,
                                rr_get_msg_by_cursor_cb_t cb)
{
    SDF_protocol_msg_t *pm = NULL;
    struct sdf_msg *msg = NULL;
    struct sdf_msg_wrapper *wrapper = NULL;
    sdf_msg_wrapper_free_local_t local_free;
    uint32_t msg_len;
    struct rr_request_state *request_state;
    sdf_msg_recv_wrapper_t recv_cb;

    msg_len = sizeof(*msg) + sizeof(SDF_protocol_msg_t) + cursor_len;
    msg = sdf_msg_calloc(msg_len);

    msg->msg_len = msg_len;

    pm = (SDF_protocol_msg_t *)msg->msg_payload;
    pm->msgtype   = HFGBC;
    pm->shard     = shard;
    pm->data_size = cursor_len;
    memcpy((char *)pm + sizeof(SDF_protocol_msg_t), cursor, cursor_len);

    /* send a start message to framework */
    plat_calloc_struct(&request_state);
    plat_assert(request_state);
    request_state->get_msg_by_cursor_closure = cb;
    request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_ONLY_ERROR;

    recv_cb =
        sdf_msg_recv_wrapper_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                    &rr_get_msg_by_cursor_response,
                                    request_state),
    request_state->mbx =
        sdf_fth_mbx_resp_closure_alloc(recv_cb,
                                       /* XXX: release arg goes away */
                                       SACK_REL_YES,
                                       request_state->timeout_usecs);
    plat_assert(request_state->mbx);
    /* Wrapper as a sdf_msg_wrapper */
    local_free =
        sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                          &rr_msg_free, NULL);
    wrapper =
        sdf_msg_wrapper_local_alloc(msg, local_free, SMW_MUTABLE_FIRST,
                                    SMW_TYPE_REQUEST,
                                    src_node /* src */, SDF_RESPONSES,
                                    dest_node /* dest */, SDF_FLSH /* dest */,
                                    FLSH_REQUEST, NULL /* not a response */);
    /* send to dest node */
    plat_closure_apply(sdf_replicator_send_msg_cb, &send_closure, wrapper,
                       request_state->mbx, NULL);
}

/**
 * @brief Get by cursor synchronously
 *
 * This is a thin wrapper around #rr_get_iteration_cursors_async
 *
 * @param rr <IN> Replicator structure
 *
 * @param sguid <IN> data shard GUID
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
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
SDF_status_t
rr_get_by_cursor_sync(struct sdf_replicator *rr,
                      SDF_container_meta_t *cmeta,
                      sdf_replicator_send_msg_cb_t send_closure,
                      SDF_shardid_t shard, vnode_t src_node,
                      vnode_t dest_node, void *cursor,
                      size_t cursor_len, char *key,
                      int max_key_len, int *key_len,
                      SDF_time_t *exptime, SDF_time_t *createtime,
                      uint64_t *seqno, void **data, size_t *data_len,
                      rr_read_data_free_cb_t *free_cb)
{
    SDF_status_t ret;
    struct rr_get_by_cursor_sync_state *state;
    plat_alloc_struct(&state);
    plat_assert(state);
    rr_get_by_cursor_cb_t cb;

    state->max_key_len = max_key_len;
    state->key         = key;

    fthMboxInit(&state->mbox);
    cb = rr_get_by_cursor_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                    &rr_get_by_cursor_sync_cb, state);
    rr_get_by_cursor_async(rr, cmeta, send_closure, shard, src_node, dest_node,
                           cursor, cursor_len, cb);

    fthMboxWait(&state->mbox);
    if (state->status == SDF_SUCCESS) {
        // key has already been loaded via state->key
        *key_len    = state->key_len;
        *exptime    = state->exptime;
        *createtime = state->createtime;
        *seqno      = state->seqno;
        *data_len = state->data_len;
        *free_cb = state->free_cb;
        *data = plat_alloc(state->data_len);
        memcpy(*data, state->data, state->data_len);
        /* free data */
        plat_closure_apply(rr_read_data_free_cb, &state->free_cb, state->data, state->data_len);
    } else if (state->status == SDF_OBJECT_UNKNOWN) {
        // key has already been loaded via state->key
        *key_len    = state->key_len;
        *exptime    = state->exptime;
        *createtime = state->createtime;
        *seqno      = state->seqno;
    } else {
        plat_log_msg(21476, LOG_CAT, LOG_TRACE,
                     "get by cursor failed");
        ret = SDF_FAILURE;
    }
    ret = state->status;
    plat_free(state);
    return (ret);
}

/**
 *  @brief Process framework user operation response for last_seqno
 */
static void
rr_last_seqno_response(struct plat_closure_scheduler *context, void *env,
                       struct sdf_msg_wrapper *response)
{
    struct sdf_msg *response_msg = NULL;
    SDF_status_t status;
    uint64_t     seqno = SDF_SEQUENCE_NO_INVALID;
    SDF_protocol_msg_t *pm;
    struct rr_request_state *request_state =
        (struct rr_request_state *)env;

    sdf_msg_wrapper_rwref(&response_msg, response);
    status = sdf_msg_get_error_status(response_msg);
    if (status == SDF_SUCCESS) {
        pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
        status = pm->status;
        if (status == SDF_SUCCESS) {
            seqno = pm->seqno;
        }
    }

    /* apply read complete for both sync and async */

    plat_closure_chain(rr_last_seqno_cb, context,
                       &request_state->last_seqno_closure, status, seqno);

    sdf_msg_wrapper_ref_count_dec(response);
    rr_req_state_free(request_state);
}

/**
 *  @brief Process framework user operation response for last_seqno
 */
static void
rr_get_seqno_response(struct plat_closure_scheduler *context, void *env,
                      struct sdf_msg_wrapper *response)
{
    struct sdf_msg *response_msg = NULL;
    SDF_status_t status;
    uint64_t     seqno = SDF_SEQUENCE_NO_INVALID;
    SDF_protocol_msg_t *pm;
    struct rr_request_state *request_state =
        (struct rr_request_state *)env;

    sdf_msg_wrapper_rwref(&response_msg, response);
    status = sdf_msg_get_error_status(response_msg);
    if (status == SDF_SUCCESS) {
        pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
        status = pm->status;
        if (status == SDF_SUCCESS) {
            seqno = pm->seqno;
        }
    }

    /* apply read complete for both sync and async */

    plat_closure_chain(rr_get_seqno_cb, context,
                       &request_state->get_seqno_closure, status, seqno);

    sdf_msg_wrapper_ref_count_dec(response);
    rr_req_state_free(request_state);
}

/**
 *  @brief Process framework user operation response for get cursors
 */
static void
rr_get_iteration_cursors_response(struct plat_closure_scheduler *context,
                                  void *env, struct sdf_msg_wrapper *response)
{
    struct sdf_msg *response_msg = NULL;
    SDF_status_t status;
    SDF_protocol_msg_t *pm;
    struct rr_request_state *request_state = (struct rr_request_state *)env;
    struct flashGetIterationOutput *out = NULL;

    sdf_msg_wrapper_rwref(&response_msg, response);
    status = sdf_msg_get_error_status(response_msg);
    if (status == SDF_SUCCESS) {
        pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
        status = pm->status;
        if (status == SDF_SUCCESS) {
            out = (struct flashGetIterationOutput *)plat_alloc(pm->data_size);
            memcpy(out, ((char *)((char *)pm+sizeof(*pm))), pm->data_size);
        }
    }

    /* apply read complete for both sync and async */

    plat_closure_chain(rr_get_iteration_cursors_cb, context,
                       &request_state->get_iteration_cursors_closure, status,
                       out);
    sdf_msg_wrapper_ref_count_dec(response);
    rr_req_state_free(request_state);
}

/**
 *  @brief Process framework user operation response for get by cursor
 */
static void
rr_get_by_cursor_response(struct plat_closure_scheduler *context, void *env,
                          struct sdf_msg_wrapper *response)
{
    struct sdf_msg *response_msg = NULL;
    SDF_status_t status;
    SDF_protocol_msg_t *pm;
    struct rr_request_state *request_state =
        (struct rr_request_state *)env;
    void *data;
    size_t data_len;
    char  *key;
    int    key_len;
    SDF_time_t  exptime;
    SDF_time_t  createtime;
    uint64_t    seqno;

    sdf_msg_wrapper_rwref(&response_msg, response);
    pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
    status = sdf_msg_get_error_status(response_msg);
    if (status == SDF_SUCCESS) {
        status = pm->status;
    }

    if (status == SDF_SUCCESS) {
        key        = plat_alloc(pm->key.len+1);
        plat_assert(key);
        memcpy(key, pm->key.key, pm->key.len+1);
        key_len    = pm->key.len;
        exptime    = pm->exptime;
        createtime = pm->createtime;
        seqno      = pm->seqno;
        data_len   = pm->data_size;
        data       = plat_alloc(data_len);
        plat_assert(data);
        memcpy(data, ((char *)((char *)pm+sizeof(*pm))), data_len);
    } else if (status == SDF_OBJECT_UNKNOWN) {
        key        = plat_alloc(pm->key.len+1);
        plat_assert(key);
        memcpy(key, pm->key.key, pm->key.len+1);
        key_len    = pm->key.len;
        exptime = 0;
        createtime = 0;
        seqno = pm->seqno;
        data = NULL;
        data_len = 0;
    } else {
        key     = NULL;
        key_len = 0;
        exptime = 0;
        createtime = 0;
        seqno = SDF_SEQUENCE_NO_INVALID;
        data = NULL;
        data_len = 0;
    }

    rr_read_data_free_cb_t free_cb =
        rr_read_data_free_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                    &rr_read_free, env);

    /* apply read complete for both sync and async */
    plat_closure_chain(rr_get_by_cursor_cb, context,
                       &request_state->get_by_cursor_closure, status,
                       data /* data */, data_len /* data_len */,
                       key, key_len, exptime, createtime, seqno,
                       free_cb /* read_free_closure */);
    sdf_msg_wrapper_ref_count_dec(response);
    rr_req_state_free(request_state);
}

/**
 *  @brief Process framework user operation response for get by cursor
 */
static void
rr_get_msg_by_cursor_response(struct plat_closure_scheduler *context, void *env,
                              struct sdf_msg_wrapper *response)
{
    struct rr_request_state *request_state = (struct rr_request_state *)env;
    rr_get_msg_by_cursor_cb_t cb = request_state->get_msg_by_cursor_closure;

    rr_req_state_free(request_state);

    plat_closure_apply(rr_get_msg_by_cursor_cb, &cb, response);
}

static void
rr_last_seqno_sync_cb(plat_closure_scheduler_t *context, void *env,
                      SDF_status_t status, uint64_t seqno)
{
    struct rr_last_seqno_sync_state *state =
        (struct rr_last_seqno_sync_state *)env;
    state->seqno = seqno;
    state->status = status;
    plat_log_msg(21477, LOG_CAT, LOG_TRACE,
                 "send completed to last_seqno sync");
    fthMboxPost(&state->mbox, 0 /* value doesn't matter */);
}

static void
rr_get_seqno_sync_cb(plat_closure_scheduler_t *context, void *env,
                     SDF_status_t status, uint64_t seqno)
{
    struct rr_get_seqno_sync_state *state =
        (struct rr_get_seqno_sync_state *)env;
    state->seqno = seqno;
    state->status = status;
    plat_log_msg(21477, LOG_CAT, LOG_TRACE,
                 "send completed to last_seqno sync");
    fthMboxPost(&state->mbox, 0 /* value doesn't matter */);
}

static void
rr_get_iteration_cursors_sync_cb(plat_closure_scheduler_t *context, void *env,
                                 SDF_status_t status,
                                 struct flashGetIterationOutput *out) {
    struct rr_get_iteration_cursors_sync_state *state =
        (struct rr_get_iteration_cursors_sync_state *)env;
    state->status = status;
    state->out = out;
    plat_log_msg(21478, LOG_CAT, LOG_TRACE,
                 "send completed to get cursors");
    fthMboxPost(&state->mbox, 0 /* value doesn't matter */);
}

static void
rr_get_by_cursor_sync_cb(plat_closure_scheduler_t *context, void *env,
                         SDF_status_t status, const void *data, size_t data_len,
                         char *key, int key_len, SDF_time_t exptime,
                         SDF_time_t createtime, uint64_t seqno,
                         rr_read_data_free_cb_t free_cb)
{
    struct rr_get_by_cursor_sync_state *state = (struct rr_get_by_cursor_sync_state *)env;

    if (key != NULL) {
        if (key_len < state->max_key_len) {
            (void) strncpy(state->key, key, key_len + 1);
        } else {
            (void) strncpy(state->key, key, state->max_key_len);
        }
        plat_free(key);
    }
    state->key_len = key_len;
    state->exptime = exptime;
    state->createtime = createtime;
    state->seqno = seqno;
    state->data = (void *)data;
    state->data_len = data_len;
    state->status = status;
    state->free_cb = free_cb;
    plat_log_msg(21479, LOG_CAT, LOG_TRACE,
                 "send completed to get by cursor");
    fthMboxPost(&state->mbox, 0 /* value doesn't matter */);
}

void
rr_get_seqno_async(sdf_replicator_send_msg_cb_t send_closure,
                   vnode_t src_node, vnode_t dest_node, SDF_shardid_t shard,
                   rr_get_seqno_cb_t cb)
{
    SDF_protocol_msg_t *pm = NULL;
    struct sdf_msg *msg = NULL;
    struct sdf_msg_wrapper *wrapper = NULL;
    sdf_msg_wrapper_free_local_t local_free;
    uint32_t msg_len;
    struct rr_request_state *request_state;
    sdf_msg_recv_wrapper_t recv_cb;


    msg = sdf_msg_calloc(sizeof(*msg) + sizeof(SDF_protocol_msg_t));
    plat_assert(msg);
    msg_len = sizeof(*msg) + sizeof(SDF_protocol_msg_t);

    msg->msg_len = msg_len;

    pm = (SDF_protocol_msg_t *)msg->msg_payload;
    pm->msgtype = HFGSN;
    pm->shard = shard;

    /* send a start message to framework */

    plat_calloc_struct(&request_state);
    plat_assert(request_state);
    request_state->get_seqno_closure = cb;
    request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_ONLY_ERROR;

    recv_cb =
        sdf_msg_recv_wrapper_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                    &rr_get_seqno_response, request_state),
    request_state->mbx =
        sdf_fth_mbx_resp_closure_alloc(recv_cb,
                                       /* XXX: release arg goes away */
                                       SACK_REL_YES,
                                       request_state->timeout_usecs);
    plat_assert(request_state->mbx);

    /* Wrapper as a sdf_msg_wrapper */
    local_free =
        sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                          &rr_msg_free, NULL);
    wrapper =
        sdf_msg_wrapper_local_alloc(msg, local_free,
                                    SMW_MUTABLE_FIRST,
                                    SMW_TYPE_REQUEST,
                                    src_node /* src */, SDF_RESPONSES,
                                    dest_node /* dest */, SDF_FLSH,
                                    FLSH_REQUEST, NULL /* not a response */);
    plat_assert(wrapper);
    /* send to dest node */
    plat_closure_apply(sdf_replicator_send_msg_cb, &send_closure, wrapper,
                       request_state->mbx, NULL);
}

SDF_status_t
rr_get_seqno_sync(sdf_replicator_send_msg_cb_t send_closure, vnode_t src_node,
                  vnode_t dest_node, SDF_shardid_t sguid, uint64_t *pseqno)
{
    SDF_status_t ret;
    struct rr_get_seqno_sync_state *state;
    plat_alloc_struct(&state);
    plat_assert(state);
    rr_get_seqno_cb_t cb;

    fthMboxInit(&state->mbox);
    cb = rr_get_seqno_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                &rr_get_seqno_sync_cb, state);
    rr_get_seqno_async(send_closure, src_node, dest_node, sguid, cb);

    fthMboxWait(&state->mbox);
    if (state->status == SDF_SUCCESS) {
        *pseqno = state->seqno;
    } else {
        plat_log_msg(21473, LOG_CAT, LOG_FATAL,
                     "get last sequence number failed");
        ret = SDF_FAILURE;
    }
    ret = state->status;
    plat_free(state);
    return (ret);
}

uint64_t rr_get_seqno_rpc(struct sdf_replicator *replicator,
                          SDF_vnode_t mynode, struct shard *shard)
{
    sdf_replicator_send_msg_cb_t send_msg;
    uint64_t seqno;
    int rc;

    send_msg = sdf_replicator_send_msg_cb_create(PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS,
                                                 &rr_send_msg, NULL);

    rc = rr_get_seqno_sync(send_msg, mynode, mynode, shard->shardID, &seqno);
    if (rc != SDF_SUCCESS) {
        plat_log_msg(21480, LOG_CAT, LOG_FATAL,
                     "get sequence number failed");
        return (0);
    }

    return (seqno);
}

/**
 * @brief Free sdf_msg
 */
static void
rr_msg_free(plat_closure_scheduler_t *context, void *env, struct sdf_msg *msg)
{
    plat_assert(msg);
    sdf_msg_free(msg);
}

void
rr_read_free(plat_closure_scheduler_t *context, void *env, const void *data,
             size_t data_len) {
    plat_free((void*)data);
}

/**
 * @brief Free request state
 */
static void
rr_req_state_free(struct rr_request_state *request_state) {
    plat_assert(request_state);
    if (request_state->mbx) {
        sdf_fth_mbx_free(request_state->mbx);
    }
    plat_free(request_state);
}




/* XXX: drew 2009-06-01 move this to a separate file */
/**
 * @brief Test routine
 */

void rr_iterate_rpc_test(struct sdf_replicator *replicator,
                         SDF_container_meta_t *cmeta, SDF_vnode_t mynode,
                         struct shard *shard)
{
    void * cursors = 0;
    int cursor_size = 0;
    it_cursor_t *it_cursor;
    int rc;
    char key[SDF_SIMPLE_KEY_SIZE];
    int key_len = 0;
    SDF_time_t exptime = 0;
    SDF_time_t createtime = 0;
    uint64_t seqno = 0;

    sdf_replicator_send_msg_cb_t send_msg;
    rr_read_data_free_cb_t free_cb;

    int j, i;

    void * data = 0;
    size_t data_len = 0;
    uint64_t seqno_start = 1;
    uint64_t seqno_len = 20;
    uint64_t seqno_max = 0;


    free_cb = rr_read_data_free_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                          &rr_read_free,
                                          NULL);

    send_msg = sdf_replicator_send_msg_cb_create(PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS,
                                                 &rr_send_msg, NULL);

    rc = rr_get_last_seqno_sync(send_msg, mynode, mynode, cmeta->shard, &seqno_max);
    if (rc != SDF_SUCCESS) {
        printf("rr_get_last_seqno_sync failed\n");
        return;
    }

    printf("Last seqno: %ld\n", seqno_max);

    for (j = 0; j < 5; j++) {
        rc = rr_get_iteration_cursors_sync(send_msg, cmeta->shard, mynode,
                                           mynode, seqno_start, seqno_len,
                                           seqno_max, cursors, cursor_size,
                                           &it_cursor);
        if (rc != SDF_SUCCESS) {
            printf("get_iteration_cursors failed: %d\n", rc);
            continue;
        }

        for (i = 0; i < it_cursor->cursor_count; i++) {
            rc = rr_get_by_cursor_sync(replicator,
                                       cmeta,
                                       send_msg,
                                       cmeta->shard, mynode, mynode,
                                       (void *)&it_cursor->cursors[i*it_cursor->cursor_len],
                                       it_cursor->cursor_len,
                                       key, SDF_SIMPLE_KEY_SIZE, &key_len, &exptime, &createtime,
                                       &seqno, &data, &data_len, &free_cb);

            if (rc == SDF_SUCCESS) {
                printf("Ok:    [%d]:    \n", i);
                printf("      key_len:  %d\n", key_len);
                printf("      data_len: %ld\n", data_len);
                printf("      seqno:    %ld\n", seqno);
                printf("      create:    %d\n", createtime);
                printf("      expire:    %d\n", exptime);
            } else {
                printf("Stale: [%d]:\n", i);
            }

            fflush(stdout);
        }

        // Setup resume cursor for the next iteration call
        cursors = (void *)it_cursor;
        cursor_size = sizeof(it_cursor_t);
    }
}

/*
 * @brief Send message
 *
 * 1.  We don't do anything about the reference counts and assume
 *     that the caller will deal with that.
 *
 * 2.  The current sdf_msg interface modifies the header of the
 *     sdf_msg which was passed in.  We don't do anything to
 *     control this.
 */
static void
rr_send_msg(plat_closure_scheduler_t *context, void *env,
            struct sdf_msg_wrapper *msg_wrapper, struct sdf_fth_mbx *ar_mbx,
            SDF_status_t *status)
{
    struct sdf_msg *msg;
    SDF_protocol_msg_t *pm;
    SDF_Protocol_Msg_Info_t *pmi;
    int result;

    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, msg_wrapper);
    pm = (SDF_protocol_msg_t *)msg->msg_payload;
    pmi = &(SDF_Protocol_Msg_Info[pm->msgtype]);

    /* XXX: add access functions which sanity check msg */
    plat_log_msg(21481, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "rr iteration test send"
                 " Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d",
                 pmi->name, pmi->shortname,
                 pm->node_from, SDF_Protocol_Nodes_Info[pmi->src].name,
                 pm->node_to, SDF_Protocol_Nodes_Info[pmi->dest].name,
                 pm->tag);

    sdf_msg_wrapper_rwrelease(&msg, msg_wrapper);

    result = sdf_msg_wrapper_send(msg_wrapper, ar_mbx);

    if (status) {
        if (!result) {
            *status = SDF_SUCCESS;
        } else {
            *status = SDF_FAILURE_MSG_SEND;
        }
    }
}
