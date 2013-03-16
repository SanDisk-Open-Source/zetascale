/*
 * File:   sdf/sdfmsg/sdf_msg_wrapper.c
 * Author: drew
 *
 * Created on May 8, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_wrapper.c 1480 2008-06-05 09:23:13Z drew $
 */

/**
 * struct sdf_msg_wrapper is a thin wrapper around a message which
 * describes where it came from and who owns it with the intent of providing
 * zero-copy messaging including fan-out.
 */

#include "platform/assert.h"
#include "platform/stdlib.h"
#include <stdio.h>

#include "sdf_msg_wrapper.h"

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

enum send_action {
    SA_FREE,
    SA_DEC
};

static void
sdf_msg_wrapper_init(struct sdf_msg_wrapper *sdf_msg_wrapper,
                     enum sdf_msg_wrapper_mutability mutability,
                     enum sdf_msg_wrapper_type msg_wrapper_type,
                     vnode_t src_vnode, service_t src_service,
                     vnode_t dest_vnode, service_t dest_service,
                     msg_type_t msg_type,
                     struct sdf_resp_mbx *mresp_mbx);
static void
sdf_msg_wrapper_sdf_msg_free(plat_closure_scheduler_t *contect, void *env,
                             struct sdf_msg *msg);
static int sdf_msg_wrapper_start_send(struct sdf_msg_wrapper *wrapper,
                                      struct sdf_msg **tmp_msg,
                                      struct sdf_msg **out_msg,
                                      enum send_action *out_action);
static void sdf_msg_wrapper_end_send(struct sdf_msg_wrapper *wrapper,
                                     struct sdf_msg **tmp_msg,
                                     enum send_action out_action);

struct sdf_msg_wrapper *
sdf_msg_wrapper_local_alloc(struct sdf_msg *local,
                            sdf_msg_wrapper_free_local_t local_free,
                            enum sdf_msg_wrapper_mutability mutability,
                            enum sdf_msg_wrapper_type msg_wrapper_type,
                            vnode_t src_vnode, service_t src_service,
                            vnode_t dest_vnode, service_t dest_service,
                            msg_type_t msg_type,
                            struct sdf_resp_mbx *mresp_mbx) {
    struct sdf_msg_wrapper *ret;

    ret = plat_alloc(sizeof (*ret));
    if (ret) {
        ret->ptr.local.ptr = local;
        ret->ptr.local.mfree = local_free;
        ret->ptr_type = SMW_LOCAL;

        ret->len = PLAT_OFFSET_OF(struct sdf_msg, msg_payload) + local->msg_len;

        sdf_msg_wrapper_init(ret, mutability, msg_wrapper_type,
                             src_vnode, src_service, dest_vnode, dest_service,
                             msg_type, mresp_mbx);

    }

    return (ret);
}

struct sdf_msg_wrapper *
sdf_msg_wrapper_shared_alloc(sdf_msg_sp_t shared,
                             sdf_msg_wrapper_free_shared_t shared_free,
                             enum sdf_msg_wrapper_mutability mutability,
                             enum sdf_msg_wrapper_type msg_wrapper_type,
                             vnode_t src_vnode, service_t src_service,
                             vnode_t dest_vnode, service_t dest_service,
                             msg_type_t msg_type,
                             struct sdf_resp_mbx *mresp_mbx) {
    struct sdf_msg_wrapper *ret;
    const struct sdf_msg *msg = NULL;

    ret = plat_alloc(sizeof (*ret));
    if (ret) {
        ret->ptr.shared.ptr = shared;
        ret->ptr.shared.mfree = shared_free;
        ret->ptr_type = SMW_SHARED;

        sdf_msg_sp_var_rref(&msg, shared, sizeof (*msg));
        ret->len = PLAT_OFFSET_OF(struct sdf_msg, msg_payload) +
            msg->msg_len;
        sdf_msg_sp_var_rrelease(&msg, sizeof (*msg));

        sdf_msg_wrapper_init(ret, mutability, msg_wrapper_type, src_vnode,
                             src_service, dest_vnode, dest_service, msg_type,
                             mresp_mbx);
    }

    return (ret);
}

struct sdf_msg_wrapper *
sdf_msg_wrapper_forward_reply_alloc(struct sdf_msg_wrapper *request,
                                    struct sdf_msg_wrapper *reply) {
    struct sdf_msg_wrapper *ret;
    struct sdf_msg *send_msg;
    struct sdf_msg *request_msg;
    struct sdf_msg *reply_msg;
    sdf_msg_wrapper_free_local_t local_free;
    int alias;
    uint32_t flags;

    request_msg = NULL;
    sdf_msg_wrapper_rwref(&request_msg, request);

    reply_msg = NULL;
    sdf_msg_wrapper_rwref(&reply_msg, reply);

    plat_assert(request->msg_wrapper_type == SMW_TYPE_REQUEST);
    plat_assert(request->response_mbx_valid);
    plat_assert(reply->msg_wrapper_type == SMW_TYPE_RESPONSE);

    local_free = sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                   &sdf_msg_wrapper_sdf_msg_free, NULL);

    alias = reply->ref_count == 1;
    if (alias) {
        ret = reply;
        ret->dest_vnode = request->src_vnode;
        ret->dest_service = request->src_service;
        ret->src_vnode = request->dest_vnode;
        ret->src_service = request->dest_service;
        ret->response_mbx = request->response_mbx;
        ret->response_mbx_valid = request->response_mbx_valid;

        plat_log_msg(21486, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Alias replymsg %p req wrapper %p rep dn %d"
                     " rep ds %d ret sn %d ret ss %d\n"
                     "        ret response_mkx %p ret mkeyflags %lx\n"
                     "        mkey %lx\n",
                     ret->src_vnode, reply_msg, ret, ret->dest_vnode, ret->dest_service,
                     ret->src_vnode, ret->src_service,
                     ret->response_mbx.rbox, ret->response_mbx.resp_id,
                     reply_msg->sent_id);

    } else {
        send_msg = sdf_msg_alloc(reply_msg->msg_len - sizeof(*reply_msg));
        if (send_msg) {
            flags = send_msg->msg_flags;
            memcpy(send_msg, reply_msg, reply_msg->msg_len);

            /* Shouldn't need to add flags */
            send_msg->msg_flags = flags |
                (reply_msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_INCLUDED);

            ret = sdf_msg_wrapper_local_alloc(send_msg, local_free,
                                              SMW_MUTABLE_FIRST,
                                              SMW_TYPE_RESPONSE,
                                              request->src_vnode /* dest */,
                                              request->src_service /* dest */,
                                              request->dest_vnode /* src */,
                                              request->dest_service /* src */,
                                              reply->msg_type,
                                              &request->response_mbx);
        } else {
            ret = NULL;
        }
    }

    sdf_msg_wrapper_rwrelease(&reply_msg, reply);

#ifndef notyet
    /*
     * XXX: drew 2009-01-12 This should let the replication test framework
     * function until we finish a more complete transition
     */
    if (ret) {
        sdf_msg_wrapper_rwref(&reply_msg, ret);
        reply_msg->sent_id = request_msg->sent_id;

        sdf_msg_wrapper_rwrelease(&reply_msg, ret);
    }
#endif /* ndef notyet */

    /* Must happen after release */
    if (!alias) {
        sdf_msg_wrapper_ref_count_dec(reply);
    }

    sdf_msg_wrapper_rwrelease(&request_msg, request);

    return (ret);
}

struct sdf_msg_wrapper *
sdf_msg_wrapper_copy(struct sdf_msg_wrapper *wrapper,
                     enum sdf_msg_wrapper_type msg_wrapper_type,
                     vnode_t src_vnode, service_t src_service,
                     vnode_t dest_vnode, service_t dest_service,
                     msg_type_t msg_type, struct sdf_resp_mbx *mresp_mbx) {
    int failed;
    struct sdf_msg *msg_local;
    struct sdf_msg *src_local;
    struct sdf_msg_wrapper *ret;
    sdf_msg_wrapper_free_local_t local_free;

    local_free = sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                   &sdf_msg_wrapper_sdf_msg_free, NULL);

    failed = !plat_alloc_struct(&ret);
    if (!failed) {
        sdf_msg_wrapper_init(ret, SMW_MUTABLE_FIRST, msg_wrapper_type,
                             src_vnode, src_service, dest_vnode, dest_service,
                             msg_type, mresp_mbx);

        /* FIXME: This should move into shared memory */
        ret->ptr_type = SMW_LOCAL;
        ret->ptr.local.mfree = local_free;
        ret->ptr.local.ptr = sdf_msg_alloc(wrapper->len -
                                           sizeof (struct sdf_msg));
        ret->len = wrapper->len;
        failed = !ret->ptr.local.ptr;
    }

    if (!failed) {
        msg_local = NULL;
        sdf_msg_wrapper_rwref(&msg_local, ret);
        src_local = NULL;
        sdf_msg_wrapper_rwref(&src_local, wrapper);

        memcpy(msg_local, src_local, ret->len);

        sdf_msg_wrapper_rwrelease(&src_local, wrapper);
        sdf_msg_wrapper_rwrelease(&msg_local, ret);
    }

    if (failed && ret) {
        plat_free(ret);
    }

    return (ret);
}

static void
sdf_msg_wrapper_init(struct sdf_msg_wrapper *sdf_msg_wrapper,
                     enum sdf_msg_wrapper_mutability mutability,
                     enum sdf_msg_wrapper_type msg_wrapper_type,
                     vnode_t src_vnode, service_t src_service,
                     vnode_t dest_vnode, service_t dest_service,
                     msg_type_t msg_type, struct sdf_resp_mbx *mresp_mbx) {
    plat_assert_imply(mresp_mbx != NULL, msg_wrapper_type != SMW_TYPE_ONE_WAY);
    plat_assert_imply(msg_wrapper_type == SMW_TYPE_RESPONSE,
                      mresp_mbx != NULL);

    sdf_msg_wrapper->mutability = mutability;

    sdf_msg_wrapper->msg_wrapper_type = msg_wrapper_type;

    sdf_msg_wrapper->src_vnode = src_vnode;
    sdf_msg_wrapper->src_service = src_service;

    sdf_msg_wrapper->dest_vnode = dest_vnode;
    sdf_msg_wrapper->dest_service = dest_service;

    sdf_msg_wrapper->msg_type = msg_type;

    if (mresp_mbx == NULL) {
        sdf_msg_wrapper->response_mbx_valid = 0;
    } else {
        sdf_msg_wrapper->response_mbx = *mresp_mbx;
        sdf_msg_wrapper->response_mbx_valid = 1;
    }

    sdf_msg_wrapper->ref_count = 1;
}

struct sdf_msg_wrapper *
sdf_msg_wrapper_recv(struct sdf_msg *msg) {
    struct sdf_msg_wrapper *ret = NULL;
    sdf_msg_wrapper_free_local_t local_free;

    local_free = sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                   &sdf_msg_wrapper_sdf_msg_free, NULL);

    /*
     * The mresp gets populated with the hash from and the msg
     */
    struct sdf_resp_mbx mrespbx;
    struct sdf_resp_mbx *mresp = sdf_msg_initmresp(&mrespbx);
    enum sdf_msg_wrapper_type msg_wrapper_type;

    /*
     * debug prints for tomr, setup for the recv wrapper, flags determine type so log to trace
     * will remove these when no longer needed
     *
     * XXX: drew 2009-06-14 The plethora of log messages is silly
     */
    if (msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_EXPECTED) {
        msg_wrapper_type = SMW_TYPE_REQUEST;

        plat_log_msg(21487, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Request MSG %p wrapper %p dn %d ds %d sn %d ss %d type %d flags 0x%x\n"
                     "        mkey %lx\n",
                     msg->msg_src_vnode, msg, ret, msg->msg_dest_vnode, msg->msg_dest_service,
                     msg->msg_src_vnode, msg->msg_src_service, msg->msg_type, msg->msg_flags,
                     msg->sent_id);
    } else if (msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_INCLUDED) {
        msg_wrapper_type = SMW_TYPE_RESPONSE;

        plat_log_msg(21488, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Response MSG %p wrapper %p dn %d ds %d sn %d ss %d type %d flags 0x%x\n"
                     "        mkey %lx\n",
                     msg->msg_src_vnode, msg, ret, msg->msg_dest_vnode, msg->msg_dest_service,
                     msg->msg_src_vnode, msg->msg_src_service, msg->msg_type, msg->msg_flags,
                     msg->sent_id);
    } else {
        msg_wrapper_type = SMW_TYPE_ONE_WAY;

        plat_log_msg(21489, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: One-way MSG %p wrapper %p dn %d ds %d sn %d ss %d type %d flags 0x%x\n"
                     "        mkey %lx\n",
                     msg->msg_src_vnode, msg, ret, msg->msg_dest_vnode, msg->msg_dest_service,
                     msg->msg_src_vnode, msg->msg_src_service, msg->msg_type, msg->msg_flags,
                     msg->sent_id);
    }

    /*
     * note in this case we just pull out the info from the received msg and
     * populate the wrapper it is up to the handler to determine if it is a
     * response or request and do the right thing
     */
    ret = sdf_msg_wrapper_local_alloc(msg, local_free, SMW_MUTABLE_FIRST,
                                      msg_wrapper_type,
                                      msg->msg_src_vnode, msg->msg_src_service,
                                      msg->msg_dest_vnode,
                                      msg->msg_dest_service, msg->msg_type,
                                      msg_wrapper_type != SMW_TYPE_ONE_WAY ?
                                      sdf_msg_get_response(msg, mresp) : NULL);
    return (ret);
}

static void
sdf_msg_wrapper_sdf_msg_free(plat_closure_scheduler_t *contect, void *env,
                             struct sdf_msg *msg) {

    plat_log_msg(21490, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: MSG Freeing %p dn %d ds %d sn %d ss %d type %d len %d\n",
                 msg->msg_src_vnode, msg, msg->msg_dest_vnode, msg->msg_dest_service,
                 msg->msg_src_vnode, msg->msg_src_service, msg->msg_type, msg->msg_len);

    sdf_msg_free(msg);

}

enum sdf_msg_wrapper_to_msg {
    SDF_MSG_WRAPPER_SEND,
    SDF_MSG_WRAPPER_RECV
};

static struct sdf_msg *
sdf_msg_wrapper_to_common_msg_alloc(struct sdf_msg_wrapper *wrapper,
                                    enum sdf_msg_wrapper_to_msg how) {
    int status;
    struct sdf_msg *msg;
    enum send_action action;
    struct sdf_msg *tmp = NULL;
    // int include_mbx;

    status = sdf_msg_wrapper_start_send(wrapper, &tmp, &msg, &action);

    if (status) {
        msg = NULL;
    } else {
        msg->msg_src_vnode = wrapper->src_vnode;
        msg->msg_src_service = wrapper->src_service;
        msg->msg_dest_vnode = wrapper->dest_vnode;
        msg->msg_dest_service = wrapper->dest_service;
        msg->msg_type = wrapper->msg_type;

        msg->msg_flags &= ~(SDF_MSG_FLAG_MBX_RESP_EXPECTED|
                            SDF_MSG_FLAG_MBX_RESP_INCLUDED);

        switch (wrapper->msg_wrapper_type) {
        case SMW_TYPE_REQUEST:
            msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;
            // include_mbx = 1;
            break;
        case SMW_TYPE_RESPONSE:
            plat_assert(wrapper->response_mbx_valid);
            msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_INCLUDED;
            // include_mbx = 1;
            break;
        case SMW_TYPE_ONE_WAY:
            // include_mbx = 0;
            break;
        default:
            plat_fatal("msg_wrapper_type not in enum");
            // include_mbx = 0;
            break;
        }

        if (wrapper->response_mbx_valid) {
            plat_assert(wrapper->response_mbx_valid);
            msg->sent_id = wrapper->response_mbx.resp_id;
        } else {
            msg->sent_id = 0;
        }
    }

    sdf_msg_wrapper_end_send(wrapper, &tmp, action);

    return (msg);
}

struct sdf_msg *
sdf_msg_wrapper_to_send_msg_alloc(struct sdf_msg_wrapper *wrapper) {
    return (sdf_msg_wrapper_to_common_msg_alloc(wrapper,
                                                SDF_MSG_WRAPPER_SEND));
}

struct sdf_msg *
sdf_msg_wrapper_to_recv_msg_alloc(struct sdf_msg_wrapper *wrapper) {
    return (sdf_msg_wrapper_to_common_msg_alloc(wrapper,
                                                SDF_MSG_WRAPPER_RECV));
}

int
sdf_msg_wrapper_send(struct sdf_msg_wrapper *wrapper,
                     struct sdf_fth_mbx *ar_mbx) {
    int status;
    enum send_action action;
    struct sdf_msg *send_msg;
    struct sdf_msg *tmp;

    status = sdf_msg_wrapper_start_send(wrapper, &tmp, &send_msg, &action);

    if (!status) {
        status = sdf_msg_send(send_msg, send_msg->msg_len - sizeof(*send_msg),
                              wrapper->dest_vnode, wrapper->dest_service,
                              wrapper->src_vnode, wrapper->src_service,
                              wrapper->msg_type, ar_mbx,
                              !ar_mbx && wrapper->response_mbx_valid ?
                              &wrapper->response_mbx : NULL);
    }

    sdf_msg_wrapper_end_send(wrapper, &tmp, action);

    return (status);
}

/**
 * @brief Start send-like action
 *
 * @param wrapper <IN> Wrapper
 * @param tmp_msg <OUT> Temporary pointer
 * @param out_msg <OUT> Allocated message is stored here, NULL on failure
 * @param out_action <OUT> disposition for sdf_msg_wrapper
 */
static int
sdf_msg_wrapper_start_send(struct sdf_msg_wrapper *wrapper,
                           struct sdf_msg **tmp_msg,
                           struct sdf_msg **out_msg,
                           enum send_action *out_action) {
    int ret;
    struct sdf_msg *tmp;

    *tmp_msg = NULL;
    sdf_msg_wrapper_rwref(tmp_msg, wrapper);

    tmp = *tmp_msg;

    /*
     * XXX: drew 2008-10-15 With unified shmem + local memory, the same free
     * can be made to work both ways.  Add a hook plat_freeable(void *)
     * which indicates whether the memory can be treated that way.
     */
    if (wrapper->ref_count == 1 &&
        wrapper->ptr_type == SMW_LOCAL) {
        *out_msg = wrapper->ptr.local.ptr;
        wrapper->ptr.local.ptr = NULL;
        *out_action = SA_FREE;
        ret = 0;
    } else {
        *out_msg = sdf_msg_alloc(tmp->msg_len - sizeof(*tmp));
        if (*out_msg) {
            memcpy(*out_msg, tmp, tmp->msg_len);
            ret = 0;
        } else {
            ret = -ENOMEM;
        }
        *out_action = SA_DEC;
    }

    return (ret);
}

/**
 * @brief End send-like action
 *
 * @param wrapper <IN> Wrapper
 * @param tmp_msg <INOUT> Temporary pointer
 * @param out_action <IN> disposition for sdf_msg_wrapper
 */
static void
sdf_msg_wrapper_end_send(struct sdf_msg_wrapper *wrapper,
                         struct sdf_msg **tmp_msg, enum send_action action) {

    sdf_msg_wrapper_rwrelease(tmp_msg, wrapper);

    switch (action) {
    case SA_FREE:
        plat_free(wrapper);
        break;
    case SA_DEC:
        sdf_msg_wrapper_ref_count_dec(wrapper);
        break;
    }
}

int
sdf_msg_wrapper_forward_reply(struct sdf_msg_wrapper *request,
                              struct sdf_msg_wrapper *reply,
                              struct sdf_fth_mbx *ar_mbx) {
    int ret;
    enum send_action action;
    struct sdf_msg *send_msg;
    struct sdf_msg *tmp;

    tmp = NULL;
    sdf_msg_wrapper_rwref(&tmp, reply);

    /*
     * XXX: drew 2008-10-15 With unified shmem + local memory, the same free
     * can be made to work both ways.  Add a hook plat_freeable(void *)
     * which indicates whether the memory can be treated that way.
     */
    if (reply->ref_count == 1 && reply->ptr_type == SMW_LOCAL) {
        send_msg = reply->ptr.local.ptr;
        reply->ptr.local.ptr = NULL;
        action = SA_FREE;
        ret = 0;
    } else {
        send_msg = sdf_msg_alloc(tmp->msg_len - sizeof(*tmp));
        if (send_msg) {
            memcpy(send_msg, tmp, tmp->msg_len);
            ret = 0;
        } else {
            ret = -ENOMEM;
        }
        action = SA_DEC;
    }

    if (!ret) {
        ret = sdf_msg_send(send_msg,
                           send_msg->msg_len - sizeof(*send_msg) /* payload */,
                           request->src_vnode /* dest */,
                           request->src_service /* dest */,
                           request->dest_vnode /* src */,
                           request->dest_service /* src */,
                           reply->msg_type, ar_mbx,
                           sdf_msg_wrapper_get_response_mbx(request));
    }

    sdf_msg_wrapper_rwrelease(&tmp, reply);

    switch (action) {
    case SA_FREE:
        plat_free(reply);
        break;
    case SA_DEC:
        sdf_msg_wrapper_ref_count_dec(reply);
        break;
    }

    return (ret);
}


void
sdf_msg_wrapper_ref_count_inc(struct sdf_msg_wrapper *sdf_msg_wrapper) {
    (void) __sync_add_and_fetch(&sdf_msg_wrapper->ref_count, 1);
}

void
sdf_msg_wrapper_ref_count_dec(struct sdf_msg_wrapper *sdf_msg_wrapper) {
    if (!__sync_sub_and_fetch(&sdf_msg_wrapper->ref_count, 1)) {
        switch (sdf_msg_wrapper->ptr_type) {
        case SMW_SHARED:
            plat_closure_apply(sdf_msg_wrapper_free_shared,
                               &sdf_msg_wrapper->ptr.shared.mfree,
                               sdf_msg_wrapper->ptr.shared.ptr);
            break;
        case SMW_LOCAL:
            plat_closure_apply(sdf_msg_wrapper_free_local,
                               &sdf_msg_wrapper->ptr.local.mfree,
                               sdf_msg_wrapper->ptr.local.ptr);
            break;
        }
        plat_free(sdf_msg_wrapper);
    }
}

void
sdf_msg_wrapper_rwref(struct sdf_msg **local,
                      struct sdf_msg_wrapper *sdf_msg_wrapper) {
    plat_assert(!*local);

    switch (sdf_msg_wrapper->ptr_type) {
    case SMW_SHARED:
        sdf_msg_sp_var_rwref(local, sdf_msg_wrapper->ptr.shared.ptr,
                             sdf_msg_wrapper->len);
        break;

    case SMW_LOCAL:
        *local = sdf_msg_wrapper->ptr.local.ptr;
        break;
    }
}

void
sdf_msg_wrapper_rwrelease(struct sdf_msg **local,
                          struct sdf_msg_wrapper *sdf_msg_wrapper) {
    switch (sdf_msg_wrapper->ptr_type) {
    case SMW_SHARED:
        sdf_msg_sp_var_rwrelease(local, sdf_msg_wrapper->len);
        break;

    case SMW_LOCAL:
        *local = NULL;
        break;
    }
}

struct sdf_resp_mbx *
sdf_msg_wrapper_get_response_mbx(struct sdf_msg_wrapper *msg_wrapper) {
    struct sdf_resp_mbx *ret;

    if (msg_wrapper->response_mbx_valid) {
        ret = &msg_wrapper->response_mbx;
    } else {
        ret = NULL;
    }

    return (ret);
}
