/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef SDF_MSG_WRAPPER_H
#define SDF_MSG_WRAPPER_H 1

/*
 * File:   sdf/sdfmsg/sdf_msg_wrapper.h
 * Author: drew
 *
 * Created on May 8, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_wrapper.h 1480 2008-06-05 09:23:13Z drew $
 */

/**
 * struct sdf_msg_wrapper is a thin wrapper around a message which
 * describes where it came from and who owns it with the intent of providing
 * zero-copy messaging including fan-out.
 *
 * With the exception of sdf_msg_wrapper_send, it's safe for use in
 * simulated environments (notably sdf/protocol/replication/tests)
 */

#include "platform/closure.h"
#include "platform/defs.h"
#include "platform/types.h"
#include "sdfmsg/sdf_msg.h"
#include "sdf_fth_mbx.h"

PLAT_CLOSURE1(sdf_msg_wrapper_free_local, struct sdf_msg *, msg_ptr)
PLAT_CLOSURE1(sdf_msg_wrapper_free_shared, sdf_msg_sp_t, msg_ptr)

/** @brief Mutability of message coming in. */
enum sdf_msg_wrapper_mutability {
    /**
     * @brief The first owner may mutate the message
     *
     * This allows the first (reference count 1) user to change the message,
     * probably to change its type for forwarding or add fields like replication
     * sequence numbers.
     */
    SMW_MUTABLE_FIRST,

    /**
     * @brief This is a read-only message
     */
    SMW_READ_ONLY
};

enum sdf_msg_wrapper_type {
    /** @brief Message is a request */
    SMW_TYPE_REQUEST,
    /** @brief Message is a response to a request */
    SMW_TYPE_RESPONSE,
    /** @brief Message is neither a request nor a response */
    SMW_TYPE_ONE_WAY
};

/**
 * @brief Thin wrapper around sdf_msg with separate envelope information
 *
 * The idea here is to separate envelope information from the payload
 * so that messages can be updated and forwarded (as for replication)
 * without incurring copies.
 *
 * At the same time, memory allocation and referencing (shared vs. local
 * memory) are abstracted to facilitate 0-copy operation.
 *
 * FIXME: This needs to split into this front-end piece containing just
 * the envelope information and a back-end reference counted buffer piece.
 */

struct sdf_msg_wrapper {
    /** @brief Pointer and free abstraction */
    union {
        struct {
            struct sdf_msg *ptr;
            sdf_msg_wrapper_free_local_t mfree;
        } local;

        struct {
            sdf_msg_sp_t ptr;
            sdf_msg_wrapper_free_shared_t mfree;
        } shared;
    } ptr;

    /** @brief Total message len */
    size_t len;

    /** @brief Type of ptr field */
    enum {
        SMW_SHARED,
        SMW_LOCAL
    } ptr_type;

    /** @brief Whether this is allowed to change */
    enum sdf_msg_wrapper_mutability mutability;

    /** @brief Type of message wrapper */
    enum sdf_msg_wrapper_type msg_wrapper_type;

    /** @brief Vnode from which this came */
    vnode_t src_vnode;
    /** @brief Service from which this came */
    service_t src_service;

    /** @brief Vnode at which this was received */
    vnode_t dest_vnode;
    /** @brief Service at which this was received */
    service_t dest_service;

    msg_type_t msg_type;

    /**
     * @brief response mailbox
     *
     * XXX: drew 2009-01-13 This has gotten outright confusing as a side
     * effect of changes in the messaging layer + misunderstandings  and
     * needs to change for the better.  Since there's only one mkey which
     * can be used for both requests and replies, it should probably assume
     * the same either-or semantics.
     *
     * Currently, in receive contexts this has the inbound akrpmbx and mkey.
     *
     * In send contexts it's akrpmbx_from_req and its mkey, with the send call
     * providing the new akrpmbx.
     */
    struct sdf_resp_mbx response_mbx;

    /**
     * @brief 1 when #response_mbx is valid, 0 otherwise
     *
     * This is not entirely implied by msg_wrapper_type because
     * SMW_TYPE_REQUEST messages don't have a valid response_mbx on
     * the sending side but do have one on the receiving side.
     */
    unsigned response_mbx_valid : 1;

    int ref_count;
};

__BEGIN_DECLS

/**
 * @brief Allocate sdf_msg_wrapper arround local sdf_msg
 *
 * A new sdf_msg_wrapper is returned with a reference count of 1.  Users
 * should call #sdf_msg_wrapper_ref_count_dec to decrement and
 * #sdf_msg_wrapper_ref_count_inc to grab references.
 *
 * @param local <IN> message in local memory which will may be modified
 * if mutability == SMW_MUTABLE_FIRST.
 * @param local_free <IN> closure to free message when the reference count
 * reachs 0
 * @param mutability <IN> whether the first user may modify this
 * @param msg_wrapper_type <IN> type of message wrapper;
 * SMW_TYPE_REQUEST, SMW_TYPE_RESPONSE, SMW_TYPE_ONE_WAY.
 * @param recevied_vnode <IN> vnode at which this was received
 * @param recevied_service <IN> service at which this was received
 * @param response_mbx <IN> The original request's mailbox information,
 * which implies NULL in a sending context for a new request.  See
 * the response_mbx field in #sdf_msg_wrapper
 */
struct sdf_msg_wrapper *
sdf_msg_wrapper_local_alloc(struct sdf_msg *local,
                            sdf_msg_wrapper_free_local_t local_free,
                            enum sdf_msg_wrapper_mutability mutability,
                            enum sdf_msg_wrapper_type msg_wrapper_type,
                            vnode_t src_vnode, service_t src_service,
                            vnode_t dest_vnode, service_t dest_service,
                            msg_type_t msg_type,
                            struct sdf_resp_mbx *mresp_mbx /* for cstyle */);


/**
 * @brief Allocate sdf_msg_wrapper around shared sdf_msg
 *
 * A new sdf_msg_wrapper is returned with a reference count of 1.  Users
 * should call #sdf_msg_wrapper_ref_count_dec to decrement and
 * #sdf_msg_wrapper_/ef_count_inc to grab references.
 *
 * @param shared <IN> message in shared memory which will may be modified
 * if mutability == SMW_MUTABLE_FIRST.
 * @param local_free <IN> closure to free message when the referfe
 * @param mutability <IN> whether the first user may modify this
 * @param msg_wrapper_type <IN> type of message wrapper;
 * SMW_TYPE_REQUEST, SMW_TYPE_RESPONSE, SMW_TYPE_ONE_WAY.
 * @param recevied_vnode <IN> vnode at which this was received
 * @param recevied_service <IN> service at which this was received
 * @param response_mbx <IN> The original request's mailbox information,
 * which implies NULL in a sending context for a new request.  See
 * the response_mbx field in #sdf_msg_wrapper
 */
struct sdf_msg_wrapper *
sdf_msg_wrapper_shared_alloc(sdf_msg_sp_t shared,
                             sdf_msg_wrapper_free_shared_t shared_free,
                             enum sdf_msg_wrapper_mutability mutability,
                             enum sdf_msg_wrapper_type msg_wrapper_type,
                             vnode_t src_vnode, service_t src_service,
                             vnode_t dest_vnode, service_t dest_service,
                             msg_type_t msg_type,
                             struct sdf_resp_mbx *mresp_mbx);

/**
 * @brief Allocate a new sdf_msg_wrapper for a forwarded reply
 *
 * One reference count of the reply is consumed.  When this is the last
 * reference count the same structure may be returned.
 *
 * @param request <IN> The original request.  The returned sdf_msg_wrapper
 * has the request destination as its source and source as the destination
 * which is to say the return is a reply to it.  The original response
 * mailbox is used.
 *
 * @param reply <IN> The forwardeded reply.  One reference count is consumed
 * regardless of success or failure.
 *
 * @return sdf_msg_wrapper which is a reply to request using the contents
 * of reply.  NULL on failure.
 */
struct sdf_msg_wrapper *
sdf_msg_wrapper_forward_reply_alloc(struct sdf_msg_wrapper *request,
                                    struct sdf_msg_wrapper *reply);

/**
 * @brief Create a message copy with new envelope
 */
struct sdf_msg_wrapper *
sdf_msg_wrapper_copy(struct sdf_msg_wrapper *wrapper,
                     enum sdf_msg_wrapper_type msg_wrapper_type,
                     vnode_t src_vnode, service_t src_service,
                     vnode_t dest_vnode, service_t dest_service,
                     msg_type_t msg_type, struct sdf_resp_mbx *mresp_mbx);

/**
 * @brief Send message
 *
 * One reference count is consumed by the function.
 *
 * @param wrapper <IN> sdf_msg wrapper
 * @param ar_mbx <IN> ar_mbx as in #sdf_msg_send
 * @return 0 on success, non-zero on failure
 */
int sdf_msg_wrapper_send(struct sdf_msg_wrapper *wrapper,
                         struct sdf_fth_mbx *ar_mbx);

/**
 * @brief Convert sdf_msg_wrapper to sdf_msg for sending
 *
 * One reference count is consumed by the function.
 *
 * The response field becomes the ackrpmbx_from_req field.
 *
 * @param wrapper <IN> sdf_msg wrapper
 * @return sdf_msg with appropriately initialized headers which can be
 * freed with sdf_msg_free()
 */
struct sdf_msg *sdf_msg_wrapper_to_send_msg_alloc(struct sdf_msg_wrapper *wrapper);

/**
 * @brief Convert sdf_msg_wrapper to sdf_msg for delivery
 *
 * One reference count is consumed by the function.  The response field
 * becomes the ackrpmbx field.
 *
 * @param wrapper <IN> sdf_msg wrapper
 * @return sdf_msg with appropriately initialized headers which can be
 * freed with sdf_msg_free()
 */

struct sdf_msg *sdf_msg_wrapper_to_recv_msg_alloc(struct sdf_msg_wrapper *wrapper);

/**
 * @brief Forward reply
 *
 * One reference count is consumed on the reply.
 *
 * @param request <IN> The original request for which the reply
 * is being forwarded.
 *
 * @param request <IN> Original request from which envelope information
 * is extracted.
 *
 * @param reply <IN> Reply being sent.
 *
 * @param ar_mbx <IN> Where a subsequent response goes.
 *
 * @return 0 on success, non-zero on failure
 */
int sdf_msg_wrapper_forward_reply(struct sdf_msg_wrapper *request,
                                  struct sdf_msg_wrapper *reply,
                                  struct sdf_fth_mbx *ar_mbx);

/**
 * @brief Construct message on receive
 *
 * A #sdf_msg_wrapper is created from the given message.
 *
 */
struct sdf_msg_wrapper *sdf_msg_wrapper_recv(struct sdf_msg *msg);

void
sdf_msg_wrapper_ref_count_inc(struct sdf_msg_wrapper *sdf_msg_wrapper);

/** @brief Decrement reference count */
void
sdf_msg_wrapper_ref_count_dec(struct sdf_msg_wrapper *sdf_msg_wrapper);

/** @brief Get read-write reference. local must be NULL */
void sdf_msg_wrapper_rwref(struct sdf_msg **local,
                           struct sdf_msg_wrapper *sdf_msg_wrapper);

/** @brief Release read-write reference. */
void sdf_msg_wrapper_rwrelease(struct sdf_msg **local,
                               struct sdf_msg_wrapper *sdf_msg_wrapper);

/**
 * @brief Get response structure
 *
 * @return Pointer to sdf_resp_mbx structure when one exists, NULL on
 * failure.  sdf_resp_mbx should be used immediately.
 */
struct sdf_resp_mbx *
sdf_msg_wrapper_get_response_mbx(struct sdf_msg_wrapper *msg_wrapper);

/**
 * @brief Kludge for current sdf_msg api
 *
 * Inherits one of the references, returning a buffer suitable for
 * the current sdf_msg_send() API which can be freed  with
 * sdf_msg_free()
 */
struct sdf_msg *
sdf_msg_wrapper_extract_for_send(struct sdf_msg_wrapper *sdf_msg_wrapper);

__END_DECLS

#endif /* def SDF_MSG_WRAPPER_H */
