/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef _SDF_FTH_MBX_H
#define _SDF_FTH_MBX_H

/*
 * File:   sdf/sdfmsg/sdf_fth_mbx.h
 *
 * Author: drew
 *
 * Created on June 3, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: replicator_adapter.c 1397 2008-05-28 11:30:11Z drew $
 */

/**
 * Trivial send action allocator/deallocator functions used for
 * the soon to be deprecated sdf_fth_mbx structure.
 *
 * This is mis-named, but should go away RSN
 */
#include "platform/defs.h"
#include "platform/closure.h"

#include "fth/fthMbox.h"

#include "sdf_msg_types.h"
#include "sdf_msg_action.h"


/**
 * @brief Out-of-band values for #sdf_fth_mbx timeout_usc field
 *
 * XXX: Overloading this for errors too is probably bad
 */
 
enum sdf_fth_mbx_timeout {
    /** @brief Value for error resposnes being acceptable */
    SDF_FTH_MBX_TIMEOUT_ONLY_ERROR = -1,

    /** @brief Value for no timeout */
    SDF_FTH_MBX_TIMEOUT_NONE = 0,
};

/*
 * FIXME: Life doesn't become an N^2 problem when the response/ack
 * structure splits into separate response and ack activities.
 */
typedef struct sdf_fth_mbx {
    /** @brief One of the #SDF_msg_SACK enum except in test code */
    enum SDF_msg_SACK actlvl;        /* Modern to use aaction, raction, etc. */

    /** @brief Time of send in ns since the epoch is posted on send */
    fthMbox_t *abox;                 /* sending fth threads ack mailbox */

    /** @brief The sdf_msg response to this */
    fthMbox_t *rbox;                 /* response mailbox, should be NULL if no return is reqd */

    /** @brief What to do on ack when actlvl is MODERN or MODERN_REL */
    struct sdf_msg_action *aaction;

    /** @brief What to do on resp when actlvl is MODERN or MODERN_REL */
    struct sdf_msg_action *raction;

    /** @brief Release on send (valid when modern) */
    int release_on_send;

    /**
     * @brief Timeout in microseconds from send.
     *
     * See #sdf_fth_mbx_timeout for out-of-band values
     */
    int64_t timeout_usec;

    /** @brief hashed */
    struct sdf_resp_mbx *mkeybx;
} sdf_fth_mbx_t;

/*
 * facilitate the use of the hash key and the response mbox pointer
 * co-locating them here
 */

typedef struct sdf_resp_mbx {
    int64_t resp_id;

    /** @brief The sdf_msg response mbx to this */
    struct sdf_fth_mbx *rbox;        /* the real response mailbox, NULL if not return reqd */

} sdf_resp_mbx_t;

__BEGIN_DECLS


/**
 * @brief Allocate #sdf_fth_mbx which directs messages to closure
 *
 * @param closure <IN> Closure which is applied when a message is received,
 * with an #sdf_msg_wrapper as its argument.  It is the caller's
 * responsibility to guarantee that the closure remains valid until
 * a response (potentially synthetic for shutdown or timeout) is
 * received.
 *
 * See #sdf_msg_recv_wrapper in sdf_msg_action.h for notes on reference
 * counting, envelope and message header validity, etc.
 *
 * @param  release <IN> SACK_REL_YES to release the message buffer on
 * send.
 *
 * @param timeout_usec <IN> Timeout in usecs.  A positive timeout
 * will result in an SDF_MSG_TIMEOUT message type being delivered to the
 * rbox action of ar_mbx from the local node with source service
 * SDF_SDFMSG.  Use SDF_FTH_MBX_TIMEOUT_NONE for no timeout.
 */
struct sdf_fth_mbx *
sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_t closure,
                               enum SDF_msg_SACK_rel release,
                               int64_t timeout_usec);

/*
 * @brief Allocate #sdf_fth_mbx which directs to fthMbox
 *
 * @param mbox <IN> fthMbox which struct sdf_msg * results are posted.  The
 * receiver should call #sdf_msg_free when it is done using the buffer.
 *
 * @param  release <IN> SACK_REL_YES to release the message buffer on
 * send.
 *
 * @param timeout_usec <IN> Timeout in usecs.  A positive timeout
 * will result in an SDF_MSG_TIMEOUT message type being delivered to the
 * rbox action of ar_mbx from the local node with source service
 * SDF_SDFMSG.  Use SDF_FTH_MBX_TIMEOUT_NONE for no timeout.
 */
struct sdf_fth_mbx *
sdf_fth_mbx_resp_mbox_alloc(fthMbox_t *mbox,
                            enum SDF_msg_SACK_rel release,
                            int64_t timeout_usec);

/** @brief Deep copy */
struct sdf_fth_mbx *
sdf_fth_mbx_copy(const struct sdf_fth_mbx *mbox);

void sdf_fth_mbx_free(struct sdf_fth_mbx *);

/**
 * @brief Deliver ack msg
 *
 * After a message has been sent the messaging layer shall call this
 * function to indicate that the msg has been sent
 *
 * @param msg <IN> Pointer to original message
 * @return 0 on success, non-zero on failure.
 */
int sdf_fth_mbx_deliver_ack(struct sdf_fth_mbx *ackmbx, struct sdf_msg *msg);

/**
 * @brief Deliver ack msg as wrapper
 *
 * After a message has been sent the messaging layer shall call this
 * function to indicate that the msg has been sent
 *
 * @param msg <IN> Pointer to original message
 * @return 0 on success, non-zero on failure.
 */
int sdf_fth_mbx_deliver_ack_wrapper(struct sdf_fth_mbx *ackmbx,
                                    struct sdf_msg_wrapper *msg_wrapper);


/**
 * @brief Deliver response msg
 *
 * When a response was successfully received the messaging layer shall call
 * this method to deliver it.
 *
 * @param ackmbx <IN> The original response_mbx parameter to
 * #sdf_msg_send
 * @param msg <IN> A pointer to the response message which the receiver
 * shall free with #sdf_msg_free.
 * @return 0 on success, non-zero on failure.
 */
int sdf_fth_mbx_deliver_resp(struct sdf_fth_mbx *ackmbx, struct sdf_msg *msg);

/**
 * @brief Deliver response msg as wrapper
 *
 * This consumes one reference count.  The message must not be mutated
 * as long as the reference count is greater than one.
 *
 * See #sdf_msg_recv_wrapper in sdf_msg_action.h for notes on reference
 * counting, envelope and message header validity, etc.
 */
int sdf_fth_mbx_deliver_resp_wrapper(struct sdf_fth_mbx *ackmbx,
                                     struct sdf_msg_wrapper *msg_wrapper);

#ifdef notyet
/**
 * @brief Return whether the given sdf_fth_mbx is expecting a response
 *
 * Currently a message can solicit a response, provide a response,
 * or do neither.
 *
 * Return true when a response is expected with delivery information
 * specified in the mbx.
 */
SDF_boolean_t sdf_fth_mbx_resp_expected(const struct sdf_fth_mbx *mbx);
#endif

__END_DECLS

#endif /* _SDF_FTH_MBX_H */
