/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_MSG_H
#define PLATFORM_MSG_H 1

/*
 * File:   sdf/platform/msg.h
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: msg.h 587 2008-03-14 00:13:21Z drew $
 */

#include <sys/cdefs.h>
#include <sys/types.h>

#include "platform/closure.h"
#include "platform/types.h"

__BEGIN_DECLS

/*
 * Message headers plus functions to send and receive over sockets, pipes,
 * etc.
 *
 * FIXME: I (drew) need message headers for connection oriented sockets which
 * usually implies stream behavior; this does that.  This needs to get unified
 * with whatever we're doing for MPI so the same tools work regardless of
 * transport.
 */

/**
 * Value for msg_header.magic
 *
 * Expands to "msg0" on little endian systems.  Subsequent header changes
 * will have a different last (high) byte to allow forwards/backwards 
 * compatability (the entire cluster can't stop on upgrade and IP clients
 * like memcached can't be upgraded at the same time)
 */
#define PLAT_MSG_MAGIC 0x3067736d

/**
 * Base values for subsystem message types.  A common name space makes
 * it easier to sort out routing errors from programming errors.
 */
enum plat_msg_type_base {
    PLAT_MSG_BASE_SHMEM = 0x1000
};

/**
 * Special values of message type useful for generic dispatchers
 */
enum plat_msg_type {
    PLAT_MSG_TYPE_RESERVED = (1 << 31),
    PLAT_MSG_TYPE_DEFAULT = PLAT_MSG_TYPE_RESERVED|0x1,
    PLAT_MSG_TYPE_IN_EOF = PLAT_MSG_TYPE_RESERVED|0x2,
    PLAT_MSG_TYPE_OUT_EOF = PLAT_MSG_TYPE_RESERVED|0x3,
    PLAT_MSG_TYPE_UNKNOWN = PLAT_MSG_TYPE_RESERVED|0x4
};

struct plat_msg_header {
    /*
     * Either MSG_MAGIC or a version permuted based on endianess since
     * messages are always sent in native endian-order thus optimizing
     * for the normal case.
     *
     * Subsequent versions of the header protocol will use different
     * magic numbers (msg1, msg2, etc.)
     */
    int32_t magic;

    /*
     * Message type which should be an offset from the appropriate
     * plat_msg_type_base.  We could also split into application
     * and message fields.
     */
    int32_t type;

    /* Version of this message */
    int32_t version;

    /* Length including header */
    u_int32_t len;

    /* Sequence number.  64 bits to not wrap at 4H at 1Mops/sec */
    u_int64_t seqno;

    /* Microseconds since the unix epoch of midnight UTC Jan 1 1970  */
    u_int64_t time_stamp;

    /* Hopefully globally unique */
    plat_op_label_t label;

    /* This message is a response to what message */
    u_int64_t response_to_seqno;
   
    /* 
     * Non-zero on error.  Included in the header so that the remote end 
     * can nak an individual message it does not understand.
     */
    int response_status;
};

/**
 * Free closure type which is paired with a buffer allowing the receive
 * system flexibility in message allocation.  Free could map to a local
 * memory plat_free, shmem_free, or increase-read-pointer from a circular
 * buffer as consecutive reads complete.
 */
PLAT_CLOSURE1(plat_msg_free, struct plat_msg_header *, msg);

/**
 * Message received closure applied with a message and its free closure
 * for asynchronous messaging and generic message dispatching.
 */
PLAT_CLOSURE2(plat_msg_recv, struct plat_msg_header *, msg, 
    plat_msg_free_t, free_closure);

/*
 * Synchronously send a message.
 *
 * Returns positive on success, 0 on EOF, -errno on failure
 */
int plat_send_msg(int fd, const struct plat_msg_header *msg);

/*
 * Synchronously receive a message.
 *
 * On success, *msg contains the message which was received and
 * *msg_free_closure code which will free it.
 *
 * Returns positive on success, 0 on EOF, -errno on failure
 */
int plat_recv_msg(int fd, struct plat_msg_header **msg,
    plat_msg_free_t *msg_free_closure);


__END_DECLS
#endif /* ndef PLATFORM_MSG_H */
