/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_SHMEM_MSGS_H
#define PLATFORM_SHMEM_MSGS_H 1

/*
 * File:   sdf/platform/msg.h
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmem_msgs.h 587 2008-03-14 00:13:21Z drew $
 */

/*
 * Messages used by shmem client <-> shmemd.  During normal operation the
 * client library only sends messages to shmemd on startup and when the
 * shared memory pool is empty and more space is needed.
 *
 * This is split out from shmem_internal.h because at some point we should
 * have a toy parser that automagically builds the message pretty printer
 * out of *_msgs.h.
 */

#include <sys/types.h>
#include <sys/un.h>

#include "platform/msg.h"

#include "shmem_internal.h"

/* FIXME: message bases should come from a central authority like msg.h */
enum {
    SHMEM_MSG_BASE = 0x1000,
    SHMEM_MSG_VERSION = 1
};

struct shmem_attach_request {
    struct plat_msg_header header;

    /**
     * Class of user indicating how the system will recover. 
     */
    enum plat_shmem_user_class user_class;

    /**
     * Pid of process attaching used to implement fail-stop behavior. Any
     * abnormal termination in fail-stop mode results in all attached 
     * processes being terminated by a SIGTERM, SIGKILL sequence.
     */
    pid_t pid;

    /**
     * Name of the unix domain socket bound by this for incoming shmemd
     * connections.  When shmemd terminates abnormally and is restarted, the
     * new shmemd iterates over all processes and reaps those which no longer
     * have their socket file flock'd.  It connects to the rest via a unix
     * domain socket with this endpoint.
     */
    union shmem_socket_addr reconnect_addr;
};

struct shmem_attach_response {
    struct plat_msg_header header;

    /* Pointer to structure this process should use */
    plat_process_sp_t process_ptr;

    struct shmem_descriptor first_descriptor;
};

struct shmem_detach_request {
    struct plat_msg_header header;
};

struct shmem_detach_response {
    struct plat_msg_header header;
};

struct shmem_reattach_request {
    struct plat_msg_header header;
};

struct shmem_reattach_response {
    struct plat_msg_header header;

    /* Are you the process I think you are? */
    plat_process_sp_t me;
};

struct shmem_grow_request {
    struct plat_msg_header header;
    size_t failed_allocation_len;
};

struct shmem_grow_response {
    struct plat_msg_header header;
};

/* Explicitly specify values so programmers can't scramble */
enum {
    SHMEM_ATTACH_REQUEST = SHMEM_MSG_BASE + 1,
    SHMEM_ATTACH_RESPONSE = SHMEM_MSG_BASE + 2,
    SHMEM_DETACH_REQUEST = SHMEM_MSG_BASE + 3,
    SHMEM_DETACH_RESPONSE = SHMEM_MSG_BASE + 4,
    SHMEM_REATTACH_REQUEST = SHMEM_MSG_BASE + 5,
    SHMEM_REATTACH_RESPONSE = SHMEM_MSG_BASE + 6,
    SHMEM_GROW_REQUEST = SHMEM_MSG_BASE + 7,
    SHMEM_GROW_RESPONSE = SHMEM_MSG_BASE + 8,
};

#endif /* ndef PLATFORM_SHMEM_MSGS_H */
