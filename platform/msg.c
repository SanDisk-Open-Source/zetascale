/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   msg.c
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: msg.c 10527 2009-12-12 01:55:08Z drew $
 */

/*
 * Messaging for sockets, pipes, and ??
 */

#define PLATFORM_INTERNAL 1

#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/msg.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/unistd.h"

static int recv_bytes(int fd, void *buf, size_t len);
static void free_msg(plat_closure_scheduler_t *context, void *ignore,
                     struct plat_msg_header *msg);

/**
 * Synchronously send a message.
 *
 * Returns positive on success, 0 on EOF, and -errno on failure.
 */
int
plat_send_msg(int fd, const struct plat_msg_header *msg) {
    size_t remain = msg->len;
    const char *ptr = (const char *)msg;
    int ret = 0;
    int got;

    while (remain > 0 && ret >= 0) {
        got = plat_write(fd, ptr, remain);
        /* This is coming next */
        if (got > 0) {
            remain -= got;
            ptr += got;
            ret += got;
        } else if (!got) {
            ret = -EPIPE;
        } else if (plat_errno != EINTR) {
            plat_log_msg(20958, PLAT_LOG_CAT_PLATFORM_MSG,
                         PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "plat_send_msg write error %d", plat_errno);
            ret = -plat_errno;
        }
    }

    return (ret);
}

/**
 * Synchronously receive a message.
 *
 * On success, *msg contains the message which was received and
 * *msg_free_closure code which will free it.
 *
 * Returns positive on success, 0 on EOF, -errno on failure
 */
int
plat_recv_msg(int fd, struct plat_msg_header **msg_ptr,
              plat_msg_free_t *msg_free_closure) {
    struct plat_msg_header header, *msg = NULL;
    int got;
    size_t remain;
    int ret;

    ret = recv_bytes(fd, &header, sizeof (header));
    if (!ret) {
    } else if (0 <= ret && ret < sizeof (header)) {
        plat_log_msg(20959, PLAT_LOG_CAT_PLATFORM_MSG,
                     PLAT_LOG_LEVEL_DIAGNOSTIC,
                     "plat_recv_msg short header %d of %u bytes", ret,
                     (unsigned)sizeof (header));
        ret = -PLAT_EEOF;
    } else if (ret == sizeof (header) && header.magic != PLAT_MSG_MAGIC) {
        plat_log_msg(20960, PLAT_LOG_CAT_PLATFORM_MSG,
                     PLAT_LOG_LEVEL_DIAGNOSTIC, "plat_recv_msg bad magix %x",
                     header.magic);
        ret = -EILSEQ;
    } else {
        msg = plat_alloc(header.len);
        if (!msg) {
            ret = -ENOMEM;
        };
    };
    if (ret > 0) {
        memcpy(msg, &header, sizeof (header));
        remain = header.len - sizeof (header);
        got = recv_bytes(fd, ((char *)msg) + sizeof (header), remain);
        if (got < 0) {
            ret = got;
        } else if (!got) {
            ret = -PLAT_EEOF;
        } else if (got < remain) {
            plat_log_msg(20961, PLAT_LOG_CAT_PLATFORM_MSG,
                         PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "plat_recv_msg short payload %d of %u bytes", got,
                         (unsigned)remain);
            ret = -PLAT_EEOF;
        } else {
            ret += got;
        }
    }
    if (ret > 0)  {
        *msg_ptr = msg;
        *msg_free_closure =
            plat_msg_free_create(PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS, &free_msg,
                                 NULL);
    } else {
        plat_free(msg);
    }

    return (ret);
}

/**
 * Returns number of bytes read on success, with a short read indicating
 * EOF; -errno on failure.
 */
static int
recv_bytes(int fd, void *buf, size_t len) {
    size_t remain = len;
    char *ptr = buf;
    int done = 0;
    int ret = 0;
    int got;

    while (remain > 0 && ret >= 0 && !done) {
        got = plat_read(fd, ptr, remain);
        /* This is coming next */
        if (got > 0) {
            remain -= got;
            ptr += got;
            ret += got;
        } else if (!got) {
            done = 1;
        } else if (plat_errno != EINTR) {
            ret = -plat_errno;
        }
    }

    return (ret);
}

/* Closure to free message */
static void
free_msg(plat_closure_scheduler_t *context, void *ignore,
         struct plat_msg_header *msg) {
    plat_free(msg);
}
