/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/platform/shmem_client.c
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmem_client.c 10527 2009-12-12 01:55:08Z drew $
 */

/**
 * The shmem subsystem provides crash-consistent access to shared memory for
 * IPC between user processes, kernel scheduled user threads, user scheduled
 * non-premptive threads, and perhaps kernel subsystems.
 */

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/mman.h"
#include "platform/shm.h"
#include "platform/socket.h"
#include "platform/stdlib.h"
#include "platform/time.h"
#include "platform/unistd.h"

#include "platform/attr.h"
#include "platform/logging.h"
#include "platform/shmem.h"

#include "private/shmem_internal.h"
#include "private/shmem_msgs.h"

#undef min
#define min(a, b) ((a) <= (b) ? (a) : (b))

static int shmem_attach_listen(struct plat_shmem *shmem);
static int shmem_attach_connect(struct plat_shmem *shmem, const char *space);
static int shmem_attach_handshake(struct plat_shmem *shmem);
static void shmem_init_msg_header(struct plat_shmem *shmem,
    struct plat_msg_header *header, int type, u_int32_t len);
static int shmem_close(struct plat_shmem *shmem);
static int attached_refresh(struct plat_shmem *shmem);
static int attach_segment(struct shmem_attached_segment *attached,
    const struct shmem_descriptor *descriptor);
static int attach_segment_mmap(struct shmem_attached_segment *attached,
    const struct shmem_descriptor *descriptor);
static int attach_segment_sysv(struct shmem_attached_segment *attached,
    const struct shmem_descriptor *descriptor);
static int detach_segment(struct shmem_attached_segment *segment);

/**
 * Attach to the shared memory space, where the name space is shared across
 * the node.  Due to the tight integration needed with the user thread
 * scheduler for blocking IPC objects, an individual process may attach to
 * at most one shared space but the mapped subset of the space can vary
 * between processes.
 *
 * Spaces may be composed of multiple non-contiguous segments and may not
 * be fully mapped.
 *
 * plat_shmem_attach must be called before the first user thread is
 * created so that the scheduler can allocate shared memory. It is
 * not reentrant.
 *
 * plat_shmem_detach should be called on termination for diagnosic
 * purposes but for correctness both abnormal and normal termination
 * imply detach.
 *
 * Returns:
 *      -ENOENT where space does not have exist
 *      -EBUSY
 */
int
plat_shmem_attach(const char *space) {
    struct plat_shmem *shmem;
    int ret;

    ret = plat_attr_get_prog_shmem(&shmem) ? -EBUSY : 0;

    if (!ret) {
        shmem = plat_calloc(1, sizeof (*shmem));
        if (!shmem) {
            ret = -ENOMEM;
        }
    }

    if (!ret) {
        plat_mutex_init(&shmem->attached.lock);
        shmem->attached.attached = plat_calloc(1,
            sizeof (struct plat_shmem_attached) +
            sizeof (struct shmem_attached_segment) * 2);
        shmem->listen.fd = -1;
        plat_mutex_init(&shmem->connect.lock);
        shmem->connect.seqno = 0;
        shmem->connect.fd = -1;
        if (!shmem->attached.attached) {
            ret = -ENOMEM;
        }
    }

    if (!ret) {
        shmem->attached.attached->nsegments = 2;
    }

    if (!ret) {
        ret = shmem_attach_listen(shmem);
    }

    if (!ret) {
        ret = shmem_attach_connect(shmem, space);
    }

    if (!ret) {
        ret = shmem_attach_handshake(shmem);
    }

    if (ret) {
        shmem_close(shmem);
    }

    return (ret);
}


/**
 * Detach from the shared memory object.  All plat_shmem_ref_t references
 * associated with plat_shmem_t are released.
 *
 * Preconditions: All lightweight processes have terminated
 *
 * Returns:
 *      PLAT_EBUSY when shmem is still being referenced, whether via
 *      a user allocated pointer or lightweight process.
 */
int
plat_shmem_detach() {
    int ret;
    struct plat_shmem *shmem;

    ret = plat_attr_get_prog_shmem(&shmem);
    if (!ret) {
        ret = shmem_close(shmem);
    }

    if (!ret) {
        plat_attr_set_prog_shmem(NULL);
    }

    return (ret);
}

/*
 * Type unsafe conversion between internal smart pointer structures
 * and dumb pointers not for user use.
 */
void *
plat_shmem_ptr_base_to_ptr(shmem_ptr_base_t *shmem_ptr_base) {
    struct plat_shmem *shmem;
    int segment = shmem_ptr_base->segment;
    void *ret;
    int tmp;

    if (!segment) {
        ret = NULL;
    } else {
        plat_assert(segment > 0);

        tmp = plat_attr_get_prog_shmem(&shmem);
        plat_assert(!tmp);

        plat_mutex_lock(&shmem->attached.lock);
        if (segment > shmem->attached.attached->nsegments) {
            attached_refresh(shmem);
        }
        plat_assert(segment < shmem->attached.attached->nsegments);
        ret = ((char *)shmem->attached.attached->segments[segment].ptr) +
            shmem_ptr_base->offset;
        plat_mutex_unlock(&shmem->attached.lock);

        /*
         * FIXME: SIGBUS delivered via plat_thread_kill() would be
         * more appropriate.
         */
        plat_assert(ret);
    }

    return (ret);
}

/*
 * Listen to back connect socket.  Returns 0 on success, -errno on failure.
 */
static int
shmem_attach_listen(struct plat_shmem *shmem) {
    int ret = 0;

    shmem->listen.addr.unix_addr.sun_family = AF_UNIX;
    /* FIXME: Should use something better than mktemp. */
    strcpy(shmem->listen.addr.unix_addr.sun_path, "/tmp/shmem_client_XXXXXX");
    mktemp(shmem->listen.addr.unix_addr.sun_path);

    /* No reconnect on other end yet */
#ifdef notyet
    if (!ret)  {
        shmem->listen.fd = plat_socket(AF_UNIX, SOCK_STREAM, 0);
        if (shmem->listen.fd == -1) {
            ret = -plat_errno;
        }
    }

    if (!ret && bind(shmem->listen.fd,
        (struct sockaddr *)&shmem->listen.addr.unix_addr,
        sizeof (shmem->listen.addr.unix_addr)) == -1) {
        ret = -plat_errno;
    }

    if (!ret && listen(shmem->listen.fd, 1) == -1) {
        ret = -plat_errno;
    }
#endif

    return (ret);
}

/*
 * Connect to shmemd.  Returns 0 on success, errno on failure
 */
static int
shmem_attach_connect(struct plat_shmem *shmem, const char *space) {
    int ret = 0;
    struct sockaddr_un addr = { AF_UNIX, };

    strncpy(addr.sun_path, space, sizeof (addr.sun_path));

    shmem->connect.fd = plat_socket(AF_UNIX, SOCK_STREAM, 0);
    if (shmem->connect.fd == -1) {
        ret = -plat_errno;
    }

    if (!ret &&
        plat_connect(shmem->connect.fd, (struct sockaddr *)&addr,
            sizeof (addr)) == -1) {
        ret = -plat_errno;
    }

    return (ret);
}

/*
 * Handshake with shmemd.
 * Precondition: Connection established to shmemd.
 * Returns
 */
static int
shmem_attach_handshake(struct plat_shmem *shmem) {
    int ret;
    struct shmem_attach_request attach_request;
    struct plat_msg_header *response_header = NULL;
    struct shmem_attach_response *response;
    plat_msg_free_t response_free_closure = PLAT_CLOSURE_INITIALIZER;

    memset(&attach_request, 0, sizeof (attach_request));

    shmem_init_msg_header(shmem, &attach_request.header, SHMEM_ATTACH_REQUEST,
        sizeof (struct shmem_attach_request));
    attach_request.reconnect_addr = shmem->listen.addr;
    ret = plat_send_msg(shmem->connect.fd, &attach_request.header);
    if (!ret) {
        ret = -PLAT_EEOF;
    } else if (ret > 0) {
        ret = plat_recv_msg(shmem->connect.fd, &response_header,
            &response_free_closure);
        if (!ret) {
            ret = -PLAT_EEOF;
        }
    }
    if (ret >= 0 &&
        response_header->response_to_seqno != attach_request.header.seqno) {
        plat_log_msg(21035, PLAT_LOG_CAT_PLATFORM_SHMEM,
            PLAT_LOG_LEVEL_DIAGNOSTIC,
            "shmem_attach_handshake unexpected response_to_seqno %llu not %llu",
            (unsigned long long) response_header->response_to_seqno,
            (unsigned long long) attach_request.header.seqno);
        ret = -EILSEQ;
    }
    if (ret >= 0 && response_header->type != SHMEM_ATTACH_RESPONSE) {
        plat_log_msg(21036, PLAT_LOG_CAT_PLATFORM_SHMEM,
            PLAT_LOG_LEVEL_WARN,
            "shmem_attach_handshake unexpected type  %x not %x",
            response_header->type, SHMEM_ATTACH_RESPONSE);
        ret = -EILSEQ;
    }
    response = (struct shmem_attach_response *)response_header;
    if (ret >= 0 && response_header->status) {
        plat_log_msg(21037, PLAT_LOG_CAT_PLATFORM_SHMEM,
            PLAT_LOG_LEVEL_WARN,
            "shmem_attach_handshake error %s", 
            plat_strerror(response_header->status));
        ret = -response_header->status;
    }
    if (ret >= 0) {
        plat_assert(shmem->attached.attached->nsegments >= 2);

        ret = attach_segment(&shmem->attached.attached->segments[1],
            &response->first_descriptor);
    }
    if (ret >= 0) {
        shmem->attached.attached->nsegments = 2;
        shmem->process_ptr = response->process_ptr;
    }

    if (response_header) {
        plat_closure_apply(plat_msg_free, &response_free_closure,
                           response_header);
    }

    return (ret);
}

/*
 * Precondition: connection lock held or uneeded as during init.
 */
static void
shmem_init_msg_header(struct plat_shmem *shmem,
    struct plat_msg_header *header, int type, u_int32_t len) {
    struct timeval tv;

    plat_gettimeofday(&tv, NULL);

    memset(header, 0, sizeof (*header));
    header->magic = PLAT_MSG_MAGIC;
    header->type = type;
    header->version = 0;
    header->len = len;
    header->seqno = shmem->connect.seqno++;
    header->time_stamp = tv.tv_sec * 1000000LL + tv.tv_usec;
}


static int
shmem_close(struct plat_shmem *shmem) {
    int i;

    if (shmem) {
        if (shmem->attached.attached) {
            for (i = 1; i < shmem->attached.attached->nsegments; ++i) {
                detach_segment(&shmem->attached.attached->segments[i]);
            }
            plat_free(shmem->attached.attached);
        }
        if (shmem->connect.fd != -1) {
            plat_close(shmem->connect.fd);
        }
        if (shmem->listen.fd != -1) {
            plat_close(shmem->listen.fd);
        }
    }

    return (0);
}
