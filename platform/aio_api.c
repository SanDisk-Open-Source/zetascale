/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/aio_api.c $
 * Author: drew
 *
 * Created on March 8, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aio_api.c 12398 2010-03-20 00:58:14Z drew $
 */

#include "platform/aio_api.h"
#include "platform/aio_api_internal.h"
#include "platform/logging.h"

#define LOG_CAT PLAT_LOG_CAT_PLATFORM_AIO

static void paio_process_event(struct paio_context *ctx,
                               struct io_event *event);

void
paio_api_destroy(struct paio_api *api) {
    plat_log_msg(21812, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "paio_api %p destroy", api);

    return ((*api->api_destroy)(api));
}

int
paio_setup(struct paio_api *api, int maxevents, struct paio_context **ctxp) {
    int ret;

    ret = (*api->io_setup)(api, maxevents, ctxp);

    plat_log_msg(21813, LOG_CAT,
                 !ret ?  PLAT_LOG_LEVEL_TRACE : PLAT_LOG_LEVEL_WARN,
                 "paio_api %p setup paio_context %p maxevents %d = %d", api,
                 *ctxp, maxevents, ret);

    return (ret);
}

int
paio_destroy(struct paio_context *ctx) {
    plat_log_msg(21814, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "paio_context %p destroy", ctx);

    return ((*ctx->api->io_destroy)(ctx));
}

int
paio_submit(struct paio_context *ctx, long nr, struct iocb *ios[]) {
    int ret;
    int i;

    ret = ((*ctx->api->io_submit)(ctx, nr, ios));

    if (ret != -1) {
        for (i = 0; i < ret; ++i) {
            switch (ios[i]->aio_lio_opcode) {
            case IO_CMD_PWRITE:
            case IO_CMD_PREAD:
                plat_log_msg(21815, LOG_CAT,
                             PLAT_LOG_LEVEL_TRACE,
                             "paio_context %p submit %d of %ld iocb %p"
                             " op %d fd %d buf %p nbytes %ld offset %lld",
                             ctx, i, nr, ios[i], ios[i]->aio_lio_opcode,
                             ios[i]->aio_fildes, ios[i]->u.c.buf,
                             ios[i]->u.c.nbytes, ios[i]->u.c.offset);
                break;
            default:
                plat_log_msg(21816, LOG_CAT,
                             PLAT_LOG_LEVEL_TRACE,
                             "paio_context %p submit %d of %ld iocb %p"
                             " op %d fd %d",
                             ctx, i, nr, ios[i], ios[i]->aio_lio_opcode,
                             ios[i]->aio_fildes);
                break;
            }

            if (ios[i]->aio_lio_opcode == IO_CMD_PWRITE) {
                (void) __sync_add_and_fetch(&ctx->writes_inflight_count, 1);
                (void) __sync_add_and_fetch(&ctx->writes_inflight_bytes,
                                            ios[i]->u.c.nbytes);
            }
        }
    }

    plat_log_msg(21817, LOG_CAT,
                 ret != -1 ?  PLAT_LOG_LEVEL_TRACE : PLAT_LOG_LEVEL_WARN,
                 "paio_context %p submit nr %ld write ios %ld bytes %ld = %d",
                 ctx, nr, (long)ctx->writes_inflight_count,
                 (long)ctx->writes_inflight_bytes, ret);

    return (ret);
}

int
paio_cancel(struct paio_context *ctx, struct iocb *iocb, struct io_event *evt) {
    int ret;

    ret = (*ctx->api->io_cancel)(ctx, iocb, evt);

    plat_log_msg(21818, LOG_CAT,
                 !ret ? PLAT_LOG_LEVEL_TRACE : PLAT_LOG_LEVEL_DIAGNOSTIC,
                 "paio_context %p cancel iocb %p op %d fd %d  = %d",
                 ctx, iocb, iocb->aio_lio_opcode, iocb->aio_fildes, ret);

    if (!ret) {
        paio_process_event(ctx, evt);
    }

    return (ret);
}

long
paio_getevents(struct paio_context *ctx, long min_nr, long nr,
               struct io_event *events, struct timespec *timeout) {
    long ret;
    int i;

    ret = (*ctx->api->io_getevents)(ctx, min_nr, nr, events, timeout);

    if (ret > 0) {
        for (i = 0; i < ret; ++i) {
            paio_process_event(ctx, events + i);
        }
    }

    plat_log_msg(21819, LOG_CAT,
                 ret != -1 ?  PLAT_LOG_LEVEL_TRACE : PLAT_LOG_LEVEL_WARN,
                 "paio_context %p getevents min_nr %ld nr %ld"
                 " write ios %ld bytes %ld = %ld",
                 ctx, min_nr, nr, (long)ctx->writes_inflight_count,
                 (long)ctx->writes_inflight_bytes, ret);

    return (ret);
}

static void
paio_process_event(struct paio_context *ctx, struct io_event *event) {
    struct iocb *iocb;
    iocb = event->obj;
    if (iocb) {
        switch (iocb->aio_lio_opcode) {
        case IO_CMD_PWRITE:
        case IO_CMD_PREAD:
            plat_log_msg(21820, LOG_CAT,
                         event->res2 || event->res != iocb->u.c.nbytes ?
                         PLAT_LOG_LEVEL_WARN : PLAT_LOG_LEVEL_TRACE,
                         "paio_context %p complete iocb %p"
                         " op %d fd %d buf %p nbytes %ld offset %lld"
                         " res %ld res2 %ld",
                         ctx, iocb, iocb->aio_lio_opcode, iocb->aio_fildes,
                         iocb->u.c.buf, iocb->u.c.nbytes, iocb->u.c.offset,
                         event->res, event->res2);
            break;

        default:
            plat_log_msg(21821, LOG_CAT,
                         event->res2 || event->res != iocb->u.c.nbytes ?
                         PLAT_LOG_LEVEL_WARN : PLAT_LOG_LEVEL_TRACE,
                         "paio_context %p complete iocb %p"
                         " op %d fd %d res %ld res2 %ld",
                         ctx, iocb, iocb->aio_lio_opcode, iocb->aio_fildes,
                         event->res, event->res2);
            break;
        }
        
        if (iocb->aio_lio_opcode == IO_CMD_PWRITE) {
            (void) __sync_fetch_and_sub(&ctx->writes_inflight_count, 1);
            (void) __sync_fetch_and_sub(&ctx->writes_inflight_bytes,
                                        iocb->u.c.nbytes);
            (void) __sync_fetch_and_add(&ctx->writes_completed_count, 1);
            (void) __sync_fetch_and_add(&ctx->writes_completed_bytes,
                                        iocb->u.c.nbytes);
        }
    }
}

int
paio_get_writes_inflight_count(struct paio_context *ctx) {
    return (ctx->writes_inflight_count);
}

long
paio_get_writes_inflight_bytes(struct paio_context *ctx) {
    return (ctx->writes_inflight_count);
}

int64_t
paio_get_writes_completed_count(struct paio_context *ctx) {
    return (ctx->writes_completed_count);
}

int64_t
paio_get_writes_completed_bytes(struct paio_context *ctx) {
    return (ctx->writes_completed_count);
}
