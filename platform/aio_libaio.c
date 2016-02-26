/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/aio_libaio.c $
 * Author: drew
 *
 * Created on March 8, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aio_libaio.c 15015 2010-11-10 23:09:06Z briano $
 */


#include "platform/aio_api.h"
#include "platform/aio_api_internal.h"
#include "platform/aio_libaio.h"

#include "platform/alloc.h"

struct paio_libaio_api {
    /* Must be first for type-pun */
    struct paio_api base;

    /* Reference count for robustness */
    int ref_count;
};

struct paio_libaio_context {
    /* Must be first for type-pun */
    struct paio_context base;

    /** @brief Wrapped context from io_setup */
    io_context_t context;

    /** @brief #context must be released with io_destroy */
    unsigned context_initialized : 1;
};

static void paio_libaio_api_destroy(struct paio_api *api);
static void paio_libaio_api_ref_count_dec(struct paio_libaio_api *api);
static int paio_libaio_setup(struct paio_api *api_base, int maxevents,
                             struct paio_context **ctxp);
static int paio_libaio_destroy(struct paio_context *ctx_base);
static int paio_libaio_submit(struct paio_context *ctx_base, long nr,
                              struct iocb *ios[]);
static int paio_libaio_cancel(struct paio_context *ctx_base, struct iocb *iocb,
                              struct io_event *evt);
static long paio_libaio_getevents(struct paio_context *ctx_base, long min_nr,
                                  long nr, struct io_event *events,
                                  struct timespec *timeout);

struct paio_api *
paio_libaio_create(const struct paio_libaio_config *config) {
    struct paio_libaio_api *ret;

    if (plat_calloc_struct(&ret)) {
        ret->base.api_destroy = &paio_libaio_api_destroy;

        ret->base.io_setup = &paio_libaio_setup;
        ret->base.io_destroy = &paio_libaio_destroy;
        ret->base.io_submit = &paio_libaio_submit;
        ret->base.io_cancel = &paio_libaio_cancel;
        ret->base.io_getevents = &paio_libaio_getevents;

        ret->ref_count = 1;
    }

    /* Coverity does not like this safe type-pun */
#if 0
    return (&ret->base);
#else
    /* But may accept this */
    return ((struct paio_api *)ret);
#endif
}

void
paio_libaio_config_init(struct paio_libaio_config *config) {
    /* nop */
}

void
paio_libaio_config_destroy(struct paio_libaio_config *config) {
    /* nop */
}

static void
paio_libaio_api_destroy(struct paio_api *api_base) {
    struct paio_libaio_api *api = (struct paio_libaio_api *)api_base;

    if (api) {
        paio_libaio_api_ref_count_dec(api);
    }
}

static void
paio_libaio_api_ref_count_dec(struct paio_libaio_api *api) {
    int after;

    after = __sync_sub_and_fetch(&api->ref_count, 1);
    plat_assert(after >= 0);
    if (!after) {
        plat_free(api);
    }
}

static int
paio_libaio_setup(struct paio_api *api_base, int maxevents,
                  struct paio_context **ctxp) {
    struct paio_libaio_api *api = (struct paio_libaio_api *)api_base;
    struct paio_libaio_context *context;
    int ret;

    if (!plat_calloc_struct(&context)) {
        ret = ENOMEM;
    } else {
        (void) __sync_fetch_and_add(&api->ref_count, 1);
        context->base.api = api_base;
        ret = io_setup(maxevents, &context->context);
        if (!ret) {
            context->context_initialized = 1;
        }
    }

    if (ret && context) {
        paio_libaio_destroy(&context->base);
        context = NULL;
    }

    if (context) {
        *ctxp = &context->base;
    } else {
        *ctxp = NULL;
    }

    return (ret);
}

static int
paio_libaio_destroy(struct paio_context *ctx_base) {
    struct paio_libaio_context *context =
        (struct paio_libaio_context *)ctx_base;
    struct paio_libaio_api *api;
    int ret;

    if (context) {
        if (context->context_initialized) {
            ret = io_destroy(context->context);
            context->context_initialized = 0;
        } else {
            ret = 0;
        }
        api = (struct paio_libaio_api *)context->base.api;
        plat_free(context);
        paio_libaio_api_ref_count_dec(api);
    } else {
        ret = EINVAL;
    }

    return (ret);
}

static int
paio_libaio_submit(struct paio_context *ctx_base, long nr, struct iocb *ios[]) {
    struct paio_libaio_context *context =
        (struct paio_libaio_context *)ctx_base;
    int ret;

    ret = io_submit(context->context, nr, ios);

    return (ret);
}

static int
paio_libaio_cancel(struct paio_context *ctx_base, struct iocb *iocb,
                   struct io_event *evt) {
    struct paio_libaio_context *context =
        (struct paio_libaio_context *)ctx_base;
    int ret;

    ret = io_cancel(context->context, iocb, evt);

    return (ret);
}

static long
paio_libaio_getevents(struct paio_context *ctx_base, long min_nr, long nr,
                      struct io_event *events, struct timespec *timeout) {
    struct paio_libaio_context *context =
        (struct paio_libaio_context *)ctx_base;
    long ret;

    ret = io_getevents(context->context, min_nr, nr, events, timeout);

    return (ret);
}
