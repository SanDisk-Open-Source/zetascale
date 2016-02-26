/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/sdfmsg/sdf_fth_mbx.c
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
 * Trivial response action allocator/deallocator functions used for
 * the deprecated sdf_fth_mbx structure.
 */

#include "platform/stdlib.h"

#include "sdf_fth_mbx.h"
#include "sdf_msg_action.h"
#include "sdf_msg_wrapper.h"

static struct sdf_msg_action *
sdf_fth_mbx_deliver_ack_action(sdf_fth_mbx_t *ackmbx, struct sdf_msg *msg,
                               struct sdf_msg_action *old_action);

static struct sdf_msg_action *
sdf_fth_mbx_deliver_resp_action(sdf_fth_mbx_t *ackmbx, struct sdf_msg *msg,
                                struct sdf_msg_action *old_action);

/** @brief Allocate #sdf_fth_mbx which directs messages to closure */
struct sdf_fth_mbx *
sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_t closure,
                               enum SDF_msg_SACK_rel release,
                               int64_t timeout_usec) {
    int failed;
    struct sdf_fth_mbx *ret;

    failed = !plat_alloc_struct(&ret);
    if (!failed) {
        ret->actlvl = SACK_MODERN;
        ret->aaction = NULL;
        ret->raction = sdf_msg_action_closure_alloc(closure);
        failed = !ret->raction;
        ret->release_on_send = (release == SACK_REL_YES);
        ret->timeout_usec = timeout_usec;
    }

    if (failed && ret) {
        sdf_fth_mbx_free(ret);
        ret = NULL;
    }

    return (ret);
}

struct sdf_fth_mbx *
sdf_fth_mbx_resp_mbox_alloc(fthMbox_t *mbox, enum SDF_msg_SACK_rel release,
                            int64_t timeout_usec) {
    int failed;
    struct sdf_fth_mbx *ret;

    failed = !plat_alloc_struct(&ret);
    if (!failed) {
        ret->actlvl = SACK_MODERN;
        ret->aaction = NULL;
        ret->raction = sdf_msg_action_fth_mbox_alloc(mbox);
        failed = !ret->raction;
        ret->release_on_send = (release == SACK_REL_YES);
        ret->timeout_usec = timeout_usec;
    }

    if (failed && ret) {
        sdf_fth_mbx_free(ret);
        ret = NULL;
    }

    return (ret);
}

struct sdf_fth_mbx *
sdf_fth_mbx_copy(const struct sdf_fth_mbx *mbox) {
    struct sdf_fth_mbx *ret;
    int failed;

    failed = !plat_alloc_struct(&ret);
    if (!failed) {
        *ret = *mbox;
        if (ret->actlvl == SACK_MODERN) {
            if (mbox->aaction) {
                ret->aaction = sdf_msg_action_copy(mbox->aaction);
                if (!ret->aaction) {
                    failed = 1;
                }
            }
            if (mbox->raction) {
                ret->raction = sdf_msg_action_copy(mbox->raction);
                if (!ret->raction) {
                    failed = 1;
                }
            }
        }
    }

    if (failed && ret) {
        sdf_fth_mbx_free(ret);
        ret = NULL;
    }

    return (ret);
}

void
sdf_fth_mbx_free(struct sdf_fth_mbx *mbox)  {
    if (mbox) {
        if (mbox->actlvl == SACK_MODERN) {
            if (mbox->aaction) {
                sdf_msg_action_free(mbox->aaction);
            }
            if (mbox->raction) {
                sdf_msg_action_free(mbox->raction);
            }
        }
        plat_free(mbox);
    }
}

int
sdf_fth_mbx_deliver_ack(struct sdf_fth_mbx *ackmbx, struct sdf_msg *msg) {
    struct sdf_msg_action *action_ptr;
    struct sdf_msg_action action;

    action_ptr = sdf_fth_mbx_deliver_ack_action(ackmbx, msg,  &action);
    return (action_ptr ? sdf_msg_action_apply(action_ptr, msg) : 0);
}

int
sdf_fth_mbx_deliver_ack_wrapper(sdf_fth_mbx_t *ackmbx,
                                struct sdf_msg_wrapper *wrapper) {
    struct sdf_msg_action *action_ptr;
    struct sdf_msg_action action;
    struct sdf_msg *msg;

    plat_assert(wrapper);
    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, wrapper);

    action_ptr = sdf_fth_mbx_deliver_ack_action(ackmbx, msg,  &action);

    sdf_msg_wrapper_rwrelease(&msg, wrapper);

    return (action_ptr ? sdf_msg_action_apply_wrapper(action_ptr, wrapper) : 0);
}

static struct sdf_msg_action *
sdf_fth_mbx_deliver_ack_action(sdf_fth_mbx_t *ackmbx, struct sdf_msg *msg,
                               struct sdf_msg_action *action_ptr) {
    struct sdf_msg_action *action;

    plat_assert(msg);
    plat_assert(ackmbx);

    if (ackmbx->actlvl != SACK_MODERN) {
        action = action_ptr;
        action->how = sdf_msg_sack_ack((SDF_msg_SACK) ackmbx->actlvl);
        action->what.mbox = ackmbx->abox;
    } else {
        action = ackmbx->aaction;
    }

    return (action);
}

int
sdf_fth_mbx_deliver_resp(sdf_fth_mbx_t *ackmbx, struct sdf_msg *msg) {
    struct sdf_msg_action *action;
    struct sdf_msg_action old_action;

    action = sdf_fth_mbx_deliver_resp_action(ackmbx, msg,  &old_action);
    return (action ? sdf_msg_action_apply(action, msg) : 0);
}

int
sdf_fth_mbx_deliver_resp_wrapper(sdf_fth_mbx_t *ackmbx,
                                 struct sdf_msg_wrapper *wrapper) {
    struct sdf_msg_action *action_ptr;
    struct sdf_msg_action action;
    struct sdf_msg *msg;

    plat_assert(wrapper);
    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, wrapper);

    action_ptr = sdf_fth_mbx_deliver_resp_action(ackmbx, msg,  &action);

    sdf_msg_wrapper_rwrelease(&msg, wrapper);

    return (action_ptr ? sdf_msg_action_apply_wrapper(action_ptr, wrapper) : 0);
}

static struct sdf_msg_action *
sdf_fth_mbx_deliver_resp_action(sdf_fth_mbx_t *ackmbx, struct sdf_msg *msg,
                                struct sdf_msg_action *action_ptr) {
    struct sdf_msg_action *action;

    plat_assert(msg);
    plat_assert(ackmbx);

    if (ackmbx->actlvl != SACK_MODERN) {
        action = action_ptr;
        action->how = sdf_msg_sack_resp((SDF_msg_SACK) ackmbx->actlvl);
        action->what.mbox = ackmbx->rbox;
    } else {
        action = ackmbx->raction;
    }

    return (action);
}
