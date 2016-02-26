//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/*
 * File:   sdf/sdfmsg/sdf_msg_action.c
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

#include <time.h>

#include "platform/stdlib.h"
#include "platform/stdio.h"
#include "platform/logging.h"
#include "sdf_msg_types.h"
#include "sdf_msg_wrapper.h"
#include "sdf_msg_action.h"

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

typedef uint64_t msgtime_t;
msgtime_t get_the_nstimestamp();


/*
 * XXX: These can all be thin wrappers around a closure once we deal with
 * the backwards compatable old modes.
 */

struct sdf_msg_action *
sdf_msg_action_closure_alloc(sdf_msg_recv_wrapper_t closure) {
    struct sdf_msg_action *ret;

    plat_alloc_struct(&ret);
    if (ret) {
        ret->how = SACK_HOW_CLOSURE_MSG_WRAPPER;
        ret->what.wrapper_closure = closure;
    }

    return (ret);
}

struct sdf_msg_action *
sdf_msg_action_fth_mbox_alloc(fthMbox_t *mbox) {
    struct sdf_msg_action *ret;

    plat_alloc_struct(&ret);
    if (ret) {
        ret->how = SACK_HOW_FTH_MBOX_MSG;
        ret->what.mbox = mbox;
    }

    return (ret);
}

struct sdf_msg_action *
sdf_msg_action_copy(const struct sdf_msg_action *action) {
    struct sdf_msg_action *ret;

    plat_alloc_struct(&ret);
    if (ret) {
        *ret = *action;
    }

    return (ret);
}

void
sdf_msg_action_free(struct sdf_msg_action *action) {
    plat_free(action);
}

int
sdf_msg_action_apply(struct sdf_msg_action *action, struct sdf_msg *msg) {
    uint64_t ackme;
    struct sdf_msg_wrapper *wrapper;
    int ret = 0;

    plat_log_msg(21485, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: what action-how %s mbox %p\n", msg->msg_src_vnode,
                 SDF_msg_SACK_how_to_string(action->how), action->what.mbox);

    switch (action->how) {
    case SACK_HOW_FTH_MBOX_TIME:
        plat_assert(action->what.mbox);
        ackme = get_the_nstimestamp();
        fthMboxPost(action->what.mbox, ackme);
        break;

    case SACK_HOW_FTH_MBOX_MSG:
        plat_assert(action->what.mbox);
        fthMboxPost(action->what.mbox, (uint64_t)msg);
        break;

    case SACK_HOW_CLOSURE_MSG_WRAPPER:
        plat_assert(!sdf_msg_recv_wrapper_is_null(&action->what.wrapper_closure));
        wrapper = sdf_msg_wrapper_recv(msg);
        plat_assert(wrapper);
        plat_closure_apply(sdf_msg_recv_wrapper, &action->what.wrapper_closure,
                           wrapper);
        break;

    case SACK_HOW_NONE:
        break;
    }

    return (ret);
}

int
sdf_msg_action_apply_wrapper(struct sdf_msg_action *action,
                             struct sdf_msg_wrapper *wrapper) {
    uint64_t ackme;
    struct sdf_msg *msg;
    int ret = 0;

    plat_log_msg(21485, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: what action-how %s mbox %p\n", wrapper->src_vnode,
                 SDF_msg_SACK_how_to_string(action->how), action->what.mbox);

    switch (action->how) {
    case SACK_HOW_FTH_MBOX_TIME:
        plat_assert(action->what.mbox);
        ackme = get_the_nstimestamp();
        fthMboxPost(action->what.mbox, ackme);
        break;

    case SACK_HOW_FTH_MBOX_MSG:
        msg = sdf_msg_wrapper_to_recv_msg_alloc(wrapper);
        plat_assert(msg);
        plat_assert(action->what.mbox);
        fthMboxPost(action->what.mbox, (uint64_t)msg);
        break;

    case SACK_HOW_CLOSURE_MSG_WRAPPER:
        plat_assert(!sdf_msg_recv_wrapper_is_null(&action->what.wrapper_closure));
        plat_closure_apply(sdf_msg_recv_wrapper, &action->what.wrapper_closure,
                           wrapper);
        break;

    case SACK_HOW_NONE:
        break;
    }

    return (ret);
}

msgtime_t
get_the_nstimestamp() {
    /* What environment variable needs to be set to get clock_gettime() */
    struct timespec curtime;

    (void) clock_gettime(CLOCK_REALTIME, &curtime);
    return (curtime.tv_nsec);
}
