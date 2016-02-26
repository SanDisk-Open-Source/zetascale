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

#ifndef _SDF_MSG_ACTION_H
#define _SDF_MSG_ACTION_H

/*
 * File:   sdf/sdfmsg/sdf_msg_action.h
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
 * Action associated with solicited or unsolicited msg activity including
 * inbound messages, response to message sent, etc.
 *
 * XXX: We currently don't address the shutdown use case with the
 * problem being that message delivery is asynchronous.   Most reasonable
 * would be an asynchronous shutdown() as in sdf/platform which cuts off
 * new messages and fires a closure on completion which can integrate to
 * whatever event/message/threading model is used.  An ack would be needed
 * for queued responses which could be integrated into wrapping code for
 * composition to provide other APIs.
 *
 * XXX: We currently only implement the initial set of actions. Some one
 * might want pthread based messaging, etc.  Add appropriate constructors
 * here.  With sdf_msg_action initialized by constructors (with the
 * constructor/destructor paradigm necessary to accomodate asynchronous
 * delivery with shutdown) it may be more reasonable to make the internals
 * closure based.
 *
 * FIXME: SACK_HOW_FTH_MBOX_TIME will not deliver the actual time
 * in simulated environments.  Otherwise it's safe for use in simulated
 * environments (notably sdf/protocol/replication/tests)
 */

#include "platform/defs.h"
#include "platform/closure.h"

#include "fth/fthMbox.h"

#include "sdf_msg_types.h"

struct sdf_msg_wrapper;

/**
 * @brief Closure to receive a message
 *
 * The receiver shall consume one reference count when it is
 * through with the message.
 *
 * XXX: drew 2008-12-29 For historical reasons which resulted in a combined
 * envelope and message, the message header fields (source and destination
 * node, service) may be incorrect.  The message wrapper's fields should
 * be used instead.
 *
 */
PLAT_CLOSURE1(sdf_msg_recv_wrapper, struct sdf_msg_wrapper *, msg_wrapper)

typedef struct sdf_msg_action {
    enum SDF_msg_SACK_how how;       /* How now */

    union {
        fthMbox_t *mbox;
        sdf_msg_recv_wrapper_t wrapper_closure;
    } what;
} sdf_msg_action_t;

__BEGIN_DECLS

/**
 * @brief Allocate #sdf_msg_action which directs to a closure
 *
 * See #sdf_msg_recv_wrapper in sdf_msg_action.h for notes on reference
 * counting, envelope and message header validity, etc.
 */
struct sdf_msg_action *
sdf_msg_action_closure_alloc(sdf_msg_recv_wrapper_t closure);

/** @brief Allocate #sdf_msg_action which directs to a mbox */
struct sdf_msg_action *
sdf_msg_action_fth_mbox_alloc(fthMbox_t *mbox);

/** @brief Allocate #sdf_msg_action which directs timestamps to mbox */
struct sdf_msg_action *
sdf_msg_action_fth_ack_mbox_alloc(fthMbox_t *mbox);

struct sdf_msg_action *sdf_msg_action_copy(const struct sdf_msg_action *action);

void sdf_msg_action_free(struct sdf_msg_action *action);

/**
 * @brief Apply to msg
 *
 * FIXME: Whether or not the action assumes ownership of msg depends on its
 * type.  #sdf_msg_action_fth_ack_mbox_alloc currently does not assume
 * ownership.  #sdf_msg_action_closure_alloc wraps the msg_reference in a
 * a reference counted sdf_msg_wrapper.  #sdf_msg_action_fth_mbox_alloc
 * passes the message along.
 *
 * With this fixed we should junk the sdf_fth_mbx release_on_send field;
 * with no action implying release and the action otherwise assuming
 * ownership for consistent behavior.  OTOH, a wrapper based send will
 * simplify this.
 */
int sdf_msg_action_apply_wrapper(struct sdf_msg_action *action,
                                 struct sdf_msg_wrapper *msg_wrapper);

/**
 * @brief Apply to msg
 *
 * FIXME: Whether or not the action assumes ownership of msg depends on its
 * type.  #sdf_msg_action_fth_ack_mbox_alloc currently does not assume
 * ownership.  #sdf_msg_action_closure_alloc wraps the msg_reference in a
 * a reference counted sdf_msg_wrapper.  #sdf_msg_action_fth_mbox_alloc
 * passes the message along.
 *
 * With this fixed we should junk the sdf_fth_mbx release_on_send field;
 * with no action implying release and the action otherwise assuming
 * ownership for consistent behavior.  OTOH, a wrapper based send will
 * simplify this.
 */
int sdf_msg_action_apply(struct sdf_msg_action *action, struct sdf_msg *msg);


__END_DECLS

#endif /* _SDF_MSG_ACTION_H */
