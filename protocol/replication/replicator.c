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
 * File:   sdf/protocol/replication/replicator.c
 *
 * Author: drew
 *
 * Created on April 18, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: replicator.c 1480 2008-06-05 09:23:13Z drew $
 */

#include "platform/assert.h"
#include "platform/closure.h"
#include "platform/stdio.h"
#include "platform/string.h"

#include "sdfmsg/sdf_msg_types.h"
/* For SDF_FTH_MBX_TIMEOUT_ONLY_ERROR */
#include "sdfmsg/sdf_fth_mbx.h"

#include "replicator.h"

static void sr_command_cb(plat_closure_scheduler_t *context, void *env,
                          SDF_status_t status, char *output);

struct sr_command_state {
    fthMbox_t mbox;
    SDF_status_t status;
    char *output;
};

SDF_status_t
sdf_replicator_command_sync(struct sdf_replicator *replicator,
                            SDF_shardid_t shard,
                            const char *command, char **output) {
    struct sr_command_state *state;
    SDF_status_t ret;
    sdf_replicator_command_cb_t cb;

    if (!plat_calloc_struct(&state)) {
        ret = SDF_FAILURE_MEMORY_ALLOC;
    } else {
        fthMboxInit(&state->mbox);
        cb = sdf_replicator_command_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                              &sr_command_cb, state);
        sdf_replicator_command_async(replicator, shard, command, cb);
        fthMboxWait(&state->mbox);
        if (output) {
            *output = state->output;
        } else if (state->output) {
            plat_free(state->output);
        }
        ret = state->status;
        plat_free(state);
    }

    return (ret);
}

static void
sr_command_cb(plat_closure_scheduler_t *context, void *env,
              SDF_status_t status, char *output) {
    struct sr_command_state *state = (struct sr_command_state *)env;

    state->status = status;
    state->output = output;
    fthMboxPost(&state->mbox, 0);
}

/** @brief Provide default initialization */
void
sdf_replicator_config_init(struct sdf_replicator_config *config,
                           vnode_t my_node, size_t node_count) {
    memset(config, 0, sizeof (*config));
    config->my_node = my_node;
    config->node_count = node_count;

    config->replication_service = SDF_REPLICATION;

    config->replication_peer_service = SDF_REPLICATION_PEER;

    config->flash_service = SDF_FLSH;

    config->response_service = SDF_RESPONSES;

    /*
     * Entirely arbitrary.  We should find out customer requirements
     * for switch-over.
     */
    config->lease_usecs = 2000000;

    /* Arbitrary, but should be >> expected switch over time */
    config->switch_back_timeout_usecs =  30 * PLAT_MILLION;

    /*
     * Currently arbitrary.  Needs to be based on how many operations
     * we need to keep in-flight to keep the drives busy.  Should have
     * a similar value which determines queue depth for async replication.
     */
    config->outstanding_window = 100000;

    /* Arbitrrary */
    config->recovery_ops = 100;

    /*
     * Default to liveness based operation.
     *
     * XXX: drew 2009-06-10 Originally I thought that timeouts were the
     * mechanism to cleanup operations initiated to nodes that died but
     * then realized it was more robust, efficient, and simple to just
     * drive failures off the liveness system.  This should probably
     * stop being a tunable.
     */
    config->timeout_usecs = SDF_FTH_MBX_TIMEOUT_ONLY_ERROR;

    /*
     * Default to 1ms timer granularity.
     *
     * XXX: drew 2009-06-10 While arbitrary, this may need to figure into the
     * epsilon adjustment for lease intervals and other timeouts.
     */
    config->timer_poll_usecs = 1000;
}


const char *
sdf_replicator_access_to_string(enum sdf_replicator_access access) {
    switch (access) {
#define item(caps, lower) \
    case caps: return (#lower);
    SDF_REPLICATOR_ACCESS_ITEMS()
#undef item
    default:
        return ("Invalid");
    }
}

char *
sdf_replicator_events_to_string(int events) {
    /* Be paranoid and find maximum buffer size */
    static __thread char buf[] =
#define item(caps, lower, value) #lower " "
        SDF_REPLICATOR_EVENT_ITEMS();
#undef item
    int got;
    char *last;

    /*
     * Use a temporary for format because we can't have a redefinition of
     * item within a macro and we don't know if plat_asprintf is.
     */
    const char fmt[] =
#define item(caps, lower, value) "%s"
        SDF_REPLICATOR_EVENT_ITEMS();
#undef item

#define item(caps, lower, value) \
    /* cstyle */, (events & (caps)) ? #lower " " : ""
    got = snprintf(buf, sizeof (buf), fmt SDF_REPLICATOR_EVENT_ITEMS());
#undef item
    plat_assert(got < sizeof (buf));

    last = buf + strlen(buf) - 1;
    if (*last == ' ') {
        *last = 0;
    }

    return (buf);
}

struct sdf_replicator_config *
sdf_replicator_config_copy(const struct sdf_replicator_config *config) {
    struct sdf_replicator_config *ret;
    int failed;

    failed = !plat_calloc_struct(&ret);
    if (!failed) {
        *ret = *config;
        if (config->vip_config) {
            ret->vip_config = sdf_vip_config_copy(config->vip_config);
            failed = !ret->vip_config;
        }
    }

    if (failed && ret) {
        sdf_replicator_config_free(ret);
        ret = NULL;
    }

    return (ret);
}

void
sdf_replicator_config_free(struct sdf_replicator_config *config) {
    if (config) {
        if (config->vip_config) {
            sdf_vip_config_free(config->vip_config);
        }
        plat_free(config);
    }
}
