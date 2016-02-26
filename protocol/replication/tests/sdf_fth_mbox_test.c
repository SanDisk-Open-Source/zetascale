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
 * File:   sdf_fth_mbox_test.c
 * Author: Zhenwei Lu
 *
 * Note:simple test case using sdf_fth_mbox
 * Created on Nov 20, 2008, 12:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_fth_mbox_test.c 6180 2009-01-16 11:12:24Z yeahwo $
 */

#include "common/sdftypes.h"

#include "common/sdftypes.h"
#include "platform/prng.h"
#include "platform/time.h"
#include "platform/assert.h"
#include "platform/stdlib.h"
#include "platform/fth_scheduler.h"
#include "fth/fth.h"

#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_types.h"
#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_msg_action.h"

#include "protocol/protocol_common.h"
#include "agent/agent_helper.h"


#ifndef FTH_MBX_DEBUG
#define FTH_MBX_DEBUG
#endif

#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_PROT
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_WARN PLAT_LOG_LEVEL_WARN
#define LOG_FATAL PLAT_LOG_LEVEL_FATAL

struct fth_mbx_config {
    int nnode;
};

struct fth_mbx_node {
    struct fth_mbx_framework *framework;
    uint32_t node_id;
    struct fth_mbx_node *next_node;
    fthMbox_t inbound_mbox;
};

struct fth_mbx_framework {
    struct fth_mbx_node *nodes;
};

static struct fth_mbx_framework *framework;

void
config_init(struct fth_mbx_config *config, uint32_t nnode) {
    config->nnode = nnode;
}

SDF_boolean_t
sdf_fth_mbox_msg(struct fth_mbx_config *config, int argc, char **argv) {
    uint32_t sdf_msg_numprocs;
    SDF_boolean_t success = SDF_FALSE;
    int mpi_init_flags = SDF_MSG_NO_MPI_INIT;
    int pnodeid = -1;
    int failed;
    int rank;

    /* start sdf_msg */
#ifdef MULTI_NODE
    sdf_msg_numprocs = config->nnode;
#else
    sdf_msg_numprocs = 0;
#endif
    /* now we get our rank and the total num of processes (if any) that have been started under MPI */
    rank = sdf_msg_init_mpi(argc, argv, &sdf_msg_numprocs, &success, mpi_init_flags);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                 "rank:%d", rank);


    sdf_msg_init(rank, &pnodeid, mpi_init_flags);
    /* start sdf_msg */
    failed = sdf_msg_startmsg(rank, mpi_init_flags, NULL);
    if (!failed) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                     "start sdf_msg sucessfully!");
        success = SDF_TRUE;
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "start sdf_msg failed!");
        success = SDF_FALSE;
    }

    return (success);
}

struct fth_mbx_node *
node_alloc(struct fth_mbx_framework *framework, uint32_t node_id) {
    struct fth_mbx_node *ret = NULL;
    int failed = !plat_calloc_struct(&ret);
    if (!failed) {
        ret->framework = framework;
        ret->node_id = node_id;
        ret->next_node = NULL;
        fthMboxInit(&(ret->inbound_mbox));
    }

    if (!failed) {
#ifdef FTH_MBX_DEBUG
        plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                     "node %p %"PRIu32" allocated", ret, node_id);
#endif
    }
    return (ret);
}

struct fth_mbx_framework *
framework_alloc(struct fth_mbx_config *config) {
    struct fth_mbx_framework *ret = NULL;
    int failed = !plat_calloc_struct(&ret);

    struct fth_mbx_node *temp_node = NULL;
    int i;
    if (!failed) {
        for (i = 0; i < config->nnode; i ++) {
            temp_node = node_alloc(ret, i);
            if (!temp_node) {
                plat_assert_always(0);
            } else {
                if (!ret->nodes) {
                    ret->nodes = temp_node;
                } else {
                    temp_node->next_node = ret->nodes;
                    ret->nodes = temp_node;
                }
            }
        }
    }
#ifdef FTH_MBX_DEBUG
    plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                 "framework %p allocated", ret);
#endif
    return (ret);
}

/*
 * @brief receive Mbox and send response
 */
void
recv_and_reps(uint64_t args) {

}

/*
 * @brief send a Mbox to a specified node
 * @param framework @IN search for node associate mailbox
 * @param node      @IN node
 */
SDF_boolean_t
framework_send_message(struct fth_mbx_framework *framework, uint32_t node_to) {
    SDF_boolean_t success = SDF_FALSE;
    fthMbox_t *mbox = NULL;
    struct sdf_msg *send_msg = NULL;
    SDF_protocol_msg_t *pm = NULL;
    struct sdf_msg_action *action = NULL;
    int failed = -1;

    struct fth_mbx_node *temp_node = framework->nodes;
    while (temp_node) {
        if (temp_node->node_id != node_to) {
            temp_node = temp_node->next_node;
        } else {
            mbox = &(temp_node->inbound_mbox);
            break;
        }
    }
    if (mbox) {
#ifdef FTH_MBX_DEBUG
        plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                     "send a mbox to node %"PRIu32, node_to);
#endif
        /* construct a sdfmsg and send to node_to->mbox */
        send_msg = plat_alloc(sizeof(*send_msg)+sizeof(SDF_protocol_msg_t));
        pm = (SDF_protocol_msg_t *)send_msg->msg_payload;
        pm->node_to = node_to;
        pm->msgtype = HFPTF;

        /* construct action */
        failed = !plat_calloc_struct(&action);
        if (action) {
            action->how = SACK_HOW_FTH_MBOX_MSG;
            action->what.mbox = mbox;
            sdf_msg_action_apply(action, send_msg);
        }
        success = SDF_TRUE;
    }
    return (success);
}

void
print_sdfmsg(struct sdf_msg *msg) {
    SDF_protocol_msg_t *pm = NULL;
    pm = (SDF_protocol_msg_t *)msg->msg_payload;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                 "\n output sdfmsg:\t %d %s", pm->node_to, SDF_Protocol_Msg_Info[pm->msgtype].shortname);
}

void
node_worker(uint64_t args) {
    struct fth_mbx_node *node = (struct fth_mbx_node *)args;
    struct sdf_msg *request_msg = NULL;
#ifdef FTH_MBX_DEBUG
    plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                 "node fthread worker %p allocated", node);
#endif
    do {
        request_msg = (struct sdf_msg *)fthMboxWait(&node->inbound_mbox);
#ifdef FTH_MBX_DEBUG
        print_sdfmsg(request_msg);
#endif
    } while (request_msg);
}

void
node_start(struct fth_mbx_node *node) {
#ifdef FTH_MBX_DEBUG
    plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                 "node %"PRIu32 "started", node->node_id);
#endif
    XResume(fthSpawn(&node_worker, 40960), (uint64_t)node);
}

void
framework_start(struct fth_mbx_framework *framework) {
    struct fth_mbx_node *temp_node = framework->nodes;
    while (temp_node) {
        node_start(temp_node);
        temp_node = temp_node->next_node;
    }
}
/*
 * @brief user operations using fthread function
 */
void
user_operations(uint64_t args) {
    struct fth_mbx_framework *framework = (struct fth_mbx_framework *)args;
    fthMbox_t mbox;
    fthMboxInit(&mbox);
    framework_send_message(framework, 0);
    framework_send_message(framework, 1);
    framework_send_message(framework, 0);
}

int
main(int argc, char **argv) {
    struct fth_mbx_config *config = NULL;
    int failed;
    failed = !plat_calloc_struct(&config);

    /* start shared memory */
    init_agent_sm(0);

    /* start fthread library */
    fthInit();

    /* start sdf_msg */
    sdf_fth_mbox_msg(config, argc, argv);

    /* init config */
    config_init(config, 2 /* hard code a nnode */);

    /* framework alloc and start */
    framework = framework_alloc(config);
    framework_start(framework);


    /* user defined operations */
    XResume(fthSpawn(&user_operations, 40960), (uint64_t)framework);
    fthSchedulerPthread(0);

    return (0);
}
