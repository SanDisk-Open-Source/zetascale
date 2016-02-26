/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fcnl_system_lib1.c
 * Author: Norman Xu
 *
 * Created on June 23, 2008, 7:45AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcnl_system_lib1.c 308 2008-06-23 22:34:58Z normanxu $
 */

/* pthread test example for sending messages */

#ifdef MPI_BUILD
#include <mpi.h>
#endif

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>

#include "platform/logging.h"
//#define PLAT_OPTS_NAME(name) name ## _mpilogme
#include "platform/opts.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_types.h"
#include "fcnl_test.h"
#include "Utilfuncs.h"
#include "log.h"

extern struct test_info * info;
#define DBGP 0
#define TSZE 256
#define SHORTTEST 2
#define UNEXPT_TEST 0
#define FASTPATH_TEST 0

PLAT_LOG_CAT_LOCAL(LOG_CAT, "sdf/sdfmsg/tests/mpilog");

static service_t my_protocol = SDF_SYSTEM;
static int cluster_node[MAX_NUM_SCH_NODES];

/*
 * This pthread will be used to simulate the SDF_SYSTEM sending and
 * receiving messages to the sdf messaging engine
 */

void * SystemPthreadRoutine(void *arg) {
    int *myidt = (int *) arg;
    int myid = *myidt;
    struct sdf_queue_pair *q_pair_SYSTEM = NULL;
    struct sdf_msg *send_msg = NULL, *recv_msg = NULL;
    vnode_t node;
    serviceid_t protocol = SDF_SYSTEM;
    msg_type_t type;
    int i, j, ret;
    int debug = 0;
    sdf_fth_mbx_t fthmbx;

    /* FIXME we probably need to expand the fthmbx structure to include pthread items */
    int msg_sys_release = 0;
    fthmbx.actlvl = SACK_NONE_FTH;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Testing pthread MESSAGING SDF_SYSTEM Communication\n",
            myid);

    /*
     * Create a queue pair from this thread servicing my_protocol to the thread
     * on another node
     */
    /* node is the destination node */
    int localpn, actmask;
    uint32_t numprocs;
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    if (numprocs == 1) {
        node = 0;
    }
    else {
        node = local_get_pnode(localrank, localpn, numprocs);
        printf("Node %d: %s my pnode is  %d\n", localrank, __func__, node);
        fflush(stdout);
        for (i = 0; i < numprocs; i++) {
            printf("Node %d: %s cluster_node[%d] = %d\n", localrank, __func__, i, cluster_node[i]);
            fflush(stdout);
        }
    }

    while (!q_pair_SYSTEM) {
        usleep(10000);
        q_pair_SYSTEM = sdf_msg_getsys_qpairs(my_protocol, myid, node);
        info->queue_pair_info->queue_add[0] = q_pair_SYSTEM->q_in;
        info->queue_pair_info->queue_add[1] = q_pair_SYSTEM->q_out;
        info->queue_pair_info->queue_pair_type = my_protocol;
        if (!q_pair_SYSTEM) {
            fprintf(stderr,
                    "\nNode %d: Cannot find qpair %p sn %d dn %d ss %d ds %d\n",
                    myid, q_pair_SYSTEM, myid, myid == 0 ? 1 : 0, my_protocol,
                    SDF_SYSTEM);
			return ((void *) 1);
        } else {
            plat_log_msg(
                    PLAT_LOG_ID_INITIAL,
                    LOG_CAT,
                    PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: got SYS queue pair %p sn %d dn %d ss %d ds %d\n",
                    myid, q_pair_SYSTEM, myid, myid == 0 ? 1 : 0, my_protocol,
                    SDF_SYSTEM);
        }
    }

    if (debug)
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d SYSTEM TEST THREAD STOPPING HERE FOR DEBUG\n", myid);
    while (debug);

    j = 0;
    sdf_msg_startmsg(myid, 0, NULL);
    for (;;) {

        send_msg = (struct sdf_msg *) sdf_msg_alloc(TSZE);
        if (send_msg == NULL) {
            printf("sdf_msg_alloc(%d) failed\n", TSZE);
            return ((void *) 1);
        }

        if (DBGP) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d msg %p msg->msg_payload %p\n", myid, send_msg,
                    send_msg->msg_payload);
        }
        /* Fill in body of message with test data */
        for (i = 0; i < TSZE; ++i)
            send_msg->msg_payload[i] = (unsigned char) i;

        protocol = SDF_SYSTEM;
        type = SYS_REQUEST;

        plat_log_msg(
                PLAT_LOG_ID_INITIAL,
                LOG_CAT,
                PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: SENDING SYSTEM MSG dnode %d, proto %d, type %d loop num %d\n",
                myid, node, protocol, type, j);
        if (j < 8) {
            ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node,
                    protocol, myid, my_protocol, type, &fthmbx, NULL);

            if (DBGP) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE,
                        "\nNode %d: sdf_msg_send returned %d\n", myid, ret);
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE,
                        "\nNode %d: %s: calling sdf_msg_receive(%p, %d, %d)\n",
                        myid, __func__, q_pair_SYSTEM->q_out, 0, B_TRUE);
            }
        }

        //receive msg
        recv_msg = sdf_msg_receive(q_pair_SYSTEM->q_out, 0, B_TRUE);

        //unsigned char* m = (unsigned char *) recv_msg;
		#if 0
        for (i = 0; i < 256; i++) {
            printf(" %02x", *m);
            m++;
            if ((i % 16) == 15) {
                printf("  myid %d", myid);
                putchar('\n');
                fflush(stdout);
            }
        }
		#endif
        if (DBGP) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: back from sdf_msg_receive with msg %p\n", myid,
                    recv_msg);
        }
        plat_log_msg(
                PLAT_LOG_ID_INITIAL,
                LOG_CAT,
                PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: RECEIVING MSG vers %d clusterid %d ss %d ds %d sn %d dn %d type %d\n",
                myid, recv_msg->msg_version, recv_msg->msg_clusterid,
                recv_msg->msg_src_service, recv_msg->msg_dest_service,
                recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode,
                recv_msg->msg_type);

        sdf_msg_free_buff(recv_msg);
        msg_sys_release++; /* global sync for SYS mangmt thread init */
        sleep(5);
        j++;
        if (j == 8)
            break;
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d Exiting pthread SYSTEM Tester\n", myid);
    return (0);
}
