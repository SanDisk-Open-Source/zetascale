/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf_msg_pthread_example.c
 * Author: Tom Riddle
 *
 * Created on March 20, 2008, 11:45 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_pthread_example.c 308 2008-02-20 22:34:58Z tomr $
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
#define PLAT_OPTS_NAME(name) name ## _mpilogme
#include "platform/opts.h"

#include "sdfmsg/sdf_msg_types.h"

#define DBGP 0

PLAT_LOG_CAT_LOCAL(LOG_CAT, "sdf/sdfmsg/tests/mpilog");

static service_t my_protocol = SDF_MANAGEMENT;

/*
 * This pthread will be used to simulate the SDF_MANAGEMENT sending and
 * receiving messages to the sdf messaging engine
 */

void *
sdf_msg_pthread_test(void *arg)
{
    int *myid = (int *)arg;
    struct sdf_queue_pair *q_pair_MANAGEMENT;
    struct sdf_msg *send_msg = NULL, *recv_msg = NULL;
    vnode_t node;
    serviceid_t protocol = SDF_MANAGEMENT;
    msg_type_t type;
    int i, j, ret;
    int debug = 0;
    sdf_fth_mbx_t fthmbx;

    /* FIXME we probably need to expand the fthmbx structure to include pthread items */

    fthmbx.actlvl = 100;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Testing pthread MESSAGING SDF_MANGEMENT Communication\n", *myid);

    /*
     * Create a queue pair from this thread servicing my_protocol to the thread
     * on another node
     */

    while(!q_pair_MANAGEMENT) {
        usleep(10000);
        q_pair_MANAGEMENT = sdf_msg_get_qpairs(my_protocol, *myid);
        if (!q_pair_MANAGEMENT) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Cannot find qpair %p sn %d dn %d ss %d ds %d\n",
                         *myid, q_pair_MANAGEMENT, *myid, *myid == 0 ? 1 : 0, my_protocol, SDF_MANAGEMENT);
	    }
        else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: got SYS queue pair %p sn %d dn %d ss %d ds %d\n",
                     *myid, q_pair_MANAGEMENT, *myid, *myid == 0 ? 1 : 0, my_protocol, SDF_MANAGEMENT);
        }
    }

if (debug)
plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
"\nNode %d MNGMT TEST THREAD STOPPING HERE FOR DEBUG\n",
                    *myid);
while(debug);

    node = (*myid == 1 ? 0 : 1);
    j = 0;
    for (;;) {

    send_msg = (struct sdf_msg *) sdf_msg_alloc(1024); 
    if (send_msg == NULL) {
        printf("sdf_msg_alloc(1024) failed\n");
        return ((void *)1);
    }

        if (DBGP) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d msg %p msg->msg_payload %p\n",
                         *myid, send_msg, send_msg->msg_payload);
        }
        /* Fill in body of message with test data */
        for (i = 0; i < 1024; ++i)
            send_msg->msg_payload[i] = (unsigned char) i;

        protocol = SDF_MANAGEMENT;  /* num value of 3 */
        type = MGMT_REQUEST;        /* num value of 1 */

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: SENDING MGMNT MSG dnode %d, proto %d, type %d loop num %d\n",
                     *myid, node, protocol, type, j);
	if (!j) {
            ret = sdf_msg_send((struct sdf_msg *)send_msg, 1024, node, protocol,
                               *myid, my_protocol, type, &fthmbx, NULL);

            if (DBGP) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: sdf_msg_send returned %d\n", *myid, ret);
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE, 
                             "\nNode %d: %s: calling sdf_msg_receive(%p, %d, %d)\n",
                             *myid, __func__, q_pair_MANAGEMENT->q_out, 0, B_TRUE);
            }
	}

        recv_msg = sdf_msg_receive(q_pair_MANAGEMENT->q_out, 0, B_TRUE);

	if (DBGP) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: back from sdf_msg_receive with msg %p\n",
                         *myid, recv_msg);
	}
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
           "\nNode %d: RECEIVING MSG vers %d clusterid %d ss %d ds %d sn %d dn %d type %d\n",
           *myid, recv_msg->msg_version, recv_msg->msg_clusterid,
           recv_msg->msg_src_service, recv_msg->msg_dest_service,
           recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode, recv_msg->msg_type);

        sdf_msg_free_buff(recv_msg);
        msg_sys_release++; /* global sync for SYS mangmt thread init */
	sleep(5);
	j++;
        }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d Exiting pthread MANGEMENT Tester\n", *myid);
    return (0);
}
