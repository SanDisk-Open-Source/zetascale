/*
 * File:   fcnl_bigsize_lib1.c
 * Author: Tom Riddle Norman Xu
 *
 * Created on March 20, 2008, 11:45 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcnl_bigsize_lib1.c 308 2008-02-20 22:34:58Z normanxu $
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

#define DBGP 0
#define TSZE 256
#define BIGSIZE 1000000
#define FIXSIZE 0
#define SHORTTEST 10
#define UNEXPT_TEST 0
#define FASTPATH_TEST 0

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

static service_t my_protocol = SDF_MANAGEMENT;
static int cluster_node[MAX_NUM_SCH_NODES];
extern struct test_info * info;

/*
 * This pthread will be used to simulate the SDF_MANAGEMENT sending and
 * receiving messages to the sdf messaging engine
 */

void * BigSizePthreadRoutine(void *arg) {
    int myid = *(int *) arg;
    struct sdf_queue_pair *q_pair_MANAGEMENT = NULL;
    struct sdf_msg *send_msg = NULL, *recv_msg = NULL;
    vnode_t node;
    serviceid_t protocol = SDF_MANAGEMENT;
    msg_type_t type;
    int i, j, ret;
    int debug = 0;
    sdf_fth_mbx_t fthmbx;

    /* FIXME we probably need to expand the fthmbx structure to include pthread items */
    int msg_sys_release = 0;
    fthmbx.actlvl = SACK_NONE_FTH;
   
    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Testing PTHREAD MESSAGING BIGSIZE Communication\n",
            myid);

    /*
     * Create a queue pair from this thread servicing my_protocol to the thread
     * on another node
     */
    /* node is the destination node */
    int localpn, actmask;
    uint32_t numprocs;
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    if (localrank == localpn) {
        node = localpn;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
    } else {
        node = local_get_pnode(localrank, localpn, numprocs);
	printf("Node %d: %s my pnode is  %d\n", localrank, __func__, node);
	fflush(stdout);
	for (i = 0; i < numprocs; i++) {
            printf("Node %d: %s cluster_node[%d] = %d\n", localrank, __func__, i, cluster_node[i]);
            fflush(stdout);
        }
    }

    while (!q_pair_MANAGEMENT) {
        usleep(10000);
        q_pair_MANAGEMENT = local_create_myqpairs_with_pthread(my_protocol,
                myid, node);
        info->queue_pair_info->queue_add[0] =q_pair_MANAGEMENT->q_in;
        info->queue_pair_info->queue_add[1] =q_pair_MANAGEMENT->q_out;
        if (!q_pair_MANAGEMENT) {
        	fprintf(stderr, "\nNode %d: Cannot find qpair %p sn %d dn %d ss %d ds %d\n",
                    myid, q_pair_MANAGEMENT, myid, node,
                    my_protocol, SDF_MANAGEMENT);
			return ((void *) 1);
        } else {
            plat_log_msg(
                    PLAT_LOG_ID_INITIAL,
                    LOG_CAT,
                    PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: got q_pair_MANAGEMENT queue pair %p sn %d dn %d ss %d ds %d\n",
                    myid, q_pair_MANAGEMENT, myid, node,
                    my_protocol, SDF_MANAGEMENT);
        }
    }

    if (debug)
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d MNGMT TEST THREAD STOPPING HERE FOR DEBUG\n", myid);
    while (debug);

    j = 0;

    // start the message engine
    sdf_msg_startmsg(myid, 0, NULL);
    // start the sizes off at a small level and double it each send
    int msgsize = TSZE;
    for (;;) {

        if (FIXSIZE) {
            msgsize = BIGSIZE;
        } else {
            msgsize *= 2;
        }
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"Ssend msg with size %d\n", msgsize);
        send_msg = (struct sdf_msg *) sdf_msg_alloc(msgsize);
        if (send_msg == NULL) {
            fprintf(stderr, "sdf_msg_alloc(%d) failed\n", msgsize);
            return ((void *) 1);
        }

        if (DBGP) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d msg %p msg->msg_payload %p\n", myid, send_msg,
                    send_msg->msg_payload);
        }
        /* Fill in body of message with test data */
        for (i = 0; i < msgsize; ++i)
            send_msg->msg_payload[i] = (unsigned char) i;

        protocol = SDF_MANAGEMENT; /* num value of 3 */
        type = MGMT_REQUEST; /* num value of 1 */

        plat_log_msg(
                PLAT_LOG_ID_INITIAL,
                LOG_CAT,
                PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: SENDING MGMNT MSG dnode %d, proto %d, type %d len %d loop num %d\n",
                myid, node, protocol, type, msgsize, j);
        if (1) {
            ret = sdf_msg_send((struct sdf_msg *) send_msg, msgsize, node,
                    protocol, myid, my_protocol, type, &fthmbx, NULL);

            if (DBGP) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE,
                        "\nNode %d: Completed post. sdf_msg_send returned %d\n", myid, ret);
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE,
                        "\nNode %d: %s: calling sdf_msg_receive(%p, %d, %d)\n",
                        myid, __func__, q_pair_MANAGEMENT->q_out, 0, B_TRUE);
            }
        }

        //receive msg
        recv_msg = sdf_msg_receive(q_pair_MANAGEMENT->q_out, 0, B_TRUE);

#if 0
        unsigned char* m = (unsigned char *) recv_msg;
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
                "\nNode %d: RECEIVING MSG vers %d clusterid %d ss %d ds %d sn %d dn %d type %d len %d loop %d\n",
                myid, recv_msg->msg_version, recv_msg->msg_clusterid,
                recv_msg->msg_src_service, recv_msg->msg_dest_service,
                recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode,
                recv_msg->msg_type, recv_msg->msg_len, j);


        // FIXME presently this sleep masks a segfault, pthreads only, not a problem with fth
        sleep(1);
        sdf_msg_free_buff(recv_msg);

        msg_sys_release++; /* global sync for SYS mangmt thread init */
        if (j > SHORTTEST) {
            break;
	}
        j++;
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
		 "\nNode %d Exiting pthread MANGEMENT Tester - num msgs %d\n", myid, j);    
	return (0);
}
