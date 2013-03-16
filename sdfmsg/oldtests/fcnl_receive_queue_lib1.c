/*
 * File:   fcnl_receive_queue_lib1.c
 * Author: Norman Xu
 *
 * Created on June 23, 2008, 7:45AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcnl_receive_queue_lib1.c 308 2008-06-23 22:34:58Z normanxu $
 */

/* pthread test example for sending messages */

#ifdef MPI_BUILD
#include <mpi.h>
#endif

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <CUnit/CUnit.h>
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
#define SHORTTEST 30
#define UNEXPT_TEST 0
#define FASTPATH_TEST 0

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

static service_t my_protocol = SDF_CONSISTENCY;
static int cluster_node[MAX_NUM_SCH_NODES];

/*
 * We send msgs until the receive queue is full
 */

void * ReceiveQueuePthreadRoutine(void *arg) {
    int myid = *(int *) arg;
    struct sdf_queue_pair *q_pair_CONSISTENCY = NULL;
    struct sdf_msg *send_msg = NULL, *recv_msg = NULL;
    vnode_t node;
    serviceid_t protocol = SDF_CONSISTENCY;
    msg_type_t type;
    int i, j, ret;
    int debug = 0;
    sdf_fth_mbx_t fthmbx;
    /* FIXME we probably need to expand the fthmbx structure to include pthread items */
    fthmbx.actlvl = SACK_NONE_FTH;

    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Testing pthread MESSAGING SDF_CONSISTENCY Communication\n",
            myid);

    /*
     * Create a queue pair from this thread servicing my_protocol to the thread
     * on another node
     */
    int localpn, actmask;
    uint32_t numprocs;
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    if (localrank == localpn) {
        node = localpn;
    }
    else {
        node = local_get_pnode(localrank, localpn, numprocs);
        printf("Node %d: my pnode is  %d\n", localrank, node);
        fflush(stdout);
    }

    while (!q_pair_CONSISTENCY) {
        usleep(10000);
        printf("norman: before create qpairs\n");
        q_pair_CONSISTENCY = local_create_myqpairs_with_pthread(my_protocol, myid, node);
        info->queue_pair_info->queue_add[0] = q_pair_CONSISTENCY->q_in;
        info->queue_pair_info->queue_add[1] = q_pair_CONSISTENCY->q_out;
        info->queue_pair_info->queue_pair_type = SDF_CONSISTENCY;
        printf("norman: after create qpairs\n");
        if (!q_pair_CONSISTENCY) {
            fprintf(stderr,
                    "\nNode %d: Cannot find qpair %p sn %d dn %d ss %d ds %d\n",
                    myid, q_pair_CONSISTENCY, myid, node,
                    my_protocol, SDF_CONSISTENCY);
        } else {
            plat_log_msg(
                    PLAT_LOG_ID_INITIAL,
                    LOG_CAT,
                    PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: got SYS queue pair %p sn %d dn %d ss %d ds %d\n",
                    myid, q_pair_CONSISTENCY, myid, node,
                    my_protocol, SDF_CONSISTENCY);
        }
    }
    sdf_msg_startmsg(myid, SDF_MSG_RTF_ENBLE_MNGMT, NULL);
    if (debug) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d MNGMT TEST THREAD STOPPING HERE FOR DEBUG\n", myid);
    }
    while (debug);

    j = 0;
    int sendcount = 0, receivecount = 0;
    for (;;) {

        if (j < SHORTTEST) {
            send_msg = (struct sdf_msg *) sdf_msg_alloc(TSZE);
            if (send_msg == NULL) {
                printf("sdf_msg_alloc(%d) failed\n", TSZE);
                return ((void *) 1);
            }

            if (DBGP) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE,
                        "\nNode %d msg %p msg->msg_payload %p\n", myid,
                        send_msg, send_msg->msg_payload);
            }
            /* Fill in body of message with test data */
            for (i = 0; i < TSZE; ++i)
                send_msg->msg_payload[i] = (unsigned char) i;

            protocol = SDF_CONSISTENCY; /* num value of 3 */
            type = REQ_FLUSH; /* num value of 1 */

            plat_log_msg(
                    PLAT_LOG_ID_INITIAL,
                    LOG_CAT,
                    PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: SENDING MGMNT MSG dnode %d, proto %d, type %d loop num %d\n",
                    myid, node, protocol, type, j);
            if (1) {
                ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node,
                        protocol, myid, my_protocol, type, &fthmbx, NULL);
                sendcount++;
                printf("node %d, just send %d times.\n", myid, j);

                if (DBGP) {
                    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                            PLAT_LOG_LEVEL_TRACE,
                            "\nNode %d: sdf_msg_send returned %d\n", myid, ret);
                    plat_log_msg(
                            PLAT_LOG_ID_INITIAL,
                            LOG_CAT,
                            PLAT_LOG_LEVEL_TRACE,
                            "\nNode %d: %s: calling sdf_msg_receive(%p, %d, %d)\n",
                            myid, __func__, q_pair_CONSISTENCY->q_out, 0, B_TRUE);
                }
            }
        }
        if (j >= SHORTTEST) {
            recv_msg = sdf_msg_receive(q_pair_CONSISTENCY->q_out, 0, B_TRUE);
            receivecount++;
            printf("node %d, receivecount = %d\n", myid, receivecount);

#if SHOWBUFF
            unsigned char* m = (unsigned char *) recv_msg;
            for (i = 0; i < 256; i ++) {
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
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE,
                        "\nNode %d: back from sdf_msg_receive with msg %p\n",
                        myid, recv_msg);
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
        }
        sleep(1);
        j++;
        if (j >= (SHORTTEST * 2))
            break;
    }

    if (receivecount == sendcount) {
        printf("############################################\n"
            "############## Test PASS ####################\n"
            "#############################################\n");
    } else {
        printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n"
            "XXXXXXXXXXXXXX Test FAIL XXXXXXXXXXXXXXXXXX\n"
            "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n");
        printf("Send %d Receive %d", sendcount, receivecount);
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d Exiting pthread CONSISTENCY Tester\n", myid);
    return (0);
}
