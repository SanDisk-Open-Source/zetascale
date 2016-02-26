/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fcnl_queue_order_pth_lib2.c
 * Author: Tom Riddle Norman Xu
 *
 * Created on March 20, 2008, 11:45 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: queue_order_test_pthread.c 308 2008-02-20 22:34:58Z tomr $
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
#define SHORTTEST 20
#define UNEXPT_TEST 0
#define FASTPATH_TEST 0

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

static service_t my_protocol = SDF_MANAGEMENT;
static int cluster_node[MAX_NUM_SCH_NODES];

struct sdf_queue_pair *q_pair_MANAGEMENT;
/*
 * This pthread will be used to simulate the SDF_MANAGEMENT sending and
 * receiving messages to the sdf messaging engine and test its order
 */

void * OrderingSender1MultiPthreadRoutine(void *arg) {
    struct pth_init_order_data* data = (struct pth_init_order_data *) arg;
    struct sdf_msg *send_msg = NULL;
    int myid = data->myid;
    vnode_t node;
    serviceid_t protocol = SDF_MANAGEMENT;
    msg_type_t type;
    int i, j, ret;
    int debug = 0;
    sdf_fth_mbx_t fthmbx;
    int sendcount = 0;
    int msg_sys_release = 0;
    fthmbx.actlvl = SACK_NONE_FTH;
    fthmbx.abox = NULL;
    fthmbx.rbox = NULL;

    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Testing pthread MESSAGING SDF_MANGEMENT Communication\n",
            myid);

    /*
     * Create a queue pair from this thread servicing my_protocol to the thread
     * on another node
     */
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
    printf("norman: before while\n");
    while (!q_pair_MANAGEMENT) {
        usleep(10000);
        printf("norman: before create qpairs\n");
        q_pair_MANAGEMENT = local_create_myqpairs_with_pthread(my_protocol,
                myid, node);
        info->queue_pair_info->queue_add[0] = q_pair_MANAGEMENT->q_in;
        info->queue_pair_info->queue_add[1] = q_pair_MANAGEMENT->q_out;
        info->queue_pair_info->queue_pair_type = my_protocol;
        printf("norman: after create qpairs\n");
        if (!q_pair_MANAGEMENT) {
            plat_log_msg(
                    PLAT_LOG_ID_INITIAL,
                    LOG_CAT,
                    PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: Cannot find qpair %p sn %d dn %d ss %d ds %d\n",
                    myid, q_pair_MANAGEMENT, myid, node,
                    my_protocol, SDF_MANAGEMENT);
        } else {
            plat_log_msg(
                    PLAT_LOG_ID_INITIAL,
                    LOG_CAT,
                    PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: got SYS queue pair %p sn %d dn %d ss %d ds %d\n",
                    myid, q_pair_MANAGEMENT, myid, node,
                    my_protocol, SDF_MANAGEMENT);
        }
    }

    if (debug)
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d MNGMT TEST THREAD STOPPING HERE FOR DEBUG\n", myid);
    while (debug);
    j = 0;
    sdf_msg_startmsg(myid, 0, NULL);

    pthread_mutex_lock(&data->pth_mutex);
    pthread_cond_signal(&data->pth_cond);
    pthread_mutex_unlock(&data->pth_mutex);

    int msgsize = TSZE;
    for (j = 0; j < SHORTTEST; j++) {

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
        uint64_t command = 0x01100110;
        for (i = 0; i < msgsize; ++i)
            send_msg->msg_payload[i] = (unsigned char) 0xFF;

        pthread_mutex_lock(&data->count_mutex);
        sendcount++;
        memcpy((void *) &send_msg->msg_payload[0], &command, sizeof(uint64_t));
        memcpy((void *) &send_msg->msg_payload[8], &sendcount, sizeof(uint64_t));
        pthread_mutex_unlock(&data->count_mutex);

        protocol = SDF_MANAGEMENT; /* num value of 3 */
        type = MGMT_REQUEST; /* num value of 1 */

        plat_log_msg(
                PLAT_LOG_ID_INITIAL,
                LOG_CAT,
                PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: SENDING MGMNT MSG dnode %d, proto %d, type %d loop num %d\n",
                myid, node, protocol, type, j);
        if (1) {
            ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node,
                    protocol, myid, my_protocol, type, &fthmbx, NULL);

            printf("node %d, just send %d\n", myid, j);

            if (DBGP) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE,
                        "\nNode %d: sdf_msg_send returned %d\n", myid, ret);
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE,
                        "\nNode %d: %s: calling sdf_msg_receive(%p, %d, %d)\n",
                        myid, __func__, q_pair_MANAGEMENT->q_out, 0, B_TRUE);
            }
        }
        msg_sys_release++; /* global sync for SYS mangmt thread init */
    }
    printf("======sender 1 send finished, go to sleep.\n");
    sleep(1);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
		 "\nNode %d Exiting pthread tester - num of messages %d\n", myid, j);
    return (0);
}

/*
 * This pthread will be used to simulate the SDF_MANAGEMENT sending and
 * receiving messages to the sdf messaging engine and test its order
 */

void * OrderingSender2MultiPthreadRoutine(void *arg) {
    struct pth_init_order_data* data = (struct pth_init_order_data *) arg;
    int myid = data->myid;
    struct sdf_msg *send_msg = NULL;
    vnode_t node;
    serviceid_t protocol = SDF_MANAGEMENT;
    msg_type_t type;
    int i, j, ret;
    sdf_fth_mbx_t fthmbx;
    int sendcount = 0;
    int msg_sys_release = 0;
    fthmbx.actlvl = SACK_NONE_FTH;

    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Testing pthread MESSAGING SDF_MANGEMENT Communication\n",
            myid);

    /*
     * Create a queue pair from this thread servicing my_protocol to the thread
     * on another node
     */
    pthread_mutex_lock(&data->pth_mutex);
    pthread_cond_wait(&data->pth_cond, &data->pth_mutex);
    pthread_mutex_unlock(&data->pth_mutex);
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

    j = 0;
    int msgsize = TSZE;
    for (j = 0; j < SHORTTEST; j++) {

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
        uint64_t command = 0x01100110;
        for (i = 0; i < msgsize; ++i)
            send_msg->msg_payload[i] = (unsigned char) 0xFF;

        pthread_mutex_lock(&data->count_mutex);
        sendcount++;

        memcpy((void *) &send_msg->msg_payload[0], &command, sizeof(uint64_t));
        memcpy((void *) &send_msg->msg_payload[8], &sendcount, sizeof(uint64_t));
        pthread_mutex_unlock(&data->count_mutex);

        protocol = SDF_MANAGEMENT;
        type = MGMT_REQUEST; 

        plat_log_msg(
                PLAT_LOG_ID_INITIAL,
                LOG_CAT,
                PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: SENDING MGMNT MSG dnode %d, proto %d, type %d loop num %d\n",
                myid, node, protocol, type, j);
        if (1) {
            ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node,
                    protocol, myid, my_protocol, type, &fthmbx, NULL);

            printf("node %d, just send %d\n", myid, j);

            if (DBGP) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE,
                        "\nNode %d: sdf_msg_send returned %d\n", myid, ret);
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE,
                        "\nNode %d: %s: calling sdf_msg_receive(%p, %d, %d)\n",
                        myid, __func__, q_pair_MANAGEMENT->q_out, 0, B_TRUE);
            }
        }
        msg_sys_release++; /* global sync for SYS mangmt thread init */
    }
    printf("======sender 2 send finished, go to sleep.\n");

    sleep(1);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
		 "\nNode %d Exiting pthread queue order pth2 tester - num of messages %d\n", myid, j);
    return (0);
}

void * OrderingReceiverMultiPthreadRoutine(void *arg) {
    int myid = *(int *) arg;
    struct sdf_queue_pair *q_pair_MANAGEMENT = NULL;
    struct sdf_msg *recv_msg = NULL;
    vnode_t node;
    int j;
    int debug = 0;
    sdf_fth_mbx_t fthmbx;
    int startseq = 0, endseq = 0;
    int misordernum = 0;

    int msg_sys_release = 0;
    fthmbx.actlvl = SACK_NONE_FTH;

    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Testing pthread MESSAGING SDF_MANGEMENT Communication\n",
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
    printf("norman: before while\n");
    while (!q_pair_MANAGEMENT) {
        usleep(10000);
        q_pair_MANAGEMENT = local_create_myqpairs_with_pthread(my_protocol, myid, node);
        info->queue_pair_info->queue_add[0] = q_pair_MANAGEMENT->q_in;
        info->queue_pair_info->queue_add[1] = q_pair_MANAGEMENT->q_out;
        info->queue_pair_info->queue_pair_type = my_protocol;
        if (!q_pair_MANAGEMENT) {
            plat_log_msg(
                    PLAT_LOG_ID_INITIAL,
                    LOG_CAT,
                    PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: Cannot find qpair %p sn %d dn %d ss %d ds %d\n",
                    myid, q_pair_MANAGEMENT, myid, myid == 0 ? 1 : 0,
                    my_protocol, SDF_MANAGEMENT);
        } else {
            plat_log_msg(
                    PLAT_LOG_ID_INITIAL,
                    LOG_CAT,
                    PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: got SYS queue pair %p sn %d dn %d ss %d ds %d\n",
                    myid, q_pair_MANAGEMENT, myid, myid == 0 ? 1 : 0,
                    my_protocol, SDF_MANAGEMENT);
        }
    }

    if (debug)
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d MNGMT TEST THREAD STOPPING HERE FOR DEBUG\n", myid);
    while (debug);
    j = 0;
    sdf_msg_startmsg(myid, 0, NULL);

    for (;;) {

        recv_msg = sdf_msg_receive(q_pair_MANAGEMENT->q_out, 0, B_TRUE);

        uint64_t command = *(uint64_t *) &recv_msg->msg_payload[0];
        if (command != 0x01100110) {
            printf("Wrong msg format 0x%lx\n", command);
            sdf_msg_free_buff(recv_msg);
            continue;
        } else {
            printf("******************node %d, get a message********************\n", myid);
        }
        endseq = *(uint64_t *) &recv_msg->msg_payload[8];

        if (startseq > endseq) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nlost order with %d %d", startseq, endseq);
            misordernum++;
            printf("Miss Ordern");
            sleep(2);
        }
        startseq = endseq;

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
        msg_sys_release++;
        j++;
        if (j >= SHORTTEST * 2) {
            printf("node %d, receive message finished\n", myid);
            break;
        }
    }

//    printf("%d times mis-ordering in %d times sending\n");
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d Exiting pthread MANGEMENT Tester\n", myid);
    return (0);
}

