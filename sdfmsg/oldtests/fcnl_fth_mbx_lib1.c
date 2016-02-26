/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fcnl_fth_mbx_lib1.c
 * Author: Norman Xu
 *
 * Created on Oct 13, 2008, 7:45AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcnl_fth_mbx_lib1.c 308 2008-06-23 22:34:58Z normanxu $
 */

#ifdef MPI_BUILD
#include <mpi.h>
#endif

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>

#include "platform/time.h"
#include "platform/logging.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_types.h"

#include "fcnl_test.h"
#include "Utilfuncs.h"
#include "log.h"
extern struct test_info * info;
extern uint32_t myid;

#define DBGP 0
#define TSZE 64
#define NANSEC 1.00e-9f
#define UNEXPT_TEST 0
#define FASTPATH_TEST 0
#define ALLVALS 100

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

static struct sdf_queue_pair *q_pair_CONSISTENCY;
static struct sdf_queue_pair *q_pair_RESPONSES;
static fthMbox_t ackmbox1;

// play around with the spin locks here for test
static int mysync = 0;
static int cluster_node[MAX_NUM_SCH_NODES];
static struct startsync crp;
static startsync_t *ssync = &crp;

static int msgCount = SDF_PROTOCOL_COUNT;

/*
 * This fth thread simulates the action node, it sends a CONSISTENCY msg and
 * sleeps on a mailbox(rbox) waiting for a response, the msg types are arbitrary for now
 * here it is REQ_FLUSH
 */
void
fthThreadSingleSender(uint64_t arg) {
    int i = 0, l = 0;
    struct sdf_msg *send_msg = NULL;
    vnode_t node;
    serviceid_t protocol = SDF_CONSISTENCY;
    serviceid_t my_protocol = SDF_CONSISTENCY;
    msg_type_t type = REQ_FLUSH;

    printf("node %d, fth thread sender starting  %s: number of msgs to send = %d\n", myid, __func__, msgCount);

    fflush(stdout);

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
    // you only init this once but share the q_pairs among the other threads here
    q_pair_CONSISTENCY = local_create_myqpairs(SDF_CONSISTENCY, myid, node);
    info->queue_pair_info->queue_add[0] = q_pair_CONSISTENCY->q_in;
    info->queue_pair_info->queue_add[1] = q_pair_CONSISTENCY->q_out;
    info->queue_pair_info->queue_pair_type = SDF_CONSISTENCY;
    if (q_pair_CONSISTENCY == NULL) {
        fprintf(stderr, "%s: sdf_create_queue_pair failed\n", __func__);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: EXITING completed sending %d messages - mysync %d\n",
                     myid, l, mysync);
        fthKill(1);
        return;
    }
    // right now we are not using shmem for these buffers

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: created queue pair %p sn %d dn %d ss %d ds %d maxcnt %d\n",
                 myid, q_pair_CONSISTENCY, myid, myid == 0 ? 1 : 0,
                 SDF_CONSISTENCY, SDF_CONSISTENCY, msgCount);

    FTH_SPIN_LOCK(&ssync->spin);
    mysync = 1;
    FTH_SPIN_UNLOCK(&ssync->spin);

    // let the msg thread do it's thing
    sdf_msg_startmsg(myid, 0, NULL);

    /*
     * main loop will send SDF_CONSISTENCY protocol messages till msgCount is reached
     * this sleeps on both mailboxes ack and resp based on the lvl dictated
     */

    for (l = 0; l < msgCount; ++l) {
        sdf_msg_t *ret;

        // create the buffer on every loop to check the buffer release func
	if (UNEXPT_TEST) {
            send_msg = (struct sdf_msg *) sdf_msg_alloc(8192);
	    printf("Node %d: %s BIG Message Alloc %li\n", myid, __func__,
                   ((char *) send_msg->msg_payload - (char *) send_msg) + 8192);
            fflush(stdout);
	}
	else {
            send_msg = (struct sdf_msg *) sdf_msg_alloc(TSZE);
	}
        if (send_msg == NULL) {
             // FIXME should default to an error
             plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                          "sdf_msg_alloc(TSZE) failed\n");
             // return ((void *)1);
        }
        local_setmsg_payload(send_msg, TSZE, myid, l);
	/*
	 * Send CONSISTENCY messages with different types to track if we define SENDTWO
	 */

        type = REQ_FLUSH;

        ret = sdf_msg_send_receive((struct sdf_msg *)send_msg, TSZE, node, protocol, myid,
                my_protocol, type, SACK_REL_YES);

        plat_assert(ret!=NULL);
#if 1
        printf("node %d, sender recv response message from recriver\n", myid);
        local_printmsg_payload(ret, TSZE, myid);
#endif

        fthYield(1);
    }
    while (mysync != 17) {
        fthYield(100);
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: EXITING completed sending %d messages - mysync %d\n",
                 myid, l, mysync);
    fthKill(1);

}

/*
 * This FTH thread will act as a protocol worker thread.
 * It will wait for CONSISTENCY Messages
 * on the queue, processes them and returns appropriate RESPONSE messages
 */

void fthThreadSingleRecver(uint64_t arg) {
    int i = 0, ret, ct = 0;
    uint64_t aresp;
    struct sdf_msg *send_msg = NULL, *recv_msg = NULL;
    vnode_t node;
    serviceid_t protocol = SDF_RESPONSES;
    serviceid_t my_protocol = SDF_RESPONSES;
    msg_type_t type = RESP_ONE;
    sdf_fth_mbx_t fthmbx;
    fthmbx.actlvl = SACK_ONLY_FTH;
    fthMboxInit(&ackmbox1);
    fthmbx.abox = &ackmbox1;
    fthmbx.rbox = NULL;

    printf("node %d, fth thread starting %s\n", myid, __func__);
    fflush(stdout);

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
    if(arg == 0) {
        q_pair_RESPONSES = local_create_myqpairs(SDF_RESPONSES, myid, node);
        if(q_pair_RESPONSES != NULL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: created queue pair %p sn %d dn %d ss %d ds %d\n",
                     myid, q_pair_RESPONSES, myid, myid == 0 ? 1 : 0,
                     SDF_RESPONSES, SDF_RESPONSES);
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Now yielding waiting for mysync\n", myid);
        }
        else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: FAILED to create queue pair %p sn %d dn %d ss %d ds %d\n",
                     myid, q_pair_RESPONSES, myid, myid == 0 ? 1 : 0,
                     SDF_RESPONSES, SDF_RESPONSES);
	    return;//return but still continue ?

        }
    }

    // need to yield till all queues have been created

    while (!mysync) {
        fthYield(1);
    }

    for (;;) {

        recv_msg = sdf_msg_receive(q_pair_CONSISTENCY->q_out, 0, B_TRUE);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Waiting for messages q_pair_CONSISTENCY %p loop%d\n",
                     myid, q_pair_CONSISTENCY, ct);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d"
                     " akrpmbx %p\n", myid,
                     recv_msg, recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode,
	             recv_msg->msg_dest_service, recv_msg->msg_type,
                     recv_msg->akrpmbx);

#if 1
        printf("node %d, receiver recv message from sender\n", myid);
        local_printmsg_payload(recv_msg, TSZE, myid);
#endif

        //current receiver sends response message to the sender
        send_msg = (struct sdf_msg *) sdf_msg_alloc(recv_msg->msg_len);
        if (send_msg == NULL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "sdf_msg_alloc(recv_msg->msg_len) failed\n");
        }

        memcpy(send_msg->msg_payload, recv_msg->msg_payload, recv_msg->msg_len - sizeof(struct sdf_msg));

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Sending MSG dn %d ds %d sn %d ss %d type %d"
                     " akrpmbx %p send_msg %p\n",
                     myid, node, protocol, myid, my_protocol, type,
                     recv_msg->akrpmbx, send_msg);
        struct sdf_resp_mbx rhkey;
        struct sdf_resp_mbx *ptrkey = &rhkey;

        strncpy(rhkey.mkey, MSG_DFLT_KEY, (MSG_KEYSZE - 1));
        rhkey.mkey[MSG_KEYSZE - 1] = '\0';
        rhkey.akrpmbx_from_req = NULL;
        rhkey.rbox = NULL;
        ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, protocol,
			   myid, my_protocol, type, &fthmbx,
                           sdf_msg_get_response(recv_msg, ptrkey));
	if (ret != 0) {
            process_ret(ret, protocol, type, myid);
        }

        aresp = fthMboxWait(&ackmbox1);//must call this method?
        // release the receive buffer back to the sdf messaging thread
        ret = sdf_msg_free_buff(recv_msg);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Send Buff Freed aresp %ld loop %d\n",
                     myid, aresp, ct);
        ct++;

        if(ct == 1) break;

        /*
         * Simple exit mechanism, worker threads will just quit when predefined msgcnt
         * has been reached in the sender thread
         */
    }
    mysync++;
    printf("node %d, receiver %li, receive message finished, receive times %d\n", myid, arg, ct);

    fthYield(1);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: WORKER FTH exiting - loop %d mysync %d\n", myid, i, mysync);

}

void *
FthMbxPthreadRoutine(void *arg) {

    int index;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH threads firing up\n", myid);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH scheduler has initialized -- Now Spawning fth threads\n", myid);

    for(index = 0; index < SDF_PROTOCOL_COUNT; index ++)
        fthResume(fthSpawn(&fthThreadSingleRecver, 40960), index);
    fthResume(fthSpawn(&fthThreadSingleSender, 40960), SDF_PROTOCOL_COUNT);
    info->pthread_info = 1;
    info->fth_info = SDF_PROTOCOL_COUNT+1;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d Finished Create and Spawned -- Now starting sched\n", myid);

    fthSchedulerPthread(0);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nFTH scheduler halted\n");
    return (0);

}
