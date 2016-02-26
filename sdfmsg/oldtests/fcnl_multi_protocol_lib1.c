/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fcnl_multi_protocol_lib1.c
 * Author: mac
 *
 * Created on Jul 21, 2008, 12:02 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
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
extern uint32_t myid;

#define DBGP 0
#define TSZE 64
#define NANSEC 1.00e-9f
#define FASTPATH_TEST 0
#define ALLVALS 100
extern struct test_info * info;

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

/*
 * These fthreads will be used to simulate the SDF protocol sending and
 * receiving messages to the sdf messaging engine and across nodes
 *
 * @brief: 1(sender) -> n(recver) multiprotocol
 * Sender will send each protocl message to the receivers, this test case
 * is similar to fcnl_multiptl_sequential_lib4.c, except each sender just sends
 * one message.
 */

static struct sdf_queue_pair *q_pair[SDF_PROTOCOL_COUNT];

static int mysync = 0;
static int cluster_node[MAX_NUM_SCH_NODES];
static struct startsync crp;
static startsync_t *ssync = &crp;

static int msgCount = SDF_PROTOCOL_COUNT;

void
fthThreadMultiSender(uint64_t arg) {
    int i = 0, l = 0;
    vnode_t node;
    struct sdf_msg *send_msg = NULL;
    
    msg_type_t type = REQ_FLUSH;

    printf("node %d, fth thread sender starting  %s: number of msgs to send = %d\n", myid, __func__, msgCount);
    fflush(stdout);

    int localpn, actmask;
    uint32_t numprocs;
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: numprocs %d active_procs mask 0x%x active_mask 0x%x\n", 
                  localrank, numprocs, localpn, actmask);
    if (numprocs == 1) {
        node = 0;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
		     "\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
    }
    else {
        node = local_get_pnode(localrank, localpn, numprocs);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
	             "\nNode %d: %s my pnode is  %d\n", 
                     localrank, __func__, node);
	fflush(stdout);
	for (i = 0; i < numprocs; i++) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: %s cluster_node[%d] = %d\n", 
                         localrank, __func__, i, cluster_node[i]);
            fflush(stdout);
        }
    }
    for(i = 0; i < SDF_PROTOCOL_COUNT; i ++) {

         if(i == SDF_SYSTEM || i == SDF_DEBUG || i == GOODBYE) {
             q_pair[i] = NULL;
             continue;
         }
         q_pair[i] = local_create_myqpairs(i, myid, node);
         info->queue_pair_info->queue_add[0] = q_pair[i]->q_in;
         info->queue_pair_info->queue_add[1] = q_pair[i]->q_out;
         info->queue_pair_info->queue_pair_type = i;
         if(q_pair[i] == NULL) {
             fprintf(stderr, "%s: sdf_create_queue_pair %d failed\n", __func__, i);
             plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: EXITING completed sending %d messages - mysync %d\n", 
                     myid, l, mysync);
             fthKill(1);     
             return;
         }
    
         plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: created queue pair %p sn %d dn %d ss %d ds %d maxcnt %d\n",
                 myid, q_pair[i], myid, node, i, i, msgCount);
    }

    FTH_SPIN_LOCK(&ssync->spin);
    mysync = 1;
    FTH_SPIN_UNLOCK(&ssync->spin);

    sdf_msg_startmsg(myid, 0, NULL);//you can move this method to the main method.

    for (l = 0; l < msgCount; ++l) {
        int ret;
        if(l == SDF_SYSTEM || l == SDF_DEBUG || l == GOODBYE)//do not send SDF_SYSTEM message and SDF_DEBUG message
            continue;
        send_msg = (struct sdf_msg *) sdf_msg_alloc(TSZE); 
        if (send_msg == NULL) {
             plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                          "sdf_msg_alloc(TSZE) failed\n");
        }
        
        local_setmsg_payload(send_msg, TSZE, myid, l);
        type = REQ_FLUSH;        
        ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, l, myid, l, type, NULL, NULL);
        
	if (ret != 0 )
            process_ret(ret, l, type, myid);

        fthYield(1);
    }
    while (mysync != 17) {// senders + receivers = 17   
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

void fthThreadMultiRecver(uint64_t arg) {
    int i = 0, ret, ct = 0;
    uint64_t aresp = 0;
    struct sdf_msg *recv_msg = NULL;
    vnode_t node;

    printf("node %d, fth thread receiver %li starting %s\n", myid, arg, __func__);
    fflush(stdout);

    int localpn, actmask;
    uint32_t numprocs;
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    if (localrank == localpn) {
        node = localpn;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
		     "\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
    }
    else {
        node = local_get_pnode(localrank, localpn, numprocs);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
	             "\nNode %d: my pnode is  %d\n", 
                     localrank, node);
	fflush(stdout);
    }

    while (!mysync) {
         fthYield(1);
    }

    for (;;) {
        
        if(arg == SDF_SYSTEM || arg == SDF_DEBUG || arg == GOODBYE)
            break;    
        else
            recv_msg = sdf_msg_receive(q_pair[arg]->q_out, 0, B_TRUE);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Waiting for messages q_pair %p loop %d\n",
                     myid, q_pair[arg], ct);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d"
                     " akrpmbx %p\n", myid, 
                     recv_msg, recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode,
	             recv_msg->msg_dest_service, recv_msg->msg_type,
                     recv_msg->akrpmbx);

#if 0
        printf("node %d, receiver %d recv message from sender\n", myid, arg);
        local_printmsg_payload(recv_msg, TSZE, myid);
#endif

        ret = sdf_msg_free_buff(recv_msg);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Send Buff Freed aresp %ld loop %d\n", 
                     myid, aresp, ct);
        ct++;        
        if(ct == 1) break;
    }
    printf("@@node %d, receiver %li, receive message finished, receive %d times\n", myid, arg, ct); 
    FTH_SPIN_LOCK(&ssync->spin);
    mysync++;
    FTH_SPIN_UNLOCK(&ssync->spin);
    fthYield(1);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: WORKER FTH exiting - loop %d mysync %d\n", myid, i, mysync);    
}

void *
MultiProtocolPthreadRoutine(void *arg)
{
    int index;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH threads firing up\n", myid);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH scheduler has initialized -- Now Spawning fth threads\n", myid);

    for(index = 0; index < SDF_PROTOCOL_COUNT; index ++)
        fthResume(fthSpawn(&fthThreadMultiRecver, 40960), index);
    fthResume(fthSpawn(&fthThreadMultiSender, 40960), msgCount);
    info->pthread_info = 1;
    info->fth_info = SDF_PROTOCOL_COUNT+1;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d Finished Create and Spawned -- Now starting sched\n", myid);

    fthSchedulerPthread(0);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nFTH scheduler halted\n");
    return (0);

}

