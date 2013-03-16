/*
 * File:   fcnl_multiptl_sequential_lib2.c
 * Author: mac
 *
 * Created on Jul 27, 2008, 1:57 PM
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

extern struct test_info * info;
extern uint32_t myid;

#define DBGP 0
#define TSZE 2
#define NANSEC 1.00e-9f
#define FASTPATH_TEST 0
#define ALLVALS 100

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

/*
 * These fthreads will be used to simulate the SDF protocol sending and
 * receiving messages to the sdf messaging engine and across nodes
 *
 * @brief: n(sender) -> n(recver) "multiprotocol"
 * The node #0 just create senders and node #1 just create receivers
 * let node #0's senders send message to node #1's receivers. 
 */

static struct sdf_queue_pair *q_pair[SDF_PROTOCOL_COUNT];

#define FTHS 14
static int cluster_node[MAX_NUM_SCH_NODES];
static struct startsync crp;
static startsync_t *ssync = &crp;
static int mysync[SDF_PROTOCOL_COUNT] = {0};
static int msgCount = 18, fthCount = 0;

void
fthThreadMultiPtlSglNodeSender(uint64_t arg) {
    int l = 0;
    vnode_t node;
    struct sdf_msg *send_msg = NULL;
    
    msg_type_t type = REQ_FLUSH;

    sdf_fth_mbx_t fthmbx;
    fthMbox_t ackmbox;
    fthmbx.actlvl = SACK_ONLY_FTH;
    fthMboxInit(&ackmbox);
    fthmbx.abox = &ackmbox;
    
    printf("node %d, fth thread sender starting  %s: number of msgs to send = %d\n", myid, __func__, msgCount);
    fflush(stdout);

    int localpn, actmask;
    uint32_t numprocs;
    uint64_t ptl = arg;
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: numprocs %d active_procs mask 0x%x active_mask 0x%x\n", 
                 localrank, numprocs, localpn, actmask);
    if (numprocs == 1) {
        node = 0;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
		     "\nNode %d: FASTPATH_TEST node %d myid %d\n", 
                     myid, node, myid);
    }
    else {
        int i;
        node = local_get_pnode(localrank, localpn, numprocs);
	printf("Node %d: %s my pnode is  %d\n", localrank, __func__, node);
	fflush(stdout);
	for (i = 0; i < numprocs; i++) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: %s cluster_node[%d] = %d\n", 
                         localrank, __func__, i, cluster_node[i]);
            fflush(stdout);
        }
    }
   
    printf("In sender\n");
    if(q_pair[ptl] == NULL) {
        fprintf(stderr, "%s: sdf_create_queue_pair %li failed\n", __func__, ptl);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: EXITING completed sending %d messages\n", 
                     myid, l);
        return;
    }
    
     // right now we are not using shmem for these buffers

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: created queue pair %p sn %d dn %d ss %li ds %li maxcnt %d\n",
                 myid, q_pair[ptl], myid, myid == 0 ? 1 : 0,
                 ptl, ptl, msgCount);
    
    FTH_SPIN_LOCK(&ssync->spin);
    mysync[ptl]++;
    FTH_SPIN_UNLOCK(&ssync->spin);
    
    for (l = 0; l < msgCount; ++ l) {
        printf("Send\n");
        int ret;

        send_msg = (struct sdf_msg *) sdf_msg_alloc(TSZE); 
        if (send_msg == NULL) {
             plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                          "sdf_msg_alloc(TSZE) failed\n");
        }
        
        local_setmsg_payload(send_msg, TSZE, myid, l);
        type = REQ_FLUSH;
        
        ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, ptl, myid, ptl, type, &fthmbx, NULL);
        fthMboxWait(&ackmbox);   
#if 0 
        if(myid == 0)
            printf("node %d, sender #%d sends %d times, sdf_msg_send return %d, message contents %c-%d\n", myid, ptl, l + 1, ret, l + 65, ptl);
        else
            printf("node %d, sender #%d sends %d times, sdf_msg_send return %d, message contents %c-%d\n", myid, ptl, l + 1, ret, l + 97, ptl);
#endif  
	if (ret != 0 )
            process_ret(ret, ptl, type, myid);

        fthYield(1);
    }

    FTH_SPIN_LOCK(&ssync->spin);
    fthCount ++;
    FTH_SPIN_UNLOCK(&ssync->spin);

    printf("@@node %d, sender #%li sends %li protocol message finished, send %d times\n", myid, ptl, ptl, l);
    while (fthCount != FTHS) {
        fthYield(100);
    }

    printf("node %d, sender %li kill the scheduler.\n", myid, ptl); 

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: EXITING completed sending %d messages\n", 
                 myid, l);
    fthKill(1);
}

void fthThreadMultiPtlSglNodeRecver(uint64_t arg) {
    int ret, ct = 0;
    uint64_t aresp = 0, ptl;
    struct sdf_msg *recv_msg = NULL;
    vnode_t node;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nnode %d, fth thread receiver %li starting %s\n", 
                 myid, arg, __func__);
    fflush(stdout);

    int localpn, actmask;
    uint32_t numprocs;
    ptl = arg;
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

 	//q_pair[ptl] = local_create_myqpairs(ptl, myid, node);	
    
    sdf_msg_startmsg(myid, 0, NULL);
    while(!mysync[ptl]) {
        fthYield(1);
        printf("Yield\n");
    }
        

    for (;;) {
        
        printf("Receive\n");
        recv_msg = sdf_msg_receive(q_pair[ptl]->q_out, 0, B_TRUE);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Waiting for messages q_pair %p loop %d\n",
                     myid, q_pair[ptl], ct);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d"
                     " akrpmbx %p\n", myid, 
                     recv_msg, recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode,
	             recv_msg->msg_dest_service, recv_msg->msg_type,
                     recv_msg->akrpmbx);

#if 0
    if(recv_msg) {
        uint32_t d = recv_msg->msg_dest_service;
        printf("node %d, receiver #%d recvs protocol#%d message from sender\n", myid, ptl, d);
        local_printmsg_payload(recv_msg, TSZE, myid);
    }
    else {
        printf("!!node %d, receiver #%d recvs protocol#%d meessage from sender failed\n", myid, ptl, recv_msg->msg_dest_service);
    }    
#endif

        ret = sdf_msg_free_buff(recv_msg);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Send Buff Freed aresp %ld loop %d\n", 
                     myid, aresp, ct);
        ct++;
        
        if(ct == msgCount) break;

    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\n@@node %d, receiver #%li, receive message finished, receive %d times\n", 
                 myid, ptl, ct);
    
    FTH_SPIN_LOCK(&ssync->spin);
    fthCount ++;
    FTH_SPIN_UNLOCK(&ssync->spin);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nnode: %d, protocol: %li, fthCount:%d\n", 
                 myid, ptl, fthCount);
    
    while (fthCount != FTHS) {
         fthYield(100);
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: WORKER FTH exiting - loop %d\n", myid, ct);
    fthKill(1);
}

void *
MultiPtlSglNodeSequentialPthreadRoutine(void *arg) {

    int index;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH threads firing up\n", 
                 myid);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH scheduler has initialized -- Now Spawning fth threads\n", 
                 myid);

    uint64_t *id = arg;
    int localpn, actmask;
    uint32_t numprocs;
    vnode_t node;
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    if (localrank == localpn) {
        node = localpn;
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: FASTPATH_TEST node %d myid %d\n", 
                     myid, node, myid);
    }   
    else {
        node = local_get_pnode(localrank, localpn, numprocs);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
	             "\nNode %d: my pnode is  %d\n", 
                     localrank, node);
	fflush(stdout);
    }
    printf("Before Qpair\n");
    for (index = 0; index < SDF_PROTOCOL_COUNT; index ++) {
	if (index == SDF_DEBUG || index == SDF_SYSTEM)
            continue;
	q_pair[index] = local_create_myqpairs(index, myid, node);
    info->queue_pair_info->queue_add[0] = q_pair[index]->q_in;
    info->queue_pair_info->queue_add[1] = q_pair[index]->q_out;
    info->queue_pair_info->queue_pair_type = index;
    }	
    //sdf_msg_startmsg(myid, 1, NULL); 

    if(*id == 0)
    for(index = 0; index < SDF_PROTOCOL_COUNT; index ++) {
        if(index == SDF_DEBUG || index == SDF_SYSTEM)
            continue;
        XResume(XSpawn(&fthThreadMultiPtlSglNodeSender, 40960), index);
        printf("Sender\n");
    }
  
    if(*id == 1)
    for(index = 0; index < SDF_PROTOCOL_COUNT; index ++) {
        if(index == SDF_DEBUG || index == SDF_SYSTEM)
            continue;
        XResume(XSpawn(&fthThreadMultiPtlSglNodeRecver, 40960), index);
        printf("Recver\n");
    }
    info->pthread_info = 1;
    info->fth_info = SDF_PROTOCOL_COUNT*2;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d Finished Create and Spawned -- Now starting sched\n", 
                 myid);

    fthSchedulerPthread(0);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nFTH scheduler halted\n");
    return (0);

}

