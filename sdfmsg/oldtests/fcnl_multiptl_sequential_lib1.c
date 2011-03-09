/*
 * File:   fcnl_multiptl_sequential_lib1.c
 * Author: mac
 *
 * Created on Jul 23, 2008, 1:57 PM
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
#define TSZE 64
#define NANSEC 1.00e-9f
#define FASTPATH_TEST 0
#define ALLVALS 100

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

/*
 * These fthreads will be used to simulate the SDF protocol sending and
 * receiving messages to the sdf messaging engine and across nodes
 *
 * @brief: n(sender) -> n(receiver) "multiprotocol"
 * Let each node create senders and receivers, let each sender sends message the peer node's receivers
 */

static struct sdf_queue_pair *q_pair[SDF_PROTOCOL_COUNT];

//number of current node's receiver fth threads
#define FTHRECVERS 14
// play around with the spin locks here for test
static int mysync[SDF_PROTOCOL_COUNT] = {0};

static int cluster_node[MAX_NUM_SCH_NODES];
static struct startsync crp;
static startsync_t *ssync = &crp;

//the sender send two types protocol message
static int msgCount = 8, fthCount = 0;
/* 
 * This fth thread simulates the action node, and don't need to wait ack or resp.
 * the msg types are arbitrary for now, here it is REQ_FLUSH
 */

void
fthThreadMultiPtlSeqSender(uint64_t arg) {
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
		     "\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
    }
    else {
        int i;
        node = local_get_pnode(localrank, localpn, numprocs);
	printf("Node %d: %s my pnode is  %d\n", localrank, __func__, node);
	fflush(stdout);
	for (i = 0; i < numprocs; i++) {
            printf("Node %d: %s cluster_node[%d] = %d\n", localrank, __func__, i, cluster_node[i]);
            fflush(stdout);
        }
    }

    // you only init this once but share the q_pairs among the other threads here 
    q_pair[ptl] = local_create_myqpairs(ptl, myid, node);
    info->queue_pair_info->queue_add[0] = q_pair[ptl]->q_in;
    info->queue_pair_info->queue_add[1] = q_pair[ptl]->q_out;
    info->queue_pair_info->queue_pair_type = ptl;
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
                 myid, q_pair[ptl], myid, node, ptl, ptl, msgCount);

    FTH_SPIN_LOCK(&ssync->spin);
    mysync[ptl] = 1;
    FTH_SPIN_UNLOCK(&ssync->spin);

    //sdf_msg_startmsg(myid, 0, NULL);

    for (l = 0; l < msgCount; ++l) {
        int ret;

        // create the buffer on every loop to check the buffer release func
	
        send_msg = (struct sdf_msg *) sdf_msg_alloc(TSZE); 
        if (send_msg == NULL) {
             // FIXME should default to an error
             plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                          "sdf_msg_alloc(TSZE) failed\n");
             // return ((void *)1);
        }
        
        local_setmsg_payload(send_msg, TSZE, myid, l);             
	/* 
	 * Send messages with different types
	 */ 

        type = REQ_FLUSH;
        
        ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, ptl, myid, ptl, type, &fthmbx, NULL);
        fthMboxWait(&ackmbox);   
#if 0 
        if(myid == 0)
            printf("node %d ,sender #%d sends %d times, sdf_msg_send return %d, message contents %c-%d\n", myid, ptl, l + 1, ret, l + 65, ptl);
        else
            printf("node %d, sender #%d sends %d times, sdf_msg_send return %d, message contents %c-%d\n", myid, ptl, l + 1, ret, l + 97, ptl);
#endif  
        

	if (ret != 0 )
            //failed
            process_ret(ret, ptl, type, myid);

        fthYield(1);
    }

    printf("@@node %d, sender #%li sends %li protocol message finished, send %d times\n", myid, ptl, ptl, l);

    FTH_SPIN_LOCK(&ssync->spin);
    fthCount ++;
    FTH_SPIN_UNLOCK(&ssync->spin);

    while (fthCount != 2 * FTHRECVERS) {
        fthYield(100);
    }
    
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: EXITING completed sending %d messages - mysync %d\n", 
                 myid, l, mysync[ptl]);
    printf("node %d, sender %li kill the scheduler.\n", myid, ptl);    
    fthKill(1);
}

/* 
 * This FTH thread will act as a protocol worker thread. 
 * It will wait for CONSISTENCY Messages 
 * on the queue, processes them and returns appropriate RESPONSE messages 
 */

void fthThreadMultiPtlSeqRecver(uint64_t arg) {
    int ret, ct = 0;
    uint64_t aresp = 0, ptl;
    struct sdf_msg *recv_msg = NULL;
    vnode_t node;
    printf("node %d, fth thread receiver %li starting %s\n", myid, arg, __func__);
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
	printf("Node %d: my pnode is  %d\n", localrank, node);
	fflush(stdout);
    }
    // need to yield till all queues have been created
 		
    while (!mysync[ptl]) {
         fthYield(1);
    }

    for (;;) {
        
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


        // release the receive buffer back to the sdf messaging thread
        ret = sdf_msg_free_buff(recv_msg);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Send Buff Freed aresp %ld loop %d\n", 
                     myid, aresp, ct);
        ct++;
        
        if(ct == msgCount) break;

        /* 
         * Simple exit mechanism, worker threads will just quit when predefined msgcnt 
         * has been reached in the sender thread
         */
    }
    printf("@@node %d, receiver #%li, receive message finished, receive %d times\n", myid, ptl, ct);
     
    FTH_SPIN_LOCK(&ssync->spin);
    fthCount ++;
    FTH_SPIN_UNLOCK(&ssync->spin);
    
    printf("node: %d, protocol: %li, mysync[ptl]: %d, fthCount: %d\n", myid, ptl, mysync[ptl], fthCount);
    fthYield(1);
    
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: WORKER FTH exiting - loop %d mysync %d\n", myid, ct, mysync[ptl]);
    
}

void *
MultiPtlSequentialPthreadRoutine(void *arg) {

    int index;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH threads firing up\n", myid);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH scheduler has initialized -- Now Spawning fth threads\n", myid);

    for(index = 0; index < SDF_PROTOCOL_COUNT; index ++) {
	mysync[index] = 0;
        if(index == SDF_DEBUG || index == SDF_SYSTEM)
            continue;
        XResume(XSpawn(&fthThreadMultiPtlSeqRecver, 40960), index);
        XResume(XSpawn(&fthThreadMultiPtlSeqSender, 40960), index);
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d Finished Create and Spawned -- Now starting sched\n", myid);
    info->pthread_info = 1;
    info->fth_info = SDF_PROTOCOL_COUNT*2;
    fthSchedulerPthread(0);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nFTH scheduler halted\n");
    return (0);

}

