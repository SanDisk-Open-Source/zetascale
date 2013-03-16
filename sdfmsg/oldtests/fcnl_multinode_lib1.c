/*
 * File:   fcnl_multinode_lib1.c
 * Author: mac
 *
 * Created on Aug 22, 2008, 10:24 AM
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
 * @brief: x -> 1 (singleside, singleprotocol)
 * node 0 create only one type of protocol receiver, other nodes create only the same type 
 * of sender, they are send message to the node#0, you can control whether need response
 * from receiver. 
 */

static struct sdf_queue_pair *q_pair[SDF_PROTOCOL_COUNT];

static int cluster_node[MAX_NUM_SCH_NODES];

//the sender send one types protocol message
static int msgCount = 10;
/* 
 * This fth thread simulates the action node, and don't need to wait ack or resp.
 * the msg types are arbitrary for now, here it is REQ_FLUSH
 */
#define FLAG 1
//0 ==> only need ack
//1 ==> both ack and resp

void
fthThreadMultiNodeSender(uint64_t arg) {
    int l = 0;
    vnode_t node;
    struct sdf_msg *send_msg = NULL;
    
    msg_type_t type = REQ_FLUSH;

    sdf_fth_mbx_t fthmbx;
    fthMbox_t ackmbox, respmbox;
    fthMboxInit(&ackmbox);
    fthmbx.abox = &ackmbox;

#if FLAG
    fthMboxInit(&respmbox);
    fthmbx.rbox = &respmbox;
    fthmbx.actlvl = SACK_BOTH_FTH;
#else
    fthmbx.rbox = NULL;
    fthmbx.actlvl = SACK_ONLY_FTH;
#endif


    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d, fth thread sender starting  %s: number msgs to send = %d\n", myid, __func__, msgCount);

    int localpn, actmask;
    uint32_t numprocs;
    uint64_t ptl = arg;
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node,  &actmask);
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
	             "\nNode %d: %s my pnode is  %d\n", localrank, __func__, node);
	for (int i = 0; i < numprocs; i++) {
            printf("Node %d: %s cluster_node[%d] = %d\n", localrank, __func__, i, cluster_node[i]);
            fflush(stdout);
        }
    }

    // you only init this once but share the q_pairs among the other threads here 
    q_pair[ptl] = local_create_myqpairs(ptl, myid, node); // 1 ==> 0, 2 ==> 0, 3 ==> 0
    
   
    if(q_pair[ptl] == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: EXITING requested queue %li creation failed\n", myid, ptl);
        return;
    }
    
    for (l = 0; l < msgCount; ++l) {
        int ret;

        send_msg = (struct sdf_msg *) sdf_msg_alloc(TSZE); 
        if (send_msg == NULL) {
             plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL, "sdf_msg_alloc(TSZE) failed\n");
        }
        local_setmsg_mc_payload(send_msg, TSZE, myid, l, msgCount, ptl); 

        type = REQ_FLUSH;

        ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, ptl, myid, ptl, type, &fthmbx, NULL);
        
        /* get the ack when sending success. */
        fthMboxWait(&ackmbox);   
#if FLAG
        printf("node %d, wait and get the response message from pnode.\n", myid);
        /* get the response when receive message success. */
        sdf_msg_t * msg = (sdf_msg_t *)fthMboxWait(&respmbox);
        ret = sdf_msg_free_buff(msg);
#endif
	if (ret != 0 )
            process_ret(ret, ptl, type, myid);

        fthYield(1);
    }
    
    printf("Sender break\n");
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: sender type#%li sends protocol#%li msg finished, send %d times\n", myid, ptl, ptl, l);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d, sender type %li kill the scheduler.\n", myid, ptl);  
    fthKill(1);
}

void fthThreadMultiNodeRecver(uint64_t arg) {
    int i, ret, ct = 0;
    uint64_t aresp = 0, ptl;
    struct sdf_msg *recv_msg = NULL, *send_msg = NULL;
    vnode_t node;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: fth thread receiver %li\n", myid, arg);

    int localpn, actmask;
    uint32_t numprocs;

#if FLAG
    sdf_fth_mbx_t fthmbx;
    fthMbox_t ackmbox1;
    fthmbx.actlvl = SACK_ONLY_FTH;
    fthmbx.abox = &ackmbox1;
    fthmbx.rbox = NULL;
    fthMboxInit(&ackmbox1);
#endif

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
	             "\nNode %d: my pnode is  %d\n", localrank, node);
    }

    for(i = 1; i < numprocs; i ++ ) {
        q_pair[i] = sdf_create_queue_pair(myid, i, ptl, ptl, SDF_WAIT_FTH);
        info->queue_pair_info->queue_add[0] = q_pair[i]->q_in;
        info->queue_pair_info->queue_add[1] = q_pair[i]->q_out;
        info->queue_pair_info->queue_pair_type = ptl;
    }
    printf("************************Number:%d\n",(numprocs-1) * msgCount);
for (;;) {
    for(i = 1; i < numprocs; i ++ ) {    
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Waiting for messages q_pair %p loop %d\n", myid, q_pair[i], ct);
        ct++;
        if(ct > (numprocs-1) * msgCount)
            break;
        recv_msg = sdf_msg_receive(q_pair[i]->q_out, 0, B_TRUE);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d"
                     " akrpmbx %p\n", myid, recv_msg, recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode,
	             recv_msg->msg_dest_service, recv_msg->msg_type, recv_msg->akrpmbx);

#if 1
    if(recv_msg) {
        uint32_t d = recv_msg->msg_dest_service;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d, receiver type# %li recvs protocol# %d type msg from sender %d\n", myid, ptl, d, i);
        local_printmsg_payload(recv_msg, TSZE, myid);
    }
    else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: receiver type#%li recvs protocol#%d meessage from sender failed\n", 
                     myid, ptl, recv_msg->msg_dest_service);
    }
#endif
       
#if FLAG
        send_msg = (struct sdf_msg *) sdf_msg_alloc(recv_msg->msg_len);
        memcpy(send_msg->msg_payload, recv_msg->msg_payload, recv_msg->msg_len - sizeof(sdf_msg_t));
        
        struct sdf_resp_mbx rhkey;
        struct sdf_resp_mbx *ptrkey = &rhkey;

        strncpy(rhkey.mkey, MSG_DFLT_KEY, (MSG_KEYSZE - 1));
        rhkey.mkey[MSG_KEYSZE - 1] = '\0';
        rhkey.akrpmbx_from_req = NULL;
        rhkey.rbox = NULL;
        
        ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, i, ptl, myid, ptl, 
                           RESP_ONE, &fthmbx, sdf_msg_get_response(recv_msg, ptrkey));
        fthMboxWait(&ackmbox1);
#endif

        // release the receive buffer back to the sdf messaging thread
        ret = sdf_msg_free_buff(recv_msg);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Sent msg, Send ack'd, Buff Freed aresp %ld loop %d\n", myid, aresp, ct);
    }

    if(ct > (numprocs-1) * msgCount)
        break;
        

}//end of outside for statement

    printf("@@node %d, receiver type#%li, receive message finished, receive %d times\n", myid, ptl, ct * (numprocs - 1));
    

   plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: receiver type#%li, receive message finished, receive %d times\n", 
                myid, ptl, ct * (numprocs - 1));
   fthYield(100);
   //return;
   printf("Node %d - Before \n", myid);
       fthKill(100);
    printf("After\n");

}

void *
MultiNodePthreadRoutine(void *arg) {

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH threads firing up\n", myid);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH scheduler has initialized -- Now Spawning fth threads\n", myid);

    if(!myid) 
        fthResume(XSpawn(&fthThreadMultiNodeRecver, 40960), SDF_CONSISTENCY);
    else
        fthResume(XSpawn(&fthThreadMultiNodeSender, 40960), SDF_CONSISTENCY);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d Finished Create and Spawned -- Now starting sched\n", myid);

    fthSchedulerPthread(0);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE, "\nFTH scheduler halted\n");
    return (0);

}

