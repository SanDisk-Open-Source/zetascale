/*
 * File:   fcnl_multinode_doubleside_lib1.c
 * Author: mac
 *
 * Created on Aug 27, 2008, 12:56 AM
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
 * @brief: 1 -> x (doubleside, singleprotocol) => doubleside means each nodes both send and receive message
 * node #0 creates only one type of protocol receiver and more than two senders, other nodes create only the same type 
 * of senders and receiver. node #0 sends message to other nodes and receives message from other nodes;
 * other nodes receives message from node #0 and sends message to node #0, you can control whether need response 
 * from receiver. <pnode just hard code now>. 
 */

static struct sdf_queue_pair *q_pair[SDF_PROTOCOL_COUNT];
static int mysync = 0;
//static int mysync[SDF_PROTOCOL_COUNT] = {0};
static int cluster_node[MAX_NUM_SCH_NODES];
static struct startsync crp;
static startsync_t *ssync = &crp;

//the sender send one types protocol message
static int msgCount = 12, fthCount = 0;
/* 
 * This fth thread simulates the action node, and don't need to wait ack or resp.
 * the msg types are arbitrary for now, here it is REQ_FLUSH
 */

// 0 ==> only need ack
// 1 ==> both ack and resp
#define FLAG 1

// You can set the count of sender
#define FTHSENDERS 2
uint64_t ptl = SDF_CONSISTENCY;

void
fthThreadMultiNodeMstomrMsenSender(uint64_t arg) {
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


    printf("node %d, fth thread sender starting  %s: number of msgs to send = %d\n", myid, __func__, msgCount);
    fflush(stdout);

    int localpn, actmask;
    uint32_t numprocs;
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node,  &actmask);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: numprocs %d active_procs mask 0x%x active_mask 0x%x\n", localrank, numprocs, localpn, actmask);
    node = arg;//0: 1, 2 , 3 |  1, 2, 3: 0
    // Sender should wait for all the q-pair finish creating, i think only if all the q-pairs are created then can call sdf_msg_startmsg to start engine
    if (myid) {
        while (!mysync) fthYield(1);
    }
    else {
        while (mysync != (numprocs - 1)) fthYield(1);
    }
    //sdf_msg_startmsg(myid, 0, NULL);

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
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
        	"node %d, wait and get the response message from pnode.\n", myid);
        /* get the response when receive message success. */
        sdf_msg_t * msg = (sdf_msg_t *)fthMboxWait(&respmbox);
        ret = sdf_msg_free_buff(msg);
#endif
	if (ret != 0 )
            process_ret(ret, ptl, type, myid);

        fthYield(1);
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,"@@node %d, sender type#%li sends protocol#%li message finished, send %d times\n", myid, ptl, ptl, l);
    FTH_SPIN_LOCK(&ssync->spin);
    fthCount ++;
    FTH_SPIN_UNLOCK(&ssync->spin);

    if (myid) {
        while (fthCount != (FTHSENDERS + 1)) fthYield(10);
    }
    else {
        while (fthCount != (FTHSENDERS + 1) * (numprocs - 1)) fthYield(10);
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,"node %d, sender type%li kill the scheduler.\n", myid, ptl);  
    fthKill(1);
}

void fthThreadMultiNodeMstomrMsenRecver(uint64_t arg) {
    int ret, ct = 0;
    uint64_t aresp = 0;
    struct sdf_msg *recv_msg = NULL, *send_msg = NULL;
    vnode_t node;
    printf("node %d, fth thread receiver type#%li starting %s\n", myid, ptl, __func__);
    fflush(stdout);

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

    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    if (localrank); // keep compiler happy
    node = arg;
    q_pair[node] = sdf_create_queue_pair(myid, node, ptl, ptl, SDF_WAIT_FTH);
    info->queue_pair_info->queue_add[0] = q_pair[node]->q_in;
    info->queue_pair_info->queue_add[1] = q_pair[node]->q_out;
    info->queue_pair_info->queue_pair_type = ptl;
    
    if(q_pair[node] == NULL) {
        fprintf(stderr, "%s: sdf_create_queue_pair %li failed\n", __func__, ptl);
        return;
    }

    FTH_SPIN_LOCK(&ssync->spin);
    mysync ++;
    FTH_SPIN_UNLOCK(&ssync->spin);
    for (;;) {
        recv_msg = sdf_msg_receive(q_pair[node]->q_out, 0, B_TRUE);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Waiting for messages q_pair %p loop %d\n", myid, q_pair[node], ct);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d"
                     " akrpmbx %p\n", myid, recv_msg, recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode,
                     recv_msg->msg_dest_service, recv_msg->msg_type, recv_msg->akrpmbx);

#if 1
    if(recv_msg) {
        uint32_t d = recv_msg->msg_dest_service;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,"node %d, receiver type#%li recvs protocol#%d type message from sender %d\n", myid, ptl, d, node);
#if 0
		local_printmsg_payload(recv_msg, TSZE, myid);
#endif
    }   
    else {
        printf("!!node %d, receiver type#%li recvs protocol#%d meessage from sender failed\n", myid, ptl, recv_msg->msg_dest_service);
    }    
#endif
    
#if FLAG
        send_msg = (struct sdf_msg *) sdf_msg_alloc(recv_msg->msg_len);
        memcpy(send_msg->msg_payload, recv_msg->msg_payload, recv_msg->msg_len);
        
        struct sdf_resp_mbx rhkey;
        struct sdf_resp_mbx *ptrkey = &rhkey;

        strncpy(rhkey.mkey, MSG_DFLT_KEY, (MSG_KEYSZE - 1));
        rhkey.mkey[MSG_KEYSZE - 1] = '\0';
        rhkey.akrpmbx_from_req = NULL;
        rhkey.rbox = NULL;

        ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, ptl, myid, ptl, RESP_ONE, &fthmbx, 
                        sdf_msg_get_response(recv_msg, ptrkey));
        fthMboxWait(&ackmbox1);
#endif

        // release the receive buffer back to the sdf messaging thread
        ret = sdf_msg_free_buff(recv_msg);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Send Buff Freed aresp %ld loop %d\n", myid, aresp, ct);
        ct++;

        if(ct == msgCount * FTHSENDERS) break;

    }//end of for statement
    printf("@@node %d, receiver type#%li, receive message finished, receive %d times\n", myid, ptl, ct);

    FTH_SPIN_LOCK(&ssync->spin);
    fthCount ++;
    FTH_SPIN_UNLOCK(&ssync->spin);

    if (myid) {
        while (fthCount != (FTHSENDERS + 1))
            fthYield(10);
    }
    else {
        while (fthCount != (FTHSENDERS + 1) * (numprocs -1))
            fthYield(10);
    }
   
    fthKill(1);
}

void *
MultiNodeMstomrMsenPthreadRoutine(void *arg) {

    uint32_t *numprocs = (uint32_t *)arg;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE, "\nNode %d FTH threads firing up\n", myid);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH scheduler has initialized -- Spawning fth threads - numprocs %d\n", myid, *numprocs);

    if (myid) {
        for (int i = 0; i < FTHSENDERS; i ++) {
            XResume(XSpawn(&fthThreadMultiNodeMstomrMsenSender, 40960), 0);// 1 => 0, 2 => 0, 3 => 0 
        }
        XResume(XSpawn(&fthThreadMultiNodeMstomrMsenRecver, 40960), 0);// 1 <= 0, 2 <= 0, 3 <= 0
    }
    else {
        // let node #0 create different fths deal with different protocol nodes. <node>: hard code the pnode
        for (int index = 1; index < *numprocs; index ++) {
            for (int i = 0; i < FTHSENDERS; i ++) {
                XResume(XSpawn(&fthThreadMultiNodeMstomrMsenSender, 40960), index);// 0 => 1, 0 => 2, 0 => 3 
            }
            XResume(XSpawn(&fthThreadMultiNodeMstomrMsenRecver, 40960), index);// 0 <= 1, 0 <= 2, 0 <= 3
        }
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,"\nNode %d Finished Create and Spawned -- Now starting sched\n", myid);
    info->pthread_info = 1;
    info->fth_info = FTHSENDERS+(*numprocs)*(1+FTHSENDERS);
    fthSchedulerPthread(0);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE, "\nFTH scheduler halted\n");
    return (0);

}

