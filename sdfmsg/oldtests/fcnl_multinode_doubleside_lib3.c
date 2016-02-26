//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/*
 * File:   fcnl_multinode_doubleside_lib3.c
 * Author: mac
 *
 * Created on Aug 28, 2008, 10:45 AM
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
 * node #0 create only one type of protocol sender and more than two receivers, other nodes create only the 
 * same type of sender and receivers. node #0 sends message to other nodes and receives message from other nodes;
 * other nodes receives message from node #0 and sends message to node #0,  you can control 
 * whether need response from receiver. <pnode just hard code now>. 
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
#define FLAG 1
//0 ==> only need ack
//1 ==> both ack and resp

#define FTHRECVERS 2 // Each node can have how many receivers
uint64_t ptl = SDF_CONSISTENCY;

void
fthThreadMultiNodeMstomrMrenSender(uint64_t arg) {
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

    q_pair[node] = sdf_create_queue_pair(myid, node, ptl, ptl, SDF_WAIT_FTH);
    info->queue_pair_info->queue_add[0] = q_pair[node]->q_in;
    info->queue_pair_info->queue_add[1] = q_pair[node]->q_out;
    info->queue_pair_info->queue_pair_type = ptl;
    if(q_pair[node] == NULL) {
        fprintf(stderr, "%s: sdf_create_queue_pair %li failed\n", __func__, ptl);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                     "\nNode %d: EXITING completed sending %d messages - mysync %d\n", myid, l, mysync);
        return;
    }

    FTH_SPIN_LOCK(&ssync->spin);
    mysync ++;
    FTH_SPIN_UNLOCK(&ssync->spin); 

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

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
		"@@node %d, sender type#%li sends protocol#%li message finished, send %d times\n", myid, ptl, ptl, l);
    FTH_SPIN_LOCK(&ssync->spin);
    fthCount ++;
    FTH_SPIN_UNLOCK(&ssync->spin);

    if (myid) {
        while (fthCount != (FTHRECVERS + 1)) fthYield(10);
    }
    else {
        while (fthCount != (FTHRECVERS + 1) * (numprocs - 1)) fthYield(10);
    }

    printf("node %d, sender type%li kill the scheduler.\n", myid, ptl);  
    fthKill(1);
}

void fthThreadMultiNodeMstomrMrenRecver(uint64_t arg) {
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

    // Receiver should wait for all the q-pair finish creating, i think only if all the q-pairs are created then can call sdf_msg_startmsg to start engine
    if (myid) {
        while (!mysync) fthYield(1);
    }
    else {
        while (mysync != (numprocs - 1)) fthYield(1);
    }
    //sdf_msg_startmsg(myid, 0, NULL);

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
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"node %d, receiver type#%li recvs protocol#%d type message from sender %d\n", myid, ptl, d, node);
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

        if(ct == msgCount/FTHRECVERS) break;

    }//end of for statement
    printf("@@node %d, receiver type#%li, receive message finished, receive %d times\n", myid, ptl, ct);

    FTH_SPIN_LOCK(&ssync->spin);
    fthCount ++;
    FTH_SPIN_UNLOCK(&ssync->spin);

    if (myid) {
        while (fthCount != (FTHRECVERS + 1))
            fthYield(10);
    }
    else {
        while (fthCount != (FTHRECVERS + 1) * (numprocs -1))
            fthYield(10);
    }
   
    fthKill(1);
}

void *
MultiNodeMstomrMrenPthreadRoutine(void *arg) {

    uint32_t *numprocs = (uint32_t *)arg;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE, "\nNode %d FTH threads firing up\n", myid);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH scheduler has initialized -- Now Spawning fth threads\n", myid);

    if(myid) {
        XResume(XSpawn(&fthThreadMultiNodeMstomrMrenSender, 40960), 0);// 1 => 0, 2 => 0, 3 => 0 
        for (int i = 0; i < FTHRECVERS; i ++) {
             XResume(XSpawn(&fthThreadMultiNodeMstomrMrenRecver, 40960), 0);// 1 <= 0, 2 <= 0, 3 <= 0
        }
    }
    else {
        // let node #0 create different fths deal with different protocol nodes. <node>: hard code the pnode
        for (int index = 1; index < *numprocs; index ++) {
            XResume(XSpawn(&fthThreadMultiNodeMstomrMrenSender, 40960), index);// 0 => 1, 0 => 2, 0 => 3 
            for (int i = 0; i < FTHRECVERS; i ++) {
                 XResume(XSpawn(&fthThreadMultiNodeMstomrMrenRecver, 40960), index);// 0 <= 1, 0 <= 2, 0 <= 3
            }
        }
    }
    info->pthread_info = 1;
    info->fth_info = FTHRECVERS+(*numprocs)*(1+FTHRECVERS);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,"\nNode %d Finished Create and Spawned -- Now starting sched\n", myid);

    fthSchedulerPthread(0);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE, "\nFTH scheduler halted\n");
    return (0);

}

