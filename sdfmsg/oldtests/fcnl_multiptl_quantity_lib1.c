/*
 * File:   fcnl_multiptl_quantity_lib1.c
 * Author: mac
 *
 * Created on Jul 25, 2008, 10:35 AM
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

static struct sdf_queue_pair *q_pair[SDF_PROTOCOL_COUNT];

#define FTHRECVERS 14

static int mysync[SDF_PROTOCOL_COUNT] = {0};
static int cluster_node[MAX_NUM_SCH_NODES];
static struct startsync crp;
static startsync_t *ssync = &crp;

static int msgCount =50, fthCount = 0, msgBp = 15;
/* 
 * This fth thread simulates the action node, and don't need to wait ack or resp.
 * the msg types are arbitrary for now, here it is REQ_FLUSH
 *
 * @brief: n(sender) -> n(recver) "multiprotocol"
 * Create sender and receiver for each protocol. The sender just get the ack when it sends success and the receiver
 * do not get the message from the queue. But if when sender the #msgBp message, the receiver should send a 
 * response to the sender.
 * 
 */
void
fthThreadMultiPtlQuaSender(uint64_t arg) {
    int l = 0;
    vnode_t node;
    struct sdf_msg *send_msg = NULL;
    
    msg_type_t type = REQ_FLUSH;

    sdf_fth_mbx_t fthmbx, fthmbx_bp;
    fthMbox_t ackmbox, ackmbox_bp, respmbox_bp;
 
    uint64_t ptl = arg; 
    //fthmbx: let the sender just receives ack when sending successful
    fthmbx.actlvl = SACK_ONLY_FTH;
    fthMboxInit(&ackmbox);
    fthmbx.abox = &ackmbox;

    //fthmbx_bp: let the sender receives ack when sending successful and receive response from receiver
    fthmbx_bp.actlvl = SACK_BOTH_FTH;
    fthMboxInit(&ackmbox_bp);
    fthMboxInit(&respmbox_bp);
    fthmbx_bp.abox = &ackmbox_bp;
    fthmbx_bp.rbox = &respmbox_bp; 

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nnode %d, fth thread sender starting  %s: number of msgs to send = %d\n", 
                myid, __func__, msgCount);
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
        int i;
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

    q_pair[ptl] = local_create_myqpairs(ptl, myid, node);
    
    /*
     * Below code will cause segment error, ignore them
     */

#if 0
    info->queue_pair_info->queue_add[0] = q_pair[node]->q_in;
    info->queue_pair_info->queue_add[1] = q_pair[node]->q_out;
    info->queue_pair_info->queue_pair_type = SDF_CONSISTENCY;
    if(q_pair[ptl] == NULL) {
        fprintf(stderr, "%s: sdf_create_queue_pair %li failed\n", __func__, ptl);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: EXITING completed sending %d messages\n", 
                     myid, l);
        return;
    }
    
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: created queue pair %p sn %d dn %d ss %li ds %li maxcnt %d\n",
                 myid, q_pair[ptl], myid, myid == 0 ? 1 : 0,
                 ptl, ptl, msgCount);
#endif

    FTH_SPIN_LOCK(&ssync->spin);
    mysync[ptl] = 1;
    FTH_SPIN_UNLOCK(&ssync->spin);

    //sdf_msg_startmsg(myid, 0, NULL);

    for (l = 0; l < msgCount; ++l) {
        int ret, i;
        sdf_msg_t *msg;

        send_msg = (struct sdf_msg *) sdf_msg_alloc(TSZE); 

        if (send_msg == NULL) {
             plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                          "sdf_msg_alloc(TSZE) failed\n");
        }
        
		/*
        for (i = 0; i < TSZE; ++i) {//TSZE = 64
            if(myid == 0)
                send_msg->msg_payload[i] = (char)(l + 65);
            else
                send_msg->msg_payload[i] = (char)(l + 97);

         }
		*/

	for (i = 0; i < TSZE; ++i)
	    send_msg->msg_payload[i] = ptl * msgCount + l;

	type = REQ_FLUSH;

        if(l == msgBp-1) {
            
            ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, ptl, myid, ptl, type, &fthmbx_bp, NULL);
            fthMboxWait(&ackmbox_bp);
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
	                "\nnode %d, sender %li waiting for #%d times response.\n", 
                        myid, ptl, msgBp);

            msg = (sdf_msg_t *)fthMboxWait(&respmbox_bp);
#if 0
    if(msg) {
        uint32_t d = msg->msg_src_service;
        printf("node %d, sender #%d recvs response protocol#%d message from recver, at bp %d\n", myid, ptl, d, msgBp);
        char *m = (char *)(msg->msg_payload);
        for (i = 0; i < TSZE; i++) {
            printf("%d-%d-%d  ", *m, d, myid);//the style of data is "number-protocol-node"
            m++;
            if ((i % 16) == 15) {
                printf("  myid %d", myid);
                putchar('\n');
                fflush(stdout);
            }
        }
    }
    else {
        printf("!!node %d, sender #%li recvs response protocol#%d meessage from recver failed\n", myid, ptl, msg->msg_src_service);
    }
#endif
            ret = sdf_msg_free_buff(msg); 
        }
        else {
            ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, ptl, myid, ptl, type, &fthmbx, NULL);
            fthMboxWait(&ackmbox);
        }

        if(myid == 0) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                        "\nnode %d ,sender #%li sends %d times, message contents %li-%li-%d\n", 
                        myid, ptl, l + 1, ptl * msgCount + l, ptl, myid);
        }
        else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                        "\nnode %d, sender #%li sends %d times, message contents %li-%li-%d\n", 
                        myid, ptl, l + 1, ptl *msgCount + l, ptl, myid);
  
        } 
	if (ret != 0 )
            process_ret(ret, ptl, type, myid);

        fthYield(1);
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\n@@node %d, sender #%li sends %li protocol message finished, send %d times\n", 
                myid, ptl, ptl, l + 1);

    FTH_SPIN_LOCK(&ssync->spin);
	fthCount ++;
	FTH_SPIN_UNLOCK(&ssync->spin);
	
	while (fthCount != 2 * FTHRECVERS) {
        fthYield(100);
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nnode %d, sender %li kill the scheduler.\n", 
                 myid, ptl); 

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: EXITING completed sending %d messages - mysync %d\n", 
                 myid, l + 1, mysync[ptl]);
    fthKill(1);
}

void fthThreadMultiPtlQuaRecver(uint64_t arg) {

    int i = 0, ret, ct = 0;
    uint64_t aresp = 0, ptl;
    struct sdf_msg *recv_msg = NULL, *send_msg = NULL;
    vnode_t node;

    sdf_fth_mbx_t fthmbx;
    fthMbox_t ackmbox;
    fthmbx.actlvl = SACK_ONLY_FTH;
    fthMboxInit(&ackmbox);
    fthmbx.abox = &ackmbox;
    fthmbx.rbox = NULL;
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
 	
    while (mysync[ptl] != 1) {
         fthYield(1);
    }

    for (;;) {

        ct = q_pair[ptl]->q_out->q_atomic_val.q_atomic_bits.q_fill_index - q_pair[ptl]->q_out->q_atomic_val.q_atomic_bits.q_empty_index;

        if(ct < 0) ct = -ct;
        if(ct == msgBp) {  
            recv_msg  = q_pair[ptl]->q_out->sdf_queue[msgBp-1]->q_msg; 

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
        serviceid_t d = recv_msg->msg_dest_service;
		vnode_t nd = recv_msg->msg_src_vnode;
        printf("node %d, receiver #%d recvs protocol#%d message from sender\n", myid, ptl, d);
        char *m = (char *)(recv_msg->msg_payload);
        for (i = 0; i < TSZE; i++) {
            printf("%d-%d-%d  ", *m, d, nd);
            m++;
            if ((i % 16) == 15) {
                printf("  myid %d", myid);
                putchar('\n');
                fflush(stdout);
            }
        }
    }
    else {
        printf("!!node %d, receiver #%d recvs protocol#%d meessage from sender failed\n", myid, ptl, recv_msg->msg_dest_service);
    }    
#endif

        send_msg = (struct sdf_msg *) sdf_msg_alloc(recv_msg->msg_len);
        if (send_msg == NULL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "sdf_msg_alloc(recv_msg->msg_len) failed\n");
        }
        memcpy(send_msg->msg_payload, recv_msg->msg_payload, recv_msg->msg_len - sizeof(sdf_msg_t));
		
        struct sdf_resp_mbx rhkey;
        struct sdf_resp_mbx *ptrkey = &rhkey;

        strncpy(rhkey.mkey, MSG_DFLT_KEY, (MSG_KEYSZE - 1));
        rhkey.mkey[MSG_KEYSZE - 1] = '\0';
        rhkey.akrpmbx_from_req = NULL;
        rhkey.rbox = NULL;
        
        ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, SDF_RESPONSES,
			   myid, SDF_RESPONSES, RESP_ONE, &fthmbx,
                           sdf_msg_get_response(recv_msg, ptrkey));
        if(ret != 0) {
            process_ret(ret, SDF_RESPONSES, RESP_ONE, myid);
        }
        fthMboxWait(&ackmbox);      
      
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Send Buff Freed aresp %ld loop %d\n", 
                     myid, aresp, ct);
        i = 1;
}
        
        if(i == 1) break;
	fthYield(1);
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\n@@node %d, receiver #%li, receive message finished, receive %d times\n", 
                 myid, ptl, i);
    
    FTH_SPIN_LOCK(&ssync->spin);
    fthCount ++;
    FTH_SPIN_UNLOCK(&ssync->spin);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nnode: %d, protocol: %li, mysync[ptl]: %d, fthCount: %d\n", 
                 myid, ptl, mysync[ptl], fthCount);
    fthYield(1);
    
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: WORKER FTH exiting - loop %d mysync %d\n", 
                 myid, ct, mysync[ptl]);
    
}

void *
MultiPtlQuantityPthreadRoutine(void *arg) {

    int index;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH threads firing up\n", 
                 myid);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d FTH scheduler has initialized -- Now Spawning fth threads\n", 
                 myid);

    for(index = 0; index < SDF_PROTOCOL_COUNT; index ++) {
        if(index == SDF_DEBUG || index == SDF_SYSTEM)
            continue;
        XResume(XSpawn(&fthThreadMultiPtlQuaRecver, 40960), index);
        XResume(XSpawn(&fthThreadMultiPtlQuaSender, 40960), index);
    }
    
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d Finished Create and Spawned -- Now starting sched\n", 
                 myid);
    info->pthread_info = 1;
    info->fth_info = SDF_PROTOCOL_COUNT*2;
    
    fthSchedulerPthread(0);

    printf("the pthread including the fth scheduler end.\n");
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nFTH scheduler halted\n");

    return (0);

}

