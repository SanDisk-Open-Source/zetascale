/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifdef MPI_BUILD
#include <mpi.h>
#endif

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include "platform/logging.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_types.h"
#include "Utilfuncs.h"
#include "fcnl_test.h"
#include <memory.h>

extern uint32_t myid;
extern int sdf_msg_free_buff(sdf_msg_t *msg);
extern int outtahere;

struct sdf_queue_pair *q_pair_CONSISTENCY;
struct sdf_queue_pair *q_pair_RESPONSES;
// struct sdf_queue_pair *q_pair_local_CONSISTENCY;
// struct sdf_queue_pair *q_pair_local_RESPONSES;
struct ssdf_queue_pair *q_pair_revert_CONSISTENCY;

static fthMbox_t ackmbox, respmbox, ackmbx1, ackmbx2;
static int cluster_node[MAX_NUM_SCH_NODES];

static int mysync = 0;
static int endsync = 2;
static struct startsync crp;
static startsync_t *ssync = &crp;
#define MSG1 "Who Tell ME"
#define MSG2 "I Tell YOU"
#define MAXCNT 32


static void sendThread(uint64_t arg){
    int i, l, ret;
    uint64_t aresp;
    struct sdf_msg *send_msg = NULL;
    vnode_t node;
    serviceid_t protocol = SDF_CONSISTENCY;
    serviceid_t my_protocol = SDF_CONSISTENCY;
    msg_type_t type = REQ_FLUSH;
    sdf_fth_mbx_t fthmbx;
    fthMbox_t *fthmbxtst;
    int debug = 0;
    
    fthmbxtst = &respmbox;

    fthMboxInit(&ackmbox);
    fthMboxInit(&respmbox);

    fthmbx.actlvl = SACK_BOTH_FTH;
    fthmbx.abox = &ackmbox;
    fthmbx.rbox = &respmbox;
    
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
    
    //Initial the Q-pair
    
    q_pair_CONSISTENCY = local_create_myqpairs(SDF_CONSISTENCY, myid, node);

    if (q_pair_CONSISTENCY == NULL) {
        fprintf(stderr, "%s: sdf_create_queue_pair failed\n", __func__);
        return;
    }

    q_pair_revert_CONSISTENCY = local_create_myqpairs(SDF_CONSISTENCY, node,
            myid);

    if (q_pair_revert_CONSISTENCY == NULL) {
        fprintf(stderr, "%s: sdf_create_queue_pair failed\n", __func__);
        return;
    }

    FTH_SPIN_LOCK(&ssync->spin);
    mysync = 1;
    FTH_SPIN_UNLOCK(&ssync->spin);
    
    //Init the sdf_msg, only once
    sdf_msg_startmsg(myid, 0, NULL);


    
    for(l=0;i<MAXCNT;i++){
        sdf_msg_t *msg = (struct sdf_msg_t *)sdf_msg_alloc(8192);
        memcpy(send_msg->msg_payload,MSG1,sizeof(MSG1));
        type = REQ_FLUSH;
        
        ret = sdf_msg_send((struct sdf_msg *)send_msg, 8192, node, protocol,
                               myid, my_protocol, type, &fthmbx, NULL);
        if (ret != 0)
            process_ret(ret, protocol, type, myid);
        msg = (sdf_msg_t *)fthMboxWait(&respmbox);
        
        printf("node %d, sender recv response message from recriver\n", myid);
        
        fthYield(1);
        
        while (mysync != 3) {   
            fthYield(100);
            
            printf("Node %d has finished sending %d msg - %d!",myid,l, mysync);
    }
        
    }
}

static void rcvThread(uint64_t arg){
    int i, ret, ct = 0;
    uint64_t aresp;
    struct sdf_msg *send_msg = NULL, *recv_msg = NULL;
    vnode_t node;
    serviceid_t protocol = SDF_RESPONSES;
    serviceid_t my_protocol = SDF_RESPONSES;
    msg_type_t type = RESP_ONE;
    sdf_fth_mbx_t fthmbx;
    fthmbx.actlvl = SACK_ONLY_FTH;
    fthmbx.abox = &ackmbx1;
    fthmbx.rbox = NULL;
    fthMboxInit(&ackmbx1);

    int sendfinished = 0;
    printf("FTH Thread starting %s\n", __func__);

  /*  if (FASTPATH_TEST) {
        node = myid;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
    } else {*/
        node = myid == 0 ? 1 : 0;
    /*}*/

    q_pair_RESPONSES = local_create_myqpairs(SDF_RESPONSES, myid, node);

   /* plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: created queue pair %p sn %d dn %d ss %d ds %d\n", myid,
            q_pair_RESPONSES, myid, myid == 0 ? 1 : 0, SDF_RESPONSES,
            SDF_RESPONSES);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Now yielding waiting for mysync\n", myid);*/

    /* Need to yield till all queues have been created */
    while (!mysync)
        fthYield(1);
    
    while(1){
        recv_msg = sdf_msg_receive(q_pair_CONSISTENCY->q_out, 0, B_TRUE);
        printf("\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d"
                     " akrpmbx %p\n", myid, 
                     recv_msg, recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode,
	             recv_msg->msg_dest_service, recv_msg->msg_type,
                     recv_msg->akrpmbx);
         printf("node %d, receiver recv message from sender\n", myid);
        send_msg = sdf_msg_alloc(recv_msg->msg_len);
        
        memcpy(send_msg, recv_msg, recv_msg->msg_len);
        ret = sdf_msg_send((struct sdf_msg *)send_msg, 8192, node, protocol,
			   myid, my_protocol, type, &fthmbx,
                           sdf_msg_get_response_mbx(recv_msg));
	if (ret != 0) {
            process_ret(ret, protocol, type, myid);
        }

        aresp = fthMboxWait(&ackmbx1);//must call this method?
        // release the receive buffer back to the sdf messaging thread
        ret = sdf_msg_free_buff(recv_msg);
        aresp = fthMboxWait(&ackmbx1);
        ret = sdf_msg_free_buff(recv_msg);
       
    }
    
    mysync++;
    fthYield(1);
    
    printf("Send %i has finished",(uint64_t)arg);
}
    
    void * MultiSchedulerRoutine(void * arg){
        int index;
        
        for(index = 0 ; index < 2; index ++)
            fthResume(fthSpawn(&sendThread, 40960), index);
        for(index = 0 ; index < 4; index ++)
            fthResume(fthSpawn(&rcvThread, 40960), index);    
        fthSchedulerPthread(0);
        return (void *)0;
    }


