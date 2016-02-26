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
#include "sdfmsg/sdf_msg.h"
#include "Utilfuncs.h"
#include "fcnl_test.h"
#include <sys/time.h>
#include <string.h>

extern uint32_t myid;
extern int sdf_msg_free_buff(sdf_msg_t * msg);


#define DBGP 0
#define SENDTWO 0
#define DIVNUM 100
#define TSZE 2048
#define SHORTTEST 10
#define UNEXPT_TEST 0
#define FASTPATH_TEST 0
#define SHOWBUFF 0
#define PFM_STR "In this performance test,Role %s - %li messages are passed in %li seconds and %li u-seconds\n"
#define PFM_ASTR "In this test, average time per message is %li seconds and %li u-seconds\n"
#define SENDER 0
#define RECEIVER 1

PLAT_LOG_CAT_LOCAL(LOG_CAT, "sdf/sdfmsg/tests/mpilog");

struct sdf_queue_pair *q_pair_CONSISTENCY;
struct sdf_queue_pair *q_pair_RESPONSES;
struct sdf_queue_pair *q_pair_revert_CONSISTENCY;

static fthMbox_t ackmbox, respmbox, ackmbx1;
static int cluster_node[MAX_NUM_SCH_NODES];

static int mysync = 0, sync_resp = 0;
static struct startsync crp;
static startsync_t *ssync = &crp;
static struct startsync resp;
static startsync_t *rsync = &resp;
static int recv_ct = 0, send_ttl = 0;
/*
 * get current timestamp, and count in us
 */
static uint64_t
get_timestamps()
{
    uint64_t value = 0;
    struct timeval tv;
    struct timezone tz;

    if (!gettimeofday(&tv, &tz))
        value = tv.tv_sec * 1000 * 1000 + tv.tv_usec;
    return value;
}

void
perform_print(int role, uint64_t size, uint64_t time)
{
    uint64_t sec, usec, avg, asec, ausec;
    printf("************Perfomance Result***************\n");
    if (time <= 0) {
        printf("Error on time calculation!\n");
        fflush(stdout);
        return;
    }
    avg = time / size;
    sec = time / 1000 / 1000;
    usec = time - sec * 1000 * 1000;
    asec = avg / 1000 / 1000;
    ausec = avg - asec * 1000 * 1000;
    printf(PFM_STR, role == SENDER ? "SENDER" : "RECEIVER", size, sec, usec);
    printf(PFM_ASTR, asec, ausec);
    fflush(stdout);
}

static void
fthSender(uint64_t arg)
{
    struct sdf_msg *send_msg = NULL;
    vnode_t node;
    serviceid_t protocol = SDF_CONSISTENCY;
    serviceid_t my_protocol = SDF_CONSISTENCY;
    msg_type_t type = REQ_FLUSH;
    sdf_fth_mbx_t fthmbx;
    fthMbox_t *fthmbxtst;
    uint64_t msg_count = arg;
    uint64_t start, end;
    uint64_t aresp;
    uint64_t time = 0;
    int i, l;

    fthmbxtst = &respmbox;

    fthMboxInit(&respmbox);
    fthMboxInit(&respmbox);

    fthmbx.actlvl = SACK_BOTH_FTH;
    fthmbx.abox = &ackmbox;
    fthmbx.rbox = &respmbox;

    printf("FTH Thread starting %s Number of msgs to send = %li arg in %li\n",
           __func__, arg, arg);
    printf("Now start to timing...\n");
    fflush(stdout);

    

    /* node is the destination node */
    int localpn, actmask;
    uint32_t numprocs;
    int localrank =
        sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    if (numprocs == 1) {
        node = 0;
    }
    else {
        node = local_get_pnode(localrank, localpn, numprocs);
        printf("Node %d: %s my pnode is  %d\n", localrank, __func__, node);
        fflush(stdout);
        for (i = 0; i < numprocs; i++) {
            printf("Node %d: %s cluster_node[%d] = %d\n", localrank, __func__,
                   i, cluster_node[i]);
            fflush(stdout);
        }
    }
    /* you only init this once but share the q_pairs among the other threads here */

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
    /* let the msg thread do it's thing */

    sdf_msg_startmsg(myid, 0, NULL);


    for (l = 0; l < msg_count; ++l) {
        sdf_msg_t *msg;
        int ret;
        start = get_timestamps();
        send_msg = (struct sdf_msg *)sdf_msg_alloc(TSZE);
        
        end = get_timestamps();
        time += (end - start);
        for (i = 0; i < TSZE; ++i)
             send_msg->msg_payload[i] = (unsigned char) l;
            //memcpy(send_msg->msg_payload++, &l, sizeof(unsigned char));

            type = REQ_FLUSH;

        ret =
            sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, protocol,
                         myid, my_protocol, type, &fthmbx, NULL);

        if (ret != 0)
            process_ret(ret, protocol, type, myid);
        
        aresp = fthMboxWait(&ackmbox);

        msg = (struct sdf_msg *)fthMboxWait(&respmbox);
        sdf_msg_free_buff(msg);
        
        fthYield(1);
    }

    msg_type_t say_goodbye = GOODBYE;
    int ret = sdf_msg_say_bye(node, protocol, myid, my_protocol, say_goodbye,
                              &fthmbx, TSZE);
    if (ret != 0) {
        process_ret(ret, protocol, type, myid);
    }
    
    while (mysync <= 2)
        fthYield(100);


    perform_print(SENDER, arg, time);

    fthKill(5);                 // Kill off FTH

}

static void
fthReceiver(uint64_t arg)
{
    int ret;
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
    uint64_t start, end;
    uint64_t time = 0;
    fthMboxInit(&ackmbx1);

    
    printf("FTH Thread starting %s\n", __func__);
    fflush(stdout);

    node = myid == 0 ? 1 : 0;
    
    /*
     * You should keep the pair response be initialized ony once
     */
    
    if(arg == 0){
        q_pair_RESPONSES = local_create_myqpairs(SDF_RESPONSES, myid, node);
        FTH_SPIN_LOCK(&rsync->spin);
        sync_resp++;
        FTH_SPIN_UNLOCK(&rsync->spin);
    }
        
    else {
        while(!sync_resp)
            fthYield(100);
    }

    while (!mysync)
        fthYield(1);

    printf("Now timing!\n");
    
    struct sdf_resp_mbx rhkey;
    struct sdf_resp_mbx *ptrkey = &rhkey;

    strncpy(rhkey.mkey, MSG_DFLT_KEY, (MSG_KEYSZE - 1));
    rhkey.mkey[MSG_KEYSZE - 1] = '\0';
    rhkey.akrpmbx_from_req = NULL;
    rhkey.rbox = NULL;
    
    
    for (;;) {

        start = get_timestamps();
        recv_msg = sdf_msg_receive(q_pair_CONSISTENCY->q_out, 0, B_TRUE);

        end = get_timestamps();
        time += (end - start);
        if(recv_msg == NULL){
            printf("Receive msg NULL err\n");
            break ;
        }
        if (recv_msg->msg_type == GOODBYE && recv_ct >= send_ttl)
            break;


        send_msg = (struct sdf_msg *)sdf_msg_alloc(recv_msg->msg_len);
        if (send_msg == NULL) {
            fprintf(stderr, "sdf_msg_alloc(recv_msg->msg_len) failed\n");
            return;
        }

        for(int i = 0; i<TSZE; i++)
	    send_msg->msg_payload[i] = (unsigned char) 0x69;
	    //memcpy(send_msg, recv_msg, sizeof(struct sdf_msg));
	    

	    
        ret =
            sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, protocol,
                         myid, my_protocol, type, &fthmbx,
                         sdf_msg_get_response(recv_msg,ptrkey));

        if (ret != 0) {
            process_ret(ret, protocol, type, myid);
        }

        if(recv_msg->msg_type != GOODBYE)
            aresp = fthMboxWait(&ackmbx1);
        /* release the receive buffer back to the sdf messaging thread */
        //ret = sdf_msg_free_buff(send_msg);
        ret = sdf_msg_free_buff(recv_msg);
        recv_ct++;
      //  if(recv_ct % 100 == 0)
            printf("Receiver %li now MSG %d is received.\n",arg ,recv_ct);    // Show indicator every 100 msg
        
        fthYield(1);

    }
    
    perform_print(RECEIVER, recv_ct, time);
    fflush(stdout);
    mysync++;
    if(mysync != 3){
        msg_type_t say_goodbye = GOODBYE;
        ret = sdf_msg_say_bye(myid, SDF_CONSISTENCY, node, SDF_CONSISTENCY,
                        say_goodbye, &fthmbx, TSZE);
    }
    
    fthYield(1);

}

void *
PerformancePthreadRoutine(void *arg)
{

    int size = *(int *)arg;
    send_ttl = size;
    
    fthInit();                  // Init a scheduler

    // Start a thread
    for(int i = 0 ; i <= 2 ; i++)
        fthResume(fthSpawn(&fthReceiver, 40960), i);
    
    usleep(500);
    fthResume(fthSpawn(&fthSender, 40960), size);
    


    fthSchedulerPthread(0);

    return (0);

}
