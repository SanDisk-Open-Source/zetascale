/*
 * File:   fcnl_queue_order_fth_lib1.c
 * Author: Norman Xu
 *
 * Created on June 23, 2008, 7:45AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcnl_queue_order_fth_lib1.c 308 2008-06-23 22:34:58Z normanxu $
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
#include "fcnl_test.h"
#include "Utilfuncs.h"
#include "log.h"

extern struct test_info * info;
extern uint32_t myid;
extern int sdf_msg_free_buff(sdf_msg_t *msg);
extern int outtahere;

#define DBGP 0
#define SENDTWO 0
#define DIVNUM 100
#define TSZE 1024
#define BIGSIZE 8192
#define FIXSIZE 0
#define NANSEC 1.00e-9f
/* SHORTTEST will set number of messages sent before terminating test */
#define SHORTTEST 100
#define UNEXPT_TEST 0
#define FASTPATH_TEST 0

#define FTH_NUM 1

/* This is the number */
#define PTHREAD_NUM 4

PLAT_LOG_CAT_LOCAL(LOG_CAT, "sdf/sdfmsg/tests/mpilog");

/*
 * These fthreads will be used to simulate the SDF protocol sending and
 * receiving messages to the sdf messaging engine and across nodes
 */

struct sdf_queue_pair *q_pair_CONSISTENCY;
struct sdf_queue_pair *q_pair_RESPONSES;

static fthMbox_t ackmbox[PTHREAD_NUM * FTH_NUM], respmbox[PTHREAD_NUM * FTH_NUM], ackmbx1;
static int cluster_node[MAX_NUM_SCH_NODES];

static int mysync = 0;
static struct startsync crp;
static startsync_t *ssync = &crp;

static int sendcount = 0;
static struct startsync countsync;
static startsync_t *csync = &countsync;

/*
 * This fth thread simulates the Action Node, it sends a CONSISTENCY msg and
 * sleeps on a mailbox waiting for a response. The msg types are arbitrary for now
 */
int maxcnt = 0, pthreadcount = 0;
static void fthThreadSender(uint64_t arg) {
    int i, l;
    uint64_t aresp;
    struct sdf_msg *send_msg = NULL;

    serviceid_t protocol = SDF_CONSISTENCY;
    serviceid_t my_protocol = SDF_CONSISTENCY;
    msg_type_t type = REQ_FLUSH;
    sdf_fth_mbx_t fthmbx;
    fthMbox_t *fthmbxtst;
    int debug = 0;
    int msgsize;
    vnode_t node;
    uint64_t fthlabel = arg;

#if SENDTWO
    maxcnt = 5;
#elif SHORTTEST
    maxcnt = SHORTTEST;
#else
    maxcnt = 990000;
#endif

    fthmbxtst = &respmbox[fthlabel];

    fthMboxInit(&ackmbox[fthlabel]);
    fthMboxInit(&respmbox[fthlabel]);

    fthmbx.actlvl = SACK_BOTH_FTH;
    fthmbx.abox = &ackmbox[fthlabel];
    fthmbx.rbox = &respmbox[fthlabel];

    printf("FTH Thread starting %s Number of msgs to send = %d arg in %li\n",
            __func__, maxcnt, arg);
    fflush(stdout);

    if (DBGP) {
        plat_log_msg(
                PLAT_LOG_ID_INITIAL,
                LOG_CAT,
                PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: fth mb info fthmbxtst %p rbox %p abox %p lvl %d maxcnt %d\n",
                myid, fthmbxtst, fthmbx.rbox, fthmbx.abox, SACK_BOTH_FTH,
                maxcnt);
    }

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
    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: created queue pair %p sn %d dn %d ss %d ds %d maxcnt %d\n",
            myid, q_pair_CONSISTENCY, myid, node, SDF_CONSISTENCY,
            SDF_CONSISTENCY, maxcnt);

    FTH_SPIN_LOCK(&ssync->spin);
    mysync = 1;
    FTH_SPIN_UNLOCK(&ssync->spin);
    if (myid == 1) {
        debug = 0;
        if (debug) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: DEBUG --- NOT SENDING MESSAGES FROM HERE", myid);
            while (debug)
                fthYield(100); /* Don't send mesages from node one for now */
        }
    }

    msgsize = TSZE;
    for (l = 0; l < maxcnt; ++l) {
        sdf_msg_t *msg;
        unsigned char *m;
        int ret;

        msgsize = TSZE - sizeof(struct sdf_msg);

        send_msg = (struct sdf_msg *) sdf_msg_alloc(msgsize);

        if (send_msg == NULL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                    "sdf_msg_alloc(TSZE) failed\n");
        }

        uint64_t command = 0x01100110;
        for (i = 0; i < msgsize; ++i)
            send_msg->msg_payload[i] = (unsigned char) 0xFF;

        FTH_SPIN_LOCK(&csync->spin);
        uint64_t count = sendcount++;
        FTH_SPIN_UNLOCK(&csync->spin);

        memcpy((void *) &send_msg->msg_payload[0], &command, sizeof(uint64_t));
        memcpy((void *) &send_msg->msg_payload[8], &count, sizeof(uint64_t));
        /*
         * Send 2 CONSISTENCY messages with different types to track if we define SENDTWO
         */

        type = REQ_FLUSH;

        ret = sdf_msg_send((struct sdf_msg *) send_msg, msgsize, node,
                protocol, myid, my_protocol, type, &fthmbx, NULL);

        if (ret != 0)
            process_ret(ret, protocol, type, myid);

        printf("#########The %li fth send a message##########\n", arg);

        plat_log_msg(
                PLAT_LOG_ID_INITIAL,
                LOG_CAT,
                PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Msg %d Posted ret %d proto %d type %d Now Sleep on Ack Mbox\n",
                myid, l, ret, protocol, type);

        debug = 0;
        if (debug)
            printf("Node %d: %s STOPPING FOR DEBUG %d\n", myid, __func__, debug);
        while (debug);
        usleep(1);
        aresp = fthMboxWait(&ackmbox[fthlabel]);
        printf("fth %li: after wait for ack \n", arg);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Send Buff loop num %d Ack Freed aresp %ld\n", myid,
                l, aresp);

        if (!fthmbx.actlvl) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: actvl %d\n", myid, fthmbx.actlvl);
            plat_assert(fthmbx.actlvl >= 1);
        } else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: Sleeping on RESP %d  fth mailbox %p loop %d\n",
                    myid, l, &respmbox, l);
            /*
             * Sleep on the mailbox waiting to get a properly directed response message
             */
            msg = (sdf_msg_t *) fthMboxWait(&respmbox[fthlabel]);
            if (msg) {
                printf("$$$$$$ get response $$$$$$\n");
            }
            m = (unsigned char *) msg;

            plat_log_msg(
                    PLAT_LOG_ID_INITIAL,
                    LOG_CAT,
                    PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: RESP %d msg %p seq %lu sn %d dn %d proto %d type %d loop %d\n",
                    myid, l, msg, msg->msg_conversation, msg->msg_src_vnode,
                    msg->msg_dest_vnode, msg->msg_dest_service, msg->msg_type,
                    l);
            /*
             * Print out the buffer contents that we just got
             */
#if 0
            for (i = 0; i < 256; i++) {
                printf(" %02x", *m);
                m++;
                if ((i % 16) == 15) {
                    putchar('\n');
                    fflush(stdout);
                }
            }
#endif
            ret = sdf_msg_free_buff(msg);

            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: Returned Buff %d ret %d\n", myid, l, ret);
            fthYield(1); /* we yield to give others a chance to do some work */
        }
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: EXITING completed sending %d messages - mysync %d\n",
            myid, l, mysync);
    printf("#############fth %li runs out#############\n", arg);
    FTH_SPIN_LOCK(&ssync->spin);
    pthreadcount ++;
    FTH_SPIN_UNLOCK(&ssync->spin);
    while(pthreadcount != 4) fthYield(20);
    printf("the count of scheduler is %d, and will kill all the schedulers.\n", PTHREAD_NUM * FTH_NUM);
    fthKill(4);
    
}

static void fthEvalRoutine(uint64_t arg) {

    struct sdf_msg *eval_msg = NULL;
    struct sdf_msg *send_msg = NULL;
    int i = 0;
    int ret;
    int misordernum = 0;
    vnode_t node;
    serviceid_t protocol = SDF_RESPONSES;
    serviceid_t my_protocol = SDF_RESPONSES;
    msg_type_t type = RESP_ONE;
    sdf_fth_mbx_t fthmbx;
    fthmbx.actlvl = SACK_ONLY_FTH;
    fthmbx.abox = &ackmbx1;
    fthmbx.rbox = NULL;
    fthMboxInit(&ackmbx1);
    uint64_t aresp;

    printf("FTH Thread starting %s\n", __func__);

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
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d Starting evaluate %d\n", myid, i);

    uint64_t startseq = 0, endseq = 0;

    for (;;) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Now Sleeping on RESPONSES queue %p\n", myid,
                q_pair_CONSISTENCY->q_out);
        eval_msg = sdf_msg_receive(q_pair_CONSISTENCY->q_out, 0, B_TRUE);
        uint64_t command = *(uint64_t *) &eval_msg->msg_payload[0];

        if (command != 0x01100110) {
            printf("Wrong msg format %lx\n", command);
            sdf_msg_free_buff(eval_msg);
            continue;
        } else {
            printf("********************get a message********************\n");
        }
        endseq = *(uint64_t *) &eval_msg->msg_payload[8];

        if (startseq > endseq) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nlost order with %li %li", startseq, endseq);
            misordernum++;
        }
        startseq = endseq;
        i++;

        send_msg = (struct sdf_msg *) sdf_msg_alloc(eval_msg->msg_len);
        if (send_msg == NULL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "sdf_msg_alloc(recv_msg->msg_len) failed\n");
        }

        //memcpy(send_msg, eval_msg, eval_msg->msg_len);
        
        struct sdf_resp_mbx rhkey;
        struct sdf_resp_mbx *ptrkey = &rhkey;

        strncpy(rhkey.mkey, MSG_DFLT_KEY, (MSG_KEYSZE - 1));
        rhkey.mkey[MSG_KEYSZE - 1] = '\0';
        rhkey.akrpmbx_from_req = NULL;
        rhkey.rbox = NULL;
        
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Sending MSG dn %d ds %d sn %d ss %d type %d"
                    " akrpmbx %p send_msg %p\n", myid, node, protocol, myid,
                my_protocol, type, eval_msg->akrpmbx, send_msg);

        ret = sdf_msg_send((struct sdf_msg *) send_msg, eval_msg->msg_len,
                node, protocol, myid, my_protocol, type, &fthmbx,
                sdf_msg_get_response(eval_msg, ptrkey));

        if (ret != 0) {
            process_ret(ret, protocol, type, myid);
        }
        aresp = fthMboxWait(&ackmbx1);

        sdf_msg_free_buff(eval_msg);

        printf("@@@@@@@@ Receive msg seqnum is %li @@@@@@@@@ i = %d \n", endseq, i);
        if (i == SHORTTEST * 4)
            break;
    }
    printf("Has %d misorders\n", misordernum);
    FTH_SPIN_LOCK(&ssync->spin);
    pthreadcount ++;
    FTH_SPIN_UNLOCK(&ssync->spin);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE, "\nNode %d: evaluater exiting %d\n", myid, i);

    while(pthreadcount != 1) { 
        fthYield(20);
    }
    printf("the count of scheduler is %d, and will kill all the schedulers.\n", PTHREAD_NUM * FTH_NUM);
    fthKill(1);
    

}

static void *OrderTestSubRoutine(void* arg) {
    printf("before init!***************************************\n");
    uint64_t *subroutineno = (uint64_t *)arg;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d FTH threads firing up - subnum %li\n", myid, *subroutineno);
    printf("\nNode %d FTH threads firing up - subnum %li\n", myid, *subroutineno);

    int i;
    for (i = 0; i < FTH_NUM; i++) {
        fthResume(fthSpawn(&fthThreadSender, 16384), *subroutineno * FTH_NUM + i);
    }
    info->pthread_info = 2;
    info->fth_info = FTH_NUM+1;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
		 "\nNode %d FTH scheduler has initialized - total started %d\n", myid, FTH_NUM);
    printf("\nNode %d FTH scheduler has initialized - total started %d\n", myid, FTH_NUM);
    usleep(500);

    fthSchedulerPthread(0);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nFTH scheduler halted\n");
    return (0);
}

static void *PerfEvalThreadRoutine(void* arg) {
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d %s FTH threads firing up\n", myid, __FUNCTION__);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d FTH scheduler has initialized\n", myid);

    fthResume(fthSpawn(&fthEvalRoutine, 16384), 0);

    fthSchedulerPthread(0);
    
    return (0);
}

void * OrderTestFthPthreadRoutine(void *arg) {
    uint64_t i;
    vnode_t node;
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
            printf("Node %d: %s cluster_node[%li] = %d\n", localrank, __func__, i, cluster_node[i]);
            fflush(stdout);
        }
    }

    printf("before local create myqpairs\n");
    q_pair_CONSISTENCY = local_create_myqpairs(SDF_CONSISTENCY, myid, node);
    info->queue_pair_info->queue_add_pair[0] = q_pair_CONSISTENCY;
    info->queue_pair_info->queue_pair_type = SDF_CONSISTENCY;
    if (q_pair_CONSISTENCY == NULL) {
        fprintf(stderr, "%s: sdf_create_queue_pair failed\n", __func__);
        return NULL;
    }

    q_pair_RESPONSES = local_create_myqpairs(SDF_RESPONSES, myid, node);
    info->queue_pair_info->queue_add_pair[1] = q_pair_RESPONSES;
    if (q_pair_RESPONSES == NULL) {
        fprintf(stderr, "%s: sdf_create_queue_pair failed\n", __func__);
        return NULL;
    }

    sdf_msg_startmsg(myid, 0, NULL);

    if (myid == 0) {
        printf("create pthreads\n");
        pthread_t subtestpthreads[PTHREAD_NUM];
        for (i = 0; i < PTHREAD_NUM; i++) {
            pthread_create(&subtestpthreads[i], NULL, &OrderTestSubRoutine, &i);
        }

        printf("finished creating pthread\n");
        printf("after sleep\n");
        for (i = 0; i < PTHREAD_NUM; i++) {
            pthread_join(subtestpthreads[i], NULL);
        }
    } else {
        pthread_t evalpthread;
        pthread_create(&evalpthread, NULL, &PerfEvalThreadRoutine, NULL);
        pthread_join(evalpthread, NULL);
    }
    return 0;
}
