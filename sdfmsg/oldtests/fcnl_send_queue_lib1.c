/*
 * File:   fcnL_send_queue_lib1.c
 * Author: Norman
 *
 * Created on February 21, 2008, 11:45 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: consistency_test.c 308 2008-02-20 22:34:58Z tomr $
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
#define TSZE 2048
#define NANSEC 1.00e-9f
/* this defines the number of loops or messages that will be sent */
#define SHORTTEST 90
#define UNEXPT_TEST 0
#define FASTPATH_TEST 0

PLAT_LOG_CAT_LOCAL(LOG_CAT, "sdf/sdfmsg");

/*
 * These fthreads will be used to simulate the SDF protocol sending and
 * receiving messages to the sdf messaging engine and across nodes
 */

struct sdf_queue_pair *q_pair_CONSISTENCY;
struct sdf_queue_pair *q_pair_RESPONSES;

static fthMbox_t ackmbox, respmbox;
static int cluster_node[MAX_NUM_SCH_NODES];

static int mysync = 0;
static struct startsync crp;
static startsync_t *ssync = &crp;

/* fcnl_send_queue_test1 (library file fcnl_send_queue_lib1.c)
 * - Purpose: test single fth thread posting to queues, size 2k with protocol SDF_CONSISTENCY
 *            and look for a send acknowledge upon post. send buffer is released. There is NO waiting
 *            for a message response. Test return status blasting past the queue buffer limit
 * 
 * - ENHANCEMENTS: use this to test all return funcs of the posting operation. expand this to 
 *                 include all protocols and send buffer release activites. verify that the 
 *                 number of posts translate to messages sent. 
 *                 may want to add sdf_msg_resp_gc or just a queue cleanup
 * 
 * main testing func == void * SendQueuePthreadRoutine(void *arg);
 * other related threads == fthThreadSender, 
 * 
 */

static void fthThreadSender(uint64_t arg) {
    int i, l;
    struct sdf_msg *send_msg = NULL;
    vnode_t node;
    serviceid_t protocol = SDF_CONSISTENCY;
    serviceid_t my_protocol = SDF_CONSISTENCY;
    msg_type_t type = REQ_FLUSH;
    sdf_fth_mbx_t fthmbx;
    fthMbox_t *fthmbxtst;
    int debug = 0;
    int maxcnt, ret;

#if SENDTWO
    maxcnt = 5;
#elif SHORTTEST
    maxcnt = SHORTTEST;
#else
    maxcnt = 990000;
#endif

    fthmbxtst = &respmbox;

    fthMboxInit(&ackmbox);
    fthMboxInit(&respmbox);

    fthmbx.actlvl = SACK_BOTH_FTH;
    fthmbx.abox = &ackmbox;
    fthmbx.rbox = &respmbox;

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

    /* node is the destination node */
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

    /* you only init this once but share the q_pairs among the other threads here */

    q_pair_CONSISTENCY = local_create_myqpairs(SDF_CONSISTENCY, myid, node);
    info->queue_pair_info->queue_add[0] = q_pair_CONSISTENCY->q_in;
    info->queue_pair_info->queue_add[1] = q_pair_CONSISTENCY->q_out;
    info->queue_pair_info->queue_pair_type = SDF_CONSISTENCY;
    if (q_pair_CONSISTENCY == NULL) {
        fprintf(stderr, "%s: sdf_create_queue_pair failed\n", __func__);
        return;
    }
    /* right now we are not using shmem for these buffers */

    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: created queue pair %p sn %d dn %d ss %d ds %d maxcnt %d\n",
            myid, q_pair_CONSISTENCY, myid, node, SDF_CONSISTENCY,
            SDF_CONSISTENCY, maxcnt);

    /* main loop will send SDF_CONSISTENCY protocol messages till maxcnt is reached
     * this sleeps on both mailboxes ack and resp based on the lvl dictated
     */
    if (myid == 1) {
        debug = 0;
        if (debug) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: DEBUG --- NOT SENDING MESSAGES FROM HERE", myid);
            while (debug)
                fthYield(100); /* Don't send mesages from node one for now */
        }
    }
    FTH_SPIN_LOCK(&ssync->spin);
    mysync = 1;
    FTH_SPIN_UNLOCK(&ssync->spin);

    // start the message engine
    sdf_msg_startmsg(myid, 0, NULL);

    for (l = 0; l < maxcnt; ++l) {
        printf("A new %d round\n", l);
        /* create the buffer on every loop to check the buffer release func */
        if (UNEXPT_TEST) {
            send_msg = (struct sdf_msg *) sdf_msg_alloc(8192);
            printf("Node %d: %s BIG Message Alloc %li\n", myid, __func__,
                    sizeof((struct sdf_msg *) send_msg));
        } else {
            send_msg = (struct sdf_msg *) sdf_msg_alloc(TSZE);
        }
        if (send_msg == NULL) {
            /* FIXME should default to an error  */
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                    "sdf_msg_alloc(TSZE) failed\n");
            /* return ((void *)1); */
        }

        for (i = 0; i < TSZE; ++i)
            send_msg->msg_payload[i] = (unsigned char) i;

        /*
         * Send 2 CONSISTENCY messages with different types to track if we define SENDTWO
         */

        type = REQ_FLUSH;

        if (UNEXPT_TEST) {
            ret = sdf_msg_send((struct sdf_msg *) send_msg, 8192, node,
                    protocol, myid, my_protocol, type, &fthmbx, NULL);
        } else {
            ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node,
                    protocol, myid, my_protocol, type, &fthmbx, NULL);
        }
        while (ret != 0) {
            /* just print the alerts wrt the queues */
            ret = process_ret(ret, protocol, type, myid);
	    if (ret == QUEUE_NOQUEUE) {break;}
            ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node,
                    protocol, myid, my_protocol, type, &fthmbx, NULL);
	}
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Msg %d Posted ret %d proto %d type %d loop %d\n",
                     myid, l, ret, protocol, type, l);
    }


    /* at the end here we just do a simple test to fail a post, should print the error message
     * and we just exit this test */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: SEND QUEUE TEST - Posting to a non existent queue ds %d loop %d\n",
                 myid, SDF_METADATA, l);
    my_protocol = SDF_METADATA; /* set a bogus protocol that has no associated queue */
    ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node,
                    protocol, myid, my_protocol, type, &fthmbx, NULL);
    while (ret != 0) {
        /* just print the alerts wrt the queues */
        ret = process_ret(ret, protocol, type, myid);
        if (ret == 2) {break;}
        ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node,
                            protocol, myid, my_protocol, type, &fthmbx, NULL);
	}

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Done with SEND QUEUE TEST complete total msgs posted %d\n",
                     myid, l);
    mysync++;
    sleep(1);
    fthYield(100);
    fthKill(5); // Kill off FTH
}

void * SendQueuePthreadRoutine(void *arg) {

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d FTH threads firing up\n", myid);

    fthInit(); // Init a scheduler

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d FTH scheduler has initialized\n", myid);

    // Start a thread
    usleep(500);
    fthResume(fthSpawn(&fthThreadSender, 16384), 0);

    fthSchedulerPthread(0);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nFTH scheduler halted\n");

    return (0);

}
