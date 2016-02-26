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
 * File:   bigsize_fth_test.c
 * Author: Norman Xu
 *
 * Created on February 21, 2008, 11:45 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: bigsize_fth_test.c
 */

#ifdef MPI_BUILD
#include <mpi.h>
#endif

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include "platform/logging.h"

#include "sdfmsg/sdf_msg_types.h"

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
#define SHORTTEST 2
#define UNEXPT_TEST 0
#define FASTPATH_TEST 0

PLAT_LOG_CAT_LOCAL(LOG_CAT, "sdf/sdfmsg/tests/mpilog")
;

/*
 * These fthreads will be used to simulate the SDF protocol sending and
 * receiving messages to the sdf messaging engine and across nodes
 */

struct sdf_queue_pair *q_pair_CONSISTENCY;
struct sdf_queue_pair *q_pair_RESPONSES;

static fthMbox_t ackmbox, respmbox, ackmbx1, ackmbx2;

static int process_ret(int ret_err, int prt, int type);
static int mysync = 0;
typedef struct startsync {
    spinLock_t spin;
} startsync_t;
static struct startsync crp;
static startsync_t *ssync = &crp;

/* 
 * Here we locally create the queue pairs, for now both source and destination protocols
 * will be the same. 
 */

/*struct sdf_queue_pair *
 local_create_myqpairs(service_t protocol, uint32_t myid) {
 struct sdf_queue_pair *q_pair;

 return(q_pair = (sdf_create_queue_pair(myid, myid == 0 ? 1 : 0,
 protocol, protocol, SDF_WAIT_FTH)));
 }*/

/* 
 * This fth thread simulates the Action Node, it sends a CONSISTENCY msg and 
 * sleeps on a mailbox waiting for a response. The msg types are arbitrary for now
 */

static void fthThreadSender(uint64_t arg) {
    int i, l, thecnt= DIVNUM;
    uint64_t aresp;
    struct sdf_msg *send_msg= NULL;
    vnode_t node;
    serviceid_t protocol = SDF_CONSISTENCY;
    serviceid_t my_protocol = SDF_CONSISTENCY;
    msg_type_t type = REQ_FLUSH;
    sdf_fth_mbx_t fthmbx;
    fthMbox_t *fthmbxtst;
    int debug = 0;
    int maxcnt;
    int msgsize;

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

    printf("FTH Thread starting %s Number of msgs to send = %d arg in %d\n",
            __func__, maxcnt, arg);
    fflush(stdout);

    if (DBGP) {
        plat_log_msg (
                PLAT_LOG_ID_INITIAL,
                LOG_CAT,
                PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: fth mb info fthmbxtst %p rbox %p abox %p lvl %d maxcnt %d\n",
                myid, fthmbxtst, fthmbx.rbox, fthmbx.abox, SACK_BOTH_FTH,
                maxcnt);
    }

    /* node is the destination node */

    if (FASTPATH_TEST) {
        node = myid;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
    } else {
        node = myid == 0 ? 1 : 0;
    }

    /* you only init this once but share the q_pairs among the other threads here */

    q_pair_CONSISTENCY = local_create_myqpairs(SDF_CONSISTENCY, myid, node);

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
            myid, q_pair_CONSISTENCY, myid, myid == 0 ? 1 : 0, SDF_CONSISTENCY,
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
    printf("getting spinlock mysync %d\n", mysync);
    fflush(stdout);
    mysync = 1;
    printf("unlock spinlock mysync %d\n", mysync);
    fflush(stdout);
    FTH_SPIN_UNLOCK(&ssync->spin);

    msgsize = TSZE;
    for (l = 0; l < maxcnt; ++l) {
        sdf_msg_t *msg;
        unsigned char *m;
        int ret;

        /* create the buffer on every loop to check the buffer release func */
        if (FIXSIZE) {
            msgsize = BIGSIZE;
        } else {
            msgsize *= 2;
        }
        printf("We send sdfmsg with size %d\n", msgsize);

        /* create the buffer on every loop to check the buffer release func */
        send_msg = (struct sdf_msg *) sdf_msg_alloc(msgsize);

        if (send_msg == NULL) {
            /* FIXME should default to an error  */
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                    "sdf_msg_alloc(TSZE) failed\n");
            /* return ((void *)1); */
        }

        for (i = 0; i < msgsize; ++i)
            send_msg->msg_payload[i] = (unsigned char) i;

        /* 
         * Send 2 CONSISTENCY messages with different types to track if we define SENDTWO
         */

        type = REQ_FLUSH;

        printf("norman: before send\n");

        ret = sdf_msg_send((struct sdf_msg *)send_msg, msgsize, node, protocol,
                myid, my_protocol, type, &fthmbx, NULL);

        printf("norman: after send\n");

        if (ret != 0)
            process_ret(ret, protocol, type);

        plat_log_msg(
                PLAT_LOG_ID_INITIAL,
                LOG_CAT,
                PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Msg %d Posted ret %d proto %d type %d Now Sleep on Ack Mbox\n",
                myid, l, ret, protocol, type);

        debug = 0;
        if (debug)
            printf("Node %d: %s STOPPING FOR DEBUG %d\n", myid, __func__, debug);
        while (debug)
            ;

        aresp = fthMboxWait(&ackmbox);
        printf("norman: after wait for ack \n");
plat_log_msg        (PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
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
            printf("norman: before wait for resp\n");
            msg = (sdf_msg_t *) fthMboxWait(&respmbox);
            printf("norman: after wait for resp\n");
            m = (unsigned char *)msg;

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
#if 1
            for (i = 0; i < 256; i++) {
                printf(" %02x", *m);
                m++;
                if ((i % 16) == 15) {
                    putchar('\n');
                    fflush(stdout);
                }
            }
#endif
            /* release the receive buffer back to the sdf messaging thread */
            ret = sdf_msg_free_buff(msg);

            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: Returned Buff %d ret %d\n", myid, l, ret);

            fthYield(1); /* we yield to give others a chance to do some work */
        }
    }
    printf("fthYield 100 before\n");
    while (mysync != 3)
    fthYield(100);
    printf("fthYield 100 after\n");
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: EXITING completed sending %d messages - mysync %d\n",
            myid, l, mysync);
    fthKill(5); // Kill off FTH
}

/* This FTH thread will act as a protocol worker thread. 
 * It will wait for CONSISTENCY Messages 
 * on the queue, processes them and returns appropriate RESPONSE messages 
 */

static void fthThreadReceiver1(uint64_t arg) {
    int i, ret, ct = 0;
    uint64_t aresp;
    struct sdf_msg *send_msg= NULL, *recv_msg= NULL;
    vnode_t node;
    serviceid_t protocol = SDF_RESPONSES;
    serviceid_t my_protocol = SDF_RESPONSES;
    msg_type_t type = RESP_ONE;
    sdf_fth_mbx_t fthmbx;

    fthmbx.actlvl = SACK_ONLY_FTH;
    fthmbx.abox = &ackmbx1;
    fthmbx.rbox = NULL;
    fthMboxInit(&ackmbx1);

    printf("FTH Thread starting %s\n", __func__);

    if (FASTPATH_TEST) {
        node = myid;
plat_log_msg    (PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
} else {
    node = myid == 0 ? 1 : 0;
}

q_pair_RESPONSES = local_create_myqpairs(SDF_RESPONSES, myid, node);

plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
        "\nNode %d: created queue pair %p sn %d dn %d ss %d ds %d\n", myid,
        q_pair_RESPONSES, myid, myid == 0 ? 1 : 0, SDF_RESPONSES,
        SDF_RESPONSES);
plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
        "\nNode %d: Now yielding waiting for mysync\n", myid);

/* Need to yield till all queues have been created */
while (!mysync)
fthYield(1);

plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
        "\nNode %d: Ready To Accept 1st MESSAGE sack lvl %d\n\n\n", myid,
        fthmbx.actlvl);

for (;;) {

    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Waiting for messages q_pair_CONSISTENCY %p loop%d\n",
            myid, q_pair_CONSISTENCY, ct);
    printf("norman: before receive data in %s\n", __func__);
    recv_msg = sdf_msg_receive(q_pair_CONSISTENCY->q_out, 0, B_TRUE);
    printf("norman: after receive data in %s\n", __func__);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d"
            " akrpmbx %p\n", myid, recv_msg, recv_msg->msg_src_vnode,
            recv_msg->msg_dest_vnode, recv_msg->msg_dest_service,
            recv_msg->msg_type, recv_msg->akrpmbx);

#if 1
    unsigned char *m = (unsigned char *)recv_msg;
    for (i = 0; i < 256; i++) {
        printf(" %02x", *m);
        m++;
        if ((i % 16) == 15) {
            printf("  myid %d", myid);
            putchar('\n');
            fflush(stdout);
        }
    }
#endif

    send_msg = (struct sdf_msg *) sdf_msg_alloc(recv_msg->msg_len);
    if (send_msg == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "sdf_msg_alloc(recv_msg->msg_len) failed\n");
        /* return ((void *)1); */
    }

    //        for (i = 0; i < TSZE; ++i)
    //            send_msg->msg_payload[i] = (unsigned char) 0x69;

    memcpy(send_msg, recv_msg, recv_msg->msg_len);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Sending MSG dn %d ds %d sn %d ss %d type %d"
            " akrpmbx %p send_msg %p\n", myid, node, protocol, myid,
            my_protocol, type, recv_msg->akrpmbx, send_msg);

    ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, protocol,
            myid, my_protocol, type, &fthmbx,
            sdf_msg_get_response_mbx(recv_msg));

    if (ret != 0) {
        process_ret(ret, protocol, type);
    }

    /* release the receive buffer back to the sdf messaging thread */
    ret = sdf_msg_free_buff(recv_msg);

    aresp = fthMboxWait(&ackmbx1);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Send Buff Freed aresp %ld loop %d\n", myid, aresp,
            ct);

    ct++;

    /* Simple exit mechanism, worker threads will just quit when predefined msgcnt 
     * has been reached in the sender thread
     */
}
mysync++;
fthYield(1);
plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
        "\nNode %d: WORKER FTH exiting - loop %d mysync %d\n", myid, i,
        mysync);
}

/* This 2nd FTH thread will act as a protocol worker thread. It will wait for CONSISTENCY Messages 
 * on the queue, processes them and returns appropriate RESPONSE messages 
 */

static void fthThreadReceiver2(uint64_t arg) {
    int i, ret, ct = 0;
    uint64_t aresp;
    struct sdf_msg *send_msg= NULL, *recv_msg= NULL;
    vnode_t node;
    serviceid_t protocol = SDF_RESPONSES;
    serviceid_t my_protocol = SDF_RESPONSES;
    msg_type_t type = RESP_TWO;
    sdf_fth_mbx_t fthmbx;

    fthmbx.actlvl = SACK_ONLY_FTH;
    fthmbx.abox = &ackmbx2;
    fthMboxInit(&ackmbx2);

    printf("FTH Thread starting %s\n", __func__);

    if (FASTPATH_TEST) {
        node = myid;
plat_log_msg    (PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
} else {
    node = myid == 0 ? 1 : 0;
}

plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
        "\nNode %d: Found queue pair %p sn %d dn %d ss %d ds %d loop %d\n",
        myid, q_pair_RESPONSES, myid, (myid == 0 ? 1 : 0), SDF_RESPONSES,
        SDF_RESPONSES, ct);
plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
        "\nNode %d: Now yielding waiting for mysync\n", myid);

/* Need to yield till all queues have been created */
while (!mysync)
fthYield(1);
plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
        "\nNode %d: Ready To Accept 1st MESSAGE sack lvl %d loop %d\n\n\n",
        myid, fthmbx.actlvl, ct);

for (;;) {

    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Waiting for messages q_pair_CONSISTENCY %p loop %d\n",
            myid, q_pair_CONSISTENCY, ct);

    printf("norman: before receive data in %s\n", __func__);
    recv_msg = sdf_msg_receive(q_pair_CONSISTENCY->q_out, 0, B_TRUE);
    printf("norman: after receive data in %s\n", __func__);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d"
            " akrpmbx %p\n", myid, recv_msg, recv_msg->msg_src_vnode,
            recv_msg->msg_dest_vnode, recv_msg->msg_dest_service,
            recv_msg->msg_type, recv_msg->akrpmbx);

#if 1
    unsigned char *m = (unsigned char *)recv_msg;
    for (i = 0; i < 256; i++) {
        printf(" %02x", *m);
        m++;
        if ((i % 16) == 15) {
            printf("  myid %d", myid);
            putchar('\n');
            fflush(stdout);
        }
    }
#endif
    fthYield(1); /* let's give it up here */

    send_msg = (struct sdf_msg *) sdf_msg_alloc(recv_msg->msg_len);
    if (send_msg == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "sdf_msg_alloc(recv_msg->msg_len) failed\n");
        /* return ((void *)1); */
    }

    //        for (i = 0; i < TSZE; ++i)
    //            send_msg->msg_payload[i] = (unsigned char) 0x55;

    memcpy(send_msg, recv_msg, recv_msg->msg_len);

    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Posting reply MSG dn %d ds %d sn %d ss %d type %d loop %d\n",
            myid, node, protocol, myid, my_protocol, type, ct);

    ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, protocol,
            myid, my_protocol, type, &fthmbx,
            sdf_msg_get_response_mbx(recv_msg));
    if (ret != 0)
    process_ret(ret, protocol, type);

    /* release the receive buffer back to the sdf messaging thread */
    ret = sdf_msg_free_buff(recv_msg);

    aresp = fthMboxWait(&ackmbx2);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Send Buff Freed aresp %ld loop %d\n", myid, aresp,
            ct);
    ct++;

    /* Simple exit mechanism, worker threads will just quit when predefined msgcnt 
     * has been reached in the sender thread
     */
}
mysync++;
fthYield(1);
plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
        "\nNode %d: WORKER FTH exiting - loop %d\n mysync %d", myid, i,
        mysync);
}

static void sdf_msg_resp_gc(uint64_t arg) {

    struct sdf_msg *garb_msg= NULL;
    int i = 0;

    printf("FTH Thread starting %s\n", __func__);

plat_log_msg    (PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d Starting garbage resp collector %d\n", myid, i);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Now yielding waiting for mysync\n", myid);
    while (!mysync)
    fthYield(1);
    /* FIXME this was added to take the messages off of the SDF_RESPONSES queue 
     * but it really doesn't matter since the queue will just wrap around
     * the queue
     */
    return;
    for (;;) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Now Sleeping on RESPONSES queue %p\n", myid,
                q_pair_RESPONSES->q_out);
        garb_msg = sdf_msg_receive(q_pair_RESPONSES->q_out, 0, B_TRUE);
        i++;
        fthYield(1);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: got garbage response num %d\n", myid, i);
        if ((SHORTTEST)&&(i == 2))
        break;

        /* FIXME do not release the receive buffer back to the sdf messaging thread  
         * for this loop, it's already being done indirectly by the 
         * int ret = sdf_msg_free_buff(garb_msg);
         */
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: garbage collector exiting %d\n", myid, i);
}

static int process_ret(int ret_err, int prt, int type) {

    for (;;) {
plat_log_msg    (
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Message Sent Error ret %d protocol %d type %d HALTING\n",
            myid, ret_err, prt, type);
}
}

void * BigSizeFthPthreadRoutine(void *arg) {
    fthThread_t *sched;

plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d FTH threads firing up\n", myid);

                            sched = fthInit(); // Init a scheduler

plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d FTH scheduler has initialized\n", myid);

    // Start a thread
                            fthResume(fthSpawn(&fthThreadReceiver1, 16384), 1);
    fthResume(fthSpawn(&fthThreadReceiver2, 16384), 2); // Start a thread
    fthResume(fthSpawn(&sdf_msg_resp_gc, 16384), (uint64_t)myid); // Start response collector
    usleep(500);
    fthResume(fthSpawn(&fthThreadSender, 16384), 0);

    fthStartScheduler(sched);
    fthYield(1); // let them run

plat_log_msg    (PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nFTH scheduler halted\n");
    return (0);

}
