/*
 * File:   sdf_msg_protocol.c
 * Author: Tom Riddle
 *
 * Created on February 21, 2008, 11:45 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_protocol.c,v 1.1 2008/05/22 09:48:07 drew Exp drew $
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
#include "platform/errno.h"

#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_types.h"

extern uint32_t myid;

#define DBGP 0
#define TSZE 2048
#define BIGTZSZE 512000
#define NANSEC 1.00e-9f

/* use a multiple of 2 to have this test exit properly */
#define SHORTTEST 10
#define UNEXPT_TEST 0
#define ALT_SIZES 0
#define FASTPATH_TEST 0
#define PRNT_TS 0
#define ALLVALS 100
#define SHOWTS 1
#define SHOWTS1 1
#define SHOWPL 1
#define SHOWRT 1
#define SHOWACKBOXTS 0
#define PRINTMSGHDR 0
#define FORCETIMEOUT 0 /* set this to the timeout value you want and it will force the test */

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG 

/*
 * These fthreads will be used to simulate the SDF protocol sending and
 * receiving messages to the sdf messaging engine and across nodes
 */

struct sdf_queue_pair *q_pair_CONSISTENCY;
struct sdf_queue_pair *q_pair_RESPONSES;

struct sdf_queue_pair *q_pair_CONSISTENCY_3rd;
struct sdf_queue_pair *q_pair_RESPONSES_3rd;

static int process_ret(int ret_err, int prt, int type);

/* play around with the spin locks here for test */
static int mysync = 0;
typedef struct startsync {
    fthSpinLock_t spin;
} startsync_t;
static int cluster_node[MAX_NUM_SCH_NODES];
static struct startsync crp;
static startsync_t *ssync = &crp;
static uint64_t thousand_array[20];
static uint64_t thousand_array1[20];
static char msgseq[8][128];
static char wkrseq1[8][128];
static char wkrseq2[8][128];
static uint64_t tsttm_array[20];
static uint64_t wt1_array[20];
static uint64_t wt2_array[20];
static int maxcnt = 10;

/*
 * Here we create the queue pairs, for now both source and destination protocols
 * will be the same.
 */

static struct sdf_queue_pair *
local_create_myqpairs(service_t protocol, uint32_t myid, uint32_t pnode) {
    struct sdf_queue_pair *q_pair;

    if (myid == pnode) {
        return (q_pair = (sdf_create_queue_pair(myid, pnode, protocol,
                                                protocol, SDF_WAIT_FTH)));
    } else {
        return (q_pair = (sdf_create_queue_pair(myid, myid == 0 ? 1 : 0, protocol,
                                                protocol, SDF_WAIT_FTH)));
    }
}

/*
 * This fth thread simulates the Action Node, it sends a CONSISTENCY msg and
 * sleeps on a mailbox waiting for a response. The msg types are arbitrary for now
 */


void
fthThreadRoutine(uint64_t arg)
{
    int i, l;
    uint64_t aresp;
    struct sdf_msg *send_msg = NULL;
    vnode_t node = 0;
    serviceid_t protocol = SDF_CONSISTENCY;
    serviceid_t my_protocol = SDF_CONSISTENCY;
    msg_type_t type = REQ_FLUSH;
    sdf_fth_mbx_t fthmbx;
    fthMbox_t *fthmbxtst;
    int debug = 0;
    int mcnt = 0;
    fthMbox_t ackmbox, respmbox;

    uint64_t tms_new, tms3, tms_old = get_the_nstimestamp();
    uint64_t thousand_st, tms_msgtm, msg_avg_short = 0, msg_avg_time = 0;
    uint64_t tms_msgshorttm, msg_avg_shorttm = 0;
    uint64_t mavg[20];
    int numofmillions = 0;


#if SHORTTEST
    maxcnt = (int) arg;
#else
    maxcnt = 999999;
#endif

    fthmbxtst = &respmbox;

    fthMboxInit(&ackmbox);
    fthMboxInit(&respmbox);

    fthmbx.actlvl = SACK_BOTH_FTH;
    fthmbx.abox = &ackmbox;
    fthmbx.rbox = &respmbox;
    fthmbx.aaction = NULL;
    fthmbx.raction = NULL;
    fthmbx.release_on_send = 0;


    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nFTH Thread starting %s Number of msgs to send = %d arg in %lu\n", __func__, maxcnt, arg);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: fth mb info fthmbxtst %p rbox %p abox %p lvl %d maxcnt %d\n",
                     myid, fthmbxtst, fthmbx.rbox, fthmbx.abox, SACK_BOTH_FTH, maxcnt);

    /*
     * cluster_node is an array of destination nodes, localrank is this node
     * localpn is the bit field of active nodes in the cluster
     * numprocs are the total processes started by mpi
     * actmask is a bit field representation of numprocs
     */
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
    } else {
        int tmp = 1;
        tmp = tmp << localrank;
        tmp = tmp ^ localpn;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: %s eligible node mask 0x%x\n", localrank, __func__, tmp);
        /* just send to the 1st eligible node */
        for (i = 0; i < numprocs; i++) {
            if ((tmp >> i)&1) {
                node = i;
                break;
            }
        }
        for (i = 0; i < numprocs; i++) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: %s cluster_node[%d] = %d my pnode %d\n", 
                         localrank, __func__, i, cluster_node[i], node);
        }
    }

    /* you only init this once but share the q_pairs among the other threads here */

    q_pair_CONSISTENCY = local_create_myqpairs(SDF_CONSISTENCY, myid, node);

    if (q_pair_CONSISTENCY == NULL) {
        fprintf(stderr, "%s: sdf_create_queue_pair failed\n", __func__);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: EXITING queue was null - mysync %d\n",
                     myid, mysync);
//        fthKill(5);                              // Kill off FTH
        return;
    }
    /* right now we are not using shmem for these buffers */

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: created queue pair %p sn %d dn %d ss %d ds %d maxcnt %d\n",
                 myid, q_pair_CONSISTENCY, myid, node,
                 SDF_CONSISTENCY, SDF_CONSISTENCY, maxcnt);

#if 0

    if (myid == 1) {
        sdf_msg_barrier(myid, SDF_MSG_BARWAIT);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Going into barrier release\n",
                 myid);
        usleep(2000);
        sdf_msg_barrier(myid, SDF_MSG_BARREL);
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Barrier with slave wait done\n", myid);

    if (myid == 0) {
        sdf_msg_barrier(myid, SDF_MSG_BARCIC);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Going into slave barrier release\n",
                 myid);
        usleep(2000);
        sdf_msg_barrier(myid, SDF_MSG_BARCIW);
    }


    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Barrier's ALL DONE\n\n\n\n\n", myid);
#endif

    
    FTH_SPIN_LOCK(&ssync->spin);
    mysync = 1;
    FTH_SPIN_UNLOCK(&ssync->spin);

    /* just set debug = 1 to only send a test message from Node 0, block Node 1 from sending */
    if (myid == 1) {
        debug = 0;
        if (debug) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: DEBUG --- NOT SENDING MESSAGES FROM HERE\n", myid);
            printf("\nNode %d: DEBUG --- NOT SENDING MESSAGES FROM HERE\n", myid);
            fflush(stdout);
            while (debug) fthYield(100); /* Don't send messages from node one for now */
        }
    }

//    send_msg = (struct sdf_msg *) sdf_msg_alloc(TSZE);

    strcpy(msgseq[1], "Time to alloc send_msg");
    strcpy(msgseq[2], "Time to init send_msg buff");
    strcpy(msgseq[3], "Time to post send_msg buff");
    strcpy(msgseq[4], "Time for ack MboxWait");
    strcpy(msgseq[5], "Time for RESP MboxWait");
    strcpy(msgseq[6], "Time to free recv buff");

    sdf_msg_t *msgtst = (struct sdf_msg *)sdf_msg_alloc(0);
    msgtst->msg_q_item = NULL;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nmyid test hdr size %li\n",  ((char *)msgtst->msg_payload - (char *)msgtst));
    sdf_msg_free(msgtst);

    /*
     * main loop will send SDF_CONSISTENCY protocol messages till maxcnt is reached
     * this sleeps on both mailboxes ack and resp based on the lvl dictated
     */
    for (l = 0; l < maxcnt; ++l) {
        sdf_msg_t *msg;
        unsigned char *m;
        int ret, sendsze;

        tms_old = get_the_nstimestamp();
        thousand_st = tms_old;

        /* create the buffer on every loop to check the buffer release func */
        if (UNEXPT_TEST) {
            if (ALT_SIZES && (l%2)) {
                send_msg = (struct sdf_msg *)sdf_msg_alloc(BIGTZSZE);
                sendsze = BIGTZSZE;
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: BIG Message Alloc %li send_msg %p\n", myid,
                             ((char *)send_msg->msg_payload - (char *)send_msg) + BIGTZSZE, send_msg);
            } else {
                send_msg = (struct sdf_msg *)sdf_msg_alloc(TSZE);
                sendsze = TSZE;
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: %s BIG but SMALL Message Alloc %li send_msg %p\n", myid, __func__,
                             ((char *)send_msg->msg_payload - (char *)send_msg) + TSZE, send_msg);
            }
        } else {
            send_msg = (struct sdf_msg *)sdf_msg_alloc(TSZE);
            sendsze = TSZE;
        }

        if (send_msg == NULL) {
            /* FIXME should default to an error  */
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "sdf_msg_alloc(TSZE) failed\n");
            /* return ((void *)1); */
        }
        if (SHOWTS) {
            tms_old = show_howlong(tms_old, 1, tsttm_array);
        }

        /* init msg payload, could use memset */
        for (i = 1; i < TSZE; ++i) {
            send_msg->msg_payload[i] = (unsigned char) i;
        }

        /*
         * Send 1 CONSISTENCY message
         */

        type = REQ_FLUSH;

        if (SHOWTS) {
            tms_old = show_howlong(tms_old, 2, tsttm_array);
        }
        /* here is the b4 send marker for thousand_array index 3 */
        tms_msgtm = get_the_nstimestamp();

        struct sdf_msg *temp_msg = send_msg;

        /* NEW_MSG test -- hardware node = 1 for send test */
//        node = 1;

        ret = sdf_msg_send((struct sdf_msg *)send_msg, sendsze, node, protocol,
                           myid, my_protocol, type, &fthmbx, NULL);

        if (SHOWPL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Msg %p to be posted ret %d proto %d type %d sleep on ackmbox %p respmbox %p\n",
                         myid, temp_msg, ret, protocol, type, fthmbx.abox, fthmbx.rbox);
        }

        if (ret != 0) {
            process_ret(ret, protocol, type);
        }
        if (SHOWTS) {
            tms_old = show_howlong(tms_old, 3, tsttm_array);
        }
        
        if (SHOWACKBOXTS) {
            tms3 = tms_old;
        }

        aresp = fthMboxWait(&ackmbox);
        tms_msgshorttm = show_howlong(tms_msgtm, 3, thousand_array); /* end marker send ack thousand_array indx 3 */

        if (SHOWPL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Send Buff loop num %d Ack Freed aresp %ld\n", myid, l, aresp);
        }

        if (SHOWACKBOXTS) {
            uint64_t tms1 = get_the_nstimestamp();
            uint64_t tms2;
            if (tms1 < aresp) {
                tms2 = (1000000000 + tms1) - aresp;
            } else {
                tms2 = tms1 - aresp;
            }
            printf("\nNode %d: loop %d ACK MBOX TEST- tsb4 %lu rawts %lu usec ackval %li nsec diff %lu usec\n",
                   myid, l, (tms3/1000), (tms1/1000), aresp, tms2);
            fflush(stdout);
        }

        if (SHOWTS) {
            tms_old = show_howlong(tms_old, 4, tsttm_array);
        }

        if (!fthmbx.actlvl) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: actvl %d\n", myid, fthmbx.actlvl);
            plat_assert_always(fthmbx.actlvl >= 1);
        } else {
            if (SHOWPL) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: Sleeping on RESP %d  fth mailbox %p loop %d\n",
                             myid, l, &respmbox, l);
            }
            /*
             * Sleep on the mailbox waiting to get a properly directed response message
             */

            msg = (sdf_msg_t *)fthMboxWait(&respmbox);
            if (msg->msg_type == SDF_MSG_ERROR) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "\nNode %d: MESSAGE TIMEOUT RESP %d msg %p seq %lu sn %d dn %d proto %d type %d loop %d\n",
                             myid, l, msg, msg->msg_conversation,
                             msg->msg_src_vnode, msg->msg_dest_vnode,
                             msg->msg_dest_service, msg->msg_type, l);
            }
            m = (unsigned char *)msg;

            tms_msgtm = show_howlong(tms_msgtm, 2, thousand_array); /* full rt end marker */

            if (SHOWTS) {
                tms_old = show_howlong(tms_old, 5, tsttm_array);
            }

            if (SHOWPL) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: RESP %d msg %p seq %lu sn %d dn %d proto %d type %d loop %d\n",
                             myid, l, msg, msg->msg_conversation,
                             msg->msg_src_vnode, msg->msg_dest_vnode,
                             msg->msg_dest_service, msg->msg_type, l);
            }

            /*
             * Print out the buffer contents that we just got
             */
#if PRINTMSGHDR
            for (i = 0; i < 128; i++) {
                printf(" %02x", *m);
                fflush(stdout);
                m++;
                if ((i % 16) == 15) {
                    printf("  myid %d %s\n", myid, __func__);
                    fflush(stdout);
                }
            }
#endif
            /* release the receive buffer back to the sdf messaging thread */

            if (SHOWPL) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: Call to free resp msg %p msg_flags 0x%x msg_type %d loop %d ret %d\n",
                             myid, msg, msg->msg_flags, msg->msg_type, l, ret);
            }

            ret = sdf_msg_free_buff(msg);

            if (SHOWTS) {
                tms_old = show_howlong(tms_old, 6, tsttm_array);
            }


            if (SHOWRT) {
                /* total RT loop time in thousand_array index 1 */
                tms_new = show_howlong(thousand_st, 1, thousand_array);
                if (0) {
                    printf("\nNode %d: TIMESTAMP new %lu usec old %lu diff %lu -- loop %d\n", myid,
                           tms_new/1000, thousand_st/1000, thousand_array[1]/1000, l);
                    fflush(stdout);
                }
                msg_avg_time = msg_avg_time + thousand_array[1]/1000; /* total loop time */
                msg_avg_short = msg_avg_short + thousand_array[2]/1000; /* time to get resp from b4 send */
                msg_avg_shorttm = msg_avg_shorttm + thousand_array[3]/1000; /* time to do send/ack b4 send */
                mcnt++;
                for (i = 1; i < 7; i++) {
                    mavg[i] = mavg[i] + tsttm_array[i];
                }
                if (mcnt == 1000) {
                    printf("\nNode %d: size %d RT AVG %f usec %d msgs resp %f usec send %f usec -- loop %d\n",
                           myid, TSZE, (double)msg_avg_time/mcnt, mcnt,
                           (double)msg_avg_short/mcnt, (double)msg_avg_shorttm/mcnt, l);
                    fflush(stdout);
                    for (i = 1; i < 7; i++) {
                        printf("Node %d: %s %f usec\n", myid, msgseq[i], ((double)mavg[i]/mcnt)/mcnt);
                        fflush(stdout);
                        mavg[0] =  mavg[0]+ mavg[i]/mcnt;
                        mavg[i] = 0;
                    }
                    printf("Node %d: Additive Time %f usec\n", myid, (double)mavg[0]/mcnt);
                    fflush(stdout);
                    mavg[0] = 0;
                    mcnt = 0;
                    msg_avg_time = 0;
                    msg_avg_short = 0;
                    msg_avg_shorttm = 0;
                }
            }

            fthYield(1); /* we yield to give others a chance to do some work */

#if PRNT_TS
            for (i = 1; i < 8; i++) {
                if (tsttm_array[i] > ALLVALS) {
                    printf("\nNode %d: %s loop %d Seq %d %s - ELAPSED TIME %li usec\n",
                           myid, __func__, l, i, msgseq[i], (tsttm_array[i]/1000));
                    fflush(stdout);
                }
                tsttm_array[i] = 0;
            }
#endif

            if (l == 999000) {
                numofmillions++;
                l = 0;
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: Sent & Received %d million messages\n", myid, numofmillions);
                if (1) {
                    printf("\nNode %d: Sent & Received %d million messages\n", myid, numofmillions);
                    fflush(stdout);
                }
            }
        }
    }
    while (mysync != 3) {
        fthYield(1);
    }
    fthMboxTerm(&ackmbox);
    fthMboxTerm(&respmbox);
#if 1
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: EXITING completed sending %d messages - mysync %d\n",
                 myid, l, mysync);
    printf("\nNode %d: %s EXITING completed sending %d messages\n",
           myid, __func__, l);
    fflush(stdout);
#endif

    fthKill(1);                              // Kill off FTH
}

/*
 * This FTH thread will act as a protocol worker thread.
 * It will wait for CONSISTENCY Messages
 * on the queue, processes them and returns appropriate RESPONSE messages
 */

void fthThreadRoutine1(uint64_t arg) {
    int ret, ct = 0, mcnt1 = 0;
    uint64_t aresp;
    struct sdf_msg *send_msg = NULL, *recv_msg = NULL;
    int node = 0;
    serviceid_t protocol = SDF_RESPONSES;
    serviceid_t my_protocol = SDF_RESPONSES;
    msg_type_t type = RESP_ONE;
    sdf_fth_mbx_t fthmbx;
    fthMbox_t ackmbx1;
    uint64_t tms_new1, tms_old1 = get_the_nstimestamp();
    uint64_t thousand_st1, tms_msgtm1, msg_avg_short1 = 0, msg_avg_time1 = 0;
    uint64_t tms_msgshorttm1 = 0, msg_avg_shorttm1 = 0;

    fthmbx.actlvl = SACK_ONLY_FTH;
    fthmbx.abox = &ackmbx1;
    fthmbx.rbox = NULL;
    fthMboxInit(&ackmbx1);
    fthmbx.aaction = NULL;
    fthmbx.raction = NULL;
    fthmbx.release_on_send = 0;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: FTH Worker Thread %s starting\n", myid, __func__);

    int localpn, actmask;
    uint32_t numprocs;
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    if (localrank == localpn) {
        node = localpn;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
    } else {
        int tmp = 1;
        tmp = tmp << localrank;
        tmp = tmp ^ localpn;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: %s eligible node mask 0x%x\n", localrank, __func__, tmp);
        /* just send to the 1st eligible node */
        for (int i = 0; i < numprocs; i++) {
            if ((tmp >> i)&1) {
                node = i;
                break;
            }
        }
        for (int i = 0; i < numprocs; i++) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: %s cluster_node[%d] = %d my pnode %d\n", 
                         localrank, __func__, i, cluster_node[i], node);
        }
    }

    q_pair_RESPONSES = local_create_myqpairs(SDF_RESPONSES, myid, node);

    if (q_pair_RESPONSES != NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: created queue pair %p sn %d dn %d ss %d ds %d\n",
                     myid, q_pair_RESPONSES, myid, myid == 0 ? 1 : 0,
                     SDF_RESPONSES, SDF_RESPONSES);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Now yielding waiting for mysync\n", myid);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: FAILED to create queue pair %p sn %d dn %d ss %d ds %d\n",
                     myid, q_pair_RESPONSES, myid, myid == 0 ? 1 : 0,
                     SDF_RESPONSES, SDF_RESPONSES);
        return;

    }
    /* Need to yield till all queues have been created */
    while (!mysync)    fthYield(1);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Ready To Accept 1st MESSAGE sack lvl %d\n\n\n",
                 myid, fthmbx.actlvl);

    strcpy(wkrseq1[1], "WK1: Time before sleeping on recv queue");
    strcpy(wkrseq1[2], "WK1: Time after getting recv_msg");
    strcpy(wkrseq1[3], "WK1: Time to alloc send_msg & cpy in recv_msg");
    strcpy(wkrseq1[4], "WK1: Time to send_msg");
    strcpy(wkrseq1[5], "WK1: Time for ack MBoxWait");
    strcpy(wkrseq1[6], "WK1: Time to free recv_msg buff");

    for (;;) {

        tms_old1 = get_the_nstimestamp();
        thousand_st1 = tms_old1;

        if (SHOWPL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Waiting for messages q_pair_CONSISTENCY %p loop %d\n",
                         myid, q_pair_CONSISTENCY, ct);
        }

        if (SHOWTS1) {
            tms_old1 = show_howlong(tms_old1, 1, wt1_array);
        }

        tms_msgtm1 = get_the_nstimestamp();

        recv_msg = sdf_msg_receive(q_pair_CONSISTENCY->q_out, 0, B_TRUE);

        tms_msgshorttm1 = show_howlong(tms_msgtm1, 2, thousand_array1); /* Receive marker */

        if (SHOWTS1) {
            tms_old1 = show_howlong(tms_old1, 2, wt1_array);
        }
        if (SHOWPL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d len %d"
                         " akrpmbx %p\n", myid,
                         recv_msg, recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode,
                         recv_msg->msg_dest_service, recv_msg->msg_type, recv_msg->msg_len,
                         recv_msg->akrpmbx);
        }

#if PRINTMSGHDR
        unsigned char *m = (unsigned char *)recv_msg;
        for (int i = 0; i < 128; i++) {
            printf(" %02x", *m);
            fflush(stdout);
            m++;
            if ((i % 16) == 15) {
                printf("  myid %d %s\n", myid, __func__);
                fflush(stdout);
            }
        }
#endif

        uint32_t mlen = recv_msg->msg_len;

        send_msg = (struct sdf_msg *)sdf_msg_alloc(mlen);

        if (send_msg == NULL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "sdf_msg_alloc(recv_msg->msg_len) failed\n");
            /* return ((void *)1); */
        }

        for (int i = 0; i < TSZE; ++i)
           send_msg->msg_payload[i] = (unsigned char) 0x69;

        /* here we should grab the needed info, hkey, rbox for the response */
        struct sdf_resp_mbx rhkey;
        struct sdf_resp_mbx *ptrkey = sdf_msg_initmresp(&rhkey);

/*  we used to just reflect back the message payload   
 *  memcpy(send_msg->msg_payload, recv_msg->msg_payload, sdf_msg_get_payloadsze(struct sdf_msg *msg));
 *  
 */

        if (SHOWTS1) {
            tms_old1 = show_howlong(tms_old1, 3, wt1_array);
        }

        if (SHOWPL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Sending MSG dn %d ds %d sn %d ss %d type %d"
                         " akrpmbx %p send_msg %p\n",
                         myid, node, protocol, myid, my_protocol, type,
                         recv_msg->akrpmbx, send_msg);
        }
#if PRINTMSGHDR
        unsigned char *n = (unsigned char *)send_msg;
        for (int i = 0; i < 128; i++) {
            printf(" %02x", *n);
            fflush(stdout);
            n++;
            if ((i % 16) == 15) {
                printf("  myid %d %s\n", myid, __func__);
                fflush(stdout);
            }
        }
#endif
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: RESPONSE INFO MSG %p dflt mkey %s rbox %p\n", myid, recv_msg,
                     ptrkey->mkey, ptrkey->rbox);

        if (UNEXPT_TEST) {
            ret = sdf_msg_send((struct sdf_msg *)send_msg, mlen, node, protocol,
                               myid, my_protocol, type, &fthmbx,
                               sdf_msg_get_response(recv_msg, ptrkey));
        } else {
            ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, protocol,
                               myid, my_protocol, type, &fthmbx,
                               sdf_msg_get_response(recv_msg, ptrkey));
        }
        tms_msgshorttm1 = show_howlong(tms_msgshorttm1, 3, thousand_array1); /* Send marker */

        if (ret != 0) {
            process_ret(ret, protocol, type);
        }
        if (SHOWTS1) {
            tms_old1 = show_howlong(tms_old1, 4, wt1_array);
        }

        aresp = fthMboxWait(&ackmbx1);

        if (SHOWTS1) {
            tms_old1 = show_howlong(tms_old1, 5, wt1_array);
        }

        /* release the receive buffer back to the sdf messaging thread */
        ret = sdf_msg_free_buff(recv_msg);

        if (SHOWPL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Send Buff Freed aresp %ld loop %d\n",
                         myid, aresp, ct);
        }
        if (SHOWTS1) {
            tms_old1 = show_howlong(tms_old1, 6, wt1_array);
        }
        if (SHOWTS1) {
            if (0) {
                tms_new1 = show_howlong(tms_old1, 1, wt1_array);
                printf("\nNode %d: %s TIMESTAMP new %li usec old %li diff %li loop %d\n", myid,
                       __func__, tms_new1/1000, tms_old1/1000, wt1_array[1]/1000, ct);
                fflush(stdout);
            }

            if (1) {
                tms_new1 = show_howlong(thousand_st1, 1, thousand_array1);
                if (0) {
                    printf("\nNode %d: RECV TIMESTAMP new %li usec old %li diff %li -- loop %d\n", myid,
                           tms_new1/1000, thousand_st1/1000, thousand_array1[1]/1000, ct);
                    fflush(stdout);
                }
                msg_avg_time1 = msg_avg_time1 + thousand_array1[1]/1000;
                msg_avg_short1 = msg_avg_short1 + thousand_array1[2]/1000;
                msg_avg_shorttm1 = msg_avg_shorttm1 + thousand_array1[3]/1000;
                mcnt1++;
                if (mcnt1 == 1000) {
                    printf("\nNode %d: size %d WKR RT AVG %f usec %d msgs short %f usec send %f usec -- loop %d\n",
                           myid, TSZE, (double)msg_avg_time1/mcnt1, mcnt1, (double)msg_avg_short1/mcnt1,
                           (double)msg_avg_shorttm1/mcnt1, ct);
                    fflush(stdout);
                    mcnt1 = 0;
                    msg_avg_time1 = 0;
                    msg_avg_short1 = 0;
                    msg_avg_shorttm1 = 0;
                }
            }
        }
#if PRNT_TS
        for (i = 0; i < 8; i++) {
            if (wt1_array[i] > ALLVALS) {
                printf("\nNode %d: %s loop %d Seq %d %s - ELAPSED TIME %li usec\n",
                       myid, __func__, ct, i, wkrseq1[i], (wt1_array[i]/1000));
                fflush(stdout);
            }
            wt1_array[i] = 0;
        }
#endif
        ct++;
        if (SHORTTEST) {
            if (ct == ( maxcnt / 2)) {
                mysync++;
                break;
            }
        }
        /*
         * Simple exit mechanism, worker threads will just quit when predefined msgcnt
         * has been reached in the sender thread
         */
        fthYield(1); /* we yield to give others a chance to do some work */
    }
    fthMboxTerm(&ackmbx1);
#if 1
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: WORKER FTH exiting - loop %d mysync %d\n", myid, ct, mysync);
    printf("\nNode %d: %s EXITING completed sending %d messages\n",
           myid, __func__, ct);
    fflush(stdout);
#endif
    fthYield(1); /* we yield to give others a chance to do some work */
}

/*
 * This 2nd FTH thread will act as a protocol worker thread. It will wait for CONSISTENCY Messages
 * on the queue, processes them and returns appropriate RESPONSE messages
 */

void fthThreadRoutine2(uint64_t arg) {
    int ret, ct = 0;
    uint64_t aresp;
    struct sdf_msg *send_msg = NULL, *recv_msg = NULL;
    vnode_t node = 0;
    serviceid_t protocol = SDF_RESPONSES;
    serviceid_t my_protocol = SDF_RESPONSES;
    msg_type_t type = RESP_TWO;
    fthMbox_t ackmbx2;
    sdf_fth_mbx_t fthmbx;
    uint64_t tms_new, tms_old = get_the_nstimestamp();

    fthmbx.actlvl = SACK_ONLY_FTH;
    fthmbx.abox = &ackmbx2;
    fthmbx.rbox = NULL;
    fthMboxInit(&ackmbx2);
    fthmbx.aaction = NULL;
    fthmbx.raction = NULL;
    fthmbx.release_on_send = 0;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: FTH Worker Thread %s starting\n", myid, __func__);

    int localpn, actmask;
    uint32_t numprocs;
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    if (localrank == localpn) {
        node = localpn;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
    } else {
        int tmp = 1;
        tmp = tmp << localrank;
        tmp = tmp ^ localpn;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: %s eligible node mask 0x%x\n", localrank, __func__, tmp);
        /* just send to the 1st eligible node */
        for (int i = 0; i < numprocs; i++) {
            if ((tmp >> i)&1) {
                node = i;
                break;
            }
        }
        for (int i = 0; i < numprocs; i++) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: %s cluster_node[%d] = %d my pnode %d\n", 
                         localrank, __func__, i, cluster_node[i], node);
        }
    }

    /* Need to yield till all queues have been created */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Now yielding waiting for mysync\n", myid);
    while (!mysync)    fthYield(1);

    if (q_pair_RESPONSES != NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Found queue pair %p sn %d dn %d ss %d ds %d loop %d\n",
                     myid, q_pair_RESPONSES, myid, (myid == 0 ? 1 : 0),
                     SDF_RESPONSES, SDF_RESPONSES, ct);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Ready To Accept 1st MESSAGE sack lvl %d loop %d\n\n\n",
                     myid, fthmbx.actlvl, ct);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: FAILED to find queue pair %p sn %d dn %d ss %d ds %d loop %d\n",
                     myid, q_pair_RESPONSES, myid, (myid == 0 ? 1 : 0),
                     SDF_RESPONSES, SDF_RESPONSES, ct);
        return;
    }
    for (;;) {
        tms_old = get_the_nstimestamp();

        if (SHOWPL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Waiting for messages q_pair_CONSISTENCY %p loop %d\n",
                         myid, q_pair_CONSISTENCY, ct);
        }
        if (SHOWTS1) {
            tms_old = show_howlong(tms_old, 1, wt2_array);
            strcpy(wkrseq2[1], "WK2: Time before sleeping on recv queue");
        }

        recv_msg = sdf_msg_receive(q_pair_CONSISTENCY->q_out, 0, B_TRUE);

#if FORCETIMEOUT
/* crude way to force a timeout */
        int recv_pldsize = sdf_msg_get_payloadsze(recv_msg);
        for (int k = 0; k < FORCETIMEOUT; k++) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                         "\nNode %d: SLEEPING %d secs --- calculated payload size %d\n", myid, k, recv_pldsize);
            sleep(1);
        }
#endif
        if (SHOWTS1) {
            tms_old = show_howlong(tms_old, 2, wt2_array);
            strcpy(wkrseq2[2], "WK2: Time after getting recv_msg");
        }

        if (SHOWPL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d"
                         " akrpmbx %p mlen %d\n", myid,
                         recv_msg, recv_msg->msg_src_vnode,
                         recv_msg->msg_dest_vnode,
                         recv_msg->msg_dest_service, recv_msg->msg_type,
                         recv_msg->akrpmbx, recv_msg->msg_len);
        }
#if PRINTMSGHDR
        unsigned char *m = (unsigned char *)recv_msg;
        for (int i = 0; i < 128; i++) {
            printf(" %02x", *m);
            fflush(stdout);
            m++;
            if ((i % 16) == 15) {
                printf("  myid %d %s\n", myid, __func__);
                fflush(stdout);
            }
        }
#endif
//        fthYield(1); /* let's give it up here */

        uint32_t mlen = recv_msg->msg_len;

        send_msg = (struct sdf_msg *)sdf_msg_alloc(mlen);
        if (send_msg == NULL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "sdf_msg_alloc(recv_msg->msg_len) failed\n");
        /* return ((void *)1); */
        }

        for (int i = 0; i < TSZE; ++i)
            send_msg->msg_payload[i] = (unsigned char) 0x55;

        struct sdf_resp_mbx rhkey;
        struct sdf_resp_mbx *ptrkey = sdf_msg_initmresp(&rhkey);

/*  we used to just reflect back the message payload   
 *  memcpy(send_msg->msg_payload, recv_msg->msg_payload, sdf_msg_get_payloadsze(struct sdf_msg *msg));
 *  
 */

        if (SHOWTS1) {
            tms_old = show_howlong(tms_old, 3, wt2_array);
            strcpy(wkrseq2[3], "WK2: Time to alloc send_msg & cpy in recv_msg");
        }

        if (SHOWPL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Posting reply MSG dn %d ds %d sn %d ss %d type %d loop %d\n",
                         myid, node, protocol, myid, my_protocol, type, ct);
        }

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: RESPONSE INFO MSG %p dflt mkey %s rbox %p\n", myid, recv_msg,
                     ptrkey->mkey, ptrkey->rbox);

        if (UNEXPT_TEST) {
            ret = sdf_msg_send((struct sdf_msg *)send_msg, recv_msg->msg_len, node, protocol,
                               myid, my_protocol, type, &fthmbx,
                               sdf_msg_get_response(recv_msg, ptrkey));
        } else {
            ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, protocol,
                               myid, my_protocol, type, &fthmbx,
                               sdf_msg_get_response(recv_msg, ptrkey));
        }
        if (ret != 0)
            process_ret(ret, protocol, type);

        if (SHOWTS1) {
            tms_old = show_howlong(tms_old, 4, wt2_array);
            strcpy(wkrseq2[4], "WK2: Time to send_msg");
        }

        aresp = fthMboxWait(&ackmbx2);

        if (SHOWTS1) {
            tms_old = show_howlong(tms_old, 5, wt2_array);
            strcpy(wkrseq2[5], "WK2: Time for ack MBoxWait");
        }

        /* release the receive buffer back to the sdf messaging thread */
        ret = sdf_msg_free_buff(recv_msg);

        if (SHOWPL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Send Buff Freed aresp %ld loop %d\n", myid, aresp, ct);
        }
        if (SHOWTS1) {
            tms_old = show_howlong(tms_old, 6, wt2_array);
            strcpy(wkrseq2[6], "WK2: Time to free recv_msg buff");
        }

            if (0) {
                tms_new = show_howlong(tms_old, 1, wt2_array);
                printf("\nNode %d: %s TIMESTAMP new %li usec old %li diff %li loop %d\n", myid,
                       __func__, tms_new/1000, tms_old/1000, wt2_array[1]/1000, ct);
                fflush(stdout);
            }

#if PRNT_TS
        for (i = 0; i < 8; i++) {
            if (wt2_array[i] > ALLVALS) {
                printf("\nNode %d: %s loop %d Seq %d %s - ELAPSED TIME %li usec\n",
                       myid, __func__, ct, i, wkrseq2[i], (wt2_array[i]/1000));
                fflush(stdout);
            }
            wt2_array[i] = 0;
        }
#endif
        ct++;

        if (SHORTTEST) {
            if (ct == (maxcnt / 2)) {
                mysync++;
                break;
            }
        }
        /*
         * Simple exit mechanism, worker threads will just quit when predefined msgcnt
         * has been reached in the sender thread
         */
        fthYield(1); /* we yield to give others a chance to do some work */
    }
    fthMboxTerm(&ackmbx2);
#if 1
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: WORKER FTH exiting - loop %d mysync %d\n", myid, ct, mysync);
    printf("\nNode %d: %s EXITING completed sending %d messages\n",
           myid, __func__, ct);
    fflush(stdout);
#endif
    fthYield(1); /* we yield to give others a chance to do some work */
}


void sdf_msg_resp_gc(uint64_t arg) {

    struct sdf_msg *garb_msg = NULL;
    int i = 0;

    printf("FTH Thread starting %s\n", __func__);
    fflush(stdout);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d Starting garbage resp collector %d\n", myid, i);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Now yielding waiting for mysync\n", myid);
    while (!mysync)    fthYield(1);
    if (garb_msg);

/*
 * FIXME this was added to take the messages off of the SDF_RESPONSES queue
 * but it really doesn't matter since the queue will just wrap around
 * anyway
 */
//    return;

    uint64_t crp = get_the_nstimestamp();
    uint64_t crparr[1];

    for (;;) {
#if 0
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Now Sleeping on RESPONSES queue %p\n", myid, q_pair_RESPONSES->q_out);
        garb_msg = sdf_msg_receive(q_pair_RESPONSES->q_out, 0, B_TRUE);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: got garbage response num %d\n", myid, i);

#endif
        crp = show_howlong(crp, 0, crparr);
        i++;
#if 0
        printf("Node %d: %s loop %d time stamp %li diff %li\n", myid, __func__, i, crp, crparr[0]);
        fflush(stdout);
#endif
        fthYield(1);
        if ((SHORTTEST)&&(i == 2)) {
            break;
        }

        /*
         * FIXME do not release the receive buffer back to the sdf messaging thread
         * for this loop, it's already being done indirectly by the
         * int ret = sdf_msg_free_buff(garb_msg);
         */
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: garbage collector exiting %d\n", myid, i);
    mysync++;
}

static int
process_ret(int ret_err, int prt, int type) {

    for (;;) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: Message Sent Error ret %d protocol %d type %d HALTING\n",
                     myid, ret_err, prt, type);
        sleep(100);
    }
    return (0);
}

