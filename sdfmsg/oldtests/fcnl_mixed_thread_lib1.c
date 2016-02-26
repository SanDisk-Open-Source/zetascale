/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fcnl_mixed_thread_lib1.c
 * Author: Norman Xu
 *
 * Created on July 21, 2008, 7:45AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcnl_mixed_thread_lib1.c 308 2008-07-21 22:34:58Z normanxu $
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

extern uint32_t myid;
extern int sdf_msg_free_buff(sdf_msg_t *msg);
extern int outtahere;
extern struct test_info * info;
#define DBGP 0
#define SENDTWO 0
#define DIVNUM 100
#define TSZE 2048
#define SHORTTEST 10
#define UNEXPT_TEST 0
#define FASTPATH_TEST 0
#define SHOWBUFF 0

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

/*
 * These fthreads will be used to simulate the SDF protocol sending and
 * receiving messages to the sdf messaging engine and across nodes
 */

struct sdf_queue_pair *q_pair_CONSISTENCY;
struct sdf_queue_pair *q_pair_RESPONSES;
struct sdf_queue_pair *q_pair_revert_CONSISTENCY;

static fthMbox_t ackmbox, respmbox, ackmbx1, ackmbx2;
static int cluster_node[MAX_NUM_SCH_NODES];

static int mysync = 0;
static int endsync = 2;
static struct startsync crp;
static startsync_t *ssync = &crp;
static int pth_end = 0;
static int maxcnt;
static int ct = 0;

/*
 * This fth thread simulates the Action Node, it sends a CONSISTENCY msg and
 * sleeps on a mailbox waiting for a response. The msg types are arbitrary for now
 */

static void fthThreadSender(uint64_t arg) {
	int i, l;
	uint64_t aresp;
	struct sdf_msg *send_msg = NULL;
	vnode_t node;
	serviceid_t protocol = SDF_CONSISTENCY;
	serviceid_t my_protocol = SDF_CONSISTENCY;
	msg_type_t type = REQ_FLUSH;
	sdf_fth_mbx_t fthmbx;
	fthMbox_t *fthmbxtst;
	int debug = 0;


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

	q_pair_CONSISTENCY = local_create_myqpairs(SDF_CONSISTENCY, myid, node);
    info->queue_pair_info->queue_add_pair[0] = q_pair_CONSISTENCY;
    info->queue_pair_info->queue_pair_type = SDF_CONSISTENCY;
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

	FTH_SPIN_LOCK(&ssync->spin);
	mysync = 1;
	FTH_SPIN_UNLOCK(&ssync->spin);

	sdf_msg_startmsg(myid, 0, NULL);

	if (myid == 1) {
		debug = 0;
		if (debug) {
			plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
					"\nNode %d: DEBUG --- NOT SENDING MESSAGES FROM HERE", myid);
			while (debug)
				fthYield(100); /* Don't send mesages from node one for now */
		}
	}

	for (l = 0; l < maxcnt; ++l) {
		sdf_msg_t *msg;
		unsigned char *m;
		int ret;

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
			send_msg->msg_payload[i] = (unsigned char) (0xC0 + l);

		type = REQ_FLUSH;

		if (UNEXPT_TEST) {
			ret = sdf_msg_send((struct sdf_msg *) send_msg, 8192, node,
					protocol, myid, my_protocol, type, &fthmbx, NULL);
		} else {
			ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node,
					protocol, myid, my_protocol, type, &fthmbx, NULL);
		}

		if (ret != 0)
			process_ret(ret, protocol, type, myid);

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
			msg = (sdf_msg_t *) fthMboxWait(&respmbox);
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
#if SHOWBUFF
			for (i = 0; i < 256*2; i++) {
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

	//send over, it is time to tell receiver you are ready to finalize
//	msg_type_t say_goodbye = GOODBYE;
//	sdf_msg_say_bye(node, protocol, myid, my_protocol, say_goodbye,
//			&fthmbx, TSZE);
//	if (ret != 0) {
//		process_ret(ret, protocol, type, myid);
//	}

	// tell the receivers: we have finished the task!
	
	printf("Node %d - Sender Yield with mysync = %d, pth_end = %d\n", myid, mysync, pth_end);
	while (mysync != 2 || !pth_end)
	    
		fthYield(100);
	printf("Sender End\n");
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: EXITING completed sending %d messages - mysync %d\n",
			myid, l, mysync);
	fthKill(1); // Kill off FTH
}

/* This FTH thread will act as a protocol worker thread.
 * It will wait for CONSISTENCY Messages
 * on the queue, processes them and returns appropriate RESPONSE messages
 */

static void fthThreadReceiver1(uint64_t arg) {
	int i, ret;
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

	if (FASTPATH_TEST) {
		node = myid;
		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
	}

	q_pair_RESPONSES = local_create_myqpairs(SDF_RESPONSES, myid, node);

	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: created queue pair %p sn %d dn %d ss %d ds %d\n", myid,
			q_pair_RESPONSES, myid, myid == 0 ? 1 : 0, SDF_RESPONSES,
			SDF_RESPONSES);
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: Now yielding waiting for mysync\n", myid);

	/* Need to yield till all queues have been created */
	while (!mysync )
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

		recv_msg = sdf_msg_receive(q_pair_CONSISTENCY->q_out, 0, B_TRUE);
		if (recv_msg->msg_type == GOODBYE) {
			sendfinished = 1;
		}
		// yield to let other scheduler run
		//usleep(1);
		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d"
					" akrpmbx %p\n", myid, recv_msg, recv_msg->msg_src_vnode,
				recv_msg->msg_dest_vnode, recv_msg->msg_dest_service,
				recv_msg->msg_type, recv_msg->akrpmbx);

		unsigned char reply = recv_msg->msg_payload[0];
#if SHOWBUFF
		unsigned char *m = (unsigned char *)recv_msg;
		for (i = 0; i < 256*2; i++) {
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

		for (i = 0; i < TSZE; ++i)
			send_msg->msg_payload[i] = (unsigned char) (reply - 0x20);
		
        struct sdf_resp_mbx rhkey;
        struct sdf_resp_mbx *ptrkey = &rhkey;

        strncpy(rhkey.mkey, MSG_DFLT_KEY, (MSG_KEYSZE - 1));
        rhkey.mkey[MSG_KEYSZE - 1] = '\0';
        rhkey.akrpmbx_from_req = NULL;
        rhkey.rbox = NULL;

		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: Sending MSG dn %d ds %d sn %d ss %d type %d"
					" akrpmbx %p send_msg %p\n", myid, node, protocol, myid,
				my_protocol, type, recv_msg->akrpmbx, send_msg);

		ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node, protocol,
				myid, my_protocol, type, &fthmbx, sdf_msg_get_response(
						recv_msg, ptrkey));

		if (ret != 0) {
			process_ret(ret, protocol, type, myid);
		}
		aresp = fthMboxWait(&ackmbx1);
		/* release the receive buffer back to the sdf messaging thread */
		ret = sdf_msg_free_buff(recv_msg);

		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: Send Buff Freed aresp %ld loop %d\n", myid, aresp,
				ct);

		ct++;
		/*if (SHORTTEST)
		 break;*/
		if (ct >= maxcnt)
			break;
		/* Simple exit mechanism, worker threads will just quit when predefined msgcnt
		 * has been reached in the sender thread
		 */
	}

	endsync--;
//	if ((--endsync) != 0) {
//	    printf("Endsync:%d\n", endsync);
//		//there are still some receivers left, we need to notify them
//		msg_type_t say_goodbye = GOODBYE;
//		ret = sdf_msg_say_bye(myid, SDF_CONSISTENCY, node, SDF_CONSISTENCY,
//				say_goodbye, &fthmbx, TSZE);
//		if (ret != 0) {
//			process_ret(ret, protocol, type, myid);
//		}
//	}

    mysync++;
    printf("1 Endsync:%d, Mysync:%d\n", endsync, mysync);

	fthYield(1);
	printf("receiver1 ends\n");
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: WORKER FTH exiting - loop %d mysync %d\n", myid, i,
			mysync);
}

/* This 2nd FTH thread will act as a protocol worker thread. It will wait for CONSISTENCY Messages
 * on the queue, processes them and returns appropriate RESPONSE messages
 */

static void fthThreadReceiver2(uint64_t arg) {
	int i = 0, ret;
	uint64_t aresp;
	struct sdf_msg *send_msg = NULL, *recv_msg = NULL;
	vnode_t node;
	serviceid_t protocol = SDF_RESPONSES;
	serviceid_t my_protocol = SDF_RESPONSES;
	msg_type_t type = RESP_TWO;
	sdf_fth_mbx_t fthmbx;

	fthmbx.actlvl = SACK_ONLY_FTH;
	fthmbx.abox = &ackmbx2;
	fthMboxInit(&ackmbx2);
	int sendfinished = 0;
	printf("FTH Thread starting %s\n", __func__);

	if (FASTPATH_TEST) {
		node = myid;
		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
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

	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: Found queue pair %p sn %d dn %d ss %d ds %d loop %d\n",
			myid, q_pair_RESPONSES, myid, (myid == 0 ? 1 : 0), SDF_RESPONSES,
			SDF_RESPONSES, ct);
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: Now yielding waiting for mysync\n", myid);

	/* Need to yield till all queues have been created */
	while (!mysync )
		fthYield(1);
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: Ready To Accept 1st MESSAGE sack lvl %d loop %d\n\n\n",
			myid, fthmbx.actlvl, ct);

	for (;;) {
	    
	    if (ct >= maxcnt)
	        break;
	       
		plat_log_msg(
				PLAT_LOG_ID_INITIAL,
				LOG_CAT,
				PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: Waiting for messages q_pair_CONSISTENCY %p loop %d\n",
				myid, q_pair_CONSISTENCY, ct);

		recv_msg = sdf_msg_receive(q_pair_CONSISTENCY->q_out, 0, B_TRUE);
		if (recv_msg->msg_type == GOODBYE) {
			sendfinished = 1;
		}
		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d"
					" akrpmbx %p\n", myid, recv_msg, recv_msg->msg_src_vnode,
				recv_msg->msg_dest_vnode, recv_msg->msg_dest_service,
				recv_msg->msg_type, recv_msg->akrpmbx);

		unsigned char reply = recv_msg->msg_payload[0];
#if SHOWBUFF
		unsigned char *m = (unsigned char *)recv_msg;
		for (i = 0; i < 256*2; i++) {
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

        for (i = 0; i < TSZE; ++i)
             send_msg->msg_payload[i] = (unsigned char) (reply - 0x30);

		plat_log_msg(
				PLAT_LOG_ID_INITIAL,
				LOG_CAT,
				PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: Posting reply MSG dn %d ds %d sn %d ss %d type %d loop %d\n",
				myid, node, protocol, myid, my_protocol, type, ct);
		
        struct sdf_resp_mbx rhkey;
        struct sdf_resp_mbx *ptrkey = &rhkey;

        strncpy(rhkey.mkey, MSG_DFLT_KEY, (MSG_KEYSZE - 1));
        rhkey.mkey[MSG_KEYSZE - 1] = '\0';
        rhkey.akrpmbx_from_req = NULL;
        rhkey.rbox = NULL;

		ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node, protocol,
				myid, my_protocol, type, &fthmbx, sdf_msg_get_response(
						recv_msg, ptrkey));
		if (ret != 0)
			process_ret(ret, protocol, type, myid);

		aresp = fthMboxWait(&ackmbx2);
		/* release the receive buffer back to the sdf messaging thread */
		ret = sdf_msg_free_buff(recv_msg);

		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: Send Buff Freed aresp %ld loop %d\n", myid, aresp,
				ct);
		ct++;

		/* Simple exit mechanism, worker threads will just quit when predefined msgcnt
		 * has been reached in the sender thread
		 */
	}
	
	--endsync;

//	if ((--endsync) != 0) {
//	    printf("Endsync:%d\n", endsync);
//		//there are still some receivers left, we need to notify them
//		msg_type_t say_goodbye = GOODBYE;
//		ret = sdf_msg_say_bye(myid, SDF_CONSISTENCY, node, SDF_CONSISTENCY,
//				say_goodbye, &fthmbx, TSZE);
//		if (ret != 0) {
//			process_ret(ret, protocol, type, myid);
//		}
//	}
	mysync++;
	fthYield(1);
	printf("2 Endsync:%d, Mysync:%d\n", endsync, mysync);
	printf("receiver2 ends\n");
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: WORKER FTH exiting - loop %d\n mysync %d", myid, i,
			mysync);
}

void * MixedthreadTestpthreadRoutine(void *arg) {
	int myid = *(int *) arg;
	struct sdf_queue_pair *q_pair_MANAGEMENT = NULL;
	struct sdf_msg *send_msg = NULL, *recv_msg = NULL;
	vnode_t node;
	serviceid_t my_protocol = SDF_MANAGEMENT;
	serviceid_t protocol = SDF_MANAGEMENT;
	msg_type_t type;
	int i, j, ret;
	int debug = 0;
	sdf_fth_mbx_t fthmbx;

	/* FIXME we probably need to expand the fthmbx structure to include pthread items */
	int msg_sys_release = 0;
	fthmbx.actlvl = SACK_NONE_FTH;

	printf("here\n");
	plat_log_msg(
			PLAT_LOG_ID_INITIAL,
			LOG_CAT,
			PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: Testing pthread MESSAGING SDF_MANGEMENT Communication\n",
			myid);

	/*
	 * Create a queue pair from this thread servicing my_protocol to the thread
	 * on another node
	 */
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
	while (!q_pair_MANAGEMENT) {
		usleep(10000);
		q_pair_MANAGEMENT = local_create_myqpairs_with_pthread(my_protocol,
				myid, node);
	    info->queue_pair_info->queue_add_pair[1] = q_pair_MANAGEMENT;
		if (!q_pair_MANAGEMENT) {
			fprintf(stderr, 
					"\nNode %d: Cannot find qpair %p sn %d dn %d ss %d ds %d\n",
					myid, q_pair_MANAGEMENT, myid, node,
					my_protocol, SDF_MANAGEMENT);
		} else {
			plat_log_msg(
					PLAT_LOG_ID_INITIAL,
					LOG_CAT,
					PLAT_LOG_LEVEL_TRACE,
					"\nNode %d: got SYS queue pair %p sn %d dn %d ss %d ds %d\n",
					myid, q_pair_MANAGEMENT, myid, node,
					my_protocol, SDF_MANAGEMENT);
		}
	}

	if (debug)
		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d MNGMT TEST THREAD STOPPING HERE FOR DEBUG\n", myid);
	while (debug)
		;

	j = 0;

	for (;;) {

		send_msg = (struct sdf_msg *) sdf_msg_alloc(TSZE);
		if (send_msg == NULL) {
			printf("sdf_msg_alloc(%d) failed\n", TSZE);
			return ((void *) 1);
		}

		if (DBGP) {
			plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
					"\nNode %d msg %p msg->msg_payload %p\n", myid, send_msg,
					send_msg->msg_payload);
		}
		/* Fill in body of message with test data */
		for (i = 0; i < TSZE; ++i)
			send_msg->msg_payload[i] = (unsigned char) (0xD0 + j);

		protocol = SDF_MANAGEMENT; /* num value of 3 */
		type = MGMT_REQUEST; /* num value of 1 */

		plat_log_msg(
				PLAT_LOG_ID_INITIAL,
				LOG_CAT,
				PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: SENDING MGMNT MSG dnode %d, proto %d, type %d loop num %d\n",
				myid, node, protocol, type, j);
		if (1) {
			ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node,
					protocol, myid, my_protocol, type, &fthmbx, NULL);

			printf("just send %d\n", j);

			if (DBGP) {
				plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
						PLAT_LOG_LEVEL_TRACE,
						"\nNode %d: sdf_msg_send returned %d\n", myid, ret);
				plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
						PLAT_LOG_LEVEL_TRACE,
						"\nNode %d: %s: calling sdf_msg_receive(%p, %d, %d)\n",
						myid, __func__, q_pair_MANAGEMENT->q_out, 0, B_TRUE);
			}
		}


		recv_msg = sdf_msg_receive(q_pair_MANAGEMENT->q_out, 0, B_TRUE);

#if SHOWBUFF
		unsigned char* m = (unsigned char *) recv_msg;
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

		if (DBGP) {
			plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
					"\nNode %d: back from sdf_msg_receive with msg %p\n", myid,
					recv_msg);
		}
		plat_log_msg(
				PLAT_LOG_ID_INITIAL,
				LOG_CAT,
				PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: RECEIVING MSG vers %d clusterid %d ss %d ds %d sn %d dn %d type %d\n",
				myid, recv_msg->msg_version, recv_msg->msg_clusterid,
				recv_msg->msg_src_service, recv_msg->msg_dest_service,
				recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode,
				recv_msg->msg_type);

		sdf_msg_free_buff(recv_msg);
		msg_sys_release++; /* global sync for SYS mangmt thread init */
		sleep(1);
		j++;
		if (j >= SHORTTEST) {
		    printf("Pthread break\n");
		    pth_end = 1;
		    break;
		}
			
	}

	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d Exiting pthread MANGEMENT Tester\n", myid);
	return (0);
}

void * MixedthreadTestfthRoutine(void *arg) {

	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d FTH threads firing up\n", myid);


	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d FTH scheduler has initialized\n", myid);

	fthResume(fthSpawn(&fthThreadReceiver1, 40960), 1);
	fthResume(fthSpawn(&fthThreadReceiver2, 40960), 2);
	usleep(500);
	fthResume(fthSpawn(&fthThreadSender, 40960), 0);

	fthSchedulerPthread(0);

	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nFTH scheduler halted\n");
	return (0);

}
