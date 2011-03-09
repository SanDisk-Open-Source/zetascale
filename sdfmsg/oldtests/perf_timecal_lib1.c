/*
 * File:   perf_timecal_lib1.c
 * Author: mac
 *
 * Created on Aug 25, 2008, 9:28 AM
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

#include "Utilfuncs.h"
#include "pfm_test.h"
#include "fcnl_test.h"

extern uint32_t myid;

#define DBGP 0
#define TSZE 64
#define NANSEC 1.00e-9f
#define FASTPATH_TEST 0
#define ALLVALS 100

PLAT_LOG_CAT_LOCAL(LOG_CAT, "sdf/sdfmsg/tests/mpilog");

/*
 * These fthreads will be used to simulate the SDF protocol sending and
 * receiving messages to the sdf messaging engine and across nodes
 *
 * @brief: x -> x
 * Create x senders and x receivers on each node, let the sender sends
 * message to the receiver. the sender will get ack or ack and resp message(control
 * by the user), then receiver will give response to sender and ack when
 * sending success. You can change many parameters, they are "FLAG, MAXNODEPTL, MAXCOUNT".
 *
 */

static struct sdf_queue_pair *q_pair[SDF_PROTOCOL_COUNT];

//number of current node's receiver fth threads
static int mysync[SDF_PROTOCOL_COUNT] = { 0 };
static int cluster_node[MAX_NUM_SCH_NODES];
static struct startsync crp;
static startsync_t *ssync = &crp;
static int local_niterator = 1;
//0 ==> only need ack
//1 ==> both ack and resp
#define FLAG 1

//MAXNODEPTL is the max protocol, the min protocol is SDF_CONSISTENCY ==> [SDF_CONSISTENCY, MAXNODEPTL)
#define MAXNODEPTL 3

// Each protocol sender sends MSGCOUNT message
#define MSGCOUNT 100

// the number of RECEIVERS
#define FTHRECVERS (MAXNODEPTL - 2)
static int msgsize = TSZE;
//the sender send one types protocol message
static int msgCount = MSGCOUNT, fthCount = 0;

void fthThreadPerfTestSender(uint64_t arg) {
	int l;
	vnode_t node;
	struct sdf_msg *send_msg = NULL;

	uint64_t usec_real;
	uint64_t usec_cpu;

	struct timespec curtime1, curtime2, cputime1, cputime2;
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

	printf("node %d, fth thread sender starting  %s: number of msgs to send = %d\n",myid, __func__, msgCount);
	fflush(stdout);

	int localpn, actmask;
	uint32_t numprocs;
	uint64_t ptl = arg;
	int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node,&actmask);
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: numprocs %d active_procs mask 0x%x active_mask 0x%x\n",
			localrank, numprocs, localpn, actmask);

	if (numprocs == 1) {
		node = 0;
		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
	} else {
		node = local_get_pnode(localrank, localpn, numprocs);
		printf("Node %d: %s my pnode is  %d\n", localrank, __func__, node);
		fflush(stdout);
		for (int i = 0; i < numprocs; i++) {
			printf("Node %d: %s cluster_node[%d] = %d\n", localrank, __func__,
					i, cluster_node[i]);
			fflush(stdout);
		}
	}

	// you only init this once but share the q_pairs among the other threads here
	q_pair[ptl] = local_create_myqpairs(ptl, myid, node);

	if (q_pair[ptl] == NULL) {
		fprintf(stderr, "%s: sdf_create_queue_pair %d failed\n", __func__, ptl);
		plat_log_msg(
				PLAT_LOG_ID_INITIAL,
				LOG_CAT,
				PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: EXITING completed sending %d messages - mysync %d\n",
				myid, l, mysync);
		return;
	}

	// right now we are not using shmem for these buffers

	plat_log_msg(
			PLAT_LOG_ID_INITIAL,
			LOG_CAT,
			PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: created queue pair %p sn %d dn %d ss %d ds %d maxcnt %d\n",
			myid, q_pair[ptl], myid, myid == 0 ? 1 : 0, ptl, ptl, msgCount);

	FTH_SPIN_LOCK(&ssync->spin);
	mysync[ptl] = 1;
	FTH_SPIN_UNLOCK(&ssync->spin);

	/*
	 * main loop will send SDF_CONSISTENCY protocol messages till msgCount is reached
	 * this sleeps on both mailboxes ack and resp based on the lvl dictated
	 */
        (void) clock_gettime(CLOCK_REALTIME, &curtime1);
        (void) clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cputime1);

	for (l = 0; l < msgCount; ++l) {
		int ret, i;

		// create the buffer on every loop to check the buffer release func

		send_msg = (struct sdf_msg *) sdf_msg_alloc(msgsize);
		if (send_msg == NULL) {
			// FIXME should default to an error
			plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
					"sdf_msg_alloc(TSZE) failed\n");
		}
		local_setmsg_mc_payload(send_msg, TSZE, myid, l, msgCount, ptl);

		type = REQ_FLUSH;

		ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node, ptl, myid,
				ptl, type, &fthmbx, NULL);
		/* get the ack when sending success. */
		fthMboxWait(&ackmbox);
#if FLAG
		printf("node %d, wait and get the response message from pnode.\n", myid);
		/* get the response when receive message success. */
		sdf_msg_t * msg = fthMboxWait(&respmbox);
		ret = sdf_msg_free_buff(msg);
#endif
		if (ret != 0)
			process_ret(ret, ptl, type, myid);

		fthYield(1);
	}
                (void) clock_gettime(CLOCK_REALTIME, &curtime2);
                (void) clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cputime2);
                usec_real = get_passtime(&curtime1, &curtime2);
                usec_cpu = get_passtime(&cputime1, &cputime2);

	printf(
			"@@node %d, sender #%d sends %d protocol message finished, send %d times\n",
			myid, ptl, ptl, l);

	FTH_SPIN_LOCK(&ssync->spin);
	fthCount++;
	FTH_SPIN_UNLOCK(&ssync->spin);

	while (fthCount != 2 * FTHRECVERS) {
		fthYield(100);
	}

	//output the time value
	int cnt;

	printf("begin!\n");
	fflush(stdout);
	printf("<SDFMSG:PERF:NODE %d>\nTotal iterations is %llu\n"
			"Wallclocktime is %llu, Avg Wallclocktime is %llu\n"
			"CPUclocktime is %llu, Avg CPUclocktime is %llu\n", localrank ,
			(uint64_t)msgCount, usec_real, usec_real / msgCount,
			usec_cpu, usec_cpu / msgCount);

	printf("node %d, sender %d kill the scheduler.\n", myid, ptl);
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: EXITING completed sending %d messages - mysync %d\n",
			myid, l, mysync[ptl]);

	fthKill(1);
}

/*
 * This FTH thread will act as a protocol worker thread.
 * It will wait for CONSISTENCY Messages
 * on the queue, processes them and returns appropriate RESPONSE messages
 */

void fthThreadPerfTestRecver(uint64_t arg) {
	int i, ret, ct = 0;
	uint64_t aresp, ptl;
	struct sdf_msg *recv_msg = NULL, *send_msg = NULL;
	vnode_t node;
	printf("node %d, fth thread receiver %d starting %s\n", myid, arg, __func__);
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

	ptl = arg;
	int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node,&actmask);
	if (localrank == localpn) {
		node = localpn;
		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
	} else {
		node = local_get_pnode(localrank, localpn, numprocs);
		printf("Node %d: my pnode is  %d\n", localrank, node);
		fflush(stdout);
	}
	while (mysync[ptl] != 1) {
		fthYield(1);
	}

	for (;;) {

		printf("Inside recver loop\n");
		recv_msg = sdf_msg_receive(q_pair[ptl]->q_out, 0, B_TRUE);

		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: Waiting for messages q_pair %p loop %d\n", myid,
				q_pair[ptl], ct);

		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: Got One *msg %p sn %d dn %d proto %d type %d"
					" akrpmbx %p\n", myid, recv_msg, recv_msg->msg_src_vnode,
				recv_msg->msg_dest_vnode, recv_msg->msg_dest_service,
				recv_msg->msg_type, recv_msg->akrpmbx);

#if 0
		if (recv_msg) {
			uint32_t d = recv_msg->msg_dest_service;
			printf(
					"node %d, receiver #%d recvs protocol#%d message from sender\n",
					myid, ptl, d);
			local_printmsg_payload(recv_msg, TSZE, myid);
		} else {
			printf(
					"!!node %d, receiver #%d recvs protocol#%d meessage from sender failed\n",
					myid, ptl, recv_msg->msg_dest_service);
		}
#endif

#if FLAG
		send_msg = (struct sdf_msg *) sdf_msg_alloc(recv_msg->msg_len);
	      memcpy(send_msg->msg_payload, recv_msg->msg_payload, recv_msg->msg_len - sizeof(sdf_msg_t));
		
        struct sdf_resp_mbx rhkey;
        struct sdf_resp_mbx *ptrkey = &rhkey;

        strncpy(rhkey.mkey, MSG_DFLT_KEY, (MSG_KEYSZE - 1));
        rhkey.mkey[MSG_KEYSZE - 1] = '\0';
        rhkey.akrpmbx_from_req = NULL;
        rhkey.rbox = NULL;

		ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node, ptl, myid,
				ptl, RESP_ONE, &fthmbx, sdf_msg_get_response(recv_msg, ptrkey));
		fthMboxWait(&ackmbox1);
#endif

		// release the receive buffer back to the sdf messaging thread
		ret = sdf_msg_free_buff(recv_msg);

		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: Send Buff Freed aresp %ld loop %d\n", myid, aresp,
				ct);
		ct++;

		if (ct == msgCount)
			break;

		/*
		 * Simple exit mechanism, worker threads will just quit when predefined msgcnt
		 * has been reached in the sender thread
		 */
	}
	printf("@@node %d, receiver #%d, receive message finished, receive %d times\n",myid, ptl, ct);

	FTH_SPIN_LOCK(&ssync->spin);
	fthCount++;
	FTH_SPIN_UNLOCK(&ssync->spin);

	printf("node: %d, protocol: %d, mysync[ptl]: %d, fthCount: %d\n", myid,
			ptl, mysync[ptl], fthCount);
	fthYield(1);

	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: WORKER FTH exiting - loop %d mysync %d\n", myid, ct,
			mysync[ptl]);

}

void *
PerfTestTimeConsumePthreadRoutine(void *arg) {
	int index;
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d FTH threads firing up\n", myid);

	plat_log_msg(
			PLAT_LOG_ID_INITIAL,
			LOG_CAT,
			PLAT_LOG_LEVEL_TRACE,
			"\nNode %d FTH scheduler has initialized -- Now Spawning fth threads\n",
			myid);

	perf_arg_t* perf_arg = arg;
	msgCount = perf_arg->niterators;
	msgsize = perf_arg->nmsgsize;

	printf("\nnode %d: %d fth(s) send %d times with size %d\n",
			myid, perf_arg->nthreads, perf_arg->niterators, perf_arg->nmsgsize);

	for (index = 2; index < perf_arg->nthreads + 2; index++) {
		XResume(XSpawn(&fthThreadPerfTestRecver, 40960), index);
		XResume(XSpawn(&fthThreadPerfTestSender, 40960), index);
	}
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d Finished Create and Spawned -- Now starting sched\n",myid);

	fthSchedulerPthread(0);

	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nFTH scheduler halted\n");
	return (0);

}

