/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   system_test.c
 * Author: Norman Xu
 *
 * Created on May 9, 2008, 11:45 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: system_test.c 308 2008-05-09 22:34:58Z tomr $
 */

/* pthread test example for sending messages */

#ifdef MPI_BUILD
#include <mpi.h>
#endif

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <CUnit/CUnit.h>
#include "platform/logging.h"
//#define PLAT_OPTS_NAME(name) name ## _mpilogme
#include "platform/opts.h"
#include "sdfmsg/sdf_msg_types.h"
#include "msg_test.h"

#define DBGP 0
#define TSZE 256 
#define SHORTTEST 2
#define UNEXPT_TEST 0
#define FASTPATH_TEST 0

PLAT_LOG_CAT_LOCAL(LOG_CAT, "sdf/sdfmsg/tests/mpilog")
;

static service_t my_protocol = SDF_METADATA;

/*
 * This pthread will be used to simulate the SDF_SYSTEM sending and
 * receiving messages to the sdf messaging engine
 */

void * SystemPthreadRoutine(void *arg) {
	int myid = (int *)arg;
	struct sdf_queue_pair *q_pair_SYSTEM;
	struct sdf_msg *send_msg= NULL, *recv_msg= NULL;
	vnode_t node;
	serviceid_t protocol = SDF_SYSTEM;
	msg_type_t type;
	int i, j, ret;
	int debug = 0;
	sdf_fth_mbx_t fthmbx;

	/* FIXME we probably need to expand the fthmbx structure to include pthread items */
	int msg_sys_release = 0;
	fthmbx.actlvl = 100;

	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d: Testing pthread MESSAGING SDF_SYSTEM Communication\n", myid);

	/*
	 * Create a queue pair from this thread servicing my_protocol to the thread
	 * on another node
	 */
    if (FASTPATH_TEST) {
        node = myid;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: FASTPATH_TEST node %d myid %d\n", myid, node, myid);
    } else {
        node = myid == 0 ? 1 : 0;
    }
    
	while (!q_pair_SYSTEM) {
		usleep(10000);
		q_pair_SYSTEM = local_create_myqpairs_with_pthread(my_protocol, myid, node);
		if (!q_pair_SYSTEM) {
			plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
					"\nNode %d: Cannot find qpair %p sn %d dn %d ss %d ds %d\n",
					myid, q_pair_SYSTEM, myid, myid == 0 ? 1 : 0, my_protocol, SDF_SYSTEM);
		} else {
			plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
					"\nNode %d: got SYS queue pair %p sn %d dn %d ss %d ds %d\n",
					myid, q_pair_SYSTEM, myid, myid == 0 ? 1 : 0, my_protocol, SDF_SYSTEM);
		}
	}

	if (debug)
		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d SYSTEM TEST THREAD STOPPING HERE FOR DEBUG\n",
				myid);
	while (debug)
		;

	j = 0;
	for (;;) {

		send_msg = (struct sdf_msg *) sdf_msg_alloc(TSZE);
		if (send_msg == NULL) {
			printf("sdf_msg_alloc(%d) failed\n", TSZE);
			return ((void *)1);
		}

		if (DBGP) {
			plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
					"\nNode %d msg %p msg->msg_payload %p\n",
					myid, send_msg, send_msg->msg_payload);
		}
		/* Fill in body of message with test data */
		for (i = 0; i < TSZE; ++i)
			send_msg->msg_payload[i] = (unsigned char) i;

		protocol = SDF_SYSTEM; 
		type = SYS_REQUEST; 

		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: SENDING SYSTEM MSG dnode %d, proto %d, type %d loop num %d\n",
				myid, node, protocol, type, j);
		if (j < 8 ) {
			ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node,
					protocol, myid, my_protocol, type, &fthmbx, NULL);

			if (DBGP) {
				plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
						"\nNode %d: sdf_msg_send returned %d\n", myid, ret);
				plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
						"\nNode %d: %s: calling sdf_msg_receive(%p, %d, %d)\n",
						myid, __func__, q_pair_SYSTEM->q_out, 0, B_TRUE);
			}
		}

		//receive msg
		recv_msg = sdf_msg_receive(q_pair_SYSTEM->q_out, 0, B_TRUE);

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
		if (DBGP) {
			plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
					"\nNode %d: back from sdf_msg_receive with msg %p\n",
					myid, recv_msg);
		}
		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
				"\nNode %d: RECEIVING MSG vers %d clusterid %d ss %d ds %d sn %d dn %d type %d\n",
				myid, recv_msg->msg_version, recv_msg->msg_clusterid,
				recv_msg->msg_src_service, recv_msg->msg_dest_service,
				recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode, recv_msg->msg_type);

		sdf_msg_free_buff(recv_msg);
		msg_sys_release++; /* global sync for SYS mangmt thread init */
		sleep(5);
		j++;
	    if(j > 16)
	        break;
	}

	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			"\nNode %d Exiting pthread SYSTEM Tester\n", myid);
	return (0);
}
