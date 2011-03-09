/*
 * File:   fcnl_wrapper_lib1.c
 * Author: Zhenwei
 *
 * Created on Oct 24, 2008, 11:20 AM
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
#include "sdfmsg/sdf_msg_wrapper.h"

#include "fcnl_test.h"
#include "Utilfuncs.h"

extern uint32_t myid;

extern struct test_info * info;
#define DBGP 0
#define TSZE 256
#define SHORTTEST 20
#define UNEXPT_TEST 0
#define FASTPATH_TEST 0

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

static service_t my_protocol = SDF_REPLICATION;
static int cluster_node[MAX_NUM_SCH_NODES];

/*
 * @brief wrapper sdf_msg_send() for testing.
 * */
int wrapper_sdf_msg_send(struct sdf_msg *msg, uint32_t len, vnode_t dest_node, service_t dest_service, vnode_t src_node,
    service_t src_service, msg_type_t msg_type, sdf_fth_mbx_t *ar_mbx, sdf_fth_mbx_t *ar_mbx_from_req)
{
    return sdf_msg_send(msg, len, dest_node, dest_service, src_node, src_service, msg_type, ar_mbx, ar_mbx_from_req);
}

static void 
sdf_msg_wrapper_sdf_msg_free(plat_closure_scheduler_t *context, void *env, 
			    struct sdf_msg *msg) {
    sdf_msg_free(msg);
}

int
copyAndSend(sdf_msg_sp_t * shared, uint32_t len, vnode_t src_vnode, service_t src_service, 
		vnode_t dest_vnode, service_t dest_service, msg_type_t msg_type, struct sdf_fth_mbx *fthmbox, 
		struct sdf_fth_mbx *response_mbx) {
    uint32_t ret = -1;
    /*generate a wrapper*/
    struct sdf_msg_wrapper *wrapper = NULL;
    struct sdf_msg_wrapper *cp_wrapper = NULL;
    wrapper = sdf_msg_wrapper_shared_alloc(*shared, sdf_msg_wrapper_free_shared_null, SMW_MUTABLE_FIRST, src_vnode, src_service, dest_vnode, dest_service, msg_type, response_mbx);
    if(wrapper) {
	struct sdf_msg *temp = NULL;
	sdf_msg_wrapper_rwref(&temp, wrapper);
	temp->msg_len = len;
	sdf_msg_wrapper_rwrelease(&temp, wrapper);

	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL, 
		"\n msg:%p sn:%d s_prot:%d dn:%d d_prot:%d type:%d, len:%d\n",
		shared, src_vnode, src_service, dest_vnode, dest_service, msg_type, len);

	if((cp_wrapper = sdf_msg_wrapper_copy(wrapper, src_vnode, src_service, dest_vnode, dest_service, msg_type, response_mbx)) != NULL) {
	    ret = sdf_msg_wrapper_send(cp_wrapper, fthmbox);
	    if(0 != ret) {
		plat_assert_always(0);
	    }
	}
    }
    return ret;
}

/* XXX: drew 2009-06-14 This seems to exist both here and in fcnl_wrapper_fth_lib1.c */
static int
initializeAndSend(struct sdf_msg *local, 
                  enum sdf_msg_wrapper_type msg_wrapper_type,
                  uint32_t len, vnode_t src_vnode, service_t src_service, 
		vnode_t dest_vnode, service_t dest_service, msg_type_t msg_type, struct sdf_fth_mbx *fthmbox, 
		struct sdf_fth_mbx *response_mbx) {
	uint32_t ret = -1;
	struct sdf_msg_wrapper * wrapper = NULL;
//	sdf_msg_wrapper_free_local_t local_free;
	
/*	local_free = sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                   &sdf_msg_wrapper_sdf_msg_free, NULL);
*/
	/** Initialize sdf_msg_wrapper*/
        wrapper = sdf_msg_wrapper_local_alloc(local, sdf_msg_wrapper_free_local_null, SMW_MUTABLE_FIRST, msg_wrapper_type, src_vnode, src_service, dest_vnode, dest_service,
                                msg_type, response_mbx);
	struct sdf_msg *temp = NULL;
	sdf_msg_wrapper_rwref(&temp, wrapper);
	temp->msg_len = len + sizeof(struct sdf_msg);
	sdf_msg_wrapper_rwrelease(&temp, wrapper);

	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL, 
				"\n msg:%p sn:%d s_prot:%d dn:%d d_prot:%d type:%d, msg_len:%d, len:%d\n",
				local, src_vnode, src_service, dest_vnode, dest_service, msg_type, local->msg_len, len);

	/** send msg_wrapper*/
	if((ret = sdf_msg_wrapper_send(wrapper, fthmbox)) != 0) {
		plat_assert_always(0);
	}
	return ret;
}




void *
WrapperPthreadRoutine(void *arg) {
	   int myid = *(int *) arg;
	    struct sdf_queue_pair *q_pair_REPLICATION = NULL;
	    struct sdf_msg *send_msg = NULL, *recv_msg = NULL;
	    vnode_t node;
	    serviceid_t protocol = SDF_REPLICATION;
	    msg_type_t type;
	    int i, j, ret;
	    sdf_fth_mbx_t fthmbx;

	    /* FIXME we probably need to expand the fthmbx structure to include pthread items */
	    fthmbx.actlvl = SACK_NONE_FTH;
	    fthmbx.abox = NULL;
	    fthmbx.rbox = NULL;


	    plat_log_msg(
	            PLAT_LOG_ID_INITIAL,
	            LOG_CAT,
	            PLAT_LOG_LEVEL_TRACE,
	            "\nNode %d: Testing pthread MESSAGING SDF_REPLICATION Communication\n",
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
	    while (!q_pair_REPLICATION) {
	        usleep(10000);
	        q_pair_REPLICATION = local_create_myqpairs_with_pthread(my_protocol,
	                myid, node);
	        if (!q_pair_REPLICATION) {
	            plat_log_msg(
	                    PLAT_LOG_ID_INITIAL,
	                    LOG_CAT,
	                    PLAT_LOG_LEVEL_TRACE,
	                    "\nNode %d: Cannot find qpair %p sn %d dn %d ss %d ds %d\n",
	                    myid, q_pair_REPLICATION, myid, myid == 0 ? 1 : 0,
	                    my_protocol, SDF_REPLICATION);
	        } else {
	            plat_log_msg(
	                    PLAT_LOG_ID_INITIAL,
	                    LOG_CAT,
	                    PLAT_LOG_LEVEL_TRACE,
	                    "\nNode %d: got SYS queue pair %p sn %d dn %d ss %d ds %d\n",
	                    myid, q_pair_REPLICATION, myid, myid == 0 ? 1 : 0,
	                    my_protocol, SDF_REPLICATION);
	        }
	    }

	    int debug = 0;
	    if (debug)
	        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
	                "\nNode %d REPLICATION TEST THREAD STOPPING HERE FOR DEBUG\n",
	                myid);
	    while (debug);

	    j = 0;
	    // start the message engine
	    sdf_msg_startmsg(myid, 0, NULL);

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
	            send_msg->msg_payload[i] = (unsigned char) i;

	        protocol = SDF_REPLICATION;
	        type = REPL_REQUEST;

	        plat_log_msg(
	                PLAT_LOG_ID_INITIAL,
	                LOG_CAT,
	                PLAT_LOG_LEVEL_TRACE,
	                "\nNode %d: SENDING REPLICATION MSG dnode %d, proto %d, type %d loop num %d\n",
	                myid, node, protocol, type, j);
                /*
                 * XXX: drew 2009-06-14 This is nearly worthless since most of the message complexity
                 * is in the request and response paths.
                 */
	        if (j < SHORTTEST) {
		/*	ret = wrapper_sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, 
			    protocol, myid, my_protocol, type, &fthmbx, NULL);
			    */
/*		    ret = copyAndSend((sdf_msg_sp_t * )send_msg, SMW_TYPE_REQUEST,
				    TSZE, myid, my_protocol,node, protocol, type, &fthmbx, NULL); 
*/
		    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL, 
				"\n msg:%p sn:%d s_prot:%d dn:%d d_prot:%d type:%d\n",
				send_msg, myid, my_protocol, node, protocol, type);

		    ret = initializeAndSend((struct sdf_msg *)send_msg, SMW_TYPE_REQUEST,
	        			TSZE, myid, my_protocol,node, protocol, type, &fthmbx, NULL); 
		  	        	
		    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
				"\n send_msg:return: %d", ret);

	            /*ret = sdf_msg_send((struct sdf_msg *) send_msg, TSZE, node,
	                    protocol, myid, my_protocol, type, &fthmbx, NULL);*/

	            if (DBGP) {
	                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
	                        PLAT_LOG_LEVEL_TRACE,
	                        "\nNode %d: sdf_msg_send returned %d\n", myid, ret);
	                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
	                        PLAT_LOG_LEVEL_TRACE,
	                        "\nNode %d: %s: calling sdf_msg_receive(%p, %d, %d)\n",
	                        myid, __func__, q_pair_REPLICATION->q_out, 0, B_TRUE);
	            }
	        }

	        //receive msg
	        //recv_msg = sdf_msg_receive(q_pair_REPLICATION->q_out, 0, B_TRUE);


		/*receive msg using wrapper API*/
	        recv_msg = sdf_msg_receive(q_pair_REPLICATION->q_out, 0, B_TRUE);
		struct sdf_msg_wrapper * wrapper_recv = sdf_msg_wrapper_recv(recv_msg);
		plat_assert(wrapper_recv);

	#if 0
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
	        j++;
	        if (j == SHORTTEST)
	            break;
	    }

	    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
			 "\nNode %d Exiting pthread REPLICATION Tester - total sent %d\n", myid, j);
	    return 0;
}
