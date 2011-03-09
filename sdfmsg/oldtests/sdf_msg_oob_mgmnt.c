/*
 * File:   sdf_msg_mgmnt.c
 * Author: Tom Riddle
 *
 * Created on March 20, 2008, 11:45 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_mgmnt.c 308 2008-02-20 22:34:58Z tomr $
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
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_oob.h"

extern msg_srtup_t *msg_srt; /* global startup sync for SYS mangmt thread init */

#define DBGP 0

PLAT_LOG_CAT_LOCAL(LOG_CAT, "sdf/sdfmsg/tests/mpilog")
;

static service_t my_protocol = SDF_SYSTEM;

/*
 * This pthread will be used to simulate the SDF_MANAGEMENT sending and
 * receiving messages to the sdf messaging engine
 */
static void oob_sample_send(int sendcount, uint32_t rank, sdf_msg_oob_server_t* self);
static void oob_sample_receive(sdf_msg_oob_server_t* self);

void *
sdf_msg_tester_start(void *arg) {
    int *myid = (int *) arg;
    uint32_t localid = *myid;
    struct sdf_queue_pair *q_pair_SYSTEM = NULL;
    struct sdf_msg *send_msg = NULL;
    sdf_msg_oob_server_t serv_info; // every node will be a server, but node 0 will be a server first
    vnode_t node;
    serviceid_t protocol = SDF_SYSTEM;
    msg_type_t type;
    int i, j, ret;
    sdf_fth_mbx_t fthmbx;

    fthmbx.actlvl = 100;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: Testing pthread MESSAGING MANGEMENT Communication\n",
            localid);

    /*
     * Create a queue pair from this thread servicing my_protocol to the thread
     * on another node serving CONSISTENCY
     */

    int total_rank;
    MPI_Comm_size(MPI_COMM_WORLD, &total_rank);

    while (1) {
        usleep(500000);
        pthread_mutex_lock(&msg_srt->msg_release);
        if (msg_srt->msg_mpi_release) { /* notify that bin_init is done */
            q_pair_SYSTEM = sdf_msg_getsys_qpairs(my_protocol, localid);
        }
        pthread_mutex_unlock(&msg_srt->msg_release);
        if (!q_pair_SYSTEM) {
            plat_log_msg(
                    PLAT_LOG_ID_INITIAL,
                    LOG_CAT,
                    PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: Cannot find qpair %p sn %d dn %d ss %d ds %d\n",
                    localid, q_pair_SYSTEM, localid, localid == 0 ? 1 : 0,
                    my_protocol, SDF_SYSTEM);
        } else {
            pthread_mutex_lock(&msg_srt->msg_release);
            msg_srt->msg_sys_release++; /* notify that bin_init is done */
            plat_log_msg(
                    PLAT_LOG_ID_INITIAL,
                    LOG_CAT,
                    PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: got SYS queue pair %p sn %d dn %d ss %d ds %d sys_r %d\n",
                    localid, q_pair_SYSTEM, localid, localid == 0 ? 1 : 0,
                    my_protocol, SDF_SYSTEM, msg_srt->msg_sys_release);
            pthread_mutex_unlock(&msg_srt->msg_release);
            break;
        }
    }

    int debug = 0;
    if (debug) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d MNGMT TEST THREAD EXITING HERE - no msg sent\n",
                localid);
        //return (0);
        while (debug)
            sleep(3);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d SDF Msg Management Thread is coming up\n", localid);
    }
    /* FIXME - lets get off of this dual node harcode stuff */
    node = (localid == 1 ? 0 : 1);
    int nodecount = 2;

    send_msg = (struct sdf_msg *) sdf_msg_alloc(1024);
    if (send_msg == NULL) {
        printf("sdf_msg_alloc(1024) failed\n");
        return ((void *) 1);
    }

    if (DBGP) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d msg %p msg->msg_payload %p\n", localid, send_msg,
                send_msg->msg_payload);
    }

    protocol = SDF_SYSTEM; /* num value of 1 */
    type = SYS_REQUEST; /* num value of 1 */

    plat_log_msg(
            PLAT_LOG_ID_INITIAL,
            LOG_CAT,
            PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: SENDING SYS MSG dnode %d, proto %d, type %d loop num %d\n",
            localid, node, protocol, type, j);

    if (!j) {
        for (i = 1; i < nodecount; i++) {
            ret = sdf_msg_send((struct sdf_msg *) send_msg, 1024, i, protocol,
                    localid, my_protocol, type, &fthmbx, NULL);

            if (DBGP) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE,
                        "\nNode %d: sdf_msg_send returned %d\n", localid, ret);
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                        PLAT_LOG_LEVEL_TRACE,
                        "\nNode %d: %s: calling sdf_msg_receive(%p, %d, %d)\n",
                        localid, __func__, q_pair_SYSTEM->q_out, 0, B_TRUE);
            }
        }
    }

    //OOB start
    //if the node rank is 0, send self information to other nodes
    if (localid == 0) {

        /* Fill in body of message with test data */
        /*for (i = 0; i < 1024; ++i)
         send_msg->msg_payload[i] = (unsigned char) i;
         */
        //prepare for the message node one send to others
        if (sdf_msg_oob_init_server(&serv_info, localid, total_rank)
                != OOB_SUCCESS) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nInit node 0 error\n");
        }

        sdf_msg_oob_server_dsp_t serv_dsp;
        serv_dsp.ipaddr = serv_info.server_id.nid.ip_address;
        serv_dsp.rank = localid;
        serv_dsp.port = serv_info.server_id.port;
        serv_dsp.reserved = 0;

        //after send self information, node 0 wait for other's report
        if (OOB_SUCCESS
                != sdf_msg_oob_collect_client_information_and_send_list(
                        &serv_info)) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\ncollect client information error\n");
            printf("!!!!!!!!collect client information error\n");
        } else {
            for (i = 0; i < serv_info.connected_clients; i++) {
                printf("\nCLIENTS %d: ip %X, port %d, rank %d\n", i,
                        serv_info.clients[i].nid.ip_address,
                        serv_info.clients[i].port,
                        serv_info.clients[i].nid.mpi_rank_id);
            }
            printf("here**************************\n");
        }

    } else {
        sdf_msg_oob_server_dsp_t serv_dsp;
        sdf_msg_oob_get_server_info(&serv_dsp);

        // init itself, actually after receive list, each client will treat itself as a server
        if (sdf_msg_oob_init_server(&serv_info, localid, total_rank)
                != OOB_SUCCESS) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "Init node %d error\n", localid);
        }

        sdf_msg_oob_identity_t client_id;

        sdf_msg_oob_create_id_info(sdf_msg_oob_get_self_ipaddr(), localid,
                &client_id);

        uint32_t res = sdf_msg_oob_send_id_report_and_get_list(&client_id,
                &serv_dsp, &serv_info);

        if (res != OOB_SUCCESS) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nClient get info list failed!\n");
            return (void*) 1;
        }

        for (i = 0; i < serv_info.connected_clients; i++) {
            printf("\nCLIENT %d: ip %X, port %d, rank %d\n", i,
                    serv_info.clients[i].nid.ip_address,
                    serv_info.clients[i].port,
                    serv_info.clients[i].nid.mpi_rank_id);
        }
    }

    int sendcount;
    for(;;)
    {
        oob_sample_send(sendcount, node, &serv_info);
        oob_sample_receive(&serv_info);
        sendcount++;
        sleep(1);
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d Exiting pthread MANGEMENT Tester\n", localid);
    return (0);
}

void oob_sample_send(int sendcount, uint32_t rank, sdf_msg_oob_server_t* self)
{
    char send_buf[1024];
    sprintf(send_buf, "It is a sample message with send count %d", sendcount);
    uint32_t res = sdf_msg_oob_send_msg(send_buf, 1024, rank, self);
    if(res != OOB_SUCCESS)
    {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nsend message error, rank %d\n", rank);
    }
    else
    {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nsend message to rank %d successfully", rank);
    }
}

void oob_sample_receive(sdf_msg_oob_server_t* self)
{
    char recv_buf[1024];
    uint32_t rank;
    int len;
    uint32_t res = sdf_msg_oob_recv_msg(recv_buf, &len, &rank, self);
    if(res != OOB_SUCCESS)
    {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nRECV MESSAGE error, rank %d\n", rank);
    }
    else
    {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nRECV MESSAGE :\n%s\nfrom rank %d successfully\n", recv_buf, rank);
    }
}
