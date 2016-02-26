/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf_msg_oob.h
 * Author: Norman Xu
 *
 * Created on July 3, 2008, 3:52 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_oob.h,v 1.1 2008/07/03 15:52:59 norman Exp $
 */


#ifndef __SDF_MSG_OOB_H__
#define __SDF_MSG_OOB_H__

#define MAX_NODES 16
#define LISTEN_PORT_BASE 26130
#define SERV_PORT 5999

#define OOB_VERSION 0x01
#define OOB_SUCCESS 0
#define OOB_SOCK_ERROR  1
#define OOB_PARAM_ERROR 2

enum SDF_MSG_OOB_TYPE{
    SERVER_NOTIFY = 1, CLIENT_REPORT, CONTACTORS_SHARING, SDF_OOB_MANAGEMENT, SDF_OOB_MEMBERSHIP,
};

/**
 * @brief identity information of each node, used to establish TCP connection
 *
 * In sdf_msg_tester_start thread, each node will send a SYSTEM message to other node
 * first. This identity information is carried by payload of SYSTEM message. When
 * nodes find a node with mpi_rank_id = 0, they take it as server and establish TCP
 * connection with it.
 * The node with mpi_rank_id  = 0 will establish TCP listeners and act as a server
 */
typedef struct sdf_msg_oob_identity
{
    uint32_t ip_address;
    uint32_t mpi_rank_id;
} sdf_msg_oob_identity_t;

typedef struct sdf_msg_oob_comm_node
{
    sdf_msg_oob_identity_t nid;
    uint16_t port;
} sdf_msg_oob_comm_node_t;

/*!
 * @brief Out of band message header
 *
 */
typedef struct sdf_msg_oob_header
{
    uint16_t version;
    uint16_t msglen;
    uint32_t msgtype;
} sdf_msg_oob_header_t;

typedef struct sdf_msg_oob_server_dsp
{
    uint32_t ipaddr;
    uint32_t rank;
    uint16_t port;
    uint16_t reserved;
} sdf_msg_oob_server_dsp_t;

typedef struct sdf_msg_oob_client_report
{
    sdf_msg_oob_identity_t id_info;
} sdf_msg_oob_client_report_t;

typedef struct sdf_msg_oob_contractor_sharing
{
    uint32_t cnt_clients;
    sdf_msg_oob_comm_node_t clients[MAX_NODES];
} sdf_msg_oob_constractor_sharing_t;

typedef struct sdf_msg_oob_server
{
    uint32_t cnt_total;
    uint32_t connected_clients;
    sdf_msg_oob_comm_node_t server_id;
    sdf_msg_oob_comm_node_t clients[MAX_NODES]; // include self again
} sdf_msg_oob_server_t;

uint32_t sdf_msg_oob_get_self_ipaddr();

uint32_t sdf_msg_oob_get_server_info(sdf_msg_oob_server_dsp_t * server_info);

uint32_t sdf_msg_oob_create_id_info(uint32_t ipaddr, uint32_t rank, sdf_msg_oob_identity_t* id_info);

uint32_t sdf_msg_oob_init_server(sdf_msg_oob_server_t* server_info, uint32_t rank, int total_rank);

uint32_t sdf_msg_oob_collect_client_information_and_send_list(sdf_msg_oob_server_t* server_info);

uint32_t sdf_msg_oob_send_id_report_and_get_list (
        sdf_msg_oob_identity_t* client_info,
        sdf_msg_oob_server_dsp_t* server_info,
        sdf_msg_oob_server_t* self);

uint32_t sdf_msg_oob_send_msg(char* msg_buf, int msg_len, uint32_t rank, sdf_msg_oob_server_t * self);

uint32_t sdf_msg_oob_recv_msg(char* recv_buf, int* recv_len, uint32_t* rank, sdf_msg_oob_server_t * self);

#endif
