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
 * File:   fcnl_test.h
 * Author: Norman Xu
 *
 * Created on June 23, 2008, 11:45 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 *
 */

#ifndef FCNL_TEST_H_
#define FCNL_TEST_H_

#include "sdfmsg/sdf_msg_types.h"

void * ConsistencyPthreadRoutine(void *arg);
void * ManagementPthreadRoutine(void *arg);
void * SystemPthreadRoutine(void *arg);
void * MembershipPthreadRoutine(void *arg);
void * FlushPthreadRoutine(void *arg);
void * MetadataPthreadRoutine(void *arg);
void * ReplicationPthreadRoutine(void *arg);
void * OrderTestFthPthreadRoutine(void *arg);
void * OrderingSenderPthreadRoutine(void *arg);
void * OrderingReceiverPthreadRoutine(void *arg);
void * BigSizePthreadRoutine(void *arg);
void * BigSizeFthPthreadRoutine(void *arg);
void * ReceiveQueuePthreadRoutine(void *arg);
void * SendQueuePthreadRoutine(void *arg);
void * SimplexSendReceiveRoutine(void *arg);
void * OrderTestFthSinglePthreadRoutine(void *arg);
void * OrderingSender1MultiPthreadRoutine(void *arg);
void * OrderingSender2MultiPthreadRoutine(void *arg);
void * OrderingReceiverMultiPthreadRoutine(void *arg);
void * MixedthreadTestfthRoutine(void *arg);
void * MixedthreadTestpthreadRoutine(void *arg);
void * MixedthreadTestfthUniptlRoutine(void *arg);
void * MixedthreadTestpthreadUniptlRoutine(void *arg);

void * MultiProtocolPthreadRoutine(void *arg);
void * SingleProtocolPthreadRoutine(void *arg);
void * MultiPtlSequentialPthreadRoutine(void *arg);
void * SinglePtlSequentialPthreadRoutine(void *arg);
void * MultiPtlQuantityPthreadRoutine(void *arg);
void * MultiPtlSglNodeSequentialPthreadRoutine(void *arg);
void * MultiPtlSequentialMtosPthreadRoutine(void *arg);
void * MultiPtlSequentialStomPthreadRoutine(void *arg);
void * SinglePtlMsgContentChkPthreadRoutine(void *arg);
void * SinglePtlQsizeChkPthreadRoutine(void *arg);
void * SinglePtlSequentialMaxPthreadRoutine(void *arg);
void * SinglePtlSequentialPressPthreadRoutine(void *arg);
void * PressTestPthreadRoutine(void *arg);
void * MultiNodePthreadRoutine(void *arg);
void * MultiNodeSstomrPthreadRoutine(void *arg);
void * MultiNodeMstosrPthreadRoutine(void *arg);
void * MultiNodeTstotrPthreadRoutine(void *arg);
void * MultiNodeMultiPtlSstomrPthreadRoutine(void *arg);
void * MultiNodeMultiPtlMstosrPthreadRoutine(void *arg);
void * MultiNodeMstomrPthreadRoutine(void *arg);
void * MultiNodeTntotnPthreadRoutine(void *arg);
void * MultiNodeMstomrMrenPthreadRoutine(void *arg);
void * MultiNodeMstomrMsenPthreadRoutine(void *arg);
void * WrapperPthreadRoutine(void *arg);
void * WrapperPthreadRoutine2(void *arg);
void * FthMbxPthreadRoutine(void *arg);
void * PerformancePthreadRoutine(void *arg);
void * fthPthreadRoutine(void *arg);
void * WrapperFthRoutine(void *arg);

typedef struct pth_init_order_data {
    int myid;
    pthread_cond_t pth_cond;
    pthread_mutex_t pth_mutex;
    pthread_mutex_t count_mutex;
} pth_init_order_data_t;

typedef struct startsync {
    fthSpinLock_t spin;
} startsync_t;

typedef struct plat_opts_config_mpilogme {
    struct plat_shmem_config shmem;
    int inputarg;
    int msgtstnum;
    char propertyFileName[PATH_MAX];
    /** @brief flags obtained from "msg_mpi" command line arg are kept here */
    int sdf_msg_init_state;
    int msg_init_flags; /* initialization flags */
    int myid;           /* the node id */
    uint32_t numprocs;  /* number of processes started */
    int startMessagingThreads; /* save the props file flag */
    int cores;          /* number of cores to lock to, dflt is 2 */
    int sdf_msg_timeout;  /* response msg timeout value in sec */
    int nnum;             /* node num needed for socket msg */
    int port;          /* socket port  */
} msg_config_t;

SDF_boolean_t init_msgtest_sm(uint32_t index);

/** local shemem utility for sdfmsg tests */

#define PLAT_OPTS_ITEMS_mpilogme() \
    PLAT_OPTS_SHMEM(shmem)                                     \
    item("msg_mpi", "2 mpi w/o msg mgnmt thread, 1 starts it",          \
         MSG_MPI, parse_int(&config->sdf_msg_init_state, optarg, NULL),  \
         PLAT_OPTS_ARG_REQUIRED)                               \
    item("msg_cnt", "# of desired messages",                   \
         MSG_CNT, parse_int(&config->msgtstnum, optarg, NULL), \
         PLAT_OPTS_ARG_REQUIRED)                               \
    item("msg_timeout", "timeout limit (secs) for remote node responses",     \
         MSG_TIMEOUT, parse_int(&config->sdf_msg_timeout, optarg, NULL),       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("cores", "# of desired cores to lock to",             \
         CORES, parse_int(&config->cores, optarg, NULL),     \
         PLAT_OPTS_ARG_REQUIRED)                               \
    item("property_file", "property file name",                           \
         PROPERTY_FILE, parse_string(config->propertyFileName, optarg),   \
         PLAT_OPTS_ARG_REQUIRED)                                          \
    item("port", "socket port",                   \
         PORT, parse_int(&config->port, optarg, NULL), \
         PLAT_OPTS_ARG_REQUIRED)                               \
    item("nnum", "socket number",                           \
         NNUM, parse_int(&config->nnum, optarg, NULL),          \
         PLAT_OPTS_ARG_REQUIRED)


#endif /*FCNL_TEST_H_*/
