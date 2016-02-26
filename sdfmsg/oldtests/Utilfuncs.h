/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   Utilfuncs.h
 * Author: HT
 *
 * Created on February 25, 2008, 3:45 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: 
 */

#ifndef UTILFUNCS_H_
#define UTILFUNCS_H_

#include "sdfmsg/sdf_msg_types.h"
#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "platform/types.h"
#include "fcnl_test.h"


struct sdf_queue_pair * local_create_myqpairs(service_t protocol,
        uint32_t myid, uint32_t pnode);

struct sdf_queue_pair * local_create_myqpairs_with_pthread(service_t protocol,
        uint32_t myid, uint32_t pnode);

void TestTrace(int tracelevel, int selflevel, const char *format, ...);

int sdf_msg_say_bye(vnode_t dest_node, service_t dest_service,
        vnode_t src_node, service_t src_service, msg_type_t msg_type,
        sdf_fth_mbx_t *ar_mbx, uint64_t length);

vnode_t local_get_pnode(int localrank, int localpn, uint32_t numprocs);

void local_setmsg_payload(sdf_msg_t *msg, int size, uint32_t nodeid, int num);

void local_setmsg_mc_payload(sdf_msg_t *msg, int size, uint32_t nodeid, int index, int maxcount, uint64_t ptl);

void local_printmsg_payload(sdf_msg_t *msg, int size, uint32_t nodeid); 

int process_ret(int ret_err, int prt, int type, vnode_t myid);

uint64_t get_timestamp();

uint64_t get_passtime(struct timespec* start, struct timespec* end);

/* setup the defaults for the test config parameters */
int msgtst_setconfig(struct plat_opts_config_mpilogme *config);
/* helper funcs for setting up the configuration flags */
int msgtst_setpreflags(struct plat_opts_config_mpilogme *config);
int msgtst_setpostflags(struct plat_opts_config_mpilogme *config);

#endif


