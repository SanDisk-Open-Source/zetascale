/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/* 
 * File: msgtest_common.h
 * Author: Norman Xu, jindong Hu
 * Copyright (C) 2009, Schooner Information Technology, Inc.
 *
 * Common functions for test cases should be declared here
 */
#ifndef _MSGTEST_COMMON_H_
#define _MSGTEST_COMMON_H_

#include "msg_msg.h"

#define REMOTE_NODES 4
typedef struct msgtest_config {
    int   remote_nodes;
    char *addresses[REMOTE_NODES];
} msgtest_config_t;

#define streq(a, b) (strcmp(a, b) == 0)

void    read_config(char *filename, msgtest_config_t *config);
void    print_nodes(msg_node_t *nodes, int no_nodes);
void    send_data_to_nodes(msg_node_t *nodes, int no_nodes, 
            void *data, size_t datalen);
int     numarg(char *opt, char *arg, int min);
char   *strarg(char *opt, char *arg);
ntime_t msg_gettime(void);
#endif
