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
