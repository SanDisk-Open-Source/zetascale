/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   tcp_test.c
 * Author: gshaw
 *
 * Created on September 22, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: $
 */

/*
 * Test out Guy's TCP messaging.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define eprintf(fmt, ...) \
    fprintf(stderr, fmt, ## __VA_ARGS__)

#define panic(fmt, ...) \
    ({ fprintf(stderr, fmt, ## __VA_ARGS__); exit(1); })


#include "tcp.h"

#define RINGS 10

char *Message = "Hello world";

int
main(int argc, char **argv)
{
    TCP *tcp;
    int ret;
    char *buf;
    int i, n, len;
    int tnode;      /** @brief total number of nodes in "cluster" */
    int mnode;      /** @brief my node number */
    int lnode;      /** @brief node number of my neighbor to the left */
    int rnode;      /** @brief node number of my neighbor to the right */
    int lfd;        /** @brief underlying file descriptor of lnode */
    int rfd;        /** @brief underlying file descriptor of rnode */

    ret = tcp_init(&tcp, TCP_DEBUG | TCP_EXPLAIN);
    if (ret < 0) {
        panic("tcp_init failed; %d\n", ret);
    }

    tnode = tcp_node_count(tcp);
    mnode = tcp_me(tcp);
    /*
     * Conceptually, the nodes are arranged in a ring,
     * with node numbers in [ 0 .. tnode-1 ],
     * where my node number is mnode,
     * node $(mnode - 1) mod tnode$ is my neighbor to the left, and
     * node $(mnode + 1) mod tnode$ is my neighbor to the right.
     */
    lnode = mnode - 1;
    if (lnode < 0) {
        lnode = tnode - 1;
    }
    rnode = mnode + 1;
    if (rnode == tnode) {
        rnode = 0;
    }

    lfd = tcp_recv_fd(tcp, lnode);
    rfd = tcp_send_fd(tcp, rnode);
    eprintf("lfd=%d, rfd=%d\n", lfd, rfd);
    len = strlen(Message);
    buf = malloc(len + 1);
    if (!buf) {
        panic("out of space\n");
    }

    if (mnode == 0) {
        n = write(rfd, Message, len);
        if (n != len) {
            perror("write");
            panic("write failed\n");
        }
    }

    for (i = 0; i < RINGS; ++i) {
        n = read(lfd, buf, len);
        if (n != len) {
            perror("read");
            panic("read failed\n");
        }
        buf[n] = '\0';
        printf("Node %d: %s\n", mnode, buf);
        n = write(rfd, buf, len);
        if (n != len) {
            perror("write");
            panic("write failed\n");
        }
    }
    exit(0);
    return (0);
}
