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
#include <stdarg.h>     // Import vfprintf(), et al

#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/fcntl.h"
#include "platform/string.h"
#include "platform/errno.h"
#include "platform/stdlib.h"

#include "sdftcp/tcp.h"

#define eprintf(fmt, ...) \
    fprintf(stderr, fmt, ## __VA_ARGS__)

#ifdef _lint

void
tcp_panic(TCP *tcp, const char *fmt, ...)
{
    va_list args;

    if (tcp) {
        (void) tcp_exit(tcp);
    }
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
}

void
panic(const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
}

#else

#define tcp_panic(tcp, fmt, ...)                                               \
    ({                                                                         \
        if (tcp) {                                                             \
            tcp_exit(tcp);                                                     \
        }                                                                      \
        fprintf(stderr, fmt, ## __VA_ARGS__);                                  \
        plat_exit(1);                                                          \
    })                                                                         \

#define panic(fmt, ...)                                                        \
    ({                                                                         \
        fprintf(stderr, fmt, ## __VA_ARGS__);                                  \
        plat_exit(1);                                                          \
    })                                                                         \

#endif

static const TAG TEST_STAG = 123;
static const TAG TEST_DTAG1 = 1001;
static const TAG TEST_DTAG2 = 1002;

static int verbose;

int
main(int argc, char * const *argv)
{
    TCP *tcp;
    SMSG smsg;
    RMSG rmsg;
    char msgbuf1[128];
//    char msgbuf2[128];
    size_t msglen;
    int ret;
    int tnode;      /** @brief total number of nodes in "cluster" */
    int mnode;      /** @brief my node number */
    int nno;

    verbose = 0;
    if (argc > 1 && strcmp(argv[1], "-v") == 0) {
        verbose = 1;
    }

    tcp = NULL;
    ret = tcp_init(&tcp, TCP_DEBUG | TCP_EXPLAIN);
    if (ret < 0) {

#ifdef TESTS_OK
        tcp_panic(tcp, "tcp_init failed; %d\n", ret);
#else
        printf("TCP TEST is not yet FUNCTIONAL...\n");
        plat_exit(0);
#endif
    }

    tnode = tcp_node_count(tcp);
    mnode = tcp_me(tcp);

    if (verbose) {
        eprintf("Send messages.\n");
    }

    for (nno = 0; nno < tnode; ++nno) {
        int fd;

        if (nno == mnode) {
            continue;
        }
        fd = tcp_send_fd(tcp, nno);
        if (fd < 0) {
            tcp_panic(tcp, "tcp_send_fd failed; %d\n", fd);
        }
        ret = snprintf(msgbuf1, sizeof (msgbuf1),
            "Hello from %d, dtag=%lu", mnode, TEST_DTAG2);
        if (ret < 0 || ret >= sizeof (msgbuf1)) {
            tcp_panic(tcp, "snprintf failed\n");
        } else {
            msglen = ret;
        }
        smsg.stag = TEST_STAG;
        smsg.dtag = TEST_DTAG2;
        smsg.drank = nno;
        smsg.sgv[0].iov_base = msgbuf1;
        smsg.sgv[0].iov_len = msglen + 1;
        smsg.sgv[1].iov_base = NULL;
        ret = tcp_send(tcp, &smsg, NULL, 0);
        if (ret < 0) {
            tcp_panic(tcp, "tcp_send failed; %d\n", ret);
        }

        /*
         * Send message with tag==TEST_DTAG1, _after_ tag==TEST_DTAG2,
         * in order to test message selection and queueing of unselected
         * messages.
         */
        ret = snprintf(msgbuf1, sizeof (msgbuf1),
            "Hello from %d, dtag=%lu", mnode, TEST_DTAG1);
        if (ret < 0 || ret >= sizeof (msgbuf1)) {
            tcp_panic(tcp, "snprintf failed\n");
        } else {
            msglen = ret;
        }
        smsg.stag = TEST_STAG;
        smsg.dtag = TEST_DTAG1;
        smsg.drank = nno;
        smsg.sgv[0].iov_base = msgbuf1;
        smsg.sgv[0].iov_len = msglen + 1;
        smsg.sgv[1].iov_base = NULL;
        ret = tcp_send(tcp, &smsg, NULL, 0);
        if (ret < 0) {
            tcp_panic(tcp, "tcp_send failed; %d\n", ret);
        }
    }

    /*
     * Get messages with tag == TEST_DTAG1.
     */
    if (verbose) {
        eprintf("Read messages with tag==%lu.\n", TEST_DTAG1);
    }

    for (nno = 0; nno < tnode; ++nno) {
        int fd;

        if (nno == mnode) {
            continue;
        }
        fd = tcp_recv_fd(tcp, nno);
        if (fd < 0) {
            tcp_panic(tcp, "tcp_recv_fd failed; %d\n", fd);
        }
        ret = tcp_recv_tag(tcp, &rmsg, TEST_DTAG1, -1);
        if (ret < 0) {
            tcp_panic(tcp, "tcp_recv failed; %d\n", ret);
        }

        printf("Received message from node %lu:\n", rmsg.srank);
        printf("    '%s'\n", (char *)rmsg.buf);
        free(rmsg.buf);
    }

    /*
     * Get messages with tag == TEST_DTAG2.
     */
    if (verbose) {
        eprintf("Read messages with tag==%lu.\n", TEST_DTAG2);
    }

    for (nno = 0; nno < tnode; ++nno) {
        int fd;

        if (nno == mnode) {
            continue;
        }
        fd = tcp_recv_fd(tcp, nno);
        if (fd < 0) {
            tcp_panic(tcp, "tcp_recv_fd failed; %d\n", fd);
        }
        ret = tcp_recv_tag(tcp, &rmsg, TEST_DTAG2, -1);
        if (ret < 0) {
            tcp_panic(tcp, "tcp_recv failed; %d\n", ret);
        }

        printf("Received message from node %lu:\n", rmsg.srank);
        printf("    '%s'\n", (char *)rmsg.buf);
        free(rmsg.buf);
    }

    (void) tcp_exit(tcp);
    plat_exit(0);
}
