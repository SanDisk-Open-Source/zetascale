/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: msgtest_pplat.c
 * Author: Johann George
 * Copyright (c) 2009, Schooner Information Technology, Inc.
 *
 * Determine message latency between nodes using the low level messaging
 * system.
 *
 */
#include <time.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include "msg_msg.h"
#include "trace.h"
#include "msgtest_common.h"

/*
 * Useful definitions.
 */
#define SYNCMSG "Synchronize"
#define streq(a, b) (strcmp(a, b) == 0)
#define div_round(l1, l2) (((l1) + (l2)/2) / (l2))
#define div_roundup(l1, l2) (((l1) + (l2) - 1) / (l2))


/*
 * Static variables.
 */
static msg_init_t   InitMsg;
static int          Debug = 0;
static int          OneNode  = 0;
static int          MsgSize  = 1;
static int          Duration = 2;
static int          WaitTime = 0;


/*
 * Function prototypes.
 */
static int          recv_msg(nno_t nno, ntime_t etime, char *buf, int *len);
static void         usage(void);
static void         pingpong(void);
static void         parse_args(char **argv);
static void         send_msg(nno_t nno, char *buf, int len);
static void         syncup(ntime_t etime, nno_t rnode);
static msg_info_t  *wantmsg(ntime_t etime, int type, nno_t nno);


/*
 * Main.
 */
int
main(int argc, char *argv[])
{
    parse_args(argv+1);
    msg_init(&InitMsg);
    pingpong();
    msg_exit();
    return 0;
}


/*
 * Parse arguments.
 */
static void
parse_args(char **argv)
{
    for (;;) {
        char *arg = *argv++;

        if (!arg)
            break;
        if (streq(arg, "-1") || streq(arg, "--one"))
            OneNode = 1;
        else if (streq(arg, "-c") || streq(arg, "--class"))
            InitMsg.class = numarg(arg, *argv++, 0);
        else if (streq(arg, "-d") || streq(arg, "--debug"))
            Debug = 1;
        else if (streq(arg, "-h") || streq(arg, "--help"))
            usage();
        else if (streq(arg, "-i") || streq(arg, "--interface"))
            InitMsg.iface = strarg(arg, *argv++);
        else if (streq(arg, "-m") || streq(arg, "--msgsize"))
            MsgSize = numarg(arg, *argv++, 1);
        else if (streq(arg, "-n") || streq(arg, "--name"))
            InitMsg.alias = strarg(arg, *argv++);
        else if (streq(arg, "-p") || streq(arg, "--port"))
            InitMsg.udpport = numarg(arg, *argv++, 1);
        else if (streq(arg, "-t") || streq(arg, "--time"))
            Duration = numarg(arg, *argv++, 1);
        else if (streq(arg, "-w") || streq(arg, "--wait"))
            WaitTime = numarg(arg, *argv++, 0);
    }
}


/*
 * Print out a usage message and exit.
 */
static void
usage(void)
{
    char *s =
        "Usage\n"
        "    lat Options\n"
        "Options\n"
        "    -1|--one N\n"
        "        Run on only one node.\n"
        "    -c|--class N\n"
        "        Set class.\n"
        "    -d|--debug\n"
        "        Print debugging information.\n"
        "    -h|--help\n"
        "        Print this message.\n"
        "    -i|--interface i1:i2:...:in\n"
        "        Use only specified interfaces.\n"
        "    -m|--msg_size N\n"
        "        Use message size of N bytes.\n"
        "    -n|--name N\n"
        "        Set name.\n"
        "    -p|--port N\n"
        "        Set port.\n"
        "    -t|--time N\n"
        "        Make test last N seconds.\n"
        "    -w|--wait N\n"
        "        Wait at most N seconds for other side to start up.\n";
    fputs(s, stderr);
    plat_exit(0);
}


/*
 * Start the pingpong test.
 */
static void
pingpong(void)
{
    int count;
    nno_t rnode;
    char *sendbuf;
    ntime_t stime;
    ntime_t etime;

    /* Allocate send buffer */
    sendbuf = malloc(MsgSize);
    if (!sendbuf)
        panic("malloc failed");
    memset(sendbuf, 0, MsgSize);
    sendbuf[0] = 1;

    /* Wait for remote node and determine number */
    if (OneNode)
        rnode = msg_mynodeno();
    else {
        ntime_t etime = msg_endtime(WaitTime*NANO);
        msg_info_t *info = wantmsg(etime, MSG_EJOIN, 0);

        if (!info)
            panic("second node not found");
        rnode = info->nno;
        msg_ifree(info);
        syncup(etime, rnode);
    }

    /* Get start and end time */
    stime = msg_gettime();
    etime = stime + (ntime_t)Duration*NANO;

    /* One of the nodes will send the first message */
    if (msg_cmpnode(msg_mynodeno(), rnode) <= 0) {
        t_ubug("initiating ping pong");
        send_msg(rnode, sendbuf, MsgSize);
    }

    /* Go through ping pong */
    count = 0;
    for (;;) {
        char buf[4];

        int len = sizeof(buf);
        if (!recv_msg(rnode, etime, buf, &len)) {
            sendbuf[0] = 0;
            send_msg(rnode, sendbuf, 1);
            break;
        }
        if (len && buf[0] == 0)
            break;
        count++;
        send_msg(rnode, sendbuf, MsgSize);
    }
    etime = msg_gettime();

    /* Print out latency */
    printf("Latency: %.1f us\n", div_round((etime-stime)/1000.0, count*2));

    /* Clean up */
    free(sendbuf);
}


/*
 * Cause both sides to be synced up.
 */
static void
syncup(ntime_t etime, nno_t rnode)
{
    msg_info_t *info;
    int len = strlen(SYNCMSG);

    send_msg(rnode, SYNCMSG, len);
    info = wantmsg(etime, MSG_ERECV, rnode);
    if (info->len != len || strncmp(info->data, SYNCMSG, len) != 0)
        panic("sync failed");
    msg_ifree(info);
}


/*
 * Send a message to the other side.
 */
static void
send_msg(nno_t nno, char *buf, int len)
{
    msg_send_t *send = msg_salloc();

    //t_user("sending message: rnode=%d len=%d", nno, MsgSize);
    send->nno = nno;
    send->nsge = 1;
    send->sge[0].iov_base = buf;
    send->sge[0].iov_len = len;
    msg_send(send);
}


/*
 * Receive a message returning the contents of the first len bytes of the
 * received message.  On input, *len contains the size of the buffer.  On
 * output, *len contains the number of bytes copied.
 */
static int
recv_msg(nno_t nno, ntime_t etime, char *buf, int *len)
{
    msg_info_t *info = wantmsg(etime, MSG_ERECV, nno);

    if (!info)
        return 0;
    if (len) {
        int n = *len;

        if (n > info->len)
            n = info->len;
        memcpy(buf, info->data, n);
        *len = n;
    }
    msg_ifree(info);
    return 1;
}


/*
 * Expect a message.
 */
static msg_info_t *
wantmsg(ntime_t etime, int type, nno_t nno)
{
    msg_info_t want ={
        .type = type,
        .nno  = nno
    };
    msg_info_t *info = msg_want(etime, NULL, &want);

    if (!info)
        return NULL;
    if (info->error)
        panic("read error: %s", info->error);
    return info;
}
