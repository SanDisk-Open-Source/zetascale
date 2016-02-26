/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: lat.c
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
#include "trace.h"
#include "msg_msg.h"


/*
 * Useful definitions.
 */
#define streq(a, b) (strcmp(a, b) == 0)
#define div_round(l1, l2) (((l1) + (l2)/2) / (l2))


/*
 * Static variables.
 */
static msg_init_t   InitMsg;
static int          Debug    = 0;
static int          NoNodes  = 2;
static int          MsgSize  = 1;
static int          Duration = 5;
static int          WaitTime = 0;


/*
 * Function prototypes.
 */
static int          numarg(char *opt, char *arg, int min);
static int          recvmsg(nno_t nno, ntime_t etime, char *buf, int *len);
static char        *strarg(char *opt, char *arg);
static void         usage(void);
static void         pingpong(void);
static void         parse_args(char **argv);
static void         syncup(ntime_t etime, nno_t rnode);
static void         sendmsg(nno_t nno, char *buf, int len);
static void         print_lat(uint64_t count, ntime_t stime, ntime_t etime);
static msg_info_t  *wantmsg(ntime_t etime, int type, nno_t nno);


/*
 * Main.
 */
int
main(int argc, char *argv[])
{
    parse_args(argv+1);
    trace_init(Debug);
    msg_init(&InitMsg);
    pingpong();
    msg_exit();
    trace_exit();
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
        if (streq(arg, "-a") || streq(arg, "--msg_alias"))
            InitMsg.alias = strarg(arg, *argv++);
        else if (streq(arg, "-c") || streq(arg, "--msg_class"))
            InitMsg.class = numarg(arg, *argv++, 0);
        else if (streq(arg, "-d") || streq(arg, "--msg_debug"))
            Debug = numarg(arg, *argv++, -1);
        else if (streq(arg, "-h") || streq(arg, "--help"))
            usage();
        else if (streq(arg, "-i") || streq(arg, "--msg_iface"))
            InitMsg.iface = strarg(arg, *argv++);
        else if (streq(arg, "-n") || streq(arg, "--msg_nodes"))
            NoNodes = numarg(arg, *argv++, 1);
        else if (streq(arg, "-p") || streq(arg, "--msg_tcp"))
            InitMsg.tcpport = numarg(arg, *argv++, 1);
        else if (streq(arg, "-s") || streq(arg, "--size"))
            MsgSize = numarg(arg, *argv++, 1);
        else if (streq(arg, "-t") || streq(arg, "--time"))
            Duration = numarg(arg, *argv++, 1);
        else if (streq(arg, "-u") || streq(arg, "--msg_udp"))
            InitMsg.udpport = numarg(arg, *argv++, 1);
        else if (streq(arg, "-w") || streq(arg, "--wait"))
            WaitTime = numarg(arg, *argv++, 0);
        else
            usage();
    }

    if (NoNodes != 1 && NoNodes != 2)
        panic("number of nodes must be either 1 or 2");
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
        "    -a|--msg_alias N\n"
        "        Set alias.\n"
        "    -c|--msg_class N\n"
        "        Set class.\n"
        "    -d|--msg_debug\n"
        "        Print debugging information.\n"
        "    -h|--help\n"
        "        Print this message.\n"
        "    -i|--msg_iface i1:i2:...:in\n"
        "        Use only specified interfaces.\n"
        "    -n|--msg_nodes N\n"
        "        Run on N nodes.  N must be 1 or 2.\n"
        "    -p|--msg_tcp N\n"
        "        Set TCP port.\n"
        "    -s|--size N\n"
        "        Use message size of N bytes.\n"
        "    -t|--time N\n"
        "        Make test last N seconds.\n"
        "    -u|--msg_udp N\n"
        "        Set UDP port.\n"
        "    -w|--wait N\n"
        "        Wait at most N seconds for other side to start up.\n";
    fputs(s, stderr);
    exit(0);
}


/*
 * Return a numeric argument.
 */
static int
numarg(char *opt, char *arg, int min)
{
    int n = strtol(strarg(opt, arg), NULL, 0);

    if (n < min)
        panic("value to %s must be at least %d", opt, min);
    return n;
}


/*
 * Return a string argument.
 */
static char *
strarg(char *opt, char *arg)
{
    if (!arg)
        panic("%s requires an argument", opt);
    return arg;
}


/*
 * Start the pingpong test.
 */
static void
pingpong(void)
{
    nno_t rnode;
    char *sendbuf;
    ntime_t stime;
    ntime_t etime;
    uint64_t count;

    /* Allocate send buffer */
    sendbuf = malloc(MsgSize);
    if (!sendbuf)
        panic("malloc failed");
    memset(sendbuf, 0, MsgSize);
    sendbuf[0] = 1;

    /* Wait for remote node and determine number */
    if (NoNodes == 1)
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
        sendmsg(rnode, sendbuf, MsgSize);
    }

    /* Go through ping pong */
    count = 0;
    for (;;) {
        char buf[4];

        int len = sizeof(buf);
        if (!recvmsg(rnode, etime, buf, &len)) {
            sendbuf[0] = 0;
            sendmsg(rnode, sendbuf, 1);
            break;
        }
        if (len && buf[0] == 0)
            break;
        count++;
        sendmsg(rnode, sendbuf, MsgSize);
    }
    etime = msg_gettime();

    /* Print out latency */
    print_lat(count, stime, etime);

    /* Clean up */
    free(sendbuf);
}


/*
 * Print out the latency.
 */
static void
print_lat(uint64_t count, ntime_t stime, ntime_t etime)
{
    double lat;
    double us = (etime - stime) / 1000.0;

    if (!count)
        fatal("no messages exchanged");
    lat = us / (count*2);
    printf("latency: %.1f us\n", lat);
}


/*
 * Cause both sides to be synced up.
 */
static void
syncup(ntime_t etime, nno_t rnode)
{
    msg_info_t *info;
    char *sync = "Synchronize";
    int len = strlen(sync);

    sendmsg(rnode, sync, len);
    info = wantmsg(etime, MSG_ERECV, rnode);
    if (!info || info->len != len || strncmp(info->data, sync, len) != 0)
        panic("sync failed");
    msg_ifree(info);
}


/*
 * Send a message to the other side.
 */
static void
sendmsg(nno_t nno, char *buf, int len)
{
    static uint64_t mid = 1;
    msg_send_t *send = msg_salloc();

    t_ubug("sending message: rnode=%d len=%d", nno, len);
    send->sid = mid++;
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
recvmsg(nno_t nno, ntime_t etime, char *buf, int *len)
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
