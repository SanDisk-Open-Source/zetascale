/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: bw.c
 * Author: Johann George
 * Copyright (c) 2009, Schooner Information Technology, Inc.
 *
 * Determine message bandwidth between nodes using the low level messaging
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


/*
 * Static variables.
 */
static msg_init_t   InitMsg;
static int          Mid      = 1;
static int          BiDir    = 0;
static int          Debug    = 0;
static int          QSize    = 5;
static int          NoNodes  = 2;
static int          MsgSize  = 1024*1024;
static int          Duration = 5;
static int          WaitTime = 0;


/*
 * Function prototypes.
 */
static int          numarg(char *opt, char *arg, int min);
static char        *strarg(char *opt, char *arg);
static void         usage(void);
static void         bandwidth(void);
static void         parse_args(char **argv);
static void         syncup(ntime_t etime, nno_t rnode);
static void         sendonly(ntime_t etime, nno_t rnode);
static void         recvonly(ntime_t etime, nno_t rnode);
static void         sendrecv(ntime_t etime, nno_t rnode);
static void         sendmsg(nno_t nno, char *buf, int len);
static void         print_bw(uint64_t bytes, ntime_t stime, ntime_t etime);
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
    bandwidth();
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
        else if (streq(arg, "-b") || streq(arg, "--bi"))
            BiDir = 1;
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
        else if (streq(arg, "-q") || streq(arg, "--qsize"))
            QSize = numarg(arg, *argv++, 1);
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
}


/*
 * Print out a usage message and exit.
 */
static void
usage(void)
{
    char *s =
        "Usage\n"
        "    bw Options\n"
        "Options\n"
        "    -a|--msg_alias N\n"
        "        Set alias.\n"
        "    -b|--bi\n"
        "        Perform a bi-directional test.\n"
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
        "    -q|--qsize N\n"
        "        Set the size of the send queue which means that N+1\n"
        "        messages that can be in flight simultaneously.\n"
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
 * Start measuring bandwidth.
 */
static void
bandwidth(void)
{
    ntime_t etime = msg_endtime(WaitTime*NANO);

    if (NoNodes == 1)
        sendrecv(etime, msg_mynodeno());
    else if (NoNodes == 2) {
        nno_t rnode;
        msg_info_t *info = wantmsg(etime, MSG_EJOIN, 0);

        if (!info)
            panic("second node not found");
        rnode = info->nno;
        msg_ifree(info);

        if (BiDir)
            sendrecv(etime, rnode);
        else if (msg_cmpnode(msg_mynodeno(), rnode) <= 0)
            sendonly(etime, rnode);
        else
            recvonly(etime, rnode);
    } else
        panic("must specify 1 or 2 nodes");
}


/*
 * Send messages to the other side.
 */
static void
sendonly(ntime_t etime, nno_t rnode)
{
    ntime_t stime;
    uint64_t sentmid;
    char *sendbuf = q_malloc(MsgSize);
    uint64_t bytes = 0;

    memset(sendbuf, 0, MsgSize);
    syncup(etime, rnode);
    sentmid = Mid - 1;
    stime = msg_gettime();
    etime = stime + (ntime_t)Duration*NANO;
    while (msg_gettime() < etime) {
        msg_info_t *info = msg_poll(0);

        if (info) {
            if (info->type == MSG_ESENT) {
                if (info->mid > sentmid)
                    sentmid = info->mid;
            } else if (info->type == MSG_ERECV) {
                if (!info->len) {
                    msg_ifree(info);
                    break;
                }
            }
            msg_ifree(info);
        }
        if (Mid-1-sentmid > QSize)
            continue;
        sendmsg(rnode, sendbuf, MsgSize);
        bytes += MsgSize;
    }
    etime = msg_gettime();
    sendmsg(rnode, NULL, 0);
    printf("sender done\n");
}


/*
 * Receive messages.
 */
static void
recvonly(ntime_t etime, nno_t rnode)
{
    ntime_t stime;
    uint64_t bytes = 0;

    syncup(etime, rnode);
    stime = msg_gettime();
    etime = stime + (ntime_t)Duration*NANO;
    for (;;) {
        int len;
        msg_info_t *info = wantmsg(etime, MSG_ERECV, rnode);

        if (!info)
            break;
        len = info->len;
        msg_ifree(info);
        if (!len)
            break;
        bytes += info->len;
    }
    etime = msg_gettime();
    sendmsg(rnode, NULL, 0);
    print_bw(bytes, stime, etime);
}


/*
 * Send and receive messages to ourself.
 */
static void
sendrecv(ntime_t etime, nno_t rnode)
{
    ntime_t stime;
    uint64_t sentmid;
    char *sendbuf = q_malloc(MsgSize);
    uint64_t sbytes = 0;
    uint64_t rbytes = 0;

    memset(sendbuf, 0, MsgSize);
    sentmid = Mid - 1;
    stime = msg_gettime();
    etime = stime + (ntime_t)Duration*NANO;
    while (msg_gettime() < etime) {
        msg_info_t *info = msg_poll(0);

        if (info) {
            if (info->type == MSG_ESENT) {
                if (info->mid > sentmid)
                    sentmid = info->mid;
            } else if (info->type == MSG_ERECV) {
                if (!info->len) {
                    msg_ifree(info);
                    break;
                }
                rbytes += info->len;
            }
            msg_ifree(info);
        }
        if (Mid-sentmid-1 > QSize)
            continue;
        sendmsg(rnode, sendbuf, MsgSize);
        sbytes += MsgSize;
    }
    etime = msg_gettime();
    sendmsg(rnode, NULL, 0);
    print_bw(rbytes, stime, etime);
}


/*
 * Print out the bandwidth.
 */
static void
print_bw(uint64_t bytes, ntime_t stime, ntime_t etime)
{
    double bw;
    double mb;
    double secs;
    ntime_t times = etime - stime;

    if (!times)
        fatal("no time has elapsed");
    mb = (double)bytes / (1024*1024);
    secs = (double)times / NANO;
    bw = mb / secs;
    printf("bandwidth: %.1f MB/sec\n", bw);
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


/*
 * Send a message to the other side.
 */
static void
sendmsg(nno_t nno, char *buf, int len)
{
    msg_send_t *send = msg_salloc();

    t_ubug("sending message: rnode=%d len=%d", nno, MsgSize);
    send->sid = Mid++;
    send->nno = nno;
    send->nsge = 1;
    send->sge[0].iov_base = buf;
    send->sge[0].iov_len = len;
    msg_send(send);
}
