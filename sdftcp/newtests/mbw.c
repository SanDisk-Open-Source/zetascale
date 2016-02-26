/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: mbw.c
 * Author: Johann George
 * Copyright (c) 2009, Schooner Information Technology, Inc.
 *
 * A simple test to ensure that the low level messaging system can handle
 * limited multi-threading.
 *
 */
#include <time.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "trace.h"
#include "msg_msg.h"


/*
 * Useful definitions.
 */
#define streq(a, b) (strcmp(a, b) == 0)
typedef void *(pth_func_t)(void *);


/*
 * Pthread context.
 */
typedef struct pth_con {
    struct pth_con *next;
    pthread_t       pthread;
    int             no;
    uint64_t        npost;
    uint64_t        bytes;
    double          sendbw;
} pth_con_t;


/*
 * Parameters.
 */
static int Safe     = 0;
static int Debug    = 0;
static int QSize    = 5;
static int NoNodes  = 2;
static int MsgSize  = 1024*1024;
static int NThreads = 1;
static int Duration = 5;
static int WaitTime = 0;


/*
 * Variables.
 */
static int             NSent;
static int             ToNode;
static pth_con_t      *PthList;
static msg_init_t      InitMsg;
static pthread_mutex_t Mutex = PTHREAD_MUTEX_INITIALIZER;


/*
 * Function prototypes.
 */
static int         numarg(char *opt, char *arg, int min);
static char       *strarg(char *opt, char *arg);
static void        run(void);
static void        usage(void);
static void        mlock(void);
static void        munlock(void);
static void        set_dest(void);
static void        pth_wait(void);
static void        parse_args(char **argv);
static void        pth_start(pth_func_t *func, int no);
static void        sendmsg(char *buf, int len, int mid);
static void       *sendonly(void *arg);
static void       *recvonly(void *arg);
static double      sum_sendbw(void);
static double      calc_bw(uint64_t bytes, ntime_t stime, ntime_t etime);
static uint64_t    sum_npost(void);
static msg_info_t *mpoll(ntime_t etime);


/*
 * Main.
 */
int
main(int argc, char *argv[])
{
    parse_args(argv+1);
    trace_init(Debug);
    msg_init(&InitMsg);
    set_dest();
    run();
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
        else if (streq(arg, "-f") || streq(arg, "--safe"))
            Safe = 1;
        else if (streq(arg, "-h") || streq(arg, "--help"))
            usage();
        else if (streq(arg, "-i") || streq(arg, "--msg_iface"))
            InitMsg.iface = strarg(arg, *argv++);
        else if (streq(arg, "-j") || streq(arg, "--threads"))
            NThreads = numarg(arg, *argv++, 1);
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
        "    mbw Options\n"
        "Options\n"
        "    -a|--msg_alias N\n"
        "        Set alias.\n"
        "    -c|--msg_class N\n"
        "        Set class.\n"
        "    -d|--msg_debug\n"
        "        Print debugging information.\n"
        "    -h|--help\n"
        "        Print this message.\n"
        "    -f|--safe\n"
        "        Run in safe mode allowing only one thread to access the\n"
        "        messaging system at a time.\n"
        "    -i|--msg_iface i1:i2:...:in\n"
        "        Use only specified interfaces.\n"
        "    -j|--pthreads N\n"
        "        Number of sending pthreads to use.\n"
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
 * Set our destination node.
 */
static void
set_dest(void)
{
    ntime_t etime = msg_endtime(WaitTime*NANO);

    if (NoNodes == 1)
        ToNode = msg_mynodeno();
    else if (NoNodes == 2) {
        msg_info_t want ={.type = MSG_EJOIN};
        msg_info_t *info = msg_want(etime, NULL, &want);

        if (!info)
            panic("second node not found");
        if (info->error)
            panic("error: %s", info->error);
        ToNode = info->nno;
        msg_ifree(info);
    } else
        panic("must specify 1 or 2 nodes");
}


/*
 * Start measuring bandwidth.
 */
static void
run(void)
{
    int i;

    pth_start(recvonly, 0);
    for (i = 0; i < NThreads; i++)
        pth_start(sendonly, i+1);
    pth_wait();
    printf("send bandwidth: %.1f MB/sec\n", sum_sendbw());
}


/*
 * Start a pthread.
 */
static void
pth_start(pth_func_t *func, int no)
{
    pth_con_t *pth = q_malloc(sizeof *pth);

    memset(pth, 0, sizeof(*pth));
    if (pthread_create(&pth->pthread, NULL, func, pth) < 0)
        fatal("pthread_create failed");
    pth->next = PthList;
    pth->no = no;
    PthList = pth;
}


/*
 * Wait for the pthreads to finish.
 */
static void
pth_wait(void)
{
    pth_con_t *pth;

    for (pth = PthList; pth; pth = pth->next)
        pthread_join(pth->pthread, NULL);
}


/*
 * Send messages to the other side.
 */
static void *
sendonly(void *arg)
{
    pth_con_t *pth = (pth_con_t *)arg;
    ntime_t stime  = msg_gettime();
    ntime_t etime  = stime + (ntime_t)Duration*NANO;
    char *sendbuf  = q_malloc(MsgSize);

    memset(sendbuf, 0, MsgSize);
    while (msg_gettime() < etime) {
        if ((int64_t)sum_npost()-1-NSent > QSize)
            sched_yield();
        else {
            mlock();
            sendmsg(sendbuf, MsgSize, pth->no);
            munlock();
            pth->npost++;
            pth->bytes += MsgSize;
        }
    }
    pth->sendbw = calc_bw(pth->bytes, stime, msg_gettime());
    return NULL;
}


/*
 * Receive messages.
 */
static void *
recvonly(void *arg)
{
    double bw;
    uint64_t bytes = 0;
    ntime_t stime  = 0;
    ntime_t etime  = msg_endtime(WaitTime*NANO);

    for (;;) {
        msg_info_t *info;

        mlock();
        info = mpoll(etime);
        if (!info) {
            munlock();
            break;
        }
        if (info->type == MSG_ESENT)
            NSent++;
        else if (info->type == MSG_ERECV) {
            bytes += info->len;
            if (!stime) {
                stime = msg_gettime();
                etime = stime + Duration*NANO;
            }
        }
        msg_ifree(info);
        munlock();
    }

    bw = calc_bw(bytes, stime, msg_gettime());
    printf("recv bandwidth: %.1f MB/sec\n", bw);
    return NULL;
}


/*
 * Count the number of messages that have been posted.
 */
static uint64_t
sum_npost(void)
{
    pth_con_t *pth;
    uint64_t n = 0;

    for (pth = PthList; pth; pth = pth->next)
        n += pth->npost;
    return n;
}


/*
 * Sum the send bandwidth.
 */
static double
sum_sendbw(void)
{
    pth_con_t *pth;
    double bw = 0;

    for (pth = PthList; pth; pth = pth->next)
        bw += pth->sendbw;
    return bw;
}


/*
 * Calculate the bandwidth.
 */
static double
calc_bw(uint64_t bytes, ntime_t stime, ntime_t etime)
{
    ntime_t times = etime - stime;

    if (!times)
        return 0;
    return ((double)bytes / (1024*1024))  /  ((double)times / NANO);
}


/*
 * Send a message to the other side.
 */
static void
sendmsg(char *buf, int len, int mid)
{
    msg_send_t *send = msg_salloc();

    t_ubug("sending message: rnode=%d len=%d", ToNode, MsgSize);
    send->sid = mid;
    send->nno = ToNode;
    send->nsge = 1;
    send->sge[0].iov_base = buf;
    send->sge[0].iov_len = len;
    msg_send(send);
}


/*
 * Acquire the lock to the message system if needed.
 */
static void
mlock(void)
{
    if (!Safe)
        return;
    pthread_mutex_lock(&Mutex);
}


/*
 * Release the lock to the message system if needed.
 */
static void
munlock(void)
{
    if (!Safe)
        return;
    pthread_mutex_unlock(&Mutex);
    sched_yield();
}


/*
 * Poll the message system.
 */
static msg_info_t *
mpoll(ntime_t etime)
{
    msg_info_t *info;

    if (!Safe)
        return msg_poll(etime);

    for (;;) {
        info = msg_poll(0);
        if (info)
            return info;
        if (etime > 0 && msg_gettime() >= etime)
            return NULL;
        munlock();
        mlock();
    }
}
