/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: tm_orig.c
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * A test to send a variety of messages between two nodes using three fth
 * threads on each node.  This is based on the original messaging test that Tom
 * Riddle and Heng Tian wrote.
 */
#include <stdio.h>
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdftcp/tools.h"
#include "sdftcp/trace.h"
#include "sdftcp/msg_cat.h"
#include "sdftcp/msg_map.h"
#include "agent.h"


/*
 * Configurable parameters.
 */
#define STACK_SIZE  (128*1024)


/*
 * For convenience.
 */
#define streq(a, b) (strcmp(a, b) == 0)
typedef struct sdf_queue_pair qpair_t;


/*
 * Static variables.
 */
static int       Done;
static int       MyNode;
static int       ToNode;
static int       MsgSize;
static int       Threads;
static int       MsgCount;
static qpair_t  *ConsQueue;
static qpair_t  *RespQueue;
static char     *Data;


/*
 * Function prototypes.
 */
static qpair_t *create_queue(service_t proto);
static void     fth_master(uint64_t arg);
static void     fth_worker(uint64_t arg);
static void    *fth_pthread(void *arg);
static void     send_msg(char *buf, uint32_t len, service_t, msg_type_t,
                         sdf_fth_mbx_t *fthmbx, sdf_resp_mbx_t *respmbx);
static int      numarg(char *opt, char *arg, int min);
static void     parse(int *argcp, char ***argvp);
static void     show_recv(char *type, sdf_msg_t *msg);
static void     test(void);
static void     usage(void);


/*
 * Main.
 */
int
main(int argc, char *argv[])
{
    parse(&argc, &argv);
    agent_init(argc, argv);
    test();
    agent_exit();
    return 0;
}


/*
 * Parse arguments.
 */
static void
parse(int *argcp, char ***argvp)
{
    char **argr = (*argvp)+1;
    char **argw = argr;

    MsgSize = 2000;
    Threads = 2;
    MsgCount = 10;
    for (;;) {
        char *arg = *argr++;

        if (!arg)
            break;
        if (streq(arg, "-i") || streq(arg, "--iterations"))
            MsgCount = numarg(arg, *argr++, 1);
        else if (streq(arg, "-h") || streq(arg, "--help"))
            usage();
        else if (streq(arg, "-s") || streq(arg, "--size"))
            MsgSize = numarg(arg, *argr++, 1);
        else if (streq(arg, "-t") || streq(arg, "--threads"))
            Threads = numarg(arg, *argr++, 1);
        else
            *argw++ = arg;
    }
    *argw = NULL;
    *argcp = argw - *argvp;
}


/*
 * Return a numeric argument.
 */
static int
numarg(char *opt, char *arg, int min)
{
    int n;

    if (!arg)
        panic("%s requires an argument", opt);
    n = atoi(arg);
    if (n < min)
        panic("value to %s must be at least %d", opt, min);
    return n;
}


/*
 * Print out a usage message and exit.
 */
static void
usage(void)
{
    char *s =
        "Usage\n"
        "    tm_orig Options\n"
        "Options\n"
        "    --help|-h\n"
        "        Print this message.\n"
        "    --iterations|-i N\n"
        "        Cause the test to run for N iterations.\n"
        "    --msg_class N\n"
        "        Set the class to N.\n"
        "    --msg_debug N\n"
        "        Set debug level to N.\n"
        "    --msg_nodes N\n"
        "        Expect N nodes.\n"
        "    --size N|-s\n"
        "        Use a message size of N nodes.\n"
        "    --threads N|-t\n"
        "        Use N worker threads.\n"
        "    -y[s][tdiwef]\n"
        "        Set SDF logging level.\n";
    fputs(s, stderr);
    plat_exit(0);
}


/*
 * Start the test.
 */
static void
test(void)
{
    int nonodes = sdf_msg_numranks();

    /* Print out welcoming message */
    t_user("%d nodes, %d threads, %d messages, %d bytes/message",
                                    nonodes, Threads, MsgCount, MsgSize);

    /* Determine our destination node */
    MyNode = sdf_msg_myrank();
    if (MyNode >= nonodes)
        ToNode = MyNode;
    else {
        ToNode = MyNode  + 1;
        if (ToNode >= nonodes)
            ToNode = 0;
    }

    /* Allocate our buffer and put some data in it */
    Data = m_malloc(MsgSize, "main:N*char");
    for (int i = 0; i < MsgSize; i++)
        Data[i] = i;

    /* Start up fthreads */
    fthInit();

    /* Create queue pairs */
    ConsQueue = create_queue(SDF_CONSISTENCY);
    RespQueue = create_queue(SDF_RESPONSES);

    /* Start up a pthread */
    {
        size_t stacksize;
        pthread_attr_t attr;
        pthread_t pthread;

        if (pthread_attr_init(&attr) < 0)
            fatal("pthread_attr_init failed");
        if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE) < 0)
            fatal("pthread_attr_setdetachstate failed");
        if (pthread_attr_getstacksize(&attr, &stacksize) < 0)
            fatal("pthread_attr_getstacksize failed");
        if (pthread_create(&pthread, &attr, &fth_pthread, NULL) < 0)
            fatal("pthread_create failed");
        pthread_join(pthread, NULL);
    }

    /* Clean up */
    m_free(Data);
}


/*
 * Create a queue pair.
 */
static qpair_t *
create_queue(service_t proto)
{
    qpair_t *s;

    s = sdf_create_queue_pair(MyNode, ToNode, proto, proto, SDF_WAIT_FTH);
    if (!s)
        fatal("failed to create queue pair %d", proto);
    return s;
}


/*
 * It appears that this all must be called from a pthread.  When I attempt to
 * call it directly, it sometimes dies.
 */
void *
fth_pthread(void *arg)
{
    int i;

    fthResume(fthSpawn(&fth_master, STACK_SIZE), 0);
    for (i = 0; i < Threads; i++)
        fthResume(fthSpawn(&fth_worker, STACK_SIZE), 0);
    fthSchedulerPthread(0);
    return NULL;
}


/*
 * This fthread sends a CONSISTENCY message and sleeps on a mailbox waiting for
 * a response.  It repeats this process a fixed number of times.
 */
static void
fth_master(uint64_t arg)
{
    int i;
    fthMbox_t mbox_ack;
    fthMbox_t mbox_resp;
    sdf_fth_mbx_t fthmbx;
    service_t protocol = SDF_CONSISTENCY;

    /* Initialize acknowledge and response mailbox */
    fthMboxInit(&mbox_ack);
    fthMboxInit(&mbox_resp);

    /* Set up mailbox parameters for messaging */
    memset(&fthmbx, 0, sizeof(fthmbx));
    fthmbx.actlvl = SACK_BOTH_FTH;
    fthmbx.abox   = &mbox_ack;
    fthmbx.rbox   = &mbox_resp;

    /* Send messages */
    for (i = 0; i < MsgCount; ++i) {
        sdf_msg_t *msg;

        send_msg(Data, MsgSize, protocol, REQ_FLUSH, &fthmbx, NULL);
        msg = (sdf_msg_t *)fthMboxWait(&mbox_resp);
        show_recv("resp", msg);
        if (msg->msg_type == SDF_MSG_ERROR)
            fatal("message timed out");
        if (msg->msg_len - sizeof(*msg) != MsgSize)
            fatal("response data length is incorrect");
        if (memcmp(msg->msg_payload, Data, MsgSize) != 0)
            fatal("response data is incorrect");
        sdf_msg_free(msg);
        fthYield(0);
    }

    /* Tell the workers its quitting time and wait for them to finish */
    for (i = 0; i < Threads; ++i)
        send_msg(Data, 0, protocol, REQ_FLUSH, &fthmbx, NULL);
    while (Done != Threads)
        fthYield(0);

    /* Terminate the fth mailboxes */
    fthMboxTerm(&mbox_ack);
    fthMboxTerm(&mbox_resp);

    t_user("master sent %d messages", MsgCount);
    fthKill(1);
}


/*
 * This fthread waits for consistency messages on a queue and returns RESPONSE
 * messages.
 */
static void
fth_worker(uint64_t arg)
{
    fthMbox_t mbox_ack;
    sdf_fth_mbx_t fthmbx;
    int count = 0;
    service_t protocol = SDF_RESPONSES;

    /* Set up mailbox information */
    fthMboxInit(&mbox_ack);
    memset(&fthmbx, 0, sizeof(fthmbx));
    fthmbx.actlvl = SACK_ONLY_FTH;
    fthmbx.abox = &mbox_ack;

    for (;;) {
        int len;
        sdf_msg_t *msg;
        sdf_resp_mbx_t respmbx;

        /* Receive a message on the queue */
        msg = sdf_msg_recv(ConsQueue);
        show_recv("recv", msg);
        len = msg->msg_len - sizeof(*msg);
        if (!len) {
            sdf_msg_free(msg);
            break;
        }
        count++;

        /* Send a response */
        sdf_msg_initmresp(&respmbx);
        send_msg(msg->msg_payload, len, protocol, RESP_ONE, &fthmbx,
                 sdf_msg_get_response(msg, &respmbx));

        /* Release receive buffer */
        sdf_msg_free(msg);

        /* Allow others to run */
        fthYield(0);
    }

    /* Terminate the fth mailbox */
    fthMboxTerm(&mbox_ack);

    t_user("worker received %d messages", count);
    Done++;
}


/*
 * Send a message.
 */
static void
send_msg(char *buf, uint32_t len, service_t p,
         msg_type_t type, sdf_fth_mbx_t *fthmbx, sdf_resp_mbx_t *respmbx)
{
    int s;
    sdf_msg_t *msg = sdf_msg_alloc(len);

    if (!msg)
        fatal("sdf_msg_alloc(%d) failed", len);
    memcpy(msg->msg_payload, buf, len);

    t_ubug("send %d.%02d => %d.%02d len=%d", MyNode, p, ToNode, p, len);
    s = sdf_msg_send(msg, len, ToNode, p, MyNode, p, type, fthmbx, respmbx);
    if (s < 0)
        fatal("sdf_msg_send failed");

    fthMboxWait(fthmbx->abox);
    sdf_msg_free(msg);
}


/*
 * Show a message that has been received.
 */
static void
show_recv(char *type, sdf_msg_t *msg)
{
    t_ubug("%s %d.%02d <= %d.%02d len=%ld",
            type, msg->msg_dest_vnode, msg->msg_dest_service,
            msg->msg_src_vnode, msg->msg_src_service,
            msg->msg_len - sizeof(*msg));
}
