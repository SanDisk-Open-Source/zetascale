/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: sdf_bounce.c
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Send a variety of messages between multiple nodes to test out the SDF
 * messaging system.
 */
#include <stdio.h>
#include <arpa/inet.h>
#include "sdftcp/tools.h"
#include "sdftcp/trace.h"
#include "sdftcp/msg_cat.h"
#include "sdftcp/msg_map.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_sync.h"
#include "agent.h"


/*
 * Configurable parameters.
 */
#define MAGIC       0xFFFF
#define STACK_SIZE  (128*1024)
#define MPROTO      SDF_CONSISTENCY
#define WPROTO      SDF_RESPONSES


/*
 * For convenience.
 */
#define streq(a, b) (strcmp(a, b) == 0)
typedef struct sdf_queue_pair qpair_t;


/*
 * Node relevant information.
 */
typedef struct node {
    qpair_t  *consq;
    qpair_t  *respq;
} node_t;


/*
 * For flagging the workers.
 */
typedef struct flag {
    uint16_t  magic;
    uint16_t  value;
} flag_t;


/*
 * Static variables.
 */
static int       NodeN;
static int       MyNode;
static int       MsgSize;
static int       Workers;
static int       MsgCount;
static int       MasterDone;
static int       WorkerDone;
static char     *Data;
static node_t   *Nodes;


/*
 * Function prototypes.
 */
static void     test(void);
static void     usage(void);
static void    *fth_pthread(void *arg);
static void     fth_master(uint64_t nodei);
static void     fth_worker(uint64_t nodei);
static void     parse(int *argcp, char ***argvp);
static void     term_fthmbox(sdf_fth_mbx_t *sfm);
static void     set_name(int master, int n, int i);
static void     show_recv(char *type, sdf_msg_t *msg);
static void     flag_quit(int nodei, sdf_fth_mbx_t *sfm);
static void     send_msg(int nodei, void *buf, uint32_t len, service_t,
                                    sdf_fth_mbx_t *, sdf_resp_mbx_t *respmbx);
static void     init_fthmbox(sdf_fth_mbx_t *sfm,
                                            fthMbox_t *abox, fthMbox_t *rbox);
static int      numarg(char *opt, char *arg, int min);
static int      flag_done(int nodei, sdf_msg_t *msg, sdf_fth_mbx_t *sfm);
static qpair_t *create_queue(int nodei, service_t proto);


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

    MsgSize = 4096;
    Workers = 1;
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
        else if (streq(arg, "-w") || streq(arg, "--workers"))
            Workers = numarg(arg, *argr++, 1);
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
        "    sdf_bounce Options\n"
        "Options\n"
        "    --help|-h\n"
        "        Print this message.\n"
        "    --iterations|-i N\n"
        "        Cause the test to run for N iterations.\n"
        "    --msg_alias NAME\n"
        "        Set our node alias to NAME.\n"
        "    --msg_class N\n"
        "        Set the class to N.  N must be a positive integer\n"
        "    --msg_debug N\n"
        "        Set debug level to N.\n"
        "    --msg_iface I1:I2:...:IN\n"
        "        Use only the given interfaces.\n"
        "    --msg_nodes N\n"
        "        Expect N nodes.\n"
        "    --msg_tcp PORT\n"
        "        Use TCP port PORT.\n"
        "    --msg_udp PORT\n"
        "        Use UDP port PORT.\n"
        "    --size N|-s\n"
        "        Use a message size of N nodes.\n"
        "    --workers N|-w\n"
        "        Use N worker threads to service each node.\n"
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
    int i;
    node_t *p;

    /* Determine node information */
    NodeN = sdf_msg_numranks();
    MyNode = sdf_msg_myrank();

    /* Print out welcoming message */
    t_user("%d nodes, %d workers, %d messages, %d bytes/message",
                                    NodeN, Workers, MsgCount, MsgSize);

    /* Allocate node structure */
    Nodes = m_malloc(NodeN * sizeof(*Nodes), "main:Nodes");
    memset(Nodes, 0, NodeN * sizeof(*Nodes));

    /* Create queue pairs */
    for (i = 0; i < NodeN; i++) {
        if (i == MyNode)
            continue;
        p = &Nodes[i];
        p->consq = create_queue(i, MPROTO);
        p->respq = create_queue(i, WPROTO);
    }
    sdf_msg_sync();

    /* Allocate our buffer and put some data in it */
    Data = m_malloc(MsgSize, "main:N*char");
    for (int i = 0; i < MsgSize; i++)
        Data[i] = i;

    /* Start up fthreads */
    fthInit();

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
    m_free(Nodes);
}


/*
 * Create a queue pair.
 */
static qpair_t *
create_queue(int nodei, service_t proto)
{
    qpair_t *s;

    t_ubug("creating queue %d => %d (%d)", MyNode, nodei, proto);
    s = sdf_create_queue_pair(MyNode, nodei, proto, proto, SDF_WAIT_FTH);
    if (!s) {
        fatal("failed to create queue pair (%d, %d, %d, %d",
                                            MyNode, nodei, proto, proto);
    }
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
    int j;

    for (i = 0; i < NodeN; i++) {
        if (i == MyNode)
            continue;
        fthResume(fthSpawn(&fth_master, STACK_SIZE), i);
        for (j = 0; j < Workers; j++)
            fthResume(fthSpawn(&fth_worker, STACK_SIZE), i*Workers+j);
    }
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
    fthMbox_t abox;
    fthMbox_t rbox;
    sdf_fth_mbx_t fthmbx;
    int nodei = arg;

    set_name(1, arg, 0);
    init_fthmbox(&fthmbx, &abox, &rbox);

    /* Send messages */
    for (i = 0; i < MsgCount; ++i) {
        sdf_msg_t *msg;

        send_msg(nodei, Data, MsgSize, MPROTO, &fthmbx, NULL);
        msg = (sdf_msg_t *)fthMboxWait(&rbox);
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

    flag_quit(nodei, &fthmbx);
    term_fthmbox(&fthmbx);
    t_user("master sent %d messages", MsgCount);
    MasterDone++;

    if (nodei == !MyNode) {
        int masters = NodeN - 1;
        int workers = Workers * (NodeN-1);

        while (MasterDone != masters || WorkerDone != workers)
            fthYield(0);
        fthKill(1);
    }
}


/*
 * This fthread waits for Consistency messages on a queue and returns Response
 * messages.
 */
static void
fth_worker(uint64_t arg)
{
    fthMbox_t abox;
    sdf_fth_mbx_t fthmbx;
    int count = 0;
    int nodei = arg / Workers;

    set_name(0, nodei, arg%Workers);
    init_fthmbox(&fthmbx, &abox, NULL);

    for (;;) {
        int len;
        sdf_msg_t *msg;
        struct sdf_resp_mbx respmbx;

        /* Receive a message on the queue */
        msg = sdf_msg_recv(Nodes[nodei].consq);
        show_recv("recv", msg);

        /* See if we are done */
        if (flag_done(nodei, msg, &fthmbx)) {
            sdf_msg_free(msg);
            break;
        }

        /* Send a response */
        count++;
        len = msg->msg_len - sizeof(*msg);
        sdf_msg_initmresp(&respmbx);
        send_msg(nodei, msg->msg_payload, len, WPROTO, &fthmbx,
                                        sdf_msg_get_response(msg, &respmbx));

        /* Release receive buffer */
        sdf_msg_free(msg);

        /* Allow others to run */
        fthYield(0);
    }

    term_fthmbox(&fthmbx);
    t_user("worker received %d messages", count);
    WorkerDone++;
}


/*
 * Send a quit message to the workers.  We sent a quit message to one worker
 * who responds with how many other workers there are.  Then we send a quit
 * message to the remaining workers.
 */
static void
flag_quit(int nodei, sdf_fth_mbx_t *sfm)
{
    int i;
    uint16_t n;
    sdf_msg_t *msg;
    flag_t f ={MAGIC, 1};

    f.magic = htons(f.magic);
    f.value = htons(f.value);
    send_msg(nodei, &f, sizeof(f), MPROTO, sfm, NULL);
    msg = (sdf_msg_t *)fthMboxWait(sfm->rbox);
    show_recv("resp", msg);

    n = ntohs(*((uint16_t *)msg->msg_payload));
    sdf_msg_free(msg);
    f.value = htons(0);
    for (i = 0; i < n; ++i)
        send_msg(nodei, &f, sizeof(f), MPROTO, sfm, NULL);
}


/*
 * If the master sent us a done message, we should return true so the caller
 * knows to quit.  Also, if the associated value is non-zero, we need to let
 * the master know how many other workers there are.
 */
static int
flag_done(int nodei, sdf_msg_t *msg, sdf_fth_mbx_t *sfm)
{
    flag_t *f;
    int len = msg->msg_len - sizeof(*msg);

    if (len != sizeof(flag_t))
        return 0;

    f = (flag_t *) msg->msg_payload;
    if (ntohs(f->magic) != MAGIC)
        return 0;

    f->value = ntohs(f->value);
    if (f->value) {
        struct sdf_resp_mbx respmbx;
        uint16_t n = htons(Workers - 1);

        sdf_msg_initmresp(&respmbx);
        send_msg(nodei, &n, sizeof(n), WPROTO,
                    sfm, sdf_msg_get_response(msg, &respmbx));
    }
    return 1;
}


/*
 * Set thread name for debugging.
 */
static void
set_name(int master, int n, int i)
{
    char buf[64];

    if (master)
        snprintf(buf, sizeof(buf), "m%d", n);
    else {
        if (Workers <= 1)
            snprintf(buf, sizeof(buf), "w%d", n);
        else if (Workers <= 26)
            snprintf(buf, sizeof(buf), "w%d%c", n, 'a' + i);
        else
            snprintf(buf, sizeof(buf), "w%d.%d", n, i);
    }
    trace_setfth(buf);
}


/*
 * Initialize a fth mailbox.
 */
static void
init_fthmbox(sdf_fth_mbx_t *sfm, fthMbox_t *abox, fthMbox_t *rbox)
{
    if (abox)
        fthMboxInit(abox);
    if (rbox)
        fthMboxInit(rbox);

    memset(sfm, 0, sizeof(*sfm));
    sfm->actlvl = rbox ? SACK_BOTH_FTH : SACK_ONLY_FTH;
    sfm->abox = abox;
    sfm->rbox = rbox;
}


/*
 * Terminate a fth mailbox.
 */
static void
term_fthmbox(sdf_fth_mbx_t *sfm)
{
    if (sfm->abox)
        fthMboxTerm(sfm->abox);
    if (sfm->rbox)
        fthMboxTerm(sfm->rbox);
}


/*
 * Send a message.
 */
static void
send_msg(int tonode, void *buf, uint32_t len, service_t p,
         sdf_fth_mbx_t *sfm, sdf_resp_mbx_t *respmbx)
{
    int s;
    sdf_msg_t *msg = sdf_msg_alloc(len);

    if (!msg)
        fatal("sdf_msg_alloc(%d) failed", len);
    memcpy(msg->msg_payload, buf, len);

    t_ubug("send %d.%02d => %d.%02d len=%d", MyNode, p, tonode, p, len);
    s = sdf_msg_send(msg, len, tonode, p, MyNode, p, 0, sfm, respmbx);
    if (s < 0)
        fatal("sdf_msg_send failed");
    fthMboxWait(sfm->abox);
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
