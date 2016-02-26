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
 * File: multi_comm.c
 * Author: Norman Xu
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * 
 * A test to send a variety of message from one nodes to others using three fth threads
 * each node.
 */
#include <stdio.h>
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdftcp/msg_map.h"
#include "sdftcp/tools.h"
#include "sdftcp/trace.h"
#include "agent.h"


/*
 * Configurable parameters.
 */
#define STACK_SIZE  (128*1024)
#define MAX_NODES	8

/*
 * For convenience.
 */
#define streq(a, b) (strcmp(a, b) == 0)
typedef struct sdf_queue_pair qpair_t;


/*
 * Static variables.
 */
static int       Done[MAX_NODES - 1];
static int       Done_Master;
static int       MyNode;
static int       ToNodes[MAX_NODES - 1];
static int       MsgSize;
static int       Threads;
static int 		 Nodes;
static int       MsgCount;
static qpair_t  *ConsQueue[MAX_NODES - 1];
static qpair_t  *RespQueue[MAX_NODES - 1];
static char     *Data;


/*
 * Function prototypes.
 */
static qpair_t *create_queue(int tonode, service_t proto);
static void     fth_master(uint64_t arg);
static void     fth_worker(uint64_t arg);
static void    *fth_pthread(void *arg);
static void     send_msg(char *buf, uint32_t len, int destnode, service_t, 
                         msg_type_t, sdf_fth_mbx_t *fthmbx, 
                         sdf_resp_mbx_t *respmbx);
static int      numarg(char *opt, char *arg, int min);
static void     parse(int *argcp, char ***argvp);
static void     show_recv(char *type, sdf_msg_t *msg);
static void     test(void);
static void     usage(void);

/*
 * Main
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
 * Parse arguments
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
    	else if (streq(arg, "-m") || streq(arg, "--nodes"))
			Nodes = numarg(arg, *argr++, 1);
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
        "        Set the debug mode to N.\n"
        "    --msg_nodes N\n"
        "        Assume the test is running on N nodes.\n"
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
 * Start the test
 */
static void
test(void)
{
	char name[64];
    int i;
	int nonodes = sdf_msg_noranks();
	
	/* Print out welcoming message */
	t_user("%d nodes, %d threads, %d messages, %d bytes/message", 
							nonodes, Threads, MsgCount, MsgSize);

    if (nonodes > MAX_NODES) 
        t_user("%d exceeds the maximum node number %d", 
                            nonodes, MAX_NODES);
	
    /* Determine our destination node */
    /* It is more complex with the dual nodes testing program */
    MyNode = sdf_msg_myrank();
    if (MyNode >= nonodes) {
        fatal("MyNode %d shouldn't exceed total number %d", MyNode, nonodes);
    }
    for (i = 1; i < nonodes; i++) {
        ToNodes[i - 1] = (MyNode + i) % nonodes;
    }

    t_user("My ID is %d Other guys are", MyNode);
    for (i = 0; i < nonodes - 1; i++) {
        t_user("mine is %d", ToNodes[i]);
    }

	/* Name our node for debugging */
	snprintf(name, sizeof(name), "n%d", MyNode);
	trace_setnode(name);

	/* Create queue pairs */
    for (i = 0; i < nonodes - 1; i++) {
        ConsQueue[i] = create_queue(ToNodes[i], SDF_CONSISTENCY);
        RespQueue[i] = create_queue(ToNodes[i], SDF_RESPONSES);
        Done[i] = 0;
    }
    Done_Master = 0;
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
}


/*
 * Create a queue pair
 */
static qpair_t *
create_queue(int tonode, service_t proto)
{
    qpair_t *s;

    s = sdf_create_queue_pair(MyNode, tonode, proto, proto, SDF_WAIT_FTH);
    if (!s)
        fatal("failed to create queue pair %d for node id %d", proto, tonode);
    return s;
}


/*
 * It appears that this all must be called from a pthread.  When I attempt to
 * call it directly, it sometimes dies.
 */
void *
fth_pthread(void *arg)
{
    int i, k;
    int nonodes = sdf_msg_noranks(); 
    for (k = 0; k < nonodes - 1; k++) {
        fthResume(fthSpawn(&fth_master, STACK_SIZE), k);
        for (i = 0; i < Threads; i++)
            fthResume(fthSpawn(&fth_worker, STACK_SIZE), k);
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
    int index = (int) arg;
    fthMbox_t mbox_ack;
    fthMbox_t mbox_resp;
    sdf_fth_mbx_t fthmbx;
    serviceid_t protocol = SDF_CONSISTENCY;
    
    /* Initialize acknowledge and response mailbox */
    fthMboxInit(&mbox_ack);
    fthMboxInit(&mbox_resp);

    /* Set up mailbox parameters for messaging */
    memset(&fthmbx, 0, sizeof(fthmbx));
    fthmbx.actlvl = SACK_BOTH_FTH;
    fthmbx.abox   = &mbox_ack;
    fthmbx.rbox   = &mbox_resp;

    t_user("Master for node %d come up!", ToNodes[index]);
    /* Send messages */
    for (i = 0; i < MsgCount; ++i) {
        sdf_msg_t *msg;

        send_msg(Data, MsgSize, ToNodes[index], protocol, REQ_FLUSH, &fthmbx, NULL);
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
        send_msg(Data, 0, ToNodes[index], protocol, REQ_FLUSH, &fthmbx, NULL);
    while (Done[index] != Threads)
        fthYield(0);

    /* Terminate the fth mailboxes */
    fthMboxTerm(&mbox_ack);
    fthMboxTerm(&mbox_resp);

    t_user("master sent %d messages", MsgCount);
    Done_Master++;
    while (Done_Master != sdf_msg_noranks() - 1) 
        fthYield(0);
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
    int index = (int) arg;
    sdf_fth_mbx_t fthmbx;
    int count = 0;
    serviceid_t protocol = SDF_RESPONSES;

    /* Set up mailbox information */
    fthMboxInit(&mbox_ack);
    memset(&fthmbx, 0, sizeof(fthmbx));
    fthmbx.actlvl = SACK_BOTH_FTH;
    fthmbx.abox = &mbox_ack;

    t_user("Worker for node %d come up!", ToNodes[index]);
    for (;;) {
        int len;
        sdf_msg_t *msg;
        struct sdf_resp_mbx respmbx;

        /* Receive a message on the queue */
        msg = sdf_msg_recv(ConsQueue[index]);
        show_recv("recv", msg);
        len = msg->msg_len - sizeof(*msg);
        if (!len) {
            sdf_msg_free(msg);
            break;
        }
        count++;

        /* Send a response */
        sdf_msg_initmresp(&respmbx);
        send_msg(msg->msg_payload, len, ToNodes[index], protocol, RESP_ONE, &fthmbx,
                 sdf_msg_get_response(msg, &respmbx));

        /* Release receive buffer */
        sdf_msg_free(msg);

        /* Allow others to run */
        fthYield(0);
    }

    /* Terminate the fth mailbox */
    fthMboxTerm(&mbox_ack);

    t_user("worker received %d messages", count);
    Done[index]++;
}


/*
 * Send a message.
 */
static void
send_msg(char *buf, uint32_t len, int destnode, service_t p,
         msg_type_t type, sdf_fth_mbx_t *fthmbx, sdf_resp_mbx_t *respmbx)
{
    int s;
    sdf_msg_t *msg = sdf_msg_alloc(len);

    if (!msg)
        fatal("sdf_msg_alloc(%d) failed", len);
    memcpy(msg->msg_payload, buf, len);

    t_ubug("send %d.%02d => %d.%02d len=%d", MyNode, p, destnode, p, len);
    s = sdf_msg_send(msg, len, destnode, p, MyNode, p, type, fthmbx, respmbx);
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
    t_ubug("%s %d.%02d <= %d.%02d len=%d",
            type, msg->msg_dest_vnode, msg->msg_dest_service,
            msg->msg_src_vnode, msg->msg_src_service,
            msg->msg_len - sizeof(*msg));
}
