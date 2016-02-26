/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: sdf_live.c
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Test out liveness.
 */
#include <stdio.h>
#include <unistd.h>
#include "sdftcp/locks.h"
#include "sdftcp/stats.h"
#include "sdftcp/tools.h"
#include "sdftcp/trace.h"
#include "sdftcp/msg_cat.h"
#include "sdftcp/msg_map.h"
#include "sdftcp/msg_msg.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_sync.h"
#include "agent.h"


/*
 * Configurable parameters.
 */
#define NNODES      64
#define STACK_SIZE  (128*1024)
#define PROTOCOL    SDF_CONSISTENCY


/*
 * Useful definitions.
 */
#define streq(a, b) (strcmp(a, b) == 0)
typedef struct sdf_queue_pair qpair_t;


/*
 * Type definitions.
 */
typedef sdf_fth_mbx_t  sfm_t;
typedef sdf_resp_mbx_t srm_t;
typedef void fthfunc_t(uint64_t);


/*
 * Meta data in the message.
 */
typedef struct meta {
    atom_t oseqn;                       /* Order sequence number */
    atom_t aseqn;                       /* Request acknowledge */
} meta_t;


/*
 * Nodes.
 *
 *  abox  - Acknowledge fth mailbox.
 *  brecv - Number of bytes received.
 *  bsent - Number of bytes sent.
 *  hlen  - Header length.
 *  init  - We have been initialized.
 *  lock  - Read/write lock to know when we can shut down a node.
 *  live  - Node is alive.
 *  mfail - Number of messages failed to send.
 *  mlen  - Message length.
 *  mrecv - Number of messages received.
 *  msent - Number of messages sent.
 *  qackd - Last sequence number acknowledged.
 *  qpost - Last sequence number posted.
 *  qsent - Last sequence number sent.
 *  rseqn - Last ordered sequence number that we received.
 *  sseqn - Last ordered sequence number that we sent.
 *  sseqn - Sequence number sent.
 *  time  - Time node became alive.
 */
typedef struct node {
    int         live;
    rwlock_t   *lock;
    fthMbox_t   abox;
    ntime_t     time;
    int         hlen;
    int         mlen;
    struct {
        atom_t  sseqn;
        atom_t  rseqn;
        atom_t  qpost;
        atom_t  qsent;
        atom_t  qackd;
        int64_t msent;
        int64_t mfail;
        int64_t mrecv;
        int64_t bsent;
        int64_t brecv;
    } s;
} node_t;


/*
 * Configurable static variables.
 *
 *  AckFreq  - The frequency that we request acks.
 *  MsgSize  - The size of the messages we send.
 *  NoCheck  - Do not check sequence numbers.
 *  Duration - Length of time that we run.  If 0, we run forever.
 *  NRecvThr - The number of receive threads.  Not yet configurable.
 *  NSendThr - The number of send threads.  Not yet configurable.
 *  PostQLen - The maximum number of messages we can post until the message is
 *             sent.  If 0, there is no limit.
 *  SentQLen - The maximum number of messages we can send since we received an
 *             ack.  If 0, there is no limit.
 *  WaitTime - Time in seconds to pause between servicing each node.
 */
static int     AckFreq;
static int     MsgSize;
static int     NoCheck;
static int     Duration;
static int     NSendThr;
static int     NRecvThr;
static int     PostQLen;
static int     SentQLen;
static int     WaitTime;


/*
 * Static variables.
 */
static int    Done;
static int    MyRank;
static node_t Nodes[NNODES];


/*
 * Function prototypes.
 */
static int      numarg(char *opt, char *arg, int min);
static void     test(void);
static void     usage(void);
static void     recvonly(uint64_t arg);
static void     sendonly(uint64_t arg);
static void     node_dead(node_t *node);
static void     node_live(node_t *node);
static void     our_stats(stat_t *stat);
static void     count_sent(node_t *node);
static void     count_ackd(fthMbox_t *rbox);
static void     parse(int *argcp, char ***argvp);
static void     show_stats(int rank, ntime_t etime);
static void     livefunc(int live, int rank, void *arg);
static void     send_len(node_t *node, fthMbox_t *rbox);
static void     send_ack(node_t *node, srm_t *srm, atom_t aseqn);
static void     send_out(node_t *node, sdf_msg_t *msg,
                         int len, sfm_t *sfm, srm_t *srm);
static void     recv_msg(qpair_t *qpair, int *rankp, int *sizep,
                         atom_t *aseqnp, atom_t *oseqnp, srm_t *srm);
static void    *pth_main(void *arg);
static qpair_t *create_queue(service_t proto);


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

    for (;;) {
        char *arg = *argr++;

        if (!arg)
            break;
        if (streq(arg, "-h") || streq(arg, "--help"))
            usage();
        else if (streq(arg, "-af") || streq(arg, "--ack_freq"))
            AckFreq = numarg(arg, *argr++, 1);
        else if (streq(arg, "-ms") || streq(arg, "--msg_size"))
            MsgSize = numarg(arg, *argr++, 1);
        else if (streq(arg, "-nc") || streq(arg, "--no_check"))
            NoCheck = 1;
        else if (streq(arg, "-pq") || streq(arg, "--post_qlen"))
            PostQLen = numarg(arg, *argr++, 0);
        else if (streq(arg, "-rt") || streq(arg, "--recv_threads"))
            NRecvThr = numarg(arg, *argr++, 0);
        else if (streq(arg, "-sq") || streq(arg, "--sent_qlen"))
            SentQLen = numarg(arg, *argr++, 0);
        else if (streq(arg, "-st") || streq(arg, "--send_threads"))
            NSendThr = numarg(arg, *argr++, 0);
        else if (streq(arg, "-t") || streq(arg, "--time"))
            Duration = numarg(arg, *argr++, 1);
        else if (streq(arg, "-w") || streq(arg, "--wait_time"))
            WaitTime = numarg(arg, *argr++, 1);
        else
            *argw++ = arg;
    }
    *argw = NULL;
    *argcp = argw - *argvp;

    if (NSendThr && !MsgSize)
        panic("must set --msg_size (-msg) if --send_threads (-st) is set");
    if (!NSendThr && MsgSize)
        NSendThr = 1;
    if (!NRecvThr)
        NRecvThr = 1;
    if (!SentQLen)
        SentQLen = 8192;
    if (!AckFreq)
        AckFreq = 1024;
    NoCheck = 1;
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
        "    sdf_live Options\n"
        "Options\n"
        "    --help|-h\n"
        "        Print this message.\n"
        "    --ack_freq N|-af\n"
        "        The frequency that we request acks\n"
        "    --msg_size N|-ms\n"
        "        Use a message size of N bytes.\n"
        "    --no_check|-nc\n"
        "        Do not check sequence numbers.\n"
        "    --post_qlen N|-pq\n"
        "        The maximum number of messages we can have posted waiting\n"
        "        to be sent.\n"
        "    --recv_threads N|-rt\n"
        "        Set number of receive threads.\n"
        "    --send_threads N|-st\n"
        "        Set number of send threads.\n"
        "    --sent_qlen N|-sq\n"
        "        The maximum number of messages we can send since the last\n"
        "        ack.\n"
        "    --time N|-t\n"
        "        Make test last N seconds.\n"
        "    --wait_time N|-w\n"
        "        Time in seconds to wait before servicing each node.\n"
        "        ack.\n";
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
    int n = NSendThr + NRecvThr;
    pthread_t *pth = malloc_q(n * sizeof(*pth));

    fthInitMultiQ(1, n);
    MyRank = sdf_msg_myrank();
    sdf_msg_call_stat(our_stats);

    for (i = 0; i < NNODES; i++) {
        node_t *node = &Nodes[i];
        node->lock = rwl_init();
        fthMboxInit(&node->abox);
    }
    msg_livecall(1, 1, livefunc, NULL);

    for (i = 0; i < NSendThr; i++)
        XResume(fthSpawn(sendonly, STACK_SIZE), 0);
    for (i = 0; i < NRecvThr; i++)
        XResume(fthSpawn(recvonly, STACK_SIZE), 0);

    for (i = 0; i < n; i++)
        if (pthread_create(&pth[i], NULL, &pth_main, NULL) < 0)
            fatal("pthread_create failed");

    if (Duration) {
        sleep(Duration);
        Done = 1;
        fthKill(n);
    }

    for (i = 0; i < n; i++)
        pthread_join(pth[i], NULL);
    plat_free(pth);

    for (i = 0; i < NNODES; i++) {
        node_t *node = &Nodes[i];
        rwl_free(node->lock);
        fthMboxTerm(&node->abox);
    }

    if (0)
        show_stats(0, 0);
}


/*
 * Liveness callback function.
 */
static void
livefunc(int live, int rank, void *arg)
{
    node_t *node;

    if (rank >= NNODES)
        return;

    node = &Nodes[rank];
    if (live)
        node_live(node);
    else
        node_dead(node);
}


/*
 * A node has just become alive.
 */
static void
node_live(node_t *node)
{
    int hlen = 0;
    int mlen = 0;
    int rank = node - Nodes;

    t_user("n%d is live", rank);

    if (node->live)
        panic("node was already alive");

    rwl_lockw(node->lock);
    node->live = 1;
    node->time = msg_ntime();

    if (MsgSize) {
        hlen = sdf_msg_hlen(rank);
        if (hlen < 0)
            panic("cannot determine message header size");

        mlen = MsgSize - node->hlen;
        if (mlen < (int)sizeof(meta_t)) {
            mlen = sizeof(meta_t);
            t_user("message size too small for n%d"
                   "; using %d instead", rank, mlen+node->hlen);
        }
    }
    node->hlen = hlen;
    node->mlen = mlen;
    memset(&node->s, 0, sizeof(node->s));
    rwl_unlockw(node->lock);
}


/*
 * A node has just died.
 */
static void
node_dead(node_t *node)
{
    int rank = node - Nodes;

    t_user("n%d is dead", rank);
    rwl_lockw(node->lock);
    node->live = 0;
    rwl_unlockw(node->lock);
}


/*
 * Start a fth scheduler.
 */
static void *
pth_main(void *arg)
{
    fthSchedulerPthread(0);
    return NULL;
}


/*
 * Send messages to the other side.
 */
static void
sendonly(uint64_t arg)
{
    int rank;
    fthMbox_t rbox;

    use_all_cpus();
    fthMboxInit(&rbox);
    while (!Done) {
        for (rank = 0; rank < NNODES; rank++) {
            node_t *node = &Nodes[rank];
            if (rank == MyRank)
                continue;
            if (!node->live)
                continue;

            if (node->s.qpost - node->s.qsent >= PostQLen)
                count_sent(node);
            if (node->s.qpost - node->s.qackd >= SentQLen)
                count_ackd(&rbox);

            if (PostQLen && node->s.qpost - node->s.qsent >= PostQLen)
                continue;
            if (SentQLen && node->s.qpost - node->s.qackd >= SentQLen)
                continue;

            if (!rwl_tryr(node->lock))
                continue;
            send_len(node, &rbox);
            rwl_unlockr(node->lock);
        }
        if (WaitTime)
            sleep(WaitTime);
    }
}


/*
 * Count any messages that have actually been sent.
 */
static void
count_sent(node_t *node)
{
    for (;;) {
        int64_t s = sdf_msg_mbox_try(&node->abox);
        if (!s)
            break;

        node->s.qsent++;
        if (s > 0)
            node->s.msent++;
        if (s < 0)
            node->s.mfail++;
    }
}


/*
 * Count any messages that have actually been acknowledged by the other side.
 */
static void
count_ackd(fthMbox_t *rbox)
{
    for (;;) {
        int len;
        int rank;
        atom_t seqn;
        meta_t *meta;
        node_t *node;
        sdf_msg_t *msg = (sdf_msg_t *) sdf_msg_mbox_try(rbox);

        if (!msg)
            break;

        rank = msg->msg_src_vnode;
        if (rank < 0 || rank >= NNODES) {
            t_ubug("received ack message from unknown node: n%d", rank);
            goto err;
        }

        node = &Nodes[rank];
        len = msg->msg_len - sizeof(*msg);
        node->s.mrecv += 1;
        node->s.brecv += len;

        if (len < sizeof(meta_t)) {
            t_ubug("received truncated ack message from n%d", rank);
            goto err;
        }

        meta = (meta_t *) msg->msg_payload;
        seqn = msg_retn2h(meta->aseqn);
        node->s.qackd = seqn;
    err:
        sdf_msg_free(msg);
    }
}


/*
 * Receive messages.
 */
static void
recvonly(uint64_t arg)
{
    int rank;
    int size;
    srm_t srm;
    atom_t aseqn;
    atom_t oseqn;
    node_t *node;
    qpair_t *queue;

    use_all_cpus();
    queue = create_queue(PROTOCOL);
    sdf_msg_initmresp(&srm);
    while (!Done) {
        recv_msg(queue, &rank, &size, &aseqn, &oseqn, &srm);
        if (rank < 0 || rank >= NNODES)
            continue;
        if (rank == MyRank)
            break;
        node = &Nodes[rank];
        ++node->s.rseqn;
        if (!NoCheck && oseqn != node->s.rseqn)
            panic("from n%d: received sequence %ld expected %ld",
                    rank, oseqn, node->s.rseqn);
        if (aseqn)
            send_ack(node, &srm, aseqn);
    }
    fthKill(1);
}


/*
 * Show statistics for a particular node.
 */
static void
show_stats(int rank, ntime_t etime)
{
    double seconds;
    double sent_bytes;
    double recv_bytes;
    double sent_speed;
    double recv_speed;
    node_t *node = &Nodes[rank];
    ntime_t elapsed = etime - node->time;
    char *s = node->live ? ": " : " is dead; ";

    if (elapsed <= 0)
        return;
    seconds = (double) elapsed / NANO;
    recv_bytes = node->s.brecv;
    sent_bytes = node->s.bsent -
                 (node->s.qpost - node->s.qsent) * (node->hlen + node->mlen);
    recv_speed = recv_bytes / (1024*1024) / seconds;
    sent_speed = sent_bytes / (1024*1024) / seconds;
    t_user("n%d%stime = %.0f secs; sent = %.1f MB/sec; recv = %.1f MB/sec",
           rank, s, seconds, sent_speed, recv_speed);
}


/*
 * Show statistics.
 */
static void
our_stats(stat_t *stat)
{
    int i;

    for (i = 0; i < NNODES; i++) {
        node_t *node = &Nodes[i];

        if (!node->live)
            continue;

        stat_labn(stat, "sdf_live", i);
        stat_full(stat, "wgap", node->s.qpost - node->s.qsent);
        stat_full(stat, "agap", node->s.qpost - node->s.qackd);
        stat_endl(stat);
    }
}


/*
 * Create a queue pair.
 */
static qpair_t *
create_queue(service_t proto)
{
    qpair_t *s;

    s = sdf_create_queue_pair(MyRank, VNODE_ANY, proto, proto, SDF_WAIT_FTH);
    if (!s)
        fatal("failed to create queue pair %d", proto);
    return s;
}


/*
 * Send a message of a particular length.
 */
static void
send_len(node_t *node, fthMbox_t *rbox)
{
    sdf_msg_t *msg = sdf_msg_alloc(node->mlen);
    meta_t *meta = (meta_t *) msg->msg_payload;
    atom_t aseqn = atomic_inc_get(node->s.qpost);
    sfm_t sfm ={
        .abox = &node->abox,
        .rbox = rbox,
        .release_on_send = 1
    };

    if (AckFreq && (aseqn % AckFreq) == 0)
        sfm.actlvl = SACK_BOTH_FTH;
    else {
        aseqn = 0;
        sfm.actlvl = SACK_ONLY_FTH;
    }

    memset(msg->msg_payload, 0, node->mlen);
    meta->oseqn = ++node->s.sseqn;
    meta->aseqn = aseqn;
    msg_seth2n(meta->oseqn);
    msg_seth2n(meta->aseqn);
    send_out(node, msg, node->mlen, &sfm, NULL);
}


/*
 * Send an acknowledge.
 */
static void
send_ack(node_t *node, srm_t *srm, atom_t aseqn)
{
    int len = sizeof(meta_t);
    sfm_t sfm ={
        .actlvl = SACK_NONE_FTH,
        .release_on_send = 1
    };
    sdf_msg_t *msg = sdf_msg_alloc(len);
    meta_t *meta = (meta_t *) msg->msg_payload;

    memset(msg->msg_payload, 0, len);
    meta->aseqn = aseqn;
    msg_seth2n(meta->aseqn);
    send_out(node, msg, len, &sfm, srm);
}


/*
 * Send out a message.
 */
static void
send_out(node_t *node, sdf_msg_t *msg, int len, sfm_t *sfm, srm_t *srm)
{
    int s;
    service_t p = PROTOCOL;
    int rank = node - Nodes;

    if (t_on(UBUG)) {
        meta_t *meta = (meta_t *) msg->msg_payload;
        atom_t aseqn = msg_retn2h(meta->aseqn);
        atom_t oseqn = msg_retn2h(meta->oseqn);
        t_ubug("send %d.%02d => %d.%02d srm=%d hlen=%d mlen=%d aq=%ld oq=%ld",
           MyRank, p, rank, p, !!srm, node->hlen, len, aseqn, oseqn);
    }

    s = sdf_msg_send(msg, len, rank, p, MyRank, p, 0, sfm, srm);
    if (s < 0)
        t_user("sdf_msg_send failed");
    else
        node->s.bsent += node->hlen + len;
}


/*
 * Receive a message.
 */
static void
recv_msg(qpair_t *qpair, int *rankp, int *sizep,
         atom_t *aseqnp, atom_t *oseqnp, srm_t *srm)
{
    sdf_msg_t *msg = sdf_msg_recv(qpair);
    int       size = msg->msg_len - sizeof(*msg);
    int       type = msg->msg_type;
    int       rank = msg->msg_src_vnode;
    atom_t    aseqn = 0;
    atom_t    oseqn = 0;

    if (type != SDF_MSG_ERROR && size >= sizeof(meta_t)) {
        meta_t *meta = (meta_t *) msg->msg_payload;
        aseqn = msg_retn2h(meta->aseqn);
        oseqn = msg_retn2h(meta->oseqn);
    }

    t_ubug("recv %d.%02d <= %d.%02d len=%d aseqn=%ld type=%d",
        msg->msg_dest_vnode, msg->msg_dest_service, rank,
        msg->msg_src_service, size, aseqn, msg->msg_type);

    if (type == SDF_MSG_ERROR) {
        int error = ((sdf_msg_error_payload_t *)msg->msg_payload)->error;
        if (error == SDF_NODE_DEAD)
            t_user("n%d died", rank);
        else if (error == SDF_TIMEOUT)
            t_user("n%d timed out", rank);
        else
            t_user("read error on n%d", rank);
        size = 0;
    }

    if (rank >= 0 && rank < NNODES) {
        node_t *node = &Nodes[rank];
        atomic_inc(node->s.mrecv);
        atomic_add(node->s.brecv, size+node->hlen);
    }

    *rankp  = rank;
    *sizep  = size;
    *aseqnp = aseqn;
    *oseqnp = oseqn;
    sdf_msg_get_response(msg, srm);
    sdf_msg_free(msg);
}
