/*
 * File: sdf_bw.c
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Determine bandwidth when going through the SDF messaging system.
 */
#include <stdio.h>
#include "sdftcp/tools.h"
#include "sdftcp/trace.h"
#include "sdftcp/msg_cat.h"
#include "sdftcp/msg_msg.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "agent.h"


/*
 * Configurable parameters.
 */
#define STACK_SIZE  (128*1024)
#define PROTOCOL    SDF_CONSISTENCY


/*
 * Useful definitions.
 */
#define streq(a, b)  (strcmp(a, b) == 0)
typedef struct sdf_queue_pair qpair_t;
typedef void (*fthfunc_t)(uint64_t);


/*
 * Configurable static variables.
 */
static int QSize    = 5;
static int Binding  = 0;
static int Verbose  = 0;
static int MsgSize  = 1024*1024;
static int Duration = 5;


/*
 * Static variables.
 */
static int MyRank;
static int LoRank;
static int HiRank;
static int NRanks;


/*
 * Function prototypes.
 */
static void     test(void);
static void     usage(void);
static void     recvonly(uint64_t arg);
static void     sendonly(uint64_t arg);
static void     fth_start(fthfunc_t func);
static void     parse(int *argcp, char ***argvp);
static void     print_bw(char *who, uint64_t bytes, ntime_t elapsed);
static void    *fth_pthread(void *arg);
static int      numarg(char *opt, char *arg, int min);
static int      send_msg(sdf_fth_mbx_t *sfm, int len);
static int      recv_msg(qpair_t *queue, fthMbox_t *mbox);
static int64_t  waitack(fthMbox_t *abox, uint64_t *acks, uint64_t *sent);
static qpair_t *create_queue(int rank, service_t proto);


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
        if (streq(arg, "-b") || streq(arg, "--binding"))
            Binding = 1;
        else if (streq(arg, "-h") || streq(arg, "--help"))
            usage();
        else if (streq(arg, "-q") || streq(arg, "--qsize"))
            QSize = numarg(arg, *argr++, 1);
        else if (streq(arg, "-s") || streq(arg, "--size"))
            MsgSize = numarg(arg, *argr++, 1);
        else if (streq(arg, "-t") || streq(arg, "--time"))
            Duration = numarg(arg, *argr++, 1);
        else if (streq(arg, "-v") || streq(arg, "--verbose"))
            Verbose = 1;
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
        "    sdf_bw Options\n"
        "Options\n"
        "    --binding|-b\n"
        "        Use a binding rather than a queue.\n"
        "    --help|-h\n"
        "        Print this message.\n"
        "    --qsize|-q N\n"
        "        Set the size of the send queue which means that N+1\n"
        "        messages that can be in flight simultaneously.\n"
        "    --size|-s N\n"
        "        Use a message size of N nodes.\n"
        "    --time|-t N\n"
        "        Make test last N seconds.\n"
        "    --verbose|-v\n"
        "        Print fth idle.\n"
        "    -y\n"
        "        Turn on SDF logging.\n"
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
        "    --msg_rank N\n"
        "        Set our rank to N.\n"
        "    --msg_tcp PORT\n"
        "        Use TCP port PORT.\n"
        "    --msg_udp PORT\n"
        "        Use UDP port PORT.\n";
    fputs(s, stderr);
    plat_exit(0);
}


/*
 * Start the test.
 */
static void
test(void)
{
    int *ranks = sdf_msg_ranks(&NRanks);

    LoRank = ranks[0];
    MyRank = sdf_msg_myrank();
    if (NRanks == 1)
        HiRank = MyRank;
    else if (NRanks == 2)
        HiRank = ranks[LoRank == MyRank];
    else
        panic("number of nodes (%d) must be either 1 or 2", NRanks);
    free(ranks);

    fthInit();
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

    /* Print out fth idle time */
    if (Verbose)
        printf("fthIdle = %ld (usec)\n", fthGetSchedulerIdleTime());
}


/*
 * Start up the appropriate fthread.
 */
static void *
fth_pthread(void *arg)
{
    if (NRanks == 1) {
        fth_start(&sendonly);
        fth_start(&recvonly);
    } else
        fth_start(MyRank == LoRank ? &recvonly : &sendonly);
    fthSchedulerPthread(0);
    return NULL;
}


/*
 * Start a fthread.
 */
static void
fth_start(fthfunc_t func)
{
    fthResume(fthSpawn(func, STACK_SIZE), 0);
}


/*
 * Send messages to the other side.
 */
static void
sendonly(uint64_t arg)
{
    ntime_t stime;
    ntime_t etime;
    fthMbox_t abox;
    sdf_fth_mbx_t sfm;
    uint64_t nacks = 0;
    uint64_t nsent = 0;
    uint64_t npost = 0;

    use_all_cpus();
    fthMboxInit(&abox);
    if (NRanks > 1)
        create_queue(LoRank, PROTOCOL);
    memset(&sfm, 0, sizeof(sfm));
    sfm.actlvl = SACK_ONLY_FTH;
    sfm.abox = &abox;
    sfm.release_on_send = 1;

    stime = msg_ntime();
    etime = stime + Duration*NANO;

    for (;;) {
        if (msg_ntime() >= etime)
            break;
        if (!send_msg(&sfm, MsgSize))
            break;
        npost++;
        fthYield(0);
        if (npost-nacks <= QSize)
            continue;
        if (waitack(&abox, &nacks, &nsent) < 0)
            break;
    }

    /* Wait for messages to actually be sent */
    while (nacks < npost)
        waitack(&abox, &nacks, &npost);
    etime = msg_ntime();

    /* Tell the other side to quit */
    if (send_msg(&sfm, 0))
        fthMboxWait(&abox);

    print_bw("send", nsent*MsgSize, etime-stime);
    fthMboxTerm(&abox);
    if (NRanks > 1)
        fthKill(1);
}


/*
 * Wait for an ack on an fth mailbox updating the number sent and acknowledged.
 */
static int64_t
waitack(fthMbox_t *abox, uint64_t *acks, uint64_t *sent)
{
    int64_t s = fthMboxWait(abox);

    *acks += 1;
    if (s > 0)
        *sent += 1;
    return s;
}


/*
 * Receive messages from the other side.
 */
static void
recvonly(uint64_t arg)
{
    ntime_t stime;
    ntime_t etime;
    uint64_t bytes;
    fthMbox_t qbox;
    qpair_t *queue;
    sdf_msg_action_t *action = NULL;
    sdf_msg_binding_t *binding = NULL;

    use_all_cpus();
    queue = create_queue(HiRank, PROTOCOL);
    if (Binding) {
        fthMboxInit(&qbox);
        action = sdf_msg_action_fth_mbox_alloc(&qbox);
        binding = sdf_msg_binding_create(action, MyRank, PROTOCOL);
    }

    bytes = 0;
    stime = msg_ntime();
    etime = stime + Duration*NANO;
    while (msg_ntime() < etime) {
        int n = recv_msg(queue, &qbox);

        if (!n)
            break;
        bytes += n;
    }
    print_bw("recv", bytes, msg_ntime()-stime);

    if (Binding) {
        sdf_msg_binding_free(binding);
        sdf_msg_action_free(action);
        fthMboxTerm(&qbox);
    }
    fthKill(1);
}


/*
 * Create a queue pair.
 */
static qpair_t *
create_queue(int rank, service_t proto)
{
    qpair_t *s;

    s = sdf_create_queue_pair(MyRank, rank, proto, proto, SDF_WAIT_FTH);
    if (!s)
        fatal("failed to create queue pair %d", proto);
    return s;
}


/*
 * Print out the bandwidth.
 */
static void
print_bw(char *who, uint64_t bytes, ntime_t elapsed)
{
    double bw;
    double mb;
    double secs;

    if (!elapsed)
        fatal("no time has elapsed");
    mb = (double)bytes / (1024*1024);
    secs = (double)elapsed / NANO;
    bw = mb / secs;
    printf("%s bandwidth: %.1f MB/sec\n", who, bw);
}


/*
 * Send a message.
 */
static int
send_msg(sdf_fth_mbx_t *sfm, int len)
{
    int s;
    service_t p = PROTOCOL;
    sdf_msg_t *msg = sdf_msg_alloc(len);

    memset(msg->msg_payload, 0, len);
    t_ubug("send %d.%02d => %d.%02d len=%d", MyRank, p, LoRank, p, len);
    s = sdf_msg_send(msg, len, LoRank, p, MyRank, p, 0, sfm, NULL);
    return s == 0;
}


/*
 * Receive a message and return 1 if successful, 0 if not.
 */
static int
recv_msg(qpair_t *queue, fthMbox_t *mbox)
{
    sdf_msg_t *msg = Binding ? (sdf_msg_t *)fthMboxWait(mbox)
                             : sdf_msg_recv(queue);
    int len = msg->msg_len - sizeof(*msg);
    int type = msg->msg_type;

    t_ubug("recv %d.%02d <= %d.%02d len=%ld type=%d",
        msg->msg_dest_vnode, msg->msg_dest_service, msg->msg_src_vnode,
        msg->msg_src_service, msg->msg_len - sizeof(*msg), msg->msg_type);
    sdf_msg_free(msg);

    if (type == SDF_MSG_ERROR)
        return 0;
    return len;
}
