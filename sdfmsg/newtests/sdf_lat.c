/*
 * File: sdf_lat.c
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Determine latency when going through the SDF messaging system.
 */
#include <stdio.h>
#include "sdftcp/tools.h"
#include "sdftcp/trace.h"
#include "sdftcp/msg_cat.h"
#include "sdftcp/msg_msg.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_sync.h"
#include "agent.h"


/*
 * Configurable parameters.
 */
#define NSLOTS      10000
#define STACK_SIZE  (128*1024)
#define PROTOCOL    SDF_CONSISTENCY


/*
 * Useful definitions.
 */
#define streq(a, b) (strcmp(a, b) == 0)
typedef sdf_queue_pair_t qpair_t;
typedef uint64_t count_t;


/*
 * Configurable static variables.
 */
static int Binding  = 0;
static int MsgSize  = 1;
static int Duration = 2;


/*
 * Static variables.
 */
static int      MyRank;
static int      ToRank;
static int      LoRank;
static int      MaxLat;
static count_t  NTrips;
static count_t *Counts;


/*
 * Function prototypes.
 */
static void     test(void);
static void     usage(void);
static void     record(ntime_t l);
static void    *fth_pthread(void *arg);
static void     fth_pingpong(uint64_t arg);
static void     print_lat(ntime_t elapsed);
static void     parse(int *argcp, char ***argvp);
static int      numarg(char *opt, char *arg, int min);
static int      send_msg(sdf_fth_mbx_t *sfm, int len);
static int      recv_msg(qpair_t *qpair, fthMbox_t *mbox);
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
        if (streq(arg, "-b") || streq(arg, "--binding"))
            Binding = 1;
        else if (streq(arg, "-h") || streq(arg, "--help"))
            usage();
        else if (streq(arg, "-s") || streq(arg, "--size"))
            MsgSize = numarg(arg, *argr++, 1);
        else if (streq(arg, "-t") || streq(arg, "--time"))
            Duration = numarg(arg, *argr++, 1);
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
        "    sdf_lat Options\n"
        "Options\n"
        "    --binding|-b\n"
        "        Use a binding rather than a queue.\n"
        "    --help|-h\n"
        "        Print this message.\n"
        "    --size N|-s\n"
        "        Use a message size of N nodes.\n"
        "    --time N|-t\n"
        "        Make test last N seconds.\n"
        "    -y\n"
        "        Do not turn off SDF logging.\n"
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
    int nranks;
    int *ranks = sdf_msg_ranks(&nranks);

    LoRank = ranks[0];
    MyRank = sdf_msg_myrank();
    if (nranks == 1)
        ToRank = MyRank;
    else if (nranks == 2)
        ToRank = ranks[LoRank == MyRank];
    else
        panic("number of nodes (%d) must be either 1 or 2", nranks);
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
}


/*
 * It appears that this all must be called from a pthread.  When I attempt to
 * call it directly, it sometimes dies.
 */
void *
fth_pthread(void *arg)
{
    fthResume(fthSpawn(&fth_pingpong, STACK_SIZE), 0);
    fthSchedulerPthread(0);
    return NULL;
}


/*
 * Run a pingpong test as a fth thread.
 */
static void
fth_pingpong(uint64_t arg)
{
    ntime_t ltime;
    fthMbox_t qbox;
    qpair_t *qpair;
    sdf_fth_mbx_t sfm;
    ntime_t stime = 0;
    ntime_t etime = 0;
    sdf_msg_action_t *action = NULL;
    sdf_msg_binding_t *binding = NULL;

    use_all_cpus();
    Counts = m_malloc(NSLOTS * sizeof(*Counts), "main:N*count_t");
    memset(Counts, 0, NSLOTS * sizeof(*Counts));

    qpair = create_queue(PROTOCOL);
    memset(&sfm, 0, sizeof(sfm));
    sfm.actlvl = SACK_NONE_FTH;
    sfm.release_on_send = 1;

    if (Binding) {
        fthMboxInit(&qbox);
        action = sdf_msg_action_fth_mbox_alloc(&qbox);
        binding = sdf_msg_binding_create(action, MyRank, PROTOCOL);
    }

    if (!send_msg(&sfm, MsgSize) ||!recv_msg(qpair, &qbox))
        fatal("failed to synchronize");

    if (MyRank == LoRank)
        if (!send_msg(&sfm, MsgSize))
            fatal("failed to send");

    ltime = 0;
    while (recv_msg(qpair, &qbox)) {
        ntime_t ctime = msg_ntime();

        if (!ltime) {
            stime = ctime;
            etime = Duration*NANO + stime;
        } else {
            record(ctime-ltime);
            if (ctime >= etime) {
                send_msg(&sfm, 0);
                break;
            }
        }
        ltime = ctime;
        if (!send_msg(&sfm, MsgSize))
            break;
    }
    print_lat(ltime-stime);

    if (Binding) {
        sdf_msg_binding_free(binding);
        sdf_msg_action_free(action);
        fthMboxTerm(&qbox);
    }

    m_free(Counts);
    fthKill(1);
}


/*
 * Create a queue pair.
 */
static qpair_t *
create_queue(service_t proto)
{
    qpair_t *s;

    s = sdf_create_queue_pair(MyRank, ToRank, proto, proto, SDF_WAIT_FTH);
    if (!s)
        fatal("failed to create queue pair %d", proto);
    return s;
}


/*
 * Record a latency.
 */
static void
record(ntime_t l)
{
    if (l < 0)
        panic("negative latency encountered");

    NTrips++;
    if (l > MaxLat)
        MaxLat = l;

    l /= 100;
    if (l >= NSLOTS)
        l = NSLOTS - 1;
    Counts[l]++;
}


/*
 * Print out the latency.
 */
static void
print_lat(ntime_t elapsed)
{
    int i;
    int m;
    count_t h;
    count_t s;
    double med;
    double avg;
    double min;
    double max;

    if (!NTrips)
        fatal("no messages exchanged");
    avg = (elapsed / 1000.0) / (NTrips*2);

    s = 0;
    m = -1;
    h = NTrips / 2;
    for (i = 0; i < NSLOTS; i++) {
        count_t v = Counts[i];

        if (!v)
            continue;
        if (m < 0)
            m = i;
        s += v;
        if (s > h)
            break;
    }

    med = i / 20.0;
    min = m / 20.0;
    max = MaxLat / 2000.0;
    printf("med: %.1f us; avg: %.1f us; min: %.1f us; max: %.1f us\n",
           med, avg, min, max);
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

    t_ubug("send %d.%02d => %d.%02d len=%d", MyRank, p, ToRank, p, len);
    memset(msg->msg_payload, 0, len);
    s = sdf_msg_send(msg, len, ToRank, p, MyRank, p, 0, sfm, NULL);
    return s == 0;
}


/*
 * Receive a message and return 1 if successful.
 */
static int
recv_msg(qpair_t *qpair, fthMbox_t *mbox)
{
    sdf_msg_t *msg = Binding ? (sdf_msg_t *)fthMboxWait(mbox)
                             : sdf_msg_recv(qpair);
    int len = msg->msg_len - sizeof(*msg);
    int type = msg->msg_type;

    t_ubug("recv %d.%02d <= %d.%02d len=%ld type=%d",
        msg->msg_dest_vnode, msg->msg_dest_service, msg->msg_src_vnode,
        msg->msg_src_service, msg->msg_len - sizeof(*msg), msg->msg_type);
    sdf_msg_free(msg);

    if (!len)
        return 0;
    return type != SDF_MSG_ERROR;
}
