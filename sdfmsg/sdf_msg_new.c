/*
 * File: sdf_msg_new.c
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */
#include <ctype.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include "sdf_msg_sync.h"
#include "sdf_msg_wrapper.h"
#include "misc/misc.h"
#include "sdftcp/locks.h"
#include "sdftcp/stats.h"
#include "sdftcp/tools.h"
#include "sdftcp/trace.h"
#include "sdftcp/msg_cat.h"
#include "sdftcp/msg_int.h"
#include "sdftcp/msg_map.h"
#include "sdftcp/msg_msg.h"
#include "platform/stdio.h"
#include "utils/properties.h"
#include "protocol/replication/replicator.h"


/*
 * Configurable parameters.
 */
#define VER_SDFMSG     1
#define NHASH          4096
#define PROP_DEF_RANKS 2
#define WDOG_HOST      "localhost"
#define WDOG_PORT      "51360"
#define WDOG_LATE_TIME (4*NANO)


/*
 * Macro functions.
 */
#define nel(a)        (sizeof(a)/sizeof(*a))
#define str2str(a, b) (a)
#define max(a, b)     ((a) > (b) ? (a) : (b))
#define hash(i)       ((unsigned long)i % NHASH)
#define mintime(a, b) ((a) && (b)                   \
                       ? ((a) < (b) ? (a) : (b))    \
                       : (a) + (b))


/*
 * Message parameters.
 */
#define MP_DEAD     (1<<0)
#define MP_LIVE     (1<<1)
#define MP_PING     (1<<2)
#define MP_RANK     (1<<3)
#define MP_ALIAS    (1<<4)
#define MP_CLASS    (1<<5)
#define MP_DEBUG    (1<<6)
#define MP_FLAGS    (1<<7)
#define MP_IFACE    (1<<8)
#define MP_NCONN    (1<<9)
#define MP_NHOLD    (1<<10)
#define MP_NODES    (1<<11)
#define MP_STATS    (1<<12)
#define MP_NOBCAST  (1<<13)
#define MP_TCPPORT  (1<<14)
#define MP_TIMEOUT  (1<<15)
#define MP_UDPPORT  (1<<16)
#define MP_AFFINITY (1<<17)
#define MP_LINKLIVE (1<<18)
#define MP_NTHREADS (1<<19)
#define MP_NTUNIQUE (1<<20)
#define MP_MUSTLIVE (1<<21)
#define MP_STATFREQ (1<<22)


/*
 * Type definitions.
 */
typedef SDF_status_t err_t;
typedef struct addrinfo ai_t;
typedef sdf_queue_pair_t qpair_t;


/*
 * Message parameter table.
 */
typedef struct {
    uint64_t  pbit;                     /* Parameter bit */
    char     *name;                     /* Name */
    int       type;                     /* Type */
    int       offs;                     /* Offset in configuration structure */
    int       size;                     /* Size */
} parm_t;


/*
 * SDF read queue.
 */
typedef struct readq {
    struct readq *next;                 /* Next */
    int           move;                 /* Move entries to binding */
    int           valid;                /* Queue is valid */
    int           srank;                /* Source rank */
    int           sserv;                /* Source service */
    int           dserv;                /* Destination service */
    sdf_qwt_t     wtype;                /* Wait type */
    atom_t        count;                /* Number of entries */
    union {
        struct {
            pthread_mutex_t pmutex;     /* Pthread mutex */
            pthread_cond_t  pcondv;     /* Pthread conditional variable */
            sdf_msg_t      *pqhead;     /* Message queue head */
            sdf_msg_t      *pqtail;     /* Message queue tail */
        };
        fthMbox_t fthmbox;              /* Fth mailbox */
    };
} readq_t;


/*
 * Send queue.
 */
typedef struct sendq {
    struct sendq     *next;             /* Used for chaining */
    msg_id_t          id;               /* Message id */
    int               stag;             /* Source service */
    int               dtag;             /* Destination service */
    int               rank;             /* Destination node */
    int               mfree;            /* Release message once sent */
    char             *data;             /* Data */
    uint32_t          len;              /* Length */
    ntime_t           timeout;          /* Timeout time */
    fthMbox_t        *abox;             /* Mailbox for acknowledge */
    fthMbox_t        *rbox;             /* Mailbox for response */
    sdf_msg_action_t *ract;             /* Response action */
} sendq_t;


/*
 * Variables.
 *
 *  alive          - If set, the messaging system is alive.
 *  start          - If set, the messaging system hse been started.
 *  epoch          - Time 0.
 *  live           - An expandable byte array indexed by rank to determine if a
 *                   given node is alive.
 *  live_call      - List of functions that are called on liveness events.
 *  nxtime_late    - The next time we need to inform the watchdog about late
 *                   nodes.
 *  nxtime_stat    - The next time we need to print out statistics.
 *  nxtime_timeout - The next time we need to check for timing out nodes.
 *  pthread        - The messaging pthread.
 *  sendmid        - The last message id that was used to send a message.
 *  strayredo      - Cause our main loop to recheck the redo list.
 */
typedef struct {
    int        alive;
    int        start;
    int        strayredo;
    atom_t     sendmid;
    uint64_t   siblings;
    xstr_t     live;
    ntime_t    epoch;
    ntime_t    nxtime_late;
    ntime_t    nxtime_stat;
    ntime_t    nxtime_timeout;
    pthread_t  pthread;
    cb_list_t  live_call;
} vars_t;


/*
 * Configuration variables.
 *
 *  myrank   - The rank of our node.  A unique number that is assigned to each
 *             node.
 *  noranks  - The number of nodes that we waited for before starting.
 *  set      - A bit array of parameters that have already been set.  Bits are
 *             MP_*.
 *  statfreq - Frequency we print out statistics.
 *  timeout  - The interval at which we time out waiting for a response.
 */
typedef struct {
    uint64_t   set;
    msg_init_t tcp;
    int32_t    myrank;
    int32_t    noranks;
    ntime_t    timeout;
    ntime_t    statfreq;
} conf_t;


/*
 * For handling the read queue.
 *  list  - The list of SDF message queues that are used to store messages
 *          awaiting to be read by callers to sdf_msg_receive.
 *  lock  - Used to lock accesses to list.
 */
typedef struct {
    readq_t *list;
    wlock_t *lock;
} vars_readq_t;


/*
 * Send queue variables.
 *
 *  freelist  - Our free list of send queues.
 *  freelock  - Used to lock accesses to freelist.
 *  hashlist  - The hash table containing any messages waiting for acks or
 *              responses.
 *  hashlock  - Lock for each bucket of the hash list.
 *  posthead  - Along with posttail, contains the list of send queue elements
 *              waiting to be sent.
 *  posttail  - See posthead.
 */
typedef struct {
    wlock_t *freelock;
    sendq_t *freelist;
    sendq_t *posthead;
    sendq_t *posttail;
    sendq_t *hashlist[NHASH];
    wlock_t *hashlock[NHASH];
} vars_sendq_t;


/*
 * For handling stray messages.
 *
 *  head - Along with tail, contains any messages that have not found a home
 *         yet; perhaps because their queues or bindings have not yet been set
 *         up.  Protected by lock.
 *  lock - To protect head and tail.
 *  redo - Cause our main loop to recheck the redo list.
 *  tail - See head.  Protected by lock.
 */
typedef struct {
    int        redo;
    wlock_t   *lock;
    sdf_msg_t *head;
    sdf_msg_t *tail;
} vars_stray_t;


/*
 * Statistics we keep track of.  If a parameter is preceded by live_, it is
 * being actively updated and sometimes protected by a lock.  If preceded by
 * prev_, it is the saved value of the live_ version at the last reporting
 * interval.  If it is preceded by best_ (and this mostly applies to maximum
 * and minimum values), it is the most extreme value we have discovered since
 * the live_ version needs to be cleared at each time interval.
 *
 *  sent          - Total messages sent.
 *  recv          - Total messages received.
 *  sendq         - Number of messages waiting to be sent.  live_sendq is
 *                  protected by SQPostLock.
 *  sendq_max     - Maximum size of send queue.  live_sendq_max is protected by
 *                  SQPostLock.
 *  recvq         - Number of received messages not picked up.
 *  recvq_max     - Maximum number of received messages not picked up.
 *                  live_recvq_max is protected by StatRLock.
 *  recvq_any_max - Maximum size that any receive queue reached.
 *                  live_recvq_any_max is protected by StatRLock.
 */
typedef struct {
    uint64_t  live_sent_mesg;
    uint64_t  live_sent_byte;
    uint64_t  live_recv_mesg;
    uint64_t  live_recv_byte;
    uint64_t  live_sendq;
    uint64_t  live_sendq_max;
    uint64_t  live_recvq;
    uint64_t  live_recvq_max;
    uint64_t  live_recvq_any_max;
    uint64_t  live_no_msg_bufs;
    ntime_t   prev_time;
    uint64_t  prev_sent_mesg;
    uint64_t  prev_sent_byte;
    uint64_t  prev_recv_mesg;
    uint64_t  prev_recv_byte;
    uint64_t  best_sendq_max;
    uint64_t  best_recvq_max;
    uint64_t  best_recvq_any_max;
    uint64_t  prev_no_msg_bufs;
    cb_list_t call;
} vars_stat_t;


/*
 * Static function prototypes.
 */
static int        wdog_rnodes(void);
static int        node_line_me(char *line);
static int        node_name_me(char *name);
static int        arrival(msg_info_t *info);
static int        do_bindingq(sdf_msg_t *msg);
static int        do_response(sdf_msg_t *msg);
static int        str2int(char *str, char *par);
static int        parm_bad(char *name, char *data);
static int        ntime_amin(ntime_t *val, ntime_t new);
static int        node_eq(const char *n1, const char *n2);
static int        parm_set(conf_t *conf, parm_t *parm, char *val);
static int        readq_match(readq_t *readq, int srank, int sserv, int dserv);
static void       do_late(void);
static void       remotes(void);
static void       do_sends(void);
static void       do_stats(void);
static void       move_msg(void);
static void       set_conf(void);
static void       set_rank(void);
static void       zfree(void *p);
static void       exit_vars(void);
static void       init_late(void);
static void       init_vars(void);
static void       parm_show(void);
static void       set_alias(void);
static void       do_timeout(void);
static void       show_stats(void);
static void       stray_jolt(void);
static void       drop_node(int nno);
static void       node_died(int rank);
static void       readq_listdel(void);
static void       stray_listdel(void);
static void       uppercase(char *str);
static void       move_msg_queue(void);
static void       move_msg_stray(void);
static void       our_stats(stat_t *s);
static void       do_sent(msg_info_t *info);
static void       do_recv(msg_info_t *info);
static void       node_died_readq(int rank);
static void       node_died_sendq(int rank);
static void       readq_add(readq_t *readq);
static void       do_recv1(msg_info_t *info);
static void       do_recv2(msg_info_t *info);
static void       sendq_addq(sendq_t *sendq);
static void       sendq_free(sendq_t *sendq);
static void       sendq_hput(sendq_t *sendq);
static void       sendq_post(sendq_t *sendq);
static void       sendq_send(sendq_t *sendq);
static void       stray_post(sdf_msg_t *msg);
static void       mbox_untangle(fthMbox_t *mbox);
static void       sendq_freelist(sendq_t *sendq);
static void       setstrdup(char **val, char *str);
static void       send_error(sendq_t *sendq, int error);
static void       knowlist(int i, char *addr, char *iface);
static void       sendq_listdel(sendq_t *sendq, int freed);
static void       live_func(int live, int rank, void *arg);
static void       recv_err(sdf_msg_t *msg, char *fmt, ...);
static void       fail_op_sendq(sendq_t *sendq, err_t error);
static void       readq_post(readq_t *readq, sdf_msg_t *msg);
static void       set_msg(sdf_msg_t *msg,
                          int srank, int sserv, int drank, int dserv, int len);
static void       timed_call(ntime_t *nowp,
                             ntime_t *endp, ntime_t *nxtimep, void (*func)());
static int64_t    parm_getint(parm_t *parm);
static int64_t    str2time(char *str, char *par);
static void      *loop(void *arg);
static char      *get_prop(char *name);
static char      *error_name(int error);
static char      *get_prop_i(char *name, int i);
static char      *get_prop_ii(int i1, char *n1, int i2, char *n2);
static FILE      *wdog_connect(void);
static FILE      *wdog_send(char *fmt, ...);
static parm_t    *parm_find(char *par);
static sendq_t   *sendq_head(void);
static sendq_t   *sendq_next(void);
static sendq_t   *sendq_alloc(void);
static sendq_t   *sendq_hget(msg_id_t id);
static readq_t   *readq_find(sdf_msg_t *msg);
static sdf_msg_t *err_msg(int srank, int sserv,
                          int drank, int dserv, err_t error);


/*
 * Static variables.
 *
 *  ReadQ      - Variables for the read queue.
 *  SQPostLock - Used to lock accesses to SendQ.posthead, SendQ.posttail as
 *               well as Stat.live_sendq and Stat.live_sendq_max.
 *  StatRLock  - Used to protect Stat.live_recvq_max and
 *               Stat.live_recvq_any_max.
 *  Stat       - Current messaging statistics.
 *  SendQ      - Variables for the send queue.
 *  Stray      - Variables for stray messages.
 *  V          - Miscellaneous static variables.
 */
static vars_t        V;
static conf_t        Conf;
static vars_stat_t   Stat;
static vars_readq_t  ReadQ;
static vars_sendq_t  SendQ;
static vars_stray_t  Stray;
static wlock_t      *StatRLock;
static wlock_t      *SQPostLock;


/*
 * Message parameter table.
 * Flags are
 *  i - value is a signed integer
 *  p - value is a pointer
 *  t - value is a time
 *  u - value is an unsigned integer
 */
#define _os(e) (void *)&Conf.e - (void *)&Conf, sizeof(Conf.e)
static parm_t Parm[] ={
    { MP_DEAD,      "msg_dead",     't',  _os(tcp.dead)     },
    { MP_LIVE,      "msg_live",     't',  _os(tcp.live)     },
    { MP_PING,      "msg_ping",     't',  _os(tcp.ping)     },
    { MP_RANK,      "msg_rank",     'i',  _os(myrank)       },
    { MP_ALIAS,     "msg_alias",    'p',  _os(tcp.alias)    },
    { MP_CLASS,     "msg_class",    'u',  _os(tcp.class)    },
    { MP_DEBUG,     "msg_debug",    'd',                    },
    { MP_FLAGS,     "msg_flags",    'f',                    },
    { MP_STATS,     "msg_stats",    's',                    },
    { MP_IFACE,     "msg_iface",    'p',  _os(tcp.iface)    },
    { MP_NCONN,     "msg_nconn",    'u',  _os(tcp.nconn)    },
    { MP_NHOLD,     "msg_nhold",    'u',  _os(tcp.nhold)    },
    { MP_NODES,     "msg_nodes",    'i',  _os(noranks)      },
    { MP_NOBCAST,   "msg_nobcast",  'u',  _os(tcp.nobcast)  },
    { MP_TCPPORT,   "msg_tcpport",  'u',  _os(tcp.tcpport)  },
    { MP_TIMEOUT,   "msg_timeout",  't',  _os(timeout)      },
    { MP_UDPPORT,   "msg_udpport",  'u',  _os(tcp.tcpport)  },
    { MP_AFFINITY,  "msg_affinity", 'u',  _os(tcp.affinity) },
    { MP_LINKLIVE,  "msg_linklive", 't',  _os(tcp.linklive) },
    { MP_NTHREADS,  "msg_nthreads", 'u',  _os(tcp.nthreads) },
    { MP_NTUNIQUE,  "msg_ntunique", 'u',  _os(tcp.ntunique) },
    { MP_MUSTLIVE,  "msg_mustlive", 'u',  _os(tcp.mustlive) },
    { MP_STATFREQ,  "msg_statfreq", 't',  _os(statfreq)     },
};


/*
 * Initialize.
 */
void
sdf_msg_init(int argc, char *argv[])
{
    /* Ordering of this block is imporant */
    init_vars();
    if (!(Conf.set & MP_RANK))
        Conf.myrank = -1;
    sdf_logi(70029, "sdfmsg version %d", VER_SDFMSG);

    set_alias();
    set_rank();
    set_conf();

    if (!Conf.noranks)
        Conf.noranks = 1;
    if (Conf.statfreq > 0)
        V.nxtime_stat = V.epoch + Conf.statfreq;
    Stat.prev_time = V.epoch;

    stat_init();
    init_late();

    trace_init();
    msg_init(&Conf.tcp);
    sdf_msg_call_stat(our_stats);
    remotes();
    msg_map_init(-1, Conf.noranks, &Conf.myrank);

    xainit(&V.live, sizeof(char), 16, 1);
    msg_livecall(1, 0, live_func, NULL);

    if (getProperty_Int("SDF_MSG_ENGINE_START", 1)) {
        V.alive = 1;
        V.start = 1;
        if (pthread_create(&V.pthread, NULL, loop, NULL) < 0)
            fatal("pthread_create failed");
    }

    sdf_msg_sync_init();
    parm_show();
    if (0)
        msg_call_post(arrival);
}


/*
 * Stop the messaging system.
 */
void
sdf_msg_stop(void)
{
    if (!V.alive)
        return;
    V.alive = 0;
    msg_wake();
    pthread_join(V.pthread, NULL);
}


/*
 * Exit.
 */
void
sdf_msg_exit(void)
{
    msg_livecall(0, 0, live_func, NULL);
    sdf_msg_sync_exit();
    sdf_msg_drain();
    if (Conf.statfreq)
        show_stats();

    if (V.alive) {
        V.alive = 0;
        msg_wake();
        pthread_join(V.pthread, NULL);
    }

    msg_map_exit();
    msg_exit();
    trace_exit();
    stat_exit();

    zfree(Conf.tcp.alias);
    zfree(Conf.tcp.iface);
    readq_listdel();
    sendq_listdel(SendQ.freelist, 0);
    sendq_listdel(SendQ.posthead, 1);
    stray_listdel();
    exit_vars();
}


/*
 * Initialize variables.
 */
static void
init_vars(void)
{
    int i;

    clear(V);
    clear(Stat);
    clear(ReadQ);
    clear(SendQ);
    clear(Stray);

    V.epoch  = msg_ntime();

    StatRLock      = wl_init();
    SQPostLock     = wl_init();
    Stray.lock     = wl_init();
    ReadQ.lock     = wl_init();
    SendQ.freelock = wl_init();
    for (i = 0; i < NHASH; i++)
        SendQ.hashlock[i] = wl_init();

    cb_init(&Stat.call, 0);
    cb_init(&V.live_call, 1);
}


/*
 * Clean up variables.
 */
static void
exit_vars(void)
{
    int i;

    wl_free(StatRLock);
    wl_free(SQPostLock);
    wl_free(Stray.lock);
    wl_free(ReadQ.lock);
    wl_free(SendQ.freelock);
    for (i = 0; i < NHASH; i++)
        wl_free(SendQ.hashlock[i]);

    cb_free(&V.live_call);
    cb_free(&Stat.call);
}


/*
 * Initialize late time.
 */
static void
init_late(void)
{
    ntime_t t;
    char *par = "WDOG_LATE_TIME";
    char *str = get_prop(par);

    t = str ? str2time(str, par) : WDOG_LATE_TIME;
    if (!t)
        return;
    V.nxtime_late = V.epoch + t;
}


/*
 * Determine our alias and set in in the configuration structure that will be
 * passed to msg_init.
 */
static void
set_alias(void)
{
    char *alias;
    char host[HOST_NAME_MAX+1];

    if (Conf.set & MP_ALIAS)
        return;

    alias = (char *)getProperty_String("MSG_ALIAS", NULL);
    if (!alias) {
        if (gethostname(host, sizeof(host)) < 0)
            fatal("gethostname failed");
        alias = host;
    }
    sdf_msg_setinitn("msg_alias", alias);
}


/*
 * See if we can determine our rank from the property file and set our rank.
 */
static void
set_rank(void)
{
    int i;
    int n;

    if (Conf.set & MP_RANK) {
        if (Conf.myrank < 0)
            fatal("msg_rank must be non-negative");
        return;
    }

    n = getProperty_Int("RANKS", PROP_DEF_RANKS);
    for (i = 0;; i++) {
        char *line = get_prop_i("RANK", i);

        if (!line) {
            if (i >= n)
                break;
            else
                continue;
        }
        if (node_line_me(line)) {
            sdf_logi(70030, "found rank of %d in property file", i);
            Conf.myrank = i;
            Conf.set |= MP_RANK;
            break;
        }
    }
}


/*
 * See if my node is one of the nodes in a semicolon separated list.
 */
static int
node_line_me(char *line)
{
    for (;;) {
        int s;
        char *item = semi_item(&line);

        if (!item)
            return 0;
        s = node_name_me(item);
        plat_free(item);
        if (s)
            return 1;
    }
}


/*
 * See if the given node is me.
 */
static int
node_name_me(char *name)
{
    if (node_eq(Conf.tcp.alias, name))
        return 1;
    if (msg_ismyip(name))
        return 1;
    return 0;
}


/*
 * See if two nodes are equal.  Besides straight equality, we want lab02 to
 * compare successfully to lab02.schoonerinfotech.com.
 */
static int
node_eq(const char *n1, const char *n2)
{
    if (!n1 || !n2)
        return 0;
    for (; tolower(*n1) == tolower(*n2) && *n1 != '\0'; n1++, n2++)
        ;
    return (*n1 == '\0' || *n1 == '.') && (*n2 == '\0' || *n2 == '.');
}


/*
 * If any configuration parameters have not been set on the command line, look
 * for them in the property file.  We do not set msg_alias, msg_nodes and
 * msg_rank since those have already been set.
 */
static void
set_conf(void)
{
    char *name;
    int n = nel(Parm);
    parm_t *parm = Parm;

    for (; n--; parm++) {
        if (Conf.set & parm->pbit)
            continue;
        if (parm->pbit & (MP_ALIAS|MP_NODES|MP_RANK))
            continue;
        name = m_strdup(parm->name, "sdf_msg:");
        uppercase(name);
        if (!parm_set(&Conf, parm, get_prop(name)))
            plat_exit(1);
        plat_free(name);
    }
}


/*
 * Get the value of a parameter in the property file.
 */
static char *
get_prop(char *name)
{
    if (Conf.myrank >= 0) {
        char *data = get_prop_i(name, Conf.myrank);
        if (data)
            return data;
    }
    return (char *)getProperty_String(name, NULL);
}


/*
 * Get the value of an indexed parameter from the property file.
 */
static char *
get_prop_i(char *name, int i)
{
    char *prop;
    char *data;

    if (plat_asprintf(&prop, "%s[%d]", name, i) < 0)
        fatal("asprintf failed");
    data = (char *) getProperty_String(prop, NULL);
    plat_free(prop);
    if (data)
        return data;

    if (plat_asprintf(&prop, "NODE[%d].%s", i, name) < 0)
        fatal("asprintf failed");
    data = (char *) getProperty_String(prop, NULL);
    plat_free(prop);
    return data;
}


/*
 * Get the value of a doubly indexed parameter from the property file.
 */
static char *
get_prop_ii(int i1, char *n1, int i2, char *n2)
{
    char *prop;
    char *data;

    if (plat_asprintf(&prop, "NODE[%d].%s[%d].%s", i1, n1, i2, n2) < 0)
        fatal("asprintf failed");
    data = (char *) getProperty_String(prop, NULL);
    plat_free(prop);
    return data;
}


/*
 * Get an integer messaging parameter.
 */
int64_t
sdf_msg_getp_int(char *name)
{
    parm_t *parm = parm_find(name);

    if (!parm)
        fatal("sdf_msg_getp_int: %s parameter not handled", name);
    return parm_getint(parm);
}


/*
 * Get an integer messaging parameter.
 */
static int64_t
parm_getint(parm_t *parm)
{
    int size = parm->size;
    int type = parm->type;
    void *ptr = ((void *)&Conf) + parm->offs;

    if (type == 'i') {
        if (size == sizeof(int8_t))
            return *((int8_t *) ptr);
        else if (size == sizeof(int16_t))
            return *((int16_t *) ptr);
        else if (size == sizeof(int32_t))
            return *((int32_t *) ptr);
        else if (size == sizeof(int64_t))
            return *((int64_t *) ptr);
    } else if (type == 'u') {
        if (size == sizeof(uint8_t))
            return *((uint8_t *) ptr);
        else if (size == sizeof(uint16_t))
            return *((uint16_t *) ptr);
        else if (size == sizeof(uint32_t))
            return *((uint32_t *) ptr);
        else if (size == sizeof(uint64_t))
            return *((uint64_t *) ptr);
    } else if (type == 't')
        return *((ntime_t *) ptr);
    fatal("parm_getint: bad parameter: %s", parm->name);
}


/*
 * Set a messaging parameter before we start.
 */
void
sdf_msg_setinitn(char *name, char *data)
{
    parm_t *parm = parm_find(name);

    if (!parm) {
        parm_bad(name, data);
        plat_exit(1);
    }

    if (!parm_set(&Conf, parm, data))
        plat_exit(1);
}


/*
 * Set a messaging parameter once we have started operation by name.
 */
int
sdf_msg_setliven(char *name, char *data)
{
    conf_t conf;
    parm_t *parm = parm_find(name);

    if (!parm)
        return parm_bad(name, data);

    clear(conf);
    if (!parm_set(&conf, parm, data))
        return 0;
    return msg_setlive(name, &conf.tcp);
}


/*
 * Indicate a bad parameter.
 */
static int
parm_bad(char *name, char *data)
{
    sdf_loge(70031, "bad messaging parameter: %s = %s", name, data);
    return 0;
}


/*
 * Find a messaging parameter by name.
 */
static parm_t *
parm_find(char *par)
{
    int n = nel(Parm);
    parm_t *parm = Parm;

    for (; n--; parm++)
        if (streq(parm->name, par))
            return parm;
    return NULL;
}


/*
 * Set a messaging parameter.
 *  conf - The structure we set the parameter in.
 *  parm - The parameter entry.
 *  val  - A string giving the value of the paramter.
 */
static int
parm_set(conf_t *conf, parm_t *parm, char *val)
{
    int size = parm->size;
    int type = parm->type;
    void *ptr = ((void *)conf) + parm->offs;

    if (!val)
        return 1;

    if (type == 'd') {
        trace_setp_debug(val);
    } else if (type == 'f') {
        msg_setflags(val);
    } else if (type == 'i') {
        if (size == sizeof(int8_t))
            *((int8_t *) ptr) = str2int(val, parm->name);
        else if (size == sizeof(int16_t))
            *((int16_t *) ptr) = str2int(val, parm->name);
        else if (size == sizeof(int32_t))
            *((int32_t *) ptr) = str2int(val, parm->name);
        else if (size == sizeof(int64_t))
            *((int64_t *) ptr) = str2int(val, parm->name);
        else {
            sdf_loge(70032, "parm_set: bad size: type=%c size=%d", type, size);
            return 0;
        }
    } else if (type == 'p') {
        setstrdup((char **)ptr, val);
    } else if (type == 's') {
        msg_setstats(val);
    } else if (type == 't') {
        *((ntime_t *) ptr) = str2time(val, parm->name);
    } else if (type == 'u') {
        if (size == sizeof(uint8_t))
            *((uint8_t *) ptr) = str2int(val, parm->name);
        else if (size == sizeof(uint16_t))
            *((uint16_t *) ptr) = str2int(val, parm->name);
        else if (size == sizeof(uint32_t))
            *((uint32_t *) ptr) = str2int(val, parm->name);
        else if (size == sizeof(uint64_t))
            *((uint64_t *) ptr) = str2int(val, parm->name);
        else {
            sdf_loge(70032, "parm_set: bad size: type=%c size=%d", type, size);
            return 0;
        }
    } else {
        sdf_loge(70033, "parm_set: bad parameter %s = %s", parm->name, val);
        return 0;
    }

    conf->set |= parm->pbit;
    return 1;
}


/*
 * Convert a string to an integer.
 */
static int
str2int(char *str, char *par)
{
    int val;

    errno = parse_int32(&val, str);
    if (!errno)
        return val;
    fatal("bad %s parameter: %s", par, str);
}


/*
 * Convert a string to a time.
 */
static int64_t
str2time(char *str, char *par)
{
    int64_t val;

    errno = parse_time(&val, str);
    if (!errno)
        return val;
    fatal("bad %s parameter: %s", par, str);
}


/*
 * Set a string parameter.
 */
static void
setstrdup(char **val, char *str)
{
    if (*val)
        plat_free(*val);
    *val = m_strdup(str, "sdf_msg_new:");
}


/*
 * Show the various parameters.
 */
static void
parm_show(void)
{
    int n = nel(Parm);
    parm_t *parm = Parm;

    for (; n--; parm++) {
        int type = parm->type;
        void *ptr = ((void *)&Conf) + parm->offs;

        if (!(Conf.set & parm->pbit))
            continue;
        if (type == 'i' || type == 'u') {
            int64_t val = parm_getint(parm);
            sdf_logi(70034, "%s = %ld", parm->name, val);
        } else if (type == 't') {
            double secs = (double) *((ntime_t *) ptr) / NANO;
            sdf_logi(70035, "%s = %g", parm->name, secs);
        } else if (type == 'p') {
            sdf_logi(70036, "%s = %s", parm->name, *((char **)ptr));
        }
    }
}


/*
 * Set up a direct connection to the nodes we know about rather than using
 * broadcasts.
 */
static void
remotes(void)
{
    int r;
    int n;
    char *ip;
    int found = 0;

    if (!Conf.tcp.nobcast)
        return;

    n = getProperty_Int("SDF_CLUSTER_NUMBER_NODES", 0);
    for (r = 0;; r++) {
        int i;
        int found2 = 0;

        if (r == Conf.myrank)
            continue;
        for (i = 0;; i++) {
            char *iface = NULL;

            ip = get_prop_ii(r, "IFACE", i, "IP");
            if (!ip)
                break;
            found = 1;
            found2 = 1;
            //iface = get_prop_ii(r, "IFACE", i, "NAME");
            knowlist(r, ip, iface);
        }
        if (!found2) {
            if (r >= n)
                break;
            else
                continue;
        }
    }
    if (found)
        return;

    for (r = 0;; r++) {
        if (r == Conf.myrank)
            continue;
        ip = get_prop_i("MSG_IFACE_IP", r);
        if (!ip) {
            if (r >= n)
                break;
            else
                continue;
        }
        found = 1;
        knowlist(r, ip, NULL);
    }
    if (found)
        return;

    if (wdog_rnodes())
        return;

    n = getProperty_Int("RANKS", PROP_DEF_RANKS);
    for (r = 0;; r++) {
        if (r == Conf.myrank)
            continue;
        ip = get_prop_i("RANK", r);
        if (!ip) {
            if (r >= n)
                break;
            else
                continue;
        }
        knowlist(r, ip, NULL);
    }
}


/*
 * If the watchdog is available, learn who the remote nodes are.
 */
static int
wdog_rnodes(void)
{
    int n;
    char *ptr;
    char buf[256];
    int count = 0;
    FILE *fp = wdog_send("msgif_ips");

    if (!fp)
        return 0;
    while (fgets(buf, sizeof(buf), fp)) {
        n = strlen(buf);
        if (n && buf[n-1] == '\n')
            buf[n-1] = '0';
        sdf_logi(70061, "watchdog: R: %s", buf);
        n = strtol(buf, &ptr, 10);
        if (ptr == buf || *ptr != ' ')
            fatal("bad msgif_ips line: %s", buf);
        if (n == Conf.myrank)
            continue;
        while (*ptr == ' ')
            ptr++;
        while (*ptr != ' ' && *ptr != '\0')
            ptr++;
        while (*ptr == ' ')
            ptr++;
        count++;
        knowlist(n, ptr, NULL);
    }
    fclose(fp);
    return count ? 1 : 0;
}


/*
 * Connect to the watchdog, send a line  and return a FILE pointer to the
 * socket for reading and returned information.
 */
static FILE *
wdog_send(char *fmt, ...)
{
    int n;
    char buf[64];
    va_list alist;
    FILE *fp = wdog_connect();

    if (fp) {
        va_start(alist, fmt);
        vfprintf(fp, fmt, alist);
        va_end(alist);
        fprintf(fp, "\n");
    }

    va_start(alist, fmt);
    n = vsnprintf(buf, sizeof(buf), fmt, alist);
    va_end(alist);
    if (n >= sizeof(buf)) {
        char *sl = " ...";
        int   sn = strlen(sl) + 1;
        memcpy(&buf[sizeof(buf)-sn], sl, sn);
    }

    if (fp)
        sdf_logi(70062, "watchdog: W: %s", buf);
    else
        sdf_logi(70063, "watchdog: connect failed: %s", buf);
    return fp;
}


/*
 * Connect to the watchdog and return a FILE pointer to the socket.
 */
static FILE *
wdog_connect(void)
{
    int n;
    int fd;
    FILE *fp;
    ai_t *a;
    ai_t *ailist;
    ai_t hints ={
        .ai_family   = AF_UNSPEC,
        .ai_socktype = SOCK_STREAM
    };

    n = getaddrinfo(WDOG_HOST, WDOG_PORT, &hints, &ailist);
    if (n != 0)
        fatal("getaddrinfo failed: %s", gai_strerror(n));
    if (!ailist)
        fatal("getaddrinfo failed: no valid entries");

    fd = -1;
    for (a = ailist; a; a = a->ai_next) {
        fd = socket(a->ai_family, a->ai_socktype, a->ai_protocol);
        if (fd < 0)
            continue;
        if (connect(fd, a->ai_addr, a->ai_addrlen) == 0)
            break;
        close(fd);
    }
    freeaddrinfo(ailist);
    if (!a)
        return NULL;

    fp = fdopen(fd, "r+");
    if (!fp)
        fatal_sys("fdopen failed");
    return fp;
}


/*
 * Given an IP and interface, add them to the nodes we know to contact.
 */
static void
knowlist(int i, char *addr, char *iface)
{
    V.siblings |= 1 << i;
    if (!msg_addraddr(NULL, addr, iface))
        fatal("bad remote address: %s", addr);
}


/*
 * Free a pointer if it is not null.
 */
static void
zfree(void *p)
{
    if (p)
        plat_free(p);
}


/*
 * Liveness callback function.
 */
static void
live_func(int live, int rank, void *arg)
{
    t_smsg(0, "live: %s rank=%d", live ? "LIVE" : "DEAD", rank);
    xasubs(&V.live, rank);
    ((char *)V.live.p)[rank] = live;
    if (!live)
        node_died(rank);
}


/*
 * Determine if a node is live.
 */
int
sdf_msg_is_live(int rank)
{
    if (rank < 0 || rank >= V.live.n)
        return 0;
    return ((char *)V.live.p)[rank];
}


/*
 * Show statistics.
 */
static void
show_stats(void)
{
    stat_t stat;
    xstr_t *xp = &stat.xstr;

    stat_make(&stat);
    xsinit(xp);
    xsprint(xp, "========================================\n");
    xsprint(xp, "Statistics\n");
    xsprint(xp, "----------\n");
    cb_callarg(&Stat.call, &stat);
    xsprint(xp, "========================================\n");
    fputs(xp->p, stderr);
    stat_free(&stat);
}


/*
 * Show our local statistics.  The i_ variables are the incremental values and
 * the c_ variables are the cumulative values.
 */
static void
our_stats(stat_t *s)
{
    double i_elapsed;
    double c_elapsed;
    double i_sent_mesg;
    double c_sent_mesg;
    double i_recv_mesg;
    double c_recv_mesg;
    double i_sent_byte;
    double c_sent_byte;
    double i_recv_byte;
    double c_recv_byte;
    double i_sent_bw = 0;
    double c_sent_bw = 0;
    double i_recv_bw = 0;
    double c_recv_bw = 0;
    double i_sent_mr = 0;
    double c_sent_mr = 0;
    double i_recv_mr = 0;
    double c_recv_mr = 0;
    vars_stat_t stat = Stat;

    wl_lock(SQPostLock);
    stat.live_sendq_max = Stat.live_sendq_max;
    Stat.live_sendq_max = 0;
    wl_unlock(SQPostLock);

    wl_lock(StatRLock);
    stat.live_recvq_max = Stat.live_recvq_max;
    Stat.live_recvq_max = 0;
    stat.live_recvq_any_max = Stat.live_recvq_any_max;
    Stat.live_recvq_any_max = 0;
    wl_unlock(StatRLock);

    Stat.prev_time          = msg_ntime();
    Stat.prev_sent_mesg     = stat.live_sent_mesg;
    Stat.prev_sent_byte     = stat.live_sent_byte;
    Stat.prev_recv_mesg     = stat.live_recv_mesg;
    Stat.prev_recv_byte     = stat.live_recv_byte;
    Stat.best_sendq_max     = max(Stat.best_sendq_max, stat.live_sendq_max);
    Stat.best_recvq_max     = max(Stat.best_recvq_max, stat.live_recvq_max);
    Stat.best_recvq_any_max = max(Stat.best_recvq_any_max,
                                   stat.live_recvq_any_max);
    Stat.prev_no_msg_bufs   = stat.live_no_msg_bufs;

    /* Elapsed times */
    c_elapsed = (double) (Stat.prev_time - V.epoch)        / NANO;
    i_elapsed = (double) (Stat.prev_time - stat.prev_time) / NANO;

    /* Cumulative messages and bytes */
    c_sent_mesg = stat.live_sent_mesg;
    c_recv_mesg = stat.live_recv_mesg;
    c_sent_byte = stat.live_sent_byte;
    c_recv_byte = stat.live_recv_byte;

    /* Incremental messages and bytes */
    i_sent_mesg = stat.live_sent_mesg - stat.prev_sent_mesg;
    i_recv_mesg = stat.live_recv_mesg - stat.prev_recv_mesg;
    i_sent_byte = stat.live_sent_byte - stat.prev_sent_byte;
    i_recv_byte = stat.live_recv_byte - stat.prev_recv_byte;

    /* Cumulative message rate and bandwidth */
    if (c_elapsed) {
        c_sent_mr = c_sent_mesg / c_elapsed;
        c_recv_mr = c_recv_mesg / c_elapsed;
        c_sent_bw = c_sent_byte / c_elapsed;
        c_recv_bw = c_recv_byte / c_elapsed;
    }

    /* Incremental message rate and bandwidth */
    if (i_elapsed) {
        i_sent_mr = i_sent_mesg / i_elapsed;
        i_recv_mr = i_recv_mesg / i_elapsed;
        i_sent_bw = i_sent_byte / i_elapsed;
        i_recv_bw = i_recv_byte / i_elapsed;
    }

    stat_labl(s, "send bw");
    stat_rate(s, NULL, i_sent_bw);
    stat_rate(s, "cum", c_sent_bw);
    stat_endl(s);

    stat_labl(s, "recv bw");
    stat_rate(s, NULL, i_recv_bw);
    stat_rate(s, "cum", c_recv_bw);
    stat_endl(s);

    stat_labl(s, "send mr");
    stat_rate(s, NULL, i_sent_mr);
    stat_rate(s, "cum", c_sent_mr);
    stat_endl(s);

    stat_labl(s, "recv mr");
    stat_rate(s, NULL, i_recv_mr);
    stat_rate(s, "cum", c_recv_mr);
    stat_endl(s);

    stat_labl(s, "msgs sent");
    stat_long(s, NULL, i_sent_mesg);
    stat_long(s, "cum", c_sent_mesg);
    stat_endl(s);

    stat_labl(s, "msgs recv");
    stat_long(s, NULL, i_recv_mesg);
    stat_long(s, "cum", c_recv_mesg);
    stat_endl(s);

    stat_labl(s, "bytes sent");
    stat_long(s, NULL, i_sent_byte);
    stat_long(s, "cum", c_sent_byte);
    stat_endl(s);

    stat_labl(s, "bytes recv");
    stat_long(s, NULL, i_recv_byte);
    stat_long(s, "cum", c_recv_byte);
    stat_endl(s);

    stat_labl(s, "max sendq size");
    stat_long(s, NULL, stat.live_sendq_max);
    stat_long(s, "cum", Stat.best_sendq_max);
    stat_endl(s);

    stat_labl(s, "max recvq size");
    stat_long(s, NULL, stat.live_recvq_max);
    stat_long(s, "cum", Stat.best_recvq_max);
    stat_endl(s);

    stat_labl(s, "max single recvq size");
    stat_long(s, NULL, stat.live_recvq_any_max);
    stat_long(s, "cum", Stat.best_recvq_any_max);
    stat_endl(s);

    stat_labl(s, "msg buffers in use");
    stat_long(s, NULL, stat.live_no_msg_bufs - stat.prev_no_msg_bufs);
    stat_long(s, "cum", stat.live_no_msg_bufs);
    stat_endl(s);
}


/*
 * Report our version number.
 */
int
sdf_msg_report_version(char **buf, int *len)
{
    return plat_snprintfcat(buf, len, "sdfmsg %d.0.0\r\n", VER_SDFMSG);
}


/*
 * Declare ourselves alive.
 */
void
sdf_msg_alive(void)
{
    msg_map_alive();
}


/*
 * Return my rank.
 */
int
sdf_msg_myrank(void)
{
    return Conf.myrank;
}


/*
 * Return the number of ranks.
 */
int
sdf_msg_numranks(void)
{
    return msg_map_numranks();
}


/*
 * Return the lowest rank.
 */
int
sdf_msg_lowrank(void)
{
    return msg_map_lowrank();
}


/*
 * Return a sorted list of the valid ranks.
 */
int *
sdf_msg_ranks(int *np)
{
    return msg_map_ranks(np);
}


/*
 * Return the header size that is appended to the message.
 */
int
sdf_msg_hlen(int rank)
{
    int size = msg_hlen(rank);

    if (size < 0)
        return size;
    return sizeof(sdf_msg_t) + size;
}


/*
 * Wait until the message queue is drained and all messages have been sent.
 */
void
sdf_msg_drain(void)
{
    while (SendQ.posthead)
        sched_yield();
    msg_drain();
}


/*
 * Create a queue pair.
 */
qpair_t *
sdf_create_queue_pair(vnode_t srank, vnode_t drank,
                      service_t sserv, service_t dserv, sdf_qwt_t wtype)
{
    readq_t *readq;
    qpair_t *qpair;
    sdf_queue_t *queue;

    t_smsg(0, "init: create queue pair=(%d:%d<=%d:%d)",
           srank, sserv, drank, dserv);

    readq = m_malloc(sizeof (*readq), "sdf_msg:readq_t");
    clear(*readq);
    readq->valid = 1;
    readq->srank = drank;
    readq->sserv = dserv;
    readq->dserv = sserv;
    readq->wtype = wtype;

    if (wtype == SDF_WAIT_FTH)
        fthMboxInit(&readq->fthmbox);
    else if (wtype == SDF_WAIT_CONDVAR) {
        pthread_mutex_init(&readq->pmutex, NULL);
        pthread_cond_init(&readq->pcondv, NULL);
    } else
        fatal("bad wait type: %d", wtype);

    readq_add(readq);
    qpair = m_malloc(sizeof (*qpair), "qpair_t");
    clear(*qpair);
    queue = m_malloc(sizeof (*queue), "sdf_queue_t");
    clear(*queue);
    qpair->q_out = queue;
    queue->readq = readq;

    stray_jolt();
    return qpair;
}


/*
 * Delete a queue pair.  We do not delete the actual readq to avoid locking
 * problems but just make it as invalid.
 */
void
sdf_delete_queue_pair(qpair_t *qpair)
{
    ((readq_t *)qpair->q_out->readq)->valid = 0;
    m_free(qpair->q_out);
    m_free(qpair);
}


/*
 * Post a message to a queue pair.
 */
void
sdf_msg_post(qpair_t *qpair, sdf_msg_t *msg)
{
    readq_post((readq_t *)qpair->q_out->readq, msg);
}


/*
 * Receive a message on a queue pair.
 */
sdf_msg_t *
sdf_msg_recv(qpair_t *qpair)
{
    return sdf_msg_receive(qpair->q_out);
}


/*
 * Receive a message on a queue.  This function is deprecated.  We should use
 * sdf_msg_recv instead.
 */
sdf_msg_t *
sdf_msg_receive(sdf_queue_t *queue)
{
    sdf_msg_t *msg;
    readq_t *readq;

    if (!V.start)
        fatal("messaging system not started");

    readq = queue->readq;
    t_smsg(0, "recv: => sdf_msg_receive addr=(%d:%d<=%d:%d)",
           Conf.myrank, readq->dserv, readq->srank, readq->sserv);

    if (readq->wtype == SDF_WAIT_FTH)
        msg = (sdf_msg_t *) fthMboxWait(&readq->fthmbox);
    else if (readq->wtype == SDF_WAIT_CONDVAR) {
        pthread_mutex_lock(&readq->pmutex);
        while (!readq->pqhead)
            pthread_cond_wait(&readq->pcondv, &readq->pmutex);
        msg = readq->pqhead;
        if (msg == readq->pqtail)
            readq->pqtail = NULL;
        readq->pqhead = msg->next;
        pthread_mutex_unlock(&readq->pmutex);
    } else
        fatal("bad wait type: %d", readq->wtype);

    atomic_dec(Stat.live_recvq);
    atomic_dec(readq->count);
    t_smsg(0, "recv: sdf_msg_receive queue=(%d:%d<=%d:%d) "
              "addr=(%d:%d<=%d:%d) id=%ld len=%d",
            Conf.myrank, readq->dserv, readq->srank, readq->sserv,
            msg->msg_dest_vnode, msg->msg_dest_service, msg->msg_src_vnode,
            msg->msg_src_service, msg->sent_id, msg->msg_len);
    return msg;
}


/*
 * Send a message and return its response.  This returns NULL on failure.
 */
sdf_msg_t *
sdf_msg_send_receive(sdf_msg_t *smsg, uint32_t len, vnode_t drank,
                     service_t dserv, vnode_t srank, service_t sserv,
                     msg_type_t type, int release)
{
    sdf_msg_t *rmsg;
    fthMbox_t rbox;
    sdf_fth_mbx_t sfm ={
        .actlvl = SACK_RESP_ONLY_FTH,
        .rbox = &rbox,
        .release_on_send = release
    };

    fthMboxInit(&rbox);
    (void) sdf_msg_send(smsg, len, drank, dserv,
                        srank, sserv, type, &sfm, NULL);
    rmsg = (sdf_msg_t *) fthMboxWait(&rbox);
    fthMboxTerm(&rbox);
    return rmsg;
}


/*
 * Return the fth mailbox that a response would go to.
 */
fthMbox_t *
sdf_msg_response_rbox(sdf_resp_mbx_t *srm)
{
    fthMbox_t *rbox;
    sendq_t *sendq = sendq_hget(srm->resp_id);

    if (!sendq)
        fatal("failed to find response");
    rbox = sendq->rbox;
    if (!rbox)
        fatal("no response requested");
    sendq_free(sendq);
    return rbox;
}


/*
 * Populate a message with the fields that are used.
 */
int
sdf_msg_init_synthetic(sdf_msg_t *msg, uint32_t len, vnode_t drank,
                       service_t dserv, vnode_t srank, service_t sserv,
                       msg_type_t type, sdf_fth_mbx_t *sfm,
                       sdf_resp_mbx_t *srm)
{
    clear(*msg);
    set_msg(msg, srank, sserv, drank, dserv, sizeof(*msg)+len);
    msg->msg_type = type;
    return 0;
}


/*
 * Return the error status if an error was encountered.
 */
err_t
sdf_msg_get_error_status(sdf_msg_t *msg)
{
    if (msg->msg_type != SDF_MSG_ERROR)
        return SDF_SUCCESS;
    return ((sdf_msg_error_payload_t *)msg->msg_payload)->error;
}


/*
 * Send a message.  This returns 0 on success and 1 on failure.  Note that if
 * an acknowledge is requested through an fth mailbox once the message is sent,
 * the fth mailbox will contain 1 on success and -1 on error.
 */
int
sdf_msg_send(sdf_msg_t *msg, uint32_t len, vnode_t drank, service_t dserv,
             vnode_t srank, service_t sserv, msg_type_t type,
             sdf_fth_mbx_t *sfm, sdf_resp_mbx_t *srm)
{
    sendq_t *sendq = sendq_alloc();
    atom_t id = atomic_inc_get(V.sendmid);

    if (!V.start)
        fatal("messaging system not started");

    if (t_on(SMSG)) {
        xstr_t xstr;
        int l = sfm ? sfm->actlvl : 0;

        xsinit(&xstr);
        xsprint(&xstr, "send: sdf_msg_send addr=(%d:%d=>%d:%d) len=%d id=%ld",
                Conf.myrank, sserv, drank, dserv, len, id);
        if (type)
            xsprint(&xstr, " type=%d", type);
        if (srm)
            xsprint(&xstr, " to=%ld", srm->resp_id);
        if (l == SACK_ONLY_FTH || l == SACK_BOTH_FTH)
            xsprint(&xstr, " arq");
        if ((l == SACK_RESP_ONLY_FTH)
        ||  (l == SACK_BOTH_FTH)
        ||  (l == SACK_MODERN && sfm && sfm->raction))
            xsprint(&xstr, " rrq");
        if (l == SACK_MODERN)
            xsprint(&xstr, " mod");
        t_smsg(0, "%s", (char *)xstr.p);
        xsfree(&xstr);
    }

    sendq->id   = id;
    sendq->rank = drank;
    sendq->stag = sserv;
    sendq->dtag = dserv;
    sendq->data = (void *)msg;
    sendq->len  = sizeof(sdf_msg_t) + len;
    sendq->mfree = !sfm ? 0 : sfm->release_on_send;

    msg->cur_ver   = VER_SDFMSG;
    msg->sup_ver   = VER_SDFMSG;
    msg->msg_type  = type;
    msg->msg_flags = 0;

    if (sfm) {
        int l = sfm->actlvl;

        if (l == SACK_MODERN) {
            sendq->ract = sfm->raction;
            if (sendq->ract)
                msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;
        } else {
            if (l == SACK_ONLY_FTH || l == SACK_BOTH_FTH) {
                if (!sfm->abox)
                    fatal("no abox given but actlvl=%d", l);
                sendq->abox = sfm->abox;
            }
            if (l == SACK_RESP_ONLY_FTH || l == SACK_BOTH_FTH) {
                if (!sfm->rbox)
                    fatal("no rbox given but actlvl=%d", l);
                sendq->rbox = sfm->rbox;
                msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;
            }
        }
    }

    if (Conf.timeout)
        sendq->timeout = msg_ntime() + Conf.timeout;

    msg->resp_id = srm ? srm->resp_id : 0;
    if (msg->resp_id)
        msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_INCLUDED;

    if (drank >= V.live.n || !((char *)V.live.p)[drank]) {
        if (msg_rank2nno(drank) < 0) {
            int s = (sendq->ract || sendq->rbox) ? 0 : -1;
            sdf_logi(70037, "sdf_msg_send to dead node: %d", drank);
            fail_op_sendq(sendq, SDF_NODE_DEAD);
            if (sendq->mfree)
                plat_free(sendq->data);
            sendq_free(sendq);
            return s;
        }
    }

    msg_seth2n(msg->cur_ver);
    msg_seth2n(msg->sup_ver);
    msg_seth2n(msg->resp_id);
    msg_seth2n(msg->msg_type);
    msg_seth2n(msg->msg_flags);
    sendq_post(sendq);
    return 0;
}


/*
 * Initialize the response mailbox.
 */
sdf_resp_mbx_t *
sdf_msg_initmresp(sdf_resp_mbx_t *srm)
{
    srm->resp_id = 0;
    return srm;
}


/*
 * Save the message id in the response mailbox and return it.
 */
sdf_resp_mbx_t *
sdf_msg_get_response(sdf_msg_t *msg, sdf_resp_mbx_t *srm)
{
    srm->resp_id = msg->sent_id;
    return srm;
}


/*
 * See if there is a message in an fth mailbox.
 */
uint64_t
sdf_msg_mbox_try(fthMbox_t *mbox)
{
    return fthMboxTry(mbox);
}


/*
 * Wait for a message to appear in an fth mailbox.
 */
uint64_t
sdf_msg_mbox_wait(fthMbox_t *mbox)
{
    return fthMboxWait(mbox);
}


/*
 * Terminate a fth acknowledge mailbox.
 */
void
sdf_msg_abox_term(fthMbox_t *abox)
{
    mbox_untangle(abox);
    while (fthMboxTry(abox))
        ;
    fthMboxTerm(abox);
}


/*
 * Terminate a fth response mailbox.
 */
void
sdf_msg_rbox_term(fthMbox_t *rbox)
{
    mbox_untangle(rbox);
    for (;;) {
        sdf_msg_t *msg = (sdf_msg_t *) fthMboxTry(rbox);
        if (!msg)
            break;
        sdf_msg_free(msg);
    }
    fthMboxTerm(rbox);
}


/*
 * Stop any messaging operations on a fth mailbox.
 */
void
sdf_msg_mbox_stop(fthMbox_t *mbox)
{
    mbox_untangle(mbox);
}


/*
 * Establish a callback to show statistics.
 */
void
sdf_msg_call_stat(void (*func)(stat_t *))
{
    cb_add(&Stat.call, (cb_func_t *)func, NULL);
}


/*
 * A function to help fix places in the code that violate the messaging API and
 * call calloc rather than sdf_msg_alloc.  Hopefully, in time, all code will
 * simply call sdf_msg_alloc and sdf_msg_free.
 */
sdf_msg_t *
sdf_msg_calloc(uint32_t size)
{
    sdf_msg_t *msg;
    int len = size - sizeof(sdf_msg_t);

    if (len < 0)
        fatal("sdf_msg_calloc called with %d < %ld", size, sizeof(sdf_msg_t));
    msg = sdf_msg_alloc(len);
    memset(msg->msg_payload, 0, len);
    return msg;
}


/*
 * Allocate a SDF message structure.
 */
sdf_msg_t *
sdf_msg_alloc(uint32_t size)
{
    sdf_msg_t *msg = m_malloc(sizeof(*msg) + size, "sdf_msg_t");

    atomic_inc(Stat.live_no_msg_bufs);
    clear(*msg);
    return msg;
}


/*
 * Bless a SDF message structure we allocated elsewhere.
 */
void
sdf_msg_bless(sdf_msg_t *msg)
{
    atomic_inc(Stat.live_no_msg_bufs);
}


/*
 * Free a SDF message buffer.  Deprecated.
 */
int
sdf_msg_free_buff(sdf_msg_t *msg)
{
    sdf_msg_free(msg);
    return 0;
}


/*
 * Free a SDF message buffer.
 */
void
sdf_msg_free(sdf_msg_t *msg)
{
    atomic_dec(Stat.live_no_msg_bufs);
    plat_free(msg);
}


/*
 * A new binding with the given service was created.  Move over any messages
 * that were meant for a binding but got stuck in a queue.
 */
void
sdf_msg_new_binding(int service)
{
    readq_t *readq;

    for (readq = ReadQ.list; readq; readq = readq->next) {
        if (service != SERVICE_ANY && readq->dserv != service)
            continue;
        if (readq->wtype != SDF_WAIT_FTH)
            continue;
        readq->move = 1;
    }
    stray_jolt();
}


/*
 * Prepare to call a function from the messaging context.
 */
void
sdf_msg_int_fcall(cb_func_t func, void *arg)
{
    if (cb_add(&V.live_call, func, arg))
        msg_wake();
}


/*
 * Main messaging loop.
 */
static void *
loop(void *arg)
{
    msg_affinity(0);
    while (V.alive) {
        ntime_t now;
        ntime_t end;
        msg_info_t *info;

        cb_callrem(&V.live_call);
        if (Stray.redo) {
            Stray.redo = 0;
            move_msg();
        }

        if (SendQ.posthead)
            do_sends();

        end = -1;
        now = msg_ntime();

        if (V.nxtime_late)
            timed_call(&now, &end, &V.nxtime_late, do_late);
        if (V.nxtime_stat)
            timed_call(&now, &end, &V.nxtime_stat, do_stats);
        if (V.nxtime_timeout)
            timed_call(&now, &end, &V.nxtime_timeout, do_timeout);

        info = msg_map_poll(end);
        if (!info)
            continue;

        if (info->type == MSG_ERECV)
            do_recv(info);
        else if (info->type == MSG_ESENT)
            do_sent(info);
        msg_ifree(info);
    }
    return NULL;
}


/*
 * Handle a message we just received.
 */
static int
arrival(msg_info_t *info)
{
    if (info->type == MSG_ERECV)
        do_recv1(info);
    else {
        if (info->type == MSG_ESENT)
            do_sent(info);
        info->func = msg_ifree;
    }
    return 1;
}


/*
 * Handle a message we just received.
 */
static void
do_recv1(msg_info_t *info)
{
    int rank;
    sdf_msg_t *msg;

    atomic_inc(Stat.live_recv_mesg);
    atomic_add(Stat.live_recv_byte, info->len);
    rank = msg_nno2rank(info->nno);
    if (rank < 0) {
        sdf_logi(70038, "received message from unknown node m%d", info->nno);
        goto drop;
    }

    if (info->len < sizeof (sdf_msg_t)) {
        sdf_logi(70039, "received truncated message from node n%d", rank);
        goto drop;
    }

    atomic_inc(Stat.live_no_msg_bufs);
    msg = (sdf_msg_t *) info->data;
    msg_setn2h(msg->cur_ver);
    msg_setn2h(msg->sup_ver);
    msg_setn2h(msg->resp_id);
    msg_setn2h(msg->msg_type);
    msg_setn2h(msg->msg_flags);

    if (msg->cur_ver != VER_SDFMSG) {
        sdf_logi(70040, "received message from node n%d: "
                 "bad sdfmsg version: %d", rank, msg->cur_ver);
        goto drop;
    }

    set_msg(msg, rank, info->stag, Conf.myrank, info->dtag, info->len);
    msg->sent_id = info->mid;

    if (do_response(msg)) {
        info->data = NULL;
        info->func = msg_ifree;
    } else
        info->func = do_recv2;
    return;

drop:
    drop_node(info->nno);
    info->func = msg_ifree;
}


/*
 * Handle a message we just received.
 */
static void
do_recv2(msg_info_t *info)
{
    sdf_msg_t *msg = (sdf_msg_t *) msg_idata(info);

    msg_ifree(info);
    if (do_bindingq(msg))
        return;

    sdf_logi(70041, "stray message: addr=(%d:%d<=%d:%d) id=%ld len=%d",
             Conf.myrank, msg->msg_dest_service, msg->msg_src_vnode,
             msg->msg_src_service, msg->sent_id, msg->msg_len);
    stray_post(msg);
}


/*
 * If it is time to call a scheduled function, do so.
 */
static void
timed_call(ntime_t *nowp, ntime_t *endp, ntime_t *nxtimep, void (*func)())
{
    if (!*nxtimep)
        return;
    if (*nxtimep <= *nowp) {
        (*func)();
        *nowp = msg_ntime();
        if (!*nxtimep)
            return;
    }

    if (*endp < 0 || *endp > *nxtimep)
        *endp = *nxtimep;
}


/*
 * Send any messages on the send queue.
 */
static void
do_sends(void)
{
    sendq_t *sendq;
    sendq_t *nextq;

    for (sendq = sendq_head(); sendq; sendq = nextq) {
        nextq = sendq->next;
        sendq_send(sendq);
    }
}


/*
 * Generate an error as a response to a node.
 */
static void
send_error(sendq_t *sendq, int error)
{
    if (sendq->abox)
        fthMboxPost(sendq->abox, -1);

    if (sendq->rbox) {
        sdf_msg_t *msg;
        msg = err_msg(Conf.myrank, sendq->stag,
                      sendq->rank, sendq->dtag, error);
        fthMboxPost(sendq->rbox, (uint64_t)msg);
    }
}


/*
 * Handle a message we just received.
 */
static void
do_recv(msg_info_t *info)
{
    int rank;
    sdf_msg_t *msg;

    atomic_inc(Stat.live_recv_mesg);
    atomic_add(Stat.live_recv_byte, info->len);
    rank = msg_nno2rank(info->nno);
    if (rank < 0) {
        sdf_logi(70038, "received message from unknown node m%d", info->nno);
        return drop_node(info->nno);
    }

    if (info->len < sizeof (sdf_msg_t)) {
        sdf_logi(70039, "received truncated message from node n%d", rank);
        return drop_node(info->nno);
    }

    atomic_inc(Stat.live_no_msg_bufs);
    msg = (sdf_msg_t *) msg_idata(info);
    msg_setn2h(msg->cur_ver);
    msg_setn2h(msg->sup_ver);
    msg_setn2h(msg->resp_id);
    msg_setn2h(msg->msg_type);
    msg_setn2h(msg->msg_flags);

    if (msg->cur_ver != VER_SDFMSG) {
        sdf_logi(70040, "received message from node n%d: "
                 "bad sdfmsg version: %d", rank, msg->cur_ver);
        m_free(msg);
        return drop_node(info->nno);
    }

    set_msg(msg, rank, info->stag, Conf.myrank, info->dtag, info->len);
    msg->sent_id = info->mid;

    if (do_response(msg))
        return;
    if (do_bindingq(msg))
        return;

    sdf_logi(70041, "stray message: addr=(%d:%d<=%d:%d) id=%ld len=%d",
             Conf.myrank, msg->msg_dest_service, msg->msg_src_vnode,
             msg->msg_src_service, msg->sent_id, msg->msg_len);
    stray_post(msg);
}


/*
 * If the message we received was a response message, handle it.
 */
static int
do_response(sdf_msg_t *msg)
{
    sendq_t *sendq;
    msg_id_t id = msg->resp_id;

    if (!id)
        return 0;

    sendq = sendq_hget(id);
    t_smsg(0, "recv: rcvd resp id=%ld", id);
    if (!sendq) {
        recv_err(msg, "failed to match response mid=%ld", id);
        sdf_msg_free(msg);
    } else {
        if (sendq->abox) {
            t_smsg(0, "recv: rcvd sent msg ack early id=%ld", id);
            fthMboxPost(sendq->abox, 1);
        }

        if (sendq->ract)
            sdf_msg_action_apply(sendq->ract, msg);
        else if (sendq->rbox)
            fthMboxPost(sendq->rbox, (uint64_t)msg);
        else {
            recv_err(msg, "failed to match response mid=%ld", id);
            sdf_msg_free(msg);
        }
        sendq_free(sendq);
    }
    return 1;
}


/*
 * If the message we received is handled by a binding or a queue, handle it.
 */
static int
do_bindingq(sdf_msg_t *msg)
{
    readq_t *readq;
    int len = msg->msg_len - sizeof(sdf_msg_t);
    int s = sdf_msg_binding_match(msg);

    if (s) {
        t_smsg(0, "recv: matched binding=(%d:%d<=%d:%d) len=%d id=%lu",
               msg->msg_dest_vnode, msg->msg_dest_service, msg->msg_src_vnode,
               msg->msg_src_service, len, msg->sent_id);
        if (s < 0)
            recv_err(msg, "sdf_msg_binding_match failed");
        return 1;
    }

    readq = readq_find(msg);
    if (readq) {
        t_smsg(0, "recv: matched queue=(%d:%d<=%d:%d) "
                  "addr=(%d:%d<=%d:%d) len=%d id=%lu",
               Conf.myrank, readq->dserv, readq->srank, readq->sserv,
               msg->msg_dest_vnode, msg->msg_dest_service, msg->msg_src_vnode,
               msg->msg_src_service, len, msg->sent_id);
        readq_post(readq, msg);
        return 1;
    }
    return 0;
}


/*
 * Handle a message that was just sent.
 */
static void
do_sent(msg_info_t *info)
{
    char *error = info->error;
    sendq_t *sendq = sendq_hget(info->mid);

    if (!sendq)
        return;

    if (error) {
        err_t err;
        if (streq(error, "node down"))
            err = SDF_NODE_DEAD;
        else if (streq(error, "bad node number"))
            err = SDF_NODE_INVALID;
        else
            err = SDF_FAILURE;

        fail_op_sendq(sendq, err);
        sendq_free(sendq);
        return;
    }

    if (sendq->abox) {
        t_smsg(0, "recv: rcvd sent msg ack id=%ld", info->mid);
        fthMboxPost(sendq->abox, 1);
        sendq->abox = NULL;
    }

    if (sendq->rbox || sendq->ract)
        sendq_hput(sendq);
    else
        sendq_free(sendq);
}


/*
 * Let the watchdog know about any nodes that are late to join.  This only
 * happens once.
 */
static void
do_late(void)
{
    int i;
    uint64_t b = V.siblings;

    V.nxtime_late = 0;
    for (i = 0; b; i++, b >>= 1) {
        if (msg_rank2nno(i) <= 0) {
            FILE *fp = wdog_send("node_state %d down", i);
            if (fp)
                fclose(fp);
        }
    }
}


/*
 * Let the watchdog know about any nodes that are late to join.
 */
static void
do_stats(void)
{
    ntime_t f = Conf.statfreq;

    show_stats();
    V.nxtime_stat += (((msg_ntime()-V.nxtime_stat)/f)+1) * f;
}


/*
 * If any operations need to be timed out, do so.
 */
static void
do_timeout(void)
{
    int n;
    sendq_t  *p;
    sendq_t **pp;
    sendq_t *later = NULL;
    ntime_t now = msg_ntime();
    sendq_t **headq = SendQ.hashlist;
    wlock_t **lockp = SendQ.hashlock;
    ntime_t timeout = 0;

    V.nxtime_timeout = 0;
    for (n = NHASH; n--; headq++, lockp++) {
        if (!*headq)
            continue;
        wl_lock(*lockp);
        for (pp = headq; (p = *pp) != NULL;) {
            if (p->timeout && p->timeout <= now) {
                *pp = p->next;
                p->next = later;
                later = p;
            } else {
                pp = &p->next;
                timeout = mintime(timeout, p->timeout);
            }
        }
        wl_unlock(*lockp);
    }

    for (p = later; p; p = p->next)
        fail_op_sendq(p, SDF_TIMEOUT);
    sendq_freelist(later);
    ntime_amin(&V.nxtime_timeout, timeout);
}


/*
 * Remove any references to a particular mailbox.  While we are here, update
 * the next timeout.
 */
static void
mbox_untangle(fthMbox_t *mbox)
{
    int n;
    sendq_t  *p;
    sendq_t **pp;
    sendq_t *later = NULL;
    sendq_t **headq = SendQ.hashlist;
    wlock_t **lockp = SendQ.hashlock;
    ntime_t timeout = 0;

    V.nxtime_timeout = 0;
    for (n = NHASH; n--; headq++, lockp++) {
        if (!*headq)
            continue;
        wl_lock(*lockp);
        for (pp = headq; (p = *pp) != NULL;) {
            if (p->abox == mbox)
                p->abox = NULL;
            if (p->rbox == mbox)
                p->rbox = NULL;

            if (!p->abox && !p->rbox && !p->ract) {
                *pp = p->next;
                p->next = later;
                later = p;
            } else {
                pp = &p->next;
                timeout = mintime(timeout, p->timeout);
            }
        }
        wl_unlock(*lockp);
    }

    sendq_freelist(later);
    ntime_amin(&V.nxtime_timeout, timeout);
}


/*
 * A node just died.  Fail relevant operations and inform people who care.
 */
static void
node_died(int rank)
{
    node_died_sendq(rank);
    node_died_readq(rank);
}


/*
 * Fail all operations waiting for responses from the given node.
 */
static void
node_died_sendq(int rank)
{
    int n;
    sendq_t  *p;
    sendq_t **pp;
    sendq_t *later = NULL;
    sendq_t **headq = SendQ.hashlist;
    wlock_t **lockp = SendQ.hashlock;

    for (n = NHASH; n--; headq++, lockp++) {
        if (!*headq)
            continue;
        wl_lock(*lockp);
        for (pp = headq; (p = *pp) != NULL;) {
            if (p->rank == rank) {
                *pp = p->next;
                p->next = later;
                later = p;
            } else
                pp = &p->next;
        }
        wl_unlock(*lockp);
    }

    for (p = later; p; p = p->next)
        fail_op_sendq(p, SDF_NODE_DEAD);
    sendq_freelist(later);
}


/*
 * If anyone is waiting for messages from a node that died, inform them.
 */
static void
node_died_readq(int rank)
{
    sdf_msg_t *msg;
    readq_t *readq;

    for (readq = ReadQ.list; readq; readq = readq->next) {
        if (readq->srank != rank)
            continue;
        msg = err_msg(rank, readq->sserv,
                      Conf.myrank, readq->dserv, SDF_NODE_DEAD);
        readq_post(readq, msg);
    }
}


/*
 * Cause a node to drop and log it appropriately.
 */
static void
drop_node(int nno)
{
    sdf_logi(70042, "bad data from n%d(%d); dropped", msg_nno2rank(nno), nno);
    msg_nodedrop(nno);
}


/*
 * Fail an operation.  This happens when an operation times out or a remote
 * node dies.
 */
static void
fail_op_sendq(sendq_t *sendq, err_t error)
{
    sdf_msg_t *msg;

    t_smsg(0, "failing op due to %s id=%ld stag=%d dtag=%d rank=%d",
           error_name(error), sendq->id, sendq->stag, sendq->dtag, sendq->rank);

    if (sendq->abox)
        fthMboxPost(sendq->abox, 0);

    msg = err_msg(sendq->rank, sendq->dtag, Conf.myrank, sendq->stag, error);
    if (sendq->ract)
        sdf_msg_action_apply(sendq->ract, msg);
    else if (sendq->rbox)
        fthMboxPost(sendq->rbox, (uint64_t)msg);
    else
        sdf_msg_free(msg);
}


/*
 * Allocate a message and initialize it to signify an error.
 */
static sdf_msg_t *
err_msg(int srank, int sserv, int drank, int dserv, err_t error)
{
    int len = sizeof (sdf_msg_error_payload_t);
    sdf_msg_t *msg = sdf_msg_alloc(len);

    set_msg(msg, srank, sserv, drank, dserv, sizeof(*msg)+len);
    msg->msg_type  = SDF_MSG_ERROR;
    msg->msg_flags = SDF_MSG_FLAG_MBX_RESP_INCLUDED;
    ((sdf_msg_error_payload_t *)&msg->msg_payload)->error = error;
    return msg;
}


/*
 * Add a readq entry to our list.
 */
static void
readq_add(readq_t *readq)
{
    readq_t *q;

    wl_lock(ReadQ.lock);
    for (q = ReadQ.list; q; q = q->next) {
        if (readq_match(q, readq->srank, readq->sserv, readq->dserv))
            sdf_logi(70043, "creating queue=(%d:%d=>%d:%d) "
                     "but (%d:%d=>%d:%d) already exists",
                     readq->srank, readq->sserv, Conf.myrank, readq->dserv,
                     q->srank, q->sserv, Conf.myrank, q->dserv);
    }
    readq->next = ReadQ.list;
    ReadQ.list = readq;
    wl_unlock(ReadQ.lock);
}


/*
 * Find a read queue entry.  We do not need to lock the queue as we only add
 * entries to the head and once we read ReadQ.list, it should never change.
 */
static readq_t *
readq_find(sdf_msg_t *msg)
{
    readq_t *q;
    readq_t *bestq = NULL;
    int bestm = 0;
    int srank = msg->msg_src_vnode;
    int sserv = msg->msg_src_service;
    int dserv = msg->msg_dest_service;

    for (q = ReadQ.list; q; q = q->next) {
        int m = readq_match(q, srank, sserv, dserv);

        if (m && (!bestm || m < bestm)) {
            bestm = m;
            bestq = q;
        }
    }
    return bestq;
}


/*
 * See if a read queue matches.  If it does not match, return 0.  If it matches
 * exactly, return 1.  Otherwise return 1 plus the number of wildcard matches.
 */
static int
readq_match(readq_t *readq, int srank, int sserv, int dserv)
{
    int wild = 0;

    if (!readq->valid)
        return 0;

    if (readq->srank == VNODE_ANY || srank == VNODE_ANY)
        wild++;
    else if (readq->srank != srank)
        return 0;

    if (readq->sserv == SERVICE_ANY || sserv == SERVICE_ANY)
        wild++;
    else if (readq->sserv != sserv)
        return 0;

    if (readq->dserv == SERVICE_ANY || dserv == SERVICE_ANY)
        wild++;
    else if (readq->dserv != dserv)
        return 0;

    return 1+wild;
}


/*
 * Post a message to a read queue.
 */
static void
readq_post(readq_t *readq, sdf_msg_t *msg)
{
    int64_t all;
    int64_t any;

    if (readq->wtype == SDF_WAIT_FTH)
        fthMboxPost(&readq->fthmbox, (uint64_t)msg);
    else if (readq->wtype == SDF_WAIT_CONDVAR) {
        pthread_mutex_lock(&readq->pmutex);
        if (!readq->pqhead)
            readq->pqhead = readq->pqtail = msg;
        else
            readq->pqtail = readq->pqtail->next = msg;
        pthread_mutex_unlock(&readq->pmutex);
        pthread_cond_signal(&readq->pcondv);
    } else
        fatal("bad wait type: %d", readq->wtype);

    all = atomic_inc_get(Stat.live_recvq);
    any = atomic_inc_get(readq->count);
    if (all > Stat.live_recvq_max || any > Stat.live_recvq_any_max) {
        wl_lock(StatRLock);
        if (all > Stat.live_recvq_max)
            Stat.live_recvq_max = all;
        if (any > Stat.live_recvq_any_max)
            Stat.live_recvq_any_max = any;
        wl_unlock(StatRLock);
    }
}


/*
 * Delete the read queues.  This is only done on exit so we assume that
 * everyone is quiescient.
 */
static void
readq_listdel(void)
{
    readq_t *next;
    readq_t *readq;

    wl_lock(ReadQ.lock);
    readq = ReadQ.list;
    ReadQ.list = NULL;
    wl_unlock(ReadQ.lock);

    while (readq) {
        if (readq->wtype == SDF_WAIT_FTH)
            fthMboxTerm(&readq->fthmbox);
        else if (readq->wtype == SDF_WAIT_CONDVAR) {
            pthread_mutex_destroy(&readq->pmutex);
            pthread_cond_destroy(&readq->pcondv);
        } else
            fatal("bad wait type: %d", readq->wtype);
        next = readq->next;
        m_free(readq);
        readq = next;
    }
}


/*
 * Log a receive error.
 *
 */
static void
recv_err(sdf_msg_t *msg, char *fmt, ...)
{
    xstr_t xstr;
    va_list alist;

    xsinit(&xstr);
    va_start(alist, fmt);
    xsvprint(&xstr, fmt, alist);
    va_end(alist);
    sdf_loge(70044, "%s: addr=(%d:%d=>%d:%d)", (char *)xstr.p,
             msg->msg_src_vnode, msg->msg_src_service,
             Conf.myrank, msg->msg_dest_service);
    xsfree(&xstr);
}


/*
 * Set the source and destination node and service fields of a message along
 * with its length.
 */
static void
set_msg(sdf_msg_t *msg, int srank, int sserv, int drank, int dserv, int len)
{
    msg->msg_src_vnode    = srank;
    msg->msg_src_service  = sserv;
    msg->msg_dest_vnode   = drank;
    msg->msg_dest_service = dserv;
    msg->msg_len          = len;
}


/*
 * Send a message from a sendq.
 */
static void
sendq_send(sendq_t *sendq)
{
    msg_send_t *msend;
    int nno = msg_map_send(sendq->rank);

    if (nno < 0) {
        send_error(sendq, SDF_NODE_DEAD);
        sendq_free(sendq);
        return;
    }

    msend = msg_salloc();
    msend->sid  = sendq->id;
    msend->nno  = nno;
    msend->stag = sendq->stag;
    msend->dtag = sendq->dtag;
    msend->nsge = 1;
    msend->sge[0].iov_base = sendq->data;
    msend->sge[0].iov_len = sendq->len;

    if (sendq->mfree) {
        atomic_dec(Stat.live_no_msg_bufs);
        msend->data = sendq->data;
    }

    atomic_inc(Stat.live_sent_mesg);
    atomic_add(Stat.live_sent_byte, sendq->len);

    if (sendq->abox || sendq->rbox || sendq->ract)
        sendq_hput(sendq);
    else
        sendq_free(sendq);

    msg_send(msend);
}


/*
 * Return the head of the queue and clear the queue.
 */
static sendq_t *
sendq_head(void)
{
    sendq_t *sendq = SendQ.posthead;

    if (sendq) {
        wl_lock(SQPostLock);
        sendq = SendQ.posthead;
        SendQ.posthead = SendQ.posttail = NULL;
        Stat.live_sendq = 0;
        wl_unlock(SQPostLock);
    }
    return sendq;
}


/*
 * Get the next entry from the send queue.
 */
static sendq_t *
sendq_next(void)
{
    sendq_t *sendq = SendQ.posthead;

    if (sendq) {
        wl_lock(SQPostLock);
        sendq = SendQ.posthead;
        if (sendq == SendQ.posttail)
            SendQ.posttail = NULL;
        SendQ.posthead = sendq->next;
        Stat.live_sendq--;
        wl_unlock(SQPostLock);
    }
    return sendq;
}


/*
 * Post a sendq entry.
 */
static void
sendq_post(sendq_t *sendq)
{
    sendq_send(sendq);
}


/*
 * Add an entry to our queue.
 */
static void
sendq_addq(sendq_t *sendq)
{
    int wake = 0;

    sendq->next = NULL;
    wl_lock(SQPostLock);

    if (!SendQ.posthead) {
        wake = 1;
        SendQ.posthead = SendQ.posttail = sendq;
    } else
        SendQ.posttail = SendQ.posttail->next = sendq;

    Stat.live_sendq++;
    if (Stat.live_sendq > Stat.live_sendq_max)
        Stat.live_sendq_max = Stat.live_sendq;

    wl_unlock(SQPostLock);
    if (wake)
        msg_wake();
}


/*
 * Put a sendq entry into the hash table.
 */
static void
sendq_hput(sendq_t *sendq)
{
    int h = hash(sendq->id);
    sendq_t **pp = &SendQ.hashlist[h];
    wlock_t *lock = SendQ.hashlock[h];

    wl_lock(lock);
    sendq->next = *pp;
    *pp = sendq;
    wl_unlock(lock);
    ntime_amin(&V.nxtime_timeout, sendq->timeout);
}


/*
 * Get a matching sendq entry from the hash table.
 */
static sendq_t *
sendq_hget(msg_id_t id)
{
    sendq_t  *sendq;
    int h = hash(id);
    sendq_t **pp = &SendQ.hashlist[h];
    wlock_t *lock = SendQ.hashlock[h];

    wl_lock(lock);
    while ((sendq = *pp) != NULL) {
        if (sendq->id == id) {
            *pp = sendq->next;
            break;
        } else
            pp = &sendq->next;
    }
    wl_unlock(lock);
    return sendq;
}


/*
 * Allocate a send queue entry.
 */
static sendq_t *
sendq_alloc(void)
{
    sendq_t *sendq;

    if (!SendQ.freelist)
        sendq = m_malloc(sizeof(sendq_t), "sdf_msg:sendq_t");
    else {
        wl_lock(SendQ.freelock);
        if (!SendQ.freelist) {
            wl_unlock(SendQ.freelock);
            sendq = m_malloc(sizeof(sendq_t), "sdf_msg:sendq_t");
        } else {
            sendq = SendQ.freelist;
            SendQ.freelist = SendQ.freelist->next;
            wl_unlock(SendQ.freelock);
        }
    }

    clear(*sendq);
    return sendq;
}


/*
 * Free a send queue entry.
 */
static void
sendq_free(sendq_t *sendq)
{
    wl_lock(SendQ.freelock);
    sendq->next = SendQ.freelist;
    SendQ.freelist = sendq;
    wl_unlock(SendQ.freelock);
}


/*
 * Free a list of sendq entries.
 */
static void
sendq_freelist(sendq_t *sendq)
{
    sendq_t *tail;
    sendq_t *next;

    if (!sendq)
        return;

    for (tail = sendq; (next = tail->next) != NULL; tail = next)
        ;

    wl_lock(SendQ.freelock);
    tail->next = SendQ.freelist;
    SendQ.freelist = sendq;
    wl_unlock(SendQ.freelock);
}


/*
 * Free a send queue list.  If freed is set, also free data.
 */
static void
sendq_listdel(sendq_t *sendq, int freed)
{
    while (sendq) {
        sendq_t *next = sendq->next;

        if (freed && sendq->mfree)
            plat_free(sendq->data);
        m_free(sendq);
        sendq = next;
    }
}


/*
 * Post a message onto the stray message list.
 */
static void
stray_post(sdf_msg_t *msg)
{
    msg->next = NULL;
    wl_lock(Stray.lock);
    if (!Stray.head)
        Stray.head = Stray.tail = msg;
    else
        Stray.tail = Stray.tail->next = msg;
    wl_unlock(Stray.lock);
}


/*
 * Jolt the stray code into rechecking the redo list.
 */
static void
stray_jolt(void)
{
    Stray.redo = 1;
    msg_wake();
}


/*
 * See if any of the messages need to be moved.
 */
static void
move_msg(void)
{
    move_msg_stray();
    move_msg_queue();
}


/*
 * If possible, move messages from the stray queue to a SDF queue.
 */
static void
move_msg_stray(void)
{
    sdf_msg_t **pp = &Stray.head;

    wl_lock(Stray.lock);
    for (;;) {
        sdf_msg_t *msg = *pp;

        if (!msg)
            break;
        if (do_bindingq(msg)) {
            t_smsg(0, "delivered stray message: "
                      "addr=(%d:%d=>%d:%d) len=%d id=%lu",
                   msg->msg_src_vnode, msg->msg_src_service, Conf.myrank,
                   msg->msg_dest_service, msg->msg_len, msg->sent_id);
            *pp = msg->next;
        } else
            pp = &msg->next;
    }
    wl_unlock(Stray.lock);
}


/*
 * If necessary, move messages from a SDF queue to a binding.  Note that we do
 * this in two stages just in case do_bindingq happens to be posting it back to
 * the same mailbox.
 */
static void
move_msg_queue(void)
{
    readq_t *readq;
    sdf_msg_t *msg;
    sdf_msg_t *head = NULL;
    sdf_msg_t *tail = NULL;

    for (readq = ReadQ.list; readq; readq = readq->next) {
        if (!readq->move)
            continue;
        readq->move = 0;
        for (;;) {
            msg = (sdf_msg_t *) fthMboxTry(&readq->fthmbox);
            if (!msg)
                break;
            msg->next = NULL;
            if (!head)
                head = tail = msg;
            else
                tail = tail->next = msg;
        }
        for (msg = head; msg; msg = msg->next) {
            if (!do_bindingq(msg))
                fatal("unable to repost message from queue to binding");
            t_smsg(0, "message moved to binding: "
                      "addr=(%d:%d=>%d:%d) len=%d id=%lu",
                   msg->msg_src_vnode, msg->msg_src_service, Conf.myrank,
                   msg->msg_dest_service, msg->msg_len, msg->sent_id);
        }
    }
}


/*
 * Free the stray list.
 */
static void
stray_listdel(void)
{
    sdf_msg_t *msg;

    wl_lock(Stray.lock);
    msg = Stray.head;
    while (msg) {
        sdf_msg_t *next = msg->next;
        m_free(msg);
        msg = next;
    }
    wl_unlock(Stray.lock);
}


/*
 * Return the name of an error.
 */
static char *
error_name(int error)
{
    if (error == SDF_FAILURE)
        return "failure";
    else if (error == SDF_TIMEOUT)
        return "timeout";
    else if (error == SDF_NODE_DEAD)
        return "node dead";
    else if (error == SDF_QUEUE_FULL)
        return "queue full";
    else if (error == SDF_NODE_INVALID)
        return "invalid node";
    else
        return "unknown error";
}


/*
 * This essentially effects *val = min(*val, new) subject to some conditions.
 * The operation is being performed on time values and it is performed
 * atomically as several threads might by attempting to do so at the same time.
 * Also, our domain is the non-negative integers with a modified ordering where
 * 0 is the highest and all other numbers follow the usual ordering.  We return
 * 1 if *val was modified, else 0.
 */
static int
ntime_amin(ntime_t *val, ntime_t new)
{
    if (!new)
        return 0;
    for (;;) {
        uint64_t old = *val;
        if (old && new >= old)
            return 0;
        if (atomic_cmp_swap(*val, old, new) == old)
            return 1;
    }
}


/*
 * Change a string to uppercase.
 */
static void
uppercase(char *str)
{
    for (; *str; str++)
        *str = toupper(*str);
}


/*
 * The function is never called.  The functions listed here are not currently
 * being used.  Only here to avoid compile errors.
 */
void
sdf_msg_unused(void)
{
    sendq_next();
    sendq_addq(0);
}
