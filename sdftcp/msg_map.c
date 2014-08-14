/*
 * File: msg_map.c
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Used to map node indices.  See msg_map.h for a description of the interface.
 */
#include <pthread.h>
#include <arpa/inet.h>
#include "platform/stdio.h"
#include "tools.h"
#include "trace.h"
#include "msg_cat.h"
#include "msg_map.h"
#include "msg_msg.h"
#include "sdfmsg/sdf_msg.h"


/*
 * Configurable parameters.
 */
#define DEF_RANKS   8
#define TALK_MAGIC  0xfdb97531
#define TALK_MID    18181818181818181818ULL


/*
 * Node states.  Ordering is important here.
 */
#define UNUSED  0                       /* Unused */
#define WAITID  1                       /* Waiting for identification */
#define NORANK  2                       /* Waiting to be assigned a rank */
#define ALMOST  3                       /* Waiting for application */
#define ACTIVE  4                       /* Active */


/*
 * Used to talk to other nodes.
 */
typedef struct talk {
    uint32_t    magic;                  /* Magic number */
    uint16_t    state;                  /* State */
    int16_t     rank;                   /* Rank */
} talk_t;


/*
 * The node table.  Used to map between the SDF messaging system and low level
 * messaging.  A node can be in one of several states listed above.
 */
typedef struct node {
    msg_port_t  port;                   /* TCP port */
    int         nno;                    /* Messaging nno */
    int16_t     rank;                   /* Rank */
    uint16_t    state;                  /* State */
} node_t;


/*
 * Function prototypes.
 */
static int         arrival(msg_info_t *info);
static int         info_recv(msg_info_t *info);
static int         info_sent(msg_info_t *info);
static int         cmp_node(const void *p1, const void *p2);
static int         cmp_rank(const void *p1, const void *p2);
static int         recv_talk(talk_t *talk, msg_info_t *info);
static int         bad_talk(talk_t *talk, msg_info_t *info, char *msg);
static void        pick_rank(void);
static void        show_nodes(void);
static void        talk_nodes(void);
static void        node_drop(int nno);
static void        node_del(node_t *node);
static void        send_talk(node_t *node);
static void        wait_nodes(ntime_t etime);
static void        info_push(msg_info_t *info);
static void        set_state(node_t *node, int new);
static char       *n_state(int state);
static node_t     *mynode(void);
static node_t     *node_add(void);
static node_t     *get_node_nno(int nno);
static node_t     *get_node_rank(int rank);
static node_t     *node_join(int nno, int state, int rank);
static msg_info_t *info_next(void);
static msg_info_t *do_info(msg_info_t *info);


/*
 * Static variables.
 */
static int              NodeN;          /* Number of nodes in table */
static int              MyRank;         /* My rank */
static int              NumTalked;      /* Nodes that have responded */
static int              NumRanked;      /* Nodes that are ranked */
static int              NumActive;      /* Number of nodes active */
static int              NumNeeded;      /* Number of nodes needed */
static int              MyNno;          /* My nno */
static node_t          *Nodes;          /* Node table */
static wlock_t         *NodeLock;       /* Lock on Nodes */
static msg_info_t      *InfoHead;       /* Info list head */
static msg_info_t      *InfoTail;       /* Info list tail */


/*
 * Initialize.  etime is the time at which we give up finding our colleagues.
 * noden is the number of colleagues that must be found, including ourselves,
 * before returning.  *myrank is the rank we would like or -1 if we do not
 * care.  The rank we decide on is returned through *myrank.
 */
void
msg_map_init(ntime_t etime, int noden, int *myrank)
{
    Nodes     = NULL;
    NodeN     = 0;
    MyRank    = -1;
    NumTalked = 0;
    NumRanked = 0;
    NumActive = 0;
    NumNeeded = noden;

    InfoHead = NULL;
    InfoTail = NULL;

    NodeLock = wl_init();

    MyNno = msg_mynodeno();
    MyRank = *myrank;
    if (!node_join(MyNno, MyRank>=0 ? ALMOST : NORANK, MyRank))
        fatal("cannot find my node");
    wait_nodes(etime);
    *myrank = MyRank;

    if (0)
        msg_call_post(arrival);

    if (t_on(MAPN))
        show_nodes();
}


/*
 * Clean up.
 */
void
msg_map_exit(void)
{
    if (Nodes)
        m_free(Nodes);

    {
        msg_info_t *info;
        msg_info_t *next;

        for (info = InfoHead; info; info = next) {
            next = info->link;
            msg_ifree(info);
        }
    }

    wl_free(NodeLock);
}


/*
 * Declare ourselves ready.
 */
void
msg_map_alive(void)
{
    if (NodeLock)
        wl_lock(NodeLock);
    set_state(mynode(), ACTIVE);
    talk_nodes();
    if (NodeLock)
        wl_unlock(NodeLock);
}


/*
 * Wait for nodes to come online.
 */
static void
wait_nodes(ntime_t etime)
{
    if (MyRank < 0) {
        t_mapn(0, "waiting for %d TALKED nodes", NumNeeded);
        while (NumTalked < NumNeeded) {
            msg_info_t *info = msg_poll(etime);

            if (!info)
                fatal("only found %d/%d TALKED nodes", NumTalked, NumNeeded);
            if (do_info(info))
                info_push(info);
            else
                msg_ifree(info);
        }
        pick_rank();
    }

    t_mapn(0, "waiting for %d RANKED nodes", NumNeeded);
    while (NumRanked < NumNeeded) {
        msg_info_t *info = msg_poll(etime);

        if (!info)
            fatal("only found %d/%d RANKED nodes", NumRanked, NumNeeded);
        if (do_info(info))
            info_push(info);
        else
            msg_ifree(info);
    }
    t_mapn(0, "found %d RANKED nodes", NumRanked);
}


/*
 * Choose our rank.  The ALMOST and ACTIVE nodes already have assigned ranks.
 * The NORANK nodes are sorted by name and port and the first rank that is not
 * already taken is assigned to each of them in order.  In the end we only care
 * about what our rank is.  Presumably all nodes are using the same algorithm
 * and will assign the same rank.
 */
static void
pick_rank(void)
{
    int i;
    int n;
    int n1;
    node_t *node;

    qsort(Nodes, NodeN, sizeof(*Nodes), cmp_node);
    node = mynode();
    if (node->rank >= 0)
        return;
    if (node->state != NORANK)
        fatal("our node is in a confused state: %s", n_state(node->state));

    for (n1 = 0; n1 < NodeN; n1++) {
        int s = Nodes[n1].state;
        if (s != ACTIVE && s != ALMOST)
            break;
    }
    n = (node-Nodes) - n1;
    MyRank = 0;
    for (i = 0; i < n1; i++) {
        int rank = Nodes[i].rank;

        while (MyRank++ < rank && --n > 0)
            ;
        if (n == 0)
            break;
    }
    MyRank += n;
    node->rank = MyRank;
    msg_setrank(node->nno, MyRank);
    set_state(node, ALMOST);
    talk_nodes();
}


/*
 * Compare function to qsort our node table.
 */
static int
cmp_node(const void *p1, const void *p2)
{
    const node_t *n1 = (node_t *)p1;
    const node_t *n2 = (node_t *)p2;

    /* Highest states at the beginning */
    if (n1->state > n2->state)
        return -1;
    if (n1->state < n2->state)
        return 1;
    if (n1->state == UNUSED)
        return 0;

    /* Lowest SDF indices at beginning */
    if (n1->rank < n2->rank)
        return -1;
    if (n1->rank > n2->rank)
        return 1;

    /* Lower ports at the beginning */
    if (n1->port < n2->port)
        return -1;
    if (n1->port > n2->port)
        return 1;
    return 0;
}



/*
 * Return the number of ranks.
 */
int
msg_map_numranks(void)
{
    int n;
    node_t *node;
    int ranks = 0;

    for (node = Nodes, n = NodeN; n--; node++)
        if (node->state == ALMOST || node->state == ACTIVE)
            ranks++;
    return ranks;
}

/*
 * Return the lowest valid rank.
 */
int
msg_map_lowrank(void)
{
    int n;
    node_t *node;
    int rank = -1;

    for (node = Nodes, n = NodeN; n--; node++)
        if (node->state == ALMOST || node->state == ACTIVE)
            if (rank < 0 || node->rank < rank)
                rank = node->rank;
    return rank;
}


/*
 * Return a list of the valid ranks.
 */
int *
msg_map_ranks(int *np)
{
    int n;
    node_t *node;
    int *p;
    int *ranks;
    int nranks = 0;

    for (node = Nodes, n = NodeN; n--; node++)
        if (node->state == ALMOST || node->state == ACTIVE)
            nranks++;

    p = ranks = m_malloc((nranks+1) * sizeof(ranks[0]), "msg_map:N*int");
    for (node = Nodes, n = NodeN; n--; node++)
        if (node->state == ALMOST || node->state == ACTIVE)
            *p++ = node->rank;
    *p = -1;
    qsort(ranks, nranks, sizeof(ranks[0]), cmp_rank);
    if (np)
        *np = nranks;
    return ranks;
}


/*
 * Compare function to qsort a list of ranks.
 */
static int
cmp_rank(const void *p1, const void *p2)
{
    int r1 = *((int *)p1);
    int r2 = *((int *)p2);

    if (r1 < r2)
        return -1;
    if (r1 > r2)
        return 1;
    return 0;
}


/*
 * Map a node number for sending.  This maps a rank to a nno.  On error, -1 is
 * returned.
 */
int
msg_map_send(int rank)
{
    int n;
    node_t *node;
    int nno = -1;

    for (node = Nodes, n = NodeN; n--; node++) {
        if (node->state == ACTIVE || node->state == ALMOST) {
            if (node->rank == rank) {
                nno = node->nno;
                break;
            }
        }
    }
    
    if (nno < 0 && MyRank >= 0)
        t_mapn(0, "msg_map_send(%d) => %d", rank, nno);
    return nno;
}


/*
 * Map a node number for receiving.  This maps a nno to a rank.  On error, -1
 * is returned.
 */
int
msg_map_recv(int nno)
{
    int n;
    node_t *node;
    int rank = -1;

    for (node = Nodes, n = NodeN; n--; node++) {
        if (node->state == ACTIVE || node->state == ALMOST) {
            if (node->nno == nno) {
                rank = node->rank;
                break;
            }
        }
    }
    
    if (rank < 0 && MyRank >= 0)
        t_mapn(0, "msg_map_recv(%d) => %d", nno, rank);
    return rank;
}


/*
 * If a message has just arrived, determine if it is for us.
 */
static int
arrival(msg_info_t *info)
{
    if (do_info(info))
        return 0;
    info->func = msg_ifree;
    return 1;
}


/*
 * Return the results of poll given priority to our saved info queue.
 */
msg_info_t *
msg_map_poll(ntime_t etime)
{
    msg_info_t *info;

    for (;;) {
        info = info_next();
        if (info)
            return info;

        info = msg_poll(etime);
        if (!info)
            return NULL;

        if (do_info(info))
            return info;
        msg_ifree(info);
    }
}


/*
 * Process an info packet and deal with anything that is relevant.
 */
static msg_info_t *
do_info(msg_info_t *info)
{
    int s;
    node_t *node;

    if (info->type == MSG_EJOIN) {
        wl_lock(NodeLock);
        node = get_node_nno(info->nno);
        if (!node)
            node = node_join(info->nno, WAITID, -1);
        if (node)
            send_talk(node);
        wl_unlock(NodeLock);
    } else if (info->type == MSG_EDROP) {
        wl_lock(NodeLock);
        node_drop(info->nno);
        wl_unlock(NodeLock);
    } else if (info->type == MSG_ERECV) {
        if (info->mid != TALK_MID)
            return info;
        wl_lock(NodeLock);
        s = info_recv(info);
        wl_unlock(NodeLock);
        if (s)
            return info;
    } else if (info->type == MSG_ESENT) {
        if (info->mid != TALK_MID)
            return info;
        wl_lock(NodeLock);
        s = info_sent(info);
        wl_unlock(NodeLock);
        if (s)
            return info;
    }
    return NULL;
}


/*
 * Handle a receive notification and return true if we want it passed on to the
 * user.
 */
static int
info_recv(msg_info_t *info)
{
    talk_t  talk = {};
    node_t *node = get_node_nno(info->nno);

    if (!node)
        node = node_join(info->nno, WAITID, -1);

    if (node->state == ACTIVE) {
        if (info->mid == TALK_MID && recv_talk(&talk, info))
            return bad_talk(&talk, info, "received talk for active node");
        return 1;
    }

    if (!recv_talk(&talk, info)) {
        if (node->rank >= 0)
            return 1;
        return bad_talk(&talk, info, "received non-talk for inactive node");
    }

    if (talk.rank < 0) {
        if (talk.state != NORANK)
            return bad_talk(&talk, info, "bad state and rank in talk message");
    } else {
        node_t *twin = get_node_rank(talk.rank);

        if (twin && twin != node)
            return bad_talk(&talk, info, "claiming rank in use, rejected");
        if (talk.state != ALMOST && talk.state != ACTIVE)
            return bad_talk(&talk, info, "bad state and rank in talk message");
    }

    node->rank = talk.rank;
    msg_setrank(node->nno, talk.rank);
    set_state(node, talk.state);
    return 0;
}


/*
 * Log an invalid talk message.
 */
static int
bad_talk(talk_t *talk, msg_info_t *info, char *msg)
{
    zs_logi(70045, "%s node=m%d state=%s rank=%d",
             msg, info->nno, n_state(talk->state), talk->rank);
    return 0;
}


/*
 * Handle a sent notification and return true if we want it passed on to the
 * user.
 */
static int
info_sent(msg_info_t *info)
{
    node_t *node;

    if (info->mid == TALK_MID)
        return 0;
    node = get_node_nno(info->nno);
    if (!node)
        return 0;
    return (node->state == ACTIVE || node->state == ALMOST);
}


/*
 * Cause a node to join.
 */
static node_t *
node_join(int nno, int state, int rank)
{
    msg_node_t mnode;
    node_t *node = get_node_nno(nno);

    if (!msg_getnode(&mnode, nno))
        return NULL;

    if (!node)
        node = node_add();
    t_mapn(0, "node m%d joining state=%s rank=%d", nno, n_state(state), rank);

    node->port = mnode.port;
    node->nno  = nno;
    node->rank = rank;
    msg_setrank(node->nno, rank);
    set_state(node, state);
    return node;
}


/*
 * Drop a node from the node table.
 */
static void
node_drop(int nno)
{
    node_t *node = get_node_nno(nno);

    if (!node) {
        zs_logi(70046, "node m%d drop attempt, non-existent", nno);
        return;
    }

    t_mapn(0, "node m%d dropped state=%s rank=%d",
           nno, n_state(node->state), node->rank);
    node_del(node);
}


/*
 * Add a node.
 */
static node_t *
node_add(void)
{
    int i;
    int j;
    int n;

    for (i = 0; i < NodeN; i++)
        if (Nodes[i].state == UNUSED)
            return &Nodes[i];

    n = NodeN * 2;
    if (n < 4)
        n = 4;
    Nodes = m_realloc(Nodes, n * sizeof(*Nodes), "msg_map:N*node_t");
    memset(&Nodes[NodeN], 0, (n-NodeN) * sizeof(*Nodes));
    for (j = NodeN; j < n; j++)
        Nodes[j].state = UNUSED;
    NodeN = n;
    return &Nodes[i];
}


/*
 * Delete a node.
 */
static void
node_del(node_t *node)
{
    set_state(node, UNUSED);
    memset(node, 0, sizeof(*node));
    node->state = UNUSED;
}


/*
 * Get the node entry corresponding to a rank.
 */
static node_t *
get_node_rank(int rank)
{
    int n;
    node_t *node;

    for (node = Nodes, n = NodeN; n--; node++)
        if (node->state != UNUSED && rank == node->rank)
            return node;
    return NULL;
}


/*
 * Get the node entry referring to a nno.
 */
static node_t *
get_node_nno(int nno)
{
    int n;
    node_t *node;

    for (node = Nodes, n = NodeN; n--; node++)
        if (node->state != UNUSED && nno == node->nno)
            return node;
    return NULL;
}


/*
 * Get my node.
 */
static node_t *
mynode(void)
{
    node_t *node = get_node_nno(MyNno);

    if (!node)
        fatal("cannot find our node");
    return node;
}


/*
 * Send my current state to the other nodes.
 */
static void
talk_nodes(void)
{
    int n;
    node_t *node;

    for (n = NodeN, node = Nodes; n--; node++)
        if (node->state != UNUSED && node->nno != MyNno)
            send_talk(node);
}


/*
 * Send a talk message.
 */
static void
send_talk(node_t *node)
{
    talk_t *talk;
    node_t *me = mynode();
    msg_send_t *send = msg_salloc();

    t_mapn(0, "send_talk node=m%d state=%s rank=%d",
           node->nno, n_state(me->state), me->rank);

    talk = m_malloc(sizeof(*talk), "talk_t");
    talk->magic = htonl(TALK_MAGIC);
    talk->state = htons(me->state);
    talk->rank  = htons(me->rank);

    send->nno = node->nno;
    send->sid = TALK_MID;
    send->nsge = 1;
    send->sge[0].iov_base = talk;
    send->sge[0].iov_len = sizeof(*talk);
    send->data = talk;
    msg_send(send);
}


/*
 * See if an incoming message is a talk message and if so convert it and return
 * 1.
 */
static int
recv_talk(talk_t *talk, msg_info_t *info)
{
    talk_t *p = (talk_t *) info->data;

    if (info->type != MSG_ERECV)
        return 0;
    if (info->len != sizeof(talk_t))
        return 0;
    if (info->mid != TALK_MID)
        return 0;

    talk->magic = ntohl(p->magic);
    talk->state = ntohs(p->state);
    talk->rank  = ntohs(p->rank);

    if (talk->magic != TALK_MAGIC)
        return 0;

    t_mapn(0, "recv_talk node=m%d state=%s rank=%d",
           info->nno, n_state(talk->state), talk->rank);
    return 1;
}


/*
 * Change the state of a node.
 */
static void
set_state(node_t *node, int new)
{
    int old = node->state;

    node->state = new;
    msg_setstate(node->nno, new);

    if (old == NORANK || old == ALMOST || old == ACTIVE)
        NumTalked--;
    if (old == ALMOST || old == ACTIVE)
        NumRanked--;
    if (old == ACTIVE)
        NumActive--;

    if (new == NORANK || new == ALMOST || new == ACTIVE)
        NumTalked++;
    if (new == ALMOST || new == ACTIVE)
        NumRanked++;
    if (new == ACTIVE)
        NumActive++;

    t_mapn(0, "change state from %s to %s node=m%d rank=%d t=%d r=%d a=%d",
           n_state(old), n_state(new), node->nno, node->rank,
           NumTalked, NumRanked, NumActive);
    show_nodes();
}


/*
 * Push an entry onto the head of the info list.
 */
static void
info_push(msg_info_t *info)
{
    if (!InfoHead)
        InfoHead = InfoTail = info;
    else
        InfoTail = InfoTail->link = info;
}


/*
 * Get the next entry from the tail of the info list.
 */
static msg_info_t *
info_next(void)
{
    msg_info_t* info = InfoHead;

    if (info) {
        if (info == InfoTail)
            InfoTail = NULL;
        InfoHead = info->link;
    }
    return info;
}


/*
 * Show the node table.
 */
static void
show_nodes(void)
{
    int n;
    node_t *p;

    if (!t_on(MAPN))
        return;
    for (p = Nodes, n = NodeN; n--; p++) {
        if (p->state == UNUSED)
            continue;
        t_mapn(0, "rank %2d => nno=%d state=%s port=%d",
               p->rank, p->nno, n_state(p->state), p->port);
    }
}


/*
 * Return the name of a state.
 */
static char *
n_state(int state)
{
    if (state == UNUSED)
        return "UNUSED";
    else if (state == WAITID)
        return "WAITID";
    else if (state == NORANK)
        return "NORANK";
    else if (state == ALMOST)
        return "ALMOST";
    else if (state == ACTIVE)
        return "ACTIVE";
    else
        return "?";
}
