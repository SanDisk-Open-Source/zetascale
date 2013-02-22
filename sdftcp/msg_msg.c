/*
 * File: msg_msg.c
 * Author: Johann George, Norman Xu, Enson Zheng.
 * Copyright (c) 2008-2010, Schooner Information Technology, Inc.
 */
#include "gnusource.h"
#include <poll.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include "locks.h"
#include "stats.h"
#include "trace.h"
#include "msg_cat.h"
#include "msg_cip.h"
#include "msg_map.h"
#include "msg_msg.h"
#include "msg_hello.h"
#include "platform/stdio.h"


/*
 * Defaults.
 *  DEF_CLASS - Default class.
 *  DEF_IPING - Default ping interval in ns.
 *  DEF_UPORT - Default UDP port we listen on.
 */
#define DEF_CLASS   1
#define DEF_NCONN   1
#define DEF_IPING   (1*NANO)
#define DEF_UPORT   51350
#define DEF_NHOLD   8192


/*
 * Constants.
 *  NOSIZE      - Maximum size it takes to represent a node number.
 *  FTH_SSIZE   - Fth stack size.
 *  IPSIZE      - Maximum size it takes to represent an IP address as a string.
 *  LISTENQ     - Size of our listen queue.
 *  META_TAG    - Tag to identify a meta message.
 *  MYNNO       - Our node number.
 *  TALK_MID    - Message id that defines a talk message.
 *  TALK_MAGIC  - Magic number in a talk message.
 */
#define MYNNO      1
#define NOSIZE     24
#define IPSIZE     40
#define LISTENQ    10
#define META_TAG   0x8000
#define TALK_MID   18181818181818181818ULL
#define TALK_MAGIC 0xfdb97531
#define FTH_SSIZE  (128*1024)


/*
 * Constants we should get rid of.
 *  TIME_CONNECT - Time (us) we allow for a connect.
 *  TIME_HELLO   - Time (us) we wait for the exchange of hello messages.
 */
#define TIME_HELLO   1000000
#define TIME_CONNECT 1500000


/*
 * Message flags.
 */
#define MF_V1  0x0001                   /* Cause us to use version 1 */
#define MF_GDB 0x0002                   /* Help people using gdb */


/*
 * Meta messages.
 */
#define MM_ACKSEQ 1                     /* Acknowledge sequence number */
#define MM_RESEND 2                     /* Resend data */


/*
 * Functions for handling messaging flags.
 */
#define f_on(flag) (FlagBits&(MF_ ## flag))


/*
 * Type definitions.
 */
typedef struct sockaddr sa_t;
typedef struct ifreq ifreq_t;
typedef struct ifconf ifconf_t;
typedef struct pollfd pollfd_t;
typedef struct sockaddr_in sa_in_t;
typedef struct addrinfo addrinfo_t;
typedef unsigned int uint_t;
typedef void (*fdfunc_t)(void *p);


/*
 * Connection states.
 *  CS_AVAIL - Slot is available for use.
 *  CS_ERROR - An error has occurred.
 *  CS_READY - The connection is ready to be used.
 */
typedef enum {
    CS_AVAIL,
    CS_READY,
    CS_ERROR,
} conn_state_t;


/*
 * Node states.
 *
 *  NS_UNUSED - Unused
 *  NS_WAITID - Waiting for identification
 *  NS_NORANK - Waiting to be assigned a rank
 *  NS_ALMOST - Waiting for application
 *  NS_ACTIVE - Active
 */
typedef enum {
    NS_UNUSED,
    NS_WAITID,
    NS_NORANK,
    NS_ALMOST,
    NS_ACTIVE,
} node_state_t;


/*
 * Header encompassing versions 1 and 2.
 */
typedef struct {
    uint16_t  cver;                     /* Current version */
    uint16_t  sver;                     /* Supported version */
    msg_tag_t stag;                     /* Source tag */
    msg_tag_t dtag;                     /* Destination tag */
    msg_id_t  mid;                      /* Message id */
    int64_t   seqn;                     /* Sequence number */
    uint64_t  size;                     /* Size of payload */
    int64_t   aseqn;                    /* Last acknowledged sequence number */
} head_t;


/*
 * Message header version 1.
 */
typedef struct {
    uint16_t  cver;                     /* Current version */
    uint16_t  sver;                     /* Supported version */
    msg_tag_t stag;                     /* Source tag */
    msg_tag_t dtag;                     /* Destination tag */
    msg_id_t  mid;                      /* Message id */
    int64_t   seqn;                     /* Sequence number */
    uint64_t  size;                     /* Size of payload */
} head1_t;


/*
 * Message header version 2.
 */
typedef struct {
    uint16_t  cver;                     /* Current version */
    uint16_t  sver;                     /* Supported version */
    msg_tag_t stag;                     /* Source tag */
    msg_tag_t dtag;                     /* Destination tag */
    msg_id_t  mid;                      /* Message id */
    int64_t   seqn;                     /* Sequence number */
    uint64_t  size;                     /* Size of payload */
    int64_t   aseqn;                    /* Last acknowledged sequence number */
} head2_t;


/*
 * For communicating rank information to other nodes.
 */
typedef struct talk {
    uint32_t    magic;                  /* Magic number */
    uint16_t    state;                  /* State */
    int16_t     rank;                   /* Rank */
} talk_t;


/*
 * Meta data.
 */
typedef struct {
    uint32_t  type;                     /* Type of metadata */
    atom_t    seqn;                     /* Sequence number */
} meta_t;


/*
 * Connection information.  The first set of items below are generally
 * associated with the connection, the second set relates to the send side and
 * the third set relates to the receive side.
 *
 * Locking is controlled by trlock, tslock and fdlock.  Should a worker want to
 * poll on fd, they must acquire fdlock for reading.  If the poll indicates
 * that the descriptor is available, they must acquire trlock in order to read
 * or twlock in order to write while holding fdlock for reading.  If an error
 * occurs, the error flag is set.
 *
 *  next   - Pointer to next entry.
 *  path   - Associated path.
 *  state  - Current connection state.
 *  sfd    - Socket file descriptor.
 *
 *  shead  - Send queue head.
 *  sqlock - Send queue lock.  Used when modifying shead, stail or anything
 *           they contain.
 *  ssize  - The current size of the send queue.
 *  stail  - Send queue tail.
 *  wolock - Lock for sending over TCP.
 
 *  data   - Buffer for incoming data.
 *  dlen   - Data length already read.
 *  headr  - The incoming header is read here.
 *  headw  - We construct the outgoing header and save it here.
 *  hlen   - Header length already read.
 *  rolock - Lock for receiving over TCP.
 */
typedef struct conn {
    struct conn  *next;
    struct path  *path;
    int           sfd;
    conn_state_t  state;
    wlock_t      *sqlock;
    wlock_t      *rolock;
    wlock_t      *wolock;
    rwlock_t     *fdlock;
    msg_send_t   *shead;
    msg_send_t   *stail;
    int           ssize;
    int           hlen;
    int           dlen;
    char         *data;
    head_t        headr;
    head_t        headw;
} conn_t;


/*
 * Path information.
 *  iface - The name of the interface.
 *  init  - We need to initiate the connection.
 *  lip   - Local IP.
 *  ltime - Last time we heard a ping.
 *  mconn - Maximum number of connections our side allows.
 *  nconn - Number of agreed upon connections.
 *  next  - Next path in list.
 *  node  - Associated node.
 *  prior - Priority of path.
 *  rip   - Remote IP.
 */
typedef struct path {
    struct path *next;
    struct node *node;
    char        *iface;
    cip_t        lip;
    cip_t        rip;
    int          init;
    int          prior;
    int          nconn;
    int          mconn;
    ntime_t      ltime;
} path_t;


/*
 * Node information
 *
 *  aconn           - Number of active connections.
 *  conn            - List of connections.
 *  conn_next       - The next connection we should use as we attempt to round
 *                    robin connections.
 *  hlen            - Length of header that is used.
 *  iface           - List of interfaces.
 *  live            - Node is live.
 *  lock_recv       - Lock for updating rinfo_* and rseqn_cont.
 *  lock_send       - Lock for accessing send_head and send_tail.
 *  lock_sent       - Lock for accessing sent_head and sent_tail.
 *  name            - The node name.
 *  nipp            - A descriptive name including the name, IP and port.
 *  nno             - Node number.  Used to reference this node.
 *  port            - TCP port that node listens on.
 *  rank            - The rank of this node.  A number that uniquely identifies
 *                    this node among all other nodes.
 *  rank_state      - The state that this node is in regarding acquiring a
 *                    rank.
 *  rinfo_head      - Along with rinfo_tail, a linked list of received info
 *                    structures that need to be posted that do not qualify for
 *                    the rinfo_hold area.  Updated under lock_recv.
 *  rinfo_hold      - Received info structures waiting to be posted whose
 *                    sequence numbers are within the rinfo_size sequence
 *                    numbers starting at rseqn_cont+2.  Updated under
 *                    lock_recv.
 *  rinfo_soff      - Sequence number corresponding to rinfo_hold[0];
 *  rinfo_size      - Number of info structures the hold area can hold.
 *  rinfo_tail      - See rinfo_head.  Updated under lock_recv.
 *  rseqn_ackd      - Last sequence number we acknowledged receiving to the
 *                    other side.
 *  rseqn_ackd_last - Last sequence number we acknowledged to the other side
 *                    the last time we checked.  This is used to determine if
 *                    we need to send a special acknowledge packet as there is
 *                    not enough traffic for the automatic acknowledge to take
 *                    place.
 *  rseqn_cont      - Last contiguous sequence number we received from the
 *                    other side.  This is updated under lock_recv.
 *  rseqn_cont_last - Last contiguous sequence number we received from the
 *                    other side when we last checked.  See rseqn_ackd_last.
 *  rseqn_high      - Highest sequence number we received from the other side.
 *                    Updated under lock_recv.
 *  send_head       - Along with send_tail, the list of messages that we have
 *                    not yet sent likely because we are waiting for a
 *                    connection.
 *  send_tail       - See send_head.
 *  sent_head       - Along with sent_tail, the list of messages that have been
 *                    sent that we are awaiting to have acknowledged.
 *  sent_tail       - See sent_head.
 *  sseqn_ackd      - Last acknowledged sent sequence number.  Atomically
 *                    updated.
 *  sseqn_ackd_last - Last acknowledged sent sequence number when we last
 *                    checked.
 *  sseqn_used      - Sequence number we last used to send a message.
 *  to_die          - Node has been sentenced to die.
 *  uip             - The unique IP of this node.  Each node will choose one of
 *                    its IPs to be its unique IP.
 *  wake_time       - The time the node wakes up and starts accepting
 *                    connections.
 *  ver             - The highest common version between us and the node we are
 *                    connected to.
 */
typedef struct node {
    struct node *next;
    path_t      *path;
    conn_t      *conn;
    conn_t      *conn_next;
    msg_send_t  *send_head;
    msg_send_t  *send_tail;
    msg_send_t  *sent_head;
    msg_send_t  *sent_tail;
    msg_info_t  *rinfo_head;
    msg_info_t  *rinfo_tail;
    msg_info_t **rinfo_hold;
    wlock_t     *lock_recv;
    wlock_t     *lock_send;
    wlock_t     *lock_sent;
    char        *nipp;
    atom_t       rinfo_soff;
    atom_t       rseqn_ackd;
    atom_t       rseqn_cont;
    atom_t       rseqn_high;
    atom_t       sseqn_ackd;
    atom_t       sseqn_used;
    atom_t       rseqn_cont_last;
    atom_t       rseqn_ackd_last;
    atom_t       sseqn_ackd_last;
    char         name[NNSIZE];
    ntime_t      wake_time;
    cip_t        uip;
    int          nno;
    int          ver;
    int          hlen;
    int          live;
    int          rank;
    int          aconn;
    int          to_die;
    int          rinfo_size;
    node_state_t rank_state;
    msg_port_t   port;
} node_t;


/*
 * Local interface.
 */
typedef struct {
    char    name[IFNAMSIZ+1];           /* Name */
    uint8_t haddr[IFHWADDRLEN];         /* Hardware address */
    cip_t   local;                      /* Local IP */
    cip_t   nmask;                      /* Netmask */
    cip_t   bcast;                      /* Broadcast IP */
    int     prior;                      /* Priority */
    int     nconn;                      /* Number of connections */
} lface_t;


/*
 * Functions associated with the file descriptors in the parallel array
 * pollfd_t.
 */
typedef struct {
    fdfunc_t  rfunc;                    /* Function called for reading */
    fdfunc_t  wfunc;                    /* Function called for writing */
    void     *rarg;                     /* Argument for read function */
    void     *warg;                     /* Argument for write function */
    conn_t   *conn;                     /* Associated connection */
} callfd_t;


/*
 * One of these are kept for each pthread.  polls and funcs are parallel
 * arrays.
 */
typedef struct {
    int       run;                      /* Task should run */
    pthread_t pthread;                  /* Pthread */
    xstr_t    pollfds;                  /* File descriptor list for poll */
    xstr_t    callfds;                  /* Callback functions */
} task_t;


/*
 * This holds a mapping between file descriptors and functions to be called
 * when they become available.
 */
typedef struct {
    int       fd;                       /* File descriptor */
    void     *arg[2];                   /* Arguments */
    fdfunc_t  func[2];                  /* Functions */
} fdcall_t;


/*
 * List of remote IPs.
 */
typedef struct rips {
    struct rips *next;                  /* Next in sequence */
    cip_t        lip;                   /* Local IP; not needed to keep */
    cip_t        rip;                   /* Remote IP */
    int          fd;                    /* UDP file descriptor */
    int          fail;                  /* Send operation last failed */
} rips_t;


/*
 * Liveness callbacks.
 */
typedef struct lcall {
    struct lcall   *next;               /* Next entry */
    int             fth;                /* Call in a fthread context */
    msg_livefunc_t *func;               /* Function to call */
    void           *arg;                /* Argument */
    fthMbox_t       mbox;               /* Fth mailbox */
} lcall_t;


/*
 * Variables.
 *
 *  cast_rips      - List of remote IP addresses we send unicasts and
 *                   broadcasts.
 *  cast_lock      - To lock additions to cast_rips.
 *  info_free      - A freelist of info_t structures.
 *  info_head      - info_head and info_tail make up a FIFO containing info_t's
 *                   that will be returned by msg_poll.  msg_poll retrieves
 *                   these from the FIFO and msg_post posts them onto the FIFO.
 *  info_lock_free - Used to lock the info free list.
 *  info_lock_post - Used to lock the info post list.
 *  info_tail      - See info_head.
 *  info_used      - The number of info_t structures in use at this time.
 *  live_list      - The liveness callback list.
 *  live_lock      - The lock for the liveness callback list.
 *  lface_i        - Set once local interfaces have been obtained.
 *  lface_n        - The number of local interfaces that we have.
 *  lface_p        - Local interface table.
 *                   time, we assume it is dead.
 *  node_attn      - One of the nodes has its error flag set.
 *  nodes          - All nodes including us who is the first node.
 *  poll_fds       - Poll file descriptors.
 *  poll_show0     - Used to eliminate showing repeated calls to msg_poll(0)
 *                   when printing debugging output.
 *  post_call      - A list of callbacks when we post a message.
 *  send_free      - A freelist of msg_send_t structures.
 *  send_lock      - Used to lock the send free list.
 *  send_used      - The number of msg_send_t structures in use at this time.
 *  tasks          - One of these are kept for each pthread.
 *  time_last      - Last time that was returned for liveness.
 *  time_ping      - Last time we sent a broadcasts.
 *  udp_fd         - The UDP file descriptor.
 *  uip            - The unique IP for this node.
 *  version        - Hello version we are running at.
 *  wake_main      - Used to wake the main thread out of select.
 *  wake_work      - Used to wake the worker threads out of select.
 *  xnodes         - The list of nodes we are connected to.
 */
typedef struct {
    int         udp_fd;
    int         lface_i;
    int         tcp_lfd;
    int         version;
    int         node_attn;
    int         poll_show0;
    int         wake_main[2];
    int         wake_work[2];
    uint16_t    lface_n;
    uint64_t    info_used;
    uint64_t    send_used;
    cip_t       uip;
    xstr_t      fdcall;
    xstr_t      pollfds;
    ntime_t     time_last;
    ntime_t     time_ping;
    cb_list_t   post_call;
    msg_init_t  init;
    task_t     *tasks;
    node_t     *nodes;
    lface_t    *lface_p;
    rips_t     *cast_rips;
    lcall_t    *live_list;
    wlock_t    *live_lock;
    wlock_t    *cast_lock;
    wlock_t    *send_lock;
    wlock_t    *info_lock_free;
    wlock_t    *info_lock_post;
    msg_info_t *info_head;
    msg_info_t *info_tail;
    msg_info_t *info_free;
    msg_send_t *send_free;
} msg_var_t;


/*
 * Function prototypes to be replaced.
 */
static int  ready_nb(int fd, int events, int usecs);
static int  connect_nb(int, const sa_in_t *, socklen_t, int);
static int  read_nb(int fd, void *buf, size_t count, int usecs);


/*
 * Function prototypes.
 */
static int   head_size(int ver);
static int   ismyip(cip_t *cip);
static int   cip_2v4a(cip_t *cip);
static int   cip_isnull(cip_t *cip);
static int   cip_isbcast(cip_t *cip);
static int   poll_time(ntime_t etime);
static int   tcp_connect(path_t *path);
static int   hello_size(hellobuf_t *hbuf);
static int   errz_close(int fd, char *msg);
static int   udp_open(cip_t *ip, int port);
static int   cip_str2(cip_t *cip, char *str);
static int   cip_eq(cip_t *cip1, cip_t *cip2);
static int   cip_cmp(cip_t *cip1, cip_t *cip2);
static int   path_no(node_t *node, path_t *path);
static int   atomic_max(atom_t *atom, atom_t new);
static int   hwaddr_isnull(uint8_t h[IFHWADDRLEN]);
static int   poll_execute(task_t *task, int timeout);
static int   hello_tcp_read(int fd, hellostd_t *hstd);
static int   hello_udp_read(int fd, hellostd_t *hstd);
static int   poll_ret(int ret, callfd_t *callp, int n);
static int   conv1_name(hellostd_t *hstd, hello1_t *h1);
static int   hello_conv1(hellostd_t *hstd, hello1_t *h1);
static int   hello_conv2(hellostd_t *hstd, hello2_t *h2);
static int   conn_sqput(conn_t *conn, msg_send_t *send, int fast);
static int   hello_fill(hellobuf_t *hbuf, int type, path_t *path);
static int   hello_conv(hellostd_t *hstd, hellobuf_t *hbuf, int len);
static int   iface_prior(char *name, int *valp, int *pr1p, int *pr2p);
static void  nothing(void);
static void  swallow(void);
static void  exit_end(void);
static void  exit_tcp(void);
static void  exit_udp(void);
static void  init_end(void);
static void  init_tcp(void);
static void  init_udp(void);
static void  ping_run(void);
static void  exit_call(void);
static void  exit_cast(void);
static void  exit_face(void);
static void  exit_live(void);
static void  exit_mpar(void);
static void  exit_node(void);
static void  exit_poll(void);
static void  exit_task(void);
static void  exit_vars(void);
static void  exit_wake(void);
static void  init_call(void);
static void  init_cast(void);
static void  init_face(void);
static void  init_live(void);
static void  init_node(void);
static void  init_poll(void);
static void  init_task(void);
static void  init_vars(void);
static void  init_wake(void);
static void  node_show(void);
static void  node_talk(void);
static void  lface_init(void);
static void  node_syncseq(void);
static void  node_timeout(void);
static void  node_maintain(void);
static void  udp_recv(void *arg);
static void  hello_send(int type);
static void  tcp_accept(void *arg);
static void  cip_seth2n(cip_t *cip);
static void  cip_setn2h(cip_t *cip);
static void  poll_clr(task_t *task);
static void  setnonblocking(int fd);
static void  tcp_recv(conn_t *conn);
static void  tcp_send(conn_t *conn);
static void  node_dead(node_t *node);
static void  node_live(node_t *node);
static void  our_stats(stat_t *stat);
static void  send_talk(node_t *node);
static void  conn_close(conn_t *conn);
static void  conn_move(conn_t *oconn);
static void  send_resend(node_t *node);
static void  fdcall_clr(int fd, int rw);
static void  fth_liveness(uint64_t arg);
static void  msg_post(msg_info_t *info);
static void  path_connect(path_t *path);
static void  poll_prepare(task_t *task);
static void  init_mpar(msg_init_t *init);
static void  node_redo_time(ntime_t now);
static void  post_done(msg_info_t *info);
static void  post_find(msg_info_t *info);
static void  post_tail(msg_info_t *info);
static void  send_tome(msg_send_t *send);
static void  node_sent_fail(node_t *node);
static void  node_sent_free(node_t *node);
static void  post_seqn_fill(node_t *node);
static void  errv_close(int fd, char *msg);
static void  live_call_one(lcall_t *lcall);
static void  node_rinfo_fail(node_t *node);
static void  poll_debug_call(ntime_t etime);
static void  add_cast(cip_t *lip, cip_t *rip);
static void  send_list_fail(msg_send_t *send);
static void  show_node(xstr_t *xstr, int nno);
static void  conn_msg(conn_t *conn, char *msg);
static void  live_call_all(int live, int rank);
static void  path_msg(path_t *path, char *msg);
static void  setsockoptone(int fd, int option);
static void  head_setn2h(head_t *head, int ver);
static void  live_post(node_t *node, int event);
static void  node_drop(node_t *node, char *msg);
static void  siocgifconf(int fd, ifconf_t *ifc);
static void  path_alive(path_t *path, ntime_t new);
static void  node_resend(node_t *node, atom_t lseqn);
static void  cip_sa2(cip_t *cip, struct sockaddr *sa);
static void  post_sent(msg_send_t *send, char *error);
static void  node_no(node_t *node, char *buf, int len);
static void  post_seqn(node_t *node, msg_info_t *info);
static void  show_info(xstr_t *xstr, msg_info_t *info);
static void  show_send(xstr_t *xstr, msg_send_t *send);
static void  hello_setdip(hellobuf_t *hbuf, cip_t *dip);
static void  node_set_aseqn(node_t *node, atom_t aseqn);
static void  path_update(path_t *path, hellostd_t *hstd);
static void  conn_error(conn_t *conn, int sys, char *msg);
static void  live_call(lcall_t *lcall, int live, int rank);
static void  node_send_put(node_t *node, msg_send_t *send);
static void  conn_sqputlist(conn_t *conn, msg_send_t *send);
static void  meta_send(node_t *node, int type, atom_t seqn);
static void  tcp_recv_err(conn_t *conn, int real, char *msg);
static void  node_send_putlist(node_t *node, msg_send_t *send);
static void  post_recv(node_t *node, head_t *head, char *data);
static void  send_out(node_t *node, msg_send_t *send, int fast);
static void  fdcall_set(int fd, int rw, fdfunc_t func, void *arg);
static void  meta_recv(node_t *node, msg_info_t *info, atom_t aseqn);
static void  head_prep(head_t *head, msg_send_t *send, node_t *node, int ver);
static void  pick_best(conn_t **bestp, int *state, conn_t *conn, conn_t *next);
static void  poll_add(task_t *task, conn_t *conn, int fd,
                      fdfunc_t rfunc, void *rarg, fdfunc_t wfunc, void *warg);
static void *worker(void *arg);
static void *errp_close(int fd, char *msg);
static char *type_msg(int type);
static char *node_state_name(node_state_t state);
static char *conn_state_name(conn_t *conn);
static char *strechr(char *str, int del, char *end);
static char *cip_2str(cip_t *cip, char *str, int len);

static msg_seqn_t node_sent_seqn(node_t *node);

static conn_t     *conn_pick(node_t *node);
static conn_t     *conn_new(path_t *path, int fd);
static node_t     *node_getbynno(int nno);
static node_t     *node_getbyrank(int rank);
static node_t     *node_getbyip(cip_t *uip, msg_port_t port);
static node_t     *node_new(int ver,
                            cip_t *uip, msg_port_t port, char name[NNSIZE]);
static path_t     *node_hello(hellostd_t *hstd);
static path_t     *path_getbyip(node_t *node, cip_t *lip);
static path_t     *path_new(node_t *node, cip_t *lip, cip_t *rip);
static lface_t    *lface_getbylbip(cip_t *lip);
static lface_t    *lface_getbyname(char *name);
static msg_send_t *conn_sqget(conn_t *conn);
static msg_send_t *conn_sqret(conn_t *conn);
static msg_send_t *conn_sqgetlist(conn_t *conn);
static msg_send_t *node_send_getlist(node_t *node);
static msg_info_t *info_shift(void);
static msg_info_t *poll_debug_retn(msg_info_t *info, ntime_t etime, int wake);


/*
 * Static variables.
 */
static uint64_t  FlagBits;
static msg_var_t V;


/*
 * Messaging flag options.
 */
static opts_t FlagOpts[] ={
    { "v1",  MF_V1  },
    { "gdb", MF_GDB },
    {               }
};


/*
 * Messaging statistics options.
 */
static opts_t StatOpts[] ={
    { "live", MS_LIVE           },
    { "seqn", MS_SEQN           },
    { "rate", MS_RATE           },
    { "full", MS_FULL           },
    { "all",  MS_ALL & ~MS_FULL },
    {                           }
};


/*
 * Initialize the messaging system.
 */
void
msg_init(msg_init_t *init)
{
    init_mpar(init);
    init_vars();
    init_node();
    init_face();
    init_cast();
    init_call();
    init_live();
    init_poll();
    init_wake();
    init_tcp();
    init_udp();
    init_end();
    init_task();
}


/*
 * Exit the messaging system.
 */
void
msg_exit()
{
    msg_drain();
    exit_task();
    exit_end();
    exit_udp();
    exit_tcp();
    exit_wake();
    exit_poll();
    exit_live();
    exit_call();
    exit_cast();
    exit_face();
    exit_node();
    exit_vars();
    exit_mpar();
}


/*
 * Initialize parameters.
 */
static void
init_mpar(msg_init_t *init)
{
    if (init)
        V.init = *init;
    if (!V.init.ping)
        V.init.ping = DEF_IPING;
    if (!V.init.class)
        V.init.class = DEF_CLASS;
    if (!V.init.nconn)
        V.init.nconn = DEF_NCONN;
    if (!V.init.nhold)
        V.init.nhold = DEF_NHOLD;
    if (!V.init.udpport)
        V.init.udpport = DEF_UPORT;

    if (V.init.live) {
        if (!V.init.linklive)
            V.init.linklive = V.init.live;
        else if (V.init.linklive > V.init.live)
            fatal("msg_linklive cannot be greater than msg_live");
    }

    if (V.init.nthreads < 1)
        V.init.nthreads = 1;

    if (V.init.mustlive)
        if (V.init.nthreads < 2)
            fatal("msg_nthreads must be at least 2 if msg_mustlive is set");

    if (V.init.iface)
        V.init.iface = m_strdup(V.init.iface, "msg_msg:V.init.iface");

    if (V.init.alias)
        V.init.alias = m_strdup(V.init.alias, "msg_msg:V.init.alias");
    else {
        char host[HOST_NAME_MAX+1];
        if (gethostname(host, sizeof(host)) < 0)
            fatal("gethostname failed");
        V.init.alias = m_strdup(host, "msg_msg:V.init.alias");
    }
}


/*
 * Clean up parameters.
 */
static void
exit_mpar(void)
{
    if (V.init.alias)
        m_free(V.init.alias);
    if (V.init.iface)
        m_free(V.init.iface);
}


/*
 * Initialize variables.
 */
static void
init_vars(void)
{
    cb_init(&V.post_call, 0);

    V.send_lock      = wl_init();
    V.info_lock_free = wl_init();
    V.info_lock_post = wl_init();
}


/*
 * Clean up variables.
 */
static void
exit_vars(void)
{
    wl_free(V.send_lock);
    wl_free(V.info_lock_free);
    wl_free(V.info_lock_post);

    cb_free(&V.post_call);
}


/*
 * Initialize variables relating to nodes.
 */
static void
init_node(void)
{
    V.version = f_on(V1) ? V1_HELLO : VS_HELLO;
}


/*
 * Clean up nodes.
 */
static void
exit_node(void)
{
    node_t *node;
    node_t *noden;
    path_t *path;
    path_t *pathn;

    for (node = V.nodes; node; node = noden) {
        for (path = node->path; path; path = pathn) {
            pathn = path->next;
            m_free(path);
        }
        m_free(node->nipp);
        noden = node->next;
        m_free(node);
    }
}


/*
 * Get our local interfaces.
 */
static void
init_face(void)
{
    int n;
    lface_t *p;

    lface_init();
    p = V.lface_p;
    n = V.lface_n;
    for (; n--; p++) {
        iface_prior(p->name, &p->nconn, NULL, &p->prior);

        /* Information */
        {
            char lip[IPSIZE];
            char bip[IPSIZE];

            cip_2str(&p->local, lip, sizeof(lip));
            cip_2str(&p->bcast, bip, sizeof(bip));
            sdf_logi(70064, "if=%s con=%d pri=%d lip=%s bip=%s",
                     p->name, p->nconn, p->prior, lip, bip);
        }
    }
}


/*
 * Clean up interfaces.
 */
static void
exit_face(void)
{
    if (V.lface_p)
        m_free(V.lface_p);
}


/*
 * Get the list of nodes we need to broadcast to.  Also, choose our unique IP.
 */
static void
init_cast(void)
{
    lface_t *p = V.lface_p;
    int      n = V.lface_n;
    int  found = 0;

    V.cast_lock = wl_init();
    for (; n--; p++) {
        if (!p->prior)
            continue;

        if (!found) {
            found = 1;
            V.uip = p->local;
        }
        if (V.init.nobcast)
            continue;
        if (cip_isnull(&p->bcast))
            continue;
        add_cast(NULL, &p->bcast);
    }
    if (!found)
        fatal("cannot determine local IP for given interfaces");
}


/*
 * Clean up the list of nodes we unicast or broadcast to.
 */
static void
exit_cast(void)
{
    rips_t *cast;
    rips_t *next;

    for (cast = V.cast_rips; cast; cast = next) {
        next = cast->next;
        close(cast->fd);
        m_free(cast);
    }
    wl_free(V.cast_lock);
}


/*
 * Initialize calling structure.
 */
static void
init_call(void)
{
    xainit(&V.fdcall, sizeof(fdcall_t), 16, 1);
}


/*
 * Clean up calling structure.
 */
static void
exit_call(void)
{
    xafree(&V.fdcall);
}


/*
 * Initialize liveness.
 */
static void
init_live(void)
{
    V.live_lock = wl_init();
}


/*
 * Clean up liveness.
 */
static void
exit_live(void)
{
    wl_free(V.live_lock);
}


/*
 * Initialize polling structure.
 */
static void
init_poll(void)
{
    xainit(&V.pollfds, sizeof(pollfd_t), 16, 1);
}


/*
 * Clean up polling structure.
 */
static void
exit_poll(void)
{
    xafree(&V.pollfds);
}


/*
 * Initialize the wakeup mechanism we use for the poll loop.
 */
static void
init_wake(void)
{
    signal(SIGPIPE, SIG_IGN);
    if (pipe(V.wake_main) < 0)
        fatal("pipe failed");
    setnonblocking(V.wake_main[0]);
}


/*
 * Clean up the wakeup mechanism.
 */
static void
exit_wake(void)
{
    close(V.wake_main[0]);
    close(V.wake_main[1]);
}


/*
 * Initialize TCP.
 */
static void
init_tcp(void)
{
    int fd;
    int len;
    sa_in_t saddr ={
        .sin_port   = htons(V.init.tcpport),
        .sin_family = AF_INET,
        .sin_addr   = {.s_addr = htonl(INADDR_ANY)},
    };

    sdf_logi(70047, "msgtcp version %d", V.version);

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        fatal_sys("socket failed");
    setsockoptone(fd, SO_REUSEADDR);
    setnonblocking(fd);

    if (bind(fd, (sa_t *)&saddr, sizeof(saddr)) != 0)
        fatal_sys("bind failed");

    len = sizeof(saddr);
    if (getsockname (fd, (sa_t *)&saddr, (socklen_t *) &len) != 0)
        fatal_sys("getsockname failed");
    if (len != sizeof(saddr))
        fatal("getsockname failed");

    V.tcp_lfd = fd;
    V.init.tcpport = ntohs(saddr.sin_port);

    if (listen(fd, LISTENQ) < 0)
        fatal_sys("listen failed");
}


/*
 * Clean up TCP.
 */
static void
exit_tcp(void)
{
}


/*
 * Initialise the UDP socket.
 */
static void
init_udp(void)
{
    int fd = udp_open(NULL, V.init.udpport);

    if (fd < 0)
        fatal("could not initialize UDP socket");
    fdcall_set(fd, 0, &udp_recv, NULL);
    V.udp_fd = fd;
}


/*
 * Open a UDP socket and bind it to the given IP and port.
 */
static int
udp_open(cip_t *ip, int port)
{
    int s;
    int fd;
    sa_in_t saddr;

    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        sdf_loge(70073, "UDP socket failed");
        return -1;
    }

    setsockoptone(fd, SO_REUSEADDR);
    setsockoptone(fd, SO_BROADCAST);

    clear(saddr);
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(port);
    saddr.sin_addr.s_addr = ip ? cip_2v4a(ip) : htonl(INADDR_ANY);

    s = bind(fd, (sa_t *)&saddr, sizeof(sa_t));
    if (s < 0) {
        sdf_loge(70074, "UDP bind failed");
        close(fd);
        return -1;
    }
    return fd;
}


/* 
 * Clean up the UDP socket.
 */
static void
exit_udp(void)
{
    hello_send(MSG_EDROP);
    close(V.udp_fd);
}


/*
 * All initialization that needs to be done at the end.
 */
static void
init_end(void)
{
    node_t *node;
    char name[NNSIZE];

    strncopyfill(name, NNSIZE, V.init.alias, 0);
    node = node_new(V.version, &V.uip, V.init.tcpport, name);
    if (node->nno != MYNNO)
        fatal("my node number is %d; should be %d", node->nno, MYNNO);
    sdf_msg_call_stat(our_stats);
}


/*
 * Clean up any variables.
 */
static void
exit_end()
{
}


/*
 * Initialize tasks.
 */
static void
init_task(void)
{
    task_t *p;
    int n = V.init.nthreads;
    int s = n * sizeof(*p);

    V.tasks = m_malloc(s, "N*task_t");
    memset(V.tasks, 0, s);

    if (pipe(V.wake_work) < 0)
        fatal("pipe failed");

    for (p = V.tasks; n--; p++) {
        xainit(&p->pollfds, sizeof(pollfd_t), 16, 0);
        xainit(&p->callfds, sizeof(callfd_t), 16, 0);
        if (p != V.tasks) {
            p->run = 1;
            if (pthread_create(&p->pthread, NULL, worker, p) < 0)
                fatal("pthread_create failed");
        }
    }
}


/*
 * Clean up tasks.
 */
static void
exit_task(void)
{
    int n;
    task_t *p;

    for (p = V.tasks, n = V.init.nthreads; n--; p++)
        p->run = 0;
    barrier();
    if (write(V.wake_work[1], "", 1)) {}

    for (p = V.tasks, n = V.init.nthreads; n--; p++) {
        if (p != V.tasks)
            pthread_join(p->pthread, NULL);
        xafree(&p->pollfds);
        xafree(&p->callfds);
    }

    close(V.wake_work[0]);
    close(V.wake_work[1]);
}


/*
 * Report our version number.
 */
int
tcp_report_version(char **buf, int *len)
{
    return plat_snprintfcat(buf, len, "msgtcp %d.0.0\r\n", V.version);
}


/*
 * Set messaging flags.  str is the string that might be passed from the
 * command line.  Note that these may be set before messaging is initialized.
 */
void
msg_setflags(char *str)
{
    opts_set(&FlagBits, FlagOpts, "msg_flags", str);
}


/*
 * Set statistics flags.  str is the string that might be passed from the
 * command line.  Note that these may be set before messaging is initialized.
 */
void
msg_setstats(char *str)
{
    opts_set(&StatBits, StatOpts, "msg_stats", str);
}


/*
 * Set a messaging parameter after we have started.
 */
int
msg_setlive(char *name, msg_init_t *init)
{
    ntime_t t;

    if (streq(name, "msg_live")) {
        t = init->live;
        if (V.init.linklive > t)
            V.init.linklive = t;
        V.init.live = t;
        return 1;
    } else if (streq(name, "msg_linklive")) {
        t = init->linklive;
        if (V.init.live < t)
            V.init.live = t;
        V.init.linklive = t;
        return 1;
    } else
        return 0;
}


/*
 * Show statistics.
 */
static void
our_stats(stat_t *stat)
{
    int rank;
    node_t *node;

    for (node = V.nodes->next; node; node = node->next) {
        if (!node->live)
            continue;
        rank = node->rank;

        if (s_on(SEQN)) {
            atom_t rackd = node->rseqn_ackd;
            atom_t rcont = node->rseqn_cont;
            atom_t rhigh = node->rseqn_high;
            atom_t sackd = node->sseqn_ackd;
            atom_t sused = node->sseqn_used;

            stat_labn(stat, "rseqn high", rank);
            stat_full(stat, "gap",  rhigh - rcont);
            stat_full(stat, "high", rhigh);
            stat_full(stat, "cont", rcont);
            stat_endl(stat);

            stat_labn(stat, "rseqn ackd", rank);
            stat_full(stat, "gap",  rcont - rackd);
            stat_full(stat, "cont", rcont);
            stat_full(stat, "ackd", rackd);
            stat_endl(stat);

            stat_labn(stat, "sseqn ackd", rank);
            stat_full(stat, "gap",  sused - sackd);
            stat_full(stat, "used", sused);
            stat_full(stat, "ackd", sackd);
            stat_endl(stat);
        }
    }
}


/*
 * Return the header size that is appended to the message.
 */
int
msg_hlen(int rank)
{
    node_t *node;

    node = node_getbyrank(rank);
    if (!node)
        return -1;
    return node->hlen;
}


/*
 * Set a callback to happen on a post.
 */
void
msg_call_post(msg_post_find_t *func)
{
    cb_add(&V.post_call, (cb_func_t *)func, NULL);
}


/*
 * Return the next message that is waiting for us.  etime is the time at which
 * we are willing to wait for a message.  If a time is given (etime > 0) and
 * the time has expired, NULL is returned even if there is a ready message.
 * This indicates to those programs that the time has expired.  If etime is -1,
 * we will not return until a message is available.  If etime is 0, we will
 * make some minimal effort to get a message.
 */
msg_info_t *
msg_poll(ntime_t etime)
{
    msg_info_t *info;

    ping_run();
    poll_debug_call(etime);
    if (etime > 0 && msg_ntime() >= etime)
        return poll_debug_retn(NULL, etime, 0);

    info = info_shift();
    while (!info) {
        if (!msg_work(etime))
            return poll_debug_retn(NULL, etime, 1);
        info = info_shift();
        if (info)
            break;
        if (etime >= 0 && msg_ntime() >= etime)
            return poll_debug_retn(NULL, etime, 0);
    }
    return poll_debug_retn(info, etime, 0);
}


/*
 * When msg_poll or msg_want are called, this is called to print out debugging
 * information.
 */
static void
poll_debug_call(ntime_t etime)
{
    char *name = "msg_poll";

    if (t_on(POLL)) {
        if (etime < 0) {
            t_poll(0, "=> %s(%ld)", name, etime);
        } else if (etime == 0) {
            if (V.poll_show0)
                t_poll(0, "=> %s(0)", name);
        } else {
            ntime_t secs = (etime - msg_ntime()) / NANO;
            if (secs >= 0)
                t_poll(0, "=> %s(+%lds)", name, secs);
            else
                t_poll(0, "=> %s(%lds)", name, secs);
        }
        V.poll_show0 = etime;
    }
}


/*
 * When msg_poll or msg_want are about to return a value, this is called to
 * print out any debugging information.
 */
static msg_info_t *
poll_debug_retn(msg_info_t *info, ntime_t etime, int wake)
{
    if (t_on(POLL)) {
        if (!info) {
            if (etime)
                t_poll(0, "msg_poll() => NULL%s", wake ? " (woken)" : "");
        } else {
            xstr_t xstr;

            V.poll_show0 = 1;
            xsinit(&xstr);
            xsprint(&xstr, "msg_poll() =>");
            show_info(&xstr, info);
            t_poll(0, "%s", (char *)xstr.p);
            xsfree(&xstr);
        }
    }
    return info;
}


/*
 * Actively drop a node.
 */
void
msg_nodedrop(int nno)
{
    node_t *node;

    node = node_getbynno(nno);
    if (!node)
        return;
    node_drop(node, "user requested");
}


/*
 * The worker pthreads.
 */
static void *
worker(void *arg)
{
    task_t *task = arg;
    int        n = task - V.tasks;

    msg_affinity(n);
#ifndef SDFAPI
    while (task->run) {
        poll_prepare(task);
        poll_execute(task, -1);
    }
#endif /* SDFAPI */
    return NULL;
}


/*
 * See if anything needs to be serviced and if so, take care of it.  If nothing
 * needs to be serviced, we are willing to wait until etime at which point we
 * simply return.
 */
int
msg_work(ntime_t etime)
{
    int timeout;
    task_t *task = V.tasks;

    ping_run();
    timeout = poll_time(etime);
    poll_prepare(task);
    return poll_execute(task, timeout);
}


/*
 * Ping the other nodes if it is time, check for clock skew and perform any
 * maintenance that might be needed.
 */
static void
ping_run(void)
{
    ntime_t old = V.time_last;
    ntime_t now = msg_ntime();

    if (old && (now < old || (V.init.ping && now-old > 2*V.init.ping))) {
        if (!f_on(GDB))
            sdf_logi(70048, "possible clock skew %.1fs",
                     (double)(now-old)/NANO);
        V.time_ping = now;
        node_redo_time(now);
    }
    V.time_last = now;

    if (now >= V.time_ping) {
        hello_send(MSG_EJOIN);
        V.time_ping = now + V.init.ping;
        node_timeout();
        node_syncseq();
    }

    if (V.node_attn)
        node_maintain();
}


/*
 * Given a time that we want to give up on poll, return the time that we must
 * pass to the poll system call.  Note that we must call msg_ntime before
 * looking at V.time_ping as msg_ntime may set it if it detects clock skew.
 */
static int
poll_time(ntime_t etime)
{
    int timeout;
    ntime_t   now = msg_ntime();
    ntime_t ctime = V.time_ping;

    if (etime < 0) {
        if (ctime > 0)
            etime = ctime;
    } else if (ctime > 0 && ctime < etime)
        etime = ctime;

    if (etime < 0)
        timeout = -1;
    else {
        ntime_t t = etime - now;
        timeout = (t <= 0) ? 0 : t / (NANO/MSEC);
    }
    return timeout;
}


/*
 * Add a set of file descriptors to the poll list.
 */
static void
poll_prepare(task_t *task)
{
    node_t *node;
    conn_t *conn;

    poll_clr(task);
    if (task == V.tasks) {
        poll_add(task, NULL, V.wake_main[0],
                 (fdfunc_t)swallow, NULL, NULL, NULL);
        poll_add(task, NULL, V.udp_fd, udp_recv, NULL, NULL, NULL);
        poll_add(task, NULL, V.tcp_lfd, tcp_accept, NULL, NULL, NULL);
        if (V.init.mustlive)
            return;
    } else {
        poll_add(task, NULL, V.wake_work[0],
                 (fdfunc_t)nothing, NULL, NULL, NULL);
    }
        
    for (node = V.nodes->next; node; node = node->next) {
        for (conn = node->conn; conn; conn = conn->next) {
            void *warg;
            fdfunc_t wfunc;

            if (!rwl_tryr(conn->fdlock))
                continue;
            if (conn->shead)
                wfunc = (fdfunc_t)tcp_send, warg = conn;
            else
                wfunc = NULL, warg = NULL;
            poll_add(task, conn, conn->sfd,
                     (fdfunc_t)tcp_recv, conn, wfunc, warg);
        }
    }
}


/*
 * Clear the list of poll file descriptors.
 */
static void
poll_clr(task_t *task)
{
    task->pollfds.i = 0;
}


/*
 * Add a file descriptor to the poll list
 */
static void
poll_add(task_t *task, conn_t *conn, int fd,
         fdfunc_t rfunc, void *rarg, fdfunc_t wfunc, void *warg)
{
    int i = task->pollfds.i++;
    pollfd_t *poll = xasubs(&task->pollfds, i);
    callfd_t *call = xasubs(&task->callfds, i);

    clear(*poll);
    clear(*call);
    poll->fd = fd;
    call->conn = conn;

    if (rfunc) {
        poll->events |= POLLIN;
        call->rfunc = rfunc;
        call->rarg = rarg;
    }

    if (wfunc) {
        poll->events |= POLLOUT;
        call->wfunc = wfunc;
        call->warg = warg;
    }
}


/*
 * Swallow any data from the pipe that wakes up the main thread.
 */
static void
swallow(void)
{
    char buf[64];

    while (read(V.wake_main[0], buf, sizeof(buf)) > 0)
        ;
}


/*
 * A function that does nothing.
 */
static void
nothing(void)
{
}


/*
 * Execute a poll and take care of anything that needs servicing.  Return 0 if
 * we encountered a request to wake up, otherwise 1.
 */
static int
poll_execute(task_t *task, int timeout)
{
    int s;
    int n = task->pollfds.i;
    pollfd_t *pollp = task->pollfds.p;
    callfd_t *callp = task->callfds.p;

/* FIXME */
timeout = 0;
    s = poll((pollfd_t *)pollp, n, timeout);
    if (s < 0) {
        if (errno == EINTR)
            return poll_ret(0, callp, n);
        fatal_sys("select failed");
    }
    if (s == 0)
        return poll_ret(1, callp, n);
    
    for (; n; n--, callp++, pollp++) {
        int revents = pollp->revents;
        if (revents & POLLIN) {
            (*callp->rfunc)(callp->rarg);
            if (callp->rfunc == (fdfunc_t)swallow)
                return poll_ret(0, callp, n);
        }
        if (revents & POLLOUT)
            (*callp->wfunc)(callp->warg);
        if (callp->conn)
            rwl_unlockrb(callp->conn->fdlock);
    }
    return 1;
}


/*
 * Unlock all the file descriptors we held and return a value.
 */
static int
poll_ret(int ret, callfd_t *callp, int n)
{
    for (; n--; callp++)
        if (callp->conn)
            rwl_unlockrb(callp->conn->fdlock);
    return ret;
}


/*
 * Called when a UDP packet is received.
 */
static void
udp_recv(void *arg)
{
    path_t *path;
    node_t *node;
    hellostd_t hstd;

    if (!hello_udp_read(V.udp_fd, &hstd))
        return;

    if (t_on(CAST)) {
        char src[IPSIZE];
        cip_2str(&hstd.sip, src, sizeof(src));
        t_cast(0, "received %s from %s", type_msg(hstd.type), src);
    }

    path = node_hello(&hstd);
    node = path->node;
    if (msg_ntime() < node->wake_time)
        return;
    path_connect(path);
}


/*
 * Read a hello message from a TCP socket.
 */
static int
hello_tcp_read(int fd, hellostd_t *hstd)
{
    int size;
    hellobuf_t hbuf;
    int   len = sizeof(hello0_t);
    char *buf = (char *)&hbuf;

    if (read_nb(fd, buf, len, TIME_HELLO) != len)
        return 0;

    size = hello_size(&hbuf);
    if (size < 0)
        return 0;
    buf += len;
    len = size - sizeof(hello0_t);
    if (read_nb(fd, buf, len, TIME_HELLO) != len)
        return 0;

    if (!hello_conv(hstd, &hbuf, size))
        return 0;
    return 1;
}


/*
 * Read a hello message from a UDP socket.
 */
static int
hello_udp_read(int fd, hellostd_t *hstd)
{
    int len;
    sa_in_t saddr;
    socklen_t salen;
    hellobuf_t hbuf;

    salen = sizeof(sa_t);
    len = recvfrom(fd, &hbuf, sizeof(hbuf), 0, (sa_t *)&saddr, &salen);
    if (len < 0) {
        sdf_logi_sys(70049, "UDP receive failed");
        return 0;
    }

    if (hello_size(&hbuf) != len)
        return 0;
    if (!hello_conv(hstd, &hbuf, len))
        return 0;
    if (V.init.nobcast && cip_isbcast(&hstd->dip))
        return 0;

    cip_sa2(&hstd->sip, (sa_t *)&saddr);
    return 1;
}


/*
 * Size up a newly received hello message.
 */
static int
hello_size(hellobuf_t *hbuf)
{
    hello0_t *h0 = (hello0_t *) hbuf;
    int     cver = msg_retn2h(h0->cver);

    if (cver == V1_HELLO)
        return sizeof(hello1_t);
    if (cver == V2_HELLO)
        return sizeof(hello2_t);
    return -1;
}


/*
 * Given a hello message, convert it to our standardized version.  Return 1 if
 * the message is valid, else 0.
 */
static int
hello_conv(hellostd_t *hstd, hellobuf_t *hbuf, int len)
{
    hello0_t *h0 = (hello0_t *) hbuf;

    clear(*hstd);
    hstd->cver  = msg_retn2h(h0->cver);

    if (hstd->cver == V1_HELLO) {
        if (!hello_conv1(hstd, (hello1_t *)hbuf))
            return 0;
    } else if (hstd->cver == V2_HELLO) {
        if (!hello_conv2(hstd, (hello2_t *)hbuf))
            return 0;
    } else
        return 0;

    if (hstd->sver < hstd->cver)
        return 0;
    return 1;
}


/*
 * Convert a version 1 hello message to our standardized version.  The common
 * information in hello0_t has already been converted.
 */
static int
hello_conv1(hellostd_t *hstd, hello1_t *h1)
{
    hstd->class = msg_retn2h(h1->class);
    hstd->type  = msg_retn2h(h1->type);
    hstd->port  = msg_retn2h(h1->port);

    if (hstd->class != V.init.class)
        return 0;

    hstd->dip   = h1->dip;
    cip_setn2h(&hstd->dip);
    hstd->nconn = 1;
    return conv1_name(hstd, h1);
}


/*
 * Convert the name, supported version and unique IP from a hello1 message.
 */
static int
conv1_name(hellostd_t *hstd, hello1_t *h1)
{
    int n;
    char *q;
    char *r;
    char buf[IPSIZE];
    char *p = h1->name;
    char *e = p + sizeof(h1->name);

    hstd->sver  = V1_HELLO;
    if (*p == 'V') {
        int sver = 0;
        while (++p < e-1) {
            int c = *p;
            if (!isdigit(c))
                break;
            sver = sver*10 + c - '0';
        }
        if (*p++ == ':')
            hstd->sver = sver;
        else
            p = h1->name;
    }

    q = strechr(p, '<', e);
    if (!q)
        return 0;
    r = strechr(q, ':', e);
    if (!r)
        return 0;
    n = r - (q+1);
    if (n >= IPSIZE)
        return 0;
    memcpy(buf, q+1, n);
    buf[n] = '\0';
    if (!cip_str2(&hstd->uip, buf))
        return 0;

    n = q - p;
    strncopyfill(hstd->name, sizeof(hstd->name), p, n);
    return 1;
}


/*
 * Look for the character del in string str which is terminated by reaching end
 * or by reaching a null.
 */
static char *
strechr(char *str, int del, char *end)
{
    int c;

    for (; str < end && (c = *str) != '\0'; str++)
        if (c == del)
            return str;
    return NULL;
}


/*
 * Convert a version 2 hello message to our standardized version.  The common
 * information in hello0_t has already been converted.
 */
static int
hello_conv2(hellostd_t *hstd, hello2_t *h2)
{
    hstd->sver  = msg_retn2h(h2->sver);
    hstd->class = msg_retn2h(h2->class);
    hstd->type  = msg_retn2h(h2->type);
    hstd->port  = msg_retn2h(h2->port);

    if (hstd->class != V.init.class)
        return 0;

    hstd->uip = h2->uip;
    cip_setn2h(&hstd->dip);
    hstd->dip = h2->dip;
    cip_setn2h(&hstd->dip);
    hstd->nconn = msg_retn2h(h2->nconn);
    strncopyfill(hstd->name, sizeof(hstd->name), h2->name, sizeof(h2->name));
    return 1;
}


/*
 * Send a hello message to all the recipient nodes.
 */
static void
hello_send(int type)
{
    int len;
    rips_t *rips;
    hellobuf_t hbuf;
    char dest[IPSIZE];
    sa_in_t saddr ={
        .sin_family = AF_INET,
        .sin_port   = htons(V.init.udpport),
    };
    socklen_t salen = sizeof(struct sockaddr_in);

    len = hello_fill(&hbuf, type, NULL);
    for (rips = V.cast_rips; rips; rips = rips->next) {
        hello_setdip(&hbuf, &rips->rip);
        saddr.sin_addr.s_addr = cip_2v4a(&rips->rip);
        if (sendto(rips->fd, &hbuf, len, 0, (sa_t *)&saddr, salen) != len) {
            if (!rips->fail) {
                cip_2str(&rips->rip, dest, sizeof(dest));
                sdf_logi_sys(70050, "UDP send failed: %s to %s",
                             type_msg(type), dest);
            }
            rips->fail = 1;
        } else {
            if (rips->fail) {
                cip_2str(&rips->rip, dest, sizeof(dest));
                sdf_logi(70051, "UDP send succeeded: %s to %s",
                         type_msg(type), dest);
            }
            rips->fail = 0;
        }

        if (t_on(CAST)) {
            cip_2str(&rips->rip, dest, sizeof(dest));
            t_cast(0, "sending %s to %s", type_msg(type), dest);
        }
    }
}


/*
 * Prepare a hello message and fill everything we can.  If path is not given,
 * the destination IP is not filled in.
 */
static int
hello_fill(hellobuf_t *hbuf, int type, path_t *path)
{
    hello0_t *h0 = &hbuf->hello0;
    int      ver = path ? path->node->ver : V1_HELLO;

    clear(*hbuf);
    h0->cver  = ver;
    msg_seth2n(h0->cver);

    if (ver == V1_HELLO) {
        hello1_t *h1 = &hbuf->hello1;

        h1->class = V.init.class;
        h1->type  = type;
        h1->port  = V.init.tcpport;

        msg_seth2n(h1->class);
        msg_seth2n(h1->type);
        msg_seth2n(h1->port);

        if (path) {
            h1->dip = path->rip;
            cip_seth2n(&h1->dip);
        }

        strncopyfill(h1->name, sizeof(h1->name), V.nodes->nipp, 0);
        return sizeof(*h1);
    } else if (ver == V2_HELLO) {
        hello2_t *h2 = &hbuf->hello2;

        h2->sver  = V.version;
        h2->class = V.init.class;
        h2->type  = type;
        h2->port  = V.init.tcpport;

        msg_seth2n(h2->sver);
        msg_seth2n(h2->class);
        msg_seth2n(h2->type);
        msg_seth2n(h2->port);

        if (path) {
            h2->dip = path->rip;
            cip_seth2n(&h2->dip);
        }

        h2->uip = V.uip;
        strncopyfill(h2->name, sizeof(h2->name), V.init.alias, 0);
        cip_seth2n(&h2->uip);
        if (type == MSG_EJOIN) {
            h2->nconn = path ? path->mconn : 1;
            msg_seth2n(h2->nconn);
        }
        return sizeof(*h2);
    } else
        fatal("hello_fill: bad version %d", ver);
}


/*
 * Set the destination IP in a hello message that has been filled by
 * hello_fill.
 */
static void
hello_setdip(hellobuf_t *hbuf, cip_t *dip)
{
    int ver = msg_retn2h(hbuf->hello0.cver);

    if (ver != V1_HELLO)
        fatal("hello_setdip: bad version %d", ver);
    hbuf->hello1.dip = *dip;
    cip_seth2n(&hbuf->hello1.dip);
}


/*
 * Update nodes based on a hello message we received.
 */
static path_t *
node_hello(hellostd_t *hstd)
{
    path_t *path;
    node_t *node;

    node = node_getbyip(&hstd->uip, hstd->port);
    if (!node)
        node = node_new(hstd->sver, &hstd->uip, hstd->port, hstd->name);
    path = path_getbyip(node, &hstd->sip);
    if (!path) {
        path = path_new(node, &hstd->dip, &hstd->sip);
        path_update(path, hstd);
    }

    path_alive(path, msg_ntime());
    return path;
}


/*
 * Perform maintenance on the nodes that have errors on a connection.
 */
static void
node_maintain(void)
{
    conn_t *conn;
    node_t *node;

    if (!V.node_attn)
        return;

    V.node_attn = 0;
    for (node = V.nodes->next; node; node = node->next) {
        if (!node->live)
            continue;

        for (conn = node->conn; conn; conn = conn->next) {
            if (conn->state != CS_ERROR)
                continue;

            if (!rwl_isreqw(conn->fdlock))
                rwl_reqw(conn->fdlock);
            if (!rwl_reqtryw(conn->fdlock))
                V.node_attn = 1;
            else
                conn_close(conn);
        }

        if (node->ver == V1_HELLO) {
            if (node->conn->state != CS_READY)
                node_dead(node);
        } else {
            if (!node->aconn)
                node_dead(node);
        }
    }
}


/*
 * This is called when we detect clock skew.  We reinitialize the times
 * relevant to the node's liveness giving them a new lease on life.  We also
 * ensure that a new ping gets sent out.
 */
static void
node_redo_time(ntime_t now)
{
    node_t *node;
    path_t *path;

    for (node = V.nodes->next; node; node = node->next) {
        if (!node->live)
            continue;
        for (path = node->path; path; path = path->next)
            path_alive(path, now);
    }
}


/*
 * Timeout any interfaces and nodes that we have not heard from in a while.
 */
static void
node_timeout(void)
{
    ntime_t now;
    node_t *node;
    path_t *path;
    conn_t *conn;

    if (!V.init.live)
        return;

    now = msg_ntime();
    for (node = V.nodes->next; node; node = node->next) {
        int alive = 0;
        int ready = 0;

        if (!node->live)
            continue;
        for (conn = node->conn; conn; conn = conn->next)
            if (conn->state == CS_READY)
                ready++;
        for (path = node->path; path; path = path->next) {
            ntime_t since = now - path->ltime;
            if (since <= V.init.live)
                alive++;
            if (since > V.init.linklive) {
                for (conn = node->conn; conn; conn = conn->next) {
                    if (conn->state != CS_READY)
                        continue;
                    if (conn->path != path)
                        continue;
                    if (--ready)
                        conn_error(conn, 0, "timeout");
                }
            }
        }
        if (!alive)
            node_drop(node, "timeout");
    }
}


/*
 * Drop a node for the specified reason.
 */
static void
node_drop(node_t *node, char *msg)
{
    conn_t *conn;
    char nbuf[NOSIZE];

    node_no(node, nbuf, sizeof(nbuf));
    sdf_logi(70052, "node drop rn=%s: %s", nbuf, msg);
    node->to_die = 1;
    for (conn = node->conn; conn; conn = conn->next)
        if (conn->state == CS_READY)
            conn->state = CS_ERROR;

    barrier();
    V.node_attn = 1;
}


/*
 * Synchronize any sequence numbers that are needed.  If we have not
 * acknowledged the last message we received in a while do so.  If our last
 * contiguous message is slow to increase.  If we have not received an ack,
 * resend the last message.
 */
static void
node_syncseq(void)
{
    node_t *node;
    atom_t ackd;

    for (node = V.nodes->next; node; node = node->next) {
        if (!node->aconn)
            continue;
        if (node->ver < V2_HELLO)
            continue;

        ackd = node->rseqn_ackd;
        if (node->rseqn_cont != ackd && ackd == node->rseqn_ackd_last)
            meta_send(node, MM_ACKSEQ, 0);
        node->rseqn_ackd_last = node->rseqn_ackd;

        ackd = node->sseqn_ackd;
        if (node->sseqn_used != ackd && ackd == node->sseqn_ackd_last)
            node_resend(node, ackd+1);
        node->sseqn_ackd_last = ackd;
    }
}


/*
 * Send a metadata message to request data to be resent; if needed.
 */
static void
send_resend(node_t *node)
{
    atom_t cont;
    atom_t next;

    /* FIXME */
    wl_lock(node->lock_recv);
    cont = node->rseqn_cont;
    wl_unlock(node->lock_recv);
    next = cont+2;

    if (!next)
        return;
    if (next < cont+2)
        fatal("received contiguous sequence number yet stuck");
    meta_send(node, MM_RESEND, next-1);
}


/*
 * Send a metadata message.
 */
static void
meta_send(node_t *node, int type, atom_t seqn)
{
    msg_send_t *send = msg_salloc();
    meta_t     *meta = m_malloc(sizeof(meta_t), "meta_t");

    if (t_on(META)) {
        atom_t cseqn = node->rseqn_cont;

        if (type == MM_ACKSEQ)
            t_meta(0, "send m%d: ackseq %ld", node->nno, cseqn);
        else if (type == MM_RESEND)
            t_meta(0, "send m%d: resend %ld-%ld (%ld)",
                   node->nno, cseqn+1, seqn, seqn-cseqn);
        else
            fatal("sending bad metadata type=%d to n%d", type, node->nno);
    }

    meta->type = type;
    meta->seqn = seqn;

    msg_seth2n(meta->type);
    msg_seth2n(meta->seqn);

    send->nno   = node->nno;
    send->dtag  = META_TAG;
    send->flags = MS_USESEQ;
    send->nsge  = 1;
    send->data  = meta;
    send->sge[0].iov_base = meta;
    send->sge[0].iov_len = sizeof(*meta);
    send_out(node, send, 1);
}


/*
 * Interpret incoming metadata.
 */
static void
meta_recv(node_t *node, msg_info_t *info, atom_t aseqn)
{
    meta_t *meta = (meta_t *)info->data;
    int     type = msg_retn2h(meta->type);

    if (type == MM_ACKSEQ)
        t_meta(0, "recv m%d: ackseq %ld", node->nno, aseqn);
    else if (type == MM_RESEND) {
        t_meta(0, "recv m%d: resend from %ld", node->nno, aseqn+1);
        node_resend(node, aseqn+1);
    } else {
        t_meta(0, "recv m%d: bad metadata type=%d", node->nno, type);
        sdf_logi(70053, "recv n%d: bad metadata type=%d", node->nno, type);
    }

    msg_ifree(info);
}


/*
 * Resend messages in the given interval to the other node.
 */
static void
node_resend(node_t *node, atom_t lseqn)
{
    int n;
    msg_send_t *p;
    msg_send_t *q;
    msg_send_t **pp;
    msg_send_t **qq = &q;
    msg_send_t *tail = NULL;

    n = 0;
    wl_lock(node->lock_sent);
    for (pp = &node->sent_head; (p = *pp) != NULL;) {
        atom_t seqn = p->seqn;
        if (seqn >= lseqn) {
            n++;
            *pp = p->link;
            *qq = p;
            qq = &p->link;
        } else {
            pp = &p->link;
            tail = p;
        }
    }
    node->sent_tail = tail;
    wl_unlock(node->lock_sent);
    *qq = NULL;

    sdf_logi(70054, "resending %d messages from %ld", n, lseqn);
    while ((p = q) != NULL) {
        q = p->link;
        p->flags |= MS_USESEQ;
        send_out(node, p, 1);
    }
}


/*
 * Set a liveness function to be called.
 */
void
msg_livecall(int on, int fth, msg_livefunc_t *func, void *arg)
{
    if (on) {
        lcall_t *lcall = malloc_z(sizeof(*lcall), "lcall_t");

        lcall->func = func;
        lcall->arg  = arg;
        if (fth) {
            lcall->fth = 1;
            fthMboxInit(&lcall->mbox);
            XResume(fthSpawn(fth_liveness, FTH_SSIZE), (uint64_t)lcall);
        }

        wl_lock(V.live_lock);
        lcall->next = V.live_list;
        V.live_list = lcall;
        wl_unlock(V.live_lock);
        live_call_one(lcall);
    } else {
        lcall_t *lcall;
        lcall_t **pp = &V.live_list;

        wl_lock(V.live_lock);
        for (;;) {
            lcall = *pp;
            if (!lcall)
                break;
            if (lcall->func == func && lcall->arg == arg) {
                *pp = lcall->next;
                if (lcall->fth)
                    fthMboxPost(&lcall->mbox, 0);
                else
                    m_free(lcall);
            } else
                pp = &lcall->next;
        }
        wl_unlock(V.live_lock);
    }
}


/*
 * Call all liveness functions and inform them of a state transition.
 */
static void
live_call_all(int live, int rank)
{
    lcall_t *lcall;

    wl_lock(V.live_lock);
    for (lcall = V.live_list; lcall; lcall = lcall->next)
        live_call(lcall, live, rank);
    wl_unlock(V.live_lock);
}


/*
 * Call a liveness function about all nodes that are currently live.
 */
static void
live_call_one(lcall_t *lcall)
{
    node_t *node;

    for (node = V.nodes; node; node = node->next)
        if (node->rank_state == NS_ACTIVE)
            live_call(lcall, 1, node->rank);
}


/*
 * Call a liveness function about all nodes that are currently live.  The
 * decoding of the mailbox argument is only done by fth_liveness.
 */
static void
live_call(lcall_t *lcall, int live, int rank)
{
    if (!lcall->fth)
        (*lcall->func)(live, rank, lcall->arg);
    else
        fthMboxPost(&lcall->mbox, (rank<<2) | (live<<1) | 1);
}


/*
 * A fth function to call liveness functions as needed. The encoding of the
 * mailbox argument is only done by live_call.
 */
static void
fth_liveness(uint64_t arg)
{
    lcall_t *lcall = (lcall_t *) arg;

    for (;;) {
        int live;
        int rank;
        uint64_t x = fthMboxWait(&lcall->mbox);

        if (!x)
            break;
        live = (x >> 1) & 1;
        rank = (x >> 2);
        (*lcall->func)(live, rank, lcall->arg);
    }
    fthMboxTerm(&lcall->mbox);
    m_free(lcall);
}


/*
 * Return the name of a state.
 */
static char *
node_state_name(node_state_t state)
{
    if      (state == NS_UNUSED) return "UNUSED";
    else if (state == NS_WAITID) return "WAITID";
    else if (state == NS_NORANK) return "NORANK";
    else if (state == NS_ALMOST) return "ALMOST";
    else if (state == NS_ACTIVE) return "ACTIVE";
    else                         return "?";
}


/*
 * Send my current state to the other nodes.
 */
static void
node_talk(void)
{
    node_t *node;

    for (node = V.nodes->next; node; node = node->next)
        if (node->rank_state != NS_UNUSED)
            send_talk(node);
}


/*
 * Send a talk message.
 */
static void
send_talk(node_t *node)
{
    talk_t *talk;
    node_t       *me = V.nodes;
    msg_send_t *send = msg_salloc();

    sdf_logi(70065, "send_talk node=m%d state=%s rank=%d",
             node->nno, node_state_name(me->rank_state), me->rank);

    talk = m_malloc(sizeof(*talk), "talk_t");
    talk->magic = htonl(TALK_MAGIC);
    talk->state = htons(me->rank_state);
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
 * Return our node number.
 */
int
msg_mynodeno(void)
{
    return V.nodes->nno;
}


/*
 * Set the rank of a node.
 */
void
msg_setrank(int nno, int rank)
{
    node_t *node;

    node = node_getbynno(nno);
    if (!node)
        return;
    node->rank = rank;
}


/*
 * Set the rank state of a node.
 */
void
msg_setstate(int nno, int new)
{
    int old;
    node_t *node;

    node = node_getbynno(nno);
    if (!node)
        return;
    old = node->rank_state;
    node->rank_state = new;

    if (old != NS_ACTIVE && new == NS_ACTIVE) {
        sdf_logi(70066, "node n%d is live", node->rank);
        fsync(2);
        live_call_all(1, node->rank);
    } else if (old == NS_ACTIVE && new != NS_ACTIVE) {
        sdf_logi(70067, "node n%d is dead", node->rank);
        fsync(2);
        live_call_all(0, node->rank);
    }
}


/*
 * Convert a node number to a rank.  On error, -1 is returned.
 */
int
msg_nno2rank(int nno)
{
    node_t *node;

    node = node_getbynno(nno);
    if (!node)
        return -1;
    return node->rank;
}


/*
 * Convert a rank to a node number.  On error, -1 is returned.
 */
int
msg_rank2nno(int rank)
{
    node_t *node;

    node = node_getbyrank(rank);
    if (!node)
        return -1;
    return node->nno;
}


/*
 * Post a liveness event.
 */
static void
live_post(node_t *node, int event)
{
    msg_info_t *info = msg_ialloc();

    /* Information */
    {
        char nbuf[NOSIZE];
        char *s = (event==MSG_EJOIN) ? "connected" : "disconnected";

        node_no(node, nbuf, sizeof(nbuf));
        sdf_logi(70068, "node %s is %s", nbuf, s);
    }
    info->type = event;
    info->nno  = node->nno;
    msg_post(info);
}


/*
 * Find a node given its unique IP and port.
 */
static node_t *
node_getbyip(cip_t *uip, msg_port_t port)
{
    node_t *node;

    for (node = V.nodes; node; node = node->next)
        if (port == node->port && cip_eq(uip, &node->uip))
            return node;
    return NULL;
}


/*
 * Find a node given its node number.
 */
static node_t *
node_getbynno(int nno)
{
    node_t *node;

    for (node = V.nodes; node; node = node->next)
        if (node->nno == nno)
            return node;
    return NULL;
}


/*
 * Find a node given its rank.
 */
static node_t *
node_getbyrank(int rank)
{
    node_t *node;

    for (node = V.nodes; node; node = node->next)
        if (node->rank == rank)
            return node;
    return NULL;
}


/*
 * Allocate a new node and add it to the node list.
 *
 *  ver  - The messaging version that we will be communicating to this node.
 *  uip  - The unique IP that the node is identified by along with its port.
 *  port - See uip.
 *  name - The name of this node.
 */
static node_t *
node_new(int ver, cip_t *uip, msg_port_t port, char name[NNSIZE])
{
    int nno;
    node_t **pp;
    char ubuf[IPSIZE];
    node_t *node = malloc_z(sizeof(node_t), "node_t");

    if (ver > V.version)
        ver = V.version;

    memcpy(node->name, name, sizeof(node->name));
    node->uip        = *uip;
    node->ver        = ver;
    node->hlen       = (ver == V1_HELLO) ? sizeof(head1_t) : sizeof(head2_t);
    node->port       = port;
    node->rank       = -1;
    node->lock_send  = wl_init();
    node->lock_sent  = wl_init();
    node->lock_recv  = wl_init();
    node->rinfo_soff = 2;
    node->rinfo_size = V.init.nhold;
    node->rinfo_hold = malloc_z(node->rinfo_size * sizeof(msg_info_t *),
                                "N*msg_info_t");

    cip_2str(uip, ubuf, sizeof(ubuf));
    node->nipp = m_asprintf("msg_msg:", "V%d:%s<%s:%d>",
                            V.version, name, ubuf, port);

    nno = 1;
    for (pp = &V.nodes; *pp; pp = &(*pp)->next)
        nno++;
    node->nno = nno;
    barrier();
    *pp = node;

    /* Information */
    {
        char nbuf[NOSIZE];
        char ubuf[IPSIZE];

        node_no(node, nbuf, sizeof(nbuf));
        cip_2str(&node->uip, ubuf, sizeof(ubuf));
        sdf_logi(70069, "node new rn=%s uip=%s ver=%d", nbuf, ubuf, node->ver);
    }
    return node;
}


/*
 * Declare a node alive.
 */
static void
node_live(node_t *node)
{
    if (node->live)
        return;

    node->rinfo_soff      = 2;
    node->rseqn_ackd      = 0;
    node->rseqn_cont      = 0;
    node->rseqn_high      = 0;
    node->sseqn_ackd      = 0;
    node->sseqn_used      = 0;
    node->rseqn_cont_last = 0;
    node->rseqn_ackd_last = 0;
    node->sseqn_ackd_last = 0;

    node->live = 1;
    live_post(node, MSG_EJOIN);
    send_talk(node);
}


/*
 * Declare a node dead.
 */
static void
node_dead(node_t *node)
{
    conn_t *conn;
    msg_send_t *send;

    if (!node->live)
        return;
    node->live = 0;
    node->to_die = 0;
    if (V.init.dead)
        node->wake_time = msg_ntime() + V.init.dead;
    live_post(node, MSG_EDROP);

    for (conn = node->conn; conn; conn = conn->next) {
        if (conn->state == CS_AVAIL)
            continue;

        rwl_lockw(conn->fdlock);
        send = conn_sqgetlist(conn);
        rwl_unlockw(conn->fdlock);
        send_list_fail(send);
    }

    send = node_send_getlist(node);
    send_list_fail(send);

    node_sent_fail(node);
    node_rinfo_fail(node);
}


/*
 * Return the next sending sequence number.
 */
static msg_seqn_t
node_sent_seqn(node_t *node)
{
    return atomic_inc_get(node->sseqn_used);
}


/*
 * Set the acknowledge sequence number if our new value is higher.
 */
static void
node_set_aseqn(node_t *node, atom_t aseqn)
{
    if (atomic_max(&node->sseqn_ackd, aseqn))
        node_sent_free(node);
}


/*
 * Keep a message we just sent in case we need to resend it.
 */
static void
node_sent_save(node_t *node, msg_send_t *send)
{
    if (node->ver == V1_HELLO) {
        post_sent(send, NULL);
        return;
    }

    send->link = NULL;
    wl_lock(node->lock_sent);
    if (node->sent_head)
        node->sent_tail = node->sent_tail->link = send;
    else
        node->sent_head = node->sent_tail = send;
    wl_unlock(node->lock_sent);
}


/*
 * Free any messsage we can from the send list.  Since the list is not kept in
 * sorted order, we only go through until we get blocked by a message that
 * should not be freed knowing that we will get through them all eventually.
 */
static void
node_sent_free(node_t *node)
{
    msg_send_t *p;
    msg_send_t **pp;
    msg_send_t *post;
    atom_t aseqn = node->sseqn_ackd;

    wl_lock(node->lock_sent);
    for (pp = &node->sent_head; (p = *pp) != NULL; pp = &p->link)
        if (p->seqn > aseqn)
            break;
    *pp = NULL;
    post = node->sent_head;
    node->sent_head = p;
    if (!node->sent_head)
        node->sent_tail = NULL;
    wl_unlock(node->lock_sent);

    while ((p = post) != NULL) {
        post = p->link;
        post_sent(p, NULL);
    }
}


/*
 * Cause any messages that are waiting for acknowledgement that they were
 * received to fail.
 */
static void
node_sent_fail(node_t *node)
{
    msg_send_t *p;
    msg_send_t *sent;

    wl_lock(node->lock_sent);
    sent = node->sent_head;
    node->sent_head = NULL;
    node->sent_tail = NULL;
    wl_unlock(node->lock_sent);

    while ((p = sent) != NULL) {
        sent = p->link;
        post_sent(p, "node down");
    }
}


/*
 * Convert our node into a readable number.
 */
static void
node_no(node_t *node, char *buf, int len)
{
    if (node->rank >= 0)
        snprintf(buf, len, "n%d", node->rank);
    else
        snprintf(buf, len, "m%d", node->nno);
}


/*
 * Show our nodes.
 */
static void
node_show(void)
{
    int n;
    conn_t *conn;
    path_t *path;
    node_t *node;
    char lbuf[IPSIZE];
    char rbuf[IPSIZE];

    if (!t_on(DBUG))
        return;

    t_dbug("========================================");
    for (node = V.nodes; node; node = node->next) {
        cip_2str(&node->uip, lbuf, sizeof(lbuf));
        t_dbug("node nno=%d ver=%d aconn=%d uip=%s port=%d",
               node->nno, node->ver, node->aconn, lbuf, node->port);

        for (path = node->path, n=1; path; path = path->next, n++) {
            cip_2str(&path->lip, lbuf, sizeof(lbuf));
            cip_2str(&path->rip, rbuf, sizeof(rbuf));
            t_dbug("  p%d: path nc=%d pr=%d init=%d lip=%s rip=%s",
                   n, path->nconn, path->prior, path->init, lbuf, rbuf);
        }

        for (conn = node->conn; conn; conn = conn->next) {
            n = path_no(node, conn->path);
            t_dbug("  conn path=p%d sfd=%d state=%s shead=%p",
                   n, conn->sfd, conn_state_name(conn), conn->shead);
        }
    }
    t_dbug("========================================");
}


/*
 * Allocate a new path and add it to the path list for the given node.
 */
static path_t *
path_new(node_t *node, cip_t *lip, cip_t *rip)
{
    path_t **pp;
    int r = cip_cmp(&V.nodes->uip, &node->uip);
    path_t *path = malloc_z(sizeof(path_t), "path_t");

    path->node  = node;
    path->lip   = *lip;
    path->rip   = *rip;
    path->init  = r > 0 || (r == 0 && V.nodes->port > node->port);
    path->iface = "*";

    if (!ismyip(rip) || node->port != V.nodes->port) {
        lface_t *lface = lface_getbylbip(lip);
        if (lface) {
            path->iface = lface->name;
            path->prior = lface->prior;
            path->mconn = lface->nconn;
            if (!path->mconn)
                path->mconn = V.init.nconn;
        }
    }

    for (pp = &node->path; *pp; pp = &(*pp)->next)
        ;
    barrier();
    *pp = path;

    path_msg(path, "new");
    return path;
}


/*
 * Update a path with possibly new information.
 */
static void
path_update(path_t *path, hellostd_t *hstd)
{
    int nconn = 0;

    if (path->prior && hstd->type == MSG_EJOIN)
        nconn = hstd->nconn;

    if (nconn > path->mconn)
        nconn = path->mconn;
    if (path->nconn != nconn) {
        path->nconn = nconn;
        path_msg(path, "mod");
    }
}


/*
 * Note that we have just heard from the remote node.
 */
static void
path_alive(path_t *path, ntime_t new)
{
    for (;;) {
        ntime_t old = path->ltime;
        if (new <= old)
            break;
        if (atomic_cmp_swap(path->ltime, old, new) == old)
            break;
    }
}


/*
 * Check a path and see if we need to make or drop any connections.
 */
static void
path_connect(path_t *path)
{
    conn_t *conn;
    int cmin = 0;
    int cmax = 0;

    for (conn = path->node->conn; conn; conn = conn->next) {
        if (conn->state == CS_AVAIL)
            continue;
        if (conn->path != path)
            continue;
        cmax++;
        if (conn->state != CS_READY)
            continue;
        if (++cmin > path->nconn)
            conn_error(conn, 0, "too many connections");
    }

    if (path->init)
        for (; cmax < path->nconn; cmax++)
            if (!tcp_connect(path))
                break;
}


/*
 * Find a path by its remote IP.
 */
static path_t *
path_getbyip(node_t *node, cip_t *rip)
{
    path_t *path;

    for (path = node->path; path; path = path->next)
        if (cip_eq(rip, &path->rip))
            return path;
    return NULL;
}


/*
 * Print out a path message.
 */
static void
path_msg(path_t *path, char *msg)
{
    char nbuf[NOSIZE];
    char lbuf[IPSIZE];
    char rbuf[IPSIZE];

    node_no(path->node, nbuf, sizeof(nbuf));
    cip_2str(&path->lip, lbuf, sizeof(lbuf));
    cip_2str(&path->rip, rbuf, sizeof(rbuf));
    sdf_logi(70077, "path %s rn=%s if=%s lip=%s rip=%s nc=%d",
             msg, nbuf, path->iface, lbuf, rbuf, path->nconn);
}


/*
 * Return the number of a path relative to a node.
 */
static int
path_no(node_t *node, path_t *path)
{
    int n;
    path_t *p;

    for (p = node->path, n = 1; p; p = p->next, n++)
        if (p == path)
            return n;
    return 0;
}


/*
 * fdcall_set, fdcall_clr and fdcall_call are used to cause a function to be
 * called when its associated file descriptor becomes available.  fdcall_set
 * causes the function func to be called whenever file descriptor fd becomes
 * available for reading if rw is 0 or writing if rw is 1.  The function func
 * is then called with the argument arg.  fdcall_clr clears this association
 * and fdcall_call causes the appropriate call to take place.
 */
static void
fdcall_set(int fd, int rw, fdfunc_t func, void *arg)
{
    int i;
    int f;
    int          n = V.fdcall.i;
    fdcall_t *list = V.fdcall.p;

    f = -1;
    for (i = 0; i < n; i++) {
        if (list[i].fd == fd)
            break;
        if (f < 0 && list[i].fd < 0)
            f = i;
    }

    if (i == n) {
        if (f >= 0)
            i = f;
        else {
            xasubs(&V.fdcall, i);
            list = V.fdcall.p;
            V.fdcall.i++;
        }
        list[i].fd = fd;
        list[i].func[0] = NULL;
        list[i].func[1] = NULL;
    }

    if (list[i].func[rw])
        fatal("fdcall_set: func in use: fd=%d rw=%d func=%p", fd, rw, func);
    list[i].func[rw] = func;
    list[i].arg[rw]  = arg;
}


/*
 * See comment to fdcall_set.
 */
static void
fdcall_clr(int fd, int rw)
{
    int i;
    int          n = V.fdcall.i;
    fdcall_t *list = V.fdcall.p;

    for (i = 0; i < n; i++) {
        if (list[i].fd != fd)
            continue;
        if (!list[i].func[rw])
            fatal("fdcall_clr: func not set: fd=%d rw=%d", fd, rw);
        list[i].func[rw] = NULL;
        list[i].arg[rw]  = NULL;
        if (!list[i].func[!rw])
            list[i].fd = -1;
        return;
    }
    fatal("fdcall_clr: cannot find fd: fd=%d rw=%d", fd, rw);
}


/*
 * See if the given IP is one of ours.  This may be called before the messaging
 * system is initialized.
 */
int
msg_ismyip(char *name)
{
    cip_t cip;

    if (!cip_str2(&cip, name))
        return 0;

    lface_init();
    return ismyip(&cip);
}


/*
 * See if the given IP is one of ours.
 */
static int
ismyip(cip_t *cip)
{
    int i;

    for (i = 0; i < V.lface_n; i++)
        if (cip_eq(cip, &V.lface_p[i].local))
            return 1;
    return 0;
}


/*
 * Find an interface by name.
 */
static lface_t *
lface_getbyname(char *name)
{
    int      n = V.lface_n;
    lface_t *p = V.lface_p;

    for (; n--; p++)
        if (streq(name, p->name))
            return p;
    return NULL;
}


/*
 * Find an interface by either the local or the broadcast IP.
 */
static lface_t *
lface_getbylbip(cip_t *lip)
{
    int      n = V.lface_n;
    lface_t *p = V.lface_p;

    for (; n--; p++)
        if (p->prior)
            if (cip_eq(lip, &p->local) || cip_eq(lip, &p->bcast))
                return p;
    return NULL;
}


/*
 * Scan a sequence of interfaces and return values about a given interface.  If
 * the interface is found, 1 is returned otherwise 0.  If no list is given, we
 * assume default values of 1 for the priorities and 0 for the value.
 * V.init.iface might look something like this: eth0=3:eth1::eth7:eth9=2.
 *
 *  name - A single interface such as eth7.
 *  valp - The value associated with the name (eth0 => 3, eth9 => 4) is
 *         returned here.  If there is no value, 0 is returned.
 *  pr1p - The position in the list (eth0 => 1, eth1 => 2, eth7 => 3, eth9 =>
 *         4).  If not in the list, 0 is returned.
 *  pr1p - The position in the double colon list (eth0 => 1, eth1 => 1, eth7 =>
 *         2, eth9 => 2).  If not in the list, 0 is returned.
 */
static int
iface_prior(char *name, int *valp, int *pr1p, int *pr2p)
{
    int   val = 0;
    int   pr1 = 1;
    int   pr2 = 1;
    int   len = strlen(name);
    char *ptr = V.init.iface;

    if (ptr) {
        for (;;) {
            int n = strcspn(ptr, ":=");
            if (n == len && memcmp(ptr, name, len) == 0) {
                ptr += n;
                if (*ptr == '=')
                    val = atoi(ptr+1);
                break;
            }
            ptr += n;
            if (*ptr == '=')
                ptr += strcspn(ptr, ":");
            if (*ptr == ':') {
                if (ptr[1] == ':')
                    pr2++;
                while (*ptr == ':')
                    ptr++;
            }
            if (*ptr == '\0')
                return 0;
            pr1++;
        }
    }

    if (valp)
        *valp = val;
    if (pr1p)
        *pr1p = pr1;
    if (pr2p)
        *pr2p = pr2;
    return 1;
}


/*
 * Add a remote node name to the list of nodes that we attempt to contact.
 */
void
msg_addrname(char *name)
{
    int s;
    cip_t cip;
    char buf[16];
    addrinfo_t *a;
    addrinfo_t *ailist;
    addrinfo_t hints ={
        .ai_flags    = AI_NUMERICSERV,
        .ai_family   = AF_INET,
        .ai_socktype = SOCK_DGRAM
    };

    snprintf(buf, sizeof(buf), "%d", V.init.udpport);
    s = getaddrinfo(name, buf, &hints, &ailist);
    if (s != 0) {
        sdf_loge(70055, "getaddrinfo %s:%s failed: %s",
                 name, buf, gai_strerror(s));
        return;
    }
    if (!ailist) {
        sdf_loge(70056, "getaddrinfo %s:%s failed: no valid entries",
                 name, buf);
        return;
    }

    for (a = ailist; a; a = a->ai_next) {
        if (a->ai_family != AF_INET)
            continue;
        cip_sa2(&cip, a->ai_addr);
        add_cast(NULL, &cip);
    }
    freeaddrinfo(ailist);
}


/*
 * Add a remote node to the list of nodes that we attempt to contact.  We pass
 * its name, IP address and interface name.
 */
int
msg_addraddr(char *name, char *addr, char *iface)
{
    cip_t rip;
    cip_t *lip = NULL;

    if (!cip_str2(&rip, addr))
        return 0;
    if (iface) {
        lface_t *lface = lface_getbyname(iface);
        if (!lface)
            fatal("failed to find contact interface %s (%s)", iface, addr);
        lip = &lface->local;
    }
    add_cast(lip, &rip);
    return 1;
}


/*
 * Add an unicast/broadcast IP to the list ensuring it is unique.
 */
static void
add_cast(cip_t *lip, cip_t *rip)
{
    char rbuf[IPSIZE];
    rips_t *q = malloc_z(sizeof(*q), "rips_t");

    if (lip)
        q->lip = *lip;
    q->rip = *rip;
    q->fd = udp_open(lip, 0);

    if (q->fd < 0) {
        cip_2str(rip, rbuf, sizeof(rbuf));
        sdf_loge(70075, "failed to add contact IP: %s", rbuf);
    } else {
        rips_t *p;
        rips_t **pp;

        wl_lock(V.cast_lock);
        for (pp = &V.cast_rips; (p = *pp) != NULL; pp = &p->next)
            if (cip_eq(rip, &p->rip))
                break;
        if (!p)
            *pp = q;
        wl_unlock(V.cast_lock);

        if (!p)
            return;
        cip_2str(rip, rbuf, sizeof(rbuf));
        sdf_logi(70076, "attempting to add duplicate contact IP: %s", rbuf);
        close(q->fd);
    }
    m_free(q);
}


/*
 * Return information about a particular node.
 */
int
msg_getnode(msg_node_t *mnode, int nno)
{
    node_t *node;

    node = node_getbynno(nno);
    if (!node)
        return 0;

    mnode->nno  = nno;
    mnode->port = node->port;
    mnode->name = node->name;
    return 1;
}


/*
 * Send a message.
 */
void
msg_send(msg_send_t *send)
{
    node_t *node;
    int nno = send->nno;

    /* Debugging */
    if (t_on(SEND)) {
        xstr_t xstr;
        xsinit(&xstr);
        xsprint(&xstr, "=> msg_send()");
        show_send(&xstr, send);
        t_send(0, "%s", (char *)xstr.p);
        xsfree(&xstr);
    }

    if (send->stag < 0 || send->dtag < 0)
        return post_sent(send, "negative tags not allowed");

    /* Loopback */
    if (nno == MYNNO) {
        send_tome(send);
        return;
    }

    node = node_getbynno(nno);
    if (!node)
        return post_sent(send, "bad node number");
    send_out(node, send, 0);
}


/*
 * Send a message externally.
 */
static void
send_out(node_t *node, msg_send_t *send, int fast)
{
    conn_t *conn = conn_pick(node);

    if (conn && conn_sqput(conn, send, fast))
        return;

    if (node->live)
        node_send_put(node, send);
    else
        post_sent(send, "node down");
}


/*
 * Finish draining all queues.
 */
void 
msg_drain(void) 
{    
}


/*
 * Initiate a TCP connection.
 */
static int
tcp_connect(path_t *path)
{
    int fd;
    int len;
    hellobuf_t hbuf;
    node_t *node = path->node;
    sa_in_t laddr ={
        .sin_family = AF_INET,
    };
    sa_in_t raddr ={
        .sin_family = AF_INET,
        .sin_port   = htons(node->port),
    };

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return errz_close(fd, "socket failed");
    setnonblocking(fd);

    if (!cip_isbcast(&path->lip)) {
        laddr.sin_addr.s_addr = cip_2v4a(&path->lip);
        if (bind(fd, (sa_t *)&laddr, sizeof(laddr)) != 0)
            return errz_close(fd, "bind failed");
    }

    raddr.sin_addr.s_addr = cip_2v4a(&path->rip);
    if (connect_nb(fd, (sa_in_t *)&raddr, sizeof(raddr), TIME_CONNECT) < 0)
        return errz_close(fd, "connect failed");

    len = hello_fill(&hbuf, MSG_EJOIN, path);
    if (write(fd, &hbuf, len) != len)
        return errz_close(fd, "hello exchange failed");

    if (path->node->ver > V1_HELLO) {
        hellostd_t hstd;
        if (!hello_tcp_read(fd, &hstd))
            return errz_close(fd, "hello_tcp_read failed");
        cip_sa2(&hstd.sip, (sa_t *)&raddr);
        if (node_hello(&hstd) != path)
            return errz_close(fd, "bad hello exchange");
        path_update(path, &hstd);
    }
    return conn_new(path, fd) ? 1 : 0;
}


/*
 * Response to a TCP connect request.
 */
static void
tcp_accept(void *arg)
{
    int fd;
    path_t *path;
    node_t *node;
    sa_in_t raddr;
    socklen_t rlen;
    hellostd_t hstd;

    rlen = sizeof(raddr);
    fd = accept(V.tcp_lfd, (sa_t *)&raddr, &rlen);
    if (fd < 0)
        return errv_close(fd, "accept failed");
    setnonblocking(fd);

    if (!hello_tcp_read(fd, &hstd))
        return errv_close(fd, "hello_tcp_read failed");

    cip_sa2(&hstd.sip, (sa_t *)&raddr);
    path = node_hello(&hstd);
    path_update(path, &hstd);

    node = path->node;
    if (node->ver > V1_HELLO) {
        hellobuf_t hbuf;
        int len = hello_fill(&hbuf, MSG_EJOIN, path);
        if (write(fd, &hbuf, len) != len)
            return errv_close(fd, "hello exchange failed");
    }

    if (node->to_die)
        return errv_close(fd, "node sentenced to die");
    if (msg_ntime() < node->wake_time)
        return errv_close(fd, "node still asleep");
    conn_new(path, fd);
}


/*
 * Close a file descriptor, print out an error and return void.
 */
static void
errv_close(int fd, char *msg)
{
    sdf_loge_sys(20819, "%s", msg);
    if (fd >= 0)
        close(fd);
}


/*
 * Close a file descriptor, print out an error and return 0.
 */
static int
errz_close(int fd, char *msg)
{
    sdf_loge_sys(20819, "%s", msg);
    if (fd >= 0)
        close(fd);
    return 0;
}


/*
 * Close a file descriptor, print out an error and return NULL.
 */
static void *
errp_close(int fd, char *msg)
{
    sdf_loge(20819, "%s", msg);
    if (fd >= 0)
        close(fd);
    return NULL;
}


/*
 * Attempt to read data from a TCP socket.  We need to clear errno: see comment
 * to tcp_recv_err.
 */
static void
tcp_recv(conn_t *conn)
{
    int n;
    int len;
    int size;
    char *ptr;
    char *data;
    head_t head;
    head_t *hdr = &conn->headr;
    int     ver = conn->path->node->ver;
    int   hsize = head_size(ver);

    errno = 0;
    for (;;) {
        if (conn->state != CS_READY)
            return;
        if (!wl_try(conn->rolock))
            return;

        /* Read header */
        len = hsize - conn->hlen;
        if (len > 0) {
            ptr = ((char *)hdr) + conn->hlen;
            n = read(conn->sfd, ptr, len);
            if (n < 0)
                return tcp_recv_err(conn, 0, "read failed (header)");
            if (n == 0)
                return tcp_recv_err(conn, 0, "remote disconnect");
            conn->hlen += n;
            if (n != len)
                break;
            head_setn2h(hdr, ver);
            if (hdr->cver != ver)
                return tcp_recv_err(conn, 1, "bad header version");
        }

        /* Read data */
        size = hdr->size;
        if (!conn->data)
            conn->data = m_malloc(size, "msg_msg:data");
        len = size - conn->dlen;
        ptr = conn->data + conn->dlen;
        n = read(conn->sfd, ptr, len);
        if (n < 0)
            return tcp_recv_err(conn, 0, "read failed (data)");
        if (n == 0)
            return tcp_recv_err(conn, 0, "remote disconnect");
        conn->dlen += n;
        if (conn->dlen != size)
            break;

        /* Debugging */
        if (t_on(TCPR)) {
            char nbuf[NOSIZE];
            char rbuf[IPSIZE];
            path_t *path = conn->path;
            size_t size = conn->hlen + conn->dlen;

            node_no(path->node, nbuf, sizeof(nbuf));
            cip_2str(&path->rip, rbuf, sizeof(rbuf));
            t_tcpr(0, "recv rn=%s if=%s fd=%d rip=%s "
                      "len=%ld id=%ld sq=%ld aq=%ld",
                   nbuf, path->iface, conn->sfd, rbuf, size, hdr->mid,
                   hdr->seqn, ver == V1_HELLO ? 0 : hdr->aseqn);
        }

        /* Post message */
        head = *hdr;
        data = conn->data;
        conn->hlen = 0;
        conn->dlen = 0;
        conn->data = NULL;
        wl_unlock(conn->rolock);
        path_alive(conn->path, msg_ntime());
        post_recv(conn->path->node, &head, data);
    }
    wl_unlock(conn->rolock);
}


/*
 * Recover appropriately from a TCP receive error.  If real is set, we know we
 * have an error otherwise we check errno to ensure that this was not just
 * caused because we might block.  If drop is set, we drop the entire node
 * otherwise we drop the connection.
 */
static void
tcp_recv_err(conn_t *conn, int real, char *msg)
{
    if (real || (errno != EWOULDBLOCK && errno != EAGAIN)) {
        if (conn->data)
            m_free(conn->data);
        conn->hlen = 0;
        conn->dlen = 0;
        conn->data = NULL;
        conn_error(conn, 1, msg);
    }
    wl_unlock(conn->rolock);
}


/*
 * Send a message over TCP to the given connection.  This should be called
 * whenever there is something on the send queue and whenever the associated
 * file descriptor is available.
 */
static void
tcp_send(conn_t *conn)
{
    int i;
    int n;
    int len;
    int ver;
    int hsize;
    size_t sent;
    node_t *node;
    msg_send_t *send;
    iovec_t sge[MSG_NSGE+1];

    send = conn_sqret(conn);
    if (!send)
        return;
    if (conn->state != CS_READY)
        return;
    if (!wl_try(conn->wolock))
        return;

    /* FIXME */
    send = conn_sqret(conn);
    if (!send)
        goto ret;

    node = conn->path->node;
    ver = node->ver;
    hsize = head_size(ver);
    while (conn->state == CS_READY) {
        sent = send->sent;
        if (!sent)
            head_prep(&conn->headw, send, node, ver);

        sge[0].iov_base = &conn->headw;
        sge[0].iov_len  = hsize;
        memcpy(&sge[1], send->sge, send->nsge*sizeof(iovec_t));
        n = send->nsge + 1;
        for (i = 0; i < n; i++) {
            len = sge[i].iov_len;
            if (sent < len) {
                sge[i].iov_base += sent;
                sge[i].iov_len  -= sent;
                break;
            }
            sent -= len;
        }

        len = writev(conn->sfd, &sge[i], n-i);
        if (len < 0) {
            if (errno != EAGAIN)
                conn_error(conn, 1, "write error");
            break;
        }
        send->sent += len;
        for (; i < n; i++)
            len -= sge[i].iov_len;

        if (len)
            break;
        if (!conn_sqget(conn)) {
            fatal("entry disappeared from connection queue nr=%lx nw=%lx",
                  conn->fdlock->nr, conn->fdlock->nw);
        }

        if (t_on(TCPS)) {
            char nbuf[NOSIZE];
            char rbuf[IPSIZE];
            path_t *path = conn->path;

            node_no(path->node, nbuf, sizeof(nbuf));
            cip_2str(&path->rip, rbuf, sizeof(rbuf));
            t_tcps(0, "send rn=%s if=%s fd=%d rip=%s "
                      " len=%ld id=%ld sq=%ld aq=%ld",
                   nbuf, path->iface, conn->sfd, rbuf, send->sent,
                   send->sid, send->seqn, msg_retn2h(conn->headw.aseqn));
        }

        atomic_max(&node->rseqn_ackd, msg_retn2h(conn->headw.aseqn));
        node_sent_save(node, send);
        send = conn_sqret(conn);
        if (!send)
            break;
    }
ret:
    wl_unlock(conn->wolock);
}


/*
 * Determine the size of the message header based on a hello version.
 */
static int
head_size(int ver)
{
    return (ver == V1_HELLO) ? sizeof(head1_t) : sizeof(head2_t);
}


/*
 * Prepare a header from the send queue.
 */
static void
head_prep(head_t *head, msg_send_t *send, node_t *node, int ver)
{
    int i;
    int size = 0;

    for (i = 0; i < send->nsge; i++)
        size += send->sge[i].iov_len;

    clear(*head);
    head->cver = ver;
    head->sver = ver;
    head->stag = send->stag;
    head->dtag = send->dtag;
    head->mid  = send->sid;
    head->seqn = send->seqn;
    head->size = size;

    msg_seth2n(head->cver);
    msg_seth2n(head->sver);
    msg_seth2n(head->stag);
    msg_seth2n(head->dtag);
    msg_seth2n(head->mid);
    msg_seth2n(head->seqn);
    msg_seth2n(head->size);

    if (ver > V1_HELLO) {
        head->aseqn = node->rseqn_cont;
        msg_seth2n(head->aseqn);
    }
}


/*
 * Convert a header from network to host format.
 */
static void
head_setn2h(head_t *head, int ver)
{
    msg_setn2h(head->sver);
    msg_setn2h(head->stag);
    msg_setn2h(head->dtag);
    msg_setn2h(head->mid);
    msg_setn2h(head->seqn);
    msg_setn2h(head->size);

    if (ver == V1_HELLO)
        head->aseqn = 0;
    else
        msg_setn2h(head->aseqn);
}


/*
 * Send a message to myself.
 */
static void
send_tome(msg_send_t *send)
{
    int i;
    int len;
    char *p;
    char *q;
    msg_info_t *info;

    len = 0;
    for (i = 0; i < send->nsge; i++)
        len += send->sge[i].iov_len;

    q = p = m_malloc(len, "msg:recvbuf");
    for (i = 0; i < send->nsge; i++) {
        memcpy(q, send->sge[i].iov_base, send->sge[i].iov_len);
        q += send->sge[i].iov_len;
    }

    info = msg_ialloc();
    info->type = MSG_ERECV;
    info->nno  = send->nno;
    info->stag = send->stag;
    info->dtag = send->dtag;
    info->len  = len;
    info->mid  = send->sid;
    info->data = p;

    post_sent(send, NULL);
    msg_post(info);
}


/*
 * Return all entries from the node's send list that are waiting to be sent.
 */
static msg_send_t *
node_send_getlist(node_t *node)
{
    msg_send_t *send;

    wl_lock(node->lock_send);
    send = node->send_head;
    node->send_head = NULL;
    node->send_tail = NULL;
    wl_unlock(node->lock_send);
    return send;
}


/*
 * Post a send request onto the waiting send list.
 */
static void
node_send_put(node_t *node, msg_send_t *send)
{
    send->link = NULL;
    wl_lock(node->lock_send);
    if (!node->send_head)
        node->send_head = node->send_tail = send;
    else
        node->send_tail = node->send_tail->link = send;
    wl_unlock(node->lock_send);
}


/*
 * Post a list of send requests onto the waiting send list.
 */
static void
node_send_putlist(node_t *node, msg_send_t *send)
{
    msg_send_t *tail;

    if (!send)
        return;
    for (tail = send; tail->link; tail = tail->link)
        ;

    send->link = NULL;
    wl_lock(node->lock_send);
    if (!node->send_head) {
        node->send_head = send;
        node->send_tail = tail;
    } else {
        node->send_tail->link = send;
        node->send_tail = tail;
    }
    wl_unlock(node->lock_send);
}


/*
 * Post a sent acknowledge and free the send structure.
 */
static void
post_sent(msg_send_t *send, char *error)
{
    msg_info_t *info = msg_ialloc();

    info->type  = MSG_ESENT;
    info->nno   = send->nno;
    info->stag  = send->stag;
    info->dtag  = send->dtag;
    info->mid   = send->sid;
    info->error = error;
    msg_post(info);
    msg_sfree(send);
}


/*
 * Create a connection.  If this fails, the file descriptor is closed and an
 * error is printed.
 */
static conn_t *
conn_new(path_t *path, int fd)
{
    conn_t *p;
    conn_t **pp;
    path_t *best;
    int    nconn = 0;
    conn_t *conn = NULL;
    node_t *node = path->node;

    if (!path->prior)
        return errp_close(fd, "connection rejected: unwanted interface");

    for (pp = &node->conn; (p = *pp) != NULL; pp = &p->next) {
        if (p->state == CS_AVAIL) {
            if (!conn)
                conn = p;
        } else {
            if (p->path == path)
                nconn++;
        }
    }

    if (nconn >= path->nconn)
        return errp_close(fd, "connection rejected: too many connections");

    if (conn) {
        conn->path  = path;
        conn->sfd   = fd;
        barrier();
        conn->state = CS_READY;
        rwl_unlockw(conn->fdlock);
    } else {
        conn = malloc_z(sizeof(*conn), "conn_t");
        conn->path   = path;
        conn->sfd    = fd;
        conn->rolock = wl_init();
        conn->wolock = wl_init();
        conn->sqlock = wl_init();
        conn->fdlock = rwl_init();
        conn->state  = CS_READY;
        barrier();
        *pp = conn;
    }
    conn_msg(conn, "new");

    best = NULL;
    for (p = node->conn; p; p = p->next)
        if (p != conn && p->state == CS_READY)
            if (!best || p->path->prior < best->prior)
                best = p->path;
    if (best && best->prior > path->prior)
        sdf_logi(70057, "failing back to %s", path->iface);

    node->aconn++;
    if (node->ver == V1_HELLO) {
        if (conn == node->conn)
            node_live(node);
    } else {
        if (node->aconn == 1) {
            msg_send_t *send = node_send_getlist(node);
            if (send)
                conn_sqputlist(conn, send);
            node_live(node);
        }
    }
    return conn;
}


/*
 * Choose a connection to send a message on.
 */
static conn_t *
conn_pick(node_t *node)
{
    conn_t *p;
    conn_t *best = NULL;
    conn_t *next = node->conn_next;
    int state = 0;

    if (node->ver == V1_HELLO) {
        p = node->conn;
        return (p->state == CS_READY) ? p : NULL;
    }

    for (p = node->conn; p; p = p->next)
        if (p->state == CS_READY)
            pick_best(&best, &state, p, next);

    next = best ? best->next : NULL;
    if (!next)
        next = node->conn;
    node->conn_next = next;
    return best;
}


/*
 * See if the current connection is better than the one we have so far and if
 * so, return it.  The criteria used when choosing connections are: (1)
 * priority, (2) size of its send queue.
 *  conn  - The current connection under consideration.
 *  best  - The best connection we have found so far.
 *  next  - The connection we would like to start picking from next in order to
 *          round robin through the connection list.
 *  state -  0 => We are looking to find next in the list.
 *           1 => We have found next and are looking to find the next
 *                connection that is as good as best.
 *          -1 => We have found a connection at or after next that is as good
 *                as best.  Still, keep looking for a better one.
 */
static void
pick_best(conn_t **bestp, int *state, conn_t *conn, conn_t *next)
{
    conn_t *best = *bestp;

    if (*state == 0 && conn == next)
        *state = 1;
    if (!best)
        goto new;
    if (conn->path->prior < best->path->prior)
        goto new;
    if (conn->path->prior > best->path->prior)
        return;
    if (conn->ssize < best->ssize)
        goto new;
    if (conn->ssize > best->ssize)
        return;
    if (*state <= 0)
        return;
new:
    if (*state > 0)
        *state = -1;
    *bestp = conn;
}


/*
 * Signal that an error has occurred.
 */
static void
conn_error(conn_t *conn, int sys, char *msg)
{
    char nbuf[NOSIZE];
    char rbuf[IPSIZE];
    path_t *path = conn->path;
    node_t *node = path->node;

    node_no(node, nbuf, sizeof(nbuf));
    cip_2str(&path->rip, rbuf, sizeof(rbuf));
    if (sys) {
        sdf_logi_sys(70058, "conn err rn=%s if=%s fd=%d rip=%s: %s",
                     nbuf, path->iface, conn->sfd, rbuf, msg);
    } else {
        sdf_logi(70058, "conn err rn=%s if=%s fd=%d rip=%s: %s",
                 nbuf, path->iface, conn->sfd, rbuf, msg);
    }

    conn->state = CS_ERROR;
    barrier();
    V.node_attn = 1;
}


/*
 * Close a connection.  This is called with fdlock acquired in write mode.
 */
static void
conn_close(conn_t *conn)
{
    conn_t *p;
    path_t *best;
    path_t *path = conn->path;
    node_t *node = path->node;

    conn_msg(conn, "end");
    --node->aconn;
    conn_move(conn);
    close(conn->sfd);
    if (conn->data)
        m_free(conn->data);

    best = NULL;
    for (p = node->conn; p; p = p->next)
        if (p != conn && p->state == CS_READY)
            if (!best || p->path->prior < best->prior)
                best = p->path;
    if (best && best->prior > path->prior)
        sdf_logi(70059, "failing over to %s", best->iface);

    conn->path  = NULL;
    conn->sfd   = -1;
    conn->hlen  = 0;
    conn->dlen  = 0;
    conn->data  = NULL;
    conn->state = CS_AVAIL;
}


/*
 * Move any items that were intended to be sent on this connection to another
 * one.
 */
static void
conn_move(conn_t *oconn)
{
    node_t *node;
    conn_t *nconn;
    msg_send_t *send = conn_sqgetlist(oconn);

    if (!send)
        return;

    node = oconn->path->node;
    nconn = conn_pick(node);
    if (nconn)
        conn_sqputlist(nconn, send);
    else if (node->live)
        node_send_putlist(node, send);
    else
        send_list_fail(send);
}


/*
 * Issue a node down status to all messages on a list that were waiting to be
 * sent.
 */
static void
send_list_fail(msg_send_t *send)
{
    msg_send_t *link;

    for (; send; send = link) {
        link = send->link;
        post_sent(send, "node down");
    }
}


/*
 * Print out a connection message.
 */
static void
conn_msg(conn_t *conn, char *msg)
{
    char nbuf[NOSIZE];
    char rbuf[IPSIZE];
    path_t *path = conn->path;

    node_no(path->node, nbuf, sizeof(nbuf));
    cip_2str(&path->rip, rbuf, sizeof(rbuf));
    sdf_logi(70071, "conn %s rn=%s if=%s fd=%d rip=%s",
             msg, nbuf, path->iface, conn->sfd, rbuf);
}


/*
 * Return the first entry from the connection's send queue.
 */
static msg_send_t *
conn_sqret(conn_t *conn)
{
    return conn->shead;
}


/*
 * Get and remove the first entry from the connection's send queue.
 */
static msg_send_t *
conn_sqget(conn_t *conn)
{
    msg_send_t *send;

    wl_lock(conn->sqlock);
    send = conn->shead;
    if (send) {
        conn->ssize--;
        conn->shead = send->link;
        if (send == conn->stail)
            conn->stail = NULL;
    }
    wl_unlock(conn->sqlock);
    return send;
}


/*
 * Return all entries from the connection's send queue and clear it.
 */
static msg_send_t *
conn_sqgetlist(conn_t *conn)
{
    msg_send_t *send;

    wl_lock(conn->sqlock);
    conn->ssize = 0;
    send = conn->shead;
    conn->shead = NULL;
    conn->stail = NULL;
    wl_unlock(conn->sqlock);
    return send;
}


/*
 * Put an entry on the connection's send queue and start the send process.  If
 * the connection is in error, we return 0, otherwise 1.
 */
static int
conn_sqput(conn_t *conn, msg_send_t *send, int fast)
{
    if (conn->state != CS_READY)
        return 0;

    wl_lock(conn->sqlock);
    if (conn->state != CS_READY) {
        wl_unlock(conn->sqlock);
        return 0;
    }

    send->link = NULL;
    send->sent = 0;
    if (!(send->flags&MS_USESEQ))
        send->seqn = node_sent_seqn(conn->path->node);

    conn->ssize++;
    if (conn->shead)
        conn->stail = conn->stail->link = send;
    else
        conn->shead = conn->stail = send;
    wl_unlock(conn->sqlock);
    if (!fast || !V.init.mustlive)
        tcp_send(conn);
    return 1;
}


/*
 * Return the state of a connection.
 */
static char *
conn_state_name(conn_t *conn)
{
    conn_state_t state = conn->state;

    if      (state == CS_AVAIL) return "AVAIL";
    else if (state == CS_READY) return "READY";
    else if (state == CS_ERROR) return "ERROR";
    else                        return "?";
}


/*
 * Put an entry on the connection's send queue.  We ignore the ready and slow
 * flag since this is only called from the main thread to deal with errors and
 * even if they get set while we are placing items on this queue, those items
 * will not be stuck and will eventually be taken off.
 */
static void
conn_sqputlist(conn_t *conn, msg_send_t *send)
{
    int n;
    msg_send_t *tail;

    if (!send)
        return;
    for (tail = send, n=1; tail->link; tail = tail->link, n++)
        ;

    wl_lock(conn->sqlock);
/* FIXME */
    send->sent = 0;
/* FIXME */
    conn->ssize += n;
    if (conn->shead) {
        conn->stail->link = send;
        conn->stail = tail;
    } else {
        conn->shead = send;
        conn->stail = tail;
    }
    wl_unlock(conn->sqlock);
    tcp_send(conn);
}


/*
 * Post a message we have received.
 */
static void
post_recv(node_t *node, head_t *head, char *data)
{
    msg_info_t *info = msg_ialloc();

    info->type = MSG_ERECV;
    info->nno  = node->nno;
    info->stag = head->stag;
    info->dtag = head->dtag;
    info->len  = head->size;
    info->mid  = head->mid;
    info->data = data;
    info->seqn = head->seqn;

    if (node->ver == V1_HELLO) {
        msg_post(info);
        return;
    }

    node_set_aseqn(node, head->aseqn);
    if (info->dtag == (msg_tag_t)META_TAG)
        meta_recv(node, info, head->aseqn);
    else
        post_seqn(node, info);
}


/*
 * Post a received message in the correct sequence.
 */
static void
post_seqn(node_t *node, msg_info_t *info)
{
    int k;
    atom_t nseqn;
    msg_info_t *p;
    atom_t      iseqn = info->seqn;
    msg_info_t **hold = node->rinfo_hold;

    post_find(info);
    if (!iseqn) {
        post_done(info);
        return;
    }

    wl_lock(node->lock_recv);
    if (iseqn > node->rseqn_high)
        node->rseqn_high = iseqn;

    nseqn = node->rseqn_cont + 1;
    if (iseqn < nseqn) {
        msg_ifree(info);
    } else if (iseqn == nseqn) {
        post_done(info);
        for (k = ++nseqn - node->rinfo_soff; k < node->rinfo_size; k++) {
            p = hold[k];
            if (!p)
                break;
            post_done(p);
            hold[k] = NULL;
        }
        node->rseqn_cont = node->rinfo_soff + k-1;
        if (k == node->rinfo_size)
            post_seqn_fill(node);
    } else if (iseqn <= nseqn + node->rinfo_size) {
        k = iseqn - node->rinfo_soff;
        if (k >= node->rinfo_size)
            k -= node->rinfo_size;
        if (hold[k])
            msg_ifree(info);
        else
            hold[k] = info;
    } else {
        info->link = NULL;
        if (!node->rinfo_head)
            node->rinfo_head = node->rinfo_tail = info;
        else
            node->rinfo_tail = node->rinfo_tail->link = info;
    }
    wl_unlock(node->lock_recv);
}


/*
 * Refill the info hold area in a node from the linked list and continue to
 * post any messsages that we can.
 */
static void
post_seqn_fill(node_t *node)
{
    int k;
    int             s = node->rinfo_size;
    msg_info_t **hold = node->rinfo_hold;

    do {
        int n;
        atom_t lseqn;
        atom_t hseqn;
        msg_info_t *p;
        msg_info_t **pp;
        msg_info_t *link;
        msg_info_t *tail = NULL;

        n = 0;
        for (k = 0; k < s; k++)
            if (!hold[k])
                n++;

        node->rinfo_soff += s;
        lseqn = node->rinfo_soff;
        hseqn = lseqn + node->rinfo_size - 1;
        pp = &node->rinfo_head;
        for (p = *pp; p; p = link) {
            atom_t seqn = p->seqn;

            link = p->link;
            if (lseqn <= seqn && seqn <= hseqn) {
                k = seqn - lseqn;
                if (hold[k])
                    msg_ifree(p);
                else {
                    hold[k] = p;
                    if (!--n)
                        break;
                }
            } else {
                *pp = p;
                pp = &p->link;
                tail = p;
            }
        }

        *pp = p;
        if (!p)
            node->rinfo_tail = tail;

        for (k = 0; k < s; k++) {
            p = hold[k];
            if (!p)
                break;
            hold[k] = NULL;
            post_done(p);
        }
    } while (k == s);
    node->rseqn_cont = k-1 + node->rinfo_soff;
}


/*
 * Fail all messages that we have received but not yet delivered.
 */
static void
node_rinfo_fail(node_t *node)
{
    int i;
    msg_info_t *info;
    msg_info_t *list;
    int             n = node->rinfo_size;
    msg_info_t **hold = node->rinfo_hold;

    wl_lock(node->lock_recv);
    list = node->rinfo_head;
    node->rinfo_head = NULL;
    node->rinfo_tail = NULL;

    for (i = 0; i < n; i++) {
        info = hold[i];
        if (!info)
            continue;
        hold[i] = NULL;
        info->link = list;
        list = info;
    }
    wl_unlock(node->lock_recv);

    while ((info = list) != NULL) {
        list = info->link;
        msg_ifree(info);
    }
}


/*
 * Post an info event.
 */
static void
msg_post(msg_info_t *info)
{
    post_find(info);
    post_done(info);
}


/*
 * Post an info event into the test phase.
 */
static void
post_find(msg_info_t *info)
{
    if (!cb_callone(&V.post_call, info))
        info->func = post_tail;
}


/*
 * Post an info event to the finish phase.
 */
static void
post_done(msg_info_t *info)
{
    if (t_on(POST)) {
        xstr_t xstr;
        xsinit(&xstr);
        xsprint(&xstr, "msg_post() =>");
        show_info(&xstr, info);
        t_post(0, "%s", (char *)xstr.p);
        xsfree(&xstr);
    }

    (*info->func)(info);
}


/*
 * Post an info event onto the tail of the queue.
 */
static void
post_tail(msg_info_t *info)
{
    info->link = NULL;
    wl_lock(V.info_lock_post);
    if (!V.info_head)
        V.info_head = V.info_tail = info;
    else
        V.info_tail = V.info_tail->link = info;
    wl_unlock(V.info_lock_post);
}


/*
 * Shift the first event off the head of the queue.  Since only one thread is
 * calling this routine, we can minimize locking.
 */
static msg_info_t *
info_shift(void)
{
    msg_info_t* info = V.info_head;

    if (!info)
        return NULL;

    if (info != V.info_tail) {
        V.info_head = info->link;
        return info;
    }

    wl_lock(V.info_lock_post);
    if (info == V.info_tail)
        V.info_tail = NULL;
    V.info_head = info->link;
    wl_unlock(V.info_lock_post);
    return info;
}


/*
 * Convert a time period into an endtime suitable for passing to msg_poll.
 */
ntime_t
msg_endtime(ntime_t ntime)
{
    if (!ntime)
        return -1;
    return msg_ntime() + ntime;
}


/*
 * Return the current time in nanoseconds.  Also, if we determine that the
 * time has been changed, give other nodes the benefit of the doubt in terms of
 * liveness.
 */
ntime_t
msg_ntime(void)
{
    struct timespec t;

    clock_gettime(CLOCK_REALTIME, &t);
    return t.tv_sec * NANO + t.tv_nsec;
}


/*
 * Allocate a msg_info_t structure.
 */
msg_info_t *
msg_ialloc(void)
{
    msg_info_t *p = NULL;

    if (V.info_free) {
        wl_lock(V.info_lock_free);
        p = V.info_free;
        if (p)
            V.info_free = p->link;
        wl_unlock(V.info_lock_free);
    }

    if (!p)
        p = (msg_info_t *) m_malloc(sizeof(*p), "msg_info_t");
    memset(p, 0, sizeof(msg_info_t));
    atomic_inc(V.info_used);
    return p;
}


/*
 * Free a msg_info_t structure.
 */
void
msg_ifree(msg_info_t *info)
{
    if (info->data)
        m_free(info->data);

    wl_lock(V.info_lock_free);
    info->link = V.info_free;
    V.info_free = info;
    wl_unlock(V.info_lock_free);
    atomic_dec(V.info_used);
}


/*
 * Return the data portion of an info structure so the user can free it as
 * leisure.
 */
char *
msg_idata(msg_info_t *info)
{
    char *data = info->data;

    info->data = NULL;
    return data;
}


/*
 * Allocate a msg_send_t structure
 */
msg_send_t *
msg_salloc(void)
{
    msg_send_t *p = NULL;

    if (V.send_free) {
        wl_lock(V.send_lock);
        p = V.send_free;
        if (p)
            V.send_free = p->link;
        wl_unlock(V.send_lock);
    }

    if (!p)
        p = (msg_send_t *) m_malloc(sizeof(*p), "msg_send_t");
    memset(p, 0, sizeof(msg_send_t));
    atomic_inc(V.send_used);
    return p;
}


/*
 * Free a msg_send_t structure
 */
void
msg_sfree(msg_send_t *send)
{
    if (send->func)
        (send->func)(send);
    else if (send->data)
        m_free(send->data);

    wl_lock(V.send_lock);
    send->link = V.send_free;
    V.send_free = send;
    wl_unlock(V.send_lock);
    atomic_dec(V.send_used);
}


/*
 * Show a message.
 */
static void
show_info(xstr_t *xstr, msg_info_t *info)
{
    int type = info->type;

    xsprint(xstr, " %s", type_msg(type));
    show_node(xstr, info->nno);
    if (type == MSG_EIADD)
        xsprint(xstr, " %s", info->data);
    else if (type == MSG_ERECV || type == MSG_ESENT) {
        xsprint(xstr, " len=%d", info->len);
        if (info->stag)
            xsprint(xstr, " stag=%d", info->stag);
        if (info->dtag)
            xsprint(xstr, " dtag=%d", info->dtag);
        if (info->mid)
            xsprint(xstr, " mid=%llu", info->mid);
    }
}


/*
 * Show a message we are sending.
 */
static void
show_send(xstr_t *xstr, msg_send_t *send)
{
    int i;
    uint64_t len = 0;

    show_node(xstr, send->nno);
    for (i = 0; i < send->nsge; i++)
        len += send->sge[i].iov_len;
    xsprint(xstr, " len=%ld", len);

    if (send->stag)
        xsprint(xstr, " stag=%d", send->stag);
    if (send->dtag)
        xsprint(xstr, " dtag=%d", send->dtag);
    if (send->sid)
        xsprint(xstr, " mid=%llu", send->sid);
    if (send->nsge != 1)
        xsprint(xstr, " nsge=%d", send->nsge);
}


/*
 * Show a message.
 */
static void
show_node(xstr_t *xstr, int nno)
{
    int n = msg_map_recv(nno);

    if (n < 0)
        xsprint(xstr, " node=m%d", nno);
    else
        xsprint(xstr, " node=n%d", n);
}


/*
 * Return the type of a message.
 */
static char *
type_msg(int type)
{
    if      (type == MSG_ENONE) return "ENONE";
    else if (type == MSG_EJOIN) return "EJOIN";
    else if (type == MSG_EDROP) return "EDROP";
    else if (type == MSG_EIADD) return "EIADD";
    else if (type == MSG_EIDEL) return "EIDEL";
    else if (type == MSG_ERECV) return "ERECV";
    else if (type == MSG_ESENT) return "ESENT";
    else                        return "?";
}


/*
 * Compare two combined IP addresses.
 */
static int
cip_cmp(cip_t *cip1, cip_t *cip2)
{
    if (cip1->v4_d < cip2->v4_d)
        return -1;
    if (cip1->v4_d > cip2->v4_d)
        return 1;
    return 0;
}


/*
 * See if two combined IP addresses are equal.
 */
static int
cip_eq(cip_t *cip1, cip_t *cip2)
{
    return cip1->v4_d == cip2->v4_d;
}


/*
 * See if a combined IP addresses is null.
 */
static int
cip_isnull(cip_t *cip)
{
    return cip->v4_d == 0;
}


/*
 * Convert a combined IP address into an IPv4 address.
 */
static int
cip_2v4a(cip_t *cip)
{
    return cip->v4_d;
}


/*
 * Convert a sockaddr to a combined IP.
 */
static void
cip_sa2(cip_t *cip, sa_t *sa)
{
    clear(*cip);
    if (sa->sa_family == AF_INET)
        cip->v4_d = ((sa_in_t *)sa)->sin_addr.s_addr;
    else
        fatal("cip_sa2 can only convert v4 IPs");
}


/*
 * Convert a combined address to a string.
 */
static char *
cip_2str(cip_t *cip, char *buf, int len)
{
    uint8_t *p = cip->v4;

    snprintf(buf, len, "%d.%d.%d.%d", p[0], p[1], p[2], p[3]);
    return buf;
}


/*
 * Convert a string to a combined IP.
 */
static int
cip_str2(cip_t *cip, char *str)
{
    int i;
    int n;
    char *q;
    char *p = str;

    for (i = 0; i < 4; i++) {
        if (i && *p++ != '.')
            return 0;
        n = strtol(p, &q, 10);
        if (p == q)
            return 0;
        cip->v4[i] = n;
        p = q;
    }

    if (*p != '\0')
        return 0;
    return 1;
}


/*
 * Determine if a cip_t is a broadcast address.
 */
static int
cip_isbcast(cip_t *cip)
{
    return cip->v4[3] == 255;
}


/*
 * Convert a combined IP from host to network format.
 */
static void
cip_seth2n(cip_t *cip)
{
}


/*
 * Convert an combined IP from network to host format.
 */
static void
cip_setn2h(cip_t *cip)
{
}


/*
 * Return an integer in native format converted from little endian.
 */
int64_t
msg_getintn(void *ptr, int len)
{
    uint64_t l = 0;
    uint8_t *p = ptr + len;

    while (len--)
        l = (l << 8) | (*--p & 0xFF);
    return l;
}


/*
 * Convert an integer from native format into little endian.
 */
void
msg_setintn(void *ptr, int len, uint64_t val)
{
    uint8_t *p = ptr;

    while (len--) {
        *p++ = val;
        val >>= 8;
    }
}


/*
 * Set the processor affinity for messaging task n.
 */
void
msg_affinity(int n)
{
    int i;
    int u;
    int found;
    cpu_set_t cpus;
    uint_t   ntunique = V.init.ntunique;
    uint64_t affinity = V.init.affinity;

    if (!affinity) {
        if (n >= ntunique)
            return;
        affinity = -1;
    }

    u = 0;
    found = 0;
    CPU_ZERO(&cpus);
    for (i = 0; affinity; i++, affinity >>= 1) {
        if (!(affinity&1))
            continue;
        if ((n < ntunique) ? (n == u) : (u >= ntunique)) {
            found = 1;
            CPU_SET(i, &cpus);
        }
        u++;
    }

    if (!found)
        fatal("task %d has no affinity", n);
    if (sched_setaffinity(0, sizeof(cpus), &cpus) < 0)
        fatal_sys("sched_setaffinity failed");
}


/*
 * Set a file descriptor to be non-blocking.
 */
static void
setnonblocking(int fd)
{
    int flags;

    flags = fcntl(fd, F_GETFL);
    if (flags < 0)
        fatal("fcntl F_GETFL failed");

    flags |= O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) < 0)
        fatal("fcntl F_SETFL failed");
}


/*
 * Turn on a socket option.
 */
static void
setsockoptone(int fd, int option)
{
    int one = 1;
    int s = setsockopt(fd, SOL_SOCKET, option, &one, sizeof(int));

    if (s < 0)
        fatal("failed to set option %d on socket %d to 1", option, fd);
}


/*
 * Get our local interfaces.
 */
static void
lface_init(void)
{
    int n;
    int fd;
    ifconf_t ifc;
    ifreq_t *iptr;
    lface_t *lbuf;
    lface_t *lptr;
    ifreq_t ifreq;
    char lastname[IFNAMSIZ];

    if (V.lface_i)
        return;
    V.lface_i = 1;

    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
        fatal("socket failed");
    siocgifconf(fd, &ifc);

    n = ifc.ifc_len / sizeof(ifreq_t);
    lbuf = m_malloc(n * sizeof(lface_t), "N*lface_t");
    memset(lbuf, 0, n * sizeof(lface_t));
    memset(lastname, 0, sizeof(lastname));
    lptr = lbuf;
    iptr = (ifreq_t *) ifc.ifc_buf;

    for (iptr = (ifreq_t *)ifc.ifc_buf; n--; iptr++) {
        if (iptr->ifr_addr.sa_family != AF_INET)
            continue;
        if (strncmp(lastname, iptr->ifr_name, IFNAMSIZ) == 0)
            continue;
        memcpy(lastname, iptr->ifr_name, IFNAMSIZ);

        ifreq = *iptr;
        if (ioctl(fd, SIOCGIFFLAGS, &ifreq) < 0)
            fatal("ioctl SIOCGIFFLAGS failed");
        if (!(ifreq.ifr_flags & IFF_UP))
            continue;

        /* Name */
        strncopynull(lptr->name, sizeof(lptr->name),
                iptr->ifr_name, sizeof(iptr->ifr_name));

        /* Local address */
        cip_sa2(&lptr->local, &iptr->ifr_addr);

        /* Netmask */
        if (ioctl(fd, SIOCGIFNETMASK, &ifreq) < 0)
            fatal("ioctl SIOCGIFNETMASK failed");
        cip_sa2(&lptr->nmask, &ifreq.ifr_netmask);

        /* Broadcast address */
        if (iptr->ifr_flags & IFF_BROADCAST) {
            if (ioctl(fd, SIOCGIFBRDADDR, &ifreq) < 0)
                fatal("ioctl SIOCGIFBRDADDR failed");
            cip_sa2(&lptr->bcast, &ifreq.ifr_broadaddr);
        }

        /* Hardware address */
        if (ioctl(fd, SIOCGIFHWADDR, &ifreq) < 0)
            fatal("ioctl SIOCGIFHWADDR failed");
        memcpy(lptr->haddr, ifreq.ifr_hwaddr.sa_data, IFHWADDRLEN);
        lptr++;
    }
    plat_free(ifc.ifc_buf);
    close(fd);

    n = lptr - lbuf;
    lbuf = m_realloc(lbuf, (lptr-lbuf) * sizeof(lface_t), "N*lface_t");
    V.lface_p = lbuf;
    V.lface_n = n;
}


/*
 * Get a list of interfaces as returned from an ioctl of SIOCGIFCONF.  This
 * ought to be easier.  The magic constant for n is just an initial guess.  It
 * could be most anything.
 */
static void
siocgifconf(int fd, ifconf_t *ifc)
{
    int n = 8;

    for (;;) {
        int len = n * sizeof(ifreq_t);
        ifc->ifc_len = len;
        ifc->ifc_buf = m_malloc(len, "N*ifreq_t");
        if (ioctl(fd, SIOCGIFCONF, ifc) < 0)
            fatal("ioctl(SIOCGIFCONF) failed");
        if (ifc->ifc_len < len)
            break;
        n *= 2;
        plat_free(ifc->ifc_buf);
    }
    ifc->ifc_buf = m_realloc(ifc->ifc_buf, ifc->ifc_len, "msg_msg:ifc_buf");
}


/*
 * A blocking socket connect using non-blocking sockets.
 */
static int
connect_nb(int fd, const sa_in_t *saddr, socklen_t salen, int usecs)
{
    int n;
    int err;
    socklen_t len;

    n = connect(fd, (sa_t *)saddr, salen);
    if (n == 0)
        return 0;
    if (n < 0 && errno != EINPROGRESS)
        return -1;

    if (ready_nb(fd, POLLIN|POLLOUT, usecs) < 0)
        return -1;

    len = sizeof(err);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0)
        return -1;
    return 0;
}


/*
 * Blocking read using non-blocking file descriptors
 */
static int
read_nb(int fd, void *buf, size_t count, int usecs)
{
    int n;

    if (ready_nb(fd, POLLIN, usecs) < 0)
        return -1;

    n = read(fd, buf, count);
    if (n < 0 && errno != EWOULDBLOCK && errno != EAGAIN)
        return -1;
    return n;
}


/*
 * Ensure that a socket is ready.
 */
static int
ready_nb(int fd, int events, int usecs)
{
    int n;
    struct pollfd pollfd ={
        .fd = fd,
        .events = events
    };

    n = poll(&pollfd, 1, usecs/1000);
    if (n == 0 || !(pollfd.revents & events)) {
        errno = ETIMEDOUT;
        return -1;
    }
    return 0;
}


/*
 * Wake up msg_work from select.
 */
void
msg_wake(void)
{
    if (write(V.wake_main[1], "", 1)) {}
}


/*
 * Determine if a hardware address is null.
 */
static int
hwaddr_isnull(uint8_t h[IFHWADDRLEN])
{
    return !(h[0] || h[1] || h[2] || h[3] || h[4] || h[5]);
}


/*
 * Update the atomic atom to the value new only if it is greater.  If the
 * update was successful, return 1, otherwise 0.  Since it is always set to an
 * increasing value, the loop below is guaranteed to terminate.
 */
static int
atomic_max(atom_t *atom, atom_t new)
{
    for (;;) {
        atom_t old = *atom;
        if (new <= old)
            return 0;
        if (atomic_cmp_swap(*atom, old, new) == old)
            return 1;
    }
}


/*
 * The function is never called.  The functions listed here are not currently
 * being used.  Only here to avoid compile errors.
 */
void
sdf_tcp_unused(void)
{
    node_show();
    node_talk();
    send_resend(0);
    hwaddr_isnull(0);
    fdcall_clr(0, 0);
}


/* ------------------------------------------------------------------------- */

/* Deprecated */


#define NAMESIZE 100
#define ADDRSIZE 256


typedef int msg_nno_t;


/*
 * Old interface information.
 */
typedef struct xface {
    struct xface *next;                 /* Next */
    char         *name;                 /* Name */
    char         *laddr;                /* Local address */
    char         *raddr;                /* Remote address */
    int           iscon;                /* Interface is connected */
} xface_t;


/*
 * Old node information
 *
 *  iface  - List of interfaces.
 *  mtime  - Maximum time between broadcasts since last statistic interval.
 *  mmtime - Maximum time between broadcasts.
 *  name   - A unique name for this node; consists of the hostname followed by
 *           the IP address of one of the interfaces and the port in the
 *           following format: name<192.168.1.2:13521>.
 *  next   - Next entry in list.
 *  nno    - Node number.  Used to reference this node.
 *  port   - TCP port that node listens on.
 */
typedef struct xnode {
    struct xnode *next;
    char         *name;
    msg_nno_t     nno;
    msg_port_t    port;
    ntime_t       mtime;
    ntime_t       mmtime;
    xface_t      *xface;
    uint16_t      conn;               /* number of connected interface */
    uint16_t      alive;              /* number of interface */
    ntime_t       stamp;              /* Time stamp in ns */
} xnode_t;


/*
 * Message variables.
 */
typedef struct msg_var2 {
    int             size_alive;         /* number of alive/active nodes */
    int             size_drop;          /* number of dropped nodes */
    int             size_conn;          /* number of connected nodes */
    xnode_t    *self;
    xnode_t    *xnodes;
} msg_var2_t;


static void error(int sys, char *fmt, ...);
static void        msg_post(msg_info_t *info);
static msg_node_t *get_nodeinfo(msg_nno_t nno,
        int activity, int connectivity);
static msg_node_t *get_nodelist(int *no_nodes,
        int activity, int connectivity);
static void node_info_assembly(xnode_t * inode, msg_node_t * node,
        int connectivity);
static int copy_iface_to_conn(msg_conn_t **pconn, xnode_t *node,
        int connectivity);
static xnode_t *node_list_lookup(xnode_t *nhead, msg_nno_t nno);


/*
 * Static variables.
 */
static msg_var2_t V2;


/*
 * Comparing nodes
 * The idea is that it takes two nodes and returns some
 * ordering on it.  it will return either -1, 0 or 1.  Compare the node
 * names and if they are equal, compare the port numbers.  If both of
 * the nodes do not exist, return 0.  If one of the nodes does not exist,
 * it should compare less than a node that does exist.
 */
int
msg_cmpnode(msg_nno_t lhs, msg_nno_t rhs)
{
    xnode_t *lhs_node, *rhs_node;
    int ret;

    /* first we look up the nodes */
    lhs_node = node_list_lookup(V2.xnodes, lhs);
    rhs_node = node_list_lookup(V2.xnodes, rhs);
    if (lhs_node == NULL && rhs_node == NULL)
        return 0;
    else if (lhs_node == NULL)
        return -1;
    else if (rhs_node == NULL)
        return 1;

    /* both of node exist, compare name first */
    ret = strncmp(lhs_node->name, rhs_node->name, NAMESIZE);
    if (ret != 0)
        return ret;

    /* names are equal, compare port next */
    ret = lhs_node->port - rhs_node->port;
    if (ret < 0)
        ret = -1;
    else if (ret > 0)
        ret = 1;

    return ret;
}


/*
 * Return the node name that corresponds to the node number.  If one does not
 * exist, return NULL.
 */
char *
msg_nodename(msg_nno_t nno)
{
    xnode_t *iterator = V2.xnodes;
    char *ret;

    for (iterator = V2.xnodes; iterator != NULL; iterator = iterator->next) {
        if (iterator->nno == nno) {
            ret = (char *) m_malloc(NAMESIZE, "msg_msg:N*char");
            snprintf(ret, NAMESIZE - 1, "%s", iterator->name);
            return ret;
        }
    }
    return NULL;
}


/*
 * If nno is zero, return an array of msg_node_t structures corresponding to
 * the list of connected nodes.  If nno is non-zero, return information
 * corresponding to the single requested node.  In both cases, the list will
 * have an extra msg_node_t structure at the end which has nno set to 0.  If
 * no_nodes is set, the number of nodes is returned through it.
 */
msg_node_t *
msg_getnodes(msg_nno_t nno, int *no_nodes)
{
    if (nno == 0)
        return get_nodelist(no_nodes, 1, 1);

    if (no_nodes)
        *no_nodes = 1;
    return get_nodeinfo(nno, 1, 1);
}


/*
 * Free a structure that was returned by msg_getnodes.
 */
void
msg_freenodes(msg_node_t *nodes)
{
    int i;
    int no_nodes;
    int cnt_conn;
    msg_node_t *iterator = nodes;

    if (!nodes) return;
    no_nodes = 0;
    while (nodes[no_nodes].nno)
        no_nodes++;
    for (i = 0; i < no_nodes; i++, iterator++) {
        for (cnt_conn = 0; cnt_conn < iterator->nconn; cnt_conn++) {
            m_free(iterator->conns[cnt_conn].cface);
            m_free(iterator->conns[cnt_conn].raddr);
        }
        m_free(iterator->conns);
        m_free(iterator->name);
    }
    m_free(nodes);
}


/*
 * Given a node number, nno, return node information, if not
 * exist, return NULL.
 */
static msg_node_t *
get_nodeinfo(msg_nno_t nno, int activity, int connectivity)
{
    xnode_t *iterator = V2.xnodes;
    msg_node_t *node;

    /* connected node must be alive */
    if (connectivity > activity)
        error(0, "Parameter error, connected node must be alive");

    /* look for node in the list */
    node = (msg_node_t *) m_malloc(2*sizeof(msg_node_t), "msg_node_t");
    node[1].nno = 0;
    while (iterator) {
        if (iterator->nno == nno &&
                (iterator->alive >= activity) &&
                (iterator->conn >= connectivity)) {
            node_info_assembly(iterator, node, connectivity);
            return node;
        }
        iterator = iterator->next;
    }
    m_free(node);
    return NULL;
}


/*
 * Get node list with optional selection.
 * activity = 1 means alive nodes are required;
 * connection = 1 means connected nodes are required;
 * Number of nodes will be loaded to no_nodes.
 */
static msg_node_t *
get_nodelist(int *no_nodes, int activity, int connectivity)
{
    int n;
    xnode_t *node;
    msg_node_t *pnode;
    int count = 0;

    /* connected node must be alive */
    if (connectivity > activity)
        error(0, "Parameter error, connected node must be alive");

    if (connectivity)
        n = V2.size_conn;
    else if (activity)
        n = V2.size_alive;
    else
        n = V2.size_alive + V2.size_drop;

    if (no_nodes)
        *no_nodes = n;

    pnode = (msg_node_t *) m_malloc((n+1) * sizeof(msg_node_t), "msg_node_t");
    pnode[n].nno = 0;

    /* copy node info to msg_node_t */
    for (node = V2.xnodes; node != NULL; node = node->next)
        if (node->alive >= activity && node->conn >= connectivity)
            node_info_assembly(node, &pnode[count++], connectivity);

    return pnode;
}


/*
 * Copy interface information to msg_conn_t
 * return 0 if failed or number of interface
 */
static int
copy_iface_to_conn(msg_conn_t **pconn, xnode_t *node, int connectivity)
{
    int size;
    msg_conn_t *conn, *conniter;
    xface_t *iter;

    if (node->alive == 0) {
        error(0, "No address information for node %s", node->name);
        return 0;
    }
    size = connectivity ? node->conn : node->alive;
    conn = (msg_conn_t *) m_malloc(sizeof(msg_conn_t) * size, "msg_conn_t");
    iter = node->xface;
    conniter = conn;
    while (iter) {
        if (connectivity && !iter->iscon) {
            iter = iter->next;
            continue;
        }
        conniter->cface = (char *) m_malloc(NAMESIZE, "msg_msg:N*char");
        conniter->raddr = (char *) m_malloc(ADDRSIZE, "msg_msg:N*char");
        snprintf(conniter->cface, NAMESIZE, "%s", iter->name);
        snprintf(conniter->raddr, ADDRSIZE, "%s", iter->raddr);
        iter = iter->next;
        conniter++;
    }
    *pconn = conn;

    return size;
}


/*
 * Assemble node info for caller from xnode_t
 */
static void
node_info_assembly(xnode_t *inode, msg_node_t *node, int connectivity)
{
    int len;

    node->nno = inode->nno;
    node->port = inode->port;
    node->name = (char *) m_malloc(NAMESIZE, "msg_msg:N*char");
    len = strchrnul(inode->name, '<') - inode->name;
    if (len >= NAMESIZE)
        len = NAMESIZE - 1;
    memcpy(node->name, inode->name, len);
    node->name[len] = '\0';
    node->nconn = copy_iface_to_conn(&node->conns, inode, connectivity);
}


/*
 * Look up nodes according to nno
 */
static xnode_t *
node_list_lookup(xnode_t *nhead, msg_nno_t nno)
{
    xnode_t *niter, *nret;

    niter = nhead;
    nret = NULL;
    while (niter) {
        if (nno == niter->nno) {
            nret = niter;
            break;
        }
        niter = niter->next;
    }

    return nret;
}


/*
 * Print out a system error message.
 */
void
error(int sys, char *fmt, ...)
{
    xstr_t xstr;
    va_list alist;
    int err = errno;

    xsinit(&xstr);
    va_start(alist, fmt);
    xsvprint(&xstr, fmt, alist);
    va_end(alist);
    if (sys) {
        char buf[256];
        if (plat_strerror_r(err, buf, sizeof(buf)) >= 0)
            xsprint(&xstr, ": %s", buf);
        else
            xsprint(&xstr, ": %d", err);
    }
    sdf_loge(160069, "%p", xstr.p);
    xsfree(&xstr);
}
