/*
 * File:   sdf_msg_types.h
 * Author: Tom Riddle
 *
 * Created on February 25, 2008, 3:45 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_types.h,v 1.1 2008/05/22 09:47:59 drew Exp $
 */

#ifndef _SDF_MSG_TYPES_H
#define _SDF_MSG_TYPES_H


#include "sdf_msg.h"
#include "sdfappcommon/XList.h"
#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "fth/fthXMbox.h"
#include "platform/closure.h"
#include "applib/XMbox.h"
#include "sdfappcommon/XMbox.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * The default number of bins is static and initialized according to
 * SDF_msg_protocol below. You must set this in DEF_NUM_BINS
 * The default number of preposted receive buffers are statically
 * created and fixed. They can be increased by setting the size
 * of DEF_NUM_BUFFS below.
 */

#define DEF_NUM_BINS SDF_PROTOCOL_COUNT
#define MAX_NUM_BINS 20
#define DEF_NUM_BUFFS 100
#define DBNUM DEF_NUM_BUFFS
#define MBSZE 65536
#define CONBSZE 65536
#define MAXBUFFSZE 65536
#define SM_EAGRLMT 65536
#define MSG_DEBUG SDF_DEBUG
#define MSG_TAG_OFFSET 100
#define MAX_NUM_SCH_NODES 8
#define NUM_HBUCKETS 1000
#define MAX_QCOUNT 50
#define MPI_FAILURE 1
#define MSG_DFLT_KEY "deadbeeffeedcafe"
#define SDF_MSG_CLUSTERID 1    /* This our_clusterid should be obtained from configuration */
#define SDF_MSG_MAX_NODENAME 256
#define SDF_MSG_DFLT_SCHOONID 0x7fff
#define SDF_MSG_DFLT_MPIHOST "node unavailable"
#define SDF_MSG_DFLT_PROPHOST "unassigned"
#define SDF_MSG_NSTIMEOUT 500000000
#define SDF_MSG_SECTIMEOUT 2


typedef vnode_t bin_t;
typedef unsigned char cbuff;
typedef struct sdf_msg_bin_list bin_list;

typedef struct sdf_msg_xchng {
    cbuff **bin_buff;
    uint32_t ll_cnt;
    uint32_t xindx;
    uint32_t binnum;
    uint32_t xflags;
    uint64_t seqnum;
    struct sdf_queue_item *msg_q_item;      /* have to track this and release if present */
    struct sdf_msg_xchng **hd;
    struct sdf_msg_xchng **tl;
    struct sdf_msg_xchng *xackBuffQ;
} sdf_msg_xchng_t;

typedef struct msg_srtup {
    pthread_mutex_t msg_release;
    int msg_mpi_release;  /* global sync for MPI initialization */
    int msg_sys_release;  /* global sync for SYS mangmt thread init */
    int msg_run_release;  /* release the msg engine after the threads are ready to send */
    int msg_run_timetogo; /* flag to stop and exit msg engine send receive loop */
    int msg_timeout;      /* msg timeout can be set externally so it gets into the msg thread here */
} msg_srtup_t;

typedef struct msg_state {
    vnode_t myid;                    /* another place for your node id */
    uint32_t sdf_msg_runstat;        /* block sends or bypass unused bin checks */
    struct msg_srtup *msg_startup;   /* point to the startup state */
    struct bin_list *cstate;         /* point to the node_bins which tracks all */
    struct q_tracker *qstate[MAX_QCOUNT];    /* */
    uint32_t qtotal;                 /* total number of registered queues */
    seqnum_t respcntr;               /* sent with each response request msg, used for guid */
    seqnum_t sendstamp;              /* mpi total send stat */
    seqnum_t recvstamp;              /* mpi total recv stat */
    seqnum_t resp_n_flight;          /* number of response requests in flight */
    uint32_t gc_count;               /* tracker for the number of dynamic messages freed */
    uint32_t msg_delivered[MAX_NUM_BINS]; /* simple counter for bin msgs */
    uint32_t msg_been_enabled;       /* flag for msg thread itself  */
    struct msg_timeout *mtime;
} msg_state_t;

/* timeout message count identifiers */
typedef enum {
    PC_CNT = 0,
    PA_CNT,
    PN_CNT,
    C_CNT,
    A_CNT,
    N_CNT,
    MCNTS
} to_cnt_t;

typedef struct msg_timeout {
    uint32_t actflag;                /* flag set when timeouts are active */
    seqnum_t rseqnum;                /* every response req msg is incremented after the sync time */
    uint32_t tmout;                  /* base timeout setting for all msgs */
    int mcnts[MCNTS];
    long timemkr;                    /* current time marker in sec */
    long mstimemkr;                  /* current time marker in usec */
    long ptimemkr;                   /* store the previous timemkr here after interval update */
    long ntimemkr;                   /* store the previous timemkr here after interval update */
    long diffmkr;                    /* time difference */
} msg_timeout_t;

typedef struct q_tracker {
    struct sdf_queue_pair *q_pair_tkr;
    struct sdf_queue *q_in;
    struct sdf_queue *q_out;
    uint32_t qnum;
    uint32_t wnum;
    vnode_t src_node;       /* source node */
    vnode_t dest_node;      /* destination node */
    service_t src_srv;      /* source service */
    service_t dest_srv;     /* destination service */
    char src_srv_name[50];
    char dest_srv_name[50];
} q_tracker_t;

typedef struct msg_resptrkr {
    char mkey[MSG_KEYSZE];
    long msg_timeout;                /* timestamp in secs */
    long msg_mstimeout;              /* timestamp in msecs */
    fthMbox_t *respmbx;
    struct sdf_fth_mbx *ar_mbx;
    struct sdf_fth_mbx *ar_mbx_from_req;
    uint32_t msg_flags;              /* SDF_msg_flags */
    msgtime_t msg_basetimestamp;     /* monotonically increasing time */
    uint64_t fthid;                  /* posting thread */
    seqnum_t msg_seqnum;             /* sequence number of datagram */
    serviceid_t msg_src_service;     /* source service id */
    serviceid_t msg_dest_service;    /* dest service id aka protocol */
    vnode_t msg_src_vnode;           /* source virtual node id */
    vnode_t msg_dest_vnode;          /* destination virtual node id */
    msgtime_t msg_elaptm;            /* elapsed time from request to response received */
    int nxtmkr;
} msg_resptrkr_t;

/*
 * Note that the constant MAX_NUM_BINS must reflect the number of items in this
 * list.
 */
#define SDF_MSG_PROTOCOL_ITEMS()                                               \
    item(SDF_DEBUG)                                                            \
    item(SDF_SYSTEM)                                                           \
    item(SDF_CONSISTENCY)                                                      \
    item(SDF_MANAGEMENT)                                                       \
    item(SDF_MEMBERSHIP)                                                       \
    /** @brief Requests to flash */                                            \
    item(SDF_FLSH)                                                             \
    item(SDF_METADATA)                                                         \
    /** @brief Requests to replication service, typically home node code */    \
    item(SDF_REPLICATION)                                                      \
    /** @brief Internal requests to replication service from peers */          \
    item(SDF_REPLICATION_PEER)                                                 \
    /** @brief Internal requests for supernode metea-data implementatino */    \
    item(SDF_REPLICATION_PEER_META_SUPER)                                      \
    /** @brief Internal requests for Paxos meta-data implementatino */         \
    item(SDF_REPLICATION_PEER_META_CONSENSUS)                                  \
    item(SDF_SHMEM)                                                            \
    item(SDF_RESPONSES)                                                        \
    item(SDF_3RDPARTY)                                                         \
    item(SDF_FINALACK)                                                         \
    item(GOODBYE)                                                              \
    item(SDF_UNEXPECT)                                                         \
    /** @brief Internal message from SDF_MSG */                                \
    item(SDF_SDFMSG)                                                           \
    /** @brief Message which originates from test code */                      \
    item(SDF_TEST)                                                             \
    /* Used for syncing between nodes */                                       \
    item(SDF_SYNC)


typedef enum SDF_msg_protocol {
#define item(caps) caps,
    SDF_MSG_PROTOCOL_ITEMS()
    item(SDF_PROTOCOL_COUNT)
#undef item
} SDF_msg_protocol;

const char *SDF_msg_protocol_to_string(enum SDF_msg_protocol msg_protocol);

/**
 * @brief legal values for sdf_msg msg_type field
 *
 * At the sdf_msg layer, all messages have unique types so that synthetic
 * error resposnes can be generated.
 *
 * XXX: drew 2009-01-03 Should either merge the SDF_MSG error types into one
 * and provide a payload with specifics, or provide a macro
 * sdf_msg_is_error() which does the right thing so we don't have
 * maintenance problems as things are added.
 */
#define SDF_MSG_TYPE_ITEMS() \
    item(FLSH_REQUEST)                                                         \
    item(FLSH_RESPOND)                                                         \
    /** @brief Meta storage request */                                         \
    item(META_REQUEST)                                                         \
    item(META_RESPOND)                                                         \
    item(REPL_REQUEST)                                                         \
    item(LOCK_RESP)                                                            \
    item(REQ_FLUSH)                                                            \
    item(REQ_MISS)                                                             \
    item(RESP_ONE)                                                             \
    item(RESP_TWO)                                                             \
    item(MDAT_REQUEST)                                                         \
    item(MGMT_REQUEST)                                                         \
    item(HEARTBEAT_REQ)                                                        \
    item(SYS_REQUEST)                                                          \
    item(SYS_PP_REQ)                                                           \
    item(SYS_PP_ACK)                                                           \
    item(SYS_SHUTDOWN_SELF)                                                    \
    item(SYS_SHUTDOWN_ALL)                                                     \
    item(SYS_SHUTDOWN_ERR)                                                     \
    /** @brief Internally generated error message */                           \
    item(SDF_MSG_ERROR)

enum SDF_msg_type {
#define item(caps) caps,
    SDF_MSG_TYPE_ITEMS()
#undef item
};

const char *SDF_msg_type_to_string(enum SDF_msg_type msg_type);

/** @brief Payload for SDF_MSG_ERROR type */
typedef struct sdf_msg_error_payload {
    /** @brief Specific failure */
    SDF_status_t error;
} sdf_msg_error_payload_t;

#define SDF_MSG_SACK_HOW_ITEMS() \
    item(SACK_HOW_NONE)                                                        \
    item(SACK_HOW_FTH_MBOX_TIME)                                               \
    item(SACK_HOW_FTH_MBOX_MSG)                                                \
    item(SACK_HOW_CLOSURE_MSG_WRAPPER)

typedef enum SDF_msg_SACK_how {
#define item(caps) caps,
    SDF_MSG_SACK_HOW_ITEMS()
#undef item
} SDF_msg_SACK_how;

const char *SDF_msg_SACK_how_to_string(enum SDF_msg_SACK_how how);

typedef enum SDF_msg_SACK_rel {
    SACK_REL_NO = 0,
    SACK_REL_YES = 1
} SDF_msg_SACK_rel;

typedef enum SDF_msg_SACK_phase {
    SACK_PHASE_ACK = 1,
    SACK_PHASE_RESP
} SDF_msg_SACK_phase;

/* item(caps, val, ack, resp, rel, modern) */
#define SDF_SACK_ITEMS() \
    item(SACK_ONLY_FTH, = 1, SACK_HOW_FTH_MBOX_TIME, SACK_HOW_NONE,            \
         SACK_REL_YES, 0)                                                      \
    item(SACK_RESP_ONLY_FTH, /* */, SACK_HOW_NONE, SACK_HOW_FTH_MBOX_MSG,      \
         SACK_REL_YES, 0)                                                      \
    item(SACK_BOTH_FTH, /* */, SACK_HOW_FTH_MBOX_TIME, SACK_HOW_FTH_MBOX_MSG,  \
         SACK_REL_YES, 0)                                                      \
    item(SACK_NONE_FTH, /* */, SACK_HOW_NONE, SACK_HOW_NONE, SACK_REL_YES, 0)  \
    item(SACK_MODERN, /* */, SACK_HOW_NONE, SACK_HOW_NONE, SACK_REL_NO, 1)

typedef enum SDF_msg_SACK {
#define item(caps, val, ack, resp, rel, modern) caps val,
  SDF_SACK_ITEMS()
#undef item
} SDF_msg_SACK;


/* Helper functions to make sense of enumeration */
static __inline__ enum SDF_msg_SACK_how
sdf_msg_sack_resp(SDF_msg_SACK sack) {
    switch (sack) {
#define item(caps, val, ack, resp, rel, modern) \
    case caps: return (resp);
    SDF_SACK_ITEMS();
#undef item
    default:
        return (SACK_HOW_NONE);
    }
}

static __inline__ enum SDF_msg_SACK_how
sdf_msg_sack_ack(SDF_msg_SACK sack) {
    switch (sack) {
#define item(caps, val, ack, resp, rel, modern) \
    case caps: return (ack);
    SDF_SACK_ITEMS();
#undef item
    default:
        return (SACK_HOW_NONE);
    }
}

static __inline__ enum SDF_msg_SACK_rel
sdf_msg_sack_rel(SDF_msg_SACK sack) {
    switch (sack) {
#define item(caps, val, ack, resp, rel, modern) \
    case caps: return (rel);
    SDF_SACK_ITEMS();
#undef item
    default:
        return (SACK_REL_NO);
    }
}

/**
 * @brief Bit fielded flags for #sdf_msg flags field.
 */
typedef enum {
    /** @brief On send the original sdf_msg will be posted to akrpmbx->abox */
    SDF_MSG_FLAG_MBX_SACK_EXPECTED = 1 << 0,

    /** @brief A response is expected which will be posted to akrpmbx->rbox */
    SDF_MSG_FLAG_MBX_RESP_EXPECTED = 1 << 1,

    /** @brief All messages have this set for some reason */
    SDF_MSG_FLAG_QOS = 1 << 2,

    /** @brief Response is being delivered */
    SDF_MSG_FLAG_MBX_RESP_INCLUDED = 1 << 3,

    /** @brief Free buffer on send */
    SDF_MSG_FLAG_FREE_ON_SEND = 1 << 4,

    /** @brief Flag as static receive buffer */
    SDF_MSG_FLAG_STATIC_BUFF = 1 << 5,

    /** @brief Flag as dynamically created msg buffer */
    SDF_MSG_FLAG_ALLOC_BUFF = 1 << 6,

    /** @brief Flag set when mkey_int is used instead of mkey */
    SDF_MSG_FLAG_MKEY_INT = 1 << 7,

    /** @brief alive flag for the active node status */
    SDF_MSG_FLAG_VALID_NODE = 1 << 15

} SDF_msg_flags;


/**
 * @brief Bit fielded flags for msg startup and runtime flags the RTF field.
 */
typedef enum {
    /** @brief default is to enable the message management thread */
    SDF_MSG_RTF_ENBLE_MNGMT = 0,

    /** @brief run the messaging without the management thread but it is not advised */
    SDF_MSG_RTF_DISABLE_MNGMT = 1,

    /** @brief for flow control we can stop sends from a node */
    SDF_MSG_RTF_STOP_SENDS = 1 << 1,

    /** @brief for flow control you can stop receives at a node */
    SDF_MSG_RTF_STOP_RECV = 1 << 2,

    /** @brief flow control restart the sends */
    SDF_MSG_RTF_START_SENDS = 1 << 3,

    /** @brief flow control restart the receives  */
    SDF_MSG_RTF_START_RECV = 1 << 4,

    /** @brief single process mode without calling into MPI for the rank */
    SDF_MSG_NO_MPI_INIT = 16,

    /** @brief single or multi process mode while using MPI for the rank */
    SDF_MSG_MPI_INIT = 32,

    /** @brief single process mode with messaging but without MPI */
    SDF_MSG_SINGLE_NODE_MSG = 64,

    /** @brief single process mode without messaging */
    SDF_MSG_DISABLE_MSG = 128,

    /** @brief messaging request flag for startup */
    SDF_MSG_STARTUP_REQ = 256,

    /** @brief messaging thread flag for startup completion notification */
    SDF_MSG_STARTUP_DONE = 512,

    /** @brief messaging flag for NEW_MSG config and startup */
    SDF_MSG_NEW_MSG = 1024


} SDF_msg_surt_flags;

/**
 * @brief messaging thread delivers a received message to a posted fthMbox or queue
 *
 * @param sdf_msg <IN> basic msg structure, whether normal or timeout generated
 * @return 0 if successful
 */

int
sdf_do_receive_msg(struct sdf_msg *msg);

/**
 * @brief initialize the receiving bins as part of the messaging thread startup
 *
 * Called by sdf_msg_init func to start the msg engine and init the msg bins
 * bin_t is a uint32_t but used as a boolean DAWCIDN
 *
 * @param schooner node ID
 * @returns  0 for success
 */
bin_t
sdf_msg_init_bins(vnode_t thisnodeid);


/**
 * @brief free msg buffers, static or dynamically allocated
 *
 * Always called by thread receiving a message from the msg engine to
 * notify the msg engine the buffer can be freed. In the case of
 * a internode message the returned buffer is static and is reposted
 * to MPI_Recv, all other message buffers are dynamically allocated
 * so they will be logged in a LL and freed later
 *
 * DO NOT USE a direct sdf_mem_free of your message buffer, this approach
 * will leak q_info.
 *
 * @param sdf_msg <IN> basic msg structure, whether normal or timeout generated
 * @returns -- 0 for success
 */
int
sdf_msg_free_buff(sdf_msg_t *msg);

  /*
   * Used only by the msg engine thread to free buffers returned by the client thread.
   * basically a garbage collecting function
   */
int
sdf_msg_gcbuffs(void);


  /*
   * Called by sdfagent thread or process to initialize the msg engine. flags
   * are currently unused. The bin_t is a uint32_t and currently returns a 0
   * for success.
   */
bin_t
sdf_msg_init(vnode_t thisnodeid, int *thispnodeid, int msg_init_flags);


  /*
   * Called by sdfagent to start the message sends/receives, messages posted to
   * queues or mailboxes will not be delivered until this is called future
   * allowance for flags are here to allow different levels or options.
   * sdf_dispatch () will be registered as a callback when sdf_msg_stopmsg() is
   * envoked returns 0 if successful.
   */
int
sdf_msg_startmsg(uint32_t thisid, uint32_t flags, void *sdf_dispatch(void *));


  /*
   * Called by sdfagent to shutdown the messaging threads, stoplvl are flags to
   * allow different levels or options. stoplvl must be non zero to actually
   * stop.  returns 0 if successful
   */
int
sdf_msg_stopmsg(uint32_t thisid, uint32_t stoplvl);


   /* Used only by the SDF_SYSTEM thread to pickup the queue created by the bin init process */
struct sdf_queue_pair *
sdf_msg_getsys_qpairs(service_t protocol, uint32_t src_node, uint32_t dest_node);


   /*
    * In sdfagent the command line arg "--msg_mpi 2" will set the MPI Init in
    * motion.  the flags arg, if set to SDF_MSG_NO_MPI_INIT it will bypass MPI
    * all together under all other conditions it returns the rank of the
    * process an invalid rank or failure will return a -1,
    */
void
sdf_msg_init_mpi(int argc, char *argv[], int flags);


    /*
     * when queue pairs are created we check to see if the node is actually
     * valid and within the range of MPI processes that have been started.
     * Returns 0 if the desired queue is valid, 1 if not
     */
uint32_t
sdf_msg_node_check(vnode_t src_vnode, vnode_t dest_vnode);


    /*
     * TOBEDEPRECATED temporary yet a simple way to sync nodes via MPI before
     * releasing the messaging engine to get on with its job. This is called by
     * the sdf_msg_startmsg() and will block until every node has made the call
     * to start. pnodeid is unused.
     */
bin_t
sdf_msg_nsync(vnode_t myid, vnode_t pnodeid);

    /*
     * find out the number of processes that have been started by MPI
     * returns the rank of the calling process, if mpirun is not used
     * to start these folks then rank = 0 & numprocs = 0
     */
int
sdf_msg_nodestatus(uint32_t *sdf_msg_num_procs, int *pnodeid, int node_lst[MAX_NUM_SCH_NODES], int *actmask);

/*
 * Lock this process to the lowest number cpu available and enable 2 cores to run threads
 * start at zero. Only used for test routines in unit tests.
 */
int
lock_processor(uint32_t firstcpu, uint32_t numcpus);

/* print out the queue info and other stats */
void
sdf_msg_qinfo(void);

/*
 * call to determine if it's time to check for outstanding response msgs,
 * currently done after a predetermined number of msg send/recv cycles
 */
void
sdf_msg_chktimeout(void);

/**
 * @brief set value (in secs) for the outstanding request msgs
 *
 * the timeout value can be set at the command line with msg_timeout
 * which sets the interval time on which request messages are checked
 * for their return.
 *
 * @param timeout in secs
 */
void
sdf_msg_settimeout(int timeout);


/* calc differential time when given a timestamp */
msgtime_t
show_dtime(msgtime_t oldtm);

/**
 * @brief get a timestamp returns raw time tv_nsec in ns
 *
 * @return msgtime_t the current time in ns
 */
msgtime_t
get_the_nstimestamp();

/**
 * @brief timecalc typically used fro performance measurements and timeouts
 *
 * Pass in a previous timestamp, an array and element number and it will load
 * the diff time there returns the current time in ns
 *
 * @param msgtime_t <IN> a previous time, if zero it will take a timestamp for you
 * @param int <IN> array number, for now sender must insure there is no out of range write
 * @param msgtime_t array <IN> pointer to array
 * @return msgtime_t the current time in ns
 */
msgtime_t
show_howlong(msgtime_t oldtm, int n, msgtime_t *tsttm_array);

/* read the props file and get the hostname to init the mapping table */

int
sdf_msg_getnodename(struct sdf_msg_bin_list *myndstate);

/* init the runtime msg struct */

int
sdf_msg_setup_rtstate(vnode_t myid);

/**
 * @brief
 * sync all nodes before continuing for cmc init
 * WIP - turn this into a more generalized operation
 */
int
sdf_msg_barrier(int mynode, int flags);

#define SDF_MSG_BARRIER_ITEMS() \
    item(SDF_MSG_BARREL)        \
    item(SDF_MSG_BARWAIT)       \
    item(SDF_MSG_BARCIC)        \
    item(SDF_MSG_BARCIW)

typedef enum SDF_msg_barrier_flags {
#define item(caps) caps,
    SDF_MSG_BARRIER_ITEMS()
#undef item
} SDF_msg_barrier_flags;

const char *SDF_msg_barrier_flags_to_string(enum SDF_msg_barrier_flags barr);

#ifdef __cplusplus
}
#endif

#endif /* _SDF_MSG_TYPES_H */
