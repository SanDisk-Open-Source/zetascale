/*
 * File: sdf_msg_mpi.c
 * Author: Tom Riddle
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * $Id: sdf_msg_mpi.c 308 2008-02-20 22:34:58Z tomr $
 */
#ifdef MPI_BUILD
#include "mpi.h"
#endif
#include <sched.h>
#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include "sdf_fth_mbx.h"
#include "sdf_msg_int.h"
#include "sdf_msg_mpi.h"
#include "sdf_msg_hmap.h"
#include "sdf_msg_sync.h"
#include "sdf_msg_types.h"
#include "sdftcp/msg_map.h"
#include "sdftcp/msg_msg.h"
#include "sdftcp/msg_sdf.h"
#include "sdftcp/msg_trace.h"
#include "platform/time.h"
#include "platform/logging.h"
#include "utils/properties.h"
#include "protocol/replication/replicator.h"


/*
 * Function prototypes.
 */
static void  wait_send(void);
static void  drop_node(nno_t nno);
static void  live_back(int live, int rank, void *arg);
static void  call_send(int sdfno, void *, int len, int stag, int dtag);


/*
 * Static variables.
 */
static int SendId = 1;
static int NumNodes = MAX_NUM_SCH_NODES;

typedef struct sdf_msg_bin_list {
    struct sdf_msg_bin_init *bint[MAX_NUM_BINS];

    int mpi_init_status;       /* store the status if we are single node and have bypassed MPI Init */
    vnode_t schoon_node;       /* we store the schooner node, that we are, in integer form */
    vnode_t this_node;         /* the MPI rank or node number of the process we are */
    vnode_t mastactive_nodes;  /* MASTER 0 first creates list of nodes with verified connections to others */
    vnode_t active_nodes;      /* each node has a master bit list of with verified connections to "this_node" */
    vnode_t sch_node_lst;      /* decimal numprocs - initialized MPI processes in the cluster upon mpirun */
    vnode_t active_mask;       /* Master node bit mask of nodes that should be in the cluster  */
    vnode_t active_bins;       /* number of active bins on a node with registered threads, queues */
    vnode_t num_node_bins;     /* current number of created bins after mpi_msg init */
    uint16_t node_array[MAX_NUM_SCH_NODES]; /* array of active_nodes under mpi starts at 0 */
    char mynodename[SDF_MSG_MAX_NODENAME];      /* this nodes unix hostname */
    char clustnames[MAX_NUM_SCH_NODES][SDF_MSG_MAX_NODENAME];  /* cluster's map of unix hostnames from props */
    char mpihostnames[MAX_NUM_SCH_NODES][SDF_MSG_MAX_NODENAME]; /* MPI map of unix hostnames sent at init */
    uint16_t schoon_map[MAX_NUM_SCH_NODES]; /* the mapping of schooner node ids to MPI rank */
    struct sdf_queue_pair *sys_qpair[MAX_NUM_SCH_NODES];   /* SDF_SYSTEM queues created on init */

} bin_list_t;


#ifdef MPI_BUILD
#define PERF 0               /* enable local message timestamping */
#endif

XLIST_H(sdf_msg_xchng, sdf_msg_xchng, xackBuffQ);    /* linked list for PP buffer tracking */
XLIST_IMPL(sdf_msg_xchng, sdf_msg_xchng, xackBuffQ);

XLIST_H(sdf_msg_bbmsg, sdf_msg_bbmsg, bbBuffQ);      /* linked list for bigbuff send buffer tracking */
XLIST_IMPL(sdf_msg_bbmsg, sdf_msg_bbmsg, bbBuffQ);

XLIST_H(sdf_msg_relmsg, sdf_msg_relmsg, relBuffQ);      /* linked list for client buffer release tracking */
XLIST_IMPL(sdf_msg_relmsg, sdf_msg_relmsg, relBuffQ);

extern msg_state_t *sdf_msg_rtstate;                 /* overall message state */


/* FIXME - most likey be deprecated with multi-node capability, but a node to
 * node sync will be useful */
bin_t sdf_msg_nsync(vnode_t mynodeid, vnode_t pnodeid);
bin_list_t *ndstate;                      /* bin configuration and general node connectivity status */

#define DBGP 0
#define DBGP1 0

#define MASTER_NODE 0
#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

static uint32_t sdf_msg_create_bin(sdf_msg_bin_init_t *node_bin);
static uint32_t sdf_msg_bin_refill(sdf_msg_bin_init_t *node_bin);
static void sdf_msg_looking_fornodes(struct sdf_msg_bin_list *binlist);
static double sdf_msg_timecalc(double tstart);
static void sdf_msg_deliver(sdf_msg_bin_init_t *node_bin);
static void prt_msg_buff(sdf_msg_bin_init_t *node_bin, uint32_t bfnum, uint32_t ln);
static int sdf_msg_taglen_check(int destnode, int ptag, int mtype, int blen);
static int sdf_msg_buffchk(sdf_msg_bin_init_t *node_bin);
static sdf_sys_msg_t *sdf_msg_syspkgb(sdf_sys_msg_t *msg, sdf_msg_bin_init_t *b, int cnt);
static cbuff *sdf_msg_syshandle(sdf_msg_bin_init_t *node_bin, sdf_sys_msg_t *bf, int cnt, cbuff *bbuff);
static void sdf_msg_dumphdr(sdf_msg_t *msg, sdf_sys_msg_t *bf);
static int sdf_msg_free_sysbuff(sdf_sys_msg_t *msg);
static int sdf_msg_sendsysmsg(int cmnd, int proto, int dn, int blen, int rtag);
static int sdf_msg_isend_log(service_t dest_service, struct sdf_msg *msg, sdf_fth_mbx_t *ackmbx);

static int sdf_msg_chk_unex(sdf_msg_bin_init_t *node_bin);
static sdf_msg_t *sdf_msg_pkgb(sdf_msg_t *msg, sdf_msg_bin_init_t *b, int cnt);

/* each bin has an associated data structure */
static sdf_msg_bin_init_t node_bins[SDF_PROTOCOL_COUNT];   /* we have a static number of bins we create */
static bin_list_t bin_config;                 /* bin configuration and general node connectivity status */
static struct sdf_msg_relmsg relmsg;

static int myidp;    /* your process node id that we use only here for printfs */
static uint64_t recycd = 0;                /* keep running tab on recycled buffers */
static uint64_t tms_old, tmstmp;
static msgtime_t tsttm_array[40];


/* we just ripped off Brian Os gdb debug approach which does come handy */

void UTStartDebugg(char *sprog) {
    char   stmp[1024];
    pid_t  pid;

    pid = plat_getpid();
//    snprintf(stmp, "xterm -geometry 100x65 -e bash -c 'gdb %s %d' &", sprog, pid);
    snprintf(stmp, 1024, "xterm -geometry 100x65 -e bash -c '/opt/schooner/gdb-6.7-fth/bin/gdb %s %d' &",
             sprog, pid);
    fprintf(stderr, "Invoking debugger: '%s'\n", stmp);
    system(stmp);
    system("sleep 2");
//    sleep(2);
}

/**
 * @brief sdf_msg_init_mpi() find process rank and how many other processes are
 * started under mpirun we figure out what nodes are expected to attach and
 * later the msg thread checks for them
 *
 * If the SDF_MSG_ENGINE_START in the properties file is set to 1 messaging
 * will be started.  If you specify the cmnd arg "--msg_mpi x" messaging will
 * be started but requires an mpirun launch The call to sdf_msg_init_mpi() is
 * required by the agent in order to obtain a rank and the total number of MPI
 * processes (if multi-node has been launched).
 *
 * @return -- returns the nodes rank if successful otherwise it will exit with
 * a fatal MPI error
 */
void
sdf_msg_init_mpi(int argc, char *argv[], int flags)
{
    int i;
    int myid1 = 0;

    /* Here we set the mpi init status to the flags for everyone, it's either
     * enabled or not */
    bin_config.mpi_init_status = flags;
    ndstate = &bin_config;    /* set the extern state pointer for bin_config */
    sdf_msg_setup_rtstate(bin_config.this_node); /* init the rtstate */

    /* we skip MPI totally if we are passed the SDF_MSG_NEW_MSG flag. */
    if (flags & SDF_MSG_NEW_MSG) {
        msg_map_livecall(1, live_back, NULL);
        bin_config.this_node = MeRank;
        myid1 = MeRank;
        myidp = MeRank;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                     "\nNEW_MSG init found remote node --> my node # is %d\n", bin_config.this_node);
        bin_config.sch_node_lst = NumNodes;
        bin_config.active_nodes = 1;
        bin_config.mastactive_nodes = 1;
        for (i = 0; i < NumNodes; i++) {
            bin_config.node_array[i] = 0;
            bin_config.active_mask = bin_config.active_mask << 1;
            bin_config.active_mask = bin_config.active_mask | 1;
            /* this is our node anyway so flag it now */
            if (i == myid1) {
                bin_config.active_nodes = (bin_config.active_nodes << i);
                bin_config.node_array[i] = 1;
                bin_config.node_array[i] |= SDF_MSG_FLAG_VALID_NODE;
            }
        }
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nMPI ID %d: active_nodes 0x%x numprocs %d active_mask 0x%x sch_node_lst %d\n",
                     bin_config.this_node, bin_config.active_nodes, NumNodes, bin_config.active_mask,
                     bin_config.sch_node_lst);
        sdf_msg_getnodename(ndstate);      /* get the hostname and read props file */
        if (bin_config.sch_node_lst > 1) {
            sdf_msg_looking_fornodes(ndstate);   /* verify connection and remote hostnames */
        }
        bin_config.schoon_node = bin_config.this_node;
        return;
    }

    /* Show the messaging we are using */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_INFO,
        "\nUsing %s\n", NewMsg ? "new messaging" : "MPI");

    /* we skip MPI_Init if we are passed the SDF_MSG_NO_MPI_INIT flag. */
    if (flags & SDF_MSG_NO_MPI_INIT) {
        bin_config.this_node = myid1;
        MeRank = myid1;  /* FIXME this is currently how the queue.c gets it's settings for NODE prints */
        myidp = myid1; /* do the local print thing */
        bin_config.sch_node_lst = NumNodes;
        bin_config.active_nodes = 1;
        bin_config.mastactive_nodes = 1; /* use this to alert that the messaging is not active */
        bin_config.active_mask = 1;
        bin_config.num_node_bins = 0;
        bin_config.node_array[0] = 1;
        bin_config.node_array[0] |= SDF_MSG_FLAG_VALID_NODE;
        for (int p = 1; p < MAX_NUM_SCH_NODES; p++)
            bin_config.node_array[p] = 0;
        bin_config.mpi_init_status |= SDF_MSG_DISABLE_MSG; /* set now and clear later if we do sdf_msg_init */
        /* return the rank of zero */
        bin_config.schoon_node = bin_config.this_node;
        sdf_msg_rtstate->myid = MeRank;
        sdf_msg_getnodename(ndstate);                /* get the hostname and read props file */
        return;
    }

    /* we're here to do multi-node */

    if (MPI_Init(&argc, &argv) != MPI_SUCCESS)
        fatal("MPI_Init failed");

    MPI_Comm_rank(MPI_COMM_WORLD, &myid1);

    bin_config.this_node = myid1; /* save the mpi rank */
    bin_config.schoon_node = bin_config.this_node;
    MeRank = myid1;  /* FIXME this is currently how the queue.c gets it's settings for NODE prints */
    myidp = myid1; /* do the local print thing */
    sdf_msg_rtstate->myid = MeRank;
    bin_config.sch_node_lst = NumNodes;
    /* we always start out with at least one process */
    bin_config.active_mask = 0;
    /* active bins to be incremented during node init */
    bin_config.active_bins = 0;
    if (NumNodes == 1) {
        bin_config.active_nodes = 1;
        bin_config.mastactive_nodes = 1;
        bin_config.active_mask = 1;
        bin_config.num_node_bins = 0;
        bin_config.node_array[0] = 1;
        bin_config.node_array[0] |= SDF_MSG_FLAG_VALID_NODE;
        for (int p = 1; p < MAX_NUM_SCH_NODES; p++) {
            bin_config.node_array[p] = 0;
        }
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nMPI ID %d: SINGLE NODE CONFIG - activenodes %d numprocs %d mask 0x%x\n",
                     myid1, bin_config.active_nodes, NumNodes, bin_config.active_mask);
    } else if (NumNodes <= MAX_NUM_SCH_NODES) {
        /*
         * active_mask is the total number of processes kicked off from mpirun
         * right now we just have a maximum of 4 nodes supported
         * active_nodes & mastactive_nodes will be set to 1 as we always have a MASTER
         * the active mask is a bit field representing the total number of processes kicked off
         */
        bin_config.active_nodes = 1;
        bin_config.mastactive_nodes = 1;
        for (i = 0; i < NumNodes; i++) {
            bin_config.node_array[i] = 0;
            bin_config.active_mask = bin_config.active_mask << 1;
            bin_config.active_mask = bin_config.active_mask | 1;
            /* this is our node anyway so flag it now */
            if (i == myid1) {
                bin_config.active_nodes = (bin_config.active_nodes << i);
                bin_config.node_array[i] = 1;
                bin_config.node_array[i] |= SDF_MSG_FLAG_VALID_NODE;
            }
        }
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nMPI ID %d: active_nodes 0x%x numprocs %d active_mask 0x%x sch_node_lst %d\n",
                     bin_config.this_node, bin_config.active_nodes, NumNodes, bin_config.active_mask,
                     bin_config.sch_node_lst);
    } else {
        MPI_Finalize();
        fatal("too many MPI processes: %d > %d", NumNodes, MAX_NUM_SCH_NODES);
    }

    sdf_msg_getnodename(ndstate);                /* get the hostname and read props file */
    if (bin_config.sch_node_lst > 1)
        sdf_msg_looking_fornodes(ndstate);
}


/*
 * sdf_msg_nodestatus() will return who you are along with the status of the other nodes that
 * you see as active, connected
 */
int
sdf_msg_nodestatus(uint32_t *sdf_msg_num_procs, int *pnodeid, int node_lst[MAX_NUM_SCH_NODES], int *actmask) {
    *sdf_msg_num_procs = bin_config.sch_node_lst;
    *pnodeid = bin_config.active_nodes;
    *actmask = bin_config.active_mask;
    uint16_t nmask = 0x7fff;

    for (int i = 0; i < bin_config.sch_node_lst; i++) {
        node_lst[i] = ((int)bin_config.node_array[i] & nmask);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d: node_array[%d] = %d node_lst[%d] = %d\n", MeRank, i,
                     (bin_config.node_array[i] & nmask), i, (node_lst[i] & nmask));
    }
    return (bin_config.this_node);
}


/**
 *  @brief sdf_msg_register_queue_pair() Each queue pair creation will increment the familyid to
 * let us know what receive bins to check, no need to spend time checking bins that are unused
 * right now it just maintains a total count of how many been registered for that protocol type
 * we use this to selectively bypass bins that we know will not anything there, a perf enhancement
 */

static int wildcard_cnt = 0;

int
sdf_msg_register_queue_pair(struct sdf_queue_pair *q_pair) {
    int ret;


    if ((bin_config.mpi_init_status & SDF_MSG_NO_MPI_INIT) && (!(bin_config.mpi_init_status & SDF_MSG_SINGLE_NODE_MSG))){
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Msg Disabled bypass queue registration mpi_init_status 0x%x\n", MeRank,
                     bin_config.mpi_init_status);
        return (0);
    }
        
    struct q_tracker *saveit = (struct q_tracker *)plat_alloc(sizeof(q_tracker_t));

    /* check to see if the service number is within the allowable bin range */
    if (((q_pair->dest_service > 0) && (q_pair->dest_service < DEF_NUM_BINS)) &&
        ((q_pair->src_service > 0) && (q_pair->src_service < DEF_NUM_BINS))) {
        node_bins[q_pair->dest_service].sdf_familyid++;           /* keep track of registered queues omit wildcards */
        saveit->wnum = 0;
        saveit->qnum = sdf_msg_rtstate->qtotal++;
        saveit->src_node = q_pair->src_vnode;
        saveit->src_srv = q_pair->src_service;
        saveit->dest_node = q_pair->dest_vnode;
        saveit->dest_srv = q_pair->dest_service;
        saveit->q_pair_tkr = q_pair;
        saveit->q_in = q_pair->q_in;
        saveit->q_out = q_pair->q_out;
        saveit->src_srv_name[49] = '\0';
        saveit->dest_srv_name[49] = '\0';
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: queue %p q_in %p q_out %p registered sn %d ss %d dn %d ds %d queue count %d\n", MeRank,
                     q_pair, q_pair->q_in, q_pair->q_out, q_pair->src_vnode, q_pair->src_service, q_pair->dest_vnode,
                     q_pair->dest_service,
                     node_bins[q_pair->src_service].sdf_familyid);
        sdf_msg_rtstate->qstate[saveit->qnum] = saveit;

    } else if ((q_pair->dest_service == SERVICE_ANY) || (q_pair->src_service == SERVICE_ANY)) {
        wildcard_cnt++;
        saveit->qnum = sdf_msg_rtstate->qtotal++;
        saveit->wnum = wildcard_cnt;
        saveit->src_node = q_pair->src_vnode;
        saveit->src_srv = q_pair->src_service;
        saveit->dest_node = q_pair->dest_vnode;
        saveit->dest_srv = q_pair->dest_service;
        saveit->q_pair_tkr = q_pair;
        saveit->q_in = q_pair->q_in;
        saveit->q_out = q_pair->q_out;
        saveit->src_srv_name[49] = '\0';
        saveit->dest_srv_name[49] = '\0';
        /* FIXME need to add support for wildcards */
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Wildcard queue %p q_in %p q_out %p sn %d ss %d dn %d ds %d queue count %d\n", MeRank,
                     q_pair, q_pair->q_in, q_pair->q_out, q_pair->src_vnode, q_pair->src_service,
                     q_pair->dest_vnode, q_pair->dest_service, wildcard_cnt);
        sdf_msg_rtstate->qstate[saveit->qnum] = saveit;
    } else {
        plat_free(saveit);
    }
    plat_assert(sdf_msg_rtstate->qtotal < MAX_QCOUNT);
    /* alert the folks creating queues that messaging has been disabled and sends will not post or serviced */
    if (bin_config.mpi_init_status & SDF_MSG_DISABLE_MSG) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d: WARNING: Created queues with messaging disabled! Don't send msgs: flags-0x%x\n",
                     MeRank, bin_config.mpi_init_status);
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: q_pair %p sn %d ss %d dn %d ds %d qcnt %d wcnt %d\n", MeRank,
                 saveit->q_pair_tkr, saveit->src_node, saveit->src_srv, saveit->dest_node, saveit->dest_srv,
                 saveit->qnum, saveit->wnum);

    ((MeRank != q_pair->src_vnode) && (q_pair->src_vnode != VNODE_ANY)) ? (ret = 1) : (ret = 0);

    return (ret);
}

/*
 * upon shutdown of messaging we will deallocate the queues and anything else lying around
 *
 */

int
sdf_msg_closequeues(void) {
    for (int i = 0; i < sdf_msg_rtstate->qtotal; i++) {
        struct q_tracker *saveit = sdf_msg_rtstate->qstate[i];
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                     "\nNode %d: q_pair %p q_in %p q_out %p sn %d ss %d dn %d ds %d qcnt %d wcnt %d",
                     sdf_msg_rtstate->myid, saveit->q_pair_tkr, saveit->q_in, saveit->q_out, saveit->src_node,
                     saveit->src_srv, saveit->dest_node, saveit->dest_srv, saveit->qnum, saveit->wnum);
        sdf_delete_queue_pair(saveit->q_pair_tkr);
    }
    return (0);
}


/**
 * @brief Initialization of messaging engine
 *
 * sets housekeeping and preallocates MPI receive buffers
 * will verify the other started processes with a sync routine
 *
 * @return 0 if multinode detected, 1 for single node messaging
 */

bin_t
sdf_msg_init_bins(vnode_t thisnodeid)
{
    int i = 0, q = 0, k = 0;

    /* check to see if we are generating messages from the proper node we think we are */
    if (bin_config.schoon_node != thisnodeid) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: schoon_node %d bin_config.this_node %d\n", thisnodeid,
                     bin_config.schoon_node, bin_config.this_node);
        plat_assert(bin_config.schoon_node == thisnodeid);
    }

    /*
     * check to see that runtime eager limit size is larger then the static bin size
     * right now it is just a define and will not care after we move off of MPI
     */
    plat_assert(SM_EAGRLMT >= CONBSZE);

    /* will statically allocate these initial bins for now */

#ifndef notyet

    {
        const char log_fmt[] =
                    "Node %d: %s: \nTotal Bins %d\n"
#define item(caps) #caps "=%d\n"
                SDF_MSG_PROTOCOL_ITEMS();
#undef item

#define item(caps), caps
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     log_fmt, thisnodeid, __func__, DEF_NUM_BINS
                     SDF_MSG_PROTOCOL_ITEMS());
#undef item
    }

#else /* ndef notyet */
    /*
     * GCC bug: GCC chokes on having the same macro defined twice within
     * a macro expansion.
     */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: %s: \nTotal Bins %d\n"
#define item(caps) #caps "=%d\n"
             SDF_MSG_PROTOCOL_ITEMS()
#undef item
#define item(caps), caps
                SDF_MSG_PROTOCOL_ITEMS()
#undef item
                 );
#endif /* else ndef notyet */

/* default bins */

#define item(caps) node_bins[caps].protocol_type = caps;
    SDF_MSG_PROTOCOL_ITEMS()
#undef item

    for (q = 0; q < DEF_NUM_BINS; q++) {
        node_bins[q].binid = q;
        node_bins[q].max_msgsize = MAXBUFFSZE;   /* set these all the same size */
        if (q == 0) {
            node_bins[q].sdf_familyid = 1;       /* SDF_DEBUG bin is enabled all the time */
        } else {
            node_bins[q].sdf_familyid = 0;       /* set all of the bin checks to zero until queues are created */
        }
        node_bins[q].fam_msg_type = 0;           /* FIXME using as debug counter for total msgs seen by a bin */
        node_bins[q].sdf_msg_rnode = thisnodeid; /* we are the receive node */
        node_bins[q].sdf_msg_snode = MPI_ANY_SOURCE;  /* we will accept messages from MPI_ANY_SOURCE */
        node_bins[q].num_buffs = DEF_NUM_BUFFS;  /* total number of PrePosted buffers we statically create */
        node_bins[q].free_buffs = 0;             /* buffers that are freed and ready to Post as receive */
        node_bins[q].post_buffs = 0;             /* current number of active posted receive buffers */
        node_bins[q].proc_buffs = 0;             /* buffers delivered and being processed */
        node_bins[q].got_buffs = 0;              /* buffers filled with an incoming msg, ready to deliver */
        node_bins[q].bbindx = 0;                 /* big msg buffers use a simple counter */
        node_bins[q].head = NULL;                /* init the LL structs for the statically allocated buffs */
        node_bins[q].tail = NULL;                /* init the LL structs for the statically allocated buffs */
        node_bins[q].bbhead = NULL;              /* init the LL structs for dynamically Big Buffs */
        node_bins[q].bbtail = NULL;              /* init the LL structs for Big Buffs */
        node_bins[q].bbmsg = NULL;               /* init the LL pointer for Big Buffs */

        node_bins[q].node_q = NULL;              /* only used to for SDF_SYSTEM send queue */
        if (DBGP) {
            printf("Node %d %s head %p tail %p count %d\n", thisnodeid, __func__,
                   &node_bins[q].head, &node_bins[q].tail, q);
            fflush(stdout);
        }

        /* seed the buffers to help in debugging */
        for (k = 0; k < DEF_NUM_BUFFS; k++) {
            node_bins[q].xchng[k] = &node_bins[q].st_xchng[k];
            node_bins[q].xchng[k]->bin_buff = &node_bins[q].r_buf[k];
            node_bins[q].tagbuff[k] = 0;
            node_bins[q].mreqs[k] = MPI_REQUEST_NULL;
            node_bins[q].sreqs[k] = MPI_REQUEST_NULL;
//            node_bins[q].rstat[k] = NULL;
            node_bins[q].indx[k] = 0;
            for (i = 0; i < node_bins[q].max_msgsize; i++) {
                node_bins[q].r_buffsze[k][i] = 55;  /* ASCII 7's to buf, imprv debug readability */
            }
            node_bins[q].free_buffs++;   /* increment free buffs indicate ready to post */
            node_bins[q].r_buf[k] = &node_bins[q].r_buffsze[k][0];
            node_bins[q].unexp_buf[k] = NULL;
        }
    }

    for (q = 0; q < DEF_NUM_BINS; q++) {
        if (DBGP) {
            printf("Node %d: %s: sze %d, fid %d, proto %d, mtype %d, snode %d, buffs %d\n",
                   node_bins[q].sdf_msg_rnode, __func__,
                   node_bins[q].max_msgsize, node_bins[q].sdf_familyid,
                   node_bins[q].protocol_type, node_bins[q].fam_msg_type,
                   node_bins[q].sdf_msg_snode, node_bins[q].num_buffs);
            printf("Node %d: %s: queue_pair %p\n", thisnodeid, __func__, node_bins[q].node_q);
            fflush(stdout);
        }

        if (bin_config.sch_node_lst > 1) {
            sdf_msg_create_bin(&node_bins[q]);
        }

        relmsg.msg = NULL;
        relmsg.cntr = 0;
        relmsg.hd = NULL;
        relmsg.tl = NULL;
        relmsg.relBuffQ = NULL;


    }
    /*
     * right now only create queue on the same node for thread to thread
     * communication queues to other nodes are created when the nodes attach
     * ie: looking for nodes
     */
    node_bins[SDF_SYSTEM].node_q = sdf_create_queue_pair(thisnodeid, bin_config.this_node,
                                                         SDF_SYSTEM, SDF_SYSTEM, SDF_WAIT_CONDVAR);

    /* init the hash table for response messages, we're at 100 buckets */

    sdf_msg_setuphash(MeRank, NUM_HBUCKETS);

    if (bin_config.mpi_init_status & SDF_MSG_DISABLE_MSG) {
        bin_config.mpi_init_status ^= SDF_MSG_DISABLE_MSG; /* clear this now since we're done with sdf_msg_init */
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: DONE with message bin setup ---> flags 0x%x node_lst %d\n",
                 MeRank, bin_config.mpi_init_status, bin_config.sch_node_lst);

    sdf_msg_sync_init();
    return ((bin_config.sch_node_lst > 1) ? 0 : 1);

}

/**
 * @brief sdf_msg_node_check() when threads create q_pairs we need to check
 * if the node is indeed valid. Here we simply check if the src and dest
 * nodes are within the range of MPI processes that have been started. If
 * not the queues will not be created.  @return 0 if the check is
 * successful
 */
uint32_t
sdf_msg_node_check(vnode_t src_vnode, vnode_t dest_vnode) {
    if ((dest_vnode == VNODE_ANY ||
         dest_vnode <= (bin_config.sch_node_lst - 1)) &&
        (src_vnode == VNODE_ANY ||
         src_vnode <= (bin_config.sch_node_lst - 1))) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: OK to create q_pair dn %d procs %d\n",
                     src_vnode, dest_vnode, bin_config.sch_node_lst);
        return (B_FALSE);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: Cannot Create Queue - Requested Destination Node: %d doesn't exist - numprocs %d\n",
                     src_vnode, dest_vnode, bin_config.sch_node_lst);
        return (B_TRUE);
    }
}

struct sdf_queue_pair *
sdf_msg_getsys_qpairs(service_t protocol, uint32_t src_node, uint32_t dest_node) {
    struct sdf_queue_pair *q_pair;

    if ((protocol == SDF_SYSTEM)&&(src_node == dest_node)) {
        return (node_bins[SDF_SYSTEM].node_q);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: SDF_SYSTEM queue pair sn %d  dn %d proto %d q_pair %p\n",
                     src_node, src_node, dest_node, protocol, bin_config.sys_qpair[dest_node]);

        return (q_pair = bin_config.sys_qpair[dest_node]);
    }
}

/* Create the bins */

static bin_t
sdf_msg_create_bin(sdf_msg_bin_init_t *node_bin) {

#ifdef MPI_BUILD
    sdf_msg_bin_init_t *b;
    int i;

    b = node_bin;

    for (i = 0; i < (b->num_buffs); i++) {
        if (!NewMsg) {
            MPI_Irecv(b->r_buf[i], b->max_msgsize, MPI_BYTE, b->sdf_msg_snode,
                            b->protocol_type, MPI_COMM_WORLD, &b->mreqs[i]);
        }

        b->indx[i] = DBNUM + 1;  /* just fill in this with a num to have a better visual on printf */
        b->free_buffs--;  /* take off free buffs since they are being posted */
        b->post_buffs++;  /* increment posted buffs since they are being posted */
        if (DBGP) {
            printf("Node %d: num %d Send Node %d mreqs %p proto %d r_buf %p\n",
                   b->sdf_msg_rnode, i, b->sdf_msg_snode, &b->mreqs[i], b->protocol_type,
                   b->r_buf[i]);
            fflush(stdout);
        }
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: bin setup done protocol %d posted %d free %d\n",
                 b->sdf_msg_rnode, b->protocol_type, b->post_buffs, b->free_buffs);
#endif
    return (0);
}


/*
 * sdf_msg_recv_incoming() fundamental loop for grabbing incoming messages.
 * loops through all the bins if they have registered queues for them,
 * otherwise don't bother looking in them. First test for filled PP buffers in
 * a bin, if true then get the count and deliver them. PP buffers can be filled
 * after the Testsome and they will be delivered next go around. Returns the
 * state
 */

struct msg_state *
sdf_msg_recv_incoming(struct msg_state *rtstat_local)
{
    int n;
#ifdef MPI_BUILD
    int q = 1, i, j, astat = 0, chklp = 0;
    int outcount = 0;
    bin_t fillcnt = 0;

    /* For the new messaging system */
    unsigned char *recv_data = NULL;

    if (DBGP) {
        printf("Node %d: ----- %s: ------- checking for messages..\n",
               node_bins[q].sdf_msg_rnode, __func__);
        fflush(stdout);
        printf("max_msgsize %d, sdf_familyid %d, prottype %d, msgtype %d, rnode %d, pnode %d\n",
               node_bins[q].max_msgsize, node_bins[q].sdf_familyid,
               node_bins[q].protocol_type, node_bins[q].fam_msg_type,
               node_bins[q].sdf_msg_rnode, node_bins[q].sdf_msg_snode);
        fflush(stdout);
    }

    tms_old = get_the_nstimestamp();

    int gotone = 0;
    int seqnum = 0;

    /*
     * This code really needs to be restructured.  There should be a separate
     * routine to process a message once received and this should see if we
     * have a message and call it.  The current code is a hack but we do not
     * want to risk breaking what works at this moment.
     */
    n = NewMsg ? 1 : DEF_NUM_BINS;
    for (j = 0; j < n; j++) {
        seqnum++;
        if (node_bins[j].sdf_familyid) {
            if (NewMsg) {
                msg_info_t *info = msg_map_poll(0);
                if (info) {
                    if (info->type == MSG_ERECV) {
                        if (info->len < sizeof(sdf_msg_t))
                            drop_node(info->nno);
                        else {
                            recv_data = (unsigned char *) msg_idata(info);
                            j = ((sdf_msg_t *)recv_data)->msg_dest_service;
                            if (j >= 0 && j < DEF_NUM_BINS)
                                outcount++;
                            else {
                                drop_node(info->nno);
                                break;
                            }
                        }
                    }
                    msg_ifree(info);
                }
            } else {
                astat = MPI_Testsome(DBNUM, &node_bins[j].mreqs[0], &outcount,
                                    node_bins[j].indx, &node_bins[j].rstat[0]);
            }
        } else
            outcount = 0;
        /* Returns outcount, indx and rstat */

        if (astat == MPI_UNDEFINED) {
            while (chklp < 5) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "\nNode %d: SDF MPI ERROR Out of request handles... HALTING\n",
                             node_bins[j].sdf_msg_rnode);
                sleep(1);
                chklp ++;
            }
        }
        if ((0 < outcount) && (outcount < node_bins[j].num_buffs)) {
            for (i = 0; i < (outcount + 1); i++) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: %s MPI_Testsome outcount %d, index %d, SOURCE %d, TAG %d, ERROR %d\n",
                             node_bins[j].sdf_msg_rnode, __func__, outcount,
                             node_bins[j].indx[i], node_bins[j].rstat[i].MPI_SOURCE,
                             node_bins[j].rstat[i].MPI_TAG, node_bins[j].rstat[i].MPI_ERROR);
                }
        }
        node_bins[j].post_buffs = node_bins[j].post_buffs - outcount; /* remove num of posted buffs */
        node_bins[j].got_buffs = outcount;                            /* store the number we got */
        if (outcount) {
            node_bins[j].fam_msg_type += outcount;
            rtstat_local->msg_delivered[j] += outcount;  /* add to returning msg state */
            sdf_msg_rtstate->msg_delivered[j] += outcount; /* FIXME combine these two */
            /* FIXME using fam_msg_type as a temp debug counter for total
             * messages seen by a bin */
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Bin %d got %d messages - posted buffs %d total %d\n",
                         node_bins[j].sdf_msg_rnode, node_bins[j].protocol_type,
                         outcount, node_bins[j].post_buffs, node_bins[j].fam_msg_type);
            if (NewMsg)
                node_bins[j].r_buf[0] = recv_data; /* point to the msg buffer */
            sdf_msg_deliver(&node_bins[j]);

            /* evaluate LL so we can refill Preposted buffers */
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\n---Node %d -- bin %d ---DELIVERED--- %d msgs --- runstat %d\n",
                         node_bins[j].sdf_msg_rnode, node_bins[j].protocol_type, outcount,
                         sdf_msg_rtstate->sdf_msg_runstat);
            gotone = gotone + outcount;
        } else if (DBGP) {
            printf("Node %d: %s no refills for Bin %d have %d\n\n", node_bins[j].sdf_msg_rnode,
                   __func__, node_bins[j].protocol_type, node_bins[j].free_buffs);
            fflush(stdout);
        }
        /* Always check for refill of the buffers as processed ones could be
         * returned at any time */
        fillcnt = fillcnt + sdf_msg_bin_refill(&node_bins[j]);
//        tms_old = show_howlong(tms_old, (node_bins[j].binid * 2) + 1, tsttm_array);
    }

    /* Check if there are any unexpected messages if so flags, it1 will be true
     * */
    if (!NewMsg)
        sdf_msg_chk_unex(&node_bins[SDF_UNEXPECT]);

    return (rtstat_local);
#endif
}

/* allocate new message buffer and copy static MPI receive buffer to it. setup
 * proper flags and null  */

static struct sdf_msg *
sdf_msg_mbuffcpy(struct sdf_msg *msg) {

    struct sdf_msg *dmsg = (struct sdf_msg *)sdf_msg_alloc(msg->msg_len);
    msg->msg_q_item = NULL; /* a received static buffer will have no valid q_item */
    memcpy(dmsg, msg, msg->msg_len);
    dmsg->msg_flags |= SDF_MSG_FLAG_ALLOC_BUFF;
    if (dmsg->msg_flags & SDF_MSG_FLAG_STATIC_BUFF) {
        dmsg->msg_flags ^= SDF_MSG_FLAG_STATIC_BUFF;
        dmsg->ndbin = NULL;
    }
    if (NewMsg) {
        msg->msg_flags |= SDF_MSG_FLAG_ALLOC_BUFF;
        if (msg->msg_flags & SDF_MSG_FLAG_STATIC_BUFF) {
            msg->msg_flags ^= SDF_MSG_FLAG_STATIC_BUFF;
            msg->ndbin = NULL;
        }
    }
    return (dmsg);

}

/* deliver the messages per bin one at a time, SDF_DEBUG are handled
 * differently since msg format is unique */

static void
sdf_msg_deliver(sdf_msg_bin_init_t *node_bin) {

    struct sdf_msg_bin_init *b = node_bin;
    int j = 0, cnt = 0, k = 0, err = 0, srt = 0, errcnt = 0, sn = 0;
    struct sdf_msg *msg = NULL;
    sdf_sys_msg_t *sysmsg = NULL;

    tmstmp = get_the_nstimestamp();

    cnt = b->got_buffs;           /* the number of buffs received in this bin ie: outcount */

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: proto %d bmaxsz %d got_b %d proc_b %d base r_buf %p base r_buffsze %p\n",
                 b->sdf_msg_rnode, b->protocol_type, b->max_msgsize,
                 b->got_buffs, b->proc_buffs, b->r_buf[k], &b->r_buffsze[j]);
    /*
     * FIXME add error handling for buff counting, when we reach the max just
     * check the unexpected bin to grab the ones that have come in
     */
    if (!NewMsg) {
        if (!b->post_buffs) {
            while (MPI_FAILURE) {
            printf("Node %d: %s Fatal error halting... out of PP buffers proc_buffs %d\n",
                   b->sdf_msg_rnode, __func__, b->proc_buffs);
            fflush(stdout);
            sleep(2);
            }
        }
    }

    /* create an array of char pointers for the big buff */
    cbuff *big_buff[DBNUM];
    for (j = 0; j < cnt; j++) {
        sdf_msg_rtstate->recvstamp++;

        /* we deal with the SDF_DEBUG message bin separately */
        if (b->binid == SDF_DEBUG) {
            srt = b->indx[j];                            /* grab the actual index number */
            sysmsg = ((sdf_sys_msg_t *)b->r_buf[srt]);   /* debug msgs have a different format */
            if (DBGP1) {
                sdf_msg_dumphdr(NULL, sysmsg);           /* dump the msg hdr for debug */
            }
            big_buff[j] = NULL;
            big_buff[j] = sdf_msg_syshandle(node_bin, sysmsg, srt, big_buff[j]); /* handle SDF_DEBUG types */
            sysmsg = sdf_msg_syspkgb(sysmsg, b, srt);    /* package sysmsg buff info into msg hdr */
            int ret = sdf_msg_free_sysbuff(sysmsg);      /* and then release it to allow refilling */

            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: %s big_buff[%d] %p free_buff ret %d\n",
                         b->sdf_msg_rnode, __func__, j, big_buff[j], ret);

            /* So if there is a returned big buff message deliver it */
            if (big_buff[j]) {
                msg = ((sdf_msg_t *)big_buff[j]);
                /* set flags to indicate large buff and alloc */
                msg->msg_flags |= SDF_MSG_FLAG_ALLOC_BUFF; /* flag this so we can release it properly later */
                if (msg->msg_len > CONBSZE) {
                    err = 1;
                    do {
                        tmstmp = show_howlong(tmstmp, 34, tsttm_array);
                        err = sdf_do_receive_msg(msg);      /* try to post msg, but a queue needs to be there */
                        if (!err) {
                            break;
                        }
                        errcnt++;
                        usleep(20000);                   /* FIXME simple timeout with no hard failure */
                    } while ((errcnt < 5)&&(err));
                    if (err) {
                        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                                     "\nNode %d: MSG DELV ERROR Can't Post queue is full - err %d DROPPING MSG\n",
                                     b->sdf_msg_rnode, err);
                        sdf_msg_dumphdr(msg, NULL);
                    }
                } else {
                    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                                 "\nNode %d: BIG MSG DELIVERY ERROR: msg %p bigbuff %p has bad msg_len %d\n",
                                 b->sdf_msg_rnode, msg, big_buff, msg->msg_len);
                }
            }
        } else {
            if (NewMsg) {
                /* NEW_MSG just set the first buffer pointer to the msg */
                msg = ((struct sdf_msg *)b->r_buf[0]);
                msg->ndbin = node_bin;
            } else {
                srt = b->indx[j]; /* grab the actual index number */
                msg = ((struct sdf_msg *)b->r_buf[srt]);
            }
            msg->msg_flags |= SDF_MSG_FLAG_STATIC_BUFF;
            if (msg->msg_flags & SDF_MSG_FLAG_ALLOC_BUFF) {
                msg->msg_flags ^= SDF_MSG_FLAG_ALLOC_BUFF;
            }
            if (msg->msg_flags & SDF_MSG_FLAG_FREE_ON_SEND) {
                msg->msg_flags ^= SDF_MSG_FLAG_FREE_ON_SEND;
            }
            /* Since we're not going to recycle static buffers bypass this for now  */
            if (!NewMsg)
                msg = sdf_msg_pkgb(msg, b, srt);             /* package buff info into msg hdr */
            sn = msg->msg_src_vnode;
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Actual MSG proto %d type %d sn %d ss %d seq %lu\n",
                         b->sdf_msg_rnode, msg->msg_dest_service, msg->msg_type,
                         msg->msg_src_vnode, msg->msg_src_service, msg->msg_seqnum);
            err = 1;
            /* Since we are dumping MPI the static buffer approach will be
             * deprecated so now we do a copy */
            struct sdf_msg *dmsg = sdf_msg_mbuffcpy(msg);
            do {
                tmstmp = show_howlong(tmstmp, 34, tsttm_array);
                err = sdf_do_receive_msg(dmsg);           /* try to post the msg, but a queue needs to be there */
                if (!err) {
                    break;
                }
                errcnt++;
                usleep(20000);
            } while ((errcnt < 5)&&(err));
            if (err) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "\nNode %d: MSG DELIVERY ERROR Can't Post... is queue id full? - err %d DROPPING MSG\n",
                             b->sdf_msg_rnode, err);
                sdf_msg_dumphdr(msg, NULL);
            }
            /* now free the buff static or dynamic */
            sdf_msg_free_buff(msg);
        }
        b->proc_buffs++;    /* increment num of in process buffs delivered if successful */
        b->got_buffs--;     /* these are for the static buffers only, big buffs are handled elsewhere */

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: sn %d ds %d proc_bufs %d got_bufs %d free_buffs %d post_buffs %d total msgs %li\n",
                     b->sdf_msg_rnode, sn, b->protocol_type, b->proc_buffs, b->got_buffs,
                     b->free_buffs, b->post_buffs, sdf_msg_rtstate->recvstamp);
        if (DBGP) {
            prt_msg_buff(b, j, 128);
        }
    }
    tmstmp = show_howlong(tmstmp, 35, tsttm_array);
}


/* sdf_msg_chk_unex() when a msg is received without an expected tag we pick it
 * up here */


static int
sdf_msg_chk_unex(sdf_msg_bin_init_t *node_bin) {

#ifdef MPI_BUILD
    int acount = 0, it1, myret, err;
    int mysource = 0, mytag = 0, mycount = 0, srt;
    MPI_Status unx_stat, stat;
    struct sdf_msg *recv_msg = NULL;
    int nodenum = node_bin->sdf_msg_rnode;

    /* Look for anything and everything that hasn't been picked up by the
     * preposted buffers */

    do {
        myret = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &it1, &unx_stat);

        if (it1) {

            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: UNEXPECTED Got this unx_stat %p, SOURCE %d, TAG %d, ERROR %d\n",
                         nodenum, &unx_stat, unx_stat.MPI_SOURCE, unx_stat.MPI_TAG,
                         unx_stat.MPI_ERROR);

            mysource = unx_stat.MPI_SOURCE;
            mytag = unx_stat.MPI_TAG;
            mycount = unx_stat._count;

            MPI_Get_count(&unx_stat, MPI_BYTE, &acount);

            /*
             * so we saw messages in our unexpected bin, we have determined the
             * size and the tag. Now we need to scope the valid bins and omit
             * large pending messages after that all the rest we will alloc a
             * local buffer and do the receive FIXME -- but really where do you
             * put this thing if it has no where to go
             */
            if (((MAX_NUM_BINS < unx_stat.MPI_TAG) && (unx_stat.MPI_TAG < MSG_TAG_OFFSET))||
                (unx_stat.MPI_TAG > (MSG_TAG_OFFSET + MAX_NUM_BINS))) {
                recv_msg = (struct sdf_msg *)sdf_msg_alloc(acount);

                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                             "\nNode %d: Unexpected Msg with len %d revbuff %p\n", nodenum, acount, recv_msg);

                MPI_Recv(recv_msg, acount, MPI_BYTE, mysource, mytag, MPI_COMM_WORLD, &stat);

                srt = node_bins[SDF_UNEXPECT].got_buffs++;
                node_bins[SDF_UNEXPECT].unexp_buf[srt] = (cbuff *)recv_msg;
                /* package buff info into msg hdr for recovery */
                recv_msg = sdf_msg_pkgb(recv_msg, node_bin, srt);

                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                             "\nNode %d: Actual MSG proto %d type %d ss % d ds %d seq %lu rcvstamp %lu msg %p\n",
                             node_bins[SDF_UNEXPECT].sdf_msg_rnode, recv_msg->msg_dest_service,
                             recv_msg->msg_type, recv_msg->msg_src_service, recv_msg->msg_dest_service,
                             recv_msg->msg_seqnum, sdf_msg_rtstate->recvstamp, recv_msg);

                /* check buffer lengths sent length vs what we got here */
                if (recv_msg->msg_len != acount) {
                    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                                 "\nNode %d: ERROR size mismatch recv_msg %p msg_len %d != acount %d\n",
                                 nodenum, recv_msg, recv_msg->msg_len, acount);
                }

                err = sdf_do_receive_msg(recv_msg);

                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: recv_msg %p, err %d\n", nodenum, recv_msg, err);
            } else {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                             "\nNode %d: Proto ok but unexpected msg SOURCE %d, TAG %d, ERROR %d\n"
                             "count %d will not grab it\n",
                             nodenum, unx_stat.MPI_SOURCE, unx_stat.MPI_TAG,
                             unx_stat.MPI_ERROR, acount);
                break;
            }
        } else {
            break;
        }
    } while (!it1);
    if (DBGP) {
        printf("Node %d: %s: no unexpected msgs flags %d\n", nodenum, __func__, it1);
        fflush(stdout);
    }
#endif
    return (0);
}


/*
 * sdf_msg_sendut_outgoing() called by the twalk for each message pulled off of
 * the queue. Then check first if it's on the same node, a fastpath msg, after
 * which check if it's a size larger then the general PP bin size, then do the
 * send. returns a flag if we need to stop sending and ack a large msg send
 */
int
sdf_msg_send_outgoing(struct sdf_msg *msg)
{
#ifdef MPI_BUILD
    unsigned char *mpay;
    int destnode, dtag, msgtype, len, ret, lserv, mflags, err = 0;
    seqnum_t sstamp;

    mpay = (unsigned char *) msg;
    destnode = msg->msg_dest_vnode;
    msgtype = msg->msg_type;
    len = msg->msg_len;
    dtag = msg->msg_dest_service;
    lserv = msg->msg_src_vnode;
    msg->msg_sendstamp = __sync_fetch_and_add(&sdf_msg_rtstate->sendstamp, 1);
    sstamp = msg->msg_sendstamp;
    mflags = msg->msg_flags;

    /* check for attempted messages that someone tried to send out of allotted
     * rank range */
    plat_assert(destnode < MAX_NUM_SCH_NODES);

    /* here is where we check for invalid services */
    plat_assert(dtag < SDF_PROTOCOL_COUNT);

    if (DBGP) {
        node_bins[MSG_DEBUG].r_buf[0] = mpay;
        prt_msg_buff(&node_bins[MSG_DEBUG], 0, 128);
    }

    /*
     * fast path to sending messages on the same node
     * here we first compare the dest node with the source
     * copy the msg into another buffer and post it back to the
     * appropriate queue or fth mailbox. The copy preserves the
     * current buffer handle scheme as we use it now.
     * If we're on the same node a SDF_SYSTEM message is taken by the
     * messaging thread locally
     */
    if (lserv == destnode) {
        struct sdf_msg *send_msg = NULL;
        fatal("this code should never be executed");
        if (msg->msg_dest_service == SDF_DEBUG) {
            cbuff *jnk = sdf_msg_syshandle(NULL, (sdf_sys_msg_t *)mpay, -1, NULL); /* handle SDF_SYSTEM msgs */
            if (!jnk) {
                send_msg = (sdf_msg_t *)jnk;
                plat_assert(send_msg->msg_dest_vnode == destnode);
            }
        } else {
#ifndef MSG_DISABLE_FP
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                         "\nNode %d: WARNING FASTPATH SAME NODE msg %p with MSG THREAD flags 0x%x from thread %p\n",
                         lserv, msg, msg->msg_flags, msg->fthid);
#endif
            send_msg = (struct sdf_msg *)sdf_msg_alloc(msg->msg_len);
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                         "\nNode %d: FASTPATH copy of msg %p to new msgbuff %p has flags 0x%x but old flags 0x%x\n",
                         lserv, msg, send_msg, send_msg->msg_flags, msg->msg_flags);
            memcpy(send_msg, msg, msg->msg_len);
            if (send_msg->msg_flags & SDF_MSG_FLAG_FREE_ON_SEND) {
                send_msg->msg_flags ^= SDF_MSG_FLAG_FREE_ON_SEND;
            }
            /* what about checking for ndbin? */
            if (msg->msg_flags & SDF_MSG_FLAG_STATIC_BUFF) {
                send_msg->msg_flags ^= SDF_MSG_FLAG_STATIC_BUFF;
                send_msg->msg_flags |= SDF_MSG_FLAG_ALLOC_BUFF;
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: Updated flags send_msg %p to new_flags 0x%x note old flags 0x%x\n",
                             lserv, send_msg, send_msg->msg_flags, msg->msg_flags);
            }

            if (msg->msg_flags & SDF_MSG_FLAG_FREE_ON_SEND) {
                ret = sdf_msg_sbuff_ack(dtag, msg, msg->akrpmbx);
            }
            err = sdf_do_receive_msg(send_msg);
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Delivered MSG FASTPATH sn %d = dn %d fastpath err %d seqnum %lu msg_flags 0x%x\n",
                         lserv, lserv, destnode, err, sstamp, mflags);
        }
    /* lserve != destnode case */
    } else {
        /* Here is where we deal with messages that are larger then the eager
         * size */
        if (!NewMsg)
            dtag = sdf_msg_taglen_check(destnode, dtag, msgtype, len);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: taglen_check returned dtag %d len %d destnode %d seq %li\n", msg->msg_src_vnode,
                     dtag, len, destnode, msg->msg_seqnum);
        /* check for senders who configure for a single node but try sending to
         * someone else */

        plat_assert(bin_config.num_node_bins);

        /* if we have a big message we're doing an non-blocking send and check
         * later if it's gone */
        /* in the meantime we store it on the LL for the later check */
        if (dtag > DEF_NUM_BINS) {
            sdf_msg_rtstate->sdf_msg_runstat = 1;  /* bypass sends from the node until we get an ack back */

            node_bins[msg->msg_src_service].bbindx++;
            msg->ndbin = &node_bins[msg->msg_src_service]; /* load the node bin info into the hdr */

            /* log it on the LL */
            ret = sdf_msg_isend_log((dtag - MSG_TAG_OFFSET), msg, msg->akrpmbx);

            if (NewMsg) {
                fatal("dtag (%d) > max (%d)", dtag, DEF_NUM_BINS);
            } else {
                MPI_Isend(mpay, len, MPI_BYTE, destnode, dtag, MPI_COMM_WORLD,
                          &node_bins[msg->msg_src_service].sreqs[node_bins[msg->msg_src_service].bbindx]);

                MPI_Status stat1;
                int flag;

                MPI_Test(&node_bins[msg->msg_dest_service].sreqs[0], &flag, &stat1);
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: Big Msg Sent test flag %d runstat %d - Complete dtag %d\n",
                             msg->msg_src_vnode, flag, sdf_msg_rtstate->sdf_msg_runstat, dtag);
            }

        } else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Sending - msg %p mpay %p len %d sn %d ss %d dn %d ds %d\n"
                         "        dtag %d type %d flgs 0x%x mkey %s seq %lu\n",
                         MeRank, msg, mpay, len, msg->msg_src_vnode, msg->msg_src_service,  msg->msg_dest_vnode,
                         msg->msg_dest_service, dtag, msg->msg_type, msg->msg_flags, msg->mkey, msg->msg_seqnum);

#ifdef notyet
            if (msg->msg_flags & SDF_MSG_FLAG_STATIC_BUFF)
                msg->msg_flags ^= SDF_MSG_FLAG_STATIC_BUFF;
#endif

            if (NewMsg) {
                call_send(destnode, mpay, len, msg->msg_src_service, dtag);
                wait_send();
            } else
                MPI_Send(mpay, len, MPI_BYTE, destnode, dtag, MPI_COMM_WORLD);

            /*
             * FIXME still need to code in the ack back to the pthread sender
             * of a freed buffer status only FTH has a proper freed buffer
             * mechanism. All eager sized messages are blocking sends which
             * have PP buffers waiting already. Large messages need to have the
             * Isends complete before freeing the buffer
             */
            ret = sdf_msg_sbuff_ack(dtag, msg, msg->akrpmbx);
        }

    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Msg Sent to Node: %d Buff Released dtag %d len %d total msgs sent %li\n",
                 myidp, destnode, dtag, len, sstamp);

    return 0;

#endif
}

/**
 * @brief sdf_msg_syshandle() SDF_DEBUG handler this is the internal messaging
 * thread <-> messaging thread communication done under the queues. We are
 * called from sends when on the same node or if msg received or delivered,
 * additionally it fields large message requests and sets up the PP buffer to
 * accomadate
 */

static cbuff *
sdf_msg_syshandle(sdf_msg_bin_init_t *node_bin, sdf_sys_msg_t *bf, int cnt, cbuff *bbuff) {

    sdf_msg_bin_init_t *b;
    sdf_sys_msg_t *inmsg1, *omsg, jnk;
    unsigned char *m, *bigbuff = NULL;
    MPI_Status stat;
    int ret = 0;
    struct sdf_msg_bbmsg *bbll = NULL;

    b = node_bin;
    int rsze = sizeof(jnk);

    /* first read the hdr of type SDF_DEBUG, get the size as everything is contained within it */

    inmsg1 = bf;

#if 0
    printf("Node %d: %s proto %d prbuffs %d gtbufs %d fbuffs %d pstbuffs %d binid %d\n",
           b->sdf_msg_rnode, __func__, b->protocol_type, b->proc_buffs, b->got_buffs,
           b->free_buffs, b->post_buffs, b->binid);
    fflush(stdout);
    /* for debugging if you want to dump the hdr info */
    for (int i = 0; i < rsze; i++) {
        printf(" %02x", *bf);
        bf++;
        if ((i % 16) == 15) {
            putchar('\n');
            fflush(stdout);
        }
    }
    putchar('\n');
    fflush(stdout);
#endif

    omsg = &jnk;
    m = (unsigned char *)omsg;

    stat.MPI_SOURCE = 0;
    stat.MPI_TAG = 0;
    stat.MPI_ERROR = 0;

    switch (inmsg1->sys_command) {

    case SYS_PP_REQ:

    /*
     * here we setup an outgoing buffer since we have planned to send back a
     * confirmation then the send would commence. However with msgs larger then
     * eager size the send node just blocks until the receive buffer is setup
     * anyway so we will not do this right now, note use SYS_PP_ACK on the
     * response
     */

        sdf_msg_rtstate->msg_delivered[SDF_DEBUG]++;
        /* because the msg had been created by the sender we do not use
         * sdf_msg_alloc */
        bigbuff = (unsigned char *) plat_alloc((int)inmsg1->msg_blen);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Saw Large Msg request from dest %d send node %d hdr rsze %d id %lu\n",
                     b->sdf_msg_rnode, inmsg1->msg_dest_node, inmsg1->msg_send_node,
                     rsze, inmsg1->msg_req_id);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d: Setting up PP request buffer bigbuff %p sn %d dtag %d sze %d state %d\n",
                     b->sdf_msg_rnode, bigbuff, (int)inmsg1->msg_send_node,
                     (int)inmsg1->msg_dtag, (int)inmsg1->msg_blen, sdf_msg_rtstate->msg_delivered[SDF_DEBUG]);
    
        MPI_Recv(bigbuff, ((int)inmsg1->msg_blen), MPI_BYTE, (int)inmsg1->msg_send_node,
                 (int)inmsg1->msg_dtag, MPI_COMM_WORLD, &stat);
        /* now that we have the recvd buffer ack the sending node we have it */
        sdf_msg_sendsysmsg(SYS_PP_ACK, (int)inmsg1->msg_dtag, (int)inmsg1->msg_send_node,
                           (int)inmsg1->msg_blen, -1);

        bbuff = bigbuff;
#if 0
for (int i = 0; i < 128; i++) {
    printf(" %02x", *bigbuff);
    bigbuff++;
        if ((i % 16) == 15) {
            printf("  - myid %d %s\n", b->sdf_msg_rnode, __func__);
            fflush(stdout);
        }
    }
#endif

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d: Got the BIG PP from dest %d send %d bigbuff %p\n",
                     b->sdf_msg_rnode, inmsg1->msg_dest_node, inmsg1->msg_send_node, bigbuff);
        break;

    case SYS_PP_ACK:

        sdf_msg_rtstate->msg_delivered[SDF_DEBUG]++;

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d: Ack'd BIG PP from dest %d send %d proto %d bigbuff %p cmd %d state %d ret %d\n",
                     b->sdf_msg_rnode, inmsg1->msg_dest_node, inmsg1->msg_send_node, inmsg1->msg_src_proto,
                     bigbuff, inmsg1->sys_command, sdf_msg_rtstate->msg_delivered[SDF_DEBUG], ret);

        int proto = inmsg1->msg_src_proto - MSG_TAG_OFFSET;
        bbll = sdf_msg_bbmsg_xlist_dequeue(&node_bins[proto].bbhead,
                                           &node_bins[proto].bbtail);
        node_bins[proto].bbindx--;

        if (bbll != NULL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: ISEND DEQUEUED bbll %p dest %d send %d bigbuff %p\n",
                         b->sdf_msg_rnode, bbll, inmsg1->msg_dest_node, inmsg1->msg_send_node, bigbuff);

            ret = sdf_msg_sbuff_ack(bbll->msg->msg_dest_service, bbll->msg, bbll->msg->akrpmbx);
        } else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: WARNING bbll %p is NULL on PP ACK dest %d send %d bigbuff %p\n",
                         b->sdf_msg_rnode, bbll, inmsg1->msg_dest_node, inmsg1->msg_send_node, bigbuff);
        }
        sdf_msg_rtstate->sdf_msg_runstat = 0;  /* bypass sends from the node until we get an ack back */
        bbuff = NULL;
        break;

    case SYS_SHUTDOWN_ALL:

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d: WARNING SYS Shutdown cmd received from dest %d sent from Node %d\n",
                     b->sdf_msg_rnode, inmsg1->msg_dest_node, inmsg1->msg_send_node);
        bbuff = NULL;
        break;

    default:
        sdf_msg_dumphdr(NULL, inmsg1);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: %s Invalid SYS MSG command %d... aborting\n",
                     b->sdf_msg_rnode, __func__, inmsg1->sys_command);
        plat_assert(0);
        break;
    }
    return (bbuff);
}

/*
 * sdf_msg_bin_refill() recycle the processed pre-posted buffers or release the
 * unexpected buffers that are no longer needed
 */

static bin_t
sdf_msg_bin_refill(sdf_msg_bin_init_t *node_bin) {

#ifdef MPI_BUILD
    sdf_msg_bin_init_t *b = node_bin;
    int i, bcnt, mycnt = 0;
    bin_t lp = 0;
    int trkarray[DBNUM];
    MPI_Request hmm =  MPI_REQUEST_NULL;

    /* first check bin's LL - will not do a refill if the check returns null value */
    bcnt = sdf_msg_buffchk(b);

    if (!bcnt) {
        return (lp);
    }

    for (i = 0; i <= b->num_buffs; i++) {
        if (b->tagbuff[i]) {
            trkarray[mycnt] = i;
            mycnt++;
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Bin %d tagbuff[%d]=%d mycnt %d trk %d\n", b->sdf_msg_rnode,
                         b->protocol_type, i, b->tagbuff[i], mycnt, trkarray[mycnt-1]);
            b->tagbuff[i] = 0;
        }
    }
    lp = b->free_buffs;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Bin %d freeb %d procb %d bcnt %d lp %d\n", b->sdf_msg_rnode,
                 b->protocol_type, b->free_buffs, b->proc_buffs, bcnt, lp);

    for (i = 0; i < lp; i++) {
        if (b->binid == SDF_UNEXPECT) {
//          sdf_msg_free(b->unexp_buf[trkarray[i]]);
        } else {
            b->r_buf[trkarray[i]] = b->r_buffsze[trkarray[i]];
            hmm = b->mreqs[trkarray[i]];

            if (!NewMsg) {
                int flg;
                MPI_Status st;

                MPI_Request_get_status(hmm, &flg, &st);
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: MPI_Request_status %d\n",
                             b->sdf_msg_rnode, flg);
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: SOURCE %d TAG %d ERROR %d\n",
                             b->sdf_msg_rnode,
                             st.MPI_SOURCE, st.MPI_TAG, st.MPI_ERROR);
                MPI_Irecv(b->r_buf[trkarray[i]], b->max_msgsize, MPI_BYTE,
                    b->sdf_msg_snode, b->protocol_type,
                    MPI_COMM_WORLD, &b->mreqs[trkarray[i]]);
            }
            b->post_buffs++;
            b->free_buffs--;
            recycd++;
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: loopnum %d sn %d mreqs %p proto %d postb %d recycd %lu\n",
                         b->sdf_msg_rnode, i, b->sdf_msg_snode, b->mreqs[trkarray[i]],
                         b->protocol_type, b->post_buffs, recycd);
        }
    }

#endif
    return (lp);
}


/*
 * Look for multi node initial acknowledgement and trade hostnames
 *
 * The Master Node = rank 0, it will wait with an MPI_Recv till
 * all other nodes send an message that they are alive. The Master
 * then returns to each node, who waits after their initial msg,
 * the list of nodes in a cluster.
 *
 * LIMITATION: all nodes must respond to complete init. They should
 * or else MPI hasn't started all of the numprocs
 */
static void
sdf_msg_looking_fornodes(struct sdf_msg_bin_list *bin_config)
{
    int i;
    bin_config->num_node_bins = SDF_PROTOCOL_COUNT;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: mpi hostnames\n"
                 "        host[%d]=%s\n"
                 "        host[%d]=%s\n"
                 "        host[%d]=%s\n"
                 "        host[%d]=%s\n", bin_config->this_node,
                 0, bin_config->mpihostnames[0],
                 1, bin_config->mpihostnames[1],
                 2, bin_config->mpihostnames[2],
                 3, bin_config->mpihostnames[3]);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: COMPLETE act_nodes 0x%x active_mask 0x%x sch_node_lst %d\n"
                 "        active_bins %d num_bins %d\n",
                 bin_config->this_node, bin_config->active_nodes, bin_config->active_mask,
                 bin_config->sch_node_lst, bin_config->active_bins, bin_config->num_node_bins);
    /* FIXME Need to set and return proper error condition in case creating master node list fails */

    char *lp = SDF_MSG_DFLT_MPIHOST;
    for (i = bin_config->sch_node_lst; i < MAX_NUM_SCH_NODES; i++) {
        strncpy(bin_config->mpihostnames[i], lp, SDF_MSG_MAX_NODENAME);
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nNode %d: mpi hostnames\n"
                 "        host[%d]=%s\n"
                 "        host[%d]=%s\n"
                 "        host[%d]=%s\n"
                 "        host[%d]=%s\n", bin_config->this_node,
                 0, bin_config->mpihostnames[0],
                 1, bin_config->mpihostnames[1],
                 2, bin_config->mpihostnames[2],
                 3, bin_config->mpihostnames[3]);
}


/**
 * @brief sdf_msg_exitall() here we do a global shutdon of all of the
 * attached nodes to facilitate the testing environment
 */

int
sdf_msg_exitall(uint32_t myid, int flags) {
    int i;

    for (i = 0; i <= (bin_config.sch_node_lst - 1); i++) {
        if (i != myid) {
            sdf_msg_sendsysmsg(SYS_SHUTDOWN_ALL, 0, i, 0, -1);
        }
    }
   return (0);
}



/**
 * @brief sdf_msg_nsync() Node Sync of Isend and blocking receive to a remote
 * node Previously used this to exit test programs, a poor mans barrier to sync
 * the processes.  to be deprecated shortly
 */

bin_t
sdf_msg_nsync(vnode_t mynodeid, vnode_t pnodeid) {

#ifdef MPI_BUILD
    unsigned char outm[3], inm[3];
    int cntllmsg = 128;
    double tm;
    MPI_Status status;
    MPI_Request startreq =  MPI_REQUEST_NULL;

    tm = MPI_Wtime();
    outm[0] = 128;
    outm[1] = (char)mynodeid;
    outm[2] = (char)pnodeid;
    if (mynodeid == 0) {
        printf("Node %d: %s Waiting to receive from node %d\n",
               mynodeid, __func__, pnodeid);
        fflush(stdout);
        MPI_Recv(&inm, 3, MPI_BYTE, pnodeid, cntllmsg, MPI_COMM_WORLD, &status);
        MPI_Isend(&outm, 3, MPI_BYTE, pnodeid, cntllmsg, MPI_COMM_WORLD, &startreq);
    } else {
        printf("Node %d: %s SYNCSEND to this node %d\n",
               mynodeid, __func__, pnodeid);
        fflush(stdout);
        MPI_Ssend(&outm, 3, MPI_BYTE, pnodeid, cntllmsg, MPI_COMM_WORLD);
        MPI_Recv(&inm, 3, MPI_BYTE, pnodeid, cntllmsg, MPI_COMM_WORLD, &status);
    }
    if (1) {
        printf("Node %d: %s SYNCED --> SOURCE %d, TAG %d, ERR %d\n",
               mynodeid, __func__, status.MPI_SOURCE, status.MPI_TAG, status.MPI_ERROR);
        fflush(stdout);
    }
    tm = sdf_msg_timecalc(tm);
#endif
    return (0);
}

static double
sdf_msg_timecalc(double tstart) {

#ifdef PERF

    double myts, tend, tsnap;

    tend = MPI_Wtime();
    myts = MPI_Wtick();
    tsnap = tend - tstart;
    if (DBGP)
        printf("Node %d: %s: elapsed time = %G\n", node_bins[1].sdf_msg_rnode,
               __func__, tsnap);
    fflush(stdout);
    return (tsnap);
#else
    return (0);
#endif
}


static void
prt_msg_buff(sdf_msg_bin_init_t *node_bin, uint32_t bfnum, uint32_t ln) {

    sdf_msg_bin_init_t *c;
    int i = 0, j = 0;
    unsigned char *bf;

    c = node_bin;
    j = bfnum;
    printf("\nNode %d: %s addr %p of r_buf[%d]\n",
           c->sdf_msg_rnode, __func__, c->r_buf[j], j);
    fflush(stdout);

        bf = (unsigned char *) c->r_buf[j];
        for (i = 0; i < ln; i++) {
            printf(" %02x", *bf);
            bf++;
            if ((i % 16) == 15) {
                putchar('\n');
                fflush(stdout);
            }
        }
        i = 0;

    putchar('\n');
    fflush(stdout);
}

/*
 * sdf_msg_taglen_check() checks for send message sizes that exceed the static
 * buffer limit if so it will notify the destination node of this message size
 * request that exceeds the size allocation. We use the SDF_DEBUG bin to send
 * the right info so that a PP buffer is created We don't know yet the
 * workloads or msg sizes which would help to determine what the optimal PP
 * buffer sizes would be. Right now they are 64k since that is the eager size
 * limit for Infinipath
 */

static int
sdf_msg_taglen_check(int destnode, int ptag, int mtype, int blen) {

    int rtag = 2;

    /*
     * check the shared memory or ipath eager size limit limit and the static bin size,
     * if we're smaller then the eager limit then send it as unexpected, otherwise
     * we do the big buffer shuffle which will be to send a PP buffer setup request
     * via the SDF_SYSTEM protocol. We will block
     */

    /* the unexpected case, smaller then the eagerlimit but bigger then PP buffer size */
    if ((blen <= SM_EAGRLMT)&&(blen >= CONBSZE)) {
        rtag = ptag + MSG_TAG_OFFSET;  /* we do this for unexpect types only */
    } else if ((blen >= SM_EAGRLMT)&&(blen >= CONBSZE)) {
        /* this is the rendevous PP setup case */

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d: %s Need to send Big MSG blen %d sm sze %d PP sze %d dn %d ptag %d mtype %d\n",
                     myidp, __func__, blen, SM_EAGRLMT, CONBSZE, destnode, ptag, mtype);

        rtag = ptag + MSG_TAG_OFFSET;  /* we do this for big msg types only, and offset the tag itself by 100 */
                                        /* in order to separate it out from the std bin types */
        sdf_msg_sendsysmsg(SYS_PP_REQ, ptag, destnode, blen, rtag);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d: %s BigBuff Request made to destnod %d of sze %d will send now rtag %d\n",
                     myidp, __func__, destnode, blen, rtag);
    } else {
        rtag = ptag;
    }

    return (rtag);
}


    /*
     * sdf_msg_sendsysmsg()
     * general send routine for sdf system type messages
     * the hdr has a different format then std sdf msgs
     * currently it is used for large msg transfers
     * cmnd = the sysmsg command type
     * proto = source and dest proto usually 0
     * dn = destination node
     * blen = the buffer length
     * rtag = protocol number
     */

static int
sdf_msg_sendsysmsg(int cmnd, int proto, int dn, int blen, int rtag) {
    sdf_sys_msg_t letsgo, *omsg;
    unsigned char *m;
    MPI_Status stat;

    stat.MPI_SOURCE = 0;
    stat.MPI_TAG = 0;
    stat.MPI_ERROR = 0;

    int sendsze = sizeof(letsgo);

    letsgo.ndbin = &node_bins[0];  /* use SDF_DEBUG bin */
    letsgo.sys_command = cmnd;     /* this is a PrePosted Buffer request */
    letsgo.msg_type = cmnd;        /* SDF_DEBUG message type */
    letsgo.buff_seq = 0;
    letsgo.msg_src_proto = proto;  /* this is the source protocol,  aka base bin number or tag */
    letsgo.msg_dest_proto = proto; /* right now just use the same protocol for both */
    letsgo.msg_send_node = bin_config.this_node; /* our local struct on this node */
    letsgo.msg_dest_node = dn;     /* where it is going to */
    letsgo.msg_mpi_type = 1;       /* always set to MPI_CHAR, we can support others */
    letsgo.msg_dtag = rtag;        /* the protocol number with the offset added */
    letsgo.msg_offset = MSG_TAG_OFFSET;
    letsgo.msg_blen = blen;        /* the buffer length */
    /* unique sendstamp id that gets incr on every send */
    letsgo.msg_req_id = __sync_fetch_and_add(&sdf_msg_rtstate->sendstamp, 1);
    omsg = &letsgo;
    m = (unsigned char *)omsg;

    /*
     * after the header is generated the request for a large PP buffer is sent
     * otherwise send an ack that we completed the large msg transfer
     * FIXME we could get stuck if there is no buff available deal with this
     */
    MPI_Send(m, sendsze, MPI_BYTE, dn, SDF_DEBUG, MPI_COMM_WORLD);

    if (DBGP1) {
        sdf_msg_dumphdr(NULL, omsg);
    }

    return (0);
}


    /*
     * sdf_msg_gcbuffs()
     * check the LL for dynamic message buffers that can be freed
     * returns the number popped from the LL
     */

int
sdf_msg_gcbuffs(void) {
    int buffcnt = -1;
    struct sdf_msg_relmsg *rtmp = NULL;

    do {
        rtmp = sdf_msg_relmsg_xlist_dequeue(&relmsg.hd, &relmsg.tl);
        buffcnt++;
        if (rtmp == NULL)
            break;           /* guess there is nothing to do */
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Freeing msg %p with flags 0x%x sn %d dn %d to LL with rtmp %p\n",
                     MeRank, rtmp->msg, rtmp->msg->msg_flags, rtmp->msg->msg_src_vnode,
                     rtmp->msg->msg_dest_vnode, rtmp);
        sdf_msg_free(rtmp->msg); /* free the msg */
        plat_free(rtmp);
    } while (1);

    return (buffcnt);
}


    /*
     * sdf_msg_buffchk()
     * check the LL for preposted buffers that have been returned to the pool
     * and post them again. It looks at each ll_cnt that was loaded and sets
     * tagbuff, which is used in the refill process. Returns the number of
     * dequeued structs it found.
     */

static int
sdf_msg_buffchk(sdf_msg_bin_init_t *node_bin) {
    int buffcnt = -1;
    sdf_msg_xchng_t *g = NULL;

    do {
        g = sdf_msg_xchng_xlist_dequeue(&node_bin->head, &node_bin->tail);
        buffcnt++;
        if (g == NULL)
            break;           /* guess there is nothing to do */
        if (DBGP1) {
            printf("Node %d: %s getit %p head %p tail %p\n", myidp, __func__,
                   g, &node_bin->head, &node_bin->tail);
            fflush(stdout);
            printf("Node %d: %s ll_cnt %d binnum %d seqnum %lu binbuff %p\n", myidp,
                   __func__, g->ll_cnt, g->binnum, g->seqnum, g->bin_buff);
            fflush(stdout);
        }
        node_bin->tagbuff[g->ll_cnt] = 1;
        if (g->msg_q_item) {
            plat_free(g->msg_q_item);
        }
    } while (buffcnt < DEF_NUM_BUFFS);

    if (buffcnt >= DEF_NUM_BUFFS) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: Error with buffer processing exceeded buffcnt %d DEF_NUM_BUFFS %d\n",
                     myidp, buffcnt, DEF_NUM_BUFFS);
        plat_assert(1);
    }
    if (DBGP) {
        printf("Node %d: %s RCVR counts LL loops %d procb %d freeb %d\n",
               node_bin->sdf_msg_rnode, __func__, buffcnt,
               node_bin->proc_buffs, node_bin->free_buffs);
    }
    node_bin->proc_buffs = node_bin->proc_buffs - buffcnt;
    node_bin->free_buffs = node_bin->free_buffs + buffcnt;
    if (DBGP) {
        printf("Node %d: %s RCVR Buff check %p getit %p proto %d buffcnt %d procb %d freeb %d\n",
               node_bin->sdf_msg_rnode, __func__, node_bin->xchng, g, node_bin->protocol_type,
               buffcnt, node_bin->proc_buffs, node_bin->free_buffs);
        fflush(stdout);
    }
    if ((DBGP)&&(buffcnt == 0)) {
        printf("Node %d: %s Empty LL No refill\n", node_bin->sdf_msg_rnode, __func__);
        fflush(stdout);
    }
    return (buffcnt);
}

   /*
     * sdf_msg_pkgb() Package up the recently received MPI PP buff info into
     * the msg header via the xchng struct. In order for the receiving thread
     * to load the proper LL so that we can recycle them back into the pool. We
     * recycle the buffers during the refill process. Each msg hdr has a
     * pointer to the node_bin from which the PP buffer came from. cnt is the
     * actual number of the buffer in the array. In the node_bin there are an
     * array of xchng structs and pointers to them. We fill in the info for
     * each struct before it's delivered. So there is a 1:1 correspondence
     * between the PP buffer and the xchng info. When the receiving thread is
     * finished with the buffer it calls sdf_msg_free_buff() enqueues the
     * xchange struct. The refill operation dequeues the LL and reclaims the
     * buffer.  This may seem a bit heavyhanded but we wanted to track more
     * than just the buffer number for future debugging purposes
     */

static sdf_msg_t *
sdf_msg_pkgb(sdf_msg_t *msg, sdf_msg_bin_init_t *b, int cnt) {

    sdf_msg_t *pmsg = msg;

    msg->ndbin = b;
    msg->buff_seq = cnt;

    b->xchng[cnt]->ll_cnt = cnt;                 /* this is the buff array num */
    b->xchng[cnt]->binnum = b->protocol_type;    /* store the bin type */
    b->xchng[cnt]->seqnum = recycd;              /* FIXME temp increasing num to track */
    b->xchng[cnt]->hd = &b->head;                /* embed the head and tail of the bins LL */
    b->xchng[cnt]->tl = &b->tail;
    b->xchng[cnt]->xflags = 0;
    /*
     * note unexpected message buffers are created on the fly and they do not
     * need to be recycled, just released, do not associate the MPI reg status
     * with this either just set the xflags since the buffer pointer has
     * already been set to the alloc'd buffer when it was created
     */
    if ((b->binid) != SDF_UNEXPECT) {
        b->xchng[cnt]->bin_buff = &b->r_buf[cnt];    /* pointer to the actual buffer */
        b->xchng[cnt]->xindx = b->indx[cnt];         /* store the index */
    } else {
        b->xchng[cnt]->xflags = (1 << 0);            /* FIXME enum this properly */
        b->xchng[cnt]->bin_buff = &b->unexp_buf[cnt];    /* pointer to the actual buffer */
    }
    if (DBGP1) {
        printf("Node %d: %s nbbuff %p buff_seq %d head %p tail %p seqnum %lu indx %d buff %p flags %d\n",
               pmsg->msg_dest_vnode, __func__, msg->ndbin, cnt, b->head, b->tail,
               b->xchng[cnt]->seqnum, b->xchng[cnt]->xindx, *b->xchng[cnt]->bin_buff, b->xchng[cnt]->xflags);
        fflush(stdout);
    }
    return (pmsg);
}

    /*
     * sdf_msg_syspkgb() Clone of the above but for the SDF_DEBUG MPI PP buff
     * info into the msg header via the xchng struct.
     */

static sdf_sys_msg_t *
sdf_msg_syspkgb(sdf_sys_msg_t *msg, sdf_msg_bin_init_t *b, int cnt) {

    sdf_sys_msg_t *pmsg = msg;

    msg->ndbin = b;
    msg->buff_seq = cnt;

    b->xchng[cnt]->ll_cnt = cnt;                 /* this is the buff array num */
    b->xchng[cnt]->binnum = b->protocol_type;    /* store the bin type */
    b->xchng[cnt]->seqnum = recycd;              /* FIXME temp increasing num to track */
    b->xchng[cnt]->hd = &b->head;                /* embed the head and tail of the bins LL */
    b->xchng[cnt]->tl = &b->tail;
    b->xchng[cnt]->xflags = 0;
    /*
     * note unexpected message buffers are created on the fly and they do not
     * need to be recycled, just released, do not associate the MPI reg status
     * with this either just set the xflags since the buffer pointer has
     * already been set to the alloc'd buffer when it was created
     */
    b->xchng[cnt]->xflags = (1 << 0);            /* FIXME enum this properly */
    b->xchng[cnt]->bin_buff = &b->unexp_buf[cnt];    /* pointer to the actual buffer */

    if (DBGP1) {
        printf("Node %d: %s nbbuff %p buff_seq %d head %p tail %p seqnum %lu indx %d buff %p flags %d\n",
               pmsg->msg_dest_node, __func__, msg->ndbin, cnt, b->head, b->tail,
               b->xchng[cnt]->seqnum, b->xchng[cnt]->xindx, *b->xchng[cnt]->bin_buff, b->xchng[cnt]->xflags);
        fflush(stdout);
    }
    return (pmsg);
}


/*
 * sdf_msg_free_buff() Called by thread processing the msg to load a buffer
 * freed notification for the static MPI pre posted receives. For NON-preposted
 * message buffers we will load the LL and garbage collect them later for the
 * caller. When messages are unexpected we flag the xchng as such.  returns 0
 * for success
 */

int
sdf_msg_free_buff(sdf_msg_t *msg) {

    struct sdf_msg_xchng *xtmp;
    sdf_msg_bin_init_t *bs;
    int i = 0;

    /*
     * for all alloc'd messages we can just release the buffer directly since
     * they are not the static type and there won't be a need for a refill
     */
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: msg %p flags 0x%x\n",
                 MeRank, msg, msg->msg_flags);

    /* check for conflicting flags setting, cannot have both a static and
     * dynamic buffer */
    plat_assert(!((msg->msg_flags & SDF_MSG_FLAG_ALLOC_BUFF) && (msg->msg_flags & SDF_MSG_FLAG_STATIC_BUFF)));

    if (msg->msg_flags & SDF_MSG_FLAG_ALLOC_BUFF) {
        sdf_msg_free(msg);
        return (0);

        /*
         * the reason for having all of the message buffers garbage collected
         * by the msg thread may not be nessary anymore and is a hassle for the
         * replication test framework we flag the alloc'd buffers for large and
         * unexpected msgs now and release directly here, however keep the
         * following around for a bit longer
         */
#ifdef maynotneed
        struct sdf_msg_relmsg *rtmp = plat_alloc(sizeof(sdf_msg_relmsg_t));
        rtmp->msg = msg;
        rtmp->cntr = msg->msg_conversation;
        rtmp->mflags = msg->msg_flags;
        sdf_msg_relmsg_xlist_enqueue(&relmsg.hd, &relmsg.tl, rtmp);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Adding alloc'd msg %p with flags 0x%x sn %d dn %d to LL with rtmp %p\n",
                     MeRank, msg, msg->msg_flags, msg->msg_src_vnode, msg->msg_dest_vnode, rtmp);
        return (0);
#endif

    } else if (msg->msg_flags & SDF_MSG_FLAG_STATIC_BUFF) {
        if ((!msg->ndbin) || (!(msg->msg_flags & SDF_MSG_FLAG_STATIC_BUFF))) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "\nNode %d: ERROR ndbin is NULL should be here %p from Node %d\n",
                         msg->msg_dest_vnode, msg->ndbin, msg->msg_src_vnode);
        }
        bs = msg->ndbin;              /* here we get the bin that the msg buffer was associated with */
        i = bs->protocol_type;        /* find the protocol */
        xtmp = bs->xchng[msg->buff_seq];    /* pull out the xchng struct and enqueue it below */
        xtmp->msg_q_item = NULL;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: flags 0x%x nodebin %p proto %d head %p tail %p\n", msg->msg_dest_vnode,
                     msg->msg_flags, bs, i, bs->head, bs->tail);
        if (msg->msg_q_item) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Static Mag Buff has q_item %p releasing it\n",
                         msg->msg_dest_vnode, msg->msg_q_item);
            xtmp->msg_q_item = msg->msg_q_item;
        }
        sdf_msg_xchng_xlist_enqueue(&bs->head, &bs->tail, xtmp);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: ENQUEUED static msg buff %p ds %d bnum %d xchng %p\n"
                     "llcnt %d bin_buff %p head %p tail %p flags 0x%x\n",
                     msg->msg_dest_vnode, msg, msg->msg_dest_service, xtmp->binnum, xtmp,
                     xtmp->ll_cnt, *xtmp->bin_buff, xtmp->hd, xtmp->tl, msg->msg_flags);
        return (0);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: ERROR msg %p has incorrect flags 0x%x bin %p\n",
                     msg->msg_dest_vnode, msg, msg->msg_flags, msg->ndbin);
        sdf_msg_dumphdr(msg, NULL);
        plat_assert(0);
    }
}

/*
 * sdf_msg_free_sysbuff() Called by SDF_DEBUG processing funcs to load a buffer freed notification for
 * the static MPI pre posted receives. For NON-preposted buffers we will just deallocate the
 * memory for the caller once they say ok. When messages are on the same node or if they are unexpected we
 * flag the xchng as such.
 */

static int
sdf_msg_free_sysbuff(sdf_sys_msg_t *msg) {

    struct sdf_msg_xchng *xtmp;
    sdf_msg_bin_init_t *bs;
    int i = 0;

    /* for SDF_DEBUG messages on the same node we just bail, never should happen */

    if (msg->msg_dest_node == msg->msg_send_node) {
        printf("Node %d: %s SDF_DEBUG msg seen on the same node... exiting\n", msg->msg_dest_node,
               __func__);
        fflush(stdout);
        plat_assert(0);
    } else {
        bs = msg->ndbin;              /* here we get the bin that the msg buffer was associated with */
        i = bs->protocol_type;        /* find the protocol */
        xtmp = bs->xchng[msg->buff_seq];    /* pull out the xchng struct and enqueue it below */
        if (DBGP1) {
            printf("Node %d: %s SDF_DEBUG nodebin %p proto %d head %p tail %p\n", msg->msg_dest_node,
                   __func__, bs, i, bs->head, bs->tail);
                fflush(stdout);
        }
        sdf_msg_xchng_xlist_enqueue(&bs->head, &bs->tail, xtmp);
        if (DBGP1) {
            printf("Node %d: %s SDF_DEBUG ENQUEUED msg buff %p ds %d bnum %d xchng %p\n",
                   msg->msg_dest_node, __func__, msg, msg->msg_dest_proto, xtmp->binnum, xtmp);
            fflush(stdout);
            printf("Node %d: %s SDF_DEBUG llcnt %d bin_buff %p head %p tail %p\n", msg->msg_dest_node,
                   __func__, xtmp->ll_cnt, *xtmp->bin_buff, xtmp->hd, xtmp->tl);
            fflush(stdout);
        }
        return (0);
    }
}

/*
 * sdf_msg_isend_log() Called after sending a large msg buffer with Isend to load the msg info
 * onto the LL, later on we check if they are gone and then we released the alloc'd buffer
 */

static int
sdf_msg_isend_log(service_t dest_service, struct sdf_msg *msg, sdf_fth_mbx_t *ackmbx) {

    sdf_msg_bin_init_t *bs;
    int i = 0;

    /* for SDF_DEBUG messages on the same node we just bail, never should happen */

    if (msg->msg_dest_vnode == msg->msg_src_vnode) {
        printf("Node %d: %s FYI Isend msg seen on the same node...\n", msg->msg_dest_vnode,
               __func__);
        fflush(stdout);
        // plat_assert(0);
    } else {
        bs = msg->ndbin;              /* here we get the bin that the msg buffer was associated with */
        bs->bbmsg = plat_alloc(sizeof(struct sdf_msg_bbmsg));
        bs->bbmsg->msg = msg;
        bs->bbmsg->cntr = bs->bbindx;
        i = bs->protocol_type;        /* find the protocol */
        if (DBGP1) {
            printf("Node %d: %s nodebin %p proto %d bbhead %p bbtail %p bbmsg %p\n",
                   msg->msg_src_vnode, __func__, bs, i, bs->bbhead, bs->bbtail,
                   bs->bbmsg);
                fflush(stdout);
        }
        sdf_msg_bbmsg_xlist_enqueue(&bs->bbhead, &bs->bbtail, bs->bbmsg);
        if (DBGP1) {
            printf("Node %d: %s ENQUEUED msg buff %p ds %d\n",
                   msg->msg_src_vnode, __func__, msg, msg->msg_dest_service);
            fflush(stdout);
        }
    }
    return (0);
}


/*
 * sdf_msg_dumphdr() In case something bad happens or you need info we print out the msg hdr info
 * covers both std hdrs and SDF_DEBUG ones
 */

static void
sdf_msg_dumphdr(sdf_msg_t *msg, sdf_sys_msg_t *bf) {

    if (bf == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "Node %d: MSG HDR msg %p version %d clusterid %d srcsvc %d destsvr %d sn %d dn %d type %d\n",
                     msg->msg_src_vnode, msg, msg->msg_version, msg-> msg_clusterid, msg->msg_src_service,
                     msg->msg_dest_service, msg->msg_src_vnode, msg->msg_dest_vnode, msg->msg_type);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "Node %d: MSG HDR seq %d flags 0x%x len %d conver %ld seqnum %ld inor %ld outor %ld nacks %ld\n",
                     msg->msg_src_vnode, msg->buff_seq, msg->msg_flags, msg->msg_len, msg->msg_conversation,
                     msg->msg_seqnum, msg->msg_in_order_ack, msg->msg_out_of_order_acks, msg->msg_out_of_order_nacks);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "Node %d: MSG HDR tmstmp %ld ambx %p rplmbx %p ndbin %p sendstmp %ld\n",
                     msg->msg_src_vnode, msg->msg_timestamp, msg->akrpmbx, msg->akrpmbx_from_req,
                     msg->ndbin, msg->msg_sendstamp);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "Node %d: MSG HDR mkey %s fthid %p q_item %p payload %p\n",
                     msg->msg_src_vnode, msg->mkey, msg->fthid, msg->msg_q_item, msg->msg_payload);


        printf("Node %d: MSG HDR msg %p version %d clusterid %d srcsvc %d destsvr %d sn %d dn %d type %d\n",
               msg->msg_src_vnode, msg, msg->msg_version, msg-> msg_clusterid, msg->msg_src_service,
               msg->msg_dest_service, msg->msg_src_vnode, msg->msg_dest_vnode, msg->msg_type);
        fflush(stdout);
        printf("Node %d: MSG HDR seq %d flags 0x%x len %d conver %ld seqnum %ld inor %ld outor %ld nacks %ld\n",
               msg->msg_src_vnode, msg->buff_seq, msg-> msg_flags, msg->msg_len, msg->msg_conversation,
               msg->msg_seqnum, msg->msg_in_order_ack, msg->msg_out_of_order_acks, msg->msg_out_of_order_nacks);
        fflush(stdout);
        printf("Node %d: MSG HDR tmstmp %ld ambx %p rplmbx %p ndbin %p sendstmp %ld\n",
               msg->msg_src_vnode, msg->msg_timestamp, msg->akrpmbx, msg->akrpmbx_from_req,
               msg->ndbin, msg->msg_sendstamp);
        fflush(stdout);
        printf("Node %d: MSG HDR mkey %s fthid %p q_item %p payload %p\n",
               msg->msg_src_vnode, msg->mkey, msg->fthid, msg->msg_q_item, msg->msg_payload);
        fflush(stdout);
    } else {
        printf("Node %d: SDF_DEBUG HDR ndbin %p cmd %d src_proto %d dest_proto %d send_node %d\n", bf->msg_send_node,
               bf->ndbin, bf->sys_command, bf->msg_src_proto, bf->msg_dest_proto, bf->msg_send_node);
        fflush(stdout);
        printf("Node %d: SDF_DEBUG HDR dest_node %d mpi_type %d dtag %d type %d offset %d blen %d req_id %lu\n",
               bf->msg_send_node, bf->msg_dest_node, bf->msg_mpi_type, bf->msg_dtag, bf->msg_type,
               bf->msg_offset, bf->msg_blen, bf->msg_req_id);
        fflush(stdout);
    }
}


/* figure out and store our hostname, read in the prop file and match the
 * hostname with the MPI rank */
int
sdf_msg_getnodename(struct sdf_msg_bin_list *myndstate) {
    char prop_node[SDF_MSG_MAX_NODENAME];
    const char *crp;

    int ret = gethostname(myndstate->mynodename, sizeof(myndstate->mynodename));
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nMPI ID %d: my node is %s ret %d\n", myndstate->this_node, myndstate->mynodename, ret);

    for (int p = 0; p < MAX_NUM_SCH_NODES; p++) {
        snprintf(prop_node, 128, "NODE[%u].CLUSTER", p);
        crp = getProperty_String(prop_node, SDF_MSG_DFLT_PROPHOST);
        strncpy(myndstate->clustnames[p], crp,  SDF_MSG_MAX_NODENAME);
        /* mpi init has told us who we are, so now who are we really */
        if (myndstate->node_array[p]) {
            for (int q = 0; q < MAX_NUM_SCH_NODES; q++) {
                int ret = strncmp(myndstate->clustnames[p], myndstate->mynodename, SDF_MSG_MAX_NODENAME);
                if (!ret) {
                    myndstate->schoon_map[q] = myndstate->this_node;
                }
            }
        }
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\nMPI ID %d: SDF Property File Cluster Map of Schooner IDs\n"
                 "        node_name[%d] = %s\n"
                 "        node_name[%d] = %s\n"
                 "        node_name[%d] = %s\n"
                 "        node_name[%d] = %s\n", myndstate->this_node,
                 0, myndstate->clustnames[0],
                 1, myndstate->clustnames[1],
                 2, myndstate->clustnames[2],
                 3, myndstate->clustnames[3]);

    return (0);

}


/*
 * Send a message.
 */
static void
call_send(int sdfno, void *buf, int len, int stag, int dtag)
{
    nno_t nno = msg_map_send(sdfno);
    msg_send_t *send = msg_salloc();

    if (nno < 0)
        fatal("call_send: node map %d => %d", sdfno, nno);
    send->stag = stag;
    send->dtag = dtag;
    send->sid  = SendId++;
    send->nno  = nno;
    send->nsge = 1;
    send->sge[0].iov_base = buf;
    send->sge[0].iov_len  = len;
    msg_send(send);
}


/*
 * Wait for the last message to have been sent.
 */
static void
wait_send(void)
{
    msg_info_t want ={
        .type = MSG_ESENT,
        .mid  = SendId-1
    };
    msg_info_t *info = msg_want(-1, &want, NULL);
    msg_ifree(info);
}


/*
 * Liveness callback function.
 */
static void
live_back(int live, int rank, void *arg)
{
    t_smsg("live_back: %s rank=%d", live ? "LIVE" : "DEAD", rank);
    if (!live)
        sdf_msg_int_dead_responses(rank);
}


/*
 * Cause a node to drop and log it appropriately.
 */
static void
drop_node(nno_t nno)
{
    logi("bad data from n%d(%d); dropped", msg_map_recv(nno), nno);
    msg_nodedrop(nno);
}
