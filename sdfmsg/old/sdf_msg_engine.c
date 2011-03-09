/*
 * File:   sdf_msg_engine.c
 * Author: Tom Riddle
 *
 * Created on February 20, 2008, 7:45 AM
 * Revised on April 21, 2008 12:17 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_engine.c 308 2008-04-21 22:34:58Z tomr $
 */

#ifdef MPI_BUILD
#include <mpi.h>
#endif


#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>

#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/fcntl.h"
#include "platform/string.h"
#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "platform/errno.h"
#include "common/sdftypes.h"

#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_types.h"

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG
extern msg_state_t *sdf_msg_recv_incoming(struct msg_state *);  /* get incoming msgs - read the bins and deliver */
extern int sdf_msg_transport(void);                             /* walk the queue tree and make the sends  */
extern void* sdf_msg_tester_start(void* arg);                   /* call to start the msg mngmt thread */
extern int sdf_msg_exitall(uint32_t myid, int flags);           /* shutdown all messaging and exernal agents */
extern struct bin_list *ndstate;                                /* bin config / general node connectivity status */
extern int sdf_msg_closequeues(void);

msg_state_t *sdf_msg_rtstate = NULL; /* global runtime state of the message engine */
msg_srtup_t *msg_srt;                /* when messaging starts up we use this struct to sync threads */
uint32_t allow_posts = 0;            /* flag to allow queue posts without locking */


#define CNTIT 9000000
#define NUM_MSG_THREADS 2
#define SDF_MSG_MAXSTARTUPTIME 6 /* .5 sec timeout globally defined so give the msg thread 3 total secs */

static pthread_t threads[NUM_MSG_THREADS];
static pthread_attr_t attr;

static int show_stats = 0;

static void* sdf_msg_engine_run(void* arg);

static uint64_t tms_old;
static int thisid;                   /* this is just here for local print node identity */
static int timetogo = 0;

/**
 * @brief sync function for external nodes and useage for upper level
 * fththreads.  input are the nodeid of the caller and whether they are waiting
 * or releasing.  the current useage is for the cmc container create on init.
 * The slaves (non CMC_HOME nodes) will wait for the cmc_home to release them
 * by calling with SDF_MSG_BARWAIT. It will be the assumption that the CMC_HOME
 * will take longer to init and the slaves will be waiting for the the CMC_HOME
 * to call with SDF_MSG_BARREL.  After the above all slaves will call with
 * SDF_MSG_BARCIW which sends notifications to the CMC_HOME and waits for a
 * response. The CMC_HOME listens for all of the slaves and then will release
 * them and continue itself.
 */

#define TSZE 16

int
sdf_msg_barrier(int mynode, int flags) {
    struct sdf_queue_pair *qpair_BARRIER;
    service_t protocol = GOODBYE;
    struct sdf_msg *send_msg[MAX_NUM_SCH_NODES];
    struct sdf_msg *recv_msg = NULL;
    msg_type_t type = GOODBYE;
    sdf_fth_mbx_t fthmbx;
    fthMbox_t *fthmbxtst;
    fthMbox_t ackmbox, respmbox;
    int ret = 1;
    int cluster_node[MAX_NUM_SCH_NODES];
    uint64_t aresp;

    fthmbxtst = &respmbox;

    fthMboxInit(&ackmbox);
    fthMboxInit(&respmbox);

    fthmbx.actlvl = SACK_ONLY_FTH;
    fthmbx.abox = &ackmbox;
    fthmbx.rbox = &respmbox;
    fthmbx.aaction = NULL;
    fthmbx.raction = NULL;
    fthmbx.release_on_send = 1;
    int node = 1;
    int localpn, actmask;
    uint32_t numprocs;

    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    plat_assert(localrank == mynode);
    if (numprocs == 1) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d: Single Node SDF Barrier call will ignore this request - flags 0x%x\n",
                     localrank, flags);
        return (0);
    }

    /* queues get release at msg exit */

    if (flags == SDF_MSG_BARWAIT) { /* slaves just wait for CMC_HOME release */
        plat_assert(mynode != CMC_HOME);
        qpair_BARRIER = (sdf_create_queue_pair(mynode, CMC_HOME, protocol,
                                               protocol, SDF_WAIT_FTH));
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode ID %d: SDF Barrier Slave WAIT - flags 0x%x\n",
                     mynode, flags);
        /* will block until we get a msg on the GOODBYE queue from the home node */
        recv_msg = sdf_msg_receive(qpair_BARRIER->q_out, 0, B_TRUE);
        ret = sdf_msg_free_buff(recv_msg);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode ID %d: SDF_MSG_BARWAIT = Barrier Slave WAIT - COMPLETE\n",
                     mynode);
    } else if (flags == SDF_MSG_BARREL) { /* CMC_HOME sends release msg to all waiting slaves to continue */
        plat_assert(mynode == CMC_HOME);
        qpair_BARRIER = (sdf_create_queue_pair(CMC_HOME, node, protocol,
                                               protocol, SDF_WAIT_FTH));
        /*
         * FIXME: Extremely temporary fix to get around race condition.
         */
        sleep(5);
        for (int j = 1; j < numprocs; j++) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode ID %d: SDF Barrier CMC_HOME RELEASE to node %d - flags 0x%x numprocs %d\n",
                         mynode, j, flags, numprocs);
            send_msg[j] = (struct sdf_msg *)sdf_msg_alloc(TSZE);
            for (int i = 1; i < TSZE; ++i) {
                send_msg[j]->msg_payload[i] = (unsigned char) i;
            }
            ret = sdf_msg_send((struct sdf_msg *)send_msg[j], TSZE, j, protocol,
                               mynode, protocol, type, &fthmbx, NULL);
            aresp = fthMboxWait(&ackmbox);
        }
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode ID %d: SDF_MSG_BARREL = CMC_HOME release - COMPLETE\n",
                     mynode);
    } else if (flags == SDF_MSG_BARCIW) { /* slaves send msg that they have arrived and wait for release */
        plat_assert(mynode != CMC_HOME);
        qpair_BARRIER = (sdf_create_queue_pair(mynode, CMC_HOME, protocol,
                                               protocol, SDF_WAIT_FTH));
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode ID %d: SDF Barrier Slave CHECK IN & WAIT sending to Node %d - flags 0x%x\n",
                     mynode, CMC_HOME, flags);
        send_msg[mynode] = (struct sdf_msg *)sdf_msg_alloc(TSZE);
        for (int i = 1; i < TSZE; ++i) {
            send_msg[mynode]->msg_payload[i] = (unsigned char) i;
        }
        ret = sdf_msg_send((struct sdf_msg *)send_msg[mynode], TSZE, CMC_HOME, protocol,
                           mynode, protocol, type, &fthmbx, NULL);
        aresp = fthMboxWait(&ackmbox);
        /* will block until we get a msg on the GOODBYE queue from the home node */
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode ID %d: SDF Barrier SLAVE CHECK IN & WAIT waiting to continue- flags 0x%x\n",
                     mynode, flags);
        recv_msg = sdf_msg_receive(qpair_BARRIER->q_out, 0, B_TRUE);
        ret = sdf_msg_free_buff(recv_msg);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode ID %d: SDF_MSG_BARCIW = Barrier Slave CI&WAIT - COMPLETE\n",
                     mynode);
    } else if (flags == SDF_MSG_BARCIC) { /* CMC_HOME waits till it see all active slaves report then says go */
        plat_assert(mynode == CMC_HOME);
        qpair_BARRIER = (sdf_create_queue_pair(CMC_HOME, node, protocol,
                                               protocol, SDF_WAIT_FTH));
        for (int j = 1; j < numprocs; j++) {
            recv_msg = sdf_msg_receive(qpair_BARRIER->q_out, 0, B_TRUE);
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode ID %d: SDF Barrier CMC_HOME got arrival msg from node %d - flags 0x%x\n",
                         mynode, recv_msg->msg_src_vnode, flags);
            ret = sdf_msg_free_buff(recv_msg);
        }
        for (int j = 1; j < numprocs; j++) {
            send_msg[j] = (struct sdf_msg *)sdf_msg_alloc(TSZE);
            for (int i = 1; i < TSZE; ++i) {
                send_msg[j]->msg_payload[i] = (unsigned char) i;
            }
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode ID %d: SDF Barrier CMC_HOME sends continue msg to node %d - flags 0x%x\n",
                         mynode, recv_msg->msg_src_vnode, flags);
            ret = sdf_msg_send((struct sdf_msg *)send_msg[j], TSZE, j, protocol,
                               mynode, protocol, type, &fthmbx, NULL);
            aresp = fthMboxWait(&ackmbox);
        }
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode ID %d: SDF_MSG_BARCIC = CMC_HOME WAIT for ALL - COMPLETE\n",
                     mynode);
    } else {
        return (1);
    }
    sdf_delete_queue_pair(qpair_BARRIER);
    return (ret);

}

/* allocate and setup runtime state early on, deallocate on shutdown */

int
sdf_msg_setup_rtstate(vnode_t myid) {
    struct timeval now;

    /* runtime status for messaging and delivery statistics */
    sdf_msg_rtstate = plat_alloc(sizeof (msg_state_t));
    if (sdf_msg_rtstate == NULL) {
        plat_assert(sdf_msg_rtstate == NULL);
    }
    sdf_msg_rtstate->mtime = plat_alloc(sizeof(struct msg_timeout));
    if (sdf_msg_rtstate->mtime == NULL) {
        plat_assert(sdf_msg_rtstate->mtime == NULL);
    }
    gettimeofday(&now, NULL);

    sdf_msg_rtstate->mtime->rseqnum = 0;
    sdf_msg_rtstate->mtime->tmout = SDF_MSG_SECTIMEOUT; /* default timeout for resp msgs */
    for (int i = 0; i < MCNTS; ++i) {
        sdf_msg_rtstate->mtime->mcnts[i] = 0;
    }

    /* set the timemkr to now, all msgs sent till the first interval check will be just C_CNT */
    sdf_msg_rtstate->mtime->timemkr = now.tv_sec;
    sdf_msg_rtstate->mtime->mstimemkr = now.tv_usec;
    sdf_msg_rtstate->mtime->ptimemkr = 0;
    sdf_msg_rtstate->mtime->ntimemkr = 0;
    sdf_msg_rtstate->mtime->actflag = 0;

    sdf_msg_rtstate->sdf_msg_runstat = 0;

    msg_srt = plat_alloc(sizeof (msg_srtup_t));

    if (msg_srt == NULL) {
        plat_assert(msg_srt == NULL);
    }
    sdf_msg_rtstate->msg_startup = msg_srt; /* reference the startup state in the global state */
    sdf_msg_rtstate->cstate = ndstate;
    sdf_msg_rtstate->myid = myid;
    sdf_msg_rtstate->resp_n_flight = 0;     /* zero out the responses in flight here, gets incr during sends */
    sdf_msg_rtstate->respcntr = 0;          /* zero out the monotonically increasing resp UID for sends */
    sdf_msg_rtstate->msg_been_enabled = 0;  /* we will enable this when the msg engine get's going */
    sdf_msg_rtstate->qtotal = 0;
    sdf_msg_rtstate->sendstamp = 0;
    sdf_msg_rtstate->recvstamp = 0;
    sdf_msg_rtstate->gc_count = 0;

    for (int i = 0; i < MAX_QCOUNT; i++) {
        sdf_msg_rtstate->qstate[i] = NULL;
    }
    for (int i = 0; i < MAX_NUM_BINS; i++) {
        sdf_msg_rtstate->msg_delivered[i] = 0;
    }
    return (0);
}


/* This starts the messaging pthreads. One is for the SDF_SYSTEM bin, messaging engine */

static int
startUpMsgng(void *nodeid, int msgflags)
{
    uint32_t *myid = (uint32_t *)nodeid;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    pthread_create(&threads[0], &attr, sdf_msg_engine_run, (uint32_t *)nodeid);

    if (msgflags & SDF_MSG_RTF_DISABLE_MNGMT) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                     "\nNode ID %d SDF Msg Management Thread Will Not Been Started - flags 0x%x\n",
                     *myid, msgflags);
    } else {
        pthread_create(&threads[1], &attr, sdf_msg_tester_start, (uint32_t *)nodeid);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                     "\nNode ID %d SDF Msg Management Thread Has Been Started - flags 0x%x\n",
                     *myid, msgflags);
    }

    return (0);
}

/**
 * @brief sdf_msg_startmsg() releases the messaging thread who is polling the
 * msg_run_release in order to start reading the queues for sends and the bins
 * for receives, flags are unused right now sdf_dispatch will be registered as
 * a callback when sdf_msg_stopmsg() is envoked @returns 0 if thread is
 * successfully released, 1 if we're in single node, no msg enabled
 */

int
sdf_msg_startmsg(uint32_t thisid, uint32_t flags, void *sdf_dispatch(void *)) {

    if (flags & SDF_MSG_DISABLE_MSG) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: INFO - No messaging thread will be started - flags 0x%x\n", thisid, flags);
        return (1);
    }
    pthread_mutex_lock(&msg_srt->msg_release);
    int tmp = msg_srt->msg_run_release = SDF_MSG_STARTUP_REQ;
    pthread_mutex_unlock(&msg_srt->msg_release);
    /*
     * now wait for an a ok, we will timeout if the threads don't start this is
     * a bit heavy handed as using gettimeofday would have been fine but did
     * the ns thing for fun
     */
    msgtime_t timeout_base = get_the_nstimestamp();
    msgtime_t timeout[3] = {0, 0, 0};
    while (timeout[2] < SDF_MSG_MAXSTARTUPTIME) {
        pthread_mutex_lock(&msg_srt->msg_release);
        if (msg_srt->msg_run_release == SDF_MSG_STARTUP_DONE) {
            pthread_mutex_unlock(&msg_srt->msg_release);
            break;
        }
        pthread_mutex_unlock(&msg_srt->msg_release);
        timeout[1] = show_howlong(timeout_base, 0, timeout);
        if (timeout[0] > SDF_MSG_NSTIMEOUT) {
            timeout[2]++;
            timeout[0] = 0;
        }
    }
    if (timeout[2] > SDF_MSG_MAXSTARTUPTIME) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: msg thread failed to start - flags 0x%x fth %p?\n"
                     "       tmp %d timeout_base %lu elaptime %lu\n",
                     thisid, flags, fthId(), tmp, timeout_base, timeout[0]);
        plat_assert(0);
        return (1);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                     "\nNode %d: msg thread up - flags 0x%x started by fth %p?\n"
                     "     tmp %d timeout_base %lu elaptime %lu count %lu\n",
                     thisid, flags, fthId(), tmp, timeout_base, timeout[0], timeout[2]);
    }

    (fthId() ?  fthYield(1) : sched_yield()); /* yield on either pthread or fth */

    return (0);
}

/*
 * @brief sdf_msg_stopmsg() shutdown and cleanup of the messaging thread.
 * General stoplvls are SYS_SHUTDOWN_SELF which will only cleanup th enode
 * itself SYS_SHUTDOWN_ALL which will send a terminating message to all of the
 * other nodes besides itself both will call the provided sdf_dispatch() Note
 * that deallocating of the queues is done when the messaging thread is closed
 * out.
 */

int
sdf_msg_stopmsg(uint32_t thisid, uint32_t stoplvl)
{
    int i;

    if (stoplvl == SYS_SHUTDOWN_ALL) {
        sdf_msg_exitall(thisid, stoplvl);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: SHUTDOWN ALL joining the messaging threads stoplvl %d\n",
                     thisid, stoplvl);
    } else if (stoplvl == SYS_SHUTDOWN_SELF) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "\nNode %d Call to exit the messaging threads stoplvl %d\n",
                     thisid, stoplvl);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d invalid stoplvl %d  returning...\n",
                     thisid, stoplvl);
        return (SYS_SHUTDOWN_ERR);
    }
    /*
     * FIXME crude ungraceful stop for now, plan is to send a SDF_SYSTEM
     * message to the messaging thread via the stoplvl to allow different
     * levels or options of termination. Will do this shortly.
     *
     * NOTE: don't forget to dealloc the following items sdf_msg_rtstate, queue
     * pairs, hash structs and hash table
     */

    pthread_mutex_lock(&msg_srt->msg_release);
    msg_srt->msg_run_timetogo = stoplvl;
    pthread_mutex_unlock(&msg_srt->msg_release);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_INFO,
        "\nNode %d: got mutex, wrote stoplvl %d\n", thisid, stoplvl);
    for (i = 0; i < 1; i++) {
        pthread_cancel(threads[i]);
        pthread_join(threads[i], NULL);
    }
    pthread_attr_destroy(&attr);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d Cancelled the pthreads -- num was %d\n", thisid, i+1);
    pthread_mutex_destroy(&msg_srt->msg_release);
    return (0);
}

/* Lock this process to the lowest number cpu available and enable 2 cores to run threads */
int
lock_processor(uint32_t firstcpu, uint32_t numcpus) {
    cpu_set_t cpus;
    int i;

/*
 * FIXME need to be a bit more intelligent about recognizing the number of cores in a system
 * but for now we assume we have at least 2 cores available on any machine we do this on
 * and use this call only for the sdfmsg unit tests
 */
    if (!numcpus) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: No processors locked... numcpus %d\n",
                     thisid, numcpus);
        return (0);
    }
    CPU_ZERO(&cpus);

    if ((numcpus + firstcpu) > 7) {
        numcpus = 2;
        firstcpu = 0;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: Invalid CPU count... numcpus now %d and First CPU is %d\n",
                     thisid, numcpus, firstcpu);
    }
    for (i = firstcpu; i < (numcpus + firstcpu); ++i) {
        CPU_SET(i, &cpus);
    }
    (void) sched_setaffinity(0, sizeof(cpus), &cpus);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Completed locking cores... 1st cpu is %d total locked %d\n",
                 thisid, firstcpu, numcpus);
    return (0);
}

/* check the timeouts and print some stats if asked */

static int
poll_housekeeping(void *arg, uint32_t thisid1) {

    int *loopcount = (int *)arg;

    if (*loopcount/CNTIT) {
        if (0)
            sdf_msg_chktimeout();
        if (!show_stats) {
            /* DEBUG just spit out the registered queue info */
            sdf_msg_qinfo();
            show_stats = 1;
        }
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d Did a Loop - msg_runstat %d cnt %d\n",
                     thisid1, sdf_msg_rtstate->sdf_msg_runstat, *loopcount);
        sched_yield(); /* */
        return (0);
    } else {
        return (*loopcount);
    }
}

/*
 * main pthread for the messaging engine kicked off from above --
 * startUpMsgng() it will first make a call to initialize the mpi bins / ping
 * available nodes, create master node list next grab a lock and set the flag
 * that the msg_init is complete, then loop till the app tells the thread to
 * begin checking bins and queues. sdf_msg_runstat is used as flow control.
 */

void*
sdf_msg_engine_run(void* arg) {

    uint32_t *nodeid = (uint32_t *)arg;
    uint32_t ndid = *nodeid;
    int excntr = 0;
    int node_status = 0, msgs_sent = 0;
    char msgseq[5][128];

    /*
     * Establish Infinipath MPI connections with all other node SDF engines
     * Setup statically allocated message bins on each node
     * node_status is 0 if we're in multi-node, 1 if we're in single node
     */

    node_status = sdf_msg_init_bins(ndid);
    if (node_status) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                     "\nNode %d: SINGLE NODE MESSAGING ONLY - node_status %d\n",
                     ndid, node_status);
    }


    /* notify other waiting threads (if any) that bin_init is done and they can
     * get on with it */
    pthread_mutex_lock(&msg_srt->msg_release);
    msg_srt->msg_mpi_release++;
    pthread_mutex_unlock(&msg_srt->msg_release);

    /* loop until we're allowed to start... then set the run flags and start
     * sends/recvs */
    while (1) {
        pthread_mutex_lock(&msg_srt->msg_release);
        if (msg_srt->msg_run_release == SDF_MSG_STARTUP_REQ) {
            allow_posts = SDF_MSG_STARTUP_DONE;
            /* this will allow posting to queues */
            sdf_msg_rtstate->msg_been_enabled = SDF_MSG_STARTUP_DONE;
            pthread_mutex_unlock(&msg_srt->msg_release);
            break;
        }
        pthread_mutex_unlock(&msg_srt->msg_release);
    }

    strcpy(msgseq[1], "Time to listen");
    strcpy(msgseq[2], "Time to do Sends");
    strcpy(msgseq[3], "Time to Check Bins");
    strcpy(msgseq[4], "Time to Check for Mutex Stop");

    while (1) {
        tms_old = get_the_nstimestamp();

        /* check for response msg timeouts and dump stats if needed */
        excntr = poll_housekeeping(&excntr, ndid);
        excntr++;

        /* Here we check for the sends */
        if (!sdf_msg_rtstate->sdf_msg_runstat) {
            msgs_sent = sdf_msg_transport();
        } else {
            if (excntr / CNTIT) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d Hold the Sends - msg_runstat %d\n",
                             ndid, sdf_msg_rtstate->sdf_msg_runstat);
            }
        }
        /* garbage collect the msg buffers requested to be freed by the clients, keep track of stats */
        int gc_count = sdf_msg_gcbuffs();
        sdf_msg_rtstate->gc_count += gc_count;

        /*
         * Here we look for messages that have come in and deliver them
         * node_status is set on sdf_msg_bin_init to alert us of single node
         * fastpath so that messages get sent but skip the bin check or else
         * you'll fault under MPI
         */
        if (!node_status) {
            sdf_msg_rtstate = sdf_msg_recv_incoming(sdf_msg_rtstate);
        }

        /* check to see if we are requested to shut the msg thread down */
        pthread_mutex_lock(&msg_srt->msg_release);
        msg_srt->msg_run_release = SDF_MSG_STARTUP_DONE;
        if (msg_srt->msg_run_timetogo) {
            pthread_mutex_unlock(&msg_srt->msg_release);
            break;
        }
        pthread_mutex_unlock(&msg_srt->msg_release);
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                 "\nNode %d: closing active queues...\n", ndid);
    sdf_msg_closequeues();
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                 "\nNode %d: exiting msg_engine timetogo %d\n", ndid, timetogo);
    //MPI_Barrier(MPI_COMM_WORLD);

    return 0;
}

/**
 * @brief sdf_msg_init() start of the base messaging thread and msg monitor
 * thread initialization and global state msg_init_flags are to indicate some
 * details of the desired init options. blocks until complete @returns 0 for
 * success, 1 for failure
 */

bin_t
sdf_msg_init(uint32_t myid, int *pnodeid, int msg_init_flags) {
    uint32_t numprocs;
    int localpn = -1, actmask, cluster_node_array[MAX_NUM_SCH_NODES];

    /* We grab the rank et all, that was set in the earlier sdf_msg_init_mpi */

    int crp = pthread_mutex_init(&msg_srt->msg_release, NULL);
    /* begin the process of starting the messaging threads */
    crp = pthread_mutex_lock(&msg_srt->msg_release);
    msg_srt->msg_mpi_release = 0;
    msg_srt->msg_sys_release = 0;

    /* if we're not starting up the sdf msg management thread then we just set
     * the flag as done already */
    if (msg_init_flags & SDF_MSG_RTF_DISABLE_MNGMT) {
        msg_srt->msg_sys_release++;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode ID is %d: SDF MSG management thread not created"
                                    " - flags 0x%x\n", myid, msg_init_flags);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode ID is %d: SDF MSG management enabled - flags 0x%x\n",
                                                        myid, msg_init_flags);
    }
    msg_srt->msg_run_timetogo = 0;
    msg_srt->msg_run_release = 0;
    crp = pthread_mutex_unlock(&msg_srt->msg_release);

    /* If the props file was set but no command line given run the messaging
     * thread */
    if (msg_init_flags & SDF_MSG_NO_MPI_INIT) {
        if (msg_init_flags & SDF_MSG_SINGLE_NODE_MSG) {
            startUpMsgng(&myid, msg_init_flags);
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode ID %d No MPI Local Messaging Started...\n", myid);
        } else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode ID %d SINGLE NODE, NON-MPI MODE, no local messaging\n",
                myid);
            return 0;
        }
    } else
        startUpMsgng(&myid, msg_init_flags);

    crp = 0;

    /* now we wait for the two threads to complete initialization */
    int cntr = 0;
    while (1) {
        crp = pthread_mutex_lock(&msg_srt->msg_release);
        if ((!msg_srt->msg_mpi_release)||(!msg_srt->msg_sys_release)) {
#if 0
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode ID %d: Messenging Threads init stage %d... mpi_r %d sys_r %d\n",
                         myid, cntr, msg_srt->msg_mpi_release, msg_srt->msg_sys_release);
#endif
        } else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode ID %d: Messaging Threads Ready to Go! stage %d mpi_r %d sys_r %d\n",
                         myid, cntr, msg_srt->msg_mpi_release, msg_srt->msg_sys_release);
            crp = pthread_mutex_unlock(&msg_srt->msg_release);
            break;
        }
        crp = pthread_mutex_unlock(&msg_srt->msg_release);
        cntr++;
        sched_yield();
        usleep(100000);
    }
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node_array, &actmask);
    *pnodeid = localpn;
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Schooner Node ID %d activenodes 0x%x nodemask 0x%x\n", myid,
                 localrank, *pnodeid, actmask);
    fflush(stdout);
    return (0);

}

msgtime_t
show_howlong(msgtime_t oldtm, int n, msgtime_t *tsttm_array) {
    /* What environment variable needs to be set to get clock_gettime() */
    struct timespec curtime;
    uint64_t difftm;

    if (oldtm == 0) {
        oldtm = get_the_nstimestamp();
    }
    /* FIXME perhaps we should use CLOCK_MONOTONIC so nothing changes underneath us? */
    (void) clock_gettime(CLOCK_REALTIME, &curtime);
    /* check rollover */
    if (curtime.tv_nsec < oldtm) {
        difftm = (1000000000 + curtime.tv_nsec) - oldtm;
    } else {
        difftm = curtime.tv_nsec - oldtm;
    }
    if (tsttm_array) {
        tsttm_array[n] = difftm;
    }
    if (0) {
        printf("\nNode %d: Seq %d - ELAPSED TIME new %li old %li diff %li\n",
               thisid, n, curtime.tv_nsec, oldtm, difftm);
    }
    return (curtime.tv_nsec);
}
