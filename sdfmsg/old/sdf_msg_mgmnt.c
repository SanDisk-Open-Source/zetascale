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
 * File: sdf_msg_mgmnt.c
 * Author: Tom Riddle
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * $Id: sdf_msg_mgmnt.c 308 2008-02-20 22:34:58Z tomr $
 */
#ifdef MPI_BUILD
#include <mpi.h>
#endif
#include <sched.h>
#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include "sdf_msg_int.h"
#include "sdf_msg_mpi.h"
#include "sdf_fth_mbx.h"
#include "sdf_msg_hmap.h"
#include "sdf_msg_types.h"
#include "sdf_msg_wrapper.h"
#include "platform/logging.h"
#include "utils/properties.h"
#include "sdftcp/msg_trace.h"


#define TSZE 64
#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG


extern msg_srtup_t *msg_srt;                /* global startup sync for SYS
                                               mangmt thread init */
extern msg_state_t *sdf_msg_rtstate;        /* during runtime track simple msg
                                               engine state, allow sends */
extern struct SDFMSGMap respmsg_tracker;    /* the response hash table */

static service_t my_protocol = SDF_SYSTEM;
static int cluster_node[MAX_NUM_SCH_NODES];


/*
 * construct the err message and post it to the thread whose waiting
 * XXX 2009-01-23 tomr, currently the replication layer handles this properly
 * but upper levels after that do not
 */
static int
sdf_msg_dotimeouterr(SDFMSGMapEntry_t *hresp, SDF_status_t error_type) {
    struct sdf_msg *msg;
    struct sdf_msg_error_payload *payload;
    struct sdf_resp_mbx mresp;

    msg = sdf_msg_alloc(sizeof (*payload));
    if (!msg)
        fatal("sdf_msg_alloc failed");
    payload = (struct sdf_msg_error_payload *)&msg->msg_payload;
    payload->error = error_type;

    sdf_msg_initmresp(&mresp);
    mresp.rbox = hresp->contents->ar_mbx;
    sdf_msg_init_synthetic(msg, sizeof (*payload),
        hresp->contents->msg_dest_vnode, hresp->contents->msg_dest_service,
        hresp->contents->msg_src_vnode, hresp->contents->msg_src_service,
        SDF_MSG_ERROR, NULL, &mresp);

    /* Populate back the info from the hashed msg data */
    msg->msg_flags = hresp->contents->msg_flags;
    msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_INCLUDED;
    msg->msg_flags &= ~SDF_MSG_FLAG_MBX_RESP_EXPECTED;
    msg->msg_flags &= ~SDF_MSG_FLAG_MBX_SACK_EXPECTED;
    msg->msg_flags &= ~SDF_MSG_FLAG_FREE_ON_SEND;
    msg->msg_seqnum = hresp->contents->msg_seqnum;
    msg->mkey[MSG_KEYSZE - 1] = '\0';

    /* Grab the key from the hashed data */
    strcpy(msg->mkey, hresp->contents->mkey);

    /* Fail if we can't deliver the error */
    if (sdf_do_receive_msg(msg)) {
        fatal("ERROR delivering timeout msg resp_msg_flags 0x%x msg_type %d",
                            msg->msg_flags, msg->msg_type);
    }

    sdf_msg_rtstate->mtime->mcnts[PC_CNT]--;
    return 0;
}


/* the intervals are checked and updated upon a move from previous timestamp +
 * the timeout value  */
static int
sdf_msg_updatetimechk(struct msg_timeout *mtime) {
    struct timeval now;

    int status = gettimeofday(&now, NULL);
    /* first time through */
    if (!sdf_msg_rtstate->mtime->actflag) {
        sdf_msg_rtstate->mtime->actflag = 1;
        sdf_msg_rtstate->mtime->ptimemkr = sdf_msg_rtstate->mtime->timemkr;
        sdf_msg_rtstate->mtime->timemkr = now.tv_sec;
        sdf_msg_rtstate->mtime->mstimemkr = now.tv_usec;
        sdf_msg_rtstate->mtime->ntimemkr = sdf_msg_rtstate->mtime->timemkr + sdf_msg_rtstate->mtime->tmout;

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: INITIALIZE tmout %d ntimemkr %lu\n"
                     "        timemkr %lu usec %lu ptimemkr %lu diff %lu ne %d\n"
                     "        PC_CNT %d, PA_CNT %d, PN_CNT %d, C_CNT %d, A_CNT %d, N_CNT %d\n",
                     sdf_msg_rtstate->myid,
                     sdf_msg_rtstate->mtime->tmout, sdf_msg_rtstate->mtime->ntimemkr,
                     sdf_msg_rtstate->mtime->timemkr,
                     sdf_msg_rtstate->mtime->mstimemkr, sdf_msg_rtstate->mtime->ptimemkr,
                     sdf_msg_rtstate->mtime->diffmkr, SDFMSGMapNEntries(&respmsg_tracker),
                     sdf_msg_rtstate->mtime->mcnts[PC_CNT],
                     sdf_msg_rtstate->mtime->mcnts[PA_CNT],
                     sdf_msg_rtstate->mtime->mcnts[PN_CNT],
                     sdf_msg_rtstate->mtime->mcnts[C_CNT],
                     sdf_msg_rtstate->mtime->mcnts[A_CNT],
                     sdf_msg_rtstate->mtime->mcnts[N_CNT]);

        return (0);
    }
    /* time to do an interval update */
    if ((now.tv_sec - (sdf_msg_rtstate->mtime->tmout - 1)) > sdf_msg_rtstate->mtime->timemkr) {
        /* right now we just set the timeout once on startup but we can vary this in runtime if needed */
        pthread_mutex_lock(&msg_srt->msg_release);
        sdf_msg_rtstate->mtime->tmout = msg_srt->msg_timeout;
        pthread_mutex_unlock(&msg_srt->msg_release);

        sdf_msg_rtstate->mtime->ptimemkr = sdf_msg_rtstate->mtime->timemkr;
        sdf_msg_rtstate->mtime->timemkr = now.tv_sec;
        sdf_msg_rtstate->mtime->mstimemkr = now.tv_usec;
        /* set the next interval time */
        sdf_msg_rtstate->mtime->ntimemkr = sdf_msg_rtstate->mtime->timemkr + sdf_msg_rtstate->mtime->tmout;
        /* store the difference */
        sdf_msg_rtstate->mtime->diffmkr = sdf_msg_rtstate->mtime->timemkr - sdf_msg_rtstate->mtime->ptimemkr;
        /* move any of the previous interval's remaining counts to the timeout count PC_CNT */
        sdf_msg_rtstate->mtime->mcnts[PC_CNT] = sdf_msg_rtstate->mtime->mcnts[C_CNT] +
            sdf_msg_rtstate->mtime->mcnts[PA_CNT] + sdf_msg_rtstate->mtime->mcnts[PN_CNT];
        sdf_msg_rtstate->mtime->mcnts[PA_CNT] = sdf_msg_rtstate->mtime->mcnts[A_CNT];
        sdf_msg_rtstate->mtime->mcnts[PN_CNT] = sdf_msg_rtstate->mtime->mcnts[N_CNT];
        /* zero out counts for this new interval */
        sdf_msg_rtstate->mtime->mcnts[C_CNT] = 0;
        sdf_msg_rtstate->mtime->mcnts[A_CNT] = 0;
        sdf_msg_rtstate->mtime->mcnts[N_CNT] = 0;

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: tmout %d\n"
                     "        timemkr %lu ntimemkr %lu ptimemkr %lu diff %lu\n"
                     "        PC_CNT %d, PA_CNT %d, PN_CNT %d, C_CNT %d, A_CNT %d, N_CNT %d\n",
                     sdf_msg_rtstate->myid,
                     sdf_msg_rtstate->mtime->tmout,
                     sdf_msg_rtstate->mtime->timemkr,
                     sdf_msg_rtstate->mtime->ntimemkr, sdf_msg_rtstate->mtime->ptimemkr,
                     sdf_msg_rtstate->mtime->diffmkr,
                     sdf_msg_rtstate->mtime->mcnts[PC_CNT],
                     sdf_msg_rtstate->mtime->mcnts[PA_CNT],
                     sdf_msg_rtstate->mtime->mcnts[PN_CNT],
                     sdf_msg_rtstate->mtime->mcnts[C_CNT],
                     sdf_msg_rtstate->mtime->mcnts[A_CNT],
                     sdf_msg_rtstate->mtime->mcnts[N_CNT]);
        return (1);
    }
    return (status);
}


/**
 * @brief check for a fixed timeout interval event and act on failed responses
 *
 * Request msgs that expect respones are checked on an interval basis dictated by the timeout
 * value entered with the msg_timeout arg. To walk the hash table on every interval could be
 * time consuming if there are a large number of requests outstanding. A simple counter tracking
 * is checked first, if msg counts exceed the timeout setting then the hash table is walked
 *
 * There are 3 timestamp values in the interval cycle
 * ptimemkr is the previous interval time, timemkr is the current and ntimemkr is the future
 * msg counts are held in the mcnts array as follows
 * PC_CNT - the number of msg counts sent when the msg timestamp =  ptimemkr
 * PA_CNT - the number of msg counts sent when the msg timestamp >  ptimemkr
 * PN_CNT - the number of msg counts sent when the msg timestamp =  timemkr
 *  C_CNT - the number of msg counts sent when the msg timestamp =  timemkr
 *  A_CNT - the number of msg counts sent when the msg timestamp >  timemkr
 *  N_CNT - the number of msg counts sent when the msg timestamp =  ntimemkr
 * on an interval update the current count values are added to any existing
 * counts. A msg timesout when a count is found in PC_CNT
 */
void
sdf_msg_chktimeout(void) {
    SDFMSGMapEntry_t *checkit; // *oldone;
    msgtime_t currtm = 0;
    msgtime_t tm[NUM_HBUCKETS];

    /* check to see if we need to evaluate pending mesages */
    if (!sdf_msg_updatetimechk(sdf_msg_rtstate->mtime)) {
        return;
    }

    if (sdf_msg_rtstate->resp_n_flight) {
        int ne = SDFMSGMapNEntries(&respmsg_tracker);
        if (!ne) {
            fatal("%d responses in flight without hash entry",
                sdf_msg_rtstate->resp_n_flight, sdf_msg_rtstate->myid);
        }
        if (sdf_msg_rtstate->mtime->mcnts[PC_CNT]) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: TIMEOUT DETECTED tmout %d ntimemkr %lu\n"
                         "        timemkr %lu usec %lu prevmkr %lu diff %lu\n"
                         "        PC_CNT %d, PA_CNT %d, PN_CNT %d, C_CNT %d, A_CNT %d, N_CNT %d\n",
                         sdf_msg_rtstate->myid,
                         sdf_msg_rtstate->mtime->tmout, sdf_msg_rtstate->mtime->ntimemkr,
                         sdf_msg_rtstate->mtime->timemkr,
                         sdf_msg_rtstate->mtime->mstimemkr, sdf_msg_rtstate->mtime->ptimemkr,
                         sdf_msg_rtstate->mtime->diffmkr,
                         sdf_msg_rtstate->mtime->mcnts[PC_CNT],
                         sdf_msg_rtstate->mtime->mcnts[PA_CNT],
                         sdf_msg_rtstate->mtime->mcnts[PN_CNT],
                         sdf_msg_rtstate->mtime->mcnts[C_CNT],
                         sdf_msg_rtstate->mtime->mcnts[A_CNT],
                         sdf_msg_rtstate->mtime->mcnts[N_CNT]);

            SDFMSGMapEnumerate(&respmsg_tracker); /* set 1st entry */
            for (int i = 0; i < ne; i++) {
                checkit = SDFMSGMapNextEnumeration(&respmsg_tracker);
                if (checkit != NULL) {
                    uint64_t dtmsec = sdf_msg_rtstate->mtime->timemkr - checkit->contents->msg_timeout;
                /* this is in ns */
                    currtm = show_howlong(checkit->contents->msg_basetimestamp, ne, tm);
                    if (dtmsec >= sdf_msg_rtstate->mtime->tmout + 1) {
                        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                                     "\nNode %d: TIMEOUT DETECTED msg_seqnum %lu msg_tmout %lu timemkr %lu\n"
                                     "        inflight %lu ne %d etime %lu us difftm %lu sec\n",
                                     sdf_msg_rtstate->myid, checkit->contents->msg_seqnum,
                                     checkit->contents->msg_timeout,
                                     sdf_msg_rtstate->mtime->timemkr,
                                     sdf_msg_rtstate->resp_n_flight,
                                     SDFMSGMapNEntries(&respmsg_tracker), tm[ne]/1000, dtmsec);
                        sdf_msg_dotimeouterr(checkit, SDF_TIMEOUT);
                    }
                } else {
                    plat_assert(checkit != NULL);
                }
            }
        }
    }
}


/*
 * When a node dies, issue an error to any threads that are waiting on
 * responses from that node.
 */
void
sdf_msg_int_dead_responses(int rank)
{
    msg_resptrkr_t *resp;
    SDFMSGMapEntry_t *entry;

    if (!sdf_msg_rtstate->resp_n_flight)
        return;

    SDFMSGMapEnumerate(&respmsg_tracker);
    for (;;) {
        entry = SDFMSGMapNextEnumeration(&respmsg_tracker);
        if (!entry)
            break;
        resp = entry->contents;
        if (resp->msg_dest_vnode == rank)
            sdf_msg_dotimeouterr(entry, SDF_NODE_DEAD);
    }
}


/* just a place to dump info to the screen */

void sdf_msg_qinfo(void) {

    for (int i = 0; i < sdf_msg_rtstate->qtotal; i++) {
        struct q_tracker *saveit = sdf_msg_rtstate->qstate[i];
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: q_pair %p q_in %p q_out %p sn %d ss %d dn %d ds %d qcnt %d wcnt %d",
                     sdf_msg_rtstate->myid, saveit->q_pair_tkr, saveit->q_in, saveit->q_out, saveit->src_node,
                     saveit->src_srv, saveit->dest_node, saveit->dest_srv, saveit->qnum, saveit->wnum);
    }
}


/**
 * @brief set the msg response timeout value
 *
 * command line arg of msg_timeout can set the value of a message timeout
 * the value is applied to all internode request/response messages
 */

void
sdf_msg_settimeout(int timeout) {

    pthread_mutex_lock(&msg_srt->msg_release);
    if (!timeout) {
        msg_srt->msg_timeout = 86400; /* we will wait a day for a timeout */
    } else {
        msg_srt->msg_timeout = timeout;
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "\ntimeout %d msg_timeout %d\n", timeout, msg_srt->msg_timeout);
    pthread_mutex_unlock(&msg_srt->msg_release);


    return;

}


uint64_t
show_dtime(uint64_t oldtm)
{
    /* What environment variable needs to be set to get clock_gettime() */
    struct timespec curtime;
    uint64_t difftm;

    (void) clock_gettime(CLOCK_REALTIME, &curtime);
    if (curtime.tv_nsec < oldtm) {
        difftm = (1000000000 + curtime.tv_nsec) - oldtm;
    } else {
        difftm = curtime.tv_nsec - oldtm;
    }
    return (curtime.tv_nsec);
}

#if 0
static void
sdf_msg_timeoutfreemsg(plat_closure_scheduler_t *contect, void *env,
                       struct sdf_msg *msg) {
    plat_assert(msg);
    sdf_msg_free(msg);
}
#endif

/*
 * This pthread will be used to simulate the SDF_MANAGEMENT sending and
 * receiving messages to the sdf messaging engine. Right now there is an
 * ack back to the master for every ten messages sent to a slave node.
 */

void *
sdf_msg_tester_start(void *arg)
{
    int *myid = (int *)arg;
    uint32_t localid = *myid;
    struct sdf_queue_pair *q_pair_SYS_INTERNAL = NULL;
    struct sdf_queue_pair *q_pair_SYS[MAX_NUM_SCH_NODES];
    struct sdf_msg *send_msg = NULL, *recv_msg = NULL;
    vnode_t node = 0;
    serviceid_t protocol = SDF_SYSTEM;
    msg_type_t type;
    int i, j, ret, cnt = 0;
    sdf_fth_mbx_t fthmbx;

    memset(&fthmbx, 0, sizeof(fthmbx));
    fthmbx.actlvl = SACK_NONE_FTH;
    fthmbx.release_on_send = 1;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Testing pthread MESSAGING MANGEMENT Communication\n", localid);

    /*
     * Create a queue pair from this thread servicing my_protocol to the thread
     * on another node serving CONSISTENCY
     */
    while (1) {
        usleep(250000);
        pthread_mutex_lock(&msg_srt->msg_release);
        if (msg_srt->msg_mpi_release) { /* check if bin_init is done, if so get local SYS queue pair */
            q_pair_SYS_INTERNAL = sdf_msg_getsys_qpairs(my_protocol, localid, localid);
        }
        pthread_mutex_unlock(&msg_srt->msg_release);
        if (!q_pair_SYS_INTERNAL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Can't find internal qpair %p sn = dn %d ss %d ds %d -- try again\n",
                         localid, q_pair_SYS_INTERNAL, localid, my_protocol, SDF_SYSTEM);
        } else {
            pthread_mutex_lock(&msg_srt->msg_release);
            msg_srt->msg_sys_release++; /* notify that bin_init is done */
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: got SYS_INTERNAL queue pair %p sn = dn %d ss %d ds %d sys_r %d\n",
                         localid, q_pair_SYS_INTERNAL, localid, my_protocol,
                         SDF_SYSTEM, msg_srt->msg_sys_release);
            pthread_mutex_unlock(&msg_srt->msg_release);
            break;
        }
    }

    int debug = 0;
    if (debug) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d MNGMT TEST THREAD EXITING HERE - no msg sent\n",
                     localid);
        return (0);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d SDF Msg Management Thread is coming up\n", localid);
    }


    int localpn, actmask;
    uint32_t numprocs;
    int localrank = sdf_msg_nodestatus(&numprocs, &localpn, cluster_node, &actmask);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: numprocs %d active_procs mask 0x%x active_mask 0x%x\n",
                 localrank, numprocs, localpn, actmask);
    if (numprocs == 1) {
        node = 0;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Single node process... Msg Management exiting\n", localid);
        return (0);

    } else {
        int tmp = 1;
        tmp = tmp << localrank;
        tmp = tmp ^ localpn;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: %s eligible node mask 0x%x\n", localrank, __func__, tmp);
        /* just send to the 1st eligible node */
        for (i = 0; i < numprocs; i++) {
            if ((tmp >> i)&1) {
                node = i;
                break;
            }
        }
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: my dn will be %d\n", localrank, node);
        for (i = 0; i < numprocs; i++) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: cluster_node[%d] = %d cluster_node[%d] = %d\n", localrank,
                         i, cluster_node[i], numprocs, cluster_node[numprocs]);
        }
    }

/*
 * we are waiting for someone to start the messaging at the app, right now that is elsewhere
 * we don't want to add threads when they are doing the performance hoo haa
 * but we have to tell the tell the messaging init process that we got here so inc the sys_rel flag
 */

    while (1) {
        usleep(250000);
        pthread_mutex_lock(&msg_srt->msg_release);
        msg_srt->msg_sys_release++;
        if (msg_srt->msg_run_release) {
            pthread_mutex_unlock(&msg_srt->msg_release);
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Messaging Engine has been started -- proceed\n", localid);
            break;
        }
        pthread_mutex_unlock(&msg_srt->msg_release);
    }

    for (i = 0; i < MAX_NUM_SCH_NODES; i++) {
        if (!(q_pair_SYS[i] = sdf_msg_getsys_qpairs(my_protocol, localid, i))) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Can't find qpair_SYS[%d]=%p sn %d dn %d ss %d ds %d -- try again?\n",
                         localid, i, q_pair_SYS[i], localid, node, my_protocol, SDF_SYSTEM);
            usleep(250000);
        } else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: got qpair_SYS[%d] =  %p sn %d dn %d ss %d ds %d\n",
                         localid, i, q_pair_SYS[i], localid, node, my_protocol, SDF_SYSTEM);
        }
    }

    /* Silly loop counter for the moment */

    j = 0;
    for (;;) {

        send_msg = (struct sdf_msg *)sdf_msg_alloc(TSZE);
        if (send_msg == NULL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "\nsdf_msg_alloc() failed\n");
            return ((void *)1);
        }

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d msg %p msg->msg_payload %p\n",
                     localid, send_msg, send_msg->msg_payload);
        /* Fill in body of message with test data */
        for (i = 0; i < TSZE; ++i) {
            send_msg->msg_payload[i] = (unsigned char) i;
        }
        protocol = SDF_SYSTEM;  /* num value of 1 */
        type = SYS_REQUEST;     /* num value of 1 */

        if (localid == 0) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: SENDING SYS MSG dnode %d, proto %d, type %d loop num %d\n",
                         localid, node, protocol, type, j);
 
            ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, protocol,
                               localid, my_protocol, type, &fthmbx, NULL);
 
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: sdf_msg_send returned %d\n", localid, ret);
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: %s: calling sdf_msg_receive(%p, %d, %d)\n",
                         localid, __func__, q_pair_SYS_INTERNAL->q_out, 0, B_TRUE);
            if (j == 100) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: %d MESSAGES SENT... waiting for ack... total %d\n",
                             localid, j, cnt);
                recv_msg = sdf_msg_receive(q_pair_SYS[node]->q_out, 0, B_TRUE);
                sdf_msg_free_buff(recv_msg);
                cnt = cnt + j;
                j = 0;
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: ACK RECEIVED send again?... total %d\n",
                             localid, cnt);
            }
            if (cnt == 100000) {
                recv_msg = sdf_msg_receive(q_pair_SYS[node]->q_out, 0, B_TRUE);
                sdf_msg_free_buff(recv_msg);
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: %d messages done exiting...\n",
                             localid, cnt);
                break;
            }
            sleep(10);
        } else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Ready to RECEIVE MSG %p\n",
                         localid, recv_msg);
            recv_msg = sdf_msg_receive(q_pair_SYS[node]->q_out, 0, B_TRUE);
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: GOT ONE and back from sdf_msg_receive with msg %p\n",
                         localid, recv_msg);
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: RECEIVING MSG vers %d clusterid %d ss %d ds %d sn %d dn %d type %d loop %d\n",
                         localid, recv_msg->msg_version, recv_msg->msg_clusterid,
                         recv_msg->msg_src_service, recv_msg->msg_dest_service,
                         recv_msg->msg_src_vnode, recv_msg->msg_dest_vnode, recv_msg->msg_type, j);
            sdf_msg_free_buff(recv_msg);
            if (j == 100) {
                ret = sdf_msg_send((struct sdf_msg *)send_msg, TSZE, node, protocol,
                                   localid, my_protocol, type, &fthmbx, NULL);
                cnt = cnt + j;
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: %d MESSAGES received -- NOW sending ACK... total loop cnt %d\n",
                             localid, j, cnt);
                j = 0;
            }
            if (cnt == 100000) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: We are done total loop cnt %d\n",
                             localid, cnt);
                j = 0;
                break;
            }
        }
        usleep(500);
        j++;
        }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d Exiting pthread MANGEMENT Tester -- total msgs %d\n", localid, j);
    return (0);
}
