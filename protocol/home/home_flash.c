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
 * File:   sdf/protocol/home/home_flash.c
 * Author: Drew Eckhardt
 *         home_flash_wrapper from Bryan O'krafka
 *
 * Created on March 30, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: home_flash.c 13611 2010-05-12 20:08:47Z briano $
 */

#include <sys/time.h>
#include <event.h>

#include "platform/assert.h"
#include "platform/logging.h"
#include "platform/unistd.h"

#include "fth/fthSpinLock.h"
#include "flash/flash.h"
#include "shared/init_sdf.h"
#include "protocol/protocol_common.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_alloc.h"
#include "protocol/action/action_thread.h"
#include "protocol/action/action_new.h"
#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_msg_action.h"
#include "shared/name_service.h"
#include "shared/shard_meta.h"
#include "shared/shard_compute.h"
#include "shared/internal_blk_obj_api.h"

#include "home_flash.h"
#include "home_util.h"
#include "protocol/action/recovery.h"
#include "protocol/action/simple_replication.h"

    /*  Uncomment this macro definition to compile in
     *  trace collection code.  It is only used if
     *  the logging level for sdf/prot=trace.
     */
#define INCLUDE_TRACE_CODE

#define LOG_CAT PLAT_LOG_CAT_SDF_PROT_FLASH

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_SHARD, PLAT_LOG_CAT_SDF_PROT_FLASH, "shard");

enum hf_msg_type {
    HF_MSG_KV,
    HF_MSG_SHARD,
    HF_MSG_LCLK,
};

    // from action_new.c
extern SDF_status_t get_status(int retcode);
extern int get_retcode(SDF_status_t status);
extern int check_flash_space(SDF_action_state_t *pas, struct shard *pshard);
extern void finish_write_in_flight(SDF_action_state_t *pas);

/* Don't access directly, instead use sdf_get_current_time() as when
 * memcached is built in, their current time is used instead
 */
static volatile time_t sdf_current_time;

static struct event_base *sdf_main_base;

SDF_time_t (*memcached_get_current_time) (void) = NULL;

/**
 * @brief Drain I/O nanosleep wait time and sleep count
 */
enum {
    HF_DRAIN_SLEEP = 1000000,
    HF_DRAIN_LIMIT = 100,
};

/**
 * @brief Table of all supported messages
 *
 * item(caps, type, response_good, response_bad)
 */
#define HF_MSG_ITEMS() \
    /* Key, value operations */                                                \
    item(HFXST, HF_MSG_KV, FHXST, FHNXS)                                       \
    item(HFGFF, HF_MSG_KV, FHDAT, FHGTF)                                       \
    item(HFPTF, HF_MSG_KV, FHPTC, FHPTF)                                       \
    item(HZSF, HF_MSG_KV, FHDEC, FHDEF)                                       \
    item(HFCIF, HF_MSG_KV, FHCRC, FHCRF)                                       \
    /* XXX: Should HFCZF get a unique type? */                                 \
    item(HFCZF, HF_MSG_KV, FHCRC, FHCRF)                                       \
    item(HFSET, HF_MSG_KV, FHSTC, FHSTF)                                       \
	/* Added for writeback cache support */                                \
    item(HFFLS, HF_MSG_KV, FHFCC, FHFCF)                                       \
    item(HFFIV, HF_MSG_KV, FHFIC, FHFIF)                                       \
    item(HFINV, HF_MSG_KV, FHINC, FHINF)                                       \
    /* Shard operations */                                                     \
    item(HFCSH, HF_MSG_SHARD, FHCSC, FHCSF)                                    \
    item(HFSSH, HF_MSG_SHARD, FHSSC, FHSSF)                                    \
    item(HFDSH, HF_MSG_SHARD, FHDSC, FHDSF)                                    \
    item(HFGLS, HF_MSG_SHARD, FHGLC, FHGLF)                                    \
    item(HFGIC, HF_MSG_SHARD, FHGIC, FHGIF)                                    \
    item(HFGBC, HF_MSG_SHARD, FHGCC, FHGCF)                                    \
    item(HFGSN, HF_MSG_SHARD, FHGSC, FHGSF)                                    \
    item(HFSRR, HF_MSG_SHARD, FHSRC, FHSRF)                                    \
    item(HFSPR, HF_MSG_SHARD, FHSPC, FHSPF)                                    \
    item(HFFLA, HF_MSG_SHARD, FHFLC, FHFLF)                                    \
    item(HFRVG, HF_MSG_SHARD, FHRVC, FHRVF)                                    \
    item(HFNOP, HF_MSG_SHARD, FHNPC, FHNPF)                                    \
	/* Added for writeback cache support */                                \
    item(HFFLC, HF_MSG_SHARD, FHLCC, FHLCF)                                    \
    item(HFFLI, HF_MSG_SHARD, FHLIC, FHLIF)                                    \
    item(HFINC, HF_MSG_SHARD, FHCIC, FHCIF)                                    \
	/* Added for prefix-based delete */                                    \
    item(HFPBD, HF_MSG_SHARD, FHPBC, FHPBF)                                    \
    /* Lamport clock update */                                                 \
    item(HFOSH, HF_MSG_LCLK,  FHOSC, FHOSF)                



/**
 * @brief Home node flash protocol state shared by all worker threads.
 *
 * This should be a singleton created by the agent code.
 */
struct SDF_flash_state {
    /** @brief Configuration */
    struct SDF_flash_init config;

    /** Queue pair from flash_client_service to flash_server_service */
    struct sdf_queue_pair *recv_queue_pair;

    /** Used for a barrier for thread pool initialization. */
    fthMbox_t startup_mbx;

    /**
     * @brief Action state
     *
     * Required to initialize per-thread state for internal client
     * operations to translate container cguid to flash structures.
     */
    struct SDF_action_state *p_action_state;

    /** @brief Total refcount, 1 + number of threads */
    int ref_count;

    /** @brief Number of threads.  Included in ref count */
    int num_threads;

    /** @brief Number of times #home_flash_shutdown called */
    int shutdown_count;

    /** @brief Closure applied on shutdown completion */
    home_flash_shutdown_t shutdown_closure;

    /** @brief Current Lamport clock value */
    sdf_replication_ltime_t ltime;

    /**
     * @brief Highest sequence number we have seen
     *
     * FIXME: drew 2009-05-16 This is incorrect.  It needs to have shard scope
     * and be persistent.
     *
     * ltime based fencing of stale IOs also must have shard scope but does
     * not need to be persistent. IOs must not be allowed until the
     * container open completes.
     *
     * We want to move all this state into the replication code proper,
     * since the mechanics needed for single copy semantics with reads
     * on any replica are similar to those needed during normal operation
     * of the write coordinator.
     */
    uint64_t highest_sequence;

    /** @brief Flash I/O in progress ref count */
    int io_ref_count;

    /** @brief Home flash drain state */
    SDF_boolean_t ltime_drain;

    /** @brief Spin lock to sync ltime, highest_sequence & drain state */
    fthSpinLock_t pfs_spin;
};

struct SDF_flash_thrd_state {
    /** @brief parent state */
    struct SDF_flash_state *pfs;

    /**
     * @brief action thread state for internal client use
     *
     * Required for internal client operations to translate container
     * cguid to flash structures, etc.
     */
    struct SDF_action_thrd_state *pats;

    /** @brief Initializer for pats internal client state */
    struct SDF_action_init *pai;

    /** @brief Lamport clock value for the thread's current flash I/O request*/
    sdf_replication_ltime_t ltime;

    /** @brief Spin lock to sync ltime state */
    fthSpinLock_t pts_spin;
};

/**
 * @brief Increment in-progress home flash IO ref count
 */
static __inline__ void increment_io_ref_count(struct SDF_flash_state *pfs) {

    FTH_SPIN_LOCK(&pfs->pfs_spin);
    ++pfs->io_ref_count;
    FTH_SPIN_UNLOCK(&pfs->pfs_spin);
}

/**
 * @brief Decrement in-progress home flash IO ref count
 */
static __inline__ void decrement_io_ref_count(struct SDF_flash_state *pfs) {
    FTH_SPIN_LOCK(&pfs->pfs_spin);
    --pfs->io_ref_count;
    FTH_SPIN_UNLOCK(&pfs->pfs_spin);
}

SDF_status_t get_status(int retcode);
static void home_flash_refcount_dec(struct SDF_flash_state *pfs);
static struct SDF_flash_thrd_state *pts_alloc(struct SDF_flash_state *pfs);
static void pts_free(struct SDF_flash_thrd_state *pts);
static void pts_main(uint64_t arg);

static int shard_init_flags(struct SDF_shard_meta *meta, int *pflags);
static int hf_send_reply(struct SDF_flash_thrd_state *pts,
                         struct sdf_msg *send_msg, int msize,
                         struct sdf_msg *recv_msg);
static void sdf_clock_handler(const int fd, const short which, void *arg);

// static char *build_string_type(uint32_t len, char *pdata);
static char * build_string_type_zero(uint32_t len);

struct SDF_flash_state *
home_flash_alloc(struct SDF_flash_init *pfi,
                 struct SDF_action_state *pas) {
    struct SDF_flash_state *pfs;

    int failed;
    __attribute__((unused)) int status;

    pfs = (struct SDF_flash_state *) proto_plat_alloc_arena(1*sizeof(*pfs), NonCacheObjectArena);
#ifdef MALLOC_TRACE
        UTMallocTrace("home_flash_alloc", TRUE, FALSE, FALSE, (void *) pfs, sizeof(*pfs));
#endif // MALLOC_TRACE
    failed = !pfs;
    if (pfs)  {
        pfs->config = *pfi;
        pfs->p_action_state = pas;
        pfs->ref_count = 1;
        pfs->num_threads = 0;
        pfs->shutdown_count = 0;
        pfs->shutdown_closure = home_flash_shutdown_null;
        pfs->ltime = 0;
        pfs->highest_sequence = 0;
        pfs->io_ref_count = 0;
        pfs->ltime_drain = SDF_FALSE;
        FTH_SPIN_INIT(&pfs->pfs_spin);
	fthMboxInit(&(pfs->startup_mbx));
    }

    if (!failed) {
        pfs->recv_queue_pair =
            sdf_create_queue_pair(pfs->config.my_node /* src */,
                                  VNODE_ANY /* dest */,
                                  pfs->config.flash_server_service /* src */,
                                  SERVICE_ANY /* dest */,
                                  SDF_WAIT_FTH);
        if (!pfs->recv_queue_pair) {
            plat_log_msg(21291, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "Can't create queue pair src node %u service %u"
                         " dest node %u service %u",
                         pfs->config.my_node, pfs->config.flash_server_service,
                         pfs->config.my_node, pfs->config.flash_client_service);
            failed = 1;
        }
    }

    if (failed && pfs) {
        home_flash_shutdown(pfs, home_flash_shutdown_null);
        pfs = NULL;
    }

    if (!failed) {
        plat_log_msg(21292, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "home flash %p allocated", pfs);
    } else {
        plat_log_msg(21293, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "home_flash_alloc failed ");
    }

    return (pfs);
}

void
home_flash_shutdown(struct SDF_flash_state *pfs,
                    home_flash_shutdown_t shutdown_closure) {
    int before;
    int num_threads;
    int i;

    plat_log_msg(21294, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "shutdown flash state %p ref_count %d",
                 pfs, pfs->ref_count);

    before = __sync_fetch_and_add(&pfs->shutdown_count, 1);
    plat_assert(!before);
    if (!before) {
        pfs->shutdown_closure = shutdown_closure;

        /* Snapshot before any terminations occur */
        num_threads = pfs->num_threads;

        /* Shutdown all threads */
        for (i = 0; i < num_threads; ++i) {
            sdf_msg_post(pfs->recv_queue_pair, NULL);
        }

        home_flash_refcount_dec(pfs);
    }
}
int
home_flash_start(struct SDF_flash_state *pfs) {
    int ret;
    int i;
    struct SDF_flash_thrd_state *pts;
    fthThread_t *fth;

    plat_log_msg(21295, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "home flash %p starting", pfs);

    /* Setup clock events */
    sdf_main_base = event_init();    
    sdf_clock_handler(0, 0, 0);
    ret = event_base_loop(sdf_main_base, EVLOOP_ONCE);
    plat_assert(!ret);
    
    ret = 0;
    for (i = 0; !ret && i < pfs->config.nthreads; ++i) {
        pts = pts_alloc(pfs);
        if (!pts) {
            ret = -1;
        }

        if (!ret) {
            fth = fthSpawn(&pts_main, 40960);
            if (!fth) {
                ret = -1;
            }
        }

        if (!ret) {
            fthResume(fth, (uint64_t)pts);
        } else {
            pts_free(pts);
        }
    }

    if (!ret) {
	for (i = 0; !ret && i < pfs->config.nthreads; ++i) {
	    (void) fthMboxWait(&(pfs->startup_mbx));
	}
        plat_log_msg(21138, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "home flash %p started", pfs);
    } else {
        plat_log_msg(21139, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "home flash %p failed to start ", pfs);
    }

    return (ret);
}

static void
home_flash_refcount_dec(struct SDF_flash_state *pfs) {
    int before;

    before = __sync_fetch_and_sub(&pfs->ref_count, 1);
    plat_assert(before >= 1);

    if (before == 1) {
        if (pfs->recv_queue_pair)  {
            sdf_delete_queue_pair(pfs->recv_queue_pair);
        }

#ifdef MALLOC_TRACE
            UTMallocTrace("home_flash_refcount_dec", FALSE, TRUE, FALSE, (void *) pfs, sizeof(*pfs));
#endif // MALLOC_TRACE

        plat_log_msg(21296, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "shutdown flash state %p complete", pfs);
        plat_free(pfs);
    }
}

static struct SDF_flash_thrd_state *
pts_alloc(struct SDF_flash_state *pfs) {
    struct SDF_flash_thrd_state *pts;
    int failed;
    // int after;

    pts = proto_plat_alloc_arena(sizeof (*pts), NonCacheObjectArena);
#ifdef MALLOC_TRACE
        UTMallocTrace("pts_alloc: pts", TRUE, FALSE, FALSE, (void *) pts, sizeof(*pts));
#endif // MALLOC_TRACE
    failed = !pts;
    if (!failed) {
        // after = __sync_add_and_fetch(&pfs->ref_count, 1);
        (void) __sync_add_and_fetch(&pfs->ref_count, 1);
        pts->pfs = pfs;
        pts->pats =
            (struct SDF_action_thrd_state *)proto_plat_alloc_arena(sizeof (*pts->pats), NonCacheObjectArena);
#ifdef MALLOC_TRACE
            UTMallocTrace("pts_alloc: pats", TRUE, FALSE, FALSE, (void *) pts->pats, sizeof(*pts->pats));
#endif // MALLOC_TRACE
        pts->pai = (struct SDF_action_init *)proto_plat_alloc_arena(sizeof (*pts->pai), NonCacheObjectArena);
#ifdef MALLOC_TRACE
            UTMallocTrace("pts_alloc: pai", TRUE, FALSE, FALSE, (void *) pts->pai, sizeof(*pts->pai));
#endif // MALLOC_TRACE
        failed = !(pts->pats && pts->pai);

        pts->ltime = 0;
        FTH_SPIN_INIT(&pts->pts_spin);
    }

    if (!failed) {
        /* XXX: Move the cut-and-paste magic from home_thread.c to a fn */
        pts->pai->pcs = pfs->p_action_state;
        pts->pai->pts = pts->pats;
        // InitActionAgentPerThreadState(pts->pai->pcs, pts->pats, pts->pai);
        // pts->pai->ctxt     = ActionGetContext(pts->pats);
    }

    if (failed && pts) {
        pts_free(pts);
        pts = NULL;
    }

    return (pts);
}

static void
pts_free(struct SDF_flash_thrd_state *pts) {
    struct SDF_flash_state *pfs;

    if (pts) {
        pfs = pts->pfs;

        if (pts->pats) {
            plat_free(pts->pats);
#ifdef MALLOC_TRACE
                UTMallocTrace("pts_free: pts->pats", FALSE, TRUE, FALSE, (void *) pts->pats, 0);
#endif // MALLOC_TRACE
        }
        if (pts->pai) {
            plat_free(pts->pai);
#ifdef MALLOC_TRACE
                UTMallocTrace("pts_free: pts->pai", FALSE, TRUE, FALSE, (void *) pts->pai, 0);
#endif // MALLOC_TRACE
        }
        plat_free(pts);
#ifdef MALLOC_TRACE
            UTMallocTrace("pts_free: pts", FALSE, TRUE, FALSE, (void *) pts, 0);
#endif // MALLOC_TRACE

        home_flash_refcount_dec(pfs);
    }
}

/**
 * @brief Flash protocol thread main loop
 *
 * One flash protocol thread is required for each simultaneous command.  Threads
 * are terminated by passing a NULL message in.
 *
 * 2/1/09: Added support for handling Lamport clock updates:
 *
 * - Enter drain state on receipt of ltime update
 * - Allow pending flash request marked with old ltime to complete before
 *   initiating new I/O requests
 * - Any new request tagged with stale ltime are rejected once the ltime update
 *   notification has been received
 */
static void
pts_main(uint64_t arg) {
    struct SDF_flash_thrd_state *pts = (struct SDF_flash_thrd_state *)arg;
    struct SDF_flash_state *pfs = pts->pfs;

    struct sdf_msg *recv_msg = NULL;
    SDF_protocol_msg_t *recv_pm = NULL;
    SDF_Protocol_Msg_Info_t *pmi = NULL;
    struct sdf_msg *send_msg = NULL;
    SDF_protocol_msg_t *send_pm = NULL;
    SDF_size_t msize = 0;
    // int status = 0;
    int before = 0;
    enum hf_msg_type msg_type;

    unsigned char *pdata = NULL;
    SDF_boolean_t stale_ltime = SDF_FALSE;

    static int wait_count = 0;

    before = __sync_fetch_and_add(&pfs->num_threads, 1);
    plat_log_msg(21297, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "flash protocol thread %d starting pts %p",
                 before, pts);

    /*  Initialize per-thread state here
     *  (so aio_ctxt is set up correctly!)
     */
    InitActionAgentPerThreadState(pts->pai->pcs, pts->pats, pts->pai);
    pts->pai->ctxt = ActionGetContext(pts->pats);

    /*  Let home_flash_start know that I am initialized */ 
    fthMboxPost(&(pfs->startup_mbx), (uint64_t) 1);

    do {
        // Check the drain state
        if (!pfs->ltime_drain) {

            // Normal state
            recv_msg = sdf_msg_recv(pfs->recv_queue_pair);

            /* For fast recovery */
            if (sdf_rec_funcs) {
                typedef SDF_protocol_msg_t sp_msg_t;
                typedef SDF_protocol_msg_tiny_t sp_msg_tiny_t;
                int recv_len = recv_msg->msg_len - sizeof(sdf_msg_t);

                if (recv_len >= sizeof(sp_msg_tiny_t)) {
                    SDF_protocol_msg_type_t type;
                    type = ((sp_msg_tiny_t *) recv_msg->msg_payload)->type;
                    msg_setn2h(type);
                    if (type == HFFGB || type == HFFGC || type == HFFGD ||
                        type == HFFSX || type == HFFRC) {
                        SDF_action_init_t *pai = pts->pai;
                        struct flashDev           *f = pfs->config.flash_dev;
                        struct SDF_action_state *pas = pfs->p_action_state;
                        ((*sdf_rec_funcs->msg_recv)(recv_msg, pai, pas, f));
                        continue;
                    }
                }
            }

            send_msg = NULL;
            msize = 0;
            stale_ltime = SDF_FALSE;

            if (recv_msg) {
                recv_pm = (SDF_protocol_msg_t *)recv_msg->msg_payload;

		// Remove these until Jake can check forward compatibility!
                // plat_assert(recv_pm->current_version == PROTOCOL_MSG_VERSION);
                // plat_assert(recv_pm->supported_version == PROTOCOL_MSG_VERSION);

                pmi = &(SDF_Protocol_Msg_Info[recv_pm->msgtype]);

                // Check for a stale request (request ltime < current ltime)
                FTH_SPIN_LOCK(&pfs->pfs_spin);
                if (recv_pm->msgtype != HFOSH && recv_pm->op_meta.shard_ltime < pfs->ltime) {
                    stale_ltime = SDF_TRUE;
                }
                FTH_SPIN_UNLOCK(&pfs->pfs_spin);

                if (PLAT_UNLIKELY(stale_ltime)) {

                    plat_log_msg(21298, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                                 "STALE LTIME - recv_pm->op_meta.shard_ltime: %u - pfs->ltime: %u",
                                 recv_pm->op_meta.shard_ltime, pfs->ltime);

                    pdata = (unsigned char *) recv_pm + sizeof(SDF_protocol_msg_t);

                    // Send stale ltime error back
                    send_msg =
                        home_load_msg(recv_pm->node_to /* from */,
                                      recv_pm->node_from /* to */,
                                      recv_pm,
                                      home_flash_response_type(recv_pm->msgtype,
                                                               SDF_STALE_LTIME),
                                      pdata /* pdata */,
                                      recv_pm->data_size /* data_size */,
                                      recv_pm->exptime /* exptime */,
                                      recv_pm->createtime /* createtime */,
                                      SDF_SEQUENCE_NO_INVALID,
                                      SDF_STALE_LTIME /* status */,
                                      &msize /* pmsize */,
                                      0 /* key */,
                                      0 /* key len */,
                                      0 /* flags */);
                } else {

                    FTH_SPIN_LOCK(&pfs->pfs_spin);
                    if (recv_pm->seqno > pfs->highest_sequence) {
                        pfs->highest_sequence = recv_pm->seqno;
                    }
                    FTH_SPIN_UNLOCK(&pfs->pfs_spin);

                    plat_log_msg(21299, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                                 "received Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d"
                                 " shard 0x%lx",
                                 pmi->name, pmi->shortname,
                                 recv_pm->node_from,
                                 SDF_Protocol_Nodes_Info[pmi->src].name,
                                 recv_pm->node_to,
                                 SDF_Protocol_Nodes_Info[pmi->dest].name,
                                 recv_pm->tag, recv_pm->shard);

                    switch (recv_pm->msgtype) {
#define item(caps, type, response_good, response_bad)                \
                        case caps: msg_type = type; break;
                        HF_MSG_ITEMS()
#undef item
                            default:
                        plat_log_msg(21300, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                                     "Unsupported message type %s",
                                     SDF_Protocol_Msg_Info[recv_pm->msgtype].shortname);
                        plat_fatal("");
                    }

                    switch (msg_type) {
                    case HF_MSG_KV:
                        increment_io_ref_count(pfs);
                        send_msg = home_flash_wrapper(pfs->config.flash_dev,
                                                      pfs->config.flash_dev_count,
                                                      pts->pai, recv_msg,
                                                      recv_pm->msgtype,
                                                      recv_pm->node_from, &msize);
                        decrement_io_ref_count(pfs);
                        break;
                    case HF_MSG_SHARD:
                        increment_io_ref_count(pfs);
                        send_msg = home_flash_shard_wrapper(pfs->config.flash_dev,
                                                            pfs->config.flash_dev_count,
                                                            pts->pai,
                                                            recv_msg, &msize);
                        decrement_io_ref_count(pfs);
                        break;
                    case HF_MSG_LCLK:
                        // Update the home node copy of current ltime
                        pdata = (unsigned char *) recv_pm + sizeof(SDF_protocol_msg_t);
                        FTH_SPIN_LOCK(&pfs->pfs_spin);
                        memcpy(&pfs->ltime, pdata, sizeof(sdf_replication_ltime_t));
                        pfs->ltime_drain = SDF_TRUE;
                        
                        plat_log_msg(21301, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                                     "update ltime: %u", pfs->ltime);

                        // Return ltime update success
                        send_msg =
                            home_load_msg(recv_pm->node_to /* from */,
                                          recv_pm->node_from /* to */,
                                          recv_pm, FHOSC,
                                          &pfs->highest_sequence /* pdata */,
                                          sizeof(uint64_t) /* data_size */,
                                          recv_pm->exptime /* exptime */,
                                          recv_pm->createtime /* createtime */,
                                          pfs->highest_sequence /* sequence number */,
                                          SDF_SUCCESS /* status */,
                                          &msize /* pmsize */,
                                          NULL /* key */, 0 /* key len */,
                                          0 /* flags */);
                        FTH_SPIN_UNLOCK(&pfs->pfs_spin);
                        break;
                    }
                    plat_assert_always(send_msg);
                }
            }
            if (send_msg) {
                send_pm = (SDF_protocol_msg_t *)send_msg->msg_payload;
                plat_log_msg(21302, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "sending message msgtype = %s",
                             SDF_Protocol_Msg_Info[send_pm->msgtype].shortname);
                // status = hf_send_reply(pts, send_msg, msize, recv_msg);
                if (hf_send_reply(pts, send_msg, msize, recv_msg)) {}
            }

            if (recv_msg) {

                sdf_msg_free_buff(recv_msg);

#ifdef MALLOC_TRACE
                UTMallocTrace("pts_main: sdf_msg_free", FALSE, TRUE, FALSE, (void *) recv_msg, 0);
#endif // MALLOC_TRACE
            }

        } else {

            /**
            * We are in ltime drain mode.
            * If there are no pending flash IO, just continue on.
            * If there are pending flash IO, let them complete before letting new IO pass.
            */
            FTH_SPIN_LOCK(&pfs->pfs_spin);
            if (pfs->io_ref_count) {
                ++wait_count;
                FTH_SPIN_UNLOCK(&pfs->pfs_spin);        
                // Still draining - wait
                plat_log_msg(21303, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "Home flash drain I/O not complete - waiting - ref count: %d - wait_count: %d",
                             pfs->io_ref_count, wait_count);
                fthNanoSleep(HF_DRAIN_SLEEP);
                FTH_SPIN_LOCK(&pfs->pfs_spin);
                if (wait_count > HF_DRAIN_LIMIT) {
                    plat_log_msg(21304, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                                 "Home flash drain I/O wait limit exceeded (%d) - shutting down!",
                                 wait_count);
                    plat_abort();
                }
                FTH_SPIN_UNLOCK(&pfs->pfs_spin);        
            } else {
                // All I/O complete - continue normal processing
                pfs->ltime_drain = SDF_FALSE;
                wait_count = 0;
                FTH_SPIN_UNLOCK(&pfs->pfs_spin);        
                plat_log_msg(21305, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "Home flash drain I/O complete");
            }
        }

    } while (recv_msg);

    before = __sync_fetch_and_sub(&pfs->num_threads, 1);
    plat_assert(before > 0);

    plat_log_msg(21306, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "flash protocol thread stopping pts %p, %d remain",
                 pts, before - 1);

    pts_free(pts);
}

/*  Get a free shard map entry.
 */
static SDF_home_flash_entry_t *get_home_shard_map_entry(SDF_action_thrd_state_t *pts)
{
    SDF_home_flash_entry_t  *phfme;

    phfme = pts->free_shard_map_entries;
    if (phfme != NULL) {
        pts->free_shard_map_entries = phfme->next;
    }
    return(phfme);
}

/*  Free a shard map entry.
 */
static void free_home_shard_map_entry(SDF_action_thrd_state_t *pts, SDF_home_flash_entry_t *phfme)
{
    phfme->next = pts->free_shard_map_entries;
    pts->free_shard_map_entries = phfme;
}

/*  Stop a shard map entry.
 *  Returns 0 if success, non-zero otherwise.
 */
int stop_home_shard_map_entry(SDF_action_thrd_state_t *pts, SDF_shardid_t shard)
{
    int                      success = 0;
    SDFTLMap2Entry_t        *psme;
    SDF_home_flash_entry_t  *phfme;
    fthWaitEl_t             *wait;

    wait = fthLock(&(pts->shardmap_lock), 1, NULL);
    psme = SDFTLMap2Get(&(pts->shardmap), shard);
    if (!psme) {
	plat_log_msg(21307, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
		 "no shardmap entry for shardid 0x%lx", shard);
	success = 1;
    } else {
	phfme = (SDF_home_flash_entry_t *) psme->contents;
	plat_log_msg(30546, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
		     "stop home shard map entry (shard=%"PRIu64", pshard=%p, stopflag=%d before call)",
		     shard, phfme->pshard, phfme->stopflag);
	phfme->stopflag = SDF_TRUE;
    }
    fthUnlock(wait);
    return(success);
}

/*  Start a shard map entry.
 *  Returns 0 if success, non-zero otherwise.
 */
int start_home_shard_map_entry(SDF_action_thrd_state_t *pts, SDF_shardid_t shard)
{
    int                      success = 0;
    SDFTLMap2Entry_t        *psme;
    SDF_home_flash_entry_t  *phfme;
    fthWaitEl_t             *wait;

    wait = fthLock(&(pts->shardmap_lock), 1, NULL);
    psme = SDFTLMap2Get(&(pts->shardmap), shard);
    if (!psme) {
	plat_log_msg(21307, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
		 "no shardmap entry for shardid 0x%lx", shard);
	success = 1;
    } else {
	phfme = (SDF_home_flash_entry_t *) psme->contents;
	plat_log_msg(30547, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
		     "start home shard map entry (shard=%"PRIu64", pshard=%p, stopflag=%d before call)",
		     shard, phfme->pshard, phfme->stopflag);
	phfme->stopflag = SDF_FALSE;
    }
    fthUnlock(wait);
    return(success);
}

/*  Create a shard map entry.
 *  Returns 0 if success, non-zero otherwise.
 */
int create_home_shard_map_entry(SDF_action_thrd_state_t *pts, SDF_shardid_t shard, struct shard *pshard, SDF_boolean_t stopflag)
{
    int                      success = 0;
    SDFTLMap2Entry_t        *psme;
    SDF_home_flash_entry_t  *phfme;
    fthWaitEl_t             *wait;

    wait = fthLock(&(pts->shardmap_lock), 1, NULL);
    psme = SDFTLMap2Create(&(pts->shardmap), shard);
    if (!psme) {
	plat_log_msg(21308, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
		 "Could not create shardmap entry for shardid 0x%lx;"
		 " Entry already exists or there is insufficient memory", 
		 shard);
	success = 1;
    } else {
	phfme = get_home_shard_map_entry(pts);
	if (phfme == NULL) {
	    plat_log_msg(21309, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
			 "home_flash ran out of shard map structures"
			 "for shardid 0x%lx", shard);
	    success = 1;
	} else {
	    psme->contents  = phfme;
	    phfme->pshard   = pshard;
	    phfme->stopflag = stopflag;
	    plat_log_msg(30548, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
		     "create home shard map entry (shard=%"PRIu64", pshard=%p, stopflag=%d)",
		     shard, phfme->pshard, phfme->stopflag);
	}
    }
    fthUnlock(wait);
    return(success);
}

/*  Delete a shard map entry.
 *  Returns 0 if success, non-zero otherwise.
 */
int delete_home_shard_map_entry(SDF_action_thrd_state_t *pts, SDF_shardid_t shard)
{
    int                      success = 0;
    SDFTLMap2Entry_t        *psme;
    SDF_home_flash_entry_t  *phfme;
    fthWaitEl_t             *wait;

    wait = fthLock(&(pts->shardmap_lock), 1, NULL);
    psme = SDFTLMap2Get(&(pts->shardmap), shard);
    if (!psme) {
	plat_log_msg(21307, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
		 "no shardmap entry for shardid 0x%lx", shard);
	success = 1;
    } else {
	phfme = (SDF_home_flash_entry_t *) psme->contents;
        plat_log_msg(30549, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
	     "delete home shard map entry (shard=%"PRIu64", pshard=%p, stopflag=%d)",
	     shard, phfme->pshard, phfme->stopflag);
	free_home_shard_map_entry(pts, phfme);
	success = SDFTLMap2Delete(&(pts->shardmap), shard);
    }
    fthUnlock(wait);
    return(success);
}

struct sdf_msg *
home_flash_wrapper(
#ifdef MULTIPLE_FLASH_DEV_ENABLED
                   struct flashDev **in_flash_dev,
#else
                   struct flashDev *in_flash_dev,
#endif
                   uint32_t flash_dev_count,
                   SDF_internal_ctxt_t *pai_in,
                   struct sdf_msg *recv_msg,
                   SDF_protocol_msg_type_t type_replacement,
                   vnode_t dest_node,
                   SDF_size_t *pmsize)
{
    SDF_protocol_msg_t      *pm = NULL;
    struct sdf_msg          *new_msg = NULL;
    SDF_shardid_t            shard;
    struct shard            *pshard = NULL;
    char                    *pflash_key = NULL;
    char                    *pflash_data = NULL;
    void                    *pdata = NULL;
    char                    *pflash_data2 = NULL;
    SDF_size_t               data_size = 0;
    int                      success = 1;
    SDF_status_t             status = SDF_SUCCESS;
    SDF_status_t             inval_status = SDF_SUCCESS;
    SDF_protocol_msg_type_t  fh_mtype;
    SDF_protocol_msg_type_t  in_mtype;
    struct objMetaData       metaData;
    int                      retcode = FLASH_EOK;
    // SDF_protocol_msg_t      *pm_new = NULL;
    SDF_time_t               exptime;
    SDF_time_t               createtime;
    SDF_action_init_t       *pai;
    uint64_t                 sequence;
    SDF_action_thrd_state_t *pts;
    SDF_boolean_t            inval_cache;
    SDF_boolean_t            must_update;
    SDFTLMap2Entry_t        *psme;
    SDF_home_flash_entry_t  *phfe;

    int                     free_pflash_data2;
    int                     flash_flags;
    fthWaitEl_t            *mapwait;
    SDF_boolean_t           stopflag = SDF_FALSE;
    SDF_boolean_t           inval_ok = SDF_TRUE;

    sequence = SDF_SEQUENCE_NO_INVALID;

    pts = ((SDF_action_init_t *) pai_in)->pts;
    pai = pts->pai;

    plat_assert(recv_msg);
    pm = (SDF_protocol_msg_t *)recv_msg->msg_payload;

    // Remove these until Jake can check forward compatibility!
    // plat_assert(pm->current_version == PROTOCOL_MSG_VERSION);
    // plat_assert(pm->supported_version == PROTOCOL_MSG_VERSION);

    /*  This assumes that strings are null-terminated.
     *  The SDF interface makes sure that this is true.
     */

    pflash_key = pm->key.key;

#ifdef notyet
    /*
     * XXX: drew 2009-06-03 internal operations from meta_storage don't have
     * a valid cguid associated with them.  This doesn't look to be a problem
     * except where invalidation is involved, so I've disabled this check
     * for now and made invalidation conditional on that.
     */
    plat_assert(_sdf_cguid_valid(pm->cguid));
#endif

    shard = pm->shard;
    plat_assert(_sdf_shardid_valid(shard));

    /*  In addition to serializing accesses to the shardmap hashtable,
     *  (the shardmap hashtable is accessed by the home_flash threadpool
     *  as well as the memcached worker thread pool)
     *  mapwait is used by SDFActionStopContainer() to ensure that all
     *  pending flash accesses are completed.
     */
    mapwait = fthLock(&(pts->shardmap_lock), 1, NULL);
    psme = SDFTLMap2Get(&(pts->shardmap), shard);

    if (psme != NULL) {
	phfe     = (SDF_home_flash_entry_t *) psme->contents;
        pshard   = phfe->pshard;
	stopflag = phfe->stopflag;
	plat_log_msg(30550, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
		     "home_flash operation (%d) to a valid container"
		     "for cwguid %llu shardid 0x%lx (pshard=%p, stopflag=%d)",
		     pm->msgtype, (unsigned long long)pm->cguid, shard, pshard, stopflag);
    } else {
        /* reject the request--the container does not exist */
	plat_log_msg(30551, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
		     "home_flash operation (%d) to a non-existent container"
		     "for cwguid %llu shardid 0x%lx",
		     pm->msgtype, (unsigned long long)pm->cguid, shard);
	status   = SDF_RMT_CONTAINER_UNKNOWN;
	retcode  = FLASH_RMT_EBADCTNR;
	success  = 0;
        inval_ok = SDF_FALSE;
    }

    if (stopflag) {
	plat_log_msg(30552, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
		     "home_flash operation (%d) to a stopped container"
		     "for cwguid %llu shardid 0x%lx",
		     pm->msgtype, (unsigned long long)pm->cguid, shard);
	success = 0;
	status = SDF_STOPPED_CONTAINER;
	retcode = FLASH_ESTOPPED;
    }

    metaData.objFlags   = 0;
    metaData.expTime    = 0;
    metaData.createTime = 0;
    metaData.keyLen     = pm->key.len;
    metaData.dataLen    = 0;
    metaData.sequence   = pm->op_meta.seqno;
    exptime             = 0;
    createtime          = 0;
    inval_cache         = SDF_FALSE;
    must_update         = SDF_FALSE;

    pflash_data2 = NULL;

    if (type_replacement != ZDUMY) {
        in_mtype = type_replacement;
    } else {
        in_mtype = pm->msgtype;
    }

    /*
     * XXX: All other failures currently return SDF_SUCCESS; we need a merge
     * of struct sdf_msg and SDF_protocol_msg so everything is getting
     * ACK/NACK from the same place.
     */

    /*
     * Run through switch statement to get correct SDF_protocol_msg type
     * in spite of shard get failure.
     */
    switch (in_mtype) {
    case HFXST: /* flash exists */
        if (success) {
	    (metaData.keyLen)--; // adjust for added null from SDF
            success = (retcode = flashGet(pshard, &metaData, pflash_key, NULL, 0)) == FLASH_EOK;
	    (metaData.keyLen)++; // adjust for added null from SDF
        }
        if (!success) {
            status = get_status(retcode);
        }
        fh_mtype = success ? FHXST : FHNXS;
        break;

    case HFGFF: /* flash get */
        if (success) {
            pflash_data = NULL;
	    (metaData.keyLen)--; // adjust for added null from SDF
            success = ((retcode = flashGet(pshard, &metaData, pflash_key, &pflash_data, 0)) == FLASH_EOK);
	    (metaData.keyLen)++; // adjust for added null from SDF
            if (!success) {
                status = get_status(retcode);
            }
        } else {
            pflash_data = NULL;
        }
        if (pflash_data != NULL) {
            pdata      = pflash_data;
            data_size  = metaData.dataLen;
            exptime    = metaData.expTime;
            createtime = metaData.createTime;
            sequence   = metaData.sequence;
#ifdef MALLOC_TRACE
                UTMallocTrace("home_flash_wrapper: flashGet", FALSE, FALSE, FALSE, (void *) pflash_data, data_size);
#endif // MALLOC_TRACE
        }

        fh_mtype = success ? FHDAT : FHGTF;
        break;

    case HFPTF: /* flash put */
    case HFSET: /* flash set */
    case HFCIF: /* flash create */
        if ((!(pm->flags & f_writethru)) && (in_mtype == HFSET)) {
	    /*  If this is a writeback container, just update the cache.
	     *  Don't go to flash!
	     */
	    if (success) {
		must_update = SDF_TRUE;
	    }
	} else {
	    switch (in_mtype) {
	    case HFPTF: flash_flags = FLASH_PUT_TEST_NONEXIST; break;
	    case HFSET: flash_flags = 0; break;
	    case HFCIF: flash_flags = FLASH_PUT_TEST_EXIST; break;
	    case HFCZF: flash_flags = FLASH_PUT_TEST_EXIST; break;
	    default:
		plat_fatal("unhandled message type");
	    }

	    inval_cache = SDF_TRUE;
	    plat_assert(pm->data_offset == 0);
	    if (success) {
		if (in_mtype == HFCZF) {
		    pflash_data2 = build_string_type_zero(pm->data_size);
		    free_pflash_data2 = 1;
		} else {
		    pflash_data2 = (char *)pm + sizeof (SDF_protocol_msg_t);
		    free_pflash_data2 = 0;
		}
		plat_assert_imply(pflash_data2, pm->data_size > 0);

		metaData.dataLen    = pm->data_size;

		/* Adjust for clock skew */
		metaData.createTime = sdf_get_current_time();
		if (pm->exptime > 1) {
		    metaData.expTime = (pm->exptime - pm->createtime) + metaData.createTime;
		} else {
		    metaData.expTime    = pm->exptime;
		}
		
		plat_log_msg(21311, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_TRACE,
			     "Adjust time current_time_in: %d current_time_local: %d clock_skew: %d",
			     pm->createtime, metaData.createTime, pm->createtime - metaData.createTime);

		plat_log_msg(21312, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_TRACE,
			     "data forwarding recv: key: %s len: %d data: 0x%lx",  pflash_key, metaData.keyLen, (uint64_t) pflash_data2);

                if (!check_flash_space(pai->pcs, pshard)) {
		    (metaData.keyLen)--; // adjust for added null from SDF
		    retcode = flashPut(pshard, &metaData, pflash_key, pflash_data2,
				       flash_flags);
		    (metaData.keyLen)++; // adjust for added null from SDF
		    finish_write_in_flight(pai->pcs);
		} else {
		    retcode = FLASH_ENOSPC;
		}
		if (retcode == FLASH_EOK && (pm->flags & f_sync)) {
		    /*
		     * XXX: drew 2009-06-16 This should be a single key flush
		     * when we support that.  Since persistence errors are possible
		     * on flush this should not return void.
		     */
    #if 0
		    shardFlushAll(pshard, 0);
    #else
		    ssd_shardSync(pshard);
    #endif
		}
		status  = get_status(retcode);
		success = retcode == FLASH_EOK;
		if (!success) {
		    sequence = SDF_SEQUENCE_NO_INVALID;
		} else {
		    sequence = metaData.sequence;
		}

		if (free_pflash_data2) {
		    plat_free(pflash_data2);
		}

		if ((retcode != FLASH_EOK) && (retcode != FLASH_ENOENT)) {
		    SDF_boolean_t  delflag;

		    delflag = SDF_TRUE;
		    if ((flash_flags == FLASH_PUT_TEST_EXIST) &&
			(retcode == FLASH_EEXIST))
		    {
			/*  Don't delete because this is a special
			 *  failure case.
			 */
			delflag = SDF_FALSE;
		    } else if ((flash_flags == FLASH_PUT_TEST_NONEXIST) &&
			       (retcode == FLASH_ENOENT))
		    {
			/*  Don't delete because this is a special
			 *  failure case.
			 */
			delflag = SDF_FALSE;
		    }
		    
		    if (delflag) {
			int   retcode2;

			/*  Delete the object.
			 *  This is done so we can keep the master node in some sort
			 *  of consistency with the slave.
			 */
			
			(metaData.keyLen)--; // adjust for added null from SDF
			retcode2 = flashPut(pshard, &metaData, pflash_key, NULL, 
					    FLASH_PUT_TEST_NONEXIST);
			(metaData.keyLen)++; // adjust for added null from SDF

			if ((retcode2 != FLASH_EOK) && (retcode2 != FLASH_ENOENT)) {
			    /*  Let sender know that we couldn't delete the object
			     *  to clean up after a put failure.
			     */
			    retcode = FLASH_RMT_EDELFAIL;
			    status  = get_status(retcode);
			}
		    }
		}

	    }
        }
        fh_mtype = home_flash_response_type(in_mtype, status);
        break;

    case HZSF: /* flash delete */
        inval_cache = SDF_TRUE;
        if (success) {
	    (metaData.keyLen)--; // adjust for added null from SDF
            retcode = flashPut(pshard, &metaData, pflash_key, NULL,
                               FLASH_PUT_TEST_NONEXIST);
	    (metaData.keyLen)++; // adjust for added null from SDF
            if (retcode == FLASH_EOK && (pm->flags & f_sync)) {
                /*
                 * XXX: drew 2009-06-16 This should be a single key flush
                 * when we support that.  Since persistence errors are possible
                 * on flush this should not return void.
                 */
#if 0
                shardFlushAll(pshard, 0);
#else
                ssd_shardSync(pshard);
#endif
            }

            success = retcode == FLASH_EOK;
            if (!success) {
                status = get_status(retcode);
            }
        }
        fh_mtype = success ? FHDEC : FHDEF;
        break;

    case HFFLS: /* flush object */
        if (success) {
	    status = SDF_I_RemoteFlushObject(pai, pm->cguid, pm->key.key, pm->key.len-1);
	    if ((status != SDF_SUCCESS) && (status != SDF_OBJECT_UNKNOWN)) {
	        success = 0;
	    }
	}
        fh_mtype = success ? FHFCC : FHFCF;
        break;

    case HFFIV: /* flush inval object */
        if (success) {
	    status = SDF_I_RemoteFlushInvalObject(pai, pm->cguid, pm->key.key, pm->key.len-1);
	    if ((status != SDF_SUCCESS) && (status != SDF_OBJECT_UNKNOWN)) {
	        success = 0;
	    }
	}
        fh_mtype = success ? FHFIC : FHFIF;
        break;

    case HFINV: /* inval object */
        if (success) {
	    status = SDF_I_RemoteInvalObject(pai, pm->cguid, pm->key.key, pm->key.len-1);
	    if ((status != SDF_SUCCESS) && (status != SDF_OBJECT_UNKNOWN)) {
	        success = 0;
	    }
	}
        fh_mtype = success ? FHINC : FHINF;
        break;

    default:
        plat_fatal("");
    }

    /*
     * XXX: drew 2009-06-03 internal operations from meta_storage don't have
     * a valid cguid associated with them.  This doesn't look to be a problem
     * except where invalidation is involved, so I've disabled this check
     * for now and made invalidation conditional on that.
     */
    if (inval_ok && (inval_cache || must_update) &&
#ifndef notyet
        _sdf_cguid_valid(pm->cguid)
#else
        1
#endif
        ) {
        /*  This is a replication operation, so I must invalidate or
	 *  update any local cached copies to maintain consistency.
         */
        if (pm->node_from != pts->mynode) {
	    if (must_update) {
		SDF_time_t  t_create, t_exp;

		t_create = sdf_get_current_time();
		if (pm->exptime > 1) {
		    t_exp = (pm->exptime - pm->createtime) + t_create;
		} else {
		    t_exp = pm->exptime;
		}
		inval_status = SDF_I_RemoteUpdateObject(pai, pm->cguid, 
		                    pm->key.key, pm->key.len-1, pm->data_size, 
				    (char *) pm + sizeof(SDF_protocol_msg_t), 
				    t_create, t_exp);

		 if ((inval_status != SDF_SUCCESS) && (inval_status != SDF_OBJECT_UNKNOWN)) {
		      plat_log_msg(30571, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
				 "Object update failed with status %s(%d)",
				 SDF_Status_Strings[inval_status], inval_status);
		}
	    } else {
		 inval_status = SDF_I_RemoteInvalObject(pai, pm->cguid, pm->key.key, pm->key.len-1);
		 // inval_status = SDF_SUCCESS;
		 if ((inval_status != SDF_SUCCESS) && (inval_status != SDF_OBJECT_UNKNOWN)) {
		     plat_log_msg(21313, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
				 "Object invalidation failed with status %s(%d)",
				 SDF_Status_Strings[inval_status], inval_status);
		}
	    }
        }
    }

#ifdef INCLUDE_TRACE_CODE
    plat_log_msg(21314, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_TRACE,
	"[tag %d, node %d, pid %d, thrd %d] Slow path flash call: %s '%s':%"PRIu64", pshard=%p, ret=%s(%d), status=%s(%d)",
	pm->tag, pts->mynode, plat_getpid(), pts->thrdnum,
	SDF_Protocol_Msg_Info[in_mtype].name, pflash_key, pm->cguid, pshard,
	flashRetCodeName(retcode), retcode, SDF_Status_Strings[status], status);
#endif

    /*   Convert the flash return code to an SDF status
     *   so that the replication stuff works correctly.
     */
    if (status == SDF_SUCCESS) {
	status = get_status(retcode);
    }
    new_msg = home_load_msg(pm->node_to /* from */,
                            pm->node_from /* to */,
                            pm, fh_mtype, pdata,
                            data_size,
                            exptime, createtime, sequence,
                            status, pmsize, 0, 0, 0);

    // pm_new = (SDF_protocol_msg_t *) (new_msg->msg_payload);

    if (pflash_data != NULL) {
        flashFreeBuf(pflash_data);
#ifdef MALLOC_TRACE
            UTMallocTrace("home_flash_wrapper: flash buf", FALSE, TRUE, FALSE, (void *) pflash_data, 0);
#endif // MALLOC_TRACE
    }

    /*  The unlock for mapwait must go here because this lock
     *  is used by SDFActionStopContainer() to ensure that all
     *  pending flash accesses are completed.
     */
    fthUnlock(mapwait);

    return (new_msg);
}

static SDF_boolean_t check_stopflag(SDF_action_thrd_state_t *pts, SDF_shardid_t shard)
{
    SDF_boolean_t            stopflag;
    SDFTLMap2Entry_t        *psme;
    SDF_home_flash_entry_t  *phfe;

    psme = SDFTLMap2Get(&(pts->shardmap), shard);

    if (psme != NULL) {
	phfe     = (SDF_home_flash_entry_t *) psme->contents;
	stopflag = phfe->stopflag;
    } else {
        stopflag = SDF_TRUE;
    }

    if (stopflag) {
	plat_log_msg(21315, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
		     "home_flash_shard_wrapper operation to a stopped container"
		     "for shardid 0x%lx", shard);
    }
    return(stopflag);
}

struct sdf_msg *
home_flash_shard_wrapper(
#ifdef MULTIPLE_FLASH_DEV_ENABLED
                         struct flashDev **in_flash_dev,
#else
                         struct flashDev *in_flash_dev,
#endif
                         uint32_t flash_dev_count,
                         SDF_internal_ctxt_t *pai_in,
                         struct sdf_msg *recv_msg,
                         SDF_size_t *pmsize) 
{

    struct objMetaData            *metaData  = NULL; // metadata from flash
    struct flashDev        *flash_dev;
    struct shard           *pshard = NULL;
    SDF_action_init_t      *pai;
    SDF_action_thrd_state_t *pts;

    struct SDF_shard_meta  *recv_shard_meta = NULL;
    struct sdf_msg         *new_msg;
    SDF_protocol_msg_t     *recv_pm;
    SDF_protocol_msg_type_t new_mtype = 0;
    SDF_size_t              data_size = 0;
    uint64_t                sequence;
    uint64_t                expiry_time = 0;
    uint64_t                create_time = 0;
    uint32_t                                flags = 0;
    void                   *pdata = NULL;
    char                   *key = NULL;

    int                     success;
    int                     shard_flags;
    int                     status = SDF_SUCCESS;
    int                     rc;
    // xxxzzz fthWaitEl_t            *mapwait;
    SDF_boolean_t           stopflag;

    plat_assert(recv_msg);

    sequence = SDF_SEQUENCE_NO_INVALID;

    pts = ((SDF_action_init_t *) pai_in)->pts;
    pai = pts->pai;

    recv_pm = (SDF_protocol_msg_t *)recv_msg->msg_payload;

    // Remove these until Jake can check forward compatibility!
    // plat_assert(recv_pm->current_version == PROTOCOL_MSG_VERSION);
    // plat_assert(recv_pm->supported_version == PROTOCOL_MSG_VERSION);

    success = 1; // default

    /***************************************************************
     *
     *   "Stopped Container" interlock check
     *
     ***************************************************************/

    /*  In addition to serializing accesses to the shardmap hashtable,
     *  (the shardmap hashtable is accessed by the home_flash threadpool
     *  as well as the memcached worker thread pool)
     *  mapwait is used by SDFActionStopContainer() to ensure that all
     *  pending flash accesses are completed.
     */
    // xxxzzz mapwait = fthLock(&(pts->shardmap_lock), 1, NULL);

    switch (recv_pm->msgtype) {
	case HFCSH: // Home to Flash Create Shard
	    plat_assert(recv_pm->data_size == sizeof(struct SDF_shard_meta));
	    recv_shard_meta = (struct SDF_shard_meta *)((char *)(recv_pm + 1) +
							recv_pm->data_offset);
	    stopflag = check_stopflag(pts, recv_shard_meta->sguid);
	    break;
	case HFSSH:  // sync the shard to flash
	case HFGLS:
	case HFGIC: // Home to Flash Get Iteration Cursors
	case HFGBC: // Home to Flash Get By Cursor
	case HFSCN: 
	case HFGSN:
	case HFDSH: // Home to Flash Delete Shard
	    stopflag = check_stopflag(pts, recv_pm->shard);
	    break;
	case HFNOP: // Home to Flash noop
	case HFRVG: // Home to Flash Release VIP Group
	    stopflag = SDF_FALSE;
	    break;
	case HFSRR: // Home to Flash Start Replication
	case HFSPR: // Home to Flash Stop Replication
	case HFFLA: // Home to Flash Flush All
	case HFFLC: // Home to Flash Flush Container
	case HFFLI: // Home to Flash Flush Inval Container
	case HFINC: // Home to Flash Inval Container
	case HFPBD: // Home to Flash Prefix-Based Delete
	    stopflag = check_stopflag(pts, recv_pm->shard);
	    break;
	default:
	    stopflag = SDF_TRUE;
	    plat_fatal("Bad message type");
	    break;
    }
    stopflag = SDF_FALSE; // xxxzzz fix me

    if (stopflag) {
	switch (recv_pm->msgtype) {
	    case HFCSH: new_mtype = FHCSF; break;
	    case HFSSH: new_mtype = FHSSF; break;
	    case HFGLS: new_mtype = FHGLF; break;
	    case HFGIC: new_mtype = FHGIF; break;
	    case HFGBC: new_mtype = FHGCF; break;
	    case HFSCN: new_mtype = FHSCF; break;
	    case HFGSN: new_mtype = FHGSF; break;
	    case HFDSH: new_mtype = FHDSF; break;
	    case HFRVG: new_mtype = FHRVF; break;
	    case HFNOP: new_mtype = FHNPC; break;
	    case HFSRR: new_mtype = FHSRF; break;
	    case HFSPR: new_mtype = FHSPF; break;
	    case HFFLA: new_mtype = FHFLF; break;
	    case HFFLC: new_mtype = FHLCF; break;
	    case HFFLI: new_mtype = FHLIF; break;
	    case HFINC: new_mtype = FHCIF; break;
	    case HFPBD: new_mtype = FHPBF; break;
	    default:
		plat_fatal("Bad message type");
		break;
	}
    }

    /***************************************************************
     *
     *   End of "Stopped Container" interlock check
     *
     ***************************************************************/

    if (!stopflag) {
	switch (recv_pm->msgtype) {
	case HFCSH: // Home to Flash Create Shard
	    /* XXX should spew instead of asserting */
	    plat_assert(recv_pm->data_size == sizeof(struct SDF_shard_meta));
	    recv_shard_meta = (struct SDF_shard_meta *)((char *)(recv_pm + 1) +
							recv_pm->data_offset);

	    if ((recv_pm->flags & f_open_ctnr) ||
		(recv_msg->msg_src_vnode == recv_msg->msg_dest_vnode))
	    {
		success = !(shard_init_flags(recv_shard_meta, &shard_flags));
		#ifdef MULTIPLE_FLASH_DEV_ENABLED
		    flash_dev = get_flashdev_from_shardid(in_flash_dev, recv_shard_meta->sguid, flash_dev_count);
		#else
		    flash_dev = in_flash_dev;
		#endif

		if (success) {
		    plat_log_msg(21316, LOG_CAT_SHARD,
				 PLAT_LOG_LEVEL_TRACE,
				 "Create shard sguid 0x%lx flags %X quota %llu numObjs %d",
				 recv_shard_meta->sguid, shard_flags,
				 (unsigned long long)recv_shard_meta->quota,
				 recv_shard_meta->num_objs);
		    success = shardCreate(flash_dev, recv_shard_meta->sguid,
					  shard_flags, recv_shard_meta->quota,
					  recv_shard_meta->num_objs) != NULL; // Def set in props file
		    if (!success) {
			status = SDF_FAILURE_STORAGE_WRITE;
		    } else if (recv_pm->flags & f_open_ctnr) {
		
		    }
		    if (success) {
			if (recv_pm->flags & f_open_ctnr) {
			    pshard = shardOpen(flash_dev, recv_shard_meta->sguid);
			    if (!pshard) {
				plat_log_msg(21318, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
					     "shardOpen failed for shardid 0x%lx", recv_shard_meta->sguid);
				success = 0;
				status = SDF_CONTAINER_UNKNOWN;
			    }
			}
		    }
		} else {
		    plat_log_msg(21319, LOG_CAT_SHARD,
				 PLAT_LOG_LEVEL_ERROR,
				 "create shard sguid 0x%lx failed, can't create flags",
				 recv_shard_meta->sguid);
		    status = SDF_FAILURE_GENERIC;
		}
	    }

	    new_mtype = success ? FHCSC : FHCSF;
	    break;
	case HFSSH:  // sync the shard to flash
	   /*XXX: Cut/Paste code snippets from above. We can use some
	     refactoring in this module */
    #ifdef MULTIPLE_FLASH_DEV_ENABLED
	    flash_dev = get_flashdev_from_shardid(in_flash_dev, recv_pm->shard, flash_dev_count);
    #else
	    flash_dev = in_flash_dev;
    #endif

	    pshard = shardFind(flash_dev, recv_pm->shard);
	    if (!pshard) {
		plat_log_msg(21320, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
			     "flash returned no pshard for shardid 0x%lx",
			     recv_pm->shard);
		success = 0;
		status = SDF_CONTAINER_UNKNOWN;
	    }
	    if (success) {
		plat_log_msg(21321, LOG_CAT_SHARD,
			     PLAT_LOG_LEVEL_TRACE,
			     "sync shard for shardid %lu",
			     recv_pm->shard);

		if (!(recv_pm->flags & f_writethru)) {
		    drain_store_pipe_remote_request(pai);
		}
		ssd_shardSync(pshard);

		new_mtype = FHSSC;
	    } else {
		new_mtype = FHSSF;
	    }
	    break;
	case HFGLS:
	    recv_shard_meta = (struct SDF_shard_meta *)((char *)(recv_pm + 1) +
							recv_pm->data_offset);

	   /*XXX: Cut/Paste code snippets from above. We can use some
	     refactoring in this module */
    #ifdef MULTIPLE_FLASH_DEV_ENABLED
	    flash_dev = get_flashdev_from_shardid(in_flash_dev, recv_shard_meta->sguid, flash_dev_count);
    #else
	    flash_dev = in_flash_dev;
    #endif

	    pshard = shardFind(flash_dev, recv_pm->shard);
	    if (!pshard) {
		plat_log_msg(21322, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
			     "flash returned no pshard for cwguid %llu key %s"
			     " shardid %lx",
			     (unsigned long long)recv_pm->cguid, (char *)recv_pm->key.key,
			     recv_pm->shard);
		success = 0;
		status = SDF_CONTAINER_UNKNOWN;

	    }
	    if(success) {
		plat_log_msg(21323, LOG_CAT_SHARD,
			     PLAT_LOG_LEVEL_TRACE,
			     "get_last_sequence sguid 0x%lx quota %llu numObjs %d",
			     recv_shard_meta->sguid,
			     (unsigned long long)recv_shard_meta->quota,
			     recv_shard_meta->num_objs);

		pdata = NULL;

		sequence = flashGetHighSequence(pshard);

		plat_log_msg(21324, LOG_CAT_SHARD,
			     PLAT_LOG_LEVEL_TRACE,
			     "got sequence num %ld for  sguid 0x%lx",
			     sequence, recv_shard_meta->sguid);

		new_mtype = FHGLC;
	    } else {
		new_mtype = FHGLF;
	    }
	    break;
	case HFGIC: // Home to Flash Get Iteration Cursors
	{
	    it_cursor_t * cursors;
	    resume_cursor_t * resume_cursor_in;
	    void * data = NULL;     // data ptr from flash

	    new_mtype = FHGIF; // Flash to Home Get Iteration Cursor Fail

	     /* More cut and paste */
    #ifdef MULTIPLE_FLASH_DEV_ENABLED
	    flash_dev = get_flashdev_from_shardid(in_flash_dev, recv_shard_meta->sguid,
						  flash_dev_count);
    #else
	    flash_dev = in_flash_dev;
    #endif

	    pshard = shardFind(flash_dev, recv_pm->shard);
	    if (!pshard) {
		plat_log_msg(21325, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
			     "flash returned no pshard for cwguid %llu key %s"
			     " shardid 0x%lx",
			     (unsigned long long)recv_pm->cguid, (char *)recv_pm->key.key,
			     recv_pm->shard);
		success = 0;
		status = SDF_CONTAINER_UNKNOWN;

	    }

	    data = (unsigned char *) recv_pm + sizeof(SDF_protocol_msg_t);

	    if (success) {
		if (recv_pm->data_size) {
		    resume_cursor_in = (resume_cursor_t *)data;
		} else {
		    resume_cursor_in = 0;
		}

		// Need to make sure everything is to the log on disk at least
		ssd_shardSync(pshard);

		rc = flashGetIterationCursors(pshard, recv_pm->seqno, recv_pm->seqno_len, recv_pm->seqno_max,
						   resume_cursor_in, &cursors);
		if (rc == FLASH_EOK) {
		    pdata = cursors;
		    data_size = sizeof(it_cursor_t) + (cursors->cursor_len * cursors->cursor_count);
		    new_mtype = FHGIC; // Flash to Home Get Iteration Cursors Complete
		} else {
		    new_mtype = FHGIF; // Flash to Home Get Iteration Cursor Fail
		}

		status = get_status(rc);
	    }
	    break;
	}
	case HFGBC: // Home to Flash Get By Cursor
	{
            SDF_container_meta_t * meta;
	    void * data = NULL;     // data ptr from flash
	    void * cursor;

	    new_mtype = FHGCF;         // Flash to Home Get by Cursor Fail

    #ifdef MULTIPLE_FLASH_DEV_ENABLED
	    flash_dev = get_flashdev_from_shardid(in_flash_dev, recv_shard_meta->sguid,
						  flash_dev_count);
    #else
	    flash_dev = in_flash_dev;
    #endif

	    metaData = proto_plat_alloc_arena(sizeof(struct objMetaData), NonCacheObjectArena);
	    if (!metaData) {
		plat_log_msg(21326, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
			     "Home Flash Get by Cursor allocation failure\n");
		success = 0;
		status = SDF_FLASH_ENOMEM;
	    }

	    pshard = shardFind(flash_dev, recv_pm->shard);
	    if (!pshard) {
		plat_log_msg(21325, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
			     "flash returned no pshard for cwguid %llu key %s"
			     " shardid 0x%lx",
			     (unsigned long long)recv_pm->cguid, (char *)recv_pm->key.key,
			     recv_pm->shard);
		success = 0;
		status = SDF_CONTAINER_UNKNOWN;
	    }

            meta = sdf_get_preloaded_ctnr_meta(pai->pcs, recv_pm->cguid);
            if (!meta) {
                success = 0;
                status = SDF_CONTAINER_UNKNOWN;
            }

	    data = (void *) recv_pm + sizeof(SDF_protocol_msg_t);

	    if (success) {
		cursor = (void *)data;

                rc = flashGetByCursor(pshard, recv_pm->data_size, cursor, metaData, &key, &data, 0, meta->flush_time);

		switch (rc) {
		case FLASH_EOK:
		    pdata = data;
		    data_size = metaData->dataLen;
		    expiry_time = metaData->expTime;
		    create_time = metaData->createTime;
		    sequence = metaData->sequence;
		    new_mtype = FHGCC; // Flash to Home Get by Cursor Complete
		    break;
		case FLASH_ENOENT:
		    flags |= f_tombstone;
		    data_size = 0;
		    sequence = metaData->sequence;
		    new_mtype = FHGCC; // Flash to Home Get by Cursor Complete
		    break;
		case FLASH_ESTALE:
		    new_mtype = FHGCF; // Flash to Home Get by Cursor Fail
		    break;
		default:
		    new_mtype = FHGCF; // Flash to Home Get by Cursor Fail
		}

		status = get_status(rc);
	    }
	    break;
	}
	case HFSCN: // Home to Flash Scan Object Sequence
	{
	    char * data = NULL;     // data ptr from flash

#ifdef MULTIPLE_FLASH_DEV_ENABLED
	    flash_dev = get_flashdev_from_shardid(in_flash_dev, recv_shard_meta->sguid,
						  flash_dev_count);
#else
	    flash_dev = in_flash_dev;
#endif

	    metaData = proto_plat_alloc_arena(sizeof(struct objMetaData), NonCacheObjectArena);
	    plat_assert(metaData);

	    /*
	     * XXX: drew 2009-06-24 Could we not have the same cut-and-paste every
	     * where?
	     *
	     * FIXME: The API allows the key to be a binary string which precludes
	     * printing it as a simple string. %*.*s works with the arguments being
	     * key.len, key.len, and key.key.
	     */
	    pshard = shardFind(flash_dev, recv_pm->shard);
	    if (!pshard) {
		plat_log_msg(21325, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
			     "flash returned no pshard for cwguid %llu key %s"
			     " shardid 0x%lx",
			     (unsigned long long)recv_pm->cguid, (char *)recv_pm->key.key,
			     recv_pm->shard);
		success = 0;
		status = SDF_CONTAINER_UNKNOWN;
	    }

	    if (success) {
		success = flashSequenceScan(pshard, &recv_pm->cookie1, &recv_pm->cookie2,
					    metaData, &key, &data, 0 /*no flags*/);
		status = success;
	    }

	}
	break;
	case HFGSN:
	    recv_shard_meta = (struct SDF_shard_meta *)((char *)(recv_pm + 1) +
							recv_pm->data_offset);

	   /*XXX: Cut/Paste code snippets from above. We can use some
	     refactoring in this module */
    #ifdef MULTIPLE_FLASH_DEV_ENABLED
	    flash_dev = get_flashdev_from_shardid(in_flash_dev, recv_shard_meta->sguid, flash_dev_count);
    #else
	    flash_dev = in_flash_dev;
    #endif

	    pshard = shardFind(flash_dev, recv_pm->shard);
	    if (!pshard) {
		plat_log_msg(21325, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
			     "flash returned no pshard for cwguid %llu key %s"
			     " shardid 0x%lx",
			     (unsigned long long)recv_pm->cguid, (char *)recv_pm->key.key,
			     recv_pm->shard);
		success = 0;
		status = SDF_CONTAINER_UNKNOWN;

	    }
	    if(success) {
		plat_log_msg(21327, LOG_CAT_SHARD,
			     PLAT_LOG_LEVEL_TRACE,
			     "get_sequence sguid 0x%lx quota %llu numObjs %d",
			     recv_shard_meta->sguid,
			     (unsigned long long)recv_shard_meta->quota,
			     recv_shard_meta->num_objs);

		pdata = NULL;

		// XXX Drew, this is where to plug your code in
		//sequence = drewsGetSeqnoCall();

		plat_log_msg(21324, LOG_CAT_SHARD,
			     PLAT_LOG_LEVEL_TRACE,
			     "got sequence num %ld for  sguid 0x%lx",
			     sequence, recv_shard_meta->sguid);

		new_mtype = FHGSC;
	    } else {
		new_mtype = FHGSF;
	    }
	    break;
	case HFDSH: // Home to Flash Delete Shard

    #ifdef MULTIPLE_FLASH_DEV_ENABLED
	    flash_dev = get_flashdev_from_shardid(in_flash_dev, recv_pm->shard,
						  flash_dev_count);
    #else
	    flash_dev = in_flash_dev;
    #endif

	    pshard = shardFind(flash_dev, recv_pm->shard);
	    if (!pshard) {
		plat_log_msg(21320, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
			     "flash returned no pshard for shardid 0x%lx",
			     recv_pm->shard);
		success = 0;
		status = SDF_CONTAINER_UNKNOWN;
	    }

	    if (success) {
		if (ssd_shardDelete(pshard) != 0) {
		    success = 0;
		    plat_log_msg(21328, LOG_CAT_SHARD,
				 PLAT_LOG_LEVEL_TRACE,
				 "flashDelete failed for sguid 0x%lx",
				 recv_pm->shard);
		}
	    }
	    new_mtype = success ? FHDSC : FHDSF;
	    if (success) {
		plat_log_msg(21329, LOG_CAT_SHARD,
			     PLAT_LOG_LEVEL_TRACE,
			     "flashDelete succeeded for sguid 0x%lx",
			     recv_pm->shard);

		new_mtype = FHDSC;
	    } else {
		new_mtype = FHDSF;
		status = SDF_FLASH_DELETE_FAILED;
	    }
	    break;

	case HFRVG: // Home to Flash Release VIP Group
	    rc = simple_replicator_remove_vip_group(pai, recv_pm->node_from);
	    if (rc == SDF_SUCCESS) {
		new_mtype = FHRVC;
	    } else {
		new_mtype = FHRVF;
	    }

	    status = rc;
	    break;

	case HFNOP: // Home to Flash noop
	{
	    status = SDF_SUCCESS;
	    new_mtype = FHNPC;
	    create_time = sdf_get_current_time();

            if (sdf_rec_funcs) {
                pdata = plat_malloc(sizeof(sdf_hfnop_t));

                if (!pdata)
                    plat_assert(0);
                (*sdf_rec_funcs->nop_fill)((sdf_hfnop_t *) pdata);
                data_size = sizeof(sdf_hfnop_t);
            }
	    break;
	}


    #ifdef SIMPLE_REPLICATION
	case HFSRR: // Home to Flash Start Replication
	{
	    // direction[0] - master
	    // direction[1] - "slave"
	    rep_cntl_msg_t *cntl_msg;
	    cntl_msg = (void *) recv_pm + sizeof(SDF_protocol_msg_t);

	    if (cntl_msg->direction[0] == recv_pm->node_to) {
		// We are the master, so start replicating
		plat_log_msg(30553, LOG_CAT_SHARD, PLAT_LOG_LEVEL_TRACE,
			     "Home to Flash Start Replication (I am master): cguid=%"PRIu64", node_from=%d",
			     recv_pm->cguid, recv_pm->node_from);
		rc = simple_replicator_enable_replication(pai, recv_pm->cguid, recv_pm->node_from);
	    } else {
		// We are the slave so get caught up w/ the master
		plat_log_msg(30554, LOG_CAT_SHARD, PLAT_LOG_LEVEL_TRACE,
			     "Home to Flash Start Replication (I am slave): cntr_id=%d, cntr_status=%d",
			     cntl_msg->cntr_id, cntl_msg->cntr_status);
		SDFSetContainerStatus(pai,cntl_msg->cntr_id, cntl_msg->cntr_status);
		rc = simple_replicator_start_new_replica(pai, cntl_msg->direction[0], cntl_msg->direction[1],cntl_msg->cntr_id,0);

		/* Convert rc to SDF_Status_t */
		if (!rc) {
		    rc = SDF_SUCCESS;
		} else {
		    rc = SDF_FAILURE;
		}
	    }

	    if (rc == SDF_SUCCESS) {
		new_mtype = FHSRC;
	    } else {
		new_mtype = FHSRF;
	    }
	    
	    status = rc;

	    break;
	}
	case HFSPR: // Home to Flash Stop Replication
	    rc = simple_replicator_disable_replication(pai->pcs, recv_pm->cguid, recv_pm->node_from);

	    if (rc == SDF_SUCCESS) {
		new_mtype = FHSPC;
	    } else {
		new_mtype = FHSPF;
	    }
	    
	    status = rc;

	    break;
	case HFFLA: // Home to Flash Flush All
	{
            qrep_node_state_t *pns;
            qrep_state_t * ps;
	    SDF_appreq_t par;
            time_t clock_skew;

            ps = &(pai->pcs->qrep_state);        
            pns = &(ps->node_state[recv_pm->node_from]);

	    /* Adjust for clock skew */
	    par.curtime = sdf_get_current_time();
            clock_skew = recv_pm->curtime - par.curtime;

	    if (recv_pm->flushtime > 1) {
                par.invtime = recv_pm->flushtime - clock_skew;
	    } else {
		par.invtime = recv_pm->flushtime;
	    }

            /* Save off clock skew */
            pns->clock_skew = clock_skew;

            plat_log_msg(21330, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_TRACE,
                         "HFFLA: current_time_in: %d flush_time_in: %d current_time_local: %d clock_skew: %d flush_time_local: %d",
                         recv_pm->curtime, recv_pm->flushtime, par.curtime, (uint32_t)clock_skew, par.invtime);

	    par.ctnr = recv_pm->cguid;
	    par.reqtype = APICD;

	    flush_all_remote_request(pai, &par);

	    rc = SDF_SUCCESS;

	    if (rc == SDF_SUCCESS) {
		new_mtype = FHFLC;
	    } else {
		new_mtype = FHFLF;
	    }
	    
	    status = rc;

	    break;
	}
	case HFFLC: // Home to Flash Flush Container
	case HFFLI: // Home to Flash Flush Inval Container
	case HFINC: // Home to Flash Inval Container
	{
            // qrep_node_state_t *pns;
            // qrep_state_t * ps;
	    SDF_appreq_t par;

            // ps = &(pai->pcs->qrep_state);        
            // pns = &(ps->node_state[recv_pm->node_from]);

	    par.ctnr    = recv_pm->cguid;
	    switch (recv_pm->msgtype) {
	        case HFFLC: par.reqtype = APFCO; break;
	        case HFFLI: par.reqtype = APFCI; break;
	        case HFINC: par.reqtype = APICO; break;
		default:
		    plat_log_msg(30629, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
			   "unknown request type (%d)!", recv_pm->msgtype);
		    plat_assert(0);
		    break;
	    }

	    rc = flush_inval_remote_request(pai, &par);

	    switch (recv_pm->msgtype) {
	        case HFFLC: new_mtype = (rc==SDF_SUCCESS) ? FHLCC:FHLCF; break;
	        case HFFLI: new_mtype = (rc==SDF_SUCCESS) ? FHLIC:FHLIF; break;
	        case HFINC: new_mtype = (rc==SDF_SUCCESS) ? FHCIC:FHCIF; break;
		default: plat_assert(0); break;
	    }
	    status = rc;

	    break;
	}
	case HFPBD: // Home to Flash Prefix-based Delete
	{
            // qrep_node_state_t    *pns;
            // qrep_state_t         *ps;
	    SDF_appreq_t          par;
	    struct objMetaData    flashMetaData;

            // ps = &(pai->pcs->qrep_state);        
            // pns = &(ps->node_state[recv_pm->node_from]);

	    par.ctnr    = recv_pm->cguid;
	    par.reqtype = recv_pm->msgtype;

	    rc = prefix_delete_remote_request(pai, &par, recv_pm->key.key, recv_pm->key.len - 4);
	    if (rc != SDF_SUCCESS) {
	        success = 0;
		status  = rc;
	    }

	    #ifdef MULTIPLE_FLASH_DEV_ENABLED
		flash_dev = get_flashdev_from_shardid(in_flash_dev, recv_pm->shard,
						      flash_dev_count);
	    #else
		flash_dev = in_flash_dev;
	    #endif

	    pshard = shardFind(flash_dev, recv_pm->shard);
	    if (!pshard) {
		plat_log_msg(21320, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
			     "flash returned no pshard for shardid 0x%lx",
			     recv_pm->shard);
		success = 0;
		status = SDF_CONTAINER_UNKNOWN;
	    }


            if (success) {
	        flashMetaData.keyLen  = recv_pm->key.len;
		flashMetaData.dataLen = 0;

		(flashMetaData.keyLen)--; // adjust for added null from SDF
		rc = flashPut(pshard, &flashMetaData, recv_pm->key.key, NULL,
			      FLASH_PUT_PREFIX_DELETE);
		(flashMetaData.keyLen)++; // adjust for added null from SDF
	    }

            if (success) {
		success = (rc == FLASH_EOK);
		if (rc != FLASH_EOK) {
		    status = get_status(rc);
		}
	    }
	    if (status == SDF_SUCCESS) {
	        new_mtype = FHPBC;
	    } else {
		new_mtype = FHPBF;
	    }
	    break;
	}

    #endif
	default:
	    plat_fatal("Bad message type");
	}
    }

    new_msg = home_load_msg(recv_pm->node_to /* from */,
                            recv_pm->node_from /* to */,
                            recv_pm, new_mtype,
                            pdata, data_size,
                            expiry_time, create_time,
                            sequence,
                            status, pmsize, 
                            metaData && metaData->keyLen ? key : 0,
                            metaData && key ? metaData->keyLen : 0,
                            flags);

    if (pdata) {
        plat_free(pdata);
    }
    if (metaData) {
        plat_free(metaData);
    }
    if (key) {
        plat_free(key);
    }

    /*  The unlock for mapwait must go here because this lock
     *  is used by SDFActionStopContainer() to ensure that all
     *  pending flash accesses are completed.
     */
    // xxxzzz fthUnlock(mapwait);

    return (new_msg);
}

static int
shard_init_flags(struct SDF_shard_meta *meta, int *pflags) {
    int flags = 0;

    switch (meta->type) {
    case SDF_SHARD_TYPE_BLOCK: flags |= FLASH_SHARD_INIT_TYPE_BLOCK; break;
    case SDF_SHARD_TYPE_OBJECT: flags |= FLASH_SHARD_INIT_TYPE_OBJECT; break;
    case SDF_SHARD_TYPE_LOG: flags |= FLASH_SHARD_INIT_TYPE_LOG; break;
    }

    switch (meta->persistence) {
    case SDF_SHARD_PERSISTENCE_YES:
        flags |= FLASH_SHARD_INIT_PERSISTENCE_YES;
        break;
    case SDF_SHARD_PERSISTENCE_NO:
        flags |= FLASH_SHARD_INIT_PERSISTENCE_NO;
        break;
    }

    switch (meta->eviction) {
    case SDF_SHARD_EVICTION_CACHE:
        flags |= FLASH_SHARD_INIT_EVICTION_CACHE;
        break;
    case SDF_SHARD_EVICTION_STORE:
        flags |= FLASH_SHARD_INIT_EVICTION_STORE;
        break;
    }

    /*
     * XXX: drew 2008-11-14 This should be specified by the replication
     * layer via a change in the CSH message, but that currently treats
     * the messages headed to flash as immutable.  Undo this when that
     * gets fixed
     */
    if (meta->replication_props.enabled  &&
        meta->replication_props.type != SDF_REPLICATION_NONE &&
        meta->replication_props.type != SDF_REPLICATION_SIMPLE) {
        flags |= FLASH_SHARD_SEQUENCE_EXTERNAL;
    }

    if (pflags) {
        *pflags = flags;
    }

    return (0);
}

SDF_protocol_msg_type_t
home_flash_response_type(SDF_protocol_msg_type_t msg_type,
                         SDF_status_t status) {
    int success = (status == SDF_SUCCESS);
    SDF_protocol_msg_type_t fh_mtype;

    switch (msg_type) {
#define item(caps, type, response_good, response_bad) \
    case caps:                                                                 \
        fh_mtype = success ? (response_good) : (response_bad);                 \
        break;
    HF_MSG_ITEMS()
#undef item
    default:
        plat_log_msg(21331, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                     "No error mapping for %s\n",
                     SDF_Protocol_Msg_Info[msg_type].shortname);
        plat_fatal("");
        break;
    }

    return (fh_mtype);
}

/**
 * @brief send reply message
 *
 * @param pts <IN> Per-thread state
 * @param send_msg <IN> Response that is being sent to the client
 * @param msize <IN> Response message payload size
 * @param recv_msg <IN> Original message to which this is a response
 * @returns 0 on success, non-zero on failure.t
 */
static int
hf_send_reply(struct SDF_flash_thrd_state *pts,
              struct sdf_msg *send_msg, int msize,
              struct sdf_msg *recv_msg) {
    int ret;

    /* to facilitate hashed fth mboxes, we will be abstracting out the sdf_msg_send in the near future anyway */
    struct sdf_resp_mbx mrespbx;
    struct sdf_resp_mbx *mresp = sdf_msg_initmresp(&mrespbx);
    sdf_fth_mbx_t req_mbx;

    req_mbx.release_on_send = 1;
    req_mbx.actlvl = SACK_NONE_FTH;

    ret = sdf_msg_send(send_msg, msize,
                       recv_msg->msg_src_vnode, /* dest */
                       recv_msg->msg_src_service, /* dest */
                       pts->pfs->config.my_node, /* src */
                       pts->pfs->config.flash_server_service, /* src */
                       FLSH_RESPOND, &req_mbx /* ar_mbx */,
                       sdf_msg_get_response(recv_msg, mresp));

    return (ret);
}

#ifdef notdef
/**
 * @brief Provide key and data formatted (padded) to flash requirements.
 *
 * XXX: drew 2008-07-15 On the write-to-flash case, this needs to be replaced
 * with messages that have key, data aligned and padded per FLASH_ALIGN()
 * requirements.
 *
 * More significantly, those messages will need to be allocated in memory
 * with known, pinned, PCI-consistent physical mappings that have been
 * acquired via the shmem interface.
 */
static char *
build_string_type(uint32_t len, char *pdata)
{
    char *fkey = NULL;

    // The string_t key should NOT include the NULL termination char!
    //
    // XXX: The flash align call is unecessary as of 2008-08-08 since Jim
    // is copying into appropriate physical memory.
    fkey = proto_plat_alloc_arena(FLASH_ALIGN_LEN(len), NonCacheObjectArena);
#ifdef MALLOC_TRACE
        UTMallocTrace("build_string_type", FALSE, FALSE, FALSE, (void *) fkey, FLASH_ALIGN_LEN(len));
#endif // MALLOC_TRACE
    if (fkey == NULL) {
        UTError("plat_alloc failed: fkey == NULL!");
    }

    if (pdata == NULL) {
        UTError("pdata == NULL!");
    }
#ifdef notdef
    /* xxxzzz remove this junk */
    UTMessage("pdata = 0x%p", pdata);

    if (fkey == NULL) {
        UTError("fkey == NULL!");
    }
#endif

    if (len > 0) {
        memcpy(fkey, pdata, len);
    }

    return (fkey);
}
#endif

static char *
build_string_type_zero(uint32_t len)
{
    char *fkey = NULL;

    // The string_t key should NOT include the NULL termination char!
    //
    // XXX: The flash align call is unecessary as of 2008-08-08 since Jim
    // is copying into appropriate physical memory.
    fkey = proto_plat_alloc_arena(FLASH_ALIGN_LEN(len), NonCacheObjectArena);
#ifdef MALLOC_TRACE
    UTMallocTrace("build_string_type_zero", FALSE, FALSE, FALSE, (void *) fkey, FLASH_ALIGN_LEN(len));
#endif // MALLOC_TRACE
    if (fkey == NULL) {
        UTError("plat_alloc failed: fkey == NULL!");
    }

    if (len > 0) {
        bzero(fkey, len);
    }

    return (fkey);
}

static struct event sdf_clock_event;

SDF_time_t sdf_get_current_time(void)
{
    if (memcached_get_current_time) {
        return memcached_get_current_time();
    } else {
        return sdf_current_time;
    }
}

/* 
 * time-sensitive callers can call it by hand with this, outside the 
 * normal ever-1-second timer 
 */
static void set_current_time(void) {
    struct timeval timer;

    gettimeofday( &timer, NULL );
    sdf_current_time = timer.tv_sec;

    plat_log_msg ( PLAT_LOG_ID_INITIAL, 
                   PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                   PLAT_LOG_LEVEL_TRACE,
                   "timer triggered, current_time=%ld", sdf_current_time );
}

static void sdf_clock_handler(const int fd, const short which, void *arg) {
    struct timeval t = {.tv_sec = 1, .tv_usec = 0};
    static int initialized = 0;

    if (initialized) {
        /* only delete the event if it's actually there. */
        evtimer_del(&sdf_clock_event);
    } else {
        initialized = 1;
    }

    evtimer_set(&sdf_clock_event, sdf_clock_handler, 0);
    event_base_set(sdf_main_base, &sdf_clock_event);
    evtimer_add(&sdf_clock_event, &t);

    set_current_time();
}


