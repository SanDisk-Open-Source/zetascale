/*
 * File:   sdf/protocol/action/async_puts.c
 * Author: Brian O'Krafka
 *         (with heavy plagiarism of Drew Eckhardt's home_flash.c code)
 *
 * Created on July 28, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: async_puts.c 8487 2009-07-23 17:20:03Z briano $
 */

#include "platform/assert.h"
#include "platform/logging.h"

#include "fth/fthSpinLock.h"
#include "flash/flash.h"
#include "shared/init_sdf.h"
#include "protocol/protocol_common.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_alloc.h"
#include "protocol/action/action_thread.h"
#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_msg_action.h"
#include "shared/name_service.h"
#include "shared/shard_meta.h"
#include "shared/shard_compute.h"
#include "shared/internal_blk_obj_api.h"

#include "async_puts.h"

    /*  Uncomment this macro definition to compile in
     *  trace collection code.  It is only used if
     *  the logging level for sdf/prot=trace.
     */
#define INCLUDE_TRACE_CODE

#define LOG_CAT PLAT_LOG_CAT_SDF_PROT_FLASH

    /*  Print async write failure messages periodically,
     *  after this many errors occur:
     */
#define ASYNC_ERR_MSG_INTERVAL 1000000

//  for stats collection
#define incr(x) __sync_fetch_and_add(&(x), 1)

struct SDF_async_puts_thrd_state {
    /** @brief parent state */
    struct SDF_async_puts_state *paps;

    /** @brief SDF context. */
    SDF_context_t      ctxt;

    /**
     * @brief action thread state for internal client use
     *
     * Required for internal client operations to translate container
     * cguid to flash structures, etc.
     */
    struct SDF_action_thrd_state *pats;

    /** @brief Initializer for pats internal client state */
    struct SDF_action_init *pai;

    /** @brief My thread number */
    uint32_t       nthread;

    /** @brief fth mailbox used for message responses. */
    fthMbox_t        req_resp_fthmbx;

    /** @brief For sending requests to flash/replication (response expected) */
    sdf_fth_mbx_t    req_mbx;

    /** @brief Key buffer. */
    SDF_simple_key_t key_simple;

    /** @brief Data buffer. */
    char            *data;

    /** @brief fth mailbox used for drain operations. */
    fthMbox_t        drain_complete_mbx;
};

static struct SDF_async_puts_thrd_state *ap_alloc(struct SDF_async_puts_state *paps, uint32_t nthread);
static void ap_free(struct SDF_async_puts_thrd_state *pts);
static void ap_main(uint64_t arg);

struct SDF_async_puts_state *
async_puts_alloc(struct SDF_async_puts_init *papi,
                 struct SDF_action_state *pas) 
{
    int                          i;
    struct SDF_async_puts_state *paps;

    int failed;
    __attribute__((unused)) int status;

    paps = (struct SDF_async_puts_state *) proto_plat_alloc_arena(1*sizeof(*paps), NonCacheObjectArena);
    failed = !paps;
    if (paps)  {
        paps->config                  = *papi;
        paps->p_action_state          = pas;
        paps->num_threads             = 0;
        paps->shutdown_count          = 0;
        paps->max_flushes_in_progress = papi->max_flushes_in_progress;
        SDFNewCacheSetFlushTokens(pas->new_actiondir, papi->max_flushes_in_progress);
        SDFNewCacheSetBackgroundFlushTokens(pas->new_actiondir, papi->max_background_flushes_in_progress, papi->background_flush_sleep_msec);
	paps->flushes_in_progress     = 0;
        paps->shutdown_closure        = async_puts_shutdown_null;
	fthMboxInit(&(paps->startup_mbx));

	if (papi->nthreads > MAX_ASYNC_PUTS_THREADS) {
	    plat_log_msg(21134, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
			 "async puts thread pool %p allocated", paps);
	    plat_assert(0);
	}

        fthLockInit(&(paps->drain_mbx_lock));
	fthMboxInit(&(paps->drain_fth_mbx));
	paps->inbound_fth_mbx = (struct fthMbox *) proto_plat_alloc_arena(papi->nthreads*sizeof(struct fthMbox), NonCacheObjectArena);
	failed = !(paps->inbound_fth_mbx);
	if (!failed) {
	    for (i=0; i<papi->nthreads; i++) {
		fthMboxInit(&(paps->inbound_fth_mbx[i]));
	    }
	}
    }

    if (failed && paps) {
        async_puts_shutdown(paps, async_puts_shutdown_null);
	plat_free(paps);
        paps = NULL;
    }

    if (!failed) {
        plat_log_msg(21134, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "async puts thread pool %p allocated", paps);
	pas->async_puts_state = paps;
    } else {
        plat_log_msg(21135, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "async_puts_alloc failed ");
    }

    return (paps);
}

void async_puts_shutdown(struct SDF_async_puts_state *paps,
                    async_puts_shutdown_t shutdown_closure) 
{
    plat_log_msg(21136, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "shutdown async puts state %p NOT IMPLEMENTED!", paps);
}

int async_puts_start(struct SDF_async_puts_state *paps) 
{
    int                               ret;
    int                               i;
    struct SDF_async_puts_thrd_state *pts = NULL;
    fthThread_t                      *fth;
    SDF_status_t                      status;

    plat_log_msg(21137, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "async puts %p starting", paps);

    if (paps->config.nthreads < 1) {
	plat_log_msg(30620, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
		     "number of async put threads must be > 0");
	plat_assert(0);
    }

    ret = 0;
    for (i = 0; !ret && i < paps->config.nthreads; ++i) {
        pts = ap_alloc(paps, i);
        if (!pts) {
            ret = -1;
        }

        if (!ret) {
            fth = fthSpawn(&ap_main, 40960);
            if (!fth) {
                ret = -1;
            }
        }

        if (!ret) {
            fthResume(fth, (uint64_t)pts);
        } else {
            ap_free(pts);
        }
    }

    if (!ret) {
        for (i = 0; !ret && i < paps->config.nthreads; ++i) {
	    (void) fthMboxWait(&(paps->startup_mbx));
        }
	 
	/*  Start the background flusher thread.
	 *  This has to be done here because the async put
	 *  thread pool must be running.
	 */

        status = SDFStartBackgroundFlusher(pts->pats);
	if (status != SDF_SUCCESS) {
	    plat_log_msg(21139, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
			 "home flash %p failed to start ", paps);
	    ret = -1;
	}
    }

    if (!ret) {
        plat_log_msg(30617, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "async put thread pool %p started", paps);
    } else {
        plat_log_msg(30618, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "async put thread pool %p failed to start ", paps);
    }

    return (ret);
}

static struct SDF_async_puts_thrd_state *ap_alloc(struct SDF_async_puts_state *paps, uint32_t nthread) 
{
    struct SDF_async_puts_thrd_state *pts;
    int failed;

    pts = proto_plat_alloc_arena(sizeof (*pts), NonCacheObjectArena);
    failed = !pts;
    if (!failed) {
        pts->nthread = nthread;
        pts->paps = paps;
        pts->pats =
            (struct SDF_action_thrd_state *)proto_plat_alloc_arena(sizeof (*pts->pats), NonCacheObjectArena);
        pts->pai = (struct SDF_action_init *)proto_plat_alloc_arena(sizeof (*pts->pai), NonCacheObjectArena);
	pts->data = (char *) proto_plat_alloc_arena(pts->paps->p_action_state->max_obj_size, NonCacheObjectArena);

        failed = !(pts->pats && pts->pai && pts->data);
    }

    if (!failed) {
        pts->pai->pcs = paps->p_action_state;
        pts->pai->pts = pts->pats;
	fthMboxInit(&(pts->req_resp_fthmbx));
	fthMboxInit(&(pts->drain_complete_mbx));
	pts->req_mbx.actlvl          = SACK_RESP_ONLY_FTH;
	pts->req_mbx.release_on_send = 1;
	pts->req_mbx.abox            = NULL;
	pts->req_mbx.rbox            = &pts->req_resp_fthmbx;
    }

    if (failed && pts) {
        ap_free(pts);
        pts = NULL;
    }

    return (pts);
}

static void ap_free(struct SDF_async_puts_thrd_state *pts) 
{
    // SDF_async_puts_state_t *paps;

    if (pts) {
        // paps = pts->paps;

        if (pts->pats) {
            plat_free(pts->pats);
        }
        if (pts->pai) {
            plat_free(pts->pai);
        }
        if (pts->data) {
            plat_free(pts->data);
        }
        plat_free(pts);
    }
}

/**
 * @brief Async put thread main loop
 *
 * One async put thread is required for each simultaneous command.  Threads
 * are terminated by passing a NULL message in.
 *
 */
static void ap_main(uint64_t arg) 
{
    struct SDF_async_puts_thrd_state *pts = (struct SDF_async_puts_thrd_state *)arg;
    struct SDF_async_puts_state      *paps = pts->paps;
    SDF_async_put_request_t          *rqst_in;
    SDF_async_put_request_t           rqst;
    int                               ret = SDF_SUCCESS;
    fthMbox_t                        *ack_mbox;
    uint64_t                          x;
    SDF_action_state_t               *pas = paps->p_action_state;

    /* imported from action_new.c */
    extern int do_put(SDF_async_put_request_t *pap, SDF_boolean_t unlock_slab);
    extern int do_writeback(SDF_async_put_request_t *pap);
    extern int do_flush(SDF_async_put_request_t *pap);
    extern void finish_write_in_flight(SDF_action_state_t *pas);

    /*  Initialize per-thread state here
     *  (so aio_ctxt is set up correctly!)
     */
    InitActionAgentPerThreadState(pts->pai->pcs, pts->pats, pts->pai);
    pts->pai->ctxt = ActionGetContext(pts->pats);

    /* Let async_puts_start()  know that I am initialized. */
    fthMboxPost(&(paps->startup_mbx), (uint64_t) 1);

    while (SDF_TRUE) {
	// rqst_in = (SDF_async_put_request_t *) fthMboxWait(&paps->inbound_fth_mbx[pts->nthread]);
	rqst_in = (SDF_async_put_request_t *) fthMboxWait(&paps->inbound_fth_mbx[0]);
	if (rqst_in == NULL) {
	    break;
	}

	switch (rqst_in->rtype) {
	    
	    case ASYNC_DRAIN:

	        /*  Let the coordinator fthread know that this async put
		 *  thread has drained.
		 */

		/*  To make this reentrant, we use a second mailbox in which
		 *  to stash the mailbox to which the async_put_threads must
		 *  respond for the drain barrier.  We use a lock here to ensure
		 *  that all "config.nthreads" copies of the barrier mailbox
		 *  are contiguous.  We use this second mailbox so that normal
		 *  async_put operations won't have the overhead of an
		 *  additional lock operation.
		 *
		 *  Without this 2nd mailbox, concurrent drain operations would
		 *  interleave their requests and the system would deadlock.
		 *  This is because some of the barrier mbox posts would go to
		 *  to one drain thread, and some would go to another, and
		 *  neither would get enough posts to complete the barrier 
		 *  operation.
		 */
		
		ack_mbox = (fthMbox_t *) fthMboxWait(&paps->drain_fth_mbx);
		fthMboxPost(ack_mbox, (int64_t) &(pts->drain_complete_mbx));

		/*  Wait until all other threads have drained.
		 */

		(void) fthMboxWait(&pts->drain_complete_mbx);
		(void) incr(pas->stats_new_per_sched[curSchedNum].ctnr_stats[VDC_CGUID].n_async_drains);
	        break;

            case ASYNC_PUT:
		// Make a copy of the request structure.
		rqst = *rqst_in;

		/*  Copy over the data and key and handshake with the caller
		 *  that the copy has completed.
		 */

		pts->key_simple = *(rqst_in->pkey_simple);
		if (rqst_in->pce != NULL) {
		    // data source is a paged cached entry
		    SDFNewCacheCopyOutofObject(rqst_in->pas->new_actiondir, pts->data, rqst_in->pce, rqst_in->pas->max_obj_size);
		    rqst.pdata = pts->data;
		} else if (rqst_in->pdata == NULL) {
		    /* this is a delete operation */
		    rqst.pdata = NULL;
		} else {
		    memcpy(pts->data, rqst_in->pdata, rqst_in->flash_meta.dataLen);
		    rqst.pdata = pts->data;
		}

		/*  Let the memcached thread know the copying is done and it
		 *  can continue.
		 */
		fthMboxPost(rqst.ack_mbx, 0);

                /*  Give the requesting thread a chance to continue. */
		fthYield(0);

                /*  Do the put (and replication if enabled). */
		rqst.ctxt         = pts->ctxt;
		rqst.pai          = pts->pats->pai;
		rqst.pkey_simple  = &(pts->key_simple);
		rqst.ack_mbx      = NULL;
		rqst.req_mbx      = &(pts->req_mbx);
		rqst.req_resp_mbx = &(pts->req_resp_fthmbx);
		rqst.pts          = pts->pats;
		ret = do_put(&rqst, SDF_TRUE /* unlock slab */);
		(void) incr(pas->stats_new_per_sched[curSchedNum].ctnr_stats[rqst.n_ctnr].n_async_puts);
		if (ret != FLASH_EOK) {
		    x = incr(pas->stats_new_per_sched[curSchedNum].ctnr_stats[rqst.n_ctnr].n_async_put_fails);
		    if ((x % ASYNC_ERR_MSG_INTERVAL) == 1) {
			plat_log_msg(160066, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
			             "%ld asynchronous writes have failed!  This message is displayed only every %d failures.", x, ASYNC_ERR_MSG_INTERVAL);
		    }
		}
		finish_write_in_flight(rqst.pai->pcs);
	        break;

            case ASYNC_WRITEBACK:
		// Make a copy of the request structure.
		rqst = *rqst_in;

		/*  Copy over the data and key and handshake with the caller
		 *  that the copy has completed.
		 */

		pts->key_simple = *(rqst_in->pkey_simple);
		if (rqst_in->pce != NULL) {
		    // data source is a paged cached entry
		    SDFNewCacheCopyOutofObject(rqst_in->pas->new_actiondir, pts->data, rqst_in->pce, rqst_in->pas->max_obj_size);
		    rqst.pdata = pts->data;
		} else if (rqst_in->pdata == NULL) {
		    /* this is a delete operation */
		    rqst.pdata = NULL;
		} else {
		    memcpy(pts->data, rqst_in->pdata, rqst_in->flash_meta.dataLen);
		    rqst.pdata = pts->data;
		}

		/*  Let the memcached thread know the copying is done and it
		 *  can continue.
		 */
		fthMboxPost(rqst.ack_mbx, 0);

                /*  Give the requesting thread a chance to continue. */
		fthYield(0);

                /*  Do the writeback */
		rqst.ctxt         = pts->ctxt;
		rqst.pai          = pts->pats->pai;
		rqst.pkey_simple  = &(pts->key_simple);
		rqst.ack_mbx      = NULL;
		ret = do_writeback(&rqst);
		(void) incr(pas->stats_new_per_sched[curSchedNum].ctnr_stats[rqst.n_ctnr].n_async_wrbks);
		if (ret != FLASH_EOK) {
		    x = incr(pas->stats_new_per_sched[curSchedNum].ctnr_stats[rqst.n_ctnr].n_async_wrbk_fails);
		    if ((x % ASYNC_ERR_MSG_INTERVAL) == 1) {
			plat_log_msg(160067, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
			             "%ld asynchronous writebacks have failed!  This message is displayed only every %d failures.", x, ASYNC_ERR_MSG_INTERVAL);
		    }
		}
	        break;

            case ASYNC_FLUSH:
            case ASYNC_BACKGROUND_FLUSH:

		// Make a copy of the request structure.
		rqst = *rqst_in;

		/*  Copy over the data and key and handshake with the caller
		 *  that the copy has completed.
		 */

		pts->key_simple = *(rqst_in->pkey_simple);
		plat_assert(rqst_in->pce);

		// data source is a paged cached entry
		SDFNewCacheCopyOutofObject(rqst_in->pas->new_actiondir, pts->data, rqst_in->pce, rqst_in->pas->max_obj_size);
		rqst.pdata = pts->data;

		/*  Let the memcached thread know the copying is done and it
		 *  can continue.
		 */
		fthMboxPost(rqst.ack_mbx, 0);

                /*  Do the flush */
		rqst.ctxt         = pts->ctxt;
		rqst.pai          = pts->pats->pai;
		rqst.pkey_simple  = &(pts->key_simple);
		rqst.ack_mbx      = NULL;

		// for flow control
                (void) __sync_fetch_and_add(&paps->flushes_in_progress, 1);
		ret = do_flush(&rqst);
		(void) incr(pas->stats_new_per_sched[curSchedNum].ctnr_stats[rqst.n_ctnr].n_async_flushes);
		if (ret != FLASH_EOK) {
		    x = incr(pas->stats_new_per_sched[curSchedNum].ctnr_stats[rqst.n_ctnr].n_async_flush_fails);
		    if ((x % ASYNC_ERR_MSG_INTERVAL) == 1) {
			plat_log_msg(160068, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
			             "%ld asynchronous flushes have failed!  This message is displayed only every %d failures.", x, ASYNC_ERR_MSG_INTERVAL);
		    }
		}
		// for flow control
                __sync_fetch_and_add(&paps->flushes_in_progress, -1);

	        break;

	    default:
		plat_log_msg(21140, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
			     "Invalid asynchronous put service request type (%d)", rqst_in->rtype);
		plat_abort();
		break;
	}
    }

    plat_log_msg(21141, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "async puts thread stopping pts %p", pts);

    ap_free(pts);
}

