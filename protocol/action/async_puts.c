/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * Asynchonous writes.
 *
 * Copyright (c) 2009-2013, Schooner Information Technology, Inc.
 */

#include "flash/flash.h"
#include "protocol/protocol_alloc.h"
#include "protocol/action/action_thread.h"

#include "ssd/fifo/mcd_trx.h"
#include "sdftcp/trace.h"
#include "async_puts.h"

/*
 * For errors.
 */
#define LOG_CAT PLAT_LOG_CAT_SDF_PROT_FLASH

/*
 * Information about each asynchronous thread.
 *  aps             - parent state
 *  ctxt            - SDF context
 *  pats            - action thread state for internal client use
 *  pai             - internal client state
 *  nthread         - my thread number
 *  req_resp_fthmbx - fth mailbox for message responses
 *  req_mbx         - for sending requests to flash; response expected
 *  key_simple      - key buffer
 *  data            - data buffer
 */
typedef struct {
	SDF_async_puts_state_t       *aps;
	SDF_context_t                 ctxt;
	struct SDF_action_thrd_state *pats;
	struct SDF_action_init       *pai;
	uint32_t                      nthread;
	fthMbox_t                     req_resp_fthmbx;
	sdf_fth_mbx_t                 req_mbx;
	SDF_simple_key_t              key_simple;
	char                         *data;
} async_thr_t;

/*
 * Global references that for some reason are not defined in a header file.
 */
extern int  do_flush(ZS_async_rqst_t *pap);
extern int  do_writeback(ZS_async_rqst_t *pap);
extern int  do_put(ZS_async_rqst_t *pap, SDF_boolean_t unlock_slab);
extern void finish_write_in_flight(SDF_action_state_t *pas);

/*
 * Log an error.
 */
static void
error(char *op, int s, int64_t *cntr)
{
	uint64_t n = atomic_inc_get(*cntr);

	if ((n & (n-1)) != 0)
		return;

	zs_loge(70130, "%ld asynchronous %ss have failed; status %d", n, op, s);
}

/*
 * Allocate from the non cached object arena.
 */
static void *
alloc_nc(size_t size)
{
	return proto_plat_alloc_arena(size, NonCacheObjectArena);
}

/*
 * Allocate from the non cached object arena and clear the resultant memory.
 */
static void *
alloc_nc_clr(size_t size)
{
	void *ptr =  proto_plat_alloc_arena(size, NonCacheObjectArena);
	memset(ptr, 0, size);
	return ptr;
}

/*
 * Free an entry if it is not null.
 */
static void
plat_free_if(void *ptr)
{
	if (ptr)
		plat_free(ptr);
}

/*
 * Free the structure that is common to all asynchronous threads.
 */
static SDF_async_puts_state_t *
async_puts_free(SDF_async_puts_state_t *aps)
{
	plat_free_if(aps->q_rqsts);
	plat_free_if(aps->thr_info);
	plat_free(aps);
	return NULL;
}

/*
 * Allocate the structure that holds information common to all asynchronous
 * threads.
 */
SDF_async_puts_state_t *
async_puts_alloc(struct SDF_async_puts_init *papi,
                 struct SDF_action_state *pas) 
{
	if (papi->nthreads > MAX_ASYNC_PUTS_THREADS) {
		fatal("too many async threads: requested %d > %d allowed",
			papi->nthreads, MAX_ASYNC_PUTS_THREADS);
	}

	SDF_async_puts_state_t *aps = alloc_nc_clr(sizeof(*aps));
	if (!aps)
		return NULL;

	aps->thr_info = alloc_nc_clr(papi->nthreads * sizeof(async_thr_info_t));
	if (!aps->thr_info)
		return async_puts_free(aps);

	aps->q_size = papi->nthreads;
	aps->q_rqsts = alloc_nc_clr(aps->q_size * sizeof(ZS_async_rqst_t *));
	if (!aps->thr_info)
		return async_puts_free(aps);

	pthread_cond_init(&aps->q_cond, NULL);
	pthread_cond_init(&aps->q_drain, NULL);
	pthread_mutex_init(&aps->q_lock, NULL);

	aps->config           = *papi;
	aps->p_action_state   = pas;
	aps->num_threads      = 0;
	aps->drain_threads      = 0;
	aps->shutdown_count   = 0;
	aps->shutdown_closure = async_puts_shutdown_null;
	aps->max_flushes_in_progress = papi->max_flushes_in_progress;
	SDFNewCacheSetFlushTokens(
		pas->new_actiondir, papi->max_flushes_in_progress);
	SDFNewCacheSetBackgroundFlushTokens(
		pas->new_actiondir,
		papi->max_background_flushes_in_progress,
		papi->background_flush_sleep_msec);
	aps->flushes_in_progress = 0;

	fthMboxInit(&aps->startup_mbx);

	pas->async_puts_state = aps;
	return aps;
}

/*
 * Shurdown.
 */
 void async_puts_shutdown(SDF_async_puts_state_t *aps,
 						async_puts_shutdown_t shutdown_closure)
{
	zs_loge(70131, "async_puts_shutdown not implemented");
}

/*
 * Free an asychronous thread structure.
 */
static void
thr_free(async_thr_t *pts)
{
	if (!pts)
		return;

	plat_free_if(pts->pats);
	plat_free_if(pts->pai);
	plat_free_if(pts->data);
	plat_free(pts);
}

/*
 * Allocate an asychronous thread structure.
 */
static async_thr_t *
thr_alloc(SDF_async_puts_state_t *aps, uint32_t nthread)
{
	async_thr_t *pts = alloc_nc(sizeof(*pts));
	if (!pts)
		return NULL;
	
	memset(pts, 0, sizeof(*pts));
	pts->nthread = nthread;
	pts->aps     = aps;
	pts->pats    = alloc_nc(sizeof(*pts->pats));
	pts->pai     = alloc_nc(sizeof (*pts->pai));
	pts->data    = alloc_nc(pts->aps->p_action_state->max_obj_size);

	if (!pts->pats || !pts->pai || !pts->data) {
		thr_free(pts);
		return NULL;
	}

	pts->pai->pcs = aps->p_action_state;
	pts->pai->pts = pts->pats;
	fthMboxInit(&pts->req_resp_fthmbx);
	pts->req_mbx.actlvl          = SACK_RESP_ONLY_FTH;
	pts->req_mbx.release_on_send = 1;
	pts->req_mbx.abox            = NULL;
	pts->req_mbx.rbox            = &pts->req_resp_fthmbx;

	return pts;
}

/*
 * Determine if there are any clashes between the current request and any
 * others that might be in flight.  Return true if there is a clash.  This must
 * be called with aps->q_lock held.
 */
static int
clashes(SDF_async_puts_state_t *aps, int thr_id)
{
#if 0
	int i;
	async_thr_info_t *tinfo = &aps->thr_info[thr_id];
	uint64_t        rqst_id = tinfo->rqst_id;
	uint64_t       syndrome = tinfo->syndrome;

	if (!syndrome)
		return 0;

	for (i = 0; i < aps->num_threads; i++) {
		async_thr_info_t *tinfo = &aps->thr_info[i];
		uint64_t            syn = tinfo->syndrome;
		if (i != thr_id && syn && syn == syndrome && tinfo->rqst_id < rqst_id)
			return 1;
	}
#endif
	return 0;
}

/*
 * Get the next entry from the asynchronous queue.
 */
ZS_async_rqst_t *
async_qget(SDF_async_puts_state_t *aps, int thr_id)
{
	ZS_async_rqst_t *rqst;
	async_thr_info_t *tinfo = &aps->thr_info[thr_id];

	pthread_mutex_lock(&aps->q_lock);
	tinfo->rqst_id  = 0;
	tinfo->syndrome = 0;

	while (aps->q_r == aps->q_w)
		pthread_cond_wait(&aps->q_cond, &aps->q_lock);
	rqst = aps->q_rqsts[aps->q_r++ % aps->q_size];
	tinfo->rqst_id  = rqst->rqst_id;
	tinfo->syndrome = rqst->syndrome;
	pthread_cond_signal(&aps->q_cond);

	if (aps->drain_threads) {
		pthread_cond_broadcast(&aps->q_drain);
	}

	if (rqst->rtype == ASYNC_PUT || rqst->rtype == ASYNC_WRITEBACK)
		while (clashes(aps, thr_id))
			pthread_cond_wait(&aps->q_cond, &aps->q_lock);

	pthread_mutex_unlock(&aps->q_lock);
	return rqst;
}

/*
 * Set the syndrome.
 */
static void
set_syndrome(ZS_async_rqst_t *rqst)
{
	uint64_t syn = 0;
	int      req = rqst->rtype;

	if (req == ASYNC_PUT || req == ASYNC_WRITEBACK) {
		syn = hashck((unsigned char *) rqst->pkey_simple->key,
				rqst->pkey_simple->len, 0, 0);
	}

	rqst->syndrome = syn;
}

/*
 * Post to the asynchronous queue.
 */
void
async_qpost(SDF_action_state_t *as, ZS_async_rqst_t *rqst, int wait)
{
	SDF_async_puts_state_t *aps = as->async_puts_state;

	set_syndrome(rqst);
	pthread_mutex_lock(&aps->q_lock);
	while (aps->q_w == aps->q_r + aps->q_size)
		pthread_cond_wait(&aps->q_cond, &aps->q_lock);

	uint64_t w = aps->q_w++;
	rqst->rqst_id = w + 1;
	aps->q_rqsts[w % aps->q_size] = rqst;

	pthread_cond_signal(&aps->q_cond);
	pthread_mutex_unlock(&aps->q_lock);

	if (wait)
		(void) fthMboxWait(rqst->ack_mbx);
}

/*
 * Determine if all requests up to and including rqst_id have completed.  This
 * must be called with aps->q_lock held.
 */
static int
completed(SDF_async_puts_state_t *aps, uint64_t rqst_id)
{
	int i;

	if (aps->q_r < rqst_id)
		return 0;

	for (i = 0; i < aps->num_threads; i++) {
		uint64_t id = aps->thr_info[i].rqst_id;
		if (id && id <= rqst_id)
			return 0;
	}

	return 1;
}

/*
 * Wait until all requests up to and including rqst_id have completed.  If
 * rqst_id is 0, we wait until all currently outstanding requets have
 * completed.
 */
void
async_drain(SDF_async_puts_state_t *aps, uint64_t rqst_id)
{
	if (rqst_id == 0)
		rqst_id = aps->q_w;

	pthread_mutex_lock(&aps->q_lock);
	while (!completed(aps, rqst_id)) {
		aps->drain_threads++;
		pthread_cond_wait(&aps->q_drain, &aps->q_lock);
		aps->drain_threads--;
	}
	pthread_mutex_unlock(&aps->q_lock);
}

/*
 * Commit asynchronously.
 */
void
async_commit(void *vpai, uint64_t trx_id)
{
	SDF_action_init_t *pai = vpai;
	struct SDF_action_thrd_state *pts = pai->pts;

	ZS_async_rqst_t rqst ={
		.rtype   = ASYNC_COMMIT,
		.trx_id  = trx_id,
		.ack_mbx = &pts->async_put_ack_mbox
	};

	async_qpost(pai->pcs, &rqst, 1);
}

/*
 * Perform an asynchronous write or writeback.
 */
static void
async_do(async_thr_t *pts, ZS_async_rqst_t *rqstp)
{
	ZS_async_rqst_t   rqst = *rqstp;
	SDF_action_state_t *pas = pts->aps->p_action_state;
	int                 req = rqst.rtype;

	if (req == ASYNC_BACKGROUND_FLUSH)
		req = ASYNC_FLUSH;
	if (req == ASYNC_FLUSH && !rqst.pce)
		fatal("rqst.pce is null");

	pts->key_simple = *rqst.pkey_simple;
	if (rqst.pce) {
		// paged cache entry
		SDFNewCacheCopyOutofObject(rqst.pas->new_actiondir, pts->data,
									rqst.pce, rqst.pas->max_obj_size);
		rqst.pdata = pts->data;
	} else if (rqst.pdata) {
		// not a delete
		memcpy(pts->data, rqst.pdata, rqst.flash_meta.dataLen);
		rqst.pdata = pts->data;
	}
	fthMboxPost(rqst.ack_mbx, 0);

	rqst.ctxt        = pts->ctxt;
	rqst.pai         = pts->pats->pai;
	rqst.pkey_simple = &pts->key_simple;
	rqst.ack_mbx     = NULL;

	if (req == ASYNC_PUT) {
		rqst.req_mbx      = &pts->req_mbx;
		rqst.req_resp_mbx = &pts->req_resp_fthmbx;
		rqst.pts          = pts->pats;

		mcd_trx_attach(rqst.trx_id);
		int s = do_put(&rqst, SDF_TRUE);
		mcd_trx_detach();
		atomic_inc(pas->stats_new_per_sched[curSchedNum].
					ctnr_stats[rqst.n_ctnr].n_async_puts);
		if (s != FLASH_EOK) {
			error("write", s, &pas->stats_new_per_sched[curSchedNum].
						ctnr_stats[rqst.n_ctnr].n_async_put_fails);
		}
		finish_write_in_flight(rqst.pai->pcs);
	} else if (req == ASYNC_WRITEBACK) {
		int s = do_writeback(&rqst);
		atomic_inc(pas->stats_new_per_sched[curSchedNum].
					ctnr_stats[rqst.n_ctnr].n_async_wrbks);
		if (s != FLASH_EOK) {
			error("writeback", s, &pas->stats_new_per_sched[curSchedNum].
									ctnr_stats[rqst.n_ctnr].n_async_wrbk_fails);
		}
	} else if (req == ASYNC_FLUSH) {
		atomic_inc(pts->aps->flushes_in_progress);
		int s = do_flush(&rqst);
		atomic_inc(pas->stats_new_per_sched[curSchedNum].
					ctnr_stats[rqst.n_ctnr].n_async_flushes);
		if (s != FLASH_EOK) {
			error("flush", s, &pas->stats_new_per_sched[curSchedNum].
								ctnr_stats[rqst.n_ctnr].n_async_flush_fails);
		}
		atomic_dec(pts->aps->flushes_in_progress);
	} else
		fatal("bad asynchronous request: %d", req);
}

/*
 * Perform an asynchronous commit.
 */
static void
async_do_commit(async_thr_t *pts, ZS_async_rqst_t *rqstp)
{
	ZS_async_rqst_t rqst = *rqstp;
	fthMboxPost(rqst.ack_mbx, 0);
	async_drain(pts->aps, 0);
	mcd_trx_t s = mcd_trx_commit_id(pts->pai, rqst.trx_id);
	if (s != MCD_TRX_OKAY) {
		SDF_action_state_t *pas = pts->aps->p_action_state;
		error("commit", s, &pas->stats_new_per_sched[curSchedNum].
							ctnr_stats[rqst.n_ctnr].n_async_commit_fails);
	}
}

/*
 * Each asynchronous thread runs this.  If it receives a NULL request, it
 * terminates.
 */
static void
async_main(uint64_t arg)
{
	async_thr_t *pts = (async_thr_t *) arg;
	InitActionAgentPerThreadState(pts->pai->pcs, pts->pats, pts->pai);
	pts->pai->ctxt = ActionGetContext(pts->pats);
	fthMboxPost(&pts->aps->startup_mbx, (uint64_t) 1);

	for (;;) {
		ZS_async_rqst_t *rqst = async_qget(pts->aps, pts->nthread);
		if (!rqst)
			break;
		int req = rqst->rtype;

		if (req == ASYNC_PUT ||
			req == ASYNC_WRITEBACK ||
			req == ASYNC_FLUSH ||
			req == ASYNC_BACKGROUND_FLUSH)
		{
			async_do(pts, rqst);
		} else if (req == ASYNC_COMMIT)
			async_do_commit(pts, rqst);
		else
			fatal("bad asynchronous request: %d", req);
	}
	thr_free(pts);
}

/*
 * Start the asynchronous mechanism.  Return true on success and false on
 * error.
 */
int async_start(SDF_async_puts_state_t *aps)
{
	int i;
	async_thr_t *pts;

	if (aps->config.nthreads < 1)
		fatal("number of async put threads must be > 0");

	for (i = 0; i < aps->config.nthreads; i++) {
		pts = thr_alloc(aps, i);
		if (!pts)
			fatal("cannot allocate async thread");

		fthThread_t *fth = fthSpawn(&async_main, 0);
		if (!fth)
			fatal("failed to spawn async thread");
		fthResume(fth, (uint64_t) pts);
	}

	for (i = 0; i < aps->config.nthreads; i++)
		fthMboxWait(&aps->startup_mbx);

	// Scary to be using the last pts allocated
	int s = SDFStartBackgroundFlusher(pts->pats);
	if (s != SDF_SUCCESS)
		fatal("home flash %p failed to start ", aps);

	return 1;
}
