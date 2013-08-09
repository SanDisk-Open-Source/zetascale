#ifndef ASYNC_PUTS_H
#define ASYNC_PUTS_H 1

/*
 * File:   $HeadURL: svn://s002.schoonerinfotech.net/schooner-trunk/trunk/sdf/protocol/action/async_puts.h $
 * Author: briano
 *
 * Created on July 28, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: async_puts.h 8439 2009-07-21 14:28:07Z briano $
 */

/**
 * Action node thread pool for processing asynchronous writes and
 * replication.
 */

#include "sdfmsg/sdf_msg.h"
#include "common/sdftypes.h"
#include "protocol/protocol_common.h"
#include "fastcc_new.h"

#define MAX_ASYNC_PUTS_THREADS   256

struct SDF_action_state;
struct flashDev;

PLAT_CLOSURE(async_puts_shutdown);

typedef enum {
    ASYNC_PUT = 1,
    ASYNC_WRITEBACK,
    ASYNC_FLUSH,
    ASYNC_BACKGROUND_FLUSH,
	ASYNC_COMMIT
} SDF_async_rqst_type_t;

typedef struct FDF_async_rqst {
    SDF_async_rqst_type_t rtype;
    SDF_action_state_t   *pas;
    SDF_boolean_t         skip_for_wrbk;
    SDF_boolean_t         do_replicate;
    SDF_boolean_t         inval_flag;
    SDF_tag_t             tag;
    SDF_action_init_t    *pai;
    SDF_cguid_t           ctnr;
    int                   n_ctnr;
    SDF_container_type_t  ctnr_type;
    SDF_context_t         ctxt;
    SDF_shardid_t         shard;
    SDFNewCacheBucket_t  *pbucket;
    SDFNewCache_t        *actiondir;
    struct shard         *pshard;
    struct objMetaData    flash_meta;
    char                 *pkey;
    SDF_simple_key_t     *pkey_simple;
    SDFNewCacheEntry_t   *entry;
    SDFNewCacheEntry_t   *pce;
    char                 *pdata;
    int                   flash_flags;
    fthMbox_t            *ack_mbx;
    sdf_fth_mbx_t        *req_mbx;
    fthMbox_t            *req_resp_mbx;
    fthMbox_t            *drain_complete_mbx;
    SDF_container_meta_t *pmeta;
    struct SDF_action_thrd_state *pts;
	uint64_t              rqst_id;
	uint64_t              trx_id;
	uint64_t              syndrome;
} FDF_async_rqst_t;

/**
 * @brief Asynchronous puts/replication thread pool initialization
 *
 * User fills in all fields; with the #paps field provided for symmetry
 * with the action and home protocol configuration.
 */
typedef struct SDF_async_puts_init {
    /** Protocol state  */
    struct SDF_async_puts_state *paps;

    /**
     * @brief  Number of threads to start
     */
    int nthreads;

    /** @brief my node number */
    uint32_t my_node;

    /** @brief Total node count */
    uint32_t nnodes;

    /** @brief Limit on number of flushes in progress. */
    uint32_t max_flushes_in_progress;

    /** @brief Limit on number of background flushes in progress. */
    uint32_t max_background_flushes_in_progress;

    /** @brief Time delay between background flush scans
     *  If the background flusher finds no dirty data in the cache, it
     *  waits this many msec before trying again. 
     */
    uint32_t background_flush_sleep_msec;

#ifdef MULTIPLE_FLASH_DEV_ENABLED
    struct flashDev **flash_dev;
#else
    struct flashDev *flash_dev;
#endif
    uint32_t flash_dev_count;
} SDF_async_puts_init_t;

/*
 * Asynchronous thread information.
 */
typedef struct {
	uint64_t rqst_id;
	uint64_t syndrome;
} async_thr_info_t;

/**
 * @brief Async puts thread pool state shared by all worker threads.
 *
 * This should be a singleton created by the agent code.
 */
typedef struct SDF_async_puts_state {
    /** @brief Configuration */
    struct SDF_async_puts_init config;

    /** @brief Actual inbound mailbox (one per thread in pool) */
    fthMbox_t *inbound_fth_mbx;

    /** @brief Lock for ensuring that drain barrier information can
     *         be loaded contiguously into the drain barrier mailbox. 
     */
    fthLock_t  drain_mbx_lock;

    /** @brief Mailbox for passing drain barrier information. */
    fthMbox_t  drain_fth_mbx;

    /** @brief Startup mailbox; used for a barrier to ensure all 
     *  threads have started and set up their state.
     */
    fthMbox_t  startup_mbx;

    /**
     * @brief Action state
     *
     * Required to initialize per-thread state for internal client
     * operations to translate container cguid to flash structures.
     */
    struct SDF_action_state *p_action_state;

    /** @brief Number of threads.  Included in ref count */
    int num_threads;

    /** @brief Limit on number of flushes in progress.  */
    uint32_t max_flushes_in_progress;

    /** @brief Number of flushes in progress.  */
    uint32_t flushes_in_progress;

    /** @brief Number of times #home_flash_shutdown called */
    int shutdown_count;

    /** @brief Closure applied on shutdown completion */
    async_puts_shutdown_t shutdown_closure;

	/* Asynchronous thread information and locks */
	uint64_t           q_r;
	uint64_t           q_w;
	uint64_t           q_size;
	pthread_cond_t     q_cond;
	pthread_mutex_t    q_lock;
	FDF_async_rqst_t **q_rqsts;
	async_thr_info_t  *thr_info;
} SDF_async_puts_state_t;

__BEGIN_DECLS

/**
 * @brief Initialize async puts state
 * @param paps <OUT> flash state
 * @param papi <IN> flash initialization parameters.
 * @param pai <IN> action initialization structure referencing
 * constructed
 */
struct SDF_async_puts_state *
async_puts_alloc(struct SDF_async_puts_init *papi, struct SDF_action_state *pas);

/**
 * @brief Shutdown async puts thread pool asynchronously
 *
 * @param paps <IN> async puts common state
 * @param shutdown_closure <IN> invoked when shutdown is complete
 */
void async_puts_shutdown(struct SDF_async_puts_state *paps,
                         async_puts_shutdown_t shutdown_closure);

int  async_start(struct SDF_async_puts_state *paps);
void async_commit(void *vpai, uint64_t trx_id);
void async_drain(SDF_async_puts_state_t *aps, uint64_t rqst_id);
void async_qpost(SDF_action_state_t *as, FDF_async_rqst_t *rqst, int wait);

__END_DECLS

#endif /* ndef ASYNC_PUTS_H */
