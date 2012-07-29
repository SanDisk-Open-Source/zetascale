/*
 * File:   action_internal_ctxt.h
 * Author: Brian O'Krafka
 *
 * Created on April 9, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: action_internal_ctxt.h 802 2008-03-29 00:44:48Z darpan $
 */

#include "common/sdftypes.h"
#include "sdfmsg/sdf_msg_types.h"
#include "common/sdftypes.h"
#include "agent/agent_helper.h"
#include "fth/fth.h"
#include "protocol/replication/sdf_vips.h"
#include "protocol/action/simple_replication.h"
#include "shared/container_meta.h"

#ifndef _ACTION_INTERNAL_CTXT_H
#define _ACTION_INTERNAL_CTXT_H

#ifdef __cplusplus
extern "C" {
#endif

#define SDF_MAX_CONTAINERS 129   // 128 plus 1 for cmc

struct SDF_action_state;

typedef struct SDF_action_init {
   struct SDF_action_state   *pcs;
   uint32_t             nthreads;
   uint32_t             nnode;
   uint32_t             nnodes;
   SDF_context_t        ctxt;
   void                *pts;
   void                *phs;
   struct ssdaio_ctxt  *paio_ctxt;

   /**
    * @brief Disable fast path between action and home node code 
    *
    * This is mostly for debugging replication in a single node 
    * environment.  Long term we might want to have a non-coherent 
    * replicated case where fthHomeFunction has a fast path to 
    * local replication.
    */
   int                  disable_fast_path;

    /*  mbox_idx stores the mailbox index (of mbox_shmem array in the queue) that
        this thread will wait on. */
   int                 mbox_idx;

   /* for action_new.c */

#ifdef MULTIPLE_FLASH_DEV_ENABLED
    struct flashDev        **flash_dev;
#else
    struct flashDev        *flash_dev;
#endif
    uint32_t                flash_dev_count;
} SDF_action_init_t;

typedef struct {
    uint64_t              appreq_counts[N_SDF_APP_REQS];
    uint64_t              msg_out_counts[N_SDF_PROTOCOL_MSGS];
    uint64_t              msg_in_counts[N_SDF_PROTOCOL_MSGS];
    uint64_t              sdf_status_counts[N_SDF_STATUS_STRINGS];
    uint64_t              flash_retcode_counts[FLASH_N_ERR_CODES];
    int64_t               n_only_in_cache;
    int64_t               n_total_in_cache;
    int64_t               bytes_only_in_cache;
    int64_t               bytes_total_in_cache;
    int64_t               all_bytes_ever_created;
    int64_t               all_objects_ever_created;
    int64_t               n_overwrites_s;
    int64_t               n_overwrites_m;
    int64_t               n_in_place_overwrites_s;
    int64_t               n_in_place_overwrites_m;
    int64_t               n_new_entry;
    int64_t               n_writebacks;
    int64_t               n_writethrus;
    int64_t               n_flushes;
} SDF_cache_ctnr_stats_t;

typedef struct SDF_action_stats_new {
    SDF_cache_ctnr_stats_t   ctnr_stats[SDF_MAX_CONTAINERS];
} SDF_action_stats_new_t;

typedef struct SDF_action_stats {
    uint64_t              appreq_counts[N_SDF_APP_REQS];
    uint64_t              msg_out_counts[N_SDF_PROTOCOL_MSGS];
    uint64_t              msg_in_counts[N_SDF_PROTOCOL_MSGS];
} SDF_action_stats_t;

typedef struct {
    int                   valid;
    int                   n;
    SDF_cguid_t           cguid;
    SDF_container_meta_t  meta;
    struct shard         *pshard;
    uint64_t              n_only_in_cache;
    uint64_t              n_total_in_cache;
    uint64_t              bytes_only_in_cache;
    uint64_t              bytes_total_in_cache;
    uint64_t              all_bytes_ever_created;
    uint64_t              all_objects_ever_created;
    uint64_t              all_objects_at_restart;
    uint32_t              flush_progress_percent;

    /**
     * @brief Key based locks for recovery/new operation interaction
     *
     * At 2010-03-05 only recovery get-by-cursor, recovery put, and
     * new delete operations use this.  With only full recovery 
     * implemented recovery puts can use FLASH_PUT_TEST_NONEXIST to avoid
     * overwriting newer puts.
     */
    struct replicator_key_lock_container *lock_container;
} SDF_cache_ctnr_metadata_t;

struct SDF_action_thrd_state;
struct SDFNewCache;
struct SDF_async_puts_state;

/*
 * This is the structure that sync container worker threads use to
 * pass data back and forth between to get cursors to work on
 */
struct cursor_data {
    SDF_context_t         ctxt;
    SDF_shardid_t         shard;
    vnode_t               dest_node;
    void                 *cursor;
    int                   cursor_len;
    SDF_cguid_t           cguid;
    fthMbox_t             mbox;
    int64_t               clock_skew;
    int                   rc;
    /* Lock container for current container */
    struct replicator_key_lock_container *lock_container;
};

#define DEFAULT_NUM_SYNC_CONTAINER_THREADS 32
#define DEFAULT_NUM_SYNC_CONTAINER_CURSORS 1000

typedef struct SDF_action_state {
    SDF_vnode_t              mynode;
    SDF_context_t            next_ctxt;   /* xxxzzz fix this */
    /*
     * Arrays of pointers to queue pairs are sized by nnode and indexed
     * by the destination node.
     */
    struct sdf_queue_pair  **q_pair_consistency;
    struct sdf_queue_pair  **q_pair_responses;
    uint32_t                 nthrds;
    fthLock_t                nthrds_lock;
    uint32_t                 nnodes;
    uint64_t                 nbuckets;
    uint64_t                 nslabs;
    uint64_t                 cachesize;
    fthMbox_t                mbox_request; /* for "messages" to self-node */
    fthMbox_t                mbox_response; /* for "messages" from self-node */
    uint64_t                 n_context;
    fthLock_t                context_lock;
    fthLock_t                flush_all_lock;
    fthLock_t                flush_ctnr_lock;
    fthLock_t                sync_ctnr_lock;
    fthLock_t                ctnr_preload_lock;
    fthLock_t                stats_lock;
    SDF_action_stats_t       stats;
    struct SDF_action_thrd_state  *threadstates;
    /** @brief Disable fast path between action and home code */
    int                      disable_fast_path;
    /* for action_new.c */
    SDF_boolean_t                     state_machine_mode;
    struct SDFNewCache               *new_actiondir;
    int                               n_containers;

    SDF_cache_ctnr_metadata_t        *ctnr_meta;
    #ifdef MULTIPLE_FLASH_DEV_ENABLED
	struct flashDev             **flash_dev;
    #else
	struct flashDev              *flash_dev;
    #endif
    uint32_t                          flash_dev_count;
    SDF_boolean_t                     trace_on;
    SDF_boolean_t                     new_allocator;
    SDF_action_stats_new_t            stats_new_per_sched[FTH_MAX_SCHEDS];
    SDF_action_stats_new_t            stats_new;
    SDF_action_stats_new_t            stats_per_ctnr;
    SDF_boolean_t                     enable_replication;
    fthLock_t                         container_serialization_lock;

    // for simple replication
    SDF_boolean_t                 simple_replication;
    qrep_state_t                  qrep_state;
    SDF_boolean_t                 failback;
    fthLock_t                     sync_remote_ctnr_lock;
    fthMbox_t                     git_mbx;
    fthMbox_t                    *gbc_mbx;
    fthMbox_t                    *cursor_mbx_todo;
    fthMbox_t                    *cursor_mbx_done;
    struct cursor_data           *cursor_datas;

    // for asynchronous puts
    struct SDF_async_puts_state  *async_puts_state;
    uint32_t                      max_obj_size;
    uint32_t                      max_key_size;

    // for prefix-based delete
    char                          prefix_delete_delimiter;

} SDF_action_state_t;


#ifdef	__cplusplus
}
#endif

#endif /* _ACTION_INTERNAL_CTXT_H */
