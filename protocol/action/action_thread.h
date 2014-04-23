/*
 * File:   action_thread.h
 * Author: Brian O'Krafka
 *
 * Created on April 9, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: action_thread.h 802 2008-03-29 00:44:48Z darpan $
 */

#ifndef _ACTION_THREAD_H
#define _ACTION_THREAD_H

#include "sdfmsg/sdf_fth_mbx.h"

#include "protocol/action/tlmap.h"
#include "protocol/home/tlmap2.h"
#include "shared/container_meta.h"
#include "protocol/action/action_internal_ctxt.h"
#include "protocol/action/async_puts.h"

#ifdef __cplusplus
extern "C" {
#endif

   /*  This is the minimum amount of time that the background
    *  flush thread sleeps if it finds no dirty data in the cache.
    */
#define MIN_BACKGROUND_FLUSH_SLEEP_MSEC  100

#define N_ALLOC_DIR_ENTRIES   1024
#define N_ALLOC_DIR_LINKS     1024
#define N_ALLOC_WAIT_LINKS    1024
#define SHARDMAP_N_BUCKETS    100

#define THRDNUM_BITS   8
#define TAGNUM_BITS   16

#define SDF_MSGBUF_SIZE  1024

struct SDF_trans_state;
struct SDFNewCache;

#define SDF_MAX_OBJ_SIZE   (8 * 1024*1024 - 72)

typedef struct SDF_home_flash_entry {
    struct shard                 *pshard;
    SDF_boolean_t                 stopflag;
    struct SDF_home_flash_entry  *next;
} SDF_home_flash_entry_t;

typedef struct SDF_action_thrd_state {

    uint32_t                thrdnum;
    SDF_vnode_t             mynode;
    uint32_t                nnodes;
    char                    smsg[SDF_MSGBUF_SIZE];
    SDF_action_state_t     *phs;
    fthMbox_t               ackmbx;
    fthMbox_t               respmbx;
    sdf_fth_mbx_t           fthmbx;
    string_t               *pflash_data;
    uint32_t                curtag;
    uint32_t                n_trans_in_flight;
    SDF_tag_t               max_tag;
    SDF_action_stats_t      stats;
    struct SDF_action_thrd_state *next;

    /* for action_new.c */
    struct SDF_trans_state   *free_trans_states;
    struct SDFNewCache       *new_actiondir;
    struct SDF_appBufState   *pappbufstate;
    SDF_action_init_t         ai_struct;
    SDF_action_init_t        *pai;
    int                       total_trans_state_structs;
    int                       used_trans_state_structs;
    int                       n_underway;
    char                     *objbuf;
    char                     *pobj;
    uint64_t                  obj_size;
    SDF_home_flash_entry_t   *free_shard_map_entries;
    SDF_home_flash_entry_t   *free_shard_map_entries_alloc_ptr;

    /** @brief Fth mbox for req_fthmbx */
    fthMbox_t                 req_resp_fthmbx;
    /** @brief For sending requests to flash/replication (response expected) */
    sdf_fth_mbx_t             req_mbx;

    service_t                 replication_server_service;
    service_t                 flash_server_service;
    service_t                 client_service;

    SDFTLMap2_t               shardmap;
    fthLock_t                 shardmap_lock;

    fthWaitEl_t              *container_serialization_wait;

    /* for async puts */
    fthMbox_t                 async_put_ack_mbox;

    /* for draining the store pipe */
    FDF_async_rqst_t          drain_request;

    /* to check for memory leaks */
    int64_t                   nflash_bufs;
    int64_t                   nresp_msgs;

    /*
     * Flag that indicates this thread context is in use.
     */
    bool ctxt_in_use;

} SDF_action_thrd_state_t;

extern SDF_context_t ActionGetContext(SDF_action_thrd_state_t *pts);
extern void ActionProtocolAgentNew(SDF_action_init_t *pai, SDF_appreq_t *par);

/**
 * @brief Initialize pieces of both Home and Action node side of action 
 * node protocol.
 */
extern SDF_status_t SDFPreloadContainerMetaData(SDF_action_init_t *pai, SDF_cguid_t cguid);
extern SDF_status_t SDFUnPreloadContainerMetaData(SDF_action_init_t *pai, SDF_cguid_t cguid);
extern void InitActionProtocolCommonState(SDF_action_state_t *pcs, SDF_action_init_t *pai);
extern void ShutdownActionProtocol(SDF_action_state_t *pas);
extern void InitActionAgentPerThreadState(SDF_action_state_t *pcs, SDF_action_thrd_state_t *pts, SDF_action_init_t *pai);
extern SDF_status_t SDFGetCacheStat(SDF_action_init_t *pai, SDF_CONTAINER container, int stat_name, uint64_t *pstat);
extern void SDFClusterStatus(SDF_action_init_t *pai, uint32_t *mynode_id, uint32_t *cluster_size);

extern SDF_status_t SDFActionCreateContainer(SDF_action_init_t *pai, SDF_container_meta_t  *pmeta);
extern SDF_status_t SDFActionOpenContainer(SDF_action_init_t *pai, SDF_cguid_t cguid);
extern SDF_status_t SDFActionStartContainer(SDF_action_init_t *pai, SDF_container_meta_t  *pmeta);
extern SDF_status_t SDFActionStopContainer(SDF_action_init_t *pai, SDF_container_meta_t *pmeta);
extern SDF_status_t SDFActionDeleteContainer(SDF_action_init_t *pai, SDF_container_meta_t *pmeta);
extern SDF_status_t SDFActionChangeContainerWritebackMode(SDF_action_init_t *pai, SDF_cguid_t cguid, SDF_boolean_t enable_writeback);
extern SDF_container_meta_t *sdf_get_preloaded_ctnr_meta(SDF_action_state_t *pas, SDF_cguid_t cguid);
extern SDF_status_t SDFSetAutoflush(SDF_action_init_t *pai, uint32_t percent, uint32_t sleep_msec);
extern SDF_status_t SDFSetFlushThrottle(SDF_action_init_t *pai, uint32_t percent);
extern SDF_status_t SDFSetModThresh(SDF_action_init_t *pai, uint32_t percent);
extern SDF_status_t SDFSelfTest(SDF_action_init_t *pai, SDF_cguid_t cguid, char *args);
extern SDF_status_t SDFStartBackgroundFlusher(SDF_action_thrd_state_t *pts);

#ifdef	__cplusplus
}
#endif

#endif /* _ACTION_THREAD_H */
