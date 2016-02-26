/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   action_new.h
 * Author: Brian O'Krafka
 *
 * Created on March 3, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: action_new.h 802 2008-03-29 00:44:48Z darpan $
 */

#ifndef _ACTION_NEW_H
#define _ACTION_NEW_H

#include "sdfmsg/sdf_fth_mbx.h"
#include "protocol/action/action_thread.h"

#ifdef __cplusplus
extern "C" {
#endif

#define N_ALLOC_DIR_ENTRIES   1024

typedef enum {CS_I=0, CS_S, CS_M, CS_B, N_CACHE_STATES} SDF_cache_state_t;

#ifndef _ACTION_NEW_C
    extern char *CacheStateName[];
#else
    char *CacheStateName[] = {
	"I",
	"S",
	"M",
	"B",
    };
#endif

struct SDF_trans_state;

typedef void (*SDF_action_fn_t)(struct SDF_trans_state *ptrans);

typedef struct {
    SDF_action_fn_t         action_fn;
    SDF_cache_state_t       new_state;
} PROT_FN_STATE;

typedef struct prot_table {
    SDF_app_request_type_t  reqtype;
    PROT_FN_STATE           fn_state[N_CACHE_STATES];
} PROT_TABLE;


    /*  This structure holds all of the state required to
     *  process an SDF request from start to finish.
     */
typedef struct SDF_trans_state {

    /* this stuff stays the same when processing castouts 
     */
    SDF_action_state_t        *pas;
    SDF_action_thrd_state_t   *pts;
    SDF_action_init_t         *pai;
    uint32_t                   n_locked;
    SDF_cache_ctnr_metadata_t *meta;
    struct objMetaData         metaData;
    SDF_appreq_t              *par;
    SDF_appreq_t               co_appreq;
    struct SDFNewCacheEntry   *entry;
    SDF_tag_t                  tag;
    int                        flash_retcode;
    char                      *pflash_key;
    char                      *pflash_data;
    SDF_boolean_t              bypass;
    SDF_boolean_t              expired_or_flushed;
    SDF_boolean_t              inval_object;
    struct SDFNewCacheBucket  *pbucket;
    SDF_time_t                 old_exptime;
    SDF_time_t                 old_createtime;
    PROT_TABLE                *pte;
    SDF_cache_state_t          old_state;
    SDF_boolean_t              new_entry;
 
    struct SDF_trans_state    *next; /* for free list of transState structs */

} SDF_trans_state_t;

/**
 * @brief Initialize pieces of both Home and Action node side of action 
 * node protocol.
 */

extern void SDFFreeAppBuffer(SDF_action_init_t *pai, void *pbuf);
extern void ActionProtocolAgentResume(struct SDF_trans_state *ptrans);
extern void flush_all_remote_request(SDF_action_init_t * pai, SDF_appreq_t * par);
extern SDF_status_t flush_inval_remote_request(SDF_action_init_t * pai, SDF_appreq_t * par);
extern void drain_store_pipe_remote_request(SDF_action_init_t *pai);
extern SDF_status_t prefix_delete_remote_request(SDF_action_init_t * pai, SDF_appreq_t * par, char *inval_prefix, uint32_t len_prefix);
extern void sdf_dump_msg_error(SDF_status_t error, struct sdf_msg * msg);
extern int protocol_message_report_version( char ** buf, int * buf_len );
extern SDF_status_t flush_all(struct SDF_trans_state *ptrans);
extern struct SDF_trans_state *get_trans_state(SDF_action_thrd_state_t *pts);
extern SDF_cache_ctnr_metadata_t *get_container_metadata(SDF_action_init_t *pai, SDF_cguid_t cguid);
extern SDF_container_meta_t *sdf_get_preloaded_ctnr_meta(SDF_action_state_t *pas, SDF_cguid_t cguid);
extern void destroy_per_thread_state(SDF_action_thrd_state_t *pts);

int
cache_get_by_mhash(SDF_action_init_t *pai, struct shard *shard, baddr_t baddr,
                   uint64_t hashbkt, hashsyn_t hashsyn, char **key,
                   uint64_t *key_len, char **data, uint64_t *data_len);

int
cache_inval_by_mhash(SDF_action_init_t *pai, struct shard *shard,
		     baddr_t baddr, uint64_t hashbkt, hashsyn_t hashsyn);

void
cache_inval_by_cntr(SDF_action_init_t *pai, struct shard *shard,
		    SDF_cguid_t cguid);

#ifdef	__cplusplus
}
#endif

#endif /* _ACTION_NEW_H */
