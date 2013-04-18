/*
 * File:   fastcc_new.h
 * Author: Brian O'Krafka
 *
 * Created on March 2, 2009
 *
 * New version of fastcc with shmem stuff removed.
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fastcc_new.h 308 2008-02-20 22:34:58Z tomr $
 */

#ifndef _FASTCC_NEW_H
#define _FASTCC_NEW_H

#include "shared/sdf_sm_msg.h"
#include "protocol/protocol_common.h"
#include "protocol/action/recovery.h"

#ifdef __cplusplus
extern "C" {
#endif

    /*  Define this to compile in code to support dynamic
     *  self-check code into the SDF cache.
     */
#define CHECK_HEAP

    /*  This variable, if non-zero, enables detailed
     *  self-checking.  It should ONLY be used in
     *  unit testing!
     */
extern int SDFSelfTestCheckHeap;

#define MAX_PRINT_LEN      1024

#define CE_OBJ_CTNR       (1<<0)
#define CE_BLK_CTNR       (1<<1)
#define CE_ENUM           (1<<2)
#define CE_ENUM_RECOVERY  (1<<3)

/* number of posted inval structs to add to the free pool at a time */
#define N_ALLOC_REQUEST_STRUCTS   1

/**************************************************************************/

  /*  Lock macros */

#define CacheLockType       fthLock_t
#define CacheWaitType       fthWaitEl_t *

#define CacheInitLock(x)    fthLockInit(&(x))
#define CacheUnlock(x, w)   fthUnlock(w)
#define CacheLock(x, w)     (w = fthLock(&(x), 1, NULL))
#define CacheLockTry(x, w)  (w = fthTryLock(&(x), 1, NULL))

/**************************************************************************/

typedef struct SDFNewCacheEntry {
    uint64_t                  obj_size:32;
    uint64_t                  key_len:8;
    uint64_t                  flags:8;
    uint64_t                  state:8;
    uint64_t                  reserved:8;
    SDF_time_t                createtime;
    SDF_time_t                exptime;
    SDF_cguid_t               cguid;
    hashsyn_t                 syndrome;
    baddr_t                   blockaddr;
    struct SDFNewCacheBucket *pbucket;
    struct SDFNewCacheEntry  *next;
    struct SDFNewCacheEntry  *lru_next;
    struct SDFNewCacheEntry  *lru_prev;
    struct SDFNewCacheEntry  *modlru_next;
    struct SDFNewCacheEntry  *modlru_prev;
} SDFNewCacheEntry_t;

struct SDFNewCache;

typedef struct SDFNewCacheRequest {
    SDF_cguid_t                 cguid;
    SDF_simple_key_t            key;
    SDF_app_request_type_t      reqtype;
    SDF_container_type_t        ctype;
    SDF_protocol_msg_t         *pm;
    struct SDFNewCacheRequest  *next;
} SDFNewCacheRequest_t;

typedef struct SDFNewCacheSlab {
    SDFNewCacheEntry_t    *tmp_entry;
    struct SDFNewCache    *pc;
    CacheLockType          lock;
    CacheWaitType          lock_wait;
    SDFNewCacheEntry_t    *lru_head;
    SDFNewCacheEntry_t    *lru_tail;
    SDFNewCacheEntry_t    *modlru_head;
    SDFNewCacheEntry_t    *modlru_tail;
    uint64_t               slabsize;
    void                  *free_pages;
    uint64_t               nobjects;
    uint64_t               cursize;
    uint64_t               cursize_w_keys;
    uint64_t               n_mod;
    uint64_t               modsize_w_keys;
    uint64_t               n_mod_flushes;
    uint64_t               n_mod_background_flushes;
    uint64_t               n_mod_recovery_enums;
    int32_t                n_lockers;

    fthMbox_t              request_queue;
    SDFNewCacheRequest_t  *free_request_structs;
    CacheLockType          free_request_struct_lock;
} SDFNewCacheSlab_t;

typedef struct SDFNewCacheBucket {
    CacheLockType             lock;
    CacheWaitType             lock_wait;
    SDFNewCacheEntry_t       *entry;
    SDFNewCacheSlab_t        *slab;
} SDFNewCacheBucket_t;

typedef struct {
    uint64_t    n_bucket;
} SDF_cache_enum_state_t;

typedef struct SDFNewCache {
    uint64_t              size_limit;
    uint64_t              nbuckets;
    int                   nhashbits;
    SDFNewCacheBucket_t  *buckets;
    uint64_t              nslabs;
    uint64_t              buckets_per_slab;
    SDFNewCacheSlab_t    *slabs;
    uint64_t              slabsize;
    uint64_t               pages_per_slab;
    uint64_t            (*hash_fn)(void *hash_arg, SDF_cguid_t cguid,
                                   char *key, uint64_t key_len,
                                   uint64_t num_bkts, hashsyn_t *hashsynp);
    void                 *hash_arg;
    void                (*init_state_fn)(SDFNewCacheEntry_t *pce, SDF_time_t curtime);
    int                 (*print_fn)(SDFNewCacheEntry_t *pce, char *sout, int max_len);
    void                (*wrbk_fn)(SDFNewCacheEntry_t *pce, void *wrbk_arg);
    void                (*flush_fn)(SDFNewCacheEntry_t *pce, void *flush_arg, SDF_boolean_t background_flush, SDF_boolean_t do_inval);
    CacheLockType         enum_lock;
    CacheWaitType         enum_lock_wait;
    CacheLockType         enum_flush_lock;
    CacheWaitType         enum_flush_lock_wait;
    uint64_t              enum_bucket;
    SDF_cguid_t           enum_cguid;
    struct SDF_bufpool   *pbufpool;
    uint64_t              mem_used;
    uint64_t              max_object_size;
    uint32_t              max_key_size;
    uint64_t              max_obj_entry_size;
    void                 *pmem_start;
    void                 *pmem_used;
    SDF_boolean_t         lru_flag;
    uint32_t              page_size;
    uint32_t              page_data_size;
    uint32_t              mod_state;
    uint32_t              shared_state;
    uint32_t              max_flushes_per_mod_check;
    double                f_modified;
    uint64_t              modsize_per_slab;
    uint32_t              n_pending_requests;
    SDF_cache_enum_state_t  recovery_enum_state;
    uint32_t              n_flush_tokens;
    fthMbox_t             flush_token_pool;
    uint32_t              n_background_flush_tokens;
    fthMbox_t             background_flush_token_pool;
    uint32_t              background_flush_sleep_msec;
    uint32_t              background_flush_progress;
    uint64_t              n_background_flushes;
    void                 *background_setup_arg;
    SDF_status_t         (*background_setup_fn)(void *setup_arg, void **pflush_arg);
    void                 *background_flush_arg;
} SDFNewCache_t;

typedef struct SDFNewCacheStats {
    uint64_t     num_objects;
    uint64_t     cursize;
    uint64_t     cursize_w_keys;
    uint64_t     n_only_in_cache;
    uint64_t     n_mod;
    uint64_t     modsize_w_keys;
    uint64_t     n_mod_flushes;
    uint64_t     n_mod_background_flushes;
    uint64_t     n_mod_recovery_enums;
    uint32_t     n_pending_requests;
    uint32_t     background_flush_progress;
    uint64_t     n_background_flushes;
    uint32_t     n_flush_tokens;
    uint32_t     n_background_flush_tokens;
    uint32_t     background_flush_sleep_msec;
    uint32_t     mod_percent;
} SDFNewCacheStats_t;

struct shard;

extern void SDFNewCacheGetStats(SDFNewCache_t *pc, SDFNewCacheStats_t *ps);
void SDFNewCacheInit(SDFNewCache_t *pc, uint64_t nbuckets, uint64_t nslabs_in,
     uint64_t size, uint32_t max_key_size, uint64_t max_object_size,
     uint64_t      (*hash_fn)(void *hash_arg, SDF_cguid_t cguid, char *key, uint64_t key_len, uint64_t num_bkts, hashsyn_t *hashsynp),
     void           *hash_arg, 
     void          (*init_state_fn)(SDFNewCacheEntry_t *pce, SDF_time_t curtime),
     int           (*print_fn)(SDFNewCacheEntry_t *pce, char *sout, int max_len),
     void          (*wrbk_fn)(SDFNewCacheEntry_t *pce, void *wrbk_arg),
     void          (*flush_fn)(SDFNewCacheEntry_t *pce, void *flush_arg, SDF_boolean_t background_flush, SDF_boolean_t do_inval),
     uint32_t      mod_state, uint32_t shared_state,
     uint32_t      max_flushes_per_mod_check,
     double        f_modified);
extern void SDFNewCacheDestroy(SDFNewCache_t *pc);
extern SDFNewCacheEntry_t *SDFNewCacheGetCreate(SDFNewCache_t *pc, SDF_cguid_t cguid, 
     SDF_simple_key_t *pkey, SDF_container_type_t ctype, SDF_time_t curtime, 
     SDF_boolean_t lock_slab, SDFNewCacheBucket_t **ppbucket,
     SDF_boolean_t try_lock, SDF_boolean_t *pnewflag, void *wrbk_arg);
extern uint64_t SDFNewCacheRemove(SDFNewCache_t *pc, SDFNewCacheEntry_t *pce, SDF_boolean_t wrbk_flag, void *wrbk_arg);
extern void SDFNewUnlockBucket(SDFNewCache_t *pc, SDFNewCacheBucket_t *pbucket);
extern void SDFNewCacheStartEnumeration(SDFNewCache_t *pc, SDF_cguid_t cguid);
extern void SDFNewCacheEndEnumeration(SDFNewCache_t *pc);
extern SDF_boolean_t SDFNewCacheNextEnumeration(SDFNewCache_t *pc, 
     SDF_cguid_t *pcguid, SDF_simple_key_t *pkey, SDF_container_type_t *pctype);
extern void SDFNewCachePrintAll(FILE *f, SDFNewCache_t *pc);
extern void SDFNewCacheStats(SDFNewCache_t *pc, char *str, int size);
extern SDFNewCacheEntry_t *SDFNewCacheCreateCacheObject(SDFNewCache_t *pc, SDFNewCacheEntry_t *pce, size_t size, void *wrbk_arg);
extern SDFNewCacheEntry_t *SDFNewCacheOverwriteCacheObject(SDFNewCache_t *pc, SDFNewCacheEntry_t *pce, uint32_t size, void *wrbk_arg);
extern void SDFNewCacheCopyOutofObject(SDFNewCache_t *pc, void *pto_in, SDFNewCacheEntry_t *pce, uint64_t size);
extern void SDFNewCacheCopyIntoObject(SDFNewCache_t *pc, void *pfrom_in, SDFNewCacheEntry_t *pce, uint64_t buf_size);
extern char *SDFNewCacheCopyKeyOutofObject(SDFNewCache_t *pc, char *pkey, SDFNewCacheEntry_t *pce);
extern void SDFNewCacheCopyKeyIntoObject(SDFNewCache_t *pc, SDF_simple_key_t *pkey_simple, SDFNewCacheEntry_t *pce);
extern void SDFNewCachePostRequest(SDFNewCache_t *pc, 
              SDF_app_request_type_t reqtype, SDF_cguid_t cguid, 
              SDF_simple_key_t *pkey, SDF_container_type_t ctype,
	      SDF_protocol_msg_t *pm);
extern void SDFNewCacheCheckHeap(SDFNewCache_t *pc, SDFNewCacheBucket_t *pbucket);
extern void SDFNewCacheAddLocker(SDFNewCacheBucket_t *pbucket);
extern void SDFNewCacheAddModObjectStats(SDFNewCacheEntry_t *pce);
extern void SDFNewCacheSubtractModObjectStats(SDFNewCacheEntry_t *pce);
extern SDF_status_t SDFNewCacheFlushCache(SDFNewCache_t *cache,
			SDF_cguid_t cguid,
			SDF_boolean_t flush_flag,
			SDF_boolean_t inval_flag,
                        void (*flush_progress_fn)(SDFNewCache_t *pc, void *wrbk_arg, uint32_t percent),
			void *wrbk_arg,
			SDF_boolean_t background_flush,
			SDF_boolean_t *pdirty_found,
			char *inval_prefix,
			uint32_t len_prefix);
extern SDF_status_t SDFNewCacheStartBackgroundFlusher(SDFNewCache_t *pc, void *setup_arg, SDF_status_t (*setup_action_state)(void *setup_arg, void **pflush_arg));
extern void SDFNewCacheCheckModifiedObjectLimit(SDFNewCacheBucket_t *pbucket, void *flush_arg);
extern void SDFNewCacheFlushLRUModObject(SDFNewCacheBucket_t *pbucket, void *flush_arg);

extern void SDFNewCacheTransientEntryCheck(SDFNewCacheBucket_t *pb, SDFNewCacheEntry_t *pce);

extern SDF_status_t SDFNewCacheGetModifiedObjects(SDFNewCache_t *pc, SDF_cache_enum_t *pes);
extern SDF_status_t SDFNewCacheGetModifiedObjectsCleanup(SDFNewCache_t *pc, SDF_cache_enum_t *pes);
extern void SDFNewCacheSetFlushTokens(SDFNewCache_t *pc, uint32_t ntokens);
extern void SDFNewCacheSetBackgroundFlushTokens(SDFNewCache_t *pc, uint32_t ntokens, uint32_t sleep_msec);
extern void SDFNewCacheSetModifiedLimit(SDFNewCache_t *pc, double fmod);
extern void SDFNewCacheFlushComplete(SDFNewCache_t *pc, SDF_boolean_t background_flush);

int
SDFNewCacheGetByMhash(SDFNewCache_t *pc, struct shard *shard, baddr_t baddr,
                      uint64_t hashbkt, hashsyn_t hashsyn,
                      char **key, uint64_t *key_len,
		      char **data, uint64_t *data_len);

int
SDFNewCacheInvalByMhash(SDFNewCache_t *pc, struct shard *shard, baddr_t baddr,
                        uint64_t hashbkt, hashsyn_t hashsyn);

void
SDFNewCacheInvalByCntr(SDFNewCache_t *pc, struct shard *shard,
		       SDF_cguid_t cguid);

extern int SDFNewCacheHashBits(SDFNewCache_t *pc); // TO SUPPORT VIRTUAL CONTAINERS

#ifdef	__cplusplus
}
#endif

#endif /* _FASTCC_H */
