/*
 * File:   action_new.c
 * Author: Brian O'Krafka
 *
 * Created on March 3, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * NOTES: xxxzzz
 *
 *     3/6/09: 
 *          - add a "no-expiry" bit to Xiaonan's directory
 *
 *     4/2/09: 
 *        - This code assumes there is strictly one shard per container!
 *        - Applications shouldn't mix "expiry time"
 *          accesses with "non-expiry time" accesses!
 *
 *
 * $Id: action_new.c 11989 2010-03-06 04:15:52Z drew $
 */

#define _ACTION_NEW_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "sdfmsg/sdf_msg_types.h"

#include "platform/stdlib.h"
#include "platform/stdio.h"
#include "platform/unistd.h"
#include "platform/shmem.h"
#include "platform/assert.h"
#include "platform/stats.h"
#include "shared/sdf_sm_msg.h"
#include "shared/name_service.h"
#include "shared/shard_compute.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_common.h"
#include "shared/init_sdf.h"
#include "shared/private.h"
#include "shared/object.h"
#include "ssd/ssd.h"
#include "ssd/ssd_aio.h"
#include "protocol/action/fastcc_new.h"
#include "protocol/action/action_thread.h"
#include "protocol/action/action_new.h"
#include "protocol/action/response_tbl.h"
#include "protocol/replication/key_lock.h"
#include "protocol/replication/replicator.h"
#include "protocol/protocol_alloc.h"
#include "utils/properties.h"
#include <inttypes.h>

#include "appbuf_pool.h"
#include "async_puts.h"

    /*  Uncomment this macro definition to compile in
     *  trace collection code.  It is only used if
     *  the logging level for sdf/prot=trace.
     */
#define INCLUDE_TRACE_CODE

    /*  Uncomment this macro definition to compile in
     *  code that checks heap consistency before and after
     *  every cache operation.
     */
// #define CHECK_HEAP

    /*  Uncomment this macro definition to compile in
     *  code that stress tests the cache heap allocation code.
     *  It requires CHECK_HEAP to be defined.
     */
// #define HEAP_STRESS_TEST

extern int     totalScheds; // from fth.c

extern HashMap meta_map; // from shared/cmc.c, used in action_stats()
extern HashMap cguid_map; // from shared/cmc.c, used in action_stats()

/*  Stop a shard map entry.
 *  Returns 0 if success, non-zero otherwise.
 */
extern int stop_home_shard_map_entry(SDF_action_thrd_state_t *pts, SDF_shardid_t shard);

/*  Start a shard map entry.
 *  Returns 0 if success, non-zero otherwise.
 */
extern int start_home_shard_map_entry(SDF_action_thrd_state_t *pts, SDF_shardid_t shard);

/*  Delete a shard map entry.
 *  Returns 0 if success, non-zero otherwise.
 */
extern int delete_home_shard_map_entry(SDF_action_thrd_state_t *pts, SDF_shardid_t shard);

/*  Create a shard map entry.
 *  Returns 0 if success, non-zero otherwise.
 */
extern int create_home_shard_map_entry(SDF_action_thrd_state_t *pts, SDF_shardid_t shard, struct shard *pshard, SDF_boolean_t stopflag);

    /*  These structures are used to build protocol tables that
     *  decide what to do for all (request,current_cache_state)
     *  combinations.
     */

#ifdef CHECK_HEAP
#ifdef HEAP_STRESS_TEST
static void heap_stress_test(SDF_trans_state_t *ptrans);
#endif
#endif

    /*  Functions used in protocol tables.
     */
static void bad(SDF_trans_state_t *ptrans);
static void noop(SDF_trans_state_t *ptrans);

    // flash actions for write-thru mode
static void flash_crtput_wt(SDF_trans_state_t *ptrans);
static void flsh_crpt_wt_s(SDF_trans_state_t *ptrans);
static void flash_put_wt_i(SDF_trans_state_t *ptrans); // cache state is I
static void flash_put_wt_s(SDF_trans_state_t *ptrans); // cache state is S
static void flash_set_wt_i(SDF_trans_state_t *ptrans); // cache state is I
static void flash_set_wt_s(SDF_trans_state_t *ptrans); // cache state is S

    // flash actions for write-back mode
static void flash_flush_wb(SDF_trans_state_t *ptrans);
static void flash_rup_wb(SDF_trans_state_t *ptrans); // remote update

    // common to both writeback and writeback modes
static void flash_get(SDF_trans_state_t *ptrans);

    // delete from flash
static void flash_del_i(SDF_trans_state_t *ptrans); // cache state is I
static void flash_del_s(SDF_trans_state_t *ptrans); // cache state is S
static void flash_del_x(SDF_trans_state_t *ptrans); // object is expired

    // cache operations
static void cache_get(SDF_trans_state_t *ptrans);

static PROT_TABLE  WBProtocolTable[N_SDF_APP_REQS];
static PROT_TABLE  WBExpiredTable[N_SDF_APP_REQS];
static PROT_TABLE  WTProtocolTable[N_SDF_APP_REQS];
static PROT_TABLE  WTExpiredTable[N_SDF_APP_REQS];

    /*  Here are the protocol tables that
     *  show what to do for all (request,current_cache_state)
     *  combinations.  There are four tables for these
     *  modes:
     *
     *     - write-back cache, object is not expired
     *     - write-back cache, object is expired
     *     - write-thru cache, object is not expired
     *     - write-thru cache, object is expired
     */
static PROT_TABLE  WBProtocolTable_Init[] = {
/*                  I                     S                     M          */
{APCOE, {{flash_crtput_wt,CS_M},{flsh_crpt_wt_s,CS_M},{flsh_crpt_wt_s,CS_M}}},
{APCOP, {{flash_crtput_wt,CS_M},{flsh_crpt_wt_s,CS_M},{flsh_crpt_wt_s,CS_M}}},
{APPAE, {{flash_put_wt_i, CS_M},{flash_put_wt_s,CS_M},{flash_put_wt_s,CS_M}}},
{APPTA, {{flash_put_wt_i, CS_M},{flash_put_wt_s,CS_M},{flash_put_wt_s,CS_M}}},
{APSOE, {{flash_set_wt_i, CS_M},{flash_set_wt_s,CS_M},{flash_set_wt_s,CS_M}}},
{APSOB, {{flash_set_wt_i, CS_M},{flash_set_wt_s,CS_M},{flash_set_wt_s,CS_M}}},
{APGRX, {{  flash_get,    CS_S},{  cache_get,   CS_S},{  cache_get,   CS_M}}},
{APGRD, {{  flash_get,    CS_S},{  cache_get,   CS_S},{  cache_get,   CS_M}}},
{APDBE, {{ flash_del_i,   CS_I},{ flash_del_s,  CS_I},{ flash_del_s,  CS_I}}},
{APDOB, {{ flash_del_i,   CS_I},{ flash_del_s,  CS_I},{ flash_del_s,  CS_I}}},
{APFLS, {{    noop,       CS_I},{    noop,      CS_S},{flash_flush_wb,CS_S}}},
{APFLI, {{    noop,       CS_I},{    noop,      CS_I},{flash_flush_wb,CS_I}}},
{APINV, {{    noop,       CS_I},{    noop,      CS_I},{     noop,     CS_I}}},
{APRIV, {{    noop,       CS_I},{    noop,      CS_I},{     noop,     CS_I}}},
{APRUP, {{ flash_rup_wb,  CS_M},{ flash_rup_wb, CS_M},{ flash_rup_wb, CS_M}}},
{APDUM, {{     bad,       CS_B},{    bad,       CS_B},{      bad,     CS_B}}},
};

static PROT_TABLE  WBExpiredTable_Init[] = {
/*                  I                     S                     M          */
{APCOE, {{     bad,       CS_B},{flash_set_wt_s,CS_M},{flash_set_wt_s,CS_M}}},
{APPAE, {{     bad,       CS_B},{ flash_del_x,  CS_I},{ flash_del_x,  CS_I}}},
{APSOE, {{     bad,       CS_B},{flash_set_wt_s,CS_M},{flash_set_wt_s,CS_M}}},
{APGRX, {{     bad,       CS_B},{ flash_del_x,  CS_I},{ flash_del_x,  CS_I}}},
{APDBE, {{     bad,       CS_B},{ flash_del_x,  CS_I},{ flash_del_x,  CS_I}}},
{APFLS, {{     bad,       CS_B},{ flash_del_x,  CS_I},{ flash_del_x,  CS_I}}},
{APFLI, {{     bad,       CS_B},{ flash_del_x,  CS_I},{ flash_del_x,  CS_I}}},
{APINV, {{     bad,       CS_B},{    noop,      CS_I},{     noop,     CS_I}}},
{APRIV, {{     bad,       CS_B},{    noop,      CS_I},{     noop,     CS_I}}},
{APRUP, {{     bad,       CS_B},{ flash_rup_wb, CS_M},{ flash_rup_wb, CS_M}}},
{APDUM, {{     bad,       CS_B},{    bad,       CS_B},{      bad,     CS_B}}},
};


static PROT_TABLE  WTProtocolTable_Init[] = {
/*                  I                     S                     M          */
{APCOE, {{flash_crtput_wt,CS_S},{flsh_crpt_wt_s,CS_S},{      bad,     CS_B}}},
{APCOP, {{flash_crtput_wt,CS_S},{flsh_crpt_wt_s,CS_S},{      bad,     CS_B}}},
{APPAE, {{flash_put_wt_i, CS_S},{flash_put_wt_s,CS_S},{      bad,     CS_B}}},
{APPTA, {{flash_put_wt_i, CS_S},{flash_put_wt_s,CS_S},{      bad,     CS_B}}},
{APSOE, {{flash_set_wt_i, CS_S},{flash_set_wt_s,CS_S},{      bad,     CS_B}}},
{APSOB, {{flash_set_wt_i, CS_S},{flash_set_wt_s,CS_S},{      bad,     CS_B}}},
{APGRX, {{  flash_get,    CS_S},{  cache_get,   CS_S},{      bad,     CS_B}}},
{APGRD, {{  flash_get,    CS_S},{  cache_get,   CS_S},{      bad,     CS_B}}},
{APDBE, {{ flash_del_i,   CS_I},{ flash_del_s,  CS_I},{      bad,     CS_B}}},
{APDOB, {{ flash_del_i,   CS_I},{ flash_del_s,  CS_I},{      bad,     CS_B}}},
{APFLS, {{    noop,       CS_I},{    noop,      CS_S},{      bad,     CS_B}}},
{APFLI, {{    noop,       CS_I},{    noop,      CS_I},{      bad,     CS_B}}},
{APINV, {{    noop,       CS_I},{    noop,      CS_I},{      bad,     CS_B}}},
{APRIV, {{    noop,       CS_I},{    noop,      CS_I},{      bad,     CS_B}}},
{APRUP, {{     bad,       CS_B},{    bad,       CS_B},{      bad,     CS_B}}},
{APDUM, {{     bad,       CS_B},{    bad,       CS_B},{      bad,     CS_B}}},
};

static PROT_TABLE  WTExpiredTable_Init[] = {
/*                  I                     S                     M          */
{APCOE, {{     bad,       CS_B},{flash_set_wt_s,CS_S},{      bad,     CS_B}}},
{APPAE, {{     bad,       CS_B},{ flash_del_x,  CS_I},{      bad,     CS_B}}},
{APSOE, {{     bad,       CS_B},{flash_set_wt_s,CS_S},{      bad,     CS_B}}},
{APGRX, {{     bad,       CS_B},{ flash_del_x,  CS_I},{      bad,     CS_B}}},
{APDBE, {{     bad,       CS_B},{ flash_del_x,  CS_I},{      bad,     CS_B}}},
{APFLS, {{     bad,       CS_B},{ flash_del_x,  CS_I},{      bad,     CS_B}}},
{APFLI, {{     bad,       CS_B},{ flash_del_x,  CS_I},{      bad,     CS_B}}},
{APINV, {{     bad,       CS_B},{    noop,      CS_I},{      bad,     CS_B}}},
{APRIV, {{     bad,       CS_B},{    noop,      CS_I},{      bad,     CS_B}}},
{APRUP, {{     bad,       CS_B},{    bad,       CS_B},{      bad,     CS_B}}},
{APDUM, {{     bad,       CS_B},{    bad,       CS_B},{      bad,     CS_B}}},
};

typedef struct sdf_queue_pair *QUEUE_PAIR_PTR;

// SDF state variables
extern struct SDF_shared_state sdf_shared_state;
extern SDF_cmc_t *theCMC;

// minimum number of buckets to use as default
#define SDF_CC_MIN_DEFAULT_BUCKETS  100000

// maximum number of shards per container
#define MAX_SHARD_IDs  100

static SDF_status_t flush_stuff(SDFNewCache_t *cache,
                        SDF_cguid_t cguid,
                        SDF_action_init_t *pai,
                        SDF_boolean_t inval_flag,
                        SDF_boolean_t ctnr_only);

static SDF_status_t inval_stuff(SDFNewCache_t *cache,
                        SDF_cguid_t cguid,
                        SDF_action_init_t *pai,
                        SDF_boolean_t ctnr_only);

/* these functions are not currently used */
#ifdef notdef
static uint64_t count_cache_entries(SDFNewCache_t *cache,
                                    SDF_cguid_t cguid,
                                    SDF_action_init_t *pai,
                                    SDF_boolean_t ctnr_only);
#endif

static SDF_tag_t get_tag(SDF_action_thrd_state_t *pts);
static void release_tag(SDF_action_thrd_state_t *pts);

static int sdfcc_print_fn(SDFNewCacheEntry_t *psce, char *sout, int max_len);
static SDF_boolean_t is_dirty_fn(SDFNewCacheEntry_t *pce);
static void wrbk_fn(SDFNewCacheEntry_t *pce, void *wrbk_arg);
static void init_state_fn(SDFNewCacheEntry_t *pce, SDF_time_t curtime);
static void update_state_fn(SDFNewCacheEntry_t *pce, SDF_time_t curtime, SDF_time_t exptime, SDF_boolean_t new_entry_flag);
static void sum_sched_stats(SDF_action_state_t *pas);
static void clear_all_sched_ctnr_stats(SDF_action_state_t *pas, int ctnr_index);
static void init_stats(SDF_action_stats_new_t *ps);
static int sprint_cache_state(char *s, SDFNewCacheEntry_t *psce, int max_len);

//==========================================================================
/* start of new stuff */

static void delete_from_flash(SDF_trans_state_t *ptrans);
static void get_from_flash(SDF_trans_state_t *ptrans);
static void put_to_flash(SDF_trans_state_t *ptrans);
static void set_to_flash(SDF_trans_state_t *ptrans, SDF_boolean_t skip_for_writeback);
static void create_put_to_flash(SDF_trans_state_t *ptrans);

static void load_cache(SDF_trans_state_t *ptrans, uint64_t size, void *pdata, SDF_boolean_t fail_on_malloc_failure);

static void get_flush_time(SDF_trans_state_t *ptrans);
static void inval_container(SDF_trans_state_t *ptrans);
static void flush_container(SDF_trans_state_t *ptrans);
static void flush_inval_container(SDF_trans_state_t *ptrans);
static SDF_trans_state_t *get_req_trans_state(SDF_action_init_t *pai, SDF_appreq_t *par);
static void free_trans_state(SDF_trans_state_t *ptrans);
static void load_proto_tbl(PROT_TABLE pt_load[], PROT_TABLE pt_from[]);
static void init_protocol_tables(SDF_action_state_t *pas);

static void process_bypass_operation(SDF_trans_state_t *ptrans);
static int get_container_index(SDF_action_init_t *pai, SDF_cguid_t cguid);
static SDF_boolean_t process_object_operation(SDF_trans_state_t *ptrans, SDF_boolean_t try_flag);
static void get_directory_entry(SDF_trans_state_t *ptrans, SDF_boolean_t lock_flag, SDF_boolean_t try_flag);
static SDF_boolean_t check_expiry_cache(SDF_trans_state_t *ptrans);
static SDF_boolean_t check_expiry_flash(SDF_trans_state_t *ptrans);
static PROT_TABLE *lookup_protocol_table(SDF_trans_state_t *ptrans);
SDF_status_t get_status(int retcode);
int get_retcode(SDF_status_t status);
static void finish_up(SDF_trans_state_t *ptrans);
static void load_return_values(SDF_trans_state_t *ptrans);
static void dump_flash_trace(SDF_trans_state_t *ptrans, char *name, int retcode);
static void destroy_per_thread_state(SDF_action_thrd_state_t *pts);
static int flashPut_wrapper(SDF_trans_state_t *ptrans, struct shard *pshard, struct objMetaData *pmeta, char *pkey, char *pdata, int flags, SDF_boolean_t skip_for_writeback);
static int shardSync_wrapper(SDF_trans_state_t *ptrans, struct shard *pshard);
static void drain_store_pipe(SDF_action_thrd_state_t *pts);
int do_put(SDF_async_put_request_t *pap, SDF_boolean_t unlock_slab);

static sdf_msg_t *load_msg(SDF_tag_t tag,
                           SDF_time_t  exp_time,
                           SDF_time_t  create_time,
                           SDF_vnode_t node_from, 
                           SDF_vnode_t node_to, 
                           SDF_protocol_msg_type_t msg_type,
                           SDF_size_t data_size, void *pdata,
                           SDF_context_t ctxt, SDF_cguid_t cguid,
                           SDF_transid_t transid, SDF_simple_key_t *pkey,
                           uint32_t flags, SDF_size_t *pmsize, 
                           SDF_time_t flushtime,
                           SDF_time_t curtime,
                           SDF_shardid_t shard,
                           uint64_t seqno,
                           SDF_container_meta_t *meta);

/* end of new stuff */
//==========================================================================

static struct sdf_queue_pair *get_queue_pair(uint32_t node_from, uint32_t node_to, uint32_t protocol)
{
    struct sdf_queue_pair   *x = NULL;
    
    x = sdf_create_queue_pair(node_from, node_to, protocol, protocol, SDF_WAIT_FTH);
    if (x == NULL) {
        plat_log_msg(21067, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                     "sdf_create_queue_pair failed in get_queue_pair!");
        plat_abort();
    }
    return(x);
}

void InitActionProtocolCommonState(SDF_action_state_t *pas, SDF_action_init_t *pai)
{
    int             i;
    const char     *per_thread_object_arena, *per_thread_nonobject_arena;
    uint64_t        nslabs, max_obj_size;
    uint32_t        avg_objsize;
    uint32_t        n_sync_container_threads;
    uint32_t        n_sync_container_cursors;
    uint32_t        max_key_size;
    uint64_t        buckets_default;
    const char     *enable_replication_string;
    const char     *enable_lru_string;
    SDF_boolean_t   lru_flag;
    #ifdef SIMPLE_REPLICATION
        const char *simple_replication_string;
    #endif

    #ifdef MALLOC_TRACE
        UTMallocTraceInit();
    #endif // MALLOC_TRACE

    enable_replication_string = getProperty_String("SDF_REPLICATION", "Off");
    plat_log_msg(21068, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, "PROP: SDF_REPLICATION=%s", enable_replication_string);
    if (strcmp(enable_replication_string, "On") == 0) {
        pas->enable_replication = SDF_TRUE;
    } else {
        pas->enable_replication = SDF_FALSE;
    }

    enable_lru_string = getProperty_String("SDF_LRU", "On");
    plat_log_msg(21069, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, "PROP: SDF_LRU=%s", enable_lru_string);
    if (strcmp(enable_lru_string, "On") == 0) {
        lru_flag = SDF_TRUE;
    } else {
        lru_flag = SDF_FALSE;
    }

    #ifdef SIMPLE_REPLICATION
        simple_replication_string = getProperty_String("SDF_SIMPLE_REPLICATION", "Off");
        plat_log_msg(21070, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, "PROP: SDF_SIMPLE_REPLICATION=%s", simple_replication_string);
        if (strcmp(simple_replication_string, "On") == 0) {
            pas->simple_replication = SDF_TRUE;
        } else {
            pas->simple_replication = SDF_FALSE;
        }
    #endif

    max_obj_size = getProperty_uLongLong("SDF_MAX_OBJ_SIZE", SDF_MAX_OBJ_SIZE);
    plat_log_msg(21071, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, "PROP: SDF_MAX_OBJ_SIZE=%"PRIu64, max_obj_size);
    max_obj_size += 20; // allow extra bytes for secret memcached metadata

    uint64_t cacheSize = getProperty_uLongLong("SDF_CC_MAXCACHESIZE", 100000000ULL);
    plat_log_msg(21072, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, "PROP: SDF_CC_MAXCACHESIZE=%"PRIu64,
                 cacheSize);

    avg_objsize = getProperty_uLongInt("SDF_CC_AVG_OBJSIZE", 1000);
    plat_log_msg(21073, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, "PROP: SDF_CC_AVG_OBJSIZE=%d", avg_objsize);

    buckets_default = cacheSize/avg_objsize;
    if (buckets_default < SDF_CC_MIN_DEFAULT_BUCKETS) {
        buckets_default = SDF_CC_MIN_DEFAULT_BUCKETS;
    }
    uint64_t buckets = getProperty_uLongLong("SDF_CC_BUCKETS", buckets_default);
    plat_log_msg(21074, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, "PROP: SDF_CC_BUCKETS=%"PRIu64, buckets);

    nslabs = getProperty_uLongLong("SDF_CC_NSLABS", 100);
    plat_log_msg(21075, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, "PROP: SDF_CC_NSLABS=%"PRIu64, nslabs);

    per_thread_nonobject_arena = getProperty_String("SDF_CACHE_NONOBJECT_ARENA", "root_thread");
    per_thread_object_arena    = getProperty_String("SDF_CACHE_OBJECT_ARENA", "cache_thread");

    NonCacheObjectArena = plat_shmem_str_to_arena(per_thread_nonobject_arena,
                                                  0 /* len */);
    if (NonCacheObjectArena == PLAT_SHMEM_ARENA_INVALID) {
        plat_log_msg(21078, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                     "Bad value for SDF_PER_THREAD_NONOBJECT_ARENA");
        plat_abort();
    }

    CacheObjectArena    = plat_shmem_str_to_arena(per_thread_object_arena,
                                                  0 /* len */);
    if (CacheObjectArena == PLAT_SHMEM_ARENA_INVALID) {
        plat_log_msg(21079, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                     "Bad value for SDF_PER_THREAD_OBJECT_ARENA");
        plat_abort();
    }

    #ifdef PROTOCOL_FORCE_ROOT_ARENA
        NonCacheObjectArena = PLAT_SHMEM_ARENA_ROOT_THREAD;
        CacheObjectArena    = PLAT_SHMEM_ARENA_ROOT_THREAD;
    #endif // PROTOCOL_FORCE_ROOT_ARENA

    plat_log_msg(21080, PLAT_LOG_CAT_PRINT_ARGS,
                 PLAT_LOG_LEVEL_INFO, "Cache arenas object %s nonobject %s",
                 plat_shmem_arena_to_str(CacheObjectArena),
                 plat_shmem_arena_to_str(NonCacheObjectArena));

    // cacheSize = 10000; /* for stress testing */
    // buckets = 2;
    // cacheSize = 4000; /* for stress testing */
    // buckets = 1;
    // nslabs  = 1; // for stress testing

    init_stats(&(pas->stats_new));
    pas->threadstates = NULL;
    pas->nbuckets  = buckets;
    pas->nslabs    = nslabs;
    pas->cachesize = cacheSize;
    MAKEARRAY(pas->new_actiondir, SDFNewCache_t, 1);
    #ifdef MALLOC_TRACE
        UTMallocTrace("actiondir", SDF_TRUE, SDF_FALSE, SDF_FALSE, (void *) pas->new_actiondir, sizeof(SDFNewCache_t));
    #endif // MALLOC_TRACE

    max_key_size = 256; // includes room for trailing NULL!
    SDFNewCacheInit(pas->new_actiondir, buckets, nslabs, cacheSize,
         max_key_size, max_obj_size, init_state_fn, update_state_fn,
         sdfcc_print_fn, is_dirty_fn, wrbk_fn, lru_flag);

    pas->nthrds = 0;
    fthLockInit(&pas->nthrds_lock);
    fthLockInit(&pas->ctnr_preload_lock);
    fthLockInit(&pas->stats_lock);
    fthLockInit(&pas->container_serialization_lock);
    pas->nnodes = pai->nnodes;
    pas->mynode = pai->nnode;
    pas->max_obj_size = max_obj_size;
    pas->max_key_size = max_key_size;

    fthLockInit(&pas->flush_all_lock);  // used to serialize flush_all operations
    fthLockInit(&pas->flush_ctnr_lock);  // used to serialize flush_container operations
    fthLockInit(&pas->sync_ctnr_lock);  // used to serialize sync_container operations
    fthLockInit(&pas->context_lock);
    pas->n_context = 100; /* the first 100 are reserved */

    /* new stuff for action_new.c */

    init_protocol_tables(pas);
    pas->n_containers = 0;
    pas->ctnr_meta = plat_alloc(SDF_MAX_CONTAINERS*sizeof(SDF_cache_ctnr_metadata_t));
    if (pas->ctnr_meta == NULL) {
        plat_log_msg(21081, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                     "plat_alloc failed!");
        plat_abort();
    }
    for (i=0; i<SDF_MAX_CONTAINERS; i++) {
        pas->ctnr_meta[i].valid = 0;
    }

    pas->flash_dev          = pai->flash_dev;
    pas->flash_dev_count    = pai->flash_dev_count;

    MAKEARRAY(pas->q_pair_responses, QUEUE_PAIR_PTR, pai->nnodes);
    #ifdef MALLOC_TRACE
        UTMallocTrace("q_pair_responses", TRUE, FALSE, FALSE, (void *) pas->q_pair_responses, sizeof(QUEUE_PAIR_PTR));
    #endif // MALLOC_TRACE

    for (i=0; i<pai->nnodes; i++) {
        // pas->q_pair_consistency[i] = get_queue_pair(pai->nnode /* node from */,
        //                                             i /* node to */, SDF_CONSISTENCY);
        pas->q_pair_responses[i]   = get_queue_pair(pai->nnode /* node from */,
                                                    i /* node to */,
                                                    SDF_RESPONSES);
    }

    fthLockInit(&pas->sync_remote_ctnr_lock);
    fthMboxInit(&(pas->git_mbx));

    n_sync_container_threads = getProperty_uLongInt("SDF_N_SYNC_CONTAINER_THREADS", DEFAULT_NUM_SYNC_CONTAINER_THREADS);
    plat_log_msg(21082, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, "PROP: SDF_N_SYNC_CONTAINER_THREADS=%d", n_sync_container_threads);

    n_sync_container_cursors = getProperty_uLongInt("SDF_N_SYNC_CONTAINER_CURSORS", DEFAULT_NUM_SYNC_CONTAINER_CURSORS);
    plat_log_msg(21083, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, "PROP: SDF_N_SYNC_CONTAINER_CURSORS=%d", n_sync_container_cursors);

    pas->gbc_mbx = plat_alloc(n_sync_container_threads*sizeof(fthMbox_t));
    if (pas->gbc_mbx == NULL) {
        plat_log_msg(21081, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                     "plat_alloc failed!");
        plat_abort();
    }
    for (i=0; i<n_sync_container_threads; i++) {
        fthMboxInit(&(pas->gbc_mbx[i]));
    }

    pas->cursor_mbx_todo = plat_alloc(n_sync_container_threads*sizeof(fthMbox_t));
    pas->cursor_mbx_done = plat_alloc(n_sync_container_threads*sizeof(fthMbox_t));
    if (pas->cursor_mbx_todo == NULL) {
        plat_log_msg(21081, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                     "plat_alloc failed!");
        plat_abort();
    }
    if (pas->cursor_mbx_done == NULL) {
        plat_log_msg(21081, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                     "plat_alloc failed!");
        plat_abort();
    }
    for (i=0; i<n_sync_container_threads; i++) {
        fthMboxInit(&(pas->cursor_mbx_todo[i]));
        fthMboxInit(&(pas->cursor_mbx_done[i]));
    }

    pas->cursor_datas = plat_alloc(n_sync_container_cursors*sizeof(struct cursor_data));
    if (pas->cursor_datas == NULL) {
        plat_log_msg(21081, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                     "plat_alloc failed!");
        plat_abort();
    }

    // Initialize the response combining table for replicated puts
    init_resp_tbl();

    #ifdef SIMPLE_REPLICATION
        if (pas->simple_replication) {
            simple_replication_init(pas);
        }
    #endif
}

void ShutdownActionProtocol(SDF_action_state_t *pas)
{
    SDF_action_thrd_state_t   *pts;
    SDF_action_thrd_state_t   *pts_next;

    for (pts = pas->threadstates; pts != NULL; pts = pts_next) {
        pts_next = pts->next;
        destroy_per_thread_state(pts);
    }

    SDFNewCacheDestroy(pas->new_actiondir);

    plat_free(pas->ctnr_meta);
}

void SDFClusterStatus(SDF_action_init_t *pai, uint32_t *mynode_id, uint32_t *cluster_size)
{
    SDF_action_state_t *pas;

    pas = pai->pcs;
    *mynode_id    = sdf_msg_myrank();
    *cluster_size = sdf_msg_numranks();
    if ((*cluster_size) == 0) {
        // messaging subsystem wasn't started
        *mynode_id    = 0;
        *cluster_size = 1;
    }
}

static void bad(SDF_trans_state_t *ptrans)
{
    ptrans->par->respStatus  = SDF_PROTOCOL_ERROR;
}

static void delete_from_flash(SDF_trans_state_t *ptrans)
{
    #ifdef ENABLE_MULTIPLE_FLASH_SUBSYSTEMS
        SDF_action_init_t  __attribute__((unused)) *pai = ptrans->pts->pai;
    #endif

    /* delete from flash */

    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[HFDFF])++;

    #ifdef FAKE_FLASH
        ptrans->flash_retcode = FLASH_EOK;
    #else
        ptrans->metaData.dataLen = 0;
        ptrans->flash_retcode = flashPut_wrapper(ptrans, ptrans->meta->pshard, &ptrans->metaData, ptrans->pflash_key, NULL, FLASH_PUT_TEST_NONEXIST, SDF_FALSE /* skip for writeback */);
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].flash_retcode_counts[ptrans->flash_retcode])++;

        if (ptrans->flash_retcode == 0) {
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[FHDEC])++;
        } else {
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[FHDEF])++;
        }

        #ifdef INCLUDE_TRACE_CODE
            dump_flash_trace(ptrans, "delete_from_flash", ptrans->flash_retcode);
        #endif
    #endif
}

static void get_from_flash(SDF_trans_state_t *ptrans)
{
    #ifdef ENABLE_MULTIPLE_FLASH_SUBSYSTEMS
        SDF_action_init_t  *pai = ptrans->pts->pai;
    #endif

    /* get from flash */

    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[HFGFF])++;

    #ifdef FAKE_FLASH
        ptrans->flash_retcode = FLASH_ENOENT;
    #else
        (ptrans->metaData.keyLen--); // adjust for extra NULL added by SDF
        ptrans->flash_retcode = flashGet(ptrans->meta->pshard, &ptrans->metaData, ptrans->pflash_key, &ptrans->pflash_data, FLASH_GET_NO_TEST);
        (ptrans->metaData.keyLen++); // adjust for extra NULL added by SDF
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].flash_retcode_counts[ptrans->flash_retcode])++;

        if (ptrans->flash_retcode == 0) {
            #ifdef MALLOC_TRACE
                UTMallocTrace("fastpath: flashGet", SDF_FALSE, SDF_FALSE, SDF_FALSE, (void *) ptrans->pflash_data, ptrans->metaData.dataLen);
            #endif // MALLOC_TRACE
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[FHDAT])++;
        } else {
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[FHGTF])++;
        }

        #ifdef INCLUDE_TRACE_CODE
            dump_flash_trace(ptrans, "get_from_flash", ptrans->flash_retcode);
        #endif
    #endif
}

static void put_to_flash(SDF_trans_state_t *ptrans)
{
    #ifdef ENABLE_MULTIPLE_FLASH_SUBSYSTEMS
        SDF_action_init_t  __attribute__((unused)) *pai = ptrans->pts->pai;
    #endif

    /* put to flash (object must already exist) */

    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[HFPTF])++;

    #ifdef FAKE_FLASH
        ptrans->flash_retcode = FLASH_EOK;
    #else
        ptrans->flash_retcode = flashPut_wrapper(ptrans, ptrans->meta->pshard, &ptrans->metaData, ptrans->pflash_key, ptrans->pflash_data, FLASH_PUT_TEST_NONEXIST, SDF_TRUE /* skip for writeback */);
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].flash_retcode_counts[ptrans->flash_retcode])++;

        if (ptrans->flash_retcode == 0) {
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[FHPTC])++;
        } else {
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[FHPTF])++;
        }

        #ifdef INCLUDE_TRACE_CODE
            dump_flash_trace(ptrans, "put_to_flash", ptrans->flash_retcode);
        #endif
    #endif
}

static void set_to_flash(SDF_trans_state_t *ptrans, SDF_boolean_t skip_for_writeback)
{
    #ifdef ENABLE_MULTIPLE_FLASH_SUBSYSTEMS
        SDF_action_init_t  __attribute__((unused)) *pai = ptrans->pts->pai;
    #endif

    /* create to flash (object may or may not already exist) */

    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[HFSET])++;

    #ifdef FAKE_FLASH
        ptrans->flash_retcode = FLASH_EOK;
    #else
        ptrans->flash_retcode = flashPut_wrapper(ptrans, ptrans->meta->pshard, &ptrans->metaData, ptrans->pflash_key, ptrans->pflash_data, FLASH_PUT_NO_TEST, skip_for_writeback /* skip for writeback */);
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].flash_retcode_counts[ptrans->flash_retcode])++;

        if (ptrans->flash_retcode == 0) {
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[FHSTC])++;
        } else {
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[FHSTF])++;
        }

        #ifdef INCLUDE_TRACE_CODE
            dump_flash_trace(ptrans, "set_to_flash", ptrans->flash_retcode);
        #endif
    #endif
}

static void create_put_to_flash(SDF_trans_state_t *ptrans)
{
    #ifdef ENABLE_MULTIPLE_FLASH_SUBSYSTEMS
        SDF_action_init_t  __attribute__((unused)) *pai = ptrans->pts->pai;
    #endif

    /* create to flash (object must NOT already exist) */

    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[HFCIF])++;

    #ifdef FAKE_FLASH
        ptrans->flash_retcode = FLASH_EOK;
    #else
        ptrans->flash_retcode = flashPut_wrapper(ptrans, ptrans->meta->pshard, &ptrans->metaData, ptrans->pflash_key, ptrans->pflash_data, FLASH_PUT_TEST_EXIST, SDF_TRUE /* skip for writeback */);
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].flash_retcode_counts[ptrans->flash_retcode])++;

        if (ptrans->flash_retcode == 0) {
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[FHCRC])++;
        } else {
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[FHCRF])++;
        }

        #ifdef INCLUDE_TRACE_CODE
            dump_flash_trace(ptrans, "create_put_to_flash", ptrans->flash_retcode);
        #endif
    #endif
}

static void load_cache(SDF_trans_state_t *ptrans, uint64_t size, void *pdata, SDF_boolean_t fail_on_malloc_failure)
{
    if ((ptrans->new_entry) ||
        (ptrans->entry->obj_size != size))
    {
        if (!ptrans->new_entry) {
            /* this is an overwrite, so get a whole new cache entry */

            #ifdef MALLOC_TRACE
                UTMallocTrace("overwriteCacheObject", SDF_FALSE, SDF_TRUE, SDF_FALSE, (void *) ptrans->entry, ptrans->entry->obj_size);
            #endif // MALLOC_TRACE

            ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].bytes_total_in_cache -= ptrans->entry->obj_size;
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].n_overwrites)++;
            ptrans->entry = SDFNewCacheOverwriteCacheObject(ptrans->pts->new_actiondir, ptrans->entry, size, (void *) ptrans);
        } else { // this is a new entry
            plat_assert(ptrans->entry->obj_size == 0);
            /* append an object buffer to the end of the current cache entry */
            ptrans->entry = SDFNewCacheCreateCacheObject(ptrans->pts->new_actiondir, ptrans->entry, size, (void *) ptrans);
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].n_new_entry)++;
            #ifdef MALLOC_TRACE
                UTMallocTrace("createCacheObject", SDF_FALSE, SDF_FALSE, SDF_FALSE, (void *) ptrans->entry, size);
            #endif // MALLOC_TRACE
            if (ptrans->entry->obj_size != size) {
                if (fail_on_malloc_failure) {
                    ptrans->par->respStatus = SDF_FAILURE_MEMORY_ALLOC;
                }
                ptrans->inval_object = SDF_TRUE;
                return;
            }
        }
    } else if (ptrans->entry->obj_size == size) {
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].n_in_place_overwrites)++;
        
    }
    SDFNewCacheCopyIntoObject(ptrans->pts->new_actiondir, pdata, ptrans->entry, size);
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].bytes_total_in_cache += size;
}

static void noop(SDF_trans_state_t *ptrans)
{
    /* purposefully empty */
}

static void flash_flush_wb(SDF_trans_state_t *ptrans)
{
    void                *pbuf;
    SDF_simple_key_t     simple_key;

    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[AHFLD])++;

    // Get a temporary contiguous data buffer for performing the flash set.

    pbuf = get_app_buf(ptrans->pts->pappbufstate, ptrans->entry->obj_size);
    #ifdef MALLOC_TRACE
        UTMallocTrace("pbuf_in", SDF_FALSE, SDF_FALSE, SDF_FALSE, (void *) pbuf, ptrans->entry->obj_size);
    #endif // MALLOC_TRACE

    if (pbuf == NULL) {
        plat_log_msg(21106, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                     "Not enough memory, plat_alloc() failed.");
        ptrans->par->respStatus = SDF_FAILURE_MEMORY_ALLOC;
        return;
    } else {
        SDFNewCacheCopyOutofObject(ptrans->pts->new_actiondir, pbuf, ptrans->entry, ptrans->entry->obj_size);
    }

    /* set to flash */
    SDFNewCacheCopyKeyOutofObject(ptrans->pas->new_actiondir, &simple_key, ptrans->entry);
    ptrans->pflash_key          = simple_key.key;
    ptrans->metaData.keyLen     = ptrans->entry->key_len;
    ptrans->pflash_data         = pbuf;
    ptrans->metaData.dataLen    = ptrans->entry->obj_size;
    ptrans->metaData.expTime    = ptrans->entry->exptime;
    ptrans->metaData.createTime = ptrans->entry->createtime;
    set_to_flash(ptrans, SDF_FALSE /* skip for writeback */);

    // free the data buffer
    free_app_buf(ptrans->pts->pappbufstate, pbuf);

    ptrans->par->respStatus = get_status(ptrans->flash_retcode);
    if (ptrans->par->respStatus != SDF_SUCCESS) {
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HAFLF])++;
        return;
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HAFLC])++;
}

static void flash_crtput_wt(SDF_trans_state_t *ptrans)
{
    SDF_boolean_t existing_but_expired;

    existing_but_expired = SDF_FALSE;

    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[AHCOP])++;

    ptrans->pflash_key          = ptrans->par->key.key;
    ptrans->metaData.keyLen     = ptrans->par->key.len;

    /*  If this command requires expiry checking, I must
     *  first get the object to do the check.
     *  If memcached is operating in cache mode, I COULD
     *  skip this check by "pretending" that the object
     *  was evicted by memcached.
     *  If memcached is operating in store mode, I
     *  must perform this check.
     *
     *  Note that applications shouldn't mix "expiry time"
     *  accesses with "non-expiry time" accesses!
     */

    if (SDF_App_Request_Info[ptrans->par->reqtype].check_expiry)
    {

        /* retrieve the object to check for expiry */

        /*  pflash_key and metaData.keyLen were already
         *  set up above.
         */

        /*  pflash_data must be set to NULL, otherwise
         *  flashGet will assume it is the pointer to a
         *  buffer in which to put the data!
         *  If you don't set it to NULL, you will get
         *  intermittent memory corruption problems!
         */
        ptrans->pflash_data         = NULL;

        get_from_flash(ptrans);

        if (ptrans->flash_retcode == FLASH_ENOENT) {

            /*  Object is not in flash, so proceed with the
             *  create.
             */
            /* purposefully empty */

        } else if (ptrans->flash_retcode != FLASH_EOK) {

            /*  flashGet failed for some reason, so the
             *  create fails.
             */
            plat_log_msg(21090, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "Flash get to check expiry for a create-put (wt) failed! (flash return code = %d)", ptrans->flash_retcode);
            ptrans->par->respStatus = SDF_EXPIRY_GET_FAILED;
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HACRF])++;
            return;
        } else {
            /* get was successful, so check for expiry */

            /* don't forget to free the buffer from flashGet */
            #ifdef MALLOC_TRACE
                UTMallocTrace("flash buf", FALSE, TRUE, FALSE, (void *) ptrans->pflash_data, 0);
            #endif // MALLOC_TRACE
            flashFreeBuf(ptrans->pflash_data);

            if (check_expiry_flash(ptrans)) {

                /* object has expired so create can proceed */

                existing_but_expired = SDF_TRUE;

            } else {

                /*  Object has NOT expired, so create fails.
                 *  However, we must still call create_put_to_flash()
                 *  to replicate this operation to maintain
                 *  consistency between nodes.
                 */

                /* purposefully empty! */

            }
        }
    }

    /* create-put to flash */
    /* key and keyLen were set up above */
    ptrans->pflash_data         = ptrans->par->pbuf_out;
    ptrans->metaData.dataLen    = ptrans->par->sze;
    ptrans->metaData.expTime    = ptrans->entry->exptime;
    ptrans->metaData.createTime = ptrans->entry->createtime;

    if (existing_but_expired) {
        /* force the put operation on an expired existing object */
        set_to_flash(ptrans, SDF_TRUE /* skip for writeback */);
    } else {
        create_put_to_flash(ptrans);
    }

    ptrans->par->respStatus = get_status(ptrans->flash_retcode);
    if (ptrans->par->respStatus != SDF_SUCCESS) {
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HACRF])++;
        return;
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HACRC])++;
    load_cache(ptrans, ptrans->par->sze, ptrans->par->pbuf_out, SDF_FALSE);
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].all_bytes_ever_created += ptrans->par->sze;
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].all_objects_ever_created++;
}

/*  The object exists in cache, but we need to write-thru anyway
 *  to force consistency on the remote node if replication is
 *  enabled.  We have already determined that the object has not expired.
 */
static void flsh_crpt_wt_s(SDF_trans_state_t *ptrans)
{
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[AHCOP])++;

    /* create-put to flash */
    ptrans->pflash_key          = ptrans->par->key.key;
    ptrans->metaData.keyLen     = ptrans->par->key.len;
    ptrans->pflash_data         = ptrans->par->pbuf_out;
    ptrans->metaData.dataLen    = ptrans->par->sze;
    ptrans->metaData.expTime    = ptrans->entry->exptime;
    ptrans->metaData.createTime = ptrans->entry->createtime;

    create_put_to_flash(ptrans);

    ptrans->par->respStatus = get_status(ptrans->flash_retcode);
    if (ptrans->par->respStatus != SDF_SUCCESS) {
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HACRF])++;
        return;
    }

    /*  Usually, we should never get here because we already know that the object
     *  exists in the cache.  However, in eviction mode the object may not reside
     *  in flash and the create/put may succeed!
     */
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HACRC])++;
    load_cache(ptrans, ptrans->par->sze, ptrans->par->pbuf_out, SDF_FALSE);
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].all_bytes_ever_created += ptrans->par->sze;
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].all_objects_ever_created++;
}

   /*  Object has state I, and we must check flash
    *  if the object is expired.
    */
static void flash_put_wt_i(SDF_trans_state_t *ptrans)
{
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[AHPTA])++;

    ptrans->pflash_key          = ptrans->par->key.key;
    ptrans->metaData.keyLen     = ptrans->par->key.len;

    /*  If this command requires expiry checking, I must
     *  first get the object to do the check.
     *
     *  Note that applications shouldn't mix "expiry time"
     *  accesses with "non-expiry time" accesses!
     */

    if (SDF_App_Request_Info[ptrans->par->reqtype].check_expiry)
    {

        /* retrieve the object to check for expiry */

        /*  pflash_key and metaData.keyLen were already
         *  set up above.
         */

        /*  pflash_data must be set to NULL, otherwise
         *  flashGet will assume it is the pointer to a
         *  buffer in which to put the data!
         *  If you don't set it to NULL, you will get
         *  intermittent memory corruption problems!
         */
        ptrans->pflash_data         = NULL;

        get_from_flash(ptrans);

        if (ptrans->flash_retcode == FLASH_ENOENT) {

            /*  Object is not in flash, so put fails.
             */

            /*  Call delete_from_flash so that any replicated copy
             *  is kept consistent!
             */
            delete_from_flash(ptrans);

            ptrans->par->respStatus = get_status(ptrans->flash_retcode);

            /*  Determine the final response based on the result
             *  of the delete operation.
             */

            switch (ptrans->par->respStatus) {
                case SDF_STOPPED_CONTAINER:
                case SDF_RMT_CONTAINER_UNKNOWN:
                case SDF_FLASH_RMT_EDELFAIL:
                case SDF_FLASH_EDELFAIL:
                    /*  Purposefully empty:
                     *  return status from corrective delete operation.
                     */
                    break;
                default:
                    /*  Return normal response for a put to a 
                     *  non-existent object.
                     */
                    ptrans->par->respStatus = SDF_OBJECT_UNKNOWN;
                    break;
            }

            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HAPAF])++;
            return;

        } else if (ptrans->flash_retcode != FLASH_EOK) {

            /*  flashGet failed for some reason, so the
             *  put fails.
             */
            plat_log_msg(21091, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "Flash get to check expiry for a put (wt) failed! (flash return code = %d)", ptrans->flash_retcode);
            ptrans->par->respStatus = SDF_EXPIRY_GET_FAILED;
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HAPAF])++;
            return;
        } else {
            /* get was successful, so check for expiry */

            /* don't forget to free the buffer from flashGet */
            #ifdef MALLOC_TRACE
                UTMallocTrace("flash buf", FALSE, TRUE, FALSE, (void *) ptrans->pflash_data, 0);
            #endif // MALLOC_TRACE
            flashFreeBuf(ptrans->pflash_data);

            if (!check_expiry_flash(ptrans)) {

                /* object has not expired, so put can proceed */
                /* purposefully empty */
            } else {

                /* object has expired, so put fails */

                /* delete the object (this doesn't require any I/O) */
                delete_from_flash(ptrans);

                if ((ptrans->flash_retcode != FLASH_EOK) &&
                    (ptrans->flash_retcode != FLASH_ENOENT))
                {
                    plat_log_msg(21087, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                                 "Flash delete for an expired put failed! (flash return code = %d)", ptrans->flash_retcode);
                    ptrans->par->respStatus = SDF_EXPIRY_DELETE_FAILED;
                } else {
                    ptrans->par->respStatus    = SDF_EXPIRED;
                    ptrans->inval_object       = SDF_TRUE;
                }

                (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HAPAF])++;
                return;
            }
        }
    }

    /* put to flash */
    /* key and keyLen were set up above */
    ptrans->pflash_data         = ptrans->par->pbuf_out;
    ptrans->metaData.dataLen    = ptrans->par->sze;
    ptrans->metaData.expTime    = ptrans->entry->exptime;
    ptrans->metaData.createTime = ptrans->entry->createtime;
    put_to_flash(ptrans);

    ptrans->par->respStatus = get_status(ptrans->flash_retcode);
    if (ptrans->par->respStatus != SDF_SUCCESS) {
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HAPAF])++;
        return;
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HAPAC])++;
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].all_bytes_ever_created += ptrans->par->sze;
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].all_objects_ever_created++;
    load_cache(ptrans, ptrans->par->sze, ptrans->par->pbuf_out, SDF_FALSE);
}

   /*  Object is in cache with state S, and is not expired.
    */
static void flash_put_wt_s(SDF_trans_state_t *ptrans)
{
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[AHPTA])++;

    /* put to flash */

    ptrans->pflash_key          = ptrans->par->key.key;
    ptrans->metaData.keyLen     = ptrans->par->key.len;
    ptrans->pflash_data         = ptrans->par->pbuf_out;
    ptrans->metaData.dataLen    = ptrans->par->sze;
    ptrans->metaData.expTime    = ptrans->entry->exptime;
    ptrans->metaData.createTime = ptrans->entry->createtime;

    /*  I use set here so that it will succeed even if
     *  the object has been evicted (if we are in eviction mode).
     *  This is correct because we still have a valid copy of the
     *  object in the cache.
     */
    set_to_flash(ptrans, SDF_TRUE /* skip for writeback */);

    ptrans->par->respStatus = get_status(ptrans->flash_retcode);
    if (ptrans->par->respStatus != SDF_SUCCESS) {
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HAPAF])++;
        return;
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HAPAC])++;
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].all_bytes_ever_created += ptrans->par->sze;
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].all_objects_ever_created++;
    load_cache(ptrans, ptrans->par->sze, ptrans->par->pbuf_out, SDF_FALSE);
}

   /*  Object is not in cache (has state CS_I).
    *  I don't have to check expiry.
    */
static void flash_set_wt_i(SDF_trans_state_t *ptrans)
{
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[AHSOP])++;

    /* set to flash */
    ptrans->pflash_key          = ptrans->par->key.key;
    ptrans->metaData.keyLen     = ptrans->par->key.len;
    ptrans->pflash_data         = ptrans->par->pbuf_out;
    ptrans->metaData.dataLen    = ptrans->par->sze;
    ptrans->metaData.expTime    = ptrans->entry->exptime;
    ptrans->metaData.createTime = ptrans->entry->createtime;
    set_to_flash(ptrans, SDF_TRUE /* skip for writeback */);

    ptrans->par->respStatus = get_status(ptrans->flash_retcode);
    if (ptrans->par->respStatus != SDF_SUCCESS) {
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HASTF])++;
        return;
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HASTC])++;
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].all_bytes_ever_created += ptrans->par->sze;
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].all_objects_ever_created++;

    load_cache(ptrans, ptrans->par->sze, ptrans->par->pbuf_out, SDF_FALSE);
}

   /*  Object is in cache with state S, and is not expired.
    */
static void flash_set_wt_s(SDF_trans_state_t *ptrans)
{
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[AHSOP])++;

    /* set to flash */

    ptrans->pflash_key          = ptrans->par->key.key;
    ptrans->metaData.keyLen     = ptrans->par->key.len;
    ptrans->pflash_data         = ptrans->par->pbuf_out;
    ptrans->metaData.dataLen    = ptrans->par->sze;
    ptrans->metaData.expTime    = ptrans->entry->exptime;
    ptrans->metaData.createTime = ptrans->entry->createtime;
    set_to_flash(ptrans, SDF_TRUE /* skip for writeback */);

    ptrans->par->respStatus = get_status(ptrans->flash_retcode);
    if (ptrans->par->respStatus != SDF_SUCCESS) {
        /*  Suppress the SDF_EXPIRED status so that the more
         *  precise failure status will be returned.
         */
        ptrans->expired_or_flushed = SDF_FALSE;
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HASTF])++;
        return;
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HASTC])++;
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].all_bytes_ever_created += ptrans->par->sze;
    ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].all_objects_ever_created++;

    load_cache(ptrans, ptrans->par->sze, ptrans->par->pbuf_out, SDF_FALSE);
}

   /*  Update the cache because of a remote replication for
    *  a writeback container.
    */
static void flash_rup_wb(SDF_trans_state_t *ptrans)
{
    load_cache(ptrans, ptrans->par->sze, ptrans->par->pbuf_out, SDF_FALSE);
}

static void flash_get(SDF_trans_state_t *ptrans)
{
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[AHGTR])++;

    /* get from flash */
    ptrans->pflash_key          = ptrans->par->key.key;
    ptrans->metaData.keyLen     = ptrans->par->key.len;

    /*  pflash_data must be set to NULL, otherwise
     *  flashGet will assume it is the pointer to a
     *  buffer in which to put the data!
     *  If you don't set it to NULL, you will get
     *  intermittent memory corruption problems!
     */
    ptrans->pflash_data         = NULL;

    get_from_flash(ptrans);

    ptrans->par->respStatus = get_status(ptrans->flash_retcode);
    if (ptrans->par->respStatus != SDF_SUCCESS) {
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HAGRF])++;
        return;
    } else {
        /* get was successful, so check for expiry */
        if (!check_expiry_flash(ptrans)) {
            /* object has NOT expired */
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HAGRC])++;
        } else {
            /* object has expired, so delete it! */
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[FHGXP])++;
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HAGRF])++;

            /* don't forget to free the buffer from flashGet */
            flashFreeBuf(ptrans->pflash_data);
            #ifdef MALLOC_TRACE
                UTMallocTrace("flash buf", FALSE, TRUE, FALSE, (void *) pdata, 0);
            #endif // MALLOC_TRACE

            /* delete the object (this doesn't require any I/O) */
            delete_from_flash(ptrans);

            if ((ptrans->flash_retcode != FLASH_EOK) &&
                (ptrans->flash_retcode != FLASH_ENOENT))
            {
                (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[FHGDF])++;
                plat_log_msg(21092, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                             "Flash delete for an expired get failed! (flash return code = %d)", ptrans->flash_retcode);
                ptrans->par->respStatus = SDF_EXPIRY_DELETE_FAILED;
            } else {
                ptrans->par->respStatus    = SDF_EXPIRED;
                ptrans->inval_object       = SDF_TRUE;
            }
            return;
        }
    }

    /* Don't forget to load the expiry and create times. */
    ptrans->entry->exptime    = ptrans->metaData.expTime;
    ptrans->entry->createtime = ptrans->metaData.createTime;

    load_cache(ptrans, ptrans->metaData.dataLen, ptrans->pflash_data, SDF_TRUE);

    /* free the flash buffer */
    #ifdef MALLOC_TRACE
        UTMallocTrace("flash buf", SDF_FALSE, SDF_TRUE, SDF_FALSE, (void *) ptrans->pflash_data, 0);
    #endif // MALLOC_TRACE
    flashFreeBuf(ptrans->pflash_data);
}

/*  Delete from flash (but first do an expiry check so
 *  that I can determine if the object is unknown or not).
 */
static void flash_del_i(SDF_trans_state_t *ptrans)
{
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[AHDOB])++;

    /* delete from flash */
    ptrans->pflash_key          = ptrans->par->key.key;
    ptrans->metaData.keyLen     = ptrans->par->key.len;

    /*  If we are in cache (non-store) mode, we COULD skip
     *  the expiry time check (for the case in which the object
     *  is still in flash, but has expired, we can pretend 
     *  that the object has been evicted).
     *  We can also skip the expiry time check if the specific
     *  request does not require it.
     *
     *  Note that applications shouldn't mix "expiry time"
     *  accesses with "non-expiry time" accesses!
     */

    if (!SDF_App_Request_Info[ptrans->par->reqtype].check_expiry)
    {
        /*  I don't have to check for expiry.
         */
        delete_from_flash(ptrans);

        ptrans->par->respStatus = get_status(ptrans->flash_retcode);
        if (ptrans->par->respStatus != SDF_SUCCESS) {
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HADEF])++;
            return;
        }
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HADEC])++;
    } else {

        /* retrieve the object to check for expiry */

        /*  pflash_key and metaData.keyLen were already
         *  set up above.
         */

        /*  pflash_data must be set to NULL, otherwise
         *  flashGet will assume it is the pointer to a
         *  buffer in which to put the data!
         *  If you don't set it to NULL, you will get
         *  intermittent memory corruption problems!
         */
        ptrans->pflash_data         = NULL;

        get_from_flash(ptrans);

        if (ptrans->flash_retcode == FLASH_ENOENT) {

            /* Object does not exist. */

            /*  Call delete_from_flash so that any replicated copy
             *  is kept consistent!
             */
            delete_from_flash(ptrans);

            ptrans->par->respStatus = get_status(ptrans->flash_retcode);

            /*  Determine the final response based on the result
             *  of the delete operation.
             */

            switch (ptrans->par->respStatus) {
                case SDF_STOPPED_CONTAINER:
                case SDF_RMT_CONTAINER_UNKNOWN:
                case SDF_FLASH_RMT_EDELFAIL:
                case SDF_FLASH_EDELFAIL:
                    /*  Purposefully empty:
                     *  return status from corrective delete operation.
                     */
                    break;
                default:
                    /*  Return normal response for a delete to a 
                     *  non-existent object.
                     */
                    ptrans->par->respStatus = SDF_OBJECT_UNKNOWN;
                    break;
            }

            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HADEF])++;

        } else if (ptrans->flash_retcode != FLASH_EOK) {

            /*  flashGet failed for some reason
             */
            plat_log_msg(21093, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "Flash get to check expiry for a delete failed! (flash return code = %d)", ptrans->flash_retcode);
            ptrans->par->respStatus = SDF_EXPIRY_GET_FAILED;
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HADEF])++;
            return;
        } else {
            /* get was successful, so check for expiry */

            /* don't forget to free the buffer from flashGet */
            #ifdef MALLOC_TRACE
                UTMallocTrace("flash buf", FALSE, TRUE, FALSE, (void *) ptrans->pflash_data, 0);
            #endif // MALLOC_TRACE
            flashFreeBuf(ptrans->pflash_data);

            /*  Whether or not the object is expired, we
             *  still want to delete it.  Expiry just affects
             *  the return status (SUCCESS or OBJECT_UNKNOWN).
             */

            if (check_expiry_flash(ptrans)) {

                /*  Object has expired, so do the delete 
                 *  and return OBJECT_UNKNOWN.
                 */

                delete_from_flash(ptrans);
                if ((ptrans->flash_retcode != FLASH_EOK) &&
                    (ptrans->flash_retcode != FLASH_ENOENT))
                {
                    plat_log_msg(21094, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                                 "Flash delete of an expired object in flash failed! (flash return code = %d)", ptrans->flash_retcode);
                    ptrans->par->respStatus = SDF_EXPIRY_DELETE_FAILED;
                } else {
                    ptrans->par->respStatus = SDF_OBJECT_UNKNOWN;
                }
                (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HADEF])++;
            } else {

                /*  Object has NOT expired, so do the delete 
                 *  and return SUCCESS.
                 */
                delete_from_flash(ptrans);
                if (ptrans->flash_retcode == FLASH_EOK) {
                    ptrans->par->respStatus = SDF_SUCCESS;
                    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HADEC])++;
                } else {
                    ptrans->par->respStatus = get_status(ptrans->flash_retcode);
                    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HADEF])++;
                    return;
                }
            }
        }
    }

    /*  Deletion from the cache will occur automatically
     *  when the main routine checks the new cache state
     *  and finds it invalid.
     */
}


/* delete from flash because the object has expired */
static void flash_del_x(SDF_trans_state_t *ptrans)
{
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[AHDOB])++;

    /* delete from flash */
    ptrans->pflash_key          = ptrans->par->key.key;
    ptrans->metaData.keyLen     = ptrans->par->key.len;

    /*  I know I am expired, so I don't have to check 
     *  for expiry again.
     */
    delete_from_flash(ptrans);

    ptrans->par->respStatus = get_status(ptrans->flash_retcode);
    if (ptrans->par->respStatus == SDF_OBJECT_UNKNOWN) {
        /* object was evicted from flash, so force success */
        ptrans->par->respStatus = SDF_SUCCESS;
    }
    if (ptrans->par->respStatus != SDF_SUCCESS) {
        /*  Suppress the SDF_EXPIRED status so that the more
         *  precise "SDF_EXPIRY_DELETE_FAILED" will be returned.
         */
        ptrans->expired_or_flushed = SDF_FALSE;
        ptrans->par->respStatus = SDF_EXPIRY_DELETE_FAILED;
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HADEF])++;
        return;
    }

    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HADEC])++;
    ptrans->inval_object = SDF_TRUE;

    /*  Deletion from the cache will occur automatically
     *  when the main routine checks the new cache state
     *  and finds it invalid.
     */
}

/*  Delete from flash without doing an expiry check.
 *
 *  The object is in the cache in the S state, and an
 *  expiry check already showed that the object is not expired.
 */
static void flash_del_s(SDF_trans_state_t *ptrans)
{
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_out_counts[AHDOB])++;

    /* delete from flash */
    ptrans->pflash_key          = ptrans->par->key.key;
    ptrans->metaData.keyLen     = ptrans->par->key.len;

    delete_from_flash(ptrans);

    ptrans->par->respStatus = get_status(ptrans->flash_retcode);
    if (ptrans->par->respStatus == SDF_OBJECT_UNKNOWN) {
        /* object was evicted from flash, so force success */
        ptrans->par->respStatus = SDF_SUCCESS;
    }
    if (ptrans->par->respStatus != SDF_SUCCESS) {
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HADEF])++;
    } else {
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].msg_in_counts[HADEC])++;
    }

    /*  Deletion from the cache will occur automatically
     *  when the main routine checks the new cache state
     *  and finds it invalid.
     */
}

static void cache_get(SDF_trans_state_t *ptrans)
{
    /* purposefully empty */
    /* data copying will occur in the calling routine */
}

static void flush_all_local(SDF_trans_state_t *ptrans)
{
    fthWaitEl_t                *wait;
    SDF_cache_ctnr_metadata_t  *pmeta_cmc;

    ptrans->par->respStatus  = SDF_SUCCESS; /* default status */

    ptrans->meta = get_container_metadata(ptrans->pai, ptrans->par->ctnr);
    if (ptrans->meta == NULL) {
        plat_log_msg(21098, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "Failed to get container metadata");

        // plat_abort();
        ptrans->par->respStatus  = SDF_GET_METADATA_FAILED;
        return;
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].appreq_counts[ptrans->par->reqtype])++;

    /* serialize flush_all operations */
    wait = fthLock(&(ptrans->pts->phs->flush_all_lock), 1, NULL);

    /* just set the global flush time in the container metadata */
    // ensures that future sets will be in the next time epoch
    fthNanoSleep(1100000000ULL); 

    ptrans->meta->meta.flush_time     = ptrans->par->invtime;
    ptrans->meta->meta.flush_set_time = ptrans->par->curtime;

    if (name_service_put_meta(ptrans->pai, ptrans->par->ctnr, 
                              &(ptrans->meta->meta)) != SDF_SUCCESS) 
    {
       plat_log_msg(21099, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                    "name_service_put_meta failed!");
        ptrans->par->respStatus  = SDF_PUT_METADATA_FAILED;
    } else {

        /* do an ssd_sync on the CMC */
        pmeta_cmc = get_container_metadata(ptrans->pai, CMC_CGUID);
        if (pmeta_cmc == NULL) {
            plat_log_msg(21100, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                         "Failed to get container metadata for CMC");
            // plat_abort();
            ptrans->par->respStatus  = SDF_GET_METADATA_FAILED;
        } else {
            ptrans->par->respStatus = shardSync_wrapper(ptrans, pmeta_cmc->pshard);
        }
    }

    fthUnlock(wait);


}

static void get_flush_time(SDF_trans_state_t *ptrans)
{
    SDF_time_t                  invtime;

    ptrans->par->respStatus  = SDF_SUCCESS; /* default status */

    ptrans->meta = get_container_metadata(ptrans->pai, ptrans->par->ctnr);
    if (ptrans->meta == NULL) {
        plat_log_msg(21098, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "Failed to get container metadata");

        // plat_abort();
        ptrans->par->respStatus  = SDF_GET_METADATA_FAILED;
        return;
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].appreq_counts[ptrans->par->reqtype])++;

    invtime = ptrans->meta->meta.flush_time;
    ptrans->par->exptime = invtime;
}

SDF_status_t flush_all(SDF_trans_state_t *ptrans)
{
#ifdef SIMPLE_REPLICATION
    SDF_boolean_t send_msg_flag = SDF_TRUE;    
    vnode_t                  to_node = -1;
    service_t                to_service;
    qrep_state_t            *ps;
    SDF_size_t               msize;
    SDF_protocol_msg_type_t  hf_mtype;
    struct sdf_msg          *send_msg = NULL;
    struct sdf_msg          *new_msg = NULL;
    SDF_protocol_msg_t      *pm_new = NULL;
    SDF_status_t             ret;
    SDF_status_t             error;

    if (SDFSimpleReplication) {
        send_msg_flag = SDF_FALSE;

        ps = &(ptrans->pas->qrep_state);

        to_node = partner_replica_by_cguid(ps, ptrans->par->ctnr);
        
        if (to_node != SDF_ILLEGAL_VNODE && node_is_alive(ps, to_node)) {
            send_msg_flag = SDF_TRUE;
            to_service    = SDF_FLSH;
        }
        
        ptrans->meta = get_container_metadata(ptrans->pai, ptrans->par->ctnr);
        if (ptrans->meta == NULL) {
            plat_log_msg(21098, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                         "Failed to get container metadata");
            
            // plat_abort();
            ptrans->par->respStatus  = SDF_GET_METADATA_FAILED;
            return SDF_FAILURE;
        }

        if (send_msg_flag == SDF_FALSE) {
            flush_all_local(ptrans);
        } else {
            /*  Create a message and pass it to the replication service.
             */

            hf_mtype = HFFLA; // flush all
            
            send_msg = load_msg(ptrans->tag,
                                0,
                                0,
                                ptrans->pts->phs->mynode, // from
                                to_node, // to
                                hf_mtype,
                                0, 
                                NULL,
                                ptrans->par->ctxt, 
                                ptrans->par->ctnr,
                                0,                        // transid
                                NULL,
                                0,                        // flags
                                &msize, 
                                ptrans->par->invtime,    // flushtime
                                ptrans->par->curtime,    // curtime
                                ptrans->meta->meta.shard,
                                0,
                                &(ptrans->meta->meta));

            if (send_msg == NULL) {
                return(SDF_FAILURE_MSG_ALLOC);
            }
            
            if (sdf_msg_send(send_msg,
                             msize,
                             /* to */
                             to_node,
                             to_service,
                             /* from */
                             ptrans->pts->phs->mynode,
                             SDF_RESPONSES,
                             FLSH_REQUEST,
                             &(ptrans->pts->req_mbx), NULL)) {
                
                plat_log_msg(21101, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                             "sdf_msg_send to replication failed");
                return(SDF_FAILURE_MSG_SEND);
            }

            /*
             * Do the flush locally. This is done here to overlap the
             * remote call and the local call to have some parallelism
             */
            flush_all_local(ptrans);
            
            /* Wait for the response */
            new_msg = (struct sdf_msg *)fthMboxWait(&ptrans->pts->req_resp_fthmbx);
            
            /* Check message response */
            error = sdf_msg_get_error_status(new_msg);
            if (error != SDF_SUCCESS) {
                sdf_dump_msg_error(error, new_msg);
                sdf_msg_free_buff(new_msg);
                return SDF_FAILURE_MSG_RECEIVE;
            }
            
            pm_new = (SDF_protocol_msg_t *) (new_msg->msg_payload);
            
            ret = pm_new->status;
            
            // garbage collect the response message
            sdf_msg_free_buff(new_msg);
        }
        
    } else
#endif
    {
        flush_all_local(ptrans);
    }

    return SDF_SUCCESS;
}

void sdf_dump_msg_error(SDF_status_t error, struct sdf_msg * msg)
{
    if (error == SDF_SUCCESS) {
        plat_log_msg(21102, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                     "sdf_msg_send to replication succeed\n");
    } else if (error == SDF_NODE_DEAD) {
        plat_log_msg(21103, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                     "sdf_msg_send to failed: Node (%d) died\n", msg->msg_src_vnode);

            printf("node %d died", msg->msg_src_vnode);
    } else if (error == SDF_TIMEOUT) {
        plat_log_msg(21104, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                     "sdf_msg_send to failed: Message timeout to node %d\n", msg->msg_src_vnode);
    } else {
        plat_log_msg(21105, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                     "sdf_msg_send to failed: Unknown error (%d) to node %d\n", error, msg->msg_src_vnode);

    }
    
}

/* Request from remote node to flush all */
void flush_all_remote_request(SDF_action_init_t * pai, SDF_appreq_t * par)
{
    SDF_trans_state_t * ptrans;

    ptrans = get_trans_state((SDF_action_thrd_state_t *) pai->pts);
    ptrans->par = par;
    ptrans->pai = pai;
    ptrans->pas = pai->pcs;
    ptrans->pts = pai->pts;
    ptrans->pts->phs = pai->pcs;

    flush_all_local(ptrans);

    free_trans_state(ptrans);
}

static void sync_container(SDF_trans_state_t *ptrans)
{
    fthWaitEl_t   *wait;

    ptrans->par->respStatus  = SDF_SUCCESS; /* default status */

    ptrans->meta = get_container_metadata(ptrans->pai, ptrans->par->ctnr);
    if (ptrans->meta == NULL) {
        plat_log_msg(21098, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "Failed to get container metadata");

        // plat_abort();
        ptrans->par->respStatus  = SDF_GET_METADATA_FAILED;
        return;
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].appreq_counts[ptrans->par->reqtype])++;

    /* serialize sync_all/flush_all operations */
    wait = fthLock(&(ptrans->pts->phs->sync_ctnr_lock), 1, NULL);

    /* sync ssd devices */
    ptrans->par->respStatus = shardSync_wrapper(ptrans, ptrans->meta->pshard);

    fthUnlock(wait);
}

static void flush_container(SDF_trans_state_t *ptrans)
{
    fthWaitEl_t   *wait;

    ptrans->par->respStatus  = SDF_SUCCESS; /* default status */

    ptrans->meta = get_container_metadata(ptrans->pai, ptrans->par->ctnr);
    if (ptrans->meta == NULL) {
        plat_log_msg(21098, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "Failed to get container metadata");

        // plat_abort();
        ptrans->par->respStatus  = SDF_GET_METADATA_FAILED;
        return;
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].appreq_counts[ptrans->par->reqtype])++;

    if (!ptrans->meta->meta.properties.cache.writethru) {
        /* We only have to do this if it is a writeback container */
        /* serialize flush_all operations */
        wait = fthLock(&(ptrans->pts->phs->flush_ctnr_lock), 1, NULL);

        ptrans->par->respStatus = flush_stuff(ptrans->pts->new_actiondir, 
                                         ptrans->par->ctnr,
                                         ptrans->pai, 
                                         SDF_FALSE,  // inval_flag
                                         SDF_TRUE);  // ctnr_only

        fthUnlock(wait);
    }
}

static void inval_container(SDF_trans_state_t *ptrans)
{
    fthWaitEl_t   *wait;

    ptrans->par->respStatus  = SDF_SUCCESS; /* default status */

    ptrans->meta = get_container_metadata(ptrans->pai, ptrans->par->ctnr);
    if (ptrans->meta == NULL) {
        plat_log_msg(21098, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "Failed to get container metadata");

        // plat_abort();
        ptrans->par->respStatus  = SDF_GET_METADATA_FAILED;
        return;
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].appreq_counts[ptrans->par->reqtype])++;

    /* serialize inval operations */
    wait = fthLock(&(ptrans->pts->phs->flush_ctnr_lock), 1, NULL);

    ptrans->par->respStatus = inval_stuff(ptrans->pts->new_actiondir, 
                                     ptrans->par->ctnr,
                                     ptrans->pai, 
                                     SDF_TRUE);  // ctnr_only

    fthUnlock(wait);
}

static void flush_inval_container(SDF_trans_state_t *ptrans)
{
    fthWaitEl_t   *wait;

    ptrans->par->respStatus  = SDF_SUCCESS; /* default status */

    ptrans->meta = get_container_metadata(ptrans->pai, ptrans->par->ctnr);
    if (ptrans->meta == NULL) {
        plat_log_msg(21098, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "Failed to get container metadata");

        // plat_abort();
        ptrans->par->respStatus  = SDF_GET_METADATA_FAILED;
        return;
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].appreq_counts[ptrans->par->reqtype])++;

    /* serialize flush_all operations */
    wait = fthLock(&(ptrans->pts->phs->flush_ctnr_lock), 1, NULL);

    ptrans->par->respStatus = flush_stuff(ptrans->pts->new_actiondir, 
                                     ptrans->par->ctnr,
                                     ptrans->pai, 
                                     SDF_TRUE,  // inval_flag
                                     SDF_TRUE);  // ctnr_only

    fthUnlock(wait);
}

SDF_trans_state_t *get_trans_state(SDF_action_thrd_state_t *pts)
{
   int                   i;
   SDF_trans_state_t     *ptrans;

   ptrans = pts->free_trans_states;
   if (ptrans == NULL) {
       pts->total_trans_state_structs += 1;
       ptrans = proto_plat_alloc_arena(sizeof(SDF_trans_state_t), PLAT_SHMEM_ARENA_CACHE_THREAD);
        #ifdef MALLOC_TRACE
            UTMallocTrace("get_trans_state", SDF_FALSE, SDF_FALSE, SDF_FALSE, (void *) ptrans, sizeof(SDF_trans_state_t));
        #endif // MALLOC_TRACE
       if (ptrans == NULL) {
           plat_log_msg(21106, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                        "Not enough memory, plat_alloc() failed.");
           plat_abort();
       }

       for (i=0; i<1; i++) {
           ptrans->next = pts->free_trans_states;
           pts->free_trans_states = ptrans;
           ptrans++;
       }
       ptrans = pts->free_trans_states;
       pts->free_trans_states = ptrans->next;
   } else {
       pts->free_trans_states = ptrans->next;
   }
   (pts->used_trans_state_structs)++;

   return(ptrans);
}

static void free_trans_state(SDF_trans_state_t *ptrans)
{
   ptrans->next = ptrans->pts->free_trans_states;
   ptrans->pts->free_trans_states = ptrans;
   (ptrans->pts->used_trans_state_structs)--;
}

static void load_proto_tbl(PROT_TABLE pt_load[], PROT_TABLE pt_from[])
{
    int   i, j, ifrom;

    /* initialize everything to "bad" */
    for (i=0; i<N_SDF_APP_REQS; i++) {
        pt_load[i].reqtype   = i;
        for (j=0; j<N_CACHE_STATES; j++) {
            pt_load[i].fn_state[j].new_state = CS_B;
            pt_load[i].fn_state[j].action_fn = bad;
        }
    }

    /* load non-bad entries */
    for (ifrom=0; pt_from[ifrom].reqtype != APDUM; ifrom++) {
        for (i=0; i<N_SDF_APP_REQS; i++) {
            if (i == pt_from[ifrom].reqtype) {
                pt_load[i].reqtype   = pt_from[ifrom].reqtype;
                for (j=0; j<N_CACHE_STATES; j++) {
                    pt_load[i].fn_state[j].new_state = pt_from[ifrom].fn_state[j].new_state;
                    pt_load[i].fn_state[j].action_fn = pt_from[ifrom].fn_state[j].action_fn;
                }
                break;
            }
        }
    }
}

static void init_protocol_tables(SDF_action_state_t *pas)
{
    load_proto_tbl(WBProtocolTable, WBProtocolTable_Init);
    load_proto_tbl(WBExpiredTable,  WBExpiredTable_Init);
    load_proto_tbl(WTProtocolTable, WTProtocolTable_Init);
    load_proto_tbl(WTExpiredTable,  WTExpiredTable_Init);
}
        
    /*  Process an event that the action node state
     *  machine is waiting on.
     */
void ActionProtocolAgentResume(SDF_trans_state_t *ptrans)
{
    /* purposefully empty */
    plat_abort();
}

SDF_status_t SDFGetCacheStat(SDF_action_init_t *pai, SDF_CONTAINER container, int stat_name, uint64_t *pstat)
{
    SDF_status_t               ret = SDF_FAILURE;
    SDF_cache_ctnr_metadata_t *pmeta;
    local_SDF_CONTAINER        lc;
    SDF_cguid_t                cguid;
    uint64_t                   num_curr_flash_objects;
    uint64_t                   bytes_curr_flash_objects;
    SDF_action_state_t        *pas;
    fthWaitEl_t               *wait;

    pas = pai->pcs;

    lc = getLocalContainer( &lc, container );
    if (lc == NULL) {
        return(SDF_FAILURE);
    }

    cguid = lc->cguid;

    pmeta = get_container_metadata(pai, cguid);
    if (pmeta == NULL) {
        plat_log_msg(21098, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "Failed to get container metadata");

        // plat_abort();
        return(SDF_GET_METADATA_FAILED);
    }

    /*  Make sure that the temporary sums don't get scrambled by
     *  concurrent calls to this function.
     */
    wait = fthLock(&(pas->stats_lock), 1, NULL);

    /*  Sum stats across threads
     */
    sum_sched_stats(pas);

    *pstat = 0; /* default */

    switch (stat_name) {
        case SDF_N_CURR_ITEMS:
            ret = SDFContainerStatInternal(pai, container, FLASH_NUM_OBJECTS, &num_curr_flash_objects);
            if (ret == SDF_SUCCESS) {
                /* NOTE: n_only_in_cache should be zero in wt mode! */
                *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].n_only_in_cache + num_curr_flash_objects;
            }
            break;
        case SDF_N_TOTAL_ITEMS:
            *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].all_objects_ever_created;
            *pstat += (pmeta->all_objects_at_restart);
            break;
        case SDF_BYTES_CURR_ITEMS:
            ret = SDFContainerStatInternal(pai, container, FLASH_SPACE_USED, &bytes_curr_flash_objects);
            if (ret == SDF_SUCCESS) {
                /* NOTE: pmeta->bytes_only_in_cache should be zero in wt mode! */
                *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].bytes_only_in_cache + bytes_curr_flash_objects;
            }
            break;
        case SDF_BYTES_TOTAL_ITEMS:
            *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].all_bytes_ever_created;
            break;
        case SDF_N_ONLY_IN_CACHE:
            *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].n_only_in_cache;
            ret = SDF_SUCCESS;
            break;
        case SDF_N_TOTAL_IN_CACHE:
            *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].n_total_in_cache;
            ret = SDF_SUCCESS;
            break;
        case SDF_BYTES_ONLY_IN_CACHE:
            *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].bytes_only_in_cache;
            ret = SDF_SUCCESS;
            break;
        case SDF_BYTES_TOTAL_IN_CACHE:
            *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].bytes_total_in_cache;
            ret = SDF_SUCCESS;
            break;
        case SDF_N_OVERWRITES:
            *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].n_overwrites;
            ret = SDF_SUCCESS;
            break;
        case SDF_N_IN_PLACE_OVERWRITES:
            *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].n_in_place_overwrites;
            ret = SDF_SUCCESS;
            break;
        case SDF_N_NEW_ENTRY:
            *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].n_new_entry;
            ret = SDF_SUCCESS;
            break;
        case SDF_N_WRITETHRUS:
            *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].n_writethrus;
            ret = SDF_SUCCESS;
            break;
        case SDF_N_WRITEBACKS:
            *pstat = pas->stats_per_ctnr.ctnr_stats[pmeta->n].n_writebacks;
            ret = SDF_SUCCESS;
            break;
    }
    fthUnlock(wait);
    return(ret);
}

SDF_status_t SDFPreloadContainerMetaData(SDF_action_init_t *pai, SDF_cguid_t cguid)
{
    SDF_status_t           ret = SDF_SUCCESS;
    int                    i;
    SDF_container_meta_t   meta;
    struct shard          *pshard = NULL;
    flashDev_t            *flash_dev;
    SDF_shardid_t          shard;
    SDF_action_state_t    *pas;
    fthWaitEl_t           *wait;

    pas = pai->pcs;
    wait = fthLock(&(pas->ctnr_preload_lock), 1, NULL);

    for (i=0; i<SDF_MAX_CONTAINERS; i++) {
        if (!pas->ctnr_meta[i].valid) {
            break;
        }
    }

    if (i >= SDF_MAX_CONTAINERS) {
        plat_log_msg(21107, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "Exceeded max number of containers (%d).", SDF_MAX_CONTAINERS);
        // plat_abort();
        ret = SDF_TOO_MANY_CONTAINERS;
    }

    if ((ret == SDF_SUCCESS) &&
        (name_service_get_meta((SDF_internal_ctxt_t *) pai, 
                              cguid, &meta) == SDF_SUCCESS))
    {
        pas->ctnr_meta[i].n                        = i;
        pas->ctnr_meta[i].cguid                    = cguid;
        pas->ctnr_meta[i].meta                     = meta;

        /* get the pshard pointer once and for all */

        shard = meta.shard;

        #ifdef MULTIPLE_FLASH_DEV_ENABLED
             flash_dev = get_flashdev_from_shardid(pas->flash_dev, shard, pas->flash_dev_count);
        #else
             flash_dev = pas->flash_dev;
        #endif

        /*
         * FIXME: There may be a race condition here between container
         * deletion and in-flight operations.  We probably want some sort
         * of reference tracking on whatever replaced the shard interface.
         *
         * Jim's flashPut, etc. functions should take a shard id and handle
         * the lookup to avoid this.
         */
            
        if (flash_dev == NULL) {
            plat_log_msg(21108, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                         "Could not find flash_dev for cguid %llu"
                         " shardid %lu", (unsigned long long) cguid, shard);
            // plat_abort();
            ret = SDF_GET_METADATA_FAILED;
        } else {
            pshard = shardFind(flash_dev, shard);
            if (!pshard) {
                plat_log_msg(21109, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                             "flash returned no pshard for cguid %llu"
                             " shardid %lu",
                             (unsigned long long) cguid, shard);
                // plat_abort();
                ret = SDF_GET_METADATA_FAILED;
            }
        }

        if (ret == SDF_SUCCESS) {
            pas->ctnr_meta[i].pshard = pshard;

            /*  Load the total items to handle the case of a restart
             *  in which the flash already holds some objects.
             */
            pas->ctnr_meta[i].all_objects_at_restart = flashStats(pshard, FLASH_NUM_OBJECTS);

            pas->ctnr_meta[i].lock_container =
                replicator_key_lock_container_alloc(pas->mynode, shard,
                                                    VIP_GROUP_ID_INVALID);

#ifdef INCLUDE_TRACE_CODE
                    plat_log_msg(21110, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_TRACE,
                    "===========  name_service_get_meta(cguid=%"PRIu64"): meta.flush_time=%d meta.flush_set_time=%d ===========", cguid, meta.flush_time, meta.flush_set_time);
#endif
        }

    } else {
        plat_log_msg(21111, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "name_service_get_meta failed!");
        // plat_abort();
        ret = SDF_GET_METADATA_FAILED;
    }

    if (ret == SDF_SUCCESS) {
        (pas->n_containers)++;

        asm __volatile__("mfence":::"memory"); // Make sure all is seen
        pas->ctnr_meta[i].valid = 1;

        clear_all_sched_ctnr_stats(pas, i);
    }

    fthUnlock(wait);
    return(ret);
}

SDF_status_t SDFUnPreloadContainerMetaData(SDF_action_init_t *pai, SDF_cguid_t cguid)
{
    SDF_status_t                ret = SDF_SUCCESS;
    int                         i;
    SDF_action_state_t         *pas;
    SDF_cache_ctnr_metadata_t  *pctnr_md;
    fthWaitEl_t                *wait;

    pas  = pai->pcs;
    wait = fthLock(&(pas->ctnr_preload_lock), 1, NULL);

    pas = pai->pcs;

    if (pas->n_containers == 0) {
        ret = SDF_UNPRELOAD_CONTAINER_FAILED;
    } else {
        pctnr_md = NULL;
        for (i=0; i<SDF_MAX_CONTAINERS; i++) {
            if ((pas->ctnr_meta[i].valid) && 
                (pas->ctnr_meta[i].cguid == cguid)) 
            {
                pctnr_md = &(pas->ctnr_meta[i]);
                break;
            }
        }

        if ((pctnr_md == NULL) || (!pctnr_md->valid)) {
            ret = SDF_UNPRELOAD_CONTAINER_FAILED;
        } else {
            clear_all_sched_ctnr_stats(pas, i);
            if (pctnr_md->lock_container) {
                rklc_free(pctnr_md->lock_container);
                pctnr_md->lock_container = NULL;
            }
            asm __volatile__("mfence":::"memory"); // Make sure all is seen
            pctnr_md->valid = 0;
        }
    }

    if (ret == SDF_SUCCESS) {
        (pas->n_containers)--;
    }

    fthUnlock(wait);
    return(ret);
}

void SDFFreeAppBuffer(void *pbuf)
{
    #ifdef MALLOC_TRACE
        UTMallocTrace("memcached: pbuf_in", SDF_FALSE, SDF_TRUE, SDF_FALSE, pbuf, 0);
    #endif // MALLOC_TRACE

    plat_free(pbuf);  /* placeholder for now */

#ifdef notdef
    SDF_trans_state_t *ptrans;

    ptrans = ; /* stash ptrans in buffer somewhere */
    free_app_buf(ptrans->pts->pappbufstate, pbuf);
    return(SDF_SUCCESS);
#endif
}

static SDF_trans_state_t *get_req_trans_state(SDF_action_init_t *pai, SDF_appreq_t *par)
{
    SDF_trans_state_t   *ptrans;

    ptrans = get_trans_state((SDF_action_thrd_state_t *) pai->pts);

    ptrans->pas              = pai->pcs;
    ptrans->meta             = NULL;
    ptrans->pts              = (SDF_action_thrd_state_t *) pai->pts;
    ptrans->par              = par;
    ptrans->pai              = pai;
    ptrans->bypass           = SDF_App_Request_Info[par->reqtype].bypass_cache;

    /* initialize these here to detect bugs later */
    ptrans->pts->pobj           = NULL;
    ptrans->pts->obj_size       = 0;

    /* always get a tag, even if I don't send any messages */
    ptrans->tag = get_tag(ptrans->pts);

    return(ptrans);
}

void SDFStartSerializeContainerOp(SDF_internal_ctxt_t *pai_in)
{
    SDF_action_init_t         *pai;
    SDF_action_thrd_state_t   *pts;

    pai = (SDF_action_init_t *) pai_in;
    pts = (SDF_action_thrd_state_t *) pai->pts;
    pts->container_serialization_wait = fthLock(&(pai->pcs->container_serialization_lock), 1, NULL);
}

void SDFEndSerializeContainerOp(SDF_internal_ctxt_t *pai_in)
{
    SDF_action_init_t         *pai;
    SDF_action_thrd_state_t   *pts;

    pai = (SDF_action_init_t *) pai_in;
    pts = (SDF_action_thrd_state_t *) pai->pts;

    fthUnlock(pts->container_serialization_wait);
}

SDF_context_t ActionGetContext(SDF_action_thrd_state_t *pts)
{
    SDF_context_t  context;
    fthWaitEl_t   *wait;

    wait = fthLock(&(pts->phs->context_lock), 1, NULL);
    context = pts->phs->n_context;
    (pts->phs->n_context)++;
    fthUnlock(wait);
    if (pts->phs->n_context >= SDF_RESERVED_CONTEXTS) {
        plat_log_msg(21112, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                     "Used up all SDF_RESERVED_CONTEXTS!");
    }
    return(context);
}

void ActionProtocolAgentNew(SDF_action_init_t *pai, SDF_appreq_t *par)
{
    SDF_trans_state_t   *ptrans;

    ptrans = get_req_trans_state(pai, par);

    (ptrans->pts->n_underway)++;

    #ifdef INCLUDE_TRACE_CODE
        plat_log_msg(30557, 
                     PLAT_LOG_CAT_SDF_PROT, 
                     PLAT_LOG_LEVEL_TRACE,
                     "[tag %d, node %d, pid %d, thrd %d] request: %"FFDC_LONG_STRING(100)"", 
                     ptrans->tag, ptrans->pts->mynode, plat_getpid(), 
                     ptrans->pts->thrdnum, 
                     SDFsPrintAppReq(ptrans->pts->smsg, ptrans->par, SDF_MSGBUF_SIZE, SDF_FALSE));
    #endif

    if (ptrans->bypass) {
        process_bypass_operation(ptrans);
    } else {
        if (ptrans->par->reqtype == APRIV) {
            /*  These are caused by replication operations from remote nodes.
             *  To avoid deadlock, don't wait for the lock.  If the lock is
             *  held, put an invalidation request on the remote request queue
             *  for this slab so the next access to the slab will invalidate 
             *  the object.
             */
            if (!process_object_operation(ptrans, SDF_TRUE /* try_lock */)) {
                SDFNewCachePostRequest(ptrans->pts->new_actiondir,
                                 APRIV,
                                 ptrans->par->ctnr,
                                 &(ptrans->par->key),
                                 ptrans->par->ctnr_type,
                                 NULL);
            }
        } else if (ptrans->par->reqtype == APRUP) {
            /*  These are caused by replication operations from remote nodes.
             *  To avoid deadlock, don't wait for the lock.  If the lock is
             *  held, put an update request on the remote request queue
             *  for this slab so the next access to the slab will update
             *  the object.
             */
            if (!process_object_operation(ptrans, SDF_TRUE /* try_lock */)) {

                /*  To simplify memory allocation and freeing, 
                 *  make a copy of the protocol message.
                 */

                SDF_protocol_msg_t  *pm;
                SDF_protocol_msg_t  *pm_new;
                pm = (SDF_protocol_msg_t *) ((char *) (ptrans->par->pbuf_out) - sizeof(SDF_protocol_msg_t));
                pm_new = plat_malloc(sizeof(SDF_protocol_msg_t) + pm->data_size);
                if (pm_new == NULL) {
                    plat_log_msg(30561, 
                                 PLAT_LOG_CAT_SDF_PROT, 
                                 PLAT_LOG_LEVEL_FATAL,
                                 "malloc failed!");
                    plat_assert_always(0);
                }
                memcpy(pm_new, pm, sizeof(SDF_protocol_msg_t) + pm->data_size);

                /*  Copy over the skew-adjusted create and expiry times.
                 *  (The times in the message structure are NOT skew-
                 *  adjusted).
                 */
                pm_new->createtime = ptrans->par->curtime;
                pm_new->exptime    = ptrans->par->exptime;

                SDFNewCachePostRequest(ptrans->pts->new_actiondir,
                                 APRUP,
                                 ptrans->par->ctnr,
                                 &(ptrans->par->key),
                                 ptrans->par->ctnr_type,
                                 pm_new);
            }
        } else {
            (void) process_object_operation(ptrans, SDF_FALSE /* try_lock */);
        }
    }
    if (ptrans->meta != NULL) {
        (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].sdf_status_counts[ptrans->par->respStatus])++;
    }

    finish_up(ptrans);

    (ptrans->pts->n_underway)--;
}

static void finish_up(SDF_trans_state_t *ptrans)
{
    #ifdef INCLUDE_TRACE_CODE
            plat_log_msg(30558, 
                         PLAT_LOG_CAT_SDF_PROT, 
                         PLAT_LOG_LEVEL_TRACE,
                         "[tag %d, node %d, pid %d, thrd %d] final status: %s (%d)", 
                         ptrans->tag, ptrans->pts->mynode, plat_getpid(), 
                         ptrans->pts->thrdnum, 
                         SDF_Status_Strings[ptrans->par->respStatus],
                         ptrans->par->respStatus);
    #endif
    release_tag(ptrans->pts);
    free_trans_state(ptrans);
}

static void load_return_values(SDF_trans_state_t *ptrans)
{
    void        *pbuf;
    SDF_size_t   dsize;
    uint32_t     fmt;

    if (ptrans->par->respStatus == SDF_SUCCESS) {

        /* load return values */

        fmt = SDF_App_Request_Info[ptrans->par->reqtype].format;

        if (fmt & a_asiz) { // pactual_size
            /* return the actual size of the object */
            ptrans->par->destLen = ptrans->entry->obj_size;
        }
        if (fmt & a_pexp) { // pexptme
            /* return the last expiry time that was set for this object */
            ptrans->par->exptime = ptrans->entry->exptime;
        }
        if (fmt & a_pbfi) { // pbuf_in
            /* application provides a data buffer and a max size */
            if (ptrans->par->pbuf_in == NULL) {
                ptrans->par->respStatus = SDF_BAD_PBUF_POINTER;
            } else {
                if (ptrans->par->max_size < ptrans->entry->obj_size) {
                    dsize = ptrans->par->max_size;
                } else {
                    dsize = ptrans->entry->obj_size;
                }
                SDFNewCacheCopyOutofObject(ptrans->pts->new_actiondir, ptrans->par->pbuf_in, ptrans->entry, dsize);
            }
        }
        if (fmt & a_ppbf) { // ppbf_in
            /* SDF allocates a buffer and returns a pointer to it */
            if (ptrans->par->ppbuf_in == NULL) {
                ptrans->par->respStatus = SDF_BAD_PBUF_POINTER;
            } else {
                pbuf = get_app_buf(ptrans->pts->pappbufstate, ptrans->entry->obj_size);
                #ifdef MALLOC_TRACE
                    UTMallocTrace("pbuf_in", SDF_FALSE, SDF_FALSE, SDF_FALSE, (void *) pbuf, ptrans->entry->obj_size);
                #endif // MALLOC_TRACE

                if (pbuf == NULL) {
                    plat_log_msg(21106, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                                 "Not enough memory, plat_alloc() failed.");
                    ptrans->par->respStatus = SDF_FAILURE_MEMORY_ALLOC;
                    *(ptrans->par->ppbuf_in) = NULL;
                } else {
                    SDFNewCacheCopyOutofObject(ptrans->pts->new_actiondir, pbuf, ptrans->entry, ptrans->entry->obj_size);
                    *(ptrans->par->ppbuf_in) = pbuf;
                }
            }
        }
    }
}

static void process_bypass_operation(SDF_trans_state_t *ptrans)
{
    switch (ptrans->par->reqtype) {
        case APSYC:
            sync_container(ptrans);
            break;
        case APICD:
            flush_all(ptrans);
            break;
        case APGIT:
            get_flush_time(ptrans);
            break;
        case APFCO:
            flush_container(ptrans);
            break;
        case APFCI:
            flush_inval_container(ptrans);
            break;
        case APICO:
            inval_container(ptrans);
            break;
        default:
            ptrans->par->respStatus = SDF_UNSUPPORTED_REQUEST;
            plat_log_msg(21115, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "Unsupported command: %s (%s)!", 
                         SDF_App_Request_Info[ptrans->par->reqtype].name,
                         SDF_App_Request_Info[ptrans->par->reqtype].shortname
                         );
            break;
    }
}

/* returns -1 if cguid is unknown */
int get_container_index(SDF_action_init_t *pai, SDF_cguid_t cguid)
{
    int                         i;
    SDF_action_state_t         *pas;

    pas = pai->pcs;

    if (pas->n_containers == 0) {
        return(-1);
    }

    for (i=0; i<SDF_MAX_CONTAINERS; i++) {
        if ((pas->ctnr_meta[i].valid) && 
            (pas->ctnr_meta[i].cguid == cguid)) 
        {
            return(i);
        }
    }
    return(-1);
}

SDF_container_meta_t *sdf_get_preloaded_ctnr_meta(SDF_action_state_t *pas, SDF_cguid_t cguid)
{
    int                         i;
    SDF_cache_ctnr_metadata_t  *pctnr_md;
    SDF_container_meta_t       *pmeta;

    pmeta    = NULL;
    pctnr_md = NULL;
    for (i=0; i<SDF_MAX_CONTAINERS; i++) {
        if ((pas->ctnr_meta[i].valid) && 
            (pas->ctnr_meta[i].cguid == cguid)) 
        {
            pctnr_md = &(pas->ctnr_meta[i]);
            break;
        }
    }

    if (pctnr_md != NULL) {
        pmeta = &(pctnr_md->meta);
    }

    return(pmeta);
}

static SDF_cache_ctnr_metadata_t *get_preloaded_cache_ctnr_meta(SDF_action_state_t *pas, SDF_cguid_t cguid)
{
    int                         i;
    SDF_cache_ctnr_metadata_t  *pctnr_md;

    pctnr_md = NULL;
    for (i=0; i<SDF_MAX_CONTAINERS; i++) {
        if ((pas->ctnr_meta[i].valid) && 
            (pas->ctnr_meta[i].cguid == cguid)) 
        {
            pctnr_md = &(pas->ctnr_meta[i]);
            break;
        }
    }

    return(pctnr_md);
}

SDF_cache_ctnr_metadata_t *get_container_metadata(SDF_action_init_t *pai, SDF_cguid_t cguid)
{
    int                         i;
    SDF_action_state_t         *pas;
    SDF_cache_ctnr_metadata_t  *pctnr_md;

    pas = pai->pcs;

    if (pas->n_containers == 0) {
        /* don't forget to preload the cmc container metadata */
        if (SDFPreloadContainerMetaData(pai, CMC_CGUID) != SDF_SUCCESS) {
            return(NULL);
        }
    }

    pctnr_md = NULL;
    for (i=0; i<SDF_MAX_CONTAINERS; i++) {
        if ((pas->ctnr_meta[i].valid) && 
            (pas->ctnr_meta[i].cguid == cguid)) 
        {
            pctnr_md = &(pas->ctnr_meta[i]);
            break;
        }
    }

    if (pctnr_md == NULL) {
        if (SDFPreloadContainerMetaData(pai, cguid) == SDF_SUCCESS) {
            for (i=0; i<SDF_MAX_CONTAINERS; i++) {
                if ((pas->ctnr_meta[i].valid) && 
                    (pas->ctnr_meta[i].cguid == cguid)) 
                {
                    pctnr_md = &(pas->ctnr_meta[i]);
                    break;
                }
            }
        }
    }

    /* xxxzzz add a check to be sure container type is
       consistent with the request type */

    return(pctnr_md);
}

static SDF_boolean_t process_object_operation(SDF_trans_state_t *ptrans, SDF_boolean_t try_flag)
{
    if (!(SDF_App_Request_Info[ptrans->par->reqtype].format & a_curt)) {
        ptrans->par->curtime = 0; // force curtime to a known out-of-band value
    }
    ptrans->par->respStatus     = SDF_SUCCESS; /* default status */
    ptrans->inval_object        = SDF_FALSE;

    /* make sure that object sizes are in range */
    if (SDF_App_Request_Info[ptrans->par->reqtype].format & a_size) {
        if (ptrans->par->sze > ptrans->pas->max_obj_size) {
            ptrans->par->respStatus = SDF_OBJECT_TOO_BIG;
            return(SDF_FALSE);
        }
    }

    /* Get container properties. */

    ptrans->meta = get_container_metadata(ptrans->pai, ptrans->par->ctnr);
    if (ptrans->meta == NULL) {
        plat_log_msg(21098, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "Failed to get container metadata");

        // plat_abort();
        ptrans->par->respStatus = SDF_GET_METADATA_FAILED;
        return(SDF_FALSE);
    }
    (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].appreq_counts[ptrans->par->reqtype])++;

    /* get directory entry */

    get_directory_entry(ptrans, 
                        SDF_TRUE,  /* lock flag */
                        try_flag);

    if (ptrans->entry == NULL) {
        if (try_flag) {
            return(SDF_FALSE);
        } else {
            plat_log_msg(21116, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "Failed to get a directory entry");
            ptrans->par->respStatus = SDF_GET_DIRENTRY_FAILED;
            return(SDF_TRUE);
        }
    }
    /*  Once get_directory_entry succeeds, the slab is locked.
     *  Don't forget to unlock it, especially if an error
     *  condition is encountered!
     */

    #ifdef CHECK_HEAP
        SDFNewCacheCheckHeap(ptrans->pts->new_actiondir, ptrans->pbucket);
    #endif

    ptrans->expired_or_flushed = check_expiry_cache(ptrans);

    /* Look up the protocol table */

    ptrans->pte = lookup_protocol_table(ptrans);

    /* Follow the protocol action */
    (ptrans->pte->fn_state[ptrans->entry->state].action_fn)(ptrans);

    ptrans->old_state = ptrans->entry->state;
    if (ptrans->par->respStatus == SDF_SUCCESS) {
        /* this operation succeeded, so update cache state */
        ptrans->entry->state = ptrans->pte->fn_state[ptrans->entry->state].new_state;
    } else {
        /* don't forget to restore exptime and createtime! */
        ptrans->entry->exptime    = ptrans->old_exptime;
        ptrans->entry->createtime = ptrans->old_createtime;
    }
    if (ptrans->inval_object) {
        ptrans->entry->state = CS_I;
    }

    /* if this directory entry is invalid, remove it */

    if (ptrans->entry->state == CS_I) {
        /* cache entry has no valid contents, so free it */
        ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].n_total_in_cache--;
        ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].bytes_total_in_cache -= ptrans->entry->obj_size;
        SDFNewCacheRemove(ptrans->pts->new_actiondir, ptrans->entry, SDF_FALSE /* wrbk_flag */, NULL /* wrbk_arg */);
    } else {
        SDFNewCacheTransientEntryCheck(ptrans->pbucket, ptrans->entry);
    }

    /*  Adjust the response status if the original object
     *  was expired or flushed.
     */

    if (ptrans->expired_or_flushed) {
        ptrans->par->respStatus = SDF_EXPIRED;
    }
    if (ptrans->par->respStatus == SDF_SUCCESS) {
        /* this operation succeeded, so load return values */
        load_return_values(ptrans);
    }

    #ifdef CHECK_HEAP
        SDFNewCacheCheckHeap(ptrans->pts->new_actiondir, ptrans->pbucket);
    #endif

    /*  Unlock the slab 
     *  I must do this here because load_return_values accesses the
     *  directory entry, so it must remain protected by the slab lock.
     */

    SDFNewUnlockBucket(ptrans->pts->new_actiondir, ptrans->pbucket);
    return(SDF_TRUE);
}

static void get_directory_entry(SDF_trans_state_t *ptrans, SDF_boolean_t lock_flag, SDF_boolean_t try_flag)
{
    SDFNewCacheEntry_t         *entry;

    #ifdef CHECK_HEAP
        #ifdef HEAP_STRESS_TEST
            if (ptrans->par->ctnr != CMC_CGUID) {
                heap_stress_test(ptrans);
                plat_abort();
            }
        #endif
    #endif

    entry = SDFNewCacheGetCreate(ptrans->pts->new_actiondir,
                                 ptrans->par->ctnr,
                                 &(ptrans->par->key),
                                 ptrans->par->ctnr_type,
                                 ptrans->par->curtime,
                                 lock_flag, /* lock_bucket */
                                 &ptrans->pbucket,
                                 try_flag, /* just try the lock */
                                 &(ptrans->new_entry),
                                 (void *) ptrans);

    if (entry == NULL) {
        if (try_flag) {
            ptrans->par->respStatus = SDF_GET_DIRENTRY_FAILED;
        } else {
            ptrans->par->respStatus = SDF_FAILURE_MEMORY_ALLOC;
            /* Don't forget to unlock the bucket! */
            SDFNewUnlockBucket(ptrans->pts->new_actiondir, ptrans->pbucket);
        }
        ptrans->entry = NULL;
        return;
    }

    ptrans->entry = entry;
    if (ptrans->entry->state == CS_I) {
        /* this is a new directory entry */
        ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].n_total_in_cache++;
    }
}

static SDF_boolean_t check_expiry_cache(SDF_trans_state_t *ptrans)
{
    SDF_boolean_t  expired_or_flushed;

    ptrans->old_exptime    = ptrans->entry->exptime;
    ptrans->old_createtime = ptrans->entry->createtime;

    if (SDF_App_Request_Info[ptrans->par->reqtype].set_expiry) {
        ptrans->entry->exptime    = ptrans->par->exptime;
        ptrans->entry->createtime = ptrans->par->curtime;
    }

    /* check memcached expiry and flush times */

    expired_or_flushed = SDF_FALSE;

    if ((SDF_App_Request_Info[ptrans->par->reqtype].check_expiry) &&
        (ptrans->par->curtime != 0))
    {
        if ((ptrans->old_exptime > 0) && 
            (ptrans->old_exptime <= ptrans->par->curtime)) 
        {
            expired_or_flushed = SDF_TRUE;
        } else if ((ptrans->meta->meta.flush_time > 0) &&
                   (ptrans->old_createtime <= ptrans->meta->meta.flush_time) &&
                   (ptrans->meta->meta.flush_time <= ptrans->par->curtime))
        {
           expired_or_flushed = SDF_TRUE;
        }
    }
    return(expired_or_flushed);
}

static SDF_boolean_t check_expiry_flash(SDF_trans_state_t *ptrans)
{
    SDF_boolean_t  expired_or_flushed;

    expired_or_flushed = SDF_FALSE;

    if ((SDF_App_Request_Info[ptrans->par->reqtype].check_expiry) &&
        (ptrans->par->curtime != 0))
    {
        if ((ptrans->metaData.expTime > 0) &&
            (ptrans->metaData.expTime <= ptrans->par->curtime))
        {
           expired_or_flushed = SDF_TRUE;
        } else if ((ptrans->meta->meta.flush_time > 0) &&
                   (ptrans->metaData.createTime <= ptrans->meta->meta.flush_time) &&
                   (ptrans->meta->meta.flush_time <= ptrans->par->curtime))
        {
           expired_or_flushed = SDF_TRUE;
        }
    }
    return(expired_or_flushed);
}

static PROT_TABLE *lookup_protocol_table(SDF_trans_state_t *ptrans)
{
    PROT_TABLE *pte;

    if (ptrans->meta->meta.properties.cache.writethru) {
        if (ptrans->expired_or_flushed) {
            pte = &WTExpiredTable[ptrans->par->reqtype];
        } else {
            pte = &WTProtocolTable[ptrans->par->reqtype];
        }
    } else {
        if (ptrans->expired_or_flushed) {
            pte = &WBExpiredTable[ptrans->par->reqtype];
        } else {
            pte = &WBProtocolTable[ptrans->par->reqtype];
        }
    }

    return(pte);
}

static SDF_status_t flush_stuff(SDFNewCache_t *cache,
                        SDF_cguid_t cguid,
                        SDF_action_init_t *pai,
                        SDF_boolean_t inval_flag,
                        SDF_boolean_t ctnr_only)
{
    SDF_status_t           success, ret_status;
    SDF_simple_key_t       key;
    SDF_container_type_t   ctype;

    ret_status = SDF_SUCCESS;

    /*
    ** We don't yet support flush based on node ID so just set this parm to 
    ** an illegal value - this causes the enumeration functions to ignore
    ** node id in matches
    */
    if (ctnr_only) {
        SDFNewCacheStartEnumeration(cache, cguid);
    } else {
        SDFNewCacheStartEnumeration(cache, 0);
    }

    while (SDFNewCacheNextEnumeration(cache, &cguid, &key, &ctype)) {

        if (ctype == SDF_OBJECT_CONTAINER) {

            if (inval_flag) {
                success = SDF_I_FlushInvalObject(pai, cguid, key.key);
            } else {
                success = SDF_I_FlushObject(pai, cguid, key.key);
            }

        } else {
            plat_log_msg(21117, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                         "Invalid container type in flush_stuff: %d", ctype);
            plat_abort();
        }
        if (success != SDF_SUCCESS) {
           ret_status = success;
        }
    }
    return(ret_status);
}

static SDF_status_t inval_stuff(SDFNewCache_t *cache,
                        SDF_cguid_t cguid,
                        SDF_action_init_t *pai,
                        SDF_boolean_t ctnr_only)
{
    SDF_status_t           success, ret_status;
    SDF_simple_key_t       key;
    SDF_container_type_t   ctype;

    ret_status = SDF_SUCCESS;

    /*
    ** We don't yet support flush based on node ID so just set this parm to 
    ** an illegal value - this causes the enumeration functions to ignore
    ** node id in matches
    */
    if (ctnr_only) {
        SDFNewCacheStartEnumeration(cache, cguid);
    } else {
        SDFNewCacheStartEnumeration(cache, 0);
    }

    while (SDFNewCacheNextEnumeration(cache, &cguid, &key, &ctype)) {

        if (ctype == SDF_OBJECT_CONTAINER) {
            success = SDF_I_InvalObject(pai, cguid, key.key);
        } else {
            plat_log_msg(21118, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                         "Invalid container type in inval_stuff: %d", ctype);
            plat_abort();
        }
        if (success != SDF_SUCCESS) {
           ret_status = success;
        }
    }
    return(ret_status);
}

/* these functions are not currently used */
#ifdef notdef
static uint64_t count_cache_entries(SDFNewCache_t *cache,
                                    SDF_cguid_t cguid,
                                    SDF_action_init_t *pai,
                                    SDF_boolean_t ctnr_only)
{
    uint64_t               n;
    SDF_simple_key_t       key;
    SDF_container_type_t   ctype;

    if (ctnr_only) {
        SDFNewCacheStartEnumeration(cache, cguid);
    } else {
        SDFNewCacheStartEnumeration(cache, 0);
    }

    for (n = 0; SDFNewCacheNextEnumeration(cache, &cguid, &key, &ctype); ) {
        n++;
    }

    return(n);
}
#endif

static void init_free_home_flash_map_entries(SDF_action_thrd_state_t *pts)
{
    int                     i;
    SDF_home_flash_entry_t *phfe;

    // Initialize the array of free shard map entries
    pts->free_shard_map_entries = NULL;
    phfe = plat_alloc(SDF_MAX_CONTAINERS*sizeof(SDF_home_flash_entry_t));
    for (i=0; i<SDF_MAX_CONTAINERS; i++) {
        phfe[i].next = pts->free_shard_map_entries;
        pts->free_shard_map_entries = &(phfe[i]);
    }
}
    
SDF_status_t SDFActionStartContainer(SDF_action_init_t *pai, SDF_container_meta_t *pmeta)
{
    int                       ret;
    SDF_status_t              status = SDF_SUCCESS;
    fthWaitEl_t              *wait;
    SDF_action_thrd_state_t  *pts;
    SDF_action_state_t       *pas;
    SDF_container_meta_t     *pmeta_cached;

    pas = pai->pcs;

    // update stopflag in preloaded metadata cache
    pmeta_cached = sdf_get_preloaded_ctnr_meta(pas, pmeta->cguid);
    if (pmeta_cached == NULL) {
        status = SDF_START_SHARD_MAP_ENTRY_FAILED;
        plat_log_msg(21119, 
                     PLAT_LOG_CAT_SDF_PROT, 
                     PLAT_LOG_LEVEL_ERROR,
                     "sdf_get_preloaded_ctnr_meta failed for cguid %"PRIu64"!",
                     pmeta->cguid);
    } else {
        pmeta_cached->stopflag = SDF_FALSE;

        /* Go through all action threads and start this container.
         */

        wait = fthLock(&(pas->nthrds_lock), 1, NULL);
        for (pts = pas->threadstates; pts != NULL; pts = pts->next) {
            ret = start_home_shard_map_entry(pts, pmeta->shard);
            if (ret != 0) {
                status = SDF_START_SHARD_MAP_ENTRY_FAILED;
            }
        }
        fthUnlock(wait);
    }

    return(SDF_SUCCESS);
}

SDF_status_t SDFActionChangeContainerWritebackMode(SDF_action_init_t *pai, SDF_cguid_t cguid, SDF_boolean_t enable_writeback)
{
    SDF_status_t              status = SDF_SUCCESS;
    SDF_action_state_t       *pas;
    SDF_container_meta_t     *pmeta_cached;
    const char               *writeback_enabled_string;

    pas = pai->pcs;

    // update writethru flag in preloaded metadata cache
    pmeta_cached = sdf_get_preloaded_ctnr_meta(pas, cguid);
    if (pmeta_cached == NULL) {
        status = SDF_STOP_SHARD_MAP_ENTRY_FAILED;
        plat_log_msg(21119, 
                     PLAT_LOG_CAT_SDF_PROT, 
                     PLAT_LOG_LEVEL_ERROR,
                     "sdf_get_preloaded_ctnr_meta failed for cguid %"PRIu64"!",
                     cguid);
    } else {
        writeback_enabled_string = getProperty_String("SDF_WRITEBACK_CACHE_SUPPORT", "On");
        if (enable_writeback) {
            if (strcmp(writeback_enabled_string, "On") != 0) {
                plat_log_msg(30562, 
                             PLAT_LOG_CAT_SDF_PROT, 
                             PLAT_LOG_LEVEL_ERROR,
                             "Cannot enable writeback caching for container %"PRIu64" because writeback caching is disabled.",
                             cguid);
                pmeta_cached->properties.cache.writethru = SDF_TRUE;
            } else {
                pmeta_cached->properties.cache.writethru = SDF_FALSE;
            }
        } else {
            pmeta_cached->properties.cache.writethru = SDF_TRUE;
        }
    }

    return(status);  
}

SDF_status_t SDFActionStopContainer(SDF_action_init_t *pai, SDF_container_meta_t *pmeta)
{
    int                       ret;
    SDF_status_t              status = SDF_SUCCESS;
    fthWaitEl_t     *wait;
    SDF_action_thrd_state_t  *pts;
    SDF_action_state_t       *pas;
    SDF_container_meta_t     *pmeta_cached;

    pas = pai->pcs;

    /* make sure all outstanding operations to this container are completed! */
    drain_store_pipe((SDF_action_thrd_state_t *) pai->pts);

    // update stopflag in preloaded metadata cache
    pmeta_cached = sdf_get_preloaded_ctnr_meta(pas, pmeta->cguid);
    if (pmeta_cached == NULL) {
        status = SDF_STOP_SHARD_MAP_ENTRY_FAILED;
        plat_log_msg(21119, 
                     PLAT_LOG_CAT_SDF_PROT, 
                     PLAT_LOG_LEVEL_ERROR,
                     "sdf_get_preloaded_ctnr_meta failed for cguid %"PRIu64"!",
                     pmeta->cguid);
    } else {
        pmeta_cached->stopflag = SDF_TRUE;

        /* Go through all action threads and stop this container.
         */

        wait = fthLock(&(pas->nthrds_lock), 1, NULL);
        for (pts = pas->threadstates; pts != NULL; pts = pts->next) {
            ret = stop_home_shard_map_entry(pts, pmeta->shard);
            if (ret != 0) {
                status = SDF_STOP_SHARD_MAP_ENTRY_FAILED;
            }
        }
        fthUnlock(wait);
    }

    return(status);  
} 

SDF_status_t SDFActionDeleteContainer(SDF_action_init_t *pai, SDF_container_meta_t *pmeta)
{
    int                       ret;
    SDF_status_t              status = SDF_SUCCESS;
    fthWaitEl_t     *wait;
    SDF_action_thrd_state_t  *pts;
    SDF_action_state_t       *pas;

    pas = pai->pcs;

    status = SDFUnPreloadContainerMetaData(pai, pmeta->cguid);
    if (status != SDF_SUCCESS) {
        plat_log_msg(21120, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
                     "SDFUnPreloadContainerMetaData failed!");
        // plat_abort();
    }

    /* Go through all action threads and delete this container from the .
     * per-thread shard maps.
     */

    wait = fthLock(&(pas->nthrds_lock), 1, NULL);
    for (pts = pas->threadstates; pts != NULL; pts = pts->next) {
        ret = delete_home_shard_map_entry(pts, pmeta->shard);
        if (ret != 0) {
            status = SDF_DELETE_SHARD_MAP_ENTRY_FAILED;
        }
    }
    fthUnlock(wait);

    if (status != SDF_SUCCESS) {
        // xxxzzz container will be left in a weird state!
        plat_log_msg(21121, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
                     "delete_home_shard_map_entry failed!");
        // plat_abort();
    }

    return(status);
} 

SDF_status_t SDFActionCreateContainer(SDF_action_init_t *pai, SDF_container_meta_t *pmeta)
{
    /* purposefully empty */

    return(SDF_SUCCESS);
}

SDF_status_t SDFActionOpenContainer(SDF_action_init_t *pai, SDF_cguid_t cguid)
{
    int                         ret;
    SDF_status_t                status = SDF_SUCCESS;
    fthWaitEl_t                *wait;
    SDF_action_thrd_state_t    *pts;
    SDF_action_state_t         *pas;
    SDF_cache_ctnr_metadata_t  *pctnr_md;

    pas = pai->pcs;

    status = SDFPreloadContainerMetaData(pai, cguid);

    if (status == SDF_SUCCESS) {
        pctnr_md = get_preloaded_cache_ctnr_meta(pas, cguid);
        if (pctnr_md == NULL) {
            status = SDF_CREATE_SHARD_MAP_ENTRY_FAILED;
        } else {

            /* Go through all action threads and create this container.
             */

            wait = fthLock(&(pas->nthrds_lock), 1, NULL);
            for (pts = pas->threadstates; pts != NULL; pts = pts->next) {
                ret = create_home_shard_map_entry(pts, pctnr_md->meta.shard, pctnr_md->pshard, pctnr_md->meta.stopflag);
                if (ret != 0) {
                    status = SDF_CREATE_SHARD_MAP_ENTRY_FAILED;
                    // xxxzzz container will be left in a weird state!
                }
            }
            fthUnlock(wait);
        }
    }

    return(status);
} 

void InitActionAgentPerThreadState(SDF_action_state_t *pcs, SDF_action_thrd_state_t *pts, SDF_action_init_t *pai)
{
    SDF_appBufProps_t         appbuf_props;
    fthWaitEl_t              *wait;
    SDF_action_thrd_state_t  *pts_check;

    pts->thrdnum          = __sync_fetch_and_add(&pcs->nthrds, 1);

    wait = fthLock(&(pcs->nthrds_lock), 1, NULL);
    for (pts_check = pcs->threadstates; pts_check != NULL; pts_check = pts_check->next) {
        if (pts == pts_check) {
            break;
        }
    }
    if (pts_check == NULL) {
        // make sure only one entry in list per pts
        pts->next             = pcs->threadstates;
        pcs->threadstates     = pts;
    }
    fthUnlock(wait);

    pts->mynode           = pcs->mynode;
    pts->nnodes           = pcs->nnodes;
    pts->phs              = pcs;
    pts->new_actiondir    = pcs->new_actiondir;
    // fthMboxInit(&(pts->ackmbx));
    // fthMboxInit(&(pts->respmbx));
    // pts->fthmbx.actlvl    = SACK_RESP_ONLY_FTH; /* just release on sends, don't need send acks */
    pts->fthmbx.abox      = &(pts->ackmbx);
    pts->fthmbx.rbox      = &(pts->respmbx);
    // action_appreqlex_init(&pts->lexer_state_appreq);
    // action_appreqset_extra ((void *) &(pts->lex_buf_appreq), pts->lexer_state_appreq);

    // action_homemsglex_init(&pts->lexer_state_homemsg);
    // action_homemsgset_extra ((void *) &(pts->lex_buf_homemsg), pts->lexer_state_homemsg);

    // SDFTLMapInit(&(pts->appreq_lex_cache),  100, NULL);
    // SDFTLMapInit(&(pts->homemsg_lex_cache), 100, NULL);

    pts->curtag  = 0;
    pts->max_tag = (1 << TAGNUM_BITS);
    pts->n_trans_in_flight = 0;

    appbuf_props.dummy = 0; /* placeholder */
    if (init_app_buf_pool(&(pts->pappbufstate), &appbuf_props)) {
       plat_log_msg(21122, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                    "init_app_buf_pool failed!");
       plat_abort();
    }

    #ifdef ENABLE_MULTIPLE_FLASH_SUBSYSTEMS
        pts->pai = &(pts->ai_struct);
        pts->pai->paio_ctxt = ssdaio_init_ctxt();
        pts->pai->pts = pts;
        pts->pai->pcs = pcs;
    #endif

    pts->free_trans_states         = NULL;
    pts->total_trans_state_structs = 0;
    pts->used_trans_state_structs  = 0;
    pts->n_underway                = 0;

    pts->pobj                      = NULL;
    pts->obj_size                  = 0;

    fthMboxInit(&(pts->req_resp_fthmbx));
    pts->req_mbx.actlvl = SACK_RESP_ONLY_FTH;
    pts->req_mbx.release_on_send = 1;
    pts->req_mbx.abox  = NULL;
    pts->req_mbx.rbox = &pts->req_resp_fthmbx;

    SDFTLMap2Init(&(pts->shardmap), SHARDMAP_N_BUCKETS, NULL);
    fthLockInit(&pts->shardmap_lock);

    fthMboxInit(&(pts->async_put_ack_mbox));

    init_free_home_flash_map_entries(pts);
}

static void destroy_per_thread_state(SDF_action_thrd_state_t *pts)
{
    SDF_trans_state_t  *ptrans;
    SDF_trans_state_t  *ptrans_next;

    destroy_app_buf_pool(pts->pappbufstate);

    for (ptrans = pts->free_trans_states; ptrans != NULL; ptrans = ptrans_next) {
        ptrans_next = ptrans->next;
        plat_free(ptrans);
    }
}

static void release_tag(SDF_action_thrd_state_t *pts)
{
    if (pts->n_trans_in_flight <= 0) {
        plat_log_msg(21123, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                     "Inconsistency in number of requests in flight!");
        plat_abort();
    }
    (pts->n_trans_in_flight)--;
}

static SDF_tag_t get_tag(SDF_action_thrd_state_t *pts)
{
    SDF_tag_t   tag;
    SDF_vnode_t              mynode;

    mynode = pts->mynode;
    tag   = 0;
    tag  |= (mynode<<(THRDNUM_BITS + TAGNUM_BITS));
    tag  |= ((pts->thrdnum)<<TAGNUM_BITS);
    tag  |= (pts->curtag);

    #ifdef DEBUG_THIS
        fprintf(stderr, "XXXZZZ in get_tag: mynode=%d, thrdnum=%d, curtag=%d, maxtag=%d, tag=%d\n", pts->mynode, pts->thrdnum, pts->curtag, pts->max_tag, tag);
    #endif

    (pts->n_trans_in_flight)++;
    if (pts->n_trans_in_flight >= pts->max_tag) {
        plat_log_msg(21124, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                     "Too many requests in flight (%d)!", pts->max_tag);
        plat_abort();
    }

    (pts->curtag)++;
    pts->curtag %= pts->max_tag;

    return(tag);
}

static int sdfcc_print_fn(SDFNewCacheEntry_t *psce, char *sout, int max_len)
{
    int                     len;

    *sout++ = ' ';
    if (psce != NULL) {
        len = sprint_cache_state(sout, psce, max_len);
    } else {
        len = strlen(sout);
    }
    return(len+1);
}

static int sprint_cache_state(char *s, SDFNewCacheEntry_t *psce, int max_len)
{
    int                     len;

    if (max_len < 3) {
       s[0] = '\0';
       return(0);
    }
    s[0] = CacheStateName[psce->state][0];
    s[1] = '\0';
    len = 2;
    return(len);
}


static void clear_all_sched_ctnr_stats(SDF_action_state_t *pas, int ctnr_index)
{
    memset((void *) pas->stats_new_per_sched, 0, totalScheds*sizeof(SDF_action_stats_new_t));
}

static void init_stats(SDF_action_stats_new_t *ps)
{
    memset((void *) ps, 0, sizeof(SDF_action_stats_new_t));
}

static int UsedAppReqs[] = {
    APCOE, APCOP, APPAE, APPTA, APSOE, APSOB, APGRX, APGRD, APDBE, APDOB,
    APFLS, APFLI, APINV, APRIV, APSYC, APICD, APGIT, APFCO, APFCI, APICO,
    APRUP, APDUM,
};

static int UsedMsgs[] = {

    AHCOB, AHCOP, AHCWD, 
    AHDOB, AHFLD, AHGTR, 
    AHGTW, AHPTA, AHSOB, 
    AHSOP, 

    HACRC, HACRF, HACSC, 
    HACSF, HADEC, HADEF, 
    HAFLC, HAFLF, HAGRC, 
    HAGRF, HAGWC, HAGWF,
    HAPAC, HAPAF, HASTC, 
    HASTF, 

    HFXST,  FHXST, FHNXS,
    HFGFF,  FHDAT, FHGTF,
    HFPTF,  FHPTC, FHPTF,
    HFDFF,  FHDEC, FHDEF,
    HFCIF,  FHCRC, FHCRF,
    HFCZF,  FHCRC, FHCRF,
    HFSET,  FHSTC, FHSTF,
    HFCSH,  FHCSC, FHCSF,
    HFSSH,  FHSSC, FHSSF,
    HFDSH,  FHDSC, FHDSF,
    HFGLS,  FHGLC, FHGLF,
    HFGIC,  FHGIC, FHGIF,
    HFGBC,  FHGCC, FHGCF,
    HFGSN,  FHGSC, FHGSF,
    HFSRR,  FHSRC, FHSRF,
    HFSPR,  FHSPC, FHSPF,
    HFFLA,  FHFLC, FHFLF,
    HFRVG,  FHRVC, FHRVF,
    HFNOP,  FHNPC, FHNPF,
    HFOSH,  FHOSC, FHOSF,
    ZDUMY,
};

static int UsedFlashCodes[] = {
    FLASH_EOK,      FLASH_EPERM,    FLASH_ENOENT, FLASH_EDATASIZE, FLASH_ESTOPPED,
    FLASH_EBADCTNR, FLASH_EDELFAIL, FLASH_EAGAIN, FLASH_ENOMEM,    FLASH_EACCES,
    FLASH_EINCONS,  FLASH_EBUSY,    FLASH_EEXIST, FLASH_EINVAL,    FLASH_EMFILE,
    FLASH_ENOSPC,   FLASH_ENOBUFS,  FLASH_ESTALE, FLASH_EDQUOT,    FLASH_RMT_EDELFAIL, 
    FLASH_RMT_EBADCTNR,
    FLASH_N_ERR_CODES,
};

static void sum_sched_stats(SDF_action_state_t *pas)
{
    int                       nsched;
    int                       i, j, n;
    int                       used_ctnr[SDF_MAX_CONTAINERS+1];

    init_stats(&(pas->stats_new));
    init_stats(&(pas->stats_per_ctnr));

    // determine which containers are used

    n = 0;
    for (i=0; i<SDF_MAX_CONTAINERS; i++) {
        if (pas->ctnr_meta[i].valid) {
            used_ctnr[n] = i;
            n++;
        }
    }
    used_ctnr[n] = -1;

    for (nsched = 0; nsched < totalScheds; nsched++) {
        for (i=0; used_ctnr[i] != -1; i++) {
            for (j=0; UsedAppReqs[j] != APDUM; j++) {
                pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].appreq_counts[UsedAppReqs[j]] += pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].appreq_counts[UsedAppReqs[j]];
                pas->stats_new.ctnr_stats[0].appreq_counts[UsedAppReqs[j]] += pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].appreq_counts[UsedAppReqs[j]];
            }

            for (j=0; UsedMsgs[j] != ZDUMY; j++) {
                pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].msg_out_counts[UsedMsgs[j]] += pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].msg_out_counts[UsedMsgs[j]];
                pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].msg_in_counts[UsedMsgs[j]] += pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].msg_in_counts[UsedMsgs[j]];
                pas->stats_new.ctnr_stats[0].msg_out_counts[UsedMsgs[j]] += pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].msg_out_counts[UsedMsgs[j]];
                pas->stats_new.ctnr_stats[0].msg_in_counts[UsedMsgs[j]] += pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].msg_in_counts[UsedMsgs[j]];
            }

            for (j=0; j<N_SDF_STATUS_STRINGS; j++) {
                pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].sdf_status_counts[j] += pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].sdf_status_counts[j];
                pas->stats_new.ctnr_stats[0].sdf_status_counts[j] += pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].sdf_status_counts[j];
            }

            for (j=0; UsedFlashCodes[j] != FLASH_N_ERR_CODES; j++) {
                pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].flash_retcode_counts[UsedFlashCodes[j]] += pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].flash_retcode_counts[UsedFlashCodes[j]];
                pas->stats_new.ctnr_stats[0].flash_retcode_counts[UsedFlashCodes[j]] += pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].flash_retcode_counts[UsedFlashCodes[j]];
            }

            pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].n_only_in_cache += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_only_in_cache;
            pas->stats_new.ctnr_stats[0].n_only_in_cache += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_only_in_cache;

            pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].n_total_in_cache += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_total_in_cache;
            pas->stats_new.ctnr_stats[0].n_total_in_cache += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_total_in_cache;

            pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].bytes_only_in_cache += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].bytes_only_in_cache;
            pas->stats_new.ctnr_stats[0].bytes_only_in_cache += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].bytes_only_in_cache;

            pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].bytes_total_in_cache += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].bytes_total_in_cache;
            pas->stats_new.ctnr_stats[0].bytes_total_in_cache += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].bytes_total_in_cache;

            pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].all_objects_ever_created += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].all_objects_ever_created;
            pas->stats_new.ctnr_stats[0].all_objects_ever_created += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].all_objects_ever_created;

            pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].all_bytes_ever_created += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].all_bytes_ever_created;
            pas->stats_new.ctnr_stats[0].all_bytes_ever_created += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].all_bytes_ever_created;

            pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].n_overwrites += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_overwrites;
            pas->stats_new.ctnr_stats[0].n_overwrites += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_overwrites;

            pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].n_in_place_overwrites += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_in_place_overwrites;
            pas->stats_new.ctnr_stats[0].n_in_place_overwrites += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_in_place_overwrites;

            pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].n_new_entry += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_new_entry;
            pas->stats_new.ctnr_stats[0].n_new_entry += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_new_entry;

            pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].n_writethrus += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_writethrus;
            pas->stats_new.ctnr_stats[0].n_writethrus += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_writethrus;

            pas->stats_per_ctnr.ctnr_stats[used_ctnr[i]].n_writebacks += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_writebacks;
            pas->stats_new.ctnr_stats[0].n_writebacks += 
                pas->stats_new_per_sched[nsched].ctnr_stats[used_ctnr[i]].n_writebacks;
        }
    }
}

static int cache_stats(SDF_action_state_t *pas, char *str, int size)
{
    int                        i = 0;
    int                        j = 0;
    SDFNewCacheStats_t            cachestats;

    SDFNewCacheGetStats(pas->new_actiondir, &cachestats);

    sum_sched_stats(pas);

    // plat_assert(size > 255);
    if (size <= 255) {
        return(i);
    }

    i += snprintf(str+i, size-i, "<CACHE> [");
    i += snprintf(str+i, size-i, "policy=LRU, ");
    i += snprintf(str+i, size-i, "hashBuckets=%"PRIu64", nSlabs=%"PRIu64", numElements=%"PRIu64", maxSz=%"PRIu64", currSz=%"PRIu64", currSzWkeys=%"PRIu64"",
                 pas->nbuckets, pas->nslabs, cachestats.num_objects, pas->cachesize, 
                 cachestats.cursize, cachestats.cursize_w_keys);

    for (j=0; j<N_SDF_APP_REQS; j++) {
        if (pas->stats_new.ctnr_stats[0].appreq_counts[j] > 0) {
            i += snprintf(str+i, size-i, ", %s=%"PRIu64, SDF_App_Request_Info[j].shortname, pas->stats_new.ctnr_stats[0].appreq_counts[j]);
        }
    }
    for (j=0; j<N_SDF_PROTOCOL_MSGS; j++) {
        if (pas->stats_new.ctnr_stats[0].msg_out_counts[j] > 0) {
            i += snprintf(str+i, size-i, ", %s=%"PRIu64, SDF_Protocol_Msg_Info[j].shortname, pas->stats_new.ctnr_stats[0].msg_out_counts[j]);
        }
    }
    for (j=0; j<N_SDF_PROTOCOL_MSGS; j++) {
        if (pas->stats_new.ctnr_stats[0].msg_in_counts[j] > 0) {
            i += snprintf(str+i, size-i, ", %s=%"PRIu64, SDF_Protocol_Msg_Info[j].shortname, pas->stats_new.ctnr_stats[0].msg_in_counts[j]);
        }
    }
    for (j=0; j<N_SDF_STATUS_STRINGS; j++) {
        if (pas->stats_new.ctnr_stats[0].sdf_status_counts[j] > 0) {
            i += snprintf(str+i, size-i, ", %s=%"PRIu64, SDF_Status_Strings[j], pas->stats_new.ctnr_stats[0].sdf_status_counts[j]);
        }
    }
    for (j=0; j<FLASH_N_ERR_CODES; j++) {
        if (pas->stats_new.ctnr_stats[0].flash_retcode_counts[j] > 0) {
            i += snprintf(str+i, size-i, ", %s=%"PRIu64, flashRetCodeName(j), pas->stats_new.ctnr_stats[0].flash_retcode_counts[j]);
        }
    }

    i += snprintf(str+i, size-i, "]");
    // plat_assert(i < size); xxxzzz FIX ME
    return(i);
}

static int cache_stats_cguid(SDF_internal_ctxt_t *pac, SDF_action_state_t *pas, char *str, int size, SDF_cguid_t cguid)
{
    int                        i = 0;
    int                        j = 0;
    SDFNewCacheStats_t         cachestats;
    int                        index;
    SDF_action_init_t         *pai;

    pai = (SDF_action_init_t *) pac;

    SDFNewCacheGetStats(pas->new_actiondir, &cachestats);

    sum_sched_stats(pas);

    // plat_assert(size > 255);
    if (size <= 255) {
        return(i);
    }

    index = get_container_index(pai, cguid);
    if (index == -1) {
        return(i);
    }

    i += snprintf(str+i, size-i, "<CACHE-PER-CTNR> [");

    for (j=0; j<N_SDF_APP_REQS; j++) {
        if (pas->stats_per_ctnr.ctnr_stats[index].appreq_counts[j] > 0) {
            i += snprintf(str+i, size-i, ", %s=%"PRIu64, SDF_App_Request_Info[j].shortname, pas->stats_per_ctnr.ctnr_stats[index].appreq_counts[j]);
        }
    }
    for (j=0; j<N_SDF_PROTOCOL_MSGS; j++) {
        if (pas->stats_per_ctnr.ctnr_stats[index].msg_out_counts[j] > 0) {
            i += snprintf(str+i, size-i, ", %s=%"PRIu64, SDF_Protocol_Msg_Info[j].shortname, pas->stats_per_ctnr.ctnr_stats[index].msg_out_counts[j]);
        }
    }
    for (j=0; j<N_SDF_PROTOCOL_MSGS; j++) {
        if (pas->stats_per_ctnr.ctnr_stats[index].msg_in_counts[j] > 0) {
            i += snprintf(str+i, size-i, ", %s=%"PRIu64, SDF_Protocol_Msg_Info[j].shortname, pas->stats_per_ctnr.ctnr_stats[index].msg_in_counts[j]);
        }
    }
    for (j=0; j<N_SDF_STATUS_STRINGS; j++) {
        if (pas->stats_per_ctnr.ctnr_stats[index].sdf_status_counts[j] > 0) {
            i += snprintf(str+i, size-i, ", %s=%"PRIu64, SDF_Status_Strings[j], pas->stats_per_ctnr.ctnr_stats[index].sdf_status_counts[j]);
        }
    }
    for (j=0; j<FLASH_N_ERR_CODES; j++) {
        if (pas->stats_per_ctnr.ctnr_stats[index].flash_retcode_counts[j] > 0) {
            i += snprintf(str+i, size-i, ", %s=%"PRIu64, flashRetCodeName(j), pas->stats_per_ctnr.ctnr_stats[index].flash_retcode_counts[j]);
        }
    }
    i += snprintf(str+i, size-i, ", %s=%"PRIu64, "overwrites", pas->stats_per_ctnr.ctnr_stats[index].n_overwrites);
    i += snprintf(str+i, size-i, ", %s=%"PRIu64, "inplaceowr", pas->stats_per_ctnr.ctnr_stats[index].n_in_place_overwrites);
    i += snprintf(str+i, size-i, ", %s=%"PRIu64, "new_entries", pas->stats_per_ctnr.ctnr_stats[index].n_new_entry);
    i += snprintf(str+i, size-i, ", %s=%"PRIu64, "writethrus", pas->stats_per_ctnr.ctnr_stats[index].n_writethrus);
    i += snprintf(str+i, size-i, ", %s=%"PRIu64, "writebacks", pas->stats_per_ctnr.ctnr_stats[index].n_writebacks);

    i += snprintf(str+i, size-i, "]\n");
    // plat_assert(i < size); xxxzzz FIX ME
    return(i);
}

/**
 * @author: drew
 */
#define easy_snprintf(buf, bufmaxsize, len_used, status, args...) { \
        if (!status) {  \
                int remain = (bufmaxsize) - (len_used); \
                int want = snprintf((buf) + (len_used), remain, args); \
                if (want < remain) { \
                        (len_used) += want; \
                } else { \
                        (status) = ENOMEM; \
                } \
        } \
}

void action_stats(SDF_internal_ctxt_t *pac, char *str, int size) 
{
    int                        len = 0;
    SDF_action_state_t        *pas;
    SDF_action_thrd_state_t   *pts;
    SDF_action_init_t         *pai = NULL;
    fthWaitEl_t               *wait;

    pai = (SDF_action_init_t *) pac;
    pas = pai->pcs;
    pts = (SDF_action_thrd_state_t *) pai->pts;

    /*  Make sure concurrent calls don't scramble the stats! */
    wait = fthLock(&(pas->stats_lock), 1, NULL);
    
    len += HashMap_stats(meta_map, "meta_map", str+len, size-len);
    len += HashMap_stats(cguid_map, "cguid_map", str+len, size-len);
    (void) cache_stats(pas, str+len, size-len);

    fthUnlock(wait);
}

void action_stats_new_cguid(SDF_internal_ctxt_t *pac, char *str, int size, SDF_cguid_t cguid) 
{
    int                        len = 0;
    SDF_action_state_t        *pas;
    SDF_action_thrd_state_t   *pts;
    SDF_action_init_t         *pai = NULL;
    fthWaitEl_t               *wait;
    int                        status;
    struct SDF_shared_state   *state = &sdf_shared_state;

    pai = (SDF_action_init_t *) pac;
    pas = pai->pcs;
    pts = (SDF_action_thrd_state_t *) pai->pts;

    /*  Make sure concurrent calls don't scramble the stats! */
    wait = fthLock(&(pas->stats_lock), 1, NULL);
    
    len = cache_stats_cguid(pac, pas, str+len, size-len, cguid);

    // ----------- replication stats ---------------
    status = 0;
    #define item(lower) \
        easy_snprintf(str, size, len, status,                      \
                      "STAT replication_" #lower " %llu\r\n",                    \
                      (unsigned long long) stats.lower);
        if (state->config.replicator) {
            struct sdf_replicator_shard_stats stats;
            if (sdf_replicator_get_container_stats(state->config.replicator,
                                                   &stats, cguid) == 
                SDF_SUCCESS) 
            {
                SDF_REPLICATOR_STAT_ITEMS()
            }
        }
    #undef item

    fthUnlock(wait);
}

static void init_state_fn(SDFNewCacheEntry_t *pce, SDF_time_t curtime)
{
    /* this is a newly created entry, so initialize it */
    pce->createtime     = curtime;
    pce->exptime        = 0; /* default */
    pce->state          = CS_I; /* invalid*/
}

static void update_state_fn(SDFNewCacheEntry_t *pce, SDF_time_t curtime, SDF_time_t exptime, SDF_boolean_t new_entry_flag)
{
    pce->createtime     = curtime;
    pce->exptime        = exptime;
    pce->state          = CS_M; /* modified */
}

static SDF_boolean_t is_dirty_fn(SDFNewCacheEntry_t *pce)
{
    if (pce->state == CS_M) {
       return(SDF_TRUE);
    } else {
       return(SDF_FALSE);
    }
}

static void dump_flash_trace(SDF_trans_state_t *ptrans, char *name, int retcode)
{
    plat_log_msg(21125, 
                 PLAT_LOG_CAT_SDF_PROT, 
                 PLAT_LOG_LEVEL_TRACE,
                 "[tag %d, node %d, pid %d, thrd %d] %s (ret=%s[%d]): pshard: %p, key:%s, keyLen:%d, t_exp:%d, t_create:%d, pdata:%p, dataLen:%d, sequence:%"PRIu64"", 
                 ptrans->tag, ptrans->pts->mynode, plat_getpid(), 
                 ptrans->pts->thrdnum, name, flashRetCodeName(retcode), retcode,
                 ptrans->meta->pshard,
                 ptrans->pflash_key, ptrans->metaData.keyLen,
                 ptrans->metaData.expTime, ptrans->metaData.createTime,
                 ptrans->pflash_data, ptrans->metaData.dataLen,
                 ptrans->metaData.sequence);
}

SDF_status_t get_status(int retcode)
{
    SDF_status_t   status;

    switch (retcode) {    
        case FLASH_EOK:       status = SDF_SUCCESS;               break;
        case FLASH_EPERM:     status = SDF_FLASH_EPERM;           break;
        case FLASH_ENOENT:    status = SDF_OBJECT_UNKNOWN;        break;
        case FLASH_EDATASIZE: status = SDF_FLASH_EDATASIZE;       break;
        case FLASH_EAGAIN:    status = SDF_FLASH_EAGAIN;          break;
        case FLASH_ENOMEM:    status = SDF_FLASH_ENOMEM;          break;
        case FLASH_EBUSY:     status = SDF_FLASH_EBUSY;           break;
        case FLASH_EEXIST:    status = SDF_OBJECT_EXISTS;         break;
        case FLASH_EACCES:    status = SDF_FLASH_EACCES;          break;
        case FLASH_EINVAL:    status = SDF_FLASH_EINVAL;          break;
        case FLASH_EMFILE:    status = SDF_FLASH_EMFILE;          break;
        case FLASH_ENOSPC:    status = SDF_FLASH_ENOSPC;          break;
        case FLASH_ESTALE:    status = SDF_FLASH_STALE_CURSOR;    break;
        case FLASH_ENOBUFS:   status = SDF_FLASH_ENOBUFS;         break;
        case FLASH_EDQUOT:    status = SDF_FLASH_EDQUOT;          break;
        case FLASH_ESTOPPED:  status = SDF_STOPPED_CONTAINER;     break;
        case FLASH_EBADCTNR:  status = SDF_CONTAINER_UNKNOWN;     break;
        case FLASH_RMT_EBADCTNR:  status = SDF_RMT_CONTAINER_UNKNOWN;     break;
        case FLASH_EDELFAIL:  status = SDF_FLASH_EDELFAIL;        break;
        case FLASH_RMT_EDELFAIL:  status = SDF_FLASH_RMT_EDELFAIL;        break;
        case FLASH_EINCONS:   status = SDF_FLASH_EINCONS;         break;
        default:              status = SDF_FAILURE_STORAGE_WRITE; break;
    }
    return(status);
}

int get_retcode(SDF_status_t status)
{
    int ret;

    switch (status) {    
        case SDF_SUCCESS:           ret = FLASH_EOK;       break;       
        case SDF_FLASH_EPERM:       ret = FLASH_EPERM;     break;     
        case SDF_OBJECT_UNKNOWN:    ret = FLASH_ENOENT;    break;    
        case SDF_FLASH_EDATASIZE:   ret = FLASH_EDATASIZE; break; 
        case SDF_FLASH_EAGAIN:      ret = FLASH_EAGAIN;    break;    
        case SDF_FLASH_ENOMEM:      ret = FLASH_ENOMEM;    break;    
        case SDF_FLASH_EBUSY:       ret = FLASH_EBUSY;     break;     
        case SDF_OBJECT_EXISTS:     ret = FLASH_EEXIST;    break;    
        case SDF_FLASH_EACCES:      ret = FLASH_EACCES;    break;    
        case SDF_FLASH_EINVAL:      ret = FLASH_EINVAL;    break;    
        case SDF_FLASH_EMFILE:      ret = FLASH_EMFILE;    break;    
        case SDF_FLASH_ENOSPC:      ret = FLASH_ENOSPC;    break;    
        case SDF_FLASH_ENOBUFS:     ret = FLASH_ENOBUFS;   break;   
        case SDF_FLASH_EDQUOT:      ret = FLASH_EDQUOT;    break;    
        case SDF_STOPPED_CONTAINER: ret = FLASH_ESTOPPED;  break;    
        case SDF_CONTAINER_UNKNOWN: ret = FLASH_EBADCTNR;  break;    
        case SDF_RMT_CONTAINER_UNKNOWN: ret = FLASH_RMT_EBADCTNR;  break;    
        case SDF_FLASH_EDELFAIL:    ret = FLASH_EDELFAIL;  break;    
        case SDF_FLASH_RMT_EDELFAIL:    ret = FLASH_RMT_EDELFAIL;  break;    
        case SDF_FLASH_EINCONS:     ret = FLASH_EINCONS;   break;    
        default:                    ret = FLASH_EINVAL;    break;
    }
    return(ret);
}

#ifdef MALLOC_FAIL_TEST
static void alloc_test(void **pp)
{
    if ((random() % 100) < MALLOC_FAIL_PROB) {
        if (*pp != NULL) {
            plat_free(*pp);
            *pp = NULL;
        }
    }
}

#endif // MALLOC_FAIL_TEST

static void wrbk_fn(SDFNewCacheEntry_t *pce, void *wrbk_arg)
{
    uint32_t                 h;
    SDF_trans_state_t       *ptrans;
    SDF_async_put_request_t  rqst;
    SDF_simple_key_t         simple_key;

    ptrans = (SDF_trans_state_t *) wrbk_arg;

    rqst.rtype         = ASYNC_WRITEBACK;
    rqst.skip_for_wrbk = SDF_FALSE;
    rqst.pas           = ptrans->pas;
    rqst.tag           = ptrans->tag;
    rqst.pai           = ptrans->pts->pai;
    rqst.ctnr          = ptrans->par->ctnr;
    rqst.n_ctnr        = ptrans->meta->n;
    rqst.actiondir     = ptrans->pts->new_actiondir;
    rqst.ctnr_type     = ptrans->par->ctnr_type;
    rqst.ctxt          = ptrans->par->ctxt;
    rqst.shard         = ptrans->meta->meta.shard;
    rqst.pshard        = ptrans->meta->pshard;

    rqst.flash_meta.keyLen     = pce->key_len;
    rqst.flash_meta.dataLen    = pce->obj_size;
    rqst.flash_meta.expTime    = pce->exptime;
    rqst.flash_meta.createTime = pce->createtime;

    rqst.pbucket      = pce->pbucket;

    SDFNewCacheCopyKeyOutofObject(ptrans->pas->new_actiondir, &simple_key, pce);
    rqst.pkey_simple  = &simple_key;
    rqst.pce          = pce;
    rqst.pdata        = NULL;
    rqst.flash_flags  = FLASH_PUT_NO_TEST; // do a flash set

    rqst.ack_mbx      = &(ptrans->pts->async_put_ack_mbox);
    rqst.req_mbx      = NULL;
    rqst.req_resp_mbx = NULL;
    rqst.pmeta        = &(ptrans->meta->meta);

    /*  The slab must remain locked until the asynchronous put
     *  completes.  This function increments the count of threads
     *  holding the slab lock.
     */
    SDFNewCacheAddLocker(pce->pbucket);

    #ifdef INCLUDE_TRACE_CODE
            plat_log_msg(30563, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_TRACE,
            "===========  START async wrbk (cguid=%"PRIu64",key='%s'):  ===========", ptrans->par->ctnr, simple_key.key);
    #endif

    h = (pce->syndrome) % (ptrans->pts->phs->async_puts_state->config.nthreads);
    fthMboxPost(&(ptrans->pts->phs->async_puts_state->inbound_fth_mbx[h]), (uint64_t) &rqst);

    /*  This handshake via a mailbox does two things:
     *     - Allows the async put thread to copy the key and data.
     *     - Provides flow control, so that the async put threads
     *       don't get hopelessly behind processing async put
     *       requests.
     */
    (void) fthMboxWait(rqst.ack_mbx);
}

static int flashPut_wrapper(SDF_trans_state_t *ptrans, struct shard *pshard, struct objMetaData *pmeta, char *pkey, char *pdata, int flags, SDF_boolean_t skip_for_writeback)
{
    uint32_t                 h;
    int                      ret = 0; // default is success
    SDF_action_init_t  __attribute__((unused)) *pai = ptrans->pts->pai;
    SDF_async_put_request_t  rqst;

    #ifdef RR_ITERATION_TEST
    {
        // from replication/rpc.c
        extern void rr_iterate_rpc_test(struct sdf_replicator *replicator, SDF_container_meta_t *cmeta, SDF_vnode_t mynode, struct shard * shard);
        extern struct SDF_shared_state sdf_shared_state;

        if (strcmp(pkey, "do_rr_iteration_test") == 0) {
            rr_iterate_rpc_test(sdf_shared_state.config.replicator, &(ptrans->meta->meta), ptrans->pts->phs->mynode, pshard);
        }
    }
    #endif

    if ((
         (!ptrans->pas->enable_replication) && 
         (!ptrans->meta->meta.properties.container_type.async_writes)
        ) ||
        (ptrans->par->ctnr == CMC_CGUID))
    {
        /*  Don't replicate CMC modifications, and don't do
         *  them asynchronously! 
         */

        /*  Skip the write to flash if this is a writeback container and this IS a set.
         */
        if ((ptrans->par->ctnr == CMC_CGUID) ||
            ptrans->meta->meta.properties.cache.writethru || 
            (flags != FLASH_PUT_NO_TEST) || 
            (!skip_for_writeback))
        {
            (ptrans->pas->stats_new_per_sched[curSchedNum].ctnr_stats[ptrans->meta->n].n_writethrus)++;
            (pmeta->keyLen)--; // adjust for extra NULL added by SDF
            ret = flashPut(pshard, pmeta, pkey, pdata, flags);
            (pmeta->keyLen)++; // adjust for extra NULL added by SDF
        }
    } else {

        rqst.rtype         = ASYNC_PUT;
        rqst.skip_for_wrbk = skip_for_writeback;
        rqst.pas           = ptrans->pas;
        rqst.tag           = ptrans->tag;
        rqst.pai           = ptrans->pts->pai;
        rqst.ctnr          = ptrans->par->ctnr;
        rqst.n_ctnr        = ptrans->meta->n;
        rqst.actiondir     = ptrans->pts->new_actiondir;
        rqst.ctnr_type     = ptrans->par->ctnr_type;
        rqst.ctxt          = ptrans->par->ctxt;
        rqst.shard         = ptrans->meta->meta.shard;
        rqst.pshard        = pshard;
        rqst.flash_meta    = *pmeta;
        rqst.pbucket       = ptrans->pbucket;
        rqst.pkey_simple   = &(ptrans->par->key);
        rqst.pce           = NULL;
        rqst.pdata         = pdata;
        rqst.flash_flags   = flags;
        rqst.ack_mbx       = &(ptrans->pts->async_put_ack_mbox);
        rqst.req_mbx       = &(ptrans->pts->req_mbx);
        rqst.req_resp_mbx  = &(ptrans->pts->req_resp_fthmbx);
        rqst.pmeta         = &(ptrans->meta->meta);

        if (ptrans->meta->meta.properties.container_type.async_writes) {

            /*  The slab must remain locked until the asynchronous put
             *  completes.  This function increments the count of threads
             *  holding the slab lock.
             */
            SDFNewCacheAddLocker(ptrans->pbucket);

            #ifdef INCLUDE_TRACE_CODE
                    plat_log_msg(21126, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_TRACE,
                    "===========  START async put (cguid=%"PRIu64",key='%s'):  ===========", ptrans->par->ctnr, ptrans->par->key.key);
            #endif

            h = (ptrans->entry->syndrome) % (ptrans->pts->phs->async_puts_state->config.nthreads);
            fthMboxPost(&(ptrans->pts->phs->async_puts_state->inbound_fth_mbx[h]), (uint64_t) &rqst);

            /*  This handshake via a mailbox does two things:
             *     - Allows the async put thread to copy the key and data.
             *     - Provides flow control, so that the async put threads
             *       don't get hopelessly behind processing async put
             *       requests.
             */
            (void) fthMboxWait(rqst.ack_mbx);

            ret = 0; // success by default
        } else {
            ret = do_put(&rqst, SDF_FALSE /* unlock slab */);
        }
    }
    return(ret);
}

static int delete_local_object(SDF_async_put_request_t *pap)
{
    int                ret;
    SDF_action_init_t  __attribute__((unused)) *pai = pap->pai;

    // flash_delete
    (pap->flash_meta.keyLen)--; // adjust for extra NULL added by SDF
    ret = flashPut(pap->pshard, &(pap->flash_meta), pap->pkey_simple->key, NULL, FLASH_PUT_TEST_NONEXIST);
    (pap->flash_meta.keyLen)++; // adjust for extra NULL added by SDF

    SDFNewCachePostRequest(pap->actiondir,
                     APRIV,
                     pap->ctnr,
                     pap->pkey_simple,
                     pap->ctnr_type,
                     NULL);
    return(ret);
}

/**
 * @brief Delete object on remote node only
 *
 * Used in error recovery paths.
 */
static SDF_status_t delete_remote_object(SDF_async_put_request_t *pap)
{
    SDF_status_t             error;
    SDF_size_t               msize;
    struct sdf_msg          *new_msg = NULL;
    struct sdf_msg          *send_msg = NULL;
    SDF_boolean_t            send_msg_flag = SDF_TRUE;
    service_t                to_service;
    vnode_t                  to_node = -1;
    SDF_protocol_msg_type_t  hf_mtype;
    SDF_protocol_msg_t      *pm_new = NULL;
    int                      ret = SDF_SUCCESS;
    qrep_state_t            *ps;
    SDF_status_t             status;
    struct replicator_key_lock *key_lock;
    SDF_cache_ctnr_metadata_t *cache_ctnr_meta;
        
    ps = &(pap->pas->qrep_state);
    
    send_msg_flag = SDF_FALSE;
        
    to_node = partner_replica_by_cguid(ps, pap->ctnr);
    
    if (to_node != SDF_ILLEGAL_VNODE && node_is_alive(ps, to_node)) {
        send_msg_flag = SDF_TRUE;
        to_service    = SDF_FLSH;
    } 
    
    if (send_msg_flag) {
        
        /*  Create a message and pass it to the appropriate service.
         */
        hf_mtype = HFDFF; // delete
        
        send_msg = load_msg(pap->tag,
                            pap->flash_meta.expTime,
                            pap->flash_meta.createTime,
                            pap->pas->mynode, // from
                            to_node, // to
                            hf_mtype,
                            pap->flash_meta.dataLen, 
                            0,
                            pap->ctxt, 
                            pap->ctnr,
                            0,                        // transid
                            pap->pkey_simple,
                            0,                        // flags
                            &msize, 
                            0,                        // flushtime
                            0,                        // curtime
                            pap->shard,
                            pap->flash_meta.sequence,
                            pap->pmeta);

        key_lock = NULL;

        cache_ctnr_meta = get_container_metadata(pap->pai, pap->ctnr);
        plat_assert(cache_ctnr_meta);
        plat_assert(cache_ctnr_meta->lock_container);
        status = rklc_lock_sync(cache_ctnr_meta->lock_container,
                                pap->pkey_simple, RKL_MODE_EXCLUSIVE,
                                &key_lock);
        plat_assert(status == SDF_SUCCESS);
        
        if (sdf_msg_send(send_msg,
                         msize,
                         /* to */
                         to_node,
                         to_service,
                         /* from */
                         pap->pas->mynode,
                         SDF_RESPONSES,
                         FLSH_REQUEST,
                         pap->req_mbx, NULL)) {
            
            plat_log_msg(21101, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "sdf_msg_send to replication failed");

            /*
             * FIXME: drew 2010-03-03 This is actually a fatal situation 
             * since messaging's contract is to only return errors when
             * there has been a programming error.
             */

            ret = SDF_FAILURE_MSG_SEND;
        } else {
            
            /* Wait for the response */

            new_msg = (struct sdf_msg *)fthMboxWait(pap->req_resp_mbx);
            if (new_msg == NULL) {
                ret = SDF_FAILURE_MSG_RECEIVE;
            } else {
                error = sdf_msg_get_error_status(new_msg);
                if (error != SDF_SUCCESS) {
                    sdf_dump_msg_error(error, new_msg);
                    ret = SDF_FAILURE_MSG_RECEIVE;
                } else {
                    pm_new = (SDF_protocol_msg_t *) (new_msg->msg_payload);
                    if (pm_new == NULL) {
                        ret = SDF_FAILURE_MSG_RECEIVE;
                    } else {
                        ret = pm_new->status;
                    }
                }
            }
        }

        if (key_lock) {
            rkl_unlock(key_lock);
        }
    }
    
    return ret;
}

static SDF_status_t set_remote_object_new(SDF_async_put_request_t *pap)
{
    SDF_status_t             error;
    SDF_size_t               msize;
    struct sdf_msg          *new_msg = NULL;
    struct sdf_msg          *send_msg = NULL;
    SDF_boolean_t            send_msg_flag = SDF_TRUE;
    service_t                to_service;
    vnode_t                  to_node = -1;
    SDF_protocol_msg_type_t  hf_mtype;
    SDF_protocol_msg_t      *pm_new = NULL;
    int                      ret = SDF_SUCCESS;
    qrep_state_t            *ps;
        
    ps = &(pap->pas->qrep_state);
    
    send_msg_flag = SDF_FALSE;
        
    to_node = partner_replica_by_cguid(ps, pap->ctnr);
    
    if (to_node != SDF_ILLEGAL_VNODE && node_is_alive(ps, to_node)) {
        send_msg_flag = SDF_TRUE;
        to_service    = SDF_FLSH;
    } 
    
    if (send_msg_flag) {
        
        /*  Create a message and pass it to the appropriate service.
         */
        hf_mtype = HFSET;
        
        send_msg = load_msg(pap->tag,
                            pap->flash_meta.expTime,
                            pap->flash_meta.createTime,
                            pap->pas->mynode, // from
                            to_node, // to
                            hf_mtype,
                            pap->flash_meta.dataLen, 
                            pap->pdata,
                            pap->ctxt, 
                            pap->ctnr,
                            0,                        // transid
                            pap->pkey_simple,
                            0,                        // flags
                            &msize, 
                            0,                        // flushtime
                            0,                        // curtime
                            pap->shard,
                            pap->flash_meta.sequence,
                            pap->pmeta);
        
        if (sdf_msg_send(send_msg,
                         msize,
                         /* to */
                         to_node,
                         to_service,
                         /* from */
                         pap->pas->mynode,
                         SDF_RESPONSES,
                         FLSH_REQUEST,
                         pap->req_mbx, NULL)) 
        {
            
            plat_log_msg(21101, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "sdf_msg_send to replication failed");
            
            ret = SDF_FAILURE_MSG_SEND;
        } else {
            
            /* Wait for the response */

            new_msg = (struct sdf_msg *)fthMboxWait(pap->req_resp_mbx);
            if (new_msg == NULL) {
                ret = SDF_FAILURE_MSG_RECEIVE;
            } else {
                error = sdf_msg_get_error_status(new_msg);
                if (error != SDF_SUCCESS) {
                    sdf_dump_msg_error(error, new_msg);
                    ret = SDF_FAILURE_MSG_RECEIVE;
                } else {
                    pm_new = (SDF_protocol_msg_t *) (new_msg->msg_payload);
                    if (pm_new == NULL) {
                        ret = SDF_FAILURE_MSG_RECEIVE;
                    } else {
                        ret = pm_new->status;
                    }
                }
            }
        }
    }
    
    return ret;
}

static SDF_status_t set_remote_object_old(SDF_async_put_request_t *pap)
{
    SDF_action_init_t  __attribute__((unused)) *pai = pap->pai;
    SDF_status_t             error;
    SDF_size_t               msize;
    struct sdf_msg          *new_msg = NULL;
    struct sdf_msg          *send_msg = NULL;
    SDF_boolean_t            send_msg_flag = SDF_TRUE;
    service_t                to_service;
    vnode_t                  to_node = -1;
    SDF_protocol_msg_type_t  hf_mtype;
    SDF_protocol_msg_t      *pm_new = NULL;
    SDF_status_t             ret = SDF_SUCCESS;
    int                      retflash;
    qrep_state_t            *ps;
    char                    *pdata;
    struct objMetaData       flash_meta;
        
    ps = &(pap->pas->qrep_state);
    
    send_msg_flag = SDF_FALSE;
        
    to_node = partner_replica_by_cguid(ps, pap->ctnr);
    
    if (to_node != SDF_ILLEGAL_VNODE && node_is_alive(ps, to_node)) {
        send_msg_flag = SDF_TRUE;
        to_service    = SDF_FLSH;
    } 
    
    if (send_msg_flag) {

        //  First fetch the old local value of the object.

        // flash_get

        /*  pdata must be set to NULL, otherwise
         *  flashGet will assume it is the pointer to a
         *  buffer in which to put the data!
         *  If you don't set it to NULL, you will get
         *  intermittent memory corruption problems!
         */
        pdata = NULL;

        // Don't overwrite the flash_meta for the original flashPut request!
        flash_meta = pap->flash_meta;

        (flash_meta.keyLen)--; // adjust for extra NULL added by SDF
        retflash = flashGet(pap->pshard, &flash_meta, pap->pkey_simple->key, &pdata, FLASH_GET_NO_TEST);
        (flash_meta.keyLen)++; // adjust for extra NULL added by SDF

        if (retflash != FLASH_EOK) {

            ret = get_status(retflash);

        } else {
        
            /*  Create a message and pass it to the appropriate service.
             */
            hf_mtype = HFSET;
            
            send_msg = load_msg(pap->tag,
                                flash_meta.expTime,
                                flash_meta.createTime,
                                pap->pas->mynode, // from
                                to_node, // to
                                hf_mtype,
                                flash_meta.dataLen, 
                                pdata,
                                pap->ctxt, 
                                pap->ctnr,
                                0,                        // transid
                                pap->pkey_simple,
                                0,                        // flags
                                &msize, 
                                0,                        // flushtime
                                0,                        // curtime
                                pap->shard,
                                flash_meta.sequence,
                                pap->pmeta);
            
            /* don't forget to free the buffer from flashGet */
            flashFreeBuf(pdata);

            if (sdf_msg_send(send_msg,
                             msize,
                             /* to */
                             to_node,
                             to_service,
                             /* from */
                             pap->pas->mynode,
                             SDF_RESPONSES,
                             FLSH_REQUEST,
                             pap->req_mbx, NULL)) {
                
                plat_log_msg(21101, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                             "sdf_msg_send to replication failed");
                
                ret = SDF_FAILURE_MSG_SEND;
            } else {

                /* Wait for the response */

                new_msg = (struct sdf_msg *)fthMboxWait(pap->req_resp_mbx);
                if (new_msg == NULL) {
                    ret = SDF_FAILURE_MSG_RECEIVE;
                } else {
                    error = sdf_msg_get_error_status(new_msg);
                    if (error != SDF_SUCCESS) {
                        sdf_dump_msg_error(error, new_msg);
                        ret = SDF_FAILURE_MSG_RECEIVE;
                    } else {
                        pm_new = (SDF_protocol_msg_t *) (new_msg->msg_payload);
                        if (pm_new == NULL) {
                            ret = SDF_FAILURE_MSG_RECEIVE;
                        } else {
                            ret = pm_new->status;
                        }
                    }
                }
            }
        }
    }
    
    return ret;
}

int do_put(SDF_async_put_request_t *pap, SDF_boolean_t unlock_slab)
{
    int                      ret = FLASH_EOK;
    int                      ret_cleanup = FLASH_EOK;
    SDF_action_init_t  __attribute__((unused)) *pai = pap->pai;

    struct sdf_msg          *send_msg = NULL;
    struct sdf_msg          *new_msg = NULL;
    SDF_protocol_msg_t      *pm_new = NULL;
    SDF_size_t               msize;
    SDF_protocol_msg_type_t  hf_mtype;
    SDF_boolean_t            send_msg_flag = SDF_TRUE;
    SDF_boolean_t            delflag;
    vnode_t                  to_node = -1;
    service_t                to_service;
    SDF_status_t             msg_error;
    SDF_status_t             status_cleanup;
    SDF_status_t             status;
    struct replicator_key_lock *key_lock;
    SDF_cache_ctnr_metadata_t *cache_ctnr_meta;

    if (!(pap->pas->enable_replication)) {

        /*  There is no replication (simple or otherwise),
         *  so just write to flash.
         */

        /*  Skip the write to flash if this is a writeback container and this IS a set.
         */
        if (pap->pmeta->properties.cache.writethru || 
            (pap->flash_flags != FLASH_PUT_NO_TEST) ||
            (!pap->skip_for_wrbk)) 
        {
            (pap->pas->stats_new_per_sched[curSchedNum].ctnr_stats[pap->n_ctnr].n_writethrus)++;
            (pap->flash_meta.keyLen)--; // adjust for extra NULL added by SDF
            ret = flashPut(pap->pshard, &(pap->flash_meta), pap->pkey_simple->key, pap->pdata, pap->flash_flags);
            (pap->flash_meta.keyLen)++; // adjust for extra NULL added by SDF
#ifdef INCLUDE_TRACE_CODE
                    plat_log_msg(21127, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_TRACE,
                    "===========  END async put (cguid=%"PRIu64",key='%s'):  ===========", pap->ctnr, pap->pkey_simple->key);
#endif /* def INCLUDE_TRACE_CODE */
        }

        if (unlock_slab) {
            /*  If this is an asynchronous put,
             *  unlock the slab here.
             */
            SDFNewUnlockBucket(pap->pas->new_actiondir, pap->pbucket);
        }

        return(ret);
    }

#ifdef SIMPLE_REPLICATION
    if (SDFSimpleReplication) {
        qrep_state_t      *ps;
        
        ps = &(pap->pas->qrep_state);
        
        /*  Write to the local flash, then send a message to the 
         *  replica (if it is on-line).
         */
        send_msg_flag = SDF_FALSE;
        
        to_node = partner_replica_by_cguid(ps, pap->ctnr);
        
        if ((to_node != SDF_ILLEGAL_VNODE) && 
            (node_is_alive(ps, to_node)))
        {
            send_msg_flag = SDF_TRUE;
            to_service    = SDF_FLSH;
        } else {
            /* Need to commit it to flash here if we aren't going to
             * send it over the wire.
             */

            /*  Skip the write to flash if this is a writeback container and this IS a set.
             */
            if (pap->pmeta->properties.cache.writethru || 
                (pap->flash_flags != FLASH_PUT_NO_TEST) ||
                (!pap->skip_for_wrbk)) 
            {
                (pap->pas->stats_new_per_sched[curSchedNum].ctnr_stats[pap->n_ctnr].n_writethrus)++;
                (pap->flash_meta.keyLen)--; // adjust for extra NULL added by SDF
                ret = flashPut(pap->pshard, &(pap->flash_meta), pap->pkey_simple->key, pap->pdata, pap->flash_flags);
                plat_log_msg(21128, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                             "data local store no partner: key: %s data: 0x%lx ret: %d",  pap->pkey_simple->key, (uint64_t) pap->pdata, ret);
                (pap->flash_meta.keyLen)++; // adjust for extra NULL added by SDF
            }
        }
    }
#endif /* def SIMPLE_REPLICATION */

    if (send_msg_flag) {

        /*  Create a message and pass it to the appropriate service.
         */
        switch (pap->flash_flags) {
            case FLASH_PUT_TEST_NONEXIST: 
                if (pap->pdata == NULL) {
                    hf_mtype = HFDFF; // delete
                } else {
                    hf_mtype = HFPTF; // put
                }
                break;
            case FLASH_PUT_NO_TEST: 
                hf_mtype = HFSET; // set
                break;
            case FLASH_PUT_TEST_EXIST: 
                hf_mtype = HFCIF; // create-put
                break;
            default:
                plat_log_msg(21129, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                       "Invalid value for flags");
                plat_abort();
                break;
        }

#ifdef SIMPLE_REPLICATION
        if (!SDFSimpleReplication) {
            to_node    = pap->pas->mynode;
            to_service = SDF_REPLICATION;
        }

        plat_log_msg(21130, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                     "data forwarding sending: key: %s len: %d data: 0x%lx",  pap->pkey_simple->key, pap->pkey_simple->len, (uint64_t) pap->pdata);

#else /* def SIMPLE_REPLICATION */
            to_node    = pap->pas->mynode;
            to_service = SDF_REPLICATION;
#endif /* else def SIMPLE_REPLICATION */

        send_msg = load_msg(pap->tag,
                            pap->flash_meta.expTime,
                            pap->flash_meta.createTime,
                            pap->pas->mynode, // from
                            to_node, // to
                            hf_mtype,
                            pap->flash_meta.dataLen, 
                            pap->pdata,
                            pap->ctxt, 
                            pap->ctnr,
                            0,                        // transid
                            pap->pkey_simple,
                            0,                        // flags
                            &msize, 
                            0,                        // flushtime
                            0,                        // curtime
                            pap->shard,
                            pap->flash_meta.sequence,
                            pap->pmeta);

        if (send_msg == NULL) {
            if (unlock_slab) {
                /*  If this is an asynchronous put,
                 *  unlock the slab here.
                 */
                SDFNewUnlockBucket(pap->pas->new_actiondir, pap->pbucket);
            }
            return(FLASH_EAGAIN); // xxxzzz is this the best return code?
        }

        key_lock = NULL;

        if (hf_mtype == HFDFF) {
            cache_ctnr_meta = get_container_metadata(pai, pap->ctnr);
            plat_assert(cache_ctnr_meta);
            plat_assert(cache_ctnr_meta->lock_container);
            status = rklc_lock_sync(cache_ctnr_meta->lock_container,
                                    pap->pkey_simple, RKL_MODE_EXCLUSIVE,
                                    &key_lock);
            plat_assert(status == SDF_SUCCESS);
        }

        if (sdf_msg_send(send_msg,
                         msize,
                         /* to */
                         to_node,
                         to_service,
                         /* from */
                         pap->pas->mynode,
                         SDF_RESPONSES,
                         FLSH_REQUEST,
                         pap->req_mbx, NULL)) {

            plat_log_msg(21101, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                   "sdf_msg_send to replication failed");

            /*
             * FIXME: drew 2010-03-03 This is actually a fatal situation 
             * since messaging's contract is to only return errors when
             * there has been a programming error.
             */

            if (unlock_slab) {
                /*  If this is an asynchronous put,
                 *  unlock the slab here.
                 */
                SDFNewUnlockBucket(pap->pas->new_actiondir, pap->pbucket);
            }

            if (key_lock) {
                rkl_unlock(key_lock);
            }

            return(FLASH_EAGAIN); // xxxzzz is this the best error code?
        }

#ifdef SIMPLE_REPLICATION
        /* Overlap the outgoing message and the flashPut */
        if (SDFSimpleReplication) {

            /*  Skip the write to flash if this is a writeback container and this IS a set.
             */
            if (pap->pmeta->properties.cache.writethru || 
                (pap->flash_flags != FLASH_PUT_NO_TEST) ||
                (!pap->skip_for_wrbk)) 
            {
                (pap->pas->stats_new_per_sched[curSchedNum].ctnr_stats[pap->n_ctnr].n_writethrus)++;
                (pap->flash_meta.keyLen)--; // adjust for extra NULL added by SDF
                ret = flashPut(pap->pshard, &(pap->flash_meta), pap->pkey_simple->key, pap->pdata, pap->flash_flags);
                plat_log_msg(21131, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                             "data local store partner: key: %s len: %d data: 0x%lx ret: %d",  pap->pkey_simple->key, pap->pkey_simple->len, (uint64_t) pap->pdata, ret);
                (pap->flash_meta.keyLen)++; // adjust for extra NULL added by SDF
            }
        }
#endif /* def SIMPLE_REPLICATION */

        /* Wait for the response */
        new_msg = (struct sdf_msg *)fthMboxWait(pap->req_resp_mbx);

        if (key_lock) {
            rkl_unlock(key_lock);
        }

        if (new_msg == NULL) {
            ret_cleanup = delete_local_object(pap);
            if ((ret_cleanup != FLASH_EOK) &&
                (ret_cleanup != FLASH_ENOENT))
            {
                ret = SDF_FAILURE_MSG_RECEIVE;
            }
            ret = SDF_FAILURE_MSG_RECEIVE;
        } else {

            /* Check message response */
            msg_error = sdf_msg_get_error_status(new_msg);

            /* Error returned by messaging system */
            if (msg_error != SDF_SUCCESS) {
                sdf_dump_msg_error(msg_error, new_msg);
                
                /*  If the replication message cannot be sent, 
                 *  the combined response == local response.
                 */

                if (msg_error == SDF_NODE_DEAD) {

                    /*  If the replication message cannot be sent, 
                     *  the combined response == local response.
                     */
                    // purposefully empty: ret == local ret

                /*
                 * FIXME: drew 2010-03-05  This is incorrect for non-evicting
                 * containers.  For correctness in that case the remote 
                 * replica should be failed.
                 */

                } else {
                    ret = SDF_FAILURE_MSG_RECEIVE;

                    // delete the local copy (and invalidate the cache!)
                    (void) delete_local_object(pap);

                    // delete the remote copy (and invalidate its cache!)
                    (void) delete_remote_object(pap);
                }
            /* Messaging system OK but flash may have returned an error */
            } else {
                int                 combined_ret;
                SDF_resp_action_t   combined_action;

                pm_new = (SDF_protocol_msg_t *) (new_msg->msg_payload);
                delflag = (hf_mtype == HFDFF) ? SDF_TRUE : SDF_FALSE;

                lookup_resp_tbl(pap->pmeta->properties.container_type.caching_container, 
                                pap->flash_flags, delflag,
                                ret, pm_new->status,
                                &combined_ret, &combined_action);

                ret = combined_ret;

                switch (combined_action) {
                    case RA_None:
                        break;
                    case RA_Del_Loc:
                        ret_cleanup = delete_local_object(pap);
                        if ((ret_cleanup != FLASH_EOK) && (ret_cleanup != FLASH_ENOENT)) {
                           ret = FLASH_EDELFAIL;
                        }
                        break;
                    case RA_Del_Rem:
                        status_cleanup= delete_remote_object(pap);
                        if ((status_cleanup != SDF_SUCCESS) &&
                            (status_cleanup != SDF_OBJECT_UNKNOWN))
                        {
                           ret = FLASH_RMT_EDELFAIL;
                        }
                        break;
                    case RA_Del_Both:
                        ret_cleanup = delete_local_object(pap);
                        if ((ret_cleanup != FLASH_EOK) && (ret_cleanup != FLASH_ENOENT)) {
                           ret = FLASH_EDELFAIL;
                        }
                        status_cleanup= delete_remote_object(pap);
                        if ((status_cleanup != SDF_SUCCESS) &&
                            (status_cleanup != SDF_OBJECT_UNKNOWN))
                        {
                           ret = FLASH_RMT_EDELFAIL;
                        }
                        break;
                    case RA_Set_Rem_New:
                        status_cleanup= set_remote_object_new(pap);
                        if (status_cleanup != SDF_SUCCESS) {
                           ret = FLASH_EINVAL;
                        }
                        break;
                    case RA_Set_Rem_Old:
                        status_cleanup= set_remote_object_old(pap);
                        if ((status_cleanup != SDF_SUCCESS) &&
                            (status_cleanup != SDF_OBJECT_UNKNOWN))
                        {
                           /*  SDF_OBJECT_UNKNOWN may be returned if the
                            *  flashGet of the old value fails because the
                            *  object has been evicted since the flashPut
                            *  returned EEXIST.
                            */
                           ret = FLASH_EINVAL;
                        }
                        break;
                    default:
                        plat_log_msg(21132, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
                                     "Invalid combined_action: %d (%s) for operation xxxzzz"
                                     "(cguid=%"PRIu64", key='%s')",
                                     combined_action, RespActionStrings[combined_action],
                                     pap->ctnr, pap->pkey_simple->key);
                        plat_assert(0); // xxxzzz remove this?
                        ret = FLASH_EINVAL;
                        break;
                }
            }
        }

        // garbage collect the response message
        sdf_msg_free_buff(new_msg);
    }

    #ifdef INCLUDE_TRACE_CODE
        plat_log_msg(21127, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_TRACE,
            "===========  END async put (cguid=%"PRIu64",key='%s'):  ===========", pap->ctnr, pap->pkey_simple->key);
    #endif

    if (unlock_slab) {
        /*  If this is an asynchronous put,
         *  unlock the slab here.
         */
        SDFNewUnlockBucket(pap->pas->new_actiondir, pap->pbucket);
    }

    return(ret);
}

int do_writeback(SDF_async_put_request_t *pap)
{
    int                      ret = FLASH_EOK;
    SDF_action_init_t       *pai = pap->pai;

    /*  Writebacks are NOT replicated,
     *  so just write to flash.
     */

    (pap->pas->stats_new_per_sched[curSchedNum].ctnr_stats[pap->n_ctnr].n_writebacks)++;
    (pap->flash_meta.keyLen)--; // adjust for extra NULL added by SDF
    ret = flashPut(pap->pshard, &(pap->flash_meta), pap->pkey_simple->key, pap->pdata, pap->flash_flags);
    (pap->flash_meta.keyLen)++; // adjust for extra NULL added by SDF
    #ifdef INCLUDE_TRACE_CODE
            plat_log_msg(30564, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_TRACE,
            "===========  END async writeback (cguid=%"PRIu64",key='%s'):  ===========", pap->ctnr, pap->pkey_simple->key);
    #endif

    /*  Always unlock the slab for a writeback.
     */
    SDFNewUnlockBucket(pap->pas->new_actiondir, pap->pbucket);

    return(ret);
}

    /*  Wait until all outstanding puts/writebacks have completed.
     */
static void drain_store_pipe(SDF_action_thrd_state_t *pts)
{
    int                      i;
    SDF_async_put_request_t  rqst;

    for (i=0; i<pts->phs->async_puts_state->config.nthreads; i++) {
        rqst.rtype        = ASYNC_DRAIN;
        rqst.ack_mbx      = &(pts->async_put_ack_mbox);
        fthMboxPost(&(pts->phs->async_puts_state->inbound_fth_mbx[i]), (uint64_t) &(rqst));
    }

    for (i=0; i<pts->phs->async_puts_state->config.nthreads; i++) {
        /*  Wait for async put threads to respond that they have drained.
         */
        (void) fthMboxWait(rqst.ack_mbx);
    }

    /*  At this point all writes before drain_store_pipe() was
     *  called will have been completed.
     */
}

static sdf_msg_t *load_msg(SDF_tag_t tag,
                           SDF_time_t  exp_time,
                           SDF_time_t  create_time,
                           SDF_vnode_t node_from, 
                           SDF_vnode_t node_to, 
                           SDF_protocol_msg_type_t msg_type,
                           SDF_size_t data_size, void *pdata,
                           SDF_context_t ctxt, SDF_cguid_t cguid,
                           SDF_transid_t transid, SDF_simple_key_t *pkey,
                           uint32_t flags, SDF_size_t *pmsize, 
                           SDF_time_t flushtime,
                           SDF_time_t curtime,
                           SDF_shardid_t shard,
                           uint64_t seqno,
                           SDF_container_meta_t *meta)
{
    uint64_t             fmt;
    SDF_size_t           msize;
    sdf_msg_t           *msg = NULL;
    SDF_protocol_msg_t  *pm_new = NULL;
    struct sdf_replication_op_meta *pm_op_meta;
    unsigned char       *pdata_msg;
    struct SDF_shared_state *state = &sdf_shared_state;
    SDF_status_t status = SDF_SUCCESS;

    fmt = SDF_Protocol_Msg_Info[msg_type].format;

    msize = sizeof(SDF_protocol_msg_t);
    if (fmt & m_data) {
        msize += data_size;
        msg    = (struct sdf_msg *) sdf_msg_alloc(msize);
        #ifdef MALLOC_TRACE
            UTMallocTrace("sdf_msg_alloc", SDF_FALSE, SDF_FALSE, SDF_FALSE, (void *) msg, msize);
        #endif // MALLOC_TRACE
        if (msg == NULL) {
            plat_log_msg(21133, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "Not enough memory, sdf_msg_alloc() failed.");
            return(NULL);
        }
        pm_new = (SDF_protocol_msg_t *) msg->msg_payload;
        pdata_msg = (unsigned char *) pm_new + sizeof(SDF_protocol_msg_t);
        plat_assert(pdata != NULL);
        (void) memcpy((void *) pdata_msg, pdata, data_size);
        pm_new->data_offset = 0;
    } else {
        msg    = (struct sdf_msg *) sdf_msg_alloc(msize);
        #ifdef MALLOC_TRACE
            UTMallocTrace("sdf_msg_alloc", SDF_FALSE, SDF_FALSE, SDF_FALSE, (void *) msg, msize);
        #endif // MALLOC_TRACE
        if (msg == NULL) {
            plat_log_msg(21133, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "Not enough memory, sdf_msg_alloc() failed.");
            return(NULL);
        }
        pm_new = (SDF_protocol_msg_t *) msg->msg_payload;
        pm_new->data_offset = 0;
    }

    pm_new->current_version = PROTOCOL_MSG_VERSION;
    pm_new->supported_version = PROTOCOL_MSG_VERSION;
    pm_new->data_size   = data_size;

    /*   Remember to set the writethru flag if necessary, because the
     *   default behavior is writeback!
     */
    pm_new->flags       = flags;
    if (meta && (meta->properties.cache.writethru)) {
        pm_new->flags |= f_writethru;
    }

    pm_new->tag         = tag;
    pm_new->msgtype     = msg_type;
    pm_new->node_from   = node_from;
    pm_new->node_to     = node_to;
    pm_new->action_node = node_from;
    pm_new->cache       = node_from;
    pm_new->thrd        = ctxt;
    pm_new->transid     = transid;
    pm_new->cguid       = cguid;
    pm_new->shard       = shard;
    if (pkey != NULL) {
        pm_new->key     = *pkey;
    }
    pm_new->n_replicas  = 0;
    pm_new->flushtime   = flushtime;
    pm_new->exptime     = exp_time;
    pm_new->createtime  = create_time;
    pm_new->status      = SDF_SUCCESS;
    pm_new->seqno       = seqno;
    pm_new->curtime     = curtime;

    pm_op_meta = &pm_new->op_meta;

    if (meta) {
        pm_new->shard_count = meta->properties.shard.num_shards;
    }

    if (meta && meta->properties.replication.enabled) {
        struct sdf_replicator *replicator = state->config.replicator;

        if ((sdf_replicator_get_op_meta(replicator,
                                        meta,
                                        shard,
                                        pm_op_meta)) != SDF_SUCCESS) {
            status = SDF_FAILURE;
        }
    } else {
        pm_op_meta->shard_meta.type = SDF_REPLICATION_NONE;
    }

    if (status == SDF_SUCCESS) {
        *pmsize = msize;
    } else {
        plat_free(msg);
        msg = NULL;
        *pmsize = 0;
    }

    return(msg);
}

static int shardSync_wrapper(SDF_trans_state_t *ptrans, struct shard *pshard)
{
    int                      ret = SDF_SUCCESS;
    SDF_action_init_t  __attribute__((unused)) *pai = ptrans->pts->pai;

    struct sdf_msg          *send_msg = NULL;
    struct sdf_msg          *new_msg = NULL;
    SDF_protocol_msg_t      *pm_new = NULL;
    SDF_size_t               msize;
    SDF_protocol_msg_type_t  hf_mtype;
    SDF_boolean_t            send_msg_flag = SDF_TRUE;
    vnode_t                  to_node;
    service_t                to_service;
    SDF_status_t             error;

    if ((!ptrans->meta->meta.properties.cache.writethru) || ptrans->meta->meta.properties.container_type.async_writes) {

        /*  Drain the store/writeback pipe before doing the shardSync.
         *  This is only necessary for writeback containers or containers that
         *  are configured for async writes.
         */

        drain_store_pipe(ptrans->pts);
    }

    if (!ptrans->pas->enable_replication) {
        ssd_shardSync(pshard);
        return(SDF_SUCCESS);
    } else {

        #ifdef SIMPLE_REPLICATION
            if (SDFSimpleReplication) {
                qrep_state_t      *ps;
                
                ps = &(ptrans->pas->qrep_state);

                send_msg_flag = SDF_FALSE;

                to_node = partner_replica_by_cguid(ps, ptrans->par->ctnr);

                if (to_node != SDF_ILLEGAL_VNODE && node_is_alive(ps, to_node)) {
                    send_msg_flag = SDF_TRUE;
                    to_service    = SDF_FLSH;
                } else {
                    /*  Sync the local flash here since I am
                     *  not sending a message.
                     */
                    ssd_shardSync(pshard);
                }
            }
        #endif

        if (send_msg_flag) {

            #ifdef SIMPLE_REPLICATION
                if (!SDFSimpleReplication) {
                    to_node    = ptrans->pts->phs->mynode;
                    to_service = SDF_REPLICATION;
                }
            #else
                to_node    = ptrans->pts->phs->mynode;
                to_service = SDF_REPLICATION;
            #endif

            /*  Create a message and pass it to the replication service.
             */

            hf_mtype = HFSSH; // shardSync

            send_msg = load_msg(ptrans->tag,
                                0,
                                0,
                                ptrans->pts->phs->mynode, // from
                                to_node, // to
                                hf_mtype,
                                0, 
                                NULL,
                                ptrans->par->ctxt, 
                                ptrans->par->ctnr,
                                0,                        // transid
                                NULL,
                                0,                        // flags
                                &msize, 
                                0,                        // flushtime
                                0,                        // curtime
                                ptrans->meta->meta.shard,
                                0,
                                &(ptrans->meta->meta));

            if (send_msg == NULL) {
                return(SDF_FAILURE_MSG_ALLOC);
            }

            if (sdf_msg_send(send_msg,
                             msize,
                             /* to */
                             to_node,
                             to_service,
                             /* from */
                             ptrans->pts->phs->mynode,
                             SDF_RESPONSES,
                             FLSH_REQUEST,
                             &(ptrans->pts->req_mbx), NULL)) {

                plat_log_msg(21101, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                       "sdf_msg_send to replication failed");
                return(SDF_FAILURE_MSG_SEND);
            }

            /*  Sync the local flash while we are waiting for
             *  a response from the replica.
             */
            ssd_shardSync(pshard);

            /* Wait for the response */
            new_msg = (struct sdf_msg *)fthMboxWait(&ptrans->pts->req_resp_fthmbx);

            /* Check message response */
            error = sdf_msg_get_error_status(new_msg);
            if (error != SDF_SUCCESS) {
                sdf_dump_msg_error(error, new_msg);
                sdf_msg_free_buff(new_msg);
                return SDF_FAILURE_MSG_RECEIVE;
            }

            pm_new = (SDF_protocol_msg_t *) (new_msg->msg_payload);

            // Remove these until Jake can check forward compatibility!
            // plat_assert(pm_new->current_version == PROTOCOL_MSG_VERSION);
            // plat_assert(pm_new->supported_version == PROTOCOL_MSG_VERSION);

            ret = pm_new->status;

            // garbage collect the response message
            sdf_msg_free_buff(new_msg);
        }
    }

    return(ret); // SDF_status_t
}

int protocol_message_report_version( char **bufp, int *lenp) 
{
    return (plat_snprintfcat(bufp, lenp, "%s %d.%d.%d\r\n", 
                             "memcached/replication/protocol_message", PROTOCOL_MSG_VERSION, 0, 0));
}

#ifdef CHECK_HEAP
#ifdef HEAP_STRESS_TEST

#define N_DATA   64*1024*1024
static char stress_data[N_DATA];

static void oh_oh_stress(char *s)
{
    plat_log_msg(20819, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                 "%s", s);
    plat_abort();
}

static void heap_stress_test(SDF_trans_state_t *ptrans)
{
    /* temporary stress test for new SDF allocation stuff */

    SDFNewCache_t              *pc;
    SDFNewCacheEntry_t         *entry;
    int   nkeys = 10;
    int   i;
    int   datalen;
    int   keylen;
    char *keys[] = {
        "key0_afafaf",
        "key1_jkljasfiaerlladfjkkkll",
        "key2_adfladfllasdfjklkl",
        "key3_908",
        "key4_0989-ujlkbujas8f____dfasdfijk;;mncnlkasdfeijjkladiooesj",
        "key5_adfsioelwipp;ghaiooklkueigadflkjjlkj",
        "key6_02kreiugya",
        "key7_zz",
        "key8_098;loiiojefiiaolhjioghadd;slfkjeijfcklisljg;lkhieeslasfjiogyeialdfgue2iaf88ewsafff",
        "key9_jiekltlfiaooelifjkelicsazjdckaoeillffioasl9847dffiel",
    };
    int   keylens[20];

    char *pdata;
    char *pdata_read;
    int   data_offset;
    char  key[256];
    int   x;
    char  chrs[] = "abcdefghijklmnopqrstuvwxyz";
    SDF_simple_key_t       simple_key;
    char   *skey;
    SDF_boolean_t  new_entry;

    pc = ptrans->pts->new_actiondir;

    srandom(0);

    for (i=0; i<N_DATA; i++) {
        stress_data[i] = chrs[random() % 26];
    }

    for (i=0; i<nkeys; i++) {
        keylens[i] = strlen(keys[i]);
    }

    pdata_read = plat_alloc(ptrans->pas->max_obj_size);
    plat_assert_always(pdata_read);

    while (1) {

        x = random() % 2;
        if (x) {
            /* generate a new random key */
            keylen = 1 + (random() % 253);
            for (i=0; i<keylen; i++) {
                x = random() % 26;
                key[i] = chrs[x];
            }
            key[keylen] = '\0';

            skey = key;

        } else {
            /* use one of the standard keys */

            x = random() % nkeys;
            skey = keys[x];
            keylen = keylens[x];
        }
        // simple_key.len = keylen;
        simple_key.len = keylen + 1;
        (void) strcpy(simple_key.key, skey);
        
        entry = SDFNewCacheGetCreate(ptrans->pts->new_actiondir,
                                     ptrans->par->ctnr,
                                     &simple_key,
                                     ptrans->par->ctnr_type,
                                     0,
                                     SDF_TRUE, /* lock_bucket */
                                     &ptrans->pbucket,
                                     SDF_FALSE, /* just try the lock */
                                     &new_entry,
                                     (void *) ptrans);
        
        datalen = random() % 4096;
        for (data_offset = random() % N_DATA;
             (N_DATA - data_offset) < datalen;
             data_offset = random() % N_DATA);
        pdata = &(stress_data[data_offset]);

        (void) fprintf(stderr, "=====>  Set '%s' (keylen:%d, datalen:%d)\n", simple_key.key, keylen, datalen);

        #ifdef notdef
        if (strstr(simple_key.key, "key6_")) {
            if (datalen == 578) {
                (void) fprintf(stderr, "-----------------------------------------\n");
                (void) fprintf(stderr, "-------------------  AHA!  --------------\n");
                (void) fprintf(stderr, "-----------------------------------------\n");
            }
        }
        #endif

        if ((new_entry) ||
            (entry->obj_size != datalen))
        {
            if (!new_entry) {
                /* this is an overwrite, so get a whole new cache entry */

                entry = SDFNewCacheOverwriteCacheObject(ptrans->pts->new_actiondir, entry, datalen, (void *) ptrans);
                if (entry->obj_size != datalen) {
                    oh_oh_stress("SDFNewCacheCreateCacheObject screwed up!");
                }
            } else { // this is a new cache entry
                plat_assert(entry->obj_size == 0);
                /* append an object buffer to the end of the current cache entry */
                entry = SDFNewCacheCreateCacheObject(ptrans->pts->new_actiondir, entry, datalen, (void *) ptrans);
                if (entry->obj_size != datalen) {
                    oh_oh_stress("SDFNewCacheCreateCacheObject screwed up!");
                }
            }
        }
        SDFNewCacheTransientEntryCheck(ptrans->pbucket, entry);
        SDFNewCacheCopyIntoObject(ptrans->pts->new_actiondir, pdata, entry, datalen);
        if (ptrans->meta->meta.properties.cache.writethru) {
            entry->state = CS_S;
        } else {
            entry->state = CS_M;
        }

        SDFNewUnlockBucket(ptrans->pts->new_actiondir, ptrans->pbucket);

        /*  Read the data back.
         */

        entry = SDFNewCacheGetCreate(ptrans->pts->new_actiondir,
                                     ptrans->par->ctnr,
                                     &simple_key,
                                     ptrans->par->ctnr_type,
                                     0,
                                     SDF_TRUE, /* lock_bucket */
                                     &ptrans->pbucket,
                                     SDF_FALSE, /* just try the lock */
                                     &new_entry,
                                     (void *) ptrans);
        if (new_entry) {
            oh_oh_stress("Could not read data back!");
        }
        SDFNewCacheCopyOutofObject(pc, pdata_read, entry, ptrans->pas->max_obj_size);
        if (strncmp(pdata_read, pdata, datalen) != 0) {
            oh_oh_stress("Data mismatch!");
        }
        
        SDFNewCacheTransientEntryCheck(ptrans->pbucket, entry);
        SDFNewUnlockBucket(ptrans->pts->new_actiondir, ptrans->pbucket);

        /*  Read one of the predefined keys.
         */

        x = random() % nkeys;
        skey = keys[x];
        keylen = keylens[x];
        simple_key.len = keylen + 1;
        (void) strcpy(simple_key.key, skey);

        entry = SDFNewCacheGetCreate(ptrans->pts->new_actiondir,
                                     ptrans->par->ctnr,
                                     &simple_key,
                                     ptrans->par->ctnr_type,
                                     0,
                                     SDF_TRUE, /* lock_bucket */
                                     &ptrans->pbucket,
                                     SDF_FALSE, /* just try the lock */
                                     &new_entry,
                                     (void *) ptrans);
        
        if (!new_entry) {
            SDFNewCacheTransientEntryCheck(ptrans->pbucket, entry);
        }
        SDFNewUnlockBucket(ptrans->pts->new_actiondir, ptrans->pbucket);
    }
}
#endif
#endif

