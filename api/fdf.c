/*
 * File:   fdf.c
 * Author: Darryl Ouye
 *
 * Created on October 20, 2012
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 */
#include <time.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>

#include "sdf.h"
#include "sdf_internal.h"
#include "zs.h"
#include "fdf_internal.h"
#include "fdf_internal_cb.h"
#include "lc.h"
#include "nvr_svc.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_common.h"
#include "protocol/action/recovery.h"
#include "protocol/home/home_flash.h"
#include "protocol/action/async_puts.h"
#include "protocol/action/action_new.h"
#include "protocol/action/action_thread.h"
#include "protocol/replication/replicator.h"
#include "protocol/replication/copy_replicator.h"
#include "protocol/replication/replicator_adapter.h"
#include "agent/agent_common.h"
#include "agent/agent_helper.h"
#include "sdftcp/locks.h"
#include "sdftcp/tools.h"
#include "shared/private.h"
#include "shared/init_sdf.h"
#include "shared/name_service.h"
#include "shared/shard_compute.h"
#include "shared/container_meta.h"
#include "shared/open_container_mgr.h"
#include "shared/internal_blk_obj_api.h"
#include "ssd/fifo/mcd_osd.h"
#include "ssd/fifo/hash.h"
#include "ssd/fifo/mcd_rec2.h"
#include "ssd/fifo/mcd_bak.h"
#include "ssd/fifo/mcd_trx.h"
#include "ssd/fifo/mcd_check.h"
#include "utils/properties.h"
#include "ssd/ssd_aio.h"
#include "ssd/fifo/slab_gc.h"
#include <execinfo.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/types.h>
#ifdef FLIP_ENABLED
#include "flip/flip.h"
#endif
#include <ssd/fifo/scavenger.h>
#include "btree/btree_raw.h"

#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_NAMING
#define LOG_TRACE PLAT_LOG_LEVEL_TRACE
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_DIAG PLAT_LOG_LEVEL_DIAGNOSTIC
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_WARN PLAT_LOG_LEVEL_WARN
#define LOG_FATAL PLAT_LOG_LEVEL_FATAL
#define BUF_LEN 4096
#define STATS_API_TEST 1

#include <snappy-c.h>
__thread char *compression_buf = NULL; 
__thread int compression_buf_len = 0 ; 

/*
** Globals
*/
static int Force_async_writes;
static int Enable_async_writes;
static struct sdf_agent_state agent_state;
static sem_t Mcd_fsched_sem;
static sem_t Mcd_initer_sem;

// Global container limit. Override with ZS_MAX_NUM_CONTAINERS property.
unsigned max_num_containers = MCD_MAX_NUM_CNTRS;

extern HashMap cmap_cname_hash;           // cname -> cguid
extern __thread uint64_t *trx_bracket_slabs;
extern int storm_mode;

ZS_status_t get_btree_num_objs(ZS_cguid_t cguid, uint64_t *num_objs, uint64_t *ov_objs);

void update_container_stats(SDF_action_init_t *pai, int reqtype, SDF_cache_ctnr_metadata_t *meta, int count) {
    atomic_add(pai->pcs->stats_new_per_sched[curSchedNum].ctnr_stats[meta->n].appreq_counts[reqtype], count);
}

extern int mcd_check_meta();
extern int mcd_check_flog();
extern int mcd_check_pot();

#define MCD_MAX_NUM_SCHED       32
#define MCD_MAX_NUM_FTHREADS    1025

int zs_instance_id;

static time_t 			current_time 	= 0;
static int stats_dump = 0;
SDF_shardid_t       cmc_shardid = SDF_SHARDID_INVALID;
SDF_shardid_t      vmc_shardid = SDF_SHARDID_INVALID;
SDF_shardid_t	vdc_shardid		= SDF_SHARDID_INVALID;
static int dump_interval = 0;
int zs_dump_core = 0;
int lc_exists;

/* Id used to uniquely differenciate 
   parallel container deletes with same name */
static uint32_t delete_prefix = 0; 
 
char *ZS_Status_Strings[] = {
    "UNKNOWN_STATUS", /* since ZS_SUCCESS is 1! */
#define item(caps, value) \
        #caps,
        ZS_STATUS_ITEMS()
#undef item
    };


/* From enumerate.c */
ZS_status_t cguid_to_shard(
	SDF_action_init_t *pai, 
	ZS_cguid_t cguid, 
	shard_t **shard_ptr, 
	int delete_ok);


void zs_signal_handler(int signum);
/*
** Externals
*/
extern void						*cmc_settings;
extern int 			 			 Mcd_osd_max_nclasses;
extern SDF_cmc_t 				*theCMC;
extern uint64_t 	 			 Mcd_num_pending_ios;
extern struct SDF_shared_state 	 sdf_shared_state;
extern int __zs_check_mode_on;

extern int loadProperties(
	const char *path_arg;
	);

extern void destroy_per_thread_state(SDF_action_thrd_state_t *pts);

extern 
SDF_container_meta_t * build_meta(
	const char 				*path, 
	SDF_container_props_t 	 props, 
	SDF_cguid_t 		 	 cguid, 
	SDF_shardid_t 		 	 shard
	);

extern 
SDF_shardid_t build_shard(
	struct SDF_shared_state *state, 
	SDF_internal_ctxt_t 	*pai,
    const char 				*path, 
	uint64_t 			 		 num_objs,  
	uint32_t 		 		 in_shard_count,
    SDF_container_props_t 	 props,  
	SDF_cguid_t 		 	 cguid,
    enum build_shard_type 	 build_shard_type, 
	const char 				*cname
	);

/*   From mcd_osd.c:
 *   Enumerate the next object for this container.
 *   Very similar to process_raw_get_command().
 */
extern SDF_status_t process_raw_get_command_enum(
    mcd_osd_shard_t  *shard,
    osd_state_t      *osd_state,
    uint64_t          addr,
    uint64_t          prev_seq,
    uint64_t          curr_seq,
    int               num_sessions,
    int               session_id,
    int               max_objs,
    char            **key_out,
    uint32_t         *keylen_out,
    char            **data_out,
    uint64_t         *datalen_out,
    uint64_t         *addr_out
    );

extern
SDF_status_t delete_container_internal_low(
    SDF_internal_ctxt_t     *pai,
    const char      		*path,
    SDF_boolean_t        	 serialize,
    SDF_boolean_t        	 delete_shards,
    int 					*deleted
    );

/*
** Forward declarations
*/
static ZS_status_t zs_create_container(
    struct ZS_thread_state *zs_thread_state,
    char                    *cname,
    ZS_container_props_t   *properties,
    uint32_t                 flags,
    ZS_cguid_t             *cguid,
    ZS_container_mode_t     mode
	);

static ZS_status_t zs_open_container(
    struct ZS_thread_state *zs_thread_state,
    char                    *cname,
    ZS_container_props_t   *props,
    uint32_t                 flags,
    ZS_cguid_t             *cguid,
    ZS_container_mode_t     mode,
	ZS_boolean_t			 serialize
    );

static ZS_status_t zs_close_container(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t  		     cguid,
	ZS_container_mode_t	 mode,
	ZS_boolean_t			 serialize,
	ZS_boolean_t			 delete_check
	);

static ZS_status_t zs_delete_container(
    struct ZS_thread_state *zs_thread_state,
    ZS_cguid_t              cguid,
	ZS_container_mode_t	 mode
    );

#if 0
static SDF_container_props_t *zs_create_sdf_props(
    ZS_container_props_t 			*zs_properties,
    ZS_internal_container_props_t  *zs_internal_properties
    );
#else
static SDF_container_props_t *zs_create_sdf_props(
    ZS_container_props_t 			*zs_properties
	);
#endif

static ZS_status_t zs_create_zs_props(
    SDF_container_props_t   *sdf_properties,
    ZS_container_props_t   *zs_properties
    );
 
static ZS_status_t zs_resize_container(
    struct ZS_thread_state *zs_thread_state,
    ZS_cguid_t              cguid,
	uint64_t				 size
    );

ZS_status_t ZSInitPerThreadState(
	struct ZS_state 		 *zs_state,
    struct ZS_thread_state **thd_state
	);

static void zs_start_vc_thread( 
	struct ZS_state *sdf_state 
	);

static ZS_status_t zs_vc_init(
	struct ZS_thread_state  *zs_thread_state,
    int                  	  flags
    );

static ZS_status_t zs_generate_cguid(
	struct ZS_thread_state  *zs_thread_state,
        char                    *cname,
	ZS_cguid_t              *cguid
	);

static ZS_status_t zs_delete_object(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t          	  cguid,
	char                	 *key,
	uint32_t             	  keylen,
	bool					  raw_object
	);

static ZS_status_t zs_delete_objects(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t 			  cguid
	);

static ZS_status_t
zs_get_containers(
		struct ZS_thread_state	*zs_thread_state,
		ZS_cguid_t             *cguids,
		uint32_t                *n_cguids
		);

static ZS_status_t
zs_get_containers_int(
		struct ZS_thread_state	*zs_thread_state,
		ZS_cguid_t             *cguids,
		uint32_t                *n_cguids
		);

static ZS_status_t
zs_get_open_containers_int(
                struct ZS_thread_state *zs_thread_state,
                ZS_cguid_t             *cguids,
                uint32_t                *n_cguids
                );

static ZS_status_t
zs_get_container_props(
		struct ZS_thread_state	*zs_thread_state, 
		ZS_cguid_t 		  	 cguid, 
		ZS_container_props_t	*pprops
		);

static ZS_status_t
zs_get_container_props_int(
		struct ZS_thread_state	*zs_thread_state, 
		ZS_cguid_t 		  	 cguid, 
		ZS_container_props_t	*pprops
		);


static ZS_status_t
zs_flush_container(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t               cguid
	);

static ZS_status_t zs_delete_container_1(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t		 		 cguid,
	ZS_container_mode_t	 mode
	);

extern SDF_status_t get_status(int retcode);

/*
** Types
*/
typedef enum {
    apgrx = 0,
    apgrd,
    apcoe,
    ahgtr,
    ahcob,
    ahcwd,
    hacrc,
    hacsc,
    hagrf,
    hagrc,
    owrites_s,
    owrites_m,
    ipowr_s,
    ipowr_m,
    newents,
    MCD_NUM_SDF_COUNTS,
}ZS_cache_counts_t;

static char * ZSCacheCountStrings[] = {
    "APGRX",
    "APGRD",
    "APCOE",
    "AHGTR",
    "AHCOB",
    "AHCWD",
    "HACRC",
    "HACSC",
    "HAGRF",
    "HAGRC",
    "overwrites_s",
    "overwrites_m",
    "inplaceowr_s",
    "inplaceowr_m",
    "new_entries",
};

zs_stats_info_t zs_stats_access_type[] = {
    {"APCOE","num_created_objs_with_expiry",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APCOE*/
    {"APCOP","num_created_objs",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APCOP*/
    {"APPAE","num_put_objs_with_expiry",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APPAE*/
    {"APPTA","num_put_objs",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APPTA*/
    {"APSOE","num_set_objs_with_expiry",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APSOE*/
    {"APSOB","num_set_objs",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APSOB*/
    {"APGRX","num_get_objs_and_check_expiry",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APGRX*/
    {"APGRD","num_get_objs",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APGRD*/
    {"APDBE","num_del_objs_with_expiry",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APDBE*/
    {"APDOB","num_del_objs",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APDOB*/
    {"APFLS","num_flush_objs",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APFLS*/
    {"APFLI","num_flush_and_invalidate_objs",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APFLI*/
    {"APINV","num_invalidate_objs",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APINV*/
    {"APSYC","num_sync_to_flash",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APSYC*/
    {"APICD","num_delayed_invalidates",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APICD*/
    {"APGIT","delayed_invalidation_time",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APGIT*/
    {"APFCO","num_flush_container",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APFCO*/
    {"APFCI","num_flush_and_invalidate_container",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APFCI*/
    {"APICO","num_invalidate_container",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APICO*/
    {"APRIV","num_remote_invalidations",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRIV*/
    {"APRUP","num_remote_updates",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/

    {
        "ENUM_TOTAL",
        "completed_enumerations",
        ZS_STATS_TYPE_APP_REQ,
    },

    {
        "ENUM_ACTIVE",
        "active_enumerations",
        ZS_STATS_TYPE_APP_REQ,
    },

    {
        "ENUM_OBJECTS",
        "objects_enumerated",
        ZS_STATS_TYPE_APP_REQ,
    },

    {
        "ENUM_CACHED_OBJECTS",
        "cached_objects_enumerated",
        ZS_STATS_TYPE_APP_REQ,
    },
    {
        "ZS_ACCESS_TYPES_NUM_CONT_DELETES_PEND",
        "num_container_deletes_pending",
        ZS_STATS_TYPE_APP_REQ,
    },
    {
        "ZS_ACCESS_TYPES_NUM_CONT_DELETES_PROG",
        "num_container_deletes_progress",
        ZS_STATS_TYPE_APP_REQ,
    },
    {"ZS_ACCESS_TYPES_READ","num_reads",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_WRITE","num_writes",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_DELETE","num_deletes",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_FLUSH","num_flushes",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_MPUT","num_mputs",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_MSET","num_msets",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_RANGE","num_range",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_RANGE_NEXT","num_range_next",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_RANGE_FINISH","num_range_finish",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_RANGE_UPDATE","num_range_updates",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_CREATE_SNAPSHOT","num_create_snapshots",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_DELETE_SNAPSHOT","num_delete_snapshots",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_LIST_SNAPSHOT","num_list_snapshots",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_TRX_START","num_transaction_starts",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_ACCESS_TYPES_TRX_COMMITS","num_transaction_commits",ZS_STATS_TYPE_APP_REQ},/*ZS_ACCESS_TYPES_APRUP*/
};
char *get_access_type_stats_desc(int stat ) {
    if( stat >= sizeof(zs_stats_access_type)/sizeof(zs_stats_info_t)) {
        return "Invalid stat";
    }
    return zs_stats_access_type[stat].desc;
}
zs_stats_info_t zs_stats_btree[] = {
    // btree related categories
    {"ZS_BTREE_L1_ENTRIES","l1_cache_entries",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_L1_ENTRIES */
    {"ZS_BTREE_L1_OBJECTS","l1_cache_objects",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_L1_OBJECTS */
    {"ZS_BTREE_LEAF_L1_HITS","l1_cache_leaf_hits",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_LEAF_L1_HITS */
    {"ZS_BTREE_NONLEAF_L1_HITS","l1_cache_nonleaf_hits",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_NONLEAF_L1_HITS */
    {"ZS_BTREE_OVERFLOW_L1_HITS","l1_cache_overflow_hits",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_OVERFLOW_L1_HITS */

    {"ZS_BTREE_LEAF_L1_MISSES","l1_cache_leaf_misses/log:write slow path",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_LEAF_L1_MISSES */
    {"ZS_BTREE_NONLEAF_L1_MISSES","l1_cache_nonleaf_misses/log:mput slow:Temp buf space full",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_NONLEAF_L1_MISSES */
    {"ZS_BTREE_OVERFLOW_L1_MISSES","l1_cache_overflow_misses/log:mput slow:Temp buf small",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_OVERFLOW_L1_MISSES */
    {"ZS_BTREE_BACKUP_L1_MISSES","l1_cache_snapread_misses/log:mput slow:First record",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_NONLEAF_L1_MISSES */
    {"ZS_BTREE_BACKUP_L1_HITS","l1_cache_snapread_misses/log:mput slow:No space in NVRAM buffer",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_OVERFLOW_L1_MISSES */

    {"ZS_BTREE_LEAF_L1_WRITES","l1_cache_leaf_writes/log:mput slow:Different stream",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_LEAF_L1_WRITES */
    {"ZS_BTREE_NONLEAF_L1_WRITES","l1_cache_nonleaf_writes",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_NONLEAF_L1_WRITES */
    {"ZS_BTREE_OVERFLOW_L1_WRITES","l1_cache_overflow_writes",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_OVERFLOW_L1_WRITES */
    {"ZS_BTREE_LEAF_NODES","num_leaf_nodes",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_LEAF_NODES */
    {"ZS_BTREE_NONLEAF_NODES","num_nonleaf_nodes",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_NONLEAF_NODES */

    {"ZS_BTREE_OVERFLOW_NODES","num_overflow_nodes",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_OVERFLOW_NODES */
    {"ZS_BTREE_LEAF_BYTES","leaf_bytes",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_LEAF_BYTES */
    {"ZS_BTREE_NONLEAF_BYTES","nonleaf_bytes",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_NONLEAF_BYTES */
    {"ZS_BTREE_OVERFLOW_BYTES","overflow_bytes",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_OVERFLOW_BYTES */
    {"ZS_BTREE_NUM_OBJS","num_objs",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_NUM_OBJS */

    {"ZS_BTREE_TOTAL_BYTES","total_bytes",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_NUM_OBJS */
    {"ZS_BTREE_EVICT_BYTES","evict_bytes",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_EVICT_BYTES */
    {"ZS_BTREE_SPLITS","num_splits",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_SPLITS */
    {"ZS_BTREE_LMERGES","num_lmerges",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_LMERGES */
    {"ZS_BTREE_RMERGES","num_rmerges",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_RMERGES */

    {"ZS_BTREE_LSHIFTS","num_lshifts",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_LSHIFTS */
    {"ZS_BTREE_RSHIFTS","num_rshifts",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_RSHIFTS */
    {"ZS_BTREE_EX_TREE_LOCKS","nym_xtree_locks",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_EX_TREE_LOCKS */
    {"ZS_BTREE_NON_EX_TREE_LOCKS","num_non_xtree_locks",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_NON_EX_TREE_LOCKS */
    {"ZS_BTREE_GET","num_creates",ZS_STATS_TYPE_BTREE},/*ZS_BTREE_APRUP*/
    {"ZS_BTREE_GET_PATH_LEN","get_path_len",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_GET_PATH_LEN */
    {"ZS_BTREE_CREATE","num_creates",ZS_STATS_TYPE_BTREE},/*ZS_BTREE_APRUP*/
    {"ZS_BTREE_CREATE_PATH_LEN","create_path_len",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_CREATE_PATH_LEN */
    {"ZS_BTREE_SET","num_sets",ZS_STATS_TYPE_BTREE},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_BTREE_SET_PATH_LEN","set_path_len",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_SET_PATH_LEN */
    {"ZS_BTREE_UPDATE","num_updates",ZS_STATS_TYPE_BTREE},/*ZS_ACCESS_TYPES_APRUP*/
    {"ZS_BTREE_UPDATE_PATH_LEN","update_path_len",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_UPDATE_PATH_LEN */
    {"ZS_BTREE_DELETE_PATH_LEN","delete_path_len",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_DELETE_PATH_LEN */
    {"ZS_BTREE_FLUSH_CNT","flush_count",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_FLUSH_CNT */

    {"ZS_BTREE_DELETE_OPT_COUNT","delete_opt_count",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_DELETE_OPT_COUNT */
    {"ZS_BTREE_MPUT_IO_SAVED","mput_io_saved",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_MPUT_IO_SAVED */
    {"ZS_BTREE_PUT_RESTART_CNT","put_restart_cnt",ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_PUT_RESTART_CNT */
    {"ZS_BTREE_SPCOPT_BYTES_SAVED", "space_opt_bytes_saved", ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_SPCOPT_BYTES_SAVED */
    {"ZS_BTREE_NUM_MPUT_OBJS", "num_mput_objs", ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_SPCOPT_BYTES_SAVED */

    {"ZS_BTREE_NUM_RANGE_NEXT_OBJS", "num_range_next_objs", ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_SPCOPT_BYTES_SAVED */
    {"ZS_BTREE_NUM_RANGE_UPDATE_OBJS", "num_range_update_objs", ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_SPCOPT_BYTES_SAVED */
    {"ZS_BTREE_NUM_SNAP_OBJS", "num_snapshot_objs", ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_SPCOPT_BYTES_SAVED */
    {"ZS_BTREE_NUM_SNAP_DATA_SIZE", "num_snapshot_size", ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_SPCOPT_BYTES_SAVED */
    {"ZS_BTREE_NUM_NUM_SNAPS", "num_snapshots", ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_SPCOPT_BYTES_SAVED */
    {"ZS_BTREE_NUM_BULK_INSERT_CNT", "num_bulk_inserts", ZS_STATS_TYPE_BTREE},/* ZS_BTREE_NUM_BULK_INSERT_CNT */
    {"ZS_BTREE_NUM_BULK_INSERT_FULL_NODES_CNT", "num_bulk_fullnode_inserts", ZS_STATS_TYPE_BTREE},/* ZS_CACHE_STAT_BT_SPCOPT_BYTES_SAVED */
    
};

char *get_btree_stats_desc(int stat ) {
    if( stat >= sizeof(zs_stats_btree)/sizeof(zs_stats_info_t)) {
        return "Invalid stat";
    }    
    return zs_stats_btree[stat].desc;
}

zs_stats_info_t zs_stats_flash[] = {
    {"NUM_OBJS","num_items_flash",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_OBJS*/
    {"NUM_CREATED_OBJS","num_items_created",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_CREATED_OBJS*/
    {"NUM_EVICTIONS","num_evictions_flash",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_EVICTIONS*/
    {"HASH_EVICTIONS","num_hash_evictions",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_HASH_EVICTIONS*/
    {"INVAL_EVICTIONS","num_inval_evictions",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_INVAL_EVICTIONS*/
    {"SOFT_OVERFLOWS","num_soft_overflows",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_SOFT_OVERFLOWS*/
    {"NUM_HARD_OVERFLOWS","num_hard_overflows",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_HARD_OVERFLOWS*/
    {"GET_HASH_COLLISION","num_get_hash_collisions",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_GET_HASH_COLLISION*/
    {"SET_HASH_COLLISION","num_set_hash_collisions",ZS_STATS_TYPE_FLASH},/* ZS_FLASH_STATS_SET_HASH_COLLISION*/
    {"NUM_OVERWRITES","num_overwrites",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_OVERWRITES*/
    {"NUM_OPS","num_flash_ops",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_OPS*/
    {"READ_OPS","num_read_ops",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_READ_OPS*/
    {"GET_OPS","num_get_ops",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_GET_OPS*/
    {"PUT_OPS","num_put_ops",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_PUT_OPS*/
    {"DEL_OPS","num_del_ops",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_DEL_OPS*/
    {"EXIST_CHECKS","num_existence_checks",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_GET_EXIST_CHECKS*/
    {"FULL_BUCKETS","num_full_hash_buckets",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_NUM_FULL_BUCKETS*/
    {"PENDING_IOS","num_pending_ios",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_PENDING_IOS*/
    {"SPACE_ALLOCATED","flash_space_allocated",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_SPACE_ALLOCATED*/
    {"SPACE_CONSUMED","flash_space_consumed",ZS_STATS_TYPE_FLASH},/*ZS_FLASH_STATS_SPACE_CONSUMED*/

    {"SEGMENTS_COMPACTED","slab_gc_segments_compacted",ZS_STATS_TYPE_FLASH},
    {"SEGMENTS_FREED","slab_gc_segments_freed",ZS_STATS_TYPE_FLASH},
    {"SLABS_RELOCATED","slab_gc_slabs_relocated",ZS_STATS_TYPE_FLASH},
    {"BLOCKS_RELOCATED","slab_gc_blocks_relocated",ZS_STATS_TYPE_FLASH},
    {"RELOCATE_ERRORS","slab_gc_relocate_errors",ZS_STATS_TYPE_FLASH},
    {"SIGNALLED","slab_gc_signalled",ZS_STATS_TYPE_FLASH},
    {"SIGNALLED_SYNC","slab_gc_signalled_sync",ZS_STATS_TYPE_FLASH},
    {"WAIT_SYNC","slab_gc_wait_sync",ZS_STATS_TYPE_FLASH},
    {"SEGMENTS_CANCELLED","slab_gc_segments_cancelled",ZS_STATS_TYPE_FLASH}, 

    {"FREE_SEGMENTS","slab_free_segments",ZS_STATS_TYPE_FLASH}, 
    {"COMPRESSED_BYTES","flash_compressed_bytes",ZS_STATS_TYPE_FLASH}, 
    {"ZS_FLASH_STATS_THD_CONTEXTS","zs_thd_contexts",ZS_STATS_TYPE_FLASH}, 
    {"ESCVN_OBJ_DEL","scavenged_object_count",ZS_STATS_TYPE_FLASH},
    {"ESCVN_YLD_SCAN_CMPLTE","scavenger_scans_completed",ZS_STATS_TYPE_FLASH},
    {"ESCVN_YLD_SCAN_RATE","scavenger_yields",ZS_STATS_TYPE_FLASH},

    {"NUM_DATA_WRITES","num_data_writes",ZS_STATS_TYPE_FLASH}, 
    {"NUM_DATA_FSYNCS","num_data_fsyncs",ZS_STATS_TYPE_FLASH}, 
    {"NUM_LOG_WRITES","num_log_writes",ZS_STATS_TYPE_FLASH}, 
    {"NUM_LOG_FSYNCS","num_log_fsyncs",ZS_STATS_TYPE_FLASH}, 
};

char *get_flash_type_stats_desc(int stat ) {
    if( stat >= sizeof(zs_stats_flash)/sizeof(zs_stats_info_t)) {
        return "Invalid stat";
    }
    return zs_stats_flash[stat].desc;
}

zs_stats_info_t zs_stats_cache[] = {
    {"overwrites_s","num_overwrites_s_state",ZS_STATS_TYPE_OVERWRITES},/* ZS_CACHE_STAT_OVERWRITES_S */
    {"overwrites_m","num_overwrites_m_state",ZS_STATS_TYPE_OVERWRITES},/* ZS_CACHE_STAT_OVERWRITES_M */
    {"inplaceowr_s","num_inplace_overwrites_s_state",ZS_STATS_TYPE_OVERWRITES},/* ZS_CACHE_STAT_INPLACEOWR_S */
    {"inplaceowr_m","num_inplace_overwrites_m_state",ZS_STATS_TYPE_OVERWRITES},/* ZS_CACHE_STAT_INPLACEOWR_M */
    {"new_entries","num_new_entries",ZS_STATS_TYPE_OVERWRITES},/* ZS_CACHE_STAT_NEW_ENTRIES */
    {"writethrus","num_writethrus_to_flash",ZS_STATS_TYPE_OVERWRITES},/* ZS_CACHE_STAT_WRITETHRUS */
    {"writebacks","num_writebacks",ZS_STATS_TYPE_OVERWRITES},/* ZS_CACHE_STAT_WRITEBACKS */
    {"flushes","num_flush_ops_to_flash",ZS_STATS_TYPE_OVERWRITES},/* ZS_CACHE_STAT_FLUSHES */
    {"async_drains","async_drains",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_ASYNC_DRAINS */
    {"async_puts","async_puts",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_ASYNC_PUTS */
    {"async_put_fails","async_put_fails",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_ASYNC_PUT_FAILS */
    {"async_flushes","async_flushes",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_ASYNC_FLUSHES */
    {"async_flush_fails","async_flush_fails",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_ASYNC_FLUSH_FAILS */
    {"async_wrbks","async_wrbks",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_ASYNC_WRBKS */
    {"async_wrbk_fails","async_wrbk_fails",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_ASYNC_WRBK_FAILS */
    {"cache_misses","cache_misses",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_CACHE_MISSES */
    {"cache_hits","cache_hits",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_CACHE_HITS */
    /* request from cache to flash manager */
    {"AHCOB","num_create_objs",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_AHCOB */
    {"AHCOP","num_create_objs_and_put",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_AHCOP */
    {"AHCWD","num_create_objs_with_data",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_AHCWD */
    {"AHDOB","num_delete_objs",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_AHDOB */
    {"AHFLD","num_flush_objs",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_AHFLD */
    {"AHGTR","num_get_objs_to_read",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_AHGTR */
    {"AHGTW","num_get_objs_to_write",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_AHGTW */
    {"AHPTA","num_put_objs",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_AHPTA */
    {"AHSOB","num_set_objs",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_AHSOB */
    {"AHSOP","num_set_objs_and_put",ZS_STATS_TYPE_CACHE_TO_FLASH},/* ZS_CACHE_STAT_AHSOP */
    /* Request from flash manager to cache */
    {"HACRC","num_create_objs_completed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HACRC */
    {"HACRF","num_create_objs_failed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HACRF */
    {"HACSC","num_castout_completed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HACSC */
    {"HACSF","num_castout_failed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HACSF */
    {"HADEC","num_delete_objs_completed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HADEC */
    {"HADEF","num_delete_objs_failed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HADEF */
    {"HAFLC","num_flush_objs_completed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HAFLC */
    {"HAFLF","num_flush_objs_failed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HAFLF */
    {"HAGRC","num_get_objs_to_read_completed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HAGRC */
    {"HAGRF","num_get_objs_to_read_failed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HAGRF */
    {"HAGWC","num_get_objs_to_write_completed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HAGWC */
    {"HAGWF","num_get_objs_to_write_failed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HAGWF */
    {"HAPAC","num_put_objs_completed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HAPAC */
    {"HAPAF","num_put_objs_failed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HAPAF */
    {"HASTC","num_set_objs_completed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HASTC */
    {"HASTF","num_set_objs_failed",ZS_STATS_TYPE_FLASH_TO_CACHE},/* ZS_CACHE_STAT_HASTF */
    {"HFXST","num_existence_checks",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFXST */
    {"FHXST","num_existence_success",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHXST */
    {"FHNXS","num_existence_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHNXS */
    {"HFGFF","num_get_objs",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFGFF */
    {"FHDAT","num_objs_data",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHDAT */
    {"FHGTF","num_get_objs_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHGTF */
    {"HFPTF","num_put_objs",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFPTF */
    {"FHPTC","num_put_objs_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHPTC */
    {"FHPTF","num_put_objs_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHPTF */
    {"HZSF","num_delete_objs",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HZSF */
    {"FHDEC","num_delete_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHDEC */
    {"FHDEF","num_delete_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHDEF */
    {"HFCIF","num_create_objects",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFCIF */
    {"FHCRC","num_create_completerdf",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHCRC */
    {"FHCRF","num_create_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHCRF */
    {"HFCZF","num_create_zeroed_objs",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFCZF */
    {"HFCRC","num_create_zeroed_objs_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFCRC */
    {"HFCRF","num_create_zeroed_objs_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFCRF */
    {"HFSET","num_set_objs",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFSET */
    {"HFSTC","num_set_objs_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFSTC */
    {"FHSTF","num_set_objs_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHSTF */
    {"HFCSH","num_create_shards",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFCSH */
    {"FHCSC","num_create_shards_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHCSC */
    {"FHCSF","num_create_shards_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHCSF */
    {"FHSSH","num_sync_shards",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFSSH */
    {"FHSSC","num_sync_shards_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHSSC */
    {"FHSSF","num_sync_shards_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHSSF */
    {"HFDSH","num_delete_shards",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFDSH */
    {"FHDSC","num_delete_shards_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHDSC */
    {"FHDSF","num_delete_shards_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHDSF */
    {"HFGLS","num_get_last_seq",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFGLS */
    {"FHGLC","num_get_last_seq_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHGLC */
    {"FHGLF","num_get_last_seq_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHGLF */
    {"HFGIC","num_get_iter_cursors",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFGIC */
    {"FHGIC","num_get_iter_cursors_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHGIC */
    {"FHGIF","num_get_iter_cursors_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHGIF */
    {"HFGBC","num_get_by_cursors",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFGBC */
    {"FHGCC","num_get_by_cursors_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHGCC */
    {"FHGCF","num_get_by_cursors_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHGCF */
    {"HFGSN","num_get_seq_numbers",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFGSN */
    {"HFGCS","num_get_container_stats",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFGCS */
    {"FHGSC","num_get_container_stats_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHGSC */
    {"FHGSF","num_get_container_stats_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHGSF */
    {"HFSRR","num_replication_starts",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFSRR */
    {"FHSRC","num_replication_starts_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHSRC */
    {"FHSRF","num_replication_starts_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHSRF */
    {"HFSPR","num_replication_stops",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFSPR */
    {"FHSPC","num_replication_stops_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHSPC */
    {"FHSPF","num_replication_stops_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHSPF */
    {"HFFLA","num_flush_objs",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFFLA */
    {"FHFLC","num_flush_objs_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHFLC */
    {"FHFLF","num_flush_objs_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHFLF */
    {"HFRVG","num_release_vip_grps",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFRVG */
    {"FHRVC","num_release_vip_grps_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHRVC */
    {"FHRVF","num_release_vip_grps_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHRVF */
    {"HFNOP","num_noop",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFNOP */
    {"FHNPC","num_noop_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHNPC */
    {"FHNPF","num_noop_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHNPF */
    {"HFOSH","num_open_shards",ZS_STATS_TYPE_FLASH_MANAGER}, /* ZS_CACHE_STAT_HFOSH */
    {"FHOSC","num_open_shards_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHOSC */
    {"FHOSF","num_open_shards_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHOSF */
    {"HFFLS","num_flush_objs",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFFLS */
    {"FHFCC","num_flush_objs_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHFCC */
    {"FHFCF","num_flush_objs_fialed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHFCF */
    {"HFFIV","num_flush_invalidate_objs",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFFIV */
    {"FHFIC","num_flush_invalidate_objs_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHFIC */
    {"FHFIF","num_flush_invalidate_objs_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHFIF */
    {"HFINV","num_invalidate_objs",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFINV */
    {"FHINC","num_invalidate_objs",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHINC */
    {"FHINF","num_invalidate_objs",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHINF */
    {"HFFLC","num_flush_containers",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFFLC */
    {"FHLCC","num_flush_containers_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHLCC */
    {"FHLCF","num_flush_containers_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHLCF */
    {"HFFLI","num_flush_invalidate_containers",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFFLI */
    {"FHLIC","num_flush_invalidate_containers_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHLIC */
    {"FHLIF","num_flush_invalidate_containers_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHLIF */
    {"HFINC","num_invalidate_containers",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_HFINC */
    {"HFINC","num_invalidate_containers_completed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHCIC */
    {"FHCIF","num_invalidate_containers_failed",ZS_STATS_TYPE_FLASH_MANAGER},/* ZS_CACHE_STAT_FHCIF */
    {"EOK","num_success",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EOK */
    {"EPERM","num_errors_not_permitted_",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EPERM */
    {"ENOENT","num_errors_objects_not_found",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_ENOENT */
    {"EDATASIZE","num_errors_insufficient_buffer",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EDATASIZE */
    {"ESTOPPED","num_errors_container_stopped",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_ESTOPPED */
    {"EBADCTNR","num_errors_container_not_found",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EBADCTNR */
    {"EDELFAIL","num_errors_delete_failed",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EDELFAIL */
    {"EAGAIN","num_errors_try_again",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EAGAIN */
    {"ENOMEM","num_errors_no_memory",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_ENOMEM */
    {"EACCES","num_errors_perm_denied",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EACCES */
    {"EINCONS","num_errors_replication_inconsistencies",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EINCONS */
    {"EBUSY","num_errors_dev_busy",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EBUSY */
    {"EEXIST","num_errors_obj_exists",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EEXIST */
    {"EINVAL","num_errors_invalid_arguments",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EINVAL */
    {"EMFILE","num_errors_too_many_objs",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EMFILE */
    {"ENOSPC","num_errors_no_flash_space",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_ENOSPC */
    {"ENOBUFS","num_errors_no_system_resource",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_ENOBUFS */
    {"ESTALE","num_errors_stale_data",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_ESTALE */
    {"EDQUOT","num_errors_quota_exceeded",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_EDQUOT */
    {"EDELFAIL","num_errors_remote_delete_failures",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_RMT_EDELFAIL */
    {"EBADCTNR","num_errors_no_remote_container",ZS_STATS_TYPE_FLASH_RC},/* ZS_CACHE_STAT_RMT_EBADCTNR */
    {"hashBuckets","num_hash_buckets_in_cache",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_HASH_BUCKETS */
    {"nSlabs","num_cache_partitions",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_SLABS */
    {"numElements","num_objects_in_cache",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_ELEMENTS */
    {"maxSz","max_cache_capacity",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_MAX_SIZE */
    {"currSz","current_data_size_in_cache",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_CURR_SIZE */
    {"currSzWkeys","current_key_and_data_size_in_cache",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_CURR_SIZE_WKEYS */
    {"nMod","num_modified_objs_in_cache",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_MODIFIED_OBJS */
    {"modSzWkeys","num_bytes_of_modified_objs_in_cache",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_MODIFIED_OBJS_WKEYS */
    {"nModFlushes","num_mod_objs_flushed",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_MODIFIED_OBJS_FLUSHED */
    {"nModBGFlushes","num_mod_objs_flushed_by_bgflush",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_MODIFIED_OBJS_BGFLUSHED */
    {"nPending","num_pending_remote_cache_req",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_PENDING_REQS */
    {"nModRecEnums","num_modified_objs_copied_during_recovery",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_MODIFIED_OBJC_REC */
    {"bkFlshProg","background_flush_progress",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_BGFLUSH_PROGRESS */
    {"nBkFlsh","num_background_flushes",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_BGFLUSH */
    {"nFlshTok","max_parallel_flushes",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_FLUSH_PARALLEL */
    {"nBkFlshTok","max_parallel_bg_flushes",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_BGFLUSH_PARALLEL */
    {"FlsMs","time_to_wait_after_bgflush_for_nodirty_data",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_BGFLUSH_WAIT */
    {"modPct","max_percent_limit_on_modifiable_cache",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_MODIFIED_PCT */
    {"nAppBufs","num_app_buffers_inuse",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_APP_BUFFERS */
    {"nTrans","num_cache_ops_in_progress",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_CACHE_OPS_PROG */
    {"nFGBufs","num_flash_data_buffer_being_processed",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_FGBUFFER_PROCESSED */
    {"nResp","num_resp_msg_being_processed",ZS_STATS_TYPE_PER_CACHE},/* ZS_CACHE_STAT_NUM_RESP_PROCESSED  */
};

char *get_cache_type_stats_desc(int stat ) {
    if( stat >= sizeof(zs_stats_cache)/sizeof(zs_stats_info_t)) {
        return "Invalid stat";
    }
    return zs_stats_cache[stat].desc;
}

int get_cache_type_stats_category(int stat ) {
    if( stat >= sizeof(zs_stats_cache)/sizeof(zs_stats_info_t)) {
        return -1;
    }
    return zs_stats_cache[stat].category;
}   

zs_stats_info_t zs_stats_cntr[] = {
    {"NUM_OBJS","num_objs",ZS_STATS_TYPE_CONTAINER_FLASH},/*ZS_CNTR_STATS_NUM_OBJS*/
    {"USED_SPACE","used_space",ZS_STATS_TYPE_CONTAINER_FLASH},/*ZS_CNTR_STATS_USED_SPACE*/
};

char *get_cntr_type_stats_desc(int stat ) {
    if( stat >= sizeof(zs_stats_cntr)/sizeof(zs_stats_info_t)) {
        return "Invalid stat";
    }
    return zs_stats_cntr[stat].desc;
}

ZS_status_t verify_stats_datastruct() {
    int num_stats_array;
    /*Number of stats listed in the corresponding descriptive array zs_stats_access_type*/
    num_stats_array = sizeof(zs_stats_access_type)/sizeof(zs_stats_info_t); 
    if( ZS_N_ACCESS_TYPES != num_stats_array ) {
         plat_log_msg(70127, LOG_CAT, LOG_ERR,
            "Programming error: Numbers of stats defined in ZS_access_types_t(%d) does not match array zs_stats_access_type(%d)",
                     ZS_N_ACCESS_TYPES,num_stats_array);
         return ZS_FAILURE;
    }

    /*Number of stats listed in the corresponding descriptive array zs_stats_access_type*/
    num_stats_array = sizeof(zs_stats_cache)/sizeof(zs_stats_info_t); 
    if( ZS_N_CACHE_STATS != num_stats_array ) {
         plat_log_msg(70128, LOG_CAT, LOG_ERR,
            "Programming error: Numbers of stats defined in ZS_cache_stat_t(%d) does not match array zs_stats_cache(%d)",
                     ZS_N_CACHE_STATS,num_stats_array);
         return ZS_FAILURE;
    }

    /*Number of stats listed in the corresponding descriptive array zs_stats_access_type*/
    num_stats_array = sizeof(zs_stats_flash)/sizeof(zs_stats_info_t);
    if( ZS_N_FLASH_STATS != num_stats_array ) {
         plat_log_msg(160207, LOG_CAT, LOG_ERR,
            "Programming error: Numbers of stats defined in ZS_flash_stat_t(%d) does not match array zs_stats_flash(%d)",
                     ZS_N_FLASH_STATS,num_stats_array);
         return ZS_FAILURE;
    }
    /*Number of stats listed in the corresponding descriptive array zs_btree_stats*/
    num_stats_array = sizeof(zs_stats_btree)/sizeof(zs_stats_info_t);
    if( ZS_N_BTREE_STATS != num_stats_array ) {
         plat_log_msg(160208, LOG_CAT, LOG_ERR,
            "Programming error: Numbers of stats defined in ZS_Btree_stat_t(%d) does not match array zs_stats_btree(%d)",
                     ZS_N_BTREE_STATS,num_stats_array);
         return ZS_FAILURE;
    }
    return ZS_SUCCESS;
}

ZS_status_t verify_datastruct_consistency() {
    if( (ZS_status_t) N_SDF_STATUS_STRINGS != N_ZS_STATUS_STRINGS ) {
        plat_log_msg(80047, LOG_CAT, LOG_ERR,
           "Data structure SDF_status_t(size:%d) and ZS_status_t(size:%d) is not consistent.",N_SDF_STATUS_STRINGS, N_ZS_STATUS_STRINGS);
        return ZS_FAILURE;
    }
    return verify_stats_datastruct();
}

typedef enum {
    MCD_STAT_GET_KEY = 0,
    MCD_STAT_GET_DATA,
    MCD_STAT_SET_KEY,
    MCD_STAT_SET_DATA,
    MCD_STAT_GET_TIME,
    MCD_STAT_SET_TIME,
    MCD_STAT_MAX_COUNT,
} mcd_fth_stat_t;

typedef enum {
    ZS_DRAM_CACHE_HITS = 0,
    ZS_DRAM_CACHE_MISSES,
    ZS_FLASH_CACHE_HITS,
    ZS_FLASH_CACHE_MISSES,
    ZS_DRAM_CACHE_CASTOUTS,
    ZS_DRAM_N_OVERWRITES,
    ZS_DRAM_N_IN_PLACE_OVERWRITES,
    ZS_DRAM_N_NEW_ENTRY,
    MCD_NUM_SDF_STATS,
} ZS_cache_stats_t;

extern
struct shard *
container_to_shard( 
	SDF_internal_ctxt_t * pai, 
	local_SDF_CONTAINER lc );

extern
void
shard_recover_phase2( 
	mcd_osd_shard_t * shard );

extern 
SDF_status_t create_put_meta(
	SDF_internal_ctxt_t 	*pai, 
	const char 		*path, 
	SDF_container_meta_t 	*meta,
        SDF_cguid_t 		 cguid
	);

extern 
SDF_boolean_t agent_engine_pre_init(
	struct sdf_agent_state 	*state, 
	int 			 argc, 
	char 			*argv[]
	);

extern 
SDF_boolean_t agent_engine_post_init(
	struct sdf_agent_state 	*state
	);

ZS_status_t ZSGetStatsStr (
	struct ZS_thread_state *zs_thread_state,
	ZS_cguid_t 			 cguid,
	char 					*stats_str,
    ZS_stats_t				*stats
	);

void action_stats_new_cguid(SDF_internal_ctxt_t *pac, char *str, int size, SDF_cguid_t cguid);
void action_stats(SDF_internal_ctxt_t *pac, char *str, int size);

/*
 * Log levels.
 */
char *Log_levels[] ={
    "devel",
    "trace_low",
    "trace",
    "debug",
    "diagnostic",
    "info",
    "warning",
    "error",
    "fatal"
};

/*
 * Set the ZS log level.
 */
static void
set_log_level(unsigned int level)
{
    int i;
    char buf[256];
    char *cats[] = { "apps", "platform", "sdf", "fth", "print_args", "flash" };

    if (level >= nel(Log_levels))
        level = nel(Log_levels) - 1;

    for (i = 0; i < nel(cats); i++) {
        snprintf(buf, sizeof(buf), "%s=%s", cats[i], Log_levels[level]);
        plat_log_parse_arg(buf);
    }
}

ZS_status_t change_log_level(char *level) {
    int i;
    for (i = 0; i < nel(Log_levels); i++){
        if (streq(level, Log_levels[i])) {
            set_log_level(i);
            agent_state.flash_settings.sdf_log_level = i;
            return ZS_SUCCESS;
        }
    }
    return ZS_FAILURE;
}

/*
 * Parse the ZS_LOG_LEVEL.
 */
static int
parse_log_level()
{
    int i;
    const char *v = getProperty_String("ZS_LOG_LEVEL", NULL);

    if (!v)
        return LOG_INFO;

    if (streq(v, "none"))
        return LOG_FATAL;

    for (i = 0; i < nel(Log_levels); i++)
        if (streq(v, Log_levels[i]))
            return i;

    if (isdigit(v[0]) && v[1] == '\0') {
        i = atoi(v);
        if (i < nel(Log_levels))
            return i;
    }

    zs_loge(70125, "Bad setting of ZS_LOG_LEVEL: %s", v);
    return LOG_DBG;
}


static int zs_check_delete_in_future(void *data)
{
    return(0);
}

/**
 * @brief: Check if given cguid belongs to a valid
 *         container (virtual, non-null cguid)
 * @params[in]: cguid container ID
 * @return ZS_TRUE if cguid belongs to a virtual container
 *         ZS_FALSE otherwise
 */
static ZS_status_t
zs_validate_container(uint64_t cguid)
{
	switch (cguid) {
		case          0:
		case  CMC_CGUID:
		case  VMC_CGUID:
		case  VDC_CGUID:
			return ZS_FAILURE_ILLEGAL_CONTAINER_ID;
        case         -1:
            return ZS_FAILURE_CONTAINER_NOT_FOUND;
		default:
			return ZS_SUCCESS;
	}
}

/*
 * same as zs_validate_container(), for exclusive use
 * in ZSGetContainerStats() and ZSGetStatsStr()  for
 * trac #11290
 */
static ZS_status_t
zs_validate_container_1(uint64_t cguid)
{
    switch (cguid) {
        case          0:
        case  CMC_CGUID:
        case  VMC_CGUID:
        //excluding VDC container.
        //case  VDC_CGUID:
            return ZS_FAILURE_ILLEGAL_CONTAINER_ID;
        default:
            return ZS_SUCCESS;
    }
}

static void zs_load_settings(flash_settings_t *osd_settings)
{
    /* Set properties which defaults isn't suitable for library */
	insertProperty("SDF_PROP_FILE_VERSION", "1");
	insertProperty("SHMEM_FAKE", "1");
	insertProperty("MEMCACHED_STATIC_CONTAINERS", "1");
	insertProperty("SDF_FLASH_PROTOCOL_THREADS", storm_mode ? "16" : "1");
	insertProperty("ZS_LOG_FLUSH_DIR", "/tmp");
//	insertProperty("ZS_CC_BUCKETS", "1000");
//	insertProperty("ZS_CC_NSLABS", "100");

	/*EF: When zs_instance_id set it used as as a suffix for all shared resources,
		e.g. allows to start multiple instance of ZS on one machine */
    zs_instance_id = getProperty_Int("ZS_INSTANCE_ID", 0);

	/* Use random value to run multiple tests simultaneously */
	if(!zs_instance_id && getProperty_Int("ZS_TEST_MODE", 0))
		zs_instance_id = getpid();

    (void) strcpy(osd_settings->aio_base, getProperty_String("ZS_FLASH_FILENAME", "/tmp/schooner%d")); // base filename of flash files
	/* This is added for compatibility with old property files which don't contain ZS_FLASH_FILENAME property */
	const char *p = getProperty_String("AIO_BASE_FILENAME", osd_settings->aio_base);
	if(p != osd_settings->aio_base)
	    (void) strcpy(osd_settings->aio_base, p); // base filename of flash files

    osd_settings->aio_create          = 1;// use O_CREAT - membrain sets this to 0
    osd_settings->aio_total_size      = getProperty_Int("ZS_FLASH_SIZE", ZS_MIN_FLASH_SIZE); // this flash size counts! 3Gb by default
    osd_settings->aio_total_size      = getProperty_Int("AIO_FLASH_SIZE_TOTAL", osd_settings->aio_total_size); // compatibility with old property files
    osd_settings->aio_sync_enabled    = getProperty_Int("AIO_SYNC_ENABLED", 0); // AIO_SYNC_ENABLED
    osd_settings->no_direct_io        = !getProperty_Int("AIO_O_DIRECT", 1);
    osd_settings->rec_log_verify      = 0;
    osd_settings->enable_fifo         = 1;
    osd_settings->bypass_aio_check    = 0;
    osd_settings->storm_mode          = getProperty_Int( "ZS_STORM_MODE", 1);
    osd_settings->storm_test          = getProperty_Int( "ZS_STORM_TEST", 0);

    /*
     * checksum of metadata, data, or entire object
     */
    osd_settings->chksum_data         = getProperty_Int( "ZS_OBJECT_DATA_CHECKSUM", 0);
    osd_settings->chksum_metadata     = getProperty_Int( "ZS_OBJECT_METADATA_CHECKSUM", 0); 
    osd_settings->chksum_object       = getProperty_Int( "ZS_OBJECT_CHECKSUM", 1);

    osd_settings->sb_data_copies      = 0; // use default
    osd_settings->multi_fifo_writers  = getProperty_Int("SDF_MULTI_FIFO_WRITERS", 1);
    osd_settings->aio_wc              = false;
    osd_settings->aio_error_injection = false;
    osd_settings->aio_queue_len       = getProperty_Int( "ZS_AIO_QUEUE_LENGTH", MCD_MAX_NUM_FTHREADS);

    // num_threads // legacy--not used

    osd_settings->num_cores        = getProperty_Int("SDF_FTHREAD_SCHEDULERS", 1); // "-N" 
    osd_settings->num_sched        = osd_settings->num_cores;
    osd_settings->num_sdf_threads  = getProperty_Int("SDF_THREADS_PER_SCHEDULER", 1); // "-T"

    osd_settings->sdf_log_level    = parse_log_level();
    osd_settings->aio_num_files    = getProperty_Int("AIO_NUM_FILES", 1); // "-Z"
    osd_settings->aio_sub_files    = 0; // what are these? ignore?
    osd_settings->aio_first_file   = 0; // "-z" index of first file! - membrain sets this to -1
    osd_settings->mq_ssd_balance   = 0;  // what does this do?
    osd_settings->no_direct_io     = !getProperty_Int("ZS_O_DIRECT", 1);
    osd_settings->sdf_persistence  = 0; // "-V" force containers to be persistent!
    osd_settings->max_aio_errors   = getProperty_Int("MEMCACHED_MAX_AIO_ERRORS", 1000 );
    osd_settings->check_delete_in_future = zs_check_delete_in_future;
    osd_settings->pcurrent_time	    = &current_time;
    osd_settings->is_node_independent = 1;
    osd_settings->ips_per_cntr	    = 1;
    osd_settings->rec_log_size_factor = 0;
    osd_settings->os_blk_size = getProperty_Int("ZS_BLOCK_SIZE", ZS_DEFAULT_BLOCK_SIZE);
}

/*
 * zs_check_settings
 *
 * DESCRIPTION:
 * Check whether the settings meet the minimum requirements.
 * 
 * INPUT: Flash settings having details of device.
 *
 * OUTPUT:
 * 	true		If all inputs are valid 
 * 	false		Minimum requirements not met.
 */	
static bool
zs_check_settings(flash_settings_t *osd_settings)
{
	plat_assert(osd_settings);

	/* Device base name has to be set */
	if (osd_settings->aio_base[0] == '\0') {
		plat_log_msg(160123, LOG_CAT, LOG_ERR,	
			"Base file name of flash files not set");
		return false;
	}

	/* Device size must be atleast minimum size */
	if (osd_settings->aio_total_size < ZS_MIN_FLASH_SIZE) {
		plat_log_msg(160124, LOG_CAT, LOG_ERR,	
			"Device size is less than minimum required");
		return false;
	}
	
	if ((osd_settings->os_blk_size < MCD_OSD_BLK_SIZE_MIN) ||
		(osd_settings->os_blk_size > MCD_OSD_BLK_SIZE_MAX)) {
		plat_log_msg(160166, LOG_CAT, LOG_ERR,
			"Block size is not in supported range");
		return false;
	}

	if (osd_settings->os_blk_size &
		(osd_settings->os_blk_size - 1)) {
		plat_log_msg(160170, LOG_CAT, LOG_ERR,
			"Block size is not power of two");
		return false;
	}
	return true;
}


char *get_log_level() {
    if( agent_state.flash_settings.sdf_log_level < nel(Log_levels) ) {
        return Log_levels[agent_state.flash_settings.sdf_log_level];
    }
    return "Invalid level set";
}

/*
 * Lock and unlock the thread context.
 */
inline bool
zs_lock_thd_ctxt(struct ZS_thread_state *thd_state)
{
	bool ret = false;
	SDF_action_init_t *pai = (SDF_action_init_t*) thd_state;
	SDF_action_thrd_state_t *pts = (SDF_action_thrd_state_t *)
					pai->pts;
	plat_assert(pts->ctxt_in_use == false);

	ret = !(__sync_fetch_and_or(&pts->ctxt_in_use, true));
	return ret;
}

inline void
zs_unlock_thd_ctxt(struct ZS_thread_state *thd_state)
{
	SDF_action_init_t *pai = (SDF_action_init_t*) thd_state;
	SDF_action_thrd_state_t *pts = (SDF_action_thrd_state_t *)
					pai->pts;
	
	plat_assert(pts->ctxt_in_use == true);
	bool oldval = __sync_fetch_and_and(&pts->ctxt_in_use, false);

	/*
	 * It was indeed locked.
	 */
	plat_assert(oldval == true);
}

/*
 * Check if we could allow an operation to start
 * XXX: TODO We would pass an operation as an argument to this
 *            function.
 * @param [in] void
 * @retval SDF_boolean_t, SDF_TRUE for success
 */
ZS_status_t 
is_zs_operation_allowed(void)
{
	ZS_status_t status = ZS_SUCCESS;

	/*
	 * Check if shutdown is in progress
	 */
	if (SDF_TRUE == agent_state.op_access.is_shutdown_in_progress) {
		/*
		 * Disallow further operation
		 */
		status = ZS_FAILURE_OPERATION_DISALLOWED;

		plat_log_msg(160097, LOG_CAT, LOG_DBG, 
				"Operation denied: Shutdown in progress %s",
				ZS_Status_Strings[status]);
	} else if (NULL == fthSelf()) {
		/*
		 * Disallow further operation
		 */
		status = ZS_FAILURE_OPERATION_DISALLOWED;

		plat_log_msg(160185, LOG_CAT, LOG_DBG, 
				"Operation denied: Thread state already released, %s",
				ZS_Status_Strings[status]);
	}

	return status;
}


/*
 * Get the container map for a given container id.
 */
cntr_map_t *
get_cntr_map(cntr_id_t cntr_id)
{
    cntr_map_t *cmap = NULL;

	cmap = zs_cmap_get_by_cguid( cntr_id );

	if ( cmap )
    	atomic_inc( cmap->io_count );

    return cmap;
}

/*
 * Release the container map.
 */
void
rel_cntr_map(cntr_map_t *cmap)
{
    atomic_dec(cmap->io_count);
}


/*
 * Get information for a given container.
 */
int
get_cntr_info(cntr_id_t cntr_id,
              char *name, int name_len,
              uint64_t *objs,
              uint64_t *used,
              uint64_t *size,
              ZS_boolean_t *evicting)
{
    cntr_map_t *cmap = get_cntr_map(cntr_id);
    if (!cmap)
        return 0;

    if (name) {
        int n = name_len - 1;
        if (n > sizeof(cmap->cname))
            n = sizeof(cmap->cname);
        strncpy(name, cmap->cname, n);
        name[n] = '\0';
    }

    if (objs)
        *objs = cmap->num_obj;
    if (used)
        *used = cmap->current_size;
    if (size)
        *size = cmap->size_kb * 1024;
    if (evicting)
        *evicting = cmap->evicting;

    rel_cntr_map(cmap);
    /* Get num objs from btree if it is btree container */
    if(objs) {
		uint64_t ov_objs = 0;
		if (cmap->lc) {
			get_lc_num_objs(cntr_id,objs);
		} else if (get_btree_num_objs(cntr_id,objs, &ov_objs) == ZS_SUCCESS) {
			if (storm_mode) {
				*used += ov_objs * rawobjratio * Mcd_osd_blk_size;
			}
		}
	}
    return 1;
}


/*
 * Add a number of objects and the size consumed to a container map.  There are
 * two modes.  If check is set, we only make the change if it does not
 * contribute towards overburdening the container and return true if we made
 * the change.  If check is not set, we always make the change and return true
 * if the container is not overburdened.
 */
int
inc_cntr_map(cntr_id_t cntr_id, int64_t objs, int64_t blks, int check)
{
    int          ret = 1;
    int64_t     size = blks * Mcd_osd_blk_size;
    cntr_map_t *cmap = get_cntr_map(cntr_id);

    if (!cmap)
        return 1;

    int64_t t_objs = atomic_add_get(cmap->num_obj, objs);
    int64_t t_size = atomic_add_get(cmap->current_size, size);
    int64_t  limit = cmap->size_kb * 1024;

    if (limit && t_size > limit) {
        if (!check)
            ret = 0;
        else if (size > 0) {
            atomic_sub(cmap->num_obj, objs);
            atomic_sub(cmap->current_size, size);
            ret = 0;
        }
    }

    if (t_objs < 0)
        zs_loge(70115, "container %d would have %ld objects", cntr_id, objs);

    if (t_size < 0) {
        zs_loge(70116, "container %d would have a size of %ld bytes",
                 cntr_id, size);
    }

    rel_cntr_map(cmap);
    return ret;
}

int
inc_cntr_map_by_map(cntr_map_t *cmap, cntr_id_t cntr_id, int64_t objs, int64_t blks, int check)
{
    int          ret = 1;
    int64_t     size = blks * Mcd_osd_blk_size;

    if (!cmap)
        return 1;

    int64_t t_objs = atomic_add_get(cmap->num_obj, objs);
    int64_t t_size = atomic_add_get(cmap->current_size, size);
    int64_t  limit = cmap->size_kb * 1024;

    if (limit && t_size > limit) {
        if (!check)
            ret = 0;
        else if (size > 0) {
            atomic_sub(cmap->num_obj, objs);
            atomic_sub(cmap->current_size, size);
            ret = 0;
        }
    }

    if (t_objs < 0)
        zs_loge(70115, "container %d would have %ld objects", cntr_id, objs);

    if (t_size < 0) {
        zs_loge(70116, "container %d would have a size of %ld bytes",
                 cntr_id, size);
    }

    return ret;
}

ZS_status_t zs_get_ctnr_status(ZS_cguid_t cguid, int delete_ok) {
    cntr_map_t   *cmap   = NULL;
	ZS_status_t  status = ZS_FAILURE_CONTAINER_NOT_OPEN;

    cmap = zs_cmap_get_by_cguid( cguid );
    if ( !cmap ) {
        return ZS_FAILURE_CONTAINER_NOT_FOUND;
    } else {
        if ( !isContainerNull( cmap->sdf_container ) ) {
            status = ZS_CONTAINER_OPEN;
        }
        if ( ZS_CONTAINER_STATE_OPEN != cmap->state ) {
            status = ZS_FAILURE_CONTAINER_NOT_OPEN;
        }
        if ( delete_ok && ZS_CONTAINER_STATE_DELETE_OPEN == cmap->state ) {
            status = ZS_CONTAINER_OPEN;
        }
	}

    return status;
}

ZS_status_t
zs_get_ctnr_status_cmap(cntr_map_t *cmap, ZS_cguid_t cguid, int delete_ok)
{
	ZS_status_t  status = ZS_FAILURE_CONTAINER_NOT_OPEN;

	plat_assert(cmap && (cmap->cguid == cguid));

	if ( !isContainerNull( cmap->sdf_container ) ) {
		status = ZS_CONTAINER_OPEN;
	}
	if ( ZS_CONTAINER_STATE_OPEN != cmap->state ) {
		status = ZS_FAILURE_CONTAINER_NOT_OPEN;
	}
	if ( delete_ok && ZS_CONTAINER_STATE_DELETE_OPEN == cmap->state ) {
		status = ZS_CONTAINER_OPEN;
	}

	return status;
}

/* 
 * IMPORTANT:
 * If opening for write/delete objects, use incr_wr_io_count API
 */
inline void zs_incr_io_count( ZS_cguid_t cguid )
{
	cntr_map_t *cmap = NULL;

	cmap = zs_cmap_get_by_cguid( cguid );
	if ( cmap )
    	atomic_inc( cmap->io_count );
}

inline int zs_incr_wr_io_count( ZS_cguid_t cguid )
{
	cntr_map_t *cmap = NULL;

	cmap = zs_cmap_get_by_cguid( cguid );
	if ( cmap ) {
		if (cmap->read_only == ZS_TRUE) {
			return 0;
		} else {
			atomic_inc( cmap->io_count );
			return 1;
		}
	}
	return 0;
}

inline int zs_is_opened_ro( ZS_cguid_t cguid )
{
	cntr_map_t *cmap = NULL;

	cmap = zs_cmap_get_by_cguid( cguid );
	if ( cmap ) {
		return ((cmap->read_only == ZS_TRUE) ? 1 : 0);
	}
	return 0;
}

inline void zs_decr_io_count( ZS_cguid_t cguid )
{
	cntr_map_t *cmap = NULL;

	cmap = zs_cmap_get_by_cguid( cguid );
	if ( cmap )
    	atomic_dec( cmap->io_count );
}

// Return 0 - not open, 1 - open
int zs_is_ctnr_open(
    ZS_cguid_t cguid
    )
{
    int         result = 0;
	cntr_map_t *cmap   = NULL;

	cmap = zs_cmap_get_by_cguid( cguid );
	if ( cmap ) {
		if ( !isContainerNull( cmap->sdf_container ) ) {
			result = 1;
		}
        if ( ZS_CONTAINER_STATE_OPEN != cmap->state ) {
            result = 0;
        }
	}

    return result;
}

void
zs_cntr_set_readonly(
		cntr_map_t		*cmap
		)
{
	cmap->read_only = ZS_TRUE;
}

void
zs_cntr_set_readwrite(
		cntr_map_t		*cmap
		)
{
	cmap->read_only = ZS_FALSE;
}

ZS_status_t zs_ctnr_set_state(
    cntr_map_t          *cmap,
    ZS_CONTAINER_STATE	new_state
    )
{
    ZS_status_t status = ZS_FAILURE_INVALID_CONTAINER_STATE; 
    ZS_CONTAINER_STATE	current_state;

    if ( !cmap ) 
        return status;

    if ( cmap->cguid <= LAST_PHYSICAL_CGUID ) { 
        status = ZS_SUCCESS; 
        goto out; 
    }

    current_state = cmap->state;

    switch ( new_state ) { 
        case ZS_CONTAINER_STATE_UNINIT: 
            if ( ZS_CONTAINER_STATE_DELETE_CLOSED != current_state ) 
                goto err; 
                break;

        case ZS_CONTAINER_STATE_OPEN: 
            if ( ZS_CONTAINER_STATE_CLOSED != current_state ) 
                goto err; 
                break;

        case ZS_CONTAINER_STATE_CLOSED: 
            if ( ZS_CONTAINER_STATE_OPEN != current_state && 
                 ZS_CONTAINER_STATE_UNINIT != current_state ) 
                 goto err; 
                 break;

        case ZS_CONTAINER_STATE_DELETE_PROG: 
            if ( ZS_CONTAINER_STATE_CLOSED != current_state ) 
                 goto err; 
                 break;

        case ZS_CONTAINER_STATE_DELETE_OPEN: 
            if ( ZS_CONTAINER_STATE_DELETE_PROG != current_state ) 
                 goto err; 
                 break;

        case ZS_CONTAINER_STATE_DELETE_CLOSED: 
            if ( ZS_CONTAINER_STATE_DELETE_OPEN != current_state ) 
                 goto err; 
                 break;

         default: 
            goto err; 
            break;
    }

out:
	barrier();
	cmap->state = new_state; 
	status = ZS_SUCCESS;

err:

	if ( ZS_FAILURE_INVALID_CONTAINER_STATE == status ) {
		plat_log_msg( 150112, 
					  LOG_CAT, 
					  LOG_ERR, 
					  "Failed to change container %lu state: %d to %d", 
					  cmap->cguid, 
					  current_state, 
					  new_state);
	}

    return status;
}

static void zs_fth_initer(uint64_t arg)
{
    struct sdf_agent_state    	*state 		= (struct sdf_agent_state *) arg;



    wait_for_licd_start();
    if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		goto out;
    }	

    /*
     *  Final SDF Initialization
     */
    if ( SDF_TRUE != agent_engine_post_init( state ) ) {
        mcd_log_msg( 20147, PLAT_LOG_LEVEL_FATAL,
                     "agent_engine_post_init() failed" );
        plat_assert_always( 0 == 1 );
    }

    mcd_fth_start_osd_writers();

out:
    /*
     * signal the parent thread
     */
    sem_post( &Mcd_initer_sem );
}

#define STAT_BUFFER_SIZE 16384

#ifdef notdef
static void print_zs_stats(FILE *log, ZS_stats_t *stats, char *disp_str) {
    int i;
    char buf[BUF_LEN];
    fputs(disp_str,log); 
    for (i = 0; i < ZS_N_FLASH_STATS; i++ ) {
        if( stats->flash_stats[i] != 0 ) {
            sprintf(buf,"%s = %lu\n",zs_stats_flash[i].stat_token,stats->flash_stats[i]);
            fputs(buf,log);
        }
    }
    for (i = 0; i < ZS_N_CACHE_STATS; i++ ) {
        if( stats->cache_stats[i] != 0 ) {
            sprintf(buf,"%s = %lu\n",zs_stats_cache[i].stat_token,stats->cache_stats[i]);
            fputs(buf,log);
        }
    }
    for (i = 0; i < ZS_N_ACCESS_TYPES; i++ ) {
        if( stats->n_accesses[i] != 0 ) {
            sprintf(buf,"%s = %lu\n",zs_stats_access_type[i].stat_token,stats->n_accesses[i]);
            fputs(buf,log);
        }
    }
    fputs("---------\n",log); 
    fflush(log);
}
#endif /* notdef */

void set_stats_autodump_interval(int interval) {
    dump_interval = interval;
}

int get_autodump_interval() {
    return dump_interval;
}

void enable_stats_auto_dump() {
    stats_dump = 1;
}
void disable_stats_auto_dump() {
    stats_dump = 0;
}
int is_auto_dump_enabled() {
    return ((stats_dump == 1) && (dump_interval > 0 ));
}

static void *zs_stats_thread(void *arg) {
    FILE *stats_log = NULL;
    struct ZS_thread_state *thd_state;
    time_t st,et;
    int time_elapsed,cur_dump_int;
    int stats_level = getProperty_Int("ZS_STATS_LEVEL", ZS_STATS_LEVEL_SUMMARY);


    if (stats_level <= ZS_STATS_LEVEL_NO_STATS) {
        return NULL;
    }

    if ( ZS_SUCCESS != ZSInitPerThreadState( ( struct ZS_state * ) arg, ( struct ZS_thread_state ** ) &thd_state )) {
        fprintf(stderr,"Unable to init the stats thread, exiting.\n");
        return NULL;
    }

    stats_log = open_stats_dump_file();
    if (!stats_log) {
        fprintf(stderr,"Unable to open the stats log file. Exiting stats thread.\n");
        return NULL;
    }

    stats_dump = 1;
    dump_interval = getProperty_Int( "ZS_STATS_DUMP_INTERVAL", 60 ); 
    while(1) {
        if (agent_state.op_access.is_shutdown_in_progress) { 
            break; 
        }

        if( (stats_dump == 0) || ( dump_interval <= 0) ) {
            /* Auto dump has been disabled. sleep 5 secs and check again */
            sleep(5);
            continue;
        }

        time(&st);

        // Always print summary stats 
        log_summary_stats(thd_state, stats_log);

        switch(stats_level) {
            case ZS_STATS_LEVEL_CNTR_LIST:
                log_container_props(thd_state, stats_log);
                break;

            case ZS_STATS_LEVEL_CNTR_STATS:
                log_container_stats(thd_state, stats_log);
                break;
        
            case ZS_STATS_LEVEL_FLASH_STATS:
                log_flash_stats(thd_state, stats_log);
                break;
        
            case ZS_STATS_LEVEL_ALL_STATS:
                log_all_container_stats(thd_state, stats_log, STATS_PRINT_TYPE_DETAILED);
                break;
        
            default:
                break; // do nothing
        }

        fflush(stats_log);

        time(&et);
        /* calculate the time elapsed for getting and printing above stats */
        cur_dump_int = dump_interval;
        time_elapsed = (uint32_t)et - (uint32_t)st;
        if ( time_elapsed >= 0 ) {
            if (time_elapsed < cur_dump_int) {
                sleep(cur_dump_int - time_elapsed);
            }
        }
        else {
            /* System time changed backwords, just sleep for default interval */                    
            sleep(dump_interval);
        }
    }

    if (stats_log)  
        fclose(stats_log);
    return NULL;
}

static void *zs_scheduler_thread(void *arg)
{
    // mcd_osd_assign_pthread_id();

    /*
     * signal the parent thread
     */
    sem_post( &Mcd_fsched_sem );

    fthSchedulerPthread( 0 );

    return NULL;
}


static int zs_fth_cleanup( void )
{
    return 0;   /* SUCCESS */
}

void zs_start_stats_thread(struct ZS_state *sdf_state) {
    pthread_t thd;
    int rc;

    rc = pthread_create(&thd,NULL,zs_stats_thread,(void *)sdf_state);
    if( rc != 0 ) {
        fprintf(stderr,"Unable to start the stats thread\n");
    }
}


static void *zs_run_schedulers(void *arg)
{
    int                 rc;
    int                 i;
    pthread_t           fth_pthreads[MCD_MAX_NUM_SCHED];
    pthread_attr_t      attr;
    int                 num_sched;

    num_sched = (uint64_t) arg;

    /*
     * create the fthread schedulers
     */
    sem_init( &Mcd_fsched_sem, 0, 0 );

    pthread_attr_init( &attr );
    pthread_attr_setdetachstate( &attr, PTHREAD_CREATE_JOINABLE );

    for ( i = 1; i < num_sched; i++ ) {
        rc = pthread_create( &fth_pthreads[i], &attr, zs_scheduler_thread,
                             NULL);
        if ( 0 != rc ) {
            mcd_log_msg( 20163, PLAT_LOG_LEVEL_FATAL,
                         "pthread_create() failed, rc=%d", rc );
	    plat_assert(0);
        }
        mcd_log_msg( 20164, PLAT_LOG_LEVEL_TRACE, "scheduler %d created",
                     (int)fth_pthreads[i] );
    }

    /*
     * wait for fthreads initialization
     */
    for ( i = 1; i < num_sched; i++ ) {
        do {
            rc = sem_wait( &Mcd_fsched_sem );
        } while (rc == -1 && errno == EINTR);
        plat_assert( 0 == rc );
    }

    /*
     * Turn the main thread into a fthread scheduler directly as a
     * workaround for ensuring all the fthread stacks are registered
     * from the correct pthread in the N1 case.
     */

    fthSchedulerPthread( 0 );

    for ( i = 1; i < num_sched; i++ ) {
        pthread_join( fth_pthreads[i], NULL );
    }

    zs_fth_cleanup();

    return(NULL);
}


/*
 * Get a ZS property.
 */
const char *
ZSGetProperty(const char *key, const char *def)
{
	const char	*value;
	char 		*ret;	
	int		len;

	value = getProperty_String(key, def);
	if (value != def) {
		len = strlen(value);
		ret = (char *)plat_alloc(len + 1);
		if (ret != NULL) {
			strncpy(ret, value, len);
			*(ret + len) = '\0';
		}
		return ret;
	} else {
		return def;
	}
}


/*
** API
*/
ZS_status_t ZSSetProperty(const char* property, const char* value)
{
    int ret = 0;

    value = strndup(value, 256);
    if (value)
        ret = setProperty(property, (void*) value);

    if (ZS_log_level <= PLAT_LOG_LEVEL_INFO) {
        plat_log_msg(180021, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO,
                     "ZSSetProperty ('%s', '%s'). Old value: %s",
                     property, value, getProperty_String(property, "NULL"));
    }

    if ( ret )
        return ZS_FAILURE;
    else
        return ZS_SUCCESS;
}

ZS_status_t ZSLoadProperties(const char *prop_file)
{
	if ( !prop_file )
		return ZS_INVALID_PARAMETER;

	if ( loadProperties(prop_file) )
		return ZS_FAILURE;
	else {
        log_properties_file(prop_file,LOG_INFO);
		return ZS_SUCCESS;
    }
}

void log_properties_file(const char *path, int log_level) {
    char *line = (char *) plat_alloc(2048), *beg, *str, *key, *val;
    if ( path == NULL ) {
        return ;
    }
    FILE *fp = fopen(path, "r");
    if( fp == NULL ) {
        return;
    }
    while(fgets(line, 2048, fp)) {
        beg = line;
        /* Trim the beginning */
        while(' ' == *beg) {
            beg++;
        }
        /* search for comment */ 
        if('#' == *beg || '\0' == *beg || '\n' == *beg) {
            continue;
        }
        /* Get Key */ 
        str = beg;
        while('=' != *str && '\0' != *str && ' ' != *str && '\n' != *str) { 
            str++;
        }
	if (str - beg) {
		key = strndup(beg, str-beg);
	} else {
		continue;
	}
        /* Trim beginning */ 
        beg = str++; 
        while(' ' == *beg || '=' == *beg) {
            beg++;
        }
        str = beg;
        /* get Value */
        while('=' != *str && '\0' != *str && ' ' != *str && '\n' != *str) {
            str++;
        }
	if (str - beg) {
		val = strndup(beg, str-beg);
	} else {
		free(key);
		continue;
	}

        if (ZS_log_level <= log_level) {
            plat_log_msg(70036, PLAT_LOG_CAT_PRINT_ARGS,
                         log_level,"%s = %s",key, val);
        }
	if (key) free(key);
	if (val) free(val);
    }   
    fclose(fp);
    plat_free(line);
}       
void print_configuration(int log_level) {
    plat_log_msg(80030, LOG_CAT, log_level,
        "ZS Configuration: Storage size = %d GB,"
        "Reformat the storage = %s,"
        "Cache size = %llu,"
        "Maximum object size = %llu",
        getProperty_Int("ZS_FLASH_SIZE", ZS_MIN_FLASH_SIZE),
        getProperty_Int("ZS_REFORMAT", 0 )?"yes":"no",
        getProperty_uLongLong("ZS_CACHE_SIZE", storm_mode ? 17000000 : 100000000ULL),
        getProperty_uLongLong("SDF_MAX_OBJ_SIZE", SDF_MAX_OBJ_SIZE));
	plat_log_msg(160171, LOG_CAT, log_level,"Block size = %llu",
		getProperty_uLongLong("ZS_BLOCK_SIZE", ZS_DEFAULT_BLOCK_SIZE));
    if (getProperty_Int("ZS_TEST_MODE", 0)) {
         plat_log_msg(80031, LOG_CAT, log_level,"ZS Testmode enabled");
    }
}

ZS_status_t ZSRegisterCallbacks(struct ZS_state *zs_state, ZS_ext_cb_t *cb) 
{
    if( cb == NULL ) {
        return ZS_FAILURE;
    }

    ext_cbs = cb;
    plat_log_msg(150113, LOG_CAT, LOG_INFO, "Callback registered"); 
    return ZS_SUCCESS;
}
ZS_status_t ZSLicenseCheck(int *state)
{
	*state = (is_license_valid(false) == true) ? 1 : 0;
	return ZS_SUCCESS;
}
void agent_config_set_defaults(struct plat_opts_config_sdf_agent *config);
bool is_btree_loaded() {
    return (ext_cbs != NULL)?1:0;
}
ZS_status_t ZSInitVersioned(
	struct ZS_state	**zs_state,
	uint32_t                api_version
	)
{
    int                  rc;
    pthread_t            run_sched_pthread;
    pthread_attr_t       attr;
    uint64_t             num_sched;
    struct timeval		 timer;
    const char			*prop_file;
    struct SDF_shared_state      *state       = &sdf_shared_state; 

    char log_file[2048]="";

    if (api_version != ZS_API_VERSION) {
        mcd_log_msg(160260, PLAT_LOG_LEVEL_FATAL, 
                    "Error: Incompatibile ZS API Version. ZSInit called "
                    "with version '%u', ZS API version is '%u'\n",
                    api_version, ZS_API_VERSION);
        return ZS_VERSION_CHECK_FAILED;       
    }

    int check_x86_sse42( void);
    extern int sse42_present;

    sse42_present = check_x86_sse42( );


    if (verify_datastruct_consistency() != ZS_SUCCESS ) {
        return ZS_FAILURE;
    }
    sem_init( &Mcd_initer_sem, 0, 0 );

    gettimeofday( &timer, NULL );
    current_time = timer.tv_sec;

    *zs_state = (struct ZS_state *) &agent_state;

    prop_file = getenv("ZS_PROPERTY_FILE");
    agent_state.flash_settings.sdf_log_level = LOG_FATAL;
    if (prop_file)
        loadProperties(prop_file);

    // Set operational mode (normal API run or check). Default to run.
    __zs_check_mode_on = getProperty_Int("ZS_CHECK_MODE", ZSCHECK_NO_CHECK);
	mcd_log_msg(160280, PLAT_LOG_LEVEL_INFO, "ZS_CHECK_MODE = %d.\n", __zs_check_mode_on);

    // Pick up the max number of containers property. Should only do this on reformat!!!! FIXME
    // Allow for internal ZS containers, but limit to the absolute max.
    max_num_containers = getProperty_Int("ZS_MAX_NUM_CONTAINERS", MCD_MAX_NUM_CNTRS) + MCD_NUM_INTERNAL_CNTRS;
    if (max_num_containers > MCD_MAX_NUM_CNTRS)
        max_num_containers = MCD_MAX_NUM_CNTRS;

    mcd_log_msg(150133, PLAT_LOG_LEVEL_INFO, "max number of containers = %d.\n", max_num_containers);

    //  Initialize a crap-load of settings
    zs_load_settings( &(agent_state.flash_settings) );
    if (zs_check_settings(&(agent_state.flash_settings)) == false) {
        return ZS_FAILURE;
    } 

    // Configure cguid counter to by pass ZS internal containers
    state->config.cguid_counter = LAST_INTERNAL_CGUID + 1;
    
    // Initialize the container metadata map
    if ( ZS_SUCCESS != zs_cmap_init() )
        return ZS_FAILURE;

    mcd_aio_register_ops();
    mcd_osd_register_ops();

    //  Set the logging level
    set_log_level( agent_state.flash_settings.sdf_log_level );
    agent_config_set_defaults(& (agent_state.config));
    /* Set the log file if configured*/
    strncpy(log_file,getProperty_String("ZS_LOG_FILE",log_file),sizeof(log_file)-1);
    if( strcmp(log_file,"") ) { 
        plat_log_set_file(log_file, PLAT_LOG_REDIRECT_STDERR|PLAT_LOG_REDIRECT_STDOUT);
    } 

#ifdef ZS_REVISION
    plat_log_msg(160146, LOG_CAT, LOG_INFO,
            "Initializing %s (Rev:%s API version:%u)", ZS_PRODUCT_NAME, ZS_REVISION, ZS_API_VERSION);
#endif
    if ( prop_file != NULL ) {
        plat_log_msg(80032, LOG_CAT, LOG_INFO, "Property file: %s",prop_file);
        log_properties_file(prop_file,LOG_INFO);
    }
    print_configuration(LOG_INFO);
    if ( !agent_engine_pre_init( &agent_state, 0, NULL ) ) {
        return ZS_FAILURE; 
    }

#ifdef FLIP_ENABLED
    flip_init();
#endif
    if (getProperty_Int( "ZS_SIGNAL_HANDLERS", 0) == 1 ) {
        /* Initialize signal handler */
        signal(SIGSEGV, zs_signal_handler);  
        signal(SIGABRT, zs_signal_handler);    
        signal(SIGBUS, zs_signal_handler);     
        signal(SIGFPE, zs_signal_handler); 
    }

    if (getProperty_Int( "ZS_CORE_DUMP", 1) == 1) {
		zs_dump_core = 1;	
    }

    // spawn initer thread (like mcd_fth_initer)
    fthResume( fthSpawn( &zs_fth_initer, MCD_FTH_STACKSIZE ),
            (uint64_t) &agent_state );
    //Start License daemon
    if (!licd_start(getProperty_String("ZS_LICENSE_PATH", ZS_LICENSE_PATH),
                getProperty_Int("ZS_LICENSE_CHECK_PERIOD", ZS_LICENSE_CHECK_PERIOD),
                *zs_state)) {
        mcd_log_msg(160147, PLAT_LOG_LEVEL_FATAL, 
                "Creation of license daemon failed\n");
        return ZS_FAILURE;
    }

    //ipf_set_active( 1 );

    // spawn scheduler startup process
    pthread_attr_init( &attr );
    pthread_attr_setdetachstate( &attr, PTHREAD_CREATE_JOINABLE );

    num_sched = agent_state.flash_settings.num_cores;
    rc = pthread_create( &run_sched_pthread, 
            &attr, 
            zs_run_schedulers,
            (void *) num_sched );
    if ( 0 != rc ) {
        mcd_log_msg( 20163, 
                PLAT_LOG_LEVEL_FATAL,
                "pthread_create() failed, rc=%d", 
                rc );
        return rc;
    }
    mcd_log_msg( 150022, PLAT_LOG_LEVEL_TRACE, "scheduler startup process created" );


    wait_for_licd_start();
    if (is_license_valid(false) == false) {
        plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
        return ZS_LICENSE_CHK_FAILED;
    }	

    // Wait until mcd_fth_initer is done
    do {
        rc = sem_wait( &Mcd_initer_sem );
    } while (rc == -1 && errno == EINTR);

    plat_assert( 0 == rc );
    // Only run stats thread for normal run mode (not zsck)
    if ( ZSCHECK_NO_CHECK == mcd_check_get_level() ) {
        if ( getProperty_String("ZS_STATS_FILE","")[0] )
        {
            zs_start_stats_thread( *zs_state );
        }
        if ( getProperty_Int( "ZS_ADMIN_ENABLED", 1 ) == 1 ) {
            zs_start_admin_thread(*zs_state );
        }
        if ( getProperty_Int( "ASYNC_DELETE_CONTAINERS",0) == 1 ) {
            time((time_t *)&delete_prefix);
            init_async_cmd_handler(getProperty_Int("ASYNC_DELETE_CONTAINERS_THREADS",5),*zs_state);
        }
    }

	zs_start_vc_thread ( *zs_state );

	shard_t *shard; 
	SDF_action_init_t *pai = (SDF_action_init_t *) zs_state; 
	int s = cguid_to_shard(pai, VDC_CGUID, &shard, 0); 
	if (s != ZS_SUCCESS) 
		return s; 
	set_cntr_sizes((SDF_action_init_t *) zs_state, shard);
        mcd_osd_shard_t *mcd_shard;
        mcd_shard = (mcd_osd_shard_t *)shard;
        if ( is_btree_loaded() ) {
            ext_cbs->flash_stats_buf_cb( &(mcd_shard->blk_allocated), 
                 &(mcd_shard->free_segments_count), &(mcd_shard->blk_consumed), 
                 &mcd_shard->hash_handle->hash_size, &mcd_shard->hash_handle->alloc_count, 
                 Mcd_osd_blk_size, Mcd_osd_segment_size);
           ext_cbs->zs_funcs_cb((void *)plat_log_msg_helper);
        }

    if ( getProperty_Int( "ZS_EXPIRY_SCAVENGER_ENABLE", 1 ) == 1 ) {
        mcd_log_msg(160234, PLAT_LOG_LEVEL_DEBUG, 
                "expired object scavenging enabled.");
        zs_start_scavenger_thread( *zs_state );
    } else {
        mcd_log_msg(160235, PLAT_LOG_LEVEL_DEBUG,
                "expired object scavenging disabled.");
    }

    ZS_status_t zs_start_defrag_thread(struct ZS_state *);
    if ( getProperty_Int( "ZS_DEFRAG_ENABLE", 0) == 1 ) {
        mcd_log_msg(160291, PLAT_LOG_LEVEL_DEBUG,
                    "defragmenter enabled.\n");
        zs_start_defrag_thread(*zs_state );
    }

#if 0
    /*
     * automatically add MCD_MAX_NUM_CNTRS to the maximum recovered
     * container id to avoid potential reuse of ids from containers
     * that are partially deleted 
     */
    uint64_t cntr_id = 0; 
    for ( i = 0; i < max_num_containers; i++ ) {
        if (Mcd_containers[i] == NULL) {
	        continue;
	    }

	    SDF_container_props_t sdf_properties;
		SDF_status_t props_rc = ZSGetContainerProps( fth_state->pai,
													  pMcd_containers[i]->cguid,
	                                                  &sdf_properties );
		if ( ZS_SUCCESS != props_rc ) {
			mcd_log_msg( 50030, PLAT_LOG_LEVEL_ERROR,
	                     "failed to get SDF properties, status=%s",
	                     ZSStrError(props_rc) );
	        plat_abort();
		}

		if ( cntr_id < sdf_properties->container_id.container_id) {   
			cntr_id = sdf_properties->container_id.container_id;
        }
    }
    
    Mcd_next_cntr_id = cntr_id + max_num_containers + 1;
    mcd_log_msg( 20131, PLAT_LOG_LEVEL_TRACE,
                 "next container id %lu", Mcd_next_cntr_id );
#endif

	Force_async_writes  = getProperty_Int("FORCE_ASYNC_WRITES", 0);
	Enable_async_writes = getProperty_Int("ENABLE_ASYNC_WRITES", 0);
        /* Disabling Async writes permanently for now due to the following reasons
         * Bugs
         * we do not need for Cassandra layer */
	if (Force_async_writes)
		mcd_log_msg( 170018, PLAT_LOG_LEVEL_INFO, "property FORCE_ASYNC_WRITES overridden to 0");
	if (Enable_async_writes)
		mcd_log_msg( 170019, PLAT_LOG_LEVEL_INFO, "property ENABLE_ASYNC_WRITES overridden to 0");

	Force_async_writes  = 0;
	Enable_async_writes = 0;

	if ((agent_state.flash_settings.chksum_object)
	&& (sse42_present == 0)) {
		agent_state.flash_settings.chksum_object = 0;
		mcd_log_msg( 170040, PLAT_LOG_LEVEL_ERROR, "No x86 SSE4.2 support, ZS_OBJECT_CHECKSUM disabled");
	}

	if (is_btree_loaded()) {
		uint64_t rawobjsz;
		if (! get_rawobjsz( &rawobjsz))
			return (ZS_FAILURE);
		rawobjratio = get_rawobjratio();
		ext_cbs->zs_raw_cb(storm_mode, rawobjsz, rawobjratio);
	}
	return (lc_init( *zs_state, agent_state.config.system_recovery==SYS_FLASH_REFORMAT));
}

char *zs_init_per_thd_comp_buf(size_t len) {
	size_t tmp_len = len;

	if( getProperty_Int("ZS_COMPRESSION", 0) == 0 ) {
		return NULL;
	}

	if (len == 0) {
		tmp_len = snappy_max_compressed_length(
						getProperty_Int("ZS_COMPRESSION_MAX_OBJECT_SIZE",
							getProperty_Int("ZS_BTREE_NODE_SIZE", BTREE_MAX_NODE_SIZE) + 1024));
		if (storm_mode) {
			size_t raw_obj_len = get_rawobj_size() + 1024;
			if (tmp_len < raw_obj_len) {
				tmp_len = raw_obj_len;
			}
		}
	} else {
		tmp_len = snappy_max_compressed_length(len + 1024);
	}

	compression_buf = plat_alloc(tmp_len);
	if (compression_buf) {
		compression_buf_len = tmp_len;
	}
	return(compression_buf);
}

char *zs_get_per_thd_comp_buf(size_t len) {

	if (compression_buf_len == 0) {
		zs_init_per_thd_comp_buf(0);
	}

   if( compression_buf_len < len) {
	   if (compression_buf) {
		   plat_free(compression_buf);
	   }
	  compression_buf = plat_alloc(len);
   }
   if( compression_buf == NULL ) {
       plat_log_msg(160071,LOG_CAT,LOG_ERR,
                                   "Memory allocation failed");
	   compression_buf_len = 0;
       return NULL;
   }
   compression_buf_len = len;
   return compression_buf;
}

char *zs_compress_data(char *src, size_t src_len, 
                            char *comp_buf, size_t memsz, size_t *comp_len) {
	char *cbuf;
	snappy_status rc;
	size_t len;
	size_t btree_node_size = snappy_max_compressed_length(
			 							getProperty_Int("ZS_COMPRESSION_MAX_OBJECT_SIZE", 
											getProperty_Int("ZS_BTREE_NODE_SIZE", BTREE_MAX_NODE_SIZE)));

	if( getProperty_Int("ZS_COMPRESSION", 0) == 0 ) {
		return NULL;
	}

	 len = snappy_max_compressed_length(src_len);
     cbuf = comp_buf;
     if( cbuf == NULL ) {
         /* Use per thread compression buffer */
		 if (len < btree_node_size) { 
			 len = btree_node_size;
		 }
         cbuf = zs_get_per_thd_comp_buf(len); /* initializes per thread compression buffer */
		 *comp_len = compression_buf_len;
     } else {
		 if (len > memsz) {
			 plat_log_msg(160230, LOG_CAT, LOG_ERR,
					 "Compression buffer size passed is not sufficient len:%d memsz:%d", (int)len, (int)memsz);
			 return NULL;
		 }
		 *comp_len = memsz;
	 }

     if ( cbuf == NULL ) {
        /* No buffer available to put compressed data. return error*/
        plat_log_msg(160194,LOG_CAT,LOG_ERR,"Unable to get temporary buffer for compression ");
        return NULL;
     }

     rc = snappy_compress(src, src_len, cbuf, comp_len);
     if ( rc != SNAPPY_OK ) {
         plat_log_msg(160195,LOG_CAT,LOG_ERR,"Snappy compression failed(%d)\n",rc);
         return NULL;
     }
     return cbuf; 
}

/*
 * Uncompress the data compressed by snappy
 *
 * data		IN/OUT	pointer having compressed data and will have uncompressed data
 * datalen	IN		size of compressed data
 * memsz	IN		amount of memory pointed by data
 * uncomp_len OUT	size of uncompressed data/
 *
 * IMPORTANT: Make sure space allocated in data (of size memsz), is big enough
 * to store uncompressed data. snappy could increase the size of data instead of reducing it.
 *
 */

int zs_uncompress_data(char *data, size_t datalen, size_t memsz, size_t *uncomp_len) {
	size_t len;
    snappy_status rc;

	if ((rc = snappy_uncompressed_length(data, datalen, &len)) != SNAPPY_OK) {
		plat_log_msg(160231,LOG_CAT,LOG_FATAL,
				"Could not get size of uncompressed data(%d)", rc);
		return -1;
	}

	/* Memory allocated is not sufficient to store uncompressed data */
	if (len > memsz) {
        plat_log_msg(160232,LOG_CAT,LOG_FATAL,
                     "Data buffer not enough to hold uncompressed data memsz:%d len:%d",
                     (int)memsz,(int)len);
        return -1;
	}

	len = memsz;

    /* Get per thread comp buffer to store compresssed data*/
	if (zs_get_per_thd_comp_buf(datalen) == NULL) {
		plat_log_msg(160233, LOG_CAT,LOG_FATAL,
				"Uncompression failed, couldn't allocate buffer");
	}
	plat_assert(compression_buf != NULL);

    if ( datalen > compression_buf_len ) {
        plat_log_msg(160196,LOG_CAT,LOG_FATAL,
                     "Compression buffer not enough to hold data datalen:%d  comp_buf_len:%d",
                     (int)datalen,(int)compression_buf_len);
        return -1;
    }
    memcpy(compression_buf, data, datalen);

    rc = snappy_uncompress(compression_buf, datalen, data, &len);

    if( rc != SNAPPY_OK ) {
        plat_log_msg(160197,LOG_CAT,LOG_FATAL,
                      "Uncompression failed(%d)",rc);
        return -1;
    }
	/* len will have true size of uncompressed data now */
	*uncomp_len = len;
    return (int)*uncomp_len;
}

ZS_status_t ZSInitPerThreadState(
	struct ZS_state 		 *zs_state,
    struct ZS_thread_state **thd_state
	)
{
    SDF_action_init_t       *pai;
    SDF_action_init_t       *pai_new;
    SDF_action_thrd_state_t *pts;
	int						shutdown_thread = 0;

    if ( !zs_state ) {
        plat_log_msg(80052,LOG_CAT,LOG_DBG,
                             "ZS state is NULL");
        return ZS_INVALID_PARAMETER;
    }
    struct sdf_agent_state    *state = &agent_state;

	/* Thread already initialized or failed*/
	if (agent_state.op_access.shutdown_thread == fthSelf()) {
		shutdown_thread = 1;
	}
    if (fthSpawnPthread(shutdown_thread) == NULL) {
		return ZS_FAILURE;
	}

    pai = &state->ActionInitState;

    // Allocate the action init state
    pai_new = (SDF_action_init_t *) plat_alloc( sizeof( SDF_action_init_t ) );
    plat_assert_always( NULL != pai_new );
	bzero(pai_new, sizeof( SDF_action_init_t )); 
    // Fill in the action stats structures
    pai_new->pcs = allocate_action_state();
    plat_assert_always(NULL != pai_new->pcs);
    memcpy( pai_new, pai, (size_t) sizeof( SDF_action_init_t ));

    // Allocate the action thread state
    pts = (SDF_action_thrd_state_t *)
          plat_alloc( sizeof( SDF_action_thrd_state_t ) );
    plat_assert_always( NULL != pts );
    // Fill in action init state from above allocation
    pts->phs = state->ActionInitState.pcs;
    // There are two SDF_action_init_t structs in this record...
    // doesn't appear that they are used...CHECK THIS!!!
    bzero((void *) &pts->ai_struct, sizeof(SDF_action_init_t));
    pts->pai = NULL;

    /*
     * Mark the thread context free.
     */
    pts->ctxt_in_use = false;

    pai_new->pts = (void *) pts;
    InitActionAgentPerThreadState( pai_new->pcs, pts, pai_new );
    pai_new->paio_ctxt = pts->pai->paio_ctxt;

    pai_new->ctxt = ActionGetContext( pts );

    *thd_state = (struct ZS_thread_state *) pai_new;
    /* Initialize per thread compression/decompression buffer */
    zs_init_per_thd_comp_buf(0);

    if (trx_bracket_slabs == NULL) {
        trx_bracket_slabs = plat_alloc(MAX_TRX_BRACKET_SLAB_CNT * sizeof(uint64_t));

        if (trx_bracket_slabs == NULL) {
            mcd_log_msg(20001, PLAT_LOG_LEVEL_FATAL, "failed to allocate memory");
            plat_abort( );
        }
    }

    return ZS_SUCCESS;
}

ZS_status_t ZSReleasePerThreadState(
    struct ZS_thread_state **thd_state
    )
{
	SDF_action_thrd_state_t *pts, *pts_check;
	SDF_action_init_t *pai;
	SDF_action_state_t	*pcs;
	fthWaitEl_t              *wait;

	if (NULL == *thd_state) {
    	plat_log_msg(160098, LOG_CAT, LOG_ERR, 
                     "Thread state is null");
		return ZS_FAILURE;
	}
	/* Thread state already freed, application issue, single thread 
	 * releasing thread context multiple times. */
	if (NULL == fthSelf()) {
    	plat_log_msg(160186, LOG_CAT, LOG_ERR, 
                     "Thread state already released");
		return ZS_FAILURE;
	}
	pai = (SDF_action_init_t *)*thd_state;
	pts = (SDF_action_thrd_state_t *)pai->pts;
	if( pts != NULL ) {
		wait = fthLock(&(pts->phs->context_lock), 1, NULL);
		(pts->phs->contextcount)--;
		fthUnlock(wait);
	}
	ssdaio_free_ctxt(pts->pai->paio_ctxt, SSD_AIO_CTXT_ACTION_INIT);
	pcs = pai->pcs;
    wait = fthLock(&(pcs->nthrds_lock), 1, NULL);
	if (pcs->threadstates == pts) {
		pcs->threadstates = pts->next;
	} else {
		pts_check = pcs->threadstates;
		while (pts_check->next != pts) {
			pts_check = pts_check->next;
			plat_assert(pts_check);
		}
		if (pts_check) {
			pts_check->next = pts->next;
		}
	}
	pts->next = NULL;
    fthUnlock(wait);
    destroy_per_thread_state(pts);
    if ( pts->free_shard_map_entries_alloc_ptr != NULL ) {
        plat_free(pts->free_shard_map_entries_alloc_ptr);
    }
    if ( pts->pai->paio_ctxt != NULL ) {
        //fprintf(stderr,"freeing up paio_ctxt\n");  
        plat_free(pts->pai->paio_ctxt); 
    }
    if( pts->shardmap.buckets != NULL ) {
		SDFTLMap2Destroy(&(pts->shardmap));
        plat_free(pts->shardmap.buckets);
		pts->shardmap.buckets = NULL;
    }
    plat_free(pts);

	fthReleasePthread();
    plat_free( *thd_state );
    *thd_state = NULL;
    /* Free compression buffer */
    if( getProperty_Int("ZS_COMPRESSION", 0) ) {
        if ( compression_buf != NULL ) {
            plat_free(compression_buf);
        }
        compression_buf = NULL;
    }

    if (trx_bracket_slabs) {
        plat_free(trx_bracket_slabs);
        trx_bracket_slabs = NULL;
    }

    return ZS_SUCCESS;
}


/**
 * @brief Wait on asynchronous container deletion
 * @return None
 */
static void
zs_wait_containers_delete()
{
	uint32_t num_deletes = 0;
	uint32_t num_prog = 0;

	if (1 == getProperty_Int("ASYNC_DELETE_CONTAINERS", 0)) {

		/*
		 * Get current count for deletion operation
		 */
		get_async_delete_stats(&num_deletes, &num_prog);
        if ( num_deletes == 0 ) {
            /* No Deletes under progress. Just return */
            return;
        }

		plat_log_msg(80021,
				PLAT_LOG_CAT_SDF_PROT,
				PLAT_LOG_LEVEL_INFO,
				"Waiting for %d container deletes to finish",
				num_deletes);
		/*
		 * Blocking wait for container deletion
		 */
		wait_for_container_del();

		/*
		 * Update num_deletes. It should be zero.
		 */
		get_async_delete_stats(&num_deletes, &num_prog);

		plat_log_msg(160119,
				PLAT_LOG_CAT_SDF_PROT,
				PLAT_LOG_LEVEL_DEBUG,
				"End: Waiting for %d containers deletes to finish",
				num_deletes);
	}
}


/*
 * The phase one of ZS shutdown process
 * @param [in] Pointer to ZS state object
 * @retval ZS_status_t, ZS_SUCCES for success. Any other value indicate a failure.
 */
static ZS_status_t
zs_containers_cleanup(struct ZS_state *zs_state)
{
	plat_log_msg(160069, LOG_CAT, LOG_DBG, "%p", zs_state);

	struct ZS_thread_state *zs_thread_state = NULL;
	ZS_cguid_t *cguids = NULL;
	ZS_container_props_t props = {0};
	SDF_internal_ctxt_t *pai = NULL;

	ZS_status_t status = ZS_SUCCESS;
	SDF_status_t props_rc = ZS_SUCCESS;

	uint32_t n_cguids = 0;
	uint32_t i = 0;
	uint32_t num_closed_containers = 0;
	bool serialized = false;

	/*
	 * If container deletion operations are in progress, let them
	 * complete.
   	 */
	zs_wait_containers_delete();

	status = ZSInitPerThreadState((struct ZS_state*) zs_state,
			(struct ZS_thread_state **) &zs_thread_state);

	if ( ZS_SUCCESS != status)	{
		plat_log_msg(160100, LOG_CAT, LOG_ERR, 
				"Unable to create thread context %s",
				ZS_Status_Strings[status]);
		return status;
	}
	cguids = (ZS_cguid_t *) plat_alloc(sizeof(*cguids) * max_num_containers);
	if (cguids == NULL) {
		goto out;
	}
	memset(cguids, 0, sizeof(*cguids) * max_num_containers);

	pai = (SDF_internal_ctxt_t *) zs_thread_state;
	SDFStartSerializeContainerOp(pai);
        serialized = true;

	/*
	 * We will get all containers but physical containers (CMC, VMC and VDC).
	 * This API do _not_ list a container that's marked for deletion.
	 */
	status = zs_get_open_containers_int(zs_thread_state, cguids, &n_cguids);

	if (0 == n_cguids) {
		plat_log_msg(160055, LOG_CAT, LOG_DBG, 
				"No container exists");
		goto out;
	}

	plat_log_msg(160101, LOG_CAT, LOG_DBG, 
			"Total containers = %d", n_cguids);

	for (i = 0; i < n_cguids; i++) {
		props_rc = zs_get_container_props_int(zs_thread_state, cguids[i], &props);
		if (SDF_SUCCESS != props_rc) {
			plat_log_msg(160102, LOG_CAT, LOG_DBG, 
					"Error getting container properties for index=%d cguid=%ld: %s",
					i, cguids[i], ZS_Status_Strings[status]);

			/*
			 * Carry on with remaining containers
			 */
			continue;
		}
		plat_log_msg(160103, LOG_CAT, LOG_DBG, 
				"Got container properties for index=%d cguid=%ld: %s",
				i, cguids[i], ZS_Status_Strings[status]);

		/*
		 * Close the container
		 */
		status = zs_close_container(zs_thread_state,
					     cguids[i],
					     ZS_VIRTUAL_CNTR,
			                     ZS_FALSE, //No need to serialize
		                             ZS_TRUE);

		if (ZS_SUCCESS != status) {
			plat_log_msg(160104, LOG_CAT, LOG_DBG, 
					"Error closing container ID: %ld with %s",
					cguids[i], ZS_Status_Strings[status]);
		}

       plat_log_msg(160106, LOG_CAT,
               LOG_DBG, "Closed %d containers", num_closed_containers);


		num_closed_containers++;
	}

	if (0 == num_closed_containers) {
		plat_log_msg(20819, LOG_CAT,
				LOG_DBG, "%s", "No container closed.");
	} else {
		plat_log_msg(160120, LOG_CAT,
				LOG_DBG, "Containers closed=%d", num_closed_containers);
	}

out:
	zs_close_container( zs_thread_state, VDC_CGUID, ZS_PHYSICAL_CNTR, ZS_FALSE, ZS_TRUE);
	zs_close_container( zs_thread_state, VMC_CGUID, ZS_PHYSICAL_CNTR, ZS_FALSE, ZS_TRUE);

	if (true == serialized) {
	    SDFEndSerializeContainerOp( pai );
	}
	if (cguids) {
		plat_free(cguids);
	}
	ZSReleasePerThreadState(&zs_thread_state);

	return status;
}

/*
 * Gets the list of open containers
 */
static ZS_status_t
zs_get_open_containers_int(
                struct ZS_thread_state *zs_thread_state,
                ZS_cguid_t             *cguids,
                uint32_t                *n_cguids)
{
		int                   n_containers = 0;
        char                 *key          = NULL;
        uint32_t              keylen       = 0;
        cntr_map_t           *cmap         = NULL;
        uint64_t              cmaplen      = 0;
        struct cmap_iterator *iterator     = NULL;

    if (!cguids || !n_cguids)
            return ZS_INVALID_PARAMETER;

        iterator = zs_cmap_enum();

        if ( !iterator )
            return ZS_FAILURE;

        while ( zs_cmap_next_enum( iterator, &key, &keylen, (char **) &cmap, &cmaplen ) ) {
                if ( cmap->cguid > LAST_PHYSICAL_CGUID  &&
             strcmp( cmap->cname,SEQNO_CONTAINER_NAME ) &&
             strcmp( cmap->cname,PSTATS_CONTAINER_NAME ) &&
                         ( cmap->state == ZS_CONTAINER_STATE_OPEN )  ) {
                        cguids[n_containers] = cmap->cguid;
            n_containers++;
        }
    }

    zs_cmap_finish_enum( iterator );
    *n_cguids = n_containers;
    return ZS_SUCCESS;
}

ZS_status_t zs_stop_defrag_thread();

/*
 * The ZS shutdown process. This function should get called once.
 * @param [in] Pointer to ZS state object
 * @retval ZS_status_t, ZS_SUCCES for success. Any other value
 *         indicates a failure.
 */
ZS_status_t ZSShutdown(struct ZS_state *zs_state)
{
        if ( !zs_state ) {
            plat_log_msg(80052,LOG_CAT,LOG_DBG,
                           "ZS state is NULL");
            return ZS_INVALID_PARAMETER;
        }

	ZS_status_t status = ZS_SUCCESS;

	if (1 == getProperty_Int("GRACEFUL_SHUTDOWN", 1)) {
		plat_log_msg(20819, PLAT_LOG_CAT_SDF_PROT,
				PLAT_LOG_LEVEL_DEBUG, "%s", "Starting graceful shutdown");

        /*
         * stop scavenger thread.
         */
        zs_stop_scavenger_thread();
        zs_stop_defrag_thread();

		/*
		 * Mark shutdown in progress
		 */
		if (atomic_cmp_swap(agent_state.op_access.is_shutdown_in_progress,
					SDF_FALSE,
					SDF_TRUE)) {
			plat_log_msg(20819, PLAT_LOG_CAT_SDF_PROT,
					PLAT_LOG_LEVEL_DEBUG, "%s", "Another shutdown is in progress");

			return ZS_FAILURE;
		}
		agent_state.op_access.shutdown_thread = fthSelf();

		/*
		 * Phase 1: Process containers
		 */
		plat_log_msg(20819, PLAT_LOG_CAT_SDF_PROT,
                    PLAT_LOG_LEVEL_DEBUG, "%s", "Closing containers");

		status = zs_containers_cleanup(zs_state);

		plat_log_msg(160107, PLAT_LOG_CAT_SDF_PROT,
				PLAT_LOG_LEVEL_DEBUG, "Shutdown phase 1 returns :%s",
				ZS_Status_Strings[status]);
	}

	/* This hack should be remove or wrapped with #if 0 in release code */
	if (getProperty_Int("ZS_TEST_MODE", 0)) {
		char temp[PATH_MAX + 1];
		char *log_flush_dir = (char *)getProperty_String("ZS_LOG_FLUSH_DIR", NULL);
		snprintf(temp, sizeof(temp), "rm -rf %s/zs_%d", log_flush_dir, zs_instance_id);
		ignore(system(temp));
	}
    plat_log_msg(80033, PLAT_LOG_CAT_SDF_PROT,
                         PLAT_LOG_LEVEL_DEBUG, "Shutdown completed");
       return status;
}


ZS_status_t ZSLoadCntrPropDefaults(
		ZS_container_props_t *props
		)
{
	props->size_kb = ZS_DEFAULT_CONTAINER_SIZE_KB;
	props->fifo_mode = ZS_FALSE;
	props->persistent = ZS_TRUE; 
	props->evicting = ZS_FALSE;
	props->writethru = ZS_TRUE;
	props->async_writes = SDF_FALSE;
	props->durability_level = ZS_DURABILITY_SW_CRASH_SAFE;
	props->cguid = 0;
	props->cid = 0;
	props->num_shards = 1;
	props->flash_only = ZS_FALSE;
	props->cache_only = ZS_FALSE;
	props->compression = ZS_FALSE;
    /*
     * By default all containers are Btree enabled
     */
	props->flags = 0;
	return ZS_SUCCESS;
}

#define CONTAINER_PENDING

//static uint64_t cid_counter = 0;
char *ZSStrError(ZS_status_t zs_errno) {
	if ( zs_errno >= N_ZS_STATUS_STRINGS ) {
		return ZS_Status_Strings[0];  
	}
	return ZS_Status_Strings[zs_errno]; 
}

ZS_status_t ZSOpenContainer(
		struct ZS_thread_state	*zs_thread_state, 
		char					*cname,
		ZS_container_props_t	*properties,
		uint32_t				 flags,
		ZS_cguid_t 	 	 	*cguid
		)
{
	ZS_status_t status		= ZS_SUCCESS;
	bool thd_ctx_locked = false;

	if( getProperty_Int("ZS_ENABLE_COMPRESSION_ALL_CONTAINERS", 1) && 
			getProperty_Int("ZS_COMPRESSION", 0) ) {
		if(properties) {
			properties->compression = ZS_TRUE;
		}
	}

	if (properties->durability_level == ZS_DURABILITY_PERIODIC) {
		plat_log_msg(180216, LOG_CAT, LOG_WARN, "PERIODIC durability is not supported, set to SW_CRASH_SAFE for %s\n", cname);
		properties->durability_level = ZS_DURABILITY_SW_CRASH_SAFE;
	}

	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
		plat_log_msg(160187, LOG_CAT,
				LOG_WARN, "Operation not allowed");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
	if ( !zs_thread_state || !cguid || ISEMPTY(cname)) {
		if ( !zs_thread_state ) {
			plat_log_msg(80049,LOG_CAT,LOG_DBG,
					"ZS Thread state is NULL");
		}
		if ( ISEMPTY(cname) ) {
			plat_log_msg(80053,LOG_CAT,LOG_DBG,
					"Invalid container name");
		}
		return ZS_INVALID_PARAMETER;
	}
	if( properties ) {
		plat_log_msg(160199,LOG_CAT, LOG_DBG, "Compression %s for Container %s",
				properties->compression?"enabled":"disabled",cname);
	}

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	if ( flags & ZS_CTNR_CREATE ) {
		uint64_t vdc_size = ((uint64_t)getProperty_uLongLong("ZS_FLASH_SIZE", ZS_MIN_FLASH_SIZE)) * 1024 * 1024 -
                                                                       (2 * ZS_DEFAULT_CONTAINER_SIZE_KB);
		if (!properties) {
            status = ZS_INVALID_PARAMETER;
            goto out;
		}
		if( properties->size_kb >  vdc_size) {
			plat_log_msg(80063, LOG_CAT, LOG_DBG,
                         "Container size %lu kb greater than the flash size %lu kb",
                              properties->size_kb,vdc_size);
			status =ZS_FAILURE_INVALID_CONTAINER_SIZE;
			goto out;
		}

		status = zs_create_container( zs_thread_state,
				cname,
				properties,
				flags,
				cguid,
				ZS_VIRTUAL_CNTR );
	}

    if ( (ZS_SUCCESS == status)) {
		status = zs_open_container( zs_thread_state,
				cname,
				properties,
				flags,
				cguid,
				ZS_VIRTUAL_CNTR,
				ZS_TRUE );
	}

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}

	return status;
}

ZS_status_t ZSOpenContainerSpecial(
	struct ZS_thread_state	  *zs_thread_state, 
	char                      *cname, 
	ZS_container_props_t     *properties, 
	uint32_t                  flags,
	ZS_container_meta_t      *cmeta,
	ZS_cguid_t               *cguid
	)
{
	return (ZSOpenContainer(zs_thread_state, cname, properties, flags, cguid));
}

ZS_status_t ZSOpenPhysicalContainer(
		struct ZS_thread_state *zs_thread_state,
		char                    *cname,
		ZS_container_props_t   *properties,
		uint32_t                 flags,
		ZS_cguid_t             *cguid
		)
{
	ZS_status_t status     = ZS_SUCCESS;

	if ( flags & ZS_CTNR_CREATE ) {
		status = zs_create_container( zs_thread_state,
				cname,
				properties,
				flags,
				cguid,
				ZS_PHYSICAL_CNTR );
	}

	if ( ZS_SUCCESS == status ) {
		status = zs_open_container( zs_thread_state,
				cname,
				properties,
				flags,
				cguid,
				ZS_PHYSICAL_CNTR,
				ZS_TRUE );
	}

	return status;
}

static ZS_status_t zs_create_container(
        struct ZS_thread_state    *zs_thread_state, 
        char                    *cname,
        ZS_container_props_t    *properties,
        uint32_t                 flags,
        ZS_cguid_t               *cguid,
        ZS_container_mode_t     mode
        )
{
	int                               i                             = 0;
    struct SDF_shared_state          *state                         = &sdf_shared_state;
    ZS_status_t                      status                        = ZS_FAILURE;
    SDF_shardid_t                     shardid                       = SDF_SHARDID_INVALID;
    SDF_container_meta_t             *meta                          = NULL;
    SDF_CONTAINER_PARENT              parent                        = containerParentNull;
    local_SDF_CONTAINER_PARENT        lparent                       = NULL;
    SDF_boolean_t                     isCMC                         = SDF_FALSE;
    uint32_t                          in_shard_count                = 0;
    uint64_t                          num_objs                      = 0;
    const char                       *writeback_enabled_string      = NULL;
    SDF_internal_ctxt_t              *pai                           = (SDF_internal_ctxt_t *) zs_thread_state;
    SDF_container_props_t            *sdf_properties                = NULL;
#if 0
	ZS_internal_container_props_t    iproperties;
#endif


	if ( !properties || !cguid || !zs_thread_state ||
			ISEMPTY( cname ) ) {
		return ZS_INVALID_PARAMETER;
	}

	*cguid = 0;

	properties->persistent			= SDF_TRUE;
	//properties->evicting			= SDF_FALSE;
	if (Force_async_writes)
		properties->async_writes = ZS_TRUE;
	else if (!Enable_async_writes)
		properties->async_writes = ZS_FALSE;
#if 0
	iproperties.current_size		= 0;
	iproperties.num_obj				= 0;
	iproperties.fifo_mode			= ZS_FALSE;
	iproperties.cguid				= 0;
	iproperties.num_shards 			= 1;  
	iproperties.async_writes		= ZS_FALSE;
#endif

	plat_log_msg(160033, LOG_CAT, LOG_DBG, "%s, size=%ld bytes", cname, (long)properties->size_kb * 1024);

    // Unlimited size containers must be store mode             
    if ( properties->size_kb == 0 && properties->evicting ) {
        plat_log_msg( 160144, LOG_CAT, LOG_ERR, "Unlimited containers must be non-evicting." );
        return ZS_UNLIMITED_CONTAINER_MUST_BE_NON_EVICTING;
    }

	if ( !properties->writethru ) {
		if ( !properties->evicting ) {
			plat_log_msg( 160061, LOG_CAT, LOG_DBG,
					"Using writeback caching with store mode containers can result in lost data if the system crashes" );
		} else {
			writeback_enabled_string = getProperty_String( "SDF_WRITEBACK_CACHE_SUPPORT", "On" );
			if ( strcmp( writeback_enabled_string, "On" ) != 0 ) {
				plat_log_msg( 30575, LOG_CAT, LOG_WARN,
						"Cannot enable writeback caching for container '%s' because writeback caching is disabled.",
						cname );

				properties->writethru = SDF_TRUE;
			}
		}
	}

#ifdef notdef
	// We only allow FIFO mode for evicting, non-persistent containers
	if ( properties->fifo_mode && ( !properties->evicting || properties->persistent ) ) {
		plat_log_msg( 150043, LOG_CAT, LOG_ERR,
				"FIFO mode is only allowed for evicting, non-persistent containers" );
		return ZS_FAILURE_INVALID_CONTAINER_TYPE;
	}
#endif /* notdef */

	SDFStartSerializeContainerOp( pai );

	if ( strcmp( cname, CMC_PATH ) == 0 ) {
		*cguid = CMC_CGUID;
		isCMC = SDF_TRUE;
	} else {
		for ( i = 1; i <= max_num_containers; i++ ) {
			if ( Mcd_containers[i].cguid == 0 ) { 
				// this is an unused map entry
				break;
			}
		}

		if ( i == max_num_containers ) {
			plat_log_msg(160301, 
					LOG_CAT,LOG_ERR, 
					"ZSCreateContainer failed for container %s because max containers have already been created.", 
					cname );
			status = ZS_TOO_MANY_CONTAINERS;
			goto out;
		}

		if ( ( status = zs_generate_cguid( zs_thread_state, cname, cguid ) ) != ZS_SUCCESS ) {
			plat_log_msg( 150084,
					LOG_CAT,
					LOG_ERR,
					"Failed to generate container id for %s", 
					cname );
			status = ZS_TOO_MANY_CONTAINERS;
			SDFEndSerializeContainerOp( pai );
			return status;
		}

        status = zs_cmap_create( cname, 
	                              *cguid, 
	                              properties->size_kb, 
	                              ZS_CONTAINER_STATE_CLOSED, 
#if 0//Rico - lc
	                              properties->evicting
#else
	                              properties->evicting,
	                              properties->flags&ZS_LOG_CTNR
#endif
	                            );

	    if ( ZS_SUCCESS != status ) {
                // See if this is a duplicate entry error
                if (status == ZS_FAILURE_CANNOT_CREATE_METADATA_CACHE)
                    status = ZS_CONTAINER_EXISTS;
		    goto out;
            }

		isCMC = SDF_FALSE;
		init_get_my_node_id();
	}

#if 0
	iproperties.cguid = *cguid;
#else
	properties->cguid = *cguid;
#endif

#if 0
	if ( (sdf_properties = zs_create_sdf_props( properties, &iproperties ) ) == NULL ) {
		*cguid = SDF_NULL_CGUID;
		status = ZS_FAILURE_MEMORY_ALLOC;
		goto out;
	}
#else
	if ( (sdf_properties = zs_create_sdf_props( properties ) ) == NULL ) {
		*cguid = SDF_NULL_CGUID;
		status = ZS_FAILURE_MEMORY_ALLOC;
		goto out;
	}
#endif
#if 0
	if (IS_ZS_LOG_CONTAINER(properties->flags)) {
		sdf_properties->container_type.type = SDF_LOG_CONTAINER;
	}
#endif

	/*
	 *  Save the cguid in a useful place so that the replication code in
	 *  mcd_ipf.c can find it.
	 */
#ifdef notdef
	if (cmc_settings != NULL) {
		struct settings *settings = cmc_settings;
		int  i;

		for (i = 0; i < sizeof(settings->vips) / sizeof(settings->vips[0]); i++) {
			if (sdf_properties->container_id.container_id ==
					settings->vips[i].container_id) {
				settings->vips[i].cguid = *cguid;
			}
		}
	}
#endif /* notdef */

	num_objs = sdf_properties->container_id.sc_num_objs;

#ifdef notdef
	/*
	 * XXX: Provide default replication parameters for non-CMC containers.
	 * It would be better for this to be a test program runtime option for
	 * default container properties.
	 *
	 * 1/6/09: Enabled CMC replication.
	 *
	 * XXX: Disabling CMC replication doesn't seem to work in the current
	 * code, perhaps because it's getting to the replication code with
	 * a type of SDF_REPLICATION_NONE?  Try to make the CMC available
	 * everywhere since it should be write once.
	 */
	if (/*!isCMC &&*/ state->config.always_replicate) {
		if (isCMC && state->config.replication_type != SDF_REPLICATION_SIMPLE) {
			sdf_properties->replication.num_replicas = state->config.nnodes;
			sdf_properties->replication.num_meta_replicas = 0;
			sdf_properties->replication.type = SDF_REPLICATION_SIMPLE;
		} else {
			sdf_properties->replication.num_replicas = state->config.always_replicate;
			sdf_properties->replication.num_meta_replicas = 1;
			sdf_properties->replication.type = state->config.replication_type;
		}

		sdf_properties->replication.enabled = 1;
		sdf_properties->replication.synchronous = 1;
		if( sdf_properties->replication.type == SDF_REPLICATION_V1_2_WAY ) {
			sdf_properties->replication.num_replicas = 2;
		}
	}
#endif /* notdef */

	/*
	   How do we set shard_count :
	   1) check if shard_count in the incoming Container properties is non-zero
	   2) else use the shard_count from the properties file (by incredibly
	   complicated maze of initialization ending up in state->config)
	   3) else  use the hard-coded SDF_SHARD_DEFAULT_SHARD_COUNT macro
	 */

	in_shard_count = sdf_properties->shard.num_shards?sdf_properties->shard.num_shards:
		(state->config.shard_count?
		 state->config.shard_count:SDF_SHARD_DEFAULT_SHARD_COUNT);

	/* XXX: If we reached here without having set the shard_count in
	   container properties, set the property here. In the future we
	   might want to assert on this condition.
	 */
	if ( sdf_properties->shard.num_shards == 0 ) {
		sdf_properties->shard.num_shards = in_shard_count;
	}

#ifdef MULTIPLE_FLASH_DEV_ENABLED
	plat_log_msg( 21527, LOG_CAT, LOG_DBG, "Container: %s - Multi Devs: %d",
			path, state->config.flash_dev_count );
#else
	plat_log_msg( 21528, LOG_CAT, LOG_DBG, "Container: %s - Single Dev",
			cname );
#endif
	plat_log_msg( 21529, LOG_CAT, LOG_DBG, "Container: %s - Num Shards: %d",
			cname, sdf_properties->shard.num_shards );

	plat_log_msg( 21530, LOG_CAT, LOG_DBG, "Container: %s - Num Objs: %d",
			cname, state->config.num_objs );

	plat_log_msg( 21531, LOG_CAT, LOG_DBG, "Container: %s - DEBUG_MULTI_SHARD_INDEX: %d",
			cname, getProperty_Int( "DEBUG_MULTISHARD_INDEX", -1 ) );


    // This check is no longer needed
	if ( 0 /* doesContainerExistInBackend( pai, cname ) */) {
#ifdef CONTAINER_PENDING
		// Unset parent delete flag if with deleted flag
		if ( !isContainerParentNull( parent = isParentContainerOpened( cname ) ) ) {
			local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent( &lparent, parent );

			if ( lparent->delete_pending == SDF_TRUE ) {
				if ( !isCMC && (status = name_service_lock_meta( pai, cname )) != ZS_SUCCESS ) {
					plat_log_msg( 21532, LOG_CAT, LOG_ERR, "failed to lock %s", cname );
				}

				lparent->delete_pending = SDF_FALSE;

				if ( !isCMC && (status = name_service_unlock_meta( pai, cname )) != ZS_SUCCESS ) {
					plat_log_msg( 21533, LOG_CAT, LOG_ERR, "failed to unlock %s", cname );
				}

			}

			releaseLocalContainerParent( &lparent ); // TODO C++ please!
		}
#endif
		status = ZS_CONTAINER_EXISTS;
	} else {

		if ( mode == ZS_PHYSICAL_CNTR ) {

			if ( ( shardid = build_shard( state, pai, cname, num_objs,
							in_shard_count, *sdf_properties, *cguid,
							isCMC ? BUILD_SHARD_CMC : BUILD_SHARD_OTHER, cname ) ) <= SDF_SHARDID_LIMIT ) {
				if ( VMC_CGUID == *cguid ) {
					vmc_shardid = shardid;
				} else if ( VDC_CGUID == *cguid ) {
					vdc_shardid = shardid;
				} else if (CMC_CGUID == *cguid ) {
					cmc_shardid = shardid;
				}
			}
		} else {
			shardid = vdc_shardid;
		}

		if ( shardid != SDF_SHARDID_INVALID ) {
			if ( (meta = build_meta( cname, *sdf_properties, (SDF_cguid_t) *cguid, shardid )) != NULL ) {
#ifdef STATE_MACHINE_SUPPORT
				SDFUpdateMetaClusterGroupInfo( pai, meta, sdf_properties->container_id.container_id );
#endif

                // Copy the FDF properties into the old SDF properties
                memcpy(&meta->zs_properties, properties, sizeof(ZS_container_props_t));

				if ( (status = create_put_meta( pai, cname, meta, (SDF_cguid_t) *cguid )) == SDF_SUCCESS ) {

					if ( !isCMC && (status = name_service_lock_meta( pai, cname )) != ZS_SUCCESS ) {
						plat_log_msg(21532, LOG_CAT, LOG_ERR, "failed to lock %s", cname);
					} else if ( !isContainerParentNull(parent = createParentContainer( pai, cname, meta )) ) {
						lparent = getLocalContainerParent( &lparent, parent ); // TODO C++ please!
#ifdef notdef
						lparent->container_type = sdf_properties->container_type.type;
						if ( lparent->container_type == SDF_BLOCK_CONTAINER ) {
							lparent->blockSize = sdf_properties->specific.block_props.blockSize;
						}
#endif
						releaseLocalContainerParent(&lparent); // TODO C++ please!

						status = ZS_SUCCESS;

						if ( !isCMC && (status = name_service_unlock_meta( pai, cname )) != ZS_SUCCESS ) {
							plat_log_msg( 21533, LOG_CAT, LOG_ERR, "failed to unlock %s", cname );
						}
					} else {
						plat_log_msg( 21535, LOG_CAT, LOG_ERR, "cname=%s, build_shard failed", cname );
					}
				} else {
					plat_log_msg( 21536, LOG_CAT, LOG_ERR, "cname=%s, createParentContainer() failed", cname );
				}

				container_meta_destroy( meta );

			} else {
				plat_log_msg( 21537, LOG_CAT, LOG_ERR, "cname=%s, build_meta failed", cname );
			}
		} else {
			plat_log_msg( 21535, LOG_CAT, LOG_ERR, "cname=%s, build_shard failed", cname );
			status = ZS_OUT_OF_STORAGE_SPACE;
		}
	}

	plat_log_msg( 21511, LOG_CAT, LOG_TRACE, "%s - %s", cname, ZSStrError(status) );

	if ( status != ZS_SUCCESS && status != ZS_CONTAINER_EXISTS ) {
		plat_log_msg( 21538, LOG_CAT, LOG_ERR, "cname=%s, function returned status = %u", cname, status );
		name_service_remove_meta( pai, cname );
#if 0
		/*
		 * XXX We're leaking the rest of the shard anyways, and this could
		 * cause dangling pointer problems to manifest from the coalesce, etc.
		 * internal flash threads so skip it.
		 */
		shardDelete( shard );
		xxxzzz continue from here
#endif
	}
#ifdef SIMPLE_REPLICATION
	/*
	   else if ( status == ZS_SUCCESS ) {
	   SDFRepDataStructAddContainer( pai, sdf_properties, *cguid );
	   }
	 */
#endif

	if ( ZS_SUCCESS == status && CMC_CGUID != *cguid ) {
		for ( i = 0; i < max_num_containers; i++ ) {
#ifdef SDFAPIONLY
	        if ( Mcd_containers[i].cguid == ZS_NULL_CGUID ) {
				Mcd_containers[i].cguid = *cguid;
				strcpy( Mcd_containers[i].cname, cname );
#endif /* SDfAPIONLY */
				break;
	        }
	    }
	}

out:
	if ( NULL != sdf_properties )
		plat_free ( sdf_properties );

	SDFEndSerializeContainerOp( pai );

	plat_log_msg(160034, LOG_CAT, LOG_DBG, "%s(cguid=%lu) - %s", cname, *cguid, ZSStrError(status));

	return status;
}

static ZS_status_t zs_open_container(
		struct ZS_thread_state	*zs_thread_state, 
		char					*cname,
		ZS_container_props_t	*props,
		uint32_t				 flags,
		ZS_cguid_t 	 	 	*cguid,
		ZS_container_mode_t	 mode,
		ZS_boolean_t			 serialize
		) 
{               
	ZS_status_t 				 status 	= ZS_SUCCESS;
	local_SDF_CONTAINER 		 lc 		= NULL;
	SDF_CONTAINER_PARENT 		 parent;
	local_SDF_CONTAINER_PARENT 	 lparent 	= NULL;
	int 						 log_level 	= LOG_ERR;
	int  						 i_ctnr 	= -1, i;
	SDF_CONTAINER 				 container 	= containerNull;
	SDF_internal_ctxt_t     	*pai 		= (SDF_internal_ctxt_t *) zs_thread_state;
#ifdef SDFAPIONLY
	mcd_osd_shard_t 			*mcd_shard	= NULL;
	struct shard				*shard		= NULL;
#endif /* SDFAPIONLY */
	SDF_container_meta_t		 meta;
	cntr_map_t                  *cmap       = NULL;
	ZS_cguid_t					tcguid = -1;

	if ( serialize )
		SDFStartSerializeContainerOp( pai );


	if ( !cguid ) {
		status = ZS_INVALID_PARAMETER;
		goto out;
	}

	if ( ISEMPTY( cname ) ) { 
		status = ZS_INVALID_PARAMETER;
		*cguid = ZS_NULL_CGUID;
		goto out;
	}

	plat_log_msg( 20819, LOG_CAT, LOG_DBG, "%s", cname);
	if ( props != NULL ) {
		plat_log_msg( 160200, LOG_CAT, LOG_DBG, "compression enabled %d", props->compression);
	}

	if ( strcmp( cname, CMC_PATH ) != 0 ) {
	    if ( NULL == ( cmap = zs_cmap_get_by_cname( cname ) ) ) {
	        status = ZS_CONTAINER_UNKNOWN;
	        *cguid = ZS_NULL_CGUID;
	        goto out;
	    }
	    *cguid = tcguid = cmap->cguid;
		for ( i = 0; i < max_num_containers; i++ ) {
			if (Mcd_containers[i].cguid == tcguid) {
				i_ctnr = i;
				break;
			}
		}
		plat_assert(i_ctnr != -1);
	} else {
		i_ctnr = 0;
		*cguid = tcguid = CMC_CGUID;
	}

	if ( strcmp( cname, CMC_PATH ) != 0 ) {
		if ( ZS_CONTAINER_STATE_OPEN == cmap->state || ZS_CONTAINER_STATE_DELETE_OPEN == cmap->state) {
			plat_log_msg( 160032, LOG_CAT, LOG_DBG, "Already opened or error: %s - %s", cname, ZSStrError(status) );
			goto out;
		}
	}

	if ( !isContainerParentNull( parent = isParentContainerOpened( cname ) ) ) {

		// Test for pending delete
		lparent = getLocalContainerParent( &lparent, parent );
		if ( lparent->delete_pending == SDF_TRUE ) {
			// Need a different error?
			status = SDF_CONTAINER_UNKNOWN;
			plat_log_msg( 21552, LOG_CAT,LOG_DBG, "Delete pending for %s", cname );
		} 
		releaseLocalContainerParent( &lparent );
	}

	if ( status == ZS_SUCCESS ) {

		// Ok to open
		container = openParentContainer( pai, cname );

		if ( isContainerNull( container ) ) {
			fprintf( stderr, "ZSOpenContainer: failed to open parent container for %s\n", cname );
		}

		if ( CMC_CGUID == *cguid ) {
			theCMC->c = internal_serverToClientContainer( container );
		} else {
			cmap->sdf_container = container;
			if ( ZS_CONTAINER_STATE_DELETE_PROG == cmap->state ) {
				if ( ZS_SUCCESS != ( status = zs_ctnr_set_state( cmap, ZS_CONTAINER_STATE_DELETE_OPEN ) ) ) 
					goto out;
			} else {
				if ( ZS_SUCCESS != ( status = zs_ctnr_set_state( cmap, ZS_CONTAINER_STATE_OPEN ) ) )
					goto out;
			}

			if (flags & ZS_CTNR_RO_MODE) {
				//zs_cntr_set_readonly(cmap);
				zs_cntr_set_readwrite(cmap);
			} else if (flags & ZS_CTNR_RW_MODE) {
				zs_cntr_set_readwrite(cmap);
			} else { /*default */
				zs_cntr_set_readwrite(cmap);
			}


		}

		if ( !isContainerNull( container ) ) {
			lc = getLocalContainer( &lc, container );
			lc->mode = SDF_READ_WRITE_MODE; // (container)->mode = mode;
			_sdf_print_container_descriptor( container );
			log_level = LOG_DBG;
			if ( mode == ZS_PHYSICAL_CNTR ) {
				if ( lc->cguid == VDC_CGUID ) {
					name_service_get_meta(pai, lc->cguid, &meta);
					vdc_shardid = meta.shard;
				} else if (lc->cguid == CMC_CGUID ) {
					name_service_get_meta(pai, lc->cguid, &meta);
					cmc_shardid = meta.shard;
				}
				// FIXME: This is where the call to shardOpen goes.
#define MAX_SHARDIDS 32 // Not sure what max is today
				SDF_shardid_t shardids[MAX_SHARDIDS];
				uint32_t shard_count;
				get_container_shards( pai, lc->cguid, shardids, MAX_SHARDIDS, &shard_count );
				for ( int i = 0; i < shard_count; i++ ) {
					struct SDF_shared_state *state = &sdf_shared_state;
					shardOpen( state->config.flash_dev, shardids[i] );
				}

			}

			status = SDFActionOpenContainer( pai, lc->cguid );
			if ( status != ZS_SUCCESS ) {
				//	plat_assert(0);
				plat_log_msg( 21554, LOG_CAT,LOG_ERR, "SDFActionOpenContainer failed for container %s", cname );
			}

#ifdef SDFAPIONLY
			if ( mode == ZS_PHYSICAL_CNTR && CMC_CGUID != *cguid ) {
				shard = container_to_shard( pai, lc );
				if ( NULL != shard ) {
					mcd_shard = (mcd_osd_shard_t *)shard;
					plat_assert(i_ctnr >= 0);
					mcd_shard->cntr = &Mcd_containers[i_ctnr];
					if( 1 == mcd_shard->persistent ) {
						shard_recover_phase2( mcd_shard );
					}
				} else {
					plat_log_msg( 150026, LOG_CAT,LOG_ERR, "Failed to find shard for %s", cname );
				}
			}
#endif /* SDFAPIONLY */

			releaseLocalContainer( &lc );
			plat_log_msg( 21555, LOG_CAT, LOG_TRACE, "Opened %s", cname );
		} else {
			status = SDF_CONTAINER_UNKNOWN;
			plat_log_msg( 21556, LOG_CAT,LOG_ERR, "Failed to find %s", cname );
		}
	}

	if ( cname ) {
		if ( status != ZS_SUCCESS ) {
			if( ZS_INVALID_PARAMETER == status ) {
				plat_log_msg(80034,LOG_CAT,LOG_DIAG,"Container %s does not exist",cname);
			}
			else {
				plat_log_msg( 21511, LOG_CAT, LOG_DIAG, "%s - %s", cname, ZSStrError(status) );
			}
		}
	} else {
		plat_log_msg( 150024, LOG_CAT, log_level, "NULL - %s", ZSStrError(status) );
	}

out:
	if ((status == ZS_SUCCESS)
	&& (cmap)
	&& (cmap->lc))
		status = lc_open( zs_thread_state, *cguid);
	if (serialize )
		SDFEndSerializeContainerOp( pai );
	return status;
}

ZS_status_t ZSCloseContainer(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t  		     cguid
	)
{
    //plat_assert(zs_thread_state);

	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;
	SDF_action_init_t  *pac     = NULL;
	SDF_cache_ctnr_metadata_t *meta;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}

	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
		plat_log_msg(160108, LOG_CAT,
				LOG_ERR, "is_zs_operation_allowed:%s",
				ZS_Status_Strings[status]);

		goto out;
	}
	if ( !zs_thread_state || !cguid ) {
		if ( !zs_thread_state ) {
			plat_log_msg(80049,LOG_CAT,LOG_DBG,
					"ZS Thread state is NULL");
		}
		if ( !cguid ) {
			plat_log_msg(80050,LOG_CAT,LOG_DBG,
					"Invalid container cguid:%lu",cguid);
		}
		return ZS_INVALID_PARAMETER;
	}

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
				LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	pac = (SDF_action_init_t *) zs_thread_state;

	meta = get_container_metadata(pac, cguid);
	if (meta == NULL) {
		status = ZS_FAILURE;
		goto out;
	}

	if (meta->meta.properties.flash_only == ZS_FALSE) {
        status = zs_flush_container(zs_thread_state,cguid);
        if( status != ZS_SUCCESS ) {
            plat_log_msg(150109,LOG_CAT,LOG_ERR,
                     "Failed to flush before closing the container %lu - %s",cguid, ZSStrError(status));
		}
    } else {
	    status = zs_close_container(zs_thread_state,
					    cguid,
					    ZS_VIRTUAL_CNTR,
					    ZS_TRUE,
					    ZS_TRUE);
    }


out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}

ZS_status_t ZSClosePhysicalContainer(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t  		     cguid
	)
{
	return zs_close_container( zs_thread_state,
								cguid,
								ZS_PHYSICAL_CNTR,
								ZS_TRUE,
                                ZS_TRUE
							  );
}

/*
 * This function must be called within SDFStartSerializeContainerOp
 */
static bool 
is_container_being_deleted(ZS_cguid_t cguid)
{
	cntr_map_t *cmap = NULL;
	bool ret = ZS_TRUE;

	/*
	 * Check if the cguid is present in cmap. If not,
	 * the container for given cguid has been deleted.
	 */
	cmap = zs_cmap_get_by_cguid(cguid);

	if ( !cmap ) {
		plat_log_msg(160138, LOG_CAT, LOG_DIAG,
				"Container %lu is not found", cguid);
		return ret;
	}

	/*
	 * Check if container is being deleted, if so return error
	 */
	if ( ZS_CONTAINER_STATE_DELETE_PROG == cmap->state ||
		 ZS_CONTAINER_STATE_DELETE_OPEN == cmap->state ||
		 ZS_CONTAINER_STATE_DELETE_CLOSED == cmap->state ) {
		plat_log_msg(160139, LOG_CAT, LOG_DIAG,
				"Container %lu deletion is in progress", cguid);

		return ret;
	}

	/*
	 * Container is valid
	 */
    ret = ZS_FALSE;

	return ret;
}

static ZS_status_t zs_close_container(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t  		     cguid,
	ZS_container_mode_t	 mode,
	ZS_boolean_t			 serialize,
	ZS_boolean_t			 delete_check
	)
{
#ifdef SDFAPIONLY
    struct shard			*shard			= NULL;
    flashDev_t              *flash_dev;
    SDF_container_meta_t     meta;
    ZS_status_t			 tmp_status;
    struct SDF_shared_state *state			= &sdf_shared_state;
#endif
    ZS_status_t			 status			= SDF_FAILURE;
    SDF_CONTAINER			 container		= containerNull;
    SDF_internal_ctxt_t     *pai			= (SDF_internal_ctxt_t *) zs_thread_state;
    int						 ok_to_delete	= 0;

    plat_log_msg( 21630, LOG_CAT, LOG_DBG, "%lu", cguid);

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status && !agent_state.op_access.is_shutdown_in_progress) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		return status;
	}
	if ( !cguid )
		return ZS_INVALID_PARAMETER;

	if ( serialize )
	    SDFStartSerializeContainerOp(pai);

	cntr_map_t *cmap = zs_cmap_get_by_cguid( cguid );

	if ( !cmap ) { 
	    if ( serialize )
	        SDFEndSerializeContainerOp(pai);
	    return ZS_FAILURE_CONTAINER_NOT_FOUND;
	}
	    
	container = cmap->sdf_container;

	/*
	 * Check if container is/being deleted, if so return error
	 */
	if (delete_check && (ZS_TRUE == is_container_being_deleted(cguid))) {

		status = ZS_FAILURE_CONTAINER_NOT_FOUND;

		plat_log_msg(160140, LOG_CAT, LOG_DIAG,
				"Container %lu does not exist:%s", cguid, ZS_Status_Strings[status]);

		if ( serialize ) {
			SDFEndSerializeContainerOp(pai);
		}
		return status;
	}

    if ( ( ZS_CONTAINER_STATE_OPEN != cmap->state && 
	   ZS_CONTAINER_STATE_DELETE_OPEN != cmap->state ) || 	
        isContainerNull(container) ) {
        status = ZS_FAILURE_CONTAINER_NOT_OPEN;
    } else {

    	if ( ZS_CONTAINER_STATE_DELETE_OPEN == cmap->state ) {
   	    if ( ZS_SUCCESS != ( status = zs_ctnr_set_state( cmap, ZS_CONTAINER_STATE_DELETE_CLOSED ) ) )
		goto out;
	} else {
   	    if ( ZS_SUCCESS != ( status = zs_ctnr_set_state( cmap, ZS_CONTAINER_STATE_CLOSED ) ) )
		goto out;
	}

     	// Allow any outstanding IO to drain
    	zs_cntr_drain_io( cmap->io_count );

        // Delete the container if there are no outstanding opens and a delete is pending
        local_SDF_CONTAINER lcontainer = getLocalContainer(&lcontainer, container);
        SDF_CONTAINER_PARENT parent = lcontainer->parent;
        local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, parent);
        char path[MAX_OBJECT_ID_SIZE] = "";

        if (lparent->name) {
            memcpy(&path, lparent->name, strlen(lparent->name));
        }

        if (lparent->num_open_descriptors == 1 && lparent->delete_pending == SDF_TRUE) {
            ok_to_delete = 1;
        }
        // copy these from lparent before I nuke it!
        releaseLocalContainerParent(&lparent);

        if (closeParentContainer(container)) {
            status = ZS_SUCCESS;
        }

#ifdef SDFAPIONLY
    	if ( mode == ZS_PHYSICAL_CNTR && 
			(status = name_service_get_meta(pai, cguid, &meta)) == ZS_SUCCESS) {

			#ifdef MULTIPLE_FLASH_DEV_ENABLED
				flash_dev = get_flashdev_from_shardid(state->config.flash_dev,
											  meta.shard, state->config.flash_dev_count);
			#else
				flash_dev = state->config.flash_dev;
			#endif
			shard = shardFind(flash_dev, meta.shard);

			if(shard)
				shardSync(shard);
		}

	    // Invalidate all of the container's cached objects
	    if ((status = name_service_flush_inval_object_container(pai, path)) != ZS_SUCCESS) {
			plat_log_msg(21540, LOG_CAT, LOG_ERR,
			     "%s - failed to flush and invalidate container", path);
	    } else {
			plat_log_msg(21541, LOG_CAT, LOG_TRACE,
			     "%s - flush and invalidate container succeed", path);
	    }

		if ((tmp_status = name_service_get_meta_from_cname(pai, path, &meta)) == ZS_SUCCESS) {
		    tmp_status = SDFActionDeleteContainer(pai, &meta);
		    if (tmp_status != ZS_SUCCESS) {
				// xxxzzz container will be left in a weird state!
				plat_log_msg(21542, LOG_CAT, LOG_ERR,
					"%s - failed to delete action thread container state", path);
		    } else {
				plat_log_msg(21543, LOG_CAT, LOG_TRACE,
					"%s - action thread delete container state succeeded", path);
		    }
		}

    	if ( mode == ZS_PHYSICAL_CNTR && shard )
			shardClose(shard);
#endif

        if ( status == ZS_SUCCESS ) {
    		cmap->sdf_container = containerNull;

			if (ok_to_delete) {
				// Is this path used???
			    plat_log_msg(160031, LOG_CAT, LOG_DBG, "Delete request pending. Deleting... cguid=%lu", cguid);

		    	status = delete_container_internal_low(pai, path, SDF_FALSE, mode == ZS_PHYSICAL_CNTR ? SDF_TRUE:SDF_FALSE, NULL);
	            zs_cmap_delete( cguid, path );
			}
        }
    }

out:
	/*
	 * We would not want to see this log emitted if shutdown is in progress.
	 * Closing a closed container throws error that container was not open.
	 * We collect the error in ZSShutdown() and emit appropriate log.
	 */
	if (!agent_state.op_access.is_shutdown_in_progress) {
		plat_log_msg(150021, LOG_CAT, LOG_DIAG, "%lu - %s", cguid, ZS_Status_Strings[status]);
	}

	if ( serialize )
	    SDFEndSerializeContainerOp(pai);

    return (status);
}


static ZS_status_t
zs_delete_container(
    struct ZS_thread_state *zs_thread_state,
    ZS_cguid_t              cguid,
	ZS_container_mode_t	 mode
    )
{
    if ( getProperty_Int("ASYNC_DELETE_CONTAINERS",0) == 1 ) {
        return zs_delete_container_async_start(zs_thread_state,cguid,
                                                        ZS_VIRTUAL_CNTR);
    }
    else {
	    return zs_delete_container_1(zs_thread_state, cguid, ZS_VIRTUAL_CNTR);
    }
}


ZS_status_t ZSDeleteContainer(
    struct ZS_thread_state *zs_thread_state,
    ZS_cguid_t              cguid
    )
{

	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;

	plat_log_msg(21630, LOG_CAT, LOG_DBG, "%lu", cguid);
	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}

	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
	       plat_log_msg(160188, LOG_CAT,
               LOG_DBG, "Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
        if ( !zs_thread_state || !cguid ) {
            if ( !zs_thread_state ) {
                plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
            }
            if ( !cguid ) {
                plat_log_msg(80050,LOG_CAT,LOG_DBG,
                             "Invalid container cguid:%lu",cguid);
            }
            return ZS_INVALID_PARAMETER;
        }


	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_delete_container(zs_thread_state, cguid, ZS_VIRTUAL_CNTR);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	plat_log_msg(150021, LOG_CAT, LOG_DBG, "%lu - %s", cguid, ZSStrError(status));

	return status;
}

ZS_status_t ZSDeletePhysicalContainer(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t		 		 cguid
	)
{
	return zs_delete_container( zs_thread_state,
								 cguid,
								 ZS_PHYSICAL_CNTR
							   );
}

ZS_status_t zs_delete_container_async_end(
                                struct ZS_thread_state *zs_thread_state,
                                                         ZS_cguid_t cguid){
    ZS_status_t status;
    int ok_to_delete;
    ZS_cguid_t mycguid;
    SDF_container_meta_t meta;
    SDF_internal_ctxt_t *pai;
    ZS_container_mode_t mode;
	cntr_map_t *cmap = NULL;
    int j = 0;

    pai = (SDF_internal_ctxt_t *) zs_thread_state;
    /* Serializing container operation not required here. Because 
       it is protected by flag delete under progress */
    cmap = zs_cmap_get_by_cguid( cguid );  
    if ( !cmap ) {
        plat_log_msg( 160074, LOG_CAT, LOG_DBG,
                        "Container does not exist. Delete can not proceed" );
        return ZS_FAILURE;
    }      

    mode = ZS_VIRTUAL_CNTR;
    if( cguid <= 2 ) { 
        mode = ZS_PHYSICAL_CNTR;
    }
    //sleep(15);

    /* Open container and delete */
    status = zs_open_container( zs_thread_state,cmap->cname,
                                  NULL,0,&mycguid,mode,ZS_TRUE);
    if( status != ZS_SUCCESS ) {
        plat_log_msg(160209, LOG_CAT,LOG_ERR,
                    "Unable to open container %lu for deleting err=%s",cguid, ZSStrError(status));
        return ZS_FAILURE;
    }
    status = zs_delete_objects( zs_thread_state, cguid );
    if( status != ZS_SUCCESS ) {
        plat_log_msg(160076,LOG_CAT,LOG_ERR,
                    "Deleting all objects in container %lu failed",cguid);
        return ZS_FAILURE;
    }
    status = zs_close_container(zs_thread_state,cguid,mode,
                                                       ZS_TRUE,ZS_FALSE);
    if( status != ZS_SUCCESS ) {
        plat_log_msg(160077,LOG_CAT,LOG_WARN,
                 "Closing container %lu after deleting objects failed",cguid);
        return ZS_FAILURE;
    }

    /* All objects got deleted. Just cleanup metaadata */
    SDFStartSerializeContainerOp(pai);
	status = delete_container_internal_low( pai, cmap->cname, 
				SDF_FALSE,  (mode == ZS_PHYSICAL_CNTR ? SDF_TRUE:SDF_FALSE), 
													&ok_to_delete );
	if ( ZS_SUCCESS == status && ok_to_delete) {
            zs_ctnr_set_state( cmap, ZS_CONTAINER_STATE_UNINIT);
	    zs_cmap_delete( cguid, cmap->cname );
		cmap = NULL;
		if ( ZS_VIRTUAL_CNTR == mode ) {
            zs_flush_container( zs_thread_state, VMC_CGUID );
        }
		else {
    		zs_flush_container( zs_thread_state, CMC_CGUID );
        }
    } 
    else { 
        /* for some reason cleanup failed. Just reset delete_in_progress*/
        status = ZS_FAILURE;
        if (name_service_get_meta( pai, cguid, &meta ) != SDF_SUCCESS ) {
            plat_log_msg( 160078, LOG_CAT, LOG_ERR,
                   "Could not read metadata for %lu. Delete can not proceed\n",
                                                                        cguid );
            SDFEndSerializeContainerOp(pai);
            return ZS_FAILURE;
        }
        meta.delete_in_progress = ZS_FALSE;
        if ( name_service_put_meta( pai, cguid, &meta ) != SDF_SUCCESS ) { 
            plat_log_msg( 160079, LOG_CAT, LOG_ERR, 
                    "Could not clear Metadata for %lu after delete", cguid );
        }
        plat_log_msg( 160090, LOG_CAT, LOG_ERR, 
                    "Container %lu is not cleanedup completly", cguid );
    }

    /*
     * Clear the map entry for cguid.
     */
	for ( j = 0; j < max_num_containers; j++ ) {
		if (Mcd_containers[j].cguid == cguid) {
			Mcd_containers[j].cguid = 0;
			Mcd_containers[j].container_id  = 0;
			memcpy( Mcd_containers[j].cname, "none", strlen("none") );
		    break;
		}
	}
    


    SDFEndSerializeContainerOp( pai );
    return status;
}

#define CONTAINER_RENAME_PREFIX "$c#@@n**"

/* This function MUST be called after aquiring 
                                 SDFStartSerializeContainerOp(pai) lock */
ZS_status_t zs_rename_container(struct ZS_thread_state *zs_thread_state,
                                                         ZS_cguid_t cguid ) {
    time_t t;
    ZS_status_t status;
    SDF_container_meta_t meta;
    char cname[CONTAINER_NAME_MAXLEN];
    SDF_internal_ctxt_t *pai;
	cntr_map_t *cmap = NULL;

    pai = (SDF_internal_ctxt_t *) zs_thread_state;

    cmap = zs_cmap_get_by_cguid( cguid );
    if ( !cmap ) {
        plat_log_msg( 160110, LOG_CAT, LOG_DBG,
                        "Container does not exist. Can not rename" );
        return ZS_FAILURE;
    }

    status = name_service_get_meta( pai, cguid, &meta );
    if ( status != ZS_SUCCESS ) {
        plat_log_msg( 160111, LOG_CAT, LOG_DBG,
          "Could not read metadata for %lu. Can not rename\n", cguid );
        return ZS_FAILURE;
    }   
    if ( strncmp(meta.cname,CONTAINER_RENAME_PREFIX,
                                    strlen(CONTAINER_RENAME_PREFIX)) == 0 ) {
        plat_log_msg( 160112, LOG_CAT, LOG_DBG,
          "Container %lu is already renamed. \n", cguid );
        return ZS_SUCCESS;
    }

    time(&t);
    snprintf(cname,CONTAINER_NAME_MAXLEN,"%s_%x_%s",CONTAINER_RENAME_PREFIX,
                                 atomic_add_get(delete_prefix,1),meta.cname);
    plat_log_msg( 160113, LOG_CAT, LOG_DBG,
          "Renaming container %s to %s\n",meta.cname,cname );
    if ( getProperty_Int("ASYNC_DEL_CONT_TEST_ABORT_AFTER_REM_MAP", 0) == 1 ){
         plat_abort();
    }

    /*
     * Delete entry from hashmap. We cannot update the entry for cname since 
     * cname acts as the key and now we have a new key (i.e. the new cname).
     */
    HashMap_remove( cmap_cname_hash, meta.cname );

    snprintf(meta.cname,CONTAINER_NAME_MAXLEN,"%s",cname);
    snprintf(cmap->cname,CONTAINER_NAME_MAXLEN,"%s",cname);

	status = zs_cmap_update(cmap);

	if (ZS_SUCCESS != status) {
	    plat_log_msg( 150115, LOG_CAT, LOG_ERR, "Unable to create metadata cache for %lu. Cannot rename", cguid );
	    return status;
	}

    status = name_service_put_meta( pai, cguid, &meta );
    if ( status != ZS_SUCCESS ) {
        plat_log_msg( 160115, LOG_CAT, LOG_ERR,
                      "Unable to write metadata for %lu. Can not rename ",
                                                                      cguid );
        return status;
    }
    if ( getProperty_Int("ASYNC_DEL_CONT_TEST_ABORT_BEF_CMAP", 0) == 1 ){
         plat_abort();
    }

    return ZS_SUCCESS;
}

ZS_status_t zs_delete_container_async_start(
                                struct ZS_thread_state *zs_thread_state,
                                ZS_cguid_t cguid, ZS_container_mode_t mode ){

    ZS_status_t status;
    SDF_container_meta_t meta;
    SDF_internal_ctxt_t *pai;
    cntr_map_t *cmap = NULL;

    plat_log_msg(160091,LOG_CAT, LOG_DBG, "Deleting container %lu",cguid);
    if ( !cguid ) {
        plat_log_msg(160081,LOG_CAT, LOG_DBG, "Null container Id. Delete can not proceed.");
        return ZS_INVALID_PARAMETER;
    }

    pai = (SDF_internal_ctxt_t *) zs_thread_state;

    SDFStartSerializeContainerOp(pai);
    cmap = zs_cmap_get_by_cguid( cguid );
    if ( !cmap ) {
        plat_log_msg(160210, LOG_CAT, LOG_WARN,
                     "Container cguid=%lu does not exist. Delete can not proceed\n", cguid );
        SDFEndSerializeContainerOp(pai);
        return ZS_FAILURE;
    }

    status = name_service_get_meta( pai, cguid, &meta );
    if ( status != ZS_SUCCESS ) {
        plat_log_msg( 160078, LOG_CAT, LOG_ERR, 
          "Could not read metadata for %lu. Delete can not proceed\n", cguid );
        SDFEndSerializeContainerOp(pai);
        return ZS_FAILURE;
    }
    /* Check if delete is under progress already */
    if ( meta.delete_in_progress == SDF_TRUE ) {
        /* Check if we have initiated the async command. While doing 
           Recovery, we have to make sure we are sending the command */
        if ( ZS_CONTAINER_STATE_DELETE_PROG == cmap->state ||
             ZS_CONTAINER_STATE_DELETE_OPEN == cmap->state ||
       	     ZS_CONTAINER_STATE_DELETE_CLOSED == cmap->state ) {
            plat_log_msg(160211, LOG_CAT, LOG_DIAG,
                         "Delete already under progress for container %lu with state=%d",cguid, cmap->state);
            SDFEndSerializeContainerOp(pai);
            return ZS_FAILURE_CONTAINER_DELETED; 
        }
        /* check if it needs renaming */
        status = zs_rename_container(zs_thread_state,cguid);
        if ( status != ZS_SUCCESS ) {
            plat_log_msg(160117,LOG_CAT, LOG_ERR,
                                   "Failed to delete the container:%lu",cguid);
            SDFEndSerializeContainerOp(pai);
            return ZS_FAILURE;
        }
    }
    else {
       
        /* Close the container if it is open already */
        if ( zs_is_ctnr_open( cguid ) ) {
            status = zs_close_container(zs_thread_state, cguid,
                                               mode, ZS_FALSE,ZS_TRUE);
            if ( status != ZS_SUCCESS ) {
                plat_log_msg(160212, LOG_CAT, LOG_ERR,
                          "Failed to close container during delete status=%s", ZSStrError(status) );
                SDFEndSerializeContainerOp(pai);
                return ZS_FAILURE;
            }
        }
        meta.delete_in_progress = ZS_TRUE;
        status = name_service_put_meta( pai, cguid, &meta );
        if ( status != ZS_SUCCESS ) {
            plat_log_msg( 160093, LOG_CAT, LOG_ERR, 
                      "Could not mark delete in progress for container %lu. ",
                                                                      cguid );
            SDFEndSerializeContainerOp(pai);
            return ZS_FAILURE;
        } 
        if ( getProperty_Int("ASYNC_DEL_CONT_TEST_ABORT_AFTER_META", 0) == 1 ){
            plat_abort();
        }

        status = zs_rename_container(zs_thread_state,cguid);
        if ( status != ZS_SUCCESS ) {
            plat_log_msg(160117,LOG_CAT, LOG_ERR,
                                   "Failed to delete the container:%lu",cguid);
            SDFEndSerializeContainerOp(pai);
            return ZS_FAILURE;
        }
    }

    status = async_command_delete_container(cguid);
    if ( status != ZS_SUCCESS ) {
        plat_log_msg( 160085, LOG_CAT, LOG_ERR,
             "Failed to initiate the asynchronous container delete");
        SDFEndSerializeContainerOp(pai);
        return ZS_FAILURE;
    }
    /* Set the state that async delete is under progress */
	if ( ZS_SUCCESS != ( status = zs_ctnr_set_state( cmap, ZS_CONTAINER_STATE_DELETE_PROG ) ) ) {
    	SDFEndSerializeContainerOp(pai);
    	return status;
	}
    SDFEndSerializeContainerOp(pai);
    return ZS_SUCCESS;
}


static ZS_status_t zs_delete_container_1(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t		 		 cguid,
	ZS_container_mode_t	 mode
	)
{  
    ZS_status_t 	 	 	 status 		= ZS_FAILURE;
    ZS_status_t 	 	 	 del_status 	= ZS_FAILURE;
	int				  	 	 ok_to_delete	= 0, j = 0;
	SDF_container_meta_t	 meta;
	ZS_cguid_t	 			 mycguid		= 0;
    SDF_internal_ctxt_t 	*pai 			= (SDF_internal_ctxt_t *) zs_thread_state;
	cntr_map_t              *cmap           = NULL;
    plat_log_msg( 21630, 
				  LOG_CAT, 
				  LOG_DBG, 
				  "%lu", 
				  cguid );


	if ( !cguid )
		return ZS_INVALID_PARAMETER;

    SDFStartSerializeContainerOp(pai);

    cmap = zs_cmap_get_by_cguid( cguid );

	if ( !cmap ) {
		plat_log_msg( 150099,
					  LOG_CAT,
					  LOG_DIAG,
					  "Container does not exist" );
		status = ZS_FAILURE_CONTAINER_NOT_FOUND;
		goto out;
	}

	if ( zs_is_ctnr_open( cguid ) ) {
		if ( ( status = zs_close_container( zs_thread_state, cguid, mode, ZS_FALSE, ZS_TRUE ) ) != ZS_SUCCESS ) {
			plat_log_msg( 150097,
						  LOG_CAT,
						  LOG_DIAG,
						  "Failed to close container during delete - attempting delete" );
		}
	}

	if ( ( status = name_service_get_meta( pai, cguid, &meta ) ) != ZS_SUCCESS ) {
		plat_log_msg( 150085, LOG_CAT, LOG_ERR, "Could not read metadata for %lu\n", cguid );
    	SDFEndSerializeContainerOp(pai);
		return ZS_FAILURE;
	} 

	meta.delete_in_progress = ZS_TRUE;

	if ( name_service_put_meta( pai, cguid, &meta ) != SDF_SUCCESS ) {
		plat_log_msg( 150086, LOG_CAT, LOG_DIAG, "Could not mark delete in progress for container %lu\n", cguid );
	} 

	if ( ( status = zs_open_container( zs_thread_state,  
	                                    cmap->cname, 
	                                    NULL, 
	                                    0, 
	                                    &mycguid, 
	                                    mode, 
	                                    ZS_FALSE ) ) != ZS_SUCCESS ) {
		plat_log_msg( 150091,
					  LOG_CAT,
					  LOG_ERR,
					  "Cannot open container %lu to delete it",
					  cguid );
		goto out;
	}
	if ( ( del_status = zs_delete_objects( zs_thread_state, cguid ) ) != ZS_SUCCESS ) {
		plat_log_msg( 150092,
					  LOG_CAT,
					  LOG_ERR,
					  "Failed to delete container objects" );
	} 

	if ( ( status = zs_close_container( zs_thread_state,  
   										 mycguid,
										 mode,
										 ZS_FALSE, ZS_TRUE ) ) != ZS_SUCCESS ) {
		plat_log_msg( 150093,
				  	  LOG_CAT,
					  LOG_WARN,
					  "Cannot close container %lu to delete it",
					  cguid );

		if ( ZS_SUCCESS != del_status )
			status = del_status;
		goto out;
	} else {

        status = delete_container_internal_low( pai, 
												cmap->cname, 
												SDF_FALSE,  mode == ZS_PHYSICAL_CNTR ? SDF_TRUE:SDF_FALSE, 
												&ok_to_delete );

        if ( ZS_SUCCESS == status && ok_to_delete) {

	        zs_cmap_delete( cguid, cmap->cname );
			cmap = NULL;

			// Make sure the metadata container is in sync
			if ( ZS_VIRTUAL_CNTR == mode )
				zs_flush_container( zs_thread_state, VMC_CGUID );
			else
				zs_flush_container( zs_thread_state, CMC_CGUID );

        } else {
	        if ( ZS_SUCCESS == status )
				status = ZS_FAILURE;

			meta.delete_in_progress = ZS_FALSE;

			if ( name_service_put_meta( pai, cguid, &meta ) != SDF_SUCCESS ) {
				plat_log_msg( 150087, LOG_CAT, LOG_WARN, "Could not clear delete in progress for container %lu\n", cguid );
			} 

           plat_log_msg( 150041,
	                     LOG_CAT, 
	                     LOG_ERR, 
	                     "Container is not deleted (busy or error): cguid=%lu, status=%s", 
	                     cguid, 
	                     ZSStrError(status) );
        }
    }

    /*
     * Clear the map entry for cguid.
     */
	for ( j = 0; j < max_num_containers; j++ ) {
		if (Mcd_containers[j].cguid == cguid) {
			Mcd_containers[j].cguid = 0;
			Mcd_containers[j].container_id  = 0;
			memcpy( Mcd_containers[j].cname, "none", strlen("none") );
		    break;
		}
	}
    

    plat_log_msg( 20819, 
				  LOG_CAT, 
				  LOG_DBG, 
				  "%s", 
				  ZSStrError(status) );
 out:
    SDFEndSerializeContainerOp(pai);
    return status;
}

ZS_cguid_t ZSGetCguid (char *cname ) {
	ZS_cguid_t cguid = ZS_NULL_CGUID;

	if ( cname ) {
	    cntr_map_t *cmap = zs_cmap_get_by_cname( cname );
	    if ( cmap )
	        cguid = cmap->cguid;
	}

	return cguid;
}

char *ZSGetContainerName(ZS_cguid_t cguid) {
	cntr_map_t *cmap = NULL;

	cmap = zs_cmap_get_by_cguid( cguid );
	if ( !cmap || !cmap->cname )
	    return "";
	else {
	    return cmap->cname;
	}
}

char *ZSGetNextContainerName(struct ZS_thread_state *zs_thread_state, struct ZSCMapIterator **iterator, ZS_cguid_t *pcguid) {
    SDF_container_meta_t meta;
    SDF_internal_ctxt_t *pai;
	char *key = NULL;
	uint32_t keylen = 0;
	cntr_map_t *cmap = NULL;
	uint64_t cmaplen = 0;

    pai = (SDF_internal_ctxt_t *) zs_thread_state;
    SDFStartSerializeContainerOp(pai);
    *pcguid = ZS_NULL_CGUID;

	// See if we are just starting the enum
	if ( NULL == *iterator ) {
	    *iterator = (struct ZSCMapIterator *) zs_cmap_enum();
	    if ( NULL == *iterator ) {
                SDFEndSerializeContainerOp( pai );
	        return NULL;
            }
	}

	// Get the next container name
	while ( zs_cmap_next_enum( (struct cmap_iterator *)(*iterator), &key, &keylen, (char **) &cmap, &cmaplen ) ) {
        if ( cmap->cguid != ZS_NULL_CGUID ) {
            /* Skip CMC, VMC and VDC */
            if ( ( strcmp( cmap->cname,"/sdf/VMC" ) == 0 ) ||
                 ( strcmp( cmap->cname,"/sdf/VDC") == 0 ) ||
                 ( strcmp( cmap->cname, SEQNO_CONTAINER_NAME ) == 0 ) ||
                 ( strcmp( cmap->cname, PSTATS_CONTAINER_NAME ) == 0 ) ) {
                continue;
            }

            /* check if the container is being deleted */
            if ( name_service_get_meta( pai, cmap->cguid, &meta ) != SDF_SUCCESS ) {
                //plat_log_msg( 160087, LOG_CAT, LOG_ERR,
                //   "Could not read metadata for %lu. skipping this container from list",
                //                                                        CtnrMap[i].cguid );
                continue;
            }
            if ( meta.delete_in_progress == SDF_TRUE ) {
                //plat_log_msg( 160088, LOG_CAT, LOG_DBG,
                //            "Container %lu is being deleted. So not included in the list",
                //                                                        CtnrMap[i].cguid);
                continue;
            }
            SDFEndSerializeContainerOp( pai );
            *pcguid = cmap->cguid;
            return cmap->cname;
        }
    }

	// Must have reached the end of the enum
	zs_cmap_finish_enum( ( struct cmap_iterator * ) (*iterator) );
	*iterator = NULL;
    SDFEndSerializeContainerOp( pai );
    return NULL;
}

/*
 * Internal version of zs_get_containers_int
 */ 
#define BTREE_DELETE_CONTAINER_NAME "B#^++$(h@@n+^\0"
static ZS_status_t
zs_get_containers_int(
		struct ZS_thread_state	*zs_thread_state,
		ZS_cguid_t             *cguids,
		uint32_t                *n_cguids)
{
    int                   n_containers = 0;
	char                 *key          = NULL;
	uint32_t              keylen       = 0;
	cntr_map_t           *cmap         = NULL;
	uint64_t              cmaplen      = 0;
	struct cmap_iterator *iterator     = NULL;

    if (!cguids || !n_cguids) 
	    return ZS_INVALID_PARAMETER;

	iterator = zs_cmap_enum();

	if ( !iterator )
	    return ZS_FAILURE;

	while ( zs_cmap_next_enum( iterator, &key, &keylen, (char **) &cmap, &cmaplen ) ) {
		if (cmap->cguid > LAST_PHYSICAL_CGUID  && 
		   ((strcmp(cmap->cname,SEQNO_CONTAINER_NAME) &&
		     strcmp(cmap->cname,PSTATS_CONTAINER_NAME)) || 
		     mcd_check_get_level() != ZSCHECK_NO_CHECK) &&
             strncmp( cmap->cname,BTREE_DELETE_CONTAINER_NAME, strlen(BTREE_DELETE_CONTAINER_NAME) ) &&
			 ( cmap->state == ZS_CONTAINER_STATE_CLOSED ||
			   cmap->state == ZS_CONTAINER_STATE_OPEN )  ) {
			cguids[n_containers] = cmap->cguid;
            n_containers++;
        }
    }

	zs_cmap_finish_enum( iterator );
    *n_cguids = n_containers;
    return ZS_SUCCESS;
}

static ZS_status_t
zs_get_containers(
		struct ZS_thread_state	*zs_thread_state,
		ZS_cguid_t             *cguids,
		uint32_t                *n_cguids)
{
    SDF_internal_ctxt_t *pai = (SDF_internal_ctxt_t *) zs_thread_state;
    ZS_status_t ret = ZS_SUCCESS;

    SDFStartSerializeContainerOp(pai);  
    ret = zs_get_containers_int(zs_thread_state, cguids, n_cguids);
    SDFEndSerializeContainerOp( pai );   

    return ret;
}


ZS_status_t ZSGetContainers(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t             *cguids,
	uint32_t                *n_cguids
	)
{
	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;

	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
		plat_log_msg(160187, LOG_CAT,
			LOG_WARN, "Operation not allowed");
		goto out;
	}

	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}

    if ( !zs_thread_state ) {
        plat_log_msg(80049,LOG_CAT,LOG_DBG, "ZS Thread state is NULL");
        status = ZS_INVALID_PARAMETER;
        goto out;
    }

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);

	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_get_containers(zs_thread_state, cguids, n_cguids);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}

/*
 * Internal version of zs_get_container_props.
 */

static ZS_status_t
zs_get_container_props_int(
			struct ZS_thread_state	*zs_thread_state, 
			ZS_cguid_t 		  	 cguid, 
			ZS_container_props_t	*pprops)
{
    ZS_status_t             	 status = ZS_SUCCESS;
    SDF_container_meta_t     	 meta;
    SDF_internal_ctxt_t     	*pai = (SDF_internal_ctxt_t *) zs_thread_state;

	if ( !cguid || !pprops )
		return ZS_INVALID_PARAMETER;

	if ( ZS_TRUE == is_container_being_deleted(cguid) ) {

		status = ZS_FAILURE_CONTAINER_NOT_FOUND;

		plat_log_msg(160140, LOG_CAT, LOG_DIAG,
				"Container %lu does not exist:%s", cguid, ZS_Status_Strings[status]);

		return status;
	}

	if (( status = name_service_get_meta( pai, cguid, &meta )) == ZS_SUCCESS ) {
		status = zs_create_zs_props( &meta.properties, pprops );
		strncpy(pprops->name, meta.cname, CONTAINER_NAME_MAXLEN);
    }              
                   
    return status;
}

static ZS_status_t
zs_get_container_props(
		struct ZS_thread_state	*zs_thread_state, 
		ZS_cguid_t cguid, 
		ZS_container_props_t *pprops
		)
{   
    ZS_status_t             	 status = ZS_SUCCESS;
    SDF_internal_ctxt_t     	*pai = (SDF_internal_ctxt_t *) zs_thread_state;

    SDFStartSerializeContainerOp(pai);  
    status = zs_get_container_props_int(zs_thread_state, cguid, pprops);
    SDFEndSerializeContainerOp( pai );   
                   
    return status;
}
   

ZS_status_t ZSGetContainerProps(
	struct ZS_thread_state	*zs_thread_state, 
	ZS_cguid_t 		  	 cguid, 
	ZS_container_props_t	*pprops
	)
{
	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;

	status = zs_validate_container_1(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160261, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID: %ld %s",
				cguid, ZS_Status_Strings[status]);
		goto out;
	}

	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
		plat_log_msg(160187, LOG_CAT,
			LOG_WARN, "Operation not allowed");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
        if ( !zs_thread_state || !cguid ) {
            if ( !zs_thread_state ) {
                plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
            }
            if ( !cguid ) {
                plat_log_msg(80050,LOG_CAT,LOG_DBG,
                             "Invalid container cguid:%lu",cguid);
            }
            return ZS_INVALID_PARAMETER;
        }

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_get_container_props(zs_thread_state, cguid, pprops);	

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}


static ZS_status_t
zs_set_container_props(
		struct ZS_thread_state	*zs_thread_state, 
		ZS_cguid_t 	 	  	 cguid,
		ZS_container_props_t	*pprops
		)
{
    ZS_status_t             status = ZS_SUCCESS;
    SDF_container_meta_t     meta;
    SDF_internal_ctxt_t     *pai 	= (SDF_internal_ctxt_t *) zs_thread_state;
	cntr_map_t              *cmap   = NULL;

	if ( !cguid || !pprops )
		return ZS_INVALID_PARAMETER;

	SDFStartSerializeContainerOp(pai);
	if ( ZS_TRUE == is_container_being_deleted(cguid) ) {

		status = ZS_FAILURE_CONTAINER_NOT_FOUND;

		plat_log_msg(160140, LOG_CAT, LOG_DIAG,
				"Container %lu does not exist:%s", cguid, ZS_Status_Strings[status]);

		SDFEndSerializeContainerOp( pai );
		return status;
	}

	if (( status = name_service_get_meta( pai, cguid, &meta )) == ZS_SUCCESS ) {
		if ( pprops->size_kb != meta.properties.container_id.size ) {
			if ( ( status = zs_resize_container( zs_thread_state, cguid, pprops->size_kb ) ) != ZS_SUCCESS ) {
				plat_log_msg( 150094, LOG_CAT, LOG_ERR, "Failed to resize %lu - %s", cguid, ZSStrError( status ) );
				goto out;
			}
		}

    	meta.properties.container_id.size                     = pprops->size_kb;
		// We currently support slab only with 1 shard 
    	meta.properties.container_type.caching_container      = pprops->evicting;
    	meta.properties.cache.writethru                       = pprops->writethru;
		meta.properties.fifo_mode = SDF_FALSE;
		meta.properties.shard.num_shards 		      		  = 1;

		meta.properties.durability_level = SDF_NO_DURABILITY;
		if ( pprops->durability_level == ZS_DURABILITY_HW_CRASH_SAFE )
	    	meta.properties.durability_level = SDF_FULL_DURABILITY;
		else if ( pprops->durability_level == ZS_DURABILITY_SW_CRASH_SAFE )
	    	meta.properties.durability_level = SDF_RELAXED_DURABILITY;

 		meta.properties.flash_only = pprops->flash_only;
 		meta.properties.cache_only = pprops->cache_only;
                meta.properties.compression = pprops->compression;
#if 1//Rico - lc
                meta.properties.flags = pprops->flags;
#endif
        status = name_service_put_meta( pai, cguid, &meta );
		cmap = zs_cmap_get_by_cguid( cguid );

        if ( cmap ) {
            cmap->evicting = pprops->evicting;
            /*
             * These maps are immutable except for container deletion.
             * We should never update them.
             */
            //status = zs_cmap_update( cmap );
        }
    }

 out:
    SDFEndSerializeContainerOp( pai );

    return status;
}


ZS_status_t ZSSetContainerProps(
	struct ZS_thread_state	*zs_thread_state, 
	ZS_cguid_t 	 	  	 cguid,
	ZS_container_props_t	*pprops
	)
{
	ZS_status_t  status = ZS_SUCCESS;
	bool thd_ctx_locked = false;

	if (pprops->durability_level == ZS_DURABILITY_PERIODIC) {
		pprops->durability_level = ZS_DURABILITY_SW_CRASH_SAFE;
	}

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(160188, LOG_CAT,
               LOG_WARN, "Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
        if ( !zs_thread_state || !cguid || !pprops ) {
            if ( !zs_thread_state ) {
                plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
            }
            if ( !cguid ) {
                plat_log_msg(80050,LOG_CAT,LOG_DBG,
                             "Invalid container cguid:%lu",cguid);
            }
            if( !pprops ) {
                plat_log_msg(80054,LOG_CAT,LOG_DBG,
                             "Container property is NULL");
            }
            return ZS_INVALID_PARAMETER;
        }

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_set_container_props(zs_thread_state, cguid, pprops);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}

static ZS_status_t
zs_read_object(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t				cguid,
	char					*key,
	uint32_t				keylen,
	char					**data,
	uint64_t				*datalen,
	bool					app_buf,
	bool					rawobject	
	)
{
	SDF_appreq_t        ar;
	SDF_action_init_t  *pac;
	ZS_status_t        status  = ZS_SUCCESS;
	ZS_status_t        read_ret= ZS_SUCCESS;
	char *app_buf_data_ptr = NULL;
	cntr_map_t *cmap = NULL;

	if ( !cguid || !key ) {
		return ZS_INVALID_PARAMETER;
	}

	if (data == NULL) {
		return ZS_BAD_PBUF_POINTER;
	}

	cmap = get_cntr_map(cguid);
	if (!cmap) {
		return(ZS_CONTAINER_UNKNOWN);
	}

	if ( (status = zs_get_ctnr_status_cmap(cmap, cguid, 0)) != ZS_CONTAINER_OPEN ) {
		plat_log_msg( 160039, LOG_CAT, LOG_DIAG, "Container must be open to execute a read object" );
		goto out;     
	}

	pac = (SDF_action_init_t *) zs_thread_state;

	SDF_cache_ctnr_metadata_t *meta;
	meta = get_container_metadata(pac, cguid);
	if (meta == NULL) {
		goto out;
	}

	if (cmap->lc) {
		status = lc_read( zs_thread_state, cguid, key, keylen, data, datalen);
		goto out;
	}
	if (rawobject || (meta->meta.properties.flash_only == ZS_TRUE)) {
		char	*tdata = (char *)0x1; // Make sure its not NULL
		int		flag = 0;

		plat_log_msg(160191, LOG_CAT,
				LOG_TRACE, "ZSReadObject flash_only.");
		struct objMetaData metaData;
		metaData.keyLen = keylen;
		metaData.cguid  = cguid;

		update_container_stats(pac, APGRX, meta, 1);

		if (rawobject) {
			flag |= FLASH_GET_RAW_OBJECT;
		}

		read_ret = ssd_flashGet(pac->paio_ctxt, meta->pshard, &metaData, key, &tdata, flag | FLASH_GET_NO_TEST);

		/* If app buf, copied len should be min of datalen and metaData.dataLen */
		if (app_buf) {
			*datalen = (*datalen > metaData.dataLen) ? metaData.dataLen: *datalen;
		} else {
			*datalen = metaData.dataLen;
		}

		if (read_ret == FLASH_EOK) {
			status = ZS_SUCCESS;
			if (!app_buf) {
				*data = malloc(*datalen);
				if (*data == NULL) {
					status = ZS_OUT_OF_MEM;
					goto out;
				}
			} else {
				if(*data == NULL) {
					status = ZS_BAD_PBUF_POINTER;
					goto out;
				}
			}
				
			memcpy(*data, tdata, *datalen);
			ssd_flashFreeBuf(tdata);
		} else {
			status = get_status(read_ret);
		}
		goto out;

	} else {

		ar.reqtype = APGRX;
		ar.curtime = 0;
		ar.ctxt = pac->ctxt;
		ar.ctnr = cguid;
		ar.ctnr_type = SDF_OBJECT_CONTAINER;
		ar.internal_request = SDF_TRUE;
		ar.internal_thread = fthSelf();

		if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != ZS_SUCCESS) {
			goto out;
		}

		if (app_buf) {
			/* data ptr gets modified lower layer. Keep a copy of it */
			app_buf_data_ptr = *data;
		}

		ar.ppbuf_in = (void **)data;

		ActionProtocolAgentNew(pac, &ar);

		if (datalen == NULL) {
			return(ZS_BAD_SIZE_POINTER);
		}

		/* TODO: This is sub-optimal way of reading from app buf,
		 * however, there is no application use case so far and used
		 * only for test app, hence its fine for now. When it needs
		 * to support widely, need to put this in ActionProtocol code */
		if (app_buf && (ar.respStatus == ZS_SUCCESS)) {
			*datalen = (*datalen > ar.destLen) ? ar.destLen: *datalen;
			plat_assert(app_buf_data_ptr != *data); // Don't want to free app buf by mistake
			memcpy(app_buf_data_ptr, *data, *datalen);
			free(*data);
			*data = app_buf_data_ptr;
		} else {
			*datalen = ar.destLen;
		}

		status = ar.respStatus;
	}

out:

	rel_cntr_map(cmap);

	return status;
}


ZS_status_t ZSReadObject(
	struct ZS_thread_state   *zs_thread_state,
	ZS_cguid_t                cguid,
	char                      *key,
	uint32_t                   keylen,
	char                     **data,
	uint64_t                  *datalen
	)
{
	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(160188, LOG_CAT,
		   LOG_WARN, "Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
        if ( !zs_thread_state || !cguid || !keylen ) {
            if ( !zs_thread_state ) {
                plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
            }
            if ( !cguid ) {
                plat_log_msg(80050,LOG_CAT,LOG_DBG,
                             "Invalid container cguid:%lu",cguid);
            }
            if ( !keylen ) {
                plat_log_msg(80056,LOG_CAT,LOG_DBG,
                             "Invalid key length");
            }
            return ZS_INVALID_PARAMETER;
        }

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_read_object(zs_thread_state, cguid, key, keylen, data, datalen, false, false);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status; 
}


ZS_status_t ZSReadObject2(
	struct ZS_thread_state   *zs_thread_state,
	ZS_cguid_t                cguid,
	char                      *key,
	uint32_t                   keylen,
	char                     **data,
	uint64_t                  *datalen
	)
{
	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(160188, LOG_CAT,
		   LOG_WARN, "Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
        if ( !zs_thread_state || !cguid || !keylen ) {
            if ( !zs_thread_state ) {
                plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
            }
            if ( !cguid ) {
                plat_log_msg(80050,LOG_CAT,LOG_DBG,
                             "Invalid container cguid:%lu",cguid);
            }
            if ( !keylen ) {
                plat_log_msg(80056,LOG_CAT,LOG_DBG,
                             "Invalid key length");
            }
            return ZS_INVALID_PARAMETER;
        }

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_read_object(zs_thread_state, cguid, key, keylen, data, datalen, true, false);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status; 
}

static ZS_status_t
zs_read_object_expiry(
    struct ZS_thread_state  *zs_thread_state,
    ZS_cguid_t               cguid,
    ZS_readobject_t         *robj
    )
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;
	cntr_map_t *cmap = NULL;

    if ( !cguid )
        return ZS_INVALID_PARAMETER;

    if ((NULL == zs_thread_state) || (NULL == robj) || (NULL == robj->key)) {
        return ZS_INVALID_PARAMETER;        
    }

	cmap = get_cntr_map(cguid);
	if (!cmap) {
		return(ZS_CONTAINER_UNKNOWN);
	}

    if ( (status = zs_get_ctnr_status_cmap(cmap, cguid, 0)) != (SDF_status_t) ZS_CONTAINER_OPEN ) {
        plat_log_msg( 160039, LOG_CAT, LOG_DIAG, "Container must be open to execute a read object" );
        goto out;     
    }

    pac = (SDF_action_init_t *) zs_thread_state;
   
    ar.reqtype = APGRX;
    ar.curtime = robj->current;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) robj->key, robj->key_len)) != SDF_SUCCESS) {
		goto out;
    }
 
    ar.ppbuf_in = (void **)(&(robj->data));

    ActionProtocolAgentNew(pac, &ar);

	status = ar.respStatus;

out:

	rel_cntr_map(cmap);

    robj->data_len = ar.destLen;
    robj->expiry = ar.exptime;

    return(ar.respStatus);
}


ZS_status_t ZSReadObjectExpiry(
    struct ZS_thread_state  *zs_thread_state,
    ZS_cguid_t               cguid,
    ZS_readobject_t         *robj
    )
{
	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;
//        return ZS_UNSUPPORTED_REQUEST;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(80022, LOG_CAT,
               LOG_WARN, "Shutdown in Progress. Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
        if ( !zs_thread_state || !cguid || !robj || !robj->key_len ) {
            if ( !zs_thread_state ) {
                plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
            }
            if ( !cguid ) {
                plat_log_msg(80050,LOG_CAT,LOG_DBG,
                             "Invalid container cguid:%lu",cguid);
            }
            if( !robj ) {
                plat_log_msg(80057,LOG_CAT,LOG_DBG,
                             "Invalid container ZS_readobject_t");
            }
            if ( !robj->key_len ) {
                plat_log_msg(80056,LOG_CAT,LOG_DBG,
                             "Invalid key length");
            }
            return ZS_INVALID_PARAMETER;
        }

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_read_object_expiry(zs_thread_state, cguid, robj);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status; 
}


ZS_status_t ZSFreeBuffer(
	char *buf
	)
{
    plat_free( buf );
    return ZS_SUCCESS;
}

static ZS_status_t
zs_write_object(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t          cguid,
	char                *key,
	uint32_t             keylen,
	char                *data,
	uint64_t             datalen,
	uint32_t             xflags
	)
{
	SDF_appreq_t        ar;
	SDF_action_init_t  *pac		= NULL;
	ZS_status_t        status	= ZS_FAILURE;
	ZS_status_t        write_ret    = ZS_FAILURE;
	cntr_map_t *cmap = NULL;

	if ( !cguid || !key )
		return ZS_INVALID_PARAMETER;
 
	cmap = get_cntr_map(cguid);
	if (!cmap) {
		return(ZS_CONTAINER_UNKNOWN);
	}

	if ( (status = zs_get_ctnr_status_cmap(cmap, cguid, 0)) != ZS_CONTAINER_OPEN ) {
		plat_log_msg( 160040, LOG_CAT, LOG_DIAG, "Container must be open to execute a write object" );
		goto out;     
	}

	pac = (SDF_action_init_t *) zs_thread_state;

	SDF_cache_ctnr_metadata_t *meta;
	meta = get_container_metadata(pac, cguid);
	if (meta == NULL) {
		goto out;
	}

	if (cmap->lc) {
		if (xflags & ZS_WRITE_TRIM)
			status = lc_trim( zs_thread_state, cguid, key, keylen);
		else
			status = lc_write( zs_thread_state, cguid, key, keylen, data, datalen);

		if (status == ZS_SUCCESS) {
			inc_cntr_map_by_map(cmap, cguid, 1, 0, 0);
		}
		goto out;
	}
	if (meta->meta.properties.flash_only == ZS_TRUE) {
		int flags = 0;
		plat_log_msg(160192, LOG_CAT,
			LOG_TRACE, "ZSWriteObject flash_only.");
		struct objMetaData metaData;
		bzero(&metaData, sizeof(metaData));
		metaData.keyLen = keylen;
		metaData.cguid  = cguid;
		metaData.dataLen = datalen;
		if (meta->meta.properties.compression) {
			flags |= FLASH_PUT_COMPRESS;
		}

		if (meta->meta.properties.durability_level == SDF_RELAXED_DURABILITY) {
			flags |= FLASH_PUT_DURA_SW_CRASH;
		} else if (meta->meta.properties.durability_level == SDF_FULL_DURABILITY) {
			flags |= FLASH_PUT_DURA_HW_CRASH;
		}


		update_container_stats(pac, APSOE, meta, 1);

		write_ret = ssd_flashPut(pac->paio_ctxt, meta->pshard, &metaData, key, data, FLASH_PUT_NO_TEST|flags);
		status = get_status(write_ret);
		goto out;
	} else {
		if ( xflags & ZS_WRITE_MUST_EXIST ) {
			ar.reqtype = APPAE;
		} else if( xflags & ZS_WRITE_MUST_NOT_EXIST ) {
			ar.reqtype = APCOE;
		} else {
			ar.reqtype = APSOE;
		}
		ar.curtime = 0;
		ar.ctxt = pac->ctxt;
		ar.ctnr = cguid;
		ar.ctnr_type = SDF_OBJECT_CONTAINER;
		ar.internal_request = SDF_TRUE;
		ar.internal_thread = fthSelf();
		if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != ZS_SUCCESS) {
			goto out;
		}
		ar.sze = datalen;
		ar.pbuf_out = (void *) data;
		ar.exptime = 0;

		if (data == NULL) {
			status = ZS_BAD_PBUF_POINTER;
			goto out;
		}

		ActionProtocolAgentNew(pac, &ar);

		status = ar.respStatus;
	}	
out:
	rel_cntr_map(cmap);

	return status;
}

static ZS_status_t
zs_write_raw_object(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t          cguid,
	char                *key,
	uint32_t             keylen,
	char                *data,
	uint64_t             datalen,
	uint32_t             xflags
	)
{
	SDF_action_init_t	*pac = NULL;
	ZS_status_t			 status	= ZS_FAILURE;
	ZS_status_t			 write_ret = ZS_FAILURE;
	int					 flags = 0;
	struct objMetaData	 metaData;
	cntr_map_t			*cmap = NULL;

	if ( !cguid || !key )
		return ZS_INVALID_PARAMETER;
 
	cmap = get_cntr_map(cguid);
	if (!cmap) {
		return(ZS_CONTAINER_UNKNOWN);
	}

	if ( (status = zs_get_ctnr_status_cmap(cmap, cguid, 0)) != ZS_CONTAINER_OPEN ) {
		plat_log_msg( 160040, LOG_CAT, LOG_DIAG, "Container must be open to execute a write object" );
		goto out;     
	}

	pac = (SDF_action_init_t *) zs_thread_state;

	SDF_cache_ctnr_metadata_t *meta;
	meta = get_container_metadata(pac, cguid);
	if (meta == NULL) {
		goto out;
	}
	metaData.keyLen = keylen;
	metaData.cguid  = cguid;
	metaData.dataLen = datalen;

	flags |= FLASH_PUT_RAW_OBJECT;

	if (meta->meta.properties.durability_level == SDF_RELAXED_DURABILITY) {
		flags |= FLASH_PUT_DURA_SW_CRASH;
	} else if (meta->meta.properties.durability_level == SDF_FULL_DURABILITY) {
		flags |= FLASH_PUT_DURA_HW_CRASH;
	}


	update_container_stats(pac, APSOE, meta, 1);

	write_ret = ssd_flashPut(pac->paio_ctxt, meta->pshard, &metaData, key, data, FLASH_PUT_NO_TEST|flags);
	status = get_status(write_ret);
out:
	rel_cntr_map(cmap);

	return status;
}
static ZS_status_t
zs_write_objects(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t          cguid,
	char                **key,
	uint32_t             keylen,
	char                **data,
	uint64_t             datalen,
	uint32_t             count,
	uint32_t             flags
	)
{
	int i;
	SDF_action_init_t  *pac		= NULL;
	ZS_status_t        status	= ZS_FAILURE;
	ZS_status_t        write_ret    = ZS_FAILURE;
	cntr_map_t *cmap = NULL;

 	plat_assert(cguid && key);
 
	cmap = get_cntr_map(cguid);
	if (!cmap) {
		return(ZS_CONTAINER_UNKNOWN);
	}

	if ( (status = zs_get_ctnr_status_cmap(cmap, cguid, 0)) != ZS_CONTAINER_OPEN ) {
		plat_log_msg( 160040, LOG_CAT, LOG_DIAG, "Container must be open to execute a write object" );
		goto out;     
	}

	pac = (SDF_action_init_t *) zs_thread_state;

	SDF_cache_ctnr_metadata_t *meta;
	meta = get_container_metadata(pac, cguid);
	if (meta == NULL) {
		goto out;
	}

	if (meta->meta.properties.flash_only != ZS_TRUE || count < 8) {
		status = ZS_SUCCESS;
		for(i = 0; i < count && status == ZS_SUCCESS; i++)
			status = zs_write_object(zs_thread_state, cguid, key[i], keylen, data[i], datalen, flags);
	} else {
		struct objMetaData metaData;
		metaData.keyLen = keylen;
		metaData.cguid  = cguid;
		metaData.dataLen = datalen;

		if (meta->meta.properties.compression)
			flags |= FLASH_PUT_COMPRESS;
		if (meta->meta.properties.durability_level == SDF_RELAXED_DURABILITY)
			flags |= FLASH_PUT_DURA_SW_CRASH;
		else if (meta->meta.properties.durability_level == SDF_FULL_DURABILITY)
			flags |= FLASH_PUT_DURA_HW_CRASH;

		update_container_stats(pac, APSOE, meta, count);

		write_ret = ssd_flashPutV(pac->paio_ctxt, meta->pshard, &metaData, key, data, count, FLASH_PUT_NO_TEST | flags);
		status = get_status(write_ret);
//		status = status == FLASH_EOK ? ZS_SUCCESS : ZS_FAILURE;
	}

out:
	rel_cntr_map(cmap);
	return status;
}

ZS_status_t ZSWriteObject(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t          cguid,
	char                *key,
	uint32_t             keylen,
	char                *data,
	uint64_t             datalen,
	uint32_t             flags
	)
{
	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(80022, LOG_CAT,
               LOG_WARN, "Shutdown in Progress. Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
	if ( !zs_thread_state || !cguid || !keylen || !data || !datalen  ) {
		if ( !zs_thread_state ) {
			plat_log_msg(80049,LOG_CAT,LOG_DBG,
						 "ZS Thread state is NULL");
		}
		if ( !cguid ) {
			plat_log_msg(80050,LOG_CAT,LOG_DBG,
						 "Invalid container cguid:%lu",cguid);
		}
		if ( !keylen ) {
			plat_log_msg(80056,LOG_CAT,LOG_DBG,
						 "Invalid key length");
		}
		if ( !data ) {
			plat_log_msg(80058,LOG_CAT,LOG_DBG,
						 "Invalid data(NULL)");
		}
		if ( !datalen ) {
			plat_log_msg(80059,LOG_CAT,LOG_DBG,
						 "Invalid data length");
		}
		return ZS_INVALID_PARAMETER;
	}

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_write_object(zs_thread_state, cguid, key, keylen, data, datalen, flags);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}

ZS_status_t ZSWriteObjects(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t          cguid,
	char                **key,
	uint32_t             keylen,
	char                **data,
	uint64_t             datalen,
	uint32_t             count,
	uint32_t             flags
	)
{
	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(80022, LOG_CAT,
               LOG_WARN, "Shutdown in Progress. Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
        if ( !zs_thread_state || !cguid || !keylen || !data || !datalen  ) {
            if ( !zs_thread_state ) {
                plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
            }
            if ( !cguid ) {
                plat_log_msg(80050,LOG_CAT,LOG_DBG,
                             "Invalid container cguid:%lu",cguid);
            }
            if ( !keylen ) {
                plat_log_msg(80056,LOG_CAT,LOG_DBG,
                             "Invalid key length");
            }
            if ( !data ) {
                plat_log_msg(80058,LOG_CAT,LOG_DBG,
                             "Invalid data(NULL)");
            }
            if ( !datalen ) {
                plat_log_msg(80059,LOG_CAT,LOG_DBG,
                             "Invalid data length");
            }
            if ( !count ) {
                plat_log_msg(180207,LOG_CAT,LOG_DBG,
                             "Invalid objects count");
            }
            return ZS_INVALID_PARAMETER;
        }

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_write_objects(zs_thread_state, cguid, key, keylen, data, datalen, count, flags);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}


static ZS_status_t
zs_write_object_expiry (
    struct ZS_thread_state  *zs_thread_state,
    ZS_cguid_t               cguid,
    ZS_writeobject_t        *wobj,
    uint32_t                  flags
    )
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    ZS_status_t        status;
	cntr_map_t *cmap = NULL;

    if ( !cguid )
        return ZS_INVALID_PARAMETER;
 
    if ((NULL == zs_thread_state) || (NULL == wobj) || (NULL == wobj->key)) {
        return ZS_INVALID_PARAMETER;        
    }

    if (NULL == wobj->data) {
        return ZS_BAD_PBUF_POINTER;
    }

	cmap = get_cntr_map(cguid);
	if (!cmap) {
		return(ZS_CONTAINER_UNKNOWN);
	}

    if ( (status = zs_get_ctnr_status_cmap(cmap, cguid, 0)) != ZS_CONTAINER_OPEN ) {
        plat_log_msg( 160040, LOG_CAT, LOG_DIAG, "Container must be open to execute a write object" );
		goto out;
    }

    pac = (SDF_action_init_t *) zs_thread_state;

    if ( flags & ZS_WRITE_MUST_EXIST ) {
        ar.reqtype = APPAE;
    } else if( flags & ZS_WRITE_MUST_NOT_EXIST ) {
        ar.reqtype = APCOE;
    } else {
        ar.reqtype = APSOE;
    }
    ar.curtime = wobj->current;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();

    status = SDFObjnameToKey(&(ar.key), (char *)wobj->key, wobj->key_len);
    if (status != ZS_SUCCESS) {
		goto out;
	}
    ar.sze = wobj->data_len;
    ar.pbuf_out = (void *) (wobj->data);
    ar.exptime = wobj->expiry;

    ActionProtocolAgentNew(pac, &ar);
	status = ar.respStatus;

out: 
	rel_cntr_map(cmap);

	return status;
}


ZS_status_t ZSWriteObjectExpiry(
    struct ZS_thread_state  *zs_thread_state,
    ZS_cguid_t               cguid,
    ZS_writeobject_t        *wobj,
    uint32_t                  flags
    )
{
	ZS_status_t        status	= ZS_SUCCESS;
	bool thd_ctx_locked = false;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
		plat_log_msg(80022, LOG_CAT,
				LOG_WARN, "Shutdown in Progress. Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
	if ( !zs_thread_state || !cguid || !wobj->key_len || !wobj->data || !wobj->data_len  ) {
		if ( !zs_thread_state ) {
			plat_log_msg(80049,LOG_CAT,LOG_DBG,
					"ZS Thread state is NULL");
		}
		if ( !cguid ) {
			plat_log_msg(80050,LOG_CAT,LOG_DBG,
					"Invalid container cguid:%lu",cguid);
		}
		if ( !wobj ) {
			plat_log_msg(80060,LOG_CAT,LOG_DBG,
					"Invalid ZS_writeobject_t");
		}
		if ( !wobj->key_len ) {
			plat_log_msg(80056,LOG_CAT,LOG_DBG,
					"Invalid key length");
		}
		if ( !wobj->data ) {
			plat_log_msg(80061,LOG_CAT,LOG_DBG,
					"Invalid data (NULL)");
		}
		if ( !wobj->data_len ) {
			plat_log_msg(80059,LOG_CAT,LOG_DBG,
					"Invalid data length");
		}
		return ZS_INVALID_PARAMETER;
	}

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
				LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_write_object_expiry(zs_thread_state, cguid, wobj, flags);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}


static ZS_status_t
zs_delete_object(
	struct ZS_thread_state		*zs_thread_state,
	ZS_cguid_t					cguid,
	char						*key,
	uint32_t					keylen,
	bool						raw_object
	)
{
	SDF_appreq_t        ar;
	SDF_action_init_t  *pac		= NULL;
	ZS_status_t        status	= ZS_SUCCESS;
	cntr_map_t *cmap = NULL;

	if ( !cguid || !key )
		return ZS_INVALID_PARAMETER;

	cmap = get_cntr_map(cguid);
	if (!cmap) {
		return(ZS_CONTAINER_UNKNOWN);
	}

	if ( (status = zs_get_ctnr_status_cmap(cmap, cguid, 0)) != ZS_CONTAINER_OPEN ) {
		plat_log_msg( 160041, LOG_CAT, LOG_DIAG, "Container must be open to execute a delete object" );
		goto out;     
	}

	pac = (SDF_action_init_t *) zs_thread_state;

	SDF_cache_ctnr_metadata_t *meta;
	meta = get_container_metadata(pac, cguid);
	if (meta == NULL) {
		goto out;
	}

	if (cmap->lc) {
		status = lc_delete( zs_thread_state, cguid, key, keylen);
		goto out;
	}
	if (raw_object || (meta->meta.properties.flash_only == ZS_TRUE)) {
		int flags = 0;
		plat_log_msg(160193, LOG_CAT,
				LOG_TRACE, "ZSDeleteObject flash_only.");
		struct objMetaData metaData;
		metaData.keyLen = keylen;
		metaData.cguid  = cguid;
		metaData.dataLen = 0;
		if (meta->meta.properties.durability_level == SDF_RELAXED_DURABILITY)
			flags |= FLASH_PUT_DURA_SW_CRASH;
		else if (meta->meta.properties.durability_level == SDF_FULL_DURABILITY)
			flags |= FLASH_PUT_DURA_HW_CRASH;

		if (raw_object == true) {
			flags |= FLASH_PUT_RAW_OBJECT;
		}
		status=ssd_flashPut(pac->paio_ctxt, meta->pshard, &metaData, key, NULL, FLASH_PUT_TEST_NONEXIST|flags);
		if (status == FLASH_EOK) {
			status = ZS_SUCCESS;
		} else {
			status = ZS_FAILURE;
		}
		goto out;
	} else {

		ar.reqtype = APDBE;
		ar.prefix_delete = 0;
		ar.curtime = 0;
		ar.ctxt = pac->ctxt;
		ar.ctnr = cguid;
		ar.ctnr_type = SDF_OBJECT_CONTAINER;
		ar.internal_request = SDF_TRUE;    
		ar.internal_thread = fthSelf();

		if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != ZS_SUCCESS) {
			goto out;
		}

		ActionProtocolAgentNew(pac, &ar);

		status = ar.respStatus;
	}	

out:

	rel_cntr_map(cmap);

	return status;
}


ZS_status_t ZSDeleteObject(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t          	  cguid,
	char                	 *key,
	uint32_t             	  keylen
	)
{
    ZS_status_t        status	= ZS_FAILURE;
	bool thd_ctx_locked = false;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(80022, LOG_CAT,
               LOG_WARN, "Shutdown in Progress. Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
        if ( !zs_thread_state || !cguid || !keylen ) {
            if ( !zs_thread_state ) {
                plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
            }
            if ( !cguid ) {
                plat_log_msg(80050,LOG_CAT,LOG_DBG,
                             "Invalid container cguid:%lu",cguid);
            }
            if ( !keylen ) {
                plat_log_msg(80056,LOG_CAT,LOG_DBG,
                             "Invalid key length");
            }
            return ZS_INVALID_PARAMETER;
        }

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_delete_object(zs_thread_state, cguid, key, keylen, false);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}


static ZS_status_t
zs_flush_object(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t          	  cguid,
	char                	 *key,
	uint32_t             	  keylen
	)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    ZS_status_t        status;
	cntr_map_t *cmap = NULL;

    pac = (SDF_action_init_t *) zs_thread_state;

	if ( !cguid || !key )
		return ZS_INVALID_PARAMETER;

	cmap = get_cntr_map(cguid);
	if (!cmap) {
		return(ZS_CONTAINER_UNKNOWN);
	}

    if ( (status = zs_get_ctnr_status_cmap(cmap, cguid, 0)) != ZS_CONTAINER_OPEN ) {
        plat_log_msg( 160043, LOG_CAT, LOG_DIAG, "Container must be open to execute a flush object" );
        goto out;     
    }

    ar.reqtype = APFLS;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != ZS_SUCCESS) {
		goto out;
    }

    ActionProtocolAgentNew(pac, &ar);
    status = ar.respStatus;

out:
	rel_cntr_map(cmap);
    return status;
}


ZS_status_t ZSFlushObject(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t          	  cguid,
	char                	 *key,
	uint32_t             	  keylen
	)
{

    ZS_status_t        status	= ZS_FAILURE;
	bool thd_ctx_locked = false;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(80022, LOG_CAT,
               LOG_WARN, "Shutdown in Progress. Operation not allowed ");
		goto out;
	}
        if ( !zs_thread_state || !cguid || !keylen ) {
            if ( !zs_thread_state ) {
                plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
            }
            if ( !cguid ) {
                plat_log_msg(80050,LOG_CAT,LOG_DBG,
                             "Invalid container cguid:%lu",cguid);
            }
            if ( !keylen ) {
                plat_log_msg(80056,LOG_CAT,LOG_DBG,
                             "Invalid key length");
            }
            return ZS_INVALID_PARAMETER;
        }

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_flush_object(zs_thread_state, cguid, key, keylen);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}


static ZS_status_t
zs_flush_container(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t               cguid
	)
{
    SDF_appreq_t        	 ar;
    SDF_action_init_t  		*pac;
	ZS_status_t			 status			= ZS_FAILURE;
#ifdef SDFAPIONLY
    SDF_internal_ctxt_t     *pai			= (SDF_internal_ctxt_t *) zs_thread_state;
    struct shard            *shard          = NULL;
    flashDev_t              *flash_dev;
    SDF_container_meta_t     meta;
    struct SDF_shared_state *state          = &sdf_shared_state;
#endif
	cntr_map_t *cmap = NULL;

    if ( !cguid )
        return ZS_INVALID_PARAMETER;

	cmap = get_cntr_map(cguid);
	if (!cmap) {
		return(ZS_CONTAINER_UNKNOWN);
	}

    if ( (status = zs_get_ctnr_status_cmap(cmap, cguid, 0)) != ZS_CONTAINER_OPEN ) {
        plat_log_msg( 160044, LOG_CAT, LOG_DIAG, "Container must be open to execute a flush container" );
        goto out;     
    }

    pac = (SDF_action_init_t *) zs_thread_state;

    ar.reqtype = APFCO;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();

    ActionProtocolAgentNew(pac, &ar);

	status = ar.respStatus;

	if ( ZS_SUCCESS != status ) {
		rel_cntr_map(cmap);
    	return status;         
	}

#ifdef SDFAPIONLY
	if ((status = name_service_get_meta(pai, cguid, &meta)) == ZS_SUCCESS) {

#ifdef MULTIPLE_FLASH_DEV_ENABLED
		flash_dev = get_flashdev_from_shardid( state->config.flash_dev,
                                               meta.shard, 
											   state->config.flash_dev_count
											 );
#else
		flash_dev = state->config.flash_dev;
#endif
		shard = shardFind(flash_dev, meta.shard);

		if (shard)
			shardSync(shard);
	}
#endif /* SDFAPIONLY */

out:
	rel_cntr_map(cmap);
	return status;
}


ZS_status_t ZSFlushContainer(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t               cguid
	)
{
	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(80022, LOG_CAT,
               LOG_WARN, "Shutdown in Progress. Operation not allowed ");
		goto out;
	}
        if ( !zs_thread_state || !cguid ) {
            if ( !zs_thread_state ) {
                plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
            }
            if ( !cguid ) {
                plat_log_msg(80050,LOG_CAT,LOG_DBG,
                             "Invalid container cguid:%lu",cguid);
            }
            return ZS_INVALID_PARAMETER;
        }

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_flush_container(zs_thread_state, cguid);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}


static ZS_status_t
zs_flush_cache(
		struct ZS_thread_state  *zs_thread_state
		)
{
	ZS_status_t          status       = ZS_SUCCESS;
    char                 *key          = NULL;
    uint32_t              keylen       = 0;
    cntr_map_t           *cmap         = NULL;
    uint64_t              cmaplen      = 0;
    struct cmap_iterator *iterator     = NULL;

    iterator = zs_cmap_enum();

    if ( !iterator )
        return ZS_FAILURE;

    while ( zs_cmap_next_enum( iterator, &key, &keylen, (char **) &cmap, &cmaplen ) ) {
        plat_assert(cmap);
        if ( (cmap->cguid != ZS_NULL_CGUID ) && 
			 (cmap->state != ZS_CONTAINER_STATE_DELETE_PROG) &&
			 (cmap->state != ZS_CONTAINER_STATE_DELETE_OPEN) &&
			 (cmap->state != ZS_CONTAINER_STATE_DELETE_CLOSED) ) {
			(void)zs_flush_container( zs_thread_state, cmap->cguid );
        }
    }

    zs_cmap_finish_enum( iterator );
    return status;
}


ZS_status_t ZSFlushCache(
	struct ZS_thread_state  *zs_thread_state
	)
{
	ZS_status_t	status	= ZS_SUCCESS;
	bool thd_ctx_locked = false;

	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(80022, LOG_CAT,
               LOG_WARN, "Shutdown in Progress. Operation not allowed ");
		goto out;
	}
        if ( !zs_thread_state ) {
            plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
            return ZS_INVALID_PARAMETER;
        }

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_flush_cache(zs_thread_state);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}


ZS_status_t ZSContainerStat(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, int key, uint64_t *stat) 
{
    ZS_status_t   status;

	status = SDFContainerStatInternal( pai, container, key, stat ); 
	return status;
}

#if 0
static void zs_get_fth_stats(SDF_internal_ctxt_t *pai, char ** ppos, int * lenp,
                                 SDF_CONTAINER sdf_container, ZS_stats_t *stats )
{
    uint64_t            old_value;
    static uint64_t     idle_time = 0;
    static uint64_t     dispatch_time = 0;
    static uint64_t     low_prio_dispatch_time = 0;
    static uint64_t     num_dispatches = 0;
    static uint64_t     num_low_prio_dispatches = 0;
    static uint64_t     avg_dispatch = 0;
    static uint64_t     thread_time = 0;
    static uint64_t     ticks = 0;

    extern uint64_t Mcd_num_pending_ios;
    plat_snprintfcat( ppos, lenp, "STAT pending_ios %lu\r\n",
                      Mcd_num_pending_ios );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_PENDING_IOS] = Mcd_num_pending_ios;

    extern uint64_t Mcd_fth_waiting_io;
    plat_snprintfcat( ppos, lenp, "STAT fth_waiting_io %lu\r\n",
                      Mcd_fth_waiting_io );

    plat_snprintfcat( ppos, lenp, "STAT fth_reverses %lu\r\n",
                      (unsigned long) fthReverses );

    plat_snprintfcat( ppos, lenp, "STAT fth_float_stats" );
    for ( int i = 0; i <= fthFloatMax; i++ ) {
        plat_snprintfcat( ppos, lenp, " %lu",
                          (unsigned long) fthFloatStats[i] );
    }
    plat_snprintfcat( ppos, lenp, "\r\n");

    old_value = idle_time;
    ZSContainerStat( pai, sdf_container,
                             FLASH_FTH_SCHEDULER_IDLE_TIME,
                             &idle_time );
    plat_snprintfcat( ppos, lenp, "STAT fth_idle_time %lu %lu\r\n",
                      (unsigned long)idle_time,
                      (unsigned long)(idle_time - old_value) );

    old_value = num_dispatches;
    ZSContainerStat( pai, sdf_container,
                             FLASH_FTH_NUM_DISPATCHES,
                             &num_dispatches );
    plat_snprintfcat( ppos, lenp,
                      "STAT fth_num_dispatches %lu %lu\r\n",
                      num_dispatches, num_dispatches - old_value );

    old_value = dispatch_time;
    ZSContainerStat( pai, sdf_container,
                             FLASH_FTH_SCHEDULER_DISPATCH_TIME,
                             &dispatch_time );
    plat_snprintfcat( ppos, lenp, "STAT fth_dispatch_time %lu %lu\r\n",
                      dispatch_time, dispatch_time - old_value );

    old_value = num_low_prio_dispatches;
    ZSContainerStat( pai, sdf_container,
                             FLASH_FTH_NUM_LOW_PRIO_DISPATCHES,
                             &num_low_prio_dispatches );
    plat_snprintfcat( ppos, lenp,
                      "STAT fth_num_low_prio_dispatches %lu %lu\r\n",
                      num_low_prio_dispatches,
                      num_low_prio_dispatches - old_value );

    old_value = low_prio_dispatch_time;
    ZSContainerStat( pai, sdf_container,
                             FLASH_FTH_SCHEDULER_LOW_PRIO_DISPATCH_TIME,
                             &low_prio_dispatch_time );
    plat_snprintfcat( ppos, lenp,
                      "STAT fth_low_prio_dispatch_time %lu %lu\r\n",
                      low_prio_dispatch_time,
                      low_prio_dispatch_time - old_value );

    old_value = thread_time;
    ZSContainerStat( pai, sdf_container,
                             FLASH_FTH_TOTAL_THREAD_RUN_TIME,
                             &thread_time );
    plat_snprintfcat( ppos, lenp,
                      "STAT fth_thread_run_time %lu %lu\r\n",
                      thread_time, thread_time - old_value );

    old_value = ticks;
    ZSContainerStat( pai, sdf_container,
                             FLASH_TSC_TICKS_PER_MICROSECOND, &ticks);
    plat_snprintfcat( ppos, lenp,
                      "STAT fth_tsc_ticks_per_usec %lu %lu\r\n",
                      ticks, old_value );

    ZSContainerStat( pai, sdf_container,
                             FLASH_FTH_AVG_DISPATCH_NANOSEC,
                             &avg_dispatch );
    plat_snprintfcat( ppos, lenp,
                      "STAT fth_avg_dispatch_nanosec %lu\r\n",
                      avg_dispatch );
}
#endif

void zs_get_flash_map(struct ZS_thread_state *thd_state, ZS_cguid_t cguid, 
                       char *buf, int *size){
    int rc;
    uint64_t *stats_ptr;
    SDF_internal_ctxt_t *pai ;
    SDF_CONTAINER sdf_container = containerNull;
	cntr_map_t *cmap = NULL;

    *size = 0;
    stats_ptr = NULL;
    pai  = (SDF_internal_ctxt_t *)thd_state;
    SDFStartSerializeContainerOp(pai);

    cmap = zs_cmap_get_by_cguid(cguid);
    if (!cmap) {
        *size = sprintf(buf,"Unable to get container from cguid\n");
        SDFEndSerializeContainerOp(pai);
        return;
    }   
    if ( zs_get_ctnr_status(cguid, 0) != ZS_CONTAINER_OPEN ) {
        SDFEndSerializeContainerOp(pai);
        return;
    }

    sdf_container = cmap->sdf_container;
    ZSContainerStat( pai, sdf_container,
                             FLASH_SLAB_CLASS_SEGS,
                             (uint64_t *)&stats_ptr );
    if( stats_ptr != NULL ) {
        rc = snprintf(buf+ (*size),(STATS_BUFFER_SIZE - (*size)),
                                                       "   flash_class_map" );
        *size = *size + rc;
        for ( int i = 0; i < Mcd_osd_max_nclasses; i++ ) {
            rc = snprintf(buf+ (*size),(STATS_BUFFER_SIZE - (*size)),
                                                       " %lu", stats_ptr[i] );
            *size = *size + rc;
        }
        rc = snprintf(buf+ (*size),(STATS_BUFFER_SIZE - (*size)),"\n" );
        *size = *size + rc;
    }
    else {
        rc = snprintf(buf+ (*size),(STATS_BUFFER_SIZE - (*size)),
                                                      "   flash_class_map" );
        *size = *size + rc;
    }
    
    stats_ptr = NULL;
    ZSContainerStat( pai, sdf_container,
                             FLASH_SLAB_CLASS_SLABS,
                             (uint64_t *)&stats_ptr );
    if( stats_ptr != NULL ) {
        rc = snprintf(buf+ (*size),(STATS_BUFFER_SIZE - (*size)),
                                                       "   flash_slab_map" );
        *size = *size + rc;
        for ( int i = 0; i < Mcd_osd_max_nclasses; i++ ) {
            rc = snprintf(buf+ (*size),(STATS_BUFFER_SIZE - (*size)),
                                                       " %lu", stats_ptr[i] );
            *size = *size + rc;
        }
        rc = snprintf(buf+ (*size),(STATS_BUFFER_SIZE - (*size)),"\n" );
        *size = *size + rc;
    }
    else {
        rc = snprintf(buf+ (*size),(STATS_BUFFER_SIZE - (*size)), 
                                                        "   flash_slab_map" );
        *size = *size + rc;
    }

    stats_ptr = NULL;
    ZSContainerStat( pai, sdf_container,
                             FLASH_SPACE_COMP_HISTOGRAM,
                             (uint64_t *)&stats_ptr );
    if( stats_ptr != NULL ) {
        rc = snprintf(buf+ (*size),(STATS_BUFFER_SIZE - (*size)),
                                                       "   compression_histogram" );
        *size = *size + rc;
        for ( int i = 0; i < MCD_OSD_MAX_COMP_HIST; i++ ) {
            rc = snprintf(buf+ (*size),(STATS_BUFFER_SIZE - (*size)),
                                                       " %lu", stats_ptr[i] );
            *size = *size + rc;
        }    
        rc = snprintf(buf+ (*size),(STATS_BUFFER_SIZE - (*size)),"\n" );
        *size = *size + rc;
    }    
    else {
        rc = snprintf(buf+ (*size),(STATS_BUFFER_SIZE - (*size)), 
                                                        "   compression_histogram" );
        *size = *size + rc;
    }    


    SDFEndSerializeContainerOp(pai);
}



static void zs_get_flash_stats( SDF_internal_ctxt_t *pai, char ** ppos, int * lenp,
                                 SDF_CONTAINER sdf_container, ZS_stats_t *stats)
{
    uint64_t            space_allocated = 0;
    uint64_t            space_consumed = 0;
    uint64_t            num_objects = 0;
    uint64_t            num_created_objects = 0;
    uint64_t            num_evictions = 0;
    uint64_t            num_hash_evictions = 0;
    uint64_t            num_inval_evictions = 0;
    uint64_t            num_hash_overflows = 0;
    uint64_t            get_hash_collisions = 0;
    uint64_t            set_hash_collisions = 0;
    uint64_t            num_overwrites = 0;
    uint64_t            num_ops = 0;
    uint64_t            num_read_ops = 0;
    uint64_t            num_get_ops = 0;
    uint64_t            num_put_ops = 0;
    uint64_t            num_del_ops = 0;
    uint64_t            comp_bytes  = 0;
    uint64_t            num_ext_checks = 0;
    uint64_t            num_full_buckets = 0;
    uint64_t            val = 0;
    uint64_t          * stats_ptr = NULL;

    ZSContainerStat( pai, sdf_container,
                             FLASH_SLAB_CLASS_SEGS,
                             (uint64_t *)&stats_ptr );
    if( stats_ptr != NULL ) {
        plat_snprintfcat( ppos, lenp, "STAT flash_class_map" );
        for ( int i = 0; i < Mcd_osd_max_nclasses; i++ ) {
            plat_snprintfcat( ppos, lenp, " %lu", stats_ptr[i] );
        }
        plat_snprintfcat( ppos, lenp, "\r\n" );
    }
    else {
        plat_snprintfcat( ppos, lenp, "STAT flash_class_map:\r\n" );
    }
    stats_ptr = NULL;
    ZSContainerStat( pai, sdf_container,
                             FLASH_SLAB_CLASS_SLABS,
                             (uint64_t *)&stats_ptr );
    if( stats_ptr != NULL ) {
        plat_snprintfcat( ppos, lenp, "STAT flash_slab_map" );
        for ( int i = 0; i < Mcd_osd_max_nclasses; i++ ) {
            plat_snprintfcat( ppos, lenp, " %lu", stats_ptr[i] );
        }
        plat_snprintfcat( ppos, lenp, "\r\n" );
    }
    else {
        plat_snprintfcat( ppos, lenp, "STAT flash_slab_map:\r\n" );
    }

    ZSContainerStat( pai, sdf_container,
                             FLASH_SPACE_ALLOCATED,
                             &space_allocated );
    plat_snprintfcat( ppos, lenp,
                      "STAT flash_space_allocated %lu\r\n",
                      space_allocated );
    if (stats != NULL) 
        stats->flash_stats[ZS_FLASH_STATS_SPACE_ALLOCATED] = space_allocated;

    ZSContainerStat( pai, sdf_container,
                             FLASH_SPACE_CONSUMED,
                             &space_consumed );
    plat_snprintfcat( ppos, lenp, "STAT flash_space_consumed %lu\r\n",
                      space_consumed );
    if (stats != NULL) 
        stats->flash_stats[ZS_FLASH_STATS_SPACE_CONSUMED] = space_consumed;

    ZSContainerStat( pai, sdf_container,
                             FLASH_NUM_OBJECTS,
                             &num_objects );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_objects %lu\r\n",
                      num_objects );
    if (stats != NULL) 
        stats->flash_stats[ZS_FLASH_STATS_NUM_OBJS] = num_objects;

    ZSContainerStat( pai, sdf_container,
                             FLASH_NUM_CREATED_OBJECTS,
                             &num_created_objects );
    plat_snprintfcat( ppos, lenp,
                      "STAT flash_num_created_objects %lu\r\n",
                      num_created_objects );
    if (stats != NULL) 
        stats->flash_stats[ZS_FLASH_STATS_NUM_CREATED_OBJS] = num_created_objects;

    ZSContainerStat( pai, sdf_container,
                             FLASH_NUM_EVICTIONS,
                             &num_evictions );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_evictions %lu\r\n",
                      num_evictions );
    if (stats != NULL) 
        stats->flash_stats[ZS_FLASH_STATS_NUM_EVICTIONS] = num_evictions;

    ZSContainerStat( pai, sdf_container,
                                 FLASH_NUM_HASH_EVICTIONS,
                                 &num_hash_evictions );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_num_hash_evictions %lu\r\n",
                          num_hash_evictions );
    if (stats != NULL) 
        stats->flash_stats[ZS_FLASH_STATS_NUM_HASH_EVICTIONS] = num_hash_evictions;

    ZSContainerStat( pai, sdf_container,
                                 FLASH_NUM_INVAL_EVICTIONS,
                                 &num_inval_evictions );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_num_inval_evictions %lu\r\n",
                          num_inval_evictions );
    if (stats != NULL) 
        stats->flash_stats[ZS_FLASH_STATS_NUM_INVAL_EVICTIONS] = num_inval_evictions;

    ZSContainerStat( pai, sdf_container,
                                 FLASH_NUM_SOFT_OVERFLOWS,
                                 &num_hash_overflows );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_num_soft_overflows %lu\r\n",
                          num_hash_overflows );
    if (stats != NULL) 
        stats->flash_stats[ZS_FLASH_STATS_NUM_SOFT_OVERFLOWS] = num_hash_overflows;

    ZSContainerStat( pai, sdf_container,
                                 FLASH_NUM_HARD_OVERFLOWS,
                                 &num_hash_overflows );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_num_hard_overflows %lu\r\n",
                          num_hash_overflows );
    if (stats != NULL) 
        stats->flash_stats[ZS_FLASH_STATS_NUM_HARD_OVERFLOWS] = num_hash_overflows;

    ZSContainerStat( pai, sdf_container,
                             FLASH_GET_HASH_COLLISIONS,
                                 &get_hash_collisions );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_get_hash_collisions %lu\r\n",
                          get_hash_collisions );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_GET_HASH_COLLISION] = get_hash_collisions;


    ZSContainerStat( pai, sdf_container,
                                 FLASH_SET_HASH_COLLISIONS,
                                 &set_hash_collisions );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_set_hash_collisions %lu\r\n",
                          set_hash_collisions );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_SET_HASH_COLLISION] = set_hash_collisions;

    ZSContainerStat( pai, sdf_container,
                             FLASH_NUM_OVERWRITES,
                             &num_overwrites );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_overwrites %lu\r\n",
                      num_overwrites );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_NUM_OVERWRITES] = num_overwrites;

    ZSContainerStat( pai, sdf_container,
                             FLASH_OPS,
                             &num_ops );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_ops %lu\r\n",
                      num_ops );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_NUM_OPS] = num_ops;

    ZSContainerStat( pai, sdf_container,
                             FLASH_READ_OPS,
                             &num_read_ops );

    plat_snprintfcat( ppos, lenp, "STAT flash_num_read_ops %lu\r\n",
                      num_read_ops );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_NUM_READ_OPS] = num_read_ops;

    ZSContainerStat( pai, sdf_container,
                             FLASH_NUM_GET_OPS,
                             &num_get_ops );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_get_ops %lu\r\n",
                      num_get_ops );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_NUM_GET_OPS] = num_get_ops;

    ZSContainerStat( pai, sdf_container,
                             FLASH_NUM_PUT_OPS,
                             &num_put_ops );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_put_ops %lu\r\n",
                      num_put_ops );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_NUM_PUT_OPS] = num_put_ops;

    ZSContainerStat( pai, sdf_container,
                             FLASH_SPACE_COMP_BYTES,
                             &comp_bytes );
    if(stats != NULL) 
        stats->flash_stats[ZS_FLASH_STATS_COMP_BYTES] = comp_bytes;


    ZSContainerStat( pai, sdf_container,
                             FLASH_NUM_DELETE_OPS,
                             &num_del_ops );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_del_ops %lu\r\n",
                      num_del_ops );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_NUM_DEL_OPS] = num_del_ops;

    ZSContainerStat( pai, sdf_container,
                                 FLASH_NUM_EXIST_CHECKS,
                                 &num_ext_checks );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_get_exist_checks %lu\r\n",
                          num_ext_checks );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_GET_EXIST_CHECKS] = num_ext_checks;

    ZSContainerStat( pai, sdf_container,
                                 FLASH_NUM_FULL_BUCKETS,
                                 &num_full_buckets );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_num_full_buckets %lu\r\n",
                          num_full_buckets );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_NUM_FULL_BUCKETS] = num_full_buckets;

    ZSContainerStat( pai, sdf_container,
                             FLASH_NUM_FREE_SEGS,
                             &val );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_free_segs %lu\r\n",
                      val );
    if (stats != NULL) 
        stats->flash_stats[ZS_FLASH_STATS_NUM_FREE_SEGMENTS] = val;

    ZSContainerStat( pai, sdf_container,
                             FLASH_NUM_DATA_FSYNCS,
                             &val );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_data_fsyncs %lu\r\n",
                      val );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_NUM_DATA_FSYNCS] = val;

    ZSContainerStat( pai, sdf_container,
                             FLASH_NUM_DATA_WRITES,
                             &val );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_data_writes %lu\r\n",
                      val );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_NUM_DATA_WRITES] = val;

    ZSContainerStat( pai, sdf_container,
                             FLASH_NUM_LOG_WRITES,
                             &val );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_log_writes %lu\r\n",
                      val );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_NUM_LOG_WRITES] = val;

    ZSContainerStat( pai, sdf_container,
                             FLASH_NUM_LOG_FSYNCS,
                             &val );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_log_fsyncs %lu\r\n",
                      val );
    if (stats != NULL)
        stats->flash_stats[ZS_FLASH_STATS_NUM_LOG_FSYNCS] = val;

    /* Get Number of contexts */
    if(stats != NULL) {
         SDF_action_thrd_state_t *pts = ((SDF_action_init_t *)pai)->pts;
         stats->flash_stats[ZS_FLASH_STATS_THD_CONTEXTS] = pts->phs->contextcount;
    }

    if (stats != NULL) {
        extern uint64_t num_objs_expired;
        extern uint64_t num_scav_scan_yld;
        extern uint64_t num_scav_scan_complete;

        stats->flash_stats[ZS_FLASH_STATS_ESCVN_OBJ_DEL] = num_objs_expired;
        stats->flash_stats[ZS_FLASH_STATS_ESCVN_YLD_SCAN_CMPLTE] = num_scav_scan_complete;
        stats->flash_stats[ZS_FLASH_STATS_ESCVN_YLD_SCAN_RATE] = num_scav_scan_yld;
    }

    //mcd_osd_recovery_stats( mcd_osd_container_shard(c->mcd_container), ppos, lenp );

    //mcd_osd_auto_del_stats( mcd_osd_container_shard(c->mcd_container), ppos, lenp );

    //mcd_osd_eviction_age_stats( mcd_osd_container_shard(c->mcd_container), ppos, lenp );
}


static uint64_t parse_count( char * buf, char * name )
{
    char      * s;
    uint64_t    x;

    if ( NULL == ( s = strstr( buf, name ) ) ) {
        // this is ok--stats don't show up if they are zero
        return 0;
    }

    s += strlen( name );
    s++; /* skip '=' */

    if ( 1 != sscanf(s, "%"PRIu64"", &x) ) {
        plat_log_msg( 20648,
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_WARN,
                      "sdf stat %s parsing failure", name );
        return 0;
    }

    return x;
}
#if 0
char *ZSGetStatsDescription(ZS_cache_stat_t stat) {
    return ZSCacheStatsDescStrings[stat];
}
#endif

SDF_status_t zs_parse_access_stats (char * stat_buf,ZS_stats_t *stats ) {
    int i;                       
    uint64_t val;
                          
    if (stats == NULL ) { 
        return ZS_SUCCESS;
    }   
    for ( i = 0; i < ZS_N_ACCESS_TYPES; i++ ) {
        val = parse_count( stat_buf, zs_stats_access_type[i].stat_token );
        if ( val != 0 ) {        
            stats->n_accesses[i] = val;
        }
    }                     
    return ZS_SUCCESS;   
}   


SDF_status_t zs_parse_cache_stats (char * stat_buf,ZS_stats_t *stats ) {
    int i;
    uint64_t val;

    if (stats == NULL ) {
        return ZS_SUCCESS;
    }
    //fprintf(stderr,"zs_parse_cache_stats: %s\n",stat_buf);
    for ( i = 0; i < ZS_N_CACHE_STATS; i++ ) {
        val = parse_count( stat_buf, zs_stats_cache[i].stat_token );
        if ( val != 0 ) {
            stats->cache_stats[i] = val;
        }
    }
    return ZS_SUCCESS;
}

SDF_status_t
zs_parse_stats( char * stat_buf, int stat_key, uint64_t * pstat)
{
    int         i;
    uint64_t    stats[MCD_NUM_SDF_STATS];
    uint64_t    counts[MCD_NUM_SDF_COUNTS];

    if ( MCD_NUM_SDF_STATS <= stat_key ) {
#ifdef NOT_NEEDED
        plat_log_msg( 20649,
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_WARN,
                      "invalid stat key %d", stat_key );
#endif
        return SDF_FAILURE;
    }

    for ( i = 0; i < MCD_NUM_SDF_COUNTS; i++ ) {
        counts[i] = parse_count( stat_buf, ZSCacheCountStrings[i] );
    }

    if ( ( counts[apgrd] + counts[apgrx] ) >= counts[ahgtr] ) {
        stats[ZS_DRAM_CACHE_HITS] =
            counts[apgrd] + counts[apgrx] - counts[ahgtr];
    }
    else {
        plat_log_msg( 20650,
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_DEBUG,
                      "invalid sdf cache stats, rd=%lu rx=%lu tr=%lu",
                      counts[apgrd], counts[apgrx], counts[ahgtr] );
        stats[ZS_DRAM_CACHE_HITS] = 0;
    }
    stats[ZS_DRAM_CACHE_MISSES]   = counts[ahgtr];
    stats[ZS_FLASH_CACHE_HITS]    = counts[hagrc];
    stats[ZS_FLASH_CACHE_MISSES]  = counts[hagrf];
    stats[ZS_DRAM_CACHE_CASTOUTS] = counts[ahcwd];
    stats[ZS_DRAM_N_OVERWRITES]   = counts[owrites_s] + counts[owrites_m];
    stats[ZS_DRAM_N_IN_PLACE_OVERWRITES] = counts[ipowr_s] + counts[ipowr_m];
    stats[ZS_DRAM_N_NEW_ENTRY]    = counts[newents];

    *pstat = stats[stat_key];

    return ZS_SUCCESS;
}


static void get_zs_stats( SDF_internal_ctxt_t *pai, char ** ppos, int * lenp,
                               SDF_CONTAINER sdf_container, ZS_stats_t *stat)
{
    char                buf[BUF_LEN];
    uint64_t            n;
    uint64_t            sdf_cache_hits = 0;
    uint64_t            sdf_cache_misses = 0;
    uint64_t            sdf_flash_hits = 0;
    uint64_t            sdf_flash_misses = 0;
    uint64_t            sdf_cache_evictions = 0;
    uint64_t            sdf_n_overwrites = 0;
    uint64_t            sdf_n_in_place_overwrites = 0;
    uint64_t            sdf_n_new_entry = 0;
    //SDF_CONTAINER       container;
    local_SDF_CONTAINER lc;

    /*
     * get stats for this specific container (cguid)
     */
    //container = internal_clientToServerContainer( sdf_container );
    //lc = getLocalContainer( &lc, container );
    lc = getLocalContainer( &lc, sdf_container );
    if ( lc == NULL ) {  
	    //plat_assert( NULL != lc );
        plat_snprintfcat( ppos, lenp, "STAT sdf_stats: None\r\n");
        return;
    }

    memset( buf, 0, BUF_LEN);
    action_stats_new_cguid(pai, buf, BUF_LEN, lc->cguid );

    zs_parse_stats( buf, ZS_DRAM_CACHE_HITS, &sdf_cache_hits );
    zs_parse_stats( buf, ZS_DRAM_CACHE_MISSES, &sdf_cache_misses );
    if ( stat != NULL ) {
        stat->cache_stats[ZS_CACHE_STAT_CACHE_MISSES] = sdf_cache_misses;
        stat->cache_stats[ZS_CACHE_STAT_CACHE_HITS] = sdf_cache_hits;
    }
    zs_parse_stats( buf, ZS_FLASH_CACHE_HITS, &sdf_flash_hits );
    zs_parse_stats( buf, ZS_FLASH_CACHE_MISSES, &sdf_flash_misses );
    zs_parse_stats( buf, ZS_DRAM_CACHE_CASTOUTS, &sdf_cache_evictions );
    zs_parse_stats( buf, ZS_DRAM_N_OVERWRITES, &sdf_n_overwrites );
    zs_parse_stats( buf, ZS_DRAM_N_IN_PLACE_OVERWRITES, &sdf_n_in_place_overwrites );
    zs_parse_stats( buf, ZS_DRAM_N_NEW_ENTRY, &sdf_n_new_entry );

    plat_snprintfcat( ppos, lenp, "STAT sdf_cache_hits %lu\r\n",
                      sdf_cache_hits );

    plat_snprintfcat( ppos, lenp, "STAT sdf_cache_misses %lu\r\n",
                      sdf_cache_misses );

    plat_snprintfcat( ppos, lenp, "STAT sdf_flash_hits %lu\r\n",
                      sdf_flash_hits );

    plat_snprintfcat( ppos, lenp, "STAT sdf_flash_misses %lu\r\n",
                      sdf_flash_misses );

    plat_snprintfcat( ppos, lenp, "STAT sdf_cache_evictions %lu\r\n",
                      sdf_cache_evictions );

    plat_snprintfcat( ppos, lenp, "STAT sdf_n_overwrites %lu\r\n",
                      sdf_n_overwrites);

    plat_snprintfcat( ppos, lenp, "STAT sdf_n_in_place_overwrites %lu\r\n",
                      sdf_n_in_place_overwrites);

    plat_snprintfcat( ppos, lenp, "STAT sdf_n_new_entries %lu\r\n",
                      sdf_n_new_entry);

    ZSContainerStat( pai, sdf_container,
                             SDF_N_ONLY_IN_CACHE, &n);
    plat_snprintfcat( ppos, lenp, "STAT sdf_cache_only_items %lu\r\n", n);

    plat_snprintfcat( ppos, lenp, "STAT %s", buf );
    zs_parse_access_stats(buf,stat);
    zs_parse_cache_stats(buf,stat);

	/*
     * get stats for entire cache (for all cguid's)
     */
    memset( buf, 0, BUF_LEN );
    action_stats(pai, buf, BUF_LEN );
    plat_snprintfcat( ppos, lenp, "STAT %s\r\n", buf );

    // memset( buf, 0, 1024 );
    // home_stats( buf, 1024 );
    // plat_snprintfcat( ppos, lenp, "STAT %s\r\n", buf );
    if( stat == NULL ) {
        return;
    }

    /* Enumeration statistics */
    enum_stats_t e;
    uint64_t *p = stat->n_accesses;

    enumerate_stats(&e, lc->cguid);
    p[ZS_ACCESS_TYPES_ENUM_TOTAL]          = e.num_total;
    p[ZS_ACCESS_TYPES_ENUM_ACTIVE]         = e.num_active;
    p[ZS_ACCESS_TYPES_ENUM_OBJECTS]        = e.num_objects;
    p[ZS_ACCESS_TYPES_ENUM_CACHED_OBJECTS] = e.num_cached_objects;

    /* Async deletes stats */
#if 0
    uint32_t num_dels, dels_prog;
    get_async_delete_stats(&num_dels,&dels_prog);
    p[ZS_ACCESS_TYPES_NUM_CONT_DELETES_PEND] = num_dels;
    p[ZS_ACCESS_TYPES_NUM_CONT_DELETES_PROG] = dels_prog;
#endif
    
}
static void get_proc_stats( char ** ppos, int * lenp )
{
    int             rc;
    int             proc_fd = 0;
    char            proc_buf[128];
    char          * ptr = NULL;

    proc_fd = open( "/proc/stat", O_RDONLY );
    if ( 0 > proc_fd ) {
        mcd_log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_WARN,
                      "cannot open /proc/stat" );
        return;
    }
    mcd_log_msg( 20694, PLAT_LOG_LEVEL_TRACE,
                 "/proc/stat opened, fd=%d", proc_fd );

    memset( proc_buf, 0, 128 );
    if ( 0 < ( rc = read( proc_fd, proc_buf, 127 ) ) ) {
        if ( NULL != ( ptr = strchr( proc_buf, '\n' ) ) ) {
            *ptr = '\0';
            plat_snprintfcat( ppos, lenp, "STAT %s\r\n", proc_buf );
        }
    }
    else {
        mcd_log_msg( 20695, PLAT_LOG_LEVEL_WARN,
                     "failed to read from proc, rc=%d errno=%d", rc, errno );
    }

    close( proc_fd );
}

ZS_status_t ZSGetStatsStr (
    struct ZS_thread_state *zs_thread_state,
    ZS_cguid_t 			 cguid,
	char 					*stats_str,
    ZS_stats_t *stats
	) 
{
    ZS_status_t status = SDF_FAILURE;
    SDF_CONTAINER sdf_container = containerNull;
    SDF_internal_ctxt_t *pai = (SDF_internal_ctxt_t *) zs_thread_state;
    ZS_container_props_t   pprops = {0};
    SDF_container_meta_t    meta;
    time_t t;
    //SDF_status_t lock_status = ZS_SUCCESS;
    //SDF_cguid_t parent_cguid = SDF_NULL_CGUID;
    //int log_level = LOG_ERR;
    uint64_t space_used = 0;
    uint64_t maxbytes = 0;
    uint64_t num_evictions = 0;
    uint64_t      n = 0;
    //static struct timeval reset_time = {0, 0} ;
    int buf_len;
    char *temp;
    char * pos;
    ZS_container_props_t       dummy_prop;
	cntr_map_t *cmap = NULL;

	status = zs_validate_container_1(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		return status;
	}
    memset( (void *)&dummy_prop, 0, sizeof(dummy_prop) );

    SDFStartSerializeContainerOp(pai);

    cmap = zs_cmap_get_by_cguid(cguid);
    if (!cmap) {
        status = SDF_INVALID_PARAMETER;
        goto out;
    }
    else {
        sdf_container = cmap->sdf_container;
    }
    if ( (status = zs_get_ctnr_status(cguid, 0)) != ZS_CONTAINER_OPEN ) {
        goto out;
    }
    /* Check if this container is being deleted */
    if( cmap->state == ZS_CONTAINER_STATE_DELETE_PROG ||
        cmap->state == ZS_CONTAINER_STATE_DELETE_OPEN ||
        cmap->state == ZS_CONTAINER_STATE_DELETE_CLOSED ) {
        SDFEndSerializeContainerOp(pai);
        return ZS_FAILURE_CONTAINER_DELETED;     
    }

    if (( status = name_service_get_meta( pai, cguid, &meta )) == ZS_SUCCESS ) {
        status = zs_create_zs_props( &meta.properties, &pprops );
    }
    buf_len = STAT_BUFFER_SIZE;
    temp = stats_str;
    memset( temp, 0, buf_len );
    pos = temp;
    buf_len -= strlen( "\r\nEND\r\n" ) + 1;
    time(&t);
    plat_snprintfcat( &pos, &buf_len, "STAT Time %s\r\n", ctime(&t) );    

	// Get per container stats
    if ( status == ZS_SUCCESS ) {
        //Add container properties to the list
        plat_snprintfcat( &pos, &buf_len, "STAT Container Name %s size:%lukb FIFO:%d persistence:%d eviction:%d writethru:%d\r\n", 
                                           cmap->cname, pprops.size_kb, /*pprops.fifo_mode*/ 0, pprops.persistent, pprops.evicting, pprops.writethru );
    }
    else {
        plat_snprintfcat( &pos, &buf_len, "STAT Container Name %s\r\n", cmap->cname );
    }
    plat_snprintfcat( &pos, &buf_len, "STAT CGUID %lu\r\n", cguid );
    ZSContainerStat( pai, sdf_container, SDF_N_CURR_ITEMS, &n );
    plat_snprintfcat( &pos, &buf_len, "STAT curr_items %lu\r\n", n );

    ZSContainerStat( pai, sdf_container,
                             FLASH_SPACE_USED, &space_used );
    plat_snprintfcat( &pos, &buf_len, "STAT bytes %lu\r\n", space_used );
    ZSContainerStat( pai, sdf_container,
                                         FLASH_NUM_EVICTIONS, &num_evictions );
    plat_snprintfcat( &pos, &buf_len, "STAT evictions %lu\r\n",
                              num_evictions );
    ZSContainerStat( pai, sdf_container,
                                         FLASH_SHARD_MAXBYTES, &maxbytes );
    plat_snprintfcat( &pos, &buf_len, "STAT limit_maxbytes %lu\r\n",
                              maxbytes );

    plat_snprintfcat( &pos, &buf_len, "STAT container_num_evictions %lu\r\n",
                              cmap->container_stats.num_evictions );

	// Get server-wide flash stats
    zs_get_flash_stats( pai, &pos, &buf_len, sdf_container,stats);
    #if 0
    zs_get_fth_stats( pai, &pos, &buf_len, sdf_container,stats);
    #endif
    if (stats != NULL) {
        stats->flash_stats[ZS_FLASH_STATS_PENDING_IOS] = Mcd_num_pending_ios;
    }
    get_zs_stats(pai, &pos, &buf_len, sdf_container,stats);
#if 0
    if( (cguid > 3) && (stats != NULL) ) {
        uint64_t num_objs = 0;
        uint64_t used = 0;    
        /* Virtual containers. Get the size and space consumed */
        get_cntr_info(cguid,NULL, 0, &num_objs, &used, NULL, NULL);
        stats->flash_stats[ZS_FLASH_STATS_SPACE_CONSUMED] = used;
        stats->flash_stats[ZS_FLASH_STATS_NUM_CREATED_OBJS] = num_objs;
    }
#endif
    get_proc_stats(&pos, &buf_len);
out:

    //plat_log_msg(20819, LOG_CAT, log_level, "%s", ZSStrError(status));
    SDFEndSerializeContainerOp(pai);

    return (status);
}


ZS_status_t ZSGetStats(
	struct ZS_thread_state *zs_thread_state,
	ZS_stats_t             *stats
	)
{
    int i;
    ZS_status_t rc;
    ZS_stats_t VDC_flash_stats;
    char buf[BUF_LEN];
    char *ptr;
    if ( stats == NULL ) {
        return ZS_SUCCESS;
    }
    if ( !zs_thread_state ) {
        plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
    }

    memset (stats, 0, sizeof(ZS_stats_t));
    SDF_internal_ctxt_t *pai = (SDF_internal_ctxt_t *)zs_thread_state;
    action_stats(pai, buf, BUF_LEN );
    ptr = strstr(buf,"<CACHE>"); 
    zs_parse_access_stats(ptr,stats);
    zs_parse_cache_stats(ptr,stats);

    /* VDC flash stats */
    rc = ZSGetContainerStats(zs_thread_state, VDC_CGUID, &VDC_flash_stats);
    if ( rc != ZS_SUCCESS ) {
        plat_log_msg(160206, LOG_CAT, LOG_DBG,
            "ZSGetContainerStats failed for VDC (error:%s)",ZSStrError(rc));
        return ZS_FAILURE;
    }
    for (i = 0; i < ZS_N_FLASH_STATS; i++ ) {
        stats->flash_stats[i] = VDC_flash_stats.flash_stats[i];
    }

    /* Async deletes stats */
    uint32_t num_dels, dels_prog;
    uint64_t *p = stats->n_accesses;
    get_async_delete_stats(&num_dels,&dels_prog);
	get_nvram_stats(stats);
    p[ZS_ACCESS_TYPES_NUM_CONT_DELETES_PEND] = num_dels;
    p[ZS_ACCESS_TYPES_NUM_CONT_DELETES_PROG] = dels_prog;
    return( ZS_SUCCESS );
}

ZS_status_t update_btree_stats(ZS_cguid_t cguid,ZS_stats_t *stats) {
    ZS_ext_stat_t *estats;
    uint32_t n_stats, i;
    ZS_status_t rc;

    if( ext_cbs == NULL ) {
        /* Non btree container, return */
        return ZS_SUCCESS;
    }
    rc = ext_cbs->stats_cb(cguid,&estats,&n_stats);
    if( rc != ZS_SUCCESS ) {
        return rc;
    }
    //stats->n_accesses[ZS_ACCESS_TYPES_APCOE] = 0;
    //stats->n_accesses[ZS_ACCESS_TYPES_APPAE] = 0;
    //stats->n_accesses[ZS_ACCESS_TYPES_APSOE] = 0;
    for ( i = 0; i < n_stats; i++ ) {
       if( estats[i].ftype == ZS_STATS_TYPE_APP_REQ ) {
           stats->n_accesses[estats[i].fstat] = estats[i].value;
       }
       else if(estats[i].ftype == ZS_STATS_TYPE_BTREE ) {
           stats->btree_stats[estats[i].fstat] = estats[i].value;
       }
       else {
           fprintf(stderr,"Invalid zs type(%lu) for btree stats:%d\n",estats[i].ftype,i);
       }
#if 0
       else if( estats[i].ftype == ZS_STATS_TYPE_CACHE_TO_FLASH ) {
           stats->cache_stats[estats[i].fstat] = estats[i].value;
       }
#endif
    }
    plat_free(estats);
    return ZS_SUCCESS;
}

ZS_status_t update_log_stats(ZS_cguid_t cguid, ZS_stats_t *stats) {
	get_log_container_stats(cguid, stats);
	return ZS_SUCCESS;
}
ZS_status_t get_btree_num_objs(ZS_cguid_t cguid, uint64_t *num_objs, uint64_t *ov_objs) {
    ZS_ext_stat_t *estats;
    uint32_t n_stats, i;
    ZS_status_t rc;
	int j = 0;

    if( ext_cbs == NULL ) {
        /* Non btree container, return */
        return ZS_SUCCESS;
    }
    rc = ext_cbs->stats_cb(cguid,&estats,&n_stats);
    if( rc != ZS_SUCCESS ) {
        return rc;
    }
    for ( i = 0; (i < n_stats) && j < 2; i++ ) {
       if( estats[i].fstat == ZS_BTREE_NUM_OBJS ) {
           *num_objs = estats[i].value;
		   j++;
       } else if( estats[i].fstat == ZS_BTREE_OVERFLOW_NODES) {
           *ov_objs = estats[i].value;
		   j++;
       }
    }
    plat_free(estats);
    return ZS_SUCCESS;
}


#define MAX_STATS_RETRY 5
ZS_status_t ZSGetContainerStats(
	struct ZS_thread_state   	*zs_thread_state,
	ZS_cguid_t                	 cguid,
	ZS_stats_t					*stats
	)
{
    char stats_str[STAT_BUFFER_SIZE];
    ZS_status_t rc;
    uint64_t num_objs = 0;
    uint64_t used_space = 0;

	rc = zs_validate_container_1(cguid);
	if (ZS_SUCCESS != rc) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[rc]);
		return rc;
	}
    if ( !zs_thread_state || !cguid || !stats ) {
        if ( !zs_thread_state ) {
            plat_log_msg(80049,LOG_CAT,LOG_DBG,
                         "ZS Thread state is NULL");
        }
        if ( !cguid ) {
            plat_log_msg(80050,LOG_CAT,LOG_DBG,
                         "Invalid container cguid:%lu",cguid);
        }
        if ( !stats ) {
            plat_log_msg(80062,LOG_CAT,LOG_DBG,
                         "Invalid ZS_stats_t(NULL)");
        }
        return ZS_INVALID_PARAMETER;
    }

    // excluded VDC, trac #11290
    if (cguid < VDC_CGUID ) {
        /* The cguid is for physical container. So return error */
        return ZS_FAILURE;
    }

    memset (stats, 0, sizeof(ZS_stats_t));
    rc = ZSGetStatsStr(zs_thread_state,cguid,stats_str, stats);
    if ( rc != ZS_SUCCESS ) { 
        plat_log_msg( 80024, LOG_CAT, LOG_DIAG,
                      "Failed to get stats for container:%lu (%s)",cguid,ZS_Status_Strings[rc] );
    }

    get_cntr_info(cguid, NULL, 0, &num_objs, &used_space, NULL, NULL);
    stats->cntr_stats[ZS_CNTR_STATS_NUM_OBJS] = num_objs;
    stats->cntr_stats[ZS_CNTR_STATS_USED_SPACE] = used_space;

    //  no-op in this simple implementation
    slab_gc_get_stats(NULL, stats, NULL);

    ZS_container_props_t props;
    rc = ZSGetContainerProps(zs_thread_state, cguid, &props);
    if (props.flags & ZS_LOG_CTNR) {
		update_log_stats(cguid, stats);
    } else if (props.flags & ZS_HASH_CTNR) {
    } else {
        update_btree_stats(cguid,stats);
	}
    return rc;
}


ZS_status_t ZSBackupContainer(
	struct ZS_thread_state   *zs_thread_state,
	ZS_cguid_t                cguid,
	char                      *backup_directory
	)
{
    //  no-op in this simple implementation
    return( ZS_SUCCESS );
}

ZS_status_t ZSRestoreContainer(
	struct ZS_thread_state   *zs_thread_state,
	ZS_cguid_t                cguid,
	char                      *backup_directory
	)
{
    //  no-op in this simple implementation
    return( ZS_SUCCESS );
}

// Internal functions
#if 1
static SDF_container_props_t *zs_create_sdf_props(
    ZS_container_props_t           *zs_properties
    )
{
    SDF_container_props_t   *sdf_properties = (SDF_container_props_t *) plat_alloc ( sizeof ( SDF_container_props_t ) );

    if ( NULL != sdf_properties ) {
        sdf_properties->container_id.owner                    	= 0;
        sdf_properties->container_id.size                     	= zs_properties->size_kb;
        sdf_properties->container_id.container_id             	= 0;
        sdf_properties->container_id.sc_num_objs                 = (zs_properties->size_kb * 1024 / Mcd_osd_blk_size);

        sdf_properties->cguid                                 	= zs_properties->cguid;

        sdf_properties->container_type.type                   	= SDF_OBJECT_CONTAINER;
        sdf_properties->container_type.persistence            	= SDF_TRUE /* zs_properties->persistent */;
        sdf_properties->container_type.caching_container      	= zs_properties->evicting;
        sdf_properties->container_type.async_writes           	= zs_properties->async_writes;

        sdf_properties->replication.enabled                     = 0;
        sdf_properties->replication.type                        = SDF_REPLICATION_NONE;
        sdf_properties->replication.num_replicas                = 1;
        sdf_properties->replication.num_meta_replicas           = 0;
        sdf_properties->replication.synchronous                 = 1;

        sdf_properties->cache.not_cacheable                     = SDF_FALSE;
        sdf_properties->cache.shared                            = SDF_FALSE;
        sdf_properties->cache.coherent                          = SDF_FALSE;
        sdf_properties->cache.enabled                           = SDF_TRUE;
        sdf_properties->cache.writethru                         = zs_properties->writethru;
        sdf_properties->cache.size                              = 0;
        sdf_properties->cache.max_size                          = 0;

        sdf_properties->shard.enabled                           = SDF_TRUE;
        sdf_properties->shard.num_shards                        = 1 /* zs_properties->num_shards */;
        sdf_properties->fifo_mode                               = SDF_FALSE /* zs_properties->fifo_mode */;
        sdf_properties->durability_level                        = SDF_NO_DURABILITY;

    	if ( zs_properties->durability_level == ZS_DURABILITY_HW_CRASH_SAFE )
			sdf_properties->durability_level = SDF_FULL_DURABILITY;
    	else if ( zs_properties->durability_level == ZS_DURABILITY_SW_CRASH_SAFE )
        	sdf_properties->durability_level = SDF_RELAXED_DURABILITY;

		sdf_properties->flash_only                              = zs_properties->flash_only;
		sdf_properties->cache_only                              = zs_properties->cache_only;
                sdf_properties->compression = zs_properties->compression;
        sdf_properties->flags = zs_properties->flags;        
    }

    return sdf_properties;
}
#else
static SDF_container_props_t *zs_create_sdf_props(
    ZS_container_props_t           *zs_properties,
	ZS_internal_container_props_t	*zs_internal_properties
    )
{
    SDF_container_props_t   *sdf_properties = (SDF_container_props_t *) plat_alloc ( sizeof ( SDF_container_props_t ) );

    if ( NULL != sdf_properties ) {
        sdf_properties->container_id.owner                      = 0;
        sdf_properties->container_id.size                       = zs_properties->size_kb;
        sdf_properties->container_id.container_id               = 0 /* zs_internal_properties->cid */;
        sdf_properties->container_id.owner                      = 0;
        sdf_properties->container_id.num_objs                   = (zs_properties->size_kb * 1024 / Mcd_osd_blk_size);

        sdf_properties->cguid                                   = zs_internal_properties->cguid;

        sdf_properties->container_type.type                     = SDF_OBJECT_CONTAINER;
        sdf_properties->container_type.persistence              = zs_properties->persistent;
        sdf_properties->container_type.caching_container        = zs_properties->evicting;
        sdf_properties->container_type.async_writes             = zs_internal_properties->async_writes;

        sdf_properties->replication.enabled                     = 0;
        sdf_properties->replication.type                        = SDF_REPLICATION_NONE;
        sdf_properties->replication.num_replicas                = 1;
        sdf_properties->replication.num_meta_replicas           = 0;
        sdf_properties->replication.synchronous                 = 1;

        sdf_properties->cache.not_cacheable                     = SDF_FALSE;
        sdf_properties->cache.shared                            = SDF_FALSE;
        sdf_properties->cache.coherent                          = SDF_FALSE;
        sdf_properties->cache.enabled                           = SDF_TRUE;
        sdf_properties->cache.writethru                         = zs_properties->writethru;
        sdf_properties->cache.size                              = 0;
        sdf_properties->cache.max_size                          = 0;

        sdf_properties->shard.enabled                           = SDF_TRUE;
        sdf_properties->shard.num_shards                        = zs_internal_properties->num_shards;
        sdf_properties->fifo_mode                               = zs_internal_properties->fifo_mode;

        sdf_properties->durability_level                        = SDF_NO_DURABILITY;
        sdf_properties->flags = zs_properties->flags;

        if ( zs_properties->durability_level == ZS_DURABILITY_HW_CRASH_SAFE )
            sdf_properties->durability_level = SDF_FULL_DURABILITY;
        else if ( zs_properties->durability_level == ZS_DURABILITY_SW_CRASH_SAFE )
            sdf_properties->durability_level = SDF_RELAXED_DURABILITY;
    }

    return sdf_properties;
}
#endif


static ZS_status_t zs_create_zs_props(
    SDF_container_props_t   *sdf_properties,
    ZS_container_props_t   *zs_properties
    )
{
	ZS_status_t	status	= ZS_INVALID_PARAMETER;

    if ( NULL != zs_properties ) {
        zs_properties->size_kb 							= sdf_properties->container_id.size;
        zs_properties->fifo_mode							= sdf_properties->fifo_mode;
        zs_properties->persistent							= sdf_properties->container_type.persistence;
        zs_properties->evicting							= sdf_properties->container_type.caching_container;
        zs_properties->writethru							= sdf_properties->cache.writethru;
        zs_properties->async_writes						= sdf_properties->container_type.async_writes;
        zs_properties->cguid       						= sdf_properties->cguid;
        zs_properties->cid       							= sdf_properties->container_id.container_id;
        zs_properties->num_shards							= sdf_properties->shard.num_shards;

		zs_properties->durability_level = ZS_DURABILITY_PERIODIC;
		if ( sdf_properties->durability_level == SDF_FULL_DURABILITY )
	    	zs_properties->durability_level = ZS_DURABILITY_HW_CRASH_SAFE;
		else if ( sdf_properties->durability_level == SDF_RELAXED_DURABILITY )
	    	zs_properties->durability_level = ZS_DURABILITY_SW_CRASH_SAFE;
		zs_properties->flash_only = sdf_properties->flash_only;
		zs_properties->cache_only = sdf_properties->cache_only;
                zs_properties->compression = sdf_properties->compression;

        zs_properties->flags = sdf_properties->flags;

		status												= ZS_SUCCESS;
    }

    return status;
}

ZS_status_t zs_resize_container(
    struct ZS_thread_state *zs_thread_state,
    ZS_cguid_t              cguid,
    uint64_t                 size
    )
{   
    ZS_status_t                 status     = ZS_FAILURE;
    SDF_internal_ctxt_t         *pai        = (SDF_internal_ctxt_t *) zs_thread_state;
    SDF_container_meta_t         meta;
	cntr_map_t                  *cmap       = NULL;
        
    // Cannot change physical container size
    if ( CMC_CGUID == cguid || VMC_CGUID == cguid || VDC_CGUID == cguid ) {
        plat_log_msg( 150078, LOG_CAT, LOG_ERR, "Cannnot change container size" );
        return ZS_CANNOT_REDUCE_CONTAINER_SIZE;
    }
	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
    cmap = zs_cmap_get_by_cguid( cguid );
    if ( !cmap ) {
        plat_log_msg( 150082, LOG_CAT, LOG_ERR, "Cannnot find container id %lu", cguid );
        status = ZS_CONTAINER_UNKNOWN;
		goto out;
    } else if ( size < cmap->size_kb ) {
            plat_log_msg( 150079, LOG_CAT, LOG_DIAG, "Cannnot reduce container size" );
            status = ZS_CANNOT_REDUCE_CONTAINER_SIZE;
			goto out;
    }

	if ( ( status = name_service_get_meta( pai,
                                           cguid,
                                           &meta ) ) != ZS_SUCCESS) {
		plat_log_msg( 150080, LOG_CAT, LOG_ERR, "Cannnot read container metadata for %lu", cguid );
   	} else {

		meta.properties.container_id.size = size; 

		if ( ( status = name_service_put_meta( pai, 
											   cguid, 
											   &meta ) ) != ZS_SUCCESS ) {
           	plat_log_msg( 150081, LOG_CAT, LOG_ERR, "Cannnot write container metadata for %lu", cguid );
       	} else {
           	cmap->size_kb = size;
			status = ZS_SUCCESS;
		}
    }

out:
    return status;
}

static void *zs_vc_thread(
	void *arg
	) 
{
	struct ZS_thread_state *zs_thread_state		= NULL;
	ZS_status_t             status         		= ZS_FAILURE;
	struct ZS_iterator     *_zs_iterator  		= NULL;
	int                      i              		= 1000;
	int                      j              		= 0;
	int                      k              		= 0;
	char                    *key            		= NULL;
	uint32_t                 keylen         		= 0;
	uint64_t                 datalen        		= 0;
	SDF_container_meta_t    *meta           		= NULL;
	struct SDF_shared_state *state 	= &sdf_shared_state;
	int	flags					= ZS_CTNR_CREATE;
	ZS_cguid_t *deletes = NULL;


	if ( ZS_SUCCESS != ZSInitPerThreadState((struct ZS_state *)arg, 
					( struct ZS_thread_state ** ) &zs_thread_state)) {
		plat_log_msg( 150088,
			  LOG_CAT,
			  LOG_ERR,
			  "Unable to initialize ZS thread state, exiting" );
		return NULL;
	}


	if ( state->config.system_recovery == SYS_FLASH_RECOVERY )
	    flags = 0;  // Just open the VMC/VDC
	if ( ( status = zs_vc_init( zs_thread_state, flags ) ) != ZS_SUCCESS ) {
	    plat_log_msg(150076,
			 LOG_CAT, LOG_ERR,
			 "Failed to open support containers: %s", ZSStrError( status ) );
		ZSReleasePerThreadState(&zs_thread_state);
		return NULL;
	}

	deletes = (ZS_cguid_t *) plat_alloc(sizeof(*deletes) * max_num_containers);
	if (deletes == NULL) {
		ZSReleasePerThreadState(&zs_thread_state);
		return NULL;
	}
	memset(deletes, 0, sizeof(*deletes) * max_num_containers);

	do {
		status = ZSEnumerateContainerObjects(zs_thread_state, 
						VMC_CGUID,  
						&_zs_iterator);
	} while (status == ZS_FLASH_EBUSY && i--);

	while ((status = ZSNextEnumeratedObject(zs_thread_state,
						 _zs_iterator,
						 &key,
						 &keylen,
						 (char **) &meta,
						 &datalen
					       )) == ZS_SUCCESS) {

	    status = zs_cmap_create( meta->cname,
	                              meta->cguid,
	                              meta->properties.container_id.size,
	                              ZS_CONTAINER_STATE_CLOSED,
	                              meta->properties.container_type.caching_container
#if 1//Rico - lc
	                              ,
	                              meta->flags&ZS_LOG_CTNR
#endif
	                            );
	    if (meta->flags&ZS_LOG_CTNR) {
		    lc_exists = 1;
	    }

	    if ( ZS_SUCCESS != status ) {
           	plat_log_msg( 150134, LOG_CAT, LOG_ERR, "Failed to create cmap for %s - %lu", meta->cname, meta->cguid );
	        continue; 
            } else {
           	plat_log_msg( 150135, LOG_CAT, LOG_DBG, "Created cmap for %s - %lu", meta->cname, meta->cguid );
            }

		for ( j = 0; j < max_num_containers; j++ ) {
			if (Mcd_containers[j].cguid == 0) {
				Mcd_containers[j].cguid         = meta->cguid;
				Mcd_containers[j].container_id  = meta->properties.container_id.container_id;
				memcpy( Mcd_containers[j].cname, meta->cname, strlen( meta->cname ) );
			    break;
	        }
		}

		if ( meta->delete_in_progress ) {
			deletes[k] = meta->cguid;
			k++;
		}
	}
    
	status = ZSFinishEnumeration( zs_thread_state, 
				   _zs_iterator);
	
	for( k = 0; k < max_num_containers && deletes[k] != ZS_NULL_CGUID; k++ ) {
		if ( ( status = zs_delete_container( zs_thread_state, deletes[k], ZS_VIRTUAL_CNTR) ) != ZS_SUCCESS )
			plat_log_msg( 150098,
				  LOG_CAT, 
				  LOG_ERR, 
				  "Failed to delete container %lu during recovery - %s", 
				  deletes[k], 
				  ZSStrError( status ) );
	}

	ZSReleasePerThreadState(&zs_thread_state);
	plat_free(deletes);
	return NULL;
}

static void 
zs_start_vc_thread(
	struct ZS_state *zs_state
	) 
{
    pthread_t 	thd;
    int 		rc		= -1;

    rc = pthread_create( &thd,
						 NULL,
						 zs_vc_thread,
						 (void *) zs_state
						);

    if ( rc != 0 ) {
        plat_log_msg( 150089,
					  LOG_CAT,
					  LOG_ERR,
					  "Unable to start the virtual container initialization thread.");
    }

	pthread_join( thd, NULL );
}
#define MAX_PHYSICAL_CONT_SIZE 2147483648

static ZS_status_t
zs_vc_init(
	struct ZS_thread_state  *zs_thread_state,
    int                  	  flags
    )
{
    ZS_container_props_t    p;
    ZS_cguid_t              cguid       = ZS_NULL_CGUID;
    SDF_status_t             status      = ZS_FAILURE;
    SDF_internal_ctxt_t		*pai		= (SDF_internal_ctxt_t *) zs_thread_state;

    // Create the VMC
    ZSLoadCntrPropDefaults(&p);
    p.durability_level      = ZS_DURABILITY_HW_CRASH_SAFE;
    plat_log_msg(80035,LOG_CAT, LOG_DBG, "%s Virtual Metadata Container"
                          " (name = %s,size = %lu kbytes,"
                          "persistence = %s,eviction = %s,writethrough = %s,fifo = %s,"
                          "async_writes = %s,durability = %s)",
                          flags & ZS_CTNR_CREATE?"Creating":"Opening",
                          VMC_PATH,p.size_kb, get_bool_str(p.persistent),
                          get_bool_str(p.evicting),get_bool_str(p.writethru),
                          get_bool_str(p.fifo_mode),get_bool_str(p.async_writes),
                          get_durability_str(p.durability_level));

    if ((status = ZSOpenPhysicalContainer(pai, VMC_PATH, &p, flags, &cguid)) != SDF_SUCCESS) {
        plat_log_msg(150057, LOG_CAT, LOG_ERR, "Failed to create VMC container - %s\n", ZSStrError(status));
        return status;
    }

    // Create the VDC
    ZSLoadCntrPropDefaults(&p);
    p.durability_level      = ZS_DURABILITY_HW_CRASH_SAFE;
	p.size_kb               = (uint64_t)(((uint64_t)getProperty_uLongLong("ZS_FLASH_SIZE", ZS_MIN_FLASH_SIZE)) * 1024 * 1024 -
							  (2 * ZS_DEFAULT_CONTAINER_SIZE_KB) - (32 * 1024)); // Minus CMC/VMC allocation & super block;

    plat_log_msg(80037,LOG_CAT, LOG_DBG, "%s Virtual Data Container"
                           " (name = %s,size = %lu kbytes,"
                           "persistence = %s,eviction = %s,writethrough = %s,fifo = %s,"
                           "async_writes = %s,durability = %s)",
                           flags & ZS_CTNR_CREATE?"Creating":"Opening",
                           VDC_PATH,p.size_kb, get_bool_str(p.persistent),
                           get_bool_str(p.evicting),get_bool_str(p.writethru),
                           get_bool_str(p.fifo_mode),get_bool_str(p.async_writes),
                           get_durability_str(p.durability_level));

    if ((status = ZSOpenPhysicalContainer(pai, VDC_PATH, &p, flags, &cguid)) != SDF_SUCCESS) {
        plat_log_msg(150114, LOG_CAT, LOG_ERR, "Failed to create VDC container - %s\n", ZSStrError(status));
        return status;
    }

    plat_log_msg(20168, LOG_CAT, LOG_DBG, "%s\n", ZSStrError(status));

    return status;
}

// Generate a unique container id. Check the metadata map for existing id allocation.
// Note that this function returns a 16bit value, where the cguid is actually 64bit.
// We need to change to a 16bit cguid eventually.
static ZS_status_t zs_generate_cguid(
	struct ZS_thread_state *zs_thread_state,
        char                   *cname,
	ZS_cguid_t             *cguid
	)
{ 
    ZS_status_t                   status      = ZS_OBJECT_EXISTS;
    struct SDF_shared_state      *state       = &sdf_shared_state; 
    uint16_t                      i           = 0; 
    uint16_t                      start	      = 0;

    start = state->config.cguid_counter;

    // Check for the ZS internal containers, these have fixed cguid.
    if (strcmp(cname, CMC_PATH) == 0)
        *cguid = CMC_CGUID;
    else if (strcmp(cname, VMC_PATH) == 0)
        *cguid = VMC_CGUID;
    else if (strcmp(cname, VDC_PATH) == 0)
        *cguid = VDC_CGUID;
    else if (strcmp(cname, SEQNO_CONTAINER_NAME) == 0)
        *cguid = SEQNO_CONTAINER_CGUID;
    else if (strcmp(cname, PSTATS_CONTAINER_NAME) == 0)
        *cguid = PSTATS_CONTAINER_CGUID;

    if (*cguid != ZS_NULL_CGUID) {
        status = ZS_SUCCESS;
        goto out;
    }

    // We allow the counter to rollover as we check for duplicates
    for ( i = start + 1; i != start; i++ ) { 
        state->config.cguid_counter += 1; 
        /* 
         * 
         * Skip cguids 0 and max  */	
        if (state->config.cguid_counter == max_num_containers ||
            state->config.cguid_counter == 0) {
            state->config.cguid_counter = LAST_INTERNAL_CGUID; 
        } 
        if ( !zs_cmap_get_by_cguid( state->config.cguid_counter ) ) { 
           *cguid = state->config.cguid_counter; 
           status = ZS_SUCCESS; 
           break; 
        }
    }

out:
    plat_log_msg( 150083, LOG_CAT, LOG_DBG, "%lu - %s\n", *cguid, ZSStrError( status ) );

    if (*cguid == 0) { 
        plat_assert(status != ZS_SUCCESS); 
    }

    return status;
}


/*
 * Delete all objects.
 */
static ZS_status_t
zs_delete_objects(struct ZS_thread_state *ts, ZS_cguid_t cguid)
{
    ZS_status_t s;
    struct shard *shard = NULL;
    SDF_action_init_t *pai = (SDF_action_init_t *) ts;

    s = cguid_to_shard(pai, cguid, &shard, 1);
    if (s != ZS_SUCCESS)
        return s;

    delete_all_objects(pai, shard, cguid);
    return ZS_SUCCESS;
}


/**
 * @brief Start transaction
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @return ZS_SUCCESS on success
 *         ZS_TRANS_LEVEL_EXCEEDED if transaction is nested too deeply
 *         ZS_OUT_OF_MEM if memory exhausted
 *         ZS_FAILURE for error unspecified
 */
ZS_status_t ZSTransactionStart(
	struct ZS_thread_state	*zs_thread_state
	)
{

	if (zs_thread_state == 0)
		plat_log_msg( 80049, LOG_CAT, LOG_DBG, "ZS Thread state is NULL");

	switch (mcd_trx_start( )) {
	case MCD_TRX_OKAY:
		return (ZS_SUCCESS);
	case MCD_TRX_TOO_MANY:
		return (ZS_TRANS_LEVEL_EXCEEDED);
	case MCD_TRX_NO_MEM:
		return (ZS_OUT_OF_MEM);
	}
	return (ZS_FAILURE);
}


/**
 * @brief Commit transaction
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @return ZS_SUCCESS on success
 *         ZS_FAILURE_NO_TRANS if there is no active transaction in the current thread
 *         ZS_TRANS_ABORTED if transaction aborted due to excessive size or internal error
 */
ZS_status_t ZSTransactionCommit(
	struct ZS_thread_state	*zs_thread_state
	)
{
	if (zs_thread_state == 0) {
		plat_log_msg( 80049, LOG_CAT, LOG_DBG,
						"ZS Thread state is NULL");
	}
	if (Enable_async_writes) {
		uint64_t trx_id = mcd_trx_id();
		if (!trx_id)
			return ZS_FAILURE_NO_TRANS;

		mcd_trx_t s = mcd_trx_detach();
		if (s == MCD_TRX_NO_TRANS)
			return (ZS_FAILURE_NO_TRANS);
		if (s != MCD_TRX_OKAY)
			return ZS_FAILURE;

		async_commit(zs_thread_state, trx_id);
		return ZS_SUCCESS;
	}

	switch (mcd_trx_commit( zs_thread_state)) {
	case MCD_TRX_NO_TRANS:
		return (ZS_FAILURE_NO_TRANS);
	case MCD_TRX_BAD_SHARD:
	case MCD_TRX_TOO_BIG:
		return (ZS_TRANS_ABORTED);
	case MCD_TRX_OKAY:
		return (ZS_SUCCESS);
	}
	return (ZS_FAILURE);
}


/**
 * @brief Roll back transaction
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @return ZS_SUCCESS on success
 *         ZS_FAILURE_NO_TRANS if there is no active transaction in the current thread
 *         ZS_TRANS_ABORTED if transaction aborted due to excessive size or internal error
 */
ZS_status_t ZSTransactionRollback(
	struct ZS_thread_state	*zs_thread_state
	)
{

	switch (mcd_trx_rollback( zs_thread_state)) {
	case MCD_TRX_NO_TRANS:
		return (ZS_FAILURE_NO_TRANS);
	case MCD_TRX_BAD_SHARD:
	case MCD_TRX_TOO_BIG:
		return (ZS_TRANS_ABORTED);
	case MCD_TRX_HASHTABLE_FULL:
		return (ZS_CONTAINER_FULL);
	case MCD_TRX_OKAY:
		return (ZS_SUCCESS);
	}
	return (ZS_FAILURE);
}


/**
 * @brief Quit a transaction
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @return ZS_SUCCESS on success
 *         ZS_FAILURE_NO_TRANS if there is no active transaction in the current thread
 */
ZS_status_t ZSTransactionQuit(
	struct ZS_thread_state	*zs_thread_state
	)
{

	/* code here */
	return (ZS_FAILURE);
}


/**
 * @brief ID of current transaction
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @return Non-zero transaction ID on success
 *         Zero if there is no active transaction in the current thread
 */
uint64_t ZSTransactionID(
	struct ZS_thread_state	*zs_thread_state
	)
{

	return (mcd_trx_id( ));
}


/**
 * @brief Perform internal transaction service
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @return ZS_SUCCESS on success
 *         ZS_FAILURE_OPERATION_DISALLOWED for unbalanced/nested rollback brackets
 *         ZS_FAILURE for error unspecified
 */
ZS_status_t ZSTransactionService(
	struct ZS_thread_state	*zs_thread_state,
	int			cmd,
	void			*arg
	)

{
	switch (mcd_trx_service( zs_thread_state, cmd, arg)) {
	case MCD_TRX_OKAY:
		return (ZS_SUCCESS);
	case MCD_TRX_BAD_CMD:
		return (ZS_FAILURE_OPERATION_DISALLOWED);
	}
	return (ZS_FAILURE);
}

/*
 * ZSTransactionGetMode
 */
ZS_status_t
ZSTransactionGetMode(struct ZS_thread_state *zs_thread_state, int *mode)
{
	fprintf(stderr, "ZS: ZSTransactionGetMode without unified FDF is not supported\n");
	return ZS_UNSUPPORTED_REQUEST;
}

/*
 * ZSTransactionSetMode
 */
ZS_status_t
ZSTransactionSetMode(struct ZS_thread_state *zs_thread_state, int mode)
{
	fprintf(stderr, "ZS: ZSTransactionGetMode without unified FDF is not supported\n");
	return ZS_UNSUPPORTED_REQUEST;
}

/**
 * @brief Return version of ZS
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @return String having the versions
 * 	   NULL if failed internally
 */

ZS_status_t
ZSGetVersion(
	char **str
	)
{
	if (!str)
		return ZS_INVALID_PARAMETER;

#ifdef ZS_REVISION
	*str = plat_strdup(ZS_REVISION);
#else
	*str = NULL;
#endif

	return *str ? ZS_SUCCESS : ZS_FAILURE;
}

ZS_status_t ZSGetRange(struct ZS_thread_state *thrd_state, 
                         ZS_cguid_t              cguid,
                         ZS_indexid_t            indexid,
                         struct ZS_cursor      **cursor,
                         ZS_range_meta_t        *meta)
{
	fprintf(stderr, "ZS: ZSGetRange without btree is not supported\n");
	return ZS_UNSUPPORTED_REQUEST;
}

ZS_status_t
ZSGetNextRange(struct ZS_thread_state *thrd_state, 
                struct ZS_cursor       *cursor,
                int                      n_in, 
                int                     *n_out,
                ZS_range_data_t        *values)
{
	fprintf(stderr, "ZS: ZSGetNextRange without btree is not supported\n");
	return ZS_UNSUPPORTED_REQUEST;
}

ZS_status_t 
ZSGetRangeFinish(struct ZS_thread_state *thrd_state, 
                  struct ZS_cursor *cursor)
{
	fprintf(stderr, "ZS: ZSGetRangeFinish without btree is not supported\n");
	return ZS_UNSUPPORTED_REQUEST;
}

ZS_status_t 
ZSMPut(struct ZS_thread_state *zs_thread_state, 
	ZS_cguid_t cguid,
	uint32_t num_objs,
	ZS_obj_t *objs,
	uint32_t flags,
	uint32_t *objs_written)
{
	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;
	cntr_map_t *cmap = NULL;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(80022, LOG_CAT,
               LOG_WARN, "Shutdown in Progress. Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
	if ( !zs_thread_state || !cguid || !objs_written) {
		if ( !zs_thread_state ) {
			plat_log_msg(80049,LOG_CAT,LOG_DBG,
						 "ZS Thread state is NULL");
		}
		if ( !cguid ) {
			plat_log_msg(80050,LOG_CAT,LOG_DBG,
						 "Invalid container cguid:%lu",cguid);
		}
		if ( !objs_written) {
			plat_log_msg(170047,LOG_CAT,LOG_DBG,
						 "objs_written parameter cannot be NULL");
		}
		return ZS_INVALID_PARAMETER;
	}

	cmap = get_cntr_map(cguid);
	if (!cmap)
		return(ZS_CONTAINER_UNKNOWN);
	if ( (status = zs_get_ctnr_status_cmap(cmap, cguid, 0)) != ZS_CONTAINER_OPEN ) {
		plat_log_msg( 160040, LOG_CAT, LOG_DIAG, "Container must be open to execute a write object" );
		goto out;     
	}

	if (!cmap->lc) {
		plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, LOG_DIAG, "ZSMPut is supported only on btree and log containers\n");
		status = ZS_UNSUPPORTED_REQUEST;
		goto out;
	}

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = lc_mput(zs_thread_state, cguid, num_objs, objs, flags, objs_written);

out:
	if (cmap) {
		if (status == ZS_SUCCESS) {
			inc_cntr_map_by_map(cmap, cguid, *objs_written, 0, 0);
		}
		rel_cntr_map(cmap);
	}
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}

ZS_status_t
ZSRangeUpdate(struct ZS_thread_state *zs_thread_state, 
	       ZS_cguid_t cguid,
	       char *range_key,
	       uint32_t range_key_len,
	       ZS_range_update_cb_t callback_func,
	       void * callback_args,	
	       ZS_range_cmp_cb_t range_cmp_callback,
	       void *range_cmp_cb_args,
	       uint32_t *objs_updated)
{
	fprintf(stderr, "ZS: ZSRangeUpdate without btree is not supported\n");
	return ZS_UNSUPPORTED_REQUEST;
}

ZS_status_t
ZSCheckBtree(struct ZS_thread_state *zs_thread_state, 
	       ZS_cguid_t cguid, uint64_t flags)
{

	fprintf(stderr, "ZS: ZSCheckBtree without btree is not supported\n");
	return ZS_FAILURE;
}

ZS_status_t
ZSCheckFlog()
{
    if (0 == mcd_check_flog())
        return ZS_SUCCESS;
    else
        return ZS_FAILURE;
}

ZS_status_t
ZSCheckPOT()
{
    //if (0 == mcd_check_pot())
        return ZS_SUCCESS;
    //else
    //    return ZS_FAILURE;
}

ZS_status_t 
ZSCheckMeta()
{
    if (0 == mcd_check_meta())
        return ZS_SUCCESS;
    else
        return ZS_FAILURE;
}

ZS_status_t 
ZSCheckInit(char *logfile)
{
    if (0 == zscheck_init_log(logfile))
        return ZS_SUCCESS;
    else
        return ZS_FAILURE;
}

ZS_status_t 
ZSCheckClose()
{
    if (0 == zscheck_close_log())
        return ZS_SUCCESS;
    else
        return ZS_FAILURE;
}

void
ZSCheckSetLevel(int level)
{
    mcd_check_set_level(level);
}

int 
ZSCheckGetLevel()
{
    return mcd_check_get_level();
}

void
ZSCheckMsg(ZS_check_entity_t entity,
           uint64_t id,
           ZS_check_error_t error,
           char *msg
           )
{
    zscheck_log_msg(entity,
                    id,
                    error,
                    msg
                   );
}

ZS_status_t
ZSCheck(struct ZS_thread_state *zs_thread_state, uint64_t flags)
{

	fprintf(stderr, "ZS: ZSCheck without btree is not supported\n");
	return ZS_FAILURE;
}

ZS_status_t
ZSIoctl(struct ZS_thread_state *zs_thread_state, 
         ZS_cguid_t cguid,
         uint32_t ioctl_type,
         void *data)
{
	switch (ioctl_type) {
#ifdef FLIP_ENABLED
	case ZS_IOCTL_FLIP:
		flip_handle_ioctl(data);
		break;
#endif
	default:
		break;
	}
	return ZS_SUCCESS;
}

/*
 * @brief Create a snapshot for a container  
 * 
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param snap_seq <OUT> sequence number of snapshot
 * @return ZS_SUCCESS if successful
 *         ZS_TOO_MANY_SNAPSHOTS if snapshot limit is reached
 */
ZS_status_t
ZSCreateContainerSnapshot(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t		cguid,
	uint64_t		*snap_seq
	)
{

	/* supported in btree only */
	return (ZS_FAILURE);
}

/*
 * @brief Delete a snapshot
 * 
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param snap_seq <IN> snapshot to be deleted
 * @return ZS_SUCCESS if successful
 *         ZS_SNAPSHOT_NOT_FOUND if no snapshot for snap_seq is found
 */
ZS_status_t
ZSDeleteContainerSnapshot(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t		cguid,
	uint64_t		snap_seq
	)
{

	/* supported in btree only */
	return (ZS_FAILURE);
}

/*
 * @brief Get a list of all current snapshots
 *
 * Array returned in snap_seqs is allocated by ZS and must be freed by
 * application.
 * 
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param n_snapshots <OUT> number of snapshots retrieved
 * @param snap_seqs <OUT> retrieved snapshots
 * @return ZS_SUCCESS if successful
 *         ZS_xxxzzz if snap_seqs cannot be allocated
 */
ZS_status_t
ZSGetContainerSnapshots(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t		cguid,
	uint32_t		*n_snapshots,
	ZS_container_snapshots_t **snap_seqs
	)
{
	/* supported in btree only */
	return (ZS_FAILURE);
}

ZS_status_t (ZSScavenger) (struct ZS_state *zs_state) 
{
	fprintf(stderr, "ZS: ZSScavenger without btree is not supported\n");
	return ZS_UNSUPPORTED_REQUEST;	
}

ZS_status_t (ZSScavengeContainer) (struct ZS_state *zs_state, ZS_cguid_t cguid) 
{
	fprintf(stderr, "ZS: ZSScavengeContainer without btree is not supported\n");
	return ZS_UNSUPPORTED_REQUEST;
}

ZS_status_t (ZSScavengeSnapshot) (struct ZS_state *zs_state, ZS_cguid_t cguid, uint64_t snap_seq) 
{
	fprintf(stderr, "ZS: ZSScavengeSnapshot without btree is not supported\n");
	return ZS_UNSUPPORTED_REQUEST;
}

ZS_status_t
ZSOperationAllowed( void )
{
	return is_zs_operation_allowed();
}
#if 0
#define NUM_BTRACE_ENTRIES 50
void zs_print_backtrace() {
    void *array[NUM_BTRACE_ENTRIES];
    char **strings;
    int size,i;
    /*Get the trace */
    size = backtrace (array, NUM_BTRACE_ENTRIES);
    /*Print in stderr*/
    strings = backtrace_symbols(array, size);
    for (i = 0; i < size; i++) {
        fprintf (stderr,"%s\n", strings[i]);
    }
    plat_free(strings);
    return;
}
#else
char pstack_out_file[100] = "stack_at_crach.info" ;

void zs_print_backtrace()
{

	pid_t pid = 0;
	char cmd[300];
	char *filename = NULL;

	filename = getenv("PSTACK_OUT_FILE");
	if (filename == NULL) {
		filename = pstack_out_file;
	}

	pid = getpid();

	snprintf(cmd, 200, "pstack %d | tee %s", pid, filename);
	system(cmd);

	return;
}

#endif 


void zs_signal_handler(int signum) { 
    plat_log_msg(80064, LOG_CAT, LOG_ERR,
                "Got signal %d(%s):",signum,strsignal(signum)); 
    zs_print_backtrace(); 

    signal(SIGABRT, SIG_DFL);

    if (zs_dump_core) {
	plat_abort();
    } else {
	plat_exit(1); 
    }
} 

/*
 * Function to check the allocated slabs.
 */
ZS_status_t
zs_check_slab_space(struct ZS_thread_state *zs_thread_state)
{

	ZS_status_t status = ZS_SUCCESS;
	SDF_cache_ctnr_metadata_t *meta = NULL;
	SDF_action_init_t *pac = NULL;
	bool res = false;
    char err_msg[1024];

	pac = (SDF_action_init_t *) zs_thread_state;

	meta = get_container_metadata(pac, VDC_CGUID);
	if (meta == NULL) {
		status = ZS_FAILURE_CONTAINER_NOT_FOUND;
        ZSCheckMsg(ZSCHECK_CONTAINER_META, VDC_CGUID, ZSCHECK_CONTAINER_META_ERROR, "Failed to find VDC");
		goto out;
	}
	
	res = mcd_osd_cmp_space_maps(meta->pshard);
	if (res == false) {
		status = ZS_FAILURE;
        sprintf(err_msg, "Space map comparison failed for shard %lu", meta->pshard->shardID);
        ZSCheckMsg(ZSCHECK_SHARD_SPACE_MAP, meta->pshard->shardID, ZSCHECK_SHARD_SPACE_MAP_ERROR, err_msg);
	} else {
        sprintf(err_msg, "Space map comparison succeeded for shard %lu", meta->pshard->shardID);
        ZSCheckMsg(ZSCHECK_SHARD_SPACE_MAP, meta->pshard->shardID, ZSCHECK_SUCCESS, err_msg);
    }

out:
	return status;
}

ZS_status_t
ZSCheckSpace(struct ZS_thread_state *zs_thread_state)
{
	return zs_check_slab_space(zs_thread_state);
}

ZS_status_t ZSDeleteRawObject(
	struct ZS_thread_state		*zs_thread_state,
	ZS_cguid_t					cguid,
	baddr_t						key,
	uint32_t					keylen,
	uint32_t					flags
	)
{
    ZS_status_t        status	= ZS_FAILURE;
	bool thd_ctx_locked = false;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(80022, LOG_CAT,
               LOG_WARN, "Shutdown in Progress. Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
        if ( !zs_thread_state || !cguid || !keylen ) {
            if ( !zs_thread_state ) {
                plat_log_msg(80049,LOG_CAT,LOG_DBG,
                             "ZS Thread state is NULL");
            }
            if ( !cguid ) {
                plat_log_msg(80050,LOG_CAT,LOG_DBG,
                             "Invalid container cguid:%lu",cguid);
            }
            if ( !keylen ) {
                plat_log_msg(80056,LOG_CAT,LOG_DBG,
                             "Invalid key length");
            }
            return ZS_INVALID_PARAMETER;
        }

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_delete_object(zs_thread_state, cguid, (char *)&key, keylen, true);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}

ZS_status_t ZSRenameContainer( 
    struct ZS_thread_state *zs_thread_state, 
    ZS_cguid_t              cguid, 
    char                    *name
    )
{
    ZS_status_t status = ZS_SUCCESS; 
    SDF_container_meta_t meta; 
    SDF_internal_ctxt_t *pai; 
    cntr_map_t *cmap = NULL; 
    SDF_CONTAINER_PARENT parent = containerParentNull; 
    local_SDF_CONTAINER_PARENT lparent = NULL; 
    SDF_CONTAINER container = containerNull;
    int reopen = 0;

    /* 
     * Check if operation can begin 
     */ 
    if (ZS_SUCCESS != (status = is_zs_operation_allowed())) { 
        plat_log_msg(160187, LOG_CAT, LOG_WARN, "Operation not allowed"); 
        return status; 
    }

    if ( !zs_thread_state ) {
        plat_log_msg(80049,LOG_CAT,LOG_DBG, "ZS Thread state is NULL");
        return ZS_INVALID_PARAMETER;
    }

    if (is_license_valid(is_btree_loaded()) == false) { 
        plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed."); 
        return ZS_LICENSE_CHK_FAILED; 
    }

    status = zs_validate_container(cguid); 
    if (ZS_SUCCESS != status) { 
        plat_log_msg(160292, LOG_CAT, LOG_ERR, 
                     "Failed due to an illegal container ID:%s %d", 
                     ZS_Status_Strings[status], (int)cguid); 
        return status; 
    }

    if (false == zs_lock_thd_ctxt(zs_thread_state)) { 
        /* 
         * Could not get thread context lock, error out.  
         */ 
        plat_log_msg(160161, LOG_CAT, LOG_DBG, "Could not get thread context lock"); 
        return ZS_THREAD_CONTEXT_BUSY; 
    }

    pai = (SDF_internal_ctxt_t *) zs_thread_state;

    SDFStartSerializeContainerOp(pai); 
    cmap = zs_cmap_get_by_cguid( cguid ); 
    if ( !cmap ) { 
        plat_log_msg(160210, LOG_CAT, LOG_WARN, 
                     "Container cguid=%lu does not exist. Delete can not proceed\n", cguid ); 
        status = ZS_FAILURE; 
        goto out; 
    }

    status = name_service_get_meta( pai, cguid, &meta ); 
    if ( status != ZS_SUCCESS ) { 
        plat_log_msg( 160078, LOG_CAT, LOG_ERR, 
                      "Could not read metadata for %lu. Delete can not proceed\n", cguid );
        status = ZS_FAILURE; 
        goto out; 
    } 

    if (meta.delete_in_progress == ZS_TRUE) {
        plat_log_msg(150130, LOG_CAT, LOG_WARN, 
                     "Container cguid=%lu cannot be renamed as it is being deleted.\n", cguid ); 
        status = ZS_FAILURE; 
        goto out;
    }

    // Allow any outstanding IO to drain 
    zs_cntr_drain_io( cmap->io_count );

    container = cmap->sdf_container;
    local_SDF_CONTAINER lcontainer = getLocalContainer(&lcontainer, container);
    if (lcontainer) { 
        parent = lcontainer->parent; 
        lparent = getLocalContainerParent(&lparent, parent); 
        char path[MAX_OBJECT_ID_SIZE] = "";

        if (lparent && lparent->name) { 
            memcpy(&path, lparent->name, strlen(lparent->name)); 
            // copy these from lparent before I nuke it!  
            releaseLocalContainerParent(&lparent); 
        }

        if (closeParentContainer(container)) { 
            status = ZS_SUCCESS; 
        }
      
        reopen = 1;
    }

    /*
     * Delete entry from hashmap. We cannot update the entry for cname since
     * cname acts as the key and now we have a new key (i.e. the new cname).
     */
    if (HashMap_get(cmap_cname_hash, name) == ZS_NULL_CGUID) { 
        HashMap_remove( cmap_cname_hash, meta.cname ); 
    } else { 
        status = ZS_FAILURE; 
        goto out; 
    }

    snprintf(meta.cname,CONTAINER_NAME_MAXLEN,"%s",name); 
    snprintf(cmap->cname,CONTAINER_NAME_MAXLEN,"%s",name);

    status = zs_cmap_update(cmap);

    if (ZS_SUCCESS != status) { 
        plat_log_msg( 150115, LOG_CAT, LOG_ERR, 
                      "Unable to create metadata cache for %lu. Cannot rename", cguid ); 
        goto out; 
    }

#ifdef FLIP_ENABLED
    if (flip_get("rename_pre_meta_error")) {
        plat_log_msg( 150131, LOG_CAT, LOG_ERR,
                      "flip rename_pre_meta_error\n");
        plat_abort();
    }
#endif

    status = name_service_put_meta( pai, cguid, &meta ); 
    if ( status != ZS_SUCCESS ) { 
        plat_log_msg( 160115, LOG_CAT, LOG_ERR, 
                      "Unable to write metadata for %lu. Can not rename ", 
                      cguid );
        goto out; 
    }

#ifdef FLIP_ENABLED
    if (flip_get("rename_post_meta_error")) {
        plat_log_msg( 150132, LOG_CAT, LOG_ERR,
                      "flip rename_post_meta_error\n");
        plat_abort();
    }
#endif

    // At this point the rename is crash safe
    if (reopen) {
        if ((status = name_service_lock_meta( pai, meta.cname )) != ZS_SUCCESS ) { 
            plat_log_msg(21532, LOG_CAT, LOG_ERR, "failed to lock %s", meta.cname); 
        } else if ( !isContainerParentNull(parent = createParentContainer( pai, meta.cname, &meta )) ) { 
            lparent = getLocalContainerParent( &lparent, parent ); // TODO C++ please!
            releaseLocalContainerParent(&lparent); // TODO C++ please!

            status = ZS_SUCCESS;
            cmap->sdf_container = openParentContainer(pai, meta.cname);

            if ((status = name_service_unlock_meta( pai, meta.cname )) != ZS_SUCCESS ) {
                plat_log_msg( 21533, LOG_CAT, LOG_ERR, "failed to unlock %s", meta.cname ); 
            }
        }
    }

out:
    SDFEndSerializeContainerOp(pai); 
    zs_unlock_thd_ctxt(zs_thread_state); 
    return(status);
}

ZS_status_t
ZSGetBtDelContainers(struct ZS_thread_state *zs_thread_state, 
						ZS_cguid_t *cguids, uint32_t *ncguids)
{
	ZS_status_t 		status		   = ZS_SUCCESS;
	bool 				thd_ctx_locked = false;
    SDF_internal_ctxt_t *pai 		   = (SDF_internal_ctxt_t *) zs_thread_state;
    int                   n_containers = 0;
	char                 *key          = NULL;
	uint32_t              keylen       = 0;
	cntr_map_t           *cmap         = NULL;
	uint64_t              cmaplen      = 0;
	struct cmap_iterator *iterator     = NULL;

	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
		plat_log_msg(160187, LOG_CAT,
			LOG_WARN, "Operation not allowed");
		goto out;
	}

	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}

    if ( !zs_thread_state ) {
        plat_log_msg(80049,LOG_CAT,LOG_DBG, "ZS Thread state is NULL");
        status = ZS_INVALID_PARAMETER;
        goto out;
    }
    if (!cguids || !ncguids) {
		status = ZS_INVALID_PARAMETER;
		goto out;
	}
	    

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);

	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}


    SDFStartSerializeContainerOp(pai);  


	iterator = zs_cmap_enum();

	if ( !iterator ) {
		SDFEndSerializeContainerOp( pai );   
	    status = ZS_FAILURE;
		goto out;
	}

	while ( zs_cmap_next_enum( iterator, &key, &keylen, (char **) &cmap, &cmaplen ) ) {
		if ( cmap->cguid > LAST_PHYSICAL_CGUID  && 
			 strcmp( cmap->cname,SEQNO_CONTAINER_NAME ) &&
			 strcmp( cmap->cname,PSTATS_CONTAINER_NAME ) &&
             !strncmp( cmap->cname,BTREE_DELETE_CONTAINER_NAME, strlen(BTREE_DELETE_CONTAINER_NAME) ) &&
			 ( cmap->state == ZS_CONTAINER_STATE_CLOSED ||
			   cmap->state == ZS_CONTAINER_STATE_OPEN )  ) {
			cguids[n_containers] = cmap->cguid;
            n_containers++;
        }
    }

	zs_cmap_finish_enum( iterator );
    *ncguids = n_containers;
	SDFEndSerializeContainerOp( pai );   

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}


static ZS_status_t
zs_create_raw_object(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t				cguid,
	baddr_t					*key,
	uint64_t				datalen,
	uint32_t				xflags
	)
{
	SDF_action_init_t  *pac		= NULL;
	ZS_status_t        status	= ZS_FAILURE;
	cntr_map_t *cmap = NULL;

	if ( !cguid || !key )
		return ZS_INVALID_PARAMETER;
 
	cmap = get_cntr_map(cguid);
	if (!cmap) {
		return(ZS_CONTAINER_UNKNOWN);
	}

	if ( (status = zs_get_ctnr_status_cmap(cmap, cguid, 0)) != ZS_CONTAINER_OPEN ) {
		plat_log_msg( 160040, LOG_CAT, LOG_DIAG, "Container must be open to execute a write object" );
		goto out;     
	}

	pac = (SDF_action_init_t *) zs_thread_state;

	SDF_cache_ctnr_metadata_t *meta;
	meta = get_container_metadata(pac, cguid);
	if (meta == NULL) {
		goto out;
	}

	int flags = 0;
	plat_log_msg(160192, LOG_CAT,
			LOG_TRACE, "ZSWriteObject flash_only.");
	struct objMetaData metaData;
	metaData.keyLen = sizeof(uint64_t);
	metaData.cguid  = cguid;
	metaData.dataLen = datalen;
	if (meta->meta.properties.compression) {
		flags |= FLASH_PUT_COMPRESS;
	}

	if (meta->meta.properties.durability_level == SDF_RELAXED_DURABILITY) {
		flags |= FLASH_PUT_DURA_SW_CRASH;
	} else if (meta->meta.properties.durability_level == SDF_FULL_DURABILITY) {
		flags |= FLASH_PUT_DURA_HW_CRASH;
	}

	flags |= FLASH_CREATE_RAW_OBJECT;

	update_container_stats(pac, APSOE, meta, 1);

	status = ssd_flashPut(pac->paio_ctxt, meta->pshard, &metaData, (char *)key, NULL, FLASH_PUT_NO_TEST|flags);
	if (status == FLASH_EOK) {
		status = ZS_SUCCESS;
	} else {
		status = ZS_FAILURE;
	}

out:
	rel_cntr_map(cmap);

	return status;
}

ZS_status_t ZSCreateRawObject(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t				cguid,
	baddr_t					*key,
	uint64_t				datalen,
	uint32_t				flags
	)
{
	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(80022, LOG_CAT,
               LOG_WARN, "Shutdown in Progress. Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
	if ( !zs_thread_state || !cguid || !key  ) {
		if ( !zs_thread_state ) {
			plat_log_msg(80049,LOG_CAT,LOG_DBG,
						 "ZS Thread state is NULL");
		}
		if ( !cguid ) {
			plat_log_msg(80050,LOG_CAT,LOG_DBG,
						 "Invalid container cguid:%lu",cguid);
		}
		if ( !key ) {
			plat_log_msg(160278,LOG_CAT,LOG_DBG,
						 "Invalid key argument");
		}

		return ZS_INVALID_PARAMETER;
	}

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_create_raw_object(zs_thread_state, cguid, key, datalen, flags);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}


ZS_status_t ZSReadRawObject(
	struct ZS_thread_state		*zs_thread_state,
	ZS_cguid_t					cguid,
	baddr_t						key,
	char						**data,
	uint64_t					*datalen,
	uint32_t					flags
	)
{
	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
		plat_log_msg(160188, LOG_CAT,
				LOG_WARN, "Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
	if ( !zs_thread_state || !cguid) {
		if ( !zs_thread_state ) {
			plat_log_msg(80049,LOG_CAT,LOG_DBG,
					"ZS Thread state is NULL");
		}
		if ( !cguid ) {
			plat_log_msg(80050,LOG_CAT,LOG_DBG,
					"Invalid container cguid:%lu",cguid);
		}
		return ZS_INVALID_PARAMETER;
	}

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
				LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_read_object(zs_thread_state, cguid, (char *)&key, sizeof(key), data, datalen, flags, true);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status; 
}

ZS_status_t ZSWriteRawObject(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t          cguid,
	char                *key,
	uint32_t             keylen,
	char                *data,
	uint64_t             datalen,
	uint32_t             flags
	)
{
	ZS_status_t status = ZS_SUCCESS;
	bool thd_ctx_locked = false;

	status = zs_validate_container(cguid);
	if (ZS_SUCCESS != status) {
		plat_log_msg(160125, LOG_CAT,
				LOG_ERR, "Failed due to an illegal container ID:%s",
				ZS_Status_Strings[status]);
		goto out;
	}
	/*
	 * Check if operation can begin
	 */
	if (ZS_SUCCESS != (status = is_zs_operation_allowed())) {
        plat_log_msg(80022, LOG_CAT,
               LOG_WARN, "Shutdown in Progress. Operation not allowed ");
		goto out;
	}
	if (is_license_valid(is_btree_loaded()) == false) {
		plat_log_msg(160145, LOG_CAT, LOG_WARN, "License check failed.");
		status = ZS_LICENSE_CHK_FAILED;
		goto out;
	}
	if ( !zs_thread_state || !cguid || !keylen || !data || !datalen  ) {
		if ( !zs_thread_state ) {
			plat_log_msg(80049,LOG_CAT,LOG_DBG,
						 "ZS Thread state is NULL");
		}
		if ( !cguid ) {
			plat_log_msg(80050,LOG_CAT,LOG_DBG,
						 "Invalid container cguid:%lu",cguid);
		}
		if ( !keylen ) {
			plat_log_msg(80056,LOG_CAT,LOG_DBG,
						 "Invalid key length");
		}
		if ( !data ) {
			plat_log_msg(80058,LOG_CAT,LOG_DBG,
						 "Invalid data(NULL)");
		}
		if ( !datalen ) {
			plat_log_msg(80059,LOG_CAT,LOG_DBG,
						 "Invalid data length");
		}
		return ZS_INVALID_PARAMETER;
	}

	thd_ctx_locked = zs_lock_thd_ctxt(zs_thread_state);
	if (false == thd_ctx_locked) {
		/*
		 * Could not get thread context lock, error out.
		 */
		status = ZS_THREAD_CONTEXT_BUSY;
		plat_log_msg(160161, LOG_CAT,
		       	     LOG_DBG, "Could not get thread context lock");
		goto out;
	}

	status = zs_write_raw_object(zs_thread_state, cguid, key, keylen, data, datalen, flags);

out:
	if (thd_ctx_locked) {
		zs_unlock_thd_ctxt(zs_thread_state);
	}
	return status;
}


ZS_status_t
zs_read_object_lc( struct ZS_thread_state *t, ZS_cguid_t c, char *k, uint32_t kl, char **d, uint64_t *dl, bool app_buf)
{
	SDF_appreq_t    ar;
	SDF_action_init_t *pac;
	ZS_status_t     status = ZS_SUCCESS;
	ZS_status_t     read_ret = ZS_SUCCESS;
	char           *app_buf_data_ptr = NULL;
	cntr_map_t     *cmap = NULL;

	if (!c || !k)
		return ZS_INVALID_PARAMETER;
	if (d == NULL)
		return ZS_BAD_PBUF_POINTER;
	cmap = get_cntr_map(c);
	if (!cmap)
		return (ZS_CONTAINER_UNKNOWN);
	if ((status = zs_get_ctnr_status_cmap(cmap, c, 0)) != ZS_CONTAINER_OPEN) {
		plat_log_msg(160039, LOG_CAT, LOG_DIAG, "Container must be open to execute a read object");
		goto out;
	}
	pac = (SDF_action_init_t *) t;
	SDF_cache_ctnr_metadata_t *meta;
	meta = get_container_metadata(pac, c);
	if (meta == NULL)
		goto out;
	char           *tdata = (char *) 0x1;	// Make sure its not NULL
	int             flag = 0;
	struct objMetaData metaData;
	metaData.keyLen = kl;
	metaData.cguid = c;
	update_container_stats(pac, APGRD, meta, 1);
	read_ret = ssd_flashGet(pac->paio_ctxt, meta->pshard, &metaData, k, &tdata, flag | FLASH_GET_NO_TEST);
	/*
	 * If app buf, copied len should be min of dl and metaData.dataLen 
	 */
	if (app_buf)
		*dl = (*dl > metaData.dataLen) ? metaData.dataLen : *dl;
	else
		*dl = metaData.dataLen;
	if (read_ret == FLASH_EOK) {
		status = ZS_SUCCESS;
		if (!app_buf) {
			*d = malloc(*dl);
			if (*d == NULL) {
				status = ZS_OUT_OF_MEM;
				goto out;
			}
		} else {
			if (*d == NULL) {
				status = ZS_BAD_PBUF_POINTER;
				goto out;
			}
		}
		memcpy(*d, tdata, *dl);
		ssd_flashFreeBuf(tdata);
	}
	else
		status = get_status(read_ret);
out:
	rel_cntr_map(cmap);
	return status;
}


ZS_status_t
zs_write_object_lc( struct ZS_thread_state *t, ZS_cguid_t c, char *k, uint32_t kl, char *d, uint64_t dl)
{
	SDF_appreq_t        ar;
	SDF_action_init_t  *pac		= NULL;
	ZS_status_t        status	= ZS_FAILURE;
	ZS_status_t        write_ret    = ZS_FAILURE;
	cntr_map_t *cmap = NULL;

	if ( !c || !k )
		return ZS_INVALID_PARAMETER;
	cmap = get_cntr_map(c);
	if (!cmap)
		return(ZS_CONTAINER_UNKNOWN);
	if ( (status = zs_get_ctnr_status_cmap(cmap, c, 0)) != ZS_CONTAINER_OPEN ) {
		plat_log_msg( 160040, LOG_CAT, LOG_DIAG, "Container must be open to execute a write object" );
		goto out;     
	}
	pac = (SDF_action_init_t *) t;
	SDF_cache_ctnr_metadata_t *meta;
	meta = get_container_metadata(pac, c);
	if (meta == NULL)
		goto out;
	int flags = 0;
	struct objMetaData metaData;
	bzero(&metaData, sizeof(metaData));
	metaData.keyLen = kl;
	metaData.cguid  = c;
	metaData.dataLen = dl;
	if (meta->meta.properties.durability_level == SDF_RELAXED_DURABILITY)
		flags |= FLASH_PUT_DURA_SW_CRASH;
	else if (meta->meta.properties.durability_level == SDF_FULL_DURABILITY)
		flags |= FLASH_PUT_DURA_HW_CRASH;
	update_container_stats(pac, APSOB, meta, 1);
	write_ret = ssd_flashPut(pac->paio_ctxt, meta->pshard, &metaData, k, d, FLASH_PUT_NO_TEST|flags);
	status = get_status(write_ret);
out:
	rel_cntr_map(cmap);

	return status;
}


ZS_status_t
zs_delete_object_lc( struct ZS_thread_state *t, ZS_cguid_t c, char *k, uint32_t kl)
{
	SDF_appreq_t        ar;
	SDF_action_init_t  *pac		= NULL;
	ZS_status_t        status	= ZS_SUCCESS;
	cntr_map_t *cmap = NULL;

	if ( !c || !k )
		return ZS_INVALID_PARAMETER;
	cmap = get_cntr_map(c);
	if (!cmap)
		return(ZS_CONTAINER_UNKNOWN);
	if ( (status = zs_get_ctnr_status_cmap(cmap, c, 0)) != ZS_CONTAINER_OPEN ) {
		plat_log_msg( 160041, LOG_CAT, LOG_DIAG, "Container must be open to execute a delete object" );
		goto out;     
	}
	pac = (SDF_action_init_t *) t;
	SDF_cache_ctnr_metadata_t *meta;
	meta = get_container_metadata(pac, c);
	if (meta == NULL)
		goto out;
	int flags = 0;
	struct objMetaData metaData;
	bzero(&metaData, sizeof(metaData));
	metaData.keyLen = kl;
	metaData.cguid  = c;
	metaData.dataLen = 0;
	if (meta->meta.properties.durability_level == SDF_RELAXED_DURABILITY)
		flags |= FLASH_PUT_DURA_SW_CRASH;
	else if (meta->meta.properties.durability_level == SDF_FULL_DURABILITY)
		flags |= FLASH_PUT_DURA_HW_CRASH;
	status = ssd_flashPut(pac->paio_ctxt, meta->pshard, &metaData, k, NULL, FLASH_PUT_TEST_NONEXIST|flags);
	if (status == FLASH_EOK)
		status = ZS_SUCCESS;
	else
		status = ZS_FAILURE;
out:
	rel_cntr_map(cmap);
	return status;
}
