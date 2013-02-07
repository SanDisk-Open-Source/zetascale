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
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include "sdf.h"
#include "sdf_internal.h"
#include "fdf.h"
#include "utils/properties.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_common.h"
#include "protocol/action/action_thread.h"
#include "protocol/action/async_puts.h"
#include "protocol/home/home_flash.h"
#include "protocol/action/async_puts.h"
#include "protocol/replication/copy_replicator.h"
#include "protocol/replication/replicator.h"
#include "protocol/replication/replicator_adapter.h"
#include "ssd/fifo/mcd_ipf.h"
#include "ssd/fifo/mcd_osd.h"
#include "ssd/fifo/mcd_bak.h"
#include "shared/init_sdf.h"
#include "shared/private.h"
#include "shared/open_container_mgr.h"
#include "shared/container_meta.h"
#include "shared/name_service.h"
#include "shared/shard_compute.h"
#include "shared/internal_blk_obj_api.h"
#include "agent/agent_common.h"
#include "agent/agent_helper.h"
#include "fdf_trans.h"

#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_NAMING
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_WARN PLAT_LOG_LEVEL_WARN
#define LOG_FATAL PLAT_LOG_LEVEL_FATAL
#define BUF_LEN 4096
#define STATS_API_TEST 1

static time_t 			current_time 	= 0;
static SDF_shardid_t	vdc_shardid		= SDF_SHARDID_INVALID;

char *FDF_Status_Strings[] = {
    "UNKNOWN_STATUS", /* since FDF_SUCCESS is 1! */
#define item(caps, value) \
        #caps,
        FDF_STATUS_ITEMS()
#undef item
    };

/*
** Externals
*/
extern void						*cmc_settings;
extern int 			 			 Mcd_osd_max_nclasses;
extern SDF_cmc_t 				*theCMC;
extern uint64_t 	 			 Mcd_num_pending_ios;
extern struct SDF_shared_state 	 sdf_shared_state;

extern int loadProperties(
	const char *path_arg;
	);

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
	int 			 		 num_objs,  
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
static FDF_status_t fdf_create_container(
    struct FDF_thread_state *fdf_thread_state,
    char                    *cname,
    FDF_container_props_t   *properties,
    uint32_t                 flags,
    FDF_cguid_t             *cguid,
    FDF_container_mode_t     mode
	);

static FDF_status_t fdf_open_container(
    struct FDF_thread_state *fdf_thread_state,
    char                    *cname,
    FDF_container_props_t   *props,
    uint32_t                 flags,
    FDF_cguid_t             *cguid,
    FDF_container_mode_t     mode,
	FDF_boolean_t			 serialize
    );

static FDF_status_t fdf_close_container(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t  		     cguid,
	FDF_container_mode_t	 mode,
	FDF_boolean_t			 serialize
	);

static FDF_status_t fdf_delete_container(
    struct FDF_thread_state *fdf_thread_state,
    FDF_cguid_t              cguid,
	FDF_container_mode_t	 mode
    );

#if 0
static SDF_container_props_t *fdf_create_sdf_props(
    FDF_container_props_t 			*fdf_properties,
    FDF_internal_container_props_t  *fdf_internal_properties
    );
#else
static SDF_container_props_t *fdf_create_sdf_props(
    FDF_container_props_t 			*fdf_properties
	);
#endif

static FDF_status_t fdf_create_fdf_props(
    SDF_container_props_t   *sdf_properties,
    FDF_container_props_t   *fdf_properties
    );
 
static FDF_status_t fdf_resize_container(
    struct FDF_thread_state *fdf_thread_state,
    FDF_cguid_t              cguid,
	uint64_t				 size
    );

FDF_status_t FDFInitPerThreadState(
	struct FDF_state 		 *fdf_state,
    struct FDF_thread_state **thd_state
	);

static void fdf_start_vc_thread( 
	struct FDF_state *sdf_state 
	);

static FDF_status_t fdf_vc_init(
	struct FDF_thread_state  *fdf_thread_state,
    int                  	  flags
    );

static FDF_status_t fdf_generate_cguid(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t 			 *cguid
	);

static FDF_status_t fdf_delete_objects(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t 			  cguid
	);

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
}FDF_cache_counts_t;

static char * FDFCacheCountStrings[] = {
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


static char *FDFAccessTypeSearchStrings[] = {
    "APCOE",/*FDF_ACCESS_TYPES_APCOE*/
    "APCOP",/*FDF_ACCESS_TYPES_APCOP*/
    "APPAE",/*FDF_ACCESS_TYPES_APPAE*/
    "APPTA",/*FDF_ACCESS_TYPES_APPTA*/
    "APSOE",/*FDF_ACCESS_TYPES_APSOE*/
    "APSOB",/*FDF_ACCESS_TYPES_APSOB*/
    "APGRX",/*FDF_ACCESS_TYPES_APGRX*/
    "APGRD",/*FDF_ACCESS_TYPES_APGRD*/
    "APDBE",/*FDF_ACCESS_TYPES_APDBE*/
    "APFLS",/*FDF_ACCESS_TYPES_APFLS*/
    "APFLI",/*FDF_ACCESS_TYPES_APFLI*/
    "APINV",/*FDF_ACCESS_TYPES_APINV*/
    "APSYC",/*FDF_ACCESS_TYPES_APSYC*/
    "APICD",/*FDF_ACCESS_TYPES_APICD*/
    "APGIT",/*FDF_ACCESS_TYPES_APGIT*/
    "APFCO",/*FDF_ACCESS_TYPES_APFCO*/
    "APFCI",/*FDF_ACCESS_TYPES_APFCI*/
    "APICO",/*FDF_ACCESS_TYPES_APICO*/
    "APRIV",/*FDF_ACCESS_TYPES_APRIV*/
    "APRIV",/*FDF_ACCESS_TYPES_APRUP*/
};

static char *FDFFlashStatsSearchStrings[] = {
    "NUM_OBJS",/*FDF_FLASH_STATS_NUM_OBJS*/
    "NUM_CREATED_OBJS",/*FDF_FLASH_STATS_NUM_CREATED_OBJS*/
    "NUM_EVICTIONS",/*FDF_FLASH_STATS_NUM_EVICTIONS*/
    "HASH_EVICTIONS",/*FDF_FLASH_STATS_NUM_HASH_EVICTIONS*/
    "INVAL_EVICTIONS",/*FDF_FLASH_STATS_NUM_INVAL_EVICTIONS*/
    "SOFT_OVERFLOWS",/*FDF_FLASH_STATS_NUM_SOFT_OVERFLOWS*/
    "NUM_HARD_OVERFLOWS",/*FDF_FLASH_STATS_NUM_HARD_OVERFLOWS*/
    "GET_HASH_COLLISION",/*FDF_FLASH_STATS_GET_HASH_COLLISION*/
    "SET_HASH_COLLISION",/* FDF_FLASH_STATS_SET_HASH_COLLISION*/
    "NUM_OVERWRITES",/*FDF_FLASH_STATS_NUM_OVERWRITES*/
    "NUM_OPS",/*FDF_FLASH_STATS_NUM_OPS*/
    "READ_OPS",/*FDF_FLASH_STATS_NUM_READ_OPS*/
    "GET_OPS",/*FDF_FLASH_STATS_NUM_GET_OPS*/
    "PUT_OPS",/*FDF_FLASH_STATS_NUM_PUT_OPS*/
    "DEL_OPS",/*FDF_FLASH_STATS_NUM_DEL_OPS*/
    "FULL_BUCKETS",/*FDF_FLASH_STATS_GET_EXIST_CHECKS*/
    "FULL_BUCKETS",/*FDF_FLASH_STATS_NUM_FULL_BUCKETS*/
    "PENDING_IOS",/*FDF_FLASH_STATS_PENDING_IOS*/
    "SPACE_ALLOCATED",/*FDF_FLASH_STATS_SPACE_ALLOCATED*/
    "SPACE_CONSUMED",/*FDF_FLASH_STATS_SPACE_CONSUMED*/
};




static char *FDFCacheStatsSearchStrings[] = {
    "overwrites_s",/* FDF_CACHE_STAT_OVERWRITES_S */
    "overwrites_m",/* FDF_CACHE_STAT_OVERWRITES_M */
    "inplaceowr_s",/* FDF_CACHE_STAT_INPLACEOWR_S */
    "inplaceowr_m",/* FDF_CACHE_STAT_INPLACEOWR_M */
    "new_entries",/* FDF_CACHE_STAT_NEW_ENTRIES */
    "writethrus",/* FDF_CACHE_STAT_WRITETHRUS */
    "writebacks",/* FDF_CACHE_STAT_WRITEBACKS */
    "flushes",/* FDF_CACHE_STAT_FLUSHES */
    /* request from cache to flash manager */
    "AHCOB",/* FDF_CACHE_STAT_AHCOB */
    "AHCOP",/* FDF_CACHE_STAT_AHCOP */
    "AHCWD", /* FDF_CACHE_STAT_AHCWD */
    "AHDOB",/* FDF_CACHE_STAT_AHDOB */
    "AHFLD",/* FDF_CACHE_STAT_AHFLD */
    "AHGTR", /* FDF_CACHE_STAT_AHGTR */
    "AHGTW", /* FDF_CACHE_STAT_AHGTW */
    "AHPTA", /* FDF_CACHE_STAT_AHPTA */
    "AHSOB", /* FDF_CACHE_STAT_AHSOB */
    "AHSOP",/* FDF_CACHE_STAT_AHSOP */
    /* Request from flash manager to cache */
    "HACRC",/* FDF_CACHE_STAT_HACRC */
    "HACRF",/* FDF_CACHE_STAT_HACRF */
    "HACSC",/* FDF_CACHE_STAT_HACSC */
    "HACSF",/* FDF_CACHE_STAT_HACSF */
    "HADEC",/* FDF_CACHE_STAT_HADEC */
    "HADEF",/* FDF_CACHE_STAT_HADEF */
    "HAFLC",/* FDF_CACHE_STAT_HAFLC */
    "HAFLF",/* FDF_CACHE_STAT_HAFLF */
    "HAGRC",/* FDF_CACHE_STAT_HAGRC */
    "HAGRF",/* FDF_CACHE_STAT_HAGRF */
    "HAGWC",/* FDF_CACHE_STAT_HAGWC */
    "HAGWF",/* FDF_CACHE_STAT_HAGWF */
    "HAPAC",/* FDF_CACHE_STAT_HAPAC */
    "HAPAF",/* FDF_CACHE_STAT_HAPAF */
    "HASTC",/* FDF_CACHE_STAT_HASTC */
    "HASTF",/* FDF_CACHE_STAT_HASTF */
    "HFXST",/* FDF_CACHE_STAT_HFXST */
    "FHXST",/* FDF_CACHE_STAT_FHXST */
    "FHNXS",/* FDF_CACHE_STAT_FHNXS */
    "HFGFF",/* FDF_CACHE_STAT_HFGFF */
    "FHDAT",/* FDF_CACHE_STAT_FHDAT */
    "FHGTF",/* FDF_CACHE_STAT_FHGTF */
    "HFPTF",/* FDF_CACHE_STAT_HFPTF */
    "FHPTC",/* FDF_CACHE_STAT_FHPTC */
    "FHPTF",/* FDF_CACHE_STAT_FHPTF */
    "HFDFF",/* FDF_CACHE_STAT_HFDFF */
    "FHDEC",/* FDF_CACHE_STAT_FHDEC */
    "FHDEF",/* FDF_CACHE_STAT_FHDEF */
    "HFCIF",/* FDF_CACHE_STAT_HFCIF */
    "FHCRC",/* FDF_CACHE_STAT_FHCRC */
    "FHCRF",/* FDF_CACHE_STAT_FHCRF */
    "HFCZF",/* FDF_CACHE_STAT_HFCZF */
    "HFSET",/* FDF_CACHE_STAT_HFSET */
    "HFSTC",/* FDF_CACHE_STAT_HFSTC */
    "FHSTF",/* FDF_CACHE_STAT_FHSTF */
    "HFCSH",/* FDF_CACHE_STAT_HFCSH */
    "FHCSC",/* FDF_CACHE_STAT_FHCSC */
    "FHCSF",/* FDF_CACHE_STAT_FHCSF */
    "FHCSF",/* FDF_CACHE_STAT_HFSSH */
    "FHSSC",/* FDF_CACHE_STAT_FHSSC */
    "FHSSF",/* FDF_CACHE_STAT_FHSSF */
    "HFDSH",/* FDF_CACHE_STAT_HFDSH */
    "FHDSC",/* FDF_CACHE_STAT_FHDSC */
    "FHDSF",/* FDF_CACHE_STAT_FHDSF */
    "HFGLS",/* FDF_CACHE_STAT_HFGLS */
    "FHGLC",/* FDF_CACHE_STAT_FHGLC */
    "FHGLF",/* FDF_CACHE_STAT_FHGLF */
    "HFGIC",/* FDF_CACHE_STAT_HFGIC */
    "FHGIC",/* FDF_CACHE_STAT_FHGIC */
    "FHGIF",/* FDF_CACHE_STAT_FHGIF */
    "HFGBC",/* FDF_CACHE_STAT_HFGBC */
    "FHGCC",/* FDF_CACHE_STAT_FHGCC */
    "FHGCF",/* FDF_CACHE_STAT_FHGCF */
    "HFGSN",/* FDF_CACHE_STAT_HFGSN */
    "HFGCS",/* FDF_CACHE_STAT_HFGCS */
    "FHGSC",/* FDF_CACHE_STAT_FHGSC */
    "FHGSF",/* FDF_CACHE_STAT_FHGSF */
    "HFSRR",/* FDF_CACHE_STAT_HFSRR */
    "FHSRC",/* FDF_CACHE_STAT_FHSRC */
    "FHSRF",/* FDF_CACHE_STAT_FHSRF */
    "HFSPR",/* FDF_CACHE_STAT_HFSPR */
    "FHSPC",/* FDF_CACHE_STAT_FHSPC */
    "FHSPF",/* FDF_CACHE_STAT_FHSPF */
    "HFFLA",/* FDF_CACHE_STAT_HFFLA */
    "FHFLC",/* FDF_CACHE_STAT_FHFLC */
    "FHFLF",/* FDF_CACHE_STAT_FHFLF */
    "HFRVG",/* FDF_CACHE_STAT_HFRVG */
    "FHRVC",/* FDF_CACHE_STAT_FHRVC */
    "FHRVF",/* FDF_CACHE_STAT_FHRVF */
    "HFNOP",/* FDF_CACHE_STAT_HFNOP */
    "FHNPC",/* FDF_CACHE_STAT_FHNPC */
    "FHNPF",/* FDF_CACHE_STAT_FHNPF */
    "HFOSH", /* FDF_CACHE_STAT_HFOSH */
    "FHOSC",/* FDF_CACHE_STAT_FHOSC */
    "FHOSF",/* FDF_CACHE_STAT_FHOSF */
    "HFFLS",/* FDF_CACHE_STAT_HFFLS */
    "FHFCC",/* FDF_CACHE_STAT_FHFCC */
    "FHFCF",/* FDF_CACHE_STAT_FHFCF */
    "HFFIV",/* FDF_CACHE_STAT_HFFIV */
    "FHFIC",/* FDF_CACHE_STAT_FHFIC */
    "FHFIF",/* FDF_CACHE_STAT_FHFIF */
    "HFINV",/* FDF_CACHE_STAT_HFINV */
    "FHINC",/* FDF_CACHE_STAT_FHINC */
    "FHINF",/* FDF_CACHE_STAT_FHINF */
    "HFFLC",/* FDF_CACHE_STAT_HFFLC */
    "FHLCC",/* FDF_CACHE_STAT_FHLCC */
    "FHLCF",/* FDF_CACHE_STAT_FHLCF */
    "HFFLI",/* FDF_CACHE_STAT_HFFLI */
    "FHLIC",/* FDF_CACHE_STAT_FHLIC */
    "FHLIF",/* FDF_CACHE_STAT_FHLIF */
    "HFINC",/* FDF_CACHE_STAT_HFINC */
    "HFINC",/* FDF_CACHE_STAT_FHCIC */
    "FHCIF",/* FDF_CACHE_STAT_FHCIF */
    "EOK",/* FDF_CACHE_STAT_EOK */
    "EPERM",/* FDF_CACHE_STAT_EPERM */
    "ENOENT",/* FDF_CACHE_STAT_ENOENT */
    "EDATASIZE",/* FDF_CACHE_STAT_EDATASIZE */
    "ESTOPPED",/* FDF_CACHE_STAT_ESTOPPED */
    "EBADCTNR",/* FDF_CACHE_STAT_EBADCTNR */
    "EDELFAIL",/* FDF_CACHE_STAT_EDELFAIL */
    "EAGAIN",/* FDF_CACHE_STAT_EAGAIN */
    "ENOMEM",/* FDF_CACHE_STAT_ENOMEM */
    "EACCES",/* FDF_CACHE_STAT_EACCES */
    "EINCONS",/* FDF_CACHE_STAT_EINCONS */
    "EBUSY",/* FDF_CACHE_STAT_EBUSY */
    "EEXIST",/* FDF_CACHE_STAT_EEXIST */
    "EINVAL",/* FDF_CACHE_STAT_EINVAL */
    "EMFILE",/* FDF_CACHE_STAT_EMFILE */
    "ENOSPC",/* FDF_CACHE_STAT_ENOSPC */
    "ENOBUFS",/* FDF_CACHE_STAT_ENOBUFS */
    "ESTALE",/* FDF_CACHE_STAT_ESTALE */
    "EDQUOT",/* FDF_CACHE_STAT_EDQUOT */
    "EDELFAIL",/* FDF_CACHE_STAT_RMT_EDELFAIL */
    "EBADCTNR",/* FDF_CACHE_STAT_RMT_EBADCTNR */
    "hashBuckets",/* FDF_CACHE_STAT_HASH_BUCKETS */
    "nSlabs",/* FDF_CACHE_STAT_NUM_SLABS */
    "numElements",/* FDF_CACHE_STAT_NUM_ELEMENTS */
    "maxSz",/* FDF_CACHE_STAT_MAX_SIZE */
    "currSz",/* FDF_CACHE_STAT_CURR_SIZE */
    "currSzWkeys",/* FDF_CACHE_STAT_CURR_SIZE_WKEYS */
    "nMod",/* FDF_CACHE_STAT_NUM_MODIFIED_OBJS */
    "modSzWkeys",/* FDF_CACHE_STAT_NUM_MODIFIED_OBJS_WKEYS */
    "nModFlushes",/* FDF_CACHE_STAT_NUM_MODIFIED_OBJS_FLUSHED */
    "nModBGFlushes",/* FDF_CACHE_STAT_NUM_MODIFIED_OBJS_BGFLUSHED */
    "nPending",/* FDF_CACHE_STAT_NUM_PENDING_REQS */
    "nModRecEnums",/* FDF_CACHE_STAT_NUM_MODIFIED_OBJC_REC */
    "bkFlshProg",/* FDF_CACHE_STAT_BGFLUSH_PROGRESS */
    "nBkFlsh",/* FDF_CACHE_STAT_NUM_BGFLUSH */
    "nFlshTok",/* FDF_CACHE_STAT_NUM_FLUSH_PARALLEL */
    "nBkFlshTok",/* FDF_CACHE_STAT_NUM_BGFLUSH_PARALLEL */
    "FlsMs",/* FDF_CACHE_STAT_BGFLUSH_WAIT */
    "modPct",/* FDF_CACHE_STAT_MODIFIED_PCT */
    "nAppBufs",/* FDF_CACHE_STAT_NUM_APP_BUFFERS */
    "nTrans",/* FDF_CACHE_STAT_NUM_CACHE_OPS_PROG */
    "nFGBufs",/* FDF_CACHE_STAT_NUM_FGBUFFER_PROCESSED */
    "nResp",/* FDF_CACHE_STAT_NUM_RESP_PROCESSED  */
};

static char *FDFCacheStatsDescStrings[] = {
    "",/* FDF_CACHE_STAT_OVERWRITES_S */
    "",/* FDF_CACHE_STAT_OVERWRITES_M */
    "",/* FDF_CACHE_STAT_INPLACEOWR_S */
    "placeowr_m",/* FDF_CACHE_STAT_INPLACEOWR_M */
    "new_entries",/* FDF_CACHE_STAT_NEW_ENTRIES */
    "writethrus",/* FDF_CACHE_STAT_WRITETHRUS */
    "writebacks",/* FDF_CACHE_STAT_WRITEBACKS */
    "flushes",/* FDF_CACHE_STAT_FLUSHES */
    /* request from cache to flash manager */
    "AHCOB",/* FDF_CACHE_STAT_AHCOB */
    "AHCOP",/* FDF_CACHE_STAT_AHCOP */
    "AHCWD", /* FDF_CACHE_STAT_AHCWD */
    "AHDOB",/* FDF_CACHE_STAT_AHDOB */
    "AHFLD",/* FDF_CACHE_STAT_AHFLD */
    "AHGTR", /* FDF_CACHE_STAT_AHGTR */
    "AHGTW", /* FDF_CACHE_STAT_AHGTW */
    "AHPTA", /* FDF_CACHE_STAT_AHPTA */
    "AHSOB", /* FDF_CACHE_STAT_AHSOB */
    "AHSOP",/* FDF_CACHE_STAT_AHSOP */
    /* Request from flash manager to cache */
    "HACRC",/* FDF_CACHE_STAT_HACRC */
    "HACRF",/* FDF_CACHE_STAT_HACRF */
    "HACSC",/* FDF_CACHE_STAT_HACSC */
    "HACSF",/* FDF_CACHE_STAT_HACSF */
    "HADEC",/* FDF_CACHE_STAT_HADEC */
    "HADEF",/* FDF_CACHE_STAT_HADEF */
    "",/* FDF_CACHE_STAT_HAFLC */
    "",/* FDF_CACHE_STAT_HAFLF */
    "",/* FDF_CACHE_STAT_HAGRC */
    "",/* FDF_CACHE_STAT_HAGRF */
    "",/* FDF_CACHE_STAT_HAGWC */
    "",/* FDF_CACHE_STAT_HAGWF */
    "",/* FDF_CACHE_STAT_HAPAC */
    "",/* FDF_CACHE_STAT_HAPAF */
    "",/* FDF_CACHE_STAT_HASTC */
    "",/* FDF_CACHE_STAT_HASTF */
    "",/* FDF_CACHE_STAT_HFXST */
    "",/* FDF_CACHE_STAT_FHXST */
    "",/* FDF_CACHE_STAT_FHNXS */
    "",/* FDF_CACHE_STAT_HFGFF */
    "",/* FDF_CACHE_STAT_FHDAT */
    "",/* FDF_CACHE_STAT_FHGTF */
    "",/* FDF_CACHE_STAT_HFPTF */
    "",/* FDF_CACHE_STAT_FHPTC */
    "",/* FDF_CACHE_STAT_FHPTF */
    "",/* FDF_CACHE_STAT_HFDFF */
    "",/* FDF_CACHE_STAT_FHDEC */
    "",/* FDF_CACHE_STAT_FHDEF */
    "",/* FDF_CACHE_STAT_HFCIF */
    "",/* FDF_CACHE_STAT_FHCRC */
    "",/* FDF_CACHE_STAT_FHCRF */
    "",/* FDF_CACHE_STAT_HFCZF */
    "",/* FDF_CACHE_STAT_HFSET */
    "",/* FDF_CACHE_STAT_HFSTC */
    "",/* FDF_CACHE_STAT_FHSTF */
    "",/* FDF_CACHE_STAT_HFCSH */
    "",/* FDF_CACHE_STAT_FHCSC */
    "",/* FDF_CACHE_STAT_FHCSF */
    "",/* FDF_CACHE_STAT_HFSSH */
    "",/* FDF_CACHE_STAT_FHSSC */
    "",/* FDF_CACHE_STAT_FHSSF */
    "",/* FDF_CACHE_STAT_HFDSH */
    "",/* FDF_CACHE_STAT_FHDSC */
    "",/* FDF_CACHE_STAT_FHDSF */
    "",/* FDF_CACHE_STAT_HFGLS */
    "",/* FDF_CACHE_STAT_FHGLC */
    "",/* FDF_CACHE_STAT_FHGLF */
    "",/* FDF_CACHE_STAT_HFGIC */
    "",/* FDF_CACHE_STAT_FHGIC */
    "",/* FDF_CACHE_STAT_FHGIF */
    "",/* FDF_CACHE_STAT_HFGBC */
    "",/* FDF_CACHE_STAT_FHGCC */
    "",/* FDF_CACHE_STAT_FHGCF */
    "",/* FDF_CACHE_STAT_HFGSN */
    "",/* FDF_CACHE_STAT_HFGCS */
    "",/* FDF_CACHE_STAT_FHGSC */
    "",/* FDF_CACHE_STAT_FHGSF */
    "",/* FDF_CACHE_STAT_HFSRR */
    "",/* FDF_CACHE_STAT_FHSRC */
    "",/* FDF_CACHE_STAT_FHSRF */
    "",/* FDF_CACHE_STAT_HFSPR */
    "",/* FDF_CACHE_STAT_FHSPC */
    "",/* FDF_CACHE_STAT_FHSPF */
    "",/* FDF_CACHE_STAT_HFFLA */
    "",/* FDF_CACHE_STAT_FHFLC */
    "",/* FDF_CACHE_STAT_FHFLF */
    "",/* FDF_CACHE_STAT_HFRVG */
    "",/* FDF_CACHE_STAT_FHRVC */
    "",/* FDF_CACHE_STAT_FHRVF */
    "",/* FDF_CACHE_STAT_HFNOP */
    "",/* FDF_CACHE_STAT_FHNPC */
    "",/* FDF_CACHE_STAT_FHNPF */
   "", /* FDF_CACHE_STAT_HFOSH */
    "",/* FDF_CACHE_STAT_FHOSC */
    "",/* FDF_CACHE_STAT_FHOSF */
    "",/* FDF_CACHE_STAT_HFFLS */
    "",/* FDF_CACHE_STAT_FHFCC */
    "",/* FDF_CACHE_STAT_FHFCF */
    "",/* FDF_CACHE_STAT_HFFIV */
    "",/* FDF_CACHE_STAT_FHFIC */
    "",/* FDF_CACHE_STAT_FHFIF */
    "",/* FDF_CACHE_STAT_HFINV */
    "",/* FDF_CACHE_STAT_FHINC */
    "",/* FDF_CACHE_STAT_FHINF */
    "",/* FDF_CACHE_STAT_HFFLC */
    "",/* FDF_CACHE_STAT_FHLCC */
    "",/* FDF_CACHE_STAT_FHLCF */
    "",/* FDF_CACHE_STAT_HFFLI */
    "",/* FDF_CACHE_STAT_FHLIC */
    "",/* FDF_CACHE_STAT_FHLIF */
    "",/* FDF_CACHE_STAT_HFINC */
    "",/* FDF_CACHE_STAT_FHCIC */
    "",/* FDF_CACHE_STAT_FHCIF */
    "",/* FDF_CACHE_STAT_EOK */
    "",/* FDF_CACHE_STAT_EPERM */
    "",/* FDF_CACHE_STAT_ENOENT */
    "",/* FDF_CACHE_STAT_EDATASIZE */
    "",/* FDF_CACHE_STAT_ESTOPPED */
    "",/* FDF_CACHE_STAT_EBADCTNR */
    "",/* FDF_CACHE_STAT_EDELFAIL */
    "",/* FDF_CACHE_STAT_EAGAIN */
    "",/* FDF_CACHE_STAT_ENOMEM */
    "",/* FDF_CACHE_STAT_EACCES */
    "",/* FDF_CACHE_STAT_EINCONS */
    "",/* FDF_CACHE_STAT_EBUSY */
    "",/* FDF_CACHE_STAT_EEXIST */
    "",/* FDF_CACHE_STAT_EINVAL */
    "",/* FDF_CACHE_STAT_EMFILE */
    "",/* FDF_CACHE_STAT_ENOSPC */
    "",/* FDF_CACHE_STAT_ENOBUFS */
    "",/* FDF_CACHE_STAT_ESTALE */
    "",/* FDF_CACHE_STAT_EDQUOT */
    "",/* FDF_CACHE_STAT_RMT_EDELFAIL */
    "",/* FDF_CACHE_STAT_RMT_EBADCTNR */
    "",/* FDF_CACHE_STAT_HASH_BUCKETS */
    "",/* FDF_CACHE_STAT_NUM_SLABS */
    "",/* FDF_CACHE_STAT_NUM_ELEMENTS */
    "",/* FDF_CACHE_STAT_MAX_SIZE */
    "",/* FDF_CACHE_STAT_CURR_SIZE */
    "",/* FDF_CACHE_STAT_CURR_SIZE_WKEYS */
    "",/* FDF_CACHE_STAT_NUM_MODIFIED_OBJS */
    "",/* FDF_CACHE_STAT_NUM_MODIFIED_OBJS_WKEYS */
    "",/* FDF_CACHE_STAT_NUM_MODIFIED_OBJS_FLUSHED */
    "",/* FDF_CACHE_STAT_NUM_MODIFIED_OBJS_BGFLUSHED */
    "",/* FDF_CACHE_STAT_NUM_PENDING_REQS */
    "",/* FDF_CACHE_STAT_NUM_MODIFIED_OBJC_REC */
    "",/* FDF_CACHE_STAT_BGFLUSH_PROGRESS */
    "",/* FDF_CACHE_STAT_NUM_BGFLUSH */
    "",/* FDF_CACHE_STAT_NUM_FLUSH_PARALLEL */
    "",/* FDF_CACHE_STAT_NUM_BGFLUSH_PARALLEL */
    "",/* FDF_CACHE_STAT_BGFLUSH_WAIT */
    "",/* FDF_CACHE_STAT_MODIFIED_PCT */
    "",/* FDF_CACHE_STAT_NUM_APP_BUFFERS */
    "",/* FDF_CACHE_STAT_NUM_CACHE_OPS_PROG */
    "",/* FDF_CACHE_STAT_NUM_FGBUFFER_PROCESSED */
    "",/* FDF_CACHE_STAT_NUM_RESP_PROCESSED  */
};

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
    FDF_DRAM_CACHE_HITS = 0,
    FDF_DRAM_CACHE_MISSES,
    FDF_FLASH_CACHE_HITS,
    FDF_FLASH_CACHE_MISSES,
    FDF_DRAM_CACHE_CASTOUTS,
    FDF_DRAM_N_OVERWRITES,
    FDF_DRAM_N_IN_PLACE_OVERWRITES,
    FDF_DRAM_N_NEW_ENTRY,
    MCD_NUM_SDF_STATS,
} FDF_cache_stats_t;

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

FDF_status_t FDFGetStatsStr (
	struct FDF_thread_state *fdf_thread_state,
	FDF_cguid_t 			 cguid,
	char 					*stats_str,
    FDF_stats_t				*stats
	);

void action_stats_new_cguid(SDF_internal_ctxt_t *pac, char *str, int size, SDF_cguid_t cguid);
void action_stats(SDF_internal_ctxt_t *pac, char *str, int size);

static void set_log_level( unsigned int log_level )
{
    char                buf[80];
    char              * levels[] = { "devel", "trace", "debug", "diagnostic",
                                     "info", "warn", "error", "fatal" };

    sprintf( buf, "apps/memcached/server=%s", levels[log_level] );
    plat_log_parse_arg( buf );
}

static int fdf_check_delete_in_future(void *data)
{
    return(0);
}

static void fdf_load_settings(flash_settings_t *osd_settings)
{
    /* Set properties which defaults isn't suitable for library */
	insertProperty("SDF_PROP_FILE_VERSION", "1");
	insertProperty("SHMEM_FAKE", "1");
	insertProperty("MEMCACHED_STATIC_CONTAINERS", "1");
	insertProperty("SDF_MSG_ENGINE_START", "0");
	insertProperty("SDF_FLASH_PROTOCOL_THREADS", "1");
	insertProperty("FDF_LOG_FLUSH_DIR", "/tmp");
//	insertProperty("FDF_CC_BUCKETS", "1000");
//	insertProperty("FDF_CC_NSLABS", "100");

    (void) strcpy(osd_settings->aio_base, getProperty_String("FDF_FLASH_FILENAME", "/tmp/schooner%d")); // base filename of flash files

	/* This is added for compatibility with old property files which don't contain FDF_FLASH_FILENAME property */
	const char *p = getProperty_String("AIO_BASE_FILENAME", osd_settings->aio_base);
	if(p != osd_settings->aio_base)
	    (void) strcpy(osd_settings->aio_base, p); // base filename of flash files

    osd_settings->aio_create          = 1;// use O_CREAT - membrain sets this to 0
    osd_settings->aio_total_size      = getProperty_Int("FDF_FLASH_SIZE", 2); // this flash size counts! 2Gb by default
    osd_settings->aio_total_size      = getProperty_Int("AIO_FLASH_SIZE_TOTAL", osd_settings->aio_total_size); // compatibility with old property files
    osd_settings->aio_sync_enabled    = getProperty_Int("AIO_SYNC_ENABLED", 0); // AIO_SYNC_ENABLED
    osd_settings->rec_log_verify      = 0;
    osd_settings->enable_fifo         = 1;
    osd_settings->bypass_aio_check    = 0;
    osd_settings->chksum_data         = 1; // membrain sets this to 0
    osd_settings->chksum_metadata     = 1; // membrain sets this to 0
    osd_settings->sb_data_copies      = 0; // use default
    osd_settings->multi_fifo_writers  = getProperty_Int("SDF_MULTI_FIFO_WRITERS", 1);
    osd_settings->aio_wc              = false;
    osd_settings->aio_error_injection = false;
    osd_settings->aio_queue_len       = MCD_MAX_NUM_FTHREADS;

    // num_threads // legacy--not used

    osd_settings->num_cores        = getProperty_Int("SDF_FTHREAD_SCHEDULERS", 1); // "-N" 
    osd_settings->num_sched        = osd_settings->num_cores;
    osd_settings->num_sdf_threads  = getProperty_Int("SDF_THREADS_PER_SCHEDULER", 1); // "-T"

    osd_settings->sdf_log_level    = getProperty_Int("SDF_LOG_LEVEL", 4); 
    osd_settings->aio_num_files    = getProperty_Int("AIO_NUM_FILES", 1); // "-Z"
    osd_settings->aio_sub_files    = 0; // what are these? ignore?
    osd_settings->aio_first_file   = 0; // "-z" index of first file! - membrain sets this to -1
    osd_settings->mq_ssd_balance   = 0;  // what does this do?
    osd_settings->no_direct_io     = 0; // do NOT use O_DIRECT
    osd_settings->sdf_persistence  = 0; // "-V" force containers to be persistent!
    osd_settings->max_aio_errors   = getProperty_Int("MEMCACHED_MAX_AIO_ERRORS", 1000 );
    osd_settings->check_delete_in_future = fdf_check_delete_in_future;
    osd_settings->pcurrent_time	    = &current_time;
    osd_settings->is_node_independent = 1;
    osd_settings->ips_per_cntr	    = 1;
    osd_settings->rec_log_size_factor = 0;
}

/*
** Globals
*/
static struct sdf_agent_state agent_state;
static sem_t Mcd_fsched_sem;
static sem_t Mcd_initer_sem;

ctnr_map_t CtnrMap[MCD_MAX_NUM_CNTRS];

#ifdef notdef
static int count_containers() {
    int i = 0;
    int count = 0;

    for (i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
        if ( CtnrMap[i].cguid != 0 ) {
            ++count;
        }
    }

    return count;
}
#endif /* notdef */

#if 1
void dump_map() {
    for (int i=0; i<MCD_MAX_NUM_CNTRS; i++) {
        if (CtnrMap[i].cguid != 0) {
            fprintf(stderr, ">>>CtnrMap[%d].cname           = %s\n", i, CtnrMap[i].cname);
            fprintf(stderr, ">>>CtnrMap[%d].cguid           = %lu\n", i, CtnrMap[i].cguid);
            fprintf(stderr, ">>>CtnrMap[%d].sdf_container   = %d\n", i, !isContainerNull(CtnrMap[i].sdf_container));
            fprintf(stderr, ">>>CtnrMap[%d].current_size 	= %lu\n", i, CtnrMap[i].current_size);
        }
    }
}
#endif /* notdef */

int fdf_get_ctnr_from_cguid(
	FDF_cguid_t cguid
	)
{
    int i;
    int i_ctnr = -1;

    for ( i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
        if ( CtnrMap[i].cguid == cguid ) {
	    	i_ctnr = i;
	    	break;
		}
    }

    return i_ctnr;
}

int fdf_get_ctnr_from_cname(
	char *cname
	)
{
    int i;
    int i_ctnr = -1;

    for ( i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
	if ( (NULL != CtnrMap[i].cname) && (0 == strcmp( CtnrMap[i].cname, cname )) ) {
            i_ctnr = i;
            break;
        }
    }

    return i_ctnr;
}

// Return 0 - not open, 1 - open
int fdf_is_ctnr_open(
    FDF_cguid_t cguid
    )
{
    int i		= -1;
    int result	= 0;

	i = fdf_get_ctnr_from_cguid( cguid );

	if ( i >= 0 ) {
		if ( !isContainerNull(CtnrMap[i].sdf_container) ) {
			result = 1;
		}
	}

    return result;
}

#define MCD_FTH_STACKSIZE       81920

static void fdf_fth_initer(uint64_t arg)
{
    struct sdf_agent_state    	*state 		= (struct sdf_agent_state *) arg;

    /*
     *  Final SDF Initialization
     */
    if ( SDF_TRUE != agent_engine_post_init( state ) ) {
        mcd_log_msg( 20147, PLAT_LOG_LEVEL_FATAL,
                     "agent_engine_post_init() failed" );
        plat_assert_always( 0 == 1 );
    }

    mcd_fth_start_osd_writers();

    /*
     * signal the parent thread
     */
    sem_post( &Mcd_initer_sem );
}

#define STAT_BUFFER_SIZE 16384

static void print_fdf_stats(FILE *log, FDF_stats_t *stats, char *disp_str) {
    int i;
    char buf[BUF_LEN];
    fputs(disp_str,log); 
    for (i = 0; i < FDF_N_FLASH_STATS; i++ ) {
        if( stats->flash_stats[i] != 0 ) {
            sprintf(buf,"%s = %lu\n",FDFFlashStatsSearchStrings[i],stats->flash_stats[i]);
            fputs(buf,log);
        }
    }
    for (i = 0; i < FDF_N_CACHE_STATS; i++ ) {
        if( stats->cache_stats[i] != 0 ) {
            sprintf(buf,"%s = %lu\n",FDFCacheStatsSearchStrings[i],stats->cache_stats[i]);
            fputs(buf,log);
        }
    }
    for (i = 0; i < FDF_N_ACCESS_TYPES; i++ ) {
        if( stats->n_accesses[i] != 0 ) {
            sprintf(buf,"%s = %lu\n",FDFAccessTypeSearchStrings[i],stats->n_accesses[i]);
            fputs(buf,log);
        }
    }
    fputs("---------\n",log); 
    fflush(log);
}

/* Defined later in this file */
void fdf_trx_print_stats(FILE *log);

static void *fdf_stats_thread(void *arg) {
    FDF_cguid_t cguids[MCD_MAX_NUM_CNTRS];
    uint32_t n_cguids;
    char stats_str[STAT_BUFFER_SIZE];
    FILE *stats_log;
    int i;
    struct FDF_thread_state *thd_state;
    FDF_stats_t stats;


    if ( FDF_SUCCESS != FDFInitPerThreadState( ( struct FDF_state * ) arg, ( struct FDF_thread_state ** ) &thd_state )) {
        fprintf(stderr,"Stats Thread:Unable to open the log file /tmp/fdf_stats.log. Exiting\n");
        return NULL;
    }

    stats_log = fopen("/var/log/fdfstats.log","a+");
    if( stats_log == NULL ) {
        fprintf(stderr,"Stats Thread:Unable to open the log file /tmp/fdf_stats.log. Exiting\n");
        return NULL;
    }

    while(1) {
        FDFGetContainers(thd_state,cguids,&n_cguids);
        if( n_cguids <= 0 ) {
             fprintf(stderr,"Stats Thread:No container exists\n");    
             sleep(10);
             continue;
        }
        for ( i = 0; i < n_cguids; i++ ) {
            memset(stats_str,0,STAT_BUFFER_SIZE);
            FDFGetStatsStr(thd_state,cguids[i],stats_str,NULL);
            fputs(stats_str,stats_log);
            if ( getProperty_Int( "FDF_STATS_API_DEBUG", 0 ) == 1 ) {
                FDFGetContainerStats(thd_state,cguids[i],&stats);
                print_fdf_stats(stats_log,&stats,"Container\n");
                FDFGetStats(thd_state,&stats);
                print_fdf_stats(stats_log,&stats,"Flash\n");
            }
        }

		fdf_trx_print_stats(stats_log);

        sleep(getProperty_Int( "FDF_STATS_DUMP_INTERVAL", 60 ));
    }

    fclose(stats_log);
}

static void *fdf_scheduler_thread(void *arg)
{
    // mcd_osd_assign_pthread_id();

    /*
     * signal the parent thread
     */
    sem_post( &Mcd_fsched_sem );

    fthSchedulerPthread( 0 );

    return NULL;
}


static int fdf_fth_cleanup( void )
{
    return 0;   /* SUCCESS */
}

void fdf_start_stats_thread(struct FDF_state *sdf_state) {
    pthread_t thd;
    int rc;
    fprintf(stderr,"starting stats thread\n");
    rc = pthread_create(&thd,NULL,fdf_stats_thread,(void *)sdf_state);
    if( rc != 0 ) {
        fprintf(stderr,"Unable to start the stats thread\n");
    }
}


static void *fdf_run_schedulers(void *arg)
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
        rc = pthread_create( &fth_pthreads[i], &attr, fdf_scheduler_thread,
                             NULL);
        if ( 0 != rc ) {
            mcd_log_msg( 20163, PLAT_LOG_LEVEL_FATAL,
                         "pthread_create() failed, rc=%d", rc );
	    plat_assert(0);
        }
        mcd_log_msg( 20164, PLAT_LOG_LEVEL_DEBUG, "scheduler %d created",
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

    fdf_fth_cleanup();

    return(NULL);
}

/*
** API
*/
void FDFSetProperty(const char* property, const char* value)
{
	value = strndup(value, 256);

	if(value)
		setProperty(property, (void*)value);
}

int FDFLoadProperties(const char *prop_file)
{
	return prop_file ? loadProperties(prop_file) : 0;
}

FDF_status_t FDFInit(
	struct FDF_state	**fdf_state
	)
{
    int                  rc;
    pthread_t            run_sched_pthread;
    pthread_attr_t       attr;
    uint64_t             num_sched;
    struct timeval		 timer;
	const char			*prop_file;

    sem_init( &Mcd_initer_sem, 0, 0 );

    gettimeofday( &timer, NULL );
    current_time = timer.tv_sec;

    *fdf_state = (struct FDF_state *) &agent_state;

    #ifdef FDF_REVISION
    plat_log_msg(160038, LOG_CAT, LOG_INFO, "Flash Data Fabric:%s",FDF_REVISION);
    #endif
	prop_file = getenv("FDF_PROPERTY_FILE");
	if (prop_file)
		loadProperties(prop_file);

    //  Initialize a crap-load of settings
    fdf_load_settings( &(agent_state.flash_settings) );

    mcd_aio_register_ops();
    mcd_osd_register_ops();

    //  Set the logging level
    set_log_level( agent_state.flash_settings.sdf_log_level );

    if ( !agent_engine_pre_init( &agent_state, 0, NULL ) ) {
        return FDF_FAILURE; 
    }

    // spawn initer thread (like mcd_fth_initer)
    fthResume( fthSpawn( &fdf_fth_initer, MCD_FTH_STACKSIZE ),
               (uint64_t) &agent_state );

    ipf_set_active( 1 );

    // spawn scheduler startup process
    pthread_attr_init( &attr );
    pthread_attr_setdetachstate( &attr, PTHREAD_CREATE_JOINABLE );

    num_sched = agent_state.flash_settings.num_cores;

    rc = pthread_create( &run_sched_pthread, 
						 &attr, 
						 fdf_run_schedulers,
			 			 (void *) num_sched );
    if ( 0 != rc ) {
		mcd_log_msg( 20163, 
					 PLAT_LOG_LEVEL_FATAL,
		     		 "pthread_create() failed, rc=%d", 
					 rc );
		return rc;
    }

    mcd_log_msg( 150022, PLAT_LOG_LEVEL_DEBUG, "scheduler startup process created" );

    // Wait until mcd_fth_initer is done
    do {
		rc = sem_wait( &Mcd_initer_sem );
    } while (rc == -1 && errno == EINTR);

   	plat_assert( 0 == rc );
   	if ( getProperty_Int( "FDF_STATS_THREAD", 0 ) == 1 ) {
       	fprintf( stderr,"Starting the stats thread. Check the stats at /var/log/fdfstats.log\n" );
       	fdf_start_stats_thread( *fdf_state );
   	}

	fdf_start_vc_thread ( *fdf_state );

#if 0
    /*
     * automatically add MCD_MAX_NUM_CNTRS to the maximum recovered
     * container id to avoid potential reuse of ids from containers
     * that are partially deleted 
     */
    uint64_t cntr_id = 0; 
    for ( i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
        if (Mcd_containers[i] == NULL) {
	        continue;
	    }

	    SDF_container_props_t sdf_properties;
		SDF_status_t props_rc = FDFGetContainerProps( fth_state->pai,
													  pMcd_containers[i]->cguid,
	                                                  &sdf_properties );
		if ( FDF_SUCCESS != props_rc ) {
			mcd_log_msg( 50030, PLAT_LOG_LEVEL_ERROR,
	                     "failed to get SDF properties, status=%s",
	                     SDF_Status_Strings[props_rc] );
	        plat_abort();
		}

		if ( cntr_id < sdf_properties->container_id.container_id) {   
			cntr_id = sdf_properties->container_id.container_id;
        }
    }
    
    Mcd_next_cntr_id = cntr_id + MCD_MAX_NUM_CNTRS + 1;
    mcd_log_msg( 20131, PLAT_LOG_LEVEL_DEBUG,
                 "next container id %lu", Mcd_next_cntr_id );
#endif

    return FDF_SUCCESS;
}

FDF_status_t FDFInitPerThreadState(
	struct FDF_state 		 *fdf_state,
    struct FDF_thread_state **thd_state
	)
{
    SDF_action_init_t       *pai;
    SDF_action_init_t       *pai_new;
    SDF_action_thrd_state_t *pts;

    struct sdf_agent_state    *state = &agent_state;

    fthSpawnPthread();

    pai = &state->ActionInitState;

    pai_new = (SDF_action_init_t *) plat_alloc( sizeof( SDF_action_init_t ) );
    plat_assert_always( NULL != pai_new );
    memcpy( pai_new, pai, (size_t) sizeof( SDF_action_init_t ));

    //==================================================


    pts = (SDF_action_thrd_state_t *)
          plat_alloc( sizeof( SDF_action_thrd_state_t ) );
    plat_assert_always( NULL != pts );
    pts->phs = state->ActionInitState.pcs;

    pai_new->pts = (void *) pts;
    InitActionAgentPerThreadState( pai_new->pcs, pts, pai_new );
    pai_new->paio_ctxt = pts->pai->paio_ctxt;

    pai_new->ctxt = ActionGetContext( pts );

    *thd_state = (struct FDF_thread_state *) pai_new;

    return FDF_SUCCESS;
}

FDF_status_t FDFReleasePerThreadState(
    struct FDF_thread_state **thd_state
    )
{
	plat_free( *thd_state );
	return FDF_SUCCESS;
}

FDF_status_t FDFShutdown(
	struct FDF_state	*fdf_state
	)
{
	return FDF_SUCCESS;
}

FDF_status_t FDFLoadCntrPropDefaults(
	FDF_container_props_t *props
	)
{
	props->size_kb = 1024 * 1024;
	//props->fifo_mode = SDF_TRUE;
	props->persistent = SDF_FALSE;
    props->evicting = SDF_TRUE;
	props->writethru = SDF_TRUE;
	//props->async_writes = SDF_FALSE;
	props->durability_level = FDF_DURABILITY_PERIODIC;
	//props->cguid = 0;
	//props->num_shards = 1;
	return FDF_SUCCESS;
}

#define CONTAINER_PENDING

//static uint64_t cid_counter = 0;
char *FDFStrError(FDF_status_t fdf_errno) {
    if ( fdf_errno >= N_FDF_STATUS_STRINGS ) {
        return FDF_Status_Strings[0];  
    }
    return FDF_Status_Strings[fdf_errno]; 
}

FDF_status_t FDFOpenContainer(
	struct FDF_thread_state	*fdf_thread_state, 
	char					*cname,
	FDF_container_props_t	*properties,
	uint32_t				 flags,
	FDF_cguid_t 	 	 	*cguid
	)
{
	FDF_status_t status		= FDF_SUCCESS;

	if ( flags & FDF_CTNR_CREATE ) {
		status = fdf_create_container( fdf_thread_state,
									   cname,
									   properties,
									   flags,
									   cguid,
									   FDF_VIRTUAL_CNTR );
	}
		
	if ( FDF_SUCCESS == status ) {
		status = fdf_open_container( fdf_thread_state,
									 cname,
									 properties,
									 flags,
									 cguid,
									 FDF_VIRTUAL_CNTR,
									 FDF_TRUE );
	}

	return status;
}

FDF_status_t FDFOpenPhysicalContainer(
    struct FDF_thread_state *fdf_thread_state,
    char                    *cname,
    FDF_container_props_t   *properties,
    uint32_t                 flags,
    FDF_cguid_t             *cguid
    )
{
    FDF_status_t status     = FDF_SUCCESS;

    if ( flags & FDF_CTNR_CREATE ) {
        status = fdf_create_container( fdf_thread_state,
                                       cname,
                                       properties,
                                       flags,
                                       cguid,
									   FDF_PHYSICAL_CNTR );
    }

    if ( FDF_SUCCESS == status ) {
        status = fdf_open_container( fdf_thread_state,
                                     cname,
                                     properties,
                                     flags,
                                     cguid,
									 FDF_PHYSICAL_CNTR,
									 FDF_TRUE );
    }

    return status;
}

static FDF_status_t fdf_create_container(
	struct FDF_thread_state	*fdf_thread_state, 
	char					*cname,
	FDF_container_props_t	*properties,
	uint32_t				 flags,
	FDF_cguid_t 	 	 	*cguid,
	FDF_container_mode_t	 mode
	)
{
    int					 	 	 	 i							= 0;
    struct SDF_shared_state			*state 						= &sdf_shared_state;
    FDF_status_t 			 	 	 status 					= FDF_FAILURE;
    SDF_shardid_t 			 	 	 shardid 					= SDF_SHARDID_INVALID;
    SDF_container_meta_t 			*meta 						= NULL;
    SDF_CONTAINER_PARENT 		 	 parent 					= containerParentNull;
    local_SDF_CONTAINER_PARENT 	 	 lparent 					= NULL;
    SDF_boolean_t 			 	 	 isCMC						= SDF_FALSE;
    uint32_t 				 	 	 in_shard_count				= 0;
    uint32_t 				 	 	 num_objs 					= 0;
    const char 						*writeback_enabled_string	= NULL;
    SDF_internal_ctxt_t				*pai 						= (SDF_internal_ctxt_t *) fdf_thread_state;
	SDF_container_props_t 			*sdf_properties				= NULL;
#if 0
	FDF_internal_container_props_t   iproperties;
#endif

	if ( !properties || !cguid || !fdf_thread_state ||
		  ISEMPTY( cname ) ) {
		return FDF_INVALID_PARAMETER;
	}

	*cguid = 0;

	properties->persistent			= SDF_TRUE;
	properties->evicting  			= SDF_FALSE;
	properties->writethru  			= SDF_TRUE;
#if 0
    iproperties.current_size		= 0;
    iproperties.num_obj				= 0;
    iproperties.fifo_mode			= FDF_FALSE;
    iproperties.cguid				= 0;
	iproperties.num_shards 			= 1;  
    iproperties.async_writes		= FDF_FALSE;
#endif

     plat_log_msg(160033, LOG_CAT, LOG_INFO, "%s, size=%ld bytes", cname, (long)properties->size_kb * 1024);

#ifdef notdef
	if( properties->size_kb < 1024 * 1024 )
	{
     	plat_log_msg(160036, LOG_CAT, LOG_ERR, "%s, container size=%d bytes is less then minimum container size, which is 1Gb", cname, properties->size_kb * 1024);
		return FDF_FAILURE_CONTAINER_TOO_SMALL;
	}
#endif

    if ( !properties->writethru ) {
        if ( !properties->evicting ) {
            plat_log_msg( 30572, LOG_CAT, LOG_ERR,
                          "Writeback caching can only be enabled for eviction mode containers" );
        	return FDF_FAILURE_INVALID_CONTAINER_TYPE;
        } else {
            writeback_enabled_string = getProperty_String( "SDF_WRITEBACK_CACHE_SUPPORT", "On" );
            if ( strcmp( writeback_enabled_string, "On" ) != 0 ) {
                plat_log_msg( 30575, LOG_CAT, LOG_ERR,
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
        return FDF_FAILURE_INVALID_CONTAINER_TYPE;
	}
#endif /* notdef */

    SDFStartSerializeContainerOp( pai );

    if ( strcmp( cname, CMC_PATH ) == 0 ) {
        *cguid = CMC_CGUID;
        isCMC = SDF_TRUE;
    } else {
        // Make sure we have not gone over the container limit
        for ( i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
            if ( CtnrMap[i].cguid == 0 ) { 
                // this is an unused map entry
                break;
            }
        }

		if ( i == MCD_MAX_NUM_CNTRS ) {
	    	plat_log_msg( 150033, 
						  LOG_CAT,LOG_ERR, 
						  "FDFCreateContainer failed for container %s because 128 containers have already been created.", 
						  cname );
	    	status = FDF_TOO_MANY_CONTAINERS;
			goto out;
		}

		if ( ( status = fdf_generate_cguid( fdf_thread_state, cguid ) ) != FDF_SUCCESS ) {
			plat_log_msg( 150084,
						  LOG_CAT,
						  LOG_ERR,
						  "Failed to generate container id for %s", 
						  cname );
	    	status = FDF_TOO_MANY_CONTAINERS;
	    	SDFEndSerializeContainerOp( pai );
	    	return status;
		}

        isCMC = SDF_FALSE;
        init_get_my_node_id();
	
		// Save the current cguid counter for use in recovery
        if ( SDF_SUCCESS != name_service_put_cguid_state( pai,
                                                          init_get_my_node_id(),
                                                          *cguid ) ) {
            plat_log_msg( 150034,
                        LOG_CAT,
                        LOG_ERR,
                        "Failed to save cguid state: %s",
                        SDF_Status_Strings[status] );

            status = FDF_PUT_METADATA_FAILED;
			goto out;
        }
    }

#if 0
    iproperties.cguid = *cguid;
#else
	properties->cguid = *cguid;
#endif

#if 0
	if ( (sdf_properties = fdf_create_sdf_props( properties, &iproperties ) ) == NULL ) {
		*cguid = SDF_NULL_CGUID;
		status = FDF_FAILURE_MEMORY_ALLOC;
		goto out;
	}
#else
	if ( (sdf_properties = fdf_create_sdf_props( properties ) ) == NULL ) {
		*cguid = SDF_NULL_CGUID;
		status = FDF_FAILURE_MEMORY_ALLOC;
		goto out;
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

    num_objs = sdf_properties->container_id.num_objs;

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
    plat_log_msg( 21527, LOG_CAT, LOG_INFO, "Container: %s - Multi Devs: %d",
                  path, state->config.flash_dev_count );
#else
    plat_log_msg( 21528, LOG_CAT, LOG_INFO, "Container: %s - Single Dev",
                  cname );
#endif
    plat_log_msg( 21529, LOG_CAT, LOG_INFO, "Container: %s - Num Shards: %d",
                  cname, sdf_properties->shard.num_shards );

    plat_log_msg( 21530, LOG_CAT, LOG_INFO, "Container: %s - Num Objs: %d",
                  cname, state->config.num_objs );

    plat_log_msg( 21531, LOG_CAT, LOG_INFO, "Container: %s - DEBUG_MULTI_SHARD_INDEX: %d",
                  cname, getProperty_Int( "DEBUG_MULTISHARD_INDEX", -1 ) );


    if ( doesContainerExistInBackend( pai, cname )) {
        #ifdef CONTAINER_PENDING
        // Unset parent delete flag if with deleted flag
        if ( !isContainerParentNull( parent = isParentContainerOpened( cname ) ) ) {
                local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent( &lparent, parent );

             if ( lparent->delete_pending == SDF_TRUE ) {
                    if ( !isCMC && (status = name_service_lock_meta( pai, cname )) != FDF_SUCCESS ) {
                            plat_log_msg( 21532, LOG_CAT, LOG_ERR, "failed to lock %s", cname );
                }

                lparent->delete_pending = SDF_FALSE;

                if ( !isCMC && (status = name_service_unlock_meta( pai, cname )) != FDF_SUCCESS ) {
                            plat_log_msg( 21533, LOG_CAT, LOG_ERR, "failed to unlock %s", cname );
                }

             }

            releaseLocalContainerParent( &lparent ); // TODO C++ please!
        }
        #endif
		status = FDF_CONTAINER_EXISTS;
    } else {

        if ( mode == FDF_PHYSICAL_CNTR ) {

			if ( ( shardid = build_shard( state, pai, cname, num_objs,
                                          in_shard_count, *sdf_properties, *cguid,
                                          isCMC ? BUILD_SHARD_CMC : BUILD_SHARD_OTHER, cname ) ) <= SDF_SHARDID_LIMIT ) {
				if ( VDC_CGUID == *cguid )
					vdc_shardid = shardid;
			}
		} else {
			shardid = vdc_shardid;
		}

		if ( shardid != SDF_SHARDID_INVALID ) {
            if ( (meta = build_meta( cname, *sdf_properties, (SDF_cguid_t) *cguid, shardid )) != NULL ) {
#ifdef STATE_MACHINE_SUPPORT
                SDFUpdateMetaClusterGroupInfo( pai, meta, sdf_properties->container_id.container_id );
#endif
                if ( create_put_meta( pai, cname, meta, (SDF_cguid_t) *cguid ) == SDF_SUCCESS ) {

                    // For non-CMC, map the cguid and cname
                    if ( !isCMC && (name_service_create_cguid_map( pai, cname, *cguid )) != SDF_SUCCESS ) {
                        plat_log_msg( 21534, LOG_CAT, LOG_ERR,
                                      "failed to map cguid: %s", cname );
                    }
                    if ( !isCMC && (status = name_service_lock_meta( pai, cname )) != FDF_SUCCESS ) {
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

                        status = FDF_SUCCESS;

                        if ( !isCMC && (status = name_service_unlock_meta( pai, cname )) != FDF_SUCCESS ) {
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
			status = FDF_OUT_OF_STORAGE_SPACE;
        }
    }

    plat_log_msg( 21511, LOG_CAT, LOG_DBG, "%s - %s", cname, SDF_Status_Strings[status] );

    if ( status != FDF_SUCCESS && status != FDF_CONTAINER_EXISTS ) {
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
    else if ( status == FDF_SUCCESS ) {
        SDFRepDataStructAddContainer( pai, sdf_properties, *cguid );
    }
*/
#endif

    if ( FDF_SUCCESS == status && CMC_CGUID != *cguid ) {
        for ( i=0; i<MCD_MAX_NUM_CNTRS; i++ ) {
            if ( CtnrMap[i].cguid == 0 ) {
                // this is an unused map entry
                strcpy( CtnrMap[i].cname, cname );
                CtnrMap[i].cguid         = *cguid;
                CtnrMap[i].sdf_container = containerNull;
                CtnrMap[i].size_kb     	 = properties->size_kb;
                CtnrMap[i].current_size	 = 0;
#ifdef SDFAPIONLY
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

	//plat_assert(status != FDF_SUCCESS || *cguid);

    plat_log_msg(160034, LOG_CAT, LOG_INFO, "%s(cguid=%lu) - %s", cname, *cguid, SDF_Status_Strings[status]);

    return status;
}

static FDF_status_t fdf_open_container(
	struct FDF_thread_state	*fdf_thread_state, 
	char					*cname,
	FDF_container_props_t	*props,
	uint32_t				 flags,
	FDF_cguid_t 	 	 	*cguid,
	FDF_container_mode_t	 mode,
	FDF_boolean_t			 serialize
	) 
{               
    FDF_status_t 				 status 	= FDF_SUCCESS;
    local_SDF_CONTAINER 		 lc 		= NULL;
    SDF_CONTAINER_PARENT 		 parent;
    local_SDF_CONTAINER_PARENT 	 lparent 	= NULL;
    int 						 log_level 	= LOG_ERR;
    int  						 i_ctnr 	= -1;
    SDF_CONTAINER 				 container 	= containerNull;
    SDF_internal_ctxt_t     	*pai 		= (SDF_internal_ctxt_t *) fdf_thread_state;
#ifdef SDFAPIONLY
    mcd_osd_shard_t 			*mcd_shard	= NULL;
    struct shard				*shard		= NULL;
#endif /* SDFAPIONLY */
	SDF_container_meta_t		 meta;

	if ( serialize )                        
	    SDFStartSerializeContainerOp( pai );

	if ( !cguid ) {
		status = FDF_INVALID_PARAMETER;
		goto out;
	}

    if ( ISEMPTY( cname ) ) { 
        status = SDF_INVALID_PARAMETER;
		*cguid = SDF_NULL_CGUID;
		goto out;
	}

    plat_log_msg( 20819, LOG_CAT, LOG_INFO, "%s", cname );

    if ( strcmp( cname, CMC_PATH ) != 0 ) {
		i_ctnr = fdf_get_ctnr_from_cname( cname );

    	if (i_ctnr == -1) {
        	status = SDF_INVALID_PARAMETER;
    	} else {
			*cguid = CtnrMap[i_ctnr].cguid;
    	}

    } else {
		i_ctnr = 0;
		*cguid = CMC_CGUID;
    }

    if ( !isContainerNull( CtnrMap[i_ctnr].sdf_container ) ) {
        SDFEndSerializeContainerOp( pai );
        plat_log_msg( 160032, LOG_CAT, log_level, "Already opened or error: %s - %s", cname, SDF_Status_Strings[status] );
        status = FDF_SUCCESS;
		goto out;
    }

    if ( !isContainerParentNull( parent = isParentContainerOpened( cname ) ) ) {
                
        plat_log_msg( 20819, LOG_CAT, LOG_INFO, "%s", cname );

        // Test for pending delete
        lparent = getLocalContainerParent( &lparent, parent );
        if ( lparent->delete_pending == SDF_TRUE ) {
            // Need a different error?
            status = SDF_CONTAINER_UNKNOWN;
            plat_log_msg( 21552, LOG_CAT,LOG_ERR, "Delete pending for %s", cname );
        } 
        releaseLocalContainerParent( &lparent );
    }

    if ( status == FDF_SUCCESS ) {

        // Ok to open
        container = openParentContainer( pai, cname );

        if ( isContainerNull( container ) ) {
	    	fprintf( stderr, "FDFOpenContainer: failed to open parent container for %s\n", cname );
		}

        if ( CMC_CGUID == *cguid ) {
            theCMC->c = internal_serverToClientContainer( container );
        } else {
            CtnrMap[i_ctnr].sdf_container = container;
        }

        if ( !isContainerNull( container ) ) {
            lc = getLocalContainer( &lc, container );
            lc->mode = SDF_READ_WRITE_MODE; // (container)->mode = mode;
            _sdf_print_container_descriptor( container );
            log_level = LOG_INFO;
			if ( mode == FDF_PHYSICAL_CNTR ) {
            	// FIXME: This is where the call to shardOpen goes.
            	#define MAX_SHARDIDS 32 // Not sure what max is today
            	SDF_shardid_t shardids[MAX_SHARDIDS];
            	uint32_t shard_count;
            	get_container_shards( pai, lc->cguid, shardids, MAX_SHARDIDS, &shard_count );
            	for ( int i = 0; i < shard_count; i++ ) {
                	struct SDF_shared_state *state = &sdf_shared_state;
                	shardOpen( state->config.flash_dev, shardids[i] );
            	}

				if ( lc->cguid == VDC_CGUID ) {
    				name_service_get_meta(pai, lc->cguid, &meta);
					vdc_shardid = meta.shard;
				}
			}

            status = SDFActionOpenContainer( pai, lc->cguid );
            if ( status != FDF_SUCCESS ) {
                plat_log_msg( 21554, LOG_CAT,LOG_ERR, "SDFActionOpenContainer failed for container %s", cname );
            }

#ifdef SDFAPIONLY
            if ( mode == FDF_PHYSICAL_CNTR && CMC_CGUID != *cguid ) {
                shard = container_to_shard( pai, lc );
                if ( NULL != shard ) {
                    mcd_shard = (mcd_osd_shard_t *)shard;
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
            plat_log_msg( 21555, LOG_CAT, LOG_DBG, "Opened %s", cname );
        } else {
            status = SDF_CONTAINER_UNKNOWN;
            plat_log_msg( 21556, LOG_CAT,LOG_ERR, "Failed to find %s", cname );
        }
    }

    if ( cname ) {
		plat_log_msg( 21511, LOG_CAT, log_level, "%s - %s", cname, SDF_Status_Strings[status] );
    } else {
		plat_log_msg( 150024, LOG_CAT, log_level, "NULL - %s", SDF_Status_Strings[status] );
    }

 out:
	if (serialize )
	    SDFEndSerializeContainerOp( pai );

    return status;
}

FDF_status_t FDFCloseContainer(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t  		     cguid
	)
{
	return fdf_close_container( fdf_thread_state,
								cguid,
								FDF_VIRTUAL_CNTR,
								FDF_TRUE
							  );
}

FDF_status_t FDFClosePhysicalContainer(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t  		     cguid
	)
{
	return fdf_close_container( fdf_thread_state,
								cguid,
								FDF_PHYSICAL_CNTR,
								FDF_TRUE
							  );
}

static FDF_status_t fdf_close_container(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t  		     cguid,
	FDF_container_mode_t	 mode,
	FDF_boolean_t			 serialize
	)
{
#ifdef SDFAPIONLY
    struct shard			*shard			= NULL;
    flashDev_t              *flash_dev;
    SDF_container_meta_t     meta;
    FDF_status_t			 tmp_status;
    struct SDF_shared_state *state			= &sdf_shared_state;
#endif
    FDF_status_t			 status			= SDF_FAILURE;
    int						 i_ctnr;
    SDF_CONTAINER			 container		= containerNull;
    SDF_internal_ctxt_t     *pai			= (SDF_internal_ctxt_t *) fdf_thread_state;
    int						 log_level		= LOG_ERR;
    int						 ok_to_delete	= 0;

    plat_log_msg(21630, LOG_CAT, LOG_INFO, "%lu", cguid);

	if ( !cguid )
		return FDF_INVALID_PARAMETER;

	if ( serialize )
	    SDFStartSerializeContainerOp(pai);

    i_ctnr = fdf_get_ctnr_from_cguid(cguid);

    if (i_ctnr == -1) {
        status = FDF_INVALID_PARAMETER;
		goto out;
    } else {
		container = CtnrMap[i_ctnr].sdf_container;
    }

    if (isContainerNull(container)) {
        status = FDF_FAILURE_CONTAINER_NOT_OPEN;
    } else {

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
            status = FDF_SUCCESS;
            log_level = LOG_INFO;
        }

#ifdef SDFAPIONLY
    	if ( mode == FDF_PHYSICAL_CNTR && 
			(status = name_service_get_meta(pai, cguid, &meta)) == FDF_SUCCESS) {

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
	    if ((status = name_service_flush_inval_object_container(pai, path)) != FDF_SUCCESS) {
			plat_log_msg(21540, LOG_CAT, LOG_ERR,
			     "%s - failed to flush and invalidate container", path);
			log_level = LOG_ERR;
	    } else {
			plat_log_msg(21541, LOG_CAT, LOG_DBG,
			     "%s - flush and invalidate container succeed", path);
	    }

		if ((tmp_status = name_service_get_meta_from_cname(pai, path, &meta)) == FDF_SUCCESS) {
		    tmp_status = SDFActionDeleteContainer(pai, &meta);
		    if (tmp_status != FDF_SUCCESS) {
				// xxxzzz container will be left in a weird state!
				plat_log_msg(21542, LOG_CAT, LOG_ERR,
					"%s - failed to delete action thread container state", path);
				log_level = LOG_ERR;
		    } else {
				plat_log_msg(21543, LOG_CAT, LOG_DBG,
					"%s - action thread delete container state succeeded", path);
		    }
		}

		if(shard)
			shardClose(shard);
#endif

        if ( status == FDF_SUCCESS ) {
    		CtnrMap[i_ctnr].sdf_container = containerNull;

			if (ok_to_delete) {
			    plat_log_msg(160031, LOG_CAT, LOG_INFO, "Delete request pending. Deleting... cguid=%lu", cguid);

		    	status = delete_container_internal_low(pai, path, SDF_FALSE, mode == FDF_PHYSICAL_CNTR ? SDF_TRUE:SDF_FALSE, NULL);

	    		CtnrMap[i_ctnr].cguid           = 0;
	    		CtnrMap[i_ctnr].cname[0]		= '\0';
	    		CtnrMap[i_ctnr].size_kb			= 0;
	    		CtnrMap[i_ctnr].current_size	= 0;
			}
        }
    }

out:

    plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);

	if ( serialize )
	    SDFEndSerializeContainerOp(pai);

    return (status);
}

FDF_status_t FDFDeleteContainer(
    struct FDF_thread_state *fdf_thread_state,
    FDF_cguid_t              cguid
    )
{
	return fdf_delete_container( fdf_thread_state,
								 cguid,
								 FDF_VIRTUAL_CNTR
							   );
}

FDF_status_t FDFDeletePhysicalContainer(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t		 		 cguid
	)
{
	return fdf_delete_container( fdf_thread_state,
								 cguid,
								 FDF_PHYSICAL_CNTR
							   );
}

static FDF_status_t fdf_delete_container(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t		 		 cguid,
	FDF_container_mode_t	 mode
	)
{  
    FDF_status_t 	 	 	 status 		= FDF_FAILURE;
    FDF_status_t 	 	 	 del_status 	= FDF_FAILURE;
    int  			 	 	 i_ctnr			= -1;
	int				  	 	 ok_to_delete	= 0;
	SDF_container_meta_t	 meta;
	FDF_cguid_t	 			 mycguid		= 0;
    SDF_internal_ctxt_t 	*pai 			= (SDF_internal_ctxt_t *) fdf_thread_state;

    plat_log_msg( 21630, 
				  LOG_CAT, 
				  LOG_INFO, 
				  "%lu", 
				  cguid );

	if ( !cguid )
		return SDF_INVALID_PARAMETER;

    SDFStartSerializeContainerOp(pai);

	if ( ( status = name_service_get_meta( pai, cguid, &meta ) ) != FDF_SUCCESS ) {
		plat_log_msg( 150085, LOG_CAT, LOG_ERR, "Could not read metadata for %lu\n", cguid );
    	SDFEndSerializeContainerOp(pai);
		return FDF_FAILURE;
	} 

	meta.delete_in_progress = FDF_TRUE;

	if ( name_service_put_meta( pai, cguid, &meta ) != SDF_SUCCESS ) {
		plat_log_msg( 150086, LOG_CAT, LOG_ERR, "Could not mark delete in progress for container %lu\n", cguid );
	} 

    i_ctnr = fdf_get_ctnr_from_cguid( cguid );

    if ( i_ctnr >= 0 && !ISEMPTY( CtnrMap[i_ctnr].cname ) ) {

		if ( ( status = fdf_open_container( fdf_thread_state,  
   											CtnrMap[ i_ctnr ].cname,
   											NULL,
   											0,
   											&mycguid,
											mode,
											FDF_FALSE ) ) != FDF_SUCCESS ) {
			plat_log_msg( 150091,
						  LOG_CAT,
						  LOG_ERR,
						  "Cannot open container %lu to delete it",
						  cguid );
			goto out;
		}

		if ( ( del_status = fdf_delete_objects( fdf_thread_state, cguid ) ) != FDF_SUCCESS ) {
			plat_log_msg( 150092,
						  LOG_CAT,
						  LOG_ERR,
						  "Failed to delete container objects" );
		} 

		if ( ( status = fdf_close_container( fdf_thread_state,  
   											 mycguid,
											 mode,
											 FDF_FALSE ) ) != FDF_SUCCESS ) {
			plat_log_msg( 150093,
					  	  LOG_CAT,
						  LOG_ERR,
						  "Cannot close container %lu to delete it",
						  cguid );

			if ( FDF_SUCCESS != del_status )
				status = del_status;
			goto out;
		} else {

	        status = delete_container_internal_low( pai, 
													CtnrMap[i_ctnr].cname, 
													SDF_FALSE,  mode == FDF_PHYSICAL_CNTR ? SDF_TRUE:SDF_FALSE, 
													&ok_to_delete );

	        if ( FDF_SUCCESS == status && ok_to_delete) {
	        	CtnrMap[i_ctnr].cname[0]		= '\0';
	            CtnrMap[i_ctnr].cguid         	= 0;
	            CtnrMap[i_ctnr].sdf_container 	= containerNull;
	            CtnrMap[i_ctnr].size_kb			= 0;
	            CtnrMap[i_ctnr].current_size  	= 0;
	        } else {
	            if ( FDF_SUCCESS == status )
					status = FDF_FAILURE;

				meta.delete_in_progress = FDF_FALSE;

				if ( name_service_put_meta( pai, cguid, &meta ) != SDF_SUCCESS ) {
					plat_log_msg( 150087, LOG_CAT, LOG_ERR, "Could not clear delete in progress for container %lu\n", cguid );
				} 

	            plat_log_msg( 160030, 
							  LOG_CAT, 
							  LOG_INFO, 
							  "Container is not deleted (busy or error): cguid=%lu(%d), status=%s", 
							  cguid, 
							  i_ctnr, 
							  SDF_Status_Strings[status] );
			}
        }
    }

    plat_log_msg( 20819, 
				  LOG_CAT, 
				  LOG_INFO, 
				  "%s", 
				  SDF_Status_Strings[status] );
 out:
    SDFEndSerializeContainerOp(pai);

    return status;
}

FDF_status_t FDFGetContainers(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t             *cguids,
	uint32_t                *n_cguids
	)
{
    int   i;
    int   n_containers;

    n_containers = 0;

    for ( i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
		if ( CtnrMap[i].cguid != 0 ) {
			cguids[n_containers] = Mcd_containers[i].cguid;
            n_containers++;
        }
    }

    *n_cguids = n_containers;

    return FDF_SUCCESS;
}

FDF_status_t FDFGetContainerProps(
	struct FDF_thread_state	*fdf_thread_state, 
	FDF_cguid_t 		  	 cguid, 
	FDF_container_props_t	*pprops
	)
{   
    FDF_status_t             	 status = FDF_SUCCESS;
    SDF_container_meta_t     	 meta;
    SDF_internal_ctxt_t     	*pai 	= (SDF_internal_ctxt_t *) fdf_thread_state;

	if ( !cguid )
		return SDF_INVALID_PARAMETER;

    SDFStartSerializeContainerOp(pai);  
    if (( status = name_service_get_meta( pai, cguid, &meta )) == FDF_SUCCESS ) {
		status = fdf_create_fdf_props( &meta.properties, pprops );
    }              
    SDFEndSerializeContainerOp( pai );   
                   
    return status;
}
    
FDF_status_t FDFSetContainerProps(
	struct FDF_thread_state	*fdf_thread_state, 
	FDF_cguid_t 	 	  	 cguid,
	FDF_container_props_t	*pprops
	)
{
    FDF_status_t             status = FDF_SUCCESS;
    SDF_container_meta_t     meta;
    SDF_internal_ctxt_t     *pai = (SDF_internal_ctxt_t *) fdf_thread_state;

	if ( !cguid )
		return SDF_INVALID_PARAMETER;

    SDFStartSerializeContainerOp(pai);
    if (( status = name_service_get_meta( pai, cguid, &meta )) == FDF_SUCCESS ) {

		if ( pprops->size_kb != meta.properties.container_id.size ) {
			if ( ( status = fdf_resize_container( fdf_thread_state, cguid, pprops->size_kb ) ) != FDF_SUCCESS ) {
				plat_log_msg( 150094, LOG_CAT, LOG_ERR, "Failed to resize %lu - %s", cguid, FDFStrError( status ) );
				goto out;
			}
		}

    	meta.properties.container_id.size                     = pprops->size_kb;
#if 0
		// We currently support persistent, store containers
    	meta.properties.container_type.caching_container      = pprops->evicting;
    	meta.properties.cache.writethru                       = pprops->writethru;
#else
    	meta.properties.container_type.caching_container      = SDF_FALSE;
    	meta.properties.cache.writethru                       = SDF_TRUE;
		meta.properties.fifo_mode 							  = SDF_FALSE;
		meta.properties.shard.num_shards 					  = 1;
		/* TRAC:10469
           Disabling the following line because cguid can never be changed and if allowed
           apps can set invalid value for cguid, GetContainerProperties return invalid cguids
        */
        /*meta.properties.cguid 							  = pprops->cguid;*/
#endif

		meta.properties.durability_level = SDF_NO_DURABILITY;
		if ( pprops->durability_level == FDF_DURABILITY_HW_CRASH_SAFE )
	    	meta.properties.durability_level = SDF_FULL_DURABILITY;
		else if ( pprops->durability_level == FDF_DURABILITY_SW_CRASH_SAFE )
	    	meta.properties.durability_level = SDF_RELAXED_DURABILITY;

        status = name_service_put_meta( pai, cguid, &meta );
    }

 out:
    SDFEndSerializeContainerOp( pai );

    return status;
}

FDF_status_t FDFReadObject(
	struct FDF_thread_state   *fdf_thread_state,
	FDF_cguid_t                cguid,
	char                      *key,
	uint32_t                   keylen,
	char                     **data,
	uint64_t                  *datalen
	)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    if ( !cguid )
        return FDF_INVALID_PARAMETER;

    if ( !fdf_is_ctnr_open( cguid) ) {
        plat_log_msg( 160039, LOG_CAT, LOG_ERR, "Container must be open to execute a read object" );
        return FDF_FAILURE_CONTAINER_NOT_OPEN;
    }

    pac = (SDF_action_init_t *) fdf_thread_state;
   
    ar.reqtype = APGRX;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    if (data == NULL) {
        return(FDF_BAD_PBUF_POINTER);
    }
    ar.ppbuf_in = (void **)data;

    ActionProtocolAgentNew(pac, &ar);

    if (datalen == NULL) {
        return(FDF_BAD_SIZE_POINTER);
    }
    *datalen = ar.destLen;

    return(ar.respStatus);
}

FDF_status_t FDFReadObjectExpiry(
    struct FDF_thread_state  *fdf_thread_state,
    FDF_cguid_t               cguid,
    FDF_readobject_t         *robj
    )
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    if ( !cguid )
        return FDF_INVALID_PARAMETER;

    if ((NULL == fdf_thread_state) || (NULL == robj) || (NULL == robj->key)) {
        return FDF_INVALID_PARAMETER;        
    }

    if ( !fdf_is_ctnr_open( cguid) ) {
        plat_log_msg( 160039, LOG_CAT, LOG_ERR, "Container must be open to execute a read object" );
        return FDF_FAILURE_CONTAINER_NOT_OPEN;
    }

    pac = (SDF_action_init_t *) fdf_thread_state;
   
    ar.reqtype = APGRX;
    ar.curtime = robj->current;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) robj->key, robj->key_len)) != SDF_SUCCESS) {
        return(status);
    }
 
    ar.ppbuf_in = (void **)(&(robj->data));

    ActionProtocolAgentNew(pac, &ar);

    robj->data_len = ar.destLen;
    robj->expiry = ar.exptime;

    return(ar.respStatus);
}

FDF_status_t FDFFreeBuffer(
	char *buf
	)
{
    plat_free( buf );
    return FDF_SUCCESS;
}

FDF_status_t FDFWriteObject(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t          cguid,
	char                *key,
	uint32_t             keylen,
	char                *data,
	uint64_t             datalen,
	uint32_t             flags
	)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac		= NULL;
    FDF_status_t        status	= FDF_FAILURE;
	int					index 	= -1;

 	if ( !cguid )
 		return FDF_INVALID_PARAMETER;
 
    if ( !fdf_is_ctnr_open( cguid) ) {
    	plat_log_msg( 160040, LOG_CAT, LOG_ERR, "Container must be open to execute a write object" );
        return FDF_FAILURE_CONTAINER_NOT_OPEN;
	}

	// NOTE: FDF tests don't like this
	index = fdf_get_ctnr_from_cguid( cguid );
	if ( 0 && index > -1 ) {
		if ( CtnrMap[ index ].current_size + keylen + datalen > CtnrMap[ index ].size_kb * 1024 ) {
    		plat_log_msg( 150077, LOG_CAT, LOG_ERR, "Container full" );
			return FDF_FLASH_ENOSPC;
		}
	}
		
    pac = (SDF_action_init_t *) fdf_thread_state;

	if ( flags & FDF_WRITE_MUST_EXIST ) {
    	ar.reqtype = APPAE;
	} else if( flags & FDF_WRITE_MUST_NOT_EXIST ) {
    	ar.reqtype = APCOE;
	}
    else {
    	ar.reqtype = APSOE;
    }
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != FDF_SUCCESS) {
        return(status);
    }
    ar.sze = datalen;
    ar.pbuf_out = (void *) data;
    ar.exptime = 0;
    if (data == NULL) {
        return(FDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);

	if ( SDF_SUCCESS == ar.respStatus ) {
		index = fdf_get_ctnr_from_cguid( cguid );
		if ( index > -1 ) {
			CtnrMap[ index ].current_size += keylen + datalen;
		}
	}
	
    return ar.respStatus;
}

FDF_status_t FDFWriteObjectExpiry(
    struct FDF_thread_state  *fdf_thread_state,
    FDF_cguid_t               cguid,
    FDF_writeobject_t        *wobj,
    uint32_t                  flags
    )
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    FDF_status_t        status;

    if ( !cguid )
        return FDF_INVALID_PARAMETER;
 
    if ((NULL == fdf_thread_state) || (NULL == wobj) || (NULL == wobj->key)) {
        return FDF_INVALID_PARAMETER;        
    }

    if (NULL == wobj->data) {
        return FDF_BAD_PBUF_POINTER;
    }

    if ( !fdf_is_ctnr_open( cguid) ) {
        plat_log_msg( 160040, LOG_CAT, LOG_ERR, "Container must be open to execute a write object" );
        return FDF_FAILURE_CONTAINER_NOT_OPEN;
    }

    pac = (SDF_action_init_t *) fdf_thread_state;

    if ( flags & FDF_WRITE_MUST_EXIST ) {
        ar.reqtype = APPAE;
    } else if( flags & FDF_WRITE_MUST_NOT_EXIST ) {
        ar.reqtype = APCOE;
    }
    else {
        ar.reqtype = APSOE;
    }
    ar.curtime = wobj->current;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *)wobj->key, wobj->key_len)) != SDF_SUCCESS) {
        return(status);
    }
    ar.sze = wobj->data_len;
    ar.pbuf_out = (void *) (wobj->data);
    ar.exptime = wobj->expiry;

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

FDF_status_t FDFDeleteObject(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t          	  cguid,
	char                	 *key,
	uint32_t             	  keylen
	)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac		= NULL;
    FDF_status_t        status	= FDF_FAILURE;

    if ( !cguid )
        return FDF_INVALID_PARAMETER;
        
    if ( !fdf_is_ctnr_open( cguid) ) {
        plat_log_msg( 160041, LOG_CAT, LOG_ERR, "Container must be open to execute a delete object" );
        return FDF_FAILURE_CONTAINER_NOT_OPEN;
    }

    pac = (SDF_action_init_t *) fdf_thread_state;

    ar.reqtype = APDBE;
    ar.prefix_delete = 0;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;    
	ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != FDF_SUCCESS) {
        return(status); 
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

FDF_status_t FDFFlushObject(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t          	  cguid,
	char                	 *key,
	uint32_t             	  keylen
	)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    FDF_status_t        status;

    pac = (SDF_action_init_t *) fdf_thread_state;
   
	if ( !cguid )
		return FDF_INVALID_PARAMETER;

    if ( !fdf_is_ctnr_open( cguid) ) {
        plat_log_msg( 160043, LOG_CAT, LOG_ERR, "Container must be open to execute a flush object" );
        return FDF_FAILURE_CONTAINER_NOT_OPEN;
    }

    ar.reqtype = APFLS;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != FDF_SUCCESS) {
        return(status);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

FDF_status_t FDFFlushContainer(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t               cguid
	)
{
    SDF_appreq_t        	 ar;
    SDF_action_init_t  		*pac;
	FDF_status_t			 status			= FDF_FAILURE;
#ifdef SDFAPIONLY
    SDF_internal_ctxt_t     *pai			= (SDF_internal_ctxt_t *) fdf_thread_state;
    struct shard            *shard          = NULL;
    flashDev_t              *flash_dev;
    SDF_container_meta_t     meta;
    struct SDF_shared_state *state          = &sdf_shared_state;
#endif
	
    if ( !cguid )
        return FDF_INVALID_PARAMETER;

    if ( !fdf_is_ctnr_open( cguid) ) {
        plat_log_msg( 160044, LOG_CAT, LOG_ERR, "Container must be open to execute a flush container" );
        return FDF_FAILURE_CONTAINER_NOT_OPEN;
    }

    pac = (SDF_action_init_t *) fdf_thread_state;

    ar.reqtype = APFCO;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();

    ActionProtocolAgentNew(pac, &ar);

	if ( SDF_SUCCESS != ar.respStatus ) 
    	return(ar.respStatus);

#ifdef SDFAPIONLY
	if ((status = name_service_get_meta(pai, cguid, &meta)) == FDF_SUCCESS) {

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

	return status;
}

FDF_status_t FDFFlushCache(
	struct FDF_thread_state  *fdf_thread_state
	)
{
	FDF_status_t	status	= FDF_FAILURE;
    int				i;

    for ( i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
        if ( CtnrMap[i].cguid != 0 ) {
			status = FDFFlushContainer( fdf_thread_state, Mcd_containers[i].cguid );

			if ( status != FDF_SUCCESS )
				return status;
        }
    }

    return FDF_SUCCESS;
}

FDF_status_t FDFContainerStat(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, int key, uint64_t *stat) 
{
    FDF_status_t   status;

    status = SDFContainerStatInternal(pai, container, key, stat);
    return(status);
}

#if 0
static void fdf_get_fth_stats(SDF_internal_ctxt_t *pai, char ** ppos, int * lenp,
                                 SDF_CONTAINER sdf_container, FDF_stats_t *stats )
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
        stats->flash_stats[FDF_FLASH_STATS_PENDING_IOS] = Mcd_num_pending_ios;

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
    FDFContainerStat( pai, sdf_container,
                             FLASH_FTH_SCHEDULER_IDLE_TIME,
                             &idle_time );
    plat_snprintfcat( ppos, lenp, "STAT fth_idle_time %lu %lu\r\n",
                      (unsigned long)idle_time,
                      (unsigned long)(idle_time - old_value) );

    old_value = num_dispatches;
    FDFContainerStat( pai, sdf_container,
                             FLASH_FTH_NUM_DISPATCHES,
                             &num_dispatches );
    plat_snprintfcat( ppos, lenp,
                      "STAT fth_num_dispatches %lu %lu\r\n",
                      num_dispatches, num_dispatches - old_value );

    old_value = dispatch_time;
    FDFContainerStat( pai, sdf_container,
                             FLASH_FTH_SCHEDULER_DISPATCH_TIME,
                             &dispatch_time );
    plat_snprintfcat( ppos, lenp, "STAT fth_dispatch_time %lu %lu\r\n",
                      dispatch_time, dispatch_time - old_value );

    old_value = num_low_prio_dispatches;
    FDFContainerStat( pai, sdf_container,
                             FLASH_FTH_NUM_LOW_PRIO_DISPATCHES,
                             &num_low_prio_dispatches );
    plat_snprintfcat( ppos, lenp,
                      "STAT fth_num_low_prio_dispatches %lu %lu\r\n",
                      num_low_prio_dispatches,
                      num_low_prio_dispatches - old_value );

    old_value = low_prio_dispatch_time;
    FDFContainerStat( pai, sdf_container,
                             FLASH_FTH_SCHEDULER_LOW_PRIO_DISPATCH_TIME,
                             &low_prio_dispatch_time );
    plat_snprintfcat( ppos, lenp,
                      "STAT fth_low_prio_dispatch_time %lu %lu\r\n",
                      low_prio_dispatch_time,
                      low_prio_dispatch_time - old_value );

    old_value = thread_time;
    FDFContainerStat( pai, sdf_container,
                             FLASH_FTH_TOTAL_THREAD_RUN_TIME,
                             &thread_time );
    plat_snprintfcat( ppos, lenp,
                      "STAT fth_thread_run_time %lu %lu\r\n",
                      thread_time, thread_time - old_value );

    old_value = ticks;
    FDFContainerStat( pai, sdf_container,
                             FLASH_TSC_TICKS_PER_MICROSECOND, &ticks);
    plat_snprintfcat( ppos, lenp,
                      "STAT fth_tsc_ticks_per_usec %lu %lu\r\n",
                      ticks, old_value );

    FDFContainerStat( pai, sdf_container,
                             FLASH_FTH_AVG_DISPATCH_NANOSEC,
                             &avg_dispatch );
    plat_snprintfcat( ppos, lenp,
                      "STAT fth_avg_dispatch_nanosec %lu\r\n",
                      avg_dispatch );
}
#endif

static void fdf_get_flash_stats( SDF_internal_ctxt_t *pai, char ** ppos, int * lenp,
                                 SDF_CONTAINER sdf_container, FDF_stats_t *stats)
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
    uint64_t            num_ext_checks = 0;
    uint64_t            num_full_buckets = 0;
    uint64_t          * stats_ptr;

    FDFContainerStat( pai, sdf_container,
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

    FDFContainerStat( pai, sdf_container,
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

    FDFContainerStat( pai, sdf_container,
                             FLASH_SPACE_ALLOCATED,
                             &space_allocated );
    plat_snprintfcat( ppos, lenp,
                      "STAT flash_space_allocated %lu\r\n",
                      space_allocated );
    if (stats != NULL) 
        stats->flash_stats[FDF_FLASH_STATS_SPACE_ALLOCATED] = space_allocated;

    FDFContainerStat( pai, sdf_container,
                             FLASH_SPACE_CONSUMED,
                             &space_consumed );
    plat_snprintfcat( ppos, lenp, "STAT flash_space_consumed %lu\r\n",
                      space_consumed );
    if (stats != NULL) 
        stats->flash_stats[FDF_FLASH_STATS_SPACE_CONSUMED] = space_consumed;

    FDFContainerStat( pai, sdf_container,
                             FLASH_NUM_OBJECTS,
                             &num_objects );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_objects %lu\r\n",
                      num_objects );
    if (stats != NULL) 
        stats->flash_stats[FDF_FLASH_STATS_NUM_OBJS] = num_objects;

    FDFContainerStat( pai, sdf_container,
                             FLASH_NUM_CREATED_OBJECTS,
                             &num_created_objects );
    plat_snprintfcat( ppos, lenp,
                      "STAT flash_num_created_objects %lu\r\n",
                      num_created_objects );
    if (stats != NULL) 
        stats->flash_stats[FDF_FLASH_STATS_NUM_CREATED_OBJS] = num_created_objects;

    FDFContainerStat( pai, sdf_container,
                             FLASH_NUM_EVICTIONS,
                             &num_evictions );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_evictions %lu\r\n",
                      num_evictions );
    if (stats != NULL) 
        stats->flash_stats[FDF_FLASH_STATS_NUM_EVICTIONS] = num_evictions;

    FDFContainerStat( pai, sdf_container,
                                 FLASH_NUM_HASH_EVICTIONS,
                                 &num_hash_evictions );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_num_hash_evictions %lu\r\n",
                          num_hash_evictions );
    if (stats != NULL) 
        stats->flash_stats[FDF_FLASH_STATS_NUM_HASH_EVICTIONS] = num_hash_evictions;

    FDFContainerStat( pai, sdf_container,
                                 FLASH_NUM_INVAL_EVICTIONS,
                                 &num_inval_evictions );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_num_inval_evictions %lu\r\n",
                          num_inval_evictions );
    if (stats != NULL) 
        stats->flash_stats[FDF_FLASH_STATS_NUM_INVAL_EVICTIONS] = num_inval_evictions;

    FDFContainerStat( pai, sdf_container,
                                 FLASH_NUM_SOFT_OVERFLOWS,
                                 &num_hash_overflows );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_num_soft_overflows %lu\r\n",
                          num_hash_overflows );
    if (stats != NULL) 
        stats->flash_stats[FDF_FLASH_STATS_NUM_SOFT_OVERFLOWS] = num_hash_overflows;

    FDFContainerStat( pai, sdf_container,
                                 FLASH_NUM_HARD_OVERFLOWS,
                                 &num_hash_overflows );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_num_hard_overflows %lu\r\n",
                          num_hash_overflows );
    if (stats != NULL) 
        stats->flash_stats[FDF_FLASH_STATS_NUM_HARD_OVERFLOWS] = num_hash_overflows;

    FDFContainerStat( pai, sdf_container,
                             FLASH_GET_HASH_COLLISIONS,
                                 &get_hash_collisions );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_get_hash_collisions %lu\r\n",
                          get_hash_collisions );
    if (stats != NULL)
        stats->flash_stats[FDF_FLASH_STATS_GET_HASH_COLLISION] = get_hash_collisions;


    FDFContainerStat( pai, sdf_container,
                                 FLASH_SET_HASH_COLLISIONS,
                                 &set_hash_collisions );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_set_hash_collisions %lu\r\n",
                          set_hash_collisions );
    if (stats != NULL)
        stats->flash_stats[FDF_FLASH_STATS_SET_HASH_COLLISION] = set_hash_collisions;

    FDFContainerStat( pai, sdf_container,
                             FLASH_NUM_OVERWRITES,
                             &num_overwrites );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_overwrites %lu\r\n",
                      num_overwrites );
    if (stats != NULL)
        stats->flash_stats[FDF_FLASH_STATS_NUM_OVERWRITES] = num_overwrites;

    FDFContainerStat( pai, sdf_container,
                             FLASH_OPS,
                             &num_ops );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_ops %lu\r\n",
                      num_ops );
    if (stats != NULL)
        stats->flash_stats[FDF_FLASH_STATS_NUM_OPS] = num_ops;

    FDFContainerStat( pai, sdf_container,
                             FLASH_READ_OPS,
                             &num_read_ops );

    plat_snprintfcat( ppos, lenp, "STAT flash_num_read_ops %lu\r\n",
                      num_read_ops );
    if (stats != NULL)
        stats->flash_stats[FDF_FLASH_STATS_NUM_READ_OPS] = num_read_ops;

    FDFContainerStat( pai, sdf_container,
                             FLASH_NUM_GET_OPS,
                             &num_get_ops );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_get_ops %lu\r\n",
                      num_get_ops );
    if (stats != NULL)
        stats->flash_stats[FDF_FLASH_STATS_NUM_GET_OPS] = num_get_ops;

    FDFContainerStat( pai, sdf_container,
                             FLASH_NUM_PUT_OPS,
                             &num_put_ops );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_put_ops %lu\r\n",
                      num_put_ops );
    if (stats != NULL)
        stats->flash_stats[FDF_FLASH_STATS_NUM_PUT_OPS] = num_put_ops;

    FDFContainerStat( pai, sdf_container,
                             FLASH_NUM_DELETE_OPS,
                             &num_del_ops );
    plat_snprintfcat( ppos, lenp, "STAT flash_num_del_ops %lu\r\n",
                      num_del_ops );
    if (stats != NULL)
        stats->flash_stats[FDF_FLASH_STATS_NUM_DEL_OPS] = num_del_ops;

    FDFContainerStat( pai, sdf_container,
                                 FLASH_NUM_EXIST_CHECKS,
                                 &num_ext_checks );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_get_exist_checks %lu\r\n",
                          num_ext_checks );
    if (stats != NULL)
        stats->flash_stats[FDF_FLASH_STATS_GET_EXIST_CHECKS] = num_ext_checks;

    FDFContainerStat( pai, sdf_container,
                                 FLASH_NUM_FULL_BUCKETS,
                                 &num_full_buckets );
    plat_snprintfcat( ppos, lenp,
                          "STAT flash_num_full_buckets %lu\r\n",
                          num_full_buckets );
    if (stats != NULL)
        stats->flash_stats[FDF_FLASH_STATS_NUM_FULL_BUCKETS] = num_full_buckets;

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
                      PLAT_LOG_LEVEL_ERROR,
                      "sdf stat %s parsing failure", name );
        return 0;
    }

    return x;
}

char *FDFGetStatsDescription(FDF_cache_stat_t stat) {
    return FDFCacheStatsDescStrings[stat];
}

SDF_status_t fdf_parse_access_stats (char * stat_buf,FDF_stats_t *stats ) {
    int i;                       
    uint64_t val;
                          
    if (stats == NULL ) { 
        return FDF_SUCCESS;
    }   
    for ( i = 0; i < FDF_N_ACCESS_TYPES; i++ ) {
        val = parse_count( stat_buf, FDFAccessTypeSearchStrings[i] );
        if ( val != 0 ) {        
            stats->n_accesses[i] = val;
        }
    }                     
    return FDF_SUCCESS;   
}   


SDF_status_t fdf_parse_cache_stats (char * stat_buf,FDF_stats_t *stats ) {
    int i;
    uint64_t val;

    if (stats == NULL ) {
        return FDF_SUCCESS;
    }
    //fprintf(stderr,"fdf_parse_cache_stats: %s\n",stat_buf);
    for ( i = 0; i < FDF_N_CACHE_STATS; i++ ) {
        val = parse_count( stat_buf, FDFCacheStatsSearchStrings[i] );
        if ( val != 0 ) {
            stats->cache_stats[i] = val;
        }
    }
    return FDF_SUCCESS;
}

SDF_status_t
fdf_parse_stats( char * stat_buf, int stat_key, uint64_t * pstat)
{
    int         i;
    uint64_t    stats[MCD_NUM_SDF_STATS];
    uint64_t    counts[MCD_NUM_SDF_COUNTS];

    if ( MCD_NUM_SDF_STATS <= stat_key ) {
#ifdef NOT_NEEDED
        plat_log_msg( 20649,
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_ERROR,
                      "invalid stat key %d", stat_key );
#endif
        return SDF_FAILURE;
    }

    for ( i = 0; i < MCD_NUM_SDF_COUNTS; i++ ) {
        counts[i] = parse_count( stat_buf, FDFCacheCountStrings[i] );
    }

    if ( ( counts[apgrd] + counts[apgrx] ) >= counts[ahgtr] ) {
        stats[FDF_DRAM_CACHE_HITS] =
            counts[apgrd] + counts[apgrx] - counts[ahgtr];
    }
    else {
        plat_log_msg( 20650,
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_ERROR,
                      "invalid sdf cache stats, rd=%lu rx=%lu tr=%lu",
                      counts[apgrd], counts[apgrx], counts[ahgtr] );
        stats[FDF_DRAM_CACHE_HITS] = 0;
    }
    stats[FDF_DRAM_CACHE_MISSES]   = counts[ahgtr];
    stats[FDF_FLASH_CACHE_HITS]    = counts[hagrc];
    stats[FDF_FLASH_CACHE_MISSES]  = counts[hagrf];
    stats[FDF_DRAM_CACHE_CASTOUTS] = counts[ahcwd];
    stats[FDF_DRAM_N_OVERWRITES]   = counts[owrites_s] + counts[owrites_m];
    stats[FDF_DRAM_N_IN_PLACE_OVERWRITES] = counts[ipowr_s] + counts[ipowr_m];
    stats[FDF_DRAM_N_NEW_ENTRY]    = counts[newents];

    *pstat = stats[stat_key];

    return FDF_SUCCESS;
}


static void get_fdf_stats( SDF_internal_ctxt_t *pai, char ** ppos, int * lenp,
                               SDF_CONTAINER sdf_container, FDF_stats_t *stat)
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

    fdf_parse_stats( buf, FDF_DRAM_CACHE_HITS, &sdf_cache_hits );
    fdf_parse_stats( buf, FDF_DRAM_CACHE_MISSES, &sdf_cache_misses );
    fdf_parse_stats( buf, FDF_FLASH_CACHE_HITS, &sdf_flash_hits );
    fdf_parse_stats( buf, FDF_FLASH_CACHE_MISSES, &sdf_flash_misses );
    fdf_parse_stats( buf, FDF_DRAM_CACHE_CASTOUTS, &sdf_cache_evictions );
    fdf_parse_stats( buf, FDF_DRAM_N_OVERWRITES, &sdf_n_overwrites );
    fdf_parse_stats( buf, FDF_DRAM_N_IN_PLACE_OVERWRITES, &sdf_n_in_place_overwrites );
    fdf_parse_stats( buf, FDF_DRAM_N_NEW_ENTRY, &sdf_n_new_entry );

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

    FDFContainerStat( pai, sdf_container,
                             SDF_N_ONLY_IN_CACHE, &n);
    plat_snprintfcat( ppos, lenp, "STAT sdf_cache_only_items %lu\r\n", n);

    plat_snprintfcat( ppos, lenp, "STAT %s", buf );
    fdf_parse_access_stats(buf,stat);
    fdf_parse_cache_stats(buf,stat);

	/*
     * get stats for entire cache (for all cguid's)
     */
    memset( buf, 0, BUF_LEN );
    action_stats(pai, buf, BUF_LEN );
    plat_snprintfcat( ppos, lenp, "STAT %s\r\n", buf );

    // memset( buf, 0, 1024 );
    // home_stats( buf, 1024 );
    // plat_snprintfcat( ppos, lenp, "STAT %s\r\n", buf );
}
static void get_proc_stats( char ** ppos, int * lenp )
{
    int             rc;
    int             proc_fd = 0;
    char            proc_buf[128];
    char          * ptr = NULL;

    proc_fd = open( "/proc/stat", O_RDONLY );
    if ( 0 > proc_fd ) {
        mcd_log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_ERROR,
                      "cannot open /proc/stat" );
        return;
    }
    mcd_log_msg( 20694, PLAT_LOG_LEVEL_DEBUG,
                 "/proc/stat opened, fd=%d", proc_fd );

    memset( proc_buf, 0, 128 );
    if ( 0 < ( rc = read( proc_fd, proc_buf, 127 ) ) ) {
        if ( NULL != ( ptr = strchr( proc_buf, '\n' ) ) ) {
            *ptr = '\0';
            plat_snprintfcat( ppos, lenp, "STAT %s\r\n", proc_buf );
        }
    }
    else {
        mcd_log_msg( 20695, PLAT_LOG_LEVEL_ERROR,
                     "failed to read from proc, rc=%d errno=%d", rc, errno );
    }

    close( proc_fd );
}

FDF_status_t FDFGetStatsStr (
    struct FDF_thread_state *fdf_thread_state,
    FDF_cguid_t 			 cguid,
	char 					*stats_str,
    FDF_stats_t *stats
	) 
{
    FDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER sdf_container = containerNull;
    SDF_internal_ctxt_t *pai = (SDF_internal_ctxt_t *) fdf_thread_state;
    FDF_container_props_t   pprops;
    SDF_container_meta_t    meta;
    time_t t;
    //SDF_status_t lock_status = FDF_SUCCESS;
    //SDF_cguid_t parent_cguid = SDF_NULL_CGUID;
    //int log_level = LOG_ERR;
    int  i_ctnr;
    uint64_t space_used = 0;
    uint64_t maxbytes = 0;
    uint64_t num_evictions = 0;
    uint64_t      n = 0;
    //static struct timeval reset_time = {0, 0} ;
    int buf_len;
    char *temp;
    char * pos;
    FDF_container_props_t       dummy_prop;
    memset( (void *)&dummy_prop, 0, sizeof(dummy_prop) );

	if(!cguid)
		return SDF_INVALID_PARAMETER;

    SDFStartSerializeContainerOp(pai);

    //fprintf(stderr,"Container CGID:%u\n",cguid);
    i_ctnr = fdf_get_ctnr_from_cguid(cguid);
    if (i_ctnr == -1) {
        status = SDF_INVALID_PARAMETER;
        goto out;
    }
    else {
        sdf_container = CtnrMap[i_ctnr].sdf_container;
    }

    if (( status = name_service_get_meta( pai, cguid, &meta )) == FDF_SUCCESS ) {
        status = fdf_create_fdf_props( &meta.properties, &pprops );
    }

    buf_len = STAT_BUFFER_SIZE;
    temp = stats_str;
    memset( temp, 0, buf_len );
    pos = temp;
    buf_len -= strlen( "\r\nEND\r\n" ) + 1;
    time(&t);
    plat_snprintfcat( &pos, &buf_len, "STAT Time %s\r\n", ctime(&t) );    
    if ( status == FDF_SUCCESS ) {
        //Add container properties to the list
        plat_snprintfcat( &pos, &buf_len, "STAT Container Name %s size:%lukb FIFO:%d persistence:%d eviction:%d writethru:%d\r\n", 
                                           CtnrMap[i_ctnr].cname, pprops.size_kb, /*pprops.fifo_mode*/ 0, pprops.persistent, pprops.evicting, pprops.writethru );
    }
    else {
        plat_snprintfcat( &pos, &buf_len, "STAT Container Name %s\r\n", CtnrMap[i_ctnr].cname );
    }
    plat_snprintfcat( &pos, &buf_len, "STAT CGUID %lu\r\n", cguid );
    FDFContainerStat( pai, sdf_container, SDF_N_CURR_ITEMS, &n );
    plat_snprintfcat( &pos, &buf_len, "STAT curr_items %lu\r\n", n );
    FDFContainerStat( pai, sdf_container,
                             FLASH_SPACE_USED, &space_used );
    plat_snprintfcat( &pos, &buf_len, "STAT bytes %lu\r\n", space_used );
    FDFContainerStat( pai, sdf_container,
                                         FLASH_NUM_EVICTIONS, &num_evictions );
    plat_snprintfcat( &pos, &buf_len, "STAT evictions %lu\r\n",
                              num_evictions );
    FDFContainerStat( pai, sdf_container,
                                         FLASH_SHARD_MAXBYTES, &maxbytes );
    plat_snprintfcat( &pos, &buf_len, "STAT limit_maxbytes %lu\r\n",
                              maxbytes );
    fdf_get_flash_stats( pai, &pos, &buf_len, sdf_container,stats);
    #if 0
    fdf_get_fth_stats( pai, &pos, &buf_len, sdf_container,stats);
    #endif
    if (stats != NULL) {
        stats->flash_stats[FDF_FLASH_STATS_PENDING_IOS] = Mcd_num_pending_ios;
    }
    get_fdf_stats(pai, &pos, &buf_len, sdf_container,stats);
    get_proc_stats(&pos, &buf_len);
out:

    //plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);
    SDFEndSerializeContainerOp(pai);

    return (status);
}

FDF_status_t FDFGetStats(
	struct FDF_thread_state *fdf_thread_state,
	FDF_stats_t             *stats
	)
{
    char buf[BUF_LEN];
    char *ptr;
    if ( stats == NULL ) {
        return FDF_SUCCESS;
    }
    memset (stats, 0, sizeof(FDF_stats_t));
    SDF_internal_ctxt_t *pai = (SDF_internal_ctxt_t *)fdf_thread_state;
    action_stats(pai, buf, BUF_LEN );
    ptr = strstr(buf,"<CACHE>"); 
    fdf_parse_access_stats(ptr,stats);
    fdf_parse_cache_stats(ptr,stats);
    return( FDF_SUCCESS );
}

FDF_status_t FDFGetContainerStats(
	struct FDF_thread_state   	*fdf_thread_state,
	FDF_cguid_t                	 cguid,
	FDF_stats_t					*stats
	)
{
    char stats_str[STAT_BUFFER_SIZE];
    if ( stats == NULL ) {
        return FDF_SUCCESS;
    }
    memset (stats, 0, sizeof(FDF_stats_t));
    FDFGetStatsStr(fdf_thread_state,cguid,stats_str, stats);
    //  no-op in this simple implementation
    return( FDF_SUCCESS );
}

FDF_status_t FDFBackupContainer(
	struct FDF_thread_state   *fdf_thread_state,
	FDF_cguid_t                cguid,
	char                      *backup_directory
	)
{
    //  no-op in this simple implementation
    return( FDF_SUCCESS );
}

FDF_status_t FDFRestoreContainer(
	struct FDF_thread_state   *fdf_thread_state,
	FDF_cguid_t                cguid,
	char                      *backup_directory
	)
{
    //  no-op in this simple implementation
    return( FDF_SUCCESS );
}

// Internal functions
#if 1
static SDF_container_props_t *fdf_create_sdf_props(
    FDF_container_props_t           *fdf_properties
    )
{
    SDF_container_props_t   *sdf_properties = (SDF_container_props_t *) plat_alloc ( sizeof ( SDF_container_props_t ) );

    if ( NULL != sdf_properties ) {
        sdf_properties->container_id.owner                    	= 0;
        sdf_properties->container_id.size                     	= fdf_properties->size_kb;
        sdf_properties->container_id.container_id             	= 0;
        sdf_properties->container_id.num_objs                 	= (fdf_properties->size_kb * 1024 / 512);

        sdf_properties->cguid                                 	= fdf_properties->cguid;

        sdf_properties->container_type.type                   	= SDF_OBJECT_CONTAINER;
        sdf_properties->container_type.persistence            	= SDF_TRUE /* fdf_properties->persistent */;
        sdf_properties->container_type.caching_container      	= SDF_FALSE /* fdf_properties->evicting */;
        sdf_properties->container_type.async_writes           	= fdf_properties->async_writes;

        sdf_properties->replication.enabled                     = 0;
        sdf_properties->replication.type                        = SDF_REPLICATION_NONE;
        sdf_properties->replication.num_replicas                = 1;
        sdf_properties->replication.num_meta_replicas           = 0;
        sdf_properties->replication.synchronous                 = 1;

        sdf_properties->cache.not_cacheable                     = SDF_FALSE;
        sdf_properties->cache.shared                            = SDF_FALSE;
        sdf_properties->cache.coherent                          = SDF_FALSE;
        sdf_properties->cache.enabled                           = SDF_TRUE;
        sdf_properties->cache.writethru                         = SDF_TRUE /* fdf_properties->writethru */;
        sdf_properties->cache.size                              = 0;
        sdf_properties->cache.max_size                          = 0;

        sdf_properties->shard.enabled                           = SDF_TRUE;
        sdf_properties->shard.num_shards                        = 1 /* fdf_properties->num_shards */;
        sdf_properties->fifo_mode                               = SDF_FALSE /* fdf_properties->fifo_mode */;

        sdf_properties->durability_level                        = SDF_NO_DURABILITY;

    	if ( fdf_properties->durability_level == FDF_DURABILITY_HW_CRASH_SAFE )
			sdf_properties->durability_level = SDF_FULL_DURABILITY;
    	else if ( fdf_properties->durability_level == FDF_DURABILITY_SW_CRASH_SAFE )
        	sdf_properties->durability_level = SDF_RELAXED_DURABILITY;
    }

    return sdf_properties;
}
#else
static SDF_container_props_t *fdf_create_sdf_props(
    FDF_container_props_t           *fdf_properties,
	FDF_internal_container_props_t	*fdf_internal_properties
    )
{
    SDF_container_props_t   *sdf_properties = (SDF_container_props_t *) plat_alloc ( sizeof ( SDF_container_props_t ) );

    if ( NULL != sdf_properties ) {
        sdf_properties->container_id.owner                      = 0;
        sdf_properties->container_id.size                       = fdf_properties->size_kb;
        sdf_properties->container_id.container_id               = 0 /* fdf_internal_properties->cid */;
        sdf_properties->container_id.owner                      = 0;
        sdf_properties->container_id.num_objs                   = (fdf_properties->size_kb * 1024 / 512);

        sdf_properties->cguid                                   = fdf_internal_properties->cguid;

        sdf_properties->container_type.type                     = SDF_OBJECT_CONTAINER;
        sdf_properties->container_type.persistence              = fdf_properties->persistent;
        sdf_properties->container_type.caching_container        = fdf_properties->evicting;
        sdf_properties->container_type.async_writes             = fdf_internal_properties->async_writes;

        sdf_properties->replication.enabled                     = 0;
        sdf_properties->replication.type                        = SDF_REPLICATION_NONE;
        sdf_properties->replication.num_replicas                = 1;
        sdf_properties->replication.num_meta_replicas           = 0;
        sdf_properties->replication.synchronous                 = 1;

        sdf_properties->cache.not_cacheable                     = SDF_FALSE;
        sdf_properties->cache.shared                            = SDF_FALSE;
        sdf_properties->cache.coherent                          = SDF_FALSE;
        sdf_properties->cache.enabled                           = SDF_TRUE;
        sdf_properties->cache.writethru                         = fdf_properties->writethru;
        sdf_properties->cache.size                              = 0;
        sdf_properties->cache.max_size                          = 0;

        sdf_properties->shard.enabled                           = SDF_TRUE;
        sdf_properties->shard.num_shards                        = fdf_internal_properties->num_shards;
        sdf_properties->fifo_mode                               = fdf_internal_properties->fifo_mode;

        sdf_properties->durability_level                        = SDF_NO_DURABILITY;

        if ( fdf_properties->durability_level == FDF_DURABILITY_HW_CRASH_SAFE )
            sdf_properties->durability_level = SDF_FULL_DURABILITY;
        else if ( fdf_properties->durability_level == FDF_DURABILITY_SW_CRASH_SAFE )
            sdf_properties->durability_level = SDF_RELAXED_DURABILITY;
    }

    return sdf_properties;
}
#endif


static FDF_status_t fdf_create_fdf_props(
    SDF_container_props_t   *sdf_properties,
    FDF_container_props_t   *fdf_properties
    )
{
	FDF_status_t	status	= FDF_INVALID_PARAMETER;

    if ( NULL != fdf_properties ) {
        fdf_properties->size_kb 							= sdf_properties->container_id.size;
        fdf_properties->fifo_mode							= sdf_properties->fifo_mode;
        fdf_properties->persistent							= sdf_properties->container_type.persistence;
        fdf_properties->evicting							= sdf_properties->container_type.caching_container;
        fdf_properties->writethru							= sdf_properties->cache.writethru;
        fdf_properties->async_writes						= sdf_properties->container_type.async_writes;
        fdf_properties->cguid       						= sdf_properties->cguid;
        fdf_properties->cid       							= sdf_properties->container_id.container_id;
        fdf_properties->num_shards							= sdf_properties->shard.num_shards;

		fdf_properties->durability_level = FDF_DURABILITY_PERIODIC;
		if ( sdf_properties->durability_level == SDF_FULL_DURABILITY )
	    	fdf_properties->durability_level = FDF_DURABILITY_HW_CRASH_SAFE;
		else if ( sdf_properties->durability_level == SDF_RELAXED_DURABILITY )
	    	fdf_properties->durability_level = FDF_DURABILITY_SW_CRASH_SAFE;

		status												= FDF_SUCCESS;
    }

    return status;
}

FDF_status_t fdf_resize_container(
    struct FDF_thread_state *fdf_thread_state,
    FDF_cguid_t              cguid,
    uint64_t                 size
    )
{   
    FDF_status_t                 status     = FDF_FAILURE;
    int                          index      = -1;
    SDF_internal_ctxt_t         *pai        = (SDF_internal_ctxt_t *) fdf_thread_state;
    SDF_container_meta_t         meta;
        
    // Cannot change physical container size
    if ( CMC_CGUID == cguid || VMC_CGUID == cguid || VDC_CGUID == cguid ) {
        plat_log_msg( 150078, LOG_CAT, LOG_ERR, "Cannnot change container size" );
        return FDF_CANNOT_REDUCE_CONTAINER_SIZE;
    }
    
    index = fdf_get_ctnr_from_cguid( cguid );
    if ( index > -1 ) {
        plat_log_msg( 150082, LOG_CAT, LOG_ERR, "Cannnot find container id %lu", cguid );
        status = FDF_CONTAINER_UNKNOWN;
// If this code is executed, will likely segfault - Johann
#if 0
    } else if ( size < CtnrMap[ index ].size_kb ) {
            plat_log_msg( 150079, LOG_CAT, LOG_ERR, "Cannnot reduce container size" );
            status = FDF_CANNOT_REDUCE_CONTAINER_SIZE;
#endif
    } else if ( ( status = name_service_get_meta( pai,
                                               	  cguid,
                                               	  &meta ) ) != FDF_SUCCESS) {
            plat_log_msg( 150080, LOG_CAT, LOG_ERR, "Cannnot read container metadata for %lu", cguid );
    } else {

        meta.properties.container_id.size = size;

        if ( ( status = name_service_put_meta( pai,
                                               cguid,
                                               &meta ) ) != FDF_SUCCESS ) {
            plat_log_msg( 150081, LOG_CAT, LOG_ERR, "Cannnot write container metadata for %lu", cguid );
        } else {
// If this code is executed, will likely segfault - Johann
#if 0
            CtnrMap[ index ].size_kb = size;
#endif
        }
    }

    return FDF_SUCCESS;
}

static void *fdf_vc_thread(
	void *arg
	) 
{
    struct FDF_thread_state *fdf_thread_state		= NULL;
    FDF_status_t             status         		= FDF_FAILURE;
    struct FDF_iterator     *_fdf_iterator  		= NULL;
    int                      i              		= 1000;
    int                      j              		= 0;
    char                    *key            		= NULL;
    uint32_t                 keylen         		= 0;
    uint64_t                 datalen        		= 0;
    SDF_container_meta_t    *meta           		= NULL;
	struct SDF_shared_state *state 					= &sdf_shared_state;
	int						 flags					= FDF_CTNR_CREATE;


    if ( FDF_SUCCESS != FDFInitPerThreadState( ( struct FDF_state * ) arg, 
											   ( struct FDF_thread_state ** ) &fdf_thread_state ) ) 
	{
        plat_log_msg( 150088,
					  LOG_CAT,
					  LOG_ERR,
					  "Unable to initialize FDF thread state, exiting" );
        return NULL;
    }

    if ( state->config.system_recovery == SYS_FLASH_RECOVERY )
        flags = 0;  // Just open the VMC/VDC
    if ( ( status = fdf_vc_init( fdf_thread_state, flags ) ) != FDF_SUCCESS ) {
            plat_log_msg( 150076,
                          LOG_CAT, LOG_ERR,
                          "Failed to open support containers: %s", FDFStrError( status ) );
        return NULL;
    }

    do{
        status = FDFEnumerateContainerObjects( fdf_thread_state, 
											   VMC_CGUID,  
											   &_fdf_iterator
											 );
    } while ( status == FDF_FLASH_EBUSY && i-- );

    while ( ( status = FDFNextEnumeratedObject ( fdf_thread_state,
                                              	 _fdf_iterator,
                                              	 &key,
                                              	 &keylen,
                                              	 (char **) &meta,
                                              	 &datalen
                                               ) ) == FDF_SUCCESS ) {
    
        for ( j = 0; j < MCD_MAX_NUM_CNTRS; j++ ) {
            if (CtnrMap[j].cguid == 0) {
                // this is an unused map entry
fprintf(stderr, ">>>fdf_vc_thread: recovering %s - %lu\n", meta->cname, meta->cguid);
				memcpy( CtnrMap[j].cname, meta->cname, strlen( meta->cname ) );
                CtnrMap[j].cguid                = meta->cguid;
                CtnrMap[j].sdf_container        = containerNull;
                Mcd_containers[j].cguid         = meta->cguid;
                Mcd_containers[j].container_id  = meta->properties.container_id.container_id;
                //strcpy( Mcd_containers[j].cname, meta->cname );
				memcpy( Mcd_containers[j].cname, meta->cname, strlen( meta->cname ) );

				name_service_create_cguid_map( fdf_thread_state,
											   meta->cname,
											   meta->cguid
											 );
                break;
            }
        }
    }
    
    status = FDFFinishEnumeration( fdf_thread_state, 
								   _fdf_iterator
								 );

	return NULL;
}

static void 
fdf_start_vc_thread(
	struct FDF_state *fdf_state
	) 
{
    pthread_t 	thd;
    int 		rc		= -1;

    rc = pthread_create( &thd,
						 NULL,
						 fdf_vc_thread,
						 (void *) fdf_state
						);

    if ( rc != 0 ) {
        plat_log_msg( 150089,
					  LOG_CAT,
					  LOG_ERR,
					  "Unable to start the virtual container initialization thread.");
    }

	pthread_join( thd, NULL );
}

static FDF_status_t
fdf_vc_init(
	struct FDF_thread_state  *fdf_thread_state,
    int                  	  flags
    )
{
    FDF_container_props_t    p;
    FDF_cguid_t              cguid       = FDF_NULL_CGUID;
    SDF_status_t             status      = FDF_FAILURE;
    SDF_internal_ctxt_t		*pai		= (SDF_internal_ctxt_t *) fdf_thread_state;

    // Create the VMC
    p.persistent            = FDF_TRUE;
    p.evicting              = FDF_TRUE;
    p.writethru             = FDF_TRUE;
    p.durability_level      = FDF_DURABILITY_HW_CRASH_SAFE;
    p.size_kb               = 1024 * 1024; // kB

    if ((status = FDFOpenPhysicalContainer(pai, VMC_PATH, &p, flags, &cguid)) != SDF_SUCCESS) {
        plat_log_msg(150057, LOG_CAT, LOG_ERR, "Failed to create VMC container - %s\n", SDF_Status_Strings[status]);
        return status;
    }

    // Create the VDC
    p.persistent            = FDF_TRUE;
    p.evicting              = FDF_FALSE;
    p.writethru             = FDF_TRUE;
    p.durability_level      = FDF_DURABILITY_HW_CRASH_SAFE;
    p.size_kb               = getProperty_Int("FDF_FLASH_SIZE", 2) * 1024 * 1024 - (2 * 1024 * 1024) - (32 * 1024); // Minus CMC/VMC allocation

    if ((status = FDFOpenPhysicalContainer(pai, VDC_PATH, &p, flags, &cguid)) != SDF_SUCCESS) {
        plat_log_msg(150057, LOG_CAT, LOG_ERR, "Failed to create VMC container - %s\n", SDF_Status_Strings[status]);
        return status;
    }

    plat_log_msg(20168, LOG_CAT, LOG_INFO, "%s\n", SDF_Status_Strings[status]);

    return status;
}

// Generate a unique container id. Check the metadata map for existing id allocation.
// Note that this function returns a 16bit value, where the cguid is actually 64bit.
// We need to change to a 16bit cguid eventually.
static FDF_status_t fdf_generate_cguid(
	struct FDF_thread_state *fdf_thread_state,
	FDF_cguid_t				*cguid
	)
{
	FDF_status_t				 status	= FDF_OBJECT_EXISTS;
    struct SDF_shared_state		*state	= &sdf_shared_state;
    SDF_internal_ctxt_t 		*pai 	= (SDF_internal_ctxt_t *) fdf_thread_state;
	uint16_t		 			 i		= 0;
	uint16_t		 			 start	= 0;

	start = state->config.cguid_counter;

	// We allow the counter to rollover as we check for duplicates
	for ( i = start + 1; i != start; i++ ) {
		state->config.cguid_counter += 1; 
		if ( state->config.cguid_counter == 0 )
			state->config.cguid_counter += 1; 
		if ( ( status = name_service_cguid_exists( pai, state->config.cguid_counter ) ) == FDF_OBJECT_UNKNOWN ) {
			*cguid = state->config.cguid_counter;
			status = FDF_SUCCESS;
			break;
		}
	}

	plat_log_msg( 150083, LOG_CAT, LOG_INFO, "%lu - %s\n", *cguid, FDFStrError( status ) );

	return status;
}


static FDF_status_t fdf_delete_objects(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t 			  cguid
	)
{
    FDF_status_t             status                 = FDF_FAILURE;
    FDF_status_t             del_status             = FDF_FAILURE;
    struct FDF_iterator     *_fdf_iterator          = NULL;
    int                      i                      = 1000;
    char                    *key            		= NULL;
    uint32_t                 keylen         		= 0;
    uint64_t                 datalen        		= 0;
    char                    *data           		= NULL;

    do{
        status = FDFEnumerateContainerObjects( fdf_thread_state,
                                               cguid,
                                               &_fdf_iterator
                                             );
    } while ( status == FDF_FLASH_EBUSY && i-- );

    while ( ( status = FDFNextEnumeratedObject ( fdf_thread_state,
                                                 _fdf_iterator,
                                                 &key,
                                                 &keylen,
                                                 &data,
                                                 &datalen
                                               ) ) == FDF_SUCCESS ) {

		if ( ( del_status = FDFDeleteObject( fdf_thread_state, cguid, key, keylen ) ) != FDF_SUCCESS ) { 
			plat_log_msg( 150095,
						  LOG_CAT, 
						  LOG_ERR, 
						  "Failed to delete object while deleting container: %s", 
						  FDFStrError( del_status ) ); 
			break;
        }
    }

    if ( ( status = FDFFinishEnumeration( fdf_thread_state,
                          		  		  _fdf_iterator ) ) != FDF_SUCCESS ) {

		plat_log_msg( 150096,
				  	  LOG_CAT, 
				  	  LOG_ERR, 
				  	  "Failed to end enumeration while deleting container: %s", 
				  	  FDFStrError( del_status ) ); 
	}

	if ( FDF_SUCCESS != del_status )
		status = del_status;

	return status;
}

/* Mini transactions handling */

static uint64_t next_trx_id = 0;
uint64_t stat_trx_active_count = 0;

static __thread trx_t trx = {0};

void fdf_trx_print_stats(FILE *log)
{
    char buf[BUF_LEN];

	sprintf(buf, "trx_active_count %ld\ntrx_id_counter %ld\n",
		stat_trx_active_count, next_trx_id);

	fputs(buf, log);
}

trx_t* fdf_trx_get()
{
	return &trx;
}

bool	mcd_trx_start( ),
	mcd_trx_commit( );

/**
 * @brief Start mini transaction
 *
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
 * @return FDF_SUCCESS on success
 *         FDF_FAILURE_ALREADY_IN_TRANS if thread has active transaction already
 */
FDF_status_t FDFMiniTransactionStart(
	struct FDF_thread_state	*fdf_thread_state
	)
{
	if(trx.id)
		return FDF_FAILURE_ALREADY_IN_TRANS;

	trx.id = __sync_add_and_fetch(&next_trx_id, 1);

	__sync_add_and_fetch(&stat_trx_active_count, 1);

	return (mcd_trx_start( )? FDF_SUCCESS: FDF_OUT_OF_MEM);
}

/**
 * @brief Commit mini transaction
 *
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
 * @return FDF_SUCCESS on success
 *         FDF_FAILURE_NO_TRANS if there is no active transaction in the current thread
 */
FDF_status_t FDFMiniTransactionCommit(
	struct FDF_thread_state	*fdf_thread_state
	)
{
	FDF_status_t status;

	if(!trx.id)
		return FDF_FAILURE_NO_TRANS;

	status = mcd_trx_commit( )? FDF_SUCCESS: FDF_TRANS_ABORTED;

	__sync_sub_and_fetch(&stat_trx_active_count, 1);

	trx.id = 0;

	return status;
}

