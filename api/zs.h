//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/*
 * File:   zs.h
 * Author: Darryl Ouye
 *
 * Created on August 22, 2012
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 */
#ifndef __ZS_H
#define __ZS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>

#include "common/zstypes.h"

#define zs_cntr_drain_io( v )  while ( unlikely( v > 0 ) ) fthYield(0)

#define ZS_API_VERSION                 2

#define CONTAINER_NAME_MAXLEN		64
#define ZS_DEFAULT_CONTAINER_SIZE_KB	(2 * 1024 * 1024)	//2GB
#define ZS_MIN_FLASH_SIZE		6		//3GB

typedef enum {
	ZS_CHECK_FIX_BOJ_CNT = 1,
} ZS_check_flags_t;

typedef struct {
	uint64_t		n;
	uint64_t		min;
	uint64_t		max;
	double			avg;
	double			geo;
	double			std;
	uint64_t		counts[64];
} ZS_histo_t;

typedef enum {
	ZS_DURABILITY_PERIODIC = 0,
	ZS_DURABILITY_SW_CRASH_SAFE    = 0x1,
	ZS_DURABILITY_HW_CRASH_SAFE    = 0x2,
} ZS_durability_level_t;

typedef enum {
	ZS_HASH_CTNR	= 0x01,
	ZS_LOG_CTNR		= 0x02,
	ZS_UNUSED_PROP	= 0xffffffffffffffff,
} ZS_prop_flag_t;

#define ZS_BTREE_CTNR	(~(ZS_HASH_CTNR | ZS_LOG_CTNR))
typedef enum {
    ZS_ACCESS_TYPES_APCOE,
    ZS_ACCESS_TYPES_APCOP,
    ZS_ACCESS_TYPES_APPAE,
    ZS_ACCESS_TYPES_APPTA,
    ZS_ACCESS_TYPES_APSOE,
    ZS_ACCESS_TYPES_APSOB,
    ZS_ACCESS_TYPES_APGRX,
    ZS_ACCESS_TYPES_APGRD,
    ZS_ACCESS_TYPES_APDBE,
    ZS_ACCESS_TYPES_APDOB,
    ZS_ACCESS_TYPES_APFLS,
    ZS_ACCESS_TYPES_APFLI,
    ZS_ACCESS_TYPES_APINV,
    ZS_ACCESS_TYPES_APSYC,
    ZS_ACCESS_TYPES_APICD,
    ZS_ACCESS_TYPES_APGIT,
    ZS_ACCESS_TYPES_APFCO,
    ZS_ACCESS_TYPES_APFCI,
    ZS_ACCESS_TYPES_APICO,
    ZS_ACCESS_TYPES_APRIV,
    ZS_ACCESS_TYPES_APRUP,
    ZS_ACCESS_TYPES_ENUM_TOTAL,
    ZS_ACCESS_TYPES_ENUM_ACTIVE,
    ZS_ACCESS_TYPES_ENUM_OBJECTS,
    ZS_ACCESS_TYPES_ENUM_CACHED_OBJECTS,
    ZS_ACCESS_TYPES_NUM_CONT_DELETES_PEND,
    ZS_ACCESS_TYPES_NUM_CONT_DELETES_PROG,
    /* Application request at Btree layer.
       Add new zs specific stats above this line */
    ZS_ACCESS_TYPES_READ,
    ZS_ACCESS_TYPES_WRITE,
    ZS_ACCESS_TYPES_DELETE,
    ZS_ACCESS_TYPES_FLUSH,
    ZS_ACCESS_TYPES_MPUT,
    ZS_ACCESS_TYPES_MSET,
    ZS_ACCESS_TYPES_RANGE,
    ZS_ACCESS_TYPES_RANGE_NEXT,
    ZS_ACCESS_TYPES_RANGE_FINISH,
    ZS_ACCESS_TYPES_RANGE_UPDATE,
    ZS_ACCESS_TYPES_CREATE_SNAPSHOT,
    ZS_ACCESS_TYPES_DELETE_SNAPSHOT,
    ZS_ACCESS_TYPES_LIST_SNAPSHOT,
    ZS_ACCESS_TYPES_TRX_START,
    ZS_ACCESS_TYPES_TRX_COMMIT,
    ZS_N_ACCESS_TYPES
} ZS_access_types_t;

typedef enum {
    /* Btree L1 stats */
    ZS_BTREE_L1_ENTRIES,
    ZS_BTREE_L1_OBJECTS,
    ZS_BTREE_LEAF_L1_HITS,
    ZS_BTREE_NONLEAF_L1_HITS,
    ZS_BTREE_OVERFLOW_L1_HITS,

    ZS_BTREE_LEAF_L1_MISSES,
    ZS_BTREE_NONLEAF_L1_MISSES,
    ZS_BTREE_OVERFLOW_L1_MISSES,
    ZS_BTREE_BACKUP_L1_MISSES,
    ZS_BTREE_BACKUP_L1_HITS,

    ZS_BTREE_LEAF_L1_WRITES,
    ZS_BTREE_NONLEAF_L1_WRITES,
    ZS_BTREE_OVERFLOW_L1_WRITES,
    /* Btree stats */
    ZS_BTREE_LEAF_NODES,
    ZS_BTREE_NONLEAF_NODES,

    ZS_BTREE_OVERFLOW_NODES,
    ZS_BTREE_LEAF_BYTES,
    ZS_BTREE_NONLEAF_BYTES,
    ZS_BTREE_OVERFLOW_BYTES,
    ZS_BTREE_NUM_OBJS,

    ZS_BTREE_TOTAL_BYTES,
    ZS_BTREE_EVICT_BYTES,
    ZS_BTREE_SPLITS,
    ZS_BTREE_LMERGES,
    ZS_BTREE_RMERGES,

    ZS_BTREE_LSHIFTS,
    ZS_BTREE_RSHIFTS,
    ZS_BTREE_EX_TREE_LOCKS,
    ZS_BTREE_NON_EX_TREE_LOCKS,
    ZS_BTREE_GET,
    ZS_BTREE_GET_PATH_LEN,
    ZS_BTREE_CREATE,
    ZS_BTREE_CREATE_PATH_LEN,
    ZS_BTREE_SET,
    ZS_BTREE_SET_PATH_LEN,
    ZS_BTREE_UPDATE,
    ZS_BTREE_UPDATE_PATH_LEN,
    ZS_BTREE_DELETE_PATH_LEN,
    ZS_BTREE_FLUSH_CNT,

    ZS_BTREE_DELETE_OPT_COUNT,
    ZS_BTREE_MPUT_IO_SAVED,
    ZS_BTREE_PUT_RESTART_CNT,
    ZS_BTREE_SPCOPT_BYTES_SAVED,
    ZS_BTREE_NUM_MPUT_OBJS,

    ZS_BTREE_NUM_RANGE_NEXT_OBJS,
    ZS_BTREE_NUM_RANGE_UPDATE_OBJS,
    ZS_BTREE_NUM_SNAP_OBJS,
    ZS_BTREE_SNAP_DATA_SIZE,
    ZS_BTREE_NUM_SNAPS,

    ZS_BTREE_NUM_BULK_INSERT_CNT,
    ZS_BTREE_NUM_BULK_INSERT_FULL_NODES_CNT,
    ZS_N_BTREE_STATS
}ZS_Btree_stat_t;
#if 0
typedef enum {
    /* Log container stats */
    ZS_LOG_NUM_OBJS,
    ZS_LOG_MPUT_IO_SAVED,
    ZS_LOG_NUM_MPUT_OBJS,

    ZS_N_LOG_STATS
}ZS_Log_stat_t;
#endif

typedef enum {
	/* NVRAM stats */
	ZS_NVR_WRITE_REQS,
	ZS_NVR_WRITE_SAVED,
	ZS_NVR_DATA_IN,
	ZS_NVR_DATA_OUT,
	ZS_NVR_NOSPC,
	ZS_NVR_SYNC_REQS,
	ZS_NVR_SYNC_RESTARTS,
	ZS_NVR_SYNC_SPLITS,
	ZS_N_NVR_STATS
} ZS_Nvr_stat_t;

typedef enum {
    ZS_CACHE_STAT_OVERWRITES_S,
    ZS_CACHE_STAT_OVERWRITES_M,
    ZS_CACHE_STAT_INPLACEOWR_S,
    ZS_CACHE_STAT_INPLACEOWR_M,
    ZS_CACHE_STAT_NEW_ENTRIES,
    ZS_CACHE_STAT_WRITETHRUS,
    ZS_CACHE_STAT_WRITEBACKS,
    ZS_CACHE_STAT_FLUSHES,
    ZS_CACHE_STAT_ASYNC_DRAINS,
    ZS_CACHE_STAT_ASYNC_PUTS,
    ZS_CACHE_STAT_ASYNC_PUT_FAILS,
    ZS_CACHE_STAT_ASYNC_FLUSHES,
    ZS_CACHE_STAT_ASYNC_FLUSH_FAILS,
    ZS_CACHE_STAT_ASYNC_WRBKS,
    ZS_CACHE_STAT_ASYNC_WRBK_FAILS,
    ZS_CACHE_STAT_CACHE_MISSES,
    ZS_CACHE_STAT_CACHE_HITS,

    /* request from cache to flash manager */
    ZS_CACHE_STAT_AHCOB,
    ZS_CACHE_STAT_AHCOP,
    ZS_CACHE_STAT_AHCWD,
    ZS_CACHE_STAT_AHDOB,
    ZS_CACHE_STAT_AHFLD,
    ZS_CACHE_STAT_AHGTR,
    ZS_CACHE_STAT_AHGTW,
    ZS_CACHE_STAT_AHPTA,
    ZS_CACHE_STAT_AHSOB,
    ZS_CACHE_STAT_AHSOP,
    /* Request from flash manager to cache */
    ZS_CACHE_STAT_HACRC,
    ZS_CACHE_STAT_HACRF,
    ZS_CACHE_STAT_HACSC,
    ZS_CACHE_STAT_HACSF,
    ZS_CACHE_STAT_HADEC,
    ZS_CACHE_STAT_HADEF,
    ZS_CACHE_STAT_HAFLC,
    ZS_CACHE_STAT_HAFLF,
    ZS_CACHE_STAT_HAGRC,
    ZS_CACHE_STAT_HAGRF,
    ZS_CACHE_STAT_HAGWC,
    ZS_CACHE_STAT_HAGWF,
    ZS_CACHE_STAT_HAPAC,
    ZS_CACHE_STAT_HAPAF,
    ZS_CACHE_STAT_HASTC,
    ZS_CACHE_STAT_HASTF,
    ZS_CACHE_STAT_HFXST,
    ZS_CACHE_STAT_FHXST,
    ZS_CACHE_STAT_FHNXS,
    ZS_CACHE_STAT_HFGFF,
    ZS_CACHE_STAT_FHDAT,
    ZS_CACHE_STAT_FHGTF,
    ZS_CACHE_STAT_HFPTF,
    ZS_CACHE_STAT_FHPTC,
    ZS_CACHE_STAT_FHPTF,
    ZS_CACHE_STAT_HZSF,
    ZS_CACHE_STAT_FHDEC,
    ZS_CACHE_STAT_FHDEF,
    ZS_CACHE_STAT_HFCIF,
    ZS_CACHE_STAT_FHCRC,
    ZS_CACHE_STAT_FHCRF,
    ZS_CACHE_STAT_HFCZF,
    ZS_CACHE_STAT_HFCRC,
    ZS_CACHE_STAT_HFWRF,
    ZS_CACHE_STAT_HFSET,
    ZS_CACHE_STAT_HFSTC,
    ZS_CACHE_STAT_FHSTF,
    ZS_CACHE_STAT_HFCSH,
    ZS_CACHE_STAT_FHCSC,
    ZS_CACHE_STAT_FHCSF,
    ZS_CACHE_STAT_HFSSH,
    ZS_CACHE_STAT_FHSSC,
    ZS_CACHE_STAT_FHSSF,
    ZS_CACHE_STAT_HFDSH,
    ZS_CACHE_STAT_FHDSC,
    ZS_CACHE_STAT_FHDSF,
    ZS_CACHE_STAT_HFGLS,
    ZS_CACHE_STAT_FHGLC,
    ZS_CACHE_STAT_FHGLF,
    ZS_CACHE_STAT_HFGIC,
    ZS_CACHE_STAT_FHGIC,
    ZS_CACHE_STAT_FHGIF,
    ZS_CACHE_STAT_HFGBC,
    ZS_CACHE_STAT_FHGCC,
    ZS_CACHE_STAT_FHGCF,
    ZS_CACHE_STAT_HFGSN,
    ZS_CACHE_STAT_HFGCS,
    ZS_CACHE_STAT_FHGSC,
    ZS_CACHE_STAT_FHGSF,
    ZS_CACHE_STAT_HFSRR,
    ZS_CACHE_STAT_FHSRC,
    ZS_CACHE_STAT_FHSRF,
    ZS_CACHE_STAT_HFSPR,
    ZS_CACHE_STAT_FHSPC,
    ZS_CACHE_STAT_FHSPF,
    ZS_CACHE_STAT_HFFLA,
    ZS_CACHE_STAT_FHFLC,
    ZS_CACHE_STAT_FHFLF,
    ZS_CACHE_STAT_HFRVG,
    ZS_CACHE_STAT_FHRVC,
    ZS_CACHE_STAT_FHRVF,
    ZS_CACHE_STAT_HFNOP,
    ZS_CACHE_STAT_FHNPC,
    ZS_CACHE_STAT_FHNPF,
    ZS_CACHE_STAT_HFOSH,
    ZS_CACHE_STAT_FHOSC,
    ZS_CACHE_STAT_FHOSF,
    ZS_CACHE_STAT_HFFLS,
    ZS_CACHE_STAT_FHFCC,
    ZS_CACHE_STAT_FHFCF,
    ZS_CACHE_STAT_HFFIV,
    ZS_CACHE_STAT_FHFIC,
    ZS_CACHE_STAT_FHFIF,
    ZS_CACHE_STAT_HFINV,
    ZS_CACHE_STAT_FHINC,
    ZS_CACHE_STAT_FHINF,
    ZS_CACHE_STAT_HFFLC,
    ZS_CACHE_STAT_FHLCC,
    ZS_CACHE_STAT_FHLCF,
    ZS_CACHE_STAT_HFFLI,
    ZS_CACHE_STAT_FHLIC,
    ZS_CACHE_STAT_FHLIF,
    ZS_CACHE_STAT_HFINC,
    ZS_CACHE_STAT_FHCIC,
    ZS_CACHE_STAT_FHCIF,
    ZS_CACHE_STAT_EOK,
    ZS_CACHE_STAT_EPERM,
    ZS_CACHE_STAT_ENOENT,
    ZS_CACHE_STAT_EDATASIZE,
    ZS_CACHE_STAT_ESTOPPED,
    ZS_CACHE_STAT_EBADCTNR,
    ZS_CACHE_STAT_EDELFAIL,
    ZS_CACHE_STAT_EAGAIN,
    ZS_CACHE_STAT_ENOMEM,
    ZS_CACHE_STAT_EACCES,
    ZS_CACHE_STAT_EINCONS,
    ZS_CACHE_STAT_EBUSY,
    ZS_CACHE_STAT_EEXIST,
    ZS_CACHE_STAT_EINVAL,
    ZS_CACHE_STAT_EMFILE,
    ZS_CACHE_STAT_ENOSPC,
    ZS_CACHE_STAT_ENOBUFS,
    ZS_CACHE_STAT_ESTALE,
    ZS_CACHE_STAT_EDQUOT,
    ZS_CACHE_STAT_RMT_EDELFAIL,
    ZS_CACHE_STAT_RMT_EBADCTNR,
    ZS_CACHE_STAT_HASH_BUCKETS,
    ZS_CACHE_STAT_NUM_SLABS,
    ZS_CACHE_STAT_NUM_ELEMENTS,
    ZS_CACHE_STAT_MAX_SIZE,
    ZS_CACHE_STAT_CURR_SIZE,
    ZS_CACHE_STAT_CURR_SIZE_WKEYS,
    ZS_CACHE_STAT_NUM_MODIFIED_OBJS,
    ZS_CACHE_STAT_NUM_MODIFIED_OBJS_WKEYS,
    ZS_CACHE_STAT_NUM_MODIFIED_OBJS_FLUSHED,
    ZS_CACHE_STAT_NUM_MODIFIED_OBJS_BGFLUSHED,
    ZS_CACHE_STAT_NUM_PENDING_REQS,
    ZS_CACHE_STAT_NUM_MODIFIED_OBJC_REC,
    ZS_CACHE_STAT_BGFLUSH_PROGRESS,
    ZS_CACHE_STAT_NUM_BGFLUSH,
    ZS_CACHE_STAT_NUM_FLUSH_PARALLEL,
    ZS_CACHE_STAT_NUM_BGFLUSH_PARALLEL,
    ZS_CACHE_STAT_BGFLUSH_WAIT,
    ZS_CACHE_STAT_MODIFIED_PCT,
    ZS_CACHE_STAT_NUM_APP_BUFFERS,
    ZS_CACHE_STAT_NUM_CACHE_OPS_PROG,
    ZS_CACHE_STAT_NUM_FDBUFFER_PROCESSED,
    ZS_CACHE_STAT_NUM_RESP_PROCESSED ,
    ZS_N_CACHE_STATS
} ZS_cache_stat_t;

typedef enum {
    ZS_FLASH_STATS_NUM_OBJS,
    ZS_FLASH_STATS_NUM_CREATED_OBJS,
    ZS_FLASH_STATS_NUM_EVICTIONS,
    ZS_FLASH_STATS_NUM_HASH_EVICTIONS,
    ZS_FLASH_STATS_NUM_INVAL_EVICTIONS,
    ZS_FLASH_STATS_NUM_SOFT_OVERFLOWS,
    ZS_FLASH_STATS_NUM_HARD_OVERFLOWS,
    ZS_FLASH_STATS_GET_HASH_COLLISION,
    ZS_FLASH_STATS_SET_HASH_COLLISION,
    ZS_FLASH_STATS_NUM_OVERWRITES,
    ZS_FLASH_STATS_NUM_OPS,
    ZS_FLASH_STATS_NUM_READ_OPS,
    ZS_FLASH_STATS_NUM_GET_OPS,
    ZS_FLASH_STATS_NUM_PUT_OPS,
    ZS_FLASH_STATS_NUM_DEL_OPS,
    ZS_FLASH_STATS_GET_EXIST_CHECKS,
    ZS_FLASH_STATS_NUM_FULL_BUCKETS,
    ZS_FLASH_STATS_PENDING_IOS,
    ZS_FLASH_STATS_SPACE_ALLOCATED,
    ZS_FLASH_STATS_SPACE_CONSUMED,

    ZS_FLASH_STATS_SLAB_GC_SEGMENTS_COMPACTED,
    ZS_FLASH_STATS_SLAB_GC_SEGMENTS_FREED,
    ZS_FLASH_STATS_SLAB_GC_SLABS_RELOCATED,
    ZS_FLASH_STATS_SLAB_GC_BLOCKS_RELOCATED,
    ZS_FLASH_STATS_SLAB_GC_RELOCATE_ERRORS,
    ZS_FLASH_STATS_SLAB_GC_SIGNALLED,
    ZS_FLASH_STATS_SLAB_GC_SIGNALLED_SYNC,
    ZS_FLASH_STATS_SLAB_GC_WAIT_SYNC,
    ZS_FLASH_STATS_SLAB_GC_SEGMENTS_CANCELLED,
    ZS_FLASH_STATS_NUM_FREE_SEGMENTS,
    ZS_FLASH_STATS_COMP_BYTES,
    ZS_FLASH_STATS_THD_CONTEXTS,
    ZS_FLASH_STATS_ESCVN_OBJ_DEL,
    ZS_FLASH_STATS_ESCVN_YLD_SCAN_CMPLTE,
    ZS_FLASH_STATS_ESCVN_YLD_SCAN_RATE,

    ZS_FLASH_STATS_NUM_DATA_WRITES,
    ZS_FLASH_STATS_NUM_DATA_FSYNCS,
    ZS_FLASH_STATS_NUM_LOG_WRITES,
    ZS_FLASH_STATS_NUM_LOG_FSYNCS,

    ZS_N_FLASH_STATS
} ZS_flash_stat_t;

typedef enum {
    ZS_CNTR_STATS_NUM_OBJS,
    ZS_CNTR_STATS_USED_SPACE,

    ZS_N_CNTR_STATS
} ZS_cntr_stat_t;

typedef struct {
	uint64_t		 n_accesses[ZS_N_ACCESS_TYPES];
	uint64_t		 flash_stats[ZS_N_FLASH_STATS];
	uint64_t		 cache_stats[ZS_N_CACHE_STATS];
	uint64_t		 cntr_stats[ZS_N_CNTR_STATS];
	uint64_t		 btree_stats[ZS_N_BTREE_STATS];
	ZS_histo_t		 key_size_histo;
	ZS_histo_t		 data_size_histo;
	ZS_histo_t		 access_time_histo[ZS_N_ACCESS_TYPES];
	//uint64_t		 log_stats[ZS_N_LOG_STATS];
	uint64_t		 nvr_stats[ZS_N_NVR_STATS];
} ZS_stats_t;

typedef struct {
	uint64_t		timestamp;
	uint64_t		seqno;
} ZS_container_snapshots_t;

typedef struct {
    uint64_t				size_kb;
    char					name[CONTAINER_NAME_MAXLEN];
    ZS_boolean_t			fifo_mode;
    ZS_boolean_t			persistent;
    ZS_boolean_t			evicting;
    ZS_boolean_t			writethru;
    ZS_boolean_t			async_writes;
    ZS_durability_level_t	durability_level;
    ZS_cguid_t				cguid;
    uint64_t				cid;
    uint32_t                num_shards;
	ZS_boolean_t			flash_only;
	ZS_boolean_t			cache_only;
    ZS_boolean_t			compression; /* Flag to enable/disable compression */
    ZS_prop_flag_t			flags;
} ZS_container_props_t;


#define ZS_CTNR_CREATE   1
#define ZS_CTNR_RO_MODE  2
#define ZS_CTNR_RW_MODE  4

typedef enum {
    ZS_WRITE_MUST_NOT_EXIST = 1,
    ZS_WRITE_MUST_EXIST    	= 2,
	ZS_WRITE_TRIM			= 4,
} ZS_write_mode_t;

typedef struct {
    char            *key;
    uint32_t         key_len;
    char            *data;
    uint64_t         data_len;
    ZS_time_t       current;
    ZS_time_t       expiry;
} ZS_readobject_t;

typedef struct {
    char            *key;
    uint32_t         key_len;
    char            *data;
    uint64_t         data_len;
    ZS_time_t       current;
    ZS_time_t       expiry;
} ZS_writeobject_t;

/*
 *  Function used to compare keys
 *  to determine ordering in the index.
 *
 *  Returns: -1 if key1 comes before key2
 *            0 if key1 is the same as key2
 *            1 if key1 comes after key2
 */
typedef int (ZS_cmp_fn_t)(void     *index_data, //  opaque user data
                           char     *key1,       //  first secondary key
                           uint32_t  keylen1,    //  length of first secondary key
                           char     *key2,       //  second secondary key
                           uint32_t  keylen2);   //  length of second secondary key

typedef int (* ZS_mput_cmp_cb_t)(void     *data, 	//  opaque user data
                                 char     *key,
                                 uint32_t  keylen,
				 char *old_data,
				 uint64_t old_datalen,
				 char *new_data,
				 uint64_t new_datalen);



typedef struct ZS_container_meta_s {
	ZS_cmp_fn_t   *sort_cmp_fn;             // compare function for key
	void           *cmp_data;                // Any data to provide for cmp

	ZS_mput_cmp_cb_t mput_cmp_fn;
	void *mput_cmp_cb_data;
} ZS_container_meta_t;

struct ZS_state;
struct ZS_thread_state;
struct ZS_iterator;

typedef int (* ZS_range_cmp_cb_t)(void     *data, 	//  opaque user data
			         void     *range_data,
                                 char     *range_key,
                                 uint32_t  range_keylen,
                                 char     *key1,
                                 uint32_t  keylen1);

/*
 * Get a ZS property.
 */
const char *ZSGetProperty(const char *key, const char *def);


/**
 * @brief set ZS property
 *
 * @param propery <IN> property name
 * @param value <IN> pointer to value
 * @return ZS_SUCCESS on success
 *
 */
ZS_status_t ZSSetProperty(
	const char* property,
	const char* value
	);

/**
 * @brief Load properties from specified file
 *
 * @param proper_file <IN> properties file
 * @return ZS_SUCCESS on success
 *
 */
ZS_status_t ZSLoadProperties(
	const char *prop_file
	);

/**
 * @brief ZS initialization
 *
 * @param zs_state <OUT> ZS state variable
 * @param api_version <IN> ZS API version
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSInitVersioned(
	struct ZS_state	**zs_state,
	uint32_t                api_version
	);
#define ZSInit(state)           ZSInitVersioned(state, ZS_API_VERSION)

/**
 * @brief ZS per thread state initialization
 *
 * @param zs_state <IN> ZS state variable
 * @param thd_state <OUT> ZS per thread state variable
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSInitPerThreadState(
	struct ZS_state		 *zs_state,
	struct ZS_thread_state	**thd_state
	);

/**
 * @brief ZS release per thread state initialization
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSReleasePerThreadState(
	struct ZS_thread_state	**thd_state
	);

/**
 * @brief ZS shutdown
 *
 * @param zs_state <IN> ZS state variable
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSShutdown(
	struct ZS_state	*zs_state
	);

/**
 * @brief ZS load default container properties
 *
 * @param props <IN> ZS container properties pointer
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSLoadCntrPropDefaults(
	ZS_container_props_t *props
	);

 /**
 * @brief Create and open a virtual container.
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cname <IN> container name
 * @param properties <IN> container properties
 * @param flags <IN> container open options
 * @param cguid <OUT> container GUID
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSOpenContainer(
	struct ZS_thread_state	*zs_thread_state,
	const char					*cname,
	ZS_container_props_t 	*properties,
	uint32_t			 	 flags,
	ZS_cguid_t				*cguid
	);

 /**
 * @brief Create and open a virtual container.
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cname <IN> container name
 * @param properties <IN> container properties
 * @param flags <IN> container open options
 * @param cguid <OUT> container GUID
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSOpenContainerSpecial(
	struct ZS_thread_state	  *zs_thread_state,
	const char                      *cname,
	ZS_container_props_t     *properties,
	uint32_t                  flags,
	ZS_container_meta_t      *cmeta,
	ZS_cguid_t               *cguid
	);

/**
 * @brief Close a virtual container.
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container CGUID
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSCloseContainer(
	struct ZS_thread_state *zs_thread_state,
	ZS_cguid_t				 cguid
	);

/**
 * @brief Delete a virtual container
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container CGUID
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSDeleteContainer(
	struct ZS_thread_state *zs_thread_state,
	ZS_cguid_t				 cguid
	);

 /**
 * @brief Rename a container.
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container GUID
 * @param new_cname <IN> new container name
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSRenameContainer(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t		 cguid,
	const char			*new_cname
	);

/**
 * @brief Get container list
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies.
 * @param cguids  <OUT> pointer to container GUID array
 * @param n_cguids <OUT> pointer to container GUID count
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSGetContainers(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t             *cguids,
	uint32_t                *n_cguids
	);

/**
 * @brief Get container properties
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param pprops <IN> pointer to structure into which to copy properties
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSGetContainerProps(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t            	 cguid,
	ZS_container_props_t	*pprops
	);

/**
 * @brief Set container properties
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param pprops <IN> pointer to structure into which to copy properties
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSSetContainerProps(
	struct ZS_thread_state 	*zs_thread_state,
	ZS_cguid_t              	 cguid,
	ZS_container_props_t   	*pprops
	);

/**
 *  @brief Get a copy of an object for read-only  access. Return its current expiry time.
 *
 *  Get an object and copy it into an ZS-allocated buffer. The application
 *  only intends to read the object. The current expiry time is returned.
 *
 *  @param sdf_thread_state <IN> The ZS context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param data <IN> Variable in which to return a pointer to the object (in a buffer
 *  allocated by ZS; it must be freed by the application with a call
 *  to ZSFreeObjectBuffer).
 *  @param datalen <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OBJECT_UNKNOWN: the object does not exist.
 *          ZS_IN_TRANS: this operation cannot be done inside a transaction.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t ZSReadObject(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t               cguid,
	const char                     *key,
	uint32_t                  keylen,
	char                     **data,
	uint64_t                 *datalen
	);

ZS_status_t ZSReadObject2(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t               cguid,
	const char                     *key,
	uint32_t                  keylen,
	char                     **data,
	uint64_t                 *datalen
	);

/**
 *  @brief Get a copy of an object for read-only  access. Return its current expiry time.
 *
 *  Get an object and copy it into an ZS-allocated buffer. The application
 *  only intends to read the object. The current expiry time is returned.
 *
 *  @param zs_thread_state <IN> The ZS context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param robj <IN> Identity of a read object structure
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OBJECT_UNKNOWN: the object does not exist.
 *          ZS_IN_TRANS: this operation cannot be done inside a transaction.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t ZSReadObjectExpiry(
    struct ZS_thread_state  *zs_thread_state,
    ZS_cguid_t               cguid,
    ZS_readobject_t         *robj
    );

/**
 * @brief Free an object buffer
 *
 * @param buf <IN> object buffer
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSFreeBuffer(
	char *buf
	);

/**
 *  @brief Write entire object, creating it if necessary.
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer. This may change the size of the object. The expiry time
 *  is set. If the object does not exist, create it and assign its
 *  value.
 *
 *  @param zs_state <IN> The ZS context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param datalen <IN> Size of object.
 *  @param data <IN> Pointer to application buffer from which to copy data.
 *  @param flags <IN> create/update flags
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OUT_OF_MEM: there is insufficient memory/flash.
 *          ZS_IN_TRANS: this operation cannot be done inside a transaction.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t ZSWriteObject(
	struct ZS_thread_state  *sdf_thread_state,
	ZS_cguid_t          cguid,
	const char                *key,
	uint32_t             keylen,
	const char                *data,
	uint64_t             datalen,
	uint32_t			 flags
	);

/**
 *  @brief Write entire object, creating it if necessary.
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer. This may change the size of the object. The expiry time
 *  is set. If the object does not exist, create it and assign its
 *  value.
 *
 *  @param zs_thread_state <IN> The ZS context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param wobj <IN> Identity of a write object structure
 *  @param flags <IN> create/update flags
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OUT_OF_MEM: there is insufficient memory/flash.
 *          ZS_IN_TRANS: this operation cannot be done inside a transaction.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t ZSWriteObjectExpiry(
    struct ZS_thread_state  *zs_thread_state,
    ZS_cguid_t               cguid,
    ZS_writeobject_t        *wobj,
    uint32_t                  flags
    );

/**
 *  @brief Delete an object, but check for expiry first.
 *
 *  Delete an object, but check for expiry first.
 *
 *  @param zs_thread_state <IN> The ZS context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OBJECT_UNKNOWN: the object does not exist.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t ZSDeleteObject(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t               cguid,
	const char                     *key,
	uint32_t                  keylen
	);

/**
 * @brief Enumerate container objects
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid  <IN> container global identifier
 * @param iterator <IN> enumeration iterator
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSEnumerateContainerObjects(
	struct ZS_thread_state *zs_thread_state,
	ZS_cguid_t              cguid,
	struct ZS_iterator    **iterator
	);

/**
 * @brief Enumerate PG objects
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid  <IN> container global identifier
 * @param iterator <IN> enumeration iterator
 * @param key <IN> pointer to key variable
 * @param keylen <IN> pointer to key length variable
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSEnumeratePGObjects(
	struct ZS_thread_state *zs_thread_state,
	ZS_cguid_t              cguid,
	struct ZS_iterator    **iterator,
	char                    *key,
	uint32_t                keylen
	);

/**
 * @brief Enumerate All PG objects
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid  <IN> container global identifier
 * @param iterator <IN> enumeration iterator
 * @param key <IN> pointer to key variable
 * @param keylen <IN> pointer to key length variable
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSEnumerateAllPGObjects(
	struct ZS_thread_state *zs_thread_state,
	ZS_cguid_t              cguid,
	struct ZS_iterator    **iterator
	);

/**
 * @brief Container object enumration iterator
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param iterator <IN> enumeration iterator
 * @param cguid  <IN> container global identifier
 * @param key <OUT> pointer to key variable
 * @param keylen <OUT> pointer to key length variable
 * @param data <OUT> pointer to data variable
 * @param datalen <OUT> pointer to data length variable
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSNextEnumeratedObject(
	struct ZS_thread_state *zs_thread_state,
	struct ZS_iterator     *iterator,
	char                    **key,
	uint32_t                *keylen,
	char                    **data,
	uint64_t                *datalen
	);

/**
 * @brief Terminate enumeration
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param iterator <IN> enumeration iterator
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSFinishEnumeration(
	struct ZS_thread_state *zs_thread_state,
	struct ZS_iterator     *iterator
	);

/**
 *  @brief Force modifications of an object to primary storage.
 *
 *  Flush any modified contents of an object to its backing store
 *  (as determined by its container type). For coherent containers,
 *  this is a global operation that applies to any cache or buffer
 *  in the ZS cluster. For non-coherent containers, this only applies
 *  to the local cache.
 *
 *  @param zs_thread_state <IN> The ZS context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OBJECT_UNKNOWN: the object does not exist.
 *          ZS_IN_TRANS: this operation cannot be done inside a transaction.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t ZSFlushObject(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t               cguid,
	const char               *key,
	uint32_t                  keylen
	);

/**
 * @brief Flush container
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies.
 * @param cguid  <IN> container global identifier
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSFlushContainer(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t               cguid
	);

/**
 * @brief Flush the cache
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies.
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSFlushCache(
	struct ZS_thread_state  *zs_thread_state
	);

/**
 * @brief Get ZS statistics
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param stats <OUT> pointer to statistics return structure
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSGetStats(
	struct ZS_thread_state *zs_thread_state,
	ZS_stats_t             *stats
	);

/**
 * @brief Get per container statistics
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid  <IN> container global identifier
 * @param stats <OUT> pointer to statistics return structure
 * @return ZS_SUCCESS on success
 */
ZS_status_t ZSGetContainerStats(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t				 cguid,
	ZS_stats_t     		*stats
	);

/**
 * @brief Get error string for given error code
 *
 * @param errno ZS error number
 * @return  error string
 */
char *ZSStrError(ZS_status_t zs_errno);

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
	);

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
	);

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
	);

/**
 * @brief Quit a transaction
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @return ZS_SUCCESS on success
 *         ZS_FAILURE_NO_TRANS if there is no active transaction in the current thread
 */
ZS_status_t ZSTransactionQuit(
	struct ZS_thread_state	*zs_thread_state
	);

/**
 * @brief ID of current transaction
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @return Non-zero transaction ID on success
 *         Zero if there is no active transaction in the current thread
 */
uint64_t ZSTransactionID(
	struct ZS_thread_state	*zs_thread_state
	);

/**
 * @brief Perform internal transaction service
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @return ZS_SUCCESS on success
 *         ZS_FAILURE for error unspecified
 */
ZS_status_t ZSTransactionService(
	struct ZS_thread_state	*zs_thread_state,
	int			cmd,
	void			*arg
	);



/**
 * @brief Return version of ZS
 *
 * @param  Address of pointer to hold the version string
 * @return String having the versions
 *         NULL if failed internally
 */
ZS_status_t ZSGetVersion(
	char **str
	);

#define N_ENTRIES_TO_MALLOC    100
#define N_ITERATORS_TO_MALLOC  100

struct ZSCMapIterator;

struct ZSTLMapBucket;

typedef struct ZSTLMapEntry {
    char                  *contents;
    uint64_t               datalen;
    int32_t                refcnt;
    char                  *key;
    uint32_t               keylen;
    struct ZSTLMapEntry  *next;
    struct ZSTLMapEntry  *next_lru;
    struct ZSTLMapEntry  *prev_lru;
    struct ZSTLMapBucket *bucket;
} ZSTLMapEntry_t;

typedef struct ZSTLMapBucket {
    struct ZSTLMapEntry *entry;
} ZSTLMapBucket_t;

typedef struct ZSTLIterator {
    uint64_t                enum_bucket;
    ZSTLMapEntry_t        *enum_entry;
    struct ZSTLIterator   *next;
} ZSTLIterator_t;

typedef struct ZSTLMap {
    uint64_t          nbuckets;
    uint64_t          max_entries;
    uint64_t          n_entries;
    char              use_locks;
    ZSTLMapBucket_t *buckets;
    pthread_mutex_t   mutex;
    pthread_mutex_t   enum_mutex;
    ZSTLMapEntry_t  *lru_head;
    ZSTLMapEntry_t  *lru_tail;
    void              (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen);
    void             *replacement_callback_data;
    uint32_t          NEntries;
    uint32_t          NUsedEntries;
    ZSTLMapEntry_t  *FreeEntries;
    uint32_t          NIterators;
    uint32_t          NUsedIterators;
    struct ZSTLIterator *FreeIterators;
} ZSTLMap_t;

struct ZSTLMap* ZSTLMapInit(uint64_t nbuckets, uint64_t max_entries, char use_locks, void (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen), void *replacement_callback_data);
void ZSTLMapDestroy(struct ZSTLMap *pm);
void ZSTLMapClear(struct ZSTLMap *pm);
struct ZSTLMapEntry* ZSTLMapCreate(struct ZSTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
struct ZSTLMapEntry* ZSTLMapUpdate(struct ZSTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
struct ZSTLMapEntry* ZSTLMapSet(struct ZSTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, char **old_pdata, uint64_t *old_datalen);
struct ZSTLMapEntry* ZSTLMapGet(struct ZSTLMap *pc, char *key, uint32_t keylen, char** data, uint64_t *pdatalen);
int ZSTLMapIncrRefcnt(struct ZSTLMap *pm, char *key, uint32_t keylen);
void ZSTLMapCheckRefcnts(struct ZSTLMap *pm);
int ZSTLMapRelease(struct ZSTLMap *pm, char *key, uint32_t keylen);
int ZSTLMapReleaseEntry(struct ZSTLMap *pm, struct ZSTLMapEntry *pme);
struct ZSTLIterator* ZSTLMapEnum(struct ZSTLMap *pm);
void ZSTLFinishEnum(struct ZSTLMap *pm, struct ZSTLIterator *iterator);
int ZSTLMapNextEnum(struct ZSTLMap *pm, struct ZSTLIterator *iterator, char **key, uint32_t *keylen, char **data, uint64_t *datalen);
int ZSTLMapDelete(struct ZSTLMap *pm, char *key, uint32_t keylen);

/************ All Range Query related structures and functions ***************/

typedef enum {
	ZS_RANGE_BUFFER_PROVIDED      = 1<<0,  // buffers for keys and data provided by application
	ZS_RANGE_ALLOC_IF_TOO_SMALL   = 1<<1,  // if supplied buffers are too small, ZS will allocate
	ZS_RANGE_SEQNO_LE             = 1<<5,  // only return objects with seqno <= end_seq
	ZS_RANGE_SEQNO_GT_LE          = 1<<6,  // only return objects with start_seq < seqno <= end_seq

	ZS_RANGE_START_GT             = 1<<7,  // keys must be >  key_start
	ZS_RANGE_START_GE             = 1<<8,  // keys must be >= key_start
	ZS_RANGE_START_LT             = 1<<9,  // keys must be <  key_start
	ZS_RANGE_START_LE             = 1<<10, // keys must be <= key_start

	ZS_RANGE_END_GT               = 1<<11, // keys must be >  key_end
	ZS_RANGE_END_GE               = 1<<12, // keys must be >= key_end
	ZS_RANGE_END_LT               = 1<<13, // keys must be <  key_end
	ZS_RANGE_END_LE               = 1<<14, // keys must be <= key_end

	ZS_RANGE_KEYS_ONLY            = 1<<15, // only return keys (data is not required)

	ZS_RANGE_PRIMARY_KEY          = 1<<16, // return primary keys in secondary index query
	ZS_RANGE_INDEX_USES_DATA      = 1<<17, // Indicates that secondary index key
	                                        // is derived from object data
	ZS_RANGE_INPLACE_POINTERS      = 1<<18,  //
} ZS_range_enums_t;

/*
 *
 * Type definition for function that determines if we are allowed to return
 * this key as part of a range query.
 *
 * Return true (1) if the key is allowed and false (0) otherwise.
 */
typedef int (ZS_allowed_fn_t)(void *context_data,  // context data (opaque)
                               char *key,           // key to check if allowed
                               uint32_t len);       // length of the key

typedef struct ZS_range_meta {
	ZS_range_enums_t flags;         // flags controlling type of range query (see above)
	uint32_t          keybuf_size;   // size of application provided key buffers (if applicable)
	uint64_t          databuf_size;  // size of application provided data buffers (if applicable)
	char              *key_start;    // start key
	uint32_t          keylen_start;  // length of start key
	char              *key_end;      // end key
	uint32_t          keylen_end;    // length of end key
	uint64_t          start_seq;     // starting sequence number (if applicable)
	uint64_t          end_seq;       // ending sequence number (if applicable)
	ZS_cmp_fn_t      *class_cmp_fn; // Fn to cmp two keys are in same equivalence class
	ZS_allowed_fn_t  *allowed_fn;   // Fn to check if this key is allowed to put in range result
	void              *cb_data;      // Any data to be passed for allowed function
} ZS_range_meta_t;

struct           ZS_cursor;       // opaque cursor handle
typedef uint64_t ZS_indexid_t;    // persistent opaque index handle

#define ZS_RANGE_PRIMARY_INDEX     0

/*
 * Multiple objs put info.
 */
typedef struct {
    uint32_t flags;
    uint32_t key_len;
    uint64_t data_len;
    char *key;
    char *data;
} ZS_obj_t;

/*
 * ZSRangeUpdate Callback.
 */
typedef bool (* ZS_range_update_cb_t) (char *key, uint32_t keylen, char *data, uint32_t datalen,
				        void *callback_args, char **new_data, uint32_t *new_datalen);

/* Start an index query.
 *
 * Returns: ZS_SUCCESS if successful
 *          ZS_FAILURE if unsuccessful
 */
ZS_status_t ZSGetRange(struct ZS_thread_state *thrd_state, //  client thread ZS context
                         ZS_cguid_t              cguid,      //  container
                         ZS_indexid_t            indexid,    //  handle for index to use (use PRIMARY_INDEX for primary)
                         struct ZS_cursor      **cursor,     //  returns opaque cursor for this query
                         ZS_range_meta_t        *meta);      //  query attributes (see above)

typedef enum {
	ZS_RANGE_STATUS_NONE       = 0,
	ZS_RANGE_DATA_SUCCESS      = 1,
	ZS_KEY_BUFFER_TOO_SMALL    = 2,
	ZS_DATA_BUFFER_TOO_SMALL   = 4,
	ZS_RANGE_PAUSED            = 8
} ZS_range_status_t;

typedef struct ZS_range_data {
	ZS_range_status_t  status;           // status
	char         *key;              // index key value
	uint32_t      keylen;           // index key length
	char         *data;             // data
	uint64_t      datalen;          // data length
	uint64_t      seqno;            // sequence number for last update
	uint64_t      syndrome;         // syndrome for key
	char         *primary_key;      // primary key value (if required)
	uint32_t      primary_keylen;   // primary key length (if required)
} ZS_range_data_t;

/* Gets next n_in keys in the indexed query.
 *
 * The 'values' array must be allocated by the application, and must hold up to
 * 'n_in' entries.
 * If the BUFFER_PROVIDED flag is set, the key and data fields in 'values' must be
 * pre-populated with buffers provided by the application (with sizes that were
 * specified in 'meta' when the index query was started).  If the application provided
 * buffer is too small for returned item 'i', the status for that item will
 * be ZS_BUFFER_TOO_SMALL; if the ALLOC_IF_TOO_SMALL flag is set, ZS will allocate
 * a new buffer whenever the provided buffer is too small.
 *
 * If the SEQNO_LE flag is set, only items whose sequence number is less than or
 * equal to 'end_seq' from ZS_range_meta_t above are returned.
 * If there are multiple versions of an item that satisfy the inequality,
 * always return the most recent version.
 *
 * If the SEQNO_GT_LE flag is set, only items for which start_seq < item_seqno <= end_seq
 * are returned.  If there are multiple versions of an item that satisfy the inequality,
 * always return the most recent version.
 *
 * SEQNO_LE and SEQNO_GT_LE are mutually exclusive and must not be set together.
 * If neither SEQNO_LE or SEQNO_GT_LE are set the most recent version of each key
 * is returned.
 *
 *
 * Returns: ZS_SUCCESS    if all statuses are successful
 *          ZS_QUERY_DONE if query is done (n_out will be set to 0)
 *          ZS_FAILURE    if one or more of the key fetches fails (see statuses for the
 *                         status of each fetched object)
 *
 * statuses[i] returns: ZS_SUCCESS if the i'th data item was retrieved successfully
 *                      ZS_BUFFER_TOO_SMALL  if the i'th buffer was too small to retrieve the object
 */
ZS_status_t
ZSGetNextRange(struct ZS_thread_state *thrd_state,  //  client thread ZS context
                struct ZS_cursor       *cursor,      //  cursor for this indexed search
                int                      n_in,        //  size of 'values' array
                int                     *n_out,       //  number of items returned
                ZS_range_data_t        *values);     //  array of returned key/data values

/* End an index query.
 *
 * Returns: ZS_SUCCESS if successful
 *          ZS_UNKNOWN_CURSOR if the cursor is invalid
 */
ZS_status_t
ZSGetRangeFinish(struct ZS_thread_state *thrd_state,
                  struct ZS_cursor *cursor);

ZS_status_t
ZSMPut(struct ZS_thread_state *zs_ts,
        ZS_cguid_t cguid,
        uint32_t num_objs,
        ZS_obj_t *objs,
	uint32_t flags,
	uint32_t *objs_written);

ZS_status_t
ZSRangeUpdate(struct ZS_thread_state *zs_thread_state,
	       ZS_cguid_t cguid,
	       char *range_key,
	       uint32_t range_key_len,
	       ZS_range_update_cb_t callback_func,
	       void * callback_args,
	       ZS_range_cmp_cb_t range_cmp_callback,
	       void *range_cmp_cb_args,
	       uint32_t *objs_updated);

ZS_status_t
ZSCheckBtree(struct ZS_thread_state *zs_thread_state,
	       ZS_cguid_t cguid, uint64_t flags);

ZS_status_t
ZSCheck(struct ZS_thread_state *zs_thread_state, uint64_t flags);

// For ZS metadata checker
ZS_status_t
ZSCheckMeta();

ZS_status_t
ZSCheckFlog();

ZS_status_t
ZSCheckPOT();

ZS_status_t
ZSCheckInit(const char *logfile);

ZS_status_t
ZSCheckClose();

void
ZSCheckSetLevel(int level);

int
ZSCheckGetLevel();

void
ZSCheckMsg(ZS_check_entity_t entity,
           uint64_t id,
           ZS_check_error_t error,
           const char *msg
           );

ZS_status_t
ZSIoctl(struct ZS_thread_state *zs_thread_state,
         ZS_cguid_t cguid,
         uint32_t ioctl_type,
         void *data);

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
	);

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
	);

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
	ZS_container_snapshots_t	**snap_seqs
	);

ZS_status_t ZSScavenger(struct ZS_state *zs_state);
ZS_status_t ZSScavengeContainer(struct ZS_state *zs_state, ZS_cguid_t cguid);
ZS_status_t ZSScavengeSnapshot(struct ZS_state *zs_state, ZS_cguid_t cguid, uint64_t snap_seq);

/*
 * @brief Check that ZS operations are allowed (e.g., not in shutdown).
 *
 * @return ZS_SUCCESS if operations are allowed.
*/
ZS_status_t
ZSOperationAllowed( void );

ZS_status_t
ZSTransactionGetMode(struct ZS_thread_state *zs_thread_state, int *mode);

ZS_status_t
ZSTransactionSetMode(struct ZS_thread_state *zs_thread_state, int mode);

/*
 * @brief Get the context of last error reported by ZS
 *
 * The context returned by the ZS will have to be freed by the application
 *
 * @param cguid <IN> container global identifier
 * @param pp_err_context <OUT> The opaque error context allocated
 * @return ZS_SUCCESS if successful
 *         ZS_xxxzzz if snap_seqs cannot be allocated
 */
ZS_status_t
ZSGetLastError(ZS_cguid_t cguid, void **pp_err_context, uint32_t *p_err_size);

ZS_status_t
ZSRescueContainer(struct ZS_thread_state *zs_thread_state, ZS_cguid_t cguid, void *p_err_context);


ZS_status_t
ZSCheckSpace(struct ZS_thread_state *zs_thread_state);
#if 0
/*********************************************************
 *
 *    SECONDARY INDEXES
 *
 *********************************************************/

/*
 *   Function used to extract secondary key
 *   from primary key and/or data.
 *   This function must use ZSGetSecondaryKeyBuffer() below
 *   to allocate a buffer for the extracted key.
 *
 *   Returns:  0 if successful
 *             1 if fails (eg: ZSGetSecondaryKeyBuffer() fails)
 */
typedef int (ZS_index_fn_t)(void     *index_data,        //  opaque user data
                             char     *key,               //  primary key of object
                             uint32_t  keylen,            //  length of primary key
                             char     *data,              //  object data (if required, see flags)
                             uint64_t  datalen,           //  length of object data (if required, see flags)
                             char    **secondary_key,     //  returned secondary key
                             uint32_t *keylen_secondary); //  returned length of secondary key

/*
 *  Allocate a buffer in which to place an extracted
 *  secondary key.
 *
 *  Returns NULL if a buffer cannot be allocated.
 */
char * ZSGetSecondaryKeyBuffer(uint32_t len);

typedef struct ZS_index_meta {
    uint32_t        flags;       //  flags (see ZS_range_enums_t)
    ZS_index_fn   *index_fn;    //  function used to extract secondary key
    ZS_cmp_fn     *cmp_fn;      //  function used to compare index values
    void           *index_data;  //  opaque user data for index/cmp functions
} ZS_index_meta_t;

/*
 * Create a secondary index
 * Index creation is done synchronously: the function does
 * not return until the index is fully created.
 * Secondary index creation in crash-safe: if ZS crashes while
 * index creation is in progress, index creation will be completed
 * when ZS restarts.
 *
 * Returns: ZS_SUCCESS if successful
 *          ZS_xxxzzz if ZS runs out of memory
 *          ZS_xxxzzz if ZS runs out of storage
 */
ZS_status_t
ZSAddSecondaryIndex(struct ZS_thread_state *thrd_state,   //  client thread ZS context
                     ZS_cguid_t              cguid,        //  container in which to add index
                     ZS_index_id_t          *indexid,      //  persistent opaque handle for new index
                     ZS_index_meta_t        *meta);        //  attributes of new index

/*
 * Delete a secondary index
 *
 * Index deletion is done synchronously: the function does
 * not return until the index is fully deleted.
 * Secondary index deletion is crash-safe: if ZS crashes while
 * index deletion is in progress, index deletion will be completed
 * when ZS restarts.
 *
 * Returns: ZS_SUCCESS if successful
 *          ZS_INVALID_INDEX if index is invalid
 */
ZS_status_t
ZSDeleteSecondaryIndex(struct ZS_thread_state *thrd_state, //  client thread ZS context
                        ZS_cguid_t              cguid,      //  container in which to add index
                        ZS_indexid_t            indexid);   //  persistent opaque handle of index to delete

/*
 * Get a list of all current secondary indices.
 * Array returned in index_ids is allocated by ZS and
 * must be freed by application.
 *
 * Returns: ZS_SUCCESS if successful
 *          ZS_xxxzzz if index_ids cannot be allocated
 */
ZS_status_t
ZSGetContainerIndices(struct ZS_thread_state *ts,         //  client thread ZS context
                         ZS_cguid_t            cguid,      //  container
                         uint32_t              *n_indices,  //  returns number of indices
                         ZS_indexid_t         *index_ids); //  returns array of index ids

/*
 * Get attributes for a particular indexid.
 *
 * Returns: ZS_SUCCESS if successful
 *          ZS_xxxzzz if indexid is invalid
 */
ZS_status_t
ZSGetIndexMeta(struct ZS_thread_state *ts,       //  client thread ZS context
                ZS_cguid_t              cguid,    //  container
                ZS_indexid_t            indexid,  //  index id
                ZS_index_meta_t        *meta);    //  attributes of index


#endif /* If 0 */

#ifdef __cplusplus
}
#endif

#endif // __ZS_H
