/*
 * File:   fdf.h
 * Author: Darryl Ouye
 *
 * Created on August 22, 2012
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 */
#ifndef __FDF_H
#define __FDF_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>

#include "common/fdftypes.h"

#define fdf_cntr_drain_io( v )  while ( unlikely( v > 0 ) ) fthYield(0)

#define CONTAINER_NAME_MAXLEN		64
#define FDF_DEFAULT_CONTAINER_SIZE_KB	(1024 * 1024)	//1GB
#define FDF_MIN_FLASH_SIZE		3		//3GB

typedef struct {
	uint64_t		n;
	uint64_t		min;
	uint64_t		max;
	double			avg;
	double			geo;
	double			std;
	uint64_t		counts[64];
} FDF_histo_t;

typedef enum {
	FDF_DURABILITY_PERIODIC = 0,
	FDF_DURABILITY_SW_CRASH_SAFE    = 0x1,
	FDF_DURABILITY_HW_CRASH_SAFE    = 0x2,
} FDF_durability_level_t;

typedef enum {
    FDF_ACCESS_TYPES_APCOE, 
    FDF_ACCESS_TYPES_APCOP, 
    FDF_ACCESS_TYPES_APPAE, 
    FDF_ACCESS_TYPES_APPTA, 
    FDF_ACCESS_TYPES_APSOE, 
    FDF_ACCESS_TYPES_APSOB, 
    FDF_ACCESS_TYPES_APGRX, 
    FDF_ACCESS_TYPES_APGRD, 
    FDF_ACCESS_TYPES_APDBE, 
    FDF_ACCESS_TYPES_APDOB, 
    FDF_ACCESS_TYPES_APFLS, 
    FDF_ACCESS_TYPES_APFLI, 
    FDF_ACCESS_TYPES_APINV, 
    FDF_ACCESS_TYPES_APSYC, 
    FDF_ACCESS_TYPES_APICD, 
    FDF_ACCESS_TYPES_APGIT, 
    FDF_ACCESS_TYPES_APFCO, 
    FDF_ACCESS_TYPES_APFCI, 
    FDF_ACCESS_TYPES_APICO, 
    FDF_ACCESS_TYPES_APRIV, 
    FDF_ACCESS_TYPES_APRUP, 
    FDF_ACCESS_TYPES_ENUM_TOTAL,
    FDF_ACCESS_TYPES_ENUM_ACTIVE,
    FDF_ACCESS_TYPES_ENUM_OBJECTS,
    FDF_ACCESS_TYPES_ENUM_CACHED_OBJECTS,
    FDF_ACCESS_TYPES_NUM_CONT_DELETES_PEND,
    FDF_ACCESS_TYPES_NUM_CONT_DELETES_PROG,
    FDF_N_ACCESS_TYPES
} FDF_access_types_t;

typedef enum {
    FDF_CACHE_STAT_OVERWRITES_S,
    FDF_CACHE_STAT_OVERWRITES_M,
    FDF_CACHE_STAT_INPLACEOWR_S,
    FDF_CACHE_STAT_INPLACEOWR_M,
    FDF_CACHE_STAT_NEW_ENTRIES,
    FDF_CACHE_STAT_WRITETHRUS,
    FDF_CACHE_STAT_WRITEBACKS,
    FDF_CACHE_STAT_FLUSHES,
    FDF_CACHE_STAT_ASYNC_DRAINS,
    FDF_CACHE_STAT_ASYNC_PUTS,
    FDF_CACHE_STAT_ASYNC_PUT_FAILS,
    FDF_CACHE_STAT_ASYNC_FLUSHES,
    FDF_CACHE_STAT_ASYNC_FLUSH_FAILS,
    FDF_CACHE_STAT_ASYNC_WRBKS,
    FDF_CACHE_STAT_ASYNC_WRBK_FAILS,
    FDF_CACHE_STAT_CACHE_MISSES,
    FDF_CACHE_STAT_CACHE_HITS,
    FDF_CACHE_STAT_L1_ENTRIES,
    FDF_CACHE_STAT_L1_HITS,
    FDF_CACHE_STAT_L1_MISSES,
    FDF_CACHE_STAT_L1_WRITES,
    FDF_CACHE_STAT_L1_OBJECTS,
    FDF_CACHE_STAT_L1_LEAVES,
    FDF_CACHE_STAT_L1_NLEAVES,
    FDF_CACHE_STAT_L1_OVERFLOW,
    FDF_CACHE_STAT_BT_NUM_OBJS,
    FDF_CACHE_STAT_BT_LEAVES,
    FDF_CACHE_STAT_BT_NLEAVES,
    FDF_CACHE_STAT_BT_OVERFLOW_NODES,
    FDF_CACHE_STAT_BT_LEAVE_BYTES,
    FDF_CACHE_STAT_BT_NLEAVE_BYTES,
    FDF_CACHE_STAT_BT_OVERFLOW_BYTES,
    FDF_CACHE_STAT_BT_EVICT_BYTES,
    FDF_CACHE_STAT_BT_SPLITS,
    FDF_CACHE_STAT_BT_LMERGES,
    FDF_CACHE_STAT_BT_RMERGES,
    FDF_CACHE_STAT_BT_LSHIFTS,
    FDF_CACHE_STAT_BT_RSHIFTS,
    FDF_CACHE_STAT_BT_EX_TREE_LOCKS,
    FDF_CACHE_STAT_BT_NON_EX_TREE_LOCKS,
    FDF_CACHE_STAT_BT_GET_PATH_LEN,
    FDF_CACHE_STAT_BT_CREATE_PATH_LEN,
    FDF_CACHE_STAT_BT_SET_PATH_LEN,
    FDF_CACHE_STAT_BT_UPDATE_PATH_LEN,
    FDF_CACHE_STAT_BT_DELETE_PATH_LEN,
    FDF_CACHE_STAT_BT_FLUSH_CNT,
    FDF_CACHE_STAT_BT_DELETE_OPT_COUNT,
    FDF_CACHE_STAT_BT_MPUT_IO_SAVED,

    /* request from cache to flash manager */
    FDF_CACHE_STAT_AHCOB,
    FDF_CACHE_STAT_AHCOP,
    FDF_CACHE_STAT_AHCWD,
    FDF_CACHE_STAT_AHDOB,
    FDF_CACHE_STAT_AHFLD,
    FDF_CACHE_STAT_AHGTR,
    FDF_CACHE_STAT_AHGTW,
    FDF_CACHE_STAT_AHPTA,
    FDF_CACHE_STAT_AHSOB,
    FDF_CACHE_STAT_AHSOP,
    /* Request from flash manager to cache */
    FDF_CACHE_STAT_HACRC,
    FDF_CACHE_STAT_HACRF,
    FDF_CACHE_STAT_HACSC,
    FDF_CACHE_STAT_HACSF,
    FDF_CACHE_STAT_HADEC,
    FDF_CACHE_STAT_HADEF,
    FDF_CACHE_STAT_HAFLC,
    FDF_CACHE_STAT_HAFLF,
    FDF_CACHE_STAT_HAGRC,
    FDF_CACHE_STAT_HAGRF,
    FDF_CACHE_STAT_HAGWC,
    FDF_CACHE_STAT_HAGWF,
    FDF_CACHE_STAT_HAPAC,
    FDF_CACHE_STAT_HAPAF,
    FDF_CACHE_STAT_HASTC,
    FDF_CACHE_STAT_HASTF,
    FDF_CACHE_STAT_HFXST,
    FDF_CACHE_STAT_FHXST,
    FDF_CACHE_STAT_FHNXS,
    FDF_CACHE_STAT_HFGFF,
    FDF_CACHE_STAT_FHDAT,
    FDF_CACHE_STAT_FHGTF,
    FDF_CACHE_STAT_HFPTF,
    FDF_CACHE_STAT_FHPTC,
    FDF_CACHE_STAT_FHPTF,
    FDF_CACHE_STAT_HFDFF,
    FDF_CACHE_STAT_FHDEC,
    FDF_CACHE_STAT_FHDEF,
    FDF_CACHE_STAT_HFCIF,
    FDF_CACHE_STAT_FHCRC,
    FDF_CACHE_STAT_FHCRF,
    FDF_CACHE_STAT_HFCZF,
    FDF_CACHE_STAT_HFCRC,
    FDF_CACHE_STAT_HFWRF,
    FDF_CACHE_STAT_HFSET,
    FDF_CACHE_STAT_HFSTC,
    FDF_CACHE_STAT_FHSTF,
    FDF_CACHE_STAT_HFCSH,
    FDF_CACHE_STAT_FHCSC,
    FDF_CACHE_STAT_FHCSF,
    FDF_CACHE_STAT_HFSSH,
    FDF_CACHE_STAT_FHSSC,
    FDF_CACHE_STAT_FHSSF,
    FDF_CACHE_STAT_HFDSH,
    FDF_CACHE_STAT_FHDSC,
    FDF_CACHE_STAT_FHDSF,
    FDF_CACHE_STAT_HFGLS,
    FDF_CACHE_STAT_FHGLC,
    FDF_CACHE_STAT_FHGLF,
    FDF_CACHE_STAT_HFGIC,
    FDF_CACHE_STAT_FHGIC,
    FDF_CACHE_STAT_FHGIF,
    FDF_CACHE_STAT_HFGBC,
    FDF_CACHE_STAT_FHGCC,
    FDF_CACHE_STAT_FHGCF,
    FDF_CACHE_STAT_HFGSN,
    FDF_CACHE_STAT_HFGCS,
    FDF_CACHE_STAT_FHGSC,
    FDF_CACHE_STAT_FHGSF,
    FDF_CACHE_STAT_HFSRR,
    FDF_CACHE_STAT_FHSRC,
    FDF_CACHE_STAT_FHSRF,
    FDF_CACHE_STAT_HFSPR,
    FDF_CACHE_STAT_FHSPC,
    FDF_CACHE_STAT_FHSPF,
    FDF_CACHE_STAT_HFFLA,
    FDF_CACHE_STAT_FHFLC,
    FDF_CACHE_STAT_FHFLF,
    FDF_CACHE_STAT_HFRVG,
    FDF_CACHE_STAT_FHRVC,
    FDF_CACHE_STAT_FHRVF,
    FDF_CACHE_STAT_HFNOP,
    FDF_CACHE_STAT_FHNPC,
    FDF_CACHE_STAT_FHNPF,
    FDF_CACHE_STAT_HFOSH,
    FDF_CACHE_STAT_FHOSC,
    FDF_CACHE_STAT_FHOSF,
    FDF_CACHE_STAT_HFFLS,
    FDF_CACHE_STAT_FHFCC,
    FDF_CACHE_STAT_FHFCF,
    FDF_CACHE_STAT_HFFIV,
    FDF_CACHE_STAT_FHFIC,
    FDF_CACHE_STAT_FHFIF,
    FDF_CACHE_STAT_HFINV,
    FDF_CACHE_STAT_FHINC,
    FDF_CACHE_STAT_FHINF,
    FDF_CACHE_STAT_HFFLC,
    FDF_CACHE_STAT_FHLCC,
    FDF_CACHE_STAT_FHLCF,
    FDF_CACHE_STAT_HFFLI,
    FDF_CACHE_STAT_FHLIC,
    FDF_CACHE_STAT_FHLIF,
    FDF_CACHE_STAT_HFINC,
    FDF_CACHE_STAT_FHCIC,
    FDF_CACHE_STAT_FHCIF,
    FDF_CACHE_STAT_EOK,
    FDF_CACHE_STAT_EPERM,
    FDF_CACHE_STAT_ENOENT,
    FDF_CACHE_STAT_EDATASIZE,
    FDF_CACHE_STAT_ESTOPPED,
    FDF_CACHE_STAT_EBADCTNR,
    FDF_CACHE_STAT_EDELFAIL,
    FDF_CACHE_STAT_EAGAIN,
    FDF_CACHE_STAT_ENOMEM,
    FDF_CACHE_STAT_EACCES,
    FDF_CACHE_STAT_EINCONS,
    FDF_CACHE_STAT_EBUSY,
    FDF_CACHE_STAT_EEXIST,
    FDF_CACHE_STAT_EINVAL,
    FDF_CACHE_STAT_EMFILE,
    FDF_CACHE_STAT_ENOSPC,
    FDF_CACHE_STAT_ENOBUFS,
    FDF_CACHE_STAT_ESTALE,
    FDF_CACHE_STAT_EDQUOT,
    FDF_CACHE_STAT_RMT_EDELFAIL,
    FDF_CACHE_STAT_RMT_EBADCTNR,
    FDF_CACHE_STAT_HASH_BUCKETS,
    FDF_CACHE_STAT_NUM_SLABS,
    FDF_CACHE_STAT_NUM_ELEMENTS,
    FDF_CACHE_STAT_MAX_SIZE,
    FDF_CACHE_STAT_CURR_SIZE,
    FDF_CACHE_STAT_CURR_SIZE_WKEYS,
    FDF_CACHE_STAT_NUM_MODIFIED_OBJS,
    FDF_CACHE_STAT_NUM_MODIFIED_OBJS_WKEYS,
    FDF_CACHE_STAT_NUM_MODIFIED_OBJS_FLUSHED,
    FDF_CACHE_STAT_NUM_MODIFIED_OBJS_BGFLUSHED,
    FDF_CACHE_STAT_NUM_PENDING_REQS,
    FDF_CACHE_STAT_NUM_MODIFIED_OBJC_REC,
    FDF_CACHE_STAT_BGFLUSH_PROGRESS,
    FDF_CACHE_STAT_NUM_BGFLUSH,
    FDF_CACHE_STAT_NUM_FLUSH_PARALLEL,
    FDF_CACHE_STAT_NUM_BGFLUSH_PARALLEL,
    FDF_CACHE_STAT_BGFLUSH_WAIT,
    FDF_CACHE_STAT_MODIFIED_PCT,
    FDF_CACHE_STAT_NUM_APP_BUFFERS,
    FDF_CACHE_STAT_NUM_CACHE_OPS_PROG,
    FDF_CACHE_STAT_NUM_FDBUFFER_PROCESSED,
    FDF_CACHE_STAT_NUM_RESP_PROCESSED ,
    FDF_N_CACHE_STATS
} FDF_cache_stat_t;

typedef enum {
    FDF_FLASH_STATS_NUM_OBJS,
    FDF_FLASH_STATS_NUM_CREATED_OBJS,
    FDF_FLASH_STATS_NUM_EVICTIONS,
    FDF_FLASH_STATS_NUM_HASH_EVICTIONS,
    FDF_FLASH_STATS_NUM_INVAL_EVICTIONS,
    FDF_FLASH_STATS_NUM_SOFT_OVERFLOWS,
    FDF_FLASH_STATS_NUM_HARD_OVERFLOWS,
    FDF_FLASH_STATS_GET_HASH_COLLISION,
    FDF_FLASH_STATS_SET_HASH_COLLISION,
    FDF_FLASH_STATS_NUM_OVERWRITES,
    FDF_FLASH_STATS_NUM_OPS,
    FDF_FLASH_STATS_NUM_READ_OPS,
    FDF_FLASH_STATS_NUM_GET_OPS,
    FDF_FLASH_STATS_NUM_PUT_OPS,
    FDF_FLASH_STATS_NUM_DEL_OPS,
    FDF_FLASH_STATS_GET_EXIST_CHECKS,
    FDF_FLASH_STATS_NUM_FULL_BUCKETS,
    FDF_FLASH_STATS_PENDING_IOS,
    FDF_FLASH_STATS_SPACE_ALLOCATED,
    FDF_FLASH_STATS_SPACE_CONSUMED,

    FDF_FLASH_STATS_SLAB_GC_SEGMENTS_COMPACTED,
    FDF_FLASH_STATS_SLAB_GC_SEGMENTS_FREED,
    FDF_FLASH_STATS_SLAB_GC_SLABS_RELOCATED,
    FDF_FLASH_STATS_SLAB_GC_BLOCKS_RELOCATED,
    FDF_FLASH_STATS_SLAB_GC_RELOCATE_ERRORS,
    FDF_FLASH_STATS_SLAB_GC_SIGNALLED,
    FDF_FLASH_STATS_SLAB_GC_SIGNALLED_SYNC,
    FDF_FLASH_STATS_SLAB_GC_WAIT_SYNC,
    FDF_FLASH_STATS_SLAB_GC_SEGMENTS_CANCELLED,
    FDF_FLASH_STATS_NUM_FREE_SEGMENTS,

    FDF_N_FLASH_STATS
} FDF_flash_stat_t;

typedef struct {
	uint64_t		 n_accesses[FDF_N_ACCESS_TYPES];
	uint64_t		 flash_stats[FDF_N_FLASH_STATS];
	uint64_t		 cache_stats[FDF_N_CACHE_STATS];
	FDF_histo_t		 key_size_histo;
	FDF_histo_t		 data_size_histo;
	FDF_histo_t		 access_time_histo[FDF_N_ACCESS_TYPES];
} FDF_stats_t;

#if 0
typedef struct {
	uint64_t				size_kb;
	FDF_boolean_t			persistent;
    FDF_boolean_t			evicting;
	FDF_boolean_t			writethru;
	FDF_durability_level_t	durability_level;
} FDF_container_props_t;

typedef struct {
	uint64_t				current_size;
	uint64_t				num_obj;
	FDF_boolean_t			fifo_mode;
    FDF_cguid_t             cguid;
    uint32_t                num_shards;
	FDF_boolean_t			async_writes;
} FDF_internal_container_props_t;
#else
typedef struct {
    uint64_t                size_kb;
    FDF_boolean_t           fifo_mode;
    FDF_boolean_t           persistent;
    FDF_boolean_t           evicting;
    FDF_boolean_t           writethru;
    FDF_boolean_t           async_writes;
    FDF_durability_level_t  durability_level;
    FDF_cguid_t             cguid;
    uint64_t                cid;
    uint32_t                num_shards;
} FDF_container_props_t;
#endif

	
#define FDF_CTNR_CREATE   1
#define FDF_CTNR_RO_MODE  2
#define FDF_CTNR_RW_MODE  4

typedef enum {
    FDF_WRITE_MUST_NOT_EXIST    = 1,
    FDF_WRITE_MUST_EXIST    	= 2,
} FDF_write_mode_t;

typedef struct {
    char            *key;
    uint32_t         key_len;
    char            *data;
    uint64_t         data_len;
    FDF_time_t       current;
    FDF_time_t       expiry;
} FDF_readobject_t;

typedef struct {
    char            *key;
    uint32_t         key_len;
    char            *data;
    uint64_t         data_len;
    FDF_time_t       current;
    FDF_time_t       expiry;
} FDF_writeobject_t;

/*
 *  Function used to compare keys
 *  to determine ordering in the index.
 *
 *  Returns: -1 if key1 comes before key2
 *            0 if key1 is the same as key2
 *            1 if key1 comes after key2
 */
typedef int (FDF_cmp_fn_t)(void     *index_data, //  opaque user data
                           char     *key1,       //  first secondary key
                           uint32_t  keylen1,    //  length of first secondary key
                           char     *key2,       //  second secondary key
                           uint32_t  keylen2);   //  length of second secondary key

typedef struct FDF_container_meta_s {
	FDF_cmp_fn_t   *sort_cmp_fn;             // compare function for key
	void           *cmp_data;                // Any data to provide for cmp
} FDF_container_meta_t;

struct FDF_state;
struct FDF_thread_state;
struct FDF_iterator;


/*
 * Get a FDF property.
 */
const char *FDFGetProperty(const char *key, const char *def);


/**
 * @brief set FDF property
 *
 * @param propery <IN> property name
 * @param value <IN> pointer to value
 * 
 */
void FDFSetProperty(
	const char* property,
	const char* value
	);

/**
 * @brief Load properties from specified file
 *
 * @param proper_file <IN> properties file
 * @return FDF_SUCCESS on success
 * 
 */
FDF_status_t FDFLoadProperties(
	const char *prop_file
	);

/**
 * @brief FDF initialization
 *
 * @param fdf_state <OUT> FDF state variable
 * @param prop_file <IN> FDF property file or NULL
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFInit(
	struct FDF_state	**fdf_state
	);

/**
 * @brief FDF per thread state initialization
 *
 * @param fdf_state <IN> FDF state variable
 * @param thd_state <OUT> FDF per thread state variable
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFInitPerThreadState(
	struct FDF_state		 *fdf_state,
	struct FDF_thread_state	**thd_state
	);

/**
 * @brief FDF release per thread state initialization
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFReleasePerThreadState(
	struct FDF_thread_state	**thd_state
	);

/**
 * @brief FDF shutdown
 *
 * @param fdf_state <IN> FDF state variable
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFShutdown(
	struct FDF_state	*fdf_state
	);

/**
 * @brief FDF load default container properties
 *
 * @param props <IN> FDF container properties pointer
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFLoadCntrPropDefaults(
	FDF_container_props_t *props
	);

 /**
 * @brief Create and open a virtual container.
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cname <IN> container name
 * @param properties <IN> container properties
 * @param flags <IN> container open options
 * @param cguid <OUT> container GUID
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFOpenContainer(
	struct FDF_thread_state	*fdf_thread_state, 
	char					*cname, 
	FDF_container_props_t 	*properties, 
	uint32_t			 	 flags,
	FDF_cguid_t				*cguid
	);

 /**
 * @brief Create and open a virtual container.
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cname <IN> container name
 * @param properties <IN> container properties
 * @param flags <IN> container open options
 * @param cguid <OUT> container GUID
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFOpenContainerSpecial(
	struct FDF_thread_state	  *fdf_thread_state, 
	char                      *cname, 
	FDF_container_props_t     *properties, 
	uint32_t                  flags,
	FDF_container_meta_t      *cmeta,
	FDF_cguid_t               *cguid
	);

/**
 * @brief Close a virtual container.
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container CGUID
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFCloseContainer(
	struct FDF_thread_state *fdf_thread_state,
	FDF_cguid_t				 cguid
	);

/**
 * @brief Delete a virtual container
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container CGUID
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFDeleteContainer(
	struct FDF_thread_state *fdf_thread_state,
	FDF_cguid_t				 cguid
	);

/**
 * @brief Get container list
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies.
 * @param cguids  <OUT> pointer to container GUID array
 * @param n_cguids <OUT> pointer to container GUID count
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFGetContainers(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t             *cguids,
	uint32_t                *n_cguids
	);

/**
 * @brief Get container properties
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param pprops <IN> pointer to structure into which to copy properties
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFGetContainerProps(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t            	 cguid,
	FDF_container_props_t	*pprops
	);

/**
 * @brief Set container properties
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param pprops <IN> pointer to structure into which to copy properties
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFSetContainerProps(
	struct FDF_thread_state 	*fdf_thread_state,
	FDF_cguid_t              	 cguid,
	FDF_container_props_t   	*pprops
	);

/**
 *  @brief Get a copy of an object for read-only  access. Return its current expiry time.
 *
 *  Get an object and copy it into an FDF-allocated buffer. The application
 *  only intends to read the object. The current expiry time is returned.
 *
 *  @param sdf_thread_state <IN> The FDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param data <IN> Variable in which to return a pointer to the object (in a buffer
 *  allocated by FDF; it must be freed by the application with a call
 *  to FDFFreeObjectBuffer).
 *  @param datalen <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *
 *  @return FDF_SUCCESS: operation completed successfully.
 *          FDF_BAD_CONTEXT: the provided context is invalid.
 *          FDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          FDF_OBJECT_UNKNOWN: the object does not exist.
 *          FDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          FDF_FAILURE: operation failed.
 */
FDF_status_t FDFReadObject(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen,
	char                     **data,
	uint64_t                 *datalen
	);

/**
 *  @brief Get a copy of an object for read-only  access. Return its current expiry time.
 *
 *  Get an object and copy it into an FDF-allocated buffer. The application
 *  only intends to read the object. The current expiry time is returned.
 *
 *  @param fdf_thread_state <IN> The FDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param robj <IN> Identity of a read object structure
 *
 *  @return FDF_SUCCESS: operation completed successfully.
 *          FDF_BAD_CONTEXT: the provided context is invalid.
 *          FDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          FDF_OBJECT_UNKNOWN: the object does not exist.
 *          FDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          FDF_FAILURE: operation failed.
 */
FDF_status_t FDFReadObjectExpiry(
    struct FDF_thread_state  *fdf_thread_state,
    FDF_cguid_t               cguid,
    FDF_readobject_t         *robj
    );


/**
 * @brief Free an object buffer
 *
 * @param buf <IN> object buffer
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFFreeBuffer(
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
 *  @param fdf_state <IN> The FDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param datalen <IN> Size of object.
 *  @param data <IN> Pointer to application buffer from which to copy data.
 *  @param flags <IN> create/update flags
 *
 *  @return FDF_SUCCESS: operation completed successfully.
 *          FDF_BAD_CONTEXT: the provided context is invalid.
 *          FDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          FDF_OUT_OF_MEM: there is insufficient memory/flash.
 *          FDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          FDF_FAILURE: operation failed.
 */
FDF_status_t FDFWriteObject(
	struct FDF_thread_state  *sdf_thread_state,
	FDF_cguid_t          cguid,
	char                *key,
	uint32_t             keylen,
	char                *data,
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
 *  @param fdf_thread_state <IN> The FDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param wobj <IN> Identity of a write object structure
 *  @param flags <IN> create/update flags
 *
 *  @return FDF_SUCCESS: operation completed successfully.
 *          FDF_BAD_CONTEXT: the provided context is invalid.
 *          FDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          FDF_OUT_OF_MEM: there is insufficient memory/flash.
 *          FDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          FDF_FAILURE: operation failed.
 */
FDF_status_t FDFWriteObjectExpiry(
    struct FDF_thread_state  *fdf_thread_state,
    FDF_cguid_t               cguid,
    FDF_writeobject_t        *wobj,
    uint32_t                  flags
    );

/**
 *  @brief Delete an object, but check for expiry first.
 *
 *  Delete an object, but check for expiry first.
 *
 *  @param fdf_thread_state <IN> The FDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *
 *  @return FDF_SUCCESS: operation completed successfully.
 *          FDF_BAD_CONTEXT: the provided context is invalid.
 *          FDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          FDF_OBJECT_UNKNOWN: the object does not exist.
 *          FDF_FAILURE: operation failed.
 */
FDF_status_t FDFDeleteObject(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen
	);

/**
 * @brief Enumerate container objects
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid  <IN> container global identifier
 * @param iterator <IN> enumeration iterator
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFEnumerateContainerObjects(
	struct FDF_thread_state *fdf_thread_state,
	FDF_cguid_t              cguid,
	struct FDF_iterator    **iterator
	);

/**
 * @brief Container object enumration iterator
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param iterator <IN> enumeration iterator
 * @param cguid  <IN> container global identifier
 * @param key <OUT> pointer to key variable
 * @param keylen <OUT> pointer to key length variable
 * @param data <OUT> pointer to data variable
 * @param datalen <OUT> pointer to data length variable
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFNextEnumeratedObject(
	struct FDF_thread_state *fdf_thread_state,
	struct FDF_iterator     *iterator,
	char                    **key,
	uint32_t                *keylen,
	char                    **data,
	uint64_t                *datalen
	);

/**
 * @brief Terminate enumeration
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param iterator <IN> enumeration iterator
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFFinishEnumeration(
	struct FDF_thread_state *fdf_thread_state,
	struct FDF_iterator     *iterator
	);

/**
 *  @brief Force modifications of an object to primary storage.
 *
 *  Flush any modified contents of an object to its backing store
 *  (as determined by its container type). For coherent containers,
 *  this is a global operation that applies to any cache or buffer
 *  in the FDF cluster. For non-coherent containers, this only applies
 *  to the local cache.
 *
 *  @param fdf_thread_state <IN> The FDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *
 *  @return FDF_SUCCESS: operation completed successfully.
 *          FDF_BAD_CONTEXT: the provided context is invalid.
 *          FDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          FDF_OBJECT_UNKNOWN: the object does not exist.
 *          FDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          FDF_FAILURE: operation failed.
 */
FDF_status_t FDFFlushObject(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen
	);

/**
 * @brief Flush container
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies.
 * @param cguid  <IN> container global identifier
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFFlushContainer(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t               cguid
	);

/**
 * @brief Flush the cache
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies.
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFFlushCache(
	struct FDF_thread_state  *fdf_thread_state
	);

/**
 * @brief Get FDF statistics
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param stats <OUT> pointer to statistics return structure
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFGetStats(
	struct FDF_thread_state *fdf_thread_state,
	FDF_stats_t             *stats
	);

/**
 * @brief Get per container statistics
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid  <IN> container global identifier
 * @param stats <OUT> pointer to statistics return structure
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFGetContainerStats(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t				 cguid,
	FDF_stats_t     		*stats
	);

/**
 * @brief Get error string for given error code
 *
 * @param errno FDF error number
 * @return  error string
 */
char *FDFStrError(FDF_status_t fdf_errno);

/**
 * @brief Start transaction
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @return FDF_SUCCESS on success
 *         FDF_TRANS_LEVEL_EXCEEDED if transaction is nested too deeply
 *         FDF_OUT_OF_MEM if memory exhausted
 *         FDF_FAILURE for error unspecified
 */
FDF_status_t FDFTransactionStart(
	struct FDF_thread_state	*fdf_thread_state
	);

/**
 * @brief Commit transaction
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @return FDF_SUCCESS on success
 *         FDF_FAILURE_NO_TRANS if there is no active transaction in the current thread
 *         FDF_TRANS_ABORTED if transaction aborted due to excessive size or internal error
 */
FDF_status_t FDFTransactionCommit(
	struct FDF_thread_state	*fdf_thread_state
	);

/**
 * @brief Roll back transaction
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @return FDF_SUCCESS on success
 *         FDF_FAILURE_NO_TRANS if there is no active transaction in the current thread
 *         FDF_TRANS_ABORTED if transaction aborted due to excessive size or internal error
 */
FDF_status_t FDFTransactionRollback(
	struct FDF_thread_state	*fdf_thread_state
	);

/**
 * @brief Quit a transaction
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @return FDF_SUCCESS on success
 *         FDF_FAILURE_NO_TRANS if there is no active transaction in the current thread
 */
FDF_status_t FDFTransactionQuit(
	struct FDF_thread_state	*fdf_thread_state
	);

/**
 * @brief ID of current transaction
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @return Non-zero transaction ID on success
 *         Zero if there is no active transaction in the current thread
 */
uint64_t FDFTransactionID(
	struct FDF_thread_state	*fdf_thread_state
	);

/**
 * @brief Return version of FDF
 *
 * @param  Address of pointer to hold the version string 
 * @return String having the versions
 *         NULL if failed internally
 */
FDF_status_t FDFGetVersion(
	char **str
	);

#define N_ENTRIES_TO_MALLOC    100
#define N_ITERATORS_TO_MALLOC  100

struct FDFCMapIterator;

struct FDFTLMapBucket;

typedef struct FDFTLMapEntry {
    char                  *contents;
    uint64_t               datalen;
    int32_t                refcnt;
    char                  *key;
    uint32_t               keylen;
    struct FDFTLMapEntry  *next;
    struct FDFTLMapEntry  *next_lru;
    struct FDFTLMapEntry  *prev_lru;
    struct FDFTLMapBucket *bucket;
} FDFTLMapEntry_t;

typedef struct FDFTLMapBucket {
    struct FDFTLMapEntry *entry;
} FDFTLMapBucket_t;

typedef struct FDFTLIterator {
    uint64_t                enum_bucket;
    FDFTLMapEntry_t        *enum_entry;
    struct FDFTLIterator   *next;
} FDFTLIterator_t;

typedef struct FDFTLMap {
    uint64_t          nbuckets;
    uint64_t          max_entries;
    uint64_t          n_entries;
    char              use_locks;
    FDFTLMapBucket_t *buckets;
    pthread_mutex_t   mutex;
    pthread_mutex_t   enum_mutex;
    FDFTLMapEntry_t  *lru_head;
    FDFTLMapEntry_t  *lru_tail;
    void              (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen);
    void             *replacement_callback_data;
    uint32_t          NEntries;
    uint32_t          NUsedEntries;
    FDFTLMapEntry_t  *FreeEntries;
    uint32_t          NIterators;
    uint32_t          NUsedIterators;
    struct FDFTLIterator *FreeIterators;
} FDFTLMap_t;

struct FDFTLMap* FDFTLMapInit(uint64_t nbuckets, uint64_t max_entries, char use_locks, void (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen), void *replacement_callback_data);
void FDFTLMapDestroy(struct FDFTLMap *pm);
void FDFTLMapClear(struct FDFTLMap *pm);
struct FDFTLMapEntry* FDFTLMapCreate(struct FDFTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
struct FDFTLMapEntry* FDFTLMapUpdate(struct FDFTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
struct FDFTLMapEntry* FDFTLMapSet(struct FDFTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, char **old_pdata, uint64_t *old_datalen);
struct FDFTLMapEntry* FDFTLMapGet(struct FDFTLMap *pc, char *key, uint32_t keylen, char** data, uint64_t *pdatalen);
int FDFTLMapIncrRefcnt(struct FDFTLMap *pm, char *key, uint32_t keylen);
void FDFTLMapCheckRefcnts(struct FDFTLMap *pm);
int FDFTLMapRelease(struct FDFTLMap *pm, char *key, uint32_t keylen);
int FDFTLMapReleaseEntry(struct FDFTLMap *pm, struct FDFTLMapEntry *pme);
struct FDFTLIterator* FDFTLMapEnum(struct FDFTLMap *pm);
void FDFTLFinishEnum(struct FDFTLMap *pm, struct FDFTLIterator *iterator);
int FDFTLMapNextEnum(struct FDFTLMap *pm, struct FDFTLIterator *iterator, char **key, uint32_t *keylen, char **data, uint64_t *datalen);
int FDFTLMapDelete(struct FDFTLMap *pm, char *key, uint32_t keylen);

/************ All Range Query related structures and functions ***************/

typedef enum {
	FDF_RANGE_BUFFER_PROVIDED      = 1<<0,  // buffers for keys and data provided by application
	FDF_RANGE_ALLOC_IF_TOO_SMALL   = 1<<1,  // if supplied buffers are too small, FDF will allocate
	FDF_RANGE_SEQNO_LE             = 1<<5,  // only return objects with seqno <= end_seq
	FDF_RANGE_SEQNO_GT_LE          = 1<<6,  // only return objects with start_seq < seqno <= end_seq

	FDF_RANGE_START_GT             = 1<<7,  // keys must be >  key_start
	FDF_RANGE_START_GE             = 1<<8,  // keys must be >= key_start
	FDF_RANGE_START_LT             = 1<<9,  // keys must be <  key_start
	FDF_RANGE_START_LE             = 1<<10, // keys must be <= key_start

	FDF_RANGE_END_GT               = 1<<11, // keys must be >  key_end
	FDF_RANGE_END_GE               = 1<<12, // keys must be >= key_end
	FDF_RANGE_END_LT               = 1<<13, // keys must be <  key_end
	FDF_RANGE_END_LE               = 1<<14, // keys must be <= key_end

	FDF_RANGE_KEYS_ONLY            = 1<<15, // only return keys (data is not required)

	FDF_RANGE_PRIMARY_KEY          = 1<<16, // return primary keys in secondary index query
	FDF_RANGE_INDEX_USES_DATA      = 1<<17, // Indicates that secondary index key 
	                                        // is derived from object data
} FDF_range_enums_t;

/*
 *
 * Type definition for function that determines if we are allowed to return
 * this key as part of a range query.
 *
 * Return true (1) if the key is allowed and false (0) otherwise.
 */
typedef int (FDF_allowed_fn_t)(void *context_data,  // context data (opaque)
                               char *key,           // key to check if allowed
                               uint32_t len);       // length of the key

typedef struct FDF_range_meta {
	FDF_range_enums_t flags;         // flags controlling type of range query (see above)
	uint32_t          keybuf_size;   // size of application provided key buffers (if applicable)
	uint64_t          databuf_size;  // size of application provided data buffers (if applicable)
	char              *key_start;    // start key
	uint32_t          keylen_start;  // length of start key
	char              *key_end;      // end key
	uint32_t          keylen_end;    // length of end key
	uint64_t          start_seq;     // starting sequence number (if applicable)
	uint64_t          end_seq;       // ending sequence number (if applicable)
	FDF_cmp_fn_t      *class_cmp_fn; // Fn to cmp two keys are in same equivalence class
	FDF_allowed_fn_t  *allowed_fn;   // Fn to check if this key is allowed to put in range result
	void              *cb_data;      // Any data to be passed for allowed function
} FDF_range_meta_t;

struct           FDF_cursor;       // opaque cursor handle
typedef uint64_t FDF_indexid_t;    // persistent opaque index handle

#define FDF_RANGE_PRIMARY_INDEX     0

/*
 * Multiple objs put info.
 */
typedef struct {
    uint32_t flags;
    uint32_t key_len;
    uint64_t data_len;
    char *key;
    char *data;
} FDF_obj_t;

/*
 * FDFRangeUpdate Callback.
 */
typedef bool (* FDF_range_update_cb_t) (char *key, uint32_t keylen, char *data, uint32_t datalen,
				        void *callback_args, char **new_data, uint32_t *new_datalen);

/* Start an index query.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_FAILURE if unsuccessful
 */
FDF_status_t FDFGetRange(struct FDF_thread_state *thrd_state, //  client thread FDF context
                         FDF_cguid_t              cguid,      //  container
                         FDF_indexid_t            indexid,    //  handle for index to use (use PRIMARY_INDEX for primary)
                         struct FDF_cursor      **cursor,     //  returns opaque cursor for this query
                         FDF_range_meta_t        *meta);      //  query attributes (see above)

typedef enum {
	FDF_RANGE_STATUS_NONE       = 0,
	FDF_RANGE_DATA_SUCCESS      = 1,
	FDF_KEY_BUFFER_TOO_SMALL    = 2,
	FDF_DATA_BUFFER_TOO_SMALL   = 4,
	FDF_RANGE_PAUSED            = 8
} FDF_range_status_t;

typedef struct FDF_range_data {
	FDF_range_status_t  status;           // status
	char         *key;              // index key value
	uint32_t      keylen;           // index key length
	char         *data;             // data
	uint64_t      datalen;          // data length
	uint64_t      seqno;            // sequence number for last update
	uint64_t      syndrome;         // syndrome for key
	char         *primary_key;      // primary key value (if required)
	uint32_t      primary_keylen;   // primary key length (if required)
} FDF_range_data_t;

/* Gets next n_in keys in the indexed query.
 *
 * The 'values' array must be allocated by the application, and must hold up to
 * 'n_in' entries.
 * If the BUFFER_PROVIDED flag is set, the key and data fields in 'values' must be
 * pre-populated with buffers provided by the application (with sizes that were
 * specified in 'meta' when the index query was started).  If the application provided
 * buffer is too small for returned item 'i', the status for that item will 
 * be FDF_BUFFER_TOO_SMALL; if the ALLOC_IF_TOO_SMALL flag is set, FDF will allocate
 * a new buffer whenever the provided buffer is too small.
 * 
 * If the SEQNO_LE flag is set, only items whose sequence number is less than or
 * equal to 'end_seq' from FDF_range_meta_t above are returned.
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
 * Returns: FDF_SUCCESS    if all statuses are successful
 *          FDF_QUERY_DONE if query is done (n_out will be set to 0)
 *          FDF_FAILURE    if one or more of the key fetches fails (see statuses for the
 *                         status of each fetched object)
 * 
 * statuses[i] returns: FDF_SUCCESS if the i'th data item was retrieved successfully
 *                      FDF_BUFFER_TOO_SMALL  if the i'th buffer was too small to retrieve the object
 */
FDF_status_t
FDFGetNextRange(struct FDF_thread_state *thrd_state,  //  client thread FDF context
                struct FDF_cursor       *cursor,      //  cursor for this indexed search
                int                      n_in,        //  size of 'values' array
                int                     *n_out,       //  number of items returned
                FDF_range_data_t        *values);     //  array of returned key/data values

/* End an index query.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_UNKNOWN_CURSOR if the cursor is invalid
 */
FDF_status_t 
FDFGetRangeFinish(struct FDF_thread_state *thrd_state, 
                  struct FDF_cursor *cursor);
FDF_status_t
FDFMPut(struct FDF_thread_state *fdf_ts,
        FDF_cguid_t cguid,
        uint32_t num_objs,
        FDF_obj_t *objs,
	uint32_t flags,
	uint32_t *objs_written);

FDF_status_t
FDFRangeUpdate(struct FDF_thread_state *fdf_thread_state, 
	       FDF_cguid_t cguid,
	       char *range_key,
	       uint32_t range_key_len,
	       FDF_range_update_cb_t callback_func,
	       void * callback_args,	
	       uint32_t *objs_updated);

/*
 * @brief Create a snapshot for a container  
 * 
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param snap_seq <OUT> sequence number of snapshot
 * @return FDF_SUCCESS if successful
 *         FDF_TOO_MANY_SNAPSHOTS if snapshot limit is reached
 */
FDF_status_t
FDFCreateContainerSnapshot(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t		cguid,
	uint64_t		*snap_seq
	);

/*
 * @brief Delete a snapshot
 * 
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param snap_seq <IN> snapshot to be deleted
 * @return FDF_SUCCESS if successful
 *         FDF_SNAPSHOT_NOT_FOUND if no snapshot for snap_seq is found
 */
FDF_status_t
FDFDeleteContainerSnapshot(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t		cguid,
	uint64_t		snap_seq
	);

/*
 * @brief Get a list of all current snapshots
 *
 * Array returned in snap_seqs is allocated by FDF and must be freed by
 * application.
 * 
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param n_snapshots <OUT> number of snapshots retrieved
 * @param snap_seqs <OUT> retrieved snapshots
 * @return FDF_SUCCESS if successful
 *         FDF_xxxzzz if snap_seqs cannot be allocated
 */
FDF_status_t
FDFGetContainerSnapshots(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t		cguid,
	uint32_t		n_snapshots,
	uint64_t		snap_seqs
	);

/*
 * @brief Check that FDF operations are allowed (e.g., not in shutdown).
 *
 * @return FDF_SUCCESS if operations are allowed.
*/
FDF_status_t
FDFOperationAllowed( void );

#if 0
/*********************************************************
 *
 *    SECONDARY INDEXES
 *
 *********************************************************/

/*
 *   Function used to extract secondary key
 *   from primary key and/or data.
 *   This function must use FDFGetSecondaryKeyBuffer() below
 *   to allocate a buffer for the extracted key.
 *
 *   Returns:  0 if successful
 *             1 if fails (eg: FDFGetSecondaryKeyBuffer() fails)
 */
typedef int (FDF_index_fn_t)(void     *index_data,        //  opaque user data 
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
char * FDFGetSecondaryKeyBuffer(uint32_t len);

typedef struct FDF_index_meta {
    uint32_t        flags;       //  flags (see FDF_range_enums_t)
    FDF_index_fn   *index_fn;    //  function used to extract secondary key
    FDF_cmp_fn     *cmp_fn;      //  function used to compare index values
    void           *index_data;  //  opaque user data for index/cmp functions
} FDF_index_meta_t;

/*
 * Create a secondary index
 * Index creation is done synchronously: the function does
 * not return until the index is fully created.
 * Secondary index creation in crash-safe: if FDF crashes while
 * index creation is in progress, index creation will be completed
 * when FDF restarts.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_xxxzzz if FDF runs out of memory
 *          FDF_xxxzzz if FDF runs out of storage
 */
FDF_status_t
FDFAddSecondaryIndex(struct FDF_thread_state *thrd_state,   //  client thread FDF context
                     FDF_cguid_t              cguid,        //  container in which to add index
                     FDF_index_id_t          *indexid,      //  persistent opaque handle for new index
                     FDF_index_meta_t        *meta);        //  attributes of new index

/*
 * Delete a secondary index
 * 
 * Index deletion is done synchronously: the function does
 * not return until the index is fully deleted.
 * Secondary index deletion is crash-safe: if FDF crashes while
 * index deletion is in progress, index deletion will be completed
 * when FDF restarts.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_INVALID_INDEX if index is invalid
 */
FDF_status_t
FDFDeleteSecondaryIndex(struct FDF_thread_state *thrd_state, //  client thread FDF context
                        FDF_cguid_t              cguid,      //  container in which to add index
                        FDF_indexid_t            indexid);   //  persistent opaque handle of index to delete

/*
 * Get a list of all current secondary indices.
 * Array returned in index_ids is allocated by FDF and
 * must be freed by application.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_xxxzzz if index_ids cannot be allocated
 */
FDF_status_t
FDFGetContainerIndices(struct FDF_thread_state *ts,         //  client thread FDF context
                         FDF_cguid_t            cguid,      //  container
                         uint32_t              *n_indices,  //  returns number of indices
                         FDF_indexid_t         *index_ids); //  returns array of index ids

/*
 * Get attributes for a particular indexid.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_xxxzzz if indexid is invalid
 */
FDF_status_t
FDFGetIndexMeta(struct FDF_thread_state *ts,       //  client thread FDF context
                FDF_cguid_t              cguid,    //  container
                FDF_indexid_t            indexid,  //  index id
                FDF_index_meta_t        *meta);    //  attributes of index

#endif /* If 0 */
#ifdef __cplusplus
}
#endif

#endif // __FDF_H
