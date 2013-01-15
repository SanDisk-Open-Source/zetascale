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

#include "common/fdftypes.h"
#include "common/fdfstats.h"

#define CONTAINER_NAME_MAXLEN 64
/* FDF */
extern char *FDF_Status_Strings[];

typedef struct FDF_state {
    uint64_t           cguid_cntr;

} FDF_state_t;

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

typedef struct {
	uint32_t				size_kb;
	FDF_boolean_t			fifo_mode;
	FDF_boolean_t			persistent;
    FDF_boolean_t			evicting;
	FDF_boolean_t			writethru;
	FDF_boolean_t			async_writes;
	FDF_durability_level_t	durability_level;
	FDF_cguid_t				cguid;
	uint64_t				cid;
	uint32_t				num_shards;
} FDF_container_props_t;
	
#define FDF_CTNR_CREATE   1
#define FDF_CTNR_RO_MODE  2
#define FDF_CTNR_RW_MODE  4

typedef enum {
    FDF_WRITE_MUST_NOT_EXIST    = 1,
    FDF_WRITE_MUST_EXIST    	= 2,
} FDF_write_mode_t;

typedef struct FDF_iterator {
    uint64_t          addr;
    uint64_t          prev_seq;
    uint64_t          curr_seq;
    struct shard     *shard;
    FDF_cguid_t       cguid;
} FDF_iterator_t;


struct FDF_state;
struct FDF_thread_state;
struct FDF_iterator;

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
 * @return 0 on success
 * 
 */
int FDFLoadProperties(
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
 * @brief Create and open a container.
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
 * @brief Close a container.
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
 * @brief Delete a container
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
 *  Get an object and copy it into an SDF-allocated buffer. The application
 *  only intends to read the object. The current expiry time is returned.
 *
 *  @param sdf_thread_state <IN> The FDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param data <IN> Variable in which to return a pointer to the object (in a buffer
 *  allocated by SDF; it must be freed by the application with a call
 *  to SDFFreeObjectBuffer).
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
 *  @param fdf_state <IN> The SDF context for which this operation applies.
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
 *  @brief Delete an object, but check for expiry first.
 *
 *  Delete an object, but check for expiry first.
 *
 *  @param fdf_thread_state <IN> The SDF context for which this operation applies.
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
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
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
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
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
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
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
 *  in the SDF cluster. For non-coherent containers, this only applies
 *  to the local cache.
 *
 *  @param fdf_thread_state <IN> The SDF context for which this operation applies.
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
 * @param fdf_thread_state <IN> The SDF context for which this operation applies.
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFFlushCache(
	struct FDF_thread_state  *fdf_thread_state
	);

/**
 * @brief Get FDF statistics
 *
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
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
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
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

#ifdef __cplusplus
}
#endif

#endif // __FDF_H
