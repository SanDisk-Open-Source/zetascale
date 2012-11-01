/*
 * File:   fdf.h
 * Author: Darryl Ouye
 *
 * Created on October 18, 2012
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

//  use this to adjust include files to be usable for compiling applications
// #define SDF_APP

#ifdef SDF_APP
#define SDFAPI
#endif

#include "common/sdftypes.h"
#include "common/sdfstats.h"
#include "common/fdftypes.h"

#define CONTAINER_NAME_MAXLEN 64


/* FDF */

typedef struct FDF_state {
    uint64_t           cguid_cntr;

} FDF_state_t;


typedef struct {
	uint32_t		 version;
	uint32_t		 n_flash_devices;
	char			*flash_base_name;
	uint32_t		 flash_size_per_device_gb;
	uint32_t		 dram_cache_size_gb;
	uint32_t		 n_cache_partitions;
	FDF_boolean_t	 reformat;
	uint32_t		 max_object_size;
	uint32_t		 max_background_flushes;
	uint32_t		 background_flush_msec;
	uint32_t		 max_outstanding_writes;
	double			 cache_modified_fraction;
	uint32_t		 max_flushes_per_mod_check;
} FDF_config_t;

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
	FDF_FULL_DURABILITY 	= 0,
	FDF_PERIODIC_DURABILITY = 1,
} FDF_durability_level_t;

typedef enum {
    FDF_N_ACCESS_TYPES = 10
} FDF_access_types_t;

typedef enum {
    FDF_N_CACHE_STATS = 10
} FDF_cache_stat_t;

typedef enum {
    FDF_N_FLASH_STATS = 10
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
	FDF_durability_level_t	durability_level;
	FDF_cguid_t				cguid;
	uint32_t				num_shards;
} FDF_container_props_t;
	
typedef enum {
	FDF_CTNR_RW_MODE 	= 0,
	FDF_CTNR_CREATE		= 1,
} FDF_container_mode_t;

typedef enum {
    FDF_WRITE_MUST_NOT_EXIST    = 0,
    FDF_WRITE_MUST_EXIST    	= 1,
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
 * @brief FDF load default container properties
 *
 * @param props <IN> FDF container properties pointer
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFLoadConfigDefaults(
	FDF_config_t	*fdf_config;
	char			*defaults_filename;
	);

/**
 * @brief FDF initialization
 *
 * @param fdf_state <OUT> FDF state variable
 * @param fdf_config <IN> FDF configuration
 * @return FDF_SUCCESS on success
 */
FDF_status_t FDFInit(
	struct FDF_state	**fdf_state, 
	FDF_config_t		 *fdf_config
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

#ifdef __cplusplus
}
#endif

#endif // __FDF_H
