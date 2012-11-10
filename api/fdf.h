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
#ifndef SDFAPI

struct SDF_state;
struct SDF_thread_state;
struct SDF_iterator;

typedef enum {
    SDF_OBJECT_CONTAINER, SDF_BLOCK_CONTAINER,
    SDF_LOG_CONTAINER, SDF_UNKNOWN_CONTAINER,
    SDF_ANY_CONTAINER, SDF_LOCK_CONTAINER, SDF_INVALID_CONTAINER = -1
} SDF_container_type_t;

typedef enum {
    SDF_FULL_DURABILITY = 0,
    SDF_RELAXED_DURABILITY,
    SDF_NO_DURABILITY
} SDF_durability_level_t;

typedef struct {
    /** properties specific to block containers */
    unsigned blockSize;
} SDF_block_container_props_t;

typedef struct {
    /** properties specific to object containers */
} SDF_object_container_props_t;

typedef struct {
    /** properties specific to log containers */
} SDF_log_container_props_t;
    
typedef struct {
    uint32_t owner;
    uint64_t size;    // In KB
    uint32_t num_objs;
    uint64_t  container_id;
} SDF_container_id_props_t;

/** Container type */
typedef struct {
    SDF_container_type_t type;
    SDF_boolean_t persistence;
    SDF_boolean_t caching_container;
    SDF_boolean_t async_writes;
} SDF_container_type_props_t;

typedef enum {
    SDF_READ_MODE,
    SDF_WRITE_MODE,
    SDF_APPEND_MODE,
    SDF_READ_WRITE_MODE,
    SDF_CNTR_MODE_MAX   /*Used to check validity of the container mode*/
} SDF_container_mode_t;

typedef enum {
    SDF_N_ACCESS_TYPES = 10
} SDF_access_types_t;

#define descrChangesNull DescrChangesPtr_sp_null

/*
 * item(upper, lower)
 */
#define SDF_REPLICATION_TYPE_ITEMS() \
    /** @brief No replication */                                               \
    item(SDF_REPLICATION_NONE, none)                                           \
    /**                                                                        \
     * @brief Fan-out only for performance comparisons                         \
     *                                                                         \
     * No coherency, no recovery, nothing.                                     \
     */                                                                        \
    item(SDF_REPLICATION_SIMPLE, simple)                                       \
    /**                                                                        \
     * @brief Single non-replicated meta-data node for test                    \
     *                                                                         \
     * Supports recovery, etc. but with the meta-data node as as single        \
     * point of failure.                                                       \
     */                                                                        \
    item(SDF_REPLICATION_META_SUPER_NODE, super_node)                          \
    /**                                                                        \
     * @brief External source determines replica authority                     \
     *                                                                         \
     * Multiple copies of the meta-data are maintained, with an external       \
     * source determining which is authoritative.  This allows for             \
     * more interesting demonstrations than SDF_REPLICATION_META_SUPER_NODE    \
     * and may be applicable to a subset of customer problems                  \
     */                                                                        \
    item(SDF_REPLICATION_META_EXTERNAL_AUTHORITY, external_authority)          \
    /**                                                                        \
     * @brief Consensus Replication on meta-data                               \
     *                                                                         \
     * This allows for automated switch-over without a single point            \
     * of failure but is vulnerable to sequential failures due to the          \
     * shard granularity of replica authority.                                 \
     */                                                                        \
    item(SDF_REPLICATION_META_CONSENSUS, meta_consensus)                       \
    /**                                                                        \
     * @brief v1 2way replication                                              \
     *                                                                         \
     * This a simple 2-way mutual replication scheme with best efforts to      \
     * avoid a single point of failure and consistency.  Without consensus we  \
     * cannot guarantee that two nodes won't each assume ownership of the same \
     * VIPs.  We also cannot guarantee consistency.                            \
     *                                                                         \
     * The container must be created on both nodes, with the same container    \
     * ID used on both.                                                        \
     *                                                                         \
     * XXX: drew 2009-07-30 For v1, this implies shared creation performed     \
     * both via the replicator and direct paths while the data path is via     \
     * simple replication in sdf/protocol/action                               \
     */                                                                        \
    item(SDF_REPLICATION_V1_2_WAY, v1_2_way)                                   \
    /**                                                                        \
     * @brief v1 N+1 service availability                                      \
     *                                                                         \
     * This is N+1 service availability with best efforts to avoid a single    \
     * point of failure.  Without consensus we cannot guarantee that two nodes \
     * won't each assume ownership of the same VIPs.                           \
     *                                                                         \
     * XXX: drew 2009-07-30 For v1, this implies shared creation performed     \
     * both via the replicator and direct paths while the data path is via     \
     * simple replication in sdf/protocol/action                               \
     */                                                                        \
    item(SDF_REPLICATION_V1_N_PLUS_1, v1_n1_plus_1)                            \
    /**                                                                        \
     * @brief Consensus replication of data                                    \
     *                                                                         \
     * This allows us to survive any sequence of failures as long as           \
     * a majority of nodes exist.                                              \
     */                                                                        \
    item(SDF_REPLICATION_CONSENSUS, consensus)                                 \
    /** @brief Out-of-band value used as return from parsing code */           \
    item(SDF_REPLICATION_INVALID, invalid)

/** @brief Replication type */
typedef enum {
#define item(upper, lower) upper,
    SDF_REPLICATION_TYPE_ITEMS()
#undef item
} SDF_replication_t;

static inline const char *
sdf_replication_to_string(SDF_replication_t replication) {
    switch (replication) {
#define item(upper, lower) \
    case upper: return (#lower);
    SDF_REPLICATION_TYPE_ITEMS()
#undef item
    default:
        return ("invalid");
    }
}

static inline SDF_replication_t
str_to_sdf_replication(const char *in) {
#define item(upper, lower) \
    if (!strcmp(in, #lower)) {                                                 \
        return (upper);                                                        \
    } else
    SDF_REPLICATION_TYPE_ITEMS()
#undef item
    {
        return (SDF_REPLICATION_INVALID);
    }
}

static inline void
sdf_replication_usage() {
    fprintf(stderr, "replication is one of:\n%s",
#define item(upper, lower) "\t" #lower "\n"
            SDF_REPLICATION_TYPE_ITEMS()
#undef item
            );
}

/**
 * Replication
 */
typedef struct {
    /** @brief Turned on or off.  Rest invalid if off  */
    SDF_boolean_t enabled;

    /** @brief Type of replication */
    SDF_replication_t type;

    /** @brief Number of data replicas */
    uint32_t num_replicas;

    /**
     * @brief Number of meta-data replicas
     *
     * 2n + 1 are required to tolerate n failures; so we may have
     * num_replicas = 2, num_meta_replicas = 3.
     */
    uint32_t num_meta_replicas;

    SDF_boolean_t synchronous;


} SDF_replication_props_t;

/** Cache */
typedef struct {
    SDF_boolean_t not_cacheable;
    SDF_boolean_t shared;
    SDF_boolean_t coherent;
    SDF_boolean_t enabled;
    SDF_boolean_t writethru;
    uint32_t size;
    uint32_t max_size;
} SDF_cache_props_t;

/** Shard */
typedef struct {
    SDF_boolean_t enabled;
    uint32_t num_shards;
} SDF_shard_props_t;

/** Structure for container properties */
typedef struct {

    // container configuration
    int master_vnode;  // node that is the master for replication
    
    SDF_container_id_props_t    container_id;
    SDF_cguid_t                 cguid;
    SDF_container_type_props_t  container_type;
    SDF_replication_props_t     replication;
    SDF_cache_props_t           cache;
    SDF_shard_props_t           shard;
    uint32_t                    fifo_mode;
    SDF_durability_level_t      durability_level;
    union {
        SDF_block_container_props_t        block_props;
        SDF_object_container_props_t       object_props;
        SDF_log_container_props_t          log_props;
    } specific;
    
    // Mcd index
    int                         mcd_index;

} SDF_container_props_t;

/** Legacy structure for container properties */
typedef struct {

    int master_vnode;  // node that is the master for replication

    SDF_container_id_props_t container_id;
    SDF_container_type_props_t container_type;
    char hierarchy[8];
    SDF_replication_props_t replication;
    char quota[8];
    char compression[12];
    char encryption[8];
    char migration[8];
    char flush[4];
    SDF_cache_props_t cache;
    char transaction[12];
    char access_control[12];
    char access_hints[8];
    char conflict[8];
    char attributes[12];
    char debug[12];
    SDF_shard_props_t shard;
    uint32_t blockSize;

} SDF_container_props_v1_t;

typedef enum {
    SDF_N_CACHE_STATS = 10
} SDF_cache_stat_t;

typedef enum {
    SDF_N_FLASH_STATS = 10
} SDF_flash_stat_t;

typedef struct {
    uint64_t    n_accesses[SDF_N_ACCESS_TYPES];
    uint64_t    cache_stats[SDF_N_CACHE_STATS];
    uint64_t    flash_stats[SDF_N_FLASH_STATS];
} SDF_stats_t;

/**
 * @brief FDF initialization
 *
 * @param fdf_state <IN> FDF state variable
 * @param argc <IN> program arguments count
 * @param argv <IN> program arguments array
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFInit(
	struct SDF_state **sdf_state, 
	int 		   argc, 
	char 		 **argv
	);

/**
 * @brief FDF per thread state initialization
 *
 * @param fdf_state <IN> FDF thread state variable
 * @return SDF_SUCCESS on success
 */
struct SDF_thread_state *SDFInitPerThreadState(
	struct SDF_state *sdf_state
	);

/**
 * @brief Create a container.
 *
 * @param cname <IN> container name
 * @param properties <IN> container properties
 * @param cguid <IN> container GUID
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFCreateContainer(
	struct SDF_thread_state	*sdf_thread_state, 
	char 		        *cname, 
	SDF_container_props_t 	*properties, 
	SDF_cguid_t		*cguid 
	);

/**
 * @brief Open a container.
 *
 * @param cguid <IN> container GUID
 * @param mode <IN> mode to open the container in (e.g. read/write/append/read-write)
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFOpenContainer(
	struct SDF_thread_state	*sdf_thread_state,
	SDF_cguid_t		 cguid,
	SDF_container_mode_t 	 mode
	);

/**
 * @brief Close a container.
 *
 * @param cguid <IN> container CGUID
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFCloseContainer(
	struct SDF_thread_state *sdf_thread_state,
	SDF_cguid_t		 cguid
	);

/**
 * @brief Delete a container
 *
 * @param cguid <IN> container CGUID
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFDeleteContainer(
	struct SDF_thread_state *sdf_thread_state,
	SDF_cguid_t		 cguid
	);

/**
 * @brief Start accepting accesses to a container
 *
 * @param cguid <IN> container global identifier
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFStartContainer(
	struct SDF_thread_state *sdf_thread_state, 
	SDF_cguid_t 		 cguid
	);
 
/**
 * @brief Stop accepting accesses to a container
 * 
 * @param cguid <IN> container global identifier
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFStopContainer(
	struct SDF_thread_state *sdf_thread_state,
	SDF_cguid_t cguid
	);

/**
 * @brief Get container properties
 *
 * @param cguid  <IN> container global identifier
 * @param pprops <IN> pointer to structure into which to copy properties
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFGetContainerProps(
        struct SDF_thread_state *sdf_thread_state,
        SDF_cguid_t              cguid,
        SDF_container_props_t   *pprops
        );

/**
 * @brief Set container properties
 *
 * @param cguid  <IN> container global identifier
 * @param pprops <IN> pointer to structure into which to copy properties
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFSetContainerProps(
        struct SDF_thread_state *sdf_thread_state,
        SDF_cguid_t              cguid,
        SDF_container_props_t   *pprops
        );

/**
 * @brief Get container list
 *
 * @param cguids  <OUT> pointer to container GUID array
 * @param n_cguids <OUT> pointer to container GUID count
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFGetContainers(
	struct SDF_thread_state	*sdf_thread_state,
	SDF_cguid_t             *cguids,
	uint32_t                *n_cguids
	);

/**
 * @brief Flush container
 *
 * @param cguid  <IN> container global identifier
 * @param current_time <IN> time
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFFlushContainer(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_cguid_t               cguid,
	SDF_time_t                current_time
	);

/**
 *  @brief Get a copy of an object for read-only  access. Return its current expiry time.
 *
 *  Get an object and copy it into an SDF-allocated buffer. The application
 *  only intends to read the object. The current expiry time is returned.
 *
 *  @param sdf_thread_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param data <IN> Variable in which to return a pointer to the object (in a buffer
 *  allocated by SDF; it must be freed by the application with a call
 *  to SDFFreeObjectBuffer).
 *  @param datalen <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  @param current_time <IN> Current time.
 *  @param expiry_time <OUT> Current expiry time for an object.
 *
 *  @return SDF_SUCCESS: operation completed successfully.
 *          SDF_BAD_CONTEXT: the provided context is invalid.
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          SDF_OBJECT_UNKNOWN: the object does not exist.
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          SDF_FAILURE: operation failed.
 */
SDF_status_t SDFGetForReadBufferedObject(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen,
	char                     **data,
	uint64_t                 *datalen,
	SDF_time_t                current_time,
	SDF_time_t               *expiry_time
	);

SDF_status_t SDFFreeBuffer(
                   struct SDF_thread_state  *sdf_thread_state,
		   char                     *data
               );

SDF_status_t SDFGetBuffer(
                   struct SDF_thread_state  *sdf_thread_state,
		   char                     **data,
		   uint64_t                   datalen
               );

SDF_status_t SDFCreateBufferedObject(
                   struct SDF_thread_state  *sdf_thread_state,
		   SDF_cguid_t          cguid,
		   char                *key,
		   uint32_t             keylen,
		   char                *data,
		   uint64_t             datalen,
		   SDF_time_t           current_time,
		   SDF_time_t           expiry_time
	       );

/**
 *  @brief Copy back an entire object, creating it if necessary.  Set an expiry time.
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer. This may change the size of the object. The expiry time
 *  is set. If the object does not exist, create it and assign its
 *  value.
 *
 *  @param sdf_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param datalen <IN> Size of object.
 *  @param data <IN> Pointer to application buffer from which to copy data.
 *  @param current_time <IN> Current time.
 *  @param expiry_time <IN> New expiry time for an object.
 *
 *  @return SDF_SUCCESS: operation completed successfully.
 *          SDF_BAD_CONTEXT: the provided context is invalid.
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash.
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          SDF_FAILURE: operation failed.
 */
SDF_status_t SDFSetBufferedObject(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen,
	char                     *data,
	uint64_t                  datalen,
	SDF_time_t                current_time,
	SDF_time_t                expiry_time
	);

/**
 *  @brief Copy back an entire object that was modified,  and set an expiry time.
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer. put_all may change the size of the object. The expiry
 *  time is set.
 *
 *  @param sdf_thread_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param datalen <IN> Size of object.
 *  @param data <IN> Pointer to application buffer from which to copy data.
 *  @param current_time <IN> Current time.
 *  @param expiry_time <IN> New expiry time for an object.
 *
 *  @return SDF_SUCCESS: operation completed successfully.
 *          SDF_BAD_CONTEXT: the provided context is invalid.
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          SDF_OBJECT_UNKNOWN: the object does not exist.
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          SDF_FAILURE: operation failed.
 */
SDF_status_t SDFPutBufferedObject(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen,
	char                     *data,
	uint64_t                  datalen,
	SDF_time_t                current_time,
	SDF_time_t                expiry_time
	);

/**
 *  @brief Delete an object, but check for expiry first.
 *
 *  Delete an object, but check for expiry first.
 *
 *  @param sdf_thread_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param current_time <IN> Current time.
 *
 *  @return SDF_SUCCESS: operation completed successfully.
 *          SDF_BAD_CONTEXT: the provided context is invalid.
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          SDF_OBJECT_UNKNOWN: the object does not exist.
 *          SDF_FAILURE: operation failed.
 */
SDF_status_t SDFRemoveObjectWithExpiry(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen,
	SDF_time_t                current_time
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
 *  @param sdf_thread_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param current_time <IN> Current time.
 *
 *  @return SDF_SUCCESS: operation completed successfully.
 *          SDF_BAD_CONTEXT: the provided context is invalid.
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          SDF_OBJECT_UNKNOWN: the object does not exist.
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          SDF_FAILURE: operation failed.
 */
SDF_status_t SDFFlushObject(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen,
	SDF_time_t                current_time
	);

/**
 * @brief Generate a container CGUID
 *
 * @param cntr_id64  <IN> container identifier
 * @return cguid on SUCCESS
 */
SDF_cguid_t SDFGenerateCguid(
        struct SDF_thread_state *sdf_thread_state,
        int64_t             cntr_id64
        );

/**
 * @brief Flush the cache
 *
 * @param current_time  <IN> time
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFFlushCache(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_time_t                current_time
	);

/**
 * @brief Enumerate container objects
 *
 * @param cguid  <IN> container global identifier
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFEnumerateContainerObjects(
	struct SDF_thread_state *sdf_thread_state,
	SDF_cguid_t              cguid,
	struct SDF_iterator    **iterator
	);

/**
 * @brief Container object enumration iterator
 *
 * @param cguid  <IN> container global identifier
 * @param key <OUT> pointer to key variable
 * @param keylen <OUT> pointer to key length variable
 * @param data <OUT> pointer to data variable
 * @param datalen <OUT> pointer to data length variable
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFNextEnumeratedObject(
	struct SDF_thread_state *sdf_thread_state,
	struct SDF_iterator     *iterator,
	char                    **key,
	uint32_t                *keylen,
	char                    **data,
	uint64_t                *datalen
	);

SDF_status_t SDFFinishEnumeration(
                   struct SDF_thread_state *sdf_thread_state,
		   struct SDF_iterator     *iterator
	       );

/**
 * @brief Get SDF statistics
 *
 * @param stats <OUT> pointer to statistics return structure
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFGetStats(
	struct SDF_thread_state *sdf_thread_state,
	SDF_stats_t             *stats
	);

/**
 * @brief Get per container statistics
 *
 * @param cguid  <IN> container global identifier
 * @param stats <OUT> pointer to statistics return structure
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFGetContainerStats(
	struct SDF_thread_state   *sdf_thread_state,
	SDF_cguid_t                cguid,
	SDF_container_stats_t     *stats
	);

/**
 * @brief Backup a container
 *
 * @param cguid  <IN> container global identifier
 * @param backup_directory <OUT> pointer to backup directory
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFBackupContainer(
	struct SDF_thread_state   *sdf_thread_state,
	SDF_cguid_t                cguid,
	char                      *backup_directory
	);

/**
 * @brief Restore a container
 *
 * @param cguid  <IN> container global identifier
 * @param backup_directory <IN> pointer to backup directory
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFRestoreContainer(
	struct SDF_thread_state   *sdf_thread_state,
	SDF_cguid_t                cguid,
	char                      *backup_directory
	);

/**
 * @brief Free an object buffer
 *
 * @param data  <IN> pointer to object buffer
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFFreeBuffer(
	struct SDF_thread_state  *sdf_thread_state,
	char                     *data
	);
#endif


/* FDF */

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
	FDF_FULL_DURABILITY 	= 0,
	FDF_PERIODIC_DURABILITY = 1,
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

#ifdef __cplusplus
}
#endif

#endif // __FDF_H
