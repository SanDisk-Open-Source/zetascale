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
 * File:   sdf.h
 * Author: Darryl Ouye
 *
 * Created on August 22, 2012
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 */
#ifndef __SDF_H
#define __SDF_H

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

#define CONTAINER_NAME_MAXLEN 64

#if 0
/**
 * @brief Statuses and their value
 *
 * item(caps, value)
 */

/*  Be sure to add new codes at end so that rolling upgrades
 *  don't get screwed up because of inconsistent status codes!!!!!
 */

#define SDF_STATUS_ITEMS() \
    item(SDF_SUCCESS, = 1) \
    item(SDF_FAILURE, /* default */) \
    item(SDF_INVALID_PARAMETER, /* default */) \
    item(SDF_CONTAINER_UNKNOWN, /* default */) \
    item(SDF_CONTAINER_EXISTS, /* default */) \
    item(SDF_OBJECT_UNKNOWN, /* default */) \
    item(SDF_OBJECT_EXISTS, /* default */) \
    item(SDF_OBJECT_TOO_BIG, /* default */) \
    item(SDF_FAILURE_STORAGE_READ, /* default */) \
    item(SDF_FAILURE_STORAGE_WRITE, /* default */) \
    item(SDF_FAILURE_MEMORY_ALLOC, /* default */) \
    item(SDF_OUT_OF_CONTEXTS, /* default */) \
    item(SDF_OUT_OF_MEM, /* default */) \
    item(SDF_EXPIRED, /* default */) \
    item(SDF_TOO_MANY_CONTAINERS, /* default */)\
    item(SDF_STOPPED_CONTAINER, /* default */)\
    item(SDF_EXPIRY_GET_FAILED, /* default */)\
    item(SDF_EXPIRY_DELETE_FAILED, /* default */)\
    item(SDF_TIMEOUT, /* default */)


typedef enum {
#define item(caps, value) \
    caps value,
    SDF_STATUS_ITEMS()
#undef item
    N_SDF_STATUS_STRINGS
} SDF_status_t;

    /* these MUST be kept in sync with above enums! */
#ifndef _INSTANTIATE_SDF_STATUS_STRINGS
    extern char *SDF_Status_Strings[];
#else
    char *SDF_Status_Strings[] = {
	"UNKNOWN_STATUS", /* since SDF_SUCCESS is 1! */
#define item(caps, value) \
        #caps,
        SDF_STATUS_ITEMS()
#undef item
    };
#endif

static inline int
sdf_status_valid(SDF_status_t status) {
    return (status < N_SDF_STATUS_STRINGS);
}

/* Avoid link order problem with strings */
inline const char *
SDFStatusToString(SDF_status_t status) {
    switch (status) {
#define item(caps, value) \
    case caps: return (#caps);
    SDF_STATUS_ITEMS()
#undef item
    default:
        return ("Invalid");
    }
}
#endif /* 0 */

struct SDF_state;
struct SDF_thread_state;
struct SDF_iterator;

#if 0
typedef uint64_t SDF_cguid_t;
typedef uint32_t SDF_time_t;
#endif

#ifdef SDFAPI 
typedef enum {
    SDF_OBJECT_CONTAINER, SDF_BLOCK_CONTAINER,
    SDF_LOG_CONTAINER, SDF_UNKNOWN_CONTAINER,
    SDF_ANY_CONTAINER, SDF_LOCK_CONTAINER, SDF_INVALID_CONTAINER = -1
} SDF_container_type_t;

typedef enum {
    SDF_NO_DURABILITY = 0,
    SDF_RELAXED_DURABILITY,
    SDF_FULL_DURABILITY,
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
    uint64_t sc_num_objs;
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

#ifndef SDF_APP

struct _SDF_container_parent;
PLAT_SP(_SDF_container_parent_sp, struct _SDF_container_parent);

struct _SDF_container;
PLAT_SP(_SDF_container_sp, struct _SDF_container);

PLAT_SP_VAR_OPAQUE(DescrChangesPtr_sp, void);

#ifndef SDFCACHEOBJ_SP
#define SDFCACHEOBJ_SP
PLAT_SP_VAR_OPAQUE(SDFCacheObj_sp, void);
#endif

/**
 * Each unique open container has _SDF_container_parent structures. Each open
 * container creates a new _SDF_container, that has a pointer to the parent.
 *
 * Need to be cautious about space, so using bit fields.
 */
struct _SDF_container_parent {
    SDF_vnode_t node;
    SDF_cguid_t cguid;
    uint64_t container_id;
    uint32_t num_shards;
    unsigned container_type: 4; // allows 16 container types
    unsigned blockSize;
    char *dir;
    char *name;
    unsigned num_open_descriptors;
    SDF_boolean_t delete_pending;
    _SDF_container_parent_sp_t bucket_next;
    _SDF_container_sp_t open_containers;
};

/**
 * Container handle provided with each SDFOpenContainer() call.
 */
struct _SDF_container {
    _SDF_container_parent_sp_t parent;
    _SDF_container_sp_t next;
    DescrChangesPtr_sp_t ptr; // TODO pointer to list of block/object updates, etc.
    SDF_container_mode_t mode;
    SDF_vnode_t node; // Copied from parent for convenience
    SDF_cguid_t cguid; // Copied from parent for convenience
    uint64_t container_id;
    uint32_t num_shards; // Copied from parent for convenience
    unsigned blockSize;
    SDF_object_info_t **info;
    uint32_t info_index;

    // TODO add "access permission"
};

typedef _SDF_container_parent_sp_t SDF_CONTAINER_PARENT;
typedef _SDF_container_sp_t SDF_CONTAINER;
typedef SDFCacheObj_sp_t SDF_CACHE_OBJ;

typedef struct _SDF_container_parent *local_SDF_CONTAINER_PARENT;
typedef struct _SDF_container *local_SDF_CONTAINER;
typedef void * local_SDF_CACHE_OBJ;

SDF_CONTAINER createContainer();
void freeContainer(SDF_CONTAINER c);
__inline__ local_SDF_CONTAINER getLocalContainer(local_SDF_CONTAINER *lc, SDF_CONTAINER c);
__inline__ void releaseLocalContainer(local_SDF_CONTAINER *lc);
__inline__ int isContainerNull(SDF_CONTAINER c);
__inline__ int containerPtrEqual(SDF_CONTAINER c1, SDF_CONTAINER c2);
#define containerNull _SDF_container_sp_null 
 
SDF_CONTAINER_PARENT createContainerParent();
void freeContainerParent(SDF_CONTAINER_PARENT p);
__inline__ local_SDF_CONTAINER_PARENT getLocalContainerParent(local_SDF_CONTAINER_PARENT *lp, SDF_CONTAINER_PARENT p);
__inline__ void releaseLocalContainerParent(local_SDF_CONTAINER_PARENT *lp);
__inline__ int isContainerParentNull(SDF_CONTAINER_PARENT p);
#define containerParentNull _SDF_container_parent_sp_null
    
__inline__ SDF_CACHE_OBJ createCacheObject(size_t size);
__inline__ void freeCacheObject(SDF_CACHE_OBJ o, size_t size);
__inline__ local_SDF_CACHE_OBJ getLocalCacheObject(local_SDF_CACHE_OBJ *lo, SDF_CACHE_OBJ o, size_t size);
__inline__ void releaseLocalCacheObject(local_SDF_CACHE_OBJ *lo, size_t size);
__inline__ int isCacheObjectNull(SDF_CACHE_OBJ o);
__inline__ int cacheObjectPtrEqual(SDF_CACHE_OBJ o1, SDF_CACHE_OBJ o2);
#define cacheObjectNull SDFCacheObj_sp_null

#endif // SDF_APP

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
	uint32_t					flash_only;
	uint32_t					cache_only;
    SDF_boolean_t compression;  /*Flag to enable/disable compression */
    uint64_t  flags;
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

#if 0
typedef struct {
    uint64_t    n_accesses[SDF_N_ACCESS_TYPES];
    uint64_t    cache_stats[SDF_N_CACHE_STATS];
    uint64_t    flash_stats[SDF_N_FLASH_STATS];
} SDF_container_stats_t;
#endif

typedef struct ZS_operational_states_t_ {
    SDF_boolean_t is_shutdown_in_progress;
	void		  *shutdown_thread;
    /*
     * Add more flags if needed.
     */
} ZS_operational_states_t;


/**
 * @brief set ZS property
 *
 * @param propery <IN> property name
 * @param value <IN> pointer to value
 * 
 */
void SDFSetProperty(
	const char* property,
	const char* value
	);

/**
 * @brief Load properties from specified file
 *
 * @param propery <IN> properties file name
 * @return 0 on success
 * 
 */
int SDFLoadProperties(
	const char* prop_file
	);

/**
 * @brief ZS initialization
 *
 * @param zs_state <IN> ZS state variable
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
 * @brief ZS per thread state initialization
 *
 * @param zs_state <IN> ZS thread state variable
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
 * @brief Force any buffered changes to any container to storage and sync storage.
 *
 *  @param sdf_thread_state <IN> The SDF context for which this operation applies.
 *
 *  @return SDF_SUCCESS on success
 */
SDF_status_t SDFFlushCache (
	struct SDF_thread_state *sdf_thread_state
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

#ifndef SDF_APP
/*
** Temporary for internal compatibility
*/
SDF_status_t
SDFOpenContainerPath(
	struct SDF_thread_state 	*sdf_thread_state,
	const char *path, 
	SDF_container_mode_t mode, 
	SDF_CONTAINER *container
	);

SDF_status_t SDFCloseContainerPath(
	struct SDF_thread_state 	*sdf_thread_state,
	SDF_CONTAINER 		 container
	);

SDF_status_t SDFDeleteContainerPath(
	struct SDF_thread_state 	*sdf_thread_state,
	const char 		*path
	);
void SDFShutdown();
#endif // SDF_APP

#endif /* SDFAPI */

#ifdef __cplusplus
}
#endif

#endif // __SDF_H
