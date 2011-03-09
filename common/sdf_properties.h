/* 
 * File:   sdf_properties.h
 * Author: Darpan Dinker
 *
 * Created on March 18, 2008, 11:51 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_properties.h 8562 2009-07-31 01:37:58Z drew $
 */

#ifndef _SDF_PROPERTIES_H
#define	_SDF_PROPERTIES_H

/*
 * XXX: drew 2008-11-24 We should move all the inline code for 
 * string conversion to an sdf common library.
 */
#include <stdio.h>
#include <string.h>

#ifdef	__cplusplus
extern "C" {
#endif

#include <inttypes.h>
#include "sdftypes.h"

typedef enum {
    SDF_OBJECT_CONTAINER, SDF_BLOCK_CONTAINER,
    SDF_LOG_CONTAINER, SDF_UNKNOWN_CONTAINER,
    SDF_ANY_CONTAINER, SDF_LOCK_CONTAINER, SDF_INVALID_CONTAINER = -1
} SDF_container_type_t;

typedef enum {
    SDF_READ_MODE,
    SDF_WRITE_MODE,
    SDF_APPEND_MODE,
    SDF_READ_WRITE_MODE
} SDF_container_mode_t;

typedef enum {
    SDF_LOCAL_STORAGE,
    SDF_GLOBAL_STORAGE,
} SDF_storage_hierarchy_t;

typedef enum {
    SDF_DEVICE_PLACEMENT,
    SDF_NODE_PLACEMENT,
    SDF_RACK_PLACEMENT,
    SDF_DATA_CENTER_PLACEMENT,
} SDF_placement_t;

typedef enum {
    SDF_NO_COMPRESSION,
    SDF_LZ_COMPRESSION,
} SDF_compression_t;

typedef enum {
    SDF_NO_ENCRYPTION,
    SDF_3DES_COMPRESSION,
    SDF_AES_COMPRESSION,
} SDF_encryption_t;

typedef enum {
    SDF_DISK_MIGRATION,
    SDF_TAPE_MIGRATION,
    SDF_SERVER_MIGRATION,
    SDF_DATA_CENTER_MIGRATION,
} SDF_migration_t;

typedef enum {
    SDF_SYNCHRONOUS_FLUSH,
    SDF_ASYNCHRONOUS_FLUSH,
} SDF_flush_t;

typedef enum {
    SDF_VOLATILE_TRANSACTION,
    SDF_NONVOLATILE_TRANSACTION,
} SDF_transaction_t;

/** Container's user access permissions */
typedef enum {
    SDF_USER_ACCESS,
    SDF_GROUP_ACCESS,
    SDF_GLOBAL__ACCESS,
} SDF_access_control_t;

/** Container's RW permissions */
typedef enum {
    SDF_READ_PERMISSIONS,
    SDF_WRITE_PERMISSIONS,
    SDF_READ_WRITE_PERMISSIONS,
} SDF_access_permissions_t;

/** Access patterns to data in container */
typedef enum {
    SDF_SEQUENTIAL_ACCESS,
    SDF_RANDOM_ACCESS,
    SDF_READ_ONLY_ACCESS,
    SDF_READ_MOSTLY_ACCESS,
    SDF_READ_WRITE_ACCESS,
    SDF_WRITE_MOSTLY_ACCESS,
    SDF_APPEND_ONLY_ACCESS,
} SDF_access_pattern_t;

/** Granularity of conflict detection, if enabled */
typedef enum {
    SDF_ENTITY_CONFLICT,
    SDF_SUBENTITY_CONFLICT,
} SDF_conflict_t;

typedef enum {
    SDF_ALL_DEBUG,
    SDF_FUNCTION_DEBUG,
    SDF_THREAD_DEBUG,
} SDF_debug_level_t;

typedef struct {
    uint32_t owner;
    uint64_t size;    // In KB
    uint32_t num_objs;
    int64_t  container_id;
} SDF_container_id_props_t;

/** Container type */
typedef struct {
    SDF_container_type_t type;
    SDF_boolean_t persistence;
    SDF_boolean_t caching_container;
    SDF_boolean_t async_writes;
} SDF_container_type_props_t;

/** Storage hierarcy */
typedef struct {
    SDF_storage_hierarchy_t level;
    SDF_placement_t distribution;
} SDF_storage_hierarchy_props_t;


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

#if 0
    /*
     * XXX: drew 2008-11-14 I have no idea what these are.  An enum should
     * probably be used for different shard placement schemes.
     */
    uint32_t shard_placement;
    uint32_t size;
#endif

    SDF_boolean_t synchronous;


} SDF_replication_props_t;

/** Quota */
typedef struct {
    SDF_boolean_t enabled;
    uint32_t size;
} SDF_quota_props_t;

/** Compression */
typedef struct {
    SDF_boolean_t enabled;
    SDF_compression_t type;
    uint32_t size;
} SDF_compression_props_t;

/** Encryption */
typedef struct {
    SDF_boolean_t enabled;
    SDF_encryption_t type;
} SDF_encryption_props_t;

/** Data migration */
typedef struct {
    SDF_boolean_t enabled;
    SDF_migration_t policy;
} SDF_migration_props_t;

/** Flush */
typedef struct {
    SDF_flush_t type;
} SDF_flush_props_t;

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

/** Transaction management */
typedef struct {
    SDF_boolean_t enabled;
    SDF_transaction_t type;
    uint32_t log;
} SDF_transaction_props_t;

/** Access control */
typedef struct {
    SDF_boolean_t enabled;
    SDF_access_control_t type;
    SDF_access_permissions_t permissions;
} SDF_access_control_props_t;

/** Access hints */
typedef struct {
    SDF_access_pattern_t pattern;
    uint32_t size;
} SDF_access_hints_props_t;

/** Conflict detection */
typedef struct {
    SDF_boolean_t enabled;
    SDF_conflict_t boundary;
} SDF_conflict_props_t;

/** Attributes */
typedef struct {
    uint32_t last_update;
    uint32_t extended_attributes;
    SDF_boolean_t inherit_properties;
} SDF_attributes_props_t;

/** Debug */
typedef struct {
    SDF_boolean_t enabled;
    SDF_debug_level_t level;
    SDF_boolean_t log_enabled;
} SDF_debug_props_t;


/** Shard */
typedef struct {
    SDF_boolean_t enabled;
    uint32_t num_shards;
} SDF_shard_props_t;

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

/** Structure for container properties */
typedef struct {
#if 0
    /**
     * @brief Properties have been changed from default
     *
     * This is mostly a kludge so most tests can be applied to mulitple
     * configurations (ex: replicated vs. not replicated) while ones which
     * depend on specifics can have them.
     *
     * A scheme where each field provided a default or non-default
     * value would be better.
     */
    SDF_boolean_t not_default;
#endif

    int master_vnode;  // node that is the master for replication

    SDF_container_id_props_t container_id;
    SDF_container_type_props_t container_type;
    SDF_storage_hierarchy_props_t hierarchy;
    SDF_replication_props_t replication;
    SDF_quota_props_t quota;
    SDF_compression_props_t compression;
    SDF_encryption_props_t encryption;
    SDF_migration_props_t migration;
    SDF_flush_props_t flush;
    SDF_cache_props_t cache;
    SDF_transaction_props_t transaction;
    SDF_access_control_props_t access_control;
    SDF_access_hints_props_t access_hints;
    SDF_conflict_props_t conflict;
    SDF_attributes_props_t attributes;
    SDF_debug_props_t debug;
    SDF_shard_props_t shard;

    union {
        SDF_block_container_props_t        block_props;
        SDF_object_container_props_t       object_props;
        SDF_log_container_props_t          log_props;
    } specific;

} SDF_container_props_t;


#ifdef	__cplusplus
}
#endif

#endif	/* _SDF_PROPERTIES_H */
