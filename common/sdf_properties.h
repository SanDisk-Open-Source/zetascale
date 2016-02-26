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
#include <stdint.h>
#include "sdftypes.h"

#define CONTAINER_NAME_MAXLEN 64

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
    SDF_NO_DURABILITY = 0,
    SDF_RELAXED_DURABILITY,
    SDF_FULL_DURABILITY
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
    int				mcd_index;
	uint32_t		flash_only;//Bypass ZS DRAM Cache
	uint32_t		cache_only;//Terminate at ZS DRAM Cache 
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


#ifdef	__cplusplus
}
#endif

#endif	/* _SDF_PROPERTIES_H */
