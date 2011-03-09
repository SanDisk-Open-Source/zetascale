/*
 * File:   common/sdfstats.h
 * Author: Jim
 *
 * Created on June 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdfstats.h 12880 2010-04-13 22:30:12Z hiney $
 */

#ifndef _SDFSTATS_H
#define _SDFSTATS_H

#include <stdint.h>

#define FIRST_STAT_TYPE_MONOLITHIC      1
#define FIRST_STAT_TYPE_PER_DEV        50
#define FIRST_STAT_TYPE_PER_SHARD     100
#define FIRST_STAT_TYPE_PER_CONTAINER 150
#define FIRST_STAT_TYPE_PER_DUMMY     200

// Enumeration for stats keys
const enum {
    // Monolithic stats
    FLASH_FTH_SCHEDULER_IDLE_TIME = FIRST_STAT_TYPE_MONOLITHIC,
    FLASH_FTH_SCHEDULER_DISPATCH_TIME,
    FLASH_FTH_SCHEDULER_LOW_PRIO_DISPATCH_TIME,
    FLASH_FTH_TOTAL_THREAD_RUN_TIME,
    FLASH_FTH_NUM_DISPATCHES,
    FLASH_FTH_NUM_LOW_PRIO_DISPATCHES,
    FLASH_FTH_AVG_DISPATCH_NANOSEC,
    FLASH_TSC_TICKS_PER_MICROSECOND,
    FLASH_SIM_TOTAL_TIME,
    FLASH_IO_BLOCK_SIZE,
    FLASH_IO_FRAGMENT_SIZE,
    FLASH_DREWS_BIRTHDAY,

    // Per device stats
    FLASH_OPS = FIRST_STAT_TYPE_PER_DEV,
    FLASH_READ_OPS,
    FLASH_NV_OPS,
    FLASH_NV_READ_OPS,
    FLASH_BYTES_TRANSFERRED,
    FLASH_NV_BYTES_TRANSFERRED,
    FLASH_NUM_COALESCE_OPS,
    FLASH_COALESCE_BYTES_TRANSFERRED,
    FLASH_SINGLE_BIT_ECC_ERRORS,
    FLASH_MULTI_BIT_ECC_ERRORS,
    FLASH_TRANSIENT_ECC_ERRORS,
    FLASH_NV_SINGLE_BIT_ECC_ERRORS,
    FLASH_NV_MULTI_BIT_ECC_ERRORS,
    FLASH_PENDING_COALESCE_BLOCKS,
    FLASH_PENDING_ERASE_BLOCKS,
    FLASH_STATUS1_COMPLETIONS,
    FLASH_STATUS2_COMPLETIONS,

    // Per shard stats
    FLASH_SHARD_ID = FIRST_STAT_TYPE_PER_SHARD,
    FLASH_SHARD_FLAGS,
    FLASH_MAX_SPACE,
    FLASH_SHARD_MAX_OBJS,
    FLASH_SHARD_MAXBYTES,
    FLASH_SPACE_USED,
    FLASH_SPACE_ALLOCATED,
    FLASH_SPACE_CONSUMED,
    FLASH_SLAB_CLASS_SEGS,
    FLASH_SLAB_CLASS_SLABS,
    FLASH_NUM_OBJECTS,
    FLASH_NUM_DEAD_OBJECTS,
    FLASH_NUM_CREATED_OBJECTS,
    FLASH_NUM_EVICTIONS,
    FLASH_NUM_HASH_EVICTIONS,
    FLASH_NUM_INVAL_EVICTIONS,
    FLASH_NUM_SOFT_OVERFLOWS,
    FLASH_NUM_HARD_OVERFLOWS,
    FLASH_GET_HASH_COLLISIONS,
    FLASH_SET_HASH_COLLISIONS,
    FLASH_NUM_OVERWRITES,
    FLASH_NUM_GET_OPS,
    FLASH_NUM_PUT_OPS,
    FLASH_NUM_DELETE_OPS,
    FLASH_NUM_EXIST_CHECKS,
    FLASH_NUM_FULL_BUCKETS,

    // Per container stats
    SDF_N_ONLY_IN_CACHE = FIRST_STAT_TYPE_PER_CONTAINER,   
    SDF_N_TOTAL_IN_CACHE,
    SDF_BYTES_ONLY_IN_CACHE, 
    SDF_BYTES_TOTAL_IN_CACHE,
    SDF_N_CURR_ITEMS,        
    SDF_N_TOTAL_ITEMS,
    SDF_BYTES_CURR_ITEMS,    
    SDF_BYTES_TOTAL_ITEMS,
    SDF_N_OVERWRITES,
    SDF_N_IN_PLACE_OVERWRITES,
    SDF_N_NEW_ENTRY,
    SDF_N_WRITETHRUS,
    SDF_N_WRITEBACKS,
    SDF_N_FLUSHES,

    // Next available region
    SDF_DUMMY_STAT = FIRST_STAT_TYPE_PER_DUMMY,   

} flashStatKeys;



// Define the types of stats
const enum {
    FLASH_STAT_MONLITHIC,
    FLASH_STAT_PER_DEV ,
    FLASH_STAT_PER_SHARD
} flashStatTypes_t;

#define FLASH_STAT_TYPE(stat) ((stat >= FIRST_STAT_TYPE_PER_SHARD) ? FLASH_STAT_PER_SHARD : \
                               (stat >= FIRST_STAT_TYPE_PER_DEV) ? FLASH_STAT_PER_DEV : FLASH_SAT_MONOLITHIC)

// structure for a time
typedef struct
{
   uint64_t  t_seconds;
   uint64_t  t_nanoseconds;
} SDF_file_time_t;

// Structure for SDFGetContainerStatsMultiple
typedef struct
{
   uint64_t         cs_size;
   uint64_t         cs_blocks;
   SDF_file_time_t  cs_access;
   SDF_file_time_t  cs_modify;
   SDF_file_time_t  cs_change;
} SDF_container_stats_t;


// Structure for SDFGetFlashStatsMultiple
typedef struct
{
   uint64_t  fs_io_block_size;
   uint64_t  fs_io_fragmet_size;
   uint64_t  fs_total_blocks;
   uint64_t  fs_block_in_use;
} SDF_flash_stats_t;


#endif
