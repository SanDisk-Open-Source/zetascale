/************************************************************************
 *
 * File:   ssd_defaults.c
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: ssd_defaults.c 308 2008-02-20 22:34:58Z briano $
 ************************************************************************/

#define _SSD_DEFAULTS_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdarg.h>
#include <aio.h>
#include "platform/logging.h"
#include "fth/fth.h"
#include "ssd_aio.h"
#include "ssd_local.h"
#include "clipper/clipper.h"
#include "ssd_defaults.h"
#include "utils/hash.h"
#include "common/sdfstats.h"

/**
 * @brief - get an object
 *
 * @param shard <IN> shard pointer
 * @param metaData <IN> object metadata pointer (key len filled in;  rest overwritten)
 * @param key <IN> key chars
 * @param dataPtr <IN> pointer to place to store pointer to malloc'd area for data
 *                     If NULL then this just does a lookup of the object
 *                     If non-null then it must point to a NULL pointer (for now)
 * @param flags <IN>
 *                   0x..8 - Hidden override - causes hidden flag to be ignored (must be same bit as flashPut)
 *
 * @return EOK     - Succeeded
 *         ENOENT  - Key does not exist or has expired
 *
 *         Other errors possible - see flash.h
 */

//
// get - basic get interface
//
int default_flashGet(struct ssdaio_ctxt *pctxt, struct shard *shard, struct objMetaData *metaData, char *key, char **dataPtr, int flags) 
{
    plat_log_msg(21724, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "flashGet is not yet implemented!");
    plat_abort();
}

/**
 * @brief - put an object
 *
 * @param shard <IN> shard pointer
 * @param metaData <IN> object metadata
 * @param key - key to put (len, chars)
 * @param data <IN> pointer to data string or NULL (which just deletes the entry)
 * @param flags <IN> 0x..0 - Put proceeds whether key exists or not
 *                   0x..1 - Put fails with EEXIST if key already exists and is not hidden
 *                   0x..2 - Put fails with ENOENT if key does not already exist or hidden
 *                   0x..4 - Lock override - overwrite locked objects
 *                   0x..8 - Hidden override - causes hidden flag to be ignored
 *                   0x.10 - Do not write and return ENOENT if sequence number provided is
 *                           less than the sequence number of the existing object
 *
 * @return EOK     - Succeeded
 *         ENOENT  - No entry
 *         EEXIST  - Entry already exists (including sequence number too small)
 *         EDQUOT  - Put would cause shard to exceed quota
 *         ENOSPC  - Flash device is full
 *         EMFILE  - Too many objects in this shard
 *
 *         Other errors possible - see flash.h
 */

int default_flashPut(struct ssdaio_ctxt *pctxt, struct shard *shard, struct objMetaData *metaData, char *key, char *data, int flags) 
{
    plat_log_msg(21725, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "flashPut is not yet implemented!");
    plat_abort();
}


/**
 * @brief - enumerate all of the objects one at a time
 *
 * @param shard - shard pointer
 * @param prevOBJ - object returned in previous call to this routine or NULL if first call
 * @param hashIndex - Pointer to hash index set by a previous call.  Not used if prevObj is NULL
 * @param key - pointer to place to put malloc'd pointer to next key
 * @param metaData - pointer to metadata area to fill in
 *
 * @return object pointer to pass as prevObj in next call to this routine or NULL if end
 */

objDesc_t *default_flashEnumerate(struct shard *shard, objDesc_t *prevObj, int *hashIndex, char **key) 
{
    plat_log_msg(21726, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "flashEnumerate is not yet implemented!");
    plat_abort();
}

/**
 * @brief Open flash device
 *
 * @param devName <IN> device name (i.e., /dev/flash)
 * @param flags <IN> :
 *    0x00 - Normal initialization (recover block and shard areas)
 *    0x02 - Reformat flash
 *    0x03 - Virgin flash - read manufacturers bad block info
 *    0x08 - Persistence available on H/W (temp)
 *
 * @return flash device structure pointer
 */
struct flashDev *default_flashOpen(char *devName, flash_settings_t *flash_settings, int flags) 
{
    plat_log_msg(21727, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "flashOpen is not yet implemented!");
    plat_abort();
}

/*
 * XXX: as of 2008-06-26 there was no delete and the shard list was updated 
 * with cas so this can be lock free.  Things get more interesting with 
 * deletes and pthread consumers.
 */
struct shard *default_shardFind(struct flashDev *dev, uint64_t shardID) 
{
    shard_t *shard = dev->shardList;

    while ((shard != NULL) && (shard->shardID != shardID)) {
        shard = shard->next;
    }

    return (shard);
}

/**
 * @brief Allocated and initialize the in-core object and flash tables for the shard
 *
 * @param dev <IN> flash dev pointer
 * @param shardID <IN> Unique and persistent shard ID
 * @param flags <IN> :
 *    0x..0 - fixed-block shard
 *    0x..1 - object-type shard
 *    0x..2 - log-type shard
 *
 *    0x.0. - Persistant
 *    0x.1. - Non-persistant (objects do not get restored from flash on flashOpen call)
 *    0x0.. - Cache (objects can be cast out)
 *    0x1.. - Store (objects cannot be cast out)
 *
 *    0x1.... - Object sequence numbers provided by flashPut caller
 *
 * @param quota <IN> max bytes this shard can hold
 * @param maxObjs <IN> maximum objects that this shard can hold
 *
 * @return - shard pointer or NULL
 */

struct shard *default_shardCreate(struct flashDev *dev, uint64_t shardID, int flags, uint64_t quota, unsigned maxObjs) 
{
    plat_log_msg(21728, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "shardCreate is not yet implemented!");
    plat_abort();
}


/**
 * @brief - open an existing shard
 */
struct shard *default_shardOpen(struct flashDev *dev, uint64_t shardID ) 
{
    plat_log_msg(21729, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "shardOpen is not yet implemented!");
    plat_abort();
}


/**
 * @brief - destroy a shard
 *
 * This does not erase all of the blocks but marks the shard for erasure
 *
 *  @param shard <in> shard structure
 */

int default_shardDelete(shard_t *shard) 
{
    plat_log_msg(21730, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "shardDelete is not yet implemented!");
    // plat_abort();
    return(-1);
}

/**
 * @brief - free a buffer returned by flashGet
 *
 *  @param p <in> pointer to buffer
 */

int default_flashFreeBuf(void *p)
{
    plat_free(p);
    return(FLASH_EOK);
}

/**
 * @brief - sync a shard to flash
 *
 *  @param shard <in> shard structure
 */

void default_shardSync(shard_t *shard) 
{
    plat_log_msg(21731, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "shardSync is not yet implemented!");
    plat_abort();
}

/**
 * @brief - start allowing accesses to a shard
 *
 *  @param shard <in> shard structure
 */

int default_shardStart(shard_t *shard) 
{
    plat_log_msg(21732, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "shardStart is not yet implemented!");
    plat_abort();
    return(-1);
}

/**
 * @brief - stop allowing accesses to a shard
 *
 *  @param shard <in> shard structure
 */

int default_shardStop(shard_t *shard) 
{
    plat_log_msg(21733, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "shardStop is not yet implemented!");
    plat_abort();
    return(-1);
}

/**
 * @brief flash stats
 *
 * @param shard <IN> shard to collect stats from
 * @param results <OUT> pointer to area to store results
 * @param ... <IN> List of stat items to return in results
 *
 * @return - success (0) or fail (unknown stat code)
 *
 */

uint64_t default_flashStats(struct shard *shard, int key) {
    uint64_t rv = 0;
    flashDev_t *dev = shard->dev;
    if (key == FLASH_SPACE_USED) {
        return(shard->usedSpace);
        
    } else if (key == FLASH_NUM_OBJECTS) {
        return(shard->numObjects);
        
    } else if (key == FLASH_NUM_DEAD_OBJECTS) {
        return(shard->numDeadObjects);
        
    } else if (key == FLASH_NUM_CREATED_OBJECTS) {
        return(shard->numCreatedObjects);
        
    } else if (key == FLASH_NUM_EVICTIONS) {
        for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
            rv += shard->stats[i].numEvictions;
        }
        return(rv);
        
    } else if (key == FLASH_OPS) {
        for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
            rv += dev->stats[i].flashOpCount;
        }
        return(rv);
        
    } else if (key == FLASH_READ_OPS) {
        for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
            rv += dev->stats[i].flashReadOpCount;
        }
        return(rv);
        
    } else if (key == FLASH_NV_OPS) {
        return(0);
        
    } else if (key == FLASH_NV_READ_OPS) {
        return(0);
        
    } else if (key == FLASH_BYTES_TRANSFERRED) {
        for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
            rv += dev->stats[i].flashBytesTransferred;
        }
        return(rv);
        
    } else if (key == FLASH_NV_BYTES_TRANSFERRED) {
        return(0);
        
    } else if (key == FLASH_NUM_GET_OPS) {
        for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
            rv += shard->stats[i].numGetOps;
        }
        return(rv);
        
    } else if (key == FLASH_NUM_PUT_OPS) {
        for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
            rv += shard->stats[i].numPutOps;
        }
        return(rv);
        
    } else if (key == FLASH_NUM_DELETE_OPS) {
        for (int i = 0; i < FTH_MAX_SCHEDS; i++) {
            rv += shard->stats[i].numDeleteOps;
        }
        return(rv);
        
    } else if (key == FLASH_NUM_COALESCE_OPS) {
        return(0);
        
    } else if (key == FLASH_COALESCE_BYTES_TRANSFERRED) {
        return(0);

    } else if (key == FLASH_SINGLE_BIT_ECC_ERRORS) {
        return(0);

    } else if (key == FLASH_MULTI_BIT_ECC_ERRORS) {
        return(0);

    } else if (key == FLASH_TRANSIENT_ECC_ERRORS) {
        return(0);

    } else if (key == FLASH_PENDING_COALESCE_BLOCKS) {
        return(0);

    } else if (key == FLASH_PENDING_ERASE_BLOCKS) {
        return(0);

    } else if (key == FLASH_STATUS1_COMPLETIONS) {
        return(0);

    } else if (key == FLASH_STATUS2_COMPLETIONS) {
        return(0);

    } else if (key == FLASH_FTH_SCHEDULER_IDLE_TIME) {
        return(fthGetSchedulerIdleTime());
    } else if (key == FLASH_FTH_SCHEDULER_DISPATCH_TIME) {
        return(fthGetSchedulerDispatchTime());
    } else if (key == FLASH_FTH_SCHEDULER_LOW_PRIO_DISPATCH_TIME) {
        return(fthGetSchedulerLowPrioDispatchTime());
    } else if (key == FLASH_FTH_TOTAL_THREAD_RUN_TIME) {
        return(fthGetTotalThreadRunTime());
    } else if (key == FLASH_FTH_NUM_DISPATCHES) {
        return(fthGetSchedulerNumDispatches());
    } else if (key == FLASH_FTH_NUM_LOW_PRIO_DISPATCHES) {
        return(fthGetSchedulerNumLowPrioDispatches());
    } else if (key == FLASH_FTH_AVG_DISPATCH_NANOSEC) {
        return(fthGetSchedulerAvgDispatchNanosec());
    } else if (key == FLASH_TSC_TICKS_PER_MICROSECOND) {
        return(fthGetTscTicksPerMicro());
    } else if (key == FLASH_DREWS_BIRTHDAY) {
        return(0x02171973);
#if FLASH_TYPE_MEM || FLASH_TYPE_FILE        
    } else if (key == FLASH_SIM_TOTAL_TIME) {
        return(flashTypeSimTotalTsc() / fthGetTscTicksPerMicro());
#endif        
    } else {
        return(-1);
    }
}

struct objMetaData *default_getMetaData(struct objDesc *obj)
{
    return(&(obj->metaData));
}

void default_shardAttributes(struct shard *shard, int *p_flags, uint64_t *p_quota, unsigned *p_maxObjs)
{
    *p_flags   = shard->flags;
    *p_quota   = shard->quota;
    *p_maxObjs = shard->maxObjs;
}

/***********************   NO-OP!!   **************************/

void default_shardFlushAll(struct shard *shard, flashTime_t expTime)
{
    /* purposefully empty! */
}

uint64_t default_getSequence(shard_t *shard)
{
    /* purposefully empty! */
    return(0);
}

void default_flashSetSyncedSequence(shard_t *shard, uint64_t seqno)
{
    /* purposefully empty! */
}

int default_flashSequenceScan(struct shard *shard, uint64_t *id1, uint64_t *id2,
                      struct objMetaData *metaData, char **key, 
		      char **dataPtr, int flags)
{
    /* purposefully empty! */
    return(0);
}

uint64_t default_flashGetRetainedTombstoneGuarantee(struct shard *shard)
{
    /* purposefully empty! */
    return(0);
}

/***********************   UNIMPLEMENTED!!   **************************/

void default_flashClose(struct flashDev *dev)
{
    plat_log_msg(21734, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "flashClose is not yet implemented!");
    plat_abort();
}

void default_shardFree(struct shard *shard)
{
    plat_log_msg(21735, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "shardFree is not yet implemented!");
    plat_abort();
}

struct shard *default_getNextShard(struct flashDev *dev, struct shard *prevShard)
{
    plat_log_msg(21736, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "getNextShard is not yet implemented!");
    plat_abort();
}

void default_setLRUCallback(struct shard *shard, uint64_t (*lruCallback)(syndrome_t syndrome, uint64_t newSeqNo))
{
    plat_log_msg(21737, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "setLRUCallback is not yet implemented!");
    plat_abort();
}

void default_flashRegisterSetRetainedTombstoneGuaranteeCallback(void (*callback)(uint64_t shardID, uint64_t seqno))
{
    plat_log_msg(21738, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "flashRegisterSetRTGCallback is not yet implemented!");
    plat_abort();
}

