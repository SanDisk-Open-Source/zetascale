/************************************************************************
 *
 * File:   ssd.c
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 *  Issues:
 *     - should LRU be updated on an existence check operation? 
 *       (flashGet with NULL dataPtr)
 *
 *  Notes:
 *     - metadata is put at END of block array so that the object itself can be
 *       copied directly into a buffer without any offset
 *
 * $Id: ssd.c 308 2008-02-20 22:34:58Z briano $
 ************************************************************************/

#define _SSD_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdarg.h>
#include <aio.h>
#include "platform/logging.h"
#include "fth/fth.h"
#include "utils/properties.h"
#include "ssd_aio.h"
#include "ssd_aio_local.h"
#include "ssd_local.h"
#include "ssd_defaults.h"
#include "ssd.h"
#include "clipper/clipper.h"
#include "fifo/fifo.h"
#include "utils/hash.h"
#include "common/sdfstats.h"
#include "../protocol/action/simple_replication.h"

static ssdState_t ssdState;

void ssd_Init()
{
    ssdScheme         scheme;
    ssdState_t       *psss;
    const char       *scheme_name;

    psss = &ssdState;

    scheme_name = getProperty_String("FLASH_SUBSYSTEM_TYPE", "Fifo");

    if (strcmp(scheme_name, "Clipper") == 0) {
	scheme = SSD_Clipper;
    } else if (strcmp(scheme_name, "Fifo") == 0) {
	scheme = SSD_Fifo;
    } else {
	plat_log_msg(21722, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "Invalid SSD scheme (%s)!", scheme_name);
	plat_abort();
    }

    /* assign to defaults */

    psss->flashGet          = default_flashGet;
    psss->flashPut          = default_flashPut;
    psss->flashEnumerate    = default_flashEnumerate;
    psss->flashOpen         = default_flashOpen;
    psss->shardFind         = default_shardFind;
    psss->shardCreate       = default_shardCreate;
    psss->shardOpen         = default_shardOpen;
    psss->shardDelete       = default_shardDelete;
    psss->shardStart        = default_shardStart;
    psss->shardStop         = default_shardStop;
    psss->flashStats        = default_flashStats;
    psss->getMetaData       = default_getMetaData;
    psss->shardAttributes   = default_shardAttributes;
    psss->shardFlushAll     = default_shardFlushAll;
    psss->flashSetSyncedSequence = default_flashSetSyncedSequence;
    psss->flashGetHighSequence = default_getSequence;
    psss->flashSequenceScan = default_flashSequenceScan;
    psss->flashClose        = default_flashClose;
    psss->shardFree         = default_shardFree;
    psss->getNextShard      = default_getNextShard;
    psss->setLRUCallback    = default_setLRUCallback;
    psss->flashFreeBuf      = default_flashFreeBuf;
    psss->shardSync         = default_shardSync;
    psss->flashGetRetainedTombstoneGuarantee = default_flashGetRetainedTombstoneGuarantee;
    psss->flashRegisterSetRetainedTombstoneGuaranteeCallback = default_flashRegisterSetRetainedTombstoneGuaranteeCallback;

    /* override defaults as necessary */

    switch (scheme) {
        case SSD_Clipper:
	    psss->flashGet          = clipper_flashGet;
	    psss->flashPut          = clipper_flashPut;
	    psss->flashOpen         = clipper_flashOpen;
	    psss->shardCreate       = clipper_shardCreate;
	    psss->flashFreeBuf      = default_flashFreeBuf;
	    break;
        case SSD_Fifo:
            psss->flashGet          = fifo_flashGet;
            psss->flashPut          = fifo_flashPut;
            psss->flashOpen         = fifo_flashOpen;
            psss->shardCreate       = fifo_shardCreate;
            psss->shardOpen         = fifo_shardOpen;
            psss->flashFreeBuf      = fifo_flashFreeBuf;
            psss->flashStats        = fifo_flashStats;
            psss->shardSync         = fifo_shardSync;
            psss->shardDelete       = fifo_shardDelete;
            psss->shardStart        = fifo_shardStart;
            psss->shardStop         = fifo_shardStop;
            psss->flashGetIterationCursors = fifo_flashGetIterationCursors;
            psss->flashGetByCursor = fifo_flashGetByCursor;
            psss->flashGetHighSequence = fifo_flashGetHighSequence;
            psss->flashSetSyncedSequence = fifo_flashSetSyncedSequence;
            psss->flashGetRetainedTombstoneGuarantee = fifo_flashGetRetainedTombstoneGuarantee;
            psss->flashRegisterSetRetainedTombstoneGuaranteeCallback = fifo_flashRegisterSetRetainedTombstoneGuaranteeCallback;
	    break;
	default:
	    plat_log_msg(21723, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
			 "Invalid SSD scheme (%d)!", scheme);
	    plat_abort();
	    break;
    }

    simple_replication_flash_put = ssd_flashPut;
    simple_replication_init_ctxt = ssdaio_init_ctxt;
    simple_replication_shard_find = ssd_shardFind;
}

int ssd_flashGet(struct ssdaio_ctxt *pctxt, struct shard *shard, struct objMetaData *metaData, char *key, char **dataPtr, int flags) 
{
    return((ssdState.flashGet)(pctxt, shard, metaData, key, dataPtr, flags));
}

int ssd_flashPut(struct ssdaio_ctxt *pctxt, struct shard *shard, struct objMetaData *metaData, char *key, char *data, int flags) 
{
    return((ssdState.flashPut)(pctxt, shard, metaData, key, data, flags));
}


objDesc_t *ssd_flashEnumerate(struct shard *shard, objDesc_t *prevObj, int *hashIndex, char **key) 
{
    return((ssdState.flashEnumerate)(shard, prevObj, hashIndex, key));
}

struct flashDev *ssd_flashOpen(char *devName, flash_settings_t *flash_settings, int flags) 
{
    return((ssdState.flashOpen)(devName, flash_settings, flags));
}

struct shard *ssd_shardFind(struct flashDev *dev, uint64_t shardID) 
{
    return((ssdState.shardFind)(dev, shardID));
}

struct shard *ssd_shardCreate(struct flashDev *dev, uint64_t shardID, int flags, uint64_t quota, unsigned maxObjs) 
{
    return((ssdState.shardCreate)(dev, shardID, flags, quota, maxObjs));
}

struct shard *ssd_shardOpen( struct flashDev * dev, uint64_t shardID )
{
    return ((ssdState.shardOpen)(dev, shardID));
}

int ssd_shardDelete(shard_t *shard) 
{
    return (ssdState.shardDelete)(shard);
}

int ssd_shardStart(shard_t *shard) 
{
    return (ssdState.shardStart)(shard);
}

int ssd_shardStop(shard_t *shard) 
{
    return (ssdState.shardStop)(shard);
}

uint64_t ssd_flashStats(struct shard *shard, int key) 
{
    return((ssdState.flashStats)(shard, key));
}

struct objMetaData *ssd_getMetaData(struct objDesc *obj)
{
    return((ssdState.getMetaData)(obj));
}

void ssd_shardAttributes(struct shard *shard, int *p_flags, uint64_t *p_quota, unsigned *p_maxObjs)
{
    (ssdState.shardAttributes)(shard, p_flags, p_quota, p_maxObjs);
}

void ssd_shardFlushAll(struct shard *shard, flashTime_t expTime)
{
    (ssdState.shardFlushAll)(shard, expTime);
}

uint64_t ssd_flashGetHighSequence(shard_t *shard)
{
    return((ssdState.flashGetHighSequence)(shard));
}

void ssd_flashSetSyncedSequence(shard_t *shard, uint64_t seqno)
{
    return((ssdState.flashSetSyncedSequence)(shard, seqno));
}

int ssd_flashSequenceScan(struct shard *shard, uint64_t *id1, uint64_t *id2,
                      struct objMetaData *metaData, char **key, 
		      char **dataPtr, int flags)
{
    return((ssdState.flashSequenceScan)(shard, id1, id2, metaData, key, dataPtr, flags));
}

void ssd_flashClose(struct flashDev *dev)
{
    (ssdState.flashClose)(dev);
}

void ssd_shardFree(struct shard *shard)
{
    (ssdState.shardFree)(shard);
}

struct shard *ssd_getNextShard(struct flashDev *dev, struct shard *prevShard)
{
    return((ssdState.getNextShard)(dev, prevShard));
}

int ssd_flashGetIterationCursors(struct shard *shard, uint64_t seqno_start,
                                 uint64_t seqno_len, uint64_t seqno_max,
                                 const struct flashGetIterationResumeOutput * resume_cursor_in,
                                 struct flashGetIterationOutput **cursors_out)
{
    return ((ssdState.flashGetIterationCursors)(shard, seqno_start, seqno_len, seqno_max,
                                                resume_cursor_in, cursors_out));
}

int ssd_flashGetByCursor(struct shard *shard, int cursor_len, const void *cursor,
                         struct objMetaData *metaData, char **key, void **data, int flags,
                         time_t flush_time)
{
    return ((ssdState.flashGetByCursor)(shard, cursor_len, cursor, metaData, key, data, flags, flush_time));
}

void ssd_setLRUCallback(struct shard *shard, uint64_t (*lruCallback)(syndrome_t syndrome, uint64_t newSeqNo))
{
    (ssdState.setLRUCallback)(shard, lruCallback);
}

int ssd_flashFreeBuf(void *p)
{
    return((ssdState.flashFreeBuf)(p));
}

void ssd_shardSync(shard_t *shard) 
{
    (ssdState.shardSync)(shard);
}

uint64_t ssd_flashGetRetainedTombstoneGuarantee(struct shard *shard)
{
    return (ssdState.flashGetRetainedTombstoneGuarantee)(shard);
}

void ssd_flashRegisterSetRetainedTombstoneGuaranteeCallback( void (*callback)(uint64_t shardID, uint64_t seqno) )
{
    (ssdState.flashRegisterSetRetainedTombstoneGuaranteeCallback)(callback);
}

