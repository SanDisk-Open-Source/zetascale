/************************************************************************
 *
 * File:   ssd.h
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: ssd.h 308 2008-02-20 22:34:58Z briano $
 ************************************************************************/

#ifndef _SSD_H
#define _SSD_H

#include "flash/flash.h"

#ifdef __cplusplus
extern "C" {
#endif

struct ssdaio_ctxt;

extern void ssd_Init();
extern struct flashDev *ssd_flashOpen(char *name, flash_settings_t *flash_settings, int flags);
extern void ssd_flashClose(struct flashDev *dev);
extern struct shard *ssd_shardCreate(struct flashDev *dev, uint64_t shardID, 
       int flags, uint64_t quota, unsigned maxObjs);
extern struct shard *ssd_shardOpen( struct flashDev * dev, uint64_t shardID );
extern void ssd_shardFree(struct shard *);
extern void ssd_shardAttributes(struct shard *shard, int *p_flags, 
	  uint64_t *p_quota, unsigned *p_maxObjs);
extern void ssd_shardClose(struct shard *shard);
extern int ssd_shardDelete(struct shard *shard);
extern int ssd_shardStart(struct shard *shard);
extern int ssd_shardStop(struct shard *shard);
extern void ssd_shardFlushAll(struct shard *shard, flashTime_t expTime);
extern struct shard *ssd_getNextShard(struct flashDev *dev, struct shard *prevShard);
extern struct shard *ssd_shardFind(struct flashDev *dev, uint64_t shardID);
extern int ssd_flashGet(struct ssdaio_ctxt *pctxt, struct shard *shard, struct objMetaData *metaData, 
	  char *key, char **dataPtr, int flags);
extern int ssd_flashPut(struct ssdaio_ctxt *pctxt, struct shard *shard, struct objMetaData *metaData, 
	  char *key, char *data, int flags);
extern struct objDesc *ssd_flashEnumerate(struct shard *shard, 
	  struct objDesc *prevObj, int *hashIndex, char **key);
extern void ssd_setLRUCallback(struct shard *shard, 
	  uint64_t (*lruCallback)(syndrome_t syndrome, uint64_t newSeqNo));
extern struct objMetaData *ssd_getMetaData(struct objDesc *obj);
extern uint64_t ssd_flashGetHighSequence(shard_t *shard);
extern void ssd_flashSetSyncedSequence(shard_t *shard, uint64_t seqno);
extern uint64_t ssd_flashStats(struct shard *shard, int key);
extern int ssd_flashSequenceScan(struct shard *shard, uint64_t *id1, uint64_t *id2,
	  struct objMetaData *metaData, char **key, 
	  char **dataPtr, int flags);
extern int ssd_flashFreeBuf(void *p);
extern void ssd_shardSync(struct shard *);

extern int ssd_flashGetIterationCursors(struct shard *shard, uint64_t seqno_start,
					uint64_t seqno_len, 
					uint64_t seqno_max,
					const resume_cursor_t * resume_cursor_in,
					struct flashGetIterationOutput ** cursors_out);
extern int ssd_flashGetByCursor(struct shard *shard, int cursor_len, const void *cursor,
				struct objMetaData *metaData, char **key, void **data, int flags, time_t flush_time);

extern uint64_t ssd_flashGetRetainedTombstoneGuarantee( struct shard *shard );
extern void ssd_flashRegisterSetRetainedTombstoneGuaranteeCallback( void (*callback)(uint64_t shardID, uint64_t seqno) );

#ifdef	__cplusplus
}
#endif

#endif /* _SSD_H */

