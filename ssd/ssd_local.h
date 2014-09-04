/************************************************************************
 *
 * File:   ssd_local.h
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: ssd_local.h 308 2008-02-20 22:34:58Z briano $
 ************************************************************************/

#ifndef _SSD_LOCAL_H
#define _SSD_LOCAL_H

#include "flash/flash.h"

#ifdef __cplusplus
extern "C" {
#endif

/**************************************************************************/

  /*  Lock macros */

    #define LockType       fthLock_t
    #define WaitType       fthWaitEl_t *

    #define InitLock(x)    fthLockInit(&(x))
    #define Unlock(x, w)   fthUnlock(w)
    #define Lock(x, w)     (w = fthLock(&(x), 1, NULL))
    #define LockTry(x, w)  (w = fthTryLock(&(x), 1, NULL))

/**************************************************************************/

typedef enum {
    SSD_Legacy=0,
    SSD_Clipper,
    SSD_Fifo,
} ssdScheme;

typedef struct flashShardStats {
    uint64_t numEvictions;
    uint64_t numGetOps;
    uint64_t numPutOps;
    uint64_t numDeleteOps;
} flashShardStats_t;

struct ssdState;

struct shard {
    struct flashDev   *dev;
    SDF_shardid_t      shardID;
    uint32_t           flags;
    uint64_t           quota;
    uint64_t           s_maxObjs;
    uint64_t           usedSpace;
    uint64_t           numObjects;
    uint64_t           numDeadObjects;
    uint64_t           numCreatedObjects;
    flashShardStats_t  stats[FTH_MAX_SCHEDS];
    uint64_t           flash_offset;
    struct shard      *next;
    union {
        struct Clipper *pclipper;
    }                  scheme;
};

typedef struct flashStats {
    uint64_t flashOpCount;
    uint64_t flashReadOpCount;
    uint64_t flashBytesTransferred;
} flashStats_t;    

struct flashDev {
    struct ssdaio_state  *paio_state;
    flashStats_t    stats[FTH_MAX_SCHEDS];
    shard_t        *shardList;
    LockType        lock;
    uint64_t        size;
    uint64_t        used;
}; 

struct objDesc {
    objMetaData_t  metaData;
};

struct ssdaio_ctxt;

typedef struct ssdState {
    ssdScheme       scheme_type;

    /* function pointers for flash interface routines */

    struct flashDev *(*flashOpen)(char *name, flash_settings_t *flash_settings, int flags);
    void (*flashClose)(struct flashDev *dev);
    struct shard *(*shardCreate)(struct flashDev *dev, uint64_t shardID, 
              int flags, uint64_t quota, unsigned maxObjs);
    struct shard *(*shardOpen)(struct flashDev *dev, uint64_t shardID);
    void (*shardFree)(struct shard *);
    void (*shardAttributes)(struct shard *shard, int *p_flags, 
              uint64_t *p_quota, unsigned *p_maxObjs);
    int (*shardDelete)(struct shard *shard);
    int (*shardStop)(struct shard *shard);
    int (*shardStart)(struct shard *shard);
    void (*shardFlushAll)(struct shard *shard, flashTime_t expTime);
    struct shard *(*getNextShard)(struct flashDev *dev, struct shard *prevShard);
    struct shard *(*shardFind)(struct flashDev *dev, uint64_t shardID);
    int (*flashGet)(struct ssdaio_ctxt *pctxt, struct shard *shard, struct objMetaData *metaData, 
              char *key, char **dataPtr, int flags);
    int (*flashPut)(struct ssdaio_ctxt *pctxt, struct shard *shard, struct objMetaData *metaData, 
              char *key, char *data, int flags);
    int (*flashPutV)(struct ssdaio_ctxt *pctxt, struct shard *shard, struct objMetaData *metaData, 
              char **key, char **data, int count, int flags);
    struct objDesc *(*flashEnumerate)(struct shard *shard, 
              struct objDesc *prevObj, int *hashIndex, char **key);
    void (*setLRUCallback)(struct shard *shard, 
              uint64_t (*lruCallback)(syndrome_t syndrome, uint64_t newSeqNo));
    struct objMetaData *(*getMetaData)(struct objDesc *obj);
    uint64_t (*flashGetHighSequence)(shard_t *shard);
    void (*flashSetSyncedSequence)(shard_t *shard, uint64_t seqno);
    uint64_t (*flashStats)(struct shard *shard, int key);
    void (*debugPrintfInit)(void);
    void (*debugPrintf)(char *format, ...);
    int (*flashSequenceScan)(struct shard *shard, uint64_t *id1, uint64_t *id2,
	      struct objMetaData *metaData, char **key, 
	      char **dataPtr, int flags);
    int (*flashFreeBuf)(void *p);
    void (*shardSync)(struct shard *shard);
    void (*shardClose)(struct shard *shard);
    int (*flashGetIterationCursors)(struct shard *shard, uint64_t seqno_start,
                                    uint64_t seqno_len, uint64_t seqno_max,
                                    const resume_cursor_t * resume_cursor_in,
                                    struct flashGetIterationOutput ** cursors_out);
    int (*flashGetByCursor)(struct shard *shard, int cursor_len, const void *cursor,
                            struct objMetaData *metaData, char **key, void **data, int flags, time_t flush_time);
    uint64_t (*flashGetRetainedTombstoneGuarantee)(struct shard *shard);
    void (*flashRegisterSetRetainedTombstoneGuaranteeCallback)(void (*callback)(uint64_t shardID, uint64_t seqno));
} ssdState_t;

#ifdef	__cplusplus
}
#endif

#endif /* _SSD_LOCAL_H */

