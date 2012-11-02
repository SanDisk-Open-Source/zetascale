/*
 * File:   flash.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: flash.h 396 2008-02-29 22:55:43Z jim $
 */

#ifndef __FLASH_H
#define __FLASH_H

#include "common/sdftypes.h"

// Flags for opening flash devices
enum flashOpenFlags {
    FLASH_OPEN_NORMAL_RECOVERY       = 0x00,
    FLASH_OPEN_REVIRGINIZE_DEVICE    = 0x01,
    FLASH_OPEN_REFORMAT_DEVICE       = 0x02,
    FLASH_OPEN_FORMAT_VIRGIN_DEVICE  = 0x04,
    FLASH_OPEN_FLAGS_MASK            = 0x07,
    FLASH_OPEN_PERSISTENCE_AVAILABLE = 0x08  // temporary
};

enum flashPutFlags {
    FLASH_PUT_NO_TEST         = 0x00,  // Put proceeds wither key exists or not
    FLASH_PUT_TEST_EXIST      = 0x01,  // Put fails if key already exists and not hidden
    FLASH_PUT_TEST_NONEXIST   = 0x02,  // Put fails if key does not exist or hidden
    FLASH_PUT_OVERRIDE_LOCK   = 0x04,  // Overwrite locked objects
    FLASH_PUT_OVERRIDE_HIDDEN = 0x08,  // Causes hidden flag to be ignored
    FLASH_PUT_IF_NEWER        = 0x10,  // Put fails if provided sequence number is
                                       //   less than sequence number of existing object
    FLASH_PUT_RESTORE         = 0x20,  // Put is a restore operation
    FLASH_PUT_DEL_EXPIRED     = 0x40,  // Delete if object expired
    FLASH_PUT_PREFIX_DELETE   = 0x80,  // Perform a prefix-based delete on this shard
    FLASH_PUT_PREFIX_DO_DEL   = 0x180  // Prefix-based object deletion
};

enum flashGetFlags {
    FLASH_GET_NO_TEST         = 0x00,  // Get according to metadata flags (no overrides)
    FLASH_GET_OVERRIDE_HIDDEN = 0x08   // Causes hidden flag to be ignored
};

#define FLASH_ALIGNMENT_LEN 16
#define FLASH_ALIGNMENT_ADDR 16

#define FLASH_ALIGN_LEN(l) (((l) + FLASH_ALIGNMENT_LEN - 1) & ~(FLASH_ALIGNMENT_LEN-1))

#define flash_is_aligned(x) \
            ((((uint64_t) (x)) & (FLASH_ALIGNMENT_LEN - 1)) == 0)

#define nvram_is_aligned(x) \
            ((((uint64_t) (x)) & (FLASH_ALIGNMENT_ADDR - 1)) == 0)

typedef uint64_t syndrome_t;

typedef uint32_t flashTime_t;                // Timestamp used for flash

const enum {
    FLASH_META_HIDDEN_FLAG = (1 << 2),
    FLASH_META_LOCKED_FLAG = (1 << 1),
    FLASH_META_RESERVED_FLAG = (1 << 0)      // **ORDER** //
} flashMetaFlags;

// Metadata included associated with each object and included in the first flash chunk
typedef struct objMetaData {
    flashTime_t expTime;
    flashTime_t createTime;
    uint32_t dataLen;
    uint16_t keyLen;
    uint8_t objFlags;                        // ** ORDER ** //
    uint8_t reserved;
    uint64_t sequence;                       // Sequence number (used only if associated
                                             // shard flag is set on shard create call)

    char userMetaData[0];                    // Arbitrary user data
    
} objMetaData_t;

/*
 * The high-level cursor for iterating over large data
 */
typedef struct flashGetIterationResumeOutput {
    /** @brief what table iterating over */
    uint64_t cursor_state;

    /** @brief what logbuf/blk on that table we're on */
    uint64_t cursor1;

    /** @brief last index on that logbuf/blk */
    uint64_t cursor2;

    /* these are used in replication test framework test_flash.c */
    uint64_t seqno_start;
    uint64_t seqno_len;
    uint64_t seqno_max;
    uint64_t seqno_left;	// Number of sequence numbers left to return
} resume_cursor_t;

/*
 * Cursors to get key/object pairs
 */
typedef struct flashGetIterationOutput {
    /** @brief the resume cursor */
    resume_cursor_t resume_cursor;
    
    /** @brief len of one cursor in bytes */
    int cursor_len;
    /** @brief Number of cursors returned */
    int cursor_count;

    /** @brief Cursor data  */
    char cursors[0];
} it_cursor_t;

/**
 * @brief External interface to flash
 */

// Return codes for flash (these largely mirror standard errno)
const enum {
    FLASH_EOK = 0,                  // Call succeeded
    FLASH_EPERM = 1,                // Operation not permitted
    FLASH_ENOENT = 2,               // No entry
    FLASH_EDATASIZE = 7,            // User-supplied data buffer too small
    FLASH_ESTOPPED = 8,             // Container is stopped
    FLASH_EBADCTNR = 9,             // Container does not exist
    FLASH_EDELFAIL = 10,            // Deletion for a local failure failed
    FLASH_EAGAIN = 11,              // Try again (transient error)
    FLASH_ENOMEM = 12,              // Out of system memory
    FLASH_EACCES = 13,              // Permission denied
    FLASH_EINCONS= 14,              // Inconsistency during replication
    FLASH_RMT_EDELFAIL = 15,        // Deletion for a remote failure failed
    FLASH_EBUSY = 16,               // Device busy
    FLASH_EEXIST = 17,              // Object exists
    FLASH_RMT_EBADCTNR = 18,        // Container does not exist on remote node
    FLASH_EINVAL = 22,              // Invalid argument
    FLASH_EMFILE = 24,              // Too many objects
    FLASH_ENOSPC = 28,              // Out of flash space
    FLASH_ENOBUFS = 105,            // Out of system resource
    FLASH_ESTALE = 116,             // Stale data
    FLASH_EDQUOT = 122,             // Quota exceeded
    FLASH_N_ERR_CODES = 123         // Max number of error codes
} flashErrno;

enum shardFlags {
    FLASH_SHARD_INIT_TYPE_MASK = 0x0f,
    FLASH_SHARD_INIT_TYPE_BLOCK = 0x0,
    FLASH_SHARD_INIT_TYPE_OBJECT = 0x1,
    FLASH_SHARD_INIT_TYPE_LOG = 0x2,

    FLASH_SHARD_INIT_PERSISTENCE_MASK = 0xf0,
    FLASH_SHARD_INIT_PERSISTENCE_YES = 0x10,
    FLASH_SHARD_INIT_PERSISTENCE_NO = 0x00,

    FLASH_SHARD_INIT_EVICTION_MASK = 0xf00,
    FLASH_SHARD_INIT_EVICTION_CACHE = 0x000,
    FLASH_SHARD_INIT_EVICTION_STORE = 0x100,

    FLASH_SHARD_SEQUENCE_MASK = 0x10000,
    FLASH_SHARD_SEQUENCE_EXTERNAL = 0x10000
};

typedef struct flashDev flashDev_t;
typedef struct objDesc objDesc_t;
typedef struct shard shard_t;

struct flashDev *flashOpen(char *name, flash_settings_t *flash_settings, int flags);
void flashClose(struct flashDev *dev);
struct shard *shardCreate(struct flashDev *dev, uint64_t shardID, int flags, uint64_t quota, unsigned maxObjs);
struct shard *shardOpen(struct flashDev *dev, uint64_t shardID);
void shardFree(struct shard *);
void shardAttributes(struct shard *shard, int *p_flags, uint64_t *p_quota, unsigned *p_maxObjs);
int shardDelete(struct shard *shard);
void shardFlushAll(struct shard *shard, flashTime_t expTime);

struct shard *getNextShard(struct flashDev *dev, struct shard *prevShard);
struct shard *shardFind(struct flashDev *dev, uint64_t shardID);

int flashGet(struct shard *shard, struct objMetaData *metaData, char *key, char **dataPtr, int flags);
int flashPut(struct shard *shard, struct objMetaData *metaData, char *key, char *data, int flags);
struct objDesc *flashEnumerate(struct shard *shard, struct objDesc *prevObj, int *hashIndex, char **key);
void setLRUCallback(struct shard *shard, uint64_t (*lruCallback)(syndrome_t syndrome, uint64_t newSeqNo));
struct objMetaData *getMetaData(struct objDesc *obj);
uint64_t getSequence(shard_t *shard);
uint64_t flashStats(struct shard *shard, int key);
int flashFreeBuf(void *p);

void debugPrintfInit(void);
void debugPrintf(char *format, ...);

#ifdef FLASH_SIM_LATENCY
void flashSimCompleteOps (flashDev_t *dev);
#endif
void flashClose(flashDev_t *);
int flashSequenceScan(struct shard *shard, uint64_t *id1, uint64_t *id2,
                      struct objMetaData *metaData, char **key, char **dataPtr, int flags);

#ifdef ENABLE_MULTIPLE_FLASH_SUBSYSTEMS

   /* make calls to the flash interface go to a redirection point */

    #define flashGet(shard, metaData, key, dataPtr, flags) \
	ssd_flashGet(pai->paio_ctxt, shard, metaData, key, dataPtr, flags) 
    #define flashPut(shard, metaData, key, data, flags) \
	ssd_flashPut(pai->paio_ctxt, shard, metaData, key, data, flags) 
    #define flashEnumerate(shard, prevObj, hashIndex, key) \
	ssd_flashEnumerate(shard, prevObj, hashIndex, key) 
    #define flashOpen(devName, settings, flags) \
	ssd_flashOpen(devName, settings, flags) 
    #define shardFind(dev, shardID) \
	ssd_shardFind(dev, shardID) 
    #define shardCreate(dev, shardID, flags, quota, maxObjs) \
	ssd_shardCreate(dev, shardID, flags, quota, maxObjs) 
    #define shardOpen(dev, shardID) \
	ssd_shardOpen(dev, shardID) 
    #define shardDelete(shard) \
	ssd_shardDelete(shard) 
    #define shardSync(shard) \
	ssd_shardSync(shard) 
    #define shardStart(shard) \
	ssd_shardStart(shard) 
    #define shardStop(shard) \
	ssd_shardStop(shard) 
    #define flashStats(shard, key) \
	ssd_flashStats(shard, key) 
    #define getMetaData(obj) \
	ssd_getMetaData(obj)
    #define shardAttributes(shard, p_flags, p_quota, p_maxObjs) \
	ssd_shardAttributes(shard, p_flags, p_quota, p_maxObjs)
    #define shardFlushAll(shard, expTime) \
	ssd_shardFlushAll(shard, expTime)
    #define flashGetHighSequence(shard) \
	ssd_flashGetHighSequence(shard)
    #define flashSetSyncedSequence(shard, seqno) \
        ssd_flashSetSyncedSequence(shard, seqno)
    #define flashGetIterationCursors(shard, seqno_start, seqno_len, seqno_max, resume_cursor_in, out) \
	ssd_flashGetIterationCursors(shard, seqno_start, seqno_len, seqno_max, resume_cursor_in, out)
    #define flashGetByCursor(shard, cursor_len, cursor, metaData, key, data, flags, flush_time) \
        ssd_flashGetByCursor(shard, cursor_len, cursor, metaData, key, data, flags, flush_time)
    #define flashSequenceScan(shard, id1, id2, metaData, key, dataPtr, flags) \
	ssd_flashSequenceScan(shard, id1, id2, metaData, key, dataPtr, flags)
    #define flashClose(dev) \
	ssd_flashClose(dev)
    #define shardFree(shard) \
	ssd_shardFree(shard)
    #define getNextShard(dev, prevShard) \
	ssd_getNextShard(dev, prevShard)
    #define setLRUCallback(shard, lruCallback) \
	ssd_setLRUCallback(shard, lruCallback)
    #define flashFreeBuf(p) \
	ssd_flashFreeBuf(p)
    #define flashGetRetainedTombstoneGuarantee(shard) \
        ssd_flashGetRetainedTombstoneGuarantee(shard)
    #define flashRegisterSetRetainedTombstoneGuaranteeCallback(callback) \
        ssd_flashRegisterSetRetainedTombstoneGuaranteeCallback(callback)

    #include "ssd/ssd.h"
#endif // ENABLE_MULTIPLE_FLASH_SUBSYSTEMS

#endif
