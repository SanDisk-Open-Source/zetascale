/*
 * File:   apps/memcached/server/memcached-1.2.5-schooner/mcd_rep.c
 * Author: Jake Moilanen
 *
 * Created on May 06, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id:
 */


#include <stdio.h>
#include <signal.h>
#include <aio.h>
#include <math.h>

#include "common/sdftypes.h"
#include "common/sdfstats.h"
#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/signal.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/time.h"
#include "platform/unistd.h"
#include "utils/hash.h"
#include "fth/fthMbox.h"

#include "fth/fth.h"
#include "fth/fthSem.h"

#include "shared/private.h"

#include "protocol/replication/replicator.h"
#include "protocol/replication/rpc.h"
#include "protocol/action/simple_replication.h"

//#include "memcached.h"
//#include "command.h"
#include "mcd_osd.h"
#include "mcd_osd.h"
#include "mcd_rec.h"
#include "mcd_rep.h"
//#include "mcd_sdf.h"
#include "mcd_aio.h"

#include "ssd/fifo/fifo.h"
#include "ssd/ssd_aio.h"

#define min(a, b) ((a) <= (b) ? (a) : (b))
#define max(a, b) ((a) >= (b) ? (a) : (b))

#undef  mcd_log_msg
#define mcd_log_msg(id, args...)                                           \
                   plat_log_msg( id,                                       \
                                 PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY,  \
                                 ##args )

// -----------------------------------------------------
//    Forward Declarations
// -----------------------------------------------------
static int logs_iterate(struct shard *shard, resume_cursor_t * resume_cursor, int seqno_start,
                        int seqno_max, int seqno_len, rep_cursor_t * cursors);
static int object_table_iterate(struct shard *shard, resume_cursor_t * resume_cursor,
                        int seqno_start, int seqno_max, int seqno_len,
                         rep_cursor_t * cursors);
static int volatile_object_table_iterate(struct shard *shard, resume_cursor_t * resume_cursor,
                                         int seqno_start, int seqno_max, int seqno_len,
                                         rep_cursor_t * cursors);
static int obj_read(struct shard * shard, uint64_t blk_offset, uint64_t nbytes, void ** data, mcd_osd_fifo_wbuf_t ** wbuf);
static int logbuf_read(struct shard * shard, int logbuf_number, char * logbuf_buf);
static int max_hash_bucket_size();

extern int Mcd_osd_rand_blksize;
extern uint64_t Mcd_osd_overflow_depth;
//extern time_t current_time;

/**
 * @brief Return cursors determining iteration order
 * 
 * @param shard <IN> Shard to iterate over
 * @param seqno_start <IN> Starting sequence number
 * @param seqno_len <IN> Maximum number of sequence numbers to return
 * @param seqno_max <IN> Highest sequence number 
 * @param resumeCursorLen <IN> len of #resumeCursorData
 * @param ResumeCursorData <IN> Pointer to last cursor from previous 
 * return.  NULL for no previous return.
 * @param out <OUT> Return.  Free with #flashGetIterationCursorsFree
 * @return FLASH_EOK on success, otherwise on failure.
 *
 */
int rep_get_iteration_cursors(struct shard *shard, uint64_t seqno_start,
                              uint64_t seqno_len, 
                              uint64_t seqno_max,
                              const resume_cursor_t *resume_cursor_in,
                              it_cursor_t ** cursors_out)
{
    resume_cursor_t * resume_cursor;
    uint64_t resume_cursor_size;
    it_cursor_t * it_cursor;
    rep_cursor_t * cursors;
    uint64_t high_watermark;
    uint64_t num_cursors;
    int it_cursor_size;

    num_cursors = 0;

    if (SDFSimpleReplication) {

        if (seqno_max || seqno_start) {
            return FLASH_EINVAL;
        }

        // For correctness, all entries in a bucket must be grabbed in
        // one iteration
        if (seqno_len < max_hash_bucket_size()) {
            return FLASH_EINVAL;
        }
    }

    // 0's for both start and end means get everything, otherwise we
    // need to fix up the values
    if (seqno_start || seqno_max) {

        // Check the high watermark to make sure we aren't trying to get
        // something that doesn't exist
        high_watermark = flashGetHighSequence(shard);
        
        // Sequence numbers are not getting updated properly if this
        // assert happens
        if (seqno_max > high_watermark) {
            seqno_max = high_watermark;
            
            if (seqno_max - seqno_start < seqno_len) {
                seqno_len = seqno_max - seqno_start;
            }
        }
    }

    // Reduce the number of seqno to return if we've almost gotten
    // them all
    if (resume_cursor_in) {
        seqno_len = min(resume_cursor_in->seqno_left, seqno_len);
    }

    // Setup our cursors
    it_cursor_size = sizeof(it_cursor_t) + (sizeof(rep_cursor_t) * seqno_len);
    it_cursor = plat_alloc(it_cursor_size);
    if (!it_cursor) {
        plat_log_msg(20572, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "Not enough memory, rep_get_iteration_cursor() failed.");
        return FLASH_ENOMEM;
    }
        
    memset(it_cursor, 0, it_cursor_size);

    // Setup the pointers to our working cursors
    cursors = (rep_cursor_t *)it_cursor->cursors;

    resume_cursor = &it_cursor->resume_cursor;
    resume_cursor_size = sizeof(resume_cursor_t);

    if (resume_cursor_in) {
        memcpy(resume_cursor, resume_cursor_in, sizeof(resume_cursor_t));
    } else {
        memset(resume_cursor, 0, sizeof(struct flashGetIterationResumeOutput));

        if (SDFSimpleReplication) {
            // Simple replication grabs everything, so use volatile
            // object table. It's quicker, and fewer race conditions
            resume_cursor->cursor_state = VOLATILE_OBJECT_TABLE;

            resume_cursor->seqno_left = shard->quota;
        } else {
            // Start with the log table
            resume_cursor->cursor_state = LOG_TABLE;

            // try bounding the number of seqno needed
            resume_cursor->seqno_left = seqno_max - seqno_start;
        }

    }

    // Iterate over the log tables
    if (resume_cursor->cursor_state == LOG_TABLE && seqno_len) {
        num_cursors = logs_iterate(shard, resume_cursor, seqno_start, seqno_max, seqno_len, cursors);
    }

    // Iterate over the object tables
    if (resume_cursor->cursor_state == OBJECT_TABLE && seqno_len) {
        num_cursors += object_table_iterate(shard, resume_cursor, seqno_start, seqno_max, seqno_len - num_cursors, &cursors[num_cursors]);
    }
    
    // Iterate over the volatile object table in memory
    if (resume_cursor->cursor_state == VOLATILE_OBJECT_TABLE && seqno_len) {
        num_cursors += volatile_object_table_iterate(shard, resume_cursor, seqno_start, seqno_max, seqno_len - num_cursors, &cursors[num_cursors]);
    }

    // Setup the iteration cursor
    it_cursor->cursor_len = sizeof(rep_cursor_t);
    it_cursor->cursor_count = num_cursors;

    // Setup the resume cursor
    plat_assert(resume_cursor->seqno_left >= num_cursors);
    resume_cursor->seqno_left -= num_cursors;
    *cursors_out = it_cursor;

    return FLASH_EOK;
}

static int logs_iterate(struct shard *shard, resume_cursor_t * resume_cursor,
                        int seqno_start, int seqno_max, int seqno_len,
                        rep_cursor_t * cursors) {
    struct mcd_rec_shard * pshard;
    struct mcd_osd_shard * osd_shard;
    seqno_logbuf_cache_t * logbuf_seqno_cache;
    seqno_logbuf_cache_t * logbuf;
    mcd_logrec_object_t * rec;
    uint64_t logbuf_start;
    uint64_t logbuf_end;

    char * logbuf_buf_unaligned;
    char * logbuf_buf;
    int num_cursors;
    int num_logbuf;
    int rc;
    int i, r=0;

    num_cursors = 0;

    logbuf_buf_unaligned = plat_alloc(MCD_REC_LOGBUF_SIZE + MCD_OSD_BLK_SIZE - 1);
    if (!logbuf_buf_unaligned) {
        mcd_log_msg(20573,PLAT_LOG_LEVEL_FATAL, "failed to alloc buffer");
        plat_abort();
    }

    // Align the logbuf
    logbuf_buf = (char *)(((uint64_t)logbuf_buf_unaligned + MCD_OSD_BLK_SIZE - 1) 
                    & MCD_OSD_BLK_MASK);

    // Get the pshard
    osd_shard = (mcd_osd_shard_t *)shard;
    pshard = ((mcd_osd_shard_t *)shard)->pshard;

    // Persistence must be enabled
    plat_assert(osd_shard->persistent == 1);

    num_logbuf = pshard->rec_log_blks / MCD_REC_LOGBUF_BLKS;
    logbuf_seqno_cache = osd_shard->logbuf_seqno_cache;

    // Search seqno_logbuf_cache for what log bufs we need
    for (i = resume_cursor->cursor1; i < num_logbuf; i++) {
        // Get our log buf and it's start and end
        logbuf = &logbuf_seqno_cache[i];
        logbuf_start = logbuf->seqno_start;
        logbuf_end = logbuf->seqno_end;
        
        // Check if overlap
#if notyet
        if (!(logbuf_start > seqno_max) && !((logbuf_end) <= seqno_start)) {
#else
        if (1) { // Reading everything until cache is fully in place
#endif
            // Read logbuf
            rc = logbuf_read(shard, i, logbuf_buf);
            if (rc != FLASH_EOK) {
                mcd_log_msg(20574,PLAT_LOG_LEVEL_FATAL,
                            "failed to read buffer, rc=%d", rc);
                plat_abort();
            }

            // Parse logbuf
            for (r = resume_cursor->cursor2; r < MCD_REC_LOGBUF_SLOTS; r++) { 

                // Skip headers
                if ((r % MCD_REC_LOGBUF_BLK_SLOTS) == 0) {
                    continue;
                }

                // Get the record
                rec = (mcd_logrec_object_t *)(logbuf_buf + (r * sizeof(mcd_logrec_object_t)));

                // See if this is a record we want. Either:
                // 1.) We want all records, so check it's valid
                // 2.) It's seqno is in our range
                if (rec->syndrome && ((!seqno_start && !seqno_max) || 
                                     (rec->seqno >= seqno_start && rec->seqno <= seqno_max))) {

                    // Add this record onto the cursor list
                    cursors[num_cursors].seqno = rec->seqno;
                    cursors[num_cursors].syndrome = rec->syndrome;
                    cursors[num_cursors].blocks = rec->blocks;
                    // If blocks == 0, then it was either:
                    //  - delete 
                    //  - eviction
                    //  - overwrite (in cache mode)
                    // The overwrite in cache mode should get an
                    // ESTALE returned in the rep_get_by_cursor() call
                    // because the syndromes / sequence number don't
                    // match instead of the ENOENT for the delete and
                    // eviction case
                    if (!rec->blocks) {
                        cursors[num_cursors].tombstone = 1;
                    }
                    cursors[num_cursors].blk_offset = rec->blk_offset;
                    num_cursors++;

                    // We have enough. Time to get out of here
                    if (num_cursors == seqno_len) {
                        goto out;
                    }
                    plat_assert(num_cursors <= seqno_len);
                }
            }

            // Make sure we start from the beginning on a new logbuf
            resume_cursor->cursor2 = 0;
        }
    }
    
out:

    // Set this point for the next traversal
    resume_cursor->cursor1 = i;
    resume_cursor->cursor2 = r + 1;

    // Advance the state to start looking at the object table
    if (seqno_len != num_cursors ||
        i >= num_logbuf) {
        resume_cursor->cursor_state = OBJECT_TABLE;
        resume_cursor->cursor1 = 0;
        resume_cursor->cursor2 = 0;
    }

    plat_free(logbuf_buf_unaligned);

    return num_cursors;
}

static int object_table_iterate(struct shard *shard, resume_cursor_t * resume_cursor,
                                int seqno_start, int seqno_max, int seqno_len,
                                rep_cursor_t * cursors) {
    struct mcd_rec_shard * pshard;
    struct mcd_osd_shard * osd_shard;
    mcd_rec_flash_object_t * rec;
    osd_state_t * context;
    uint64_t read_size;
    uint64_t total_num_blks;
    uint64_t cursors_left;
    uint64_t num_cursors;
    uint64_t offset;
    uint64_t i=0, j=0;
    char * data;
    char * data_buf;
    int rc;

    // Get the pshard
    pshard = ((mcd_osd_shard_t *)shard)->pshard;
    osd_shard = (mcd_osd_shard_t *)shard;

    // Setup our loop
    total_num_blks = pshard->rec_log_blks / sizeof(mcd_rec_flash_object_t);
    cursors_left = seqno_len;
    context = mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REP_ITER );
    
    num_cursors = 0;

    read_size = MCD_REC_UPDATE_IOSIZE;
    data_buf = plat_alloc(read_size + MCD_OSD_BLK_SIZE - 1);
    if (!data_buf) {
        mcd_log_msg(20561,PLAT_LOG_LEVEL_FATAL, "failed to allocate buffer");
        plat_abort();
    }

    // Align the buffer
    data = (char *)(((uint64_t)data_buf + MCD_OSD_BLK_SIZE - 1) 
                    & MCD_OSD_BLK_MASK);

    // Outer loop reads in big chunks of data
    for (i = resume_cursor->cursor1; i < pshard->rec_table_blks;) {
        offset = i; 

        // Read from flash
        rc = table_chunk_op(context, osd_shard, TABLE_READ, offset, read_size / MCD_OSD_BLK_SIZE, data);
        if (rc != FLASH_EOK) {
            mcd_log_msg(20574,PLAT_LOG_LEVEL_FATAL,
                        "failed to read buffer, rc=%d", rc);
            plat_abort();
        }

        // Inner loop goes through those big chunks
        for (j = resume_cursor->cursor2; j < read_size / sizeof(mcd_rec_flash_object_t); j++) {
            rec = &((mcd_rec_flash_object_t *)data)[j];

            // See if this is a record we want. Either:
            // 1.) We want all records, so check it's valid
            // 2.) It's seqno is in our range
            if (rec->syndrome && ((!seqno_start && !seqno_max) || 
                                  (rec->seqno >= seqno_start && rec->seqno <= seqno_max))) {
                cursors[num_cursors].seqno = rec->seqno;
                cursors[num_cursors].syndrome = rec->syndrome;
                cursors[num_cursors].blocks = rec->blocks;
                cursors[num_cursors].tombstone = rec->tombstone;
                cursors[num_cursors].blk_offset = (i * MCD_OSD_BLK_SIZE / sizeof(mcd_rec_flash_object_t)) + j;
                
                num_cursors++;

                if (num_cursors == seqno_len) {
                    goto out;
                }
            }
        }

        resume_cursor->cursor2 = 0;

        i += min(read_size / MCD_OSD_BLK_SIZE, 1);
    }

out:

    resume_cursor->cursor1 = i;
    resume_cursor->cursor2 = j + 1;

    plat_free(data_buf);

    return num_cursors;
}

static int max_hash_bucket_size()
{
    if (flash_settings.enable_fifo) {
        return MCD_OSD_BUCKET_SIZE;
    } else {
        return MCD_OSD_BUCKET_SIZE + Mcd_osd_overflow_depth;
    }
}

static int volatile_object_table_iterate(struct shard *shard, resume_cursor_t * resume_cursor,
                                         int seqno_start, int seqno_max, int seqno_len,
                                         rep_cursor_t * cursors)
{
    struct mcd_rec_shard * pshard;
    struct mcd_osd_shard * osd_shard;
    mcd_osd_bucket_t * bucket;
    fthLock_t * bucket_lock;
    fthWaitEl_t * wait;
    mcd_osd_hash_t * entry;
    uint64_t num_cursors;
    uint64_t i, j;
    uint16_t * overflow_index;
    uint16_t index;
//    int percentage;

    plat_assert(seqno_len > max_hash_bucket_size());

    // Get the pshard
    pshard = ((mcd_osd_shard_t *)shard)->pshard;
    osd_shard = (mcd_osd_shard_t *)shard;

    num_cursors = 0;

    // Iterate over the table
    for (i = resume_cursor->cursor1; i < (osd_shard->hash_size / MCD_OSD_BUCKET_SIZE); i++) {
        // Lock bucket
        bucket_lock = osd_shard->bucket_locks +
            ((i * MCD_OSD_BUCKET_SIZE) / osd_shard->lock_bktsize);
        wait = fthLock(bucket_lock, 0, NULL);
        
        bucket = osd_shard->hash_buckets + i;

        // Get the head of the entries in the bucket
        entry = osd_shard->hash_table + (i * MCD_OSD_BUCKET_SIZE);

        // Walk entries in bucket
        for (j = 0; j < bucket->next_item; j++, entry++) {
            plat_assert(entry->used);

            cursors[num_cursors].seqno = 0;
            cursors[num_cursors].syndrome = entry->syndrome;
            cursors[num_cursors].blocks = entry->blocks;
            cursors[num_cursors].tombstone = 0;
            cursors[num_cursors].blk_offset = entry->address;
            cursors[num_cursors].bucket = i;
            
            num_cursors++;
            
            plat_log_msg(20575, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                         "add cursor[%ld][%ld]: syndrome: %x", i, j, entry->syndrome);

            if (num_cursors == seqno_len) {
                fthUnlock(wait);
                goto out;
            }
        }
        // Handle overflow here
        if (bucket->overflowed) {

            // Get first entry of this bucket's overflow table
            entry = osd_shard->overflow_table +
                (((i * MCD_OSD_BUCKET_SIZE) / osd_shard->lock_bktsize)
                 * Mcd_osd_overflow_depth);

            overflow_index  = osd_shard->overflow_index + 
                (((i * MCD_OSD_BUCKET_SIZE) / osd_shard->lock_bktsize) 
                * Mcd_osd_overflow_depth);

            // Walk overflow table
            for (j = 0; j < Mcd_osd_overflow_depth; j++, entry++) {
                if (!entry->used) {
                    continue;
                }

                index = (bucket - osd_shard->hash_buckets)
                            % osd_shard->lock_bktsize;

                // If this entry came from a bucket we are walking,
                // then add it
                if (overflow_index[j] == index) {
                    plat_assert(entry->used);

                    cursors[num_cursors].seqno = 0;
                    cursors[num_cursors].syndrome = entry->syndrome;
                    cursors[num_cursors].blocks = entry->blocks;
                    cursors[num_cursors].tombstone = 0;
                    cursors[num_cursors].blk_offset = entry->address;
            
                    num_cursors++;
            
                    if (num_cursors == seqno_len) {
                        fthUnlock(wait);
                        goto out;
                    }
                }
            }
        }

        fthUnlock(wait);

        // Update the data copy progress
//        percentage = (i * 100) / (osd_shard->hash_size / MCD_OSD_BUCKET_SIZE);
//        update_data_copy_progress(shard, percentage);

        // Make sure we have enough room for another pass, if not send
        // what we have
        if ((num_cursors + max_hash_bucket_size()) > seqno_len) {
            break;
        }
    }

out:

    // Update the data copy progress
//    percentage = (i * 100) / (osd_shard->hash_size / MCD_OSD_BUCKET_SIZE);
//    update_data_copy_progress(shard, percentage);

    resume_cursor->cursor1 = i + 1;
    resume_cursor->cursor2 = 0;

    return num_cursors;
}

fthWaitEl_t * rep_lock_bucket(struct shard * shard, rep_cursor_t * rep_cursor)
{
    struct mcd_rec_shard * pshard;
    struct mcd_osd_shard * osd_shard;
    fthLock_t * bucket_lock;
    fthWaitEl_t * wait;
    
    pshard = ((mcd_osd_shard_t *)shard)->pshard;
    osd_shard = (mcd_osd_shard_t *)shard;

    bucket_lock = osd_shard->bucket_locks +
        ((rep_cursor->bucket * MCD_OSD_BUCKET_SIZE) / osd_shard->lock_bktsize);

    // Read only
    wait = fthLock(bucket_lock, 0, NULL);
    
    return wait;
}

void rep_unlock_bucket(fthWaitEl_t * wait)
{
    fthUnlock(wait);
}

int check_object_exists(struct shard *shard, rep_cursor_t * rep_cursor)
{
    struct mcd_rec_shard * pshard;
    struct mcd_osd_shard * osd_shard;

    mcd_osd_bucket_t * bucket;
    mcd_osd_hash_t * entry;
    int i;
    
    pshard = ((mcd_osd_shard_t *)shard)->pshard;
    osd_shard = (mcd_osd_shard_t *)shard;

    bucket = osd_shard->hash_buckets + rep_cursor->bucket;

    // Get the head of the entries in the bucket
    entry = osd_shard->hash_table + (rep_cursor->bucket * MCD_OSD_BUCKET_SIZE);

    // Walk entries in bucket
    for (i = 0; i < bucket->next_item; i++, entry++) {
        plat_assert(entry->used);

        if ( (uint16_t)(rep_cursor->syndrome) == entry->syndrome ) {
            return 1;
        }
    }
        
    if (bucket->overflowed) {
        // Get first entry of this bucket's overflow table
        entry = osd_shard->overflow_table +
            (((rep_cursor->bucket * MCD_OSD_BUCKET_SIZE) / osd_shard->lock_bktsize)
             * Mcd_osd_overflow_depth);
        
        // Walk overflow table
        for (i = 0; i < Mcd_osd_overflow_depth; i++, entry++) {
            if (!entry->used) {
                continue;
            }

            if ( (uint16_t)(rep_cursor->syndrome) == entry->syndrome ) {
                return 1;
            }
        }
    }

    return 0;
}

/** 
 * @brief Get using a cursor from #flashGetIterationCursors
 * 
 * @param shard <IN> Shard to get from
 * @param cursor <IN> Cursor from #flashGetIterationCursors
 * @param metaData <OUT> Meta-data from retrieval including key and 
 * data length.
 * @param key <OUT> Key output.  Free with plat_free. Length in #metaData.
 * @param data <OUT> Data output.  Free with plat_free. Length in #metaData.
 * @param flags <IN> Flags
 * @return FLASH_EOK on success, otherwise on failure.  FLASH_ESTALE
 * may be returned when the data is no longer available because it has
 * been superceded. 
 *
 * We also want a flash "stale" return, where a newer update 
 */

int rep_get_by_cursor(struct shard *shard, int cursor_len, const void *cursor,
                      struct objMetaData *metaData, char **key, void **data, int flags, time_t flush_time) {
    mcd_osd_fifo_wbuf_t * wbuf = 0;
    rep_cursor_t * rep_cursor;
    mcd_osd_meta_t * meta;
    fthWaitEl_t * wait;
    int blocks_to_read;
    uint64_t syndrome;
    char * data_buf;
    char * buf;
    char * s;
    int rc;

    rep_cursor = (rep_cursor_t *)cursor;

    // Find any transport errors
    if (cursor_len != sizeof(rep_cursor_t)) {
        return FLASH_EINVAL;
    }

    // If its been deleted or evicted just return that it doesn't exist

    data_buf = (char *)plat_alloc(rep_cursor->blocks * MCD_OSD_BLK_SIZE  + 
                                  MCD_OSD_BLK_SIZE - 1 );
    if (!data_buf) {
        mcd_log_msg(20328,PLAT_LOG_LEVEL_ERROR, 
                    "failed to allocate data buffer" );
        return FLASH_ENOMEM;
    }

    // Align the buffer
    buf = (char *)(((uint64_t)data_buf + MCD_OSD_BLK_SIZE - 1) 
                    & MCD_OSD_BLK_MASK);

    // Lock bucket
    wait = rep_lock_bucket(shard, rep_cursor);

    // Check for the entry in the bucket
    if (!check_object_exists(shard, rep_cursor)) {
        rep_unlock_bucket(wait);
        plat_free(data_buf);
        plat_log_msg(20576, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                     "data copy send: stale: no longer on flash: %d syndrome requested: 0x%x", rep_cursor->bucket, rep_cursor->syndrome);

        return FLASH_ESTALE;
    }

    // Read the object from flash
    blocks_to_read = max(rep_cursor->blocks, 1);
    rc = obj_read(shard, rep_cursor->blk_offset, blocks_to_read * MCD_OSD_BLK_SIZE, (void **)&buf, &wbuf);

    // Unlock bucket
    rep_unlock_bucket(wait);

    meta = (mcd_osd_meta_t *)buf;
    s = (char *)(buf + sizeof(mcd_osd_meta_t));

    // Validate return values since we could get junk
    if (meta->key_len > SDF_SIMPLE_KEY_SIZE) {
        plat_log_msg(20577, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                     "data copy send: stale: invalid keylen: %d syndrome requested: 0x%x", meta->key_len, rep_cursor->syndrome);
        plat_free(data_buf);
        if (wbuf) {
            (void) __sync_fetch_and_sub(&wbuf->ref_count, 1);
        }
        return FLASH_ESTALE;
    }

    syndrome = hash((const unsigned char *)s, meta->key_len, 0);
    
    // Check that we have the right object
    // 1.) Make sure we have a matching syndrome
    if ((uint16_t)(syndrome >> 48) != rep_cursor->syndrome) {
        plat_log_msg(20578, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                     "data copy send: stale: syndrome found: 0x%lx syndrome requested: 0x%x", syndrome, rep_cursor->syndrome);
        plat_free(data_buf);
        if (wbuf) {
            (void) __sync_fetch_and_sub(&wbuf->ref_count, 1);
        }
        return FLASH_ESTALE;
    }

    // Check to see if object has expired or been flushed
    if ((flush_time <= (*(flash_settings.pcurrent_time)) && flush_time >= meta->create_time) ||
	(meta->expiry_time && ((*(flash_settings.pcurrent_time)) > meta->expiry_time))) {
        plat_log_msg(20579, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                     "data copy send: stale: expired/flushed syndrome: 0x%x flush_time: %d create_time: %d", rep_cursor->syndrome, (uint32_t)flush_time, meta->create_time);
        plat_free(data_buf);
        if (wbuf) {
            (void) __sync_fetch_and_sub(&wbuf->ref_count, 1);
        }

        return FLASH_ESTALE;
    }

    // If this is a delete. The rest is invalid
    if (!rep_cursor->tombstone) {
        // 2.) Make sure we have a matching seqno
        if (!SDFSimpleReplication && rep_cursor->seqno != meta->seqno) {
            plat_free(data_buf);
            if (wbuf) {
                (void) __sync_fetch_and_sub(&wbuf->ref_count, 1);
            }

            return FLASH_ESTALE;
        }
        
        // Copy over the data
        *data = plat_alloc(meta->data_len);
        if (!*data) {
            plat_free(data_buf);
            if (wbuf) {
                (void) __sync_fetch_and_sub(&wbuf->ref_count, 1);
            }

            return FLASH_ENOMEM;
        }
        
        memcpy(*data, buf + sizeof(mcd_osd_meta_t) + meta->key_len, meta->data_len);
    }

    // Copy over the key
    *key = plat_alloc(meta->key_len);
    if (!*key) {
        plat_free(data_buf);
        plat_free(*data);
        if (wbuf) {
            (void) __sync_fetch_and_sub(&wbuf->ref_count, 1);
        }

        return FLASH_ENOMEM;
    }

    memcpy(*key, buf + sizeof(mcd_osd_meta_t), meta->key_len);

    // Copy over the meta data
    metaData->keyLen = meta->key_len;
    metaData->dataLen = meta->data_len;
    metaData->createTime = meta->create_time;
    metaData->expTime = meta->expiry_time;
    metaData->sequence = meta->seqno;

    plat_log_msg(20580, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                 "data copy send: key: %s len: %d data: 0x%lx tombstone: %d syndrome: 0x%x", *key, meta->key_len, (uint64_t)*data, rep_cursor->tombstone, rep_cursor->syndrome);

    plat_free(data_buf);

    if (wbuf) {
        (void) __sync_fetch_and_sub(&wbuf->ref_count, 1);
    }

    if (rep_cursor->tombstone || (!SDFSimpleReplication && !rep_cursor->syndrome)) {
       return FLASH_ENOENT;
    }
    
    return FLASH_EOK;
}

/** 
 * @brief Get using a cursor from #flashGetIterationCursors
 * 
 * @param shard <IN> Shard to get from
 * @param cursorLenIn <IN> Length of cursor
 * @param cursorIn <IN> CursorOut from previous call to #flashGetIteration
 * @param offset <IN> Offset from continuation cursor in the closed interval
 * 0 .. #interval - 1.
 * @param interval <IN> Skip interval between cursors
 * common cursors should be returned. 
 * 
 * data length.
 * @param metaData <OUT> Meta-data from retrieval including key and 
 * @param key <OUT> Key output.  Free with plat_free. Length in #metaData.
 * @param data <OUT> Data output.  Free with plat_free. Length in #metaData.
 * @param flags <IN> Flags
 * @return FLASH_EOK on success, otherwise on failure.
 *
 */
SDF_status_t flashGetIteration(struct shard *shard,
                               int cursorLenIn,
                               const void *cursorIn, 
                               struct objMetaData *metaData,
                               char *objMetaData,
                               char **key,
                               void **data,
                               int *cursorLenOut,
                               const void **cursorOut,
                               int flags) {
    // Not implemented
    return 0;
}

int replication_init(void) {

    Ssd_fifo_ops.flashGetIterationCursors = rep_get_iteration_cursors;
    Ssd_fifo_ops.flashGetByCursor = rep_get_by_cursor;

    sdf_iterate_cursors_progress = rep_iterate_cursors_progress;

    return 0;
}

int seqno_cache_init(struct shard *shard) {
    struct mcd_rec_shard * pshard;
    struct mcd_osd_shard * osd_shard;
    int num_logbuf;

    pshard = ((mcd_osd_shard_t *)shard)->pshard;
    osd_shard = (mcd_osd_shard_t *)shard;

    num_logbuf = pshard->rec_log_blks / MCD_REC_LOGBUF_BLKS;
    
    // Create a seqno cache per shard
    osd_shard->logbuf_seqno_cache = plat_alloc(sizeof(seqno_logbuf_cache_t) * num_logbuf);
    if (!osd_shard->logbuf_seqno_cache) {
        mcd_log_msg(20581,PLAT_LOG_LEVEL_ERROR, "failed to alloc seqno cache");
        return FLASH_ENOMEM;
    }

    memset(osd_shard->logbuf_seqno_cache, 0, sizeof(seqno_logbuf_cache_t) * num_logbuf);

    return 0;
}

// Read a particular object off flash
static int obj_read(struct shard * shard, uint64_t blk_offset, uint64_t nbytes, void ** data, mcd_osd_fifo_wbuf_t ** wbuf) {
    osd_state_t * context;
    struct mcd_osd_shard * osd_shard;
    void * tmp_data;
    uint64_t tmp_offset;
    uint64_t offset;
    int rc;

    osd_shard = (mcd_osd_shard_t *)shard;
    context = mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REP_READ );
    
    if (osd_shard->use_fifo) {
        int committed;

        // Check if buffered
        tmp_data = mcd_fifo_find_buffered_obj(osd_shard, 0, 0, blk_offset, wbuf, &committed);

        if (!committed) {
            *data = tmp_data;
            return 0;
        }
    }
    *wbuf = 0;

    tmp_offset = osd_shard->rand_table[blk_offset / Mcd_osd_rand_blksize]
        + (blk_offset % Mcd_osd_rand_blksize);
    offset = tmp_offset * Mcd_osd_blk_size;

    rc = mcd_fth_aio_blk_read((void *)context,
                              *data,
                              offset,
                              nbytes);
    if (rc != FLASH_EOK) {
        mcd_log_msg(20574,PLAT_LOG_LEVEL_FATAL,
                    "failed to read buffer, rc=%d", rc);
    }

    return rc;
}

static int logbuf_read(struct shard * shard, int logbuf_number, char * logbuf_buf) {
    osd_state_t * context;
    struct mcd_osd_shard * mcd_shard;
    struct mcd_rec_shard * pshard;
    uint64_t num_logbuf;
    uint64_t log_blk_start; // where the log actually starts
    uint64_t tmp_offset;	// in blks
    uint64_t log_blk_offset; // offset into the log
    uint64_t offset;	// physical offset into log
    uint64_t end_blk;
    uint64_t start_seg;
    uint64_t end_seg;

    int rc;
    int lognum;
    
    // Get the pshard
    pshard = ((mcd_osd_shard_t *)shard)->pshard;
    mcd_shard = (mcd_osd_shard_t *)shard;

    context = mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REP_LGRD );
    
    // Figure out how many logbufs we have total
    num_logbuf = pshard->rec_log_blks / MCD_REC_LOGBUF_BLKS;

    // Figure out which log the requested logbuf is in
    lognum = logbuf_number / (num_logbuf / MCD_REC_NUM_LOGS);

    // Get the offset to that log
    log_blk_start = relative_log_offset(pshard, lognum);

    // Get the offset to that logbuf
    tmp_offset = logbuf_number % (num_logbuf / MCD_REC_NUM_LOGS);
    log_blk_offset = log_blk_start + (tmp_offset * MCD_REC_LOGBUF_BLKS);
    end_blk = log_blk_offset + MCD_REC_LOGBUF_BLKS - 1;

    // calculate start/end segments
    start_seg = log_blk_offset / Mcd_osd_segment_blks;
    end_seg   = end_blk / Mcd_osd_segment_blks;
    plat_assert_always( start_seg == end_seg );

    // Get the physical offset
    offset = mcd_shard->segments[start_seg] +
        (log_blk_offset % Mcd_osd_segment_blks);

    // read the logbuf buffer
    rc = mcd_fth_aio_blk_read((void *)context,
                              logbuf_buf,
                              offset * Mcd_osd_blk_size,
                              MCD_REC_LOGBUF_BLKS * Mcd_osd_blk_size);
    if (rc != FLASH_EOK) {
        mcd_log_msg(20574,PLAT_LOG_LEVEL_FATAL,
                    "failed to read buffer, rc=%d", rc);
        plat_abort();
    }

    return rc;
}

/** @brief Free return from #flashGetIterationCursors */
void flashGetIterationCursorsFree(struct flashGetIterationOutput *out)
{
    // Not currently used
    
    // Free it
    plat_free(out);
}

uint64_t rep_seqno_get(struct shard * shard)
{
    struct sdf_replicator *replicator = sdf_shared_state.config.replicator;
    SDF_vnode_t mynode = sdf_shared_state.config.my_node;
    
    return rr_get_seqno_rpc(replicator, mynode, shard);
}

// For now, we are doing everything
// Update the seqno logbuf cache
int rep_logbuf_seqno_update(struct shard * shard, uint64_t logbuf_num, uint64_t seq_num) {
    return 0;
}

/* The percentage complete in the iteration process
 *
 * Returns a percentage (0 - 100)
 *
 * Assumes shard sizes are the same between nodes
 */
int rep_iterate_cursors_progress(struct shard * shard, resume_cursor_t * cursor)
{
    //#define SHOW_REPLICATION_STATUS 1
    struct mcd_rec_shard * pshard;
    struct mcd_osd_shard * osd_shard;
    int percentage = 0;
    uint64_t finished_ops = 0;
    uint64_t logbuf_inner;
    uint64_t logbuf_outer;
    uint64_t logbuf_ops;
    uint64_t object_table_inner;
    uint64_t object_table_outer;
    uint64_t object_table_ops;
    uint64_t total_ops;

    if (!SDFSimpleReplication) {
        pshard = ((mcd_osd_shard_t *)shard)->pshard;
        
        logbuf_inner = MCD_REC_LOGBUF_SLOTS;
        logbuf_outer = pshard->rec_log_blks / MCD_REC_LOGBUF_BLKS;
        logbuf_ops = logbuf_inner * logbuf_outer;
        
        object_table_inner = MCD_REC_UPDATE_IOSIZE / sizeof(mcd_rec_flash_object_t);
        object_table_outer = pshard->rec_table_blks;
        object_table_ops = object_table_inner * object_table_outer;
        
        total_ops = logbuf_ops + object_table_ops;
        
        if (!total_ops) {
            plat_assert(0); // is this really wrong?
            return 0;
        }
        
        if (cursor->cursor_state == LOG_TABLE) {
            finished_ops = (cursor->cursor1 * logbuf_inner) + cursor->cursor2;
            
#if SHOW_REPLICATION_STATUS
            printf("[%ld / %ld][%d / %ld] ", finished_ops,
                   logbuf_ops,
                   0, 
                   object_table_ops
                );
#endif
        }
        
        if (cursor->cursor_state == OBJECT_TABLE) {
            // Add in all the logtable blocks
            finished_ops = logbuf_ops;
            
            // Add in the object table blocks
            finished_ops += (cursor->cursor1 * object_table_inner) + cursor->cursor2;
            
#if SHOW_REPLICATION_STATUS
            printf("[%ld / %ld][%ld / %ld] ", logbuf_ops,
                   logbuf_ops,
                   (cursor->cursor1 * object_table_inner) + cursor->cursor2,
                   object_table_ops
                );
#endif
        }
        
    } else {
        osd_shard = (mcd_osd_shard_t *)shard;
        total_ops = osd_shard->hash_size / MCD_OSD_BUCKET_SIZE;
        finished_ops = cursor->cursor1;
    }

#ifndef SDFAPIONLY
    percentage = floor((finished_ops * 100) / total_ops);
#else
    percentage = finished_ops * 100 / total_ops;
#endif

#if SHOW_REPLICATION_STATUS
    printf("%d%%\n", percentage);
#endif

    return percentage;
}

#if notyet
// Update the seqno logbuf cache
int rep_logbuf_seqno_invalidate(struct shard * shard, uint64_t logbuf_num) {
    return 0;
}

// For recovery
int reconstruct_seqno_cache() {
    return 0;
}
#endif // notyet
