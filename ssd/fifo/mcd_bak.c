/*
 * File:   mcd_bak.c
 * Author: Wayne Hineman
 *
 * Created on July 1, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_bak.c 15035 2010-11-12 16:50:11Z briano $
 */

#include <stdio.h>
#include <signal.h>
#include <aio.h>
#include <linux/fs.h>

#include "common/sdftypes.h"
#include "common/sdfstats.h"
#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/signal.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/time.h"
#include "platform/unistd.h"
#include "utils/hash.h"
#include "utils/properties.h"
#include "fth/fthMbox.h"

#include "fth/fth.h"
#include "fth/fthSem.h"
#include "fth/fthLock.h"
#include "ssd/ssd_local.h"
#include "ssd/fifo/fifo.h"

// #include "memcached.h"
#include "mcd_osd.h"
#include "mcd_rec.h"
#include "mcd_bak.h"

#undef  mcd_dbg_msg
#define mcd_dbg_msg(...)                                                \
    plat_log_msg( PLAT_LOG_ID_INITIAL,                                  \
                  PLAT_LOG_CAT_SDF_APP_MEMCACHED_BACKUP,                \
                  __VA_ARGS__ )

// FIXME: ffdc parser error requires this
#define foo

/************************************************************************
 *                                                                      *
 *                      Backup functions                                *
 *                                                                      *
 ************************************************************************/

int
backup_report_version( char ** bufp, int * lenp )
{
    return plat_snprintfcat( bufp, lenp,
                             "backup protocol %d.0.0\r\n"
                             "restore protocol %d.0.0\r\n",
                             MCD_BAK_BACKUP_PROTOCOL_VERSION,
                             MCD_BAK_RESTORE_PROTOCOL_VERSION );
}

int
backup_init( mcd_osd_shard_t * shard )
{
    mcd_bak_state_t           * bk;

    // create backup descriptor
    bk = plat_alloc( sizeof( mcd_bak_state_t ) );
    if ( bk == NULL ) {
        mcd_bak_msg( 20063, PLAT_LOG_LEVEL_ERROR,
                     "failed to alloc memory for backup desc" );
        return FLASH_ENOMEM;
    }

    // initialize
    memset( bk, 0, sizeof( mcd_bak_state_t ) );
    bk->full_backup_required = 1;
    bk->snapshot_logbuf      = MCD_BAK_SNAPSHOT_LOGBUF_INITIAL;
    TAILQ_INIT( &bk->ps_list );

    // install backup state in shard
    shard->backup = bk;

    return 0;
}


inline int
backup_incr_pending_seqno( mcd_osd_shard_t * shard )
{
    mcd_bak_state_t           * bk = shard->backup;
    int                         window = bk->seqno_window;

    (void) __sync_add_and_fetch( &bk->seqno_pending[window], 1 );

    return window;
}


inline void
backup_decr_pending_seqno( mcd_osd_shard_t * shard, int window )
{
    mcd_bak_state_t           * bk = shard->backup;

    (void) __sync_sub_and_fetch( &bk->seqno_pending[window], 1 );
}


void
backup_maintain_bitmaps( mcd_osd_shard_t * shard, uint32_t blk_offset,
                         int delete )
{
    int                         map_offset;
    mcd_osd_segment_t         * segment;

	return;
    mcd_bak_msg( 20064, PLAT_LOG_LEVEL_TRACE,
                 "ENTERING, shardID=%lu, off=%u, del=%d",
                 shard->id, blk_offset, delete );

    segment = shard->segment_table[blk_offset / Mcd_osd_segment_blks];

    if(!segment)
        return;

    map_offset = blk_offset - segment->blk_offset;
    map_offset /= segment->class->slab_blksize;

    if ( delete ) {
        (void) __sync_fetch_and_or( &segment->update_map[map_offset / 64],
                                    Mcd_osd_bitmap_masks[map_offset % 64] );
        (void) __sync_fetch_and_and( &segment->alloc_map[map_offset / 64],
                                     ~Mcd_osd_bitmap_masks[map_offset % 64] );
    } else {
        (void) __sync_fetch_and_or( &segment->update_map[map_offset / 64],
                                    Mcd_osd_bitmap_masks[map_offset % 64] );
        (void) __sync_fetch_and_or( &segment->alloc_map[map_offset / 64],
                                    Mcd_osd_bitmap_masks[map_offset % 64] );
    }

    return;
}


void
backup_merge_bitmaps( mcd_osd_shard_t * shard )
{
    int                         i, j, k;
    int                         blksize;
    int                         bitmap_size;

    mcd_bak_msg( 20065, PLAT_LOG_LEVEL_TRACE,
                 "ENTERING, shardID=%lu", shard->id );

    for ( i = 0, blksize = 1; i < MCD_OSD_MAX_NCLASSES; i++ ) {
        bitmap_size = Mcd_osd_segment_blks / blksize / 8;
        if ( sizeof(uint64_t) > bitmap_size ) {
            bitmap_size = sizeof(uint64_t);
        }

        for ( j = 0; j < shard->slab_classes[i].num_segments; j++ ) {
            mcd_osd_segment_t * seg = shard->slab_classes[i].segments[j];
            for ( k = 0; k < bitmap_size / sizeof(uint64_t); k++ ) {
                (void) __sync_fetch_and_or( &seg->update_map[k],
                                            seg->update_map_s[k] );
            }
        }
    }

    return;
}


void
backup_snapshot_bitmaps( mcd_osd_shard_t * shard )
{
    int                         i, j;
    int                         blksize;
    int                         bitmap_size;

    mcd_bak_msg( 20065, PLAT_LOG_LEVEL_TRACE,
                 "ENTERING, shardID=%lu", shard->id );

    for ( i = 0, blksize = 1; i < MCD_OSD_MAX_NCLASSES; i++ ) {
        bitmap_size = Mcd_osd_segment_blks / blksize / 8;
        if ( sizeof(uint64_t) > bitmap_size ) {
            bitmap_size = sizeof(uint64_t);
        }

        for ( j = 0; j < shard->slab_classes[i].num_segments; j++ ) {
            mcd_osd_segment_t * seg = shard->slab_classes[i].segments[j];

            memcpy( seg->alloc_map_s, seg->alloc_map, bitmap_size );
            memcpy( seg->update_map_s, seg->update_map, bitmap_size );

            // reset update bitmap for next incremental backup
            memset( seg->update_map, 0, bitmap_size );
        }
    }

    return;
}


int
backup_start_prepare( mcd_osd_shard_t * shard, int full_backup )
{
    bool                        success = false;
    mcd_bak_state_t           * bk = shard->backup;

    mcd_bak_msg( 20065, PLAT_LOG_LEVEL_TRACE,
                 "ENTERING, shardID=%lu", shard->id );

    plat_assert( bk != NULL );

    if ( !shard->persistent || shard->replicated ) {
        return FLASH_EPERM;        // non-persistent/replicated
    }

    // set backup in progress
    while ( !success &&
            !shard->backup_running &&
            !shard->restore_running ) {
        success =
            __sync_bool_compare_and_swap( &shard->backup_running, 0, 1 );
    }

    if ( !success ) {
        return FLASH_EBUSY;        // another backup/restore is running
    }

    // check full backup requirement
    if ( !full_backup &&
         (bk->backup_prev_seqno == 0 || bk->full_backup_required) ) {
        shard->backup_running = 0;
        return FLASH_EINVAL;       // full backup required
    }

    return 0;
}


int
backup_start( mcd_osd_shard_t * shard, int full_backup, uint64_t * prev_seqno,
              uint64_t * backup_seqno, time_t * backup_time )
{
    mcd_bak_state_t           * bk = shard->backup;

    mcd_bak_msg( 20065, PLAT_LOG_LEVEL_TRACE,
                 "ENTERING, shardID=%lu", shard->id );

    plat_assert( bk != NULL );
    plat_assert( shard->backup_running );

    // get a sequence number
    bk->backup_curr_seqno = __sync_add_and_fetch( &shard->sequence, 1 );

    // switch the pending sequence number window
    bk->seqno_window = 1 - bk->seqno_window;

    // wait for sequence numbers less than the backup to commit
    while ( bk->seqno_pending[ 1 - bk->seqno_window ] > 0 ) {
        fthYield( 1 );
    }

    // snapshot the bitmaps
    bk->snapshot_logbuf = log_get_buffer_seqno( shard );
    log_sync( shard );

    // reset stats
    memset( &bk->stats, 0, sizeof( mcd_bak_stats_t ) );

    // return information
    * prev_seqno   = full_backup ? 0 : bk->backup_prev_seqno;
    * backup_seqno = bk->backup_curr_seqno;
    * backup_time  = time( 0 );

    plat_assert( shard->backup_running );
    plat_assert( !shard->restore_running );

    return 0;
}


int
backup_end( mcd_osd_shard_t * shard, int cancel )
{
    mcd_bak_state_t           * bk = shard->backup;

    mcd_bak_msg( 20065, PLAT_LOG_LEVEL_TRACE,
                 "ENTERING, shardID=%lu", shard->id );

    // cancel/complete a backup while restore running
    if ( shard->restore_running ) {
        return FLASH_EBUSY;
    }

    // cancel/complete a backup that isn't running
    if ( !shard->backup_running ) {
        plat_assert( bk->backup_prev_seqno == bk->backup_curr_seqno );
        return FLASH_EPERM;
    }

    // backup cancelled
    if ( cancel ) {
        // merge the snapshot update bitmap back into the live bitmap
        backup_merge_bitmaps( shard );

        bk->backup_curr_seqno = bk->backup_prev_seqno; // revert bkup seqno
        bk->snapshot_complete = 0;
    }

    // backup completed
    else {
        bk->backup_prev_seqno = bk->backup_curr_seqno; // advance bkup seqno
        bk->snapshot_complete = 0;
        bk->full_backup_required = 0;
    }

    mcd_bak_msg( 40119, PLAT_LOG_LEVEL_DEBUG,
                 "container (%s) backup: %lu of %lu allocated segments, "
                 "empty=%lu, error=%lu, total=%lu; "
                 "objects=%lu, blks=%lu; deleted=%lu, blks=%lu; "
                 "expired=%lu, blks=%lu; flushed=%lu, blks=%lu; "
                 "error_obj=%lu, error_blks=%lu",
                 mcd_osd_container_cname(shard->cntr), bk->stats.seg_read,
                 shard->blk_allocated / Mcd_osd_segment_blks,
                 bk->stats.seg_empty, bk->stats.seg_error,
                 shard->total_segments,
                 bk->stats.obj_count, bk->stats.obj_blocks,
                 bk->stats.deleted_count, bk->stats.deleted_blocks,
                 bk->stats.expired_count, bk->stats.expired_blocks,
                 bk->stats.flushed_count, bk->stats.flushed_blocks,
                 bk->stats.error_obj_count, bk->stats.error_obj_blocks );

    // set backup not in progress
    shard->backup_running = 0;

    return 0;
}


/************************************************************************
 *                                                                      *
 *                      Restore functions                               *
 *                                                                      *
 ************************************************************************/

int
restore_start( mcd_osd_shard_t * shard, uint64_t prev_seqno,
               uint64_t curr_seqno, uint32_t client_version,
               uint64_t * err_seqno )
{
    bool                        success = false;
    mcd_bak_state_t           * bk = shard->backup;

    mcd_bak_msg( 20065, PLAT_LOG_LEVEL_TRACE,
                 "ENTERING, shardID=%lu", shard->id );

    plat_assert( bk != NULL );

    if ( !shard->persistent || shard->replicated ) {
        return FLASH_EPERM;        // non-persistent/replicated
    }

    // set restore in progress
    while ( !success &&
            !shard->backup_running &&
            !shard->restore_running ) {
        success =
            __sync_bool_compare_and_swap( &shard->restore_running, 0, 1 );
    }

    if ( !success ) {
        return FLASH_EBUSY;        // another backup/restore is running
    }

    // container must be empty
    if ( 0 < shard->num_objects ) {
        shard->restore_running = 0;
        return FLASH_EEXIST;
    }

    // check previous seqno
    if ( prev_seqno != 0 && prev_seqno != bk->restore_curr_seqno ) {
        *err_seqno             = bk->restore_curr_seqno;
        shard->restore_running = 0;
        return FLASH_EINVAL;       // prev_seqno doesn't match
    }

    // set restore state
    bk->restore_curr_seqno = curr_seqno;
    bk->client_version     = client_version;

    plat_assert( shard->restore_running );
    plat_assert( !shard->backup_running );

    return 0;
}


int
restore_end( mcd_osd_shard_t * shard, int cancel )
{
    mcd_bak_state_t           * bk = shard->backup;

    // cancel/complete restore while backup running
    if ( shard->backup_running ) {
        return FLASH_EBUSY;
    }

    // restore not running
    if ( !shard->restore_running ) {
        return FLASH_EPERM;
    }

    if ( cancel ) {
        bk->restore_curr_seqno = bk->restore_prev_seqno; // revert seqno
    } else {
        bk->restore_prev_seqno = bk->restore_curr_seqno; // advance seqno

        // set shard sequence number to the restored backup
        shard->sequence = bk->restore_curr_seqno;

        // require a full backup after restore
        bk->full_backup_required = 1;
    }

    // set restore not in progress
    shard->restore_running = 0;

    return 0;
}


inline int
restore_requires_conversion( mcd_osd_shard_t * shard )
{
    // restore protocol 3 is specifically used when restoring data that
    // was backed up without having the struct object_data_t converted to
    // network format.
    if ( shard->restore_running &&
         shard->backup->client_version ==
         MCD_BAK_RESTORE_PROTOCOL_VERSION_V3 ) {
        return 0;
    }
    return 1;
}
