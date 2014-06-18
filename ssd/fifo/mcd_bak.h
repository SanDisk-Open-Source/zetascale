/*
 * File:   mcd_bak.h
 * Author: Wayne Hineman
 *
 * Created on July 1, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_bak.h 14160 2010-06-14 22:16:12Z hiney $
 */
#ifdef BACKUP_SUPPORT

#ifndef __MCD_BAK_H__
#define __MCD_BAK_H__

#include "sys/queue.h"
#include "mcd_osd.h"

#undef  mcd_bak_msg
#define mcd_bak_msg(id, args...)                                        \
    plat_log_msg( id,                                                   \
                  PLAT_LOG_CAT_SDF_APP_MEMCACHED_BACKUP,                \
                  ##args )

#define MCD_BAK_NUM_SEQ_WINDOWS 2

#define MCD_BAK_SNAPSHOT_LOGBUF_INITIAL 0xffffffffffffffffull

#define MCD_BAK_BACKUP_PROTOCOL_VERSION_V1  1
#define MCD_BAK_BACKUP_PROTOCOL_VERSION_V2  2
#define MCD_BAK_BACKUP_PROTOCOL_VERSION_V3  3
#define MCD_BAK_BACKUP_PROTOCOL_VERSION_V4  4
#define MCD_BAK_BACKUP_PROTOCOL_VERSION     MCD_BAK_BACKUP_PROTOCOL_VERSION_V4

#define MCD_BAK_RESTORE_PROTOCOL_VERSION_V1 1
#define MCD_BAK_RESTORE_PROTOCOL_VERSION_V2 2
#define MCD_BAK_RESTORE_PROTOCOL_VERSION_V3 3
#define MCD_BAK_RESTORE_PROTOCOL_VERSION_V4 4
#define MCD_BAK_RESTORE_PROTOCOL_VERSION    MCD_BAK_RESTORE_PROTOCOL_VERSION_V4

#define MCD_BAK_DEAD_OBJECT_MAGIC           0xdeadbeef

// -----------------------------------------------------
//    Protocol Compatibility
// -----------------------------------------------------

#define MCD_BAK_BACKUP_PROTOCOL_COMPAT_V4(v)            \
    ( (v) == MCD_BAK_BACKUP_PROTOCOL_VERSION_V4 ||      \
      (v) == MCD_BAK_BACKUP_PROTOCOL_VERSION_V3 )
#define MCD_BAK_BACKUP_PROTOCOL_COMPAT_V2(v)    \
    ( (v) <= MCD_BAK_BACKUP_PROTOCOL_VERSION_V2 )      // v is unsigned
#define MCD_BAK_BACKUP_PROTOCOL_COMPAT_V1(v)    \
    ( (v) <= MCD_BAK_BACKUP_PROTOCOL_VERSION_V2 )      // v is unsigned

#define MCD_BAK_RESTORE_PROTOCOL_COMPAT_V4(v)           \
    ( (v) == MCD_BAK_RESTORE_PROTOCOL_VERSION_V4 ||     \
      (v) == MCD_BAK_RESTORE_PROTOCOL_VERSION_V3 )
#define MCD_BAK_RESTORE_PROTOCOL_COMPAT_V2(v)   \
    ( (v) <= MCD_BAK_RESTORE_PROTOCOL_VERSION_V2 )     // v is unsigned
#define MCD_BAK_RESTORE_PROTOCOL_COMPAT_V1(v)   \
    ( (v) <= MCD_BAK_RESTORE_PROTOCOL_VERSION_V2 )     // v is unsigned

// -----------------------------------------------------
//    In-memory structures
// -----------------------------------------------------

// List entry
// Elements of a list used to keep track of bitmap updates while
// a bitmap snapshot is taking place (part of backup command processing).
typedef struct mcd_bak_ps_entry {
    TAILQ_ENTRY(mcd_bak_ps_entry) list_entry; // entry in the list
    mcd_logrec_object_t           rec;        // list entry payload
} mcd_bak_ps_entry_t;


// Backup stats
typedef struct mcd_bak_stats {
    uint64_t        seg_read;              // count of segments read
    uint64_t        seg_empty;             // count of empty segments skipped
    uint64_t        seg_error;             // count of error segments skipped
    uint64_t        obj_count;             // count of valid objects
    uint64_t        obj_blocks;            // block count of valid objects
    uint64_t        deleted_count;         // count of deleted objects
    uint64_t        deleted_blocks;        // block count of deleted objects
    uint64_t        expired_count;         // count of expired objects
    uint64_t        expired_blocks;        // block count of expired objects
    uint64_t        flushed_count;         // count of flushed objects
    uint64_t        flushed_blocks;        // block count of flushed objects
    uint64_t        error_obj_count;       // count of error objects
    uint64_t        error_obj_blocks;      // block count of error objects
} mcd_bak_stats_t;


// Backup/restore state
// Kept with every persistent shard. This state is volatile, so if a
// crash happens, the state is lost. The only thing of any consequence
// is the current backup sequence number. Losing that requires the next
// backup to be a full backup.
typedef struct mcd_bak_state {
    uint64_t        restore_curr_seqno;    // ordering for
    uint64_t        restore_prev_seqno;    //   restores

    uint64_t        backup_curr_seqno;     // ordering for
    uint64_t        backup_prev_seqno;     //   backups

    uint64_t        snapshot_logbuf;       // logbuf seqno for bitmap snapshot
    int             snapshot_complete;     // bitmap snapshot completion state
    int             full_backup_required;  // volatile backup state
    uint32_t        client_version;        // protocol version used by client

    int             seqno_window;          // index into following array
    int             seqno_pending[MCD_BAK_NUM_SEQ_WINDOWS]; // count of pending
                                                            //   operations in
                                                            //   a window
    int             ps_count;               // count of items in list
    TAILQ_HEAD(, mcd_bak_ps_entry) ps_list; // list of pending operations kept
                                            //   while snapshot is taking place

    // stats
    mcd_bak_stats_t  stats;                 // backup/restore statistics
} mcd_bak_state_t;


// -----------------------------------------------------
//    Forward declarations
// -----------------------------------------------------

struct mcd_osd_shard;

int  backup_report_version( char ** bufp, int * lenp );
int  backup_init( struct mcd_osd_shard * shard );
inline int  backup_incr_pending_seqno( mcd_osd_shard_t * shard );
inline void backup_decr_pending_seqno( mcd_osd_shard_t * shard, int window );
void backup_maintain_bitmaps( struct mcd_osd_shard * shard,
                              uint32_t blk_offset, int delete );
void backup_snapshot_bitmaps( struct mcd_osd_shard * shard );
int  backup_start_prepare( struct mcd_osd_shard * shard, int full_backup );
int  backup_start( struct mcd_osd_shard * shard, int full_backup,
                   uint64_t * prev_seqno, uint64_t * curr_seqno,
                   time_t * backup_time );
int  backup_end( struct mcd_osd_shard * shard, int cancel );

int  restore_start( mcd_osd_shard_t * shard, uint64_t prev_seqno,
                    uint64_t curr_seqno, uint32_t client_version,
                    uint64_t * err_seqno );
int  restore_end( mcd_osd_shard_t * shard, int cancel );
inline int restore_requires_conversion( mcd_osd_shard_t * shard );


#endif
#endif // BACKUP_SUPPORT
