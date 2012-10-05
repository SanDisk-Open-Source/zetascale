/*
 * File:   mcd_rec.c
 * Author: Wayne Hineman
 *
 * Created on Mar 03, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_rec.c 16149 2011-02-15 16:07:23Z briano $
 */

#include <stdio.h>
#include <signal.h>
#include <aio.h>
#include <linux/fs.h>
#include <ctype.h>

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
#include "ssd/ssd_aio.h"

//#include "memcached.h"
//#include "command.h"
//#include "mcd_sdf.h"
#include "mcd_aio.h"
#include "mcd_osd.h"
#include "container_meta_blob.h"
#include "mcd_rec.h"
#include "mcd_rep.h"
#include "mcd_bak.h"


/*
 * For flushing logs.
 */
#define FLUSH_LOG_MAGIC     0x0feedbead
#define FLUSH_LOG_PREFIX    "mbfl_"
#define FLUSH_LOG_MAX_PATH  256
#define FLUSH_LOG_BUFFERED  (1024*1024)
#define FLUSH_LOG_SEC_SIZE  512
#define FLUSH_LOG_SEC_ALIGN 512
#define FLUSH_LOG_FILE_MODE 0755


/*
 * For boundary alignment.  n must be a power of 2.
 */
#define align(p, n) ((void *) (((uint64_t)(p)+(n)-1) & ~((n)-1)))


/*
 * Flushed to disk with every write.
 */
typedef struct {
    uint32_t            magic;
    uint32_t            shard_off;
    uint64_t            flush_blk;
    uint64_t            shard_blk;
    uint64_t            lsn;
    mcd_logrec_object_t logrec_obj;
} flog_rec_t;


/*
 * Used to handle buffered I/O when patching up during recovery.
 */
typedef struct {
    int              dirty;
    uint64_t         blkno;
    uint64_t         lsn;
    struct osd_state *context;
    unsigned char   *buf;
    unsigned char   *abuf;
} flog_bio_t;


/*
 * Static variables.
 */
static int Sync_data;


/*
 * turns out plat_alloc allocates outside of guma for requests above
 * certain size (currently 64MB)
 */
#define plat_alloc_large        plat_alloc_steal_from_heap

#undef  mcd_log_msg
#define mcd_log_msg(id, args...)                                        \
    plat_log_msg( id,                                                   \
                  PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY,              \
                  ##args)

#define mcd_rlg_msg(id, args...)                                        \
    plat_log_msg( id,                                                   \
                  PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY_LOG,          \
                  ##args)

#undef  mcd_dbg_msg
#define mcd_dbg_msg(...)                                                \
    plat_log_msg( PLAT_LOG_ID_INITIAL,                                  \
                  PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY,              \
                  __VA_ARGS__ )

#ifdef  MCD_REC_DEBUGGING
#  define MCD_REC_LOG_LVL_DIAG  PLAT_LOG_LEVEL_INFO
#  define MCD_REC_LOG_LVL_DEBUG PLAT_LOG_LEVEL_INFO
#  define MCD_REC_LOG_LVL_TRACE PLAT_LOG_LEVEL_DIAGNOSTIC
#  define MCD_REC_LOG_LVL_DEVEL PLAT_LOG_LEVEL_DEBUG
#else
#  define MCD_REC_LOG_LVL_DIAG  PLAT_LOG_LEVEL_DIAGNOSTIC
#  define MCD_REC_LOG_LVL_DEBUG PLAT_LOG_LEVEL_DEBUG
#  define MCD_REC_LOG_LVL_TRACE PLAT_LOG_LEVEL_TRACE
#  define MCD_REC_LOG_LVL_DEVEL PLAT_LOG_LEVEL_DEVEL
#endif

// max number of shards used for formatting purposes
#define MCD_REC_MAX_SHARDS      (MEGABYTE / MCD_OSD_BLK_SIZE)

// default copies of superblock to write on RAID device
#define MCD_REC_SB_RAID_DEFAULT 3

// number of blocks to reserve for volume label
#define MCD_REC_LABEL_BLKS      8

// -----------------------------------------------------
//    Globals
// -----------------------------------------------------

static int                      Log_flush_in_place           = 0;

static mcd_rec_superblock_t     Mcd_rec_superblock;
static int                      Mcd_rec_superblock_formatted = 0;
static int                      Mcd_rec_sb_data_copies       = 0;
static void                  (* Mcd_set_rtg_callback)(uint64_t shardID,
                                                      uint64_t seqno);

static uint64_t                 Mcd_rec_update_bufsize       = 0;
static uint64_t                 Mcd_rec_update_segment_size  = 0;
static uint64_t                 Mcd_rec_update_segment_blks  = 0;
static uint64_t                 Mcd_rec_update_max_chunks    = 0;
static uint64_t                 Mcd_rec_update_yield         = 0;
static uint64_t                 Mcd_rec_update_verify        = 0;
static uint64_t                 Mcd_rec_update_verify_log    = 0;

static uint64_t                 Mcd_rec_free_upd_seg_curr    = 0;
static void                  ** Mcd_rec_free_upd_segments    = NULL;
static void                   * Mcd_rec_upd_segments_anchor  = NULL;
static fthSem_t                 Mcd_rec_upd_seg_sem;
static fthLock_t                Mcd_rec_upd_segment_lock;

static uint64_t                 Mcd_rec_log_segment_size     = 0;
static uint64_t                 Mcd_rec_log_segment_blks     = 0;

static uint64_t                 Mcd_rec_free_log_seg_curr    = 0;
static void                  ** Mcd_rec_free_log_segments    = NULL;
static void                   * Mcd_rec_log_segments_anchor  = NULL;
static fthLock_t                Mcd_rec_log_segment_lock;

static mcd_rec_aio_ctxt_t     * Mcd_rec_free_aio_ctxt_list   = NULL;
static fthLock_t                Mcd_rec_free_aio_ctxt_lock;
static fthMbox_t                Mcd_rec_updater_mbox;

static int                      Mcd_rec_updater_threads      = 0;
static int                      Mcd_rec_log_writer_threads   = 0;

static int                      Mcd_rec_chicken              = 1;
static fthMbox_t                Mcd_rec_updater_mbox;

// FIXME: hack alert
static int                      Mcd_rec_first_shard_recovered = 0;
static char                     Mcd_rec_cmc_blobs[ MCD_METABLOB_MAX_SLOTS ]
                                                 [ MCD_OSD_BLK_SIZE ];


enum flush_mode {
    FLUSH_MODE_FILL,
    FLUSH_MODE_SYNC,
};

enum verify_mode {
    VERIFY_ABORT_IF_CLEAN = 1,
    VERIFY_ABORT_IF_DIRTY = 2,
};


// -----------------------------------------------------
//    For internal tests
// -----------------------------------------------------

static int                Mcd_rec_attach_test_running         = 0;
static int                Mcd_rec_attach_test_waiters_special = 0;
static int                Mcd_rec_attach_test_waiters         = 0;
static fthMbox_t          Mcd_rec_attach_test_mbox_special;
static fthMbox_t          Mcd_rec_attach_test_mbox;
static fthMbox_t          Mcd_rec_attach_test_updated_mbox;
static mcd_rec_update_t   Mcd_rec_attach_test_update_mail[ MCD_MAX_NUM_CNTRS ];


// -----------------------------------------------------
//    Exported Globals
// -----------------------------------------------------


// -----------------------------------------------------
//    External declarations
// -----------------------------------------------------

extern void mcd_fth_osd_slab_dealloc( mcd_osd_shard_t * shard,
                                      uint32_t address );
extern inline uint32_t mcd_osd_lba_to_blk( uint32_t blocks );


// -----------------------------------------------------
//    External Globals
// -----------------------------------------------------

extern mcd_container_t          Mcd_osd_cmc_cntr;


// FIXME: hack alert
extern int (* init_container_meta_blob_put)( uint64_t shard_id, char * data,
                                             int len );
extern int (* init_container_meta_blob_get)( char * blobs[], int num_slots );


// -----------------------------------------------------
//    Forward declarations
// -----------------------------------------------------

int  read_label( int order[] );
int  validate_superblock_data( char * dest, int good_count, int good_copy[],
                               uint64_t blk_offset, int blk_count,
                               char * source[] );
int  write_superblock( char * buf, int blks, uint64_t blk_offset );
void shard_set_properties_internal( mcd_container_t * cntr,
                                    mcd_rec_properties_t * prop );
void snap_dump( char * str, int32_t len );
void updater_thread( uint64_t arg );
void log_write_internal( mcd_osd_shard_t * shard, mcd_logrec_object_t * data );
void log_writer_thread( uint64_t arg );
int  log_init( mcd_osd_shard_t * shard );
int  log_init_phase2( void * context, mcd_osd_shard_t * shard );

#ifndef MCD_REC_NOT_CHICKEN
// for the chickens among us...
void updater_thread_chicken( uint64_t arg );
#endif


/************************************************************************
 *                                                                      *
 *                      "Metablob" storage                              *
 *                                                                      *
 ************************************************************************/

int
container_meta_blob_put( uint64_t shard_id, char * data, int len )
{
    int                         i, rc;
    uint64_t                    blob_offset;
    mcd_rec_shard_t           * pshard;
    osd_state_t               * context =
        mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_BLOB );
    char                      * buf;
    mcd_rec_blob_t            * blob;

    mcd_log_msg( 20065, PLAT_LOG_LEVEL_DEBUG,
                 "ENTERING, shardID=%lu", shard_id );

    // make buffer aligned
    buf = (char *)( ( (uint64_t)context->osd_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );
    memset( buf, 0, Mcd_osd_blk_size );

    // sanity
    if ( len > MCD_METABLOB_MAX_LEN ) {
        mcd_log_msg( 20431, PLAT_LOG_LEVEL_ERROR,
                     "invalid length %d (max=%d)",
                     len, MCD_METABLOB_MAX_LEN );
        return SDF_FAILURE;
    }

    // install the data, length, and checksum
    blob              = (mcd_rec_blob_t *)buf;
    blob->eye_catcher = MCD_REC_BLOB_EYE_CATCHER;
    blob->version     = MCD_REC_BLOB_VERSION;
    blob->length      = len;
    blob->checksum    = 0;

    memcpy( blob->data, data, len );
    blob->checksum    = hash( (unsigned char *)blob, Mcd_osd_blk_size, 0 );

    // "global" blob
    if ( shard_id == 0 ) {
        blob_offset = Mcd_rec_superblock.flash_desc->blk_offset +
            Mcd_rec_superblock.flash_desc->blob_offset;

        rc = write_superblock( buf, 1, blob_offset );

    // shard blob
    } else {
        // find the shard
        for ( i = 0; i < MCD_OSD_MAX_NUM_SHARDS; i++ ) {
            if ( Mcd_osd_slab_shards[ i ] != NULL &&
                 Mcd_osd_slab_shards[ i ]->shard.shardID == shard_id ) {
                break;
            }
        }
        if ( i == MCD_OSD_MAX_NUM_SHARDS ) {
            mcd_log_msg( 20432, PLAT_LOG_LEVEL_ERROR,
                         "couldn't find shardID=%lu", shard_id );
            return SDF_FAILURE;
        }
        pshard = Mcd_osd_slab_shards[ i ]->pshard;

        if ( pshard == NULL ) {
            mcd_log_msg( 20433, PLAT_LOG_LEVEL_ERROR,
                         "non-persistent shard, shardID=%lu", shard_id );
            return SDF_FAILURE;
        }

        blob_offset = pshard->blk_offset + pshard->blob_offset;

        // write the blob
        rc = mcd_fth_aio_blk_write( context,
                                    buf,
                                    blob_offset * Mcd_osd_blk_size,
                                    Mcd_osd_blk_size );
    }

    if ( FLASH_EOK != rc ) {
        mcd_log_msg( 20434, PLAT_LOG_LEVEL_ERROR,
                     "failed to write blob, shardID=%lu, offset=%lu, rc=%d",
                     shard_id, blob_offset, rc );
        return SDF_FAILURE;
    }

    return SDF_SUCCESS;
}

int
read_blob( void * context, uint64_t blk_offset, char * buf )
{
    int                         rc;
    uint64_t                    checksum;
    mcd_rec_blob_t            * blob;

    // read blob block
    rc = mcd_fth_aio_blk_read( context,
                               buf,
                               blk_offset * Mcd_osd_blk_size,
                               Mcd_osd_blk_size );
    if ( FLASH_EOK != rc ) {
        return rc;
    }

    // verify checksum
    blob           = (mcd_rec_blob_t *)buf;
    checksum       = blob->checksum;
    blob->checksum = 0;
    blob->checksum = hash( (unsigned char *)buf, Mcd_osd_blk_size, 0 );

    if ( checksum != blob->checksum ||
         blob->eye_catcher != MCD_REC_BLOB_EYE_CATCHER ||
         blob->version != MCD_REC_BLOB_VERSION ) {
        blob->checksum = checksum;   // restore original contents
        return -1;
    }

    return 0;
}

int
container_meta_blob_get( char * blobs[], int num_slots )
{
    int                         i, ssd, rc, count = 0;
    int                         size;
    int                         blob_count;
    int                       * good_copy;
    uint64_t                    stripe_offset;
    char                      * buf;
    osd_state_t               * context =
        mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_BLOB );
    mcd_rec_shard_t           * pshard;
    mcd_rec_superblock_t      * sb = &Mcd_rec_superblock;
    mcd_rec_flash_t           * fd = sb->flash_desc;
    char                      * source[ MCD_AIO_MAX_NFILES ];

    mcd_log_msg( 20435, PLAT_LOG_LEVEL_DEBUG,
                 "ENTERING, slots=%d", num_slots );

    // calculate buffer size to hold aligned 1-block buffers
    // for each blob, plus 1 block for alignment (blobs are 1 block in length)
    size = ( Mcd_rec_sb_data_copies * Mcd_osd_blk_size ) + Mcd_osd_blk_size;

    // get aligned buffer
    buf = (char *)( ( (uint64_t)context->osd_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

    // get array for keeping track of "good" copies
    good_copy = (int *)( (uint64_t)context->osd_buf + size );

    plat_assert( size + (sizeof( int ) * Mcd_rec_sb_data_copies) < MEGABYTE );

    blob_count = 0;
    memset( context->osd_buf, 0, size );

    // read global blob on each device
    for ( ssd = 0; ssd < Mcd_rec_sb_data_copies; ssd++ ) {

        source[ ssd ]    = buf + (ssd * Mcd_osd_blk_size);
        good_copy[ ssd ] = 0;

        // using stripe size as offset will write on each drive
        stripe_offset = ssd * Mcd_aio_strip_size;

        // read the "global" blob
        rc = read_blob( context,
                        (stripe_offset / Mcd_osd_blk_size) +
                        fd->blk_offset + fd->blob_offset,
                        source[ ssd ] );
        if ( rc < 0 ) {
            mcd_log_msg( 20436, PLAT_LOG_LEVEL_ERROR,
                         "global blob verification failure, "
                         "offset=%lu, copy=%d",
                         fd->blk_offset + fd->blob_offset, ssd );
            snap_dump( source[ ssd ], Mcd_osd_blk_size );
            continue;
        } else if ( rc > 0 ) {
            mcd_log_msg( 20437, PLAT_LOG_LEVEL_ERROR,
                         "global blob read failure, "
                         "offset=%lu, copy=%d, rc=%d",
                         fd->blk_offset + fd->blob_offset, ssd, rc );
            continue;
        }

        good_copy[ ssd ] = 1;
        blob_count++;
    }

    // validate N copies of the global blob
    rc = validate_superblock_data( Mcd_rec_cmc_blobs[ count ],
                                   blob_count,
                                   good_copy,
                                   fd->blk_offset + fd->blob_offset,
                                   1,
                                   source );
    plat_assert_rc( rc );

    // return global blob data
    blobs[ count ] = ((mcd_rec_blob_t *)Mcd_rec_cmc_blobs[ count ])->data;
    count++;

    // find all shards' blobs
    for ( i = 0; i < MCD_OSD_MAX_NUM_SHARDS; i++ ) {
        if ( Mcd_osd_slab_shards[ i ] != NULL ) {
            pshard = Mcd_osd_slab_shards[ i ]->pshard;
            if ( pshard == NULL ) {  // non-persistent shard
                continue;
            }

            // read this shard's blob
            rc = read_blob( context,
                            pshard->blk_offset + pshard->blob_offset,
                            buf );
            if ( rc != 0 ) {
                if ( rc < 0 ) {
                    mcd_log_msg( 20438, PLAT_LOG_LEVEL_FATAL,
                                 "blob verification failure, shardID=%lu, "
                                 "shard_offset=%lu, offset=%lu",
                                 pshard->shard_id, pshard->blk_offset,
                                 pshard->blob_offset );
                    snap_dump( buf, Mcd_osd_blk_size );
                } else {
                    mcd_log_msg( 20439, PLAT_LOG_LEVEL_FATAL,
                                 "blob read failure, shardID=%lu, "
                                 "shard_offset=%lu, offset=%lu, rc=%d",
                                 pshard->shard_id, pshard->blk_offset,
                                 pshard->blob_offset, rc );
                }
                plat_abort();
            }

            // copy blob block to unaligned cache
            memcpy( Mcd_rec_cmc_blobs[ count ], buf, Mcd_osd_blk_size );
            blobs[ count ] =
                ((mcd_rec_blob_t *)Mcd_rec_cmc_blobs[ count ])->data;
            count++;
         }
    }

    return count;
}


/************************************************************************
 *                                                                      *
 *              Memcached persistence utility functions                 *
 *                                                                      *
 ************************************************************************/

void
do_aio_init( int recovering )
{
    // initialize
    Mcd_rec_sb_data_copies = Mcd_aio_num_files;

    // running on raid device, write fewer redundant copies
    if ( Mcd_aio_raid_device ) {
        if ( flash_settings.sb_data_copies > Mcd_aio_num_files ) {
            mcd_log_msg( 20440, PLAT_LOG_LEVEL_FATAL,
                         "superblock data copies (%d) can't be more than "
                         "the number of files (%d)",
                         flash_settings.sb_data_copies, Mcd_aio_num_files );
            plat_abort();
        }

        Mcd_rec_sb_data_copies = MCD_REC_SB_RAID_DEFAULT;
        if ( flash_settings.sb_data_copies > 0 ) {
            Mcd_rec_sb_data_copies = flash_settings.sb_data_copies;
        }
    }

    // no raid, validate disk signatures during recovery
    else if ( recovering ) {
        int  order[ MCD_AIO_MAX_NFILES ];
        if ( read_label( order ) != 0 ) {
            mcd_aio_set_fds( order );
        }
    }

    return;
}

void
remove_segment( uint64_t seg_addr )
{
    for ( int i = 0; i < MCD_OSD_MAX_SEGMENTS - 1; i++ ) {
        if ( Mcd_osd_free_segments[ i ] == 0 ) {
            return;
        } else if ( Mcd_osd_free_segments[ i ] == seg_addr ) {
            for ( int j = i; j < MCD_OSD_MAX_SEGMENTS - 2; j++ ) {
                if ( Mcd_osd_free_segments[ j ] == 0 ) {
                    return;
                }
                Mcd_osd_free_segments[ j ] = Mcd_osd_free_segments[ j+1 ];
            }
        }
    }
    return;
}

inline int
find_properties( uint64_t shard_id )
{
    int                         slot;
    mcd_rec_superblock_t      * sb = &Mcd_rec_superblock;
    mcd_rec_flash_t           * fd = sb->flash_desc;

    plat_assert( shard_id != 0 );
    for ( slot = 0; slot < fd->max_shards; slot++ ) {
        if ( sb->props[ slot ]->shard_id == shard_id ) {
            return slot;
        }
    }
    return -1;
}

int
read_label( int order[] )
{
    int                         ssd, rc;
    int                         result = 0;
    int                         label_num;
    uint64_t                    stripe_offset;
    char                      * label;
    osd_state_t               * context =
        mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_LABEL );

    // get pointer to label position in buffer
    label = (char *)( ( (uint64_t)context->osd_buf + Mcd_osd_blk_size - 1 ) &
                      Mcd_osd_blk_mask );

    // write label on each device
    for ( ssd = 0; ssd < Mcd_rec_sb_data_copies; ssd++ ) {

        // using stripe size as offset will write on each drive
        stripe_offset = ssd * Mcd_aio_strip_size;

        // read label
        rc = mcd_fth_aio_blk_read( context,
                                   label,
                                   stripe_offset,
                                   Mcd_osd_blk_size );
        if ( FLASH_EOK != rc ) {
            mcd_log_msg( 40011, PLAT_LOG_LEVEL_FATAL,
                         "failed to read label, blk_offset=%lu, ssd=%d, rc=%d",
                         stripe_offset / Mcd_osd_blk_size, ssd, rc );
            plat_abort();
        }

        // validate label
        label_num = atoi( label + strlen( "Schooner" ) );
        if ( 0 != memcmp( label, "Schooner", strlen( "Schooner" ) ) ||
             Mcd_aio_num_files < label_num ||
             0 > label_num ) {
            mcd_log_msg( 40012, PLAT_LOG_LEVEL_FATAL,
                         "Invalid signature '%s' read from fd %d",
                         label, ssd );
            plat_abort();
        }

        // check for swizzled files
        if ( ssd != label_num ) {
            mcd_log_msg( 40013, PLAT_LOG_LEVEL_WARN,
                         "Unexpected signature '%s' read from fd %d",
                         label, ssd );
            result = 1;
        }

        // install file descriptor in proper slot
        order[ label_num ] = ssd;
    }

    return result;
}

int
write_label( char * buf, int blks, uint64_t blk_offset )
{
    int                         ssd, rc;
    uint64_t                    stripe_offset;
    void                      * context =
        mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_LABEL );

    // write label on each device
    for ( ssd = 0; ssd < Mcd_rec_sb_data_copies; ssd++ ) {

        // format a label in the buffer
        // Note: this label will be at the beginning of the first
        // sector of an 8-sector reserved area
        snprintf( buf, 12, "Schooner%d", ssd );

        // using stripe size as offset will write on each drive
        stripe_offset = ssd * Mcd_aio_strip_size;

        // write label
        rc = mcd_fth_aio_blk_write( context,
                                    buf,
                                    stripe_offset +
                                    (blk_offset * Mcd_osd_blk_size),
                                    blks * Mcd_osd_blk_size );
        if ( FLASH_EOK != rc ) {
            mcd_log_msg( 20441, PLAT_LOG_LEVEL_ERROR,
                         "failed to write label, blks=%d, "
                         "blk_offset=%lu, ssd=%d, rc=%d",
                         blks, blk_offset, ssd, rc );
            return rc;
        }
    }

    return 0;
}

int
write_superblock( char * buf, int blks, uint64_t blk_offset )
{
    int                         ssd, rc;
    uint64_t                    stripe_offset;
    void                      * context =
        mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_SPBLK );

    // write superblock on each device
    for ( ssd = 0; ssd < Mcd_rec_sb_data_copies; ssd++ ) {

        // using stripe size as offset will write on each drive
        stripe_offset = ssd * Mcd_aio_strip_size;

        // write superblock
        rc = mcd_fth_aio_blk_write( context,
                                    buf,
                                    stripe_offset +
                                    (blk_offset * Mcd_osd_blk_size),
                                    blks * Mcd_osd_blk_size );
        if ( FLASH_EOK != rc ) {
            mcd_log_msg( 20442, PLAT_LOG_LEVEL_ERROR,
                         "failed to write superblock data, blks=%d, "
                         "blk_offset=%lu, ssd=%d, rc=%d",
                         blks, blk_offset, ssd, rc );
            return rc;
        }
    }

    return 0;
}

int
write_property( int slot )
{
    int                         rc;
    mcd_rec_superblock_t      * sb = &Mcd_rec_superblock;
    mcd_rec_flash_t           * fd = sb->flash_desc;

    plat_assert( slot >= 0 );
    plat_assert( slot < MCD_OSD_MAX_NUM_SHARDS );

    // write properties on each device
    rc = write_superblock( (char *)sb->props[ slot ],
                           1,
                           fd->blk_offset + fd->prop_offset + slot );
    if ( FLASH_EOK != rc ) {
        mcd_log_msg( 20443, PLAT_LOG_LEVEL_ERROR,
                     "failed to write shard props shardID=%lu, slot=%d, rc=%d",
                     sb->props[ slot ]->shard_id, slot, rc );
        return rc;
    }

    return 0;
}

inline uint64_t
relative_log_offset( mcd_rec_shard_t * pshard, int log )
{
    return ( pshard->rec_md_blks +
             pshard->rec_table_blks +
             pshard->rec_table_pad +
             (log * (pshard->rec_log_blks + pshard->rec_log_pad)) );
}

uint64_t
read_log_page( osd_state_t * context, mcd_osd_shard_t * shard, int log,
               uint64_t rel_offset )
{
    int                         rc;
    uint64_t                    log_offset;
    uint64_t                    blk_offset;
    char                      * buf;

    // get aligned buffer
    buf = (char *)( ( (uint64_t)context->osd_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

    // calculate block offset
    log_offset = relative_log_offset( shard->pshard, log );
    blk_offset =
        shard->segments[ (log_offset + rel_offset) / Mcd_osd_segment_blks ] +
        ((log_offset + rel_offset) % Mcd_osd_segment_blks);

    // read last page of first log
    rc = mcd_fth_aio_blk_read( context,
                               buf,
                               blk_offset * Mcd_osd_blk_size,
                               Mcd_osd_blk_size );
    if ( FLASH_EOK != rc ) {
        mcd_rlg_msg( 40031, PLAT_LOG_LEVEL_FATAL,
                     "failed to read log %d, shardID=%lu, "
                     "rel_offset=%lu, blk_offset=%lu, rc=%d",
                     log, shard->id, rel_offset, blk_offset, rc );
        plat_abort();
    }

    return ((mcd_rec_logpage_hdr_t *)buf)->LSN;
}

inline void
delete_object( mcd_rec_flash_object_t * object )
{
    object->syndrome  = 0;
    object->tombstone = 0;
    object->deleted   = 0;
    object->reserved  = 0;
    object->blocks    = 0;
    object->bucket    = 0;
    object->seqno     = 0;
}

uint64_t
table_obj_count( char * buffer, uint64_t num_objs, uint64_t * ts )
{
    int                         i;
    int                         count = 0;
    int                         ts_count = 0;
    mcd_rec_flash_object_t    * obj;

    for ( i = 0; i < num_objs; i++ ) {

        obj = (mcd_rec_flash_object_t *)
            (buffer + (i * sizeof( mcd_rec_flash_object_t )) );
        if ( obj->tombstone ){
            ts_count++;
        } else if ( obj->blocks > 0 ) {
            count++;
        }
    }
    if ( ts != NULL ) {
        *ts = ts_count;
    }
    return count;
}

void
snap_dump( char * str, int32_t len )
{
    int                           buf_len;
    uint32_t                      i, j, ch;
    char                        * pos;
    char                          pbuf[ 128 ];

    mcd_log_msg( 40060, PLAT_LOG_LEVEL_DIAGNOSTIC,
                 "snap_dump from address %p:", str );

    for ( i = 0; i < ((len + 15)/16); i++ ) {
        pos = pbuf;
        buf_len = sizeof( pbuf );
        plat_snprintfcat( &pos, &buf_len, "0x%6.6X: ", (i*16) );

        for ( j = 0; j < 16; j++ ) {
            if ( j % 4 == 0 ) {
                plat_snprintfcat( &pos, &buf_len, " " );
            }
            if ( ((i * 16) + j) < len ) {
                plat_snprintfcat( &pos, &buf_len, "%2.2X",
                                  (((uint8_t) str[ (i*16) + j ] )) );
            } else {
                plat_snprintfcat( &pos, &buf_len, "  " );
            }
        }

        plat_snprintfcat( &pos, &buf_len, "  \"" );
        for ( j = 0; j < 16; j++ ) {
            if ( ((i*16) + j) < len ) {
                ch = (uint8_t) str[ (i*16) + j ];
                plat_snprintfcat( &pos, &buf_len, "%c",
                                  isprint( ch ) ? ch : '.' );
            } else {
                break;
            }
        }

        plat_snprintfcat( &pos, &buf_len, "\"%c", '\0' );
        mcd_log_msg( 20819, PLAT_LOG_LEVEL_DIAGNOSTIC, "%s", pbuf );
    }

    return;
}

inline int64_t
power_of_two_roundup( int64_t i )
{
    if ( i < 0 ) {
        return 0;
    }
    --i;
    i |= i >> 1;
    i |= i >> 2;
    i |= i >> 4;
    i |= i >> 8;
    i |= i >> 16;
    i |= i >> 32;
    return i + 1;
}

void *
context_alloc( int category )
{
    if ( Mcd_rec_free_aio_ctxt_list != NULL ) {
        fthWaitEl_t               * wait;
        mcd_rec_aio_ctxt_t        * list_element;
        mcd_rec_aio_ctxt_t        * ctxt;

        wait = fthLock( &Mcd_rec_free_aio_ctxt_lock, 1, NULL );

        if ( Mcd_rec_free_aio_ctxt_list != NULL ) {
            list_element               = Mcd_rec_free_aio_ctxt_list;
            Mcd_rec_free_aio_ctxt_list = list_element->next;
            fthUnlock( wait );

            ctxt = list_element->ctxt;
            plat_free( list_element );
            return ctxt;
        }
        fthUnlock( wait );
    }

    return mcd_fth_init_aio_ctxt( category );
}

void
context_free( void * context )
{
    fthWaitEl_t               * wait;
    mcd_rec_aio_ctxt_t        * list_element;

    list_element = plat_malloc( sizeof( mcd_rec_aio_ctxt_t ) );
    plat_assert_always( list_element );

    wait = fthLock( &Mcd_rec_free_aio_ctxt_lock, 1, NULL );
    list_element->ctxt         = context;
    list_element->next         = Mcd_rec_free_aio_ctxt_list;
    Mcd_rec_free_aio_ctxt_list = list_element;
    fthUnlock( wait );

    return;
}

/************************************************************************
 *                                                                      *
 *                      Memcached SLAB persistence                      *
 *                                                                      *
 ************************************************************************/

int
recovery_report_version( char ** bufp, int * lenp )
{
    return plat_snprintfcat( bufp, lenp,
                             "flash_descriptor %d.0.0\r\n"
                             "shard_descriptor %d.0.0\r\n"
                             "class_descriptor %d.0.0\r\n"
                             "props_descriptor %d.0.0\r\n"
                             "ckpt_record %d.0.0\r\n",
                             MCD_REC_FLASH_VERSION,
                             MCD_REC_SHARD_VERSION,
                             MCD_REC_CLASS_VERSION,
                             MCD_REC_PROP_VERSION,
                             MCD_REC_CKPT_VERSION );
}

int
validate_superblock_data( char * dest, int good_count, int good_copy[],
                          uint64_t blk_offset, int blk_count, char * source[] )
{
    int                         i, j, ssd, rc = 0;
    int                         log_level;
    int                         max_tries = 3;
    int                         copy_count = good_count;
    int                         copy_good[ MCD_AIO_MAX_NFILES ];
    uint64_t                    stripe_offset;
    void                      * context =
        mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_SPBLK );

    // copy the list of which source buffers are "good"
    memcpy( copy_good, good_copy, sizeof( int ) * Mcd_rec_sb_data_copies );
#if 0
    for ( i = 0; i < Mcd_rec_sb_data_copies; i++ ) {
        if ( i < good_count ) {
            copy_good[i] = good_copy[i];
        } else {
            copy_good[i] = 0;
        }
    }
#endif

    // loop through source buffer list, comparing buffers
    for ( i = 0; i < Mcd_rec_sb_data_copies - 1; i++ ) {

        // find a good copy
        if ( !copy_good[ i ] ) {
            continue;
        } else if ( good_count == 1 ) {
            break;
        }

        // look for matches to this copy
        for ( j = i+1; j < Mcd_rec_sb_data_copies; j++ ) {
            // compare good copies
            if ( copy_good[ j ] ) {
                // remove next copy if comparison fails
                if ( memcmp( source[ i ],
                             source[ j ],
                             blk_count * Mcd_osd_blk_size ) != 0 ) {
                    copy_good[ j ] = 0;
                    copy_count--;
                }
            }
        }

        // found at least 2 good copies that were equal
        if ( copy_count > 1 ) {
            break;
        }

        // this copy didn't match any other, remove it
        good_copy[ i ] = 0;
        good_count--;

        // reset
        memcpy( copy_good, good_copy, sizeof( int ) * Mcd_rec_sb_data_copies );
        copy_count = good_count;
    }

    // put out a message
    log_level = PLAT_LOG_LEVEL_INFO;
    if ( copy_count == 0 ) {
        log_level = PLAT_LOG_LEVEL_FATAL;
    } else if ( copy_count < Mcd_rec_sb_data_copies ) {
        log_level = PLAT_LOG_LEVEL_WARN;
    }

    mcd_log_msg( 20444, log_level,
                 "found %d of %d %.*s (offset=%lu) copies ok",
                 copy_count, Mcd_rec_sb_data_copies, 4, (char *)source[ i ],
                 blk_offset );

    // no good copies
    if ( copy_count == 0 ) {
        plat_abort();
    }

    // keep the "good" copy, note that "dest" may not be aligned
    memcpy( dest, source[ i ], blk_count * Mcd_osd_blk_size );

    // try to "fix" (re-write) any bad copies
    if ( copy_count < Mcd_rec_sb_data_copies ) {
        mcd_log_msg( 20445, PLAT_LOG_LEVEL_INFO,
                     "attempting to fix %d of %d bad/mismatched copies",
                     Mcd_rec_sb_data_copies - copy_count,
                     Mcd_rec_sb_data_copies );

        for ( ssd = 0; ssd < Mcd_rec_sb_data_copies; ssd++ ) {

            // skip the good copies
            if ( copy_good[ ssd ] ) {
                continue;
            }

            // try max_tries times to re-write this copy
            for ( j = 1; j <= max_tries; j++ ) {
                stripe_offset = ssd * Mcd_aio_strip_size;
                rc = mcd_fth_aio_blk_write( context,
                                            source[ i ], // i has the good copy
                                            stripe_offset +
                                            (blk_offset * Mcd_osd_blk_size),
                                            blk_count * Mcd_osd_blk_size );
                if ( rc == FLASH_EOK ) {
                    break;
                }
                mcd_log_msg( 20446, PLAT_LOG_LEVEL_ERROR,
                             "failed to write superblock data, try=%d, "
                             "offset=%lu, count=%d, copy=%d, rc=%d",
                             j, blk_offset, blk_count, ssd, rc );
                if ( j == max_tries ) {
                    mcd_log_msg( 20447, PLAT_LOG_LEVEL_WARN,
                                 "giving up trying to fix superblock data, "
                                 "offset=%lu, count=%d, copy=%d",
                                 blk_offset, blk_count, ssd );
                }
            }
            if ( rc == FLASH_EOK ) {
                mcd_log_msg( 20448, PLAT_LOG_LEVEL_INFO,
                             "successfully re-wrote superblock data, "
                             "offset=%lu, count=%d, copy=%d",
                             blk_offset, blk_count, ssd );

                // sync all devices
                rc = mcd_aio_sync_devices();
                if ( rc != 0 ) {
                    mcd_log_msg( 20449, PLAT_LOG_LEVEL_ERROR,
                                 "error syncing superblock, rc=%d", rc );
                }
            }
        }
    }

    return rc;
}

int
recovery_init( void )
{
    int                         rc;
    uint64_t                    s, i, j, ssd;
    uint64_t                    size;
    uint64_t                    blk_count;
    uint64_t                    good_count;
    int                         version_change;
    int                         good_copy[ MCD_AIO_MAX_NFILES ];
    uint64_t                    checksum;
    uint64_t                    seg_buf_size;
    uint64_t                    seg_list_size;
    uint64_t                    metadata_segs;
    uint64_t                    actual_segs;
    uint64_t                    blk_offset;
    uint64_t                    stripe_offset;
    uint64_t                    seg_count;
    char                      * data_buf = NULL;
    char                      * buf;
    osd_state_t               * context =
        mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_INIT );
    mcd_rec_list_block_t      * seg_list;
    mcd_osd_shard_t           * shard;
    mcd_rec_shard_t           * pshard;
    mcd_rec_properties_t      * prop;
    mcd_rec_superblock_t      * sb = &Mcd_rec_superblock;
    mcd_rec_flash_t           * fd = NULL;
    char                      * source[ MCD_AIO_MAX_NFILES ];
    fthWaitEl_t               * wait;

    mcd_log_msg( 20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    // set flags if we want to flush in place
    Log_flush_in_place = getProperty_Int("LOG_FLUSH_IN_PLACE", 0);
    if (Log_flush_in_place) {
        mcd_log_msg(70098, PLAT_LOG_LEVEL_INFO,
                    "Flushing logs in place (LOG_FLUSH_IN_PLACE set)");
    }


    // initialize
    do_aio_init( 1 );
    fthLockInit( &Mcd_rec_free_aio_ctxt_lock );
    fthLockInit( &Mcd_rec_log_segment_lock );
    fthLockInit( &Mcd_rec_upd_segment_lock );
    fthSemInit( &Mcd_rec_upd_seg_sem, 0 );
    memset( sb, 0, sizeof( mcd_rec_superblock_t ) ); // superblock

    plat_assert( Mcd_rec_sb_data_copies <= MCD_OSD_MAX_NUM_SHARDS );

    // FIXME: hack alert
    init_container_meta_blob_put = &container_meta_blob_put;
    init_container_meta_blob_get = &container_meta_blob_get;

    // calculate buffer size to hold aligned 1-block buffers
    // for each shard, the flash descriptor (+1), plus alignment (+1)
    size = (MCD_OSD_MAX_NUM_SHARDS + 2) * Mcd_osd_blk_size;

    // allocate buffer to hold entire superblock
    sb->sb_buf = plat_alloc( size );
    if ( sb->sb_buf == NULL ) {
        mcd_log_msg( 20450, PLAT_LOG_LEVEL_ERROR,
                     "failed to allocate shard props" );
        return FLASH_ENOMEM;
    }
    memset( sb->sb_buf, 0, size );

    // get aligned buffer
    buf = (char *)
        ( ((uint64_t)sb->sb_buf + Mcd_osd_blk_size - 1) & Mcd_osd_blk_mask );

    // initialize shard properties list
    for ( s = 0; s < MCD_OSD_MAX_NUM_SHARDS; s++ ) {
        sb->props[s] = (mcd_rec_properties_t *)(buf + (s * Mcd_osd_blk_size));
    }

    // initialize flash descriptor
    sb->flash_desc = (mcd_rec_flash_t *)(buf + (s * Mcd_osd_blk_size));

    // get segment address for the superblock
    wait = fthLock( &Mcd_osd_segment_lock, 1, NULL );
    blk_offset = Mcd_osd_free_segments[ Mcd_osd_free_seg_curr ] +
                 MCD_REC_LABEL_BLKS;
    blk_count  = 1;
    fthUnlock( wait );

    // --------------------------------------------
    // Get and validate superblock from each device
    // --------------------------------------------

    // calculate buffer size to hold aligned 1-block buffers
    // for each flash desc, plus 1 block for alignment
    size = ( blk_count * Mcd_rec_sb_data_copies * Mcd_osd_blk_size ) +
        Mcd_osd_blk_size;
    plat_assert( size < MEGABYTE );

    // get aligned buffer
    buf = (char *)( ( (uint64_t)context->osd_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

 revalidate_flash_desc:

    good_count     = 0;
    version_change = 0;
    memset( context->osd_buf, 0, size );

    // read and validate flash descriptor on each device
    for ( ssd = 0; ssd < Mcd_rec_sb_data_copies; ssd++ ) {

        source[ ssd ]    = buf + (ssd * Mcd_osd_blk_size);
        good_copy[ ssd ] = 0;

        // using stripe size as offset will read from each drive
        stripe_offset = ssd * Mcd_aio_strip_size;

        // read flash descriptor
        rc = mcd_fth_aio_blk_read( context,
                                   source[ ssd ],
                                   stripe_offset +
                                   (blk_offset * Mcd_osd_blk_size),
                                   blk_count * Mcd_osd_blk_size );
        if ( FLASH_EOK != rc ) {
            mcd_log_msg( 160005, PLAT_LOG_LEVEL_ERROR,
                         "failed to read flash desc, offset=%lu, copy=%lu, "
                         "rc=%d", blk_offset, ssd, rc );
            continue;
        }

        // verify superblock checksum on each device
        fd           = (mcd_rec_flash_t *)source[ ssd ];
        checksum     = fd->checksum;
        fd->checksum = 0;
        fd->checksum = hash( (unsigned char *)fd,
                             blk_count * Mcd_osd_blk_size,
                             MCD_REC_FLASH_EYE_CATCHER );
        if ( fd->checksum != checksum ) {
            mcd_log_msg( 160006, PLAT_LOG_LEVEL_ERROR,
                         "invalid flash desc checksum, offset=%lu, copy=%lu",
                         blk_offset * Mcd_osd_blk_size, ssd );
            fd->checksum = checksum;   // restore original contents
            snap_dump( source[ ssd ], Mcd_osd_blk_size );
            continue;
        }

        // verify static data
        if ( fd->eye_catcher != MCD_REC_FLASH_EYE_CATCHER ||
             fd->blk_size != MCD_OSD_BLK_SIZE ) {
            mcd_log_msg( 160007, PLAT_LOG_LEVEL_ERROR,
                         "invalid flash desc, offset=%lu, copy=%lu",
                         blk_offset, ssd );
            snap_dump( source[ ssd ], Mcd_osd_blk_size );
            continue;
        }

        // check structure version
        if ( fd->version != MCD_REC_FLASH_VERSION ) {
            mcd_log_msg( 160008, PLAT_LOG_LEVEL_WARN,
                         "flash desc version change, "
                         "offset=%lu, copy=%lu, old=%d, new=%d",
                         fd->blk_offset, ssd,
                         fd->version, MCD_REC_FLASH_VERSION );
            version_change = fd->version;
        }

        good_copy[ ssd ] = 1;
        good_count++;
    }

    // validate N copies of the flash descriptor
    rc = validate_superblock_data( (char *)sb->flash_desc,
                                   good_count,
                                   good_copy,
                                   blk_offset,
                                   blk_count,
                                   source );
    plat_assert_rc( rc );

    // recover valid flash descriptor
    fd = sb->flash_desc;

    // handle version change
    if ( version_change ) {
        fd->write_version++;
        fd->version  = MCD_REC_FLASH_VERSION;
        fd->checksum = 0;
        fd->checksum = hash( (unsigned char *)fd,
                             Mcd_osd_blk_size,
                             MCD_REC_FLASH_EYE_CATCHER );
        mcd_log_msg( 40009, PLAT_LOG_LEVEL_INFO,
                     "updating superblock, offset=%lu",
                     fd->blk_offset );

        // write superblock
        if ( (rc = write_superblock( (char *)sb->flash_desc,
                                     blk_count,
                                     blk_offset )) != 0 ||
             (rc = mcd_aio_sync_devices()) != 0 ) {
            mcd_log_msg( 40010, PLAT_LOG_LEVEL_FATAL,
                         "failed to update superblock, offset=%lu, rc=%d",
                         fd->blk_offset, rc );
            plat_abort();
        }
        goto revalidate_flash_desc;
    }

    // check if max_shards has changed; using last fd read from disk
    if ( fd->max_shards != MCD_OSD_MAX_NUM_SHARDS ) {

        plat_assert( MCD_OSD_MAX_NUM_SHARDS <= MCD_REC_MAX_SHARDS );

        // update flash descriptor
        fd->write_version++;
        fd->max_shards = MCD_OSD_MAX_NUM_SHARDS;
        fd->checksum   = 0;
        fd->checksum   = hash( (unsigned char *)fd,
                               Mcd_osd_blk_size,
                               MCD_REC_FLASH_EYE_CATCHER );

        // write superblock
        rc = write_superblock( (char *)sb->flash_desc, blk_count, blk_offset );
        if ( rc != 0 ) {
            mcd_log_msg( 20454, PLAT_LOG_LEVEL_ERROR,
                         "error updating superblock, rc=%d", rc );
            return rc;
        }

        // sync all devices
        rc = mcd_aio_sync_devices();
        if ( rc != 0 ) {
            mcd_log_msg( 20449, PLAT_LOG_LEVEL_ERROR,
                         "error syncing superblock, rc=%d", rc );
            return rc;
        }
    }

    // --------------------------------------------------------------
    // Get list of all persistent and non-persistent shard properties
    // --------------------------------------------------------------

    // number of blocks in prop desc
    blk_count = 1;

    // calculate buffer size to hold aligned 1-block buffers
    // for each shard property desc, plus 1 block for alignment
    size = ( blk_count * Mcd_rec_sb_data_copies * Mcd_osd_blk_size ) +
        Mcd_osd_blk_size;
    plat_assert( size < MEGABYTE );

    // get aligned buffer
    buf = (char *)( ((uint64_t)context->osd_buf + Mcd_osd_blk_size - 1) &
                    Mcd_osd_blk_mask );

 revalidate_props:

    version_change = 0;
    memset( context->osd_buf, 0, size );

    // loop through each shard property descriptor
    for ( s = 0; s < fd->max_shards; s++ ) {
        good_count = 0;

        // prop desc block offset on each device
        blk_offset = fd->blk_offset + fd->prop_offset + (blk_count * s);

        // read shard properties on each device
        for ( ssd = 0; ssd < Mcd_rec_sb_data_copies; ssd++ ) {

            source[ ssd ]    = buf + (ssd * Mcd_osd_blk_size);
            good_copy[ ssd ] = 0;

            // using stripe size as offset will read from each drive
            stripe_offset = ssd * Mcd_aio_strip_size;

            // read shard properties list
            rc = mcd_fth_aio_blk_read( context,
                                       source[ ssd ],
                                       stripe_offset +
                                       (blk_offset * Mcd_osd_blk_size),
                                       blk_count * Mcd_osd_blk_size );
            if ( FLASH_EOK != rc ) {
                mcd_log_msg( 160009, PLAT_LOG_LEVEL_ERROR,
                             "failed to read shard props, offset=%ld, "
                             "ssd=%lu, rc=%d",
                             blk_offset, ssd, rc );
                continue;
            }

            // skip unused slots
            prop = (mcd_rec_properties_t *)source[ ssd ];
            if ( prop->checksum == 0 ) {
                continue;
            }

            // verify checksum on each property descriptor
            checksum       = prop->checksum;
            prop->checksum = 0;
            prop->checksum = hash( (unsigned char *)prop,
                                   Mcd_osd_blk_size,
                                   MCD_REC_PROP_EYE_CATCHER );
            if ( prop->checksum != checksum ) {
                mcd_log_msg( 160010, PLAT_LOG_LEVEL_FATAL,
                             "Invalid property checksum, offset=%lu, "
                             "ssd=%lu, slot=%lu",
                             blk_offset, ssd, s );
                prop->checksum = checksum;   // restore original contents
                snap_dump( source[ ssd ], Mcd_osd_blk_size );
                continue;
            }

            // verify static data
            if ( prop->eye_catcher != MCD_REC_PROP_EYE_CATCHER ) {
                mcd_log_msg( 160011, PLAT_LOG_LEVEL_ERROR,
                             "invalid property desc, offset=%lu, "
                             "copy=%lu, slot=%lu",
                             blk_offset, ssd, s );
                snap_dump( source[ ssd ], Mcd_osd_blk_size );
                continue;
            }

            // check structure version
            if ( prop->version != MCD_REC_PROP_VERSION ) {
                mcd_log_msg( 160012, PLAT_LOG_LEVEL_WARN,
                             "property desc version change, "
                             "offset=%lu, copy=%lu, slot=%lu, old=%d, new=%d",
                             blk_offset, ssd, s,
                             prop->version, MCD_REC_PROP_VERSION );
                version_change = prop->version;
            }

            good_copy[ ssd ] = 1;
            good_count++;
        }

        // skip empty property blocks
        if ( good_count == 0 ) {
            continue;
        }

        // validate N copies of this property descriptor
        rc = validate_superblock_data( (char *)sb->props[ s ],
                                       good_count,
                                       good_copy,
                                       blk_offset,
                                       blk_count,
                                       source );
        plat_assert_rc( rc );
    }

    // handle version change
    if ( version_change ) {
        for ( s = 0; s < fd->max_shards; s++ ) {
            prop = sb->props[ s ];
            if ( prop->checksum == 0 ) {
                continue;
            }

            // property version mismatch
            if ( prop->version != MCD_REC_PROP_VERSION ) {

                // convert V1 -> V2
                if ( prop->version == MCD_REC_PROP_VERSION_V1 ) {
                    mcd_container_t  cntr;
                    memset( &cntr, 0, sizeof( mcd_container_t ) );
                    shard_get_properties( s, &cntr );
                    if ( 0 !=
                         (*flash_settings.cntr_update_callback)( &cntr,
                                                   MCD_CNTR_VERSION_V2 ) ) {
                        mcd_log_msg( 50022, PLAT_LOG_LEVEL_FATAL,
                                     "failed to upgrade mcd_container" );
                        plat_abort();
                    }
                    shard_set_properties_internal( &cntr, prop );
                    prop->version = MCD_REC_PROP_VERSION;
                }

                // install new version of props
                prop->write_version++;
                prop->checksum = 0;
                prop->checksum = hash( (unsigned char *)prop,
                                       blk_count * Mcd_osd_blk_size,
                                       MCD_REC_PROP_EYE_CATCHER );
                mcd_log_msg( 160013, PLAT_LOG_LEVEL_INFO,
                             "updating properties, offset=%lu, slot=%lu",
                             blk_offset, s );

                // write shard properties
                if ( (rc = write_property( s )) != 0 ||
                     (rc = mcd_aio_sync_devices()) != 0 ) {
                    mcd_log_msg( 160014, PLAT_LOG_LEVEL_FATAL,
                                 "failed to update shard props, "
                                 "offset=%lu, slot=%lu, rc=%d",
                                 blk_offset, s, rc );
                    plat_abort();
                }
            }
        }
        goto revalidate_props;
    }

    // --------------------------------------------
    // Get and validate all persistent shards
    // --------------------------------------------

    // get aligned buffer
    buf = (char *)( ( (uint64_t)context->osd_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

    // retrieve shard descriptors
    for ( s = 0; s < fd->max_shards; s++ ) {

        // skip empty slots and non-persistent shards
        if ( sb->props[ s ]->shard_id == 0 ||
             sb->props[ s ]->persistent == 0 ) {
            continue;
        }

        // read shard descriptor
        rc = mcd_fth_aio_blk_read( context,
                                   buf,
                                   (sb->props[ s ]->blk_offset *
                                    Mcd_osd_blk_size),
                                   Mcd_osd_blk_size );
        if ( FLASH_EOK != rc ) {
            mcd_log_msg( 20458, PLAT_LOG_LEVEL_ERROR,
                         "failed to read shard desc, rc=%d", rc );
            return rc;
        }

        pshard = (mcd_rec_shard_t *)buf;

        // verify shard descriptor checksum
        checksum         = pshard->checksum;
        pshard->checksum = 0;
        pshard->checksum = hash( (unsigned char *)pshard,
                                 Mcd_osd_blk_size,
                                 MCD_REC_SHARD_EYE_CATCHER );
        if ( pshard->checksum != checksum ||
             pshard->eye_catcher != MCD_REC_SHARD_EYE_CATCHER ||
             pshard->version != MCD_REC_SHARD_VERSION ) {
            mcd_log_msg( 20459, PLAT_LOG_LEVEL_FATAL,
                         "invalid shard checksum, blk_offset=%lu",
                         sb->props[ s ]->blk_offset );
            pshard->checksum = checksum;   // restore original contents
            snap_dump( buf, Mcd_osd_blk_size );
            plat_abort();
        }

        // create volatile shard descriptor
        shard = (mcd_osd_shard_t *)plat_alloc( sizeof( mcd_osd_shard_t ) );
        if ( shard == NULL ) {
            mcd_log_msg( 20460, PLAT_LOG_LEVEL_ERROR,
                         "failed to allocate shard" );
            return FLASH_ENOMEM;
        }
        memset( shard, 0, sizeof( mcd_osd_shard_t ) );

        // create persistent shard descriptor
        pshard = plat_alloc( sizeof( mcd_rec_shard_t ) );
        if ( pshard == NULL ) {
            mcd_log_msg( 20461, PLAT_LOG_LEVEL_ERROR,
                         "failed to allocate pshard" );
            plat_free( shard );
            return FLASH_ENOMEM;
        }
        shard->ps_alloc += sizeof( mcd_rec_shard_t );

        // recover valid persistent shard descriptor
        memcpy( pshard, buf, sizeof( mcd_rec_shard_t ) );

        shard->pshard = pshard;
        shard->open   = 0;

        // this read should not cross a segment boundary
        plat_assert( (pshard->blk_offset + pshard->seg_list_offset) /
                     Mcd_osd_segment_blks ==
                     (pshard->blk_offset + pshard->seg_list_offset +
                      pshard->map_blks - 1) /
                     Mcd_osd_segment_blks );

        // read the segment table for this shard
        rc = mcd_fth_aio_blk_read( context,
                                   buf,
                                   (pshard->blk_offset +
                                    pshard->seg_list_offset) *
                                   Mcd_osd_blk_size,
                                   pshard->map_blks * Mcd_osd_blk_size );
        if ( FLASH_EOK != rc ) {
            mcd_log_msg( 20462, PLAT_LOG_LEVEL_ERROR,
                         "failed to read shard metadata, shrad_offset=%lu, "
                         "offset=%lu, rc=%d", pshard->blk_offset,
                         pshard->seg_list_offset, rc );
            plat_free( pshard );
            plat_free( shard );
            return rc;
        }

        // calculate number of segments reserved for metadata
        metadata_segs = ( (pshard->reserved_blks + Mcd_osd_segment_blks - 1) /
                          Mcd_osd_segment_blks );
        actual_segs   = pshard->total_blks / Mcd_osd_segment_blks;
        seg_list_size = actual_segs * sizeof( uint32_t ); // yes, uint32_t

        // create the segment table for this shard
        shard->segments = plat_alloc( seg_list_size );
        if ( shard->segments == NULL ) {
            mcd_log_msg( 20463, PLAT_LOG_LEVEL_ERROR,
                         "failed to allocate segment list" );
            plat_free( pshard );
            plat_free( shard );
            return FLASH_ENOMEM;
        }

        // set key fields of shard descriptor (needed by shard init)
        shard->total_segments  = actual_segs - metadata_segs;
        shard->data_blk_offset = metadata_segs * Mcd_osd_segment_blks;

        // install the segment mapping table
        seg_count = 0;
        for ( i = 0;
              i < pshard->map_blks && seg_count < actual_segs;
              i++ ) {

            seg_list = (mcd_rec_list_block_t *)(buf + (i * Mcd_osd_blk_size));

            // verify class segment block checksum
            checksum           = seg_list->checksum;
            seg_list->checksum = 0;
            seg_list->checksum = hash( (unsigned char *)seg_list,
                                       Mcd_osd_blk_size,
                                       0 );
            if ( seg_list->checksum != checksum ) {
                mcd_log_msg( 20464, PLAT_LOG_LEVEL_FATAL,
                             "invalid class seglist checksum, "
                             "shard_offset=%lu, offset=%lu",
                             pshard->blk_offset, pshard->seg_list_offset + i);
                seg_list->checksum = checksum;   // restore original contents
                snap_dump( (char *)seg_list, Mcd_osd_blk_size );
                plat_abort();
            }

            for ( j = 0;
                  j < MCD_REC_LIST_ITEMS_PER_BLK && seg_count < actual_segs;
                  j++ ) {
                shard->segments[ seg_count++ ] = (uint32_t)seg_list->data[ j ];
                mcd_log_msg( 40064, PLAT_LOG_LEVEL_TRACE,
                             "recovered segment[%lu]: blk_offset=%u",
                             seg_count - 1, shard->segments[ seg_count ] );
            }
        }

        // initialize parts of public shard descriptor
        shard->shard.shardID = pshard->shard_id;
        shard->shard.flags   = pshard->flags;
        shard->shard.quota   = pshard->quota;
        shard->shard.maxObjs = pshard->obj_quota;

        // cache the shard descriptor
        for ( i = 0; i < MCD_OSD_MAX_NUM_SHARDS; i++ ) {
            if ( NULL == Mcd_osd_slab_shards[ i ] ) {
                Mcd_osd_slab_shards[ i ] = shard;
                break;
            }
        }
        plat_assert_always( i < MCD_OSD_MAX_NUM_SHARDS );

        mcd_log_msg( 20465, PLAT_LOG_LEVEL_INFO,
                     "found persistent shard, id=%lu, blk_offset=%lu, "
                     "md_blks=%lu, table_blks=%lu, pad=%lu, log_blks=%lu, "
                     "pad=%lu (%lu entries)",
                     pshard->shard_id, pshard->blk_offset, pshard->rec_md_blks,
                     pshard->rec_table_blks, pshard->rec_table_pad,
                     pshard->rec_log_blks, pshard->rec_log_pad,
                     pshard->rec_log_blks * MCD_REC_LOG_BLK_RECS );
    }

    // FIXME: temporary!
    // set the chicken switch based on a property (default=OLD)
    // Default for chicken switch is now NEW, as of 2010-03-31
    Mcd_rec_chicken =
        strncmp( getProperty_String( "MCD_PERSISTENT_UPDATES", "NEW" ),
                 "NEW", 3 );

    // Set defaults for recovery buffer sizes and such
    if ( !Mcd_rec_chicken ) {
        Mcd_rec_update_bufsize      = MCD_REC_UPDATE_BUFSIZE;
        Mcd_rec_update_segment_size = MCD_REC_UPDATE_SEGMENT_SIZE;
        Mcd_rec_update_segment_blks = MCD_REC_UPDATE_SEGMENT_BLKS;
        Mcd_rec_update_max_chunks   = MCD_REC_UPDATE_MAX_CHUNKS;
        Mcd_rec_update_yield        = MCD_REC_UPDATE_YIELD;
    } else {
        Mcd_rec_update_bufsize      = MCD_REC_UPDATE_BUFSIZE_CHICKEN;
        Mcd_rec_update_segment_size = MCD_REC_UPDATE_SEGMENT_SIZE_CHICKEN;
        Mcd_rec_update_segment_blks = MCD_REC_UPDATE_SEGMENT_BLKS_CHICKEN;
    }

    Mcd_rec_log_segment_size = MCD_REC_LOG_SEGMENT_SIZE;
    Mcd_rec_log_segment_blks = MCD_REC_LOG_SEGMENT_BLKS;

    // Override defaults with command line settings
    if ( flash_settings.rec_upd_bufsize != 0 ) {
        Mcd_rec_update_bufsize = flash_settings.rec_upd_bufsize * MEGABYTE;
    }

    if ( flash_settings.rec_upd_max_chunks != 0 ) {
        Mcd_rec_update_max_chunks = flash_settings.rec_upd_max_chunks;
    }

    if ( flash_settings.rec_upd_yield != -1 ) { // zero has meaning
        Mcd_rec_update_yield = flash_settings.rec_upd_yield;
    }

    if ( flash_settings.rec_upd_segsize != 0 ) {
        Mcd_rec_update_segment_size = flash_settings.rec_upd_segsize * MEGABYTE;
        Mcd_rec_update_segment_blks = (Mcd_rec_update_segment_size /
                                       Mcd_osd_blk_size);
    }

    if ( flash_settings.rec_log_segsize != 0 ) {
        Mcd_rec_log_segment_size = flash_settings.rec_log_segsize * MEGABYTE;
        Mcd_rec_log_segment_blks = Mcd_rec_log_segment_size / Mcd_osd_blk_size;
    }

    // 0=no verify; 1=verify with notification; 2=verify with abort
    if ( flash_settings.rec_upd_verify != 0 ) {
        Mcd_rec_update_verify = flash_settings.rec_upd_verify;
    }

    // 0=no verify; 1=verify with notification; 2=verify with abort
    if ( flash_settings.rec_log_verify != 0 ) {
        Mcd_rec_update_verify_log = flash_settings.rec_log_verify;
    }

    // allocate recovery buffers for object table and log
    if ( !Mcd_rec_chicken ) {
        // alloc a massive chunk of memory to carve up into aligned segments
        // (in MB) used to read persistent logs during object table updates
        seg_count    =
            (Mcd_aio_total_size / GIGABYTE * 4) /    // max log size (MB)
            (Mcd_rec_log_segment_size / MEGABYTE);   // divided by segment MB
        seg_buf_size = ( (seg_count * Mcd_rec_log_segment_size) +
                         Mcd_osd_blk_size );

        data_buf = plat_alloc_large( seg_buf_size );
        if ( data_buf == NULL ) {
            mcd_log_msg( 40092, PLAT_LOG_LEVEL_ERROR,
                         "failed to allocate %lu byte buffer for %lu log "
                         "segments", seg_buf_size, seg_count );
            return FLASH_ENOMEM;
        }
        // xxxzzz
        memset( data_buf, 0, seg_buf_size );
        Mcd_rec_log_segments_anchor = data_buf;
        mcd_log_msg( 40062, PLAT_LOG_LEVEL_INFO,
                     "allocated %lu byte buffer for %lu log segments",
                     seg_buf_size, seg_count );

        // align to 1-block boundary
        buf = (char *)
            ( ((uint64_t)data_buf + Mcd_osd_blk_size - 1) & Mcd_osd_blk_mask );

        // alloc an array to hold unused log segments
        Mcd_rec_free_log_segments = plat_alloc( seg_count * sizeof( char * ) );
        if ( Mcd_rec_free_log_segments == NULL ) {
            mcd_log_msg( 40093, PLAT_LOG_LEVEL_ERROR,
                         "failed to allocate segment array, size=%lu",
                         seg_count * sizeof( char * ) );
            return FLASH_ENOMEM;
        }

        // carve the big chunk of memory into aligned segments
        for ( s = 0; s < seg_count; s++ ) {
            Mcd_rec_free_log_segments[ Mcd_rec_free_log_seg_curr++ ] =
                buf + (s * Mcd_rec_log_segment_size);
        }

        // alloc another chunk of memory to carve up into aligned segments
        // (in MB) used to read the object table during object table updates
        seg_count    = Mcd_rec_update_bufsize / Mcd_rec_update_segment_size;
        seg_buf_size = ( (seg_count * Mcd_rec_update_segment_size) +
                         Mcd_osd_blk_size );

        data_buf = plat_alloc_large( seg_buf_size );
        if ( data_buf == NULL ) {
            mcd_log_msg( 40094, PLAT_LOG_LEVEL_ERROR,
                         "failed to allocate %lu byte buffer for %lu update "
                         "segments", seg_buf_size, seg_count );
            return FLASH_ENOMEM;
        }
        Mcd_rec_upd_segments_anchor = data_buf;
        mcd_log_msg( 40095, PLAT_LOG_LEVEL_INFO,
                     "allocated %lu byte buffer for %lu update segments",
                     seg_buf_size, seg_count );

        // align to 1-block boundary
        buf = (char *)
            ( ((uint64_t)data_buf + Mcd_osd_blk_size - 1) & Mcd_osd_blk_mask );

        // alloc an array to hold unused buffer segments
        Mcd_rec_free_upd_segments = plat_alloc( seg_count * sizeof( char * ) );
        if ( Mcd_rec_free_upd_segments == NULL ) {
            mcd_log_msg( 40093, PLAT_LOG_LEVEL_ERROR,
                         "failed to allocate segment array, size=%lu",
                         seg_count * sizeof( char * ) );
            return FLASH_ENOMEM;
        }

        // carve the big chunk of memory into aligned segments
        for ( s = 0; s < seg_count; s++ ) {
            Mcd_rec_free_upd_segments[ Mcd_rec_free_upd_seg_curr++ ] =
                buf + (s * Mcd_rec_update_segment_size);
            fthSemUp( &Mcd_rec_upd_seg_sem, 1 );
        }
    }

    // start thread for updating shards
    if ( Mcd_rec_chicken ) {
        fthMboxInit( &Mcd_rec_updater_mbox );
        fthResume( fthSpawn( &updater_thread_chicken, 81920 ),(uint64_t)NULL );
    }

    return 0;
}

int
recovery_reclaim_space( void )
{
    bool                        updated = false;
    int                         s, i, rc = 0;
    int                         slot;
    mcd_osd_shard_t           * shard;
    mcd_rec_properties_t      * prop;
    mcd_rec_superblock_t      * sb = &Mcd_rec_superblock;
    mcd_rec_flash_t           * fd = sb->flash_desc;
    fthWaitEl_t               * wait;
    int                         open_shards[ MCD_OSD_MAX_NUM_SHARDS ];

    mcd_log_msg( 20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    memset( open_shards, 0, sizeof( int ) * MCD_OSD_MAX_NUM_SHARDS );

    wait = fthLock( &Mcd_osd_segment_lock, 1, NULL );

    // account for superblock
    if ( !Mcd_rec_superblock_formatted ) {
        Mcd_osd_free_seg_curr--;
    }

    // examine entire list of shards
    for ( s = 0; s < MCD_OSD_MAX_NUM_SHARDS; s++ ) {

        shard = Mcd_osd_slab_shards[ s ];

        // shard is open
        if ( shard != NULL && shard->open ) {

            // find shard props, mark open
            slot = ~( shard->prop_slot );
            prop = sb->props[ slot ];

            open_shards[ slot ] = 1;

            // Note: when a container is changed from P to NP without
            // changing the "container_id" in the prop file, the original
            // P container is opened, recovered, and has its properties
            // updated, which include the NP property from the prop file.
            // We can catch it here (when it's already too late), and
            // either abort or change it back to persistent. Going from
            // NP to P doesn't cause a problem, since the NP shard
            // isn't read during init and placed in a list of P shards
            // to recover; it just gets created normally.
            if ( prop->persistent != shard->persistent ) {
                mcd_log_msg( 20466, PLAT_LOG_LEVEL_FATAL,
                             "container %s.%d.%d property changed from "
                             "persistent to non-persistent - leaving as "
                             "persistent",
                             prop->cname, prop->tcp_port, prop->container_id );
                prop->persistent = 1;
                prop->checksum = 0;
                prop->checksum = hash( (unsigned char *)prop,
                                       Mcd_osd_blk_size,
                                       MCD_REC_PROP_EYE_CATCHER );
                rc = write_property( slot );
                plat_assert_rc( rc );
            }

            // update free segment list by removing open shards' segments
            for ( i = 0;
                  i < shard->pshard->total_blks / Mcd_osd_segment_blks;
                  i++ ) {

                // remove allocated segment from free list
                remove_segment( shard->segments[ i ] );

                // update current pointer unless we're formatting
                if ( !Mcd_rec_superblock_formatted ) {
                    Mcd_osd_free_seg_curr--;
                }
            }
        }

        // remove unopened, persistent shards (remove props below)
        else if ( shard != NULL && shard->pshard != NULL ) {

            // find block offset for shard in props
            for ( i = 0; i < fd->max_shards; i++ ) {
                if ( sb->props[i]->blk_offset == shard->pshard->blk_offset ) {
                    break;
                }
            }
            if ( i == fd->max_shards ) {
                mcd_log_msg( 20467, PLAT_LOG_LEVEL_FATAL,
                             "can't find shard, shardID=%lu",
                             shard->id );
                plat_abort();
            }

            // update in-memory structures
            Mcd_osd_slab_shards[ s ] = NULL;

            // free allocated memory
            plat_free( shard->pshard );
            shard->ps_alloc -= sizeof( mcd_rec_shard_t );
            plat_free( shard->segments );
            plat_free( shard );
        }
    }

    fthUnlock( wait );

    // remove unopened persistent and non-persistent shard props
    for ( slot = 0; slot < fd->max_shards; slot++ ) {

        if ( open_shards[ slot ] == 0 &&
             sb->props[ slot ]->shard_id != 0 ) {

            prop = sb->props[ slot ];
            mcd_log_msg( 20468, PLAT_LOG_LEVEL_INFO,
                         "removing %s shard, shardID=%lu "
                         "name=%s, tcpport=%d, id=%d "
                         "(not in current configuration)",
                         (prop->persistent ? "persistent" : "non-persistent"),
                         prop->shard_id, prop->cname,
                         prop->tcp_port, prop->container_id );

            // remove the shard properties
            memset( sb->props[ slot ], 0, Mcd_osd_blk_size );

            // write the formatted property slot
            rc = write_property( slot );
            if ( rc != 0 ) {
                mcd_log_msg( 20469, PLAT_LOG_LEVEL_ERROR,
                             "write property failed, rc=%d", rc );
                return rc;
            }
            updated = true;
        }
    }

    // sync all devices
    if ( updated ) {
        rc = mcd_aio_sync_devices();
    }

    return rc;
}

void
recovery_halt( flashDev_t * flashDev )
{
    shard_t                   * lshard = flashDev->shardList;
    mcd_osd_shard_t           * shard;

    // tell each shard log writer to end
    while ( lshard != NULL ) {
        shard = (mcd_osd_shard_t *)lshard;

        // stop background threads, free memory structures
        if ( shard->persistent ) {
            shard_unrecover( shard );
        }

        lshard = lshard->next;
    }

    if ( Mcd_rec_chicken ) {
        // tell updater thread to end
        fthMboxPost( &Mcd_rec_updater_mbox, 0 );
    }

    // free all memory
    plat_free( Mcd_rec_superblock.sb_buf );
    plat_free( Mcd_rec_free_log_segments );        // arrays
    plat_free( Mcd_rec_free_upd_segments );
    plat_free( Mcd_rec_log_segments_anchor );      // huge buffers
    plat_free( Mcd_rec_upd_segments_anchor );

    return;
}

int
update_class( mcd_osd_shard_t * shard, mcd_osd_slab_class_t * class,
              uint32_t segment_num )
{
    int                         bs, rc;
    int                         class_index;
    int                         blks;
    int                         seg_slot;
    int                         seg_blk_offset;
    char                      * buf;
    osd_state_t               * context =
        mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_CLASS );
    uint64_t                    offset;
    uint64_t                    pclass_offset;
    uint64_t                    checksum;
    mcd_rec_list_block_t      * seg_list;
    mcd_rec_shard_t           * pshard;
    mcd_rec_class_t           * pclass;

    // get aligned buffer
    buf = (char *)( ( (uint64_t)context->osd_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

    // get persistent shard descriptor
    pshard = shard->pshard;
    plat_assert_always( pshard != NULL );
    plat_assert( pshard->shard_id == shard->id );

    // find which persistent class descriptor to read
    class_index = 0;
    for ( bs = 1; bs < class->slab_blksize; bs *= 2 ) {
        class_index++;
    }
    pclass_offset = pshard->blk_offset + pshard->class_offset[ class_index ];
    blks          = 1 + pshard->map_blks;

    // this read should not cross a segment boundary
    plat_assert( pclass_offset / Mcd_osd_segment_blks ==
                 (pclass_offset + blks - 1) / Mcd_osd_segment_blks );

    // read persistent class desc and segment array for this class
    rc = mcd_fth_aio_blk_read( context,
                               buf,
                               pclass_offset * Mcd_osd_blk_size,
                               blks * Mcd_osd_blk_size );
    if ( FLASH_EOK != rc ) {
        mcd_log_msg( 20470, PLAT_LOG_LEVEL_ERROR,
                     "failed to read seg list, shardID=%lu, blk_offset=%lu, "
                     "blks=%d, rc=%d", shard->id, pclass_offset,
                     (1 + pshard->map_blks), rc );
        return rc;
    }

    pclass = (mcd_rec_class_t *)buf;

    // verify class descriptor checksum
    checksum         = pclass->checksum;
    pclass->checksum = 0;
    pclass->checksum = hash( (unsigned char *)pclass,
                             Mcd_osd_blk_size,
                             MCD_REC_CLASS_EYE_CATCHER );
    if ( pclass->checksum != checksum ||
         pclass->eye_catcher != MCD_REC_CLASS_EYE_CATCHER ||
         pclass->version != MCD_REC_CLASS_VERSION ||
         pclass->slab_blksize != class->slab_blksize ) {
        mcd_log_msg( 20471, PLAT_LOG_LEVEL_FATAL,
                     "invalid class checksum, shardID=%lu, blk_offset=%lu "
                     "(blksize=%d)",
                     shard->id, pclass_offset, class->slab_blksize );
        pclass->checksum = checksum;   // restore original contents
        snap_dump( buf, Mcd_osd_blk_size );
        plat_abort();
    }

    // validate checksum on each block
    for ( int b = 0; b < pshard->map_blks; b++ ) {

        // map list structure on to buffer
        seg_list = (mcd_rec_list_block_t *)
            ( buf + ( (pclass->seg_list_offset + b) * Mcd_osd_blk_size ) );

        // check the checksum
        checksum           = seg_list->checksum;
        seg_list->checksum = 0;
        seg_list->checksum = hash( (unsigned char *)seg_list,
                                   Mcd_osd_blk_size,
                                   0 );
        if ( seg_list->checksum != checksum ) {
            mcd_log_msg( 20472, PLAT_LOG_LEVEL_FATAL,
                         "invalid class seglist checksum, blk_offset=%lu "
                         "(blksize=%d, slo=%lu, offset=%d)",
                         pclass_offset + pclass->seg_list_offset + b,
                         class->slab_blksize, pclass->seg_list_offset, b );
            seg_list->checksum = checksum;   // restore original contents
            snap_dump( (char *)seg_list, Mcd_osd_blk_size );
            plat_abort();
        }
    }

    // calculate where the new segment goes
    seg_blk_offset = segment_num / MCD_REC_LIST_ITEMS_PER_BLK;
    seg_slot       = segment_num % MCD_REC_LIST_ITEMS_PER_BLK;

    // map list structure on to buffer
    seg_list = (mcd_rec_list_block_t *)
        ( buf + ( (pclass->seg_list_offset + seg_blk_offset) *
                  Mcd_osd_blk_size ) );

    // install new segment
    // Note: since block offset 0 is a valid segment address, the offset
    // is bitwise inverted when stored so it can be found in recovery
    seg_list->data[ seg_slot ] = ~(class->segments[ segment_num ]->blk_offset);

    // install new checksum
    seg_list->checksum = 0;
    seg_list->checksum = hash( (unsigned char *)seg_list,
                               Mcd_osd_blk_size,
                               0 );

    offset = pclass_offset + pclass->seg_list_offset + seg_blk_offset;

    mcd_log_msg( 40065, PLAT_LOG_LEVEL_TRACE,
                 "class[%d], blksize=%d: new_seg[%d], blk_offset=%lu",
                 class_index, class->slab_blksize, segment_num,
                 class->segments[ segment_num ]->blk_offset );

    // write persistent segment list (single updated block)
    rc = mcd_fth_aio_blk_write( context,
                                (char *)seg_list,
                                offset * Mcd_osd_blk_size,
                                Mcd_osd_blk_size );
    if ( FLASH_EOK != rc ) {
        mcd_log_msg( 20473, PLAT_LOG_LEVEL_ERROR,
                     "failed to write seg list blk, shardID=%lu, "
                     "blk_offset=%lu (blksize=%d), off/slot=%d/%d, rc=%d",
                     shard->id, pclass_offset, class->slab_blksize,
                     seg_blk_offset, seg_slot, rc );

        return rc;
    }

    // sync device that was written to
    rc = mcd_aio_sync_device_offset( offset * Mcd_osd_blk_size,
                                     Mcd_osd_blk_size );
    plat_assert_rc( rc );

    return 0;
}


/************************************************************************
 *                                                                      *
 *                 Memcached SLAB Clustering support                    *
 *                                                                      *
 ************************************************************************/

void
tombstone_register_rtg_callback( void (*callback)( uint64_t shardID,
                                                   uint64_t seqno ) )
{
    Mcd_set_rtg_callback = callback;
}

inline uint64_t
tombstone_get_rtg( mcd_osd_shard_t * shard )
{
    return shard->log->rtg_seqno;
}

inline uint64_t
tombstone_get_lcss( mcd_osd_shard_t * shard )
{
    if ( shard->replicated ) {
        return shard->lcss;
    }
    return 0xffffffffffffffffull;
}

#ifdef MCD_ENABLE_TOMBSTONES
mcd_rec_tombstone_t *
tombstone_get( mcd_osd_shard_t * shard )
{
    mcd_rec_tombstone_t       * ts;

    mcd_rlg_msg( 20474, PLAT_LOG_LEVEL_DEVEL,
                 "ENTERING, shardID=%lu, curr=%d, "
                 "free=%d, xtra=%d", shard->id, shard->ts_list->curr_count,
                 shard->ts_list->free_count, shard->ts_list->xtra_count );

    if ( shard->ts_list->free_tail != NULL ) {
        ts                        = shard->ts_list->free_tail;
        shard->ts_list->free_tail = ts->next;

        if ( shard->ts_list->free_tail != NULL ) {
            shard->ts_list->free_tail->prev = NULL;
        } else {
            shard->ts_list->free_head = NULL;
        }

        shard->ts_list->free_count--;
        ts->malloc = false;

    } else {
        ts = plat_alloc( sizeof( mcd_rec_tombstone_t ) );
        if ( ts == NULL ) {
            mcd_rlg_msg( 20475, PLAT_LOG_LEVEL_FATAL, "shardID=%lu, no more "
                         "tombstones available, curr=%d, free=%d, xtra=%d",
                         shard->id, shard->ts_list->curr_count,
                         shard->ts_list->free_count,
                         shard->ts_list->xtra_count );
            plat_abort();
        }

        ts->malloc = true;
        shard->ts_list->xtra_count++;
    }
    shard->ts_list->curr_count++;

    ts->next       = NULL;
    ts->prev       = NULL;
    ts->seqno      = 0;
    ts->blk_offset = 0;
    ts->syndrome   = 0;

    return ts;
}

inline void
tombstone_free( mcd_osd_shard_t * shard, mcd_rec_tombstone_t * ts )
{
    mcd_rlg_msg( 20474, PLAT_LOG_LEVEL_DEVEL,
                 "ENTERING, shardID=%lu, curr=%d, "
                 "free=%d, xtra=%d", shard->id, shard->ts_list->curr_count,
                 shard->ts_list->free_count, shard->ts_list->xtra_count );

    ts->next = NULL;
    ts->prev = NULL;

    if ( ts->malloc ) {
        shard->ts_list->xtra_count--;
        plat_free( ts );

    } else {
        if ( shard->ts_list->free_head != NULL ) {
            shard->ts_list->free_head->next = ts;
            ts->prev                        = shard->ts_list->free_head;
            shard->ts_list->free_head       = ts;
        } else {
            plat_assert( shard->ts_list->free_tail == NULL );
            plat_assert( shard->ts_list->free_count == 0 );
            shard->ts_list->free_head = ts;
            shard->ts_list->free_tail = ts;
        }
        shard->ts_list->free_count++;
    }

    shard->ts_list->curr_count--;

    return;
}

int
tombstone_init( mcd_osd_shard_t * shard )
{
    int                         ts_max;

    mcd_rlg_msg( 20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    // FIXME: pick a size for the in-memory tombstone list.
    // This list is used to dealloc flash space after tombstones
    // are truncated. Later, we'll revert back to freeing space
    // immediately after a log buffer is written, and do away with
    // this in-memory data structure completely.

    // this will be about 1/16th the size of the log
    ts_max = shard->pshard->rec_log_blks + shard->pshard->rec_log_pad;

    // alloc a tombstone list struct
    shard->ts_list = plat_alloc( sizeof( mcd_rec_ts_list_t ) );
    if ( shard->ts_list == NULL ) {
        mcd_rlg_msg( 20476, PLAT_LOG_LEVEL_ERROR,
                     "failed to alloc tombstone list anchor" );
        return FLASH_ENOMEM;
    }
    shard->ps_alloc += sizeof( mcd_rec_ts_list_t );

    // initialize
    shard->ts_list->slab       = NULL;
    shard->ts_list->free_head  = NULL;
    shard->ts_list->free_tail  = NULL;
    shard->ts_list->free_count = 0;
    shard->ts_list->xtra_count = 0;
    shard->ts_list->curr_count = ts_max;
    shard->ts_list->list_head  = NULL;
    shard->ts_list->list_tail  = NULL;

    // alloc enough memory for "maximum" number of tombstones
    shard->ts_list->slab = plat_alloc( sizeof(mcd_rec_tombstone_t) * ts_max );
    if ( shard->ts_list->slab == NULL ) {
        mcd_rlg_msg( 20477, PLAT_LOG_LEVEL_ERROR,
                     "failed to alloc tombstone list" );
        return FLASH_ENOMEM;
    }
    shard->ps_alloc += (sizeof( mcd_rec_tombstone_t ) * ts_max);

    for ( int i = 0; i < ts_max; i++ ) {
        mcd_rec_tombstone_t * ts;
        ts = (mcd_rec_tombstone_t *)((uint64_t)shard->ts_list->slab +
                                     (i * sizeof( mcd_rec_tombstone_t )));
        ts->malloc = false;
        tombstone_free( shard, ts );
    }

    return 0;
}

void
tombstone_add( mcd_osd_shard_t * shard, uint16_t syndrome,
               uint32_t blk_offset, uint64_t seqno )
{
    mcd_rec_tombstone_t       * ts;

    mcd_rlg_msg( 20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    // Note: no locking is needed since this is called by a single thread:
    // during recovery it is called by the updater thread; during normal
    // operation it is called by the log writer thread.

    // get a tombstone and initialize it
    ts             = tombstone_get( shard );
    ts->seqno      = seqno;
    ts->blk_offset = blk_offset;
    ts->syndrome   = syndrome;

    // add new tombstone to empty queue
    if ( shard->ts_list->list_head == NULL ) {
        shard->ts_list->list_head = ts;
        shard->ts_list->list_tail = ts;
        ts->next                  = NULL;
        ts->prev                  = NULL;
    }

    // add new tombstone to head of queue
    else if ( ts->seqno > shard->ts_list->list_head->seqno ) {
        ts->next                        = NULL;
        ts->prev                        = shard->ts_list->list_head;
        shard->ts_list->list_head->next = ts;
        shard->ts_list->list_head       = ts;
    }

    // traverse list backward from head to insert tombstone in seqno order
    else {
        mcd_rec_tombstone_t * pts;
        for ( pts = shard->ts_list->list_head->prev;
              pts != NULL && pts->seqno > seqno;
              pts = pts->prev ) {
        }
        if ( pts != NULL ) {
            pts->next->prev = ts;
            ts->next        = pts->next;
            pts->next       = ts;
            ts->prev        = pts;
        } else {
            shard->ts_list->list_tail->prev = ts;
            ts->next                        = shard->ts_list->list_tail;
            ts->prev                        = NULL;
            shard->ts_list->list_tail       = ts;
        }
    }

    return;
}

void
tombstone_prune( mcd_osd_shard_t * shard )
{
    mcd_rec_tombstone_t       * ts;
    mcd_rec_tombstone_t       * del_ts;
    uint64_t                    lcss = tombstone_get_lcss( shard );

    mcd_rlg_msg( 20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    // Note: no locking is needed since this is called by a single thread:
    // during recovery it is called by the updater thread; during normal
    // operation it is called by the log writer thread.

    // access tail (oldest tombstone)
    ts = shard->ts_list->list_tail;

    // remove tombstones when lcss advances
    while ( ts != NULL && ts->seqno < lcss ) {

        // pull oldest tombstone off
        del_ts = ts;
        ts     = ts->next;

        // dealloc disk space
        mcd_fth_osd_slab_dealloc( shard, del_ts->blk_offset );

        // delete the tombstone
        tombstone_free( shard, del_ts );
    }

    // update the tail pointer
    shard->ts_list->list_tail = ts;
    if ( shard->ts_list->list_tail == NULL ) {
        shard->ts_list->list_head = NULL;
    } else {
        shard->ts_list->list_tail->prev = NULL;
    }

    return;
}
#endif  // MCD_ENABLE_TOMBSTONES

/************************************************************************
 *                                                                      *
 *                      Memcached SLAB Recovery                         *
 *                                                                      *
 ************************************************************************/


/*
 * Show a buffer.  Used for debugging.
 */
#if 0
static void
show_buf(unsigned char *buf, char *msg)
{
    int i;
    const int l = 16;
    const int n = 512;

    fprintf(stderr, "%s\n", msg);
    for (i = 0; i < n; i++) {
        if (i%l == 0)
            fprintf(stderr, "  %03x ", i);
        fprintf(stderr, " %02x", buf[i]);
        if (i%l == l-1)
            fprintf(stderr, "\n");
    }
}
#endif

#ifdef FLOG
/*
 * Allocate the fbio buffers.
 */
static int
fbio_alloc(flog_bio_t *fbio)
{
    const int  align = MCD_OSD_BLK_SIZE;
    unsigned char *p = plat_alloc(MCD_OSD_BLK_SIZE + align - 1);

    if (!p) {
        mcd_log_msg(70103, PLAT_LOG_LEVEL_ERROR,
                    "Flush log sync: failed to allocate fbio");
        return 0;
    }

    fbio->buf  = p;
    fbio->abuf = (unsigned char *) (((uint64_t)(p) + align-1) & ~(align-1));
    return 1;
}


/*
 * Flush the fbio.
 */
static int
fbio_flush(flog_bio_t *fbio)
{
    if (!fbio->blkno || !fbio->dirty)
        return 1;

    mcd_rec_logpage_hdr_t *hdr = (mcd_rec_logpage_hdr_t *)fbio->abuf;
    hdr->eye_catcher = MCD_REC_LOGHDR_EYE_CATCHER;
    hdr->version     = MCD_REC_LOGHDR_VERSION;
    hdr->LSN         = fbio->lsn;
    hdr->checksum    = 0;
    hdr->checksum    = hash(fbio->abuf, Mcd_osd_blk_size, hdr->LSN);

    int s = mcd_fth_aio_blk_write(fbio->context, (char *)fbio->abuf,
                                  fbio->blkno * MCD_OSD_BLK_SIZE,
                                  MCD_OSD_BLK_SIZE);
    if (s != FLASH_EOK) {
        mcd_log_msg(70104, PLAT_LOG_LEVEL_ERROR,
                    "Flush log sync: write failed: blk=%ld err=%d",
                    fbio->blkno, s);
        return 0;
    }

    fbio->dirty = 0;
    return 1;
}


/*
 * Write a record using fbio.
 */
static int
fbio_write(flog_bio_t *fbio, flog_rec_t *frec)
{
    if (frec->shard_blk != fbio->blkno) {
        if (fbio->dirty)
            if (!fbio_flush(fbio))
                return 0;
        int s = mcd_fth_aio_blk_read(fbio->context, (char *)fbio->abuf,
                                     frec->shard_blk * MCD_OSD_BLK_SIZE,
                                     MCD_OSD_BLK_SIZE);
        if (s != FLASH_EOK) {
            mcd_log_msg(70105, PLAT_LOG_LEVEL_ERROR,
                        "Flush log sync: read failed: blk=%ld err=%d",
                        frec->shard_blk, s);
            return 0;
        }
        fbio->lsn   = 0;
        fbio->blkno = frec->shard_blk;
    }

    if (!fbio->lsn)
        fbio->lsn = frec->lsn;
    else if (fbio->lsn > frec->lsn)
        return 1;
    else if (fbio->lsn < frec->lsn)
        memset(fbio->abuf, 0, MCD_OSD_BLK_SIZE);

    *(mcd_logrec_object_t *)(&fbio->abuf[frec->shard_off]) = frec->logrec_obj;
    fbio->dirty = 1;
    return 1;
}


/*
 * Patch up the log pages of a shard.
 */
static void
flog_patchup(mcd_osd_shard_t *shard, void *context, FILE *fp)
{
    int           patched = 0;
    unsigned char *sector = NULL;
    flog_bio_t      fbio_ = {0};

    do {
        if (!fbio_alloc(&fbio_))
            break;

        sector = plat_alloc(FLUSH_LOG_SEC_SIZE);
        if (!sector) {
            mcd_log_msg(70106, PLAT_LOG_LEVEL_ERROR,
                        "Flush log sync: failed to allocate sector");
            break;
        }
        fbio_.context = context;

        flog_rec_t *frec = (flog_rec_t *)sector;
        while (fread(sector, FLUSH_LOG_SEC_SIZE, 1, fp) == 1) {
            if (frec->magic != FLUSH_LOG_MAGIC)
                continue;
            int s = fbio_write(&fbio_, frec);
            if (!s)
                break;
            patched++;
        }
        fbio_flush(&fbio_);
    } while (0);

    if (patched) {
        mcd_log_msg(70107, PLAT_LOG_LEVEL_INFO,
                    "Flush log sync: patched %d log records for shard %lu",
                    patched, shard->id);
    }

    if (sector)
        plat_free(sector);
    if (fbio_.buf)
        plat_free(fbio_.buf);
}


/*
 * Recover the flushed pages of a shard.
 */
static void
flog_recover(mcd_osd_shard_t *shard, void *context)
{
    char path[FLUSH_LOG_MAX_PATH];
    FILE            *fp = NULL;
    char          *fast = NULL;
    char *log_flush_dir = (char *) getProperty_String("LOG_FLUSH_DIR", NULL);

    if (log_flush_dir == NULL)
        return;

    snprintf(path, sizeof(path), "%s/%s%lu",
             log_flush_dir, FLUSH_LOG_PREFIX, shard->id);
    fp = fopen(path, "r");
    if (!fp)
        return;

    fast = plat_alloc(FLUSH_LOG_BUFFERED);
    if (fast)
        setbuffer(fp, fast, FLUSH_LOG_BUFFERED);

    flog_patchup(shard, context, fp);
    fclose(fp);
    if (fast)
        plat_free(fast);
}


/*
 * Set parameters relevant to flushing and syncing of logs.
 */
static void
flog_prepare(mcd_osd_shard_t *shard)
{
    char path[FLUSH_LOG_MAX_PATH];
    char *log_flush_dir = (char *)getProperty_String("LOG_FLUSH_DIR", NULL);

    if (log_flush_dir == NULL)
        return;

    snprintf(path, sizeof(path), "%s/%s%lu",
             log_flush_dir, FLUSH_LOG_PREFIX, shard->id);
    mcd_log_msg(70080, PLAT_LOG_LEVEL_INFO, "Flushing logs to %s", path);

    Sync_data = getProperty_Int("SYNC_DATA", SYNC_DATA);
    int flags = O_CREAT|O_TRUNC|O_WRONLY;
    if (Sync_data) {
        flags |= O_DIRECT;
        mcd_log_msg(70102, PLAT_LOG_LEVEL_INFO,
                    "Syncing logs (SYNC_DATA set)");
    }

    int fd = open(path, flags, FLUSH_LOG_FILE_MODE);
    if (fd < 0) {
        mcd_log_msg(70108, PLAT_LOG_LEVEL_ERROR,
                    "Flush log sync:"
                    " cannot open sync log file %s flags=%x error=%d",
                    path, flags, errno);
        return;
    }
    shard->flush_fd = fd;
}


/*
 * Initialise log flushing.
 */
static void
flog_init(mcd_osd_shard_t *shard, void *context)
{
    flog_recover(shard, context);
    flog_prepare(shard);
}
#endif /* FLOG */

void
shard_recovery_stats( mcd_osd_shard_t * shard, char ** ppos, int * lenp )
{
    if ( shard->persistent ) {
        plat_snprintfcat( ppos, lenp,
                          "STAT flash_rec_update_segments "
                          "%lu %lu %lu %lu %lu %lu %lu\r\n",
                          Mcd_rec_update_bufsize / MEGABYTE,
                          Mcd_rec_update_max_chunks,
                          Mcd_rec_update_yield,
                          shard->pshard->rec_table_blks,
                          shard->pshard->rec_table_pad,
                          Mcd_rec_update_segment_size / MEGABYTE,
                          Mcd_rec_free_upd_seg_curr );
        plat_snprintfcat( ppos, lenp,
                          "STAT flash_rec_log_consumed %lu %lu %lu %lu %lu\r\n",
                          (shard->log->write_buffer_seqno %
                           shard->log->total_slots) / MCD_REC_LOG_BLK_SLOTS,
                          shard->pshard->rec_log_blks,
                          shard->pshard->rec_log_pad,
                          Mcd_rec_log_segment_size / MEGABYTE,
                          Mcd_rec_free_log_seg_curr );
    }
}

int
shard_recover( mcd_osd_shard_t * shard )
{
    int                         b, c, s, rc;
    int                         c_seg;
    uint64_t                    checksum;
    uint64_t                    class_offset;
    char                      * buf;
    osd_state_t               * context =
        mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_RCVR );
    mcd_osd_slab_class_t      * class;
    mcd_rec_shard_t           * pshard = shard->pshard;
    mcd_rec_class_t           * pclass;
    mcd_rec_list_block_t      * seg_list;
    mcd_rec_ckpt_t            * ckpt;

    // This function recovers the shard metadata, which is all contained
    // in the first segment of the shard. There should be no danger of
    // I/O's crossing segment boundaries.

    mcd_log_msg( 20478, PLAT_LOG_LEVEL_INFO,
                 "Recovering persistent shard, id=%lu, blk_offset=%lu, "
                 "md_blks=%lu, table_blks=%lu, pad=%lu, log_blks=%lu, "
                 "pad=%lu (%lu entries)",
                 pshard->shard_id, pshard->blk_offset, pshard->rec_md_blks,
                 pshard->rec_table_blks, pshard->rec_table_pad,
                 pshard->rec_log_blks, pshard->rec_log_pad,
                 pshard->rec_log_blks * MCD_REC_LOG_BLK_RECS );

    // initialize log flushing
#ifdef FLOG
    flog_init(shard, context);
#endif /* FLOG */

    // get aligned buffer
    buf = (char *)( ( (uint64_t)context->osd_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

    // -----------------------------------------------------
    // Recover all persistent metadata
    // -----------------------------------------------------

    plat_assert_always( pshard != NULL );
    plat_assert_always( pshard->eye_catcher == MCD_REC_SHARD_EYE_CATCHER );
    plat_assert_always( pshard->version == MCD_REC_SHARD_VERSION );

    shard->blk_allocated = 0;

    // recover shard classes
    for ( c = 0; c < MCD_OSD_MAX_NCLASSES; c++ ) {

        class_offset = pshard->blk_offset + pshard->class_offset[ c ];

        // this read should not cross a segment boundary
        plat_assert( class_offset / Mcd_osd_segment_blks ==
                     (class_offset+pshard->map_blks) / Mcd_osd_segment_blks );

        // read class desc + segment table for each class
        rc = mcd_fth_aio_blk_read( context,
                                   buf,
                                   class_offset * Mcd_osd_blk_size,
                                   (1 + pshard->map_blks) * Mcd_osd_blk_size );
        if ( FLASH_EOK != rc ) {
            mcd_log_msg( 20479, PLAT_LOG_LEVEL_ERROR,
                         "failed to read class metadata, shardID=%lu, "
                         "blk_offset=%lu, blks=%d, rc=%d",
                         shard->id, class_offset, (1 + pshard->map_blks), rc );
            return rc;
        }

        pclass = (mcd_rec_class_t *)buf;

        // verify class descriptor checksum
        checksum         = pclass->checksum;
        pclass->checksum = 0;
        pclass->checksum = hash( (unsigned char *)pclass,
                                 Mcd_osd_blk_size,
                                 MCD_REC_CLASS_EYE_CATCHER );
        if ( pclass->checksum != checksum ||
             pclass->eye_catcher != MCD_REC_CLASS_EYE_CATCHER ||
             pclass->version != MCD_REC_CLASS_VERSION ) {
            mcd_log_msg( 20480, PLAT_LOG_LEVEL_FATAL,
                         "invalid class checksum, shardID=%lu, blk_offset=%lu",
                         shard->id, class_offset );
            pclass->checksum = checksum;   // restore original contents
            snap_dump( buf, Mcd_osd_blk_size );
            plat_abort();
        }

        class = &shard->slab_classes[ c ];
        c_seg = 0;

        bool list_end = false;

        // not all classes are initialized in volatile shard
        if ( class->slab_blksize != 0 ) {
            plat_assert_always( class->slab_blksize == pclass->slab_blksize );
        }

        // recover segment list for class
        for ( b = 0; b < pshard->map_blks; b++ ) {

            seg_list = (mcd_rec_list_block_t *)
                (buf + ((pclass->seg_list_offset + b) * Mcd_osd_blk_size));

            // verify class segment block checksum
            checksum           = seg_list->checksum;
            seg_list->checksum = 0;
            seg_list->checksum = hash( (unsigned char *)seg_list,
                                       Mcd_osd_blk_size,
                                       0 );
            if ( seg_list->checksum != checksum ) {
                mcd_log_msg( 20481, PLAT_LOG_LEVEL_FATAL,
                             "invalid class seglist checksum, shardID=%lu, "
                             "blk_offset=%lu", shard->id,
                             class_offset + pclass->seg_list_offset + b );
                seg_list->checksum = checksum;   // restore original contents
                snap_dump( (char *)seg_list, Mcd_osd_blk_size );
                plat_abort();
            }

            for ( s = 0; s < MCD_REC_LIST_ITEMS_PER_BLK && !list_end; s++ ) {

                // done when we hit a zero entry
                if ( seg_list->data[ s ] == 0 ) {
                    list_end = true;
                    break;
                }

                class->segments[ c_seg ] =
                    &shard->base_segments[ ~(seg_list->data[ s ]) /
                                             Mcd_osd_segment_blks ];

                // Note: must bitwise invert segment address
                class->segments[ c_seg ]->blk_offset = ~(seg_list->data[ s ]);
                class->segments[ c_seg ]->class      = class;

                // Note: bitmap rebuilt with hash table, initialize it here
                memset( class->segments[ c_seg ]->bitmap,
                        0,
                        class->slabs_per_segment / 8 );

                class->num_segments  += 1;
                class->total_slabs   += class->slabs_per_segment;

                shard->blk_allocated += Mcd_osd_segment_blks;
                shard->segment_table[ ~(seg_list->data[ s ]) /
                                      Mcd_osd_segment_blks ] =
                    class->segments[ c_seg ];

                mcd_log_msg( 40066, PLAT_LOG_LEVEL_TRACE,
                             "recovered class[%d], blksize=%d, segment[%d]: "
                             "blk_offset=%lu, seg_table[%lu]",
                             c, class->slab_blksize, c_seg,
                             class->segments[ c_seg ]->blk_offset,
                             ~(seg_list->data[ s ]) / Mcd_osd_segment_blks );

                c_seg++;
            }

            for ( /**/ ; s < MCD_REC_LIST_ITEMS_PER_BLK && list_end; s++ ) {
                plat_assert( seg_list->data[ s ] == 0 );
            }
        }
    }

    // -----------------------------------------------------
    // Retrieve the checkpoint record for this shard, and
    // find which log to apply (or which order for both)
    // -----------------------------------------------------

    // read in the ckpt record for this shard
    rc = mcd_fth_aio_blk_read( context,
                               buf,
                               (pshard->blk_offset + pshard->rec_md_blks - 1) *
                               Mcd_osd_blk_size,
                               Mcd_osd_blk_size );
    if ( FLASH_EOK != rc ) {
        mcd_log_msg( 20482, PLAT_LOG_LEVEL_ERROR,
                     "failed to read ckpt, shardID=%lu, blk_offset=%lu, rc=%d",
                     shard->id, pshard->blk_offset + pshard->rec_md_blks - 1,
                     rc );
        return rc;
    }

    ckpt = (mcd_rec_ckpt_t *)buf;

    // verify ckpt record checksum
    checksum       = ckpt->checksum;
    ckpt->checksum = 0;
    ckpt->checksum = hash( (unsigned char *)buf,
                           Mcd_osd_blk_size,
                           MCD_REC_CKPT_EYE_CATCHER );

    if ( ckpt->checksum != checksum ||
         ckpt->eye_catcher != MCD_REC_CKPT_EYE_CATCHER ||
         ckpt->version != MCD_REC_CKPT_VERSION ) {
        mcd_log_msg( 40061, PLAT_LOG_LEVEL_FATAL,
                     "invalid ckpt record, shardID=%lu, blk_offset=%lu",
                     shard->id, pshard->blk_offset + pshard->rec_md_blks - 1 );
        ckpt->checksum = checksum;   // restore original contents
        snap_dump( buf, Mcd_osd_blk_size );
        plat_abort();
    }

    // allocate ckpt record
    shard->ckpt = plat_alloc( sizeof( mcd_rec_ckpt_t ) );
    if ( shard->ckpt == NULL ) {
        mcd_log_msg( 20483, PLAT_LOG_LEVEL_ERROR,
                     "failed to alloc shard ckpt, shardID=%lu", shard->id );
        return FLASH_ENOMEM;
    }
    shard->ps_alloc += sizeof( mcd_rec_ckpt_t );

    memcpy( shard->ckpt, buf, sizeof( mcd_rec_ckpt_t ) );

#ifdef MCD_ENABLE_TOMBSTONES
    // init tombstoning
    rc = tombstone_init( shard );
    if ( rc != 0 ) {
        mcd_log_msg( 20484, PLAT_LOG_LEVEL_ERROR,
                     "failed to init tombstones, shardID=%lu", shard->id );
        return rc;
    }
#endif

    // pre-allocate log resources here
    rc = log_init( shard );
    if ( rc != 0 ) {
        mcd_log_msg( 20485, PLAT_LOG_LEVEL_ERROR,
                     "failed to pre-alloc log, shardID=%lu", shard->id );
        return rc;
    }

    // allow for easy property retrieval
    s = find_properties( shard->id );
    plat_assert( s >= 0 );
    plat_assert( s < MCD_OSD_MAX_NUM_SHARDS );
    shard->prop_slot = ~( s );

    // FIXME: hack alert
    if ( Mcd_rec_first_shard_recovered == 0 ) {
        shard_recover_phase2( shard );
        Mcd_rec_first_shard_recovered = 1;
    }

    if ( !Mcd_rec_chicken ) {
        // start updater thread for this shard
        fthResume( fthSpawn( &updater_thread, 40960 ), (uint64_t)shard );
    }

    return 0;
}


void
shard_recover_phase2( mcd_osd_shard_t * shard )
{
    int                         i, rc;
    uint64_t                    recovered_objs = 0;
    void                      * context =
        mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_RCVR );
    mcd_rec_update_t            update_mail;
    fthMbox_t                   updated_mbox;

    // This function recovers shard data; that is, it oversees reading
    // the persistent log(s) and udpating the object table. To start
    // this off, it reads the first page of each persistent log.

    mcd_log_msg( 20486, MCD_REC_LOG_LVL_DEBUG,
                 "Recovering persistent shard, id=%lu", shard->id );

    // -----------------------------------------------------
    // Replay the recovery log(s) to update on-disk object
    // table and rebuild the in-memory object table
    // -----------------------------------------------------

    // FIXME
    // This little hack allocates a container struct for the cmc shard,
    // which doesn't have one because it doesn't get opened the same way
    // as all the data shards. The reason for doing this is to make the
    // rest of the log code work without special-casing the cmc shard
    // everywhere.
    if ( shard->cntr == NULL ) {
        shard->cntr = &Mcd_osd_cmc_cntr;
        memset( shard->cntr, 0, sizeof( mcd_container_t ) );
        strcpy( shard->cntr->cname, "/sdf/CMC" );
        shard->cntr->state          = cntr_running;
        shard->cntr->shard          = shard;
        shard->cntr->eviction       = 0;
        shard->cntr->persistent     = 1;
    }

    // update the recovery object table in flash
    else {
        // initialize
        fthMboxInit( &updated_mbox );
        update_mail.log          = 0; // ignored during recovery
        update_mail.cntr         = shard->cntr;
        update_mail.in_recovery  = 1;
        update_mail.updated_sem  = NULL;
        update_mail.updated_mbox = &updated_mbox;

        // signal updater thread to apply logs to this shard
        if ( Mcd_rec_chicken ) {
            fthMboxPost( &Mcd_rec_updater_mbox, (uint64_t)(&update_mail) );
        } else {
            fthMboxPost( &shard->log->update_mbox, (uint64_t)(&update_mail) );
        }

        // wait for updater thread to finish
        recovered_objs = fthMboxWait( &updated_mbox );
    }

    // -----------------------------------------------------
    // Recovery is complete, initialize logging system
    // -----------------------------------------------------

    if ( Mcd_rec_chicken ) {
#ifndef MCD_REC_NOT_CHICKEN
        // Initialize container log buffer write semaphores
        for ( i = 0; i < MCD_LOGBUFS_PER_CNTR; i++ ) {
            fthSemInit( &shard->cntr->write_sem[i], 0 );
            shard->log->logbufs[i].write_sem = &(shard->cntr->write_sem[i]);
        }
#endif

        // initialize container pointers
        for ( i = 0; i < MCD_REC_NUM_LOGS; i++ ) {
            shard->log->update_mail[i].cntr = shard->cntr;
        }
    }

    // make the log available for writing
    rc = log_init_phase2( context, shard );
    if ( rc != 0 ) {
        mcd_log_msg( 20489, PLAT_LOG_LEVEL_FATAL,
                     "failed to init log, rc=%d", rc );
        plat_abort();
    }

    // recover cas id and add a "safe" value to it
    // don't persist the update here
    shard->cntr->cas_id += (MCD_OSD_CAS_UPDATE_INTERVAL *
                            shard->cntr->cas_num_nodes);

    // high sequence number recovered, now add "safe" value to it
    shard->sequence += (MCD_REC_LOGBUF_RECS * MCD_REC_NUM_LOGBUFS);

    mcd_log_msg( 20490, PLAT_LOG_LEVEL_INFO,
                 "Recovered persistent shard, id=%lu, objects=%lu, seqno=%lu, "
                 "cas_id=%lu, bytes allocated=%lu", shard->id, recovered_objs,
                 shard->sequence, shard->cntr->cas_id, shard->ps_alloc );

    return;
}

void
shard_set_properties_internal( mcd_container_t * cntr,
                               mcd_rec_properties_t * prop )
{
    plat_assert( cntr != NULL );
    plat_assert( prop != NULL );

    prop->flush_time    = cntr->flush_time;
    prop->cas_id        = cntr->cas_id;
    prop->size_quota    = cntr->size_quota;
    prop->obj_quota     = cntr->obj_quota;
    prop->state         = cntr->state;
    prop->tcp_port      = cntr->tcp_port;
    prop->udp_port      = cntr->udp_port;
    prop->eviction      = cntr->eviction;
    prop->persistent    = cntr->persistent;
    prop->container_id  = cntr->container_id;
    prop->sync_updates  = cntr->sync_updates;
    prop->sync_msec     = cntr->sync_msec;
    prop->num_ips       = cntr->num_ips;
    prop->sasl          = cntr->sasl;
    prop->prefix_delete = cntr->prefix_delete;
    prop->sync_backup   = cntr->sync_backup;
    memset( prop->cname, 0, sizeof( prop->cname ) );
    strcpy( prop->cname, cntr->cname );
    memset( prop->cluster_name, 0, sizeof( prop->cluster_name ) );
    strcpy( prop->cluster_name, cntr->cluster_name );

    for ( int i = 0; i < MCD_CNTR_MAX_NUM_IPS; i++ ) {
        prop->ip_addrs[ i ] = cntr->ip_addrs[ i ].s_addr;
    }
    plat_assert_always( sizeof( in_addr_t ) == sizeof( uint32_t ) );
}

int
shard_set_properties( mcd_osd_shard_t * shard, mcd_container_t * cntr )
{
    int                         rc;
    int                         slot;
    mcd_rec_properties_t        old_prop;
    mcd_rec_properties_t      * prop;
    mcd_rec_superblock_t      * sb = &Mcd_rec_superblock;

    mcd_log_msg( 20065, PLAT_LOG_LEVEL_DEBUG,
                 "ENTERING, shardID=%lu", shard->id );

    // find which slot holds the properties for this shard
    if ( shard->prop_slot == 0 ) {
        shard->prop_slot = ~( find_properties( shard->id ) );
    }
    slot = ~( shard->prop_slot );
    prop = sb->props[ slot ];

    // save old contents
    old_prop = *prop;

    // install new properties
    shard_set_properties_internal( cntr, prop );

    prop->write_version++;
    prop->checksum = 0;
    prop->checksum = hash( (unsigned char *)prop,
                           Mcd_osd_blk_size,
                           MCD_REC_PROP_EYE_CATCHER );

    mcd_log_msg( 20491, PLAT_LOG_LEVEL_DEBUG,
                 "writing properties for shardID=%lu, slot=%d",
                 shard->id, slot );

    // write shard properties
    rc = write_property( slot );
    if ( FLASH_EOK != rc ) {
        mcd_log_msg( 20492, PLAT_LOG_LEVEL_ERROR,
                     "failed to set shard props, rc=%d", rc );
        *prop = old_prop;
        return rc;
    }

    // sync all devices
    rc = mcd_aio_sync_devices();

    return rc;
}

int
shard_get_properties( int slot, mcd_container_t * cntr )
{
    mcd_rec_superblock_t      * sb = &Mcd_rec_superblock;
    mcd_rec_properties_t      * prop = sb->props[ slot ];

    mcd_log_msg( 20065, PLAT_LOG_LEVEL_DEBUG, "ENTERING, shardID=%lu",
                 prop->shard_id );

    // fill in container properties
    cntr->flush_time   = prop->flush_time;
    cntr->cas_id       = prop->cas_id;
    cntr->size_quota   = prop->size_quota;
    cntr->obj_quota    = prop->obj_quota;
    cntr->state        = prop->state;
    cntr->tcp_port     = prop->tcp_port;
    cntr->udp_port     = prop->udp_port;
    cntr->eviction     = prop->eviction;
    cntr->persistent   = prop->persistent;
    cntr->container_id = prop->container_id;
    cntr->sync_updates = prop->sync_updates;
    cntr->sync_msec    = prop->sync_msec;
    cntr->num_ips      = prop->num_ips;
    cntr->sasl         = prop->sasl;
    cntr->prefix_delete= prop->prefix_delete;
    cntr->sync_backup  = prop->sync_backup;
    strcpy( cntr->cname, prop->cname );
    strcpy( cntr->cluster_name, prop->cluster_name );

    for ( int i = 0; i < MCD_CNTR_MAX_NUM_IPS; i++ ) {
        cntr->ip_addrs[ i ].s_addr = prop->ip_addrs[ i ];
    }
    plat_assert_always( sizeof( in_addr_t ) == sizeof( uint32_t ) );

    return 0;
}

int
shard_set_state( mcd_osd_shard_t * shard, int new_state )
{
    int                         rc;
    int                         slot;
    int                         old_state;
    mcd_rec_properties_t      * prop;
    mcd_rec_superblock_t      * sb = &Mcd_rec_superblock;

    mcd_log_msg( 20065, PLAT_LOG_LEVEL_DEBUG,
                 "ENTERING, shardID=%lu", shard->id );

    // find which slot holds the properties for this shard
    if ( shard->prop_slot == 0 ) {
        shard->prop_slot = ~( find_properties( shard->id ) );
    }
    slot = ~( shard->prop_slot );
    prop = sb->props[ slot ];

    plat_assert( slot >= 0 );
    plat_assert( slot < MCD_OSD_MAX_NUM_SHARDS );

    old_state      = prop->state;
    prop->state    = new_state;
    prop->checksum = 0;
    prop->checksum = hash( (unsigned char *)prop,
                           Mcd_osd_blk_size,
                           MCD_REC_PROP_EYE_CATCHER );

    mcd_log_msg( 20493, PLAT_LOG_LEVEL_INFO,
                 "persisting new state '%s' for shardID=%lu, slot=%d",
                 (new_state == cntr_running ? "running" :
                  (new_state == cntr_stopping ? "stopping" : "stopped")),
                 shard->id, slot );

    // write shard properties
    rc = write_property( slot );
    if ( FLASH_EOK != rc ) {
        mcd_log_msg( 20492, PLAT_LOG_LEVEL_FATAL,
                     "failed to set shard props, rc=%d", rc );
        prop->state = old_state;
        return rc;
    }

    // sync all devices
    rc = mcd_aio_sync_devices();

    return rc;
}

void
shard_unrecover( mcd_osd_shard_t * shard )
{
    mcd_rec_log_t             * log = shard->log;

    mcd_log_msg( 20065, PLAT_LOG_LEVEL_DEBUG, "ENTERING, shardID=%lu",
                 shard->id );

    if ( !shard->pshard ) {
        mcd_log_msg( 20494, PLAT_LOG_LEVEL_DEBUG,
                     "no pshard for shard shardID=%lu", shard->id );
        return;
    }

    // free all persistent shard memory

    if ( shard->log ) {
        // kill the update thread for this shard
        if ( !Mcd_rec_chicken && log->updater_started ) {
            // send empty message to halt updater thread
            fthMboxPost( &log->update_mbox, 0 );
            fthMboxWait( &log->update_stop_mbox );
            plat_assert( log->updater_started == 0 );
        }

        // kill the log writer and wait for it to end
        if ( log->started ) {
            log->shutdown = 1;
            log_write_internal( shard, NULL );
            if ( Mcd_rec_chicken ) {
                // make sure the updater thread isn't working on this shard
                log_wait( shard );
            }
            plat_assert( log->started == 0 );
            if ( shard->refcount != 0 ) {
                mcd_log_msg( 20559, PLAT_LOG_LEVEL_WARN,
                             "shardID=%lu reference count is %u",
                             shard->id, shard->refcount );
            }
        }

        if ( log->pp_state.dealloc_list ) {
            plat_free( log->pp_state.dealloc_list );
            shard->ps_alloc -= ( sizeof( uint32_t ) *
                                 (log->pp_max_updates + MCD_REC_LOGBUF_RECS) );
        }

        if ( log->logbuf ) {
            plat_free( log->logbuf );
            shard->ps_alloc -= (MCD_REC_LOGBUF_SIZE * MCD_REC_NUM_LOGBUFS) +
                Mcd_osd_blk_size;
        }

        if ( log->segments ) {
            fthWaitEl_t * wait = fthLock( &Mcd_rec_log_segment_lock, 1, NULL );
            for ( int s = log->segment_count - 1; s >= 0; s-- ) {
                Mcd_rec_free_log_segments[ Mcd_rec_free_log_seg_curr++ ] =
                    log->segments[ s ];
            }
            mcd_log_msg( 160015, PLAT_LOG_LEVEL_INFO,
                         "deallocated %d log segments from shardID=%lu, "
                         "remaining=%lu",
                         log->segment_count, shard->id,
                         Mcd_rec_free_log_seg_curr );
            fthUnlock( wait );

            plat_free( log->segments );
            shard->ps_alloc   -= ( log->segment_count * sizeof( void * ) );
            log->segments      = NULL;
            log->segment_count = 0;

        shard->ps_alloc += ( log->segment_count * sizeof( void * ) );
        }

        plat_free( shard->log );
        shard->ps_alloc -= sizeof( mcd_rec_log_t );
    }

    if ( shard->ckpt ) {
        plat_free( shard->ckpt );
        shard->ps_alloc -= sizeof( mcd_rec_ckpt_t );
    }

    if ( shard->ts_list ) {
#ifdef MCD_ENABLE_TOMBSTONES
        if ( shard->ts_list->xtra_count > 0 ) {
            // free all "extra" tombstones
            shard->lcss = 0xffffffffffffffffull;
            tombstone_prune( shard );
        }

        if ( shard->ts_list->slab ) {
            plat_free( shard->ts_list->slab );
            shard->ps_alloc -= ( sizeof( mcd_rec_tombstone_t ) *
                                 (shard->pshard->rec_log_blks +
                                  shard->pshard->rec_log_pad) );
        }

        plat_free( shard->ts_list );
        shard->ps_alloc -= sizeof( mcd_rec_ts_list_t );
#else
        plat_assert( 0 == 1 );
#endif
    }

    plat_free( shard->pshard );
    shard->pshard = NULL;
    shard->ps_alloc -= sizeof( mcd_rec_shard_t );

    mcd_log_msg( 20495, PLAT_LOG_LEVEL_INFO,
                 "pshard deleted, allocated=%lu", shard->ps_alloc );

    return;
}

void
dump_hash_bucket( void * context, mcd_osd_shard_t * shard,
                  mcd_rec_flash_object_t * obj, uint64_t obj_offset,
                  uint64_t obj_blk_offset, mcd_osd_bucket_t * bucket,
                  mcd_osd_hash_t * bucket_head )
{
    int                         i, rc;
    uint64_t                    syn;
    uint64_t                    blk_offset = obj_blk_offset;
    uint64_t                    tmp_offset;
    mcd_osd_hash_t            * hash_entry;
    mcd_osd_meta_t            * meta;
    char                        data_buf[ 1024 ];
    char                      * buf;
    char                      * key;

    // get aligned buffer of one 512-byte block
    buf = (char *)( ( (uint64_t)data_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

    // show what we know of the offending table entry
    mcd_dbg_msg( PLAT_LOG_LEVEL_INFO,
                 "table_obj: syn=%u, ts=%u, del=%u, blocks=%u, "
                 "bucket=%u, seqno=%lu, boff=%lu, ooff=%lu",
                 obj->syndrome, obj->tombstone, obj->deleted,
                 mcd_osd_lba_to_blk( obj->blocks ),
                 obj->bucket, obj->seqno, blk_offset, obj_offset );

    // read object from disk and show information
    tmp_offset =
        shard->rand_table[blk_offset /
                          (Mcd_osd_segment_blks / Mcd_aio_num_files)] +
        (blk_offset % (Mcd_osd_segment_blks / Mcd_aio_num_files));
    rc = mcd_fth_aio_blk_read( context,
                               buf,
                               tmp_offset * Mcd_osd_blk_size,
                               Mcd_osd_blk_size );
    if ( FLASH_EOK != rc ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_FATAL, "I/O error, rc=%d", rc );
        plat_abort();
    }
    meta = (mcd_osd_meta_t *)buf;
    key  = (char *)(buf + sizeof( mcd_osd_meta_t ));
    syn  = hash( (unsigned char *)key, meta->key_len, 0 );
    mcd_dbg_msg( PLAT_LOG_LEVEL_INFO,
                 "disk_object: magic=0x%X, vers=%u, key_len=%u, "
                 "data_len=%d, b1chk=%u, ctime=%u, etime=%u, "
                 "seqno=%lu, chksum=%lu, syn=%lu, ssyn=%u",
                 meta->magic, meta->version, meta->key_len,
                 meta->data_len, meta->blk1_chksum, meta->create_time,
                 meta->expiry_time, meta->seqno, meta->checksum, syn,
                 (uint16_t)(syn >> 48) );

    // show all the accessible hash entries and disk objects
    mcd_dbg_msg( PLAT_LOG_LEVEL_INFO,
                 ">>>> hash entries <<<< bucket_size=%lu, next_item=%d, ",
                 Mcd_osd_bucket_size, bucket->next_item );

    hash_entry = bucket_head;
    for ( i = 0; i < bucket->next_item; i++, hash_entry++ ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_INFO,
                     "hash_entry[%d]: used=%d, ref=%u, del=%u, "
                     "blocks=%u, syn=%u, addr=%u",
                     i, hash_entry->used, hash_entry->referenced,
                     hash_entry->deleted, hash_entry->blocks,
                     hash_entry->syndrome, hash_entry->address );
        if ( context == NULL ) {
            continue;
        }
        blk_offset = hash_entry->address;
        tmp_offset =
            shard->rand_table[blk_offset /
                              (Mcd_osd_segment_blks / Mcd_aio_num_files)] +
            (blk_offset % (Mcd_osd_segment_blks / Mcd_aio_num_files));
        rc = mcd_fth_aio_blk_read( context,
                                   buf,
                                   tmp_offset * Mcd_osd_blk_size,
                                   Mcd_osd_blk_size );
        if ( FLASH_EOK != rc ) {
            mcd_dbg_msg( PLAT_LOG_LEVEL_FATAL, "I/O error, rc=%d", rc );
            plat_abort();
        }
        meta = (mcd_osd_meta_t *)buf;
        key  = (char *)(buf + sizeof( mcd_osd_meta_t ));
        syn  = hash( (unsigned char *)key, meta->key_len, 0 );
        mcd_dbg_msg( PLAT_LOG_LEVEL_INFO,
                     "object[%d]: magic=0x%X, vers=%u, key_len=%u, "
                     "data_len=%d, b1chk=%u, ctime=%u, etime=%u, "
                     "seqno=%lu, chksum=%lu, syn=%lu, ssyn=%u",
                     i, meta->magic, meta->version, meta->key_len,
                     meta->data_len, meta->blk1_chksum, meta->create_time,
                     meta->expiry_time, meta->seqno, meta->checksum, syn,
                     (uint16_t)(syn >> 48) );
    }

    // show all the inaccessible hash entries (if any) and disk objects
    if ( i < Mcd_osd_bucket_size ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_INFO,
                     ">>>> hash entries that are not accessible <<<<" );
    }
    for ( /* */; i < Mcd_osd_bucket_size; i++, hash_entry++ ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_INFO,
                     "hash_entry_NA[%d]: used=%d, ref=%d, del=%d, "
                     "blocks=%d, syn=%u, addr=%u",
                     i, hash_entry->used, hash_entry->referenced,
                     hash_entry->deleted, hash_entry->blocks,
                     hash_entry->syndrome, hash_entry->address );
        if ( context == NULL ) {
            continue;
        }
        blk_offset = hash_entry->address;
        tmp_offset =
            shard->rand_table[blk_offset /
                              (Mcd_osd_segment_blks / Mcd_aio_num_files)] +
            (blk_offset % (Mcd_osd_segment_blks / Mcd_aio_num_files));
        rc = mcd_fth_aio_blk_read( context,
                                   buf,
                                   tmp_offset * Mcd_osd_blk_size,
                                   Mcd_osd_blk_size );
        if ( FLASH_EOK != rc ) {
            mcd_dbg_msg( PLAT_LOG_LEVEL_FATAL, "I/O error, rc=%d", rc );
            plat_abort();
        }
        meta = (mcd_osd_meta_t *)buf;
        key  = (char *)(buf + sizeof( mcd_osd_meta_t ));
        syn  = hash( (unsigned char *)key, meta->key_len, 0 );
        mcd_dbg_msg( PLAT_LOG_LEVEL_INFO,
                     "object_NA[%d]: magic=0x%X, vers=%u, key_len=%u, "
                     "data_len=%d, b1chk=%u, ctime=%u, etime=%u, "
                     "seqno=%lu, chksum=%lu, syn=%lu, ssyn=%u",
                     i, meta->magic, meta->version, meta->key_len,
                     meta->data_len, meta->blk1_chksum, meta->create_time,
                     meta->expiry_time, meta->seqno, meta->checksum, syn,
                     (uint16_t)(syn >> 48) );
    }
}

void
update_hash_table( void * context, mcd_osd_shard_t * shard,
                   mcd_rec_obj_state_t * state, uint64_t * rec_objs,
                   mcd_rec_lru_scan_t * lru_scan )
{
    int                         j;
    int                         class_index;
    uint16_t                  * index;
    uint64_t                    obj_offset;
    uint64_t                    blk_offset;
    uint64_t                    map_offset;
    mcd_rec_flash_object_t    * obj;
    mcd_osd_hash_t            * hash_entry = NULL;
    mcd_osd_hash_t            * bucket_head;
    mcd_osd_bucket_t          * bucket;
    mcd_osd_segment_t         * segment;
    mcd_osd_slab_class_t      * class;

    mcd_log_msg( 20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

#ifdef MCD_ENABLE_TOMBSTONES
    // lcss may have advanced, try to prune old tombstones
    tombstone_prune( shard );
#endif

    // process all flash objects in range
    for ( obj_offset = 0; obj_offset < state->num_objs; /* No increment! */ ) {

        // calculate block offset in flash for this object
        blk_offset = state->start_obj + obj_offset;

        // get pointer to flash object entry in block
        obj = (mcd_rec_flash_object_t *)
            state->segments[ obj_offset / state->seg_objects ] +
            (obj_offset % state->seg_objects);

        plat_assert( obj_offset / state->seg_objects < state->seg_count );

#ifdef MCD_ENABLE_TOMBSTONES
        // recover tombstones
        if ( obj->tombstone ) {
            // no persisted tombstones in cache mode
            plat_assert( !shard->evict_to_free );

            // delete tombstone if lcss has advanced
            if ( obj->seqno < tombstone_get_lcss( shard ) ) {
                delete_object( obj );
                obj_offset++;
                continue;
            }
            tombstone_add( shard, obj->syndrome, blk_offset, obj->seqno );
            goto ts_housekeeping;
        }
#endif

        // skip over non-existent entries
        if ( obj->blocks == 0 ) {
            plat_assert_always( obj->syndrome == 0 );
            plat_assert_always( obj->bucket == 0 );
            obj_offset++;
            continue;
        }

        // find the right hash entry to use
        bucket = shard->hash_buckets +
            ( obj->bucket / Mcd_osd_bucket_size );
        bucket_head = shard->hash_table +
            ( obj->bucket & Mcd_osd_bucket_mask );

        if ( Mcd_osd_bucket_size > bucket->next_item ) {
            hash_entry = bucket_head + bucket->next_item++;
        }
        else {
            // hash bucket is full, try the overflow table
            if ( 0 == shard->evict_to_free ) {

                index = shard->overflow_index + Mcd_osd_overflow_depth
                    * ( obj->bucket / shard->lock_bktsize );
                hash_entry = shard->overflow_table + Mcd_osd_overflow_depth
                    * ( obj->bucket / shard->lock_bktsize );

                for ( j = 0; j < Mcd_osd_overflow_depth; j++,hash_entry++ ) {
                    if ( 0 == hash_entry->used ) {
                        bucket->overflowed = 1;
                        index[j] = ( bucket - shard->hash_buckets )
                            % shard->lock_bktsize;
                        break;
                    }
                }
                if ( Mcd_osd_overflow_depth > j ) {
                    shard->num_soft_overflows++;
                }
                else {
                    // hard overflow for store mode shard, should not happen
                    mcd_log_msg( 20496, PLAT_LOG_LEVEL_FATAL,
                                 "recovery overflow for store mode shard!" );
                    plat_abort();
                }
            }
            else {
                // soft overflow for cache mode shard, should not happen
                mcd_log_msg( 20497, PLAT_LOG_LEVEL_FATAL,
                             "recovery overflow for cache mode shard!" );
                dump_hash_bucket( context, shard, obj, obj_offset,
                                  blk_offset, bucket, bucket_head );
                plat_abort();
            }
        }
        plat_assert( hash_entry->used == 0 );

        // update hash table entry
        hash_entry->used       = 1;
        hash_entry->referenced = 1;
        hash_entry->deleted    = obj->deleted;
        hash_entry->blocks     = obj->blocks;
        hash_entry->syndrome   = obj->syndrome;
        hash_entry->address    = blk_offset;

        mcd_log_msg( 20498, MCD_REC_LOG_LVL_TRACE,
                     "<<<< upd_HT: syn=%u, blocks=%u, del=%u, bucket=%u, "
                     "addr=%lu",
                     obj->syndrome, mcd_osd_lba_to_blk( obj->blocks ),
                     obj->deleted, obj->bucket, blk_offset );

        plat_assert( blk_offset / Mcd_osd_segment_blks <
                     shard->total_segments );

        // update addr table entry
        shard->addr_table[ blk_offset ] = hash_entry - shard->hash_table;

        // housekeeping
        shard->blk_consumed  += mcd_osd_lba_to_blk( obj->blocks );
        shard->num_objects   += 1;
        shard->total_objects += 1;

#ifdef MCD_ENABLE_TOMBSTONES
    ts_housekeeping:
#endif

        // get class pointer, update used slab count
        class_index = shard->class_table[ mcd_osd_lba_to_blk( obj->blocks ) ];
        class = shard->slab_classes + class_index;
        plat_assert_always( class->used_slabs <= class->total_slabs );
        class->used_slabs += 1;

        // update bitmap for class segment
        segment = shard->segment_table[ blk_offset / Mcd_osd_segment_blks ];
        plat_assert( segment->class == class );

        map_offset = (blk_offset - segment->blk_offset) / class->slab_blksize;
        segment->bitmap[ map_offset / 64 ] |=
            Mcd_osd_bitmap_masks[ map_offset % 64 ];
        segment->alloc_map[ map_offset / 64 ] |=
            Mcd_osd_bitmap_masks[ map_offset % 64 ]; // used by full backup

        // save the oldest item (by seqno)
        if ( lru_scan[ class_index ].seqno == 0 ||
             obj->seqno < lru_scan[ class_index ].seqno ) {
            lru_scan[ class_index ].seqno      = obj->seqno;
            lru_scan[ class_index ].hash_entry = hash_entry;
        }

        // if this segment is the last segment allocated for this class
        // then keep track of the highest slab allocated
        if ( segment == class->segments[ class->num_segments - 1 ] ) {
            uint32_t temp;

            if ( class->slab_blksize * Mcd_osd_blk_size <=
                 Mcd_aio_strip_size ) {
                temp = ( ( map_offset %
                           ( Mcd_aio_strip_size /
                             ( class->slab_blksize * Mcd_osd_blk_size ) ) )
                         + 1 ) * Mcd_aio_num_files;
            }
            else {      /* FIXME_8MB */
                temp = map_offset + 1;
            }

            if ( temp > segment->next_slab ) {
                segment->next_slab = temp;
            }
            plat_assert_always( segment->next_slab <=
                                class->slabs_per_segment );
        } else {
            segment->next_slab = class->slabs_per_segment;
        }

        // save highest sequence number found
        if ( obj->seqno > shard->sequence ) {
            shard->sequence = obj->seqno;
        }

#ifdef MCD_ENABLE_TOMBSTONES
        if ( obj->tombstone ) {
            class->dealloc_pending     += 1;
            shard->blk_dealloc_pending += class->slab_blksize;
            obj_offset++;
            continue;
        }
#endif

        // increment count of recovered objects
        (*rec_objs)++;

        // increment to next possible object location
        obj_offset += power_of_two_roundup( mcd_osd_lba_to_blk(obj->blocks) );
    }

    return;
}

int
apply_log_record( mcd_osd_shard_t * shard, char * data,
                  mcd_rec_obj_state_t * state,
                  uint64_t * high_offset, uint64_t * low_offset )
{
    int                         applied = 0;
    uint64_t                    hwm_seqno = tombstone_get_lcss( shard );
    uint64_t                    obj_offset;
    mcd_rec_flash_object_t    * object;
    mcd_logrec_object_t       * rec = (mcd_logrec_object_t *)data;
    mcd_logrec_object_t       * orig_rec = rec;
    mcd_logrec_object_t         mod_rec;

    // skip dummy records
    if ( rec->syndrome == 0 && rec->blocks == 0 &&
         rec->bucket == 0 && rec->seqno == 0 &&
         rec->old_offset == 0 ) {
        if ( rec->blk_offset == 0xffffffffu &&   // special cas_id record
             shard->cntr->cas_id < rec->target_seqno ) {
            shard->cntr->cas_id = rec->target_seqno;
        }
        return 0;
    }

    // return log record address (record offset)
    if ( rec->blk_offset > *high_offset ) {
        *high_offset = rec->blk_offset;
    }
    if ( rec->blk_offset < *low_offset ) {
        *low_offset = rec->blk_offset;
    }
    if ( rec->old_offset != 0 ) {
        if ( ~(rec->old_offset) > *high_offset ) {
            *high_offset = ~(rec->old_offset);
        }
        if ( ~(rec->old_offset) < *low_offset ) {
            *low_offset = ~(rec->old_offset);
        }
    }

 reapply:

    // check that record applies to this table range
    if ( rec->blk_offset < state->start_obj ||
         rec->blk_offset > state->start_obj + state->num_objs - 1 ) {

        // special delete/create record has an "old_offset"
        if ( rec->old_offset != 0 ) {
            // copy original record
            mod_rec = *rec;
            rec     = &mod_rec;

            // make it into a delete record and reapply it
            rec->blk_offset = ~(rec->old_offset);
            rec->old_offset = 0;
            rec->blocks     = 0;
            goto reapply;
        }

        // record is out of this table range
        mcd_log_msg( 20499, PLAT_LOG_LEVEL_DEVEL,
                     "<<<< skipping offset=%u, start=%lu, end=%lu",
                     rec->blk_offset, state->start_obj,
                     state->start_obj + state->num_objs - 1 );
        return applied;
    }

    // calculate offset to the object, in the proper segment
    obj_offset  = rec->blk_offset - state->start_obj;
    object      = (mcd_rec_flash_object_t *)
        state->segments[ obj_offset / state->seg_objects ] +
        (obj_offset % state->seg_objects);
    plat_assert( obj_offset / state->seg_objects < state->seg_count );

    mcd_log_msg( 20500, MCD_REC_LOG_LVL_TRACE,
                 "<<<< apply_log_rec: syn=%u, blocks=%u, del=%u, "
                 "bucket=%u, boff=%u, ooff=%u, seq=%lu, tseq=%lu, "
                 "obj: syn=%u, ts=%u, blocks=%u, bucket=%u, toff=%lu, "
                 "seq=%lu",
                 rec->syndrome, mcd_osd_lba_to_blk( rec->blocks ),
                 rec->deleted, rec->bucket, rec->blk_offset,
                 (rec->old_offset == 0 ? 0 : ~(rec->old_offset)),
                 rec->seqno, rec->target_seqno,
                 object->syndrome, object->tombstone,
                 mcd_osd_lba_to_blk( object->blocks ),
                 object->bucket, obj_offset, object->seqno );

    if ( rec != orig_rec ) {
        mcd_log_msg( 20502, PLAT_LOG_LEVEL_TRACE,
                     "orig_rec: syn=%u, blocks=%u, del=%u, bucket=%u, "
                     "boff=%u, ooff=%u, seq=%lu, tseq=%lu",
                     orig_rec->syndrome,
                     mcd_osd_lba_to_blk( orig_rec->blocks ),
                     orig_rec->deleted, orig_rec->bucket,
                     orig_rec->blk_offset, orig_rec->old_offset,
                     orig_rec->seqno, orig_rec->target_seqno );
    }

    // garbage collect tombstone if needed
    if ( object->tombstone ) {
#ifdef MCD_ENABLE_TOMBSTONES
        // no persisted tombstones in cache mode
        plat_assert( shard->evict_to_free == 0 );

        // delete object if hwm seqno reached
        if ( object->seqno < hwm_seqno ) {
            delete_object( object );

        } else {
            mcd_log_msg( 20501, PLAT_LOG_LEVEL_FATAL,
                         "rec: syn=%u, blocks=%u, del=%u, bucket=%u, boff=%u, "
                         "ooff=%u, seq=%lu, tseq=%lu, obj: syn=%u, ts=%u, "
                         "blocks=%u, del=%u, bucket=%u, toff=%lu, seq=%lu, "
                         " hwm_seqno=%lu",
                         rec->syndrome, mcd_osd_lba_to_blk( rec->blocks ),
                         rec->deleted, rec->bucket,
                         rec->blk_offset, rec->old_offset, rec->seqno,
                         rec->target_seqno, object->syndrome,
                         object->tombstone,
                         mcd_osd_lba_to_blk( object->blocks ), object->deleted,
                         object->bucket, obj_offset, object->seqno, hwm_seqno);
            if ( rec != orig_rec ) {
                mcd_log_msg( 20502, PLAT_LOG_LEVEL_FATAL,
                             "orig_rec: syn=%u, blocks=%u, del=%u, bucket=%u, "
                             "boff=%u, ooff=%u, seq=%lu, tseq=%lu",
                             orig_rec->syndrome,
                             mcd_osd_lba_to_blk( orig_rec->blocks ),
                             orig_rec->deleted, orig_rec->bucket,
                             orig_rec->blk_offset, orig_rec->old_offset,
                             orig_rec->seqno, orig_rec->target_seqno );
            }
            plat_abort();
        }
#endif
    }

    // apply the record
    if ( rec->blocks == 0 ) {                  // delete record
        if ( !state->in_recovery ) {
            if ( ! (object->bucket / Mcd_osd_bucket_size ==
                    rec->bucket / Mcd_osd_bucket_size &&
                    object->syndrome == rec->syndrome &&
                    ( object->seqno == rec->target_seqno ||
                      shard->evict_to_free )) ) {
                mcd_log_msg( 20503, PLAT_LOG_LEVEL_FATAL,
                             "rec: syn=%u, blocks=%u, del=%u, bucket=%u, "
                             "boff=%u, ooff=%u, seq=%lu, tseq=%lu, obj: "
                             "syn=%u, ts=%u, blocks=%u, del=%u, bucket=%u, "
                             "toff=%lu, seq=%lu, hwm_seqno=%lu",
                             rec->syndrome, mcd_osd_lba_to_blk( rec->blocks ),
                             rec->deleted, rec->bucket, rec->blk_offset,
                             rec->old_offset, rec->seqno, rec->target_seqno,
                             object->syndrome, object->tombstone,
                             mcd_osd_lba_to_blk( object->blocks ),
                             object->deleted, object->bucket, obj_offset,
                             object->seqno, hwm_seqno );
                if ( rec != orig_rec ) {
                    mcd_log_msg( 20502, PLAT_LOG_LEVEL_FATAL,
                                 "orig_rec: syn=%u, blocks=%u, del=%u, "
                                 "bucket=%u, boff=%u, ooff=%u, seq=%lu, "
                                 "tseq=%lu",
                                 orig_rec->syndrome,
                                 mcd_osd_lba_to_blk( orig_rec->blocks ),
                                 orig_rec->deleted, orig_rec->bucket,
                                 orig_rec->blk_offset, orig_rec->old_offset,
                                 orig_rec->seqno, orig_rec->target_seqno );
                }
                plat_abort();
            }
        }

        // replicated shard has more cases
        if ( shard->replicated ) {
#ifdef MCD_ENABLE_TOMBSTONES
            // cache mode: don't persist tombstones in object table (delete)
            // store mode: delete object if hwm seqno reached
            if ( shard->evict_to_free || rec->seqno < hwm_seqno ) {
                delete_object( object );
            }
            // store mode: persist delete as tombstone (hwm seqno not reached)
            else {
                object->seqno     = rec->seqno;
                object->tombstone = 1;
                //object->blocks    = 0;  // leave blocks in place
            }
#endif
        }
        // non-replicated shard, delete the object
        else {
            delete_object( object );
        }
        applied++;

    } else {                                   // put record
        if ( !state->in_recovery ) {
            if ( ! (object->blocks == 0 &&
                    object->bucket == 0 &&
                    object->syndrome == 0 &&
                    object->tombstone == 0 &&
                    object->seqno == 0) ) {
                mcd_log_msg( 20503, PLAT_LOG_LEVEL_FATAL,
                             "rec: syn=%u, blocks=%u, del=%u, bucket=%u, "
                             "boff=%u, ooff=%u, seq=%lu, tseq=%lu, obj: "
                             "syn=%u, ts=%u, blocks=%u, del=%u, bucket=%u, "
                             "toff=%lu, seq=%lu, hwm_seqno=%lu",
                             rec->syndrome, mcd_osd_lba_to_blk( rec->blocks ),
                             rec->deleted, rec->bucket, rec->blk_offset,
                             rec->old_offset, rec->seqno, rec->target_seqno,
                             object->syndrome, object->tombstone,
                             mcd_osd_lba_to_blk( object->blocks ),
                             object->deleted, object->bucket,
                             obj_offset, object->seqno, hwm_seqno );
                if ( rec != orig_rec ) {
                    mcd_log_msg( 20502, PLAT_LOG_LEVEL_FATAL,
                                 "orig_rec: syn=%u, blocks=%u, del=%u, "
                                 "bucket=%u, boff=%u, ooff=%u, seq=%lu, "
                                 "tseq=%lu",
                                 orig_rec->syndrome,
                                 mcd_osd_lba_to_blk( orig_rec->blocks ),
                                 orig_rec->deleted, orig_rec->bucket,
                                 orig_rec->blk_offset, orig_rec->old_offset,
                                 orig_rec->seqno, orig_rec->target_seqno );
                }
                plat_abort();
            }
        }
        object->syndrome  = rec->syndrome;
        object->deleted   = rec->deleted;
        object->blocks    = rec->blocks;
        object->bucket    = rec->bucket;
        object->seqno	  = rec->seqno;
        object->tombstone = 0;
        applied++;

        // special delete/create record has an "old_offset"
        if ( rec->old_offset != 0 ) {
            // copy original record
            mod_rec = *rec;
            rec     = &mod_rec;

            // make it into a delete record and reapply it
            rec->blk_offset = ~(rec->old_offset);
            rec->old_offset = 0;
            rec->blocks     = 0;
            goto reapply;
        }
    }

    return applied;
}

int
read_log_segment( void * context, int segment, mcd_osd_shard_t * shard,
                  mcd_rec_log_state_t * log_state, char * buf )
{
    int                         rc;
    int                         blk_count;
    uint64_t                    io_blks = 0;
    uint64_t                    start_seg;
    uint64_t                    end_seg;
    uint64_t                    blk_offset = 0;
    uint64_t                    rel_offset;
    uint64_t                    num_blks;

    // calculate start block offset for this segment
    log_state->start_blk = segment * Mcd_rec_log_segment_blks;

    // calculate remaining blocks in segment (could be less on last segment)
    num_blks = (Mcd_rec_update_segment_blks >
                log_state->num_blks - log_state->start_blk
                ? log_state->num_blks - log_state->start_blk
                : Mcd_rec_update_segment_blks);

    mcd_log_msg( 40097, PLAT_LOG_LEVEL_DEBUG,
                 "Reading log segment %d: shardID=%lu, start=%lu, buf=%p",
                 segment, shard->id, log_state->start_blk, buf );

    // read one log segment
    for ( blk_count = 0; blk_count < num_blks; blk_count += io_blks ) {

        // compute number of blocks to read in a single I/O
        io_blks = MCD_REC_UPDATE_LGIOSIZE / Mcd_osd_blk_size;
        if ( io_blks > num_blks ) {
            io_blks = num_blks;
        }

        // calculate starting relative block in this log
        rel_offset = log_state->start_blk +
            relative_log_offset( shard->pshard, log_state->log );

        // calculate starting logical segment number
        start_seg = ( rel_offset / Mcd_osd_segment_blks );
        plat_assert_always( start_seg < shard->data_blk_offset /
                            Mcd_osd_segment_blks );

        // get logical block offset to starting block
        blk_offset = shard->segments[ start_seg ] +
            ( rel_offset % Mcd_osd_segment_blks );

        // sanity check
        end_seg = ( (rel_offset + io_blks - 1) /
                    Mcd_osd_segment_blks );
        plat_assert_always( end_seg < shard->data_blk_offset /
                            Mcd_osd_segment_blks );

        // make sure the I/O doesn't cross a segment boundary
        if ( start_seg != end_seg ) {
            io_blks = Mcd_osd_segment_blks -
                (blk_offset % Mcd_osd_segment_blks);
        }

        mcd_log_msg( 40098, PLAT_LOG_LEVEL_TRACE,
                     "reading log buffer blk_off=%lu, blks=%lu, buf=%p",
                     blk_offset, io_blks,
                     buf + (blk_count * Mcd_osd_blk_size) );

        (void) __sync_add_and_fetch( &shard->rec_log_reads, 1 );

        // do the I/O operation
        rc = mcd_fth_aio_blk_read( context,
                                   buf + (blk_count * Mcd_osd_blk_size),
                                   blk_offset * Mcd_osd_blk_size,
                                   io_blks * Mcd_osd_blk_size );
        if ( FLASH_EOK != rc ) {
            mcd_log_msg( 20506, PLAT_LOG_LEVEL_FATAL,
                         "failed to read log, shardID=%lu, "
                         "blk_offset=%lu, blks=%lu, rc=%d",
                         shard->id, blk_offset, io_blks, rc );
            plat_abort();
        }
    }

    // make sure we don't pick up left over garbage
    if ( blk_count < Mcd_rec_log_segment_blks ) {
        mcd_rec_logpage_hdr_t * hdr1 = (mcd_rec_logpage_hdr_t *)
            (buf + ((blk_count - 1) * Mcd_osd_blk_size));
        mcd_rec_logpage_hdr_t * hdr2 = (mcd_rec_logpage_hdr_t *)
            (buf + (blk_count * Mcd_osd_blk_size));
        mcd_log_msg( 40118, PLAT_LOG_LEVEL_DEBUG,
                     "Read less than a full log segment: "
                     "buf=%p, blk_count=%d, logblks=%lu, soff=%lu, io=%lu, "
                     "boff=%lu, LSN1=%lu, LSN2=%lu",
                     buf, blk_count, log_state->num_blks, log_state->start_blk,
                     io_blks, blk_offset, hdr1->LSN, hdr2->LSN );
        memset( buf + (blk_count * Mcd_osd_blk_size), 0, Mcd_osd_blk_size );
    }

    return blk_count;
}

void
verify_log_segment( void * context, int segment, mcd_osd_shard_t * shard,
                  mcd_rec_log_state_t * log_state )
{
    int                         rc;
    int                         blk_count;
    char                      * data_buf;
    char                      * buf;

    // allocate temporary buffer
    data_buf = plat_alloc( Mcd_rec_log_segment_size + Mcd_osd_blk_size );
    plat_assert( data_buf != NULL );

    // make buffer aligned
    buf = (char *)( ( (uint64_t)data_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

    mcd_dbg_msg( MCD_REC_LOG_LVL_DEBUG,
                 "Verifying log segment %d: shardID=%lu, start=%lu, "
                 "segbuf=%p, tmpbuf=%p",
                 segment, shard->id, log_state->start_blk,
                 log_state->segments[ segment ], buf );

    // read one log segment into temporary buffer
    blk_count = read_log_segment( context, segment, shard, log_state, buf );

    // now verify contents
    rc = memcmp( buf,
                 log_state->segments[ segment ],
                 blk_count * Mcd_osd_blk_size );
    if ( rc != 0 ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_FATAL,
                     "Verify log segment %d failed: shardID=%lu, "
                     "start=%lu, blks=%d, segbuf=%p, tmpbuf=%p",
                     segment, shard->id,
                     log_state->start_blk +
                     (segment * Mcd_rec_log_segment_blks),
                     blk_count, log_state->segments[ segment ], buf );

        // try to fine tune the error
        int offset = 0;
        for ( int r = 0; r < blk_count * MCD_REC_LOG_BLK_SLOTS; r++ ) {

            rc = memcmp( buf + offset,
                         log_state->segments[ segment ] + offset,
                         sizeof( mcd_logrec_object_t ) );
            if ( rc != 0 ) {
                mcd_dbg_msg( PLAT_LOG_LEVEL_FATAL,
                             "Verify log failed, shardID=%lu, seg=%d, "
                             "blk=%lu, rec=%lu, segbuf=%p, tmpbuf=%p",
                             shard->id, segment,
                             r / MCD_REC_LOG_BLK_SLOTS,
                             r % MCD_REC_LOG_BLK_SLOTS,
                             log_state->segments[ segment ] + offset,
                             buf + offset );
            }
            offset += sizeof( mcd_logrec_object_t );
        }

        // abort if specified
        if ( Mcd_rec_update_verify_log == 2 ) {
            plat_abort();
        }
    }

    plat_free( data_buf );

    return;
}

uint64_t
process_log( void * context, mcd_osd_shard_t * shard,
             mcd_rec_obj_state_t * state, mcd_rec_log_state_t * log_state )
{
    bool                        end_of_log = false;
    int                         s, p, r;
    char                      * buf;
    uint64_t                    blk_count = 0;
    uint64_t                    page_offset;
    uint64_t                    rec_offset;
    uint64_t                    prev_LSN = 0;
    uint64_t                    applied = 0;
    uint64_t                    checksum;
    mcd_rec_logpage_hdr_t     * page_hdr;

    // process the log for this chunk if we know that there
    // is an object in the log that falls in this table range
    if ( log_state->high_obj_offset < state->start_obj ||
         (log_state->low_obj_offset != 0xffffffffffffffffull &&
          log_state->low_obj_offset > state->start_obj + state->num_objs-1) ||
         (log_state->high_LSN > 0 &&
          log_state->high_LSN <= shard->ckpt->LSN) ) {
        mcd_log_msg( 40085, MCD_REC_LOG_LVL_DEBUG,
                     "Skip process log, shardID=%lu: pass %d of %d, log %d; "
                     "obj: start=%lu, end=%lu; highOff=%lu, lowOff=%lu, "
                     "highLSN=%lu, ckptLSN=%lu",
                     shard->id, state->pass, state->passes, log_state->log,
                     state->start_obj, state->start_obj + state->num_objs - 1,
                     log_state->high_obj_offset, log_state->low_obj_offset,
                     log_state->high_LSN, shard->ckpt->LSN );
        if ( ! Mcd_rec_update_verify ) {
            return 0;
        }
        mcd_dbg_msg( MCD_REC_LOG_LVL_DEBUG,
                     "Processing log anyway, shardID=%lu", shard->id );
    }

    mcd_log_msg( 40099, MCD_REC_LOG_LVL_DEBUG,
                 "Processing log, shardID=%lu, pass %d of %d, "
                 "log %d: start=%lu, blks=%lu, segments=%d, cached=%u; "
                 "obj: start=%lu, end=%lu; highOff=%lu, lowOff=%lu, "
                 "highLSN=%lu, ckptLSN=%lu",
                 shard->id, state->pass, state->passes, log_state->log,
                 log_state->start_blk, log_state->num_blks,
                 log_state->seg_count, log_state->seg_cached,
                 state->start_obj, state->start_obj + state->num_objs - 1,
                 log_state->high_obj_offset, log_state->low_obj_offset,
                 log_state->high_LSN, shard->ckpt->LSN );

    // read entire log, in segments
    for ( s = 0;
          s < log_state->seg_count && !end_of_log;
          s++, blk_count += Mcd_rec_log_segment_blks ) {

        // get log segment buffer
        buf = log_state->segments[ s ];

        // skip if already cached
        if ( s >= log_state->seg_cached ) {
            read_log_segment( context, s, shard, log_state, buf );
            log_state->seg_cached++;
        }
        else if ( Mcd_rec_update_verify_log ) {
            verify_log_segment( context, s, shard, log_state );
        }

        // process each log page (512-byte block) in segment
        for ( p = 0; p < Mcd_rec_log_segment_blks; p++ ) {

            page_offset = p * Mcd_osd_blk_size;
            page_hdr    = (mcd_rec_logpage_hdr_t *)( buf + page_offset );

            // yield every so often to avoid consuming too much CPU
            if ( Mcd_rec_update_yield > 0 &&
                 page_offset % (MEGABYTE / Mcd_rec_update_yield ) == 0 ) {
                fthYield( 0 );
            }

            // verify page header
            checksum = page_hdr->checksum;
            if ( checksum == 0 ) {
                plat_assert( page_hdr->LSN == 0 );
                plat_assert( page_hdr->eye_catcher == 0 );
                plat_assert( page_hdr->version == 0 );

            } else {
                // verify checksum
                page_hdr->checksum = 0;
                page_hdr->checksum = hash( (unsigned char *)(buf+page_offset),
                                           Mcd_osd_blk_size,
                                           page_hdr->LSN );
                if ( page_hdr->checksum != checksum ) {
                    mcd_log_msg( 40036, PLAT_LOG_LEVEL_FATAL,
                                 "Invalid log page checksum, shardID=%lu, "
                                 "found=%lu, calc=%lu, boff=%lu, poff=%d",
                                 shard->id, checksum, page_hdr->checksum,
                                 blk_count, p );
                    page_hdr->checksum = checksum; // restore original contents
                    snap_dump( (char *)page_hdr, Mcd_osd_blk_size );
                    plat_abort();
                }

                // verify header
                if ( page_hdr->eye_catcher != MCD_REC_LOGHDR_EYE_CATCHER ||
                     page_hdr->version != MCD_REC_LOGHDR_VERSION ) {
                    mcd_log_msg( 20508, PLAT_LOG_LEVEL_FATAL,
                                 "Invalid log page header, shardID=%lu, "
                                 "magic=0x%x, version=%d, boff=%lu, poff=%d",
                                 shard->id, page_hdr->eye_catcher,
                                 page_hdr->version, blk_count, p );
                    snap_dump( (char *)page_hdr, Mcd_osd_blk_size );
                    plat_abort();
                }
            }

            // end of log?
            if ( page_hdr->LSN < prev_LSN ) {
                mcd_log_msg( 40115, MCD_REC_LOG_LVL_DEBUG,
                             "LSN fell off, shardID=%lu, prevLSN=%lu, "
                             "pageLSN=%lu, ckptLSN=%lu, seg=%d, page=%d",
                             shard->id, prev_LSN, page_hdr->LSN,
                             shard->ckpt->LSN, s, p );
                end_of_log = true;
                break;
            }

            // LSN must advance by one
            else if ( page_hdr->LSN != prev_LSN + 1 && prev_LSN > 0 ) {
                mcd_log_msg( 40110, PLAT_LOG_LEVEL_FATAL,
                             "Unexpected LSN, shardID=%lu, LSN=%lu, "
                             "prev_LSN=%lu; seg=%d, page=%d",
                             shard->id, page_hdr->LSN, prev_LSN, s, p );
                plat_abort();
            }

            prev_LSN = page_hdr->LSN;

            // Note: the following condition is hit during the first log
            // read, before the high and low LSNs have been established.
            // If we change the log to start writing immediately after the
            // checkpoint page following a recovery, then we can't do this
            // (or the same check at the top of this function).

            // log records in this page already applied?
            if ( page_hdr->LSN <= shard->ckpt->LSN ) {
                mcd_log_msg( 40038, MCD_REC_LOG_LVL_DEBUG,
                             "Skipping log page, shardID=%lu, "
                             "boff=%lu, pageLSN=%lu, ckptLSN=%lu",
                             shard->id, blk_count + p,
                             page_hdr->LSN, shard->ckpt->LSN );
                end_of_log = true;
                break;
            }

            // apply each log record in this page to current metadata chunk
            // Note: log page header and log record sizes are the same!
            for ( r = 1; r < MCD_REC_LOG_BLK_SLOTS; r++ ) { // skip page hdr

                // get record offset
                rec_offset = page_offset + (r * sizeof( mcd_logrec_object_t ));

                // apply log record
                applied += apply_log_record( shard,
                                             (char *)(buf + rec_offset),
                                             state,
                                             &log_state->high_obj_offset,
                                             &log_state->low_obj_offset );
            }
        }
    }

    plat_assert_always( blk_count == log_state->num_blks || end_of_log );

    // return LSN from last valid page
    log_state->high_LSN = prev_LSN;

    mcd_log_msg( 40039, MCD_REC_LOG_LVL_DEBUG,
                 "processed shardID=%lu, pass %d of %d, log %u: "
                 "high=%lu, low=%lu; highLSN=%lu; applied=%lu",
                 shard->id, state->pass, state->passes, log_state->log,
                 log_state->high_obj_offset, log_state->low_obj_offset,
                 log_state->high_LSN, applied );

    return applied;
}

int
table_chunk_op( void * context, mcd_osd_shard_t * shard, int op,
                uint64_t start_blk, uint64_t num_blks, char * buf )
{
    int                         rc;
    uint64_t                    io_blks;
    uint64_t                    blk_count;
    uint64_t                    end_seg;
    uint64_t                    start_seg;
    uint64_t                    blk_offset;
    uint64_t                    rel_offset;

    mcd_log_msg( 20511, PLAT_LOG_LEVEL_TRACE,
                 "ENTERING, %s start=%lu, count=%lu",
                 (op == TABLE_READ ? "read" : "write"),
                 start_blk, num_blks );

    // iterate by number of blocks per I/O operation
    for ( blk_count = 0; blk_count < num_blks; blk_count += io_blks ) {

        // compute number of blocks to read in a single I/O
        io_blks = MCD_REC_UPDATE_IOSIZE / Mcd_osd_blk_size;
        if ( io_blks > num_blks - blk_count ) {
            io_blks = num_blks - blk_count;
        }

        // calculate starting logical segment number
        rel_offset = shard->pshard->rec_md_blks + start_blk + blk_count;
        start_seg  = rel_offset / Mcd_osd_segment_blks;
        plat_assert_always( start_seg <
                            shard->data_blk_offset / Mcd_osd_segment_blks );

        // get logical block offset to starting block
        blk_offset = shard->segments[ start_seg ] +
            ( rel_offset % Mcd_osd_segment_blks );

        // sanity check
        end_seg = (rel_offset + io_blks - 1) / Mcd_osd_segment_blks;
        plat_assert_always( end_seg <
                            shard->data_blk_offset / Mcd_osd_segment_blks );

        // make sure the I/O doesn't cross a segment boundary
        if ( start_seg != end_seg ) {
            io_blks = Mcd_osd_segment_blks -
                (blk_offset % Mcd_osd_segment_blks);
        }

        mcd_log_msg( 40100, MCD_REC_LOG_LVL_TRACE,
                     "%s blocks, start=%lu, count=%lu, buf=%p",
                     (op == TABLE_READ ? "reading" : "writing"),
                     blk_offset, io_blks,
                     buf + (blk_count * Mcd_osd_blk_size) );

        // do the I/O operation
        if ( op == TABLE_READ ) {
            (void) __sync_add_and_fetch( &shard->rec_table_reads, 1 );
            rc = mcd_fth_aio_blk_read( context,
                                       buf + (blk_count * Mcd_osd_blk_size),
                                       blk_offset * Mcd_osd_blk_size,
                                       io_blks * Mcd_osd_blk_size );
        } else {
            (void) __sync_add_and_fetch( &shard->rec_table_writes, 1 );
            rc = mcd_fth_aio_blk_write( context,
                                        buf + (blk_count * Mcd_osd_blk_size),
                                        blk_offset * Mcd_osd_blk_size,
                                        io_blks * Mcd_osd_blk_size );
        }

        if ( FLASH_EOK != rc ) {
            mcd_log_msg( 20512, PLAT_LOG_LEVEL_ERROR,
                         "failed to %s table, start=%lu, count=%lu, "
                         "blk_offset=%lu, count=%lu, rc=%d",
                         (op == TABLE_READ ? "read" : "write"),
                         start_blk, blk_count, blk_offset, io_blks, rc );
            return rc;
        }
    }

    return 0;
}

int
read_object_table( void * context, mcd_osd_shard_t * shard,
                   mcd_rec_obj_state_t * state,
                   mcd_rec_log_state_t * log_state )
{
    int                         s, rc;
    int                         seg_blks = Mcd_rec_update_segment_blks;
    int                         blk_count;
    uint64_t                    ts  = 0;
    uint64_t                    obj = 0;

    // calculate relative start block for this chunk
    state->start_blk = state->chunk * state->chunk_blks;

    // calculate remaining blocks in chunk (could be less on last chunk)
    state->chunk_blks = (state->seg_count * Mcd_rec_update_segment_blks >
                         state->num_blks - state->start_blk
                         ? state->num_blks - state->start_blk
                         : state->seg_count * Mcd_rec_update_segment_blks);

    // calculate relative start offset and count for objects in this chunk
    state->start_obj = state->start_blk * (Mcd_osd_blk_size /
                                           sizeof( mcd_rec_flash_object_t ));
    state->num_objs  = state->chunk_blks * (Mcd_osd_blk_size /
                                            sizeof( mcd_rec_flash_object_t ));

    if ( state->pass == 1 &&
         (log_state->high_obj_offset < state->start_obj ||
          (log_state->low_obj_offset != 0xffffffffffffffffull &&
           log_state->low_obj_offset > (state->start_obj +
                                        state->num_objs - 1))) ) {
        mcd_log_msg( 40040, MCD_REC_LOG_LVL_DEBUG,
                     "Skip table read, shardID=%lu: pass %d of %d, "
                     "chunk %d of %d; "
                     "blk: start=%lu, count=%d; "
                     "obj: start=%lu, count=%lu; "
                     "log: high=%lu, low=%lu",
                     shard->id, state->pass, state->passes,
                     state->chunk, state->num_chunks,
                     state->start_blk, state->chunk_blks,
                     state->start_obj, state->num_objs,
                     log_state->high_obj_offset, log_state->low_obj_offset );
        if ( ! Mcd_rec_update_verify ) {
            return 0;
        }
        mcd_dbg_msg( MCD_REC_LOG_LVL_DEBUG,
                     "Reading table anyway, shardID=%lu", shard->id );
    }

    mcd_log_msg( 40101, MCD_REC_LOG_LVL_DEBUG,
                 "Reading table, shardID=%lu: pass %d of %d, "
                 "chunk %d of %d; segments=%d;  "
                 "blk: start=%lu, count=%d; "
                 "obj: start=%lu, count=%lu, "
                 "log: high=%lu, low=%lu",
                 shard->id, state->pass, state->passes,
                 state->chunk, state->num_chunks, state->seg_count,
                 state->start_blk, state->chunk_blks,
                 state->start_obj, state->num_objs,
                 log_state->high_obj_offset, log_state->low_obj_offset );

    // read a chunk of the recovery object table, in segments
    for ( s = 0, blk_count = 0;
          s < state->seg_count && blk_count < state->chunk_blks;
          s++, blk_count += Mcd_rec_update_segment_blks ) {

        // if less than a full segment left, adjust number of blocks
        if ( Mcd_rec_update_segment_blks > state->chunk_blks - blk_count ) {
            seg_blks = state->chunk_blks - blk_count;
        }

        mcd_log_msg( 40116, PLAT_LOG_LEVEL_DEBUG,
                     "Reading segment %d: shardID=%lu, start=%lu, count=%d, "
                     "buf=%p",
                     s, shard->id,
                     state->start_blk + (s * Mcd_rec_update_segment_blks),
                     seg_blks,
                     state->segments[ s ] );

        rc = table_chunk_op( context,
                             shard,
                             TABLE_READ,
                             state->start_blk +
                             (s * Mcd_rec_update_segment_blks),
                             seg_blks,
                             state->segments[ s ] );
        plat_assert_rc( rc );

        if ( plat_log_enabled( PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY,
                               MCD_REC_LOG_LVL_DEBUG ) ) {
            obj += table_obj_count( state->segments[ s ],
                                    seg_blks * Mcd_osd_blk_size /
                                    sizeof( mcd_rec_flash_object_t ),
                                    &ts );
        }
    }

    if ( plat_log_enabled( PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY,
                           MCD_REC_LOG_LVL_DEBUG ) ) {
        mcd_log_msg( 20520, MCD_REC_LOG_LVL_DEBUG,
                     ">>>> Found %lu objects, %lu tombstones "
                     "in table chunk %d of %d, shardID=%lu",
                     obj, ts, state->chunk, state->num_chunks, shard->id );
#ifndef MCD_ENABLE_TOMBSTONES
        plat_assert( ts == 0 );
#endif
    }

    return 1;
}

void
write_object_table( void * context, mcd_osd_shard_t * shard,
                    mcd_rec_obj_state_t * state,
                    mcd_rec_log_state_t * log_state )
{
    int                         s, rc;
    int                         seg_blks = Mcd_rec_update_segment_blks;
    int                         blk_count;

    mcd_log_msg( 40103, MCD_REC_LOG_LVL_DEBUG,
                 "Writing table, shardID=%lu: pass %d of %d, "
                 "chunk %d of %d; segments=%d, "
                 "blk: start=%lu, count=%d; "
                 "obj: start=%lu, count=%lu, "
                 "log: high=%lu, low=%lu",
                 shard->id, state->pass, state->passes,
                 state->chunk, state->num_chunks, state->seg_count,
                 state->start_blk, state->chunk_blks,
                 state->start_obj, state->num_objs,
                 log_state->high_obj_offset, log_state->low_obj_offset );

    // write back this chunk of the recovery table, in segments
    for ( s = 0, blk_count = 0;
          s < state->seg_count && blk_count < state->chunk_blks;
          s++, blk_count += Mcd_rec_update_segment_blks ) {

        // if less than a full segment left, adjust number of blocks
        if ( Mcd_rec_update_segment_blks > state->chunk_blks - blk_count ) {
            seg_blks = state->chunk_blks - blk_count;
        }

        mcd_log_msg( 40117, PLAT_LOG_LEVEL_DEBUG,
                     "Writing segment %d: shardID=%lu, start=%lu, count=%d, "
                     "buf=%p",
                     s, shard->id,
                     state->start_blk + (s * Mcd_rec_update_segment_blks),
                     seg_blks,
                     state->segments[ s ] );

        rc = table_chunk_op( context,
                             shard,
                             TABLE_WRITE,
                             state->start_blk +
                             (s * Mcd_rec_update_segment_blks),
                             seg_blks,
                             state->segments[ s ] );
        plat_assert_rc( rc );
    }

    return;
}

void
verify_object_table( void * context, mcd_osd_shard_t * shard,
                     mcd_rec_obj_state_t * state,
                     mcd_rec_log_state_t * log_state, int verify_abort )

{
    bool                        dirty = false;
    int                         s, rc;
    int                         seg_blks = Mcd_rec_update_segment_blks;
    int                         blk_count;
    char                      * data_buf = NULL;
    char                      * buf;

    if ( ! Mcd_rec_update_verify ) {
        return;
    }

    mcd_dbg_msg( MCD_REC_LOG_LVL_DEBUG,
                 "Verifying table chunk, abort=%s, shardID=%lu: "
                 "pass %d of %d, chunk %d of %d; segments=%d, "
                 "blk: start=%lu, count=%d; "
                 "obj: start=%lu, count=%lu, "
                 "log: high=%lu, low=%lu",
                 verify_abort == VERIFY_ABORT_IF_CLEAN ? "clean" : "dirty",
                 shard->id, state->pass, state->passes,
                 state->chunk, state->num_chunks, state->seg_count,
                 state->start_blk, state->chunk_blks,
                 state->start_obj, state->num_objs,
                 log_state->high_obj_offset, log_state->low_obj_offset );

    // allocate temporary buffer
    data_buf = plat_alloc( Mcd_rec_update_segment_size + Mcd_osd_blk_size );
    plat_assert( data_buf != NULL );

    // make buffer aligned
    buf = (char *)( ( (uint64_t)data_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

    // verify this chunk of the recovery table, in segments;
    // must look at whole chunk result, not individual segment results
    for ( s = 0, blk_count = 0;
          s < state->seg_count && blk_count < state->chunk_blks;
          s++, blk_count += Mcd_rec_update_segment_blks ) {

        // if less than a full segment left, adjust number of blocks
        if ( Mcd_rec_update_segment_blks > state->chunk_blks - blk_count ) {
            seg_blks = state->chunk_blks - blk_count;
        }

        // read segment into temporary buffer
        rc = table_chunk_op( context,
                             shard,
                             TABLE_READ,
                             state->start_blk +
                             (s * Mcd_rec_update_segment_blks),
                             seg_blks,
                             buf );
        plat_assert_rc( rc );

        // compare segment from disk with segment from chunk
        rc = memcmp( buf, state->segments[ s ], seg_blks * Mcd_osd_blk_size );

        // keep dirty state if already dirty,
        // otherwise set dirty if compare fails
        dirty = ( dirty || rc != 0 );

        mcd_dbg_msg( PLAT_LOG_LEVEL_DEBUG,
                     "Verified table chunk %d segment %d %s: abort=%s, seg=%s,"
                     " shardID=%lu, start=%lu, blks=%d, segbuf=%p, tmpbuf=%p",
                     state->chunk, s, (dirty ? "DIRTY" : "CLEAN"),
                     (VERIFY_ABORT_IF_CLEAN ? "dirty" : "clean"),
                     (rc != 0 ? "DIRTY" : "CLEAN"), shard->id,
                     state->start_blk + (s * Mcd_rec_update_segment_blks),
                     seg_blks, state->segments[ s ], buf );
    }

    // try to fine tune the error if chunk is supposed to be clean
    if ( Mcd_rec_update_verify == 2 &&
         verify_abort == VERIFY_ABORT_IF_DIRTY &&
         dirty ) {
        for ( int i = 0;
              i < seg_blks * sizeof( mcd_rec_flash_object_t );
              i++ ) {

            uint64_t offset = i * sizeof( mcd_rec_flash_object_t );
            rc = memcmp( buf + offset,
                         state->segments[ s ] + offset,
                         sizeof( mcd_rec_flash_object_t ) );

            if ( rc != 0 ) {

                mcd_rec_flash_object_t * obj1 =
                    (mcd_rec_flash_object_t *)(state->segments[ s ] + offset);
                mcd_rec_flash_object_t * obj2 =
                    (mcd_rec_flash_object_t *)(buf + offset);

                mcd_dbg_msg( PLAT_LOG_LEVEL_FATAL,
                             "Verify object failed, segment %d: "
                             "shardID=%lu, rel_offset=%d; "
                             "seg obj: syn=%u, ts=%u, blocks=%u, "
                             "del=%u, bucket=%u, boff=%lu, seq=%lu; "
                             "disk obj: syn=%u, ts=%u, blocks=%u, "
                             "del=%u, bucket=%u, boff=%lu, seq=%lu",
                             s, shard->id, i,
                             obj1->syndrome, obj1->tombstone,
                             mcd_osd_lba_to_blk( obj1->blocks ),
                             obj1->deleted, obj1->bucket,
                             state->start_obj + i, obj1->seqno,
                             obj2->syndrome, obj2->tombstone,
                             mcd_osd_lba_to_blk( obj2->blocks ),
                             obj2->deleted, obj2->bucket,
                             state->start_obj + i, obj2->seqno );
                break;
            }
        }
    }

    // abort if not verified
    if ( ( Mcd_rec_update_verify == 2 ) &&
         ( (verify_abort == VERIFY_ABORT_IF_CLEAN && !dirty) ||
           (verify_abort == VERIFY_ABORT_IF_DIRTY && dirty) ) ) {
        mcd_dbg_msg( MCD_REC_LOG_LVL_DEBUG,
                     "Verified table chunk %s, abort=%s, shardID=%lu: "
                     "pass %d of %d, chunk %d of %d; segments=%d, "
                     "blk: start=%lu, count=%d; "
                     "obj: start=%lu, count=%lu, "
                     "log: high=%lu, low=%lu",
                     dirty ? "DIRTY" : "CLEAN",
                     verify_abort == VERIFY_ABORT_IF_CLEAN ? "CLEAN" : "DIRTY",
                     shard->id, state->pass, state->passes,
                     state->chunk, state->num_chunks, state->seg_count,
                     state->start_blk, state->chunk_blks,
                     state->start_obj, state->num_objs,
                     log_state->high_obj_offset, log_state->low_obj_offset );
        plat_abort();
    }

    plat_free( data_buf );

    return;
}

void
recovery_checkpoint( osd_state_t * context, mcd_osd_shard_t * shard,
                     uint64_t new_LSN )
{
    int                         rc;
    mcd_rec_shard_t           * pshard = shard->pshard;
    mcd_rec_ckpt_t            * ckpt = shard->ckpt;
    char                      * buf;

    // get aligned ckpt record buffer
    buf = (char *)( ( (uint64_t)context->osd_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

    // install new checkpoint LSN
    ckpt->LSN = new_LSN;

    mcd_log_msg( 20531, PLAT_LOG_LEVEL_DIAGNOSTIC,
                 ">>>> Updating ckptLSN=%lu, shardID=%lu",
                 ckpt->LSN, shard->id );

    // copy checkpoint record to buffer for writing
    memset( buf, 0, Mcd_osd_blk_size );
    memcpy( buf, ckpt, sizeof( mcd_rec_ckpt_t ) );

    // install new checksum
    ckpt           = (mcd_rec_ckpt_t *)buf;
    ckpt->checksum = 0;
    ckpt->checksum = hash( (unsigned char *)buf,
                           Mcd_osd_blk_size,
                           MCD_REC_CKPT_EYE_CATCHER );

    // write ckpt record with updated LSN
    rc = mcd_fth_aio_blk_write( context,
                                buf,
                                Mcd_osd_blk_size *
                                (pshard->blk_offset + pshard->rec_md_blks - 1),
                                Mcd_osd_blk_size );
    if ( FLASH_EOK != rc ) {
        mcd_log_msg( 20532, PLAT_LOG_LEVEL_FATAL,
                     "failed to write ckpt, shardID=%lu, offset=%lu, "
                     "rc=%d", shard->id, pshard->rec_md_blks - 1, rc );
        plat_abort();
    }

    // make sure these updates stick
    rc = mcd_aio_sync_devices();
    plat_assert_rc( rc );

    return;
}

void
attach_buffer_segments( mcd_osd_shard_t * shard, int in_recovery,
                        int * seg_count, char ** segments )
{
    int                         s;
    uint64_t                    obj_table_size;
    fthWaitEl_t               * wait;
    mcd_rec_shard_t           * pshard = shard->pshard;

    obj_table_size = pshard->rec_table_blks * Mcd_osd_blk_size;

    // last resort
    *seg_count = 1;

    // recovery is done one shard at a time; use full update buffer
    if ( in_recovery ) {
        *seg_count = (obj_table_size < Mcd_rec_update_bufsize
                      ? obj_table_size / Mcd_rec_update_segment_size
                      : Mcd_rec_update_bufsize / Mcd_rec_update_segment_size);
    }

    // online update, limit update buffer size
    else if ( obj_table_size / Mcd_rec_update_segment_size >
              Mcd_rec_update_max_chunks ) {
        *seg_count = (((obj_table_size / Mcd_rec_update_segment_size) +
                       Mcd_rec_update_max_chunks - 1) /
                      Mcd_rec_update_max_chunks);
    }

    if ((*seg_count) > (Mcd_rec_update_bufsize/Mcd_rec_update_segment_size)) {
        mcd_log_msg( 160019, PLAT_LOG_LEVEL_FATAL, 
            "Segment count (%d) exceeded number of recovery "
            "update buffer segments (%lu)!", 
            *seg_count, Mcd_rec_update_bufsize/Mcd_rec_update_segment_size);
        plat_abort();
    }

    mcd_log_msg( 40105, PLAT_LOG_LEVEL_DIAGNOSTIC,
                 "reserving %d update segments for shardID=%lu",
                 *seg_count, shard->id );

    // reserve segments; fthread blocks if not enough are available
    fthSemDown( &Mcd_rec_upd_seg_sem, *seg_count );

    // attach segments
    wait = fthLock( &Mcd_rec_upd_segment_lock, 1, NULL );
    for ( s = 0; s < *seg_count; s++ ) {
        segments[ s ] =
            Mcd_rec_free_upd_segments[ --Mcd_rec_free_upd_seg_curr ];
    }
    mcd_log_msg( 160016, PLAT_LOG_LEVEL_DIAGNOSTIC,
                 "attached %d update segments to shardID=%lu, remaining=%lu",
                 *seg_count, shard->id, Mcd_rec_free_upd_seg_curr );
    fthUnlock( wait );

    if ( Mcd_rec_attach_test_running ) {
        if ( *seg_count > 1 ) {
            fthMboxWait( &Mcd_rec_attach_test_mbox_special );
        } else {
            fthMboxWait( &Mcd_rec_attach_test_mbox );
        }
    }

    return;
}

void
detach_buffer_segments( mcd_osd_shard_t * shard, int seg_count,
                        char ** segments )
{
    int                         s;
    fthWaitEl_t               * wait;

    // detach segments
    wait = fthLock( &Mcd_rec_upd_segment_lock, 1, NULL );
    for ( s = seg_count - 1; s >= 0; s-- ) {
        Mcd_rec_free_upd_segments[ Mcd_rec_free_upd_seg_curr++ ] =
            segments[ s ];
    }
    mcd_log_msg( 160017, PLAT_LOG_LEVEL_DIAGNOSTIC,
                 "detached %d update segments from shardID=%lu, free=%lu",
                 seg_count, shard->id, Mcd_rec_free_upd_seg_curr );
    fthUnlock( wait );

    // return segments
    fthSemUp( &Mcd_rec_upd_seg_sem, seg_count );
    memset( segments, 0, seg_count * sizeof( char *) );

    return;
}

void
updater_thread( uint64_t arg )
{
    bool                        old_merged;
    int                         pct_complete;
    uint64_t                    thread_id;
    uint64_t                    recovered_objs;
    uint64_t                    buf_size;
    char                     ** buf_segments;
    mcd_osd_shard_t           * shard;
    mcd_rec_shard_t           * pshard;
    mcd_rec_ckpt_t            * ckpt;
    mcd_rec_log_t             * log;
    mcd_rec_update_t          * mail;
    osd_state_t               * context;
    mcd_rec_lru_scan_t          lru_scan[ MCD_OSD_MAX_NCLASSES ];
    mcd_rec_obj_state_t         state;
    mcd_rec_log_state_t         old_log_state;
    mcd_rec_log_state_t         curr_log_state;
    struct timeval              tv;

    mcd_log_msg( 20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    // get aio context and free unused buffer
    context = context_alloc( SSD_AIO_CTXT_MCD_REC_UPDT );
    if ( context->osd_buf != NULL ) {
        mcd_fth_osd_iobuf_free( context->osd_buf );
        context->osd_buf = NULL;
    }

    // recover shard pointer
    shard = (mcd_osd_shard_t *)arg;
    plat_assert_always( shard != NULL );

    thread_id = __sync_add_and_fetch( &Mcd_rec_updater_threads, 1 );
    mcd_log_msg( 40075, PLAT_LOG_LEVEL_DIAGNOSTIC,
                 "Updater thread %lu assigned to shardID=%lu",
                 thread_id, shard->id );

    // get array for object table recovery buffer pointers
    buf_size     = ( (Mcd_rec_update_bufsize / Mcd_rec_update_segment_size) *
                     sizeof( char * ) );
    buf_segments = plat_alloc( buf_size );
    if ( buf_segments == NULL ) {
        mcd_log_msg( 40108, PLAT_LOG_LEVEL_FATAL,
                     "failed to allocate %lu byte array for "
                     "object table recovery, shardID=%lu",
                     buf_size, shard->id );
        plat_abort();
    }
    memset( buf_segments, 0, buf_size );

    pshard = shard->pshard;
    ckpt   = shard->ckpt;
    log    = shard->log;

    plat_assert_always( pshard != NULL );
    plat_assert_always( ckpt != NULL );
    plat_assert_always( log != NULL );

    // updater thread holds a reference for its lifetime
    (void) __sync_add_and_fetch( &shard->refcount, 1 );

    // show that updater is running
    log->updater_started = 1;

    // -----------------------------------------------------
    // Main processing loop
    // -----------------------------------------------------

    while ( 1 ) {

        // wait on mailbox
        mail = (mcd_rec_update_t *)fthMboxWait( &log->update_mbox );

        // terminate thread if halting
        if ( mail == 0 ) {
            break;
        }

        // mail has out-of-band log, no-op
        if ( mail->log == -1 ) {
            mcd_log_msg( 20516, MCD_REC_LOG_LVL_DEBUG,
                         "Object table updater no-op for shardID=%lu",
                         shard->id );

            // signal this shard update is done
            if ( mail->updated_mbox != NULL ) {
                fthMboxPost( mail->updated_mbox, 0 );
            }
            continue;
        }

        plat_assert( mail->log >= 0 );
        plat_assert( mail->log < MCD_REC_NUM_LOGS );

        shard->rec_num_updates  += 1;
        shard->rec_upd_running   = 1;
        shard->rec_upd_prev_usec = shard->rec_upd_usec;
        fthGetTimeOfDay( &tv );
        shard->rec_upd_usec = (tv.tv_sec * PLAT_MILLION) + tv.tv_usec;
        shard->rec_log_reads_cum    += shard->rec_log_reads;
        shard->rec_table_reads_cum  += shard->rec_table_reads;
        shard->rec_table_writes_cum += shard->rec_table_writes;
        shard->rec_log_reads         = 0;
        shard->rec_table_reads       = 0;
        shard->rec_table_writes      = 0;

        mcd_log_msg( 20517, MCD_REC_LOG_LVL_DIAG,
                     ">>>> Object table updater running, shardID=%lu, "
                     "log=%d, in_rec=%d, update=%lu",
                     shard->id, mail->log, mail->in_recovery,
                     shard->rec_num_updates );

        memset( lru_scan, 0, sizeof( lru_scan ) );
        memset( &state, 0, sizeof( mcd_rec_obj_state_t ) );
        memset( &old_log_state, 0, sizeof( mcd_rec_log_state_t ) );
        memset( &curr_log_state, 0, sizeof( mcd_rec_log_state_t ) );

        // -----------------------------------------------------
        // Merge "old" log with the object table
        // -----------------------------------------------------

        old_merged     = false;
        recovered_objs = 0;
        pct_complete   = 0;

        state.in_recovery = mail->in_recovery;
        state.pass        = 1;
        state.passes      = state.in_recovery ? 2 : 1;
        state.start_blk   = 0;
        state.num_blks    = pshard->rec_table_blks;
        state.start_obj   = 0;
        state.num_objs    = 0;
        state.seg_objects = (Mcd_rec_update_segment_size /
                             sizeof( mcd_rec_flash_object_t ));
        state.seg_count   = 0;
        state.segments    = buf_segments;

        // attach object table buffer segments for this update/recovery
        attach_buffer_segments( shard, state.in_recovery,
                                &state.seg_count, state.segments );

        state.chunk       = 0;
        state.chunk_blks  = state.seg_count * Mcd_rec_update_segment_blks;
        state.num_chunks  =
            (state.num_blks + state.chunk_blks - 1) / state.chunk_blks;

        mcd_log_msg( 40072, PLAT_LOG_LEVEL_INFO,
                     "updater thread %lu allocated %lu byte buffer", thread_id,
                     (uint64_t)state.seg_count * Mcd_rec_update_segment_size );

        // FIXME
        context->osd_buf = state.segments[0];

        old_log_state.log             = mail->log;   // may be modified
        old_log_state.start_blk       = 0;
        old_log_state.num_blks        = pshard->rec_log_blks;
        old_log_state.high_LSN        = 0;
        old_log_state.high_obj_offset = 0;
        old_log_state.low_obj_offset  = 0xffffffffffffffffull;
        old_log_state.seg_cached      = 0;
        old_log_state.seg_count       = log->segment_count;
        old_log_state.segments        = log->segments;

        // when recovering, skip old log if already merged into object table
        if ( state.in_recovery ) {
            int         ckpt_log;
            uint64_t    ckpt_page;
            uint64_t    ckpt_page_LSN;
            uint64_t    next_LSN = 0;
            uint64_t    LSN[ 2 ] = { 0, 0 };

            // get LSN of page 0 from both logs
            LSN[ 0 ] = read_log_page( context, shard, 0, 0 );
            LSN[ 1 ] = read_log_page( context, shard, 1, 0 );

            old_log_state.log = 1;             // Assume log 1 is older

            // compare LSNs to find older log
            if ( LSN[ 0 ] < LSN[ 1 ] ) {
                old_log_state.log = 0;         // Log 0 is older
            }

            // no valid checkpoint (transient condition)
            if ( ckpt->LSN == 0 ) {
                ckpt_log      = old_log_state.log;
                ckpt_page     = 0;
                ckpt_page_LSN = 0;
                next_LSN      = 0;

                mcd_log_msg( 40087, PLAT_LOG_LEVEL_DEBUG,
                             "ckptLSN=%lu, next_LSN=%lu, log_blks=%lu, "
                             "LSN[0]=%lu, lastLSN[0]=%lu, "
                             "LSN[1]=%lu, lastLSN[1]=%lu",
                             ckpt->LSN, next_LSN, shard->pshard->rec_log_blks,
                             LSN[0],
                             read_log_page( context, shard, 0,
                                            shard->pshard->rec_log_blks - 1 ),
                             LSN[1],
                             read_log_page( context, shard, 1,
                                            shard->pshard->rec_log_blks - 1 ));

                // case 1: both logs empty, nothing to recover
                if ( LSN[ old_log_state.log ] == 0 &&
                     LSN[ 1 - old_log_state.log ] == 0 ) {
                    goto updater_reply;
                }
                // case 2: old log empty, new log not empty
                else if ( LSN[ old_log_state.log ] == 0 ) {
                    old_merged = true;  // new_merged = false;
                }
                // case 3: both logs not empty
                else {
                    old_merged = false; // new_merged = false;
                }
            }

            // checkpoint exists
            else {
                ckpt_log      = ( ((ckpt->LSN - 1) / pshard->rec_log_blks) %
                                  MCD_REC_NUM_LOGS );
                ckpt_page     = (ckpt->LSN - 1) % pshard->rec_log_blks;
                ckpt_page_LSN = read_log_page( context, shard,
                                               ckpt_log, ckpt_page );

                // Read LSN of next page beyond ckpt page (for sanity)
                if ( ckpt_page != pshard->rec_log_blks - 1 ) {
                    next_LSN = read_log_page( context, shard,
                                              ckpt_log, ckpt_page + 1 );
                } else {
                    next_LSN = read_log_page( context, shard,
                                              1 - ckpt_log, 0 );
                }

                mcd_log_msg( 40087, PLAT_LOG_LEVEL_DEBUG,
                             "ckptLSN=%lu, next_LSN=%lu, log_blks=%lu, "
                             "LSN[0]=%lu, lastLSN[0]=%lu, "
                             "LSN[1]=%lu, lastLSN[1]=%lu",
                             ckpt->LSN, next_LSN, shard->pshard->rec_log_blks,
                             LSN[0],
                             read_log_page( context, shard, 0,
                                            shard->pshard->rec_log_blks - 1 ),
                             LSN[1],
                             read_log_page( context, shard, 1,
                                            shard->pshard->rec_log_blks - 1 ));

                // case 5: ckpt in old log, new log not empty
                if ( ckpt_log == old_log_state.log ) {
                    old_merged = true;  // new_merged = false;
                }
                // case 4: ckpt in new log, no new log records since ckpt
                else if ( LSN[ ckpt_log ] <= ckpt->LSN ) {
                    old_merged = true;  // new_merged = true;
                }
                // case 6: ckpt in new log, new log records since ckpt
                else {
                    old_merged = false; // new_merged = false;
                }
            }

            mcd_log_msg( 40088, PLAT_LOG_LEVEL_INFO,
                         "Old log %d merge %s, shardID=%lu, "
                         "ckpt_LSN=%lu, log_pages=%lu, ckpt_log=%d, "
                         "ckpt_page=%lu, ckpt_page_LSN=%lu",
                         old_log_state.log,
                         old_merged ? "not needed" : "required",
                         shard->id, ckpt->LSN, pshard->rec_log_blks,
                         ckpt_log, ckpt_page, ckpt_page_LSN );
        }

        mcd_log_msg( 40089, PLAT_LOG_LEVEL_INFO,
                     "%s object table, shardID=%lu, pass %d of %d",
                     state.in_recovery ? "Recovering" : "Merging",
                     shard->id, state.pass, state.passes );

        // read the object table
        for ( state.chunk = 0;
              state.chunk < state.num_chunks && !old_merged;
              state.chunk++ ) {

            if ( state.num_chunks < 10 ||
                 state.chunk % ((state.num_chunks + 9) / 10) == 0 ) {
                if ( state.chunk > 0 ) {
                    mcd_log_msg( 40090, PLAT_LOG_LEVEL_INFO,
                                 "%s object table, shardID=%lu, "
                                 "pass %d of %d: %d percent complete",
                                 state.in_recovery ? "Recovering" : "Merging",
                                 shard->id, state.pass, state.passes,
                                 pct_complete );
                }
                pct_complete += 10;
            }

            // read part of the object table
            if ( read_object_table( context, shard, &state,
                                    &old_log_state ) ) {

                // apply log records from the specified log to this section
                if ( process_log( context, shard, &state, &old_log_state ) ) {

                    // log records applied, verify table was updated
                    verify_object_table( context, shard, &state,
                                         &old_log_state,
                                         VERIFY_ABORT_IF_CLEAN );

                    // write this chunk of the object table
                    write_object_table( context, shard, &state,
                                        &old_log_state );

                } else {

                    // verify table is still the same
                    verify_object_table( context, shard, &state,
                                         &old_log_state,
                                         VERIFY_ABORT_IF_DIRTY );

                    // no log records applied to this chunk
                    mcd_log_msg( 40047, MCD_REC_LOG_LVL_DEBUG,
                                 "Skip table write, shardID=%lu, "
                                 "pass %d of %d, chunk %d of %d,",
                                 shard->id, state.pass, state.passes,
                                 state.chunk, state.num_chunks );
                }

            } else {

                // no updates beyond this point in object table
                if ( old_log_state.high_obj_offset < state.start_obj ) {
                    mcd_log_msg( 40048, MCD_REC_LOG_LVL_DEBUG,
                                 "Table update complete, shardID=%lu, "
                                 "pass %d of %d, chunk %d of %d",
                                 shard->id, state.pass, state.passes,
                                 state.chunk, state.num_chunks );
                    break;
                }

                // log updates start at higher table offset, keep going
            }
        }

        mcd_log_msg( 40091, PLAT_LOG_LEVEL_INFO,
                     "%s object table, shardID=%lu, "
                     "pass %d of %d: complete, highLSN=%lu, ckptLSN=%lu",
                     state.in_recovery ? "Recovering" : "Merging",
                     shard->id, state.pass, state.passes,
                     curr_log_state.high_LSN, ckpt->LSN );

        // set checkpoint LSN to highest LSN found
        if ( old_log_state.high_LSN > ckpt->LSN ) {
            recovery_checkpoint( context, shard, old_log_state.high_LSN );
        }

        // -----------------------------------------------------
        // Merge "current" log when in recovery
        // -----------------------------------------------------

        if ( state.in_recovery ) {

            pct_complete = 0;

            state.pass = 2;

            curr_log_state.log             = 1 - old_log_state.log;
            curr_log_state.start_blk       = 0;
            curr_log_state.num_blks        = pshard->rec_log_blks;
            curr_log_state.high_LSN        = 0;
            curr_log_state.high_obj_offset = 0;
            curr_log_state.low_obj_offset  = 0xffffffffffffffffull;
            curr_log_state.seg_cached      = 0;
            curr_log_state.seg_count       = log->segment_count;
            curr_log_state.segments        = log->segments;

            mcd_log_msg( 40045, PLAT_LOG_LEVEL_INFO,
                         "Recovering object table, shardID=%lu, pass %d of %d",
                         shard->id, state.pass, state.passes );

            // read the object table
            for ( state.chunk = 0;
                  state.chunk < state.num_chunks;
                  state.chunk++ ) {

                if ( state.num_chunks < 10 ||
                     state.chunk % ((state.num_chunks + 9) / 10) == 0 ) {
                    if ( state.chunk > 0 ) {
                        mcd_log_msg( 40081, PLAT_LOG_LEVEL_INFO,
                                     "Recovering object table, shardID=%lu, "
                                     "pass %d of %d: %d percent complete",
                                     shard->id, state.pass, state.passes,
                                     pct_complete );
                    }
                    pct_complete += 10;
                }

                // read part of the object table
                if ( read_object_table( context, shard, &state,
                                        &curr_log_state ) ) {

                    // apply log records from current log
                    if ( process_log( context, shard, &state,
                                      &curr_log_state ) ) {

                        // log records applied, verify table was updated
                        verify_object_table( context, shard, &state,
                                             &old_log_state,
                                             VERIFY_ABORT_IF_CLEAN );

                        // update in-memory hash table with up-to-date metadata
                        update_hash_table( context, shard, &state,
                                           &recovered_objs, lru_scan );

                        // write this chunk of the object table
                        write_object_table( context, shard, &state,
                                            &curr_log_state );

                    } else {

                        // verify table is still the same
                        verify_object_table( context, shard, &state,
                                             &curr_log_state,
                                             VERIFY_ABORT_IF_DIRTY );

                        // update in-memory hash table with up-to-date metadata
                        update_hash_table( context, shard, &state,
                                           &recovered_objs, lru_scan );

                        // no log records applied to this chunk
                        mcd_log_msg( 40049, MCD_REC_LOG_LVL_DEBUG,
                                     "Skip table write,shardID=%lu, "
                                     "pass %d of %d, chunk %d of %d",
                                     shard->id, state.pass, state.passes,
                                     state.chunk, state.num_chunks );
                    }

                } else {
                    mcd_log_msg( 40050, PLAT_LOG_LEVEL_FATAL,
                                 "Skipped object table read, shardID=%lu, "
                                 "pass %d of %d; chunk %d of %d",
                                 shard->id, state.pass, state.passes,
                                 state.chunk, state.num_chunks );
                    plat_abort();
                }

                mcd_log_msg( 40051, MCD_REC_LOG_LVL_DEBUG,
                             "Recovered shardID=%lu, chunk %d of %d, "
                             "obj=%lu, seq=%lu",
                             shard->id, state.chunk, state.num_chunks,
                             recovered_objs, shard->sequence );
            }

            mcd_log_msg( 40083, PLAT_LOG_LEVEL_INFO,
                         "Recovering object table, shardID=%lu, "
                         "pass %d of %d: complete, highLSN=%lu, ckptLSN=%lu",
                         shard->id, state.pass, state.passes,
                         curr_log_state.high_LSN, ckpt->LSN );

            // set checkpoint LSN to highest LSN found
            if ( curr_log_state.high_LSN > ckpt->LSN ) {
                plat_assert( curr_log_state.high_LSN >=
                             old_log_state.high_LSN );
                recovery_checkpoint( context, shard, curr_log_state.high_LSN );
            }
        }

        // -----------------------------------------------------
        // Object table update complete
        // -----------------------------------------------------

        // Update per-class eviction clock hands after recovery
        if ( mail->in_recovery ) {
            int                         i, j;
            uint32_t                    hand;
            uint32_t                    slabs_per_pth;
            mcd_osd_hash_t            * hash_entry;
            mcd_osd_slab_class_t      * class;

            for ( i = 0; i < MCD_OSD_MAX_NCLASSES; i++ ) {

                hash_entry = lru_scan[i].hash_entry;
                if ( NULL == hash_entry ) {
                    continue;
                }
                class = &shard->slab_classes[i];
                slabs_per_pth = class->slabs_per_segment / (flash_settings.num_sdf_threads*flash_settings.num_cores);

                for ( j = 0; j < class->num_segments; j++ ) {
                    if ( class->segments[j]->blk_offset <= hash_entry->address
                         && class->segments[j]->blk_offset +
                         Mcd_osd_segment_blks > hash_entry->address ) {
                        break;
                    }
                }

                if ( 1 >= slabs_per_pth ) {
                    hand = j * class->slabs_per_segment +
                        ( hash_entry->address % Mcd_osd_segment_blks ) /
                        class->slab_blksize + 1;
                    class->clock_hand[0] = hand;
                }
                else {
                    hand = j * slabs_per_pth +
                        ( ( hash_entry->address % Mcd_osd_segment_blks ) /
                          class->slab_blksize ) % slabs_per_pth + 1;
                    for ( int n = 0; n < MCD_OSD_MAX_PTHREADS; n++ ) {
                        class->clock_hand[n] = hand;
                    }
                }
            }
        }

        updater_reply:

        // -----------------------------------------------------
        // Reply to requestor
        // -----------------------------------------------------

        // signal this shard update is done
        if ( mail->updated_mbox != NULL ) {
            fthMboxPost( mail->updated_mbox, recovered_objs );
        }

        // make persistent log available for writing
        if ( mail->updated_sem != NULL ) {
            fthSemUp( mail->updated_sem, 1 );
        }

        fthGetTimeOfDay( &tv );
        shard->rec_upd_usec    = ( (tv.tv_sec * PLAT_MILLION) + tv.tv_usec -
                                   shard->rec_upd_usec );
        shard->rec_upd_running = 0;

        mcd_log_msg( 20533, MCD_REC_LOG_LVL_DIAG,
                     ">>>> Updated shard_id=%lu, ckptLSN=%lu, old_log=%d, "
                     "in_recovery=%d, recovered_objs=%lu, high_seqno=%lu",
                     shard->id, ckpt->LSN, mail->log, mail->in_recovery,
                     recovered_objs, shard->sequence );

        // let go of the segmented object table buffer
        detach_buffer_segments( shard, state.seg_count, state.segments );
        context->osd_buf = NULL;
    }

    mcd_log_msg( 40076, PLAT_LOG_LEVEL_DIAGNOSTIC,
                 "Updater thread %lu halting, shardID=%lu",
                 thread_id, shard->id );

    (void) __sync_sub_and_fetch( &Mcd_rec_updater_threads, 1 );

    // show that updater has ended
    log->updater_started = 0;

    // acknowledge halt signal
    fthMboxPost( &log->update_stop_mbox, 0 );

    // decrement refcount
    (void) __sync_sub_and_fetch( &shard->refcount, 1 );

    // return segment array and aio context
    plat_free( buf_segments );
    context_free( context );

    return;
}

// ----------------------------------------------------------------
//
//   Chicken switch code
//
// ----------------------------------------------------------------

#ifndef MCD_REC_NOT_CHICKEN
inline void
update_hash_table_chicken( mcd_osd_shard_t * shard, uint64_t start_offset,
                           uint64_t num_objs, char * buffer,
                           uint64_t * rec_objs, mcd_rec_lru_scan_t * lru_scan )
{
    mcd_rec_obj_state_t         state;

    memset( &state, 0, sizeof( mcd_rec_obj_state_t ) );
    state.start_obj   = start_offset;
    state.num_objs    = num_objs;
    state.seg_objects = ( Mcd_rec_update_segment_size /
                          sizeof( mcd_rec_flash_object_t ) );
    state.seg_count   = 1;
    state.segments    = &buffer;

    update_hash_table( NULL, shard, &state, rec_objs, lru_scan );

    return;
}

inline int
apply_log_record_chicken( mcd_osd_shard_t * shard, char * data,
                          uint64_t start_offset, uint64_t num_objs,
                          char * obj_table, int in_recovery,
                          uint64_t * high_offset )
{
    uint64_t                    low_offset = 0xffffffffffffffffull;
    mcd_rec_obj_state_t         state;

    memset( &state, 0, sizeof( mcd_rec_obj_state_t ) );
    state.start_obj   = start_offset;
    state.num_objs    = num_objs;
    state.in_recovery = in_recovery;
    state.seg_objects = ( Mcd_rec_update_segment_size /
                          sizeof( mcd_rec_flash_object_t ) );
    state.seg_count   = 1;
    state.segments    = &obj_table;

    return apply_log_record( shard, data, &state, high_offset, &low_offset );
}

int
process_log_chicken( void * context, mcd_osd_shard_t * shard, int num_segs,
                     uint64_t log_offset, uint64_t start_obj, uint64_t num_obj,
                     char * obj_table, int in_recovery, uint64_t * last_LSN,
                     uint64_t * high_offset )
{
    bool                          end_of_log = false;
    int                           log_applied = 0;
    int                           p, r, rc;
    char                        * data_buf = NULL;
    char                        * buf;
    uint64_t                      buf_size = MCD_REC_UPDATE_LGIOSIZE;
    uint64_t                      buf_blks = buf_size / Mcd_osd_blk_size;
    uint64_t                      log_blks = shard->pshard->rec_log_blks;
    uint64_t                      obj_offset = 0;
    uint64_t                      buf_offset;
    uint64_t                      page_offset;
    uint64_t                      rec_offset;
    uint64_t                      start_seg;
    uint64_t                      end_seg;
    uint64_t                      blk_offset;
    uint64_t                      prev_LSN = 0;
    uint64_t                      checksum;
    mcd_rec_logpage_hdr_t       * page_hdr;

    mcd_log_msg( 20504, PLAT_LOG_LEVEL_DEBUG, "ENTERING, log_offset=%lu, "
                 "log_blks=%lu, start_obj=%lu, num_objs=%lu, last_LSN=%lu, "
                 "high_offset=%lu, in_rec=%d",
                 log_offset, log_blks, start_obj, num_obj, *last_LSN,
                 *high_offset, in_recovery );

    // Note: all I/Os performed to the log should fall within
    // the same segment; a segment boundary may fall between
    // two I/Os. This is because:
    // (1) the logs are aligned to an MCD_REC_ALIGN_BOUNDARY
    // (2) all I/Os are done from MCD_REC_UPDATE_LGIOSIZE buffers,
    //     and are started on MCD_REC_UPDATE_LGIOSIZE boundaries
    // (3) the size of the I/O buffer, MCD_REC_UPDATE_LGIOSIZE,
    //     must be a power of 2 and <= MCD_REC_ALIGN_BOUNDARY

    // get aligned buffer
    data_buf = ((osd_state_t *)context)->osd_buf;
    buf      = (char *)( ( (uint64_t)data_buf + Mcd_osd_blk_size - 1 ) &
                         Mcd_osd_blk_mask );

    // read buffers of log data
    for ( buf_offset = 0;
          buf_offset < log_blks && !end_of_log;
          buf_offset += buf_blks ) {

        if ( buf_blks > log_blks - buf_offset ) {
            buf_blks = log_blks - buf_offset;
        }

        start_seg = ( log_offset + buf_offset ) / Mcd_osd_segment_blks;
        plat_assert_always( start_seg < num_segs );

        blk_offset = shard->segments[ start_seg ] +
            ( ( log_offset + buf_offset ) % Mcd_osd_segment_blks );

        // sanity check
        end_seg = (log_offset + buf_offset + buf_blks - 1) /
            Mcd_osd_segment_blks;
        plat_assert_always( end_seg < num_segs );
        plat_assert_always( start_seg == end_seg );

        mcd_log_msg( 20505, PLAT_LOG_LEVEL_DEVEL,
                     "reading log buffer blk_off=%lu, blks=%lu",
                     blk_offset, buf_blks );

        (void) __sync_add_and_fetch( &shard->rec_log_reads, 1 );

        // read a chunk of the log
        rc = mcd_fth_aio_blk_read( context,
                                   buf,
                                   blk_offset * Mcd_osd_blk_size,
                                   buf_blks * Mcd_osd_blk_size );
        if ( FLASH_EOK != rc ) {
            mcd_log_msg( 20506, PLAT_LOG_LEVEL_FATAL,
                         "failed to read log, shardID=%lu, blk_offset=%lu, "
                         "blks=%lu, rc=%d",
                         shard->id, blk_offset, buf_blks, rc );
            plat_abort();
        }

        // process each log page (512-byte block) in buffer
        for ( p = 0; p < buf_blks; p++ ) {

            page_offset = p * Mcd_osd_blk_size;
            page_hdr    = (mcd_rec_logpage_hdr_t *)( buf + page_offset );

            // verify page header
            checksum = page_hdr->checksum;
            if ( checksum == 0 ) {
                plat_assert( page_hdr->LSN == 0 );
                plat_assert( page_hdr->eye_catcher == 0 );
                plat_assert( page_hdr->version == 0 );

            } else {
                // verify checksum
                page_hdr->checksum = 0;
                page_hdr->checksum = hash( (unsigned char *)(buf+page_offset),
                                           Mcd_osd_blk_size,
                                           page_hdr->LSN );
                if ( page_hdr->checksum != checksum ) {
                    mcd_log_msg( 20507, PLAT_LOG_LEVEL_FATAL,
                                 "Invalid log page checksum, "
                                 "found=%lu, calc=%lu, boff=%lu, poff=%d",
                                 checksum, page_hdr->checksum, blk_offset, p );
                    plat_abort();
                }

                // verify header
                if ( page_hdr->eye_catcher != MCD_REC_LOGHDR_EYE_CATCHER ||
                     page_hdr->version != MCD_REC_LOGHDR_VERSION ) {
                    mcd_log_msg( 20508, PLAT_LOG_LEVEL_FATAL,
                                 "Invalid log page header, shardID=%lu, "
                                 "magic=0x%x, version=%d, boff=%lu, poff=%d",
                                 shard->id, page_hdr->eye_catcher,
                                 page_hdr->version, blk_offset, p );
                    plat_abort();
                }
            }

            // end of log?
            if ( page_hdr->LSN < prev_LSN ) {
                mcd_log_msg( 20509, MCD_REC_LOG_LVL_DEBUG, "LSN fell off, "
                             "prevLSN=%lu, pageLSN=%lu, lastLSN=%lu",
                             prev_LSN, page_hdr->LSN, *last_LSN );
                end_of_log = true;
                break;
            }

            prev_LSN = page_hdr->LSN;

            // log records in this page already applied?
            if ( page_hdr->LSN <= (*last_LSN) ) {
                end_of_log = true;
                mcd_log_msg( 20510, MCD_REC_LOG_LVL_DEBUG,
                             "Skipping log, offset=%lu, prevLSN=%lu, "
                             "lastLSN=%lu", log_offset, prev_LSN, *last_LSN );
                break;
            }

            //plat_assert( page_hdr->LSN == 1 + (*last_LSN) + buf_offset + p );

            // apply each log record in this page to current metadata chunk
            // Note: log page header and log record sizes are the same!
            for ( r = 1; r < MCD_REC_LOG_BLK_SLOTS; r++ ) { // skip page hdr

                rec_offset = page_offset + (r * sizeof( mcd_logrec_object_t ));

                // apply log record
                log_applied +=
                    apply_log_record_chicken( shard,
                                      (char *)(buf + rec_offset),
                                      start_obj,
                                      num_obj,
                                      obj_table,
                                      in_recovery,
                                      &obj_offset );

                // return highest offset into object table that was found
                if ( obj_offset > *high_offset ) {
                    *high_offset = obj_offset;
                }
            }
        }
    }

    // return high LSN
    if ( *last_LSN < prev_LSN ) {
        *last_LSN = prev_LSN;
    }

    return log_applied;
}

void
updater_thread_chicken( uint64_t arg )
{
    int                         rc;
    int                         chunk;
    int                         num_chunks;
    int                         num_md_segs;
    int                         obj_per_blk;
    int                         old_applied;
    int                         curr_applied;
    uint64_t                    num_objs;
    uint64_t                    num_blks;
    uint64_t                    blks_left;
    uint64_t                    recovered_objs;
    uint64_t                    buf_size = Mcd_rec_update_bufsize;
    uint64_t                    buf_blks;
    uint64_t                    start_obj;
    uint64_t                    start_blk;
    uint64_t                    high_offset;
    uint64_t                    log_offset;
    uint64_t                    high_LSN = 0;
    char                      * data_buf = NULL;
    char                      * buf;
    char                      * ckpt_buf;
    mcd_rec_ckpt_t            * ckpt;
    mcd_container_t           * cntr = NULL;
    mcd_osd_shard_t           * shard;
    mcd_rec_shard_t           * pshard;
    mcd_rec_update_t          * mail;
    osd_state_t               * context =
        mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_UPDT );
    mcd_rec_lru_scan_t          lru_scan[ MCD_OSD_MAX_NCLASSES ];
    char                        ckpt_buffer[ 2 * MCD_OSD_BLK_SIZE ];
    struct timeval              tv;

    mcd_log_msg( 20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    // get aligned ckpt record buffer
    ckpt_buf = (char *)( ( (uint64_t)ckpt_buffer + Mcd_osd_blk_size - 1 ) &
                         Mcd_osd_blk_mask );

    // get a very large aligned buffer up front and keep it
    data_buf = plat_alloc_large( buf_size + Mcd_osd_blk_size );
    if ( data_buf == NULL ) {
        mcd_log_msg( 20514, PLAT_LOG_LEVEL_FATAL, "can't allocate %luM buffer",
                     buf_size / MEGABYTE );
        plat_abort();
    }
    mcd_log_msg( 20515, PLAT_LOG_LEVEL_INFO,
                 "allocated %lu byte buffer", buf_size );

    buf = (char *)( ( (uint64_t)data_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

    obj_per_blk = Mcd_osd_blk_size / sizeof( mcd_rec_flash_object_t );
    buf_blks    = buf_size / Mcd_osd_blk_size;

    while ( 1 ) {

        // wait on mailbox
        mail = (mcd_rec_update_t *)fthMboxWait( &Mcd_rec_updater_mbox );

        // terminate thread if halting
        if ( mail == 0 ) {
            break;
        }

        // get container pointer and increment reference count
        cntr = mail->cntr;
        (void) __sync_fetch_and_add( &cntr->shard_refcount, 1 );

        // mail has out-of-band log, no-op
        if ( mail->log == -1 ) {
            mcd_log_msg( 20516, MCD_REC_LOG_LVL_DEBUG,
                         "Object table updater no-op for shardID=%lu",
                         ((mcd_osd_shard_t *)cntr->shard)->id );

            // decrement refrence count
            (void) __sync_fetch_and_sub( &cntr->shard_refcount, 1 );

            // signal this shard update is done
            if ( mail->updated_mbox != NULL ) {
                fthMboxPost( mail->updated_mbox, 0 );
            }
            continue;
        }

        plat_assert( mail->log >= 0 );
        plat_assert( mail->log < MCD_REC_NUM_LOGS );

        // recover pointers
        shard  = cntr->shard;
        pshard = shard->pshard;
        ckpt   = shard->ckpt;

        shard->rec_num_updates  += 1;
        shard->rec_upd_running   = 1;
        shard->rec_upd_prev_usec = shard->rec_upd_usec;
        fthGetTimeOfDay( &tv );
        shard->rec_upd_usec = (tv.tv_sec * PLAT_MILLION) + tv.tv_usec;
        shard->rec_log_reads_cum    += shard->rec_log_reads;
        shard->rec_table_reads_cum  += shard->rec_table_reads;
        shard->rec_table_writes_cum += shard->rec_table_writes;
        shard->rec_log_reads         = 0;
        shard->rec_table_reads       = 0;
        shard->rec_table_writes      = 0;
        mcd_log_msg( 20517, MCD_REC_LOG_LVL_DIAG,
                     ">>>> Object table updater running, shardID=%lu, "
                     "log=%d, in_rec=%d, update=%lu",
                     shard->id, mail->log, mail->in_recovery,
                     shard->rec_num_updates );

        memset( lru_scan, 0, sizeof( lru_scan ) );

        // -----------------------------------------------------
        // read the recovery object table in sections (chunks)
        // of MCD_REC_UPDATE_BUFSIZE bytes, and apply log records
        // to each section (make multiple passes through the log).
        // -----------------------------------------------------

        blks_left      = pshard->rec_table_blks;
        num_chunks     = ( blks_left + buf_blks - 1 ) / buf_blks;
        num_md_segs    = ( (pshard->total_blks / Mcd_osd_segment_blks) -
                           shard->total_segments );
        high_offset    = 0;
        recovered_objs = 0;

        // process each chunk of the recovery table
        for ( chunk = 0; chunk < num_chunks; chunk++ ) {

            // initialize
            old_applied  = 0;
            curr_applied = 0;

            // calculate relative block offsets for this chunk
            start_blk  = chunk * buf_blks;
            num_blks   = (blks_left > buf_blks ? buf_blks : blks_left);
            blks_left -= num_blks;

            // calculate relative start offset for objects in this chunk
            start_obj = start_blk * obj_per_blk;
            num_objs  = num_blks * obj_per_blk;

            if ( !mail->in_recovery && high_offset < start_obj ) {
                mcd_log_msg( 20518, MCD_REC_LOG_LVL_DEBUG,
                             ">>>> Stopping table update, shardID=%lu, "
                             "start=%lu, high=%lu",
                             shard->id, start_obj, high_offset );
                break;
            }

            mcd_log_msg( 20519, MCD_REC_LOG_LVL_DEBUG,
                         ">>>> Reading table chunk %d of %d, shardID=%lu, "
                         "startBlk=%lu, count=%lu (startObj=%lu, count=%lu, "
                         "highOff=%lu)",
                         chunk, num_chunks, shard->id, start_blk, num_blks,
                         start_obj, num_objs, high_offset );

            // read a chunk of the recovery object table
            rc = table_chunk_op( context,
                                 shard,
                                 TABLE_READ,
                                 start_blk,
                                 num_blks,
                                 buf );
            plat_assert_rc( rc );

            if ( plat_log_enabled( PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY,
                                   MCD_REC_LOG_LVL_DEBUG ) ) {
                uint64_t ts  = 0;
                uint64_t obj = table_obj_count( buf, num_objs, &ts );
                mcd_log_msg( 20520, MCD_REC_LOG_LVL_DEBUG,
                             ">>>> Found %lu objects, %lu tombstones "
                             "in table chunk %d of %d, shardID=%lu",
                             obj, ts, chunk, num_chunks, shard->id );
#ifndef MCD_ENABLE_TOMBSTONES
	plat_assert( ts == 0 );
#endif
            }

            // process the log for this chunk if we know that there
            // is an object in the log that falls in this table range
            if ( high_offset < start_obj ) {

                mcd_log_msg( 20521, MCD_REC_LOG_LVL_DEBUG,
                             ">>>> Skip process old log %u, shardID=%lu, "
                             "start=%lu, highOff=%lu, end=%lu, ckptLSN=%lu",
                             mail->log, shard->id, start_obj, high_offset,
                             start_obj + num_objs - 1, ckpt->LSN );
            } else {

                mcd_log_msg( 20522, MCD_REC_LOG_LVL_DEBUG,
                             ">>>> Process old log %u, shardID=%lu, "
                             "start=%lu, highOff=%lu, end=%lu, ckptLSN=%lu",
                             mail->log, shard->id, start_obj, high_offset,
                             start_obj + num_objs - 1, ckpt->LSN );

                // find offset for the log specified
                log_offset = relative_log_offset( pshard, mail->log );

                // reset highest LSN found on each iteration
                high_LSN = ckpt->LSN;

                // apply log records from the specified log to this section
                old_applied = process_log_chicken( context,
                                           shard,
                                           num_md_segs,
                                           log_offset,    // specified log
                                           start_obj,
                                           num_objs,
                                           buf,
                                           mail->in_recovery,
                                           &high_LSN,
                                           &high_offset );

                mcd_log_msg( 20523, MCD_REC_LOG_LVL_DEBUG,
                             ">>>> End process old log %u, shardID=%lu, "
                             "start=%lu, highOff=%lu, end=%lu, ckptLSN=%lu, "
                             "highLSN=%lu, applied=%d",
                             mail->log, shard->id, start_obj, high_offset,
                             start_obj + num_objs - 1, ckpt->LSN, high_LSN,
                             old_applied );
            }

            // when in recovery
            if ( mail->in_recovery ) {

                // process the log for this chunk if we know that there
                // is an object in the log that falls in this table range
                if ( high_offset < start_obj ) {

                    mcd_log_msg( 20524, MCD_REC_LOG_LVL_DEBUG,
                                 ">>>> Skip process curr log %u, shardID=%lu, "
                                 "start=%lu, highOff=%lu, end=%lu, "
                                 "ckptLSN=%lu",
                                 1 - mail->log, shard->id, start_obj,
                                 high_offset, start_obj + num_objs - 1,
                                 ckpt->LSN );
                } else {

                    mcd_log_msg( 20525, MCD_REC_LOG_LVL_DEBUG,
                                 ">>>> Process curr log %u, shardID=%lu, "
                                 "start=%lu, highOff=%lu, end=%lu, "
                                 "ckptLSN=%lu",
                                 1 - mail->log, shard->id, start_obj,
                                 high_offset, start_obj + num_objs - 1,
                                 ckpt->LSN );

                    // find offset for the current log
                    log_offset = relative_log_offset( pshard, 1 - mail->log );

                    // apply log records from current log
                    curr_applied = process_log_chicken( context,
                                                shard,
                                                num_md_segs,
                                                log_offset,   // current log
                                                start_obj,
                                                num_objs,
                                                buf,
                                                mail->in_recovery,
                                                &high_LSN,
                                                &high_offset );

                    mcd_log_msg( 20526, MCD_REC_LOG_LVL_DEBUG,
                                 ">>>> End process curr log %u, shardID=%lu, "
                                 "start=%lu, highOff=%lu, end=%lu, "
                                 "ckptLSN=%lu, highLSN=%lu, applied=%d",
                                 1 - mail->log, shard->id, start_obj,
                                 high_offset, start_obj + num_objs - 1,
                                 ckpt->LSN, high_LSN, curr_applied );
                }

                mcd_log_msg( 20527, PLAT_LOG_LEVEL_INFO,
                             "Recovering object table, part %d of %d",
                             chunk + 1, num_chunks );

                // update the in-memory hash table with up-to-date metadata
                update_hash_table_chicken( shard,
                                   start_obj,
                                   num_objs,
                                   buf,
                                   &recovered_objs,
                                   lru_scan );

                mcd_log_msg( 20528, MCD_REC_LOG_LVL_DEBUG,
                             ">>>> Recovered chunk %d, objects=%lu, seqno=%lu",
                             chunk, recovered_objs, shard->sequence );
            }

            // write the updated chunk back to flash only when there
            // were log records applied
            if ( !old_applied && !curr_applied ) {

                mcd_log_msg( 20529, MCD_REC_LOG_LVL_DEBUG,
                             ">>>> Skip writing table chunk %d of %d, "
                             "shardID=%lu, start=%lu, count=%lu (startObj=%lu,"
                             " count=%lu, highOff=%lu, old=%d, curr=%d)",
                             chunk, num_chunks, shard->id, start_blk, num_blks,
                             start_obj, num_objs, high_offset, old_applied,
                             curr_applied );
            } else {

                mcd_log_msg( 20530, MCD_REC_LOG_LVL_DEBUG,
                             ">>>> Writing table chunk %d of %d, shardID=%lu, "
                             "startBlk=%lu, count=%lu (startObj=%lu, "
                             "count=%lu, highOff=%lu, old=%d, curr=%d)",
                             chunk, num_chunks, shard->id, start_blk, num_blks,
                             start_obj, num_objs, high_offset, old_applied,
                             curr_applied );

                // write back this section of the recovery table
                rc = table_chunk_op( context,
                                     shard,
                                     TABLE_WRITE,
                                     start_blk,
                                     num_blks,
                                     buf );
                plat_assert_rc( rc );
            }
        }

        // Update per-class eviction clock hands after recovery
        if ( mail->in_recovery ) {
            int                         i, j;
            uint32_t                    hand;
            uint32_t                    slabs_per_pth;
            mcd_osd_hash_t            * hash_entry;
            mcd_osd_slab_class_t      * class;

            for ( i = 0; i < MCD_OSD_MAX_NCLASSES; i++ ) {

                hash_entry = lru_scan[i].hash_entry;
                if ( NULL == hash_entry ) {
                    continue;
                }
                class = &shard->slab_classes[i];
                slabs_per_pth = class->slabs_per_segment / (flash_settings.num_sdf_threads*flash_settings.num_cores);

                for ( j = 0; j < class->num_segments; j++ ) {
                    if ( class->segments[j]->blk_offset <= hash_entry->address
                         && class->segments[j]->blk_offset +
                         Mcd_osd_segment_blks > hash_entry->address ) {
                        break;
                    }
                }

                if ( 1 >= slabs_per_pth ) {
                    hand = j * class->slabs_per_segment +
                        ( hash_entry->address % Mcd_osd_segment_blks ) /
                        class->slab_blksize + 1;
                    class->clock_hand[0] = hand;
                }
                else {
                    hand = j * slabs_per_pth +
                        ( ( hash_entry->address % Mcd_osd_segment_blks ) /
                          class->slab_blksize ) % slabs_per_pth + 1;
                    for ( int n = 0; n < MCD_OSD_MAX_PTHREADS; n++ ) {
                        class->clock_hand[n] = hand;
                    }
                }
            }
        }

        // -----------------------------------------------------
        // Write the checkpoint record for this shard with an
        // updated LSN of the last log page applied.
        // -----------------------------------------------------

        // set checkpoint LSN to highest LSN found
        if ( high_LSN > ckpt->LSN ) {

            ckpt->LSN = high_LSN;
            mcd_log_msg( 20531, MCD_REC_LOG_LVL_DEBUG,
                         ">>>> Updating ckptLSN=%lu, shardID=%lu",
                         ckpt->LSN, shard->id );

            memset( ckpt_buf, 0, Mcd_osd_blk_size );
            memcpy( ckpt_buf, ckpt, sizeof( mcd_rec_ckpt_t ) );

            ((mcd_rec_ckpt_t *)ckpt_buf)->checksum = 0;
            ((mcd_rec_ckpt_t *)ckpt_buf)->checksum =
                hash( (unsigned char *)ckpt_buf,
                      Mcd_osd_blk_size,
                      MCD_REC_CKPT_EYE_CATCHER );

            // write ckpt record with updated LSN
            rc = mcd_fth_aio_blk_write( context,
                                        ckpt_buf,
                                        (pshard->blk_offset +
                                         pshard->rec_md_blks - 1) *
                                        Mcd_osd_blk_size,
                                        Mcd_osd_blk_size );
            if ( FLASH_EOK != rc ) {
                mcd_log_msg( 20532, PLAT_LOG_LEVEL_FATAL,
                             "failed to write ckpt, shardID=%lu, offset=%lu, "
                             "rc=%d", shard->id, pshard->rec_md_blks - 1, rc );
                plat_abort();
            }

            // make sure these updates stick
            rc = mcd_aio_sync_devices();
            plat_assert_rc( rc );
        }

        // decrement reference count on shard
        (void) __sync_fetch_and_sub( &cntr->shard_refcount, 1 );

        // signal this shard update is done
        if ( mail->updated_mbox != NULL ) {
            fthMboxPost( mail->updated_mbox, recovered_objs );
        }

        // make persistent log available for writing
        if ( mail->updated_sem != NULL ) {
            fthSemUp( mail->updated_sem, 1 );
        }

        fthGetTimeOfDay( &tv );
        shard->rec_upd_usec    = ( (tv.tv_sec * PLAT_MILLION) + tv.tv_usec -
                                   shard->rec_upd_usec );
        shard->rec_upd_running = 0;

        mcd_log_msg( 20533, MCD_REC_LOG_LVL_DIAG,
                     ">>>> Updated shard_id=%lu, ckptLSN=%lu, old_log=%d, "
                     "in_recovery=%d, recovered_objs=%lu, high_seqno=%lu",
                     shard->id, ckpt->LSN, mail->log, mail->in_recovery,
                     recovered_objs, shard->sequence );
    }

    mcd_log_msg( 20534, PLAT_LOG_LEVEL_DIAGNOSTIC, "updater thread halting" );

    // free big buffer
    plat_free( buf );

    return;
}
#endif // MCD_REC_NOT_CHICKEN

/************************************************************************
 *                                                                      *
 *                      Memcached SLAB Logging                          *
 *                                                                      *
 ************************************************************************/

int
log_init( mcd_osd_shard_t * shard )
{
    int                         i;
    int                         size;
    uint64_t                    log_slots;
    mcd_rec_shard_t           * pshard = shard->pshard;
    mcd_rec_log_t             * log;

    mcd_rlg_msg( 20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    plat_assert_always( shard->ckpt != NULL );

    // calculate number of records in a persistent log
    log_slots = pshard->rec_log_blks * (MCD_OSD_BLK_SIZE /
                                        sizeof( mcd_logrec_object_t ));

    // allocate and initialize log descriptor for this shard
    log = plat_alloc( sizeof( mcd_rec_log_t ) );
    if ( log == NULL ) {
        mcd_rlg_msg( 20535, PLAT_LOG_LEVEL_ERROR,
                     "failed to allocate log desc" );
        return FLASH_ENOMEM;
    }
    shard->ps_alloc += sizeof( mcd_rec_log_t );

    memset( log, 0, sizeof( mcd_rec_log_t ) );
    log->curr_LSN             = 0; // must be initialized after recovery
    log->next_fill            = 0;
    log->total_slots          = log_slots;
    log->hwm_seqno            = 0;
    log->rtg_seqno            = 0;
    log->write_buffer_seqno   = 0;
    log->sync_completed_seqno = 0;
    log->logbuf               = NULL; // allocated below
    log->logbuf_base          = NULL;
    log->started              = 0;
    log->shutdown             = 0;
    log->pp_max_updates       = 0;
    log->segment_count        = 0;
    log->segments             = NULL; // allocated below

    log->pp_state.sync_recs     = 0; // init after recovery
    log->pp_state.curr_recs     = 0;
    log->pp_state.fill_count    = 0;
    log->pp_state.slot_count    = 0;
    log->pp_state.dealloc_count = 0;
    log->pp_state.dealloc_list  = NULL; // allocated below

    fthMboxInit( &log->update_mbox );
    fthMboxInit( &log->update_stop_mbox );
    fthSemInit( &log->fill_sem, MCD_REC_LOGBUF_RECS * MCD_REC_NUM_LOGBUFS );
    fthSemInit( &log->log_sem, MCD_REC_NUM_LOGS - 1 );
    fthLockInit( &log->sync_fill_lock );

    // initialize mail for updater
    for ( i = 0; i < MCD_REC_NUM_LOGS; i++ ) {
        log->update_mail[ i ].cntr         = NULL; // FIXME: delete
        log->update_mail[ i ].log          = i;
        log->update_mail[ i ].in_recovery  = 0;
        log->update_mail[ i ].updated_sem  = NULL;
        log->update_mail[ i ].updated_mbox = NULL;
    }

    // allocate and align the write buffers
    log->logbuf = plat_alloc( (MCD_REC_LOGBUF_SIZE * MCD_REC_NUM_LOGBUFS) +
                              Mcd_osd_blk_size );
    if ( log->logbuf == NULL ) {
        mcd_rlg_msg( 20536, PLAT_LOG_LEVEL_ERROR,
                     "failed to allocate log buffer" );
        return FLASH_ENOMEM;
    }
    shard->ps_alloc += ( (MCD_REC_LOGBUF_SIZE * MCD_REC_NUM_LOGBUFS) +
                         Mcd_osd_blk_size );

    log->logbuf_base = (char *)( ( (uint64_t)(log->logbuf) +
                                   Mcd_osd_blk_size - 1 ) & Mcd_osd_blk_mask );

    // initialize log buffers
    for ( i = 0; i < MCD_REC_NUM_LOGBUFS; i++ ) {
        fthSemInit( &log->logbufs[i].real_write_sem, 0 );
        log->logbufs[i].id         = i;
        log->logbufs[i].seqno      = ( log->write_buffer_seqno +
                                       (i * MCD_REC_LOGBUF_SLOTS) );
        log->logbufs[i].fill_count = 0;
        log->logbufs[i].sync_blks  = 0;
        log->logbufs[i].write_sem  = &log->logbufs[i].real_write_sem; // FIXME
        log->logbufs[i].sync_sem   = NULL;
        log->logbufs[i].buf        = ( log->logbuf_base +
                                       (i * MCD_REC_LOGBUF_SIZE) );
        log->logbufs[i].entries = (mcd_logrec_object_t *)log->logbufs[i].buf;
        memset( log->logbufs[i].buf, 0, MCD_REC_LOGBUF_SIZE );
    }

    // find max log records in default 1MB buffer or in the persistent log
    log->pp_max_updates  = (MEGABYTE > pshard->rec_log_blks * Mcd_osd_blk_size
                            ? pshard->rec_log_blks * Mcd_osd_blk_size
                            : MEGABYTE);
    log->pp_max_updates  = ( ( log->pp_max_updates / MCD_REC_LOGBUF_SIZE ) *
                             MCD_REC_LOGBUF_RECS );

    // allocate memory for sync postprocessing
    size = sizeof( uint32_t ) * (log->pp_max_updates + MCD_REC_LOGBUF_RECS);
    log->pp_state.dealloc_list = plat_alloc( size );
    if ( log->pp_state.dealloc_list == NULL ) {
        mcd_rlg_msg( 20538, PLAT_LOG_LEVEL_ERROR,
                     "failed to allocate PP logbuf" );
        return FLASH_ENOMEM;
    }
    memset( log->pp_state.dealloc_list, 0, size );
    shard->ps_alloc += size;

    if ( !Mcd_rec_chicken ) {
        // allocate log segments to this shard
        log->segment_count =
            (pshard->rec_log_blks + Mcd_rec_log_segment_blks - 1) /
            Mcd_rec_log_segment_blks;

	if (log->segment_count > Mcd_rec_free_log_seg_curr ) {

	    mcd_log_msg( 30655, PLAT_LOG_LEVEL_ERROR,
			 "Ran out of free log segments, shardID=%lu",
			 shard->id );

	    // Free memory alloc'ed in this function
	    if ( log->pp_state.dealloc_list ) {
		plat_free( log->pp_state.dealloc_list );
		shard->ps_alloc -= ( sizeof( uint32_t ) *
				     (log->pp_max_updates + MCD_REC_LOGBUF_RECS) );
	    }

	    if ( log->logbuf ) {
		plat_free( log->logbuf );
		shard->ps_alloc -= (MCD_REC_LOGBUF_SIZE * MCD_REC_NUM_LOGBUFS) +
		    Mcd_osd_blk_size;
	    }

	    plat_free( log );
	    shard->ps_alloc -= sizeof( mcd_rec_log_t );

	    return FLASH_ENOMEM;
	}

        log->segments = plat_alloc( log->segment_count * sizeof( void * ) );
        if ( log->segments == NULL ) {
            mcd_log_msg( 40033, PLAT_LOG_LEVEL_ERROR,
                         "failed to alloc shard log segments, shardID=%lu",
                         shard->id );
            return FLASH_ENOMEM;
        }
        shard->ps_alloc += (log->segment_count * sizeof( void * ));

        // attach log segments to shard
        fthWaitEl_t * wait = fthLock( &Mcd_rec_log_segment_lock, 1, NULL );
        for ( int s = 0; s < log->segment_count; s++ ) {
            log->segments[ s ] =
                Mcd_rec_free_log_segments[ --Mcd_rec_free_log_seg_curr ];
        }
        mcd_log_msg( 160018, PLAT_LOG_LEVEL_INFO,
                     "allocated %d log segments to shardID=%lu, remaining=%lu",
                     log->segment_count, shard->id,
                     Mcd_rec_free_log_seg_curr );
        fthUnlock( wait );
    }

    // set the log state pointer
    shard->log = log;

    return 0;
}

int
log_init_phase2( void * context, mcd_osd_shard_t * shard )
{
    int                         i;
    int                         curr_log = 0;
    uint64_t                    log_page;
    uint64_t                    last_LSN;
    uint64_t                    next_LSN;
    mcd_rec_shard_t           * pshard = shard->pshard;
    mcd_rec_log_t             * log = shard->log;

    mcd_rlg_msg( 20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    plat_assert_always( shard->ckpt != NULL );
    plat_assert_always( log != NULL );

    // get the current (ckpt) LSN *after* recovery
    log->curr_LSN = shard->ckpt->LSN;

    // find current log page and switch logs
    if ( log->curr_LSN > 0 ) {

        // calculate last page of log (contains last LSN)
        log_page = (log->curr_LSN - 1) % pshard->rec_log_blks;
        curr_log =
            ((log->curr_LSN - 1) / pshard->rec_log_blks) % MCD_REC_NUM_LOGS;

        // get LSN for last log page
        last_LSN = read_log_page( context, shard, curr_log, log_page );

        // FIXME - do something better than assert here
        // verify end LSN
        plat_assert_always( last_LSN == log->curr_LSN );

        // find offset of next page (last plus one page)
        if ( log_page < pshard->rec_log_blks - 1 ) {
            next_LSN = read_log_page( context, shard, curr_log, log_page + 1 );
        } else {
            next_LSN = read_log_page( context, shard, 1 - curr_log, 0 );
        }

        // FIXME - do something better than assert here
        // make sure LSN fell off in the next log page
        plat_assert_always( last_LSN > next_LSN );

        // after initialization, log 0 always fills first;
        // to fill log 1 first, switch persistent logs
        if ( curr_log == 0 ) {
            log->write_buffer_seqno += log->total_slots;
            log->next_fill           = log->write_buffer_seqno;
            for ( i = 0; i < MCD_REC_NUM_LOGBUFS; i++ ) {
                log->logbufs[i].seqno = ( log->write_buffer_seqno +
                                          (i * MCD_REC_LOGBUF_SLOTS) );
            }
        }

        // update current LSN for consistency (not strictly required)
        log->curr_LSN = ( (log->curr_LSN + pshard->rec_log_blks - 1) /
                          pshard->rec_log_blks ) * pshard->rec_log_blks;
    }

    // get number of updates per sync for the container
    if ( shard->cntr != NULL ) {
        // not previously set
        if ( shard->cntr->sync_updates == 0 ) {
            shard->cntr->sync_updates = log->pp_max_updates;
            log->pp_state.sync_recs   = log->pp_max_updates;
        }
        // set previously saved value
        else {
            if ( shard->cntr->sync_updates > log->pp_max_updates ) {
                mcd_rlg_msg( 20537, PLAT_LOG_LEVEL_INFO,
                             "shardId=%lu, limiting SYNC_UPDATES to %d",
                             shard->id, log->pp_max_updates );
                shard->cntr->sync_updates = log->pp_max_updates;
            }
            log->pp_state.sync_recs = shard->cntr->sync_updates;
        }
    }

    // start log writer thread
    fthResume( fthSpawn( &log_writer_thread, 40960 ), (uint64_t)shard );

    mcd_rlg_msg( 20541, PLAT_LOG_LEVEL_INFO,
                 "log initialized, shardID=%lu, "
                 "curr_log=%d, log_offset=%lu/%lu, log_blks=%lu, "
                 "curr_LSN=%lu, wbs=%lu, nextfill=%lu, lbs0=%lu, lbs1=%lu",
                 pshard->shard_id, curr_log,
                 relative_log_offset( pshard, curr_log ),
                 relative_log_offset( pshard, 1 - curr_log ),
                 pshard->rec_log_blks, log->curr_LSN,
                 log->write_buffer_seqno, log->next_fill,
                 log->logbufs[0].seqno, log->logbufs[1].seqno );

    return 0;
}


/*
 * Persist the log.
 */
static void
persist_log(mcd_osd_shard_t *shard,
            uint64_t slot_seqno,
            mcd_logrec_object_t *logrec_obj)
{
    char buf[FLUSH_LOG_SEC_SIZE+FLUSH_LOG_SEC_ALIGN-1];
    char       *sector = align(buf, FLUSH_LOG_SEC_ALIGN);
    mcd_rec_log_t *log = shard->log;
    uint64_t   log_blk = ((slot_seqno % log->total_slots) *
                          sizeof(mcd_logrec_object_t)) / Mcd_osd_blk_size;
    int        log_num = ((slot_seqno / log->total_slots) % MCD_REC_NUM_LOGS);
    uint64_t start_blk = relative_log_offset(shard->pshard, log_num) + log_blk;
    uint64_t start_seg = start_blk / Mcd_osd_segment_blks;
    uint64_t shard_blk = shard->segments[start_seg] +
                         (start_blk % Mcd_osd_segment_blks);
    int      shard_off = (slot_seqno % MCD_REC_LOG_BLK_SLOTS) *
                         sizeof(mcd_logrec_object_t);
    uint64_t flush_blk = slot_seqno %
                         (MCD_REC_NUM_LOGBUFS * MCD_REC_LOGBUF_SLOTS);
    ssize_t   sec_size = FLUSH_LOG_SEC_SIZE;
    ssize_t flush_seek = flush_blk * sec_size;
    flog_rec_t   *frec = (flog_rec_t *)sector;

    memset(sector, 0, sec_size);
    frec->magic      = FLUSH_LOG_MAGIC;
    frec->flush_blk  = flush_blk;
    frec->shard_blk  = shard_blk;
    frec->shard_off  = shard_off;
    frec->lsn        = (slot_seqno / MCD_REC_LOG_BLK_SLOTS) + 1;
    frec->logrec_obj = *logrec_obj;

    size_t size = pwrite(shard->flush_fd, sector, sec_size, flush_seek);
    if (size != sec_size) {
        mcd_log_msg(70109, PLAT_LOG_LEVEL_ERROR,
                    "Flush log sync:"
                    " log flush write failed seek=%ld errno=%d size=%ld",
                    flush_seek, errno, size);
    }
    if (Sync_data)
        fdatasync(shard->flush_fd);
}


void
log_write_internal( mcd_osd_shard_t * shard, mcd_logrec_object_t * data )
{
    enum flush_mode             mode;
    fthWaitEl_t               * wait;
    uint64_t                    slot_seqno;
    uint64_t                    rec_filled;
    uint64_t                    nth_buffer;
    uint64_t                    buf_offset;
    mcd_rec_log_t             * log = shard->log;
    mcd_rec_logbuf_t          * logbuf;
    fthSem_t                    sync_sem;

    // Note: this assumes the page header and all log records are
    //       of equal size, and a power of 2 !!

    //mcd_rlg_msg( 20000, PLAT_LOG_LEVEL_TRACE, "ENTERING" );

    if ( data ) {
        mode = FLUSH_MODE_FILL;
    } else {
        mode = FLUSH_MODE_SYNC;
    }

    // get write lock for sync, read  lock for fill
    if (Log_flush_in_place)
        wait = fthLock(&log->sync_fill_lock, 1, NULL);
    else
        wait = fthLock(&log->sync_fill_lock, mode == FLUSH_MODE_SYNC, NULL);

    /*
     * We won't actually consume a slot in the sync case but need to
     * wait for a buffer to become available.  This is undone when the
     * sync completes.
     */
    fthSemDown( &log->fill_sem, 1 );

    if ( mode == FLUSH_MODE_FILL ) {
        // leave room for page header
        do {
            slot_seqno = __sync_fetch_and_add( &log->next_fill, 1 );
        } while ( (slot_seqno % MCD_REC_LOG_BLK_SLOTS) == 0 );
    } else {
        slot_seqno = log->next_fill;
    }

    buf_offset = slot_seqno % MCD_REC_LOGBUF_SLOTS;
    nth_buffer = slot_seqno / MCD_REC_LOGBUF_SLOTS;
    logbuf     = &log->logbufs[ nth_buffer % MCD_REC_NUM_LOGBUFS ];

    if ( mode == FLUSH_MODE_FILL ) {

        mcd_rlg_msg( 40070, MCD_REC_LOG_LVL_TRACE,
                     "<<<< log_write: shardID=%lu, syn=%u, blocks=%u, "
                     "del=%u, bucket=%u, blk_offset=%u, seqno=%lu, "
                     "old_offset=%u, tseq=%lu",
                     shard->id, data->syndrome,
                     mcd_osd_lba_to_blk( data->blocks ), data->deleted,
                     data->bucket, data->blk_offset, data->seqno,
                     data->old_offset, data->target_seqno );

        plat_assert_always( slot_seqno == logbuf->seqno + buf_offset );

        logbuf->entries[ buf_offset ] = *data;
        rec_filled = __sync_add_and_fetch( &logbuf->fill_count, 1 );

        if (shard->flush_fd > 0)
            persist_log(shard, slot_seqno, data);

        if ( rec_filled == MCD_REC_LOGBUF_RECS ) {
            mcd_rlg_msg( 20543, PLAT_LOG_LEVEL_DEBUG,
                         "<<<< log_write: shardID=%lu, logbuf %d filled",
                         shard->id, logbuf->id );

            fthSemUp( logbuf->write_sem, 1 );
        }

        // Update the logbuf seqno cache
        rep_logbuf_seqno_update( (struct shard *)shard,
                                 nth_buffer,
                                 data->seqno );

        if (Log_flush_in_place) {
            fthSemInit( &sync_sem, 0 );
            logbuf->sync_sem  = &sync_sem;
            logbuf->sync_blks = 1 + (buf_offset / MCD_REC_LOG_BLK_SLOTS);

            fthSemUp( logbuf->write_sem, 1 );
            fthSemDown( &sync_sem, 1 );

            logbuf->sync_blks = 0;
            logbuf->sync_sem  = NULL;
        }
    } else {

        fthSemInit( &sync_sem, 0 );
        logbuf->sync_sem  = &sync_sem;
        logbuf->sync_blks = 1 + (buf_offset / MCD_REC_LOG_BLK_SLOTS);

        mcd_rlg_msg( 20544, MCD_REC_LOG_LVL_DEBUG,
                     "<<<< sync: shardID=%lu, logbuf=%d, blks=%u",
                     shard->id, logbuf->id, logbuf->sync_blks );
        fthSemUp( logbuf->write_sem, 1 );
        fthSemDown( &sync_sem, 1 );

        mcd_rlg_msg( 20545, MCD_REC_LOG_LVL_DEBUG,
                     ">>>> sync complete: shardID=%lu, logbuf=%d, blks=%u",
                     shard->id, logbuf->id, logbuf->sync_blks );

        logbuf->sync_blks = 0;
        logbuf->sync_sem  = NULL;

        /* We didn't actually consume a slot */
        fthSemUp( &log->fill_sem, 1 );
    }

    fthUnlock( wait );

    return;
}

void
log_write( mcd_osd_shard_t * shard, mcd_logrec_object_t * data )
{
    mcd_rlg_msg( 20546, PLAT_LOG_LEVEL_DEBUG,
                 "ENTERING: shardID=%lu", shard->id );

    // write the log record
    (void) __sync_fetch_and_add( &shard->refcount, 1 );
    log_write_internal( shard, data );
    (void) __sync_fetch_and_sub( &shard->refcount, 1 );

    return;
}

void
log_sync( mcd_osd_shard_t * shard )
{
    mcd_rlg_msg( 20546, PLAT_LOG_LEVEL_DEBUG,
                 "ENTERING: shardID=%lu", shard->id );

    // sync the log
    (void) __sync_fetch_and_add( &shard->refcount, 1 );
    log_write_internal( shard, NULL );
    (void) __sync_fetch_and_sub( &shard->refcount, 1 );

    return;
}

void
log_wait( mcd_osd_shard_t * shard )
{
    mcd_rec_update_t            update_mail;
    fthMbox_t                   updated_mbox;

    mcd_rlg_msg( 20547, PLAT_LOG_LEVEL_DEBUG,
                 "ENTERING: shardID=%lu, state=%s",
                 shard->id, shard->cntr->state == cntr_stopping ? "stopping" :
                 shard->cntr->state == cntr_stopped ? "stopped" : "running" );

    plat_assert( shard->cntr->state == cntr_stopping ||
                 shard->cntr->state == cntr_stopped );

    // initialize
    fthMboxInit( &updated_mbox );
    update_mail.log          = -1;            // out-of-band log value
    update_mail.cntr         = shard->cntr;
    update_mail.in_recovery  = 0;
    update_mail.updated_sem  = NULL;
    update_mail.updated_mbox = &updated_mbox;

    // signal updater thread
    if ( Mcd_rec_chicken ) {
        fthMboxPost( &Mcd_rec_updater_mbox, (uint64_t)(&update_mail) );
    } else {
        fthMboxPost( &shard->log->update_mbox, (uint64_t)(&update_mail) );
    }

    // wait for updater thread to finish
    fthMboxWait( &updated_mbox );

    return;
}

inline uint64_t
log_get_buffer_seqno( mcd_osd_shard_t * shard )
{
    return shard->log->write_buffer_seqno;
}

void
log_sync_postprocess( mcd_osd_shard_t * shard,
                      mcd_rec_pp_state_t * pp_state )
{
    mcd_rlg_msg( 40053, PLAT_LOG_LEVEL_DEBUG,
                 "ENTERING: shardID=%lu, count=%d, fill_count=%u",
                 shard->id, pp_state->dealloc_count, pp_state->fill_count );

    for ( int d = 0; d < pp_state->dealloc_count; d++ ) {
        mcd_rlg_msg( 40123, PLAT_LOG_LEVEL_TRACE,
                     "Dealloc[%d]: %d", d, pp_state->dealloc_list[ d ]  );
        mcd_fth_osd_slab_dealloc( shard, pp_state->dealloc_list[ d ] );
    }

    pp_state->dealloc_count = 0;

    return;
}

void
log_write_postprocess( mcd_osd_shard_t * shard, mcd_rec_logbuf_t * logbuf,
                       uint64_t * high_seqno )
{
    int                         s;
    mcd_logrec_object_t       * rec;
    mcd_bak_ps_entry_t        * entry = NULL;
    mcd_bak_state_t           * bk = shard->backup;
    mcd_rec_log_t             * log = shard->log;
    mcd_rec_pp_state_t        * pp = &log->pp_state;

    mcd_rlg_msg( 40054, PLAT_LOG_LEVEL_DEBUG,
                 "ENTERING: shardID=%lu, seqno=%lu, count=%d, fill_count=%u",
                 shard->id, logbuf->seqno, pp->dealloc_count, pp->fill_count );

    // initialize
    *high_seqno = 0;

#ifdef MCD_ENABLE_TOMBSTONES
    // prune the tombstone list
    tombstone_prune( shard );
#endif

    // examine every slot since last time
    for ( s = pp->slot_count; s < MCD_REC_LOGBUF_SLOTS; s++ ) {

        // skip over slot for page LSN
        if ( (s % MCD_REC_LOG_BLK_SLOTS) == 0 ) {
            continue;
        }

        // map a log record
        rec = (mcd_logrec_object_t *)(logbuf->buf +
                                      (s * sizeof( mcd_logrec_object_t )));

        // no log record - done for now
        if ( rec->syndrome == 0 && rec->blocks == 0 &&
             rec->bucket == 0 && rec->seqno == 0 &&
             rec->old_offset == 0 ) {
            if ( rec->blk_offset == 0xffffffffu ) {
                continue;
            }
            break;
        }

        // save high sequence number
        if ( rec->seqno > *high_seqno ) {
            *high_seqno = rec->seqno;
        }

        // update bitmaps for backup
        if ( bk->backup_prev_seqno == bk->backup_curr_seqno ||
             rec->seqno <= bk->backup_curr_seqno ||
             bk->snapshot_complete ||
             shard->restore_running ) {

            // update the alloc/update bitmaps
            backup_maintain_bitmaps( shard, rec->blk_offset,
                                     (rec->blocks == 0) );
            if ( rec->old_offset != 0 && rec->blocks > 0 ) {
                backup_maintain_bitmaps( shard, ~(rec->old_offset), 1 );
            }
        }
        else if ( rec->seqno > bk->backup_curr_seqno ) {
            // add to pending seqno queue
            plat_calloc_struct( &entry );
            plat_assert( entry );
            entry->rec = (*rec);
            TAILQ_INSERT_TAIL( &bk->ps_list, entry, list_entry );
            bk->ps_count++;
            mcd_bak_msg( 20549, MCD_REC_LOG_LVL_DEBUG,
                         "shardId=%lu, curr_seqno=%lu, rec_seqno=%lu, "
                         "count=%d", shard->id, bk->backup_curr_seqno,
                         rec->seqno, bk->ps_count );
        }

        // ------------------------------------------------------
        // Main processing (what this function was created to do)
        // ------------------------------------------------------

        // handle replicated shard
        if ( shard->replicated ) {
#ifdef MCD_ENABLE_TOMBSTONES
            // process delete record
            if ( rec->blocks == 0 ) {
                // dealloc space when sequence number has been synced
                if ( rec->seqno < tombstone_get_lcss( shard ) ) {
                    pp->dealloc_list[ pp->dealloc_count++ ] = rec->blk_offset;
                }
                // add tombstone to list
                else {
                    tombstone_add( shard, rec->syndrome, rec->blk_offset,
                                   rec->seqno );
                }
            }

            // process overwrite record
            else if ( rec->old_offset != 0 ) {
                // dealloc space when sequence number has been synced
                if ( rec->seqno < tombstone_get_lcss( shard ) ) {
                    pp->dealloc_list[ pp->dealloc_count++ ] =
                        ~(rec->old_offset);
                }
                // add tombstone to list
                else {
                    tombstone_add( shard, rec->syndrome, ~(rec->old_offset),
                                   rec->seqno );
                }
            }
#endif
        }

        // no replication, process overwrite record
        else if ( rec->old_offset != 0 ) {

            // add to dealloc list
            pp->dealloc_list[ pp->dealloc_count++ ] = ~(rec->old_offset);
            mcd_rlg_msg( 40124, PLAT_LOG_LEVEL_TRACE,
                         "Pending dealloc[%d]: %d",
                         pp->dealloc_count - 1,
                         pp->dealloc_list[ pp->dealloc_count - 1 ] );
        }
    }

    // check for snapshot of bitmaps for backup
    if ( bk->backup_prev_seqno != bk->backup_curr_seqno &&
         bk->snapshot_logbuf == logbuf->seqno ) {

        mcd_bak_msg( 40055, MCD_REC_LOG_LVL_DEBUG,
                     "shardId=%lu, snap_seqno=%lu, logbuf_seqno=%lu, "
                     "apply %d pending updates",
                     shard->id, bk->snapshot_logbuf, logbuf->seqno,
                     bk->ps_count );

        // do the bitmap snapshot
        backup_snapshot_bitmaps( shard );
        bk->snapshot_logbuf   = MCD_BAK_SNAPSHOT_LOGBUF_INITIAL;
        bk->snapshot_complete = 1;

        // remove pending seqno items and apply to live bitmaps
        while ( !TAILQ_EMPTY( &bk->ps_list ) ) {
            entry = TAILQ_FIRST( &bk->ps_list );
            TAILQ_REMOVE( &bk->ps_list, entry, list_entry );
            bk->ps_count--;

            mcd_bak_msg( 20549, MCD_REC_LOG_LVL_TRACE,
                         "shardId=%lu, curr_seqno=%lu, rec_seqno=%lu, "
                         "count=%d", shard->id, bk->backup_curr_seqno,
                         entry->rec.seqno, bk->ps_count );

            if ( bk->ps_count < 0 ) {
                plat_abort();
            }

            // update the alloc/update bitmaps
            backup_maintain_bitmaps( shard, entry->rec.blk_offset,
                                     (entry->rec.blocks == 0) );
            if ( entry->rec.old_offset != 0 &&
                 entry->rec.blocks > 0 ) {
                backup_maintain_bitmaps( shard,
                                         ~(entry->rec.old_offset), 1 );
            }
        }
    }

    mcd_rlg_msg( 40056, MCD_REC_LOG_LVL_DEBUG,
                 "shardID=%lu, seqno=%lu, prev=%u, curr=%u, left=%lu, "
                 "hiseq=%lu", shard->id, logbuf->seqno, pp->fill_count,
                 s - pp->fill_count, MCD_REC_LOGBUF_SLOTS - s, *high_seqno );

    pp->slot_count = s;
    return;
}

void
log_writer_thread( uint64_t arg )
{
    bool                        persistent_log_filled;
    int                         i, rc;
    int                         curr_log;
    uint64_t                    thread_id;
    uint64_t                    LSN;
    uint64_t                    blk_count;
    uint64_t                    log_blk;
    uint64_t                    start_blk;
    uint64_t                    end_blk;
    uint64_t                    start_seg;
    uint64_t                    end_seg;
    uint64_t                    offset;
    uint64_t                    high_seqno;
    mcd_osd_shard_t           * shard;
    mcd_rec_shard_t           * pshard;
    mcd_rec_log_t             * log;
    mcd_rec_logbuf_t          * logbuf;
    mcd_rec_logpage_hdr_t     * hdr;
    osd_state_t               * context;
    fthSem_t                  * sync_sem;

    mcd_rlg_msg( 20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    // free unused buffer
    context = context_alloc( SSD_AIO_CTXT_MCD_REC_LGWR );
    if ( context->osd_buf != NULL ) {
        mcd_fth_osd_iobuf_free( context->osd_buf );
        context->osd_buf = NULL;
    }

    // recover shard pointer
    shard = (mcd_osd_shard_t *)arg;
    plat_assert_always( shard != NULL );

    thread_id = __sync_add_and_fetch( &Mcd_rec_log_writer_threads, 1 );
    mcd_rlg_msg( 40079, PLAT_LOG_LEVEL_DIAGNOSTIC,
                 "Log writer thread %lu assigned to shardID=%lu",
                 thread_id, shard->id );

    pshard = shard->pshard;
    log    = shard->log;

    // Note: this assumes the page header and all log records
    // are of equal size, and a power of 2 !!

    plat_assert_always( sizeof( mcd_rec_logpage_hdr_t ) ==
                        sizeof( mcd_logrec_object_t ) );
    plat_assert_always( MCD_OSD_BLK_SIZE % sizeof(mcd_logrec_object_t) == 0 );
    plat_assert_always( pshard != NULL );
    plat_assert_always( log != NULL );

    // log writer holds a reference for its lifetime
    (void) __sync_add_and_fetch( &shard->refcount, 1 );

    // show that the log writer has been started
    log->started = 1;
    sync_sem     = NULL;

    // -------------------------------------
    // wait for signal to write a log buffer
    // -------------------------------------

    while ( 1 ) {

        // get pointer to next logbuf
        logbuf = &log->logbufs[ (log->write_buffer_seqno /
                                 MCD_REC_LOGBUF_SLOTS) % MCD_REC_NUM_LOGBUFS ];
        /* #flush_fill exclusive lock on synced buffer will be released here */
        if ( sync_sem ) {
            // signal thread waiting for sync to complete
            fthSemUp( sync_sem, 1 );
        }

        // ------------------------------------------------------
        // wait for the opposite logbuf (enforces write ordering)
        // ------------------------------------------------------

        fthSemDown( logbuf->write_sem, 1 );

        // check for shutdown request
        if ( log->shutdown ) {
            break;
        }

        /* ---- Write magic happens here! ---- */

        persistent_log_filled = false;

        if ( logbuf->sync_blks == 0 ) {
            blk_count = MCD_REC_LOGBUF_BLKS;
        } else {
            blk_count = logbuf->sync_blks;
        }

        LSN = log->curr_LSN;

        // install LSN and checksum in every page header in logbuf
        for ( i = 0; i < blk_count; i++ ) {
            hdr = (mcd_rec_logpage_hdr_t *)(logbuf->buf +
                                            (i * Mcd_osd_blk_size));
            plat_assert_always( hdr->LSN == 0 || hdr->LSN == LSN + 1 );
            hdr->eye_catcher = MCD_REC_LOGHDR_EYE_CATCHER;
            hdr->version     = MCD_REC_LOGHDR_VERSION;
            hdr->LSN         = ++LSN;
            hdr->checksum    = 0;
            hdr->checksum    = hash( (unsigned char *)
                                     (logbuf->buf + (i * Mcd_osd_blk_size)),
                                     Mcd_osd_blk_size, hdr->LSN );
        }

#ifdef MCD_REC_DEBUGGING
        for ( /* no initializer */ ; i < MCD_REC_LOGBUF_BLKS; i++ ) {
            hdr = (mcd_rec_logpage_hdr_t *)(logbuf->buf +
                                            (i * Mcd_osd_blk_size));
            plat_assert_always( hdr->LSN == 0 );
        }
#endif

        // find current persistent log
        curr_log  = ( (log->write_buffer_seqno / log->total_slots) %
                      MCD_REC_NUM_LOGS );

        // calculate block offset within log to write this log buffer
        log_blk   = ( (log->write_buffer_seqno % log->total_slots) *
                      sizeof( mcd_logrec_object_t ) ) / Mcd_osd_blk_size;

        // calculate start/end offsets within current log
        start_blk = relative_log_offset( pshard, curr_log ) + log_blk;
        end_blk   = start_blk + blk_count - 1;

        // calculate start/end segments
        start_seg = start_blk / Mcd_osd_segment_blks;
        end_seg   = end_blk / Mcd_osd_segment_blks;

        // sanity: start/end segments must be equal and within metadata area
        plat_assert( start_seg < ((pshard->total_blks / Mcd_osd_segment_blks) -
                                  shard->total_segments) );
        plat_assert( end_seg < ((pshard->total_blks / Mcd_osd_segment_blks) -
                                shard->total_segments) );

        if (start_seg != end_seg) {
            fprintf(stderr, "start_seg=%ld end_seg=%ld start_blk=%ld"
                            " end_blk=%ld blk_count=%ld\n",
                    start_seg, end_seg, start_blk, end_blk, blk_count);
        }
        plat_assert( start_seg == end_seg );

        // look up physical block offset of start
        offset = shard->segments[ start_seg ] +
            (start_blk % Mcd_osd_segment_blks);

        // write the (perhaps partially-filled) logbuf buffer

        rc = mcd_fth_aio_blk_write( (void *)context,
                                    logbuf->buf,
                                    offset * Mcd_osd_blk_size,
                                    blk_count * Mcd_osd_blk_size );
        if ( FLASH_EOK != rc ) {
            mcd_rlg_msg( 20552, PLAT_LOG_LEVEL_FATAL,
                         "failed to commit log buffer, shardID=%lu, "
                         "blk_offset=%lu, count=%lu, rc=%d",
                         shard->id, offset, blk_count, rc );
            plat_abort();
        }

        /* ---- end of write magic ----- */

        /* sync_sem is grabbed before the buffer may be reused */
        sync_sem = logbuf->sync_sem;

        // update number of records written since last sync
        plat_assert( logbuf->fill_count >= log->pp_state.fill_count );
        log->pp_state.curr_recs += (logbuf->fill_count -
                                    log->pp_state.fill_count);
        log->pp_state.fill_count = logbuf->fill_count;

        // postprocess after write with current logbuf
        log_write_postprocess( shard, logbuf, &high_seqno );

        /* advance to next buffer if this one was full */
        if ( logbuf->fill_count == MCD_REC_LOGBUF_RECS ) {

            // reset logbuf buffer after copying to postprocess buffer
            memset( logbuf->buf, 0, MCD_REC_LOGBUF_SIZE );
            logbuf->fill_count       = 0;
            log->pp_state.fill_count = 0;
            log->pp_state.slot_count = 0;

            /* Sanity check that we're writing in order */
            plat_assert_always( log->write_buffer_seqno == logbuf->seqno );

            log->write_buffer_seqno += MCD_REC_LOGBUF_SLOTS;
            log->curr_LSN           += MCD_REC_LOGBUF_BLKS;

            // when persistent log is full, signal updater thread
            if ( log->write_buffer_seqno % log->total_slots == 0 ) {
                persistent_log_filled = true;
            }

            /* This works because we use buffers in round-robin order */
            logbuf->seqno += ( MCD_REC_LOGBUF_SLOTS * MCD_REC_NUM_LOGBUFS );

            fthSemUp( &log->fill_sem, MCD_REC_LOGBUF_RECS );
        }

        // sync device(s) and postprocess sync'ed logbuf(s) when any
        // of the following is true:
        //  - explicit sync request received
        //  - hit the write limit
        //  - a persistent log is filled
        if ( sync_sem ||
             log->pp_state.curr_recs >= log->pp_state.sync_recs ||
             persistent_log_filled ) {

            mcd_rlg_msg( 40057, MCD_REC_LOG_LVL_DEBUG,
                         "SYNC: shardID=%lu, pp_recs=%d, sync_recs=%d, "
                         "seqno=%lu, sync=%s",
                         shard->id, log->pp_state.curr_recs,
                         log->pp_state.sync_recs, logbuf->seqno,
                         (sync_sem ? "true" : "false") );

            // sync all devices for explicit sync
            if ( sync_sem ) {
                rc = mcd_aio_sync_devices();
                plat_assert_rc( rc );
            }
            // sync if any records have been written since last sync
            else if ( log->pp_state.curr_recs > 0 ) {
                // sync log device(s); syncs all on raid
                rc = mcd_aio_sync_device_offset( offset * Mcd_osd_blk_size,
                                                 blk_count *
                                                 Mcd_osd_blk_size );
                plat_assert_rc( rc );
            }

            // postprocess after sync
            log_sync_postprocess( shard, &log->pp_state );
            log->pp_state.curr_recs   = 0;

            // FIXME: this isn't used for anything; could it be made useful?
            log->sync_completed_seqno = (logbuf->fill_count == 0
                                         ? logbuf->seqno - MCD_REC_LOGBUF_SLOTS
                                         : logbuf->seqno + blk_count);
        }

        // when persistent log is full, signal updater thread
        if ( persistent_log_filled ) {

            mcd_rlg_msg( 20556, MCD_REC_LOG_LVL_DEBUG,
                         "signaling update thread, "
                         "shardID=%lu, log=%d, hwm_seqno=%lu",
                         shard->id, curr_log, high_seqno );

            // build mail to send to updater thread
            log->update_mail[ curr_log ].updated_sem  = &log->log_sem;
            log->update_mail[ curr_log ].updated_mbox = NULL;

            // signal updater_thread to apply logs to this shard
            if ( Mcd_rec_chicken ) {
                fthMboxPost( &Mcd_rec_updater_mbox,
                             (uint64_t)(&log->update_mail[ curr_log ]) );
            } else {
                fthMboxPost( &shard->log->update_mbox,
                             (uint64_t)(&log->update_mail[ curr_log ]) );
            }

            // wait for opposite log if updater is still accessing it
            fthSemDown( &log->log_sem, 1 );

#ifdef MCD_ENABLE_TOMBSTONES
            // limit tombstone longevity for cache mode shards
            if ( shard->evict_to_free ) {

                // remember sequence numbers in the persistent log
                log->rtg_seqno = log->hwm_seqno; // truncated seqno
                log->hwm_seqno = high_seqno;     // curr log high seqno

                // Tell the replicator the "retained tombstone guarantee",
                // the highest truncated (unretrievable) seqno
                if ( Mcd_set_rtg_callback != NULL ) {
                    (*Mcd_set_rtg_callback)( shard->id, log->rtg_seqno );
                }
            }
#endif
        }

        mcd_rlg_msg( 20557, MCD_REC_LOG_LVL_DIAG,
                     "shardID=%lu, logbuf %d written, sync=%s, "
                     "log=%d, rel_off=%lu, blk_off=%lu, blk_cnt=%lu",
                     shard->id, logbuf->id, (sync_sem ? "true" : "false"),
                     curr_log, log_blk, offset, blk_count );
    }

    mcd_rlg_msg( 40080, PLAT_LOG_LEVEL_DIAGNOSTIC,
                 "Log writer thread %lu halting, shardID=%lu",
                 thread_id, shard->id );

    (void) __sync_sub_and_fetch( &Mcd_rec_log_writer_threads, 1 ),

    // show that the log writer has ended
    log->started = 0;

    // signal waiter
    if ( logbuf->sync_sem ) {
        fthSemUp( logbuf->sync_sem, 1 );
    }

    // decrement refcount
    (void) __sync_sub_and_fetch( &shard->refcount, 1 );

    // return context
    context_free( context );

    return;
}


/************************************************************************
 *                                                                      *
 *                      Memcached SLAB formatting                       *
 *                                                                      *
 ************************************************************************/

int
shard_format( uint64_t shard_id, int flags, uint64_t quota, unsigned max_objs,
              mcd_osd_shard_t * shard )
{
    int                         b, c, s, rc;
    int                         obj_per_blk;
    int                         blksize;
    int                         slot;
    int                         buf_size = MCD_REC_FORMAT_BUFSIZE;
    int                         buf_blks;
    int                         align_blks = MCD_REC_ALIGN_BOUNDARY /
                                             Mcd_osd_blk_size;
    char                      * data_buf = NULL;
    char                      * buf;
    void                      * context =
        mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_FRMT );
    uint32_t                    seg_list_blks;
    uint64_t                    total_blks;
    uint64_t                    max_table_blks;
    uint64_t                    max_log_blks;
    uint64_t                    max_table_pad;
    uint64_t                    max_log_pad;
    uint64_t                    max_md_blks;
    uint64_t                    act_log_blks;
    uint64_t                    blk_offset;
    uint64_t                    reserved_blks;
    uint64_t                    reserved_segs;
    uint64_t                    seg_list_size;
    uint64_t                    seg_list_offset;
    uint64_t                    base_class_offset;
    uint64_t                    blob_offset;
    uint64_t                    seg_count;
    mcd_rec_list_block_t      * seg_list;
    mcd_rec_shard_t           * pshard = NULL;
    mcd_rec_class_t           * pclass;
    mcd_rec_ckpt_t            * ckpt;
    mcd_rec_properties_t      * prop;
    mcd_rec_superblock_t      * sb = &Mcd_rec_superblock;
    mcd_rec_flash_t           * fd = sb->flash_desc;

    mcd_log_msg( 20065, PLAT_LOG_LEVEL_DEBUG,
                 "ENTERING, shardID=%lu", shard_id );

    // format properties for non-persistent shards
    if ( FLASH_SHARD_INIT_PERSISTENCE_NO ==
         (flags & FLASH_SHARD_INIT_PERSISTENCE_MASK) ) {
        goto format_properties;
    }

    // --------------------------------------------
    // Create metadata areas for the new shard
    // --------------------------------------------

    // find max metadata size
    seg_list_size     = shard->total_segments * sizeof( uint64_t );
    seg_list_blks     = ( ( shard->total_segments +
                            MCD_REC_LIST_ITEMS_PER_BLK - 1 ) /
                          MCD_REC_LIST_ITEMS_PER_BLK );

    seg_list_offset   = ( (sizeof( mcd_rec_shard_t ) + Mcd_osd_blk_size - 1) /
                          Mcd_osd_blk_size );
    base_class_offset = seg_list_offset + seg_list_blks;
    plat_assert( ( (sizeof( mcd_rec_class_t ) + Mcd_osd_blk_size - 1) /
                   Mcd_osd_blk_size ) == 1 );

    blob_offset       = ( base_class_offset +
                          (MCD_OSD_MAX_NCLASSES * (1 + seg_list_blks)) );
    plat_assert( ( (MCD_METABLOB_MAX_LEN + Mcd_osd_blk_size - 1) /
                   Mcd_osd_blk_size ) == 1 );

    plat_assert( ( (sizeof( mcd_rec_ckpt_t ) + Mcd_osd_blk_size - 1) /
                   Mcd_osd_blk_size ) == 1 );

    max_md_blks       = ( 1 +                              // shard desc
                          seg_list_blks +                  // map blocks
                          ( MCD_OSD_MAX_NCLASSES *         // class desc
                            ( 1 + seg_list_blks ) ) +      //   + seg addrs
                          1 +                              // blob store
                          1 );                             // ckpt record

    // round up to align object table
    max_md_blks = ( ( (max_md_blks + align_blks - 1) / align_blks ) *
                    align_blks );

    plat_assert( max_md_blks <= Mcd_osd_segment_blks );

    // calculate size of recovery object table
    total_blks     = shard->total_segments * Mcd_osd_segment_blks;
    obj_per_blk    = Mcd_osd_blk_size / sizeof( mcd_rec_flash_object_t );
    max_table_blks = ( total_blks + obj_per_blk - 1 ) / obj_per_blk;

    // round up to align logs
    max_table_pad = ( ( (max_table_blks + align_blks - 1) / align_blks ) *
                      align_blks ) - max_table_blks;

    // pick default log size to be one-eighth the size of the table
    max_log_blks = max_table_blks >> 3;
    act_log_blks = max_log_blks;

    // override log size with specified size
    if ( flash_settings.rec_log_size_factor != 0 ) {
        act_log_blks = power_of_two_roundup( max_log_blks *
                                             flash_settings.rec_log_size_factor );
        if ( act_log_blks < MCD_REC_LOGBUF_BLKS ) {
            act_log_blks = MCD_REC_LOGBUF_BLKS;
        }
        mcd_log_msg( 40109, PLAT_LOG_LEVEL_WARN,
                     "%screasing log size from %lu to %lu blocks",
                     flash_settings.rec_log_size_factor < 1 ? "de" : "in",
                     max_log_blks, act_log_blks );
        if ( act_log_blks > max_log_blks ) {
            max_log_blks = act_log_blks;
        }
    }

    // pad size of logs to an alignment boundary
    max_log_pad = ( ( (max_log_blks + align_blks - 1) / align_blks ) *
                    align_blks ) - max_log_blks;
    if ( act_log_blks < max_log_blks ) {
        max_log_pad += (max_log_blks - act_log_blks);
    }

    // calculate total reserved blocks to format
    reserved_blks = ( max_md_blks +                           // metadata
                      max_table_blks +                        // recovery table
                      max_table_pad +                         // table padding
                      ( MCD_REC_NUM_LOGS * (act_log_blks +    // two logs
                                            max_log_pad) ) ); // log padding

    // convert to segments
    reserved_segs = ( ( reserved_blks + Mcd_osd_segment_blks - 1 ) /
                      Mcd_osd_segment_blks );
    plat_assert_always( reserved_segs < shard->total_segments );

    // get aligned buffer
    data_buf = plat_alloc( buf_size + Mcd_osd_blk_size );
    if ( data_buf == NULL ) {
        mcd_log_msg( 20561, PLAT_LOG_LEVEL_ERROR, "failed to allocate buffer" );
        return FLASH_ENOMEM;
    }
    buf = (char *)( ( (uint64_t)data_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

    plat_assert_always( max_md_blks * Mcd_osd_blk_size <= buf_size );

    // clear the buffer
    memset( buf, 0, buf_size );

    // format metadata, recovery table and recovery logs
    for ( s = 0; s < reserved_segs; s++ ) {

        blk_offset = shard->segments[ s ];
        buf_blks   = buf_size / Mcd_osd_blk_size;

        if ( s == reserved_segs - 1 ||
             reserved_segs <= 8 ||
             (reserved_segs >= 8 && (s % (reserved_segs / 8) == 0)) ) {
            mcd_log_msg( 20562, PLAT_LOG_LEVEL_INFO,
                         "formatting shard id=%lu segment %d of %lu, "
                         "offset=%lu",
                         shard_id, s+1, reserved_segs, blk_offset );
        }

        for ( b = 0;
              b < Mcd_osd_segment_blks;
              b += buf_blks ) {

            if ( ( Mcd_osd_segment_blks - b  ) < buf_blks ) {
                buf_blks = Mcd_osd_segment_blks - b;
            }

            rc = mcd_fth_aio_blk_write( context,
                                        buf,
                                        (blk_offset + b) * Mcd_osd_blk_size,
                                        buf_blks * Mcd_osd_blk_size );
            if ( FLASH_EOK != rc ) {
                mcd_log_msg( 20563, PLAT_LOG_LEVEL_ERROR,
                             "failed to format shard, shardID=%lu, "
                             "blk_offset=%lu, b=%d, count=%d, rc=%d",
                             shard_id, blk_offset, b, buf_blks, rc );
                plat_free( data_buf );
                return rc;
            }
        }
    }

    // install shard descriptor in buffer
    pshard                  = (mcd_rec_shard_t *)buf;
    pshard->eye_catcher     = MCD_REC_SHARD_EYE_CATCHER;
    pshard->version         = MCD_REC_SHARD_VERSION;
    pshard->r1              = 0;
    pshard->r2              = 0;
    pshard->map_blks        = seg_list_blks;
    pshard->flags           = flags;
    pshard->obj_quota       = max_objs;
    pshard->quota           = quota;
    pshard->shard_id        = shard_id;
    pshard->blk_offset      = shard->segments[ 0 ];
    pshard->blob_offset     = blob_offset;
    pshard->seg_list_offset = seg_list_offset;
    pshard->total_blks      = total_blks;
    pshard->rec_md_blks     = max_md_blks;
    pshard->rec_table_blks  = max_table_blks;
    pshard->rec_table_pad   = max_table_pad;
    pshard->rec_log_blks    = act_log_blks;
    pshard->rec_log_pad     = max_log_pad;
    pshard->reserved_blks   = reserved_blks;
    pshard->checksum        = 0;

    // install the segment mapping table
    seg_count = 0;
    for ( b = 0; b < seg_list_blks; b++ ) {

        seg_list = (mcd_rec_list_block_t *)
            (buf + ((pshard->seg_list_offset + b) * Mcd_osd_blk_size));

        for ( s = 0;
              s < MCD_REC_LIST_ITEMS_PER_BLK &&
                  seg_count < shard->total_segments;
              s++ ) {
            // install segments in this block
            seg_list->data[ s ] = shard->segments[ seg_count++ ];
        }

        // install checksum in this block
        seg_list->checksum = 0;
        seg_list->checksum = hash( (unsigned char *)seg_list,
                                   Mcd_osd_blk_size,
                                   0 );
    }

    // install all class descriptors for this shard
    for ( c = 0, blksize = 1; c < MCD_OSD_MAX_NCLASSES; c++, blksize *= 2 ) {

        // calculate offset in blocks from beginning of shard desc
        pshard->class_offset[ c ] = (base_class_offset +
                                     (c * (seg_list_offset + seg_list_blks)));

        // install class descriptor in buffer
        pclass = (mcd_rec_class_t *)
            ( buf + (pshard->class_offset[ c ] * Mcd_osd_blk_size) );
        pclass->eye_catcher     = MCD_REC_CLASS_EYE_CATCHER;
        pclass->version         = MCD_REC_CLASS_VERSION;
        pclass->r1              = 0;
        pclass->slab_blksize    = blksize;
        pclass->seg_list_offset = seg_list_offset;
        pclass->checksum        = 0;
        pclass->checksum        = hash( (unsigned char *)pclass,
                                        Mcd_osd_blk_size,
                                        MCD_REC_CLASS_EYE_CATCHER );

        // install class segment list (empty)
        for ( b = 0; b < seg_list_blks; b++ ) {
            seg_list = (mcd_rec_list_block_t *)
                (buf + ( (pshard->class_offset[ c ] +
                          pclass->seg_list_offset +  b) * Mcd_osd_blk_size ));
            seg_list->checksum = 0;
            seg_list->checksum = hash( (unsigned char *)seg_list,
                                       Mcd_osd_blk_size,
                                       0 );
        }
    }

    // install checksum in shard desc now that class offsets are in place
    pshard->checksum = hash( (unsigned char *)pshard,
                             Mcd_osd_blk_size,
                             MCD_REC_SHARD_EYE_CATCHER );

    // calculate offset to checkpoint record (last metadata block)
    ckpt = (mcd_rec_ckpt_t *)
        ( buf + ( (max_md_blks - 1) * Mcd_osd_blk_size ) );

    // install checkpoint record in buffer
    ckpt->eye_catcher = MCD_REC_CKPT_EYE_CATCHER;
    ckpt->version     = MCD_REC_CKPT_VERSION;
    ckpt->r1          = 0;
    ckpt->LSN         = 0;
    ckpt->checksum    = 0;
    ckpt->checksum    = hash( (unsigned char *)ckpt,
                              Mcd_osd_blk_size,
                              MCD_REC_CKPT_EYE_CATCHER );

    // write shard recovery metadata
    rc = mcd_fth_aio_blk_write( context,
                                buf,
                                pshard->blk_offset * Mcd_osd_blk_size,
                                max_md_blks * Mcd_osd_blk_size );
    if ( FLASH_EOK != rc ) {
        mcd_log_msg( 20564, PLAT_LOG_LEVEL_ERROR,
                     "failed to format shard metadata, shardID=%lu, "
                     "blk_offset=%lu, count=%lu, rc=%d",
                     shard_id, pshard->blk_offset, max_md_blks, rc );
        plat_free( data_buf );
        return rc;
    }

    // allocate new persistent shard descriptor
    pshard = plat_alloc( sizeof( mcd_rec_shard_t ) );
    if ( pshard == NULL ) {
        mcd_log_msg( 20565, PLAT_LOG_LEVEL_ERROR, "cannot alloc pshard" );
        plat_free( data_buf );
        return FLASH_ENOMEM;
    }
    shard->ps_alloc += sizeof( mcd_rec_shard_t );

    // cache the persistent shard descriptor
    memcpy( pshard, buf, sizeof( mcd_rec_shard_t ) );

    // initialize fields in volatile shard struct
    shard->pshard          = pshard;
    shard->total_segments  = shard->total_segments - reserved_segs;
    shard->data_blk_offset = reserved_segs * Mcd_osd_segment_blks;

    plat_free( data_buf );

    mcd_log_msg( 20566, PLAT_LOG_LEVEL_INFO,
                 "formatted persistent shard, id=%lu, blk_offset=%lu, "
                 "md_blks=%lu, table_blks=%lu, pad=%lu, log_blks=%lu, "
                 "pad=%lu (%lu entries)",
                 pshard->shard_id, pshard->blk_offset, pshard->rec_md_blks,
                 pshard->rec_table_blks, pshard->rec_table_pad,
                 pshard->rec_log_blks, pshard->rec_log_pad,
                 pshard->rec_log_blks * MCD_REC_LOG_BLK_RECS );

    // --------------------------------------------
    // Now update the superblock with the new shard
    // --------------------------------------------

 format_properties:

    // find an empty properties slot
    for ( slot = 0; slot < fd->max_shards; slot++ ) {
        if ( sb->props[ slot ]->shard_id == 0 ) {
            break;
        }
    }
    if ( slot == fd->max_shards ) {
        mcd_log_msg( 20567, PLAT_LOG_LEVEL_ERROR,
                     "can't find empty prop slot shardID=%lu", shard->id );
        return FLASH_ENOENT;  // shouldn't happen
    }

    // install the basic shard properties
    prop                = sb->props[ slot ];
    prop->eye_catcher   = MCD_REC_PROP_EYE_CATCHER;
    prop->version       = MCD_REC_PROP_VERSION;
    prop->write_version = 1;
    prop->shard_id      = shard_id;

    // set block offset and persistence flag for persistent shards
    // so they will be found during recovery init
    if ( FLASH_SHARD_INIT_PERSISTENCE_YES ==
         (flags & FLASH_SHARD_INIT_PERSISTENCE_MASK) ) {
        prop->blk_offset = pshard->blk_offset;
        prop->persistent = 1;
    } else {
        prop->blk_offset = 0xffffffffffffffffull;
        prop->persistent = 0;
    }

    prop->checksum = hash( (unsigned char *)prop,
                           Mcd_osd_blk_size,
                           MCD_REC_PROP_EYE_CATCHER );

    // write shard properties
    rc = write_property( slot );
    if ( FLASH_EOK != rc ) {
        mcd_log_msg( 20568, PLAT_LOG_LEVEL_ERROR,
                     "failed to write shard props, rc=%d", rc );
        return rc;
    }

    // sync all devices
    rc = mcd_aio_sync_devices();

    return rc;
}

int
shard_unformat( uint64_t shard_id )
{
    int                         rc;
    int                         slot;
    mcd_rec_superblock_t      * sb = &Mcd_rec_superblock;

    mcd_log_msg( 20065, PLAT_LOG_LEVEL_DEBUG,
                 "ENTERING, shardID=%lu", shard_id );

    // find shard in shard properties list
    slot = find_properties( shard_id );
    if ( slot < 0 ) {
        return 0;
    }

    // zero-out the entire block
    memset( sb->props[ slot ], 0, Mcd_osd_blk_size );

    // write the property block
    rc = write_property( slot );
    if ( rc != 0 ) {
        mcd_log_msg( 20469, PLAT_LOG_LEVEL_ERROR,
                     "write property failed, rc=%d", rc );
        return rc;
    }

    // sync all devices
    rc = mcd_aio_sync_devices();

    return rc;
}

int
flash_format ( uint64_t total_size )
{
    int                         rc;
    int                         reserved_blks;
    int                         buf_size = MCD_REC_FORMAT_BUFSIZE;
    int                         fd_blks;
    int                         blob_blks;
    int                         prop_blks;
    uint64_t                    prop_offset;
    uint64_t                    blob_offset;
    uint64_t                    fd_offset;
    char                      * data_buf = NULL;
    char                      * buf;
    uint64_t                    blk_offset;
    mcd_rec_flash_t           * fd;
    fthWaitEl_t               * wait;

    mcd_log_msg( 20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    // initialize
    do_aio_init( 0 );

    // get a segment for the superblock
    wait = fthLock( &Mcd_osd_segment_lock, 1, NULL );
    blk_offset = Mcd_osd_free_segments[ --Mcd_osd_free_seg_curr ];
    fthUnlock( wait );

    // calculate block offsets and sizes for persistent structures
    fd_offset     = blk_offset + MCD_REC_LABEL_BLKS;
    fd_blks       = ( (sizeof( mcd_rec_flash_t ) + Mcd_osd_blk_size - 1) /
                      Mcd_osd_blk_size );

    blob_offset   = fd_blks;               // origin is start of flash desc
    blob_blks     = ( (MCD_METABLOB_MAX_LEN + Mcd_osd_blk_size - 1) /
                      Mcd_osd_blk_size );

    prop_offset   = blob_offset + blob_blks;
    prop_blks     = MCD_REC_MAX_SHARDS;    // max supported

    // total blocks to format
    reserved_blks = ( MCD_REC_LABEL_BLKS + // volume label
                      fd_blks +            // flash descriptor
                      blob_blks +          // "global" blob store
                      prop_blks );         // shard properties

    // get aligned buffer
    data_buf = plat_alloc( buf_size + Mcd_osd_blk_size );
    if ( data_buf == NULL ) {
        mcd_log_msg( 20561, PLAT_LOG_LEVEL_ERROR,
                     "failed to allocate buffer" );
        return FLASH_ENOMEM;
    }
    buf = (char *)( ( (uint64_t)data_buf + Mcd_osd_blk_size - 1 ) &
                    Mcd_osd_blk_mask );

    plat_assert_always( reserved_blks * Mcd_osd_blk_size <= buf_size );

    // clear the buffer
    memset( buf, 0, reserved_blks * Mcd_osd_blk_size );

    mcd_log_msg( 20569, PLAT_LOG_LEVEL_INFO,
                 "formatting reserved areas, offset=%lu, blks=%d",
                 blk_offset, reserved_blks );

    // format reserved blocks in first segment on each device
    rc = write_superblock( buf, reserved_blks, blk_offset );
    if ( FLASH_EOK != rc ) {
        mcd_log_msg( 20570, PLAT_LOG_LEVEL_ERROR,
                     "failed to format reserved areas, rc=%d", rc );
        return rc;
    }

    // write label on each volume
    if ( !Mcd_aio_raid_device ) {
        rc = write_label( buf, MCD_REC_LABEL_BLKS, blk_offset );
        if ( rc != 0 ) {
            mcd_log_msg( 20571, PLAT_LOG_LEVEL_ERROR,
                         "failed to write label, rc=%d", rc );
            return rc;
        }
    }

    // install flash descriptor in buffer
    fd                = (mcd_rec_flash_t *)buf;
    fd->eye_catcher   = MCD_REC_FLASH_EYE_CATCHER;
    fd->version       = MCD_REC_FLASH_VERSION;
    fd->r1            = 0;
    fd->write_version = 1;
    fd->blk_size      = MCD_OSD_BLK_SIZE;
    fd->max_shards    = MCD_OSD_MAX_NUM_SHARDS;
    fd->reserved_blks = reserved_blks;
    fd->blob_offset   = blob_offset;
    fd->prop_offset   = prop_offset;
    fd->blk_offset    = fd_offset;
    fd->total_blks    = total_size / Mcd_osd_blk_size;
    fd->checksum      = 0;
    fd->checksum      = hash( (unsigned char *)buf,
                              Mcd_osd_blk_size,
                              MCD_REC_FLASH_EYE_CATCHER );

    // write superblock on each device
    rc = write_superblock( buf, 1, fd_offset );

    // sync all devices
    if ( rc == 0 ) {
        rc = mcd_aio_sync_devices();
    }

    plat_free( data_buf );

    // mark superblock formatted
    Mcd_rec_superblock_formatted = 1;

    return rc;
}

int
mcd_rec_attach_test( int testcmd, int test_arg )
{
    switch ( testcmd ) {
    case 1:                              // start test
        Mcd_rec_attach_test_running = 1;
        fthMboxInit( &Mcd_rec_attach_test_mbox_special );
        fthMboxInit( &Mcd_rec_attach_test_mbox );
        fthMboxInit( &Mcd_rec_attach_test_updated_mbox );
        // signal each updater thread
        for ( int i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
            if ( Mcd_containers[ i ].tcp_port != 0 ) {
                mcd_osd_shard_t * shard =
                    (mcd_osd_shard_t *)Mcd_containers[ i ].shard;
                Mcd_rec_attach_test_update_mail[ i ].log  = 0;
                Mcd_rec_attach_test_update_mail[ i ].cntr = shard->cntr;
                Mcd_rec_attach_test_update_mail[ i ].in_recovery  = 0;
                Mcd_rec_attach_test_update_mail[ i ].updated_sem  = NULL;
                Mcd_rec_attach_test_update_mail[ i ].updated_mbox =
                    &Mcd_rec_attach_test_updated_mbox;
                fthMboxPost( &shard->log->update_mbox,
                             (uint64_t)(&Mcd_rec_attach_test_update_mail[i]) );
                if ( shard->total_segments > 32 ) {   // >1GB container
                    Mcd_rec_attach_test_waiters_special++;
                } else {
                    Mcd_rec_attach_test_waiters++;
                }
            }
        }
        break;

    case 2:                              // stop test
        if ( !Mcd_rec_attach_test_running ) {
            return 1;
        }
        // ensure no threads still stuck
        while ( Mcd_rec_attach_test_waiters_special > 0 ) {
            fthMboxPost( &Mcd_rec_attach_test_mbox_special, 0 );
            fthMboxWait( &Mcd_rec_attach_test_updated_mbox );
            Mcd_rec_attach_test_waiters_special--;
        }
        for ( int s = 0; s < Mcd_rec_attach_test_waiters; s++ ) {
            fthMboxPost( &Mcd_rec_attach_test_mbox, 0 );
        }
        while ( Mcd_rec_attach_test_waiters > 0 ) {
            fthMboxWait( &Mcd_rec_attach_test_updated_mbox );
            Mcd_rec_attach_test_waiters--;
        }
        Mcd_rec_attach_test_running = 0;
        break;

    case 3:                              // release one thread
        // if thread == 0, then first container;
        //                 otherwise it doesn't matter which one
        if ( !Mcd_rec_attach_test_running ) {
            return 1;
        }
        if ( test_arg == 0 ) {
            Mcd_rec_attach_test_waiters_special--;
            fthMboxPost( &Mcd_rec_attach_test_mbox_special, 0 );
        } else {
            Mcd_rec_attach_test_waiters--;
            fthMboxPost( &Mcd_rec_attach_test_mbox, 0 );
        }
        fthMboxWait( &Mcd_rec_attach_test_updated_mbox );
        break;

    default:                             // unknown command
        return 2;
    }

    return 0;
}
