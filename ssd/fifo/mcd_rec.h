/*
 * File:   apps/memcached/server/memcached-1.2.5-schooner/mcd_rec.h
 * Author: Wayne Hineman
 *
 * Created on May 11, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_rec.h 
 */

#ifndef __MCD_REC_H__
#define __MCD_REC_H__

#define KILOBYTE  (1024)
#define MEGABYTE  (1024 * 1024)
#define GIGABYTE  (1024 * 1024 * 1024ULL)

#define MCD_REC_ALIGN_BOUNDARY   MEGABYTE

#define MCD_REC_UPDATE_BUFSIZE_CHICKEN      (256 * MEGABYTE)
#define MCD_REC_UPDATE_SEGMENT_SIZE_CHICKEN MCD_REC_UPDATE_BUFSIZE_CHICKEN
#define MCD_REC_UPDATE_SEGMENT_BLKS_CHICKEN                     \
    (MCD_REC_UPDATE_SEGMENT_SIZE_CHICKEN / Mcd_osd_blk_size)

#define MCD_REC_UPDATE_YIELD        128  // from empirical data
#define MCD_REC_UPDATE_MAX_CHUNKS   64

/*********************************************************************
 *   IMPORTANT NOTE!!!!!   2/15/11  Brian O'Krafka
 *
 *   The value of MCD_REC_UPDATE_BUFSIZE is related to the maximum 
 *   amount of flash that is supported by membrain.
 *
 *   In particular:
 *
 *   MCD_REC_UPDATE_BUFSIZE must be >= (total_flash_size_in_512B_blocks)*16/64
 *
 *   This is because:
 *    - A buffer of this size is used to hold a portion of the in-flash 
 *      object table when the updater_thread updates it via the logs.
 *    - For large containers, it buffers the object table in 64 "chunks"
 *      (MCD_REC_UPDATE_MAX_CHUNKS).  
 *    - This buffer must therefore be able to hold 1/64 of an object table
 *      for the maximum number of objects  that can be held by the 
 *      largest possible container.
 *    - Since an object table entry uses 16B per object, and the smallest 
 *      possible object uses a minimum of one block (512B), we get the formula
 *      above.
 * 
 *   The old value of MCD_REC_UPDATE_BUFSIZE (256MB) supported up to 512GB
 *   of flash.
 *   The new value of MCD_REC_UPDATE_BUFSIZE (1GB) supports up to 2TB of flash.
 * 
 *********************************************************************/
// #define MCD_REC_UPDATE_BUFSIZE      (256 * MEGABYTE)
#define MCD_REC_UPDATE_BUFSIZE      (1024 * MEGABYTE)
#define MCD_REC_UPDATE_SEGMENT_SIZE (1 * MEGABYTE)
#define MCD_REC_UPDATE_SEGMENT_BLKS (MCD_REC_UPDATE_SEGMENT_SIZE /      \
                                     MCD_OSD_META_BLK_SIZE)

#define MCD_REC_LOG_SEGMENT_SIZE (1 * MEGABYTE)
#define MCD_REC_LOG_SEGMENT_BLKS (MCD_REC_LOG_SEGMENT_SIZE / MCD_OSD_META_BLK_SIZE)

#define MCD_REC_UPDATE_IOSIZE    (1 * MEGABYTE)   // <= upd seg size, power 2
#define MCD_REC_UPDATE_LGIOSIZE  (1 * MEGABYTE)   // <= log seg size, power 2
#define MCD_REC_FORMAT_BUFSIZE   (32 * MEGABYTE) //EF: need to write full segment for 655xx shards */

#define MCD_REC_NUM_LOGS         2

#define MCD_REC_NUM_LOGBUFS      2
#define MCD_REC_LOGBUF_SIZE      (16 * KILOBYTE)

#define MCD_REC_LOGBUF_BLKS      (MCD_REC_LOGBUF_SIZE / MCD_OSD_META_BLK_SIZE)
#define MCD_REC_LOGBUF_SLOTS     (MCD_REC_LOGBUF_SIZE /                 \
                                  sizeof( mcd_logrec_object_t ))
#define MCD_REC_LOGBUF_RECS      (MCD_REC_LOGBUF_SLOTS - MCD_REC_LOGBUF_BLKS)
#define MCD_REC_LOGBUF_BLK_SLOTS (MCD_OSD_META_BLK_SIZE/                    \
                                  sizeof( mcd_logrec_object_t ))

#define MCD_REC_LOG_BLK_SLOTS    MCD_REC_LOGBUF_BLK_SLOTS
#define MCD_REC_LOG_BLK_RECS     (MCD_REC_LOG_BLK_SLOTS - 1)

#define MCD_REC_LIST_ITEMS_PER_BLK ((MCD_OSD_BLK_SIZE_MAX/ sizeof(uint64_t)) - 1)

// -----------------------------------------------------
//    Persistent recovery structures
// -----------------------------------------------------
//
// Logical layout in flash (in 512-byte blocks):
//
// Superblock:
//
//  This unusual arrangement actually writes a copy of the superblock
//  and shard anchors at the beginning of each of 8 physical devices
//  by exploiting knowledge of the underlying striping algorithm (i.e.
//  by breaking code encapsulation). When run on RAID devices, the
//  superblock is written only to the first 3 "devices" (partitions on
//  the raid device). All blocks after are unused, up to block 65536.
//
//  Block     Count      Contents
//  -----     -----      --------
//  0         8          reserved (label)
//  8         1          mcd_rec_flash (flash descriptor)
//  9         1          mcd_rec_blob ("global" blob)
//  10        129        mcd_rec_properties (props for cmc & 128 containers)
//  139       1919       reserved (end of superblock copy)
//  2058      6134       unused
//  8192      8          reserved (label)
//  8200      1          mcd_rec_flash (flash descriptor)
//  8201      1          mcd_rec_blob ("global" blob)
//  8202      129        mcd_rec_properties (container properties)
//  8331      1919       reserved (end of superblock copy)
//  10250     6134       unused
//  ...                  (same every 8192 blocks)
//
//  57344     8          reserved (label)
//  57352     1          mcd_rec_flash (flash descriptor)
//  57353     1          mcd_rec_blob
//  57354     129        mcd_rec_properties
//  57483     1919       reserved (end of superblock copy)
//  59402     6134       unused
//
//  65536     ...        first shard
//
//
// Shard Layout:
//
//  Shard size 1GB = 32 segments
//  Metadata uses 2 segments
//
//  Block     Count     Contents
//  -----     -----     --------
//  0         1         mcd_rec_shard (first shard descriptor)
//                      including block offsets to MCD_OSD_MAX_NCLASSES
//                      number of class descriptors
//  1         1         segment address mapping table (block count is map_blks)
//                      (Note: number of blocks depends on shard size)
//  2         1         mcd_rec_class (first class descriptor)
//  3         1         relative block offsets, segments with slab size 512
//                      (Note: number of blocks depends on shard size)
//  4         1         mcd_rec_class (second class descriptor)
//  5         1         relative block offsets, segments with slab size 1024
//  ...                 (same for all 16 classes)
//  34        1         blob store
//  35        2012      unused (alignment)
//  2047      1         checkpoint record for shard
//  2048                Note: count of blocks to here is rec_md_blks
//
//  0x800     0x10000   recovery object table (block count is rec_table_blks)
//  0x10800   0x2000    recovery log 1 (block count is rec_log_blks)
//  0x12800   0x2000    recovery log 2
//  0x14800             Note: count of blocks to here is reserved_blks
//  0x14800   0xb800    unused
//
//  0x20000             actual data (starts on segment boundary)
//
//
// Shard Layout:
//
//  Shard size 512GB = 16384 segments (theoretical maximum for 64GB SSDs):
//  Metadata uses 641 segments
//
//  Block     Count     Contents
//  -----     -----     --------
//  0         1         mcd_rec_shard (first shard descriptor)
//                      including block offsets to MCD_OSD_MAX_NCLASSES
//                      number of class descriptors
//  1         211       segment address mapping table (block count is map_blks)
//                      (Note: number of blocks depends on shard size)
//  212       1         mcd_rec_class (first class descriptor)
//  213       211       relative block offsets, segments with slab size 512
//                      (Note: number of blocks depends on shard size)
//  424       1         mcd_rec_class (second class descriptor)
//  425       211       relative block offsets, segments with slab size 1024
//  ...                 (same for all 16 classes)
//  3604      1         blob store
//  3605      490       unused (alignment)
//  4095      1         checkpoint record for shard
//                      Note: count of blocks to here is rec_md_blks
//
//  0x1000    0x2000000 recovery object table (block count is rec_table_blks)
//  0x2001000 0x400000  recovery log 1 (block count is rec_log_blks)
//  0x2401000 0x400000  recovery log 2
//  0x2801000           Note: count of blocks to here is reserved_blks
//  0x2801000 0xf000    unused
//
//  0x2810000           actual data (starts on segment boundary)
// -----------------------------------------------------

// Persistent flash subsystem descriptor.
// Written to a known location in flash (LBA 0).
typedef struct mcd_rec_flash {
    uint32_t        eye_catcher;       // eye-catching magic number
    uint16_t        version;           // version number of this structure
    uint16_t        r1;                // alignment
    uint64_t        checksum;          // checksum of flash descriptor
    uint32_t        write_version;     // incremented on each write
    uint32_t        blk_size;          // block size
    uint32_t        max_shards;        // maximum number of shards
    uint32_t        reserved_blks;     // total size (blocks) of reserved area
    uint64_t        blob_offset;       // relative block offset to blob store
    uint64_t        prop_offset;       // relative block offset to properties
    uint64_t        blk_offset;        // offset of flash descriptor
    uint64_t        total_blks;        // size of flash in 512-byte blocks
    uint64_t        reserved[ 8 ];     // -- reserved for future use --
} mcd_rec_flash_t;

enum {
    MCD_REC_FLASH_VERSION     = 1,
    MCD_REC_FLASH_EYE_CATCHER = 0x48534c46, // "FLSH" (in little endian)
};

// Persistent shard descriptor.
// Written at the beginning of each shard.
typedef struct mcd_rec_shard {
    uint32_t        eye_catcher;       // eye-catching magic number
    uint16_t        version;           // version number of this structure
    uint16_t        r1;                // alignment
    uint64_t        checksum;          // checksum of shard descriptor
    uint32_t        r2;                // alignment
    uint32_t        map_blks;          // number of address map blocks; also
                                       // number of seg addr blocks for classes
    uint32_t        flags;             // flags from shard create
    uint32_t        obj_quota;         // object quota from shard create
    uint64_t        quota;             // size quota from shard create
    uint64_t        shard_id;          // shard ID
    uint64_t        blk_offset;        // block offset from origin (phys addr)
    uint64_t        total_blks;        // number of 512-byte blocks in shard
    uint64_t        blob_offset;       // relative block offset to blob store
    uint64_t        seg_list_offset;   // relative block offset to segment list
    uint64_t        rec_md_blks;       // recovery metadata blocks
    uint64_t        rec_table_blks;    // recovery object table size (blocks)
    uint64_t        rec_table_pad;     // pad blocks to next alignment boundary
    uint64_t        rec_log_blks;      // single recovery log size (blocks)
    uint64_t        rec_log_pad;       // pad blocks to next alignment boundary
    uint64_t        reserved_blks;     // total size (blocks) of reserved area
    uint64_t        class_offset[ MCD_OSD_MAX_NCLASSES ]; // relative offset
                                                          //   to class desc
                                                          //   from shard
    uint64_t        reserved[ 8 ];     // -- reserved for future use --
} mcd_rec_shard_t;

enum {
    MCD_REC_SHARD_VERSION     = 1,
    MCD_REC_SHARD_EYE_CATCHER = 0x44524853, // "SHRD"
};

// Persistent slab class descriptor.
// Written on block boundaries following Shard descriptor.
// This is kind of content free, but it does provide a delimiter in
// what would otherwise be back-to-back segment address tables.
typedef struct mcd_rec_class {
    uint32_t        eye_catcher;       // eye-catching magic number
    uint16_t        version;           // version number of this structure
    uint16_t        r1;                // alignment
    uint64_t        checksum;          // checksum of class record
    uint64_t        seg_list_offset;   // relative block offset to segment list
    uint32_t        slab_blksize;      // block size for class (power of 2)
    uint32_t        r2;                // alignment
    uint64_t        reserved[ 8 ];     // -- reserved for future use --
} mcd_rec_class_t;

enum {
    MCD_REC_CLASS_VERSION     = 1,
    MCD_REC_CLASS_EYE_CATCHER = 0x53414c43, // "CLAS"
};

// "Checkpoint" record
// Lives at the start of each shard's Recovery Object Table. Updated during
// normal operation when a complete log has been applied and all updates are
// written to flash; value will be the LSN of the last log page applied
// (i.e. the last page of the log). Updated during recovery when all updates
// are written to flash; value will be LSN of last log page applied
// (i.e. likely not to be the last page of the log).
typedef struct mcd_rec_ckpt {
    uint32_t        eye_catcher;       // eye-catching magic number
    uint16_t        version;           // This version number covers the flash
                                       // object table and related structs.
    uint16_t        r1;                // alignment
    uint64_t        checksum;          // checksum of checkpoint record
    uint64_t        LSN;               // "Log Sequence Number". LSN of the
                                       // last log page applied in its entirety
                                       // to the persistent flash object table.
    uint64_t        reserved[ 8 ];     // -- reserved for future use --
} mcd_rec_ckpt_t;

enum {
    MCD_REC_CKPT_VERSION     = 1,
    MCD_REC_CKPT_EYE_CATCHER = 0x54504b43, // "CKPT" (in little endian)
};


// "Flash Object"
// An item in the "Recovery Object Table", the persistent table of objects
// in flash maintained by applying updates from a log.
// This must divide evenly into MCD_OSD_BLK_SIZE
typedef struct mcd_rec_flash_object {
    uint16_t        syndrome;          // 16-bit syndrome
    // use some extra bits
    uint16_t        tombstone:1;       // 1=entry is a tombstone
    uint16_t        deleted:1;         // 1=marked for delete-in-future
    uint16_t        reserved:2;        // reserved
    uint16_t        blocks:12;         // number of 512-byte blocks occupied
    // ------------------
    uint32_t        bucket;            // hash bucket
    uint64_t        cntr_id:16;        // container id
    uint64_t        seqno:48;          // sequence number
} mcd_rec_flash_object_t;


// Log page header
// Lives at the beginning of each page of the log. Contains the LSN of the
// log page, which is used to find the most recent page in the log. The LSN
// from the last log page applied to the flash object table is stored in the
// checkpoint record to keep track of which updates have been applied.
typedef struct mcd_rec_log_page_header {
    uint32_t        eye_catcher;       // eye-catching magic number
    uint16_t        version;           // version number of this structure
    uint16_t        r1;                // alignment

    uint64_t        LSN;               // "Log Sequence Number". A monotone
                                       // that increments by one for each
                                       // log page written.
    uint64_t        checksum;          // checksum of filled log page
    uint64_t        pad1;              // Make sure this is the same
                                       //   size as mcd_logrec_object_t
                                       //   Also evenly divisible into
                                       //   MCD_OSD_BLK_SIZE
} mcd_rec_logpage_hdr_t;

enum {
    MCD_REC_LOGHDR_VERSION     = 1,
    MCD_REC_LOGHDR_EYE_CATCHER = 0x50474f4c, // "LOGP" (in little endian)
};


// Container metadata
typedef struct mcd_rec_properties {
    uint32_t        eye_catcher;       // eye-catching magic number
    uint16_t        version;           // version number of this structure
    uint16_t        r1;                // alignment
    uint64_t        checksum;          // checksum of properties record
    uint64_t        write_version;     // incremented with each write

    // shard basics
    uint64_t        blk_offset;        // offset of persistent shard desc
    uint64_t        shard_id;          // shard id

    // needed by the memcached protocol layer
    uint64_t        flush_time;        // "flush_all" time (may be in future)
    uint64_t        cas_id;            // cas version number

    // container configuration
    uint64_t        size_quota;        // size of container
    uint32_t        obj_quota;         // max number of objects
    uint32_t        state;             // container state, 'running', etc
    uint32_t        tcp_port;          // listen tcp port
    uint32_t        udp_port;          // listen udp port
    uint32_t        eviction;          // 1=cache mode, 0=store mode
    uint32_t        persistent;        // 1=persistent, 0=non-persistent
    uint32_t        container_id;      // unique container id
    uint32_t        sync_updates;      // updates between syncs (persistent)
    uint32_t        sync_msec;         // msecs between syncs (not used)

    uint32_t        r2:29;             // alignment
    uint32_t        sync_backup:1;     // 1=sync_all before backup; 0=don't
    uint32_t        sasl:1;            // 1=SASL-enabled
    uint32_t        prefix_delete:1;   // 1=prefix_delete-enabled

    char            cname[64];         // container name
    char            cluster_name[64];  // cluster name

    // container IPs
    uint32_t        num_ips;           // number of ip addrs used
    uint32_t        ip_addrs[ MCD_CNTR_MAX_NUM_IPS ]; // ip addrs

    uint64_t        reserved[ 8 ];     // -- reserved for future use --
} mcd_rec_properties_t;

enum {
    MCD_REC_PROP_VERSION     = 2,
    MCD_REC_PROP_VERSION_V2  = 2,
    MCD_REC_PROP_VERSION_V1  = 1,
    MCD_REC_PROP_EYE_CATCHER = 0x504f5250, // "PROP" (in little endian)
};


// Blob structure
typedef struct mcd_rec_blob {
    uint32_t        eye_catcher;       // eye-catching magic number
    uint16_t        version;           // version number of this structure
    uint16_t        length;            // data length
    uint64_t        checksum;          // checksum of blob
    char            data[0];           // payload
} mcd_rec_blob_t;

enum {
    MCD_REC_BLOB_VERSION     = 1,
    MCD_REC_BLOB_EYE_CATCHER = 0x424f4c42, // "BLOB" (in little endian)
};


// -----------------------------------------------------
//    In-memory structures
// -----------------------------------------------------

// Tombstone
typedef struct mcd_rec_tombstone {
    struct mcd_rec_tombstone * next;     // next element pointer
    struct mcd_rec_tombstone * prev;     // previous element pointer
    uint64_t                seqno;       // sequence number of delete op
    uint32_t                blk_offset;  // logical block offset within shard
    uint16_t                syndrome;    // 16-bit syndrome
    bool                    was_malloc;  // true=tombstone was malloc'd
} mcd_rec_tombstone_t;

// Tombstone list
typedef struct mcd_rec_ts_list {
    mcd_rec_tombstone_t   * list_head;   // head of the tombstone list
    mcd_rec_tombstone_t   * list_tail;   // tail of the tombstone list
    mcd_rec_tombstone_t   * slab;        // allocation slab for tombstones
    mcd_rec_tombstone_t   * free_head;   // head of free list (from slab)
    mcd_rec_tombstone_t   * free_tail;   // tail of free list (from slab)
    int                     free_count;  // count of free tombstones
    int                     xtra_count;  // count of plat_alloc'd tombstones
    int                     curr_count;  // total tombstones in list
} mcd_rec_ts_list_t;

// Update mail
// Used during shard recovery or when a log is filled
// to signal the updater thread to apply log records to the
// recovery object table for the shard.
typedef struct mcd_rec_update {
    // static data
    mcd_container_t       * cntr;          // pointer to container, which has
                                           //   pointer to shard to update
    int                     log;           // 0=recover first log; 1=second log
    int                     in_recovery;   // 1=in recovery; 0=not in recovery

    // variable data - set with each mail
    fthSem_t              * updated_sem;   // pointer to update complete sem
    fthMbox_t             * updated_mbox;  // pointer to update complete mbox
} mcd_rec_update_t;

// Log buffer postprocess state
typedef struct mcd_rec_pp_state {
    int                     sync_recs;     // number of updates between syncs
    int                     curr_recs;     // number of recs to be sync'd
    int                     fill_count;    // recs in logbuf added to pp list
    int                     slot_count;    // slots in logbuf already pp'd
    int                     dealloc_count; // number of dealloc block addresses
    uint32_t              * dealloc_list;  // list of dealloc block addresses
} mcd_rec_pp_state_t;

// Log buffer
typedef struct mcd_rec_logbuf {
    int                     id;          // buffer id
    uint64_t                seqno;       // expected seqno of this logbuf
    uint32_t                fill_count;  // records filled in buffer
    uint32_t                sync_blks;   // blocks (within buffer) to sync
    fthSem_t                real_write_sem; // write sem for this logbuf
    fthSem_t              * write_sem;   // ptr to write sem for this logbuf
    fthSem_t              * sync_sem;    // ptr to sync sem for this logbuf
    char                  * buf;         // data buffer for this log
    mcd_logrec_object_t   * entries;     // record index into data buffer
} mcd_rec_logbuf_t;

// Recovery log
typedef struct mcd_rec_log {

    // log processing

    uint64_t                curr_LSN;      // current LSN
    uint64_t                next_fill;     // next fill slot
    uint64_t                total_slots;   // record slots in persistent log
    uint64_t                hwm_seqno;     // high seqno from most recent log
                                           //  merge (for clustering)
    uint64_t                rtg_seqno;     // retained tombstone guarantee
                                           //   (tombstone truncate seqno)
    uint64_t                write_buffer_seqno;   // where to write in log
    uint64_t                sync_completed_seqno; // last completed sync slot
    fthLock_t               sync_fill_lock;       // lock for fill/sync ops
    char                  * logbuf;        // pointer to allocate log buffer
    char                  * logbuf_base;   // aligned pointer
    fthSem_t                fill_sem;      // +indicates fill slot available
    fthSem_t                log_sem;       // +indicates log available
    mcd_rec_logbuf_t        logbufs[ MCD_REC_NUM_LOGBUFS ]; // logbuf state
    int                     started;       // 1=log writer started
    int                     shutdown;      // 1=shutdown log writer

    int                     segment_count; // number of segments
    char                 ** segments;      // array of log buffers used
                                           //   during object table merge

    // log <-> updater interaction

    int                     updater_started;  // 1=updater started
    mcd_rec_update_t        update_mail[ MCD_REC_NUM_LOGS ]; // update state
    fthMbox_t               update_mbox;      // post here to update objtable
    fthMbox_t               update_stop_mbox; // post here when updater is
                                              //   stopped for this shard
    // log buffer post-processing

    int                     pp_max_updates;   // max value for sync_recs 
    mcd_rec_pp_state_t      pp_state;         // state for pp logbuf

} mcd_rec_log_t;

// Superblock
// Anchors critical data structures.
typedef struct mcd_rec_superblock {
    mcd_rec_flash_t       * flash_desc;    // flash descr
    mcd_rec_properties_t  * props[ MCD_OSD_MAX_NUM_SHARDS ];
                                           // array of aligned prop list blocks
    char                  * sb_buf;        // ptr to buffer of superblock+props
} mcd_rec_superblock_t;

// Generic list of 64-bit integers, with a checksum
// that fits in a MCD_OSD_BLK_SIZE page.
typedef struct mcd_rec_list_block {
    uint64_t                checksum;
    uint64_t                data[MCD_REC_LIST_ITEMS_PER_BLK];
    					///data has to be more than MCD_REC_LIST_ITEMS_PER_BLK
} mcd_rec_list_block_t;

enum trx_rec {
	TRX_REC_OKAY	= 0,
	TRX_REC_NOMEM,
	TRX_REC_OVERFLOW,
	TRX_REC_BAD_SEQ
};

// Object Table State
// Used during online object table update and during recovery.
// Holds state of the object table, e.g. read/write cursor, etc.
typedef struct mcd_rec_obj_state {
    int                   in_recovery;   // 1=in recovery; 0=not in recovery
    int                   pass;          // current pass, 1 or 2
    int                   passes;        // 1 pass online; 2 passes in recovery

    int                   chunk;         // current "chunk" # of object table
    int                   num_chunks;    // total number of "chunks"
    int                   chunk_blks;    // # blks in chunk to read/write

    uint64_t              start_blk;     // blk to start read/write operation
    uint64_t              num_blks;      // # blks in object table

    uint64_t              start_obj;     // rel obj num (or blk off) in chunk
    uint64_t              num_objs;      // # objects in table chunk

    int                   seg_objects;   // # objects per segment
    int                   seg_count;     // # object table segments
    char               ** segments;      // list of object table segments

    mcd_logrec_object_t  *trxbuf;        // trx log-record accumulator
    uint                  trxmax;        // trx record maximum
    uint                  trxnum;        // trx record current count
    enum trx_rec          trxstatus;     // status of accumulation
} mcd_rec_obj_state_t;

// Log State
// Used during object table update, both online and during recovery.
// Keeps track of the state of log operations, e.g. read cursor, etc.
// When processing log during update, log is read in fixed size
// segments. Segments are allocated at startup to hold entire log.
typedef struct mcd_rec_log_state {
    int              log;                // which log 0 or 1

    uint64_t         start_blk;          // rel start blk of this log
    uint64_t         num_blks;           // # blks in this log

    uint64_t         high_LSN;           // highest LSN found in log
    uint64_t         high_obj_offset;    // highest obj offset found in log
    uint64_t         low_obj_offset;     // loweest obj offset found in log

    int              seg_cached;         // # of log segments cached
    int              seg_count;          // # log segments
    char          ** segments;           // list of log segments
} mcd_rec_log_state_t;

// Free context pointer
// This is used to keep a list of freed aio contexts since
// they can't really be freed. 
typedef struct mcd_rec_aio_ctxt {
    struct mcd_rec_aio_ctxt * next;      
    void                    * ctxt;
} mcd_rec_aio_ctxt_t;

// LRU Scan state
// Used during recovery to keep track of oldest
// item (seqno) in each slab class for a container
typedef struct mcd_rec_lru_scan {
    uint64_t                seqno;
    mcd_osd_hash_t        * hash_entry;
} mcd_rec_lru_scan_t;

// Read/Write operation
enum table_op {
    TABLE_READ  = 1,
    TABLE_WRITE = 2,
};

// -----------------------------------------------------
//    Exported functions
// -----------------------------------------------------

inline uint64_t log_get_buffer_seqno( mcd_osd_shard_t * shard );
inline uint64_t relative_log_offset( mcd_rec_shard_t * pshard, int log );
int  table_chunk_op( void * context, mcd_osd_shard_t * shard, int op,
                     uint64_t start_blk, uint64_t num_blks, char * buf );

#endif
