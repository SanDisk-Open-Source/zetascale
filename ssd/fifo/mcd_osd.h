/*
 * File:   apps/memcached/server/memcached-1.2.5-schooner/mcd_osd.h
 * Author: Xiaonan Ma
 *
 * Created on Mar 09, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_osd.h 16149 2011-02-15 16:07:23Z briano $
 */

#ifndef __MCD_OSD_H__
#define __MCD_OSD_H__


#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "ssd/ssd_local.h"
#include "ssd/fifo/mcd_osd_internal.h"
#include "ssd/fifo/mcd_aio.h"
#include "api/sdf_internal.h"
#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif /* SDFAPI */

#define mcd_log_msg(id, args...)        plat_log_msg( id, PLAT_LOG_CAT_SDF_APP_MEMCACHED, ##args )
#define mcd_dbg_msg(...)                plat_log_msg( PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_APP_MEMCACHED, __VA_ARGS__ )

// #define MCD_OSD_MAX_PTHREADS    16
#define MCD_OSD_MAX_PTHREADS    128
#define MCD_OSD_MAX_NCLASSES    16
#define MCD_OSD_MAX_COMP_HIST   16

/*
 * Block size in segment 0 (flash descriptor)
 */
#define MCD_OSD_SEG0_BLK_SIZE		512
#define MCD_OSD_SEG0_BLK_MASK		0xfffffffffffffe00ULL

/*
 * minimum allocation size
 */
#define MCD_OSD_BLK_SIZE_MIN	512
#define MCD_OSD_BLK_SIZE_MAX	8192
#define MCD_OSD_BLK_MASK_MAX	0xffffffffffffe000ULL

#define MCD_OSD_SEGMENT_SIZE    (32 * 1024 * 1024)

/*
 * Block size for doing IO in log and object table
 */
#define MCD_OSD_META_BLK_SIZE		512
#define MCD_OSD_META_BLK_MASK		0xfffffffffffffe00ULL
#define MCD_OSD_META_SEGMENT_BLKS	((MCD_OSD_SEGMENT_SIZE)/(MCD_OSD_META_BLK_SIZE))

#define VAR_BLKS_TO_META_BLKS(blks)	(((blks) * (Mcd_osd_blk_size)) / (MCD_OSD_META_BLK_SIZE))
/*
 * FIXME_8MB
 *
 * IMPORTANT: for backward compatibility, MCD_OSD_LBA_MIN_BLKS
 * must be >= 2 ^ (MCD_OSD_LBA_SHIFT_BITS + 1)
 */
#define MCD_OSD_LBA_MIN_BLKS    64
#define MCD_OSD_LBA_SHIFT_BITS  5
#define MCD_OSD_LBA_SHIFT_FLAG  0x800
#define MCD_OSD_LBA_SHIFT_MASK  0x7ff
#define MCD_OSD_MAX_BLKS_OLD    (2048 + 1)
#define MCD_OSD_OBJ_MAX_SIZE    (8 * 1024 * 1024 - 72) /* - key_size */
#define MCD_OSD_OBJ_MAX_BLKS    4095

/*
 * add one for cmc shard
 */
#define MCD_OSD_MAX_NUM_SHARDS  (MCD_MAX_NUM_CNTRS + 1)

/*
 * number of updates between cas_id log records
 */
#define MCD_OSD_CAS_UPDATE_INTERVAL 480

enum cntr_states {
    cntr_running = 1,           /** container is ready for requests */
    cntr_stopping,              /** container is being stopped */
    cntr_stopped,               /** container is inactive */
};

//struct mcd_container;

//#define BTREE_HACK

struct mcd_osd_slab_class;

typedef struct mcd_osd_segment {
    uint32_t                    used_slabs;
    uint32_t                    idx;  // index in segments list of class
    uint32_t                    used; // guard segment deallocation
    uint32_t                    next_slab;
    uint64_t                    blk_offset;
    uint64_t                  * bitmap;
    uint64_t                  * update_map;
    uint64_t                  * update_map_s;
    uint64_t                  * alloc_map;
    uint64_t                  * alloc_map_s;
    uint64_t                  * refmap;
    struct mcd_osd_slab_class * class;
} mcd_osd_segment_t;

typedef struct slab_gc_class_struct slab_gc_class_t;

typedef struct mcd_osd_slab_class {
    uint64_t                    used_slabs;
    uint64_t                    total_slabs;
    uint64_t                    dealloc_pending;
    int                         slab_blksize;
    int                         slabs_per_segment;
    uint32_t                    num_segments;
    mcd_osd_segment_t        ** segments;
    int                         freelist_len;
    int                         free_slab_curr[MCD_OSD_MAX_PTHREADS];
    uint32_t                  * free_slabs[MCD_OSD_MAX_PTHREADS];
    uint32_t                    clock_hand[MCD_OSD_MAX_PTHREADS];
    uint32_t                    scan_hand[MCD_OSD_MAX_PTHREADS];

    // for serializing persistent class update writes
    fthLock_t                   lock;

    slab_gc_class_t           * gc;
} mcd_osd_slab_class_t;

/*
 * FIFO related
 */
#define MCD_OSD_NUM_WBUFS       2

typedef struct mcd_osd_fifo_wbuf {
    int                         id;
    uint32_t                    ref_count;
    uint32_t                    filled;
    uint64_t                    blk_offset;
    char                      * buf;
    uint32_t                    items;
    char                      * meta;
} mcd_osd_fifo_wbuf_t;


typedef struct mcd_osd_shard_fifo {
    int                         fth_count;
    uint32_t                    pending_wmails;
    uint64_t                    blk_reserved;
    uint64_t                    blk_allocated;
    uint64_t                    blk_committed;
    uint64_t                    blk_nextbuf;
    uint64_t                    free_blks;
    fthMbox_t                   sleeper_mbox;
    char                      * wbuf_base;
    mcd_osd_fifo_wbuf_t         wbufs[MCD_OSD_NUM_WBUFS];
} mcd_osd_shard_fifo_t;


/*
 * SLAB related
 */
#define MCD_OSD_EVAGE_SAMPLES   64
#define MCD_OSD_EVAGE_FEQUENCY  128

typedef struct mcd_osd_shard_slab {
    uint64_t                    ad_next_addr;   /* auto-delete cursor */
    uint64_t                    ad_pfx_cursor; /* for prefix_delete */
    uint64_t                    eviction_ages[MCD_OSD_EVAGE_SAMPLES];
} mcd_osd_shard_slab_t;

typedef struct slab_gc_shard_struct slab_gc_shard_t;

typedef struct mcd_osd_shard {

    /*
     * the public shard structure
     */
    struct shard                shard;

    /*
     * private fields not exposed to other components
     */

    // persistence
    struct mcd_rec_shard      * pshard;
    struct mcd_rec_log        * log;
    struct mcd_rec_ckpt       * ckpt;
    int64_t                     ps_alloc;
    uint32_t                    prop_slot;
    int                         refcount;

    // replication
    struct seqno_logbuf_cache * logbuf_seqno_cache;
    struct mcd_rec_ts_list    * ts_list;
    uint64_t                    sequence;
    uint64_t                    lcss;

    // backup
    struct mcd_bak_state      * backup;
    int                         backup_running;
    int                         restore_running;

    uint64_t                    id;
    int                         opened;
    int                         flush_fd;
    int                         persistent;
    int                         durability_level;
    int                         replicated;
    int                         evict_to_free;
    int                         use_fifo;
    uint64_t                    total_segments;
    uint64_t                    total_size;
    uint64_t                    total_blks;
    uint64_t                    data_blk_offset;
    uint64_t                    blk_allocated;
    uint64_t                    blk_dealloc_pending;
    uint32_t                  * segments;       /* logical addr -> physical */
    uint32_t                  * rand_table;
    struct hash_handle        * hash_handle;
    mcd_osd_segment_t        ** segment_table;  /* logical block -> segment */
    mcd_osd_segment_t         * base_segments;

    fthLock_t                   free_list_lock;
    uint64_t                    free_segments_count;
    uint64_t                  * free_segments;  /* logical block -> segment */

    slab_gc_shard_t           * gc;

    int                         num_classes;
    mcd_osd_slab_class_t        slab_classes[MCD_OSD_MAX_NCLASSES];
    int                         class_table[(MCD_OSD_OBJ_MAX_SIZE / MCD_OSD_BLK_SIZE_MIN + MCD_OSD_LBA_MIN_BLKS) + 1];
    mcd_container_t	      * cntr;

    union {
        mcd_osd_shard_fifo_t    fifo;
        mcd_osd_shard_slab_t    slab;
    };

    /*
     * stats
     */
    uint64_t                    num_objects;
    uint64_t                    total_objects;
    uint64_t                    blk_consumed;
    uint64_t                    blk_delayed;
    uint64_t                    invalid_evictions;
    uint64_t                    num_slab_evictions;
    uint64_t                    num_hash_evictions;
    uint64_t                    num_soft_overflows;
    uint64_t                    num_hard_overflows;
    uint64_t                    get_hash_collisions;
    uint64_t                    set_hash_collisions;
    uint64_t                    num_overwrites;
    uint64_t                    num_gets;
    uint64_t                    num_puts;
    uint64_t                    num_deletes;
    uint64_t                    get_exist_checks;
    uint64_t                    get_size_overrides;
    uint64_t                    num_stolen_slabs;
    uint64_t                    num_full_buckets;
    uint64_t                    rec_num_updates;
    uint64_t                    rec_upd_running;
    uint64_t                    rec_upd_usec;
    uint64_t                    rec_upd_prev_usec;
    uint64_t                    rec_log_reads;
    uint64_t                    rec_table_reads;
    uint64_t                    rec_table_writes;
    uint64_t                    rec_log_reads_cum;
    uint64_t                    rec_table_reads_cum;
    uint64_t                    rec_table_writes_cum;
    uint64_t                    auto_del_bytes;
    uint64_t                    auto_del_scanned;
    uint64_t                    auto_del_expired;
    uint64_t                    auto_del_nomagic;
    uint64_t                    auto_del_prefix;
    uint64_t                    class_segments[MCD_OSD_MAX_NCLASSES];
    uint64_t                    class_slabs[MCD_OSD_MAX_NCLASSES];
    uint64_t                    comp_bytes; /*Total number of compressed bytes. comp_bytes/num_puts gives 
                                              average compresstion ratio*/
    uint64_t                    comp_hist[MCD_OSD_MAX_COMP_HIST];  /*Compression histogram*/

#ifdef  MCD_ENABLE_SLAB_CACHE
    char                      * slab_cache;
#endif

} mcd_osd_shard_t;

typedef struct mcd_osd_meta {
    uint32_t            magic;
    uint16_t            version;
    uint16_t            key_len;
    uint32_t            data_len;    // NOTE: changed from SDF_size_t (8-bytes)
    uint64_t            blk1_chksum;
    uint64_t            data_chksum;
    uint32_t            create_time;
    uint32_t            expiry_time;
    uint64_t            cguid;
    uint64_t            seqno;
    uint64_t            checksum;
    uint32_t            uncomp_datalen;
    
} mcd_osd_meta_t;

enum {
    MCD_OSD_META_MAGIC   = 0x4154454d,  // "META" (in little endian)
    MCD_OSD_META_VERSION = 1,
};


#include <endian.h>
#include <byteswap.h>

/* Need to keep this whole thing in sync with struct mcd_osd_meta */
#if __BYTE_ORDER == __BIG_ENDIAN
/* The host byte order is the same as network byte order,
   so these functions are all just identity.  */

# define mcd_ntohll(x)      (x)
# define mcd_ntohl(x)       (x)
# define mcd_ntohs(x)       (x)
# define mcd_htonll(x)      (x)
# define mcd_htonl(x)       (x)
# define mcd_htons(x)       (x)

# define mcd_meta_hton(m)
# define mcd_meta_ntoh(m)

#elif __BYTE_ORDER == __LITTLE_ENDIAN

# define mcd_ntohll(x)    __bswap_64 (x)
# define mcd_ntohl(x)     __bswap_32 (x)
# define mcd_ntohs(x)     __bswap_16 (x)
# define mcd_htonll(x)    __bswap_64 (x)
# define mcd_htonl(x)     __bswap_32 (x)
# define mcd_htons(x)     __bswap_16 (x)

# define mcd_meta_hton(m)                               \
        m->magic       = mcd_htonl(m->magic);           \
        m->version     = mcd_htons(m->version);         \
        m->key_len     = mcd_htons(m->key_len);         \
        m->data_len    = mcd_htonl(m->data_len);        \
        m->blk1_chksum = mcd_htonl(m->blk1_chksum);     \
        m->create_time = mcd_htonl(m->create_time);     \
        m->expiry_time = mcd_htonl(m->expiry_time);     \
        m->seqno       = mcd_htonll(m->seqno);          \
        m->checksum    = mcd_htonll(m->checksum);

# define mcd_meta_ntoh(m)                               \
        m->magic       = mcd_ntohl(m->magic);           \
        m->version     = mcd_ntohs(m->version);         \
        m->key_len     = mcd_ntohs(m->key_len);         \
        m->data_len    = mcd_ntohl(m->data_len);        \
        m->blk1_chksum = mcd_ntohl(m->blk1_chksum);     \
        m->create_time = mcd_ntohl(m->create_time);     \
        m->expiry_time = mcd_ntohl(m->expiry_time);     \
        m->seqno       = mcd_ntohll(m->seqno);          \
        m->checksum    = mcd_ntohll(m->checksum);

#endif

// -----------------------------------------------------
//    Logging structures
// -----------------------------------------------------

// Log record
// All log records use this structure. Unused fields for a particular record
// should be set to zero.
//
// There are three basic types of log records:
//   (1) create records: contain a syndrome, a block count, a bucket,
//       a block offset, and a seqno.
//   (2) delete records: contain a syndrome, a block count of ZERO,
//       a bucket, a block offset, a seqno, and a target seqno
//   (3) overwrite records: same as create record, plus the target
//       seqno of the deprecated object. Store mode overwrite records
//       also have the old offset of the deprecated object.
// As of 2010-03-22, there is a fourth type of log record. It is used to
// "checkpoint" the current cas_id of the container.
//   (4) cas_id records: contain a block offset of -1, and overloads
//       the target seqno field for the cas_id.
//
// Do not change the size of this structure without also changing the log
// page header structure. Code is dependent on the sizes of these structs
// being the same, and an even divisor of a log page (512-byte block).
typedef struct mcd_logrec_object {
    uint16_t    syndrome;              // 16-bit syndrome
    uint16_t    deleted:1;             // 1=marked for delete-in-future
    uint16_t    reserved:1;            // reserved
    uint16_t    trx:2;                 // reserved
    uint16_t    blocks:12;             // number of 512-byte blocks occupied
    uint32_t    bucket;                // hash bucket
    uint32_t    blk_offset;            // logical block offset within shard
    uint32_t    old_offset;            // old offset within shard (overwrite)
    uint64_t    cntr_id:16;            // seqno of superceded (target) object
    uint64_t    seqno:48;              // sequence number for this record
    uint64_t    target_seqno:48;       // seqno of superceded (target) object
                                       //   used when deleting or overwriting,
                                       //   but not during eviction
    int16_t     bracket_id;            // active btree mput if nonzero
} mcd_logrec_object_t;

#define MCD_FTH_OSD_BUF_SIZE    (1024 * 1024)

typedef struct osd_state {
    aio_state_t       * osd_aio_state;
    void              * osd_mbox;
    int                 osd_blocks;
    void              * osd_lock;
    void              * osd_wait;
    char              * osd_buf;
    int                 index;
} osd_state_t;

/*
 * IMPORTANT: this should be updated whenever MCD_REC_PROP_VERSION_*
 * is updated
 */
enum mcd_cntr_ver {
    MCD_CNTR_VERSION_V1 = 1,
    MCD_CNTR_VERSION_V2 = 2,
};

typedef struct mcd_cntr_ips {
    int                         num_ips;
    struct in_addr              ip_addrs[MCD_CNTR_MAX_NUM_IPS];
} mcd_cntr_ips_t;

/*
 * additional container properties not covered by SDF_container_props
 */
typedef struct mcd_cntr_props {
    mcd_cntr_ips_t            * cntr_ips;
    bool                        sasl;
    bool                        prefix_delete;
    int                         sync_backup;
    int                         sync_updates;
    int                         sync_msec;
    time_t                      flush_time;
    uint64_t                    cas_id;
} mcd_cntr_props_t;

/*
 * globals in mcd_osd.c
 */
extern uint64_t         Mcd_osd_blk_size;
extern uint64_t         Mcd_osd_blk_mask;

extern uint64_t		Mcd_rec_list_items_per_blk;

extern uint64_t		Mcd_osd_total_blks;

extern uint64_t         Mcd_osd_segment_size;
extern uint64_t         Mcd_osd_segment_blks;
extern fthLock_t        Mcd_osd_segment_lock;

extern uint64_t         Mcd_osd_bucket_size;
extern uint64_t         Mcd_osd_bucket_mask;

extern uint64_t         Mcd_osd_free_seg_curr;
extern uint64_t         *Mcd_osd_free_segments;

extern uint64_t         Mcd_osd_bitmap_masks[];

extern uint64_t         Mcd_osd_overflow_depth;

extern mcd_osd_shard_t        * Mcd_osd_slab_shards[];

extern int mcd_osd_init( void );
extern void mcd_osd_assign_pthread_id();
extern int  recovery_report_version( char ** bufp, int * lenp );
extern int  recovery_init( void );
extern int  update_class( mcd_osd_shard_t * shard,
                          mcd_osd_slab_class_t * class,
                          mcd_osd_segment_t* segment );
extern int  flash_format( uint64_t total_size );
extern int  shard_format( uint64_t shard_id, int flags, uint64_t quota,
                          unsigned max_objs, mcd_osd_shard_t * shard );
extern int  shard_unformat( uint64_t shard );
#ifdef SDFAPIONLY
extern int  shard_unformat_api( mcd_osd_shard_t* shard );
#endif
extern int  shard_recover( mcd_osd_shard_t * shard );
extern void shard_recover_phase2( mcd_osd_shard_t * shard );
extern void shard_unrecover( mcd_osd_shard_t * shard );
extern int  shard_get_properties( int index, mcd_container_t * cntr );
extern int  shard_set_properties( mcd_osd_shard_t * shard,
                                  mcd_container_t * cntr );
extern int  shard_set_state( mcd_osd_shard_t * shard, int new_state );
extern int mcd_osd_shard_set_flush_time( struct shard * shard, time_t new_time );
extern void shard_recovery_stats( mcd_osd_shard_t * shard, char ** ppos,
                                  int * lenp );
extern void log_write( mcd_osd_shard_t *, mcd_logrec_object_t *);
struct hash_entry;
extern void log_write_trx( mcd_osd_shard_t *, mcd_logrec_object_t *, uint64_t, struct hash_entry *);
extern void log_sync( mcd_osd_shard_t * shard );
extern void log_wait( mcd_osd_shard_t * shard );
extern uint64_t tombstone_get_rtg( mcd_osd_shard_t * shard );
extern void tombstone_register_rtg_callback(void (*callback)(uint64_t shardID,
                                                             uint64_t seqno));

extern inline char * mcd_fifo_find_buffered_obj( mcd_osd_shard_t * shard,
                                                 char * key, int key_len,
                                                 uint64_t address,
                                                 mcd_osd_fifo_wbuf_t ** wbufp,
                                                 int * committed );

extern SDF_status_t mcd_osd_raw_set( osd_state_t     *osd_state, 
				     mcd_osd_shard_t *shard, 
				     uint64_t         initial, 
				     uint8_t          keyLen, 
				     char            *key, 
				     void            *raw_data,
				     SDF_size_t       raw_len );

SDF_status_t
mcd_osd_raw_get( osd_state_t     *osd_state, 
                 mcd_osd_shard_t *shard, 
		 time_t expTime,
		 time_t flush_time,
		 bool multiKey,
                 void **ppdata, 
		 uint64_t * pactual_size,
                 uint64_t next_addr, 
		 uint64_t * curr_addr,
                 uint64_t prev_seq, 
		 uint64_t curr_seq, 
		 int num_sessions );

extern void mcd_osd_auto_delete( struct ssdaio_ctxt *osd_state);
extern void mcd_osd_incr_container_ref_count(mcd_container_t  *container);
extern void mcd_osd_check_for_stopped_container(mcd_container_t *container);
extern int mcd_stop_container( void *pai, mcd_container_t * container );
extern int mcd_stop_container_internal( void * pai, int tcp_port );
extern int mcd_stop_container_byname_internal( void * pai, char * cname );
extern SDF_status_t mcd_fth_container_init( void * pai, int system_recovery, int *tcp_ports, int *udp_ports, char *ctnr_name );

extern int mcd_osd_container_state(mcd_container_t *container);
extern uint32_t mcd_osd_container_generation(mcd_container_t  *container);
extern int mcd_osd_container_tcp_port(mcd_container_t  *container);
extern char *mcd_osd_container_cluster_name(mcd_container_t *container);
extern int mcd_osd_container_udp_port(mcd_container_t  *container);
extern int mcd_osd_container_cas_num_nodes(mcd_container_t *container);
extern SDF_cguid_t mcd_osd_container_cguid(mcd_container_t  *container);
extern mcd_container_t *mcd_osd_container_from_cguid(SDF_cguid_t cguid);
extern SDFContainer mcd_osd_container_sdf_container(mcd_container_t  *container);
extern void *mcd_osd_container_shard(mcd_container_t  *container);
extern struct in_addr *mcd_osd_container_ip_addrs(mcd_container_t  *container);
extern void mcd_osd_set_container_ip_addrs(mcd_container_t  *container, int i, struct in_addr addr);
extern void mcd_osd_set_container_ip_s_addr(mcd_container_t  *container, int i, int addr);
extern int mcd_osd_container_num_ips(mcd_container_t  *container);
extern char *mcd_osd_container_cname(mcd_container_t  *container);
extern void mcd_osd_container_set_stopped(mcd_container_t  *container);
extern int mcd_osd_container_persistent(mcd_container_t  *container);
extern bool mcd_osd_container_sasl(mcd_container_t  *container);
extern void mcd_osd_set_container_sasl(mcd_container_t  *container, bool sasl);
extern bool mcd_osd_container_prefix_delete(mcd_container_t  *container);
extern void mcd_osd_set_container_prefix_delete(mcd_container_t  *container, bool prefix_delete);
extern void *mcd_osd_container_pfx_deleter(mcd_container_t  *container);
extern void mcd_osd_set_container_pfx_deleter(mcd_container_t  *container, void * pfx_deleter);
extern int mcd_osd_container_hot_key_stats(mcd_container_t  *container);
extern void *mcd_osd_container_hot_key_reporter(mcd_container_t  *container);
extern int mcd_osd_container_max_hot_keys(mcd_container_t  *container);
extern void mcd_osd_set_container_hot_key_reporter(mcd_container_t  *container, void *hot_key_reporter);
extern void mcd_osd_set_container_max_hot_keys(mcd_container_t  *container, int max_hot_keys);
extern void mcd_osd_set_container_hot_key_stats(mcd_container_t  *container, int hot_key_stats);
extern bool mcd_osd_container_binary_tracing(mcd_container_t  *container);
extern void mcd_osd_set_container_binary_tracing(mcd_container_t  *container, bool binary_tracing);
extern int mcd_osd_container_container_id(mcd_container_t  *container);
extern uint64_t mcd_osd_container_size_quota(mcd_container_t  *container);
extern uint32_t mcd_osd_container_obj_quota(mcd_container_t  *container);
extern int mcd_osd_container_sync_backup(mcd_container_t  *container);
extern void mcd_osd_set_container_sync_backup(mcd_container_t  *container, int sync_backup);
extern int mcd_osd_container_sync_updates(mcd_container_t  *container);
extern void mcd_osd_set_container_sync_updates(mcd_container_t  *container, int sync_updates);
extern uint32_t mcd_osd_container_sync_msec(mcd_container_t  *container);
extern int mcd_osd_container_defunct(mcd_container_t  *container);
extern void mcd_osd_set_container_defunct(mcd_container_t  *container, int defunct);
extern int mcd_osd_container_need_reinstate(mcd_container_t  *container);
extern void mcd_osd_set_container_need_reinstate(mcd_container_t  *container, int need_reinstate);
extern time_t mcd_osd_container_flush_time(mcd_container_t  *container);
extern void mcd_osd_set_container_flush_time(mcd_container_t *container, time_t flush_time);
extern uint64_t mcd_osd_container_cas_id(mcd_container_t  *container);
extern uint64_t mcd_osd_container_incr_cas_id( mcd_container_t  *container, uint64_t amount);
extern int mcd_osd_container_prev_state(mcd_container_t  *container);
extern void mcd_osd_get_containers(struct ssdaio_ctxt *osd_state, mcd_container_t  **containers, int *pn_containers);
extern void mcd_osd_get_containers_cguids(struct ssdaio_ctxt *osd_state, SDF_cguid_t *cguid, uint32_t *n_cguids);
extern void mcd_osd_set_shard_cas_id_if_higher( mcd_container_t  *container, void * data );
extern void osd_set_shard_cas_id_if_higher( struct shard *shard, void * data );
extern void mcd_osd_set_container_state(mcd_container_t *container, int state);
extern void mcd_osd_set_container_num_ips(mcd_container_t *container, int num_ips);
extern int mcd_osd_container_eviction(mcd_container_t *container);

/************************************************************************
 *                                                                      *
 *                      MCD OSD routines                                *
 *                                                                      *
 ************************************************************************/

extern int mcd_osd_init( void );

extern void mcd_fth_osd_writer( uint64_t arg );

extern void mcd_osd_register_ops( void );

extern SDF_status_t mcd_osd_get_shard_stats( void * pai, SDFContainer ctnr, int stat_key, uint64_t * stat );

extern void mcd_osd_auto_del_stats( mcd_osd_shard_t *shard, char ** ppos, int * lenp );

extern void mcd_osd_eviction_age_stats( mcd_osd_shard_t *shard, char ** ppos, int * lenp );

extern void mcd_osd_recovery_stats( mcd_osd_shard_t *shard, char ** ppos, int * lenp );

extern uint64_t mcd_osd_get_shard_minsize( void );

extern uint32_t mcd_osd_get_lba_minsize( void );

// extern void * mcd_fth_osd_iobuf_alloc( size_t size, bool is_static );

extern void mcd_fth_osd_iobuf_free( void * buf );

struct shard;

extern int
mcd_osd_shard_backup_prepare( struct shard * shard, int full_backup,
                              uint32_t client_version,
                              uint32_t * server_version );

extern int
mcd_osd_shard_backup( struct shard * shard, int full_backup,
                      int cancel, int complete, uint32_t client_version,
                      uint32_t * server_version, uint64_t * prev_seqno,
                      uint64_t * backup_seqno, time_t * backup_time );

extern int
mcd_osd_shard_restore( struct shard * shard, uint64_t prev_seqno,
                       uint64_t curr_seqno, int cancel, int complete,
                       uint32_t client_version, uint32_t * server_version,
                       uint64_t * err_seqno );

extern int mcd_osd_shard_set_state( struct shard * shard, int new_state );

//struct mcd_container;

extern int mcd_osd_shard_set_properties( struct shard * shard, mcd_container_t * cntr );

extern int mcd_osd_shard_get_properties( int index, mcd_container_t * cntr );


extern int mcd_rec_attach_test( int cmd, int arg );
extern int mcd_start_container_byname_internal( void * pai, char * cname );
extern int mcd_start_container_internal( void * pai, int tcp_port );
extern SDF_status_t mcd_osd_do_prefix_delete( mcd_container_t *container, char *key, int key_len);
extern SDF_status_t mcd_osd_prefix_stats( mcd_container_t *container, char **pdata, uint32_t *pdata_len);
extern SDF_status_t mcd_fth_container_start( void * context, mcd_container_t * container );
extern SDF_status_t mcd_fth_container_stop( void * context, mcd_container_t * container );

extern osd_state_t *mcd_fth_init_aio_ctxt( int category );
extern int mcd_fth_free_aio_ctxt( osd_state_t *context, int category );
extern void mcd_fth_start_osd_writers();

extern int mcd_fth_do_try_container( void * pai, mcd_container_t **ppcontainer, bool open_only,
                              int tcp_port, int udp_port,
                              SDF_container_props_t * prop, char * cntr_name,
                              mcd_cntr_props_t * cntr_props );

/* Next functions used in slab_gc.c */
int
mcd_fth_osd_slab_alloc( void * context, mcd_osd_shard_t * shard, int blocks, int count, uint64_t * blk_offset );

/*
 * remove an entry from the hash table, free up its slab and update
 * the bitmap as well as the free list
 */
int
mcd_fth_osd_remove_entry( mcd_osd_shard_t * shard,
    struct hash_entry* hash_entry,
    bool delayed,
    bool remove_entry);

uint64_t
mcd_fth_osd_shrink_class(mcd_osd_shard_t * shard, mcd_osd_segment_t *segment, bool gc);

int
mcd_osd_slab_segments_get_free_slot(mcd_osd_slab_class_t* class);

int
mcd_osd_slab_segments_free_slot(mcd_osd_slab_class_t* class, mcd_osd_segment_t* segment, void* value);

uint64_t mcd_osd_get_free_segments_count(mcd_osd_shard_t* shard);

void
mcd_osd_segment_unlock(mcd_osd_segment_t* segment);

mcd_osd_segment_t*
mcd_osd_segment_lock(mcd_osd_slab_class_t* class, mcd_osd_segment_t* segment);

uint64_t
mcd_osd_rand_address(mcd_osd_shard_t *shard, uint64_t offset);

int
mcd_onflash_key_match(void *context, mcd_osd_shard_t * shard, uint32_t addr, char *key, int key_len);
#endif  /* __MCD_OSD_H__ */
