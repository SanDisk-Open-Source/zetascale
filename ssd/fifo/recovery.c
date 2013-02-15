/*
 * Fast recovery and enumeration.
 * Author: Johann George
 *
 * Exported
 *   qrecovery_init()
 *   qrecovery_exit()
 *   enumerate_init()
 *   enumerate_done()
 *   enumerate_next()
 *
 * Notes
 *   - Since the enumeration routines share this file, exported enumeration
 *     routines start with enumerate_ and internal enumeration routines start
 *     with e_.
 *
 * Copyright (c) 2010-2013, SanDisk Corporation.  All rights reserved.
 */
#include <time.h>
#include <ctype.h>
#include <libaio.h>
#include "mcd_bak.h"
#include "mcd_osd.h"
#include "utils/hash.h"
#include "fth/fthSem.h"
#include "ssd/ssd_aio.h"
#include "mcd_aio.h"
#include "fth/fthMbox.h"
#include "sdftcp/trace.h"
#include "shared/private.h"
#include "utils/properties.h"
#include "mcd_aio_internal.h"
#include "protocol/protocol_common.h"
#include "protocol/action/recovery.h"
#include "shared/internal_blk_obj_api.h"
#include "protocol/replication/key_lock.h"


/*
 * Recovery parameters.
 */
#define VER_REC          1
#define MIN_SLABS        2
#define MAX_SLABS        0
#define NUM_FTH          64
#define NUM_RSX          (64*1024)
#define NUM_SSX          (1024*1024)
#define MAX_BIG_POST     (200*1000)
#define MAX_ONE_POST     (200*1000)
#define FTH_STACK_SIZE   (64*1024)
#define L2_MAX_SLAB_BLKS 11


/*
 * Recovery constants.
 */
#define BMAP_BITS      (8 * sizeof(bitmap_t))
#define MOBJ_MAGIC     0xface
#define HASH_SYN_SHIFT 48
#define FAKE_KLOCK     (klock_t)1


/*
 * Enumeration constants.
 */
#define E_ROOM_FOR_LOCKS 1


/*
 * This used to match sizeof(object_data_t) from memcached and the header file
 * was included directly.
 */
#define OBJECT_DATA_SIZE 16


/*
 * Logging category.
 */
#define LOG_CAT PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY


/*
 * Macros.
 */
#define pow2(a)              (1 << (a))
#define pow2q(a)             (1ULL << (a))
#define chunk_end(a, n)      (-(a) & (n-1))
#define chunk_div(a, n)      ((a+n-1) / (n))
#define chunk_next(a, n)     ((a+n-1) & ~(n-1))
#define chunk_next_ptr(a, n) (void *) (((uint64_t)(a) + n-1) & ~(n-1))


/*
 * Names.
 */
#define BLK_SIZE MCD_OSD_BLK_SIZE
#define get_prop_ulong getProperty_uLongInt


/*
 * Type definitions.
 */
typedef int64_t blkno_t;
typedef uint32_t addr_t;
typedef uint64_t setx_t;
typedef uint32_t slab_t;
typedef uint64_t bitmap_t;
typedef fthWaitEl_t wait_t;
typedef unsigned int uint_t;
typedef SDF_cguid_t cguid_t;
typedef unsigned char uchar_t;
typedef struct flashDev flash_t;
typedef SDF_action_init_t pai_t;
typedef SDF_shardid_t shardid_t;
typedef mcd_osd_hash_t mo_hash_t;
typedef mcd_osd_meta_t mo_meta_t;
typedef SDF_action_state_t pas_t;
typedef mcd_osd_fifo_wbuf_t wbuf_t;
typedef mcd_osd_shard_t mo_shard_t;
typedef struct rklc_get rklc_get_t;
typedef struct ssdaio_ctxt aioctx_t;
typedef mcd_osd_bucket_t mo_bucket_t;
typedef mcd_osd_segment_t mo_segment_t;
typedef SDF_cache_enum_t s_cache_enum_t;
typedef struct FDF_thread_state fdf_ts_t;
typedef struct replicator_key_lock *klock_t;
typedef mcd_osd_slab_class_t mo_slab_class_t;
typedef SDF_protocol_msg_tiny_t sp_msg_tiny_t;
typedef SDF_protocol_msg_type_t sp_msg_type_t;
typedef struct replicator_key_lock_container rklc_t;


/*
 * Errors.
 */
typedef enum {
    ERR_NONE   = 0,                     /* Success */
    ERR_DEAD   = 1,                     /* Node is dead */
    ERR_READ   = 2,                     /* Read error */
    ERR_SHARD  = 3,                     /* Bad shard */
    ERR_RANGE  = 4,                     /* Index out of range */
    ERR_INVAL  = 5,                     /* Other invalid parameter */
} rerror_t;


/*
 * Message reply types.
 */
typedef enum {
    MREP_FBMAP = 1,                     /* Flash bitmap reply */
    MREP_CACHE = 2,                     /* Cache data reply */
    MREP_FLASH = 3,                     /* Flash data reply */
} mrep_type_t;


/*
 * Fth request types.
 */
typedef enum {
    FREQ_ONE = 1,                       /* Single set */
    FREQ_BIG = 2,                       /* Multi-object write */
} freq_type_t;


/*
 * Used for enumerating objects from the cache.
 */
typedef struct {
    int   count;                        /* Object count */
    char *ptr;                          /* Current place in buffer */
    char *end;                          /* Buffer end */
} cache_fill_t;


/*
 * A linked list of AIO contexts.
 */
typedef struct aioctx_list {
    struct aioctx_list *next;           /* Pointer to next */
    aioctx_t           *aioctx;         /* Pointer to AIO context */
} aioctx_list_t;


/*
 * Message object starting with metadata.
 */
typedef struct {
    uint16_t magic;                     /* Magic number */
    uint16_t key_len;                   /* Key length */
    uint32_t data_len;                  /* Data length */
    uint32_t create_time;               /* Create time */
    uint32_t expiry_time;               /* Expiry time */
    uint64_t seqno;                     /* Sequence number */
    setx_t   setx;                      /* Set index */
    char     data[];                    /* Start of data */
} mobj_t;


/*
 * List of message objects.
 */
typedef struct mlist {
    uint_t        size;                 /* Size */
    uint32_t      num_objs;             /* Number of objects */
    uint_t        slab_l2b;             /* Log2 of blocks in slab */
    mobj_t       *mobj[];               /* List of objects */
} mlist_t;


/*
 * A message to request a bitmap.
 */
typedef struct {
    sp_msg_tiny_t h;                    /* Header */
    shardid_t     shard_id;             /* Shard id */
} mreq_bmap_t;


/*
 * A message to request a section.
 */
typedef struct {
    sp_msg_tiny_t h;                    /* Header */
    shardid_t     shard_id;             /* Shard id */
    uint64_t      index1;               /* Index 1 */
    uint64_t      index2;               /* Index 2 */
    uint64_t      seqn_beg;             /* Beginning sequence number */
    uint64_t      seqn_end;             /* Ending sequence number */
    uint64_t      full_bits;            /* Sizes that are full */
} mreq_sect_t;


/*
 * A message to send a group of set indices that have completed.
 */
typedef struct {
    sp_msg_tiny_t h;                    /* Header */
    shardid_t     shard_id;             /* Shard id */
    uint64_t      num_setx;             /* Number of set indices */
    setx_t        setx[];               /* Set indices */
} mreq_setx_t;


/*
 * A message to indicate that recovery is complete.
 */
typedef struct {
    sp_msg_tiny_t h;                    /* Header */
    shardid_t     shard_id;             /* Shard id */
} mreq_done_t;


/*
 * Reply header.
 */
typedef struct {
    mrep_type_t type;                   /* Type */
    rerror_t    error;                  /* Status */
} mrep_t;


/*
 * Header for a bitmap sent as a reply.
 */
typedef struct {
    mrep_t    h;                        /* Reply header */
    uint32_t  nsegs;                    /* Number of segments */
    uint64_t  size;                     /* Size of the data */
    uint64_t  bitmap[];                 /* Bitmap */
} mrep_bmap_t;


/*
 * Header for section sent as a reply.
 */
typedef struct {
    mrep_t    h;                        /* Reply header */
    uint32_t  send_time;                /* Sending node's time */
    uint32_t  recv_time;                /* May be useful in future */
    uint32_t  num_obj;                  /* Number of objects */
    uint64_t  size;                     /* Size of the data */
    uint64_t  index1;                   /* Next value for index1 */
    uint64_t  index2;                   /* Next value for index2 */
    uint64_t  need_setx;                /* Must finish these operations */
    char      data[];                   /* Start of data */
} mrep_sect_t;


/*
 * Represents a series of contiguous slabs and a buffer to hold them.
 *
 *  index1    - This along with index2 are opaque indices used to determine the
 *              position we are on the disk or the cache.
 *  index2    - See index1.
 *  seqn_beg  - The beginning sequence we care about.
 *  seqn_end  - The ending sequence we care about.
 *  buf_size  - The actual size of the buffer allocated.
 *  data_off  - Offset into the buffer where the data is read.  This must be
 *              aligned to a multiple of 512.
 *  slab_l2b  - The log base 2 of the number of blocks we read as a unit;
 *              referred to as as slab.
 *  init_slab - Initial slab.
 *  num_slabs - Number of slabs.
 *  buf       - Buffer used to read from the disk and also to build up the
 *              message that is sent to the recovering node.
 */
typedef struct {
    uint64_t  index1;
    uint64_t  index2;
    uint64_t  seqn_beg;
    uint64_t  seqn_end;
    uint_t    buf_size;
    uint_t    data_off;
    uint_t    slab_l2b;
    slab_t    init_slab;
    slab_t    num_slabs;
    char     *buf;
} sect_t;


/*
 * Information about each slab we allocate and free from.
 */
typedef struct slab_free {
    struct slab_free *next;             /* Next entry */
    mo_shard_t       *shard;            /* Shard */
    mo_slab_class_t  *class;            /* Class */
    mo_segment_t     *segment;          /* Segment number */
    uint_t            seg_ci;           /* Segment class index */
    uint_t            slab_l2b;         /* Log2 of blocks in slab */
    uint_t            num_free;         /* Number of free slabs */
    uint_t            num_words;        /* Number of words in bitmap */
    slab_t            next_slab;        /* Next slab in bitmap */
    slab_t            num_slabs;        /* Number of slabs in bitmap */
    slab_t            slabs_file;       /* Number of slabs per file */
    bitmap_t         *bitmap;           /* Bitmap */
} slab_free_t;


/*
 * Common to all fth requests.
 */
typedef struct {
    freq_type_t type;                   /* Request type */
    uint_t      size;                   /* Size */
} freq_hdr_t;


/*
 * A fth request to perform a single set.
 */
typedef struct freq_one {
    freq_hdr_t       h;                 /* Header */
    struct rec      *rec;               /* Recovery information */
    struct freq_one *next;              /* Pointer to next */
    mobj_t           o;                 /* Object */
} freq_one_t;


/*
 * A fth request to perform a multi-object write.
 */
typedef struct {
    freq_hdr_t  h;                      /* Header */
    struct rec *rec;                    /* Recovery information */
    blkno_t     blkno;                  /* Block number */
    uint_t      slab_l2b;               /* Log2 of blocks in slab */
    uint_t      num_objs;               /* Number of objects */
    uint_t      num_setx;               /* Number of setx elements */
    uint_t      num_wait;               /* Number of wait elements */
    uint_t      buf_size;               /* Size of buffer */
    char       *data;                   /* Data */
    char       *buf;                    /* Buffer to free */
    wait_t    **wait;                   /* For unlocking after write */
    setx_t      setx[];                 /* Set index */
} freq_big_t;


/*
 * Variables used when coalescing objects.
 */
typedef struct {
    uint_t      slab_l2b;               /* Log2 of blocks in slab */
    uint_t      slab_blks;              /* Blocks in slab */
    uint_t      slab_size;              /* Size of slab */
    uint_t      hash_blks;              /* mcd_osd_blk_to_lba(slab_blks) */
    uint_t      rem_objs;               /* Objects remaining to set */
    uint_t      max_objs;               /* Max objects for this chunk */
    freq_big_t *freq;                   /* Fthread big request */
    uint_t      hash_off;               /* Hash offset */
    blkno_t     blkno;                  /* Block number */
    uint64_t    syndrome;               /* Hash syndrome */
    uint64_t    seqno;                  /* Sequence number */
} coal_var_t;


/*
 * Information about a various group.  Objects are grouped by their slab size.
 */
typedef struct {
    freq_one_t *head;                   /* Head of list */
    freq_one_t *tail;                   /* Tail of list */
    uint_t      num_objs;               /* Number of objects */
} obj_group_t;


/*
 * Information relating to a particular container that are recovering from us.
 */
typedef struct cntr {
    struct cntr *next;                  /* Next in list */
    vnode_t      rank;                  /* Rank of recovering node */
    mo_shard_t  *shard;                 /* Shard we are recovering */
    fthLock_t    ssx_lock;              /* Lock on set indices */
    wait_t      *ssx_wait;              /* Wait entry for lock */
    setx_t       ssx_head;              /* Head */
    setx_t       ssx_tail;              /* Tail */
    setx_t       ssx_mdel;              /* Delete marker */
    uint_t       ssx_num;               /* Number of entries buf holds */
    klock_t     *ssx_buf;               /* The entries */
} cntr_t;


/*
 * Information for a container on the surviving node.
 */
typedef struct {
    vnode_t     rank;                   /* Rank of recovering node*/
    pai_t      *pai;                    /* Action internal pointer */
    pas_t      *pas;                    /* Action state pointer */
    cntr_t     *cntr;                   /* Recovering container information */
    sect_t     *sect;                   /* Section */
    rklc_t     *lock_ctr;               /* Key lock container */
    flash_t    *flash;                  /* Flash device */
    aioctx_t   *aioctx;                 /* AIO context */
    mo_shard_t *shard;                  /* Shard */
} sur_t;


/*
 * Information for a container on the recovering node.
 *
 *  full     - In slab mode, a bit vector consisting of any slabs classes that
 *             are full.
 *  group    - An array of groups.  There is one for each possible slab size
 *             that we wish to coalesce.
 *  lock_map - A bitmap describing which hash buckets are locked.
 *  map_size - Number of bytes in lock_map.
 *  pai      - Action internal pointer.
 *  rank     - Rank of surviving node.
 *  rsx_buf  - The circular buffer of set indices.
 *  rsx_head - Along with setx_tail, these are the head and tail of the
 *             setx indices that are sent to the surviving node once a set is
 *             completed.  The head indicates intent and the tail indicates
 *             completion.
 *  rsx_lock - The lock to ensure that only one thread is sending indices to
 *             the other side.
 *  rsx_num  - The number of indices in rsx_buf;
 *  rsx_sent - The next index that needs to be sent to the other side.
 *  rsx_tail - See rsx_head.
 *  sect     - Current section.
 *  shard    - Current shard.
 *  v        - Variables used for coalescing.
 */
typedef struct rec {
    pai_t       *pai;
    sect_t      *sect;
    bitmap_t    *lock_map;
    mo_shard_t  *shard;
    obj_group_t *group;
    uint_t       map_size;
    vnode_t      rank;
    coal_var_t   v;
    uint64_t     full;
    uint64_t     rsx_head;
    uint64_t     rsx_tail;
    uint64_t     rsx_sent;
    uint64_t     rsx_num;
    fthLock_t    rsx_lock;
    setx_t      *rsx_buf;
    setx_t       need_setx;
} rec_t;


/*
 * The following are all counts of various statistics.
 *
 *  num_objects    - Number of objects we are attempting to write.
 *  num_existed    - Number of writes that did not complete because the key
 *                   already existed.
 *  num_o_writes   - Number of successful single write operations.
 *  num_o_errors   - Number of single write errors.
 *  num_o_objects  - Number of objects dispatched to single writes.
 *  num_m_writes   - Number of successful multi-write operations.
 *  num_m_errors   - Number of multi-write errors.
 *  num_m_objects  - Number of objects dispatched to multi-writes.
 *  num_m_clashed  - We skipped a multi-write because the syndrome clashed.
 *  num_m_lockfail - We skipped a multi-write as we failed to get the lock.
 *  num_m_overflow - We skipped a multi-write as the hash table was full.
 *  num_rsx_sent   - Number of rsx packets sent.
 */
typedef struct {
    uint64_t num_objects;
    uint64_t num_existed;
    uint64_t num_o_writes;
    uint64_t num_o_errors;
    uint64_t num_o_objects;
    uint64_t num_m_writes;
    uint64_t num_m_errors;
    uint64_t num_m_objects;
    uint64_t num_m_clashed;
    uint64_t num_m_lockfail;
    uint64_t num_m_overflow;
    uint64_t num_rsx_sent;
} rstat_t;


/*
 * Recovering node variables.
 *
 *  stats            - statistics.
 *  num_fth          - Number of fthreads.
 *  num_rsx          - Number of set indices a message can hold.
 *  min_slabs        - Minimum number of slabs we attempt to coalesce.
 *  max_slabs        - Maximum number of slabs we attempt to coalesce.
 *  no_coalesce      - Turn off coalescing in slab mode.
 *  num_big_post     - Number of active big posts.
 *  num_one_post     - Number of active one posts.
 *  max_big_post     - Maximum number of active big posts.
 *  max_one_post     - Maximum number of active one posts.
 *  fth_stack_size   - Size of Fth stack.
 *  num_act_big_fth  - Number of active fthreads for coalesced sets.
 *  num_act_one_fth  - Number of active fthreads for single sets.
 *  num_ask_big_fth  - Number of requested fthreads for coalesced sets.
 *  l2_max_slab_blks - Log base 2 of the maximum slab we coalesce.
 *  big_mbox         - Used for coalesced sets.
 *  one_mbox         - Used for single sets.
 *  slab_free        - Information about allocating and freeing slabs.
 *
 */
typedef struct {
    rstat_t        stats;
    uint32_t       num_fth;
    uint32_t       num_rsx;
    uint32_t       min_slabs;
    uint32_t       max_slabs;
    uint32_t       no_coalesce;
    uint32_t       num_big_post;
    uint32_t       num_one_post;
    uint32_t       max_big_post;
    uint32_t       max_one_post;
    uint32_t       fth_stack_size;
    uint32_t       num_act_big_fth;
    uint32_t       num_act_one_fth;
    uint32_t       num_ask_big_fth;
    uint32_t       l2_max_slab_blks;
    fthMbox_t      big_mbox;
    fthMbox_t      one_mbox;
    slab_free_t   *slab_free;
    fthLock_t      aioctx_lock;
    aioctx_list_t *aioctx_list;
} rec_var_t;


/*
 * Surviving node variables.
 *
 *  no_del_opt - Do not try to optimize delete code.
 *  num_ssx    - Number of set indices a message can hold.
 *  cntrs      - Information about containers we are helping to recover.
 *  cntrs_lock - Fth lock for containers.
 *
 */
typedef struct {
    int        no_del_opt;
    uint32_t   num_ssx;
    cntr_t    *cntrs;
    fthLock_t  cntrs_lock;
} sur_var_t;


/*
 * Variables used by both the survivor and recoverer.
 *
 *  no_fast    - Disallow fast recovery.
 *  show_stats - Show statistic information at the end of recovery.
 *  mem_used   - Memory currently used.
 *
 */
typedef struct {
    int      no_fast;
    int      show_stats;
    uint64_t mem_used;
} all_var_t;


/*
 * Container enumeration state information.
 */
typedef struct FDF_iterator {
    mo_shard_t *shard;
    FDF_cguid_t cguid;
    void       *data_buf_alloc;
    void       *data_buf_align;
    mo_hash_t  *hash_buf;
    uint64_t    hash_buf_i;
    uint64_t    hash_buf_n;
    uint64_t    hash_lock_i;
    uint64_t    hash_per_lock;
} e_state_t;


/*
 * Static variables.
 *
 *  AV       - Variables used by both nodes.
 *  RV       - Variables used by the recovering node.
 *  SV       - Variables used by the surviving node.
 */
static all_var_t AV;
static rec_var_t RV;
static sur_var_t SV;


/*
 * Static function for linkage.
 */
static int  ctr_copy(vnode_t rank, struct shard *shard, pai_t *pai);
static void nop_fill(sdf_hfnop_t *nop);
static void prep_del(vnode_t rank, struct shard *shard);
static void msg_recv(sdf_msg_t *req_msg, pai_t *pai, pas_t *pas,
                     flash_t *flash);


/*
 * Linkage.
 */
struct sdf_rec_funcs Funcs ={
    .ctr_copy = ctr_copy,
    .msg_recv = msg_recv,
    .nop_fill = nop_fill,
    .prep_del = prep_del,
};


/*
 * External function prototypes.
 */
uint32_t mcd_osd_blk_to_lba_x(uint32_t blocks);
uint32_t mcd_osd_lba_to_blk_x(uint32_t blocks);
int      mcd_fth_osd_grow_class_x(mo_shard_t *shard, mo_slab_class_t *class);


/*
 * External variables.
 */
extern int Mcd_aio_num_files;


/*
 * Free memory accounting for usage.
 */
static void
free_n(void *p, size_t size, char *desc)
{
    plat_free(p);
    atomic_sub(AV.mem_used, size);
}


/*
 * Bless a message that we have unofficially allocated.  Since it is about to
 * be sent, we also account for it as being freed.  The description would be
 * "sdf_msg".
 */
static void
bless_n(sdf_msg_t *msg, size_t size)
{
    sdf_msg_bless(msg);
    atomic_sub(AV.mem_used, size);
}


/*
 * Allocate memory accounting for usage.
 */
static void *
malloc_n(size_t size, char *desc)
{
    void *p = plat_malloc(size);

    if (p)
        atomic_add(AV.mem_used, size);
    return p;
}


/*
 * Reallocate memory accounting for usage.  Since we currently only use this to
 * decrease the size of memory, it should never fail.
 */
static void *
realloc_nq(void *p, size_t new_size, size_t old_size)
{
    if (new_size > old_size)
        fatal("realloc_nq called to grow segment");
    if (new_size == old_size)
        return p;

    p = plat_realloc(p, new_size);
    if (!p)
        fatal("plat_realloc failed %ld => %ld", new_size, old_size);

    atomic_add(AV.mem_used, new_size - old_size);
    return p;
}


/*
 * Allocate memory.  If it is not available, wait, in a fthread context, for it
 * to become available.
 */
static void *
malloc_nfw(size_t size, char *desc)
{
    int said = 0;

    for (;;) {
        void *ptr = malloc_n(size, desc);

        if (ptr) {
            if (said)
                sdf_logi(70027, "recovery: found buffer, size=%ld", size);
            return ptr;
        }

        if (!said) {
            said = 1;
            sdf_logi(70028, "recovery: waiting for buffer, size=%ld", size);
        }
        fthYield(0);
    }
}


/*
 * Convert a mobj_t from network to host format.
 */
static void
n2h_mobj(mobj_t *mobj)
{
    msg_setn2h(mobj->magic);
    msg_setn2h(mobj->key_len);
    msg_setn2h(mobj->data_len);
    msg_setn2h(mobj->create_time);
    msg_setn2h(mobj->expiry_time);
    msg_setn2h(mobj->seqno);
    msg_setn2h(mobj->setx);
}


/*
 * Convert a mobj_t from host to network format.
 */
static void
h2n_mobj(mobj_t *mobj)
{
    msg_seth2n(mobj->magic);
    msg_seth2n(mobj->key_len);
    msg_seth2n(mobj->data_len);
    msg_seth2n(mobj->create_time);
    msg_seth2n(mobj->expiry_time);
    msg_seth2n(mobj->seqno);
    msg_seth2n(mobj->setx);
}


/*
 * Convert a mrep_t from network to host format.
 */
static void
n2h_mrep_h(mrep_t *m)
{
    msg_setn2h(m->type);
    msg_setn2h(m->error);
}


/*
 * Convert a mrep_t from host to network format.
 */
static void
h2n_mrep_h(mrep_t *m)
{
    msg_seth2n(m->type);
    msg_seth2n(m->error);
}


/*
 * Convert a mrep_bmap_t from network to host format.
 */
static void
n2h_mrep_bmap(mrep_bmap_t *m)
{
    n2h_mrep_h(&m->h);
    msg_setn2h(m->size);
    msg_setn2h(m->nsegs);
}


/*
 * Convert a mrep_bmap_t from host to network format.
 */
static void
h2n_mrep_bmap(mrep_bmap_t *m)
{
    h2n_mrep_h(&m->h);
    msg_seth2n(m->size);
    msg_seth2n(m->nsegs);
}


/*
 * Convert a mrep_sect_t from network to host format.
 */
static void
n2h_mrep_sect(mrep_sect_t *m)
{
    n2h_mrep_h(&m->h);
    msg_setn2h(m->num_obj);
    msg_setn2h(m->size);
    msg_setn2h(m->index1);
    msg_setn2h(m->index2);
    msg_setn2h(m->need_setx);
}


/*
 * Convert a mrep_sect_t from host to network format.
 */
static void
h2n_mrep_sect(mrep_sect_t *m)
{
    h2n_mrep_h(&m->h);
    msg_seth2n(m->num_obj);
    msg_seth2n(m->size);
    msg_seth2n(m->index1);
    msg_seth2n(m->index2);
    msg_seth2n(m->need_setx);
}


/*
 * Convert a sp_msg_tiny_t from host to network format.
 */
static void
h2n_mreq_h(sp_msg_tiny_t *tiny)
{
    msg_seth2n(tiny->cur_ver);
    msg_seth2n(tiny->sup_ver);
    msg_seth2n(tiny->type);
}


/*
 * Convert a sp_msg_tiny_t from network to host format.
 */
static void
n2h_mreq_h(sp_msg_tiny_t *tiny)
{
    msg_setn2h(tiny->cur_ver);
    msg_setn2h(tiny->sup_ver);
    msg_setn2h(tiny->type);
}


/*
 * Convert a mreq_sect_t from network to host format.
 */
static void
n2h_mreq_sect(mreq_sect_t *m)
{
    n2h_mreq_h(&m->h);
    msg_setn2h(m->shard_id);
    msg_setn2h(m->index1);
    msg_setn2h(m->index2);
    msg_setn2h(m->seqn_beg);
    msg_setn2h(m->seqn_end);
}


/*
 * Convert a mreq_sect_t from host to network format.
 */
static void
h2n_mreq_sect(mreq_sect_t *m)
{
    h2n_mreq_h(&m->h);
    msg_seth2n(m->shard_id);
    msg_seth2n(m->index1);
    msg_seth2n(m->index2);
    msg_seth2n(m->seqn_beg);
    msg_seth2n(m->seqn_end);
}


/*
 * Convert a mreq_bmap_t from network to host format.
 */
static void
n2h_mreq_bmap(mreq_bmap_t *m)
{
    n2h_mreq_h(&m->h);
    msg_setn2h(m->shard_id);
}


/*
 * Convert a mreq_bmap_t from host to network format.
 */
static void
h2n_mreq_bmap(mreq_bmap_t *m)
{
    h2n_mreq_h(&m->h);
    msg_seth2n(m->shard_id);
}


/*
 * Return the number of bytes a bitmap of a given length uses.
 */
static int
bit_bytes(int n)
{
    return chunk_div(n, BMAP_BITS) * sizeof(bitmap_t);
}


/*
 * Count the number of bits in a bitmap.
 */
static int
bit_count(bitmap_t *map, uint_t n)
{
    int      k = 0;
    int  count = 0;
    bitmap_t b = 0;
    bitmap_t w = 0;

    while (n) {
        if (!b) {
            w = map[k++];
            if (n >= BMAP_BITS) {
                if (w == 0) {
                    n -= BMAP_BITS;
                    continue;
                }
                if (w == -1ULL) {
                    count += BMAP_BITS;
                    n -= BMAP_BITS;
                    continue;
                }
            }
            b = 1;
        }
        if (w & b)
            count++;
        n--;
        b <<= 1;
    }
    return count;
}


/*
 * See if any of the bits are set in a range in a bitmap.
 */
static int
bit_isset_range(bitmap_t *map, uint_t lo, uint_t hi)
{
    uint_t i = lo;

    while (i < hi) {
        if ((i & (BMAP_BITS-1)) == 0 && i+BMAP_BITS <= hi) {
            if (map[i / BMAP_BITS])
                return 1;
            i += BMAP_BITS;
        } else {
            if ((map[i / BMAP_BITS] & pow2q((i & (BMAP_BITS-1)))) != 0)
                return 1;
            i += 1;
        }
    }
    return 0;
}


/*
 * See if a bit is set in a bitmap.
 */
static int
bit_isset(bitmap_t *map, uint_t n)
{
    return (map[n / BMAP_BITS] & pow2q(n & (BMAP_BITS-1))) ? 1 : 0;
}


/*
 * Clear all the bits between lo and hi not including hi in a bitmap.
 */
static void
bit_clr_range(bitmap_t *map, uint_t lo, uint_t hi)
{
    uint_t i = lo;

    while (i < hi) {
        if ((i & (BMAP_BITS-1)) != 0) {
            map[i / BMAP_BITS] &= ~pow2q(i & (BMAP_BITS-1));
            i += 1;
        } else if (i+BMAP_BITS <= hi) {
            map[i / BMAP_BITS] = 0;
            i += BMAP_BITS;
        } else {
            int n = hi - i;
            map[i / BMAP_BITS] &= ~(pow2q(n & (BMAP_BITS-1)) - 1);
            i += n;
        }
    }
}


/*
 * Set all the bits between lo and hi not including hi in a bitmap.
 */
static void
bit_set_range(bitmap_t *map, uint_t lo, uint_t hi)
{
    uint_t i = lo;

    while (i < hi) {
        if ((i & (BMAP_BITS-1)) != 0) {
            map[i / BMAP_BITS] |= pow2q(i & (BMAP_BITS-1));
            i += 1;
        } else if (i+BMAP_BITS <= hi) {
            map[i / BMAP_BITS] = -1ULL;
            i += BMAP_BITS;
        } else {
            int n = hi - i;
            map[i / BMAP_BITS] |= pow2q(n & (BMAP_BITS-1)) - 1;
            i += n;
        }
    }
}


/*
 * Set a bit in a bitmap.
 */
static void
bit_set(bitmap_t *map, uint_t n)
{
    map[n / BMAP_BITS] |= pow2q(n & (BMAP_BITS-1));
}


/*
 * Clear a bit in a bitmap.
 */
static void
bit_clr(bitmap_t *map, uint_t n)
{
    map[n / BMAP_BITS] &= ~pow2q(n & (BMAP_BITS-1));
}


/*
 * Show a statistics variable.
 */
static void
show_var(uint64_t val, char *name)
{
    if (val)
        fprintf(stderr, "%-14s = %ld\n", name, val);
}


/*
 * Show statistics.
 */
static void
show_stats(int is_recoverer)
{
    fprintf(stderr, "Fast Recovery Stats\n");
    fprintf(stderr, "===================\n");

    if (is_recoverer) {
        show_var(RV.stats.num_objects,    "num_objects");
        show_var(RV.stats.num_existed,    "num_existed");
        show_var(RV.stats.num_o_writes,   "num_o_writes");
        show_var(RV.stats.num_o_errors,   "num_o_errors");
        show_var(RV.stats.num_o_objects,  "num_o_objects");
        show_var(RV.stats.num_m_writes,   "num_m_writes");
        show_var(RV.stats.num_m_errors,   "num_m_errors");
        show_var(RV.stats.num_m_objects,  "num_m_objects");
        show_var(RV.stats.num_m_clashed,  "num_m_clashed");
        show_var(RV.stats.num_m_lockfail, "num_m_lockfail");
        show_var(RV.stats.num_m_overflow, "num_m_overflow");
        show_var(RV.stats.num_rsx_sent,   "num_rsx_sent");
    }

    show_var(AV.mem_used,   "mem_used");
    fprintf(stderr, "===================\n");
}


/*
 * Given a request, generate a reply.
 */
static char *
flash_msg_type(sp_msg_type_t type)
{
    if (type == HFFGB)
        return "HFFGB";
    if (type == HFFGC)
        return "HFFGC";
    if (type == HFFGD)
        return "HFFGD";
    if (type == HFFSX)
        return "HFFSX";
    if (type == HFFRC)
        return "HFFRC";
    return "?";
}


/*
 * Free an AIO context.
 */
static void
aioctx_free(aioctx_t *aioctx)
{
    wait_t       *wait = fthLock(&RV.aioctx_lock, 1, NULL);
    aioctx_list_t *acl = malloc_nfw(sizeof(aioctx_list_t), "aioctx_list");

    acl->aioctx = aioctx;
    acl->next = RV.aioctx_list;
    RV.aioctx_list = acl;
    fthUnlock(wait);
}


/*
 * Allocate an AIO context.  This really ought to be in mcd_fth.c since it
 * knows more details than it ought.
 */
static aioctx_t *
aioctx_alloc(void)
{
    if (RV.aioctx_list) {
        wait_t       *wait = fthLock(&RV.aioctx_lock, 1, NULL);
        aioctx_list_t *acl = RV.aioctx_list;

        if (acl) {
            aioctx_t *aioctx = acl->aioctx;

            RV.aioctx_list = acl->next;
            fthUnlock(wait);
            free_n(acl, sizeof(*acl), "aioctx_list");
            // ((osd_state_t *) aioctx)->uid = fth_uid();
            return aioctx;
        }
        fthUnlock(wait);
    }
    return ssdaio_init_ctxt(SSD_AIO_CTXT_REC_FTH_ONE);
}


/*
 * Clean up the AIO contexts.
 */
static void
aioctx_exit(void)
{
    aioctx_list_t *acl = RV.aioctx_list;

    while (acl) {
        aioctx_list_t *next = acl->next;
        ssdaio_free_ctxt(acl->aioctx, SSD_AIO_CTXT_REC_FTH_ONE);
        plat_free(acl);
        acl = next;
    }
}


/*
 * Unlock a replicator_key_lock.
 */
static void
kl_unlock(klock_t key_lock)
{
    if (key_lock == FAKE_KLOCK)
        return;
    rkl_unlock(key_lock);
}


/*
 * Lock a replicator_key_lock and return the lock.
 */
static klock_t
kl_lock(sur_t *sur, char *key, uint_t key_len)
{
    int s;
    SDF_simple_key_t lock_key;
    klock_t klock = NULL;

    if (!sur->cntr->ssx_mdel)
        return FAKE_KLOCK;
    if (key_len > SDF_SIMPLE_KEY_SIZE)
        fatal("key too large: %d", key_len);
    memcpy(&lock_key.key, key, key_len);
    lock_key.len = key_len;
    s = rklc_lock_sync(sur->lock_ctr, &lock_key, RKL_MODE_RECOVERY, &klock);
    if (s == SDF_LOCK_RESERVED)
        return NULL;
    if (s == SDF_SUCCESS)
        return klock;
    fatal("rklc_lock_sync returned %d", s);
}


/*
 * Free the given container info.
 */
static void
cntr_free(cntr_t *cntr)
{
    setx_t  head = cntr->ssx_head;
    setx_t  tail = cntr->ssx_tail;
    uint_t   num = cntr->ssx_num;
    klock_t *buf = cntr->ssx_buf;

    while (tail < head) {
        klock_t k = buf[tail++ % num];
        if (k)
            kl_unlock(k);
    }

    free_n(buf, SV.num_ssx * sizeof(klock_t), "N*klock");
    free_n(cntr, sizeof(*cntr), "cntr");
}


/*
 * Free any container infos matching the given rank and shard.
 */
static void
sur_clean(int rank, mo_shard_t *shard)
{
    cntr_t *p;
    wait_t *wait = fthLock(&SV.cntrs_lock, 1, NULL);
    cntr_t  **pp = &SV.cntrs;

    while ((p = *pp) != NULL) {
        if (p->rank == rank && (!shard || p->shard == shard)) {
            *pp = p->next;
            cntr_free(p);
        } else
            pp = &p->next;
    }
    fthUnlock(wait);

    if (AV.show_stats)
        show_stats(0);
}


/*
 * Find a container, and return the wait element resulting from locking
 * SV.cntrs_lock for reading.  If the container does not exists and force is
 * set, the container is created.  If force is not set, we return NULL.  In all
 * cases, if the remote node is dead, we return NULL.
 */
static wait_t *
cntr_lock(sur_t *sur, int force)
{
    cntr_t *cntr;
    vnode_t      rank = sur->rank;
    mo_shard_t *shard = sur->shard;
    wait_t *wait = fthLock(&SV.cntrs_lock, 0, NULL);

    if (!sdf_msg_is_live(rank)) {
        fthUnlock(wait);
        return NULL;
    }

    for (cntr = SV.cntrs; cntr; cntr = cntr->next)
        if (cntr->rank == rank && cntr->shard == shard)
            break;

    if (!cntr) {
        fthUnlock(wait);
        if (!force)
            return NULL;

        wait = fthLock(&SV.cntrs_lock, 1, NULL);
        for (cntr = SV.cntrs; cntr; cntr = cntr->next)
            if (cntr->rank == rank && cntr->shard == shard)
                break;

        if (!cntr) {
            cntr = malloc_nfw(sizeof(*cntr), "cntr");
            clear(*cntr);
            cntr->next     = SV.cntrs;
            cntr->rank     = rank;
            cntr->shard    = shard;
            cntr->ssx_head = 1;
            cntr->ssx_tail = 1;
            cntr->ssx_num  = SV.num_ssx;
            cntr->ssx_buf  = malloc_nfw(SV.num_ssx * sizeof(klock_t),
                                        "N*klock");
            if (SV.no_del_opt)
                cntr->ssx_mdel = 1;
            fthLockInit(&cntr->ssx_lock);
            SV.cntrs = cntr;
        }
        fthDemoteLock(wait);
    }

    sur->cntr = cntr;
    return wait;
}


/*
 * Set a message request header.
 */
static void
set_mreq_h(sp_msg_tiny_t *h, sp_msg_type_t type)
{
    clear(*h);
    h->type = type;
}


/*
 * Send a message containing any set indices that have not already been sent
 * between rsx_sent and head.
 */
static void
rsx_send(rec_t *rec, uint64_t head)
{
    wait_t *wait = fthLock(&rec->rsx_lock, 1, NULL);
    int        n = head - rec->rsx_sent;

    if (n > 0) {
        int i;
        uint_t n1;
        uint_t n2;
        setx_t            *buf = rec->rsx_buf;
        uint_t        num_size = rec->rsx_num;
        uint_t        sent_mod = rec->rsx_sent % num_size;
        uint_t        num_wrap = num_size - sent_mod;
        uint_t             len = sizeof(mreq_setx_t) + n * sizeof(setx_t);
        uint_t         msg_len = sizeof(sdf_msg_t) + len;
        sdf_msg_t         *msg = malloc_nfw(msg_len, "sdf_msg");
        mreq_setx_t *mreq_setx = (mreq_setx_t *) msg->msg_payload;
        sdf_fth_mbx_t sfm ={
            .actlvl          = SACK_NONE_FTH,
            .release_on_send = 1,
        };

        n1 = n;
        if (n1 > num_wrap)
            n1 = num_wrap;
        if (n1)
            memcpy(mreq_setx->setx, &buf[sent_mod], n1 * sizeof(setx_t));

        n2 = n - n1;
        if (n2)
            memcpy(&mreq_setx->setx[n1], buf, n2 * sizeof(setx_t));

        set_mreq_h(&mreq_setx->h, HFFSX);
        mreq_setx->shard_id = rec->shard->id;
        mreq_setx->num_setx = n;

        msg_seth2n(mreq_setx->shard_id);
        msg_seth2n(mreq_setx->num_setx);
        for (i = 0; i < n; i++)
            msg_seth2n(mreq_setx->setx[i]);

        bless_n(msg, msg_len);
        (void) sdf_msg_send(msg, len, rec->rank, SDF_FLSH,
                            sdf_msg_myrank(), SDF_RESPONSES, 0, &sfm, NULL);
        atomic_inc(RV.stats.num_rsx_sent);
        rec->rsx_sent += n;
    }
    fthUnlock(wait);
}


/*
 * Wait for the tail to catch up with our head.
 */
static void
rsx_wait(rec_t *rec, uint64_t head)
{
    while (rec->rsx_tail != head)
        fthYield(0);
}


/*
 * Flush and clean up set message indices.
 */
static void
rsx_done(rec_t *rec)
{
    uint64_t head = rec->rsx_head;

    rsx_wait(rec, head);
    rsx_send(rec, head);
}


/*
 * We have completed a series of set indices on the recovering side and need to
 * send them to the surviving node.  The final rsx_send at the end is to avoid
 * a potential deadlock situation.  This is the case where the surviving node
 * cannot fulfill a HFFGC/HFFGD request from the recovering node because it
 * wants to ensure that there is enough space in the sur->cntr->ssx_buf and
 * wants to advance sur->cntr->ssx_head but the surviving node is waiting for
 * set indices to arrive from the recovering node to advance
 * sur->cntr->ssx_tail and these indices are buffered but not sent.  This last
 * rsx_send will ensure that the last write on the recovering node to complete
 * will send the set indices to the recovering node and avoid the deadlock.
 */
static void
rsx_put(rec_t *rec, setx_t *setx, int num_setx)
{
    int i;
    setx_t       *buf = rec->rsx_buf;
    uint_t   num_size = rec->rsx_num;
    uint64_t head_old = atomic_get_add(rec->rsx_head, num_setx);

    rsx_wait(rec, head_old);
    for (i = 0; i < num_setx;) {
        uint64_t   sent = rec->rsx_sent;
        uint64_t   head = head_old + i;
        uint_t head_mod = head % num_size;
        uint_t num_wrap = num_size - head_mod;
        uint_t num_room = sent + num_size - 1 - head;
        uint_t        n = num_setx - i;

        if (n > num_room)
            n = num_room;
        if (n > num_wrap)
            n = num_wrap;

        if (n > 0) {
            memcpy(&buf[head_mod], &setx[i], n * sizeof(setx_t));
            i += n;
        }

        if (head - sent >= num_size - 1)
            rsx_send(rec, head);
    }

    if (!RV.num_one_post && !RV.num_big_post)
        rsx_send(rec, head_old+num_setx);
    atomic_add(rec->rsx_tail, num_setx);
}


/*
 * Delete a set index and return the klock_t entry.  cntr->ssx_lock must be
 * locked.
 */
static void
ssx_del(cntr_t *cntr, setx_t setx)
{
    int n;
    klock_t klock;
    setx_t  head = cntr->ssx_head;
    setx_t  tail = cntr->ssx_tail;
    uint_t   num = cntr->ssx_num;
    klock_t *buf = cntr->ssx_buf;

    if (setx < tail || setx >= head) {
        sdf_loge(70025,
                 "bad set index: %ld head=%ld tail=%ld", setx, head, tail);
        return;
    }

    n = setx % num;
    klock = buf[n];
    if (!klock) {
        sdf_loge(70026,
                 "null set index: %ld head=%ld tail=%ld", setx, head, tail);
    } else {
        kl_unlock(klock);
        buf[n] = NULL;
    }

    if (setx == tail) {
        while (tail < head && buf[tail % num] == NULL)
            tail++;
        cntr->ssx_tail = tail;
    }
}


/*
 * Create a new set index and associate a value with it.  cntr->ssx_lock must
 * be locked.
 */
static setx_t
ssx_add(cntr_t *cntr, klock_t dlock)
{
    setx_t  head = cntr->ssx_head;
    setx_t  tail = cntr->ssx_tail;
    uint_t   num = cntr->ssx_num;

    if (head + 1 == tail + num)
        fatal("ssx_add: head (%ld) caught up to tail (%ld)", head, tail);

    cntr->ssx_head++;
    cntr->ssx_buf[head % num] = dlock;
    return head;
}


/*
 * Unlock cntr->ssx_lock.
 */
static void
ssx_unlock(cntr_t *cntr)
{
    wait_t *wait = cntr->ssx_wait;

    if (!wait)
        fatal("ssx_unlock: NULL entry");
    cntr->ssx_wait = NULL;
    fthUnlock(wait);
}


/*
 * Reserve n setx entries and lock cntr->ssx_lock.  We return the number of
 * entries that are available as more might be.  If the recovering node is
 * dead, we return -1.
 */
static int
ssx_lock(cntr_t *cntr, uint_t n)
{
    int k;
    uint_t num = cntr->ssx_num;

    for (;;) {
        cntr->ssx_wait = fthLock(&cntr->ssx_lock, 1, NULL);

        if (!sdf_msg_is_live(cntr->rank))
            return -1;

        k = cntr->ssx_tail + num - cntr->ssx_head - 1;
        if (k >= n)
            return k;
        fthUnlock(cntr->ssx_wait);
        fthYield(0);
    }
}


/*
 * Return a reasonable set index number we need to complete to keep things
 * flowing.
 */
static setx_t
ssx_need(cntr_t *cntr)
{
    int64_t x = cntr->ssx_head - cntr->ssx_num/2;

    return x < 0 ? 0 : x;

}


/*
 * Flush out the current segment in a slab_free_t entry.
 */
static void
sf_flush_seg(slab_free_t *sf)
{
    int i;
    bitmap_t *real_map;

    if (!sf->segment)
        return;

    real_map = sf->segment->bitmap;
    for (i = 0; i < sf->num_words; i++)
        atomic_and(real_map[i], sf->bitmap[i]);
    atomic_sub(sf->class->used_slabs, sf->num_free);
    sf->segment = NULL;
}


/*
 * Flush out the current segment in a slab_free_t entry.
 */
static void
sf_flush_shard(mo_shard_t *shard)
{
    slab_free_t *sf;

    for (sf = RV.slab_free; sf; sf = sf->next)
        if (sf->shard == shard)
            sf_flush_seg(sf);
}


/*
 * Determine if a shard is almost full.  We want to ensure that there are at
 * least two segments free since otherwise we might run into a scenario where
 * there is only one segment in a particular class which we are using and the
 * regular flashPut needs a segment from the same class.  If it is not able to
 * grow the class and sees that one exists, we clash and it complains loudly.
 * No errors.  Just lots of noise.
 */
static int
almost_full(mo_shard_t *shard)
{
    blkno_t avail = shard->total_blks - shard->blk_allocated;
    int    almost = avail < (2 * Mcd_osd_segment_blks);

    if (almost)
        sf_flush_shard(shard);
    return almost;
}


/*
 * Read the next segment in.
 */
static int
sf_next(slab_free_t *sf)
{
    int i;
    slab_t n;
    slab_t slab_i;
    bitmap_t *real_map;
    mo_segment_t  *segment;
    mo_shard_t      *shard = sf->shard;
    mo_slab_class_t *class = sf->class;
    bitmap_t       *bitmap = sf->bitmap;
    int          seg_slabs = class->slabs_per_segment;

    atomic_add(class->used_slabs, seg_slabs);
    while (sf->seg_ci >= class->num_segments) {
        if (almost_full(shard) || !mcd_fth_osd_grow_class_x(shard, class)) {
            atomic_sub(class->used_slabs, seg_slabs);
            return 0;
        }
    }

    /*
     * It is possible that class->num_segments is incremented before
     * class->segments[] being fully populated.  If we find that to be the
     * case, someone likely has the class lock and is modifying the segments.
     * We acquire and release the lock knowing that the segment should then be
     * populated.  We are assuming that segments are never deallocated from a
     * class.
     */
    segment = class->segments[sf->seg_ci++];
    if (!segment) {
        wait_t *wait = fthLock(&class->lock, 1, NULL);

        segment = class->segments[sf->seg_ci-1];
        if (!segment)
            fatal("class->segments not populated");
        fthUnlock(wait);
    }

    slab_i = atomic_get_add(segment->next_slab, seg_slabs);
    n = (slab_i > seg_slabs) ? seg_slabs : slab_i;
    atomic_sub(segment->next_slab, n);

    real_map = segment->bitmap;
    for (i = 0; i < sf->num_words; i++)
        bitmap[i] = atomic_get_or(real_map[i], -1ULL);

    for (i = slab_i; i < seg_slabs; i++) {
        slab_t b = i/Mcd_aio_num_files +
                   i%Mcd_aio_num_files * sf->slabs_file;
        bit_clr(bitmap, b);
    }

    sf->segment = segment;
    sf->next_slab = 0;
    sf->num_free = sf->num_slabs - bit_count(sf->bitmap, sf->num_slabs);
    atomic_sub(class->used_slabs, seg_slabs - sf->num_free);
    return 1;
}


/*
 * Ensure there are available slabs in the current segment.  If not, attempt to
 * read a new segment in.
 */
static int
sf_avail(slab_free_t *sf)
{
    if (sf->segment && sf->next_slab < sf->num_slabs)
        return 1;
    sf_flush_seg(sf);
    return sf_next(sf);
}


/*
 * Find the next run of contiguous free slabs as indicated by the bitmap.  No
 * need to initialize slab to 0 except that Coverity does not realize that
 * RV.min_slabs must be at least 1 and complains.
 */
static int
sf_find_free(slab_free_t *sf, int req_len, slab_t *slab_ptr)
{
    int len;
    int      slab = 0;
    bitmap_t *map = sf->bitmap;

    if (!req_len)
        return 0;

    for (;;) {
        uint_t i;

        if (!sf_avail(sf))
            return 0;

        len = 0;
        for (i = sf->next_slab; i < sf->num_slabs;) {
            if (bit_isset(map, i)) {
                if (len >= RV.min_slabs)
                    break;
                len = 0;
                i++;
            } else {
                if (!len++)
                    slab = i;
                i++;
                if (len == req_len)
                    break;
            }
        }

        sf->next_slab = i;
        if (len == req_len || len >= RV.min_slabs)
            break;
    }

    bit_set_range(map, slab, slab+len);
    sf->num_free -= len;
    *slab_ptr = slab;
    return len;
}


/*
 * Find the appropriate slab_free_t entry.  If one does not exist, create one.
 */
static slab_free_t *
sf_get(mo_shard_t *shard, int slab_l2b)
{
    int seg_ci;
    uint_t num_slabs;
    uint_t num_words;
    slab_free_t *sf;
    mo_slab_class_t *class;
    uint_t slab_blks = pow2(slab_l2b);

    for (sf = RV.slab_free; sf; sf = sf->next)
        if (sf->shard == shard && sf->slab_l2b == slab_l2b)
            return sf;

    sf = malloc_n(sizeof(*sf),  "slab_free");
    if (!sf)
        return NULL;

    class = &shard->slab_classes[shard->class_table[slab_blks]];
    seg_ci = class->num_segments - 1;
    if (seg_ci < 0)
        seg_ci = 0;
    num_slabs = Mcd_osd_segment_blks / slab_blks;
    num_words = chunk_div(num_slabs, BMAP_BITS);

    clear(*sf);
    sf->shard      = shard;
    sf->class      = class;
    sf->seg_ci     = seg_ci;
    sf->slab_l2b   = slab_l2b;
    sf->num_slabs  = num_slabs;
    sf->num_words  = num_words;
    sf->slabs_file = num_slabs / Mcd_aio_num_files;
    sf->bitmap     = malloc_nfw(num_words * sizeof(uint64_t), "bitmap");

    sf->next = RV.slab_free;
    RV.slab_free = sf;
    return sf;
}


/*
 * Clean up after ourselves.
 */
static void
sf_exit(void)
{
    slab_free_t *sf = RV.slab_free;

    while (sf) {
        slab_free_t *next = sf->next;

        sf_flush_seg(sf);
        if (sf->bitmap)
            free_n(sf->bitmap, sf->num_words * sizeof(uint64_t), "bitmap");
        free_n(sf, sizeof(*sf), "slab_free");
        sf = next;
    }
    RV.slab_free = NULL;
}


/*
 * Free the last n slabs that were allocated.
 */
static void
slab_free(rec_t *rec, slab_t nslabs)
{
    slab_t slab_i;
    uint_t   slab_l2b = rec->v.slab_l2b;
    mo_shard_t *shard = rec->shard;
    slab_free_t   *sf = sf_get(shard, slab_l2b);

    if (!sf)
        fatal("slab_free: no segment: shard=%p l2b=%d", shard, slab_l2b);

    slab_i = sf->next_slab -= nslabs;
    bit_clr_range(sf->bitmap, slab_i, slab_i + nslabs);
    sf->num_free += nslabs;
}


/*
 * Attempt to allocate num_slabs contiguous slabs and return the starting block
 * number through blkno.  We return the number of slabs we were able to
 * allocated.
 */
static int
slab_alloc(rec_t *rec, slab_t nslab, uint64_t *blkno)
{
    int n;
    slab_t slab_i;
    uint_t   slab_l2b = rec->v.slab_l2b;
    mo_shard_t *shard = rec->shard;
    slab_free_t   *sf = sf_get(shard, slab_l2b);

    if (!sf)
        return 0;

    n = sf_find_free(sf, nslab, &slab_i);
    if (!n)
        return 0;

    *blkno = sf->segment->blk_offset + slab_i * pow2(slab_l2b);
    return n;
}


/*
 * Indicate in a bitmap all SLAB segments that are in use and return one past
 * the highest segment in use.
 */
static int
slab_set_map(mo_shard_t *shard, bitmap_t *seg_map)
{
    uint_t segno;
    uint_t max_segno = 0;
    uint_t     nsegs = shard->total_blks / Mcd_osd_segment_blks;

    for (segno = 0; segno < nsegs; segno++) {
        int i;
        int n;
        bitmap_t *slab_map;
        mo_segment_t *seg = shard->segment_table[segno];

        if (!seg)
            continue;
        if (seg->next_slab == 0)
            continue;
        slab_map = seg->bitmap;
        n = Mcd_osd_segment_blks / seg->class->slab_blksize;
        n = chunk_div(n, BMAP_BITS);
        for (i = 0; i < n && slab_map[i] == 0; i++)
            ;
        if (i != n) {
            bit_set(seg_map, segno);
            max_segno = segno + 1;
        }
    }
    return max_segno;
}


/*
 * Indicate in a bitmap all FIFO segments that are in use and return one past
 * the highest segment in use.
 */
static int
fifo_set_map(mo_shard_t *shard, bitmap_t *seg_map)
{
    int i;
    uint_t nblks;
    uint_t nsegs;

    nblks = shard->fifo.blk_committed;
    for (i = 0; i < MCD_OSD_NUM_WBUFS; i++) {
        blkno_t b;
        mcd_osd_fifo_wbuf_t *wbuf = &shard->fifo.wbufs[i];

        if (!wbuf->filled)
            continue;
        b = wbuf->blk_offset + wbuf->filled;
        if (nblks < b)
            nblks = b;
    }
    if (nblks > shard->total_blks)
        nblks = shard->total_blks;

    nsegs = chunk_div(nblks, Mcd_osd_segment_blks);
    bit_set_range(seg_map, 0, nsegs);
    return nsegs;
}


/*
 * Make a message containing a bitmap of which segments are in use.
 */
static sdf_msg_t *
mkmsg_fbmap(sur_t *sur)
{
    int nsegs;
    mo_shard_t *shard = sur->shard;
    int      max_segs = chunk_div(shard->total_blks, Mcd_osd_segment_blks);
    int       max_len = bit_bytes(max_segs);
    int       pay_len = sizeof(mrep_bmap_t) + max_len;
    int       msg_len = sizeof(sdf_msg_t) + pay_len;
    sdf_msg_t    *msg = malloc_nfw(msg_len, "sdf_msg");
    mrep_bmap_t    *m = (mrep_bmap_t *) msg->msg_payload;
    bitmap_t     *map = m->bitmap;

    memset(msg, 0, msg_len);
    if (shard->use_fifo)
        nsegs = fifo_set_map(shard, map);
    else
        nsegs = slab_set_map(shard, map);

    m->h.type  = MREP_FBMAP;
    m->h.error = ERR_NONE;
    m->size    = pay_len;
    m->nsegs   = nsegs;
    h2n_mrep_bmap(m);

    msg->msg_len = sizeof(sdf_msg_t) + sizeof(mrep_bmap_t) + bit_bytes(nsegs);
    bless_n(msg, msg_len);
    return msg;
}


/*
 * Unlock a fth lock and return an integer.
 */
static int
unlock_iret(wait_t *wait, int ret)
{
    fthUnlock(wait);
    return ret;
}


/*
 * Determine if an object is valid.
 */
static int
obj_valid(mo_shard_t *shard, mo_meta_t *meta, addr_t addr)
{
    int n;
    uchar_t        *key = (void *) &meta[1];
    uint64_t   syndrome = hash(key, meta->key_len, 0);
    uint16_t        syn = syndrome >> HASH_SYN_SHIFT;
    uint64_t         hi = syndrome % shard->hash_size;
    mo_bucket_t *bucket = &shard->hash_buckets[hi / Mcd_osd_bucket_size];
    mo_hash_t     *hash = &shard->hash_table[hi & Mcd_osd_bucket_mask];
    fthLock_t     *lock = &shard->bucket_locks[hi / shard->lock_bktsize];
    wait_t        *wait = fthLock(lock, 0, NULL);

    for (n = bucket->next_item; n--; hash++) {
        if (hash->syndrome != syn)
            continue;
        if (hash->address != addr)
            continue;
        return unlock_iret(wait, 1);
    }

    if (shard->use_fifo || !bucket->overflowed)
        return unlock_iret(wait, 0);

    n = Mcd_osd_overflow_depth;
    hash = &shard->overflow_table[hi/shard->lock_bktsize * n];
    for (; n--; hash++) {
        if (hash->syndrome != syn)
            continue;
        if (hash->address != addr)
            continue;
        return unlock_iret(wait, 1);
    }

    return unlock_iret(wait, 0);
}


/*
 * Copy an object from its representation in flash to a format suitable for
 * transmitting over the wire.  We have to be a little careful as the source
 * and destination areas may overlap.  We need to save everything we need from
 * meta before writing out mobj.
 */
static char *
obj_copy(sur_t *sur, char *dst, char *src, klock_t klock)
{
    int n;
    mo_meta_t    *meta = (mo_meta_t *) src;
    mobj_t       *mobj = (mobj_t *) dst;
    uint_t      keylen = meta->key_len;
    uint_t     datalen = meta->data_len;
    time_t create_time = meta->create_time;
    time_t expiry_time = meta->expiry_time;

    mobj->magic       = MOBJ_MAGIC;
    mobj->key_len     = keylen;
    mobj->data_len    = datalen;
    mobj->create_time = create_time;
    mobj->expiry_time = expiry_time;
    mobj->seqno       = 0;
    mobj->setx        = ssx_add(sur->cntr, klock);
    h2n_mobj(mobj);

    n = keylen + datalen;
    src += sizeof(mo_meta_t);
    dst += sizeof(mobj_t);
    if (dst > src)
        fatal("mobj metadata likely too large");

    memmove(dst, src, n);
    dst += n;
    n += sizeof(mobj_t);
    n = -n & (sizeof(uint64_t) - 1);
    if (n) {
        memset(dst, 0, n);
        dst += n;
    }
    return dst;
}


/*
 * Go through the buffer finding the objects that are live and prepare them for
 * sending over the wire.  If we cannot make any progress because we do not
 * even have enough room for the first object, We return the number of slabs
 * that we need.  If everything succeeds, we return 0.
 */
static int
obj_prep(sur_t *sur, uint_t max_obj)
{
    int n;
    addr_t addr;
    mrep_sect_t *mrep_sect;
    sect_t     *sect = sur->sect;
    int         fifo = sur->shard->use_fifo;
    uint_t slab_blks = pow2(sect->slab_l2b);
    uint_t slab_size = slab_blks * BLK_SIZE;
    uint_t  seg_slab = Mcd_osd_segment_blks / slab_blks;
    uint_t  fifo_max = (Mcd_osd_segment_blks - sect->index2) * slab_size;
    uint_t   buf_end = sect->num_slabs * slab_size;
    uint_t   src_off = 0;
    uint_t   num_obj = 0;
    char        *dst = sect->buf;
    char        *buf = &sect->buf[sect->data_off];

    n = sizeof(sdf_msg_t) + sizeof(mrep_sect_t);
    memset(dst, 0, n);
    dst += n;

    while (src_off + sizeof(mo_meta_t) < buf_end) {
        mo_meta_t *meta = (mo_meta_t *) &buf[src_off];
        uint_t raw_size = meta->key_len + meta->data_len;
        uint_t slab_max = src_off + slab_size;
        uint_t  key_off = src_off + sizeof(*meta);
        uint_t  obj_end = key_off + raw_size;

        if (obj_end > (fifo ? fifo_max : slab_max) ||
            meta->magic != MCD_OSD_META_MAGIC) {
            src_off = chunk_next(src_off+1, slab_size);
            continue;
        }

        if (obj_end > buf_end) {
            if (!src_off)
                return chunk_div(obj_end, slab_size);
            break;
        }

        addr = sect->index1 * Mcd_osd_segment_blks +
               sect->index2 * slab_blks +
               src_off / BLK_SIZE;
        if (obj_valid(sur->shard, meta, addr)) {
            klock_t klock = kl_lock(sur, &buf[key_off], meta->key_len);
            if (klock) {
                num_obj++;
                dst = obj_copy(sur, dst, &buf[src_off], klock);
            }
        }
        src_off = chunk_next(obj_end, slab_size);
        if (max_obj && num_obj >= max_obj)
            break;
    }

    sect->index2 += src_off / slab_size;
    if (sect->index2 == seg_slab) {
        sect->index2 = 0;
        sect->index1++;
    }

    memset(sect->buf, 0, sizeof(sdf_msg_t) + sizeof(mrep_sect_t));
    mrep_sect = (mrep_sect_t *)(sect->buf + sizeof(sdf_msg_t));
    mrep_sect->h.type    = MREP_FLASH;
    mrep_sect->h.error   = ERR_NONE;
    mrep_sect->send_time = time(NULL);
    mrep_sect->num_obj   = num_obj;
    mrep_sect->size      = dst - mrep_sect->data;
    mrep_sect->index1    = sect->index1;
    mrep_sect->index2    = sect->index2;
    mrep_sect->need_setx = ssx_need(sur->cntr);
    h2n_mrep_sect(mrep_sect);
    return 0;
}


/*
 * Allocate a buffer large enough to hold meta data (meta_size) followed by
 * sect->num_slabs slabs.  Each slab is sect->slab_size bytes long.  The start
 * of the slabs is aligned on a multiple of boundary which must be a power of
 * 2.  If we cannot find space, we reduce the number of slabs we are attempting
 * to allocate ensuring it is not smaller than amin_slab slabs.  If we fail to
 * allocate the minimum size, if wait is set, we wait for space to become
 * available otherwise we return failure.  data_off is the offset in the buffer
 * just after the meta data starts.  It is the start of the area that is
 * aligned on boundary.
 */
static int
sect_alloc(sect_t *sect, int meta_size, int min_slabs,
           int boundary, int wait, char *desc)
{
    char *buf;
    uint_t size;
    uint_t num_slabs = sect->num_slabs;
    uint_t slab_size = pow2(sect->slab_l2b) * BLK_SIZE;

    for (;;) {
        size = meta_size + num_slabs*slab_size + boundary-1;
        buf = (char *) malloc_n(size, desc);
        if (buf)
            break;

        if (num_slabs == min_slabs) {
            if (!wait)
                return 0;
            buf = malloc_nfw(size, desc);
            break;
        }

        num_slabs = num_slabs / 2;
        if (num_slabs < min_slabs)
            num_slabs = min_slabs;
    }

    sect->buf       = buf;
    sect->buf_size  = size;
    sect->data_off  = (char *)chunk_next_ptr(buf + meta_size, boundary) - buf;
    sect->num_slabs = num_slabs;
    return 1;
}


/*
 * Determine the intersection of two block ranges.
 */
static void
intersect(blkno_t *l_p, blkno_t *n_p,
          blkno_t l1, blkno_t n1, blkno_t l2, blkno_t n2)
{
    blkno_t h1 = l1 + n1;
    blkno_t h2 = l2 + n2;
    blkno_t  l = (l1 > l2) ? l1 : l2;
    blkno_t  h = (h1 < h2) ? h1 : h2;
    blkno_t  n = h - l;

    if (n <= 0) {
        *n_p = 0;
        return;
    }

    *n_p = n;
    *l_p = l;
}


/*
 * Given a logical block and a size, map it to the physical block using the
 * rand table.  We return the number of contiguous blocks we satisfied which
 * might be less than the number we wanted.
 */
static blkno_t
rand_map(mo_shard_t *shard, blkno_t blkno, blkno_t want_blks, blkno_t *cont_p)
{
    blkno_t rand_blks = Mcd_osd_segment_blks / Mcd_aio_num_files;
    blkno_t     index = blkno / rand_blks;
    blkno_t    offset = blkno % rand_blks;
    blkno_t cont_blks = rand_blks - offset;
    blkno_t    mapblk = shard->rand_table[index];
    blkno_t    newblk = mapblk + offset;

    while (cont_blks < want_blks) {
        index++;
        mapblk += rand_blks;
        if (shard->rand_table[index] != mapblk)
            break;
        cont_blks += rand_blks;
    }

    if (cont_blks > want_blks)
        cont_blks = want_blks;

    if (cont_p)
        *cont_p = cont_blks;
    return newblk;
}


/*
 * Read a section of disk.  This attempts to honor the rand table even if it is
 * fully scattered (unlike the memcached code).
 */
static int
read_disk(mo_shard_t *shard, aioctx_t *aioctx,
          char *buf, blkno_t blkno, blkno_t nblks)
{
    char *p = buf;

    while (nblks) {
        int s;
        blkno_t n;
        blkno_t mapblk = rand_map(shard, blkno, nblks, &n);

        sdf_logt(70023, "recovery read %ld:%ld (%ld)", blkno, n, mapblk);
        s = mcd_fth_aio_blk_read((osd_state_t *) aioctx,
                                 p, mapblk*BLK_SIZE, n*BLK_SIZE);
        if (s != FLASH_EOK) {
            sdf_loge(70024, "mcd_fth_aio_blk_read failed: "
                     "st=%d blkno=%ld mapblk=%ld nb=%ld", s, blkno, mapblk, n);
            return 0;
        }
        blkno += n;
        nblks -= n;
        p += n * BLK_SIZE;
    }
    return 1;
}


/*
 * Read a section of disk using the survivor node context.
 */
static int
sur_read_disk(sur_t *sur, char *buf, blkno_t blkno, blkno_t nblks)
{
    return read_disk(sur->shard, sur->aioctx, buf, blkno, nblks);
}


/*
 * Read blocks from a FIFO container.  Return 1 if successful otherwise 0.
 * The data might be either on disk or in the in-core FIFO buffers.  Since
 * these buffers are live, we need to go through an elaborate process to ensure
 * that we get the correct data.
 */
static int
read_fifo(sur_t *sur, char *buf, blkno_t blkno, blkno_t nblks)
{
    int i;
    struct incore {
        blkno_t blkno;
        blkno_t nblks;
        blkno_t offset;
        blkno_t blkoff;
    } incore[MCD_OSD_NUM_WBUFS];
    mo_shard_t *shard = sur->shard;

    /*
     * Mark the sections that might be in cache.
     */
    for (i = 0; i < MCD_OSD_NUM_WBUFS; i++) {
        incore[i].blkno = 0;
        struct incore      *c = &incore[i];
        volatile wbuf_t *wbuf = &shard->fifo.wbufs[i];

        c->nblks  = wbuf->filled;
        c->offset = wbuf->blk_offset;
        c->blkoff = c->offset % shard->total_blks;
        if (!c->nblks)
            continue;
        intersect(&c->blkno, &c->nblks, blkno, nblks, c->blkoff, c->nblks);
    }

    /*
     * Read everything from disk.
     */
    if (!sur_read_disk(sur, buf, blkno, nblks))
        return 0;

    /*
     * We really do not want an else connecting the if statement comparing
     * c->offset and wbuf->blk_offset.  If wbuf->offset changes while read_fifo
     * is called, we will want to perform the read from disk even though we
     * have done the memcpy.
     */
    for (i = 0; i < MCD_OSD_NUM_WBUFS; i++) {
        struct incore      *c = &incore[i];
        volatile wbuf_t *wbuf = &shard->fifo.wbufs[i];
        uint_t            off = (c->blkno - blkno) * BLK_SIZE;

        if (c->nblks == 0)
            continue;
        if (c->offset == wbuf->blk_offset) {
            int    n = c->nblks * BLK_SIZE;
            uint_t o = (c->blkno - c->blkoff) * BLK_SIZE;

            memcpy(&buf[off], &wbuf->buf[o], n);
        }
        if (c->offset != wbuf->blk_offset)
            if (!sur_read_disk(sur, &buf[off], c->blkno, c->nblks))
                return 0;
    }
    return 1;
}


/*
 * Read a series of blocks in a segment.
 */
static int
sect_read(sur_t *sur)
{
    mo_shard_t *shard = sur->shard;
    sect_t      *sect = sur->sect;
    slab_t  slab_blks = pow2(sect->slab_l2b);
    uint_t    seg_off = sect->index1 * Mcd_osd_segment_blks;
    blkno_t     blkno = seg_off + sect->init_slab * slab_blks;
    blkno_t     nblks = sect->num_slabs * slab_blks;
    char         *buf = &sect->buf[sect->data_off];

    if (shard->use_fifo)
        return read_fifo(sur, buf, blkno, nblks);
    else
        return sur_read_disk(sur, buf, blkno, nblks);
}


/*
 * Return the size of a unit in blocks.
 */
static int
sect_unit_l2b(mo_shard_t *shard, uint_t segno)
{
    uint_t n;
    uint_t b;

    if (shard->use_fifo)
        return 0;

    b =  shard->segment_table[segno]->class->slab_blksize;
    for (n = 0; (b >>= 1) != 0; n++)
        ;
    return n;
}


/*
 * Return a message with a status but without any data to be sent to the other
 * side.
 */
static sdf_msg_t *
mkmsg_null(mrep_type_t type, rerror_t error)
{
    int    msg_len = sizeof(sdf_msg_t) + sizeof(mrep_t);
    sdf_msg_t *msg = malloc_nfw(msg_len, "sdf_msg");
    mrep_t      *h = (mrep_t *) msg->msg_payload;

    clear(*h);
    h->type  = type;
    h->error = error;
    h2n_mrep_h(h);

    msg->msg_len = msg_len;
    bless_n(msg, msg_len);
    return msg;
}


/*
 * Clean up sect_t and return a null message.
 */
static sdf_msg_t *
mkmsg_sect_null(sect_t *sect, rerror_t error)
{
    if (sect->buf)
        free_n(sect->buf, sect->buf_size, "sect->buf");
    return mkmsg_null(MREP_FLASH, error);
}


/*
 * Get a section of disk prepared as a message to send to the other side.
 */
static sdf_msg_t *
mkmsg_flash(sur_t *sur)
{
    int i;
    int n;
    int min_slabs;
    sdf_msg_t *msg;
    rklc_get_t *get_lock = NULL;
    mrep_sect_t *mrep_sect;
    cntr_t      *cntr = sur->cntr;
    sect_t      *sect = sur->sect;
    mo_shard_t *shard = sur->shard;
    uint_t  meta_size = sizeof(sdf_msg_t) + sizeof(mrep_sect_t);
    uint_t   slab_l2b = sect_unit_l2b(shard, sect->index1);
    uint_t  slab_blks = pow2(slab_l2b);

    sur->aioctx = ssdaio_init_ctxt(SSD_AIO_CTXT_REC_FLASH);
    sect->init_slab = sect->index2;
    sect->num_slabs = (Mcd_osd_segment_blks / slab_blks) - sect->index2;
    sect->slab_l2b  = slab_l2b;

    if (sect->num_slabs <= 0)
        return mkmsg_sect_null(sect, ERR_RANGE);

    min_slabs = 1;
    for (i = 0; i < 2; i++) {
        sect_alloc(sect, meta_size, min_slabs, BLK_SIZE, 1, "sdf_msg");
        if (!sect_read(sur)) {
            sdf_logi(70021, "mkmsg_flash: read error");
            return mkmsg_sect_null(sect, ERR_READ);
        }
        n = ssx_lock(sur->cntr, min_slabs);
        if (n < min_slabs) {
            ssx_unlock(sur->cntr);
            sdf_logi(70022, "mkmsg_flash: recovering node died");
            return mkmsg_sect_null(sect, ERR_DEAD);
        }
        if (cntr->ssx_mdel)
            get_lock = rklc_start_get(sur->lock_ctr);
        min_slabs = obj_prep(sur, n);
        if (cntr->ssx_mdel)
            rklc_get_complete(get_lock);
        ssx_unlock(sur->cntr);
        if (min_slabs == 0)
            break;
        if (i == 1)
            fatal("obj_prep failed twice");
        free_n(sect->buf, sect->buf_size, "sdf_msg");
    }

    msg = (sdf_msg_t *) sect->buf;
    mrep_sect = (mrep_sect_t *) msg->msg_payload;
    msg->msg_len = meta_size + mrep_sect->size;
    bless_n(msg, sect->buf_size);
    return msg;
}


/*
 * Get a section of the cache prepared as a message to send to the other side.
 */
static char *
cache_fill(s_cache_enum_t *cenum)
{
    cache_fill_t *cf = cenum->fill_state;
    int      key_len = cenum->key_len;
    int     data_len = cenum->data_len;
    int          len = sizeof(mobj_t) + key_len + data_len;
    char        *ptr = cf->ptr;
    char        *end = chunk_next_ptr(ptr+len, sizeof(uint64_t));
    mobj_t     *mobj = (mobj_t *) ptr;

    if (end > cf->end)
        return NULL;

    clear(*mobj);
    mobj->magic       = MOBJ_MAGIC;
    mobj->key_len     = key_len;
    mobj->data_len    = data_len;
    mobj->create_time = cenum->create_time;
    mobj->expiry_time = cenum->expiry_time;
    mobj->seqno       = 0;
    h2n_mobj(mobj);

    cf->count++;
    cf->ptr = end;
    return mobj->data;
}


/*
 * Get a section of the cache prepared as a message to send to the other side.
 */
static sdf_msg_t *
mkmsg_cache(sur_t *sur)
{
    int i;
    int min_slabs;
    sdf_msg_t *msg;
    cache_fill_t cf;
    mrep_sect_t *mrep_sect;
    sect_t *sect = sur->sect;
    s_cache_enum_t cenum ={
        .cguid       = sur->shard->cntr->cguid,
        .fill        = cache_fill,
        .fill_state  = &cf,
        .enum_index1 = sect->index1,
        .enum_index2 = sect->index2,
    };
    uint_t meta_size = sizeof(sdf_msg_t) + sizeof(mrep_sect_t);

    sect->init_slab = 0;
    sect->num_slabs = Mcd_osd_segment_blks;
    sect->slab_l2b  = 0;

    min_slabs = 1;
    for (i = 0; i < 2; i++) {
        uint_t len;
        sect_alloc(sect, meta_size, min_slabs, 1, 1, "sdf_msg");
        cf.count = 0;
        cf.ptr   = sect->buf + meta_size;
        cf.end   = cf.ptr + sect->num_slabs * BLK_SIZE;
        SDFGetModifiedObjects(sur->pai, &cenum);
        if (cf.ptr != sect->buf + meta_size)
            break;
        if (!cenum.key_len && !cenum.data_len) {
            cenum.enum_index1 = -1ULL;
            break;
        }
        if (i == 1)
            fatal("SDFGetModifiedObjects failed twice");
        free_n(sect->buf, sect->buf_size, "sdf_msg");
        len = sizeof(mo_meta_t) + cenum.key_len + cenum.data_len;
        min_slabs = chunk_div(len, BLK_SIZE);
    }

    msg = (sdf_msg_t *) sect->buf;
    memset(msg, 0, meta_size);

    mrep_sect = (mrep_sect_t *) msg->msg_payload;
    mrep_sect->h.type    = MREP_FLASH;
    mrep_sect->h.error   = ERR_NONE;
    mrep_sect->send_time = time(NULL);
    mrep_sect->num_obj   = cf.count;
    mrep_sect->size      = cf.ptr - mrep_sect->data;
    mrep_sect->index1    = cenum.enum_index1;
    mrep_sect->index2    = cenum.enum_index2;

    msg->msg_len = meta_size + mrep_sect->size;
    h2n_mrep_sect(mrep_sect);
    bless_n(msg, sect->buf_size);
    return msg;
}


/*
 * Determine if a string is printable.
 */
static int
printable(char *ptr, uint_t len)
{
    while (len--)
        if (!isprint(*ptr++))
            return 0;
    return 1;
}


/*
 * Show an object.  Used for debugging.
 */
static void
obj_show(char *msg, char *key, int keylen, char *data, int datalen)
{
    int key_ok;
    int data_ok;
    int msglen = msg ? strlen(msg) : 0;

    data += OBJECT_DATA_SIZE;
    datalen -= OBJECT_DATA_SIZE + 2;
    key_ok = printable(key, keylen) && msglen+keylen < 60;
    data_ok = printable(data, datalen) && msglen+keylen+datalen < 60;

    if (msg)
        fprintf(stderr, "%s: ", msg);

    if (key_ok && data_ok)
        fprintf(stderr, "%.*s = %.*s\n", keylen, key, datalen, data);
    else if (key_ok)
        fprintf(stderr, "%.*s = ...[%d]\n", keylen, key, datalen);
    else if (data_ok)
        fprintf(stderr, "[%d] = %.*s\n", keylen, datalen, data);
    else
        fprintf(stderr, "[%d] = [%d]\n", keylen, datalen);
}


/*
 * Show a message object.
 */
static void
mobj_show(char *msg, mobj_t *mobj)
{
    char *obj_beg = mobj->data;
    uint_t klen = mobj->key_len;

    obj_show(msg, obj_beg, klen, obj_beg + klen, mobj->data_len);
}


/*
 * Show the objects in our message.
 */
static void
mrep_sect_show(mrep_sect_t *msg_sect)
{
    char *ptr = msg_sect->data;
    char *end = ptr + msg_sect->size;

    while (ptr < end) {
        mobj_t *mobj = (mobj_t *)ptr;
        char *obj_beg = mobj->data;
        char *obj_end = obj_beg + mobj->key_len + mobj->data_len;

        if (obj_end > end) {
            sdf_loge(70020, "bad recovery meta data");
            return;
        }
        obj_show(NULL, obj_beg, mobj->key_len,
                 obj_beg + mobj->key_len, mobj->data_len);
        ptr = chunk_next_ptr(obj_end, sizeof(uint64_t));
    }
}


/*
 * Handle a fth request for a single set.
 */
static void
fth_req_one(freq_one_t *freq, aioctx_t *aioctx)
{
    int s;
    rec_t    *rec = freq->rec;
    int   key_len = freq->o.key_len;
    char     *key = freq->o.data;
    char    *data = key + key_len;
    struct objMetaData omd ={
        .keyLen     = key_len,
        .dataLen    = freq->o.data_len,
        .createTime = freq->o.create_time,
        .expTime    = freq->o.expiry_time,
    };

    s = ssd_flashPut(aioctx, &rec->shard->shard, &omd,
                     key, data, FLASH_PUT_TEST_EXIST);
    if (freq->o.setx)
        rsx_put(rec, &freq->o.setx, 1);

    if (s == FLASH_EOK)
        atomic_inc(RV.stats.num_o_writes);
    else if (s == FLASH_EEXIST)
        atomic_inc(RV.stats.num_existed);
    else
        atomic_inc(RV.stats.num_o_errors);
    free_n(freq, freq->h.size, "freq_one");
}


/*
 * Handle a fth multi-object write request.
 */
static void
fth_req_big(freq_big_t *freq, aioctx_t  *aioctx)
{
    int i;
    rec_t        *rec = freq->rec;
    blkno_t     blkno = freq->blkno;
    uint64_t    nblks = freq->num_objs * pow2(freq->slab_l2b);
    char           *p = freq->data;
    mo_shard_t *shard = rec->shard;

    while (nblks) {
        int s;
        blkno_t n;
        blkno_t mapblk = rand_map(shard, blkno, nblks, &n);

        sdf_logt(70018, "recovery write %ld:%ld (%ld)", blkno, n, mapblk);
        s = mcd_fth_aio_blk_write((osd_state_t *) aioctx, p,
                                  mapblk*BLK_SIZE, n*BLK_SIZE);
        if (s == FLASH_EOK)
            atomic_inc(RV.stats.num_m_writes);
        else {
            atomic_inc(RV.stats.num_m_errors);
            sdf_loge(70019, "write failed bo=%ld nb=%ld", mapblk, n);
        }

        blkno += n;
        nblks -= n;
        p += n * BLK_SIZE;
    }

    for (i = 0; i < freq->num_wait; i++)
        fthUnlock(freq->wait[i]);
    rsx_put(rec, freq->setx, freq->num_setx);
    free_n(freq->buf, freq->buf_size, "freq_big->buf");
    free_n(freq, freq->h.size, "freq_big");
}


/*
 * An fthread that handles coalesced set requests.
 */
static void
fth_big(uint64_t arg)
{
    aioctx_t *aioctx = aioctx_alloc();

    for (;;) {
        freq_big_t *freq = (freq_big_t *) fthMboxWait(&RV.big_mbox);

        atomic_dec(RV.num_big_post);
        if (!freq)
            break;
        if (freq->h.type != FREQ_BIG)
            fatal("fth_big got bad request: %d\n", freq->h.type);
        fth_req_big(freq, aioctx);
    }
    aioctx_free(aioctx);
}


/*
 * An fthread that handles single sets requests.
 */
static void
fth_one(uint64_t arg)
{
    aioctx_t *aioctx = aioctx_alloc();

    for (;;) {
        freq_one_t *freq = (freq_one_t *) fthMboxWait(&RV.one_mbox);

        atomic_dec(RV.num_one_post);
        if (!freq)
            break;
        if (freq->h.type != FREQ_ONE)
            fatal("fth_one got bad request: %d\n", freq->h.type);
        fth_req_one(freq, aioctx);
    }
    aioctx_free(aioctx);
}


/*
 * Post a one request.
 */
static void
post_one(freq_one_t *freq)
{
    atomic_inc(RV.num_one_post);
    fthMboxPost(&RV.one_mbox, (uint64_t) freq);
}


/*
 * Post a big request.
 */
static void
post_big(freq_big_t *freq)
{
    atomic_inc(RV.num_big_post);
    fthMboxPost(&RV.big_mbox, (uint64_t) freq);
}


/*
 * Stop the fthreads.
 */
static void
fth_stop(void)
{
    int i;

    for (i = 0; i < RV.num_act_big_fth; i++)
        post_big(NULL);

    for (i = 0; i < RV.num_act_one_fth; i++)
        post_one(NULL);

    while (RV.num_big_post || RV.num_one_post)
        fthYield(0);
}


/*
 * Initialize fthreads and get parameters from property file.
 */
static void
fth_init(mo_shard_t *shard)
{
    int i;

    RV.num_act_big_fth = shard->use_fifo ? 0 : RV.num_ask_big_fth;
    RV.num_act_one_fth = RV.num_fth - RV.num_act_big_fth;

    for (i = 0; i < RV.num_act_big_fth; i++)
        fthResume(fthSpawn(fth_big, RV.fth_stack_size), 0);
    for (i = 0; i < RV.num_act_one_fth; i++)
        fthResume(fthSpawn(fth_one, RV.fth_stack_size), 0);
}


/*
 * Make a fth request to post a single object.
 */
static freq_one_t *
make_one(rec_t *rec, mobj_t *mobj)
{
    int          len = mobj->key_len + mobj->data_len;
    uint_t      size = sizeof(freq_one_t) + len;
    freq_one_t *freq = malloc_nfw(size, "freq_one");

    freq->h.type = FREQ_ONE;
    freq->h.size = size;
    freq->rec    = rec;
    freq->next   = NULL;
    freq->o      = *mobj;

    memcpy(freq->o.data, mobj->data, len);
    return freq;
}


/*
 * Post an already prepared freq_one_t.
 */
static void
post_freq_one(freq_one_t *freq)
{
    atomic_inc(RV.stats.num_o_objects);
    post_one(freq);
}


/*
 * Set a single object.
 */
static void
post_mobj(rec_t *rec, mobj_t *mobj)
{
    freq_one_t *freq = make_one(rec, mobj);
    post_freq_one(freq);
}


/*
 * Take a list and return a new list of objects that fall within certain sizes.
 */
static mlist_t *
mlist_pick(mlist_t *mlist, uint_t min_size, uint_t max_size, uint_t slab_l2b)
{
    int si;
    uint_t nsize;
    int         di = 0;
    int      count = mlist->num_objs;
    uint_t   msize = sizeof(mlist_t) + count * sizeof(mobj_t *);
    mlist_t *nlist = malloc_nfw(msize, "mlist");

    for (si = 0; si < count; si++) {
        mobj_t *mobj = mlist->mobj[si];
        int     size = sizeof(mo_meta_t) + mobj->key_len + mobj->data_len;

        if (min_size && size < min_size)
            continue;
        if (max_size && size > max_size)
            continue;
        nlist->mobj[di++] = mobj;
    }

    if (!di) {
        free_n(nlist, msize, "mlist");
        return NULL;
    }

    nsize = sizeof(mlist_t) + di * sizeof(mobj_t *);
    nlist->size     = nsize;
    nlist->num_objs = di;
    nlist->slab_l2b = slab_l2b;
    if (nsize != msize)
        nlist = realloc_nq(nlist, nsize, msize);
    return nlist;
}


/*
 * Set a list of objects one by one.
 */
static void
onebyone(rec_t *rec, mlist_t *mlist)
{
    int i;

    if (!mlist)
        return;
    for (i = 0; i < mlist->num_objs; i++)
        post_mobj(rec, mlist->mobj[i]);
    free_n(mlist, mlist->size, "mlist");
}


/*
 * Layout an object in a buffer to be written to disk.
 */
static void
obj_layout(mobj_t *mobj, char *ptr, int size)
{
    mo_meta_t *meta = (mo_meta_t *) ptr;
    char *end       = ptr + size;
    uint_t      len = mobj->key_len + mobj->data_len;

    clear(*meta);
    meta->magic       = MCD_OSD_META_MAGIC;
    meta->version     = MCD_OSD_META_VERSION;
    meta->key_len     = mobj->key_len;
    meta->data_len    = mobj->data_len;
    meta->create_time = mobj->create_time;
    meta->expiry_time = mobj->expiry_time;

    ptr += sizeof(mo_meta_t);
    if (ptr+len > end)
        fatal("object placed in incorrect slab");

    memcpy(ptr, mobj->data, len);
    ptr += len;
    if (ptr < end)
        memset(ptr, 0, end-ptr);
}


/*
 * Update the hash table with the new entry.  We are opportunistic and if
 * anything looks difficult, we give up.
 */
static int
coal_puthash(rec_t *rec)
{
    int n;
    mo_hash_t *h;
    coal_var_t       *v = &rec->v;
    mo_shard_t   *shard = rec->shard;
    uint_t     hash_off = v->hash_off;
    uint_t    hash_blks = v->hash_blks;
    blkno_t       blkno = v->blkno;
    uint64_t   syndrome = v->syndrome;
    uint16_t        syn = syndrome >> HASH_SYN_SHIFT;
    mo_bucket_t *bucket = &shard->hash_buckets[hash_off / Mcd_osd_bucket_size];
    mo_hash_t     *hash = &shard->hash_table[hash_off & Mcd_osd_bucket_mask];

    if (bucket->next_item == Mcd_osd_bucket_size) {
        atomic_inc(RV.stats.num_m_overflow);
        return 0;
    }

    for (n = bucket->next_item, h = hash; n--; h++) {
        if (h->syndrome == syn) {
            atomic_inc(RV.stats.num_m_clashed);
            return 0;
        }
    }

    h = &hash[bucket->next_item++];
    atomic_inc(shard->num_objects);
    if (bucket->next_item == Mcd_osd_bucket_size)
        atomic_inc(shard->num_full_buckets);

    /* Write out log entry */
    if (shard->persistent) {
        mcd_logrec_object_t log ={
            .syndrome   = syn,
            .blocks     = hash_blks,
            .bucket     = hash_off,
            .blk_offset = blkno,
            .seqno      = v->seqno,
        };
        log_write(shard, &log);
    }

    /* Update hash table */
    {
        mo_hash_t new ={
            .used       = 1,
            .referenced = 1,
            .blocks     = hash_blks,
            .syndrome   = syn,
            .address    = blkno,
        };
        *h = new;
    }

    shard->addr_table[blkno] = h - shard->hash_table;
    atomic_add(shard->blk_consumed, v->slab_blks);
    return 1;
}


/*
 * Flush out the freq_big_t request.
 */
static void
coal_flush(rec_t *rec)
{
    coal_var_t     *v = &rec->v;
    freq_big_t  *freq = v->freq;
    uint_t   max_objs = v->max_objs;
    uint_t   num_objs = freq ? freq->num_objs : 0;

    if (!freq)
        return;

    if (num_objs) {
        atomic_add(RV.stats.num_m_objects, num_objs);
        post_big(freq);
    } else {
        free_n(freq->buf, freq->buf_size, "sect->buf");
        free_n(freq, freq->h.size, "freq_big");
    }

    if (num_objs < max_objs)
        slab_free(rec, max_objs - num_objs);
    v->freq = NULL;
}


/*
 * Allocate space to hold a freq_big_t request.
 */
static int
coal_alloc(rec_t *rec)
{
    uint_t size;
    uint_t nslab;
    uint64_t blkno;
    coal_var_t     *v = &rec->v;
    uint_t   slab_l2b = v->slab_l2b;
    uint_t nslab_disk = v->rem_objs;
    freq_big_t  *freq = v->freq;
    uint_t    one_len = sizeof(freq->wait[0]) + sizeof(freq->setx[0]);
    sect_t       sect = {.slab_l2b = slab_l2b};

    if (freq)
        fatal("coal_alloc called with freq set");

    if (!nslab_disk)
        return 0;

    if (RV.max_slabs && nslab_disk > RV.max_slabs)
        nslab_disk = RV.max_slabs;

    nslab_disk = slab_alloc(rec, nslab_disk, &blkno);
    if (!nslab_disk)
        return 0;

    sect.num_slabs = nslab_disk;
    if (!sect_alloc(&sect, 0, RV.min_slabs, BLK_SIZE, 0, "freq_big->buf"))
        goto err;
    nslab = sect.num_slabs;

    size = sizeof(*freq) + nslab * one_len;
    freq = malloc_n(size, "freq_big");
    if (!freq)
        goto err;

    if (nslab != nslab_disk)
        slab_free(rec, nslab_disk - nslab);

    clear(*freq);
    freq->h.type   = FREQ_BIG;
    freq->h.size   = size;
    freq->rec      = rec;
    freq->blkno    = blkno;
    freq->slab_l2b = slab_l2b;
    freq->buf_size = sect.buf_size;
    freq->data     = &sect.buf[sect.data_off];
    freq->buf      = sect.buf;
    freq->wait     = (wait_t **) &freq->setx[nslab];

    memset(rec->lock_map, 0, rec->map_size);
    v->freq     = freq;
    v->max_objs = nslab;
    return 1;

err:
    if (sect.buf)
        free_n(sect.buf, sect.buf_size, "freq_big->buf");
    slab_free(rec, nslab_disk);
    return 0;
}


/*
 * Attempt to set an object coalescing it. Return 1 if we are successful, 0 if
 * we are simply unable to get the lock and -1 if is too hard to coalesce.
 */
static int
coal_set(rec_t *rec, mobj_t *mobj)
{
    coal_var_t     *v = &rec->v;
    mo_shard_t *shard = rec->shard;
    freq_big_t  *freq = v->freq;
    uint64_t syndrome = hash((uchar_t *) mobj->data, mobj->key_len, 0);
    uint_t  slab_size = v->slab_size;
    uint64_t hash_off = syndrome % shard->hash_size;
    uint_t   buck_ind = hash_off / shard->lock_bktsize;
    wait_t      *wait = NULL;

    if (freq && freq->num_objs == v->max_objs) {
        coal_flush(rec);
        freq = v->freq;
    }

    if (!freq) {
        if (!coal_alloc(rec))
            return -1;
        freq = v->freq;
    }

    if (!bit_isset(rec->lock_map, buck_ind)) {
        fthLock_t *lock = &shard->bucket_locks[buck_ind];
        wait = fthTryLock(lock, 1, NULL);
        if (!wait) {
            atomic_inc(RV.stats.num_m_lockfail);
            return 0;
        }
    }

    v->syndrome = syndrome;
    v->hash_off = hash_off;
    v->seqno    = mobj->seqno;
    v->blkno    = freq->blkno + freq->num_objs * v->slab_blks;

    if (!coal_puthash(rec)) {
        if (wait)
            fthUnlock(wait);
        return -1;
    }

    v->rem_objs--;
    bit_set(rec->lock_map, buck_ind);
    if (mobj->setx)
        freq->setx[freq->num_setx++] = mobj->setx;
    if (wait)
        freq->wait[freq->num_wait++] = wait;
    obj_layout(mobj, freq->data + freq->num_objs++ * slab_size, slab_size);
    return 1;
}


/*
 * Add a request to a group.
 */
static void
group_add(obj_group_t *group, freq_one_t *freq)
{
    freq->next = NULL;
    if (!group->head)
        group->head = group->tail = freq;
    else
        group->tail = group->tail->next = freq;
    group->num_objs++;
}


/*
 * Given a list of objects of the same size, attempt to write as many as we can
 * simultaneously.
 */
static void
coalesce_same(rec_t *rec, mlist_t *mlist)
{
    int i;
    int n;
    freq_one_t *freq;
    obj_group_t new_group = {};
    uint_t       slab_l2b = mlist->slab_l2b;
    uint_t       num_objs = mlist->num_objs;
    obj_group_t    *group = &rec->group[slab_l2b];
    coal_var_t         *v = &rec->v;

    clear(*v);
    v->rem_objs  = num_objs + group->num_objs;
    v->slab_l2b  = slab_l2b;
    v->slab_blks = pow2(v->slab_l2b);
    v->slab_size = v->slab_blks * BLK_SIZE;
    v->hash_blks = mcd_osd_blk_to_lba_x(v->slab_blks);

    n = num_objs;
    for (i = 0 ; i < n; i++) {
        mobj_t *mobj = mlist->mobj[i];
        int s = coal_set(rec, mobj);

        if (s == 0) {
            freq = make_one(rec, mobj);
            group_add(&new_group, freq);
        } else if (s < 0) {
            v->rem_objs--;
            post_mobj(rec, mobj);
        }
    }

    freq = group->head;
    while (freq) {
        freq_one_t *next = freq->next;
        int s = coal_set(rec, &freq->o);

        if (s > 0)
            free_n(freq, freq->h.size, "freq_one");
        else if (s == 0)
            group_add(&new_group, freq);
        else if (s < 0) {
            v->rem_objs--;
            post_freq_one(freq);
        }
        freq = next;
    }
    *group = new_group;
    coal_flush(rec);
}


/*
 * Flush any objects that exist in the groups that have a set index not greater
 * than setx.  If setx is 0, fluah all objects.
 */
static void
coal_group_flush(rec_t *rec, setx_t setx)
{
    int i;

    for (i = 0; i <= RV.l2_max_slab_blks; i++) {
        freq_one_t  *p;
        freq_one_t **pp;
        obj_group_t *group = &rec->group[i];
        mlist_t      mlist = {.slab_l2b = i};

        if (!group->head)
            continue;

        coalesce_same(rec, &mlist);
        if (!group->head)
            continue;

        group->tail = NULL;
        for (pp = &group->head; (p = *pp) != NULL; ) {
            if (setx && p->o.setx > setx) {
                pp = &p->next;
                group->tail = p;
            } else {
                *pp = p->next;
                post_freq_one(p);
                group->num_objs--;
            }
        }
    }
}


/*
 * Given a list of objects of mixed sizes, attempt to coalesce them.
 */
static void
coalesce(rec_t *rec, mlist_t *mlist)
{
    int i;
    int min = 0;

    for (i = 0; i <= RV.l2_max_slab_blks; i++) {
        int        max = pow2(i) * BLK_SIZE;
        mlist_t *nlist = mlist_pick(mlist, min, max, i);

        if (nlist) {
            coalesce_same(rec, nlist);
            free_n(nlist, nlist->size, "mlist");
        }
        min = max + 1;
    }

    onebyone(rec, mlist_pick(mlist, min, 0, 0));
    free_n(mlist, mlist->size, "mlist");
}


/*
 * Convert all objects from network to host format and ensure that the data is
 * valid.
 */
static mlist_t *
mrep_sect_obj_ready(mrep_sect_t *mrep_sect)
{
    int    num_obj = mrep_sect->num_obj;
    int       skew = time(NULL) - mrep_sect->send_time;
    int      count = 0;
    char      *ptr = mrep_sect->data;
    char      *end = ptr + mrep_sect->size;
    uint_t    size = sizeof(mlist_t) + num_obj * sizeof(mobj_t *);
    mlist_t *mlist = malloc_nfw(size, "mlist");

    while (ptr < end) {
        mobj_t  *mobj = (mobj_t *)ptr;
        char *obj_end = mobj->data + mobj->key_len + mobj->data_len;

        n2h_mobj(mobj);
        if (mobj->magic != MOBJ_MAGIC) {
            sdf_loge(70014, "recovery: mrep_sect: invalid magic %x",
                     mobj->magic);
            break;
        } else if (count >= num_obj) {
            sdf_loge(70015, "recovery: mrep_sect: invalid num_obj %d",
                     num_obj);
            break;
        }

        if (mobj->create_time)
            mobj->create_time += skew;
        if (mobj->expiry_time)
            mobj->expiry_time += skew;

        mlist->mobj[count++] = mobj;
        ptr = chunk_next_ptr(obj_end, sizeof(uint64_t));
    }

    if (ptr != end)
        goto err;

    if (count != num_obj) {
        sdf_loge(70016,
                 "recovery: mrep_sect: invalid num_obj (%d) should be %d",
                 num_obj, count);
        goto err;
    } else if (ptr != end) {
        sdf_loge(70017, "recovery: mrep_sect: bad object list");
        goto err;
    }

    mlist->size     = size;
    mlist->num_objs = count;
    mlist->slab_l2b = 0;
    return mlist;

err:
    free_n(mlist, size, "mlist");
    return NULL;
}


/*
 * Return the lock container for a given shard.
 */
static rklc_t *
get_lock_container(pas_t *pas, mo_shard_t *shard)
{
    SDF_cguid_t cguid = shard->cntr->cguid;

    if (pas->ctnr_meta[cguid].cguid == cguid)
        return pas->ctnr_meta[cguid].lock_container;
    fatal("could not obtain lock_container for shard");
}


/*
 * Handle a request to receive a section which is a collection of objects.
 */
static sdf_msg_t *
mreq_sect(sur_t *sur, mrep_type_t type, sdf_msg_t *req_msg)
{
    sect_t sect;
    wait_t *wait;
    mo_shard_t *shard;
    sdf_msg_t *rep_msg;
    mreq_sect_t *m = (mreq_sect_t *) req_msg->msg_payload;
    uint       len = req_msg->msg_len - sizeof(sdf_msg_t);

    if (len < sizeof(mreq_sect_t)) {
        sdf_logi(70011, "bad mreq_sect msg: bad length");
        return mkmsg_null(type, ERR_INVAL);
    }
    n2h_mreq_sect(m);

    shard = (mo_shard_t *) shardFind(sur->flash, m->shard_id);
    if (!shard) {
        sdf_logi(70012, "bad mreq_sect msg: bad shard");
        return mkmsg_null(type, ERR_SHARD);
    }

    clear(sect);
    sect.index1   = m->index1;
    sect.index2   = m->index2;
    sect.seqn_beg = m->seqn_beg;
    sect.seqn_end = m->seqn_end;

    sur->sect     = &sect;
    sur->shard    = shard;
    sur->lock_ctr = get_lock_container(sur->pas, shard);

    wait = cntr_lock(sur, 1);
    if (!wait) {
        sdf_logi(70013, "mreq_sect: recovering node died");
        return mkmsg_null(type, ERR_DEAD);
    }

    if (type == MREP_CACHE)
        rep_msg = mkmsg_cache(sur);
    else if (type == MREP_FLASH)
        rep_msg = mkmsg_flash(sur);
    else
        fatal("mreq_sect: bad type: %d", type);

    fthUnlock(wait);
    return rep_msg;
}


/*
 * Recovery is finished.
 */
static sdf_msg_t *
mreq_rdone(sur_t *sur, sdf_msg_t *req_msg)
{
    mreq_done_t *m = (mreq_done_t *) req_msg->msg_payload;
    uint len       = req_msg->msg_len - sizeof(sdf_msg_t);

    if (len < sizeof(mreq_done_t))
        return NULL;

    msg_setn2h(m->shard_id);
    sur->shard = (mo_shard_t *) shardFind(sur->flash, m->shard_id);
    if (!sur->shard)
        return NULL;

    sur_clean(sur->rank, sur->shard);
    return NULL;
}


/*
 * Receive a list of lock indices that have completed.  If we cannot find the
 * matching container, we assume that the recovery done message has already
 * appeared and released the lock indices so we just return.
 */
static sdf_msg_t *
mreq_setix(sur_t *sur, sdf_msg_t *req_msg)
{
    int i;
    int n;
    wait_t *wait;
    mreq_setx_t *m = (mreq_setx_t *) req_msg->msg_payload;
    uint len       = req_msg->msg_len - sizeof(sdf_msg_t);

    if (len < sizeof(mreq_setx_t))
        return NULL;

    msg_setn2h(m->shard_id);
    sur->shard = (mo_shard_t *) shardFind(sur->flash, m->shard_id);
    if (!sur->shard)
        return NULL;

    msg_setn2h(m->num_setx);
    n = m->num_setx;
    if (n * sizeof(setx_t) != len - sizeof(mreq_setx_t)) {
        sdf_loge(70010, "bad HFFSX message: num_setx=%d len=%d", n, len);
        return NULL;
    }

    wait = cntr_lock(sur, 0);
    if (!wait)
        return NULL;

    ssx_lock(sur->cntr, 0);
    for (i = 0; i < n; i++) {
        msg_setn2h(m->setx[i]);
        ssx_del(sur->cntr, m->setx[i]);
    }
    ssx_unlock(sur->cntr);
    fthUnlock(wait);
    return NULL;
}


/*
 * Handle a request to receive a section of a flash container.
 */
static sdf_msg_t *
mreq_flash(sur_t *sur, sdf_msg_t *req_msg)
{
    return mreq_sect(sur, MREP_FLASH, req_msg);
}


/*
 * Handle a request to receive a section of the cache.
 */
static sdf_msg_t *
mreq_cache(sur_t *sur, sdf_msg_t *req_msg)
{
    return mreq_sect(sur, MREP_CACHE, req_msg);
}


/*
 * Handle a request for bitmaps for a shard.
 */
static sdf_msg_t *
mreq_fbmap(sur_t *sur, sdf_msg_t *req_msg)
{
    mreq_bmap_t *m = (mreq_bmap_t *) req_msg->msg_payload;
    uint len       = req_msg->msg_len - sizeof(sdf_msg_t);

    if (len < sizeof(mreq_bmap_t)) {
        sdf_logi(70008, "bad mreq_fbmap msg: bad length");
        return mkmsg_null(MREP_FBMAP, ERR_INVAL);
    }
    n2h_mreq_bmap(m);

    sur->shard = (mo_shard_t *) shardFind(sur->flash, m->shard_id);
    if (!sur->shard) {
        sdf_logi(70009, "bad mreq_fbmap msg: bad shard");
        return mkmsg_null(MREP_FBMAP, ERR_SHARD);
    }
    return mkmsg_fbmap(sur);
}


/*
 * Given a request, generate a reply.
 */
static sdf_msg_t *
req_to_rep(sur_t *sur, sdf_msg_t *req_msg)
{
    sp_msg_tiny_t h;
    uint len = req_msg->msg_len - sizeof(sdf_msg_t);

    if (len < sizeof(sp_msg_tiny_t))
        return NULL;

    h = *((sp_msg_tiny_t *) req_msg->msg_payload);
    n2h_mreq_h(&h);

    if (h.type == HFFGB)
        return mreq_fbmap(sur, req_msg);
    if (h.type == HFFGC)
        return mreq_cache(sur, req_msg);
    if (h.type == HFFGD)
        return mreq_flash(sur, req_msg);
    if (h.type == HFFSX)
        return mreq_setix(sur, req_msg);
    if (h.type == HFFRC)
        return mreq_rdone(sur, req_msg);
    return NULL;
}


/*
 * Handle a recovery message that was received.
 */
static void
msg_recv(sdf_msg_t *req_msg, pai_t *pai, pas_t *pas, flash_t *flash)
{
    sur_t sur ={
        .pai   = pai,
        .pas   = pas,
        .rank  = req_msg->msg_src_vnode,
        .flash = flash,
    };
    sdf_msg_t *rep_msg = req_to_rep(&sur, req_msg);

    if (rep_msg) {
        sdf_resp_mbx_t srm;
        sdf_fth_mbx_t sfm ={
            .actlvl          = SACK_NONE_FTH,
            .release_on_send = 1,
        };
        int         len = rep_msg->msg_len - sizeof(sdf_msg_t);
        vnode_t      dn = req_msg->msg_src_vnode;
        vnode_t      sn = req_msg->msg_dest_vnode;
        service_t    ds = req_msg->msg_src_service;
        service_t    ss = req_msg->msg_dest_service;
        msg_type_t type = rep_msg->msg_type;

        sdf_msg_get_response(req_msg, &srm);
        (void) sdf_msg_send(rep_msg, len, dn, ds, sn, ss, type, &sfm, &srm);
    }
    sdf_msg_free(req_msg);
}


/*
 * Given a request, send it to the remote node and wait for a reply.
 */
static sdf_msg_t *
remote_req_to_rep(vnode_t rank, sdf_msg_t *msg)
{
    vnode_t me = sdf_msg_myrank();
    int    len = msg->msg_len - sizeof(sdf_msg_t);

    return sdf_msg_send_receive(msg, len, rank, SDF_FLSH,
                                me, SDF_RESPONSES, 0, 1);
}


/*
 * Determine if the common part of a response message is valid.
 */
static int
msg_rep_check(sdf_msg_t *msg, mrep_type_t type, int size)
{
    mrep_t h;
    int msg_size;
    char *s = "recovery response";

    if (!msg)
        return 0;

    msg_size = msg->msg_len - sizeof(sdf_msg_t);
    if (msg_size < sizeof(mrep_t)) {
        sdf_loge(70004, "%s: msg size too small: %d", s, msg_size);
        return 0;
    }

    h = *((mrep_t *) msg->msg_payload);
    n2h_mrep_h(&h);

    if (h.type != type)
        sdf_loge(70005, "%s: wrong type: %d != %d", s, h.type, type);
    else if (h.error != ERR_NONE)
        sdf_loge(70006, "%s: error: %d", s, h.error);
    else if (msg_size < size)
        sdf_loge(70007, "%s: wrong msg size: %d < %d", s, msg_size, size);
    else
        return 1;
    return 0;
}


/*
 * Determine if a section reply message is valid and if so, convert it to the
 * host format.  Return 1 if valid otherwise 0.
 */
static int
msg_rep_sect_check(sdf_msg_t *msg)
{
    int len;
    mrep_sect_t *m;

    if (!msg)
        return 0;

    if (!msg_rep_check(msg, MREP_FLASH, sizeof(mrep_sect_t)))
        return 0;

    m = (mrep_sect_t *) msg->msg_payload;
    n2h_mrep_sect(m);

    len = msg->msg_len - sizeof(sdf_msg_t) - sizeof(mrep_sect_t);
    if (m->size != len) {
        sdf_loge(70003, "recovery: mrep_sect: bad size: %ld != %d",
                 m->size, len);
        return 0;
    }
    return 1;
}


/*
 * Determine if a bitmap reply message is valid and if so, convert it to the
 * host format.  Return 1 if valid otherwise 0.
 */
static int
msg_rep_bmap_check(sdf_msg_t *msg)
{
    int n;
    int nsegs;
    mrep_bmap_t *m;

    if (!msg)
        return 0;

    if (!msg_rep_check(msg, MREP_FBMAP, sizeof(mrep_bmap_t)))
        return 0;

    m = (mrep_bmap_t *) msg->msg_payload;
    n2h_mrep_bmap(m);

    nsegs = m->nsegs;
    n = chunk_div(nsegs, BMAP_BITS) * sizeof(bitmap_t);
    if (n != msg->msg_len - sizeof(sdf_msg_t) - sizeof(mrep_bmap_t)) {
        sdf_loge(70002, "recovery: mrep_bmap: bitmap too small: nsegs=%d",
                 nsegs);
        return 0;
    }
    return 1;
}


/*
 * Get as many objects as we can from a container on the surviving node given a
 * main and sub index and a beginning and ending sequence number.
 */
static sdf_msg_t *
get_remote_sect(rec_t *rec, sp_msg_type_t type)
{
    sdf_msg_t *rep_msg;
    sect_t           *sect = rec->sect;
    int            msg_len = sizeof(sdf_msg_t) + sizeof(mreq_sect_t);
    sdf_msg_t     *req_msg = malloc_nfw(msg_len, "sdf_msg");
    mreq_sect_t *mreq_sect = (mreq_sect_t *) req_msg->msg_payload;

    set_mreq_h(&mreq_sect->h, type);
    mreq_sect->shard_id = rec->shard->id;
    mreq_sect->index1   = sect->index1;
    mreq_sect->index2   = sect->index2;
    mreq_sect->seqn_beg = sect->seqn_beg;
    mreq_sect->seqn_end = sect->seqn_end;

    h2n_mreq_sect(mreq_sect);
    req_msg->msg_len = msg_len;
    bless_n(req_msg, msg_len);

    rep_msg = remote_req_to_rep(rec->rank, req_msg);
    if (msg_rep_sect_check(rep_msg))
        return rep_msg;

    sdf_msg_free(rep_msg);
    return NULL;
}


/*
 * Get a bitmap for a container from a remote node indicating what segments are
 * in use.
 */
static sdf_msg_t *
get_remote_bmap(rec_t *rec)
{
    sdf_msg_t *rep_msg;
    int            msg_len = sizeof(sdf_msg_t) + sizeof(mreq_bmap_t);
    sdf_msg_t     *req_msg = malloc_nfw(msg_len, "sdf_msg");
    mreq_bmap_t *mreq_bmap = (mreq_bmap_t *) req_msg->msg_payload;

    set_mreq_h(&mreq_bmap->h, HFFGB);
    mreq_bmap->shard_id = rec->shard->id;
    h2n_mreq_bmap(mreq_bmap);
    req_msg->msg_len = msg_len;
    bless_n(req_msg, msg_len);

    rep_msg = remote_req_to_rep(rec->rank, req_msg);
    if (msg_rep_bmap_check(rep_msg))
        return rep_msg;

    sdf_msg_free(rep_msg);
    return NULL;
}


/*
 * Update progress counter.
 */
static void
progress(rec_t *rec)
{
    pai_t   *pai = rec->pai;
    sect_t *sect = rec->sect;

    if (pai) {
        int i;
        qrep_node_state_t *node;
        cguid_t cguid = rec->shard->cntr->cguid;
        int     count = sect->init_slab;
        int     total = sect->num_slabs;
        int   percent = total ? (count * 100 / total) : 0;

        node = &pai->pcs->qrep_state.node_state[sdf_msg_myrank()];
        for (i = 0; i < node->nctnrs_node; i++)
            if (node->cntrs[i].cguid == cguid)
                node->cntrs[i].rec_prog = percent;
    }
}


/*
 * Get a section from the remote node and process it.  Return 1 on success and
 * 0 on failure.
 */
static int
do_sect(rec_t *rec, sp_msg_type_t type)
{
    sdf_msg_t *msg;
    mlist_t *mlist;
    mrep_sect_t *mrep_sect;
    int             s = 0;
    setx_t  need_setx = rec->need_setx;
    sect_t      *sect = rec->sect;
    mo_shard_t *shard = rec->shard;

    if (need_setx)
        coal_group_flush(rec, need_setx);
    msg = get_remote_sect(rec, type);
    if (!msg)
        return 0;

    if (!msg_rep_sect_check(msg))
        goto err;

    mrep_sect = (mrep_sect_t *) msg->msg_payload;
    mlist = mrep_sect_obj_ready(mrep_sect);
    if (!mlist)
        goto err;

    atomic_add(RV.stats.num_objects, mlist->num_objs);
    if (shard->use_fifo || RV.no_coalesce || almost_full(shard))
        onebyone(rec, mlist);
    else
        coalesce(rec, mlist);

    sect->index1   = mrep_sect->index1;
    sect->index2   = mrep_sect->index2;
    rec->need_setx = mrep_sect->need_setx;
    s = 1;

err:
    if (msg)
        sdf_msg_free(msg);
    if (!shard->use_fifo)
        almost_full(shard);
    return s;
}


/*
 * If eviction is on and we are evicting, declare ourselves done.  Also, slow
 * ourselves down if needed so we do not post too many items to a single fth
 * mailbox.
 */
static int
all_done(rec_t *rec)
{
    mo_shard_t *shard = rec->shard;

    if (shard->use_fifo) {
        if (shard->fifo.blk_committed >= shard->total_blks)
            return 1;
    } else if (shard->evict_to_free) {
        if (shard->num_slab_evictions)
            return 1;
    }

    while (RV.num_big_post >= RV.max_big_post ||
           RV.num_one_post >= RV.max_big_post) {
        fthYield(0);
    }
    return 0;
}


/*
 * Copy data from flash for a remote container.
 */
static int
ctr_copy_flash(rec_t *rec, mrep_bmap_t *mrep_bmap)
{
    int i;
    int         n = mrep_bmap->nsegs;
    bitmap_t *map = mrep_bmap->bitmap;
    sect_t  *sect = rec->sect;

    for (i = 0; i < n; i++) {
        if (!bit_isset(map, i))
            continue;

        sect->index1 = i;
        sect->index2 = 0;
        do {
            if (all_done(rec))
                break;
            if (!do_sect(rec, HFFGD))
                return 0;
        } while (sect->index2);
        sect->init_slab++;
        progress(rec);
    }
    return 1;
}


/*
 * Copy data from the remote cache.
 */
static int
ctr_copy_cache(rec_t *rec)
{
    sect_t *sect = rec->sect;

    sect->index1 = 0;
    sect->index2 = 0;
    do {
        if (all_done(rec))
            break;
        if (!do_sect(rec, HFFGC))
            return 0;
        sect->init_slab++;
        sect->num_slabs++;
        progress(rec);
    } while (sect->index1 != -1ULL);
    return 1;
}


/*
 * Let the other side know that recovery is complete.
 */
static void
send_done(rec_t *rec)
{
    uint_t             len = sizeof(mreq_done_t);
    uint_t         msg_len = sizeof(sdf_msg_t) + len;
    sdf_msg_t         *msg = malloc_nfw(msg_len, "sdf_msg");
    mreq_done_t *mreq_done = (mreq_done_t *) msg->msg_payload;
    sdf_fth_mbx_t sfm ={
        .actlvl          = SACK_NONE_FTH,
        .release_on_send = 1,
    };

    set_mreq_h(&mreq_done->h, HFFRC);
    mreq_done->shard_id = rec->shard->id;
    msg_seth2n(mreq_done->shard_id);
    bless_n(msg, msg_len);
    (void) sdf_msg_send(msg, len, rec->rank, SDF_FLSH,
                        sdf_msg_myrank(), SDF_RESPONSES, 0, &sfm, NULL);
}


/*
 * Recovery is finished.  Flush and clean up.
 */
static void
rec_done(rec_t *rec)
{
    mo_shard_t *shard = rec->shard;
    uint_t   buf_size = RV.num_rsx * sizeof(setx_t);
    uint_t   map_size = bit_bytes(shard->hash_size / Mcd_osd_bucket_size);
    uint_t group_size = (RV.l2_max_slab_blks+1) * sizeof(obj_group_t);

    rsx_done(rec);
    send_done(rec);
    free_n(rec->rsx_buf,  buf_size, "N*setx");
    free_n(rec->lock_map, map_size, "bitmap");
    free_n(rec->group,    group_size, "obj_group");
}


/*
 * Initialise recovery information.
 */
static void
rec_init(rec_t *rec, pai_t *pai, vnode_t rank, struct shard *s, sect_t *sect)
{
    mo_shard_t *shard = (mo_shard_t *) s;
    uint_t   map_size = bit_bytes(shard->hash_size / Mcd_osd_bucket_size);
    uint_t group_size = (RV.l2_max_slab_blks+1) * sizeof(obj_group_t);

    clear(*rec);
    rec->pai      = pai;
    rec->shard    = shard;
    rec->group    = malloc_nfw(group_size, "obj_group");
    rec->lock_map = malloc_nfw(map_size, "bitmap"),
    rec->sect     = sect;
    rec->rank     = rank;
    rec->map_size = map_size;
    rec->rsx_num  = RV.num_rsx;
    rec->rsx_buf  = malloc_nfw(RV.num_rsx * sizeof(setx_t), "N*setx");
    memset(rec->group, 0, group_size);
    fthLockInit(&rec->rsx_lock);
}


/*
 * Copy a container from the surviving node.  Return -1 if we do not want to
 * use fast recovery.  Otherwise, return 1 on success and 0 on failure.  We
 * hijack the init_slab and num_slabs entries in sect_t as the number of
 * segments transferred and the total segments respectively for the progress
 * indicator.
 */
static int
ctr_copy(vnode_t rank, struct shard *shardx, pai_t *pai)
{
    rec_t rec;
    mrep_bmap_t *mrep_bmap;
    sdf_msg_t *msg_mrep_bmap;
    int             s = 0;
    sect_t       sect = {};

    if (AV.no_fast)
        return -1;
    sdf_logi(70001, "using fast recovery");

    rec_init(&rec, pai, rank, shardx, &sect);
    msg_mrep_bmap = get_remote_bmap(&rec);
    if (!msg_mrep_bmap)
        goto err;

    mrep_bmap = (mrep_bmap_t *) msg_mrep_bmap->msg_payload;
    sect.num_slabs = bit_count(mrep_bmap->bitmap, mrep_bmap->nsegs);

    fth_init(rec.shard);
    if (!ctr_copy_cache(&rec))
        goto err;
    if (!ctr_copy_flash(&rec, mrep_bmap))
        goto err;
    coal_group_flush(&rec, 0);

    sect.init_slab = 1;
    sect.num_slabs = 1;
    progress(&rec);
    s = 1;

err:
    fth_stop();
    if (msg_mrep_bmap)
        sdf_msg_free(msg_mrep_bmap);
    sf_exit();
    rec_done(&rec);

    if (AV.show_stats)
        show_stats(1);
    return s;
}


/*
 * Indicate that a delete has taken place on a given container.  We call
 * ssx_lock to ensure that we do not set ssx_mdel while another thread is in
 * the middle of reading data which might result in confusion.  There is more
 * room for optimization here by turning off ssx_mdel once we know that all
 * outstanding deletes are written on both sides.
 */
static void
prep_del(vnode_t rank, struct shard *shard)
{
    cntr_t *cntr;
    setx_t  mdel;
    wait_t *wait;
    sur_t sur ={
        .rank  = rank,
        .shard = (mo_shard_t *) shard,
    };

    wait = cntr_lock(&sur, 0);
    if (!wait)
        return;

    cntr = sur.cntr;
    mdel = cntr->ssx_mdel;
    if (!mdel) {
        ssx_lock(cntr, 0);
        mdel = cntr->ssx_mdel = cntr->ssx_head;
        ssx_unlock(cntr);
    }

    while (cntr->ssx_tail < mdel) {
        fthUnlock(wait);
        fthYield(0);
        wait = cntr_lock(&sur, 0);
        if (!wait)
            return;
    }
    fthUnlock(wait);
}


/*
 * Fill a HFNOP structure.
 */
static void
nop_fill(sdf_hfnop_t *nop)
{
    nop->rec_ver = AV.no_fast ? 0 : VER_REC;
}


/*
 * Liveness callback function.  Clean up if a node dies.
 */
static void
liveness(int live, int rank, void *arg)
{
    if (!live)
        sur_clean(rank, NULL);
}


/*
 * Unused functions for now.  This is never called but prevents warning
 * messages from the compiler.
 */
static void
unused(void)
{
    mobj_show(0, 0);
    flash_msg_type(0);
    mrep_sect_show(0);
    bit_isset_range(0, 0, 0);
}


/*
 * Clean up after recovery.
 */
void
qrecovery_exit(void)
{
    msg_livecall(0, 1, liveness, NULL);
    fthMboxTerm(&RV.one_mbox);
    fthMboxTerm(&RV.big_mbox);
    aioctx_exit();
}


/*
 * Initialize for recovery.
 */
void
qrecovery_init(void)
{
    int numbigfth;
    sdf_rec_funcs = &Funcs;

    numbigfth           = get_prop_ulong("REC_NUM_BIG_FTH",    -1);
    AV.no_fast          = get_prop_ulong("REC_NO_FAST",        0);
    AV.show_stats       = get_prop_ulong("REC_SHOW_STATS",     0);
    SV.num_ssx          = get_prop_ulong("REC_NUM_SSX",        NUM_SSX);
    SV.no_del_opt       = get_prop_ulong("REC_NO_DEL_OPT",     0);
    RV.num_rsx          = get_prop_ulong("REC_NUM_RSX",        NUM_RSX);
    RV.num_fth          = get_prop_ulong("REC_NUM_FTH",        NUM_FTH);
    RV.min_slabs        = get_prop_ulong("REC_MIN_SLABS",      MIN_SLABS);
    RV.max_slabs        = get_prop_ulong("REC_MAX_SLABS",      MAX_SLABS);
    RV.no_coalesce      = get_prop_ulong("REC_NO_COALESCE",    0);
    RV.max_big_post     = get_prop_ulong("REC_MAX_BIG_POST",   MAX_BIG_POST);
    RV.max_one_post     = get_prop_ulong("REC_MAX_ONE_POST",   MAX_ONE_POST);
    RV.fth_stack_size   = get_prop_ulong("REC_FTH_STACK_SIZE", FTH_STACK_SIZE);
    RV.l2_max_slab_blks = get_prop_ulong("REC_L2_MAX_SLAB_BLKS",
                                         L2_MAX_SLAB_BLKS);

    RV.num_ask_big_fth = numbigfth < 0 ? RV.num_fth / 2 : numbigfth;
    if (RV.num_ask_big_fth == 0)
        RV.no_coalesce = 1;

    if (RV.num_fth < 1)
        fatal("REC_NUM_FTH (%d) must be at least 1", RV.num_fth);
    if (RV.min_slabs < 1)
        fatal("REC_MIN_SLABS (%d) must be at least 1", RV.min_slabs);
    if (RV.max_big_post < 1)
        fatal("REC_MAX_BIG_POST (%d) must be at least 1", RV.max_big_post);
    if (RV.max_one_post < 1)
        fatal("REC_MAX_ONE_POST (%d) must be at least 1", RV.max_one_post);
    if (RV.num_rsx < 1)
        fatal("REC_SETX_MSG_NUM (%d) must be at least 1", RV.num_rsx);
    if (RV.num_ask_big_fth >= RV.num_fth) {
        fatal("REC_NUM_BIG_FTH (%d) >= REC_NUM_FTH (%d)",
              RV.num_ask_big_fth, RV.num_fth);
    }

    fthMboxInit(&RV.one_mbox);
    fthMboxInit(&RV.big_mbox);
    fthLockInit(&RV.aioctx_lock);
    fthLockInit(&SV.cntrs_lock);
    msg_livecall(1, 1, liveness, NULL);
    if (0)
        unused();
}


/*
 * Unencode a block count stored in a hash entry.
 */
static inline uint64_t
lba_to_blk(uint32_t nb)
{
    if (nb < MCD_OSD_MAX_BLKS_OLD)
        return nb;
    return (nb & MCD_OSD_LBA_SHIFT_MASK) << MCD_OSD_LBA_SHIFT_BITS;
}


/*
 * Given a data block, extract the key and data and return it.
 */
static FDF_status_t
e_extr_obj(e_state_t *es, time_t now, char **key, uint32_t *keylen,
	   char **data, uint64_t  *datalen)
{
    mo_meta_t     *meta = (mo_meta_t *) es->data_buf_align;
    const uint64_t mlen = sizeof(*meta);
    const uint64_t klen = meta->key_len;
    const uint64_t dlen = meta->data_len;

    if (mlen + klen + dlen > MCD_OSD_SEGMENT_SIZE)
        return FDF_OBJECT_UNKNOWN;

    if (meta->magic != MCD_OSD_META_MAGIC)
        return FDF_OBJECT_UNKNOWN;

    if (meta->version != MCD_OSD_META_VERSION)
        return FDF_OBJECT_UNKNOWN;

    if (meta->expiry_time && meta->expiry_time <= now)
        return FDF_OBJECT_UNKNOWN;

    char *kptr = plat_malloc(klen);
    if (!kptr)
        return FDF_FAILURE_MEMORY_ALLOC;

    char *dptr = plat_malloc(dlen);
    if (!dptr) {
        plat_free(kptr);
        return FDF_FAILURE_MEMORY_ALLOC;
    }

    memcpy(kptr, es->data_buf_align + mlen, klen);
    memcpy(dptr, es->data_buf_align + mlen + klen, dlen);

    *key = kptr;
    *keylen = klen;
    *data = dptr;
    *datalen = dlen;
    return FDF_SUCCESS;
}


/*
 * Notify the user of an enumeration error.  This should never happen.
 */
static void
e_enum_error(e_state_t *es)
{
    sdf_loge(PLAT_LOG_ID_INITIAL, "enumeration error %ld >= %ld",
             es->hash_buf_i, es->hash_buf_n);
}


/*
 * Fill our internal hash buffer with hash entries from all buckets and
 * overflow entries within a given lock that correspond to the container we are
 * interested in.  When called, we should always have enough room to fill the
 * current bucket.
 */
static void
e_hash_fill(pai_t *pai, void *state, int lock_i)
{
    int b;
    int n;
    mo_hash_t *hash;
    e_state_t          *es = (e_state_t *) state;
    mo_shard_t      *shard = es->shard;
    uint64_t bkts_per_lock = shard->lock_bktsize / Mcd_osd_bucket_size;
    cntr_id_t      cntr_id = es->cguid;
    fthLock_t        *lock = &shard->bucket_locks[lock_i];
    wait_t           *wait = fthLock(lock, 0, NULL);

    for (b = 0; b < bkts_per_lock; b++) {
        uint64_t      bkt_i = lock_i * bkts_per_lock + b;
        mo_bucket_t *bucket = &shard->hash_buckets[bkt_i];

        hash = &shard->hash_table[bkt_i * Mcd_osd_bucket_size];
        for (n = bucket->next_item; n--; hash++) {
            if (hash->cntr_id != cntr_id)
                continue;
            if (es->hash_buf_i < es->hash_buf_n)
                es->hash_buf[es->hash_buf_i++] = *hash;
            else {
                e_enum_error(es);
                break;
            }
        }
    }

    hash = &shard->overflow_table[lock_i * Mcd_osd_overflow_depth];
    for (n = Mcd_osd_overflow_depth; n--; hash++) {
        if (hash->cntr_id != cntr_id)
            continue;
        if (es->hash_buf_i < es->hash_buf_n)
            es->hash_buf[es->hash_buf_i++] = *hash;
        else {
            e_enum_error(es);
            break;
        }
    }

    fthUnlock(wait);
}


/*
 * Return the next enumerated object in a container.
 */
FDF_status_t
enumerate_next(pai_t *pai, void *state, char **key, uint32_t *keylen,
	       char **data, uint64_t  *datalen)
{
    time_t        now = time(NULL);
    e_state_t     *es = (e_state_t *) state;
    mo_shard_t *shard = es->shard;
    cntr_id_t cntr_id = es->cguid;

    for (;;) {
        while (!es->hash_buf_i) {
            if (es->hash_lock_i >= shard->lock_buckets)
                return FDF_OBJECT_UNKNOWN;
            e_hash_fill(pai, state, es->hash_lock_i++); }

        int s;
        mo_hash_t *hash = &es->hash_buf[--es->hash_buf_i];
        if (!hash->used || hash->cntr_id != cntr_id)
            continue;

        uint64_t nb = lba_to_blk(hash->blocks);
        if (nb > MCD_OSD_SEGMENT_BLKS)
            return FDF_FLASH_EINVAL;

        s = read_disk(shard, (aioctx_t *)&pai->ctxt,
                      es->data_buf_align, hash->address, nb);
        if (!s)
            return FDF_FLASH_EINVAL;

        s = e_extr_obj(es, now, key, keylen, data, datalen);
        if (s == FDF_OBJECT_UNKNOWN)
            continue;
        return s;
    }
}


/*
 * Finish enumeration of a container.
 */
FDF_status_t
enumerate_done(pai_t *pai, e_state_t *es)
{
    if (es->data_buf_alloc)
        plat_free(es->data_buf_alloc);
    if (es->hash_buf)
        plat_free(es->hash_buf);
    plat_free(es);
    return 0;
}


/*
 * Return an error indicating we could not allocate memory while initializing
 * and clean up.
 */
static FDF_status_t
e_init_fail(pai_t *pai, e_state_t *es)
{
    if (es)
        enumerate_done(pai, (void *)es);
    return FDF_FAILURE_MEMORY_ALLOC;
}


/*
 * Prepare for the enumeration of all objects in a container.
 */
FDF_status_t
enumerate_init(pai_t *pai, struct shard *shard_arg,
               FDF_cguid_t cguid, e_state_t **esp)
{
    mo_shard_t *shard = (mo_shard_t *) shard_arg;

    e_state_t *es = plat_malloc(sizeof(*es));
    if (!es)
        return e_init_fail(pai, es);
    memset(es, 0, sizeof(*es));

    es->data_buf_alloc = plat_malloc(MCD_OSD_SEGMENT_SIZE + BLK_SIZE-1);
    if (!es->data_buf_alloc)
        return e_init_fail(pai, es);
    es->data_buf_align = chunk_next_ptr(es->data_buf_alloc, BLK_SIZE);

    es->hash_per_lock = shard->lock_bktsize + Mcd_osd_overflow_depth;
    es->hash_buf_n    = E_ROOM_FOR_LOCKS * es->hash_per_lock;

    es->hash_buf = plat_malloc(es->hash_buf_n * sizeof(mo_hash_t));
    if (!es->hash_buf)
        return e_init_fail(pai, es);

    es->shard = shard;
    es->cguid = cguid;
    *((e_state_t **) esp) = es;
    return 0;
}


/*
 * Go through the hash table computing the number of objects and bytes used by
 * each of the containers.
 */
void
set_cntr_sizes(pai_t *pai, shard_t *shard_arg)
{
    typedef struct {
        uint64_t bytes;
        uint64_t objects;
    } info_t;
    uint64_t n;
    mo_shard_t *shard = (mo_shard_t *) shard_arg;

    if (sizeof(cntr_id_t) != 2)
        fatal("sizeof cntr_id must be 2");

    uint64_t cntr_n = 1 << (sizeof(cntr_id_t) * 8);
    info_t *cntr_p = plat_alloc(cntr_n * sizeof(info_t));
    if (!cntr_p)
        fatal("Cannot allocate container information");
    memset(cntr_p, 0, cntr_n * sizeof(info_t));

    mo_hash_t *hash = shard->hash_table;
    for (n = shard->hash_size; n--; hash++) {
        uint64_t blks = mcd_osd_lba_to_blk_x(hash->blocks);
        blks = mcd_osd_blk_to_use(shard, blks);
        if (!hash->used)
            continue;
        info_t *p = &cntr_p[hash->cntr_id];
        p->objects++;
        p->bytes += blks * MCD_OSD_BLK_SIZE;
    }

    info_t *p = cntr_p;
    for (n = 0; n < cntr_n; n++, p++) {
        if (!p->objects)
            continue;
        inc_cntr_map(n, p->objects, p->bytes);

        uint64_t objs;
        uint64_t used;
        uint64_t size;
        char name[256];
        if (!get_cntr_info(n, name, sizeof(name), &objs, &used, &size))
            sdf_loge(PLII, "Failed on get_cntr_info for container %ld", n);
        else {
            if (size) {
                sdf_logi(PLII, "Container %s: id=%ld objs=%ld used=%ld"
                               " size=%ld full=%.1f%%",
                         name, n, objs, used, size, used*100.0/size);
            } else {
                sdf_logi(PLII, "Container %s: id=%ld objs=%ld used=%ld",
                         name, n, objs, used);
            }
        }
    }

    plat_free(cntr_p);
}
