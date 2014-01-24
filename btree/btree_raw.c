/************************************************************************
 * 
 *  btree_raw.c  Jan. 21, 2013   Brian O'Krafka
 * 
 * xxxzzz NOTES:
 *     - check all uses of "ret"
 *     - make sure that all btree updates are logged!
 *     - add doxygen comments for all functions
 *     - make asserts a compile time option
 *     - make sure that left/right node pointers are properly maintained
 *     - check insert_ptr arithmetic
 *     - optimize key search within a node?
 *     - how is logical_id_counter persisted?
 *     - where is rightmost updated?
 *     - what if multiple matches for a particular syndrome?  must be
 *       be able to return multiple matches--NO--MUST JUST CHECK MULTIPLE
 *       MATCHES INTERNALLY TO FIND EXACT MATCH
 *     - is there a need to support non-uniqueue keys?
 *       if not, must enforce uniqueness somehow
 *     - add upsert flag and support
 *     - add check that keylen/datalen are correct when using 
 *       size fixed keys/data
 *     - if btree_raw provides returned data/key buffer, a special btree_raw_free_buffer() call
 *       should be used to free the buffer(s).  This will allow optimized buffer management
 *       allocation/deallocation methpnode (that avoid malloc) in the future.
 *     - modularize l1cache stuff
 *     - add free buffer callback
 *     - add get buffer callback
 *     - optimize updates to manipulate btree node in a single operation
 *     - if updates decrease node size below a threshold, must coalesce nodes!!!
 *     - stash overflow key in overflow node(s)!
 *     - use "right-sized" FDF objects for overflow objects, without chaining fixed
 *       sized nodes!
 *     - add 'leftmost' pointers for use with leaf nodes for reverse scans and
 *       simplified update of 'rightmost' pointers
 *     - where is max key length enforced?
 *     - add stats
 *     - add upsert (set) function to btree_raw_write()
 *     - change chunk size in DRAM cache code to match btree node size!
 *     - improve object packing in b-tree nodes
 * 
 * Flavors of b-tree:
 *     - Syndrome search + variable sized keys with variable sized data (primary index)
 *       ((btree->flags & SYNDROME_INDEX) == 0)
 *         - non-leaf nodes: fixed length syndrom, no data
 *         - leaf nodes: fixed length syndrom + variable length key + variable length data
 *     - Variable sized keys with variable sized data (secondary indices)
 *       ((btree->flags & SECONDARY_INDEX) == 0)
 *         - non-leaf nodes: fixed length key, no data
 *         - leaf nodes: fixed length syndrom + variable length key + variable length data
 * 
 ************************************************************************/

//  This instantiates the stats string array
#define _INSTANTIATE_BTSTATS_STRINGS

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <inttypes.h>
#include <assert.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include "btree_hash.h"
#include "btree_raw.h"
#include "btree_map.h"
#include "btree_pmap.h"
#include "btree_raw_internal.h"
#include <api/fdf.h>
#include "btree_sync_th.h"
#include "fdf.h"
#include <pthread.h>
#include "btree_var_leaf.h"
#include "btree_malloc.h"
#include "packet.h"
#include "trxcmd.h"
#include "flip/flip.h"

//  Define this to include detailed debugging code
//#define BTREE_RAW_CHECK
#ifdef DEBUG_STUFF
static int Verbose = 1;
#else
static int Verbose = 0;
#endif

#define W_UPDATE  1
#define W_CREATE  2
#define W_SET     4
#define W_ASC     8
#define W_DESC    16
#define W_APPEND  32

#define RIGHT 0
#define LEFT 1

#define READ 0
#define WRITE 1

#define MODIFY_TREE 1
#define META_COUNTER_SAVE_INTERVAL 100000

#define MPUT_BATCH_SIZE 128

extern uint64_t n_global_l1cache_buckets;
extern struct PMap *global_l1cache;
extern int btree_parallel_flush_disabled;
extern int btree_parallel_flush_minbufs;
extern uint64_t fdf_flush_pstats_frequency;
extern uint64_t l1cache_size;
extern uint64_t l1cache_partitions;

//  used to count depth of btree traversal for writes/deletes
__thread int _pathcnt;

//  used to hold key values during delete or write operations
__thread char      *_keybuf      = NULL;
__thread uint32_t   _keybuf_size = 0;

#define MAX_BTREE_HEIGHT 6400
extern __thread struct FDF_thread_state *my_thd_state;
__thread btree_raw_mem_node_t* modified_nodes[MAX_BTREE_HEIGHT];
__thread btree_raw_mem_node_t* referenced_nodes[MAX_BTREE_HEIGHT];
__thread btree_raw_mem_node_t* deleted_nodes[MAX_BTREE_HEIGHT];
__thread int modified_written[MAX_BTREE_HEIGHT];
__thread int deleted_written[MAX_BTREE_HEIGHT];
__thread uint64_t modified_nodes_count=0, referenced_nodes_count=0, deleted_nodes_count=0;

static __thread char tmp_key_buf[8100] = {0};
__thread uint64_t dbg_referenced = 0;

static __thread uint64_t _pstats_ckpt_index = 0;

extern struct FDF_state *FDFState;

#ifdef FLIP_ENABLED
bool recovery_write = false;
#endif

#define bt_err(msg, args...) \
    (bt->msg_cb)(0, 0, __FILE__, __LINE__, msg, ##args)
#define bt_warn(msg, args...) \
    (bt->msg_cb)(1, 0, __FILE__, __LINE__, msg, ##args)

#define zmemcpy(to_in, from_in, n_in)  \
{\
    uint64_t zi;\
    uint64_t zn = (n_in);\
    char  *zto = ((char *) to_in);\
    char  *zfrom = ((char *) from_in);\
    for (zi=0; zi<zn; zi++) {\
        *zto++ = *zfrom++;\
    }\
}

#define zmemmove(to_in, from_in, n_in)  \
{\
    uint64_t zi;\
    uint64_t zn = (n_in);\
    char  *zto = ((char *) to_in);\
    char  *zfrom = ((char *) from_in);\
    if (zto < zfrom) {\
	for (zi=0; zi<zn; zi++) {\
	    *zto++ = *zfrom++;\
	}\
    } else {\
        zto   += zn;\
        zfrom += zn;\
	for (zi=0; zi<zn; zi++) {\
	    *(--zto) = *(--zfrom);\
	}\
    }\
}

#define add_node_stats(bt, pn, s, c) \
{ \
    if (pn->flags & OVERFLOW_NODE) \
        __sync_add_and_fetch(&(bt->stats.stat[BTSTAT_OVERFLOW_##s]),c); \
    else if (pn->flags & LEAF_NODE) \
        __sync_add_and_fetch(&(bt->stats.stat[BTSTAT_LEAF_##s]),c); \
    else \
        __sync_add_and_fetch(&(bt->stats.stat[BTSTAT_NONLEAF_##s]),c); \
}

#define sub_node_stats(bt, pn, s, c) \
{ \
    if (pn->flags & OVERFLOW_NODE) \
        __sync_sub_and_fetch(&(bt->stats.stat[BTSTAT_OVERFLOW_##s]),c); \
    else if (pn->flags & LEAF_NODE) \
        __sync_sub_and_fetch(&(bt->stats.stat[BTSTAT_LEAF_##s]),c); \
    else \
        __sync_sub_and_fetch(&(bt->stats.stat[BTSTAT_NONLEAF_##s]),c); \
}

#define stats_inc(bt, s, val)	(void)__sync_add_and_fetch(&(bt->stats.stat[s]), val);
#define stats_dec(bt, s, val)	(void)__sync_sub_and_fetch(&(bt->stats.stat[s]), val);

#ifdef DEBUG_STUFF
static void dump_node(btree_raw_t *bt, FILE *f, btree_raw_node_t *n, char *key, uint32_t keylen);
static char *dump_key(char *key, uint32_t keylen);
#endif

#define vlnode_bytes_free(x) ((x)->insert_ptr - sizeof(btree_raw_node_t) - (x)->nkeys * sizeof(node_vlkey_t))
#define vnode_bytes_free(x) ((x)->insert_ptr - sizeof(btree_raw_node_t) - (x)->nkeys * sizeof(node_vkey_t))

#define big_object(bt, x) (((x)->keylen + (x)->datalen) >= (bt)->big_object_size)
#define big_object_kd(bt, k, d) ((k + d) >= (bt)->big_object_size)
#define object_inline_size(bt, ks) ((ks)->keylen + (((ks)->leaf && !big_object_kd((bt), (ks)->keylen, (ks)->datalen)) ? (ks)->datalen : 0))

static uint64_t get_syndrome(btree_raw_t *bt, char *key, uint32_t keylen);

static bool
find_key(btree_raw_t *bt, btree_raw_node_t *n, char *key_in, uint32_t keylen_in,
         btree_metadata_t *meta, uint64_t syndrome,
         uint64_t *child_id, uint64_t *child_id_before, uint64_t *child_id_after,
         int32_t *index);
static btree_status_t loadpersistent( btree_raw_t *);
static void free_node(btree_status_t *ret, btree_raw_t *btree, btree_raw_mem_node_t *n);

static inline btree_raw_mem_node_t *get_new_node(btree_status_t *ret, btree_raw_t *btree, uint32_t leaf_flags, uint16_t level);
static btree_raw_mem_node_t *create_new_node(btree_raw_t *btree, uint64_t logical_id, bool ref, bool pinned);
//static btree_raw_mem_node_t *get_new_node_low(btree_status_t *ret, btree_raw_t *btree, uint32_t leaf_flags, int ref);

int init_l1cache();
void destroy_l1cache();
void clean_l1cache(btree_raw_t* btree);
uint32_t get_btree_node_size();

btree_status_t deref_l1cache(btree_raw_t *btree);
static void btree_sync_flush_entry(btree_raw_t *btree, struct FDF_thread_state *thd_state, btSyncRequest_t *list);
static void btree_sync_remove_entry(btree_raw_t *btree, btSyncRequest_t *list);
static void modify_l1cache_node(btree_raw_t *btree, btree_raw_mem_node_t *n);

static void lock_modified_nodes_func(btree_raw_t *btree, int lock);
#define lock_modified_nodes(btree) lock_modified_nodes_func(btree, 1)
#define unlock_modified_nodes(btree) lock_modified_nodes_func(btree, 0)

static void update_keypos(btree_raw_t *btree, btree_raw_node_t *n, uint32_t n_key_start);

typedef struct deldata {
    btree_raw_mem_node_t   *balance_node;
} deldata_t;

static btree_status_t writepersistent( btree_raw_t *bt, bool create, bool force_write);

static void delete_key(btree_status_t *ret, btree_raw_t *btree, btree_raw_mem_node_t *x, char *key, uint32_t keylen, uint64_t seqno, uint64_t syndrome);
static void delete_key_by_pkrec(btree_status_t *ret, btree_raw_t *btree, btree_raw_mem_node_t *x, node_key_t *pk_delete);

static int find_rebalance(btree_status_t *ret, btree_raw_t *btree, uint64_t this_id, uint64_t left_id, uint64_t right_id, uint64_t l_anchor_id, key_stuff_t *l_anchor_stuff, uint64_t r_anchor_id, key_stuff_t *r_anchor_stuff, int l_this_parent_in, int r_this_parent_in, char *key, uint32_t keylen, btree_metadata_t *meta, uint64_t syndrome);
static void collapse_root(btree_status_t *ret, btree_raw_t *btree, btree_raw_mem_node_t *old_root_node);
static int rebalance(btree_status_t *ret, btree_raw_t *btree, btree_raw_mem_node_t *this_node, uint64_t left_id, uint64_t right_id, uint64_t l_anchor_id, key_stuff_t *l_anchor_stuff, uint64_t r_anchor_id, key_stuff_t *r_anchor_stuff, int l_this_parent, int r_this_parent);
static int
equalize_keys(btree_raw_t *btree, btree_raw_mem_node_t *anchor_mem, btree_raw_mem_node_t *left_mem,
	      btree_raw_mem_node_t *right_mem, char *s_key, uint32_t s_keylen, uint64_t s_syndrome,
	      uint64_t s_seqno, char **r_key, uint32_t *r_keylen, uint64_t *r_syndrome, uint64_t *r_seqno,
	      int left, bool * free_key);
static int check_per_thread_keybuf(btree_raw_t *btree);
static void btree_raw_init_stats(struct btree_raw *btree, btree_stats_t *stats, fdf_pstats_t *pstats);
#ifdef DEBUG_STUFF
static void btree_raw_dump(FILE *f, struct btree_raw *btree);
#endif

#ifdef BTREE_RAW_CHECK
static void btree_raw_check(struct btree_raw *btree, char* func, char* key);
#endif

void btree_sync_thread(uint64_t arg);

static void reset_node_pstats(btree_raw_node_t *pnode);
static void pstats_flush(struct btree_raw *btree, btree_raw_mem_node_t *n);
static void set_node_pstats(btree_raw_t *btree, btree_raw_node_t *pnode, uint64_t num_obj, bool is_delta_positive);

static void default_msg_cb(int level, void *msg_data, char *filename, int lineno, char *msg, ...)
{
    char     stmp[512];
    va_list  args;
    char    *prefix;
    int      quit = 0;

    va_start(args, msg);

    vsprintf(stmp, msg, args);
    strcat(stmp, "\n");

    va_end(args);

    switch (level) {
        case 0:  prefix = "ERROR";                quit = 1; break;
        case 1:  prefix = "WARNING";              quit = 0; break;
        case 2:  prefix = "INFO";                 quit = 0; break;
        case 3:  prefix = "DEBUG";                quit = 0; break;
        default: prefix = "PROBLEM WITH MSG_CB!"; quit = 1; break;
	    break;
    } 

    (void) fprintf(stderr, "%s: %s", prefix, stmp);
    if (quit) {
        exit(1);
    }
}

static int default_cmp_cb(void *data, char *key1, uint32_t keylen1, char *key2, uint32_t keylen2)
{
    if (keylen1 < keylen2) {
        return(-1);
    } else if (keylen1 > keylen2) {
        return(1);
    } else if (keylen1 == keylen2) {
        return(memcmp(key1, key2, keylen1));
    }
    assert(0);
    return(0);
}

//======================   INIT  =========================================

static void
l1cache_replace(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen)
{
    btree_raw_mem_node_t *n = (btree_raw_mem_node_t*)pdata;

    free_buffer((btree_raw_t *) callback_data, (void*)n->pnode);

    plat_rwlock_destroy(&n->lock);
    free(n);
}


/*
 * This function is called at recovery time and finds out valid in-flight
 * objects at crash time.
 */ 
static void
btree_raw_crash_stats( btree_raw_t* bt, void *data, uint32_t datalen )
{
    assert( bt );
    assert( data );
    assert( datalen > 0 );

    fdf_pstats_delta_t *pstats_new = (fdf_pstats_delta_t*) data;
#ifdef PSTATS_1
    fprintf(stderr, "btree_raw_crash_stats: Last seq num = %ld\n", bt->pstats.seq_num);
    fprintf(stderr, "btree_raw_crash_stats: record: seq num=%ld d_obj_cnt=%ld is_positive_d=0x%x d_num_snap_objs=%ld d_snap_data_size=%ld unique=%ld\n",
                 pstats_new->seq_num, pstats_new->delta[PSTAT_OBJ_COUNT], pstats_new->is_pos_delta, pstats_new->delta[PSTAT_NUM_SNAP_OBJS], pstats_new->delta[PSTAT_SNAP_DATA_SIZE], pstats_new->seq); 
#endif

    if ( pstats_new->seq_num >= bt->pstats.seq_num) {
#ifdef PSTATS_1
		static uint64_t count = 0;
		int flag =0;
#endif
		if (pstats_new->delta[PSTAT_OBJ_COUNT] > 0 ) {
#ifdef PSTATS_1
			fprintf(stderr, "btree_raw_crash_stats: Valid object found: count=%ld\n", count++);
			flag =1;
#endif
			if ((pstats_new->is_pos_delta | (0x1 << PSTAT_OBJ_COUNT)) != 0) {
				bt->pstats.obj_count += pstats_new->delta[PSTAT_OBJ_COUNT];
			} else {
				bt->pstats.obj_count -= pstats_new->delta[PSTAT_OBJ_COUNT];
			}
		} 
		if (pstats_new->delta[PSTAT_NUM_SNAP_OBJS] > 0) {
#ifdef PSTATS_1
			if (flag ==0) {
				fprintf(stderr, "btree_raw_crash_stats: Valid object found: count=%ld\n", count++);
				flag =1;
			}
#endif
			if ((pstats_new->is_pos_delta | (0x1 << PSTAT_NUM_SNAP_OBJS)) != 0) {
				bt->pstats.num_snap_objs += pstats_new->delta[PSTAT_NUM_SNAP_OBJS];
			} else {
				bt->pstats.num_snap_objs -= pstats_new->delta[PSTAT_NUM_SNAP_OBJS];
			}
		}
		if (pstats_new->delta[PSTAT_SNAP_DATA_SIZE] > 0) {
#ifdef PSTATS_1
			if (flag ==0) {
				fprintf(stderr, "btree_raw_crash_stats: Valid object found: count=%ld\n", count++);
				flag =1;
			}
#endif
			if ((pstats_new->is_pos_delta | (0x1 << PSTAT_SNAP_DATA_SIZE)) != 0 ) {
				bt->pstats.snap_data_size += pstats_new->delta[PSTAT_SNAP_DATA_SIZE];
			} else {
				bt->pstats.snap_data_size -= pstats_new->delta[PSTAT_SNAP_DATA_SIZE];
			}
		}

    }
}


btree_raw_t *
btree_raw_init(uint32_t flags, uint32_t n_partition, uint32_t n_partitions, uint32_t max_key_size, uint32_t min_keys_per_node, uint32_t nodesize, create_node_cb_t *create_node_cb, void *create_node_data, read_node_cb_t *read_node_cb, void *read_node_cb_data, write_node_cb_t *write_node_cb, void *write_node_cb_data, flush_node_cb_t *flush_node_cb, void *flush_node_cb_data, freebuf_cb_t *freebuf_cb, void *freebuf_cb_data, delete_node_cb_t *delete_node_cb, void *delete_node_data, log_cb_t *log_cb, void *log_cb_data, msg_cb_t *msg_cb, void *msg_cb_data, cmp_cb_t *cmp_cb, void * cmp_cb_data, bt_mput_cmp_cb_t mput_cmp_cb, void *mput_cmp_cb_data, trx_cmd_cb_t *trx_cmd_cb, uint64_t cguid, fdf_pstats_t *pstats, seqno_alloc_cb_t *ptr_seqno_alloc_cb)
{
    btree_raw_t      *bt;
    uint32_t          nbytes_meta;
    btree_status_t    ret = BTREE_SUCCESS;
	int					i, sync_threads;
	char				*env;

    dbg_print("start dbg_referenced %ld\n", dbg_referenced);

    bt = (btree_raw_t *) malloc(sizeof(btree_raw_t));
    if (bt == NULL) {
        return(NULL);
    }
    bt->version = BTREE_VERSION;

    if(global_l1cache){
        bt->l1cache = global_l1cache;
    } else {
        return NULL;
    }

    if(n_global_l1cache_buckets)
        bt->n_l1cache_buckets = n_global_l1cache_buckets;

    bt->cguid = cguid;

    if (flags & VERBOSE_DEBUG) {
        Verbose = 1;
    }

    bt->n_partition          = n_partition;
    bt->n_partitions         = n_partitions;
    bt->flags                = flags;
    bt->max_key_size         = max_key_size;
    bt->min_keys_per_node    = min_keys_per_node;;
    bt->nodesize             = nodesize;
    bt->nodesize_less_hdr    = nodesize - sizeof(btree_raw_node_t);
    // bt->big_object_size      = (nodesize - sizeof(btree_raw_mem_node_t))/2; // xxxzzz check this
    bt->big_object_size      = bt->nodesize_less_hdr / 4 - sizeof(node_vlkey_t); // xxxzzz check this
    dbg_print("nodesize_less_hdr=%d nodezie=%d raw_node_size=%ld\n", bt->nodesize_less_hdr, nodesize, sizeof(btree_raw_node_t));
    bt->logical_id_counter   = 1;
    bt->next_logical_id	     = META_COUNTER_SAVE_INTERVAL; 
    bt->create_node_cb       = create_node_cb;
    bt->create_node_cb_data  = create_node_data;
    bt->read_node_cb         = read_node_cb;
    bt->read_node_cb_data    = read_node_cb_data;
    bt->write_node_cb        = write_node_cb;
    bt->write_node_cb_data   = write_node_cb_data;
    bt->flush_node_cb        = flush_node_cb;
    bt->flush_node_cb_data   = flush_node_cb_data;
    bt->freebuf_cb           = freebuf_cb;
    bt->freebuf_cb_data      = freebuf_cb_data;
    bt->delete_node_cb       = delete_node_cb;
    bt->delete_node_cb_data  = delete_node_data;
    bt->log_cb               = log_cb;
    bt->log_cb_data          = log_cb_data;
    bt->msg_cb               = msg_cb;
    bt->msg_cb_data          = msg_cb_data;
    if (msg_cb == NULL) {
        bt->msg_cb           = default_msg_cb;
        bt->msg_cb_data      = NULL;
    }
    bt->cmp_cb               = cmp_cb;
    bt->cmp_cb_data          = cmp_cb_data;
    if (cmp_cb == NULL) {
        bt->cmp_cb           = default_cmp_cb;
        bt->cmp_cb_data      = NULL;
    }

    bt->mput_cmp_cb = mput_cmp_cb;
    bt->mput_cmp_cb_data = mput_cmp_cb_data;

    assert(mput_cmp_cb != NULL);

    bt->trx_cmd_cb           = trx_cmd_cb;
    bt->seqno_alloc_cb = ptr_seqno_alloc_cb;

    if (min_keys_per_node < 4) {
        bt_err("min_keys_per_node must be >= 4");
        free(bt);
        return(NULL);
    }

    bt->fkeys_per_node = (nodesize - sizeof(btree_raw_node_t))/sizeof(node_fkey_t);

    nbytes_meta = sizeof(node_vkey_t);
    if (nbytes_meta < sizeof(node_vlkey_t)) {
        nbytes_meta = sizeof(node_vlkey_t);
    }
    nbytes_meta += max_key_size;
    nbytes_meta *= min_keys_per_node;
    nbytes_meta += sizeof(btree_raw_node_t);

    if (nodesize < nbytes_meta) {
        bt_err("Node size (%d bytes) must be large enough to hold at least %d max sized keys (%d bytes each).", nodesize, min_keys_per_node, max_key_size);
        free(bt);
        return(NULL);
    }

    pthread_rwlock_init(&bt->snap_lock, NULL);

    if (flags & RELOAD) {
        if (BTREE_SUCCESS != loadpersistent( bt)) {
            bt_err( "Could not identify root node!");
            free( bt);
            return (NULL);
        }
    } else {
        bt->rootid = bt->logical_id_counter * bt->n_partitions + bt->n_partition;
        if (BTREE_SUCCESS != savepersistent( bt, true /* create */)) {
            free( bt);
            return (NULL);
        }

        btree_raw_mem_node_t *root_node = get_new_node( &ret, bt, LEAF_NODE, 0);
        if (BTREE_SUCCESS != ret) {
            bt_warn( "Could not allocate root node! %p", root_node);
            free( bt);
            return (NULL);
        }

		assert(root_node->pnode->logical_id == bt->rootid);
        lock_modified_nodes(bt);
    }

	bt->no_sync_threads = 0;
	bt->deleting = 0;
	bt->sync_first = NULL;
	bt->sync_last = NULL;

	if (btree_parallel_flush_disabled == 0) {
		pthread_mutex_init(&(bt->bt_async_mutex), NULL);
		pthread_cond_init(&(bt->bt_async_cv), NULL);
		env = (char *)FDFGetProperty("FDF_BTREE_SYNC_THREADS", NULL);
		sync_threads = env ? (int)atoi(env): 32;
		assert(sync_threads);
		bt->syncthread = (btSyncThread_t **)malloc(sync_threads * sizeof(btSyncThread_t *)); 

		bt->worker_threads = sync_threads;
		bt->io_bufs = bt->io_threads = 0;

		for ( i = 0; i < sync_threads; i++) {
			btSyncResume( btSyncSpawn(bt, i, &btree_sync_thread), (uint64_t)(bt));
		}
		while (bt->no_sync_threads < sync_threads) {
			sched_yield();
		}
	}


    if (BTREE_SUCCESS != deref_l1cache(bt)) {
        ret = BTREE_FAILURE;
    }

#ifdef DEBUG_STUFF
    if (Verbose) {
        btree_raw_dump(stderr, bt);
    }
#endif

    plat_rwlock_init(&bt->lock);
    bt->modified = 0;
    bt->trxenabled = (*bt->trx_cmd_cb)( TRX_ENABLED);

    dbg_print("dbg_referenced %ld\n", dbg_referenced);
    assert(!dbg_referenced);
    dbg_print("bt %p lock %p n_part %d\n", bt, &bt->lock, n_partition);

    /*
     * Persistent stats initialization
     */
    pthread_mutex_init(&bt->pstat_lock, NULL);
    assert(pstats);
    memcpy( &(bt->pstats), pstats, sizeof(fdf_pstats_t) );
    bt->last_flushed_seq_num = pstats->seq_num;
    bt->pstats_modified = false;
    bt->pstat_ckpt.pstat.obj_count = 0;
    bt->pstat_ckpt.pstat.seq_num = 0; 
    bt->active_writes[0] = 0;
    bt->active_writes[1] = 0;
    bt->current_active_write_idx = 0;

    if (flags & RELOAD) {
        /*
         * first, consolidate persistent stats
         */
        stats_packet_t *s = stats_packet_open( cguid);
        if (s) {
            stats_packet_node_t *n;
            while (n = stats_packet_get_node( s)) {
                btree_raw_crash_stats( bt, n->data, n->datalen);
                stats_packet_free_node( n);
            }
            stats_packet_close( s);
            stats_packet_delete( cguid);
        }
        /*
         * second, undo all dangling trx
         */
        rec_packet_t *r = recovery_packet_open( cguid);
        if (r) {
            rec_packet_trx_t *t;
            while (t = recovery_packet_get_trx( r)) {
                btree_recovery_process_minipkt( bt, (btree_raw_node_t **)t->oldnodes, t->oldcount, (btree_raw_node_t **)t->newnodes, t->newcount);
                recovery_packet_free_trx( t);
            }
            recovery_packet_close( r);
            recovery_packet_delete( cguid);
        }
    }
    else {
        stats_packet_delete( cguid);
        recovery_packet_delete( cguid);
    }
    btree_raw_init_stats(bt, &(bt->stats), &(bt->pstats));
	bt->stats.stat[BTSTAT_NUM_SNAPS] = bt->snap_meta->total_snapshots;

    return(bt);
}


void
btree_raw_destroy (struct btree_raw **bt)
{
	int i, syncthreads = (*bt)->no_sync_threads;
	clean_l1cache(*bt);
	(*bt)->deleting = 1;

	if (!btree_parallel_flush_disabled) {
		while ((*bt)->no_sync_threads) {
			pthread_mutex_lock(&((*bt)->bt_async_mutex));
			pthread_cond_broadcast(&((*bt)->bt_async_cv));
			pthread_mutex_unlock(&((*bt)->bt_async_mutex));
			sched_yield();
		}
		for (i = 0; i < syncthreads; i++) {
			free((*bt)->syncthread[i]);
		}
		free((*bt)->syncthread);
	}
	free(*bt);
	*bt = NULL;
}


extern __thread long long locked;

/* Prevent lock in dump_node, btree_raw_dump. It causes hangs. */
#ifdef DEBUG_STUFF
__thread int no_lock = 0;
#endif

inline static
void node_lock(btree_raw_mem_node_t* node, int write_lock) 
{
#ifdef DEBUG_STUFF
	if(no_lock) return;
#endif
	dbg_print("node_id=%ld write_lock %d locked=%lld\n", node->pnode ? node->pnode->logical_id : -1, write_lock, locked);

	if(write_lock)
		plat_rwlock_wrlock(&node->lock);
	else
		plat_rwlock_rdlock(&node->lock);
#ifdef DEBUG_STUFF
	if(write_lock)
		node->lock_id = pthread_self();
#endif
}

inline static
void node_unlock(btree_raw_mem_node_t* node)
{
#ifdef DEBUG_STUFF
	if(no_lock) return;
	node->lock_id = 0;
#endif
	plat_rwlock_unlock(&node->lock);

	dbg_print("node_id=%ld locked=%lld\n", node->pnode->logical_id, locked);
}

/*
 * save persistent btree metadata
 *
 * Info is stored as a btree node with special logical ID. 
 */
#define offsetof(st, m) ((size_t)(&((st *)0)->m))

btree_status_t
savepersistent(btree_raw_t *bt, bool create)
{
	return writepersistent( bt, create, false);
}

btree_status_t
flushpersistent(btree_raw_t *bt)
{
	return writepersistent( bt, false , true);
}


static btree_status_t
writepersistent( btree_raw_t *bt, bool create, bool force_write)
{
    btree_raw_mem_node_t* mem_node;
    btree_status_t ret = BTREE_SUCCESS;
    bool tb_flushed = false;

    if(create) {
        mem_node = create_new_node(bt,
                                   META_LOGICAL_ID+bt->n_partition,
                                   true /* ref */, true /* pinned */);
        if (mem_node == NULL) {
            ret = BTREE_FAILURE;
            goto done;
        }

        btree_raw_persist_t *r = (btree_raw_persist_t*)mem_node->pnode;

        dbg_print("ret=%d nodeid=%lx r->lic %ld r->rootid %ld bt->logical_id_counter %ld\n",
                  ret, META_LOGICAL_ID + bt->n_partition, r->logical_id_counter,
                  r->rootid, r->logical_id_counter + META_COUNTER_SAVE_INTERVAL);
        r->meta_version = META_VERSION;
        bt->snap_meta = &(r->snap_details); /* Keep the pinned snap details node info in btree */
        (void)btree_snap_init_meta(bt, bt->nodesize - offsetof(btree_raw_persist_t, snap_details));

    } else {
        mem_node = get_existing_node_low(&ret, bt,
			                 META_LOGICAL_ID+bt->n_partition,
                                         1, true /* pinned */, false /* deref_delete */);
    }

    if (mem_node) {
		/*
		 * Lock the node to change it.
		 */
		node_lock(mem_node, WRITE);

        btree_raw_persist_t *r = (btree_raw_persist_t*)mem_node->pnode;

        tb_flushed = force_write ||
            r->rootid != bt->rootid || !(bt->logical_id_counter % META_COUNTER_SAVE_INTERVAL);
        dbg_print("ret=%d create=%d nodeid=%lx lic=%ld rootid=%ld save=%d\n",
                  ret, create, META_LOGICAL_ID+bt->n_partition, bt->logical_id_counter,
                  bt->rootid,
                  tb_flushed);


		bt->stats.stat[BTSTAT_NUM_SNAPS] = bt->snap_meta->total_snapshots;
		r->logical_id_counter = bt->logical_id_counter;
		r->rootid = bt->rootid;

		if (tb_flushed) {
			ret = BTREE_SUCCESS;

			if (bt->trxenabled) {
				(*bt->trx_cmd_cb)( TRX_START);
			}

			uint64_t* logical_id = &mem_node->pnode->logical_id;
            bt->write_node_cb(my_thd_state, &ret, bt->write_node_cb_data,
                              &logical_id, (char**)&mem_node->pnode,
                              bt->nodesize, 1);
            if (bt->trxenabled) {
                (*bt->trx_cmd_cb)( TRX_COMMIT);
            }
		}
		node_unlock(mem_node);

    } else {
        ret = BTREE_FAILURE;
    }

done:
    if (BTREE_SUCCESS != ret) {
        bt_warn( "Could not persist btree!");
	// Write now we simply ignore the storage errors.
	ret = BTREE_SUCCESS;
     }

    return ret;
}

/*
 * load persistent btree metadata
 *
 * Info is stored as a btree node with special logical ID. 
 */
static btree_status_t
loadpersistent( btree_raw_t *bt)
{
    btree_raw_mem_node_t *mem_node;
    btree_status_t ret = BTREE_SUCCESS;

    mem_node = get_existing_node_low(&ret, bt,
                                     META_LOGICAL_ID + bt->n_partition,
                                     1, true /* pinned */, 
                                     false /* deref_delete */);

    if (BTREE_SUCCESS != ret)
        return (ret);

    btree_raw_persist_t *r = (btree_raw_persist_t*)mem_node->pnode;

    dbg_print("ret=%d nodeid=%lx r->lic %ld r->rootid %ld bt->logical_id_counter %ld\n", ret, META_LOGICAL_ID + bt->n_partition, r->logical_id_counter, r->rootid, r->logical_id_counter + META_COUNTER_SAVE_INTERVAL);

    bt->logical_id_counter = r->logical_id_counter + META_COUNTER_SAVE_INTERVAL;
    bt->rootid = r->rootid;
    bt->snap_meta = &r->snap_details; /* Keep the pinned snap details node info in btree */

	return flushpersistent(bt);
}

int btree_raw_free_buffer(btree_raw_t *btree, char *buf)
{
    free_buffer(btree, buf);
    return(0);
}

//======================   GET  =========================================

int is_overflow(btree_raw_t *btree, btree_raw_node_t *node) { return node->flags & OVERFLOW_NODE; }

int is_leaf(btree_raw_t *btree, btree_raw_node_t *node) { return node->flags & LEAF_NODE; }

inline static
int is_root(btree_raw_t *btree, btree_raw_node_t *node) { return btree->rootid == node->logical_id; }

int get_key_stuff(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_t *pks)
{
    node_vkey_t   *pvk;
    node_vlkey_t  *pvlk;
    node_fkey_t   *pfk;
    int            leaf = 0;

    pks->nkey = nkey;
    if (bt->flags & SECONDARY_INDEX) {
        pks->fixed = 0;
        //  used for secondary indices
	if (n->flags & LEAF_NODE) {
	    leaf               = 1;
	    pvlk               = ((node_vlkey_t *) n->keys) + nkey;
	    pks->ptr           = pvlk->ptr;
	    pks->offset        = sizeof(node_vlkey_t);
	    pks->pkey_struct   = (void *) pvlk;
	    pks->pkey_val      = (char *) n + pvlk->keypos;
	    pks->keylen        = pvlk->keylen;
	    pks->datalen       = pvlk->datalen;
	    pks->fkeys_per_node = 0;
	    pks->seqno         = pvlk->seqno;
	    pks->syndrome      = 0;
	} else {
	    pvk                = ((node_vkey_t *) n->keys) + nkey;
	    pks->ptr           = pvk->ptr;
	    pks->offset        = sizeof(node_vkey_t);
	    pks->pkey_struct   = (void *) pvk;
	    pks->pkey_val      = (char *) n + pvk->keypos;
	    pks->keylen        = pvk->keylen;
	    pks->datalen       = sizeof(uint64_t);
	    pks->fkeys_per_node = 0;
	    pks->seqno         = pvk->seqno;
	    pks->syndrome      = 0;
	}
    } else if (bt->flags & SYNDROME_INDEX) {
        //  used for primary indices
	if (n->flags & LEAF_NODE) {
	    leaf               = 1;
	    pvlk               = ((node_vlkey_t *) n->keys) + nkey;
	    pks->fixed         = 0;
	    pks->ptr           = pvlk->ptr;
	    pks->offset        = sizeof(node_vlkey_t);
	    pks->pkey_struct   = (void *) pvlk;
	    pks->pkey_val      = (char *) n + pvlk->keypos;
	    pks->keylen        = pvlk->keylen;
	    pks->datalen       = pvlk->datalen;
	    pks->fkeys_per_node = 0;
	    pks->seqno         = pvlk->seqno;
            pks->syndrome      = 0;
	} else {
	    pfk                = ((node_fkey_t *) n->keys) + nkey;
	    pks->fixed         = 1;
	    pks->ptr           = pfk->ptr;
	    pks->offset        = sizeof(node_fkey_t);
	    pks->pkey_struct   = (void *) pfk;
	    pks->pkey_val      = (char *) (pfk->key);
	    pks->keylen        = sizeof(uint64_t);
	    pks->datalen       = sizeof(uint64_t);
	    pks->fkeys_per_node = bt->fkeys_per_node;
	    pks->seqno         = pfk->seqno;
	    pks->syndrome      = pfk->key;
	}
    } else {
        assert(0);
    }
    pks->leaf = leaf;
    return(leaf);
}

void
get_key_stuff_non_leaf(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_info_t *key_sinfo)
{
	key_stuff_t ks;

	assert(!is_leaf(bt, n));

	(void) get_key_stuff(bt, n, nkey, &ks);

	key_sinfo->index = nkey;
	key_sinfo->key = ks.pkey_val;
	key_sinfo->keylen = ks.keylen;
	key_sinfo->ptr = ks.ptr;
	key_sinfo->datalen = ks.datalen;
	key_sinfo->seqno = ks.seqno;

	key_sinfo->leaf = false;

	key_sinfo->keyrec = ks.pkey_struct;
}

/*
 * Caller must free the key_sinfo.key after use.
 *
 * Caller must hold the lock on node to keep pks->pkey_struct valid.
 */
void
get_key_stuff_leaf(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_info_t *key_sinfo)
{
	bool res = false;
	key_info_t key_info;

	res = btree_leaf_get_nth_key_info(bt, n, nkey, &key_info);
	assert(res == true);

	key_sinfo->index = nkey;
	key_sinfo->key = key_info.key;
	key_sinfo->keylen = key_info.keylen;
	key_sinfo->ptr = key_info.ptr;
	key_sinfo->datalen = key_info.datalen;
	key_sinfo->seqno = key_info.seqno;

	key_sinfo->leaf = true;
	key_sinfo->keyrec = NULL;
}

/*
 * Expectes caller to pass buffer big enough to hold key.
 */
void
get_key_stuff_leaf2(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_info_t *key_sinfo)
{
	bool res = false;
	key_info_t key_info;

	key_info.key = key_sinfo->key;

	res = btree_leaf_get_nth_key_info2(bt, n, nkey, &key_info);
	assert(res == true);

	key_sinfo->index = nkey;
	key_sinfo->key = key_info.key;
	key_sinfo->keylen = key_info.keylen;
	key_sinfo->ptr = key_info.ptr;
	key_sinfo->datalen = key_info.datalen;
	key_sinfo->seqno = key_info.seqno;

	key_sinfo->leaf = true;
	key_sinfo->keyrec = NULL;
}

/*
 * Get information about the nthe key in key_stuff structure.
 *
 * Caller must free the key_info->key if the key info is for leaf node.
 */
int 
get_key_stuff_info(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_info_t *key_info)
{
	int ret = 0;

	if (is_leaf(bt, n)) {
		get_key_stuff_leaf(bt, n, nkey, key_info);
		ret = 1;
	} else {
		get_key_stuff_non_leaf(bt, n, nkey, key_info);
		ret = 0;
	}
	return ret;
}

/*
 * Expects caller to pass buffer big enough to hold key.
 */
int 
get_key_stuff_info2(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_info_t *key_info)
{
	int ret = 0;

	if (is_leaf(bt, n)) {
		get_key_stuff_leaf2(bt, n, nkey, key_info);
		ret = 0;
	} else {
		get_key_stuff_non_leaf(bt, n, nkey, key_info);
		ret = 0;
	}
	return ret;
}

void
dump_node_keys(btree_raw_t *bt, btree_raw_node_t *n)
{
	key_stuff_info_t ks = {0};
	static char  stmp1[1024];
	static char  stmp2[80];
	int i, j;

	printf("Leaf Node = %d, Logical ID = %"PRIu64
	         ", Next Node Logical ID = %"PRIu64
	         ", Total Keys = %u\n", is_leaf(bt, n),
	         n->logical_id, n->next, n->nkeys);

	ks.key = (char *) &tmp_key_buf;

	for (i = 0; i < n->nkeys; i++) {
		get_key_stuff_info2(bt, n, i, &ks);

		sprintf(stmp1, "%04d: seqno = %"PRIu64", Key=[", 
		           ks.index, ks.seqno);
		for (j = 0; (j < 100 && j < ks.keylen); j++) {
			if ((ks.key[j] < 32) || (ks.key[j] > 126)) {
				sprintf(stmp2, "'%u' ", ks.key[j]);
			} else {
				sprintf(stmp2, "%c ", ks.key[j]);
			}
			strcat(stmp1, stmp2);
		}
		if (j != ks.keylen) {
			strcat(stmp1, "...");
		}

		printf("%s]\n", stmp1);
	}
}

inline int
seqno_cmp_range(btree_metadata_t *smeta, uint64_t key_seqno,
                 bool *exact_match, bool *range_match)
{
	*exact_match = false;
	*range_match = false;

	if ((smeta == NULL) || 
	     (!(smeta->flags & (READ_SEQNO_EQ | READ_SEQNO_LE | 
	                        READ_SEQNO_GT_LE)))) {
		*range_match = true;
		return (0);
	}

	if (smeta->flags & READ_SEQNO_EQ) {
		if (smeta->seqno == key_seqno) {
			*exact_match = true;
			return (0);
		} else {
			return ((key_seqno > smeta->start_seqno) ? 1 : -1);
		}
	}

	if (smeta->flags & READ_SEQNO_LE) {
		if (key_seqno <= smeta->end_seqno) {
			*range_match = true;
			return (0);
		} else {
			return (1);
		}
	}

	if (smeta->flags & READ_SEQNO_GT_LE) {
		if ((key_seqno > smeta->start_seqno) &&
		    (key_seqno <= smeta->end_seqno)) {
			*range_match = true;
			return (0);
		} else if (key_seqno <= smeta->start_seqno) {
			return (-1);
		} else {
			return (1);
		}
	}

	assert(0);
        return(1);
}

#define DIR_LEFT     -1
#define DIR_UNKNOWN   0
#define DIR_RIGHT     1

static int
get_latest_seqno(btree_raw_t *bt, btree_raw_node_t *n, char *key_in, uint32_t keylen_in,
                 key_stuff_info_t *ks, btree_metadata_t *smeta, int i_cur, bool *found)
{
	int x;
	int i_ret = i_cur;
	int dir = DIR_UNKNOWN;
	bool exact_match, range_match;

	*found = false;
	do {
		x = seqno_cmp_range(smeta, ks->seqno, &exact_match, &range_match);
		if (exact_match) {
			*found = true;
			return (i_ret);
		} else if (range_match) {
			*found = true;

			/* If we are moving towards right, first
			 * range match is the LATEST */
			if (dir == DIR_RIGHT) {
				return (i_ret);
			}

			/* A range match with either not started to
			 * move yet or if we are moving left, we
			 * need further to the left (since latest
			 * is at left) */
			dir = DIR_LEFT; 
			i_ret = i_cur;
			i_cur--;
		} else if (x > 0) {
			/* key seqno is greater than query range.
			 * need to move right, if we already not 
			 * going left */
			if (dir == DIR_LEFT) {
				/* If we were moving left and now
				 * have to move right, which means
				 * no seqno within range */
				return i_ret;
			}
			dir = DIR_RIGHT;
			i_cur++;
			i_ret = i_cur;
		} else if (x < 0) {
			if (dir == DIR_RIGHT) {
				return i_ret;
			}
			dir = DIR_LEFT;
			i_ret = i_cur;
			i_cur--;
		} else {
			assert(0);
		}

		if ((i_cur < 0) || (i_cur >= n->nkeys)) {
			break;
		}

		(void) get_key_stuff_info2(bt, n, i_cur, ks);
	} while (bt->cmp_cb(bt->cmp_cb_data, key_in, keylen_in,
	                       ks->key, ks->keylen) == 0);

	return (i_ret);
}

/* Returns an index of key_in in the node. If the found is set to false,
 * return indicates where the key_in could be inserted if need be.
 * 
 * Input: 
 * 1. Key and Keylen are key to search
 * 2. Metadata is additional condition to search with. It predominantly contains
 *    seqnos and its flags. If NULL, assumes wildcard, (i.e. for seqno, it considers
 *    all sequence numbes: 0 - infinity). If non-NULL will use that as a secondary search
 *    after the key is found.
 * 3. i_start and i_end are the outerbound index within the node it has to search.
 *    If complete node has to be searched, boundary starts from -1 to n->nkeys.
 * 4. Flags: 
 *    Following flags is applied only if there is one key of key_in present in the node.
 *      BSF_MATCH  - find first matching value, ignores the meta (secondary search criteria)
 *      BSF_LATEST - If there are multiple keys based on the search (after both key_in and
 *                   meta), it will pick the latest among them and return its index.
 *      BSF_OLDEST - Converse to the BSF_LATEST.
 *      BSF_NEXT   - Returns the index of the first value greater than key_in. 
 * 
*/

int bsearch_key_low(btree_raw_t *bt, btree_raw_node_t *n,
                    char *key_in, uint32_t keylen_in,
                    btree_metadata_t *meta, uint64_t syndrome,
                    int i_start, int i_end, int flags,
                    bool *found)
{
	int i_check, x;
	key_stuff_info_t ks = {0};

	if(found) *found = true;

	ks.key = (char *) &tmp_key_buf;

	while ((i_end - i_start) > 1) {
		i_check = i_start + (i_end - i_start)/2;

		(void) get_key_stuff_info2(bt, n, i_check, &ks);

//		dbg_print_key(key_in, keylen_in, "key_in i_check=%d i_start=%d i_end=%d", i_check, i_start, i_end);
//		dbg_print_key(ks.pkey_val, ks.keylen, "pkey_val");

		if (!ks.fixed && !(bt->flags & SYNDROME_INDEX))
			x = bt->cmp_cb(bt->cmp_cb_data, key_in, keylen_in,
			               ks.key, ks.keylen);
		else
			x = syndrome - ks.syndrome;

		if (x == 0) {
			if (flags & BSF_MATCH) {
				return (i_check);
			} else if (flags & BSF_NEXT) {
				/* Look linearly till we get to the next keyset */
				while (++i_check < n->nkeys) {
					(void) get_key_stuff_info2(bt, n, i_check, &ks);
					if (bt->cmp_cb(bt->cmp_cb_data, key_in, keylen_in,
					               ks.key, ks.keylen) != 0) {
						break;
					}
				}
				return i_check;
			} else if (flags & BSF_LATEST) {
				i_check = get_latest_seqno(bt, n, key_in, keylen_in,
				                           &ks, meta, i_check, found);
				return (i_check);
			}
		} else if (x < 0) {
			i_end = i_check;
		} else {
			i_start = i_check;
		}
	}

	if (found) *found = 0;

	return (i_end);
}

/*
 * Find a key in node.
 *
 * Returns true if keys is found, false otherwise.
 * the child_id is set to child to follow to find this key for non-leaf nodes.
 *
 */
static int 
bsearch_key(btree_raw_t *bt, btree_raw_node_t *n, char *key_in, uint32_t keylen_in,
            btree_metadata_t *meta, uint64_t syndrome, int flags, 
            bool *found, uint64_t *child_id)
{

	int index;
	key_stuff_t ks;

	index = bsearch_key_low(bt, n, key_in, keylen_in, meta, syndrome, -1,
	                        n->nkeys, flags, found);

	*child_id = BAD_CHILD;
	if (!is_leaf(bt, n)) {
		if (index < n->nkeys) {
			get_key_stuff(bt, n, index, &ks);
			*child_id = ks.ptr;
		} else {
			*child_id = n->rightmost;
		}
	}

	return (index);
}

static node_key_t *find_key_non_leaf(btree_raw_t *bt, btree_raw_node_t *n, char *key_in, uint32_t keylen_in,
                                     btree_metadata_t *meta, uint64_t syndrome,
				     uint64_t *child_id, uint64_t *child_id_before, uint64_t *child_id_after,
				     node_key_t **pk_insert, int32_t *nkey_child)
{
	int x;
	bool found;
	node_key_t    *pk = NULL;
	uint64_t       id_child;
	key_stuff_t    ks;

	*child_id_before = *child_id = *child_id_after = BAD_CHILD;
	*nkey_child      = -1;
	*pk_insert       = NULL;

	if (!n->nkeys) {
		if(n->rightmost && is_root(bt, n)) {
			// YES, this is really possible!
			// For example, when the root is a leaf and overflows on an insert.
			*child_id = n->rightmost;
			*nkey_child = 0;
		}
		return(NULL);
	}

	x = bsearch_key_low(bt, n, key_in, keylen_in, meta, syndrome, -1, n->nkeys, BSF_LATEST, &found);

	if(x < n->nkeys) {
		get_key_stuff(bt, n, x, &ks);
		*pk_insert  = ks.pkey_struct;

		*child_id = ks.ptr;
		*nkey_child = x;
	} else /*if (x == n->nkeys) */ {
		*child_id = n->rightmost;
		*nkey_child = n->nkeys;
	}

	if (x > 0) {
		get_key_stuff(bt, n, x - 1, &ks);
		*child_id_before = ks.ptr;
	}

	if(x < n->nkeys - 1) {
		get_key_stuff(bt, n, x + 1, &ks);
		*child_id_after = ks.ptr;
	} else if (x == n->nkeys - 1) {
		*child_id_after  = n->rightmost;
	}

	return !found ? NULL : *pk_insert;
}

/*
 *  Returns: key structure which matches 'key', if one is found; NULL otherwise
 *           'pk_insert' returns a pointer to the key struct that would FOLLOW 'key' on 
 *           an insertion into this node. If 'pk_insert' is NULL, 'key' must be
 *           inserted at end of node, or key is already in node.

 *  If node is NOT a leaf node, these 3 values are returned:
 *    child_id:        id of child node that may contain this key
 *    child_id_before: id of child sibling before 'child_id'
 *    child_id_after:  id of child sibling after 'child_id'
 *
 *    If the before or after siblings don't exist, BAD_CHILD is returned.
 *
 *    nkey_child returns the index into the key array of the key entry corresponding
 *    to 'child_id' (for a non-leaf), or for the matching record (for a leaf).  
 *    If 'child_id' corresponds to the rightmost pointer, nkey_child is
 *    set to n->nkeys.  If there is no valid child_id (nonleaf) or matching record (leaf),
 *    nkey_child is set to -1.
 *
 *  Snapshot behavior:
 *  -------------------
 *  If there are multiple versions of the same key, it returns the latest version of
 *  the key. However, if exclude_snapshots option is set and if latest key is part of
 *  any snapshot sequence range, will return NULL (as if no key exists)
 *  
 *  leaf_key_exists will be set if there is any key (of any seqnos), even if they were
 *  excluded by the exclude_snapshot option.
 * 
 */
static bool
find_key(btree_raw_t *bt, btree_raw_node_t *n, char *key_in, uint32_t keylen_in,
          btree_metadata_t *meta, uint64_t syndrome,
	  uint64_t *child_id, uint64_t *child_id_before, uint64_t *child_id_after,
	  int32_t *index)
{
	node_key_t *pk_insert = NULL;
	bool res = false;

	if (is_leaf(bt, n)) {
		*child_id = BAD_CHILD;
		*child_id_before = BAD_CHILD;
		*child_id_after = BAD_CHILD;
		res = btree_leaf_find_key(bt, n, key_in, keylen_in, meta, syndrome, index);
	} else {
		/*
		 * Search cannot end at non-leaf node.
		 */ 
		find_key_non_leaf(bt, n, key_in, keylen_in, meta, syndrome, child_id,
				  child_id_before, child_id_after, &pk_insert, index);
		assert(*child_id != BAD_CHILD);
		res = true;
	}

	return res;
}

char *
get_buffer(btree_raw_t *btree, uint64_t nbytes)
{
    char  *p;

    p = btree_malloc(nbytes);
    return(p);
}

void 
free_buffer(btree_raw_t *btree, void* buf)
{
    btree_free(buf);
}

btree_status_t 
get_leaf_data_index(btree_raw_t *bt, btree_raw_node_t *n, int index, char **data,
		    uint64_t *datalen, uint32_t meta_flags, int ref, bool deref_delete_cache)
{
    btree_raw_node_t   *z;
    btree_status_t      ret=BTREE_SUCCESS, ret2 = BTREE_SUCCESS;
    char               *buf;
    char               *p;
    int                 buf_alloced=0;
    uint64_t            nbytes;
    uint64_t            copybytes;
    uint64_t            z_next;
    key_info_t 	key_info = {0};
    bool res = false;
    char *datap = NULL;
    uint64_t datalen1 = 0;

    *data = NULL;
    *datalen = 0; 

    key_info.key = tmp_key_buf;
    res = btree_leaf_get_nth_key_info2(bt, n, index, &key_info); 
    assert(res == true);
    key_info.key = NULL;


    if (meta_flags & INPLACE_POINTERS) {
	*datalen = big_object(bt, &key_info) ? 0 : key_info.datalen;
    }

    if (meta_flags & (BUFFER_PROVIDED|INPLACE_POINTERS)) {
        if (*datalen < key_info.datalen) {
			ret = BTREE_BUFFER_TOO_SMALL;
			if (meta_flags & ALLOC_IF_TOO_SMALL) {
				buf = get_buffer(bt, key_info.datalen);
				if (buf == NULL) {
					bt_err("Failed to allocate a buffer of size %lld in get_leaf_data!", key_info.datalen);
					return(BTREE_FAILURE);
				}
				buf_alloced = 1;
			} else {
				return(BTREE_BUFFER_TOO_SMALL);
			}
		} else {
			buf = *data;
		}
    } else {
        buf = get_buffer(bt, key_info.datalen);
		if (buf == NULL) {
			bt_err("Failed to allocate a buffer of size %lld in get_leaf_data!", key_info.datalen);
			return(BTREE_FAILURE);
		}
		buf_alloced = 1;
    }

    *datalen = key_info.datalen;
    *data    = buf;

    if (big_object(bt, (&key_info))) {
        /*
	 *  data is in overflow btree nodes
	 */

        if (key_info.datalen > 0) {
			nbytes = key_info.datalen;
			p      = buf;
			z_next = key_info.ptr;
			while(nbytes > 0 && z_next) {
				btree_raw_mem_node_t *node = get_existing_node_low(&ret2, bt, z_next, ref, false, 
				                                                   deref_delete_cache);
				if(!node)
					break;
				z = node->pnode;
				copybytes = nbytes >= bt->nodesize_less_hdr ? bt->nodesize_less_hdr : nbytes;
				memcpy(p, ((char *) z + sizeof(btree_raw_node_t)), copybytes);
				nbytes -= copybytes;
				p      += copybytes;
				z_next  = z->next;
				if(!ref)
					deref_l1cache_node(bt, node);
			}
			if (nbytes) {
				bt_err("Failed to get overflow node (logical_id=%lld)(nbytes=%ld) in get_leaf_data!", z_next, nbytes);
				if (buf_alloced) {
					free_buffer(bt, buf);
				}
				return(ret2);
			}
			assert(z_next == 0);
		}
    } else if (meta_flags & INPLACE_POINTERS) {
	assert(0);
    } else {
	btree_leaf_get_data_nth_key(bt, n, index, &datap, &datalen1);
	assert(key_info.datalen == datalen1);
	memcpy(buf, datap, datalen1);
    }

    return(ret);
}

btree_status_t 
get_leaf_data_nth_key(btree_raw_t *bt, btree_raw_node_t *n, int index,
		      btree_metadata_t *meta, uint64_t syndrome,
		      char **data, uint64_t *datalen, int ref)
{
    btree_raw_node_t   *z;
    btree_status_t      ret=BTREE_SUCCESS;
    char               *buf;
    char               *p;
    int                 buf_alloced=0;
    uint64_t            nbytes;
    uint64_t            copybytes;
    uint64_t            z_next;
    key_info_t 	key_info = {0};
    bool res = false;
    char *datap = NULL;
    uint64_t datalen1 = 0;
    uint32_t meta_flags = meta->flags;

    *data = NULL;
    *datalen = 0; 

    res = btree_leaf_get_nth_key_info(bt, n, index, &key_info); 
    assert(res == true);
    free_buffer(bt, key_info.key);
    key_info.key = NULL;

    if (meta_flags & BUFFER_PROVIDED) {
        if (*datalen < key_info.datalen) {
	    ret = BTREE_BUFFER_TOO_SMALL;
	    if (meta_flags & ALLOC_IF_TOO_SMALL) {
		buf = get_buffer(bt, key_info.datalen);
		if (buf == NULL) {
		    bt_err("Failed to allocate a buffer of size %lld in get_leaf_data!", key_info.datalen);
		    return(BTREE_FAILURE);
		}
		buf_alloced = 1;
	    } else {
	        return(BTREE_BUFFER_TOO_SMALL);
	    }
	} else {
	    buf = *data;
	}
    } else {
        buf = get_buffer(bt, key_info.datalen);
	if (buf == NULL) {
	    bt_err("Failed to allocate a buffer of size %lld in get_leaf_data!", key_info.datalen);
	    return(BTREE_FAILURE);
	}
	buf_alloced = 1;
    }

    if ((key_info.keylen + key_info.datalen) < bt->big_object_size) {
        /*
	 *  key and data are in this btree node
	 */
	btree_leaf_get_data_nth_key(bt, n, index, &datap, &datalen1);
	assert(key_info.datalen == datalen1);
	memcpy(buf, datap, datalen1);
    } else {
        /*
	 *  data is in overflow btree nodes
	 */
        if (key_info.datalen > 0) {
	    nbytes = key_info.datalen;
	    p      = buf;
	    z_next = key_info.ptr;
	    while(nbytes > 0 && z_next)
	    {
		btree_raw_mem_node_t *node = get_existing_node_low(&ret, bt, z_next, ref, false, false);
		if(!node)
		    break;
		z = node->pnode;
                copybytes = nbytes >= bt->nodesize_less_hdr ? bt->nodesize_less_hdr : nbytes;
		memcpy(p, ((char *) z + sizeof(btree_raw_node_t)), copybytes);
		nbytes -= copybytes;
		p      += copybytes;
		z_next  = z->next;
		if(!ref)
		    deref_l1cache_node(bt, node);
	    }
	    if (nbytes) {
		bt_err("Failed to get overflow node (logical_id=%lld)(nbytes=%ld) in get_leaf_data!", z_next, nbytes);
		if (buf_alloced) {
		    free_buffer(bt, buf);
		}
		return(BTREE_FAILURE);
	    }
	    assert(z_next == 0);
	}
    }
    *datalen = key_info.datalen;
    *data    = buf;
    return(ret);
}

btree_status_t 
get_leaf_data(btree_raw_t *bt, btree_raw_node_t *n, char *key, uint32_t keylen,
		btree_metadata_t *meta, uint64_t syndrome, char **data, uint64_t *datalen, int *idx,
		int ref)
{
    int index = -1;
    bool res = false;

    *data = NULL;
    *datalen = 0; 
    res = btree_leaf_find_key(bt, n, key, keylen, meta, syndrome, &index); 
    if (res == false) {
		return  BTREE_KEY_NOT_FOUND;
    }

	if (idx) {
		*idx = index;
	}

    return get_leaf_data_nth_key(bt, n, index, meta, syndrome, data, datalen, ref);
}

btree_status_t 
get_leaf_key_index(btree_raw_t *bt, btree_raw_node_t *n, int index, char **key, 
		   uint32_t *keylen, uint32_t meta_flags, key_stuff_info_t *pks)
{
	btree_status_t     ret = BTREE_SUCCESS;
	char               *buf;
	key_stuff_info_t ks = {0};

	if (pks == NULL) {
		ks.key = tmp_key_buf;
		get_key_stuff_info2(bt, n, index, &ks);
		pks = &ks;
	}

	if (meta_flags & INPLACE_POINTERS) {
	    assert(0);
	    *keylen = pks->keylen;
	    *key    = pks->key;
	    return(ret);
	}

	if (meta_flags & BUFFER_PROVIDED) {
		if (*keylen < pks->keylen) {
			ret = BTREE_BUFFER_TOO_SMALL;
			if (!(meta_flags & ALLOC_IF_TOO_SMALL)) {
				return ret;
			}
			buf = get_buffer(bt, pks->keylen);
			btree_memcpy(buf, pks->key, pks->keylen, false);
			pks->key =  NULL;
		} else {
			/*
			 * Buffer provided and sufficient to hold the key.
			 */
			buf = *key;
			btree_memcpy(buf, pks->key, pks->keylen, false);
			pks->key =  NULL;
		}
	} else {
		/*
		 * No buffer provided, return the allocated one.
		 */
		buf = get_buffer(bt, pks->keylen);
		btree_memcpy(buf, pks->key, pks->keylen, false);
		pks->key =  NULL;
		assert(buf != NULL);
	}

	*keylen = pks->keylen;
	*key    = buf;

	return(ret);
}

static void delete_overflow_data(btree_status_t *ret, btree_raw_t *bt, uint64_t ptr_in, uint64_t datalen)
{
    uint64_t            ptr;
    uint64_t            ptr_next;
    btree_raw_mem_node_t   *n;

    if (*ret) { return; }

    for (ptr = ptr_in; ptr != 0; ptr = ptr_next) {
	n = get_existing_node(ret, bt, ptr);
	if (BTREE_SUCCESS != *ret) {
	    bt_err("Failed to find an existing overflow node in delete_overflow_data!");
	    return;
	}

	ptr_next = n->pnode->next;
	free_node(ret, bt, n);
	// current free_node doesn't change value of ret...
	if (BTREE_SUCCESS != *ret) {
	    bt_err("Failed to free an existing overflow node in delete_overflow_data!");
	}
    }
    __sync_sub_and_fetch(&(bt->stats.stat[BTSTAT_OVERFLOW_BYTES]), datalen);
}

static uint64_t allocate_overflow_data(btree_raw_t *bt, uint64_t datalen, char *data)
{
    uint64_t            n_nodes;
    btree_raw_mem_node_t   *n, *n_first = NULL, *n_last = NULL;
    btree_status_t      ret = BTREE_SUCCESS;
    char               *p = data;;
    uint64_t            nbytes = datalen;

    dbg_print("datalen %ld nodesize_less_hdr: %d bt %p\n", datalen, bt->nodesize_less_hdr, bt);

    if (!datalen)
        return(BTREE_SUCCESS);

    n_nodes = (datalen + bt->nodesize_less_hdr - 1) / bt->nodesize_less_hdr;

    n_first = n = get_new_node(&ret, bt, OVERFLOW_NODE, 0);
    while(nbytes > 0 && !ret) {
	n->next = 0;

	if (n_last != NULL)
	    n_last->pnode->next = n->pnode->logical_id;

	int b = nbytes < bt->nodesize_less_hdr ? nbytes : bt->nodesize_less_hdr;

	memcpy(((char *) n->pnode + sizeof(btree_raw_node_t)), p, b);

	p += b;
	nbytes -= b;
	n_last = n;

        __sync_add_and_fetch(&(bt->stats.stat[BTSTAT_OVERFLOW_BYTES]), 
                               b + sizeof(btree_raw_node_t));
	if(nbytes)
	    n = get_new_node(&ret, bt, OVERFLOW_NODE, 0);
    }

    if(BTREE_SUCCESS == ret) 
	return n_first->pnode->logical_id;

    /* Error. Delete partially allocated data */
    ret = BTREE_SUCCESS;

    if(n_first)
        delete_overflow_data(&ret, bt, n_first->pnode->logical_id, datalen);

    return(ret);
}

static uint64_t get_syndrome(btree_raw_t *bt, char *key, uint32_t keylen)
{
    uint64_t   syndrome;

    syndrome = btree_hash((const unsigned char *) key, keylen, 0);
    return(syndrome);
}


btree_raw_mem_node_t*
root_get_and_lock(btree_raw_t* btree, int write_lock)
{
	btree_status_t ret = BTREE_SUCCESS;
	uint64_t child_id;
	btree_raw_mem_node_t *node;

	while(1) {
		child_id = btree->rootid;

		node = get_existing_node_low(&ret, btree, child_id, 0, false, false);
		if(!node)
			return NULL;

		node_lock(node, is_leaf(btree, node->pnode) && write_lock);

		if(child_id == btree->rootid)
			break;

		node_unlock(node);
		deref_l1cache_node(btree, node);
	}

	return node;
}

/* Caller is responsible for leaf_lock unlock and node dereferencing */
int
btree_raw_find(struct btree_raw *btree, char *key, uint32_t keylen, uint64_t syndrome,
	       btree_metadata_t *meta, btree_raw_mem_node_t** node, int write_lock,
               int* pathcnt, bool *found)
{
    btree_raw_mem_node_t *parent;
    btree_status_t    ret = BTREE_SUCCESS;
    uint64_t          child_id;
    int               index;

    /* Do not cache entries for snapshot finds */
    bool snap_query = meta->flags & (READ_SEQNO_EQ | READ_SEQNO_LE | READ_SEQNO_GT_LE);

    *node = root_get_and_lock(btree, write_lock);
    assert(*node);

    while(!is_leaf(btree, (*node)->pnode)) {
        (void) bsearch_key(btree, (*node)->pnode, key, keylen, meta, syndrome,
                           BSF_LATEST, found, &child_id);
        assert(child_id != BAD_CHILD);

        parent = *node;

        *node = get_existing_node_low(&ret, btree, child_id, 0, false, snap_query);
        assert(BTREE_SUCCESS == ret && *node); //FIXME add correct error checking here

        node_lock(*node, is_leaf(btree, (*node)->pnode) && write_lock);

        node_unlock(parent);
        deref_l1cache_node(btree, parent);

        (*pathcnt)++;
    }

    index = bsearch_key(btree, (*node)->pnode, key, keylen, meta, syndrome,
                        BSF_LATEST, found, &child_id);

    if ((*found) == true) {
        /* Unless this find is for a forced delete, if we 
	 * encounter a tombstone, pretend as if nothing is found */
        if (btree_leaf_is_key_tombstoned(btree, (*node)->pnode, index) &&
            !(meta->flags & FORCE_DELETE)) {
            *found = false;
        }
    }

    return (index);
}

btree_status_t btree_raw_get(struct btree_raw *btree, char *key, uint32_t keylen, char **data, uint64_t *datalen, btree_metadata_t *meta)
{
    btree_status_t    ret = BTREE_KEY_NOT_FOUND;
    int               pathcnt = 1;
    btree_raw_mem_node_t *node;
    uint64_t          syndrome = get_syndrome(btree, key, keylen);
    bool found = false;
    int index;

    dbg_print_key(key, keylen, "before ret=%d lic=%ld", ret, btree->logical_id_counter);

    plat_rwlock_rdlock(&btree->lock);

    index = btree_raw_find(btree, key, keylen, syndrome, meta, &node, 0 /* shared */, &pathcnt, &found);

    plat_rwlock_unlock(&btree->lock);

    if(found) {
        ret = get_leaf_data_nth_key(btree, node->pnode, index, meta, syndrome, data, datalen, 0);
	assert(BTREE_SUCCESS == ret);
    }

    node_unlock(node);
    deref_l1cache_node(btree, node);

    __sync_add_and_fetch(&(btree->stats.stat[BTSTAT_GET_CNT]), 1);
    __sync_add_and_fetch(&(btree->stats.stat[BTSTAT_GET_PATH]), pathcnt);

    assert(!dbg_referenced);

    return(ret);
}

//======================   INSERT/UPDATE/UPSERT  =========================================

//  return 0 if success, 1 otherwise
int
init_l1cache()
{
    int n = 0;
    char *fdf_prop = NULL;

    n_global_l1cache_buckets = 0;
    /* Check if cache size is configured through fdf property file  */
    fdf_prop = (char *)FDFGetProperty("FDF_BTREE_L1CACHE_SIZE",NULL);
    if( fdf_prop != NULL ) {
         n_global_l1cache_buckets = (uint64_t)atoll(fdf_prop);
    }
    else {
         /* Check if the size is set through env. variable */
         char *env = getenv("BTREE_L1CACHE_SIZE");
         n_global_l1cache_buckets = env ? (uint64_t)atoll(env) : 0;
    }
    l1cache_size = n_global_l1cache_buckets;

    if ( n_global_l1cache_buckets ){
        n_global_l1cache_buckets = n_global_l1cache_buckets / 16 / get_btree_node_size();
    } else {
        n_global_l1cache_buckets = DEFAULT_N_L1CACHE_BUCKETS;
    }
    if ( !l1cache_size ) {
        l1cache_size = n_global_l1cache_buckets * 16 * 8192;
    }

    /* check if cache partitions is configured through fdf property */
    fdf_prop = (char *)FDFGetProperty("FDF_N_L1CACHE_PARTITIONS",NULL);
    if( fdf_prop != NULL ) {
        n = atoi(fdf_prop);
    }
    else {
        /* check if cache partitions is configured through env */
        char *p = getenv("N_L1CACHE_PARTITIONS");
        if(p)
            n = atoi(p);
    }

    if(n <=0 || n > 10000000)
        n = DEFAULT_N_L1CACHE_PARTITIONS;

    l1cache_partitions = n;
    global_l1cache = PMapInit(n, n_global_l1cache_buckets / n + 1, 16 * (n_global_l1cache_buckets / n + 1), 1, l1cache_replace);
    if (global_l1cache == NULL) {
        return(1);
    }
    return(0);
}

void
destroy_l1cache()
{
	if( global_l1cache ){
		PMapDestroy(&global_l1cache);
	}
}

void
clean_l1cache(btree_raw_t* btree)
{
	PMapClean(&(btree->l1cache), btree->cguid, (void *)btree);
}

static void delete_l1cache(btree_raw_t *btree, uint64_t logical_id)
{
    (void) PMapDelete(btree->l1cache, (char *) &logical_id, sizeof(uint64_t), btree->cguid, (void *)btree);
    dbg_referenced--;

    btree->stats.stat[BTSTAT_L1ENTRIES] = PMapNEntries(btree->l1cache);
}

void
deref_l1cache_node(btree_raw_t* btree, btree_raw_mem_node_t *node)
{
    /* Delete the cache instead of deref, if it asked to do so */
    if (node->deref_delete_cache) {
        assert(!node->pinned);
        delete_l1cache(btree, node->pnode->logical_id);
        return;
    }

    /* Do not deref a pinned node */
    if (!node->pinned) {
        dbg_print("node %p id %ld root: %d leaf: %d dbg_referenced: %lx mpnode %ld refs %ld\n", node, node->pnode->logical_id, is_root(btree, node->pnode), is_leaf(btree, node->pnode), dbg_referenced, modified_nodes_count, referenced_nodes_count);

        if (!PMapRelease(btree->l1cache, (char *) &node->pnode->logical_id, sizeof(node->pnode->logical_id), btree->cguid, (void *)btree))
            assert(0);
    }

    assert(dbg_referenced);
    dbg_referenced--;
}

static void
reset_node_pstats(btree_raw_node_t *n)
{
    n->pstats.seq_num = 0;
    n->pstats.is_pos_delta = 0;
    n->pstats.delta[PSTAT_OBJ_COUNT] = 0;
	n->pstats.delta[PSTAT_NUM_SNAP_OBJS] = 0;
	n->pstats.delta[PSTAT_SNAP_DATA_SIZE] = 0;

    n->pstats.seq = 0;
#ifdef PSTATS_2
    fprintf(stderr, "Resetting the node\n");
#endif
}

static uint64_t unique;

/*
 * Set per node stats. This information is used to recover nodes
 * involved in a crashed session.
 */
static void
set_node_pstats(btree_raw_t *btree, btree_raw_node_t *x, uint64_t num_obj, bool is_delta_positive)
{
    pthread_mutex_lock(&btree->pstat_lock);
    x->pstats.seq_num = btree->last_flushed_seq_num;
    x->pstats.delta[PSTAT_OBJ_COUNT] += num_obj;
    x->pstats.is_pos_delta |= is_delta_positive << PSTAT_OBJ_COUNT;
    /*
     * Unique temporary sequence number exists for debugging purposes
     */
    x->pstats.seq = __sync_fetch_and_add( &unique, 1 );
    btree->pstats_modified = true;

    _pstats_ckpt_index = btree->current_active_write_idx;

    /*
     * This write is active now.
     */
    __sync_add_and_fetch(&btree->active_writes[_pstats_ckpt_index], 1);

    pthread_mutex_unlock(&btree->pstat_lock);

#ifdef PSTATS_1
    fprintf(stderr, "set_node_pstats: d_obcount=%ld seq_num=%ld positive=%d unique=%ld\n",
            x->pstats.delta[PSTAT_OBJ_COUNT], x->pstats.seq_num, is_delta_positive, x->pstats.seq);
#endif
}

static void
set_node_snapobjs_pstats(btree_raw_t *btree, btree_raw_node_t *x, uint64_t num_obj, uint64_t data_size, bool is_delta_positive)
{
    //btree_raw_node_t* x = n->pnode;
    x->pstats.seq_num = btree->last_flushed_seq_num;
    x->pstats.delta[PSTAT_NUM_SNAP_OBJS] += num_obj;
    x->pstats.delta[PSTAT_SNAP_DATA_SIZE] += data_size;
	x->pstats.is_pos_delta |= is_delta_positive << PSTAT_NUM_SNAP_OBJS;
	x->pstats.is_pos_delta |= is_delta_positive << PSTAT_SNAP_DATA_SIZE;
    x->pstats.seq = __sync_fetch_and_add( &unique, 1 );
    btree->pstats_modified = true;

#ifdef PSTATS_2
    fprintf(stderr, "delta_num_obj=%ld delta_snap_data_size=%ld seq_num=%ld is_delta_positive=%d unique=%ld\n",
           x->pstats.delta[PSTAT_NUM_SNAP_OBJS], x->pstats.delta[PSTAT_SNAP_DATA_SIZE], x->pstats.seq_num, is_delta_positive, x->pstats.seq);
#endif
}

/*
 * Conditionally wakes up flusher thread.
 */
static void
pstats_flush(struct btree_raw *btree, btree_raw_mem_node_t *n)
{
    assert( btree );
    assert( n );

    if ( fdf_flush_pstats_frequency > 0 ) {
        if ( __sync_fetch_and_add( &total_sys_writes, 1 ) >= fdf_flush_pstats_frequency ) {
            /*
             * Signal flusher thread
             */
            pthread_mutex_lock( &pstats_mutex );

            total_sys_writes = 0;

            /*
             * Later on, all nodes will assume this sequence number
             * till next flush happens.
             */
#ifdef PSTATS_1
            btree_raw_node_t* x = n->pnode;
            fprintf(stderr, "pstats_flush: obcount=%ld seq_num=%ld unique=%ld\n",
                    x->pstats.delta[PSTAT_OBJ_COUNT], x->pstats.seq_num, x->pstats.seq);
#endif
            //n->pnode->pstats.seq_num = btree->last_flushed_seq_num;

            pthread_cond_signal( &pstats_cond_var );
            pthread_mutex_unlock( &pstats_mutex );
        }
    }
}

static void btree_parallel_flush_write(btree_raw_t* btree, btree_raw_node_t** nodes, int count)
{
	btSyncRequest_t			req;

	pthread_cond_init(&(req.ret_condvar), NULL);
	req.dir_nodes = nodes;
	req.dir_count = count;
	req.del_nodes = deleted_nodes;
	req.del_count = deleted_nodes_count;
	req.dir_written = modified_written;
	req.del_written = deleted_written;
	req.dir_index = 0;
	req.del_index = 0;
	req.total_flush = 0;
	req.ref_count = 0;
	req.next = NULL;
	req.prev = NULL;
	req.ret = BTREE_SUCCESS;

	pthread_mutex_lock(&(btree->bt_async_mutex));
	if (btree->sync_first == NULL) {
		btree->sync_first = &req;
		btree->sync_last = &req;
	} else {
		req.prev = btree->sync_last;
		btree->sync_last->next = &req;
		btree->sync_last = &req;
	}
	btree->io_threads++;
	btree->io_bufs += ((count + deleted_nodes_count));
#if 0
	if ((btree->io_bufs) < btree->no_sync_threads) {
		pthread_cond_signal(&(btree->bt_async_cv));
	} else {
		pthread_cond_broadcast(&(btree->bt_async_cv));
	}
#endif
	pthread_cond_broadcast(&(btree->bt_async_cv));
	pthread_mutex_unlock(&(btree->bt_async_mutex));

	btree_sync_flush_entry(btree, my_thd_state, &req);

	pthread_mutex_lock(&(btree->bt_async_mutex));
	btree->io_threads--;
	btree->io_bufs -= ((count + deleted_nodes_count));

	if (req.ref_count == 0) {
		btree_sync_remove_entry(btree, &req);
	} else {
		pthread_cond_wait(&(req.ret_condvar), &(btree->bt_async_mutex));
	}
	pthread_mutex_unlock(&(btree->bt_async_mutex));


	assert(req.total_flush == (req.del_count + req.dir_count));
	assert(req.ref_count == 0);

	deleted_nodes_count = 0;
}

/*
 * Flush the modified and deleted nodes, unlock those nodes, cleare the reference
 * for such nodes.
 */
#define MIN_COUNT_FOR_ASYNC			3

btree_status_t deref_l1cache(btree_raw_t *btree)
{
    uint64_t i, j, actual_nodes = 0, count = 0;
    btree_raw_mem_node_t *n;
    btree_status_t        ret = BTREE_SUCCESS;
    int                   index;
    char* nodes[MPUT_BATCH_SIZE];
    uint64_t *ids[MPUT_BATCH_SIZE];
#ifdef FLIP_ENABLED
    static uint32_t node_write_cnt = 0;
#endif

    bzero(modified_written, modified_nodes_count * sizeof(int));
    bzero(deleted_written, deleted_nodes_count * sizeof(int));

    if (btree->trxenabled) {
        (*btree->trx_cmd_cb)( TRX_START);
    }

    for ( i = 0; i < modified_nodes_count; i++ ) {
        n = modified_nodes[i];

        dbg_print("write_node_cb key=%ld data=%p datalen=%d\n", n->pnode->logical_id, n, btree->nodesize);

        j = 0;
        while(j < i && modified_nodes[j] != modified_nodes[i])
            j++;

        if(j >= i) {
            nodes[count] = (void*)n->pnode;
            ids[count] = &n->pnode->logical_id;
            /*
             * Try to start flush of persistent stats if write count
             * meets the condition of flush frequency.
             */
            pstats_flush(btree, n);
            count++;
        }

        if(count == MPUT_BATCH_SIZE) {
            if (!btree_parallel_flush_disabled && (count + deleted_nodes_count) > btree_parallel_flush_minbufs)
                btree_parallel_flush_write(btree, (btree_raw_node_t**)nodes, count);
            else
                btree->write_node_cb(my_thd_state, &ret, btree->write_node_cb_data, ids, (char**)nodes, btree->nodesize, count);
            count = 0;
        }
    }

    if(count)
    {
        if (!btree_parallel_flush_disabled && (count + deleted_nodes_count) > btree_parallel_flush_minbufs)
            btree_parallel_flush_write(btree, (btree_raw_node_t**)nodes, count);
        else
            btree->write_node_cb(my_thd_state, &ret, btree->write_node_cb_data, ids, nodes, btree->nodesize, count);
    }

#ifdef FLIP_ENABLED
    if (flip_get("sw_crash_on_single_write", 
                (uint32_t)n->pnode->flags,
                recovery_write,
                node_write_cnt)) {
        exit(0);
    }

    if (flip_get("set_btree_fdf_write_ret",
                (uint32_t)n->pnode->flags,
                recovery_write,
                node_write_cnt, (uint32_t *)&ret)) {
        goto write_done;               
    }
    __sync_fetch_and_add(&node_write_cnt, 1);
#endif

    for(i = 0; i < modified_nodes_count; i++)
    {
        n = modified_nodes[i];

        mark_node_clean(n);
        reset_node_pstats(n->pnode);

        assert(!is_node_dirty(n));
        deref_l1cache_node(btree, n);
        add_node_stats(btree, n->pnode, L1WRITES, 1);
    }

    /*
     * Deleted node writes
     */ 
    if (!(!btree_parallel_flush_disabled && (count + deleted_nodes_count) > btree_parallel_flush_minbufs)) {
        for(i = 0; i < deleted_nodes_count; i++)
        {
            n = deleted_nodes[i];

            dbg_print("delete_node_cb key=%ld data=%p datalen=%d\n", n->pnode->logical_id, n, btree->nodesize);

            /*
             * Try to start flush of persistent stats if delete write count
             * meets the condition of flush frequency.
             */
            pstats_flush(btree, n);

            ret = btree->delete_node_cb(btree->create_node_cb_data, n->pnode->logical_id);
#ifdef BTREE_UNDO_TEST
            btree_rcvry_test_delete(btree, n->pnode);
#endif
            add_node_stats(btree, n->pnode, L1WRITES, 1);
            reset_node_pstats(n->pnode);
        }
    }

    if (btree->trxenabled) {
        (*btree->trx_cmd_cb)( TRX_COMMIT);
    }

    unlock_modified_nodes(btree);

    //  clear reference bits
    for(i = 0; i < referenced_nodes_count; i++)
    {
        n = referenced_nodes[i];
        deref_l1cache_node(btree, n);
    }

    for(i = 0; i < deleted_nodes_count; i++)
    {
        n = deleted_nodes[i];
        delete_l1cache(btree, n->pnode->logical_id);
    }

    modified_nodes_count = 0;
    referenced_nodes_count = 0;
    deleted_nodes_count = 0;

    //    assert(PMapNEntries(btree->l1cache) <= 16 * (btree->n_l1cache_buckets / 1000 + 1) * 1000 + 1);

    return ret;
}

void
btree_sync_thread(uint64_t arg)
{
	btree_raw_t				*btree = (btree_raw_t *)(arg);
	btSyncRequest_t			*list = NULL;
	btree_raw_mem_node_t	*n = NULL;
	btree_status_t			ret = BTREE_SUCCESS;
	FDF_status_t			fdfret = FDF_SUCCESS;
	uint64_t				logical_id;
	struct FDF_thread_state	*thd_state = NULL;

	assert(btree_parallel_flush_disabled == 0);

	fdfret = FDFInitPerThreadState(FDFState, &thd_state);
	assert(fdfret == FDF_SUCCESS);


	pthread_mutex_lock(&(btree->bt_async_mutex));
	__sync_fetch_and_add(&(btree->no_sync_threads), 1);
	while (1) {
		while (list == NULL) {
			if (btree->deleting) {
				pthread_mutex_unlock(&(btree->bt_async_mutex));
				FDFReleasePerThreadState(&thd_state);
				__sync_fetch_and_sub(&(btree->no_sync_threads), 1);
				return;
			}
			assert((btree->sync_first == NULL) ||
					((btree->sync_last->del_index == btree->sync_last->del_count)
					 && (btree->sync_last->dir_index == btree->sync_last->dir_count)));
			btree->worker_threads--;

			pthread_cond_wait(&(btree->bt_async_cv), &(btree->bt_async_mutex));
			btree->worker_threads++;
			list = btree->sync_first;
		}

		list->ref_count++;
		if ((list->total_flush == (list->del_count + list->dir_count))
					|| (list->ret != BTREE_SUCCESS)) {
			goto next;
		}
		pthread_mutex_unlock(&(btree->bt_async_mutex));

		btree_sync_flush_entry(btree, thd_state, list);
		
		pthread_mutex_lock(&(btree->bt_async_mutex));
next:
		list->ref_count--;

		btree_sync_remove_entry(btree, list);

		if (list->ref_count == 0) {
			assert((list->total_flush >= (list->del_count + list->dir_count - 1))
						|| (list->ret != BTREE_SUCCESS));
			pthread_cond_signal(&(list->ret_condvar));
		}
		list = btree->sync_first;

	}

	assert(0);

}

static void
btree_sync_flush_entry(btree_raw_t *btree, struct FDF_thread_state *thd_state, btSyncRequest_t *list)
{
	btree_raw_mem_node_t	*n = NULL;
	btree_raw_node_t		*pnode = NULL;
	btree_status_t			ret = BTREE_SUCCESS;
	int						index;

	while ((list->ret == BTREE_SUCCESS) && ((list->dir_index < list->dir_count) ||
			(list->del_index < list->del_count))) {
		if (list->dir_index < list->dir_count) {
			while (list->dir_index < list->dir_count) {
				index = __sync_fetch_and_add(&(list->dir_index), 1);
				if (index < list->dir_count) {
					__sync_fetch_and_add(&(list->total_flush), 1);
					pnode = list->dir_nodes[index];
				} else {
					pnode = NULL;
				}
				if (pnode) {
					__sync_fetch_and_add(&(list->dir_written[index]), 1);
					assert(list->dir_written[index] == 1);
					uint64_t* logical_id = &pnode->logical_id;
					btree->write_node_cb(thd_state, &ret, btree->write_node_cb_data, &logical_id, (char**)&pnode, btree->nodesize, 1);
#if 0
					if (ret != BTREE_SUCCESS) {
						list->ret = ret;
						break;
					}
#endif
				}

			}
		} else if (list->del_index < list->del_count) {
			while (list->del_index < list->del_count) {
				index = __sync_fetch_and_add(&(list->del_index), 1);
				if (index < list->del_count) {
					__sync_fetch_and_add(&(list->total_flush), 1);
					n = list->del_nodes[index];
				} else {
					n = NULL;
				}
				if (n) {
					assert(list->del_written[index] == 0);
					list->del_written[index] = 1;
					ret = btree->delete_node_cb(btree->create_node_cb_data, n->pnode->logical_id);
					add_node_stats(btree, n->pnode, L1WRITES, 1);
#if 0
					if (ret != BTREE_SUCCESS) {
						list->ret = ret;
						break;
					}
#endif
				}
			}
		}
	}
}

static void
btree_sync_remove_entry(btree_raw_t *btree, btSyncRequest_t *list)
{
	btSyncRequest_t		*tmplist;

	if (((list->next == NULL) && (list->prev == NULL) && (btree->sync_first == list) && (btree->sync_last == list)) ||
		   (list->next || list->prev)) {
		tmplist = list->next;
		assert(tmplist != list);
		if (list->next) {
			list->next->prev = list->prev;
		} else {
			assert(list == btree->sync_last);
			btree->sync_last = list->prev;
			if (btree->sync_last) {
				btree->sync_last->next = NULL;
			}
		}
		if (list->prev) {
			list->prev->next = list->next;
		} else {
			assert(list == btree->sync_first);
			btree->sync_first = list->next;
			if ( btree->sync_first) {
				 btree->sync_first->prev = NULL;
			}
		}
		list->next = NULL;
		list->prev = NULL;
		if (btree->sync_first == NULL) {
			assert(btree->sync_last == NULL);
		}
	}
}

void unlock_and_unreference(btree_raw_t* btree, int last)
{
    btree_raw_mem_node_t *n;
	int i;

	assert(referenced_nodes_count);
	for(i = 0; i < referenced_nodes_count - last; i++)
	{
		n = referenced_nodes[i];
		node_unlock(n);
		deref_l1cache_node(btree, n);
	}

	if(last)
		referenced_nodes[0] = referenced_nodes[referenced_nodes_count - 1];

	referenced_nodes_count = last;
}

static btree_raw_mem_node_t* add_l1cache(btree_raw_t *btree, uint64_t logical_id, bool pinned)
{
    btree_raw_mem_node_t *node;

    node = malloc(sizeof(btree_raw_mem_node_t));
    assert(node);

    node->pnode = NULL;
    node->modified = 0;
    node->flag = 0;
#ifdef DEBUG_STUFF
    node->last_dump_modified = 0;
#endif
    node->pinned = pinned;
    plat_rwlock_init(&node->lock);

    node_lock(node, WRITE);

    if(!PMapCreate(btree->l1cache, (char *) &logical_id, sizeof(uint64_t), (char *) node, sizeof(uint64_t), btree->cguid, (void *)btree)) {
        node_unlock(node);
        plat_rwlock_destroy(&node->lock);
        free(node);
        return NULL;
    }

    dbg_referenced++;

    /*FE: This has to iterate through all parititions of the map, so costly. */
    //btree->stats.stat[BTSTAT_L1ENTRIES] = PMapNEntries(btree->l1cache);

    return node;
}

void ref_l1cache(btree_raw_t *btree, btree_raw_mem_node_t *n)
{
    assert(referenced_nodes_count < MAX_BTREE_HEIGHT);
    assert(n);
    dbg_print("%p id %ld root: %d leaf: %d over: %d dbg_referenced %lx\n", n, n->pnode->logical_id, is_root(btree, n->pnode), is_leaf(btree, n->pnode), is_overflow(btree, n->pnode), dbg_referenced);
    referenced_nodes[referenced_nodes_count++] = n;
}

static btree_raw_mem_node_t *get_l1cache(btree_raw_t *btree, uint64_t logical_id)
{
    btree_raw_mem_node_t *n;
    uint64_t datalen;

    if (PMapGet(btree->l1cache, (char *) &logical_id, sizeof(uint64_t), (char **) &n, &datalen, btree->cguid) == NULL)
        return NULL;

    dbg_referenced++;

    dbg_print("n %p node %p id %ld(%ld) lock %p root: %d leaf: %d over %d refcnt %d\n", n, n->pnode, n->pnode->logical_id, logical_id, &n->lock, is_root(btree, n->pnode), is_leaf(btree, n->pnode), is_overflow(btree, n->pnode), PMapGetRefcnt(btree->l1cache, (char *) &logical_id, sizeof(uint64_t), btree->cguid));

    return n;
}

static void modify_l1cache_node(btree_raw_t *btree, btree_raw_mem_node_t *node)
{
	assert(modified_nodes_count < MAX_BTREE_HEIGHT);
	node->modified++;
	if(!is_node_dirty(node)) {
		dbg_print("marked modified id=%ld modified_nodes_count=%ld\n", node->pnode->logical_id, modified_nodes_count);
		mark_node_dirty(node);
		modified_nodes[modified_nodes_count++] = node;
		PMapIncrRefcnt(btree->l1cache,(char *) &(node->pnode->logical_id), sizeof(uint64_t), btree->cguid);
		dbg_referenced++;
	}
}

inline static
void lock_nodes_list(btree_raw_t *btree, int lock, btree_raw_mem_node_t** list, int count)
{
    int i, j;
    btree_raw_mem_node_t     *node;

    for(i = 0; i < count; i++)
    {
        node = list[i];
        assert(node); // the node is in the cache, hence, get_l1cache cannot fail

		j = 0;
		while(j < i && list[j] != list[i]) {
			j++;
		}

		assert(j >= i);
        //if(j >= i && !is_overflow(btree, node->pnode) && node->pnode->logical_id != META_LOGICAL_ID+btree->n_partition) {
        dbg_print("list[%d]->logical_id=%ld lock=%p lock=%d locked=%lld\n", i, list[i]->pnode->logical_id, &node->lock, lock, locked);

		if(is_overflow(btree, node->pnode) || node->pnode->logical_id == META_LOGICAL_ID+btree->n_partition)
			continue;

        if(lock)
            node_lock(node, WRITE);
        else
            node_unlock(node);
        //}
    }
}

static void lock_modified_nodes_func(btree_raw_t *btree, int lock)
{
    dbg_print("lock %d start\n", lock);
    lock_nodes_list(btree, lock, modified_nodes, modified_nodes_count);
    lock_nodes_list(btree, lock, deleted_nodes, deleted_nodes_count);
    dbg_print("lock %d finish\n", lock);
}

btree_raw_mem_node_t *get_existing_node_low(btree_status_t *ret, btree_raw_t *btree, uint64_t logical_id,
		int ref, bool pinned, bool delete_after_deref)
{
	btree_raw_node_t  *pnode;
	btree_raw_mem_node_t  *n = NULL;

	if (*ret != BTREE_SUCCESS) { return(NULL); }

	*ret = BTREE_SUCCESS;

retry:
	//  check l1cache first
	n = get_l1cache(btree, logical_id);
	if (n != NULL) {
		//Got a deleted node?? Parent referring to deleted child??
		//Check the locking of btree/nodes
		if (is_node_deleted(n)) {
			assert(0);
		}

#ifndef _OPTIMIZED
		if (pinned && !n->pinned) {
			assert(0);
		}
#endif

		/* Below lock doesn't allow get_existing node return before pnode is really in the cache */
		node_lock(n, READ);

		/* IO Context has read this node, lets not delete the cache
		 * once we are done using the node. Also not count into hits
		 * stats if we are going to delete after the deref */
		if (delete_after_deref) {
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_BACKUP_L1HITS]), 1);
		} else {
			n->deref_delete_cache = false;
			add_node_stats(btree, n->pnode, L1HITS, 1);
		}
		node_unlock(n);
	} else {
		// already in the cache retry get
		n = add_l1cache(btree, logical_id, pinned);
		if(!n)
			goto retry;

		//  look for the node the hard way
		//  If we don't look at the ret code, why does read_node_cb need one?
		pnode = (btree_raw_node_t*)btree->read_node_cb(ret, btree->read_node_cb_data, logical_id);
		if (pnode == NULL) {
			*ret = BTREE_FAILURE;
			delete_l1cache(btree, logical_id);
			node_unlock(n);
			return(NULL);
		}
		assert(logical_id == pnode->logical_id);

		/* For scavenger and backup context reads, once we are done
		 * with the cache delete it. The PMapDelete already has
		 * the mechanism to avoid delete if someone else is already
		 * referring to the node */
		if (delete_after_deref) {
			n->deref_delete_cache = true;
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_BACKUP_L1MISSES]), 1);
		} else {
			n->deref_delete_cache = false;
			add_node_stats(btree, pnode, L1MISSES, 1);
		}

		n->pnode = pnode;
		plat_rwlock_unlock(&n->lock);
	}
	if(ref)
		ref_l1cache(btree, n);

	if (n == NULL) {
		*ret = BTREE_FAILURE;
		return(NULL);
	}

	return(n);
}

btree_raw_mem_node_t *get_existing_node(btree_status_t *ret, btree_raw_t *btree, uint64_t logical_id)
{
    return get_existing_node_low(ret, btree, logical_id, 1, 
                                 false /* pinned */, false /* deref_delete */);
}

static
btree_raw_mem_node_t *create_new_node(btree_raw_t *btree, uint64_t logical_id, bool ref, bool pinned)
{
    btree_raw_mem_node_t *n = NULL;
    btree_raw_node_t *pnode = (btree_raw_node_t *) btree_malloc(btree->nodesize);
//    memset(pnode, 0, btree->nodesize);
    // n = btree->create_node_cb(ret, btree->create_node_cb_data, logical_id);
    //  Just malloc the node here.  It will be written
    //  out at the end of the request by deref_l1cache().
    if (pnode != NULL) {
        pnode->logical_id = logical_id;
        n = add_l1cache(btree, logical_id, pinned);

        n->deref_delete_cache = false; /* New node should be in l1cache. Its for writes */
        n->pnode = pnode;

        /*
         * Zero out pstats for new node
         */
        reset_node_pstats(n->pnode);
        node_unlock(n);
        assert(n); /* the tree is exclusively locked */
		if(ref) {
			ref_l1cache(btree, n);
			modify_l1cache_node(btree, n);
		}
    }

    return n;
}

static btree_raw_mem_node_t *get_new_node_low(btree_status_t *ret, btree_raw_t *btree, uint32_t leaf_flags, uint16_t level, int ref)
{
	btree_raw_node_t  *n;
	btree_raw_mem_node_t  *node;
	uint64_t           logical_id;
	// pid_t  tid = syscall(SYS_gettid);

	if (*ret) { return(NULL); }

	logical_id = __sync_fetch_and_add(&btree->logical_id_counter, 1)*btree->n_partitions + btree->n_partition;
	if (BTREE_SUCCESS != savepersistent( btree, false)) {
		*ret = BTREE_FAILURE;
		return (NULL);
	}
	node = create_new_node(btree, logical_id, ref, false /*pinned */);
	n = node->pnode;

	if (n == NULL) {
		*ret = BTREE_FAILURE;
		return(NULL);
	}

	n->flags      = leaf_flags;
	//n->lsn        = 0;
	n->checksum   = 0;
	n->insert_ptr = btree->nodesize;
	n->nkeys      = 0;
	//n->prev       = 0; // used for chaining nodes for large objects
	n->next       = 0; // used for chaining nodes for large objects
	n->rightmost  = BAD_CHILD;
	n->level      = level;

	/* Update relevent node types, total count and bytes used in node */
	add_node_stats(btree, n, NODES, 1);
	add_node_stats(btree, n, BYTES, sizeof(btree_raw_node_t));

	return node;
}

static inline
btree_raw_mem_node_t *get_new_node(btree_status_t *ret, btree_raw_t *btree, uint32_t leaf_flags, uint16_t level)
{
	return get_new_node_low(ret, btree, leaf_flags, level, 1);
}

static void free_node(btree_status_t *ret, btree_raw_t *btree, btree_raw_mem_node_t *n)
{
	if (*ret) { return; }

    sub_node_stats(btree, n->pnode, NODES, 1);
    sub_node_stats(btree, n->pnode, BYTES, sizeof(btree_raw_node_t));

	assert(deleted_nodes_count < MAX_BTREE_HEIGHT);
	deleted_nodes[deleted_nodes_count++] = n;
	mark_node_deleted(n);
	PMapIncrRefcnt(btree->l1cache,(char *) &(n->pnode->logical_id), sizeof(uint64_t), btree->cguid);
	dbg_referenced++;
	//*ret = btree->delete_node_cb(n, btree->create_node_cb_data, n->logical_id);
}

/*   Split the 'from' node across 'from' and 'to'.
 *
 *   Returns: pointer to the key at which the split was done
 *            (all keys < key must go in node 'to')
 *
 *   THIS FUNCTION DOES NOT SET THE RETURN CODE...DOES ANY LOOK AT IT???
 */
static void split_copy(btree_status_t *ret, btree_raw_t *btree,
                       btree_raw_node_t *from, btree_raw_node_t *to,
                       char **key_out, uint32_t *keylen_out,
                       uint64_t *split_syndrome_out, uint64_t *split_seqno_out, uint32_t split_key)
{
	//node_fkey_t   *pfk;
	uint32_t       threshold, nbytes_to, nbytes_from, nkeys_to, nkeys_from;
	uint32_t       nbytes_fixed;
	key_stuff_t    ks;
	uint64_t       n_right     = 0;

	(void) get_key_stuff(btree, from, 0, &ks);

#ifdef DEBUG_STUFF
	if (Verbose) {
		fprintf(stderr, "********  Before split_copy for key '%s' [syn=%lu], rightmost %lx B-Tree BEGIN:  *******\n", dump_key(ks.pkey_val, ks.keylen), *split_syndrome_out, to->rightmost);
		btree_raw_dump(stderr, btree);
	//	fprintf(stderr, "********  Before split_copy for key '%s' [syn=%lu], To-Node:  *******\n", dump_key(ks.pkey_val, ks.keylen), *split_syndrome_out);
//		dump_node(btree, stderr, to, ks.pkey_val, ks.keylen);
//		fprintf(stderr, "********  Before split_copy for key '%s' [syn=%lu], B-Tree END:  *******\n", dump_key(ks.pkey_val, ks.keylen), *split_syndrome_out);
	}
#endif

	nbytes_fixed = ks.offset;

	if (ks.fixed) {
		nkeys_from    = ks.fkeys_per_node / 2; // from->nkeys / 2 ???
		nbytes_from   = nkeys_from * nbytes_fixed;

		nkeys_to   = from->nkeys - nkeys_from;
		nbytes_to  = nkeys_to * nbytes_fixed;

		//  last key in 'from' node gets inserted into parent
		//  For lack of a better place, we stash the split key
		//  in an unused key slot in the 'from' node.
		//  This temporary copy is only used by the caller to
		//  split_copy to insert the split key into the parent.

		assert(0); // I couldn't understand the following code
		/* pfk            = (node_fkey_t *) to->keys + nkeys_to;
		pfk->ptr       = ((node_fkey_t *) from->keys + nkeys_to)->ptr;
		key            = (char *) &(pfk->ptr);
		keylen         = sizeof(uint64_t);
		split_syndrome = ((node_fkey_t *) from->keys + nkeys_to - 1)->key;
		n_right        = ((node_fkey_t *) from->keys + nkeys_to - 1)->ptr; */

		ks.pkey_val    = (char *) &((node_fkey_t *) from->keys + nkeys_from - 1)->ptr;
		ks.keylen      = sizeof(uint64_t);
		ks.syndrome    = ((node_fkey_t *) from->keys + nkeys_from - 1)->key;
		n_right        = ((node_fkey_t *) from->keys + nkeys_from - 1)->ptr;
	} else {
		threshold = (btree->nodesize - sizeof(btree_raw_node_t)) / 2;

		nbytes_from = nkeys_from = 0;

		while(nkeys_from < from->nkeys && ((split_key && nkeys_from < split_key) ||
				(!split_key && (nbytes_from + nkeys_from * nbytes_fixed) <= threshold)))
		{
			(void) get_key_stuff(btree, from, nkeys_from, &ks);

			nbytes_from += ks.keylen;

			if (ks.leaf && !big_object_kd(btree, ks.keylen, ks.datalen))
				nbytes_from += ks.datalen;

			n_right = ks.ptr;

			nkeys_from++;
		}
		assert(nkeys_from > 0 && nkeys_from < from->nkeys); // xxxzzz remove me!

		nkeys_to   = from->nkeys - nkeys_from;
		nbytes_to  = btree->nodesize - from->insert_ptr - nbytes_from;
	}

	dbg_print_key(ks.pkey_val, ks.keylen, "nkeys_from=%d nkeys_to=%d nbytes_from=%d nbytes_to=%d nbytes_fixed=%d", nkeys_from, nkeys_to, nbytes_from, nbytes_to, nbytes_fixed);

	memcpy(to->keys, (char*)from->keys + nkeys_from * nbytes_fixed, nkeys_to * nbytes_fixed);
	to->nkeys = nkeys_to;
	from->nkeys = ks.leaf ? nkeys_from : nkeys_from - 1;

	if (ks.fixed) {
		to->insert_ptr   = 0;
		from->insert_ptr = 0;
	} else {
		// for variable sized keys, copy the variable sized portion
		//  For leaf nodes, copy the data too

		memcpy(((char *) to) + btree->nodesize - nbytes_to,
				((char *) from) + from->insert_ptr,
				nbytes_to);

		to->insert_ptr   = btree->nodesize - nbytes_to;
		from->insert_ptr = btree->nodesize - nbytes_from;

		/* skip rightmost key for non-leaves */
		if (!ks.leaf)
			from->insert_ptr += ks.keylen;

		update_keypos(btree, to, 0);
	}

	// update the rightmost pointers of the 'to' node
	/* pnode->next pointer used to link nodes instead */
	/*if (ks.leaf)
	{
		to->rightmost = from->rightmost;
		from->rightmost = to->logical_id;
	}*/
	if(!ks.leaf) {
		to->rightmost = from->rightmost;
		from->rightmost = n_right;
	}

	*key_out            = ks.pkey_val;
	*keylen_out         = ks.keylen;
	*split_syndrome_out = ks.syndrome;
	*split_seqno_out    = ks.seqno;

#ifdef DEBUG_STUFF
	if (Verbose) {
//		fprintf(stderr, "********  After split_copy for key '%s' [syn=%lu], rightmost %lx B-Tree BEGIN:  *******\n", dump_key(ks.pkey_val, ks.keylen), *split_syndrome_out, to->rightmost);
//		btree_raw_dump(stderr, btree);
		fprintf(stderr, "********  After split_copy for key '%s' [syn=%lu], From-Node:  *******\n", dump_key(ks.pkey_val, ks.keylen), *split_syndrome_out);
		dump_node(btree, stderr, from, ks.pkey_val, ks.keylen);
		fprintf(stderr, "********  After split_copy for key '%s' [syn=%lu], To-Node:  *******\n", dump_key(ks.pkey_val, ks.keylen), *split_syndrome_out);
		dump_node(btree, stderr, to, ks.pkey_val, ks.keylen);
		fprintf(stderr, "********  After split_copy for key '%s' [syn=%lu], B-Tree END:  *******\n", dump_key(ks.pkey_val, ks.keylen), *split_syndrome_out);
	}
#endif

	return;
}

static int has_fixed_keys(btree_raw_t *btree, btree_raw_node_t *n)
{
    return((btree->flags & SYNDROME_INDEX) && !is_leaf(btree, n));
}

static void update_keypos_low(btree_raw_t *btree, btree_raw_node_t *n, uint32_t n_key_start, uint32_t keypos)
{
    int            i;
    node_vkey_t   *pvk;
    node_vlkey_t  *pvlk;

    if (has_fixed_keys(btree, n)) {
        return;
    }

    if (n->flags & LEAF_NODE) {
	for (i=n_key_start; i<n->nkeys; i++) {
	    pvlk = (node_vlkey_t *) (((char *) n->keys) + i*sizeof(node_vlkey_t));
	    keypos -= pvlk->keylen;
	    if (!big_object(btree, pvlk)) {
	        //  data is NOT overflowed!
		keypos -= pvlk->datalen;
	    }
	    pvlk->keypos = keypos;
		//dbg_print("nkey=%d keypos=%d\n", i, keypos);
	}
    } else {
	for (i=n_key_start; i<n->nkeys; i++) {
	    pvk = (node_vkey_t *) (((char *) n->keys) + i*sizeof(node_vkey_t));
	    keypos -= pvk->keylen;
	    pvk->keypos = keypos;
	}
    }
}

static void update_keypos(btree_raw_t *btree, btree_raw_node_t *n, uint32_t n_key_start)
{
	update_keypos_low(btree, n, n_key_start, btree->nodesize);
}

/*   Insert a new key into a node (and possibly its data if this is a leaf)
 *   This assumes that we have enough space!
 *   It is the responsibility of the caller to check!
 */
static void insert_key_low(btree_status_t *ret, btree_raw_t *btree, btree_raw_mem_node_t *node, char *key, uint32_t keylen, uint64_t seqno, uint64_t datalen, char *data, uint64_t syndrome, node_key_t* pkrec, node_key_t* pk_insert)
{
    btree_raw_node_t* x = node->pnode;
    btree_metadata_t meta;
    node_vkey_t   *pvk;
    node_vlkey_t  *pvlk;
    node_fkey_t   *pfk;
    uint32_t       nkeys_to = 0, nkeys_from = 0;
    uint32_t       fixed_bytes;
    uint64_t       child_id, child_id_before, child_id_after;
    node_vkey_t   *pvk_insert;
    node_vlkey_t  *pvlk_insert;
    uint32_t       pos_new_key = 0;
    key_stuff_t    ks;
    uint32_t       vbytes_this_node = 0;
    uint64_t       ptr_overflow = 0;
    uint32_t       pos_split = 0;
    uint32_t       nbytes_split = 0;
    uint32_t       nbytes_free;
    int32_t        nkey_child;
    uint64_t       nbytes_stats;

	if(!is_leaf(btree, x))
    dbg_print_key(key, keylen, "node: %p id %ld keylen: %d datalen: %ld ptr=%ld ret=%d", x, x->logical_id, keylen, datalen, *(uint64_t*)data, *ret);
	else
    dbg_print_key(key, keylen, "node: %p id %ld keylen: %d datalen: %ld ret=%d", x, x->logical_id, keylen, datalen, *ret);

    assert(!is_leaf(btree, node->pnode));

    if (*ret) { return; }

    nbytes_stats = keylen;

    if (pkrec != NULL) {
        // delete existing key first
        delete_key_by_pkrec(ret, btree, node, pkrec);
        assert((*ret) == BTREE_SUCCESS);

        meta.flags = READ_SEQNO_LE;
        meta.end_seqno = seqno - 1;
        pkrec = find_key_non_leaf(btree, x, key, keylen, &meta, syndrome, &child_id, &child_id_before, &child_id_after, &pk_insert, &nkey_child);
        assert(pkrec == NULL);
    } else {
        modify_l1cache_node(btree, node);
    }

    (void) get_key_stuff(btree, x, 0, &ks);

    if (pk_insert == NULL || !x->nkeys) {
	nkeys_to     = x->nkeys;
	pos_split    = x->insert_ptr;
	nbytes_split = 0;
    }
    else
    {
	nkeys_to = (((char *) pk_insert) - ((char *) x->keys)) / ks.offset;

	if (!ks.fixed) {
		if (x->flags & LEAF_NODE) {
			pvlk_insert = (node_vlkey_t *) pk_insert;
			pos_split = pvlk_insert->keypos + pvlk_insert->keylen;
			if(!big_object(btree, pvlk_insert))
				pos_split += pvlk_insert->datalen;
			nbytes_stats += sizeof(node_vlkey_t);
			assert(pvlk_insert->keypos < btree->nodesize);
			assert(pvlk_insert->keylen < btree->nodesize);
		} else {
			pvk_insert = (node_vkey_t *) pk_insert;
			pos_split = pvk_insert->keypos + pvk_insert->keylen;
			nbytes_stats += sizeof(node_vkey_t);

			assert(pvk_insert->keypos < btree->nodesize);
			assert(pvk_insert->keylen < btree->nodesize);
		}
		nbytes_split = pos_split - x->insert_ptr;
	} else
		nbytes_stats += sizeof(node_fkey_t);
    }

    fixed_bytes = ks.offset;
    nkeys_from = x->nkeys - nkeys_to;

    if ((!ks.fixed) && 
        (x->flags & LEAF_NODE) && 
	big_object_kd(btree, keylen, datalen)) // xxxzzz check this!
    { 
	//  Allocate nodes for overflowed objects first, in case
	//  something goes wrong.
        
		ptr_overflow = allocate_overflow_data(btree, datalen, data);
		if ((ptr_overflow == 0) && (datalen != 0)) {
			// something went wrong with the allocation
			*ret = BTREE_FAILURE;
			return;
		}
    }

    if (ks.fixed) {
		// check that there is enough space!
		assert(x->nkeys < (btree->fkeys_per_node));
    } else {
        if (x->flags & LEAF_NODE) {

			//  insert variable portion of new key (and possibly data) in
			//  sorted key order at end of variable data stack in node

			if (big_object_kd(btree, keylen, datalen)) { // xxxzzz check this!
				//  put key in this node, data in overflow nodes
				vbytes_this_node = keylen;
			} else {
				//  put key and data in this node
				vbytes_this_node = keylen + datalen;
			}
			// check that there is enough space!
			nbytes_free = vlnode_bytes_free(x);
			assert(nbytes_free >= (sizeof(node_vlkey_t) + vbytes_this_node));

			//  make space for variable portion of new key/data

			memmove((char *) x + pos_split - nbytes_split - vbytes_this_node,
				(char *) x + pos_split - nbytes_split, 
				nbytes_split);

			pos_new_key = pos_split - vbytes_this_node;

			//  insert variable portion of new key

			memcpy((char *) x + pos_new_key, key, keylen);
			if (vbytes_this_node > keylen) {
				//  insert data
				memcpy((char *) x + pos_new_key + keylen, data, datalen);
			}
		} else {
			vbytes_this_node = keylen;

			// check that there is enough space!
			nbytes_free = vnode_bytes_free(x);
			assert(nbytes_free >= (sizeof(node_vkey_t) + vbytes_this_node));

			//  make space for variable portion of new key/data

			memmove((char *) x + pos_split - nbytes_split - vbytes_this_node,
				(char *) x + pos_split - nbytes_split, 
				nbytes_split);

			pos_new_key = pos_split - vbytes_this_node;

			//  insert variable portion of new key

			memcpy((char *) x + pos_new_key, key, keylen);
		}
    }

    //  Make space for fixed portion of new key.
    // 
    //  NOTE: This MUST be done after updating the variable part
    //        because the variable part uses key data in its old location!
    //

    if (nkeys_from != 0) {
		memmove((char *) (x->keys) + (nkeys_to + 1)*fixed_bytes, (char *) (x->keys) + nkeys_to*fixed_bytes, nkeys_from*fixed_bytes);
    }

    if (!ks.fixed) {
		x->insert_ptr -= vbytes_this_node;
    } else {
		x->insert_ptr = 0; // xxxzzz this should be redundant!
    }

    //  Do this here because update_keypos() requires it!
    x->nkeys += 1;

    //  insert fixed portion of new key
    if (!ks.fixed) {
		if (x->flags & LEAF_NODE) {
			pvlk           = (node_vlkey_t *) ((char *) (x->keys) + nkeys_to*fixed_bytes);
			pvlk->keylen   = keylen;
			pvlk->keypos   = pos_new_key;
			pvlk->datalen  = datalen;
			pvlk->seqno    = seqno;
			if (big_object_kd(btree, keylen, datalen)) { // xxxzzz check this!
				//  data is in overflow nodes
				pvlk->ptr = ptr_overflow;
			} else {
				//  data is in this node
				pvlk->ptr = 0;
                nbytes_stats += datalen;
			}
		} else {
			pvk          = (node_vkey_t *) ((char *) (x->keys) + nkeys_to*fixed_bytes);
			pvk->keylen  = keylen;
			pvk->keypos  = pos_new_key;
			pvk->seqno   = seqno;
			assert(datalen == sizeof(uint64_t));
			pvk->ptr     = *((uint64_t *) data);
		}

		//  update all of the 'keypos' fields in the fixed portion
		update_keypos_low(btree, x, nkeys_to, pos_split);
		//update_keypos(btree, x, 0);

    } else {
        pfk            = (node_fkey_t *) ((char *) (x->keys) + nkeys_to*fixed_bytes);
		pfk->key       = syndrome;
		pfk->seqno     = seqno;
		assert(datalen == sizeof(uint64_t));
		pfk->ptr       = *((uint64_t *) data);
    }

    /*
     * Update node stats
     */
    //set_node_pstats(btree, x, 1, true);

    if (x->flags & LEAF_NODE) {
	assert(0);
        /* A new object has been inserted. increment the count */
	__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_NUM_OBJS]), 1);
	__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_LEAF_BYTES]), nbytes_stats);
    } else {
	__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_NONLEAF_BYTES]), nbytes_stats);
    }

#if 0
    /*
     * Update node stats
     */
    x->pstats.seq_num = btree->last_flushed_seq_num;
    x->pstats.delta_cntr_sz    += nbytes_stats;
    x->pstats.delta_obj_count  += 1;
    x->pstats.is_positive_delta  = true;
    fprintf(stderr, "Insert delta_obj_count=%ld seq_num=%ld\n", x->pstats.delta_obj_count, x->pstats.seq_num);
#endif

#if 0 //def DEBUG_STUFF
//#ifdef DEBUG_STUFF
	if (Verbose) {
	    char  stmp[10000];
	    int   len;
	    if ((btree->flags & SYNDROME_INDEX) && !(x->flags & LEAF_NODE)) {
	        sprintf(stmp, "%p", key);
			len = strlen(stmp);
	    } else {
			strncpy(stmp, key, keylen);
			len = keylen;
	    }

        uint64_t ddd = dbg_referenced;
	    fprintf(stderr, "%x ********  After insert_key '%s' [syn %lu], datalen=%ld, node %p BEGIN:  *******\n", (int)pthread_self(), dump_key(stmp, len), syndrome, datalen, x);
	    btree_raw_dump(stderr, btree);
	    fprintf(stderr, "%x ********  After insert_key '%s' [syn %lu], datalen=%ld, NODE:  *******\n", (int)pthread_self(), dump_key(stmp, len), syndrome, datalen);
        assert(ddd == dbg_referenced);
	    (void) get_key_stuff(btree, x, 0, &ks);
	    if ((btree->flags & SYNDROME_INDEX) && !(x->flags & LEAF_NODE)) {
	        sprintf(stmp, "%p", ks.pkey_val);
			dump_node(btree, stderr, x, stmp, strlen(stmp));
	    } else {
			dump_node(btree, stderr, x, ks.pkey_val, ks.keylen);
	    }
        assert(ddd == dbg_referenced);
	    fprintf(stderr, "%x ********  After insert_key '%s' [syn %lu], datalen=%ld, node %p END  *******\n", (int)pthread_self(), dump_key(stmp, len), syndrome, datalen, x);
	}
#endif
}

static void insert_key(btree_status_t *ret, btree_raw_t *btree, btree_raw_mem_node_t *node, char *key, uint32_t keylen, uint64_t seqno, uint64_t datalen, char *data, uint64_t syndrome)
{
    btree_metadata_t meta;
    btree_raw_node_t* x = node->pnode;
    uint64_t       child_id, child_id_before, child_id_after;
    int32_t        nkey_child;
    node_key_t    *pk_insert;
    node_key_t *pkrec = NULL;

    assert(!is_leaf(btree, node->pnode));

    /* If there are multiple entries for same keys, find the one
     * next least to new seqno to be inserted */
    meta.flags     = READ_SEQNO_LE;
    meta.end_seqno = seqno - 1; /* Need to insert before prev seqno */
    (void)find_key_non_leaf(btree, x, key, keylen, &meta, syndrome,
                            &child_id, &child_id_before, &child_id_after,
                            &pk_insert, &nkey_child);

    return insert_key_low(ret, btree, node, key, keylen, seqno, datalen, data, syndrome, pkrec, pk_insert);
}

static void delete_key_by_pkrec(btree_status_t* ret, btree_raw_t *btree, btree_raw_mem_node_t *node, node_key_t *pk_delete)
{
    btree_raw_node_t* x = node->pnode;
    uint32_t       nkeys_to, nkeys_from;
    uint32_t       fixed_bytes;
    uint64_t       datalen = 0;
    uint64_t       keylen = 0;
    uint64_t       nbytes_stats = 0;
    node_vkey_t   *pvk_delete = NULL;
    node_vlkey_t  *pvlk_delete = NULL;
    key_stuff_t    ks;

	dbg_print("node_id=%ld pk_delete=%p\n", node->pnode->logical_id, pk_delete);

    assert(pk_delete);

    assert(!is_leaf(btree, node->pnode));

    assert(!is_leaf(btree, node->pnode));

    if(*ret) return;

    (void) get_key_stuff(btree, x, 0, &ks);

    modify_l1cache_node(btree, node);

    if (!ks.fixed) {
        if (x->flags & LEAF_NODE) {
	    pvlk_delete = (node_vlkey_t *) pk_delete;

	    keylen = pvlk_delete->keylen;
	    if (big_object(btree, pvlk_delete)) {
	        // data NOT stored in the node
		datalen = 0;
                delete_overflow_data(ret, btree, pvlk_delete->ptr, pvlk_delete->datalen);
	    } else {
	        // data IS stored in the node
		datalen = pvlk_delete->datalen;
	    }
            nbytes_stats = sizeof(node_vlkey_t) + datalen;
	} else {
	    pvk_delete = (node_vkey_t *) pk_delete;
	    keylen = pvk_delete->keylen;
            nbytes_stats = sizeof(node_vkey_t);
	}
        nbytes_stats += keylen;
    } else
        nbytes_stats = sizeof(node_fkey_t);

    fixed_bytes = ks.offset;
    nkeys_to = (((char *) pk_delete) - ((char *) x->keys))/ks.offset;

    if (x->flags & LEAF_NODE) {
	assert(0);
        __sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_NUM_OBJS]), 1);
	__sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_LEAF_BYTES]), nbytes_stats);
    } else {
	__sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_NONLEAF_BYTES]), nbytes_stats);
    }

    nkeys_from = x->nkeys - nkeys_to - 1;

    if (!ks.fixed) {
	assert(keylen);
	//  remove variable portion of key
        if (x->flags & LEAF_NODE) {
	    memmove((char *) x + x->insert_ptr + keylen + datalen, 
		    (char *) x + x->insert_ptr, 
		    pvlk_delete->keypos - x->insert_ptr);
	    x->insert_ptr += (keylen + datalen);
	} else {
	    memmove((char *) x + x->insert_ptr + keylen, 
		    (char *) x + x->insert_ptr, 
		    pvk_delete->keypos - x->insert_ptr);
	    x->insert_ptr += keylen;
	}
    }

    //  Remove fixed portion of deleted key.
    // 
    //  NOTE: This MUST be done after deleting the variable part
    //        because the variable part uses key data in its old location!
    //

    memmove((char *) (x->keys) + nkeys_to*fixed_bytes, (char *) (x->keys) + (nkeys_to+1)*fixed_bytes, nkeys_from*fixed_bytes);

    //  Do this here because update_keypos() requires it!
    x->nkeys -= 1;

    //  delete fixed portion of new key
    if (!ks.fixed) {
	//  update all of the 'keypos' fields in the fixed portion
	update_keypos(btree, x, 0);
    } else {
	x->insert_ptr = 0; // xxxzzz this should be redundant!
    }

    #if 0 //def DEBUG_STUFF
	if (Verbose) {
	    char stmp[10000];
	    int  len;
	    if (btree->flags & SYNDROME_INDEX) {
	        sprintf(stmp, "%p", pvlk_delete->key);
		len = strlen(stmp);
	    } else {
		strncpy(stmp, pvlk_delete->key, pvlk_delete->keylen);
		len = pvlk_delete->keylen;
	    }
	    fprintf(stderr, "********  After delete_key '%s' [syn %lu]:  *******\n", dump_key(stmp, len), syndrome);
	    btree_raw_dump(stderr, btree);
	}
    #endif
}

static void update_ptr(btree_raw_t *btree, btree_raw_node_t *n, uint32_t nkey, uint64_t ptr)
{
    node_vlkey_t  *pvlk;
    node_vkey_t   *pvk;
    node_fkey_t   *pfk;

    if (is_leaf(btree, n)) {
	pvlk = ((node_vlkey_t *) n->keys) + nkey;
	pvlk->ptr = ptr;
    } else if (btree->flags & SECONDARY_INDEX) {
	pvk      = ((node_vkey_t *) n->keys) + nkey;
	pvk->ptr = ptr;
    } else {
	pfk      = ((node_fkey_t *) n->keys) + nkey;
	pfk->ptr = ptr;
    }
}

void delete_key_by_index_non_leaf(btree_status_t* ret, btree_raw_t *btree, btree_raw_mem_node_t *node, int index)
{
	key_stuff_info_t key_info = {0};
	node_key_t *pkeyrec = NULL;
	
	if (*ret) { return; }
	assert(!is_leaf(btree, node->pnode));

	get_key_stuff_info(btree, node->pnode, index, &key_info);		
	pkeyrec = key_info.keyrec;
	delete_key_by_pkrec(ret, btree, node, pkeyrec);

	assert(*ret == BTREE_SUCCESS);
}

void 
delete_key_by_index_leaf(btree_status_t* ret, btree_raw_t *btree, btree_raw_mem_node_t *node, int index)
{
	key_info_t key_info = {0};
	bool res = false; 
	uint64_t datalen = 0;
	int32_t bytes_decreased = 0;

	assert(is_leaf(btree, node->pnode));

	modify_l1cache_node(btree, node);

	res = btree_leaf_get_nth_key_info(btree, node->pnode, index, &key_info);
	assert(res == true);

	if ((key_info.keylen + key_info.datalen) >=
	    btree->big_object_size) {
		datalen = 0;
                delete_overflow_data(ret, btree, key_info.ptr, key_info.datalen);
	} else {
		datalen = key_info.datalen;
	}

	res = btree_leaf_remove_key_index(btree, node->pnode, index, &key_info, &bytes_decreased);	

	assert(res == true);
	if (res == true) {
		__sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_LEAF_BYTES]), bytes_decreased);
		*ret = BTREE_SUCCESS;
	} else {
		*ret = BTREE_FAILURE;
	}
	free_buffer(btree, key_info.key);
}

void
delete_key_by_index(btree_status_t* ret, btree_raw_t *btree,
		    btree_raw_mem_node_t *node, int index)
{
	if (is_leaf(btree, node->pnode)) {
		delete_key_by_index_leaf(ret, btree, node, index);
	} else {
		delete_key_by_index_non_leaf(ret, btree, node, index);
	}
}

static void delete_key(btree_status_t *ret, btree_raw_t *btree, btree_raw_mem_node_t *node, char *key, uint32_t keylen, uint64_t seqno, uint64_t syndrome)
{
    uint64_t       child_id;
    bool key_exists = true;
    int index = -1;
    uint64_t child_id_before, child_id_after;
    btree_metadata_t meta;

    if (*ret) { return; }

    meta.flags = READ_SEQNO_EQ;
    meta.seqno = seqno;
    index = bsearch_key(btree, node->pnode, key, keylen, &meta, syndrome, BSF_LATEST, &key_exists, &child_id);

    if (key_exists == false) {
	*ret = BTREE_KEY_NOT_FOUND; 
	return;
    }

    delete_key_by_index(ret, btree, node, index);
}

static btree_raw_mem_node_t *
btree_split_child(btree_status_t *ret, btree_raw_t *btree, btree_raw_mem_node_t *n_parent,
                  btree_raw_mem_node_t *n_child, btree_metadata_t *meta,
                  uint64_t syndrome, int child_nkey, int split_key)
{
    btree_raw_mem_node_t     *n_new;
    uint32_t              keylen = 0;
    char                 *key = NULL;
    uint64_t              split_syndrome = 0;
	btree_status_t ret1 = BTREE_FAILURE;
    uint64_t              split_seqno = 0;
    bool free_key = false;
    bool res = false;
    int32_t bytes_increased = 0;
    btree_metadata_t tmp_meta;

    if (*ret) { return NULL; }

    __sync_add_and_fetch(&(btree->stats.stat[BTSTAT_SPLITS]),1);

    n_new = get_new_node(ret, btree, is_leaf(btree, n_child->pnode) ? LEAF_NODE : 0, n_child->pnode->level);
    if (BTREE_SUCCESS != *ret)
        return NULL;

	dbg_print("split_key=%d n_child_nkeys=%d n_new_nkeys=%d\n", split_key, n_child->pnode->nkeys, n_new->pnode->nkeys);
    // n_parent will be marked modified by insert_key()
    // n_new was marked in get_new_node()
    dbg_print("n_child=%ld n_parent=%ld n_new=%ld\n", n_child->pnode->logical_id, n_parent->pnode->logical_id, n_new->pnode->logical_id);

    modify_l1cache_node(btree, n_child);

	if(split_key < n_child->pnode->nkeys)
 	{
		if (is_leaf(btree, n_child->pnode)) {
			/*
			 *  split btree leaf node
			 */
			res =  btree_leaf_split(btree, n_child->pnode, n_new->pnode, &key,
					&keylen, &split_syndrome, &split_seqno, &bytes_increased, split_key);
			if (res == false) {
				*ret = BTREE_FAILURE;
			} else {
				*ret = BTREE_SUCCESS;
			}
			free_key = true;
			/*
			 * If split has increased the used space, then it is loss in our space saving.
			 */
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_SPCOPT_BYTES_SAVED]),
					-bytes_increased);
			/*
			 * Split has increased the space used, so adjust the num leaf bytes counter.
			 */
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_LEAF_BYTES]), bytes_increased);

		} else {
			split_copy(ret, btree, n_child->pnode, n_new->pnode, &key, &keylen, &split_syndrome, &split_seqno, split_key);
		}
	}
    
    if (BTREE_SUCCESS == *ret) {
	//  Add the split key in the parent
	/* Update old record in parent to point to newly created right node */
	assert(child_nkey != -1);
	assert(n_parent->pnode->rightmost != 0);
        dbg_print("child_nkey=%d n_child->pnode->nkeys=%d !!!\n", child_nkey, n_parent->pnode->nkeys);
	if(child_nkey == n_parent->pnode->nkeys)
	    n_parent->pnode->rightmost = n_new->pnode->logical_id;
	else
	    update_ptr(btree, n_parent->pnode, child_nkey, n_new->pnode->logical_id);

	/* Insert new record in parent for the updated left child. If its not
	 * bulk insert split */
	if(!split_key)
		insert_key(ret, btree, n_parent, key, keylen, split_seqno, 
				sizeof(uint64_t), (char *) &(n_child->pnode->logical_id), split_syndrome);

	assert(n_parent->pnode->rightmost != 0);

	btree->log_cb(ret, btree->log_cb_data, BTREE_UPDATE_NODE, btree, n_parent);
	btree->log_cb(ret, btree->log_cb_data, BTREE_UPDATE_NODE, btree, n_child);
	btree->log_cb(ret, btree->log_cb_data, BTREE_CREATE_NODE, btree, n_new);
    }

    #ifdef DEBUG_STUFF
	if (Verbose) {
		dump_node(btree, stderr, n_child->pnode, key, keylen);
		dump_node(btree, stderr, n_new->pnode, NULL, 0);
		dump_node(btree, stderr, n_parent->pnode, NULL, 0);
	}
    #endif

    /* Link nodes of one level with next pointers */
    n_new->pnode->next = n_child->pnode->next;
    n_child->pnode->next = n_new->pnode->logical_id;
    dbg_print("nextptr after n_child=%ld n_parent=%ld n_new=%ld\n", n_child->pnode->next, n_parent->pnode->next, n_new->pnode->next);

    if (free_key) {	
	free_buffer(btree, key);
    }	

    return n_new;
}

//  Check if a node has enough space for insertion
//  of a totally new item.
static btree_status_t is_full_insert(btree_raw_t *btree, btree_raw_node_t *n, uint32_t keylen, uint64_t datalen)
{
    btree_status_t ret = BTREE_SUCCESS;
    uint32_t   nbytes_free = 0;

    if (n->flags & LEAF_NODE) {
        // vlkey
        nbytes_free = vlnode_bytes_free(n);
		if (big_object_kd(btree, keylen, datalen)) { // xxxzzz check this!
			//  don't include datalen because object data is kept
			//  in overflow btree nodes
			if (nbytes_free < (sizeof(node_vlkey_t) + keylen)) {
				ret = BTREE_FAILURE;
			}
		} else {
			if (nbytes_free < (sizeof(node_vlkey_t) + keylen + datalen)) {
				ret = BTREE_FAILURE;
			}
		}
    } else if (btree->flags & SECONDARY_INDEX) {
        // vkey
        nbytes_free = vnode_bytes_free(n);
		// if (nbytes_free < (sizeof(node_vkey_t) + keylen)) {
		if (nbytes_free < (sizeof(node_vkey_t) + btree->max_key_size)) {
			ret = BTREE_FAILURE;
		}
    } else {
        // fkey
		if (n->nkeys > (btree->fkeys_per_node-1)) {
			ret = BTREE_FAILURE;
		}
    }

//    dbg_print("nbytes_free: %d keylen %d datalen %ld nkeys %d vlkey_t %ld raw_node_t %ld insert_ptr %d ret %d\n", nbytes_free, keylen, datalen, n->nkeys, sizeof(node_vlkey_t), sizeof(btree_raw_node_t), n->insert_ptr, ret);

    return (ret);
}

//  Check if a leaf node has enough space for an update of
//  an existing item.
static btree_status_t is_full_update(btree_raw_t *btree, btree_raw_node_t *n, node_vlkey_t *pvlk, uint32_t keylen, uint64_t datalen)
{
    btree_status_t        ret = BTREE_SUCCESS;
    uint32_t              nbytes_free = 0;
    uint64_t              update_bytes = 0;

    assert(n->flags & LEAF_NODE);  //  xxxzzz remove this

    if (big_object_kd(btree, keylen, datalen)) { // xxxzzz check this!
        //  updated data will be put in overflow node(s)
        update_bytes = keylen;
    } else {
        //  updated data fits in a node
        update_bytes = keylen + datalen;
    }

    // must be vlkey!
    nbytes_free = vlnode_bytes_free(n);
    if (big_object(btree, pvlk)) { // xxxzzz check this!
        //  Data to be overwritten is in overflow node(s).
	if ((nbytes_free + pvlk->keylen) < update_bytes) {
	    ret = BTREE_FAILURE;
	}
    } else {
        //  Data to be overwritten is in this node.
	if ((nbytes_free + pvlk->keylen + pvlk->datalen) < update_bytes) {
	    ret = BTREE_FAILURE;
	}
    }
//    dbg_print("nbytes_free: %d pvlk->keylen %d pvlk->datalen %ld keylen %d datalen %ld update_bytes %ld insert_ptr %d nkeys %d ret %d\n", nbytes_free, pvlk->keylen, pvlk->datalen, keylen, datalen, update_bytes, n->insert_ptr, n->nkeys, ret);

    return(ret);
}

static bool 
is_node_full_non_leaf(btree_raw_t *bt, btree_raw_node_t *r, char *key, uint32_t keylen,
		      uint64_t datalen, btree_metadata_t *meta, uint64_t syndrome, bool key_exists)
{
	btree_status_t btree_ret = BTREE_FAILURE;

	assert(!is_leaf(bt, r));

	//  non-leaf nodes
	if (key_exists == false) {
		btree_ret = is_full_insert(bt, r, keylen, datalen);
	} else if (bt->flags & SECONDARY_INDEX) {
		// must be enough room for max sized key in case child is split!
		btree_ret = is_full_insert(bt, r, keylen, datalen);
	} else {
		// SYNDROME_INDEX
		// must be enough room for max sized key in case child is split!
		btree_ret = is_full_insert(bt, r, keylen, datalen);
	}

	if (btree_ret == BTREE_SUCCESS) {
		return false;
	} else {
		return true;
	}
}

static bool
is_node_full_leaf(btree_raw_t *bt, btree_raw_node_t *r, char *key, uint32_t keylen,
		  uint64_t datalen, btree_metadata_t *meta, uint64_t syndrome,bool key_exists, int index)
{
	bool res = false;

	res = btree_leaf_is_full_index(bt, r, key, keylen, datalen,
				       meta, syndrome, key_exists, index);
	return res;
}

static bool 
is_node_full(btree_raw_t *bt, btree_raw_node_t *r, char *key, uint32_t keylen,
	     uint64_t datalen, btree_metadata_t *meta, uint64_t syndrome,bool key_exists, int index)
{
	bool res = false;
	if (is_leaf(bt, r) == true) {
		res = is_node_full_leaf(bt, r, key, keylen, datalen,
					meta, syndrome, key_exists, index);
	} else {
		res = is_node_full_non_leaf(bt, r, key, keylen, datalen,
					    meta, syndrome, key_exists);
	}

	return res;
}

int 
is_leaf_minimal_after_delete(btree_raw_t *btree, btree_raw_node_t *n, int index)
{
#ifdef UNUSED
    key_meta_t key_meta = {0};
    uint32_t datalen = 0;
#endif
    uint32_t nbytes_used = 0;

    assert(is_leaf(btree, n));

#ifdef UNUSED
    btree_leaf_get_meta(node, index, &key_meta);
    datalen = ((key_meta.keylen + key_meta.datalen) < btree->big_object_size) ? key_meta.datalen : 0;
#endif
    nbytes_used = btree_leaf_used_space(btree, n);

    // TBD:  Need to account for the space occupied by the key to be deleted

    return (2 * nbytes_used) < (btree->nodesize_less_hdr);
}

static int is_minimal_non_leaf(btree_raw_t *btree, btree_raw_node_t *n, uint32_t l_balance_keylen, uint32_t r_balance_keylen)
{
    uint32_t   nbytes_used;
    int        ret = 0;
    uint32_t   max_balance_keylen;

    if (n->logical_id == btree->rootid) {
        // root
	if (!(n->flags & LEAF_NODE) && (n->nkeys == 0)) {
	    ret = 1;
	} else {
	    ret = 0;
	}
    } else {
        // non-root
	if (n->flags & LEAF_NODE) {
	    nbytes_used = (btree->nodesize - n->insert_ptr) + n->nkeys*sizeof(node_vlkey_t);
	} else if (btree->flags & SYNDROME_INDEX) {
	    //  The '+1' here is to allow for conversion of a rightmost pointer to
	    //  a key value during a merge!
	    nbytes_used = (n->nkeys + 1)*sizeof(node_fkey_t);
	} else {
	    max_balance_keylen = (l_balance_keylen > r_balance_keylen) ? l_balance_keylen : r_balance_keylen;
	    nbytes_used  = (btree->nodesize - n->insert_ptr) + n->nkeys*sizeof(node_vkey_t);
	    //  This allows for conversion of the rightmost 
	    //  pointer to a normal key, using the anchor key value.
	    nbytes_used  += max_balance_keylen + sizeof(node_vkey_t);
	}
	if ((2*nbytes_used) < (btree->nodesize - sizeof(btree_raw_node_t))) {
	    ret = 1;
	} else {
	    ret = 0;
	}
    }
    return(ret);
}

static bool
is_minimal_leaf(btree_raw_t *btree, btree_raw_node_t *node)
{
	uint32_t bytes_used = 0;
	bool ret = false;

	if (node->logical_id == btree->rootid) {
		/*
		 * root and leaf
		 */
		if (!(node->flags & LEAF_NODE) && (node->nkeys == 0)) {
			ret = true;
		} else {
			ret = false;
		}
	} else {

		bytes_used = btree_leaf_used_space(btree, node);

		if ((2 * bytes_used) < (btree->nodesize_less_hdr)) {
		    ret = true;
		} else {
		    ret = false;
		}
	}
	return ret;
}

static int 
is_minimal(btree_raw_t *btree, btree_raw_node_t *n, uint32_t l_balance_keylen,
	   uint32_t r_balance_keylen)
{
	bool res = false;

	if (is_leaf(btree, n)) {
		res = is_minimal_leaf(btree, n);
	} else {
		res = is_minimal_non_leaf(btree, n, l_balance_keylen, r_balance_keylen);
	}
	return res;
}

/*
 * Given a set of keys and a reference key, find all keys in set less than
 * the reference key.
 */
static inline uint32_t 
get_keys_less_than(btree_raw_t *btree, char *key, uint32_t keylen,
	       btree_mput_obj_t *objs, uint32_t count)

{
	int i_start, i_end, i_center;
	int x;
	int i_largest = 0;
	uint32_t num = 0;

	i_start = 0;
	i_end   = count - 1;
	i_largest = -1;

	num = count;
	while (i_start <= i_end) {
		i_center = (i_start + i_end) / 2;

	        x = btree->cmp_cb(btree->cmp_cb_data, key, keylen,
				 objs[i_center].key, objs[i_center].key_len);
		if (x < 0) {
			/*
			 * We are smaller than i_center,
			 * So the last closest to our key 
			 * and largest seen is i_center so far.
			 */
			i_largest = i_center;
			i_end = i_center - 1;
		} else if (x > 0) {
			i_start = i_center + 1;
		} else {
			/*
			 * Got a match. Our btree stores the matching
			 * key on left node. So this keys is inclusive.
			 */
			i_largest = i_center + 1;
			break;
		}
	}


	if (i_largest >= 0 && i_largest <= (count - 1)) {
		num = i_largest; //count is index + 1
	}


#ifdef DEBUG_BUILD
	/*
	 * check that keys are sorted.
	 */
	int i = 0;
	for (i = 0; i < count - 1; i++) {
		if (btree->cmp_cb(btree->cmp_cb_data, objs[i].key, objs[i].key_len,
				  objs[i + 1].key, objs[i + 1].key_len) >= 0) {
			/*
			 * Found key out of place.
			 */
			assert(0);
		}
	}
#endif 

	assert(num);
	return num;
}

/*
 * Get number of keys that can be taken to child for mput
 * without voilating btree order.
 */
static inline uint32_t  
get_adjusted_num_objs(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey,
                      btree_mput_obj_t *objs, uint32_t count)
{
	key_stuff_info_t ks;
	int x;

	/* If we are the rightmost or leaf node, let all of
	 * keys in the list to pass through to next stage */
	if (count <= 1 || is_leaf(bt, n) || (nkey >= n->nkeys)) {
		return count;
	}

 	ks.key = tmp_key_buf;
 	(void) get_key_stuff_info2(bt, n, nkey, &ks); //get key_stuff_info

	/* This comparision eliminates binary search in cases when all
	 * keys goes to the same node */
	x = bt->cmp_cb(bt->cmp_cb_data, ks.key, ks.keylen,
	               objs[count - 1].key, objs[count - 1].key_len);
	if (x >= 0) {
		return count;
	}

	return (get_keys_less_than(bt, ks.key, ks.keylen, objs, count));
}

/*
 * Check if an update is allowed or not by calling mput callback with old and new data
 * for an object.
 */
bool
mput_update_allowed(btree_raw_t *bt, btree_raw_mem_node_t *mem_node,
	  	    char *key, uint32_t keylen, char *new_data, uint64_t new_datalen,
		    int index, bool key_exists)
{
	int ret = 0;
	bool res = false;
	char *old_data = NULL;
	uint64_t old_datalen = 0;
	key_info_t key_info = {0};

	if (key_exists) {
		/*
		 * It is an update for existing obj.
		 */
		res = btree_leaf_get_data_nth_key(bt, mem_node->pnode, 
						  index, &old_data, &old_datalen);
		assert(res == true);
	} else {
		old_data = NULL;
		old_datalen = 0;
	}

	ret = (*bt->mput_cmp_cb) (bt->mput_cmp_cb_data, 
				  key, keylen,
				  old_data, old_datalen,
				  new_data, new_datalen);

	if (ret == 1) {
		return true;
	} else {
		return false;
	}
}

static bool
is_leaf_key_active(btree_raw_t *bt, btree_raw_node_t *node, int index)
{
	key_stuff_info_t ks;

	ks.key = (char *) &tmp_key_buf;
	get_key_stuff_leaf2(bt, node, index, &ks);

	return (btree_snap_find_meta_index(bt, ks.seqno) == -1);
}

/*
 * Insert a set of kyes to a leaf node. It takes are of finiding proper position of
 * key within leaf and also check for space in leaf.
 * Caller must make sure that this is the correct leat to insert.
 * The caller must also take write lock on leaf.
 *
 * Returns BTREE_SUCCESS if all keys inserted without a problem.
 * If any error while inserting, it will provide number of keys successfully 
 * inserted in 'objs_written' with an error code as return value.
 */
static bool
btree_leaf_insert_low(btree_raw_t *bt, btree_raw_mem_node_t *n, char *key, uint32_t keylen,
		      char *data, uint64_t datalen, uint64_t seqno, btree_metadata_t *meta,
		      uint64_t syndrome, int index, bool key_exists)
{
	bool res = false;
	uint64_t new_overflow_ptr = 0;
	key_info_t key_info = {0};
	uint64_t old_datalen = 0;
	uint64_t datalen_in_node = datalen;
	btree_status_t ret = BTREE_SUCCESS;
	int32_t bytes_saved = 0;
	int32_t size_increased = 0;

    dbg_print_key(key, keylen, "node: %p id %ld keylen: %d datalen: %ld index: %ld key_exists %ld", n, n->pnode->logical_id, keylen, datalen, index, key_exists);

	assert(is_leaf(bt, n->pnode));

	modify_l1cache_node(bt, n);

	/*
	 * Delete the older overflow area if key already exists.
	 */
	if (key_exists) {
		key_info.key = tmp_key_buf;
		res = btree_leaf_get_nth_key_info2(bt, n->pnode, index, &key_info);
		if (res == false) {
			assert(0);
			return false;
		}

		if ((key_info.keylen + key_info.datalen) 
				>= bt->big_object_size) {
			old_datalen = 0;
			delete_overflow_data(&ret, bt, key_info.ptr, key_info.datalen);
			assert(ret == BTREE_SUCCESS);
		} else {
			old_datalen = key_info.datalen;	
		}
	}

	/*
	 * Allocate new overflow area if required.
	 */
	if ((keylen + datalen) >= bt->big_object_size) {
		assert(datalen > 0);
		new_overflow_ptr = allocate_overflow_data(bt, datalen, data);
	}

	/*
	 * Finally insert the key.
	 */	
	assert(index >= 0);

	res = btree_leaf_update_key(bt, n->pnode, key, keylen,
				    data, datalen_in_node, seqno, new_overflow_ptr,
				    meta, syndrome, index, key_exists,
				    &bytes_saved, &size_increased);
#if DEBUG_BUILD
	assert(res == true);
	assert(btree_leaf_find_key2(bt, n->pnode, key, keylen, &index));
#endif 

	__sync_add_and_fetch(&(bt->stats.stat[BTSTAT_SPCOPT_BYTES_SAVED]), bytes_saved);

	if (res == true) {
#if DEBUG_BUILD
//		assert(size_increased > 0 || bt->stats.stat[BTSTAT_LEAF_BYTES] > (-size_increased));
#endif
		__sync_add_and_fetch(&(bt->stats.stat[BTSTAT_LEAF_BYTES]), size_increased);
	}

	return res;
}

static bool
btree_leaf_update_low(btree_raw_t *bt, btree_raw_mem_node_t *n, char *key, uint32_t keylen,
		      char *data, uint64_t datalen, uint64_t seqno, btree_metadata_t *meta,
		      uint64_t syndrome)
{
	int index = -1;
	bool res = false;

	res = btree_leaf_find_key(bt, n->pnode, key, keylen, meta,
				  syndrome, &index);
	assert(res == true);

	res = btree_leaf_insert_low(bt, n, key, keylen, data, datalen, seqno,
				    meta, syndrome, index, true);

	return res;
}

/*
 * Given a leaf node and set of keys, insert keys in to leaf node at appropriate positions.
 */
static btree_status_t
btree_insert_keys_leaf(btree_raw_t *btree, btree_metadata_t *meta, uint64_t syndrome, 
		       btree_raw_mem_node_t *mem_node, int flags,
		       btree_mput_obj_t *objs, uint32_t count, int32_t index, bool key_exists,
                       bool is_update, uint32_t *objs_written, uint64_t* last_seqno, uint32_t *new_inserts)
{
	btree_status_t ret = BTREE_SUCCESS;
	btree_status_t ret1 = BTREE_SUCCESS;
	uint64_t child_id, child_id_before, child_id_after;
	int32_t  nkey_child;
	uint32_t written = 0, idx = (flags & W_DESC) ? count - 1 : 0;
	uint64_t seqno = -1;
	bool res = false;
        *new_inserts = 0;

#ifdef PSTATS_1
        static uint64_t total_count = 0;
#endif

        key_meta_t key_meta;

	/* Explanation for 2 variables:
	 * key_exists: Used from appln perspective, if key existed. This will be used
	 *             to determine, if we should veto a create, pass on the existing
	 *             data to appln for the mput_update_allowed callback etc.
	 *
	 * is_update: Internal purpose to determine, if we need to consider it is as an
	 *            update for calculating node full, replacing or inserting new obj etc..
	 */

	while (!ret && written < count) {

		dbg_print_key(objs[idx].key, objs[idx].key_len, "index=%d written=%d cont=%d idx=%d", index, written, count, idx);
		key_meta.datalen = 0;
		/*
		 * Find position for next key in this node, if the index is not known already.
		 */
		if (index == -1) {
			if(!(flags & W_APPEND))
				key_exists = btree_leaf_find_key(btree, mem_node->pnode, objs[idx].key,
						         objs[idx].key_len, meta, syndrome, &index);
			else
				index = mem_node->pnode->nkeys;

			if (key_exists) {
				is_update = true;
				/* If we need to put a tombstone over existing key
				 * or if we the key is part of a snapshot, need to do insert */
				if ((meta->flags & INSERT_TOMBSTONE) || 
				    !(is_leaf_key_active(btree, mem_node->pnode, index)))
					is_update = false;
			} else {
				is_update = false;
			}

	 		if (is_node_full(btree, mem_node->pnode, objs[idx].key,
			                 objs[idx].key_len, objs[idx].data_len,
			                 meta, syndrome, is_update, index)) {
				//assert(written > 1);
				break;
			}
		}

		/* Key_exists, but we cannot update, because of snapshots, collect 
		 * metadata to update stats */
		if (key_exists && !is_update) {
			btree_leaf_get_meta(mem_node->pnode, index, &key_meta);
		}

		if (((flags & W_UPDATE) && !key_exists) ||
		    ((flags & W_CREATE) && key_exists)) {
			/*
			 * key not found for an update! or key was found for an insert!
			 */
			ret = BTREE_KEY_NOT_FOUND;

			/*
			 * We dont try if any one failed.
			 */
			break;
		}

		/*
		 * Check if an update is allowed or required.
		 * However, do not check with callback for the following
		 *  1. If we are inserting a tombstone - since its an internal operation
		 *  2. If we are updating a tombstone.
		 */
		if ((flags & W_APPEND) || (meta->flags & INSERT_TOMBSTONE) ||
		    (btree_leaf_is_key_tombstoned(btree, mem_node->pnode, index)) ||
		     mput_update_allowed(btree, mem_node, objs[idx].key, objs[written].key_len,
					objs[idx].data, objs[idx].data_len, index, 
					key_exists) == true) {
				
			if (meta->flags & UPDATE_USE_SEQNO) {
				seqno = meta->seqno;
			} else {
				seqno = btree->seqno_alloc_cb();
			}

			if (seqno == -1) {
				ret = BTREE_FAILURE;
				break;
			}
			
			res = btree_leaf_insert_low(btree, mem_node, objs[idx].key,
						    objs[idx].key_len, objs[idx].data,
						    objs[idx].data_len, seqno,
						    meta, syndrome, index, is_update);

			assert(res == true);
                        if (!key_exists) {
                            __sync_add_and_fetch(&(btree->stats.stat[BTSTAT_NUM_OBJS]), 1);
                            /*
                             * If it is new inserts, adjust the counter.
                             */
                            (*new_inserts)++;
#ifdef PSTATS_1
                            fprintf(stderr, "total=%ld BTSTAT_NUM_OBJS=%ld, new_inserts=%d\n", total_count++, btree->stats.stat[BTSTAT_NUM_OBJS], *new_inserts);
#endif
                        } else if (!is_update) {
				/* Key_exists, but we cannot update, because of snapshots */
				__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_NUM_SNAP_OBJS]), 1);
				__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_SNAP_DATA_SIZE]), key_meta.datalen);
				set_node_snapobjs_pstats(btree, mem_node->pnode, 1, key_meta.datalen, true);

				if (meta->flags & INSERT_TOMBSTONE) {
					__sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_NUM_OBJS]), 1);
					set_node_pstats(btree, mem_node->pnode, 1, false);
				}
			}
		}

		// TBD: need to handle the return code

		written++;
		idx += (flags & W_DESC) ? -1 : 1;

		btree->log_cb(&ret, btree->log_cb_data, BTREE_UPDATE_NODE, btree, mem_node);

		/* Next Iteration onwards, it has to do the search */
		index = -1; key_exists = false; is_update = false;
	}

	if(last_seqno) *last_seqno = seqno;

	*objs_written = written;
//	assert(res == true);
	return ret;
}

static inline
int split_root(btree_raw_t* btree, btree_raw_mem_node_t** parent_out, int* parent_write_locked,
		int* parent_nkey_child, uint16_t level)
{
	btree_status_t ret = BTREE_SUCCESS;

	btree_raw_mem_node_t* parent = get_new_node(&ret, btree, 0, level + 1);

	if(!parent)
		return 0;

	node_lock(parent, WRITE);
	*parent_write_locked = 1;

	dbg_print("root split %ld new root %ld\n",
			btree->rootid, parent->pnode->logical_id);

	parent->pnode->rightmost = btree->rootid;
	*parent_nkey_child = parent->pnode->nkeys;

	uint64_t saverootid = btree->rootid;
	btree->rootid = parent->pnode->logical_id;
	if (BTREE_SUCCESS != savepersistent( btree, 0)) {
		assert(0);
		btree->rootid = saverootid;
		return 0;
	}

	*parent_out = parent;

	return 1;
}

btree_status_t
btree_raw_bulk_insert(struct btree_raw *btree, btree_mput_obj_t **objs_in_out, uint32_t count,
		uint32_t write_type, btree_metadata_t *meta, btree_raw_mem_node_t* parent,
		btree_raw_mem_node_t* left, int parent_nkey_child, uint32_t* not_written)
{
	btree_status_t ret = BTREE_SUCCESS;
	bool free_key = false;
	int x = 0;
	int32_t nkey_child, split_key;
	uint32_t j, i = 0, written = 0, r_keylen, left_minimal;
	uint64_t child_id_before, child_id_after, child_id, r_seqno, r_syndrome;
	char* r_key;
	node_key_t *pk_insert_not_used;
	btree_mput_obj_t* objs = *objs_in_out;
	btree_raw_mem_node_t *right, *node;
	key_stuff_info_t ksi;
        uint32_t new_inserts;

	stats_inc(btree, BTSTAT_BULK_INSERT_CNT, 1);

	int found = find_key(btree, left->pnode, objs->key, objs->key_len,
			meta, 0, &child_id, &child_id_before, &child_id_after,
			&split_key);

	assert(!found || index > 0);

	dbg_print_key(objs[0].key, objs[0].key_len, "bulk_insert count=%d parent_id=%ld left=%ld split_key=%d locked=%lld\n", count, parent->pnode->logical_id, left->pnode->logical_id, split_key, locked);

	*not_written = count;

	/* Split the node by the key where first object of the batch would be inserted */
	if(!(right = btree_split_child(&ret, btree, parent,
			left, meta, 0, parent_nkey_child, split_key)))
		return BTREE_FAILURE;

	node_lock(right, WRITE);

	/* Fill right node */
	if(btree_insert_keys_leaf(btree, meta, 0, right, write_type | W_DESC,
				objs, count, -1, false, false, &written, NULL, &new_inserts)) {
#ifdef PSTATS_1
        fprintf(stderr, "btree_raw_bulk_insert:right : new_inserts=%d\n", new_inserts);
#endif
		return BTREE_FAILURE;
        }

	count -= written;

        if(count) {
            /* Check if last key from the remaning batch should still
               go to the right node. */
            ksi.key = tmp_key_buf;
            (void) get_key_stuff_info2(btree, right->pnode, 0, &ksi);
            x = btree->cmp_cb(btree->cmp_cb_data, objs[count-1].key, objs[count-1].key_len,
                    ksi.key, ksi.keylen);

            if(x < 0) {
                /* Fill left node */
                if(btree_insert_keys_leaf(btree, meta, 0, left, W_CREATE | W_ASC | W_APPEND,
                            objs, count, -1, false, false, &written, NULL, &new_inserts)) {
#ifdef PSTATS_1
                    fprintf(stderr, "btree_raw_bulk_insert:left : new_inserts=%d\n", new_inserts);
#endif
                    return BTREE_FAILURE;
                }

                objs += written;
                count -= written;
            }
        }

	/* Find the last key of the left node */
	ksi.key = tmp_key_buf;
	(void) get_key_stuff_info2(btree, left->pnode, left->pnode->nkeys - 1, &ksi);
	r_key = ksi.key;
	r_keylen = ksi.keylen;
	r_seqno = ksi.seqno;

	if(((left_minimal = is_minimal(btree, left->pnode, 0, 0)) ||
				is_minimal(btree, right->pnode, 0, 0))) {
		assert(!count || x >= 0);
		(void) equalize_keys(btree, parent, left, right, r_key, r_keylen, 0,
				r_seqno, &r_key, &r_keylen, &r_syndrome, &r_seqno, left_minimal
				? LEFT : RIGHT, &free_key);
	}

	/* Put anchor key of the left node to the parent */
	insert_key(&ret, btree, parent, r_key, r_keylen, r_seqno,
			sizeof(uint64_t), (char *) &(left->pnode->logical_id), 0);

	if(free_key)
		free_buffer(btree, r_key);

	if(!count || x >= 0) {
		*not_written = count;
		*objs_in_out = objs;
		return BTREE_SUCCESS;
	}

	/* Fill the node and insert an anchor for as many nodes as possible */
	while(count && (node = get_new_node(&ret, btree, LEAF_NODE, 0)))
	{
		node_lock(node, WRITE);

		if(btree_insert_keys_leaf(btree, meta, 0, node, W_CREATE | W_ASC | W_APPEND,
				objs, count, -1, false, false, &written, &r_seqno, &new_inserts)) {
#ifdef PSTATS_1
                    fprintf(stderr, "btree_raw_bulk_insert:anchor : new_inserts=%d\n", new_inserts);
#endif
			break;
                }

		assert(count || !is_minimal(btree, node->pnode, 0, 0));
		assert(written > 0);
		assert(r_seqno >=0 );

		if(is_minimal(btree, node->pnode, 0, 0) || is_node_full(btree,
				parent->pnode, objs[written - 1].key, objs[written - 1].key_len,
				sizeof(uint64_t), meta, 0, false, -1))
			break;

		objs += written - 1;

		insert_key(&ret, btree, parent, objs->key, objs->key_len, r_seqno,
				sizeof(uint64_t), (char *) &(node->pnode->logical_id), 0);

		count -= written;
		objs++;

		stats_inc(btree, BTSTAT_BULK_INSERT_FULL_NODES_CNT, 1);
	}

	/* Last node is minimal or parent is full. Delete last node */
	if(count && node)
	{
		node_unlock(node);
		mark_node_clean(node);
		deref_l1cache_node(btree, node);
		deref_l1cache_node(btree, node);
		referenced_nodes_count--;
		modified_nodes_count--;
		stats_dec(btree, BTSTAT_NUM_OBJS, written);
	}

	*not_written = count;
	*objs_in_out = objs;

	dbg_print("not_written=%d objs=%p locked=%lld\n", *not_written, objs, locked);

	return BTREE_SUCCESS;
}


static inline
int relock_parent_write(btree_raw_t* btree, btree_raw_mem_node_t** parent_out, int* parent_write_locked,
		int* parent_nkey_child)
{
	btree_raw_mem_node_t *parent = *parent_out;
	uint64_t save_modified = parent->modified;

	dbg_print("enter parent_id=%ld\n", parent->pnode->logical_id);

	/*
	 * Promote lock of parent from read to write.
	 * parent->modified state used to check if anything
	 * changed during that promotion.
	 */
	assert(parent);
	node_unlock(parent);
	node_lock(parent, WRITE);

	if(parent->modified != save_modified) {
		__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_PUT_RESTART_CNT]), 1);
		node_unlock(parent);
		*parent_out = NULL;
		*parent_nkey_child = -1;

		if (BTREE_SUCCESS != deref_l1cache(btree)) {
			assert(0);
		}

		/*
		 * Parent got changed while we tried to promote lock to write.
		 * restart from root with hope to get write lock next try.
		 */
		return 0;
	}

	dbg_print("success parent_id=%ld\n", parent->pnode->logical_id);

	*parent_write_locked = 1;
	return 1;
}

static inline
bool
get_child_id_and_count(btree_raw_t* btree, btree_raw_node_t* pnode,
		uint64_t* child_id, btree_mput_obj_t* objs, uint32_t* num_objs,
		btree_metadata_t *meta, uint64_t syndrome, int32_t* nkey_child)
{
	uint64_t child_id_before, child_id_after;

	/*
	 * Just split the node, so start again from parent.
	 */
	int key_found = find_key(btree, pnode, objs[0].key, objs[0].key_len,
			meta, syndrome, child_id, &child_id_before, &child_id_after,
			nkey_child);

	/*
	 * Get the set of keys less than child_id.
	 */
	*num_objs= get_adjusted_num_objs(btree, pnode, 
			*nkey_child,
			objs, *num_objs);

	return key_found;
}


/*
 * Main routine for inserting one or more keys in to a btree. 
 *
 * Single Mput algo:
 * It try to find the leaf where input key can be inserted. While doing the
 * search, it also splits any node in path that is full. Once leaf is found, the
 * key is inserted in to it.
 *
 * MPut algo: 
 * This rouine takes first keys in input set as reference and find the leaf where
 * this key can be inserted. As it goes towards the leaf in tree, it will keep on trimming
 * the input key list to what is actually applicable to the subtree where serach is heading.
 * Once it has that leaf, it will insert all keys that could
 * possibly fit to that leaf.
 */
//#define COLLECT_TIME_STATS
#undef COLLECT_TIME_STATS
uint64_t bt_mwrite_total_time = 0;

btree_status_t 
btree_raw_mwrite_low(btree_raw_t *btree, btree_mput_obj_t **objs_in_out, uint32_t* num_objs,
		     btree_metadata_t *meta, uint64_t syndrome, int write_type,
		     int* pathcnt, uint32_t *written)
{

	int               split_pending = 0, parent_write_locked = 0;
	int32_t           nkey_child, parent_nkey_child = -1;
	uint64_t          child_id;
	btree_status_t    ret = BTREE_SUCCESS;
	btree_status_t    txnret = BTREE_SUCCESS;
	btree_status_t    ret1 = BTREE_SUCCESS;
	btree_raw_node_t *node = NULL;
	btree_raw_mem_node_t *mem_node = NULL, *parent = NULL;
	bool key_found = false;
	bool is_update = false;
	btree_mput_obj_t* objs=*objs_in_out;
	uint32_t count = *num_objs;

        uint32_t new_inserts = 0;

	*written = 0;

	assert(!locked);

	dbg_print("write_type=%d num_objs %d key_len=%d\n", write_type, *num_objs, objs[0].key_len);

#ifdef COLLECT_TIME_STATS 
	uint64_t start_time = 0;
	start_time = get_tod_usecs();
#endif

	assert(referenced_nodes_count == 0);
	plat_rwlock_rdlock(&btree->lock);
	assert(referenced_nodes_count == 0);

restart:
	child_id = btree->rootid;

	while(1) {
		if(!(mem_node = get_existing_node_low(&ret, btree, child_id, 1, false, false)))
			goto err_exit;

		node = mem_node->pnode;

mini_restart:
		(*pathcnt)++;

		node_lock(mem_node, is_leaf(btree, node) || split_pending ||
				(count > 1 && node->level == 1));

		//if(!parent && mem_node->pnode->logical_id != btree->rootid) {
		if(!parent && child_id != btree->rootid) {
			/*
			 * While we reach here it is possible that root got changed.
			 */
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_PUT_RESTART_CNT]), 1);
			dbg_print("rootid_changed child_id=%ld rootid=%ld locked=%lld\n", child_id, btree->rootid, locked);
			node_unlock(mem_node);

			if (BTREE_SUCCESS != deref_l1cache(btree)) {
				assert(0);
			}
			goto restart;
		}

		/* In case of leaf node, before finding where this new key has to go
		 * in, scavenge the node to see if any duplicate entries can be removed.
		 * This can help more keys to insert in the node */
		if (is_leaf(btree, node)) {
			scavenge_node(btree, mem_node, NULL, NULL);

			/* TODO: Do we need to set modify_l1cache_node. It will be set
			 * when we insert the object. But in erronoues case it is not
			 * set, is it better not to write the scavenged node as well */
		}

		key_found = get_child_id_and_count(btree, node, &child_id, objs, &count,
				meta, syndrome, &nkey_child);

		if ((is_leaf(btree, node)) && key_found) {
			is_update = true;
			if ((meta->flags & INSERT_TOMBSTONE) || 
			    !(is_leaf_key_active(btree, node, nkey_child)))
				is_update = false;
		} else {
			is_update = key_found;
		}

		if (!is_node_full(btree, node, objs[0].key, objs[0].key_len,
				   objs[0].data_len, meta, syndrome, is_update, nkey_child)) {

			if(parent && (!parent_write_locked || (!is_node_dirty(parent) &&
							!(count > 1 && parent->pnode->level == 1)))) {
				node_unlock(parent);
			}

			if(child_id == BAD_CHILD) break;

			parent = mem_node;
			parent_nkey_child = nkey_child;
			parent_write_locked = is_leaf(btree, node) || split_pending ||
				(count > 1 && node->level == 1);
			split_pending = 0;
			continue;
		}

		/* Found a full node on the way, split it first. */
		if(!split_pending && (!is_leaf(btree, node) ||
			    (parent && !parent_write_locked))) {

			node_unlock(mem_node);

			if(parent && !relock_parent_write(btree, &parent, &parent_write_locked,
						&parent_nkey_child))
				goto restart; /* Parent has changed */

			split_pending = 1;

			goto mini_restart; /* Relock current node write */
		}

		if(is_root(btree, node) && !split_root(btree, &parent, &parent_write_locked,
		 		&parent_nkey_child, node->level))
			goto err_exit;

		btree_raw_mem_node_t *new_node = btree_split_child(&ret, btree, parent,
		                                                   mem_node, meta,
		                                                   syndrome,
		                                                   parent_nkey_child, 0);
		if(BTREE_SUCCESS != ret)
			goto err_exit;

		node_lock(new_node, WRITE);

		key_found = get_child_id_and_count(btree, parent->pnode, &child_id, objs, &count,
				meta, syndrome, &nkey_child);

		assert(child_id != BAD_CHILD);

		if(mem_node->pnode->logical_id != child_id) {
			mem_node = new_node; /* The key is in the new node */
			parent_nkey_child++;
  		}

		key_found = get_child_id_and_count(btree, mem_node->pnode, &child_id, objs, &count,
				meta, syndrome, &nkey_child);

		if(child_id == BAD_CHILD) break;

		parent = mem_node;

		(*pathcnt)++;

		parent_nkey_child = nkey_child;
		split_pending = 0;
	}

	dbg_print("before modifiing leaf node id %ld is_leaf: %d is_root: %d is_over: %d\n",
		  node->logical_id, is_leaf(btree, node), is_root(btree, node), is_overflow(btree, node));

	if (key_found) {
		key_found = !btree_leaf_is_key_tombstoned(btree, mem_node->pnode, nkey_child);
	}

	assert(is_leaf(btree, node));
        plat_rwlock_unlock(&btree->lock);

        new_inserts = 0;
        ret = btree_insert_keys_leaf(btree, meta, syndrome, mem_node, write_type | W_ASC,
                objs, count, nkey_child, key_found, is_update, written, NULL, &new_inserts);

        objs += *written;
        count -= *written;

#ifdef PSTATS_1
        fprintf(stderr, "btree_raw_mwrite_low: delta obj = %d\n", new_inserts);
#endif
        set_node_pstats(btree, mem_node->pnode, new_inserts, true);

        if(count > 1 && parent && *written) {
            uint32_t not_written;
            assert(parent->pnode->logical_id != mem_node->pnode->logical_id);
            ret = btree_raw_bulk_insert(btree, &objs, count, write_type,
                    meta, parent, mem_node, parent_nkey_child, &not_written);
		*written += count - not_written;
		count = not_written;
		dbg_print("bulk_insert returned error: %d written=%d not_written=%d num_objs=%d count=%d\n", ret, *written, not_written, *num_objs, count);
	}

	if(count + *written > 1 && parent && !is_node_dirty(parent))
		node_unlock(parent);

	*objs_in_out = objs;
	*num_objs = count;

	dbg_print("objs=%p num_objs=%d count=%d written=%d ret=%d\n", objs, *num_objs, count, *written, ret);

	/*
	 * If node is unchanged, no point of keeping it locked.
	 */
	if (!is_node_dirty(mem_node))
		node_unlock(mem_node);

	/*
	 * the deref_l1cache will release the references and lock of modified nodes.
	 */
	ret1 = deref_l1cache(btree);
	if (BTREE_SUCCESS != ret1) {
		ret = ret1;
	}

	/*
	 * Writes are flushed, decrement pending write count.
	 */
	__sync_sub_and_fetch(&btree->active_writes[_pstats_ckpt_index], 1);
//	assert(btree->active_writes[_pstats_ckpt_index] == 0);

	if(locked)
		fprintf(stderr, "%x locked=%lld modified_nodes_count=%ld\n", (int)pthread_self(), locked, modified_nodes_count);
	assert(!locked);

	if (*written > 1) {
	    __sync_add_and_fetch(&(btree->stats.stat[BTSTAT_MPUT_IO_SAVED]), *written - 1);
	}

#ifdef COLLECT_TIME_STATS 
	__sync_add_and_fetch(&bt_mwrite_total_time, get_tod_usecs() - start_time);
#endif

    dbg_print("after write_type=%d ret=%d lic=%ld\n", write_type, ret, btree->logical_id_counter);

	assert(referenced_nodes_count == 0);
	return ret;

err_exit:

	plat_rwlock_unlock(&btree->lock);
	assert(referenced_nodes_count == 0);
	return BTREE_FAILURE;
}

/*
 * return 0 if key falls in range
 * returns -1 if range_key is less than key.
 * returns +1 if range_key is greater than key.
 */
static int 
btree_key_in_range(void *args, void *range_args,
		   char *range_key, uint32_t range_key_len,
		   char *key, uint32_t keylen)
{
	int x = 0;

	if (keylen < range_key_len) {
		return 1;
	}
	x = memcmp(range_key, key, range_key_len);

	return x;
}

static inline bool
find_first_key_in_range_non_leaf(btree_raw_t *bt,
				btree_raw_node_t *n,
				char *range_key,
				uint32_t range_key_len,
				key_stuff_t *ks,
				btree_range_cmp_cb_t range_cmp_cb, 
				void *range_cmp_cb_args,
				int *index)
{
	int i_start, i_end, i_center;
	int i_last_match = -1;
	key_stuff_t ks_last_match = {0};
	key_stuff_t ks_tmp;
	int x = 0;

	if (index) {
		(*index) = -1;
	}

	i_start = 0;
	i_end = n->nkeys - 1;

	while (i_start <= i_end) {
		i_center = (i_start + i_end) / 2;

		(void) get_key_stuff(bt, n, i_center, &ks_tmp);
		x = (*range_cmp_cb) (bt->cmp_cb_data, range_cmp_cb_args,
				     range_key, range_key_len,
				     ks_tmp.pkey_val, ks_tmp.keylen);
					 
		if (x <= 0) {
			/*
			 * Got first greater than or equal to range_key.
			 */	
			i_last_match = i_center;
			i_end = i_center - 1;
			ks_last_match = ks_tmp;

		} else if (x > 0) {
			i_start = i_center + 1;	
		}
	}

	if (i_last_match >= 0 && i_last_match <= (n->nkeys - 1)) {
		*ks = ks_last_match;
		(*index) = i_last_match;
		assert(!ks->fixed); //Not supprted for fixed keys yet
		return true;
	} else if (!is_leaf(bt, n)) {
		/*
		 * Range is greater than the last key in non leaf node.
		 * Only path we can follow is the rightmost child.
		 */
		ks->ptr = n->rightmost;
		(*index) = n->nkeys - 1;
		assert(i_start > (n->nkeys - 1));
		return true;
	}

	return false;
}

/*
 * Caller must free the key_out after use.
 */
static inline bool 
find_first_key_in_range_leaf(btree_raw_t *bt, btree_raw_node_t *n,
			char *range_key, uint32_t range_key_len, 
			char **key_out, uint32_t *keyout_len, 
			btree_range_cmp_cb_t range_cmp_cb, 
			void *range_cmp_cb_args, int *index, uint64_t *seqno)
{
	int32_t num_keys;
	int i = 0;
	char *tmp_key = NULL;
	uint32_t tmp_key_len = 0;
	int x = 0;
	bool res = false;
	bool res1 = false;
	bool in_snap;

	*key_out = NULL;
	*keyout_len = 0;

	num_keys = btree_leaf_num_entries(bt, n);

	for (i = 0; i < num_keys; i++) {
		res1 = btree_leaf_get_nth_key(bt, n, i, 
					     &tmp_key, &tmp_key_len, seqno);
		assert(res1 == true);
		x = (*range_cmp_cb) (bt->cmp_cb_data, range_cmp_cb_args,
					 range_key, range_key_len,
					 tmp_key, tmp_key_len);
		if (x == 0) {
			*key_out = tmp_key;
			*keyout_len = tmp_key_len;	
			*index = i;
			res = true;
			break;
		}

		free_buffer(bt, tmp_key);
	}

	return res;
}

/*
 * Caller must free the ks->pkey_val after use.
 */
static inline bool
find_first_key_in_range(btree_raw_t *bt,
			btree_raw_node_t *n,
			char *range_key,
			uint32_t range_key_len,
			key_stuff_t *ks,
			btree_range_cmp_cb_t range_cmp_cb, 
			void *range_cmp_cb_args,
			int *index, bool *free_key_mem)
{

	bool res = false;
	if (is_leaf(bt, n)) {
		char *key_out = NULL;
		uint32_t key_out_len = 0;
		uint64_t seqno;
		res = find_first_key_in_range_leaf(bt, n, range_key, range_key_len,
						   &key_out, &key_out_len, 
						   range_cmp_cb, range_cmp_cb_args,
						   index, &seqno);
		if (res == true) {
			assert(key_out_len != 0);
			ks->pkey_val = key_out;
			ks->keylen = key_out_len;
			ks->seqno = seqno;
			*free_key_mem = true;
		} else {
			ks->pkey_val = NULL;
			ks->keylen = 0;
		}
		ks->ptr = BAD_CHILD;
	} else {
		res = find_first_key_in_range_non_leaf(bt, n, range_key, range_key_len,
						       ks, range_cmp_cb, range_cmp_cb_args,
						       index);
		*free_key_mem = false;
	}

	return res;
}


/*
 * Search the new key that applies to the given range.
 *
 * If Marker is set to NULL, this is the first key search, so search
 * first key that falls in the range. If marker is not null, search the next
 * key that falls in the range according to marker.
 *
 * Returns: if non-leaf node, the child_id is set to child that we must
 * 	    traverse to find the next range key to update.
 *	    If leaf node, the ks is set to next key that falls in range.
 */
static bool
find_next_rupdate_key(btree_raw_t *bt, btree_metadata_t *meta, btree_raw_node_t *n, char *range_key,
		      uint32_t range_key_len, char **key_out, uint32_t *key_out_len,
		      uint64_t *child_id, btree_range_cmp_cb_t range_cmp_cb, 
		      void *range_cmp_cb_args, btree_rupdate_marker_t **marker,
		      bool *free_key_mem, bool *in_snap)
{
	key_stuff_t ks = {0,};
	bool res = false;
	int index = -1;

	*child_id = BAD_CHILD;
	*free_key_mem = false;

	if ((*marker)->set) {
		key_stuff_info_t ksi;
		/*
		 * Get next key from the marker.
		 */
		index = bsearch_key_low(bt, n, (*marker)->last_key, (*marker)->last_key_len,
				meta, 0, -1, n->nkeys, BSF_NEXT, &res);

		res = index < n->nkeys;

		ksi.key = tmp_key_buf;

		if(res)
		{
			if (is_leaf(bt, n)) {
				(void) get_key_stuff_info(bt, n, index, &ksi);
				*free_key_mem = true;
			}
			else
				(void) get_key_stuff_info2(bt, n, index, &ksi);
		}

		ks.pkey_val = ksi.key;
		ks.keylen = ksi.keylen;
		ks.ptr = ksi.ptr;

		assert(res == false || bt->cmp_cb(bt->cmp_cb_data, ks.pkey_val, ks.keylen,
					     (*marker)->last_key, (*marker)->last_key_len) > 0);

		/*
		 * Search end at end of the node, consider righmost key as well
		 */
		if ((res == false) && !is_leaf(bt, n)) {
			ks.ptr = n->rightmost;
			res = true;
		}
	} else {
		/*
		 * First key in the range.
		 */
		res = find_first_key_in_range(bt, n, range_key,
					      range_key_len, &ks,
					      range_cmp_cb, range_cmp_cb_args,
					      &index, free_key_mem);
	}

	if (res == true) {
		/*
		 * Init or update the marker.
		 * Marker is across calls, need to do a deep copy.
		 */
		if (is_leaf(bt, n)) {
			assert(in_snap);
			/*
			 * Marker get updated only for leaf nodes.
			 */

			if ((*range_cmp_cb)(bt->cmp_cb_data, range_cmp_cb_args, 
					    range_key, range_key_len,
					    ks.pkey_val, ks.keylen) != 0) {
				/*
				 * Got a key out of range key in leaf.
				 */
				(*marker)->set = false;
				res = false;
			} else {
				/* Check whether the key is removed in active container */

				memcpy((*marker)->last_key, ks.pkey_val, ks.keylen);
				(*marker)->last_key[ks.keylen] = 0;
				(*marker)->last_key_len = ks.keylen;
				(*marker)->index = index;
				(*marker)->skip = btree_leaf_is_key_tombstoned(bt, n, index);
				(*marker)->set = true;

				*in_snap = btree_snap_seqno_in_snap(bt, ks.seqno);
			}
			assert(*free_key_mem == true);
		}

		/*
		 * Set the child id as well.
		 */
		*child_id = ks.ptr;
	}

	*key_out = ks.pkey_val;
	*key_out_len = ks.keylen;

	assert(is_leaf(bt, n) || *free_key_mem == false);

	return res;
}

/*
 * Given a range and a leaf node, update all keys falling in that range.
 * Caller must hold lock and ref on this leaf.
 */
static btree_status_t
btree_rupdate_raw_leaf(
		struct btree_raw *btree, 
		btree_raw_mem_node_t *node,
	        char *range_key,
	        uint32_t range_key_len,
		btree_metadata_t *meta,
	        btree_rupdate_cb_t callback_func,
	        void * callback_args,	
		btree_range_cmp_cb_t range_cmp_cb,
		void *range_cmp_cb_args,
	        uint32_t *objs_updated,
	        btree_rupdate_marker_t **marker)
{
	btree_status_t ret1 = BTREE_SUCCESS, ret2 = BTREE_SUCCESS;
	btree_rupdate_cb_t cb_func = 
			(btree_rupdate_cb_t) callback_func;
	char *new_data = NULL;
	uint64_t datalen = 0;
	uint64_t new_data_len = 0;
	int count = 0;
	uint32_t objs_done = 0;
	char **bufs = NULL;
	int i = 0;
	bool no_modify = true;
	uint64_t child_id = BAD_CHILD;
	uint64_t seqno = meta->start_seqno;
	char *key_out = NULL;
	uint32_t key_out_len = 0;
	bool key_allocated = false;
	bool res = false;
	bool in_snap = false;
	int	index;
	key_meta_t key_meta;

	assert(is_leaf(btree, node->pnode));

	(*objs_updated) = 0;

	bufs = (char **) malloc(sizeof(char *) * 
				btree_leaf_num_entries(btree, node->pnode));
	if (bufs == NULL) {
		ret1 = BTREE_FAILURE;
		goto exit;
	}
	
	while (find_next_rupdate_key(btree, meta, node->pnode, range_key,
				      range_key_len, &key_out, &key_out_len,
				      &child_id, range_cmp_cb, range_cmp_cb_args,
				      marker, &key_allocated, &in_snap) == true) {

		assert(key_allocated == true);

		/* If we find tombstoned object, skip */
		if ((*marker)->set && (*marker)->skip) {
			fprintf(stderr, "SKIPPING tombstoned key: %s\n", key_out);
			free_buffer(btree, key_out);
			key_out = NULL;
			key_out_len = 0;
			bufs[count] = NULL;
			count++;
			continue;
		}

		ret1 = get_leaf_data(btree, node->pnode, key_out, key_out_len,
				     meta, 0, &bufs[count], &datalen, &index, 0);

		if (ret1 != BTREE_SUCCESS) {
			goto done;
		}

		new_data_len = 0;
		new_data = NULL;

		if (cb_func != NULL) {
			if ((*cb_func) (key_out, key_out_len,
					bufs[count], datalen,
					callback_args, &new_data, &new_data_len) == false) {
				/*
				 * object data not changed, no need to update/create irrespective of
				 * whether its active copy or in snapshot.
				 */
				count++;
				free_buffer(btree, key_out);
				continue;
			}
		}

		if (new_data_len != 0) {
			/*
			 * The callback has set new data in new_data.
			 */
			free(bufs[count]);		
			bufs[count] = new_data;
			datalen = new_data_len;
		}

		res = btree_leaf_is_full(btree, node->pnode, key_out, key_out_len,
					 datalen, meta, 0, in_snap ? false : true);
		if (res == true) {
			/*
			 * Node does not have space for new data.
			 */
			ret1 = BTREE_RANGE_UPDATE_NEEDS_SPACE;

			/*
			 * Set this key and data in marker to retry single key update
			 * for this key.
			 */
			(*marker)->retry_key = key_out;
			(*marker)->retry_keylen = key_out_len;
				
			(*marker)->retry_data = bufs[count];
			(*marker)->retry_datalen = datalen;
			(*marker)->retry_update = !in_snap;
			goto done;
		}

		if (meta->flags & UPDATE_USE_SEQNO) {
			seqno = meta->seqno;
		} else {
			seqno = btree->seqno_alloc_cb();
		}
		
		if (seqno == -1) {
			ret1 = BTREE_FAILURE;
			break;
		}

		/*
		 * Update the key.
		 */
		if (in_snap) {
			res = btree_leaf_insert_low(btree, node, key_out, key_out_len,
							bufs[count], datalen, seqno, meta, 0, index, false);
			btree_leaf_get_meta(node->pnode, index, &key_meta);
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_NUM_SNAP_OBJS]), 1);
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_SNAP_DATA_SIZE]), key_meta.datalen);
			set_node_snapobjs_pstats(btree, node->pnode, 1, key_meta.datalen, true);
		} else {
			res = btree_leaf_update_low(btree, node, key_out, key_out_len,
							bufs[count], datalen, seqno, meta, 0);
		}
		assert(res == true);

		no_modify = false;

		free_buffer(btree, key_out);
		key_out = NULL;
		key_out_len = 0;

		count++;
		objs_done++;
	}

done:
	if (count == 0) {
		/*
		 * Got the end, set marker to invalid
		 */
		(*marker)->set = false;
	}

	/*
	 * Flush this node to disk.
	 */
	(*objs_updated) = objs_done;

exit:
	/*
	 * Could not modify anything in node,
	 * so release the lock explicitly.
	 */
	if (no_modify) {
		plat_rwlock_unlock(&node->lock);
	}

	/*
	 * the deref_l1cache will release the lock of modified nodes.
         * Also the references of looked up nodes.
	 */
	ret2 = deref_l1cache(btree);

	/*
	 * Free the temporary buffers.
	 */
	if (bufs) {
		for (i = 0; i < count; i++) {
			if (bufs[i]) {
				free(bufs[i]);
			}
		}
		free(bufs);
	}

	assert(referenced_nodes_count == 0);
	if (ret2 == BTREE_SUCCESS) {
		return(ret1);
	} else {
		return(ret2);
	}
}

static btree_status_t
btree_raw_rupdate_low(
		struct btree_raw *btree, 
		uint64_t node_id,
		btree_metadata_t *meta,
	        char *range_key,
	        uint32_t range_key_len,
	        btree_rupdate_cb_t callback_func,
	        void * callback_args,	
		btree_range_cmp_cb_t range_cmp_cb,
		void *range_cmp_cb_args,
	        uint32_t *objs_updated,
	        btree_rupdate_marker_t **marker,
		btree_raw_mem_node_t *parent);

static btree_status_t
btree_rupdate_raw_non_leaf(
		struct btree_raw *btree, 
		btree_raw_mem_node_t *mem_node,
	        char *range_key,
	        uint32_t range_key_len,
		btree_metadata_t *meta,
	        btree_rupdate_cb_t callback_func,
	        void * callback_args,	
		btree_range_cmp_cb_t range_cmp_cb,
		void *range_cmp_cb_args,
	        uint32_t *objs_updated,
	        btree_rupdate_marker_t **marker)
{
	key_stuff_t ks;
	uint64_t child_id = BAD_CHILD;
	btree_status_t ret = BTREE_SUCCESS;
	bool res = false;
	char *key_out = NULL;
	uint32_t key_out_len = 0;
	bool key_allocated = false;

	assert(!is_leaf(btree, mem_node->pnode));

	/*
	 * Not at leaf yet, keep on seraching down the tree.
	 */
	res = find_next_rupdate_key(btree, meta, mem_node->pnode, 
				    range_key, range_key_len, 
				    &key_out, &key_out_len,
				    &child_id, range_cmp_cb,
				    range_cmp_cb_args, marker,
				    &key_allocated, NULL);

	/*
	 * Allocated key is not returned for non-leaf nodes.
	 */
	assert(key_allocated == false);

	/*
	 * Search cannot end at non-leaf.
	 */
	assert(res);
	if (res == true) {
		ret = btree_raw_rupdate_low(btree, child_id, meta, 
					    range_key, range_key_len, callback_func,
					    callback_args, range_cmp_cb, range_cmp_cb_args,
					    objs_updated, marker, mem_node);
	}

	return ret;
}

/*
 * Do range update for a subtree starting with other than root node.
 */
static btree_status_t
btree_raw_rupdate_low(
		struct btree_raw *btree, 
		uint64_t node_id,
		btree_metadata_t *meta,
	        char *range_key,
	        uint32_t range_key_len,
	        btree_rupdate_cb_t callback_func,
	        void * callback_args,	
		btree_range_cmp_cb_t range_cmp_cb,
		void *range_cmp_cb_args,
	        uint32_t *objs_updated,
	        btree_rupdate_marker_t **marker,
		btree_raw_mem_node_t *parent)
{
	btree_raw_mem_node_t *mem_node = NULL;
	btree_status_t ret = BTREE_SUCCESS;

	mem_node = get_existing_node_low(&ret, btree, node_id, 1, false, false);
	if (ret != BTREE_SUCCESS) {
		node_unlock(parent);
		return ret;
	}

	/*
	 * Take write lock on leaf nodes and read on other nodes.
	 */
	node_lock(mem_node, is_leaf(btree, mem_node->pnode));

	node_unlock(parent);

	if (!is_leaf(btree, mem_node->pnode)) {
		/*
		 * Not at leaf yet, keep on seraching down the tree.
		 */
		ret = btree_rupdate_raw_non_leaf(btree, mem_node, range_key, range_key_len,
					  	 meta, callback_func, callback_args,
						 range_cmp_cb, range_cmp_cb_args, objs_updated,
						 marker);

	}  else {

		/*
		 * Found the leaf, update the keys in range.
		 */
		ret = btree_rupdate_raw_leaf(btree, mem_node, range_key, range_key_len,
					     meta, callback_func, callback_args,
					     range_cmp_cb, range_cmp_cb_args,
					     objs_updated, marker);	
	}

	return ret;
}

/*
 * Do range update for Btree tree.
 */
static btree_status_t
btree_raw_rupdate_low_root(
		struct btree_raw *btree, 
		btree_metadata_t *meta,
	        char *range_key,
	        uint32_t range_key_len,
	        btree_rupdate_cb_t callback_func,
	        void * callback_args,	
		btree_range_cmp_cb_t range_cmp_cb,
		void *range_cmp_cb_args,
	        uint32_t *objs_updated,
	        btree_rupdate_marker_t **marker)
{
	btree_raw_mem_node_t *mem_node = NULL;
	btree_status_t ret = BTREE_SUCCESS;

	plat_rwlock_rdlock(&btree->lock);

	mem_node = root_get_and_lock(btree, 1);
	assert(mem_node);
	ref_l1cache(btree, mem_node);

	if (!is_leaf(btree, mem_node->pnode)) {
		/*
		 * Not at leaf yet, keep on seraching down the tree.
		 */
		ret = btree_rupdate_raw_non_leaf(btree, mem_node, range_key,
						 range_key_len, meta, callback_func,
						 callback_args, range_cmp_cb, range_cmp_cb_args,
						 objs_updated, marker);
	}  else {

		/*
		 * Found the leaf, update the keys in range.
		 */
		ret = btree_rupdate_raw_leaf(btree, mem_node, range_key, 
					     range_key_len, meta, callback_func,
					     callback_args, range_cmp_cb, range_cmp_cb_args,
					     objs_updated, marker);	
	}

out:
	plat_rwlock_unlock(&btree->lock);
	return ret;
}

btree_status_t
btree_raw_rupdate(
		struct btree_raw *btree, 
		btree_metadata_t *meta,
	        char *range_key,
	        uint32_t range_key_len,
	        btree_rupdate_cb_t callback_func,
	        void * callback_args,	
		btree_range_cmp_cb_t range_cmp_cb,
		void *range_cmp_cb_args,
	        uint32_t *objs_updated,
	        btree_rupdate_marker_t **marker)
{

	btree_status_t ret = BTREE_SUCCESS;
	if (range_cmp_cb == NULL) {
		range_cmp_cb = btree_key_in_range;
	}
	// Hard coded for now, should change it to be a configurable
	// parameter per btree
	//range_cmp_cb_args = btree->cmp_cb_data;
	ret = btree_raw_rupdate_low_root(btree, meta,
					 range_key, range_key_len, 
					 callback_func, callback_args,
					 range_cmp_cb, range_cmp_cb_args, 
					 objs_updated, marker);
	return ret;
}

btree_status_t
btree_raw_mput_recursive(struct btree_raw *btree, btree_mput_obj_t *objs, uint32_t num_objs,
	       uint32_t write_type, btree_metadata_t *meta, uint32_t *objs_written, int* pathcnt)
{
	btree_status_t ret = BTREE_SUCCESS;
	btree_mput_obj_t* s_objs = objs;
	uint32_t written, count = num_objs;

	if(!num_objs) return BTREE_SUCCESS;

//	dbg_print("1objs=%p num_objs=%d objs_written=%d write_type=%d\n", objs, num_objs, *objs_written, write_type);
	dbg_print_key(objs[0].key, objs[0].key_len, "objs=%p num_objs=%d objs_written=%d write_type=%d", objs, num_objs, *objs_written, write_type);

	ret = btree_raw_mwrite_low(btree, &objs, &num_objs, meta, 0,
			write_type, pathcnt, &written);

	*objs_written += written;

	if(!ret && count - written) // there is something yet to write
	{
		dbg_print_key(objs[0].key, objs[0].key_len, "middle objs=%p s_objs=%p objs-s_objs=%d num_objs=%d objs_written=%d written=%d count=%d", objs, s_objs, objs - s_objs, num_objs, *objs_written, written, count);

		if(objs - s_objs != written) // hole
		{
			ret = btree_raw_mput_recursive(btree, s_objs + written + num_objs,
					count - written - num_objs, write_type, meta, objs_written,
					pathcnt);
			if(!ret)
				ret = btree_raw_mput_recursive(btree, objs, num_objs, write_type,
					meta, objs_written, pathcnt);
		}
		else // no hole
			ret = btree_raw_mput_recursive(btree, s_objs + written, count -
					written, write_type, meta, objs_written, pathcnt);
	}

	return ret;
}

btree_status_t
btree_raw_mput(struct btree_raw *btree, btree_mput_obj_t *objs, uint32_t num_objs,
	       uint32_t flags, btree_metadata_t *meta, uint32_t *objs_written)
{
	btree_status_t      ret = BTREE_SUCCESS;
	int                 pathcnt = 0;
    uint64_t            syndrome = 0; //no use of syndrome in variable keys //get_syndrome(btree, key, keylen);
	int write_type = W_SET;

	if (flags & FDF_WRITE_MUST_NOT_EXIST)
		write_type = W_CREATE;
	else if (flags & FDF_WRITE_MUST_EXIST)
		write_type = W_UPDATE;

    dbg_print_key(objs[0].key, objs[0].key_len, "flags=%d count=%d key_len=%d lic=%ld", flags, num_objs, objs[0].key_len, btree->logical_id_counter);

    assert(!dbg_referenced);
	assert(!locked);

	*objs_written = 0;

	ret = btree_raw_mput_recursive(btree, objs, num_objs, write_type, meta, objs_written, &pathcnt);

#ifdef BTREE_RAW_CHECK
    btree_raw_check(btree, (char *) __FUNCTION__, dump_key(objs[0].key, objs[0].key_len));
#endif
	if(locked)
		fprintf(stderr, "%x locked=%lld\n", (int)pthread_self(), locked);

	assert(!locked);
	if(dbg_referenced)
		fprintf(stderr, "%x dbg_referenced=%ld\n", (int)pthread_self(), dbg_referenced);
    assert(!dbg_referenced);

    dbg_print_key(objs[0].key, objs[0].key_len, "after write_type=%d ret=%d lic=%ld", write_type, ret, btree->logical_id_counter);

	if (BTREE_SUCCESS == ret) {
		switch (write_type) {
		case W_CREATE:
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_CREATE_CNT]),*objs_written);
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_CREATE_PATH]),pathcnt);
			break;
		case W_SET:
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_SET_CNT]),*objs_written);
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_SET_PATH]), pathcnt);
			break;
		case W_UPDATE:
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_UPDATE_CNT]),*objs_written);
			__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_UPDATE_PATH]), pathcnt);
			break;
		default:
			assert(0);
		}
	}

	return ret;
}

btree_status_t 
btree_raw_write(struct btree_raw *btree, char *key, uint32_t keylen,
		char *data, uint64_t datalen, btree_metadata_t *meta, uint32_t flags)
{
    btree_mput_obj_t objs; 
    uint32_t objs_done = 0;

    objs.key = key;
    objs.key_len = keylen;
    objs.data = data;
    objs.data_len = datalen;

    return btree_raw_mput(btree, &objs, 1, flags, meta, &objs_done);
}


static btree_status_t btree_raw_flush_low(btree_raw_t *btree, char *key, uint32_t keylen, uint64_t syndrome)
{
    btree_status_t    ret = BTREE_SUCCESS;
    node_key_t       *pkrec = NULL;
    btree_raw_mem_node_t *node = NULL;
    int				  pathcnt = 0;
    btree_metadata_t  meta;
    bool found = false;

    meta.flags = 0;

    plat_rwlock_rdlock(&btree->lock);

    (void)btree_raw_find(btree, key, keylen, syndrome, &meta, &node, 1 /* EX */, &pathcnt, &found);

    plat_rwlock_unlock(&btree->lock);

    if (found)
      btree->flush_node_cb(&ret, btree->flush_node_cb_data, (uint64_t) node->pnode->logical_id);

    deref_l1cache_node(btree, node);
    node_unlock(node);

    return ret;
}

//======================   FLUSH   =========================================
btree_status_t btree_raw_flush(struct btree_raw *btree, char *key, uint32_t keylen)
{
    btree_status_t      ret = BTREE_FAILURE;
    uint64_t            syndrome = get_syndrome(btree, key, keylen);

    dbg_print_key(key, keylen, "before ret=%d lic=%ld", ret, btree->logical_id_counter);

    ret = btree_raw_flush_low(btree, key, keylen, syndrome);

    dbg_print_key(key, keylen, "after ret=%d lic=%ld", ret, btree->logical_id_counter);

    //TODO change to atomics
    if (BTREE_SUCCESS == ret) 
        btree->stats.stat[BTSTAT_FLUSH_CNT]++;

#ifdef BTREE_RAW_CHECK
    btree_raw_check(btree, (char *) __FUNCTION__, dump_key(key, keylen));
#endif

    assert(!dbg_referenced);

    return(ret);
}

btree_status_t 
btree_raw_ioctl(struct btree_raw *bt, uint32_t ioctl_type, void *data)
{
	btree_status_t ret = BTREE_SUCCESS;

	switch (ioctl_type) {
#ifdef BTREE_UNDO_TEST
	case BTREE_IOCTL_RECOVERY:
		ret = btree_recovery_ioctl(bt, ioctl_type, data);
		break;
#endif
	default:
		break;
	}

	return ret;
}

//======================   DELETE   =========================================
static btree_status_t
rebalanced_delete(btree_raw_t *btree, char *key, uint32_t keylen,
                  btree_metadata_t *meta, uint64_t syndrome)
{
	btree_status_t ret = BTREE_SUCCESS;

	dbg_print("dbg_referenced %ld\n", dbg_referenced);
	assert(!dbg_referenced);

	/* Need tree restructure. Write lock whole tree and retry */
	plat_rwlock_wrlock(&btree->lock);

	// make sure that the temporary key buffer has been allocated
	if (check_per_thread_keybuf(btree)) {
		plat_rwlock_unlock(&btree->lock);

		return(BTREE_FAILURE); // xxxzzz is this the best I can do?
	}

	(void) find_rebalance(&ret, btree, btree->rootid, BAD_CHILD, BAD_CHILD,
	                      BAD_CHILD, NULL, BAD_CHILD, NULL, 0, 0,
	                      key, keylen, meta, syndrome);

	lock_modified_nodes(btree);

	plat_rwlock_unlock(&btree->lock);

	if (BTREE_SUCCESS != deref_l1cache(btree)) {
		ret = BTREE_FAILURE;
	}

	dbg_print("dbg_referenced %ld\n", dbg_referenced);
	assert(!dbg_referenced);

#ifdef BTREE_RAW_CHECK
	btree_raw_check(btree, (char *) __FUNCTION__, dump_key(key, keylen));
#endif

	return(ret);
}

/* Assumption: Caller holds the node lock.
 * This function will attempt to insert tombstone object within the node
 * if the node is not full 
 */
static bool
insert_tombstone_optimized(btree_raw_t *bt, btree_raw_mem_node_t *mnode,
                           int index, char *key, uint32_t keylen,
                           uint64_t syndrome)
{
	btree_status_t ret;
	btree_metadata_t tmp_meta = {0};
	btree_metadata_t search_meta = {0};
	uint32_t objs_written = 0;
	btree_mput_obj_t obj;
	uint32_t scavenged_cnt;
	uint64_t  child_id;
	bool found;
        uint32_t new_inserts = 0;

	assert(is_leaf(bt, mnode->pnode));

	tmp_meta.flags = INSERT_TOMBSTONE;
	obj.key = key;
	obj.key_len = keylen;
	obj.data = NULL;
	obj.data_len = 0;

	if (is_node_full(bt, mnode->pnode, obj.key, obj.key_len,
	                 obj.data_len, &tmp_meta, syndrome, false, /* is_update */
	                 index)) {
		return (false);
	}
	                 
	scavenged_cnt = scavenge_node(bt, mnode, NULL, NULL);
	if (scavenged_cnt != 0) {
		/* If we have scavenged some keys, tombstone key's index would have
		 * changed, do find again */
    		index = bsearch_key(bt, mnode->pnode, key, keylen, &search_meta,
		                    syndrome, BSF_LATEST, &found, &child_id);

	}

	ret = btree_insert_keys_leaf(bt, &tmp_meta, syndrome, mnode, W_SET,
	                             &obj, 1, index, true, /* key exists */
	                             false, /* is_update */ &objs_written, NULL, &new_inserts);
#ifdef PSTATS_1
        fprintf(stderr, "insert_tombstone_optimized: new_inserts=%d\n", new_inserts);
#endif
	return ((ret == BTREE_SUCCESS) && (objs_written > 0));
}

static btree_status_t
insert_tombstone_unoptimized(btree_raw_t *bt, char *key, uint32_t keylen)
{
	btree_metadata_t tmp_meta;
	char *data = NULL;
	uint64_t datalen = 0;

	dbg_print("dbg_referenced %ld\n", dbg_referenced);
	assert(!dbg_referenced);

	tmp_meta.flags = INSERT_TOMBSTONE;
	return (btree_raw_write(bt, key, keylen, data, datalen, &tmp_meta, W_SET));
}

/*   delete a key
 *
 *   returns BTREE_SUCCESS
 *   returns BTREE_FAILURE
 *   returns BTREE_NOT_FOUND
 *
 *   Reference: "Implementing Deletion in B+-trees", Jan Jannink, SIGMOD RECORD,
 *              Vol. 24, No. 1, March 1995
 */
btree_status_t btree_raw_delete(struct btree_raw *btree, char *key, uint32_t keylen, btree_metadata_t *meta)
{
	btree_status_t        ret = BTREE_SUCCESS;
	int                   pathcnt = 0, opt;
	btree_raw_mem_node_t     *node;
	bool key_exists = false;
	uint64_t syndrome = get_syndrome(btree, key, keylen);
	int index = -1;
	uint64_t child_id, child_id_before, child_id_after;
	int del_type;
	enum {
		KEY_NOT_FOUND = 0,
		OPTIMISTIC,
		NEED_TOMBSTONE,
		REBALANCE_NEEDED,
	};
	bool done = false;
	key_info_t key_info;
	key_meta_t key_meta;

	__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_DELETE_CNT]),1);

	plat_rwlock_rdlock(&btree->lock);

	index = btree_raw_find(btree, key, keylen, syndrome, meta, &node, 
	                       1 /* EX */, &pathcnt, &key_exists);
	if (key_exists) {
		/* If its a regular delete (force delete is a delete from scavenger)
		 * we need to insert tombstone */
		if (meta->flags & FORCE_DELETE) {
			if (_keybuf && 
			    !is_leaf_minimal_after_delete(btree, node->pnode, index)) {
				del_type = OPTIMISTIC;
			} else {
				del_type = REBALANCE_NEEDED;
			}
		} else {
			/*
			 * Check whether we can remove the entry instead of adding tombstone entry.
			 * If the key belongs to active container and not to any snapshot, we can remove it.
			 * But if this is last key, there could be records of this key in the next node,
			 * so lets add tombstone for last key.
			 */
			if (index < (node->pnode->nkeys - 1)) {

				btree_leaf_get_meta(node->pnode, index, &key_meta);
				/* Try to remove only if key belongs to active container */
				if (btree_snap_seqno_in_snap(btree, key_meta.seqno) == false) {
					key_info.key = tmp_key_buf;
					btree_leaf_get_nth_key_info2(btree, node->pnode, index + 1, &key_info);

					/*
					 * If the next record is of different key or tombstone for same key
					 * exists, we can remove the entry.
					 */
					if ((btree->cmp_cb(btree->cmp_cb_data, key, keylen, key_info.key, key_info.keylen) != 0) ||
							(key_info.tombstone == true)) {
						if (_keybuf &&
							!is_leaf_minimal_after_delete(btree, node->pnode, index)) {
							del_type = OPTIMISTIC;
						} else {
							del_type = REBALANCE_NEEDED;
						}
					} else {
						/* Next record is of same key we are deleting. So, lets add tombstone. */
						del_type = NEED_TOMBSTONE;
					}
				} else {
					/* The key belongs to snapshot, need to add tombstone. */
					del_type = NEED_TOMBSTONE;
				}
			} else {
				/* This is the last key in node, need to add tombstone */
				del_type = NEED_TOMBSTONE;
			}
		}
	} else {
		del_type = KEY_NOT_FOUND;
	}

	if (del_type == OPTIMISTIC) {
		btree_leaf_get_meta(node->pnode, index, &key_meta);
		if (btree_snap_seqno_in_snap(btree, key_meta.seqno) == true) {
			 __sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_NUM_SNAP_OBJS]), 1);
			 __sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_SNAP_DATA_SIZE]), key_meta.datalen);
			 set_node_snapobjs_pstats(btree, node->pnode, 1, key_meta.datalen, false);
		}

		delete_key_by_index(&ret, btree, node, index);
		__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_DELETE_OPT_CNT]),1);
		done = true;

		//fprintf(stderr, "Deleting key %s in-place\n", key);
	} else if (del_type == NEED_TOMBSTONE) {
		done = insert_tombstone_optimized(btree, node, index, key, keylen, syndrome);

		/* TODO: Remove these couple of lines */
#if 0
		if (!done) {
			fprintf(stderr, "Optimized tombstone insert not possible, "
			                "will try unoptimized\n");
		}
#endif
	}

	/* If done, deref_l1cache will deref and unlock the nodes after
	 * flushing */
	if (done) {
		ref_l1cache(btree, node);

		plat_rwlock_unlock(&btree->lock);
		if (deref_l1cache(btree) != BTREE_SUCCESS)
			ret = BTREE_FAILURE;

#ifdef BTREE_RAW_CHECK
		btree_raw_check(btree, (char *) __FUNCTION__, dump_key(key, keylen));
#endif
		return (ret);
	} else {
		deref_l1cache_node(btree, node);
		plat_rwlock_unlock(&node->lock);
		plat_rwlock_unlock(&btree->lock);
	}

	if (del_type == KEY_NOT_FOUND) {
		ret = BTREE_KEY_NOT_FOUND;
	} else if (del_type == REBALANCE_NEEDED) {
		//fprintf(stderr, "Deleting key %s in-place\n", key);
		ret = rebalanced_delete(btree, key, keylen, meta, syndrome);

		__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_DELETE_PATH]), pathcnt);

	} else {
		ret = insert_tombstone_unoptimized(btree, key, keylen);
	}

	return (ret);
}

/*   recursive deletion/rebalancing routine
 *
 *   ret = 0: don't rebalance this level
 *   ret = 1: rebalance this level if necessary
 *
 */
static int 
find_rebalance(btree_status_t *ret, btree_raw_t *btree, 
		uint64_t this_id, uint64_t left_id, uint64_t right_id, 
		uint64_t l_anchor_id, key_stuff_t *l_anchor_stuff, 
		uint64_t r_anchor_id, key_stuff_t *r_anchor_stuff, 
		int l_this_parent_in, int r_this_parent_in, 
		char *key, uint32_t keylen, btree_metadata_t *meta, uint64_t syndrome)
{
    node_key_t         *pk_insert;
    btree_raw_mem_node_t   *this_mem_node, *left_mem_node, *right_mem_node;
    btree_raw_node_t   *this_node, *left_node, *right_node;
    uint64_t            next_node, next_left, next_right, next_l_anchor, next_r_anchor;
    uint64_t            child_id, child_id_before, child_id_after;
    int                 l_this_parent, r_this_parent;
    key_stuff_t         ks, ks_l, ks_r;
    key_stuff_t        *next_l_anchor_stuff;
    key_stuff_t        *next_r_anchor_stuff;
    int32_t             nkey_child;
    int                 do_rebalance = 1;
    uint32_t            l_balance_keylen = 0;
    uint32_t            r_balance_keylen = 0;
    bool found = false;
	key_meta_t			key_meta;

    if (*ret) { return(0); }

    this_mem_node = get_existing_node(ret, btree, this_id);
    this_node = this_mem_node->pnode;
    assert(this_node != NULL); // xxxzzz remove this
    _pathcnt++;

    //  PART 1: recursive descent from root to leaf node

        //  find path in this node for key
    found = find_key(btree, this_node, key, keylen, meta, syndrome, &child_id, &child_id_before, &child_id_after, &nkey_child);

    next_node = child_id;

    if (is_leaf(btree, this_node)) {
        if (found) {

            btree_leaf_get_meta(this_node, nkey_child, &key_meta);
            if (btree_snap_seqno_in_snap(btree, key_meta.seqno) == true) {
                __sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_NUM_SNAP_OBJS]), 1);
                __sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_SNAP_DATA_SIZE]), key_meta.datalen);
				set_node_snapobjs_pstats(btree, this_mem_node->pnode, 1, key_meta.datalen, false);
            }
            delete_key_by_index(ret, btree, this_mem_node, nkey_child);
            btree->log_cb(ret, btree->log_cb_data, BTREE_UPDATE_NODE, btree, this_mem_node);
        } else {
            // key NOT found at leaf
            *ret = 1;
	}
    } else {
        //   this node is internal

	    // calculate neighbor and anchor nodes
		if (child_id_before == BAD_CHILD) {
			// next_node is least entry in this_node
			if (left_id != BAD_CHILD) {
				left_mem_node = get_existing_node(ret, btree, left_id);
                left_node = left_mem_node->pnode;
				next_left = left_node->rightmost;
			} else {
				next_left = BAD_CHILD;
			}
			next_l_anchor       = l_anchor_id;
			next_l_anchor_stuff = l_anchor_stuff;
			l_this_parent       = 0;
			if (l_anchor_stuff == NULL) {
				l_balance_keylen = 0;
			} else {
				l_balance_keylen = l_anchor_stuff->keylen;
			}
		} else {
			next_left           = child_id_before;
			next_l_anchor       = this_node->logical_id;
			(void) get_key_stuff(btree, this_node, nkey_child - 1, &ks_l);
			next_l_anchor_stuff = &ks_l;
			l_this_parent       = 1;
			l_balance_keylen    = ks_l.keylen;
		}

		if (child_id_after == BAD_CHILD) {
			// next_node is greatest entry in this_node
			if (right_id != BAD_CHILD) {
				right_mem_node = get_existing_node(ret, btree, right_id);
				right_node = right_mem_node->pnode;
				assert(right_node); // xxxzzz fix this!
				(void) get_key_stuff(btree, right_node, 0, &ks);
				next_right = ks.ptr;
			} else {
				next_right = BAD_CHILD;
			}
			next_r_anchor       = r_anchor_id;
			next_r_anchor_stuff = r_anchor_stuff;
			r_this_parent       = 0;
			if (r_anchor_stuff == NULL) {
				r_balance_keylen = 0;
			} else {
				r_balance_keylen = r_anchor_stuff->keylen;
			}
		} else {
			next_right          = child_id_after;
			next_r_anchor       = this_node->logical_id;
			(void) get_key_stuff(btree, this_node, nkey_child, &ks_r);
			next_r_anchor_stuff = &ks_r;
			r_this_parent       = 1;
			r_balance_keylen    = ks_r.keylen;
		}

	    // recursive call
		do_rebalance = find_rebalance(ret, btree, next_node, next_left, next_right, next_l_anchor, next_l_anchor_stuff, next_r_anchor, next_r_anchor_stuff, l_this_parent, r_this_parent, key, keylen, meta, syndrome);
    }

	//  does this node need to be rebalanced?
    if ((!do_rebalance) || (!is_minimal(btree, this_node, l_balance_keylen, r_balance_keylen)))
		return 0;

    if (this_id == btree->rootid) {
        collapse_root(ret, btree, this_mem_node);
		return 0;
    }

	return rebalance(ret, btree, this_mem_node, left_id, right_id, l_anchor_id, l_anchor_stuff, r_anchor_id, r_anchor_stuff, l_this_parent_in, r_this_parent_in);
}

static void collapse_root(btree_status_t *ret, btree_raw_t *btree, btree_raw_mem_node_t *old_root_mem_node)
{
    btree_raw_node_t* old_root_node = old_root_mem_node->pnode;

    if (*ret) { return; }

    if (is_leaf(btree, old_root_node)) {
	//  just keep old empty root node
        if (old_root_node->nkeys != 0) {
	    *ret = 1; // this should never happen!
	}
    } else {
	assert(old_root_node->nkeys == 0);
	assert(old_root_node->rightmost != BAD_CHILD);
	btree->rootid = old_root_node->rightmost;
        if (BTREE_SUCCESS != savepersistent( btree, false))
                assert( 0);
	free_node(ret, btree, old_root_mem_node);
    }
    return;
}

static inline
void exclude_key(btree_raw_t* btree, btree_raw_node_t* node, uint32_t idx,
		uint32_t* nbytes_shift, uint32_t* nkeys_shift, key_stuff_t* ks)
{
	*nbytes_shift -= object_inline_size(btree, ks);
	(*nkeys_shift)--;

	dbg_print("btree=%p node=%p idx=%d ks=%p\n", btree, node, idx, ks);
	(void) get_key_stuff(btree, node, idx, ks);
}

/*   Equalize keys between 'from' node and 'to' node, given that 'to' is to the right of 'from'.
 */
static
int
equalize_keys_non_leaf(btree_raw_t *btree, btree_raw_mem_node_t *anchor_mem, btree_raw_mem_node_t *from_mem, btree_raw_mem_node_t *to_mem, char *s_key, uint32_t s_keylen, uint64_t s_syndrome, uint64_t s_seqno, char **r_key, uint32_t *r_keylen, uint64_t *r_syndrome, uint64_t *r_seqno, int left)
{
    int            i, key_diff = 0;
    uint32_t       threshold;
    node_fkey_t   *pfk;
    node_vkey_t   *pvk;
    node_vlkey_t  *pvlk;
    uint32_t       nbytes_fixed;
    //uint32_t       nbytes_free;
    uint32_t       nkeys_shift;
    uint32_t       nbytes_shift;
    key_stuff_t    ks;
    uint32_t       nbytes_f;
    uint32_t       nbytes_t;
    //uint32_t       nbytes;
	uint32_t	   max_key_size;
	uint64_t       r_ptr;
//	btree_raw_mem_node_t* from_mem = left ? right_mem : left_mem;
//	btree_raw_mem_node_t* to_mem = left ? left_mem : right_mem;
	btree_raw_node_t *anchor = anchor_mem->pnode, *from = from_mem->pnode, *to = to_mem->pnode;

	dbg_print_key(s_key, s_keylen, "from_id=%ld to_id=%ld dir: %s", from->logical_id, to->logical_id, left ? "left" : "right");

	assert(!is_leaf(btree, to_mem->pnode));

    if (check_per_thread_keybuf(btree))
		return 0;

//	*r_key = NULL;

	__sync_add_and_fetch(&(btree->stats.stat[left ? BTSTAT_LSHIFTS : BTSTAT_RSHIFTS]),1);

	//Get the maximum key size that can be accomodated in parent on moving keys
	max_key_size = vnode_bytes_free(anchor) + s_keylen;

	(void) get_key_stuff(btree, from, 0, &ks);
	nbytes_fixed = ks.offset;

	if (ks.fixed) {
		if (from->nkeys <= to->nkeys)
			return 0;
		// xxxzzz should the following takes into account the inclusion of the anchor separator key?
		nkeys_shift = (from->nkeys - to->nkeys)/2;
		if (nkeys_shift == 0) {
			nkeys_shift = 1; // always shift at least one key!
		}
		nbytes_shift = nkeys_shift*ks.offset;
		if(left)
			pfk = (node_fkey_t *) ((char *) from->keys + (nkeys_shift - 1)*nbytes_fixed);
		else
			pfk = (node_fkey_t *) ((char *) from->keys + (from->nkeys - nkeys_shift)*nbytes_fixed);
		*r_key      = (char *) pfk->key;
		*r_keylen   = sizeof(uint64_t);
		*r_syndrome = pfk->key;
		*r_seqno    = pfk->seqno;
		r_ptr      = pfk->ptr;
	} else {
		nbytes_f     = (btree->nodesize - from->insert_ptr) + from->nkeys * nbytes_fixed;
		nbytes_t     = (btree->nodesize - to->insert_ptr)   + to->nkeys   * nbytes_fixed;
		if ((nbytes_f <= nbytes_t) || (from->nkeys <= 1))
			return 0;

		threshold = (nbytes_f - nbytes_t) / 2;

		nbytes_shift= nkeys_shift = key_diff = 0;

		while(nkeys_shift < from->nkeys &&
				(nbytes_shift + nkeys_shift * nbytes_fixed + key_diff) <= threshold)
		{
			(void) get_key_stuff(btree, from, left ? nkeys_shift : from->nkeys - nkeys_shift - 1, &ks);
			key_diff = s_keylen - ks.keylen; // len diff between old and new rightmost keys
			nbytes_shift += object_inline_size(btree, &ks);
			nkeys_shift++;
		}
		assert(nkeys_shift > 0 && nkeys_shift < from->nkeys); // xxxzzz remove me!

		if(nkeys_shift < 2 && left)
			return 0;

		/* Exclude right key from left move */
		if(left)
			exclude_key(btree, from, nkeys_shift - 2, &nbytes_shift, &nkeys_shift, &ks);

		/*
		 * Check whether the new rightmost in from, can be added in "parent".
		 * If it doesn't keep reverting till we get the key at least which can be 
		 * put in parent.
		 */
		while(ks.keylen > max_key_size && nkeys_shift > 1)
			exclude_key(btree, from, left ? nkeys_shift - 2 :
				from->nkeys - nkeys_shift + 1, &nbytes_shift, &nkeys_shift, &ks);

		/* No key can fit parent as rightmost or its already in the parent */
		if(ks.keylen > max_key_size || (ks.leaf && !left && nkeys_shift == 1))
			return 0;

		/* Exclude left key from right move */
		if(!left || !ks.leaf)
			nbytes_shift -= object_inline_size(btree, &ks);

		if(!left)
			nkeys_shift--;

		memcpy(_keybuf, ks.pkey_val, ks.keylen);
		*r_key      = _keybuf;
		*r_keylen   = ks.keylen;
		*r_syndrome = 0;
		*r_seqno    = ks.seqno;
		r_ptr      = ks.ptr;
	}

	if(!left) {
		// make room for the lower fixed keys
		memmove((char *) to->keys + (nkeys_shift + (ks.leaf ? 0 : 1))*nbytes_fixed, (char *) to->keys, to->nkeys*nbytes_fixed);
		// copy the fixed size portion of the keys. skip last key for non leaves
		if(nkeys_shift)
			memcpy(to->keys, (char *) from->keys + (from->nkeys - nkeys_shift) * nbytes_fixed, nkeys_shift * nbytes_fixed);
	} else if(nkeys_shift) {
		// copy the fixed size portion of the keys. skip last key for non leaves
		memcpy((char *) to->keys + (to->nkeys + (ks.leaf ? 0 : 1)) * nbytes_fixed, from->keys, (nkeys_shift  - (ks.leaf ? 0 : 1)) * nbytes_fixed);
		// remove keys from 'from' node
		memmove(from->keys, (char *) from->keys + nkeys_shift * nbytes_fixed, (from->nkeys - nkeys_shift) * nbytes_fixed);
	}

	if(!ks.leaf) {
		int rightmost_pos =  left ? to->nkeys : nkeys_shift;
		if (ks.fixed) {
			pfk = (node_fkey_t *) ((char *) to->keys + rightmost_pos * nbytes_fixed);
			pfk->key   = (uint64_t) s_key;
			pfk->ptr   = left ? to->rightmost : from->rightmost;
			pfk->seqno = s_seqno;
		} else {
			pvk = (node_vkey_t *) ((char *) to->keys + rightmost_pos * nbytes_fixed);
			pvk->keylen = s_keylen;
			pvk->keypos = 0; // will be set in update_keypos below
			pvk->ptr    = left ? to->rightmost : from->rightmost;
			pvk->seqno  = s_seqno;
		}
	}

	to->nkeys   = to->nkeys + nkeys_shift;
	from->nkeys = from->nkeys - nkeys_shift;

	if(!left && !ks.leaf) {
		to->nkeys++;
		from->nkeys--;
	}

	// copy variable sized stuff
	if (ks.fixed) {
		to->insert_ptr = 0;
	} else {
		int rlen = *r_keylen;

		/* Exclude rightmost copying for leaves */
		if(ks.leaf) {
			rlen = 0;
			s_keylen = 0;
		}

		dbg_print("insert_ptr=%d nbytes_shift=%d key_diff=%d rlen=%d s_keylen=%d r_keylen=%d to->nkeys=%d from->nkeys=%d\n", from->insert_ptr, nbytes_shift, key_diff, rlen, s_keylen, *r_keylen, to->nkeys, from->nkeys);

		if(left) {
			if (!ks.leaf)
				memcpy(((char *) to) + to->insert_ptr - s_keylen, s_key, s_keylen);

			memcpy(((char *) to) + to->insert_ptr - nbytes_shift - s_keylen,
					((char *) from) + btree->nodesize - nbytes_shift,
					nbytes_shift);

			memmove(((char *) from) + from->insert_ptr + nbytes_shift + rlen,
					((char *) from) + from->insert_ptr,
					btree->nodesize - from->insert_ptr - nbytes_shift - rlen);
		} else {
			memmove(((char *) to) + to->insert_ptr - nbytes_shift - s_keylen,
					((char *) to) + to->insert_ptr,
					btree->nodesize - to->insert_ptr);

			if (!ks.leaf)
				memcpy(((char *) to) + btree->nodesize - nbytes_shift - s_keylen, s_key, s_keylen);

			memcpy(((char *) to) + btree->nodesize - nbytes_shift,
					((char *) from) + from->insert_ptr,
					nbytes_shift);
		}

		to->insert_ptr = to->insert_ptr - nbytes_shift - s_keylen;
		from->insert_ptr = from->insert_ptr + nbytes_shift + rlen;

		//  update the keypos pointers
		update_keypos(btree, to,   0);
		update_keypos(btree, from, 0);
	}

	if(left)
		to->rightmost = r_ptr;
	else
		from->rightmost = r_ptr;

	dbg_print_key(s_key, s_keylen, "after s_key");
	dbg_print_key(*r_key, *r_keylen, "after r_key");

    #ifdef DEBUG_STUFF
	if (Verbose) {
	    btree_raw_dump(stderr, btree);
	}
    #endif

    return 1;
}

//Caller must free key_info.key after use
static int
equalize_keys_leaf(btree_raw_t *btree, btree_raw_mem_node_t *anchor_mem, btree_raw_mem_node_t *from_mem,
		      btree_raw_mem_node_t *to_mem, char *s_key, uint32_t s_keylen, uint64_t s_syndrome,
		      uint64_t s_seqno, char **r_key, uint32_t *r_keylen, uint64_t *r_syndrome, uint64_t *r_seqno, int left)
{
	key_info_t key_info;
	bool res = false;
	int32_t old_used_space = 0;
	int32_t new_used_space = 0;
	int32_t bytes_changed = 0;
	uint32_t max_anchor_keylen = 0;

	dbg_print("from_id=%ld to_id=%ld left=%d\n", from_mem->pnode->logical_id, to_mem->pnode->logical_id, left);

	old_used_space = btree_leaf_used_space(btree, from_mem->pnode) +
			 btree_leaf_used_space(btree, to_mem->pnode);

	max_anchor_keylen = vnode_bytes_free(anchor_mem->pnode) + 
			    s_keylen;
	if (left) {
		res = btree_leaf_shift_left(btree, from_mem->pnode, to_mem->pnode,
					    &key_info, max_anchor_keylen);
	} else {
		res = btree_leaf_shift_right(btree, from_mem->pnode, to_mem->pnode,
					     &key_info, max_anchor_keylen);
	}
        assert(res == true);

	new_used_space = btree_leaf_used_space(btree, from_mem->pnode) +
	                 btree_leaf_used_space(btree, to_mem->pnode);

        *r_key = key_info.key;
        *r_keylen = key_info.keylen;
        *r_syndrome = key_info.syndrome;
        *r_seqno = key_info.seqno;

	bytes_changed = new_used_space - old_used_space;

	__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_LEAF_BYTES]), bytes_changed);
	__sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_SPCOPT_BYTES_SAVED]), -bytes_changed);

	return res;
}

static int
equalize_keys(btree_raw_t *btree, btree_raw_mem_node_t *anchor_mem, btree_raw_mem_node_t *left_mem,
	      btree_raw_mem_node_t *right_mem, char *s_key, uint32_t s_keylen, uint64_t s_syndrome,
	      uint64_t s_seqno, char **r_key, uint32_t *r_keylen, uint64_t *r_syndrome, uint64_t *r_seqno,
	      int left, bool * free_key)
{
	int ret = 0;
	btree_raw_mem_node_t* from_mem = left ? right_mem : left_mem;
	btree_raw_mem_node_t* to_mem = left ? left_mem : right_mem;

	if (is_leaf(btree, from_mem->pnode)) {
		ret = equalize_keys_leaf(btree, anchor_mem, from_mem, to_mem, s_key, s_keylen, s_syndrome,
					      s_seqno, r_key, r_keylen, r_syndrome, r_seqno, left);		
		*free_key = true;
	} else {
		ret = equalize_keys_non_leaf(btree, anchor_mem, from_mem, to_mem, s_key, s_keylen, s_syndrome,
					      s_seqno, r_key, r_keylen, r_syndrome, r_seqno, left);		
		*free_key = false;
	}
	return ret;
}

/*   Copy keys from 'from' node to 'to' node, given that 'to' is to left of 'from'.
 */
static void
merge_nodes_non_leaf(btree_raw_t *btree, btree_raw_node_t *anchor, btree_raw_node_t *from, btree_raw_node_t *to, char *s_key, uint32_t s_keylen, uint64_t s_syndrome, uint64_t s_seqno)
{
    node_fkey_t   *pfk;
    node_vkey_t   *pvk;
    uint32_t       nbytes_from;
    uint32_t       nbytes_to;
    uint32_t       nbytes_fixed;
    uint32_t       nbytes_free;
    uint32_t       nbytes_needed;
    key_stuff_t    ks;

	assert(!is_leaf(btree, to));

	__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_LMERGES]),1);

	(void) get_key_stuff(btree, from, 0, &ks);

	nbytes_fixed = ks.offset;
	nbytes_from = !ks.fixed ? btree->nodesize - from->insert_ptr : from->nkeys*ks.offset;

	if(ks.leaf) s_keylen = 0;

#ifdef DEBUG_STUFF
	if (ks.leaf) {
		assert(vlnode_bytes_free(to) >= btree->nodesize - from->insert_ptr + from->nkeys*sizeof(node_vlkey_t) ); // xxxzzz remove this!
	} else if (ks.fixed) {
			assert((to->nkeys + from->nkeys + 1) <= btree->fkeys_per_node); // xxxzzz remove this!
	} else {
			assert(vnode_bytes_free(to) >=btree->nodesize - from->insert_ptr + from->nkeys*sizeof(node_vkey_t) + s_keylen + nbytes_fixed); // xxxzzz remove this!
	}
#endif

	//  Copy the fixed size portion of the keys, leaving space for the
	//  converting the 'to' right pointer to a regular key for non leaves.
	memcpy((char *) to->keys + (to->nkeys + (ks.leaf ? 0 : 1)) * nbytes_fixed, from->keys, from->nkeys * nbytes_fixed);

	if (!ks.leaf) {
		// convert 'to' rightmost pointer to a regular key
		if (ks.fixed) {
			pfk = (node_fkey_t *) ((char *) to->keys + to->nkeys * nbytes_fixed);
			pfk->key   = (uint64_t) s_key;
			pfk->ptr   = to->rightmost;
			pfk->seqno = s_seqno;
		} else {
			pvk = (node_vkey_t *) ((char *) to->keys + to->nkeys * nbytes_fixed);
			pvk->keylen = s_keylen;
			pvk->keypos = 0; // will be set in update_keypos below
			pvk->ptr    = to->rightmost;
			pvk->seqno  = s_seqno;
		}
	}
	to->nkeys = to->nkeys + from->nkeys + (ks.leaf ? 0 : 1);

    // copy variable sized stuff
	if (ks.fixed) {
		to->insert_ptr = 0;
	} else {
		//  Copy key that converts 'right' pointer of 'to' node to a regular key.
		if (!ks.leaf)
			memcpy(((char *) to) + to->insert_ptr - s_keylen, s_key, s_keylen);

		//  Copy over the 'from' stuff.
		memcpy(((char *) to) + to->insert_ptr - nbytes_from - s_keylen, 
				((char *) from) + from->insert_ptr,
				nbytes_from);

		to->insert_ptr = to->insert_ptr- nbytes_from - (ks.leaf ? 0 : s_keylen);

		//  update the keypos pointers
		update_keypos(btree, to, 0);
	}

	to->rightmost = from->rightmost;
	to->next = from->next;

#ifdef DEBUG_STUFF
	if (Verbose) {
		char stmp[10000];
		int  len;
		if (btree->flags & SYNDROME_INDEX) {
			sprintf(stmp, "%p", s_key);
			len = strlen(stmp);
		} else {
			strncpy(stmp, s_key, s_keylen);
			len = s_keylen;
		}
		fprintf(stderr, "********  After merge_nodes for key '%s' [syn=%lu], B-Tree:  *******\n", dump_key(stmp, len), s_syndrome);
		btree_raw_dump(stderr, btree);
	}
#endif

	return;
}

static void
merge_nodes_leaf(btree_raw_t *btree, btree_raw_node_t *anchor, btree_raw_node_t *from,
		 btree_raw_node_t *to, char *s_key, uint32_t s_keylen, uint64_t s_syndrome, uint64_t s_seqno)
{
	bool res = false;
	int32_t bytes_changed = 0;
	int32_t old_used_space = 0;
	int32_t new_used_space = 0;

	old_used_space = btree_leaf_used_space(btree, from) +
			 btree_leaf_used_space(btree, to);

	res = btree_leaf_merge_left(btree, from, to);
	assert(res == true);

	new_used_space = btree_leaf_used_space(btree, to);

	bytes_changed = new_used_space - old_used_space;
#ifdef DEBUG_BUILD
//	assert(bytes_changed <= 0);
#endif

	__sync_add_and_fetch(&(btree->stats.stat[BTSTAT_LEAF_BYTES]), bytes_changed);
	__sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_SPCOPT_BYTES_SAVED]), -bytes_changed);
}

static void
merge_nodes(btree_raw_t *btree, btree_raw_node_t *anchor, btree_raw_node_t *from,
	    btree_raw_node_t *to, char *s_key, uint32_t s_keylen, uint64_t s_syndrome, uint64_t s_seqno)
{
	if (is_leaf(btree, from)) {
		merge_nodes_leaf(btree, anchor, from, to, s_key, s_keylen, s_syndrome, s_seqno);
	} else {
		merge_nodes_non_leaf(btree, anchor, from, to, s_key, s_keylen, s_syndrome, s_seqno);
	}
}


static int rebalance(btree_status_t *ret, btree_raw_t *btree, btree_raw_mem_node_t *this_mem_node, uint64_t left_id, uint64_t right_id, uint64_t l_anchor_id, key_stuff_t *l_anchor_stuff, uint64_t r_anchor_id, key_stuff_t *r_anchor_stuff, int l_this_parent, int r_this_parent)
{
    btree_raw_node_t     *this_node = this_mem_node->pnode, *left_node, *right_node, *balance_node, *anchor_node, *merge_node;
    btree_raw_mem_node_t *left_mem_node, *right_mem_node, *balance_mem_node, *anchor_mem_node;
    char                 *s_key;
    uint32_t              s_keylen;
    uint64_t              s_syndrome;
    uint64_t              s_seqno;
    uint64_t              s_ptr;
    char                 *r_key = NULL;
    uint32_t              r_keylen = 0;
    uint64_t              r_syndrome = 0;
    uint64_t              r_seqno = 0;
    uint32_t              balance_keylen;
    key_stuff_t           ks;
    int                   balance_node_is_sibling;
    int                   next_do_rebalance = 0;
    bool free_key = false; 
    btree_metadata_t      meta;

    if (*ret) { return(0); }

    if (left_id == BAD_CHILD) {
        left_node = NULL;
        left_mem_node = NULL;
    } else {
		left_mem_node = get_existing_node(ret, btree, left_id);
        left_node = left_mem_node->pnode;
    }

    if (right_id == BAD_CHILD) {
        right_node = NULL;
        right_mem_node = NULL;
    } else {
		right_mem_node = get_existing_node(ret, btree, right_id);
        right_node = right_mem_node->pnode;
    }

    if (left_node == NULL) {
        balance_node   = right_node;
        balance_mem_node   = right_mem_node;
		balance_keylen = r_anchor_stuff->keylen;
    } else if (right_node == NULL) {
        balance_node   = left_node;
        balance_mem_node   = left_mem_node;
		balance_keylen = l_anchor_stuff->keylen;
    } else {
        // give siblings preference
		if (l_this_parent && (!r_this_parent)) {
			balance_node   = left_node;
			    balance_mem_node   = left_mem_node;
			balance_keylen = l_anchor_stuff->keylen;
		} else if (r_this_parent && (!l_this_parent)) {
			balance_node   = right_node;
		    balance_mem_node   = right_mem_node;
			balance_keylen = r_anchor_stuff->keylen;
		} else if (left_node->insert_ptr > right_node->insert_ptr) {
			balance_node   = right_node;
		    balance_mem_node   = right_mem_node;
			balance_keylen = r_anchor_stuff->keylen;
		} else {
			balance_node   = left_node;
		    balance_mem_node   = left_mem_node;
			balance_keylen = l_anchor_stuff->keylen;
		}
    }

    balance_node_is_sibling = balance_node == left_node ? l_this_parent : r_this_parent;

    assert(balance_node != NULL);

    if ((!is_minimal(btree, balance_node, balance_keylen, 0)) ||
        (!balance_node_is_sibling)) {
        next_do_rebalance = 0;
        if (balance_node == left_node) {
			anchor_mem_node    = get_existing_node(ret, btree, l_anchor_id);
            anchor_node = anchor_mem_node->pnode;

			s_key      = l_anchor_stuff->pkey_val;
			s_keylen   = l_anchor_stuff->keylen;
			s_syndrome = l_anchor_stuff->syndrome;
			s_seqno    = l_anchor_stuff->seqno;
			s_ptr      = l_anchor_stuff->ptr;

			int res = equalize_keys(btree, anchor_mem_node, balance_mem_node, this_mem_node,
					        s_key, s_keylen, s_syndrome, s_seqno,
					        &r_key, &r_keylen, &r_syndrome, &r_seqno, RIGHT, &free_key);
			if(!res)
				return next_do_rebalance;
		} else {
			anchor_mem_node = get_existing_node(ret, btree, r_anchor_id);
            anchor_node = anchor_mem_node->pnode;

			s_key      = r_anchor_stuff->pkey_val;
			s_keylen   = r_anchor_stuff->keylen;
			s_syndrome = r_anchor_stuff->syndrome;
			s_seqno    = r_anchor_stuff->seqno;
			s_ptr      = r_anchor_stuff->ptr;

			int res = equalize_keys(btree, anchor_mem_node, this_mem_node, balance_mem_node,
					        s_key, s_keylen, s_syndrome, s_seqno,
					        &r_key, &r_keylen, &r_syndrome, &r_seqno, LEFT, &free_key);
			if(!res)
				return next_do_rebalance;
		}

		/*
		 * Along with "this_node", balance_node will also get modified. Hence need to flush the balance_node.
		 * "this_node" is added to modified l1 cache list in find_rebalance function as a part of delete key call.
		 * Anchor node will also be added to modified l1 cache as a part of delete key call in this function
		 */
		modify_l1cache_node(btree,balance_mem_node);

		if (r_key != NULL) {
			// update keyrec in anchor
			delete_key(ret, btree, anchor_mem_node, s_key, s_keylen, s_seqno, s_syndrome);

			insert_key(ret, btree, anchor_mem_node, r_key, r_keylen, r_seqno, sizeof(uint64_t), (char *) &s_ptr, r_syndrome);
			modify_l1cache_node(btree, anchor_mem_node);
			if (free_key == true) {
				free_buffer(btree, r_key);
			}
		}
    } else {
        next_do_rebalance = 1;
        if (balance_node == left_node) {
			//  left anchor is parent of this_node
			anchor_mem_node    = get_existing_node(ret, btree, l_anchor_id);
		    anchor_node = anchor_mem_node->pnode;
			merge_node     = left_node;

			s_key      = l_anchor_stuff->pkey_val;
			s_keylen   = l_anchor_stuff->keylen;
			s_syndrome = l_anchor_stuff->syndrome;
			s_seqno    = l_anchor_stuff->seqno;

			merge_nodes(btree, anchor_node, this_node, merge_node, s_key, s_keylen, s_syndrome, s_seqno);

           /*
            * Along with "this_node", merge_node will also get modified. Hence need to flush the merge_node.
            * "this_node" is added to modified l1 cache list in find_rebalance function as a part of delete key call.
            * Anchor node will also be added to modified l1 cache as a part of delete key call in this function
			* merge_node in this case is left_node.
            */
			modify_l1cache_node(btree,left_mem_node);

			//  update the anchor
			//  cases:
			//       1) this_node is the rightmost pointer
			//       2) this_node is NOT a rightmost pointer
			//

            if (this_node->logical_id == anchor_node->rightmost) {
				//  child is the rightmost pointer
				// 
				//  Make the 'rightmost' point to the merge_node,
				//  then delete the key for the merge_node.
				anchor_node->rightmost = l_anchor_stuff->ptr;
			} else {
				//  Make the key for 'this_node' point to the merge_node,
				//  then delete the key for the merge_node.
				//  Note that this_node corresponds to l_anchor_stuff->nkey+1!
				// 
				update_ptr(btree, anchor_node, l_anchor_stuff->nkey+1, l_anchor_stuff->ptr);
			}
			delete_key(ret, btree, anchor_mem_node, l_anchor_stuff->pkey_val, l_anchor_stuff->keylen, l_anchor_stuff->seqno, l_anchor_stuff->syndrome);

			// free this_node
			if (!(*ret)) {
				free_node(ret, btree, this_mem_node);
			}
		} else {
			//  Since the left anchor is not the parent of this_node,
			//  the right anchor MUST be parent of this_node.
			//  Also, this_node must be key number 0.

			assert(r_this_parent);
			anchor_mem_node    = get_existing_node(ret, btree, r_anchor_id);
            anchor_node = anchor_mem_node->pnode;
			merge_node     = right_node;

			s_key      = r_anchor_stuff->pkey_val;
			s_keylen   = r_anchor_stuff->keylen;
			s_syndrome = r_anchor_stuff->syndrome;
			s_seqno    = r_anchor_stuff->seqno;

			merge_nodes(btree, anchor_node, merge_node, this_node, s_key, s_keylen, s_syndrome, s_seqno);

#if 0
			//EF: this_node should be marked modified here if it not yet, otherwise remove this
           /*
            * Along with "this_node", merge_node will also get modified. Hence need to flush the merge_node.
            * "this_node" is added to modified l1 cache list in find_rebalance function as a part of delete key call.
            * Anchor node will also be added to modified l1 cache as a part of delete key call in this function
            * merge_node in this case is right_node.
            */
			modify_l1cache_node(btree,right_mem_node);
#endif

			//  update the anchor

			//  If anchor is 'rightmost', r_anchor_stuff holds data for 'this_node'.
			//  Otherwise, r_anchor_stuff holds data for the node to the
			//  immediate right of 'this_node'.

            //  Get data for 'this_node'.
            if (anchor_node->rightmost== merge_node->logical_id) {
				//  Anchor is 'rightmost' node.
				//  Delete key for 'this_node'.
				anchor_node->rightmost = this_node->logical_id;
			} else {
				//  Anchor is NOT 'rightmost' node.
				//  Delete key for 'this_node'.
				update_ptr(btree, anchor_node, r_anchor_stuff->nkey + 1, this_node->logical_id);
			}
				delete_key(ret, btree, anchor_mem_node, r_anchor_stuff->pkey_val, r_anchor_stuff->keylen, r_anchor_stuff->seqno, r_anchor_stuff->syndrome);

			// free this_node
			if (!(*ret)) {
				free_node(ret, btree, right_mem_node);
			}
		}
		btree->log_cb(ret, btree->log_cb_data, BTREE_UPDATE_NODE, btree, anchor_mem_node);
    }

    return(next_do_rebalance);
}

static int check_per_thread_keybuf(btree_raw_t *btree)
{
    //  Make sure that the per-thread key buffer has been allocated,
    //  and that it is big enough!
    if (_keybuf_size < btree->nodesize) {
	if (_keybuf != NULL) {
	    free(_keybuf);
	    _keybuf_size = 0;
	}
	_keybuf = malloc(btree->nodesize);
	if (_keybuf == NULL) {
	    return(1);
	}
	_keybuf_size = btree->nodesize;
    }
    return(0);
}

void release_per_thread_keybuf()
{
	if (_keybuf) {
		free(_keybuf);
		_keybuf = NULL;
	}
}


//======================   FAST_BUILD  =========================================

int btree_raw_fast_build(btree_raw_t *btree)
{
    // TBD xxxzzz
    return(0);
}

//======================   DUMP  =========================================
#ifdef DEBUG_STUFF
static char *dump_key(char *key, uint32_t keylen)
{
    static char  stmp[200];

    stmp[0] = '\0';
    if (keylen > 100) {
	strncat(stmp, key, 100);
	strcat(stmp, "...");
    } else {
	strncat(stmp, key, keylen);
    }
    return(stmp);
}

static void dump_line(FILE *f, char *key, uint32_t keylen)
{
    if (key != NULL) {
	fprintf(f, "----------- Key='%s' -----------\n", dump_key(key, keylen));
    } else {
	fprintf(f, "-----------------------------------------------------------------------------------\n");
    }
}

static void dump_node_low(btree_raw_t *bt, FILE *f, btree_raw_node_t *n, char *key, uint32_t keylen)
{
    int             i;
    char           *sflags;
    int             nfreebytes;
    int             nkey_bytes;
    node_fkey_t    *pfk;
    node_vkey_t    *pvk;
    node_vlkey_t   *pvlk;
    key_stuff_t     ks;
    btree_raw_mem_node_t   *n_child;

	if(key)
		dump_line(f, key, keylen);

    if (n == NULL) {
        fprintf(f, "***********   BAD NODE!!!!   **********\n");
        abort();
	return;
    }

    if (is_leaf(bt, n)) {
        sflags = "LEAF";
	nkey_bytes = sizeof(node_vlkey_t);
    } else {
        sflags = "";
	if (bt->flags & SYNDROME_INDEX) {
	    nkey_bytes = sizeof(node_fkey_t);
	} else {
	    nkey_bytes = sizeof(node_vkey_t);
	}
	//assert(n->rightmost != 0);
    }

    if ((bt->flags & SYNDROME_INDEX) && !(n->flags & LEAF_NODE)) {
	nfreebytes = bt->nodesize - sizeof(btree_raw_node_t) - n->nkeys*nkey_bytes;
    } else {
	nfreebytes = n->insert_ptr - sizeof(btree_raw_node_t) - n->nkeys*nkey_bytes;
    }
    //assert(nfreebytes >= 0);

    fprintf(f, "%x Node [%ld][%p]: %d keys, ins_ptr=%d, %d free bytes, flags:%s%s, right=[%ld]\n", (int)pthread_self(), n->logical_id, n, n->nkeys, n->insert_ptr, nfreebytes, sflags, is_root(bt, n) ? ":ROOT" : "", n->rightmost);

    for (i=0; i<n->nkeys; i++) {

		key_stuff_info_t ksi;
		ksi.key = tmp_key_buf;
		get_key_stuff_info2(bt, n, i, &ksi);
	if (n->flags & LEAF_NODE) {
//	    fprintf(f, "   Key='%s': ", dump_key((char *) n + pvlk->keypos, pvlk->keylen));
	    fprintf(f, "%x   Key='%s': keylen=%d, datalen=%ld, ptr=%ld, seqno=%ld %s\n", (int)pthread_self(), dump_key(ksi.key, ksi.keylen), ksi.keylen, ksi.datalen, ksi.ptr, ksi.seqno, big_object_kd(bt, ksi.keylen, ksi.datalen) ? "[OVERFLOW]" : "");
#if 0
	    if (big_object(bt, pvlk)) {
		//  overflow object
		fprintf(f, " [OVERFLOW!]\n");
	    } else {
		fprintf(f, "\n");
	    }
#endif
	} else if (bt->flags & SECONDARY_INDEX) {
	    fprintf(f, "%x   Key='%s': ", (int)pthread_self(), dump_key(ksi.key, ksi.keylen));
	    fprintf(f, "%x keylen=%d, ptr=%ld, seqno=%ld\n", (int)pthread_self(), ksi.keylen, ksi.ptr, ksi.seqno);
	} else if (bt->flags & SYNDROME_INDEX) {
		assert(0);
	    pfk  = ((node_fkey_t *) n->keys) + i;
	    fprintf(f, "   syn=%lu: ", pfk->key);
	    fprintf(f, "ptr=%ld, seqno=%ld\n", pfk->ptr, pfk->seqno);
	} else {
	    assert(0);
	}
    }

	return;
    if (!(n->flags & LEAF_NODE)) {
	btree_status_t ret = BTREE_SUCCESS;
	char  stmp[100];

	// non-leaf
	for (i=0; i<n->nkeys; i++) {
	    get_key_stuff(bt, n, i, &ks);
	    n_child = get_existing_node_low(&ret, bt, ks.ptr, 0, false, false); 
            if(n_child->modified != n_child->last_dump_modified)
            {
	    if (bt->flags & SYNDROME_INDEX) {
	        sprintf(stmp, "%p", ks.pkey_val);
		dump_node(bt, f, n_child->pnode, stmp, strlen(stmp));
	    } else {
		dump_node(bt, f, n_child->pnode, ks.pkey_val, ks.keylen);
	    }
            n_child->last_dump_modified = n_child->modified;
            }
            deref_l1cache_node(bt, n_child);
	}
	assert(n->rightmost != 0);
	if (n->rightmost != 0) {
	    n_child = get_existing_node_low(&ret, bt, n->rightmost, 0, false, false); 
            if(n_child->modified != n_child->last_dump_modified)
            {
	    dump_node(bt, f, n_child->pnode, "==RIGHT==", 9);
            n_child->last_dump_modified = n_child->modified;
            }
            deref_l1cache_node(bt, n_child);
	}
    }
}

static void dump_node(btree_raw_t *bt, FILE *f, btree_raw_node_t *n, char *key, uint32_t keylen)
{
	no_lock =1 ;
	dump_node_low(bt, f, n, key, keylen);
	no_lock = 0;
}

static
void btree_raw_dump(FILE *f, btree_raw_t *bt)
{
    btree_status_t     ret = BTREE_SUCCESS;
    btree_raw_mem_node_t  *n;
    char               sflags[1000];

    sflags[0] = '\0';
    if (bt->flags & SYNDROME_INDEX) {
        strcat(sflags, "SYN ");
    }
    if (bt->flags & SECONDARY_INDEX) {
        strcat(sflags, "SEC ");
    }

    dump_line(f, NULL, 0);

    fprintf(f, "B-Tree: flags:(%s), node:%dB, maxkey:%dB, minkeys:%d, bigobj:%dB\n", sflags, bt->nodesize, bt->max_key_size, bt->min_keys_per_node, bt->big_object_size);
no_lock = 1;
    n = get_existing_node_low(&ret, bt, bt->rootid, 0, false, false); 
    if (BTREE_SUCCESS != ret || (n == NULL)) {
	fprintf(f, "*********************************************\n");
	fprintf(f, "    *****  Could not get root node!!!!  *****\n");
	fprintf(f, "*********************************************\n");
    }
    
    if(n->modified != n->last_dump_modified)
    {
        dump_node_low(bt, f, n->pnode, "===ROOT===", 10);
        n->last_dump_modified = n->modified;
    }

    dump_line(f, NULL, 0);
    deref_l1cache_node(bt, n);
no_lock = 0;
//    deref_l1cache(bt);
}
#endif

//======================   CHECK   =========================================
#ifdef DBG_PRINT
void print_key_func(FILE *f, const char* func, int line, char* key, int keylen, char *msg, ...)
{
	int i;
    char     stmp[1024];
    char     stmp1[1024];
    va_list  args;

    va_start(args, msg);

    vsprintf(stmp, msg, args);

    va_end(args);

	if(key) {
		assert(keylen + 1 < sizeof(stmp1));
		for(i=0;i<keylen;i++)
			stmp1[i] = key[i] < 32 ? '.' : key[i];
		stmp1[i] = 0;
		(void) fprintf(stderr, "%x %s:%d %s key=[%lx][%s] len=%d\n", (int)pthread_self(), func, line,  stmp, *((uint64_t*)key), stmp1, keylen);
	}
	else
		(void) fprintf(stderr, "%x %s:%d %s key=[NULL] len=%d\n", (int)pthread_self(), func, line,  stmp, keylen);
}
#endif

static void check_err(FILE *f, char *msg, ...)
{
    char     stmp[1024];
    va_list  args;

    va_start(args, msg);

    vsprintf(stmp, msg, args);
    strcat(stmp, "\n");

    va_end(args);

    (void) fprintf(stderr, "%x %s", (int)pthread_self(), stmp);
    abort();
}

static void check_node(btree_raw_t *bt, FILE *f, btree_raw_mem_node_t *node, char *key_in_left, uint32_t keylen_in_left, char *key_in, uint32_t keylen_in, char *key_in_right, uint32_t keylen_in_right, int rightmost_flag)
{
    btree_raw_node_t *n = node->pnode;
    int                 i;
    int                 nfreebytes;
    int                 nkey_bytes;
    node_fkey_t        *pfk;
    node_vkey_t        *pvk;
    node_vlkey_t       *pvlk;
    key_stuff_t         ks;
    key_stuff_t         ks_left;
    key_stuff_t         ks_right;
    btree_raw_mem_node_t   *n_child;
    int                 x;

    if (n == NULL) {
        fprintf(f, "***********   ERROR: check_node: BAD NODE!!!!   **********\n");
	return;
    }
#ifdef DEBUG_STUFF
#if 0
    fprintf(stderr, "%x %s node=%p\n", (int)pthread_self(), __FUNCTION__, n);
    if(key_in_left)
    fprintf(stderr, "%x %s left [%.*s]\n", (int)pthread_self(), __FUNCTION__, keylen_in_left, key_in_left);
    if(key_in_right)
    fprintf(stderr, "%x %s right [%.*s]\n", (int)pthread_self(), __FUNCTION__, keylen_in_right, key_in_right);
    if(key_in)
    fprintf(stderr, "%x %s in [%.*s]\n", (int)pthread_self(), __FUNCTION__, keylen_in, key_in);
#endif
#endif
    if (n->flags & LEAF_NODE) {
	nkey_bytes = sizeof(node_vlkey_t);
    } else {
	if (bt->flags & SYNDROME_INDEX) {
	    nkey_bytes = sizeof(node_fkey_t);
	} else {
	    nkey_bytes = sizeof(node_vkey_t);
	}
	assert(n->rightmost != 0);
    }
    if ((bt->flags & SYNDROME_INDEX) && !(n->flags & LEAF_NODE)) {
	nfreebytes = bt->nodesize - sizeof(btree_raw_node_t) - n->nkeys*nkey_bytes;
    } else {
	nfreebytes = n->insert_ptr - sizeof(btree_raw_node_t) - n->nkeys*nkey_bytes;
    }
    assert(nfreebytes >= 0);

    for (i=0; i<n->nkeys; i++) {
        if (n->flags & LEAF_NODE) {
	    assert(get_key_stuff(bt, n, i, &ks));
	} else {
	    assert(!get_key_stuff(bt, n, i, &ks));
	}
	assert(ks.keylen < bt->nodesize);
	if (key_in_left != NULL) {
	    x = bt->cmp_cb(bt->cmp_cb_data, key_in_left, keylen_in_left, ks.pkey_val, ks.keylen);
	    if (rightmost_flag) {
		if (x != -1) {
		    check_err(f, "***********   ERROR: check_node left (right): node %p key %d out of order!!!!   **********\n", n, i);
		}
	    } else {
		if (x != -1) {
		    check_err(f, "***********   ERROR: check_node left: node %p key %d out of order!!!!   **********\n", n, i);
		}
	    }
	}

	if (key_in != NULL) {
	    x = bt->cmp_cb(bt->cmp_cb_data, key_in, keylen_in, ks.pkey_val, ks.keylen);
	    if (rightmost_flag) {
		if (x == -1) {
		    check_err(f, "***********   ERROR: check_node (right): node %p key %d out of order!!!!   **********\n", n, i);
		}
	    } else {
		if (x == -1) {
		    check_err(f, "***********   ERROR: check_node: node %p key %d out of order!!!!   **********\n", n, i);
		}
	    }
	}

	if (key_in_right != NULL) {
	    x = bt->cmp_cb(bt->cmp_cb_data, key_in_right, keylen_in_right, ks.pkey_val, ks.keylen);
	    if (rightmost_flag) {
		if (x == -1) {
		    check_err(f, "***********   ERROR: check_node right (right): node %p key %d out of order!!!!   **********\n", n, i);
		}
	    } else {
		if (x == -1) {
		    check_err(f, "***********   ERROR: check_node right: node %p key %d out of order!!!!   **********\n", n, i);
		}
	    }
	}

	if (i > 0) {
	    // make sure the keys within this node are sorted! 
	    (void) get_key_stuff(bt, n, i, &ks_left);
	    x = bt->cmp_cb(bt->cmp_cb_data, ks_left.pkey_val, ks_left.keylen, ks.pkey_val, ks.keylen);
	    if (x == -1) {
		check_err(f, "***********   ERROR: check_node internal: node %p key %d out of order!!!!   **********\n", n, i);
	    }
	}

	if (n->flags & LEAF_NODE) {
	    pvlk = ((node_vlkey_t *) n->keys) + i;
	    // purposefully empty 
	} else if (bt->flags & SECONDARY_INDEX) {
	    pvk  = ((node_vkey_t *) n->keys) + i;
	    // purposefully empty 
	} else if (bt->flags & SYNDROME_INDEX) {
	    pfk  = ((node_fkey_t *) n->keys) + i;
	    // purposefully empty 
	} else {
	    assert(0);
	}
    }

    if (!(n->flags & LEAF_NODE)) {
	btree_status_t ret = BTREE_SUCCESS;
	char  stmp[100];
	char  stmp_left[100];
	char  stmp_right[100];

	// non-leaf
	for (i=0; i<n->nkeys; i++) {
	    (void) get_key_stuff(bt, n, i, &ks);
	    n_child = get_existing_node(&ret, bt, ks.ptr); 
	    if (bt->flags & SYNDROME_INDEX) {
	        sprintf(stmp, "%p", ks.pkey_val);
		if (i == 0) {
		    if (key_in_left == NULL) {
			strcpy(stmp_left, "");
		    } else {
			strcpy(stmp_left, key_in_left);
		    }
		} else {
		    (void) get_key_stuff(bt, n, i-1, &ks_left);
		    sprintf(stmp_left, "%p", ks_left.pkey_val);
		}
		if (i == (n->nkeys-1)) {
		    if (key_in_right == NULL) {
			strcpy(stmp_right, "");
		    } else {
			strcpy(stmp_right, key_in_right);
		    }
		} else {
		    (void) get_key_stuff(bt, n, i+1, &ks_right);
		    sprintf(stmp_right, "%p", ks_right.pkey_val);
		}
		check_node(bt, f, n_child, stmp_left, strlen(stmp_left), stmp, strlen(stmp), stmp_right, strlen(stmp_right), 0 /* right */);
	    } else {
		if (i == 0) {
		    if (key_in_left != NULL) {
			ks_left.pkey_val = key_in_left;
			ks_left.keylen   = keylen_in_left;
		    } else {
			ks_left.pkey_val = NULL;
			ks_left.keylen   = 0;
		    }
		} else {
		    (void) get_key_stuff(bt, n, i-1, &ks_left);
		}
		if (i == (n->nkeys-1)) {
		    ks_right.pkey_val = key_in_right;
		    ks_right.keylen   = keylen_in_right;
		} else {
		    (void) get_key_stuff(bt, n, i+1, &ks_right);
		}
		check_node(bt, f, n_child, ks_left.pkey_val, ks_left.keylen, ks.pkey_val, ks.keylen, ks_right.pkey_val, ks_right.keylen, 0 /* right */);
	    }
	}
	if (n->rightmost != 0) {
	    n_child = get_existing_node(&ret, bt, n->rightmost); 
	    if (n->nkeys == 0) {
	        //  this can only happen for the root!
	        assert(n->logical_id == bt->rootid);
		check_node(bt, f, n_child, NULL, 0, NULL, 0, NULL, 0, 1 /* right */);
	    } else {
		(void) get_key_stuff(bt, n, n->nkeys-1, &ks_left);
		check_node(bt, f, n_child, ks_left.pkey_val, ks_left.keylen, key_in_right, keylen_in_right, NULL, 0, 1 /* right */);
	    }
	}
    }
}

#ifdef BTREE_RAW_CHECK
static
void btree_raw_check(btree_raw_t *bt, char* func, char* key)
{
    btree_status_t     ret = BTREE_SUCCESS;
    btree_raw_mem_node_t  *n;

    plat_rwlock_wrlock(&bt->lock);

#ifdef DEBUG_STUFF
	fprintf(stderr, "BTREE_CHECK %x %s btree %p key %s lock %p BEGIN\n", (int)pthread_self(), func, bt, key, &bt->lock);
#endif
    n = get_existing_node(&ret, bt, bt->rootid); 
    if (BTREE_SUCCESS != ret || (n == NULL)) {
	check_err(stderr, "*****  ERROR: btree_raw_check: Could not get root node!!!!  *****\n");
    }
    
    check_node(bt, stderr, n, NULL, 0, NULL, 0, NULL, 0, 0);

    (void)deref_l1cache(bt);

#ifdef DEBUG_STUFF
    fprintf(stderr, "BTREE_CHECK %x %s btree %p key %s lock %p END\n", (int)pthread_self(), func, bt, key, &bt->lock);
#endif

    plat_rwlock_unlock(&bt->lock);
}
#endif

//======================   TEST  =========================================

void btree_raw_test(btree_raw_t *btree)
{
    // TBD xxxzzz
}

//======================   SNAPSHOTS   =========================================

extern int btree_raw_snapshot(struct btree_raw *btree, uint64_t *seqno)
{
    // TBD xxxzzz
    return(0);
}

extern int btree_raw_delete_snapshot(struct btree_raw *btree, uint64_t seqno)
{
    // TBD xxxzzz
    return(0);
}

extern int btree_raw_get_snapshots(struct btree_raw *btree, uint32_t *n_snapshots, uint64_t *seqnos)
{
    // TBD xxxzzz
    return(0);
}

//======================   STATS   =========================================

static void
btree_raw_init_stats(struct btree_raw *btree, btree_stats_t *stats, fdf_pstats_t *pstats)
{
    memset(stats, 0, sizeof(btree_stats_t));

    /*
     * Initialize stats from recovered session
     */
    assert(pstats);
    btree->stats.stat[BTSTAT_NUM_OBJS]    = pstats->obj_count;
    btree->stats.stat[BTSTAT_NUM_SNAP_OBJS]    = pstats->num_snap_objs;
    btree->stats.stat[BTSTAT_SNAP_DATA_SIZE]    = pstats->snap_data_size;
    fprintf(stderr, "btree_raw_init_stats:BTSTAT_NUM_OBJS= %ld\n",  pstats->obj_count);
    //btree->stats.stat[BTSTAT_TOTAL_BYTES] = pstats->cntr_sz;
//  btree->stats.stat[BTSTAT_SPCOPT_BYTES_SAVED] = 0;
}


void btree_raw_get_stats(struct btree_raw *btree, btree_stats_t *stats)
{
    memcpy(stats, &(btree->stats), sizeof(btree_stats_t));

    btree->stats.stat[BTSTAT_MPUT_IO_SAVED] = 0;
}


char *btree_stat_name(btree_stat_t stat_type)
{
    return(btree_stats_strings[stat_type]);
}


void btree_dump_stats(FILE *f, btree_stats_t *stats)
{
    int j;

    fprintf(stderr, "==============================================================\n");
    for (j=0; j<N_BTSTATS; j++) {
        fprintf(stderr, "%-23s = %"PRIu64"\n", btree_stat_name(j), stats->stat[j]);
    }
    fprintf(stderr, "==============================================================\n");
}
