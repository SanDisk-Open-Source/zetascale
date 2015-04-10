#ifndef __BTREE_RAW_INTERNAL_H
#define __BTREE_RAW_INTERNAL_H

#include "btree_raw.h"
#include "platform/rwlock.h"
#include "btree_range.h"
#include <assert.h>
#include <api/zs.h>

typedef enum {
	 BT_CNTR_UNUSED,		/* cguid will be NULL for unused entries */
	 BT_CNTR_INIT,			/* Initializing */
	 BT_CNTR_OPEN,			/* Opened */
	 BT_CNTR_CLOSING,		/* Closing in progress */
	 BT_CNTR_CLOSED,		/* Closed */
	 BT_CNTR_DELETING,		/* Deletion in progress */
} BT_CNTR_STATE;

typedef struct read_node {
    ZS_cguid_t              cguid;
    uint64_t                 nodesize;
} read_node_t;

typedef struct cmap {
    char				cname[CONTAINER_NAME_MAXLEN];
    uint64_t			cguid;
    struct btree		*btree;
    int					read_by_rquery;
    read_node_t			node_data;
	bool				read_only;
	BT_CNTR_STATE		bt_state;
	int					bt_wr_count, bt_rd_count;
	int					snap_initiated;
	bool				scavenger_state;
	pthread_mutex_t		bt_snap_mutex;
	pthread_cond_t		bt_snap_wr_cv;
	pthread_cond_t		bt_snap_cv;
	pthread_rwlock_t	bt_cm_rwlock;
	uint64_t			flags;
} ctrmap_t;

#define BAD_CHILD       0
#define BTREE_VERSION   0

#define META_LOGICAL_ID_MASK          0x8000000000000000L
#define META_ROOT_LOGICAL_ID          (META_LOGICAL_ID_MASK | (1 << 0))
#define META_COUNTER_LOGICAL_ID       (META_LOGICAL_ID_MASK | (1 << 1))
#define META_SNAPSHOT_LOGICAL_ID      (META_LOGICAL_ID_MASK | (1 << 2))
#define META_TOTAL_NODES              3

typedef struct node_vkey {
    uint32_t    keylen:31;
    uint32_t    invalid:1;
    uint32_t    keypos;
    uint64_t    ptr;
    uint64_t    seqno;
} 
__attribute__ ((__packed__))
node_vkey_t;

#ifdef BIG_NODES
typedef uint32_t keylen_t;
typedef uint32_t keypos_t;
#else
typedef uint16_t keylen_t;
typedef uint16_t keypos_t;
#endif

typedef struct node_vlkey {
    keylen_t    keylen;
    keypos_t    keypos;
    uint64_t    datalen;
    uint64_t    ptr;
    uint64_t    seqno;
} 
__attribute__ ((__packed__))
node_vlkey_t;

typedef struct node_fkey {
    uint64_t    key;
    uint64_t    ptr;
    uint64_t    seqno;
} node_fkey_t;

typedef struct node_flkey {
    uint64_t    key;
    uint64_t    datalen;
    uint64_t    ptr;
    uint64_t    seqno;
} node_flkey_t;

// xxxzzz
// typedef union node_key {
//     node_fkey_t    fkey;
//     node_vkey_t    vkey;
// } node_key_t;

typedef void *node_key_t;

typedef struct key_stuff {
    int       fixed;
    int       leaf;
    uint64_t  ptr;
    uint32_t  nkey;
    uint32_t  offset;
    void     *pkey_struct;
    char     *pkey_val;
    keylen_t  keylen;
    uint64_t  datalen;
    uint32_t  fkeys_per_node;
    uint64_t  seqno;
    uint64_t  syndrome;
} key_stuff_t;


/*
 * Per node persistent stats
 */
enum delta_indx {PSTAT_OBJ_COUNT, PSTAT_NUM_SNAP_OBJS, PSTAT_SNAP_DATA_SIZE, PSTAT_OVERFLW_NODES, PSTAT_MAX_STATS=8};
typedef struct zs_pstats_delta_ {
    uint64_t		seq_num;
    //uint64_t		delta_obj_count;
    uint64_t		delta[PSTAT_MAX_STATS];
    //bool			is_positive_delta;
	char			is_pos_delta;
    uint64_t		seq;
} zs_pstats_delta_t;

typedef struct btree_raw_node {
    /*
     * stats field should be the first field
     * in this structure. Recovery of persistent stats
     * assumes that stats are present in first field of
     * btree node.
     */
    zs_pstats_delta_t     pstats;

    uint32_t      flags;
    uint16_t      level;
    uint32_t      checksum;
    uint32_t      insert_ptr;
    uint32_t      nkeys;
    uint64_t      logical_id;
//    uint64_t      lsn;
//    uint64_t      prev;
    uint64_t      next;
    uint64_t      rightmost;
    node_key_t    keys[0];
} btree_raw_node_t;


#define	NODE_DIRTY		0x1
#define NODE_DELETED	0x2

#define	mark_node_dirty(n)	((n)->flag |= (char)NODE_DIRTY)
#define mark_node_clean(n)	((n)->flag &= (char)(~((char)NODE_DIRTY)))
#define is_node_dirty(n)	((n)->flag & NODE_DIRTY)
#define	mark_node_deleted(n)	((n)->flag |= (char)NODE_DELETED)
#define mark_node_undeleted(n)  ((n)->flag &= (char)(~((char)NODE_DELETED)))          
#define	is_node_deleted(n)	((n)->flag & NODE_DELETED)


typedef struct btree_raw_mem_node btree_raw_mem_node_t;
//#define DEBUG_STUFF
struct btree_raw_mem_node {
	struct btree_raw_mem_node  *free_next; // free list   
	uint16_t datalen;
	bool     malloced;       // Is it pool alloced or malloced
	char     flag;
	uint64_t modified;
#ifdef DEBUG_STUFF
	uint64_t last_dump_modified;
	pthread_t lock_id;
#endif
	bool pinned;
	bool deref_delete_cache;
	bool cache_valid;
	plat_rwlock_t lock;
	btree_raw_mem_node_t *dirty_next; // dirty list
	btree_raw_node_t *pnode;
};


/* Memory alloc functions */
#define MEM_NODE_SIZE  (sizeof(btree_raw_mem_node_t) + get_btree_node_size())  

typedef struct {
	btree_raw_mem_node_t *head;
	uint64_t size;
	uint64_t n_entries;
	uint64_t n_free_entries;
	pthread_mutex_t mem_mgmt_lock;

#ifdef MEM_SIZE_DEBUG
	uint64_t n_threshold_entries;
	uint64_t min_free_entries;
#endif
} btree_node_list_t;

btree_node_list_t *btree_node_list_init(uint64_t n_entries, uint64_t size);
btree_status_t btree_node_list_alloc(btree_node_list_t *l, uint64_t n_entries, uint64_t size);
btree_raw_mem_node_t *btree_node_alloc(btree_node_list_t *l);
void btree_node_free(btree_raw_mem_node_t *mnode);
void btree_node_free2(btree_node_list_t *l, btree_raw_mem_node_t *mnode);

/************** Snapshot related structures *****************/
#define SNAP_VERSION1			0x98760001
#define SNAP_VERSION			SNAP_VERSION1

#define SNAP_DELETED		0x01
typedef struct __attribute__((__packed__)) btree_snap_info_v1 {
	uint64_t 	seqno;
	uint64_t 	timestamp;
} btree_snap_info_v1_t;

typedef struct __attribute__((__packed__)) btree_snap_meta_v1 {
	btree_snap_info_v1_t snapshots[0];
} btree_snap_meta_v1_t;


typedef enum sc_status {
	SC_STALE_ENT 		= 0x01,	//Purge duplicate/stale entries
	SC_OVERFLW_DELCONT	= 0x02,	//Purge Overflow nodes and delete container
} scs_t;
typedef struct __attribute__((__packed__)) btree_snap_meta {
	btree_raw_node_t                n_hdr; // this must be first member

	uint32_t			snap_version;
	uint32_t			max_snapshots;
	uint32_t			total_snapshots;
	uint32_t			sc_status;
	union {
		btree_snap_meta_v1_t	v1_meta;
	} meta;
} btree_snap_meta_t;

#define BTREE_RAW_L1CACHE_LIST_MAX 10000
#define BT_SYNC_THREADS				32

typedef struct btSyncThread {
	uint32_t				id;
	pthread_t				pthread;
	void					(*startfn)(uint64_t arg);
	pthread_mutex_t			mutex;
	pthread_cond_t			condvar;
	uint64_t				rv_wait;
	uint32_t				is_waiting;
	uint32_t				do_resume;
	struct btSyncThread		*next;
	struct btSyncThread		*prev;
} btSyncThread_t;

struct btree_raw;

typedef struct btSyncRequest {
	struct btSyncRequest	*next, *prev;
//	struct btree_raw        *bt;
	btree_raw_node_t    **dir_nodes;
	btree_raw_mem_node_t    **del_nodes;
	int						*dir_written, *del_written;
	int                     ret;
	int                     dir_count, del_count;
	int                     dir_index, del_index, total_flush, ref_count;
	pthread_cond_t			ret_condvar;
} btSyncRequest_t;

typedef struct pstat_ckpt_info {
	int64_t *active_writes;
	zs_pstats_t pstat;
} pstat_ckpt_info_t;

typedef struct btree_raw {
    uint64_t           version;
    uint32_t           n_partition;
    uint32_t           n_partitions;
    uint32_t           flags;
    uint32_t           max_key_size;
    uint32_t           min_keys_per_node;
    uint32_t           nodesize;
    uint32_t           nodesize_less_hdr;
    uint32_t           big_object_size;
    uint32_t           fkeys_per_node;
    uint64_t           logical_id_counter;
    uint64_t           rootid;
    uint32_t           n_l1cache_buckets;
    struct PMap       *l1cache;
    read_node_cb_t    *read_node_cb;
    void              *read_node_cb_data;
    write_node_cb_t   *write_node_cb;
    void              *write_node_cb_data;
    flush_node_cb_t   *flush_node_cb;
    void              *flush_node_cb_data;
    freebuf_cb_t      *freebuf_cb;
    void              *freebuf_cb_data;
    create_node_cb_t  *create_node_cb;
    void              *create_node_cb_data;
    delete_node_cb_t  *delete_node_cb;
    void              *delete_node_cb_data;
    log_cb_t          *log_cb;
    void              *log_cb_data;
    msg_cb_t          *msg_cb;
    void              *msg_cb_data;
    cmp_cb_t          *cmp_cb;
    void              *cmp_cb_data;

    bt_mput_cmp_cb_t   mput_cmp_cb;
    void              *mput_cmp_cb_data;

    trx_cmd_cb_t      *trx_cmd_cb;
    bool               trxenabled;
    seqno_alloc_cb_t  *seqno_alloc_cb;
    btree_stats_t      stats;

    plat_rwlock_t      lock;

    uint64_t           modified;
    uint64_t           cguid;
    uint64_t           next_logical_id;
    uint32_t           no_sync_threads;
    pthread_mutex_t    bt_async_mutex;
    pthread_cond_t     bt_async_cv;
    btSyncThread_t   **syncthread;
    btSyncRequest_t   *sync_first,
                      *sync_last;
    int                deleting;
    int                io_threads, io_bufs, worker_threads;

    /*
     * Persistent stats related variables
     */
    pthread_mutex_t    pstat_lock;
    uint64_t           last_flushed_seq_num;
    uint64_t           pstats_modified;
    zs_pstats_t       pstats; 

    pstat_ckpt_info_t pstat_ckpt;
    uint64_t current_active_write_idx;
    int64_t active_writes[2]; // Current and in-checkpoint active writes counter


    /* Snapshot related variables */
    pthread_rwlock_t     snap_lock;
    btree_raw_mem_node_t *snap_mnode;
    btree_snap_meta_t    *snap_meta;
} btree_raw_t;

#define META_VERSION1	0x88880001
#define META_VERSION	META_VERSION1

typedef struct btree_raw_persist {
    btree_raw_node_t      n; // this must be first member
    uint32_t              meta_version;
    uint64_t              rootid;
    uint64_t              logical_id_counter;
} btree_raw_persist_t;

typedef enum {
	FLUSH_NOT_NEEDED,
	FLUSH_ROOT_CHANGED,
	FLUSH_COUNTER_INTERVAL,
	FLUSH_SNAPSHOT,
	FLUSH_PSTATS,
	FLUSH_FORCE,
} flush_persist_type_t;

btree_status_t btree_snap_init(btree_raw_t *bt, bool create);
btree_status_t btree_snap_create_meta(btree_raw_t *bt, uint64_t seqno);
btree_status_t btree_snap_delete_meta(btree_raw_t *bt, uint64_t seqno);
int btree_snap_find_meta_index(btree_raw_t *bt, uint64_t seqno);
btree_status_t btree_snap_get_meta_list(btree_raw_t *bt, uint32_t *n_snapshots,
 		                             ZS_container_snapshots_t **snap_seqs);
bool btree_snap_seqno_in_snap(btree_raw_t *bt, uint64_t seqno);
 
btree_status_t savepersistent(btree_raw_t *bt, flush_persist_type_t flush_type, bool flush_now);

int get_key_stuff(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_t *pks);
int 
get_key_stuff_info2(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_info_t *key_info);

/* NODE Flags */
typedef enum {
	NODE_REF                = 1<<0,
	NODE_PIN                = 1<<1,
	NODE_CACHE_VALIDATE     = 1<<2,
	NODE_CACHE_DEREF_DELETE = 1<<3,
	NODE_RAW_OBJ            = 1<<4,
} getnode_flags_t;

typedef enum {
	LOCKTYPE_NOLOCK,
	LOCKTYPE_WRITE,
	LOCKTYPE_READ,
	LOCKTYPE_LEAF_WRITE_REST_READ
} bt_locktype_t;

btree_status_t get_leaf_data_index(btree_raw_t *bt, btree_raw_node_t *n, int index, char **data, uint64_t *datalen, uint32_t meta_flags, int ref, bool deref_delete_cache);
btree_status_t get_leaf_key_index(btree_raw_t *bt, btree_raw_node_t *n, int index, char **key, uint32_t *keylen, uint32_t meta_flags, key_stuff_info_t *pks);
btree_raw_mem_node_t *get_existing_node_low(btree_status_t *ret, btree_raw_t *btree, uint64_t logical_id, getnode_flags_t flags);
btree_raw_mem_node_t *get_existing_node(btree_status_t *ret, btree_raw_t *btree, uint64_t logical_id, getnode_flags_t flags, bt_locktype_t locktype_in);
btree_raw_mem_node_t *get_existing_overflow_node(btree_status_t *ret, btree_raw_t *btree, uint64_t logical_id, getnode_flags_t flags);
btree_raw_mem_node_t *get_existing_overflow_node_for_delete(btree_status_t *ret, btree_raw_t *btree, uint64_t logical_id, getnode_flags_t flags);
int is_leaf(btree_raw_t *btree, btree_raw_node_t *node);
int is_overflow(btree_raw_t *btree, btree_raw_node_t *node);
void delete_key_by_index_non_leaf(btree_status_t* ret, btree_raw_t *btree, btree_raw_mem_node_t *node, int index);
void delete_key_by_index_leaf(btree_status_t* ret, btree_raw_t *btree, btree_raw_mem_node_t *node, int index);
void delete_key_by_index(btree_status_t* ret, btree_raw_t *btree, btree_raw_mem_node_t *node, int index);
  
int is_leaf_minimal_after_delete(btree_raw_t *btree, btree_raw_node_t *n, int index);
btree_status_t btree_recovery_process_minipkt(btree_raw_t *bt,
                               btree_raw_node_t **onodes, uint32_t on_cnt, 
                               btree_raw_node_t **nnodes, uint32_t nn_cnt);

void deref_l1cache_node(btree_raw_t* btree, btree_raw_mem_node_t *node);
void deref_l1cache_node_all(btree_raw_t* btree, btree_raw_mem_node_t *node);
btree_raw_mem_node_t* root_get_and_lock(btree_raw_t* btree, int write_lock, btree_status_t *ret);
void free_buffer(btree_raw_t *btree, void* buf);
char *get_buffer(btree_raw_t *btree, uint64_t nbytes);

btree_status_t deref_l1cache(btree_raw_t *btree);

int
btree_raw_find(btree_status_t *ret, struct btree_raw *btree, char *key, uint32_t keylen,
               uint64_t syndrome, btree_metadata_t *meta, btree_raw_mem_node_t** node,
               int write_lock, int* pathcnt, bool *found);

int seqno_cmp_range(btree_metadata_t *smeta, uint64_t key_seqno,
                 bool *exact_match, bool *range_match);

int scavenge_node(struct btree_raw *btree, btree_raw_mem_node_t* node, key_stuff_info_t *ks_prev_key, key_stuff_info_t **key);

void ref_l1cache(btree_raw_t *btree, btree_raw_mem_node_t *n);
void unlock_and_unreference();
#define unlock_and_unreference_all_but_last(b) unlock_and_unreference(b, 1)
#define unlock_and_unreference_all(b) unlock_and_unreference(b, 0)

/* Info about any storage error happened */
typedef struct {
	btree_raw_t          *btree;
	uint64_t             cguid;
	uint64_t             logical_id;
	uint32_t             size;
	uint32_t             op_type;

	union {
		struct {
			char             *key;
			uint32_t         keylen;
			btree_metadata_t meta;
		} single;

		struct {
			char               *key_start;
			uint32_t           keylen_start;
			char               *key_end;
			uint32_t           keylen_end;
			btree_range_meta_t rmeta;
		} rquery;

		struct {
			char             *key;
			uint32_t         keylen;
			btree_metadata_t meta;
		} rupdate;
	} u;
} btree_op_err_t;

typedef struct {
	btree_op_err_t       *err;            // Current error info getting rescue'd
	uint64_t             err_logical_id;  // Logical id which failed

	btree_raw_node_t     *node;           // Info on parent key to rescue
	char                 *key;
	uint32_t             keylen;
	uint64_t             seqno;
	bool                 rightmost;
	int                  idx;
} btree_op_err_rescue_t;

enum {
	ERR_OPTYPE_UNKNOWN,
	ERR_OPTYPE_SINGLE,
	ERR_OPTYPE_RQUERY,
	ERR_OPTYPE_RUPDATE
};

void set_lasterror(btree_raw_t *btree, uint64_t err_logical_id);
uint64_t get_lasterror(btree_raw_t *btree);
void set_lasterror_single(btree_raw_t *btree, char *key, uint32_t keylen, btree_metadata_t *meta);
void set_lasterror_rquery(btree_raw_t *btree, btree_range_meta_t *rmeta);
void set_lasterror_rupdate(btree_raw_t *btree, char *key, uint32_t keylen, btree_metadata_t *meta);
void reset_lasterror(btree_raw_t *btree);

btree_op_err_rescue_t *btree_raw_get_cur_rescue(btree_raw_t *btree);
void btree_raw_exit_rescue(btree_raw_t *btree);
bool btree_in_rescue_mode(btree_raw_t *btree);
void add_to_rescue(btree_raw_t *btree, btree_raw_node_t *parent, uint64_t err_logical_id, int idx);
ZS_cguid_t btree_raw_get_cguid_from_op_err(void *context);
btree_status_t btree_raw_rescue(btree_raw_t *btree, void *context);

#ifdef BTREE_UNDO_TEST
enum {
	BTREE_IOCTL_RECOVERY=1,
};

#define BTREE_IOCTL_RECOVERY_COLLECT_1    1
#define BTREE_IOCTL_RECOVERY_COLLECT_2    2
#define BTREE_IOCTL_RECOVERY_START        3

btree_status_t btree_recovery_ioctl(struct btree_raw *bt, uint32_t ioctl_type, void *data);

void btree_rcvry_test_collect(btree_raw_t *bt, btree_raw_node_t *node);
void btree_rcvry_test_delete(btree_raw_t *bt, btree_raw_node_t *node);
void btree_rcvry_test_recover(btree_raw_t *bt);
#endif

#ifdef FLIP_ENABLED
extern bool recovery_write;
#endif
extern int bt_storm_mode, overflow_node_ratio;
extern uint64_t overflow_node_sz;
extern uint64_t datasz_in_overflow;

#define BT_USE_RAWOBJ(flag)			((bt_storm_mode && ((flag) & OVERFLOW_NODE))? 1 : 0)
#define BT_GET_L1CACHE(robj)		((robj) ? global_raw_l1cache : global_l1cache)	
#define BT_GET_L1CACHE_NODE(pnode)	(BT_GET_L1CACHE(BT_USE_RAWOBJ((pnode)->flags)))

static inline int key_idx(struct btree_raw *btree, btree_raw_node_t* node, node_key_t* key)
{
	int size = sizeof(node_fkey_t);

	if(node->flags & LEAF_NODE)
		size = sizeof(node_vlkey_t);
	else if(btree->flags & SECONDARY_INDEX)
		size = sizeof(node_vkey_t);

	return ((void*)key - (void*)node->keys) / size;
}

static inline node_key_t* key_offset(struct btree_raw *btree, btree_raw_node_t* node, int nkey)
{
	if(node->flags & LEAF_NODE)
		return (node_key_t*)(((node_vlkey_t *) node->keys) + nkey);
	else if(btree->flags & SECONDARY_INDEX)
		return (node_key_t*)(((node_vkey_t *) node->keys) + nkey);

	return ((node_key_t*)((node_vkey_t *) node->keys) + nkey);
}

inline static
void get_key_val(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, char** key, int* keylen)
{
	if (n->flags & LEAF_NODE) {
		node_vlkey_t* pvlk = ((node_vlkey_t *) n->keys) + nkey;
		*key               = (char *) n + pvlk->keypos;
		*keylen            = pvlk->keylen;
	}
	else if (bt->flags & SECONDARY_INDEX) {
		node_vkey_t* pvk   = ((node_vkey_t *) n->keys) + nkey;
		*key      = (char *) n + pvk->keypos;
		*keylen        = pvk->keylen;
	} else if (bt->flags & SYNDROME_INDEX) {
		node_fkey_t *pfk   = ((node_fkey_t *) n->keys) + nkey;
		*key      = (char *) (pfk->key);
		*keylen        = sizeof(uint64_t);
	} else {
		assert(0);
	}
}

int bsearch_key_low(btree_raw_t *bt, btree_raw_node_t *n,
                    char *key_in, uint32_t keylen_in,
                    btree_metadata_t *meta, uint64_t syndrome,
                    int i_start, int i_end, int flags,
                    bool *found);

#define BSF_LATEST  1
#define BSF_OLDEST  2
#define BSF_NEXT    4
#define BSF_MATCH   8

#ifdef DBG_PRINT
#define dbg_print(msg, ...) do { fprintf(stderr, "%x %s:%d " msg, (int)pthread_self(), __FUNCTION__, __LINE__, ##__VA_ARGS__); } while(0)
#define dbg_print_key(key, keylen, msg, ...) do { print_key_func(stderr, __FUNCTION__, __LINE__, key, keylen, msg, ##__VA_ARGS__); } while(0)
#else
#define dbg_print(msg, ...) do { } while(0)
#define dbg_print_key(key, keylen, msg, ...) do { } while(0)
#endif
extern void print_key_func(FILE *f, const char* func, int line, char* key, int keylen, char *msg, ...);
extern __thread uint64_t dbg_referenced;

#ifdef _OPTIMIZE
#undef assert
#define assert(a)
#endif

#define storage_error(r) (((r) == BTREE_FLASH_EINCONS) || ((r) == BTREE_FLASH_EIO))
#if 0
bool 
btree_raw_node_check(struct btree_raw *btree, btree_raw_node_t *node,
		  char *prev_anchor_key, uint32_t prev_anchor_keylen,
		  char *next_anchor_key, uint32_t next_anchor_keylen);

bool
btree_raw_check_node_subtree(struct btree_raw *btree, btree_raw_node_t *node,
			  char *prev_anchor_key, uint32_t prev_anchor_keylen,
			  char *next_anchor_key, uint32_t next_anchor_keylen);

bool
btree_raw_check(struct btree_raw *btree);
#endif 
void node_lock(btree_raw_mem_node_t* node, int write_lock);
void node_unlock(btree_raw_mem_node_t* node);
void delete_overflow_data(btree_status_t *ret, btree_raw_t *bt, btree_raw_node_t *leaf, uint64_t ptr_in, uint64_t datalen);
uint64_t get_data_in_overflownode(btree_raw_t *bt);
#endif // __BTREE_RAW_INTERNAL_H
