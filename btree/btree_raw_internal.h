#ifndef __BTREE_RAW_INTERNAL_H
#define __BTREE_RAW_INTERNAL_H

#include "btree_raw.h"
#include "platform/rwlock.h"
#include <assert.h>
#include "fdf.h"
#include <api/fdf.h>


#define BAD_CHILD       0
#define META_LOGICAL_ID 0x8000000000000000L

typedef struct node_vkey {
    uint32_t    keylen;
    uint32_t    keypos;
    uint64_t    ptr;
    uint64_t    seqno;
} node_vkey_t;

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
typedef struct fdf_pstats_delta_ {
    uint64_t      seq_num;
    uint64_t      delta_obj_count;
    bool          is_positive_delta;
    uint64_t      seq;
} fdf_pstats_delta_t;

typedef struct btree_raw_node {
    /*
     * stats field should be the first field
     * in this structure. Recovery of persistent stats
     * assumes that stats are present in first field of
     * btree node.
     */
    fdf_pstats_delta_t     pstats;

    uint32_t      flags;
    uint64_t      logical_id;
    uint64_t      lsn;
    uint32_t      checksum;
    uint32_t      insert_ptr;
    uint32_t      nkeys;
    uint64_t      prev;
    uint64_t      next;
    uint64_t      rightmost;
    node_key_t    keys[0];
} btree_raw_node_t;

typedef struct btree_raw_mem_node btree_raw_mem_node_t;

#define	NODE_DIRTY		0x1
#define NODE_DELETED	0x2

#define	mark_node_dirty(n)		((n)->flag |= (char)NODE_DIRTY)
#define mark_node_clean(n)		((n)->flag &= (char)(~((char)NODE_DIRTY)))
#define is_node_dirty(n)		((n)->flag & NODE_DIRTY)
#define	mark_node_deleted(n)	((n)->flag |= (char)NODE_DELETED)
#define	is_node_deleted(n)		((n)->flag & NODE_DELETED)


//#define DEBUG_STUFF
struct btree_raw_mem_node {
	char     flag;
	uint64_t modified;
#ifdef DEBUG_STUFF
	uint64_t last_dump_modified;
	pthread_t lock_id;
#endif
	plat_rwlock_t lock;
	btree_raw_mem_node_t *next; // dirty list
	btree_raw_node_t *pnode;
};

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
	btree_raw_mem_node_t    **dir_nodes;
	btree_raw_mem_node_t    **del_nodes;
	int						*dir_written, *del_written;
	int                     ret;
	int                     dir_count, del_count;
	int                     dir_index, del_index, total_flush, ref_count;
	pthread_cond_t			ret_condvar;
}btSyncRequest_t;

typedef struct btree_raw {
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
    uint64_t           last_flushed_seq_num;
    uint64_t           pstats_modified;
    fdf_pstats_t      *pstats; 
} btree_raw_t;

typedef struct btree_raw_persist {
    btree_raw_node_t n; // this must be first member
    uint64_t    rootid,
                logical_id_counter,next_logical_id;
} btree_raw_persist_t;

int get_key_stuff(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_t *pks);
int 
get_key_stuff_info2(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_info_t *key_info);
btree_status_t get_leaf_data_index(btree_raw_t *bt, btree_raw_node_t *n, int index, char **data, uint64_t *datalen, uint32_t meta_flags, int ref);
btree_status_t get_leaf_key_index(btree_raw_t *bt, btree_raw_node_t *n, int index, char **key, uint32_t *keylen, uint32_t meta_flags);
btree_raw_mem_node_t *get_existing_node_low(btree_status_t *ret, btree_raw_t *btree, uint64_t logical_id, int ref);
btree_raw_mem_node_t *get_existing_node(btree_status_t *ret, btree_raw_t *btree, uint64_t logical_id);
int is_leaf(btree_raw_t *btree, btree_raw_node_t *node);
int is_overflow(btree_raw_t *btree, btree_raw_node_t *node);
  
btree_status_t btree_recovery_process_minipkt(btree_raw_t *bt,
                               btree_raw_node_t **onodes, uint32_t on_cnt, 
                               btree_raw_node_t **nnodes, uint32_t nn_cnt);

void deref_l1cache_node(btree_raw_t* btree, btree_raw_mem_node_t *node);
btree_raw_mem_node_t* root_get_and_lock(btree_raw_t* btree, int write_lock);
void free_buffer(btree_raw_t *btree, void* buf);
char *get_buffer(btree_raw_t *btree, uint64_t nbytes);

btree_status_t deref_l1cache(btree_raw_t *btree);

bool 
btree_raw_find(struct btree_raw *btree, char *key, uint32_t keylen, uint64_t syndrome, btree_metadata_t *meta, btree_raw_mem_node_t** node, int write_lock, int* pathcnt);

void ref_l1cache(btree_raw_t *btree, btree_raw_mem_node_t *n);
void unlock_and_unreference();
#define unlock_and_unreference_all_but_last(b) unlock_and_unreference(b, 1)
#define unlock_and_unreference_all(b) unlock_and_unreference(b, 0)

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

int bsearch_key_low(btree_raw_t *bt, btree_raw_node_t *n, char *key_in,
		uint32_t keylen_in, uint64_t syndrome, int i_start, int i_end, int *found, int flags);

#define BSF_LEFT 1
#define BSF_RIGHT 2
#define BSF_MATCH 4

#ifdef DBG_PRINT
#define dbg_print(msg, ...) do { fprintf(stderr, "%x %s:%d " msg, (int)pthread_self(), __FUNCTION__, __LINE__, ##__VA_ARGS__); } while(0)
#define dbg_print_key(key, keylen, msg, ...) do { print_key_func(stderr, __FUNCTION__, __LINE__, key, keylen, msg, ##__VA_ARGS__); } while(0)
#else
#define dbg_print(msg, ...)
#define dbg_print_key(key, keylen, msg, ...)
#endif
extern void print_key_func(FILE *f, const char* func, int line, char* key, int keylen, char *msg, ...);
extern __thread uint64_t dbg_referenced;

#ifdef _OPTIMIZE
#undef assert
#define assert(a)
#endif

#endif // __BTREE_RAW_INTERNAL_H
