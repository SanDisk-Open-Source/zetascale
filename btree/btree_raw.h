/************************************************************************
 * 
 *  btree_raw.h  Jan. 21, 2013   Brian O'Krafka
 * 
 *  Low-level btree code for a single-threaded b-tree implementation.
 * 
 * xxxzzz NOTES:
 *     - check all uses of "ret"
 *     - make sure that all btree updates are logged!
 *     - add doxygen comments for all functions
 *     - make asserts a compile time option
 *     - make sure that left/right node pointers are properly maintained
 *     - check insert_ptr arithmetic
 *     - optimize key search within a node?
 * 
 ************************************************************************/

#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdbool.h>
#include <api/zs.h>
//#include "btree_raw_internal.h"

#ifndef __BTREE_RAW_H
#define __BTREE_RAW_H

typedef enum btree_status {
	BTREE_SUCCESS = 0,                /* Fine & Dandy */
	BTREE_FAILURE = 1,                /* Generic failure */
	BTREE_FAILURE_GENERIC,
	BTREE_FAILURE_CONTAINER_GENERIC,
	BTREE_FAILURE_CONTAINER_NOT_OPEN,
	BTREE_FAILURE_INVALID_CONTAINER_TYPE,
	BTREE_INVALID_PARAMETER,
	BTREE_CONTAINER_UNKNOWN,
	BTREE_UNPRELOAD_CONTAINER_FAILED,
	BTREE_CONTAINER_EXISTS,
	BTREE_SHARD_NOT_FOUND,
	BTREE_OBJECT_UNKNOWN,
	BTREE_OBJECT_EXISTS,
	BTREE_OBJECT_TOO_BIG,
	BTREE_FAILURE_STORAGE_READ,
	BTREE_FAILURE_STORAGE_WRITE,
	BTREE_FAILURE_MEMORY_ALLOC,
	BTREE_LOCK_INVALID_OP,
	BTREE_ALREADY_UNLOCKED,
	BTREE_ALREADY_READ_LOCKED,
	BTREE_ALREADY_WRITE_LOCKED,
	BTREE_OBJECT_NOT_CACHED,
	BTREE_SM_WAITING,
	BTREE_TOO_MANY_OPIDS,
	BTREE_TRANS_CONFLICT,
	BTREE_PIN_CONFLICT,
	BTREE_OBJECT_DELETED,
	BTREE_TRANS_NONTRANS_CONFLICT,
	BTREE_ALREADY_READ_PINNED,
	BTREE_ALREADY_WRITE_PINNED,
	BTREE_TRANS_PIN_CONFLICT,
	BTREE_PIN_NONPINNED_CONFLICT,
	BTREE_TRANS_FLUSH,
	BTREE_TRANS_LOCK,
	BTREE_TRANS_UNLOCK,
	BTREE_UNSUPPORTED_REQUEST,
	BTREE_UNKNOWN_REQUEST,
	BTREE_BAD_PBUF_POINTER,
	BTREE_BAD_PDATA_POINTER,
	BTREE_BAD_SUCCESS_POINTER,
	BTREE_NOT_PINNED,
	BTREE_NOT_READ_LOCKED,
	BTREE_NOT_WRITE_LOCKED,
	BTREE_PIN_FLUSH,
	BTREE_BAD_CONTEXT,
	BTREE_IN_TRANS,
	BTREE_NONCACHEABLE_CONTAINER,
	BTREE_OUT_OF_CONTEXTS,
	BTREE_INVALID_RANGE,
	BTREE_OUT_OF_MEM,
	BTREE_NOT_IN_TRANS,
	BTREE_TRANS_ABORTED,
	BTREE_FAILURE_MBOX,
	BTREE_FAILURE_MSG_ALLOC,
	BTREE_FAILURE_MSG_SEND,
	BTREE_FAILURE_MSG_RECEIVE,
	BTREE_ENUMERATION_END,
	BTREE_BAD_KEY,
	BTREE_FAILURE_CONTAINER_OPEN,
	BTREE_BAD_PEXPTIME_POINTER,
	BTREE_BAD_PINVTIME_POINTER,
	BTREE_BAD_PSTAT_POINTER,
	BTREE_BAD_PPCBUF_POINTER,
	BTREE_BAD_SIZE_POINTER,
	BTREE_EXPIRED,
	BTREE_EXPIRED_FAIL,
	BTREE_PROTOCOL_ERROR,
	BTREE_TOO_MANY_CONTAINERS,
	BTREE_STOPPED_CONTAINER,
	BTREE_GET_METADATA_FAILED,
	BTREE_PUT_METADATA_FAILED,
	BTREE_GET_DIRENTRY_FAILED,
	BTREE_EXPIRY_GET_FAILED,
	BTREE_EXPIRY_DELETE_FAILED,
	BTREE_EXIST_FAILED,
	BTREE_NO_PSHARD,
	BTREE_SHARD_DELETE_SERVICE_FAILED,
	BTREE_START_SHARD_MAP_ENTRY_FAILED,
	BTREE_STOP_SHARD_MAP_ENTRY_FAILED,
	BTREE_DELETE_SHARD_MAP_ENTRY_FAILED,
	BTREE_CREATE_SHARD_MAP_ENTRY_FAILED,
	BTREE_FLASH_DELETE_FAILED,
	BTREE_FLASH_EPERM,
	BTREE_FLASH_ENOENT,
	BTREE_FLASH_EIO,
	BTREE_FLASH_EAGAIN,
	BTREE_FLASH_ENOMEM,
	BTREE_FLASH_EDATASIZE,
	BTREE_FLASH_EBUSY,
	BTREE_FLASH_EEXIST,
	BTREE_FLASH_EACCES,
	BTREE_FLASH_EINVAL,
	BTREE_FLASH_EMFILE,
	BTREE_FLASH_ENOSPC,
	BTREE_FLASH_ENOBUFS,
	BTREE_FLASH_EDQUOT,
	BTREE_FLASH_STALE_CURSOR,
	BTREE_FLASH_EDELFAIL,
	BTREE_FLASH_EINCONS,
	BTREE_STALE_LTIME,
	BTREE_WRONG_NODE,
	BTREE_UNAVAILABLE,
	BTREE_TEST_FAIL,
	BTREE_TEST_CRASH,
	BTREE_VERSION_CHECK_NO_PEER,
	BTREE_VERSION_CHECK_BAD_VERSION,
	BTREE_VERSION_CHECK_FAILED,
	BTREE_META_DATA_VERSION_TOO_NEW,
	BTREE_META_DATA_INVALID,
	BTREE_BAD_META_SEQNO,
	BTREE_BAD_LTIME,
	BTREE_LEASE_EXISTS,
	BTREE_BUSY,
	BTREE_SHUTDOWN,
	BTREE_TIMEOUT,
	BTREE_NODE_DEAD,
	BTREE_SHARD_DOES_NOT_EXIST,
	BTREE_STATE_CHANGED,
	BTREE_NO_META,
	BTREE_TEST_MODEL_VIOLATION,
	BTREE_REPLICATION_NOT_READY,
	BTREE_REPLICATION_BAD_TYPE,
	BTREE_REPLICATION_BAD_STATE,
	BTREE_NODE_INVALID,
	BTREE_CORRUPT_MSG,
	BTREE_QUEUE_FULL,
	BTREE_RMT_CONTAINER_UNKNOWN,
	BTREE_FLASH_RMT_EDELFAIL,
	BTREE_LOCK_RESERVED,
	BTREE_KEY_TOO_LONG,
	BTREE_NO_WRITEBACK_IN_STORE_MODE,
	BTREE_WRITEBACK_CACHING_DISABLED,
	BTREE_UPDATE_DUPLICATE,
	BTREE_FAILURE_CONTAINER_TOO_SMALL,
	BTREE_CONTAINER_FULL,
	BTREE_CANNOT_REDUCE_CONTAINER_SIZE,
	BTREE_CANNOT_CHANGE_CONTAINER_SIZE,
	BTREE_OUT_OF_STORAGE_SPACE,
	BTREE_TRANS_LEVEL_EXCEEDED,
	BTREE_FAILURE_NO_TRANS,
	BTREE_CANNOT_DELETE_OPEN_CONTAINER,
	BTREE_FAILURE_INVALID_KEY_SIZE,
	BTREE_FAILURE_OPERATION_DISALLOWED,
	BTREE_FAILURE_ILLEGAL_CONTAINER_ID,
	BTREE_FAILURE_CONTAINER_NOT_FOUND,
	BTREE_UNLIMITED_CONTAINER_MUST_BE_NON_EVICTING,
	BTREE_THREAD_CONTEXT_BUSY,
	BTREE_LICENSE_CHK_FAILED,
	BTREE_CONTAINER_OPEN,
	BTREE_FAILURE_INVALID_CONTAINER_SIZE,
	BTREE_FAILURE_INVALID_CONTAINER_STATE,
	BTREE_FAILURE_CONTAINER_DELETED,
	BTREE_QUERY_DONE,
	BTREE_FAILURE_CANNOT_CREATE_METADATA_CACHE,
	BTREE_WARNING,		
	BTREE_BUFFER_TOO_SMALL,		/* Give me more */
	BTREE_INVALID_QUERY,		/* Correct questions please */
	BTREE_KEY_NOT_FOUND,		/* Cannot find key */
	BTREE_FAIL_TXN_START,		/* Failed to start transaction */
	BTREE_FAIL_TXN_COMMIT,		/* Failed to commit transaction */
	BTREE_FAIL_TXN_ROLLBACK,	/* Failed to rollback transaction */
	BTREE_OPERATION_DISALLOWED,	/* Shutdown in progress */
	BTREE_QUERY_PAUSED,             /* Range query passed by callback */
	BTREE_RANGE_UPDATE_NEEDS_SPACE,	/* Range update need space for update */
	BTREE_UNKNOWN_STATUS,
	BTREE_SKIPPED,                    /* Generic skip return */
	BTREE_TOO_MANY_SNAPSHOTS,         /* Too many snapshots, cannot create more */
	BTREE_PARENT_FULL_FOR_SPLIT,	  /* Parent is full and need split before next split of leaf */
	BTREE_NO_NODE_REFS,		  /* No more reference availabed for modified nodes. */
	BTREE_RESCUE_INVALID_REQUEST,     /* Context provided for rescue is invalid */
	BTREE_RESCUE_NOT_NEEDED,          /* Btree rescued either by other request or naturally */
	BTREE_RESCUE_IO_ERROR,            /* Rescue faced a media error or storage error */
} btree_status_t;

typedef enum node_flags {
    UNKNOWN_NODE	= 1,
    LEAF_NODE		= 2,
    OVERFLOW_NODE	= 4, 
} node_flags_t;

typedef enum btree_flags {
    SYNDROME_INDEX  = 1,
    SECONDARY_INDEX = 2,
    VERBOSE_DEBUG   = 8,
    RELOAD          = 16,
} btree_flags_t; 

typedef enum btree_log_events {
    BTREE_CREATE_NODE = 1,
    BTREE_UPDATE_NODE,
    BTREE_DELETE_NODE
} btree_log_events_t;

/* This space overlaps with btree_range_flags_t.
 * Hence please modify this structure and btree_range_flags_t
 * in unison, so that all entries in both structure are unique */
typedef enum btree_meta_flags {
    BUFFER_PROVIDED      = 1<<0,
    ALLOC_IF_TOO_SMALL   = 1<<1,
    OLD_SEQNO_MUST_MATCH = 1<<2,
    UPDATE_USE_SEQNO     = 1<<3,
    READ_SEQNO_EQ        = 1<<4,
    READ_SEQNO_LE        = 1<<5,
    READ_SEQNO_GT_LE     = 1<<6,
    INPLACE_POINTERS     = 1<<18,
    INSERT_TOMBSTONE     = 1<<19,
    FORCE_DELETE         = 1<<20,
    DELETE_INTERIOR_ENTRY = 1<<21,
} btree_meta_flags_t;

typedef struct btree_metadata {
    uint32_t     flags;
    uint32_t     keybuf_size;
    uint64_t     databuf_size;
    uint64_t     seqno;
    uint64_t     start_seqno;
    uint64_t     end_seqno;
    uint64_t     checksum;
    uint64_t     logical_id;
    int          index;
} btree_metadata_t;

#define BTREE_MAX_DATA_SIZE_SUPPORTED           (25 * 1024 * 1024)   // 25 MB

#define BTREE_DEFAULT_NODE_SIZE	        8100
#define BTREE_STORM_MODE_NODE_SIZE   	16200
#define BTREE_MAX_NODE_SIZE             32768

#define BTSTATS_ITEMS() \
/* 0 */		item(BTSTAT_L1ENTRIES, =0) \
			item(BTSTAT_L1OBJECTS, /* default */) \
			item(BTSTAT_LEAF_L1HITS, /* default */) \
			item(BTSTAT_NONLEAF_L1HITS, /* default */) \
			item(BTSTAT_OVERFLOW_L1HITS, /* default */) \
/* 5 */		item(BTSTAT_LEAF_L1MISSES, /* default */) \
			item(BTSTAT_NONLEAF_L1MISSES, /* default */) \
			item(BTSTAT_OVERFLOW_L1MISSES, /* default */) \
			item(BTSTAT_BACKUP_L1MISSES, /* default */) \
			item(BTSTAT_BACKUP_L1HITS, /* default */) \
/* 10 */	item(BTSTAT_LEAF_L1WRITES, /* default */) \
			item(BTSTAT_NONLEAF_L1WRITES, /* default */) \
			item(BTSTAT_OVERFLOW_L1WRITES, /* default */) \
			item(BTSTAT_LEAF_NODES, /* default */) \
			item(BTSTAT_NONLEAF_NODES, /* default */) \
/* 15 */    item(BTSTAT_OVERFLOW_NODES, /* default */) \
			item(BTSTAT_LEAF_BYTES, /* default */) \
			item(BTSTAT_NONLEAF_BYTES, /* default */) \
			item(BTSTAT_OVERFLOW_BYTES, /* default */) \
			item(BTSTAT_NUM_OBJS, /* default */) \
/* 20 */    item(BTSTAT_TOTAL_BYTES, /* default */) \
                        item(BTSTAT_EVICT_BYTES, /* default */) \
			item(BTSTAT_SPLITS, /* default */) \
			item(BTSTAT_LMERGES, /* default */) \
			item(BTSTAT_RMERGES, /* default */) \
/* 25 */    item(BTSTAT_LSHIFTS, /* default */) \
                        item(BTSTAT_RSHIFTS, /* default */) \
			item(BTSTAT_EX_TREE_LOCKS, /* default */) \
			item(BTSTAT_NON_EX_TREE_LOCKS, /* default */) \
			item(BTSTAT_GET_CNT, /* default */) \
/* 30 */    item(BTSTAT_GET_PATH, /* default */) \
                        item(BTSTAT_CREATE_CNT, /* default */) \
			item(BTSTAT_CREATE_PATH, /* default */) \
			item(BTSTAT_SET_CNT, /* default */) \
			item(BTSTAT_SET_PATH, /* default */) \
/* 35 */    item(BTSTAT_UPDATE_CNT, /* default */) \
                        item(BTSTAT_UPDATE_PATH, /* default */) \
			item(BTSTAT_DELETE_CNT, /* default */) \
			item(BTSTAT_DELETE_PATH, /* default */) \
			item(BTSTAT_FLUSH_CNT, /* default */) \
/* 40 */    item(BTSTAT_DELETE_OPT_CNT, /* default */) \
                        item(BTSTAT_MPUT_IO_SAVED, /* default */)  \
			item(BTSTAT_PUT_RESTART_CNT, /* default */)	\
			item(BTSTAT_SPCOPT_BYTES_SAVED, /* default */)  \
                        item(BTSTAT_MPUT_CNT, /* default */)  \
/* 45 */    item(BTSTAT_MSET_CNT, /* default */)  \
                        item(BTSTAT_RANGE_CNT, /* default */)  \
                        item(BTSTAT_RANGE_NEXT_CNT, /* default */)  \
                        item(BTSTAT_RANGE_FINISH_CNT, /* default */)  \
                        item(BTSTAT_RANGE_UPDATE_CNT, /* default */)  \
/* 50 */    item(BTSTAT_CREATE_SNAPSHOT_CNT, /* default */)  \
                        item(BTSTAT_DELETE_SNAPSHOT_CNT, /* default */)  \
                        item(BTSTAT_LIST_SNAPSHOT_CNT, /* default */)  \
                        item(BTSTAT_TRX_START_CNT, /* default */)  \
                        item(BTSTAT_TRX_COMMIT_CNT, /* default */)  \
/* 55 */    item(BTSTAT_NUM_MPUT_OBJS, /* default */)  \
                        item(BTSTAT_NUM_RANGE_NEXT_OBJS, /* default */)  \
                        item(BTSTAT_NUM_RANGE_UPDATE_OBJS, /* default */)  \
			item(BTSTAT_NUM_SNAP_OBJS , /* default */)  \
			item(BTSTAT_SNAP_DATA_SIZE , /* default */)  \
/* 60 */    item(BTSTAT_NUM_SNAPS , /* default */)  \
                        item(BTSTAT_BULK_INSERT_CNT, /* default */) \
			item(BTSTAT_BULK_INSERT_FULL_NODES_CNT, /* default */) \
                        item(BTSTAT_READ_CNT, /* default */)  \
                        item(BTSTAT_WRITE_CNT, /* default */)  \

typedef enum {
#define item(caps, value) \
    caps value,
    BTSTATS_ITEMS()
#undef item
    N_BTSTATS
} btree_stat_t;

#ifndef _INSTANTIATE_BTSTATS_STRINGS
    extern char *btree_stats_strings[];
#else
    char *btree_stats_strings[] = {
#define item(caps, value) \
        #caps,
        BTSTATS_ITEMS()
#undef item
    };
#endif

typedef struct btree_stats {
    uint64_t     stat[N_BTSTATS];
} btree_stats_t;

struct btree_raw;
struct btree_raw_mem_node;
/*
 * Multiple objs put info.
 */
typedef struct {
    uint32_t flags;
    uint32_t key_len;
    uint64_t data_len;
    char *key;
    char *data;
} btree_mput_obj_t;

typedef struct btree_rupdate_marker {
	char *last_key;
	uint32_t last_key_len;	

	/*
	 * Index of the marker key in a node.
	 */
	int index;
	bool set;
	bool skip;

	/*
	 * Data for retry for space in other node
	 */
	char *retry_key;
	uint32_t retry_keylen;
	char *retry_data;
	uint64_t retry_datalen;
	bool retry_update;
} btree_rupdate_marker_t;

typedef struct key_stuff_info {
    int       fixed;
    bool      leaf;
    uint64_t  ptr;
    char *    key;
    uint32_t  keylen;
    uint64_t  datalen;
    uint32_t  fkeys_per_node;
    uint64_t  seqno;
    uint64_t  syndrome;

    int index; // Index of the key in leaf node
    void *keyrec; // Pointerto key structure for non-leaf nodes
} key_stuff_info_t;

/*
 * Variables to support persistent stats
 */
typedef struct zs_pstats_ {
    uint64_t seq_num;
    uint64_t obj_count;
	uint64_t num_snap_objs;
	uint64_t snap_data_size;
	uint64_t num_overflw_nodes;
} zs_pstats_t;

uint64_t total_sys_writes;
pthread_mutex_t pstats_mutex;
pthread_cond_t  pstats_cond_var;

/**/

typedef int (* btree_range_cmp_cb_t)(void     *data, 	//  opaque user data
                                 void     *range_data,
                                 char     *range_key,       
                                 uint32_t  range_keylen,   
                                 char     *key1,       
                                 uint32_t  keylen1); 

typedef int (* bt_mput_cmp_cb_t)(void  *data, 	//  opaque user data
				 char *key,
				 uint32_t keylen,
				 char *old_data,
				 uint64_t old_datalen,
				 char *new_data,
				 uint64_t new_datalen);
					
typedef void (read_node_cb_t)(btree_status_t *ret, void *data, void *pnode, uint64_t lnodeid, node_flags_t flag);
typedef void (write_node_cb_t)(struct ZS_thread_state *thd_state, btree_status_t *ret, void *cb_data, uint64_t **lnodeid, char **data, uint64_t datalen, uint32_t count, uint32_t rawobj);
typedef void (flush_node_cb_t)(btree_status_t *ret, void *cb_data, uint64_t lnodeid);
typedef int (freebuf_cb_t)(void *data, char *buf);
typedef struct btree_raw_mem_node *(create_node_cb_t)(btree_status_t *ret, void *data, uint64_t lnodeid);
typedef int (delete_node_cb_t)(void *data, uint64_t lnodeid, uint32_t flag);
typedef void (log_cb_t)(btree_status_t *ret, void *data, uint32_t event_type, struct btree_raw *btree, struct btree_raw_mem_node *n);
typedef int (cmp_cb_t)(void *data, char *key1, uint32_t keylen1, char *key2, uint32_t keylen2);
typedef int (trx_cmd_cb_t)( int, ...);
typedef uint64_t (seqno_alloc_cb_t)(void);

typedef bool (* btree_rupdate_cb_t) (char *key, uint32_t keylen, char *data, uint64_t datalen, void * callback_args, char **new_data, uint64_t *new_data_len);

/****************************************************
 *
 * Message Levels:
 *   0: error
 *   1: warning
 *   2: info
 *   3: debug
 *
 ****************************************************/
typedef void (msg_cb_t)(int level, void *msg_data, char *filename, int lineno, char *msg, ...);

struct btree_raw;

struct btree_raw* btree_raw_init(uint32_t flags, uint32_t n_partition, uint32_t n_partitions, uint32_t max_key_size, uint32_t min_keys_per_node, uint32_t nodesize,
	create_node_cb_t *create_node_cb, void *create_node_data,
	read_node_cb_t *read_node_cb, void *read_node_cb_data,
	write_node_cb_t *write_node_cb, void *write_node_cb_data,
	flush_node_cb_t *flush_node_cb, void *flush_node_cb_data,
	freebuf_cb_t *freebuf_cb, void *freebuf_cb_data,
	delete_node_cb_t *delete_node_cb, void *delete_node_data,
	log_cb_t *log_cb, void *log_cb_data,
	msg_cb_t *msg_cb, void *msg_cb_data,
	cmp_cb_t *cmp_cb, void * cmp_cb_data,
	bt_mput_cmp_cb_t mput_cmp_cb, void *mput_cmp_cb_data,
	trx_cmd_cb_t *trx_cmd_cb, uint64_t cguid, zs_pstats_t *pstats, 
        seqno_alloc_cb_t *ptr_seqno_alloc_cb
	);

void btree_raw_destroy(struct btree_raw **, bool clean_l1cache);

extern btree_status_t btree_raw_get(struct btree_raw *btree, char *key, uint32_t keylen, char **data, uint64_t *datalen, btree_metadata_t *meta);

extern btree_status_t btree_raw_write(struct btree_raw *btree, char *key, uint32_t keylen, char *data, uint64_t datalen, btree_metadata_t *meta, uint32_t flags);

btree_status_t
btree_raw_mput(struct btree_raw *btree, btree_mput_obj_t *objs, uint32_t num_objs, uint32_t flags , btree_metadata_t *meta, uint32_t *objs_written);

extern btree_status_t btree_raw_flush(struct btree_raw *btree, char *key, uint32_t keylen);

extern btree_status_t btree_raw_ioctl(struct btree_raw *bt, uint32_t ioctl_type, void *data);

/*   delete a key
 *
 *   returns BTREE_SUCCESS
 *   returns BTREE_FAILURE
 *   returns BTREE_KEY_NOT_FOUND
 *
 *   Reference: "Implementing Deletion in B+-trees", Jan Jannink, SIGMOD RECORD,
 *              Vol. 24, No. 1, March 1995
 */
extern btree_status_t btree_raw_delete(struct btree_raw *btree, char *key, uint32_t keylen, btree_metadata_t *meta);
extern void release_per_thread_keybuf();

/* Like btree_get, but gets next n_in keys after a specified key.
 * Use key=NULL and keylen=0 for first call in enumeration.
 */
extern int btree_raw_get_next_n(uint32_t n_in, uint32_t *n_out, struct btree_raw *btree, char *key_in, uint32_t keylen_in, char **keys_out, uint32_t *keylens_out, char **data_out, uint64_t datalens_out, btree_metadata_t *meta);

extern int btree_raw_fast_build(struct btree_raw *btree);

//extern void btree_raw_dump(FILE *f, struct btree_raw *btree);

//extern void btree_raw_check(struct btree_raw *btree);

extern void btree_raw_test(struct btree_raw *btree);

extern int btree_raw_snapshot(struct btree_raw *btree, uint64_t *seqno);
extern int btree_raw_delete_snapshot(struct btree_raw *btree, uint64_t seqno);
extern int btree_raw_get_snapshots(struct btree_raw *btree, uint32_t *n_snapshots, uint64_t *seqnos);
extern int btree_raw_free_buffer(struct btree_raw *btree, char *buf);

extern void btree_raw_get_stats(struct btree_raw *btree, btree_stats_t *stats);
extern char *btree_stat_name(btree_stat_t stat_type);
extern void btree_dump_stats(FILE *f, btree_stats_t *stats);
extern void btree_raw_alloc_thread_bufs(void);
extern void btree_raw_free_thread_bufs(void);

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
	        btree_rupdate_marker_t **marker);

btree_status_t btree_raw_move_lasterror(struct btree_raw *btree, void **err_out, uint32_t *err_size);
btree_status_t btree_raw_rescue(struct btree_raw *btree, void *p_err_context);

#define DEFAULT_N_L1CACHE_BUCKETS 5000
#define DEFAULT_N_L1CACHE_PARTITIONS 256
#if 0
bool 
btree_raw_node_check(struct btree_raw *btree, btree_raw_node_t *node,
		  char *prev_anchor_key, uint32_t prev_anchor_keylen,
		  char *next_anchor_key, uint32_t next_anchor_keylen);

bool
btree_raw_check_node_subtree(struct btree_raw *btree, btree_raw_node_t *node,
			  char *prev_anchor_key, uint32_t prev_anchor_keylen,
			  char *next_anchor_key, uint32_t next_anchor_keylen);

#endif 
bool
btree_raw_check(struct btree_raw *btree, uint64_t *num_objs);




#endif // __BTREE_RAW_H
