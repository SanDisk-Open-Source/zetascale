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
#include <api/fdf.h>


#ifndef __BTREE_RAW_H
#define __BTREE_RAW_H

typedef enum btree_status {
	BTREE_SUCCESS = 0,                /* Fine & Dandy */
	BTREE_FAILURE = 1,                /* Generic failure */
	BTREE_BUFFER_TOO_SMALL,           /* Give me more */
	BTREE_QUERY_DONE,                 /* No more queries please */
	BTREE_INVALID_QUERY,              /* Correct questions please */
	BTREE_KEY_NOT_FOUND,              /* Cannot find key */
	BTREE_FAIL_TXN_START,             /* Failed to start transaction */
	BTREE_FAIL_TXN_COMMIT,            /* Failed to commit transaction */
	BTREE_FAIL_TXN_ROLLBACK,          /* Failed to rollback transaction */
	BTREE_OPERATION_DISALLOWED,       /* Shutdown in progress */
	BTREE_WARNING,                    /* Any partial success */
	BTREE_QUERY_PAUSED,               /* Range query passed by callback */
	BTREE_RANGE_UPDATE_NEEDS_SPACE,	  /* Range update need space for update */
	BTREE_SKIPPED,                    /* Generic skip return */
	BTREE_TOO_MANY_SNAPSHOTS,         /* Too many snapshots, cannot create more */
} btree_status_t;

typedef enum node_flags {
    LEAF_NODE     = 1,
    OVERFLOW_NODE = 2
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
} btree_meta_flags_t;

typedef struct btree_metadata {
    uint32_t     flags;
    uint32_t     keybuf_size;
    uint64_t     databuf_size;
    uint64_t     seqno;
    uint64_t     start_seqno;
    uint64_t     end_seqno;
    uint64_t     checksum;
} btree_metadata_t;

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
/* 20 */    item(BTSTAT_EVICT_BYTES, /* default */) \
			item(BTSTAT_SPLITS, /* default */) \
			item(BTSTAT_LMERGES, /* default */) \
			item(BTSTAT_RMERGES, /* default */) \
			item(BTSTAT_LSHIFTS, /* default */) \
/* 25 */    item(BTSTAT_RSHIFTS, /* default */) \
			item(BTSTAT_EX_TREE_LOCKS, /* default */) \
			item(BTSTAT_NON_EX_TREE_LOCKS, /* default */) \
			item(BTSTAT_GET_CNT, /* default */) \
			item(BTSTAT_GET_PATH, /* default */) \
/* 30 */    item(BTSTAT_CREATE_CNT, /* default */) \
			item(BTSTAT_CREATE_PATH, /* default */) \
			item(BTSTAT_SET_CNT, /* default */) \
			item(BTSTAT_SET_PATH, /* default */) \
			item(BTSTAT_UPDATE_CNT, /* default */) \
/* 35 */    item(BTSTAT_UPDATE_PATH, /* default */) \
			item(BTSTAT_DELETE_CNT, /* default */) \
			item(BTSTAT_DELETE_PATH, /* default */) \
			item(BTSTAT_FLUSH_CNT, /* default */) \
			item(BTSTAT_DELETE_OPT_CNT, /* default */) \
/* 40 */    item(BTSTAT_MPUT_IO_SAVED, /* default */)  \
			item(BTSTAT_PUT_RESTART_CNT, /* default */)	\
			item(BTSTAT_SPCOPT_BYTES_SAVED, /* default */)  \
			item(BTSTAT_NUM_SNAP_OBJS , /* default */)  \
			item(BTSTAT_SNAP_DATA_SIZE , /* default */)  \
/* 45 */    item(BTSTAT_NUM_SNAPS , /* default */)  \
			item(BTSTAT_BULK_INSERT_CNT, /* default */) \
			item(BTSTAT_BULK_INSERT_FULL_NODES_CNT, /* default */) \

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
typedef struct fdf_pstats_ {
    uint64_t seq_num;
    uint64_t obj_count;
	uint64_t num_snap_objs;
	uint64_t snap_data_size;
} fdf_pstats_t;

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
					
typedef struct btree_raw_mem_node *(read_node_cb_t)(btree_status_t *ret, void *data, uint64_t lnodeid);
typedef void (write_node_cb_t)(struct FDF_thread_state *thd_state, btree_status_t *ret, void *cb_data, uint64_t **lnodeid, char **data, uint64_t datalen, uint32_t count);
typedef void (flush_node_cb_t)(btree_status_t *ret, void *cb_data, uint64_t lnodeid);
typedef int (freebuf_cb_t)(void *data, char *buf);
typedef struct btree_raw_mem_node *(create_node_cb_t)(btree_status_t *ret, void *data, uint64_t lnodeid);
typedef int (delete_node_cb_t)(void *data, uint64_t lnodeid);
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
	trx_cmd_cb_t *trx_cmd_cb, uint64_t cguid, fdf_pstats_t *pstats, 
        seqno_alloc_cb_t *ptr_seqno_alloc_cb
	);

void btree_raw_destroy(struct btree_raw **);

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

#define DEFAULT_N_L1CACHE_BUCKETS 5000
#define DEFAULT_N_L1CACHE_PARTITIONS 256

#endif // __BTREE_RAW_H
