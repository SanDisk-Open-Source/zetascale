#ifndef __BTREE_RAW_INTERNAL_H
#define __BTREE_RAW_INTERNAL_H

#include "btree_raw.h"
#include "platform/rwlock.h"

#define BAD_CHILD       0
#define META_LOGICAL_ID 0x8000000000000000L

typedef struct node_vkey {
    uint32_t    keylen;
    uint32_t    keypos;
    uint64_t    ptr;
    uint64_t    seqno;
} node_vkey_t;

typedef struct node_vlkey {
    uint32_t    keylen;
    uint32_t    keypos;
    uint64_t    datalen;
    uint64_t    ptr;
    uint64_t    seqno;
    uint64_t    syndrome;
} node_vlkey_t;

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
    uint32_t  keylen;
    uint64_t  datalen;
    uint32_t  fkeys_per_node;
    uint64_t  seqno;
    uint64_t  syndrome;
} key_stuff_t;

typedef struct btree_raw_node {
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

typedef struct btree_raw_mem_node {
	plat_rwlock_t lock;
	btree_raw_node_t* node;
} btree_raw_mem_node_t;

#define BTREE_RAW_L1CACHE_LIST_MAX 10000

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
    struct Map        *l1cache_refs;
    struct Map        *l1cache_mods;
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
    trx_cmd_cb_t      *trx_cmd_cb;

    btree_stats_t      stats;

    plat_rwlock_t      lock;
    plat_rwlock_t      write_io_lock;

    uint64_t           modified;
} btree_raw_t;

typedef struct btree_raw_persist {
    btree_raw_node_t n; // this must be first member
    uint64_t    rootid,
                logical_id_counter;
} btree_raw_persist_t;

int get_key_stuff(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_t *pks);
btree_status_t get_leaf_data(btree_raw_t *bt, btree_raw_node_t *n, void *pkey, char **data, uint64_t *datalen, uint32_t meta_flags, int ref);
btree_status_t get_leaf_key(btree_raw_t *bt, btree_raw_node_t *n, void *pkey, char **key, uint32_t *keylen, uint32_t meta_flags);
btree_raw_node_t *get_existing_node_low(btree_status_t *ret, btree_raw_t *btree, uint64_t logical_id, plat_rwlock_t** lock, int ref);
btree_raw_node_t *get_existing_node(btree_status_t *ret, btree_raw_t *btree, uint64_t logical_id);
int is_leaf(btree_raw_t *btree, btree_raw_node_t *node);

void deref_l1cache_node(btree_raw_t* btree, btree_raw_node_t *node);
#endif // __BTREE_RAW_INTERNAL_H
