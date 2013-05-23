#ifndef __BTREE_RAW_INTERNAL_H
#define __BTREE_RAW_INTERNAL_H

#include "btree_raw.h"
#include "platform/rwlock.h"

#define BAD_CHILD       0
#define META_LOGICAL_ID 0x8000000000000000

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
    struct Map   *l1cache;
    struct Map   *l1cache_refs;
    struct Map   *l1cache_mods;
    read_node_cb_t    *read_node_cb;
    void              *read_node_cb_data;
    write_node_cb_t   *write_node_cb;
    void              *write_node_cb_data;
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

    btree_stats_t      stats;

    plat_rwlock_t      lock;
} btree_raw_t;

typedef struct btree_raw_persist {
    uint64_t    rootid,
                logical_id_counter;
} btree_raw_persist_t;

#endif // __BTREE_RAW_INTERNAL_H
