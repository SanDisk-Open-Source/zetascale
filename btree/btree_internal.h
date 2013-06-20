#ifndef __BTREE_INTERNAL_H
#define __BTREE_INTERNAL_H

#include <pthread.h>
#include "btree_raw.h"

typedef struct btree_iterator {
    int    dummy;
} btree_iterator_t;

typedef struct btree {
    /* these are the same as those stashed in the partitions */
    uint32_t           flags;
    uint32_t           max_key_size;
    uint32_t           min_keys_per_node;
    uint32_t           nodesize;
    uint32_t           keys_per_node;
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
//    trx_cmd_cb_t      *trx_cmd_cb;
//    void              *trx_cmd_cb_data;

    /* fields unique to btree_t */
    uint32_t           n_partitions;
    struct btree_raw  **partitions;
    uint32_t           n_iterators;
    uint32_t           n_free_iterators;
    btree_iterator_t  *free_iterators;
    btree_iterator_t  *used_iterators;

} btree_t;


#endif // __BTREE_INTERNAL_H
