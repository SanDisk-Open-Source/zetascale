/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/protocol/replication/tests/test_model.c
 *
 * Author: YAO Haowei
 *
 * Created on Nov 17, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_model.c 9798 2009-06-02 03:47:32Z lzwei $
 * TODO: shard
 * TODO: synchronize
 * TODO: vnode on create shard
 */
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/logging.h"
#include "platform/stdlib.h"
#include "fth/fth.h"


#include "protocol/init_protocol.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_common.h"
#include "protocol/action/action_thread.h"
#include "protocol/home/home_flash.h"

#include "protocol/init_protocol.h"
#include "protocol/replication/copy_replicator.h"
#include "protocol/replication/replicator.h"
#include "protocol/replication/replicator_adapter.h"

#include "test_common.h"
#include "test_flash.h"
#include "test_model.h"
#include "utils/hash.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/generator");

#define INIT_HASH_N 4093
#define MAX_LAMBDA 0.5
#define STRUCT_NEW(target) (target *)plat_alloc(sizeof(target))

#define MIN_CTN_SIZE 20
#define rtm_container_def(element_type) \
    typedef element_type * element_type ## _ptr;                              \
                                                                              \
    struct rtm_ctn_ ## element_type {                                         \
        element_type ## _ptr *e;                                              \
        uint64_t cur_size;                                                    \
        uint64_t max_size;                                                    \
    };                                                                        \
    typedef struct rtm_ctn_ ## element_type rtm_ctn_ ## element_type;         \
                                                                              \
    static rtm_ctn_ ## element_type *                                         \
    rtm_ctn_alloc_ ## element_type()                                          \
    {                                                                         \
        rtm_ctn_ ## element_type * ret;                                       \
        ret = STRUCT_NEW(rtm_ctn_ ## element_type);                           \
        plat_assert(ret != NULL);                                             \
        ret->e = (element_type ## _ptr *)                                     \
                    plat_malloc(MIN_CTN_SIZE * sizeof(element_type ## _ptr)); \
        plat_assert(ret->e);                                                  \
        memset((void *)(ret->e), 0, MIN_CTN_SIZE * sizeof(element_type ## _ptr));                    \
        ret->cur_size = 0;                                                    \
        ret->max_size = MIN_CTN_SIZE;                                         \
        return (ret);                                                         \
    }                                                                         \
                                                                              \
    inline int rtm_ctn_equal_ ## element_type(element_type x, element_type y) \
    {                                                                         \
        return (x == y);                                                      \
    }                                                                         \
                                                                              \
    static int                                                                \
    rtm_ctn_exist_ ## element_type(rtm_ctn_ ## element_type * ctn,            \
                                   element_type e)                            \
    {                                                                         \
        uint64_t i;                                                           \
        for (i = 0; i < ctn->cur_size; i ++) {                                \
            if (rtm_ctn_equal_ ## element_type(*(ctn->e[i]), e))              \
                return (1);                                                   \
        }                                                                     \
        return (0);                                                           \
    }                                                                         \
                                                                              \
    static void                                                               \
    rtm_ctn_destroy_ ## element_type(rtm_ctn_ ## element_type * ctn)          \
    {                                                                         \
        uint64_t i;                                                           \
        for (i = 0; i < ctn->cur_size; i ++) {                                \
            if (ctn->e[i])                                                    \
                plat_free(ctn->e[i]);                                         \
        }                                                                     \
        plat_free(ctn->e);                                                    \
        plat_free(ctn);                                                       \
    }                                                                         \
                                                                              \
    static int                                                                \
    rtm_ctn_put_ ## element_type(rtm_ctn_ ## element_type * ctn,              \
                                 element_type e)                              \
    {                                                                         \
        if (rtm_ctn_exist_ ## element_type(ctn, e)) {                         \
            return (0);                                                       \
        }                                                                     \
        if (ctn->cur_size == ctn->max_size) {                                 \
            element_type ## _ptr * ptr = (element_type ## _ptr *)             \
                        plat_malloc(ctn->max_size * 2 * sizeof(element_type ## _ptr));                \
            plat_assert(ptr != NULL);                                         \
            memcpy((void *)ptr, (void *)ctn->e, sizeof(element_type ## _ptr) * ctn->max_size);        \
            plat_free(ctn->e);                                                \
            ctn->e = ptr;                                                     \
            ctn->max_size *= 2;                                               \
        }                                                                     \
        ctn->e[ctn->cur_size] = (element_type *)                              \
                        plat_malloc(sizeof(element_type));                    \
        *(ctn->e[ctn->cur_size]) = e;                                         \
        ctn->cur_size ++;                                                     \
        return (0);                                                           \
    }                                                                         \
                                                                              \
    static int                                                                \
    rtm_ctn_delete_ ## element_type(rtm_ctn_ ## element_type * ctn,           \
                                    element_type e)                           \
    {                                                                         \
        uint64_t i;                                                           \
        for (i = 0; i < ctn->cur_size; i ++) {                                \
            if (rtm_ctn_equal_ ## element_type(*(ctn->e[i]), e)) {            \
                plat_free(ctn->e[i]);                                         \
                memmove((void *)(ctn->e + i), (void *)(ctn->e + i + 1),       \
                        (ctn->cur_size - i - 1) * sizeof(element_type *));    \
                ctn->cur_size --;                                             \
                return (0);                                                   \
            }                                                                 \
        }                                                                     \
        return (1);                                                           \
    }

typedef SDF_shardid_t sid;
rtm_container_def(sid);

struct replication_test_model_create_shard_op {
    struct replication_test_model *rtm;
    sdf_replication_ltime_t ltime;
    SDF_shardid_t shard_id;
};

struct replication_test_model_last_seqno_op {
    struct replication_test_model *rtm;
    sdf_replication_ltime_t ltime;
    SDF_shardid_t shard_id;
};

struct replication_test_model_get_cursors_op {
    struct replication_test_model *rtm;
    sdf_replication_ltime_t ltime;
    SDF_shardid_t shard_id;
    uint64_t seqno_start;
    uint64_t seqno_len;
    uint64_t seqno_max;
};

struct replication_test_model_get_by_cursor_op {
    struct replication_test_model *rtm;
    sdf_replication_ltime_t ltime;
    SDF_shardid_t shard_id;
    const void *cursor;
    size_t      cursor_len;
};

struct replication_test_model_delete_shard_op {
    struct replication_test_model *rtm;
    sdf_replication_ltime_t ltime;
    SDF_shardid_t shard_id;
};

struct replication_test_model_write_op {
    struct replication_test_model *rtm;
    SDF_shardid_t shard;
    sdf_replication_ltime_t ltime;
    struct timeval start_time;
    vnode_t node;
    const void * key;
    size_t key_len;
    uint64_t data_hash;
};

struct replication_test_model_read_op {
    struct replication_test_model *rtm;
    SDF_shardid_t shard;
    sdf_replication_ltime_t ltime;
    struct timeval start_time;
    vnode_t node;
    const void * key;
    size_t key_len;
};

struct replication_test_model_delete_op {
    struct replication_test_model *rtm;
    SDF_shardid_t shard;
    sdf_replication_ltime_t ltime;
    struct timeval start_time;
    vnode_t node;
    const void * key;
    size_t key_len;
};

typedef struct replication_test_model_create_shard_op rtm_create_shard_op_t;
typedef struct replication_test_model_last_seqno_op rtm_last_seqno_op_t;
typedef struct replication_test_model_get_cursors_op rtm_get_cursors_op_t;
typedef struct replication_test_model_get_by_cursor_op rtm_get_by_cursor_op_t;
typedef struct replication_test_model_delete_shard_op rtm_delete_shard_op_t;
typedef struct replication_test_model_write_op rtm_write_op_t;
typedef struct replication_test_model_read_op rtm_read_op_t;
typedef struct replication_test_model_delete_op rtm_delete_op_t;

typedef enum {
    RTM_WS, RTM_WC, /* write start and complete */
    RTM_RS, RTM_RC, /* read start and complete */
    RTM_DS, RTM_DC, /* delete start and complete */
    RTM_HEAD,
} rtm_op_t;

struct rtm_op_entry_t {
    rtm_op_t op;
    uint64_t data_hash;
    vnode_t vnode;
    SDF_status_t status;
    unsigned int in_u_set : 1;
    unsigned int in_x_set : 1;
    struct rtm_op_entry_t *next;
    struct rtm_op_entry_t *prev;
    struct rtm_op_entry_t *match;
};

typedef struct rtm_op_entry_t rtm_op_entry_t;

/**
 * @brief generic op allocator
 *
 * @param type rtm general op type
 * @param data pointer to specific op struct, like replication_test_model_delete_op
 * @param entry pointer to op history entry for each object
 *
 * @return pointer of the generic op
 */
static rtm_general_op_t *
rtm_general_op_alloc(rtm_go_type_t type, void *data, rtm_op_entry_t *entry)
{
    rtm_general_op_t *ret = STRUCT_NEW(rtm_general_op_t);
    plat_assert(ret != NULL);
    ret->go_type = type;
    ret->op_data = data;
    ret->entry = entry;
    return (ret);
}

/**
 * @brief generic op destroyer
 *
 * @param op rtm generic op to destory
 */
static void
rtm_general_op_destroy(rtm_general_op_t *op)
{
    if (op) {
        if (op->op_data) {
            plat_free(op->op_data);
        }
        plat_free(op);
    }
}

/**
 * @brief replication test model data for each object
 *
 * maintains the operation history in a doubly-linked history
 */
typedef struct {
    /** @brief fthLock for data */
    fthLock_t dataLock;

    /** @brief double-direction link for op history */
    rtm_op_entry_t *op_history;

    /** @brief ptr to end of op_history */
    rtm_op_entry_t *end;

    /** @brief uncompleted write op */
    uint64_t u_count;
} rtm_data_t;

typedef rtm_data_t *rtm_data_ptr;

/**
 * @brief rtm data destroyer
 *
 * @param data rtm data to destroy
 */
static void
rtm_data_destroy(rtm_data_t *data)
{
    rtm_op_entry_t *p = data->op_history, *tmp;
    while (p) {
        tmp = p->next;
        plat_free(p);
        p = tmp;
    }
    plat_free(data);
}

/**
 * @brief add a entry at the end of current history
 *
 * @param data rtm data to add entry on it
 * @param op op type for the operation
 * @param hash value of the data of the operation, 0 if no data contained
 * @param vnode vnode id for this operation
 * @param match match entry for this operation.
 * (if it is a start, match will be NULL, if it is a complete, match will be the start)
 *
 * @return op entry which has just been added on the rtm_data
 */
static rtm_op_entry_t *
rtm_data_add(rtm_data_t *data, rtm_op_t op,
             uint64_t data_hash, vnode_t vnode, rtm_op_entry_t *match)
{
    rtm_op_entry_t *op_entry = STRUCT_NEW(rtm_op_entry_t);
    plat_assert(op_entry != NULL);
    memset((char *)op_entry, 0, sizeof(rtm_op_entry_t));
    op_entry->op = op;
    op_entry->data_hash = data_hash;
    op_entry->vnode = vnode;
    op_entry->next = NULL;
    op_entry->prev = data->end;
    op_entry->in_x_set = 0;
    op_entry->in_u_set = 0;
    op_entry->match = NULL;

    plat_assert(data != NULL);
    plat_assert(data->op_history != NULL);
    fthWaitEl_t *lock = fthLock(&(data->dataLock), 1, NULL);

    if (op == RTM_RC) {
        plat_assert(match != NULL);
        op_entry->match = match;
        op_entry->match->match = op_entry;
    } else if (op == RTM_WC || op == RTM_DC) {
        plat_assert(match != NULL);
        op_entry->match = match;
        op_entry->match->match = op_entry;
        op_entry->match->in_u_set = 0;
        data->u_count --;
    } else if (op == RTM_WS || op == RTM_DS) {
        op_entry->in_u_set = 1;
        data->u_count ++;
    }
    data->end->next = op_entry;
    data->end = op_entry;

    fthUnlock(lock);
    return (op_entry);
}

/**
 * @brief rtm data allocator
 *
 * initialize the history and reset the u_count
 *
 * @return the rtm data allocated
 */
static rtm_data_t *
rtm_data_alloc()
{
    rtm_data_t *ret = (rtm_data_t *)plat_malloc(sizeof(rtm_data_t));
    plat_assert(ret != NULL);

    /* allocate head for this history */
    rtm_op_entry_t *op_entry = STRUCT_NEW(rtm_op_entry_t);
    plat_assert(op_entry != NULL);
    memset((char *)op_entry, 0, sizeof(rtm_op_entry_t));
    op_entry->op = RTM_HEAD;
    ret->op_history = op_entry;
    ret->end = op_entry;
    ret->u_count = 0;
    fthLockInit(&(ret->dataLock));
    return (ret);
}


/**
 * @brief Actual data verify algorithm
 * see arch_manual/Replication_Data_Verification_Algorithm.txt
 *
 * log a msg to identify error
 *
 * @param rtm_data to verify
 * @return 0 if verify passed, 1 if failed
 */
static int
rtm_data_verify(rtm_data_t *data)
{
    rtm_op_entry_t *cur = data->end;

    rtm_op_entry_t *ptr = cur;
    int break_flag = 0;
    uint64_t u_count = data->u_count;

    fthWaitEl_t *lock = fthLock(&(data->dataLock), 1, NULL);

    /* Area 1 Search */
    plat_assert(ptr && (ptr->prev != NULL || ptr->op == RTM_HEAD));
    while ((ptr = ptr->prev) != NULL && ptr->op != RTM_HEAD && ptr->match != cur) {
        plat_assert(ptr->prev != NULL || ptr->op == RTM_HEAD);
        switch (ptr->op) {
        case RTM_RC:
            if (ptr->vnode == cur->vnode) {
                if (ptr->data_hash == cur->data_hash) {
                    goto success;
                }
            }
            ptr->in_x_set = 0;
            break;
        case RTM_WC:
        case RTM_DC:
            if (ptr->data_hash == cur->data_hash) {
                goto success;
            }
            ptr->in_x_set = 0;
            break;
        case RTM_WS:
        case RTM_DS:
            if (ptr->data_hash == cur->data_hash) {
                goto success;
            }
            if (ptr->in_u_set == 1) {
                u_count --;
            }
            break;
        default: break;
        }
    }

    /* Area 2 Search */
    plat_assert(ptr && (ptr->prev != NULL || ptr->op == RTM_HEAD));
    while (break_flag == 0 && ptr->op != RTM_HEAD &&
           (ptr = ptr->prev) != NULL && ptr->op != RTM_HEAD) {
        plat_assert(ptr->prev != NULL || ptr->op == RTM_HEAD);
        switch (ptr->op) {
        case RTM_RS:
            if (ptr->vnode == cur->vnode) {
                if (ptr->in_u_set == 0 && ptr->match != NULL &&
                    ptr->match->in_x_set == 1) {
                    break_flag = 1;
                }
            }
            break;
        case RTM_RC:
            if (ptr->vnode == cur->vnode) {
                if (ptr->data_hash == cur->data_hash) {
                    goto success;
                }
                ptr->in_x_set = 1;
            }
            break;
        case RTM_WC:
        case RTM_DC:
            if (ptr->data_hash == cur->data_hash) {
                goto success;
            }
            ptr->in_x_set = 1;
            break;
        case RTM_WS:
        case RTM_DS:
            if (ptr->data_hash == cur->data_hash) {
                goto success;
            }
            if (ptr->in_u_set == 1) {
                u_count --;
            } else if (ptr->match->in_x_set == 1) {
                break_flag = 1;
            }
            break;
        default: break;
        }
    }

    plat_assert(ptr && (ptr->prev != NULL || ptr->op == RTM_HEAD));
    if (ptr->op == RTM_HEAD) {
        if (ptr->data_hash == cur->data_hash) {
            goto success;
        } else {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "reached the end of history but still"
                         "have not find a match");
            goto failed;
        }
    }

    /* Area 3 Search */
    while (u_count > 0 && (ptr = ptr->prev) != NULL) {
        plat_assert(ptr->prev != NULL || ptr->op == RTM_HEAD);
        switch (ptr->op) {
        case RTM_WS:
        case RTM_DS:
            if (ptr->in_u_set == 1) {
                u_count --;
                if (ptr->data_hash == cur->data_hash) {
                    goto success;
                }
            }
            break;
        default: break;
        }
    }

    plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR, "value not possible");
failed:
    fthUnlock(lock);
    return (1);
success:
    fthUnlock(lock);
    return (0);
}

/**
 * @brief rtm hash entry
 * hash entry for object
 */
typedef struct {
    /** @brief key for the object */
    char *key;

    /** @brief key length */
    uint64_t key_len;

    /** @brief shard id for this object */
    SDF_shardid_t shard_id;

    /** @brief history data for this object */
    rtm_data_t *data;
} rtm_hash_entry_t;

typedef rtm_hash_entry_t *rtm_hash_entry_ptr;

/**
 * @brief
 */
static void
rtm_hash_entry_destroy(rtm_hash_entry_t *entry)
{
    if (entry) {
        if (entry->data) {
            rtm_data_destroy(entry->data);
        }
        if (entry->key) {
            plat_free(entry->key);
        }
        plat_free(entry);
    }
}

struct rtm_hash_map_t {
    uint64_t size;
    uint64_t cur_size;
    rtm_hash_entry_ptr *entries;
    fthLock_t rehash_lock;
};

typedef struct rtm_hash_map_t rtm_hash_map_t;

static rtm_hash_map_t *
rtm_hash_map_alloc()
{
    rtm_hash_map_t *ptr = STRUCT_NEW(rtm_hash_map_t);
    plat_assert(ptr != NULL);
    ptr->size = INIT_HASH_N;
    ptr->cur_size = 0;
    ptr->entries = (rtm_hash_entry_ptr *)plat_malloc(sizeof(rtm_hash_entry_ptr) * INIT_HASH_N);
    plat_assert(ptr->entries != NULL);
    memset((char *)(ptr->entries), '\0', sizeof(rtm_hash_entry_ptr) * INIT_HASH_N);
    return (ptr);
}

static void
rtm_hash_map_rehash(rtm_hash_map_t *hash_map)
{
    uint64_t new_size, i;
    uint64_t index = 0, level = 0;

    for (new_size = hash_map->size * 2; 1; new_size ++) {
        int find = 1;
        for (i = 2; i <= new_size / 2; i ++) {
            if (new_size % i == 0) {
                find = 0;
                break;
            }
        }
        if (find) {
            break;
        }
    }

    rtm_hash_entry_ptr *new_entries =
        (rtm_hash_entry_ptr *)plat_malloc(sizeof(rtm_hash_entry_ptr) * new_size);
    plat_assert(new_entries != NULL);
    memset((char *)new_entries, '\0', sizeof(rtm_hash_entry_ptr) * new_size);

    for (i = 0; i < hash_map->size; i ++) {
        rtm_hash_entry_ptr entry = hash_map->entries[i];
        if (entry && entry->key != NULL) {
            level = entry->shard_id;
            do {
                index = hash((unsigned char *)(entry->key), entry->key_len, level) % new_size;
                level ++;
            } while (new_entries[index] != NULL);
            new_entries[index] = entry;
        }
    }

    plat_free(hash_map->entries);
    hash_map->entries = new_entries;
    hash_map->size = new_size;
}

static rtm_data_t *
rtm_hash_map_get(rtm_hash_map_t *hash_map,
                 char *key, uint64_t len, SDF_shardid_t shard_id)
{
    uint64_t index, level = shard_id;
    rtm_hash_entry_t *entry;
    uint64_t cur_size = hash_map->cur_size;
    uint64_t max_size = hash_map->size;
    if ((double)cur_size / (double)max_size > MAX_LAMBDA) {
        fthWaitEl_t *rehash_lock = fthLock(&(hash_map->rehash_lock), 1, NULL);
        if ((double)cur_size / (double)max_size > MAX_LAMBDA) {
            rtm_hash_map_rehash(hash_map);
        }
        fthUnlock(rehash_lock);
    }

    do {
        index = hash((unsigned char *)(key), len, level) % max_size;
        entry = hash_map->entries[index];
        level ++;
        if (entry != NULL) {
            if (len == entry->key_len &&
                strncmp(entry->key, key, len) == 0 &&
                shard_id == entry->shard_id) {
                return (hash_map->entries[index]->data);
            }
        }
    } while (entry != NULL);

    hash_map->entries[index] = STRUCT_NEW(rtm_hash_entry_t);
    plat_assert(hash_map->entries[index] != NULL);
    plat_asprintf(&hash_map->entries[index]->key, "%.*s", (int)len, key);
    hash_map->entries[index]->key_len = len;
    hash_map->entries[index]->data = rtm_data_alloc();

    hash_map->entries[index]->shard_id = shard_id;
    __sync_add_and_fetch(&hash_map->cur_size, 1);

    return (hash_map->entries[index]->data);
}

static void
rtm_hash_map_destroy(rtm_hash_map_t *hash_map)
{
    uint64_t i;
    for (i = 0; i < hash_map->size; i ++) {
        rtm_hash_entry_destroy(hash_map->entries[i]);
    }
    plat_free(hash_map->entries);
    plat_free(hash_map);
}

/**
 * @brief Create replication test model
 */
struct replication_test_model *
replication_test_model_alloc(const struct replication_test_config *config)
{
    struct replication_test_model *rtm;
    rtm = STRUCT_NEW(struct replication_test_model);
    plat_assert(rtm != NULL);
    rtm->obj_status = rtm_hash_map_alloc();
    rtm->flags = 0;
    rtm->shard_status = rtm_ctn_alloc_sid();
    rtm->ltime = 0;
    fthLockInit(&(rtm->model_lock));
    return (rtm);
}

/**
 * @brief Free replication test model
 */
void
rtm_free(struct replication_test_model *rtm)
{
    rtm_hash_map_destroy(rtm->obj_status);
    rtm_ctn_destroy_sid(rtm->shard_status);
    plat_free(rtm);
}

/**
 * @brief Return non-zero to indicate the model's constraints have
 * been violated.
 */
int
rtm_failed(const struct replication_test_model *rtm)
{
    return (rtm->flags);
}

/**
 * @brief Start shard create operation
 *
 * @param test_model <IN> Test model
 * @param now <IN> Simulated time at start of the request
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard_meta <IN> shard to create, caller owns buffer
 */
rtm_general_op_t *
rtm_start_create_shard(struct replication_test_model *test_model,
                       struct timeval now, vnode_t node,
                       struct SDF_shard_meta *shard_meta)
{
    if (rtm_ctn_exist_sid(test_model->shard_status, shard_meta->sguid)) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "shard already exists when create start,"
                     "node:%"PRIu32" shard_id %d.",
                     node, (int)(shard_meta->sguid));
        test_model->flags = 1;
//        return NULL;
    } else {
        rtm_ctn_put_sid(test_model->shard_status, shard_meta->sguid);
    }
    rtm_create_shard_op_t *op_data = STRUCT_NEW(rtm_create_shard_op_t);
    plat_assert(op_data != NULL);
    op_data->shard_id = shard_meta->sguid;
    op_data->rtm = test_model;
    rtm_general_op_t *ret = rtm_general_op_alloc(RTM_GO_CR_SHARD, (void *)op_data, NULL);
    plat_assert(ret != NULL);
    return (ret);
}

/**
 * Shard create operation completed
 *
 * Logs a message via #plat_log_msg when the status is unexpected and
 * indicates failure in the return.  Notably a shard create will fail if
 * a shard create is started later before this message is processed.
 *
 * @param op <IN> operation returned from #rtm_shard_create
 * @param now <IN> Simulated time at end of request
 * @param status <IN> returned status
 * @return 0 when status was accepted, non-zero when it is
 * inappropriate.
 */
int
rtm_create_shard_complete(rtm_general_op_t *op,
                          struct timeval now, SDF_status_t status)
{
    plat_assert(op->go_type == RTM_GO_CR_SHARD);
    rtm_create_shard_op_t *op_data = (rtm_create_shard_op_t *)(op->op_data);
    if (!rtm_ctn_exist_sid(op_data->rtm->shard_status, op_data->shard_id)) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "shard not found when create complete, shard_id %d.",
                     (int)(op_data->shard_id));
        op_data->rtm->flags = 1;
        return (1);
    }
    rtm_general_op_destroy(op);
    return (0);
}

rtm_general_op_t *
rtm_start_last_seqno(struct replication_test_model *test_model,
                     struct timeval now, vnode_t node,
                     SDF_shardid_t shard)
{
    // xxxzzz placeholder for now
    rtm_last_seqno_op_t *op_data = STRUCT_NEW(rtm_last_seqno_op_t);
    plat_assert(op_data != NULL);
    op_data->shard_id = shard;
    op_data->rtm = test_model;
    rtm_general_op_t *ret = rtm_general_op_alloc(RTM_GO_LAST_SEQNO, (void *)op_data, NULL);
    plat_assert(ret != NULL);
    return (ret);
}

int
rtm_last_seqno_complete(rtm_general_op_t *op,
                        struct timeval now, SDF_status_t status, uint64_t seqno)
{
    // xxxzzz placeholder for now
    plat_assert(op->go_type == RTM_GO_LAST_SEQNO);
    // rtm_last_seqno_op_t *op_data = (rtm_last_seqno_op_t *)(op->op_data);
    rtm_general_op_destroy(op);
    return (0);
}

rtm_general_op_t *
rtm_start_get_cursors(struct replication_test_model *test_model,
                      SDF_shardid_t shard, struct timeval now, vnode_t node,
                      uint64_t seqno_start, uint64_t seqno_len, uint64_t seqno_max)
{
    // xxxzzz placeholder for now
    rtm_get_cursors_op_t *op_data = STRUCT_NEW(rtm_get_cursors_op_t);
    plat_assert(op_data != NULL);
    op_data->shard_id = shard;
    op_data->rtm = test_model;
    op_data->seqno_start = seqno_start;
    op_data->seqno_len   = seqno_len;
    op_data->seqno_max   = seqno_max;
    rtm_general_op_t *ret = rtm_general_op_alloc(RTM_GO_GET_CURSORS, (void *)op_data, NULL);
    plat_assert(ret != NULL);
    return (ret);
}

int
rtm_get_cursors_complete(rtm_general_op_t *op, struct timeval now, SDF_status_t status,
                         const void *data, size_t data_len)
{
    // xxxzzz placeholder for now
    plat_assert(op->go_type == RTM_GO_GET_CURSORS);
    // rtm_get_cursors_op_t *op_data = (rtm_get_cursors_op_t *)(op->op_data);
    rtm_general_op_destroy(op);
    return (0);
}

rtm_general_op_t *
rtm_start_get_by_cursor(struct replication_test_model *test_model,
                        SDF_shardid_t shard, struct timeval now, vnode_t node,
                        const void *cursor, size_t cursor_len)
{
    // xxxzzz placeholder for now
    rtm_get_by_cursor_op_t *op_data = STRUCT_NEW(rtm_get_by_cursor_op_t);
    plat_assert(op_data != NULL);
    op_data->shard_id = shard;
    op_data->rtm = test_model;
    op_data->cursor = cursor;
    op_data->cursor_len = cursor_len;
    rtm_general_op_t *ret = rtm_general_op_alloc(RTM_GO_GET_BY_CURSOR, (void *)op_data, NULL);
    plat_assert(ret != NULL);
    return (ret);
}

int
rtm_get_by_cursor_complete(rtm_general_op_t *op, struct timeval now,
                           char *key, int key_len, SDF_time_t exptime,
                           SDF_time_t createtime, uint64_t seqno,
                           SDF_status_t status, const void *data, size_t data_len)
{
    // xxxzzz placeholder for now
    plat_assert(op->go_type == RTM_GO_GET_BY_CURSOR);
    // rtm_get_by_cursor_op_t *op_data = (rtm_get_by_cursor_op_t *)(op->op_data);
    rtm_general_op_destroy(op);
    return (0);
}


/**
 * @brief Start shard delete operation
 *
 * @param test_model <IN> Test model
 * @param now <IN> Simulated time at start of the request
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard <IN> Shard id to delete
 */
rtm_general_op_t *
rtm_start_delete_shard(struct replication_test_model *test_model,
                       sdf_replication_ltime_t ltime,
                       struct timeval now, vnode_t node,
                       SDF_shardid_t shard)
{
    if (!rtm_ctn_exist_sid(test_model->shard_status, shard)) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "shard not found when delete start,"
                     "node:%"PRIu32" shard_id %d.",
                     node, (int)shard);
        test_model->flags = 1;
    }
#if 0
    /* for every objects in this shard, commit a delete start */
    uint64_t i = 0;
    for (i = 0; i < test_model->obj_status->cur_size; i ++) {
        rtm_hash_entry_t *entry = test_model->obj_status->entries[i];
        if (entry && entry->shard_id == shard) {
            rtm_data_add(entry->data, RTM_DS, ltime, 0, 0);
        }
    }
#endif
    rtm_delete_shard_op_t *op_data = STRUCT_NEW(rtm_delete_shard_op_t);
    plat_assert(op_data != NULL);
    op_data->shard_id = shard;
    op_data->ltime = ltime;
    op_data->rtm = test_model;
    rtm_general_op_t *ret = rtm_general_op_alloc(RTM_GO_DEL_SHARD, (void *)op_data, NULL);
    plat_assert(ret != NULL);
    return (ret);
}

/**
 * Shard create operation completed
 *
 * Logs a message via #plat_log_msg when the status is unexpected and
 * indicates failure in the return.  Notably a shard delete will fail if
 * ltime has advanced.
 *
 * @param op <IN> operation returned from #rtm_delete_shard
 * @param now <IN> Simulated time at end of request
 * @param status <IN> returned status
 * @return 0 when status was accepted, non-zero when it is
 * inappropriate.
 */
int
rtm_delete_shard_complete(rtm_general_op_t *op,
                          struct timeval now, SDF_status_t status)
{
    plat_assert(op->go_type == RTM_GO_DEL_SHARD);
    rtm_delete_shard_op_t *op_data = (rtm_delete_shard_op_t *)(op->op_data);
    if (rtm_ctn_delete_sid(op_data->rtm->shard_status, op_data->shard_id)) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "shard not found when delete start,"
                     "shard_id %d.", (int)(op_data->shard_id));
        op_data->rtm->flags = 1;
        return (1);
    }
#if 0
    /* for every objects in this shard, commit a delete complete */
    uint64_t i = 0;
    for (i = 0; i < op_data->rtm->obj_status->cur_size; i ++) {
        rtm_hash_entry_t *entry = op_data->rtm->obj_status->entries[i];
        if (entry && entry->shard_id == op_data->shard_id) {
            rtm_data_add(entry->data, RTM_DC, op_data->ltime, 0, 0);
        }
    }
#endif
    rtm_general_op_destroy(op);
    return (0);
}


/**
 * @brief Start write operation
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param now <IN> Simulated time at start of the request
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param meta <IN> Meta-data associated with write including sie of key, data
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @param data <IN> Data
 * @param data_len <IN> Data length
 * @param meta <IN> Meta-data
 * @return  a handle on the write operation which is passed to
 * #rtm_write_complete on completion.
 */
rtm_general_op_t *
rtm_start_write(struct replication_test_model *test_model,
                SDF_shardid_t shard,
                sdf_replication_ltime_t ltime, struct timeval now,
                vnode_t node,
                struct replication_test_meta *meta, const void *key,
                size_t key_len, const void *data, size_t data_len,
                const struct replication_test_model_meta *model_meta)
{
    if (!rtm_ctn_exist_sid(test_model->shard_status, shard)) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "shard not found when write start,"
                     "node:%"PRIu32" shard_id %d.",
                     node, (int)shard);
        test_model->flags = 1;
//        return NULL;
    }

    rtm_data_t *rtm_data;
    rtm_data = rtm_hash_map_get(test_model->obj_status, (char *)key, key_len, shard);
    uint64_t data_hash = hash(data, data_len, 0);
    rtm_op_entry_t *entry = rtm_data_add(rtm_data, RTM_WS, data_hash, node, NULL);
    rtm_write_op_t *op = STRUCT_NEW(rtm_write_op_t);
    plat_assert(op != NULL);
    op->key = key;
    op->key_len = key_len;
    op->data_hash = data_hash;
    op->ltime = ltime;
    op->node = node;
    op->shard = shard;
    op->start_time = now;
    op->rtm = test_model;
    rtm_general_op_t *ret = rtm_general_op_alloc(RTM_GO_WR, (void *)op, entry);
    return (ret);
}

/**
 * @brief Write operation complete
 *
 * Logs a message via #plat_log_msg when the status is unexpected and
 * failure indicated in the return.  Notably a write started in ltime n
 * which was observed by a read in ltime n+1 to be incomplete
 * cannot complete successfully.
 *
 * @param op <IN> operation returned from #rtm_start_write
 * @param now <IN> Simulated time at end of request
 * @param status <IN> returned status
 * @return 0 when status was accepted, non-zero when it is
 * inappropriate.
 */
int
rtm_write_complete(rtm_general_op_t *op,
                   struct timeval now, SDF_status_t status)
{
    rtm_data_t *data;
    int ret = 0;
    plat_assert(op->go_type == RTM_GO_WR);
    rtm_write_op_t *op_data = (rtm_write_op_t *)(op->op_data);
    data = rtm_hash_map_get(op_data->rtm->obj_status,
                            (char *)(op_data->key), op_data->key_len, op_data->shard);
    if (status == SDF_SUCCESS /* plus here we need time checking */) {
        rtm_data_add(data, RTM_WC, op_data->data_hash, op_data->node, op->entry);
    }
    rtm_general_op_destroy(op);
    return (ret);
}

/**
 * @brief Start read operation
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param now <IN> Simulated time at start of the request
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @return  a handle on the read operation which is passed to
 * #rtm_read_complete on completion.
 */
rtm_general_op_t *
rtm_start_read(struct replication_test_model *test_model,
               SDF_shardid_t shard, sdf_replication_ltime_t ltime,
               const struct timeval now, vnode_t node,
               const void *key, size_t key_len)
{
    if (!rtm_ctn_exist_sid(test_model->shard_status, shard)) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "shard not found when read start,"
                     "node:%"PRIu32" shard_id %d.",
                     node, (int)shard);
        test_model->flags = 1;
//        return NULL;
    }
    rtm_data_t *rtm_data;
    rtm_data = rtm_hash_map_get(test_model->obj_status, (char *)key, key_len, shard);
    rtm_op_entry_t *entry = rtm_data_add(rtm_data, RTM_RS, 0, node, NULL);
    rtm_read_op_t *op = STRUCT_NEW(rtm_read_op_t);
    plat_assert(op != NULL);
    op->key = key;
    op->key_len = key_len;
    op->ltime = ltime;
    op->node = node;
    op->shard = shard;
    op->start_time = now;
    op->rtm = test_model;
    rtm_general_op_t *ret = rtm_general_op_alloc(RTM_GO_RD, (void *)op, entry);
    return (ret);
}

/**
 * @brief Read operation complete
 *
 * Logs a message when the status or result is incorrect and indicates
 * the faiure in the return.  Notably read cannot observe a value older
 * than one which has been observed by another read.
 *
 * @param op <IN> operation returned from #rtm_start_read
 * @param now <IN> Simulated time at end of request
 * @param status <IN> returned status
 * @param data <IN> returned data
 * @param data_len <IN> data length
 * @return 0 when status was accepted, non-zero when it is
 * inappropriate.  Notably a write started in ltime n
 * which was observed by a read in ltime n+1 to be incomplete
 * cannot complete successfully.
 */
int
rtm_read_complete(rtm_general_op_t *op,
                  struct timeval now, SDF_status_t status,
                  const void *data, size_t data_len)
{
    int ret = 0;
    plat_assert(op->go_type == RTM_GO_RD);
    rtm_read_op_t *op_data = (rtm_read_op_t *)(op->op_data);
    uint64_t data_hash;
    if (status == SDF_SUCCESS && data != NULL && data_len != 0) {
        data_hash = hash(data, data_len, 0);
    } else {
        data_hash = 0;
    }
    rtm_data_t *rtm_data =
        rtm_hash_map_get(op_data->rtm->obj_status,
                         (char *)(op_data->key), op_data->key_len, op_data->shard);
    if (status == SDF_SUCCESS || status == SDF_OBJECT_UNKNOWN) {
        rtm_data_add(rtm_data, RTM_RC, data_hash, op_data->node, op->entry);
        ret = rtm_data_verify(rtm_data);
        if (ret) {
            op_data->rtm->flags = 1;
        } else {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                         "read verification key:%s data:%s successful!",
                         (char *)(op_data->key), (char *)(data));
        }
    }
    rtm_general_op_destroy(op);
    return (ret);
}

/**
 * @brief Start delete operation
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param now <IN> Simulated time at start of the request
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @return a handle on the read operation which is passed to
 * #rtm_read_complete on completion.
 */
rtm_general_op_t *
rtm_start_delete(struct replication_test_model *test_model,
                 SDF_shardid_t shard, sdf_replication_ltime_t ltime,
                 struct timeval now, vnode_t node,
                 const void *key, size_t key_len)
{
    if (!rtm_ctn_exist_sid(test_model->shard_status, shard)) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "shard not found when delete start, shard_id %d.", (int)shard);
        test_model->flags = 1;
//        return NULL;
    }
    rtm_data_t *rtm_data;
    rtm_data = rtm_hash_map_get(test_model->obj_status, (char *)key, key_len, shard);
    rtm_op_entry_t *entry = rtm_data_add(rtm_data, RTM_DS, 0, node, NULL);
    rtm_delete_op_t *op = STRUCT_NEW(rtm_delete_op_t);
    plat_assert(op != NULL);
    op->key = key;
    op->key_len = key_len;
    op->ltime = ltime;
    op->node = node;
    op->shard = shard;
    op->start_time = now;
    op->rtm = test_model;
    rtm_general_op_t *ret = rtm_general_op_alloc(RTM_GO_DEL, (void *)op, entry);
    return (ret);
}

/**
 * @brief Delete operation complete
 *
 * Logs a message via #plat_log_msg when the status is unexpected and
 * indeicates failure in the return.  Notably a delete that starts in
 * ltime n will fail if a read in ltime n+1 successfully returns data.
 *
 * @param op <IN> operation returned from #rtm_start_write
 * @param now <IN> Simulated time at end of request
 * @param status <IN> returned status
 * @return 0 when status was accepted, non-zero when it is
 * inappropriate.
 */
int
rtm_delete_complete(rtm_general_op_t *op,
                    struct timeval now, SDF_status_t status)
{
    int ret = 0;
    rtm_data_t *data;
    plat_assert(op->go_type == RTM_GO_DEL);
    rtm_write_op_t *op_data = (rtm_write_op_t *)(op->op_data);
    data =
        rtm_hash_map_get(op_data->rtm->obj_status,
                         (char *)(op_data->key), op_data->key_len, op_data->shard);
    if (status == SDF_SUCCESS /* plus here we need time checking */) {
        rtm_data_add(data, RTM_DC, op_data->data_hash, op_data->node, op->entry);
    }
    rtm_general_op_destroy(op);
    return (ret);
}
