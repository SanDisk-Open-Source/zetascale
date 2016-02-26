/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fcnl_rtf_stress_test.c
 * Author: Songhe
 *
 * Created on Jun 12, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 */

/**
 * Test switch-over, recovery, switch-back
 */

#include "fth/fthOpt.h"
#include "platform/stdio.h"

#include "protocol/replication/copy_replicator.h"
#include "test_framework.h"

#undef MILLION
#define MILLION 1000000

#define RT_USE_COMMON 1
#include "test_common.h"

/**
 * We use a sub-category under test because test implies a huge number
 * of log messages out of simulated networking, flash, etc.
 */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION, "test/case");

#define PLAT_OPTS_NAME(name) name ## _sync_test
#include "platform/opts.h"
#include "misc/misc.h"

#define PLAT_OPTS_ITEMS_sync_test() \
    PLAT_OPTS_COMMON_TEST(common_config)

#define SLEEP_US 1000000

struct plat_opts_config_sync_test {
    struct rt_common_test_config common_config;
};
#define REPLICATION_TEST_RUN_T

#define RANDOM_OBJ_SIZE
#define RANDOM_KEY_LEN

#define MAX_KEY_LEN  512
#define MIN_KEY_LEN  10

#define MAX_OBJECT_SIZE 4096
#define MIN_OBJECT_SIZE 20

#define MAX_OBJECTS_NUM         (1024 * 2)

#define REP_STRESS_WORKERS      2
#define REP_FTH_STACKSIZE       81920

static int    syncControl  = 0;


static int rep_stress_workers = REP_STRESS_WORKERS;
static int total_objects_num  = 0;

static uint64_t max_objects_num    = MAX_OBJECTS_NUM;
static uint32_t max_object_size    = MAX_OBJECT_SIZE;
static uint32_t min_object_size    = MIN_OBJECT_SIZE;
static uint16_t max_key_len        = MAX_KEY_LEN;
static uint16_t min_key_len        = MIN_KEY_LEN;

static int is_diff_keys    = 1;
static int random_obj_size = 0;
static int random_key_len  = 0;

static int err_count = 0;
static int suc_count = 0;

/*
 * create, set, delete, get
 * only used by method "rt_operations_entry"
 * each key has it own function
 */
int create_rate = 0, set_rate = 0, delete_rate = 0, get_rate = 0;
int hit_rate = 0, write_rate = 20;
int op_type = 0;

typedef struct fth_arg {
    uint32_t       id;
    uint64_t       objs;
    uint64_t       max_objs;
    /**
     * Drop shard and node
     */
    SDF_shardid_t  shard;
    vnode_t        node;
    void          *ctxt;
}fth_arg_t;

enum {
    KEY_PUT        = 0,     // put object to home node
    KEY_SET        = 1,     // set object to home node
    KEY_DEL        = 2,     // delete object to home node
    KEY_GET        = 3,     // get object from home node or replica node
    KEY_RANDOM     = 4,     // random get an operation
    KEY_NONE       = 5      // no operation, just do crash/restart
};

/**
 * 1. the test case must management the shard's node list
 * which node is its home node, which node is its replica
 * node
 *
 * 2. How to choose the max value of these variable
 */
#define MAX_NODE_NUM            32
#define MAX_SHARD_NUM           32
#define MAX_REPLICA_NUM         32
#define MAX_SHARD_ID            1024

static uint32_t rep_node_num;
static uint32_t rep_replica_num;
static uint32_t rep_shard_num;
static uint32_t bt_event_freq;

/**
 * the client how to manage node is a big problem
 * need upgrade
 */
enum rt_node_state {
    RT_NODE_DEAD,
    RT_NODE_LIVE
};

typedef struct rt_node {
    vnode_t            id;
    enum rt_node_state state;
}rt_node_t;

typedef struct rt_shardid {
    vnode_t   current_home_node;
    /**
     * locate at these nodes
     */
    rt_node_t nodes[MAX_REPLICA_NUM];
    uint32_t ref_count;
}rt_shard_t;

/**
 * Record the first shard id,
 * then shard id range: "first_shard_id ~ first_shard_id + rep_shard_num -1"
 */
SDF_shardid_t initial_shard_id = 100;
SDF_shardid_t first_shard_id   = 100;
rt_shard_t shard_list[MAX_SHARD_ID];

/**
 * This is a good template
 */
typedef struct {
    /**
     * test if the object exist or not
     * 0 => not exist in ssd
     * 1 => exist in ssd
     */
    uint8_t exist;
    /**
     * if the slab eviction happens and so many operations operate that key, generate fail, then as
     * one error ? If i can know the keys of slab eviction or hash eviction seems much better
     */
    uint8_t eviction;
    /**
     * the offset and the size => object content
     */
    uint32_t offset;
    uint32_t size;

    /**
     * shard id, write this object to this shard,
     * don't care who is the current shard's home
     * node
     */
    SDF_shardid_t shard;

    /**
     * when the operation is write operation,
     * home node VS replica node
     */
    vnode_t node;

    /**
     * when the node is dead, don't need do write/read
     */
    SDF_boolean_t live;
    /**
     * when more than one fth operate the same key, need lock it first
     */
    fthLock_t lock;

    /**
     * key => object
     */
    char *key;
}rep_wrapper_t;

/**
 * data_table has a lot of random data, you can choose random field to write into flash
 */
#define TOTAL_TABLE_SIZE 2097152  // the real size is TOTAL_TABLE_SIZE + 2M
static char *data_table   = NULL;
rep_wrapper_t *rep_keyset = NULL;

static fthThread_t *fth_entry = NULL;

int
shuffle_v1(int *ptr, int length)
{

    int i, j, tmp;
    for (i = length - 1; i >= 1; i--) {
        j = random() % i;
        tmp = ptr[j];
        ptr[j] = ptr[i];
        ptr[i] = tmp;
    }
    return (0);
}

static int
shuffle_v2(int *ptr, int length)
{
    int i, j, tmp;
    for (i = 0; i < length; i++) {
        int rnd = random();
        j = (int)(rnd % (length - i)) + i;

        tmp = ptr[j];
        ptr[j] = ptr[i];
        ptr[i] = tmp;
    }
    return (0);
}

/**
 * according shard id, get its home node
 * but how to get it easily?
 */
vnode_t
get_home_node(SDF_shardid_t shard)
{
    vnode_t current_home_node = 1;
    current_home_node  = shard_list[shard].current_home_node;
    return (current_home_node);

}

SDF_status_t
rt_shard_init(SDF_shardid_t shard, vnode_t initial_home_node)
{

    int i;
    plat_assert(shard < MAX_SHARD_ID);
    rt_shard_t *current_shard = &shard_list[shard];
    current_shard->current_home_node = initial_home_node;
    current_shard->ref_count = 0;
    for (i = 0; i < rep_replica_num; i ++) {
        current_shard->nodes[i].id = initial_home_node % rep_node_num;
        current_shard->nodes[i].state = RT_NODE_LIVE;
        initial_home_node ++;
    }

    return (SDF_SUCCESS);
}

static void
user_notification_cb(plat_closure_scheduler_t *context, void *env,
                     int events, struct cr_shard_meta *shard_meta,
                     enum sdf_replicator_access access, struct timeval expires,
                     sdf_replicator_notification_complete_cb_t complete,
                     vnode_t node) {
    struct replication_test_framework *test_framework =
        (struct replication_test_framework *)env;
    int us_sleep, index = -1;
    int i;

    plat_assert(shard_meta != NULL);

    /* sleep a random seconds to gernates more notifies */
    us_sleep =
        plat_prng_next_int(test_framework->api->prng,
                           test_framework->config.replicator_config.lease_usecs);

    for (i = first_shard_id; i < rep_shard_num + first_shard_id; i++) {
        if (i == shard_meta->persistent.sguid) {
            index = i;
            break;
        }
    }
    plat_assert(-1 != index && 1 >= shard_list[index].ref_count);

    (void) __sync_fetch_and_add(&shard_list[index].ref_count, 1);
    shard_list[index].current_home_node = shard_meta->persistent.current_home_node;

    cr_shard_meta_free(shard_meta);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "shard_meta: %p, ltime: %d, shard: %d, "
                 "seqno: %d, home_node: %d, access: %d, working node: %u",
                 shard_meta, shard_meta->persistent.ltime,
                 (int)shard_meta->persistent.sguid,
                 shard_meta->persistent.shard_meta_seqno,
                 shard_meta->persistent.current_home_node,
                 access, node);

    plat_closure_apply(sdf_replicator_notification_complete_cb, &complete);
    (void) __sync_fetch_and_sub(&shard_list[index].ref_count, 1);
}


static void
rt_shards_liveness_change(vnode_t pnode, enum rt_node_state state)
{
    SDF_shardid_t index = first_shard_id;
    for (; index < first_shard_id + rep_shard_num; index ++) {
        if ((shard_list[index].current_home_node == pnode) && (state == RT_NODE_DEAD)) {
            /**
             * If the crash node is home node, then shard's home node changed
             */
        }
        for (int i = 0; i < rep_replica_num; i ++) {
            if (shard_list[index].nodes[i].id == pnode) {
                shard_list[index].nodes[i].state = state;
            }
        }
    }

}

static enum rt_node_state
rt_node_state(SDF_shardid_t shard, vnode_t pnode) {
    for (int i = 0; i < rep_replica_num; i ++) {
        if (shard_list[shard].nodes[i].id == pnode) {
            return (shard_list[shard].nodes[i].state);
        }
    }
    return (RT_NODE_LIVE);
}

static int
init_cards(int *ptr, int length)
{
    int index;
    for (index = 0; index < length; index++) {
        ptr[index] = index;
    }
    return (0);
}

static SDF_boolean_t
data_table_init(uint32_t real_size)
{
    int i, j;

    if (data_table == NULL) {
        return (1);
    }
    char number_to_char[62] = { 0 }; // number -> char
    for (i = 0; i < 10; i++) {
        number_to_char[i] = '0' + i;
    }

    for (i = 10; i < 36; i++) {
        number_to_char[i] = 'A' + i - 10;
    }

    for (i = 36; i < 62; i++) {
        number_to_char[i] = 'a' + i - 36;
    }

    for (i = 0; i < real_size; i++) {
        j = random() % 62;
        data_table[i] = number_to_char[j];
    }
    return (0);
}

static char *
rep_random_key(uint16_t key_len) {
    char *buf = NULL;
    int i, j;
    buf = (char *)plat_alloc(key_len + 1);
    plat_assert(buf);
    buf[key_len] = '\0';
    char number_to_char[62] = { 0 }; // number -> char

    for (i = 0; i < 10; i++) {
        number_to_char[i] = '0' + i;
    }

    for (i = 10; i < 36; i++) {
        number_to_char[i] = 'A' + i - 10;
    }

    for (i = 36; i < 62; i++) {
        number_to_char[i] = 'a' + i - 36;
    }

    for (i = 0; i < key_len; i++) {
        j = random() % 62;
        buf[i] = number_to_char[j];
    }
    return (buf);
}

/**
 * PRE_Create a lot of keys and initial the field of the keyset
 */
static void
rep_keyset_init(uint64_t objs, uint16_t max_key_len)
{

    uint16_t key_len = max_key_len;
    rep_keyset = (rep_wrapper_t *)plat_alloc(objs * sizeof(rep_wrapper_t));
    plat_assert(rep_keyset);

    SDF_shardid_t shard;
    vnode_t node;

    for (uint64_t index = 0; index < objs; index ++) {
        if (random_key_len) {
            key_len = min_key_len + random() % (max_key_len - min_key_len);
        }
        /**
         * shard range: [first_shard_id, (first_shard_id + rep_shard_num - 1)]
         */
        shard = first_shard_id + random() % rep_shard_num;
        node = get_home_node(shard);

        /**
         * default: random a key
         */
        rep_keyset[index].key    = rep_random_key(key_len);
        /**
         * default: object not exist
         */
        rep_keyset[index].exist  = 0;
        /**
         * default: error count is 0
         */
        rep_keyset[index].eviction = 0;
        /**
         * default: index is -1, size is 0
         */
        rep_keyset[index].offset  = 0;
        rep_keyset[index].size   = 0;

        /**
         * default: if replica number is 1, then shard is 1, node is 1
         */
        rep_keyset[index].shard = shard;
        rep_keyset[index].node = node;

        /**
         * init the lock for each key set
         */
        fthLockInit(&rep_keyset[index].lock);
    }
}

static void
rep_keyset_free(uint64_t keyset_size)
{

    if (rep_keyset) {
        for (uint64_t index = 0; index < keyset_size; index ++) {
            if (rep_keyset[index].key) plat_free(rep_keyset[index].key);
        }
        plat_free(rep_keyset);
    }
}

static uint64_t
get_fail_count(uint64_t keyset_size)
{

    uint64_t count = 0;
    if (rep_keyset) {
        for (uint64_t index = 0; index < keyset_size; index ++) {
            if (rep_keyset[index].eviction == 1) count ++;
        }
    }
    return (count);
}

/**
 * PRE-Create some objects and initial its content
 */
static SDF_status_t
rep_create_objs(void *ctxt, uint64_t objs, uint32_t max_object_size, uint16_t key_len, char **fail_key)
{

    SDF_status_t status = SDF_SUCCESS;
    char *key = NULL, *src = NULL;
    uint32_t obj_size = max_object_size, offset = 0;

    struct replication_test_framework *framework = (struct replication_test_framework *)ctxt;

    /**
     * temp default value
     */
    SDF_shardid_t shard = 1;
    vnode_t node = get_home_node(shard);

    total_objects_num = objs;

    /**
     * remember: just use part of keys
     */
    rep_keyset_init(max_objects_num, key_len);

    for (int index = 0; index < objs; index++) {
        key = rep_keyset[index].key;
        if (random_obj_size) {
            /**
             * the smallest data size is 1, the biggest data size is data_size
             */
            obj_size = min_object_size + (random() % (max_object_size - min_object_size));
        }

        offset = random() % TOTAL_TABLE_SIZE;
        src = data_table + offset;

        /**
         * random get shard and node which are generated in "rep_keyset_init"
         */
        shard = rep_keyset[index].shard;
        /**
         * maybe home node has been changed, so get again
         */
        node  = get_home_node(shard);

        status = rtfw_write_sync(framework, shard, node, meta, key, strlen(key) + 1, src, obj_size);

        if (status != SDF_SUCCESS) {
            *fail_key = key;
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "PRE-Create %d object[%s] on node %u:shard %lu failed",
                         index, *fail_key, node, shard);
            break;
        } else {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "PRE-Create object[%s] on node %u:shard %lu successful",
                         key, node, shard);
            /**
             * only when the operation successful,then can update the field of key wrapper
             */
            rep_keyset[index].exist = 1;
            rep_keyset[index].eviction = 0;
            rep_keyset[index].offset = offset;
            rep_keyset[index].size = obj_size;
            rep_keyset[index].shard = shard;
            rep_keyset[index].node = node;
        }
    }
    return (status);
}

static SDF_boolean_t
compare_data(char *dest, char *src, uint32_t length)
{

    for (uint32_t index = 0; index < length; index ++) {
        if (src[index] != dest[index]) {
            return (1);
        }
    }
    return (0);
}

/**
 * save the data-table info file at the first running
 */
static void
save_rt_data_table(char *s_fname, uint32_t real_size)
{

    FILE *fd;
    fd = fopen(s_fname, "w+");
    plat_assert(fd != NULL);
    printf("$$ write data table into file: %s\n", s_fname);

    fwrite(data_table, sizeof(char), real_size, fd);
    fclose(fd);
}

/**
 * recover the data-table from file at the beginning of program
 */
static void
recover_rt_data_table(char *s_fname, uint32_t real_size)
{

    FILE *fd;
    fd = fopen(s_fname, "r+");
    plat_assert(fd != NULL);
    printf("$$ read data table from file: %s\n", s_fname);

    fread(data_table, sizeof(char), real_size, fd);
    fclose(fd);
}

/**
 * save the key-set into file at the checkpoint
 */
static void
write_keyset_into_file(char *s_fname, uint64_t total_objs)
{

    FILE *fd;
    fd = fopen(s_fname, "w+");
    plat_assert(fd != NULL);
    printf("$$ write key set in file: %s\n", s_fname);
    for (uint64_t index = 0; index < total_objs; index ++) {
        /**
         * write sequence: exist, eviction, offset, size, key
         */
        fprintf(fd, "%d,%d,%i,%i,%s\n",
                rep_keyset[index].exist, rep_keyset[index].eviction,
                rep_keyset[index].offset, rep_keyset[index].size,
                rep_keyset[index].key);
    }

    fclose(fd);
}

/**
 * recover the key-set from file which saved at checkpoint
 */
static void
read_keyset_from_file(char *s_fname, uint64_t total_objs)
{

    FILE *fd;
    fd = fopen(s_fname, "r+");
    plat_assert(fd != NULL);

    rep_keyset = (rep_wrapper_t *)plat_alloc(total_objs * sizeof(rep_wrapper_t));
    plat_assert(rep_keyset);

    for (uint64_t index = 0; index < total_objs; index ++) {
        /**
         * read sequence: exist, eviction, offset, size, key
         */
        rep_keyset[index].key = (char *)plat_alloc(max_key_len);
        plat_assert(rep_keyset[index].key);
        memset(rep_keyset[index].key, '\0', max_key_len);

        fscanf(fd, "%i,%i,%i,%i,%s\n",
               (int *)&rep_keyset[index].exist, (int *)&rep_keyset[index].eviction,
               &rep_keyset[index].offset, &rep_keyset[index].size,
               rep_keyset[index].key);

        /**
         * initial the lock for each key
         */
        fthLockInit(&rep_keyset[index].lock);
    }

    fclose(fd);
}

/**
 * this is an fth thread entry, each fth thread call the same entry,
 * each fth thread can visit any live node, any exist shard, any operations
 */
void
rt_operations_entry(uint64_t arg)
{

    fth_arg_t *temp = (fth_arg_t *)arg;
    int key_type, t = 0;
    SDF_status_t status;

    uint32_t cur_fth         = temp->id;
    uint64_t objs            = temp->max_objs;
    void * ctxt              = temp->ctxt;
    SDF_shardid_t shard      = temp->shard;
    vnode_t node             = temp->node;    // initial home node, will dynamic change
    enum rt_node_state node_state = RT_NODE_LIVE;

    char *key = NULL, *src = NULL;
    fthWaitEl_t *wait = NULL;

    /**
     * test framework context
     */
    struct replication_test_framework *framework = (struct replication_test_framework *)ctxt;
    replication_test_framework_read_data_free_cb_t free_cb;

    /**
     * used by get operation
     */
    uint32_t data_size = 0;
    char *dest = NULL;

    /**
     * used by create and set operation
     */
    uint32_t obj_size = max_object_size, offset;

    /**
     * used by all of operations
     */
    int *fcards = (int *)plat_alloc(max_objects_num * sizeof(int));
    plat_assert(fcards);
    init_cards(fcards, max_objects_num);
    shuffle_v2(fcards, max_objects_num);

    int index = -1;
    for (int i = 0; i < objs; i ++) {

        /**
         * random choose a key from key set
         */

        if (is_diff_keys) {
#ifdef RANDOM_KEY
            index = random() % max_objects_num;
#else
            /**
             * this case can cover all of keys, and each key will have "fth-count" operate on it
             */
            index = fcards[i];
#endif
        } else {
            index = 0;
        }
        key = rep_keyset[index].key;
        shard = rep_keyset[index].shard;

        /**
         * random choose a operation from operation set
         */
        switch (op_type) {
        case 0:
            key_type = KEY_PUT;
            break;
        case 1:
            key_type = KEY_SET;
            break;
        case 2:
            key_type = KEY_DEL;
            break;
        case 3:
            key_type = KEY_GET;
            break;
        case 4:
            key_type = random() % KEY_RANDOM;
            break;
        default:
            key_type = KEY_NONE;
            break;
        }

        switch (key_type) {
        case KEY_PUT:
            /**
             * address in data table
             */
            offset = random() % TOTAL_TABLE_SIZE;
            src = data_table + offset;

            wait = fthLock(&rep_keyset[index].lock, 1, NULL);

            /**
             * when do write operation, should write to home node
             * current replication just home node has RW permition
             * and confirm home node is still live
             */
            node = get_home_node(shard);
            if (rt_node_state(shard, node) == RT_NODE_DEAD) {
                fthUnlock(wait);
                break;
            }

            if (random_obj_size) {
                obj_size = min_object_size + (random() % (max_object_size - min_object_size));
            }

            status = rtfw_write_sync(framework, shard, node, meta, key, strlen(key) + 1, src, obj_size);

            if (status == SDF_SUCCESS) {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                             "fth %d: Put object[%s] on node %u:shard %lu success",
                             cur_fth, key, node, shard);

                (void) __sync_fetch_and_add(&suc_count, 1);
                /**
                 * update this key wrapper
                 */
                rep_keyset[index].exist  = 1;
                rep_keyset[index].offset = offset;
                rep_keyset[index].size   = obj_size;
            } else if (status != SDF_SUCCESS) {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                             "fth %d: Put object[%s] on node %u:shard %lu failed",
                             cur_fth, key, node, shard);

                (void) __sync_fetch_and_add(&err_count, 1);
            }

            fthUnlock(wait);
            break;
        case KEY_SET:
            /**
             * address in data table
             */
            offset = random() % TOTAL_TABLE_SIZE;
            src = data_table + offset;

            wait = fthLock(&rep_keyset[index].lock, 1, NULL);
            /**
             * Confirm home node is still live
             */
            node = get_home_node(shard);
            if (rt_node_state(shard, node) == RT_NODE_DEAD) {
                fthUnlock(wait);
                break;
            }

            if (random_obj_size) {
                obj_size = min_object_size + (random() % (max_object_size - min_object_size));
            }

            status = rtfw_write_sync(framework, shard, node, meta, key, strlen(key) + 1, src, obj_size);

            if (status == SDF_SUCCESS) {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                             "fth %d: Set object[%s] to node %u:shard %lu success",
                             cur_fth, key, node, shard);

                (void) __sync_fetch_and_add(&suc_count, 1);
                rep_keyset[index].exist = 1;
                rep_keyset[index].offset = offset;
                rep_keyset[index].size  = obj_size;
            } else if (status != SDF_SUCCESS) {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                             "fth %d: Set object[%s] to node %u:shard %lu failed",
                             cur_fth, key, node, shard);

                (void) __sync_fetch_and_add(&err_count, 1);
            }

            fthUnlock(wait);
            break;
        case KEY_DEL:

            wait = fthLock(&rep_keyset[index].lock, 1, NULL);

            /**
             * Confirm home node is still live
             */
            node = get_home_node(shard);
            if (rt_node_state(shard, node) == RT_NODE_DEAD) {
                fthUnlock(wait);
                break;
            }
            status = rtfw_delete_sync(framework, shard, node, key, strlen(key) + 1);

            if (status == SDF_SUCCESS) {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                             "fth %d: Delete object[%s] to node %u:shard %lu success",
                             cur_fth, key, node, shard);

                (void) __sync_fetch_and_add(&suc_count, 1);
                rep_keyset[index].exist = 0;
                rep_keyset[index].offset = 0;
                rep_keyset[index].size  = 0;

            } else if (status != SDF_SUCCESS) {
                if (rep_keyset[index].exist) {
                    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                                 "fth %d: Delete object[%s] to node %u:shard %lu failed, but object exist",
                                 cur_fth, key, node, shard);

                    rep_keyset[index].eviction = 1;
                    (void) __sync_fetch_and_add(&err_count, 1);
                } else {
                    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                                 "fth %d: Delete object[%s] to node %u:shard %lu success, because object not exist",
                                 cur_fth, key, node, shard);

                    rep_keyset[index].exist = 0;
                    rep_keyset[index].offset = 0;
                    rep_keyset[index].size  = 0;
                    (void) __sync_fetch_and_add(&suc_count, 1);
                }
            }

            fthUnlock(wait);
            break;
        case KEY_GET:
            wait = fthLock(&rep_keyset[index].lock, 0, NULL);
            /**
             * read object from home node and replica node
             */
            for (t = 0; t < rep_replica_num; t++) {

                node = shard_list[shard].nodes[t].id;
                node_state = shard_list[shard].nodes[t].state;

                /**
                 * current revision replication just can do write to home node or read from home node
                 */
                if (node_state == RT_NODE_DEAD || node != shard_list[shard].current_home_node) {
                    continue;
                }
                status = rtfw_read_sync(framework, shard, node,
                                        (void *) key, (size_t)(strlen(key) + 1),
                                        (void **) &dest, (size_t *)&data_size,
                                        &free_cb);

                if (status == SDF_SUCCESS) {
                    src = data_table + rep_keyset[index].offset;
                    if ((data_size == rep_keyset[index].size) && !compare_data(dest, src, data_size)) {
                        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                                     "fth %d: Get object[%s] from node %u:shard %lu success",
                                     cur_fth, key, node, shard);

                        (void) __sync_fetch_and_add(&suc_count, 1);
                    } else {
                        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                                     "fth %d: Get object[%s] from node %u:shard %lu failed, the content not right",
                                     cur_fth, key, node, shard);

                        (void) __sync_fetch_and_add(&err_count, 1);
                    }
                    plat_closure_apply(replication_test_framework_read_data_free_cb, &free_cb, dest, data_size);
                } else {
                    if (rep_keyset[index].exist) {
                        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                                     "fth %d: Get object[%s] from node %u:shard %lu failed, the object exist",
                                     cur_fth, key, node, shard);
                        rep_keyset[index].eviction = 1;
                        (void) __sync_fetch_and_add(&err_count, 1);
                    } else {
                        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                                     "fth %d: Get object[%s] from node %u:shard %lu success, the object not exist)",
                                     cur_fth, key, node, shard);
                        (void) __sync_fetch_and_add(&suc_count, 1);
                    }
                }
            }
            fthUnlock(wait);
            break;
        default:
            break;
        }

        /**
         *  crash node or restart node
         *  CHN, CRN, RHN, RRN
         */
        if (!((i + 1) % bt_event_freq)) {

            SDF_boolean_t do_crash = 1;
            vnode_t       rt_node_id;
            uint32_t      rt_node_index;

            rt_shard_t *temp = &shard_list[shard];

            /**
             * random select a node, test it dead/live => restart/crash
             */
            rt_node_index = random() % rep_replica_num;

            if (temp->nodes[rt_node_index].state == RT_NODE_DEAD) {
                /**
                 * this node dead now, restart it
                 */
                do_crash   = 0;
                rt_node_id = temp->nodes[rt_node_index].id;
            } else {
                /**
                 * this node live now, crash it
                 */
                do_crash = 1;
                rt_node_id = temp->nodes[rt_node_index].id;
            }

            if (rt_node_id == 0) {
                /**
                 * Don't touch node 0, it's super node
                 */
                continue;
            }

            if (do_crash) {

                printf("$$$$$$$$$ crash node %u\n", rt_node_id);
                status = rtfw_crash_node_sync(framework, rt_node_id);
                plat_assert(status == SDF_SUCCESS);

                rtfw_sleep_usec(framework, framework->config.replicator_config.lease_usecs * 2);

                /**
                 * crash node is home node, who is the new home node ?
                 * it's better meta_storage.c tell me or round-robin read an key from
                 * node, success => it's home node
                 */
                rt_shards_liveness_change(rt_node_id, RT_NODE_DEAD);

                plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                             "fth %d: crash node %d success, shard: %lu, next home node: %d",
                             cur_fth, rt_node_id, shard, temp->current_home_node);

            } else {
                printf("$$$$$$$$$ restart node %u\n", rt_node_id);
                status = rtfw_start_node(framework, rt_node_id);
                plat_assert(status == SDF_SUCCESS);
                plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                             "fth %d: restart crashed node %d success, shard: %lu, next home node: %d",
                             cur_fth, rt_node_id, shard, temp->current_home_node);
                rtfw_sleep_usec(framework, framework->config.replicator_config.lease_usecs * 2);

                rt_shards_liveness_change(rt_node_id, RT_NODE_LIVE);
            }

        }

    }

    /**
     * let all of fth thread finish running main target, then just let
     * fth thread 0 do some sample things
     */
    (void) __sync_fetch_and_add(&syncControl, 1);
    while (syncControl % rep_stress_workers != 0) {
        fthYield(-1);
    }

    if (fcards) plat_free(fcards);

    if (cur_fth == 0) {
        /**
         * let the first fthreads resume the entry fthreads
         */
        fthResume(fth_entry, 0);
    }
}

void
rep_worker_thread_create(uint64_t arg)
{

    char *fail_key = NULL, *df_name = NULL, *kf_name = NULL;

    int first_run = 1, failed = 0;

    vnode_t first_home_node;
    SDF_boolean_t op_ret;
    SDF_shardid_t shard_id;

    struct replication_test_framework *test_framework = NULL;
    SDF_replication_props_t *replication_props = NULL;
    struct SDF_shard_meta *shard_meta = NULL;
    rtfw_replicator_notification_cb_t notify_cb;

    test_framework = (struct replication_test_framework *)arg;

    fth_entry = fthSelf();

#ifdef REPLICATION_TEST_RUN_T

    /**
     * get these variables's value from environment
     */
    rep_shard_num       = atoi(getenv("REP_SHARD_NUM"));
    max_objects_num     = atoi(getenv("MAX_OBJECTS_NUM"));
    rep_stress_workers  = atoi(getenv("REP_STRESS_WORKERS"));
    is_diff_keys        = atoi(getenv("IS_DIFF_KEYS"));
    random_obj_size     = atoi(getenv("RANDOM_OBJ_SIZE"));
    random_key_len      = atoi(getenv("RANDOM_KEY_LEN"));
    max_object_size     = atoi(getenv("MAX_OBJECT_SIZE"));
    max_key_len         = atoi(getenv("MAX_KEY_LEN"));

    min_object_size     = atoi(getenv("MIN_OBJECT_SIZE"));
    min_key_len         = atoi(getenv("MIN_KEY_LEN"));

    hit_rate            = 100 - atoi(getenv("MISS_RATE"));

    first_run           = atoi(getenv("FIRST_RUN"));
    df_name             = getenv("DATA_TABLE_FILE_NAME");
    kf_name             = getenv("KEY_SET_FILE_NAME");

    first_home_node     = atoi(getenv("FIRST_HOME_NODE"));
    bt_event_freq       = atoi(getenv("BT_EVENT_FREQ"));
    op_type             = atoi(getenv("OP_TYPE"));

    /**
     * check the variables
     */
    plat_assert(is_diff_keys == 0 || is_diff_keys == 1);
    plat_assert(random_obj_size == 0 || random_obj_size == 1);
    plat_assert(random_key_len == 0 || random_key_len == 1);
    plat_assert(max_key_len > min_key_len);
    plat_assert(max_object_size > min_object_size);
    plat_assert(df_name);
    plat_assert(kf_name);
    plat_assert(first_home_node < rep_node_num);
    plat_assert(rep_shard_num <= MAX_SHARD_NUM);

#else
    rep_shard_num = 1;
    max_objects_num = 200;
    rep_stress_workers = 1;
    is_diff_keys = 1;
    random_obj_size = 1;
    random_key_len  = 1;
    max_object_size = 2048;
    max_key_len = 20;
    min_object_size = 1024;
    min_key_len = 10;

    hit_rate = 100;
    first_run = 1;
    df_name = "df_name";
    kf_name = "kf_name";

    first_home_node = 1;
    bt_event_freq  = 10;
    op_type = KEY_GET;

#endif

    /**
     * declare "struct replication_test_meta *meta " in "test_model.h"
     */
    failed = !plat_calloc_struct(&meta);
    plat_assert(!failed);
    replication_test_meta_init(meta);

    // start replication test framework
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start replication test framework");
    rtfw_start(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "replication test framework started\n");

    // start all nodes, home node and replica node
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start nodes");
    rtfw_start_all_nodes(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "nodes started");

    failed = !plat_calloc_struct(&replication_props);
    plat_assert(!failed);
    rtfw_set_default_replication_props(&test_framework->config, replication_props);


    /* set notication cb */
    notify_cb =
        rtfw_replicator_notification_cb_create(test_framework->closure_scheduler,
                                               &user_notification_cb,
                                               test_framework);

    rtfw_set_replicator_notification_cb(test_framework, notify_cb);

    /**
     * 1. enhancement
     * Create multi-thread
     */
    for (uint32_t i = 0; i < rep_shard_num; i ++) {

        /**
         * shard_meta_id = __sync_add_and_fetch(&test_framework->max_shard_id, 1);
         */

        shard_id = __sync_fetch_and_add(&initial_shard_id, 1);
        if (i == 0) {
            /**
             * Record the first shard ID
             */
            first_shard_id = shard_id;
        }
        shard_meta = rtfw_init_shard_meta(&test_framework->config,
                                          first_home_node % rep_node_num /* first node */, shard_id
                                          /* shard_id, in real system generated by generate_shard_ids() */,
                                          replication_props);

        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "create on node %d", first_home_node % rep_node_num);
        op_ret = rtfw_create_shard_sync(test_framework, first_home_node % rep_node_num, shard_meta);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "create on node %d complete", first_home_node % rep_node_num);
        plat_assert(op_ret == SDF_SUCCESS);

        rt_shard_init(shard_id, first_home_node % rep_node_num);
        first_home_node ++;
    }
    /**
     * malloc a large data table and initial it with random character, and save it into file
     * also pre-create keys with different type
     */
    data_table = (char *)plat_malloc(TOTAL_TABLE_SIZE + 2097152);
    plat_assert(data_table);

    if (first_run) {
        data_table_init(TOTAL_TABLE_SIZE + 2097152);
        SDF_status_t status = rep_create_objs(test_framework, (max_objects_num * hit_rate) / 100,
                                              max_object_size, max_key_len, &fail_key);
        if (status != SDF_SUCCESS) {
            printf("$$create a key-obj <%s> failed\n", fail_key);
            if (data_table) plat_free(data_table);
            plat_assert(0 == 1);
        } else {

        }
    } else {
        recover_rt_data_table(df_name, TOTAL_TABLE_SIZE + 2097152);
        read_keyset_from_file(kf_name, max_objects_num);
    }

    fth_arg_t *fth_args = (fth_arg_t *)plat_alloc(rep_stress_workers * sizeof(fth_arg_t));
    plat_assert(fth_args);

    for (int i = 0; i < rep_stress_workers; i++) {
        fth_args[i].id = i;
        fth_args[i].objs = 0;
        fth_args[i].max_objs = max_objects_num;
        fth_args[i].shard = 1;
        fth_args[i].node = 1;
        fth_args[i].ctxt = test_framework;
        fthResume(fthSpawn(&rt_operations_entry, REP_FTH_STACKSIZE), (uint64_t)(&fth_args[i]));
    }

    fthWait();

    /**
     * checkpoint: save data-table and key-set, note data-table just need save once
     */
    if (first_run) {
        save_rt_data_table(df_name, TOTAL_TABLE_SIZE + 2097152);
    }
    /**
     * save the last key-set info
     */
    if (!first_run) {
        system("cp kf_name kf_name.bak");
    }
    write_keyset_into_file(kf_name, max_objects_num);

    int count = get_fail_count(max_objects_num);
    printf("[Warning] fail reason for object not exist, count is %d\n", count);
    rep_keyset_free(max_objects_num);

    /**
     * shutdown replication test framework, end test case
     */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "shutdown");
    rtfw_shutdown_sync(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "shutdown complete");

    /**
     * plat_free the memory of cards and fth_args
     */
    if (fth_args) plat_free(fth_args);
    if (data_table) plat_free(data_table);
    if (meta) plat_free(meta);
    if (replication_props) plat_free(replication_props);
    if (shard_meta) plat_free(shard_meta);

    // Terminate scheduler if idle_thread exit
    while (test_framework->timer_dispatcher) {
        fthYield(-1);
    }
    plat_free(test_framework);

    fthKill(1);
    printf("finished testing replication stress test\n");
    printf("\nALL\t %d FAILED\t %d FAILED/ALL\t %f\n",
           err_count + suc_count, err_count, 1.0 * err_count / (err_count + suc_count));
    printf("DONE\n");
    fflush(stdout);

}

int main(int argc, char **argv) {
    SDF_status_t status;
    struct replication_test_framework *test_framework = NULL;

    struct plat_opts_config_sync_test opts_config;
    memset(&opts_config, 0, sizeof(opts_config));

    int rep_random_seed;

#ifdef REPLICATION_TEST_RUN_T

    /**
     * get these variables's value from environment
     */

    rep_node_num     = atoi(getenv("REP_NODE_NUM"));
    rep_replica_num  = atoi(getenv("REP_REPLICA_NUM"));
    rep_random_seed  = atoi(getenv("REP_RANDOM_SEED"));
    if (rep_random_seed == 0) {
        rep_random_seed = (int)time(0);
    }
    plat_assert(rep_node_num <= MAX_NODE_NUM);
    plat_assert(rep_replica_num <= MAX_REPLICA_NUM);
#else
    rep_node_num    = 4;
    rep_replica_num = 3;
    rep_random_seed = (int)time(0);
#endif

    /**
     * set a seed to random method according to current time or configure file
     */
    printf("REP_RANDOM_SEED = %d\n", rep_random_seed);
    srandom(rep_random_seed);

    rt_common_test_config_init(&opts_config.common_config);

    /* Three plus the super node */
    opts_config.common_config.test_config.nnode = rep_node_num;
    opts_config.common_config.test_config.num_replicas = rep_replica_num;
    opts_config.common_config.test_config.replication_type = SDF_REPLICATION_META_SUPER_NODE;
    opts_config.common_config.test_config.replicator_config.lease_usecs = 100 * MILLION;

    SDF_boolean_t opts_status = plat_opts_parse_sync_test(&opts_config, argc, argv);
    if (opts_status) {
        return (1);
    }

    status = rt_sm_init(&opts_config.common_config.shmem_config);
    if (status) {
        return (1);
    }

    fthInit();

    test_framework = replication_test_framework_alloc(&opts_config.common_config.test_config);
    if (test_framework) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework %p allocated\n", test_framework);
    }
    XResume(fthSpawn(&rep_worker_thread_create, 40960), (uint64_t)test_framework);
    fthSchedulerPthread(0);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "JOIN");

    rt_sm_detach();
    rt_common_test_config_destroy(&opts_config.common_config);

    return (0);
}

#include "platform/opts_c.h"
