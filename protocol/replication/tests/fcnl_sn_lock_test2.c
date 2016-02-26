/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: fcnl_sn_lock_test2.c
 * Author: Zhenwei Lu
 *
 * For shard key lock issues
 *
 * Scenario:
 * 4 nodes, 0, 1, 3, 3 replicas
 * single shard 1, default home node 1
 * create shards, write some objects and verify
 * cut off network of a replica node 2, 3
 * modify some objects on replica node 2 and 3,
 * write some new objects, del some on them, too.
 * turn on network of node 2, 3
 * crash home node to do home node switch over
 * confirm objects
 *
 * Created on July 6, 2009, 12:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id$
 */

#include "fth/fthOpt.h"
#include "platform/stdio.h"
#include "test_framework.h"

#define RT_USE_COMMON 1
#include "test_common.h"

/*
 * We use a sub-category under test because test implies a huge number
 * of log messages out of simulated networking, flash, etc.
 */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/case");


#define PLAT_OPTS_NAME(name) name ## _sync_test
#include "platform/opts.h"
#include "misc/misc.h"

#define PLAT_OPTS_ITEMS_sync_test() \
    PLAT_OPTS_COMMON_TEST(common_config) \
    item("case_no", "test case no", CASE_NO,                                    \
         parse_int(&config->case_no, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)     \
    item("with_data", "with get/put/delete ops or not", WITH_DATA,              \
         parse_int(&config->with_data, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)

#define SLEEP_US 1000000

struct plat_opts_config_sync_test {
    struct rt_common_test_config common_config;
    int case_no;
    int with_data;
};

#define NUM_REPLICAS 3
#define NUM_NODES 4
#define NUM_OBJS 10
#define NUM_OBJS_EX 5

int case_no = 0;
int with_data = 0;
char keys[NUM_OBJS][5] = {"key0", "key1", "key2", "key3", "key4", "key5",
    "key6", "key7", "key8", "key9"};
char keys_ex[NUM_OBJS_EX][6] = {"key10", "key11", "key12", "key13", "key14"};
char keys_home[NUM_OBJS][6] = {"home0", "home1", "home2", "home3", "home4"};
char *key;
int to_del = 5;
int to_get = 4;
int always_exist = 3;
int to_not_exist = 15;
int to_del_replica = 7;
SDF_shardid_t shard_id = 100;
size_t key_len;
char *data_in[NUM_OBJS];
char *data_in_ex[NUM_OBJS_EX];
void *data_out;
size_t data_len_out;
int data_generation = 0;
replication_test_framework_read_data_free_cb_t free_cb;

static vnode_t
case_get_cur_home_node_per_shard(struct replication_test_framework *test_framework,
                                 SDF_shardid_t shard, const void *key,
                                 size_t key_len)  __attribute__((unused));
static void
case_write_to_replica_node(struct replication_test_framework *test_framework);

/**
 * @brief try to get home_node of specified shard_id,
 * of course a existed key should provided
 * @return -1 if 1 not existed
 */
static vnode_t
case_get_cur_home_node_per_shard(struct replication_test_framework *test_framework,
                                 SDF_shardid_t shard, const void *key,
                                 size_t key_len) {
    SDF_status_t op_ret;
    void *data_out;
    size_t data_len_out;
    replication_test_framework_read_data_free_cb_t free_cb;
    vnode_t ret = -1;
    int i;

    plat_assert(key);
    plat_assert(test_framework);

    if (!with_data) {
        return (1);
    }

    /* fault injected here since we don't know replica located */
    for (i = 0; i < NUM_NODES; i++) {
        op_ret = rtfw_read_sync(test_framework, shard, i, key, key_len,
                                &data_out, &data_len_out, &free_cb);
        if (op_ret == SDF_SUCCESS) {
            ret = i;
            break;
        }
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "read from node %d key:%s return status %s",
                     i, (char *)key, sdf_status_to_string(op_ret));
    }
    return (ret);
}

/**
 * @brief write to replica node with its network down,
 * crash replica node and restart it
 */
static void
case_write_to_replica_node(struct replication_test_framework *test_framework) {
    SDF_status_t op_ret;
    int i, j;
    vnode_t replica_nodes[2] = {2, 3};

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "home node %d", 1);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "turn off network of node %d", 2);
    rtfw_shutdown_node_network_sync(test_framework, 2);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "turn off network of node 3");
    rtfw_shutdown_node_network_sync(test_framework, 3);

    /* delete some objects from home node */
    for (i = 0; i < NUM_OBJS; i++) {
        if (i % 2 == 1) {
            continue;
        }
        key = keys[i];
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "del %s from shard:%d node:%d",
                     key, (int)shard_id, 1);
        key_len = strlen(key) + 1;
        op_ret = rtfw_delete_sync(test_framework, shard_id, 1, key, key_len);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "del %s from shard:%d node:%d complete, status:%s",
                     key, (int)shard_id, 2, sdf_status_to_string(op_ret));
    }

    /* write some objects to home node */
    for (i = 0; i < NUM_OBJS_EX; i++) {
        char *temp_data;
        plat_asprintf(&temp_data, "data_home_%s_%d_%s_%d", "node",
                      i, keys_home[i], data_generation);

        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "write to home node:%d", 1);
        op_ret = rtfw_write_sync(test_framework, shard_id /* shard */,
                                 1 /* node */, meta /* test_meta */,
                                 keys_home[i], strlen(keys_home[i])+1,
                                 temp_data, strlen(temp_data)+1);
        plat_free(temp_data);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "write on node %d complete return status: %s",
                     1, sdf_status_to_string(op_ret));
    }

    for (j = 0; j < 2; j++) {
        /* delete an object from replica node */
        key = keys[to_del_replica];
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "del %s from shard:%d node:%d",
                     key, (int)shard_id, replica_nodes[j]);
        key_len = strlen(key) + 1;
        op_ret = rtfw_delete_sync(test_framework, shard_id, 2, key, key_len);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "del %s from shard:%d node:%d complete, status:%s",
                     key, (int)shard_id, replica_nodes[j], sdf_status_to_string(op_ret));


        /* read from replica node to get a object unknown */
        op_ret = rtfw_read_sync(test_framework, shard_id, replica_nodes[j] /* node */, key,
                                key_len, &data_out, &data_len_out, &free_cb);

        /* FIXME: should be SDF_WRONG_NODE now */
        plat_assert(op_ret != SDF_SUCCESS);


        /* write some extra objects to replica node */
        for (i = 0; i < NUM_OBJS_EX; i++) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "write to replica node %d", replica_nodes[j]);
            op_ret = rtfw_write_sync(test_framework, shard_id /* shard */,
                                     replica_nodes[j] /* node */, meta /* test_meta */,
                                     keys_ex[i], strlen(keys_ex[i])+1,
                                     data_in_ex[i], strlen(data_in_ex[i])+1);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "write on node %d complete return status: %s",
                         replica_nodes[j], sdf_status_to_string(op_ret));
        }


        /* modify some objects on replica node */
        for (i = 0; i < NUM_OBJS; i++) {
            char *temp_data;
            if (i % 2 == 1) {
                continue;
            }
            plat_asprintf(&temp_data, "data_modify_%s_%d_%s_%d", "node",
                          i, keys[i], data_generation);

            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "modify to replica node %d", replica_nodes[j]);
            op_ret = rtfw_write_sync(test_framework, shard_id /* shard */,
                                     replica_nodes[j] /* node */, meta /* test_meta */,
                                     keys[i], strlen(keys[i])+1,
                                     temp_data, strlen(temp_data)+1);
            plat_free(temp_data);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "write on node %d complete return status: %s",
                         replica_nodes[j], sdf_status_to_string(op_ret));

        }

        /* modify some objects to home node */
        for (i = 0; i < NUM_OBJS_EX; i++) {
            char *temp_data;
            plat_asprintf(&temp_data, "data_home_%s_%d_%s_%d", "node",
                          i, keys[i], data_generation);

            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "write to home node:%d", 1);
            op_ret = rtfw_write_sync(test_framework, shard_id /* shard */,
                                     1 /* node */, meta /* test_meta */,
                                     keys[i], strlen(keys[i])+1,
                                     temp_data, strlen(temp_data)+1);
            plat_free(temp_data);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "write on node %d complete return status: %s",
                         1, sdf_status_to_string(op_ret));
        }

        /* turn on network of replica */
        rtfw_start_node_network_sync(test_framework, replica_nodes[j]);
    }

    for (i = 0; i < NUM_OBJS_EX; i++) {
        char *temp_data;
        plat_asprintf(&temp_data, "data_home_%s_%d_%s_%d_replica_on", "node",
                      i, keys_home[i], data_generation);

        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "write to home node:%d", 1);
        op_ret = rtfw_write_sync(test_framework, shard_id /* shard */,
                                 1 /* node */, meta /* test_meta */,
                                 keys_home[i], strlen(keys_home[i])+1,
                                 temp_data, strlen(temp_data)+1);
        plat_free(temp_data);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "write on node %d complete return status: %s",
                     1, sdf_status_to_string(op_ret));
        plat_assert(SDF_SUCCESS == op_ret);
    }

    /* read from home node to assure object is not deleted from home node */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "read from home node %d", 1);
    op_ret = rtfw_read_sync(test_framework, shard_id, 1 /* node */, key,
                            key_len, &data_out, &data_len_out, &free_cb);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "read on node %d complete", 1);

    /* read from home node to assure keys_home stored */
    for (i = 0; i < NUM_OBJS_EX; i++) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "read from home node %d", 1);
        op_ret = rtfw_read_sync(test_framework, shard_id,
                                1 /* node */, keys_home[i],
                                strlen(keys_home[i])+1, &data_out,
                                &data_len_out, &free_cb);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "read on node %d complete return %s, status: %s",
                     1, (char *)data_out,
                     sdf_status_to_string(op_ret));
        plat_assert(op_ret == SDF_SUCCESS);
    }

    /* crash node 1 make node 2 or node 3 to be home node */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "crash home node %d", 1);
    op_ret = rtfw_crash_node_sync(test_framework, 1);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "crash home node %d complete", 1);
    plat_assert(op_ret == SDF_SUCCESS);
    /* Wait for recovery.  Delay is arbitrary and long */
    rtfw_sleep_usec(test_framework,
                    test_framework->config.replicator_config.lease_usecs * 2);

    /* get cur home node */

    /* restart home node 1 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start node %d", 1);
    op_ret = rtfw_start_node(test_framework, 1);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start node %d complete, status:%s",
                 1, sdf_status_to_string(op_ret));
    plat_assert(op_ret == SDF_SUCCESS);
    /* Wait for recovery.  Delay is arbitrary and long */
    rtfw_sleep_usec(test_framework,
                    test_framework->config.replicator_config.lease_usecs * 2);

#if 0
    /**
     * read from replica node to assure replica node is recovered
     * via replication service
     */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "read from repica node %d", 2);
    op_ret = rtfw_read_sync(test_framework, shard_id,
                            2 /* node */, key,
                            key_len, &data_out, &data_len_out, &free_cb);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "read on node %d complete, ret status: %s, return data: %s",
                 2, sdf_status_to_string(op_ret), (char *)data_out);
//    plat_assert(op_ret == SDF_SUCCESS);

    /* read extra objects from replica node */
    for (i = 0; i < NUM_OBJS_EX; i++) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "read from repica node %d", 2);
        op_ret = rtfw_read_sync(test_framework, shard_id,
                                2 /* node */, keys_ex[i],
                                strlen(keys_ex[i]) + 1, &data_out,
                                &data_len_out, &free_cb);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "read on node %d complete, ret status: %s",
                     2, sdf_status_to_string(op_ret));
        plat_assert(op_ret == SDF_SUCCESS);
        plat_assert(strcmp(data_out, data_in_ex[i]) == 0);
    }

    /* restart home node to ensure some extra objects exist */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start node %d", 1);
    op_ret = rtfw_start_node(test_framework, 1);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start node %d complete, status:%s",
                 1, sdf_status_to_string(op_ret));
    plat_assert(op_ret == SDF_SUCCESS);
    /* Wait for recovery.  Delay is arbitrary and long */
    rtfw_sleep_usec(test_framework,
                    test_framework->config.replicator_config.lease_usecs * 2);

    /* crash replica node again to make node 1 become home node */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "crash replica node 2");
    op_ret = rtfw_crash_node_sync(test_framework, 2);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "crash replica node 2 complete");
    plat_assert(op_ret == SDF_SUCCESS);
    /* Wait for recovery.  Delay is arbitrary and long */
    rtfw_sleep_usec(test_framework,
                    test_framework->config.replicator_config.lease_usecs * 2);

    /* read extra objects from home node */
    for (i = 0; i < NUM_OBJS_EX; i++) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "read from home node %d", 1);
        op_ret = rtfw_read_sync(test_framework, shard_id,
                                1 /* node */, keys_ex[i],
                                strlen(keys_ex[i]) + 1, &data_out,
                                &data_len_out, &free_cb);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "read on node %d complete, ret status: %s",
                     1, sdf_status_to_string(op_ret));
        /**
         * FIXME: Should confirm with Drew, why only in-flight created objects
         * would be preserved and forwarded to all replicas via MUTUAL_REDO
         */
        plat_assert(op_ret == SDF_SUCCESS);
        plat_assert(strcmp(data_out, data_in_ex[i]) == 0);
    }
#endif
}


void
user_operations_mc_test(uint64_t args) {
    struct replication_test_framework *test_framework =
        (struct replication_test_framework *)args;
    struct SDF_shard_meta *shard_meta;
    /* configuration infomation about shard */
    SDF_replication_props_t *replication_props = NULL;
    SDF_status_t op_ret = SDF_SUCCESS;
    vnode_t node[NUM_REPLICAS] = {1, 2};
    int failed, i;


    failed = !plat_calloc_struct(&meta);
    plat_assert(!failed);
    replication_test_meta_init(meta);

    /* Assure test_framework is started?! */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start test_framework");
    rtfw_start(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework started\n");

    /* Start all nodes */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start nodes");
    rtfw_start_all_nodes(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "nodes started");


    /* configures test framework accommodate to RT_TYPE_META_STORAGE */
    failed = !(plat_calloc_struct(&replication_props));
    plat_assert(!failed);

    /* initialize replciation properties and create shards */
    rtfw_set_default_replication_props(&test_framework->config,
                                       replication_props);
    shard_meta = rtfw_init_shard_meta(&test_framework->config, node[0] /* first */,
                                      shard_id
                                      /* shard_id, in real system generated by generate_shard_ids() */,
                                      replication_props);
    plat_assert(shard_meta);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "create on node %d", 1);
    op_ret = rtfw_create_shard_sync(test_framework, 1, shard_meta);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "create on node %d complete", 1);
    plat_assert(op_ret == SDF_SUCCESS);
    
    /* Ugly fix, case 0 need --with_data not null */
    if (0 == case_no) {
        with_data = 1;
    }

    for (i = 0; i < NUM_OBJS_EX; i++) {
        plat_asprintf(&data_in_ex[i], "data_%s_%d_%s_%d", "node",
                      i, keys_ex[i], data_generation);
    }

    if (with_data) {
        /* write some objects to nodes separately and get from home node */
        for (i = 0; i < NUM_OBJS; i++) {
            plat_asprintf(&data_in[i], "data_%s_%d_%s_%d", "node", i, keys[i], data_generation);
            key = keys[i];
            key_len = strlen(key) + 1;
            plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                         "write on node %d key:%s, key_len:%u, data:%s, data_len:%u",
                         1, key, (int)(strlen(key)+1), data_in[i],
                         (int)(strlen(data_in[i])+1));
            op_ret = rtfw_write_sync(test_framework, shard_id /* shard */, 1 /* node */,
                                     meta /* test_meta */, key, key_len, data_in[i],
                                     strlen(data_in[i])+1);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "write on node %d complete", 1);
            plat_assert(op_ret == SDF_SUCCESS);

            /* get object from nodes */
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "read on node %d", 1);
            op_ret = rtfw_read_sync(test_framework, shard_id, 1 /* node */, key,
                                    key_len, &data_out, &data_len_out, &free_cb);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "read on node %d complete", 1);
            plat_assert(op_ret == SDF_SUCCESS);
            plat_assert(strcmp(data_out, data_in[i]) == 0);
            plat_closure_apply(replication_test_framework_read_data_free_cb, &free_cb,
                               data_out, data_len_out);
        }
    }

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "home node %d", 1);
    /* write to replica node and crash->start it */
    case_write_to_replica_node(test_framework);

    /* Shutdown test framework */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                 "\n************************************************************\n"
                 "                  Test framework shutdown                       "
                 "\n************************************************************");
    rtfw_shutdown_sync(test_framework);

    int j;
    for (j = 0; j < NUM_OBJS; j++) {
        plat_free(data_in[j]);
    }

    for (j = 0; j < NUM_OBJS_EX; j++) {
        plat_free(data_in_ex[j]);
    }

    plat_free(meta);
    plat_free(replication_props);
    plat_free(shard_meta);

    /* Terminate scheduler if idle_thread exit */
    while (test_framework->timer_dispatcher) {
        fthYield(-1);
    }
    plat_free(test_framework);

    /* Terminate scheduler */
    fthKill(1);
}

int main(int argc, char **argv) {
    SDF_status_t status;
    struct replication_test_framework *test_framework = NULL;

    struct plat_opts_config_sync_test opts_config;
    memset(&opts_config, 0, sizeof (opts_config));

    rt_common_test_config_init(&opts_config.common_config);
    opts_config.common_config.test_config.replicator_config.timeout_usecs =
        opts_config.common_config.test_config.replicator_config.lease_usecs * 2;
    opts_config.common_config.test_config.nnode = NUM_NODES;
    opts_config.common_config.test_config.num_replicas = NUM_REPLICAS;
    opts_config.common_config.test_config.replication_type =
        SDF_REPLICATION_META_SUPER_NODE;
    opts_config.common_config.test_config.replicator_config.lease_usecs =
        100 * MILLION;

    int opts_status = plat_opts_parse_sync_test(&opts_config, argc, argv);
    if (opts_status) {
        plat_opts_usage_sync_test();
        return (1);
    }

    if (opts_config.with_data) {with_data = opts_config.with_data; }
    if (opts_config.case_no) {case_no = opts_config.case_no; }

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "with_data:%d, case_no:%d,"
                 "with_data:%d, case_no:%d\n",
                 with_data, case_no, opts_config.with_data, opts_config.case_no);

    status = rt_sm_init(&opts_config.common_config.shmem_config);
    if (status) {
        return (1);
    }

    /* start fthread library */
    fthInit();

    test_framework =
        replication_test_framework_alloc(&opts_config.common_config.test_config);
    if (test_framework) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework %p allocated\n",
                     test_framework);
    }
    XResume(fthSpawn(&user_operations_mc_test, 40960), (uint64_t)test_framework);
    fthSchedulerPthread(0);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "JOIN");

    rt_sm_detach();

    rt_common_test_config_destroy(&opts_config.common_config);

    return (0);
}
#include "platform/opts_c.h"
