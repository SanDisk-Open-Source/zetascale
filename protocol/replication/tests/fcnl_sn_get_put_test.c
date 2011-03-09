/*
 * File: fcnl_sn_get_put_test.c
 * Author: Zhenwei Lu
 *
 * Basic get/put/delete from home/replica node
 *
 * Scenario:
 * 4 nodes, 0, 1, 2, 3, 3 replicas
 * single shard 1
 * create shards, write some objects and verify
 * delete some objects from node 2, 3
 *
 * Created on Jun 22, 2009, 12:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcnl_sn_get_put_test.c 10756 2009-07-14 04:44:23Z lzwei $
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
         parse_int(&config->case_no, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)

#define SLEEP_US 1000000

struct plat_opts_config_sync_test {
    struct rt_common_test_config common_config;
    int case_no;
};

#define NUM_REPLICAS 3
#define NUM_NODES 4
#define NUM_OBJS 10

int case_no = 0;
char keys[NUM_OBJS][5] = {"key0", "key1", "key2", "key3", "key4", "key5",
    "key6", "key7", "key8", "key9"};
char *key;
int to_del = 5;
int to_get = 4;
int always_exist = 3;
int to_not_exist = 15;
SDF_shardid_t shard_id = 100;
size_t key_len;
char *data_in[NUM_OBJS];
void *data_out;
size_t data_len_out;
int data_generation = 0;
vnode_t cur_home_node = 1;
vnode_t replica_node = 2;
replication_test_framework_read_data_free_cb_t free_cb;

static SDF_status_t
case_get_existed_obj(struct replication_test_framework *test_framework,
                     SDF_shardid_t shard_id, vnode_t node,
                     char *key, size_t key_len, void *data_out,
                     size_t *data_len_out, char *data_verify,
                     replication_test_framework_read_data_free_cb_t *free_cb) {
    SDF_status_t op_ret;

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "read on node %d", node);
    op_ret = rtfw_read_sync(test_framework, shard_id, node /* node */, key,
                            key_len, &data_out, data_len_out, free_cb);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "read on node %d complete", node);
    plat_assert(op_ret == SDF_SUCCESS);
    
    plat_assert(strcmp(data_out, data_verify) == 0);
    plat_closure_apply(replication_test_framework_read_data_free_cb, free_cb,
                       data_out, *data_len_out);
    
    return (op_ret);
}

static void
case_get_un_existed_obj(struct replication_test_framework *test_framework,
                        SDF_shardid_t shard_id, vnode_t node, char *key,
                        size_t key_len, void *data_out, size_t *data_len_out,
                        replication_test_framework_read_data_free_cb_t *free_cb) {
    SDF_status_t op_ret;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "read on node %d", node);
    op_ret = rtfw_read_sync(test_framework, shard_id, node /* node */, key,
                            key_len, &data_out, data_len_out, free_cb);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "read on node %d complete", node);
    plat_assert(op_ret != SDF_SUCCESS);
}

static SDF_status_t
case_put_existsed_obj(struct replication_test_framework *test_framework,
                      SDF_shardid_t shard_id, vnode_t node, char *key,
                      size_t key_len, void *data, size_t data_len) {
    SDF_status_t op_ret;

    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                 "write on node %d key:%s, key_len:%d, data:%s, data_len:%d",
                 node, key, (int)key_len, data_in[to_del], (int)data_len);

    op_ret = rtfw_write_sync(test_framework, shard_id, node /* node */,
                             meta /* test_meta */, key, key_len, data, data_len);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "write on node %d complete", node);
    /**
     * FIXME: why write the existed key returns SDF_SUCCESS
     */
    plat_assert(op_ret == SDF_SUCCESS);

    return (op_ret);
}

static SDF_status_t
case_del_exsited_obj(struct replication_test_framework *test_framework,
                     SDF_shardid_t shard_id, vnode_t node,
                     char *key, size_t key_len) {
    SDF_status_t op_ret;

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "del %s from shard:%d node:%d", key,
                 (int)shard_id, node);
    op_ret = rtfw_delete_sync(test_framework, shard_id, node, key, key_len);
    plat_assert(op_ret == SDF_SUCCESS);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "del %s from shard:%d node:%d complete",
                 key, (int)shard_id, node);

    return (op_ret);
}

static void
case_data_prepare(struct replication_test_framework *test_framework) {
    int i;
    SDF_status_t op_ret;

    for (i = 0; i < NUM_OBJS; i++) {
        plat_asprintf(&data_in[i], "data_%s_%d_%s_%d", "node",
                      i, keys[i], data_generation);
        key = keys[i];
        key_len = strlen(key) + 1;

        plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                     "write on node %d key:%s, key_len:%u, data:%s, data_len:%u",
                     cur_home_node, key, (int)(strlen(key)), data_in[i],
                     (int)(strlen(data_in[i])));
        op_ret = rtfw_write_sync(test_framework, shard_id /* shard */,
                                 cur_home_node /* node */,
                                 meta /* test_meta */, key, key_len, data_in[i],
                                 strlen(data_in[i])+1);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "write on node %d complete",
                     cur_home_node);
        plat_assert(op_ret == SDF_SUCCESS);
    }
}

/**
 * @brief common used operation set to verify replica node funcationality
 */
static void
case_verify_replica_node_opset(struct replication_test_framework *test_framework) {

    key = keys[to_get];
    key_len = strlen(key)+1;
    case_get_existed_obj(test_framework, shard_id, replica_node,
                         key, key_len, data_out, &data_len_out,
                         data_in[to_get], &free_cb);
}

/**
 * @brief common used operation set to verify home node funcationality
 */
static void
case_verify_home_node_opset(struct replication_test_framework *test_framework) {
    char *key;
    key = keys[to_get];
    key_len = strlen(key)+1;
    case_get_existed_obj(test_framework, shard_id, cur_home_node, key, key_len,
                         data_out, &data_len_out, data_in[to_get], &free_cb);

    /* get unexisted object from home_node */
    key = keys[to_not_exist];
    key_len = strlen(key)+1;
    case_get_un_existed_obj(test_framework, shard_id, cur_home_node, key,
                            key_len, data_out,
                            &data_len_out, &free_cb);

    /* put the existed object to node1 */
    key = keys[to_del];
    key_len = strlen(key) + 1;
    case_put_existsed_obj(test_framework, shard_id, cur_home_node, key, key_len, data_in[to_del],
                          strlen(data_in[to_del])+1);


    /* del an object from node1 */
    key = keys[to_del];
    key_len = strlen(key) + 1;
    case_del_exsited_obj(test_framework, shard_id, cur_home_node, key, key_len);
}

static void
case_basic_get_put_test(struct replication_test_framework *test_framework) {
    case_data_prepare(test_framework);
    case_verify_home_node_opset(test_framework);
    case_verify_replica_node_opset(test_framework);
}

void
user_operations_mc_test(uint64_t args) {
    struct replication_test_framework *test_framework =
        (struct replication_test_framework *)args;
    struct SDF_shard_meta *shard_meta;
    /* configuration infomation about shard */
    SDF_replication_props_t *replication_props = NULL;
    SDF_status_t op_ret = SDF_SUCCESS;
    vnode_t node[NUM_REPLICAS] = {1, 2, 3};
    int failed;


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
    cur_home_node = node[0];

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "create on node %d", cur_home_node);
    op_ret = rtfw_create_shard_sync(test_framework, cur_home_node, shard_meta);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "create on node %d complete", cur_home_node);
    plat_assert(op_ret == SDF_SUCCESS);


    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "home node %d", cur_home_node);
    switch (case_no) {
    case 0:
        /* node 2 is a replica node */
        case_basic_get_put_test(test_framework);
    default:
        break;
    }

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

    if (opts_config.case_no) {case_no = opts_config.case_no; }

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
