/*
 * File:   fcnl_framework_async_get_put_stress.c
 * Author: Zhenwei Lu
 *
 * Note:stress test case for replication test_framework
 * Created on Nov 17, 2008, 12:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id$
 */

#include "platform/stdio.h"
#include "protocol/replication/copy_replicator_internal.h"

#include "test_framework.h"
#include "test_common.h"

#define KEY_MAX 5

/*
 * We use a sub-category under test because test implies a huge number
 * of log messages out of simulated networking, flash, etc.
 */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/case");

struct rtfw_async_state {
    struct global_state *g_state;
    char *key;
    size_t key_len;
    char *data;
    size_t data_len;
    SDF_shardid_t shard_id;
    vnode_t node;
};

struct global_state {
    struct replication_test_framework *fm;
    SDF_shardid_t shard_id;

    int ops_complete;
    int total_ops;
    int ops_read_start;
    int ops_read_suc;
    int ops_read_complete;
    int ops_write_start;
    int ops_write_suc;
    int ops_write_complete;
    int ops_del_start;
    int ops_del_suc;
    int ops_del_complete;

};

struct global_state g_state_t;
struct global_state *g_state;

#define PLAT_OPTS_NAME(name) name ## _async_test
#include "platform/opts.h"
#include "misc/misc.h"

#define PLAT_OPTS_ITEMS_async_test()                                                  \
    item("iterations", "how many operations", ITERATIONS,                                  \
         parse_int(&config->iterations, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)

struct plat_opts_config_async_test {
    int iterations;
};
static void run_read_async();
static void run_write_async();
static void run_del_async();

static void
gen_and_run_random_op() {
    int rand = plat_prng_next_int(g_state->fm->api->prng, 3);
    if (0 == rand) {
        run_read_async();
    } else if (1 == rand) {
        run_write_async();
    } else {
        run_del_async();
    }
}

static void
run_test_case_complete() {
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "\n**************************************************\n"
                 "                  test case complete                 "
                 "\n**************************************************");
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Summary of stress get_put async test case\n"
                 "total_ops:%d, ops_complete:%d,\n"
                 "read_start:%d, read_complete:%d, read_suc:%d,\n"
                 "write_start:%d, write_complete:%d, write_suc:%d,\n"
                 "del_start:%d, del_complete:%d, del_suc:%d\n",
                 g_state->total_ops, g_state->ops_complete,
                 g_state->ops_read_start, g_state->ops_read_complete, g_state->ops_read_suc,
                 g_state->ops_write_start, g_state->ops_write_complete, g_state->ops_write_suc,
                 g_state->ops_del_start, g_state->ops_del_complete, g_state->ops_del_suc);

    rtfw_shutdown_sync(g_state->fm);
    while (g_state->fm->timer_dispatcher) {
        fthYield(-1);
    }
    plat_free(g_state->fm);
    fthKill(1);
}

static void
next_op() {
   if (g_state->ops_complete == g_state->total_ops) {
        run_test_case_complete();
   } else {
        gen_and_run_random_op();
   }
}

/**
 * @brief Callback function for delete complete
 */
static void
rtfw_del_complete_cb(struct plat_closure_scheduler *context, void *env,
                     SDF_status_t status) {
    struct rtfw_async_state *state = (struct rtfw_async_state *)env;
    if (status == SDF_SUCCESS) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "read complete node:%d, key:%s, key_len:%d",
                     (int)state->node, (char *)state->key, (int)state->key_len);
        (void) __sync_add_and_fetch(&g_state->ops_del_suc, 1);
    }
    (void) __sync_add_and_fetch(&g_state->ops_complete, 1);
    (void) __sync_add_and_fetch(&g_state->ops_del_complete, 1);
    next_op();
}

/**
 * @brief Callback function for read complete
 */
static void
rtfw_read_complete_cb(plat_closure_scheduler_t *context, void *env,
                      SDF_status_t status, const void *data, size_t data_len,
                      replication_test_framework_read_data_free_cb_t free_cb) {
    struct rtfw_async_state *state = (struct rtfw_async_state *)env;
    if (status == SDF_SUCCESS) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "read complete node:%d, key:%s, key_len:%d, data:%s, data_len:%d",
                     (int)state->node, (char *)state->key, (int)state->key_len,
                     (char *)data, (int)data_len);
        (void) __sync_add_and_fetch(&g_state->ops_read_suc, 1);
    }
    (void) __sync_add_and_fetch(&g_state->ops_complete, 1);
    (void) __sync_add_and_fetch(&g_state->ops_read_complete, 1);
   
    plat_closure_apply(replication_test_framework_read_data_free_cb,
                       &free_cb, data, data_len);
    next_op();
}

/**
 * @brief Callback function for write completed
 */
static void
rtfw_write_complete_cb(struct plat_closure_scheduler *context, void *env,
                       SDF_status_t status) {
    struct rtfw_async_state *state = (struct rtfw_async_state *)env;
    if (status == SDF_SUCCESS) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "write completed node:%d, key:%s, key_len:%d, data:%s, data_len:%d",
                     (int)state->node, (char *)state->key, (int)state->key_len,
                     (char *)state->data, (int)state->data_len);
        plat_free(state->key);
        plat_free(state->data);
        plat_free(state);
        (void) __sync_add_and_fetch(&g_state->ops_write_suc, 1);
    }
    (void) __sync_add_and_fetch(&g_state->ops_complete, 1);
    (void) __sync_add_and_fetch(&g_state->ops_write_complete, 1);
    next_op();
}

static vnode_t
get_random_node() {
    return ((vnode_t)plat_prng_next_int(g_state->fm->api->prng, 2));
}

static int
get_random_key_no() {
    return (plat_prng_next_int(g_state->fm->api->prng, KEY_MAX));
}

static char *
get_malloc_key(int key_no) {
    char *ret;
    plat_asprintf(&ret, "google:%d", key_no);
    return (ret);
}

static char *
get_malloc_data(int key_no) {
    char *ret;
    plat_asprintf(&ret, "google:%d", key_no);
    return (ret);
}

static void
run_del_async() {
    replication_test_framework_cb_t delete_cb;
    int key_no;
    struct rtfw_async_state *state = NULL;

    if (plat_calloc_struct(&state)) {
        state->node = get_random_node();
        key_no = get_random_key_no();
        state->key = get_malloc_key(key_no);
        state->key_len = strlen(state->key);
        
        delete_cb =
            replication_test_framework_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                 &rtfw_del_complete_cb,
                                                 state);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "\n**************************************************\n"
                     "                  delete async                 "
                     "\n**************************************************");

        rtfw_delete_async(g_state->fm, g_state->shard_id /* shard */,
                          state->node /* node */, state->key, state->key_len+1, delete_cb);
        (void) __sync_add_and_fetch(&g_state->ops_del_start, 1);
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_WARN,
                     "malloc error, not enough memory");
        plat_abort();
    }
}

static void
run_read_async() {
    replication_test_framework_read_async_cb_t async_cb;
    int key_no;
    struct rtfw_async_state *state = NULL;

    if (plat_calloc_struct(&state)) {
        state->node = get_random_node();
        key_no = get_random_key_no();
        state->key = get_malloc_key(key_no);
        state->key_len = strlen(state->key);

        async_cb =
            replication_test_framework_read_async_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                            rtfw_read_complete_cb,
                                                            state);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "\n**************************************************\n"
                     "                  read async                 "
                     "\n**************************************************");

        rtfw_read_async(g_state->fm, g_state->shard_id /* shard */,
                        state->node /* node */, state->key,
                        state->key_len+1, async_cb);
        (void) __sync_add_and_fetch(&g_state->ops_read_start, 1);
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_WARN,
                     "malloc error, not enough memory");
        plat_abort();
    }
}

static void
run_write_async() {
    char *key;
    char *data;
    int key_no;
    uint32_t key_len;
    replication_test_framework_cb_t cb;

    struct rtfw_async_state *state = NULL;
    if (plat_calloc_struct(&state)) {
        vnode_t node = get_random_node();
        key_no = get_random_key_no();
        key = get_malloc_key(key_no);
        data = get_malloc_data(key_no);
        key_len = strlen(key);
        state->node = node;
        state->key = key;
        state->key_len = key_len;
        state->data = data;

        cb = replication_test_framework_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                  &rtfw_write_complete_cb,
                                                  state);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "\n**************************************************\n"
                     "                  write async                 "
                     "\n**************************************************");

        rtfw_write_async(g_state->fm, g_state->shard_id /* shard */,
                         node /* node */, meta /* test_meta */,
                         state->key, strlen(state->key)+1, data, strlen(data)+1, cb);
        (void) __sync_add_and_fetch(&g_state->ops_write_start, 1);
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_WARN,
                     "memory not enough error");
        plat_abort();
    }
}



/**
 * @brief synchronized create_shard/write/read/delete/delete_shard operations
 */
void
user_operations_async_stress(uint64_t args) {
    struct replication_test_framework *test_framework =
            (struct replication_test_framework *)args;
    struct SDF_shard_meta *shard_meta = NULL;
    struct sdf_replication_shard_meta r_shard_meta;
    SDF_replication_props_t *replication_props = NULL;
    int i, failed = 0;
    struct cr_shard_meta *in;
    SDF_shardid_t shard_id;
    vnode_t node = 0;
    SDF_status_t status;

    /* Assure test_framework is started?! */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start test_framework");
    rtfw_start(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework started\n");

    shard_id = g_state->shard_id;

    failed = !plat_calloc_struct(&meta);
    replication_test_meta_init(meta);

    if (!failed) {
        failed = !plat_calloc_struct(&replication_props);
        if (!failed) {
            rtfw_set_default_replication_props(&test_framework->config, replication_props);
            shard_meta = rtfw_init_shard_meta(&test_framework->config,
                                              node /* first_node */,
                                              /* shard_id, in real system generated by generate_shard_ids() */
                                              shard_id,
                                              replication_props);
            plat_assert(shard_meta);

            /*
             * XXX: drew 2009-05-10 Move this to a helper function or get away
             * from the separate SDF_shard_meta type.
             */
            memset(&r_shard_meta, 0, sizeof(r_shard_meta));
            r_shard_meta.type = shard_meta->replication_props.type;
            r_shard_meta.nreplica = shard_meta->replication_props.num_replicas;
            r_shard_meta.current_home_node = node;

            /* XXX: This all will end up on node 0 */
            for (i = 0; i < r_shard_meta.nreplica; ++i) {
                r_shard_meta.pnodes[i] =
                    (shard_meta->first_node + i) % test_framework->config.nnode;
            }

            r_shard_meta.meta_pnode  = shard_meta->first_meta_node;
            
            /*
             * create a shard on test flash to ensure meta data
             * persitence will sucess(currently it fails for no shard).
             * However, that will change replicator test mode
             */
            status = cr_shard_meta_create(&in,
                                          &test_framework->config.replicator_config,
                                          shard_meta);
            plat_assert(status == SDF_SUCCESS);
            cr_shard_meta_free(in);

            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "\n**************************************************\n"
                         "                  create shard sync                 "
                         "\n**************************************************");
            rtfw_create_shard_sync(test_framework, node, shard_meta);
            next_op();
        }
    }
    plat_free(meta);
    plat_free(replication_props);
    if (shard_meta) {
        plat_free(shard_meta);
    }
}

static void
init_global_state(struct replication_test_framework *fm,
                  int max_ops) {
    g_state = &g_state_t;
    g_state->fm = fm;
    g_state->shard_id = 0;

    g_state->total_ops = max_ops;
    g_state->ops_complete = 0;
    g_state->ops_read_complete = 0;
    g_state->ops_write_complete = 0;
    g_state->ops_read_suc = 0;
    g_state->ops_read_start = 0;
    g_state->ops_write_start = 0;
    g_state->ops_write_suc = 0;
    g_state->ops_del_complete = 0;
    g_state->ops_del_complete = 0;
    g_state->ops_del_suc = 0;

}

int
main(int argc, char **argv) {
    SDF_status_t status;
    struct replication_test_framework *test_framework = NULL;
    struct replication_test_config *config = NULL;
    int failed;

    struct plat_opts_config_async_test opts_config;
    memset(&opts_config, 0, sizeof (opts_config));
    int opts_status = plat_opts_parse_async_test(&opts_config, argc, argv);
    if (opts_status) {
        plat_opts_usage_async_test();
        return (1);
    }

    if (!opts_config.iterations) {
        opts_config.iterations = 10;
    }

    failed = !plat_calloc_struct(&config);

    struct plat_opts_config_replication_test_framework_sm *sm_config;
    plat_calloc_struct(&sm_config);

    /* start shared memory */
    status = framework_sm_init(0, NULL, sm_config);

    /* start fthread library */
    fthInit();

    rt_config_init(config, opts_config.iterations);
    test_framework = replication_test_framework_alloc(config);
    plat_assert(test_framework);
    if (test_framework) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework %p allocated\n",
                     test_framework);
        init_global_state(test_framework, opts_config.iterations);
    }

    XResume(fthSpawn(&user_operations_async_stress, 40960), (uint64_t)test_framework);
    fthSchedulerPthread(0);
    plat_free(config);
    framework_sm_destroy(sm_config);
    return (0);
}
#include "platform/opts_c.h"
