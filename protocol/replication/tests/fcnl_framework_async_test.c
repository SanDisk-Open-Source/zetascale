//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/*
 * File:   fcnl_framework_async_test.c
 * Author: Zhenwei Lu
 *
 * Note:test case for replication test_framework
 * Created on Nov 17, 2008, 12:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcnl_framework_async_test.c 9779 2009-05-31 10:40:53Z lzwei $
 */

#include "platform/stdio.h"

#include "test_framework.h"
#include "test_common.h"

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
    SDF_shardid_t shard_id;
    vnode_t node_id;
};

struct global_state {
    struct replication_test_framework *rtfw;
    /** @brief Completed read ops number */
    int ops_complete;
    int total_ops;
};

struct global_state g_state;

#define PLAT_OPTS_NAME(name) name ## _async_test
#include "platform/opts.h"
#include "misc/misc.h"

#define PLAT_OPTS_ITEMS_async_test()                                                  \
    item("iterations", "how many operations", ITERATIONS,                                  \
         parse_int(&config->iterations, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)

struct plat_opts_config_async_test {
    int iterations;
};

static void user_async_shutdown_cb(struct plat_closure_scheduler *context,
                                   void *env);

/**
 * @brief Callback function for delete shard from test framework
 */
static void
rtfw_async_test_delete_cb(struct plat_closure_scheduler *context, void *env,
                          SDF_status_t status) {
    struct rtfw_async_state *state = (struct rtfw_async_state *)env;
    (void) __sync_add_and_fetch(&state->g_state->ops_complete, 1);
    if (state->g_state->ops_complete == state->g_state->total_ops) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                     "\n************************************************************\n"
                     "                  Test framework shutdown                       "
                     "\n************************************************************");
        /* Sync not allowed in callback fuctions, which will block the closure_thread */
//        rtfw_shutdown_sync(state->g_state->rtfw);
        replication_test_framework_shutdown_async_cb_t cb;
        cb = replication_test_framework_shutdown_async_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                                 user_async_shutdown_cb,
                                                                 state->g_state->rtfw);
        rtfw_shutdown_async(state->g_state->rtfw, cb);
    }
    plat_free(state->key);
    plat_free(state);
}


/**
 * @brief Callback function for read complete, if all read complete, delete shard will
 * be triggered.
 */
static void
rtfw_async_test_read_cb(plat_closure_scheduler_t *context, void *env,
                        SDF_status_t status, const void *data, size_t data_len,
                        replication_test_framework_read_data_free_cb_t free_cb) {
    struct rtfw_async_state *state = (struct rtfw_async_state *)env;
    replication_test_framework_cb_t delete_cb;
    delete_cb = replication_test_framework_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                     // state->g_state->rtfw->closure_scheduler,
                                                     &rtfw_async_test_delete_cb,
                                                     state);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "data:%s, data_len:%d", (char *)data, (int)data_len);
    plat_free((void *)data);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "\n***************************************************\n"
                 "                  delete object async                 "
                 "\n***************************************************");
    rtfw_delete_async(state->g_state->rtfw, state->shard_id /* shard */,
                      state->node_id /* node */, state->key, strlen(state->key)+1, delete_cb);
}
/**
 * @brief Callback function when async write completed, then a read_async
 * will be triggered
 */
static void
rtfw_async_test_write_cb(struct plat_closure_scheduler *context, void *env,
                         SDF_status_t status) {
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "write completed");
#if 0
    plat_free(env);
#endif
    struct rtfw_async_state *state = (struct rtfw_async_state *)env;
    replication_test_framework_read_async_cb_t async_cb;

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "\n**************************************************\n"
                 "                  read object async                  "
                 "\n**************************************************");
    async_cb =
        replication_test_framework_read_async_cb_create(// state->g_state->rtfw->closure_scheduler,
                                                        PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                        rtfw_async_test_read_cb,
                                                        state);
//    plat_closure_apply(replication_test_framework_read_async_cb, &async_cb, );
    rtfw_read_async(state->g_state->rtfw, state->shard_id /* shard */,
                    state->node_id /* node */, state->key, state->key_len, async_cb);

}

/**
 * @brief synchronized create_shard/write/read/delete/delete_shard operations
 */
void
user_operations_async(uint64_t args) {
    int i;
    struct replication_test_framework *test_framework =
            (struct replication_test_framework *)args;
    replication_test_framework_cb_t write_cb;
    struct SDF_shard_meta *shard_meta = NULL;
    SDF_replication_props_t *replication_props = NULL;
    int failed = 0;
    SDF_shardid_t shard_id;
    vnode_t first_node = 1;
    vnode_t node_id = 0;
    struct rtfw_async_state *state = NULL;

#if 0
    sdf_replication_ltime_t ltime;
    void **data_read;
    size_t *data_read_len;
    int t_read, t_write, t_delete, t_c_shard, t_d_shard = 0;
    int read, write, delete, c_shard, d_shard = 0;
#endif

    /* Assure test_framework is started?! */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start test_framework");
    rtfw_start(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework started\n");

    shard_id = __sync_add_and_fetch(&test_framework->max_shard_id, 1);
    char *key;
    char *data;

    failed = !plat_calloc_struct(&meta);
    replication_test_meta_init(meta);

    if (!failed) {
        failed = !plat_calloc_struct(&replication_props);
        if (!failed) {
            rtfw_set_default_replication_props(&test_framework->config, replication_props);
            shard_meta = rtfw_init_shard_meta(&test_framework->config,
                                              first_node /* first_node */,
                                              /* shard_id, in real system generated by generate_shard_ids() */
                                              shard_id,
                                              replication_props);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "\n**************************************************\n"
                         "                  create shard sync                 "
                         "\n**************************************************");
            rtfw_create_shard_sync(test_framework, node_id, shard_meta);

            for (i = 0; i < g_state.total_ops; i ++) {
                plat_calloc_struct(&state);
                plat_assert(state);

                plat_asprintf(&key, "google:%d", i);
                plat_asprintf(&state->key, "google:%d", i);
                plat_asprintf(&data, "Sebstian:%d", i);

                state->key_len = strlen(key)+1;
                state->shard_id = shard_id;
                state->node_id = node_id;
                state->g_state = &g_state;

                write_cb = replication_test_framework_cb_create(// test_framework->closure_scheduler,
                                                                PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                                &rtfw_async_test_write_cb,
                                                                state);

                plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                             "\n**************************************************\n"
                             "                 write object async                  "
                             "\n**************************************************");
                rtfw_write_async(test_framework, shard_id /* shard */,
                                 node_id /* node */, meta /* test_meta */,
                                 state->key, strlen(state->key)+1, data, strlen(data)+1, write_cb);
                plat_free(key);
                plat_free(data);
            }
        }
    }
    plat_free(meta);
    plat_free(replication_props);
    if (shard_meta) {
        plat_free(shard_meta);
    }
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
        opts_config.iterations = 1000;
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
        g_state.rtfw = test_framework;
        g_state.ops_complete = 0;
        g_state.total_ops = opts_config.iterations;
    }

    XResume(fthSpawn(&user_operations_async, 40960), (uint64_t)test_framework);
    fthSchedulerPthread(0);
    plat_free(config);
    framework_sm_destroy(sm_config);
    return (0);
}
/*
 * @brief Callback function for rtfw shutdown asynchronous
 */
static void
user_async_shutdown_cb(struct plat_closure_scheduler *context,
                       void *env) {
    fthKill(1);
}

#include "platform/opts_c.h"
