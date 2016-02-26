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
 * File:  fcnl_framework_async_shard.c
 * Author: Zhenwei Lu
 *
 * Note:stress test case for shard ops in replication test_framework
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

#define SHARD_MAX 5

/*
 * We use a sub-category under test because test implies a huge number
 * of log messages out of simulated networking, flash, etc.
 */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/case");

struct rtfw_async_state {
    struct SDF_shard_meta *shard_meta;
    SDF_shardid_t shard_id;
    vnode_t node;
};

struct global_state {
    struct replication_test_framework *fm;

    int ops_complete;
    int total_ops;
    int ops_cs_start;
    int ops_cs_suc;
    int ops_cs_complete;
    int ops_ds_start;
    int ops_ds_suc;
    int ops_ds_complete;
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

static void run_cs_async();
static void run_ds_async();

static void
gen_and_run_random_op() {
    int is_cs = plat_prng_next_int(g_state->fm->api->prng, 2);
    if (0 == is_cs) {
        run_ds_async();
    } else {
        run_cs_async();
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
                 "cs_start:%d, cs_complete:%d, cs_suc:%d,\n"
                 "ds_start:%d, ds_complete:%d, ds_suc:%d,\n",
                 g_state->total_ops, g_state->ops_complete,
                 g_state->ops_cs_start, g_state->ops_cs_complete, g_state->ops_cs_suc,
                 g_state->ops_ds_start, g_state->ops_ds_complete, g_state->ops_ds_suc);

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
 * @brief Callback function for create shard complete
 */
static void
rtfw_cs_complete_cb(plat_closure_scheduler_t *context, void *env,
                    SDF_status_t status) {
    struct rtfw_async_state *state = (struct rtfw_async_state *)env;
    if (status == SDF_SUCCESS) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "cs complete node:%d, shard:%d",
                     (int)state->node, (int)state->shard_id);
        (void) __sync_add_and_fetch(&g_state->ops_cs_suc, 1);
        plat_free(state->shard_meta);
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_WARN,
                     "cs complete node:%d, shard:%d error",
                     (int)state->node, (int)state->shard_id);
    }
    (void) __sync_add_and_fetch(&g_state->ops_complete, 1);
    (void) __sync_add_and_fetch(&g_state->ops_cs_complete, 1);

    next_op();
}

/**
 * @brief Callback function for delete shard completed
 */
static void
rtfw_ds_complete_cb(struct plat_closure_scheduler *context, void *env,
                    SDF_status_t status) {
    struct rtfw_async_state *state = (struct rtfw_async_state *)env;
    if (status == SDF_SUCCESS) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "ds completed node:%d, shard_id:%d",
                     (int)state->node, (int)state->shard_id);
        plat_free(state);
        (void) __sync_add_and_fetch(&g_state->ops_ds_suc, 1);
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_WARN,
                     "ds complete node:%d, shard:%d error",
                     (int)state->node, (int)state->shard_id);
    }
    (void) __sync_add_and_fetch(&g_state->ops_complete, 1);
    (void) __sync_add_and_fetch(&g_state->ops_ds_complete, 1);
    next_op();
}

static vnode_t
get_random_node() {
    return ((vnode_t)plat_prng_next_int(g_state->fm->api->prng, 2));
}

static int
get_random_shard_id() {
    return (plat_prng_next_int(g_state->fm->api->prng, SHARD_MAX));
}

static void
run_ds_async() {
    replication_test_framework_cb_t ds_cb;
    struct rtfw_async_state *state = NULL;

    if (plat_calloc_struct(&state)) {
        state->node = get_random_node();
        state->shard_id = get_random_shard_id();
        
        ds_cb =
            replication_test_framework_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                 &rtfw_ds_complete_cb,
                                                 state);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "\n**************************************************\n"
                     "                  delete shard async                 "
                     "\n**************************************************");

        rtfw_delete_shard_async(g_state->fm, state->node, state->shard_id /* shard */, ds_cb);
        (void) __sync_add_and_fetch(&g_state->ops_ds_start, 1);
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_WARN,
                     "malloc error, not enough memory");
        plat_abort();
    }
}

static void
gen_shard_meta(vnode_t node, SDF_shardid_t shard_id,
               struct SDF_shard_meta **shard_meta) {
    SDF_status_t status;
    struct SDF_shard_meta *ret = NULL;
    struct sdf_replication_shard_meta r_shard_meta;
    SDF_replication_props_t *replication_props = NULL;
    int i, failed = 0;
    struct cr_shard_meta *in;

    failed = !plat_calloc_struct(&replication_props);
    if (!failed) {
        rtfw_set_default_replication_props(&g_state->fm->config, replication_props);
        ret = rtfw_init_shard_meta(&g_state->fm->config,
                                   node /* first_node */,
                                   /* shard_id, in real system generated by generate_shard_ids() */
                                   shard_id,
                                   replication_props);
        plat_assert(ret);

        /*
         * XXX: drew 2009-05-10 Move this to a helper function or get away
         * from the separate SDF_shard_meta type.
         */
        memset(&r_shard_meta, 0, sizeof(r_shard_meta));
        r_shard_meta.type = ret->replication_props.type;
        r_shard_meta.nreplica = ret->replication_props.num_replicas;
        r_shard_meta.current_home_node = node;

        /* XXX: This all will end up on node 0 */
        for (i = 0; i < r_shard_meta.nreplica; ++i) {
            r_shard_meta.pnodes[i] =
                (ret->first_node + i) % g_state->fm->config.nnode;
        }

        r_shard_meta.meta_pnode  = ret->first_meta_node;
        
        /*
         * create a shard on test flash to ensure meta data
         * persitence will sucess(currently it fails for no shard).
         * However, that will change replicator test mode
         */
        status = cr_shard_meta_create(&in,
                                      &g_state->fm->config.replicator_config,
                                      ret);
        plat_assert(status == SDF_SUCCESS);
        cr_shard_meta_free(in);
        *shard_meta = ret;
    }
    plat_free(replication_props);
}

static void
run_cs_async() {
    replication_test_framework_cb_t cb;
    struct rtfw_async_state *state = NULL;

    if (plat_calloc_struct(&state)) {
        state->node = get_random_node();
        state->shard_id = get_random_shard_id();
        gen_shard_meta(state->node, state->shard_id, &state->shard_meta);

        cb =
            replication_test_framework_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                 rtfw_cs_complete_cb,
                                                 state);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "\n**************************************************\n"
                     "                  create shard async                 "
                     "\n**************************************************");

        rtfw_create_shard_async(g_state->fm, state->node, state->shard_meta, cb);
        (void) __sync_add_and_fetch(&g_state->ops_cs_start, 1);
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_WARN,
                     "malloc error, not enough memory");
        plat_abort();
    }
}

/**
 * @brief synchronized create_shard/write/read/delete/delete_shard operations
 */
void
user_operations_async_stress(uint64_t args) {
    /* Assure test_framework is started?! */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start test_framework");
    rtfw_start(g_state->fm);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework started\n");

    next_op();
}

static void
init_global_state(struct replication_test_framework *fm,
                  int max_ops) {
    g_state = &g_state_t;
    g_state->fm = fm;

    g_state->total_ops = max_ops;
    g_state->ops_complete = 0;
    g_state->ops_cs_complete = 0;
    g_state->ops_ds_complete = 0;
    g_state->ops_cs_suc = 0;
    g_state->ops_cs_start = 0;
    g_state->ops_ds_start = 0;
    g_state->ops_ds_suc = 0;
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
