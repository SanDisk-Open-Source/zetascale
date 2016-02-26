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
 * File: fcnl_rms_multi_put_test.c_
 * Author: Zhenwei Lu
 *
 * multiple put shard meta to modify shard meta
 *
 * Created on Jun 15, 2009, 12:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id$
 */

#include "misc/misc.h"

#include "platform/assert.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _sn_get_put_test
#include "platform/opts.h"

#include "protocol/replication/copy_replicator_internal.h"

#include "test_common.h"
#include "test_framework.h"

#define NUM_REPLICAS 4
#define LEASE_USECS 10000000
/*
 * We use a sub-category under test because test implies a huge number
 * of log messages out of simulated networking, flash, etc.
 */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/case");

#define PLAT_OPTS_ITEMS_sn_get_put_test() \
    PLAT_OPTS_COMMON_TEST(common_config)

struct common_test_config {
    struct plat_shmem_config shmem_config;
};

#define PLAT_OPTS_COMMON_TEST(config_field) \
    PLAT_OPTS_SHMEM(config_field.shmem_config)

struct plat_opts_config_sn_get_put_test {
    struct common_test_config common_config;
};

#if 0
enum shard_meta_event_type {
    GET_SHARD_META,
    PUT_SHARD_META,
    /* Fixme: zhenwei, 6-18, currently del shard meta is not available */
    DEL_SHARD_META
};
#endif

struct update_meta_data_at_event_state {
    struct replication_test_framework *test_framework;
    /* enum shard_meta_event_type type; */
    struct cr_shard_meta *in;
    struct cr_shard_meta *out;
    struct sdf_replication_shard_meta *r_shard_meta;
    uint64_t lease_usecs;
    struct timeval expires;
    vnode_t node;
};

/**
 * Apply shard_meta event at given time
 */
static void
user_at_event(struct plat_closure_scheduler *context, void *env,
              SDF_status_t status) {
    struct update_meta_data_at_event_state *state =
        (struct update_meta_data_at_event_state *)env;
    struct cr_shard_meta *in, *out;
    SDF_status_t tar_status;

    plat_assert(state != NULL && state->in != NULL);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "fire an event for node:%d, shard:%d\n",
                 (int)state->node, (int)state->in->persistent.sguid);

    status = rtfw_get_shard_meta_sync(state->test_framework, state->node,
                                      state->in->persistent.sguid,
                                      state->r_shard_meta, &state->out,
                                      &state->expires);
    /**
     * Fixme: zhenwei, sometimes event are fired after test framework are shutdown
     */
    plat_assert(status == SDF_SUCCESS || status == SDF_SHUTDOWN);
    if (status == SDF_SHUTDOWN) {
        return;
    }

    in = state->in;
    out = state->out;
    in->persistent.current_home_node = state->node;
    in->persistent.shard_meta_seqno = out->persistent.shard_meta_seqno + 1;
    in->persistent.ltime = out->persistent.ltime + 1;
    in->persistent.lease_usecs = state->lease_usecs;

    status = rtfw_put_shard_meta_sync(state->test_framework, state->node,
                                      state->in, &state->out, &state->expires);
    if (in->persistent.current_home_node != out->persistent.current_home_node) {
        tar_status = SDF_LEASE_EXISTS;
    } else {
        tar_status = SDF_SUCCESS;
    }

    /* these below should be changed according to #rms_op_allowed */
    plat_assert(status == tar_status);
    if (status == SDF_SUCCESS) {
        plat_assert(0 == cr_shard_meta_cmp(state->in, state->out));
    }

#if 0
        rtfw_delete_shard_meta_sync(state->test_framework, state->node,
                                    state->in, &state->out,
                                    &state->expires);
#endif

    cr_shard_meta_free(state->in);
    plat_free(state);
}

static void
user_start_shard_meta_event(struct replication_test_framework *test_framework,
                            vnode_t node, /* enum shard_meta_event_type type, */
                            struct cr_shard_meta *in, struct cr_shard_meta *out,
                            struct sdf_replication_shard_meta *r_shard_meta,
                            uint64_t lease_usecs, struct timeval expires) {
    struct update_meta_data_at_event_state *state = NULL;
    replication_test_framework_cb_t cb;

    if (plat_calloc_struct(&state)) {
        state->test_framework = test_framework;
        state->node = node;
        /* state->type = type; */
        state->in = in;
        state->out = out;
        state->r_shard_meta = r_shard_meta;
        state->expires = expires;
        state->lease_usecs = lease_usecs;

        cb = replication_test_framework_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                  &user_at_event, state);
        rtfw_at_async(test_framework, &expires, cb);
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_WARN,
                     "failed to allocate memory");
    }
}

static void
hs_print_current_lease(struct cr_shard_meta *csm) {
    plat_assert(csm);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "lease usecs: %d", (int)csm->persistent.lease_usecs);
}

/**
 * @brief testing race condition on meta data modification
 */
static int
hs_multi_modify_meta_data(struct replication_test_framework *test_framework,
                          SDF_replication_props_t *replication_props) {
    SDF_status_t status;
    struct SDF_shard_meta *shard_meta;
    struct sdf_replication_shard_meta *r_shard_meta;
    SDF_shardid_t shard_id = 1;
    vnode_t node = 0;
    int i;
    struct cr_shard_meta *in;
    struct cr_shard_meta *out;
    struct timeval expires;
    struct cr_shard_meta *out_at[NUM_REPLICAS];
    struct cr_shard_meta *in_at[NUM_REPLICAS];
    struct timeval expires_at[NUM_REPLICAS];

    memset(out_at, 0, NUM_REPLICAS*sizeof(struct cr_shard_meta*));

    /* init cr_shard_meta and shard_meta */
    init_meta_data(test_framework, replication_props, &r_shard_meta, node, shard_id,
                   LEASE_USECS, &shard_meta, &in);
    plat_assert(r_shard_meta);
    plat_assert(shard_meta);
    plat_assert(in);

    /* put meta on node 0 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "create on node 0");
    status = rtfw_create_shard_meta_sync(test_framework, node, in, &out,
                                         &expires);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "create on node 0 complete");
    plat_assert(status == SDF_SUCCESS);
    plat_assert(out);
    hs_print_current_lease(in);
    hs_print_current_lease(out);

    out->persistent.lease_usecs = in->persistent.lease_usecs;
    plat_assert(0 == cr_shard_meta_cmp(in, out));
    cr_shard_meta_free(out);

    /**
     * register some modification shard meta closure
     * to put shard meta into every replica node
     */
    for (i = 0; i < test_framework->config.num_replicas; i++) {
        struct timeval temp;
        uint64_t lease_usecs = LEASE_USECS;
        in_at[i] = cr_shard_meta_dup(in);
        rtfw_get_time(test_framework, &temp);
        expires_at[i].tv_sec = temp.tv_sec + plat_prng_next_int(test_framework->api->prng,
                                                                LEASE_USECS / 1000000);
        expires_at[i].tv_usec = 0;
        user_start_shard_meta_event(test_framework, i, /* PUT_SHARD_META, */ in_at[i], out_at[i],
                                    r_shard_meta, lease_usecs, expires_at[i] /* absolute time */);
    }
    
    /* pass half lease to make registered closure applied */
    expires.tv_sec = 5;
    expires.tv_usec = 0;
    rtfw_block_until(test_framework, expires);


    return (status == SDF_SUCCESS ? 0 : 1);
}

/**
 * @brief synchronized create_shard/write/read/delete/delete_shard operations
 */
void
user_operations_rms_mp(uint64_t args) {
    struct replication_test_framework *test_framework =
        (struct replication_test_framework *)args;
    SDF_shardid_t shard_id = 1;
    struct SDF_shard_meta *shard_meta = NULL;
    struct sdf_replication_shard_meta *r_shard_meta;
    /* configuration infomation about shard */
    SDF_replication_props_t *replication_props = NULL;
    struct cr_shard_meta *in;
    vnode_t node = 0;
    int failed;

    /* configures test framework accommodate to RT_TYPE_META_STORAGE */
    failed = !(plat_calloc_struct(&replication_props));
    plat_assert(!failed);

    rtfw_set_default_replication_props(&test_framework->config,
                                       replication_props);

    init_meta_data(test_framework, replication_props, &r_shard_meta, node, shard_id,
                   LEASE_USECS, &shard_meta, &in);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start test_framework");
    rtfw_start(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework started\n");

    failed = hs_multi_modify_meta_data(test_framework, replication_props);
    plat_assert(failed != 1);

    cr_shard_meta_free(in);

    /* Shutdown test framework */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                 "\n************************************************************\n"
                 "                  Test framework shutdown                       "
                 "\n************************************************************");
    rtfw_shutdown_sync(test_framework);

    /* Terminate scheduler */
    fthKill(1);
}

int main(int argc, char **argv) {
    SDF_status_t status;
    struct replication_test_framework *test_framework = NULL;
    struct replication_test_config *config = NULL;
    int failed;

    struct plat_opts_config_sn_get_put_test opts_config;
    memset(&opts_config, 0, sizeof (opts_config));
    int opts_status = plat_opts_parse_sn_get_put_test(&opts_config, argc, argv);
    if (opts_status) {
        plat_opts_usage_sn_get_put_test();
        return (1);
    }

    failed = !plat_calloc_struct(&config);

    struct plat_opts_config_replication_test_framework_sm *sm_config;
    plat_calloc_struct(&sm_config);
    plat_assert(sm_config);

    /* start shared memory */
    status = framework_sm_init(0, NULL, sm_config);

    /* start fthread library */
    fthInit();

    rt_config_init(config, 10 /* hard code iterations here */);
    config->test_type = RT_TYPE_META_STORAGE;
    config->nnode = 5;
    config->num_replicas = NUM_REPLICAS;

    test_framework = replication_test_framework_alloc(config);
    if (test_framework) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE, "test_framework %p allocated\n",
                     test_framework);
    }
    XResume(fthSpawn(&user_operations_rms_mp, 40960), (uint64_t)test_framework);
    fthSchedulerPthread(0);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE, "JOIN");
    plat_free(config);
    framework_sm_destroy(sm_config);
    return (0);
}

#include "platform/opts_c.h"
