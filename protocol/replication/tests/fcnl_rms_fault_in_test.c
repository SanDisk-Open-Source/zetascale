/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: fcnl_rms_shard_meta_fault_in.c
 * Author: Zhenwei Lu
 *
 * fault injection for modify shard meta
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

enum shard_meta_event_type {
    GET_SHARD_META,
    PUT_SHARD_META,
    /* Fixme: zhenwei, 6-18, currently del shard meta is not available */
    DEL_SHARD_META
};

struct update_meta_data_at_event_state {
    struct replication_test_framework *test_framework;
    enum shard_meta_event_type type;
    struct cr_shard_meta *in;
    struct cr_shard_meta *out;
    struct sdf_replication_shard_meta *r_shard_meta;
    uint64_t lease_usecs;
    struct timeval expires;
    vnode_t node;
};

#if 0
/**
 * Apply shard_meta event at given time
 */
static void
user_at_event(struct plat_closure_scheduler *context, void *env,
              SDF_status_t status) {
    struct update_meta_data_at_event_state *state =
        (struct update_meta_data_at_event_state *)env;
    struct cr_shard_meta *in;

    plat_assert(state != NULL && state->in != NULL);

    if (state->type == GET_SHARD_META) {
        status = rtfw_get_shard_meta_sync(state->test_framework, state->node,
                                          state->in->persistent.sguid,
                                          state->r_shard_meta, &state->out,
                                          &state->expires);

    } else if (state->type == PUT_SHARD_META) {
        in = state->in;
        in->persistent.current_home_node = state->node;
        ++in->persistent.shard_meta_seqno;
        ++in->persistent.ltime;
        in->persistent.lease_usecs = state->lease_usecs;

        rtfw_put_shard_meta_sync(state->test_framework, state->node,
                                 state->in, &state->out, &state->expires);
    } else {
        rtfw_delete_shard_meta_sync(state->test_framework, state->node,
                                    state->in, &state->out,
                                    &state->expires);
    }

    cr_shard_meta_free(state->in);
    plat_free(state);
}

static void
user_start_shard_meta_event(struct replication_test_framework *test_framework,
                            vnode_t node, enum shard_meta_event_type type,
                            struct cr_shard_meta *in, struct cr_shard_meta *out,
                            struct sdf_replication_shard_meta *r_shard_meta,
                            uint64_t lease_usecs, struct timeval expires) {
    struct update_meta_data_at_event_state *state = NULL;
    replication_test_framework_cb_t cb;

    if (plat_calloc_struct(&state)) {
        state->test_framework = test_framework;
        state->node = node;
        state->type = type;
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
#endif
static void
hs_print_current_lease(struct cr_shard_meta *csm) {
    plat_assert(csm);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "lease usecs: %d", (int)csm->persistent.lease_usecs);
}

#if 0
static int
hs_expire_simple(struct replication_test_framework *test_framework,
                 SDF_replication_props_t *replication_props) {
    SDF_status_t status;
    struct SDF_shard_meta *shard_meta;
    struct sdf_replication_shard_meta *r_shard_meta;
    SDF_shardid_t shard_id;
    vnode_t node = 0;
    struct cr_shard_meta *in;
    struct cr_shard_meta *out;
    struct timeval expires;

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

    /* perhaps shard has been created in other unit test */
    plat_assert(status == SDF_SUCCESS || status == SDF_FAILURE_STORAGE_WRITE);
    if (status == SDF_SUCCESS) {
        plat_assert(out);
        hs_print_current_lease(in);
        hs_print_current_lease(out);

        out->persistent.lease_usecs = in->persistent.lease_usecs;
        plat_assert(0 == cr_shard_meta_cmp(in, out));
        cr_shard_meta_free(out);
    }

    /*
     * XXX: drew 2009-05-09 sleep and validate that less time remains on the
     * lease.
     */
    
    rtfw_sleep_usec(test_framework, 2000000);
    /* get meta on node0 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "get on node 0");
    status = rtfw_get_shard_meta_sync(test_framework, node,
                                      in->persistent.sguid, r_shard_meta, &out,
                                      &expires);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "get on node 0 complete");
    plat_assert(status == SDF_SUCCESS);
    plat_assert(out);
    
    hs_print_current_lease(in);
    hs_print_current_lease(out);
    plat_assert(in->persistent.lease_usecs > out->persistent.lease_usecs);

    out->persistent.lease_usecs = in->persistent.lease_usecs;
    plat_assert(0 == cr_shard_meta_cmp(in, out));
    cr_shard_meta_free(out);

    /* verify get shard meta does not renew lease */
    rtfw_sleep_usec(test_framework, 2000000);
    /* get meta on node1 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "first get on node 1");
    status = rtfw_get_shard_meta_sync(test_framework, 1,
                                      in->persistent.sguid,
                                      r_shard_meta, &out, &expires);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "first get on node 1 complete");
    plat_assert(status == SDF_SUCCESS);
    plat_assert(out);
    
    hs_print_current_lease(in);
    hs_print_current_lease(out);

    out->persistent.lease_usecs = in->persistent.lease_usecs;
    plat_assert(0 == cr_shard_meta_cmp(in, out));
    cr_shard_meta_free(out);

    plat_free(shard_meta);
    plat_free(r_shard_meta);

    return ((status == SDF_SUCCESS)? 0 : 1);
}
#endif

static int
rms_modify_shard_meta_fault_in(struct replication_test_framework *test_framework,
                               SDF_replication_props_t *replication_props) {
    SDF_status_t status;
    struct SDF_shard_meta *shard_meta;
    struct sdf_replication_shard_meta *r_shard_meta;
    SDF_shardid_t shard_id = 1;
    vnode_t node = 1;
    struct cr_shard_meta *in;
    struct cr_shard_meta *out;
    struct cr_shard_meta *temp;
    struct timeval expires;

    /* init cr_shard_meta and shard_meta */
    init_meta_data(test_framework, replication_props, &r_shard_meta, node, shard_id,
                   LEASE_USECS, &shard_meta, &in);
    plat_assert(r_shard_meta);
    plat_assert(shard_meta);
    plat_assert(in);

    /* put meta on node 1 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "create on node 1");
    status = rtfw_create_shard_meta_sync(test_framework, node, in, &out,
                                         &expires);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "create on node 1 complete");
    /* perhaps shard has been created in other unit test since currently we can't delete shard meta */
    plat_assert(status == SDF_SUCCESS || status == SDF_FAILURE_STORAGE_WRITE);
    if (status == SDF_SUCCESS) {
        plat_assert(out);
        hs_print_current_lease(in);
        hs_print_current_lease(out);

        out->persistent.lease_usecs = in->persistent.lease_usecs;
        plat_assert(0 == cr_shard_meta_cmp(in, out));
        cr_shard_meta_free(out);
    }

    /* get meta on node0 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "get on node 0");
    status = rtfw_get_shard_meta_sync(test_framework, node,
                                      in->persistent.sguid, r_shard_meta, &out,
                                      &expires);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "get on node 0 complete");
    plat_assert(status == SDF_SUCCESS);
    plat_assert(out);
    hs_print_current_lease(in);
    hs_print_current_lease(out);

    /* buffer an cr_shard_meta here */
    temp = cr_shard_meta_dup(out);
    plat_assert(temp);

    out->persistent.lease_usecs = in->persistent.lease_usecs;
    plat_assert(0 == cr_shard_meta_cmp(in, out));
    cr_shard_meta_free(out);

    /* modify meta data before expire */
    /* fault injections */
    /* (1) put meta data with incorrect ltime non home node */
    in->persistent.ltime = -1;
    ++in->persistent.shard_meta_seqno;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "put shard meta on node 1 with illegal ltime");
    status = rtfw_put_shard_meta_sync(test_framework, 1, in, &out,
                                      &expires);
    plat_assert(status == SDF_SUCCESS);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "put shard meta on node 1 with illegal ltime complete");
    /**
     * Fixme: zhenwei, shard meta with incorrect ltime can be set to any node
     */
    temp->persistent.lease_usecs = out->persistent.lease_usecs;
    plat_assert(0 != cr_shard_meta_cmp(temp, out));

    /* buffer the latest cr_shard_meta */
    cr_shard_meta_free(temp);
    temp = cr_shard_meta_dup(out);
    cr_shard_meta_free(out);

    /* (2) put meta data with incorrect seqno */
    in->persistent.shard_meta_seqno += 2;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "put shard meta on node 1 with illegal ltime");
    status = rtfw_put_shard_meta_sync(test_framework, 1, in, &out,
                                      &expires);
    plat_assert(status != SDF_SUCCESS);
    status = SDF_SUCCESS;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "put shard meta on node 1 with illegal ltime complete");
    plat_assert(temp->persistent.ltime == out->persistent.ltime &&
                temp->persistent.shard_meta_seqno == out->persistent.shard_meta_seqno);
                
    /* buffer the latest cr_shard_meta */
    cr_shard_meta_free(temp);
    temp = cr_shard_meta_dup(out);
    cr_shard_meta_free(out);

    /* (3) put meta data with a lease than LEASE_USECS */
    in->persistent.lease_usecs = LEASE_USECS * 2;
    /* skip since (2) +2 for it */
    /* ++in->persistent.shard_meta_seqno; */
    --in->persistent.shard_meta_seqno;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "put shard meta on node 1 with illegal ltime");
    status = rtfw_put_shard_meta_sync(test_framework, 1, in, &out,
                                      &expires);
    plat_assert(status == SDF_SUCCESS);
    out->persistent.lease_usecs = in->persistent.lease_usecs;
    plat_assert(0 == cr_shard_meta_cmp(in, out));

    cr_shard_meta_free(out);
    cr_shard_meta_free(temp);
    cr_shard_meta_free(in);


    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "put shard meta on node 1 with illegal ltime");

    plat_free(shard_meta);
    plat_free(r_shard_meta);

    return (status == SDF_SUCCESS ? 0 : 1);
}

/**
 * @brief synchronized create_shard/write/read/delete/delete_shard operations
 */
void
user_operations_rms_sm_fault_in(uint64_t args) {
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
#if 0
    struct cr_shard_meta *out = NULL;
    SDF_status_t status = SDF_SUCCESS;
    struct timeval expires;
    struct timeval now;
#endif

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

    failed = rms_modify_shard_meta_fault_in(test_framework, replication_props);
    plat_assert(failed != 1);

    cr_shard_meta_free(in);
    plat_free(replication_props);

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
    XResume(fthSpawn(&user_operations_rms_sm_fault_in, 40960), (uint64_t)test_framework);
    fthSchedulerPthread(0);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE, "JOIN");
    plat_free(config);
    framework_sm_destroy(sm_config);
    return (0);
}

#include "platform/opts_c.h"
