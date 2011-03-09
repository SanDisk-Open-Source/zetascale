/*
 * File: fcnl_rms_put_crash_get.c
 * Author: Zhenwei Lu
 *
 * put shard meta => crash node => restart node => get shard meta
 *
 * Created on Jun 17, 2009, 12:04 AM
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
#define EXPIRE_USEC 20
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

void
user_operations_rms_put_crash_get_test(uint64_t args) {
    struct replication_test_framework *test_framework =
        (struct replication_test_framework *)args;
    SDF_shardid_t shard_id = 1;
    struct SDF_shard_meta *shard_meta = NULL;
    struct sdf_replication_shard_meta *r_shard_meta;
    /* configuration infomation about shard */
    SDF_replication_props_t *replication_props = NULL;
    SDF_status_t status = SDF_SUCCESS;
    struct cr_shard_meta *in;
    struct cr_shard_meta *out = NULL;
    vnode_t node = 0;
    int failed;
    struct timeval expires;

    /* configures test framework accommodate to RT_TYPE_META_STORAGE */
    failed = !(plat_calloc_struct(&replication_props));
    plat_assert(!failed);

    rtfw_set_default_replication_props(&test_framework->config,
                                       replication_props);

    /* init cr_shard_meta and shard_meta */
    init_meta_data(test_framework, replication_props, &r_shard_meta, node, shard_id,
                   LEASE_USECS, &shard_meta, &in);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start test_framework");
    rtfw_start(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework started\n");

    /* put meta on node0 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "create on node 0");
    status = rtfw_create_shard_meta_sync(test_framework, node, in, &out,
                                         &expires);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "create on node 0 complete");
    plat_assert(status == SDF_SUCCESS);
    plat_assert(out);

    out->persistent.lease_usecs = in->persistent.lease_usecs;
    plat_assert(0 == cr_shard_meta_cmp(in, out));
    cr_shard_meta_free(out);

    /* get meta on node0 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "get on node 0");
    status = rtfw_get_shard_meta_sync(test_framework, node,
                                      in->persistent.sguid, r_shard_meta, &out,
                                      &expires);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "get on node 0 complete");
    plat_assert(status == SDF_SUCCESS);
    plat_assert(out);
    out->persistent.lease_usecs = in->persistent.lease_usecs;
    plat_assert(0 == cr_shard_meta_cmp(in, out));
    cr_shard_meta_free(out);

    /* crash node 0 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "crash node 0");
    status = rtfw_crash_node_sync(test_framework, node);
    plat_assert(status == SDF_SUCCESS);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "crash node 0 complete");


    /* get meta on node3 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "first get on node 3");
    status = rtfw_get_shard_meta_sync(test_framework, 3,
                                      in->persistent.sguid,
                                      r_shard_meta, &out, &expires);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "first get on node 3 complete");
    plat_assert(status == SDF_NODE_DEAD);

    /* get meta on node0 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "first get on node 0");
    status = rtfw_get_shard_meta_sync(test_framework, 0,
                                      in->persistent.sguid,
                                      r_shard_meta, &out, &expires);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "first get on node 3 complete");
    plat_assert(status == SDF_TEST_CRASH);

    /* let it expire */
    expires.tv_sec = 15;
    rtfw_block_until(test_framework, expires);

    /* restart node 0 */
    status = rtfw_start_node(test_framework, node);
    plat_assert(status == SDF_SUCCESS);

    /* get meta on node0 to verify shard meta persistence */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "first get on node 0");
    status = rtfw_get_shard_meta_sync(test_framework, 0,
                                      in->persistent.sguid,
                                      r_shard_meta, &out, &expires);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "first get on node 3 complete");
    plat_assert(status == SDF_SUCCESS);
   
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
    XResume(fthSpawn(&user_operations_rms_put_crash_get_test, 40960), (uint64_t)test_framework);
    fthSchedulerPthread(0);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE, "JOIN");
    plat_free(config);
    framework_sm_destroy(sm_config);
    return (0);
}

#include "platform/opts_c.h"
