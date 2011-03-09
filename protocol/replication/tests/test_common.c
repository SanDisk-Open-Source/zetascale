/*
 * File:   test_common.c
 * Author: Zhenwei Lu
 *
 * Note:test case for replication test_framework
 * Created on Dec 8, 2008, 12:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_common.c 10166 2009-06-22 06:57:34Z lzwei $
 */

#include "common/sdftypes.h"
#include "misc/misc.h"

#include "platform/assert.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _replication_test_framework_sm
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/stdlib.h"
#include "platform/time.h"
#include "platform/prng.h"
#include "platform/fth_scheduler.h"
#include "fth/fth.h"

#include "protocol/replication/copy_replicator.h"
#include "protocol/replication/replicator.h"

#define RT_USE_COMMON 1
#include "test_common.h"


#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_PROT
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_WARN PLAT_LOG_LEVEL_WARN
#define LOG_FATAL PLAT_LOG_LEVEL_FATAL

#define CRASH_INCLUDED
#define MAX(a, b) (a) > (b)? (a) : (b)
#define MILLION 1000000

SDF_status_t
framework_sm_init(int argc, char **argv,
                  struct plat_opts_config_replication_test_framework_sm
                  *config)
{
    plat_assert(config);
    SDF_status_t ret = SDF_SUCCESS;
    plat_shmem_config_init(&config->shmem);
    /* start shared memory */
    if (plat_opts_parse_replication_test_framework_sm(config, argc, argv)) {
        ret = SDF_FALSE;
    }

    if (ret == SDF_SUCCESS) {
        ret = rt_sm_init(&config->shmem) ? SDF_FALSE : SDF_SUCCESS;
    }

    return (ret);
}

int
rt_sm_init(struct plat_shmem_config *shmem)
{
    int status;

    status = plat_shmem_prototype_init(shmem);
    if (status) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                     "shmem init failure: %s", plat_strerror(-status));
    }

    if (!status) {
        status = plat_shmem_attach(plat_shmem_config_get_path(shmem));
        if (status) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "shmem attach failure: %s", plat_strerror(-status));
        }
    }

    return (status);
}

int
framework_sm_destroy(struct plat_opts_config_replication_test_framework_sm
                     *config)
{
    int ret;

    ret = rt_sm_detach(&config->shmem) ? 1 : 0;
    plat_shmem_config_destroy(&config->shmem);

    plat_free(config);
    return (ret);
}

int
rt_sm_detach()
{
    int status;

    status = plat_shmem_detach();
    if (status) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                     "shmem detach failure: %s", plat_strerror(-status));
    }
    return (status);
}

const char *
rt_type_to_string(enum replication_test_type test_type) {
    switch (test_type) {
#define item(caps, lower) case caps: return (#caps);
    REPLICATION_TEST_TYPE_ITEMS()
#undef item
    default:
        return ("Invalid");
    }
}

void
rt_timing_init(struct replication_test_timing *timing)
{
    timing->max_delay_us = 1000;
    timing->min_delay_us = 20;
    timing->min_delay_percent = 20;
}

int64_t
rt_set_async_timeout(struct replication_test_config config)
{
    int64_t ret;
#if 0
    /*
     * XXX: drew 2009-05-29 This is incorrect due to how multi-phase
     * operations are handled.
     */
    ret = (int64_t)(2 * config.network_timing.max_delay_us +
                    config.flash_timing.max_delay_us) *
        config.iterations * TIMEOUT_FACTOR;
#else
    ret = SDF_FTH_MBX_TIMEOUT_ONLY_ERROR;
#endif

    return (ret);
}

void
rt_config_init(struct replication_test_config *config, uint32_t iterations)
{
    config->nnode = MAX_NODE;
    config->nparallel_flash_ops_per_node = MAX_NODE;
    config->iterations = iterations;
    rt_timing_init(&(config->network_timing));
    rt_timing_init(&(config->flash_timing));

    /*
     * XXX: drew 2010-04-01 This is for backwards compatability but
     * should become something more realistic once the tests have been
     * adjusted to take it into account.
     */
    config->liveness_timing.min_delay_us = 0;
    config->liveness_timing.max_delay_us = 0;
    config->liveness_timing.min_delay_percent = 100;

    config->num_replicas = 2;
    config->replication_type = SDF_REPLICATION_INVALID;

    sdf_replicator_config_init(&(config->replicator_config), SDF_ILLEGAL_PNODE,
                               0 /* number of nodes which is invalid */);
    config->replicator_config.timeout_usecs = SDF_FTH_MBX_TIMEOUT_ONLY_ERROR;
#ifdef CRASH_INCLUDED
    /* Crash timeout setting, max(flash_max_delay, network_max_delay) */
    config->replicator_config.timeout_usecs = SDF_FTH_MBX_TIMEOUT_ONLY_ERROR;
    /* MAX(config->network_timing.max_delay_us, config->flash_timing.max_delay_us); */
#endif

}

void
rt_common_test_config_init(struct rt_common_test_config *config)
{
    memset(config, 0, sizeof (*config));
    plat_shmem_config_init(&config->shmem_config);
    rt_config_init(&config->test_config, 1000);

    config->ffdc_buffer_len = FFDC_THREAD_BUFSIZE;
}

void
rt_common_test_config_destroy(struct rt_common_test_config *config) {
    plat_shmem_config_destroy(&config->shmem_config);
}

int
rt_common_init(struct rt_common_test_config *config) {
    int ret;

    ret = rt_sm_init(&config->shmem_config);

    if (!config->ffdc_disable) {
        ret = ffdc_initialize(0 /* read-write */, BLD_VERSION,
                              config->ffdc_buffer_len);
    }

    return (ret);
}

void
rt_common_detach(struct rt_common_test_config *config) {
    if (!config->ffdc_disable) {
        ffdc_detach();
    }

    rt_sm_detach();
}

int
r_shard_meta_init(struct SDF_shard_meta *shard_meta, vnode_t cur_home_node,
                  struct sdf_replication_shard_meta **out, uint32_t nnode) {
    struct sdf_replication_shard_meta *r_shard_meta;
    int failed, i;
  
    plat_assert(shard_meta);
    if (plat_calloc_struct(&r_shard_meta)) {
        r_shard_meta->type = shard_meta->replication_props.type;
        r_shard_meta->nreplica = shard_meta->replication_props.num_replicas;
        r_shard_meta->current_home_node = cur_home_node;
        r_shard_meta->meta_pnode = shard_meta->first_meta_node;
        r_shard_meta->meta_shardid = shard_meta->sguid_meta;
       
        /* XXX: This all will end up on node 0 */
        for (i = 0; i < r_shard_meta->nreplica; ++i) {
            r_shard_meta->pnodes[i] =
                (shard_meta->first_node + i) % nnode;
        }
       
        *out = r_shard_meta;
        failed = 0;
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                     "Out of memory ");
        failed = 1;
    }
    return (failed);
}

void
init_meta_data(struct replication_test_framework *test_framework,
               SDF_replication_props_t *replication_props,
               struct sdf_replication_shard_meta **out_r_shard_meta,
               vnode_t node, SDF_shardid_t shard_id, uint32_t lease_usecs,
               struct SDF_shard_meta **out_shard_meta,
               struct cr_shard_meta **out_cr_meta) {
    struct SDF_shard_meta *shard_meta;
    SDF_status_t status;

    shard_meta = rtfw_init_shard_meta(&test_framework->config,
                                      node /* first_node */,
                                      shard_id
                                      /* shard_id, in real system generated by generate_shard_ids() */,
                                      replication_props);
    plat_assert(shard_meta);

    r_shard_meta_init(shard_meta, node, out_r_shard_meta,
                      test_framework->config.nnode);
    plat_assert(*out_r_shard_meta);

    status = cr_shard_meta_create(out_cr_meta,
                                  &test_framework->config.replicator_config,
                                  shard_meta);
    plat_assert(status == SDF_SUCCESS);
    plat_assert(*out_cr_meta);
    (*out_cr_meta)->persistent.current_home_node = node;
    (*out_cr_meta)->persistent.lease_usecs = lease_usecs;

    *out_shard_meta = shard_meta;
}

#include "platform/opts_c.h"
