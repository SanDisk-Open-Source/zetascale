/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fcnl_framework_msg_timeout_test.c
 * Author: Zhenwei Lu
 *
 * Note:timeout test case for replication test_framework
 * Created on Feb 9, 2009, 12:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcnl_framework_msg_timeout_test.c 9779 2009-05-31 10:40:53Z lzwei $
 */



#include "platform/stdio.h"

#include "protocol/protocol_common.h"

#include "test_framework.h"
#include "test_model.h"
#include "test_common.h"
#include "test_generator.h"

/*
 * We use a sub-category under test because test implies a huge number
 * of log messages out of simulated networking, flash, etc.
 */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/case");

#define PLAT_OPTS_NAME(name) name ## _msg_timeout
#include "platform/opts.h"
#include "misc/misc.h"

#define PLAT_OPTS_ITEMS_msg_timeout()                                                  \
    PLAT_OPTS_COMMON_TEST(common_config)                                                   \
    item("timeout_usecs", "user specified timeout us_secs", ITERATIONS,                            \
         parse_int(&config->timeout_usecs, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)

/* 1 million usecs default */
#define DEFAUT_USECS 1000000

int timeout_usecs;

struct common_test_config {
    struct plat_shmem_config shmem_config;
};

static int wrapper_response_received;

#define PLAT_OPTS_COMMON_TEST(config_field) \
    PLAT_OPTS_SHMEM(config_field.shmem_config)

struct plat_opts_config_msg_timeout {
    struct common_test_config common_config;
    int timeout_usecs;
};

struct smt_request_state {
    /** @brief send msg wrapper */
    struct sdf_msg_wrapper *send_wrapper;

    /** @brief receiving mbx */
    struct sdf_fth_mbx *mbx;
};

static void
smt_msg_free(struct plat_closure_scheduler *context, void *env,
             struct sdf_msg *msg) {
    sdf_msg_free(msg);
}

static void
smt_request_state_destroy(struct smt_request_state *request_state) {
    plat_assert(request_state);

    if (request_state->send_wrapper) {
        sdf_msg_wrapper_ref_count_dec(request_state->send_wrapper);
    }
    if (request_state->mbx) {
        plat_free(request_state->mbx);
    }
    plat_free(request_state);
}

/**
 * @brief replica distribution, the same to #rtfw_replica_destribute
 */
static void
smt_replica_destribute(struct replication_test_config *config,
                       SDF_protocol_msg_t *pm) {
    int i;
    for (i = 0; i < config->nnode; i++) {
        pm->op_meta.shard_meta.pnodes[i] = i;
    }
}

static void
smt_send_response(struct plat_closure_scheduler *context, void *env,
                  struct sdf_msg_wrapper *response) {
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "smt get response");
    plat_assert(response);
    struct sdf_msg *msg = NULL;
    struct sdf_msg_error_payload *msg_payload;

    sdf_msg_wrapper_rwref(&msg, response);
    plat_assert(msg);
    if (response->msg_type == SDF_MSG_ERROR) {
        msg_payload = (struct sdf_msg_error_payload *)msg->msg_payload;
        if (msg_payload->error == SDF_TIMEOUT) {
            wrapper_response_received = 1;
        } else {
            wrapper_response_received = 1;
        }
    }
    sdf_msg_wrapper_rwrelease(&msg, response);
    sdf_msg_wrapper_ref_count_dec(response);
}


/**
 * @brief generator a msg wrapper for timeout
 */
static struct smt_request_state *
smt_create_shard_sync(struct replication_test_framework *test_framework,
                      vnode_t node, struct SDF_shard_meta *shard_meta,
                      int timeout_usecs) {
    SDF_status_t status;
    struct smt_request_state *request_state = NULL;
    struct sdf_msg *response_msg = NULL;
    struct replication_test_node *temp_node;
    struct sdf_msg_wrapper *wrapper = NULL;
    sdf_msg_wrapper_free_local_t local_free;
    struct timeval now; /* how to get logic time */
    struct SDF_shard_meta *sd_meta;
    uint32_t msg_len;
    int failed;


    plat_assert(test_framework);
    if (node >= test_framework->config.nnode) {
        failed = 1;
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "Cannot find node %d from framework", node);
    } else {
        temp_node = test_framework->nodes[node];
        if (temp_node) {
            response_msg = plat_calloc(1, sizeof(*response_msg)
                                       +sizeof(SDF_protocol_msg_t)+sizeof(struct SDF_shard_meta));
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "got a reponse_msg %p", response_msg);
            msg_len = sizeof(*response_msg) + sizeof(SDF_protocol_msg_t) +sizeof(struct SDF_shard_meta);
            response_msg->msg_len = msg_len;
            response_msg->msg_dest_vnode = node;
            response_msg->msg_src_vnode = node;
            response_msg->msg_src_service = SDF_RESPONSES;
            response_msg->msg_dest_service = SDF_REPLICATION_PEER_META_SUPER /* right? */;
            response_msg->msg_flags &= ~SDF_MSG_FLAG_MBX_RESP_INCLUDED;
            /* msg flags should have response expected when the src_service is SDF_RESPONSES */
            response_msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;

            SDF_protocol_msg_t *pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
            pm->msgtype = HFCSH;
            pm->n_replicas = shard_meta->replication_props.num_replicas;
            pm->shard = shard_meta->sguid;
            pm->data_offset = 0;
            pm->data_size = sizeof(struct SDF_shard_meta);
            /* also repliation_props.num_replicas */
            pm->op_meta.shard_meta.nreplica = test_framework->config.nnode;
            smt_replica_destribute(&test_framework->config, pm);
            sd_meta = (struct SDF_shard_meta *)((char *)pm+sizeof(*pm));
            memcpy(sd_meta, shard_meta, sizeof(struct SDF_shard_meta));

            /* add an entry in test_model, return op? */
            __sync_fetch_and_add(&test_framework->ltime, 1);
            now = test_framework->now;
            if (plat_calloc_struct(&request_state)) {
                request_state->mbx =
                    sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_create(test_framework->closure_scheduler,
                                                                               &smt_send_response,
                                                                               request_state),
                                                   /* XXX: release arg goes away */
                                                   SACK_REL_YES,
                                                   timeout_usecs /* user specified usecs */);
                /* assigned but never used with the exception of the log msg */
                if (!request_state->mbx) {
                    status = SDF_FAILURE_MEMORY_ALLOC;
                }
            } else {
                status = SDF_FAILURE_MEMORY_ALLOC;
            }
            /* Wrapper as a sdf_msg_wrapper */
            local_free =
                sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                  &smt_msg_free,
                                                  NULL);
            wrapper =
                sdf_msg_wrapper_local_alloc(response_msg, local_free,
                                            SMW_MUTABLE_FIRST, SMW_TYPE_REQUEST,
                                            node /* src */, SDF_RESPONSES,
                                            node /* dest */,
                                            SDF_REPLICATION_PEER_META_SUPER /* dest */,
                                            REPL_REQUEST,
                                            NULL /* not a response */);
            if (request_state && wrapper) {
                request_state->send_wrapper = wrapper;
            }
        }
    }
    plat_assert(request_state && request_state->mbx &&
                request_state->send_wrapper);
    return (request_state);
}

/**
 * @brief create shard meta
 */
static struct SDF_shard_meta *
smt_create_shard_meta(struct replication_test_framework *test_framework,
                      vnode_t first_node, SDF_shardid_t shard_id) {
    int failed;
    struct SDF_shard_meta *shard_meta = NULL;
    SDF_replication_props_t *replication_props = NULL;
    failed = !plat_calloc_struct(&replication_props);
    if (!failed) {
        rtfw_set_default_replication_props(&test_framework->config, replication_props);
        shard_meta = rtfw_init_shard_meta(&test_framework->config,
                                          first_node /* first_node */,
                                          shard_id
                                          /* shard_id, in real system generated by generate_shard_ids() */,
                                          replication_props);
    }
    plat_assert(shard_meta);
    plat_free(replication_props);
    return (shard_meta);
}


/**
 * @brief start two nodes, and send a wrapper to another for timeout testing
 */
void
smt_timeout_test(uint64_t args) {
    struct replication_test_framework *test_framework =
            (struct replication_test_framework *)args;
    struct SDF_shard_meta *shard_meta = NULL;
    SDF_replication_props_t *replication_props = NULL;
    struct smt_request_state *request_state = NULL;
    int failed = 0;
    SDF_shardid_t shard_id;
    vnode_t node_from;
    vnode_t node_to;
    
    shard_id = __sync_add_and_fetch(&test_framework->max_shard_id, 1);

    failed = !plat_calloc_struct(&meta);
    replication_test_meta_init(meta);

    /* Assure test_framework is started?! */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start test_framework");
    rtfw_start(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework started\n");

    node_from = 0;
    node_to = 1;
    shard_meta = smt_create_shard_meta(test_framework, node_from, shard_id);
    plat_assert(shard_meta);

    request_state = smt_create_shard_sync(test_framework, node_to,
                                          shard_meta, timeout_usecs);
    /* send wrapper from 0 */
    rtfw_send_msg(test_framework, node_from, request_state->send_wrapper,
                  request_state->mbx);

    /* block for a while and check flag */
    rtfw_sleep_usec(test_framework,
                    /* sleep current thread usecs greater than given */
                    timeout_usecs+1000);

    plat_assert(wrapper_response_received);


    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                 "\n************************************************************\n"
                 "                  Test framework shutdown                       "
                 "\n************************************************************");
    rtfw_shutdown_sync(test_framework);
    plat_free(test_framework);

    smt_request_state_destroy(request_state);
    plat_free(meta);
    plat_free(replication_props);
    plat_free(shard_meta);

    /* Terminate scheduler */
    fthKill(1);
}

int main(int argc, char **argv) {
    SDF_status_t status;
    struct replication_test_framework *test_framework = NULL;
    struct replication_test_config *config = NULL;
    int failed;

    struct plat_opts_config_msg_timeout opts_config;
    memset(&opts_config, 0, sizeof (opts_config));
    int opts_status = plat_opts_parse_msg_timeout(&opts_config, argc, argv);
    if (opts_status) {
        plat_opts_usage_msg_timeout();
        return (1);
    }
    if (!opts_config.timeout_usecs) {
        opts_config.timeout_usecs = DEFAUT_USECS;
    }

    timeout_usecs = opts_config.timeout_usecs;
    failed = !plat_calloc_struct(&config);

    struct plat_opts_config_replication_test_framework_sm *sm_config;
    plat_calloc_struct(&sm_config);
    plat_assert(sm_config);

    /* start shared memory */
    status = framework_sm_init(0, NULL, sm_config);

    /* start fthread library */
    fthInit();

    rt_config_init(config, 10 /* hard code here */);
    config->nnode = 2 /* allocate 2 nodes */;
    config->test_type = RT_TYPE_META_STORAGE;
    test_framework = replication_test_framework_alloc(config);
    if (test_framework) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework %p allocated\n",
                     test_framework);
    }
    XResume(fthSpawn(&smt_timeout_test, 40960), (uint64_t)test_framework);
    fthSchedulerPthread(0);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "JOIN");
    plat_free(config);
    framework_sm_destroy(sm_config);
    return (0);
}

#include "platform/opts_c.h"
