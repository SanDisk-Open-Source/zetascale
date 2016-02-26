/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   test_framework.c
 * Author: Zhenwei Lu
 *
 * Created on Nov 12, 2008, 13:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_framework.c 10829 2009-07-16 02:45:45Z lzwei $
 *
 */

#include "common/sdftypes.h"

#include "platform/assert.h"
#include "platform/prng.h"
#include "platform/time.h"
#include "platform/stdio.h"

#include "sdfmsg/sdf_msg_action.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_msg_types.h"

#include "protocol/protocol_common.h"
#include "protocol/replication/copy_replicator_internal.h"

#include "test_api.h"
#include "test_flash.h"
#include "test_config.h"
#include "test_common.h"
#include "test_meta.h"
#include "test_node.h"
#include "test_model.h"
#include "test_framework.h"

#include <sys/time.h>

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/framework");
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_TIME, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/time");
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_EVENT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/event");

struct replication_test_node;
#define REPLICATOR_META_STORAGE

#ifndef CMP_LESS
#define CMP_LESS <
#endif

#ifndef CMP_MORE
#define CMP_MORE >
#endif


#ifndef MULTI_NODE
#define MULTI_NODE
#endif

#undef SHARD_BASE
#define SHARD_BASE 100

/**
 * @brief item(type, test model complete lambda returning 0 on success
 */
#define MSG_ITEMS(op, now, status) \
    item(RTM_GO_CR_SHARD,  rtm_create_shard_complete(op, now, status))         \
    item(RTM_GO_DEL_SHARD, rtm_delete_shard_complete(op, now, status))         \
    item(RTM_GO_WR, rtm_write_complete(op, now, status))                       \
    item(RTM_GO_DEL, rtm_delete_complete(op, now, status))

enum {
    /* something large, assert if #rtfw_allocate_shard_id gets here */
    TEST_SHARD_META = 0x10000000,
    /* not used yet, but cguid of messages has to come from some where */
    TEST_CONTAINER_META = TEST_SHARD_META << 4
};

struct framework_request_state {
    /** @brief Associated test framework */
    struct replication_test_framework *test_framework;

    /** @brief response callback closure except read */
    replication_test_framework_cb_t response_closure;

    /** @brief response callback for read */
    replication_test_framework_read_async_cb_t read_aync_closure;

    /** @brief response callback for last seqno */
    rtfw_last_seqno_cb_t last_seqno_closure;

    /** @brief response callback for get cursors */
    rtfw_get_cursors_cb_t get_cursors_closure;

    /** @brief response callback for get cursors */
    rtfw_get_by_cursor_cb_t get_by_cursor_closure;

    /** @brief dynamically allocated mbx structure */
    struct sdf_fth_mbx *mbx;

    /** @brief general op */
    rtm_general_op_t *op;

    /** @brief Shard_meta */
    struct SDF_shard_meta *shard_meta;

    /** @brief Shard id for delete shard, whose shard_meta is NULL */
    SDF_shardid_t shard_id;

    /** @brief timeout usecs */
    int64_t timeout_usecs;
};

/**
 * @brief Shutdown state for asynchronous
 */
struct rtfw_shutdown_state {
    /** @brief Associated callback closure */
    replication_test_framework_shutdown_async_cb_t cb;

    /** @brief Completed node count */
    uint32_t comp_node;

    /** @brief Associated test framework */
    struct replication_test_framework *rtfw;
};

struct rtfw_crash_state {
    /** @brief Crash closure */
    replication_test_framework_cb_t cb;

    /** @brief Associated test framework */
    struct replication_test_framework *rtfw;

    /** @brief Crashed node id */
    vnode_t node;
};

struct rtfw_at_state {
    /** @brief Callback closure */
    replication_test_framework_cb_t cb;

    /** @brief Status for cb */
    SDF_status_t status;
};

struct rtfw_block_state {
    fthMbox_t mbox;

    char *event_name;
};

struct rtfw_sync_state {
    /** @brief Response mbox */
    fthMbox_t mbox;

    /** @brief Enumerate all the response closure fields here */
    SDF_status_t status;
};


struct rtfw_read_sync_state {
    /** @brief Response mbx */
    fthMbox_t mbox;

    /** @brief Enumerate all the response closure fields */
    SDF_status_t status;

    void *data;
    size_t data_len;
    replication_test_framework_read_data_free_cb_t free_cb;
};

struct rtfw_last_seqno_sync_state {
    /** @brief Response mbx */
    fthMbox_t mbox;

    /** @brief Enumerate all the response closure fields */
    SDF_status_t status;
    uint64_t     seqno;
};

struct rtfw_get_cursors_sync_state {
    /** @brief Response mbx */
    fthMbox_t mbox;

    /** @brief Enumerate all the response closure fields */
    SDF_status_t status;

    void *data;
    size_t data_len;
    replication_test_framework_read_data_free_cb_t free_cb;
};

struct rtfw_get_by_cursor_sync_state {
    /** @brief Response mbx */
    fthMbox_t mbox;

    /** @brief Enumerate all the response closure fields */
    SDF_status_t status;

    char              *key;
    int                key_len;
    int                max_key_len;
    SDF_time_t         exptime;
    SDF_time_t         createtime;
    uint64_t           seqno;
    void              *data;
    size_t             data_len;
    replication_test_framework_read_data_free_cb_t free_cb;
};

// item(RTM_GO_RD, rtm_read_complete(op, now, status))

static void
rtfw_gettime(void *env, struct timeval *ret);

static void
rtfw_gettime_cb(plat_closure_scheduler_t *context, void *env,
                struct timeval *ret);

static SDF_status_t
rtfw_add_shard(struct replication_test_framework *framework,
               const struct SDF_shard_meta *shard_meta);

static SDF_status_t
rtfw_del_shard(struct replication_test_framework *test_framework,
               const SDF_shardid_t shard_id);

static void
rtfw_msg_free(plat_closure_scheduler_t *context, void *env,
              struct sdf_msg *msg);

static void
rtfw_response(struct plat_closure_scheduler *context, void *env,
              struct sdf_msg_wrapper *response);

static void
rtfw_shutdown_response(struct plat_closure_scheduler *context, void *env);

static void
rtfw_read_response(struct plat_closure_scheduler *context, void *env,
                   struct sdf_msg_wrapper *response);

static void
rtfw_last_seqno_response(struct plat_closure_scheduler *context, void *env,
                         struct sdf_msg_wrapper *response);

static void
rtfw_get_cursors_response(struct plat_closure_scheduler *context, void *env,
                          struct sdf_msg_wrapper *response);

static void
rtfw_get_by_cursor_response(struct plat_closure_scheduler *context, void *env,
                            struct sdf_msg_wrapper *response);

static void
rtfw_sync_cb(plat_closure_scheduler_t *context, void *env,
             SDF_status_t status);

static void
rtfw_read_sync_cb(plat_closure_scheduler_t *context, void *env,
                  SDF_status_t status, const void * data, size_t data_len,
                  replication_test_framework_read_data_free_cb_t free_cb);

static void
rtfw_last_seqno_sync_cb(plat_closure_scheduler_t *context, void *env,
                        uint64_t seqno, SDF_status_t status);

static void
rtfw_get_cursors_sync_cb(plat_closure_scheduler_t *context, void *env,
                         SDF_status_t status, const void * data, size_t data_len,
                         replication_test_framework_read_data_free_cb_t free_cb);

static void
rtfw_get_by_cursor_sync_cb(plat_closure_scheduler_t *context, void *env,
                           SDF_status_t status,
                           const void * data, size_t data_len,
                           char *key, int key_len, SDF_time_t exptime,
                           SDF_time_t createtime, uint64_t seqno,
                           replication_test_framework_read_data_free_cb_t free_cb);

static void
rtfw_req_state_free(struct framework_request_state *request_state);

static SDF_status_t rtfw_op_complete(struct replication_test_framework *test_framework,
                                     SDF_status_t status, rtm_general_op_t *op);

static SDF_status_t
rtfw_get_container_meta(struct replication_test_framework *test_framework,
                        SDF_shardid_t shard,
                        struct SDF_container_meta **cmeta);

/**
 * @brief idle thread to send settime to trigger msg send
 */
static void
rtfw_idle_thread(uint64_t);

static void
rtfw_at_timer_fired(plat_closure_scheduler_t *context, void *env,
                    struct plat_event *event);

/** @brief Timer fired (all common code) */
static void rtfw_block_timer_fired(plat_closure_scheduler_t *context, void *env,
                                   struct plat_event *event);
static void rtfw_block_event_free(plat_closure_scheduler_t *context,
                                  void *env);

static void
rtfw_shutdown_sync_cb(struct plat_closure_scheduler *context, void *env);

static void
rtfw_closure_scheduler_shutdown(plat_closure_scheduler_t *context, void *env);

static void
rtfw_crash_node_async_cb(struct plat_closure_scheduler *context,
                         void *env, SDF_status_t status);
static void rtfw_post_mbox_cb(plat_closure_scheduler_t *context, void *env);

/**
 * @brief Create replication test framework
 *
 * @param replicator_alloc <IN> Allocate the replicator callback
 * @param replicator_alloc_extra <IN> Extra parameter.
 */
struct replication_test_framework *
replication_test_framework_alloc(const struct replication_test_config *config) {
    struct replication_test_framework *ret = NULL;
    int failed;
    int i;
    plat_timer_dispatcher_gettime_t gettime;
    struct replication_test_node *temp_node = NULL;
    failed = !plat_calloc_struct(&ret);
    if (!failed) {
        ret->config = *config;
        ret->config.replicator_config.my_node = SDF_ILLEGAL_PNODE;
        ret->config.replicator_config.node_count = config->nnode;

        replication_test_api_t *api = NULL;

        plat_calloc_struct(&api);
        plat_assert(api);

        ret->api = api;
        /* replicator api */

        /* start timer dispatcher */
        gettime =
            plat_timer_dispatcher_gettime_create(PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS,
                                                 &rtfw_gettime_cb,
                                                 ret);
        ret->timer_dispatcher = plat_timer_dispatcher_alloc(gettime);

        api->gettime = gettime;
        api->timer_dispatcher = ret->timer_dispatcher;
        api->send_msg =
            sdf_replicator_send_msg_cb_create(PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS,
                                              NULL, ret);
        api->prng = plat_prng_alloc(ret->config.prng_seed);
        plat_prng_next_int(api->prng, 100);

        ret->ltime = 0;
        ret->closure_scheduler = plat_mbox_scheduler_alloc();
        ret->closure_scheduler_thread =
            fthSpawn(&plat_mbox_scheduler_main, 40960);

        fthLockInit(&ret->rtfw_lock);
        ret->max_shard_id = 0;
#if 0
        ret->prng = plat_prng_alloc(ret->config.prng_seed);
        plat_prng_next_int(ret->prng, 100);
#endif
        /* initialize test_model */
        ret->model = replication_test_model_alloc(config);
        if (!(ret->model)) {
            failed = 1;
        }

        if (!failed) {
            /* set container_meta null */
            for (i = 0; i < CONTAINER_META_MAX; i++) {
                ret->cmeta[i] = NULL;
            }
        }

        /* Allocate test node */
        if (!failed) {
            /* Append a node at the header of test_framework */
            ret->nodes = plat_alloc(config->nnode *
                                    sizeof(struct replication_test_node *));
            for (i = 0; i < config->nnode; i++) {
                temp_node = replication_test_node_alloc(&ret->config, ret, api, i);
                if (temp_node) {
                    ret->nodes[i] = temp_node;
                } else {
                    failed = 1;
                    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                                 "alloc test_node failed!");
                }
            }
            ret->active_nodes = 0;
            if (!(ret->nodes)) {
                failed = 1;
            }
        }
#ifdef DEBUG
        ret->n_wrapper = 0;
#endif
        /* set shutdown phase */
        ret->final_shutdown_phase = 0;
    }

    if (!failed) {
        if (!ret->config.log_real_time) {
            plat_log_set_gettime(&rtfw_gettime, ret, &ret->old_log_time_fn,
                                 &ret->old_log_time_extra);
        }

        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "alloc framework %p successfully type %s", ret,
                     rt_type_to_string(ret->config.test_type));
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR, "alloc framework failed");
        if (ret) {
            plat_free(ret);
            ret = NULL;
        }
    }
    plat_assert(ret);
    return (ret);
}

/**
 * @brief Start test framework
 */
void
rtfw_start(struct replication_test_framework *test_framework) {
    SDF_status_t status;

    plat_assert(test_framework);

    status = rtfw_start_all_nodes(test_framework);
    plat_assert_always(status == SDF_SUCCESS);

    fthResume(test_framework->closure_scheduler_thread, (uint64_t)test_framework->closure_scheduler);
    fthResume(fthSpawn(&rtfw_idle_thread, 40960), (uint64_t)test_framework);
}

/**
 * @brief Shutdown and free asynchronously
 * @param cb <IN> closure invoked on completion
 */
void
rtfw_shutdown_async(struct replication_test_framework *test_framework,
                    replication_test_framework_shutdown_async_cb_t cb) {
    /* shutdown all test nodes */
    replication_test_node_shutdown_async_cb_t node_cb;
    struct rtfw_shutdown_state *shutdown_state;
    struct replication_test_node *node;
    int i;

    plat_alloc_struct(&shutdown_state);
    plat_assert(shutdown_state != NULL);
    shutdown_state->comp_node = 0;
    shutdown_state->rtfw = test_framework;
    shutdown_state->cb = cb;

    /* fixme: zhenwei, closure_scheduler can't receive this shutdown_response closure */
    node_cb =
        replication_test_node_shutdown_async_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                       /* test_framework->closure_scheduler, */
                                                       &rtfw_shutdown_response, shutdown_state);

    for (i = 0; i < test_framework->config.nnode; i++) {
        node = test_framework->nodes[i];
        plat_assert(node != NULL);
        rtn_shutdown_async(node, node_cb);
    }
}

/**
 * @brief Synchronous shutdown and free
 *
 * This is a thin wrapper arround #rtfw_shutdown_async which uses
 * #fthMbox_t to allow synchronous use.
 */
void
rtfw_shutdown_sync(struct replication_test_framework *test_framework) {
    fthMbox_t *mbox;
    plat_alloc_struct(&mbox);
    plat_assert(mbox != NULL);

    replication_test_framework_shutdown_async_cb_t cb;

    fthMboxInit(mbox);
    cb =
        replication_test_framework_shutdown_async_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                            &rtfw_shutdown_sync_cb,
                                                            mbox);
    rtfw_shutdown_async(test_framework, cb);
    fthMboxWait(mbox);
    plat_free(mbox);
}

/* Generic test hooks */

unsigned
rtfw_set_timeout_usecs(struct replication_test_framework *test_framework,
                       int64_t timeout_usecs) {
    unsigned old;

    do {
        old = test_framework->timeout_usecs;
    } while (!__sync_bool_compare_and_swap(&test_framework->timeout_usecs,
                                           old, timeout_usecs));

    return (old);
}

/**
 * @brief Return whether the test has failed
 *
 * This function aggregates the model (#rtm_get_failed), test result,
 * and other failures.
 */
int
rtfw_get_failed(struct replication_test_framework *test_framework) {
    return (rtm_failed(test_framework->model));
}

/**
 * @brief Set failure
 */
void
rtfw_set_failed(struct replication_test_framework *test_framework,
                int status) {

}

/**
 * @brief Get time from simulated timebase.
 */
void
rtfw_get_time(struct replication_test_framework *test_framework,
              struct timeval *tv) {
    plat_assert(tv);
    *tv = test_framework->now;
}

/**
 * @brief Apply given closure at scheduled simulated time
 *
 * @param when <IN> Time to block until
 * @param cb <IN> Closure to apply at given time
 */
void
rtfw_at_async(struct replication_test_framework *test_framework,
              const struct timeval *when, replication_test_framework_cb_t cb) {
    struct plat_event *event;
    plat_event_fired_t fired;
    struct rtfw_at_state *state;
    plat_alloc_struct(&state);
    plat_assert(state != NULL);
    state->cb = cb;
    /* Fixme: where is state.status from */
    fired = plat_event_fired_create(test_framework->closure_scheduler,
                                    &rtfw_at_timer_fired, state);
    event =
        plat_timer_dispatcher_timer_alloc(test_framework->timer_dispatcher, "test",
                                          LOG_CAT_EVENT, fired,
                                          1 /* free_count */, when,
                                          PLAT_TIMER_ABSOLUTE, NULL /* rank_ptr */);
    plat_assert(event);
}

/**
 * @brief Block calling #fthThread until we reach when
 * It'better to implemented via event, the background rtwf_idle_thread()
 * will dispatch it
 */
void
rtfw_block_until(struct replication_test_framework *test_framework,
                 const struct timeval when) {
    plat_event_fired_t fired;
    int status;
    struct plat_event *event;
    struct rtfw_block_state *state;

    status = timercmp(&test_framework->now, &when, CMP_LESS);
    plat_assert_always(status);

    plat_alloc_struct(&state);
    plat_assert(state != NULL);
    fthMboxInit(&state->mbox);
    status = plat_asprintf(&state->event_name, "rtfw_block_until %s",
                           plat_log_timeval_to_string(&when));
    plat_assert(status != -1);

    fired = plat_event_fired_create(test_framework->closure_scheduler,
                                    &rtfw_block_timer_fired, state);
    event =
        plat_timer_dispatcher_timer_alloc(test_framework->timer_dispatcher,
                                          state->event_name,
                                          LOG_CAT_EVENT, fired,
                                          1 /* free_count */, &when,
                                          PLAT_TIMER_ABSOLUTE,
                                          NULL /* rank_ptr */);
    plat_assert(event);
    fthMboxWait(&state->mbox);
    plat_free(state);
}

/**
 * @brief Sleep for given number of simulated microseconds.
 * @param usec <IN> Relative time from now
 */
void
rtfw_sleep_usec(struct replication_test_framework *test_framework,
                unsigned usec) {
    struct timeval when;
    struct timeval incre;
    incre.tv_sec = usec / 1000000;
    incre.tv_usec = usec % 1000000;

    timeradd(&test_framework->now, &incre, &when);
    rtfw_block_until(test_framework, (const struct timeval)when);
}

/* Node operations */

/**
 * @brief Crash node asynchronously
 *
 * Node crashes are simulated by discarding everything in the inbound
 * and outbound message queues, returning timeout errors for all existing
 * message requests from the node and new requests, and shutting the node
 * down.
 *
 * @param test_framework <IN> A running test framework.
 * @param node <IN> Vnode to crash
 * @param cb <IN> Callback applied when the simulated crash is complete
 * and the node can be restarted.  The callback may return other than
 * SDF_SUCCESS when the node is in the process of crashing.
 */
void
rtfw_crash_node_async(struct replication_test_framework *test_framework,
                      vnode_t node, replication_test_framework_cb_t cb) {
    struct replication_test_node *test_node;
    struct rtfw_crash_state *crash_state;
    plat_alloc_struct(&crash_state);
    plat_assert(crash_state != NULL);
    replication_test_node_crash_async_cb_t crash_cb;
    int i;

    plat_assert(node < test_framework->config.nnode);
    test_node = test_framework->nodes[node];
    plat_assert(test_node);

    if (!test_node) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "Cannot find node:%d", node);
        plat_closure_apply(replication_test_framework_cb, &cb, SDF_FAILURE);
        plat_free(crash_state);
    } else {
        crash_state->cb = cb;
        crash_state->node = node;
        crash_state->rtfw = test_framework;
        crash_cb =
            replication_test_node_crash_async_cb_create(test_framework->closure_scheduler,
                                                        &rtfw_crash_node_async_cb, crash_state);
        rtn_crash_async(test_node, crash_cb);

        /*
         * XXX: drew 2010-04-06 Add a clean-shutdown mode that
         * does the pass-through even in the new liveness mode
         * as in the close-of-socket case.
         */
        if (!test_framework->config.new_liveness) {
            /* Simulate liveness subsystem */
            for (i = 0; i < test_framework->config.nnode; i++) {
                plat_assert(test_framework->nodes[i]);
                rtn_node_dead(test_framework->nodes[i], node, 0 /* epoch */);
            }
        }
    }
}

/**
 * @brief Crash node synchronously from fthThread.
 *
 * This is a thin wrapper around #rtfw_crash_node_async which uses a
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_framework <IN> A running test framework.
 * @param node <IN> Vnode to crash
 * @return SDF_SUCCESS on success, otherwise on failure such as when the
 * node was already in the process of crashing.
 */
SDF_status_t
rtfw_crash_node_sync(struct replication_test_framework *test_framework,
                     vnode_t node) {
    struct rtfw_sync_state *state;
    SDF_status_t rt;
    plat_alloc_struct(&state);
    plat_assert(state != NULL);
    replication_test_framework_cb_t cb;

    fthMboxInit(&state->mbox);
    cb = replication_test_framework_cb_create(test_framework->closure_scheduler,
                                              &rtfw_sync_cb,
                                              state);
    rtfw_crash_node_async(test_framework, node, cb);
    fthMboxWait(&state->mbox);
    rt = state->status;
    plat_free(state);
    return (rt);
}

/**
 * @brief Start node
 *
 * Start the given simulated node. By default, all of the nodes in the
 * simulated cluster are in the unstarted state.
 *
 * XXX: drew 2010-04-06 This needs to be asynchronous like rtn_start_node
 * which it calls.
 */
SDF_status_t
rtfw_start_node(struct replication_test_framework *test_framework,
                vnode_t node) {
    SDF_status_t ret = SDF_FAILURE;
    struct replication_test_node *temp_node = NULL;
    int i;
    struct rtfw_sync_state *state;
    replication_test_framework_cb_t cb;

    plat_assert(node < test_framework->config.nnode);
    temp_node = test_framework->nodes[node];
    plat_assert(temp_node != NULL);

    plat_calloc_struct(&state);
    cb = replication_test_framework_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                              &rtfw_sync_cb,
                                              state);

    if (!state) {
        ret = SDF_FAILURE_MEMORY_ALLOC;
    } else if (temp_node) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "get node %u from nodelist of test_framework", node);

        fthMboxInit(&state->mbox);
        rtn_start(temp_node, cb);
        fthMboxWait(&state->mbox);
        ret = state->status;

        if (ret == SDF_SUCCESS && !test_framework->config.new_liveness) {
            (void) __sync_fetch_and_add(&test_framework->active_nodes, 1);

            /* Simulate liveness subsystem */
            for (i = 0; i < test_framework->config.nnode; i++) {
                plat_assert(test_framework->nodes[i]);
                rtn_node_live(test_framework->nodes[i], node,
                              0 /* epoch */);
                if (i != node && test_framework->nodes[i] &&
                    rtn_node_is_live(test_framework->nodes[i])) {
                    rtn_node_live(temp_node, i, 0 /* epoch */);
                }
            }
        }
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "get node %u error", node);
    }

    plat_free(state);

    return (ret);
}

/**
 * @brief Start all nodes
 */
SDF_status_t
rtfw_start_all_nodes(struct replication_test_framework *test_framework) {
    SDF_status_t ret = SDF_SUCCESS;
    SDF_status_t ret_temp = SDF_SUCCESS;
    int i;

    for (i = 0; i < test_framework->config.nnode; i++) {
        plat_assert(test_framework->nodes[i] != NULL);
        ret_temp = rtfw_start_node(test_framework, i);
        if (ret_temp == SDF_SUCCESS) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "start node %d successfully", i);
        } else {
            ret = ret_temp;
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "start node %d failed", i);
        }
    }
    return (ret);
}

/* Functions for RT_TYPE_REPLICATOR */
void
rtfw_set_replicator_notification_cb(struct replication_test_framework *test_framework,
                                    rtfw_replicator_notification_cb_t cb) {
    int i;
    struct replication_test_node *test_node;

    for (i = 0; i < test_framework->config.nnode; i++) {
        test_node = test_framework->nodes[i];
        plat_assert(test_node);
        rtn_set_replicator_notification_cb(test_node, cb);
    }
}

void
rtfw_command_async(struct replication_test_framework *test_framework,
                   vnode_t node, SDF_shardid_t shard, const char *command_arg,
                   sdf_replicator_command_cb_t cb) {
    SDF_status_t status;
    struct replication_test_node *temp_node;
    char *output;

    if (node >= test_framework->config.nnode) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "Cannot find node %d from framework", node);
        status = SDF_NODE_INVALID;
    } else if (!(temp_node = test_framework->nodes[node])) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_WARN,
                     "node:%d is null", (int)node);
        status = SDF_NODE_INVALID;
    } else {
        rtn_command_async(temp_node, shard, command_arg, cb);
        status = SDF_SUCCESS;
    }

    if (status != SDF_SUCCESS) {
        plat_asprintf(&output, "SERVER_ERROR %s\r\n",
                      sdf_status_to_string(status));
        plat_closure_apply(sdf_replicator_command_cb, &cb, status, output);
    }
}

static void rtfw_command_cb(plat_closure_scheduler_t *context, void *env,
                            SDF_status_t status, char *output);

struct rtfw_command_state {
    fthMbox_t mbox;
    SDF_status_t status;
    char *output;
};

SDF_status_t
rtfw_command_sync(struct replication_test_framework *test_framework,
                  vnode_t node, SDF_shardid_t shard, const char *command_arg,
                  char **output) {
    struct rtfw_command_state *state;
    SDF_status_t ret;
    sdf_replicator_command_cb_t cb;

    if (!plat_calloc_struct(&state)) {
        ret = SDF_FAILURE_MEMORY_ALLOC;
    } else {
        fthMboxInit(&state->mbox);
        cb = sdf_replicator_command_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                              &rtfw_command_cb, state);
        rtfw_command_async(test_framework, node, shard, command_arg, cb);
        fthMboxWait(&state->mbox);
        if (output) {
            *output = state->output;
        } else if (state->output) {
            plat_free(state->output);
        }
        ret = state->status;
        plat_free(state);
    }

    return (ret);
}

static void
rtfw_command_cb(plat_closure_scheduler_t *context, void *env,
                SDF_status_t status, char *output) {
    struct rtfw_command_state *state = (struct rtfw_command_state *)env;

    state->status = status;
    state->output = output;
    fthMboxPost(&state->mbox, 0);
}

/**
 * @brief Asynchronous shard create operation
 *
 * This is a thin wrapper which invokes the model's #rtm_create_shard function,
 * sends sends a HFCSH message to the replication service on the given node,
 * and applies the closure on completion.
 *
 * @param test_framework <IN> Test framework
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard_meta <IN> shard to create, caller owns buffer
 * @param cb <IN> closure invoked on completion
 */
void
rtfw_create_shard_async(struct replication_test_framework *test_framework,
                        vnode_t node, struct SDF_shard_meta *shard_meta,
                        replication_test_framework_cb_t cb) {
    SDF_status_t status;
    int failed = 0;
    sdf_fth_mbx_t *ar_mbx_from_req = NULL;
    struct sdf_msg *response_msg = NULL;
    struct replication_test_node *temp_node;
    struct sdf_msg_wrapper *wrapper = NULL;
    sdf_msg_wrapper_free_local_t local_free;
    rtm_general_op_t *op;
    struct timeval now; /* how to get logic time */
    struct framework_request_state *request_state;
    struct SDF_container_meta *cmeta;
    struct SDF_shard_meta *sd_meta;
    uint32_t msg_len;

    if (node >= test_framework->config.nnode) {
        failed = 1;
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "Cannot find node %d from framework", node);
    } else {
        temp_node = test_framework->nodes[node];
        plat_assert(temp_node);
        status = rtfw_get_container_meta(test_framework, shard_meta->sguid, &cmeta);
        if (temp_node && SDF_SUCCESS != status) {
            response_msg = plat_calloc(1, sizeof(*response_msg)
                                       +sizeof(SDF_protocol_msg_t)+sizeof(struct SDF_shard_meta));
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "got a reponse_msg %p", response_msg);
            msg_len = sizeof(*response_msg) + sizeof(SDF_protocol_msg_t) +sizeof(struct SDF_shard_meta);
            response_msg->msg_len = msg_len;
            response_msg->msg_type = REPL_REQUEST;
            response_msg->msg_dest_vnode = node;
            response_msg->msg_src_vnode = node;
            response_msg->msg_src_service = SDF_RESPONSES;
            response_msg->msg_dest_service = SDF_REPLICATION;
            response_msg->msg_flags &= ~SDF_MSG_FLAG_MBX_RESP_INCLUDED;
            /* msg flags should have response expected when the src_service is SDF_RESPONSES */
            response_msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;

            SDF_protocol_msg_t *pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
            pm->msgtype = HFCSH;
            pm->shard = shard_meta->sguid;
            pm->data_offset = 0;
            pm->data_size = sizeof(struct SDF_shard_meta);
            /* FIXME: copy_replicator checks replication_type here, should be NULL */
            pm->op_meta.shard_meta.type = 0;

            /* also repliation_props.num_replicas */
            /*
             * FIXME: drew 2009-05-19 This needs to come from some place else,
             * because we want to test 2 replicas with 3 meta-data replicas
             * where 2n+1 meta-data nodes and n+1 data replicas are needed
             * to survive n failures.
             */
            sd_meta = (struct SDF_shard_meta *)((char *)pm+sizeof(*pm));
            memcpy(sd_meta, shard_meta, sizeof(struct SDF_shard_meta));

            /* add an entry in test_model, return op? */
            __sync_fetch_and_add(&test_framework->ltime, 1);
            now = test_framework->now;
            op = rtm_start_create_shard(test_framework->model, now,
                                        node, shard_meta);
            plat_assert_always(op);
            if (plat_calloc_struct(&request_state)) {
                request_state->response_closure = cb;
                request_state->test_framework = test_framework;
                request_state->op = op;
                request_state->shard_meta =
                plat_calloc(1, sizeof(struct SDF_shard_meta));
                memcpy(request_state->shard_meta,
                       shard_meta, sizeof(struct SDF_shard_meta));
#ifdef NETWORK_DELAY
                request_state->timeout_usecs = rt_set_async_timeout(test_framework->config);
#else
                request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_NONE;
#endif

                request_state->mbx =
                    sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_create(test_framework->closure_scheduler,
                                                                               &rtfw_response,
                                                                               request_state),
                                                   /* XXX: release arg goes away */
                                                   SACK_REL_YES,
                                                   request_state->timeout_usecs);
                /* assigned but never used with the exception of the log msg */
#ifndef RTFW_OP_MAP
                ar_mbx_from_req = request_state->mbx;
#endif
                plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "mbx:%p",
                             ar_mbx_from_req);
                if (!request_state->mbx) {
                    status = SDF_FAILURE_MEMORY_ALLOC;
                }
            } else {
                status = SDF_FAILURE_MEMORY_ALLOC;
            }
            /* Wrapper as a sdf_msg_wrapper */
            local_free =
                sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                  &rtfw_msg_free,
                                                  NULL);
            wrapper = sdf_msg_wrapper_local_alloc(response_msg, local_free,
                                                  SMW_MUTABLE_FIRST,
                                                  SMW_TYPE_REQUEST,
                                                  node /* src */,
                                                  SDF_RESPONSES,
                                                  node /* dest */,
                                                  SDF_REPLICATION /* dest */,
                                                  REPL_REQUEST,
                                                  NULL /* not a response */);
            /* send to dest node */
            rtn_send_msg(temp_node, wrapper, request_state->mbx);
            failed = 0;
        } else {
            if (!temp_node) {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_WARN,
                             "node:%d is null", (int)node);
            } else {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_WARN,
                             "shard %d already exists", (int)shard_meta->sguid);
            }
            failed = 1;
        }
    }
    if (failed) {
        /* set a response to sync_functions */
        plat_closure_apply(replication_test_framework_cb,
                           &cb, SDF_FAILURE);
    }
}

/**
 * @brief Synchronous shard create operation from fthThread
 *
 * This is a thin wrapper arround #rtfw_create_shard_async which uses a
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_framework <IN> Test framework
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard_meta <IN> shard to create, caller owns buffer
 * @return SDF status
 */
SDF_status_t
rtfw_create_shard_sync(struct replication_test_framework *framework,
                       vnode_t node, struct SDF_shard_meta *shard_meta) {
    SDF_status_t ret;
    struct rtfw_sync_state *state;
    plat_alloc_struct(&state);
    plat_assert(state != NULL);
    replication_test_framework_cb_t cb;

    fthMboxInit(&state->mbox);
    cb = replication_test_framework_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                              &rtfw_sync_cb,
                                              state);
    rtfw_create_shard_async(framework, node, shard_meta, cb);

    fthMboxWait(&state->mbox);
    ret = state->status;
    plat_free(state);
    return (ret);
}

/**
 * @brief Get last sequence number asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param sguid <IN> data shard GUID
 *
 * @param cb <IN> applied on completion.
 */
void rtfw_get_last_seqno_async(struct replication_test_framework *test_framework,
                               vnode_t node, SDF_shardid_t shard,
                               rtfw_last_seqno_cb_t cb)
{
    SDF_status_t status = SDF_FALSE;
    int failed = 0;
    rtm_general_op_t *op;
    sdf_fth_mbx_t *ar_mbx_from_req = NULL;
    SDF_protocol_msg_t *pm = NULL;
    struct sdf_msg *response_msg = NULL;
    struct sdf_msg_wrapper *wrapper = NULL;
    struct replication_test_node *temp_node;
    sdf_msg_wrapper_free_local_t local_free;
    struct timeval now;
    uint32_t msg_len;
    struct SDF_container_meta *cmeta;
    struct framework_request_state *request_state;

    if (node >= test_framework->config.nnode) {
        failed  = 1;
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "illegal or crashed node:%d", node);
    } else {
        temp_node = test_framework->nodes[node];
        if (temp_node) {
            response_msg = plat_calloc(1, sizeof(*response_msg) +
                                       sizeof(SDF_protocol_msg_t));
            msg_len = sizeof(*response_msg) + sizeof(SDF_protocol_msg_t);

            response_msg->msg_len = msg_len;
            response_msg->msg_dest_service = SDF_REPLICATION;
            response_msg->msg_src_service = SDF_RESPONSES;
            response_msg->msg_flags &= ~SDF_MSG_FLAG_MBX_RESP_INCLUDED;
            response_msg->msg_dest_vnode = node;
            response_msg->msg_src_vnode = node;

            pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
            pm->msgtype = HFGLS;
            pm->shard = shard;
            if (SDF_SUCCESS == rtfw_get_container_meta(test_framework, shard, &cmeta)) {
                /* get node replicator */
                if (SDF_SUCCESS != rtn_get_op_meta(temp_node, cmeta, shard, &pm->op_meta)) {
                    plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                                 "get op_meta failed");
                    failed = 1;
                } else {
                    /* send a start message to framework */
                    now = test_framework->now;
                    op = rtm_start_last_seqno(test_framework->model, now,
                                              node, shard);

                    plat_assert(op);
                    if (plat_calloc_struct(&request_state)) {
                        request_state->last_seqno_closure = cb;
                        request_state->test_framework = test_framework;
                        request_state->op = op;
                        request_state->shard_meta = NULL;
#ifdef NETWORK_DELAY
                        request_state->timeout_usecs = rt_set_async_timeout(test_framework->config);
#else
                        request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_NONE;
#endif

                        request_state->mbx =
                        sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_create(test_framework->closure_scheduler,
                                                                                   &rtfw_last_seqno_response,
                                                                                   request_state),
                                                       /* XXX: release arg goes away */
                                                       SACK_REL_YES,
                                                       request_state->timeout_usecs);

#ifndef RTFW_OP_MAP
                        ar_mbx_from_req = request_state->mbx;
#endif
                        if (!request_state->mbx) {
                            status = SDF_FAILURE_MEMORY_ALLOC;
                        }
                    } else {
                        status = SDF_FAILURE_MEMORY_ALLOC;
                    }
                    /* Wrapper as a sdf_msg_wrapper */
                    local_free = sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                                   &rtfw_msg_free, NULL);
                    wrapper =
                        sdf_msg_wrapper_local_alloc(response_msg, local_free,
                                                    SMW_MUTABLE_FIRST,
                                                    SMW_TYPE_REQUEST,
                                                    node /* src */,
                                                    SDF_RESPONSES,
                                                    node /* dest */,
                                                    SDF_FLSH /* dest */,
                                                    FLSH_REQUEST, NULL /* not a response */);
                    /* send to dest node */
                    rtn_send_msg(temp_node, wrapper, request_state->mbx);
                }
            } else {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                             "get node:%d shard_id:%d container_meta failed ",
                             (int)node, (int)shard);
                failed = 1;
            }
        }
    }
    if (failed) {
        if (response_msg != NULL) {
            sdf_msg_free_buff(response_msg);
        }
        plat_closure_apply(rtfw_last_seqno_cb,
                           &cb, SDF_SEQUENCE_NO_INVALID, SDF_FAILURE);
    }
}

/**
 * @brief Get last sequence number synchronously
 *
 * This is a thin wrapper around #rtfw_get_get_last_seqno_async
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param sguid <IN> data shard GUID
 *
 * @param pseqno <OUT> The sequence number retrieved is stored here.
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t rtfw_get_last_seqno_sync(struct replication_test_framework *framework,
                                      vnode_t node, SDF_shardid_t sguid,
                                      uint64_t *pseqno)
{
    SDF_status_t ret;
    struct rtfw_last_seqno_sync_state *state;
    plat_alloc_struct(&state);
    plat_assert(state);
    rtfw_last_seqno_cb_t cb;

    fthMboxInit(&state->mbox);
    cb = rtfw_last_seqno_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                   &rtfw_last_seqno_sync_cb,
                                   state);
    rtfw_get_last_seqno_async(framework, node, sguid, cb);

    fthMboxWait(&state->mbox);
    if (state->status == SDF_SUCCESS) {
        *pseqno = state->seqno;
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                     "get last sequence number failed");
        ret = SDF_FAILURE;
    }
    ret = state->status;
    plat_free(state);
    return (ret);
}

/**
 * @brief Get iteration cursors asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param sguid <IN> data shard GUID
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param seqno_start <IN> start of sequence number range, inclusive
 *
 * @param seqno_len   <IN> max number of cursors to return at a time
 *
 * @param seqno_max   <IN> end of sequence number range, inclusive
 *
 * @param cb <IN> applied on completion.
 */
void rtfw_get_cursors_async(struct replication_test_framework *test_framework,
                            SDF_shardid_t shard, vnode_t node,
                            uint64_t seqno_start, uint64_t seqno_len, uint64_t seqno_max,
                            void *cursor, int cursor_size,
                            rtfw_get_cursors_cb_t cb)
{
    SDF_status_t status = SDF_FALSE;
    int failed = 0;
    rtm_general_op_t *op;
    sdf_fth_mbx_t *ar_mbx_from_req = NULL;
    SDF_protocol_msg_t *pm = NULL;
    struct sdf_msg *response_msg = NULL;
    struct sdf_msg_wrapper *wrapper = NULL;
    struct replication_test_node *temp_node;
    sdf_msg_wrapper_free_local_t local_free;
    struct timeval now;
    uint32_t msg_len;
    struct SDF_container_meta *cmeta;
    struct framework_request_state *request_state;

    if (node >= test_framework->config.nnode) {
        failed  = 1;
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "illegal or crashed node:%d", node);
    } else {
        temp_node = test_framework->nodes[node];
        if (temp_node) {
            msg_len = sizeof(*response_msg) + sizeof(SDF_protocol_msg_t) + cursor_size;
            response_msg = plat_calloc(1, msg_len);

            response_msg->msg_len = msg_len;
            response_msg->msg_dest_service = SDF_FLSH;
            response_msg->msg_src_service = SDF_RESPONSES;
            response_msg->msg_flags &= ~SDF_MSG_FLAG_MBX_RESP_INCLUDED;
            response_msg->msg_dest_vnode = node;
            response_msg->msg_src_vnode = node;

            pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
            pm->msgtype   = HFGIC;
            pm->shard     = shard;
            pm->seqno     = seqno_start;
            pm->seqno_len = seqno_len;
            pm->seqno_max = seqno_max;
            pm->data_size = cursor_size;
            memcpy((char *)pm + sizeof(SDF_protocol_msg_t), cursor, cursor_size);

            if (SDF_SUCCESS == rtfw_get_container_meta(test_framework, shard, &cmeta)) {
                /* get node replicator */
                if (SDF_SUCCESS != rtn_get_op_meta(temp_node, cmeta, shard, &pm->op_meta)) {
                    plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                                 "get op_meta failed");
                    failed = 1;
                } else {
                    /* send a start message to framework */
                    now = test_framework->now;
                    op = rtm_start_get_cursors(test_framework->model, shard,
                                               now, node,
                                               seqno_start, seqno_len,
                                               seqno_max);
                    plat_assert(op);
                    if (plat_calloc_struct(&request_state)) {
                        request_state->get_cursors_closure = cb;
                        request_state->test_framework = test_framework;
                        request_state->op = op;
                        request_state->shard_meta = NULL;
#ifdef NETWORK_DELAY
                        request_state->timeout_usecs = rt_set_async_timeout(test_framework->config);
#else
                        request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_NONE;
#endif

                        request_state->mbx =
                        sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_create(test_framework->closure_scheduler,
                                                                                   &rtfw_get_cursors_response,
                                                                                   request_state),
                                                       /* XXX: release arg goes away */
                                                       SACK_REL_YES,
                                                       request_state->timeout_usecs);

#ifndef RTFW_OP_MAP
                        ar_mbx_from_req = request_state->mbx;
#endif
                        if (!request_state->mbx) {
                            status = SDF_FAILURE_MEMORY_ALLOC;
                        }
                    } else {
                        status = SDF_FAILURE_MEMORY_ALLOC;
                    }
                    /* Wrapper as a sdf_msg_wrapper */
                    local_free = sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                                   &rtfw_msg_free, NULL);
                    wrapper =
                        sdf_msg_wrapper_local_alloc(response_msg, local_free,
                                                    SMW_MUTABLE_FIRST,
                                                    SMW_TYPE_REQUEST,
                                                    node /* src */,
                                                    SDF_RESPONSES,
                                                    node /* dest */,
                                                    SDF_FLSH /* dest */,
                                                    FLSH_REQUEST, NULL /* not a response */);
                    /* send to dest node */
                    rtn_send_msg(temp_node, wrapper, request_state->mbx);
                }
            } else {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                             "get node:%d shard_id:%d container_meta failed ",
                             (int)node, (int)shard);
                failed = 1;
            }
        }
    }
    if (failed) {
        if (response_msg != NULL) {
            sdf_msg_free_buff(response_msg);
        }
        /* set a response to sync_functions */
        replication_test_framework_read_data_free_cb_t free_cb =
            replication_test_framework_read_data_free_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                                &rtfw_read_free,
                                                                NULL);
        plat_closure_apply(rtfw_get_cursors_cb,
                           &cb, SDF_FAILURE, NULL, 0, free_cb);
    }
}


/**
 * @brief Get iteration cursors synchronously
 *
 * This is a thin wrapper around #rtfw_get_cursors_async
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param sguid <IN> data shard GUID
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param seqno_start <IN> start of sequence number range, inclusive
 *
 * @param seqno_len   <IN> max number of cursors to return at a time
 *
 * @param seqno_max   <IN> end of sequence number range, inclusive
 *
 * @param data        <OUT> Data, must free with free_cb.
 * @param data_len    <OUT> Data length
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t rtfw_get_cursors_sync(struct replication_test_framework *test_framework,
                                   SDF_shardid_t shard, vnode_t node,
                                   uint64_t seqno_start, uint64_t seqno_len,
                                   uint64_t seqno_max,
                                   void *cursor, int cursor_size,
                                   void **data, size_t *data_len,
                                   replication_test_framework_read_data_free_cb_t *free_cb)
{
    SDF_status_t ret;
    struct rtfw_get_cursors_sync_state *state;
    plat_alloc_struct(&state);
    plat_assert(state);
    rtfw_get_cursors_cb_t cb;

    fthMboxInit(&state->mbox);
    cb = rtfw_get_cursors_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                    &rtfw_get_cursors_sync_cb,
                                    state);
    rtfw_get_cursors_async(test_framework, shard, node, seqno_start, seqno_len, seqno_max, cursor, cursor_size, cb);

    fthMboxWait(&state->mbox);
    if (state->status == SDF_SUCCESS) {
        *data_len = state->data_len;
        *free_cb = state->free_cb;
        *data = plat_alloc(state->data_len);
        memcpy(*data, state->data, state->data_len);
        /* free data */
        plat_closure_apply(replication_test_framework_read_data_free_cb, &state->free_cb, state->data, state->data_len);
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                     "get cursors failed");
        ret = SDF_FAILURE;
    }
    ret = state->status;
    plat_free(state);
    return (ret);
}

/**
 * @brief Get by cursor asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param sguid <IN> data shard GUID
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param cursor     <IN> Opaque cursor data
 * @param cursor_len <IN> Cursor length
 *
 * @param cb <IN> applied on completion.
 */
void
rtfw_get_by_cursor_async(struct replication_test_framework *test_framework,
                         SDF_shardid_t shard, vnode_t node,
                         const void *cursor, size_t cursor_len,
                         rtfw_get_by_cursor_cb_t cb)
{
    SDF_status_t status = SDF_FALSE;
    int failed = 0;
    rtm_general_op_t *op;
    sdf_fth_mbx_t *ar_mbx_from_req = NULL;
    SDF_protocol_msg_t *pm = NULL;
    struct sdf_msg *response_msg = NULL;
    struct sdf_msg_wrapper *wrapper = NULL;
    struct replication_test_node *temp_node;
    sdf_msg_wrapper_free_local_t local_free;
    struct timeval now;
    uint32_t msg_len;
    struct SDF_container_meta *cmeta;
    struct framework_request_state *request_state;

    if (node >= test_framework->config.nnode) {
        failed  = 1;
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "illegal or crashed node:%d", node);
    } else {
        temp_node = test_framework->nodes[node];
        if (temp_node) {
            msg_len = sizeof(*response_msg) + sizeof(SDF_protocol_msg_t) + cursor_len;
            response_msg = plat_calloc(1, msg_len);

            response_msg->msg_len = msg_len;
            response_msg->msg_dest_service = SDF_FLSH;
            response_msg->msg_src_service = SDF_RESPONSES;
            response_msg->msg_flags &= ~SDF_MSG_FLAG_MBX_RESP_INCLUDED;
            response_msg->msg_dest_vnode = node;
            response_msg->msg_src_vnode = node;

            pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
            pm->msgtype   = HFGBC;
            pm->shard     = shard;
            pm->data_size = cursor_len;
            memcpy((char *)pm + sizeof(SDF_protocol_msg_t), cursor, cursor_len);

            if (SDF_SUCCESS == rtfw_get_container_meta(test_framework, shard, &cmeta)) {
                /* get node replicator */
                if (SDF_SUCCESS != rtn_get_op_meta(temp_node, cmeta, shard, &pm->op_meta)) {
                    plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                                 "get op_meta failed");
                    failed = 1;
                } else {
                    /* send a start message to framework */
                    now = test_framework->now;
                    op = rtm_start_get_by_cursor(test_framework->model, shard,
                                                 now, node, cursor, cursor_len);
                    plat_assert(op);
                    if (plat_calloc_struct(&request_state)) {
                        request_state->get_by_cursor_closure = cb;
                        request_state->test_framework = test_framework;
                        request_state->op = op;
                        request_state->shard_meta = NULL;
#ifdef NETWORK_DELAY
                        request_state->timeout_usecs = rt_set_async_timeout(test_framework->config);
#else
                        request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_NONE;
#endif

                        request_state->mbx =
                        sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_create(test_framework->closure_scheduler,
                                                                                   &rtfw_get_by_cursor_response,
                                                                                   request_state),
                                                       /* XXX: release arg goes away */
                                                       SACK_REL_YES,
                                                       request_state->timeout_usecs);

#ifndef RTFW_OP_MAP
                        ar_mbx_from_req = request_state->mbx;
#endif
                        if (!request_state->mbx) {
                            status = SDF_FAILURE_MEMORY_ALLOC;
                        }
                    } else {
                        status = SDF_FAILURE_MEMORY_ALLOC;
                    }
                    /* Wrapper as a sdf_msg_wrapper */
                    local_free = sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                                   &rtfw_msg_free, NULL);
                    wrapper =
                        sdf_msg_wrapper_local_alloc(response_msg, local_free,
                                                    SMW_MUTABLE_FIRST,
                                                    SMW_TYPE_REQUEST,
                                                    node /* src */,
                                                    SDF_RESPONSES,
                                                    node /* dest */,
                                                    SDF_FLSH /* dest */,
                                                    FLSH_REQUEST, NULL /* not a response */);
                    /* send to dest node */
                    rtn_send_msg(temp_node, wrapper, request_state->mbx);
                }
            } else {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                             "get by cursor (node:%d shard_id:%d) failed ",
                             (int)node, (int)shard);
                failed = 1;
            }
        }
    }
    if (failed) {
        if (response_msg != NULL) {
            sdf_msg_free_buff(response_msg);
        }
        /* set a response to sync_functions */
        replication_test_framework_read_data_free_cb_t free_cb =
            replication_test_framework_read_data_free_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                                &rtfw_read_free,
                                                                NULL);
        plat_closure_apply(rtfw_get_by_cursor_cb, &cb,
                           SDF_FAILURE, NULL, 0, NULL, 0, 0, 0,
                           SDF_SEQUENCE_NO_INVALID,
                           free_cb);
    }
}

/**
 * @brief Get by cursor synchronously
 *
 * This is a thin wrapper around #rtfw_get_cursors_async
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param sguid <IN> data shard GUID
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param cursor      <IN> Opaque cursor data
 * @param cursor_len  <IN> Cursor length
 * @param max_key_len <OUT> Maximum key length
 *
 * @param key         <OUT> Key (points to buffer of length max_key_len provided by caller)
 * @param key_len     <OUT> Key length
 * @param exptime     <OUT> Expiry time
 * @param createtime  <OUT> Create time
 * @param seqno       <OUT> Sequence number
 * @param data        <OUT> Data, must free with free_cb.
 * @param data_len    <OUT> Data length
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t
rtfw_get_by_cursor_sync(struct replication_test_framework *test_framework,
                        SDF_shardid_t shard, vnode_t node,
                        void *cursor, size_t cursor_len,
                        char *key, int max_key_len, int *key_len,
                        SDF_time_t *exptime, SDF_time_t *createtime,
                        uint64_t *seqno, void **data, size_t *data_len,
                        replication_test_framework_read_data_free_cb_t *free_cb)
{
    SDF_status_t ret;
    struct rtfw_get_by_cursor_sync_state *state;
    plat_alloc_struct(&state);
    plat_assert(state);
    rtfw_get_by_cursor_cb_t cb;

    state->max_key_len = max_key_len;
    state->key         = key;

    fthMboxInit(&state->mbox);
    cb = rtfw_get_by_cursor_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                      &rtfw_get_by_cursor_sync_cb,
                                      state);
    rtfw_get_by_cursor_async(test_framework, shard, node,
                             cursor, cursor_len, cb);

    fthMboxWait(&state->mbox);
    if (state->status == SDF_SUCCESS) {
        // key has already been loaded via state->key
        *key_len    = state->key_len;
        *exptime    = state->exptime;
        *createtime = state->createtime;
        *seqno      = state->seqno;
        *data_len = state->data_len;
        *free_cb = state->free_cb;
        *data = plat_alloc(state->data_len);
        memcpy(*data, state->data, state->data_len);
        /* free data */
        plat_closure_apply(replication_test_framework_read_data_free_cb, &state->free_cb, state->data, state->data_len);
    } else if (state->status == SDF_OBJECT_UNKNOWN) {
        // key has already been loaded via state->key
        *key_len    = state->key_len;
        *exptime    = state->exptime;
        *createtime = state->createtime;
        *seqno      = state->seqno;
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "get by cursor failed");
        ret = SDF_FAILURE;
    }
    ret = state->status;
    plat_free(state);
    return (ret);
}

/**
 * @brief Asynchronous shard delete operation
 *
 * This is a thin wrapper which invokes the model's #rtm_delete_shard function,
 * sends sends a HFDSH message to the replication service on the given node,
 * and applies the closure on completion.
 *
 * @param test_framework <IN> Test framework
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard <IN> Shard id to delete
 * @param cb <IN> closure invoked on completion
 */
void
rtfw_delete_shard_async(struct replication_test_framework *test_framework,
                        vnode_t node, SDF_shardid_t shard,
                        replication_test_framework_cb_t cb) {
    SDF_status_t status = SDF_FALSE;
    int failed = 0;
    rtm_general_op_t *op;
    sdf_fth_mbx_t *ar_mbx_from_req = NULL;
    struct sdf_msg *response_msg = NULL;
    struct replication_test_node *temp_node;
    struct sdf_msg_wrapper *wrapper = NULL;
    sdf_msg_wrapper_free_local_t local_free;
    struct timeval now;
    uint32_t msg_len;
    struct SDF_container_meta *cmeta;
    struct framework_request_state *request_state;

    if (node >= test_framework->config.nnode) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "Cannot find node %d from framework", node);
        failed = 1;
    } else {
        temp_node = test_framework->nodes[node];
        if (temp_node) {
            response_msg = plat_calloc(1, sizeof(*response_msg)
                                       + sizeof(SDF_protocol_msg_t));
            msg_len = sizeof(*response_msg) + sizeof(SDF_protocol_msg_t);

            response_msg->msg_len = msg_len;
            response_msg->msg_type = REPL_REQUEST;
            response_msg->msg_dest_vnode = node;
            response_msg->msg_src_vnode = node;
            response_msg->msg_src_service = SDF_RESPONSES;
            response_msg->msg_dest_service = SDF_REPLICATION;
            response_msg->msg_flags &= ~SDF_MSG_FLAG_MBX_RESP_INCLUDED;
            response_msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;

            SDF_protocol_msg_t *pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
            pm->msgtype = HFDSH;
            pm->shard = shard;
            pm->data_offset = 0;
            pm->data_size = 0;

            if (SDF_SUCCESS == rtfw_get_container_meta(test_framework,
                                                       shard, &cmeta)) {
                /* get node replicator */
                if (SDF_SUCCESS != rtn_get_op_meta(temp_node, cmeta, shard, &pm->op_meta)) {
                        plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                                     "get op_meta failed");
                        failed = 1;
                } else {
                    now = test_framework->now;
                    op = rtm_start_delete_shard(test_framework->model,
                                                test_framework->ltime, now, node, shard);
                    plat_assert(op);

                    if (plat_calloc_struct(&request_state)) {
                        request_state->response_closure = cb;
                        request_state->test_framework = test_framework;
                        request_state->op = op;
                        request_state->shard_meta = NULL;
                        request_state->shard_id = shard;
#ifdef NETWORK_DELAY
                        request_state->timeout_usecs = rt_set_async_timeout(test_framework->config);
#else
                        request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_NONE;
#endif

                        request_state->mbx =
                        sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_create(test_framework->closure_scheduler,
                                                                                   &rtfw_response,
                                                                                   request_state),
                                                       /* XXX: release arg goes away */
                                                       SACK_REL_YES,
                                                       request_state->timeout_usecs);
#ifndef RTFW_OP_MAP
                        ar_mbx_from_req = request_state->mbx;
#endif

                        if (!request_state->mbx) {
                            status = SDF_FAILURE_MEMORY_ALLOC;
                        }
                    } else {
                        status = SDF_FAILURE_MEMORY_ALLOC;
                    }

                    /* Wrapper as a sdf_msg_wrapper */
                    local_free = sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                                   &rtfw_msg_free,
                                                                   NULL);
                    wrapper = sdf_msg_wrapper_local_alloc(response_msg,
                                                          local_free,
                                                          SMW_MUTABLE_FIRST,
                                                          SMW_TYPE_REQUEST,
                                                          node /* src */,
                                                          SDF_RESPONSES,
                                                          node /* dest */,
                                                          SDF_REPLICATION /* dest */,
                                                          REPL_REQUEST,
                                                          NULL /* not a response */);
                    /* send to dest node */
                    rtn_send_msg(temp_node, wrapper, request_state->mbx);
                }
            } else {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                             "container meta null");
                failed = 1;
            }
        }
    }
    if (failed) {
        if (response_msg != NULL) {
            sdf_msg_free_buff(response_msg);
        }
        /* set a response to sync_functions */
        plat_closure_apply(replication_test_framework_cb,
                           &cb, SDF_FAILURE);
    }
}

/**
 * @brief Synchronous shard delete operation from fthThread
 *
 * This is a thin wrapper arround #rtfw_delete_shard_sync which uses n
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_framework <IN> Test framework
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard <IN> Shard id to delete
 * @param cb <IN> closure invoked on completion
 */
SDF_status_t
rtfw_delete_shard_sync(struct replication_test_framework *test_framework,
                       vnode_t node, SDF_shardid_t shard) {
    SDF_status_t ret;
    struct rtfw_sync_state *state;
    plat_alloc_struct(&state);
    plat_assert(state);
    replication_test_framework_cb_t cb;

    fthMboxInit(&state->mbox);
    cb = replication_test_framework_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                              &rtfw_sync_cb,
                                              state);
    rtfw_delete_shard_async(test_framework, node, shard, cb);

    fthMboxWait(&state->mbox);
    ret = state->status;
    plat_free(state);
    return (ret);
}


/**
 * @brief Asynchronous write operation
 *
 * This is a thin wrapper which invokes the node's replicator
 * #sdf_replicator_get_op_meta function, starts the operation in the
 * model with #rtm_start_write, and sends an HFPTF message to the
 * replicator.
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param meta <IN> Meta-data.  Should be initialized with
 * #replication_test_meta_init before non-defaults are specified.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @param data <IN> Data
 * @param data_len <IN> Data length
 * @param cb <IN> closure invoked on completion.  The buffer should be treated
 * as read-only until released by rtfw_read_free.
 */
void
rtfw_write_async(struct replication_test_framework *test_framework,
                 SDF_shardid_t shard, vnode_t node,
                 const struct replication_test_meta *meta, const void *key,
                 size_t key_len, const void *data, size_t data_len,
                 replication_test_framework_cb_t cb) {
    int failed = 0;
    sdf_fth_mbx_t *ar_mbx_from_req = NULL;
    rtm_general_op_t *op;
    SDF_status_t status;
    SDF_protocol_msg_t *pm = NULL;
    struct sdf_msg *response_msg = NULL;
    struct sdf_msg_wrapper *wrapper = NULL;
    struct replication_test_node *temp_node;
    sdf_msg_wrapper_free_local_t local_free;
    struct timeval now;
    struct framework_request_state *request_state;

    uint32_t msg_len;
    struct SDF_container_meta *cmeta;
    if (node >= test_framework->config.nnode) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "Can't find node node_id:%d", node);
        failed = 1;
    } else {
        temp_node = test_framework->nodes[node];
        if (temp_node) {
            response_msg = plat_calloc(1, sizeof(*response_msg)+sizeof(SDF_protocol_msg_t)+data_len);
            msg_len = sizeof(*response_msg)+ sizeof(SDF_protocol_msg_t) + data_len;
            response_msg->msg_len = msg_len;
            response_msg->msg_dest_service = SDF_REPLICATION;
            response_msg->msg_src_service = SDF_RESPONSES;
            response_msg->msg_dest_vnode = node;
            response_msg->msg_src_vnode = node;
            response_msg->msg_type = REPL_REQUEST;
            response_msg->msg_flags &= ~SDF_MSG_FLAG_MBX_RESP_INCLUDED;
            response_msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;

            pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
            pm->msgtype = HFSET;
            pm->data_offset = 0;
            pm->shard = shard;
            pm->data_size = data_len;
            pm->key.len = key_len;
            snprintf(pm->key.key, key_len+1, "%s", (char *)key);
            strncpy((char *)pm + sizeof(SDF_protocol_msg_t), data, data_len);
            if (SDF_SUCCESS == rtfw_get_container_meta(test_framework, shard, &cmeta) && cmeta) {
                /* get node replicator */
                if (SDF_SUCCESS != rtn_get_op_meta(temp_node, cmeta, shard, &pm->op_meta)) {
                    plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                                 "get op_meta failed");
                    failed = 1;
                } else {
                    now = test_framework->now;
                    op = rtm_start_write(test_framework->model, shard, test_framework->ltime, now, node,
                                         (struct replication_test_meta *)meta, key, key_len,
                                         data, data_len, NULL);
                    plat_assert(op);

                    if (plat_calloc_struct(&request_state)) {
                        request_state->response_closure = cb;
                        request_state->test_framework = test_framework;
                        request_state->op = op;
                        request_state->shard_meta = NULL;
#ifdef NETWORK_DELAY
                        request_state->timeout_usecs = rt_set_async_timeout(test_framework->config);
#else
                        request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_NONE;
#endif

                        request_state->mbx =
                        sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_create(test_framework->closure_scheduler,
                                                                                   &rtfw_response,
                                                                                   request_state),
                                                       /* XXX: release arg goes away */
                                                       SACK_REL_YES,
                                                       request_state->timeout_usecs);

#ifndef RTFW_OP_MAP
                        ar_mbx_from_req = request_state->mbx;
#endif
                        if (!request_state->mbx) {
                            status = SDF_FAILURE_MEMORY_ALLOC;
                        }
                    } else {
                        status = SDF_FAILURE_MEMORY_ALLOC;
                    }

                    /* Wrapper as a sdf_msg_wrapper */
                    local_free =
                        sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                          &rtfw_msg_free,
                                                          NULL);
                    wrapper =
                        sdf_msg_wrapper_local_alloc(response_msg, local_free,
                                                    SMW_MUTABLE_FIRST,
                                                    SMW_TYPE_REQUEST,
                                                    node /* src */,
                                                    SDF_RESPONSES,
                                                    node /* dest */,
                                                    SDF_REPLICATION /* dest */,
                                                    REPL_REQUEST,
                                                    NULL /* not a response */);
                    /* send to dest node */
                    rtn_send_msg(temp_node, wrapper, request_state->mbx);
                }
            } else {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                             "get container meta failed");
                failed = 1;
            }
        }
    }
    if (failed) {
        if (response_msg != NULL) {
            sdf_msg_free_buff(response_msg);
        }
        /* set a response to sync_functions */
        plat_closure_apply(replication_test_framework_cb,
                           &cb, SDF_FAILURE);
    }
}

/**
 * @brief Synchronous write operation from fthThread
 *
 * This is a thin wrapper arround #rtfw_write_async  which uses n
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param meta <IN> Meta-data.  Should be initialized with
 * #replication_test_meta_init before non-defaults are specified.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @param data <IN> Data
 * @param data_len <IN> Data length
 * @return SDF status
 */

SDF_status_t
rtfw_write_sync(struct replication_test_framework *test_framework,
                SDF_shardid_t shard, vnode_t node,
                const struct replication_test_meta *meta, const void *key,
                size_t key_len, const void *data, size_t data_len) {
    SDF_status_t ret;
    struct rtfw_sync_state *state;
    plat_alloc_struct(&state);
    plat_assert(state);
    replication_test_framework_cb_t cb;

    fthMboxInit(&state->mbox);
    cb = replication_test_framework_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                              &rtfw_sync_cb,
                                              state /* Here! */);
    rtfw_write_async(test_framework, shard, node, meta, key, key_len, data, data_len, cb);

    fthMboxWait(&state->mbox);
    ret = state->status;
    plat_free(state);
    return (ret);
}



/**
 * @brief Asynchronous read operation
 *
 * This is a thin wrapper which invokes the node's replicator
 * #sdf_replicator_get_op_meta function, starts the operation in the
 * model with #rtm_start_read, and sends an HFGFF message to the
 * replicator.
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @param cb <IN> closure invoked on completion.  The buffer should be treated
 * as read-only until released by rtfw_read_free.
 */
void
rtfw_read_async(struct replication_test_framework *test_framework,
                SDF_shardid_t shard, vnode_t node, const void *key,
                size_t key_len,
                replication_test_framework_read_async_cb_t cb) {
    SDF_status_t status = SDF_FALSE;
    int failed = 0;
    rtm_general_op_t *op;
    sdf_fth_mbx_t *ar_mbx_from_req = NULL;
    SDF_protocol_msg_t *pm = NULL;
    struct sdf_msg *response_msg = NULL;
    struct sdf_msg_wrapper *wrapper = NULL;
    struct replication_test_node *temp_node;
    sdf_msg_wrapper_free_local_t local_free;
    struct timeval now;
    uint32_t msg_len;
    struct SDF_container_meta *cmeta;
    struct framework_request_state *request_state;

    if (node >= test_framework->config.nnode) {
        failed  = 1;
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "illegal or crashed node:%d", node);
    } else {
        temp_node = test_framework->nodes[node];
        if (temp_node) {
            response_msg = plat_calloc(1, sizeof(*response_msg) +
                                       sizeof(SDF_protocol_msg_t));
            msg_len = sizeof(*response_msg) + sizeof(SDF_protocol_msg_t);

            response_msg->msg_len = msg_len;
            response_msg->msg_type = REPL_REQUEST;
            response_msg->msg_dest_service = SDF_REPLICATION;
            response_msg->msg_src_service = SDF_RESPONSES;
            response_msg->msg_flags &= ~SDF_MSG_FLAG_MBX_RESP_INCLUDED;
            response_msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;
            response_msg->msg_dest_vnode = node;
            response_msg->msg_src_vnode = node;

            pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
            pm->msgtype = HFGFF;
            pm->key.len = key_len;
            pm->shard = shard;
            snprintf(pm->key.key, key_len+1, "%s", (char *)key);
            if (SDF_SUCCESS == rtfw_get_container_meta(test_framework, shard, &cmeta)) {
                /* get node replicator */
                if (SDF_SUCCESS != rtn_get_op_meta(temp_node, cmeta, shard, &pm->op_meta)) {
                    plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                                 "get op_meta failed");
                    failed = 1;
                } else {
                    /* send a start message to framework */
                    now = test_framework->now;
                    op = rtm_start_read(test_framework->model, shard,
                                        test_framework->ltime, now, node, key, key_len);
                    plat_assert(op);
                    if (plat_calloc_struct(&request_state)) {
                        request_state->read_aync_closure = cb;
                        request_state->test_framework = test_framework;
                        request_state->op = op;
                        request_state->shard_meta = NULL;
#ifdef NETWORK_DELAY
                        request_state->timeout_usecs = rt_set_async_timeout(test_framework->config);
#else
                        request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_NONE;
#endif

                        request_state->mbx =
                        sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_create(test_framework->closure_scheduler,
                                                                                   &rtfw_read_response,
                                                                                   request_state),
                                                       /* XXX: release arg goes away */
                                                       SACK_REL_YES,
                                                       request_state->timeout_usecs);

#ifndef RTFW_OP_MAP
                        ar_mbx_from_req = request_state->mbx;
#endif
                        if (!request_state->mbx) {
                            status = SDF_FAILURE_MEMORY_ALLOC;
                        }
                    } else {
                        status = SDF_FAILURE_MEMORY_ALLOC;
                    }
                    /* Wrapper as a sdf_msg_wrapper */
                    local_free = sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                                   &rtfw_msg_free, NULL);
                    wrapper =
                        sdf_msg_wrapper_local_alloc(response_msg, local_free,
                                                    SMW_MUTABLE_FIRST,
                                                    SMW_TYPE_REQUEST,
                                                    node /* src */,
                                                    SDF_RESPONSES,
                                                    node /* dest */,
                                                    SDF_REPLICATION /* dest */,
                                                    FLSH_REQUEST,
                                                    NULL /* not a response */);
                    /* send to dest node */
                    rtn_send_msg(temp_node, wrapper, request_state->mbx);
                }
            } else {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                             "get node:%d shard_id:%d container_meta failed ",
                             (int)node, (int)shard);
                failed = 1;
            }
        }
    }
    if (failed) {
        if (response_msg != NULL) {
            sdf_msg_free_buff(response_msg);
        }
        /* set a response to sync_functions */
        replication_test_framework_read_data_free_cb_t free_cb =
            replication_test_framework_read_data_free_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                                &rtfw_read_free,
                                                                NULL);
        plat_closure_apply(replication_test_framework_read_async_cb,
                           &cb, SDF_FAILURE, NULL, 0, free_cb);
    }
}


/**
 * @brief Synchronous read operation from fthThread
 *
 * This is a thin wrapper arround #rtfw_read_async which uses n
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @param data <OUT> Data, must free with free_cb.
 * @param data_len <OUT> Data length
 * @param free_cb <OUT> Free callback for data.  Call
 * replication_test_framework_read_data_free_cb_apply(free_cb, data, data_len)
 * @return SDF status
 */
SDF_status_t
rtfw_read_sync(struct replication_test_framework *test_framework,
               SDF_shardid_t shard, vnode_t node, const void *key,
               size_t key_len, void **data, size_t *data_len,
               replication_test_framework_read_data_free_cb_t *free_cb) {
    SDF_status_t ret;
    struct rtfw_read_sync_state *state;
    plat_alloc_struct(&state);
    plat_assert(state);
    replication_test_framework_read_async_cb_t cb;

    fthMboxInit(&state->mbox);
    cb = replication_test_framework_read_async_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                         &rtfw_read_sync_cb,
                                                         state);
    rtfw_read_async(test_framework, shard, node, key, key_len, cb);

    fthMboxWait(&state->mbox);
    if (state->status == SDF_SUCCESS) {
        *data_len = state->data_len;
        *free_cb = state->free_cb;
        *data = plat_alloc(state->data_len);
        memcpy(*data, state->data, state->data_len);
        /* free data */
        plat_closure_apply(replication_test_framework_read_data_free_cb, &state->free_cb, state->data, state->data_len);
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                     "sync read failed");
        ret = SDF_FAILURE;
    }
    ret = state->status;
    plat_free(state);
    return (ret);
}

/**
 * @brief Asynchronous delete operation
 *
 * This is a thin wrapper which invokes the node's replicator
 * #sdf_replicator_get_op_meta function, starts the operation in the
 * model with #rtm_start_write, and sends an HZSF message to the
 * replicator.
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @param cb <IN> closure invoked on completion.  The buffer should be treated
 * as read-only until released by rtfw_read_free.
 */
void
rtfw_delete_async(struct replication_test_framework *test_framework,
                  SDF_shardid_t shard, vnode_t node, const void *key,
                  size_t key_len, replication_test_framework_cb_t cb) {
    SDF_status_t status = SDF_FALSE;
    sdf_fth_mbx_t *ar_mbx_from_req = NULL;
    rtm_general_op_t *op;
    SDF_protocol_msg_t *pm = NULL;
    struct sdf_msg *response_msg = NULL;
    struct sdf_msg_wrapper *wrapper = NULL;
    struct replication_test_node *temp_node;
    sdf_msg_wrapper_free_local_t local_free;
    struct timeval now;
    struct framework_request_state *request_state;
    struct SDF_container_meta *cmeta;
    uint32_t msg_len;
    int failed = 0;

    if (node >= test_framework->config.nnode) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "illegal or crashed node:%d", node);
        failed = 1;
    } else {
        temp_node = test_framework->nodes[node];
        if (temp_node) {
            response_msg = plat_calloc(1, sizeof(*response_msg) +
                                       sizeof(SDF_protocol_msg_t));
            msg_len = sizeof(*response_msg) + sizeof(SDF_protocol_msg_t);
            response_msg->msg_len = msg_len;
            response_msg->msg_type = REPL_REQUEST;
            response_msg->msg_dest_service = SDF_REPLICATION;
            response_msg->msg_src_service = SDF_RESPONSES;
            response_msg->msg_flags &= ~SDF_MSG_FLAG_MBX_RESP_INCLUDED;
            response_msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;
            response_msg->msg_dest_vnode = node;
            response_msg->msg_src_vnode = node;

            pm = (SDF_protocol_msg_t *)response_msg->msg_payload;
            pm->msgtype = HZSF;
            pm->key.len = key_len;
            pm->shard = shard;
            snprintf(pm->key.key, key_len+1, "%s", (char *)key);

            if (SDF_SUCCESS == rtfw_get_container_meta(test_framework,
                                                       shard, &cmeta)) {
                /* get node replicator */
                if (SDF_SUCCESS != rtn_get_op_meta(temp_node, cmeta, shard, &pm->op_meta)) {
                        plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL,
                                     "get op_meta failed");
                        failed = 1;
                } else {
                    now = test_framework->now;
                    op = rtm_start_delete(test_framework->model, shard,
                                          test_framework->ltime,
                                          now, node, key, key_len);

                    if (plat_calloc_struct(&request_state)) {
                        request_state->response_closure = cb;
                        request_state->op = op;
                        request_state->test_framework = test_framework;
                        request_state->shard_meta = NULL;
#ifdef NETWORK_DELAY
                        request_state->timeout_usecs = rt_set_async_timeout(test_framework->config);
#else
                        request_state->timeout_usecs = SDF_FTH_MBX_TIMEOUT_NONE;
#endif

                        request_state->mbx =
                        sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_create(test_framework->closure_scheduler,
                                                                                   &rtfw_response, request_state),
                                                       /* XXX: release arg goes away */
                                                       SACK_REL_YES,
                                                       request_state->timeout_usecs);

#ifndef RTFW_OP_MAP
                        ar_mbx_from_req = request_state->mbx;
#endif
                        if (!request_state->mbx) {
                            status = SDF_FAILURE_MEMORY_ALLOC;
                        }
                    } else {
                        status = SDF_FAILURE_MEMORY_ALLOC;
                    }

                    /* Wrapper as a sdf_msg_wrapper */
                    local_free =
                        sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                          &rtfw_msg_free, NULL);
                    wrapper =
                        sdf_msg_wrapper_local_alloc(response_msg, local_free,
                                                    SMW_MUTABLE_FIRST,
                                                    SMW_TYPE_REQUEST,
                                                    node /* src */,
                                                    SDF_RESPONSES,
                                                    node /* dest */,
                                                    SDF_REPLICATION /* dest */,
                                                    REPL_REQUEST,
                                                    NULL /* not a response */);
                    /* send to dest node */
                    rtn_send_msg(temp_node, wrapper, request_state->mbx);
                }
            } else {
                plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                             "container meta null");
                failed = 1;
            }
        }
    }
    if (failed) {
        if (response_msg != NULL) {
            sdf_msg_free_buff(response_msg);
        }
        /* set a response to sync_functions */
        plat_closure_apply(replication_test_framework_cb,
                           &cb, SDF_FAILURE);
    }
}

/**
 * @brief Synchronous delete operation from fthThread
 *
 * This is a thin wrapper arround #rtfw_delete_async  which uses n
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @return SDF status
 */

SDF_status_t
rtfw_delete_sync(struct replication_test_framework *test_framework,
                 SDF_shardid_t shard, vnode_t node, const void *key,
                 size_t key_len) {
    SDF_status_t ret;
    struct rtfw_sync_state *state;
    plat_alloc_struct(&state);
    plat_assert(state);
    replication_test_framework_cb_t cb;

    fthMboxInit(&state->mbox);
    cb = replication_test_framework_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                              &rtfw_sync_cb,
                                              state);
    rtfw_delete_async(test_framework, shard, node, key, key_len, cb);

    fthMboxWait(&state->mbox);
    ret = state->status;
    plat_free(state);
    return (ret);
}

/* RT_TYPE_META_STORAGE functions */

void
rtfw_set_meta_storage_cb(struct replication_test_framework *test_framework,
                         rtfw_shard_meta_cb_t update_cb) {
    int i;
    struct replication_test_node *test_node;

    for (i = 0; i < test_framework->config.nnode; i++) {
        test_node = test_framework->nodes[i];
        plat_assert(test_node);
        rtn_set_meta_storage_cb(test_node, update_cb);
    }
}

/** @brief State for all synchronous RT_TYPE_META_STORAGE functions */
struct rtfw_shard_meta_sync_state {
    /** @brief Response mbx */
    fthMbox_t mbox;

    /** @brief Closure wrapper around rtfw_shard_meta_sync_cb */
    rtfw_shard_meta_cb_t cb;

    /** @brief Enumerate all the response closure fields */
    SDF_status_t status;
    struct cr_shard_meta *cr_shard_meta;
    struct timeval lease_expires;
};

/** @brief Completion function for all synchronous RT_TYPE_META_STORAGE functions */
static void
rtfw_shard_meta_sync_cb(plat_closure_scheduler_t *context, void *env,
                        SDF_status_t status,
                        struct cr_shard_meta *cr_shard_meta,
                        struct timeval lease_expires,
                        vnode_t node)
{
    struct rtfw_shard_meta_sync_state *state =
        (struct rtfw_shard_meta_sync_state *)env;

    state->status = status;
    state->cr_shard_meta = cr_shard_meta;
    state->lease_expires = lease_expires;
    fthMboxPost(&state->mbox, 0 /* value doesn't matter */);
}

/** @brief Start function for all synchronous RT_TYPE_META_STORAGE functions */
static struct rtfw_shard_meta_sync_state *
rtfw_start_meta_op(struct replication_test_framework *test_framework) {
    struct rtfw_shard_meta_sync_state *state;

    plat_calloc_struct(&state);
    plat_assert(state);
    fthMboxInit(&state->mbox);
    state->cb = rtfw_shard_meta_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                          &rtfw_shard_meta_sync_cb, state);
    return (state);
}

/** @brief End function for all synchronous RT_TYPE_META_STORAGE functions */
static SDF_status_t
rtfw_finish_meta_op(struct replication_test_framework *framework,
                    struct cr_shard_meta **meta_out,
                    struct timeval *lease_expires_out,
                    struct rtfw_shard_meta_sync_state *state) {
    SDF_status_t ret;

    fthMboxWait(&state->mbox);
    ret = state->status;
    if (meta_out) {
        *meta_out = state->cr_shard_meta;
    } else if (state->cr_shard_meta) {
        cr_shard_meta_free(state->cr_shard_meta);
    }

    if (lease_expires_out) {
        *lease_expires_out = state->lease_expires;
    }

    plat_free(state);
    return (ret);
}

void
rtfw_create_shard_meta_async(struct replication_test_framework *test_framework,
                             vnode_t node,
                             const struct cr_shard_meta *cr_shard_meta,
                             rtfw_shard_meta_cb_t cb) {
    struct replication_test_node *test_node;

    plat_assert(node < test_framework->config.nnode);
    test_node = test_framework->nodes[node];
    plat_assert(test_node);
    rtn_create_shard_meta(test_node, cr_shard_meta, cb);
}

SDF_status_t
rtfw_create_shard_meta_sync(struct replication_test_framework *test_framework,
                            vnode_t node,
                            const struct cr_shard_meta *cr_shard_meta,
                            struct cr_shard_meta **meta_out,
                            struct timeval *lease_expires_out) {
    struct rtfw_shard_meta_sync_state *state;

    state = rtfw_start_meta_op(test_framework);
    rtfw_create_shard_meta_async(test_framework, node, cr_shard_meta,
                                 state->cb);
    return (rtfw_finish_meta_op(test_framework, meta_out,
                                lease_expires_out, state));
}

void
rtfw_get_shard_meta_async(struct replication_test_framework *test_framework,
                          vnode_t node, SDF_shardid_t sguid,
                          const struct sdf_replication_shard_meta *shard_meta,
                          rtfw_shard_meta_cb_t cb) {
    struct replication_test_node *test_node;

    plat_assert(node < test_framework->config.nnode);
    test_node = test_framework->nodes[node];
    plat_assert(test_node);
    rtn_get_shard_meta(test_node, sguid, shard_meta, cb);
}

SDF_status_t
rtfw_get_shard_meta_sync(struct replication_test_framework *test_framework,
                         vnode_t node, SDF_shardid_t sguid,
                         const struct sdf_replication_shard_meta *shard_meta,
                         struct cr_shard_meta **meta_out,
                         struct timeval *lease_expires_out) {
    struct rtfw_shard_meta_sync_state *state;

    state = rtfw_start_meta_op(test_framework);
    rtfw_get_shard_meta_async(test_framework, node, sguid, shard_meta,
                              state->cb);
    return (rtfw_finish_meta_op(test_framework, meta_out, lease_expires_out,
                                state));
}

void
rtfw_put_shard_meta_async(struct replication_test_framework *test_framework,
                          vnode_t node,
                          const struct cr_shard_meta *cr_shard_meta,
                          rtfw_shard_meta_cb_t cb) {
    struct replication_test_node *test_node;

    plat_assert(node < test_framework->config.nnode);
    test_node = test_framework->nodes[node];
    rtn_put_shard_meta(test_node, cr_shard_meta, cb);
}

SDF_status_t
rtfw_put_shard_meta_sync(struct replication_test_framework *test_framework,
                         vnode_t node,
                         const struct cr_shard_meta *cr_shard_meta,
                         struct cr_shard_meta **meta_out,
                         struct timeval *lease_expires_out) {
    struct rtfw_shard_meta_sync_state *state;

    state = rtfw_start_meta_op(test_framework);
    rtfw_put_shard_meta_async(test_framework, node, cr_shard_meta, state->cb);
    return (rtfw_finish_meta_op(test_framework, meta_out, lease_expires_out,
                                state));
}

void
rtfw_delete_shard_meta_async(struct replication_test_framework *test_framework,
                             vnode_t node,
                             const struct cr_shard_meta *cr_shard_meta,
                             rtfw_shard_meta_cb_t cb) {
    struct replication_test_node *test_node;

    plat_assert(node < test_framework->config.nnode);
    test_node = test_framework->nodes[node];
    rtn_delete_shard_meta(test_node, cr_shard_meta, cb);
}

SDF_status_t
rtfw_delete_shard_meta_sync(struct replication_test_framework *test_framework,
                            vnode_t node,
                            const struct cr_shard_meta *cr_shard_meta,
                            struct cr_shard_meta **meta_out,
                            struct timeval *lease_expires_out) {
    struct rtfw_shard_meta_sync_state *state;

    state = rtfw_start_meta_op(test_framework);
    rtfw_delete_shard_meta_async(test_framework, node, cr_shard_meta,
                                 state->cb);
    return (rtfw_finish_meta_op(test_framework, meta_out, lease_expires_out,
                                state));
}

/* Internal API for nodes, etc. */

void
rtfw_send_msg(struct replication_test_framework *test_framework,
              vnode_t node, struct sdf_msg_wrapper *msg_wrapper,
              struct sdf_fth_mbx *ar_mbx) {
    struct replication_test_node *test_node;

    plat_assert(node < test_framework->config.nnode);
    test_node = test_framework->nodes[node];
    plat_assert(test_node);

    rtn_send_msg(test_node, msg_wrapper, ar_mbx);
}

/**
 * @brief Synchronously deliver message
 *
 * The function shall plat_assert_always that the source and destination
 * nodes are valid and that the destination service is in the set which
 * are supported (initially SDF_REPLICATION, SDF_REPLICATION_PEER, and
 * SDF_FLSH)
 *
 * @param test_framework <IN> Test framework
 * @param msg_wrapper <IN> Message being sent
 */
SDF_status_t
rtfw_receive_message(struct replication_test_framework *test_framework,
                     struct sdf_msg_wrapper *msg_wrapper) {
    int failed = 0;
    SDF_status_t ret = SDF_SUCCESS;
    struct sdf_msg *request_msg = NULL;
    SDF_protocol_msg_t *pm;
    service_t dest_serv;
    struct replication_test_node *dest_node;

    sdf_msg_wrapper_rwref(&request_msg, msg_wrapper);
    pm = (SDF_protocol_msg_t *)request_msg->msg_payload;

    /* validate src_node and deset_node */
    if (msg_wrapper->src_vnode > test_framework->config.nnode ||
        msg_wrapper->dest_vnode > test_framework->config.nnode) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "msg_wraper:%p illegal "
                         "src_vnode:%d, dest_vnode:%d, nnode:%d",
                         msg_wrapper, msg_wrapper->src_vnode,
                         msg_wrapper->dest_vnode,
                         test_framework->config.nnode);
        failed = 1;
    }


    /* validate src_dest service */
    if (!failed) {
        dest_serv = msg_wrapper->dest_service;
        if (dest_serv != SDF_REPLICATION && dest_serv != SDF_REPLICATION_PEER &&
            dest_serv != SDF_REPLICATION_PEER_META_SUPER &&
            dest_serv != SDF_FLSH && dest_serv != SDF_RESPONSES) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "Illegal service type:%d", dest_serv);
            failed = 1;
        }
    }

    if (!failed) {
        dest_node = test_framework->nodes[msg_wrapper->dest_vnode];
        plat_assert(dest_node);
        if (!dest_node) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "No destination node: %d", msg_wrapper->dest_vnode);
            failed = 1;
        }
    }

    /* validate src/dest node network state */
    if (!failed) {
        int dest_down = !rtn_node_is_network_up(test_framework->nodes[msg_wrapper->dest_vnode]);
        int src_down = !rtn_node_is_network_up(test_framework->nodes[msg_wrapper->src_vnode]);
        if (msg_wrapper->src_vnode != msg_wrapper->dest_vnode && src_down) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "src node: %d network is down, dest_node: %d",
                         msg_wrapper->src_vnode, msg_wrapper->dest_vnode);
            failed = 1;
        }

        /*
         * FIXME: drew 2010-04-09 Asynchronous node manipulations mean that checking at
         * this layer does not guarantee message delivery.  Instead we need to change
         * the rtn_receive_msg API to take a completed closure with a status and
         * maintain a reference count on the msg_wrapper in the sending side
         * (rtfw_receive caller) so that it can be resent when the network is down
         */

        if (msg_wrapper->src_vnode != msg_wrapper->dest_vnode && dest_down) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "dest node: %d network is down", msg_wrapper->dest_vnode);
            failed = 1;
            /**
             * FIXME: for src_node: UP, dest_node: DOWN, we need send an
             * error response message to entry from
             */
            ret = SDF_NODE_DEAD;
        }
    }

    sdf_msg_wrapper_rwrelease(&request_msg, msg_wrapper);

    if (!failed) {
        rtn_receive_msg(dest_node, msg_wrapper);
    } else {
        sdf_msg_wrapper_ref_count_dec(msg_wrapper);
    }

    return (ret);
}

void
rtfw_receive_liveness_ping(struct replication_test_framework *framework,
                           vnode_t src_node, vnode_t dest_node,
                           int64_t src_epoch) {
    struct replication_test_node *test_node;

    plat_assert(src_node < framework->config.nnode);
    plat_assert(dest_node < framework->config.nnode);
    test_node = framework->nodes[dest_node];
    plat_assert(test_node);

    rtn_node_live(test_node, src_node, src_epoch);
}

void
rtfw_receive_connection_close(struct replication_test_framework *framework,
                              vnode_t src_node, vnode_t dest_node,
                              int64_t dest_epoch) {
    struct replication_test_node *test_node;

    plat_assert(src_node < framework->config.nnode);
    plat_assert(dest_node < framework->config.nnode);
    test_node = framework->nodes[dest_node];
    plat_assert(test_node);

    rtn_node_dead(test_node, src_node, dest_epoch);
}

/**
 * @brief Free sdf_msg in test framework
 */
static void
rtfw_msg_free(plat_closure_scheduler_t *context, void *env,
              struct sdf_msg *msg) {
    plat_assert(msg);
    sdf_msg_free_buff(msg);
//    sdf_msg_free(msg);
}

void
rtfw_set_default_replication_props(struct replication_test_config *config,
                                   SDF_replication_props_t *props) {
    props->enabled  = SDF_TRUE;

    props->num_replicas = config->num_replicas;

    if (config->replication_type != SDF_REPLICATION_INVALID) {
        props->type = config->replication_type;
    } else {
        switch (config->test_type) {
        case RT_TYPE_META_STORAGE:
            props->type = SDF_REPLICATION_META_SUPER_NODE;
            break;

        case RT_TYPE_REPLICATOR:
            props->type = SDF_REPLICATION_SIMPLE /* need verify */;
            break;
        }
    }

    if (props->type == SDF_REPLICATION_META_SUPER_NODE) {
        props->num_meta_replicas = 1;
    } else if (props->type == SDF_REPLICATION_SIMPLE) {
        props->num_meta_replicas = 0;
    }


    props->synchronous = SDF_TRUE;
}

struct SDF_shard_meta *
rtfw_init_shard_meta(struct replication_test_config *config,
                     vnode_t first_node,
                     /* in real system generated by generate_shard_ids() */
                     SDF_shardid_t shard_id,
                     SDF_replication_props_t *replication_props) {
    struct SDF_shard_meta *ret = NULL;
    int failed = !plat_calloc_struct(&ret);
    if (!failed) {
        ret->first_node = first_node;
        ret->sguid = shard_id;
        /* mock sguid_meta genaration, possibly a shard_count needed? */
        ret->sguid_meta = SHARD_BASE + shard_id;
        ret->type = SDF_SHARD_TYPE_OBJECT;
        ret->num_objs = config->num_obj_shard;
        /* FIXME: From historic code */
        ret->quota = 0xffffffffffffffff;
        ret->persistence = SDF_SHARD_PERSISTENCE_YES;
        ret->eviction = SDF_SHARD_EVICTION_STORE /* non cached */;
        memcpy(&ret->replication_props, replication_props,
               sizeof(*replication_props));
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "create shard_meta ok");
    }
    return (ret);
}

void
rtfw_read_free(plat_closure_scheduler_t *context, void *env,
               const void *data, size_t data_len) {
    plat_free((void*)data);
}


/**
 *  @brief Process framework user operation response except read
 *  both sync and async operations
 */
static void
rtfw_response(struct plat_closure_scheduler *context,
              void *env,
              struct sdf_msg_wrapper *response) {
    SDF_status_t op_fail;
    SDF_status_t status;
    struct sdf_msg *request_msg = NULL;
    SDF_protocol_msg_t *pm;
    const struct SDF_shard_meta *shard_meta = NULL;
    struct sdf_msg_error_payload *error_payload;

    struct framework_request_state *request_state = (struct framework_request_state *)env;
    sdf_msg_wrapper_rwref(&request_msg, response);
    pm = (SDF_protocol_msg_t *)request_msg->msg_payload;
    if (RTM_GO_CR_SHARD == request_state->op->go_type) {
        shard_meta = request_state->shard_meta;
        plat_assert_always(shard_meta);
    }

    if (request_msg->msg_type == SDF_MSG_ERROR) {
        error_payload = (struct sdf_msg_error_payload *)request_msg->msg_payload;
        status = error_payload->error;
    } else {
        status = pm->status;
    }

    if (status == SDF_SUCCESS) {
        /* create container meta in framework */
        if (RTM_GO_CR_SHARD == request_state->op->go_type) {
            rtfw_add_shard(request_state->test_framework,
                           shard_meta);
        }
        if (RTM_GO_DEL_SHARD == request_state->op->go_type) {
            rtfw_del_shard(request_state->test_framework,
                           request_state->shard_id);
        }
    }

    /* do completed for both sync and async */
    op_fail = rtfw_op_complete(request_state->test_framework, status,
                               request_state->op);
    if (status == SDF_SUCCESS) {
        status = op_fail;
    }
    plat_closure_chain(replication_test_framework_cb, context,
                       &request_state->response_closure, status);

    sdf_msg_wrapper_ref_count_dec(response);
    rtfw_req_state_free(request_state);
}

/**
 *  @brief Process framework user operation response for read_async
 */
static void
rtfw_read_response(struct plat_closure_scheduler *context, void *env,
                   struct sdf_msg_wrapper *response) {
    struct timeval now;
    struct sdf_msg *request_msg = NULL;
    SDF_status_t status;
    int op_fail;
    SDF_protocol_msg_t *pm;
    struct framework_request_state *request_state =
        (struct framework_request_state *)env;
    void *data;
    size_t data_len;

    sdf_msg_wrapper_rwref(&request_msg, response);
    pm = (SDF_protocol_msg_t *)request_msg->msg_payload;
    if (pm->status == SDF_SUCCESS && request_msg->msg_type != SDF_MSG_ERROR) {
        data_len = pm->data_size;
        data = plat_alloc(data_len);
        memcpy(data, ((char *)((char *)pm+sizeof(*pm))), data_len);
    } else {
        data = NULL;
        data_len = 0;
    }
    status = pm->status;

    replication_test_framework_read_data_free_cb_t free_cb =
        replication_test_framework_read_data_free_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                            &rtfw_read_free,
                                                            env);

    /* apply read complete for both sync and async */
    now = request_state->test_framework->now;
    op_fail = rtm_read_complete(request_state->op, now,
                                status, data, data_len);
    if (op_fail) {
        /* Is there any test model related errtype? */
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "test model verify error");
        status = SDF_FAILURE;
    }

    plat_closure_chain(replication_test_framework_read_async_cb, context,
                       &request_state->read_aync_closure, status,
                       data /* data */, data_len /* data_len */,
                       free_cb /* read_free_closure */);
    sdf_msg_wrapper_ref_count_dec(response);
    rtfw_req_state_free(request_state);
}

/**
 *  @brief Process framework user operation response for last_seqno
 */
static void
rtfw_last_seqno_response(struct plat_closure_scheduler *context, void *env,
                         struct sdf_msg_wrapper *response)
{
    struct timeval now;
    struct sdf_msg *request_msg = NULL;
    SDF_status_t status;
    uint64_t     seqno;
    int op_fail;
    SDF_protocol_msg_t *pm;
    struct framework_request_state *request_state =
        (struct framework_request_state *)env;

    sdf_msg_wrapper_rwref(&request_msg, response);
    pm = (SDF_protocol_msg_t *)request_msg->msg_payload;
    if (pm->status == SDF_SUCCESS) {
        seqno = pm->seqno;
    } else {
        seqno = SDF_SEQUENCE_NO_INVALID;
    }
    status = pm->status;

    /* apply read complete for both sync and async */
    now = request_state->test_framework->now;
    op_fail = rtm_last_seqno_complete(request_state->op, now, status, seqno);
    if (op_fail) {
        /* Is there any test model related errtype? */
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "test model verify error");
        status = SDF_FAILURE;
    }

    plat_closure_chain(rtfw_last_seqno_cb, context,
                       &request_state->last_seqno_closure, seqno, status);

    sdf_msg_wrapper_ref_count_dec(response);
    rtfw_req_state_free(request_state);
}


/**
 *  @brief Process framework user operation response for get cursors
 */
static void
rtfw_get_cursors_response(struct plat_closure_scheduler *context, void *env,
                          struct sdf_msg_wrapper *response)
{
    struct timeval now;
    struct sdf_msg *request_msg = NULL;
    SDF_status_t status;
    int op_fail;
    SDF_protocol_msg_t *pm;
    struct framework_request_state *request_state =
        (struct framework_request_state *)env;
    void *data;
    size_t data_len;

    sdf_msg_wrapper_rwref(&request_msg, response);
    pm = (SDF_protocol_msg_t *)request_msg->msg_payload;
    if (pm->status == SDF_SUCCESS) {
        data_len = pm->data_size;
        data = plat_alloc(data_len);
        memcpy(data, ((char *)((char *)pm+sizeof(*pm))), data_len);
    } else {
        data = NULL;
        data_len = 0;
    }
    status = pm->status;

    replication_test_framework_read_data_free_cb_t free_cb =
        replication_test_framework_read_data_free_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                            &rtfw_read_free,
                                                            env);

    /* apply read complete for both sync and async */
    now = request_state->test_framework->now;
    op_fail = rtm_get_cursors_complete(request_state->op, now,
                                       status, data, data_len);
    if (op_fail) {
        /* Is there any test model related errtype? */
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "test model verify error");
        status = SDF_FAILURE;
    }

    plat_closure_chain(rtfw_get_cursors_cb, context,
                       &request_state->get_cursors_closure, status,
                       data /* data */, data_len /* data_len */,
                       free_cb /* read_free_closure */);
    sdf_msg_wrapper_ref_count_dec(response);
    rtfw_req_state_free(request_state);
}

/**
 *  @brief Process framework user operation response for get by cursor
 */
static void
rtfw_get_by_cursor_response(struct plat_closure_scheduler *context, void *env,
                            struct sdf_msg_wrapper *response)
{
    struct timeval now;
    struct sdf_msg *request_msg = NULL;
    SDF_status_t status;
    int op_fail;
    SDF_protocol_msg_t *pm;
    struct framework_request_state *request_state =
        (struct framework_request_state *)env;
    void *data;
    size_t data_len;
    char  *key;
    int    key_len;
    SDF_time_t  exptime;
    SDF_time_t  createtime;
    uint64_t    seqno;

    sdf_msg_wrapper_rwref(&request_msg, response);
    pm = (SDF_protocol_msg_t *)request_msg->msg_payload;
    if (pm->status == SDF_SUCCESS) {
        key        = plat_alloc(pm->key.len+1);
        plat_assert(key);
        memcpy(key, pm->key.key, pm->key.len+1);
        key_len    = pm->key.len;
        exptime    = pm->exptime;
        createtime = pm->createtime;
        seqno      = pm->seqno;
        data_len   = pm->data_size;
        data       = plat_alloc(data_len);
        plat_assert(data);
        memcpy(data, ((char *)((char *)pm+sizeof(*pm))), data_len);
    } else if (pm->status == SDF_OBJECT_UNKNOWN) {
        key        = plat_alloc(pm->key.len+1);
        plat_assert(key);
        memcpy(key, pm->key.key, pm->key.len+1);
        key_len    = pm->key.len;
        exptime = 0;
        createtime = 0;
        seqno = pm->seqno;
        data = NULL;
        data_len = 0;
    } else {
        key     = NULL;
        key_len = 0;
        exptime = 0;
        createtime = 0;
        seqno = SDF_SEQUENCE_NO_INVALID;
        data = NULL;
        data_len = 0;
    }
    status = pm->status;

    replication_test_framework_read_data_free_cb_t free_cb =
        replication_test_framework_read_data_free_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                                            &rtfw_read_free,
                                                            env);

    /* apply read complete for both sync and async */
    now = request_state->test_framework->now;
    op_fail = rtm_get_by_cursor_complete(request_state->op, now,
                                         key, key_len, exptime, createtime, seqno,
                                         status, data, data_len);
    if (op_fail) {
        /* Is there any test model related errtype? */
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "test model verify error");
        status = SDF_FAILURE;
    }

    plat_closure_chain(rtfw_get_by_cursor_cb, context,
                       &request_state->get_by_cursor_closure, status,
                       data /* data */, data_len /* data_len */,
                       key, key_len, exptime, createtime, seqno,
                       free_cb /* read_free_closure */);
    sdf_msg_wrapper_ref_count_dec(response);
    rtfw_req_state_free(request_state);
}

/**
 * @brief Free framework request state
 */
static void
rtfw_req_state_free(struct framework_request_state *request_state) {
    plat_assert(request_state);
    if (request_state->mbx) {
        sdf_fth_mbx_free(request_state->mbx);
    }
    if (request_state->shard_meta) {
        plat_free(request_state->shard_meta);
    }
    plat_free(request_state);
}

/**
 * @brief call op complete at test framework level
 *
 * @param test_framework <IN> Associated test framework
 * @param status <IN> operation complete status
 * @param op <IN> op pointer comes from test model
 * @return 0 if complete success and -1 if failed.
 */
static SDF_status_t
rtfw_op_complete(struct replication_test_framework *test_framework,
                 SDF_status_t status, rtm_general_op_t *op) {
    SDF_status_t ret;

    switch (op->go_type) {
#define item(op_type, action) \
    case op_type:                                                              \
        ret = !(action) ? SDF_SUCCESS : SDF_TEST_MODEL_VIOLATION;              \
        break;
    MSG_ITEMS(op, test_framework->now, status)
#undef item
    default:
        plat_fatal("unhandled op type");
        ret = SDF_FAILURE;
    }
    return (ret);
}

/**
 * @brief An internal add shard function called in the successful
 * completion code of the rtfw_create_shard  functions
 */
static SDF_status_t
rtfw_add_shard(struct replication_test_framework *test_framework,
               const struct SDF_shard_meta *shard_meta) {
    SDF_status_t ret;
    SDF_shardid_t max_shard_id;
    int failed;
    struct SDF_container_meta *cmeta;

    failed = !plat_calloc_struct(&cmeta);
    if (!failed) {
        memset(cmeta, 0, sizeof (*cmeta));
        cmeta->properties.replication = shard_meta->replication_props;
        cmeta->shard = shard_meta->sguid;
        cmeta->node = shard_meta->first_node;
        cmeta->meta_shard = shard_meta->sguid | TEST_SHARD_META;
        max_shard_id = __sync_add_and_fetch(&test_framework->max_shard_id, 1);
        if (test_framework->cmeta[shard_meta->sguid]) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "shard :%"PRIu64"exist!", shard_meta->sguid);
            plat_free(cmeta);
        } else {
            test_framework->cmeta[shard_meta->sguid] = cmeta;
            plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                         "create container meta success");
        }
        ret = SDF_SUCCESS;
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "memory allocate error");
        ret = SDF_FAILURE_MEMORY_ALLOC;
    }
    return (ret);
}

/**
 * @brief Get container meta
 */
static SDF_status_t
rtfw_get_container_meta(struct replication_test_framework *test_framework,
                        SDF_shardid_t shard,
                        struct SDF_container_meta **cmeta) {
    SDF_status_t ret;
    *cmeta = test_framework->cmeta[shard];
    if (*cmeta) {
        ret = SDF_SUCCESS;
    } else {
        ret = SDF_CONTAINER_UNKNOWN;
    }
    return (ret);
}


/**
 * @brief Delete container meta from test framework in the
 * successful completion code of rtfw_delete_shard functions
 */
static SDF_status_t
rtfw_del_shard(struct replication_test_framework *test_framework,
               const SDF_shardid_t shard_id) {
    SDF_status_t ret;
    struct SDF_container_meta *cmeta;
    cmeta = test_framework->cmeta[shard_id];
    if (cmeta) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "del container meta:%p, shard_id:%"PRIu64, cmeta, shard_id);
        test_framework->cmeta[shard_id] = NULL;
        plat_free(cmeta);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "del container meta success");
        ret = SDF_SUCCESS;
    } else {
        ret = SDF_CONTAINER_UNKNOWN;
    }
    return (ret);
}

/*
 * XXX: drew 2009-08-28 We want to modify this so that we can
 * advance some reasonable time past the next scheduled event
 * in order to simulate a busy system where the value comes out
 * of the test configuration.
 */
void
rtfw_idle_thread(uint64_t arg) {
    int fired;
    struct timeval next;
    struct timeval *next_ptr;
    struct replication_test_framework *rtfw =
        (struct replication_test_framework *)arg;

    /* Sleep until the system become idle for the first time */
    fthYield(-1);
    /*
     * rtfw->final_shutdown_phase becomes true after all nodes have
     * shut down.
     */
    while (!rtfw->final_shutdown_phase) {
        /*
         * With only fth threads in the test environment, we can only
         * become idle when there's nothing to run and time is advancing
         * to the next shceduled event or the system has shutdown in
         * which case rtfw->active_nodes would be zero.
         *
         * The assert can fire when the system has lost a message
         * and when multiple fthThreads are dead locking.
         */
        fired = plat_timer_dispatcher_fire(rtfw->timer_dispatcher);

        /* Only advance to the next time when nothing remains to do now */
        if (!fired) {
            next_ptr = plat_timer_dispatcher_get_next(rtfw->timer_dispatcher,
                                                      &next,
                                                      PLAT_TIMER_ABSOLUTE);
            /*
             * DO NOT REMOVE THIS EVER AGAIN!!!!!!!!!!!!! WHEN IT FIRES IT
             * MEANS THAT THE TEST HAS DEADLOCKED.
             */
            if (!next_ptr) {
                plat_log_msg(LOG_ID, LOG_CAT_TIME, LOG_FATAL,
                             "Aborting due to test deadlock, %d", (int)rtfw->final_shutdown_phase);
                plat_abort();
            }
            // The same thing.  Less log output
            plat_assert_always(next_ptr);

            /*
             * Scheduled timer may be in the past, so only advance if things are
             * newer.
             */

            if (next_ptr && timercmp(&next, &rtfw->now, CMP_MORE)) {
                rtfw->now = next;
                plat_assert(rtfw->now.tv_sec >= 0);
                plat_assert(rtfw->now.tv_usec >= 0);
                plat_assert(rtfw->now.tv_usec < 1000000);

                plat_log_msg(LOG_ID, LOG_CAT_TIME, LOG_TRACE,
                             "time now %ld secs %ld usecs",
                             rtfw->now.tv_sec, rtfw->now.tv_usec);
            }
        }

        /* Yield until next idle period */
        fthYield(-1);
    }
    plat_timer_dispatcher_free(rtfw->timer_dispatcher);
    rtfw->timer_dispatcher = NULL;
}

static void
rtfw_sync_cb(plat_closure_scheduler_t *context, void *env,
             SDF_status_t status) {
    struct rtfw_sync_state *state =
        (struct rtfw_sync_state *)env;

    /* Assign all the response closure fields here */
    state->status = status;
    if (status) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "send completed to sync");
    }
    fthMboxPost(&state->mbox, 0 /* value doesn't matter */);
}

static void
rtfw_shutdown_sync_cb(struct plat_closure_scheduler *context, void *env) {
    fthMbox_t *mbox = (fthMbox_t *)env;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "rtfw shutdown sync completed");
    fthMboxPost(mbox, 0 /* value doesn't matter */);
}

static void
rtfw_read_sync_cb(plat_closure_scheduler_t *context, void *env,
                  SDF_status_t status, const void * data, size_t data_len,
                  replication_test_framework_read_data_free_cb_t free_cb) {
    struct rtfw_read_sync_state *state = (struct rtfw_read_sync_state *)env;
    state->data = (void *)data;
    state->data_len = data_len;
    state->status = status;
    state->free_cb = free_cb;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "send completed to read sync");
    fthMboxPost(&state->mbox, 0 /* value doesn't matter */);
}

static void
rtfw_last_seqno_sync_cb(plat_closure_scheduler_t *context, void *env,
                        uint64_t seqno, SDF_status_t status)
{
    struct rtfw_last_seqno_sync_state *state = (struct rtfw_last_seqno_sync_state *)env;
    state->seqno = seqno;
    state->status = status;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "send completed to last_seqno sync");
    fthMboxPost(&state->mbox, 0 /* value doesn't matter */);
}

static void
rtfw_get_cursors_sync_cb(plat_closure_scheduler_t *context, void *env,
                         SDF_status_t status, const void * data, size_t data_len,
                         replication_test_framework_read_data_free_cb_t free_cb) {
    struct rtfw_get_cursors_sync_state *state = (struct rtfw_get_cursors_sync_state *)env;
    state->data = (void *)data;
    state->data_len = data_len;
    state->status = status;
    state->free_cb = free_cb;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "send completed to get cursors");
    fthMboxPost(&state->mbox, 0 /* value doesn't matter */);
}

static void
rtfw_get_by_cursor_sync_cb(plat_closure_scheduler_t *context, void *env,
                           SDF_status_t status,
                           const void * data, size_t data_len,
                           char *key, int key_len, SDF_time_t exptime,
                           SDF_time_t createtime, uint64_t seqno,
                           replication_test_framework_read_data_free_cb_t
                           free_cb)
{
    struct rtfw_get_by_cursor_sync_state *state = (struct rtfw_get_by_cursor_sync_state *)env;

    if (key != NULL) {
        if (key_len < state->max_key_len) {
            (void) strncpy(state->key, key, key_len + 1);
        } else {
            (void) strncpy(state->key, key, state->max_key_len);
        }
        plat_free(key);
    }
    state->key_len = key_len;
    state->exptime = exptime;
    state->createtime = createtime;
    state->seqno = seqno;
    state->data = (void *)data;
    state->data_len = data_len;
    state->status = status;
    state->free_cb = free_cb;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "send completed to get by cursor");
    fthMboxPost(&state->mbox, 0 /* value doesn't matter */);
}

void
rtfw_read_async_cb(plat_closure_scheduler_t *context, void *env,
                   SDF_status_t status, const void * data, size_t data_len,
                   replication_test_framework_read_data_free_cb_t free_cb) {
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Do nothing");
}

/** @brief Timer fired (all common code) */
static void rtfw_block_timer_fired(plat_closure_scheduler_t *context,
                                   void *env, struct plat_event *event) {
    struct rtfw_block_state *state = (struct rtfw_block_state *)env;
    plat_event_free_done_t free_done_cb;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, LOG_DBG,
                 "rtfw block timer fired");

    free_done_cb =
        plat_event_free_done_create(context, &rtfw_block_event_free,
                                    state->event_name);

    plat_event_free(event, free_done_cb);
    fthMboxPost(&state->mbox, 0);
}

static void rtfw_block_event_free(plat_closure_scheduler_t *context,
                                  void *env) {
    char *event_name = (char *)env;
    plat_free(event_name);
}

/** @brief Timer fired at when(all common code) */
static void rtfw_at_timer_fired(plat_closure_scheduler_t *context, void *env,
                                struct plat_event *event) {
    struct rtfw_at_state *state = (struct rtfw_at_state *)env;
    /* Fix me:what's the status for? */
    plat_closure_apply(replication_test_framework_cb, &state->cb, state->status);
    plat_free(state);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, LOG_DBG,
                 "rtfw at timer fired");
}

/**
 * @brief get current time closure
 */
static void
rtfw_gettime(void *env, struct timeval *ret) {
    struct replication_test_framework *test_framework =
        (struct replication_test_framework *)env;
    *ret = test_framework->now;
}

static void
rtfw_gettime_cb(plat_closure_scheduler_t *context, void *env,
                struct timeval *ret) {
    rtfw_gettime(env, ret);
}


/**
 * @brief Callback closure of shutdown test framework async
 */
static void
rtfw_shutdown_response(struct plat_closure_scheduler *context, void *env) {
    struct replication_test_framework *test_framework;
    struct rtfw_shutdown_state *shutdown_state = (struct rtfw_shutdown_state *)env;
    test_framework = shutdown_state->rtfw;
    plat_closure_scheduler_shutdown_t shutdown;
    int i;
    fthWaitEl_t *lock;

    __sync_fetch_and_add(&shutdown_state->comp_node, 1);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "shutdown_state->comp_node:%d",
                 (int)shutdown_state->comp_node);
    if (shutdown_state->comp_node == shutdown_state->rtfw->config.nnode) {
        lock = fthLock(&test_framework->rtfw_lock, 1, NULL);

        shutdown_state->rtfw->final_shutdown_phase = 1;

        /* set container_meta null */
        for (i = 0; i < CONTAINER_META_MAX; i++) {
            if (test_framework->cmeta[i]) {
                plat_free(test_framework->cmeta[i]);
            }
        }
        /* shutdown closure scheduler */
        /* will be dispatched in plat_mbox_scheduler_main() automatically */

        /* free prng */
        plat_prng_free(test_framework->api->prng);

        /* free api */
        plat_free(test_framework->api);

        /* free nodes */
        plat_free(test_framework->nodes);

        /* Apply specified closure */

        /* free test model */
        rtm_free(test_framework->model);

        /* shutdown closure */
        if (test_framework->closure_scheduler) {
            shutdown =
                plat_closure_scheduler_shutdown_create(/* can not set to rtfw->closure_scheduler here */
                                                       test_framework->closure_scheduler,
                                                       &rtfw_closure_scheduler_shutdown,
                                                       test_framework);
            plat_closure_scheduler_shutdown(test_framework->closure_scheduler,
                                            shutdown);
        }

        plat_closure_apply(replication_test_framework_shutdown_async_cb, &(shutdown_state->cb));

        /* Fixme: maybe can not be freed before closure applied */
        plat_free(shutdown_state);
        fthUnlock(lock);
    }
}

/**
 * @brief Closure scheduler done so terminate test
 */
static void
rtfw_closure_scheduler_shutdown(plat_closure_scheduler_t *context, void *env) {
    struct replication_test_framework *rtfw = (struct replication_test_framework *)env;
    rtfw->closure_scheduler = NULL;

    if (!rtfw->config.log_real_time) {
        plat_log_set_gettime(rtfw->old_log_time_fn, rtfw->old_log_time_extra,
                             NULL /* old function */, NULL /* old extra */);
    }


    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "free closure scheduler");

    /* XXX: drew 2009-06-20 We're missing the rtfw free */
}

/**
 * @brief Callback function of rtfw_crash_node_async
 *
 * Set specified node related issues in test framework,
 * and apply callback clousre
 */
static void
rtfw_crash_node_async_cb(struct plat_closure_scheduler *context,
                         void *env, SDF_status_t status_arg) {
    struct rtfw_crash_state *crash_state = (struct rtfw_crash_state *)env;
    SDF_status_t status;
    int after;

    status = status_arg;

    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                 "node_id:%"PRIu32" crashed!", crash_state->node);

    if (status == SDF_SUCCESS) {
        after = __sync_sub_and_fetch(&crash_state->rtfw->active_nodes, 1);
        plat_assert(after >= 0);
    /* XXX: drew 2010-04-15 For historical behavior; should change */
    } else if (status == SDF_NODE_DEAD) {
        status = SDF_NODE_DEAD;
    }

    plat_closure_apply(replication_test_framework_cb,
                       &crash_state->cb, status);
    plat_free(crash_state);
}

SDF_status_t
rtfw_start_node_network_sync(struct replication_test_framework *test_framework,
                             vnode_t node) {
    int i;
    struct replication_test_node *temp_node = NULL;
    rtfw_void_cb_t cb;
    fthWaitEl_t *lock;
    struct fthMbox mbox;
    SDF_status_t ret;

    plat_assert(node < test_framework->config.nnode);
    temp_node = test_framework->nodes[node];
    plat_assert(temp_node);

    cb = rtfw_void_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                             &rtfw_post_mbox_cb, &mbox);

    lock = fthLock(&test_framework->rtfw_lock, 1 /* write */, NULL);

    if (temp_node) {
        fthMboxInit(&mbox);
        rtn_start_network(temp_node, cb);
        fthMboxWait(&mbox);
        ret = SDF_SUCCESS;
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "get node %u error", node);
        ret = SDF_FAILURE;
    }

    if (!test_framework->config.new_liveness && ret == SDF_SUCCESS) {
        /* Simulate liveness subsystem */
        for (i = 0; i < test_framework->config.nnode; i++) {
            plat_assert(test_framework->nodes[i]);
            rtn_node_live(test_framework->nodes[i], node, 0 /* epoch */);

            if (i != node && test_framework->nodes[i] &&
                rtn_node_is_live(test_framework->nodes[i])) {
                rtn_node_live(temp_node, i, 0 /* epoch */);
            }
        }
    }

    fthUnlock(lock);

    return (ret);
}

SDF_status_t
rtfw_shutdown_node_network_sync(struct replication_test_framework *test_framework,
                                vnode_t node) {
    int i;
    struct replication_test_node *temp_node;
    rtfw_void_cb_t cb;
    fthWaitEl_t *lock;
    struct fthMbox mbox;
    SDF_status_t ret;

    plat_assert(node < test_framework->config.nnode);
    temp_node = test_framework->nodes[node];
    plat_assert(temp_node);

    lock = fthLock(&test_framework->rtfw_lock, 1 /* write */, NULL);

    cb = rtfw_void_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                             &rtfw_post_mbox_cb, &mbox);

    if (temp_node) {
        fthMboxInit(&mbox);
        rtn_shutdown_network(temp_node, cb);
        fthMboxWait(&mbox);
        ret = SDF_SUCCESS;
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "get node %u error", node);
        ret = SDF_FAILURE;
    }

    if (!test_framework->config.new_liveness && ret == SDF_SUCCESS) {
        /* Simulate liveness subsystem */
        for (i = 0; i < test_framework->config.nnode; i++) {
            plat_assert(test_framework->nodes[i]);
            if (node != i) {
                rtn_node_dead(test_framework->nodes[i], node, 0 /* epoch */);
                rtn_node_dead(test_framework->nodes[node], i, 0 /* epoch */);
            }
        }
    }

    fthUnlock(lock);

    return (ret);
}

static void
rtfw_post_mbox_cb(plat_closure_scheduler_t *context, void *env) {
    struct fthMbox *mbox = (struct fthMbox *)env;

    fthMboxPost(mbox, 0 /* don't care */);
}

int
rtfw_get_delay_us(struct replication_test_framework *framework,
                  const struct replication_test_timing *timing,
                  struct timeval *tv_out) {
    int ret;

    if (plat_prng_next_int(framework->api->prng, 100) <
        timing->min_delay_percent) {
        ret = timing->min_delay_us;
    } else {
        ret = timing->min_delay_us +
            plat_prng_next_int(framework->api->prng,
                               timing->max_delay_us - timing->min_delay_us);
    }

    if (tv_out) {
        tv_out->tv_sec = ret / PLAT_MILLION;
        tv_out->tv_usec = ret % PLAT_MILLION;
    }

    return (ret);
}
