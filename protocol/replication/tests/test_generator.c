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
 * File:   sdf/protocol/replication/tests/test_generator.h
 *
 * Author: Haowei
 *
 * Created on December 8, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_generator.c 9804 2009-06-02 08:50:48Z lzwei $
 */
#include "platform/mbox_scheduler.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/types.h"
#include "platform/prng.h"

#include "protocol/init_protocol.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_common.h"
#include "protocol/action/action_thread.h"
#include "protocol/home/home_flash.h"
#include "protocol/replication/copy_replicator.h"
#include "protocol/replication/replicator.h"
#include "protocol/replication/replicator_adapter.h"

#include "test_common.h"
#include "test_generator.h"
#include "test_framework.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/generator");

typedef struct replication_test_generator_config
        replication_test_generator_config;
typedef struct replication_test_generator replication_test_generator;
typedef struct replication_test_config replication_test_config;
typedef struct replication_test_framework replication_test_framework;
typedef struct plat_prng *plat_prng;

const char ALPHANUMBERICS[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.-";
#define ALPHANUMBERICS_SIZE 64

#define MAX_KEY_NUM 100
#define MAX_KEY_LENGTH 32
#define MIN_KEY_LENGTH 16
#define MAX_DATA_LENGTH 128
#define MIN_DATA_LENGTH 64
#define MAX_SHARD_NUM 10
#define GENERATOR_READ_MISS_RATE 0.2

extern char *SDF_Status_Strings[];

#define RTG_OP_GROUP \
    item(RTG_OP_C_SHARD,   "create_shard",   0.05)           \
    item(RTG_OP_D_SHARD,   "delete_shard",   0.05)           \
    item(RTG_OP_CRASH,     "crash",          0.05)           \
    item(RTG_OP_READ,      "read",           0.55)           \
    item(RTG_OP_WRITE,     "write",          0.20)           \
    item(RTG_OP_DELETE,    "delete",         0.10)

typedef enum {
#define item(name, string, prob) name,
    RTG_OP_GROUP
#undef item
    RTG_OP_NUM,
} rtg_op;

const float prob[] = {
#define item(name, string, prob) prob,
        RTG_OP_GROUP
#undef item
};

struct rtg_env {
    replication_test_generator *generator;
    rtg_op op_type;
    int op_id;
};
typedef struct rtg_env rtg_env;

struct rtg_shutdown_fm_async_state {
    struct replication_test_framework  *fm;
    replication_test_framework_shutdown_async_cb_t  cb;
};

static void rtg_shutdown_fm_async_wrapper(struct plat_closure_scheduler *context, void *env,
                                          SDF_status_t status);

static void rtg_async_cb(struct plat_closure_scheduler *context, void *env,
                         SDF_status_t status);

static void rtg_read_async_cb(plat_closure_scheduler_t *context, void *env,
                              SDF_status_t status, const void *data, size_t data_len,
                              replication_test_framework_read_data_free_cb_t free_cb);

static void
rtg_shutdown_cb(struct plat_closure_scheduler *context, void *env);


const char *
rtg_op_name(rtg_op op) {
    switch (op) {
#define item(name, string, prob) case name: return string;
    RTG_OP_GROUP
#undef item
    default: return "(invalid op)";
    }
}

static float
random_float(plat_prng prng) {
    return ((double)plat_prng_next_int(prng, RAND_MAX) / (double)RAND_MAX);
}

static int
random_prob(plat_prng prng) {
    float rd = random_float(prng);
    int i;
    for (i = 0; i < RTG_OP_NUM; i++) {
        rd -= prob[i];
        if (rd < 0) {
            return (i);
        }
    }
    plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR, "Probability error");
    plat_abort();
    return (0);
}

static char *
random_string(size_t min, size_t max, size_t *length, plat_prng prng) {
    plat_assert(max > min && min > 0);
    size_t len = plat_prng_next_int(prng, max - min) + min;
    char *ret = (char *) plat_calloc(sizeof(char) * len+1, 1);
    plat_assert(ret != NULL);
    int i;
    for (i = 0; i < len; i++) {
        ret[i] = ALPHANUMBERICS[plat_prng_next_int(prng, ALPHANUMBERICS_SIZE)];
    }
    *length = len;
    return (ret);
}

struct rtg_shard_data_t {
    struct SDF_shard_meta *shard_meta;
    int flag;
};
typedef struct rtg_shard_data_t rtg_shard_data_t;

static rtg_shard_data_t *
rtg_shard_data_alloc(replication_test_config *config, vnode_t first_node,
                     SDF_shardid_t shard_id, SDF_replication_props_t *replication_props) {
    rtg_shard_data_t *ret;
    plat_calloc_struct(&ret);
    ret->shard_meta = rtfw_init_shard_meta(config, first_node, shard_id,
                                           replication_props);
    ret->flag = 0;
    return (ret);
}

static void
rtg_shard_data_destroy(rtg_shard_data_t *data) {
    plat_free(data->shard_meta);
    plat_free(data);
}

struct rtg_shard_status_t {
    rtg_shard_data_t ** data;
    int shard_num;
    int cur_shard;
};
typedef struct rtg_shard_status_t rtg_shard_status_t;

static rtg_shard_status_t *
rtg_shard_status_alloc(replication_test_config *config,
                       SDF_replication_props_t *replication_props, plat_prng prng) {
    rtg_shard_status_t *ret;
    plat_calloc_struct(&ret);
    ret->shard_num = MAX_SHARD_NUM;
    ret->cur_shard = 0;
    ret->data = (rtg_shard_data_t **)plat_alloc(sizeof(rtg_shard_data_t *) * ret->shard_num);
    int i;
    for (i = 0; i < ret->shard_num; i++) {
        ret->data[i] = rtg_shard_data_alloc(config, plat_prng_next_int(prng, config->nnode),
                                            i, replication_props);
    }
    return (ret);
}

static void
rtg_shard_status_destroy(rtg_shard_status_t *status) {
    int i;
    for (i = 0; i < status->shard_num; i++) {
        rtg_shard_data_destroy(status->data[i]);
    }
    plat_free(status->data);
    plat_free(status);
}

static struct SDF_shard_meta *
rtg_shard_status_create(rtg_shard_status_t *status, int *shard_id) {
    int ret = status->cur_shard;
    if (ret == status->shard_num) {
        return (NULL);
    }
    status->data[ret]->flag = 1;
    status->cur_shard++;
    *shard_id = ret;
    return (status->data[*shard_id]->shard_meta);
}

static struct SDF_shard_meta *
rtg_shard_status_delete(rtg_shard_status_t *status, int *shard_id) {
    if (status->cur_shard <= 1) {
        return (NULL);
    }
    status->cur_shard--;
    *shard_id = status->cur_shard;
    status->data[status->cur_shard]->flag = 0;
    return (status->data[*shard_id]->shard_meta);
}

static struct SDF_shard_meta *
rtg_shard_status_get(rtg_shard_status_t *status, int *shard_id,
                     plat_prng prng) {
    *shard_id = plat_prng_next_int(prng, status->cur_shard);
    struct SDF_shard_meta *ret = status->data[*shard_id]->shard_meta;
    plat_assert(ret != NULL);
    return (ret);
}

typedef struct {
    char *key;
    size_t key_len;
} rtg_key_t;

struct rtg_data_t {
    rtg_key_t *keys;
    uint64_t key_num;
};

typedef struct rtg_data_t rtg_data_t;

static rtg_data_t *
rtg_data_alloc(int max, plat_prng prng) {
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "max number of keys: %d", max);
    int i;
    int num = plat_prng_next_int(prng, max - 1) + 1;
    rtg_data_t *ret;
    plat_calloc_struct(&ret);
    plat_assert(ret != NULL);
    if (num > MAX_KEY_NUM) {
        num = MAX_KEY_NUM;
    }
    ret->key_num = num;
    ret->keys = (rtg_key_t *)plat_alloc(sizeof(rtg_key_t) * num);
    plat_assert(ret->keys != NULL);
    for (i = 0; i < num; i++) {
        ret->keys[i].key = random_string(MIN_KEY_LENGTH, MAX_KEY_LENGTH,
                                         &(ret->keys[i].key_len), prng);
    }
    return (ret);
}

static void
rtg_data_destroy(rtg_data_t *data) {
    int i;
    for (i = 0; i < data->key_num; i++) {
        plat_free(data->keys[i].key);
    }
    plat_free(data->keys);
    plat_free(data);
}

replication_test_generator *
replication_test_generator_alloc(replication_test_generator_config *config,
                                 int argc, char *argv[]) {
    replication_test_generator *rtg;
    plat_calloc_struct(&rtg);
    plat_assert(rtg != NULL);
    rtg->prng = plat_prng_alloc(config->prng_seed);
    replication_test_config *rt_config;
    plat_calloc_struct(&rt_config);
    plat_assert(rt_config != NULL);
    rt_config_init(rt_config, config->iterations);
    rt_config->prng_seed = config->prng_seed;
    SDF_replication_props_t *replication_props;
    plat_calloc_struct(&replication_props);
    rtfw_set_default_replication_props(rt_config, replication_props);
    rtg->fm = replication_test_framework_alloc(rt_config);
    plat_assert(rtg->fm);
    plat_calloc_struct(&(rtg->test_meta));
    plat_assert(rtg->test_meta != NULL);
    replication_test_meta_init(rtg->test_meta);

    rtg->shard_status = rtg_shard_status_alloc(rt_config, replication_props,
                                               rtg->prng);
    plat_free(replication_props);

    config->nnode = rt_config->nnode;
    config->num_obj_shard = rt_config->num_obj_shard;
    config->max_shard_id = rtg->fm->max_shard_id;

    rtg->config = config;
    rtg->max_parallel_operations = config->max_parallel;
    rtg->num_operations_running = 0;
    rtg->num_parallel_operations = 0;
    rtg->operations_remain = config->iterations;
    rtg->operations_remain_at_load = 0;
    rtg->op_count = 0;
    rtg->data = rtg_data_alloc(config->max_shard_id, rtg->prng);
    rtg->closure_scheduler = plat_mbox_scheduler_alloc();
    rtg->closure_scheduler_thread = fthSpawn(&plat_mbox_scheduler_main, 40960);
    fthResume(rtg->closure_scheduler_thread, (uint64_t)rtg->closure_scheduler);
    fthLockInit(&(rtg->generatorLock));

    rtg->free_cb
            = replication_test_framework_read_data_free_cb_create(rtg->closure_scheduler,
                                                                  &rtfw_read_free,
                                                                  rtg->fm);
    rtg->shutdown_async_cb
            = replication_test_framework_shutdown_async_cb_create(rtg->closure_scheduler,
                                                                  rtg_shutdown_cb,
                                                                  // TODO: rtfw_shutdown_async_op_complete
                                                                  rtg->fm);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Test generator allocated");
    plat_free(rt_config);
    return (rtg);
}

void
replication_test_generator_destroy(replication_test_generator *generator) {
    plat_free(generator->test_meta);
    plat_free(generator->prng);
    rtg_data_destroy(generator->data);
    rtg_shard_status_destroy(generator->shard_status);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO, "Test generator destroyed");
}

void
rtg_config_init(replication_test_generator_config *config,
                rtg_test_mode mode, uint64_t seed, int max_parallel, int iterations,
                int work_set_size) {
    config->mode = mode;
    config->prng_seed = seed;
    config->iterations = iterations;
    config->work_set_size = work_set_size;
    config->max_parallel = max_parallel;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Test generator configure initialized, mode = %s, "
                 "seed = %d, max_parallel = %d, iterations = %d, "
                 "work_set_size = %d", mode == RTG_TM_SYNC ? "SYNC" : "ASYNC",
                 (int)seed, max_parallel, iterations, work_set_size);
}

static void
rtg_start_ops_async(replication_test_generator *generator) {
    fthWaitEl_t *lock = fthLock(&(generator->generatorLock), 1, NULL);
    if (generator->max_parallel_operations == 1) {
        generator->num_parallel_operations = 1;
    } else {
        generator->num_parallel_operations =
            plat_prng_next_int(generator->prng,
                               generator->max_parallel_operations - 1) + 1;
    }
    if (generator->num_parallel_operations > generator->operations_remain) {
        generator->num_parallel_operations = generator->operations_remain;
    }
    generator->num_operations_running = generator->num_parallel_operations;

    plat_assert(generator->num_operations_running > 0);
    generator->operations_remain -= generator->num_operations_running;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "loading operation %d, ruuning %d, remaining %d, generator %p",
                 (int)(generator->num_parallel_operations),
                 (int)(generator->num_operations_running),
                 (int)(generator->operations_remain), generator);
    fthUnlock(lock);

    int i;
    int shard_id;
    replication_test_framework_cb_t cb;
    replication_test_framework_read_async_cb_t async_cb;
    struct SDF_shard_meta *shard_meta;

    for (i = 0; i < generator->num_parallel_operations; i++) {
        rtg_op op = (rtg_op)random_prob(generator->prng);
        rtg_shard_status_get(generator->shard_status, &shard_id,
                             generator->prng);
        rtg_key_t key_struct =
            generator->data->keys[plat_prng_next_int(generator->prng, generator->data->key_num)];
        char *key = key_struct.key;
        size_t key_len = key_struct.key_len;
        size_t data_len;
        char *data;
        vnode_t node_id = plat_prng_next_int(generator->prng,
                                             generator->config->nnode);
        rtg_env *env;
        plat_alloc_struct(&env);
        plat_assert(env != NULL);
        env->generator = generator;
        env->op_type = op;
        env->op_id = __sync_add_and_fetch(&(generator->op_count), 1);
        cb = replication_test_framework_cb_create(generator->closure_scheduler,
                                                  &rtg_async_cb,
                                                  env);

        switch (op) {
        case RTG_OP_C_SHARD:
            shard_meta = rtg_shard_status_create(generator->shard_status,
                                                 &shard_id);
            if (shard_meta == NULL) {
                i --;
                break;
            }
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "calling rtfw_create_shard_async "
                         "shard_id %d op_id %d over", shard_id, env->op_id);
            rtfw_create_shard_async(generator->fm, shard_id, shard_meta, cb);
            break;
        case RTG_OP_D_SHARD:
            shard_meta = rtg_shard_status_delete(generator->shard_status,
                                                 &shard_id);
            if (shard_meta == NULL) {
                i --;
                break;
            }
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "calling rtfw_delete_shard_async "
                         "shard_id %d op_id %d over", shard_id,  env->op_id);
            rtfw_delete_shard_async(generator->fm, node_id, shard_id, cb);
            break;
        case RTG_OP_CRASH:
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "calling rtfw_crash_node_async op_id %d", env->op_id);
            rtfw_crash_node_async(generator->fm, node_id, cb);
            break;
        case RTG_OP_READ:
            async_cb =
                replication_test_framework_read_async_cb_create(generator->closure_scheduler,
                                                                rtg_read_async_cb, env);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "calling rtfw_read_async op_id %d over", env->op_id);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "shard_id %d, node_id %d, key %.*s", shard_id, node_id,
                         (int)key_len, key);
            rtfw_read_async(generator->fm, shard_id, node_id, key, key_len,
                            async_cb);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "rtfw_read_async returned");
            break;
        case RTG_OP_WRITE:
            data = random_string(MIN_DATA_LENGTH, MAX_DATA_LENGTH, &data_len,
                                 generator->prng);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "calling rtfw_write_async op_id %d over", env->op_id);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "key:%.*s, key_len:%d",
                         (int)key_len, (char *)key, (int)key_len);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "data: %.*s",
                         (int)data_len, data);
            rtfw_write_async(generator->fm, shard_id, node_id,
                             generator->test_meta, key, key_len, data, data_len, cb);
            plat_free(data);
            break;
        case RTG_OP_DELETE:
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "calling rtfw_delete_async op_id %d over", env->op_id);
            rtfw_delete_async(generator->fm, shard_id, node_id, key, key_len,
                              cb);
            break;
        default:
            break;
        }
    }
}

static void
rtg_stop_async(replication_test_generator *generator) __attribute__((unused));
static void
rtg_stop_async(replication_test_generator *generator) {
    plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                 "shutdown test framework in ASYNC mode");
    struct timeval now, when;
    struct rtg_shutdown_fm_async_state *state;
    replication_test_framework_cb_t cb;

    if (plat_calloc_struct(&state)) {
        plat_closure_apply(plat_timer_dispatcher_gettime,
                           &generator->fm->api->gettime, &now);
        
        state->cb = generator->shutdown_async_cb;
        state->fm = generator->fm;
        
        when.tv_sec = rt_set_async_timeout(generator->fm->config) / MILLION;
        when.tv_usec = 0;
        plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO, "when:%d", (int)when.tv_sec);

        cb = replication_test_framework_cb_create(generator->fm->closure_scheduler,
                                                  &rtg_shutdown_fm_async_wrapper, state);
        rtfw_at_async(generator->fm, &when, cb);
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_FATAL, "not enough memory");
        plat_abort();
    }
}

static void
rtg_run_async(replication_test_generator *generator) {
    struct rtg_async_state state;
    state.generator = generator;

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Starting test framework in ASYNC mode");
    rtfw_start(generator->fm);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "pre-seeding calling rtfw_create_shard_sync");

    int shard_id;
    struct SDF_shard_meta *shard_meta;

    shard_meta = rtg_shard_status_get(generator->shard_status, &shard_id,
                                      generator->prng);
    plat_assert(shard_meta != NULL);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "calling rtfw_create_shard_sync shard_id %d", shard_id);
    rtfw_create_shard_sync(generator->fm, shard_id, shard_meta);

    rtg_start_ops_async(generator);
}

static void
rtg_run_sync(replication_test_generator *generator) {
    int i = 0;
    SDF_status_t op_ret;
    int shard_id;
    struct SDF_shard_meta *shard_meta;

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Starting test framework in SYNC mode");
    rtfw_start(generator->fm);

    shard_meta = rtg_shard_status_create(generator->shard_status, &shard_id);
    plat_assert(shard_meta != NULL);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "pre-seeding calling rtfw_create_shard_sync");
    op_ret = rtfw_create_shard_sync(generator->fm, 0, shard_meta);

    for (i = 0; i < generator->operations_remain_at_load; i++) {
        rtg_op op = (rtg_op) random_prob(generator->prng);
        rtg_shard_status_get(generator->shard_status, &shard_id,
                             generator->prng);
        rtg_key_t key_struct =
            generator->data->keys[plat_prng_next_int(generator->prng, generator->data->key_num)];
        char *key = key_struct.key;
        size_t key_len = key_struct.key_len;
        size_t data_len;
        char *data;
        vnode_t node_id = plat_prng_next_int(generator->prng,
                                             generator->config->nnode);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "op type: %s", rtg_op_name(op));

        switch (op) {
        case RTG_OP_C_SHARD:
            shard_meta = rtg_shard_status_create(generator->shard_status,
                                                 &shard_id);
            if (shard_meta == NULL)
                break;
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "calling rtfw_create_shard_sync shard_id %d", shard_id);
            op_ret
                    = rtfw_create_shard_sync(generator->fm, shard_id,
                                             shard_meta);
            break;
        case RTG_OP_D_SHARD:
            shard_meta = rtg_shard_status_delete(generator->shard_status,
                                                 &shard_id);
            if (shard_meta == NULL)
                break;
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "calling rtfw_delete_shard_sync shard_id %d", shard_id);
            op_ret = rtfw_delete_shard_sync(generator->fm, node_id, shard_id);
            break;
        case RTG_OP_CRASH:
            plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                         "calling rtfw_crash_node_sync");
            op_ret = rtfw_crash_node_sync(generator->fm, node_id);
            break;
        case RTG_OP_READ:
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "calling rtfw_read_sync shard_id %d, node_id %d, key %s, key_len %d",
                         shard_id,
                         (int)node_id, key, (int)key_len);
            op_ret = rtfw_read_sync(generator->fm, shard_id, node_id, key,
                                    key_len, (void **)&data, &data_len, &(generator->free_cb));
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "rtfw_read_sync returned");
            if (op_ret == SDF_SUCCESS) {
                plat_free(data);
            }
            break;
        case RTG_OP_WRITE:
            data = random_string(MIN_DATA_LENGTH, MAX_DATA_LENGTH, &data_len,
                                 generator->prng);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "calling rtfw_write_sync");
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                         "key:%s, key_len:%d, strlen(key):%d", (char *)key,
                         (int)key_len, (int)strlen((char *)key));
            op_ret = rtfw_write_sync(generator->fm, shard_id, node_id,
                                     generator->test_meta, key, key_len, data, data_len);
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "rtfw_write_sync returned");
            plat_free(data);
            break;
        case RTG_OP_DELETE:
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "calling rtfw_delete_sync");
            op_ret = rtfw_delete_sync(generator->fm, shard_id, node_id, key,
                                      key_len);
            break;
        default:
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR, "internal error occurred");
            break;
        }
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "return status = %s",
                     SDF_Status_Strings[op_ret]);
        fthYield(1);
    }

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "shutdown test framework in SYNC mode");
    rtfw_shutdown_sync(generator->fm);
    fthKill(1);
}

void rtg_run(uint64_t args) {
    replication_test_generator *generator =
            (replication_test_generator *) args;
    switch (generator->config->mode) {
    case RTG_TM_SYNC:
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "start random running in SYNC mode");
        rtg_run_sync(generator);
        break;
    case RTG_TM_ASYNC:
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "start random running in ASYNC mode");
        rtg_run_async(generator);
        break;
    }
}

static void
rtg_continue_async(replication_test_generator *generator) {
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "continue");
    fthWaitEl_t *lock = fthLock(&(generator->generatorLock), 1, NULL);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "before: %"PRId64, generator->num_operations_running);
    uint64_t remain = __sync_sub_and_fetch(&(generator->num_operations_running), 1);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "after: %"PRId64, generator->num_operations_running);
    plat_assert(generator->num_operations_running >= 0);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "%"PRId64" ops remaining at this load", (generator->num_operations_running));
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "%d ops remaining at this run", (int)(generator->operations_remain));
    fthUnlock(lock);
    if (remain == 0 && generator->operations_remain != 0) {
        rtg_start_ops_async(generator);
    } else if (remain == 0 && generator->operations_remain == 0) {
        rtg_stop_async(generator);
    }
}

static void
rtg_async_cb(struct plat_closure_scheduler *context, void *env,
             SDF_status_t status) {
    rtg_env *current = (rtg_env *)env;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "calling back env: %p, op type %s, op_id %d over",
                 env, rtg_op_name(current->op_type), current->op_id);
    replication_test_generator *generator = current->generator;
    plat_free(env);
    rtg_continue_async(generator);
}

static void
rtg_read_async_cb(plat_closure_scheduler_t *context, void *env,
                  SDF_status_t status, const void *data, size_t data_len,
                  replication_test_framework_read_data_free_cb_t free_cb) {
    rtg_env *current = (rtg_env *)env;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "calling back env: %p, op type %s, op_id %d over",
                 env, rtg_op_name(current->op_type), current->op_id);
    plat_free((void *)data);
    replication_test_generator *generator = current->generator;
    plat_free(env);
    rtg_continue_async(generator);
}


static void
rtg_shutdown_cb(struct plat_closure_scheduler *context, void *env) {
    struct replication_test_framework *rtfw = (struct replication_test_framework *)env;
    while (rtfw->timer_dispatcher) {
        fthYield(-1);
    }
    plat_free(rtfw);
    fthKill(1);
}

static void
rtg_shutdown_fm_async_wrapper(struct plat_closure_scheduler *context, void *env,
                              SDF_status_t status) {
    struct rtg_shutdown_fm_async_state *state =
        (struct rtg_shutdown_fm_async_state *)env;
    rtfw_shutdown_async(state->fm, state->cb);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "shutdown complete");
}
