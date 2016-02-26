/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef REPLICATION_TEST_GENERATOR_H
#define REPLICATION_TEST_GENERATOR_H 1

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
 * $Id: test_generator.h 6183 2009-01-16 12:35:46Z yeahwo $
 */

#include "common/sdftypes.h"
#include "platform/timer_dispatcher.h"
#include "platform/closure.h"
#include "platform/defs.h"
#include "shared/shard_meta.h"
#include "platform/types.h"

#include "protocol/replication/meta_types.h"
#include "protocol/replication/replicator.h"
#include "protocol/replication/tests/test_config.h"
#include "protocol/replication/tests/test_common.h"

/**
 * @brief replication test generator mode
 * specify the mode of replication generator: synchronous or asynchronous
 */
typedef enum {
    /* synchronous mode */
    RTG_TM_SYNC,
    /* asynchronous mode */
    RTG_TM_ASYNC,
}rtg_test_mode;

struct rtg_data_t *rtg_data;
/**
 * @brief replication test generator configuration
 */
struct replication_test_generator_config {
    /** @brief the mode of replication generator */
    rtg_test_mode mode;

    /** @brief random seed */
    uint64_t prng_seed;

    /** @brief iterations: numbers of operation to run */
    int iterations;

    /** @brief max parallel to run at once */
    int max_parallel;

    /** @brief work set size: number of keys to operate */
    int work_set_size;

    /** @brief Number of nodes */
    int nnode;

    /** @brief max object per shard */
    int num_obj_shard;

    /** @brief Current biggest shard_id */
    SDF_shardid_t max_shard_id;
};


struct rtg_async_state {
    struct replication_test_generator *generator;
};

struct rtg_shard_status_t *status;
/**
 * @brief replication test generator structure
 */
struct replication_test_generator {
    /** @brief test framework */
    struct replication_test_framework *fm;

    /** @brief configuration of test generator */
    struct replication_test_generator_config *config;

    /** @brief replication test generator data (keys, key number) */
    struct rtg_data_t *data;

    /** @brief replication test meta */
    struct replication_test_meta *test_meta;

    /** @brief callbacks */
    replication_test_framework_read_async_cb_t async_cb;
    replication_test_framework_read_data_free_cb_t free_cb;
    replication_test_framework_shutdown_async_cb_t shutdown_async_cb;

    /** @brief Closure_scheduler */
    struct plat_closure_scheduler *closure_scheduler;

    /** @brief scheduler fthread */
    fthThread_t *closure_scheduler_thread;

    /** @brief replication test generator shard status */
    struct rtg_shard_status_t *shard_status;

    /** @brief Current number of operations to run in parallel */
    uint64_t num_parallel_operations;

    /** @brief number of operations running now */
    int64_t num_operations_running;

    /** @brief 1 to max parallel operations are run at once */
    uint64_t max_parallel_operations;

    /** @brief Number of operations to run at this load */
    uint64_t operations_remain_at_load;

    /** @brief Number of operations which remain until completion */
    uint64_t operations_remain;

    /** @brief Op counter */
    uint64_t op_count;

    /** @brief fth lock for counters */
    fthLock_t generatorLock;

    struct plat_prng *prng;
};

/**
 * @brief replication test generator allocator
 */
struct replication_test_generator *
replication_test_generator_alloc(struct replication_test_generator_config *config,
                                 int argc, char *argv[]);
/**
 * @brief replication test generator destroyer
 */
void
replication_test_generator_destroy(struct replication_test_generator *generator);

/**
 * @brief replication test generator configuration initialization
 */
void
rtg_config_init(struct replication_test_generator_config *config,
                rtg_test_mode mode, uint64_t seed,
                int max_parallel, int iterations, int work_set_size);

/**
 * @brief start the replication test generator
 */
void rtg_run(uint64_t args);

#endif /* REPLICATION_TEST_GENERATOR_H */
