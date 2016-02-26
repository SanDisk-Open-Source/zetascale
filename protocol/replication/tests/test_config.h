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

#ifndef REPLICATION_TEST_CONFIG_H
#define REPLICATION_TEST_CONFIG_H 1

/*
 * File:   sdf/protocol/replication/tests/replication_test_config.h
 *
 * Author: drew
 *
 * Created on October 31, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_config.h 12947 2010-04-16 02:05:47Z drew $
 */

/**
 * Test configuration for replication tests
 */

#include "platform/defs.h"
#include "platform/types.h"

#include "common/sdftypes.h"
#include "common/sdf_properties.h"
#include "protocol/replication/meta_types.h"
#include "protocol/replication/replicator.h"

#define REPLICATION_TEST_TYPE_ITEMS() \
    /** @brief Test replicator */                                              \
    item(RT_TYPE_REPLICATOR, replicator)                                       \
    /** @brief Test meta_storage */                                            \
    item(RT_TYPE_META_STORAGE, meta_storage)


enum replication_test_type {
#define item(caps, lower) caps,
    REPLICATION_TEST_TYPE_ITEMS()
#undef item
};

/** @brief Convert enum replication_test_type to string */
const char *rt_type_to_string(enum replication_test_type test_type);

/**
 * @brief Configure simulated timing for test aspect
 *
 * Note that for events which are inherently queued (for example simulated
 * network messages) that delays are additive.  For example the message
 * sequence A then B with a [10us, 10us] delay on each causes A to be
 * delivered 10us after the current time and B 20us
 */

struct replication_test_timing {
    /** @brief Minimum delay in microseconds */
    unsigned min_delay_us;

    /** @brief Maximum delay in microseconds */
    unsigned max_delay_us;

    /**
     * @brief Percentage of time using minimum
     *
     * The idea is to get a high probability of many events occurring
     * "simultaneously" while still allowing for an otherwise random
     * distribution.
     *
     * It may be nice to have a more configurable property distribution.
     */
    unsigned min_delay_percent;
};

enum replication_test_fail {
    /** @brief Call #plat_abort when an error is detected */
    RTF_ABORT,

    /**
     * @brief Subsequent test functions all return SDF_TEST_FAIL
     *
     * The replication_test_model code treats this result the same
     * as RTF_IGNORE; while the replication_test_framework code
     * has all subsequent calls to the _sync and _async test
     * functions return SDF_TEST_FAIL.
     */
    RTF_ERROR_SUBSEQUENTLY,

    /**
     * @brief Ignore failure.
     *
     * The replication_test_model's rtm_failed() function returns
     * non-zero as does the replication_test_framework rtfw_failed.
     *
     * Subsequent rtfw_{read,write,delete}_{sync,async} operations
     * run normally.
     */
    RTF_IGNORE
};


struct replication_test_config {
    /**
     * @brief Type of test
     *
     * This configures what services will be started by the
     * #replication_test_framework so that the test environment
     * can be leveraged for (sub) unit testing.
     */
    enum replication_test_type test_type;

    /** @brief Number of nodes */
    int nnode;

    /** @brief iterations in test case */
    int iterations;

    /** @brief Number of parallel flash operations per node */
    int nparallel_flash_ops_per_node;

    /** @biref Max object per shard */
    uint32_t num_obj_shard;

    /** @brief Timing on flash operations */
    struct replication_test_timing flash_timing;

    /** @brief Timing on network operations */
    struct replication_test_timing network_timing;

    /** @brief Time for liveness detector to notice a transition */
    struct replication_test_timing liveness_timing;

    /** @brief What happens when the test framework detects a failure */
    enum replication_test_fail failure_mode;

    /** @brief Seed for test pseudo-random number generator */
    int64_t prng_seed;

    /** @brief Number of replicas to create */
    int num_replicas;

    /** @brief What sort of replication? */
    SDF_replication_t replication_type;

    /** @brief Log real time instead of simulated time */
    unsigned log_real_time : 1;

    /**
     * @brief Replicator configuration
     *
     * my_node is ignored by #replication_test_framework_alloc and shall be set
     * appropriately by #replication_test_node_alloc.
     *
     * node_count is ignored by #replication_test_framework_alloc (nnode is
     * used instead) and set appropriately when this is passed to sub-systems.
     *
     * my_node is set to #SDF_ILLEGAL_PNODE and filled in appropriately
     * as this is propagated to the various nodes.
     */
    struct sdf_replicator_config replicator_config;

    /** @brief Liveness detector timeout in seconds */
    int msg_live_secs;

    /** @brief Liveness detector ping time */
    int msg_ping_secs;

    /** @brief Use new liveness simulation */
    unsigned new_liveness : 1;
};

__BEGIN_DECLS

/**
 * @brief Intialize replication test configuration with defaults.
 *
 * Tests should call #rt_config_init and then replace the non-defaults
 * so that additional options can be transparently added.
 */
void
rt_config_init(struct replication_test_config *config, uint32_t iterations);

/**
 * @brief Initialize replication test timing with defaults
 *
 * #rt_config_init and tests shall call #rt_timing_init and replace the
 * non-defaults so options on probability distribution, etc. can be
 * added.
 */
void
rt_timing_init(struct replication_test_timing *timing);

__END_DECLS

#endif /* ndef REPLICATION_TEST_CONFIG_H */
