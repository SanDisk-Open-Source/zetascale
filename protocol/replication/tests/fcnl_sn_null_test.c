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
 * File:   fcnl_sn_null_test.c
 * Author: Zhenwei Lu
 *
 * Note: Write a null test which configures a replication_test_framework for
 * the RT_TYPE_META_STORAGE, starts, and shuts downtest case for
 * replication test_framework
 *
 * Created on Febrary 3, 2009, 12:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcnl_sn_null_test.c 6323 2009-02-04 15:39:36Z lzwei $
 */
#include "platform/stdio.h"

#include "test_framework.h"
#include "test_common.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/case");


#define PLAT_OPTS_NAME(name) name ## _sn_null_test
#include "platform/opts.h"
#include "misc/misc.h"

#define PLAT_OPTS_ITEMS_sn_null_test()                                                  \
    PLAT_OPTS_COMMON_TEST(common_config)                                                   \
    item("iterations", "how many operations", ITERATIONS,                            \
         parse_int(&config->iterations, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)

#define SLEEP_US 1000000
int iterations;

struct common_test_config {
    struct plat_shmem_config shmem_config;
};

#define PLAT_OPTS_COMMON_TEST(config_field) \
    PLAT_OPTS_SHMEM(config_field.shmem_config)

struct plat_opts_config_sn_null_test {
    struct common_test_config common_config;
    int iterations;
};

/**
 * @brief synchronized create_shard/write/read/delete/delete_shard operations
 */
void
user_operations_sn_null(uint64_t args) {
    struct replication_test_framework *test_framework =
        (struct replication_test_framework *)args;
    /*
     * struct SDF_shard_meta *shard_meta = NULL;
     * SDF_replication_props_t *replication_props = NULL;
     * int failed = 0;
     */
    /* configures test framework accommodate to RT_TYPE_META_STORAGE */
    

 
    /* Assure test_framework is started?! */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start test_framework");
    rtfw_start(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework started\n");

    /* Shutdown test framework */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                 "\n************************************************************\n"
                 "                  Test framework shutdown                       "
                 "\n************************************************************");
    rtfw_shutdown_sync(test_framework);
    plat_free(test_framework);
    
    /* Terminate scheduler */
    fthKill(1);
}

int
main(int argc, char **argv) {
    SDF_status_t status;
    struct replication_test_framework *test_framework = NULL;
    struct replication_test_config *config = NULL;
    int failed;

    plat_log_parse_arg("sdf/prot/replication/test=info");

    struct plat_opts_config_sn_null_test opts_config;
    memset(&opts_config, 0, sizeof (opts_config));
    int opts_status = plat_opts_parse_sn_null_test(&opts_config, argc, argv);
    if (opts_status) {
        plat_opts_usage_sn_null_test();
        return (1);
    }
    if (!opts_config.iterations) {
        opts_config.iterations = 1000;
    }

    iterations = opts_config.iterations;
    failed = !plat_calloc_struct(&config);

    struct plat_opts_config_replication_test_framework_sm *sm_config;
    plat_calloc_struct(&sm_config);
    plat_assert(sm_config);

    /* start shared memory */
    status = framework_sm_init(0, NULL, sm_config);

    /* start fthread library */
    fthInit();

    rt_config_init(config, opts_config.iterations);
    test_framework = replication_test_framework_alloc(config);
    if (test_framework) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "test_framework %p allocated\n",
                     test_framework);
    }
    XResume(fthSpawn(&user_operations_sn_null, 40960), (uint64_t)test_framework);
    fthSchedulerPthread(0);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "JOIN");
    plat_free(config);
    framework_sm_destroy(sm_config);
    return (0);
}

#include "platform/opts_c.h"
