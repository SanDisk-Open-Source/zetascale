/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   mem_debug_test.c
 * Author: Mingqiang Zhuang
 *
 * Created on March 7, 2008, 9:05 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 */

/** include files **/
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _mem_debug_test
#include "platform/opts.h"
#include "CUnit.h"
#include "Basic.h"
#include "testall.h"

#define PLAT_OPTS_ITEMS_mem_debug_test()

struct plat_opts_config_mem_debug_test {
    char *log_level;
};

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_TEST_MEM_DEBUG, "mem_debug_test");

static CU_BasicRunMode mode = CU_BRM_VERBOSE;
static CU_ErrorAction error_action = CUEA_IGNORE;

int
main(int argc, char* argv[])
{
    int ret = 0;
    struct plat_opts_config_mem_debug_test config;

    setvbuf(stdout, NULL, _IONBF, 0);

    if (plat_opts_parse_mem_debug_test(&config, argc, argv)) {
        plat_log_msg(21770, PLAT_LOG_CAT_PLATFORM_TEST_MEM_DEBUG,
                     PLAT_LOG_LEVEL_ERROR, "Parse options failed.");
        ret = 1;
    }
    if (!ret) {
        if (CU_initialize_registry()) {
            plat_log_msg(21771, PLAT_LOG_CAT_PLATFORM_TEST_MEM_DEBUG,
                         PLAT_LOG_LEVEL_ERROR,
                         "Initialization of Test Registry failed.");
            ret = 1;
        } else {
            add_tests();
            CU_basic_set_mode(mode);
            CU_set_error_action(error_action);
            ret = CU_basic_run_tests();
            if (ret == 0) {
                ret = CU_get_number_of_failures();
            }
            plat_log_msg(21772, PLAT_LOG_CAT_PLATFORM_TEST_MEM_DEBUG,
                         PLAT_LOG_LEVEL_DEBUG,
                         "Tests completed with return value %d.", ret);
            CU_cleanup_registry();
        }
    }

    return (ret);
}

#include "platform/opts_c.h"
