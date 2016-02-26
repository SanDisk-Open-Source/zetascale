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
