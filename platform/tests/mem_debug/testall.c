/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   testall.c
 * Author: Mingqiang Zhuang
 *
 * Created on March 8, 2008, 9:24 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 */

/** include files **/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "testall.h"
#include "Basic.h"

/** public data **/
CU_SuiteInfo allsuites[] = {
    {"test_mem_debug", mem_debug_init, mem_debug_cleanup, tests_mem_debug},
    CU_SUITE_INFO_NULL,
};

void
add_tests(void)
{
    assert(NULL != CU_get_registry());
    assert(!CU_is_test_running());

    /* Register suites. */
    if (CU_register_suites(allsuites) != CUE_SUCCESS) {
        fprintf(stderr, "suite registration failed - %s\n",
                CU_get_error_msg());
        exit(EXIT_FAILURE);
    }
}
