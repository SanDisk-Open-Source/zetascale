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
