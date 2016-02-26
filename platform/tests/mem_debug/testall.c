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
