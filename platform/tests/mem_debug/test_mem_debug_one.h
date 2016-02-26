/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   test_mem_debug_one.h
 * Author: Mingqiang Zhuang
 *
 * Created on April 17, 2008, 13:12 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 */

#ifndef  TEST_MEM_DEBUG_ONE_H
#define  TEST_MEM_DEBUG_ONE_H

/** include files **/
#include "CUnit.h"
#include <sys/types.h>

/** external data **/
extern CU_TestInfo tests_mem_debug[];

/** public functions **/
/* test suite init and clean*/
int32_t mem_debug_init(void);
int32_t mem_debug_cleanup(void);

void test_mem_debug_init(void);
void test_mem_debug_stats(void);
void test_check_free(void);
void test_check_referenced(void);
void test_reference_with_seperated_block(void);
void test_release_with_seperated_block(void);
void test_reference_with_near_block(void);
void test_release_with_near_block(void);
void test_reference_release_with_diff_size(void);
void test_with_two_pool(void);
void test_log_reference(void);
void test_reference_failture(void);
void test_release_failture(void);
void test_read_write(void);

#endif   /* TEST_MEM_DEBUG_ONE_H */
