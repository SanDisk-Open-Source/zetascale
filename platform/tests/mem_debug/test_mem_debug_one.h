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
