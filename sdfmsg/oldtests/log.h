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
 * File: log.h
 * Author: Enson Zheng
 * 
 * Created on Oct 22nd,2008, 14:13PM
 * 
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 * 
 * Description: This is a log system for SDF tests. Also you can use your function with extend of custom_info 
 * to get your test log. You can refer to print_test_info.
 * 
 *  
 * */

#ifndef LOG_H_
#define LOG_H_

#include "platform/types.h"
#include <stdio.h>
#include <stdlib.h>
#include "sdfmsg/sdf_msg.h"

struct Queue_pair_info{
    struct sdf_queue *queue_add[2];    	            // queue address
    struct sdf_queue_pair *queue_add_pair[2]; // queue pair address
    uint32_t queue_pair_type;   // indicate which type of queue pair
};

struct fail_element{
	uint64_t fail_segment;		 //Used for recording which segment is error. Maybe null
	char errmsg[128];				 //Description for Error in plat_log
};


struct fail_list{
	struct fail_element *element;       //fail element
	struct fail_list *next;		  //Pointer for next element, null for tail
};



struct test_info{
	int success;		//Success cases num
	int fail;		    //Fail cases num
	uint32_t msg_count;		//Total message num
	uint32_t myid, pnodeid;	//Node info
	uint32_t lock_cpu;		//How many cpus are locked during the test
	uint32_t pthread_info;	//Pthread routine
	uint32_t fth_info;		//FTH routine
	enum Test_type{
		NORMAL_TEST,
		SHORT_TEST,
		UNEXPT_TEST,
		FASTPATH_TEST
	}test_type;
	struct Queue_pair_info *queue_pair_info;  //Queue pair info. Will this should be included?
	struct fail_list *Fail_list;
};


void print_test_info(struct test_info *info);

void test_info_init(struct test_info *info);

void test_info_final(struct test_info *info);

void custom_info(void fnc(void *), void * arg);

#endif
