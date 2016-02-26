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
 * File: log.c
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

#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
/*
 * SDF tests version print function
 * you can simply write your version and use custom_info function to run it
 */

void print_test_info(struct test_info *info){
	struct fail_element *tmp;
	printf("*****************Test Case Info*****************\n");
	if(info==NULL)
		{
			printf("No Test Case Info!\n");
			printf("************************************************\n");
			return;
		}
	//Begin to print information
	printf("\tSummary:\n\tThere are %d test cases totally. %d passed and %d failed.\n",
               info->fail+info->success,info->success,info->fail);
	
	printf("\tIn this case, %d CPU(s) are occupied , for each node: %d PTHREAD(s) are started, %d FTH(s) are started.\n",info->lock_cpu, info->pthread_info, info->fth_info);
	
	printf("\tNode info:MyID - %d, PnodeID - %d\n\tMessages info: %d messages are transfered.\n",
               info->myid, info->pnodeid, info->msg_count);
	
	printf("\tThe test case type is: ");
	if(info->test_type == NORMAL_TEST)
		printf("NORMARL_TEST");
	else if(info->test_type == SHORT_TEST)
		printf("SHORT_TEST\n");
	else if(info->test_type == UNEXPT_TEST)
		printf("UNEXPT_TEST\n");
	else if(info->test_type == FASTPATH_TEST)
		printf("FASTPATH_TEST\n");
	else printf("UNTYPED TEST\n");
	
	printf("\n*****************Queue Pair Info****************\n");
	printf("\tThe queue_pair address is: %p & %p.\n\tThe queue pair type/size is: %d",
               info->queue_pair_info->queue_add_pair[0], info->queue_pair_info->queue_add_pair[1], 
               info->queue_pair_info->queue_pair_type);
	
	printf("\n*****************Fail Case Info*****************\n");
	if(info->fail==0)
		printf("\tNo cases fail\n");
	else{
		int i = 0;
		struct fail_list *ls = info->Fail_list;
		while(ls){
			tmp = ls->element;
			printf("\t\t %d Fail:\n",i);
			printf("\t\t\tSegment Error at %lu, Err Message: %s\n",tmp->fail_segment, tmp->errmsg);
			i++;
			ls = ls->next;
		}
		
	}
	
	printf("************************************************\n\n");
	
	fflush(stdout);
	
}

/*
 * Alocate a fail_list
 * 
 */

struct fail_list * alloc_fail_list(){
	return (struct fail_list *)malloc(sizeof(struct fail_list *));
}


/*
 * put an element of fail on the fail_list
 */

void insert_fail_element(struct fail_list * head, struct fail_element * e){
	struct fail_list * head1 = head;
	if(head1 == NULL){
		head1 = alloc_fail_list();
		head1->element = e;
		head1->next = NULL;
	}
	else{
		while(head1->next)
			head1 = head1->next;
		head1->next->element = e;
		head1->next->next = NULL;
	}
	
}

/*
 * allocate an element for fail 
 * 
 */

struct fail_element * get_fail_element(uint64_t seg, char * msg){
	struct fail_element * fe = (struct fail_element *)malloc(sizeof(struct fail_element));
	if(fe == NULL)
		return NULL;
	fe->fail_segment = seg;
	strncpy(fe->errmsg, msg, 128);
	return fe;
}

/* Before using this log sys, you must initilize the info
 */

void test_info_init(struct test_info *info){
	
	info->success = info->fail = 0;
	info->msg_count = 0;
	info->myid = info->pnodeid = 0;
	info->lock_cpu = 0;
	info->pthread_info = info->fth_info = 0;
	info->test_type = 0;
	info->queue_pair_info = (struct Queue_pair_info *)malloc(sizeof(struct Queue_pair_info));
	info->queue_pair_info->queue_add_pair[0] = 0;
	info->queue_pair_info->queue_add_pair[1] = 0;
	info->queue_pair_info->queue_pair_type = 0;
	info->Fail_list = alloc_fail_list();
}

/* After all you must call this to finalize memory space
 */

void test_info_final(struct test_info *info){
	free(info->queue_pair_info);
	free(info->Fail_list);
	free(info);
}

/* You can call all this function for customize information print
 * arg is the argument that the function fnc will use
 * you must be charge for free it
 */

void custom_info(void fnc(void *), void * arg){
		fnc(arg);
}
