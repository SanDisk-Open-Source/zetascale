/*
 * Malloc and memcpy wrappers for FDF/Btree.
 * Author: Ramesh Chander.
 * Created on Nov, 2013.
 * (c) Sandisk Inc.
 */

#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <errno.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <inttypes.h>
#include <sys/time.h>
#include <stdbool.h>

uint64_t btree_malloc_total_time = 0;
uint64_t btree_malloc_count = 0;
uint64_t btree_memcpy_total_time = 0;
uint64_t btree_memcpy_count = 0;
uint64_t btree_free_count = 0;

//#define COLLECT_TIME_STATS
#undef COLLECT_TIME_STATS  

uint64_t
get_tod_usecs(void)
{
	struct timeval tv;
	uint64_t time;

	gettimeofday(&tv, NULL);
	time = tv.tv_usec  + tv.tv_sec * 1000 * 1000; //usecs

	return time;
}

void
btree_memcpy(void *dst, const void *src, size_t length, bool dry_run)
{

	void *p = NULL;

	if (length == 0) {
		return;
	}

#ifdef COLLECT_TIME_STATS 
	if (!dry_run) {
		uint64_t start_time = 0;

		__sync_add_and_fetch(&btree_memcpy_count, 1);

		start_time = get_tod_usecs();
		memcpy(dst, src, length);

		__sync_add_and_fetch(&btree_memcpy_total_time, get_tod_usecs() - start_time);
	}
#else 

	if (!dry_run) {
		memcpy(dst, src, length);
	}
#endif 

}


void *
btree_malloc(size_t nbytes)
{
	void *p = NULL;
#ifdef COLLECT_TIME_STATS 
	uint64_t start_time = 0;

	__sync_add_and_fetch(&btree_malloc_count, 1);

	start_time = get_tod_usecs();
	p = malloc(nbytes);

	__sync_add_and_fetch(&btree_malloc_total_time, get_tod_usecs() - start_time);
#else
	
	p = malloc(nbytes);
#endif 
	return p;	
}

void btree_free(void *p)
{
#ifdef COLLECT_TIME_STATS 
	__sync_add_and_fetch(&btree_free_count, 1);
#endif 

	if (p != NULL) {
		free(p);	// another hack to avoid panic
	}
}
