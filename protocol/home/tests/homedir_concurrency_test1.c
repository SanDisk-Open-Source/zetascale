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
 * File:   home.c
 * Author: Darpan Dinker Norman Xu
 *
 * Created on August 5, 2008, 12:19 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcnl_homedir_concurrency.c 2566 2008-08-05 19:56:28Z norman $
 */

/*
 * Create serveral pthreads and fths to create a concurrency environment
 * they create, get and remove to only one item.
 */
#include <pthread.h>
#include <sched.h>

#include "agent/agent_helper.h"
#include "fth/fth.h"
#include "protocol/home/direntry.h"
#include "protocol/home/homedir.h"
#include "protocol/reqq.h"
#include "shared/container.h"
#include "utils/properties.h"

extern struct plat_shmem_alloc_stats g_init_sm_stats, g_end_sm_stats;
extern void print_sm_stats(struct plat_shmem_alloc_stats init,
		struct plat_shmem_alloc_stats end);

static int nthreads = 3;
static int ncores = 3;
static int threads_done = 0;
static int niterator = 1024;
HomeDir homedir;

#define MAX_FTH_THREAD 100
#define MAX_CORES 8
#define MAX_NUMBLOCKS 8192
#define threadstacksize 4096*3
static int g_seq = 0;
//Compare with the status of the homedir
static int nputs = 0;
static int nmisses = 0;
static int ngets = 0;
static int nhits = 0;
static int nremoves = 0;

void testcreate(uint64_t seq) {
	uint64_t cguid = 1;
	SDF_container_type_t ctype = SDF_BLOCK_CONTAINER;
	SDF_boolean_t bEntryCreated;

	for (int i = 0; i < niterator; i++) {
		local_key_t *lkey = get_local_block_key(0);
		DirEntry *entry = HomeDir_get_create(homedir, cguid, ctype, lkey,
				&bEntryCreated);
		if (SDF_TRUE == bEntryCreated) {
			printf("fth %d create block\n", seq);
			fflush(stdout);
			//record actions
			(void) __sync_fetch_and_add(&nputs, 1);
		}
		else
		{
			(void) __sync_fetch_and_add(&ngets, 1);
			(void) __sync_fetch_and_add(&nhits, 1);
		}
		plat_assert_always(entry != NULL);
		// {{
		fthWaitEl_t *wait = reqq_lock(entry->q); // LOCK REQQ
		fthThread_t *top = reqq_peek(entry->q);
		plat_assert_always(top != 0);
//		plat_assert_always(top == fthSelf());
		fthThread_t *self = reqq_dequeue(entry->q);
		plat_assert_always(self != 0);
//		plat_assert_always(self == fthSelf());
		reqq_unlock(entry->q, wait); // UNLOCK REQQ
		// }}
		if (bEntryCreated) {
			free_local_key(lkey); // HomeDir_get_create makes a copy if created via LinkedDirList_put
		}
	}
}

void testget(uint64_t seq) {
	uint64_t cguid = 1;
	SDF_container_type_t ctype = SDF_BLOCK_CONTAINER;

	for (int i = 0; i < niterator; i++) {
		local_key_t *lkey = get_local_block_key(0);
		DirEntry *entry = HomeDir_get(homedir, cguid, ctype, lkey);

		if (entry == NULL)
		{
			(void) __sync_fetch_and_add(&nmisses, 1);
			printf("Miss\n");
			continue;
		}
		else
		{
			(void) __sync_fetch_and_add(&ngets, 1);
			(void) __sync_fetch_and_add(&nhits, 1);
		}
		// {{
		/*if (blockNum < numBlocks) {*/
		fthWaitEl_t *wait = reqq_lock(entry->q); // LOCK REQQ
		fthThread_t *top = reqq_peek(entry->q);
		plat_assert_always(top != 0);
//		plat_assert_always(top == fthSelf());
		fthThread_t *self = reqq_dequeue(entry->q);
		plat_assert_always(self != 0);
//		plat_assert_always(self == fthSelf());
		reqq_unlock(entry->q, wait); // UNLOCK REQQ
/*		if (NULL != (top = reqq_peek(entry->q))) {
			fthResume(top, 0);
			printf(
					"Thread 1: yielding after setting thread 2 to run for block=%u\n",
					blockNum);
			fthYield(1);
		}*/
		//}
		// }}
		free_local_key(lkey);
	}
}

void testremove(uint64_t seq) {
	uint64_t cguid = 1;
	SDF_container_type_t ctype = SDF_BLOCK_CONTAINER;

	for (int i = 0; i < niterator; i++) {
		local_key_t *lkey = get_local_block_key(0);
		DirEntry *entry = HomeDir_remove(homedir, cguid, ctype, lkey);
		if (entry) {
			printf("fth %d remove block\n", seq);
			fflush(stdout);
			(void)__sync_fetch_and_add(&nremoves, 1);

		} else {
			free_local_key(lkey);
			continue;
		}

		// {{
		plat_assert_always(entry);

		fthThread_t *top = reqq_peek(entry->q);
		if (top)
			plat_assert_always(top == fthSelf());
		fthWaitEl_t *wait = reqq_lock(entry->q);
		fthThread_t *self = reqq_dequeue(entry->q);
		if (self)
			plat_assert_always(self == fthSelf());
		reqq_unlock(entry->q, wait);

		reqq_destroy(entry->q);
		plat_assert_always(NULL == entry->home);
		plat_free(entry);
		// }}
		free_local_key(lkey);
	}
}

void testRoutine1(uint64_t arg) {
	int size = 1024;
	char str[size];

	int seq = __sync_fetch_and_add(&g_seq, 1);
	printf("\n%d fth begins\n", seq);
	HomeDir_printStats(homedir, str, size);
	printf("%s\n", str);
	if (seq % 3 == 0) {
		testcreate(seq);
	} else if (seq % 3 == 1) {
		testget(seq);
	} else if (seq % 3 == 2) {
		testremove(seq);
	}
	if (__sync_add_and_fetch(&threads_done, 1) == nthreads * ncores) {
		printf("\nTotal Iterations Completed: %d\n", nthreads * ncores);
		fthKill(222);
	}
	printf("\n%d ends\n", seq);
	fthYield(1);
}

void* pthread_routine(void* arg) {
	uint64_t i;
	fthThread_t *threads[MAX_FTH_THREAD];
	for (i = 0; i < nthreads; i++) {
		threads[i] = fthSpawn(&testRoutine1, threadstacksize);
		fthResume(threads[i], 0);
	}
	fthSchedulerPthread(0);
	return 0;
}

int execute_test() {
	fthInit();
	int size = 1024;
	char str[size];
	uint64_t i;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_t fthPthread[MAX_CORES];

	for (i = 0; i < ncores; i++) {
		pthread_create(&fthPthread[i], &attr, &pthread_routine, (void*) i);
	}

	for (i = 0; i < ncores; i++) {
		pthread_join(fthPthread[i], NULL);
	}

	HomeDir_printStats(homedir, str, size);
	printf("%s\n", str);
	printf("The status should be: nputs is %d, ngets is %d, nremoves is %d,"
			"nhits is %d, nmisses is %d\n", nputs, ngets, nremoves, nhits, nmisses);
	fflush(stdout);

	//Compare with stats_puts, stats_gets, stats_removes,
	//stats_hits, stats_misses in homedir
	plat_assert_always(nputs == homedir->stats_puts);
	plat_assert_always(ngets == homedir->stats_gets);
	plat_assert_always(nremoves == homedir->stats_removes);
	plat_assert_always(nhits == homedir->stats_hits);
	plat_assert_always(nmisses == homedir->stats_misses);
	HomeDir_destroy(homedir);
	return 0;
}

SDF_boolean_t internal_testhomedir_init() {
	SDF_boolean_t ret = SDF_FALSE;
	loadProperties("/opt/schooner/config/schooner-med.properties"); // TODO get filename from command line
	// plat_log_parse_arg("platform/alloc=trace");

	if (SDF_TRUE != (ret = init_agent_sm(0))) {
		printf("init_agent_sm() failed!\n");
	} else {
		plat_shmem_alloc_get_stats(&g_init_sm_stats);
		uint64_t buckets = getProperty_uLongLong("SDF_HOME_DIR_BUCKETS",
				MAX_BUCKETS - 1);
		uint32_t lockType = getProperty_uLongInt("SDF_HOME_DIR_LOCKTYPE",
				HMDIR_FTH_BUCKET);
		homedir = HomeDir_create(buckets, lockType);
	}

	return (ret);
}

int main(int argc, char *argv[]) {
	printf("Start of %s.\n", argv[0]);
	nthreads = 5, ncores = 5;
	if (SDF_TRUE != internal_testhomedir_init()) {
		return -1;
	} else if (argc > 1) {
		niterator = atoi(argv[1]);
	} else if (argc > 2) {
		ncores = atoi(argv[2]);
		if (ncores > MAX_CORES)
			ncores = MAX_CORES;
	} else if (argc > 3) {
		nthreads = atoi(argv[3]);
		if (nthreads > MAX_FTH_THREAD)
			nthreads = MAX_FTH_THREAD;
	}

	plat_assert_always(niterator > 0);
	plat_assert_always(nthreads > 0);
	plat_assert_always(ncores > 0);

	int ret = execute_test();
	printf("End of %s.\n", argv[0]);

	// plat_log_parse_arg("sdf/shared=debug");
	plat_shmem_alloc_get_stats(&g_end_sm_stats);
	print_sm_stats(g_init_sm_stats, g_end_sm_stats);
	return (ret);
}
