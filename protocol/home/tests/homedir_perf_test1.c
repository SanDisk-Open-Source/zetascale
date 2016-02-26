/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   homedir_perf_test1.c
 * Author: Darpan Dinker Norman Xu
 *
 * Created on August 26, 2008, 12:19 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: homedir_perf_test1.c 2566 2008-08-26 19:56:28Z norman $
 */

/*
 *
 * hardcode ncores=1 nthreads=1, let create get and remove run sequentially
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

#define PERF_DAILY_RUN

static int nthreads = 3;
static int ncores = 3;
static int threads_done = 0;

HomeDir homedir;
int numBlocks = 1024;
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

static fthThread_t* wait_and_reqq_dequeue(reqq_t *q);
static uint64_t get_passtime(struct timespec* start, struct timespec* end);

static uint64_t time_create_real = 0;
static uint64_t time_get_real = 0;
static uint64_t time_remove_real = 0;
static uint64_t time_create_cpu = 0;
static uint64_t time_get_cpu = 0;
static uint64_t time_remove_cpu = 0;

void testcreate(uint64_t seq) {
	uint64_t cguid = 1;
	SDF_container_type_t ctype = SDF_BLOCK_CONTAINER;
	SDF_boolean_t bEntryCreated;
	struct timespec time1, time2, cputime1, cputime2;

	//calculate start time
	clock_gettime(CLOCK_REALTIME, &time1);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cputime1);

	for (int blockNum = 0; blockNum < numBlocks; blockNum++) {
		local_key_t *lkey = get_local_block_key(blockNum);
		DirEntry *entry = HomeDir_get_create(homedir, cguid, ctype, lkey,
				&bEntryCreated);
		if (SDF_TRUE == bEntryCreated) {
			//printf("fth %d create NO.%d block\n", seq, blockNum);
			//fflush(stdout);
			//record actions
			(void) __sync_fetch_and_add(&nputs, 1);
		} else {
			(void) __sync_fetch_and_add(&ngets, 1);
			(void) __sync_fetch_and_add(&nhits, 1);
		}
		plat_assert_always(entry != NULL);

		(void) wait_and_reqq_dequeue(entry->q);

		if (bEntryCreated) {
			free_local_key(lkey); // HomeDir_get_create makes a copy if created via LinkedDirList_put
		}
	}

	//calculate end time
	clock_gettime(CLOCK_REALTIME, &time2);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cputime2);

	uint64_t real_usec = get_passtime(&time1, &time2);
	uint64_t cpu_usec = get_passtime(&cputime1, &cputime2);

	/*	printf(
	 "homedir_get_create %d times\n\
		Wallclocktime is %"PRIu64" usec, Avg Wallclocktime is %"PRIu64" usec\n\
		Cpuclocktime is %"PRIu64" usec, Avg Cpuclocktime is %"PRIu64" usec\n",
	 numBlocks, real_usec, real_usec / numBlocks, cpu_usec, cpu_usec
	 / numBlocks);*/

	(void) __sync_fetch_and_add(&time_create_real, real_usec);
	(void) __sync_fetch_and_add(&time_create_cpu, cpu_usec);
	printf("Thread 1: Created %d blocks in the directory\n", numBlocks);
}

void testget(uint64_t seq) {
	uint64_t cguid = 1;
	SDF_container_type_t ctype = SDF_BLOCK_CONTAINER;
	struct timespec time1, time2, cputime1, cputime2;

	//calculate start time
	clock_gettime(CLOCK_REALTIME, &time1);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cputime1);

	for (int blockNum = 0; blockNum < numBlocks; blockNum++) {
		local_key_t *lkey = get_local_block_key(blockNum);
		DirEntry *entry = HomeDir_get(homedir, cguid, ctype, lkey);

		if (entry == NULL) {
			(void) __sync_fetch_and_add(&nmisses, 1);
			//printf("Miss %d \n", blockNum);
			continue;
		} else {
			(void) __sync_fetch_and_add(&ngets, 1);
			(void) __sync_fetch_and_add(&nhits, 1);
		}

		(void) wait_and_reqq_dequeue(entry->q);

		free_local_key(lkey);
	}

	//calculate end time
	clock_gettime(CLOCK_REALTIME, &time2);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cputime2);

	uint64_t real_usec = get_passtime(&time1, &time2);
	uint64_t cpu_usec = get_passtime(&cputime1, &cputime2);

	(void) __sync_fetch_and_add(&time_get_real, real_usec);
	(void) __sync_fetch_and_add(&time_get_cpu, cpu_usec);
	printf("Thread 1: Got %d blocks from the directory\n", numBlocks);
}

void testremove(uint64_t seq) {
	uint64_t cguid = 1;
	SDF_container_type_t ctype = SDF_BLOCK_CONTAINER;
	struct timespec time1, time2, cputime1, cputime2;

	//calculate start time
	clock_gettime(CLOCK_REALTIME, &time1);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cputime1);

	for (int blockNum = 0; blockNum < numBlocks; blockNum++) {
		local_key_t *lkey = get_local_block_key(blockNum);
		DirEntry *entry = HomeDir_remove(homedir, cguid, ctype, lkey);
		if (entry) {
			//printf("fth %d remove NO.%d block\n", seq, blockNum);
			fflush(stdout);
			(void) __sync_fetch_and_add(&nremoves, 1);

		} else {
			free_local_key(lkey);
			continue;
		}

		//	(void) wait_and_reqq_dequeue(entry->q);

		free_local_key(lkey);
	}
	//calculate end time
	clock_gettime(CLOCK_REALTIME, &time2);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cputime2);

	uint64_t real_usec = get_passtime(&time1, &time2);
	uint64_t cpu_usec = get_passtime(&cputime1, &cputime2);

	(void) __sync_fetch_and_add(&time_remove_real, real_usec);
	(void) __sync_fetch_and_add(&time_remove_cpu, cpu_usec);

	printf("Thread 1: Removed %d blocks from the directory\n", numBlocks);
}

void testRoutine1(uint64_t arg) {
	int size = 1024;
	char str[size];

	int seq = __sync_fetch_and_add(&g_seq, 1);
	printf("\n%d fth begins\n", seq);
	HomeDir_printStats(homedir, str, size);
	printf("%s\n", str);

	testcreate(seq);
	testget(seq);
	testremove(seq);
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

#ifdef PERF_DAILY_RUN
	printf("<SDF_PROTOCOL_HOME:PERF:TEST1>\n");
	printf(
			"#ACTION#CREATE#Create %d times\nWallclocktime is %"PRIu64" usec, AvgWallclocktime=>%"PRIu64" usec\nCpuclocktime is %"PRIu64" usec, AvgCpuclocktime=>%"PRIu64" usec\n",
			nputs, time_create_real, time_create_real / nputs, time_create_cpu,
			time_create_cpu / nputs);

	printf(
			"#ACTION#GET#Get %d times\nWallclocktime is %"PRIu64" usec, AvgWallclocktime=>%"PRIu64" usec\nCpuclocktime is %"PRIu64" usec, AvgCpuclocktime=>%"PRIu64" usec\n",
			ngets, time_get_real, time_get_real / ngets, time_get_cpu,
			time_get_cpu / ngets);

	printf(
			"#ACTION#REMOVE#Remove %d times\nWallclocktime is %"PRIu64" usec, AvgWallclocktime=>%"PRIu64" usec\nCpuclocktime is %"PRIu64" usec, AvgCpuclocktime=>%"PRIu64" usec\n",
			nremoves, time_remove_real, time_remove_real / nremoves,
			time_remove_cpu, time_remove_cpu / nremoves);

	printf("</SDF_PROTOCOL_HOME:PERF:TEST1>\n");
#else
	printf(
			"Create %d times\nWallclocktime is %"PRIu64" usec, Avg Wallclocktime=>%"PRIu64" usec\nCpuclocktime is %"PRIu64" usec, Avg Cpuclocktime is %"PRIu64" usec\n",
			nputs, time_create_real, time_create_real / nputs, time_create_cpu,
			time_create_cpu / nputs);

	printf(
			"Get %d times\nWallclocktime is %"PRIu64" usec, Avg Wallclocktime is %"PRIu64" usec\nCpuclocktime is %"PRIu64" usec, Avg Cpuclocktime is %"PRIu64" usec\n",
			ngets, time_get_real, time_get_real / ngets, time_get_cpu,
			time_get_cpu / ngets);

	printf(
			"Remove %d times\nWallclocktime is %"PRIu64" usec, Avg Wallclocktime is %"PRIu64" usec\nCpuclocktime is %"PRIu64" usec, Avg Cpuclocktime is %"PRIu64" usec\n",
			nremoves, time_remove_real, time_remove_real / nremoves,
			time_remove_cpu, time_remove_cpu / nremoves);
#endif
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
	}
	if (argc > 1) {
		numBlocks = atoi(argv[1]);
	}
	if (argc > 2) {
		ncores = atoi(argv[2]);
		if (ncores > MAX_CORES)
			ncores = MAX_CORES;
	}
	if (argc > 3) {
		nthreads = atoi(argv[3]);
		if (nthreads > MAX_FTH_THREAD)
			nthreads = MAX_FTH_THREAD;
	}

	//for run once
	ncores = 1;
	nthreads = 1;

	plat_assert_always(nthreads > 0);
	plat_assert_always(ncores > 0);

	int ret = execute_test();
	printf("End of %s.\n", argv[0]);

	// plat_log_parse_arg("sdf/shared=debug");
	plat_shmem_alloc_get_stats(&g_end_sm_stats);
	print_sm_stats(g_init_sm_stats, g_end_sm_stats);
	return (ret);
}

fthThread_t* wait_and_reqq_dequeue(reqq_t *q) {
	fthThread_t *top;
	for (top = reqq_peek(q); top != fthSelf() && top != NULL; top
			= reqq_peek(q)) {
		fthYield(1);
	}

	if (!top) {
	//	printf("%"PRIu64": not in queue %s\n", (uint64_t)q, __FUNCTION__);
		return NULL;
	} else {
		fthWaitEl_t *wait = reqq_lock(q);
		top = reqq_dequeue(q);
		plat_assert_always(top);
		plat_assert_always(top == fthSelf());
		reqq_unlock(q, wait);
	}
	return top;
}

uint64_t get_passtime(struct timespec* start, struct timespec* end) {
	uint64_t sec, nsec, ret;
	if (start->tv_nsec > end->tv_nsec) {
		sec = end->tv_sec - 1 - start->tv_sec;
		nsec = end->tv_nsec + 1000000000 - start->tv_nsec;
	} else {
		sec = end->tv_sec - start->tv_sec;
		nsec = end->tv_nsec - start->tv_nsec;
	}

	ret = sec * 1000000 + nsec / 1000;
	return ret;
}
