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

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include "zs.h"
#include "../test.h"

static ZS_cguid_t  cguid_shared;
static long size = 1024 * 1024 * 1024;
/* Following used to pass parameter to threads */
static int threads = 1, count, start_id, step = 1, obj_size, op;

static int max_512 = 1900544; /* Maximum number of 512 SLABS in 1G */

int hw_dur = 0;

enum {
	SET = 0,
	GET,
	DEL,
	ENUM
};

void* worker(void *arg)
{
	t(zs_init_thread(), ZS_SUCCESS);

	switch(op)
	{
		case SET:
			t(set_objs(cguid_shared, (long)arg, obj_size, start_id, count, step), count);
			break;
		case GET:
			t(get_objs(cguid_shared, (long)arg, start_id, count, step), count);
			break;
		case DEL:
			t(del_objs(cguid_shared, (long)arg, start_id, count, step), count);
			break;
		case ENUM:
			t(enum_objs(cguid_shared), count);
			break;
	}

	return 0;
}

void check_stat()
{
	ZS_stats_t stats;

	zs_get_container_stats(cguid_shared, &stats);

#define PRINT_STAT(a) \
	fprintf(stderr, "slab_gc_stat: " #a "(%d) = %ld\n", a, stats.flash_stats[a]);

	PRINT_STAT(ZS_FLASH_STATS_NUM_DATA_FSYNCS);
	PRINT_STAT(ZS_FLASH_STATS_NUM_DATA_WRITES);

	PRINT_STAT(ZS_FLASH_STATS_NUM_LOG_FSYNCS);
	PRINT_STAT(ZS_FLASH_STATS_NUM_LOG_WRITES);

	if(!hw_dur) {
		assert(stats.flash_stats[ZS_FLASH_STATS_NUM_LOG_FSYNCS] == 0);
		assert(stats.flash_stats[ZS_FLASH_STATS_NUM_LOG_WRITES] == 0);

		assert(stats.flash_stats[ZS_FLASH_STATS_NUM_DATA_FSYNCS] < 5000);
		assert(stats.flash_stats[ZS_FLASH_STATS_NUM_DATA_WRITES] > max_512);
	}
	else
	{
		assert(stats.flash_stats[ZS_FLASH_STATS_NUM_LOG_FSYNCS] > 10000);
		assert(stats.flash_stats[ZS_FLASH_STATS_NUM_LOG_WRITES] > max_512);

		assert(stats.flash_stats[ZS_FLASH_STATS_NUM_DATA_FSYNCS] > 5000);
		assert(stats.flash_stats[ZS_FLASH_STATS_NUM_DATA_WRITES] > max_512);

		assert(stats.flash_stats[ZS_FLASH_STATS_NUM_DATA_FSYNCS] < stats.flash_stats[ZS_FLASH_STATS_NUM_DATA_WRITES]);
		assert(stats.flash_stats[ZS_FLASH_STATS_NUM_LOG_FSYNCS] < stats.flash_stats[ZS_FLASH_STATS_NUM_LOG_WRITES]);
	}
}

void start_threads(int count, void* (*worker)(void*))
{
	pthread_t thread_id[threads];

	int i;

	for(i = 0; i < threads; i++)
		pthread_create(&thread_id[i], NULL, worker, (void*)(long)i);

	for(i = 0; i < threads; i++)
		pthread_join(thread_id[i], NULL);
}

int main(int argc, char *argv[])
{
	ZSLoadProperties(getenv("ZS_PROPERTY_FILE"));
	ZSSetProperty("ZS_FLASH_SIZE", "3");
	ZSSetProperty("ZS_COMPRESSION", "0");
	ZSSetProperty("ZS_BLOCK_SIZE", "512");
	ZSSetProperty("ZS_FLASH_FILENAME", "/dev/shm/schooner%d");
	ZSSetProperty("ZS_LOG_FLUSH_DIR", "/dev/shm");
	ZSSetProperty("ZS_O_DIRECT", "0");

	unsetenv("ZS_PROPERTY_FILE");

	size = 1 * 1024 * 1024 * 1024;

	if(argc > 1)
		hw_dur = 1;

	ZSSetProperty("ZS_GROUP_COMMIT", hw_dur ? "1" : "0");

	t(zs_init(), ZS_SUCCESS);

	t(zs_init_thread(), ZS_SUCCESS);

	t(zs_create_container_dur("container-slab-gc", size,
				hw_dur ? ZS_DURABILITY_HW_CRASH_SAFE : ZS_DURABILITY_SW_CRASH_SAFE,
				&cguid_shared), ZS_SUCCESS);

	threads = 16; /* Used in threads */

	/* Fill container with 512 SLABs using 16 threads */
	count = max_512 / threads;
	start_id = 0;
	step = 1;
	op = SET;
	obj_size = 400;
	fprintf(stderr, "SET(before): count=%d\n", threads * count / step);
	start_threads(threads, worker);
	fprintf(stderr, "SET(after): count=%d\n", threads * count / step);

	count = max_512 / threads;
	start_id = 0;
	step = 1;
	op = GET;
	fprintf(stderr, "GET(before): count=%d\n", threads * count / step);
	start_threads(threads, worker);
	fprintf(stderr, "GET(after): count=%d\n", threads * count / step);

	check_stat();

	zs_flush_container(cguid_shared);

	fprintf(stderr, "All tests passed\n");

	zs_shutdown();

	return(0);
}

