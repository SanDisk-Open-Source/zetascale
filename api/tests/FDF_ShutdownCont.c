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

/****************************
#function : ZSShutdown
#author   : Vishal Kanaujia
#date     : 2013
 *****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "zs.h"
#include <string.h>
#include <pthread.h>
#include <semaphore.h>

#define MAX_ITERATION  32

FILE                           *fp             = NULL;
static struct ZS_state        *zs_state      = NULL;
static struct ZS_thread_state *zs_thrd_state1 = NULL;

ZS_cguid_t                    cguid[MAX_ITERATION] = {0};


static ZS_status_t
pre_env()
{
	ZS_status_t ret = ZS_FAILURE;

	ZSSetProperty("SDF_REFORMAT", "1");
	ZSSetProperty("ZS_BTREE_PARALLEL_FLUSH", "1");
	ZSSetProperty("ZS_BTREE_SYNC_THREADS", "2");


	ret = ZSInit(&zs_state);

	if (ZS_SUCCESS != ret)	{
		fprintf(fp, "ZS initialization failed!\n");
	} else {
		fprintf(fp, "ZS initialization succeed!\n");

		ret = ZSInitPerThreadState(zs_state, &zs_thrd_state1);
		if( ZS_SUCCESS == ret) {
			fprintf(fp, "ZS thread initialization succeed!\n");
		}
	}
	return ret;
}


static ZS_status_t
clear_env()
{
	fprintf(fp,"IN: clear env!\n");

	ZS_status_t ret = ZS_SUCCESS;

	(void)ZSReleasePerThreadState(&zs_thrd_state1);
	ret = ZSShutdown(zs_state);

	fprintf(fp,"OUT: clear env!\n");

	return ret;
}


static ZS_status_t
OpenContainer(struct ZS_thread_state *zs_thrd_state, char *cname, uint32_t flag, uint32_t dura, uint64_t *cguid)
{
	fprintf(fp, "%s", "IN: ZSOpenContainer\n");
	ZS_status_t          ret;
	ZS_container_props_t p;

	ret = ZS_FAILURE;      
	(void)ZSLoadCntrPropDefaults(&p);

	p.durability_level = 0;
	p.fifo_mode = ZS_FALSE;
	p.num_shards = 1;
	p.persistent = ZS_TRUE;
	p.writethru = ZS_TRUE;
	p.evicting = ZS_TRUE;
	p.async_writes = ZS_TRUE;
	p.size_kb = 1024*1024;

	ret = ZSOpenContainer(zs_thrd_state, cname, &p, flag,	cguid);

	fprintf(fp, "%s", "OUT: ZSOpenContainer\n");
	return ret;
}


static ZS_status_t
DeleteContainer(struct ZS_thread_state *zs_thrd_state, ZS_cguid_t cid)
{
	ZS_status_t ret;
	ret = ZSDeleteContainer(zs_thrd_state, cid);
	fprintf(fp,"ZSDeleteContainer : %s\n",ZSStrError(ret));
	return ret;
}



/***************** test ******************/

pthread_cond_t pcv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t pmutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Flag to manage wait between threads
 */
int x = 0;


static void*
container_deletes()
{
	ZS_status_t ret = ZS_SUCCESS;
	struct ZS_thread_state *zs_thrd_state2 = NULL;

	fprintf(fp, "%s", "Inside container_deletes\n");

	uint32_t i = 0;
	uint32_t flag = ZS_CTNR_CREATE; //ZS_CTNR_RO_MODE
	uint32_t dura = ZS_DURABILITY_PERIODIC;

	char cname[1024] = {'\0'};

	ret = ZSInitPerThreadState(zs_state, &zs_thrd_state2);
	if( ZS_SUCCESS == ret) {
		fprintf(fp, "ZS thread initialization succeed!\n");
	}
	/*
	 * Create  containers
	 */
	for (i = 0; i < MAX_ITERATION; i++) {
		/*
		 * Create a new container
		 */
		sprintf(cname, "test_%d", i);
		ret = OpenContainer(zs_thrd_state2, cname, flag, dura, &cguid[i]);
		if (ZS_SUCCESS != ret) {
			fprintf(fp, "io failed with err=%s\n", ZSStrError(ret));
			goto exit_container_deletes;
		}
	}

	/*
	 * Delete containers
	 */
	for (i = 0; i < MAX_ITERATION; i++) {
		/*
		 * Signal for shutdown when half of the container are
		 * requested to get deleted
		 */
		if (MAX_ITERATION/2 == i) {
			pthread_mutex_lock(&pmutex);
			if (0 == x) {
				x = 1;
				fprintf(fp, "%s", "Wake up shutdown thread\n");
				pthread_cond_signal(&pcv);
			}
			pthread_mutex_unlock(&pmutex);
		}

		sprintf(cname, "test_%d", i);
		ret = DeleteContainer(zs_thrd_state2, cguid[i]);
		if (ZS_SUCCESS != ret) {
			fprintf(fp, "DeleteContainer failed with err=%s\n", ZSStrError(ret));
			goto exit_container_deletes;
		}
	}
exit_container_deletes:
	pthread_mutex_lock(&pmutex);
	if (0 == x) {
		x = 1;
		fprintf(fp, "%s", "Wake up shutdown thread\n");
		pthread_cond_signal(&pcv);
	}
	pthread_mutex_unlock(&pmutex);
	return 0;
}


static int32_t
mgr2()
{
	ZS_status_t ret = ZS_SUCCESS;
	pthread_t cont_thread;

	int ret1 = 0;

	fprintf(fp, "%s", "****** Test 2 *****\n");

	/*
	 * Init ZS
	 */
	ret = pre_env();

	if (ZS_SUCCESS != ret) {
		fprintf(fp, "pre_env failed with err=%s\n", ZSStrError(ret));
		goto exit_mgr2;
	}

	ret1 = pthread_create(&cont_thread, NULL, container_deletes, NULL);

	pthread_mutex_lock(&pmutex);
	if (0 == x) {
		pthread_cond_wait(&pcv, &pmutex);
		fprintf(fp, "%s", "Shutdown thread got signal from deletion thread\n");
	}
	pthread_mutex_unlock(&pmutex);

	/*
	 * Start graceful shutdown
	 */
	fprintf(fp, "%s", "Starting graceful shutdown\n");
	ret = clear_env();
	if (ZS_SUCCESS != ret) {
		return 1;	
	}

	ret1 = pthread_join(cont_thread, NULL);
	if (ret1) {
		return 1;	
	}

exit_mgr2:
	return 0;
}


/****** main function ******/

int32_t
main(int argc, char *argv[]) 
{
	int32_t ret = 1;

	if((fp = fopen("ZS_ShutdownCont.log", "w+")) == 0)
	{
		fprintf(stderr, "open log file failed!.\n");
		return -1;
	}

	/*
	 * Init ZS, creates containers, performs I/O and 
	 * random graceful shutdown.
	 * Perform container and object operations after
	 * shutdown.
	 */
	ret = mgr2();

	/*
	 * Clean up
	 */
	fclose(fp);

	fprintf(stderr, "Test Result:%s\n", (!ret ? "Passed":"Failed"));
	return ret;
}
