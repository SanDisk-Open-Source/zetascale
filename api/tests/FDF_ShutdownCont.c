/****************************
#function : FDFShutdown
#author   : Vishal Kanaujia
#date     : 2013
 *****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "fdf.h"
#include <string.h>
#include <pthread.h>
#include <semaphore.h>

#define MAX_ITERATION  64

FILE                           *fp             = NULL;
static struct FDF_state        *fdf_state      = NULL;
static struct FDF_thread_state *fdf_thrd_state = NULL;

FDF_cguid_t                    cguid[MAX_ITERATION] = {0};


static FDF_status_t
pre_env()
{
	FDF_status_t ret = FDF_FAILURE;

	FDFSetProperty("SDF_REFORMAT", "1");


	ret = FDFInit(&fdf_state);

	if (FDF_SUCCESS != ret)	{
		fprintf(fp, "FDF initialization failed!\n");
	} else {
		fprintf(fp, "FDF initialization succeed!\n");

		ret = FDFInitPerThreadState(fdf_state, &fdf_thrd_state);
		if( FDF_SUCCESS == ret) {
			fprintf(fp, "FDF thread initialization succeed!\n");
		}
	}
	return ret;
}


static FDF_status_t
clear_env()
{
	fprintf(fp,"IN: clear env!\n");

	FDF_status_t ret = FDF_SUCCESS;

	ret = FDFShutdown(fdf_state);
	(void)FDFReleasePerThreadState(&fdf_thrd_state);

	fprintf(fp,"OUT: clear env!\n");

	return ret;
}


static FDF_status_t
OpenContainer(char *cname, uint32_t flag, uint32_t dura, uint64_t *cguid)
{
	fprintf(fp, "%s", "IN: FDFOpenContainer\n");
	FDF_status_t          ret;
	FDF_container_props_t p;

	ret = FDF_FAILURE;      
	(void)FDFLoadCntrPropDefaults(&p);

	p.durability_level = 0;
	p.fifo_mode = FDF_FALSE;
	p.num_shards = 1;
	p.persistent = FDF_TRUE;
	p.writethru = FDF_TRUE;
	p.evicting = FDF_TRUE;
	p.async_writes = FDF_TRUE;
	p.size_kb = 1024*1024;

	ret = FDFOpenContainer(fdf_thrd_state, cname, &p, flag,	cguid);

	fprintf(fp, "%s", "OUT: FDFOpenContainer\n");
	return ret;
}


static FDF_status_t
DeleteContainer(FDF_cguid_t cid)
{
	FDF_status_t ret;
	ret = FDFDeleteContainer(fdf_thrd_state, cid);
	fprintf(fp,"FDFDeleteContainer : %s\n",FDFStrError(ret));
	return ret;
}



/***************** test ******************/

pthread_cond_t cv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * Flag to manage wait between threads
 */
int x = 0;


static void*
container_deletes()
{
	FDF_status_t ret = FDF_SUCCESS;

	fprintf(fp, "%s", "Inside container_deletes\n");

	uint32_t i = 0;
	uint32_t flag = FDF_CTNR_CREATE; //FDF_CTNR_RO_MODE
	uint32_t dura = FDF_DURABILITY_PERIODIC;

	char cname[1024] = {'\0'};

	/*
	 * Create  containers
	 */
	for (i = 0; i < MAX_ITERATION; i++) {
		/*
		 * Create a new container
		 */
		sprintf(cname, "test_%d", i);
		ret = OpenContainer(cname, flag, dura, &cguid[i]);
		if (FDF_SUCCESS != ret) {
			fprintf(fp, "io failed with err=%s\n", FDFStrError(ret));
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
			pthread_mutex_lock(&mutex);
			if (0 == x) {
				x = 1;
				fprintf(fp, "%s", "Wake up shutdown thread\n");
				pthread_cond_signal(&cv);
			}
			pthread_mutex_unlock(&mutex);
		}

		sprintf(cname, "test_%d", i);
		ret = DeleteContainer(cguid[i]);
		if (FDF_SUCCESS != ret) {
			fprintf(fp, "DeleteContainer failed with err=%s\n", FDFStrError(ret));
			goto exit_container_deletes;
		}
	}
exit_container_deletes:
	return 0;
}


static int32_t
mgr2()
{
	FDF_status_t ret = FDF_SUCCESS;
	pthread_t cont_thread;

	int ret1 = 0;

	fprintf(fp, "%s", "****** Test 2 *****\n");

	/*
	 * Init FDF
	 */
	ret = pre_env();

	if (FDF_SUCCESS != ret) {
		fprintf(fp, "pre_env failed with err=%s\n", FDFStrError(ret));
		goto exit_mgr2;
	}

	ret1 = pthread_create(&cont_thread, NULL, container_deletes, NULL);

	pthread_mutex_lock(&mutex);
	if (0 == x) {
		pthread_cond_wait(&cv, &mutex);
		fprintf(fp, "%s", "Shutdown thread got signal from deletion thread\n");
	}
	pthread_mutex_unlock(&mutex);

	/*
	 * Start graceful shutdown
	 */
	fprintf(fp, "%s", "Starting graceful shutdown\n");
	ret = clear_env();
	if (FDF_SUCCESS != ret) {
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

	if((fp = fopen("FDF_ShutdownCont.log", "w+")) == 0)
	{
		fprintf(stderr, "open log file failed!.\n");
		return -1;
	}

	/*
	 * Init FDF, creates containers, performs I/O and 
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
