/****************************
#function : FDFShutdown
#author   : Vishal Kanaujia
#date     : 2013
 *****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "fdf.h"
#include <string.h>
#include <semaphore.h>

#define MAX_ITERATION 256

FILE                           *fp             = NULL;
static struct FDF_state        *fdf_state      = NULL;
static struct FDF_thread_state *fdf_thrd_state = NULL;

FDF_cguid_t cguid = 0;


static FDF_status_t
pre_env()
{
	FDF_status_t ret = FDF_FAILURE;

	FDFSetProperty("SDF_REFORMAT", "1");

	ret = FDFInit(&fdf_state);

	if (FDF_SUCCESS != ret)
	{
		fprintf(fp, "FDF initialization failed!\n");
	} else {
		fprintf(fp, "FDF initialization succeed!\n");

		ret = FDFInitPerThreadState(fdf_state, &fdf_thrd_state);
		if (FDF_SUCCESS == ret)
		{
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

	(void)FDFReleasePerThreadState(&fdf_thrd_state);
	ret = FDFShutdown(fdf_state);

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
CloseContainer(FDF_cguid_t cid)
{
	FDF_status_t ret;
	ret = FDFCloseContainer(fdf_thrd_state, cid);
	fprintf(fp,"FDFCloseContainer : %s\n",FDFStrError(ret));
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


static FDF_status_t
WriteObject(FDF_cguid_t cid, char *key,char *data, uint32_t flags)
{
	FDF_status_t ret;
	ret = FDFWriteObject(fdf_thrd_state, cid, key, strlen(key)+1, data, strlen(data)+1, flags);
	fprintf(fp,"FDFWriteObject : %s\n",FDFStrError(ret));
	return ret;
}


static FDF_status_t
ReadObject(FDF_cguid_t cid, char *key, char **data, uint64_t *datalen)
{
	FDF_status_t ret;
	ret = FDFReadObject(fdf_thrd_state, cid, key, strlen(key)+1, data, datalen);
	fprintf(fp,"FDFReadObject : %s\n",FDFStrError(ret));
	return ret;
}


static FDF_status_t
FlushObject(FDF_cguid_t cid,char *key)
{
	FDF_status_t ret;
	ret = FDFFlushObject(fdf_thrd_state,cid,key,strlen(key)+1);
	fprintf(fp,"FDFFlushObject : %s\n",FDFStrError(ret));
	return ret;
}


/***************** test ******************/


static FDF_status_t
container_ops(char *cname, uint64_t flag, uint64_t dura)
{
	FDF_status_t ret = FDF_SUCCESS;

	ret = OpenContainer(cname, flag, dura, &cguid);
	if (FDF_SUCCESS != ret) {
		fprintf(fp, "Open container failed with: %s\n", FDFStrError(ret));
		goto exit_container_ops;
	}

	ret = CloseContainer(cguid);
	if (FDF_SUCCESS != ret) {
		fprintf(fp, "Close container failed with: %s\n", FDFStrError(ret));
		goto exit_container_ops;
	}

exit_container_ops:
	return ret;	
}


static FDF_status_t
io()
{
	FDF_status_t ret;

	uint32_t wr_flags = 0;
	uint32_t i = 0;

	char key[256] = {'\0'};
	char value[256] = {'\0'};

	char *data = NULL;
	uint64_t len = 0;

	fprintf(fp,"****** io started ******\n");

	fprintf(fp,"****** Writing objects *******\n");
	for (i = 0; i < MAX_ITERATION; i++) {
		snprintf(key, 255, "%s_%d", "key", i);
		snprintf(value, 255, "%s_%d", "value", i);
		if(FDF_SUCCESS == (ret = WriteObject(cguid, key, value, wr_flags)))
		{
			fprintf(fp,"write sucessful: key%s\n", key);
		} else {
			fprintf(fp,"write failed: key=%s error=%s\n", key, FDFStrError(ret));
			goto exit_io;
		}

		/*
		 * Flush few object
		 */
		if (0 == (i % 2)) {
			if(FDF_SUCCESS == (ret = FlushObject(cguid, key)))
			{
				fprintf(fp,"flush sucessful: key%s\n", key);
			} else {
				fprintf(fp,"flush failed: key=%s error=%s\n", key, FDFStrError(ret));
				goto exit_io;
			}
		}
	}

	/*
	 * Issue read
	 */
	fprintf(fp,"****** Reading objects *******\n");

	for (i = 0; i < MAX_ITERATION; i++) {
		snprintf(key, 255, "%s_%d", "key", i);
		if(FDF_SUCCESS == (ret = ReadObject(cguid, key, &data, &len)))
		{
			fprintf(fp,"read successful: key=%s data=%s\n", key, data);
		} else {
			fprintf(fp,"read failed: read key= %s error=%s\n", key, FDFStrError(ret));
			goto exit_io;
		}
	}
exit_io:
	return ret;
}


static int32_t
mgr1()
{
	FDF_status_t ret = FDF_SUCCESS;

	fprintf(fp, "%s", "****** Test 1 *****\n");

	/*
	 * Init FDF
	 */
	ret = pre_env();

	if (FDF_SUCCESS != ret) {
		fprintf(fp, "pre_env failed with err=%s\n", FDFStrError(ret));
		goto exit_mgr1;
	}

	/*
	 * Create a container
	 */
	uint32_t flag = FDF_CTNR_CREATE; //FDF_CTNR_RO_MODE
	uint32_t dura = FDF_DURABILITY_PERIODIC;
	char cname[1024] = {'\0'};

	uint32_t i;
	int32_t r = 0;
	int32_t count = 0;

	sprintf(cname, "test_%d", 0);

	do {
		ret = OpenContainer(cname, flag, dura, &cguid);

		/*
		 * Start high frequency I/O
		 */
		for (i = 0; i < MAX_ITERATION; i++)
		{
			r = rand() % 16 + 1;
			if (0 == (r % 7)) {
				fprintf(fp, "Random failure at %d\n", r);
				goto exit_mgr1;	
			}

			ret = io();
			if (FDF_SUCCESS != ret) {
				fprintf(fp, "io failed with err=%s\n", FDFStrError(ret));
				goto exit_mgr1;
			}
		}

		ret = DeleteContainer(cguid);
		if (FDF_SUCCESS != ret) {
			fprintf(fp, "DeleteContainer failed with err=%s\n", FDFStrError(ret));
			goto exit_mgr1;
		}

		count++;
	} while (count < 5);

exit_mgr1:
	/*
	 * Start graceful shutdown
	 */
	fprintf(fp, "%s", "Starting graceful shutdown\n");
	ret = clear_env();
	fprintf(fp, "Shutdown returned %s:\n", FDFStrError(ret));

	if (FDF_SUCCESS != ret) {
		fprintf(fp, "%s", "Graceful shutdown test1 failed\n");
		return 1;
	}

	/*
	 * Attempt another shutdown
	 */
	fprintf(fp, "%s", "Attempt another shutdown\n");
	ret = FDFShutdown(fdf_state);
	fprintf(fp, "Next shutdown returned %s:\n", FDFStrError(ret));

	if (FDF_SUCCESS == ret) {
		fprintf(fp, "%s", "Next shutdown test2 failed\n");	
		return 1;
	}

    /*
	 * Perform I/O and container ops after shutdown
	 * It should fail
	 */
	ret = io();
	fprintf(fp, "Post shutdown: I/O failed with %s\n", FDFStrError(ret));

	if (FDF_SUCCESS == ret) {
		fprintf(fp, "%s", "Post shutdown: I/O test3 failed\n");	
		return 1;
	}

	/*
	 * Perform container ops: It should fail
	 */
	ret = container_ops(cname, flag, dura);
	fprintf(fp, "Post shutdown: container ops returns %s\n", FDFStrError(ret));

	if (FDF_SUCCESS == ret) {
		fprintf(fp, "%s", "Post shutdown: container ops test4 failed\n");	
		return 1;
	}

	return 0;
}


/****** main function ******/

int32_t
main(int argc, char *argv[]) 
{
	int32_t ret = 0;

	if((fp = fopen("FDF_ShutdownIO.log", "w+")) == 0)
	{
		fprintf(stderr, "open log file failed!.\n");
		return -1;
	}

	/*
	 * Init FDF, creates containers, performs I/O and 
	 * random graceful shutdown.
	 */
	ret = mgr1();

	/*
	 * Clean up
	 */
	fclose(fp);

	fprintf(stderr, "Test Completed\n");
	fprintf(stderr, "Test Result:%s\n", (!ret ? "Passed":"Failed"));
	return ret;
}
