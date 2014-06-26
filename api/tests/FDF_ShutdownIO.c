/****************************
#function : ZSShutdown
#author   : Vishal Kanaujia
#date     : 2013
 *****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "zs.h"
#include <string.h>
#include <semaphore.h>

#define MAX_ITERATION 256

FILE                           *fp             = NULL;
static struct ZS_state        *zs_state      = NULL;
static struct ZS_thread_state *zs_thrd_state = NULL;

ZS_cguid_t cguid = 0;


static ZS_status_t
pre_env()
{
	ZS_status_t ret = ZS_FAILURE;

	ZSSetProperty("SDF_REFORMAT", "1");

	ret = ZSInit(&zs_state);

	if (ZS_SUCCESS != ret)
	{
		fprintf(fp, "ZS initialization failed!\n");
	} else {
		fprintf(fp, "ZS initialization succeed!\n");

		ret = ZSInitPerThreadState(zs_state, &zs_thrd_state);
		if (ZS_SUCCESS == ret)
		{
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

	(void)ZSReleasePerThreadState(&zs_thrd_state);
	ret = ZSShutdown(zs_state);

	fprintf(fp,"OUT: clear env!\n");

	return ret;
}


static ZS_status_t
OpenContainer(char *cname, uint32_t flag, uint32_t dura, uint64_t *cguid)
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
CloseContainer(ZS_cguid_t cid)
{
	ZS_status_t ret;
	ret = ZSCloseContainer(zs_thrd_state, cid);
	fprintf(fp,"ZSCloseContainer : %s\n",ZSStrError(ret));
	return ret;
}


static ZS_status_t
DeleteContainer(ZS_cguid_t cid)
{
	ZS_status_t ret;
	ret = ZSDeleteContainer(zs_thrd_state, cid);
	fprintf(fp,"ZSDeleteContainer : %s\n",ZSStrError(ret));
	return ret;
}


static ZS_status_t
WriteObject(ZS_cguid_t cid, char *key,char *data, uint32_t flags)
{
	ZS_status_t ret;
	ret = ZSWriteObject(zs_thrd_state, cid, key, strlen(key)+1, data, strlen(data)+1, flags);
	fprintf(fp,"ZSWriteObject : %s\n",ZSStrError(ret));
	return ret;
}


static ZS_status_t
ReadObject(ZS_cguid_t cid, char *key, char **data, uint64_t *datalen)
{
	ZS_status_t ret;
	ret = ZSReadObject(zs_thrd_state, cid, key, strlen(key)+1, data, datalen);
	fprintf(fp,"ZSReadObject : %s\n",ZSStrError(ret));
	return ret;
}


static ZS_status_t
FlushObject(ZS_cguid_t cid,char *key)
{
	ZS_status_t ret;
	ret = ZSFlushObject(zs_thrd_state,cid,key,strlen(key)+1);
	fprintf(fp,"ZSFlushObject : %s\n",ZSStrError(ret));
	return ret;
}


/***************** test ******************/


static ZS_status_t
container_ops(char *cname, uint64_t flag, uint64_t dura)
{
	ZS_status_t ret = ZS_SUCCESS;

	ret = OpenContainer(cname, flag, dura, &cguid);
	if (ZS_SUCCESS != ret) {
		fprintf(fp, "Open container failed with: %s\n", ZSStrError(ret));
		goto exit_container_ops;
	}

	ret = CloseContainer(cguid);
	if (ZS_SUCCESS != ret) {
		fprintf(fp, "Close container failed with: %s\n", ZSStrError(ret));
		goto exit_container_ops;
	}

exit_container_ops:
	return ret;	
}


static ZS_status_t
io()
{
	ZS_status_t ret;

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
		if(ZS_SUCCESS == (ret = WriteObject(cguid, key, value, wr_flags)))
		{
			fprintf(fp,"write sucessful: key%s\n", key);
		} else {
			fprintf(fp,"write failed: key=%s error=%s\n", key, ZSStrError(ret));
			goto exit_io;
		}

		/*
		 * Flush few object
		 */
		if (0 == (i % 2)) {
			if(ZS_SUCCESS == (ret = FlushObject(cguid, key)))
			{
				fprintf(fp,"flush sucessful: key%s\n", key);
			} else {
				fprintf(fp,"flush failed: key=%s error=%s\n", key, ZSStrError(ret));
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
		if(ZS_SUCCESS == (ret = ReadObject(cguid, key, &data, &len)))
		{
			fprintf(fp,"read successful: key=%s data=%s\n", key, data);
		} else {
			fprintf(fp,"read failed: read key= %s error=%s\n", key, ZSStrError(ret));
			goto exit_io;
		}
	}
exit_io:
	return ret;
}


static int32_t
mgr1()
{
	ZS_status_t ret = ZS_SUCCESS;

	fprintf(fp, "%s", "****** Test 1 *****\n");

	/*
	 * Init ZS
	 */
	ret = pre_env();

	if (ZS_SUCCESS != ret) {
		fprintf(fp, "pre_env failed with err=%s\n", ZSStrError(ret));
		return -1;
	}

	/*
	 * Create a container
	 */
	uint32_t flag = ZS_CTNR_CREATE; //ZS_CTNR_RO_MODE
	uint32_t dura = ZS_DURABILITY_PERIODIC;
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
			if (ZS_SUCCESS != ret) {
				fprintf(fp, "io failed with err=%s\n", ZSStrError(ret));
				goto exit_mgr1;
			}
		}

		ret = DeleteContainer(cguid);
		if (ZS_SUCCESS != ret) {
			fprintf(fp, "DeleteContainer failed with err=%s\n", ZSStrError(ret));
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
	fprintf(fp, "Shutdown returned %s:\n", ZSStrError(ret));

	if (ZS_SUCCESS != ret) {
		fprintf(fp, "%s", "Graceful shutdown test1 failed\n");
		return 1;
	}

	/*
	 * Attempt another shutdown
	 */
	fprintf(fp, "%s", "Attempt another shutdown\n");
	ret = ZSShutdown(zs_state);
	fprintf(fp, "Next shutdown returned %s:\n", ZSStrError(ret));

	if (ZS_SUCCESS == ret) {
		fprintf(fp, "%s", "Next shutdown test2 failed\n");	
		return 1;
	}

    /*
	 * Perform I/O and container ops after shutdown
	 * It should fail
	 */
	ret = io();
	fprintf(fp, "Post shutdown: I/O failed with %s\n", ZSStrError(ret));

	if (ZS_SUCCESS == ret) {
		fprintf(fp, "%s", "Post shutdown: I/O test3 failed\n");	
		return 1;
	}

	/*
	 * Perform container ops: It should fail
	 */
	ret = container_ops(cname, flag, dura);
	fprintf(fp, "Post shutdown: container ops returns %s\n", ZSStrError(ret));

	if (ZS_SUCCESS == ret) {
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

	if((fp = fopen("ZS_ShutdownIO.log", "w+")) == 0)
	{
		fprintf(stderr, "open log file failed!.\n");
		return -1;
	}

	/*
	 * Init ZS, creates containers, performs I/O and 
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
