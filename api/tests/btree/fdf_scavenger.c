/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * Flash Data Fabric - Sample program on API usage.
 *
 * (c) Copyright 2013 SanDisk, all rights reserved.
 * http://www.sandisk.com
 */

/*
 * This program helps in understanding the usage of ZS APIs. This program
 * reads properties file and sets some of the properties of ZS. It spawns
 * worker threads, where each thread creates its own container, writes
 * <key, object> pair.
 */ 

#include 	<stdio.h>
#include 	<stdlib.h>
#include 	<string.h>
#include 	<unistd.h>
#include 	<pthread.h>
#include	<zs.h>

#define	ZS_PROP_FILE		"conf/zs.prop"	//Configuration file
#define	THREADS			1 //Number of threads
#define CNTR_SIZE 		1	/* GB */
#define DATA_SIZE		20
#define NUM_OBJS			15000
int keynumlen = 7;

static char *base = "container";

void worker(void *arg);

int
main( )
{
	struct ZS_state		*zs_state;
	struct ZS_thread_state		*thd_state;
	ZS_cguid_t			cguid_list[THREADS] = {0};
	ZS_status_t			status;
	pthread_t			thread_id[THREADS];
	char				*version;
	int				indx;
	//uint32_t		t;
	uint32_t			ncg;
//	const char			*path;
	ZS_container_props_t		props;
	char				cname[32] = {0};
	ZS_cguid_t			cguid;
//	uint64_t		seq;
	//ZS_container_snapshots_t *snaps;

	//Create the container name based on thread id.
	sprintf(cname, "%s", base);

	//Get the version ZS the program running with.
	if (ZSGetVersion(&version) == ZS_SUCCESS) {
		printf("This is a sample program using ZS %s\n", version);
		ZSFreeBuffer(version);
	}

#if 0
	if (ZSLoadProperties(ZS_PROP_FILE) != ZS_SUCCESS) {
		printf("Couldn't load properties from %s. ZSInit()"
			" will use default properties or from file specified"
			" in ZS_PROPRTY_FILE environment variable if set.\n",
			ZS_PROP_FILE);
	} else {
		/*
		 * Propertie were loaded from file successfully, dont overwrite
		 * them by reading file specified in environment variable.
		 */
		unsetenv("ZS_PROPERTY_FILE");
	}

	path = ZSGetProperty("ZS_LICENSE_PATH", "Default path");
	if (path && (strcmp(path, "Default path") != 0)) {
		printf("License will be searched at: %s\n", path);
		ZSFreeBuffer((char *)path);
	}
#endif

	//Initialize ZS state.
	if ((status = ZSInit(&zs_state)) != ZS_SUCCESS) {
		printf("ZSInit failed with error %s\n", ZSStrError(status));
		return 1;
	}

	//Initialize per-thread ZS state for main thread.
	if ((status = ZSInitPerThreadState(zs_state, &thd_state)) != 
								ZS_SUCCESS) {
		printf("ZSInitPerThreadState failed with error %s\n",
							ZSStrError(status));
		return 1;
	}

	//Fill up property with default values.
	ZSLoadCntrPropDefaults(&props);

	//Set size of container to 256MB and retain other values.
	props.size_kb = CNTR_SIZE*10*1024 *1024;

	//Create container in read/write mode with properties specified.
	status = ZSOpenContainer(thd_state, cname, &props, 
				  ZS_CTNR_CREATE | ZS_CTNR_RW_MODE, &cguid);

	if (status == ZS_SUCCESS) {
		//If created successfully, get the container properties. 
		ZSGetContainerProps(thd_state, cguid, &props);
		printf("Container %s (cguid: %ld) created with size: %ldKB.\n", 
						cname, cguid, props.size_kb);
	} else {
		printf("ZSOpenContainer (of %s) failed with %s.\n", 
						cname, ZSStrError(status));
		return 1;
	}
	//Spawn worker threads.
	for (indx = 0; indx < THREADS; indx++) {
		pthread_create(&thread_id[indx], NULL, (void *)worker,
						(void *)zs_state);
	}

	//Wait for worker threads to complete.
	for (indx = 0; indx < THREADS; indx++) {
		pthread_join(thread_id[indx], NULL);
	}
	sleep(2);
//	while(1);

	//Flush all the cache contents created by workers.
	ZSFlushCache(thd_state);
	
	//Get the number of containers on the device.
	if ((status = ZSGetContainers(thd_state, cguid_list, &ncg)) !=
								ZS_SUCCESS) {
		printf("ZSGetContainers failed with error %s\n",
							ZSStrError(status));
		return 1;
	}
	printf("Number of containers created by workers: %d\n", ncg);
	ZSCloseContainer(thd_state, cguid);
	ZSDeleteContainer(thd_state, cguid);

	//Gracefuly shutdown ZS.
	ZSShutdown(zs_state);
	return (0);
}


/*
 * This routine is called by each pthread created by main thread. This creates
 * a container and writes <key, object> in it.
 */
void
worker(void *arg)
{
	struct ZS_state		*zs_state = (struct ZS_state *)arg;
	struct ZS_thread_state		*thd_state;
	char				*keyw, *dataw;
	char				cname[32] = {0};
	ZS_status_t			status;
	ZS_cguid_t			cguid;
	uint32_t			keylen;
	ZS_container_props_t		props;


	//Create the container name based on thread id.
	sprintf(cname, "%s%x", base,(int)pthread_self());

	//Initialize per thread state of ZS for this thread.
	ZSInitPerThreadState(zs_state, &thd_state);

	ZSLoadProperties(ZS_PROP_FILE);
	ZSLoadCntrPropDefaults(&props);


	//Create container in read/write mode with properties specified.
	status = ZSOpenContainer(thd_state, cname, &props,
			ZS_CTNR_CREATE | ZS_CTNR_RW_MODE, &cguid);

	keylen = strlen("key") + keynumlen + 1 + 8;
	keyw = (char *)malloc(keylen);
	dataw = (char *)malloc(keylen);
	for (int i=0; i <NUM_OBJS;i++) {
		sprintf(keyw, "key%07d%x", i, (int)pthread_self());
		if (strlen(keyw) != (keylen - 1)) {
			printf("Key format mismatch\n");
			return;
		}
		/*
		if (i==500) {
			sleep(1);
		}
		*/
		//Create initial data.
		status = ZSWriteObject(thd_state, cguid, keyw, keylen, dataw, DATA_SIZE, 0);
	}
	printf("Write done --- \n");
	free(dataw);

	for (int i=0; i <NUM_OBJS;i=i+1) {
		sprintf(keyw, "key%07d%x", i, (int)pthread_self());
		if (strlen(keyw) != (keylen - 1)) {
			printf("Key format mismatch\n");
			return;
		}
		status = ZSDeleteObject(thd_state, cguid, keyw, keylen);
        }

	status = ZSScavengeContainer(zs_state,cguid);
	if (status == ZS_SUCCESS)
		printf(" scavenger operation started %s\n", ZSStrError(status));


	//Release/Free per thread state.
	ZSReleasePerThreadState(&thd_state);
	return;
}
