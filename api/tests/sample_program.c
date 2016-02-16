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
#include	"zs.h"

#define	ZS_PROP_FILE		"config/zs_sample.prop"	//Configuration file
#define	THREADS			1 //Number of threads
#define CNTR_SIZE 		1	/* GB */
#define DATA_SIZE	100
#define NUM_OBJS			50000
int keynumlen = 47;

static const char *base = "container";

void worker(void *arg);

int
main( )
{
	struct ZS_state		*zs_state;
	struct ZS_thread_state		*thd_state;
	ZS_cguid_t			cguid_list[THREADS] = {0};
	ZS_status_t			status, status2;
	pthread_t			thread_id[THREADS];
	char				*version;
	uint32_t		t, indx;
	uint32_t			ncg = 0;
	const char			*path;
	ZS_container_props_t		props;
	char				cname[32] = {0};
	ZS_cguid_t			cguid;
	uint64_t		seq = 0;
	uint64_t		seq1 = 0;
	ZS_container_snapshots_t *snaps;

	//Create the container name based on thread id.
	sprintf(cname, "%s", base);

	//Get the version ZS the program running with.
	if (ZSGetVersion(&version) == ZS_SUCCESS) {
		printf("This is a sample program using ZS %s\n", version);
		ZSFreeBuffer(version);
	}
#if 0
	if (ZSLoadProperties(ZS_PROPERTY_FILE) != ZS_SUCCESS) {
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
#endif

	path = ZSGetProperty("ZS_LICENSE_PATH", "Default path");
	if (path && (strcmp(path, "Default path") != 0)) {
		printf("License will be searched at: %s\n", path);
		ZSFreeBuffer((char *)path);
	}

	//Initialize ZS state.
	if ((status = ZSInit(&zs_state)) != ZS_SUCCESS) {
		printf("ZSInit failed with error %s\n", ZSStrError(status));
		return 1;
	}

	//Initialize per-thread ZS state for main thread.
	if ((status = ZSInitPerThreadState(zs_state, &thd_state)) != ZS_SUCCESS) {
		printf("ZSInitPerThreadState failed with error %s\n",
							ZSStrError(status));
		return 1;
	}

	//Fill up property with default values.
	ZSLoadCntrPropDefaults(&props);

	//Set size of container to 256MB and retain other values.
	props.size_kb = CNTR_SIZE*6*1024 *1024;
	props.flash_only = (ZS_boolean_t)0;

	//Create container in read/write mode with properties specified.
	status = ZSOpenContainer(thd_state, cname, &props,
			ZS_CTNR_CREATE | ZS_CTNR_RW_MODE, &cguid);
#if 0
	for (int i=0; i< 5000; i++) {
		status = ZSWriteObject(thd_state, cguid, "test", 4, "test", 4, 0);
	}

	ZSCloseContainer(thd_state, cguid);
	return 0;
#endif
	if (status == ZS_SUCCESS) {
		//If created successfully, get the container properties.
		props.size_kb = 0;
		status = ZSGetContainerProps(thd_state, cguid, &props);
		printf("Container %s (cguid: %ld) created with size: %ldKB.\n",
						cname, cguid, props.size_kb);
	} else {
		printf("ZSOpenContainer (of %s) failed with %s.\n",
						cname, ZSStrError(status));
		return 1;
	}
	//Spawn worker threads.
	for (indx = 0; indx <1; indx++) {
		pthread_create(&thread_id[indx], NULL, (void*(*)(void*))worker,
						(void *)zs_state);
	}

	//Wait for worker threads to complete.
	for (indx = 0; indx < 1; indx++) {
		pthread_join(thread_id[indx], NULL);
	}
	goto out;
	return (0);
	sleep(2);
	status = ZSWriteObject(thd_state, cguid, (char*)"test", 4, (char*)"test",
			4, 0);
	fprintf(stderr, "Write data status: %s <--------------\n",
			ZSStrError(status));
	for (indx=0; indx < 2; indx++) {
		status2 = ZSCreateContainerSnapshot(thd_state, cguid, &seq);
		if ((status2 == ZS_SUCCESS) && indx == 0) {
			seq1 = seq;
			status = ZSWriteObject(thd_state, cguid, (char*)"test", 4,
					(char*)"testabc", 7, ZS_WRITE_MUST_EXIST);
			fprintf(stderr, "Update data status: %s <--------------\n",
					ZSStrError(status));
		}
	}
	if (status2 == ZS_SUCCESS) {
		ZSGetContainerSnapshots(thd_state, cguid, &t, &snaps);
		fprintf(stderr, "No of snaps: %d\n", (int)t);
		for (indx = 0; indx < t; indx++) {
			fprintf(stderr, "snap[%d]: timestamp:%" PRId64" seqno:%" PRId64
					"\n", (int)indx, snaps[indx].timestamp, snaps[indx].seqno);
		}
		ZSDeleteContainerSnapshot(thd_state, cguid, seq1);
		ZSGetContainerSnapshots(thd_state, cguid, &t, &snaps);
		fprintf(stderr, "No of snaps: %d\n", (int)t);
		for (indx = 0; indx < t; indx++) {
			fprintf(stderr, "snap[%d]: timestamp:%" PRId64" seqno:%" PRId64
					"\n", (int)indx, snaps[indx].timestamp, snaps[indx].seqno);
		}
	}

	//Flush all the cache contents created by workers.
	ZSFlushCache(thd_state);

	//Get the number of containers on the device.
	if ((status = ZSGetContainers(thd_state, cguid_list, &ncg)) !=
								ZS_SUCCESS) {
		printf("ZSGetContainers failed with error %s\n",
							ZSStrError(status));
		return 1;
	}
out:
	printf("Number of containers created by workers: %d\n", ncg);
	ZSCloseContainer(thd_state, cguid);
	//ZSDeleteContainer(thd_state, cguid);

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
	uint64_t			datalen;
	ZS_container_props_t		props;


	//Create the container name based on thread id.
	sprintf(cname, "%s", base);

	//Initialize per thread state of ZS for this thread.
	ZSInitPerThreadState(zs_state, &thd_state);

	//Create container in read/write mode with properties specified.
	props.flash_only = (ZS_boolean_t)0;
	status = ZSOpenContainer(thd_state, cname, &props,
			ZS_CTNR_RW_MODE, &cguid);
#if 0
	if (status == ZS_SUCCESS) {
		//If created successfully, get the container properties.
		ZSGetContainerProps(thd_state, cguid, &props);
		printf("Container %s (cguid: %ld) created with size: %ldKB.\n",
						cname, cguid, props.size_kb);
	} else {
		printf("ZSOpenContainer (of %s) failed with %s.\n",
						cname, ZSStrError(status));
		return;
	}
#endif

	keylen = strlen("key") + keynumlen + 1;
	keyw = (char *)malloc(keylen);
	dataw = (char *)malloc(DATA_SIZE+ 5);
	bzero(keyw, keylen);
	bzero(dataw, DATA_SIZE+5);
	int i;
	for (i=0; i <NUM_OBJS;i++) {
		//sprintf(keyw, "key%07d%x", i, (int)pthread_self());
		sprintf(keyw, "NIRANJAN_key%038d", i);
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
		sprintf(dataw, "NIRANJAN_data%071d", i);
		status = ZSWriteObject(thd_state, cguid, keyw, keylen, dataw,
				DATA_SIZE, 0);
		if (status == ZS_SUCCESS) {
	//		printf("%x: Key, %s, created/modified.\n", (int)pthread_self(),
	//		keyw);
		} else {
			printf("%x: Key, %s, couldn't be written. %s\n",
					(int)pthread_self(), keyw, ZSStrError(status));
			return;
		}
	}
	printf("Write done --- \n");
	//free(dataw);
	return;
	for (int i=0; i <NUM_OBJS;i++) {
		//sprintf(keyw, "key%07d%x", i, (int)pthread_self());
		sprintf(keyw, "key%047d", i);
		if (strlen(keyw) != (keylen - 1)) {
			printf("Key format mismatch\n");
			return;
		}
		//Create initial data.
		status = ZSReadObject(thd_state, cguid, keyw, keylen, &dataw,
				&datalen);
		if (status == ZS_SUCCESS) {
			printf("%x: Key, %s, read %s.\n", (int)pthread_self(), keyw,
					dataw);
			ZSFreeBuffer(dataw);
		} else {
			printf("%x: Key, %s, couldn't be read.\n", (int)pthread_self(),
					keyw);
			return;
		}
	}
#if 0
	for (int i=0; i <NUM_OBJS;i++) {
		sprintf(keyw, "key%06d%x", i, (int)pthread_self());
		if (strlen(keyw) != (keylen - 1)) {
			printf("Key format mismatch\n");
			return;
		}
		//Create initial data.
		status = ZSDeleteObject(thd_state, cguid, keyw, keylen);
		if (status == ZS_SUCCESS) {
			printf("%x: Key, %s, deleted.\n", (int)pthread_self(), keyw);
		} else {
			printf("%x: Key, %s, couldn't be deleted.\n", (int)pthread_self(),
					keyw);
			return;
		}
	}
#endif

#if 0
	if (status == ZS_OBJECT_EXISTS) {
		//If data already exists, get the object assocaited with key.
		printf("cguid %ld: Key, key2, already exists."
				" Not overwriting.\n", cguid);
		ZSReadObject(thd_state, cguid, "key2", 5, &data, &datalen);
		printf("cguid %ld: Contents of key2: data=%s, datalen=%ld\n",
							cguid, data, datalen);
		ZSFreeBuffer(data);
	} else if (status == ZS_SUCCESS) {
		printf("cguid %ld: Key, key2, was not existing, created now.\n",
						cguid);
	} else {
		printf("cguid %ld: Key, key2, failed with error %d.\n",
							cguid, status);
	}

	//ZS_WRITE_MUST_EXIST - Modify only, don't create if doesn't exist.
	status = ZSWriteObject(thd_state, cguid, "key3", 5, "key3_data", 10,
				ZS_WRITE_MUST_EXIST);
	if (status == ZS_OBJECT_UNKNOWN) {
		printf("cguid %ld: Key, key3, doesn't exists. Not created.\n",
							cguid);
	} else if (status == ZS_SUCCESS) {
		printf("cguid %ld: Key, key3, was existing, object is "
				"modified.\n", cguid);
	} else {
		printf("cguid %ld: Key, key3, failed with error %d.\n",
							cguid, status);
	}
#endif
	//Flush the contents of key only.
	ZSFlushObject(thd_state, cguid, (char*)"key2", 5);


	//Close the Container.
	//ZSCloseContainer(thd_state, cguid);

	//Release/Free per thread state.
	ZSReleasePerThreadState(&thd_state);
	return;
}
