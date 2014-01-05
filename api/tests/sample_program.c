/*
 * Flash Data Fabric - Sample program on API usage.
 *
 * (c) Copyright 2013 SanDisk, all rights reserved.
 * http://www.sandisk.com
 */

/*
 * This program helps in understanding the usage of FDF APIs. This program
 * reads properties file and sets some of the properties of FDF. It spawns
 * worker threads, where each thread creates its own container, writes
 * <key, object> pair.
 */ 

#include 	<stdio.h>
#include 	<stdlib.h>
#include 	<string.h>
#include 	<unistd.h>
#include 	<pthread.h>
#include	<fdf.h>

#define	FDF_PROP_FILE		"conf/fdf.prop"	//Configuration file
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
	struct FDF_state		*fdf_state;
	struct FDF_thread_state		*thd_state;
	FDF_cguid_t			cguid_list[THREADS] = {0};
	FDF_status_t			status, status2;
	pthread_t			thread_id[THREADS];
	char				*version;
	int				indx;
	uint32_t		t;
	uint32_t			ncg;
	const char			*path;
	FDF_container_props_t		props;
	char				cname[32] = {0};
	FDF_cguid_t			cguid;
	uint64_t		seq, seq1;
	FDF_container_snapshots_t *snaps;

	//Create the container name based on thread id.
	sprintf(cname, "%s", base);

	//Get the version FDF the program running with.
	if (FDFGetVersion(&version) == FDF_SUCCESS) {
		printf("This is a sample program using FDF %s\n", version);
		FDFFreeBuffer(version);
	}

	if (FDFLoadProperties(FDF_PROP_FILE) != FDF_SUCCESS) {
		printf("Couldn't load properties from %s. FDFInit()"
			" will use default properties or from file specified"
			" in FDF_PROPRTY_FILE environment variable if set.\n",
			FDF_PROP_FILE);
	} else {
		/*
		 * Propertie were loaded from file successfully, dont overwrite
		 * them by reading file specified in environment variable.
		 */
		unsetenv("FDF_PROPERTY_FILE");
	}

	path = FDFGetProperty("FDF_LICENSE_PATH", "Default path");
	if (path && (strcmp(path, "Default path") != 0)) {
		printf("License will be searched at: %s\n", path);
		FDFFreeBuffer((char *)path);
	}

	//Initialize FDF state.
	if ((status = FDFInit(&fdf_state)) != FDF_SUCCESS) {
		printf("FDFInit failed with error %s\n", FDFStrError(status));
		return 1;
	}

	//Initialize per-thread FDF state for main thread.
	if ((status = FDFInitPerThreadState(fdf_state, &thd_state)) != 
								FDF_SUCCESS) {
		printf("FDFInitPerThreadState failed with error %s\n",
							FDFStrError(status));
		return 1;
	}

	//Fill up property with default values.
	FDFLoadCntrPropDefaults(&props);

	//Set size of container to 256MB and retain other values.
	props.size_kb = CNTR_SIZE*10*1024 *1024;

	//Create container in read/write mode with properties specified.
	status = FDFOpenContainer(thd_state, cname, &props, 
				  FDF_CTNR_CREATE | FDF_CTNR_RW_MODE, &cguid);

	if (status == FDF_SUCCESS) {
		//If created successfully, get the container properties. 
		FDFGetContainerProps(thd_state, cguid, &props);
		printf("Container %s (cguid: %ld) created with size: %ldKB.\n", 
						cname, cguid, props.size_kb);
	} else {
		printf("FDFOpenContainer (of %s) failed with %s.\n", 
						cname, FDFStrError(status));
		return 1;
	}
	//Spawn worker threads.
	for (indx = 0; indx < 0; indx++) {
		pthread_create(&thread_id[indx], NULL, (void *)worker,
						(void *)fdf_state);
	}

	//Wait for worker threads to complete.
	for (indx = 0; indx < 0; indx++) {
		pthread_join(thread_id[indx], NULL);
	}
	sleep(2);
	status = FDFWriteObject(thd_state, cguid, "test", 4, "test", 4, 0);
	fprintf(stderr, "Write data status: %s <--------------\n", FDFStrError(status));
	for (indx=0; indx < 2; indx++) {
		status2 = FDFCreateContainerSnapshot(thd_state, cguid, &seq);
		if ((status2 == FDF_SUCCESS) && indx == 0) {
			seq1 = seq;
			status = FDFWriteObject(thd_state, cguid, "test", 4, "testabc", 7, FDF_WRITE_MUST_EXIST);
			fprintf(stderr, "Update data status: %s <--------------\n", FDFStrError(status));
		}
	}
	if (status2 == FDF_SUCCESS) {
		FDFGetContainerSnapshots(thd_state, cguid, &t, &snaps);
		fprintf(stderr, "No of snaps: %d\n", (int)t);
		for (indx = 0; indx < t; indx++) {
			fprintf(stderr, "snap[%d]: timestamp:%"PRId64" seqno:%"PRId64"\n", (int)indx, snaps[indx].timestamp, snaps[indx].seqno);
		}
		FDFDeleteContainerSnapshot(thd_state, cguid, seq1);
		FDFGetContainerSnapshots(thd_state, cguid, &t, &snaps);
		fprintf(stderr, "No of snaps: %d\n", (int)t);
		for (indx = 0; indx < t; indx++) {
			fprintf(stderr, "snap[%d]: timestamp:%"PRId64" seqno:%"PRId64"\n", (int)indx, snaps[indx].timestamp, snaps[indx].seqno);
		}
	}

	//Flush all the cache contents created by workers.
	FDFFlushCache(thd_state);
	
	//Get the number of containers on the device.
	if ((status = FDFGetContainers(thd_state, cguid_list, &ncg)) !=
								FDF_SUCCESS) {
		printf("FDFGetContainers failed with error %s\n",
							FDFStrError(status));
		return 1;
	}
	printf("Number of containers created by workers: %d\n", ncg);
	FDFCloseContainer(thd_state, cguid);
	FDFDeleteContainer(thd_state, cguid);

	//Gracefuly shutdown FDF.
	FDFShutdown(fdf_state);
	return (0);
}


/*
 * This routine is called by each pthread created by main thread. This creates
 * a container and writes <key, object> in it.
 */
void
worker(void *arg)
{
	struct FDF_state		*fdf_state = (struct FDF_state *)arg;
	struct FDF_thread_state		*thd_state;
	char				*keyw, *dataw;
	char				cname[32] = {0};
	FDF_status_t			status;
	FDF_cguid_t			cguid;
	uint32_t			keylen;
	//uint64_t			datalen;
	FDF_container_props_t		props;


	//Create the container name based on thread id.
	sprintf(cname, "%s", base);

	//Initialize per thread state of FDF for this thread.
	FDFInitPerThreadState(fdf_state, &thd_state);

	//Create container in read/write mode with properties specified.
	status = FDFOpenContainer(thd_state, cname, &props, 
				  FDF_CTNR_RW_MODE, &cguid);
#if 0
	if (status == FDF_SUCCESS) {
		//If created successfully, get the container properties. 
		FDFGetContainerProps(thd_state, cguid, &props);
		printf("Container %s (cguid: %ld) created with size: %ldKB.\n", 
						cname, cguid, props.size_kb);
	} else {
		printf("FDFOpenContainer (of %s) failed with %s.\n", 
						cname, FDFStrError(status));
		return;
	}
#endif

	keylen = strlen("key") + keynumlen + 1 + 8;
	keyw = (char *)malloc(keylen);
	dataw = (char *)malloc(DATA_SIZE);
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
		status = FDFWriteObject(thd_state, cguid, keyw, keylen, dataw, DATA_SIZE, 0);
		if (status == FDF_SUCCESS) {
	//		printf("%x: Key, %s, created/modified.\n", (int)pthread_self(), keyw);
		} else {
			printf("%x: Key, %s, couldn't be written. %s\n", (int)pthread_self(), keyw, FDFStrError(status));
			return;
		}
	}
	printf("Write done --- \n");
	free(dataw);
#if 0
	for (int i=0; i <NUM_OBJS;i++) {
		sprintf(keyw, "key%06d%x", i, (int)pthread_self());
		if (strlen(keyw) != (keylen - 1)) {
			printf("Key format mismatch\n");
			return;
		}
		//Create initial data.
		status = FDFReadObject(thd_state, cguid, keyw, keylen, &dataw, &datalen);
		if (status == FDF_SUCCESS) {
			printf("%x: Key, %s, read.\n", (int)pthread_self(), keyw);
			FDFFreeBuffer(dataw);
		} else {
			printf("%x: Key, %s, couldn't be read.\n", (int)pthread_self(), keyw);
			return;
		}
	}
	for (int i=0; i <NUM_OBJS;i++) {
		sprintf(keyw, "key%06d%x", i, (int)pthread_self());
		if (strlen(keyw) != (keylen - 1)) {
			printf("Key format mismatch\n");
			return;
		}
		//Create initial data.
		status = FDFDeleteObject(thd_state, cguid, keyw, keylen);
		if (status == FDF_SUCCESS) {
			printf("%x: Key, %s, deleted.\n", (int)pthread_self(), keyw);
		} else {
			printf("%x: Key, %s, couldn't be deleted.\n", (int)pthread_self(), keyw);
			return;
		}
	}

#endif
#if 0
	if (status == FDF_OBJECT_EXISTS) {
		//If data already exists, get the object assocaited with key.
		printf("cguid %ld: Key, key2, already exists."
				" Not overwriting.\n", cguid);
		FDFReadObject(thd_state, cguid, "key2", 5, &data, &datalen);
		printf("cguid %ld: Contents of key2: data=%s, datalen=%ld\n",
							cguid, data, datalen);
		FDFFreeBuffer(data);
	} else if (status == FDF_SUCCESS) {
		printf("cguid %ld: Key, key2, was not existing, created now.\n",
						cguid);
	} else {
		printf("cguid %ld: Key, key2, failed with error %d.\n",
							cguid, status);
	}

	//FDF_WRITE_MUST_EXIST - Modify only, don't create if doesn't exist.
	status = FDFWriteObject(thd_state, cguid, "key3", 5, "key3_data", 10,
				FDF_WRITE_MUST_EXIST);
	if (status == FDF_OBJECT_UNKNOWN) {
		printf("cguid %ld: Key, key3, doesn't exists. Not created.\n",
							cguid);
	} else if (status == FDF_SUCCESS) {
		printf("cguid %ld: Key, key3, was existing, object is "
				"modified.\n", cguid);
	} else {
		printf("cguid %ld: Key, key3, failed with error %d.\n",
							cguid, status);
	}
#endif
	//Flush the contents of key only.
	FDFFlushObject(thd_state, cguid, "key2", 5);


	//Close the Container.
	//FDFCloseContainer(thd_state, cguid);

	//Release/Free per thread state.
	FDFReleasePerThreadState(&thd_state);
	return;
}
