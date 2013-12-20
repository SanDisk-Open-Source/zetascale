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
	FDF_status_t			status;
	pthread_t			thread_id[THREADS];
	char				*version;
	int				indx;
	//uint32_t		t;
	uint32_t			ncg;
//	const char			*path;
	FDF_container_props_t		props;
	char				cname[32] = {0};
	FDF_cguid_t			cguid;
//	uint64_t		seq;
	//FDF_container_snapshots_t *snaps;

	//Create the container name based on thread id.
	sprintf(cname, "%s", base);

	//Get the version FDF the program running with.
	if (FDFGetVersion(&version) == FDF_SUCCESS) {
		printf("This is a sample program using FDF %s\n", version);
		FDFFreeBuffer(version);
	}

#if 0
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
#endif

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
	for (indx = 0; indx < THREADS; indx++) {
		pthread_create(&thread_id[indx], NULL, (void *)worker,
						(void *)fdf_state);
	}

	//Wait for worker threads to complete.
	for (indx = 0; indx < THREADS; indx++) {
		pthread_join(thread_id[indx], NULL);
	}
	sleep(2);
//	while(1);

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
	FDF_container_props_t		props;


	//Create the container name based on thread id.
	sprintf(cname, "%s%x", base,(int)pthread_self());

	//Initialize per thread state of FDF for this thread.
	FDFInitPerThreadState(fdf_state, &thd_state);

	FDFLoadProperties(FDF_PROP_FILE);
	FDFLoadCntrPropDefaults(&props);


	//Create container in read/write mode with properties specified.
	status = FDFOpenContainer(thd_state, cname, &props,
			FDF_CTNR_CREATE | FDF_CTNR_RW_MODE, &cguid);

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
		status = FDFWriteObject(thd_state, cguid, keyw, keylen, dataw, DATA_SIZE, 0);
	}
	printf("Write done --- \n");
	free(dataw);

	for (int i=0; i <NUM_OBJS;i=i+1) {
		sprintf(keyw, "key%07d%x", i, (int)pthread_self());
		if (strlen(keyw) != (keylen - 1)) {
			printf("Key format mismatch\n");
			return;
		}
		status = FDFDeleteObject(thd_state, cguid, keyw, keylen);
        }

	status = FDFScavengeContainer(fdf_state,cguid);
	if (status == FDF_SUCCESS)
		printf(" scavenger operation started %s\n", FDFStrError(status));


	//Release/Free per thread state.
	FDFReleasePerThreadState(&thd_state);
	return;
}
