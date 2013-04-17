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
#include 	<unistd.h>
#include 	<pthread.h>
#include	<fdf.h>

#define	FDF_PROP_FILE		"conf/fdf.prop"	//Configuration file
#define	THREADS			2		//Number of threads

static char *base = "container";

void *worker(void *arg);

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
	uint32_t			ncg;

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

	/*
	 * Override properties set in the file.
	 * Format the device and set maximum object size to 4MB.
	 */
	FDFSetProperty("SDF_REFORMAT", "1");
	FDFSetProperty("SDF_MAX_OBJ_SIZE", "4194304");

	//Initialize FDF state.
	if ((status = FDFInit(&fdf_state)) != FDF_SUCCESS) {
		printf("FDFInit failed with error %d\n", status);
		return 0;
	}

	//Initialize per-thread FDF state for main thread.
	FDFInitPerThreadState(fdf_state, &thd_state);

	//Spawn worker threads.
	for (indx = 0; indx < THREADS; indx++) {
		pthread_create(&thread_id[indx], NULL, worker,
						(void *)fdf_state);
	}

	//Wait for worker threads to complete.
	for (indx = 0; indx < THREADS; indx++) {
		pthread_join(thread_id[indx], NULL);
	}

	//Get the number of containers on the device.
	FDFGetContainers(thd_state, cguid_list, &ncg);
	printf("Number of containers created by workers: %d\n", ncg);

	//Gracefuly shutdown FDF.
	FDFShutdown(fdf_state);
	return (0);
}


/*
 * This routine is called by each pthread created by main thread. This creates
 * a container and writes <key, object> in it.
 */
void *
worker(void *arg)
{
	FDF_container_props_t		props;
	struct FDF_state		*fdf_state = (struct FDF_state *)arg;
	struct FDF_thread_state		*thd_state;
	struct FDF_iterator		*iterator;
	char				*key, *data;
	char				cname[32] = {0};
	FDF_status_t			status;
	FDF_cguid_t			cguid;
	uint32_t			keylen;
	uint64_t			datalen;
	FDF_stats_t			stats;

	//Create the container name based on thread id.
	sprintf(cname, "%s-%x", base, (int)pthread_self());

	//Initialize per thread state of FDF for this thread.
	FDFInitPerThreadState(fdf_state, &thd_state);

	//Fill up property with default values.
	FDFLoadCntrPropDefaults(&props);

	//Set size of container to 256MB and retain other values.
	props.size_kb = 256*1024;

	//Create container in read/write mode with properties specified.
	status = FDFOpenContainer(thd_state, cname, &props, 
				  FDF_CTNR_CREATE | FDF_CTNR_RW_MODE, &cguid);

	if (status == FDF_SUCCESS) {
		//If created successfully, get the container properties. 
		FDFGetContainerProps(thd_state, cguid, &props);
		printf("Container %s (cguid: %ld) created with size: %ldKB.\n", 
						cname, cguid, props.size_kb);
	} else {
		return 0;
	}


	//Modify property - Extend size of container to 512MB.
	props.size_kb = 512 * 1024;
	status = FDFSetContainerProps(thd_state, cguid, &props);
	if (status == FDF_SUCCESS) {
		FDFGetContainerProps(thd_state, cguid, &props);
		printf("Container %s (cguid: %ld) size set to : %ldKB.\n",
						cname, cguid, props.size_kb);
	}

	//Create initial data.
	status = FDFWriteObject(thd_state, cguid, "key1", 5, "Init_data", 10,
				0);
	status = FDFWriteObject(thd_state, cguid, "key2", 5, "Init_data", 10,
				0);
	status = FDFWriteObject(thd_state, cguid, "key3", 5, "Init_data", 10,
				0);

	//Create/modify data of a key irrespective of whether it exists.
	status = FDFWriteObject(thd_state, cguid, "key1", 5, "key1_data", 10,
				0);
	if (status == FDF_SUCCESS) {
		printf("cguid %ld: Key, key1, created/modified.\n", cguid);
	} else {
		printf("cguid %ld: Key, key1, couldn't be written.\n", cguid);
	}

	//FDF_WRITE_MUST_NOT_EXIST - Create only, don't modify if exists.
	status = FDFWriteObject(thd_state, cguid, "key2", 5, "key2_data", 10,
				FDF_WRITE_MUST_NOT_EXIST);
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

	//Flush the contents of key only.
	FDFFlushObject(thd_state, cguid, "key2", 5);

	//Get all the objects written in container.
	printf("cguid %ld: Enumerating container %s\n", cguid, cname);
	status = FDFEnumerateContainerObjects(thd_state, cguid, &iterator);
	while (FDFNextEnumeratedObject(thd_state, iterator, &key,
				&keylen, &data, &datalen) == FDF_SUCCESS) {
		printf("cguid %ld: Enum: key=%s, keylen=%d, data=%s, "
			"datalen=%ld\n", cguid, key, keylen, data, datalen);
		FDFFreeBuffer(key);
		FDFFreeBuffer(data);
	}
	FDFFinishEnumeration(thd_state, iterator);

	//Delete a <key, object> pair.
	if (FDFDeleteObject(thd_state, cguid, "key2", 5) == FDF_SUCCESS) {
		printf("cguid %ld: Key, key2, deleted successfully\n", cguid);
	} else {
		printf("cguid %ld: Key, key2, deletion failed\n", cguid);
	}

	//Flush contents of container.
	if (FDFFlushContainer(thd_state, cguid) == FDF_SUCCESS) {
		printf("cguid %ld: Container contents flushed successfully\n",
									cguid);
	}

	/*
	 * Transaction:
	 * Do set of operations, if atleast one fails, revert all other 
	 * operations. Else commit all of them.
	 */
	if ((status = FDFTransactionStart(thd_state)) == FDF_SUCCESS) {
		printf("cguid %ld: Transaction start\n", cguid);
		//Operation 1: Modify contents of key1
		status = FDFWriteObject(thd_state, cguid, "key1", 5,
					"tran_data", 10, 0);
		FDFReadObject(thd_state, cguid, "key1", 5, &data, &datalen);
		printf("cguid %ld: Contents of key1: data=%s, datalen=%ld\n",
						cguid, data, datalen);

		//Operation 2: Create contents for key3.
		status = FDFWriteObject(thd_state, cguid, "key3", 5,
				"key3_data", 10, FDF_WRITE_MUST_NOT_EXIST);
		if (status == FDF_OBJECT_EXISTS) {
			//Operation failed, rollback contents of key1 too. 
			status = FDFTransactionRollback(thd_state);
			if (status == FDF_SUCCESS) {
				printf("cguid %ld: Transaction rolled back\n",
						cguid);
			}
		} else {
			//Operation succeeded. Commit both operations.
			status = FDFTransactionCommit(thd_state);
			if (status == FDF_SUCCESS) {
				printf("cguid %ld: Transaction committed\n",
									cguid);
			}
		}

		//Print contents of key1 based on result.
		FDFReadObject(thd_state, cguid, "key1", 5, &data, &datalen);
		printf("cguid %ld: Contents of key1 after transaction:"
			" data=%s, datalen=%ld\n", cguid, data, datalen);
	}

	//Get container statistics. Print number of objects in container.
	if ((status = FDFGetContainerStats(thd_state, cguid, &stats)) ==
								FDF_SUCCESS) {
		printf("cguid %ld: Number of objects: %"PRId64"\n",
			cguid, stats.flash_stats[FDF_FLASH_STATS_NUM_OBJS]);
	}

	//Close the Container.
	FDFCloseContainer(thd_state, cguid);

	//Release/Free per thread state.
	FDFReleasePerThreadState(&thd_state);
	return 0;
}
