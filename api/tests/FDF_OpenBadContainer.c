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
#define CNTR_SIZE 		1	/* GB */

static char *base = "container";

int
main( )
{
	struct FDF_state		*fdf_state;
	struct FDF_thread_state		*thd_state;
	FDF_status_t			status;
	char				*version;
	FDF_container_props_t		props;
	char				cname[32] = {0};
	FDF_cguid_t			cguid;

	//Create the container name based on thread id.
	sprintf(cname, "%s", base);

	//Get the version FDF the program running with.
	if (FDFGetVersion(&version) == FDF_SUCCESS) {
		printf("This is a sample program using FDF %s\n", version);
		FDFFreeBuffer(version);
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

	 props.Fault_Injec_ConatinerFail = 1; // Injecting Fault to fail the container create before btree is initialized

	//Create container in read/write mode with properties specified.
	status = FDFOpenContainer(thd_state, cname, &props, 
				  FDF_CTNR_CREATE | FDF_CTNR_RW_MODE, &cguid);

	props.Fault_Injec_ConatinerFail = 0;
	status = FDFOpenContainer(thd_state, cname, &props, FDF_CTNR_RW_MODE, &cguid);

	if (status == FDF_CONTAINER_UNKNOWN) {
		fprintf(stderr,"Container %s doesn't exist\n",cname);
		fprintf(stderr,"yay...\n");
	} else {
		printf("FDFOpenContainer (of %s) failed with %s.\n", 
						cname, FDFStrError(status));
	}

	FDFReleasePerThreadState(&thd_state);
	//Gracefuly shutdown FDF.
	FDFShutdown(fdf_state);
	return (0);
}
