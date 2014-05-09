#include "fdf.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

#define FDF_MAX_KEY_LEN 256

#define OBJ_COUNT 100000

int
main()
{
    FDF_container_props_t props;
    struct FDF_state *fdf_state;
    struct FDF_thread_state *thd_state;
    FDF_status_t	status;
    FDF_cguid_t cguid;
    char	data1[256] = "data is just for testing pstats";
    uint64_t i;

    char key_var[FDF_MAX_KEY_LEN] ={0};

    FDFSetProperty("FDF_REFORMAT", "1");
    if (FDFInit(&fdf_state) != FDF_SUCCESS) {
		return -1;
	}
    FDFInitPerThreadState(fdf_state, &thd_state);	

    FDFLoadCntrPropDefaults(&props);

    props.persistent = 1;
    props.evicting = 0;
    props.writethru = 1;
    props.durability_level= FDF_DURABILITY_SW_CRASH_SAFE;
    props.fifo_mode = 0;

    /*
     * Test 1: Check if asynchronous delete container works
     */

    status = FDFOpenContainer(thd_state, "cntr", &props, FDF_CTNR_CREATE, &cguid);
    if (status != FDF_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        return -1;	
    }

    /*
     * Write a million objects
     */
    for (i = 1; i <= OBJ_COUNT; i++) {
        memset(key_var, 0, FDF_MAX_KEY_LEN);
        sprintf(key_var, "key_%ld", i);
        sprintf(data1, "data_%ld", i);

        status = FDFWriteObject(thd_state, cguid, key_var, strlen(key_var) + 1, data1, strlen(data1), 0);
        if (FDF_SUCCESS != status) {
            fprintf(stderr, "FDFWriteObject= %s\n", FDFStrError(status));
        }
        assert(FDF_SUCCESS == status);
    }

    /*
     * Delete the container
     */
    status = FDFDeleteContainer(thd_state, cguid);
    if (status != FDF_SUCCESS) {
        printf("Delete container failed with error=%s.\n", FDFStrError(status));
        return -1;	
    }

    /*
     * Make sure container do not exist now!
     */
    status = FDFOpenContainer(thd_state, "cntr", &props, FDF_CTNR_RO_MODE, &cguid);
    if (status == FDF_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        return -1;	
    }

    /*
     * Test 2
     */
     
    status = FDFOpenContainer(thd_state, "cntr", &props, FDF_CTNR_CREATE, &cguid);
    if (status != FDF_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        return -1;	
    }

    /*
     * Write two million objects
     */
    for (i = 1; i <= 2 * OBJ_COUNT; i++) {
        memset(key_var, 0, FDF_MAX_KEY_LEN);
        sprintf(key_var, "key_%ld", i);
        sprintf(data1, "data_%ld", i);

        status = FDFWriteObject(thd_state, cguid, key_var, strlen(key_var) + 1, data1, strlen(data1), 0);
        assert(FDF_SUCCESS == status);
    }

    /*
     * Delete the container
     */
    status = FDFDeleteContainer(thd_state, cguid);
    if (status != FDF_SUCCESS) {
        printf("Delete container failed with error=%s.\n", FDFStrError(status));
        return -1;	
    }

    /*
     * Make sure container do not exist now!
     */
    status = FDFOpenContainer(thd_state, "cntr", &props, FDF_CTNR_RO_MODE, &cguid);
    if (status == FDF_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        return -1;	
    }

    status = FDFOpenContainer(thd_state, "cntr", &props, FDF_CTNR_CREATE, &cguid);
    if (status != FDF_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        return -1;	
    }

    /*
     * Write a million objects
     */
    for (i = 1; i <= OBJ_COUNT; i++) {
        memset(key_var, 0, FDF_MAX_KEY_LEN);
        sprintf(key_var, "key_%ld", i);
        sprintf(data1, "data_%ld", i);

        status = FDFWriteObject(thd_state, cguid, key_var, strlen(key_var) + 1, data1, strlen(data1), 0);
        assert(FDF_SUCCESS == status);
    }
    FDFCloseContainer(thd_state, cguid);
    status = FDFDeleteContainer(thd_state, cguid);
    assert(FDF_SUCCESS == status);

    FDFReleasePerThreadState(&thd_state);

    FDFShutdown(fdf_state);

    return 0;
}
