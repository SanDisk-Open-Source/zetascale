#include     <stdio.h>
#include     <stdlib.h>
#include     <string.h>
#include     <unistd.h>
#include     <pthread.h>
#include    <fdf.h>
#include    <assert.h>

#define    FDF_PROP_FILE        "conf/fdf.prop"    //Configuration file
#define DATA_LEN 2048

int
main( int argc, char **argv)
{
    struct FDF_state        *fdf_state;
    struct FDF_thread_state *thd_state;
    FDF_status_t            status;
    char                    data[DATA_LEN];
    char                    cname[32] = {0};
    FDF_cguid_t             cguid;
    FDF_container_props_t   props;
    FDF_stats_t stats;
                                
    strcpy(cname, "testc");
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
    FDFSetProperty("FDF_COMPRESSION", "1");
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
    FDFLoadCntrPropDefaults(&props);
    props.compression = 1;
    status = FDFOpenContainer(thd_state, cname, &props,
                          FDF_CTNR_CREATE | FDF_CTNR_RW_MODE, &cguid);
    if (status != FDF_SUCCESS) {
          printf("FDFOpenContainer failed\n");
          return 1;
    }
    memset(data,0,DATA_LEN);
    status = FDFWriteObject(thd_state, cguid, "key1", 5, data, DATA_LEN, 0);  
    if( status != FDF_SUCCESS ) {
          printf("FDFWriteObject failed\b");
          return 1;
    }
            //Get container statistics. Print number of objects in container.
    if ((status = FDFGetContainerStats(thd_state, cguid, &stats)) ==
                                                            FDF_SUCCESS) {
            printf("cguid %ld: Number of objects: %"PRId64"\n",
                    cguid, stats.flash_stats[FDF_FLASH_STATS_NUM_OBJS]);
    }   
    if ( stats.flash_stats[FDF_FLASH_STATS_COMP_BYTES] == 0 ) {
         printf("Compression bytes must be a non-zero\n");
         return 1;
    }
    assert(FDF_SUCCESS == FDFReleasePerThreadState(&thd_state));

    printf("Compressed bytes:%lu\n",stats.flash_stats[FDF_FLASH_STATS_COMP_BYTES]);
    /*Get FDF stats and see compressed_bytes */
    //Gracefuly shutdown FDF.
    FDFShutdown(fdf_state);
    return (0);
}
