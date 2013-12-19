#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "fdf.h"
static struct FDF_state     *fdf_state;
struct FDF_thread_state     *_fdf_thd_state;


int preEnvironment()
{
    if(FDFInit( &fdf_state) != FDF_SUCCESS ) {
        fprintf( stderr, "FDF initialization failed!\n" );
        return 0 ;
    }
    fprintf( stderr, "FDF was initialized successfully!\n" );
    if(FDF_SUCCESS != FDFInitPerThreadState( fdf_state, &_fdf_thd_state ) ) {
        fprintf( stderr, "FDF thread initialization failed!\n" );
        return 0;
    }
    fprintf( stderr, "FDF thread was initialized successfully!\n" );
    return 1;
}

void CleanEnvironment()
{
    FDFReleasePerThreadState(&_fdf_thd_state);
    FDFShutdown(fdf_state);
}

#define DATA_LEN 1024

FDF_status_t verify_fdf_container_stats() {
    FDF_status_t ret;
    FDF_container_props_t p;
    FDF_cguid_t cguid;
    uint32_t flag = 1;
    char key[256], cname[64]="cont1";
    char value[1024];
    //uint32_t keylen = 0;
    char *data;
    uint64_t datalen;
    FDF_stats_t stats;;
   
     
    /* Create a container */
    FDFLoadCntrPropDefaults(&p);
    p.size_kb = 0;
    ret = FDFOpenContainer(_fdf_thd_state,cname,&p,flag,&cguid);
    if( ret != FDF_SUCCESS ) {
        fprintf( stderr, "Container open failed\n");
        return FDF_FAILURE;
    } 
    strcpy(key,"key_test"); 
    /* Write object */
    ret = FDFWriteObject(_fdf_thd_state, cguid, key, strlen(key), value, DATA_LEN, 1);
    if( ret != FDF_SUCCESS ) {
        fprintf( stderr, "Write failed\n");
        return FDF_FAILURE;
    } 
    ret = FDFReadObject(_fdf_thd_state,cguid,key,strlen(key),&data,&datalen);
    if( ret != FDF_SUCCESS ) {
        fprintf( stderr, "Read failed\n");
        return FDF_FAILURE;
    } 
    free(data);

    FDFGetContainerStats(_fdf_thd_state,cguid,&stats); 
    if(stats.cntr_stats[FDF_CNTR_STATS_NUM_OBJS] != 1 ) {
        fprintf( stderr, "Number of objects (%lu) does not match expected count %d\n",
                   stats.cntr_stats[FDF_CNTR_STATS_NUM_OBJS], 1);
         return FDF_FAILURE;

    }
    if(stats.cntr_stats[FDF_CNTR_STATS_USED_SPACE] <= 0 ) {
        fprintf( stderr, "Used space is < 0 and does not match expected value\n");
         return FDF_FAILURE;
    }

    if( (stats.n_accesses[FDF_ACCESS_TYPES_APCOE] <= 0 ) &&
        (stats.n_accesses[FDF_ACCESS_TYPES_APCOP] <= 0 )&&
        (stats.n_accesses[FDF_ACCESS_TYPES_WRITE] <= 0 )) {
        fprintf( stderr, "Number of create requests is not as expected\n");
        return FDF_FAILURE;
    }
    if( (stats.n_accesses[FDF_ACCESS_TYPES_READ] <= 0 ) &&
        (stats.n_accesses[FDF_ACCESS_TYPES_APGRX] <= 0 )) {
        fprintf( stderr, "Number of get requests is not as expected\n");
        return FDF_FAILURE;
    }
    /* Check if create  */
    FDFCloseContainer(_fdf_thd_state,cguid);
     return FDF_SUCCESS;

}

int main(int argc, char *argv[])
{
    if( 1 != preEnvironment()) {
        return 1;
    }
    if( verify_fdf_container_stats() != FDF_SUCCESS ) {
        return 1;
    }
    CleanEnvironment();
}
