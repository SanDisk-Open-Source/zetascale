/*********************************************
**********   Author:  Lisa

**********   Function: FDFGetStats
***********************************************/
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

FDF_status_t GetStats(FDF_stats_t *stats)
{
    FDF_status_t           ret;

    ret = FDFGetStats(_fdf_thd_state,stats);
    if(FDF_SUCCESS == ret){
        fprintf( stderr, "FDFGetStats return success.\n");
    }
    else fprintf( stderr, "FDFGetStats return failed:%s.\n",FDFStrError(ret));
    return ret;
}


int GetStats_basic_check1()
{
    FDF_stats_t            stats;
    FDF_status_t           ret;

    ret = GetStats(&stats);
    if(FDF_SUCCESS == ret)
        return 1;
    return 0;
}


int GetStats_basic_check2()
{
    FDF_status_t           ret;

    ret = GetStats(NULL);
    if(FDF_SUCCESS == ret){
        fprintf( stderr, "FDFGetStats use stats = NULL, success.\n");
        return 1;
    }
    else fprintf( stderr, "FDFGetStats use stats = NULL, failed.\n");
    return 0;
}


/**********  main function *******/

int main(int argc, char *argv[])
{
    int result[4] = {0,0};
    int resultCount = 2;
    int num = 0;

    if( 1 != preEnvironment())
        return 0;

    fprintf(stderr, "************Begin to test ***************\n");
    result[0] = GetStats_basic_check1();
    result[1] = GetStats_basic_check2();
    
    CleanEnvironment();
    fprintf(stderr, "************test result as below***************\n");

    for(int j = 0; j < 2; j++){
        if(result[j] == 1){
            num++;
            fprintf( stderr, "FDFGetStats test %drd success.\n",j+1);
        }
        else fprintf( stderr, "FDFGetStats test %drd failed.\n",j+1);
    }
    if(resultCount == num){
        fprintf(stderr, "************ test pass!******************\n");
	fprintf(stderr, "#The related test script is FDF_GetStats.c\n");
        return 0;
    }
    else 
        fprintf(stderr, "************%d test failed!******************\n",resultCount-num);
	fprintf(stderr, "#The related test script is FDF_GetStats.c\n");
        return 1;
}
