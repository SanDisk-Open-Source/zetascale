/*********************************************
**********   Author:  Lisa

**********   Function: ZSGetStats
***********************************************/
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "zs.h"
static struct ZS_state     *zs_state;
struct ZS_thread_state     *_zs_thd_state;


int preEnvironment()
{
    if(ZSInit( &zs_state) != ZS_SUCCESS ) {
        fprintf( stderr, "ZS initialization failed!\n" );
        return 0 ;
    }

    fprintf( stderr, "ZS was initialized successfully!\n" );

    if(ZS_SUCCESS != ZSInitPerThreadState( zs_state, &_zs_thd_state ) ) {
        fprintf( stderr, "ZS thread initialization failed!\n" );
        return 0;
    }

    fprintf( stderr, "ZS thread was initialized successfully!\n" );
    return 1;
}

void CleanEnvironment()
{
    ZSReleasePerThreadState(&_zs_thd_state);
    ZSShutdown(zs_state);
}

ZS_status_t GetStats(ZS_stats_t *stats)
{
    ZS_status_t           ret;

    ret = ZSGetStats(_zs_thd_state,stats);
    if(ZS_SUCCESS == ret){
        fprintf( stderr, "ZSGetStats return success.\n");
    }
    else fprintf( stderr, "ZSGetStats return failed:%s.\n",ZSStrError(ret));
    return ret;
}


int GetStats_basic_check1()
{
    ZS_stats_t            stats;
    ZS_status_t           ret;

    ret = GetStats(&stats);
    if(ZS_SUCCESS == ret)
        return 1;
    return 0;
}


int GetStats_basic_check2()
{
    ZS_status_t           ret;

    ret = GetStats(NULL);
    if(ZS_SUCCESS == ret){
        fprintf( stderr, "ZSGetStats use stats = NULL, success.\n");
        return 1;
    }
    else fprintf( stderr, "ZSGetStats use stats = NULL, failed.\n");
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
            fprintf( stderr, "ZSGetStats test %drd success.\n",j+1);
        }
        else fprintf( stderr, "ZSGetStats test %drd failed.\n",j+1);
    }
    if(resultCount == num){
        fprintf(stderr, "************ test pass!******************\n");
	fprintf(stderr, "#The related test script is ZS_GetStats.c\n");
        return 0;
    }
    else 
        fprintf(stderr, "************%d test failed!******************\n",resultCount-num);
	fprintf(stderr, "#The related test script is ZS_GetStats.c\n");
        return 1;
}
