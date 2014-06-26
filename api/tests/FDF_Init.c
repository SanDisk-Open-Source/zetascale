/****************************
#function : ZSInit
#author   : AliceXu
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "zs.h"

FILE *fp;
static struct ZS_state        *zs_state1,*zs_state2;
static struct ZS_thread_state *zs_thrd_state;
//ZS_config_t                   *fdf.config;
ZS_cguid_t                    cguid;
char *testname[10] = {NULL};
int result[10] = {0};

/***************** test ******************/
/*
int test_invalid_output()
{
    ZS_status_t ret = ZS_FAILURE;
    testname[0] = "#test0: test with out-parameter 'zs_state' is NULL.";
    fprintf(fp,"%s\n",testname[0]);
    
    ret = ZSInit(NULL);
    fprintf(fp,"ZSInit(NULL): %s\n",ZSStrError(ret));
    if(ZS_FAILURE == ret)
    {
        result[0] = 1;
        return 1;
    }else{
        return 0;
    }
}
*/

int test_basic_check()
{
    ZS_status_t ret = ZS_FAILURE;
    int tag = 0;
    testname[1] = "#test1: ZSInit / basic ZS operation / close ZS.";
    fprintf(fp,"%s\n",testname[1]);
    
    ret = ZSInit(&zs_state1);  
    fprintf(fp,"ZSInit: %s\n",ZSStrError(ret));
    if(ZS_SUCCESS == ret)
    {
        tag += 1;
        ret = ZSInitPerThreadState(zs_state1, &zs_thrd_state);
        fprintf(fp,"ZSInitPerThreadState: %s\n",ZSStrError(ret));
        if(ZS_SUCCESS == ret)
        {
            tag += 1;
            (void)ZSReleasePerThreadState(&zs_thrd_state);
        }
        (void)ZSShutdown(zs_state1);
    }
    if(2 == tag)
    {
        result[1] = 1;
        return 1;
    }else{
        return 0;
    }
}   

int test_double_init()
{
    ZS_status_t ret = ZS_FAILURE;
    testname[2] = "#test2: Double ZSInit and check whether it succedd.";
    fprintf(fp,"%s\n",testname[2]);

    ret = ZSInit(&zs_state1);
    fprintf(fp,"-step1:%s\n",ZSStrError(ret));
    if(ZS_SUCCESS == ret)
    {
        ret = ZSInit(&zs_state2);
        fprintf(fp,"-step2:%s\n",ZSStrError(ret));
        if(ZS_FAILURE == ret)
        {
            result[2] = 1;
        }
    }
    (void)ZSShutdown(zs_state1);
    (void)ZSShutdown(zs_state2);

    return result[2];
}
  
/************** main function ***********/

int main() 
{
    int testnumber = 2;
	int count      = 0;

    if((fp = fopen("ZS_Init.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!.\n");
        return -1;
    }

//    count += test_invalid_output();
      count += test_basic_check();
//    count += test_double_init();
     
    fclose(fp);
      
    fprintf(stderr, "Test Result:\n");
    for(int i = 0; i < testnumber; i++)
    {
    if(NULL != testname[i])
    {
        fprintf(stderr, "%s\n", testname[i]);
        if(1 == result[i])
        {
            fprintf(stderr, "result: pass\n");
        }else{
            fprintf(stderr, "result: fail\n");
        }
    }
    }

    if(1 == count)
    {
        fprintf(stderr, "#Test of ZSInit pass!\n");
	fprintf(stderr, "#The related test script is ZS_Init.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_Init.log\n");
    }else{
        fprintf(stderr, "#Test of ZSInit fail!\n");
	fprintf(stderr, "#The related test script is ZS_Init.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_Init.log\n");
    }


	return (!(1 == count));
}
