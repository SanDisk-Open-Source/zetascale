/****************************
#function : FDFLoadProperties
#author   : AliceXu
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "fdf.h"

FILE *fp;
static struct FDF_state        *fdf_state;
static struct FDF_thread_state *fdf_thrd_state;
//FDF_config_t                   *fdf_config;
FDF_cguid_t                    cguid;
char *testname[10] = {NULL};
int result[10] = {0};

FDF_status_t pre_env()
{
     FDF_status_t ret = FDF_FAILURE;
   //(void)FDFLoadConfigDefaults(fdf_config, NULL);
   //  if (FDFInit(&fdf_state, fdf_config) != FDF_SUCCESS) {
     ret = FDFInit(&fdf_state);
     if (FDF_SUCCESS != ret)
     {
        fprintf(fp, "FDF initialization failed!\n");
     }else {
        fprintf(fp, "FDF initialization succeed!\n");
        ret = FDFInitPerThreadState(fdf_state, &fdf_thrd_state);
        if( FDF_SUCCESS == ret)
        {
            fprintf(fp, "FDF thread initialization succeed!\n");
        }
     }
     return ret;
}

void clear_env()
{
    (void)FDFReleasePerThreadState(&fdf_thrd_state);
    (void)FDFShutdown(fdf_state);
    fprintf(fp, "clear env!\n");
}                       

/***************** test ******************/

int test_property_is_null()
{
    int tag = 0;
    testname[0] = "#test0: test with incoming parmeter is NULL.";

    tag = FDFLoadProperties(NULL);
    if(0 == tag)
    {
        result[0] = 1;
        fprintf(fp,"ok -> can load properties with input is NULL!\n");
        return 1;
    }else{
        fprintf(fp,"fail -> can't load properties with input is NULL!\n");
        return 0;
    }
}

int test_property_is_wrong()
{
    int tag = 0;
    testname[1] = "#test1: test with property path is none exist or wrong.";

    tag = FDFLoadProperties("lalala");
    if(-1 == tag)
    {
        result[1] = 1;
        fprintf(fp,"ok -> can't load properties with property path is none-exist!\n");
        return 1;
    }else{
        fprintf(fp,"fail -> can't load properties with property path is none-exist!\n");
        return 0;
    }
}

int test_basic_check()
{
    int tag = 0;
    testname[2] = "#test2: test with basic function.";

    if (system("mkdir /tmp/my.properties")) {}
    sleep(5);
    tag = FDFLoadProperties("/tmp/my.properties");
    fprintf(stderr, "FDFLoadProperties test_basic_check() returned %d\n", tag);
    if (system("rm -rf /tmp/my.properties")) {}
    if(0 == tag)
    {
        result[2] = 1;
        fprintf(fp,"ok -> can't load properties with valid property path!\n");
        return 1;
    }else{
        fprintf(fp,"fail -> can't load properties with valid property path!\n");
        return 0;
    }
}

/************** main function ***********/

int main() 
{
    int testnumber = 3;
	int count      = 0;

    if((fp = fopen("FDF_LoadProperties.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!.\n");
        return -1;
    }

    if(FDF_SUCCESS == pre_env())
    {
        count += test_property_is_null();
        count += test_property_is_wrong();
        count += test_basic_check();
        clear_env();
    }
    fclose(fp);
   
    fprintf(stderr, "Test Result:\n");
    for(int i = 0; i < testnumber; i++)
    {
        fprintf(stderr, "%s\n", testname[i]);
        if(1 == result[i])
        {
            fprintf(stderr, "result: pass\n");
        }else{
            fprintf(stderr, "result: fail\n");
        }
    }

    if(testnumber == count)
    {
        fprintf(stderr, "#Test of FDFLoadProperties pass!\n");
	fprintf(stderr, "#The related test script is FDF_LoadProperties.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_LoadProperties.log\n");
    }else{
        fprintf(stderr, "#Test of FDFLoadProperties fail!\n");
	fprintf(stderr, "#The related test script is FDF_LoadProperties.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_LoadProperties.log\n");
    }


	return (!(testnumber == count));
}
