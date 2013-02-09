/****************************
#function : FDFInit
#author   : AliceXu
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "fdf.h"

FILE *fp;
static struct FDF_state        *fdf_state1,*fdf_state2;
static struct FDF_thread_state *fdf_thrd_state;
//FDF_config_t                   *fdf_config;
FDF_cguid_t                    cguid;
char *testname[10] = {NULL};
int result[10] = {0};

/***************** test ******************/
/*
int test_invalid_output()
{
    FDF_status_t ret = FDF_FAILURE;
    testname[0] = "#test0: test with out-parameter 'fdf_state' is NULL.";
    fprintf(fp,"%s\n",testname[0]);
    
    ret = FDFInit(NULL);
    fprintf(fp,"FDFInit(NULL): %s\n",FDFStrError(ret));
    if(FDF_FAILURE == ret)
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
    FDF_status_t ret = FDF_FAILURE;
    int tag = 0;
    testname[1] = "#test1: FDFInit / basic FDF operation / close FDF.";
    fprintf(fp,"%s\n",testname[1]);
    
    ret = FDFInit(&fdf_state1);  
    fprintf(fp,"FDFInit: %s\n",FDFStrError(ret));
    if(FDF_SUCCESS == ret)
    {
        tag += 1;
        ret = FDFInitPerThreadState(fdf_state1, &fdf_thrd_state);
        fprintf(fp,"FDFInitPerThreadState: %s\n",FDFStrError(ret));
        if(FDF_SUCCESS == ret)
        {
            tag += 1;
            (void)FDFReleasePerThreadState(&fdf_thrd_state);
        }
        (void)FDFShutdown(fdf_state1);
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
    FDF_status_t ret = FDF_FAILURE;
    testname[2] = "#test2: Double FDFInit and check whether it succedd.";
    fprintf(fp,"%s\n",testname[2]);

    ret = FDFInit(&fdf_state1);
    fprintf(fp,"-step1:%s\n",FDFStrError(ret));
    if(FDF_SUCCESS == ret)
    {
        ret = FDFInit(&fdf_state2);
        fprintf(fp,"-step2:%s\n",FDFStrError(ret));
        if(FDF_FAILURE == ret)
        {
            result[2] = 1;
        }
    }
    (void)FDFShutdown(fdf_state1);
    (void)FDFShutdown(fdf_state2);

    return result[2];
}
  
/************** main function ***********/

int main() 
{
    int testnumber = 2;
	int count      = 0;

    if((fp = fopen("FDF_Init.log", "w+")) == 0)
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
        fprintf(stderr, "#Test of FDFInit pass!\n");
	fprintf(stderr, "#The related test script is FDF_Init.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_Init.log\n");
    }else{
        fprintf(stderr, "#Test of FDFInit fail!\n");
	fprintf(stderr, "#The related test script is FDF_Init.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_Init.log\n");
    }


	return (!(1 == count));
}
