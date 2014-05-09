/****************************
#function : FDFGetContainers
#author   : AliceXu
#date     : 2012.11.08
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
int result[2][10][3] = {{{0}}};
//uint32_t mode[6][4] = {{0,0,0,1},{0,1,0,1},{0,1,1,0},{0,1,1,1},{1,0,1,0},{1,0,1,1}};


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

FDF_status_t OpenContainer(char *cname, uint32_t flag, uint32_t asyncwrite,uint32_t dura)
{
    FDF_status_t          ret;
    FDF_container_props_t p;

    ret = FDF_FAILURE;  
    (void)FDFLoadCntrPropDefaults(&p);
    p.async_writes = asyncwrite;
    p.durability_level = dura;
    p.fifo_mode = 0;
    p.persistent = 1;
    p.writethru = 1;
    p.size_kb = 1024*1024;
    p.num_shards = 1;
    p.evicting = 0;
 
    ret = FDFOpenContainer(
                        fdf_thrd_state,
                        cname,
                        &p,
                        flag,
                        &cguid
                        );
    fprintf(fp, "Open container:");
    fprintf(fp, "durability type = %d\n",dura);
    fprintf(fp,"result:%s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t CloseContainer(FDF_cguid_t cid)
{
    FDF_status_t ret;
    ret = FDFCloseContainer(fdf_thrd_state, cid);
    fprintf(fp,"FDFCloseContainer : ");
    fprintf(fp,"%s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t DeleteContainer(FDF_cguid_t cid)
{
    FDF_status_t ret;
    ret = FDFDeleteContainer(fdf_thrd_state, cid);
    fprintf(fp,"FDFDeleteContainer : ");
    fprintf(fp,"%s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t GetContainers(FDF_cguid_t *cid, uint32_t *n_cguids)
{
    FDF_status_t ret;
    ret = FDFGetContainers(fdf_thrd_state, cid, n_cguids);
    fprintf(fp,"GetContainers : ");
    fprintf(fp,"%s\n",FDFStrError(ret));
    return ret;
}

/***************** test ******************/

int test_get_with_nocontainer(uint32_t aw)
{
    FDF_status_t  ret;
    int tag = 0;
    FDF_cguid_t   cguid_out[1] = {0};
    uint32_t      number;
    testname[0] = "#test:get container with no container exists.";
    fprintf(fp, "****** async write = %d ******\n",aw);
    fprintf(fp, "%s\n",testname[0]);

    ret = GetContainers(cguid_out, &number);
    if((FDF_SUCCESS ==ret) && (0 == number) && (0 == cguid_out[0]))
    {
        tag += 1;
        result[aw][0][0] = 1;
        result[aw][0][1] = 1;
        result[aw][0][2] = 1;
        fprintf(fp, "$the test -> pass\n");
    }else {
        fprintf(fp, "$the test -> fail\n");
    }
    return (1 == tag);
}

int test_basic_check(uint32_t aw)
{
    FDF_status_t  ret;
    int tag = 0;
    uint32_t      number;
    FDF_cguid_t   cguid_out[1];
    testname[1] = "#test1: basic check.";
    fprintf(fp, "****** async write = %d ******\n",aw);
    fprintf(fp, "%s\n",testname[1]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test1", 1, aw, i);
        if(FDF_SUCCESS == ret)
        {
            cguid_out[0] = 0;
            number = 0;
            ret = GetContainers(cguid_out, &number);
            if((FDF_SUCCESS == ret) && (1 == number) && (cguid == cguid_out[0]))
            {
                result[aw][1][i] += 1;
            }
        }
        (void)CloseContainer(cguid_out[0]);
        (void)DeleteContainer(cguid_out[0]);
        cguid_out[0] = 0;
        number = 0;
        ret = GetContainers(cguid_out, &number);
        if((FDF_SUCCESS == ret) && (0 == number) && (0 == cguid_out[0]))
        {
            result[aw][1][i] += 1;
        }
        if(2 == result[aw][1][i])
        {
             tag += 1;
             result[aw][1][i] = 1;
             fprintf(fp, "$the test -> pass\n");
        }else{
             result[aw][1][i] = 0;
             fprintf(fp, "$the test -> fail\n");
        }
    }
    return (3 == tag);
}

/****** main function ******/

int main() 
{
    int testnumber = 2;
	int count      = 0;

    if((fp = fopen("FDF_GetContainers.log", "w+")) == 0){
        fprintf(stderr, " open failed!.\n");
        return -1;
    }
    if(FDF_SUCCESS == pre_env())
    {
        for(uint32_t aw = 0; aw < 2; aw++)
        {
            count += test_get_with_nocontainer(aw);
            count += test_basic_check(aw);
        }
    } else {
		return -1;
	}
    clear_env();
    fclose(fp);
   
    fprintf(stderr, "Test Result:\n");
    for(int aw = 0; aw < 2; aw++)
    {
        if(0 == aw)
        {
            fprintf(stderr, "***** When disable async write: *****\n");
        }else{
            fprintf(stderr, "***** When enable async write: *****\n");
        }
        for(int i = 0; i < testnumber; i++)
        {
            if(NULL != testname[i])
            {
                fprintf(stderr, "%s\n", testname[i]);
                for(int j = 0; j < 3; j++)
                {
                    if(result[aw][i][j] == 1)
                    {
                        fprintf(stderr, "[durability type = %d] pass\n", j);
                    }else{
                        fprintf(stderr, "[durability type = %d] fail\n", j);
                    }
                }
            }
        }
    }
    if(testnumber*2 == count)
    {
       fprintf(stderr, "#Test of FDFGetContainers pass!\n");
    }else{
       fprintf(stderr, "#Test of FDFGetContainers fail!\n");
    }
    return (!(testnumber*2 == count));
}
