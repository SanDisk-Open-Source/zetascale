/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/****************************
#function : ZSGetContainers
#author   : AliceXu
#date     : 2012.11.08
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "zs.h"

FILE *fp;
static struct ZS_state        *zs_state;
static struct ZS_thread_state *zs_thrd_state;
//ZS_config_t                   *fdf.config;
ZS_cguid_t                    cguid;
char *testname[10] = {NULL};
int result[2][10][3] = {{{0}}};
//uint32_t mode[6][4] = {{0,0,0,1},{0,1,0,1},{0,1,1,0},{0,1,1,1},{1,0,1,0},{1,0,1,1}};


ZS_status_t pre_env()
{
     ZS_status_t ret = ZS_FAILURE;
   //(void)ZSLoadConfigDefaults(fdf.config, NULL);
   //  if (ZSInit(&zs_state, fdf.config) != ZS_SUCCESS) {
     ret = ZSInit(&zs_state);
     if (ZS_SUCCESS != ret)
     {
        fprintf(fp, "ZS initialization failed!\n");
     }else {
        fprintf(fp, "ZS initialization succeed!\n");
        ret = ZSInitPerThreadState(zs_state, &zs_thrd_state);
        if( ZS_SUCCESS == ret)
        {
            fprintf(fp, "ZS thread initialization succeed!\n");
        }
     }
     return ret;
}

void clear_env()
{
    (void)ZSReleasePerThreadState(&zs_thrd_state);
    (void)ZSShutdown(zs_state);
    fprintf(fp, "clear env!\n");
}                       

ZS_status_t OpenContainer(char *cname, uint32_t flag, uint32_t asyncwrite,uint32_t dura)
{
    ZS_status_t          ret;
    ZS_container_props_t p;

    ret = ZS_FAILURE;  
    (void)ZSLoadCntrPropDefaults(&p);
    p.async_writes = asyncwrite;
    p.durability_level = dura;
    p.fifo_mode = 0;
    p.persistent = 1;
    p.writethru = 1;
    p.size_kb = 1024*1024;
    p.num_shards = 1;
    p.evicting = 0;
 
    ret = ZSOpenContainer(
                        zs_thrd_state,
                        cname,
                        &p,
                        flag,
                        &cguid
                        );
    fprintf(fp, "Open container:");
    fprintf(fp, "durability type = %d\n",dura);
    fprintf(fp,"result:%s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t CloseContainer(ZS_cguid_t cid)
{
    ZS_status_t ret;
    ret = ZSCloseContainer(zs_thrd_state, cid);
    fprintf(fp,"ZSCloseContainer : ");
    fprintf(fp,"%s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t DeleteContainer(ZS_cguid_t cid)
{
    ZS_status_t ret;
    ret = ZSDeleteContainer(zs_thrd_state, cid);
    fprintf(fp,"ZSDeleteContainer : ");
    fprintf(fp,"%s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t GetContainers(ZS_cguid_t *cid, uint32_t *n_cguids)
{
    ZS_status_t ret;
    ret = ZSGetContainers(zs_thrd_state, cid, n_cguids);
    fprintf(fp,"GetContainers : ");
    fprintf(fp,"%s\n",ZSStrError(ret));
    return ret;
}

/***************** test ******************/

int test_get_with_nocontainer(uint32_t aw)
{
    ZS_status_t  ret;
    int tag = 0;
    ZS_cguid_t   cguid_out[1] = {0};
    uint32_t      number;
    testname[0] = "#test:get container with no container exists.";
    fprintf(fp, "****** async write = %d ******\n",aw);
    fprintf(fp, "%s\n",testname[0]);

    ret = GetContainers(cguid_out, &number);
    if((ZS_SUCCESS ==ret) && (0 == number) && (0 == cguid_out[0]))
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
    ZS_status_t  ret;
    int tag = 0;
    uint32_t      number;
    ZS_cguid_t   cguid_out[1];
    testname[1] = "#test1: basic check.";
    fprintf(fp, "****** async write = %d ******\n",aw);
    fprintf(fp, "%s\n",testname[1]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test1", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            cguid_out[0] = 0;
            number = 0;
            ret = GetContainers(cguid_out, &number);
            if((ZS_SUCCESS == ret) && (1 == number) && (cguid == cguid_out[0]))
            {
                result[aw][1][i] += 1;
            }
        }
        (void)CloseContainer(cguid_out[0]);
        (void)DeleteContainer(cguid_out[0]);
        cguid_out[0] = 0;
        number = 0;
        ret = GetContainers(cguid_out, &number);
        if((ZS_SUCCESS == ret) && (0 == number) && (0 == cguid_out[0]))
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

    if((fp = fopen("ZS_GetContainers.log", "w+")) == 0){
        fprintf(stderr, " open failed!.\n");
        return -1;
    }
    if(ZS_SUCCESS == pre_env())
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
       fprintf(stderr, "#Test of ZSGetContainers pass!\n");
    }else{
       fprintf(stderr, "#Test of ZSGetContainers fail!\n");
    }
    return (!(testnumber*2 == count));
}
