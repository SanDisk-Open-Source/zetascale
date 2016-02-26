/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/********************************
#function : ZSGetContainerStats
#author   : AliceXu
********************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "zs.h"

FILE *fp;
static struct ZS_state        *zs_state;
static struct ZS_thread_state *zs_thrd_state;
ZS_cguid_t                    cguid;
char *testname[10] = {NULL};
int result[2][10][6] = {{{0}}};
uint32_t mode[6][4] = {{0,0,0,1},{0,1,0,1},{0,1,1,0},{0,1,1,1},{1,0,1,0},{1,0,1,1}};

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
    fprintf(fp,"clear env!\n");
}

ZS_status_t OpenContainer(char *cname, uint32_t flag, uint32_t asyncwrite,
                           uint32_t fifo, uint32_t persist, uint32_t evicting,uint32_t wt)
{
    ZS_status_t          ret;
    ZS_container_props_t p;

    ret = ZS_FAILURE;
    (void)ZSLoadCntrPropDefaults(&p);
    p.async_writes = asyncwrite;
    p.durability_level = 0;
    p.fifo_mode = fifo;
    p.persistent = persist;
    p.writethru = wt;
    p.size_kb = 1024*1024;
    p.num_shards = 1;
    p.evicting = evicting;
 
    ret = ZSOpenContainer(
                        zs_thrd_state,
                        cname,
                        &p,
                        flag,
                        &cguid
                        );
    fprintf(fp,"container type: fifo=%d/persist=%d/evicting=%d/writethru=%d\n",fifo,persist,evicting,wt);
    fprintf(fp,"ZSOpenContainer: %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t CloseContainer(ZS_cguid_t cid)
{
    ZS_status_t ret;
    ret = ZSCloseContainer(zs_thrd_state, cid);
    fprintf(fp,"ZSCloseContainer : %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t DeleteContainer(ZS_cguid_t cid)
{
    ZS_status_t ret;
    ret = ZSDeleteContainer(zs_thrd_state, cid);
    fprintf(fp,"ZSDeleteContainer : %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t WriteObject(ZS_cguid_t cid,char *key,uint32_t keylen,char *data,uint64_t datalen,uint32_t flags)
{
    ZS_status_t ret;
    ret = ZSWriteObject(zs_thrd_state, cid, key, keylen, data, datalen, flags);
    fprintf(fp,"ZSWriteObject : %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t ReadObject(ZS_cguid_t cid,char *key,uint32_t keylen,char **data,uint64_t *datalen)
{
    ZS_status_t ret;
    ret = ZSReadObject(zs_thrd_state,cid,key,keylen,data,datalen);
    fprintf(fp,"ZSReadObject : %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t DeleteObject(ZS_cguid_t cid,char *key,uint32_t keylen)
{
    ZS_status_t ret;
    ret = ZSDeleteObject(zs_thrd_state,cid,key,keylen);
    fprintf(fp,"ZSDeleteObject : %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t GetContainerStats(ZS_cguid_t cid,ZS_stats_t *stats)
{
    ZS_status_t ret;
    ret = ZSGetContainerStats(zs_thrd_state,cid,stats);
    fprintf(fp,"ZSGetContainerStats : %s\n",ZSStrError(ret));
    return ret;
}

/***************** test ******************/

int  test_invalid_para(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[0] = "#test0: ZSGetContainerStats with invalid in parameters.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[0]);
 
    ZS_stats_t p;
    ret = GetContainerStats(2, NULL);
    if(ZS_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][0] =1;
    }
    ret =  GetContainerStats(-1, &p);
    if(ZS_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][1] =1;
    }
    ret =  GetContainerStats(0, &p);
    if(ZS_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][2] =1;
    }
    ret =  GetContainerStats(1111, &p);
    if(ZS_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][3] =1;
    }
    ret =  GetContainerStats(11111111, &p);
    if(ZS_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][4] =1;
    }
    ret =  GetContainerStats(-215, &p);
    if(ZS_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][5] =1;
    }
 
    return (6 == tag);
}

int test_basic_check(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[1] = "#test1: basic check.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[1]);
    
    ZS_stats_t stats1,stats2;
    for(int i = 0; i < 6; i++)
    {
        ret = OpenContainer("test1", 1, aw, mode[i][0], mode[i][1], mode[i][2], mode[i][3]);
        if(ZS_SUCCESS == ret)
        {
            (void)WriteObject(cguid, "xxxx", 5, "123", 4, ZS_WRITE_MUST_NOT_EXIST);
            (void)WriteObject(cguid, "yyyy", 5, "456", 4, ZS_WRITE_MUST_NOT_EXIST);
            ret = GetContainerStats(cguid, &stats1);
            if(ZS_SUCCESS == ret)
            {
                result[aw][1][i] += 1;
            }
            (void)DeleteObject(cguid, "yyyy", 5);
            (void)DeleteObject(cguid, "xxxx", 5);
            ret = GetContainerStats(cguid, &stats2);
            if(ZS_SUCCESS == ret)
            {
                result[aw][1][i] += 1;
            }
        }
        if(2 == result[aw][1][i])
        {
            tag += 1;
            result[aw][1][i] = 1;
        }else {
            result[aw][1][i] = 0;
        }
        (void)CloseContainer(cguid);
        (void)DeleteContainer(cguid);
    }
    return (6 == tag);
}


/****** main function ******/

int main() 
{
    int testnumber = 2;
	int count      = 0;

    if((fp = fopen("ZS_GetContainerStats.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!.\n");
        return -1;
    }

    if(ZS_SUCCESS == pre_env())
    {
        for(uint32_t aw = 0; aw < 2; aw++)
        {
            count += test_invalid_para(aw);
    	    count += test_basic_check(aw);
         }
         clear_env();
    }
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
            fprintf(stderr, "%s\n", testname[i]);
            for(int j = 0; j < 5; j++)
            {
            if(result[aw][i][j] == 1)
            {
                fprintf(stderr, "[mode fifo=%d/persist=%d/evict=%d/writethru=%d] pass\n",mode[j][0],mode[j][1],mode[j][2],mode[j][3]);
            }else{
                fprintf(stderr, "[mode fifo=%d/persist=%d/evict=%d/writethru=%d] fail\n",mode[j][0],mode[j][1],mode[j][2],mode[j][3]);
            }
        }
    }
   }
   if(testnumber*2 == count)
   {
      fprintf(stderr, "#Test of ZSGetContainerStats pass!\n");
   }else{
      fprintf(stderr, "#Test of ZSGetContainerStats fail!\n");
   }
   return (!(testnumber*2 == count));
}
