/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/****************************
#function : ZSFlushCache
#author   : AliceXu
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "zs.h"

FILE *fp;
static struct ZS_state        *zs_state;
static struct ZS_thread_state *zs_thrd_state;
ZS_cguid_t                    cguid;
char *testname[10] = {NULL};
int result[2][10][3];
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
    fprintf(fp, "clear env!");
}

ZS_status_t OpenContainer(char *cname, uint32_t flag, uint32_t asyncwrite, uint32_t dura)
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
     fprintf(fp,"durability type: %d\n", dura);
     fprintf(fp,"ZSOpenContainer : %s\n", ZSStrError(ret));
     return ret;
}

ZS_status_t CloseContainer(ZS_cguid_t cid)
{
    ZS_status_t ret;
    ret = ZSCloseContainer(zs_thrd_state, cid);
    fprintf(fp,"ZSCloseContainer : %s\n", ZSStrError(ret));
    return ret;
}

ZS_status_t DeleteContainer(ZS_cguid_t cid)
{
    ZS_status_t ret;
    ret = ZSDeleteContainer(zs_thrd_state, cid);
    fprintf(fp, "ZSDeleteContainer : %s\n", ZSStrError(ret));
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

ZS_status_t FlushCache()
{
    ZS_status_t ret;
    ret = ZSFlushCache(zs_thrd_state);
    fprintf(fp,"ZSFlushCache : %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t SetContainerProps(ZS_cguid_t cid,ZS_container_props_t *pprops)
{
    ZS_status_t ret;
    ret = ZSSetContainerProps(zs_thrd_state,cid,pprops);
    fprintf(fp,"ZSSetContainerProps : %s\n",ZSStrError(ret));
    return ret;
}

/***************** test ******************/
int test_basic_check_0(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[0] = "#test0:flush cache with none containers in thread.";
    fprintf(fp,"******async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[0]);

    ret = FlushCache();
    if(ZS_SUCCESS == ret)
    {
        tag += 1;
        result[aw][0][0] = 1;
        result[aw][0][1] = 1;
        result[aw][0][2] = 1;
    }

    return (1 == tag);;
}

int test_basic_check_1(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[1] = "#test1:open c 1/close c/flush cache/ open c 2/flush cache.";
    fprintf(fp,"******async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[1]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test1", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            ret = CloseContainer(cguid);
            if(ZS_SUCCESS == ret)
            {
                ret = FlushCache();
                if(ZS_FAILURE == ret)
                {
                    result[aw][1][i] += 1;
                }
                ret = OpenContainer("test1", 2, aw, i);
                if(ZS_SUCCESS == ret)
                {
                    ret = FlushCache();
                    if(ZS_SUCCESS == ret)
                    {
                        result[aw][1][i] += 1;
                    }
                }
            }
        (void)CloseContainer(cguid);
        (void)DeleteContainer(cguid);
        }
        if(2 == result[aw][1][i])
        {
            result[aw][1][i] = 1;
            tag += 1;
        }else{
            result[aw][1][i] = 0;
        }
    }
    return (3 == tag);
}

int test_basic_check_2(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[2] = "#test2:open c 1/close c/flush cache/ open c 4/flush cache.";
    fprintf(fp,"******async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[2]);
    
    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test2", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            ret = CloseContainer(cguid);
            (void)OpenContainer("test2", 4, aw, i);
            (void)CloseContainer(cguid);
            if(ZS_SUCCESS == ret)
            {
                ret = FlushCache();
                if(ZS_FAILURE == ret)
                {
                    result[aw][2][i] += 1;
                }
                ret = OpenContainer("test1", 4, aw, i);
                if(ZS_SUCCESS == ret)
                {
                    ret = FlushCache();
                    if(ZS_SUCCESS == ret)
                    {
                        result[aw][2][i] += 1;
                    }
                }
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
        if(2 == result[aw][2][i])
        {
            result[aw][2][i] = 1;
            tag += 1;
        }else{
            result[aw][2][i] = 0;
        }
    }
    return (3 == tag);
}
 
int test_basic_check_3(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[3] = "#test3:open c/write o/flush cache.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[3]);

    char         *data;
    uint64_t     datalen;
    data = (char *)malloc(1*1024*1024*sizeof(char));
    datalen = 1*1024*1024*sizeof(char);
    memset(data,'a',datalen);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test3", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            ret = WriteObject(cguid, "xxxx", 5, data, datalen,1);
            if(ZS_SUCCESS == ret)
            {
                ret = FlushCache();
                if(ZS_SUCCESS == ret)
                {
                    result[aw][3][i] = 1;
                    tag += 1;
                }
                (void)DeleteObject(cguid, "xxxx", 5);
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }
    free(data);
    return (3 == tag);
}

int test_basic_check_4(uint32_t aw)
{
    ZS_status_t           ret;
    int tag = 0;
    testname[4] = "#test4: open c/write o/flush cache/set c props/flush cache.";
    fprintf(fp, "****** async write = %d ******\n",aw);
    fprintf(fp, "%s\n",testname[4]);

    ZS_container_props_t  props;
    props.size_kb = 2*1024*1024;
    props.persistent = ZS_FALSE;
    props.evicting = ZS_FALSE;
    props.fifo_mode = ZS_FALSE;

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test4", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            (void)WriteObject(cguid, "xxxx", 5, "pppp", 5, 1);
            (void)WriteObject(cguid, "yyyy", 5, "qqqq", 5, 1);
            ret = FlushCache();
            if(ZS_SUCCESS == ret)
            {
                result[aw][4][i] += 1;
            }
        }
        ret = SetContainerProps(cguid, &props);
        if(ZS_SUCCESS == ret)
        {
            ret = FlushCache();
            if(ZS_SUCCESS == ret)
            {
                result[aw][4][i] += 1;
            }
       }
       (void)DeleteObject(cguid, "xxxx", 5);
       (void)DeleteObject(cguid, "yyyy", 5);
       (void)CloseContainer(cguid);
       (void)DeleteContainer(cguid);

       if(2 == result[aw][4][i])
       {
           result[aw][4][i] = 1;
           tag += 1;
       }else{
           result[aw][4][i] = 0;
       }
    }
    return (3 == tag);
}


/****** main function ******/
int main() 
{
    int testnumber = 5;
    int count      = 0;
    
    if((fp = fopen("ZS_FlushCache.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!.\n");
        return -1;
    }
   if(ZS_SUCCESS == pre_env())
    {
        for(uint32_t aw = 0; aw < 2; aw++)
        {
            count += test_basic_check_0(aw);
//           count += test_basic_check_1(aw);
//10450      count += test_basic_check_2(aw);
            count += test_basic_check_3(aw);
            count += test_basic_check_4(aw);
        }
        clear_env();
    }
    fclose(fp);

    fprintf(stderr, "Test Result:\n");
    for(int aw = 0; aw < 2; aw++)
    {
        if(0 == aw)
        {
            fprintf(stderr, "# When disable async write:\n");
        }else{
            fprintf(stderr, "# When enable async write:\n");
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

    if(2*3 == count)
    {
        fprintf(stderr, "#Test of ZSFlushCache pass!\n");
	fprintf(stderr, "#The related test script is ZS_FlushCache.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_FlushCache.log\n");
    }else { 
        fprintf(stderr, "#Test of ZSFlushCache fail!\n");
	fprintf(stderr, "#The related test script is ZS_FlushCache.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_FlushCache.log\n");
    }

    return (!(2*3 == count));
}
