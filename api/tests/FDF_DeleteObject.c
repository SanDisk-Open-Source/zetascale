/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/****************************
#function : ZSDeleteObject
#author   : AliceXu
#date     : 2012.11.10
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "zs.h"

FILE *fp;
static struct ZS_state        *zs_state;
static struct ZS_thread_state *zs_thrd_state;
//ZS_config_t                   *fdf.config;
ZS_cguid_t                    cguid;
char *testname[10] = { NULL } ;
int result[2][10][3] = {{{0}}};
//uint32_t mode[6][4] = {{0,0,0,1},{0,1,0,1},{0,1,1,0},{0,1,1,1},{1,0,1,0},{1,0,1,1}};

ZS_status_t pre_env()
{
    ZS_status_t ret;

//    (void)ZSLoadConfigDefaults(fdf.config, NULL);
//  if (ZSInit(&zs_state, fdf.config) != ZS_SUCCESS) {
    
    ret = ZSInit(&zs_state);
    if (ret != ZS_SUCCESS)
    {
        fprintf(fp, "ZS initialization failed!\n");
    }else{
        fprintf(fp, "ZS initialization passed!\n");
        ret = ZSInitPerThreadState(zs_state, &zs_thrd_state);
        if( ZS_SUCCESS == ret)
        {
            fprintf(fp, "ZS thread initialization passed!\n");
        }else{
            fprintf(fp, "ZS thread initialization failed!\n");
        }
    }
    return ret;
}

void clear_env()
{
    (void)ZSReleasePerThreadState(&zs_thrd_state);
    (void)ZSShutdown(zs_state);
    fprintf(fp, "clear env\n");
}

ZS_status_t OpenContainer(char *cname, uint32_t flag, uint32_t asyncwrite, 
                           uint32_t dura)
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
    fprintf(fp,"ZSOpenContainer : ");
    fprintf(fp,"container type: %d\n",dura);
    fprintf(fp,"open result: %s\n",ZSStrError(ret));
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

ZS_status_t WriteObject(ZS_cguid_t cid,char *key,uint32_t keylen,char *data,uint64_t datalen,uint32_t flags)
{
    ZS_status_t ret;
    ret = ZSWriteObject(zs_thrd_state, cid, key, keylen, data, datalen, flags);
    fprintf(fp,"ZSWriteObject : ");
    fprintf(fp,"%s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t ReadObject(ZS_cguid_t cid,char *key,uint32_t keylen,char **data,uint64_t *datalen)
{
    ZS_status_t ret;
    ret = ZSReadObject(zs_thrd_state,cid,key,keylen,data,datalen);
    fprintf(fp,"ZSReadObject : ");
    fprintf(fp,"%s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t DeleteObject(ZS_cguid_t cid,char *key,uint32_t keylen)
{
    ZS_status_t ret;
    ret = ZSDeleteObject(zs_thrd_state,cid,key,keylen);
    fprintf(fp,"ZSDeleteObject : ");
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

int test_with_invalid_cguid(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[0] = "#test0:delete object with invalid cguids.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[0]);

    ret = DeleteObject(0, "xxxx", 5);
    if(ZS_CONTAINER_UNKNOWN == ret)
    {
        tag += 1;
        result[aw][0][0] = 1;
    }
    ret = DeleteObject(-1, "xxxx", 5);
    if(ZS_CONTAINER_UNKNOWN == ret)
    {
        tag += 1;
        result[aw][0][1] = 1;
    }
    ret = DeleteObject(11111111111, "xxxx", 5);
    if(ZS_CONTAINER_UNKNOWN == ret)
    {
        tag += 1;
        result[aw][0][2] = 1;
    }
   return (3 == tag);
}

int test_with_noexist_object(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[1] = "#test1: delete none-exist objects.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[1]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test1", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            ret = DeleteObject(cguid, "xxxx", 5);
            if(ZS_OBJECT_UNKNOWN == ret)
            {
                tag += 1;
                result[aw][1][i] = 1;
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }
    return (3 == tag);
}

int test_close_open_deleteobject(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[2] = "#test2:open c/write o/close c/delete o/openc/delete o.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[2]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test2", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            ret = WriteObject(cguid, "xxxx", 5, "yyyyyyy", 7, 1);
            if(ZS_SUCCESS == ret)
            {
                if(ZS_SUCCESS == CloseContainer(cguid))
                {

                    ret = DeleteObject(cguid, "xxxx", 5);
                    if(ZS_FAILURE == ret)
                    {
                       result[aw][2][i] += 1;
                    }
                   if(ZS_SUCCESS == OpenContainer("test2", 4, aw, i))
                   {
                    ret = DeleteObject(cguid, "xxxx", 5);
                    if(ZS_SUCCESS == ret)
                    {
                        result[aw][2][i] += 1;
                    }
                   }
                }
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
        if(2 == result[aw][2][i])
        {
            tag += 1;
            result[aw][2][i] = 1;
        }else{
            result[aw][2][i] = 0;
        }
    }
    return (3 == tag);
}

int test_basic_check(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[3] = "test3: delete object basic check.";
    fprintf(fp,"******* async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[3]);

    char *data;
    uint64_t datalen = 1*1024*1024*sizeof(char);
    data = (char *)malloc(datalen);
    memset(data, 'x', datalen);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test3", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            ret = WriteObject(cguid, "xxxx", 5, data, datalen, 1);
            if(ZS_SUCCESS == ret)
            {
                ret = DeleteObject(cguid, "xxxx", 5);
                if(ZS_SUCCESS == ret)
                {
                    tag += 1;
                    result[aw][3][i] = 1;
                }
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }
    free (data);

    return (3 == tag);
}

int test_double_deleteobject(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[4] = "#test4:double delete object.";
    fprintf(fp,"******* async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[4]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test4", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            if(ZS_SUCCESS == WriteObject(cguid, "xxx", 4, "xxxx", 5, ZS_WRITE_MUST_NOT_EXIST))
            {
                ret = DeleteObject(cguid, "xxx", 4);
                if(ZS_SUCCESS == ret)
                {
                    ret = DeleteObject(cguid, "xxx", 4);
                    if(ZS_OBJECT_UNKNOWN == ret)
                    {
                        tag += 1;
                        result[aw][4][i] = 1;
                    }
                }
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }
    return (3 == tag);
}

int test_doublewrite_delete(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[5] = "#test5: write twice and delete object. ";
    fprintf(fp,"******* async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[5]);
    char *data;
    uint64_t datalen;
    data = (char *)malloc(1*1024*1024*sizeof(char));
    datalen = 1*1024*1024*sizeof(char);
    memset(data, 'y', datalen);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test4", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            if(ZS_SUCCESS == WriteObject(cguid, "dd", 3, data, datalen, ZS_WRITE_MUST_NOT_EXIST))
            {
                if(ZS_SUCCESS == WriteObject(cguid, "dd", 3, "jssd", 5, ZS_WRITE_MUST_EXIST))
                {
                     ret = DeleteObject(cguid, "dd", 3);
                     if(ZS_SUCCESS == ret)
                     {
                        tag += 1;
                        result[aw][5][i] = 1;
                     }
                }
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }
    return (3 == tag);
}

int test_invalid_key_keylen(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[6] = "#test6: delete object with invalid key/keylen.";
    fprintf(fp,"******* async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[6]);
    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test4", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            (void)WriteObject(cguid, "dd", 3, "sss", 4, 1);
/*
            ret = DeleteObject(cguid, NULL, 3);
            if(ZS_FAILURE == ret)
            {
                result[6][i] += 1;
            }
  */
            ret = DeleteObject(cguid, "dd", 0);
            if(ZS_FAILURE == ret)
            {
                result[aw][6][i] += 1;
            }
            ret = DeleteObject(cguid, "dd", 555);
            if(ZS_FAILURE == ret)
            {
                result[aw][6][i] += 1;
            }
            ret = DeleteObject(cguid, "sss", 3);
            if(ZS_FAILURE == ret)
            {
                result[aw][6][i] += 1;
            }
            ret = DeleteObject(cguid, "dd", -1);
            if(ZS_FAILURE == ret)
            {
                result[aw][6][i] += 1;
            }
            (void)DeleteObject(cguid, "dd", 3);
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
        if(5 == result[aw][6][i])
        {
            result[aw][6][i] = 1;
            tag += 1;
        }else{
            result[aw][6][i] = 0;
        }
    }
    return (3 == tag);
}
 

/****** main function ******/

int main() 
{
    int testnumber = 7;
	int count      = 0;
    
    if((fp = fopen("ZS_DeleteObject.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!.\n");
        return -1;
    }

    if( ZS_SUCCESS == pre_env())
    {
        for(uint32_t aw = 0; aw < 2; aw++)
        {
//          count += test_with_invalid_cguid(aw);
            count += test_with_noexist_object(aw);
//          count += test_close_open_deleteobject(aw);
            count += test_basic_check(aw);
            count += test_double_deleteobject(aw);
            count += test_doublewrite_delete(aw);
//          count += test_invalid_key_keylen(aw);
        }
        clear_env();
    }
    fclose(fp);
        
    fprintf(stderr, "Test result:\n");
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
    if(4*2 == count)
    {
        fprintf(stderr, "#Test of ZSDeleteObject pass!\n");
	fprintf(stderr, "#The related test script is ZS_DeleteObject.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_DeleteObject.log\n");
    }else{
        fprintf(stderr, "#Test of ZSDeleteObject fail!\n");
	fprintf(stderr, "#The related test script is ZS_DeleteObject.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_DeleteObject.log\n");
    }

	return (!(4*2 == count));
}
