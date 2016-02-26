//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/****************************
#function : ZSFlushObject
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
    fprintf(fp,"clear env!\n");
}


ZS_status_t OpenContainer(char *cname, uint32_t flag,uint32_t asyncwrite,uint32_t dura)
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

ZS_status_t FlushObject(ZS_cguid_t cid,char *key,uint32_t keylen)
{
    ZS_status_t ret;
    ret = ZSFlushObject(zs_thrd_state,cid,key,keylen);
    fprintf(fp,"ZSFlushObject : %s\n",ZSStrError(ret));
    return ret;
}

/***************** test ******************/

int  test_invalid_cguids(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[0] = "#test0: flush object with invalid cguids.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[0]);
        
    ret = FlushObject(-1, "xxx", 4);
    if(ZS_FAILURE == ret)
    {
        tag += 1;
        result[aw][0][0] =1;
    }
    ret = FlushObject(0, "xxx", 4);
    if(ZS_FAILURE == ret)
    {
        tag += 1;
        result[aw][0][1] =1;
    }
    ret = FlushObject(1048580, "xxx", 4);
    if(ZS_FAILURE == ret)
    {
        tag += 1;
        result[aw][0][2] +=1;
    }
    return (3 == tag);
}

int test_invalid_object(uint32_t aw)
{
     ZS_status_t ret;
     int tag = 0;
     testname[1] = "#test1: flush object with invalid objects.";
     fprintf(fp,"****** async write = %d ******\n",aw);
     fprintf(fp,"%s\n",testname[1]);

     for(int i = 0; i < 3; i++)
     {
        ret = OpenContainer("test1", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            ret = FlushObject(cguid, "xxx", 4);
            if(ZS_OBJECT_UNKNOWN == ret)
            {
                result[aw][1][i] = 1;
                tag += 1;
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
    testname[2] = "#test2: open c/ write o/flush object with wrong key/keylen.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[2]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test2", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            ret = WriteObject(cguid, "aaa", 4, "sadf", 5, 1);
            if(ZS_SUCCESS == ret)
            {
/*    
                ret = FlushObject(cguid, NULL, 4);
                if(ZS_OBJECT_UNKNOWN == ret)
                {
                    result[aw][2][i] += 1;
                }
*/    
                ret = FlushObject(cguid, "aa", 4);
                if(ZS_OBJECT_UNKNOWN == ret)
                {
                    result[aw][2][i] += 1;
                }
                ret = FlushObject(cguid, "aaaaa", 4);
                if(ZS_OBJECT_UNKNOWN == ret)
                {
                    result[aw][2][i] += 1;
                }
                ret = FlushObject(cguid, "aaa", 0);
                if(ZS_OBJECT_UNKNOWN == ret)
                {
                    result[aw][2][i] += 1;
                }
                ret = FlushObject(cguid, "aaa", 1);
                if(ZS_OBJECT_UNKNOWN == ret)
                {
                    result[aw][2][i] += 1;
                }
                ret = FlushObject(cguid, "aaa", 10);
                if(ZS_OBJECT_UNKNOWN == ret)
                {
                    result[aw][2][i] += 1;
                }
                (void)DeleteObject(cguid, "aaa", 4);
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
        if(5 == result[aw][2][i])
        {
            result[aw][2][i] = 1;
            tag += 1;
        }else{
              result[aw][2][i] = 0;
        }
    }
    return (3 == tag);
}

int test_basic_check_1(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[3] = "#test3:basic check for flush object.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[3]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test3", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
             if(ZS_SUCCESS == WriteObject(cguid, "xxx", 4, "sadf", 5, 1))
             {
                ret = FlushObject(cguid, "xxx", 4);
                if(ZS_SUCCESS == ret)
                {
                    result[aw][3][i] += 1;
                }
                if(ZS_SUCCESS == WriteObject(cguid, "xxx", 4, "yyy", 4, 2))
                {
                    ret = FlushObject(cguid, "xxx", 4);
                    if(ZS_SUCCESS == ret)
                    {
                        result[aw][3][i] += 1;
                    }
                }
                (void)DeleteObject(cguid, "xxx", 4);
             }
             (void)CloseContainer(cguid);
             (void)DeleteContainer(cguid);
        }
        if(2 == result[aw][3][i])
        {
            result[aw][3][i] = 1;
            tag += 1;
        }else{
            result[aw][3][i] = 0;
        }
    }
    return (3 == tag);
}


/****** main function ******/

int main() 
{
    int testnumber = 4;
	int count      = 0;

    if((fp = fopen("ZS_FlushObject.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!.\n");
        return -1;
    }
     
    if(ZS_SUCCESS == pre_env())
    {
        for(uint32_t aw = 0; aw < 2; aw++)
        {
//            count += test_invalid_cguids(aw);
//            count += test_invalid_object(aw);
//            count += test_invalid_key_keylen(aw);
            count += test_basic_check_1(aw);
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
            if(NULL != testname[i])
            {
                fprintf(stderr, "%s\n", testname[i]);
                for(int j = 0; j < 3; j++)
                {
                    if(result[aw][i][j] == 1)
                    {
                        fprintf(stderr, "[durability type = %d] pass\n",j);
                    }else{
                        fprintf(stderr, "[durability type = %d] fail\n",j);
                    }
                }
            }
        }
    }

if(1*2 == count)
{
    fprintf(stderr, "#Test of ZSFlushObject pass!\n");
    fprintf(stderr, "#The related test script is ZS_FlushObject.c\n");
    fprintf(stderr, "#If you want, you can check test details in ZS_FlushObject.log\n");
}else{
    fprintf(stderr, "#Test of ZSFlushObject fail!\n");
    fprintf(stderr, "#The related test script is ZS_FlushObject.c\n");
    fprintf(stderr, "#If you want, you can check test details in ZS_FlushObject.log\n");
}

return (!(1*2 == count));
}
