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
#function : ZSWriteObject
#author   : AliceXu
#date     : 2012.11.09
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include "zs.h"


FILE  *fp;
static struct ZS_state        *zs_state;
static struct ZS_thread_state *zs_thrd_state;
//ZS_config_t                   *fdf.config;
ZS_cguid_t                    cguid;
char *testname[10] = {NULL};
int result[2][10][3];
//uint32_t mode[6][4] = {{0,0,0,1},{0,1,0,1},{0,1,1,0},{0,1,1,1},{1,0,1,0},{1,0,1,1}};
//ZS_status_t debug[2][6][4] = {{1, 1, 1,1}};
//int data_out[2][4] = {0,0,0,0};

ZS_status_t pre_env()
{
    ZS_status_t ret = ZS_FAILURE;
//    (void)ZSLoadConfigDefaults(fdf.config, NULL);
//  if (ZSInit(&zs_state, fdf.config) != ZS_SUCCESS) {
    
    ret = ZSInit(&zs_state);
    if (ret != ZS_SUCCESS)
    {
        fprintf(fp, "ZS initialization failed!\n");
    }else{
        fprintf(fp, "ZS initialization succeed!\n");
        ret = ZSInitPerThreadState(zs_state, &zs_thrd_state);
        if( ZS_SUCCESS == ret)
        {
            fprintf(fp, "ZS thread initialization succeed!\n");
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
    fprintf(fp, "clear env!\n");
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
    fprintf(fp,"dura type: %d\n",dura);
    fprintf(fp,"ZSOpenContainer : %s\n",ZSStrError(ret));
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

/***************** test ******************/

int test_invalid_cguid(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[0] = "#test0: write object with invalid cguid.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[0]);

    ret = WriteObject(0, "xxxx", 5, "1111", 5, 1);
    if(ZS_CONTAINER_UNKNOWN == ret)
    {
        tag += 1;
        result[aw][0][0] = 1;
    }
    ret = WriteObject(-1, "xxxx", 5, "1111", 5, 1);
    if(ZS_CONTAINER_UNKNOWN == ret)
    {
        tag += 1;
        result[aw][0][1] = 1;
    }
    ret = WriteObject(1111111, "xxxx", 5, "1111", 5, 1);
    if(ZS_CONTAINER_UNKNOWN == ret)
    {
        tag += 1;
        result[aw][0][2] = 1;
    }
    return (3 == tag);
}

int test_write_with_close(uint32_t aw)
{
    int tag = 0;
    testname[1] = "#test1: write object with closed container.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[1]); 

    for(int i = 0; i < 3; i++)
    {
        if(ZS_SUCCESS == OpenContainer("c2", 1, aw, i))
        {
            if(ZS_SUCCESS == CloseContainer(cguid))
            {
                if(ZS_FAILURE == WriteObject(cguid, "x", 2, "xx", 3, 1))
                {
                    tag += 1;
                    result[aw][1][i] = 1;
                }
            }
        (void)DeleteContainer(cguid);
        }
    }
    return (3 == tag);
}

int test_rewrite_with_noexist_object(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[2] = "#test2: rewrite object with none exist object.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[2]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("c3", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            if(ZS_OBJECT_UNKNOWN == WriteObject(cguid, "xxxx", 5, "xxxx", 5, ZS_WRITE_MUST_EXIST))
            {
                tag += 1;
                result[aw][2][i] = 1;
            }
        (void)CloseContainer(cguid);
        (void)DeleteContainer(cguid);
       }
    }
    return (3 == tag);
}

int test_double_write(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[3] = "#test3: create object with object exist.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[3]);

    char *data;
    uint64_t datalen;
    
    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("c4", 1, aw, i);
  //      debug[aw][i][0] = ret;
        if(ZS_SUCCESS == ret)
        {
            ret = WriteObject(cguid, "xxx", 4, "xxxx", 5, 1);
    //        debug[aw][i][1] = ret;
            if(ZS_SUCCESS == ret)
             {
                ret = WriteObject(cguid, "xxx", 4, "yyyy", 5, 1);
      //          debug[aw][i][2] = ret;
                if(ZS_OBJECT_EXISTS == ret)
                {
                    tag += 1;
                    result[aw][3][i] = 1;
                }
                data = NULL;
                datalen = 0;
                ret = ReadObject(cguid, "xxx", 4, &data, &datalen);
        //        debug[aw][i][3] = ret;
                if((ZS_SUCCESS == ret)&&(!strcmp(data, "xxxx")))
                {
          //          data_out[aw][i] = 1;
                    free (data);
                    
                }
                (void)DeleteObject(cguid, "xxx", 4);
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }
    return (3 == tag);
}


int test_write_with_null_data(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[4] = "#test4: write object with data = null.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[4]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("c4", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            ret = WriteObject(cguid, "xxx", 4, NULL, 0, ZS_WRITE_MUST_NOT_EXIST);
            if(ZS_FAILURE == ret)
            {
                tag += 1;
                result[aw][4][i] = 1;
            }
        (void)CloseContainer(cguid);
        (void)DeleteContainer(cguid);
        }
    }
    return (3 == tag);
}

int test_basic_check(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[5] = "#test5: basic check write and rewrite";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[5]);

    char *key = "abcdefg";
    uint32_t keylen = 8;
    char *data;
    uint64_t datalen;

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("c4", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            ret = WriteObject(cguid, key, keylen, "123", 4, ZS_WRITE_MUST_NOT_EXIST);
            if(ZS_SUCCESS == ret)
            {
                ret = WriteObject(cguid, key, keylen, "123456789", 10, ZS_WRITE_MUST_EXIST);
                if(ZS_SUCCESS == ret)
                {
                    data = NULL;
                    datalen = 0;
                    (void)ReadObject(cguid, key, keylen, &data, &datalen);
                     if((!strcmp(data, "123456789")) &&(10 == datalen))
                     {
                        tag += 1;
                        result[aw][5][i] = 1;
                     }
                     ZSFreeBuffer(data);
                }
                (void)DeleteObject(cguid, key, keylen);
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
    testname[6] = "#test6: write with invalid keylen.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[6]);
    
    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("c6", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
    /*
            ret = WriteObject(cguid, NULL, 4, "123", 4, ZS_WRITE_MUST_NOT_EXIST);
            if(ZS_FAILURE == ret)
            {
                result[aw][6][i] += 1;
            }
      */
            ret = WriteObject(cguid, "11", 111111, "123", 4, ZS_WRITE_MUST_NOT_EXIST);
            if(ZS_FAILURE == ret)
            {
               result[aw][6][i] += 1;
            }
            ret = WriteObject(cguid, "11", 0, "123", 4, ZS_WRITE_MUST_NOT_EXIST);
            if(ZS_FAILURE == ret)
            {
               result[aw][6][i] += 1;
            }
            ret = WriteObject(cguid, "11", -4, "123", 4, ZS_WRITE_MUST_NOT_EXIST);
            if(ZS_FAILURE == ret)
            {
               result[aw][6][i] += 1;
            }
            if(3 == result[aw][6][i])
            {
                tag += 1;
                result[aw][6][i] = 1;
            }else{
                result[aw][6][i] = 0;
            }
            
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }
    return (3 == tag);

}

int test_write_invalid_flags(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[7] = "#test7: write object with invalid flags.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[7]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("c6", 1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            ret = WriteObject(cguid, "xxxx", 5, "123", 4, -1);
            if(ZS_FAILURE == ret)
            {
                result[aw][7][i] += 1;
            }
            ret = WriteObject(cguid, "xxxx", 5, "123", 4, 256);
            if(ZS_FAILURE == ret)
            {
                result[aw][7][i] += 1;
            }
            ret = WriteObject(cguid, "xxxx", 5, "123", 4, 171);
            if(ZS_FAILURE == ret)
            {
                result[aw][7][i] += 1;
            }
            ret = WriteObject(cguid, "xxxx", 5, "123", 4, 0);
            if(ZS_FAILURE == ret)
            {
                result[aw][7][i] += 1;
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
       }
       if(4 == result[aw][7][i])
       {
         tag += 1;
         result[aw][7][i] = 1;
       }else{
        result[aw][7][i] = 0;
       }
    }

    return (3 == tag);
}

int test_write_doubleopen(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[8] = "#test8: write object with double open container.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[8]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("c8",1, aw, i);
        if(ZS_SUCCESS == ret)
        {
            ret = WriteObject(cguid, "xxx", 4, "yyyy", 5, ZS_WRITE_MUST_NOT_EXIST);
            if(ZS_SUCCESS == ret)
            {
                (void)CloseContainer(cguid);
                ret = OpenContainer("c8", 4, aw, i);
                if(ZS_SUCCESS == ret)
                {
                    ret = WriteObject(cguid, "kkk", 4, "hhhh", 5, ZS_WRITE_MUST_NOT_EXIST);
                    if(ZS_SUCCESS == ret)
                    {
                        tag += 1;
                        result[aw][8][i] = 1;
                    }
                }
                (void)DeleteObject(cguid, "kkk", 4);
                (void)DeleteObject(cguid, "xxx", 4);
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }
    return (3 == tag);
}

/****** main function ******/

int main() 
{
    int testnumber = 9;
	int count      = 0;

    if((fp = fopen("ZS_WriteObject.log", "w+"))== 0)
    {
        fprintf(stderr, " open failed!.\n");
        return -1;
    }    
    if(ZS_SUCCESS == pre_env())
    {
        for(uint32_t aw = 0; aw < 2; aw++)
        {
//          count += test_invalid_cguid(aw);
//          count += test_write_with_close(aw);
            count += test_rewrite_with_noexist_object(aw);
//#10342            count += test_double_write(aw);
//          count += test_write_with_null_data(aw);
            count += test_basic_check(aw);
//            count += test_invalid_key_keylen(aw);
//            count += test_write_invalid_flags(aw);
            count += test_write_doubleopen(aw);
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
                fprintf(stderr, "[durability type = %d/] pass\n",j);
            }else {
	        fprintf(stderr, "[durability type = %d/] fail\n",j);
            }

/*
            if(3 == i)
            {
                fprintf(stderr,"-step1:%s\n",ZSStrError(debug[aw][j][0]));
                fprintf(stderr,"-step2:%s\n",ZSStrError(debug[aw][j][1]));
                fprintf(stderr,"-step3:%s\n",ZSStrError(debug[aw][j][2]));
                fprintf(stderr,"-step4:%s\n",ZSStrError(debug[aw][j][3]));
                if(1 == data_out[aw][j])
                {
                    fprintf(stderr, "get object data is : %s\n", "xxxx");
                }else{
                    fprintf(stderr, "get object data is : %s\n", "yyyy");
                }
            }
  */
        }
    }
    }
}

fprintf(stderr,"count is %d\n", count);
    if(3*2 == count)
    {
        fprintf(stderr, "#Test of ZSWriteObject pass!\n");
	fprintf(stderr, "#The related test script is ZS_WriteObject.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_WriteObject.log\n");
    }else{
        fprintf(stderr, "#Test of ZSWriteObject fail!\n");
	fprintf(stderr, "#The related test script is ZS_WriteObject.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_WriteObject.log\n");
    }
    return (!(3*2 == count));
}
