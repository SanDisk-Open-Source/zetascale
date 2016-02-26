/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/****************************
#function : ZSReadObjectExpiry
#author   : RoyGao
#date     : 2013.1.28
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "zs.h"

static FILE *fp;
static struct ZS_state        *zs_state;
static struct ZS_thread_state *zs_thrd_state;
ZS_cguid_t                    cguid;
char* testname[10] = {NULL};
int result[2][3][3];

ZS_status_t pre_env()
{
    ZS_status_t ret;
   
    ret = ZSInit(&zs_state);
    if(ret != ZS_SUCCESS)
    {
        fprintf(fp, "ZS initialization failed!\n");
    }else{
        fprintf(fp, "ZS initialization succeed!\n");
        ret = ZSInitPerThreadState(zs_state, &zs_thrd_state);
        if( ZS_SUCCESS == ret)
        {
            fprintf(fp, "ZS thread initialization succeed!\n");
        }else{
            fprintf(fp, "ZS thread initialization fail!\n");
            }
    }
    return ret;
}

void clear_env()
{
    (void)ZSReleasePerThreadState(&zs_thrd_state);
    (void)ZSShutdown(zs_state);
    fprintf(stderr,"clear env!\n");
}

ZS_status_t opencontainer(char *cname, uint32_t flag, uint32_t asyncwrite, uint32_t dura)
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
    fprintf(stderr,"durability type: %d\n",dura);
    fprintf(stderr,"ZSOpenContainer: %s\n",ZSStrError(ret));
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

ZS_status_t WriteObjectExpiry(ZS_cguid_t cid,ZS_writeobject_t* wobj,uint32_t flags)
{
    ZS_status_t              ret;

    ret = ZSWriteObjectExpiry(zs_thrd_state, cid, wobj, flags);
    if(ret != ZS_SUCCESS)
    {
        fprintf(fp,"ZSWriteObjectExpiry : %s\n",ZSStrError(ret));
    }
    return ret;
}


ZS_status_t ReadObjectExpiry(ZS_cguid_t cid,ZS_readobject_t* robj)
{
    ZS_status_t              ret;

    ret = ZSReadObjectExpiry(zs_thrd_state, cid, robj);
    if(ret != ZS_SUCCESS)
    {
        fprintf(fp,"ZSReadObjectExpiry : %s\n",ZSStrError(ret));
    }
    return ret;
}
/****** basic test *******/

int test_basic_check(uint32_t aw)
{
    ZS_status_t ret;
    ZS_writeobject_t wobj;
    ZS_readobject_t robj;
    int tag = 0; 
    testname[0] = "#test1: readobjectexpiry with basic check.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[0]);

    wobj.key = "case0";
    wobj.key_len = 6;
    wobj.data = "512";
    wobj.data_len = 4;
    wobj.current = 100;
    wobj.expiry = 100;
    
    robj.key = "case0";
    robj.key_len = 6;
    robj.current = 250;
    
    for(int i = 0;i < 3; i++)
    {
        if(ZS_SUCCESS == opencontainer("c0", 1, aw, i))
        {  
            if(ZS_SUCCESS == WriteObjectExpiry(cguid,&wobj,1))
            {
                ret = ReadObjectExpiry(cguid,&robj);
                if((ZS_EXPIRED == ret))
                {
                    tag++;
                    result[aw][0][i] = 1;
                }
                else
                free(robj.data);
                robj.data = NULL;
                robj.data_len = 0;
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }

    robj.current = 90;
    for(int i = 0;i < 3; i++)
    {
        if(ZS_SUCCESS == opencontainer("c0", 1, aw, i))
        {  
            if(ZS_SUCCESS == WriteObjectExpiry(cguid,&wobj,1))
            {
                ret = ReadObjectExpiry(cguid,&robj);
                if((ZS_SUCCESS == ret)&&(4 == robj.data_len)&&(!strcmp("512",robj.data)))
                {
                    tag++;
                }
                else
                result[aw][0][i] = 0; 
                free(robj.data);
                robj.data = NULL;
                robj.data_len = 0;
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }
    return (6 == tag);
}

/****** Update Expiry Ops ******/

int test_update_expiry_check(uint32_t aw)
{
    ZS_status_t ret;
    ZS_writeobject_t wobj1,wobj2;
    ZS_readobject_t robj;
    int tag = 0; 
    testname[1] = "#test2: readobjectexpiry with update expiry.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[1]);

    wobj1.key = "case1";
    wobj1.key_len = 6;
    wobj1.data = "512";
    wobj1.data_len = 4;
    wobj1.current = 100;
    wobj1.expiry = 90;
    
    wobj2.key = "case1";
    wobj2.key_len = 6;
    wobj2.data = "1024";
    wobj2.data_len = 5;
    
    robj.key = "case1";
    robj.key_len = 6;
    
    for(int i = 0;i < 3; i++)
    {
        wobj2.current = 80;
        wobj2.expiry = 100;
        robj.current = 90;
        if(ZS_SUCCESS == opencontainer("c0", 1, aw, i))
        {  
            if(ZS_SUCCESS == WriteObjectExpiry(cguid,&wobj1,ZS_WRITE_MUST_NOT_EXIST))
            {
                ret = WriteObjectExpiry(cguid,&wobj2,ZS_WRITE_MUST_EXIST);
                if(ZS_SUCCESS == ret)
                {
                    result[aw][1][i] = 1;
                }
                ret = ReadObjectExpiry(cguid,&robj);
                if(ZS_SUCCESS == ret && robj.expiry == 100)
                {
                    free(robj.data);
                    robj.data = NULL;
                    robj.data_len = 0;
                }
                else
                    result[aw][1][i] = 0; 
                wobj2.current = 110;
                ret = WriteObjectExpiry(cguid,&wobj2,ZS_WRITE_MUST_EXIST);
                
                if(ZS_EXPIRED != ret) 
                {
                    result[aw][1][i] = 0;
                }
                robj.current = 50;
                ret = ReadObjectExpiry(cguid,&robj);
                if(ZS_OBJECT_UNKNOWN == ret)
                {
                    tag++;
                }
                else
                {
                    result[aw][1][i] = 0;
                    free(robj.data);
                }
                robj.data = NULL;
                robj.data_len = 0;
            }   
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }

    return (3 == tag);
}

/****** Multi keys expired ******/

int test_multikeys_expiry_check(uint32_t aw)
{
    ZS_status_t ret;
    ZS_writeobject_t* wobjs;
    ZS_readobject_t* robjs;
    char keyname[15];
    int tag = 0; 
    testname[2] = "#test3: readobjectexpiry with multi keys expired.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[2]);
    
    char value[1024];
    memset(value,'a',1024);
    
    wobjs = (ZS_writeobject_t*)malloc(1024*512*sizeof(ZS_writeobject_t));
    robjs = (ZS_readobject_t*)malloc(1024*512*sizeof(ZS_readobject_t));

    for(int i = 0;i < 3; i++)
    {
        if(ZS_SUCCESS == opencontainer("c0", 1, aw, i))
        {  
            result[aw][2][i] = 1;
            for (long j=0;j < 1024*512;j++)
            {
                wobjs[j].data = value;
                wobjs[j].data_len = 1024;
                wobjs[j].current = 100;
                wobjs[j].expiry = 100;
                sprintf(keyname,"%s%ld","key",j);
                wobjs[j].key = keyname;
                wobjs[j].key_len = strlen(keyname)+1;
                if(ZS_SUCCESS != WriteObjectExpiry(cguid,&(wobjs[j]),ZS_WRITE_MUST_NOT_EXIST))
                    result[aw][2][i] = 0;
            }
            for (long j=0;j < 1024*512;j++)
            {
                sprintf(keyname,"%s%ld","key",j);
                robjs[j].current = 90;
                robjs[j].expiry = 100;
                robjs[j].key = keyname;
                robjs[j].key_len = strlen(keyname)+1;
                ret = ReadObjectExpiry(cguid,&(robjs[j]));
                if(ZS_SUCCESS != ret)
                    result[aw][2][i] = 0;
                else
                {
                    free(robjs[j].data);
                    robjs[j].data = NULL;
                    robjs[j].data_len = 0;
              
               }
            }
            for (long j=0;j < 1024*512;j++)
            {
                sprintf(keyname,"%s%ld","key",j);
                robjs[j].current = 110;
                robjs[j].expiry = 100;
                robjs[j].key = keyname;
                robjs[j].key_len = strlen(keyname)+1;
                ret = ReadObjectExpiry(cguid,&(robjs[j]));
                if(ZS_EXPIRED != ret)
                    result[aw][2][i] = 0;
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
            if(result[aw][2][i] == 1)
                tag++;
        }
    }
    free(wobjs);
    free(robjs);
       
    return (3 == tag);
}
/****** main function ******/

int main() 
{
    int testnumber = 3;
    int count = 0;

    if((fp = fopen("ZS_ReadObjectExpiry.log","w+")) == 0)
    {
        fprintf(stderr, "open log file failed! \n");
        return -1;
    }
    if(ZS_SUCCESS == pre_env())
    {
        for(uint32_t aw = 0; aw < 2; aw++)
        {
            count +=test_basic_check(aw);
            count +=test_update_expiry_check(aw);
            count +=test_multikeys_expiry_check(aw);
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
        }
        else
        {
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
                }
                else
                {
                    fprintf(stderr, "[durability type = %d] fail\n", j);
                }
            }
        }
        }
    }
    if(3*2 == count)
    {
        fprintf(stderr, "#Test of ZSReadObjectExpiry pass!\n");
    }else{
        fprintf(stderr, "#Test of ZSReadObjectExpiry fail!\n");
    }

	return (!(3*2 == count));
}
