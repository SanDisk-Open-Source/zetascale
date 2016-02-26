/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*********************************************
**********   Author:  Lisa

**********   Function: ZSFinishEnumeration
***********************************************/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "zs.h"
static struct ZS_state     *zs_state;
struct ZS_thread_state     *_zs_thd_state;
ZS_container_props_t       p;
ZS_cguid_t                 cguid;
FILE                        *fp;
int                         testCount = 0;

int preEnvironment()
{
   /* ZS_config_t            fdf.config;

    fdf.config.version                      = 1;
    fdf.config.n_flash_devices              = 1;
    fdf.config.flash_base_name              = "/schooner/data/schooner%d";
    fdf.config.flash_size_per_device_gb     = 12;
    fdf.config.dram_cache_size_gb           = 8;
    fdf.config.n_cache_partitions           = 100;
    fdf.config.reformat                     = 1;
    fdf.config.max_object_size              = 1048576;
    fdf.config.max_background_flushes       = 8;
    fdf.config.background_flush_msec        = 1000;
    fdf.config.max_outstanding_writes       = 32;
    fdf.config.cache_modified_fraction      = 1.0;
    fdf.config.max_flushes_per_mod_check    = 32;
*/
    //ZSLoadConfigDefaults(&zs_state);
    //if(ZSInit( &zs_state, &fdf.config ) != ZS_SUCCESS ) {
    if(ZSInit( &zs_state) != ZS_SUCCESS ) {
         fprintf( fp, "ZS initialization failed!\n" );
         return 0 ;
    }

    fprintf( fp, "ZS was initialized successfully!\n" );

    if(ZS_SUCCESS != ZSInitPerThreadState( zs_state, &_zs_thd_state ) ) {
         fprintf( fp, "ZS thread initialization failed!\n" );
         return 0;
    }
    fprintf( fp, "ZS thread was initialized successfully!\n" );

    p.durability_level = 0;
    p.fifo_mode = 0;
    p.size_kb = 1024*1024;
    p.num_shards = 1;
    p.persistent = ZS_TRUE;
    p.writethru = ZS_TRUE;
    p.evicting = 0;
    p.async_writes = ZS_TRUE;    
    return 1;
}

void CleanEnvironment()
{
    ZSReleasePerThreadState(&_zs_thd_state);
    ZSShutdown(zs_state);
}

void SetPropMode(ZS_boolean_t evicting,ZS_boolean_t persistent, ZS_boolean_t fifo,ZS_boolean_t writethru,ZS_boolean_t async_writes,ZS_durability_level_t durability)
{
    p.evicting = evicting;
    p.persistent = persistent;
    p.fifo_mode = fifo;
    p.writethru = writethru;
    p.async_writes = async_writes;
    p.durability_level = durability;
}

void OpenContainer(char *cname,uint32_t flags,ZS_cguid_t *cguid)
{
    ZS_status_t           ret;
    ret = ZSOpenContainer(_zs_thd_state,cname,&p,flags, cguid);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSOpenContainer cguid=%ld,cname=%s,mode=%d success\n",*cguid,cname,flags);
    }
    else fprintf(fp, "ZSOpenContainer cguid=%ld,cname=%s,mode=%d fail:%s\n",*cguid,cname,flags,ZSStrError(ret));
}


ZS_status_t CloseContainer(ZS_cguid_t cguid)
{
    ZS_status_t           ret;
    ret = ZSCloseContainer(_zs_thd_state, cguid );
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSCloseContainer cguid=%ld success.\n",cguid);
    }
    else fprintf(fp,"ZSCloseContainer cguid=%ld failed:%s.\n",cguid,ZSStrError(ret));
    return ret;
}

ZS_status_t DeleteContainer(ZS_cguid_t cguid)
{ 
    ZS_status_t           ret;
    ret = ZSDeleteContainer (_zs_thd_state, cguid);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSDeleteContainer cguid=%ld success.\n",cguid);
    }
    else fprintf(fp,"ZSDeleteContainer cguid=%ld failed:%s.\n",cguid,ZSStrError(ret));
    return ret;
}

ZS_status_t CreateObject(ZS_cguid_t cguid,char *key,uint32_t keylen,char *data,uint64_t dataln)
{
    ZS_status_t           ret;
    ret = ZSWriteObject(_zs_thd_state,cguid,key,keylen,data,dataln,1);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSWriteObject cguid=%ld,key=%s,data=%s success.\n",cguid,key,data);
    }
    else fprintf(fp,"ZSWriteObject cguid=%ld,key=%s,data=%s failed:%s.\n",cguid,key,data,ZSStrError(ret));
    //sleep(5);
    return ret;
}

ZS_status_t DeleteObject(ZS_cguid_t cguid,char *key,uint32_t keylen)
{
    ZS_status_t           ret;
    ret = ZSDeleteObject(_zs_thd_state,cguid,key,keylen);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSDeleteObject cguid=%ld,key=%s success.\n",cguid,key);
    }
    else fprintf(fp,"ZSDeleteObject cguid=%ld,key=%s fail:%s.\n",cguid,key,ZSStrError(ret));
    return ret;
}

ZS_status_t EnumerateContainerObjects(ZS_cguid_t cguid,struct ZS_iterator **iterator)
{
    ZS_status_t           ret;
    ret = ZSEnumerateContainerObjects(_zs_thd_state,cguid,iterator);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSEnumerateContainerObjects cguid=%ld return success.\n",cguid);
    }
    else fprintf(fp,"ZSEnumerateContainerObjects cguid=%ld return fail:%s.\n",cguid,ZSStrError(ret));
    return ret;
}


ZS_status_t NextEnumeratedObject(struct ZS_iterator *iterator)
{
    char                   *key;
    uint32_t               keylen;
    char                   *data;
    uint64_t               datalen;
    ZS_status_t           ret;

    ret = ZSNextEnumeratedObject(_zs_thd_state,iterator,&key,&keylen,&data,&datalen);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSNextEnumeratedObject return success.\n");
        fprintf(fp,"Object:key=%s,keylen=%d,data=%s,datalen=%ld.\n",key,keylen,data,datalen);
    }
    else fprintf(fp,"ZSNextEnumeratedObject return fail:%s.\n",ZSStrError(ret));
    return ret;
}

ZS_status_t FinishEnumeration(struct ZS_iterator *iterator)
{
    ZS_status_t           ret;

    ret = ZSFinishEnumeration(_zs_thd_state,iterator);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSFinishEnumeration return success.\n");
    }
    else fprintf(fp,"ZSFinishEnumeration return fail:%s.\n",ZSStrError(ret));
    return ret;
}


int ZSFinishEnumeration_basic_check1()
{

    ZS_status_t           ret = ZS_SUCCESS;
    int                    flag;
    struct ZS_iterator    *iterator;
    fprintf(fp,"test %d:\n",++testCount);

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(ZS_SUCCESS != ret)
        flag =  -1;
    else{
        while(ZS_SUCCESS == NextEnumeratedObject(iterator));
        ret = FinishEnumeration(iterator);
        if(ZS_SUCCESS == ret)
            flag = 1;
        else flag = 0;
    }

    return flag;    
}

int ZSFinishEnumeration_basic_check2()
{
    ZS_status_t           ret = ZS_SUCCESS;
    int                    flag;
    struct ZS_iterator    *iterator;
    fprintf(fp,"test %d:\n",++testCount);

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(ZS_SUCCESS != ret)
        flag =  -1;
    else{
        ret = FinishEnumeration(iterator);
        if(ZS_SUCCESS == ret)
            flag = 1;
        else flag = 0;
    }
    return flag;
}


/**********  main function *******/

int main(int argc, char *argv[])
{
    int result[2][3][13] = {{{10,10}}};
    ZS_boolean_t eviction[] = {0,0,0};
    ZS_boolean_t persistent[] = {1,1,1};
    ZS_boolean_t fifo[] = {0,0,0};
    ZS_boolean_t writethru[] = {1,1,1};
    int resultCount = 12;
    int num =0;
    ZS_boolean_t async_writes[] = {1,0};
    ZS_durability_level_t durability[] = {0,1,2};

    if((fp = fopen("ZS_FinishEnumeration.log", "w+")) == 0){
        fprintf(stderr, " open failed!.\n");
        return -1;
    }
    if( 1 != preEnvironment())
        return 0;
    
    fprintf(fp, "************Begin to test ***************\n");
    
    for(int j = 0;j < 2;j++){
    for(int i =0 ; i < 3;i++){
        testCount = 0;
        SetPropMode(eviction[i],persistent[i],fifo[i],writethru[i],async_writes[j],durability[i]);
        OpenContainer("x",ZS_CTNR_CREATE,&cguid);
        CreateObject(cguid,"key",4,"data",5);
        
        result[j][i][0] = ZSFinishEnumeration_basic_check1();
        result[j][i][1] = ZSFinishEnumeration_basic_check2();
        
        DeleteObject(cguid,"key",4);
        CloseContainer(cguid );
        DeleteContainer(cguid);
    }
    }
    CleanEnvironment();
     
    for(int k = 0; k < 2 ;k++){
    for(int j = 0 ; j< 3;j++){
    fprintf(stderr, "test mode:eviction=%d,persistent=%d,fifo=%d.durability=%d,async=%d\n",eviction[j],persistent[j],fifo[j],durability[j],async_writes[k]);
        for(int i = 0; i < 2; i++){
            if(result[k][j][i] == 1){
                num++;
                fprintf(stderr, "ZSFinishEnumeration %drd success.\n",i+1);
            }
            else if(result[k][j][i] == -1)
                fprintf(stderr, "ZSFinishEnumeration test %drd fail to test.\n",i+1);
            else if(result[k][j][i] == 0)
                fprintf(stderr, "ZSFinishEnumeration test %drd failed.\n",i+1);
            else fprintf(stderr, "ZSFinishEnumeration test %drd hit wrong.\n",i+1);
        }
    }
    }
    if(resultCount == num){
        fprintf(stderr, "************ test pass!******************\n");   
	fprintf(stderr, "#The related test script is ZS_FinishEnumeration.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_FinishEnumeration.log\n");
        return 0;
    }
    else 
        fprintf(stderr, "************%d test fail!******************\n",resultCount-num);
	fprintf(stderr, "#The related test script is ZS_FinishEnumeration.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_FinishEnumeration.log\n");
        return 1;
}



