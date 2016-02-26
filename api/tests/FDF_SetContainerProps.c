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

/*********************************************
**********   Author:  Lisa

**********   Function: ZSSetContainerProps
***********************************************/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "zs.h"

static struct ZS_state     *zs_state;
struct ZS_thread_state     *_zs_thd_state;
ZS_container_props_t       p;
ZS_container_props_t       props_set;
FILE                        *fp;
int                         testCount = 0;

int preEnvironment()
{
/*    ZS_config_t            fdf.config;

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
    p.persistent = 1;
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

void SetPropMode(ZS_boolean_t evicting,ZS_boolean_t persistent, ZS_boolean_t fifo,
            ZS_boolean_t writethru,ZS_boolean_t async_writes,ZS_durability_level_t durability)
{
    p.evicting = evicting;
    p.persistent = persistent;
    p.fifo_mode = fifo;
    p.writethru = writethru;
    p.async_writes = async_writes;
    p.durability_level = durability;
}

ZS_status_t GetContainerProps(ZS_cguid_t cguid,ZS_container_props_t *props)
{
    ZS_status_t           ret;
    ret = ZSGetContainerProps(_zs_thd_state,cguid,props);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSGetContainerProps cguid=%ld success .\n",cguid);
        fprintf(fp,"evict=%d,persistent=%d,fifo=%d,async=%d,size=%ld.\n",props->evicting,props->persistent,props->fifo_mode,props->async_writes,props->size_kb);
    }
    else fprintf(fp,"ZSGetContainerProps cguid=%ld fail:.%s\n",cguid,ZSStrError(ret));
    return ret;
}


ZS_status_t SetContainerProps(ZS_cguid_t cguid,uint64_t size,ZS_boolean_t evict,
   ZS_boolean_t persistent,ZS_boolean_t fifo,ZS_durability_level_t ability,ZS_boolean_t async)
{
    ZS_status_t           ret;
    
    ret = GetContainerProps(cguid,&props_set);
    if(ZS_SUCCESS != ret)
        return ret;
    props_set.durability_level = ability;
    props_set.size_kb = size;
    props_set.persistent = persistent;
    props_set.fifo_mode = fifo;
    props_set.evicting = evict;
    props_set.async_writes = async;

    fprintf(fp,"evict=%d,persistent=%d,fifo=%d,ability=%d,async=%d,size=%ld.\n",evict,persistent,fifo,ability,async,size);

    ret = ZSSetContainerProps(_zs_thd_state,cguid,&props_set);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSSetContainerProps success .\n");
    }
    else fprintf(fp,"ZSSetContainerProps fail:.%s\n",ZSStrError(ret));
    return ret;
}


ZS_status_t SetContainerProps_async_durability(ZS_cguid_t cguid,uint64_t size,
     ZS_durability_level_t ability,ZS_boolean_t async)
{
    ZS_status_t           ret;

    ret = GetContainerProps(cguid,&props_set);
    if(ZS_SUCCESS != ret)
        return ret;
    props_set.durability_level = ability;
    props_set.size_kb = size;
    props_set.async_writes = async;

    fprintf(fp,"ability=%d,async=%d,size=%ld.\n",ability,async,size);

    ret = ZSSetContainerProps(_zs_thd_state,cguid,&props_set);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSSetContainerProps success .\n");
    }
    else fprintf(fp,"ZSSetContainerProps fail:.%s\n",ZSStrError(ret));
    return ret;
}

int CheckProps(ZS_cguid_t cguid ,int persistent)
{
    ZS_container_props_t  props;
    ZS_status_t           ret;

    ret = GetContainerProps(cguid,&props);
    if(ZS_SUCCESS != ret)
        return -1;

    if( (props_set.size_kb == props.size_kb)
       &&(persistent == props.persistent)
       &&(props_set.writethru == props.writethru)
       &&(props_set.durability_level == props.durability_level)
       &&(props_set.cguid == props.cguid)
       &&(props_set.evicting == props.evicting)){
        fprintf(fp,"check ContainerProps is right .\n");    
        return 1;
    }
    else
        fprintf(fp,"check ContainerProps is wrong .\n");
        return 0;
}



ZS_status_t OpenContainer(char *cname,uint32_t flags,ZS_cguid_t *cguid)
{
    ZS_status_t           ret;
    ret = ZSOpenContainer(_zs_thd_state,cname,&p,flags, cguid);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSOpenContainer cguid=%ld,cname=%s,mode=%d success.\n",*cguid,cname,flags);
    }
    else fprintf(fp, "ZSOpenContainer cguid=%ld,cname=%s,mode=%d fail:%s\n",*cguid,cname,flags,ZSStrError(ret));
    return ret;
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


int ZSSetContainerProps_basic_check1()
{

    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    int                    flag;
    
    fprintf(fp,"test %d:\n",++testCount);
    OpenContainer("x",ZS_CTNR_CREATE,&cguid);
    
    if(p.async_writes== 0 ){
        ret =SetContainerProps(cguid,1048576,0,1,0,1,1);
    }
    else 
        ret = SetContainerProps(cguid,1024*1024,0,1,0,2,0);

    if(ZS_SUCCESS != ret)
        flag =  -2;
    else flag = CheckProps(cguid,p.persistent);

    if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;    
}



int ZSSetContainerProps_basic_check_size()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    int                    flag;

    fprintf(fp,"test %d:\n",++testCount);
    OpenContainer("test",ZS_CTNR_CREATE,&cguid);
    if(p.async_writes == 0 )
        ret =SetContainerProps(cguid,1024*10,0,1,0,0,1);
    else 
        ret = SetContainerProps(cguid,1024*10,0,1,0,1,0);
    
    if(ZS_SUCCESS != ret){
        fprintf(fp,"ZSSetContainerProps set size<1G failedi:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"ZSSetContainerProps set size<1G Success.\n");
        flag = CheckProps(cguid,p.persistent);
        flag = 1-flag;
    }
    if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;     
}

int ZSSetContainerProps_basic_check_failed_mode()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    int                    flag;
    
    fprintf(fp,"test %d:\n",++testCount);
    OpenContainer("test",ZS_CTNR_CREATE,&cguid);
    
    p.persistent = 1- p.persistent;
    ret =ZSSetContainerProps(_zs_thd_state,cguid,&p);

    p.persistent = 1- p.persistent; 
    
    if(ZS_SUCCESS != ret){
        fprintf(fp,"ZSSetContainerProps set persisent mode fail:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"ZSSetContainerProps set persistent mode Success.\n");
        flag = CheckProps(cguid,p.persistent);
        flag = 1-flag;
    }
    if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int ZSSetContainerProps_SetMore1(int count)
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    int                    flag;
    ZS_durability_level_t durability[3]={0,1,2};
    ZS_boolean_t          async[2] = {0,1};
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",ZS_CTNR_CREATE,&cguid);
    
    for(int i =0; i < count;i++){
        ret = SetContainerProps_async_durability(cguid,p.size_kb,durability[i%3],async[i%2]);

        if(ZS_SUCCESS != ret){
            flag = -2;
            if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
            if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
            return flag;
        }
    }

    flag = CheckProps(cguid,p.persistent);

    if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int ZSSetContainerProps_SetMore2()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    int                    flag = 0;
    ZS_boolean_t          async[2] = {1,0};
    ZS_durability_level_t durability[] = {0,1,2};
    uint32_t               size[] = {1024*1025,1024*1026,1048577};
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",ZS_CTNR_CREATE,&cguid);

    for(int i =0; i < 2;i++){
    for(int j = 0 ;j < 3;j++){

        ret= SetContainerProps_async_durability(cguid,size[j],durability[j],async[i]);
        if(ZS_SUCCESS != ret && ZS_CANNOT_REDUCE_CONTAINER_SIZE != ret){
            fprintf(fp,"ZSSetContainerProps:size=%d,durability=%d,async=%d failed:%s\n",size[j],durability[j],async[i],ZSStrError(ret));
            flag = -2; 
            if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
            if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
            return flag;
        }

 		if(ZS_CANNOT_REDUCE_CONTAINER_SIZE != ret)
        		flag = CheckProps(cguid,p.persistent);
        if(flag != 1){
            fprintf(fp,"SetContainerProps:size=%d,durability=%d,async=%d wrong.\n",size[j],durability[j],async[i]);
            
            if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
            if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
            return flag;
        }
    }
    }
    

    if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int ZSSetContainerProps_ClosedSet()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    int                    flag;
    
    fprintf(fp,"test %d:\n",++testCount);
    OpenContainer("test",ZS_CTNR_CREATE,&cguid);
    CloseContainer(cguid );
    
    ret = SetContainerProps(cguid,1024*1024,0,1,0,1,0);
    
    if(ZS_SUCCESS != ret){
        flag = -2;
        if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
        return flag;
    }

    flag = CheckProps(cguid,p.persistent);

    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int ZSSetContainerProps_TwoContainerSet()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid1,cguid2;
    int                    flag;

    fprintf(fp,"test %d:\n",++testCount);
    OpenContainer("test1",ZS_CTNR_CREATE,&cguid1);
    OpenContainer("test2",ZS_CTNR_CREATE,&cguid2);

    if(p.async_writes == 0 )
        ret =SetContainerProps_async_durability(cguid1,1024*1024,2,1);
    else
        ret = SetContainerProps_async_durability(cguid1,1025*1024,0,0);
    
    if(ZS_SUCCESS != ret){
        flag = -2;
        if(ZS_SUCCESS != CloseContainer(cguid1 ))flag = -3;
        if(ZS_SUCCESS != DeleteContainer(cguid1))flag = -3;
        return flag;
    }

    flag = CheckProps(cguid1,p.persistent);
    if(1 != flag ){
        if(ZS_SUCCESS != CloseContainer(cguid1 ))flag = -3;
        if(ZS_SUCCESS != DeleteContainer(cguid1))flag = -3;
        return flag;
    }

    if(p.persistent == 0 )
        ret =SetContainerProps_async_durability(cguid2,1024*1024,0,1);
    else
        ret = SetContainerProps_async_durability(cguid2,1025*1024,1,0);
    if(ZS_SUCCESS != ret){
        flag = -2;
        if(ZS_SUCCESS != CloseContainer(cguid2 ))flag = -3;
        if(ZS_SUCCESS != DeleteContainer(cguid2))flag = -3;
        return flag;
    }

    flag = CheckProps(cguid2,p.persistent);

    if(ZS_SUCCESS != CloseContainer(cguid1 ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid1))flag = -3;
    if(ZS_SUCCESS != CloseContainer(cguid2 ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid2))flag = -3;
    return flag;
}


int ZSSetContainerProps_DeletedSet()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    int                    flag;

    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",ZS_CTNR_CREATE,&cguid);
    CloseContainer(cguid);
    DeleteContainer(cguid);
    
    ret = ZSSetContainerProps(_zs_thd_state,cguid,&p);
    
    if(ZS_SUCCESS != ret){
        fprintf(fp,"ZSSetContainerProps Set deleted one fail:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"ZSSetContainerProps Set deleted one success\n");
        flag = 0;
    }
    
    return flag;
}

int ZSSetContainerProps_invalid_cguid()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",ZS_CTNR_CREATE,&cguid);
    GetContainerProps(cguid,&props_set);
    
    ret = ZSSetContainerProps(_zs_thd_state,-1,&props_set);
    if(ZS_SUCCESS != ret){
        fprintf(fp,"ZSSetContainerProps use invalid cguid fail:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"ZSSetContainerProps use invalid cguid success\n");
        flag = 0;
    }

    if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int ZSSetContainerProps_set_invalid_cguid()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",ZS_CTNR_CREATE,&cguid);
    GetContainerProps(cguid,&props_set);
    props_set.cguid = -1;

    ret = ZSSetContainerProps(_zs_thd_state,cguid,&props_set);
    if(ZS_SUCCESS != ret){
        fprintf(fp,"SetContainerProps set invalid cguid fail:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"ZSSetContainerProps set invalid cguid success\n");
        flag =  CheckProps(cguid,p.persistent);
        flag = 1-flag;
    }

    if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int ZSSetContainerProps_invalid_props()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",ZS_CTNR_CREATE,&cguid);
    GetContainerProps(cguid,&props_set);

    p.writethru = ZS_FALSE;
    
    ret =ZSSetContainerProps(_zs_thd_state,cguid,&p);
    
    if(ZS_SUCCESS != ret){
        fprintf(fp,"ZSSetContainerProps set invalid props fail:%s\n",ZSStrError(ret));
        flag = 1;
    }

    else{
        fprintf(fp,"ZSSetContainerProps set invalid props success.\n");
        flag = 0;
    }

    p.writethru = ZS_TRUE;
    if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}


/**********  main function *******/

int main(int argc, char *argv[])
{
    int result[2][3][15] = {{{0,0}}};
    int resultCount = 48;
    int num = 0;

    if((fp = fopen("ZS_SetContainerProps.log", "w+")) == 0){
        fprintf(stderr, " open failed!.\n");
        return -1;
    }
    if( 1 != preEnvironment())
        return 0;
    
    ZS_boolean_t eviction[] = {0,0,0};
    ZS_boolean_t persistent[] = {1,1,1};
    ZS_boolean_t fifo[] = {0,0,0};
    ZS_boolean_t writethru[] = {1,1,1};
    ZS_boolean_t async_writes[] = {0,1};
    ZS_durability_level_t durability[] = {0,1,2};

    fprintf(fp, "************Begin to test ***************\n");
    
    for(int i = 0; i < 2; i=i+1){
    for(int j = 0; j < 3; j++){
        testCount = 0;
        SetPropMode(eviction[j],persistent[j],fifo[j],writethru[j],async_writes[i],durability[j]);
        result[i][j][0] = ZSSetContainerProps_basic_check1();
        result[i][j][1] = ZSSetContainerProps_basic_check_failed_mode();
        result[i][j][2] = ZSSetContainerProps_SetMore1(2);
        result[i][j][3] = ZSSetContainerProps_SetMore2();
        result[i][j][4] = ZSSetContainerProps_ClosedSet();
        result[i][j][5] = ZSSetContainerProps_TwoContainerSet();
        result[i][j][6] = ZSSetContainerProps_DeletedSet();
        result[i][j][7] = ZSSetContainerProps_invalid_cguid();
        //result[i][j][8] = ZSSetContainerProps_basic_check_size();
        //result[i][j][9] = ZSSetContainerProps_set_invalid_cguid();
        //result[i][j][10] = ZSSetContainerProps_invalid_props();
    }
    }
    CleanEnvironment();
    for(int k = 0; k < 2; k++){
    for(int j = 0; j < 3;j=j+1){
        fprintf(stderr, "test mode:eviction=%d,persistent=%d,fifo=%d.async=%d,durability=%d.\n",eviction[j],persistent[j],fifo[j],async_writes[k],durability[j]);
        for(int i = 0; i < 8; i++){
            if(result[k][j][i] == 1){
                fprintf(stderr, "ZSSetContainerProps test %drd success.\n",i+1);
                num++;
            }
            else if(result[k][j][i] == -1)
                fprintf(stderr, "ZSSetContainerProps test %drd GetProps failed\n",i+1);
            else if(result[k][j][i] == -2)
                fprintf(stderr, "ZSSetContainerProps test %drd set return fail.\n",i+1);
            else if(result[k][j][i]== 0)
                fprintf(stderr, "ZSSetContainerProps test %drd failed.\n",i+1);
            else fprintf(stderr, "ZSSetContainerProps test %drd hit wrong.\n",i+1);
        }
    }
    }
    if(resultCount == num){
        fprintf(stderr, "************ test pass!******************\n");
	fprintf(stderr, "#The related test script is ZS_SetContainerProps.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_SetContainerProps.log\n");
        return 0;
    }
    else 
        fprintf(stderr, "************%d test fail!******************\n",resultCount-num);
	fprintf(stderr, "#The related test script is ZS_SetContainerProps.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_SetContainerProps.log\n");
        return 1;
}



