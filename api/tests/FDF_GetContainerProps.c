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

**********   Function: ZSGetContainerProps
***********************************************/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "zs.h"
static struct ZS_state     *zs_state;
struct ZS_thread_state     *_zs_thd_state;
ZS_container_props_t       p;
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
    p.fifo_mode = ZS_FALSE;
    p.size_kb = 1024*1024;
    p.num_shards = 1;
    p.persistent = ZS_TRUE;
    p.writethru = ZS_TRUE;
    p.evicting = ZS_FALSE;
    p.cid = 1;
    p.async_writes = ZS_TRUE;    
    return 1;
}

void CleanEnvironment()
{
    ZSReleasePerThreadState(&_zs_thd_state);
    ZSShutdown(zs_state);
}

void SetPropsMode(uint64_t size,ZS_boolean_t evicting,ZS_boolean_t persistence,ZS_boolean_t fifo,ZS_boolean_t writethru,ZS_boolean_t async_writes,ZS_durability_level_t durability)
{
    p.durability_level = durability;
    p.size_kb = size;
    p.persistent = persistence;
    p.evicting = evicting;
    p.fifo_mode = fifo;
    p.writethru = writethru;
    p.async_writes = async_writes;
}

int CheckProps(ZS_cguid_t cguid,ZS_container_props_t props)
{
    if( (p.size_kb == props.size_kb)
       &&(p.fifo_mode == props.fifo_mode)
       &&(p.persistent == props.persistent)
       &&(p.writethru == props.writethru)
       &&(p.durability_level == props.durability_level)
       &&(p.evicting == props.evicting)
       &&(p.num_shards == props.num_shards)
       //&&(p.cid == props.cid)
       &&(cguid == props.cguid)){
        fprintf(fp,"Check get container's Props is right\n");
        return 1;
    }
    else
        fprintf(fp,"Check get container's Props is wrong\n");
        return 0;
}

ZS_status_t GetContainerProps(ZS_cguid_t cguid,ZS_container_props_t *props)
{
    ZS_status_t           ret;
    ret = ZSGetContainerProps(_zs_thd_state,cguid,props);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSGetContainerProps cguid=%ld success .\n",cguid);
    }
    else fprintf(fp,"ZSGetContainerProps cguid=%ld fail:%s\n",cguid,ZSStrError(ret));
    return ret;
}


ZS_status_t OpenContainer(char *cname,ZS_container_props_t *props,uint32_t flags,ZS_cguid_t *cguid)
{
    ZS_status_t           ret;
    ret = ZSOpenContainer(_zs_thd_state,cname,props,flags, cguid);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSOpenContainer cguid=%ld,cname=%s,flags=%d success\n",*cguid,cname,flags);
    }
    else fprintf(fp, "ZSOpenContainer cguid=%ld,cname=%s,flags=%d fail:%s\n",*cguid,cname,flags,ZSStrError(ret));
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


int ZSGetContainerProps_basic_check()
{

    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    ZS_container_props_t  props;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("x",&p,ZS_CTNR_CREATE,&cguid);

    ret = GetContainerProps(cguid,&props);
    if(ZS_SUCCESS != ret)
        flag = -1;
    else flag = CheckProps(cguid,props);

    if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;    
}



int ZSGetContainerProps_GetMore(int count)
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    ZS_container_props_t  props;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",&p,ZS_CTNR_CREATE,&cguid);

    for(int i =0; i < count;i++){
        ret = GetContainerProps(cguid,&props);
        if(ZS_SUCCESS != ret){
            flag = -1;
            if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
            if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
            return flag;
        }
    }

    flag = CheckProps(cguid,props);

    if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int ZSGetContainerProps_ClosedGet()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    ZS_container_props_t  props;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",&p,ZS_CTNR_CREATE,&cguid);
    CloseContainer(cguid );


    ret = GetContainerProps(cguid,&props);
    if(ZS_SUCCESS != ret)
        flag = -1;
    else  flag = CheckProps(cguid,props);

    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int ZSGetContainerProps_TwoContainerGet1()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid1,cguid2;
    ZS_container_props_t  props1,props2;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test1",&p,ZS_CTNR_CREATE,&cguid1);

    ret = GetContainerProps(cguid1,&props1);
    if(ZS_SUCCESS != ret){
        flag = -2;
        if(ZS_SUCCESS != CloseContainer(cguid1 ))flag = -3;
        if(ZS_SUCCESS != DeleteContainer(cguid1))flag = -3;
        return flag;
    }
     
    flag = CheckProps(cguid1,props1);
    if(1 != flag ){
        flag = -1;
        if(ZS_SUCCESS != CloseContainer(cguid1 ))flag = -3;
        if(ZS_SUCCESS != DeleteContainer(cguid1))flag = -3;
        return flag;
    }
    if(p.async_writes == 1)
        SetPropsMode(1024*1024,0,1,0,1,2,0);
    else  SetPropsMode(1024*10240,0,1,0,1,1,1);
    
    OpenContainer("test2",&p,ZS_CTNR_CREATE,&cguid2);

    ret = GetContainerProps(cguid2,&props2);
    if(ZS_SUCCESS != ret)
        flag = -1;
    /* check name of the congainer */
    else if ( strcmp(props2.name,"test2") ) {
        fprintf(fp,"Name in prop(%s) does not match container name (%s)\n",props2.name,"test2");
        flag = -1;
    }
    else  flag = CheckProps(cguid2,props2);
    
    if(ZS_SUCCESS != CloseContainer(cguid1 ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid1))flag = -3;
    if(ZS_SUCCESS != CloseContainer(cguid2 ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid2))flag = -3;
    return flag;
}

int ZSGetContainerProps_TwoContainerGet2()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    ZS_container_props_t  props1,props2;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test1",&p,ZS_CTNR_CREATE,&cguid);

    ret = GetContainerProps(cguid,&props1);
    if(ZS_SUCCESS != ret){
        flag = -1;
    }
    else  flag = CheckProps(cguid,props1);

    if(1 != flag ){
        flag = -2;
        if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
        if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
        return flag;
    }

    if(p.async_writes == 1)
        SetPropsMode(1024*1024,0,1,0,1,2,0);
    else  SetPropsMode(1024*1024,0,1,0,1,1,1);
    OpenContainer("test1",&p,ZS_CTNR_RW_MODE,&cguid);

    ret = GetContainerProps(cguid,&props2);
    if(ZS_SUCCESS != ret)
        flag = -1;
    else flag = 1- CheckProps(cguid,props2);

    if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int ZSGetContainerProps_DeletedGet()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    ZS_container_props_t  props;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",&p,ZS_CTNR_CREATE,&cguid);
    CloseContainer(cguid);
    DeleteContainer(cguid);

    ret = ZSGetContainerProps(_zs_thd_state,cguid,&props);
    if(ZS_SUCCESS != ret){
        fprintf(fp,"GetContainerProps get deleted one fail:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"ZSGetContainerProps get deleted one success\n");
        flag = 0;
    }
    
    return flag;
}

int ZSGetContainerProps_invalid_cguid()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    ZS_container_props_t  props;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",&p,ZS_CTNR_CREATE,&cguid);

    ret = ZSGetContainerProps(_zs_thd_state,-1,&props);
    if(ZS_SUCCESS != ret){
        fprintf(fp,"GetContainerProps use invalid cguid fail:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"ZSGetContainerProps use invalid cguid success\n");
        flag = 0;
    }

    if(ZS_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}


/**********  main function *******/

int main(int argc, char *argv[])
{
    int result[3][2][7] = {{{0,0}}};
    ZS_boolean_t eviction[] = {0,0,0};
    ZS_boolean_t persistent[] = {1,1,1};
    ZS_boolean_t fifo[] = {0,0,0};
    ZS_boolean_t writethru[] = {1,1,1};
    ZS_boolean_t async_writes[] = {1,0};
    ZS_durability_level_t durability[] = {0,1,2};
    int resultCount = 42;
    int num = 0;

    if((fp = fopen("ZS_GetContainerProps.log", "w+")) == 0){
        fprintf(stderr, " open failed!.\n");
        return -1;
    }
    if( 1 != preEnvironment())
        return 0;

    fprintf(fp, "************Begin to test ***************\n");
    for(int i = 0; i < 3; i++){
        for(int j = 0 ; j < 2;j++){
            testCount = 0;
            SetPropsMode(1024*1024,eviction[i],persistent[i],fifo[i],writethru[i],async_writes[j],durability[i]);
            result[i][j][0] = ZSGetContainerProps_basic_check();
            result[i][j][1] = ZSGetContainerProps_GetMore(2);
            result[i][j][2] = ZSGetContainerProps_ClosedGet();
            result[i][j][3] = ZSGetContainerProps_TwoContainerGet1();
            result[i][j][4] = ZSGetContainerProps_TwoContainerGet2();
            result[i][j][5] = ZSGetContainerProps_DeletedGet();
            result[i][j][6] = ZSGetContainerProps_invalid_cguid();
        }
    }

    CleanEnvironment();
    
    for(int j = 0;j<3;j++){
        for(int k = 0;k < 2;k++){
            fprintf(stderr, "test mode:eviction=%d,persistent=%d,fifo=%d,async_writes=%d,durability=%d.\n",eviction[j],persistent[j],fifo[j],async_writes[k],k+1);
            for(int i = 0; i < 7; i++){
                if(result[j][k][i] == 1){
                    fprintf(stderr, "ZSGetContainerProps test %drd success.\n",i+1);
                    num++;
                }
                else if(result[j][k][i] == -1)
                    fprintf(stderr, "ZSGetContainerProps test %drd return Fail.\n",i+1);
                else if(result[j][k][i] == 0)
                    fprintf(stderr, "ZSGetContainerProps test %drd failed.\n",i+1);
                else if(result[j][k][i] == -2)
                    fprintf(stderr, "ZSGetContainerProps test %drd fail to test.\n",i+1);
                else fprintf(stderr, "ZSGetContainerProps test %drd hit wrong.\n",i+1);
            }
        }
    }
    if(resultCount == num){
        fprintf(stderr, "************ test pass!******************\n");
        fprintf(stderr, "#The related test script is ZS_GetContainerProps.c\n");
        fprintf(stderr, "#If you want, you can check test details in ZS_GetContainerProps.log\n");
	return 0;
    }
    else 
        fprintf(stderr, "************%d test fail!******************\n",resultCount-num);
	fprintf(stderr, "#The related test script is ZS_GetContainerProps.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_GetContainerProps.log\n");
   	return 1;
}



