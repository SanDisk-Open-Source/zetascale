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

**********   Function: ZSCloseContainer
***********************************************/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "zs.h"
static struct ZS_state     *zs_state;
struct ZS_thread_state     *_zs_thd_state;
ZS_container_props_t       p;
FILE                        *fp;
int                         testCount = 0;

int preEnvironment()
{
    /*ZS_config_t            fdf.config;

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
    fdf.config.max_flushes_per_mod_check    = 32;*/

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
    p.size_kb = 10;
    p.num_shards = 1;
    p.persistent = ZS_TRUE;
    p.writethru = ZS_TRUE;
    p.evicting = ZS_TRUE;
    p.async_writes = ZS_TRUE;    
    return 1;
}

void CleanEnvironment()
{
    ZSReleasePerThreadState(&_zs_thd_state);
    ZSShutdown(zs_state);
}

void SetProps(uint64_t size,ZS_boolean_t evicting,ZS_boolean_t persistence,ZS_boolean_t fifo,
           ZS_boolean_t writethru,ZS_durability_level_t durability,ZS_boolean_t async_writes)
{
    p.durability_level = durability;
    p.size_kb = size;
    p.fifo_mode = fifo;
    p.persistent = persistence;
    p.evicting = evicting;
    p.writethru = writethru;
    p.async_writes = async_writes;
}

ZS_status_t OpenContainer(char *cname,ZS_container_props_t *props,uint32_t flags,ZS_cguid_t *cguid)
{
    ZS_status_t           ret;
    ret = ZSOpenContainer(_zs_thd_state,cname,props,flags, cguid);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSOpenContainer cguid=%lu,cname=%s,mode=%d success\n",*cguid,cname,flags);
    }
    else fprintf(fp, "ZSOpenContainer cguid=%lu,cname=%s,mode=%d fail:.%s\n",*cguid,cname,flags,ZSStrError(ret));
    return ret;
}


ZS_status_t CloseContainer(ZS_cguid_t cguid)
{
    ZS_status_t           ret;
    ret = ZSCloseContainer(_zs_thd_state, cguid );
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSCloseContainer cguid=%lu success.\n",cguid);
    }
    else fprintf(fp,"ZSCloseContainer cguid=%lu failed:%s.\n",cguid,ZSStrError(ret));
    return ret;
}

ZS_status_t DeleteContainer(ZS_cguid_t cguid)
{ 
    ZS_status_t           ret;
    ret = ZSDeleteContainer (_zs_thd_state, cguid);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSDeleteContainer cguid=%lu success.\n",cguid);
    }
    else fprintf(fp,"ZSDeleteContainer cguid=%lu failed:%s.\n",cguid,ZSStrError(ret));
    return ret;
}


int ZSCloseContainer_basic_check1()
{

    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    
    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("x",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;

    ret = CloseContainer(cguid );

    if(ZS_SUCCESS != DeleteContainer(cguid))
        return -2;
    
    if(ZS_SUCCESS == ret)
        return 1;
    return 0;
}


int ZSCloseContainer_basic_check2()
{

    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    fprintf(fp,"test %d:\n",++testCount);
    
    ret = OpenContainer("x",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;

    OpenContainer("x",&p,2,&cguid);
    OpenContainer("x",&p,4,&cguid);

    ret = CloseContainer(cguid );

    if(ZS_SUCCESS != DeleteContainer(cguid))
        return -2;
    if(ZS_SUCCESS == ret)
        return 1;
    return 0;
}


int ZSCloseContainer_basic_check3(int count)
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
    fprintf(fp,"test %d:\n",++testCount);

    for(int i = 0;i < count;i++){
        ret = OpenContainer("test",&p,ZS_CTNR_CREATE,&cguid);
        if(ZS_SUCCESS != ret)
            return -1;
    
        ret = CloseContainer(cguid );
        if(ZS_SUCCESS != DeleteContainer(cguid))
            return -2;
        if(ZS_SUCCESS != ret)
            return 0;
    }    
     
    if(ZS_SUCCESS == ret)
        return 1;
    return 0;
}

int ZSCloseContainer_openCloseMore(uint32_t flags,int count)
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    ret = OpenContainer("test6",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;

    for(int i = 0;i < count; i++){
        ret = OpenContainer("test6",&p,flags,&cguid);
        if(ZS_SUCCESS != ret)
            return -1;
        ret = CloseContainer(cguid);
        if(ZS_SUCCESS != ret)
            break;
    }

    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSCloseContainer OpenCloseMore success.\n");
        flag = 1;
    }
    else{
        fprintf(fp,"ZSCloseContainer OpenCloseMore fail:%s.\n",ZSStrError(ret));
        flag = 0;
    }

    if(ZS_SUCCESS != DeleteContainer(cguid))
        return -2;
    return flag;
}

int ZSCloseContainer_closeTwice()
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    ret = OpenContainer("test7",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;

    for(int i = 0;i < 2; i++){
        ret = CloseContainer(cguid);
        if(ZS_SUCCESS != ret)
            break;
    }

    if(ZS_SUCCESS != ret){
        fprintf(fp,"ZSCloseContainer CloseTwice fail:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"ZSCloseContainer CloseTwice success.\n");
        flag = 0;
    }

    if(ZS_SUCCESS != DeleteContainer(cguid))
        return -2;
    return flag;
}

int ZSCloseContainer_invalid_cguid1()
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    ZS_cguid_t            cguid_invalid = 0;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    ret = OpenContainer("test8",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;
    
    ret = CloseContainer(cguid_invalid);
    if(ZS_SUCCESS != ret){
        fprintf(fp,"ZSCloseContainer invalid cguid fail:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"ZSCloseContainer invalid cguid success.\n");
        flag = 0;
    }
    CloseContainer(cguid);
    if(ZS_SUCCESS != DeleteContainer(cguid))
        return -2;
    
    return flag;
}

int ZSCloseContainer_invalid_cguid2()
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    ret = OpenContainer("test9",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;

    ret = ZSCloseContainer(_zs_thd_state,-1);
    if(ZS_SUCCESS != ret){
        fprintf(fp,"ZSCloseContainer invalid cguid fail:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"ZSCloseContainer invalid cguid success.\n");
        flag = 0;
    }
    CloseContainer(cguid);
    if(ZS_SUCCESS != DeleteContainer(cguid))
        return -2;
    return flag;
}

int ZSCloseContainer_delete1()
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    ret = OpenContainer("test9",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;
    DeleteContainer(cguid);

    ret = ZSCloseContainer(_zs_thd_state,cguid);
    if(ZS_SUCCESS != ret){
        fprintf(fp,"ZSCloseContainer after noclose_deleted success\n");
        flag = 1;
    }
    else{
        fprintf(fp,"ZSCloseContainer after noclose_deleted fail:%s\n",ZSStrError(ret));
        flag = 0;
    }
    if(ZS_SUCCESS == DeleteContainer(cguid))
        return -2;
    return flag;
}

int ZSCloseContainer_delete2()
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    ret = OpenContainer("test9",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;
    ret = CloseContainer(cguid);
    if(ZS_SUCCESS != ret)
        return -1;
    if(ZS_SUCCESS != DeleteContainer(cguid))
        return -2;

    ret = ZSCloseContainer(_zs_thd_state,cguid);
    if(ZS_SUCCESS != ret){
        fprintf(fp,"ZSCloseContainer after deleted fail:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"ZSCloseContainer after deleted success.\n");
        flag = 0;
    }

    return flag;
}

int ZSCloseContainer_delete3(int count)
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    for(int i = 0 ;i < count;i++){
        ret = OpenContainer("test9",&p,ZS_CTNR_CREATE,&cguid);
        if(ZS_SUCCESS != ret)
            return -1;
        ret = CloseContainer(cguid);
        if(ZS_SUCCESS != ret)
            return -1;
        if(ZS_SUCCESS != DeleteContainer(cguid))
            return -2;
    }
    ret = OpenContainer("test9",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;
    ret = ZSCloseContainer(_zs_thd_state,cguid);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSCloseContainer after open deleted success.\n");
        flag = 1;
    }
    else{
        fprintf(fp,"ZSCloseContainer after open deleted fail:%s.\n",ZSStrError(ret));
        flag = 0;
    }
    if(ZS_SUCCESS != DeleteContainer(cguid))
        return -2;
    return flag;
}

/**********  main function *******/

int main(int argc, char *argv[])
{
    int result[2][3][13] = {{{0,0}}};
    ZS_boolean_t eviction[] = {0,0,0};
    ZS_boolean_t persistent[] = {1,1,1};
    ZS_boolean_t fifo[] = {0,0,0};
    ZS_boolean_t writethru[] = {1,1,1};
    ZS_boolean_t async_writes[] = {0,1};
    ZS_durability_level_t durability[] = {0,1,2};
    int     totalCount = 66;
    int     num = 0;

    if((fp = fopen("ZS_CloseContainer.log", "w+")) == 0){
        fprintf(stderr, "open failed!.\n");
        return -1;
    }

    if( 1 != preEnvironment())
        return 0;

    fprintf(fp, "************Begin to test ***************\n");
    
    for(int i = 0; i < 2; i++){
        for(int j = 0 ; j < 3;j++){
            testCount = 0;
            SetProps(1024*1024,eviction[j],persistent[j],fifo[j],writethru[j],durability[j],async_writes[i]);
            result[i][j][0] = ZSCloseContainer_basic_check1();
            result[i][j][1] = ZSCloseContainer_basic_check2();
            result[i][j][2] = ZSCloseContainer_basic_check3(5);
            result[i][j][3] = ZSCloseContainer_openCloseMore(ZS_CTNR_RW_MODE,2);
            result[i][j][4] = ZSCloseContainer_openCloseMore(ZS_CTNR_RO_MODE,2);
            result[i][j][5] = ZSCloseContainer_closeTwice(2);
            result[i][j][6] = ZSCloseContainer_invalid_cguid1();
            result[i][j][7] = ZSCloseContainer_invalid_cguid2();
            result[i][j][8] = ZSCloseContainer_delete1();
            result[i][j][9] = ZSCloseContainer_delete2();
            result[i][j][10] = ZSCloseContainer_delete3(9);
        }
    }
    CleanEnvironment();
    
    for(int j = 0;j<2;j++){
        for(int k = 0;k < 3;k++){
            fprintf(stderr, "test mode:async_writes = %d.\n",async_writes[j]);
            fprintf(stderr, "test mode:eviction=%d,persistent=%d,fifo=%d,durability=%d.\n",eviction[k],persistent[k],fifo[k],k+1);
            for(int i = 0; i < 11; i++){
                if(result[j][k][i] == 1){
                    fprintf(stderr, "ZSCloseContainer test %drd success.\n",i+1);
                    num++;
                }
                else if(result[j][k][i] == -1)
                    fprintf(stderr, "ZSCloseContainer test %drd fail to test.\n",i+1);
                else if(result[j][k][i] == 0)
                    fprintf(stderr, "ZSCloseContainer test %drd failed.\n",i+1);
                else fprintf(stderr, "ZSCloseContainer test %drd hit wrong.\n",i+1);
            }
        }
    }
    if(totalCount == num){
       fprintf(stderr, "************ test pass!******************\n");
       fprintf(stderr, "#The related test script is ZS_CloseContainer.c\n");
       fprintf(stderr, "#If you want,you can check test details in ZS_CloseContainer.log!\n");
       return 0;
    }
    else {
       fprintf(stderr, "************%d test fail!******************\n",totalCount-num);
       fprintf(stderr, "#The related test script is ZS_CloseContainer.c\n");
       fprintf(stderr, "#If you want,you can check test details in ZS_CloseContainer.log!\n");
       return 1;
   }
}



