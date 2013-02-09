/*********************************************
**********   Author:  Lisa

**********   Function: FDFCloseContainer
***********************************************/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "fdf.h"
static struct FDF_state     *fdf_state;
struct FDF_thread_state     *_fdf_thd_state;
FDF_container_props_t       p;
FILE                        *fp;
int                         testCount = 0;

int preEnvironment()
{
    /*FDF_config_t            fdf_config;

    fdf_config.version                      = 1;
    fdf_config.n_flash_devices              = 1;
    fdf_config.flash_base_name              = "/schooner/data/schooner%d";
    fdf_config.flash_size_per_device_gb     = 12;
    fdf_config.dram_cache_size_gb           = 8;
    fdf_config.n_cache_partitions           = 100;
    fdf_config.reformat                     = 1;
    fdf_config.max_object_size              = 1048576;
    fdf_config.max_background_flushes       = 8;
    fdf_config.background_flush_msec        = 1000;
    fdf_config.max_outstanding_writes       = 32;
    fdf_config.cache_modified_fraction      = 1.0;
    fdf_config.max_flushes_per_mod_check    = 32;*/

    //FDFLoadConfigDefaults(&fdf_state);
    //if(FDFInit( &fdf_state, &fdf_config ) != FDF_SUCCESS ) {
    if(FDFInit( &fdf_state) != FDF_SUCCESS ) {
         fprintf( fp, "FDF initialization failed!\n" );
         return 0 ;
    }

    fprintf( fp, "FDF was initialized successfully!\n" );

    if(FDF_SUCCESS != FDFInitPerThreadState( fdf_state, &_fdf_thd_state ) ) {
         fprintf( fp, "FDF thread initialization failed!\n" );
         return 0;
    }
    fprintf( fp, "FDF thread was initialized successfully!\n" );

    p.durability_level = 0;
    p.fifo_mode = FDF_FALSE;
    p.size_kb = 10;
    p.num_shards = 1;
    p.persistent = FDF_TRUE;
    p.writethru = FDF_TRUE;
    p.evicting = FDF_TRUE;
    p.async_writes = FDF_TRUE;    
    return 1;
}

void CleanEnvironment()
{
    FDFReleasePerThreadState(&_fdf_thd_state);
    FDFShutdown(fdf_state);
}

void SetProps(uint64_t size,FDF_boolean_t evicting,FDF_boolean_t persistence,FDF_boolean_t fifo,
           FDF_boolean_t writethru,FDF_durability_level_t durability,FDF_boolean_t async_writes)
{
    p.durability_level = durability;
    p.size_kb = size;
    p.fifo_mode = fifo;
    p.persistent = persistence;
    p.evicting = evicting;
    p.writethru = writethru;
    p.async_writes = async_writes;
}

FDF_status_t OpenContainer(char *cname,FDF_container_props_t *props,uint32_t flags,FDF_cguid_t *cguid)
{
    FDF_status_t           ret;
    ret = FDFOpenContainer(_fdf_thd_state,cname,props,flags, cguid);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFOpenContainer cguid=%lu,cname=%s,mode=%d success\n",*cguid,cname,flags);
    }
    else fprintf(fp, "FDFOpenContainer cguid=%lu,cname=%s,mode=%d fail:.%s\n",*cguid,cname,flags,FDFStrError(ret));
    return ret;
}


FDF_status_t CloseContainer(FDF_cguid_t cguid)
{
    FDF_status_t           ret;
    ret = FDFCloseContainer(_fdf_thd_state, cguid );
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFCloseContainer cguid=%lu success.\n",cguid);
    }
    else fprintf(fp,"FDFCloseContainer cguid=%lu failed:%s.\n",cguid,FDFStrError(ret));
    return ret;
}

FDF_status_t DeleteContainer(FDF_cguid_t cguid)
{ 
    FDF_status_t           ret;
    ret = FDFDeleteContainer (_fdf_thd_state, cguid);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFDeleteContainer cguid=%lu success.\n",cguid);
    }
    else fprintf(fp,"FDFDeleteContainer cguid=%lu failed:%s.\n",cguid,FDFStrError(ret));
    return ret;
}


int FDFCloseContainer_basic_check1()
{

    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    
    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("x",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;

    ret = CloseContainer(cguid );

    if(FDF_SUCCESS != DeleteContainer(cguid))
        return -2;
    
    if(FDF_SUCCESS == ret)
        return 1;
    return 0;
}


int FDFCloseContainer_basic_check2()
{

    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    fprintf(fp,"test %d:\n",++testCount);
    
    ret = OpenContainer("x",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;

    OpenContainer("x",&p,2,&cguid);
    OpenContainer("x",&p,4,&cguid);

    ret = CloseContainer(cguid );

    if(FDF_SUCCESS != DeleteContainer(cguid))
        return -2;
    if(FDF_SUCCESS == ret)
        return 1;
    return 0;
}


int FDFCloseContainer_basic_check3(int count)
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    fprintf(fp,"test %d:\n",++testCount);

    for(int i = 0;i < count;i++){
        ret = OpenContainer("test",&p,FDF_CTNR_CREATE,&cguid);
        if(FDF_SUCCESS != ret)
            return -1;
    
        ret = CloseContainer(cguid );
        if(FDF_SUCCESS != DeleteContainer(cguid))
            return -2;
        if(FDF_SUCCESS != ret)
            return 0;
    }    
     
    if(FDF_SUCCESS == ret)
        return 1;
    return 0;
}

int FDFCloseContainer_openCloseMore(uint32_t flags,int count)
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    ret = OpenContainer("test6",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;

    for(int i = 0;i < count; i++){
        ret = OpenContainer("test6",&p,flags,&cguid);
        if(FDF_SUCCESS != ret)
            return -1;
        ret = CloseContainer(cguid);
        if(FDF_SUCCESS != ret)
            break;
    }

    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFCloseContainer OpenCloseMore success.\n");
        flag = 1;
    }
    else{
        fprintf(fp,"FDFCloseContainer OpenCloseMore fail:%s.\n",FDFStrError(ret));
        flag = 0;
    }

    if(FDF_SUCCESS != DeleteContainer(cguid))
        return -2;
    return flag;
}

int FDFCloseContainer_closeTwice()
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    ret = OpenContainer("test7",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;

    for(int i = 0;i < 2; i++){
        ret = CloseContainer(cguid);
        if(FDF_SUCCESS != ret)
            break;
    }

    if(FDF_SUCCESS != ret){
        fprintf(fp,"FDFCloseContainer CloseTwice fail:%s.\n",FDFStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"FDFCloseContainer CloseTwice success.\n");
        flag = 0;
    }

    if(FDF_SUCCESS != DeleteContainer(cguid))
        return -2;
    return flag;
}

int FDFCloseContainer_invalid_cguid1()
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    FDF_cguid_t            cguid_invalid = 0;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    ret = OpenContainer("test8",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;
    
    ret = CloseContainer(cguid_invalid);
    if(FDF_SUCCESS != ret){
        fprintf(fp,"FDFCloseContainer invalid cguid fail:%s.\n",FDFStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"FDFCloseContainer invalid cguid success.\n");
        flag = 0;
    }
    CloseContainer(cguid);
    if(FDF_SUCCESS != DeleteContainer(cguid))
        return -2;
    
    return flag;
}

int FDFCloseContainer_invalid_cguid2()
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    ret = OpenContainer("test9",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;

    ret = FDFCloseContainer(_fdf_thd_state,-1);
    if(FDF_SUCCESS != ret){
        fprintf(fp,"FDFCloseContainer invalid cguid fail:%s.\n",FDFStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"FDFCloseContainer invalid cguid success.\n");
        flag = 0;
    }
    CloseContainer(cguid);
    if(FDF_SUCCESS != DeleteContainer(cguid))
        return -2;
    return flag;
}

int FDFCloseContainer_delete1()
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    ret = OpenContainer("test9",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;
    DeleteContainer(cguid);

    ret = FDFCloseContainer(_fdf_thd_state,cguid);
    if(FDF_SUCCESS != ret){
        fprintf(fp,"FDFCloseContainer after noclose_deleted success\n");
        flag = 1;
    }
    else{
        fprintf(fp,"FDFCloseContainer after noclose_deleted fail:%s\n",FDFStrError(ret));
        flag = 0;
    }
    if(FDF_SUCCESS == DeleteContainer(cguid))
        return -2;
    return flag;
}

int FDFCloseContainer_delete2()
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    ret = OpenContainer("test9",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;
    ret = CloseContainer(cguid);
    if(FDF_SUCCESS != ret)
        return -1;
    if(FDF_SUCCESS != DeleteContainer(cguid))
        return -2;

    ret = FDFCloseContainer(_fdf_thd_state,cguid);
    if(FDF_SUCCESS != ret){
        fprintf(fp,"FDFCloseContainer after deleted fail:%s.\n",FDFStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"FDFCloseContainer after deleted success.\n");
        flag = 0;
    }

    return flag;
}

int FDFCloseContainer_delete3(int count)
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    for(int i = 0 ;i < count;i++){
        ret = OpenContainer("test9",&p,FDF_CTNR_CREATE,&cguid);
        if(FDF_SUCCESS != ret)
            return -1;
        ret = CloseContainer(cguid);
        if(FDF_SUCCESS != ret)
            return -1;
        if(FDF_SUCCESS != DeleteContainer(cguid))
            return -2;
    }
    ret = OpenContainer("test9",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;
    ret = FDFCloseContainer(_fdf_thd_state,cguid);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFCloseContainer after open deleted success.\n");
        flag = 1;
    }
    else{
        fprintf(fp,"FDFCloseContainer after open deleted fail:%s.\n",FDFStrError(ret));
        flag = 0;
    }
    if(FDF_SUCCESS != DeleteContainer(cguid))
        return -2;
    return flag;
}

/**********  main function *******/

int main(int argc, char *argv[])
{
    int result[2][3][13] = {{{0,0}}};
    FDF_boolean_t eviction[] = {0,0,0};
    FDF_boolean_t persistent[] = {1,1,1};
    FDF_boolean_t fifo[] = {0,0,0};
    FDF_boolean_t writethru[] = {1,1,1};
    FDF_boolean_t async_writes[] = {0,1};
    FDF_durability_level_t durability[] = {0,1,2};
    int     totalCount = 66;
    int     num = 0;

    if((fp = fopen("FDF_CloseContainer.log", "w+")) == 0){
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
            result[i][j][0] = FDFCloseContainer_basic_check1();
            result[i][j][1] = FDFCloseContainer_basic_check2();
            result[i][j][2] = FDFCloseContainer_basic_check3(5);
            result[i][j][3] = FDFCloseContainer_openCloseMore(FDF_CTNR_RW_MODE,2);
            result[i][j][4] = FDFCloseContainer_openCloseMore(FDF_CTNR_RO_MODE,2);
            result[i][j][5] = FDFCloseContainer_closeTwice(2);
            result[i][j][6] = FDFCloseContainer_invalid_cguid1();
            result[i][j][7] = FDFCloseContainer_invalid_cguid2();
            result[i][j][8] = FDFCloseContainer_delete1();
            result[i][j][9] = FDFCloseContainer_delete2();
            result[i][j][10] = FDFCloseContainer_delete3(9);
        }
    }
    CleanEnvironment();
    
    for(int j = 0;j<2;j++){
        for(int k = 0;k < 3;k++){
            fprintf(stderr, "test mode:async_writes = %d.\n",async_writes[j]);
            fprintf(stderr, "test mode:eviction=%d,persistent=%d,fifo=%d,durability=%d.\n",eviction[k],persistent[k],fifo[k],k+1);
            for(int i = 0; i < 11; i++){
                if(result[j][k][i] == 1){
                    fprintf(stderr, "FDFCloseContainer test %drd success.\n",i+1);
                    num++;
                }
                else if(result[j][k][i] == -1)
                    fprintf(stderr, "FDFCloseContainer test %drd fail to test.\n",i+1);
                else if(result[j][k][i] == 0)
                    fprintf(stderr, "FDFCloseContainer test %drd failed.\n",i+1);
                else fprintf(stderr, "FDFCloseContainer test %drd hit wrong.\n",i+1);
            }
        }
    }
    if(totalCount == num){
       fprintf(stderr, "************ test pass!******************\n");
       fprintf(stderr, "#The related test script is FDF_CloseContainer.c\n");
       fprintf(stderr, "#If you want,you can check test details in FDF_CloseContainer.log!\n");
       return 0;
    }
    else {
       fprintf(stderr, "************%d test fail!******************\n",totalCount-num);
       fprintf(stderr, "#The related test script is FDF_CloseContainer.c\n");
       fprintf(stderr, "#If you want,you can check test details in FDF_CloseContainer.log!\n");
       return 1;
   }
}



