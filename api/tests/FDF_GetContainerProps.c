/*********************************************
**********   Author:  Lisa

**********   Function: FDFGetContainerProps
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
/*    FDF_config_t            fdf_config;

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
    fdf_config.max_flushes_per_mod_check    = 32;
*/
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
    p.size_kb = 1024*1024;
    p.num_shards = 1;
    p.persistent = FDF_TRUE;
    p.writethru = FDF_TRUE;
    p.evicting = FDF_FALSE;
    p.cid = 1;
    p.async_writes = FDF_TRUE;    
    return 1;
}

void CleanEnvironment()
{
    FDFReleasePerThreadState(&_fdf_thd_state);
    FDFShutdown(fdf_state);
}

void SetPropsMode(uint64_t size,FDF_boolean_t evicting,FDF_boolean_t persistence,FDF_boolean_t fifo,FDF_boolean_t writethru,FDF_boolean_t async_writes,FDF_durability_level_t durability)
{
    p.durability_level = durability;
    p.size_kb = size;
    p.persistent = persistence;
    p.evicting = evicting;
    p.fifo_mode = fifo;
    p.writethru = writethru;
    p.async_writes = async_writes;
}

int CheckProps(FDF_cguid_t cguid,FDF_container_props_t props)
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

FDF_status_t GetContainerProps(FDF_cguid_t cguid,FDF_container_props_t *props)
{
    FDF_status_t           ret;
    ret = FDFGetContainerProps(_fdf_thd_state,cguid,props);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFGetContainerProps cguid=%ld success .\n",cguid);
    }
    else fprintf(fp,"FDFGetContainerProps cguid=%ld fail:%s\n",cguid,FDFStrError(ret));
    return ret;
}


FDF_status_t OpenContainer(char *cname,FDF_container_props_t *props,uint32_t flags,FDF_cguid_t *cguid)
{
    FDF_status_t           ret;
    ret = FDFOpenContainer(_fdf_thd_state,cname,props,flags, cguid);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFOpenContainer cguid=%ld,cname=%s,flags=%d success\n",*cguid,cname,flags);
    }
    else fprintf(fp, "FDFOpenContainer cguid=%ld,cname=%s,flags=%d fail:%s\n",*cguid,cname,flags,FDFStrError(ret));
    return ret;
}


FDF_status_t CloseContainer(FDF_cguid_t cguid)
{
    FDF_status_t           ret;
    ret = FDFCloseContainer(_fdf_thd_state, cguid );
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFCloseContainer cguid=%ld success.\n",cguid);
    }
    else fprintf(fp,"FDFCloseContainer cguid=%ld failed:%s.\n",cguid,FDFStrError(ret));
    return ret;
}

FDF_status_t DeleteContainer(FDF_cguid_t cguid)
{ 
    FDF_status_t           ret;
    ret = FDFDeleteContainer (_fdf_thd_state, cguid);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFDeleteContainer cguid=%ld success.\n",cguid);
    }
    else fprintf(fp,"FDFDeleteContainer cguid=%ld failed:%s.\n",cguid,FDFStrError(ret));
    return ret;
}


int FDFGetContainerProps_basic_check()
{

    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    FDF_container_props_t  props;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("x",&p,FDF_CTNR_CREATE,&cguid);

    ret = GetContainerProps(cguid,&props);
    if(FDF_SUCCESS != ret)
        flag = -1;
    else flag = CheckProps(cguid,props);

    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;    
}



int FDFGetContainerProps_GetMore(int count)
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    FDF_container_props_t  props;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",&p,FDF_CTNR_CREATE,&cguid);

    for(int i =0; i < count;i++){
        ret = GetContainerProps(cguid,&props);
        if(FDF_SUCCESS != ret){
            flag = -1;
            if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
            if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
            return flag;
        }
    }

    flag = CheckProps(cguid,props);

    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int FDFGetContainerProps_ClosedGet()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    FDF_container_props_t  props;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",&p,FDF_CTNR_CREATE,&cguid);
    CloseContainer(cguid );


    ret = GetContainerProps(cguid,&props);
    if(FDF_SUCCESS != ret)
        flag = -1;
    else  flag = CheckProps(cguid,props);

    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int FDFGetContainerProps_TwoContainerGet1()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid1,cguid2;
    FDF_container_props_t  props1,props2;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test1",&p,FDF_CTNR_CREATE,&cguid1);

    ret = GetContainerProps(cguid1,&props1);
    if(FDF_SUCCESS != ret){
        flag = -2;
        if(FDF_SUCCESS != CloseContainer(cguid1 ))flag = -3;
        if(FDF_SUCCESS != DeleteContainer(cguid1))flag = -3;
        return flag;
    }
     
    flag = CheckProps(cguid1,props1);
    if(1 != flag ){
        flag = -1;
        if(FDF_SUCCESS != CloseContainer(cguid1 ))flag = -3;
        if(FDF_SUCCESS != DeleteContainer(cguid1))flag = -3;
        return flag;
    }
    if(p.async_writes == 1)
        SetPropsMode(1024*1024,0,1,0,1,2,0);
    else  SetPropsMode(1024*10240,0,1,0,1,1,1);
    
    OpenContainer("test2",&p,FDF_CTNR_CREATE,&cguid2);

    ret = GetContainerProps(cguid2,&props2);
    if(FDF_SUCCESS != ret)
        flag = -1;
    else  flag = CheckProps(cguid2,props2);

    if(FDF_SUCCESS != CloseContainer(cguid1 ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid1))flag = -3;
    if(FDF_SUCCESS != CloseContainer(cguid2 ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid2))flag = -3;
    return flag;
}

int FDFGetContainerProps_TwoContainerGet2()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    FDF_container_props_t  props1,props2;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test1",&p,FDF_CTNR_CREATE,&cguid);

    ret = GetContainerProps(cguid,&props1);
    if(FDF_SUCCESS != ret){
        flag = -1;
    }
    else  flag = CheckProps(cguid,props1);

    if(1 != flag ){
        flag = -2;
        if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
        if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
        return flag;
    }

    if(p.async_writes == 1)
        SetPropsMode(1024*1024,0,1,0,1,2,0);
    else  SetPropsMode(1024*1024,0,1,0,1,1,1);
    OpenContainer("test1",&p,FDF_CTNR_RW_MODE,&cguid);

    ret = GetContainerProps(cguid,&props2);
    if(FDF_SUCCESS != ret)
        flag = -1;
    else flag = 1- CheckProps(cguid,props2);

    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int FDFGetContainerProps_DeletedGet()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    FDF_container_props_t  props;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",&p,FDF_CTNR_CREATE,&cguid);
    CloseContainer(cguid);
    DeleteContainer(cguid);

    ret = FDFGetContainerProps(_fdf_thd_state,cguid,&props);
    if(FDF_SUCCESS != ret){
        fprintf(fp,"GetContainerProps get deleted one fail:%s.\n",FDFStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"FDFGetContainerProps get deleted one success\n");
        flag = 0;
    }
    
    return flag;
}

int FDFGetContainerProps_invalid_cguid()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    FDF_container_props_t  props;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",&p,FDF_CTNR_CREATE,&cguid);

    ret = FDFGetContainerProps(_fdf_thd_state,-1,&props);
    if(FDF_SUCCESS != ret){
        fprintf(fp,"GetContainerProps use invalid cguid fail:%s.\n",FDFStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"FDFGetContainerProps use invalid cguid success\n");
        flag = 0;
    }

    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}


/**********  main function *******/

int main(int argc, char *argv[])
{
    int result[3][2][7] = {{{0,0}}};
    FDF_boolean_t eviction[] = {0,0,0};
    FDF_boolean_t persistent[] = {1,1,1};
    FDF_boolean_t fifo[] = {0,0,0};
    FDF_boolean_t writethru[] = {1,1,1};
    FDF_boolean_t async_writes[] = {1,0};
    FDF_durability_level_t durability[] = {0,1,2};
    int resultCount = 42;
    int num = 0;

    if((fp = fopen("FDF_GetContainerProps.log", "w+")) == 0){
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
            result[i][j][0] = FDFGetContainerProps_basic_check();
            result[i][j][1] = FDFGetContainerProps_GetMore(2);
            result[i][j][2] = FDFGetContainerProps_ClosedGet();
            result[i][j][3] = FDFGetContainerProps_TwoContainerGet1();
            result[i][j][4] = FDFGetContainerProps_TwoContainerGet2();
            result[i][j][5] = FDFGetContainerProps_DeletedGet();
            result[i][j][6] = FDFGetContainerProps_invalid_cguid();
        }
    }

    CleanEnvironment();
    
    for(int j = 0;j<3;j++){
        for(int k = 0;k < 2;k++){
            fprintf(stderr, "test mode:eviction=%d,persistent=%d,fifo=%d,async_writes=%d,durability=%d.\n",eviction[j],persistent[j],fifo[j],async_writes[k],k+1);
            for(int i = 0; i < 7; i++){
                if(result[j][k][i] == 1){
                    fprintf(stderr, "FDFGetContainerProps test %drd success.\n",i+1);
                    num++;
                }
                else if(result[j][k][i] == -1)
                    fprintf(stderr, "FDFGetContainerProps test %drd return Fail.\n",i+1);
                else if(result[j][k][i] == 0)
                    fprintf(stderr, "FDFGetContainerProps test %drd failed.\n",i+1);
                else if(result[j][k][i] == -2)
                    fprintf(stderr, "FDFGetContainerProps test %drd fail to test.\n",i+1);
                else fprintf(stderr, "FDFGetContainerProps test %drd hit wrong.\n",i+1);
            }
        }
    }
    if(resultCount == num){
        fprintf(stderr, "************ test pass!******************\n");
        fprintf(stderr, "#The related test script is FDF_GetContainerProps.c\n");
        fprintf(stderr, "#If you want, you can check test details in FDF_GetContainerProps.log\n");
	return 0;
    }
    else 
        fprintf(stderr, "************%d test fail!******************\n",resultCount-num);
	fprintf(stderr, "#The related test script is FDF_GetContainerProps.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_GetContainerProps.log\n");
   	return 1;
}



