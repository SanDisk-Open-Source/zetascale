/*********************************************
**********   Author:  Lisa

**********   Function: FDFSetContainerProps
***********************************************/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "fdf.h"

static struct FDF_state     *fdf_state;
struct FDF_thread_state     *_fdf_thd_state;
FDF_container_props_t       p;
FDF_container_props_t       props_set;
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
    p.fifo_mode = 0;
    p.size_kb = 1024*1024;
    p.num_shards = 1;
    p.persistent = 1;
    p.writethru = FDF_TRUE;
    p.evicting = 0;
    p.async_writes = FDF_TRUE; 
    return 1;
}

void CleanEnvironment()
{
    FDFReleasePerThreadState(&_fdf_thd_state);
    FDFShutdown(fdf_state);
}

void SetPropMode(FDF_boolean_t evicting,FDF_boolean_t persistent, FDF_boolean_t fifo,
            FDF_boolean_t writethru,FDF_boolean_t async_writes,FDF_durability_level_t durability)
{
    p.evicting = evicting;
    p.persistent = persistent;
    p.fifo_mode = fifo;
    p.writethru = writethru;
    p.async_writes = async_writes;
    p.durability_level = durability;
}

FDF_status_t GetContainerProps(FDF_cguid_t cguid,FDF_container_props_t *props)
{
    FDF_status_t           ret;
    ret = FDFGetContainerProps(_fdf_thd_state,cguid,props);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFGetContainerProps cguid=%ld success .\n",cguid);
        fprintf(fp,"evict=%d,persistent=%d,fifo=%d,async=%d,size=%ld.\n",props->evicting,props->persistent,props->fifo_mode,props->async_writes,props->size_kb);
    }
    else fprintf(fp,"FDFGetContainerProps cguid=%ld fail:.%s\n",cguid,FDFStrError(ret));
    return ret;
}


FDF_status_t SetContainerProps(FDF_cguid_t cguid,uint64_t size,FDF_boolean_t evict,
   FDF_boolean_t persistent,FDF_boolean_t fifo,FDF_durability_level_t ability,FDF_boolean_t async)
{
    FDF_status_t           ret;
    
    ret = GetContainerProps(cguid,&props_set);
    if(FDF_SUCCESS != ret)
        return ret;
    props_set.durability_level = ability;
    props_set.size_kb = size;
    props_set.persistent = persistent;
    props_set.fifo_mode = fifo;
    props_set.evicting = evict;
    props_set.async_writes = async;

    fprintf(fp,"evict=%d,persistent=%d,fifo=%d,ability=%d,async=%d,size=%ld.\n",evict,persistent,fifo,ability,async,size);

    ret = FDFSetContainerProps(_fdf_thd_state,cguid,&props_set);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFSetContainerProps success .\n");
    }
    else fprintf(fp,"FDFSetContainerProps fail:.%s\n",FDFStrError(ret));
    return ret;
}


FDF_status_t SetContainerProps_async_durability(FDF_cguid_t cguid,uint64_t size,
     FDF_durability_level_t ability,FDF_boolean_t async)
{
    FDF_status_t           ret;

    ret = GetContainerProps(cguid,&props_set);
    if(FDF_SUCCESS != ret)
        return ret;
    props_set.durability_level = ability;
    props_set.size_kb = size;
    props_set.async_writes = async;

    fprintf(fp,"ability=%d,async=%d,size=%ld.\n",ability,async,size);

    ret = FDFSetContainerProps(_fdf_thd_state,cguid,&props_set);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFSetContainerProps success .\n");
    }
    else fprintf(fp,"FDFSetContainerProps fail:.%s\n",FDFStrError(ret));
    return ret;
}

int CheckProps(FDF_cguid_t cguid ,int persistent)
{
    FDF_container_props_t  props;
    FDF_status_t           ret;

    ret = GetContainerProps(cguid,&props);
    if(FDF_SUCCESS != ret)
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



FDF_status_t OpenContainer(char *cname,uint32_t flags,FDF_cguid_t *cguid)
{
    FDF_status_t           ret;
    ret = FDFOpenContainer(_fdf_thd_state,cname,&p,flags, cguid);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFOpenContainer cguid=%ld,cname=%s,mode=%d success.\n",*cguid,cname,flags);
    }
    else fprintf(fp, "FDFOpenContainer cguid=%ld,cname=%s,mode=%d fail:%s\n",*cguid,cname,flags,FDFStrError(ret));
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


int FDFSetContainerProps_basic_check1()
{

    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    int                    flag;
    
    fprintf(fp,"test %d:\n",++testCount);
    OpenContainer("x",FDF_CTNR_CREATE,&cguid);
    
    if(p.async_writes== 0 ){
        ret =SetContainerProps(cguid,1048576,0,1,0,1,1);
    }
    else 
        ret = SetContainerProps(cguid,1024*1024,0,1,0,2,0);

    if(FDF_SUCCESS != ret)
        flag =  -2;
    else flag = CheckProps(cguid,p.persistent);

    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;    
}



int FDFSetContainerProps_basic_check_size()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    int                    flag;

    fprintf(fp,"test %d:\n",++testCount);
    OpenContainer("test",FDF_CTNR_CREATE,&cguid);
    if(p.async_writes == 0 )
        ret =SetContainerProps(cguid,1024*10,0,1,0,0,1);
    else 
        ret = SetContainerProps(cguid,1024*10,0,1,0,1,0);
    
    if(FDF_SUCCESS != ret){
        fprintf(fp,"FDFSetContainerProps set size<1G failedi:%s.\n",FDFStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"FDFSetContainerProps set size<1G Success.\n");
        flag = CheckProps(cguid,p.persistent);
        flag = 1-flag;
    }
    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;     
}

int FDFSetContainerProps_basic_check_failed_mode()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    int                    flag;
    
    fprintf(fp,"test %d:\n",++testCount);
    OpenContainer("test",FDF_CTNR_CREATE,&cguid);
    
    p.persistent = 1- p.persistent;
    ret =FDFSetContainerProps(_fdf_thd_state,cguid,&p);

    p.persistent = 1- p.persistent; 
    
    if(FDF_SUCCESS != ret){
        fprintf(fp,"FDFSetContainerProps set persisent mode fail:%s.\n",FDFStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"FDFSetContainerProps set persistent mode Success.\n");
        flag = CheckProps(cguid,p.persistent);
        flag = 1-flag;
    }
    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int FDFSetContainerProps_SetMore1(int count)
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    int                    flag;
    FDF_durability_level_t durability[3]={0,1,2};
    FDF_boolean_t          async[2] = {0,1};
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",FDF_CTNR_CREATE,&cguid);
    
    for(int i =0; i < count;i++){
        ret = SetContainerProps_async_durability(cguid,p.size_kb,durability[i%3],async[i%2]);

        if(FDF_SUCCESS != ret){
            flag = -2;
            if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
            if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
            return flag;
        }
    }

    flag = CheckProps(cguid,p.persistent);

    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int FDFSetContainerProps_SetMore2()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    int                    flag = 0;
    FDF_boolean_t          async[2] = {1,0};
    FDF_durability_level_t durability[] = {0,1,2};
    uint32_t               size[] = {1024*1025,1024*1026,1048577};
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",FDF_CTNR_CREATE,&cguid);

    for(int i =0; i < 2;i++){
    for(int j = 0 ;j < 3;j++){

        ret= SetContainerProps_async_durability(cguid,size[j],durability[j],async[i]);
        if(FDF_SUCCESS != ret && FDF_CANNOT_REDUCE_CONTAINER_SIZE != ret){
            fprintf(fp,"FDFSetContainerProps:size=%d,durability=%d,async=%d failed:%s\n",size[j],durability[j],async[i],FDFStrError(ret));
            flag = -2; 
            if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
            if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
            return flag;
        }

 		if(FDF_CANNOT_REDUCE_CONTAINER_SIZE != ret)
        		flag = CheckProps(cguid,p.persistent);
        if(flag != 1){
            fprintf(fp,"SetContainerProps:size=%d,durability=%d,async=%d wrong.\n",size[j],durability[j],async[i]);
            
            if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
            if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
            return flag;
        }
    }
    }
    

    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int FDFSetContainerProps_ClosedSet()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    int                    flag;
    
    fprintf(fp,"test %d:\n",++testCount);
    OpenContainer("test",FDF_CTNR_CREATE,&cguid);
    CloseContainer(cguid );
    
    ret = SetContainerProps(cguid,1024*1024,0,1,0,1,0);
    
    if(FDF_SUCCESS != ret){
        flag = -2;
        if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
        return flag;
    }

    flag = CheckProps(cguid,p.persistent);

    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int FDFSetContainerProps_TwoContainerSet()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid1,cguid2;
    int                    flag;

    fprintf(fp,"test %d:\n",++testCount);
    OpenContainer("test1",FDF_CTNR_CREATE,&cguid1);
    OpenContainer("test2",FDF_CTNR_CREATE,&cguid2);

    if(p.async_writes == 0 )
        ret =SetContainerProps_async_durability(cguid1,1024*1024,2,1);
    else
        ret = SetContainerProps_async_durability(cguid1,1025*1024,0,0);
    
    if(FDF_SUCCESS != ret){
        flag = -2;
        if(FDF_SUCCESS != CloseContainer(cguid1 ))flag = -3;
        if(FDF_SUCCESS != DeleteContainer(cguid1))flag = -3;
        return flag;
    }

    flag = CheckProps(cguid1,p.persistent);
    if(1 != flag ){
        if(FDF_SUCCESS != CloseContainer(cguid1 ))flag = -3;
        if(FDF_SUCCESS != DeleteContainer(cguid1))flag = -3;
        return flag;
    }

    if(p.persistent == 0 )
        ret =SetContainerProps_async_durability(cguid2,1024*1024,0,1);
    else
        ret = SetContainerProps_async_durability(cguid2,1025*1024,1,0);
    if(FDF_SUCCESS != ret){
        flag = -2;
        if(FDF_SUCCESS != CloseContainer(cguid2 ))flag = -3;
        if(FDF_SUCCESS != DeleteContainer(cguid2))flag = -3;
        return flag;
    }

    flag = CheckProps(cguid2,p.persistent);

    if(FDF_SUCCESS != CloseContainer(cguid1 ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid1))flag = -3;
    if(FDF_SUCCESS != CloseContainer(cguid2 ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid2))flag = -3;
    return flag;
}


int FDFSetContainerProps_DeletedSet()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    int                    flag;

    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",FDF_CTNR_CREATE,&cguid);
    CloseContainer(cguid);
    DeleteContainer(cguid);
    
    ret = FDFSetContainerProps(_fdf_thd_state,cguid,&p);
    
    if(FDF_SUCCESS != ret){
        fprintf(fp,"FDFSetContainerProps Set deleted one fail:%s.\n",FDFStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"FDFSetContainerProps Set deleted one success\n");
        flag = 0;
    }
    
    return flag;
}

int FDFSetContainerProps_invalid_cguid()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",FDF_CTNR_CREATE,&cguid);
    GetContainerProps(cguid,&props_set);
    
    ret = FDFSetContainerProps(_fdf_thd_state,-1,&props_set);
    if(FDF_SUCCESS != ret){
        fprintf(fp,"FDFSetContainerProps use invalid cguid fail:%s.\n",FDFStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"FDFSetContainerProps use invalid cguid success\n");
        flag = 0;
    }

    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int FDFSetContainerProps_set_invalid_cguid()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",FDF_CTNR_CREATE,&cguid);
    GetContainerProps(cguid,&props_set);
    props_set.cguid = -1;

    ret = FDFSetContainerProps(_fdf_thd_state,cguid,&props_set);
    if(FDF_SUCCESS != ret){
        fprintf(fp,"SetContainerProps set invalid cguid fail:%s.\n",FDFStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"FDFSetContainerProps set invalid cguid success\n");
        flag =  CheckProps(cguid,p.persistent);
        flag = 1-flag;
    }

    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int FDFSetContainerProps_invalid_props()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
    int                    flag;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",FDF_CTNR_CREATE,&cguid);
    GetContainerProps(cguid,&props_set);

    p.writethru = FDF_FALSE;
    
    ret =FDFSetContainerProps(_fdf_thd_state,cguid,&p);
    
    if(FDF_SUCCESS != ret){
        fprintf(fp,"FDFSetContainerProps set invalid props fail:%s\n",FDFStrError(ret));
        flag = 1;
    }

    else{
        fprintf(fp,"FDFSetContainerProps set invalid props success.\n");
        flag = 0;
    }

    p.writethru = FDF_TRUE;
    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}


/**********  main function *******/

int main(int argc, char *argv[])
{
    int result[2][3][15] = {{{0,0}}};
    int resultCount = 48;
    int num = 0;

    if((fp = fopen("FDF_SetContainerProps.log", "w+")) == 0){
        fprintf(stderr, " open failed!.\n");
        return -1;
    }
    if( 1 != preEnvironment())
        return 0;
    
    FDF_boolean_t eviction[] = {0,0,0};
    FDF_boolean_t persistent[] = {1,1,1};
    FDF_boolean_t fifo[] = {0,0,0};
    FDF_boolean_t writethru[] = {1,1,1};
    FDF_boolean_t async_writes[] = {0,1};
    FDF_durability_level_t durability[] = {0,1,2};

    fprintf(fp, "************Begin to test ***************\n");
    
    for(int i = 0; i < 2; i=i+1){
    for(int j = 0; j < 3; j++){
        testCount = 0;
        SetPropMode(eviction[j],persistent[j],fifo[j],writethru[j],async_writes[i],durability[j]);
        result[i][j][0] = FDFSetContainerProps_basic_check1();
        result[i][j][1] = FDFSetContainerProps_basic_check_failed_mode();
        result[i][j][2] = FDFSetContainerProps_SetMore1(2);
        result[i][j][3] = FDFSetContainerProps_SetMore2();
        result[i][j][4] = FDFSetContainerProps_ClosedSet();
        result[i][j][5] = FDFSetContainerProps_TwoContainerSet();
        result[i][j][6] = FDFSetContainerProps_DeletedSet();
        result[i][j][7] = FDFSetContainerProps_invalid_cguid();
        //result[i][j][8] = FDFSetContainerProps_basic_check_size();
        //result[i][j][9] = FDFSetContainerProps_set_invalid_cguid();
        //result[i][j][10] = FDFSetContainerProps_invalid_props();
    }
    }
    CleanEnvironment();
    for(int k = 0; k < 2; k++){
    for(int j = 0; j < 3;j=j+1){
        fprintf(stderr, "test mode:eviction=%d,persistent=%d,fifo=%d.async=%d,durability=%d.\n",eviction[j],persistent[j],fifo[j],async_writes[k],durability[j]);
        for(int i = 0; i < 8; i++){
            if(result[k][j][i] == 1){
                fprintf(stderr, "FDFSetContainerProps test %drd success.\n",i+1);
                num++;
            }
            else if(result[k][j][i] == -1)
                fprintf(stderr, "FDFSetContainerProps test %drd GetProps failed\n",i+1);
            else if(result[k][j][i] == -2)
                fprintf(stderr, "FDFSetContainerProps test %drd set return fail.\n",i+1);
            else if(result[k][j][i]== 0)
                fprintf(stderr, "FDFSetContainerProps test %drd failed.\n",i+1);
            else fprintf(stderr, "FDFSetContainerProps test %drd hit wrong.\n",i+1);
        }
    }
    }
    if(resultCount == num){
        fprintf(stderr, "************ test pass!******************\n");
	fprintf(stderr, "#The related test script is FDF_SetContainerProps.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_SetContainerProps.log\n");
        return 0;
    }
    else 
        fprintf(stderr, "************%d test fail!******************\n",resultCount-num);
	fprintf(stderr, "#The related test script is FDF_SetContainerProps.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_SetContainerProps.log\n");
        return 1;
}



