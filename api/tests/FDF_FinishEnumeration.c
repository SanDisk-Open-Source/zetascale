/*********************************************
**********   Author:  Lisa

**********   Function: FDFFinishEnumeration
***********************************************/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "fdf.h"
static struct FDF_state     *fdf_state;
struct FDF_thread_state     *_fdf_thd_state;
FDF_container_props_t       p;
FDF_cguid_t                 cguid;
FILE                        *fp;
int                         testCount = 0;

int preEnvironment()
{
   /* FDF_config_t            fdf_config;

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
    p.persistent = FDF_TRUE;
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

void SetPropMode(FDF_boolean_t evicting,FDF_boolean_t persistent, FDF_boolean_t fifo,FDF_boolean_t writethru,FDF_boolean_t async_writes,FDF_durability_level_t durability)
{
    p.evicting = evicting;
    p.persistent = persistent;
    p.fifo_mode = fifo;
    p.writethru = writethru;
    p.async_writes = async_writes;
    p.durability_level = durability;
}

void OpenContainer(char *cname,uint32_t flags,FDF_cguid_t *cguid)
{
    FDF_status_t           ret;
    ret = FDFOpenContainer(_fdf_thd_state,cname,&p,flags, cguid);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFOpenContainer cguid=%ld,cname=%s,mode=%d success\n",*cguid,cname,flags);
    }
    else fprintf(fp, "FDFOpenContainer cguid=%ld,cname=%s,mode=%d fail:%s\n",*cguid,cname,flags,FDFStrError(ret));
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

FDF_status_t CreateObject(FDF_cguid_t cguid,char *key,uint32_t keylen,char *data,uint64_t dataln)
{
    FDF_status_t           ret;
    ret = FDFWriteObject(_fdf_thd_state,cguid,key,keylen,data,dataln,1);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFWriteObject cguid=%ld,key=%s,data=%s success.\n",cguid,key,data);
    }
    else fprintf(fp,"FDFWriteObject cguid=%ld,key=%s,data=%s failed:%s.\n",cguid,key,data,FDFStrError(ret));
    //sleep(5);
    return ret;
}

FDF_status_t DeleteObject(FDF_cguid_t cguid,char *key,uint32_t keylen)
{
    FDF_status_t           ret;
    ret = FDFDeleteObject(_fdf_thd_state,cguid,key,keylen);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFDeleteObject cguid=%ld,key=%s success.\n",cguid,key);
    }
    else fprintf(fp,"FDFDeleteObject cguid=%ld,key=%s fail:%s.\n",cguid,key,FDFStrError(ret));
    return ret;
}

FDF_status_t EnumerateContainerObjects(FDF_cguid_t cguid,struct FDF_iterator **iterator)
{
    FDF_status_t           ret;
    ret = FDFEnumerateContainerObjects(_fdf_thd_state,cguid,iterator);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFEnumerateContainerObjects cguid=%ld return success.\n",cguid);
    }
    else fprintf(fp,"FDFEnumerateContainerObjects cguid=%ld return fail:%s.\n",cguid,FDFStrError(ret));
    return ret;
}


FDF_status_t NextEnumeratedObject(struct FDF_iterator *iterator)
{
    char                   *key;
    uint32_t               keylen;
    char                   *data;
    uint64_t               datalen;
    FDF_status_t           ret;

    ret = FDFNextEnumeratedObject(_fdf_thd_state,iterator,&key,&keylen,&data,&datalen);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFNextEnumeratedObject return success.\n");
        fprintf(fp,"Object:key=%s,keylen=%d,data=%s,datalen=%ld.\n",key,keylen,data,datalen);
    }
    else fprintf(fp,"FDFNextEnumeratedObject return fail:%s.\n",FDFStrError(ret));
    return ret;
}

FDF_status_t FinishEnumeration(struct FDF_iterator *iterator)
{
    FDF_status_t           ret;

    ret = FDFFinishEnumeration(_fdf_thd_state,iterator);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFFinishEnumeration return success.\n");
    }
    else fprintf(fp,"FDFFinishEnumeration return fail:%s.\n",FDFStrError(ret));
    return ret;
}


int FDFFinishEnumeration_basic_check1()
{

    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    fprintf(fp,"test %d:\n",++testCount);

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(FDF_SUCCESS != ret)
        flag =  -1;
    else{
        while(FDF_SUCCESS == NextEnumeratedObject(iterator));
        ret = FinishEnumeration(iterator);
        if(FDF_SUCCESS == ret)
            flag = 1;
        else flag = 0;
    }

    return flag;    
}

int FDFFinishEnumeration_basic_check2()
{
    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    fprintf(fp,"test %d:\n",++testCount);

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(FDF_SUCCESS != ret)
        flag =  -1;
    else{
        ret = FinishEnumeration(iterator);
        if(FDF_SUCCESS == ret)
            flag = 1;
        else flag = 0;
    }
    return flag;
}


/**********  main function *******/

int main(int argc, char *argv[])
{
    int result[2][3][13] = {{{10,10}}};
    FDF_boolean_t eviction[] = {0,0,0};
    FDF_boolean_t persistent[] = {1,1,1};
    FDF_boolean_t fifo[] = {0,0,0};
    FDF_boolean_t writethru[] = {1,1,1};
    int resultCount = 12;
    int num =0;
    FDF_boolean_t async_writes[] = {1,0};
    FDF_durability_level_t durability[] = {0,1,2};

    if((fp = fopen("FDF_FinishEnumeration.log", "w+")) == 0){
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
        OpenContainer("x",FDF_CTNR_CREATE,&cguid);
        CreateObject(cguid,"key",4,"data",5);
        
        result[j][i][0] = FDFFinishEnumeration_basic_check1();
        result[j][i][1] = FDFFinishEnumeration_basic_check2();
        
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
                fprintf(stderr, "FDFFinishEnumeration %drd success.\n",i+1);
            }
            else if(result[k][j][i] == -1)
                fprintf(stderr, "FDFFinishEnumeration test %drd fail to test.\n",i+1);
            else if(result[k][j][i] == 0)
                fprintf(stderr, "FDFFinishEnumeration test %drd failed.\n",i+1);
            else fprintf(stderr, "FDFFinishEnumeration test %drd hit wrong.\n",i+1);
        }
    }
    }
    if(resultCount == num){
        fprintf(stderr, "************ test pass!******************\n");   
	fprintf(stderr, "#The related test script is FDF_FinishEnumeration.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_FinishEnumeration.log\n");
        return 0;
    }
    else 
        fprintf(stderr, "************%d test fail!******************\n",resultCount-num);
	fprintf(stderr, "#The related test script is FDF_FinishEnumeration.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_FinishEnumeration.log\n");
        return 1;
}



