/*********************************************
**********   Author:  Lisa

**********   Function: FDFEnumerateContainerObjects
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
  /*  FDF_config_t            fdf_config;

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

void SetPropMode(FDF_boolean_t evicting,FDF_boolean_t persistent,FDF_boolean_t fifo,FDF_boolean_t writethru,FDF_boolean_t async_writes,FDF_durability_level_t durability)
{
    p.evicting = evicting;
    p.persistent = persistent;
    p.fifo_mode = fifo;
    p.writethru = writethru;
    p.async_writes = async_writes;
    p.durability_level = durability;
}

FDF_status_t OpenContainer(char *cname,uint32_t flags,FDF_cguid_t *cguid)
{
    FDF_status_t           ret;
    ret = FDFOpenContainer(_fdf_thd_state,cname,&p,flags, cguid);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFOpenContainer cguid=%ld,cname=%s,mode=%d success\n",*cguid,cname,flags);
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
    else fprintf(fp,"DeleteContainer cguid=%ld failed:%s.\n",cguid,FDFStrError(ret));
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
    else fprintf(fp,"FDFDeleteObject cguid=%ld,key=%s failed:%s.\n",cguid,key,FDFStrError(ret));
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


int FDFEnumerateContainerObjects_basic_check1()
{

    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    fprintf(fp,"test %d:\n",++testCount);

    ret = CreateObject(cguid,"key",4,"data",5);
    if(FDF_SUCCESS != ret)
        flag =  -1;
    else{
        ret = EnumerateContainerObjects(cguid,&iterator);
        if(FDF_SUCCESS == ret){
            while(FDF_SUCCESS == NextEnumeratedObject(iterator));
            FinishEnumeration(iterator);
            flag = 1;
        }
        else flag = 0;
    }
    if(FDF_SUCCESS != DeleteObject(cguid,"key",4))flag = -3;
    return flag;    
}

int FDFEnumerateContainerObjects_basic_check2()
{
    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    fprintf(fp,"test %d:\n",++testCount);

    ret = CreateObject(cguid,"key",4,"data",5);
    if(FDF_SUCCESS != ret)
       flag =  -1;
    else{
       ret = EnumerateContainerObjects(cguid,&iterator);
       if(FDF_SUCCESS == ret){
           //while(FDF_SUCCESS == NextEnumeratedObject(iterator));
           FinishEnumeration(iterator);
           flag = 1;
       }
       else flag = 0;
    }
    if(FDF_SUCCESS != DeleteObject(cguid,"key",4)) flag = -3;
    return flag;
}

int FDFEnumerateContainerObjects_noObject1()
{

    FDF_status_t           ret;
    int                    flag;
    struct FDF_iterator    *iterator;
    fprintf(fp,"test %d:\n",++testCount);

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(FDF_SUCCESS == ret){
        while(FDF_SUCCESS == NextEnumeratedObject(iterator));
        FinishEnumeration(iterator);
        flag = 1;
    }
    else flag = 0;
       
    return flag;
}

int FDFEnumerateContainerObjects_noObject2()
{

    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    fprintf(fp,"test %d:\n",++testCount);

    ret = CreateObject(cguid,"key",4,"data",5);
    if(FDF_SUCCESS != ret){
        return -1;
    }    
    DeleteObject(cguid,"key",4);

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(FDF_SUCCESS == ret){
        while(FDF_SUCCESS == NextEnumeratedObject(iterator));
        FinishEnumeration(iterator);
        flag = 1;
    }
    else flag = 0;

    return flag;
}

int FDFEnumerateContainerObjects_Twice()
{

    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator1,*iterator2;
    fprintf(fp,"test %d:\n",++testCount);

    ret = CreateObject(cguid,"key",4,"data",5);
    if(FDF_SUCCESS != ret){
        return -1;
    }

    ret = EnumerateContainerObjects(cguid,&iterator1);
    if(FDF_SUCCESS == ret){
        while(FDF_SUCCESS == NextEnumeratedObject(iterator1));

        ret = EnumerateContainerObjects(cguid,&iterator2);
        if(FDF_SUCCESS == ret){
            while(FDF_SUCCESS == NextEnumeratedObject(iterator2));
            FinishEnumeration(iterator2);
            fprintf(fp,"EnumerateContainerObjects contiuous twice success.\n");
            flag = 1;
        }
        else{
            fprintf(fp,"EnumerateContainerObjects contiuous twice fail:%s.\n",FDFStrError(ret));
            flag = 0;
        }

        FinishEnumeration(iterator1);
    }
    else flag = -1;

    if(FDF_SUCCESS != DeleteObject(cguid,"key",4)) flag = -3;   
    return flag;
}

int FDFEnumerateContainerObjects_noObject_close()
{

    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    fprintf(fp,"test %d:\n",++testCount);

    CloseContainer(cguid );

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(FDF_SUCCESS == ret){
        while(FDF_SUCCESS == NextEnumeratedObject(iterator));
        FinishEnumeration(iterator);
        flag = 1;
    }
    else flag = 0;

    OpenContainer("x",FDF_CTNR_RW_MODE,&cguid);
    return flag;
}

int FDFEnumerateContainerObjects_invalid_cguid()
{

    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    fprintf(fp,"test %d:\n",++testCount);

    ret = CreateObject(cguid,"key",4,"data",5);
    if(FDF_SUCCESS != ret){
        return -1;
    }

    ret = FDFEnumerateContainerObjects(_fdf_thd_state,-1,&iterator);
    if(FDF_SUCCESS == ret){
        while(FDF_SUCCESS == NextEnumeratedObject(iterator));
        FinishEnumeration(iterator);
        fprintf(fp,"EnumerateContainerObjects use invalid cguid return success.\n");
        flag = 0;
    }
    else{
        fprintf(fp,"EnumerateContainerObjects use invalid cguid return fail:%s.\n",FDFStrError(ret));
        flag = 1;
    }

    if(FDF_SUCCESS != DeleteObject(cguid,"key",4)) flag = -3;
    return flag;
}

int FDFEnumerateContainerObjects_MoreObject1(int count)
{

    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    char                   key[5] = "key1";
    fprintf(fp,"test %d:\n",++testCount);

    for(int i =0; i < count; i++){
        ret = CreateObject(cguid,key,5,"data",5);
        if(FDF_SUCCESS != ret){
            flag = -1;
            for(int j = i-1;j >= 0;j--){
                key[3]--;
                if(FDF_SUCCESS != DeleteObject(cguid,key,5)) flag = -3;
            }

            return flag;
        }

        key[3]++;
    }

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(FDF_SUCCESS == ret){
        while(FDF_SUCCESS == NextEnumeratedObject(iterator));
        FinishEnumeration(iterator);
        flag = 1;
    }
    else flag = 0;

    for(int j = count-1;j >= 0;j--){
        key[3]--;
        if(FDF_SUCCESS != DeleteObject(cguid,key,5))flag = -3;
    }
    return flag;
}

int FDFEnumerateContainerObjects_MoreObject2(int count)
{

    FDF_status_t           ret1 = FDF_SUCCESS,ret2;
    FDF_cguid_t            cguid1;
    int                    flag;
    struct FDF_iterator    *iterator1,*iterator2;
    char                   key1[6] = "key_a";
    char                   key2[7] = "test_1";
    char                   data1[7] = "data_a";
    char                   data2[7] = "data_1";
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",FDF_CTNR_CREATE,&cguid1);

    for(int i =0; i < count; i++){
        ret1 = CreateObject(cguid,key1,6,data1,7);
        ret2 = CreateObject(cguid1,key2,7,data2,7);
        if(FDF_SUCCESS != ret1 || FDF_SUCCESS != ret2){
            flag = -1;
            if(FDF_SUCCESS == ret1)
                if(FDF_SUCCESS != DeleteObject(cguid,key1,6)) flag = -3;
            if(FDF_SUCCESS == ret2)
                if(FDF_SUCCESS != DeleteObject(cguid1,key2,7)) flag = -3;
            for(int j = i-1;j >= 0;j--){
                key1[4]--;
                if(FDF_SUCCESS != DeleteObject(cguid,key1,6)) flag = -3;
                key2[5]--;
                if(FDF_SUCCESS != DeleteObject(cguid1,key2,7)) flag = -3;
            }
            if(FDF_SUCCESS != CloseContainer(cguid1 ))flag = -3;
            if(FDF_SUCCESS != DeleteContainer(cguid1))flag = -3;
            return flag;
        }

        key1[4]++;
        key2[5]++;
    }

    ret1 = EnumerateContainerObjects(cguid,&iterator1);
    ret2 = EnumerateContainerObjects(cguid1,&iterator2);
    if(FDF_SUCCESS == ret1 && FDF_SUCCESS == ret2){
        while(FDF_SUCCESS == NextEnumeratedObject(iterator1));
        while(FDF_SUCCESS == NextEnumeratedObject(iterator2));
        FinishEnumeration(iterator1);
        FinishEnumeration(iterator2);
        flag = 1;
    }
    else{
        if(FDF_SUCCESS == ret1){
            while(FDF_SUCCESS == NextEnumeratedObject(iterator1));
            FinishEnumeration(iterator1);
        }
        if(FDF_SUCCESS == ret2){
            while(FDF_SUCCESS == NextEnumeratedObject(iterator2));
             FinishEnumeration(iterator2);
        }
            
        flag = 0;
    }

    for(int j = count-1;j >= 0;j--){
        key1[4]--;
        key2[5]--;
        if(FDF_SUCCESS != DeleteObject(cguid,key1,6)) flag = -3;
        if(FDF_SUCCESS != DeleteObject(cguid1,key2,7)) flag = -3;
    }
    if(FDF_SUCCESS != CloseContainer(cguid1 ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid1))flag = -3;
    return flag;
}

int FDFEnumerateContainerObjects_Open_CreateObj_close1()
{

    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    fprintf(fp,"test %d:\n",++testCount);

    ret = CreateObject(cguid,"key1",5,"data",5);
    if(FDF_SUCCESS != ret){
        return -1;
    }
    CloseContainer(cguid );
    OpenContainer("key",FDF_CTNR_RO_MODE,&cguid);

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(FDF_SUCCESS == ret){
        while(FDF_SUCCESS == NextEnumeratedObject(iterator));
        FinishEnumeration(iterator);
        flag = 1;
    }
    else flag = 0;
    
    if(FDF_FAILURE != DeleteObject(cguid,"key1",5)) flag = -3; 
    return flag;
}

int FDFEnumerateContainerObjects_Open_CreateObj_close2()
{

    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    fprintf(fp,"test %d:\n",++testCount);

    ret = CreateObject(cguid,"key1",5,"data",5);
    if(FDF_SUCCESS != ret){
        return -1;
    }
    CloseContainer(cguid );
    //OpenContainer("key",FDF_CTNR_RW_MODE,&cguid);
    //CreateObject(cguid,"key2",5,"data",5);

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(FDF_SUCCESS == ret){
    while(FDF_SUCCESS == NextEnumeratedObject(iterator));
        FinishEnumeration(iterator);
        flag = 1;
    }
    else flag = 0;
    OpenContainer("key",FDF_CTNR_RW_MODE,&cguid); 
    if(FDF_SUCCESS != DeleteObject(cguid,"key1",5)) flag = -3;
    //DeleteObject(cguid,"key2",5);
    return flag;
}

/**********  main function *******/

int main(int argc, char *argv[])
{
    int result[3][13] = {{0,0}};
    FDF_boolean_t eviction[] = {0,0,0};
    FDF_boolean_t persistent[] = {1,1,1};
    FDF_boolean_t fifo[] = {0,0,0};
    FDF_boolean_t writethru[] = {1,1,1};
    int resultCount = 27;
    int num = 0;
    FDF_boolean_t async_writes[] = {0,1,0};
    FDF_durability_level_t durability[] = {0,1,2};

    if((fp = fopen("FDF_EnumerateContainerObjects.log", "w+")) == 0){
        fprintf(stderr, " open failed!.\n");
        return -1;
    }

    if( 1 != preEnvironment())
        return 0;

    fprintf(fp, "************Begin to test ***************\n");
 
    for(int i = 0 ;  i < 3;i++){
        SetPropMode(eviction[i],persistent[i],fifo[i],writethru[i],async_writes[i],durability[i]);
        testCount = 0;
        OpenContainer("key",FDF_CTNR_CREATE,&cguid);

        result[i][0] = FDFEnumerateContainerObjects_basic_check1();
        result[i][1] = FDFEnumerateContainerObjects_basic_check2();
        result[i][2] = FDFEnumerateContainerObjects_noObject1();
        result[i][3] = FDFEnumerateContainerObjects_noObject2();
        result[i][4] = FDFEnumerateContainerObjects_Twice();
        result[i][5] = FDFEnumerateContainerObjects_invalid_cguid();
        result[i][6] = FDFEnumerateContainerObjects_MoreObject1(2);
        result[i][7] = FDFEnumerateContainerObjects_MoreObject2(3);
        result[i][8] = FDFEnumerateContainerObjects_Open_CreateObj_close1();
        //result[i][9] = FDFEnumerateContainerObjects_Open_CreateObj_close2();
    
        CloseContainer(cguid );
        DeleteContainer(cguid);
    }

    CleanEnvironment();
    
    for(int j = 0; j < 3;j++){
        fprintf(stderr, "test mode:eviction=%d,persistent=%d,fifo=%d,async_writes=%d,durability=%d.\n",eviction[j],persistent[j],fifo[j],async_writes[j],(j+1)%2+1);
        for(int i = 0; i < 9; i++){
            if(result[j][i] == 1){
                num++;
                fprintf(stderr, "FDFEnumerateContainerObjects test %drd success.\n",i+1);
            }
            else if(result[j][i] == -1)
                fprintf(stderr, "FDFEnumerateContainerObjects test %drd fail to test.\n",i+1);
            else if(result[j][i] == 0)
                fprintf(stderr, "FDFEnumerateContainerObjects test %drd failed.\n",i+1);
            else fprintf(stderr, "FDFEnumerateContainerObjects test %drd hit wrong.\n",i+1);
        }
    }

    if(resultCount == num){
        fprintf(stderr, "************ test pass!******************\n");
	fprintf(stderr, "#The related test script is FDF_EnumerateContainerObjects.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_EnumerateContainerObjects.log\n");
        return 0;
    }
    else 
        fprintf(stderr, "************%d test fail!******************\n",resultCount-num);
	fprintf(stderr, "#The related test script is FDF_EnumerateContainerObjects.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_EnumerateContainerObjects.log\n");
    return 1;
}



