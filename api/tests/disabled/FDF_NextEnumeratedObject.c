/*********************************************
**********   Author:  Lisa

**********   Function: FDFNextEnumeratedObject
***********************************************/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "fdf.h"

#define  MAX_OBJECT_COUNT   20
static struct FDF_state     *fdf_state;
struct FDF_thread_state     *_fdf_thd_state;
FDF_container_props_t       p;
char                        key_out[MAX_OBJECT_COUNT][10]={""};
char                        data_out[MAX_OBJECT_COUNT][10]={""};
int                         num_out;
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
    p.evicting = FDF_TRUE;
    p.async_writes = FDF_TRUE;    
    return 1;
}

void CleanEnvironment()
{
    FDFReleasePerThreadState(&_fdf_thd_state);
    FDFShutdown(fdf_state);
}

void SetPropMode(FDF_boolean_t evicting,FDF_boolean_t persistent, FDF_boolean_t fifo,FDF_boolean_t writethru,FDF_boolean_t async_writes, FDF_durability_level_t durability)
{
    p.evicting = evicting;
    p.persistent = persistent;
    p.fifo_mode = fifo;
    p.writethru = writethru;
    p.async_writes = async_writes;
    p.durability_level = durability;
}

FDF_status_t SetContainerProps(FDF_cguid_t cguid,FDF_boolean_t evicting,FDF_boolean_t persistent,FDF_boolean_t fifo)
{
    FDF_status_t           ret;
    FDF_container_props_t  props_set;
    ret = FDFGetContainerProps(_fdf_thd_state,cguid,&props_set);
    if(FDF_SUCCESS != ret)
        return ret;

    props_set.persistent = persistent;
    props_set.fifo_mode = fifo;
    props_set.evicting = evicting;

    ret = FDFSetContainerProps(_fdf_thd_state,cguid,&props_set);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFSetContainerProps cguid=%ld success .\n",cguid);
    }
    else fprintf(fp,"FDFSetContainerProps cguid=%ld fail:%s\n",cguid,FDFStrError(ret));
    return ret;
}

FDF_status_t OpenContainer(char *cname,uint32_t flags,FDF_cguid_t *cguid)
{
    FDF_status_t           ret;
    ret = FDFOpenContainer(_fdf_thd_state,cname,&p,flags, cguid);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFOpenContainer cguid=%ld,cname=%s,mode=%d success\n",*cguid,cname,flags);
    }
    else fprintf(fp,"FDFOpenContainer cguid=%ld,cname=%s,mode=%d fail:%s\n",*cguid,cname,flags,FDFStrError(ret));
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

FDF_status_t ReadObject(FDF_cguid_t cguid,char *key,uint32_t keylen,char **data,uint64_t *datalen)
{
    FDF_status_t           ret;
    ret = FDFReadObject(_fdf_thd_state,cguid,key,keylen,data,datalen);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFReadObject cguid=%ld,key=%s success.\n",cguid,key);
        fprintf(fp,"FDFReadObject:key=%s data=%s,dataln=%ld.\n",key,*data,*datalen);
    }
    else fprintf(fp,"FDFReadObject cguid=%ld,key=%s fail:%s.\n",cguid,key,FDFStrError(ret));
    return ret;
}

FDF_status_t DeleteObject(FDF_cguid_t cguid,char *key,uint32_t keylen)
{
    FDF_status_t           ret;

    ret = FDFDeleteObject(_fdf_thd_state,cguid,key,keylen);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFDeleteObject cguid=%ld key=%s success.\n",cguid,key);
    }
    else fprintf(fp,"FDFDeleteObject cguid=%ld key=%s fail:%s.\n",cguid,key,FDFStrError(ret));
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
        strcpy(key_out[num_out],key);
        strcpy(data_out[num_out],data);
        num_out++;
        fprintf(fp,"FDFNextEnumeratedObject return success.\n");
        fprintf(fp,"Object:key=%s,keylen=%d,data=%s,datalen=%ld.\n",key,keylen,data,datalen);
    }
    else fprintf(fp,"FDFNextEnumeratedObject return fail:%s.\n",FDFStrError(ret));
    return ret;
}

int CheckEnumeratedObject(char key_in[][10],char data_in[][10],int num_in)
{

    if(num_in == num_out){
        for(int i = 0; i < num_out;i++){
            if( (strcmp(key_in[i],key_out[i]) != 0)
               || (strcmp(data_in[i],data_out[i]) != 0)){
                
                fprintf(fp,"CheckEnumeratedObject is not same.\n");
                fprintf(fp,"check:key_in=%s,data_in=%s,key_out=%s,data_out=%s.\n",key_in[i],data_in[i],key_out[i],data_out[i]);
                return 0;
            }
        }
        fprintf(fp,"CheckEnumeratedObject is right.\n");
        return 1;
    }
    fprintf(fp,"CheckEnumeratedObject is wrong.\n");
    return 0;
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


int FDFNextEnumeratedObject_basic_check1()
{

    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    char                   key[] = "key_a";
    char                   data[] = "data_a";
    char                   key_in[MAX_OBJECT_COUNT][10]={""};
    char                   data_in[MAX_OBJECT_COUNT][10]={""};
    int                    num_in = 0;
    char                  *r_data;
    uint64_t               datalen;
    num_out = 0;
    FDF_cguid_t  cguid;   
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("x1",FDF_CTNR_CREATE,&cguid);
    
    fprintf(stderr,"test read!\n");
    CreateObject(cguid,key,6,data,7);
    fprintf(stderr,"test read--write ok!\n");
    FDFReadObject(_fdf_thd_state,cguid,key,6,&r_data,&datalen);
    strcpy(key_in[num_in],key);
    strcpy(data_in[num_in],data);
    num_in++;

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(FDF_SUCCESS != ret)
        flag =  -2;
    else{
        while(FDF_SUCCESS ==(ret =  NextEnumeratedObject(iterator)));
        FinishEnumeration(iterator);
        if(FDF_OBJECT_UNKNOWN == ret)
            flag = CheckEnumeratedObject(key_in,data_in,num_in);
        else flag = -1;
    }

    if(FDF_SUCCESS != DeleteObject(cguid,"key_a",6))flag = -3;
    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;    
}

int FDFNextEnumeratedObject_basic_check2()
{
    FDF_status_t           ret;
    int                    flag;
    struct FDF_iterator    *iterator;
    FDF_cguid_t            cguid;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("x2",FDF_CTNR_CREATE,&cguid);
    CreateObject(cguid,"key_a",6,"data_a",7);
    ret = EnumerateContainerObjects(cguid,&iterator);
    if(FDF_SUCCESS != ret)
        flag =  -2;
    else{
        ret =  NextEnumeratedObject(iterator);
        FinishEnumeration(iterator);
        if(FDF_SUCCESS == ret)
            flag = 1;
        else flag = -1;
    }
    if(FDF_SUCCESS != DeleteObject(cguid,"key_a",6))flag = -3;
    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}


int FDFNextEnumeratedObject_Open_CreateObj_close(int count)
{

    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    char                   key[6] = "key_a";
    char                   data[7] = "data_a";
    char                   key_in[MAX_OBJECT_COUNT][10]={""};
    char                   data_in[MAX_OBJECT_COUNT][10]={""};
    int                    num_in = 0;
    int                    flags[] = {FDF_CTNR_RO_MODE,FDF_CTNR_RW_MODE};
    FDF_cguid_t            cguid;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("x3",FDF_CTNR_CREATE,&cguid);
    for(int i = 0; i < count;i++){
        ret = CreateObject(cguid,key,6,data,7);
        if(FDF_SUCCESS != ret){
            flag = -2;
            while(i>0){
                key[4]--;
                if(FDF_SUCCESS != DeleteObject(cguid,key,6))flag = -3;
                i--;
            }
            if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
            if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
            return flag;
        }

        strcpy(key_in[num_in],key);
        strcpy(data_in[num_in],data);
        num_in++;
        CloseContainer(cguid );
        OpenContainer("x3",flags[i%2],&cguid);
        key[4]++;
    }

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(FDF_SUCCESS == ret){
        num_out = 0;
        while(FDF_SUCCESS == (ret = NextEnumeratedObject(iterator)));
        FinishEnumeration(iterator);
        if(FDF_OBJECT_UNKNOWN == ret){
            flag = CheckEnumeratedObject(key_in,data_in,num_in);
        }
        else flag = -1;
    }
    else flag = -2;


    for(int i = count;i > 0;i--){
        key[4]--;
        if(FDF_SUCCESS != DeleteObject(cguid,key,6)) flag = -3;
    }
    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}
int FDFNextEnumeratedObject_MoreObject1(int count)
{
    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    char                   key[6] = "key_a";
    char                   data[7] = "data_a";
    char                   key_in[MAX_OBJECT_COUNT][10]={""};
    char                   data_in[MAX_OBJECT_COUNT][10]={""};
    int                    num_in = 0;
    num_out = 0;
    FDF_cguid_t            cguid;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("x4",FDF_CTNR_CREATE,&cguid);
    for(int i =0; i < count; i++){
        ret = CreateObject(cguid,key,6,data,7);
        strcpy(key_in[num_in],key);
        strcpy(data_in[num_in],data);
        num_in++;
        if(FDF_SUCCESS != ret){
            flag = -2;
            for(int j = i-1;j >= 0;j--){
                key[4]--;
                if(FDF_SUCCESS != DeleteObject(cguid,key,6)) flag = -3;
            }
            if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
            if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
            return flag;
        }
        key[4]++;
    }

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(FDF_SUCCESS == ret){
        while(FDF_SUCCESS == (ret = NextEnumeratedObject(iterator)));
        FinishEnumeration(iterator);
        if(FDF_OBJECT_UNKNOWN == ret){
            flag = CheckEnumeratedObject(key_in,data_in,num_in);
        }
        else flag = -1;
    }
    else flag = -2;

    for(int j = count-1;j >= 0;j--){
        key[4]--;
        if(FDF_SUCCESS != DeleteObject(cguid,key,6)) flag = -3;
    }
    
    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}

int FDFNextEnumeratedObject_MoreObject2(int count)
{

    FDF_status_t           ret1 = FDF_SUCCESS,ret2;
    FDF_cguid_t            cguid1;
    int                    flag;
    struct FDF_iterator    *iterator1,*iterator2;
    char                   key1[6] = "key_a";
    char                   key2[7] = "test_1";
    char                   data1[7] = "data_a";
    char                   data2[7] = "data_1";
    char                   key_in[2][MAX_OBJECT_COUNT][10]={{""}};
    char                   data_in[2][MAX_OBJECT_COUNT][10]={{""}};
    int                    num_in[2] ={ 0};
    FDF_cguid_t            cguid;
    fprintf(fp,"test %d:\n",++testCount);
    
    OpenContainer("x5",FDF_CTNR_CREATE,&cguid);
    OpenContainer("test",FDF_CTNR_CREATE,&cguid1);

    for(int i =0; i < count; i++){
        ret1 = CreateObject(cguid,key1,6,data1,7);
        strcpy(key_in[0][num_in[0]],key1);
        strcpy(data_in[0][num_in[0]],data1);
        num_in[0]++;
        
        ret2 = CreateObject(cguid1,key2,7,data2,7);
        strcpy(key_in[1][num_in[1]],key2);
        strcpy(data_in[1][num_in[1]],data2);
        num_in[1]++;

        if(FDF_SUCCESS != ret1 || FDF_SUCCESS != ret2){
            flag = -2;
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
            if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
            if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
            return flag;
        }
        key1[4]++;
        key2[5]++;
    }
    ret1 = EnumerateContainerObjects(cguid,&iterator1);
    ret2 = EnumerateContainerObjects(cguid1,&iterator2);
    if(FDF_SUCCESS == ret1 && FDF_SUCCESS == ret2){
        num_out = 0;
        while(FDF_SUCCESS == (ret1 = NextEnumeratedObject(iterator1)));
        if(FDF_OBJECT_UNKNOWN == ret1 ){
            flag = CheckEnumeratedObject(key_in[0],data_in[0],num_in[0]);
            if(flag == 1){
                num_out = 0;
                while(FDF_SUCCESS == (ret2 = NextEnumeratedObject(iterator2)));
                if(FDF_OBJECT_UNKNOWN == ret2)
                    flag = CheckEnumeratedObject(key_in[1],data_in[1],num_in[1]);
                else flag = -1;
            }
            else flag = -2;
        }
        else flag = -1;
        FinishEnumeration(iterator1);
        FinishEnumeration(iterator2);
    }
    else{
        if(FDF_SUCCESS == ret1){
            FinishEnumeration(iterator1);
        }
        if(FDF_SUCCESS == ret2){
            FinishEnumeration(iterator2);
        }

        flag = -2;
    }

    for(int j = count-1;j >= 0;j--){
        key1[4]--;
        key2[5]--;
        if(FDF_SUCCESS != DeleteObject(cguid,key1,6)) flag = -3;
        if(FDF_SUCCESS != DeleteObject(cguid1,key2,7)) flag = -3;
    }
    if(FDF_SUCCESS != CloseContainer(cguid1 ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid1))flag = -3;
    if(FDF_SUCCESS != CloseContainer(cguid ))flag = -3;
    if(FDF_SUCCESS != DeleteContainer(cguid))flag = -3;
    return flag;
}


int FDFNextEnumeratedObject_MoreObject3(int count)
{
    FDF_status_t           ret = FDF_SUCCESS;
    int                    flag;
    struct FDF_iterator    *iterator;
    char                   key[6] = "key_a";
    char                   data[1024] = "data_a";
    int                    i;
    char                   *key_enum;
    uint32_t               keylen;
    char                   *data_enum;
    uint64_t               datalen;
    int                    num_in = 0;
    num_out = 0;
    FDF_cguid_t            cguid;
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("x4",FDF_CTNR_CREATE,&cguid);

    for( i=0; i < count; i++){
        ret = CreateObject(cguid,key,6,data,1024);
        if(FDF_SUCCESS != ret){
            break; 
        }
        num_in++;
        key[4]++;
    }

    ret = EnumerateContainerObjects(cguid,&iterator);
    while(FDF_SUCCESS == ret){
        ret = FDFNextEnumeratedObject(_fdf_thd_state,iterator,&key_enum,&keylen,&data_enum,&datalen);
        if(FDF_SUCCESS == ret)num_out++;
    }
    
    FinishEnumeration(iterator);
    fprintf(fp,"num_in=%d,num_out=%d.\n",num_in,num_out);
    if(num_out == num_in){
        flag = 1;
        fprintf(fp,"num_in=%d,num_out=%d.\n",num_in,num_out);
    }
    else flag = 0;

    for(int j = i;j > 0;j--){
        key[4]--;
        if(FDF_SUCCESS != DeleteObject(cguid,key,6)) flag = -3;
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
    int resultCount = 12;
    int num = 0;
    FDF_boolean_t async_writes[] = {0,1};
    FDF_durability_level_t durability[] = {0,1,2};

    if((fp = fopen("FDF_NextEnumeratedObject.log", "w+")) == 0){
        fprintf(stderr, " open failed!.\n");
        return -1;
    }
    if( 1 != preEnvironment())
        return 0;
    fprintf(fp, "************Begin to test ***************\n");

    for(int j = 0; j < 1; j++){
    for(int i = 0 ;  i < 3;i++){
        testCount = 0;
        SetPropMode(eviction[i],persistent[i],fifo[i],writethru[i],async_writes[j],durability[i]);

        result[j][i][0] = FDFNextEnumeratedObject_basic_check1();
        result[j][i][1] = FDFNextEnumeratedObject_basic_check2();
//        result[j][i][2] = FDFNextEnumeratedObject_Open_CreateObj_close(3);
        result[j][i][3] = FDFNextEnumeratedObject_MoreObject1(2);
//        result[j][i][4] = FDFNextEnumeratedObject_MoreObject2(2);
        result[j][i][5] = FDFNextEnumeratedObject_MoreObject3(1025);
    }
    }
    CleanEnvironment();
    
    for(int k = 0;k < 1;k++){
    for(int j = 0;j < 3;j++){
        fprintf(stderr, "test mode:eviction=%d,persistent=%d,fifo=%d,async=%d,durability=%d.\n",eviction[j],persistent[j],fifo[j],async_writes[k],durability[j]);
        for(int i = 0; i < 6; i++){
            if(result[k][j][i] == 1){
                num++;
                fprintf(stderr, "FDFNextEnumeratedObject %drd success.\n",i+1);
            }
            else if(result[k][j][i] == -2)
                fprintf(stderr, "FDFNextEnumeratedObject test %drd fail to test.\n",i+1);
            else if(result[k][j][i] == 0)
                fprintf(stderr, "FDFNextEnumeratedObject test %drd failed.\n",i+1);
            else if(result[k][j][i] == -1)
                fprintf(stderr, "FDFNextEnumeratedObject test %drd return failed.\n",i+1);
            else fprintf(stderr, "FDFNextEnumeratedObject test %drd hit wrong.\n",i+1);
        }
    }
    }
    if(resultCount == num){
        fprintf(stderr, "************ test pass!******************\n");
	fprintf(stderr, "#The related test script is FDF_NextEnumeratedObject.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_NextEnumeratedObject.log\n");
        return 0;
    }
    else 
        fprintf(stderr, "************%d test fail!******************\n",resultCount-num);
	fprintf(stderr, "#The related test script is FDF_NextEnumeratedObject.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_NextEnumeratedObject.log\n");
    	return 1;
}



