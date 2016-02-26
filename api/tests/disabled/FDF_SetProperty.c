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

**********   Function: ZSSetProperty
***********************************************/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "zs.h"

#define  MAX_OBJECT_COUNT   20
static struct ZS_state     *zs_state;
struct ZS_thread_state     *_zs_thd_state;
ZS_container_props_t       p;
ZS_cguid_t                 cguid;

char                        key_out[MAX_OBJECT_COUNT][10];
char                        data_out[MAX_OBJECT_COUNT][10];
int                         num_out;
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
    p.evicting = ZS_TRUE;
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

ZS_status_t SetContainerProps(ZS_boolean_t evicting,ZS_boolean_t persistent,ZS_boolean_t fifo)
{
    ZS_status_t           ret;
    ZS_container_props_t  props_set;
    ret = ZSGetContainerProps(_zs_thd_state,cguid,&props_set);
    if(ZS_SUCCESS != ret)
        return ret;

    props_set.persistent = persistent;
    props_set.fifo_mode = fifo;
    props_set.evicting = evicting;

    ret = ZSSetContainerProps(_zs_thd_state,cguid,&props_set);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSSetContainerProps cguid=%ld success .\n",cguid);
    }
    else fprintf(fp,"ZSSetContainerProps cguid=%ld fail:.%s\n",cguid,ZSStrError(ret));
    return ret;
}

void OpenContainer(char *cname,uint32_t flags,ZS_cguid_t *cguid)
{
    ZS_status_t           ret;
    ret = ZSOpenContainer(_zs_thd_state,cname,&p,flags, cguid);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSOpenContainer cguid=%ld,cname=%s,flags=%d success\n",*cguid,cname,flags);
    }
    else fprintf(fp, "ZSOpenContainer cguid=%ld,cname=%s,flags=%d fail:.%s\n",*cguid,cname,flags,ZSStrError(ret));
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
        strcpy(key_out[num_out],key);
        strcpy(data_out[num_out],data);
        num_out++;
        fprintf(fp,"ZSNextEnumeratedObject return success.\n");
        fprintf(fp,"Object:key=%s,keylen=%d,data=%s,datalen=%ld.\n",key,keylen,data,datalen);
    }
    else fprintf(fp,"ZSNextEnumeratedObject return fail:%s.\n",ZSStrError(ret));
    return ret;
}

int CheckEnumeratedObject(char key_in[][10],char data_in[][10],int num_in)
{

    if(num_in == num_out){
        for(int i = 0; i < num_out;i++){
            if( (strcmp(key_in[i],key_out[i]) != 0)
               || (strcmp(data_in[i],data_out[i]) != 0)){
                fprintf(fp,"check object is not consistent.\n");
                return 0;
            }
        }
        fprintf(fp,"check object is right.\n");
        return 1;
    }
    fprintf(fp,"check object is wrong.\n");
    return 0;
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


int ZSSetProperty_basic_check()
{

    ZS_status_t           ret = ZS_SUCCESS;
    int                    flag;
    struct ZS_iterator    *iterator;
    char                   key[] = "key_a";
    char                   data[] = "data_a";
    char                   key_in[MAX_OBJECT_COUNT][10];
    char                   data_in[MAX_OBJECT_COUNT][10];
    int                    num_in = 0;
    num_out = 0;
    fprintf(fp,"test %d:\n",++testCount);
    
    CreateObject(cguid,key,6,data,7);
    strcpy(key_in[num_in],key);
    strcpy(data_in[num_in],data);
    num_in++;

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(ZS_SUCCESS != ret)
        flag =  -2;
    else{
        while(ZS_SUCCESS ==(ret =  NextEnumeratedObject(iterator)));
        FinishEnumeration(iterator);
        if(ZS_OBJECT_UNKNOWN == ret)
            flag = CheckEnumeratedObject(key_in,data_in,num_in);
        else flag = -1;
    }

    if(ZS_SUCCESS != DeleteObject(cguid,"key_a",6)) flag = -3;
    return flag;    
}



int ZSSetProperty_Open_CreateObj_close(int count)
{

    ZS_status_t           ret = ZS_SUCCESS;
    int                    flag;
    struct ZS_iterator    *iterator;
    char                   key[6] = "key_a";
    char                   data[7] = "data_a";
    char                   key_in[MAX_OBJECT_COUNT][10];
    char                   data_in[MAX_OBJECT_COUNT][10];
    int                    num_in = 0;
    int                flags[] = {ZS_CTNR_RO_MODE,ZS_CTNR_RW_MODE};
    fprintf(fp,"test %d:\n",++testCount);
        
    for(int i = 0; i < count;i++){
        ret = CreateObject(cguid,key,6,data,7);
        if(ZS_SUCCESS != ret){
            flag = -2;
            while(i>0){
                key[4]--;
                if(ZS_SUCCESS != DeleteObject(cguid,key,6)) flag = -3;
                i--;
            }
            return flag;
        }

        strcpy(key_in[num_in],key);
        strcpy(data_in[num_in],data);
        num_in++;
        CloseContainer(cguid );
        OpenContainer("x",flags[i%2],&cguid);
        key[4]++;
    }
    ret = EnumerateContainerObjects(cguid,&iterator);
    if(ZS_SUCCESS == ret){
        num_out = 0;
        while(ZS_SUCCESS == (ret = NextEnumeratedObject(iterator)));
        FinishEnumeration(iterator);
        if(ZS_OBJECT_UNKNOWN == ret){
            flag = CheckEnumeratedObject(key_in,data_in,num_in);
        }
        else flag = -1;
    }
    else flag = -2;

    for(int i = count;i > 0;i--){
        key[4]--;
        if(ZS_SUCCESS != DeleteObject(cguid,key,6)) flag = -3;
    }
    return flag;
}
int ZSSetProperty_MoreObject1(int count)
{
    ZS_status_t           ret = ZS_SUCCESS;
    int                    flag;
    struct ZS_iterator    *iterator;
    char                   key[6] = "key_a";
    char                   data[7] = "data_a";
    char                   key_in[MAX_OBJECT_COUNT][10];
    char                   data_in[MAX_OBJECT_COUNT][10];
    int                    num_in = 0;
    num_out = 0;
    fprintf(fp,"test %d:\n",++testCount);

    for(int i =0; i < count; i++){
        ret = CreateObject(cguid,key,6,data,7);
        strcpy(key_in[num_in],key);
        strcpy(data_in[num_in],data);
        num_in++;

        if(ZS_SUCCESS != ret){
            flag = -2;
            for(int j = i-1;j >= 0;j--){
                key[4]--;
                if(ZS_SUCCESS != DeleteObject(cguid,key,6)) flag = -3;
            }
            return flag;
        }
        key[4]++;
    }

    ret = EnumerateContainerObjects(cguid,&iterator);
    if(ZS_SUCCESS == ret){
        while(ZS_SUCCESS == (ret = NextEnumeratedObject(iterator)));
        FinishEnumeration(iterator);
        if(ZS_OBJECT_UNKNOWN == ret){
            flag = CheckEnumeratedObject(key_in,data_in,num_in);
        }
        else flag = -1;
    }
    else flag = -2;

    for(int j = count-1;j >= 0;j--){
        key[4]--;
        if(ZS_SUCCESS != DeleteObject(cguid,key,6)) flag = -3;
    }
    return flag;
}

int ZSSetProperty_MoreObject2(int count)
{

    ZS_status_t           ret1 = ZS_SUCCESS,ret2;
    ZS_cguid_t            cguid1;
    int                    flag;
    struct ZS_iterator    *iterator1,*iterator2;
    char                   key1[6] = "key_a";
    char                   key2[7] = "test_1";
    char                   data1[7] = "data_a";
    char                   data2[7] = "data_1";
    char                   key_in[2][MAX_OBJECT_COUNT][10];
    char                   data_in[2][MAX_OBJECT_COUNT][10];
    int                    num_in[2] ={ 0};
    fprintf(fp,"test %d:\n",++testCount);

    OpenContainer("test",ZS_CTNR_CREATE,&cguid1);

    for(int i =0; i < count; i++){
        ret1 = CreateObject(cguid,key1,6,data1,7);
        strcpy(key_in[0][num_in[0]],key1);
        strcpy(data_in[0][num_in[0]],data1);
        num_in[0]++;
        
        ret2 = CreateObject(cguid1,key2,7,data2,7);
        strcpy(key_in[1][num_in[1]],key2);
        strcpy(data_in[1][num_in[1]],data2);
        num_in[1]++;

        if(ZS_SUCCESS != ret1 || ZS_SUCCESS != ret2){
            flag = -2;
            if(ZS_SUCCESS == ret1)
                if(ZS_SUCCESS != DeleteObject(cguid,key1,6)) flag = -3;
            if(ZS_SUCCESS == ret2)
                if(ZS_SUCCESS != DeleteObject(cguid1,key2,7)) flag = -3;
            for(int j = i-1;j >= 0;j--){
                key1[4]--;
                if(ZS_SUCCESS != DeleteObject(cguid,key1,6))flag = -3;
                key2[5]--;
                if(ZS_SUCCESS != DeleteObject(cguid1,key2,7))flag = -3;
            }
            if(ZS_SUCCESS != CloseContainer(cguid1 ))flag = -3;
            if(ZS_SUCCESS != DeleteContainer(cguid1))flag = -3;
            return -2;
        }
        key1[4]++;
        key2[5]++;
    }
    ret1 = EnumerateContainerObjects(cguid,&iterator1);
    ret2 = EnumerateContainerObjects(cguid1,&iterator2);
    if(ZS_SUCCESS == ret1 && ZS_SUCCESS == ret2){
        num_out = 0;
        while(ZS_SUCCESS == (ret1 = NextEnumeratedObject(iterator1)));
        if(ZS_OBJECT_UNKNOWN == ret1 ){
            flag = CheckEnumeratedObject(key_in[0],data_in[0],num_in[0]);
            if(flag == 1){
                num_out = 0;
                while(ZS_SUCCESS == (ret2 = NextEnumeratedObject(iterator2)));
                if(ZS_OBJECT_UNKNOWN == ret2)
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
        if(ZS_SUCCESS == ret1){
            FinishEnumeration(iterator1);
        }
        if(ZS_SUCCESS == ret2){
            FinishEnumeration(iterator2);
        }

        flag = -2;
    }

    for(int j = count-1;j >= 0;j--){
        key1[4]--;
        key2[5]--;
        if(ZS_SUCCESS != DeleteObject(cguid,key1,6)) flag = -3;
        if(ZS_SUCCESS != DeleteObject(cguid1,key2,7)) flag = -3;
    }
    if(ZS_SUCCESS != CloseContainer(cguid1 ))flag = -3;
    if(ZS_SUCCESS != DeleteContainer(cguid1))flag = -3;
    return flag;
}
/**********  main function *******/

int main(int argc, char *argv[])
{
    int result[3][2][5] = {{{0,0}}};
    ZS_boolean_t eviction[] = {0,0,0};
    ZS_boolean_t persistent[] = {1,1,1};
    ZS_boolean_t fifo[] = {0,0,0};
    ZS_boolean_t writethru[] = {1,1,1};
    ZS_boolean_t async_writes[] = {0,1};
    ZS_durability_level_t durability[] = {0,1,2};
    char          property[] = "SDF_REFORMAT";
    char          value[][4] = {"1","0"};
    int     resultCount = 12;
    int     num = 0;

    if((fp = fopen("ZS_SetProperty.log", "w+")) == 0){
        fprintf(stderr, " open failed!.\n");
        return -1;
    }
    if( 1 != preEnvironment())
        return 0;
    fprintf(fp, "************Begin to test ***************\n");

    for(int j = 0 ;j < 1;j++){
    for(int i = 0 ;  i < 3;i++){

        ZSSetProperty(property,value[i]);
        testCount = 0;
        SetPropMode(eviction[i],persistent[i],fifo[i],writethru[i],async_writes[j],durability[i]);
        OpenContainer("x",ZS_CTNR_CREATE,&cguid);

        result[j][i][0] = ZSSetProperty_basic_check();
        result[j][i][1] = ZSSetProperty_Open_CreateObj_close(2);
//        result[j][i][2] = ZSSetProperty_MoreObject1(6);
//        result[j][i][3] = ZSSetProperty_MoreObject2(5);

        CloseContainer(cguid );
        DeleteContainer(cguid);
    }
    }
    CleanEnvironment();
    
    for(int k = 0; k < 1; k++){
    for(int j = 0;j < 3;j++){
        fprintf(stderr, "test mode:eviction=%d,persistent=%d,fifo=%d,writethru=%d,async_writes=%d,durability=%d.SDF_REFORMAT=%d\n",eviction[j],persistent[j],fifo[j],writethru[j],async_writes[k],durability[j],(j+1)%2);
        for(int i = 0; i < 4; i++){
            if(result[k][j][i] == 1){
                num++;
                fprintf(stderr, "ZSSetProperty %drd success.\n",i+1);
            }
            else if(result[k][j][i] == -2)
                fprintf(stderr, "ZSSetProperty test %drd fail to test.\n",i+1);
            else if(result[k][j][i] == 0)
                fprintf(stderr, "ZSSetProperty test %drd failed.\n",i+1);
            else if(result[k][j][i] == -1)
                fprintf(stderr, "ZSSetProperty test %drd return failed.\n",i+1);
            else fprintf(stderr, "ZSSetProperty test %drd hit wrong.\n",i+1);
        }
    }
    }
    if(resultCount == num){
        fprintf(stderr, "************ test pass!******************\n");
	fprintf(stderr, "#The related test script is ZS_SetProperty.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_SetProperty.log\n");
        return 0;
    }
    else 
        fprintf(stderr, "************%d test fail!******************\n",resultCount-num);
        fprintf(stderr, "#The related test script is ZS_SetProperty.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_SetProperty.log\n");
	return 1;
}



