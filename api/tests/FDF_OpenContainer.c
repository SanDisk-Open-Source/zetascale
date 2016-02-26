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

**********   Function: ZSOpenContainer
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
const int                   MAX_COUNT = 128;
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
/*
    p.durability_level = 0;
    p.fifo_mode = ZS_FALSE;
    p.size_kb = 10;
    p.num_shards = 1;
    p.persistent = ZS_TRUE;
    p.writethru = ZS_TRUE;
    p.evicting = ZS_TRUE;
    p.async_writes = ZS_TRUE; 
    */
    (void)ZSLoadCntrPropDefaults(&p);
    return 1;
}

void CleanEnvironment()
{
    ZSReleasePerThreadState(&_zs_thd_state);
    ZSShutdown(zs_state);
}

void SetProps(uint64_t size,ZS_boolean_t evicting,ZS_boolean_t persistence,ZS_boolean_t fifo,
               ZS_boolean_t writethru,ZS_durability_level_t durability)
{
    p.durability_level = durability;
    p.size_kb = size;
    p.persistent = persistence;
    p.fifo_mode = fifo;
    p.evicting = evicting;
    p.writethru = writethru;
}

ZS_status_t SetContainerProps(ZS_cguid_t cguid,ZS_boolean_t evicting,ZS_boolean_t persistent,ZS_boolean_t fifo,ZS_boolean_t async)
{
    ZS_status_t           ret;
    ZS_container_props_t  props_set;
    ret = ZSGetContainerProps(_zs_thd_state,cguid,&props_set);
    if(ZS_SUCCESS != ret)
    return ret;

    props_set.persistent = persistent;
    props_set.fifo_mode = fifo;
    props_set.evicting = evicting;
    props_set.async_writes = async;

    fprintf(fp,"Props persistet=%d,fifo=%d,evict=%d,async=%d.\n",persistent,fifo,evicting,async);
    ret = ZSSetContainerProps(_zs_thd_state,cguid,&props_set);
    if(ZS_SUCCESS == ret){
    fprintf(fp,"ZSSetContainerProps success .\n");
    }
    else fprintf(fp,"ZSSetContainerProps fail:.%s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t OpenContainer(char *cname,ZS_container_props_t *props,uint32_t flags,ZS_cguid_t *cguid)
{
    ZS_status_t           ret;
    ret = ZSOpenContainer(_zs_thd_state,cname,props,flags, cguid);
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSOpenContainer cguid=%ld cname=%s,flags=%d success.\n",*cguid,cname,flags);
    }
    else fprintf(fp, "ZSOpenContainer %s fail:%s.\n",cname,ZSStrError(ret));
    return ret;
}


ZS_status_t CloseContainer(ZS_cguid_t cguid)
{
    ZS_status_t           ret;
    ret = ZSCloseContainer(_zs_thd_state, cguid );
    if(ZS_SUCCESS == ret){
        fprintf(fp,"ZSCloseContainer success cguid=%ld.\n",cguid);
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
        fprintf(fp," ZSWriteObject cguid=%ld,key=%s,data=%s success.\n",cguid,key,data);
    }
    else fprintf(fp," ZSWriteObject cguid=%ld,key=%s,data=%s failed:%s.\n",cguid,key,data,ZSStrError(ret));
//    sleep(5); // Why is it here?
    return ret;
}


ZS_status_t DeleteObject(ZS_cguid_t cguid,char *key,uint32_t keylen)
{
    ZS_status_t           ret;
    ret = ZSDeleteObject(_zs_thd_state,cguid,key,keylen);
    if(ZS_SUCCESS == ret){
        fprintf(fp," ZSDeleteObject cguid=%ld,key=%s success.\n",cguid,key);
    }
    else fprintf(fp," ZSDeleteObject cguid=%ld,key=%s failed:%s.\n",cguid,key,ZSStrError(ret));
    return ret;
}


int ZSOpenContainer_basic_check1()
{

    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;

    fprintf(fp,"test %d:\n" ,++testCount);
    ret = OpenContainer("t",&p,ZS_CTNR_CREATE,&cguid);
    
    if(ZS_SUCCESS == ret){
        ret = OpenContainer("t",&p,ZS_CTNR_RO_MODE,&cguid);
    }
    else return -1;

    if(ZS_SUCCESS == ret){
        ret = OpenContainer("t",&p,ZS_CTNR_RW_MODE,&cguid);
    }
    CloseContainer(cguid );
    if(ZS_SUCCESS != DeleteContainer(cguid))
        return -2;
    
    if(ZS_SUCCESS == ret)
        return 1;
    return 0;
}


/*int ZSOpenContainer_basic_check2()
{

    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;
      
    fprintf(fp,"test %d:\n",++testCount);
    if(p.evicting == 1)
        p.writethru = 0;
    ret = OpenContainer("test",&p,ZS_CTNR_CREATE,&cguid);
    
    if(ZS_SUCCESS == ret){
        ret = OpenContainer("test",&p,ZS_CTNR_RW_MODE,&cguid);
    }
    else return -1;
    if(ZS_SUCCESS == ret){
        ret = OpenContainer("test",&p,ZS_CTNR_RO_MODE,&cguid);
    }
    CloseContainer(cguid );
    if(ZS_SUCCESS != DeleteContainer(cguid))
        return -2;

    p.writethru = 1;
    if(ZS_SUCCESS == ret)
        return 1;
    return 0;
}
*/


int ZSOpenContainer_basic_check_size()
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;

    fprintf(fp,"test %d:\n",++testCount);
    p.size_kb = 0;
    ret = OpenContainer("test",&p,ZS_CTNR_CREATE,&cguid);
    p.size_kb = 3;   

    if(ZS_SUCCESS == ret){
        fprintf(fp,"set size = 0 Create container success.\n");
        CloseContainer(cguid );
        if(ZS_SUCCESS != DeleteContainer(cguid))
            return -2;
        return 1;
    }
    else{
        fprintf(fp,"set size = 0 Create container failed.\n");
        return 0;
    }
}

int ZSOpenContainer_openCloseMore1(int count)
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;
    int                    i;
    uint32_t               flags[2]={ZS_CTNR_RO_MODE,ZS_CTNR_RW_MODE};

    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("test6",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;

    CloseContainer(cguid);
    for(i = 0;i < count-1; i++){
        ret = OpenContainer("test6",&p,flags[i%2],&cguid);
        CloseContainer(cguid);
        if(ZS_SUCCESS != ret)
            break;
    }
    if(ZS_SUCCESS == ret){
        fprintf(fp,"OpenContainer OpenCloseMore success.\n");
        flag = 1;
    }
    else{
        fprintf(fp,"OpenContainer OpenCloseMore fail:%s.\n",ZSStrError(ret));
        flag = 0;
    }

    if(ZS_SUCCESS != DeleteContainer(cguid))
        return -2;
    return flag;
}


int ZSOpenContainer_openCloseMore2(int count)
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;
    ZS_boolean_t          eviction[] = {0,0};
    ZS_boolean_t          persistence[] = {1,1};
    ZS_boolean_t          fifo[] = {0,0};
    int                    i;
    uint32_t               flags[2]={ZS_CTNR_RO_MODE,ZS_CTNR_RW_MODE};

    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("test6",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;

    for(i = 0; i < count;i++){
        ret = OpenContainer("test6",&p,flags[i%2],&cguid);
        SetContainerProps(cguid,eviction[i%2],persistence[i%2],fifo[i%2],i%2);
        CloseContainer(cguid);
        
        if(ZS_SUCCESS != ret)
            break;
    }

    if(ZS_SUCCESS == ret){
        fprintf(fp,"OpenContainer OpenCloseMore success.\n");
        flag = 1;
    }
    else{
        fprintf(fp,"OpenContainer OpenCloseMore fail:%s.\n",ZSStrError(ret));
        flag = 0;
    }
    if(ZS_SUCCESS != DeleteContainer(cguid))
        return -2;
    return flag;
}

/* On enabling RO containers, define this MACRO */
#undef RO_MODE_SUPPORTED
int ZSOpenContainer_openCloseMore_createObj(int count)
{
    ZS_status_t           ret,ret_obj;
    ZS_cguid_t            cguid;
    int                    flag, i;
    char                   key[6]="key_a";
    uint32_t               flags[2]={ZS_CTNR_RO_MODE,ZS_CTNR_RW_MODE};
	int						tmp = 0;

    // ZS + btree minimum container size is 8KB.
    p.size_kb = 32;
    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("test6",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;

    ret_obj = CreateObject(cguid,key,6,"data",5);
    CloseContainer(cguid);

    for(i = 0;i < count; i++){
        key[4]++;
        ret = OpenContainer("test6",&p,flags[i%2],&cguid);
        ret_obj = CreateObject(cguid,key,6,"data",5);
        CloseContainer(cguid);
#ifdef RO_MODE_SUPPORTED
        if(ZS_SUCCESS != ret) {
            break;
		} else if ((i%2 == 0) && (ret_obj == ZS_SUCCESS)) {
			tmp = 1;
			break;
		} else if ((i%2 != 0) && (ret_obj != ZS_SUCCESS)) {
			tmp = 1;
			break;
		}
#else
		if (ZS_SUCCESS != ret || ZS_SUCCESS != ret_obj) {
			tmp = 1;
			break;
		}
#endif
    }

    if(i == count) {
        fprintf(fp,"OpenContainer OpenCloseMore to create obj  success.\n");
        flag = 1;
    }
    else if(tmp == 1) {
        fprintf(fp,"OpenContainer OpenCloseMore to create obj  failed:%s.\n",ZSStrError(ret));
        flag = -2;
    }
    else{
        fprintf(fp,"OpenContainer OpenCloseMore_obj fail:%s.\n",ZSStrError(ret));
        flag = 0;
    }
    ret = OpenContainer("test6",&p,flags[1],&cguid);
    if(ZS_SUCCESS != ret)
        return -1;
    for(int i = count;i >=0; i--){
		ret = DeleteObject(cguid,key,6);
#ifdef RO_MODE_SUPPORTED
        if((i%2 == 0) && (ret != ZS_SUCCESS)) {
            flag = -2;
		} else if ((i%2 != 0) && (ret == ZS_SUCCESS)) {
			flag = -2;
		}
#else
		if (ret != ZS_SUCCESS) {
			flag = -2;
		}
#endif
        key[4]--;
    }
    CloseContainer(cguid);
    if(ZS_SUCCESS != DeleteContainer(cguid))
        flag = -2;
    return flag;
}

int ZSOpenContainer_openMore(int count)
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;
    uint32_t               flags[2]={ZS_CTNR_RO_MODE,ZS_CTNR_RW_MODE};

    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("test7",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;

    for(int i = 0;i < count; i++){
        ret = OpenContainer("test7",&p,flags[i%2],&cguid);
        if(ZS_SUCCESS != ret)
            break;
    }

    if(ZS_SUCCESS == ret){
        fprintf(fp,"OpenContainer OpenMore success.\n");
        flag = 1;
    }
    else{
        fprintf(fp,"OpenContainer OpenMore fail:%s.\n",ZSStrError(ret));
        flag = 0;
    }

    CloseContainer(cguid);
    if(ZS_SUCCESS != DeleteContainer(cguid))
        flag = -2;
    return flag;
}

int ZSOpenContainer_createDeletedMore(int count)
{
    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid;

    fprintf(fp,"test %d:\n",++testCount);
    for(int i =0 ;i < count;i++){
        ret = OpenContainer("t",&p,ZS_CTNR_CREATE,&cguid);
        CloseContainer(cguid );
        DeleteContainer(cguid);

        if(ZS_SUCCESS != ret){
            fprintf(fp,"OpenContainer %drd create deleted container failed:%s.\n",count,ZSStrError(ret));
            return 0;
        }
    }

    fprintf(fp,"OpenContainer  create deleted %d times success.\n",count);
    return 1;
}

int ZSOpenContainer_createMore(int count)
{

    ZS_status_t           ret = ZS_SUCCESS;
    ZS_cguid_t            cguid[MAX_COUNT];
    char                   cname[13] = "cntr_test_a";
    int                    flag;

    fprintf(fp,"test %d:\n",++testCount);
    for(int i = 0;i< count;i++)
    {
        ret = OpenContainer(cname,&p,ZS_CTNR_CREATE,&cguid[i]);
        if(ZS_SUCCESS != ret){
            flag = -1;
            for(int j = i-1;j>=0;j--){
                CloseContainer(cguid[j] );
                if(ZS_SUCCESS != DeleteContainer(cguid[j]))
                    flag = -2;
            }
            return flag;
        }
        cname[9]++;
    }

    if(ZS_SUCCESS == ret){
        flag = 1;
        fprintf(fp,"ZSOpenContainer create %d containers success:\n",count);
    }
    else{
        flag = 0;
        fprintf(fp,"ZSOpenContainer create %d containers failed:%s\n",count,ZSStrError(ret));
    }
    for(int i = 0;i< count;i++){
        CloseContainer(cguid[i] );
        if(ZS_SUCCESS != DeleteContainer(cguid[i]))
            flag = -2;
    }

    return flag;
}

int ZSOpenContainer_invalid_cguid()
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    ZS_cguid_t            cguid_invalid;
    int                    flag;
    
    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("test8",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;
    
    ret = OpenContainer("test8",&p,ZS_CTNR_RW_MODE,&cguid_invalid);
    if(ZS_SUCCESS != ret){
        fprintf(fp,"Open created Container invalid cguid fail:%s.\n",ZSStrError(ret));
        flag = 0;
    }
    else{
        fprintf(fp,"Open created Container invalid cguid success.\n");
        flag = 1;
    }
    CloseContainer(cguid );
    if(ZS_SUCCESS != DeleteContainer(cguid))
        flag = -2;
    
    return flag;
}


int ZSOpenContainer_invalid_flags()
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;

    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("test10",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;

    ret = OpenContainer("test10",&p,0,&cguid);    
    if(ZS_SUCCESS == ret){
        fprintf(fp,"Open Created Container invalid mode SUCCESS.\n");
        flag = 1;
    }
    else{
        fprintf(fp,"Open Created Container invalid mode fail:%s.\n",ZSStrError(ret));
        flag = 0;
    }

    CloseContainer(cguid);
    if(ZS_SUCCESS != DeleteContainer(cguid))
        flag = -2;
    return flag;
}


int ZSOpenContainer_flags_check()
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid,cguid_tmp;
    int                    flag = 1;
    uint32_t               flags[2]={ZS_CTNR_RO_MODE,ZS_CTNR_RW_MODE};

    fprintf(fp,"test %d:\n",++testCount); 
    for(int i = 0;i < 2;i++){
        ret = OpenContainer("test11",&p,flags[i],&cguid);
        if(ZS_SUCCESS != ret){
            fprintf(fp,"ZSOpenContainer create one mode=%d Fail:%s.\n",flags[i],ZSStrError(ret));
            if(flag==1)flag = 1;
        }
        else{
            flag = 0;
            fprintf(fp,"ZSOpenContainer create one mode=%d success.\n",flags[i]);
            CloseContainer(cguid);
            DeleteContainer(cguid);
        }
    }

    ret = OpenContainer("test11",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret)
        return -1;

    ret = OpenContainer("test11",&p,ZS_CTNR_CREATE,&cguid_tmp);    
    
    if(ZS_SUCCESS != ret){
        fprintf(fp,"ZSOpenContainer create twice failed.\n");
        if(flag==1)flag = 1;
    }
    else{
        fprintf(fp,"ZSOpenContainer create twice succeed:%s.\n",ZSStrError(ret));
        flag = 0;
        CloseContainer(cguid_tmp);
        if(ZS_SUCCESS != DeleteContainer(cguid_tmp))
            flag = -2;
    }

    CloseContainer(cguid);
    if(ZS_SUCCESS != DeleteContainer(cguid))
        flag = -2;
    
    return flag;
}


int ZSOpenContainer_invalid_cname1()
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;
    char                   cname[1];
    cname[0] = '\0';
    
    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer(cname,&p,ZS_CTNR_CREATE,&cguid);

    if(ZS_SUCCESS != ret){
        fprintf(fp,"OpenContainer invalid cname fail:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"OpenContainer invalid cname success.\n");
        CloseContainer(cguid);
        DeleteContainer(cguid);
        flag = 0;
    }

    return flag;
}

int ZSOpenContainer_invalid_cname2()
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;
    
    fprintf(fp,"test %d:\n",++testCount);
    ret = ZSOpenContainer(_zs_thd_state,NULL,&p,ZS_CTNR_CREATE,&cguid);

    if(ZS_SUCCESS != ret){
        fprintf(fp,"OpenContainer invalid cname failed:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"OpenContainer invalid cname success.\n");
        CloseContainer(cguid);
        DeleteContainer(cguid);
        flag = 0;
    }

    return flag;
}


int ZSOpenContainer_invalid_cname3()
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid,cguid_tmp;
    int                    flag;

    fprintf(fp,"test %d:\n",++testCount);
    ret = ZSOpenContainer(_zs_thd_state,"test",&p,ZS_CTNR_CREATE,&cguid);
    if(ZS_SUCCESS != ret){
        return -1;
    }

    cguid_tmp = cguid;
    ret = ZSOpenContainer(_zs_thd_state,"x",&p,ZS_CTNR_RW_MODE,&cguid_tmp);
    if(ZS_SUCCESS != ret){
        fprintf(fp,"OpenContainer invalid cname fail:%s.\n",ZSStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"OpenContainer invalid cname success.\n");
        flag = 0;
    }
    CloseContainer(cguid);
    if(ZS_SUCCESS != DeleteContainer(cguid))
        flag = -2;
    return flag;
}

int ZSOpenContainer_invalid_props()
{
    ZS_status_t           ret;
    ZS_cguid_t            cguid;
    int                    flag;
    
    fprintf(fp,"test %d:\n",++testCount);
    p.writethru = 0;
    p.evicting = 0;
    ret = OpenContainer("test12",&p,ZS_CTNR_CREATE,&cguid);

	// We set writethru=1 and evicting=0 in create so this open will pass
    if(ZS_SUCCESS == ret){
        fprintf(fp,"OpenContainer invalid props fail:%s.\n",ZSStrError(ret));
        CloseContainer(cguid);
        DeleteContainer(cguid);
        flag = 1;
    }
    else{
        fprintf(fp,"OpenContainer invalid props success.\n");
        flag = 0;
    }
    p.writethru = 1;
    return flag;
}


/**********  main function *******/

int main(int argc, char *argv[])
{
    int result[2][3][20] = {{{0,0}}};
    int                  num = 0,totalNum = 90;
    ZS_boolean_t eviction[3] = {0,0,0};
    ZS_boolean_t persistent[3] = {1,1,1};
    ZS_boolean_t fifo[3] = {0,0,0};
    ZS_boolean_t writethru[3]={1,1,1};
    ZS_boolean_t async_write[2]={0,1};
    ZS_durability_level_t durability[3] = {0,1,2/*ZS_DURABILITY_PERIODIC,ZS_DURABILITY_SW_CRASH_SAFE,
     ZS_DURABILITY_HW_CRASH_SAFE*/};

    if((fp = fopen("ZS_OpenContainer.log", "w+")) == 0){
        fprintf(stderr, " open failed!.\n");
        return -1;
    }
    
    if( 1 != preEnvironment())
        return 0;

    fprintf(fp, "************Begin to test ***************\n");
     for(int i = 0; i < 2; i++){
        p.async_writes = async_write[i];
        for(int j = 0; j< 3 ; j++){
            testCount = 0;
            SetProps(1024*1024,eviction[j],persistent[j],fifo[j],writethru[j],durability[j]);
            result[i][j][0] = ZSOpenContainer_basic_check1();
            result[i][j][1] = ZSOpenContainer_basic_check_size();
            result[i][j][2] = ZSOpenContainer_openCloseMore1(5);
            result[i][j][3] = ZSOpenContainer_openCloseMore2(5);
            result[i][j][4] = ZSOpenContainer_openCloseMore_createObj(5);
            result[i][j][5] = ZSOpenContainer_openMore(5);
            result[i][j][6] = ZSOpenContainer_createDeletedMore(5);
            result[i][j][7] = ZSOpenContainer_createMore(2);
            result[i][j][8] = ZSOpenContainer_invalid_cguid();
            result[i][j][9] = ZSOpenContainer_invalid_flags();
            result[i][j][10] = ZSOpenContainer_flags_check();
            result[i][j][11] = ZSOpenContainer_invalid_cname1();
            result[i][j][12] = ZSOpenContainer_invalid_cname2();
            result[i][j][13] = ZSOpenContainer_invalid_cname3();
            result[i][j][14] = ZSOpenContainer_invalid_props();
       }
    }

    CleanEnvironment();
     
    for(int j = 0; j < 2; j++){
        for(int k =0; k < 3; k++){
            fprintf(stderr, "when async_write = %d.\n",async_write[j]);
            fprintf(stderr, "test mode:eviction=%d,persistent=%d,fifo=%d,durability=%d.\n",eviction[k],persistent[k],fifo[k],k+1);
            for(int i = 0; i < 15; i++){
                if(result[j][k][i] == 1){
                    fprintf(stderr, "ZSOpenContainer test %drd success.\n",i+1);
                    num++;
                }
                else if(result[j][k][i] == -1)
                    fprintf(stderr, "ZSOpenContainer test %drd fail to test.\n",i+1);
                else if(result[j][k][i] == 0)
                    fprintf(stderr, "ZSOpenContainer test %drd failed.\n",i+1);
                else fprintf(stderr, "ZSOpenContainer test %drd hit wrong.\n",i+1);
           }
        }
    }
    
    if(totalNum == num){
        fprintf(stderr,"************ test pass!******************\n");
	fprintf(stderr, "#The related test script is ZS_OpenContainer.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_OpenContainer.log\n");
        return 0;
    }
    else 
        fprintf(stderr, "************%d test fail!******************\n",totalNum-num);
	fprintf(stderr, "#The related test script is ZS_OpenContainer.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_OpenContainer.log\n");
        return -1;
}



