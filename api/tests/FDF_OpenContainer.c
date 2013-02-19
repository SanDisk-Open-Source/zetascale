/*********************************************
**********   Author:  Lisa

**********   Function: FDFOpenContainer
***********************************************/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "fdf.h"
static struct FDF_state     *fdf_state;
struct FDF_thread_state     *_fdf_thd_state;
FDF_container_props_t       p;
const int                   MAX_COUNT = 128;
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
               FDF_boolean_t writethru,FDF_durability_level_t durability)
{
    p.durability_level = durability;
    p.size_kb = size;
    p.persistent = persistence;
    p.fifo_mode = fifo;
    p.evicting = evicting;
    p.writethru = writethru;
}

FDF_status_t SetContainerProps(FDF_cguid_t cguid,FDF_boolean_t evicting,FDF_boolean_t persistent,FDF_boolean_t fifo,FDF_boolean_t async)
{
    FDF_status_t           ret;
    FDF_container_props_t  props_set;
    ret = FDFGetContainerProps(_fdf_thd_state,cguid,&props_set);
    if(FDF_SUCCESS != ret)
    return ret;

    props_set.persistent = persistent;
    props_set.fifo_mode = fifo;
    props_set.evicting = evicting;
    props_set.async_writes = async;

    fprintf(fp,"Props persistet=%d,fifo=%d,evict=%d,async=%d.\n",persistent,fifo,evicting,async);
    ret = FDFSetContainerProps(_fdf_thd_state,cguid,&props_set);
    if(FDF_SUCCESS == ret){
    fprintf(fp,"FDFSetContainerProps success .\n");
    }
    else fprintf(fp,"FDFSetContainerProps fail:.%s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t OpenContainer(char *cname,FDF_container_props_t *props,uint32_t flags,FDF_cguid_t *cguid)
{
    FDF_status_t           ret;
    ret = FDFOpenContainer(_fdf_thd_state,cname,props,flags, cguid);
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFOpenContainer cguid=%ld cname=%s,flags=%d success.\n",*cguid,cname,flags);
    }
    else fprintf(fp, "FDFOpenContainer %s fail:%s.\n",cname,FDFStrError(ret));
    return ret;
}


FDF_status_t CloseContainer(FDF_cguid_t cguid)
{
    FDF_status_t           ret;
    ret = FDFCloseContainer(_fdf_thd_state, cguid );
    if(FDF_SUCCESS == ret){
        fprintf(fp,"FDFCloseContainer success cguid=%ld.\n",cguid);
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
        fprintf(fp," FDFWriteObject cguid=%ld,key=%s,data=%s success.\n",cguid,key,data);
    }
    else fprintf(fp," FDFWriteObject cguid=%ld,key=%s,data=%s failed:%s.\n",cguid,key,data,FDFStrError(ret));
    sleep(5);
    return ret;
}


FDF_status_t DeleteObject(FDF_cguid_t cguid,char *key,uint32_t keylen)
{
    FDF_status_t           ret;
    ret = FDFDeleteObject(_fdf_thd_state,cguid,key,keylen);
    if(FDF_SUCCESS == ret){
        fprintf(fp," FDFDeleteObject cguid=%ld,key=%s success.\n",cguid,key);
    }
    else fprintf(fp," FDFDeleteObject cguid=%ld,key=%s failed:%s.\n",cguid,key,FDFStrError(ret));
    return ret;
}


int FDFOpenContainer_basic_check1()
{

    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;

    fprintf(fp,"test %d:\n" ,++testCount);
    ret = OpenContainer("t",&p,FDF_CTNR_CREATE,&cguid);
    
    if(FDF_SUCCESS == ret){
        ret = OpenContainer("t",&p,FDF_CTNR_RO_MODE,&cguid);
    }
    else return -1;

    if(FDF_SUCCESS == ret){
        ret = OpenContainer("t",&p,FDF_CTNR_RW_MODE,&cguid);
    }
    CloseContainer(cguid );
    if(FDF_SUCCESS != DeleteContainer(cguid))
        return -2;
    
    if(FDF_SUCCESS == ret)
        return 1;
    return 0;
}


/*int FDFOpenContainer_basic_check2()
{

    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;
      
    fprintf(fp,"test %d:\n",++testCount);
    if(p.evicting == 1)
        p.writethru = 0;
    ret = OpenContainer("test",&p,FDF_CTNR_CREATE,&cguid);
    
    if(FDF_SUCCESS == ret){
        ret = OpenContainer("test",&p,FDF_CTNR_RW_MODE,&cguid);
    }
    else return -1;
    if(FDF_SUCCESS == ret){
        ret = OpenContainer("test",&p,FDF_CTNR_RO_MODE,&cguid);
    }
    CloseContainer(cguid );
    if(FDF_SUCCESS != DeleteContainer(cguid))
        return -2;

    p.writethru = 1;
    if(FDF_SUCCESS == ret)
        return 1;
    return 0;
}
*/


int FDFOpenContainer_basic_check_size()
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;

    fprintf(fp,"test %d:\n",++testCount);
    p.size_kb = 0;
    ret = OpenContainer("test",&p,FDF_CTNR_CREATE,&cguid);
    p.size_kb = 3;   

    if(FDF_SUCCESS == ret){
        fprintf(fp,"set size = 0 Create container success.\n");
        CloseContainer(cguid );
        if(FDF_SUCCESS != DeleteContainer(cguid))
            return -2;
        return 1;
    }
    else{
        fprintf(fp,"set size = 0 Create container failed.\n");
        return 0;
    }
}

int FDFOpenContainer_openCloseMore1(int count)
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;
    int                    i;
    uint32_t               flags[2]={FDF_CTNR_RO_MODE,FDF_CTNR_RW_MODE};

    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("test6",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;

    CloseContainer(cguid);
    for(i = 0;i < count-1; i++){
        ret = OpenContainer("test6",&p,flags[i%2],&cguid);
        CloseContainer(cguid);
        if(FDF_SUCCESS != ret)
            break;
    }
    if(FDF_SUCCESS == ret){
        fprintf(fp,"OpenContainer OpenCloseMore success.\n");
        flag = 1;
    }
    else{
        fprintf(fp,"OpenContainer OpenCloseMore fail:%s.\n",FDFStrError(ret));
        flag = 0;
    }

    if(FDF_SUCCESS != DeleteContainer(cguid))
        return -2;
    return flag;
}


int FDFOpenContainer_openCloseMore2(int count)
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;
    FDF_boolean_t          eviction[] = {0,0};
    FDF_boolean_t          persistence[] = {1,1};
    FDF_boolean_t          fifo[] = {0,0};
    int                    i;
    uint32_t               flags[2]={FDF_CTNR_RO_MODE,FDF_CTNR_RW_MODE};

    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("test6",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;

    for(i = 0; i < count;i++){
        ret = OpenContainer("test6",&p,flags[i%2],&cguid);
        SetContainerProps(cguid,eviction[i%2],persistence[i%2],fifo[i%2],i%2);
        CloseContainer(cguid);
        
        if(FDF_SUCCESS != ret)
            break;
    }

    if(FDF_SUCCESS == ret){
        fprintf(fp,"OpenContainer OpenCloseMore success.\n");
        flag = 1;
    }
    else{
        fprintf(fp,"OpenContainer OpenCloseMore fail:%s.\n",FDFStrError(ret));
        flag = 0;
    }
    if(FDF_SUCCESS != DeleteContainer(cguid))
        return -2;
    return flag;
}

int FDFOpenContainer_openCloseMore_createObj(int count)
{
    FDF_status_t           ret,ret_obj;
    FDF_cguid_t            cguid;
    int                    flag;
    char                   key[6]="key_a";
    uint32_t               flags[2]={FDF_CTNR_RO_MODE,FDF_CTNR_RW_MODE};

    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("test6",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;

    ret_obj = CreateObject(cguid,key,6,"data",5);
    CloseContainer(cguid);

    for(int i = 0;i < count; i++){
        key[4]++;
        ret = OpenContainer("test6",&p,flags[i%2],&cguid);
        ret_obj = CreateObject(cguid,key,6,"data",5);
        CloseContainer(cguid);
        if(FDF_SUCCESS != ret || FDF_SUCCESS != ret_obj)
            break;
    }

    if(FDF_SUCCESS == ret && FDF_SUCCESS == ret_obj){
        fprintf(fp,"OpenContainer OpenCloseMore to create obj  success.\n");
        flag = 1;
    }
    else if(FDF_SUCCESS != ret_obj){
        fprintf(fp,"OpenContainer OpenCloseMore to create obj  failed:%s.\n",FDFStrError(ret));
        flag = -2;
    }
    else{
        fprintf(fp,"OpenContainer OpenCloseMore_obj fail:%s.\n",FDFStrError(ret));
        flag = 0;
    }
    ret = OpenContainer("test6",&p,flags[1],&cguid);
    if(FDF_SUCCESS != ret)
        return -1;
    for(int i = count;i >=0; i--){
        if(FDF_SUCCESS != DeleteObject(cguid,key,6))
            flag = -2;
        key[4]--;
    }
    CloseContainer(cguid);
    if(FDF_SUCCESS != DeleteContainer(cguid))
        flag = -2;
    return flag;
}

int FDFOpenContainer_openMore(int count)
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;
    uint32_t               flags[2]={FDF_CTNR_RO_MODE,FDF_CTNR_RW_MODE};

    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("test7",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;

    for(int i = 0;i < count; i++){
        ret = OpenContainer("test7",&p,flags[i%2],&cguid);
        if(FDF_SUCCESS != ret)
            break;
    }

    if(FDF_SUCCESS == ret){
        fprintf(fp,"OpenContainer OpenMore success.\n");
        flag = 1;
    }
    else{
        fprintf(fp,"OpenContainer OpenMore fail:%s.\n",FDFStrError(ret));
        flag = 0;
    }

    CloseContainer(cguid);
    if(FDF_SUCCESS != DeleteContainer(cguid))
        flag = -2;
    return flag;
}

int FDFOpenContainer_createDeletedMore(int count)
{
    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid;

    fprintf(fp,"test %d:\n",++testCount);
    for(int i =0 ;i < count;i++){
        ret = OpenContainer("t",&p,FDF_CTNR_CREATE,&cguid);
        CloseContainer(cguid );
        DeleteContainer(cguid);

        if(FDF_SUCCESS != ret){
            fprintf(fp,"OpenContainer %drd create deleted container failed:%s.\n",count,FDFStrError(ret));
            return 0;
        }
    }

    fprintf(fp,"OpenContainer  create deleted %d times success.\n",count);
    return 1;
}

int FDFOpenContainer_createMore(int count)
{

    FDF_status_t           ret = FDF_SUCCESS;
    FDF_cguid_t            cguid[MAX_COUNT];
    char                   cname[13] = "cntr_test_a";
    int                    flag;

    fprintf(fp,"test %d:\n",++testCount);
    for(int i = 0;i< count;i++)
    {
        ret = OpenContainer(cname,&p,FDF_CTNR_CREATE,&cguid[i]);
        if(FDF_SUCCESS != ret){
            flag = -1;
            for(int j = i-1;j>=0;j--){
                CloseContainer(cguid[j] );
                if(FDF_SUCCESS != DeleteContainer(cguid[j]))
                    flag = -2;
            }
            return flag;
        }
        cname[9]++;
    }

    if(FDF_SUCCESS == ret){
        flag = 1;
        fprintf(fp,"FDFOpenContainer create %d containers success:\n",count);
    }
    else{
        flag = 0;
        fprintf(fp,"FDFOpenContainer create %d containers failed:%s\n",count,FDFStrError(ret));
    }
    for(int i = 0;i< count;i++){
        CloseContainer(cguid[i] );
        if(FDF_SUCCESS != DeleteContainer(cguid[i]))
            flag = -2;
    }

    return flag;
}

int FDFOpenContainer_invalid_cguid()
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    FDF_cguid_t            cguid_invalid;
    int                    flag;
    
    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("test8",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;
    
    ret = OpenContainer("test8",&p,FDF_CTNR_RW_MODE,&cguid_invalid);
    if(FDF_SUCCESS != ret){
        fprintf(fp,"Open created Container invalid cguid fail:%s.\n",FDFStrError(ret));
        flag = 0;
    }
    else{
        fprintf(fp,"Open created Container invalid cguid success.\n");
        flag = 1;
    }
    CloseContainer(cguid );
    if(FDF_SUCCESS != DeleteContainer(cguid))
        flag = -2;
    
    return flag;
}


int FDFOpenContainer_invalid_flags()
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;

    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer("test10",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;

    ret = OpenContainer("test10",&p,0,&cguid);    
    if(FDF_SUCCESS == ret){
        fprintf(fp,"Open Created Container invalid mode SUCCESS.\n");
        flag = 1;
    }
    else{
        fprintf(fp,"Open Created Container invalid mode fail:%s.\n",FDFStrError(ret));
        flag = 0;
    }

    CloseContainer(cguid);
    if(FDF_SUCCESS != DeleteContainer(cguid))
        flag = -2;
    return flag;
}


int FDFOpenContainer_flags_check()
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid,cguid_tmp;
    int                    flag = 1;
    uint32_t               flags[2]={FDF_CTNR_RO_MODE,FDF_CTNR_RW_MODE};

    fprintf(fp,"test %d:\n",++testCount); 
    for(int i = 0;i < 2;i++){
        ret = OpenContainer("test11",&p,flags[i],&cguid);
        if(FDF_SUCCESS != ret){
            fprintf(fp,"FDFOpenContainer create one mode=%d Fail:%s.\n",flags[i],FDFStrError(ret));
            if(flag==1)flag = 1;
        }
        else{
            flag = 0;
            fprintf(fp,"FDFOpenContainer create one mode=%d success.\n",flags[i]);
            CloseContainer(cguid);
            DeleteContainer(cguid);
        }
    }

    ret = OpenContainer("test11",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret)
        return -1;

    ret = OpenContainer("test11",&p,FDF_CTNR_CREATE,&cguid_tmp);    
    
    if(FDF_SUCCESS != ret){
        fprintf(fp,"FDFOpenContainer create twice failed:%s.\n",FDFStrError(ret));
        if(flag==1)flag = 1;
    }
    else{
        flag = 0;
        CloseContainer(cguid_tmp);
        if(FDF_SUCCESS != DeleteContainer(cguid_tmp))
            flag = -2;
        fprintf(fp,"FDFOpenContainer create twice success.\n");
    }

    CloseContainer(cguid);
    if(FDF_SUCCESS != DeleteContainer(cguid))
        flag = -2;
    
    return flag;
}


int FDFOpenContainer_invalid_cname1()
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;
    char                   cname[1];
    cname[0] = '\0';
    
    fprintf(fp,"test %d:\n",++testCount);
    ret = OpenContainer(cname,&p,FDF_CTNR_CREATE,&cguid);

    if(FDF_SUCCESS != ret){
        fprintf(fp,"OpenContainer invalid cname fail:%s.\n",FDFStrError(ret));
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

int FDFOpenContainer_invalid_cname2()
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;
    
    fprintf(fp,"test %d:\n",++testCount);
    ret = FDFOpenContainer(_fdf_thd_state,NULL,&p,FDF_CTNR_CREATE,&cguid);

    if(FDF_SUCCESS != ret){
        fprintf(fp,"OpenContainer invalid cname failed:%s.\n",FDFStrError(ret));
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


int FDFOpenContainer_invalid_cname3()
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid,cguid_tmp;
    int                    flag;

    fprintf(fp,"test %d:\n",++testCount);
    ret = FDFOpenContainer(_fdf_thd_state,"test",&p,FDF_CTNR_CREATE,&cguid);
    if(FDF_SUCCESS != ret){
        return -1;
    }

    cguid_tmp = cguid;
    ret = FDFOpenContainer(_fdf_thd_state,"x",&p,FDF_CTNR_RW_MODE,&cguid_tmp);
    if(FDF_SUCCESS != ret){
        fprintf(fp,"OpenContainer invalid cname fail:%s.\n",FDFStrError(ret));
        flag = 1;
    }
    else{
        fprintf(fp,"OpenContainer invalid cname success.\n");
        flag = 0;
    }
    CloseContainer(cguid);
    if(FDF_SUCCESS != DeleteContainer(cguid))
        flag = -2;
    return flag;
}

int FDFOpenContainer_invalid_props()
{
    FDF_status_t           ret;
    FDF_cguid_t            cguid;
    int                    flag;
    
    fprintf(fp,"test %d:\n",++testCount);
    p.writethru = 0;
    p.evicting = 0;
    ret = OpenContainer("test12",&p,FDF_CTNR_CREATE,&cguid);

	// We set writethru=1 and evicting=0 in create so this open will pass
    if(FDF_SUCCESS == ret){
        fprintf(fp,"OpenContainer invalid props fail:%s.\n",FDFStrError(ret));
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
    FDF_boolean_t eviction[3] = {0,0,0};
    FDF_boolean_t persistent[3] = {1,1,1};
    FDF_boolean_t fifo[3] = {0,0,0};
    FDF_boolean_t writethru[3]={1,1,1};
    FDF_boolean_t async_write[2]={0,1};
    FDF_durability_level_t durability[3] = {0,1,2/*FDF_DURABILITY_PERIODIC,FDF_DURABILITY_SW_CRASH_SAFE,
     FDF_DURABILITY_HW_CRASH_SAFE*/};

    if((fp = fopen("FDF_OpenContainer.log", "w+")) == 0){
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
            result[i][j][0] = FDFOpenContainer_basic_check1();
            result[i][j][1] = FDFOpenContainer_basic_check_size();
            result[i][j][2] = FDFOpenContainer_openCloseMore1(5);
            result[i][j][3] = FDFOpenContainer_openCloseMore2(5);
            result[i][j][4] = FDFOpenContainer_openCloseMore_createObj(5);
            result[i][j][5] = FDFOpenContainer_openMore(5);
            result[i][j][6] = FDFOpenContainer_createDeletedMore(5);
            result[i][j][7] = FDFOpenContainer_createMore(2);
            result[i][j][8] = FDFOpenContainer_invalid_cguid();
            result[i][j][9] = FDFOpenContainer_invalid_flags();
            result[i][j][10] = FDFOpenContainer_flags_check();
            result[i][j][11] = FDFOpenContainer_invalid_cname1();
            result[i][j][12] = FDFOpenContainer_invalid_cname2();
            result[i][j][13] = FDFOpenContainer_invalid_cname3();
            result[i][j][14] = FDFOpenContainer_invalid_props();
       }
    }

    CleanEnvironment();
     
    for(int j = 0; j < 2; j++){
        for(int k =0; k < 3; k++){
            fprintf(stderr, "when async_write = %d.\n",async_write[j]);
            fprintf(stderr, "test mode:eviction=%d,persistent=%d,fifo=%d,durability=%d.\n",eviction[k],persistent[k],fifo[k],k+1);
            for(int i = 0; i < 15; i++){
                if(result[j][k][i] == 1){
                    fprintf(stderr, "FDFOpenContainer test %drd success.\n",i+1);
                    num++;
                }
                else if(result[j][k][i] == -1)
                    fprintf(stderr, "FDFOpenContainer test %drd fail to test.\n",i+1);
                else if(result[j][k][i] == 0)
                    fprintf(stderr, "FDFOpenContainer test %drd failed.\n",i+1);
                else fprintf(stderr, "FDFOpenContainer test %drd hit wrong.\n",i+1);
           }
        }
    }
    
    if(totalNum == num){
        fprintf(stderr,"************ test pass!******************\n");
	fprintf(stderr, "#The related test script is FDF_OpenContainer.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_OpenContainer.log\n");
        return 0;
    }
    else 
        fprintf(stderr, "************%d test fail!******************\n",totalNum-num);
	fprintf(stderr, "#The related test script is FDF_OpenContainer.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_OpenContainer.log\n");
        return -1;
}



