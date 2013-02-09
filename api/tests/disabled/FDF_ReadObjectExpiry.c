/****************************
#function : FDFReadObjectExpiry
#author   : RoyGao
#date     : 2013.1.28
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "fdf.h"

static FILE *fp;
static struct FDF_state        *fdf_state;
static struct FDF_thread_state *fdf_thrd_state;
FDF_cguid_t                    cguid;
char* testname[10] = {NULL};
int result[2][3][3];

FDF_status_t pre_env()
{
    FDF_status_t ret;
   
    ret = FDFInit(&fdf_state);
    if(ret != FDF_SUCCESS)
    {
        fprintf(fp, "FDF initialization failed!\n");
    }else{
        fprintf(fp, "FDF initialization succeed!\n");
        ret = FDFInitPerThreadState(fdf_state, &fdf_thrd_state);
        if( FDF_SUCCESS == ret)
        {
            fprintf(fp, "FDF thread initialization succeed!\n");
        }else{
            fprintf(fp, "FDF thread initialization fail!\n");
            }
    }
    return ret;
}

void clear_env()
{
    (void)FDFReleasePerThreadState(&fdf_thrd_state);
    (void)FDFShutdown(fdf_state);
    fprintf(stderr,"clear env!\n");
}

FDF_status_t opencontainer(char *cname, uint32_t flag, uint32_t asyncwrite, uint32_t dura)
{
    FDF_status_t          ret;
    FDF_container_props_t p;

    ret = FDF_FAILURE;        
    (void)FDFLoadCntrPropDefaults(&p);
    p.async_writes = asyncwrite;
    p.durability_level = dura;
    p.fifo_mode = 0;
    p.persistent = 1;
    p.writethru = 1;
    p.size_kb = 1024*1024;
    p.num_shards = 1;
    p.evicting = 0;
 
    ret = FDFOpenContainer(
                        fdf_thrd_state,
                        cname,
                        &p,
                        flag,
                        &cguid
                        );
    fprintf(stderr,"durability type: %d\n",dura);
    fprintf(stderr,"FDFOpenContainer: %s\n",FDFStrError(ret));
    return ret;
}


FDF_status_t CloseContainer(FDF_cguid_t cid)
{
    FDF_status_t ret;
    ret = FDFCloseContainer(fdf_thrd_state, cid);
    fprintf(fp,"FDFCloseContainer : %s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t DeleteContainer(FDF_cguid_t cid)
{
    FDF_status_t ret;
    ret = FDFDeleteContainer(fdf_thrd_state, cid);
    fprintf(fp,"FDFDeleteContainer : %s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t WriteObjectExpiry(FDF_cguid_t cid,FDF_writeobject_t* wobj,uint32_t flags)
{
    FDF_status_t              ret;

    ret = FDFWriteObjectExpiry(fdf_thrd_state, cid, wobj, flags);
    if(ret != FDF_SUCCESS)
    {
        fprintf(fp,"FDFWriteObjectExpiry : %s\n",FDFStrError(ret));
    }
    return ret;
}


FDF_status_t ReadObjectExpiry(FDF_cguid_t cid,FDF_readobject_t* robj)
{
    FDF_status_t              ret;

    ret = FDFReadObjectExpiry(fdf_thrd_state, cid, robj);
    if(ret != FDF_SUCCESS)
    {
        fprintf(fp,"FDFReadObjectExpiry : %s\n",FDFStrError(ret));
    }
    return ret;
}
/****** basic test *******/

int test_basic_check(uint32_t aw)
{
    FDF_status_t ret;
    FDF_writeobject_t wobj;
    FDF_readobject_t robj;
    int tag = 0; 
    testname[0] = "#test1: readobjectexpiry with basic check.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[0]);

    wobj.key = "case0";
    wobj.key_len = 6;
    wobj.data = "512";
    wobj.data_len = 4;
    wobj.current = 100;
    wobj.expiry = 100;
    
    robj.key = "case0";
    robj.key_len = 6;
    robj.current = 250;
    
    for(int i = 0;i < 3; i++)
    {
        if(FDF_SUCCESS == opencontainer("c0", 1, aw, i))
        {  
            if(FDF_SUCCESS == WriteObjectExpiry(cguid,&wobj,1))
            {
                ret = ReadObjectExpiry(cguid,&robj);
                if((FDF_EXPIRED == ret))
                {
                    tag++;
                    result[aw][0][i] = 1;
                }
                else
                free(robj.data);
                robj.data = NULL;
                robj.data_len = 0;
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }

    robj.current = 90;
    for(int i = 0;i < 3; i++)
    {
        if(FDF_SUCCESS == opencontainer("c0", 1, aw, i))
        {  
            if(FDF_SUCCESS == WriteObjectExpiry(cguid,&wobj,1))
            {
                ret = ReadObjectExpiry(cguid,&robj);
                if((FDF_SUCCESS == ret)&&(4 == robj.data_len)&&(!strcmp("512",robj.data)))
                {
                    tag++;
                }
                else
                result[aw][0][i] = 0; 
                free(robj.data);
                robj.data = NULL;
                robj.data_len = 0;
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }
    return (6 == tag);
}

/****** Update Expiry Ops ******/

int test_update_expiry_check(uint32_t aw)
{
    FDF_status_t ret;
    FDF_writeobject_t wobj1,wobj2;
    FDF_readobject_t robj;
    int tag = 0; 
    testname[1] = "#test2: readobjectexpiry with update expiry.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[1]);

    wobj1.key = "case1";
    wobj1.key_len = 6;
    wobj1.data = "512";
    wobj1.data_len = 4;
    wobj1.current = 100;
    wobj1.expiry = 90;
    
    wobj2.key = "case1";
    wobj2.key_len = 6;
    wobj2.data = "1024";
    wobj2.data_len = 5;
    
    robj.key = "case1";
    robj.key_len = 6;
    
    for(int i = 0;i < 3; i++)
    {
        wobj2.current = 80;
        wobj2.expiry = 100;
        robj.current = 90;
        if(FDF_SUCCESS == opencontainer("c0", 1, aw, i))
        {  
            if(FDF_SUCCESS == WriteObjectExpiry(cguid,&wobj1,FDF_WRITE_MUST_NOT_EXIST))
            {
                ret = WriteObjectExpiry(cguid,&wobj2,FDF_WRITE_MUST_EXIST);
                if(FDF_SUCCESS == ret)
                {
                    result[aw][1][i] = 1;
                }
                ret = ReadObjectExpiry(cguid,&robj);
                if(FDF_SUCCESS == ret && robj.expiry == 100)
                {
                    free(robj.data);
                    robj.data = NULL;
                    robj.data_len = 0;
                }
                else
                    result[aw][1][i] = 0; 
                wobj2.current = 110;
                ret = WriteObjectExpiry(cguid,&wobj2,FDF_WRITE_MUST_EXIST);
                
                if(FDF_EXPIRED != ret) 
                {
                    result[aw][1][i] = 0;
                }
                robj.current = 50;
                ret = ReadObjectExpiry(cguid,&robj);
                if(FDF_OBJECT_UNKNOWN == ret)
                {
                    tag++;
                }
                else
                {
                    result[aw][1][i] = 0;
                    free(robj.data);
                }
                robj.data = NULL;
                robj.data_len = 0;
            }   
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }

    return (3 == tag);
}

/****** Multi keys expired ******/

int test_multikeys_expiry_check(uint32_t aw)
{
    FDF_status_t ret;
    FDF_writeobject_t* wobjs;
    FDF_readobject_t* robjs;
    char keyname[15];
    int tag = 0; 
    testname[2] = "#test3: readobjectexpiry with multi keys expired.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[2]);
    
    char value[1024];
    memset(value,'a',1024);
    
    wobjs = (FDF_writeobject_t*)malloc(1024*512*sizeof(FDF_writeobject_t));
    robjs = (FDF_readobject_t*)malloc(1024*512*sizeof(FDF_readobject_t));

    for(int i = 0;i < 3; i++)
    {
        if(FDF_SUCCESS == opencontainer("c0", 1, aw, i))
        {  
            result[aw][2][i] = 1;
            for (long j=0;j < 1024*512;j++)
            {
                wobjs[j].data = value;
                wobjs[j].data_len = 1024;
                wobjs[j].current = 100;
                wobjs[j].expiry = 100;
                sprintf(keyname,"%s%ld","key",j);
                wobjs[j].key = keyname;
                wobjs[j].key_len = strlen(keyname)+1;
                if(FDF_SUCCESS != WriteObjectExpiry(cguid,&(wobjs[j]),FDF_WRITE_MUST_NOT_EXIST))
                    result[aw][2][i] = 0;
            }
            for (long j=0;j < 1024*512;j++)
            {
                sprintf(keyname,"%s%ld","key",j);
                robjs[j].current = 90;
                robjs[j].expiry = 100;
                robjs[j].key = keyname;
                robjs[j].key_len = strlen(keyname)+1;
                ret = ReadObjectExpiry(cguid,&(robjs[j]));
                if(FDF_SUCCESS != ret)
                    result[aw][2][i] = 0;
                else
                {
                    free(robjs[j].data);
                    robjs[j].data = NULL;
                    robjs[j].data_len = 0;
              
               }
            }
            for (long j=0;j < 1024*512;j++)
            {
                sprintf(keyname,"%s%ld","key",j);
                robjs[j].current = 110;
                robjs[j].expiry = 100;
                robjs[j].key = keyname;
                robjs[j].key_len = strlen(keyname)+1;
                ret = ReadObjectExpiry(cguid,&(robjs[j]));
                if(FDF_EXPIRED != ret)
                    result[aw][2][i] = 0;
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
            if(result[aw][2][i] == 1)
                tag++;
        }
    }
    free(wobjs);
    free(robjs);
       
    return (3 == tag);
}
/****** main function ******/

int main() 
{
    int testnumber = 3;
    int count = 0;

    if((fp = fopen("FDF_ReadObjectExpiry.log","w+")) == 0)
    {
        fprintf(stderr, "open log file failed! \n");
        return -1;
    }
    if(FDF_SUCCESS == pre_env())
    {
        for(uint32_t aw = 0; aw < 2; aw++)
        {
            count +=test_basic_check(aw);
            count +=test_update_expiry_check(aw);
            count +=test_multikeys_expiry_check(aw);
        }
        clear_env();
    }
    fclose(fp);
      
    fprintf(stderr, "Test Result:\n");
    for(int aw = 0; aw < 2; aw++)
    {
        if(0 == aw)
        {
            fprintf(stderr, "***** When disable async write: *****\n");
        }
        else
        {
            fprintf(stderr, "***** When enable async write: *****\n");
        }
        
        for(int i = 0; i < testnumber; i++)
        {
        if(NULL != testname[i])
        {
            fprintf(stderr, "%s\n", testname[i]);
            for(int j = 0; j < 3; j++)
            {
                if(result[aw][i][j] == 1)
                {
                    fprintf(stderr, "[durability type = %d] pass\n", j);
                }
                else
                {
                    fprintf(stderr, "[durability type = %d] fail\n", j);
                }
            }
        }
        }
    }
    if(3*2 == count)
    {
        fprintf(stderr, "#Test of FDFReadObjectExpiry pass!\n");
    }else{
        fprintf(stderr, "#Test of FDFReadObjectExpiry fail!\n");
    }

	return (!(3*2 == count));
}
