/****************************
#function : FDFFlushCache
#author   : AliceXu
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "fdf.h"

FILE *fp;
static struct FDF_state        *fdf_state;
static struct FDF_thread_state *fdf_thrd_state;
FDF_cguid_t                    cguid;
char *testname[10] = {NULL};
int result[2][10][3];
//uint32_t mode[6][4] = {{0,0,0,1},{0,1,0,1},{0,1,1,0},{0,1,1,1},{1,0,1,0},{1,0,1,1}};


FDF_status_t pre_env()
{
    FDF_status_t ret = FDF_FAILURE;
    //(void)FDFLoadConfigDefaults(fdf_config, NULL);
    //  if (FDFInit(&fdf_state, fdf_config) != FDF_SUCCESS) {

    ret = FDFInit(&fdf_state);
    if (FDF_SUCCESS != ret)
    {
        fprintf(fp, "FDF initialization failed!\n");
    }else {
        fprintf(fp, "FDF initialization succeed!\n");
        ret = FDFInitPerThreadState(fdf_state, &fdf_thrd_state);
        if( FDF_SUCCESS == ret)
        {
            fprintf(fp, "FDF thread initialization succeed!\n");
        }
    }
    return ret;
}

void clear_env()
{
    (void)FDFReleasePerThreadState(&fdf_thrd_state);
    (void)FDFShutdown(fdf_state);
    fprintf(fp, "clear env!");
}

FDF_status_t OpenContainer(char *cname, uint32_t flag, uint32_t asyncwrite, uint32_t dura)
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
     fprintf(fp,"durability type: %d\n", dura);
     fprintf(fp,"FDFOpenContainer : %s\n", FDFStrError(ret));
     return ret;
}

FDF_status_t CloseContainer(FDF_cguid_t cid)
{
    FDF_status_t ret;
    ret = FDFCloseContainer(fdf_thrd_state, cid);
    fprintf(fp,"FDFCloseContainer : %s\n", FDFStrError(ret));
    return ret;
}

FDF_status_t DeleteContainer(FDF_cguid_t cid)
{
    FDF_status_t ret;
    ret = FDFDeleteContainer(fdf_thrd_state, cid);
    fprintf(fp, "FDFDeleteContainer : %s\n", FDFStrError(ret));
    return ret;
}

FDF_status_t WriteObject(FDF_cguid_t cid,char *key,uint32_t keylen,char *data,uint64_t datalen,uint32_t flags)
{
    FDF_status_t ret;
    ret = FDFWriteObject(fdf_thrd_state, cid, key, keylen, data, datalen, flags);
    fprintf(fp,"FDFWriteObject : %s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t ReadObject(FDF_cguid_t cid,char *key,uint32_t keylen,char **data,uint64_t *datalen)
{
    FDF_status_t ret;
    ret = FDFReadObject(fdf_thrd_state,cid,key,keylen,data,datalen);
    fprintf(fp,"FDFReadObject : %s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t DeleteObject(FDF_cguid_t cid,char *key,uint32_t keylen)
{
    FDF_status_t ret;
    ret = FDFDeleteObject(fdf_thrd_state,cid,key,keylen);
    fprintf(fp,"FDFDeleteObject : %s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t FlushCache()
{
    FDF_status_t ret;
    ret = FDFFlushCache(fdf_thrd_state);
    fprintf(fp,"FDFFlushCache : %s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t SetContainerProps(FDF_cguid_t cid,FDF_container_props_t *pprops)
{
    FDF_status_t ret;
    ret = FDFSetContainerProps(fdf_thrd_state,cid,pprops);
    fprintf(fp,"FDFSetContainerProps : %s\n",FDFStrError(ret));
    return ret;
}

/***************** test ******************/
int test_basic_check_0(uint32_t aw)
{
    FDF_status_t ret;
    int tag = 0;
    testname[0] = "#test0:flush cache with none containers in thread.";
    fprintf(fp,"******async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[0]);

    ret = FlushCache();
    if(FDF_SUCCESS == ret)
    {
        tag += 1;
        result[aw][0][0] = 1;
        result[aw][0][1] = 1;
        result[aw][0][2] = 1;
    }

    return (1 == tag);;
}

int test_basic_check_1(uint32_t aw)
{
    FDF_status_t ret;
    int tag = 0;
    testname[1] = "#test1:open c 1/close c/flush cache/ open c 2/flush cache.";
    fprintf(fp,"******async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[1]);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test1", 1, aw, i);
        if(FDF_SUCCESS == ret)
        {
            ret = CloseContainer(cguid);
            if(FDF_SUCCESS == ret)
            {
                ret = FlushCache();
                if(FDF_FAILURE == ret)
                {
                    result[aw][1][i] += 1;
                }
                ret = OpenContainer("test1", 2, aw, i);
                if(FDF_SUCCESS == ret)
                {
                    ret = FlushCache();
                    if(FDF_SUCCESS == ret)
                    {
                        result[aw][1][i] += 1;
                    }
                }
            }
        (void)CloseContainer(cguid);
        (void)DeleteContainer(cguid);
        }
        if(2 == result[aw][1][i])
        {
            result[aw][1][i] = 1;
            tag += 1;
        }else{
            result[aw][1][i] = 0;
        }
    }
    return (3 == tag);
}

int test_basic_check_2(uint32_t aw)
{
    FDF_status_t ret;
    int tag = 0;
    testname[2] = "#test2:open c 1/close c/flush cache/ open c 4/flush cache.";
    fprintf(fp,"******async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[2]);
    
    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test2", 1, aw, i);
        if(FDF_SUCCESS == ret)
        {
            ret = CloseContainer(cguid);
            (void)OpenContainer("test2", 4, aw, i);
            (void)CloseContainer(cguid);
            if(FDF_SUCCESS == ret)
            {
                ret = FlushCache();
                if(FDF_FAILURE == ret)
                {
                    result[aw][2][i] += 1;
                }
                ret = OpenContainer("test1", 4, aw, i);
                if(FDF_SUCCESS == ret)
                {
                    ret = FlushCache();
                    if(FDF_SUCCESS == ret)
                    {
                        result[aw][2][i] += 1;
                    }
                }
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
        if(2 == result[aw][2][i])
        {
            result[aw][2][i] = 1;
            tag += 1;
        }else{
            result[aw][2][i] = 0;
        }
    }
    return (3 == tag);
}
 
int test_basic_check_3(uint32_t aw)
{
    FDF_status_t ret;
    int tag = 0;
    testname[3] = "#test3:open c/write o/flush cache.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[3]);

    char         *data;
    uint64_t     datalen;
    data = (char *)malloc(1*1024*1024*sizeof(char));
    datalen = 1*1024*1024*sizeof(char);
    memset(data,'a',datalen);

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test3", 1, aw, i);
        if(FDF_SUCCESS == ret)
        {
            ret = WriteObject(cguid, "xxxx", 5, data, datalen,1);
            if(FDF_SUCCESS == ret)
            {
                ret = FlushCache();
                if(FDF_SUCCESS == ret)
                {
                    result[aw][3][i] = 1;
                    tag += 1;
                }
                (void)DeleteObject(cguid, "xxxx", 5);
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }
    free(data);
    return (3 == tag);
}

int test_basic_check_4(uint32_t aw)
{
    FDF_status_t           ret;
    int tag = 0;
    testname[4] = "#test4: open c/write o/flush cache/set c props/flush cache.";
    fprintf(fp, "****** async write = %d ******\n",aw);
    fprintf(fp, "%s\n",testname[4]);

    FDF_container_props_t  props;
    props.size_kb = 2*1024*1024;
    props.persistent = FDF_FALSE;
    props.evicting = FDF_FALSE;
    props.fifo_mode = FDF_FALSE;

    for(int i = 0; i < 3; i++)
    {
        ret = OpenContainer("test4", 1, aw, i);
        if(FDF_SUCCESS == ret)
        {
            (void)WriteObject(cguid, "xxxx", 5, "pppp", 5, 1);
            (void)WriteObject(cguid, "yyyy", 5, "qqqq", 5, 1);
            ret = FlushCache();
            if(FDF_SUCCESS == ret)
            {
                result[aw][4][i] += 1;
            }
        }
        ret = SetContainerProps(cguid, &props);
        if(FDF_SUCCESS == ret)
        {
            ret = FlushCache();
            if(FDF_SUCCESS == ret)
            {
                result[aw][4][i] += 1;
            }
       }
       (void)DeleteObject(cguid, "xxxx", 5);
       (void)DeleteObject(cguid, "yyyy", 5);
       (void)CloseContainer(cguid);
       (void)DeleteContainer(cguid);

       if(2 == result[aw][4][i])
       {
           result[aw][4][i] = 1;
           tag += 1;
       }else{
           result[aw][4][i] = 0;
       }
    }
    return (3 == tag);
}


/****** main function ******/
int main() 
{
    int testnumber = 5;
    int count      = 0;
    
    if((fp = fopen("FDF_FlushCache.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!.\n");
        return -1;
    }
   if(FDF_SUCCESS == pre_env())
    {
        for(uint32_t aw = 0; aw < 2; aw++)
        {
            count += test_basic_check_0(aw);
//           count += test_basic_check_1(aw);
//10450      count += test_basic_check_2(aw);
            count += test_basic_check_3(aw);
            count += test_basic_check_4(aw);
        }
        clear_env();
    }
    fclose(fp);

    fprintf(stderr, "Test Result:\n");
    for(int aw = 0; aw < 2; aw++)
    {
        if(0 == aw)
        {
            fprintf(stderr, "# When disable async write:\n");
        }else{
            fprintf(stderr, "# When enable async write:\n");
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
                }else{
                    fprintf(stderr, "[durability type = %d] fail\n", j);
                }
                }
            }
        }
    }

    if(2*3 == count)
    {
        fprintf(stderr, "#Test of FDFFlushCache pass!\n");
	fprintf(stderr, "#The related test script is FDF_FlushCache.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_FlushCache.log\n");
    }else { 
        fprintf(stderr, "#Test of FDFFlushCache fail!\n");
	fprintf(stderr, "#The related test script is FDF_FlushCache.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_FlushCache.log\n");
    }

    return (!(2*3 == count));
}
