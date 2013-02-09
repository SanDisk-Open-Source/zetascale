/****************************
#function : FDFReadObject
#author   : AliceXu
#date     : 2012.11.10
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "fdf.h"

FILE *fp;
static struct FDF_state        *fdf_state;
static struct FDF_thread_state *fdf_thrd_state;
//FDF_config_t                   *fdf_config;
FDF_cguid_t                    cguid;
char *testname[10] = {NULL};
int result[2][10][3];
//uint32_t mode[6][4] = {{0,0,0,1},{0,1,0,1},{0,1,1,0},{0,1,1,1},{1,0,1,0},{1,0,1,1}};

FDF_status_t pre_env()
{
    FDF_status_t ret;
    // (void)FDFLoadConfigDefaults(fdf_config, NULL);
    //  if(FDFInit(&fdf_state, fdf_config) != FDF_SUCCESS) {
    
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
    fprintf(fp,"clear env!\n");
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
    fprintf(fp,"durability type: %d\n",dura);
    fprintf(fp,"FDFOpenContainer: %s\n",FDFStrError(ret));
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

/***************** test ******************/

int test_invalid_cguid(uint32_t aw)
{
    FDF_status_t ret = FDF_FAILURE;
    int tag = 0;
    testname[0] = "#test0: readobject with invalid cguids.";   
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[0]);

    char *data;
    uint64_t datalen; 
    ret = ReadObject(0, "xxxx", 5, &data, &datalen);
    if(FDF_FAILURE == ret)
    {
        tag += 1;
        result[aw][0][0] = 1;
    }
    ret = ReadObject(1111, "xxxx", 5, &data, &datalen);
    if(FDF_FAILURE == ret)
    {
        tag += 1;
        result[aw][0][1] = 1;
    }
    ret = ReadObject(-1, "xxxx", 5, &data, &datalen);
    if(FDF_FAILURE == ret)
    {
        tag += 1;
        result[aw][0][2] = 1;
    }
    return (3 == tag);
}

int test_invalid_objects(uint32_t aw)
{
    int tag = 0;
    testname[1] = "#test1: readobject with none exist objects.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[1]);

    char *data;
    uint64_t datalen;
    for(int i = 0; i < 3; i++)
    {
        if(FDF_SUCCESS == opencontainer("c1", 1, aw, i))
        {
            if(FDF_OBJECT_UNKNOWN == ReadObject(cguid, "xxxx", 5, &data, &datalen))
            {
                tag += 1;
                result[aw][1][i] = 1;
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
    }
    return (3 == tag);
}

int test_wrong_keykeylen(uint32_t aw)
{
    FDF_status_t ret;
    int tag = 0;
    testname[2] = "#test2: readobject with wrong key & keylen.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[2]);
 
    char *data;
    uint64_t datalen;
    for(int i = 0; i < 3; i++)
    {
        if(FDF_SUCCESS == opencontainer("c2", 1, aw, i))
        {
            if(FDF_SUCCESS == WriteObject(cguid, "xxx", 4, "yyyy", 5, FDF_WRITE_MUST_NOT_EXIST))
            {
                ret = ReadObject(cguid, "xxx", -1, &data, &datalen);
                if(FDF_FAILURE == ret)
                {
                    result[aw][2][i] += 1;
                }
                ret = ReadObject(cguid, "xxx", 0, &data, &datalen);
                if(FDF_FAILURE == ret)
                {
                    result[aw][2][i] += 1;
                }
                ret = ReadObject(cguid, "xxx", 11111, &data, &datalen);
                if(FDF_FAILURE == ret)
                {
                    result[aw][2][i] += 1;
                }
                ret = ReadObject(cguid, "xx", 4, &data, &datalen); 
                if(FDF_FAILURE == ret)
                {
                    result[aw][2][i] += 1;
                }
                ret = ReadObject(cguid, "xxy", 4, &data, &datalen);
                if(FDF_FAILURE == ret)
                {
                    result[aw][2][i] += 1;
                }
/*
                ret = ReadObject(cguid, NULL, 4, &data, &datalen);
                if(FDF_FAILURE == ret)
                {
                    result[aw][2][i] += 1;
                }
  */
                (void)DeleteObject(cguid, "xxx", 4);
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
        if(5 == result[aw][2][i])
        {
            tag += 1;
            result[aw][2][i] = 1;
        }else {
            result[aw][2][i] = 0;
        }
    }
   return (3 == tag);

}

int test_invalid_data_datalen(uint32_t aw)
{
    FDF_status_t ret;
    int tag = 0;
    testname[3] = "#test3: readobject with data & datalen = null.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[3]);
    
    char *data;
    uint64_t datalen;
    for(int i = 0; i < 3; i++)
    {
        if(FDF_SUCCESS == opencontainer("c3", 1, aw, i))
        {
            if(FDF_SUCCESS == WriteObject(cguid, "xxx", 4, "yyyy", 5, FDF_WRITE_MUST_NOT_EXIST))
            {
                ret = ReadObject(cguid, "xxx", 4, NULL, &datalen);
                if(FDF_FAILURE == ret)
                {
                    result[aw][3][i] += 1;
                }
                ret = ReadObject(cguid, "xxx", 4, &data, NULL);
                if(FDF_FAILURE == ret)
                {
                    result[aw][3][i] += 1;
                }
                (void)DeleteObject(cguid, "xxx", 4);
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
        if(2 == result[aw][3][i])
        {
            tag += 1;
            result[aw][3][i] = 1;
        }else{
            result[aw][3][i] = 0;
        }
    }
    return (3 == tag);
}

int test_check_object_change(uint32_t aw)
{
    FDF_status_t ret;
    int tag = 0;
    testname[4] = "#test4: readobject with object change.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[4]);
    char *data1, *data2;
    uint64_t datalen1, datalen2;
    data1 = (char *)malloc(64*1024*sizeof(char));
    datalen1 = strlen(data1);
    data2 = NULL;
    datalen2 = 0;

    for(int i = 0; i < 3; i++)
    {
        if(FDF_SUCCESS == opencontainer("c8", 1, aw, i))
        {
            memset(data1, 'y', datalen1);

            if(FDF_SUCCESS == WriteObject(cguid, "sdhsd", 6, data1, datalen1, 1))
            {
                ret = ReadObject(cguid, "sdhsd", 6, &data2, &datalen2);
                if((FDF_SUCCESS == ret) && (!strcmp(data1, data2)) && (datalen1 == datalen2))
                {
                    result[aw][4][i] += 1;
                }
                FDFFreeBuffer(data2);
                data2 = NULL;
                datalen2 = 0;
                memset(data1, 'z', datalen1);
            if(FDF_SUCCESS == WriteObject(cguid, "sdhsd", 6, data1, datalen1, 2))
            {
                ret = ReadObject(cguid, "sdhsd", 6, &data2, &datalen2);
                if((FDF_SUCCESS == ret) && (!strcmp(data1, data2)) && (datalen1 == datalen2))
                {
                    result[aw][4][i] += 1;
                }
                FDFFreeBuffer(data2);
                data2 = NULL;
                datalen2 = 0;
            }
            (void)DeleteObject(cguid, "sdhsd", 6);
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
        }
        if(2 == result[aw][4][i])
        {
            tag += 1;
            result[aw][4][i] = 1;
        }else {
            result[aw][4][i] = 0;
        }
    }
    free(data1);
    return (3 == tag);
}

int test_basic_check(uint32_t aw)
{
    FDF_status_t ret;
    int tag = 0;
    testname[5] = "#test5: readobject with basic check.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[5]);

    char *data;
    uint64_t datalen;
    
    for(int i = 0; i < 3; i++)
    {
        if(FDF_SUCCESS == opencontainer("c5", 1,aw, i))
        {
            if(FDF_SUCCESS == WriteObject(cguid, "asd", 4, "512", 4, 1))
            {
                ret = ReadObject(cguid, "asd", 4, &data, &datalen);
                if((FDF_SUCCESS == ret)&&(4 == datalen)&&(!strcmp("512",data)))
                {
                    tag += 1;
                    result[aw][5][i] = 1;
                }
                free(data);
                data =NULL;
                datalen = 0;
                (void)DeleteObject(cguid, "asd", 4);
            }
        (void)CloseContainer(cguid);
        (void)DeleteContainer(cguid);
        }
    }
    
    return (3 == tag);
}

/****** main function ******/

int main() 
{
    int testnumber = 6;
    int count      = 0;
    

    if((fp = fopen("FDF_ReadObject.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!.\n");
        return -1;
    }

    if(FDF_SUCCESS == pre_env())
    {
        for(uint32_t aw = 0; aw < 2; aw++)
        {
//          count += test_invalid_cguid(aw);
            count += test_invalid_objects(aw);
//          count += test_wrong_keykeylen(aw);
//          count += test_invalid_data_datalen(aw);
            count += test_check_object_change(aw);
            count += test_basic_check(aw);
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
        }else{
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
            }else{
                fprintf(stderr, "[durability type = %d] fail\n", j);
            }
         }
        }
    }
}
    if(3*2 == count)
    {
        fprintf(stderr, "#Test of FDFReadObject pass!\n");
	fprintf(stderr, "#The related test script is FDF_ReadObject.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_ReadObject.log\n");
    }else{
        fprintf(stderr, "#Test of FDFReadObject fail!\n");
	fprintf(stderr, "#The related test script is FDF_ReadObject.c\n");
	fprintf(stderr, "#If you want, you can check test details in FDF_ReadObject.log\n");
    }

	return (!(3*2 == count));
}
