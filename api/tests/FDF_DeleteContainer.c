/****************************
#function : FDFDeleteContainer
#author   : AliceXu
#date     : 2012.11.07
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "fdf.h"

FILE *fp;
static struct FDF_state        *fdf_state;
static struct FDF_thread_state *fdf_thrd_state;
//FDF_config_t                   *fdf_config;
FDF_cguid_t                    cguid;
char *testname[10] = {NULL};
int result[2][10][3] = {{{0,0,0}}};
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
    fprintf(fp, "clear env\n");
}

FDF_status_t OpenContainer(char *cname, uint32_t flag, uint32_t asyncwrite, uint32_t dura)
{
    FDF_status_t          ret;
    FDF_container_props_t p;

    (void)FDFLoadCntrPropDefaults(&p);
    p.durability_level = dura;
    p.fifo_mode = 0;
    p.persistent = 1;
    p.writethru = 1;
    p.size_kb = 1024*1024;
    p.num_shards = 1;
    p.evicting = 0;
    p.async_writes = asyncwrite;

    ret = FDFOpenContainer(
                        fdf_thrd_state,
                        cname,
                        &p,
                        flag,
                        &cguid
                        );
    fprintf(fp,"FDFOpenContainer : ");
    fprintf(fp,"durability type: %d\n",dura);
    fprintf(fp,"open result: %s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t CloseContainer(FDF_cguid_t cid)
{
    FDF_status_t ret;
    ret = FDFCloseContainer(fdf_thrd_state, cid);
    fprintf(fp,"FDFCloseContainer : ");
    fprintf(fp,"%s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t DeleteContainer(FDF_cguid_t cid)
{
    FDF_status_t ret;
    ret = FDFDeleteContainer(fdf_thrd_state, cid);
    fprintf(fp,"FDFDeleteContainer : ");
    fprintf(fp,"%s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t WriteObject(FDF_cguid_t cid,char *key,uint32_t keylen,char *data,uint64_t datalen,uint32_t flags)
{
    FDF_status_t ret;
    ret = FDFWriteObject(fdf_thrd_state, cid, key, keylen, data, datalen, flags);
    fprintf(fp,"FDFWriteObject : ");
    fprintf(fp,"%s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t ReadObject(FDF_cguid_t cid,char *key,uint32_t keylen,char **data,uint64_t *datalen)
{
    FDF_status_t ret;
    ret = FDFReadObject(fdf_thrd_state,cid,key,keylen,data,datalen);
    fprintf(fp,"FDFReadObject : ");
    fprintf(fp,"%s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t DeleteObject(FDF_cguid_t cid,char *key,uint32_t keylen)
{
    FDF_status_t ret;
    ret = FDFDeleteObject(fdf_thrd_state,cid,key,keylen);
    fprintf(fp,"FDFDeleteObject : ");
    fprintf(fp,"%s\n",FDFStrError(ret));
    return ret;
}

FDF_status_t GetContainers(FDF_cguid_t *cid, uint32_t *n_cguids)
{
    FDF_status_t ret;
    ret = FDFGetContainers(fdf_thrd_state, cid, n_cguids);
    fprintf(fp,"GetContainers : ");
    fprintf(fp,"%s\n",FDFStrError(ret));
    return ret;
}

/***************** test ******************/

int test_delete_with_opencontainer(uint32_t aw)
{
    FDF_status_t   ret = FDF_FAILURE;
    int tag = 0;
    testname[0] = "#test0 : delete with opencontainer";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[0]);
    
    for(int i = 0; i < 3; i++)
    {
        if(FDF_SUCCESS == OpenContainer("c1", 1, aw, i))
         {
            ret = DeleteContainer(cguid);
            if(FDF_SUCCESS == ret)
            {
                tag += 1;
                result[aw][0][i] = 1;
            }
            (void)CloseContainer(cguid);
            (void)DeleteContainer(cguid);
         }
    }
   return (3 == tag);
}

int test_delete_invalid_cguid(uint32_t aw)
{
    FDF_status_t  ret = FDF_FAILURE;
    int tag = 0;
    testname[1] = "#test1 : delete with invalid cguid";
    fprintf(fp,"****i* async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[1]);

    ret = DeleteContainer(2);
    if (FDF_FAILURE_ILLEGAL_CONTAINER_ID == ret)
    {
        tag += 1;
        result[aw][1][0] = 1;
    }
    ret = DeleteContainer(-1);
    if (FDF_FAILURE_ILLEGAL_CONTAINER_ID == ret)
    {
        tag += 1;
        result[aw][1][1] = 1;
    }
    ret = DeleteContainer(0);
    if (FDF_FAILURE_ILLEGAL_CONTAINER_ID == ret)
    {
        tag += 1;
        result[aw][1][2] = 1;
    }
    return (3 == tag);
}

int  test_delete_basiccheck_1(uint32_t aw)
{
    FDF_status_t ret = FDF_FAILURE;
    int tag = 0;
    testname[2] = "#test2 : delete with basic check1";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[2]);
    
    for(int i = 0; i < 3; i++)
    {
        if(FDF_SUCCESS == OpenContainer("c3", 1, aw, i))
        {
            if(FDF_SUCCESS == CloseContainer(cguid))
            {
                ret = DeleteContainer(cguid);
                if (FDF_SUCCESS == ret)
                    {
                        tag += 1;
                        result[aw][2][i] = 1;
                    }
            }
        }
    }
    return (3 == tag);
}

int test_delete_basiccheck_2(uint32_t aw)
{
    FDF_status_t ret = FDF_FAILURE;
    int tag = 0;
    testname[3] = "#test3 : delete with basic check2";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[3]);
    
    for(int i = 0; i < 3; i++)
    {
        if(FDF_SUCCESS == OpenContainer("c4", 1, aw, i))
        {
            (void)WriteObject(cguid, "kkk", 4, "ssss", 5, 1);
            (void)DeleteObject(cguid, "kkk", 4);
            (void)WriteObject(cguid, "qq", 3, "22222", 6, 1);
            (void)WriteObject(cguid, "qq", 3, "22", 3, 2);
            (void)DeleteObject(cguid, "qq", 3);
            (void)CloseContainer(cguid);
            ret = DeleteContainer(cguid);
            if(FDF_SUCCESS == ret)
            {
                tag += 1;
                result[aw][3][i] = 1;
            }
            }
    }
    return (3 == tag);
}

int test_double_delete(uint32_t aw)
{
    FDF_status_t ret = FDF_FAILURE;
    int tag = 0;
    testname[4] = "#test4 : double delete container";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[4]);
    
    for(int i = 0; i < 3; i++)
    {
        if(FDF_SUCCESS == OpenContainer("c5", 1, aw, i))
            {
                (void)CloseContainer(cguid);
                if(FDF_SUCCESS == DeleteContainer(cguid))
                    {
                        ret = DeleteContainer(cguid);
                        if(FDF_FAILURE == ret)
                            {
                                tag += 1;
                                result[aw][4][i] = 1;
                            }
                    }
            }
    }
    return (3 == tag);
}

int test_delete_doubleopen_1(uint32_t aw)
{
    FDF_status_t ret = FDF_FAILURE;
    int tag = 0;
    testname[5] = "#test5 : delete with double open mode = 2";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[5]);
    
    for(int i = 0; i < 3; i++)
    {
        if(FDF_SUCCESS == OpenContainer("c6", 1, aw, i))
        {
            (void)CloseContainer(cguid);
            (void)OpenContainer("c6", 2, aw, i);
            (void)CloseContainer(cguid);
            ret = DeleteContainer(cguid);
            if(FDF_SUCCESS == ret)
            {
                tag += 1;
                result[aw][5][i] = 1;
            }
        }
    }
    return (3 == tag);
}

int test_delete_doubleopen_2(uint32_t aw)
{
    FDF_status_t ret = FDF_FAILURE;
    int tag = 0;
    testname[6] = "#test6 : delete with double open mode = 4";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[6]);
    
    for(int i = 0; i < 3; i++)
    {
        if(FDF_SUCCESS == OpenContainer("c6", 1, aw, i))
        {
            (void)CloseContainer(cguid);
            (void)OpenContainer("c6", 4, aw, i);
            (void)CloseContainer(cguid);
            ret = DeleteContainer(cguid);
            if(FDF_SUCCESS == ret)
            {
                tag += 1;
                result[aw][6][i] = 1;
            }
        }
    }
   return (3 == tag);
}


int test_delete_doubleopen_doubledelete(uint32_t aw)
{
    int tag = 0;
    FDF_status_t ret;

    testname[7] = "#test7 : double open,delete,close,delete";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[7]);

    for(int i = 0; i < 3; i++)
    {
        if(FDF_SUCCESS == OpenContainer("c7", 1, aw, i))
        {
            (void)CloseContainer(cguid);
            if(FDF_SUCCESS == OpenContainer("c7", 2, aw, i))
            {
                if(FDF_SUCCESS == DeleteContainer(cguid))
                {
                    result[aw][7][i] += 1;
                }
                
                ret = CloseContainer(cguid);
                if((FDF_INVALID_PARAMETER == ret) || (FDF_FAILURE_CONTAINER_NOT_FOUND == ret))
                {
                    result[aw][7][i] += 1;
                }
                if(FDF_FAILURE == DeleteContainer(cguid))
                {
                    result[aw][7][i] += 1;
                }
                if(3 == result[aw][7][i])
                {
                    tag += 1;
                    result[aw][7][i] = 1;
                }else{
                    result[aw][7][i] = 0;
                }
            }
         }
    }
    return (3 == tag);
}  

/****** main function ******/

int main() 
{
	/*
	 * Number of tests we plan to run
	 */
    int testnumber = 8;

	int count      = 0;
    
    if((fp = fopen("FDF_DeleteContainer.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!\n");
        return -1;
    }
    

    if(FDF_SUCCESS == pre_env())
    {
        for(uint32_t aw = 0; aw < 2; aw++)
        {
			/*
			 * Make sure that that number of tests to run matches
			 * variable "testnumber" defined a few lines before.
			 */
            count += test_delete_with_opencontainer(aw);
            count += test_delete_invalid_cguid(aw);
            count += test_delete_basiccheck_1(aw);
            count += test_delete_basiccheck_2(aw);
            count += test_double_delete(aw);
            count += test_delete_doubleopen_1(aw);
            count += test_delete_doubleopen_2(aw);
            count += test_delete_doubleopen_doubledelete(aw);
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

		for(int i = 0; i <= testnumber; i++)
		{
			if(NULL != testname[i])
			{
				fprintf(stderr, "%s\n", testname[i]);
				for(int j = 0; j < 3; j++)
				{
					if(result[aw][i][j] == 1)
					{
						fprintf(stderr, "durability type = %d pass\n",j);
					}else {
						fprintf(stderr, "durability type = %d fail\n",j);
					}
				} 
			}
		}
	}
	fprintf(stderr,"count = %d\n",count);

	/*
	 * Test if we have run all tests as planned
	 */
	if ((testnumber * 2) == count)
	{
		fprintf(stderr, "#Test of FDFDeleteContainer pass!\n");
		fprintf(stderr, "#The related test script is FDF_DeleteContainer.c\n");
		fprintf(stderr, "#If you want, you can check test details in FDF_DeleteContainer.log\n");
	} else {
		fprintf(stderr, "#Test of FDFDeleteContainer fail!\n");
		fprintf(stderr, "#The related test script is FDF_DeleteContainer.c\n");
		fprintf(stderr, "#If you want, you can check test details in FDF_DeleteContainer.log\n");
	}

	return (!(testnumber*2 == count));
}
