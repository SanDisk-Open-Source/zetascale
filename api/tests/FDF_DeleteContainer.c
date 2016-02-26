/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/****************************
#function : ZSDeleteContainer
#author   : AliceXu
#date     : 2012.11.07
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "zs.h"

FILE *fp;
static struct ZS_state        *zs_state;
static struct ZS_thread_state *zs_thrd_state;
//ZS_config_t                   *fdf.config;
ZS_cguid_t                    cguid;
char *testname[10] = {NULL};
int result[2][10][3] = {{{0,0,0}}};
//uint32_t mode[6][4] = {{0,0,0,1},{0,1,0,1},{0,1,1,0},{0,1,1,1},{1,0,1,0},{1,0,1,1}};


ZS_status_t pre_env()
{
    ZS_status_t ret = ZS_FAILURE;
    
    //(void)ZSLoadConfigDefaults(fdf.config, NULL);
    //  if (ZSInit(&zs_state, fdf.config) != ZS_SUCCESS) {
    
    ret = ZSInit(&zs_state);
    if (ZS_SUCCESS != ret) 
    {
        fprintf(fp, "ZS initialization failed!\n");
    }else {
        fprintf(fp, "ZS initialization succeed!\n");
        ret = ZSInitPerThreadState(zs_state, &zs_thrd_state);
        if( ZS_SUCCESS == ret)
        {
            fprintf(fp, "ZS thread initialization succeed!\n");
         }
    }
    return ret;
}

void clear_env()
{
    (void)ZSReleasePerThreadState(&zs_thrd_state);
    (void)ZSShutdown(zs_state);
    fprintf(fp, "clear env\n");
}

ZS_status_t OpenContainer(char *cname, uint32_t flag, uint32_t asyncwrite, uint32_t dura)
{
    ZS_status_t          ret;
    ZS_container_props_t p;

    (void)ZSLoadCntrPropDefaults(&p);
    p.durability_level = dura;
    p.fifo_mode = 0;
    p.persistent = 1;
    p.writethru = 1;
    p.size_kb = 1024*1024;
    p.num_shards = 1;
    p.evicting = 0;
    p.async_writes = asyncwrite;

    ret = ZSOpenContainer(
                        zs_thrd_state,
                        cname,
                        &p,
                        flag,
                        &cguid
                        );
    fprintf(fp,"ZSOpenContainer : ");
    fprintf(fp,"durability type: %d\n",dura);
    fprintf(fp,"open result: %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t CloseContainer(ZS_cguid_t cid)
{
    ZS_status_t ret;
    ret = ZSCloseContainer(zs_thrd_state, cid);
    fprintf(fp,"ZSCloseContainer : ");
    fprintf(fp,"%s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t DeleteContainer(ZS_cguid_t cid)
{
    ZS_status_t ret;
    ret = ZSDeleteContainer(zs_thrd_state, cid);
    fprintf(fp,"ZSDeleteContainer : ");
    fprintf(fp,"%s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t WriteObject(ZS_cguid_t cid,char *key,uint32_t keylen,char *data,uint64_t datalen,uint32_t flags)
{
    ZS_status_t ret;
    ret = ZSWriteObject(zs_thrd_state, cid, key, keylen, data, datalen, flags);
    fprintf(fp,"ZSWriteObject : ");
    fprintf(fp,"%s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t ReadObject(ZS_cguid_t cid,char *key,uint32_t keylen,char **data,uint64_t *datalen)
{
    ZS_status_t ret;
    ret = ZSReadObject(zs_thrd_state,cid,key,keylen,data,datalen);
    fprintf(fp,"ZSReadObject : ");
    fprintf(fp,"%s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t DeleteObject(ZS_cguid_t cid,char *key,uint32_t keylen)
{
    ZS_status_t ret;
    ret = ZSDeleteObject(zs_thrd_state,cid,key,keylen);
    fprintf(fp,"ZSDeleteObject : ");
    fprintf(fp,"%s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t GetContainers(ZS_cguid_t *cid, uint32_t *n_cguids)
{
    ZS_status_t ret;
    ret = ZSGetContainers(zs_thrd_state, cid, n_cguids);
    fprintf(fp,"GetContainers : ");
    fprintf(fp,"%s\n",ZSStrError(ret));
    return ret;
}

/***************** test ******************/

int test_delete_with_opencontainer(uint32_t aw)
{
    ZS_status_t   ret = ZS_FAILURE;
    int tag = 0;
    testname[0] = "#test0 : delete with opencontainer";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[0]);
    
    for(int i = 0; i < 3; i++)
    {
        if(ZS_SUCCESS == OpenContainer("c1", 1, aw, i))
         {
            ret = DeleteContainer(cguid);
            if(ZS_SUCCESS == ret)
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
    ZS_status_t  ret = ZS_FAILURE;
    int tag = 0;
    testname[1] = "#test1 : delete with invalid cguid";
    fprintf(fp,"****i* async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[1]);

    ret = DeleteContainer(2);
    if (ZS_FAILURE_ILLEGAL_CONTAINER_ID == ret)
    {
        tag += 1;
        result[aw][1][0] = 1;
    }
    ret = DeleteContainer(-1);
    if (ZS_FAILURE_CONTAINER_NOT_FOUND == ret)
    {
        tag += 1;
        result[aw][1][1] = 1;
    }
    ret = DeleteContainer(0);
    if (ZS_FAILURE_ILLEGAL_CONTAINER_ID == ret)
    {
        tag += 1;
        result[aw][1][2] = 1;
    }
    return (3 == tag);
}

int  test_delete_basiccheck_1(uint32_t aw)
{
    ZS_status_t ret = ZS_FAILURE;
    int tag = 0;
    testname[2] = "#test2 : delete with basic check1";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[2]);
    
    for(int i = 0; i < 3; i++)
    {
        if(ZS_SUCCESS == OpenContainer("c3", 1, aw, i))
        {
            if(ZS_SUCCESS == CloseContainer(cguid))
            {
                ret = DeleteContainer(cguid);
                if (ZS_SUCCESS == ret)
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
    ZS_status_t ret = ZS_FAILURE;
    int tag = 0;
    testname[3] = "#test3 : delete with basic check2";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[3]);
    
    for(int i = 0; i < 3; i++)
    {
        if(ZS_SUCCESS == OpenContainer("c4", 1, aw, i))
        {
            (void)WriteObject(cguid, "kkk", 4, "ssss", 5, 1);
            (void)DeleteObject(cguid, "kkk", 4);
            (void)WriteObject(cguid, "qq", 3, "22222", 6, 1);
            (void)WriteObject(cguid, "qq", 3, "22", 3, 2);
            (void)DeleteObject(cguid, "qq", 3);
            (void)CloseContainer(cguid);
            ret = DeleteContainer(cguid);
            if(ZS_SUCCESS == ret)
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
    ZS_status_t ret = ZS_FAILURE;
    int tag = 0;
    testname[4] = "#test4 : double delete container";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[4]);
    
    for(int i = 0; i < 3; i++)
    {
        if(ZS_SUCCESS == OpenContainer("c5", 1, aw, i))
            {
                (void)CloseContainer(cguid);
                if(ZS_SUCCESS == DeleteContainer(cguid))
                    {
                        ret = DeleteContainer(cguid);
                        if(ZS_SUCCESS != ret)
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
    ZS_status_t ret = ZS_FAILURE;
    int tag = 0;
    testname[5] = "#test5 : delete with double open mode = 2";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[5]);
    
    for(int i = 0; i < 3; i++)
    {
        if(ZS_SUCCESS == OpenContainer("c6", 1, aw, i))
        {
            (void)CloseContainer(cguid);
            (void)OpenContainer("c6", 2, aw, i);
            (void)CloseContainer(cguid);
            ret = DeleteContainer(cguid);
            if(ZS_SUCCESS == ret)
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
    ZS_status_t ret = ZS_FAILURE;
    int tag = 0;
    testname[6] = "#test6 : delete with double open mode = 4";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[6]);
    
    for(int i = 0; i < 3; i++)
    {
        if(ZS_SUCCESS == OpenContainer("c6", 1, aw, i))
        {
            (void)CloseContainer(cguid);
            (void)OpenContainer("c6", 4, aw, i);
            (void)CloseContainer(cguid);
            ret = DeleteContainer(cguid);
            if(ZS_SUCCESS == ret)
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
	ZS_status_t ret = ZS_SUCCESS;

    testname[7] = "#test7 : double open,delete,close,delete";
    fprintf(fp,"****** async write = %d ******\n", aw);
    fprintf(fp,"%s\n",testname[7]);

    for(int i = 0; i < 3; i++)
    {
        if(ZS_SUCCESS == (ret = OpenContainer("c7", 1, aw, i)))
        {
            (void)CloseContainer(cguid);
            if(ZS_SUCCESS == (ret = OpenContainer("c7", 2, aw, i)))
            {
                if(ZS_SUCCESS == (ret = DeleteContainer(cguid)))
                {
                    result[aw][7][i] += 1;
                }

                ret = CloseContainer(cguid);
                if((ZS_INVALID_PARAMETER == ret) || (ZS_FAILURE_CONTAINER_NOT_FOUND == ret))
                {
                    result[aw][7][i] += 1;
                }
                if(ZS_SUCCESS != (ret = DeleteContainer(cguid)))
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
    
    if((fp = fopen("ZS_DeleteContainer.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!\n");
        return -1;
    }
    

    if(ZS_SUCCESS == pre_env())
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
		fprintf(stderr, "#Test of ZSDeleteContainer pass!\n");
		fprintf(stderr, "#The related test script is ZS_DeleteContainer.c\n");
		fprintf(stderr, "#If you want, you can check test details in ZS_DeleteContainer.log\n");
	} else {
		fprintf(stderr, "#Test of ZSDeleteContainer fail!\n");
		fprintf(stderr, "#The related test script is ZS_DeleteContainer.c\n");
		fprintf(stderr, "#If you want, you can check test details in ZS_DeleteContainer.log\n");
	}

	return (!(testnumber*2 == count));
}
