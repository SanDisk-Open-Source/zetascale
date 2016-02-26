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

/********************************
#function : ZSWritebackStoreMode
#author   : AliceXu, BrianO
********************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "zs.h"

FILE *fp;
static struct ZS_state        *zs_state;
static struct ZS_thread_state *zs_thrd_state;
ZS_cguid_t                    cguid;
char *testname[10] = {NULL};
int result[2][10][10] = {{{0}}};
uint32_t mode[10][4] = {
    {0,0,0,0},{0,0,0,1},{0,0,1,0},{0,0,1,1},
    {0,1,0,0},{0,1,0,1},{0,1,1,0},{0,1,1,1},
    {1,0,1,0},{1,0,1,1}};

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
    fprintf(fp,"clear env!\n");
}

ZS_status_t OpenContainer(char *cname, uint32_t flag, uint32_t asyncwrite,
                           uint32_t fifo, uint32_t persist, uint32_t evicting,uint32_t wt)
{
    ZS_status_t          ret;
    ZS_container_props_t p;

    ret = ZS_FAILURE;
    (void)ZSLoadCntrPropDefaults(&p);
    p.flash_only = 0;
    p.async_writes = asyncwrite;
    p.durability_level = 0;
    p.fifo_mode = fifo;
    p.persistent = persist;
    p.writethru = wt;
    p.size_kb = 1024*1024;
    p.num_shards = 1;
    p.evicting = evicting;
 
    ret = ZSOpenContainer(
                        zs_thrd_state,
                        cname,
                        &p,
                        flag,
                        &cguid
                        );
    fprintf(fp,"container type: fifo=%d/persist=%d/evicting=%d/writethru=%d\n",fifo,persist,evicting,wt);
    fprintf(fp,"ZSOpenContainer: %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t CloseContainer(ZS_cguid_t cid)
{
    ZS_status_t ret;
    ret = ZSCloseContainer(zs_thrd_state, cid);
    fprintf(fp,"ZSCloseContainer : %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t DeleteContainer(ZS_cguid_t cid)
{
    ZS_status_t ret;
    ret = ZSDeleteContainer(zs_thrd_state, cid);
    fprintf(fp,"ZSDeleteContainer : %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t WriteObject(ZS_cguid_t cid,char *key,uint32_t keylen,char *data,uint64_t datalen,uint32_t flags)
{
    ZS_status_t ret;
    ret = ZSWriteObject(zs_thrd_state, cid, key, keylen, data, datalen, flags);
    fprintf(fp,"ZSWriteObject : %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t ReadObject(ZS_cguid_t cid,char *key,uint32_t keylen,char **data,uint64_t *datalen)
{
    ZS_status_t ret;
    ret = ZSReadObject(zs_thrd_state,cid,key,keylen,data,datalen);
    fprintf(fp,"ZSReadObject : %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t DeleteObject(ZS_cguid_t cid,char *key,uint32_t keylen)
{
    ZS_status_t ret;
    ret = ZSDeleteObject(zs_thrd_state,cid,key,keylen);
    fprintf(fp,"ZSDeleteObject : %s\n",ZSStrError(ret));
    return ret;
}

ZS_status_t GetContainerStats(ZS_cguid_t cid,ZS_stats_t *stats)
{
    ZS_status_t ret;
    ret = ZSGetContainerStats(zs_thrd_state,cid,stats);
    fprintf(fp,"ZSGetContainerStats : %s\n",ZSStrError(ret));
    return ret;
}

/***************** test ******************/

int  test_invalid_para(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[0] = "#test0: ZSGetContainerStats with invalid in parameters.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[0]);
 
    ZS_stats_t p;
    ret = GetContainerStats(2, NULL);
    if(ZS_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][0] =1;
    }
    ret =  GetContainerStats(-1, &p);
    if(ZS_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][1] =1;
    }
    ret =  GetContainerStats(0, &p);
    if(ZS_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][2] =1;
    }
    ret =  GetContainerStats(1111, &p);
    if(ZS_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][3] =1;
    }
    ret =  GetContainerStats(11111111, &p);
    if(ZS_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][4] =1;
    }
    ret =  GetContainerStats(-215, &p);
    if(ZS_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][5] =1;
    }
 
    return (6 == tag);
}

int test_basic_check(uint32_t aw)
{
    ZS_status_t ret;
    int tag = 0;
    testname[1] = "#test1: basic check.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[1]);
    
    ZS_stats_t stats1,stats2;
    for(int i = 0; i < 10; i++)
    {
        ret = OpenContainer("test1", 1, aw, mode[i][0], mode[i][1], mode[i][2], mode[i][3]);
        // ret = OpenContainer("test1", 1, aw, mode[i][0], mode[i][1], mode[i][2], 0);
	fprintf(fp, "OpenContainer returned %d\n", ret);
        if(ZS_SUCCESS == ret)
        {
	    fprintf(fp, "OpenContainer succeeded!\n");
            //(void)WriteObject(cguid, "xxxx", 5, "123", 4, ZS_WRITE_MUST_NOT_EXIST);
            //(void)WriteObject(cguid, "yyyy", 5, "456", 4, ZS_WRITE_MUST_NOT_EXIST);
            (void)WriteObject(cguid, "xxxx", 5, "123", 4, 0);
            (void)WriteObject(cguid, "yyyy", 5, "456", 4, 0);
	    (void) ZSFlushContainer(zs_thrd_state, cguid);
            ret = GetContainerStats(cguid, &stats1);
	    fprintf(fp, "GetContainerStats returned %d for cguid=%ld\n", ret, cguid);
            if (ZS_SUCCESS == ret)
            {
		fprintf(fp, "cguid=%ld, ZS_CACHE_STAT_WRITETHRUS=%ld\n", cguid, stats1.cache_stats[ZS_CACHE_STAT_WRITETHRUS]);
		fprintf(fp, "cguid=%ld, ZS_CACHE_STAT_WRITEBACKS=%ld\n", cguid, stats1.cache_stats[ZS_CACHE_STAT_WRITEBACKS]);
		if (mode[i][3] == 0) {
			// writeback

			/*
			 * Currently write Back with Btree is not supported, Hence write back set is reset to write through in btree layer.
			 * In this unit test, This part of the test collects writeback stats and compare them, so this part is bypassed now
			 */

		    /*if (stats1.cache_stats[ZS_CACHE_STAT_WRITETHRUS] == 0) {
			result[aw][1][i] += 1;
		    }*/
			result[aw][1][i] += 1;
		} else {
		    // writethru
		    if (stats1.cache_stats[ZS_CACHE_STAT_WRITETHRUS] != 0) {
			result[aw][1][i] += 1;
		    }
		}

            }
            (void)DeleteObject(cguid, "yyyy", 5);
            (void)DeleteObject(cguid, "xxxx", 5);
            ret = GetContainerStats(cguid, &stats2);
            if(ZS_SUCCESS == ret)
            {
                result[aw][1][i] += 1;
            }
        }
        if(2 == result[aw][1][i])
        {
            tag += 1;
            result[aw][1][i] = 1;
        }else {
            result[aw][1][i] = 0;
        }
        (void)CloseContainer(cguid);
        (void)DeleteContainer(cguid);
    }
    return (10 == tag);
}


/****** main function ******/

int main() 
{
    int testnumber = 1;
	int count      = 0;

    if((fp = fopen("ZS_WritebackStoreMode.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!.\n");
        return -1;
    }

    ZSSetProperty("ZS_STRICT_WRITEBACK", "On");
    if (ZS_SUCCESS == pre_env())
    {
        for(uint32_t aw = 0; aw < 1; aw++)
        {
            count += test_invalid_para(aw);
    	    count += test_basic_check(aw);
         }
         clear_env();
    }
    fclose(fp);
  
    fprintf(stderr, "Test Result:\n");
    for(int aw = 0; aw < 1; aw++)
    {
        if(0 == aw)
        {
            fprintf(stderr, "***** When disable async write: *****\n");
        }else{
            fprintf(stderr, "***** When enable async write: *****\n");
        }
        for(int i = 0; i < testnumber; i++)
        {
	    int j_end = (i == 0) ? 6 : 10;
            fprintf(stderr, "%s\n", testname[i]);
            for(int j = 0; j < j_end; j++)
            {
            if(result[aw][i][j] == 1)
            {
                fprintf(stderr, "[mode fifo=%d/persist=%d/evict=%d/writethru=%d] pass\n",mode[j][0],mode[j][1],mode[j][2],mode[j][3]);
            }else{
                fprintf(stderr, "[mode fifo=%d/persist=%d/evict=%d/writethru=%d] fail\n",mode[j][0],mode[j][1],mode[j][2],mode[j][3]);
            }
        }
    }
   }
   if(testnumber*2 == count)
   {
      fprintf(stderr, "#Test of ZSWritebackStoreMode pass!\n");
   }else{
      fprintf(stderr, "#Test of ZSWritebackStoreMode fail!\n");
   }
   return (!(testnumber*2 == count));
}
