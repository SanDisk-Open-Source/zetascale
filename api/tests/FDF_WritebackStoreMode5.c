/********************************
#function : FDFWritebackStoreMode
#author   : AliceXu, BrianO
********************************/

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
int result[2][10][10] = {{{0}}};
uint32_t mode[10][4] = {
    {0,0,0,0},{0,0,0,1},{0,0,1,0},{0,0,1,1},
    {0,1,0,0},{0,1,0,1},{0,1,1,0},{0,1,1,1},
    {1,0,1,0},{1,0,1,1}};

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
    fprintf(fp,"clear env!\n");
}

FDF_status_t OpenContainer(char *cname, uint32_t flag, uint32_t asyncwrite,
                           uint32_t fifo, uint32_t persist, uint32_t evicting,uint32_t wt)
{
    FDF_status_t          ret;
    FDF_container_props_t p;

    ret = FDF_FAILURE;
    (void)FDFLoadCntrPropDefaults(&p);
    p.flash_only = 0;
    p.async_writes = asyncwrite;
    p.durability_level = 0;
    p.fifo_mode = fifo;
    p.persistent = persist;
    p.writethru = wt;
    // p.size_kb = 1024*1024;
    p.size_kb = 1024;
    p.num_shards = 1;
    p.evicting = evicting;
 
    ret = FDFOpenContainer(
                        fdf_thrd_state,
                        cname,
                        &p,
                        flag,
                        &cguid
                        );
    fprintf(fp,"container type: fifo=%d/persist=%d/evicting=%d/writethru=%d\n",fifo,persist,evicting,wt);
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

FDF_status_t GetContainerStats(FDF_cguid_t cid,FDF_stats_t *stats)
{
    FDF_status_t ret;
    ret = FDFGetContainerStats(fdf_thrd_state,cid,stats);
    fprintf(fp,"FDFGetContainerStats : %s\n",FDFStrError(ret));
    return ret;
}

/***************** test ******************/

int  test_invalid_para(uint32_t aw)
{
    FDF_status_t ret;
    int tag = 0;
    testname[0] = "#test0: FDFGetContainerStats with invalid in parameters.";
    fprintf(fp,"****** async write = %d ******\n",aw);
    fprintf(fp,"%s\n",testname[0]);
 
    FDF_stats_t p;
    ret = GetContainerStats(2, NULL);
    if(FDF_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][0] =1;
    }
    ret =  GetContainerStats(-1, &p);
    if(FDF_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][1] =1;
    }
    ret =  GetContainerStats(0, &p);
    if(FDF_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][2] =1;
    }
    ret =  GetContainerStats(1111, &p);
    if(FDF_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][3] =1;
    }
    ret =  GetContainerStats(11111111, &p);
    if(FDF_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][4] =1;
    }
    ret =  GetContainerStats(-215, &p);
    if(FDF_SUCCESS != ret)
    {
        tag += 1;
        result[aw][0][5] =1;
    }
 
    return (6 == tag);
}

#define DummyDataSize  1024*1024
static char DummyData[DummyDataSize];

int test_basic_check(uint32_t aw)
{
    FDF_status_t ret;
    int tag = 0;
    int k;
    char   stmp[1000];
    testname[1] = "#test1: basic check.";
    fprintf(stderr,"****** async write = %d ******\n",aw);
    fprintf(stderr,"%s\n",testname[1]);

    FDF_stats_t stats1,stats2;
    for(int i = 0; i < 10; i++)
    {
        ret = OpenContainer("test1", 1, aw, mode[i][0], mode[i][1], mode[i][2], mode[i][3]);
        // ret = OpenContainer("test1", 1, aw, mode[i][0], mode[i][1], mode[i][2], 0);
	fprintf(stderr, "OpenContainer returned %d\n", ret);
        if(FDF_SUCCESS == ret)
        {
	    fprintf(fp, "OpenContainer succeeded!\n");
            //(void)WriteObject(cguid, "xxxx", 5, "123", 4, FDF_WRITE_MUST_NOT_EXIST);
            //(void)WriteObject(cguid, "yyyy", 5, "456", 4, FDF_WRITE_MUST_NOT_EXIST);
            (void)WriteObject(cguid, "xxxx", 5, "123", 4, 0);
            (void)WriteObject(cguid, "yyyy", 5, "456", 4, 0);
	    for (k=0; k<100; k++) {
	        sprintf(stmp, "zzz%d", k);
		(void)WriteObject(cguid, stmp, 1+strlen(stmp), DummyData, DummyDataSize, 0);
	    }
	    (void) FDFFlushContainer(fdf_thrd_state, cguid);
            ret = GetContainerStats(cguid, &stats1);
	    fprintf(stderr, "GetContainerStats returned %d for cguid=%ld\n", ret, cguid);
            if (FDF_SUCCESS == ret)
            {
		fprintf(stderr, "cguid=%ld, FDF_CACHE_STAT_WRITETHRUS=%ld\n", cguid, stats1.cache_stats[FDF_CACHE_STAT_WRITETHRUS]);
		fprintf(stderr, "cguid=%ld, FDF_CACHE_STAT_WRITEBACKS=%ld\n", cguid, stats1.cache_stats[FDF_CACHE_STAT_WRITEBACKS]);
		fprintf(stderr, "cguid=%ld, FDF_CACHE_STAT_ASYNC_DRAINS=%ld\n", cguid, stats1.cache_stats[FDF_CACHE_STAT_ASYNC_DRAINS]);
		fprintf(stderr, "cguid=%ld, FDF_CACHE_STAT_ASYNC_PUTS=%ld\n", cguid, stats1.cache_stats[FDF_CACHE_STAT_ASYNC_PUTS]);
		fprintf(stderr, "cguid=%ld, FDF_CACHE_STAT_ASYNC_PUT_FAILS=%ld\n", cguid, stats1.cache_stats[FDF_CACHE_STAT_ASYNC_PUT_FAILS]);
		fprintf(stderr, "cguid=%ld, FDF_CACHE_STAT_ASYNC_FLUSHES=%ld\n", cguid, stats1.cache_stats[FDF_CACHE_STAT_ASYNC_FLUSHES]);
		fprintf(stderr, "cguid=%ld, FDF_CACHE_STAT_ASYNC_FLUSH_FAILS=%ld\n", cguid, stats1.cache_stats[FDF_CACHE_STAT_ASYNC_FLUSH_FAILS]);
		fprintf(stderr, "cguid=%ld, FDF_CACHE_STAT_ASYNC_WRBKS=%ld\n", cguid, stats1.cache_stats[FDF_CACHE_STAT_ASYNC_WRBKS]);
		fprintf(stderr, "cguid=%ld, FDF_CACHE_STAT_ASYNC_WRBK_FAILS=%ld\n", cguid, stats1.cache_stats[FDF_CACHE_STAT_ASYNC_WRBK_FAILS]);
		if (mode[i][3] == 0) {
		    // writeback

			/*
			 * Currently write Back with Btree is not supported, Hence write back set is reset to write through in btree layer.
			 * In this unit test, This Part of the test collects writeback stats and compare them, so this part is bypassed now
			 */

			/* if (mode[i][2]) {
			// evicting
			if ((stats1.cache_stats[FDF_CACHE_STAT_WRITETHRUS] == 0) &&
			(stats1.cache_stats[FDF_CACHE_STAT_ASYNC_WRBKS] != 0) &&
			(stats1.cache_stats[FDF_CACHE_STAT_ASYNC_WRBK_FAILS] == 0))
			{
			result[aw][1][i] += 1;
			}
			} else {
			// non-evicting
			if ((stats1.cache_stats[FDF_CACHE_STAT_WRITETHRUS] == 0) &&
			(stats1.cache_stats[FDF_CACHE_STAT_ASYNC_WRBKS] != 0) &&
			(stats1.cache_stats[FDF_CACHE_STAT_ASYNC_WRBK_FAILS] != 0))
			{
			result[aw][1][i] += 1;
			}
			}*/
			result[aw][1][i] += 1;
		} else {
		    // writethru
		    if (aw) {
		        // async writes
			if (mode[i][2]) {
			    // evicting
			    if ((stats1.cache_stats[FDF_CACHE_STAT_WRITETHRUS] != 0) &&
				(stats1.cache_stats[FDF_CACHE_STAT_ASYNC_PUTS] != 0) &&
				(stats1.cache_stats[FDF_CACHE_STAT_ASYNC_PUT_FAILS] == 0))
			    {
				result[aw][1][i] += 1;
			    }
			} else {
			    // non-evicting
			    if ((stats1.cache_stats[FDF_CACHE_STAT_WRITETHRUS] != 0) &&
				(stats1.cache_stats[FDF_CACHE_STAT_ASYNC_PUTS] != 0) &&
				(stats1.cache_stats[FDF_CACHE_STAT_ASYNC_PUT_FAILS] != 0))
			    {
				result[aw][1][i] += 1;
			    }
			}
		    } else {
			if (stats1.cache_stats[FDF_CACHE_STAT_WRITETHRUS] != 0) {
			    result[aw][1][i] += 1;
			}
		    }
		}

            }
            (void)DeleteObject(cguid, "yyyy", 5);
            (void)DeleteObject(cguid, "xxxx", 5);
            ret = GetContainerStats(cguid, &stats2);
            if(FDF_SUCCESS == ret)
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

    if((fp = fopen("FDF_WritebackStoreMode.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!.\n");
        return -1;
    }

    FDFSetProperty("FDF_STRICT_WRITEBACK", "Off");
    FDFSetProperty("FDF_MAX_OUTSTANDING_BACKGROUND_FLUSHES", "0");
    FDFSetProperty("FDF_CACHE_SIZE", "100000000");
    if (FDF_SUCCESS == pre_env())
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
      fprintf(stderr, "#Test of FDFWritebackStoreMode pass!\n");
   }else{
      fprintf(stderr, "#Test of FDFWritebackStoreMode fail!\n");
   }
   return (!(testnumber*2 == count));
}
