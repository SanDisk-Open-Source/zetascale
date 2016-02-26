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

/****************************
#function : ZSLoadProperties
#author   : AliceXu
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
int result[10] = {0};

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
    fprintf(fp, "clear env!\n");
}                       

/***************** test ******************/

int test_property_is_null()
{
    ZS_status_t status;
    testname[0] = "#test0: test with incoming parmeter is NULL.";

    status = ZSLoadProperties(NULL);
    if(ZS_SUCCESS == status)
    {
        fprintf(fp,"ok -> can load properties with input is NULL!\n");
        return 0;
    }else{
        fprintf(fp,"fail -> can't load properties with input is NULL!\n");
        result[0] = 1;
        return 1;
    }
}

int test_property_is_wrong()
{
    ZS_status_t status;
    testname[1] = "#test1: test with property path is none exist or wrong.";

    status = ZSLoadProperties("lalala");
    if(ZS_SUCCESS != status)
    {
        result[1] = 1;
        fprintf(fp,"ok -> can't load properties with property path is none-exist!\n");
        return 1;
    }else{
        fprintf(fp,"fail -> can't load properties with property path is none-exist!\n");
        return 0;
    }
}

int test_basic_check()
{
    ZS_status_t status;
    testname[2] = "#test2: test with basic function.";

    if (system("mkdir /tmp/my.properties")) {}
    sleep(5);
    status = ZSLoadProperties("/tmp/my.properties");
    fprintf(stderr, "ZSLoadProperties test_basic_check() returned %s\n", ZSStrError(status));
    if (system("rm -rf /tmp/my.properties")) {}
    if(ZS_SUCCESS == status)
    {
        result[2] = 1;
        fprintf(fp,"ok -> can't load properties with valid property path!\n");
        return 1;
    }else{
        fprintf(fp,"fail -> can't load properties with valid property path!\n");
        return 0;
    }
}

/************** main function ***********/

int main() 
{
    int testnumber = 3;
	int count      = 0;

    if((fp = fopen("ZS_LoadProperties.log", "w+")) == 0)
    {
        fprintf(stderr, " open log file failed!.\n");
        return -1;
    }

    if(ZS_SUCCESS == pre_env())
    {
        count += test_property_is_null();
        count += test_property_is_wrong();
        count += test_basic_check();
        clear_env();
    } else {
		return -1;
	}
    fclose(fp);
   
    fprintf(stderr, "Test Result:\n");
    for(int i = 0; i < testnumber; i++)
    {
        fprintf(stderr, "%s\n", testname[i]);
        if(1 == result[i])
        {
            fprintf(stderr, "result: pass\n");
        }else{
            fprintf(stderr, "result: fail\n");
        }
    }

    if(testnumber == count)
    {
        fprintf(stderr, "#Test of ZSLoadProperties pass!\n");
	fprintf(stderr, "#The related test script is ZS_LoadProperties.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_LoadProperties.log\n");
    }else{
        fprintf(stderr, "#Test of ZSLoadProperties fail!\n");
	fprintf(stderr, "#The related test script is ZS_LoadProperties.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_LoadProperties.log\n");
    }


	return (!(testnumber == count));
}
