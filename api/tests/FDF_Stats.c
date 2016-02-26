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

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include "zs.h"
static struct ZS_state     *zs_state;
struct ZS_thread_state     *_zs_thd_state;


int preEnvironment()
{
    if(ZSInit( &zs_state) != ZS_SUCCESS ) {
        fprintf( stderr, "ZS initialization failed!\n" );
        return 0 ;
    }
    fprintf( stderr, "ZS was initialized successfully!\n" );
    if(ZS_SUCCESS != ZSInitPerThreadState( zs_state, &_zs_thd_state ) ) {
        fprintf( stderr, "ZS thread initialization failed!\n" );
        return 0;
    }
    fprintf( stderr, "ZS thread was initialized successfully!\n" );
    return 1;
}

void CleanEnvironment()
{
    ZSReleasePerThreadState(&_zs_thd_state);
    ZSShutdown(zs_state);
}

#define DATA_LEN 1024

ZS_status_t verify_zs_container_stats() {
    ZS_status_t ret;
    ZS_container_props_t p;
    ZS_cguid_t cguid;
    uint32_t flag = 1;
    char key[256], cname[64]="cont1";
    char value[1024];
    //uint32_t keylen = 0;
    char *data;
    uint64_t datalen;
    ZS_stats_t stats;;
   
     
    /* Create a container */
    ZSLoadCntrPropDefaults(&p);
    p.size_kb = 0;
    ret = ZSOpenContainer(_zs_thd_state,cname,&p,flag,&cguid);
    if( ret != ZS_SUCCESS ) {
        fprintf( stderr, "Container open failed\n");
        return ZS_FAILURE;
    } 
    strcpy(key,"key_test"); 
    /* Write object */
    ret = ZSWriteObject(_zs_thd_state, cguid, key, strlen(key), value, DATA_LEN, 1);
    if( ret != ZS_SUCCESS ) {
        fprintf( stderr, "Write failed\n");
        return ZS_FAILURE;
    } 
    ret = ZSReadObject(_zs_thd_state,cguid,key,strlen(key),&data,&datalen);
    if( ret != ZS_SUCCESS ) {
        fprintf( stderr, "Read failed\n");
        return ZS_FAILURE;
    } 
    free(data);

    ZSGetContainerStats(_zs_thd_state,cguid,&stats); 
    if(stats.cntr_stats[ZS_CNTR_STATS_NUM_OBJS] != 1 ) {
        fprintf( stderr, "Number of objects (%lu) does not match expected count %d\n",
                   stats.cntr_stats[ZS_CNTR_STATS_NUM_OBJS], 1);
         return ZS_FAILURE;

    }
    if(stats.cntr_stats[ZS_CNTR_STATS_USED_SPACE] <= 0 ) {
        fprintf( stderr, "Used space is < 0 and does not match expected value\n");
         return ZS_FAILURE;
    }

    if( (stats.n_accesses[ZS_ACCESS_TYPES_APCOE] <= 0 ) &&
        (stats.n_accesses[ZS_ACCESS_TYPES_APCOP] <= 0 )&&
        (stats.n_accesses[ZS_ACCESS_TYPES_WRITE] <= 0 )) {
        fprintf( stderr, "Number of create requests is not as expected\n");
        return ZS_FAILURE;
    }
    if( (stats.n_accesses[ZS_ACCESS_TYPES_READ] <= 0 ) &&
        (stats.n_accesses[ZS_ACCESS_TYPES_APGRX] <= 0 )) {
        fprintf( stderr, "Number of get requests is not as expected\n");
        return ZS_FAILURE;
    }
    /* Check if create  */
    ZSCloseContainer(_zs_thd_state,cguid);
     return ZS_SUCCESS;

}

int main(int argc, char *argv[])
{
    if( 1 != preEnvironment()) {
        return 1;
    }
    if( verify_zs_container_stats() != ZS_SUCCESS ) {
        return 1;
    }
    CleanEnvironment();
}
