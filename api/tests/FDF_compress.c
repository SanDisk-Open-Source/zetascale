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

#include     <stdio.h>
#include     <stdlib.h>
#include     <string.h>
#include     <unistd.h>
#include     <pthread.h>
#include    <zs.h>
#include    <assert.h>

#define    ZS_PROP_FILE        "conf/zs.prop"    //Configuration file
#define DATA_LEN 2048

int
main( int argc, char **argv)
{
    struct ZS_state        *zs_state;
    struct ZS_thread_state *thd_state;
    ZS_status_t            status;
    char                    data[DATA_LEN];
    char                    cname[32] = {0};
    ZS_cguid_t             cguid;
    ZS_container_props_t   props;
    ZS_stats_t stats;
                                
    strcpy(cname, "testc");
    if (ZSLoadProperties(ZS_PROP_FILE) != ZS_SUCCESS) {
        printf("Couldn't load properties from %s. ZSInit()"
            " will use default properties or from file specified"
            " in ZS_PROPRTY_FILE environment variable if set.\n",
            ZS_PROP_FILE);
    } else {
        /*
         * Propertie were loaded from file successfully, dont overwrite
         * them by reading file specified in environment variable.
         */
        unsetenv("ZS_PROPERTY_FILE");
    }
    ZSSetProperty("ZS_COMPRESSION", "1");
    //Initialize ZS state.
    if ((status = ZSInit(&zs_state)) != ZS_SUCCESS) {
        printf("ZSInit failed with error %s\n", ZSStrError(status));
        return 1;
    }

    //Initialize per-thread ZS state for main thread.
    if ((status = ZSInitPerThreadState(zs_state, &thd_state)) != 
                                ZS_SUCCESS) {
        printf("ZSInitPerThreadState failed with error %s\n",
                            ZSStrError(status));
        return 1;
    }
    ZSLoadCntrPropDefaults(&props);
    props.compression = 1;
    status = ZSOpenContainer(thd_state, cname, &props,
                          ZS_CTNR_CREATE | ZS_CTNR_RW_MODE, &cguid);
    if (status != ZS_SUCCESS) {
          printf("ZSOpenContainer failed\n");
          return 1;
    }
    memset(data,0,DATA_LEN);
    status = ZSWriteObject(thd_state, cguid, "key1", 5, data, DATA_LEN, 0);  
    if( status != ZS_SUCCESS ) {
          printf("ZSWriteObject failed\b");
          return 1;
    }
            //Get container statistics. Print number of objects in container.
    if ((status = ZSGetContainerStats(thd_state, cguid, &stats)) ==
                                                            ZS_SUCCESS) {
            printf("cguid %ld: Number of objects: %"PRId64"\n",
                    cguid, stats.flash_stats[ZS_FLASH_STATS_NUM_OBJS]);
    }   
    if ( stats.flash_stats[ZS_FLASH_STATS_COMP_BYTES] == 0 ) {
         printf("Compression bytes must be a non-zero\n");
         return 1;
    }
    assert(ZS_SUCCESS == ZSReleasePerThreadState(&thd_state));

    printf("Compressed bytes:%lu\n",stats.flash_stats[ZS_FLASH_STATS_COMP_BYTES]);
    /*Get ZS stats and see compressed_bytes */
    //Gracefuly shutdown ZS.
    ZSShutdown(zs_state);
    return (0);
}
