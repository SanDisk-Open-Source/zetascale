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

#include "zs.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

#define ZS_MAX_KEY_LEN 256

#define OBJ_COUNT 100000

int
main()
{
    ZS_container_props_t props;
    struct ZS_state *zs_state;
    struct ZS_thread_state *thd_state;
    ZS_status_t	status;
    ZS_cguid_t cguid;
    char	data1[256] = "data is just for testing pstats";
    uint64_t i;

    char key_var[ZS_MAX_KEY_LEN] ={0};

    ZSSetProperty("ZS_REFORMAT", "1");
    if (ZSInit(&zs_state) != ZS_SUCCESS) {
		return -1;
	}
    ZSInitPerThreadState(zs_state, &thd_state);	

    ZSLoadCntrPropDefaults(&props);

	props.size_kb = 1024 * 1024 * 2;
    props.persistent = 1;
    props.evicting = 0;
    props.writethru = 1;
    props.durability_level= ZS_DURABILITY_SW_CRASH_SAFE;
    props.fifo_mode = 0;

    /*
     * Test 1: Check if asynchronous delete container works
     */

    status = ZSOpenContainer(thd_state, "cntr", &props, ZS_CTNR_CREATE, &cguid);
    if (status != ZS_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        return -1;	
    }

    /*
     * Write a million objects
     */
    for (i = 1; i <= OBJ_COUNT; i++) {
        memset(key_var, 0, ZS_MAX_KEY_LEN);
        sprintf(key_var, "key_%ld", i);
        sprintf(data1, "data_%ld", i);

        status = ZSWriteObject(thd_state, cguid, key_var, strlen(key_var) + 1, data1, strlen(data1), 0);
        if (ZS_SUCCESS != status) {
            fprintf(stderr, "ZSWriteObject= %s\n", ZSStrError(status));
        }
        assert(ZS_SUCCESS == status);
    }

    /*
     * Delete the container
     */
    status = ZSDeleteContainer(thd_state, cguid);
    if (status != ZS_SUCCESS) {
        printf("Delete container failed with error=%s.\n", ZSStrError(status));
        return -1;	
    }

    /*
     * Make sure container do not exist now!
     */
    status = ZSOpenContainer(thd_state, "cntr", &props, ZS_CTNR_RO_MODE, &cguid);
    if (status == ZS_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        return -1;	
    }

    /*
     * Test 2
     */
     
    status = ZSOpenContainer(thd_state, "cntr", &props, ZS_CTNR_CREATE, &cguid);
    if (status != ZS_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        return -1;	
    }

    /*
     * Write two million objects
     */
    for (i = 1; i <= 2 * OBJ_COUNT; i++) {
        memset(key_var, 0, ZS_MAX_KEY_LEN);
        sprintf(key_var, "key_%ld", i);
        sprintf(data1, "data_%ld", i);

        status = ZSWriteObject(thd_state, cguid, key_var, strlen(key_var) + 1, data1, strlen(data1), 0);
        assert(ZS_SUCCESS == status);
    }

    /*
     * Delete the container
     */
    status = ZSDeleteContainer(thd_state, cguid);
    if (status != ZS_SUCCESS) {
        printf("Delete container failed with error=%s.\n", ZSStrError(status));
        return -1;	
    }

    /*
     * Make sure container do not exist now!
     */
    status = ZSOpenContainer(thd_state, "cntr", &props, ZS_CTNR_RO_MODE, &cguid);
    if (status == ZS_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        return -1;	
    }

    status = ZSOpenContainer(thd_state, "cntr", &props, ZS_CTNR_CREATE, &cguid);
    if (status != ZS_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        return -1;	
    }

    /*
     * Write a million objects
     */
    for (i = 1; i <= OBJ_COUNT; i++) {
        memset(key_var, 0, ZS_MAX_KEY_LEN);
        sprintf(key_var, "key_%ld", i);
        sprintf(data1, "data_%ld", i);

        status = ZSWriteObject(thd_state, cguid, key_var, strlen(key_var) + 1, data1, strlen(data1), 0);
        assert(ZS_SUCCESS == status);
    }
    ZSCloseContainer(thd_state, cguid);
    status = ZSDeleteContainer(thd_state, cguid);
    assert(ZS_SUCCESS == status);

    ZSReleasePerThreadState(&thd_state);

    ZSShutdown(zs_state);

    return 0;
}
