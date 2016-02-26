/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/* 
 * File:   init_protocol.c
 * Author: Darpan Dinker
 *
 * Created on March 28, 2008, 3:09 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: init_protocol.c 10527 2009-12-12 01:55:08Z drew $
 */

#include "platform/logging.h"
#include "protocol/init_protocol.h"
#include "platform/stdio.h"

static HashMap map = NULL;
static uint32_t myRank;

SDF_boolean_t 
sdf_protocol_initialize(uint32_t rank, SDF_boolean_t usingFth)
{
    SDF_status_t ret = SDF_FALSE;
    
    myRank = rank;
    if (SDF_TRUE == usingFth) {
        map = HashMap_create(17, FTH_BUCKET_RW);
    } else {
        map = HashMap_create(17, PTHREAD_MUTEX_BUCKET);
    }
    if (NULL != map) {
        ret = SDF_TRUE;
        plat_log_msg(21336, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_TRACE,
                     "sdf/PROTOCOL is INITIALIZED \n");
    }    

    return (ret);
}

SDF_boolean_t 
sdf_protocol_reset()
{
    HashMap_destroy(map);
    return (SDF_TRUE);
}

HashMap
getGlobalQueueMap()
{
    return (map);
}

uint32_t
getRank()
{
    return (myRank);
}
