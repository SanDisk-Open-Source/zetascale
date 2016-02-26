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
