/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/* 
 * File:   protocol/init_protocol.h
 * Author: Darpan Dinker
 *
 * Created on March 28, 2008, 3:08 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: init_protocol.h 843 2008-04-01 17:30:08Z darpan $
 */

#ifndef _INIT_PROTOCOL_H
#define	_INIT_PROTOCOL_H

#ifdef	__cplusplus
extern "C" {
#endif

#include "common/sdftypes.h"
#include "utils/hashmap.h"

/**
 * @brief Create and initialize resource needed by SDF protocol engine
 *
 * @param rank id of the daemon process
 * @return SDF_TRUE on success
 */
SDF_boolean_t sdf_protocol_initialize(uint32_t rank, SDF_boolean_t usingFth);

/**
 * @brief Free up resources held by SDF protocol, e.g. HashMap
 * @return SDF_TRUE on success
 */
SDF_boolean_t sdf_protocol_reset();

/**
 * @brief HashMap storing the queue pairs for various purposes, available after
 * a successful init.
 *
 * @see sdf_protocol_initialize
 * @see HashMap
 */
HashMap getGlobalQueueMap();

/**
 * Get my ID as provided by MPI or future cluster manager
 */
uint32_t getRank();

#ifdef	__cplusplus
}
#endif

#endif	/* _INIT_PROTOCOL_H */

