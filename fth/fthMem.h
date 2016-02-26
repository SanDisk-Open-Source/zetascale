/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthMem.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthMem.h 396 2008-02-29 22:55:43Z jim $
 */

#ifndef __FTH_MEM_H
#define __FTH_MEM_H

#include "platform/types.h"

#define FTH_MEMQ_MAX 10                      // Up to 10 memQs can be active

uint32_t fthMemQAlloc(void);
uint64_t fthMemWait(uint64_t *mem, int queueNum);

#endif
