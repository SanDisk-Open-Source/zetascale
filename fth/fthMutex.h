/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthMutex.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthMutex.h 396 2008-02-29 22:55:43Z jim $
 */

#ifndef __FTH_MUTEX_H
#define __FTH_MUTEX_H

#include "platform/types.h"
#include <pthread.h>

void fthMutexLock(pthread_mutex_t *mutex);
void fthMutexUnlock(pthread_mutex_t *mutex);

#endif
