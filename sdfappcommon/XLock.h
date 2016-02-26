/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   XLock.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: XLock.h 396 2008-02-29 22:55:43Z jim $
 */

//
//  Cross-threading (pthread/fth) lock
//

#ifndef _SDFAPP_COMMON_XLOCK_H
#define _SDFAPP_COMMON_XLOCK_H

#include <pthread.h>

#include "fth/fthLock.h"

typedef struct XLock {
    int fthLock;                             // Integer count
    pthread_rwlock_t qLock;                  // For queueing control
    pthread_rwlock_t lock;                   // Main lock
    int write;                               // Set for write lock
} XLock_t;

#endif
