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
