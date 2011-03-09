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
