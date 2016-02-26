/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthCv.h
 * Author: Jonathan Bertoni
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 */

#ifndef __FTH_CV_H
#define __FTH_CV_H

#include "platform/types.h"
#include "fth.h"
#include "fthThread.h"
#include <pthread.h>

typedef struct fthCondVar {
    fthLock_t *lock;
    fthThreadQ_lll_t queue;
} fthCondVar_t;

#define fthCondVar_t struct fthCondVar

/*
 *  The wait and signal interfaces are similar to pthreads so that we can move
 *  to that interface easily.  The "lock" parameter to fthCvInit is used for
 *  debugging assertions.
 */

extern void fthCvInit     (fthCondVar_t *condVar, fthLock_t *lock);
extern void fthCvWait     (fthCondVar_t *condVar, fthLock_t *lock);
extern void fthCvSignal   (fthCondVar_t *condVar);
extern void fthCvBroadcast(fthCondVar_t *condVar);

#endif
