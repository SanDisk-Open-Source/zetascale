/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthSem.h
 * Author: drew
 *
 * Created on April 22, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthSem.h 6914 2009-04-23 06:05:29Z drew $
 */

// Semaphore

#ifndef _FTH_SEM_H
#define _FTH_SEM_H

#include "fthMbox.h"

typedef struct fthSem {
    fthMbox_t mbox;
} fthSem_t;

/** @brief Initialize sem with initial count */
void fthSemInit(struct fthSem *sem, int count);

/** @brief Add count to sem */
void fthSemUp(struct fthSem *sem, int count);

/** @brief Block until count are decremented from sem */
void fthSemDown(struct fthSem *sem, int count);

#endif
