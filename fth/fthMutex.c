/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthMutex.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthMutex.c 396 2008-02-29 22:55:43Z jim $
 */


#include "platform/types.h"
#include "platform/assert.h"

#include "fthMutex.h"
#include "fth.h"

extern fth_t *fth;

/**
 * @brief Lock a pthread mutex or wait until you can
 *
 * @param mem <IN> Pointer to mutex
 */
void fthMutexLock(pthread_mutex_t *mutex) {

    if (pthread_mutex_trylock(mutex) != 0) {
        fthThread_t *self = fthSelf();
        self->mutexWait = mutex;

        self->state = 'X';                   // Mutex wait 
        fthThreadQ_push(&fth->mutexQ, self);   // Push onto queue
        (void) fthWait();                    // Give up processor
    }

    return;
}

/**
 * @brief Unlock a pthread mutex
 *
 * @param mem <IN> Pointer to mutex
 */
void fthMutexUnlock(pthread_mutex_t *mutex) {
    plat_assert_rc(pthread_mutex_unlock(mutex));
}

