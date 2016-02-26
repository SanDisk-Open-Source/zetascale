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

