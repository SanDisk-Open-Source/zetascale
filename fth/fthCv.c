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
 * File:   fthCv.c
 * Author: Jonathan Bertoni
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 */


#include "platform/types.h"
#include "platform/assert.h"
#include "fthLock.h"
#include "fthCv.h"
#include "fth.h"

extern fth_t *fth;

/**
 * @brief Initialize a condition variable
 *
 * @parmam condVar <IN> the condition variable
 * @parmam lock <IN> the lock associated with this condition
 */

void fthCvInit(fthCondVar_t *condVar, fthLock_t *lock)
{
   memset(condVar, 0, sizeof(*condVar));
   condVar->lock = lock;
}

/**
 * @brief Wait on a condition variable
 *
 * @param condVar <IN> pointer to a condition variable
 * @param lock <IN> pointer to the corresponding lock
 */

void fthCvWait(fthCondVar_t *condVar, fthLock_t *lock)
{
    fthThread_t *  self;
    fthWaitEl_t *  waitElement;

    self = fthSelf();
    plat_assert_always(lock != NULL && lock == condVar->lock);
    plat_assert_always(lock->writeLock && lock->writer == self);
    waitElement = fthWaitQ_head(&lock->holdQ);

    self->condVar = condVar;
    self->state = 'C';                        // condition wait

    fthThreadQ_push(&condVar->queue, self);   // Push onto queue
    fthUnlock(waitElement);
    fthWait();                                // Give up processor

    fthLock(lock, 1, NULL);
    self->condVar = NULL;
    return;
}

/**
 * @brief Wake at most one process waiting on a given condition variable
 *
 * @param condVar <IN> Pointer to a condition variable
 *
 *    The thread must hold the lock specified by the previous waiter, if any.
 */

void fthCvSignal(fthCondVar_t *condVar)
{
    fthThread_t *  thread;
    fthThread_t *  self;
    fthLock_t *    lock;

    self = fthSelf();
    lock = condVar->lock;

    plat_assert_always(lock->writeLock && lock->writer == self);

    thread = fthThreadQ_head(&condVar->queue);

    if (thread != NULL) {
       fthThreadQ_remove_nospin(&condVar->queue, thread);
       fthThreadQ_push(&fth->eligibleQ[thread->schedNum], thread);
    }
}

/**
 * @brief Wake all processes waiting on a given condition variable
 *
 * @param condVar <IN> Pointer to a condition variable
 *
 *    The thread must hold the lock specified by the previous waiter, if any.
 */

void fthCvBroadcast(fthCondVar_t *condVar)
{
    fthThread_t *  thread;
    fthThread_t *  self;
    fthLock_t *    lock;

    self = fthSelf();
    lock = condVar->lock;

    plat_assert_always(lock->writeLock && lock->writer == self);

    if (fthThreadQ_head(&condVar->queue) != NULL) {
        for (;;) {
            thread = fthThreadQ_head(&condVar->queue);

            if (thread == NULL) {
                break;
            }

            plat_assert_always(thread->condVar == condVar);
            fthThreadQ_remove_nospin(&condVar->queue, thread);
            fthThreadQ_push(&fth->eligibleQ[thread->schedNum], thread);
        }
    }
}
