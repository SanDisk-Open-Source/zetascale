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
 * File:   fthStructQ.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthStructQ.c 396 2008-02-29 22:55:43Z jim $
 */


/**
 * @brief Management for queues of pre-allocated data structures
 *
 * N.B:  THE STRUCT MUST BE AT LEAST BIG ENOUGH TO HOLD 2 POINTERS!!!
 *
 *       THIS IS NOT CHECKED BY THE CODE!!!

*/

#define FTH_SPIN_CALLER

#include "fth.h"
#include "fthStructQ.h"
#include "fthlll_c.h"

/**
 * @brief Init struct malloc
 */
void fthStructQInit(fthStructQ_t *sq) {
    sq->spin = 0;
    fthDummyStructQ_lll_init(&sq->structQ);
    fthThreadQ_lll_init(&sq->threadQ);
}

/**
 * @brief Get struct or wait for one to be freed
 *
 * If a struct is available then this routine returns immediately with the struct.  If not
 * then the routine waits for a struct to be freed
 *
 * @param sq <IN> struct malloc pointer
 * @return pointer to struct
 */
void *fthStructQWait(fthStructQ_t *sq) {
    fthThread_t *self;

    /*
     * Check early so that users don't introduce latent bugs where things
     * appeared to work because their pthread didn't have to wait on 
     * resources.
     */
    self = fthSelf();
    plat_assert(self);

    // Allocate a structQ or wait trying
    FTH_SPIN_LOCK(&sq->spin);
    void *rv = (void *) fthDummyStructQ_shift_nospin(&sq->structQ);
    if (rv != NULL) {                      // If one free
        // Struct was available
        FTH_SPIN_UNLOCK(&sq->spin);
    } else {
        // None free - sleep
        fthThreadQ_push_nospin(&sq->threadQ, self); // Push onto queue
        FTH_SPIN_UNLOCK(&sq->spin);
        rv = (void *) fthWait();              // Give up processor
    }

    return (rv);
}

/**
 * @brief Get struct or return null
 *
 * If a struct is available then this routine returns immediately with the struct.  If not
 * then the routine returns null.
 *
 * @param sq <IN> struct malloc pointer
 * @return pointer to struct
 */
void *fthStructQTry(fthStructQ_t *sq) {
    // Check for available structQ
    FTH_SPIN_LOCK(&sq->spin);
    void *rv = (void *) fthDummyStructQ_shift_nospin(&sq->structQ);
    FTH_SPIN_UNLOCK(&sq->spin);
    return (rv);
}

/**
 * @brief Free a struct (add back to available pool)
 *
 * The top thread waiting for a struct is dispatched with this value or
 * (if no thread waiting) then the struct is queued.
 *
 * @param sq <IN> struct malloc structure pointer
 * @param ds <IN> struct pointer.
 */
void fthStructQFree(fthStructQ_t *sq, void *ds) {
    FTH_SPIN_LOCK(&sq->spin);
    fthThread_t *thread = fthThreadQ_shift_nospin(&sq->threadQ); // Get thread
    if (thread == NULL) {                    // If no threads
        fthDummyStructQ_push_nospin(&sq->structQ, (fthDummyStruct_t *) ds); // Just push struct onto stack
    } else {
        // Thread was waiting
        fthResume(thread, (uint64_t) ds);     // Mark eligible, set RV
    }
    FTH_SPIN_UNLOCK(&sq->spin);
}
