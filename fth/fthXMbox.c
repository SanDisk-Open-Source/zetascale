/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthXMbox.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthXFMbox.c 396 2008-02-29 22:55:43Z jim $
 */

/**
 * @brief Cross-thread messaging.  This is the FTH-callable version.
 */

#define FTH_SPIN_CALLER

#include <pthread.h>
#include <semaphore.h>

#include "platform/shmem.h"
#include "platform/assert.h"

#include "fth.h"

#include "sdfappcommon/XMbox.h"

#include "fthXMbox.h"

/**
 * @brief Initialize the cross mailbox facility
 * @return fth PtoF posting head/tail structure
 */
//ftopMbox_sp_t ftopMboxSystemInit(void) {
//    return NULL;
//}


/**
 * @brief Send a message from FTH to a Pthread
 * @param mb <IN> local pointer to FtoP mailbox
 * @param mailShmem <IN> void SHMEM pointer to mail
 */
void ftopMboxPost(ftopMbox_t *mb, shmem_void_t mailShmem) {
    FTH_SPIN_LOCK(&mb->fthSpin);             // Block other FTH threads

    // Create a new dummy elmement
    XMboxEl_sp_t dummyShmem = XMboxEl_sp_alloc();
    XMboxEl_t *dummy = XMboxEl_sp_rwref(&dummy, dummyShmem);
    dummy->mailShmem = shmem_void_null;      // Set the mail and next pointers
    dummy->nextShmem = XMboxEl_sp_null;
    asm __volatile__("mfence":::"memory");   // Push the nulls out to others
    XMboxEl_sp_rwrelease(&dummy);            // Release the SHMEM ref
    

    XMboxEl_t *tail = XMboxEl_sp_rwref(&tail, mb->tailShmem); // Reference the tail
    tail->mailShmem = mailShmem;             // Mail goes in old dummy
    asm __volatile__("mfence":::"memory");   // Make sure that all can see this
    tail->nextShmem = dummyShmem;            // Set next pointer to new dummy
    asm __volatile__("mfence":::"memory");   // Make sure that all can see this (in order)
    
    XMboxEl_sp_rwrelease(&tail);             // Release the ref

    mb->tailShmem = dummyShmem;              // Set new tail pointer

    // These mutexes are circular.  The producer adds mail to the list and then advances
    // the mutex that it holds.  If it cannot advance then the consumer is lagging behind
    // and there is no need to advance.  If the next mutex is obtained then the producer
    // releases the current lock and that will wake up any Pthreads waiting for mail
    // (actually, it only wakes up one at  a time - see the order mutex)
    int nextState = (mb->fthState == 2) ? 0 : mb->fthState + 1;
    if (sem_trywait(&mb->lock[nextState]) == 0) {
        // Got the lock - advance the state
        plat_assert_rc(sem_post(&mb->lock[mb->fthState])); // Release old lock
        mb->fthState = nextState;            // Got new state
    }

    FTH_SPIN_UNLOCK(&mb->fthSpin);           // Other FTH threads can proceed
}


/**
 * @brief Get mail from PtoF mailbox or wait for mail
 *
 * If mail is waiting then this routine returns immediately with the mail.
 * If no mail then the routine waits for mail to be posted.
 *
 * @param mb <IN> PtoF ailbox structure pointer
 * @return Arbitrary shmem_void_t shmem pointer
 */
shmem_void_t ptofMboxWait(ptofMbox_t *mb) {
    FTH_SPIN_LOCK(&mb->spin);                // Lock other FTH threads

    XMboxEl_sp_t elShmem = XMboxEl_xlist_dequeue(&mb->mailHeadShmem, &mb->mailTailShmem);
    if (XMboxEl_sp_is_null(elShmem)) {       // Check if anything there
        // Sleep until mail arrives
        fthThread_t *self = fthSelf();
        fthThreadQ_push(&mb->threadQ, self); // Push onto queue
        FTH_SPIN_UNLOCK(&mb->spin);
        shmem_void_t rv;
        rv.base.int_base = fthWait();        // Set Shmem on wait return
        return (rv);
    }
    
    // Mail waiting and no other threads in front of this one
    XMboxEl_t *el = XMboxEl_sp_rwref(&el, elShmem);
    shmem_void_t rv = el->mailShmem;         // Remember shmem pointer to mail
    XMboxEl_sp_rwrelease(&el);               // Done with ref
    
    XMboxEl_sp_free(elShmem);                // Free mbox element

    FTH_SPIN_UNLOCK(&mb->spin);

    return (rv);
}

/**
 * @brief Get mail from PtoF mailbox or return shmem_void_null.
 *
 * If mail is waiting then this routine returns immediately with the mail.
 * If no mail then returns shmem NULL.
 *
 * NB: shmem_void_null is a perfectly valid mail value so callers to this
 *     routine should ensure that a shmem null value is never posted to this mailbox.
 *
 * @param mb <IN> PtoF ailbox structure pointer
 * @return Arbitrary shmem_void_t shmem pointer
 */
shmem_void_t ptofMboxTry(ptofMbox_t *mb) {
    FTH_SPIN_LOCK(&mb->spin);                // Lock other FTH threads
    shmem_void_t rv;
    XMboxEl_sp_t elShmem = XMboxEl_xlist_dequeue(&mb->mailHeadShmem, &mb->mailTailShmem);
    if (XMboxEl_sp_is_null(elShmem)) {       // Check if anything there
        rv = shmem_void_null;
    } else {
        // Mail waiting and no other threads in front of this one
        XMboxEl_t *el = XMboxEl_sp_rwref(&el, elShmem);
        rv = el->mailShmem;                  // Remember shmem pointer to mail
        XMboxEl_sp_rwrelease(&el);           // Done with ref
        
        XMboxEl_sp_free(elShmem);            // Free mbox element
    }

    FTH_SPIN_UNLOCK(&mb->spin);

    return (rv);
}
