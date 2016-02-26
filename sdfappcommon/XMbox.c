/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   XMbox.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: XMbox.c 396 2008-02-29 22:55:43Z jim $
 */

/**
 * @brief Cross-thread messaging.  This is the common  version.
 */

#include "platform/shmem.h"
#include "platform/assert.h"
#include "fth/fth.h"
#include "XMbox.h"
#include "XList.h"

// Instantiate the shmem stuff
PLAT_SP_IMPL(XMboxEl_sp, struct XMboxEl);
XLIST_SHMEM_IMPL(XMboxEl, XMboxEl, nextShmem);
PLAT_SP_IMPL(ftopMbox_sp, struct ftopMbox);
PLAT_SP_IMPL(ptofMbox_sp, struct ptofMbox);
PLAT_SP_IMPL(ptofMboxPtrs_sp, struct ptofMboxPtrs);

/**
 * @brief Initialize cross mailbox data structure
 * @param mb <IN> local pointer to PtoF mailbox structure to be initialized
 */
void ftopMboxInit(ftopMbox_t *mb) {

    // Set up a mutex attribute to use to init the mutexes
    pthread_mutexattr_t attr;
    plat_assert_rc(pthread_mutexattr_init(&attr));
    plat_assert_rc(pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED));

    // Create the Pthread ordering Mutex.  Only Pthreads aquire this mutex and the holder of
    // the mutex is the only one that can be actively waiting for mail.  Once the active
    // Pthread receives the mail the mutex is released and the next Pthread can receive mail.
    plat_assert_rc(pthread_mutex_init(&mb->pthreadOrder, &attr));
    
    // Create the 3 shared syncronization mutexes
    for (int i = 0; i < 3; i++) {
        plat_assert_rc(sem_init(&mb->lock[i], 1, 1));
    }

    //
    // There is always a dummy element on the singly-linked list chain of mail.  This is
    // done so that the list is MP safe.  It is sort if like a circular list with fill
    // and drain pointers where the "dummy" element is the crossover point in the circular
    // list between the fill and drain.  By making this a linked list instead we do not
    // have to pre-allocate a circular list so there is no limit on the number of pieces
    // of pending mail.  The "fill" and "drain" pointers become the "tail" and "head"
    // pointers, respctively.
    XMboxEl_sp_t dummyShmem = XMboxEl_sp_alloc();
    XMboxEl_t *dummy = XMboxEl_sp_rwref(&dummy, dummyShmem);
    dummy->mailShmem = shmem_void_null;      // Set the mail and next pointers
    dummy->nextShmem = XMboxEl_sp_null;
    XMboxEl_sp_rwrelease(&dummy);            // Release the SHMEM ref

    mb->tailShmem = dummyShmem;              // Set head and tail pointers
    mb->headShmem = dummyShmem;


    mb->fthState = 0;
    mb->fthSpin = 0;
    plat_assert_rc(sem_trywait(&mb->lock[0])); // Init the lock

}

/**
 * @brief Initialize cross mailbox data structure
 * @param mb <IN> local pointer to PtoF mailbox structure to be initialized
 *
 * @return 0 = OK; nozero - something wrong
 */
int ftopMboxDestroy(ftopMbox_t *mb) {
    if (pthread_mutex_trylock(&mb->pthreadOrder)) { // See if other Pthreads using it
        return 1;
    }

    if (fthSelf() == NULL) {                 // Check for PThread
        // Get TWO locks to block everything
        int lock = (mb->fthState == 0) ? 2 : mb->fthState - 1;
        while (1) {                          // Loop until we get a lock
            if (sem_trywait(&mb->lock[lock]) == 0) { // Try this lock
                break;                       // Got the lock
            }
            lock = (lock == 2) ? 0 : lock+1; // Walk circular list

        }

        // We got a lock - now try to hold more
        lock = (lock == 0) ? 2 : lock - 1;   // Back up
        if (sem_trywait(&mb->lock[lock]) != 0) { // Try this lock
            // Could not get the previous lock so wait on the other one
            lock = (lock == 0) ? 2 : lock - 1; // Back up
            plat_assert_rc(sem_wait(&mb->lock[lock])); // Wait until free
        }

        // One way or another we now hold 2 locks and FTH holds the other one
        
    }
    
    FTH_SPIN_LOCK(&mb->fthSpin);             // Now wait until FTH exits

    XMboxEl_t *head = XMboxEl_sp_rwref(&head, mb->headShmem);
    if (!XMboxEl_sp_is_null(head->nextShmem)) { // Any mail (next pointer updated)
        XMboxEl_sp_rwrelease(&head);         // Done with ref
        FTH_SPIN_UNLOCK(&mb->fthSpin);
        return (1);                          // Mail pending
    }
    
    XMboxEl_sp_rwrelease(&head);             // Done with ref
            
    // Destroy it all
    for (int i = 0; i < 2; i++) {
        plat_assert_rc(sem_destroy(&mb->lock[i]));
    }
    plat_assert_rc(pthread_mutex_unlock(&mb->pthreadOrder)); // Cannot destroy if locked
    plat_assert_rc(pthread_mutex_destroy(&mb->pthreadOrder));

    for (int i = 0; i < 2; i++) {
        plat_assert_rc(sem_destroy(&mb->lock[i]));
    }
    plat_assert_rc(pthread_mutex_destroy(&mb->pthreadOrder));
      
    // FTH_SPIN_UNLOCK(&mb->fthSpin); Leave it locked!!!

    return (0);
}

/**
 * @brief Initialize cross mailbox data structure
 * @param mb <IN> local pointer to PtoF mailbox structure to be initialized
 */
void ptofMboxInit(ptofMbox_t *mb) {
    mb->mailHeadShmem = XMboxEl_sp_null;
    mb->mailTailShmem = XMboxEl_sp_null;
    mb->pending = 0;
    fthThreadQ_lll_init(&mb->threadQ);
    mb->spin = 0;
}

/**
 * @brief Destroy cross mailbox data structure
 * @param mb <IN> local pointer to PtoF mailbox structure to be initialized
 *
 * @return 0 = OK; nozero - something wrong
 */
int ptofMboxDestroy(ptofMbox_t *mb) {
    FTH_SPIN_LOCK(&mb->spin);

    if (mb->pending != 0) {
        FTH_SPIN_UNLOCK(&mb->spin);
        return 1;
    }

    if ((!XMboxEl_sp_is_null(mb->mailHeadShmem)) && (!XMboxEl_sp_is_null(mb->mailTailShmem))) {
        FTH_SPIN_UNLOCK(&mb->spin);
        return 1;
    }

    if (fthThreadQ_is_empty(&mb->threadQ) == 0) { // Check for waiters
        FTH_SPIN_UNLOCK(&mb->spin);
        return 1;
    }

        
    // FTH_SPIN_UNLOCK(&mb->fthSpin); Leave it locked!!!

    return (0);
}


