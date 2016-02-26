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
 * @brief Cross-thread messaging.  This is the Pthread-callable version.
 */

#include <pthread.h>

#include "platform/shmem.h"
#include "platform/shmem_global.h"

#include "platform/assert.h"

#include "XMbox.h"

/**
 * @brief Receive a message from an FTH thread or wait for one to arrive
 * @param xmboxShmem <IN> SHMEM pointer to FtoP mailbox
 * @return mailShmem void SHMEM pointer to mail
 */
shmem_void_t ftopMboxWait(ftopMbox_t *xmbox) {
    plat_assert_rc(pthread_mutex_lock(&xmbox->pthreadOrder)); // Wait for our turn

    // These mutexes are circular.  The producer adds mail to the list and then advances
    // the mutex that it holds.  The consumer (this thread) has to establish a lock and
    // then check the queue.  If it is empty then it tries to advance the lock and checks
    // the queue again (this gets around race conditions where the mail is being queued
    // at about the same time that this thread is cehcking).  If there is no mail then
    // eventually this thread will wait on the lock held by the producer until there is
    // mail.

    // Guess at the initial lock state.  Race conditions could make this guess wrong
    // but the loop will just find a free lock eventually.
    int lock = (xmbox->fthState == 0) ? 2 : xmbox->fthState - 1;
    while (1) {                              // Loop until we get a lock
        if (sem_trywait(&xmbox->lock[lock]) == 0) { // Try this lock
            break;                           // Got the lock
        }
        lock = (lock == 2) ? 0 : lock+1;     // Walk circular list

    }

    // OK - we got a lock so now we can check the queue and advance the lock
    XMboxEl_t *head = XMboxEl_sp_rwref(&head, xmbox->headShmem);
    while (1) {
        if (XMboxEl_sp_is_null(head->nextShmem)) { // Any mail (next pointer updated)
            int nextLock = (lock == 2) ? 0 : lock + 1; // Lock to advance to
            while (sem_wait(&xmbox->lock[nextLock])) {}; // Could wait here            
            plat_assert_rc(sem_post(&xmbox->lock[lock])); // Release old lock
            lock = nextLock;                 // Lock advanced
        } else {
            plat_assert_rc(sem_post(&xmbox->lock[lock])); // Release old lock
            break;                           // Got mail
        }
    }

    // We got some mail - update structures and release the element
    XMboxEl_sp_t oldHeadShmem = xmbox->headShmem; // Remember shmem pointer to head
    shmem_void_t rv = head->mailShmem;       // Remember shmem pointer to mail
    xmbox->headShmem = head->nextShmem;      // New head
    XMboxEl_sp_rwrelease(&head);             // Done with ref
    XMboxEl_sp_free(oldHeadShmem);           // Free mbox element

    plat_assert_rc(pthread_mutex_unlock(&xmbox->pthreadOrder)); // Other Pthread threads can proceed

    return (rv);                             // Return shmem_void pointer
}

/**
 * @brief Receive a message from an FTH thread or return shmem_void_null
 *
 * Note the a brief pause top lock the ordering mutex is possible.
 *
 * NB:  shemem_void_null is a valid mail valid so callers to tis routine should
 *      ensure that a shmem null value is never posted to this mailbox.
 *
 * @param xmboxShmem <IN> SHMEM pointer to FtoP mailbox
 * @return mailShmem void SHMEM pointer to mail
 */
shmem_void_t ftopMboxTry(ftopMbox_t *xmbox) {
    plat_assert_rc(pthread_mutex_lock(&xmbox->pthreadOrder)); // Wait for our turn - short lock

    // OK - we got a lock so now we can check the queue and advance the lock
    XMboxEl_t *head = XMboxEl_sp_rwref(&head, xmbox->headShmem);
    shmem_void_t rv;
    if (XMboxEl_sp_is_null(head->nextShmem)) { // Any mail (next pointer updated)
        rv = shmem_void_null;
    } else {
        // We got some mail - update structures and release the element
        XMboxEl_sp_t oldHeadShmem = xmbox->headShmem; // Remember shmem pointer to head
        rv = head->mailShmem;                // Remember shmem pointer to mail
        xmbox->headShmem = head->nextShmem;  // New head
        XMboxEl_sp_rwrelease(&head);         // Done with ref
        XMboxEl_sp_free(oldHeadShmem);       // Free mbox element
    }

    plat_assert_rc(pthread_mutex_unlock(&xmbox->pthreadOrder)); // Other Pthread threads can proceed

    return (rv);                             // Return shmem_void pointer
}

// This routine is in this file because it references ptofMutex and ptofTailShmem
void ptofMboxPost(ptofMbox_sp_t xmboxShmem, shmem_void_t mailShmem) {

    // Create a new elmement
    XMboxEl_sp_t elShmem = XMboxEl_sp_alloc();
    XMboxEl_t *el = XMboxEl_sp_rwref(&el, elShmem);
    el->ptofMboxShmem = xmboxShmem;
    el->mailShmem = mailShmem;               // Mail goes here
    XMboxEl_sp_rwrelease(&el);

    ptofMboxPtrs_sp_t ptofShmem;
    ptofShmem.base.int_base = shmem_global_get(SHMEM_GLOBAL_PTOF_MBOX_PTRS);
    plat_assert(!plat_shmem_ptr_is_null(ptofShmem));
    ptofMboxPtrs_t *ptofPtrs = ptofMboxPtrs_sp_rwref(&ptofPtrs, ptofShmem);


    // Use lockless queueing to add to tail
    XMboxEl_xlist_enqueue(&ptofPtrs->headShmem, &ptofPtrs->tailShmem, elShmem);

    ptofMboxPtrs_sp_rwrelease(&ptofPtrs);

    // Increment the pending counter
    ptofMbox_t *mb = ptofMbox_sp_rwref(&mb, xmboxShmem);
    (void) __sync_fetch_and_add(&mb->pending, 1);
    ptofMbox_sp_rwrelease(&mb);

}
