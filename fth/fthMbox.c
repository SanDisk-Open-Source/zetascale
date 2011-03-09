/*
 * File:   fthMbox.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthMbox.c 396 2008-02-29 22:55:43Z jim $
 */


/**
 * @brief Mailbox routines
 */

#define FTH_SPIN_CALLER

#include "fth.h"
#include "fthMbox.h"
#include "fthWaitEl.h"

/**
 * @brief Init mailbox
 */
void fthMboxInit(fthMbox_t *mb) {
    FTH_SPIN_INIT(&mb->spin);
    fthWaitQ_lll_init(&mb->mailQ);
    fthThreadQ_lll_init(&mb->threadQ);
#ifdef FTH_MBOX_STATS
    mb->dispatchCount = 0;
#endif
#ifndef BUGCHECK
    mb->qSize = 0;
#endif    

}

/**
 * @brief Get mail from mailbox or wait for mail
 *
 * If mail is waiting then this routine returns immediately with thye mail.  If not
 * Then the routine waits for mail to be posted.
 *
 * @param mb <IN> Mailbox structure pointer
 * @return Arbitrary 64-bit mail
 */
uint64_t fthMboxWait(fthMbox_t *mb) {
    // Check for pending mail
    uint64_t rv;
    FTH_SPIN_LOCK(&mb->spin);
    fthWaitEl_t *wait = fthWaitQ_shift_nospin(&mb->mailQ);
    if (wait != NULL) {                      // If mail
        // Mail pending - peel it off
        rv = wait->mailData;                 // Get the data
        fthFreeWaitEl(wait);                 // Release it
#ifndef BUGCHECK        
        mb->qSize--;
#endif        
    } else {
        // No mail - sleep
        fthThreadQ_push_nospin(&mb->threadQ, fthSelf()); // Push onto queue
        FTH_SPIN_UNLOCK(&mb->spin);
        rv = fthWait();                      // Give up processor
        FTH_SPIN_LOCK(&mb->spin);
    }
    FTH_SPIN_UNLOCK(&mb->spin);

    return (rv);
}

/**
 * @brief Get mail from mailbox or return 0 (NULL)
 *
 * If mail is waiting then this routine returns immediately with thye mail.  If not
 * then 0 (NULL) is returned.
 *
 * NB: 0 is a perfectly valid 64-bit mail value so routines using this function should
 *     ensure that the sender never posts a 0 to this mailbox
 *
 * @param mb <IN> Mailbox structure pointer
 * @return Arbitrary 64-bit mail
 */
uint64_t fthMboxTry(fthMbox_t *mb) {
    // Check for pending mail
    uint64_t rv;
    FTH_SPIN_LOCK(&mb->spin);
    fthWaitEl_t *wait = fthWaitQ_shift_nospin(&mb->mailQ);
    if (wait != NULL) {                      // If mail
        // Mail pending - peel it off
        rv = wait->mailData;                 // Get the data
        fthFreeWaitEl(wait);                 // Release it
#ifndef BUGCHECK        
        mb->qSize--;
#endif        
    } else {                                 // No mail
        rv = 0;
    }
    
    FTH_SPIN_UNLOCK(&mb->spin);

    return (rv);
}

/**
 * @brief Post a value to a mailbox.
 *
 * The top thread waiting for mail in this mailbox is dispatched with this value or
 * (if no thread waiting) then the value is queued.
 *
 * @param mb <IN> FTH mailbox structure pointer
 * @param mail <IN> arbitrary value to pass to waiting thread.
 */
void fthMboxPost(fthMbox_t *mb, uint64_t mail) {
    FTH_SPIN_LOCK(&mb->spin);
    fthThread_t *thread = fthThreadQ_shift_nospin(&mb->threadQ); // Get thread
    if (thread == NULL) {                    // If no threads
        fthWaitEl_t *wait = fthGetWaitEl();  // Allocate a wait element
        wait->mailData = mail;
        fthWaitQ_push_nospin(&mb->mailQ, wait); // Just push mail onto stack
#ifndef BUGCHECK
        mb->qSize++;
        plat_assert(mb->qSize < 1000000);
#endif        

    } else {
        // Thread was waiting
        fthResume(thread, mail);             // Mark eligible, set RV
#ifdef FTH_MBOX_STATS    
        mb->dispatchCount++;                 // Count number of actual dispatches
#endif    
    }
    FTH_SPIN_UNLOCK(&mb->spin);
}

/**
 * @brief Post a value to a mailbox only if the mailbox is empty.
 *
 * The top thread waiting for mail in this mailbox is dispatched with this value or
 * (if no thread waiting) then the value is queued unless there is alreay mail
 * queued.  Allows mailboxes to be used something like a semaphore.
 *
 * @param mb <IN> FTH mailbox structure pointer
 * @param mail <IN> arbitrary value to pass to waiting thread.
 */
void fthMboxPostIfEmpty(fthMbox_t *mb, uint64_t mail) {
    FTH_SPIN_LOCK(&mb->spin);
    fthThread_t *thread = fthThreadQ_shift_nospin(&mb->threadQ); // Get thread
    if (thread == NULL) {                    // If no threads
        if (fthWaitQ_head(&mb->mailQ) == NULL) { // If empty
            fthWaitEl_t *wait = fthGetWaitEl(); // Allocate a wait element
            wait->mailData = mail;
            fthWaitQ_push_nospin(&mb->mailQ, wait); // Just push mail onto stack
        }

    } else {
        // Thread was waiting
        fthResume(thread, mail);             // Mark eligible, set RV
#ifdef FTH_MBOX_STATS    
        mb->dispatchCount++;                 // Count number of actual dispatches
#endif    
    }
    FTH_SPIN_UNLOCK(&mb->spin);
}

/**
 * @brief Terminate a mailbox
 *
 * Clear the pending mail queue, post all waiters with NULL.
 *
 * @param mb <IN> FTH mailbox structure pointer
 */
void fthMboxTerm(fthMbox_t *mb) {
    FTH_SPIN_LOCK(&mb->spin);

    // Clear the pending threads
    while (1) {
        fthThread_t *thread = fthThreadQ_shift_nospin(&mb->threadQ); // Get thread
        if (thread == NULL) {                // If no threads
            break;
        }
        fthResume(thread, (uint64_t) NULL);
    }

    // Clear the pending mail
    while (1) {
        fthWaitEl_t *wait = fthWaitQ_shift_nospin(&mb->mailQ);
        if (wait == NULL) {                  // If no mail
            break;
        }
        fthFreeWaitEl(wait);                 // Release it
    }

    FTH_SPIN_UNLOCK(&mb->spin);
}

/**
 * @brief Get the mailbox dispatch count
 *
 * Return the count of the number of mail dispatches

 * @param mb <IN> FTH mailbox structure pointer
 * @return mailbox post counter
 */
uint32_t fthMboxDispatchCount(fthMbox_t *mb) {
#ifdef FTH_MBOX_STATS    
    return (mb->dispatchCount);
#else
    return (0);
#endif    
}


