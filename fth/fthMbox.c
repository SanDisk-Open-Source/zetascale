/*
 * File:   fthMbox.c
 * Author: Brian O'Krafka
 *
 * Created on August 10, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthMbox.c 396 2008-02-29 22:55:43Z briano $
 */


/**
 * @brief Mailbox routines
 */

#define FTH_SPIN_CALLER

#include "fth.h"
#include "fthMbox.h"
#include "platform/platform.h"

//  Number of mail links to allocate in a batch for a thread
#define N_ALLOC_MAIL_LINKS   1024

typedef struct mailLink {
    uint64_t           mail;
    struct mailLink   *next;
} mailLink_t;

static __thread mailLink_t *freeMailLinks = NULL;
static __thread uint32_t    nMailLinks = 0;

// Counts number of spurious pthread_cond_wait "wakeups"
static __thread uint64_t nExtraCondWaits = 0;

static mailLink_t *get_link()
{
    int          i;
    mailLink_t  *link;

    if (freeMailLinks == NULL) {
        // allocate some new mail links
        link = plat_alloc(N_ALLOC_MAIL_LINKS*sizeof(mailLink_t));
	nMailLinks += N_ALLOC_MAIL_LINKS;
	for (i=0; i<N_ALLOC_MAIL_LINKS; i++) {
	    link->next = freeMailLinks;
	    freeMailLinks = link;
	    link++;
	}
    }
    link = freeMailLinks;
    freeMailLinks = link->next;
    return(link);
}

static void free_link(mailLink_t *link)
{
    link->next = freeMailLinks;
    freeMailLinks = link;
}

/**
 * @brief Init mailbox
 */
void fthMboxInit(fthMbox_t *mb) 
{
    pthread_mutex_init(&(mb->mutex), NULL);
    pthread_cond_init(&(mb->mail_present_cv), NULL);
    mb->mail           = NULL;
    mb->nmails         = 0;
    mb->nwaiters       = 0;
    mb->ndispatch      = 0;
    mb->is_terminating = 0;
}

/**
 * @brief Get mail from mailbox or wait for mail
 *
 * If mail is waiting then this routine returns immediately with the mail.  
 * If not then the routine waits for mail to be posted.
 *
 * @param mb <IN> Mailbox structure pointer
 * @return Arbitrary 64-bit mail
 */
uint64_t fthMboxWait(fthMbox_t *mb) 
{
    uint64_t      rv;
    mailLink_t   *link;
    int32_t       n_extra_cond_waits;

    pthread_mutex_lock(&(mb->mutex));
    n_extra_cond_waits = -1;
    while (1) {
        if (mb->mail != NULL) {
	    link = mb->mail;
            mb->mail = link->next;
	    (mb->nmails)--;
	    rv = link->mail;
	    free_link(link);
	    break;
	} else if (mb->is_terminating) {
	    rv = (uint64_t) NULL;
	    break;
	} else {
	    (mb->nwaiters)++;
	    n_extra_cond_waits++;
	    pthread_cond_wait(&(mb->mail_present_cv), &(mb->mutex));
	    (mb->nwaiters)--;
	}
    }
    pthread_mutex_unlock(&(mb->mutex));
    (mb->ndispatch)++;
    if (n_extra_cond_waits > 0) {
	nExtraCondWaits += n_extra_cond_waits;
    }
    return rv;
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
uint64_t fthMboxTry(fthMbox_t *mb) 
{
    uint64_t      rv;
    mailLink_t   *link;

    pthread_mutex_lock(&(mb->mutex));
    if (mb->mail != NULL) {
	link = mb->mail;
	mb->mail = link->next;
	(mb->nmails)--;
	rv = link->mail;
	free_link(link);
	(mb->ndispatch)++;
    } else {
        rv = 0;
    }
    pthread_mutex_unlock(&(mb->mutex));
    return rv;
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
void fthMboxPost(fthMbox_t *mb, uint64_t mail) 
{
    mailLink_t   *link;

    pthread_mutex_lock(&(mb->mutex));
    link = get_link();

    link->mail = mail;
    link->next = mb->mail;
    mb->mail   = link;
    (mb->nmails)++;

    if (mb->nwaiters > 0) {
        pthread_cond_signal(&(mb->mail_present_cv));
    }

    pthread_mutex_unlock(&(mb->mutex));
}

/**
 * @brief Terminate a mailbox
 *
 * Clear the pending mail queue, post all waiters with NULL.
 *
 * @param mb <IN> FTH mailbox structure pointer
 */
void fthMboxTerm(fthMbox_t *mb) 
{
    mailLink_t   *link;
    mailLink_t   *link_next;

    pthread_mutex_lock(&(mb->mutex));
    plat_assert(mb->nwaiters*mb->nmails == 0);

    //  Clear the pending mail queue
    for (link = mb->mail; link != NULL; link = link_next) {
        link_next = link->next;
	free_link(link);
    }

    // post all waiters with NULL
    mb->is_terminating = 1;
    pthread_cond_broadcast(&(mb->mail_present_cv));

    pthread_mutex_unlock(&(mb->mutex));
}

/**
 * @brief Get the mailbox dispatch count
 *
 * Return the count of the number of mail dispatches

 * @param mb <IN> FTH mailbox structure pointer
 * @return mailbox post counter
 */
uint32_t fthMboxDispatchCount(fthMbox_t *mb) 
{
    return(mb->ndispatch);
}

