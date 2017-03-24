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
 * File:   mbox.c
 * Author: Jim, ported to pthreads by Brian O'Krafka
 *
 * Created on August 10, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: mbox.c 396 2008-02-29 22:55:43Z briano $
 */


/**
 * @brief Mailbox routines
 */

#include <sys/syscall.h>
#include <sys/types.h>
#include "mbox.h"

//  Number of mail links to allocate in a batch for a thread
#define N_ALLOC_MAIL_LINKS   1024

//  Number of mail link pools
#define N_MBOX_LINK_POOLS    1000

typedef struct mailLink {
    uint32_t           link_pool;
    uint64_t           mail;
    struct mailLink   *next;
    struct mailLink   *prev;
} mailLink_t;

static mailLink_t         *freeMailLinks[N_MBOX_LINK_POOLS];
static uint32_t            nMailLinks[N_MBOX_LINK_POOLS];
static pthread_spinlock_t  freeMailLinkLocks[N_MBOX_LINK_POOLS];
static uint64_t            nCurMailLinkPool = 0;

// Counts number of spurious pthread_cond_wait "wakeups"
static __thread uint64_t nExtraCondWaits = 0;

static mailLink_t *get_link()
{
    int          i;
    uint64_t     npool;
    mailLink_t  *link;

    npool  = __sync_fetch_and_add(&nCurMailLinkPool, 1);
    npool %= N_MBOX_LINK_POOLS;
    pthread_spin_lock(&(freeMailLinkLocks[npool]));

    if (freeMailLinks[npool] == NULL) {
        // allocate some new mail links
        link = malloc(N_ALLOC_MAIL_LINKS*sizeof(mailLink_t));
	nMailLinks[npool] += N_ALLOC_MAIL_LINKS;
	if (nMailLinks[npool] > 1000000) {
	    fprintf(stderr, "================ thread %ld allocated over 1000000 new links!\n", syscall(SYS_gettid));
	}
	for (i=0; i<N_ALLOC_MAIL_LINKS; i++) {
	    link->next = freeMailLinks[npool];
	    link->link_pool = npool;
	    freeMailLinks[npool] = link;
	    link++;
	}
    }
    link = freeMailLinks[npool];
    freeMailLinks[npool] = link->next;
    pthread_spin_unlock(&(freeMailLinkLocks[npool]));
    return(link);
}

static void free_link(mailLink_t *link)
{
    uint32_t     npool;

    npool = link->link_pool;
    pthread_spin_lock(&(freeMailLinkLocks[npool]));
    link->next = freeMailLinks[npool];
    freeMailLinks[npool] = link;
    pthread_spin_unlock(&(freeMailLinkLocks[npool]));
}

/**
 * @brief Init mailbox subsystem
 */
void mboxMasterInit()
{
    int  i;

    for (i=0; i<N_MBOX_LINK_POOLS; i++) {
        freeMailLinks[i] = NULL;
	nMailLinks[i]    = 0;
	pthread_spin_init(&(freeMailLinkLocks[i]), PTHREAD_PROCESS_PRIVATE);
    }
    nCurMailLinkPool = 0;
}

//  Used to ensure that link pool is initialized once at beginning
static uint64_t   firstFlag = 0;
static int32_t    dummyFirstFlag = 0;

/**
 * @brief Init mailbox
 */
void mboxInit(mbox_t *mb) 
{
    if (!dummyFirstFlag) {
	uint64_t   flag;
	dummyFirstFlag = 1;
	flag = __sync_fetch_and_or(&firstFlag, 1);
	if (!flag) {
	  mboxMasterInit();
	}
    }

    pthread_mutex_init(&(mb->mutex), NULL);
    pthread_cond_init(&(mb->mail_present_cv), NULL);
    mb->mail_first     = NULL;
    mb->mail_last      = NULL;
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
uint64_t mboxWait(mbox_t *mb) 
{
    uint64_t      rv;
    mailLink_t   *link;
    int32_t       n_extra_cond_waits;

    pthread_mutex_lock(&(mb->mutex));
    n_extra_cond_waits = -1;
    while (1) {
        if (mb->mail_first != NULL) {

	    link = mb->mail_first;
            mb->mail_first = link->next;
	    if (link->next != NULL) {
	        link->next->prev = NULL;
	    } else {
	        mb->mail_last = NULL;
	    }

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
    (mb->ndispatch)++;
    pthread_mutex_unlock(&(mb->mutex));
    if (n_extra_cond_waits > 0) {
	nExtraCondWaits += n_extra_cond_waits;
    }
    return rv;
}

/**
 * @brief Get batch of mail from mailbox or wait for mail
 *
 * If mail is waiting then this routine returns immediately with a batch
 * of mail (up to max_mails of them).  
 * If not then the routine waits for mail to be posted.
 *
 * @param mb <IN> Mailbox structure pointer
 * @param mails <INOUT> array in which to return batch of mail
 * @param max_mails <IN> size of mails array
 * @return number of mails retrieved
 */
uint64_t mboxWaitBatch(mbox_t *mb, uint64_t *mails, uint64_t max_mails)
{
    uint64_t      n_mails;
    mailLink_t   *link, *link_next;
    int32_t       n_extra_cond_waits;

    pthread_mutex_lock(&(mb->mutex));
    n_extra_cond_waits = -1;
    while (1) {
        if (mb->mail_first != NULL) {
	   n_mails = 0;
           for (link = mb->mail_first;
	        link != NULL;
		link = link_next)
	    {
		link_next = link->next;
		mb->mail_first = link->next;
		if (link->next != NULL) {
		    link->next->prev = NULL;
		} else {
		    mb->mail_last = NULL;
		}

		(mb->nmails)--;
		mails[n_mails] = link->mail;
		free_link(link);
	        n_mails++;
		if (n_mails >= max_mails) {
		    break;
		}
	    }
	    break;
	} else if (mb->is_terminating) {
	    n_mails = 0;
	    break;
	} else {
	    (mb->nwaiters)++;
	    n_extra_cond_waits++;
	    pthread_cond_wait(&(mb->mail_present_cv), &(mb->mutex));
	    (mb->nwaiters)--;
	}
    }
    mb->ndispatch += n_mails;
    pthread_mutex_unlock(&(mb->mutex));
    if (n_extra_cond_waits > 0) {
	nExtraCondWaits += n_extra_cond_waits;
    }
    return n_mails;
}

/**
 * @brief Dump mail in mailbox.
 *
 * Dump contents of all mail currently in queue.
 *
 * @param mb <IN> Mailbox structure pointer
 * @param dump_f <IN> Callback function for dumping contents of each mail
 * @param pstuff <IN> Anonymous state for callback function
 * @return number of mails retrieved
 */
uint64_t mboxDump(mbox_t *mb, FILE *f, dump_mail_cb_t dump_fn, void *pstuff)
{
    uint64_t      n_mails;
    mailLink_t   *link, *link_next;

    pthread_mutex_lock(&(mb->mutex));

    n_mails = 0;
    if (mb->mail_first != NULL) {
       for (link = mb->mail_first;
	    link != NULL;
	    link = link_next)
	{
	    link_next = link->next;
            (*dump_fn)(f, n_mails, link->mail, pstuff);
            n_mails++;
	}
    }

    pthread_mutex_unlock(&(mb->mutex));

    return n_mails;
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
uint64_t mboxTry(mbox_t *mb) 
{
    uint64_t      rv;
    mailLink_t   *link;

    pthread_mutex_lock(&(mb->mutex));
    if (mb->mail_first != NULL) {

	link = mb->mail_first;
	mb->mail_first = link->next;
	if (link->next != NULL) {
	    link->next->prev = NULL;
	} else {
	    mb->mail_last = NULL;
	}

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
 * @param mb <IN> mailbox structure pointer
 * @param mail <IN> arbitrary value to pass to waiting thread.
 */
void mboxPost(mbox_t *mb, uint64_t mail) 
{
    mailLink_t   *link;

    pthread_mutex_lock(&(mb->mutex));
    link = get_link();

    link->mail = mail;

    link->next     = NULL;
    link->prev     = mb->mail_last;
    mb->mail_last  = link;
    if (link->prev != NULL) {
        link->prev->next = link;
    } else {
        mb->mail_first = link;
    }

    (mb->nmails)++;

    pthread_cond_signal(&(mb->mail_present_cv));
    pthread_mutex_unlock(&(mb->mutex));
}

/**
 * @brief Terminate a mailbox
 *
 * Clear the pending mail queue, post all waiters with NULL.
 *
 * @param mb <IN> FTH mailbox structure pointer
 */
void mboxTerm(mbox_t *mb) 
{
    mailLink_t   *link;
    mailLink_t   *link_next;

    pthread_mutex_lock(&(mb->mutex));
    assert(mb->nwaiters*mb->nmails == 0);

    //  Clear the pending mail queue
    for (link = mb->mail_first; link != NULL; link = link_next) {
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
uint32_t mboxDispatchCount(mbox_t *mb) 
{
    return(mb->ndispatch);
}

