/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   tlmap3.c
 * Author: Brian O'Krafka
 *
 * Created on May 20, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tlmap3.c 308 2008-02-20 22:34:58Z tomr $
 */

#define _TLMAP3_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <string.h>
#include "platform/logging.h"
#include "platform/stdlib.h"
#include "tlmap3.h"
#include "utils/hash.h"

#define LOG_CAT_FLASH      PLAT_LOG_CAT_SDF_PROT_REPLICATION

void TLMap3Init(TLMap3_t *pm, uint64_t nbuckets, 
     int (*print_fn)(TLMap3Entry_t *pce, char *sout, int max_len))
{
    uint64_t          i;

    pm->nbuckets      = nbuckets;
    pm->print_fn      = print_fn;
    pm->buckets       = plat_alloc(nbuckets*(sizeof(TLMap3Bucket_t)));
    if (pm->buckets == NULL) {
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
	     "Could not allocate thread-local map buckets.");
	plat_abort();
    }

    for (i=0; i<nbuckets; i++) {
	pm->buckets[i].entry = NULL;
    }
}

void TLMap3Destroy(TLMap3_t *pm)
{
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
	     "TLMap3Destroy is not yet implemented.");
}

TLMap3Entry_t *TLMap3Create(TLMap3_t *pm, char *pkey, int keylen)
{
    uint64_t           h;
    TLMap3Entry_t   *pme;
    TLMap3Bucket_t  *pb;

    h = hash((const unsigned char *) pkey, strlen(pkey), 0) % pm->nbuckets;
    pb = &(pm->buckets[h]);

    for (pme = pb->entry; pme != NULL; pme = pme->next) {
	if ((pme->keylen == keylen) && 
	    (strcmp((const char *) pme->key, (const char *) pkey) == 0))
	{
	    return(NULL);
	}
    }

    if (pme == NULL) {

        /* Create a new entry. */

	pme = (TLMap3Entry_t *) plat_alloc(sizeof(TLMap3Entry_t));
	if (pme == NULL) {
	    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		 "Could not allocate a thread-local map entry.");
	    return(NULL);
	}

	pme->contents = NULL;
	pme->key      = (char *) plat_alloc(keylen+1);

	if (pme->key == NULL) {
	    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		 "Could not allocate a thread-local key.");
	    plat_free(pme);
	    return(NULL);
	}
	strcpy(pme->key, pkey);
	pme->keylen   = keylen;
	pme->seqno    = 0;

	/* put myself on the bucket list */
        pme->next = pb->entry;
        pb->entry = pme;
    }

    return(pme);
}

TLMap3Entry_t *TLMap3Get(TLMap3_t *pm, char *pkey, int keylen)
{
    uint64_t           h;
    TLMap3Entry_t   *pme;
    TLMap3Bucket_t  *pb;

    h = hash((const unsigned char *) pkey, strlen(pkey), 0) % pm->nbuckets;
    pb = &(pm->buckets[h]);

    for (pme = pb->entry; pme != NULL; pme = pme->next) {
	if ((pme->keylen == keylen) && 
	    (strcmp((const char *) pme->key, (const char *) pkey) == 0))
	{
	    break;
	}
    }

    return(pme);
}

/* return 0 if succeeds, 1 if object doesn't exist */
// TLMap3Delete frees the contents (if they are non-NULL)
int TLMap3Delete(TLMap3_t *pm, char *pkey, int keylen) 
{
    uint64_t           h;
    TLMap3Entry_t   **ppme;
    TLMap3Entry_t   *pme;
    TLMap3Bucket_t  *pb;

    h = hash((const unsigned char *)pkey, strlen(pkey), 0) % pm->nbuckets;
    pb = &(pm->buckets[h]);

    for (ppme = &(pb->entry); (*ppme) != NULL; ppme = &((*ppme)->next)) {
	pme = *ppme;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT_FLASH, PLAT_LOG_LEVEL_TRACE,
                     "\nHash Table Delete ppme %p pb %p h %lu\n",
                     ppme, &(pb->entry), h);
        if ((pme->keylen == keylen) &&
            (strcmp((const char *)pme->key, (const char *)pkey) == 0)) 
	{
            *ppme = pme->next;
	    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT_FLASH, PLAT_LOG_LEVEL_TRACE,
                         "\nHash Table Delete ppme %p key %s contents %p\n",
                         ppme, pkey, *ppme);
            plat_free(pme->key);
	    if (pme->contents != NULL) {
		plat_free(pme->contents);
	    }
            plat_free(pme);
            return (0);
        }
    }
    return (1);
}

TLMap3Entry_t *TLMap3NextEnumeration(TLMap3_t *pm, uint64_t last_bucket_no, uint64_t last_seqno, uint64_t *pnext_bucket_no)
{
    uint64_t          h;
    uint64_t          seqno_picked;
    TLMap3Entry_t    *pme;
    TLMap3Entry_t    *pme_picked;
    TLMap3Bucket_t   *pb;
    int               foundflag;

    pme          = NULL;
    pme_picked   = NULL;
    seqno_picked = UINT64_MAX;
    foundflag    = 0;
    for (h = last_bucket_no; h < pm->nbuckets; h++) {
	pb = &(pm->buckets[h]);
	/* pick smalled seqno that is greater than last_seqno */
	for (pme = pb->entry; pme != NULL; pme = pme->next) {
	    if ((pme->seqno > last_seqno) &&
	        (pme->seqno < seqno_picked))
	    {
	        pme_picked   = pme;
		seqno_picked = pme->seqno;
		foundflag    = 1;
	    }
	}
	if (foundflag) {
	    break;
	}
	last_seqno = 0; // reset seqno when we go to the next bucket
    }
    *pnext_bucket_no = h;
    return(pme_picked);
}
