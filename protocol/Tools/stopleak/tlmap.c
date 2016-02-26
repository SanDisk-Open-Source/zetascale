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
 * File:   tlmap.c
 * Author: Brian O'Krafka
 *
 * Created on September 11, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tlmap.c 308 2008-02-20 22:34:58Z tomr $
 */

#define _TLMAP_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "tlmap.h"
#include "hash.h"

void SDFTLMapInit(SDFTLMap_t *pm, uint64_t nbuckets, 
     int (*print_fn)(SDFTLMapEntry_t *pce, char *sout, int max_len))
{
    uint64_t          i;

    pm->nbuckets      = nbuckets;
    pm->print_fn      = print_fn;
    pm->buckets       = malloc(nbuckets*(sizeof(SDFTLMapBucket_t)));

    for (i=0; i<nbuckets; i++) {
	pm->buckets[i].entry = NULL;
    }
}

SDFTLMapEntry_t *SDFTLMapGetCreate(SDFTLMap_t *pm, char *pkey)
{
    int                keylen;
    uint64_t           h;
    SDFTLMapEntry_t   *pme;
    SDFTLMapBucket_t  *pb;

    keylen = strlen(pkey);
    h = hash((const unsigned char *) pkey, strlen(pkey), 0) % pm->nbuckets;
    pb = &(pm->buckets[h]);

    for (pme = pb->entry; pme != NULL; pme = pme->next) {
	if ((pme->keylen == keylen) && 
	    (strcmp((const char *) pme->key, (const char *) pkey) == 0))
	{
	    break;
	}
    }

    if (pme == NULL) {

        /* Create a new entry. */

	pme = (SDFTLMapEntry_t *) malloc(sizeof(SDFTLMapEntry_t));
	if (pme == NULL) {
	    (void) fprintf(stderr, "Could not allocate a thread-local map entry.");
	    exit(1);
	}

	pme->contents = NULL;
	pme->key      = (char *) malloc(keylen+1);
	strcpy(pme->key, pkey);
	pme->keylen   = keylen;

	/* put myself on the bucket list */
        pme->next = pb->entry;
        pb->entry = pme;
    }

    return(pme);
}

void SDFTLMapEnum(SDFTLMap_t *pm)
{
    SDFTLMapEntry_t    *pme;
    SDFTLMapBucket_t   *pb;
    uint64_t            nb;

    for (nb=0; nb < pm->nbuckets; nb++) {
	pb = &(pm->buckets[nb]);
	pme = pb->entry;
	if (pme != NULL) {
	    break;
	}
    }
    pm->enum_bucket = nb;
    pm->enum_entry  = pme;
}

SDFTLMapEntry_t *SDFTLMapNextEnum(SDFTLMap_t *pm) 
{
    SDFTLMapEntry_t    *pme_return, *pme;
    SDFTLMapBucket_t   *pb;
    uint64_t            nb;

    if (pm->enum_entry == NULL) {
        return(NULL);
    }
    pme_return = pm->enum_entry;

    if (pme_return->next != NULL) {
	pm->enum_entry = pme_return->next;
	return(pme_return);
    }

    pme = NULL;
    for (nb=pm->enum_bucket + 1; nb < pm->nbuckets; nb++) {
	pb = &(pm->buckets[nb]);
	pme = pb->entry;
	if (pme != NULL) {
	    break;
	}
    }
    pm->enum_bucket = nb;
    pm->enum_entry  = pme;

    return(pme_return);
}






