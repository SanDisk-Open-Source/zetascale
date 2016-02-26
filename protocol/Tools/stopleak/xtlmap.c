/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   xtlmap.c
 * Author: Brian O'Krafka
 *
 * Created on October 16, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tlmap.c 308 2008-02-20 22:34:58Z briano $
 */

#define _XTLMAP_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "xtlmap.h"
#include "hash.h"

void SDFxTLMapInit(SDFxTLMap_t *pm, uint64_t nbuckets, 
     int (*print_fn)(SDFxTLMapEntry_t *pce, char *sout, int max_len))
{
    uint64_t          i;

    pm->nbuckets      = nbuckets;
    pm->print_fn      = print_fn;
    pm->buckets       = malloc(nbuckets*(sizeof(SDFxTLMapBucket_t)));

    for (i=0; i<nbuckets; i++) {
	pm->buckets[i].entry = NULL;
    }
}

SDFxTLMapEntry_t *SDFxTLMapGetCreate(SDFxTLMap_t *pm, uint64_t key)
{
    uint64_t           h;
    SDFxTLMapEntry_t   *pme;
    SDFxTLMapBucket_t  *pb;

    h = hash((const unsigned char *) &key, sizeof(uint64_t), 0) % pm->nbuckets;
    pb = &(pm->buckets[h]);

    for (pme = pb->entry; pme != NULL; pme = pme->next) {
	if (pme->key == key) {
	    break;
	}
    }

    if (pme == NULL) {

        /* Create a new entry. */

	pme = (SDFxTLMapEntry_t *) malloc(sizeof(SDFxTLMapEntry_t));
	if (pme == NULL) {
	    (void) fprintf(stderr, "Could not allocate a thread-local map entry.");
	    exit(1);
	}

	pme->contents = NULL;
	pme->key      = key;

	/* put myself on the bucket list */
        pme->next = pb->entry;
        pb->entry = pme;
    }

    return(pme);
}

