/*
 * File:   tlmap4.c
 * Author: Brian O'Krafka
 *
 * Created on September 11, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tlmap4.c 308 2008-02-20 22:34:58Z briano $
 */

#define _TLMAP4_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <string.h>
#include "platform/logging.h"
#include "platform/stdlib.h"
#include "tlmap4.h"
#include "utils/hash.h"
#include "protocol/protocol_alloc.h"

void SDFTLMap4Init(SDFTLMap4_t *pm, uint64_t nbuckets, 
     int (*print_fn)(SDFTLMap4Entry_t *pce, char *sout, int max_len))
{
    uint64_t          i;

    pm->nbuckets      = nbuckets;
    pm->print_fn      = print_fn;
    pm->buckets       = proto_plat_alloc_arena(nbuckets*(sizeof(SDFTLMap4Bucket_t)), PLAT_SHMEM_ARENA_CACHE_THREAD);
    #ifdef MALLOC_TRACE
        UTMallocTrace("SDFTLMap4Init", TRUE, FALSE, FALSE, (void *) pm->buckets, nbuckets*sizeof(SDFTLMap4Bucket_t));
    #endif // MALLOC_TRACE
    if (pm->buckets == NULL) {
	plat_log_msg(21288, PLAT_LOG_CAT_SDF_CC, PLAT_LOG_LEVEL_FATAL,
	     "Could not allocate thread-local map buckets.");
	plat_abort();
    }

    for (i=0; i<nbuckets; i++) {
	pm->buckets[i].entry = NULL;
    }
}

SDFTLMap4Entry_t *SDFTLMap4Create(SDFTLMap4_t *pm, uint64_t key)
{
    uint64_t           h;
    SDFTLMap4Entry_t   *pme;
    SDFTLMap4Bucket_t  *pb;

    h = hashk((const unsigned char *) &key, sizeof(uint64_t), 0) %
        pm->nbuckets;
    pb = &(pm->buckets[h]);

    for (pme = pb->entry; pme != NULL; pme = pme->next) {
	if (pme->key == key) {
	    break;
	}
    }

    if (pme == NULL) {

        /* Create a new entry. */

	pme = (SDFTLMap4Entry_t *) proto_plat_alloc_arena(sizeof(SDFTLMap4Entry_t), PLAT_SHMEM_ARENA_CACHE_THREAD);
	#ifdef MALLOC_TRACE
	    UTMallocTrace("SDFTLMap4GetCreate: entry", FALSE, FALSE, FALSE, (void *) pme, sizeof(SDFTLMap4Entry_t));
	#endif // MALLOC_TRACE
	if (pme == NULL) {
	    plat_log_msg(21289, PLAT_LOG_CAT_SDF_CC, PLAT_LOG_LEVEL_INFO,
		 "Could not allocate a thread-local map entry.");
	    return(NULL);
	}

	pme->contents = 0;
	pme->key      = key;

	/* put myself on the bucket list */
        pme->next = pb->entry;
        pb->entry = pme;
    }

    return(pme);
}

SDFTLMap4Entry_t *SDFTLMap4Get(SDFTLMap4_t *pm, uint64_t key)
{
    uint64_t           h;
    SDFTLMap4Entry_t   *pme;
    SDFTLMap4Bucket_t  *pb;

    h = hashk((const unsigned char *) &key, sizeof(uint64_t), 0) %
        pm->nbuckets;
    pb = &(pm->buckets[h]);

    for (pme = pb->entry; pme != NULL; pme = pme->next) {
	if (pme->key == key) {
	    break;
	}
    }

    return(pme);
}

