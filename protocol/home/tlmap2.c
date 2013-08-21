/*
 * File:   tlmap2.c
 * Author: Brian O'Krafka
 *
 * Created on September 11, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tlmap.c 308 2008-02-20 22:34:58Z tomr $
 */

#define _TLMAP2_C

#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <string.h>
#include "platform/logging.h"
#include "platform/stdlib.h"
#include "tlmap2.h"
#include "utils/hash.h"
#include "protocol/protocol_alloc.h"

#define LOG_CAT_FLASH PLAT_LOG_CAT_SDF_PROT_FLASH

void SDFTLMap2Init(SDFTLMap2_t *pm, uint64_t nbuckets, 
     int (*print_fn)(SDFTLMap2Entry_t *pce, char *sout, int max_len))
{
    uint64_t          i;

    pm->nbuckets      = nbuckets;
    pm->print_fn      = print_fn;
    pm->buckets       = proto_plat_alloc_arena(nbuckets*(sizeof(SDFTLMap2Bucket_t)), PLAT_SHMEM_ARENA_CACHE_THREAD);
    #ifdef MALLOC_TRACE
        UTMallocTrace("SDFTLMap2Init", TRUE, FALSE, FALSE, (void *) pm->buckets, nbuckets*sizeof(SDFTLMap2Bucket_t));
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

SDFTLMap2Entry_t *SDFTLMap2Create(SDFTLMap2_t *pm, uint64_t key)
{
    uint64_t           h;
    SDFTLMap2Entry_t   *pme;
    SDFTLMap2Bucket_t  *pb;

    h = hashb((const unsigned char *) &key, sizeof(uint64_t), 0) %
        pm->nbuckets;
    pb = &(pm->buckets[h]);

    for (pme = pb->entry; pme != NULL; pme = pme->next) {
	if (pme->key == key) {
	    break;
	}
    }

    if (pme == NULL) {

        /* Create a new entry. */

	pme = (SDFTLMap2Entry_t *) proto_plat_alloc_arena(sizeof(SDFTLMap2Entry_t), PLAT_SHMEM_ARENA_CACHE_THREAD);
	#ifdef MALLOC_TRACE
	    UTMallocTrace("SDFTLMap2GetCreate: entry", FALSE, FALSE, FALSE, (void *) pme, sizeof(SDFTLMap2Entry_t));
	#endif // MALLOC_TRACE
	if (pme == NULL) {
	    plat_log_msg(21289, PLAT_LOG_CAT_SDF_CC, PLAT_LOG_LEVEL_ERROR,
		 "Could not allocate a thread-local map entry.");
	    return(NULL);
	}

	pme->contents = NULL;
	pme->key      = key;

	/* put myself on the bucket list */
        pme->next = pb->entry;
        pb->entry = pme;
    } else {
        pme = NULL; // entry already exists!
    }

    return(pme);
}

SDFTLMap2Entry_t *SDFTLMap2Get(SDFTLMap2_t *pm, uint64_t key)
{
    uint64_t           h;
    SDFTLMap2Entry_t   *pme;
    SDFTLMap2Bucket_t  *pb;

    h = hashb((const unsigned char *) &key, sizeof(uint64_t), 0) %
        pm->nbuckets;
    pb = &(pm->buckets[h]);

    for (pme = pb->entry; pme != NULL; pme = pme->next) {
	if (pme->key == key) {
	    break;
	}
    }

    return(pme);
}

/*   Return 0 if succeeds, 1 if object doesn't exist.
 *   TLMap2Delete does nothing with the contents, even if they are non-NULL.
 */
int SDFTLMap2Delete(SDFTLMap2_t *pm, uint64_t key)
{
    uint64_t             h;
    SDFTLMap2Entry_t   **ppme;
    SDFTLMap2Entry_t    *pme;
    SDFTLMap2Bucket_t   *pb;

    h = hashb((const unsigned char *) &key, sizeof(uint64_t), 0) %
        pm->nbuckets;
    pb = &(pm->buckets[h]);

    for (ppme = &(pb->entry); (*ppme) != NULL; ppme = &((*ppme)->next)) {
	pme = *ppme;
        plat_log_msg(21334, LOG_CAT_FLASH, PLAT_LOG_LEVEL_TRACE,
                     "\nHash Table Delete ppme %p pb %p h %lu\n",
                     ppme, &(pb->entry), h);
	if (pme->key == key) {
            *ppme = pme->next;
	    plat_log_msg(21335, LOG_CAT_FLASH, PLAT_LOG_LEVEL_TRACE,
                         "\nHash Table Delete ppme %p key %"PRIu64" contents %p\n",
                         ppme, key, *ppme);
            plat_free(pme);
            return (0);
        }
    }
    return (1);
}

void SDFTLMap2Destroy(SDFTLMap2_t *pm)
{
    uint64_t            h;
    SDFTLMap2Entry_t    *pme, *pme_next;
    SDFTLMap2Bucket_t   *pb;

	for (h = 0; h < pm->nbuckets; h++) {
		pb = &(pm->buckets[h]);

		pme = pb->entry;
		while (pme != NULL) {
			pme_next = pme->next;
			free(pme);
			pme = pme_next;
		}
		pb->entry = NULL;
	}
#if 0
		for (ppme = &(pb->entry); (*ppme) != NULL; ppme = &((*ppme)->next)) {
			pme = *ppme;
            *ppme = pme->next;
			plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT_FLASH, PLAT_LOG_LEVEL_TRACE,
                         "\nHash Table Delete ppme %p key %"PRIu64" contents %p\n",
                         ppme, pme->key, *ppme);
            plat_free(pme);
        }
    }
#endif
}



