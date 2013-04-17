/*
 * File:   fdf_tlmap_internal.h
 * Author: Brian O'Krafka
 *
 * Created on September 11, 2008
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 * $Id: tlmap.h 308 2008-02-20 22:34:58Z tomr $
 */

#ifndef _FDF_TLMAP_INTERNAL_H
#define _FDF_TLMAP_INTERNAL_H

#define N_ENTRIES_TO_MALLOC    100
#define N_ITERATORS_TO_MALLOC  100

struct FDFTLMapBucket;

typedef struct FDFTLMapEntry {
    char                  *contents;
    uint64_t               datalen;
    int32_t                refcnt;
    char                  *key;
    uint32_t               keylen;
    struct FDFTLMapEntry  *next;
    struct FDFTLMapEntry  *next_lru;
    struct FDFTLMapEntry  *prev_lru;
    struct FDFTLMapBucket *bucket;
} FDFTLMapEntry_t;

typedef struct FDFTLMapBucket {
    struct FDFTLMapEntry *entry;
} FDFTLMapBucket_t;

typedef struct FDFTLIterator {
    uint64_t                enum_bucket;
    FDFTLMapEntry_t        *enum_entry;
    struct FDFTLIterator   *next;
} FDFTLIterator_t;

typedef struct FDFTLMap {
    uint64_t          nbuckets;
    uint64_t          max_entries;
    uint64_t          n_entries;
    char              use_locks;
    FDFTLMapBucket_t *buckets;
    pthread_mutex_t   mutex;
    pthread_mutex_t   enum_mutex;
    FDFTLMapEntry_t  *lru_head;
    FDFTLMapEntry_t  *lru_tail;
    void              (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen);
    void             *replacement_callback_data;
    uint32_t          NEntries;
    uint32_t          NUsedEntries;
    FDFTLMapEntry_t  *FreeEntries;
    uint32_t          NIterators;
    uint32_t          NUsedIterators;
    struct FDFTLIterator *FreeIterators;
} FDFTLMap_t;

#endif /* _FDF_TLMAP_INTERNAL_H */
