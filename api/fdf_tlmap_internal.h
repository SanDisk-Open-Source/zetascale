/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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

#ifndef _ZS_TLMAP_INTERNAL_H
#define _ZS_TLMAP_INTERNAL_H

#define N_ENTRIES_TO_MALLOC    100
#define N_ITERATORS_TO_MALLOC  100

struct ZSTLMapBucket;

typedef struct ZSTLMapEntry {
    char                  *contents;
    uint64_t               datalen;
    int32_t                refcnt;
    char                  *key;
    uint32_t               keylen;
    struct ZSTLMapEntry  *next;
    struct ZSTLMapEntry  *next_lru;
    struct ZSTLMapEntry  *prev_lru;
    struct ZSTLMapBucket *bucket;
} ZSTLMapEntry_t;

typedef struct ZSTLMapBucket {
    struct ZSTLMapEntry *entry;
} ZSTLMapBucket_t;

typedef struct ZSTLIterator {
    uint64_t                enum_bucket;
    ZSTLMapEntry_t        *enum_entry;
    struct ZSTLIterator   *next;
} ZSTLIterator_t;

typedef struct ZSTLMap {
    uint64_t          nbuckets;
    uint64_t          max_entries;
    uint64_t          n_entries;
    char              use_locks;
    ZSTLMapBucket_t *buckets;
    pthread_mutex_t   mutex;
    pthread_mutex_t   enum_mutex;
    ZSTLMapEntry_t  *lru_head;
    ZSTLMapEntry_t  *lru_tail;
    void              (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen);
    void             *replacement_callback_data;
    uint32_t          NEntries;
    uint32_t          NUsedEntries;
    ZSTLMapEntry_t  *FreeEntries;
    uint32_t          NIterators;
    uint32_t          NUsedIterators;
    struct ZSTLIterator *FreeIterators;
} ZSTLMap_t;

#endif /* _ZS_TLMAP_INTERNAL_H */
