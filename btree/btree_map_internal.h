/*
 * File:   btree_map_internal.h
 * Author: Brian O'Krafka
 *
 * Created on September 11, 2008
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 * $Id: tlmap.h 308 2008-02-20 22:34:58Z tomr $
 */

#ifndef _BTREE_MAP_INTERNAL_H
#define _BTREE_MAP_INTERNAL_H

#define N_ENTRIES_TO_MALLOC    100
#define N_ITERATORS_TO_MALLOC  100

struct MapBucket;

typedef struct MapEntry {
    char                  *contents;
    uint64_t               datalen;
    int32_t                refcnt;
    char                  *key;
    uint32_t               keylen;
    struct MapEntry  *next;
    struct MapEntry  *next_lru;
    struct MapEntry  *prev_lru;
    struct MapBucket *bucket;
} MapEntry_t;

typedef struct MapBucket {
    struct MapEntry *entry;
} MapBucket_t;

typedef struct Iterator {
    uint64_t                enum_bucket;
    MapEntry_t        *enum_entry;
    struct Iterator   *next;
} Iterator_t;

typedef struct Map {
    uint64_t          nbuckets;
    uint64_t          max_entries;
    uint64_t          n_entries;
    char              use_locks;
    MapBucket_t *buckets;
    pthread_mutex_t   mutex;
    pthread_mutex_t   enum_mutex;
    MapEntry_t  *lru_head;
    MapEntry_t  *lru_tail;
    void              (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen);
    void             *replacement_callback_data;
    uint32_t          NEntries;
    uint32_t          NUsedEntries;
    MapEntry_t  *FreeEntries;
    uint32_t          NIterators;
    uint32_t          NUsedIterators;
    struct Iterator *FreeIterators;
} Map_t;

#endif /* _BTREE_MAP_INTERNAL_H */
