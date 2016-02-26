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
    char					ref;
	uint8_t					deleted:1;
	uint8_t					rawobj:1;
	uint8_t					reserved:6;
    int32_t					refcnt;
    uint32_t				keylen;
    uint64_t				datalen;
	uint64_t				cguid;
    char					*key;
    char					*contents;
    struct MapEntry			*next;
    struct MapEntry			*next_lru;
    struct MapEntry			*prev_lru;
    struct MapBucket		*bucket;
} MapEntry_t;

typedef struct MapEntryBlock {
	struct MapEntryBlock	*next;
	struct MapEntry			*e;
} MapEntryBlock_t;

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
    pthread_rwlock_t   lock;
    //pthread_mutex_t   enum_mutex;
    MapEntry_t  *lru_head;
    MapEntry_t  *lru_tail;
    MapEntry_t  *clock_hand;
    void              (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen);
    void             *replacement_callback_data;
    uint32_t          NEntries;
    uint32_t          NUsedEntries;
    MapEntry_t  *FreeEntries;
    uint32_t          NIterators;
    uint32_t          NUsedIterators;
	MapEntryBlock_t		*EntryBlocks;
    struct Iterator *FreeIterators;
    int               extra_pme;
} Map_t;

#endif /* _BTREE_MAP_INTERNAL_H */
