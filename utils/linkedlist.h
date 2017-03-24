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
 * File:   utils/linkedlist.h
 * Author: Darpan Dinker
 *
 * Created on February 6, 2008, 11:16 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: linkedlist.h 309 2008-02-20 23:08:19Z darpan $
 */

#ifndef _LINKEDLIST_H
#define _LINKEDLIST_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common/sdftypes.h"
#include "fth/fthSpinLock.h" // for spin locks

#define _SDF_HASHMAP_USING_BUCKET_SPINLOCKS

/** ListEntry for each {key, value} stored in the doubly linked-list */
struct ListEntry {
    /** pointer to next entry in the list */
    struct ListEntry *Next;
    /** pointer to previous entry in the list */
    struct ListEntry *Previous;
    /** pointer to the string that is the key for the object stored */
    const char *key;
    /** pointer to the object stored for the corresponding key */
    void *value;
    /** Length of the key, not counting the null terminator. What strlen would report */
    uint16_t keyLen;
    /** Hashcode for the cache object*/
    uint64_t hashcode;
};

typedef struct ListEntry *ListPtr;

struct LinkedListInstance {
    ListPtr Head;
    ListPtr Tail;
    ListPtr Curr;
    uint32_t size;
#ifdef _SDF_HASHMAP_USING_BUCKET_SPINLOCKS
    /** Spin locks (called only from Fth) per bucket, for increased concurrency */
    fthSpinLock_t bucketSpinLock; // size = 8B
#endif
};

typedef struct LinkedListInstance *LinkedList;

// static void LinkedList_debug(const char *msg);

ListPtr List_create(const char *key, void* value, uint16_t keyLen);
LinkedList LinkedList_create();
void LinkedList_destroy(LinkedList ll);

/**
 * @brief Put object in the linked list corresponding to the supplied key.
 *
 * @param ll <IN> linked list to operate on (not required when we move to C+)
 * @param key <IN> string identifier for the object to PUT
 * @param value <IN> pointer to object to PUT
 * @return SDF_TRUE on success, SDF_FALSE for duplicate keys
 */
__inline__ SDF_boolean_t LinkedList_put(LinkedList ll, const char *key, void* value, uint16_t keyLen);

__inline__ void* LinkedList_replace(LinkedList ll, const char *key, void* value, uint16_t keyLen);

__inline__ void* LinkedList_remove(LinkedList ll, const char *key, uint16_t keyLen);

/**
 * @brief Get object from the linked list for the desired key.
 *
 * @param ll <IN> linked list to operate on (not required when we move to C+)
 * @param key <IN> string identifier for the object to GET
 * @return <NULLABLE> null if key is not found, otherwise pointer to the object for the key
 */
__inline__ void* LinkedList_get(LinkedList ll, const char *key, uint16_t keyLen);

__inline__ uint32_t LinkedList_getSize(LinkedList ll);
__inline__ SDF_boolean_t LinkedList_isEmpty(LinkedList ll);
void LinkedList_printElements(LinkedList ll);

#ifdef __cplusplus
}
#endif

#endif /* _LINKEDLIST_H */
