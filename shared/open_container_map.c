/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdfclient/open_container_map.c
 * Author: Darpan Dinker
 *
 * Created on February 6, 2008, 10:41 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: open_container_map.c 10527 2009-12-12 01:55:08Z drew $
 */

#include "open_container_map.h"
#include "platform/shmem.h"
#include "platform/string.h"
#include <pthread.h>

#define FALSE 0
#define TRUE 1

struct ContainerMapBucket;
PLAT_SP(ContainerMapBucket_sp, struct ContainerMapBucket);

struct ContainerMap;
PLAT_SP(ContainerMap_sp, struct ContainerMap);

struct ContainerMapBucket {
    SDF_CONTAINER_PARENT ptr;
};

struct ContainerMap {
    int DEBUG;
    unsigned numBuckets;
    uint32_t numElements;
    ContainerMapBucket_sp_t allBuckets;
    volatile unsigned long stats_puts, stats_gets, stats_removes;
#ifdef _MULTI_THREADED

#ifdef _BUCKET_LOCKS
    pthread_mutex_t *bucketLocks; // 1 lock per bucket for increased concurrency
#endif

#ifndef _BUCKET_LOCKS
    pthread_mutex_t mapLock;
#endif

#endif
};

// =====================================================================================================================
PLAT_SP_IMPL(ContainerMapBucket_sp, struct ContainerMapBucket);
PLAT_SP_IMPL(ContainerMap_sp, struct ContainerMap);

ContainerMap_sp_t
sdf_cmap_create(unsigned numBuckets, int debug)
{
    ContainerMap_sp_t instance = ContainerMap_sp_null;
    struct ContainerMapBucket *bucketPtr = NULL;
    struct ContainerMap *p = NULL;
    int i;

    instance = plat_shmem_alloc(ContainerMap_sp);

    if (!ContainerMap_sp_is_null(instance)) {
        p = ContainerMap_sp_rwref(&p, instance);
        p->DEBUG = debug;
        p->allBuckets = plat_shmem_array_alloc(ContainerMapBucket_sp, numBuckets);
        
        ContainerMapBucket_sp_rwref(&bucketPtr, p->allBuckets);
        for (i = 0; i < numBuckets; i++) {
            (bucketPtr[i]).ptr = containerParentNull;
        }
        ContainerMapBucket_sp_rwrelease(&bucketPtr);
        
        p->numBuckets = numBuckets;
        p->numElements = 0;
        p->stats_gets = p->stats_puts = p->stats_removes = 0;
        ContainerMap_sp_rwrelease(&p);
    } else {
        plat_log_msg(20866, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_FATAL, "Not enough shared memory, plat_shmem_alloc() failed.");
        plat_assert_always(0);
    }

    return (instance);
}

void
sdf_cmap_bucket_list_deleteList(SDF_CONTAINER_PARENT entry)
 {
    SDF_CONTAINER_PARENT temp = entry;
    local_SDF_CONTAINER_PARENT p = NULL;

    while (!isContainerParentNull(entry)) {
        getLocalContainerParent(&p, entry);
        temp = p->bucket_next;
        releaseLocalContainerParent(&p);
        freeContainerParent(entry);
        entry = temp;
    }
}

void
sdf_cmap_bucket_destroy(ContainerMapBucket_sp_t map, unsigned numBuckets)
{
    struct ContainerMapBucket *bucketPtr = NULL;
    int i;

    ContainerMapBucket_sp_rwref(&bucketPtr, map);
    for (i = 0; i < numBuckets; i++) {
        sdf_cmap_bucket_list_deleteList((bucketPtr[i]).ptr);
    }
    ContainerMapBucket_sp_rwrelease(&bucketPtr);
    plat_shmem_array_free(ContainerMapBucket_sp, map, numBuckets);

    bucketPtr = NULL;
}

void
sdf_cmap_destroy(ContainerMap_sp_t map)
{
    struct ContainerMap *p = NULL;

    plat_assert(!ContainerMap_sp_is_null(map));
    p = ContainerMap_sp_rwref(&p, map);

    sdf_cmap_bucket_destroy(p->allBuckets, p->numBuckets);

    // {{ Unnecessary stuff
    p->allBuckets = ContainerMapBucket_sp_null;
    p->numBuckets = 0;
    p->numElements = 0;
    p->stats_gets = p->stats_puts = p->stats_removes = 0;
    // }}

    ContainerMap_sp_rwrelease(&p);
    plat_shmem_free(ContainerMap_sp, map);
    map = ContainerMap_sp_null;
}

int
internal_sdf_cmap_bucket_list_insertAfter(SDF_CONTAINER_PARENT entry, SDF_CONTAINER_PARENT newEntry)
{
    local_SDF_CONTAINER_PARENT tempprev = NULL, tempnew = NULL;
    SDF_CONTAINER_PARENT nextEntry = containerParentNull;
    int ret = 0;

    if (!isContainerParentNull(entry) && !isContainerParentNull(newEntry)) {
        getLocalContainerParent(&tempnew, newEntry);
        getLocalContainerParent(&tempprev, entry);
        nextEntry = tempprev->bucket_next;

        tempprev->bucket_next = newEntry;
        tempnew->bucket_next = nextEntry;

        releaseLocalContainerParent(&tempprev);
        releaseLocalContainerParent(&tempnew);

        ret = 1;
    }

    return (ret);
}

int
internal_sdf_cmap_bucket_list_keycmp(local_SDF_CONTAINER_PARENT p, const char* path)
 {
    int found = 0;

    // {{ checks
    if (0 == strcmp(p->dir, path)) { // realistically check against p->dir + '/' + p->name
        found = 1;
    }
    // }}

    return (found);
}

SDF_CONTAINER_PARENT
sdf_cmap_bucket_list_get(SDF_CONTAINER_PARENT head, const char* path)
  {
    SDF_CONTAINER_PARENT ret = containerParentNull;
    SDF_CONTAINER_PARENT entry = head;
    local_SDF_CONTAINER_PARENT p = NULL;
    int found = 0;

    while (!isContainerParentNull(entry) && !found) {
        getLocalContainerParent(&p, entry);
        if ((found = internal_sdf_cmap_bucket_list_keycmp(p, path))) {
            ret = entry;
        }
        entry = p->bucket_next;
        releaseLocalContainerParent(&p);
    }

    return (ret);
}

int
sdf_cmap_bucket_list_add(SDF_CONTAINER_PARENT head, SDF_CONTAINER_PARENT newEntry, const char* path)
{
    SDF_CONTAINER_PARENT entry = containerParentNull;
    int ret = 0;

    entry = sdf_cmap_bucket_list_get(head, path);
    if (isContainerParentNull(entry)) {
        ret = internal_sdf_cmap_bucket_list_insertAfter(head, newEntry);
    }

    return (ret);
}

int
internal_sdf_cmap_bucket_list_remove(SDF_CONTAINER_PARENT before, SDF_CONTAINER_PARENT entry)
{
    SDF_CONTAINER_PARENT after = containerParentNull;
    local_SDF_CONTAINER_PARENT pr = NULL;
    local_SDF_CONTAINER_PARENT pw = NULL;
    int ret = 0;

    if (!isContainerParentNull(before) && !isContainerParentNull(entry)) {
        getLocalContainerParent(&pr, entry);
        after = pr->bucket_next;
        releaseLocalContainerParent(&pr);

        getLocalContainerParent(&pw, before);
        pw->bucket_next = after;
        releaseLocalContainerParent(&pw);
        ret = 1;
    }

    return (ret);
}

SDF_CONTAINER_PARENT
sdf_cmap_bucket_list_remove(SDF_CONTAINER_PARENT head, const char *path)
{
    SDF_CONTAINER_PARENT ret = containerParentNull;
    SDF_CONTAINER_PARENT prev = head;
    SDF_CONTAINER_PARENT entry = head;
    local_SDF_CONTAINER_PARENT p = NULL;
    int iter = 0;
    int found = 0;

    for (iter = 0; (!isContainerParentNull(entry) && !found); iter++) {
        getLocalContainerParent(&p, entry);
        if ((found = internal_sdf_cmap_bucket_list_keycmp(p, path))) {
            ret = entry;
        } else {
            prev = entry;
        }
        entry = p->bucket_next;
        releaseLocalContainerParent(&p);
    }

    if (found) {
        if (1 == iter) { // Remove 1st element, prev not reliable
            plat_assert(0 == 1); // should have called sdf_cmap_bucket_list_checkIfFirstRemoval_ProvideNext
        } else { // Remove 2+ element, prev should be set
            if (!internal_sdf_cmap_bucket_list_remove(prev, ret)) {
                plat_assert(0 == 1);
            }
        }
    }

    return (ret);
}

/**
 * Check if the very first entry is the one that needs to be removed. If yes,
 * then return the next entry.
 */
SDF_CONTAINER_PARENT
sdf_cmap_bucket_list_checkIfFirstRemoval_ProvideNext(SDF_CONTAINER_PARENT head, const char *path, int *found)
{
    SDF_CONTAINER_PARENT ret = containerParentNull;
    SDF_CONTAINER_PARENT entry = head;
    local_SDF_CONTAINER_PARENT p = NULL;

    *found = 0;
    if (!isContainerParentNull(entry)) {
        getLocalContainerParent(&p, entry);
        if (internal_sdf_cmap_bucket_list_keycmp(p, path)) {
            ret = p->bucket_next;
            *found = 1;
        }
        releaseLocalContainerParent(&p);
    }

    return (ret);
}
// =====================================================================================================================
SDF_CONTAINER_PARENT ContainerMap_get(ContainerMap_sp_t containerMap, const char *path)
{
    const struct ContainerMap *map = NULL;
    const struct ContainerMapBucket *bucketPtr = NULL;
    SDF_CONTAINER_PARENT ret = containerParentNull;
    unsigned bucketno;

    ContainerMap_sp_rref(&map, containerMap);
    bucketno = HashMap_getBucket(map->numBuckets, path);
    ContainerMapBucket_sp_rref(&bucketPtr, map->allBuckets);
    ContainerMap_sp_rrelease(&map);
    
    if (!isContainerParentNull((bucketPtr[bucketno]).ptr)) {
        ret = sdf_cmap_bucket_list_get(((bucketPtr[bucketno]).ptr), path);
    }
    ContainerMapBucket_sp_rrelease(&bucketPtr);

    return (ret);
}

int ContainerMap_put(ContainerMap_sp_t containerMap, const char *path, SDF_CONTAINER_PARENT parent)
{
    const struct ContainerMap *map = NULL;
    struct ContainerMapBucket *bucketPtr = NULL;
    unsigned bucketno;
    int ret = 0;

    ContainerMap_sp_rref(&map, containerMap);
    bucketno = HashMap_getBucket(map->numBuckets, path);
    ContainerMapBucket_sp_rwref(&bucketPtr, map->allBuckets);
    ContainerMap_sp_rrelease(&map);
    if (!isContainerParentNull((bucketPtr[bucketno]).ptr)) {
        ret = sdf_cmap_bucket_list_add(((bucketPtr[bucketno]).ptr), parent, path);
    } else {
        (bucketPtr[bucketno]).ptr = parent;
        ret = 1;
    }
    ContainerMapBucket_sp_rwrelease(&bucketPtr);

    return (ret);
}

SDF_CONTAINER_PARENT ContainerMap_remove(ContainerMap_sp_t containerMap, const char *path)
{
    const struct ContainerMap *map = NULL;
    struct ContainerMapBucket *bucketPtr = NULL;
    SDF_CONTAINER_PARENT ret = containerParentNull;
    SDF_CONTAINER_PARENT next = containerParentNull;
    unsigned bucketno;
    int found = 0;

    ContainerMap_sp_rref(&map, containerMap);
    bucketno = HashMap_getBucket(map->numBuckets, path);
    ContainerMapBucket_sp_rwref(&bucketPtr, map->allBuckets);
    ContainerMap_sp_rrelease(&map);
    if (!isContainerParentNull((bucketPtr[bucketno]).ptr)) {
        next = sdf_cmap_bucket_list_checkIfFirstRemoval_ProvideNext(((bucketPtr[bucketno]).ptr), path, &found);
        if (!found) { // 1st entry does not need removal
            ret = sdf_cmap_bucket_list_remove(((bucketPtr[bucketno]).ptr), path);
        } else { // 1st entry needs to be removed
            ret = (bucketPtr[bucketno]).ptr;
            (bucketPtr[bucketno]).ptr = next;
        }
    }
    ContainerMapBucket_sp_rwrelease(&bucketPtr);

    return (ret);
}
// =====================================================================================================================
static ContainerMap_sp_t containerMap;
        
static ContainerMap_sp_t getMap()
{
    if (ContainerMap_sp_is_null(containerMap)) {
        // TODO protect for multiple-threads
        containerMap = sdf_cmap_create(127, FALSE);
    }

    return (containerMap);
}

int
cmap_init()
{
    int ret = -1;

    if (!ContainerMap_sp_is_null(containerMap = sdf_cmap_create(127, FALSE))) {
        ret = 0;
    }
    
    return (ret);
}

void
cmap_reset()
{
    sdf_cmap_destroy(containerMap);
}

SDF_CONTAINER_PARENT
containerMap_getParent(const char *path)
{
    SDF_CONTAINER_PARENT parent = containerParentNull;

    getMap();
    parent = ContainerMap_get(containerMap, path);
    if (!isContainerParentNull(parent)) {
        _sdf_print_parent_container_structure(parent); // DEBUG
    }

    return (parent);
}

int
containerMap_addParent(const char *path, SDF_CONTAINER_PARENT parent)
{
    getMap();
    return (ContainerMap_put(containerMap, path, parent));
}

SDF_CONTAINER_PARENT
containerMap_removeParent(const char *path)
{
    SDF_CONTAINER_PARENT parent = containerParentNull;
    
    getMap();
    parent = ContainerMap_remove(containerMap, path);

    return (parent);
}
