/*
 * File:   utils/newhashmap.c
 * Author: Darpan Dinker
 *
 * Created on November 12, 2008, 11:05 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: hashmap.c 13041 2010-04-21 03:10:03Z drew $
 */

#include <string.h>

#include "platform/logging.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"

#include "hashmap.h"
#include "hash.h"


#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_SHARED
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_INF PLAT_LOG_LEVEL_INFO

////////////////////////////////////////////////////////////////////////////////
// {{ HASHMAP OPERATIONS

// #define NUM_STATS 5 // synchronize this with newhashmap.h

/** one for each HashMapOp */
typedef enum {
    PUT = 0, INSERT, REMOVE, REPLACE, GET
} HashMapOp;

/** String names corresponding to HashMapOp enum for printing stats */
const char* OP_NAMES[] = {"PUT", "INSERT", "REMOVE", "REPLACE", "GET"};
// }} HASHMAP OPERATIONS

/** Search results for a key */
typedef enum {
    FOUND = 0, NOT_FOUND
} HashMapSearchResult_t;
////////////////////////////////////////////////////////////////////////////////
/** If HashMap should free key */
#define FREE_KEY plat_free
// #define FREE_KEY(..)

/** If HashMap should not copy key */
#define ALLOC_KEY(a, b) a = b

#define DEBUG 0
////////////////////////////////////////////////////////////////////////////////

/** 
 * Create a HashMap.
 * 
 * @param numBuckets <IN> if cmpType HASH_JENKINS, then numBuckets can be power of 2,
 * if it is LAME_SUM, then numBuckets would be better off as a prime number
 * @param lockType
 * @param cmpType <IN> quick compare type to use when comparing keys
 */
HashMap HashMap_create1(uint32_t numBuckets, SDF_utils_hashmap_locktype_t lockType,
        SDF_utils_hashmap_cmptype_t cmpType) {
    HashMap map = NULL;
    int i, size;

    plat_log_msg(21739, LOG_CAT, LOG_DBG, "In HashMap_create()");

    if (numBuckets < 1 || lockType > 7 || lockType < 0) {
        plat_log_msg(21740, LOG_CAT, LOG_ERR, "Incorrect arguments in sdf/shared/HashMap_create"
                "(numBuckets=%u, lockType=%u)", numBuckets, lockType);
        return NULL;
    }

    map = (HashMap) plat_alloc(sizeof (struct HashMapInstance));
    plat_assert(map != NULL);
    map->numElements = 0;
    map->lockType = lockType;
    map->cmpType = cmpType;
    map->numBuckets = numBuckets;
#ifdef SDF_UTILS_STATS_ON
    size = NUM_STATS * sizeof(uint64_t);
    memset(&map->stats, 0, size);
#endif
    size = numBuckets * (sizeof (ListPtr));
    map->buckets = plat_alloc(size);
    plat_assert(map->buckets != NULL);
    for (i = 0; i < numBuckets; i++) {
        map->buckets[i] = NULL;
    }

    switch (map->lockType) {
        case PTHREAD_MUTEX_BUCKET:
            map->bucketLocks = (pthread_mutex_t *) plat_alloc(numBuckets * sizeof (pthread_mutex_t));
            plat_assert(map->bucketLocks != NULL);
            for (i = 0; i < numBuckets; i++) {
                pthread_mutex_init(map->bucketLocks + i, NULL);
            }
            break;
        case PTHREAD_MUTEX_MAP:
            map->mapLock = (pthread_mutex_t *) plat_alloc(sizeof (pthread_mutex_t));
            plat_assert(map->mapLock != NULL);
            pthread_mutex_init(map->mapLock, NULL);
            break;
#ifdef USING_FTH
        case FTH_MAP:
        case FTH_MAP_RW:
            map->mapFthLock = (fthLock_t *) plat_alloc(sizeof (fthLock_t));
            plat_assert(map->mapFthLock != NULL);
            fthLockInit(map->mapFthLock);
            break;
        case FTH_BUCKET:
#ifdef _SDF_HASHMAP_USING_BUCKET_SPINLOCKS
            FTH_SPIN_INIT(&((map->buckets + i)->bucketSpinLock));
#else
            map->bucketSpinLocks = (fthSpinLock_t *) plat_alloc(numBuckets * sizeof (fthSpinLock_t));
            plat_assert(map->bucketSpinLocks != NULL);
            for (i = 0; i < numBuckets; i++) {
                FTH_SPIN_INIT((map->bucketSpinLocks + i));
            }
#endif
            break;
        case FTH_BUCKET_RW:
            map->bucketFthLocks = (fthLock_t *) plat_alloc(numBuckets * sizeof (fthLock_t));
            plat_assert(map->bucketFthLocks != NULL);
            for (i = 0; i < numBuckets; i++) {
                fthLockInit(map->bucketFthLocks + i);
            }
            break;
#endif
        case NONE:
            break;
        default: plat_assert(0 == 1);
    }

    return (map);
}

/**
 * @brief Destroy a HashMap instance.
 *
 * @param map <IN>, HashMap instance to destroy
 */
void HashMap_destroy(HashMap map) {
    int i;

    plat_log_msg(21741, LOG_CAT, LOG_DBG, "In HashMap_destroy()");
    plat_assert(map);

    for (i = 0; i < map->numBuckets; i++) {
        void *p = map->buckets[i];
        if (NULL != p) {
            ListPtr list = p;
            ListPtr next = NULL;
            while (NULL != list) {
                next = list->Next;
                plat_free(list);
                list = next;
            }
        }
    }
    plat_free(map->buckets);
    switch (map->lockType) {
        case PTHREAD_MUTEX_BUCKET:
            plat_free(map->bucketLocks);
            break;
        case PTHREAD_MUTEX_MAP:
            plat_free(map->mapLock);
            break;
#ifdef USING_FTH
        case FTH_MAP:
        case FTH_MAP_RW:
            plat_free(map->mapFthLock);
            break;
        case FTH_BUCKET:
#ifndef _SDF_HASHMAP_USING_BUCKET_SPINLOCKS
            plat_free(map->bucketSpinLocks);
#endif
            break;
        case FTH_BUCKET_RW:
            plat_free(map->bucketFthLocks);
            break;
#endif
        case NONE:
            break;
        default: plat_assert(0 == 1);
    }
    plat_free(map);
}

__inline__ uint32_t HashMap_getBucket1(uint32_t numBuckets, unsigned cmpType, const char *key,
        uint16_t keyLen, uint64_t *key_hashcode)
{
    if (0 == *key_hashcode) {
        if (HASH_JENKINS == cmpType) {
            *key_hashcode = hashb((const unsigned char *)key, keyLen, 0);
        } else {
            uint32_t sum = 0, i;
            for (i = 0; i < keyLen; i++) {
                sum += (unsigned) *(key + i);
            }
            *key_hashcode = sum;
        }
    }
    return (*key_hashcode % numBuckets);
}

uint32_t HashMap_getBucket(uint32_t numBuckets, const char *key)
{
    uint64_t key_hashcode = 0;
    return HashMap_getBucket1(numBuckets, HASH_JENKINS, key, strlen(key), &key_hashcode);
}

/**
 * @brief Lock for the HashMap methods (for private HashMap usage only)
 *
 * Flexible locking mechanisms based on what lockType is set for the HashMap instance.
 * 
 * @param map <IN>, HashMap instance
 * @param bucketNum <IN>, bucket number associated with key
 * @param fthWait <OUT>, when lockType is based on Fth, wait element for fthLock
 * @param writeLock <IN>, when using RW locks, this flag indicates which type to grab
 * 
 * @see SDF_utils_hashmap_locktype_t
 */
__inline__ void
HashMap_Lock(HashMap map, unsigned bucketNum, fthWaitEl_t **fthWait, SDF_boolean_t writeLock)
{
    if (!map->lockType) { // NONE
        return;
    }

    switch (map->lockType) {
#ifdef USING_FTH
        case FTH_BUCKET :
#ifdef _SDF_HASHMAP_USING_BUCKET_SPINLOCKS
            FTH_SPIN_LOCK(&(((map->buckets)+bucketNum)->bucketSpinLock));
#else
            FTH_SPIN_LOCK(map->bucketSpinLocks+bucketNum);
#endif
            break;
#endif
        case PTHREAD_MUTEX_BUCKET :
            pthread_mutex_lock(map->bucketLocks+bucketNum);
            break;
        case PTHREAD_MUTEX_MAP :
            pthread_mutex_lock(map->mapLock);
            break;
#ifdef USING_FTH
        case FTH_MAP :
            *fthWait = fthLock(map->mapFthLock, writeLock, NULL);
            break;
        case FTH_MAP_RW :
            *fthWait = fthLock(map->mapFthLock, writeLock, NULL);
            break;
        case FTH_BUCKET_RW :
            *fthWait = fthLock(map->bucketFthLocks+bucketNum, writeLock, NULL);
            break;
#endif
        default : plat_assert(0 == 1);
    }
}

/**
 * @brief Unlock for the HashMap methods (for private HashMap usage only)
 *
 * Flexible (un)locking mechanisms based on what lockType is set for the HashMap instance.
 * 
 * @param map <IN>, HashMap instance
 * @param bucketNum <IN>, bucket number associated with key
 * @param fthWait <IN>, when lockType is based on Fth, wait element for fthLock
 * 
 * @see SDF_utils_hashmap_locktype_t
 */
__inline__ void
HashMap_Unlock(HashMap map, unsigned bucketNum, fthWaitEl_t *fthWait)
{
    if (!map->lockType) { // NONE
        return;
    }

    switch (map->lockType) {
#ifdef USING_FTH
        case FTH_BUCKET :
#ifdef _SDF_HASHMAP_USING_BUCKET_SPINLOCKS
            FTH_SPIN_UNLOCK(&(((map->buckets)+bucketNum)->bucketSpinLock));
#else
            FTH_SPIN_UNLOCK(map->bucketSpinLocks+bucketNum);
#endif
            break;
#endif
        case PTHREAD_MUTEX_BUCKET :
            pthread_mutex_unlock(map->bucketLocks+bucketNum);
            break;
        case PTHREAD_MUTEX_MAP :
            pthread_mutex_unlock(map->mapLock);
            break;
#ifdef USING_FTH
        case FTH_MAP :
        case FTH_MAP_RW :
        case FTH_BUCKET_RW :
            fthUnlock(fthWait);
            break;
#endif
        default : plat_assert(0 == 1);
    }
}

/**
 * @brief Generic operation, funnel for all hashmap operations.
 *
 * Single choke-point for multiple hashmap operations to reduce errors.
 * <pre>
 * Search in a bucket determines whether a matching key was FOUND | NOT_FOUND.
 * Based on the results and requested operation, the following actions take place.
 *       ---------------------------------
 *       |   op    |  FOUND  | NOT_FOUND |
 *       ---------------------------------
 *       | PUT     | replace |  insert   |
 *       | INSERT  | no-op   |  insert   |
 *       | REMOVE  | remove  |  no-op    |
 *       | REPLACE | replace |  no-op    |
 *       | GET     | get     |  no-op    |
 *       ---------------------------------
 * </pre>
 * Pointers supplied as key and value in arguments are stored as required in the
 * hashmap, thus it IS UNWISE to free them or use stack storage for long-lived
 * entries. Additional information:
 * <pre>
 * IF KEY FOUND:
 * REMOVE: FREE_KEY()* called on stored key pointer. The old pointer to value is returned.
 * PUT: Old pointer to value is returned, and replaced by new.
 * REPLACE: Old pointer to value is returned, and replaced by new.
 * 
 * IF KEY NOT FOUND:
 * INSERT/ PUT: pointers to key and value are stored.
 * </pre>
 *
 * NOTE: PUT is like a GET-CREATE, get val if key exists, otherwise create with
 * value sent.
 *
 * @return NULL, or pointer to value (see individual operation method for proper
 * return status semantics:
 * <pre>
 *       ----------------------------------------
 *       |   op    |  FOUND  | NOT_FOUND        |
 *       ----------------------------------------
 *       | PUT     | old-val |   NULL (?)       |
 *       | INSERT  | val     |   NULL (success) |
 *       | REMOVE  | old-val |   NULL (failure) |
 *       | REPLACE | old-val |   NULL (failure) |
 *       | GET     | old-val |   NULL (failure) |
 *       ----------------------------------------
 * </pre>
 */
void *HashMap_genericOp(HashMap map, const char *key, void* value, uint16_t keyLen,
                        uint64_t *key_hashcode, HashMapOp op) {
    void *ret = NULL;
    fthWaitEl_t *fthWait = NULL;

    if (DEBUG) plat_log_msg(21742, LOG_CAT, LOG_DBG, "In HashMap_put(%s)", key);

    if (!key || !map) {
        return (SDF_FALSE);
    }
    if (op != GET && op != REMOVE) {
        if (!value) {
            return (SDF_FALSE);
        }
    }
    if (!keyLen) {
        keyLen = strlen(key);
    }

    unsigned int bucketNum = 0;
    if ((map->numBuckets > 1) || (0 == *key_hashcode)) {
        bucketNum = HashMap_getBucket1(map->numBuckets, map->cmpType, key, keyLen, key_hashcode);
    }

    if (map->lockType) {
        HashMap_Lock(map, bucketNum, &fthWait, SDF_TRUE);
    }
    // #####################################################################
    HashMapSearchResult_t search_result = NOT_FOUND;
    ListPtr list = map->buckets[bucketNum];
    ListPtr prev = NULL;
    while (NULL != list) {
        if ((list->hashcode == *key_hashcode) && (list->keyLen == keyLen)) {
            if (0 == memcmp(list->key, key, keyLen)) {
                search_result = FOUND;
                break;
            }
        }
        prev = list;
        list = list->Next;
    }

    // now that search for requested key is over, we know the outcome of the search

    switch (search_result) {
        case FOUND:
        {
            switch (op) {
                case GET: // get
                    ret = list->value;
                    break;
                case PUT: // replace
                    ret = list->value;
                    list->value = value;
                    break;
                case INSERT: // no-op
                    ret = value;
                    break;
                case REMOVE: // remove 2nd or further entry
                    if (NULL != prev) {
                        prev->Next = list->Next;
                        ret = list->value;
                        FREE_KEY((void *) list->key);
                        plat_free(list);
                    } else { // remove 1st entry
                        plat_assert(list == map->buckets[bucketNum]);
                        map->buckets[bucketNum] = list->Next;
                        ret = list->value;
                        FREE_KEY((void *) list->key);
                        plat_free(list);
                    }
                    __sync_fetch_and_sub(&(map->numElements), 1);
                    break;
                case REPLACE: // replace
                    ret = list->value;
                    list->value = value;
                    break;
                default: plat_assert(0 == 1);
            }
        }
            break;

        case NOT_FOUND:
        {
            switch (op) {
                case GET: break; // no-op
                case INSERT: // insert
                case PUT: // insert
                    list = (ListPtr) plat_alloc(sizeof (struct ListEntry));
                    plat_assert(list != NULL);
                    // {{ setting new entry
                    list->Next = map->buckets[bucketNum];
                    list->hashcode = *key_hashcode;
                    list->keyLen = keyLen;
                    ALLOC_KEY(list->key, key);
                    list->value = value;
                    // }} setting new entry
                    map->buckets[bucketNum] = list;
                    __sync_fetch_and_add(&(map->numElements), 1);
                    break;
                case REMOVE: // no-op
                    break;
                case REPLACE: // no-op
                    break;
                default: plat_assert(0 == 1);
            }
        }
            break;

        default: plat_assert(0 == 1);
    }
    // #####################################################################
    if (map->lockType) {
        HashMap_Unlock(map, bucketNum, fthWait);
    }

#ifdef SDF_UTILS_STATS_ON
    __sync_fetch_and_add(&(map->stats[op]), 1);
#endif

    return (ret);
}

/**
 * @brief print statistics of HashMap in the given string buffer
 *
 * @param map <IN> instance of HashMap
 * @param name <IN> name of the HashMap given by consumer to print
 * @param str <IN> string buffer to print into
 * @param size <IN> size of the buffer to max use
 * @return int number of bytes used
 */
int HashMap_stats(HashMap map, char *name, char *str, int size) {
    int i=0, j=0;

    if (NULL == map) {
        plat_log_msg(21743, LOG_CAT, LOG_DBG, "argument map=%p", map);
        return 0;
    }
    i += snprintf(str+i, size-i, "<HashMap %s> [buckets=%u, ", name, map->numBuckets);
#ifdef SDF_UTILS_STATS_ON
    for(j=0; j<NUM_STATS; j++) {
        i += snprintf(str+i, size-i, "%s=%lu, ", OP_NAMES[j], map->stats[j]);
    }
#endif
    
    i += snprintf(str+i, size-i, "numElements=%lu, lockType=%u]", map->numElements, map->lockType);

#ifdef HASHMAP_STATS_DETAIL
    for (i = 0; i < map->numBuckets; i++) {
        i += snprintf(str+i, size-i, "Bucket #%d :", i);
        ListPtr list = map->buckets[i];
        while (NULL != list) {
            i += snprintf(str+i, size-i, "%s, ", list->key);
            list = list->Next;
        }
        i += snprintf(str+i, size-i, "\n");
    }
#endif
    return (i);
}
////////////////////////////////////////////////////////////////////////////////

HashMap HashMap_create(uint32_t numBuckets, SDF_utils_hashmap_locktype_t lockType)
{
    return (HashMap_create1(numBuckets, lockType, HASH_JENKINS));
}

void* HashMap_replace(HashMap map, const char *key, void* value)
{
    uint64_t key_hashcode = 0;
    return (HashMap_genericOp(map, key, value, 0, &key_hashcode, REPLACE));
}

void* HashMap_replace1(HashMap map, const char *key, void* value, uint16_t keyLen)
{
    uint64_t key_hashcode = 0;
    return (HashMap_genericOp(map, key, value, keyLen, &key_hashcode, REPLACE));
}

SDF_boolean_t HashMap_put(HashMap map, const char *key, void* value)
{
    uint64_t key_hashcode = 0;
    if (NULL == HashMap_genericOp(map, key, value, 0, &key_hashcode, INSERT)) {
        return (SDF_TRUE);
    } else {
        return (SDF_FALSE);
    }
}

SDF_boolean_t HashMap_put1(HashMap map, const char *key, void* value, uint16_t keyLen)
{
    uint64_t key_hashcode = 0;
    if (NULL == HashMap_genericOp(map, key, value, keyLen, &key_hashcode, INSERT)) {
        return (SDF_TRUE);
    } else {
        return (SDF_FALSE);
    }
}

void* HashMap_remove(HashMap map, const char *key)
{
    uint64_t key_hashcode = 0;
    return (HashMap_genericOp(map, key, NULL, 0, &key_hashcode, REMOVE));
}

void* HashMap_remove1(HashMap map, const char *key, uint16_t keyLen)
{
    uint64_t key_hashcode = 0;
    return (HashMap_genericOp(map, key, NULL, keyLen, &key_hashcode, REMOVE));
}

void* HashMap_get(HashMap map, const char *key)
{
    uint64_t key_hashcode = 0;
    return (HashMap_genericOp(map, key, NULL, 0, &key_hashcode, GET));
}

void* HashMap_get1(HashMap map, const char *key, uint16_t keyLen)
{
    uint64_t key_hashcode = 0;
    return (HashMap_genericOp(map, key, NULL, keyLen, &key_hashcode, GET));
}

unsigned long HashMap_getSize(HashMap map) {
    return (map->numElements);
}

