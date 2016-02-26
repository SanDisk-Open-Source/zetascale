/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   utils/hashmap.h
 * Author: Darpan Dinker
 *
 * Created on November 12, 2008, 11:05 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: hashmap.h 8193 2009-06-27 06:02:43Z drew $
 */

#ifndef _HASHTABLE_H
#define _HASHTABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common/sdftypes.h"
#include "fth/fth.h"
#include "fth/fthSpinLock.h"
#include <pthread.h>

#define MAX_BUCKETS 16384  // corresponding to 18 bits
#define SDF_UTILS_STATS_ON // stats will print the number of PUT, INSERT, REPLACE, REMOVE, GETs
#define USING_FTH          // for Fth based locks
#define NUM_STATS 5        // 1 stat per operation, currently 5

/** Consumer of HashMap specifies with constructor() the type of lock to use for concurrency control */
typedef enum {
    NONE = 0, PTHREAD_MUTEX_MAP, PTHREAD_MUTEX_BUCKET, FTH_MAP, FTH_BUCKET, FTH_MAP_RW, FTH_BUCKET_RW
} SDF_utils_hashmap_locktype_t;

/** how hashing is done */
typedef enum {
    HASH_JENKINS = 0, LAME_SUM
} SDF_utils_hashmap_cmptype_t;
////////////////////////////////////////////////////////////////////////////////

/** ListEntry for each {key, value} stored in the doubly linked-list */
struct ListEntry {
    /** pointer to next entry in the list */
    struct ListEntry *Next;
    /** pointer to the string that is the key for the object stored */
    const char *key;
    /** pointer to the object stored for the corresponding key */
    void *value;
    /** Length of the key, not counting the null terminator. What strlen would report */
    uint16_t keyLen;
    /** Hashcode for the cache object*/
    uint64_t hashcode;
};

typedef struct ListEntry * ListPtr;

/** 1 instance per HashMap instance, received via the constructor */
struct HashMapInstance {
    unsigned lockType : 3;
    unsigned cmpType : 3;
    uint32_t numBuckets;
    uint64_t numElements;
    /** one lock per bucket for increased concurrency */
    pthread_mutex_t *bucketLocks;
    /** one lock per map */
    pthread_mutex_t *mapLock;
#ifdef USING_FTH
    fthLock_t *bucketFthLocks;
    fthSpinLock_t *bucketSpinLocks;
    fthLock_t *mapFthLock;
#endif
    /** buckets of pointers to Lists */
    ListPtr *buckets;
#ifdef SDF_UTILS_STATS_ON
    volatile uint64_t stats[NUM_STATS];
#endif
};

typedef struct HashMapInstance *HashMap;

////////////////////////////////////////////////////////////////////////////////

/** 
 * @brief calculated the target bucket mapping for a given string key
 *
 * Some consumers may just require a method to do the hashing for it.
 * 
 * @param numBuckets <IN> total number of buckets
 * @param key <IN> key string that maps to a unique bucket, given the internal mapping algo.
 * @return uint32_t, bucket that the key maps to
 */
uint32_t HashMap_getBucket(uint32_t numBuckets, const char *key);

/** 
 * Create a HashMap.
 *
 * @param numBuckets <IN> if cmpType HASH_JENKINS, then numBuckets can be power of 2, 
 * if it is LAME_SUM, then numBuckets would be better off as a prime number
 * @param lockType
 * @param cmpType <IN> quick compare type to use when comparing keys
 */
HashMap HashMap_create1(uint32_t numBuckets, SDF_utils_hashmap_locktype_t lockType, 
                        SDF_utils_hashmap_cmptype_t cmpType);

/** 
 * Create HashMap with compare type as HASH_JENKINS.
 * @see HashMap_create1
 */
__inline__ HashMap HashMap_create(uint32_t numBuckets, SDF_utils_hashmap_locktype_t lockType);

/**
 * @brief Destroy a HashMap instance.
 *
 * @param map <IN> instance of HashMap to be destroyed
 */
void HashMap_destroy(HashMap map);

/**
 * return NULL if no previous value, otherwise returns the previous value
 * Will not insert if existing key not found
 * @param map <IN> instance of HashMap
 */
__inline__ void* HashMap_replace(HashMap map, const char *key, void* value);
__inline__ void* HashMap_replace1(HashMap map, const char *key, void* value, uint16_t keyLen);

/**
 * returns SDF_TRUE is operation was a success, returns SDF_FALSE for duplicate keys
 * Will not insert a duplicate
 * @param map <IN> instance of HashMap
 * @parma key <IN> map assumes ownership of key which must be allocated with 
 * plat_alloc.  
 * @parma 
 */
__inline__ SDF_boolean_t HashMap_put(HashMap map, const char *key, void* value);
__inline__ SDF_boolean_t HashMap_put1(HashMap map, const char *key, void* value, uint16_t keyLen);
/**
 * returns the stored value, or NULL if key was non-existent
 * @param map <IN> instance of HashMap
 */
__inline__ void* HashMap_remove(HashMap map, const char *key);
__inline__ void* HashMap_remove1(HashMap map, const char *key, uint16_t keyLen);
/**
 * return the stored value, or NULL if key was non-existent
 * @param map <IN> instance of HashMap
 */
__inline__ void* HashMap_get(HashMap map, const char *key);
__inline__ void* HashMap_get1(HashMap map, const char *key, uint16_t keyLen);

__inline__ unsigned long HashMap_getSize(HashMap map);
// Iterator* iterator();
/** Not implemented */
void HashMap_printStats(HashMap map);
/** Not implemented */
void HashMap_printString(HashMap map);

/**
 * @brief print statistics of HashMap in the given string buffer
 *
 * @param map <IN> instance of HashMap
 * @param name <IN> name of the HashMap given by consumer to print
 * @param str <IN> string buffer to print into
 * @param size <IN> size of the buffer to max use
 * @return int number of bytes used
 */
int HashMap_stats(HashMap map, char *name, char *str, int size);

#ifdef __cplusplus
}
#endif

#endif /* _HASHTABLE_H */
