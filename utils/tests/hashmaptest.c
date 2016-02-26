/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   hashmaptest.c
 * Author: Darpan Dinker
 *
 * Created on February 15, 2008, 2:17 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id$
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "fth/fth.h"
#include "agent/agent_helper.h"
#include "utils/hashmap.h"

// ====Taken from Memcached generator.c=================================================================================
typedef struct pairs_st pairs_st;

struct pairs_st {
  char *key;
  size_t key_length;
  char *value;
  size_t value_length;
};

/* Use this for string generation */
static const char ALPHANUMERICS[]=
  "0123456789ABCDEFGHIJKLMNOPQRSTWXYZabcdefghijklmnopqrstuvwxyz";

#define ALPHANUMERICS_SIZE (sizeof(ALPHANUMERICS)-1)

static void get_random_string(char *buffer, size_t size)
{
  char *buffer_ptr= buffer;

  while (--size)
    *buffer_ptr++= ALPHANUMERICS[plat_random() % ALPHANUMERICS_SIZE];
  *buffer_ptr++= ALPHANUMERICS[plat_random() % ALPHANUMERICS_SIZE];
}

pairs_st *pairs_generate(uint32_t number_of, uint32_t keySize, uint32_t objSize)
{
  unsigned int x;
  pairs_st *pairs;

  pairs= (pairs_st*)plat_alloc(sizeof(pairs_st) * (number_of+1));

  if (!pairs)
    goto error;

  memset(pairs, 0, sizeof(pairs_st) * (number_of+1));

  for (x= 0; x < number_of; x++)
  {
    pairs[x].key= (char *)plat_alloc(sizeof(char) * keySize);
    if (!pairs[x].key)
      goto error;
    get_random_string(pairs[x].key, keySize);
    pairs[x].key_length= keySize;

    pairs[x].value= (char *)plat_alloc(sizeof(char) * objSize);
    if (!pairs[x].value)
      goto error;
    get_random_string(pairs[x].value, objSize);
    pairs[x].value_length= objSize;
  }

  return pairs;
error:
    fprintf(stderr, "Memory Allocation failure in pairs_generate.\n");
    plat_exit(0);
}

void pairs_free(pairs_st *pairs)
{
  unsigned int x;

  if (!pairs)
    return;

  /* We free until we hit the null pair we stores during creation */
  for (x= 0; pairs[x].key; x++)
  {
    plat_free(pairs[x].key);
    plat_free(pairs[x].value);
  }

  plat_free(pairs);
}
// =====================================================================================================================

HashMap map = NULL;
uint32_t numObjects = 10000;
unsigned objSize = 400;
unsigned keySize = 100;
uint32_t numBuckets = 16384;
SDF_utils_hashmap_cmptype_t cmpType = HASH_JENKINS;
SDF_utils_hashmap_locktype_t lockType = 0;
pairs_st *pairs = NULL;
float f1, f2;

void createCacheAndTest() {
    struct timespec time1, time2, cputime1, cputime2;
    map = HashMap_create1(numBuckets, lockType, cmpType);
    plat_assert_always(0 == clock_gettime(CLOCK_REALTIME, &time1));
    plat_assert_always(0 == clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cputime1));
    if (map) {
        for (uint32_t i=0; i<numObjects; i++) {
            HashMap_put1(map, pairs[i].key, pairs[i].value, pairs[i].key_length);
            // fthYield(1);
        }
    }
    plat_assert_always(0 == clock_gettime(CLOCK_REALTIME, &time2));
    plat_assert_always(0 == clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cputime2));

    int diff_time_us = ((time2.tv_sec - time1.tv_sec) * 1000000) + ((time2.tv_nsec - time1.tv_nsec) / 1000);
    int diff_cputime_us = ((cputime2.tv_sec - cputime1.tv_sec) * 1000000) + ((cputime2.tv_nsec - cputime1.tv_nsec) / 1000);

    printf("Total Wallclock time taken=%d usec\n", diff_time_us);
    printf("Total CPU time taken=%d usec\n", diff_cputime_us);
    
    f1 = ((float) diff_time_us/numObjects);
    f2 = ((float)diff_cputime_us/numObjects);
    
    pairs_free(pairs);
    fthKill(222);
}

int
execute_test() {
    fthInit();
    
    fthResume(fthSpawn(&createCacheAndTest, 4096), 1);
    
    fthSchedulerPthread(0);
    
    printf("Wallclock time per iteration=%.4f usec\n", f1);
    printf("CPU time per iteration=%.4f usec\n", f2);
    
    return 0;
}

int
prepare_test(uint32_t numObjects, uint32_t keySize, uint32_t objSize) {
    pairs = pairs_generate(numObjects, keySize, objSize);
    return (pairs?0:1);
}

int
main(int argc, char *argv[]) {
     if (SDF_FALSE == init_agent_sm(0)) {
        plat_exit(-2);
    }
    if (argc > 1 && argc < 7) {
        printf("Usage: %s num-buckets lock-type cmp-type num-objects key-size object-size\n", argv[0]);
        plat_exit(-1);
    } else if (argc > 6) {
        numBuckets = (uint32_t) atoi(argv[1]);
        lockType = (unsigned) atoi(argv[2]);
        cmpType = (unsigned) atoi(argv[3]);
        numObjects = (uint32_t) atoi(argv[4]);
        keySize = (uint32_t) atoi(argv[5]);
        objSize = (uint32_t) atoi(argv[6]);
        printf("numBuckets=%u, lockType=%u, cmp-type=%u, numObjects=%u, keySize=%u, objSize=%u\n", numBuckets, lockType, cmpType, numObjects, keySize, objSize);
    }
    int ret = prepare_test(numObjects, keySize, objSize);
    if (!ret)
        return (execute_test());
    else 
        return (ret);
}
