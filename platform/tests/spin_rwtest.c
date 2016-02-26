/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   spin_rwtest.c
 * Author: Wei,Li
 *
 * Created on July 15, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 */

/*
 * Trivial test for spin rw lock.
 */
#include <pthread.h>

#include "platform/rwlock.h"
#include "platform/spin_rw.h"

#include "platform/stdio.h"

#include "platform/types.h"

plat_spin_rwlock_t spin_rwlock=0;

#define NUM_PTHREADS 64 

int value=0;

void *pthreadReader(void *arg) {
   int local_value=0;
   plat_spin_rw_rdlock(spin_rwlock);
   local_value=value;
   plat_spin_rw_rdunlock(spin_rwlock);
   return 0;
}
void *pthreadWriter(void *arg) {
   plat_spin_rw_wrlock(spin_rwlock);
   ++value;
   plat_spin_rw_wrunlock(spin_rwlock);
   return 0;	
}
int main(int argc, char **argv) {
     pthread_t pthread_readers[NUM_PTHREADS];
     pthread_t pthread_writers[NUM_PTHREADS];
     for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_create(&pthread_readers[i], NULL, &pthreadReader, NULL);
        pthread_create(&pthread_writers[i], NULL, &pthreadWriter, NULL);
     }

    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_join(pthread_readers[i], NULL);
	pthread_join(pthread_writers[i], NULL);
    }
    printf("%d\n",value);
    plat_assert(value==NUM_PTHREADS);
    return (0);
}
