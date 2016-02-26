/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/* author: Mac
 *
 * Created on May 5, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 */

#include "fth.h"
#include "fthXLock.h"
#include "applib/XLock.h"
#include "applib/XLock.c"
#include "sdfappcommon/XLock.h"

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/signal.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/time.h"
#include "platform/unistd.h"
#include "misc/misc.h"

#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
static int failed_count = 0;
#define ASSERT(expr, str) \
     if (expr) { \
         printf("*** PASSED, TEST:%s *** \n", str); \
     } \
     else { \
         printf("*** FAILED, TEST:%s *** \n", str); \
         failed_count ++; \
     }

int exitRV = 0;
int eligibleQ_size;

static XLock_t cross;
int get_eligibleQ_size(fthThread_t *p) {
    eligibleQ_size = 0;
    while(p) {
        eligibleQ_size ++;
        p = p->threadQ.next;
    }
    return eligibleQ_size;

}
#define COUNT 10
#define NUM_FTH 3
#define NUM_PTHREAD 5
volatile int finished_fths = 0;
/*
void fth_xlock_test(uint64_t arg) {
    int index;
    fthXLock(&cross, 0);
    printf("fth %d get the read cross lock. \n", arg);
    for(index = 0; index < arg; index ++) {
       printf("fth %d print %d\n", arg, index);
       fthYield(1);
    }
    fthXUnlock(&cross);
}
*/
void fth_xlock_test(uint64_t arg) {
    int index;
    printf("fth %d want to get the write cross lock. \n", arg);
    fthXLock(&cross, 1);
    printf("fth %d get the write cross lock. \n", arg);
    for(index = 0; index < arg; index ++) {
       printf("fth %d print %d\n", arg, index);
    }
    fthXUnlock(&cross);
    finished_fths ++;
    if(finished_fths == NUM_FTH)
       fthKill(NUM_PTHREAD);
}
/*
void *pthread_xlock_test_1(void *arg) {
    int index;
    printf("pthread %d want to get the read cross lock. \n", (uint64_t)arg);
    XLock(&cross, 0);
    printf("pthread %d get the read cross lock. \n", (uint64_t)arg);
    for(index = 0; index < COUNT; index ++){
       printf("pthread %d print %d \n", (uint64_t)arg, index);
       pthread_yield();
    }

    XUnlock(&cross);
    printf("pthread %d release the read cross lock. \n", (uint64_t)arg);

    return 0;
}
*/


//this pthread get the write cross lock
void *pthread_xlock_test_1(void *arg) {

    int index;
    printf("pthread %d want to get the write cross lock. \n", (uint64_t)arg);
    XLock(&cross, 1);
    printf("pthread %d get the write cross lock. \n", (uint64_t)arg);
    for(index = 0; index < COUNT; index ++){
       printf("pthread %d print %d \n", (uint64_t)arg, index);
       pthread_yield();
    }

    printf("pthread %d release the write cross lock. \n", (uint64_t)arg);
    XUnlock(&cross);
    return 0;
}
/*
//pthread get the read cross lock 
void *pthread_xlock_test_2(void *arg) {
    int index;
    fthThread_t *sched = fthInit();
    printf("pthread %d want to get the read cross lock. \n", (uint64_t)arg);
    XLock(&cross, 0);
    printf("pthread %d get the read cross lock. \n", (uint64_t)arg);

    for (index = 1; index <= NUM_FTH; index ++) {
        fthResume(fthSpawn(&fth_xlock_test, 4096), index);
    }
    XUnlock(&cross);
    printf("pthread %d release the read cross lock. \n", (uint64_t)arg);

    fthStartScheduler(sched);
    printf("Scheduler %i halted\n", (uint64_t) arg);
    
    return 0;

}
*/

//pthread get the write cross lock 
void *pthread_xlock_test_2(void *arg) {
    int index;
    printf("pthread %d want to get the write cross lock. \n", (uint64_t)arg);
    XLock(&cross, 1);
    printf("pthread %d get the write cross lock. \n", (uint64_t)arg);

    for (index = 1; index <= NUM_FTH; index ++) {
        fthResume(fthSpawn(&fth_xlock_test, 4096), index);
    }
    pthread_yield();
    XUnlock(&cross);
    printf("pthread %d release the write cross lock. \n", (uint64_t)arg);

    fthSchedulerPthread(0);
    printf("scheduler %i halted.\n", (uint64_t) arg);

    return 0;

}

void *pthread_xlock_test_3(void *arg) {
    fthSchedulerPthread(0);
    printf("scheduler %i halted.\n", (uint64_t) arg);
    return 0;
}


#define SHM_SIZE 8 * 1024 *1024

int main(int argc, char **argv) {
    uint64_t index;
    pthread_t pthread_1[NUM_PTHREAD],pthread_2, pthread_3;

    plat_shmem_trivial_test_start(argc, argv);

#ifdef MULTIQ_SCHED
    fthInitMultiQ(1, 2); // Tell the scheduler code that there'll be 2 schedulers starting up
#else
    fthInit();
#endif 

    printf("*** === TEST THE FUNCTION IN FTHXLOCK.C/XLOCK.C === ***\n");
    fthXLockInit(&cross);
    pthread_create(&pthread_2, NULL, &pthread_xlock_test_2, (void *) (NUM_PTHREAD + 1));
    pthread_create(&pthread_3, NULL, &pthread_xlock_test_3, (void *) (NUM_PTHREAD + 2));
    for(index = 1; index <= NUM_PTHREAD; index ++)
       pthread_create(&pthread_1[index - 1], NULL, &pthread_xlock_test_1, (void *)index );

    //pthread_create(&pthread_2, NULL, &pthread_xlock_test_2, (void *) (NUM_PTHREAD + 1));
    

    for(index = 0; index < NUM_PTHREAD; index ++)
       pthread_join(pthread_1[index], NULL);
    pthread_join(pthread_2, NULL);
    pthread_join(pthread_3, NULL);

    plat_shmem_trivial_test_end();

    printf("test finished. \n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);

}
