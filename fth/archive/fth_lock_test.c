/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/* author: Mac
 *
 * Created on Apr 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 */

#include "fth.h"
#include "fthMbox.h"
#include "fthThread.h"
#include "fthWaitEl.h"
#include "fthMem.h"
#include "fthMbox.h"
#include "fthMutex.h"
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
int holdQ_size, waitQ_size, eligibleQ_size;
static fthLock_t lock; 

int get_holdQ_size(fthWaitEl_t *w) {
    holdQ_size = 0;
    while(w) {

        holdQ_size ++;
        w = w->waitQEl.next;
    }
    return holdQ_size;

}

int get_waitQ_size(fthWaitEl_t *w) {

    waitQ_size = 0;
    while(w) {
        waitQ_size ++;
        w = w->waitQEl.next;
    }
    return waitQ_size;


}
int get_eligibleQ_size(fthThread_t *p) {
    eligibleQ_size = 0;
    while(p) {
        eligibleQ_size ++;
        p = p->threadQ.next;
    }
    return eligibleQ_size;

}
void fth_lock_test_1(uint64_t arg) {
    fthWaitEl_t *wait_1;
    fthLockInit(&lock);
    printf("in fth %d: entry is %s\n", arg, __FUNCTION__);

    printf("fth %d want to get the read lock. \n", arg);
    wait_1 = fthLock(&lock, 0, NULL);
    printf("fth %d:get the read lock. \n", arg);

    printf("barrier in fth %d: \n", arg);
    ASSERT(get_holdQ_size(lock.holdQ.head) == 1, "the size of holdQ is 1. ")
    ASSERT(get_waitQ_size(lock.waitQ.head) == 0, "the size of waitQ is 0. ")
    fthYield(1);
    //----------------------------------------1
    printf("fth %d want to release the read lock. \n", arg);
    fthUnlock(wait_1);
    printf("fth %d:release the read lock. \n", arg);

    ASSERT(get_holdQ_size(lock.holdQ.head) == 1, "the size of holdQ is 1. ")
    ASSERT(get_waitQ_size(lock.waitQ.head) == 0, "the size of waitQ is 0. ")
    ASSERT(lock.readLock == 1, "one fth is reading now. ")
    ASSERT(lock.writeLock == 0, "no fth is writing now. ")

    printf("fth %d want to get the write lock. \n", arg);
    wait_1 = fthLock(&lock, 1, NULL);//one of fth is reading now ,so this fth will push into waitQ
    //-----------------------------------3
    printf("fth %d:get the write lock. \n", arg);

    printf("barrier in fth %d: \n", arg);
#ifdef MULTIQ_SCHED
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ[0].head) == 0, "eligibleQ has empty. ")
#else
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ.head) == 0, "eligibleQ has empty. ")
#endif // MULTIQ_SCHED

    ASSERT(get_waitQ_size(lock.holdQ.head) == 1, "holdQ has 1 fth hold the lock. ")
    ASSERT(get_waitQ_size(lock.waitQ.head) == 1, "waitQ has 1 fth wait for lock. ")


    printf("fth %d want to release the write lock. \n", arg);
    fthUnlock(wait_1); 
#ifdef MULTIQ_SCHED
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ[0].head) == 1, "eligibleQ hasn't fth. ")
#else
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ.head) == 1, "eligibleQ hasn't fth. ")
#endif // MULTIQ_SCHED

    printf("fth %d:release the write lock. \n", arg);   
   
    fthYield(1); 
    //-----------------------------------------------5
    printf("fth %d want to get the write lock. \n", arg);
    //wait_1 = fthTryLock(&lock, 1, NULL);
    //ASSERT(wait_1 == NULL, "through fthTryLock function, this fth cann't get write lock, so return null. ")
    printf("because  one fth got the write lock ,so this fth cann't get the write lock and will push into waitQ. \n");
    wait_1 = fthLock(&lock, 1, NULL);//one of fth is writing now ,so this fth will push into waitQ
    printf("fth %d:get the write lock. \n", arg);

    printf("barrier in fth %d: \n", arg);

    printf("fth %d want to release the write lock. \n", arg);
    fthUnlock(wait_1);
    printf("fth %d:release the write lock. \n", arg);

}

void fth_lock_test_2(uint64_t arg) {
    fthWaitEl_t *wait_2;
    printf("in fth %d: entry is %s\n", arg, __FUNCTION__);

    printf("fth %d want to get the read lock. \n", arg);
    //wait_2 = fthTryLock(&lock, 0, NULL);
    //ASSERT(wait_2 != NULL, "get the fth read lock through call fthTryLock. ")
    wait_2 = fthLock(&lock, 0, NULL);
    printf("fth %d:get the read lock. \n", arg);

    printf("fth can get the read lock currently. \n");
    printf("barrier in fth %d: \n", arg);
    ASSERT(get_holdQ_size(lock.holdQ.head) == 2, "the size of holdQ is 2. ")
    ASSERT(get_waitQ_size(lock.waitQ.head) == 0, "the size of waitQ is 0. ")
    fthYield(1);
    //---------------------------------------------2

    printf("fth %d want to release the read lock. \n", arg);
    fthUnlock(wait_2);//get the thread from waitQ, then put it into resumeQ
    printf("fth %d:release the read lock. \n", arg);

    printf("fthUnlock make the wait fth from waitQ to holdQ. \n");
    ASSERT(get_holdQ_size(lock.holdQ.head) == 1, "the size of holdQ is 1. ")
    ASSERT(get_waitQ_size(lock.waitQ.head) == 0, "the size of waitQ is 0. ")
    ASSERT(lock.readLock == 0, "no fth is reading now. ")
    ASSERT(lock.writeLock == 1, "one fth is waiting to write now. ")

    printf("fth %d want to get the read lock. \n", arg);
    wait_2 = fthLock(&lock, 0, NULL);
    //---------------------------------------4
    printf("fth %d:get the read lock. \n", arg);
    
    printf("when one fth wait to get the write lock ,the new fth which want to get to read lock must be wait. \n");
    printf("barrier in fth %d:\n", arg);

#ifdef MULTIQ_SCHED
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ[0].head) == 1, "eligibleQ has one fth. ")
#else
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ.head) == 1, "eligibleQ has one fth. ")
#endif // MULTIQ_SCHED

    printf("fth %d want to release the read lock. \n", arg);
    fthUnlock(wait_2);
    printf("fth %d:release the read lock. \n", arg);

    ASSERT(get_holdQ_size(lock.holdQ.head) == 0, "the size of holdQ is 0. ")
    ASSERT(get_waitQ_size(lock.waitQ.head) == 0, "the size of waitQ is 0. ")   

    printf("fth %d want to get the write lock. \n", arg); 
    wait_2 = fthLock(&lock, 1, NULL);
    printf("fth %d:get the write lock. \n", arg);
   
    fthYield(1); 
    printf("barrier in fth %d: \n", arg);
    ASSERT(get_holdQ_size(lock.holdQ.head) == 1, "the size of holdQ is 1. ")
    ASSERT(get_waitQ_size(lock.waitQ.head) == 1, "the size of waitQ is 1. ")

    printf("fth %d want to release the write lock. \n", arg);
    fthUnlock(wait_2);
    printf("fth %d:release the write lock. \n", arg);
    fthKill(1);
}

#define SHM_SIZE 8 * 1024 *1024

int main(void) {

    struct plat_shmem_config *shmem_config = plat_alloc(sizeof(struct plat_shmem_config));
    plat_shmem_config_init(shmem_config);
    shmem_config->size = SHM_SIZE;
    plat_shmem_prototype_init(shmem_config);
    int tmp = plat_shmem_attach(shmem_config->mmap);
    if (tmp) {
        plat_log_msg(20073, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                     PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                     shmem_config->mmap, plat_strerror(-tmp));
        plat_abort();
    }

    fthInit();
    
    printf("*** === TEST THE FUNCTION IN FTHLOCK.C === ***\n");
    /*
    printf("initialize the global lock. \n");
    fthLockInit(&lock);//can be used in pthread??
    ASSERT(lock.readLock == 0, "init readLock. ")
    ASSERT(lock.writeLock == 0, "init writeLock. ")
    ASSERT(lock.waitQ.head == NULL, "init waitQ")
    ASSERT(lock.holdQ.head == NULL, "init holdQ")
    ASSERT(lock.spin == 0, "init spin")

    ASSERT(get_waitQ_size(lock.waitQ.head) == 0, "the size of waitQ is 0. ")
    ASSERT(get_holdQ_size(lock.holdQ.head) == 0, "the size of holdQ is 0. ")
    */
    XResume(XSpawn(fth_lock_test_1, 4096), 1);
    XResume(XSpawn(fth_lock_test_2, 4096), 2);

    fthSchedulerPthread(0);
    printf("test finished. \n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);

                   
}

