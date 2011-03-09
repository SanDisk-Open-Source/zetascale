/*
 * File:   fthMPTest.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthMPTest.h 396 2008-02-29 22:55:43Z jim $
 */

//
// MPTest program for many fth functions
//

#include <pthread.h>

#include "fth.h"
#include "fthMbox.h"
#include "fthThread.h"
#include "fthWaitEl.h"
#include "fthMem.h"
#include "fthMbox.h"
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

#define pf(...) printf(__VA_ARGS__); fflush(NULL)

fthMbox_t mbox;
fthLock_t lock1;
fthLock_t lock2;
uint64_t mem[3];

volatile int threadCount;

int exitRV = 0;

int numTries = 0;

void threadRoutine3(uint64_t arg) {
    pf("Thread 3 start\n");

    while (1) {
        uint64_t mail = fthMboxTry(&mbox);
        if (mail == 0) {
            numTries++;
        } else {
            if (mail != 999) {
                exitRV = 1;
                pf("Mbox try fail: expected 999 but got %i after %i tries\n", mail, numTries);
            } else {
                pf("Mbox head wait OK: expected 999 and got %i after %i tries\n", mail, numTries);
            }
            break;

        }

        fthYield(0);
        
    }            
        
    fthKill(100);
    
}

void threadRoutine2(uint64_t arg) {
    pf("Thread 2 start - %i number %i\n", arg, __sync_fetch_and_add(&threadCount, 1));

    fthWaitEl_t *waitR = fthLock(&lock1, 0, NULL);   // Read lock

    pf("Thread 2 got read lock - %i\n", arg);
    fthUnlock(waitR);
    pf("Thread 2 released read lock - %i\n", arg);
    fthWaitEl_t *waitW = fthLock(&lock2, 1, NULL);   // Write lock
    pf("Thread 2 got write lock - %i\n", arg);

    fthUnlock(waitW);
    pf("Thread 2 released write lock - %i\n", arg);
    
    if (__sync_fetch_and_add(&threadCount, -1) == 1) {
        pf("Thread 2 last man standing starts thread3 - %i\n", arg);
        fthResume(fthSpawn(&threadRoutine3, 4096), 0);        
        sleep(1);

        pf("Thread posts mailbox singleton - %i (current try count %i)\n", arg, numTries);
        fthMboxPost(&mbox, 999);
    }
    
}

void threadRoutine1(uint64_t arg) {
    pf("Thread 1 start\n");

    fthWaitEl_t *wait = fthLock(&lock1, 1, NULL);   // Write lock
    
    pf("Thread 1 Lock\n");
    fthUnlock(wait);
    pf("Thread 1 unlock complete\n");
    
}

fthWaitEl_t *wait1, *wait2;

void initThread(uint64_t arg) {

    fthLockInit(&lock1);
    fthLockInit(&lock2);
    wait1 = fthLock(&lock1, 1, NULL);        // Write lock
    wait2 = fthLock(&lock2, 1, NULL);        // Write lock
        
    for (int i=0; i<100; i++) {
        fthResume(fthSpawn(&threadRoutine2, 4096), i);
    }

    //        fthResume(fthSpawn(&threadRoutine1, 4096), 0);
    fthYield(1000);                          // Give them a chance to start

    pf("Pre lock release\n");
    fthUnlock(wait1);
    pf("Post lock1 release\n");
    fthUnlock(wait2);
    pf("Post lock2 release\n");

}

#define NUM_PTHREADS 4

void *pthreadRoutine(void *arg) {
    fthInit();

    if ((uint64_t) arg == (NUM_PTHREADS - 1)) { // Wait for last scheduler
        XResume(XSpawn(&initThread, 4096), 0);
    }

    pf("Scheduler %i starting\n", (uint64_t) arg);

    fthSchedulerPthread(0);
    
    pf("Scheduler %i halted\n", (uint64_t) arg);

    return (0);

}


#define SHM_SIZE 8 * 1024 *1024

int main(void) {
    pthread_t pthread[10];

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


    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_create(&pthread[i], NULL, &pthreadRoutine, (void *) i);
    }

    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_join(pthread[i], NULL);
    }

    printf("Total scheduler idle time:         %lli microseconds\n", fthGetSchedulerIdleTime());
    printf("Total scheduler dispatch time:     %lli microseconds\n", fthGetSchedulerDispatchTime());
    printf("Total fth thread run time:         %lli microseconds\n", fthGetTotalThreadRunTime());
    printf("Total fth avg dispatch time:       %lli nanoseconds\n", fthGetSchedulerAvgDispatchNanosec());
    printf("Total fth dispatches:              %lli\n", fthGetSchedulerNumDispatches());
    printf("Total voluntary context switches   %lli\n", fthGetVoluntarySwitchCount());
    printf("Total involuntary context switches %lli\n", fthGetInvoluntarySwitchCount());

    plat_exit(exitRV);
}
