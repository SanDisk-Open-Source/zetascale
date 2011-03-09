/*
 * File:   fthSparseTest.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthSparseTest.h 396 2008-02-29 22:55:43Z jim $
 */

//
// Sparse lock test
//

#include <pthread.h>

#include "fth.h"
#include "fthMbox.h"
#include "fthThread.h"
#include "fthWaitEl.h"
#include "fthMem.h"
#include "fthMbox.h"
#include "fthSparseLock.h"
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
#include <inttypes.h>

fthLock_t lock1;
fthSparseLockTable_t *slt;

int threadCount;

int exitRV = 0;

void threadRoutine2(uint64_t arg) {
    printf("Thread 2 start - %"PRIu64"\n", arg);
    (void) __sync_fetch_and_add(&threadCount, 1);

    fthWaitEl_t *waitR = fthSparseLock(slt, 100, 1, NULL); // Get lock

    printf("Thread 2 got read lock - %"PRIu64"\n", arg);
    fthSparseUnlock(slt, waitR);
    printf("Thread 2 released read lock - %"PRIu64"\n", arg);
    fthWaitEl_t *waitW = fthSparseLock(slt, arg, 1, NULL); // Write lock
    printf("Thread 2 got write lock - %"PRIu64"\n", arg);

    fthSparseUnlock(slt, waitW);
    printf("Thread 2 released write lock - %"PRIu64"\n", arg);
    
    if (__sync_fetch_and_add(&threadCount, -1) == 1) {
        printf("Thread 2 last man standing - %"PRIu64"\n", arg);
        fthKill(100);
    }
    
}

void threadRoutine1(uint64_t arg) {

    fthWaitEl_t *wait[100];
    printf("Thread 1 start\n");
    for (int i=0; i < 100; i++) {
        wait[i] = fthSparseLock(slt, i, 1, NULL); // Get 100 write locks
    }
    fthWaitEl_t *readLock = fthSparseLock(slt, 100, 0, NULL); // Get read lock
    printf("Thread 1 got locks\n");

    fthWaitEl_t *stall = fthLock(&lock1, 1, NULL); // Wait for write lock
    
    printf("Thread 1 Got stall lock\n");
    fthUnlock(stall);
    for (int i=0; i < 100; i++) {
        fthSparseUnlock(slt, wait[i]);        // Release 100 write locks
    }
    fthSparseUnlock(slt, readLock);            // release read lock
    printf("Thread 1 unlock complete\n");
    
}

void initThread(uint64_t arg) {
    fthWaitEl_t *wait = fthLock(&lock1, 1, NULL);
    slt = FTH_MALLOC(FTH_SPARSE_LOCK_TABLE_SIZE(16));
    fthSparseLockTableInit(slt, 16);
    
    fthResume(fthSpawn(&threadRoutine1, 4096), 0);
    fthYield(100);
    for (int i=0; i<100; i++) {
        fthResume(fthSpawn(&threadRoutine2, 4096), i);
    }
    fthYield(1000);
    fthUnlock(wait);

}


#define NUM_PTHREADS 4

void *pthreadRoutine(void *arg) {

    if ((uint64_t) arg == (NUM_PTHREADS - 1)) { // Wait for last scheduler
        XResume(XSpawn(&initThread, 4096), 0);
    }

    fthSchedulerPthread(0);
    
    printf("Scheduler %"PRIu64" halted\n", (uint64_t) arg);

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

    fthInitMultiQ(1,NUM_PTHREADS); // Tell the scheduler code NUM_PTHREADS schedulers starting up

    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_create(&pthread[i], NULL, &pthreadRoutine, (void *) i);
    }

    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_join(pthread[i], NULL);
    }

    printf("Done\n");

    plat_exit(exitRV);
}
