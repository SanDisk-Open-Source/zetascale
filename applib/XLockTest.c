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
 * File:   XLockTest.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: XLockTest.h 396 2008-02-29 22:55:43Z jim $
 */

//
// MPTest program for many fth functions
//


#include "fth/fth.h"
#include "fth/fthXLock.h"
#include "applib/XLock.h"
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

XLock_t crossLock1, crossLock2;

#define pf(...) printf(__VA_ARGS__); fflush(NULL);

int volatile threadCount;

void *pthreadRoutine2(void *arg) {
    pf("Thread 2 start - %p number %i\n", arg, __sync_fetch_and_add(&threadCount, 1));

    XLock(&crossLock1, 0);                   // Read lock

    pf("Thread 2 got read lock - %p\n", arg);
    XUnlock(&crossLock1);
    pf("Thread 2 released read lock - %p\n", arg);
    XLock(&crossLock2, 1);   // Write lock
    pf("Thread 2 got write lock - %p\n", arg);

    XUnlock(&crossLock2);
    pf("Thread 2 released write lock - %p\n", arg);
    
    if (__sync_fetch_and_add(&threadCount, -1) == 1) {
        pf("Thread 2 last man standing - %p\n", arg);
    }

    return (NULL);
}

void threadRoutine1(uint64_t arg) {
    pf("Thread 1 start\n");
    
    //    pf("Try lock F-W returns %i\n", fthXTryLock(&crossLock1, 1));

    fthXLock(&crossLock1, 1);                 // Write lock
    
    pf("Thread 1 Lock\n");
    fthYield(0);                             // Screw with the schedulers
    fthXUnlock(&crossLock1);
    pf("Thread 1 unlock complete\n");

    int lastCount = -1;
    while (1) {
        int count = threadCount;
        if (count != lastCount) {
            pf("Thread 1 - count is %i\n", count);
        }
        if (count == 0) {
            break;
        }

        lastCount = count;
    }

    fthKill(100);
    
}

// Must be at least 2
#define NUM_PTHREADS 4

void *pthreadSchedRoutine(void *arg) {
    if ((uint64_t) arg == (NUM_PTHREADS - 1)) { // Wait for last scheduler
        fthResume(fthSpawn(&threadRoutine1, 8192), 0);
    }

    pf("Scheduler %p started\n", arg);
    fthSchedulerPthread(0);    
    pf("Scheduler %p halted\n", arg);

    return (0);

}

int main(void) {
    pthread_t pthread[10];
    pthread_t pthread2[100];

    struct plat_shmem_config *shmem_config = plat_alloc(sizeof(struct plat_shmem_config));
    plat_shmem_config_init(shmem_config);
    plat_shmem_prototype_init(shmem_config);
    const char *path = plat_shmem_config_get_path(shmem_config);
    int tmp = plat_shmem_attach(path);
    if (tmp) {
        plat_log_msg(20073, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                     PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                     path, plat_strerror(-tmp));
        plat_abort();
    }

    fthInit();


    // Start FTH but not the threads
    for (uint64_t i = 0; i < NUM_PTHREADS-1; i++) {
        pthread_create(&pthread[i], NULL, &pthreadSchedRoutine, (void *) i);
    }

    fthXLockInit(&crossLock1);
    fthXLockInit(&crossLock2);
    XLock(&crossLock1, 1);                   // Write lock
    XLock(&crossLock2, 1);                   // Write lock

    // Start the pthreads
    for (uint64_t i = 0; i < 10; i++) {
        pthread_create(&pthread2[i], NULL, &pthreadRoutine2, (void *) i);
    }

    // Start the last scheduler and the FTH threads
    pthread_create(&pthread[NUM_PTHREADS-1], NULL, &pthreadSchedRoutine, (void *) NUM_PTHREADS-1);

    for (uint64_t i = 10; i < 20; i++) {
        pthread_create(&pthread2[i], NULL, &pthreadRoutine2, (void *) i);
    }

    pf("Pre lock release\n");
    XUnlock(&crossLock1);
    pf("Post lock1 release\n");
    XUnlock(&crossLock2);
    pf("Post lock2 release\n");

    
    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_join(pthread[i], NULL);
    }
    
    pf("Try lock X-W returns %i\n", XTryLock(&crossLock1, 1));
    pf("Try lock W-R returns %i\n", XTryLock(&crossLock1, 0));
    XUnlock(&crossLock2);
    pf("Try lock X-R returns %i\n", XTryLock(&crossLock1, 0));
    pf("Try lock R-W returns %i\n", XTryLock(&crossLock1, 1));
    XUnlock(&crossLock2);
           
}
