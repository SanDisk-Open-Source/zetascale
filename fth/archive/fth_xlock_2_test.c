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

XLock_t cross;

void *pthread_xlock_test(void *arg) {

    printf("pthread %d want to get the write cross lock.\n", arg);
    XLock(&cross, 1);
    printf("pthread %d get the write cross lock.\n", arg);

    printf("pthread %d release the write cross lock.\n", arg);
    XUnlock(&cross);
    return NULL;
}
#define NUM_FTH 10
#define NUM_PTHREADS 4
volatile int finished_fths = 0;
void fth_xlock_test(uint64_t arg) {

    printf("fth %d want to get the write cross lock.\n", arg);
    fthXLock(&cross, 1);           
    printf("fth %d get the write cross lock.\n", arg);

    printf("fth %d call fthYield method.\n", arg);
    fthYield(1);                  

    printf("fth %d release the write cross lock.\n", arg);
    finished_fths ++;
    fthXUnlock(&cross);

    if(finished_fths == NUM_FTH)
        fthKill(NUM_PTHREADS);//kill all scheduler~
    
}

// Must be at least 2
void *pthread_as_scheduler(void *arg) {
    if ((uint64_t) arg == (NUM_PTHREADS - 1)) { // Wait for last scheduler
        for(int index = 1; index <= NUM_FTH; index ++)
            XResume(XSpawn(&fth_xlock_test, 8192), index);
    }

    printf("scheduler %i started\n", (uint64_t) arg);
    fthSchedulerPthread(0);    
    printf("scheduler %i halted\n", (uint64_t) arg);

    return NULL;

}


#define SHM_SIZE 8 * 1024 *1024

int main(void) {
    pthread_t sched_pthread[10];
    pthread_t common_pthread[100];
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

#ifdef MULTIQ_SCHED
    fthInitMultiQ(1,NUM_PTHREADS); // Tell the scheduler code NUM_PTHREADS schedulers starting up
#else
    fthInit();
#endif 

    // Start scheduler pthreads,just as the fth scheduler
    for (uint64_t i = 0; i < NUM_PTHREADS-1; i++) {
        pthread_create(&sched_pthread[i], NULL, &pthread_as_scheduler, (void *) i);
        printf("create scheduler pthread %d completely.\n", i);
    }

    fthXLockInit(&cross);
    printf("the main pthread want to get the write cross lock.\n");
    XLock(&cross, 1);    
    printf("the main pthread get the write cross lock.\n");

    // Start the common pthreads
    for (uint64_t i = 0; i < 10; i++) {
        pthread_create(&common_pthread[i], NULL, &pthread_xlock_test, (void *) i);
        printf("create pthread %d completely.\n", i);
    }

    // Start the last scheduler and the fth threads
    pthread_create(&sched_pthread[NUM_PTHREADS-1], NULL, &pthread_as_scheduler, (void *) NUM_PTHREADS-1);
    printf("create scheduler pthread %d completely.\n", NUM_PTHREADS-1);

    // Start the common pthreads
    for (uint64_t i = 10; i < 20; i++) {
        pthread_create(&common_pthread[i], NULL, &pthread_xlock_test, (void *) i);
        printf("create pthread %d completely.\n", i);
    }

    printf("finish creating all pthreads and fths.\n");
    
    XUnlock(&cross);

    
    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_join(sched_pthread[i], NULL);
    }
    
    printf("test finished. \n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);

}
