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
#include "applib/XLock.c"//may be there is something wrong with the lib , so i just include the source code
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
volatile int finished_fths = 0;
void fth_xlock_test(uint64_t arg) {
    int index;
    fthXLock(&cross, 1);
    printf("fth %d get the write cross lock. \n", arg);
    for(index = 0; index < arg; index ++) {
       printf("fth %d print %d\n", arg, index);
    }
    fthXUnlock(&cross);
    finished_fths ++;
    if(finished_fths == NUM_FTH)
       fthKill(100);
}

void *pthread_xlock_test_1(void *arg) {
    int index;
    printf("pthread %d want to get the write cross lock. \n", (uint64_t)arg);
    XLock(&cross, 1);
    printf("pthread %d get the write cross lock. \n", (uint64_t)arg);
    for(index = 0; index < COUNT; index ++){
       printf("pthread %d print %d \n", (uint64_t)arg, index);
       pthread_yield();
    }

    XUnlock(&cross);
    printf("pthread %d release the write cross lock. \n", (uint64_t)arg);
    return 0;
}


void *pthread_xlock_test_2(void *arg) {
    int index;
    printf("pthread %d want to get the write cross lock. \n", (uint64_t)arg);
    XLock(&cross, 1);
    printf("pthread %d get the write cross lock. \n", (uint64_t)arg);

    for (index = 1; index <= NUM_FTH; index ++) {
        XResume(XSpawn(&fth_xlock_test, 4096), index);
    }
    XUnlock(&cross);
    printf("pthread %d release the write cross lock. \n", (uint64_t)arg);

    fthSchedulerPthread(0);
    printf("scheduler %i halted\n", (uint64_t) arg);
    
    return 0;

}
void *pthread_xlock_test_3(void *arg) {
    fthSchedulerPthread(0);
    printf("scheduler %i halted.\n", (uint64_t) arg);
    return 0;
}

#define SHM_SIZE 8 * 1024 *1024

int main(void) {
    pthread_t pthread_1,pthread_2,pthread_3;
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
    fthInitMultiQ(1,2); // Tell the scheduler code 2 schedulers will be started up
#else
    fthInit();
#endif 

    printf("*** === TEST THE FUNCTION IN FTHXLOCK.C === ***\n");
    fthXLockInit(&cross);
    pthread_create(&pthread_1, NULL, &pthread_xlock_test_1, (void *) 1);
    pthread_create(&pthread_2, NULL, &pthread_xlock_test_2, (void *) 2);
    pthread_create(&pthread_3, NULL, &pthread_xlock_test_3, (void *) 3);

    pthread_join(pthread_1, NULL);
    pthread_join(pthread_2, NULL);
    pthread_join(pthread_3, NULL);
   
    printf("test finished. \n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);

}
