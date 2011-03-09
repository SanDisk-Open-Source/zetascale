/* author: Mac
 *
 * Created on Apr 30, 2008
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

#define pf(...) printf(__VA_ARGS__);fflush(NULL)

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
    pf("fth %d about to get the write cross lock. \n", arg);
    fthXLock(&cross, 1);
    pf("fth %d get the write cross lock. \n", arg);
    for(index = 0; index < arg; index ++) {
       pf("fth %d print %d\n", arg, index);
       fthYield(0);
    }
    pf("fth %d releases the cross lock\n", arg);
    fthXUnlock(&cross);
    finished_fths ++;
    if(finished_fths == NUM_FTH)
       fthKill(100);
}

void *pthread_xlock_test_1(void *arg) {
    int index;
    //test if the write cross lock can be got currently?
    pf("pthread 1 want to get the write cross lock. \n");
    XLock(&cross, 1);
    pf("pthread 1 get the write cross lock. \n");

    for(index = 1; index <= COUNT; index ++){//just do printing
       pf("pthread %d print %d \n", (uint64_t)arg, index);
       pf("pthread 1 calls pthread_yield, times %d.\n", index);
       pthread_yield();
    }

    pf("pthread 1 release the write cross lock. \n");
    XUnlock(&cross);

    return 0;
}


void *pthread_xlock_test_2(void *arg) {
    int index;

    for (index = 1; index <= NUM_FTH; index ++) {
        XResume(XSpawn(&fth_xlock_test, 4096), index);
    }
    
    pf("pthread 2 want to get the write cross lock. \n");
    XLock(&cross, 1);
    pf("pthread 2 get the write cross lock. \n");

    //do nothing, just wait
    pf("pthread 2 release the write cross lock. \n");
    XUnlock(&cross);

    fthSchedulerPthread(0);

    pf("Scheduler %i halted\n", (uint64_t) arg);
    
    return 0;

}
void *pthread_xlock_test_3(void *arg) {
     fthSchedulerPthread(0);//just create one scheduler pthread
     pf("Scheduler %i halted\n", (uint64_t) arg);
     return 0;
}

int main(int argc, char **argv) {
    pthread_t pthread_1,pthread_2, pthread_3;

    pf("*** === TEST THE FUNCTION IN FTHXLOCK.C/XLOCK.C === ***\n");
    plat_shmem_trivial_test_start(argc, argv);

#ifdef MULTIQ_SCHED
    fthInitMultiQ(1,2); // Tell the scheduler code that there'll be 2 schedulers starting up
#else
    fthInit();
#endif 
    fthXLockInit(&cross);
    XLock(&cross, 1);
    pthread_create(&pthread_1, NULL, &pthread_xlock_test_1, (void *) 1);
    pthread_create(&pthread_3, NULL, &pthread_xlock_test_3, (void *) 3);
    pthread_create(&pthread_2, NULL, &pthread_xlock_test_2, (void *) 2);
    XUnlock(&cross);

    pthread_join(pthread_1, NULL);
    pthread_join(pthread_2, NULL);
    pthread_join(pthread_3, NULL);

    plat_shmem_trivial_test_end();

    pf("test finished. \n");
    pf("In this file,the failed count is %d. \n", failed_count);

    return (failed_count);
}
