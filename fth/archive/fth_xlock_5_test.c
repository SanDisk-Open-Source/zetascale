/* author: Mac
 *
 * Created on May 6, 2008
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
#define NUM_FTH 3
void fth_xlock_test(uint64_t arg) {
    int index;
    printf("fth %d want to get the write cross lock. \n", arg); fflush(NULL);
    fthXLock(&cross, 1);
    printf("fth %d get the write cross lock. \n", arg); fflush(NULL);

    for(index = 0; index < arg; index ++) {
       printf("fth %d print %d\n", arg, index); fflush(NULL);
       fthYield(1);//If you want to call fthYield between fthXLock and fthXUnlock, you should create more than two fth scheduler threads.
    }
    fthXUnlock(&cross);
    if(arg == NUM_FTH)
       fthKill(100);
}
void *pthread_xlock_test_3(void *arg) {
    fthSchedulerPthread(0);
    printf("scheduler %i halted.\n", (uint64_t) arg); fflush(NULL);
    return 0;
}

#define SHM_SIZE 8 * 1024 *1024

int main(int argc, char **argv) {
    int index;
    pthread_t pthread_1;

    printf("Pre shemm start\n");  fflush(NULL);

    plat_shmem_trivial_test_start(argc, argv);

    printf("Pre fthinit\n");  fflush(NULL);

#ifdef MULTIQ_SCHED
    fthInitMultiQ(1,2); // Tell the scheduler code 2 schedulers will be started up
#else
    fthInit();
#endif 

    printf("*** === TEST THE FUNCTION IN FTHXLOCK.C === ***\n"); fflush(NULL);
    fthXLockInit(&cross);
    XLock(&cross, 1);
    pthread_create(&pthread_1, NULL, &pthread_xlock_test_3, (void *) 1);
    for (index = 1; index <= NUM_FTH; index ++) {
        XResume(XSpawn(&fth_xlock_test, 4096), index);
    }
    XUnlock(&cross);
    fthSchedulerPthread(0);
    pthread_join(pthread_1, NULL);

    plat_shmem_trivial_test_end();

    printf("test finished. \n"); fflush(NULL);
    printf("In this file,the failed count is %d. \n", failed_count); fflush(NULL);
    return (failed_count);

}
