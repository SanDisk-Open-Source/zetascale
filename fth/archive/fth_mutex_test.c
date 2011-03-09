/* author: Mac
 *
 * Created on May 4, 2008
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
int eligibleQ_size;
static pthread_mutex_t mutex;

int get_eligibleQ_size(fthThread_t *p) {
    eligibleQ_size = 0;
    while(p) {
        eligibleQ_size ++;
        p = p->threadQ.next;
    }
    return eligibleQ_size;

}
#define LOCKTIMES 10
void fth_lock_test_1(uint64_t arg) {
    int index;
    fthMutexLock(&mutex);
    printf("fth %d get the mutex lock. \n", arg);
    for(index = 0; index < LOCKTIMES; index ++) {
       printf("fth %d print data %d. \n", arg, index);
       fthYield(1);
    }
    fthMutexUnlock(&mutex);
    printf("fth %d release the mutex lock. \n", arg);

}
#define FTHNUM 10
void fth_lock_test(uint64_t arg);
void fth_lock_test_2(uint64_t arg) {
    int index;
    printf("fth %d want to get the mutex lock. \n", arg);
    fthMutexLock(&mutex);
    printf("fth %d get the mutex lock. \n", arg);
    fthMutexUnlock(&mutex);
    printf("fth %d release the mutex lock. \n", arg);

    printf("fth %d spawn %d thread to test the mutex:\n", arg, FTHNUM);
    for(index = 0; index < FTHNUM; index ++)
       fthResume(fthSpawn(fth_lock_test, 4096), index);
}
void fth_lock_test(uint64_t arg) {
    int index;
    printf("fth %d want to get the mutex lock. \n", arg);
    fthMutexLock(&mutex);
    printf("fth %d get the mutex lock. \n", arg);
    for(index = 0; index <= arg; index ++) {
       printf("fth %d print the value %d. \n", arg, index);
       printf("fth %d yield. \n", arg);
       fthYield(1);
       printf("fth %d resume. \n", arg);
    }
    fthMutexUnlock(&mutex);
    printf("fth %d release the mutex lock. \n", arg);
    if(arg == FTHNUM - 1)
       fthKill(1);
    
}
int main(int argc, char **argv) {

    plat_shmem_trivial_test_start(argc, argv);

    fthInit();
    
    printf("*** === TEST THE FUNCTION IN FTHMEM.C === ***\n");
    pthread_mutex_init(&mutex, NULL);        // Init and grab the mutex
    XResume(XSpawn(fth_lock_test_1, 4096), 1);
    XResume(XSpawn(fth_lock_test_2, 4096), 2);

    fthSchedulerPthread(0);

    plat_shmem_trivial_test_end();

    printf("test finished. \n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);
}

