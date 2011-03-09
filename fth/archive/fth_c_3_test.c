/* author: Mac
 *
 * Created on Apr 28, 2008
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
     	
#define QUESTION(expr, question) \
     if (expr) { \
         printf("*** QUESTION, TEST DESCRIPTION:%s *** \n", question); \
     }

int exitRV = 0;
int eligibleQ_size, freeWait_size, crossWaitQ_size;

int get_eligibleQ_size(fthThread_t *p) {
    eligibleQ_size = 0;
    while(p) {

        eligibleQ_size ++;
        p = p->threadQ.next;
    }
    return eligibleQ_size;

}

int get_freeWait_size(fthWaitEl_t *p) {
    freeWait_size = 0;
    while(p) {
        freeWait_size ++;
        p = p->waitQEl.next;
    }
    return freeWait_size;

}

int get_crossWaitQ_size(fthThread_t *p) {
    crossWaitQ_size = 0;
    while(p) {
        crossWaitQ_size ++;
        p = p->threadQ.next;
    }
    return crossWaitQ_size;

}

#define YIELDCOUNT 10
void fth_c_test_1(uint64_t arg) {

    int i;
    printf("in fth %d:\n", arg);
    printf("fth %d yield %d times:\n", arg, YIELDCOUNT);

    for(i = 0; i < YIELDCOUNT; i ++) {
        fthYield(i);
        printf("leave for statement of fth 1, the yield count is %d.\n", i);
    }
    printf("after fthYield: fth %d return. ", arg);
    fthKill(1);
}

void fth_c_test_2(uint64_t arg) {
    int i;  
    printf("in fth %d:\n", arg);
    for(i = 0; i < YIELDCOUNT; i ++) {
        fthYield(0);//if you call fthYield(0), this fth will get the exe right now.
        printf("leave for statement of fth 2, the yield count is always 0.\n");
    } 
#ifdef MULTIQ_SCHED
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ[0].head) == 1, "the size of eligibleQ is 1. ")
#else
    ASSERT(get_eligibleQ_size(fthBase()->eligibleQ.head) == 1, "the size of eligibleQ is 1. ")
#endif // MULTIQ_SCHED

}
#define WAITELNUM 10000
#define SHM_SIZE 8 * 1024 *1024
int main(void) {

    int index;
    fthWaitEl_t *temp[WAITELNUM];

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
    
    printf("*** === TEST FTHGETWAILWL, FTHFREEWAILEL AND FTHYIELD === ***\n");
    ASSERT(get_freeWait_size(fthBase()->freeWait.head) == 0, "the size of freeWait lll is 0. ")
    temp[0] = fthGetWaitEl();//the first time call this function,so it will create a new element.
    ASSERT(temp[0] != NULL, "get wait element. ")
    
    for(index = 1; index < WAITELNUM; index ++) {
        temp[index] = fthGetWaitEl();
        fthFreeWaitEl(temp[index]);

    }
    printf("random get two wail element:\n"); 
    ASSERT(temp[456] == temp[2344], "the same address. ")
    ASSERT(get_freeWait_size(fthBase()->freeWait.head) == 1, "the size of freeWait lll is 1. ")

    XResume(XSpawn(fth_c_test_1, 4096), 1);
    XResume(XSpawn(fth_c_test_2, 4096), 2);

    fthSchedulerPthread(0);
    printf("test finished. \n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);

                   
}

