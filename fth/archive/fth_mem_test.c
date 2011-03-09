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
#undef MULTIQ_SCHED
#ifdef MULTIQ_SCHED

int exitRV = 0;
int eligibleQ_size;

#define WAITCOUNT 50
static uint64_t mem[WAITCOUNT] = {0};


int get_eligibleQ_size(fthThread_t *p) {
    eligibleQ_size = 0;
    while(p) {
        eligibleQ_size ++;
        p = p->threadQ.next;
    }
    return eligibleQ_size;

}
#endif

void fth_mem_test_1(uint64_t arg) {
#ifdef MULTIQ_SCHED

    int index;
    uint64_t data;
    //printf("fth %d:\n", arg);
    for(index = 0; index < WAITCOUNT; index ++) {
       printf("fth %d want to get data. \n", arg);
       data = fthMemWait(&mem[index]);
       printf("fth %d get the wait memory data is %d\n", arg, data);
    }
#endif    
    fthKill(1);

}

#ifdef MULTIQ_SCHED

void fth_mem_test_2(uint64_t arg) {
    int index;
    //printf("fth %d:\n", arg);
    for(index = 0; index < WAITCOUNT; index ++) {
       mem[index] = index+1;
       printf("fth %d set the data %d. \n", arg, mem[index]);
       fthYield(1);
    }
}
#endif

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
    
    printf("*** === TEST THE FUNCTION IN FTHMEM.C === ***\n");

    
    XResume(XSpawn(fth_mem_test_1, 4096), 1);
#ifdef MULTIQ_SCHED
    XResume(XSpawn(fth_mem_test_2, 4096), 2);
#endif    

    fthSchedulerPthread(0);
    printf("test finished. \n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);

                   
}

