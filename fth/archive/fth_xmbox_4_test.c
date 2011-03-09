/* author: Mac
 *
 * Created on May 5, 2008
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
#include "fthXMbox.h"
#include "applib/XMbox.h"
#include "applib/XMbox.c"
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

ptofMbox_sp_t xmboxShmem;
ptofMbox_t *xmbox;

typedef struct mail {
    int mail;
} mail_t;

PLAT_SP(mail_sp, mail_t);
PLAT_SP_IMPL(mail_sp, mail_t);


void fth_xmbox_test(uint64_t arg) {
  
    mail_sp_t mailShmem;
    printf("fth %i want to get the mail data. \n", arg);
    mailShmem = shmem_cast(mail_sp, ptofMboxTry(xmbox));
    mail_t *mail = mail_sp_rwref(&mail, mailShmem);
    
    ASSERT(mail == NULL, "xmbox has not any data, ptofMboxTry return immediately. ")

    mail_sp_rwrelease(&mail);
    mail_sp_free(mailShmem);

    return;
}
#define NUM_FTH 10
void *pthreadSchedRoutine(void *arg) {
    
    for (uint64_t i = 0; i < NUM_FTH; i++) {
        XResume(XSpawn(&fth_xmbox_test, 4096), i);
    }
    
    printf("Scheduler %i started\n", (uint64_t) arg);
    fthSchedulerPthread(0);    
    printf("Scheduler %i halted\n", (uint64_t) arg);

    return 0;

}


#define SHM_SIZE 8 * 1024 *1024

int main() {
    pthread_t pth;
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
    xmboxShmem = ptofMbox_sp_alloc();
    xmbox = ptofMbox_sp_rwref(&xmbox, xmboxShmem);
    
    ptofMboxInit(xmbox);
    
    pthread_create(&pth, NULL, &pthreadSchedRoutine, (void *) 0);

    sleep(1);                                // Give the scheduler a chance to init

    int rc = ptofMboxDestroy(xmbox);
    if (rc) {
        printf("Non zero RC from ptofMboxDestroy -  %i\n", rc);
        plat_exit(1);
        
    }

    fthKill(100);
    pthread_join(pth, NULL);

    printf("test finished. \n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);

    //plat_exit(0);

}
