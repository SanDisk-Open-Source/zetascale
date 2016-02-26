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
#include "fthMbox.h"
#include "fthThread.h"
#include "fthWaitEl.h"
#include "fthMem.h"
#include "fthMbox.h"
#include "fthMutex.h"
#include "fthXMbox.h"
#include "applib/XMbox.h"
#include "applib/XMbox.c"
#include "applib/XLock.h"
#include "applib/XLock.c"
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

ftopMbox_sp_t xmboxShmem;
ftopMbox_t *xmbox;
uint64_t post_data = 0;
typedef struct mail {
    int mail;
} mail_t;

PLAT_SP(mail_sp, mail_t);
PLAT_SP_IMPL(mail_sp, mail_t);


#define NUM_FTH 2
#define DATA_PER_FTH 10
#define NUM_PTHREAD 5
#define DATA_PER_PTHREAD NUM_FTH * DATA_PER_FTH / NUM_PTHREAD
int finished_pthread = 0;
void *pthread_xmbox_test(void *arg) {
  
    uint64_t index;
    mail_sp_t mailShmem;
    for(index = 0; index < DATA_PER_PTHREAD; index ++) {
       printf("pthread %i want to get mail data. \n", (uint64_t)arg);
       mailShmem = shmem_cast(mail_sp, ftopMboxWait(xmbox));
       mail_t *mail = mail_sp_rwref(&mail, mailShmem);

       printf("pthread %i get the mail % i .\n", (uint64_t)arg, mail->mail);
       mail_sp_rwrelease(&mail);
       mail_sp_free(mailShmem);
     }
    finished_pthread ++;
    if(finished_pthread == NUM_PTHREAD)
       fthKill(100);
    return 0;
}
void fth_xmbox_test(uint64_t arg);
void *pthreadSchedRoutine(void *arg) {

    for (uint64_t i = 1; i <= NUM_FTH; i++) {
        XResume(XSpawn(&fth_xmbox_test, 4096), i);
    }
    
    printf("fth scheduler start ....\n");
    fthSchedulerPthread(0);    
    printf("fth scheduler halt ....\n");

    return 0;

}

void fth_xmbox_test(uint64_t arg) {
    printf("fth %i post %i data: \n", arg, DATA_PER_FTH);

    for (uint64_t i = 0; i < DATA_PER_FTH; i++) {
        mail_sp_t mailShmem = mail_sp_alloc();
        mail_t *mail = mail_sp_rwref(&mail, mailShmem);
        mail->mail = ++ post_data;
        mail_sp_rwrelease(&mail);
        printf("fth %i post data %i\n", arg, post_data);
        ftopMboxPost(xmbox, shmem_cast(shmem_void, mailShmem));
        fthYield(0);// you can iterrupt the current fth~
    }

    printf("fth %i post data complete\n", arg);
    return;
}

#define SHM_SIZE 8 * 1024 *1024

int main() {
    pthread_t fth_sched, pth[NUM_PTHREAD];

    uint64_t index;
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
    xmboxShmem = ftopMbox_sp_alloc();
    xmbox = ftopMbox_sp_rwref(&xmbox, xmboxShmem);
    
    ftopMboxInit(xmbox);
  
    for(index = 0; index < NUM_PTHREAD; index ++)
       pthread_create(&pth[index], NULL, &pthread_xmbox_test, (void *)(index + 1) );
   
    sleep(1);
    pthread_create(&fth_sched, NULL, &pthreadSchedRoutine, NULL);
 
    
    pthread_join(fth_sched, NULL);
    for(index = 0; index < NUM_PTHREAD; index ++)
       pthread_join(pth[index], NULL);
    
    int rc = ftopMboxDestroy(xmbox);
    if (rc) {
        printf("Non zero RC from ptofMboxDestroy -  %i\n", rc);
        plat_exit(1);
        
    }
    printf("test finished. \n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);

    //plat_exit(0);

}
