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
    mailShmem = shmem_cast(mail_sp, ptofMboxWait(xmbox));// step 7 fth get mail from ptof mail box, this method was called only by fth thread
    mail_t *mail = mail_sp_rwref(&mail, mailShmem);

    printf("fth %i get the mail % i .\n", arg, mail->mail);

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

int main(int argc, char **argv) {
    pthread_t pth;

    plat_shmem_trivial_test_start(argc, argv);

    fthInit();// step 2 init fth_t basic structure
    xmboxShmem = ptofMbox_sp_alloc();// step 3 malloc memory for xmbox and init it
    xmbox = ptofMbox_sp_rwref(&xmbox, xmboxShmem);
    
    ptofMboxInit(xmbox);// Is this method should be called after fthInit?
    
    pthread_create(&pth, NULL, &pthreadSchedRoutine, (void *) 0);//step 4 create a pthread which is used to spawn some fths

    sleep(1);                                // Give the scheduler a chance to init
    
    printf("pthread post %i data: \n", NUM_FTH);

    for (uint64_t i = 0; i < NUM_FTH; i++) {
        mail_sp_t mailShmem = mail_sp_alloc();// step 5 malloc memory for mail and set value for it
        mail_t *mail = mail_sp_rwref(&mail, mailShmem);
        mail->mail = i;
        printf("pthread post data %i\n", mail->mail);
        mail_sp_rwrelease(&mail);
        ptofMboxPost(xmboxShmem, shmem_cast(shmem_void, mailShmem));// step 6 pthread post a mail to ptof mail box, this method was called by pthread
    }

    printf("pthread post data complete\n");
    sleep(1);

    int rc = ptofMboxDestroy(xmbox);
    if (rc) {
        printf("Non zero RC from ptofMboxDestroy -  %i\n", rc);
        plat_exit(1);
        
    }

#ifdef notyet
    /* XXX: This test does not terminate the worker so it can't shutdown */
    plat_shmem_trivial_test_end();
#endif

    printf("test finished. \n");
    printf("In this file,the failed count is %d. \n", failed_count);
    return (failed_count);

    //plat_exit(0);

}
