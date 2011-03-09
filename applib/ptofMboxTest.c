/*
 * File:   ptofMboxTest.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: XMboxTest.h 396 2008-02-29 22:55:43Z jim $
 */

//
// MPTest program for many fth functions
//


#include "fth/fth.h"
#include "fth/fthXMbox.h"
#include "applib/XMbox.h"
#include "sdfappcommon/XMbox.h"

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
#include <stdint.h>
#include <inttypes.h>


int threadCount = 0;

int checksum = 0;
int calcsum = 0;

ptofMbox_sp_t xmboxShmem;
ptofMbox_t *xmbox;

typedef struct mail {
    int mail;
} mail_t;

PLAT_SP(mail_sp, mail_t);
PLAT_SP_IMPL(mail_sp, mail_t);

static volatile int kill = 0;

void threadRoutine2(uint64_t arg) {
    
    printf("Thread 2 start - %"PRIu64" number %i\n", arg, __sync_fetch_and_add(&threadCount, 1));

    mail_sp_t mailShmem;
    mailShmem = shmem_cast(mail_sp, ptofMboxWait(xmbox));
    mail_t *mail = mail_sp_rwref(&mail, mailShmem);

    printf("Thread 2 released got MB element - %i/%"PRIu64"\n", mail->mail, arg);
    (void) __sync_fetch_and_add(&checksum, 17 * mail->mail);

    mail_sp_rwrelease(&mail);
    mail_sp_free(mailShmem);

    if (arg == 9) {
        int numTries = 0;
        while (1) {
            mailShmem = shmem_cast(mail_sp, ptofMboxTry(xmbox));
            if (mail_sp_is_null(mailShmem)) {
                numTries++;
                fthYield(0);
            } else {        
                mail_t *mail = mail_sp_rwref(&mail, mailShmem);
                printf("Thread 2 try got MB element after %i tries - %i/%"PRIu64"\n", numTries, mail->mail, arg);
                (void) __sync_fetch_and_add(&checksum, 23 * mail->mail);

                mail_sp_rwrelease(&mail);
                mail_sp_free(mailShmem);
                break;
            }
        }
    }


    return;
}

void threadRoutine3(uint64_t arg) {
    
    printf("Thread 3 start - %"PRIu64" number %i\n", arg, __sync_fetch_and_add(&threadCount, 1));

    mail_sp_t mailShmem;
    for (int i = 0; i < 10; i++) {
        mailShmem = shmem_cast(mail_sp, ptofMboxWait(xmbox));
        mail_t *mail = mail_sp_rwref(&mail, mailShmem);

        printf("Thread 3 released got MB element - %i/%"PRIu64"\n", mail->mail, arg);
        (void) __sync_fetch_and_add(&checksum, 19 * mail->mail);

        mail_sp_rwrelease(&mail);
        mail_sp_free(mailShmem);
    }

    kill = 1;
    
    return;
}

void *pthreadSchedRoutine(void *arg) {
    
    if ((uint64_t) arg == 0) {
        for (uint64_t i = 0; i < 10; i++) {
            fthResume(fthSpawn(&threadRoutine2, 40960), i);
        }
    } else if ((uint64_t) arg == 1) {
            fthResume(fthSpawn(&threadRoutine3, 40960), 10);        
    }
    
    printf("Scheduler %"PRIu64" started\n", (uint64_t) arg);
    fthSchedulerPthread(0);    
    printf("Scheduler %"PRIu64" halted\n", (uint64_t) arg);

    return (0);

}


#define NUM_PTHREADS 1

int 
main()
{
    pthread_t pthread[10];

    struct plat_shmem_config *shmem_config = plat_alloc(sizeof(struct plat_shmem_config));
    plat_shmem_config_init(shmem_config);
    plat_shmem_prototype_init(shmem_config);
    const char *path = plat_shmem_config_get_path(shmem_config);
    int tmp = plat_shmem_attach(path);
    if (tmp) {
        plat_log_msg(20073, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                     PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                     path, plat_strerror(-tmp));
        plat_abort();
    }

    fthInit();

    xmboxShmem = ptofMbox_sp_alloc();
    xmbox = ptofMbox_sp_rwref(&xmbox, xmboxShmem);
    
    ptofMboxInit(xmbox);
    
    pthread_create(&pthread[0], NULL, &pthreadSchedRoutine, (void *) 0);

    sleep(1);                                // Give the scheduler a chance to init
    
    printf("Posting first 10\n");

    for (uint64_t i = 0; i < 10; i++) {
        mail_sp_t mailShmem = mail_sp_alloc();
        mail_t *mail = mail_sp_rwref(&mail, mailShmem);
        mail->mail = i + 100;
        mail_sp_rwrelease(&mail);
        ptofMboxPost(xmboxShmem, shmem_cast(shmem_void, mailShmem));
        calcsum += (17 * (i+100));
    }

    printf("First 10 complete\n");
    sleep(1);
    
    mail_sp_t mailShmem = mail_sp_alloc();
    mail_t *mail = mail_sp_rwref(&mail, mailShmem);
    mail->mail = 300;
    mail_sp_rwrelease(&mail);
    ptofMboxPost(xmboxShmem, shmem_cast(shmem_void, mailShmem));
    printf("Singleton posted\n");
    calcsum += (23 * 300);
    sleep(1);

    for (uint64_t i = 0; i < 10; i++) {
        mail_sp_t mailShmem = mail_sp_alloc();
        mail_t *mail = mail_sp_rwref(&mail, mailShmem);
        mail->mail = i + 200;
        mail_sp_rwrelease(&mail);
        ptofMboxPost(xmboxShmem, shmem_cast(shmem_void, mailShmem));
        calcsum += (19 * (i+200));
    }
    
    printf("Second 10 posted\n");

    pthread_create(&pthread[1], NULL, &pthreadSchedRoutine, (void *) 1);

    while (kill == 0) {
        sleep(1);
    }

    fthKill(2);

    for (int i = 0; i < 2; ++i) {
        pthread_join(pthread[i], NULL);
    }

    int rc = ptofMboxDestroy(xmbox);
    if (rc) {
        printf("Non zero RC from ptofMboxDestroy -  %i\n", rc);
        plat_exit(1);
        
    }

    if (calcsum != checksum) {
        printf("Expected checksum of %i but got %i\n", calcsum, checksum);
        plat_exit(1);
    }

    printf("Checksum OK - was %i\n", checksum);
    
    plat_exit(0);

}
