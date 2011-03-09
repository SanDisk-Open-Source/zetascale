/*
 * File:   XMboxTest.c
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
#include <inttypes.h>


int threadCount = 0;
int checksum = 0;
int calcsum = 0;

ftopMbox_sp_t xmboxShmem;
ftopMbox_t *xmbox;

typedef struct mail {
    int mail;
} mail_t;


PLAT_SP(mail_sp, mail_t);
PLAT_SP_IMPL(mail_sp, mail_t);

void *pthreadRoutine2(void *arg) {
    
    printf("Thread 2 start - %p number %i\n", arg, __sync_fetch_and_add(&threadCount, 1));

    mail_sp_t mailShmem;
    mailShmem = shmem_cast(mail_sp, ftopMboxWait(xmbox));
    mail_t *mail = mail_sp_rwref(&mail, mailShmem);

    printf("Thread 2 released got MB element - %i/%p\n", mail->mail, arg);
    (void) __sync_fetch_and_add(&checksum, 17 * mail->mail);

    mail_sp_rwrelease(&mail);
    mail_sp_free(mailShmem);

    return (NULL);
}

void *pthreadRoutine3(void *arg) {
    
    printf("Thread 3 start - %p number %i\n", arg, __sync_fetch_and_add(&threadCount, 1));

    mail_sp_t mailShmem;
    for (int i = 0; i < 10; i++) {
        mailShmem = shmem_cast(mail_sp, ftopMboxWait(xmbox));
        mail_t *mail = mail_sp_rwref(&mail, mailShmem);

        printf("Thread 3 released got MB element - %i/%p\n", mail->mail, arg);
        (void) __sync_fetch_and_add(&checksum, 19 * mail->mail);

        mail_sp_rwrelease(&mail);
        mail_sp_free(mailShmem);
    }

    int numTries = 0;
    while (1) {
        mailShmem = shmem_cast(mail_sp, ftopMboxTry(xmbox));
        if (mail_sp_is_null(mailShmem)) {
            numTries++;
        } else {        
            mail_t *mail = mail_sp_rwref(&mail, mailShmem);
            printf("Thread 3 try got MB element after %i tries - %i/%p\n", numTries, mail->mail, arg);
            (void) __sync_fetch_and_add(&checksum, 23 * mail->mail);
            mail_sp_rwrelease(&mail);
            mail_sp_free(mailShmem);
            break;
        }
      }

    return (NULL);
}

void threadRoutine1(uint64_t arg) {
    printf("Thread 1 start\n");

    for (uint64_t i = 0; i < 10; i++) {
        mail_sp_t mailShmem = mail_sp_alloc();
        mail_t *mail = mail_sp_rwref(&mail, mailShmem);
        mail->mail = i + 100;
        mail_sp_rwrelease(&mail);
        ftopMboxPost(xmbox, shmem_cast(shmem_void, mailShmem));
        calcsum += (17 * (i+100));
    }

    printf("Thread 1 first 10 complete\n");

    sleep(1);

    for (uint64_t i = 0; i < 10; i++) {
        mail_sp_t mailShmem = mail_sp_alloc();
        mail_t *mail = mail_sp_rwref(&mail, mailShmem);
        mail->mail = i + 200;
        mail_sp_rwrelease(&mail);
        ftopMboxPost(xmbox, shmem_cast(shmem_void, mailShmem));
        calcsum += (19 * (i+200));
    }
    
    printf("Thread 1 second 10 complete\n");
    sleep(2);
    printf("Thread 1 pre-final post\n");
    
    mail_sp_t mailShmem = mail_sp_alloc();
    mail_t *mail = mail_sp_rwref(&mail, mailShmem);
    mail->mail = 300;
    mail_sp_rwrelease(&mail);
    ftopMboxPost(xmbox, shmem_cast(shmem_void, mailShmem));
    calcsum += (23 * 300);

    printf("Thread 1 complete\n");
    
}

void *pthreadSchedRoutine(void *arg) {
    
    if (arg == 0) {
        fthResume(fthSpawn(&threadRoutine1, 4096), 0);
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
    pthread_t pthread2[100];

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

    xmboxShmem = ftopMbox_sp_alloc();
    xmbox = ftopMbox_sp_rwref(&xmbox, xmboxShmem);
    
    ftopMboxInit(xmbox);

    for (uint64_t i = 0; i < 10; i++) {
        pthread_create(&pthread2[i], NULL, &pthreadRoutine2, (void *) i);
    }

    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_create(&pthread[i], NULL, &pthreadSchedRoutine, (void *) i);
    }

    sleep(1);

    pthread_create(&pthread2[10], NULL, &pthreadRoutine3, (void *) 10);

    for (int i = 0; i < 11; i++) {
        pthread_join(pthread2[i], NULL);
    }

    fthKill(NUM_PTHREADS);

    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_join(pthread[i], NULL);
    }

    int rc = ftopMboxDestroy(xmbox);
    if (rc) {
        printf("Non zero RC from ftopMboxDestroy -  %i\n", rc);
        plat_exit(1);
        
    }

    if (calcsum != checksum) {
        printf("Expected checksum of %i but got %i\n", calcsum, checksum);
        plat_exit(1);
    }

    printf("Checksum OK - was %i\n", checksum);

    plat_exit(0);

}

