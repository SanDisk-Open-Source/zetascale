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

#include <stdio.h>
#include "common/sdftypes.h"
#include "threadPool.h"
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
#include "fth/fthXMbox.h"
#include "applib/XMbox.h"

#define STACK_SIZE (32*1024)
#define NUM_THREADS 10

//static  char *  backing_file = "/tmp/shmem";

#define SHM_SIZE 8 * 1024 *1024
typedef struct mail {
    int counter;
} mail_t;


PLAT_SP(mail_sp, mail_t);
PLAT_SP_IMPL(mail_sp, mail_t);


ptofMbox_sp_t mboxShmem;
ptofMbox_t *mbox;

waitObj_t waitee;
pthread_attr_t attr;
uint64_t test_rock;
pthread_t sched_thread;
threadPool_t * pool;
sem_t general_sem;
int test_fn_num_time_called=0;

void
test_fn(threadPool_t * pool, uint64_t mail)
{
    mail_sp_t mailShm;
    plat_assert_always(pool);
    test_fn_num_time_called++;
    /* The rock_ is what the user passes in at createThreadPool time.
       It can be any arbitrary pointer or any 64 bit quantity you like */
    plat_assert_always(pool->rock_ == 1);

    printf("Got mail %lld...", mail);
    uint64_to_shmem_ptr_cast(mailShm, mail);
    
    mail_t *local = mail_sp_rwref(&local, mailShm);
    
    printf("...with counter..%d...\n", local->counter);
    if(test_fn_num_time_called == 10){
        sem_post(&general_sem);
    }
    
}

void *
fthSchedPthread(void *arg)
{
//  get  the fthread engine going

    waitee.waitType = PTOF_WAIT;
    waitee.wu.ptof = mbox;
    test_rock = 1;
    pool = createThreadPool(test_fn, test_rock, &waitee,
                            NUM_THREADS, STACK_SIZE);

    printf("Got the new threadPool %p\n", pool);
    sem_post(&general_sem);
    printf("Posted the semaphore\n", pool);
    fthSchedulerPthread(0);

    printf("Scheduler halted\n");
    return NULL;
}

int
main ()
{
    int ret;
    int tmp;
    struct plat_shmem_config *shmem_config = plat_alloc(sizeof(struct plat_shmem_config));
    plat_shmem_config_init(shmem_config);
    plat_shmem_prototype_init(shmem_config);
    const char *path = plat_shmem_config_get_path(shmem_config);
    tmp = plat_shmem_attach(path);
    if (tmp) {
        plat_log_msg(20073, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                     PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                     path, plat_strerror(-tmp));
        plat_abort();
    }

    sem_init(&general_sem, 0, 0);

    fthInit();

    mboxShmem = ptofMbox_sp_alloc();
    plat_assert_always(!ptofMbox_sp_is_null(mboxShmem));
    mbox = ptofMbox_sp_rwref(&mbox, mboxShmem);
    
    ptofMboxInit(mbox);

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    printf("starting fth scheduler  pthread ....\n");
    if ((ret = pthread_create(&sched_thread, &attr, fthSchedPthread, NULL))) {
        plat_assert_always(0 == 1);
    }

    /* wait for all the threads to get created */

    plat_assert_always(0==sem_wait(&general_sem));

//    runThreadPool(pool);
/* kick off the mail blast */
    printf("Starting the mail blast 0\n");
    for(int ii=0; ii < NUM_THREADS ; ii++) {
        mail_sp_t mailShm = mail_sp_alloc();
        mail_t *mail = mail_sp_rwref(&mail, mailShm);
        mail->counter = ii;
        printf(" Posting mail with counter %d\n", mail->counter);
        ptofMboxPost(mboxShmem, shmem_cast(shmem_void, mailShm));
    }

    /* We wait for  all the messages to be delievered before exiting */
    plat_assert_always(0==sem_wait(&general_sem));

//    pthread_join(sched_thread, NULL);
    return 0;
}

