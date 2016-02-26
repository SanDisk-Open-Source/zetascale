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

#include <sched.h>

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

#include "sdfappcommon/XList.h"

#include "fth/fth.h"

int status = 0;

struct foo;
PLAT_SP(foo_sp, struct foo);

typedef struct foo {
    int myData;
    foo_sp_t next_foo_shmem_ptr;
} foo_t;

PLAT_SP_IMPL(foo_sp, struct foo);

XLIST_SHMEM_H(foo, next_foo_shmem_ptr);
XLIST_SHMEM_IMPL(foo, next_foo_shmem_ptr);


static  char *  backing_file = "/tmp/shmem";

#define NUM_TRIES 100000
#define NUM_PTHREADS 4


#define SHM_SIZE 8 * 1024 *1024

#include <stdio.h>

#define pf(...) printf(__VA_ARGS__); fflush(NULL);

static foo_sp_t check[NUM_TRIES*NUM_PTHREADS];
static int rands[NUM_TRIES*NUM_PTHREADS];

int randIndex = 0;
int saferand(void) {
    int index = __sync_fetch_and_add(&randIndex, 1) % (NUM_PTHREADS * NUM_TRIES);
    return rands[index];
}

static foo_sp_t head;
static foo_sp_t tail;

int done_count = 0;
int start_count = 0;

void threadRoutine1(uint64_t arg) {
    pf("Thread %i start\n", arg);
    (void) __sync_add_and_fetch(&start_count, 1);
    while (start_count < NUM_PTHREADS) {
        
    }
    pf("Thread %i running\n", arg);
    
    int read_count = 0;
    int write_count = 0;
    while((read_count < NUM_TRIES) || (write_count < NUM_TRIES)) {
        if ((read_count < NUM_TRIES) && (saferand() & 1)) { // Randomly read or write
            // Read
            foo_sp_t foo_shmem = foo_xlist_dequeue(&head, &tail);
            if (foo_sp_is_null(foo_shmem) == 0) {
                foo_t *fp = foo_sp_rwref(&fp, foo_shmem);
                if (fp->myData < 0) {
                    pf("Got pointer %x with data %i\n", fp, fp->myData);
                    status = 1;
                }
                if (read_count + 10 > NUM_TRIES) {
                    pf("Reading %i, %i, %i\n", arg, read_count, fp->myData);
                }
                fp->myData= -1;
                foo_sp_rwrelease(&fp);
                read_count++;
            } else {
                
                //                sched_yield();
            }

        } else if (write_count < NUM_TRIES) {
            if (write_count + 10 > NUM_TRIES) {
                pf("Writing %i, %i\n", arg, write_count);
            }
            foo_xlist_enqueue(&head, &tail, check[write_count++ + (arg * NUM_TRIES)]);
        }
    }

    int num = __sync_add_and_fetch(&done_count, 1);
    pf("Thread %i done - count %i\n", arg, num);
    if (num == NUM_PTHREADS) {
        pf("Last man standing\n");
        for (int i = 0; i < NUM_TRIES * NUM_PTHREADS; i++) {
            foo_t *foo = foo_sp_rwref(&foo, check[i]);
            if (foo->myData != -1) {
                pf("Element %i not cleared\n", i);
                status = 1;
            }
        }

        fthKill(100);
        pf("Done - %i\n", status);
        plat_exit(status);
    }

}


void *pthreadSchedRoutine(void *arg) {

    (void) sched_setscheduler(0, SCHED_RR, NULL);
    nice(1);
    

    fthResume(fthSpawn(&threadRoutine1, 8192), (uint64_t) arg);

    pf("Scheduler %i started\n", (uint64_t) arg);
    fthSchedulerPthread(0);    
    pf("Scheduler %i halted\n", (uint64_t) arg);

    return (0);

}


int main(void) {

    pthread_t pthread[NUM_PTHREADS];

    int tmp;
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

    head = foo_sp_null;
    tail = foo_sp_null;
    for (int i = 0; i < NUM_TRIES * NUM_PTHREADS; i++) {
        rands[i] = rand();
        foo_sp_t foo_shmem = foo_sp_alloc();
        foo_t *foo = foo_sp_rwref(&foo, foo_shmem);
        foo->myData = i;
        check[i] = foo_shmem;
        foo_sp_rwrelease(&foo);
    }

    for (uint64_t i = 0; i < NUM_PTHREADS-1; i++) {
        pthread_create(&pthread[i], NULL, &pthreadSchedRoutine, (void *) i);
    }

    pthreadSchedRoutine((void *) NUM_PTHREADS-1);

    //    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        //        pthread_join(pthread[i], NULL);
    //  }

}

