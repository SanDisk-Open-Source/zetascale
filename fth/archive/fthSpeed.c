/*
 * File:   fthSpeed.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthTest.h 396 2008-02-29 22:55:43Z jim $
 */

//
// Test program for many fth functions
//


#include "fth.h"
#include "fthMbox.h"
#include "fthThread.h"
#include "fthWaitEl.h"
#include "fthMbox.h"
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


fthMbox_t mbox;
fthLock_t lock;
uint64_t mem[3];

int iterations;
    
void threadRoutine1(uint64_t arg) {

    for (int i = 0; i < iterations; i++) {
        fthYield(0);
        //        printf("In %i/%i\n", arg, i);
    }

    fthKill(100);
}



#define SHM_SIZE 8 * 1024 *1024

int main(int argc, char **argv) {

    iterations = atoi(argv[1]);

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
    fthThread_t *thread1 = fthSpawn(&threadRoutine1, 4096);
    fthThread_t *thread2 = fthSpawn(&threadRoutine1, 4096);

    fthResume(thread1, 1);
    fthResume(thread2, 2);

    fthSchedulerPthread(0);

    return (0);
    
}
