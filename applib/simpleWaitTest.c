/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#include <stdio.h>
#include <inttypes.h>
#include "simpleWait.h"
#include "fth/fth.h"
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

#define SHM_SIZE 8 * 1024 *1024

ftopSimpleSignal_t sig;

void *
waiterPthread(void *arg) 
{
    ftopSimpleWait(&sig);
    printf("PThread BLAST OFF \n");
    ftopSimpleSignalDestroy(&sig);
    return NULL;
}

void
fthreadRoutine(uint64_t arg)
{
    printf("signalling the pthread in ... \n");
    for(int jj=5; jj >=0; jj--) {
        printf("%d..",jj);fflush(0);
        sleep(1);
    }
    ftopSimplePost(&sig);
}

void *
pthreadSchedRoutine(void *arg) 
{
    
    fthResume(fthSpawn(&fthreadRoutine, 8192), 0);

    printf("Scheduler %"PRIu64" started\n", (uint64_t) arg);
    fthSchedulerPthread(0);    
    printf("Scheduler %"PRIu64" halted\n", (uint64_t) arg);

    return (0);
}

int 
main()
{
    pthread_t  pthreads[10];
 
    ftopSimpleSignalInit(&sig);
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

    uint64_t ii =0;
    pthread_create(&pthreads[ii], NULL, &waiterPthread, (void *) ii);

    ii++;
    pthread_create(&pthreads[ii], NULL, &pthreadSchedRoutine, (void *) ii);

    sleep(1);

    for (int jj = 0; jj < 2; jj++) {
        pthread_join(pthreads[jj], NULL);
    }

    plat_exit(0);
}

