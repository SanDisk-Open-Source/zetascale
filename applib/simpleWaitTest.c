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

