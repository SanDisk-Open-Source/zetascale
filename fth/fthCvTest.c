/*
 * File:   fthCvTest.c
 * Author: Jonathan Bertoni
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */

#include <pthread.h>

#include "fthCv.h"
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
#include "platform/stdio.h"
#include "misc/misc.h"

#define SHM_SIZE 8 * 1024 *1024
#define NUM_PTHREADS 2

int           full;
int           iteration;
fthLock_t     lock;
fthCondVar_t  cond;
static int    remain = 2;

static void 
threadDone() {
   int after;

   after = __sync_sub_and_fetch(&remain, 1);
   plat_assert(after >= 0);
   if (!after) {
       fthKill(NUM_PTHREADS);
   }
}

void
threadA(uint64_t arg)
{
   int            i;
   fthWaitEl_t *  wait;

   for (i = 0; i < 1000; i++)
   {
      iteration = i;

      wait = fthLock(&lock, 1, NULL);
      full = 1;

      do {
          if (i & 1) {
             fthCvSignal(&cond);
          } else {
             fthCvBroadcast(&cond);
          }

          fthUnlock(wait);
          fthYield(1);
          wait = fthLock(&lock, 1, NULL);
      } while (full);

      fthUnlock(wait);
   }

   threadDone();
}

void
threadB(uint64_t arg)
{
   int            i;
   fthWaitEl_t *  wait;

   for (i = 0; i < 1000; i++)
   {
      wait = fthLock(&lock, 1, NULL);

      while (! full) {
         fthCvWait(&cond, &lock);
      }

      full = 0;
      fthUnlock(wait);
   }

   threadDone();
}

void *pthreadRoutine(void *arg) {
    fthSchedulerPthread(0);
    return (0);
}

int
main(void)
{
    pthread_t pthread[10];
    struct plat_shmem_config *shmem_config;
    int error;

    shmem_config = plat_alloc(sizeof(struct plat_shmem_config));
    plat_shmem_config_init(shmem_config);
    shmem_config->size = SHM_SIZE;
    plat_shmem_prototype_init(shmem_config);
    error = plat_shmem_attach(shmem_config->mmap);

    if (error) {
        plat_log_msg(20073, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                     PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                     shmem_config->mmap, plat_strerror(-error));
        plat_abort();
    }

    fthInitMultiQ(1, NUM_PTHREADS);

    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_create(&pthread[i], NULL, &pthreadRoutine, (void *) i);
    }

    fthLockInit(&lock);
    fthCvInit(&cond, &lock);

    fthResume(fthSpawn(threadA, 64 * 1024), 0);
    fthResume(fthSpawn(threadB, 64 * 1024), 0);

    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_join(pthread[i], NULL);
    }

    printf("Done.\n");
    plat_exit(0);
}
