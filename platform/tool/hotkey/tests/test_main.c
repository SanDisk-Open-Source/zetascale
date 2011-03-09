/*
 * File: $URL:$
 *
 * Forked from ht_winner_lose.c on February 26, 2010
 *
 * $Id:$
 * 
 * Author:  Hickey Liu Gengliang Wang
 */

#include "test_main.h"

#include "platform/shmem.h"
#include "fth/fth.h"

static void test_wrapper(uint64_t arg);

int
main(int argc, char *argv[]) {
    struct plat_shmem_config *shmem_config = plat_alloc(sizeof(struct plat_shmem_config));
    plat_shmem_config_init(shmem_config);
    plat_shmem_prototype_init(shmem_config);
    int tmp = plat_shmem_attach(shmem_config->mmap);
    if (tmp) {
        plat_log_msg(20073, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                     PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                     shmem_config->mmap, plat_strerror(-tmp));
        plat_abort();
    }

    // Tell the scheduler code NUM_PTHREADS schedulers starting up
    fthInitMultiQ(1, NUM_PTHREADS);

#ifdef notyet
    /*
     * XXX: drew 2010-02-26 Without multiple fthreads this is 
     * meaningless because only one scheduler is used
     */
    pthread_t pthread[NUM_PTHREADS];
    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_create(&pthread[i], NULL, &pthreadRoutine, (void *) i);
    }

#endif
    fthResume(fthSpawn(test_wrapper, 64 * 1024), 0);

#ifdef notyet
    for (int i = 0; i < NUM_PTHREADS; i++) {
        pthread_join(pthread[i], NULL);
    }
#else
    fthSchedulerPthread(0);
#endif

    return (nfailed);    
}

/* Common fth thread startup and shutdown logic */
static void
test_wrapper(uint64_t arg) {
    threadTest(arg);

    fthKill(NUM_PTHREADS);
}
