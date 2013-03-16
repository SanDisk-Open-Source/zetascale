/*
 * File:   sdf/fth/fthIdleTest
 * Author: drew
 *
 * Created on March 16, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fthIdleTest.c 10527 2009-12-12 01:55:08Z drew $
 */

/**
 * Validate that the system reports itself as idle when it is, and not 
 * idle otherwise.  We can also perform timing measurements on context
 * switches.
 */

#include <sys/time.h>
#include <sys/resource.h>

#include "platform/assert.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _fthIdleTest
#include "platform/opts.h"
#include "platform/unistd.h"

#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "fth/fthOpt.h"

#define PLAT_OPTS_ITEMS_fthIdleTest()                                          \
    item("iterations", "number of iterations", ITERATIONS,                     \
         parse_uint64(&config->iteration_limit, optarg, NULL),                 \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("threads", "number of threads", THREADS,                              \
         parse_int(&config->thread_count, optarg, NULL),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("wake", "wake delay", WAKE,                                           \
         parse_int(&config->wake_delay, optarg, NULL),                         \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    PLAT_OPTS_FTH()

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_FTH, "fthIdleTest");

struct plat_opts_config_fthIdleTest {
    struct plat_shmem_config shmem;
    uint64_t iteration_limit;
    int thread_count;
    int wake_delay;
};

enum {
    DEFAULT_ITERATION_LIMIT = 1000000,
    DEFAULT_THREAD_COUNT = 2,
    DEFAULT_WAKE_DELAY = 1
};

struct state {
    struct plat_opts_config_fthIdleTest config;

    fthMbox_t mbox;
    uint64_t iteration;
    int threads_terminated;

    uint64_t memory_wait;
    pthread_t memory_wake_pthread;
};

static void
test_main(uint64_t arg) {
    struct state *state = (struct state *)arg;
    long iteration;
    int threads_terminated;

    do {
        iteration = __sync_add_and_fetch(&state->iteration, 1);
        fthYield(0);
    } while (iteration < state->config.iteration_limit);

    threads_terminated = __sync_add_and_fetch(&state->threads_terminated, 1);

    if (threads_terminated == state->config.thread_count) {
        fthKill(1);
    }
}


static void
memory_wake_fthread(uint64_t arg) {
    struct state *state = (struct state *)arg;

    uint32_t queueNum = fthMemQAlloc();
    fthMemWait(&state->memory_wait, queueNum);
    fthKill(1);
}

static void *
memory_wake_pthread(void *arg) {
    struct state *state = (struct state *)arg;

    sleep(state->config.wake_delay);
    state->memory_wait = 1;

    return (NULL);
}

int
main(int argc, char **argv) {
    int ret = 0;
    int shmem_attached = 0;
    int status;
    int i;
    struct rusage usage;
    struct rusage usage_before;
    long long idle_time;

    struct state state = {
        .config = {
            .iteration_limit = DEFAULT_ITERATION_LIMIT,
            .thread_count = DEFAULT_THREAD_COUNT,
            .wake_delay = DEFAULT_WAKE_DELAY

        }
    };

    plat_shmem_config_init(&state.config.shmem);

    if (plat_opts_parse_fthIdleTest(&state.config, argc, argv)) {
        ret = 2;
    }

    if (!ret) {
        status = plat_shmem_prototype_init(&state.config.shmem);
        if (status) {
            plat_log_msg(20876, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "shmem init failure: %s", plat_strerror(-status));
            ret = 1;
        }
    }

    if (!ret) {
        status =
            plat_shmem_attach(plat_shmem_config_get_path(&state.config.shmem));
        if (status) {
            plat_log_msg(20877, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "shmem attach failure: %s", plat_strerror(-status));
            ret = 1;
        } else {
            shmem_attached = 1;
        }
    }

    if (!ret) {
        fthInit();
    }

    if (!ret && state.config.thread_count > 0 && 
        state.config.iteration_limit > 0) {

        for (i = 0; i < state.config.thread_count; ++i) {
            XResume(XSpawn(test_main, 4096), (uint64_t)&state);
        }

        status = getrusage(RUSAGE_SELF, &usage_before);
        plat_assert_always(!status);

        fthSchedulerPthread(0);
        
        status = getrusage(RUSAGE_SELF, &usage);
        plat_assert_always(!status);
        plat_log_msg(20878, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "%lld nsec/switch",
                     (1000000000LL *
                      (usage.ru_utime.tv_sec - usage_before.ru_utime.tv_sec)
                      + 1000 *
                      (usage.ru_utime.tv_usec - usage_before.ru_utime.tv_usec)
                      ) / state.config.iteration_limit);

        idle_time = fthGetSchedulerIdleTime();
        plat_assert_always(!idle_time);
    }

    if (!ret && state.config.wake_delay > 0) {
        XResume(XSpawn(memory_wake_fthread, 4096), (uint64_t)&state); 
        status = pthread_create(&state.memory_wake_pthread, NULL,
                                memory_wake_pthread, &state);
        plat_assert_always(!status);
        fthSchedulerPthread(0);

        status = pthread_join(state.memory_wake_pthread, NULL);
        plat_assert_always(!status);

        idle_time = fthGetSchedulerIdleTime();
        plat_assert_always(idle_time > 0);
        plat_log_msg(20879, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "%lld usec idle", idle_time);
     }

     if (shmem_attached) {
        status = plat_shmem_detach();
        if (status) {
            plat_log_msg(20880, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "shmem detach failure: %s", plat_strerror(-status));
            ret = 1;
        }
    }

    plat_shmem_config_destroy(&state.config.shmem);

    return (ret);
}

#include "platform/opts_c.h"
