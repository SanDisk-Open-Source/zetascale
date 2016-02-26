/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/platform/tests/mbox_schedulertest.c
 * Author: drew
 *
 * Created on April 2, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mbox_schedulertest.c 10648 2009-12-17 21:52:37Z drew $
 */

/**
 * Test plat_mbox_scheduler
 */
#include <pthread.h>

#include "platform/closure.h"
#include "platform/event.h"
#include "platform/logging.h"
#include "platform/mbox_scheduler.h"
#define PLAT_OPTS_NAME(name) name ## _mbox_schedulertest
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/stdlib.h"
#include "platform/string.h"

#include "fth/fth.h"
#include "fth/fthOpt.h"

#define PLAT_OPTS_ITEMS_mbox_schedulertest()                                   \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    PLAT_OPTS_FTH()

struct plat_opts_config_mbox_schedulertest {
    struct plat_shmem_config shmem;
};

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_TEST, "mbox_scheduler");

struct test_state {
    plat_closure_scheduler_t *closure_scheduler;
    int expected;
    int limit;
    int fth_running;
};

PLAT_CLOSURE1(test_closure, int, val);

static void
test_count(plat_closure_scheduler_t *context, void *env, int val) {
    struct test_state *state = (struct test_state *)env;

    plat_log_msg(21766, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "test_count val=%d", val);

    plat_assert_always(state->fth_running);
    plat_assert_always(context == state->closure_scheduler);
    plat_assert_always(val == state->expected);
    ++state->expected;
}

static void
test_done(plat_closure_scheduler_t *context, void *env) {
    struct test_state *state = (struct test_state *)env;

    plat_log_msg(21767, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "test_done");

    plat_assert_always(state->fth_running);
    plat_assert_always(context == state->closure_scheduler);
    plat_assert_always(state->expected == state->limit);

    fthKill(100);
}

static void *
pthread_main(void *arg) {
    struct test_state *state = (struct test_state *)arg;
    test_closure_t closure;
    plat_closure_scheduler_shutdown_t shutdown;
    int i;

    state->closure_scheduler = plat_mbox_scheduler_alloc();

    /*
     * XXX: Since everything is calling the same libraries, a minimum
     * recommended stack size would probably be a good idea.
     */
    fthResume(fthSpawn(&plat_mbox_scheduler_main, 40960),
              (uint64_t)state->closure_scheduler);
    closure = test_closure_create(state->closure_scheduler, &test_count, state);
    for (i = 0; i < state->limit; ++i) {
        plat_closure_apply(test_closure, &closure, i);
    }

    shutdown = plat_closure_scheduler_shutdown_create
        (state->closure_scheduler, &test_done, state);
    plat_closure_scheduler_shutdown(state->closure_scheduler, shutdown);

    plat_log_msg(21768, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "fth scheduler starting");

    ++state->fth_running;
    fthSchedulerPthread(0);
    --state->fth_running;

    plat_log_msg(21769, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "fth scheduler stopped");

    return (NULL);
}

int
main(int argc, char **argv) {
    int ret = 0;
    int shmem_attached = 0;
    struct plat_opts_config_mbox_schedulertest config;
    int status;
    struct test_state state;
    pthread_t pthread;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);
   
    if (plat_opts_parse_mbox_schedulertest(&config, argc, argv)) {
        ret = 2;
    }

    if (!ret) {
        status = plat_shmem_prototype_init(&config.shmem);
        if (status) {
            plat_log_msg(20876, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "shmem init failure: %s", plat_strerror(-status));
            ret = 1;
        }
    }

    if (!ret) {
        status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
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
        memset(&state, 0, sizeof (state));
        state.limit = 3;

        status = pthread_create(&pthread, NULL /* attr */, &pthread_main,
                                &state);
        plat_assert_always(!status);

        status = pthread_join(pthread, NULL);
        plat_assert_always(!status);
    }

    if (shmem_attached) {
        status = plat_shmem_detach();
        if (status) {
            plat_log_msg(20880, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "shmem detach failure: %s", plat_strerror(-status));
            ret = 1;
        }
    }

    plat_shmem_config_destroy(&config.shmem);

    return (ret);
}

#include "platform/opts_c.h"
