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

/*
 * File:   $URL:$
 * Author: drew
 *
 * Created on September 2, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id:$
 */

/* Validate that monotonic time actually is */

#include <pthread.h>
#include <semaphore.h>

#include "platform/assert.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _fthMonotonicTest
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/time.h"

#include "fth.h"
#include "fthMbox.h"
#include "fthOpt.h"

#define PLAT_OPTS_ITEMS_fthMonotonicTest() \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    PLAT_OPTS_FTH()                                                            \
    item("npthread", "number of pthreads", NPTHREAD,                           \
         parse_int(&config->npthread, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)   \
    item("nfthread", "number of fth threads", NFTHREAD,                        \
         parse_int(&config->nfthread, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)   \
    item("secs", "run time in seconds", SECS,                                  \
         parse_int(&config->secs, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)       \
    item("multiq", "use multiq", MULTIQ, ({ config->multiq = 1; 0; }),         \
         PLAT_OPTS_ARG_NO)                                                     \
    item("skew", "skew limit in usec", SKEW,                                   \
         parse_int(&config->skew_limit, optarg, NULL), PLAT_OPTS_ARG_REQUIRED) \
    item("nonmon", "don't use fthGetTimeMonotonic", NONMON,                    \
         ({ config->nonmon = 1; 0; }), PLAT_OPTS_ARG_NO)

#define LOG_CAT PLAT_LOG_CAT_FTH_TEST

enum {
    DEFAULT_NPTHREAD = 2,
    DEFAULT_NFTHREAD = 4,
    DEFAULT_LIMIT_SECS = 1,
    DEFAULT_MULTIQ = 0,
    DEFAULT_SKEW_LIMIT = 10 * PLAT_THOUSAND,
    DEFAULT_NONMON = 0

};

struct plat_opts_config_fthMonotonicTest {
    struct plat_shmem_config shmem;
    int npthread;
    int nfthread;
    int secs;
    int skew_limit;
    unsigned multiq : 1;
    unsigned nonmon : 1;
};

struct fthread_state {
    struct test_state *parent;
    int index;
    fthMbox_t mbox;

    int64_t now;
};

struct test_state {
    struct plat_opts_config_fthMonotonicTest *config;
    int64_t end_time;
    pthread_t *pthreads;
    struct fthread_state **fthread_state;
};

static int64_t
fth_get_time(struct fthread_state *state) {
    struct timeval now;

    if (state->parent->config->nonmon) {
        fthGetTimeOfDay(&now);
    } else {
        fthGetTimeMonotonic(&now);
    }
    return ((int64_t)now.tv_sec * PLAT_MILLION + now.tv_usec);
}

static void
fth_main(uint64_t arg) {
    struct fthread_state *state = (struct fthread_state *)arg;
    int64_t thread_last;
    int64_t other;
    int next_thread;

    next_thread = (state->index + 1) % state->parent->config->nfthread;

    if (!state->index) {
        fthMboxPost(&state->mbox, 0);
        state->parent->end_time = fth_get_time(state) +
            (int64_t)state->parent->config->secs * PLAT_MILLION;
    }

    do {
        thread_last = state->now;
        other = fthMboxWait(&state->mbox);
        plat_assert_always(other >= thread_last);
        state->now = fth_get_time(state);
        plat_assert_always(state->now >= other);
        fthMboxPost(&state->parent->fthread_state[next_thread]->mbox,
                    state->now);
    } while (state->now < state->parent->end_time);

    if (!next_thread) {
        fthKill(state->parent->config->npthread);
    }
}

static void *
pthread_main(void *arg) {
    fthSchedulerPthread(0);
    return (NULL);
}

static int64_t
get_time() {
    struct timespec now_ts;
    int64_t now_usec;

    clock_gettime(CLOCK_REALTIME, &now_ts);
    now_usec = (int64_t)now_ts.tv_sec * PLAT_MILLION +
        now_ts.tv_nsec / PLAT_THOUSAND;

    return (now_usec);
}

int
main(int argc, char **argv) {
    int ret = 0;
    int shmem_attached = 0;
    struct plat_opts_config_fthMonotonicTest config;
    int status;
    struct test_state *state;
    int i;
    struct fthread_state *fthread_state;
    int64_t now_usec;
    int64_t drift;
    const char *direction;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);
    config.npthread = DEFAULT_NPTHREAD;
    config.nfthread = DEFAULT_NFTHREAD;
    config.secs = DEFAULT_LIMIT_SECS;
    config.skew_limit = DEFAULT_SKEW_LIMIT;
    config.multiq = DEFAULT_MULTIQ;
    config.nonmon = DEFAULT_NONMON;

    if (plat_opts_parse_fthMonotonicTest(&config, argc, argv)) {
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
        plat_calloc_struct(&state);
        plat_assert_always(state);

        state->config = &config;
        if (state->config->multiq) {
            fthInitMultiQ(1, state->config->npthread);
        } else {
            fthInit();
        }

        now_usec = get_time();
        state->end_time = now_usec + (int64_t)state->config->secs *
            PLAT_MILLION;

        state->pthreads = plat_calloc(state->config->npthread,
                                      sizeof (state->pthreads[0]));
        plat_assert_always(state->pthreads);

        state->fthread_state = plat_calloc(state->config->nfthread,
                                           sizeof (state->fthread_state[0]));
        plat_assert_always(state->fthread_state);
        for (i = 0; i < state->config->nfthread; ++i) {
            plat_calloc_struct(&fthread_state);
            plat_assert_always(fthread_state);

            fthread_state->parent = state;
            fthread_state->index = i;
            fthMboxInit(&fthread_state->mbox);

            state->fthread_state[i] = fthread_state;
        }

        for (i = 0; i < state->config->npthread; ++i) {
            status = pthread_create(&state->pthreads[i], NULL /* attr */,
                                    &pthread_main, &state);
            plat_assert_always(!status);
        }

        for (i = 0; i < state->config->nfthread; ++i) {
            XResume(fthSpawn(&fth_main, 40960),
                    (uint64_t)state->fthread_state[i]);
        }

        for (i = 0; i < state->config->npthread; ++i) {
            status = pthread_join(state->pthreads[i], NULL);
            plat_assert_always(!status);
        }

        now_usec = get_time();
 
        for (i = 0; i < state->config->nfthread; ++i) {
            fthread_state = state->fthread_state[i];
            if (now_usec >= fthread_state->now) {
                drift = now_usec - fthread_state->now;
                direction = "behind";
            } else {
                drift = fthread_state->now - now_usec;
                direction = "ahead";
            }
            if (drift > state->config->skew_limit) {
                plat_log_msg(20895, LOG_CAT,
                             PLAT_LOG_LEVEL_FATAL,
                             "fth %ld usec %s with %ld allowed",
                             (long)drift, direction,
                             (long)state->config->skew_limit);
                plat_abort();
            }
            plat_free(fthread_state);
        }
        plat_free(state->pthreads);
        plat_free(state->fthread_state);
        plat_free(state);

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
