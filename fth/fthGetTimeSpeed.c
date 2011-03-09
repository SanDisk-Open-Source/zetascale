/*
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/fth/fthGetTimeSpeed.c $
 * Author: drew
 *
 * Created on November 9, 2009
 * http://www.schoonerinfotech.com/
 *
 * $Id: fthGetTimeSpeed.c 10527 2009-12-12 01:55:08Z drew $
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */

/*
 * Quantify expected timing and therefore performance impact of
 * gettimeofday(2) and fthGetTimeOfDay() calls for binary logging
 */
#include <sys/time.h>
#include <assert.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "fth/fth.h"
#include "fth/fthOpt.h"
#include "platform/assert.h"
#define PLAT_OPTS_NAME(name) name ## _fthGetTimeSpeed
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/time.h"

#define PLAT_OPTS_ITEMS_fthGetTimeSpeed() \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    PLAT_OPTS_FTH()                                                            \
    item("npthread", "number of pthreads", NPTHREAD,                           \
         parse_int(&config->npthread, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)   \
    item("secs", "run time in seconds", SECS,                                  \
         parse_int(&config->secs, optarg, NULL), PLAT_OPTS_ARG_REQUIRED)       \
    item("multiq", "use multiq", MULTIQ,                                       \
         ({ config->multiq = 1; 0; }), PLAT_OPTS_ARG_NO)                       \
    item("nomultiq", "do not use multiq", NOMULTIQ,                            \
         ({ config->multiq = 0; 0; }),                                         \
         PLAT_OPTS_ARG_NO)                                                     \
    item("time", "use system time(2)", TIME,                                   \
         ({ config->mode = MODE_TIME; 0; }), PLAT_OPTS_ARG_NO)                 \
    item("clock_gettime", "use system cloc_gettime", CLOCK_GETTIME,            \
         ({ config->mode = MODE_CLOCK_GETTIME; 0; }), PLAT_OPTS_ARG_NO)        \
    item("gettimeofday", "use system gettimeofday", GETTIMEOFDAY,              \
         ({ config->mode = MODE_GETTIMEOFDAY; 0; }), PLAT_OPTS_ARG_NO)         \
    item("fthgettimeofday", "use fthGetTimeOfDay", FTHGETTIMEOFDAY,            \
         ({ config->mode = MODE_FTHGETTIMEOFDAY; 0; }), PLAT_OPTS_ARG_NO)      \
    item("fthgettimeofday_assert", "check fthGetTimeOfDay",                    \
         FTHGETTIMEOFDAY_ASSERT,                                               \
         ({ config->mode = MODE_FTHGETTIMEOFDAY_ASSERT; 0; }),                 \
         PLAT_OPTS_ARG_NO)                                                     \
    item("none", "use none", NONE,                                             \
         ({ config->mode = MODE_NONE; 0; }), PLAT_OPTS_ARG_NO)

#define LOG_CAT PLAT_LOG_CAT_FTH_TEST

enum mode {
    MODE_GETTIMEOFDAY,
    MODE_FTHGETTIMEOFDAY,
    MODE_FTHGETTIMEOFDAY_ASSERT,
    MODE_TIME,
    MODE_CLOCK_GETTIME,
    MODE_NONE
};

enum defaults {
    DEFAULT_NPTHREAD = 2,
    DEFAULT_LIMIT_SECS = 1,
    DEFAULT_MULTIQ = 1,
    DEFAULT_MODE = MODE_FTHGETTIMEOFDAY
};

struct plat_opts_config_fthGetTimeSpeed {
    struct plat_shmem_config shmem;
    int npthread;
    int secs;
    enum mode mode;
    unsigned multiq : 1;
};

struct state {
    struct plat_opts_config_fthGetTimeSpeed *config;
    int terminated;
    struct thread_state **ts;
};

struct thread_state {
    struct state *state;
    pthread_t pthread;
    long count;
    int index;
};


static volatile int done = 0;

static void
alarm_handler(int signo) {
    done = 1;
}

static void
time_main(uint64_t arg) {
    struct thread_state *ts;
    struct timeval now;
    struct timeval prev;
    struct timespec now_ts;
    long count;
    enum mode mode;

    ts = (struct thread_state *)arg;
    mode = ts->state->config->mode;

    now.tv_sec = 0;
    now.tv_usec = 0;

    prev = now;

    count = 0;
    do {
        ++count;
        switch (mode) {
        case MODE_FTHGETTIMEOFDAY:
            fthGetTimeOfDay(&now);
            break;
        case MODE_FTHGETTIMEOFDAY_ASSERT:
            fthGetTimeOfDay(&now);
            plat_assert(now.tv_sec > prev.tv_sec ||
                        (now.tv_sec == prev.tv_sec &&
                         now.tv_usec >= prev.tv_usec));
            prev = now;
            break;
        case MODE_GETTIMEOFDAY:
            gettimeofday(&now, NULL);
            break;
        case MODE_CLOCK_GETTIME:
            clock_gettime(CLOCK_REALTIME, &now_ts);
            break;
        case MODE_NONE:
            break;
        case MODE_TIME:
            now.tv_sec = time(NULL);
            break;
        }
    } while (!done);

    printf("thread %ld\n", count);

    ts->count = count;

    if (__sync_add_and_fetch(&ts->state->terminated, 1) ==
        ts->state->config->npthread) {
        fthKill(ts->state->config->npthread);
    }
}

static void *
pthread_main(void *arg) {
    fthSchedulerPthread(0);
    return (NULL);
}

int
main(int argc, char **argv) {
    int ret = 0;
    int shmem_attached = 0;
    struct plat_opts_config_fthGetTimeSpeed config;
    int status;
    int i;
    struct state *state;
    struct thread_state *thread_state;
    long count;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);

    config.npthread = DEFAULT_NPTHREAD;
    config.secs = DEFAULT_LIMIT_SECS;
    config.multiq = DEFAULT_MULTIQ;
    config.mode = DEFAULT_MODE;

    if (plat_opts_parse_fthGetTimeSpeed(&config, argc, argv)) {
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
        if (config.multiq) {
            fthInitMultiQ(1, config.npthread);
        } else {
            fthInit();
        }

        plat_calloc_struct(&state);
        plat_assert_always(state);
        state->config = &config;
        state->ts = plat_calloc(config.npthread, sizeof (state->ts[0]));
        plat_assert_always(state->ts);

        for (i = 0; i < state->config->npthread; ++i) {
            plat_calloc_struct(&thread_state);
            plat_assert_always(thread_state);
            thread_state->state = state;
            thread_state->index = i;
            state->ts[i] = thread_state;

            XResume(fthSpawn(&time_main, 40960), (uint64_t)thread_state);
        }

        signal(SIGALRM, alarm_handler);
        signal(SIGINT, alarm_handler);
        alarm(state->config->secs);

        for (i = 0; i < state->config->npthread; ++i) {
            status = pthread_create(&state->ts[i]->pthread, NULL,
                                    &pthread_main, NULL);
            plat_assert_always(!status);
        }

        count = 0;
        for (i = 0; i < state->config->npthread; ++i) {
            status = pthread_join(state->ts[i]->pthread, NULL);
            plat_assert_always(!status);
            count += state->ts[i]->count;
            plat_free(state->ts[i]);
        }

        plat_free(state->ts);
        plat_free(state);

        printf("%ld\n", count/config.secs);
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
