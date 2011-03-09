/*
 * File:   sdf/platform/tests/timeout_dispatchertest.c
 * Author: drew
 *
 * Created on April 2, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: timer_dispatchertest.c 10648 2009-12-17 21:52:37Z drew $
 */

/**
 * Test plat_timer_dispatcher.
 *
 * This test builds on top of #plat_mbox_scheduler which we'd like to keep
 * separate for leak detection, etc.
 */
#include <sys/queue.h>

#include <pthread.h>

#include "platform/closure.h"
#include "platform/event.h"
#include "platform/logging.h"
#include "platform/mbox_scheduler.h"
#define PLAT_OPTS_NAME(name) name ## _timeout_dispatchertest
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/time.h"
#include "platform/timer_dispatcher.h"

#include "fth/fth.h"
#include "fth/fthOpt.h"
#include "fth/fthMbox.h"

/* Placate cstyle by not matching the operator regex in timercmp calls */
#undef TIMERCMP_GREATER
#define TIMERCMP_GREATER >

#undef TIMERCMP_LESS
#define TIMERCMP_LESS <

/* Virtual time test ends at, as far in the future as we might get */
static struct timeval test_ends = { INT_MAX, 0 };

#define PLAT_OPTS_ITEMS_timeout_dispatchertest()                               \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    PLAT_OPTS_FTH()

struct plat_opts_config_timeout_dispatchertest {
    struct plat_shmem_config shmem;
};

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_TEST, "timer_dispatcher");

// FIXME: LOG_CAT should evaluate as an expression so this can be
// done with LOG_CAT as the parent.
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_TIMER, PLAT_LOG_CAT_PLATFORM_TEST,
                      "timer_dispatcher/timer")

PLAT_CLOSURE(test_closure);

struct test_state {
    /** @brief plat_mbox_closure_scheduler run out of fthread */
    plat_closure_scheduler_t *closure_scheduler;
    /**
     * @brief loop around timer dispatcher run out of second thread
     *
     * Loop blocks on simulated time passed in this->new_time.
     */
    struct plat_timer_dispatcher *timer_dispatcher;
    /** @brief non-zero iff fth_scheduler is running */
    int fth_running;

    /** @brief last time put in new_time; new entries are monotonic */
    int newest_seconds;
    /** @brief pass curent time to timer_dispatcher loop */
    fthMbox_t new_time;

    /** @brief current "time" */
    struct timeval now;

    /** @brief all timers which exist */
    LIST_HEAD(/* struct name */, timer_state) timer_list;
    /** timer to cancel */
    struct timer_state *cancel;

    /** @brief for each timer (free_done pending) and timer dispatcher main */
    int ref_count;
};

struct timer_state {
    /** @brief parent */
    struct test_state *test_state;

    /** @brief additional logic invoked for this timer firing (or NULL) */
    void (*fired_fn)(struct test_state *test_state,
                     struct timer_state *timer_state);

    /** @brief When this event was scheduled for */
    int when;

    /** @brief returned from #plat_timer_dispatcher_timer_alloc */
    struct plat_event *event;

    /** @brief free called count */
    int free_called;

    /** @brief entry in test_state->timer_list */
    LIST_ENTRY(timer_state) timer_list;
};

static void *pthread_main(void *arg);
static void start(plat_closure_scheduler_t *context, void *env);
static void timer1_fired(struct test_state *test_state,
                         struct timer_state *timer_state);
static void timer2_fired(struct test_state *test_state,
                         struct timer_state *timer_state);
static void timer4_fired(struct test_state *test_state,
                         struct timer_state *timer_state);
static void timer6_fired(struct test_state *test_state,
                         struct timer_state *timer_state);
static void cancel_fired(struct test_state *test_state,
                         struct timer_state *timer_state);

static void timer_dispatcher_main(uint64_t arg);
static void test_settime(struct test_state *test_state, int seconds);
static void test_gettime(plat_closure_scheduler_t *context, void *env,
                         struct timeval *ret);
static void test_inc_ref_count(struct test_state *test_state);
static void test_dec_ref_count(struct test_state *test_state);
static void test_closure_scheduler_shutdown(plat_closure_scheduler_t *context,
                                            void *env);

static struct timer_state *
timer_alloc(struct test_state *test_state,
            void (*fired_fn)(struct test_state *test_state,
                             struct timer_state *timer_state), int when);
static void timer_free(struct timer_state *timer_state);
static void timer_free_done(plat_closure_scheduler_t *context, void *env);
static void timer_fired(plat_closure_scheduler_t *context, void *env,
                        struct plat_event *event);

int
main(int argc, char **argv) {
    int ret = 0;
    int shmem_attached = 0;
    struct plat_opts_config_timeout_dispatchertest config;
    int status;
    struct test_state test_state;
    pthread_t pthread;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);

    if (plat_opts_parse_timeout_dispatchertest(&config, argc, argv)) {
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
    
        memset(&test_state, 0, sizeof (test_state));

        status = pthread_create(&pthread, NULL /* attr */, &pthread_main,
                                &test_state);
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

/**
 * @brief Test program main
 *
 * Since some flavors of fth implement #fthKill as pthread_exit, the test
 * code runs out of a single spearate pthread.
 */
static void *
pthread_main(void *arg) {
    struct test_state *test_state = (struct test_state *)arg;
    test_closure_t closure;
    plat_timer_dispatcher_gettime_t gettime;


    test_state->closure_scheduler = plat_mbox_scheduler_alloc();
    plat_assert_always(test_state->closure_scheduler);

    gettime = plat_timer_dispatcher_gettime_create
        (PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS, &test_gettime, test_state);
    test_state->timer_dispatcher = plat_timer_dispatcher_alloc(gettime);
    plat_assert_always(test_state->timer_dispatcher);

    fthResume(fthSpawn(&plat_mbox_scheduler_main, 40960),
              (uint64_t)test_state->closure_scheduler);
    fthResume(fthSpawn(&timer_dispatcher_main, 40960),
              (uint64_t)test_state);

    // For timer_dispatcher_main
    test_inc_ref_count(test_state);

    // fthMboxPost can only be called from fth threads
    closure = test_closure_create(test_state->closure_scheduler, &start,
                                  test_state);
    plat_closure_apply(test_closure, &closure);

    plat_log_msg(21768, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "fth scheduler starting");

    ++test_state->fth_running;
    fthSchedulerPthread(0);
    --test_state->fth_running;

    plat_assert_always(LIST_EMPTY(&test_state->timer_list));

    plat_log_msg(21769, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "fth scheduler stopped");

    return (NULL);
}

/**
 * @brief Start test
 *
 * Indirect because fthMboxPost can only be called from within an fthread
 */
static void
start(plat_closure_scheduler_t *context, void *env) {
    struct test_state *test_state = (struct test_state *)env;

    timer_alloc(test_state, &timer1_fired, 1);
    test_settime(test_state, 1);
}

/** @brief First timer fired, start second and advance time */
static void
timer1_fired(struct test_state *test_state, struct timer_state *timer_state) {
    plat_log_msg(21779, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "timer fired at %d seconds", timer_state->when);

    timer_alloc(test_state, NULL, 2);
    timer_alloc(test_state, &timer2_fired, 2);
    test_settime(test_state, 2);
}

/** @brief Next timer fired, start additional and advance time part way */
static void
timer2_fired(struct test_state *test_state, struct timer_state *timer_state) {
    plat_log_msg(21779, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "timer fired at %d seconds", timer_state->when);

    // In the past
    timer_alloc(test_state, NULL, 1);
    test_state->cancel = timer_alloc(test_state, &cancel_fired, 5);
    timer_alloc(test_state, NULL, 3);
    timer_alloc(test_state, timer4_fired, 4);
    timer_alloc(test_state, timer6_fired, 6);
    test_settime(test_state, 4);
}

/** @brief cancel a timer and advance time */
static void
timer4_fired(struct test_state *test_state, struct timer_state *timer_state) {
    plat_log_msg(21779, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "timer fired at %d seconds", timer_state->when);

    plat_assert_always(test_state->cancel);
    timer_free(test_state->cancel);
    test_state->cancel = NULL;
    test_settime(test_state, 7);
}

/** @brief last timer fired, advance time to end */
static void
timer6_fired(struct test_state *test_state, struct timer_state *timer_state) {
    plat_log_msg(21779, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "timer fired at %d seconds", timer_state->when);

    test_settime(test_state, test_ends.tv_sec);
}

/** @brief cancelled timer fired, fail test */
static void
cancel_fired(struct test_state *test_state, struct timer_state *timer_state) {
    plat_assert_always(0);
}

/**
 * @brief Thread main wrapped around timer dispatcher
 *
 * Blocks on time advance and then runs timer poll.
 */
static void
timer_dispatcher_main(uint64_t arg) {
    struct test_state *test_state = (struct test_state *)arg;
    struct timeval nextval;
    struct timeval *next;

    do {
        test_state->now.tv_sec = fthMboxWait(&test_state->new_time);

        // Loop until all events fired by events have been applied
        do {
            plat_timer_dispatcher_fire(test_state->timer_dispatcher);
            next = plat_timer_dispatcher_get_next(test_state->timer_dispatcher,
                                                  &nextval,
                                                  PLAT_TIMER_ABSOLUTE);
        } while (next && !timercmp(next, &test_state->now, TIMERCMP_GREATER));
    } while (timercmp(&test_state->now, &test_ends, TIMERCMP_LESS));

    plat_log_msg(21780, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "timer_dispatcher_main out of loop");

    test_dec_ref_count(test_state);
}

/** @brief set current time */
static void
test_settime(struct test_state *test_state, int seconds) {
    plat_assert_always(seconds >= test_state->newest_seconds);

    test_state->newest_seconds = seconds;
    fthMboxPost(&test_state->new_time, (uint64_t)seconds);
}

/** @brief get current time closure */
static void
test_gettime(plat_closure_scheduler_t *context,  void *env,
             struct timeval *ret) {
    struct test_state *test_state = (struct test_state *)env;
    *ret = test_state->now;
}

/** @brief increment test reference count by timers, timer_dispatcher */
static void
test_inc_ref_count(struct test_state *test_state) {
    ++test_state->ref_count;
}

/**
 * @brief decrement test reference count.
 *
 * When the reference count hits zero (which implies the test has reached
 * its end since the timer dispatcher does not terminate before then) the
 * closure scheduler is stopped.
 */
static void
test_dec_ref_count(struct test_state *test_state) {
    plat_closure_scheduler_shutdown_t shutdown;
    int after;

    after = __sync_sub_and_fetch(&test_state->ref_count, 1);


    if (!after) {
        plat_log_msg(21781, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "ref count 0");

        shutdown = plat_closure_scheduler_shutdown_create
            (test_state->closure_scheduler, &test_closure_scheduler_shutdown,
             test_state);
        plat_closure_scheduler_shutdown(test_state->closure_scheduler,
                                        shutdown);
    }
}

/**
 * @brief Closure scheduler done so terminate test
 *
 * This implies that #test_dec_ref_count has hit zero.
 */
static void
test_closure_scheduler_shutdown(plat_closure_scheduler_t *context, void *env) {
    struct test_state *test_state = (struct test_state *)env;

    plat_log_msg(21782, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "test_closure_scheduler_shutdown");

    plat_assert_always(test_state->fth_running);
    plat_assert_always(context == test_state->closure_scheduler);
    test_state->closure_scheduler = NULL;

    plat_log_msg(21783, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "freeing dispatcher");

    plat_timer_dispatcher_free(test_state->timer_dispatcher);
    test_state->timer_dispatcher = NULL;

    plat_log_msg(21784, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "dispatcher free");

    /*
     * Kill one fth scheduler.
     * XXX: The inability to kill specific fth schedulers makes
     * them a leaky abstraction.
     */
    fthKill(1);
}


/** @brief Allocate test timer, calling user function */
static struct timer_state *
timer_alloc(struct test_state *test_state,
            void (*fired_fn)(struct test_state *test_state,
                             struct timer_state *timer_state), int when) {
    struct timer_state *timer_state;
    plat_event_fired_t fired;
    struct timeval when_tv;

    timer_state = plat_calloc(1, sizeof (*timer_state));
    plat_assert_always(timer_state);
    timer_state->test_state = test_state;
    timer_state->fired_fn = fired_fn;
    timer_state->when = when;

    LIST_INSERT_HEAD(&test_state->timer_list, timer_state, timer_list);

    test_inc_ref_count(test_state);

    fired = plat_event_fired_create(test_state->closure_scheduler,
                                    &timer_fired, timer_state);

    when_tv.tv_sec = when;
    when_tv.tv_usec = 0;
    timer_state->event = plat_timer_dispatcher_timer_alloc
        (test_state->timer_dispatcher, "test", LOG_CAT_TIMER, fired,
         1 /* free_count */, &when_tv, PLAT_TIMER_ABSOLUTE,
         NULL /* rank_ptr */);
    plat_assert_always(timer_state->event);

    plat_log_msg(21785, LOG_CAT_TIMER, PLAT_LOG_LEVEL_TRACE,
                 "timer %p time %d now %d created",
                 timer_state, timer_state->when,
                 (int)test_state->now.tv_sec);

    return (timer_state);
}

/** @brief Free timer */
static void
timer_free(struct timer_state *timer_state) {
    int free_called_before;
    plat_event_free_done_t free_done;

    free_called_before = __sync_fetch_and_add(&timer_state->free_called, 1);

    // Current test incarnation only frees when a timer is fired and when
    // cancelling.

    plat_assert_always(!free_called_before);

    if (!free_called_before) {
        struct test_state *test_state = timer_state->test_state;

        plat_log_msg(21786, LOG_CAT_TIMER, PLAT_LOG_LEVEL_TRACE,
                     "timer %p time %d now %d free",
                     timer_state, timer_state->when,
                     (int)test_state->now.tv_sec);

        // Don't leave a dangling reference
        LIST_REMOVE(timer_state, timer_list);

        free_done = plat_event_free_done_create(test_state->closure_scheduler,
                                                &timer_free_done, timer_state);
        plat_event_free(timer_state->event, free_done);
    }
}

/** @brief Free timer has completed asynchronously */
static void
timer_free_done(plat_closure_scheduler_t *context, void *env) {
    struct timer_state *timer_state = (struct timer_state *)env;
    struct test_state *test_state = timer_state->test_state;

    plat_free(timer_state);
    test_dec_ref_count(test_state);
}

/** @brief Timer fired (all common code) */
static void
timer_fired(plat_closure_scheduler_t *context, void *env,
            struct plat_event *event) {
    struct timer_state *timer_state = (struct timer_state *)env;
    struct test_state *test_state = timer_state->test_state;

    plat_log_msg(21787, LOG_CAT_TIMER, PLAT_LOG_LEVEL_TRACE,
                 "timer %p time %d now %d fired",
                 timer_state, timer_state->when, (int)test_state->now.tv_sec);

    plat_assert_always(test_state->now.tv_sec >= timer_state->when);

    if (timer_state->fired_fn) {
        (*timer_state->fired_fn)(test_state, timer_state);
    }

    timer_free(timer_state);
}

#include "platform/opts_c.h"
