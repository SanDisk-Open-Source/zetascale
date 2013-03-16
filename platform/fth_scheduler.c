/*
 * File:   sdf/platform/fth_scheduler.c
 *
 * Author: drew
 *
 * Created on June 6, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fth_scheduler.c 10527 2009-12-12 01:55:08Z drew $
 */

#include "platform/closure.h"
#include "platform/logging.h"
#include "platform/stdlib.h"

#include "fth/fth.h"

#include "fth/fthMbox.h"

#include "fth_scheduler.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_CLOSURE, "fth_scheduler");

enum pfs_state {
    PFS_STATE_INITIAL,
    PFS_STATE_RUNNING,
    PFS_STATE_STOPPING
};

struct plat_fth_scheduler {
    plat_closure_scheduler_t base;

    /** @brief Number of threads configured */
    int nthreads_config;


    /** @brief Per-thread-state allocator */
    void *(*pts_alloc)(void *extra);

    /** @brief Extra argument passed into allocate/start functions */
    void *pts_alloc_extra;

    /** @brief Called with per-pthread-state on thread start */
    void (*pts_start)(void *pts);

    /** @brief Per-thread-state free */
    void (*pts_free)(void *pts);

    /** @brief Current state */
    enum pfs_state state;

    /** @brief Mbox of plat_closure_activation_base_t * */
    fthMbox_t work;

    /** @brief Number of threads which have to be terminated */
    int nthreads_running;

    /** @brief Reference count, 1 + nthreads_running */
    int ref_count;

    /** @brief Closure applied on shutdown */
    plat_closure_scheduler_shutdown_t shutdown;
};

static void pfs_main(uint64_t arg);
static void pfs_ref_count_dec(struct plat_fth_scheduler *pfs);
static plat_closure_activation_base_t *
pfs_alloc_activation(plat_closure_scheduler_t *self, size_t size);
static void
pfs_add_activation(plat_closure_scheduler_t *self,
                   plat_closure_activation_base_t *activation);
static void
pfs_shutdown(plat_closure_scheduler_t *self,
             plat_closure_scheduler_shutdown_t shutdown);

plat_closure_scheduler_t *
plat_fth_scheduler_alloc(int nthreads,
                         void *(*pts_alloc)(void *pts_extra),
                         void *pts_alloc_extra,
                         void (*pts_start)(void *pts),
                         void (*pts_free)(void *pts)) {
    plat_assert(nthreads > 0);

    struct plat_fth_scheduler *pfs;

    if (plat_calloc_struct(&pfs)) {
        pfs->base.alloc_activation_fn = &pfs_alloc_activation;
        pfs->base.add_activation_fn = &pfs_add_activation;
        pfs->base.shutdown_fn = &pfs_shutdown;

        pfs->nthreads_config = nthreads;

        pfs->pts_alloc = pts_alloc;
        pfs->pts_alloc_extra = pts_alloc_extra;
        pfs->pts_start = pts_start;
        pfs->pts_free = pts_free;

        pfs->state = PFS_STATE_INITIAL;
        fthMboxInit(&pfs->work);
        pfs->nthreads_running = 0;
        pfs->ref_count = 1;
        pfs->shutdown = plat_closure_scheduler_shutdown_null;
    }

    return (&pfs->base);
}

int
plat_fth_scheduler_start(plat_closure_scheduler_t *scheduler) {
    struct plat_fth_scheduler *pfs = (struct plat_fth_scheduler *)scheduler;
    int status;
    int ret;
    int i;
    fthThread_t *fth;

    plat_log_msg(20943, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "plat_fth_scheduler %p start called", pfs);

    status = __sync_bool_compare_and_swap(&pfs->state, PFS_STATE_INITIAL,
                                          PFS_STATE_RUNNING);
    plat_assert(status);

    ret = 0;
    for (i = 0; !ret && i < pfs->nthreads_config; ++i) {
        (void) __sync_add_and_fetch(&pfs->ref_count, 1);
        (void) __sync_add_and_fetch(&pfs->nthreads_running, 1);

        fth = fthSpawn(&pfs_main, 40960);
        if (!fth) {
            ret = -1;
        }

        if (!ret) {
            fthResume(fth, (uint64_t)pfs);
        } else {
            (void) __sync_sub_and_fetch(&pfs->nthreads_running, 1);
            pfs_ref_count_dec(pfs);
        }
    }

    if (!ret) {
        plat_log_msg(20944, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "plat_fth_scheduler %p started", pfs);
    } else {
        plat_log_msg(20945, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "plat_fth_scheduler %p failed to start ", pfs);
    }

    return (ret);
}

/**
 * @brief Main loop called once per thread
 *
 * Assumes pfs reference count arg has been incremented once for each
 * invocation, decrements reference count on termination>
 *
 * @param arg <IN> pfs structure cast for fth
 */
static void
pfs_main(uint64_t arg) {
    struct plat_fth_scheduler *pfs = (struct plat_fth_scheduler *)arg;
    void *pts;
    plat_closure_activation_base_t *activation;


    if (pfs->pts_alloc) {
        pts = (*pfs->pts_alloc)(pfs->pts_alloc_extra);
        plat_assert_always(pts);
    } else {
        pts = NULL;
    }

    if (pfs->pts_start) {
        (*pfs->pts_start)(pts);
    }

    plat_log_msg(20946, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "plat_fth_sched %p pts %p main starting", pfs, pts);

    while ((activation =
            (plat_closure_activation_base_t *)fthMboxWait(&pfs->work))) {
        plat_closure_scheduler_set(&pfs->base);
        (activation->do_fn)(&pfs->base, activation);
        plat_free(activation);
        plat_closure_scheduler_set(NULL);
    }

    if (pfs->pts_free) {
        (*pfs->pts_free)(pts);
    }

    (void) __sync_sub_and_fetch(&pfs->nthreads_running, 1);

    plat_log_msg(20947, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "plat_fth_sched %p pts %p main stopped", pfs, pts);

    pfs_ref_count_dec(pfs);
}

/**
 * @brief Decrement pfs reference count
 *
 * The reference count decrements on thread termination and user shutdown.
 * When it reaches zero the scheduler is destroyed.
 */
static void
pfs_ref_count_dec(struct plat_fth_scheduler *pfs) {
    int after;

    after = __sync_sub_and_fetch(&pfs->ref_count, 1);
    plat_assert(after >= 0);
    if (!after) {
        plat_log_msg(20948, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "plat_fth_scheduler %p shutdown complete", pfs);

        if (!plat_closure_scheduler_shutdown_is_null
            (&pfs->shutdown)) {
            if (pfs->shutdown.base.context == &pfs->base ||
                pfs->shutdown.base.context ==
                PLAT_CLOSURE_SCHEDULER_ANY) {
                (*pfs->shutdown.fn)(&pfs->base, pfs->shutdown.base.env);
            } else {
                plat_closure_apply(plat_closure_scheduler_shutdown,
                                   &pfs->shutdown);
            }
        }

        plat_free(pfs);
    }
}

static plat_closure_activation_base_t *
pfs_alloc_activation(plat_closure_scheduler_t *self, size_t size) {
    return (plat_closure_activation_base_t *)plat_alloc(size);
}

static void
pfs_add_activation(plat_closure_scheduler_t *self,
                   plat_closure_activation_base_t *activation) {
    struct plat_fth_scheduler *pfs = (struct plat_fth_scheduler *)self;

    plat_assert(pfs->state == PFS_STATE_RUNNING);
    fthMboxPost(&pfs->work, (uint64_t)activation);
}

static void
pfs_shutdown(plat_closure_scheduler_t *self,
             plat_closure_scheduler_shutdown_t shutdown) {
    struct plat_fth_scheduler *pfs = (struct plat_fth_scheduler *)self;
    enum pfs_state old_state;
    int status;
    int nthreads;
    int i;

    plat_log_msg(20949, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "plat_fth_scheduler %p shutdown called", pfs);

    old_state = pfs->state;
    plat_assert(old_state == PFS_STATE_INITIAL ||
                old_state == PFS_STATE_RUNNING);
    status = __sync_bool_compare_and_swap(&pfs->state, old_state,
                                          PFS_STATE_STOPPING);
    plat_assert(!status);

    pfs->shutdown = shutdown;

    nthreads = pfs->nthreads_running;
    for (i = 0; i < nthreads; ++i) {
        fthMboxPost(&pfs->work, 0);
    }

    pfs_ref_count_dec(pfs);
}
