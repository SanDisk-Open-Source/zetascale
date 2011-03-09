#ifndef PLATFORM_FTH_SCHEDULER_H
#define PLATFORM_FTH_SCHEDULER_H

/*
 * File:   sdf/platform/fth_scheduler.h
 * Author: drew
 *
 * Created on June 6, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fth_scheduler.h 1561 2008-06-10 20:56:00Z drew $
 */

/**
 * Provide a closure scheduler implementation which dispatches activations
 * out of multiple fth threads.  This is useful for adapting synchronous APIs
 * to more composable closure baseed ones.
 */
#include "platform/defs.h"

__BEGIN_DECLS

/**
 * @brief Allocate scheduler
 *
 * @param num_threads <IN> number of threads to create
 *
 * @param pts_alloc <IN> allocate per-thread structure; may be NULL for
 * no state needed.  Per-thread state is allocated before threads are
 * started.
 *
 * @param pts_alloc_extra <IN>  extra argument passed into pts_alloc_extra
 *
 * @param pts_start <IN> thread stated with given pts state from within
 * thread; may be NULL for no start functionality needed.  May be used to use
 * thread-specific state setting code.
 *
 * @param pts_free <IN> free per-thread state; may be NULL.
 *
 * @return Unstarted scheduler, NULl on failure.  Start with
 * #plat_fth_scheduler_start.  Shutdown with #plat_closure_scheduler_shutdown.
 */
struct plat_closure_scheduler *
plat_fth_scheduler_alloc(int num_threads, void *(*pts_alloc)(void *extra),
                         void *pts_alloc_extra, void (*pts_start)(void *pts),
                         void (*pts_free)(void *pts));

/**
 * @brief Start scheduler
 *
 * @return 0 on success, -1 on failure
 */
int plat_fth_scheduler_start(struct plat_closure_scheduler *sched);


__END_DECLS

#endif /* ndef PLATFORM_FTH_SCHEDULER_H */
