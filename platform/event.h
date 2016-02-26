/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_EVENT_H
#define PLATFORM_EVENT_H 1

/*
 * File:   sdf/platform/event.h
 * Author: drew
 *
 * Created on March 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: event.h 13137 2010-04-23 03:58:08Z drew $
 */

/**
 * An event abstraction.
 *
 * The closure + scheduler combination which facilitates event driven
 * programming with cores dedicated to certain tasks and implicit locking,
 * but creates a disconnect between event generation and processing which
 * makes it harder to handle things like level active events which are
 * needed for scalable TCP servers.
 */
#include "platform/closure.h"
#include "platform/defs.h"

typedef struct plat_event plat_event_t;

/** @brief Closure invoked on event fire */
PLAT_CLOSURE1(plat_event_fired, struct plat_event *, event);

/**
 * @brief Closure invoked when #plat_event_free completes.
 *
 * The asynchronous decoupling means that scheduled (but not fired) closures
 * need to be reference counted which is a little ugly with light-weight
 * closures that are just a bound function pointer. This makes for an
 * asynchronous free where the closure gets applied when all pending
 * closures have run.
 */
PLAT_CLOSURE(plat_event_free_done);

/**
 * @brief Generic implementation closure
 *
 * This is used to
 * 1) Preserve the abstraction barrier between level active event generators
 * (like epoll, where we may want a single kernel thread blocking for all
 * socket consumers) and the consumers; where the generating code disables
 * the kernel event generator prior to invoking the first closure and relies
 * on the handler done closure to re-enable it thus allowing for arbitrary
 * levels of indirection between the two.
 *
 * 2) Accomodate scheduling/locking differences.  The fire and completion
 * functions can have different locking models since they're handled by way
 * of scheduler activations.
 */
PLAT_CLOSURE1(plat_event_impl_closure, struct plat_event *, event);

/** @brief type of event */
enum plat_event_policy {
    /**
     * @brief Closure will only be scheduled once at a time with multiple
     * fires before the closure execution translating into a single function
     * call.
     *
     * For fairness a fire count increase while the function is running
     * will translate into a rescheduling rather than a loop.
     */
    PLAT_EVENT_POLICY_ONCE,

    /**
     * @brief Closure may be scheduled multiple times.  Code wishing to limit
     * the number of outstanding closure activations can look at the reference
     * count.
     */
    PLAT_EVENT_POLICY_MULTI,

    /**
     * @brief The event will not fire again until the user calls
     * #plat_event_reset.
     */
    PLAT_EVENT_POLICY_RESET
};

__BEGIN_DECLS

/**
 * @brief Allocate event with hooks for service provider
 *
 * @param name <IN> Descriptive name used in log messages which remains 
 * owned by the caller but must not be freed until 
 * #plat_event_free completes asynchronously.
 *
 * XXX: drew 2010-04-22 Should eliminate that micro-optimization and just
 * make a copy of the name.
 *
 * @param logcat <IN> category for logging
 * @param policy <IN> whether closure should be scheduled multiple times.
 * @param fire_closure <IN> user closure applied when the event has fired
 * @param ref_count <IN> number of times #plat_event_free must be called
 *
 * @param impl_data <IN> impl data.  When impl data is not-null
 * the user should probably provide an impl_free_done to deallocate
 * it.  impl_data is available via #plat_event_get_impl_data
 *
 * @param impl_fire_closure <IN> start firing process
 * @param impl_fired_done_closure <IN> user closure complete
 * @param impl_free <IN> start free process.  Only invoked on the
 * first call to #plat_event_free
 * @param impl_free_done <IN> free process compelte.
 */
plat_event_t *
plat_event_base_alloc(const char *name, int logcat,
                      enum plat_event_policy policy,
                      plat_event_fired_t fired,

                      int ref_count,

                      void *impl_data,

                      plat_event_impl_closure_t impl_fire_closure,
                      plat_event_impl_closure_t impl_fired_done_closure,

                      plat_event_impl_closure_t impl_reset,

                      plat_event_impl_closure_t impl_free,
                      plat_event_impl_closure_t impl_free_done);


/**
 * @brief Allocate generic event closure
 *
 * @param name <IN> statically allocated name for user debugging purposes
 * @param logcat <IN> category for logging
 * @param policy <IN> whether closure should be scheduled multiple times.
 * @param fire_closure <IN> user closure applied when the event has fired
 * @param ref_count <IN> number of times #plat_event_free must be called
 */
plat_event_t *
plat_event_alloc(const char *name, int logcat,
                 enum plat_event_policy policy,
                 plat_event_fired_t fired,
                 int ref_count);

/**
 * @brief Fire event.
 *
 * Note that events returned by some interfaces may not be fireable by
 * users for synthetic operation.
 *
 * @param event <IN> event
 */
void plat_event_fire(plat_event_t *event);

/**
 * @brief Reset event
 *
 * Recurring events like incoming packets constructed with
 * #PLAT_EVENT_POLICY_RESET will not fire until re-armed with #plat_event_reset.
 *
 * #plat_event_reset can also be used for one-shot restartable events like
 * a timer expiration.
 */
void plat_event_reset(plat_event_t *event);

/**
 * @brief Free event closure
 *
 * @param event <IN> event
 * @param free <IN> applied when the outstanding closure count hits zero.
 */
void plat_event_free(plat_event_t *event, plat_event_free_done_t free_done);

/**
 * Hook for  impl class
 */
void *plat_event_get_impl_data(plat_event_t *event);

__END_DECLS

#endif /* ndef PLATFORM_EVENT_H */
