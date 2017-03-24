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
 * File:   sdf/platform/event.c
 * Author: drew
 *
 * Created on March 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: event.c 12530 2010-03-26 02:30:46Z drew $
 */

#define LOG_CAT LOG_CAT_PLATFORM_EVENT

#include "platform/closure.h"
#include "platform/event.h"
#include "platform/logging.h"
#include "platform/stdlib.h"

PLAT_CLOSURE(plat_event_void_closure);

/**
 * Operational states
 *
 * INITIAL: Initial operational state.  All consumers have yet to call
 * #plat_event_free and/or there are still pending #plat_event_fired_t.
 * activations.  The first call to plat_event_free() starts the shutdown process
 * which in turn should stop event generation, although already scheduled
 * activations will complete.
 *
 * IMPL_FREE_DONE: There are no remaining user references to this
 * and a non-null impl_free_done closure has been applied (but has
 * not yet completed).
 *
 * FREE_CLOSURES: There are no internal or external references to this.  Since
 * free closure invocation implies that the resources are gone and an
 * application may exit, check for memory leaks, etc. this must be freed
 * at the start of this step.
 *
 * item(caps, string, enter, zero ref event)
 */
#define STATE_ITEMS()                                                          \
    item(INITIAL, "initial", plat_assert(0),                                   \
         event_set_state(context, event, plat_event_impl_closure_is_null       \
                         (&event->impl_free_done) ? STATE_FREE_CLOSURES :      \
                         STATE_IMPL_FREE_DONE))                                \
    item(IMPL_FREE_DONE, "impl_free_done",                                     \
         event_impl_free_done_enter(context, event),                           \
         event_set_state(context, event, STATE_FREE_CLOSURES))                 \
    item(FREE_CLOSURES, "free_closures",                                       \
         event_free_closures_enter(context, event),                            \
         plat_assert(0))

enum event_state {
#define item(caps, text, enter, zero)                                          \
    STATE_ ## caps,
    STATE_ITEMS()
#undef item
};

/**
 * @brief Event, one shot or multi
 */
struct plat_event {
    /** @brief Statically allocated name */
    const char *name;

    /** @brief Logging identifier for debugging, etc. */
    int logcat;

    /** @brief Policy on closure activation scheduling */
    enum plat_event_policy policy;

    /** @brief User fired closure constructor arg */
    plat_event_fired_t fired;

    /**
     * @brief Derived class data
     *
     * The impl closures may have impl_data as their env field (with
     * their impl data including a pointer to this) or this in their
     * env field (in which case they need impl_data).
     */
    void *impl_data;

    /** @brief Implementation closure firing this */
    plat_event_impl_closure_t impl_fire;

    /** @brief Implementation closure scheduled after user closure runs */
    plat_event_impl_closure_t impl_fired_done;

    /** @brief Implementation closure scheduled when reset applied */
    plat_event_impl_closure_t impl_reset;

    /** @brief Implementation closure run before this free closure */
    plat_event_impl_closure_t impl_free;

    /** @brief Implementation closure called after free completes */
    plat_event_impl_closure_t impl_free_done;

    /**
     * @brief Closure applied to start the firing process
     *
     * May indirect to the user fired code or impl fire.
     */
    plat_event_void_closure_t fire_wrapper;

    /** @brief Closure applied to reset the event. */
    plat_event_void_closure_t reset_wrapper;

    /**
     * @brief Closure wrapping user fired wrapper
     *
     * May be invoked directly when there is no impl fire wrapper or indirectly
     * on its completion.
     */
    plat_event_void_closure_t fired_wrapper;
    plat_event_void_closure_t impl_fired_done_wrapper;

    enum event_state state;

    /** @brief Number of times fired but not yet delivered */
    int fire_count;

    /** @brief Delivery count when in normal mode of operation */
    int fire_count_delivered;

    /** Reference count by closure wrappers */
    int ref_count;

    /** Number of times free has been called */
    int free_called_count;

    /** @brief The number of entries in free_done */
    int free_done_count;

    /** @brief User arg to free method */
    plat_event_free_done_t free_done[0];
};

static void event_impl_fire_wrapper(plat_closure_scheduler_t *context,
                                    void *env);
static void event_fired_wrapper(plat_closure_scheduler_t *context, void *env);
static void event_impl_fired_done_wrapper(plat_closure_scheduler_t *context,
                                          void *env);
static void event_fire_done(plat_closure_scheduler_t *context,
                            struct plat_event *event);
static void event_reset_wrapper(plat_closure_scheduler_t *context, void *env);
static void event_impl_free_wrapper(plat_closure_scheduler_t *context,
                                    void *env);
static void __inline__
event_impl_free_done_enter(plat_closure_scheduler_t *context,
                           struct plat_event *event);
static void event_impl_free_done_wrapper(plat_closure_scheduler_t *context,
                                         void *env);
static void event_free_closures_enter(plat_closure_scheduler_t *context,
                                      void *env);
static void event_set_state(plat_closure_scheduler_t *context,
                            plat_event_t *event, enum event_state next);
static const char *event_state_to_string(enum event_state state);
static void event_do_free(plat_event_t *event);
static void event_ref_count_dec(plat_closure_scheduler_t *context,
                                plat_event_t *event);

plat_event_t *
plat_event_base_alloc(const char *name, int logcat,
                      enum plat_event_policy policy,
                      plat_event_fired_t fired,
                      int free_count,

                      void *impl_data,
                      plat_event_impl_closure_t impl_fire,
                      plat_event_impl_closure_t impl_fired_done,
                      plat_event_impl_closure_t impl_reset,
                      plat_event_impl_closure_t impl_free,
                      plat_event_impl_closure_t impl_free_done) {
    int i;

    plat_assert(free_count > 0);

    struct plat_event *event = plat_alloc(sizeof (*event) +
                                          sizeof (*event->free_done) *
                                          free_count);
    if (event) {
        event->name = name;
        event->logcat = logcat;
        event->policy = policy;

        event->fired = fired;

        event->impl_data = impl_data;

        event->impl_fire = impl_fire;
        event->impl_fired_done = impl_fired_done;
        event->impl_reset = impl_reset;
        event->impl_free = impl_free;
        event->impl_free_done = impl_free_done;

        event->fired_wrapper = plat_event_void_closure_create
            (event->fired.base.context, &event_fired_wrapper, event);

        if (!plat_event_impl_closure_is_null(&event->impl_fire)) {
            event->fire_wrapper = plat_event_void_closure_create
                (event->impl_fire.base.context,
                 &event_impl_fire_wrapper, event);
        } else {
            event->fire_wrapper = event->fired_wrapper;
        }

        if (!plat_event_impl_closure_is_null(&event->impl_fired_done)) {
            event->impl_fired_done_wrapper = plat_event_void_closure_create
                (event->impl_fired_done.base.context,
                 &event_impl_fired_done_wrapper, event);
        } else {
            event->impl_fired_done_wrapper = plat_event_void_closure_null;
        }

        if (!plat_event_impl_closure_is_null(&event->impl_reset)) {
            event->reset_wrapper = plat_event_void_closure_create
                (event->impl_reset.base.context, &event_reset_wrapper,
                 event);
        } else {
            event->reset_wrapper = plat_event_void_closure_null;
        }

        event->state = STATE_INITIAL;

        event->fire_count = 0;
        event->fire_count_delivered = 0;
        event->ref_count = free_count;
        event->free_called_count = 0;

        event->free_done_count = free_count;
        for (i = 0; i < free_count; ++i) {
            event->free_done[i] = plat_event_free_done_null;
        }

        plat_log_msg(20920, logcat, PLAT_LOG_LEVEL_TRACE,
                     "event %p name '%s' alloc free count %d", event,
                     event->name, free_count);
    }

    return (event);
}

plat_event_t *
plat_event_alloc(const char *name, int logcat,
                 enum plat_event_policy policy,
                 plat_event_fired_t fired,
                 int ref_count) {
    return (plat_event_base_alloc(name, logcat, policy, fired,
                                  ref_count, NULL,
                                  plat_event_impl_closure_null,
                                  plat_event_impl_closure_null,
                                  plat_event_impl_closure_null,
                                  plat_event_impl_closure_null,
                                  plat_event_impl_closure_null));
}

void
plat_event_free(plat_event_t *event, plat_event_free_done_t free_done) {
    plat_event_void_closure_t wrapper;
    int free_called_count_before;
    int ref_count_after;

    free_called_count_before =
        __sync_fetch_and_add(&event->free_called_count, 1);

    plat_log_msg(20921, event->logcat, PLAT_LOG_LEVEL_TRACE,
                 "event %p name '%s' free free_called_count now %d", event,
                 event->name, free_called_count_before + 1);
    plat_assert(free_called_count_before < event->free_done_count);
    event->free_done[free_called_count_before] = free_done;

    if (!free_called_count_before &&
        !plat_event_impl_closure_is_null(&event->impl_free)) {
        ref_count_after = __sync_add_and_fetch(&event->ref_count, 1);

        plat_log_msg(21835, event->logcat, PLAT_LOG_LEVEL_TRACE,
                     "event %p name '%s' scheduling impl_free ref_count now %d",
                     event, event->name, ref_count_after);

        wrapper = plat_event_void_closure_create
            (event->impl_free.base.context, &event_impl_free_wrapper,
             event);
        plat_closure_apply(plat_event_void_closure, &wrapper);
    }

    event_ref_count_dec(NULL, event);
}

void
plat_event_fire(plat_event_t *event) {
    int fire_count_after;
    int ref_count_after;

    fire_count_after = __sync_add_and_fetch(&event->fire_count, 1);

    plat_assert(event->policy == PLAT_EVENT_POLICY_MULTI ||
                event->policy == PLAT_EVENT_POLICY_ONCE ||
                event->policy == PLAT_EVENT_POLICY_RESET);

    if (event->policy == PLAT_EVENT_POLICY_MULTI || 1 == fire_count_after) {
        ref_count_after = __sync_add_and_fetch(&event->ref_count, 1);
        plat_log_msg(20923, event->logcat, PLAT_LOG_LEVEL_TRACE,
                     "event %p name '%s' fire fire_count %d (incremented)"
                     " ref_count %d (incremented)", event, event->name,
                     fire_count_after, ref_count_after);

        plat_closure_apply(plat_event_void_closure, &event->fire_wrapper);
    } else {
        plat_log_msg(20924, event->logcat, PLAT_LOG_LEVEL_TRACE,
                     "event %p name '%s' fire count %d (incremented)", event,
                     event->name, fire_count_after);
    }
}

void
plat_event_reset(plat_event_t *event) {
    int ref_count_after;

    if (plat_event_void_closure_is_null(&event->reset_wrapper)) {
        ref_count_after = __sync_add_and_fetch(&event->ref_count, 1);

        plat_log_msg(20925, event->logcat, PLAT_LOG_LEVEL_TRACE,
                     "event %p name '%s' reset ref_count %d (incremented)", event,
                     event->name, ref_count_after);

        plat_closure_apply(plat_event_void_closure, &event->reset_wrapper);
    } else {
        plat_log_msg(20926, event->logcat, PLAT_LOG_LEVEL_TRACE,
                     "event %p name '%s' reset nop", event, event->name);
    }
}

static void
event_impl_fire_wrapper(plat_closure_scheduler_t *context, void *env) {
    struct plat_event *event = (struct plat_event *)env;

    /*
     * XXX: drew 2009-01-04 It's possiblke for a fired event to be delivered
     * after free is called, although that's somewhat counter intuitive.
     * We might want to fix that.
     */
    plat_log_msg(20927, event->logcat, PLAT_LOG_LEVEL_TRACE,
                 "event %p name '%s' impl_fire", event, event->name);
    (*event->impl_fire.fn)(context, event->impl_fire.base.env, event);
    plat_closure_chain(plat_event_void_closure, context,
                       &event->fired_wrapper);
}

static void
event_fired_wrapper(plat_closure_scheduler_t *context, void *env) {
    struct plat_event *event = (struct plat_event *)env;

    plat_log_msg(20928, event->logcat, PLAT_LOG_LEVEL_TRACE,
                 "event %p name '%s' fired", event, event->name);

    switch (event->policy) {
    case PLAT_EVENT_POLICY_ONCE:
    case PLAT_EVENT_POLICY_RESET:
        event->fire_count_delivered = event->fire_count;
        break;
    case PLAT_EVENT_POLICY_MULTI:
        break;
    }

    (*event->fired.fn)(context, event->fired.base.env, event);

    if (plat_event_impl_closure_is_null(&event->impl_fired_done)) {
        event_fire_done(context, event);
    } else {
        plat_closure_chain(plat_event_void_closure, context,
                           &event->impl_fired_done_wrapper);
    }
}

static void
event_impl_fired_done_wrapper(plat_closure_scheduler_t *context, void *env) {
    struct plat_event *event = (struct plat_event *)env;

    plat_log_msg(20929, event->logcat, PLAT_LOG_LEVEL_TRACE,
                 "event %p name '%s' fired done", event, event->name);

    (*event->impl_fired_done.fn)
        (context, event->impl_fired_done.base.env, event);

    event_fire_done(context, event);
}

static void
event_fire_done(plat_closure_scheduler_t *context, struct plat_event *event) {
    int fire_count_after;

    plat_log_msg(20930, event->logcat, PLAT_LOG_LEVEL_TRACE,
                 "event %p name '%s' fire done", event, event->name);

    switch (event->policy) {
    case PLAT_EVENT_POLICY_ONCE:
    case PLAT_EVENT_POLICY_RESET:
    /* Re-fire if other events have crept in */
        fire_count_after =
            __sync_sub_and_fetch(&event->fire_count,
                                 event->fire_count_delivered);
        if (fire_count_after > 0) {
            plat_log_msg(20931, event->logcat,
                         PLAT_LOG_LEVEL_TRACE,
                         "event %p name '%s' fire count %d refire", event,
                         event->name, fire_count_after);
            plat_closure_apply(plat_event_fired, &event->fired, event);
        } else {
            event_ref_count_dec(context, event);
        }
        break;
    case PLAT_EVENT_POLICY_MULTI:
        event_ref_count_dec(context, event);
        break;
    }
}

static void
event_reset_wrapper(plat_closure_scheduler_t *context, void *env) {
    struct plat_event *event = (struct plat_event *)env;

    plat_log_msg(20932, event->logcat, PLAT_LOG_LEVEL_TRACE,
                 "event %p name '%s' impl_reset", event, event->name);
    (*event->impl_reset.fn)(context, event->impl_reset.base.env, event);
    event_ref_count_dec(context, event);
}

static void
event_impl_free_wrapper(plat_closure_scheduler_t *context, void *env) {
    struct plat_event *event = (struct plat_event *)env;

    plat_log_msg(20933, event->logcat, PLAT_LOG_LEVEL_TRACE,
                 "event %p name '%s' impl_free", event, event->name);

    (*event->impl_free.fn)(context, event->impl_free.base.env, event);

    event_ref_count_dec(context, event);
}

static void __inline__
event_impl_free_done_enter(plat_closure_scheduler_t *context,
                           struct plat_event *event) {
    plat_event_void_closure_t wrapper;

    (void) __sync_add_and_fetch(&event->ref_count, 1);

    wrapper = plat_event_void_closure_create
        (event->impl_free_done.base.context,
         &event_impl_free_done_wrapper, event);
    plat_closure_chain(plat_event_void_closure, context, &wrapper);
}

static void
event_impl_free_done_wrapper(plat_closure_scheduler_t *context, void *env) {
    struct plat_event *event = (struct plat_event *)env;

    plat_log_msg(20934, event->logcat, PLAT_LOG_LEVEL_TRACE,
                 "event %p name '%s', impl_free_done", event, event->name);

    (*event->impl_free_done.fn)(context, event->impl_free_done.base.env, event);
    event_ref_count_dec(context, event);
}

static void
event_free_closures_enter(plat_closure_scheduler_t *context, void *env) {
    struct plat_event *event = (struct plat_event *)env;
    int i;

    for (i = 0; i < event->free_done_count; ++i) {
        if (!plat_event_free_done_is_null(&event->free_done[i])) {
            plat_closure_chain(plat_event_free_done, context,
                               &event->free_done[i]);
        }
    }

    event_do_free(event);
}

static void
event_set_state(plat_closure_scheduler_t *context, plat_event_t *event,
                enum event_state next) {

    plat_log_msg(20935, event->logcat,  PLAT_LOG_LEVEL_TRACE,
                 "event %p name '%s' state %s->%s", event, event->name,
                 event_state_to_string(event->state),
                 event_state_to_string(next));
 
    switch (next) {
#define item(caps, text, enter, zero)                                          \
    case STATE_ ## caps:                                                       \
        event->state = next;                                                   \
        enter;                                                                 \
        break;
    STATE_ITEMS()
#undef item
    /* No default so compiler can warn & -Werror  */
    }
}

static const char *
event_state_to_string(enum event_state state) {
    switch (state) {
#define item(caps, text, enter, zero)                                          \
    case STATE_ ## caps: return (text);
    STATE_ITEMS()
#undef item
    }

    /* No default so compiler can warn & -Werror  */
    plat_assert(0);
    return (NULL);
}

static void
event_ref_count_dec(plat_closure_scheduler_t *context, plat_event_t *event) {
    int ref_count_after;

    ref_count_after = __sync_sub_and_fetch(&event->ref_count, 1);
    if (!ref_count_after) {
        switch (event->state) {
#define item(caps, text, enter, zero)                                          \
        case STATE_ ## caps:                                                   \
        plat_log_msg(20936, event->logcat, PLAT_LOG_LEVEL_TRACE, \
                     "event %p name '%s' state %s ref_count 0", event,           \
                     event->name, text);                                       \
            zero;                                                              \
            break;
        STATE_ITEMS()
#undef item
        /* No default so compiler can warn & -Werror  */
        }
    }
}

static void
event_do_free(plat_event_t *event) {
    plat_log_msg(20937, event->logcat, PLAT_LOG_LEVEL_TRACE,
                 "event %p name '%s' event_do_free", event, event->name);
    plat_assert(!event->ref_count);
    plat_free(event);
}
