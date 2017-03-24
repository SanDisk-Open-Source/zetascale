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
 * File:   sdf/platform/timer_dispatcher.c
 * Author: drew
 *
 * Created on April 1, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: timer_dispatcher.c 13156 2010-04-23 21:23:01Z drew $
 */

/**
 * Timer dispatcher which multiplexes timer events onto one value which
 * can be polled or blocked on in select(2)/epoll(2)/etc.
 */
#include "sys/queue.h"

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/stdlib.h"
#include "platform/timer_dispatcher.h"
#include "platform/time.h"

#include "fth/fth.h"

/* Placate cstyle */
#undef TIMERCMP_LESS
#define TIMERCMP_LESS <

enum {
    /* Completely arbitrary */
    INITIAL_HEAP_CAPACITY  = 128
};

struct plat_timer_event;

/**
 * @brief Timer dispatcher
 *
 * The timer dispatcher maintains a priority queue (min heap implementation)
 * of all #plat_timer_event objects which have been created via
 * #plat_timer_dispatcher_alloc for the given dispatcher.
 *
 * When #plat_timer_dispatcher_poll is invoked, any which expired at or before
 * the time returned by #plat_timer_dispatcher_gettime_t are fired and freed.
 *
 * A limited reference counting ("free count") model is used to accomodate
 * asynchrony between consumers and producers.
 * #plat_timer_dispatcher_timer_alloc allocates timer events with a free count
 * one higher than the user requests.  Whatever code removes events from the
 * priority queue calls #plat_event_free to remove this reference.  During
 * normal operation #plat_timer_dispatcher_poll does it after removing the
 * timer from the heap.  When the user cancels timer #event_free_start
 * does the removal and corresponding free.
 */
struct plat_timer_dispatcher {
    /** @brief Lock held for heap manipulation including event->heap.index */
    struct fthLock lock;

    /** @brief Return current logical time */
    plat_timer_dispatcher_gettime_t gettime;

    /** @brief Number of times free has been called */
    int free_count;

    /**
     * @brief  Number of references by #plat_timer_event and self.
     *
     * This avoids some lock ordering issues
     */
    int ref_count;

    struct {
        /** @brief Allocated heap capacity, grows as a power of two */
        int capacity;
        /** @brief Current heap size */
        int count;
        /** @brief Min-heap of events */
        struct plat_timer_event **data;
    } heap;
};

struct plat_timer_event {
    /** @brief Event structure to call #plat_event_fire, etc. */
    struct plat_event *event;

    /** @brief Since the timer epoch */
    struct timeval when;

    /** @brief Parent. */
    struct plat_timer_dispatcher *dispatcher;

    /**
     * @brief Heap support logic.  Generic heap code will not depend on
     */
    struct {
        /** @brief Current index in the heap */
        int index;
    } heap;

    /** Queue of events being processed */
    STAILQ_ENTRY(plat_timer_event) queue;
};

static void dispatcher_do_free(struct plat_timer_dispatcher *dispatcher);
static void event_free_start(plat_closure_scheduler_t *context, void *env,
                             struct plat_event *base);
static void event_free_done(plat_closure_scheduler_t *context, void *env,
                            struct plat_event *base);
static void heap_remove_pre_locked(struct plat_timer_dispatcher *dispatcher,
                                   struct plat_timer_event *event);
static int heap_insert_pre_locked(struct plat_timer_dispatcher *dispatcher,
                                  struct plat_timer_event *event);
static __inline__ struct plat_timer_event *
heap_top_pre_locked(struct plat_timer_dispatcher *dispatcher);
static int heap_sift_down(struct plat_timer_dispatcher *dispatcher,
                          int index);
static int heap_sift_up(struct plat_timer_dispatcher *dispatcher, int index);
static void heap_swap(struct plat_timer_dispatcher *dispatcher, int first,
                      int second);
static __inline__ int heap_parent(struct plat_timer_dispatcher *dispatcher,
                                  int index);
static __inline__ int heap_left(struct plat_timer_dispatcher *dispatcher,
                                int index);
static __inline__ int heap_right(struct plat_timer_dispatcher *dispatcher,
                                 int index);
static __inline__ int heap_is_leaf(struct plat_timer_dispatcher *dispatcher,
                                   int index);

struct plat_timer_dispatcher *
plat_timer_dispatcher_alloc(plat_timer_dispatcher_gettime_t gettime) {
    struct plat_timer_dispatcher *ret;

    plat_assert(plat_timer_dispatcher_gettime_is_sync(&gettime));
    ret = plat_alloc(sizeof (*ret));
    if (ret) {
        fthLockInit(&ret->lock);
        ret->gettime = gettime;
        ret->free_count = 0;
        ret->ref_count = 1;
        ret->heap.capacity = INITIAL_HEAP_CAPACITY;
        ret->heap.count = 0;
        ret->heap.data = plat_alloc(ret->heap.capacity *
                                    sizeof (*ret->heap.data));
        if (!ret->heap.data) {
            plat_free(ret);
            ret = NULL;
        }
    }

    return (ret);
}

void
plat_timer_dispatcher_free(struct plat_timer_dispatcher *dispatcher) {
    fthWaitEl_t *unlock;
    struct plat_timer_event *event;
    struct plat_timer_event **events;
    int nevents;
    int before_free;
    int after_ref;
    int i;

    before_free = __sync_fetch_and_add(&dispatcher->free_count, 1);
    plat_assert(!before_free);

    unlock = fthLock(&dispatcher->lock, 1 /* write lock */, NULL);
    events = dispatcher->heap.data;
    nevents = dispatcher->heap.count;

    dispatcher->heap.data = NULL;
    dispatcher->heap.count = 0;
    dispatcher->heap.capacity = 0;

    for (i = 0; i < nevents; ++i) {
        event = events[i];
        event->heap.index = -1;
    }
    fthUnlock(unlock);

    for (i = 0; i < nevents; ++i) {
        event = events[i];
        plat_event_free(event->event, plat_event_free_done_null);
    }

    plat_free(events);

    after_ref = __sync_sub_and_fetch(&dispatcher->ref_count, 1);
    if (!after_ref) {
        dispatcher_do_free(dispatcher);
    }
}

int
plat_timer_dispatcher_fire(struct plat_timer_dispatcher *dispatcher) {
    STAILQ_HEAD(/* */, plat_timer_event) removed =
        STAILQ_HEAD_INITIALIZER(removed);
    struct plat_timer_event *event;
    struct timeval now;
    fthWaitEl_t *unlock;
    int ret;

    plat_closure_apply(plat_timer_dispatcher_gettime, &dispatcher->gettime,
                       &now);
    unlock = fthLock(&dispatcher->lock, 1 /* write lock */, NULL);
    plat_assert(!dispatcher->free_count);

    while ((event = heap_top_pre_locked(dispatcher)) &&
           (now.tv_sec > event->when.tv_sec ||
            (now.tv_sec == event->when.tv_sec &&
             now.tv_usec >= event->when.tv_usec))) {
        heap_remove_pre_locked(dispatcher, event);
        STAILQ_INSERT_TAIL(&removed, event, queue);
    }
    fthUnlock(unlock);

    ret = 0;
    while (!STAILQ_EMPTY(&removed)) {
        event = STAILQ_FIRST(&removed);
        STAILQ_REMOVE_HEAD(&removed, queue);
        plat_event_fire(event->event);
        plat_event_free(event->event, plat_event_free_done_null);
        ++ret;
    }

    return (ret);
}

struct timeval *
plat_timer_dispatcher_get_next(struct plat_timer_dispatcher *dispatcher,
                               struct timeval *next,
                               enum plat_timer_whence next_whence) {
    struct plat_timer_event *event;
    struct timeval now;
    struct timeval *ret;
    fthWaitEl_t *unlock;

    unlock = fthLock(&dispatcher->lock, 1 /* write lock */, NULL);
    if ((event = heap_top_pre_locked(dispatcher)) && next) {
        plat_closure_apply(plat_timer_dispatcher_gettime, &dispatcher->gettime,
                           &now);
        switch (next_whence) {
        case PLAT_TIMER_RELATIVE:
            timersub(&event->when, &now, next);
            break;
        case PLAT_TIMER_ABSOLUTE:
            *next = event->when;
        }
        ret = next;
    } else {
        ret = NULL;
    }
    fthUnlock(unlock);

    return (ret);
}

void
plat_timerdispatcher_gettime(struct plat_timer_dispatcher *dispatcher,
                             struct timeval *now) {
        plat_closure_apply(plat_timer_dispatcher_gettime, &dispatcher->gettime,
                           now);
}

struct plat_event *
plat_timer_dispatcher_timer_alloc(struct plat_timer_dispatcher *dispatcher,
                                  const char *name, int logcat,
                                  plat_event_fired_t fired,
                                  int free_count,
                                  const struct timeval *when,
                                  enum plat_timer_whence whence,
                                  int *rank_ptr) {
    int failed = 0;
    struct plat_timer_event *event = NULL;
    struct plat_event *ret = NULL;
    struct timeval now;
    fthWaitEl_t *unlock;

    event = plat_calloc(1, sizeof (*event));
    failed = !event;
    if (!failed) {
        switch (whence) {
        case PLAT_TIMER_RELATIVE:
            plat_closure_apply(plat_timer_dispatcher_gettime,
                               &dispatcher->gettime, &now);
            timeradd(when, &now, &event->when);
            break;
        case PLAT_TIMER_ABSOLUTE:
            event->when = *when;
            break;
        }
        event->dispatcher = dispatcher;
        event->heap.index = -1;
    }

    unlock = fthLock(&dispatcher->lock, 1 /* write lock */, NULL);
    if (!failed && heap_insert_pre_locked(dispatcher, event) < 0) {
        failed = 1;
    }

    if (!failed) {
        plat_event_impl_closure_t impl_free_closure =
            plat_event_impl_closure_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                           &event_free_start, event);
        plat_event_impl_closure_t impl_free_done_closure =
            plat_event_impl_closure_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                           &event_free_done, event);
        event->event = plat_event_base_alloc(name, logcat,
                                             PLAT_EVENT_POLICY_ONCE, fired,
                                             free_count + 1, event,
                                             /* fire */
                                             plat_event_impl_closure_null,
                                             /* fired done */
                                             plat_event_impl_closure_null,
                                             /* reset */
                                             plat_event_impl_closure_null,
                                             impl_free_closure,
                                             impl_free_done_closure);
        if (!event->event) {
            failed = 1;
        }
    }

    if (!failed) {
        plat_assert(event && event->event && event->heap.index != -1);
        ret = event->event;
        if (rank_ptr) {
            *rank_ptr = event->heap.index;
        }
        (void) __sync_fetch_and_add(&dispatcher->ref_count, 1);
    } else {
        if (event) {
            plat_assert(!event->event);
            if (event->heap.index) {
                heap_remove_pre_locked(dispatcher, event);
            }
            plat_free(event);
        }
        ret = NULL;
    }
    fthUnlock(unlock);

    return (ret);
}

/** @brief Finish freeing dispatcher when reference count hits zero */
static
void dispatcher_do_free(struct plat_timer_dispatcher *dispatcher) {
    plat_assert(dispatcher->free_count);
    plat_assert(!dispatcher->ref_count);
    plat_free(dispatcher->heap.data);
    plat_free(dispatcher);
}

/**
 * @brief plat_event free implementation
 *
 * Invoked on the first call to #plat_event_free
 */
static void
event_free_start(plat_closure_scheduler_t *context, void *env,
                 struct plat_event *base) {
    struct plat_timer_event *timer_event = (struct plat_timer_event *)env;
    fthWaitEl_t *unlock;
    struct plat_timer_dispatcher *dispatcher;
    int remove;

    plat_assert(timer_event->event == base);

    if (timer_event->heap.index != -1) {
        dispatcher = timer_event->dispatcher;

        unlock = fthLock(&dispatcher->lock, 1 /* write lock */, NULL);
        remove = timer_event->heap.index != -1;
        if (remove) {
            heap_remove_pre_locked(dispatcher, timer_event);
        }
        fthUnlock(unlock);

        if (remove) {
            plat_event_free(base, plat_event_free_done_null);
        }
    }
}

/**
 * @brief plat_event free done implementation
 *
 * Invoked when the asynchronous free process has completed
 */
static void
event_free_done(plat_closure_scheduler_t *context, void *env,
                struct plat_event *base) {
    int after;
    struct plat_timer_event *timer_event =
        (struct plat_timer_event *)env;
    struct plat_timer_dispatcher *dispatcher = timer_event->dispatcher;

    plat_assert(timer_event->heap.index == -1);
    plat_free(timer_event);

    after = __sync_sub_and_fetch(&dispatcher->ref_count, 1);
    plat_assert(after >= 0);

    if (!after) {
        dispatcher_do_free(dispatcher);
    }
}

/*
 * XXX if we have need of a priority queue elsewhere this could become
 * generic with a comparison function, in the manner of sys/tree.h or
 * fth/lll.h.
 */

/**
 * @brief assert if heap is corrupt
 *
 * Leave this function defined for gdb users even if the code isn't
 * currently using it.
 */
static __attribute__((unused)) void
heap_check_pre_locked(struct plat_timer_dispatcher *dispatcher) {
    int i;
    int child;
    struct plat_timer_event *event;

    for (i = 0; i < dispatcher->heap.count; ++i) {
        event = dispatcher->heap.data[i];
        plat_assert(event);
        plat_assert(event->heap.index == i);
        if (!heap_is_leaf(dispatcher, i)) {
            child = heap_left(dispatcher, i);
            plat_assert(!timercmp(&dispatcher->heap.data[child]->when,
                                  &dispatcher->heap.data[i]->when,
                                  TIMERCMP_LESS));
            child = heap_right(dispatcher, i);
            if (child < dispatcher->heap.count) {
                plat_assert(!timercmp(&dispatcher->heap.data[child]->when,
                                      &dispatcher->heap.data[i]->when,
                                      TIMERCMP_LESS));
            }
        }
    }
}

/**
 * @brief Remove event from heap
 */
static void
heap_remove_pre_locked(struct plat_timer_dispatcher *dispatcher,
                       struct plat_timer_event *event) {
    int index;

    plat_assert(event->dispatcher == dispatcher);
    plat_assert(event->heap.index >= 0);
    plat_assert(event->heap.index < dispatcher->heap.count);

    index = event->heap.index;
    event->heap.index = -1;

    --dispatcher->heap.count;
    /*
     * Remove the entry by moving the last entry to where it was and
     * re-heaping from that point down.  Skip the process if this was
     * the last entry.
     */
    if (index != dispatcher->heap.count) {
        dispatcher->heap.data[index] =
            dispatcher->heap.data[dispatcher->heap.count];
        dispatcher->heap.data[index]->heap.index = index;

        /* Downward movement is more likely */
        if (heap_sift_down(dispatcher, index) == index) {
            heap_sift_up(dispatcher, index);
        }
    }

#ifdef HEAP_CHECK
    heap_check_pre_locked(dispatcher);
#endif
}

/**
 * @brief Insert event into the heap.  Lock should not be held
 *
 * @return heap index on success (0 for first) so wrapping code
 * can determine whether there is a new minimum.
 */
static int
heap_insert_pre_locked(struct plat_timer_dispatcher *dispatcher,
                       struct plat_timer_event *event) {
    int ret;
    int new_capacity;
    struct plat_timer_event **events;

    if (dispatcher->heap.count == dispatcher->heap.capacity) {
        new_capacity = dispatcher->heap.capacity * 2;
        events = plat_realloc(dispatcher->heap.data, new_capacity *
                              sizeof (*dispatcher->heap.data));
        if (events) {
            dispatcher->heap.capacity = new_capacity;
            dispatcher->heap.data = events;
        }
    }

    if (dispatcher->heap.count != dispatcher->heap.capacity) {
        dispatcher->heap.data[dispatcher->heap.count] = event;
        event->heap.index = dispatcher->heap.count;
        ++dispatcher->heap.count;
        ret = heap_sift_up(dispatcher, dispatcher->heap.count - 1);
    } else {
        ret = -plat_errno;
    }

#ifdef HEAP_CHECK
    heap_check_pre_locked(dispatcher);
#endif

    return (ret);
}

/**
 * @brief top element.
 */
static __inline__ struct plat_timer_event *
heap_top_pre_locked(struct plat_timer_dispatcher *dispatcher) {
    return (dispatcher->heap.count > 0 ? dispatcher->heap.data[0] : NULL);
}


/**
 * @brief Sift element down in heap until its position is correct
 *
 * For removal.  Precondition: dispatcher->lock is held.
 *
 * @return new index of element
 */
static int
heap_sift_down(struct plat_timer_dispatcher *dispatcher, int index) {
    int current;
    int next;

    for (current = index; !heap_is_leaf(dispatcher, current); current = next) {
        if (heap_right(dispatcher, current) >= dispatcher->heap.count ||
            timercmp(&dispatcher->heap.data[heap_left(dispatcher, current)]->
                     when,
                     &dispatcher->heap.data[heap_right(dispatcher, current)]->
                     when, TIMERCMP_LESS)) {
            next = heap_left(dispatcher, current);
        } else {
            next = heap_right(dispatcher, current);
        }

        if (timercmp(&dispatcher->heap.data[next]->when,
                     &dispatcher->heap.data[current]->when, TIMERCMP_LESS)) {
            heap_swap(dispatcher, next, current);
        } else {
            break;
        }
    }

    return (current);
}

/**
 * @brief Sift element up in heap until its position is correct
 *
 * For insert.  Precondition: dispatcher->lock is held.
 * @return new index of element
 */
static int
heap_sift_up(struct plat_timer_dispatcher *dispatcher, int index) {
    int current;
    int next;

    for (current = index, next = heap_parent(dispatcher, current);
         current != 0 &&
         timercmp(&dispatcher->heap.data[current]->when,
                  &dispatcher->heap.data[next]->when, TIMERCMP_LESS);
         current = next, next = heap_parent(dispatcher, current)) {
        heap_swap(dispatcher, current, next);
    }

    return (current);
}

/** @brief Swap the heap elements and index first and second */
static void
heap_swap(struct plat_timer_dispatcher *dispatcher, int first, int second) {
    PLAT_SWAP(dispatcher->heap.data[first], dispatcher->heap.data[second]);
    dispatcher->heap.data[first]->heap.index = first;
    dispatcher->heap.data[second]->heap.index = second;
}

/** @brief Return parent of index, assert on root (no parent) */
static __inline__ int
heap_parent(struct plat_timer_dispatcher *dispatcher, int index) {
    plat_assert(index >= 0);
    return ((index + 1) / 2 - 1);
}

/** @brief Return left child of index, assert on leaf (no child) */
static __inline__ int
heap_left(struct plat_timer_dispatcher *dispatcher, int index) {
    plat_assert(!heap_is_leaf(dispatcher, index));
    return ((index + 1) * 2 - 1);
}

/** @brief Return right child of index, assert on leaf (no child) */
static __inline__ int
heap_right(struct plat_timer_dispatcher *dispatcher, int index) {
    plat_assert(!heap_is_leaf(dispatcher, index));
    return ((index + 1) * 2 + 1 - 1);
}

/** @brief Return true if index is a leaf, assert on out of bounds */
static __inline__ int
heap_is_leaf(struct plat_timer_dispatcher *dispatcher, int index) {
    plat_assert(index >= 0);
    plat_assert(index < dispatcher->heap.count);
    return (index > heap_parent(dispatcher, dispatcher->heap.count - 1));
}
