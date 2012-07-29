/*
 * File:   sdf/platform/fd_dispatcher.c
 * Author: drew
 *
 * Created on April 14, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fd_dispatcher.c 10527 2009-12-12 01:55:08Z drew $
 */

/**
 * File descriptor dispatcher which provides an event/closure interface
 * for file descriptors (notably sockets), eliminates the need for each
 * fthread to poll on non-blocking sockets (which does not scale), and
 * ultimately wraps the system's efficient select(2) replacement.
 *
 * Individual blocking reads across TCP connections (as in memcached) do
 * not scale because each attempt returning EAGAIN costs about 1uS on a
 * 2 GHz x86_64.  Select is better but still falls apart from reasonable
 * numbers of clients at memcached loads due to the O(N) fd_set scan
 * per return which becomes O(N^2) when the cost of processing individual
 * returns is low.
 *
 * It may also be desireable to sleep during idle periods for power reasons -
 * I measure a 20W difference on two 2GHz cores between spinning and blocking.
 */

#include <limits.h>

#include "sys/queue.h"

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fd_dispatcher.h"
#include "platform/logging.h"
#include "platform/select.h"
#include "platform/stdlib.h"
#include "platform/time.h"

#include "fth/fth.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM, "fd_dispatcher");

/* Placate cstyle */
#undef TIMERCMP_LESS
#define TIMERCMP_LESS <

/*
 * XXX epoll_ctl() uses a list of events per file descriptor, while a
 * reasonable programming interface separates the two into receive and
 * send.
 *
 * An added level of indirection is needed to mate the two - a per file
 * descriptor object or two epoll fds (which can allegedly can be cascaded)
 * should do the trick.
 *
 * select(2) doesn't take the level of indirection and we don't yet
 * have a use case which doesn't scale so we punt for the simplest
 * implementation.
 */

#ifdef FD_DISPATCHER_EPOLL
#error "FD_DISPATCHER_EPOLL not implemented yet"
#endif

#ifdef FD_DISPATCHER_EPOLL
enum {
    /* Completely arbitrary */
    INITIAL_EPOLL_SIZE = 128
};
#endif

enum dispatcher_state {
    /** @brief Nothing going on */
    STATE_IDLE,

    /**
     * @brief #plat_fd_dispatcher_poll is running with 0 timeout.
     *
     * This should be transient unless something is seriously
     * horked up.
     */
    STATE_POLL_NONBLOCKING,

    /**
     * @brief #plat_fd_dispatcher_poll is running with a timeout
     *
     * XXX If we do this there should be a pipe that wakes select() up
     * to begin acting on the new file descriptor.
     */
    STATE_POLL_BLOCKING
};

/**
 * @brief Define event states
 *
 * INITIAL         Just constructed
 *
 * ACTIVE          Detection of the specified event will fire it
 *
 * INACTIVE        The event exists but is not being monitored
 *
 * STARTED_FREE    The user has called #plat_event_free but the dispatcher
 *                 code has not because it still has a reference to the
 *                 object other than in the state lists.
 *
 * DISPATCHER_FREE The event dispatcher has removed it from all lists
 *                 but the potentially asynchronous free has not yet
 *                 completed.
 *
 * TOTALLY_FREE    This is a dangling pointer
 */
#define EVENT_STATE_ITEMS()                                                    \
    item(INITIAL, "initial")                                                   \
    item(ACTIVE, "active")                                                     \
    item(INACTIVE, "inactive")                                                 \
    item(STARTED_FREE, "free")                                                 \
    item(DISPATCHER_FREE, "dispatcher_free")                                   \
    item(TOTALLY_FREE, "really_free")

enum event_state {
#define item(caps, text) STATE_ ## caps,
    EVENT_STATE_ITEMS()
#undef item
};

struct plat_fd_event;

LIST_HEAD(plat_fd_event_list, plat_fd_event);

/** @brief fd dispatcher */
struct plat_fd_dispatcher {
    /** @brief Lock */
    struct fthLock lock;

    /**
     * @brief Number of times free has been called
     *
     * Free may be called at most once, but this may happen when the dispatcher
     * code is running.
     */
    int free_count;

    /** @brief Current state */
    enum dispatcher_state state;

    /**
     * @brief  Number of references by #plat_fd_event and self.
     *
     * This avoids some lock ordering issues
     */
    int ref_count;

    /*
     * Each state has a list of events associated with it linked together
     * by their state_list field and protected by this->lock so that movement
     * between state queues is possible.
     *
     * So that closures can be applied without rentrant locking problems
     * a single parallell list can be maintained using their tmp_list fields
     * that is owned exclusively by the dispatcher.
     */
    struct plat_fd_event_list state_active_events;
    struct plat_fd_event_list state_inactive_events;
    struct plat_fd_event_list state_free_events;

#ifdef FD_DISPATCHER_EPOLL
    struct {
        int fd;
    } epoll;
#else
    struct {
        /** @brief maximum used fd, add 1 for select nfds parameter */
        int max_fd;
        fd_set readfds;
        fd_set writefds;
        fd_set exceptfds;
    } select;
#endif

};

struct plat_fd_event {
    /** @brief Event structure to call #plat_event_fire, etc. */
    struct plat_event *event;

    /** @brief File descriptor being processed */
    int fd;

    /** @brief Type of event */
    enum plat_fd_type fd_type;

    /**
     * @brief Current state.
     *
     * Since state transitions involve movement between the dispatcher lists,
     * dispatcher->lock must be held when changing this via #event_set_state
     */
    enum event_state state;

    /** @brief Parent. */
    struct plat_fd_dispatcher *dispatcher;

    /**
     * @brief Referenced by a dispatcher function
     *
     * This indicates the dispatcher code has a reference on the
     * event other than its entry in the appropriate state list.  There
     * may be at most one temporary reference.  The code which owns the
     * temporary reference may also use the tmp_list field for tracking
     * multiple events. Code which grabs temporary references must lock the
     * dispatcher prior to resetting this field.  If  the event transitioned
     * to the DISPATCHER_FREE state the referencing code must set the event to
     * the DISPATCHER_FREE state and call #plat_event_free while it held the
     * temporary reference.
     *
     * This allows synchronous event closure code to manipulate events
     * without needing recursive locks.
     */
    int tmp_ref_count;

    /**
     * @brief Intrusive list entry for dispatcher state list
     *
     * #state_list should only be manipulatd by #event_set_state.
     */
    LIST_ENTRY(plat_fd_event) state_list;

    /** @brief Exclusively for the code which incremented tmp_ref_count */
    LIST_ENTRY(plat_fd_event) tmp_list;
};

static void
dispatcher_queue_free_events_pre_locked(struct plat_fd_event_list *free_list,
                                        struct plat_fd_event_list *src);
static void dispatcher_do_free(struct plat_fd_dispatcher *dispatcher);

#ifndef FD_DISPATCHER_EPOLL
static void dispatcher_set_max_fd_pre_locked(struct plat_fd_dispatcher *dispatcher);
#endif
static void event_reset(plat_closure_scheduler_t *context, void *env,
                        struct plat_event *base);
static void event_free_start(plat_closure_scheduler_t *context, void *env,
                             struct plat_event *base);
static void event_free_done(plat_closure_scheduler_t *context, void *env,
                            struct plat_event *base);
static void event_set_state_pre_locked(struct plat_fd_event *event,
                                       enum event_state next);
#ifndef FD_DISPATCHER_EPOLL
static fd_set *event_set(struct plat_fd_event *event);
#endif
static const char *event_state_to_string(enum event_state state);

struct plat_fd_dispatcher *
plat_fd_dispatcher_alloc() {
    struct plat_fd_dispatcher *ret;

    ret = plat_calloc(1, sizeof (*ret));
    if (ret) {
        fthLockInit(&ret->lock);
        ret->state = STATE_IDLE;
        ret->free_count = 0;
        ret->ref_count = 1;

        LIST_INIT(&ret->state_active_events);
        LIST_INIT(&ret->state_inactive_events);
        LIST_INIT(&ret->state_free_events);

#ifdef FD_DISPATCHER_EPOLL
        ret->epoll.fd = plat_epoll_create(INITIAL_EPOLL_SIZE);
        if (ret->epoll.fd == -1) {
            plat_log_msg(20938, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "epoll(%d) failed: %d", INITIAL_EPOLL_SIZE,
                         plat_errno);
            plat_free(ret);
            ret = NULL;
        }
#else
        ret->select.max_fd = -1;
        FD_ZERO(&ret->select.readfds);
        FD_ZERO(&ret->select.writefds);
        FD_ZERO(&ret->select.exceptfds);
#endif
    }

    return (ret);
}

void
plat_fd_dispatcher_free(struct plat_fd_dispatcher *dispatcher) {
    fthWaitEl_t *unlock;
    struct plat_fd_event_list call_free_list;
    struct plat_fd_event *fd_event;
    int i;
    int after_ref;

    struct plat_fd_event_list *lists[] = {
        &dispatcher->state_active_events,
        &dispatcher->state_inactive_events,
        &dispatcher->state_free_events
    };

    LIST_INIT(&call_free_list);

    unlock = fthLock(&dispatcher->lock, 1 /* write lock */, NULL);

    plat_assert(!dispatcher->free_count);
    ++dispatcher->free_count;

    for (i = 0; i < sizeof (lists) / sizeof (*lists); ++i) {
        dispatcher_queue_free_events_pre_locked(&call_free_list, lists[i]);
    }

    fthUnlock(unlock);

    while (!LIST_EMPTY(&call_free_list)) {
        fd_event = LIST_FIRST(&call_free_list);
        plat_assert(fd_event->state == STATE_DISPATCHER_FREE);
        LIST_REMOVE(fd_event, tmp_list);
        fd_event->tmp_ref_count = 0;
        plat_event_free(fd_event->event, plat_event_free_done_null);
    }

    after_ref = __sync_sub_and_fetch(&dispatcher->ref_count, 1);
    if (!after_ref) {
        dispatcher_do_free(dispatcher);
    }
}

struct plat_event *
plat_fd_dispatcher_fd_alloc(struct plat_fd_dispatcher *dispatcher,
                            const char *name, int logcat,
                            plat_event_fired_t fired, int free_count, int fd,
                            enum plat_fd_type fd_type) {
    int failed;
    struct plat_fd_event *event;
    struct plat_event *ret;
    fthWaitEl_t *unlock;

    event = plat_calloc(1, sizeof (*event));
    failed = !event;
    if (!failed) {
        event->fd = fd;
        event->fd_type = fd_type;
        event->state = STATE_INITIAL;
        event->dispatcher = dispatcher;
        event->tmp_ref_count = 0;

        plat_event_impl_closure_t impl_reset_closure =
            plat_event_impl_closure_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                           &event_reset, event);
        plat_event_impl_closure_t impl_free_closure =
            plat_event_impl_closure_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                           &event_free_start, event);
        plat_event_impl_closure_t impl_free_done_closure =
            plat_event_impl_closure_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                           &event_free_done, event);

        event->event = plat_event_base_alloc(name, logcat,
                                             PLAT_EVENT_POLICY_RESET, fired,
                                             free_count + 1, event,
                                             /* fire */
                                             plat_event_impl_closure_null,
                                             /* fired done */
                                             plat_event_impl_closure_null,
                                             impl_reset_closure,
                                             impl_free_closure,
                                             impl_free_done_closure);
        if (!event->event) {
            failed = 1;
        }
    }


    if (!failed) {
        unlock = fthLock(&dispatcher->lock, 1 /* write lock */, NULL);
        event_set_state_pre_locked(event, STATE_INACTIVE);
        fthUnlock(unlock);
        (void) __sync_fetch_and_add(&dispatcher->ref_count, 1);
        ret = event->event;
    } else {
        if (event) {
            plat_assert(!event->event);
            plat_free(event);
        }
        ret = NULL;
    }
   
    return (ret);
}

int
plat_fd_dispatcher_poll(struct plat_fd_dispatcher *dispatcher,
                        int timeout_msecs) {
    int ret;
    fthWaitEl_t *unlock;
    int status;
    struct timeval timeout;
    struct timeval *timeout_ptr;
    struct timeval now;
    struct timeval end;
    int nfds;
    fd_set readfds;
    fd_set writefds;
    fd_set exceptfds;
    int done;
    struct plat_fd_event_list fire_list;
    struct plat_fd_event *event;

    unlock = fthLock(&dispatcher->lock, 1 /* write lock */, NULL);
    plat_assert(dispatcher->state == STATE_IDLE);
    if (timeout_msecs < 0) {
        dispatcher->state = STATE_POLL_BLOCKING;
        timeout_ptr = NULL;
        end.tv_sec = INT_MAX;
        end.tv_usec = 0;
    } else if (timeout_msecs) {
        status = plat_gettimeofday(&now, NULL);
        plat_assert(!status);
        dispatcher->state = STATE_POLL_BLOCKING;
        timeout.tv_sec = timeout_msecs / 1000;
        timeout.tv_usec = (timeout_msecs % 1000) * 1000;
        timeout_ptr = &timeout;
        timeradd(&now, timeout_ptr, &end);
    } else {
        dispatcher->state = STATE_POLL_NONBLOCKING;
        timeout.tv_sec = 0;
        timeout.tv_usec = 0;
        timeout_ptr = &timeout;
        end = timeout;
    }

    if (dispatcher->select.max_fd == -1) {
        dispatcher_set_max_fd_pre_locked(dispatcher);
    }
    nfds = dispatcher->select.max_fd + 1;
    readfds = dispatcher->select.readfds;
    writefds = dispatcher->select.writefds;
    exceptfds = dispatcher->select.exceptfds;
    fthUnlock(unlock);

    done = 0;

    do {
        ret = plat_select(dispatcher->select.max_fd + 1, &readfds, &writefds,
                          &exceptfds, timeout_ptr);
        if (ret >= 0) {
            done = 1;
        } else if (ret == -1 && errno != EINTR) {
            ret = -plat_errno;
            done = 1;
        } else if (timeout_ptr) {
            status = plat_gettimeofday(&now, NULL);
            plat_assert(!status);
            if (timercmp(&now, &end, TIMERCMP_LESS)) {
                timersub(&end, &now, timeout_ptr);
            } else {
                done = 1;
            }
        }
    } while (!done);

    plat_log_msg(20939, LOG_CAT,  PLAT_LOG_LEVEL_TRACE,
                 "polled nfds %d timeout %d ret %d", nfds, timeout_msecs, ret);

    LIST_INIT(&fire_list);
    unlock = fthLock(&dispatcher->lock, 1 /* write lock */, NULL);
    if (ret > 0) {
        LIST_FOREACH(event, &dispatcher->state_active_events, state_list) {
            LIST_INSERT_HEAD(&fire_list, event, tmp_list);
            event->tmp_ref_count = 1;
            event_set_state_pre_locked(event, STATE_INACTIVE);
        }
    }
    dispatcher->state = STATE_IDLE;
    fthUnlock(unlock);

    while (!LIST_EMPTY(&fire_list)) {
        event = LIST_FIRST(&fire_list);
        LIST_REMOVE(event, tmp_list);
        unlock = fthLock(&dispatcher->lock, 1, NULL);

        event->tmp_ref_count = 0;
        plat_assert(event->state != STATE_DISPATCHER_FREE &&
                    event->state != STATE_TOTALLY_FREE);
        if (event->state == STATE_STARTED_FREE) {
            event_set_state_pre_locked(event, STATE_DISPATCHER_FREE);
            fthUnlock(unlock);
            plat_event_free(event->event, plat_event_free_done_null);
        } else {
            fthUnlock(unlock);
        }
    }

    return (ret);
}

/**
 * @brief Free events
 *
 * Events are set to the appropriate free state, specifically DISPATCHER_FREE
 * when not referenced elsewhere and FREE when referenced (with that code
 * setting the state to DISPATCHER_FREE when it removes them from its
 * dispatcher list)
 *
 * @param free_list <OUT> all events set to the DISPATCHER_FREE state linked
 * by their tmp_list field.
 *
 * @param src <IN> list of events which are being moved.
 */
static void
dispatcher_queue_free_events_pre_locked(struct plat_fd_event_list *free_list,
                                        struct plat_fd_event_list *src) {
    struct plat_fd_event *event;
    struct plat_fd_event *next;

    LIST_FOREACH_SAFE(event, src, state_list, next) {
        if (!event->tmp_ref_count) {
            event_set_state_pre_locked(event, STATE_DISPATCHER_FREE);
            LIST_INSERT_HEAD(free_list, event, tmp_list);
            event->tmp_ref_count = 1;
        } else if (event->state != STATE_STARTED_FREE) {
            event_set_state_pre_locked(event, STATE_STARTED_FREE);
        }
    }
}

/** @brief Actual free function called when !dispatcher->ref_count */
static void
dispatcher_do_free(struct plat_fd_dispatcher *dispatcher) {
    plat_assert(dispatcher->free_count);
    plat_assert(!dispatcher->ref_count);

    plat_assert(LIST_EMPTY(&dispatcher->state_active_events));
    plat_assert(LIST_EMPTY(&dispatcher->state_inactive_events));
    plat_assert(LIST_EMPTY(&dispatcher->state_free_events));

#ifdef FD_DISPATCHER_EPOLL
    if (dispatcher->fd != -1) {
        palt_close(dispatcher->fd);
    }
#endif

    plat_free(dispatcher);
}

#ifndef FD_DISPATCHER_EPOLL
/** @brief Set largest fd */
static void
dispatcher_set_max_fd_pre_locked(struct plat_fd_dispatcher *dispatcher) {
    struct plat_fd_event *event;

    dispatcher->select.max_fd = -1;
    LIST_FOREACH(event, &dispatcher->state_active_events, state_list) {
        if (event->fd != -1 && event->fd > dispatcher->select.max_fd) {
            dispatcher->select.max_fd = event->fd;
        }
    }
}
#endif


/**
 * @brief plat_event reset implementation
 *
 * Re-arm the closure
 */
static void
event_reset(plat_closure_scheduler_t *context, void *env,
            struct plat_event *base) {
    struct plat_fd_event *fd_event = (struct plat_fd_event *)env;
    struct plat_fd_dispatcher *dispatcher = fd_event->dispatcher;
    fthWaitEl_t *unlock;

    plat_assert(fd_event->state != STATE_TOTALLY_FREE);

    unlock = fthLock(&dispatcher->lock, 1 /* write lock */, NULL);
    if (fd_event->state != STATE_STARTED_FREE &&
        fd_event->state != STATE_DISPATCHER_FREE &&
        fd_event->state != STATE_ACTIVE) {
        event_set_state_pre_locked(fd_event, STATE_ACTIVE);
    }
    fthUnlock(unlock);
}

/**
 * @brief plat_event free implementation
 *
 * Invoked on the first call to #plat_event_free
 */
static void
event_free_start(plat_closure_scheduler_t *context, void *env,
                 struct plat_event *base) {
    struct plat_fd_event *fd_event = (struct plat_fd_event *)env;
    struct plat_fd_dispatcher *dispatcher = fd_event->dispatcher;
    fthWaitEl_t *unlock;

    plat_assert(fd_event->event == base);

    unlock = fthLock(&dispatcher->lock, 1 /* write lock */, NULL);
    plat_assert(fd_event->state != STATE_STARTED_FREE &&
                fd_event->state != STATE_DISPATCHER_FREE &&
                fd_event->state != STATE_TOTALLY_FREE);

    if (fd_event->tmp_ref_count) {
        event_set_state_pre_locked(fd_event, STATE_STARTED_FREE);
        fthUnlock(unlock);
    } else {
        event_set_state_pre_locked(fd_event, STATE_DISPATCHER_FREE);
        fthUnlock(unlock);
        plat_event_free(fd_event->event, plat_event_free_done_null);
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
    struct plat_fd_event *fd_event = (struct plat_fd_event *)env;
    struct plat_fd_dispatcher *dispatcher = fd_event->dispatcher;

    plat_assert(fd_event->state == STATE_DISPATCHER_FREE);
    plat_assert(!fd_event->tmp_ref_count);

    /* The lock isn't held, but we want common code for logging, etc. */
    event_set_state_pre_locked(fd_event, STATE_TOTALLY_FREE);

    plat_free(fd_event);

    after = __sync_sub_and_fetch(&dispatcher->ref_count, 1);
    plat_assert(after >= 0);

    if (!after) {
        dispatcher_do_free(dispatcher);
    }
}

/**
 * @brief Set state for event
 *
 * The event is moved between the per-state lists as appropriate.
 * #event->dispatcher->lock must be held except when transitioning
 * between STATE_DISPATCHER_FREE and STATE_TOTALLY_FREE
 */
static void
event_set_state_pre_locked(struct plat_fd_event *event, enum event_state next) {
    struct plat_fd_dispatcher *dispatcher = event->dispatcher;
    struct plat_fd_event_list *to_list = NULL;

#ifndef FD_DISPATCHER_EPOLL
    fd_set *set =  NULL;
#endif

    plat_log_msg(20940, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "plat_fd_event %p state %s->%s", event,
                 event_state_to_string(event->state),
                 event_state_to_string(next));

    // Leave old state
    switch (event->state) {
    case STATE_INITIAL:
        break;
    case STATE_ACTIVE:
#ifndef FD_DISPATCHER_EPOLL
        set = event_set(event);
        /* Lazily readjust dispatcher->select.max_fd */
        FD_CLR(event->fd, set);
#endif
        LIST_REMOVE(event, state_list);
        break;
    case STATE_INACTIVE:
        LIST_REMOVE(event, state_list);
        break;
    case STATE_STARTED_FREE:
        LIST_REMOVE(event, state_list);
        plat_assert(STATE_DISPATCHER_FREE == next);
        break;
    case STATE_DISPATCHER_FREE:
        /* There's no going back */
        plat_assert(next == STATE_TOTALLY_FREE);
        break;
    case STATE_TOTALLY_FREE:
        /* The roach motel state.  We shouldn't be here */
        plat_assert(0);
        break;
    }

    switch (next) {
    case STATE_INITIAL:
        plat_assert(0);
        break;
    case STATE_ACTIVE:
#ifndef FD_DISPATCHER_EPOLL
        if (!set) {
            set = event_set(event);
        }
        FD_SET(event->fd, set);
        if (event->fd > dispatcher->select.max_fd) {
            dispatcher->select.max_fd = event->fd;
        }
#endif
        to_list = &dispatcher->state_active_events;
        break;
    case STATE_INACTIVE:
        to_list = &dispatcher->state_inactive_events;
        break;
    /* We can transition directly to either of these states */
    case STATE_STARTED_FREE:
    case STATE_DISPATCHER_FREE:
#ifndef FD_DISPATCHER_EPOLL
        if (event->fd == dispatcher->select.max_fd) {
            dispatcher->select.max_fd = -1;
        }
#endif
        break;
    case STATE_TOTALLY_FREE:
        break;
    }

    event->state = next;
    if (to_list) {
        LIST_INSERT_HEAD(to_list, event, state_list);
    }
}

#ifndef FD_DISPATCHER_EPOLL
/** @brief Return fd_set corresponding to event */
static fd_set *
event_set(struct plat_fd_event *event) {
    switch (event->fd_type) {
    case PLAT_FD_READ:
        return (&event->dispatcher->select.readfds);
    case PLAT_FD_WRITE:
        return (&event->dispatcher->select.writefds);
    case PLAT_FD_EXCEPT:
        return (&event->dispatcher->select.exceptfds);
    }

    plat_assert(0);
    return (NULL);
}
#endif

/** @brief convert event state to human readable form */
static const char *
event_state_to_string(enum event_state state) {
    switch (state) {
#define item(caps, text)                                                       \
    case STATE_ ## caps: return (text);
    EVENT_STATE_ITEMS()
#undef item
    }

    plat_assert(0);
    return (NULL);
}
