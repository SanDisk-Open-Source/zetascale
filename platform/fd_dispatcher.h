#ifndef FD_DISPATCHER_H
#define FD_DISPATCHER_H 1

/*
 * File:   sdf/platform/fd_dispatcher.h
 * Author: drew
 *
 * Created on March 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fd_dispatcher.h 992 2008-04-18 17:11:06Z drew $
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
 * It may also be desireable to sleep during idle periods for power reasons.
 */

#include "platform/closure.h"
#include "platform/defs.h"
#include "platform/event.h"

enum plat_fd_type {
    PLAT_FD_READ,
    PLAT_FD_WRITE,
    PLAT_FD_EXCEPT
};

__BEGIN_DECLS


/**
 * @brief Allocate fd dispatcher.
 *
 * @retrn New fd dispatcher
 */
struct plat_fd_dispatcher *plat_fd_dispatcher_alloc();

/**
 * @brief Free fd dispatcher.
 *
 * No ordering is required between #plat_fd_dispatcher_free and
 * #plat_event_free.
 *
 * @param dispatcher <IN> dispatcher to free
 */
void plat_fd_dispatcher_free(struct plat_fd_dispatcher *dispatcher);

/**
 * @brief Allocate fd event
 *
 * @param dispatcher <IN> dispatcher
 * @param fired <IN> closure applied on firing
 * @param free_count <IN> number of times user must call free (not including
 * internal dispatcher logic)
 * @param when <IN> timer with reference specieid in whence
 * @param whence <IN> PLAT_TIMER_RELATIVE or PLAT_TIMER_ABSOLUTE
 *
 * @return plat_event.  The event should not be fired by the user.
 * @plat_event_free must be called exactly #free_count times.
 */
struct plat_event *
plat_fd_dispatcher_fd_alloc(struct plat_fd_dispatcher *dispatcher,
                            const char *name, int logcat,
                            plat_event_fired_t fired, int free_count, int fd,
                            enum plat_fd_type fd_type);

/**
 * @brief Poll file descriptors
 *
 * @param dispatcher <IN> the dispatcher
 *
 * @param timeout_msecs <IN> minimum time to block when no events are
 * active.  Note that non-zero values are probably inappropriate for
 * a generic fthread implementation..
 *
 * @return -errno on failure, nummber of events fired on success
 */
int
plat_fd_dispatcher_poll(struct plat_fd_dispatcher *dispatcher,
                        int timeout_msecs);

__END_DECLS


#endif /* ndef FD_DISPATCHER_H */
