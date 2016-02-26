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

#ifndef TIMER_DISPATCHER_H
#define TIMER_DISPATCHER_H 1

/*
 * File:   sdf/platform/timer_dispatcher.h
 * Author: drew
 *
 * Created on March 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: timer_dispatcher.h 13137 2010-04-23 03:58:08Z drew $
 */

/**
 * Timer dispatcher which multiplexes timer events onto one value which
 * can be polled or blocked on in select(2)/epoll(2)/etc.
 */

#include "platform/closure.h"
#include "platform/defs.h"
#include "platform/event.h"

PLAT_CLOSURE1(plat_timer_dispatcher_gettime, struct timeval *, tv);

struct plat_timer_dispatcher;

enum plat_timer_whence {
    /** @brief Relative to now */
    PLAT_TIMER_RELATIVE,
    /** @brief Relative to dispatcher gettime closure */
    PLAT_TIMER_ABSOLUTE
};

__BEGIN_DECLS

/**
 * @brief Allocate timer dispatcher
 *
 * @param gettime <IN> Synchronous closure returning virtual time.
 * @return New timer dispatcher.
 */
struct plat_timer_dispatcher *
plat_timer_dispatcher_alloc(plat_timer_dispatcher_gettime_t gettime);

/**
 * @brief Free timer dispatcher.
 *
 * No ordering is required between #plat_timer_dispatcher_free and
 * #plat_event_free.
 *
 * @param dispatcher <IN> dispatcher to free
 */
void
plat_timer_dispatcher_free(struct plat_timer_dispatcher *dispatcher);

/**
 * @brief Activate expired timer events
 *
 * @param dispatcher <IN> dispatcher to fire events in
 * @return Number of events fired
 */
int plat_timer_dispatcher_fire(struct plat_timer_dispatcher *dispatcher);

/**
 * @brief Get next timer event
 *
 * @param dispatcher <IN> dispatcher to poll on
 * @param next <OUT> when to sleep to
 * @param next_whence <IN> whether now should be an absolute or relative
 * time.
 * @return next when there is a next timer, NULL for none pending
 */
struct timeval *
plat_timer_dispatcher_get_next(struct plat_timer_dispatcher *dispatcher,
                               struct timeval *next,
                               enum plat_timer_whence next_whence);

/**
 * @brief gettime
 *
 * Users should indirect through the timer dispatcher for getting
 * current time so that a monotonic clock can be used and test
 * environments can run faster than real time.
 *
 * @param dispatcher <IN> dispatcher
 * @param now <OUT> current time
 */
void
plat_timer_dispatcher_gettime(struct plat_timer_dispatcher *dispatcher,
                              struct timeval *now);


/**
 * @brief Allocate a timer event
 *
 * @param dispatcher <IN> dispatcher
 * @param name <IN> Descriptive name used in log messages which remains 
 * owned by the caller but must not be freed until 
 * #plat_event_free completes asynchronously.
 *
 * XXX: drew 2010-04-22 Should eliminate that micro-optimization and just
 * make a copy of the name.
 *
 * @param fired <IN> closure applied on timer firing
 * @param free_count <IN> number of times user must call free (not including
 * internal dispatcher logic)
 * @param when <IN> timer with reference specieid in whence
 * @param whence <IN> PLAT_TIMER_RELATIVE or PLAT_TIMER_ABSOLUTE
 *
 * @param rank_ptr <OUT> where this was inserted.  When the dispatcher
 * is being wrapped by other code, a return of 0 indicates that a poll
 * loop blocking until the shortest timer comes due should be woken up
 * early.  May be null.
 *
 * @return plat_event.  The event should not be fired by the user.
 * @plat_event_free must be called exactly #free_count times.
 */
struct plat_event *
plat_timer_dispatcher_timer_alloc(struct plat_timer_dispatcher *dispatcher,
                                  const char *name, int logcat,
                                  plat_event_fired_t fired,
                                  int free_count,
                                  const struct timeval *when,
                                  enum plat_timer_whence whence,
                                  int *rank_ptr);
__END_DECLS

#endif /* ndef TIMER_DISPATCHER_H */
