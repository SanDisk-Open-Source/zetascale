/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/time.c $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: time.c 12356 2010-03-19 03:36:12Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 *
 * Simulated interface implementation
 *
 * 1. All time related sleep functions (plat_select(), plat_nanosleep(),
 *    plat_cond_timedwait() should use some sort of priority queue for the
 *    node.  The nodes should get arranged into a common priority queue.
 *
 * 2. Things should sleep for longer (configurable) to simulate badly
 *    behaved other processes, interrupt handlers, etc.
 *
 * 3. The time between nodes should drift within some configurable parameters.
 */

#define PLATFORM_TIME_C 1

#include "platform/assert.h"
#include "platform/defs.h"
#include "platform/time.h"

int
plat_gettimeofday(struct timeval *tv, struct timezone *tz) {
    return (gettimeofday(tv, tz));
}

int
plat_nanosleep(const struct timespec *req, struct timespec *rem) {
    return (nanosleep(req, rem));
}

int
plat_timespec_cmp(const struct timespec *lhs, const struct timespec *rhs) {
    int ret;

    if (lhs->tv_sec > rhs->tv_sec) {
        ret = 1;
    } else if (lhs->tv_sec < rhs->tv_sec) {
        ret = -1;
    } else if (lhs->tv_nsec > rhs->tv_nsec) {
        ret = 1;
    } else if (lhs->tv_nsec < rhs->tv_nsec) {
        ret = -1;
    } else {
        plat_assert(lhs->tv_nsec == rhs->tv_nsec);
        ret = 0;
    }

    return (ret);
}

void
plat_timespec_add(const struct timespec *lhs, const struct timespec *rhs,
                  struct timespec *result) {
    plat_assert(lhs->tv_sec >= 0);
    plat_assert(lhs->tv_nsec >= 0);
    plat_assert(rhs->tv_sec >= 0);
    plat_assert(rhs->tv_nsec >= 0);

    result->tv_sec = lhs->tv_sec + rhs->tv_sec +
        (lhs->tv_nsec + rhs->tv_nsec) / PLAT_BILLION;
    result->tv_nsec = (lhs->tv_nsec + rhs->tv_nsec) % PLAT_BILLION;
}

void
plat_timespec_sub(const struct timespec *lhs, const struct timespec *rhs,
                  struct timespec *result) {
    plat_assert(lhs->tv_sec >= 0);
    plat_assert(lhs->tv_nsec >= 0);
    plat_assert(lhs->tv_nsec < PLAT_BILLION);
    plat_assert(rhs->tv_sec >= 0);
    plat_assert(rhs->tv_nsec >= 0);
    plat_assert(rhs->tv_nsec < PLAT_BILLION);

    result->tv_sec = lhs->tv_sec - rhs->tv_sec;
    result->tv_nsec = lhs->tv_nsec - rhs->tv_nsec;

    if (result->tv_nsec < 0) {
        ++result->tv_sec;
        result->tv_nsec += PLAT_BILLION;
    }
}
