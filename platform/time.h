/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_TIME_H
#define PLATFORM_TIME_H 1
/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/time.h $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: time.h 12356 2010-03-19 03:36:12Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

/*
 * Hide functions which have been (or should be) replaced, producing
 * compile time warnings which become errors with -Werror.
 */
#ifndef PLATFORM_TIME_C
#define nanosleep nanosleep_use_plat
#define gettimeofday gettimeofday_use_plat
#define settimeofday settimeofday_use_plat
#endif

/* For timercmp, timeradd, timersub, etc. */
#ifndef _BSD_SOURCE
#define UNBSD
#define _BSD_SOURCE
#endif
#include <sys/time.h>
#include <time.h>
#ifdef UNBSD
#undef _BSD_SOURCE
#endif

#ifndef PLATFORM_TIME_C
#undef nanosleep
#undef gettimeofday
#undef settimeofday
#endif

__BEGIN_DECLS

/**
 * plat_nanosleep() is preferred to sleep() since that may use alarm(),
 * usleep() which may not support over 1M seconds, or select() which
 * requires one to watch the system clock since the timeout does
 * not update on most platforms.
 *
 * use this instead of adding another wrapper for one of those functions.
 */
int plat_nanosleep(const struct timespec *req, struct timespec *rem);

int plat_gettimeofday(struct timeval *tv, struct timezone *tz);

/**
 * @brief Compare timespecs
 *
 * @return -1 if lhs is less, 0 if lhs == rhs, 1 if lhs is larger
 */
int plat_timespec_cmp(const struct timespec *lhs, const struct timespec *rhs);

/**
 * @brief Add lhs to rhs storing in result
 *
 * @param lhs <IN> timespec with tv_sec and tv_nsec both non-negative
 * @param rhs <IN> timespec with tv_sec and tv_nsec both non-negative
 * @param result <OUT> may point to lhs or rhs
 */
void plat_timespec_add(const struct timespec *lhs, const struct timespec *rhs,
                       struct timespec *result);

/**
 * @brief Subtract lhs from rhs storing in result
 *
 * @param lhs <IN> timespec with tv_sec and tv_nsec both non-negative and
 * tv_nsec < one billion
 * @param rhs <IN> timespec with tv_sec and tv_nsec both non-negative and
 * tv_nsec < one billion
 * @param result <OUT> may point to lhs or rhs
 */
void plat_timespec_sub(const struct timespec *lhs, const struct timespec *rhs,
                       struct timespec *result);

__END_DECLS

#endif /* ndef PLATFORM_TIME_H */
