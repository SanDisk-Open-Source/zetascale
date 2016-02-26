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

#ifndef PLATFORM_ASSERT_H
#define PLATFORM_ASSERT_H 1

/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/assert.h $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: assert.h 12402 2010-03-20 01:44:06Z drew $
 */

/**
 * Platform assert which tries to output to the logging subsystem before
 * giving up the ghost.  Double faults are correctly handled (where the
 * logging subsystem plat_asserts).
 */

#if defined(PLATFORM_INTERNAL)
#include <assert.h>
#define sys_assert assert
#elif !defined(PLATFORM_BYPASS)
#undef assert
#pragma GCC poison assert
#endif

#include <sys/cdefs.h>

__BEGIN_DECLS

/**
 * Always assert even when NDEBUG is defined.
 *
 * @param <IN>
 *
 * if (expr) with else clause instead of if (!(expr)) allows gcc to detect
 * assignments like assert(x = 10)
 */
#define plat_assert_always(expr)                                               \
    do {                                                                       \
        if (expr) {                                                            \
        } else {                                                               \
            plat_assert_fail(#expr, __FILE__, __LINE__, __PRETTY_FUNCTION__);  \
        }                                                                      \
    } while (0)

#define plat_fatal(expr) plat_assert_fail(expr,  __FILE__, __LINE__, __PRETTY_FUNCTION__)

#ifdef NDEBUG

#define plat_assert(expr) ((void)0)
#define plat_assert_imply(a, b) ((void)0)
#define plat_assert_iff(a, b) ((void)0)
#define plat_assert_rc(expr) ((void) (expr))

#else /* def NDEBUG */

#define plat_assert(expr) plat_assert_always(expr)

/**
 * Assert when a is true but b isn't.
 *
 * @param <IN> a first expression usable as a boolean in if(a), etc.
 * @param <IN> b second expression usable as a bool in if(b), etc.
 *
 * Use of nested if(a) if(b) instead of if (!!(a) && !(b)) allows
 * gcc to detect assignments like plat_assert_imply(x = 10, y = 1)
 */
#define plat_assert_imply(a, b)                                                \
    do {                                                                       \
        if (a) {                                                               \
            if (b) {                                                           \
            } else {                                                           \
                plat_assert_fail(#a " implies " #b,  __FILE__, __LINE__,       \
                                 __PRETTY_FUNCTION__);                         \
            }                                                                  \
        }                                                                      \
    } while (0)

/**
 * Assert that a and b must both be true or not true
 *
 * @param <IN> a first expression usable as a boolean in if(a), etc.
 * @param <IN> b second expression usable as a bool in if(b), etc.
 *
 * Use of if statements instead of a_bool = !!(a) and b_bool = !!(b)
 * allows gcc to detect assignments like plat_assert_iff(x = 10, y = 1)
 */
#define plat_assert_iff(a, b)                                                  \
    do {                                                                       \
        int a_bool;                                                            \
        int b_bool;                                                            \
                                                                               \
        if (a) {                                                               \
            a_bool = 1;                                                        \
        } else {                                                               \
            a_bool = 0;                                                        \
        }                                                                      \
                                                                               \
        if (b) {                                                               \
            b_bool = 1;                                                        \
        } else {                                                               \
            b_bool = 0;                                                        \
        }                                                                      \
                                                                               \
        if (a_bool != b_bool) {                                                \
            plat_assert_fail(#a " iff " #b,  __FILE__, __LINE__,               \
                             __PRETTY_FUNCTION__);                             \
        }                                                                      \
    } while (0)

/**
 * Assert that exactly one argument is set
 *
 * @param <IN> a first expression usable as a boolean in if(a), etc.
 * @param <IN> b second expression usable as a bool in if(b), etc.
 */
#define plat_assert_either(args...) \
    do {                                                                       \
        int args_as_bool[] = { args };                                         \
        int i;                                                                 \
        int count;                                                             \
        for (count = i = 0; i < sizeof (args_as_bool)/sizeof (args_as_bool[0]);\
             ++i) {                                                            \
            if (args_as_bool[i]) {                                             \
                ++count;                                                       \
            }                                                                  \
        }                                                                      \
        if (count != 1) {                                                      \
            plat_assert_fail("either " #args,  __FILE__, __LINE__,             \
                             __PRETTY_FUNCTION__);                             \
        }                                                                      \
    } while (0)

#define plat_assert_rc(expr) plat_assert((expr) == 0)

#endif /* else NDEBUG */

void plat_assert_fail(const char *expr, const char *file, unsigned line,
                      const char *fn) __attribute__((noreturn));

__END_DECLS

#endif /* ndef PLATFORM_ASSERT_H */
