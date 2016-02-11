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

#ifndef PLATFORM_DEFS_H
#define PLATFORM_DEFS_H 1

/*
 * File:   sdf/platform/defs.h
 * Author: drew
 *
 * Created on March 12, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: defs.h 12394 2010-03-19 21:37:46Z drew $
 */

/**
 * Miscellaneous useful macro definitions.
 */

#include <sys/cdefs.h>
#include "platform/assert.h"

/**
 * @brief Wrap C++ code so that exceptions do reach 'C' code
 *
 * @code
 *
 * class cc_obj {
 * public:
 *   int fn(int arg);
 *   void void_fn();
 * };
 *
 * extern "C" {
 *
 * int
 * c_fn(struct cc_obj *obj, int arg) {
 *      PLAT_RETURN_NO_EXCEPTIONS(obj->fn(obj, arg), -ENOMEM);
 * }
 *
 * void
 * c_void_fn(struct cc_obj *obj) {
 *      PLAT_RETURN_NO_EXCEPTIONS(obj->void_fn(obj, arg), );
 * }
 * @endcode
 *
 * @param what <IN> C++ code to run in normal case, most likely a method call
 * @param enomem <IN> C++ code to run in enomem case
 */
#define PLAT_RETURN_NO_EXCEPTIONS(what, enomem)                                \
    try {                                                                      \
        return (what);                                                         \
    } catch (std::bad_alloc) {                                                 \
        /*CSTYLED*/                                                            \
        return enomem;                                                         \
    } catch (...) {                                                            \
        plat_assert(0);                                                        \
    }

#define PLAT_FLATTEN __attribute__((flatten))

/**
 * @brief Provide extern inline correct for newer and older gcc
 */
#ifdef __GNUC_GNU_INLINE__
#define PLAT_EXTERN_INLINE extern  __attribute__((__gnu_inline__))   \
    PLAT_FLATTEN
#else
#define PLAT_EXTERN_INLINE extern  PLAT_FLATTEN
#endif

/*
 * Hooks for creating debugger friendly (out-of-line functions so break points
 * can be set, don't hint to compiler about what function calls can be
 * eliminated) and performance friendly versions of the code.
 *
 * GCC provides an extension
 * extern inline __attribute__((gnu_inline))
 * which emits an inline function definition when optimization is turned
 * on, and acts as a simple out-of-line extern declaration otherwise.
 *
 * This allows for source which is both easily debugged and performant.
 *
 * Problem 1:
 *
 * GCC sometimes limits the depth of what could be in-lined with
 * extern inline versus staic inline.  Switching to static inline
 * avoids this problem.
 *
 * Problem 2:
 *
 * GCC 4.3 complains when an extern inline function references a static
 * inline function which is bogus.
 *
 * Example:
 * error: ‘plat_shmem_get_attached’ is static but used in inline function
 * ‘plat_shmem_ptr_base_to_paddr’ which is not static
 *
 * The work-around is that all in-line functions must have the same form
 * which needs to be static inline.  Since this effects everything it has
 * to be done globally and not at a sub-system level
 */

#ifdef PLAT_DEBUG_NO_INLINE
#define PLAT_OUT_OF_LINE(x) x
#define PLAT_NEED_OUT_OF_LINE
#define PLAT_INLINE PLAT_EXTERN_INLINE
#else
#define PLAT_OUT_OF_LINE(x)
#undef PLAT_NEED_OUT_OF_LINE
#define PLAT_INLINE static  __attribute__((unused))
#endif

/* May be missing from old cdefs */
#ifndef __nonnull
#if __GNUC_PREREQ(3, 3)
#define __nonnull(params) __attribute__((__nonnull__ params))
#else
#define __nonnull(params)
#endif
#endif

/* BEGIN CSTYLED */
/* nested ifdefs are easier to read with spaces after # */
#ifndef __wur
/*
 * If fortification mode, we warn about unused results of certain
 * function calls which can lead to problems.
 */
#if __GNUC_PREREQ(3, 4)
# define __attribute_warn_unused_result__ \
   __attribute__((__warn_unused_result__))
# if __USE_FORTIFY_LEVEL > 0
#  define __wur __attribute_warn_unused_result__
# endif
#else
# define __attribute_warn_unused_result__ /* empty */
#endif
#ifndef __wur
# define __wur /* Ignore */
#endif
#endif
/* END CSTYLED */

/** @brief min macro without side effects */
#define PLAT_MIN(a, b)                                                         \
    ({                                                                         \
     typeof (a) a_tmp = (a);                                                   \
     typeof (b) b_tmp = (b);                                                   \
     (a_tmp <= b_tmp) ? a_tmp : b_tmp;                                         \
     })

/** @brief max macro without side effects */
#define PLAT_MAX(a, b)                                                         \
    ({                                                                         \
     typeof (a) a_tmp = (a);                                                   \
     typeof (b) b_tmp = (b);                                                   \
     (a_tmp >= b_tmp) ? a_tmp : b_tmp;                                         \
     })

/** @brief start platform namespace.  */
#define PLAT_NAMESPACE_BEGIN namespace platform {

/** @brief End platform namespace.  */
#define PLAT_NAMESPACE_END }

#define PLAT_OFFSET_OF(x, field) __builtin_offsetof(x, field)

/** @brief Swap a, b */
#define PLAT_SWAP(a, b)                                                        \
    do {                                                                       \
        typeof (a) tmp;                                                        \
        tmp = (a);                                                             \
        (a) = (b);                                                             \
        (b) = tmp;                                                             \
    } while (0);

/** @brief if (PLAT_LIKELY(x)) means if branch is most likely */
#define PLAT_LIKELY(val) __builtin_expect((val), 1)

/** @brief if (PLAT_UNLIKELY(x)) means else branch is most likely */
#define PLAT_UNLIKELY(val) __builtin_expect((val), 0)

/** @brief Return logical xor of a and b evaluating each once */
#define PLAT_XOR(a, b) \
    ({                                                                         \
     int a_bool = !!(a);                                                       \
     int b_bool = !!(b);                                                       \
     !!(a_bool ^ b_bool);                                                      \
     })

#define ONEXIT_CAT(x, y) x ## y
#define ONEXIT_I(i, x) \
    inline void ONEXIT_CAT(onexit_cleanup_func_, i) (int *ONEXIT_CAT(onexit_cleanup_dummy_arg_, i)) { x; } \
    int ONEXIT_CAT(onexit_cleanup_dummy_var_, i) __attribute__((cleanup(ONEXIT_CAT(onexit_cleanup_func_, i))))
/** @brief Specify function to call on exit */
#define ONEXIT(x) ONEXIT_I(__LINE__, x)

/**
 * @brief "de-project" a sub-object address to the entire structure
 *
 * @param type <IN> structure name
 * @param field <IN> sub-field name
 * @param field_ptr <IN> pointer to sub-field
 *
 * @code
 * struct io_extent {
 *     struct io_state *parent;
 *
 *     // For aio
 *     struct iocb iocb
 * };
 *
 *
 * struct iocb *completed;
 *
 * // completed initialization by io_getevents call is elided
 *
 * struct io_extent *extent = PLAT_DEPROJECT(io_extent, iocb, completed);
 * @endcode
 */
#define PLAT_DEPROJECT(type, field, field_ptr) \
    ({                                                                         \
     typeof (((type *)0)->field) *_tmp = (field_ptr); /* type check */         \
     (type *)((char *)(_tmp) - PLAT_OFFSET_OF(type, field));                   \
     })

/*
 * Provide symbolic names so developers don't write THOUSAND instead of MILLION
 * thus producing non monotonic time accross threads.
 *
 * (The specific bug was one instance of 100000 instead of 1000000 with five
 * not six zeroes)
 */
enum {
    PLAT_THOUSAND = 1000,
    PLAT_MILLION = PLAT_THOUSAND * PLAT_THOUSAND,
    PLAT_BILLION = PLAT_THOUSAND * PLAT_MILLION
};

#endif /* ndef PLATFORM_DEFS_H */
