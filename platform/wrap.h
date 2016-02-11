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

#ifndef PLATFORM_WRAP_H
#define PLATFORM_WRAP_H 1

/*
 * File:   sdf/platform/wrap.h
 * Author: drew
 *
 * Created on February 27, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: wrap.h 15015 2010-11-10 23:09:06Z briano $
 */

/**
 * Library wrapping and identifier poisoning
 *
 * Direct calls to library functions make simulation more difficult so they
 * should be avoided. 
 *
 * 1) Make it impossible for developers (who have included
 *    the apporpriate "platform/foo.h which will ultimately be transparently
 *    picked up by -nostdinc) to accidentally call system calls by
 *    "poisoning" standard identifiers.
 *
 * 2) Allow automatic wrapper generation.
 *
 * #pragma GCC poison is not used because that acts at the token level;
 * so poisoning stat causes errors referencing stat(), using struct stat,
 * and struct field names like
 *     struct file_ops {
 *         int (*stat)(struct file_ops *lhs, struct stat *buf);
 *     };
 *
 * Example in "platform/unistd.h":
 *
 * #define PLAT_UNISTD_WRAP_ITEMS() \
 *     item(int, close, (int fd), (fd)) \
 *     item(ssize_t, (int fd, void *buf, size_t count), (fd, buf, count))
 *
 * #define item(ret, sym, declare, call) \
 *     PLAT_WRAP(ret, sym, declare, call)
 * PLAT_UNISTD_WRAP_ITEMS()
 * #undef item
 *
 * in real platform/unistd.c
 * #define item(ret, sym, declare, call) \
 *     PLAT_WRAP_IMPL(ret, sym, declare, call)
 * PLAT_UNISTD_WRAP_ITEMS()
 * #undef item
 */

#include "platform/defs.h"

#define PLAT_WRAP_PRAGMA(text) _Pragma(#text)

#if (__GNUC__ > 4) || (__GNUC__ == 4 && __GNUC_MINOR__ >= 6)
#define PLATFORM_BYPASS
#endif

#ifdef PLATFORM_BYPASS

#define PLAT_WRAP_CPP_POISON(ident)

#define PLAT_WRAP(ret, sym, declare, call, cppthrow, attributes)               \
    PLAT_WRAP_NO_POISON(ret, sym, declare, call, cppthrow, attributes)

#define PLAT_WRAP_NO_PLAT(ret, sym, declare, call, cppthrow, attributes)       \
    PLAT_WRAP_SYS(ret, sym, declare, call, cppthrow, attributes)

#else /* def PLATFORM_BYPASS */

#define PLAT_WRAP_CPP_POISON(ident) PLAT_WRAP_PRAGMA(GCC poison ident)

/*
 * GNU's extern inline implementation is essentially a type safe CPP macro;
 * which is to say no code will be emitted unless it's actually referenced.
 * Declaring a function extern inline function after declaring it with the
 * same type signature is also legal.
 *
 * The net effect is that after a PLAT_WRAP call, references to sym cause
 * the function to be compiled.  Since the static variable poisoned is
 * referenced, using a wrapped function directly puts a dropping in the
 * object code which strings will pickup in a post-procesing step.
 *
 * In the unlikely event that this goes unnoticed, a call to the non-existant
 * plat_poisoned will also cause a linker error.
 *
 * FIXME: We could just use the call to plat_ ## sym ## _poisoned and pick it up
 * via nm instead of strings.
 *
 * For internal platform code, PLAT_SYS becomes an inline wrapper which takes
 * the address of the library function version and calls it so the function is
 * still accessable where it should be.
 */
#define PLAT_WRAP(ret, sym, declare, call, cppthrow, attributes)               \
    PLAT_WRAP_DECLARE(ret, sym, declare, call, cppthrow, attributes)           \
    PLAT_WRAP_POISON(ret, sym, declare, call, cppthrow, attributes)            \
    PLAT_WRAP_SYS(ret, sym, declare, call, cppthrow, attributes)

#define PLAT_WRAP_NO_PLAT(ret, sym, declare, call, cppthrow, attributes)       \
    PLAT_WRAP_POISON(ret, sym, declare, call, cppthrow, attributes)            \
    PLAT_WRAP_SYS(ret, sym, declare, call, cppthrow, attributes)
#endif /* else  def PLATFORM_BYPASS */

#define PLAT_WRAP_NO_POISON(ret, sym, declare, call, cppthrow, attributes)     \
    PLAT_WRAP_DECLARE(ret, sym, declare, call, cppthrow, attributes)           \
    PLAT_WRAP_SYS(ret, sym, declare, call, cppthrow, attributes)

// _Pragma(GCC poison #sym) works where this fails

/*
 * GCC 4.3.0 always emits the poisoned strings even if the inline function
 * isn't being referenced. Avoid this.
 */
#if (__GNUC__ == 4 && (__GNUC_MINOR__ == 1 || __GNUC_MINOR__ >= 3))
#define PLAT_WRAP_POISON(ret, sym, declare, call, cppthrow, attributes)
#else
#define PLAT_WRAP_POISON(ret, sym, declare, call, cppthrow, attributes)        \
    PLAT_EXTERN_INLINE ret sym declare cppthrow                                \
        attributes __attribute__((always_inline));                             \
    PLAT_EXTERN_INLINE ret sym declare PLAT_WRAP_THROW_IMPL(cppthrow) {        \
        static const char poisoned[] = "POISONED(" #sym ")";                   \
        extern ret plat_ ## sym ## _poisoned(const char *);                    \
        return (plat_ ## sym ## _poisoned(poisoned));                          \
    }
#endif

#define PLAT_WRAP_DECLARE(ret, sym, declare, call, cppthrow, attributes)       \
    extern ret plat_ ## sym declare attributes;

#ifdef PLATFORM_INTERNAL
#define PLAT_WRAP_SYS(ret, sym, declare, call, cppthrow, attributes)           \
    static  ret sys_ ## sym declare cppthrow attributes;             \
    static  ret sys_ ## sym declare PLAT_WRAP_THROW_IMPL(cppthrow) { \
        ret (*fn)declare = &sym;                                               \
        return ((*fn)call);                                                    \
    }
#else /* def PLATFORM_INTERNAL */
#define PLAT_WRAP_SYS(ret, sym, declare, call, cppthrow, attributes)
#endif /* else def PLATFORM_INTERNAL */

#define PLAT_WRAP_IMPL(ret, sym, declare, call, cppthrow, attributes)          \
    ret                                                                        \
    plat_ ## sym declare PLAT_WRAP_THROW_IMPL(cppthrow) {                      \
        return (sys_ ## sym call);                                             \
    }

/*
 * GCC defines __THROW as __attribute__((nothrow)) when compiling as 'C', 
 * and throw() when compiling as C++.  The former may not appear between
 * the function implementation argument closing paren and open brace while
 * the later must appear.
 *
 * Introduce a macro to fix this.
 */
#ifdef __cplusplus
#define PLAT_WRAP_THROW_IMPL(cppthrow) cppthrow 
#else
#define PLAT_WRAP_THROW_IMPL(cppthrow)
#endif

#endif /* ndef PLATFORM_WRAP_H */
