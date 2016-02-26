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
 * File:   sdf/platform/attr.c
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: attr.c 2311 2008-07-20 01:05:59Z drew $
 */

/**
 * Define attributes at process, kernel scheduled thread, and user scheduled
 * thread scopes with transparent scope for user.
 *
 * These are used to provide operational state for things which can't
 * take extra parameters (shared memory pointers) or where values must
 * be scope local (action-consistent recovery structures unique
 * to each kernel thread).
 *
 * pthread_getspecific()/setspecific() are the analog.
 */

#include <stddef.h>

#include "platform/assert.h"
#include "platform/attr.h"
#include "platform/stdlib.h"

static struct plat_attr_proc_specific {
#define item(description, level, name, type, attrs) type * name;
PLAT_ATTR_PROC_ITEMS()
#undef item
} proc_specific;

static struct plat_attr_kthread_specific {
#define item(description, level, name, type, attrs) type * name;
PLAT_ATTR_KTHREAD_ITEMS()
#undef item
} __thread kthread_specific;

struct plat_attr_uthread_specific {
#define item(description, level, name, type, attrs) type * name;
PLAT_ATTR_UTHREAD_ITEMS()
#undef item
};

static struct plat_attr_uthread_specific *(*get_attr_uthread_specific_fn)() =
    NULL;

static __inline__ struct plat_attr_proc_specific *
get_proc_specific() {
    return (&proc_specific);
}

static __inline__ struct plat_attr_kthread_specific *
get_kthread_specific() {
    return (&kthread_specific);
}

static __inline__ struct plat_attr_uthread_specific *
get_uthread_specific() {
    if (get_attr_uthread_specific_fn) {
        return ((*get_attr_uthread_specific_fn)());
    } else {
        return (NULL);
    }
}

void
plat_attr_uthread_getter_set(struct plat_attr_uthread_specific *(*getter)()) {
    if (!__sync_bool_compare_and_swap(&get_attr_uthread_specific_fn,
                                      NULL, getter)) {
        plat_assert(get_attr_uthread_specific_fn == getter);
    }
}

struct plat_attr_uthread_specific *
plat_attr_uthread_alloc() {
    struct plat_attr_uthread_specific *ret;
    plat_calloc_struct(&ret);
    return (ret);
}

void
plat_attr_uthread_free(struct plat_attr_uthread_specific *spec) {
    if (spec) {
        plat_free(spec);
    }
}

/*
 * FIXME: Add kthread specific once we have one of those which is using
 * pthread code.  When that happens, make one of the fields an at-exit
 * list and make plat_kthread_exit() iterate over that where the last item
 * frees the structure.
 */

#define item(description, level, name, type, attrs)                            \
    type *                                                                     \
    plat_attr_ ## name ## _get() {                                             \
        struct plat_attr_ ## level ## _specific * aggregate =                  \
            get_ ## level ## _specific();                                      \
        return (aggregate->name);                                              \
    }                                                                          \
                                                                               \
    type *                                                                     \
    plat_attr_ ## name ## _set(type * next) {                                  \
        struct plat_attr_ ## level ## _specific * aggregate =                  \
            get_ ## level ## _specific();                                      \
        type *ret = aggregate->name;                                           \
        aggregate->name = next;                                                \
        return (ret);                                                          \
    }
PLAT_ATTR_ITEMS()

#undef item
