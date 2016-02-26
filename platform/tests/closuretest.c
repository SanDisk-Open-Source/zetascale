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
 * File:   sdf/platform/tests/closuretest.c
 * Author: drew
 *
 * Created on January 26, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: closuretest.c 1918 2008-07-02 23:45:06Z hiney $
 */

/*
 * Trivial test for closure code which mostly just sanity
 * checks that the macros are correct.
 */

#include <stddef.h>

#include "platform/assert.h"
#include "platform/closure.h"

PLAT_CLOSURE(closure_void);
PLAT_CLOSURE1(closure_int, int, arg);

enum {
    TEST_VOID_RAN = 0xdeadbeef
};

static void
test_void(plat_closure_scheduler_t *context, void *env) {
    *(int *)env = TEST_VOID_RAN;
}

enum {
    TEST_INT_RAN = 0xeadf00d
};

static void
test_int(plat_closure_scheduler_t *context, void *env, int arg1) {
    *(int *)env = arg1;
}

int
main(int argc, char **argv) {
    closure_void_t closure_void = PLAT_CLOSURE_INITIALIZER;
    closure_int_t closure_int = PLAT_CLOSURE_INITIALIZER;
    int env;

    closure_void = closure_void_create(PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS,
                                       &test_void, &env);
    env = 0;
    plat_closure_apply(closure_void, &closure_void);
    plat_assert_always(env == TEST_VOID_RAN);

    env = 0;
    closure_int = closure_int_create(PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS,
                                     &test_int, &env);
    plat_closure_apply(closure_int, &closure_int, TEST_INT_RAN);
    plat_assert_always(env == TEST_INT_RAN);

    return (0);
}
