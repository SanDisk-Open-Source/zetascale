/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
