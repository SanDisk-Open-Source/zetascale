/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/assert.c $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: assert.c 10527 2009-12-12 01:55:08Z drew $
 */

/**
 * Platform assert which tries to output to the logging subsystem before 
 * giving up the ghost. 
 */

#include <stdio.h>

#include "platform/assert.h"
#include "platform/logging.h"
#include "platform/mutex.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/unistd.h"

void
plat_assert_fail(const char *expr, const char *file,
    unsigned line, const char *fn) {
    /* First spew wins but don't deadlock on recursion */
    static plat_mutex_t one_thread_dies = PLAT_RECURSIVE_MUTEX_INITIALIZER;
    static int recursing = 0;
    /* Use less stack space. */
    static char buf[1024];

    plat_mutex_lock(&one_thread_dies);

    ++recursing;

    snprintf(buf, sizeof (buf) - 1,
        "%s:%u: %s: Assertion `%s' failed.%s\n", file, line, fn,
        expr, recursing > 1 ? " (double fault)" : "");
    if (plat_write(2, buf, strlen(buf))) {}

    if (recursing == 1) {
        plat_log_msg(20905, PLAT_LOG_CAT_PLATFORM, 
            PLAT_LOG_LEVEL_FATAL, "%s:%u: %s: Assertion `%s' failed.",
            file, line, fn, expr);
    } 

    --recursing;

    plat_abort();
}
