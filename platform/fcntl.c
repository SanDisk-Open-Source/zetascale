/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fcntl.c
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcntl.c 396 2008-02-28 22:55:43Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#define PLATFORM_INTERNAL 1

#include <stdarg.h>

#include "platform/fcntl.h"
#include "platform/stdlib.h"
#include "platform/stdio.h"

int
plat_fcntl(int fd, int cmd, ...) {
    int ret;
    va_list ap;

    va_start(ap, cmd);

    switch (cmd) {
    case F_SETFL:
    case F_SETFD:
        ret = sys_fcntl(fd, cmd, va_arg(ap, long));
        break;
    case F_GETLK:
    case F_SETLK:
    case F_SETLKW:
        ret = sys_fcntl(fd, cmd, va_arg(ap, struct flock *));
        break;
    default:
        fprintf(stderr, "Unwrapped fcntl cmd: %d\n", cmd);
        plat_abort();
    }

    va_end(ap);

    return (ret);
}
