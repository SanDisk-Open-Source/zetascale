/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   file.c
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: file.c 427 2008-03-01 03:28:13Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#define PLATFORM_INTERNAL 1

#include <sys/socket.h>

#include <fcntl.h>
#include <stdarg.h>

#include "platform/fcntl.h"
#include "platform/stat.h"
#include "platform/unistd.h"

#define item(ret, sym, declare, call, cppthrow, attributes) \
    PLAT_WRAP_IMPL(ret, sym, declare, call, cppthrow, attributes)
PLAT_UNISTD_WRAP_FILE_ITEMS()
PLAT_STAT_WRAP_FILE_ITEMS()
#undef item

int
plat_creat(const char *pathname, int mode) {
    return (creat(pathname, mode));
}

int
plat_open(const char *pathname, int flags, ...) {
    int ret;
    va_list ap;

    va_start(ap, flags);

    if (flags & O_CREAT)  {
        ret = open(pathname, flags, va_arg(ap, int));
    } else {
        ret = open(pathname, flags);
    }

    va_end(ap);

    return (ret);
}

ssize_t
plat_readlink(const char *path, void *buf, size_t bufsize) {
    return (sys_readlink(path, buf, bufsize));
}
