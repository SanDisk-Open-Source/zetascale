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
