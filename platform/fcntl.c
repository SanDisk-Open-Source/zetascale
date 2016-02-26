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
