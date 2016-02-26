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
 * File:   sdf/platform/printf.c
 * Author: drew
 *
 * Created on February 22, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: printf.c 8107 2009-06-24 00:19:33Z drew $
 */

/*
 * Thin wrappers for functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#define PLATFORM_PRINTF_C 1

#include <stdarg.h>

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"

int
plat_asprintf(char **ptr, const char *fmt, ...) {
    int ret;
    va_list ap;

    va_start(ap, fmt);
    ret = plat_vasprintf(ptr, fmt, ap);
    va_end(ap);

    return (ret);
}

int
plat_vasprintf(char **ptr, const char *fmt, va_list ap) {
    va_list ap_copy;
    int ret;
    int tmp;

    /*
     * Determine size.  64 bit Linux vsnprintf consumes the original ap so
     * it needs a copy.
     */
    va_copy(ap_copy, ap);
    ret = vsnprintf(NULL, 0, fmt, ap_copy);
    va_end(ap_copy);

    /*
     * XXX plat_alloc may just be a thin wrapper around malloc, in which
     * case we could (on the non-simulated platform) just call asprintf()
     * directly.  I did this because it works regardless of what happens
     * in the simulated platform, etc.
     */
    *ptr = plat_alloc(ret + 1);
    if (!*ptr) {
        ret = -1;
    } else {
        va_copy(ap_copy, ap);
        tmp = vsnprintf(*ptr, ret + 1, fmt, ap_copy);
        va_end(ap_copy);
        plat_assert(tmp == ret);
    }

    return (ret);
}

int
plat_snprintfcat(char **s, int *len, const char *fmt, ...) {
    int ret;
    int got;
    va_list ap;

    if (*s) {
        va_start(ap, fmt);
        got = vsnprintf(*s, *len, fmt, ap);
        va_end(ap);

        if (got < *len) {
            *len -= got;
            *s += got;
            ret = 0;
        } else {
            (*s)[*len - 1] = 0;
            *len = 0;
            *s = NULL;
            ret = -ENOSPC;
        }
    } else {
        ret = -ENOSPC;
    }

    return (ret);
}
