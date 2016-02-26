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
 * File:   $HeaderURL:$
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: string.c 516 2008-03-06 04:19:56Z drew $
 */

/*
 * Try for standard version of strerror_r returning int == -1 on error with
 * errno == EINVAL on unknown error, but paleolithic Linux lacks this.
 */
#undef _GNU_SOURCE
#undef _XOPEN_SOURCE
#ifdef notyet
#define _XOPEN_SOURCE 600
#else
#define CHAR_STRERROR_R
#define _GNU_SOURCE
#endif

#include <stdio.h>

#define PLATFORM_INTERNAL 1

#include "platform/assert.h"
#include "platform/errno.h"

#include "platform/string.h"
#include "platform/stdlib.h"

int
plat_strerror_r(int error, char *buf, int n) {
    int ret;
    char *tmp;

    if (error < PLAT_ERRNO_BASE) {
#ifndef CHAR_STRERROR_R
        ret = sys_strerror_r(error, buf, n);
        if (ret == -1) {
            plat_errno = errno;
        }
#else
        tmp = sys_strerror_r(error, buf, n);
        if (tmp == buf) {
            ret = 0;
        } else {
            size_t len = strlen(tmp) + 1;
            if (len <= n) {
                memcpy(buf, tmp, len);
                ret = 0;
            } else {
                ret = -1;
                plat_errno = ERANGE;
            }
        }
#endif
    } else {
        switch (error) {
#define item(caps, description) \
        case (PLAT_ ## caps): tmp = description; ret = 0; break;
        PLAT_ERRNO_ITEMS()
#undef item
        default:
            tmp = NULL;
            plat_errno = EINVAL;
            ret = -1;
        }

        if (!ret && strlen(tmp) + 1 > n) {
            ret = -1;
            plat_errno = ERANGE;
        }

        if (!ret) {
            strcpy(buf, tmp);
        }
    }

    return (ret);
}

/*
 * FIXME: Use thread local storage thus making this reentrant
 */
const char *
plat_strerror(int error) {
    static char buf[1024];

    if (plat_strerror_r(error, buf, sizeof (buf))) {
        plat_assert(plat_errno == EINVAL);
        snprintf(buf, sizeof (buf), "Unknown error %d", error);
    }

    return (buf);
}


char *
plat_strdup(const char *string) {
    int len = strlen(string) + 1;
    char *ret = plat_alloc(len);
    if (ret) {
        memcpy(ret, string, len);
    }
    return (ret);
}

int
plat_strncount(const char *string, int n, int c) {
    int ret;
    const char *ptr;
    const char *end;

    for (ret = 0, ptr = string, end = string + n; ptr < end && *ptr; ++ptr) {
        if (*ptr == c) {
            ++ret;
        }
    }

    return (ret);
}

const char *
plat_strnchr(const char *string, int n, int c) {
    const char *ptr;
    const char *end;

    for (ptr = string, end = string + n;
         ptr < end && *ptr && *ptr != c; ++ptr) {
    }

    return (ptr < end ? ptr : NULL);
}

int
plat_indirect_strcmp(const void *lhs, const void *rhs) {
    const char **lhs_strp = (const char **) lhs;
    const char **rhs_strp = (const char **) rhs;
    return (strcmp(*lhs_strp, *rhs_strp));
}

/* char *strerror_r does not have a defined error return */
#ifndef CHAR_STRERROR_R

/*
 * Sanity check that our internal errno definitions do not conflict with the
 * system's.
 */
static void verify_errors() __attribute__((constructor));

static void verify_errors() {
    char buf[1024];

#define item(caps, description)                                                \
    plat_assert_always(sys_strerror_r(PLAT_ ## caps, buf, sizeof (buf)) ==     \
                       -1 && errno == EINVAL);
    PLAT_ERRNO_ITEMS()
#undef item
}

#endif /* ndef CHAR_STRERROR_R */
