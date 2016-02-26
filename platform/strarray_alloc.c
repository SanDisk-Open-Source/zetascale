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
 * Created on February 28, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: strarray_alloc.c 3220 2008-09-04 01:21:10Z jdybnis $
 */

#define PLATFORM_INTERNAL 1

#include "platform/platform.h"
#include "platform/string.h"
#include "platform/stdlib.h"

enum strarray_how {
    STRARRAY_SYS,
    STRARRAY_PLAT
};

static char *strarray_alloc_common(int nstring, const char * const *string,
                                   const char *sep, enum strarray_how how);

char *
plat_strarray_alloc(int nstring, const char * const *string, const char *sep) {
    return (strarray_alloc_common(nstring, string, sep, STRARRAY_PLAT));
}

char *
plat_strarray_sysalloc(int nstring, const char * const *string,
                       const char *sep) {
    return (strarray_alloc_common(nstring, string, sep, STRARRAY_SYS));
}

static char *
strarray_alloc_common(int nstring, const char * const *string,
                      const char *sep, enum strarray_how how) {
    char *ret = NULL; /* placate GCC */
    int seplen;
    int len;
    int count;
    char *ptr;
    int i;
    int sublen;

    ret = NULL;
    seplen = sep ? strlen(sep) : 0;
    /* NUL */
    len = 1;
    for (count = i = 0; i < nstring || (nstring == -1 && string[i]); ++i) {
        len += strlen(string[i]);
        ++count;
    }
    len += count > 1 ? seplen * (count - 1) : 0;

    switch (how) {
    case STRARRAY_PLAT:
        ret = plat_alloc(len);
        break;
    case STRARRAY_SYS:
        ret = sys_malloc(len);
        break;
    default:
        ret = NULL;
    }

    if (ret) {
        for (ptr = ret, i = 0; i < count; ++i) {
            if (sep && i > 0) {
                memcpy(ptr, sep, seplen);
                ptr += seplen;
            }
            sublen = strlen(string[i]);
            memcpy(ptr, string[i], sublen);
            ptr += sublen;
        }
        *ptr = 0;
    }

    return (ret);
}
