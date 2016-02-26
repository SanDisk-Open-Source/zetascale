/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
