/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   parse_size.c
 * Author: drew
 *
 * Created on January 26, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: parse_size.c 13945 2010-06-02 01:01:15Z drew $
 */

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>

#include "misc.h"

/* define item(lower, upper, size) to use in meta-programming */
#define SUFFIXES() \
    item('k', 'K', 1024) \
    item('m', 'M', 1024 * 1024) \
    item('g', 'G', 1024 * 1024 * 1024) \
    item('t', 'T', 1024LL * 1024 * 1024 * 1024) \
    item('p', 'P', 1024LL * 1024 * 1024 * 1024 * 1024)

int
parse_size(int64_t *out_ptr, const char *string, const char **end_ptr_ptr) {
    int ret;
    unsigned long long tmp;
    char *end_ptr;

    errno = 0;
    /*
     * Assume long long is the longest type which is potentially longer than
     * 64 bits.
     */
    tmp = strtoll((char *)string, &end_ptr, 0);
    if (((tmp == LLONG_MIN || tmp == LLONG_MAX) && errno == ERANGE) ||
            (tmp > INT64_MAX)) {
        ret = -ERANGE;
    } else {
        switch (*end_ptr) {

#define item(a, b, c) \
        case (a): \
        case (b): \
            if (INT64_MAX / (c) >= tmp) { \
                tmp *= (c); \
                ret = 0; \
            } else { \
                ret = -ERANGE; \
            } \
            ++end_ptr; \
            break;

        SUFFIXES()
#undef item

        case 0:
            ret = 0;
            break;
        default:
            ret = -EINVAL;
        }
        if (*end_ptr) {
            ret = -EINVAL;
        }
    }

    if (!ret && out_ptr) {
        *out_ptr = (int64_t)tmp;
    }

    if (end_ptr_ptr) {
        *end_ptr_ptr = end_ptr;
    }

    return (ret);
}

const char *
parse_size_usage() {
    return ("size in decimal, octal, or hex with optional k,m,g,t suffix");
}
