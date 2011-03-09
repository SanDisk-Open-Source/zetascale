/*
 * File:   sdf/misc/parse_uint.c
 * Author: drew
 *
 * Created on February 15, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: parse_uint.c 281 2008-02-17 00:58:00Z drew $
 */

#include <errno.h>
#include <limits.h>
#include <stdlib.h>

#include "misc.h"

int
parse_uint(unsigned int *out_ptr, const char *string,
    const char **end_ptr_ptr) {
    int ret;
    unsigned long tmp;
    char *end_ptr;

    errno = 0;

    tmp = strtoul((char *)string, &end_ptr, 0);
    if ((tmp == ULONG_MAX && errno == ERANGE) || tmp > INT_MAX) {
        ret = -ERANGE;
    } else if (*end_ptr) {
        ret = -EINVAL;
    } else {
        if (out_ptr) {
            *out_ptr = (unsigned int)tmp;
        }
        ret = 0;
    }

    if (end_ptr_ptr) {
        *end_ptr_ptr = end_ptr;
    }

    return (ret);
}
