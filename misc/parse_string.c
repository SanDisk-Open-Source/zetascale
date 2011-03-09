/*
 * File:   parse_string_alloc.c
 * Author: drew
 *
 * Created on April 3, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: parse_string_alloc.c 182 2008-02-06 20:00:51Z drew $
 */

#include <errno.h>
#include <limits.h>
#include <string.h>

#include "platform/stdlib.h"

int
parse_string_helper(char *out, const char *in, int max_len) {
    int ret;
    size_t in_len = strlen(in) + 1;

    if (in_len <= max_len) {
        memcpy(out, in, in_len);
        ret = 0;
    } else {
        ret = -ENAMETOOLONG;
    }

    return (ret);
}
