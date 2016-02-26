/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   parse_string_alloc.c
 * Author: drew
 *
 * Created on January 26, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: parse_string_alloc.c 3240 2008-09-05 01:50:35Z drew $
 */

#define PLATFORM_INTERNAL 1

#include <errno.h>
#include <limits.h>
#include <string.h>

#include "platform/stdlib.h"

#include "misc/misc.h"

int
parse_string_alloc(char **out, const char *in, int max_len) {
    int ret;
    size_t in_len = strlen(in);
    char *tmp;

    if (max_len <= 0 || in_len <= max_len) {
        tmp = sys_malloc(in_len + 1);
        if (!tmp) {
            ret = -ENOMEM;
        } else {
            memcpy(tmp, in, in_len + 1);
            parse_string_free(*out);
            *out = tmp;
        }
        ret = 0;
    } else {
        ret = -ENAMETOOLONG;
    }

    return (ret);
}

void
parse_string_free(char *ptr) {
    if (ptr) {
        sys_free(ptr);
    }
}
