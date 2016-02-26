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
