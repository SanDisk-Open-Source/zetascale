/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: sdf/misc/parse_int32.c
 * Author: Johann George
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include "misc.h"

int
parse_int32(int32_t *ptr, const char *str)
{
    char *end;

    if (str[0] == '@')
        str++;
    *ptr = strtol((char *)str, &end, 0);
    if (*end)
        return -EINVAL;
    return 0;
}
