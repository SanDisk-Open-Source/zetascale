/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: sdf/misc/parse_time.c
 * Author: Johann George
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "misc.h"

int
parse_time(int64_t *timep, const char *str)
{
    char *end;
    double secs;

    if (strcmp(str, "end") == 0) {
        *timep = -1;
        return 0;
    }

    secs = strtod(str, &end);
    if (*end) {
        if (strcmp(end, "ns") == 0)
            secs /= 1000 * 1000 * 1000;
        else if (strcmp(end, "us") == 0)
            secs /= 1000 * 1000;
        else if (strcmp(end, "ms") == 0)
            secs /= 1000;
        else if (strcmp(end, "s") == 0)
            ;
        else if (strcmp(end, "m") == 0)
            secs *= 60;
        else if (strcmp(end, "h") == 0)
            secs *= 60 * 60;
        else if (strcmp(end, "d") == 0)
            secs *= 24 * 60 * 60;
        else
            return -EINVAL;
    }

    *timep = secs * 1000 * 1000 * 1000;
    return 0;
}
