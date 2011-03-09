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
