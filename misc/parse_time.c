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
