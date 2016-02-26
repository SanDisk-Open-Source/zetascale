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
 * File:   sdf/misc/parse_int.c
 * Author: drew
 *
 * Created on January 26, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: parse_int.c 7862 2009-06-05 22:57:18Z johann $
 */

#include <errno.h>
#include <limits.h>
#include <stdlib.h>

#include "misc.h"

int
parse_int(int *out_ptr, const char *string, const char **end_ptr_ptr) {
    int ret;
    long tmp;
    char *end_ptr;

    /*
     * Horrible hack to allow one to pass a -1 as a parameter to an option.  If
     * you say -option -1, memcached thinks the -1 is an argument of its own.
     */
    if (string[0] == '@')
        string++;

    errno = 0;
    /*
     * Assume long long is the longest type which is potentially longer than
     * 64 bits.
     */
    tmp = strtol((char *)string, &end_ptr, 0);
    if (((tmp == LONG_MIN || tmp == LONG_MAX) && errno == ERANGE) ||
        (tmp < INT_MIN || tmp > INT_MAX)) {
        ret = -ERANGE;
    } else if (*end_ptr) {
        ret = -EINVAL;
    } else {
        if (out_ptr) {
            *out_ptr = (int)tmp;
        }
        ret = 0;
    }

    if (end_ptr_ptr) {
        *end_ptr_ptr = end_ptr;
    }

    return (ret);
}
