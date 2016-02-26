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
