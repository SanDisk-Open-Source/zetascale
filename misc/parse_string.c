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
