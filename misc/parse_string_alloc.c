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
