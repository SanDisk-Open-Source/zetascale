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
 * $Id: parse_reseed.c 281 2008-02-17 00:58:00Z drew $
 */

#include <stddef.h>

#include "platform/stdlib.h"

#include "misc.h"

/**
 * Parse reseed command line argument to seed program random number generator.
 */
int
parse_reseed(const char *string) {
    int ret;
    unsigned int seed;

    ret = parse_uint(&seed, string, NULL);
    if (!ret) {
        plat_srandom(seed);
    }

    return (ret);
}
