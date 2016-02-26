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
 * File:   alloc.c
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: alloc.c 3232 2008-09-04 18:55:54Z drew $
 */

/*
 * Allocator functions which look suspiciously like malloc but aren't.
 *
 * Non-standard allocation functions are provided so that the simulated
 * environment has an easier time separating target program and simulator
 * memory.  This is important for simulating process crashes where all
 * resources held by the target must be released.
 *
 * They are also provided so that the schooner code has separate allocation
 * arenas which can be used to make it more difficult for a misbehaving
 * third party application client with our code co-resident (like MySQL,
 * with 2500 reported segmentation faults) to interfere with our software.
 * A system compromising decreased performance for increased reliability could
 * even enable writes to our memory arena when our threads are scheduled, and
 * disable at all other times.
 */

#define PLATFORM_INTERNAL 1

#include "platform/logging.h"
#include "platform/stdlib.h"

#ifdef notyet
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_ALLOC, "malloc");

#define item(ret, sym, declare, call, cppthrow, attributes) \
    PLAT_WRAP_IMPL(ret, sym, declare, call, cppthrow, attributes)
PLAT_STDLIB_WRAP_ALLOC_ITEMS()
#undef item
#endif /* def notyet */
