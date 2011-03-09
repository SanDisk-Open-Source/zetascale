/*
 * File:   platform/random.h
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: random.c 708 2008-03-24 23:28:34Z drew $
 */

/*
 * Random number generator.  Should probably have a simple Knuth
 * implementation.
 */

#define PLATFORM_INTERNAL 1

#include "platform/stdlib.h"

#define item(ret, sym, declare, call, cppthrow, attributes) \
    PLAT_WRAP_IMPL(ret, sym, declare, call, cppthrow, attributes)
PLAT_STDLIB_WRAP_RANDOM_ITEMS()
#undef item
