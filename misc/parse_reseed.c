/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
