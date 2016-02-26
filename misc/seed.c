/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/misc/parse_seed.c
 * Author: drew
 *
 * Created on February 15, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: seed.c 281 2008-02-17 00:58:00Z drew $
 */

#include <stdio.h>

#include "platform/stdlib.h"
#include "platform/time.h"

#include "misc.h"

/* Include seed in all core dumps */
static unsigned int seed_value;

int
seed_arg() {
    struct timeval now;

    plat_gettimeofday(&now, NULL);
    seed_value = (unsigned int) now.tv_sec;

    fprintf(stderr, "seed: %u\n", seed_value);
    plat_srandom(seed_value);

    return (0);
}

unsigned int
get_seed_arg() {
    return (seed_value);
}
