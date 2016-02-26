/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/misc/timeout.c
 * Author: drew
 *
 * Created on December 23, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: timeout.c 5287 2008-12-24 04:38:05Z drew $
 */

#include <stdio.h>

#include "platform/unistd.h"

#include "misc/misc.h"

/* Include seed in all core dumps */
static int timeout;

int
parse_timeout(const char *string) {
    int ret;

    ret = parse_int(&timeout, string, NULL);
    if (!ret) {
        alarm(timeout);
    }

    return (ret);
}
