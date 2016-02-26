/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/platform/memory_size.c
 * Author: drew
 *
 * Created on December 1, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: memory_size.c 10527 2009-12-12 01:55:08Z drew $
 */

#include <errno.h>
#include <stdio.h>

#include "platform/logging.h"
#include "platform/platform.h"
#include "platform/string.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_ALLOC, "memory_size");

static const char meminfo[] = "/proc/meminfo";

ssize_t
plat_get_address_space_size() {
    ssize_t ret = 0;
    long long tmp;
    FILE *in;
    char buf[512];

    in = fopen(meminfo, "r");
    if (!in) {
        ret = errno ? -errno : -EINVAL;
    }

    while (ret == 0 && in /* placate coverity */) {
        errno = 0;
        if (!fgets(buf, sizeof(buf), in)) {
            if (!errno) {
                plat_log_msg(20952, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "Couldn't find MemTotal in %s", meminfo);
                ret = -EINVAL;
            } else {
                ret = -errno;
                plat_log_msg(20953, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "Error reading %s: %s", meminfo,
                             plat_strerror((int)-ret));
            }
        } else if (sscanf(buf, "MemTotal: %lld", &tmp) == 1) {
            ret = (ssize_t)tmp * 1024;
        }
    }

    if (in) {
        fclose(in);
    }

    return (ret);
}
