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
 * File:   sdf/platform/coredump_filter.c
 * Author: drew
 *
 * Created on February 26, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: coredump_filter.c 10527 2009-12-12 01:55:08Z drew $
 */

#include <sys/statfs.h>

#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/platform.h"
#include "platform/stat.h"
#include "platform/stdio.h"
#include "platform/string.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_ALLOC, "coredump_filter");

enum {
    HUGETLBFS_MAGIC = 0x958458f6
};

/*
 * XXX: drew 2009-02-26 These should come from <linux/sched.h> although that's
 * not up-to-date every place.
 */
enum {
    MAPPED_PRIVATE = 1 << 2,
    MAPPED_SHARED = 1 << 3,
    HUGETLB_PRIVATE = 1 << 7,
    HUGETLB_SHARED = 1 << 6
};

enum get_set {
    GET,
    SET
};

static int get_set_coredump_filter(int *data, enum get_set how);

int
plat_ensure_dumped(const char *filename, int shared)  {
    int ret;
    int flags;
    int verify_flags;
    struct stat stat_buf = {};
    struct statfs statfs_buf = {};

    /* Placate coverity - flags is an output in get mode */
    flags = 0;
    ret = get_set_coredump_filter(&flags, GET);

    if (!ret) {
        ret = plat_stat(filename, &stat_buf);
    }

    if (!ret) {
        if (S_ISREG(stat_buf.st_mode)) {
            ret = statfs(filename, &statfs_buf);
            if (!ret) {
                if (statfs_buf.f_type == HUGETLBFS_MAGIC) {
                    flags |= shared ? HUGETLB_SHARED : HUGETLB_PRIVATE;
                } else {
                    flags |= shared ? MAPPED_SHARED : MAPPED_PRIVATE;
                }
            }
        }
    }

    if (!ret) {
        ret = get_set_coredump_filter(&flags, SET);
    }

    /*
     * Check that the flags actually got set.  This won't be the case for
     * huge pages with old kernels.
     */
    if (!ret) {
        /* Placate coverity - verify_flags is an output in GET mode */
        verify_flags = 0;
        ret = get_set_coredump_filter(&verify_flags, GET);
        if (!ret && verify_flags != flags) {
            plat_log_msg(20909, LOG_CAT, PLAT_LOG_LEVEL_WARN,
                         "coredump_filter not set (try a newer kernel)");
        }
    }

    /* XXX: Should validate that what was asked for is what we got */

    return (ret);
}

static int
get_set_coredump_filter(int *data, enum get_set how) {
    int ret;
    FILE *f;

    f = fopen("/proc/self/coredump_filter", how == SET ? "w" : "r");

    if (!f) {
        ret = -errno;
    } else if (how == SET) {
        ret = fprintf(f, "0x%x\n", *data) != EOF ? 0 : -errno;
    } else {
        ret = fscanf(f, "%x", data) != EOF ? 0 : -errno;
    }

    if (f && fclose(f) == EOF && !ret) {
        ret = -errno;
    }

    if (ret) {
        plat_log_msg(20910, LOG_CAT,
                     PLAT_LOG_LEVEL_WARN, "Failed to %s coredump_filter: %s",
                     how == SET ? "set" : "get", plat_strerror(-ret));
    } else {
        plat_log_msg(20911, LOG_CAT,
                     PLAT_LOG_LEVEL_TRACE, "Core dump filter %s val 0x%x",
                     how == SET ? "set" : "get", *data);
    }

    return (ret);
}
