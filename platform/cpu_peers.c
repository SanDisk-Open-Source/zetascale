/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/platform/cpu_peers.c
 * Author: drew
 *
 * Created on March 9, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: cpu_peers.c 10527 2009-12-12 01:55:08Z drew $
 */

/* stddef avoids double definition of size_t in glob.h when using FLATTEN=1 */
#include <stddef.h>
#include <glob.h>
#include <limits.h>

#define PLATFORM_INTERNAL 1

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM, "cpu_peers");

static int glob_wrapper(const char *pattern, glob_t *globbuf);
static int update_peers(const char *dirname, uint32_t *peers,
                        int *highest_cache);
static int get_peers(const char *dirname, uint32_t *peers);
static int get_level(const char *dirname, int *level);
static int parse_file(const char *dirname, const char *file,
                      const char *fmt, ...);

int
plat_get_cpu_count() {
    int ret = 0;
    glob_t glob_ret;

    ret = glob_wrapper("/sys/devices/system/cpu/[0-9]*", &glob_ret);

    for (; ret >= 0 && glob_ret.gl_pathv[ret]; ++ret) {
    }
    globfree(&glob_ret);

    return (ret);
}

int
plat_get_cpu_cache_peers(uint32_t *peers, int cpu) {
    int ret = 0;
    int globbed = 0;
    char *cpu_dir = NULL;
    char *cpu_glob = NULL;
    glob_t glob_ret;
    int i;
    int highest_cache = -1;

    plat_assert_always(cpu <= sizeof (*peers) * CHAR_BIT);

    if (sys_asprintf(&cpu_dir, "/sys/devices/system/cpu/cpu%d", cpu) == -1) {
        ret = -errno;
    }

    if (!ret && sys_asprintf(&cpu_glob, "%s/cache/index*", cpu_dir) == -1)   {
        ret = -errno;
    }

    if (!ret) {
        ret = glob_wrapper(cpu_glob, &glob_ret);
        globbed = 1;
    }

    for (i = 0; !ret && glob_ret.gl_pathv[i]; ++i) {
        ret = update_peers(glob_ret.gl_pathv[i], peers, &highest_cache);
    }

    if (highest_cache < 0) {
        plat_log_msg(20912, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "can't find any cache levels in %s", cpu_glob);
        ret = -EINVAL;
    }

    if (globbed) {
        globfree(&glob_ret);
    }
    sys_free(cpu_glob);
    sys_free(cpu_dir);

    return (ret);
}

static int
glob_wrapper(const char *pattern, glob_t *globbuf) {
    int ret;
    int status;

    status = glob(pattern, GLOB_ERR, NULL, globbuf);
    switch (status) {
    case 0:
        ret = 0;
        break;
    case GLOB_NOSPACE:
        ret = -ENOMEM;
        break;
    case GLOB_ABORTED:
        plat_log_msg(20913, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "glob %s failed: GLOB_ABORTED", pattern);
        ret = -EIO;
        break;
    case GLOB_NOMATCH:
        plat_log_msg(20914, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "glob %s failed: GLOB_NOMATCH", pattern);
        ret = -EINVAL;
        break;
    default:
        plat_log_msg(20915, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "glob %s failed: unknown result %d", pattern,
                     status);
        ret = -EIO;
    }

    return (ret);
}

static int
update_peers(const char *dirname, uint32_t *peers, int *highest_cache) {
    int ret;
    int level;

    ret = get_level(dirname, &level);

    if (!ret && level > *highest_cache) {
        *highest_cache = level;
        ret = get_peers(dirname, peers);
    }

    return (ret);
}

static int
get_peers(const char *dirname, uint32_t *peers) {
    int ret;
    char *comma = NULL;
    char tmp[81] = {0, };

    ret = parse_file(dirname, "shared_cpu_map", "%80s", tmp);

    if (!ret) {
        comma = strrchr(tmp,  ',');
        if (!comma) {
            plat_log_msg(20916, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "expected comma delimited list in %s/shard_cpu_map",
                         dirname);
            ret = -EINVAL;
        }
    }

    if (!ret && sscanf(comma + 1, "%x", peers) != 1) {
        plat_log_msg(20917, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "can't parse %s from %s/shard_cpu_map",
                     tmp, dirname);
        ret = -EINVAL;
    }

    return (ret);
}

static int
get_level(const char *dirname, int *level) {
    return (parse_file(dirname, "level", "%d", level));
}

static int
parse_file(const char *dirname, const char *subfile, const char *fmt, ...) {
    int ret = 0;
    char *filename = NULL;
    FILE *file = NULL;
    va_list ap;

    va_start(ap, fmt);

    if (sys_asprintf(&filename, "%s/%s", dirname, subfile) == -1)  {
        ret = -ENOMEM;
    } 

    if (!ret && filename) {
        file = fopen(filename, "r");
        if (!file) {
            ret = -errno;
            plat_log_msg(20918, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "can't open %s: %s", filename, plat_strerror(-ret));
        }
    }

    if (!ret && file && vfscanf(file, fmt, ap) == EOF) {
        plat_log_msg(20919, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "error parsing %s", filename);
        ret = -EINVAL;
    }

    va_end(ap);

    if (file) {
        fclose(file);
    }

    sys_free(filename);

    return (ret);
}
