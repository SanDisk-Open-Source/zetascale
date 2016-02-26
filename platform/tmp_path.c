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
 * File:   sdf/platform/get_exe.c
 * Author: drew
 *
 * Created on February 27, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tmp_path.c 10527 2009-12-12 01:55:08Z drew $
 */

#include <limits.h>

#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/mutex.h"
#include "platform/platform.h"
#include "platform/stat.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/unistd.h"

static const char self_exe[] = "/proc/self/exe";

/*
 * Lazy default setting means that this may be invoked in a threaded environment
 * so we should lock.
 */
static plat_mutex_t init_lock = PLAT_MUTEX_INITIALIZER;
static int initialized = 0;
static char tmp_path[PATH_MAX + 1] = "";

static int set_tmp_path_internal(const char *path);

const char *
plat_get_tmp_path() {
    int was_initialized;

    plat_mutex_lock(&init_lock);
    if (!initialized) {
        set_tmp_path_internal(NULL);
    }
    was_initialized = initialized;
    plat_mutex_unlock(&init_lock);

    /*
     * XXX: Initialization happens before we start threads, so
     * there isn't a race on tmp_path setup.  It would be better
     * to have separate default and real buffers one of which would
     * be returned.
     */
    return (was_initialized ? tmp_path : NULL);
}

int
plat_set_tmp_path(const char *path) {
    int ret;

    plat_mutex_lock(&init_lock);
    if (initialized) {
        ret = set_tmp_path_internal(path);
        plat_log_msg(21046, PLAT_LOG_CAT_PLATFORM_MISC,
                     PLAT_LOG_LEVEL_WARN,
                     "temp directory already set to %s not %s", tmp_path, path);
        ret = -EEXIST;
    } else if (path && strlen(path) > PATH_MAX) {
        plat_log_msg(21047, PLAT_LOG_CAT_PLATFORM_MISC,
                     PLAT_LOG_LEVEL_ERROR, "name  %s too long", path);
        ret = -ENAMETOOLONG;
    } else {
        ret = set_tmp_path_internal(path);
    }

    plat_mutex_unlock(&init_lock);

    return (ret);
}

static int
set_tmp_path_internal(const char *path) {
    const char *tmppath_env = getenv("TMPPATH");
    const char *user_env = getenv("USER");
    int len;
    int status;
    uid_t uid;

    if (path) {
        len = snprintf(tmp_path, sizeof(tmp_path), "%s", path);
    } else if (tmppath_env) {
        len = snprintf(tmp_path, sizeof(tmp_path), "%s", tmppath_env);
    } else if (user_env) {
        len = snprintf(tmp_path, sizeof(tmp_path), "/tmp/%s", user_env);
    } else {
        uid = getuid();
        len = snprintf(tmp_path, sizeof(tmp_path), "/tmp/%d", (int)uid);
    }

    if (len >= sizeof (tmp_path)) {
        tmp_path[sizeof (tmp_path) - 1] = 0;
    }

    status = (plat_mkdir(tmp_path, 0777) != -1 || plat_errno == EEXIST) ?
        0 : -plat_errno;
    initialized = !status;
    if (!initialized) {
        plat_log_msg(21048, PLAT_LOG_CAT_PLATFORM_MISC,
                     PLAT_LOG_LEVEL_ERROR, "Cannot create temp directory: %s",
                     plat_strerror(errno));
    }

    return (status);
}
