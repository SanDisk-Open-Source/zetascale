/*
 * File:   sdf/platform/get_exe.c
 * Author: drew
 *
 * Created on February 27, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: get_exe.c 11222 2010-01-13 05:06:07Z drew $
 */

#include <limits.h>

#include "platform/mutex.h"
#include "platform/platform.h"
#include "platform/stat.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/unistd.h"

static const char self_exe[] = "/proc/self/exe";

static plat_mutex_t init_lock = PLAT_MUTEX_INITIALIZER;
static int initialized = 0;
static char exe_path[PATH_MAX + 1] = "";
static char exe_name[PATH_MAX + 1] = "";
static char prog_ident[PATH_MAX + 1] = "";

static int is_initialized();
static void initialize();

const char *
plat_get_exe_path() {
    return (is_initialized() ? exe_path : NULL);
}

const char *
plat_get_exe_name() {
    return (is_initialized() ? exe_name : NULL);
}

const char *
plat_get_prog_ident() {
    return (is_initialized() ? prog_ident : NULL);
}

static int
is_initialized() {
    if (!initialized) {
        plat_mutex_lock(&init_lock);
        if (!initialized) {
            initialize();
        }
        plat_mutex_unlock(&init_lock);
    }
    return (initialized);
}

static void
initialize() {
    int status;
    struct stat buf;
    char *tmp;
    int len;
    int max;

    status = plat_lstat(self_exe, &buf);
    if (status != -1) {
        status = plat_readlink(self_exe, exe_path, sizeof(exe_path) - 1);
    }
    if (status != -1) {
        exe_path[status] = 0;
        tmp = strrchr(exe_path, '/');
        strcpy(exe_name, tmp ? tmp + 1 : exe_path);

        max = sizeof (prog_ident);
        len = snprintf(prog_ident, max, "%s[%d]",
                       exe_name, (int)plat_getpid());
        if (len >= max) {
            prog_ident[max - 1] = 0;
        }

        initialized = 1;
    }
}

static void
plat_get_exe_at_fork(void *extra, pid_t pid) {
    initialized = 0;
}

PLAT_AT_FORK(get_exe, plat_get_exe_at_fork, NULL);
