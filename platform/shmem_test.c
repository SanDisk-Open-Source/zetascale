/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/platform/shmem_test.c
 *
 * Author: drew
 *
 * Created on July 16, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmem_test.c 10527 2009-12-12 01:55:08Z drew $
 */

/**
 * All common trivial test code.  Prototypes are in the common header.
 *
 * Address the problem of developers cutting and pasting the same
 * initialization code in 50 different places which all need to be
 * changed by hand when structures, etc. change.
 */

#include "platform/assert.h"
#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/unistd.h"

#define LOG_CAT PLAT_LOG_CAT_PLATFORM_SHMEM

static struct plat_shmem_config shmem_test_config;

static int shmem_test_started = 0;

void
plat_shmem_trivial_test_start(int argc, char **argv) {
    int status;
    const char *path;

    plat_shmem_config_init(&shmem_test_config);

    status = plat_shmem_prototype_init(&shmem_test_config);
    if (status) {
        plat_log_msg(20876, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                     "shmem init failure: %s", plat_strerror(-status));
        plat_exit(1);
    }

    path = plat_shmem_config_get_path(&shmem_test_config);
    status = plat_shmem_attach(plat_shmem_config_get_path(&shmem_test_config));
    if (status) {
        plat_log_msg(20073, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                     "shmem_attach(%s) failed: %s", path,
                     plat_strerror(-status));
        plat_exit(1);
    }

    shmem_test_started = 1;
}

void
plat_shmem_trivial_test_end() {
    int status;
    plat_assert(shmem_test_started);

    status = plat_shmem_detach();
    if (status) {
        plat_exit(1);
    }
    plat_shmem_config_destroy(&shmem_test_config);
    shmem_test_started = 0;
}

static __attribute__((constructor)) void
shmem_test_constructor() {
    plat_log_reference();
}

static __attribute__((destructor)) void
shmem_test_destructor() {
    if (shmem_test_started) {
        plat_log_msg(21044, LOG_CAT, PLAT_LOG_LEVEL_WARN,
                     "plat_shmem_trivial_test_end not called");
        plat_shmem_trivial_test_end();
    }

    plat_log_release();
}
