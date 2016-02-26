/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/fth/fth_printf_test.c
 * Author: drew
 *
 * Created on August 21, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fth_printf_test.c 10527 2009-12-12 01:55:08Z drew $
 */

/**
 * Problem: since gcc only preserves stack alignment, we get segmentation
 * faults when attempting to printf doubles.
 */
#include <math.h>
#include <stdio.h>

#include "platform/logging.h"
#include "platform/mbox_scheduler.h"
#define PLAT_OPTS_NAME(name) name ## _fth_printf_test
#include "platform/opts.h"
#include "platform/shmem.h"

#include "fth/fth.h"
#include "fth/fthOpt.h"

#define PLAT_OPTS_ITEMS_fth_printf_test()                                   \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    PLAT_OPTS_FTH()

struct plat_opts_config_fth_printf_test {
    struct plat_shmem_config shmem;
};

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_FTH, "test/printf");

static void
printfThread(uint64_t as_thread) {
    double pi = M_PI;

    printf("%g\n", pi);
    fflush(stdout);

    if (as_thread) {
        fthKill(1);
    }
}

int
main(int argc, char **argv) {
    int ret = 0;
    int shmem_attached = 0;
    struct plat_opts_config_fth_printf_test config;
    int status;
    fthThread_t *thread;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);
   
    if (plat_opts_parse_fth_printf_test(&config, argc, argv)) {
        ret = 2;
    }

    if (!ret) {
        status = plat_shmem_prototype_init(&config.shmem);
        if (status) {
            plat_log_msg(20876, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "shmem init failure: %s", plat_strerror(-status));
            ret = 1;
        }
    }

    if (!ret) {
        status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
        if (status) {
            plat_log_msg(20877, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "shmem attach failure: %s", plat_strerror(-status));
            ret = 1;
        } else {
            shmem_attached = 1;
        }
    }

    if (!ret) {
        printfThread(0 /* not as fth thread */);

        fthInit();

        thread = XSpawn(&printfThread, fthGetDefaultStackSize());
        XResume(thread, 1 /* as fth thread */);

        fthSchedulerPthread(0);
    }

    if (shmem_attached) {
        status = plat_shmem_detach();
        if (status) {
            plat_log_msg(20880, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "shmem detach failure: %s", plat_strerror(-status));
            ret = 1;
        }
    }

    plat_shmem_config_destroy(&config.shmem);

    return (ret);
}

#include "platform/opts_c.h"
