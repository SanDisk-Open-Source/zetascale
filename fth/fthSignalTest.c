/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthSignalTest
 * Author: drew
 *
 * Created on April 2, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fthSignalTest.c 6590 2009-04-02 06:23:49Z jbertoni $
 */

/**
 * Test fthSignal handling
 */


#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _fthSignalTest
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/signal.h"
#include "platform/stdlib.h"
#include "platform/unistd.h"

#include "fth/fth.h"
#include "fth/fthOpt.h"
#include "fth/fthSignal.h"

#define PLAT_OPTS_ITEMS_fthSignalTest()                                        \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    PLAT_OPTS_FTH()

struct plat_opts_config_fthSignalTest {
    struct plat_shmem_config shmem;
    int timeout;
};

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_FTH_TEST, "signal");

static int signal_received = 0;

static void 
handle_signal_fth(int signum) {
    plat_log_msg(20881, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "Received signal %d", signum);
    signal_received = signum;

    fthSignalShutdown();
    fthKill(1);
}

int
main(int argc, char **argv) {
    int ret = 0;
    int shmem_attached = 0;
    struct plat_opts_config_fthSignalTest config;
    int status;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);
   
    if (plat_opts_parse_fthSignalTest(&config, argc, argv)) {
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
        fthInit();
        fthSignalInit();

        fthSignal(SIGABRT, &handle_signal_fth);

        plat_kill(getpid(), SIGABRT);

        fthSchedulerPthread(0);

        plat_assert_always(signal_received == SIGABRT);
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
