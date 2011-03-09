/*
 * File:   logtest.c
 * Author: drew
 *
 * Created on January 26, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: logtest.c 13365 2010-05-01 06:24:09Z drew $
 */

/*
 * Trivial test for logging system.
 */

#include "platform/assert.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _logtest
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/unistd.h"
#include "platform/wait.h"

/* Command line arguments */
#define PLAT_OPTS_ITEMS_logtest() \
    PLAT_OPTS_SHMEM(shmem)

struct plat_opts_config_logtest {
    struct plat_shmem_config shmem;
};

PLAT_LOG_CAT_LOCAL(CAT_LOCAL, "platform/test/logtest");

int
main(int argc, char **argv) {
    struct plat_opts_config_logtest config;
    int tmp;
    int log_cat;
    int status;
    pid_t pid;
    pid_t wait_pid;
    int ret;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);

    if (plat_opts_parse_logtest(&config, argc, argv)) {
        return (2);
    }

    status = plat_shmem_prototype_init(&config.shmem);
    plat_assert_always(!status);

    status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
    plat_assert_always(!status);

    tmp =  ffdc_initialize(0, BLD_VERSION, 1024 * 1024);
    plat_assert_always(!tmp);

    plat_log_msg(21763, PLAT_LOG_CAT_PLATFORM_TEST, PLAT_LOG_LEVEL_DEBUG,
                 "supressed - default");
    tmp = plat_log_enabled(PLAT_LOG_CAT_PLATFORM_TEST,
                           PLAT_LOG_LEVEL_DEBUG);
    plat_assert_always(!tmp);


    /*
     * By default diagnostic logging is disabled.  See that increasing
     * default priority allows our test category to output.
     */
    tmp = plat_log_parse_arg("default=diagnostic");
    plat_assert_always(!tmp);
    plat_log_msg(21764, PLAT_LOG_CAT_PLATFORM_TEST,
                 PLAT_LOG_LEVEL_DIAGNOSTIC, "Diagnostic %d %d %d", 1, 2, 3);
    tmp = plat_log_enabled(PLAT_LOG_CAT_PLATFORM_TEST,
                           PLAT_LOG_LEVEL_DIAGNOSTIC);
    plat_assert_always(tmp);

    /*
     * Go broader and increase logging level.
     */
    tmp = plat_log_parse_arg("platform=debug");
    plat_assert_always(!tmp);
    plat_log_msg(21765, PLAT_LOG_CAT_PLATFORM_TEST,
                 PLAT_LOG_LEVEL_DEBUG, "Debug %s", "this");
    tmp = plat_log_enabled(PLAT_LOG_CAT_PLATFORM_TEST, PLAT_LOG_LEVEL_DEBUG);
    plat_assert_always(tmp);

    /*
     * Add dynamic log level.
     */
    log_cat = plat_log_add_category("platform/strange/bizarre");
    plat_assert_always(log_cat > 0);
    tmp = plat_log_enabled(log_cat, PLAT_LOG_LEVEL_DIAGNOSTIC);
    plat_assert_always(tmp);

    /*
     * Check statically constructed local log level under platform/test
     */
    plat_assert_always(CAT_LOCAL > 0);
    tmp = plat_log_enabled(CAT_LOCAL, PLAT_LOG_LEVEL_DIAGNOSTIC);
    plat_assert_always(tmp > 0);

    char long_string[100];
    plat_assert_always(sizeof (long_string) > FFDC_MAX_STR_SIZE);

    memset (long_string, 's', 50);
    memset (long_string + 50, 'L', 50);
    long_string[99] = 0; 

    plat_log_msg(21790, PLAT_LOG_CAT_PLATFORM_TEST,
                 PLAT_LOG_LEVEL_INFO, 
                 "short string %s long string %"FFDC_LONG_STRING(100)"",
                 long_string, long_string);

    tmp = plat_log_parse_arg("platform{,/strange,/test}=debug");
    plat_assert_always(!tmp);

    plat_log_usage();

    pid = plat_fork();
    plat_assert_always(pid != -1);
    if (pid == 0) {
        plat_log_msg(21793, PLAT_LOG_CAT_PLATFORM_TEST,
                     PLAT_LOG_LEVEL_INFO, "in child process");
        ret = 0;
    }  else {
        do {
            wait_pid = plat_waitpid(pid, &status, 0 /* options */);
        } while (wait_pid == -1 && errno == EINTR);
        ret = status;
    }

    return (ret);
}

#include "platform/opts_c.h"
