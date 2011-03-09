/*
 * File:   sdf/platform/opts.h
 * Author: drew
 *
 * Created on February 2, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: opts.h 12469 2010-03-24 04:29:31Z drew $
 */

/**
 * Generic command line processing
 *
 * Usage:
 *
 * @code
 * #define PLAT_OPTS_NAME(name) name ## _myapp
 * #include "platform/opts.h"
 *
 * #define PLAT_OPTS_LOG_CAT_myapp PLAT_LOG_CAT_myapp
 *
 * // Command line arguments
 * //     #define item(opt, desc, caps, parse, required)
 * // where
 * //     opt is a 'C' string describing the option
 * //     arg is a  description of the argument or its argument
 * //     caps is an all-caps version of the option for enum definition
 * //     parse is a lambda function to parse it (using config and optarg
 * //         as provided to PLAT_OPTS_ITEMS())
 * //     required is one of PLAT_OPTS_ARG_REQUIRED, PLAT_OPTS_ARG_NO, or
 * //         PLAT_OPTS_ARG_OPTIONAL indicating whether
 * //         this option takes an argument.
 *
 *
 * #define PLAT_OPTS_ITEMS_myapp()
 *     item("file", "file to operate on", FILE,                                \
 *          parse_string_alloc(&config->file, optarg, PATH_MAX),               \
 *          PLAT_OPTS_ARG_REQUIRED)                                            \
 *     item("iterations", "number of iterations", ITERATIONS,                  \
 *          parse_int(&config->iterations, optarg, NULL),
 *          PLAT_OPTS_ARG_REQUIRED)
 *
 * struct plat_opts_config_myapp {
 *    char *file;
 *    int iterations;
 * };
 *
 * int
 * main(int argc, char **argv, char **envp) {
 *     int ret = 0;
 *     struct plat_otps_config_myapp config;
 *
 *     memset (&config, 0, sizeof (config));
 *     ret = plat_opts_parse_my_app(&config, argc, argv);
 *     if (!ret && !config.exe) {
 *         plat_log_msg(20962, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
 *             "exe argument required");
 *         plat_opts_usage_myapp();
 *         ret = 1;
 *     }
 *
 *     if (!ret) {
 *         // Real stuff here!
 *     }
 *
 * #include "platform/opts_c.h"
 * @endcode
 *
 * The link line must include libmisc (before libplatform).
 *
 * XXX: This is getting out of hand.  While things (fth, shmem, etc)
 * should remain separate conceptually they need to end up in fewer
 * libraries.
 *
 * Short options may be processed by definining
 *    PLAT_OPTS_OPTSTRING
 * string for the getopt optstring argument, and providing
 * int PLAT_OPTS_NAME(plat_opts_short) with an argument list of
 *
 *     (struct PLAT_OPTS_NAME(plat_opts_config), int c)
 *
 * when there is a config type; or just the character.  This shall return
 * 0 on success, non-zero on error.
 *
 * Defining PLAT_OPTS_NESTED will suppress the extern/static definitions and
 * allow easy merging of our code and theirs (memcached).
 */

#ifdef PLAT_OPTS_SIMPLE
#error "opts.h may be included once with the default options"
#endif

// If user doesn't define the config, they get the simplest possible
// implementation.
#ifndef PLAT_OPTS_NAME
#define PLAT_OPTS_NAME(foo) foo
#define PLAT_OPTS_STATIC static
#define PLAT_OPTS_NO_CONFIG
#define PLAT_OPTS_SIMPLE
#define PLAT_OPTS_ITEMS()
#endif

#ifndef PLAT_OPTS_STATIC
#define PLAT_OPTS_STATIC
#endif

/* Common part */

#ifndef PLATFORM_OPTS_H
#define PLATFORM_OPTS_H 1
/* For CHAR_MAX */
#include <limits.h>
#ifndef _GNU_SOURCE
#define UNGNU
#define _GNU_SOURCE
#endif
/* For getopt_long */
#include <getopt.h>
#ifdef UNGNU
#undef UNGNU
#undef _GNU_SOURCE
#endif

#include "platform/logging.h"
#include "platform/platform.h"
#include "platform/stdio.h"
#include "platform/string.h"

#include "misc/misc.h"

/*
 * XXX: drew 2010-03-23 The separate include is just a hack for 
 * making the composable options macros work in memcached.c
 * 
 * If we ever re-do a reasonable configuration, we should get rid of 
 * the separate enums.
 *
 * We leave the originals here for reference, but always compile against
 * the same version in case there's drift.
 */
#ifdef notyet
#include "platform/opts_enum.h"
#else 
/** @brief option options */
enum plat_opts_opt_options {
    /** @brief Option takes no argument (default) */
    PLAT_OPTS_ARG_NO = 0,
    /** @brief Option requires an argument */
    PLAT_OPTS_ARG_REQUIRED = 1,
    /** @brief Option has optional argument (default) */
    PLAT_OPTS_ARG_OPTIONAL = 2,

    PLAT_OPTS_ARG_BITS = 0x3,

    /** @brief Option is optional (default) */
    PLAT_OPTS_OPT_OPTIONAL = 0,
    /** @brief Option is required */
    PLAT_OPTS_OPT_REQUIRED = 4,

    PLAT_OPTS_OPT_BITS = 0x4
};
#endif

#define PLAT_OPTS_ARG_NO no_argument
#define PLAT_OPTS_ARG_REQUIRED required_argument
#define PLAT_OPTS_ARG_OPTIONAL optional_argument

/* Common log options available everywhere */
#define PLAT_OPTS_COMMON_ITEMS()                                               \
    item("log", "log category=log level", LOG,                                 \
         plat_log_parse_arg(optarg), PLAT_OPTS_ARG_REQUIRED)                   \
    item("log_file", "filename for logging/stderr", LOG_FILE,                  \
         plat_log_set_file(optarg, PLAT_LOG_REDIRECT_STDERR),                  \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("log_time_format", "strftime format for log time stamps",             \
         LOG_TIME_FORMAT, plat_log_set_time_format(optarg),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("log_time_secs", "log seconds since epoch", LOG_TIME_SECS,            \
         plat_log_set_time_format_secs(), PLAT_OPTS_ARG_NO)                    \
    item("log_time_date", "log time in date(1) format ", LOG_TIME_DATE,        \
         plat_log_set_time_format_bin_date(), PLAT_OPTS_ARG_NO)                \
    item("log_time_relative_secs", "log seconds since program start",          \
         LOG_TIME_RELATIVE_SECS, plat_log_set_time_format_relative_secs(),     \
         PLAT_OPTS_ARG_NO)                                                     \
    item("log_time_relative_secs_float",                                       \
         "log fractional seconds since program start",                         \
         LOG_TIME_RELATIVE_SECS_FLOAT,                                         \
         plat_log_set_time_format_relative_secs_float(), PLAT_OPTS_ARG_NO)     \
    item("log_time_relative_hms", "log H:M:S since program start",             \
         LOG_TIME_RELATIVE_HMS, plat_log_set_time_format_relative_hms(),       \
         PLAT_OPTS_ARG_NO)                                                     \
    item("log_time_relative_hms_float",                                        \
         "log fractional H:M:S since program start",                           \
         LOG_TIME_RELATIVE_HMS_FLOAT,                                          \
         plat_log_set_time_format_relative_hms_float(), PLAT_OPTS_ARG_NO)      \
    item("log_time_none", "disable time logging", LOG_TIME_NONE,               \
         plat_log_set_time_format(NULL), PLAT_OPTS_ARG_NO)                     \
    item("seed", "seed PRNG", SEED,                                            \
         seed_arg(), PLAT_OPTS_ARG_NO)                                         \
    item("reseed", "reseed value from seed", RESEED,                           \
         parse_reseed(optarg), PLAT_OPTS_ARG_NO)                               \
    item("stop", "stop program to await debugger attachment", STOP,            \
         stop_arg(), PLAT_OPTS_ARG_NO)                                         \
    item("tmp", "temporary path", TMP,                                         \
         plat_set_tmp_path(optarg), PLAT_OPTS_ARG_REQUIRED)                    \
    item("nop", "no op (test harness can grep for argument )", NOP,            \
         0, PLAT_OPTS_ARG_REQUIRED)                                            \
    item("timeout", "kill process with SIGALRM in x seconds", TIMEOUT,         \
         parse_timeout(optarg), PLAT_OPTS_ARG_REQUIRED)

/* Print usage message for a single option */
static void
plat_opts_usage_one(const char *opt, const char *description, int required) {
    const char *before_opt;
    const char *after_opt;

    const char *before_arg;
    const char *after_arg;

    switch (required & PLAT_OPTS_OPT_BITS) {
    case PLAT_OPTS_OPT_REQUIRED:
        before_opt = "";
        after_opt = "";
        break;
    case PLAT_OPTS_OPT_OPTIONAL:
        before_opt = "[";
        after_opt = "]";
        break;
    default:
        plat_assert(0);
    }

    switch (required & PLAT_OPTS_ARG_BITS) {
    case PLAT_OPTS_ARG_REQUIRED:
        before_arg = "<";
        after_arg = ">";
        break;
    case PLAT_OPTS_ARG_OPTIONAL:
        before_arg = "[";
        after_arg = "]";
        break;
    case PLAT_OPTS_ARG_NO:
        before_arg = "";
        after_arg = "";
        break;
    default:
        plat_assert(0);
    }

    fprintf(stderr, "\t%s--%s %s%s%s%s\n",
            before_opt, opt,  before_arg, description, after_arg, after_opt);
}
#endif /* ndef PLATFORM_OPTS_H */

struct PLAT_OPTS_NAME(plat_opts_config);

#undef PLAT_OPTS_CONFIG
#ifndef PLAT_OPTS_NO_CONFIG
#define PLAT_OPTS_CONFIG struct PLAT_OPTS_NAME(plat_opts_config) *config,
#define PLAT_OPTS_CONFIG_PASS config,
#else
#define PLAT_OPTS_CONFIG
#define PLAT_OPTS_CONFIG_PASS
#endif

#ifndef PLAT_OPTS_NESTED
PLAT_OPTS_STATIC int
PLAT_OPTS_NAME(plat_opts_parse)(PLAT_OPTS_CONFIG int argc, char **argv);

PLAT_OPTS_STATIC __attribute__((unused)) int
PLAT_OPTS_NAME(plat_opts_parse_strip)(PLAT_OPTS_CONFIG int *argc, char **argv);
#endif

PLAT_OPTS_STATIC void PLAT_OPTS_NAME(plat_opts_usage)(void);

#ifdef PLAT_OPTS_SIMPLE
#include "platform/opts_c.h"
#endif
