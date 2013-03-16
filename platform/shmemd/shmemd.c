/*
 * File:   shmemd.c
 * Author: drew
 *
 * Created on January 24, 2008, 8:00 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmemd.c 10527 2009-12-12 01:55:08Z drew $
 */

/*
 * Shmemd handles shared memory pool growth and cleanup of crash consistent
 * data structures stored in shared memory when it or client processes 
 * terminate abnormally.
 *
 * This is a stub which just handles the initialization part of 
 * the equation.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <getopt.h>
#include <limits.h>
#include <stdio.h>

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/signal.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/time.h"
#include "platform/unistd.h"

#include "misc/misc.h"

/* Logging category */
#define LOG_CAT PLAT_LOG_CAT_PLATFORM_SHMEMD

/* Command line arguments */
#define OPT_ITEMS_WITH_ARG(config)                                             \
    item("log", "log category=log level", LOG,                                 \
        plat_log_parse_arg(optarg))                                            \
    item("mmap", "mmap file path", MMAP,                                       \
        parse_string_alloc(&config->mmap, optarg, PATH_MAX))                   \
    item("size", "size in bytes", SIZE,                                        \
        parse_size(&config->size, optarg, NULL))

struct config {
    /* Filename for backing store */
    char *mmap;
    /* Size of file */
    int64_t size;
};

static sig_atomic_t terminate = 0;

static int parse_args(struct config *config, int argc, char **argv);
static void usage();
static int init(const struct config *config);
static void terminate_handler(int signo);

int
main(int argc, char **argv) {
    struct config config = { NULL, 16 * 1024 * 1024 } ;
    struct timespec second = { 1, 0 };
    struct sigaction terminate_action;
    int tmp;
    struct plat_shmem_alloc_stats alloc_stats;
    int ret = 0;

    memset(&terminate_action, 0, sizeof (terminate_action));
    terminate_action.sa_handler = &terminate_handler;
    terminate_action.sa_flags = 0;

    ret = parse_args(&config, argc, argv);

    if (!ret && !config.mmap) {
        plat_log_msg(21038, LOG_CAT,
            PLAT_LOG_LEVEL_FATAL, "mmap argument required");
        usage();
        ret = 1;
    }

    tmp = plat_sigaction(SIGTERM, &terminate_action, NULL);
    if (tmp == -1) {
        plat_log_msg(21039, LOG_CAT,
            PLAT_LOG_LEVEL_FATAL, "sigaction(SIGTERM) failed: %s",
            plat_strerror(plat_errno));
        ret = 1;
    }

    tmp = plat_sigaction(SIGINT, &terminate_action, NULL);
    if (tmp == -1) {
        plat_log_msg(21040, LOG_CAT,
            PLAT_LOG_LEVEL_FATAL, "sigaction(SIGTINT) failed: %s",
            plat_strerror(plat_errno));
        ret = 1;
    }

    if (!ret) {
        ret = init(&config);
        if (ret) {
            plat_log_msg(21041, LOG_CAT,
                PLAT_LOG_LEVEL_FATAL, "init failed");
        }
    }

    if (!ret) {
        while(!terminate) {
            plat_nanosleep(&second, NULL);
        }

        tmp = plat_shmem_alloc_get_stats(&alloc_stats);
        plat_assert(!tmp);

        plat_log_msg(21042, LOG_CAT,
            PLAT_LOG_LEVEL_DEBUG, "%llu objects totaling %llu bytes in-use",
            (unsigned long long)alloc_stats.allocated_count,
            (unsigned long long)alloc_stats.allocated_bytes);

        plat_shmem_detach();
    }

    return (ret);
}

static int
parse_args(struct config *config, int argc, char **argv) {
    enum {
        OPT_LAST_SHORT = CHAR_MAX,
#define item(opt, arg, caps, parse) OPT_ ## caps,
        OPT_ITEMS_WITH_ARG()
#undef item
    };

    const struct option options[] = {
#define item(opt, arg, caps, parse) \
        { opt, required_argument, NULL, OPT_ ## caps },
        OPT_ITEMS_WITH_ARG(config)
#undef item
        /* terminate with zero-filled option */
        {} };

    int ret = 0;
    int c;

    optind = 1;
    while  ((c = getopt_long(argc, argv, "", options, NULL)) != -1) {
        switch(c) {
#define item(opt, arg, caps, parse) \
        case OPT_ ## caps: if (parse) { ret = 1; } break;
        OPT_ITEMS_WITH_ARG(config)
#undef item
        case '?':
            ret = 1;
        }
    }

    if (ret) {
        usage();
    }

    return (ret);
}

static void
usage() {
    fprintf(stderr, "usage:\n"
#define item(opt, arg, caps, parse) "\t--" opt " [" arg "]\n"
        OPT_ITEMS_WITH_ARG()
#undef item
        "");
    plat_log_usage();
}

/*
 * Initialize shared memory subsystems, including base functionality and
 * allocator.  Returns 0 on success and 1 on failure.
 */
static int
init(const struct config *config) {
    int ret = 0;
    int fd = -1;
    int tmp;

    /* FIXME: replace with real shmem goo; probably exec of shmemd */
    tmp = plat_unlink(config->mmap);
    if (tmp && plat_errno != ENOENT) {
        plat_log_msg(21012, LOG_CAT,
            PLAT_LOG_LEVEL_FATAL, "ulink(%s) failed: %s", config->mmap,
            plat_strerror(plat_errno));
        ret = 1;
    }

    if (!ret) {
        fd = plat_creat(config->mmap, 0644);
        if (fd == -1) {
            plat_log_msg(21043, LOG_CAT,
                PLAT_LOG_LEVEL_FATAL, "creat(%s) failed: %s", config->mmap,
                plat_strerror(plat_errno));
            ret = 1;
        }
    }

    if (!ret && plat_ftruncate(fd, (off_t)config->size) == -1) {
        plat_log_msg(21013, LOG_CAT,
            PLAT_LOG_LEVEL_FATAL, "truncate(%s, %lld) failed: %s",
            config->mmap, (long long) config->size, plat_strerror(plat_errno));
        ret = 1;
    }

    if (fd != -1) {
        plat_close(fd);
    }

    if (!ret) {
        tmp = plat_shmem_attach(config->mmap);
        if (tmp) {
            plat_log_msg(20073, LOG_CAT,
                PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                config->mmap, plat_strerror(-tmp));
            ret = 1;
        }
    }

    return (ret);
}

void
terminate_handler(int signo) {
    terminate = 1;
}
