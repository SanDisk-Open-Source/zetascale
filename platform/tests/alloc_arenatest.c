/*
 * File:   sdf/platform/tests/alloc_arenatest.c
 * Author: drew
 *
 * Created on December 3, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: alloc_arenatest.c 10648 2009-12-17 21:52:37Z drew $
 */

/**
 * Validate that arena based memory allocation is working correctly
 * so that we can give the cache code its own arena where it can
 * get out of memory.
 */

#include <limits.h>

#include "misc/misc.h"

#include "platform/alloc.h"
#include "platform/assert.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _alloc_arenatest
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/stats.h"
#include "platform/stdlib.h"
#include "platform/string.h"

/* Command line arguments */
#define PLAT_OPTS_ITEMS_alloc_arenatest() \
    PLAT_OPTS_SHMEM(shmem)

struct plat_opts_config_alloc_arenatest {
    struct plat_shmem_config shmem;
};

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_TEST, "alloc_arenatest");

int
main(int argc, char **argv) {
    struct plat_opts_config_alloc_arenatest config;
    int status;
    struct plat_shmem_alloc_stats init_stats;
    struct plat_shmem_alloc_stats end_stats;
    void *root_early;
    void *root_late;
    void *test_good;
    void *test_fail;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);

    plat_shmem_config_set_arena_used_limit(&config.shmem,
                                           PLAT_SHMEM_ARENA_TEST,
                                           1024 /* just 1K for test */);

    plat_shmem_config_set_flags(&config.shmem,
                                PLAT_SHMEM_CONFIG_DEBUG_REPLACE_MALLOC);

    if (plat_opts_parse_alloc_arenatest(&config, argc, argv)) {
        return (2);
    }

    status = plat_shmem_prototype_init(&config.shmem);
    plat_assert_always(!status);

    status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
    plat_assert_always(!status);

    status = plat_shmem_alloc_get_stats(&init_stats);
    plat_assert_always(!status);

    plat_shmem_pthread_started();

    root_early = plat_alloc(1024);
    plat_assert_always(root_early);

    /* Validate that space is coming from shmem */
    status = plat_shmem_alloc_get_stats(&end_stats);
    plat_assert_always(!status);
    plat_assert_always(init_stats.allocated_bytes != end_stats.allocated_bytes);
    plat_assert_always(init_stats.allocated_count != end_stats.allocated_count);

    /* Validate sub-arena works */
    test_good = plat_alloc_arena(100, PLAT_SHMEM_ARENA_TEST);
    plat_assert_always(test_good);

    /* But limit specified above is enforced */
    test_fail = plat_alloc_arena(1024, PLAT_SHMEM_ARENA_TEST);
    plat_assert_always(!test_fail);

    /* That root still works */
    root_late = plat_alloc(1024);
    plat_assert_always(root_late);

    /* That free into the arena still works */
    plat_free(test_good);
    test_good = plat_alloc_arena(100, PLAT_SHMEM_ARENA_TEST);
    plat_assert_always(test_good);

    /* Validate that stat grabbing doesn't core dump */
    plat_stat_log();

    char *buf = plat_stat_str_get_alloc("test ", NULL /* default suffix */);
    plat_assert(buf);
    plat_log_msg(20819, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "%s", buf);
    plat_stat_str_get_free(buf);

    plat_free(test_good);
    plat_free(root_late);
    plat_free(root_early);

    plat_shmem_pthread_done();

    status = plat_shmem_alloc_get_stats(&end_stats);
    plat_assert_always(!status);
    plat_assert_always(init_stats.allocated_bytes == end_stats.allocated_bytes);
    plat_assert_always(init_stats.allocated_count == end_stats.allocated_count);

    buf = plat_alloc_steal_from_heap(100);
    plat_assert_always(buf);
    plat_free(buf);

    status = plat_shmem_detach();
    plat_assert_always(!status);

    plat_shmem_config_destroy(&config.shmem);

    return (0);
}

#include "platform/opts_c.h"
