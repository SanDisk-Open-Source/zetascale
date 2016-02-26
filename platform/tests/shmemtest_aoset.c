/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/platform/tests/shmemtest_aoset.c
 * Author: gshaw
 *
 * Created on July 28, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: $
 */

/*
 * Test aoset.
 *
 * 1. Test aoset with a somewhat large number of keys.
 *
 *    It would be difficult to generate a large set of unique stack backtraces,
 *    so we test aoset on its own with other kinds of data.
 *
 * 2. Test with known sequences of values.
 *
 *    Generating known sequences of values allows us to test for conditions
 *    that are not normally things we can rely on.  For example, if we add 1..n,
 *    in order, we can expect that blob[i-1] = i.  That is not guaranteed by
 *    the API, but we are in the know, we should be able to expect that.
 *    Also, we can do some known-answer tests (KAT).
 */

#include <limits.h>
#include <inttypes.h>           // Import PRI... format strings

#include "misc/misc.h"

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _shmemtest_aoset
#include "platform/opts.h"
#include "platform/shmem.h"
// XXX #include "platform/shmem_debug.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/unistd.h"
#include "platform/aoset.h"

#define eprintf(fmt, ...) \
    fprintf(stderr, fmt, ## __VA_ARGS__)

#define dprintf(fmt, ...) \
    ({ if (opt_debug) { fprintf(stderr, fmt, ## __VA_ARGS__); }; })

/* Command line arguments */
#define PLAT_OPTS_ITEMS_shmemtest_aoset()                                      \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    item("debug", "enable debugging", DEBUG,                                   \
         ({ opt_debug = 1; 0; }), PLAT_OPTS_ARG_NO)

struct plat_opts_config_shmemtest_aoset {
    struct plat_shmem_config shmem;
    int opt_debug;
};

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_TEST, "shmemtest_aoset");

static void test_aoset(void);

/* Enable debugging */
static int opt_debug = 0;

int
main(int argc, char **argv) {
    struct plat_opts_config_shmemtest_aoset config;
    struct plat_shmem_alloc_stats init_stats;
    struct plat_shmem_alloc_stats end_stats;
    int stats_balance;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);

    int status;

    if (plat_opts_parse_shmemtest_aoset(&config, argc, argv)) {
        return (2);
    }

    if (opt_debug) {
        plat_shmem_config_set_flags(&config.shmem,
                                    PLAT_SHMEM_CONFIG_DEBUG_ALLOC);
    }

    status = plat_shmem_prototype_init(&config.shmem);
    plat_assert_always(!status);

    status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
    plat_assert_always(!status);

    status = plat_shmem_alloc_get_stats(&init_stats);
    plat_assert_always(!status);

    plat_shmem_pthread_started();

    test_aoset();

    plat_shmem_pthread_done();

    status = plat_shmem_alloc_get_stats(&end_stats);
    plat_assert_always(!status);

    /*
     * The tests should clean up after themselves, leaving no left-over
     * allocated objects.
     *
     * Complain if the bytes allocated or object count differ,
     * but do not bail out immediately; rather, continue with the
     * shutdown sequence, allowing other possible assertion failures,
     * independent of the accounting discrepency.
     */
    stats_balance = 1;
    if (!(init_stats.allocated_bytes == end_stats.allocated_bytes)) {
        eprintf("Expected "
            "init_stats.allocated_bytes == end_stats.allocated_bytes.\n");
        eprintf("  init_stats.allocated_bytes = %" PRIu64 "\n",
            init_stats.allocated_bytes);
        eprintf("  end_stats.allocated_bytes  = %" PRIu64 "\n",
            end_stats.allocated_bytes);
        stats_balance = 0;
    }
    if (!(init_stats.allocated_count == end_stats.allocated_count)) {
        eprintf("Expected "
            "init_stats.allocated_count == end_stats.allocated_count.\n");
        eprintf("  init_stats.allocated_count = %" PRIu64 "\n",
            init_stats.allocated_count);
        eprintf("  end_stats.allocated_count  = %" PRIu64 "\n",
            end_stats.allocated_count);
        stats_balance = 0;
    }

    status = plat_shmem_detach();
    plat_assert_always(!status);

    plat_shmem_config_destroy(&config.shmem);
    plat_assert_always(stats_balance);

    return (0);
}

static void
test_aoset(void)
{
    aoset_hdl_t s;
    uint_t i;
    int ret;
    const uint_t limit = 1000;

    eprintf("<test_aoset>\n");

    /*
     * Add simple unsigned integers, from 1 .. limit, in order.
     */
    s = aoset_create();
    for (i = 1; i <= limit; ++i) {
        ret = aoset_add(s, &i, sizeof (i));
        if (ret < 0) {
            eprintf("aoset_add %u failed; ret = %d\n", i, ret);
            break;
        }
    }

    /*
     * Add 1 .. limit, again, this time in descending order.
     * All the values should already be in the set.
     */
    for (i = limit; i >= 1; --i) {
        ret = aoset_add(s, &i, sizeof (i));
        if (ret < 0) {
            eprintf("aoset_add %u failed; ret = %d\n", i, ret);
            break;
        }
        if (ret > limit) {
            eprintf("aoset_add: index exceeds limit of %u; index = %u\n",
                limit, ret);
        }
    }

    /*
     * Read out all the elements of the set, in order.
     * Since they were insert in order, the values (1..limit)
     * should correspond to their indices in the set (0..limit-1).
     */
    for (i = 1; i <= limit; ++i) {
        gvptr_t objhdl;
        size_t objsize;
        uint_t *objintref;

        ret = aoset_get(s, i - 1, &objhdl, &objsize);
        if (ret < 0) {
            eprintf("aoset_get %u failed; ret = %d\n", i, ret);
            break;
        }
        if (objsize != sizeof (i)) {
            eprintf("aoset_get: bad object size; expect %lu, got %lu",
                sizeof (i), objsize);
            break;
        }
        objintref = (uint_t *)GVPTR_REF(objhdl);
        if (*objintref != i) {
            eprintf("aoset_get: expect blob[%u] = %u, got %u\n",
                i - 1, i, *objintref);
            break;
        }
    }

    if (opt_debug) {
        aoset_diag(s);
    }
    ret = aoset_destroy(s);
    if (ret < 0) {
        eprintf("aoset_destroy failed; ret = %d\n", ret);
    }
    eprintf("</test_aoset>\n");
}

#include "platform/opts_c.h"
