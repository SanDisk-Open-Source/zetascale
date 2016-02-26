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
 * File:   sdf/platform/tests/shmemtest_plat_alloc.c
 * Author: drew
 *
 * Created on September 4, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmemtest_plat_alloc.c 3240 2008-09-05 01:50:35Z drew $
 */

/**
 * Replace malloc with shmem alloc and verify no smoke comes out
 */

#include <limits.h>

#include "platform/assert.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _test
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/stdlib.h"
#include "platform/string.h"

/* Command line arguments */
#define PLAT_OPTS_ITEMS_test()                                                 \
    PLAT_OPTS_SHMEM(shmem)

struct plat_opts_config_test {
    struct plat_shmem_config shmem;
};

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_TEST,
                      "shmemtest_plat_alloc");

static const char test_string[] = "testing";
static const char more[] = " 1.2.3";

int
main(int argc, char **argv) {
    struct plat_opts_config_test config;
    int status;
    struct plat_shmem_alloc_stats init_stats;
    struct plat_shmem_alloc_stats end_stats;
    char *buffer; 
    int i;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);

    if (plat_opts_parse_test(&config, argc, argv)) {
        return (2);
    }

   plat_shmem_config_set_flags(&config.shmem,
                               PLAT_SHMEM_CONFIG_DEBUG_REPLACE_MALLOC);

    status = plat_shmem_prototype_init(&config.shmem);
    plat_assert_always(!status);

    status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
    plat_assert_always(!status);

    status = plat_shmem_alloc_get_stats(&init_stats);
    plat_assert_always(!status);

    buffer = plat_alloc(sizeof (test_string));
    status = plat_shmem_alloc_get_stats(&end_stats);
    plat_assert_always(!status);
    plat_assert_always(end_stats.allocated_bytes > init_stats.allocated_bytes);
    plat_assert_always(end_stats.allocated_count > init_stats.allocated_count);

    strcpy(buffer, test_string);
    buffer = plat_realloc(buffer, sizeof (test_string) + sizeof (more));
    plat_assert_always(buffer);
    plat_assert_always(!strcmp(buffer, test_string));

    strcat(buffer, more);

    plat_free(buffer);

    buffer = plat_calloc(1, 10);
    for (i = 0; i < 10; ++i) {
        plat_assert_always(!buffer[i]);
    }
    plat_free(buffer);

    status = plat_shmem_alloc_get_stats(&end_stats);
    plat_assert_always(!status);
    plat_assert_always(init_stats.allocated_bytes == end_stats.allocated_bytes);
    plat_assert_always(init_stats.allocated_count == end_stats.allocated_count);

    status = plat_shmem_detach();
    plat_assert_always(!status);

    plat_shmem_config_destroy(&config.shmem);

    return (0);
}

#include "platform/opts_c.h"
