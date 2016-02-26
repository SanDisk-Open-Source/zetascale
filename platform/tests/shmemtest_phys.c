/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/platform/tests/shmemtest_phys.c
 * Author: drew
 *
 * Created on July 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmemtest_phys.c 2532 2008-07-31 22:39:08Z drew $
 */

/**
 * Trivial test for physical memory pointers.  Should add logic
 * which does enough load to get us across the segment barrier
 * and import the physical addresses from the kernel to cross-check.
 */

#include <limits.h>

#include "misc/misc.h"

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _shmemtest_phys
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/unistd.h"

#undef min
#define min(a, b) ((a) <= (b) ? (a) : (b))

/* Command line arguments */
#define PLAT_OPTS_ITEMS_shmemtest_phys()                                       \
    PLAT_OPTS_SHMEM(shmem)

struct plat_opts_config_shmemtest_phys {
    struct plat_shmem_config shmem;
};

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_TEST, "shmemtest_phys");

struct text;
PLAT_SP_VAR_OPAQUE(text_sp, struct text);

struct text {
    int data_len;
    char data[];
};

PLAT_SP_VAR_OPAQUE_IMPL(text_sp, struct text);

static text_sp_t test_start(const char *data);
static void test_middle(text_sp_t shared_text, const char *data, size_t paddr);
static void test_end(text_sp_t shared_text, const char *text);

int
main(int argc, char **argv) {
    struct plat_opts_config_shmemtest_phys config;
    int status;
    size_t paddr_before;
    struct plat_shmem_alloc_stats init_stats;
    struct plat_shmem_alloc_stats end_stats;

    text_sp_t shared_text;

    const char text_of_doom[] = "Killroy was here";

    /* Physmem ininitialization is the same as regular memory */
    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);

    if (plat_opts_parse_shmemtest_phys(&config, argc, argv)) {
        return (2);
    }

    status = plat_shmem_prototype_init(&config.shmem);
    plat_assert_always(!status);

    status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
    plat_assert_always(!status);

    status = plat_shmem_alloc_get_stats(&init_stats);
    plat_assert_always(!status);

    shared_text = test_start(text_of_doom);
    plat_assert_always(!text_sp_is_null(shared_text));
    paddr_before = plat_shmem_ptr_to_paddr(shared_text);
    plat_assert_always(paddr_before);

    test_middle(shared_text, text_of_doom, paddr_before);

    /* Detach and re-attach */
    status = plat_shmem_detach();
    plat_assert_always(!status);

    status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
    plat_assert_always(!status);

    test_middle(shared_text, text_of_doom, paddr_before);

    test_end(shared_text, text_of_doom);

    status = plat_shmem_alloc_get_stats(&end_stats);
    plat_assert_always(!status);
    plat_assert_always(init_stats.allocated_bytes == end_stats.allocated_bytes);
    plat_assert_always(init_stats.allocated_count == end_stats.allocated_count);

    status = plat_shmem_detach();
    plat_assert_always(!status);

    plat_shmem_config_destroy(&config.shmem);

    return (0);
}

/**
 * @brief Start test returning pointer to shared state.
 */

/*
 * XXX - the variable length mechanations stink.  We have the size in the header
 * and might as well use it.  The debugging tools can unprotect the header to get at the size for
 * mapping.
 */
static text_sp_t
test_start(const char *data) {
    text_sp_t shared_text;
    struct text *local_text = NULL;
    size_t data_len;
    size_t size;

    data_len = strlen(data) + 1;
    size = sizeof (*local_text) + data_len;

    /*
     * Physical memory works just like normal memory; only the 
     * allocator is different.
     */
    shared_text = plat_shmem_var_phys_alloc(text_sp, size);
    plat_assert_always(!text_sp_is_null(shared_text));

    text_sp_var_rwref(&local_text, shared_text, size);
    plat_assert_always(local_text);

    local_text->data_len = data_len;
    memcpy(local_text->data, data, data_len);

    text_sp_var_rwrelease(&local_text, size);

    return (shared_text);
}

/** @brief Sanity check that everything is as initialized */
static void
test_middle(text_sp_t shared_text, const char *data, size_t paddr) {
    const struct text *local_text = NULL;
    size_t data_len;
    size_t size;
    size_t current_paddr;
    int status;

    /*
     * #plat_shmem_ptr_to_paddr returns consistent non-null values for
     *  physmem across invocations.
     */
    current_paddr = plat_shmem_ptr_to_paddr(shared_text);
    plat_assert_always(current_paddr == paddr);

    data_len = strlen(data) + 1;
    size = sizeof (*local_text) + data_len;

    text_sp_var_rref(&local_text, shared_text, size);
    plat_assert_always(local_text);

    plat_assert_always(local_text->data_len == data_len);
    status = memcmp(local_text->data, data, local_text->data_len);
    plat_assert_always(!status);

    text_sp_var_rrelease(&local_text, size);
}

/** @brief Cleanup test */
static void
test_end(text_sp_t shared_text, const char *data) {
    size_t data_len;
    size_t size;

    data_len = strlen(data) + 1;
    size = sizeof (struct text) + data_len;
    /* Physical memory free is just like normal shared memory */
    plat_shmem_var_free(text_sp, shared_text, size);
}

#include "platform/opts_c.h"
