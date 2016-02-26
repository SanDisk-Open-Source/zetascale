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
 * File:   sdf/platform/tests/shmem_oneproc.c
 * Author: drew
 *
 * Created on January 26, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmemtest_one.c 3447 2008-09-17 10:52:18Z drew $
 */

/**
 * Trivial test for shared memory pointers which mostly just sanity
 * checks that the macros are correct and provides a usage example.
 */

#include <limits.h>

#include "misc/misc.h"

#include "platform/assert.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _shmemtest_one
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/stdlib.h"
#include "platform/string.h"

#undef min
#define min(a, b) ((a) <= (b) ? (a) : (b))

/* Command line arguments */
#define PLAT_OPTS_ITEMS_shmemtest_one()                                        \
    PLAT_OPTS_SHMEM(shmem)

struct plat_opts_config_shmemtest_one {
    struct plat_shmem_config shmem;
};

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_TEST, "shmemtest_one");

/*
 * Declare pointer.  Can be declared at most once, just like other types.  The
 * referenced type does not need to be known.  For platform things declarations
 * belong in "platform/shmem_ptrs.h"
 */
struct node;
PLAT_SP(node_sp, struct node);

struct node {
    int value;
    node_sp_t next;
};

/*
 * Implement pointer.  For platform level things this belongs in
 * platform/shmem_ptrs.c.  sizeof(kind type_name) must be known.
 */
PLAT_SP_IMPL(node_sp, struct node)

PLAT_SP_VAR_OPAQUE(my_void_sp, void);
PLAT_SP_VAR_OPAQUE_IMPL(my_void_sp, void);

static void test_list();
static void test_array();
static void test_void();

int
main(int argc, char **argv) {
    struct plat_opts_config_shmemtest_one config;
    int status;
    struct plat_shmem_alloc_stats init_stats;
    struct plat_shmem_alloc_stats end_stats;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);

    if (plat_opts_parse_shmemtest_one(&config, argc, argv)) {
        return (2);
    }

    status = plat_shmem_prototype_init(&config.shmem);
    plat_assert_always(!status);

    status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
    plat_assert_always(!status);

    plat_assert_always(node_sp_is_null(node_sp_null));

    status = plat_shmem_alloc_get_stats(&init_stats);
    plat_assert_always(!status);

    plat_shmem_pthread_started();

    test_list();

    plat_shmem_pthread_done();

    /* Detach and re-attach */
    status = plat_shmem_detach();
    plat_assert_always(!status);

    status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
    plat_assert_always(!status);

    plat_shmem_pthread_started();

    test_array();
    test_void();

    plat_shmem_pthread_done();

    status = plat_shmem_alloc_get_stats(&end_stats);
    plat_assert_always(!status);
    plat_assert_always(init_stats.allocated_bytes == end_stats.allocated_bytes);
    plat_assert_always(init_stats.allocated_count == end_stats.allocated_count);

    status = plat_shmem_detach();
    plat_assert_always(!status);

    plat_shmem_config_destroy(&config.shmem);

    return (0);
}

static void
test_list() {
    int i;
    struct node *current;
    const struct node *current_read;
    node_sp_t head, second;

    /* Allocate (internal, deprecated syntax) */
    head = plat_shmem_alloc(node_sp);
    plat_assert_always(!node_sp_is_null(head));
    current = NULL;
    /* Get local read-write reference(dest, src) */
    node_sp_rwref(&current, head);
    current->value = 1;

    /* Allocate */
    second = plat_shmem_alloc(node_sp);
    current->next = second;
    plat_assert_always(!node_sp_is_null(current->next));

    /* Reference replacement implies release */
    node_sp_rwref(&current, second);
    current->value = 2;
    current->next = node_sp_null;

    /*
     * References must be explicitly rwreleased so that we can detect
     * leaks, unmap things that shouldn't be touched in debugging
     * environments, etc.
     */
    node_sp_rwrelease(&current);

    /*
     * Get read-only reference.  Since the type signature is
     * node_sp_rref(const struct node **local, node_sp_t shared)
     * this will fail at compile time if the user tries to create
     * a non-const reference.  The read-only reference returned can
     * also point into a read-only mapping.
     */
    node_sp_rref(&current_read, head);
    for (i = 1; current_read && i <= 2; ++i) {
        plat_assert_always(current_read->value == i);
        node_sp_rref(&current_read, current_read->next);
    }
#ifdef PLAT_SHMEM_DEBUG
    plat_assert_always(!current);
#endif
    plat_assert_always(i == 3);

    node_sp_rref(&current_read, head);
    /* Check const-correctness of rrelease */
    node_sp_rrelease(&current_read);

    /* Free shared memory */
    plat_shmem_free(node_sp, head);
    plat_shmem_free(node_sp, second);
}

static void
test_array() {
    const int size = 10;
    int i;
    node_sp_t shared_array;
    struct node *first;

    /* Allocate array with size elements */
    shared_array = plat_shmem_array_alloc(node_sp, size);
    plat_assert_always(!node_sp_is_null(shared_array));
    node_sp_rwref(&first, shared_array);
    plat_assert_always(first);

    /*
     * FIXME: Need to do sub-object references in a way that allows
     * for partial object mapping in debug environments; with
     * node_array_sub_rwref(struct node **local, node_array_sp_t shared,
     * size_t index)
     *
     * For arrays which are not referenceable in pieces containing
     * more than one element (lots of things -
     * hash tables, heaps for priority queues, etc) the syntax even
     * allows objects to be padded out to page boundaries.
     */
    for (i = 0; i < size; ++i) {
        first[i].value = i;
    }

    node_sp_rwrelease(&first);

    /* Free array with size elements */
    plat_shmem_array_free(node_sp, shared_array, size);
}

static void
test_void() {
    const size_t size = 100;
    void *local_void_rw = NULL;
    const void *local_void_ro = NULL;
    my_void_sp_t shared_void = my_void_sp_null;
    const char test[] = "The quick brown fox jumped over the lazy dog.";

    shared_void = plat_shmem_var_alloc(my_void_sp, size);
    plat_assert_always(!my_void_sp_is_null(shared_void));

    my_void_sp_var_rwref(&local_void_rw, shared_void, size);
    plat_assert_always(local_void_rw);

    memset(local_void_rw, 0, size);

    strncpy(local_void_rw, test, min(size, sizeof (test)));
    /*
     * Caution: GCC will implicitly convert a void * to a void ** so
     * a missing & is a silent failure.
     */
    my_void_sp_var_rwrelease(&local_void_rw, size);

    my_void_sp_var_rref(&local_void_ro, shared_void, size);
    plat_assert_always(local_void_ro);
    plat_assert_always(strncmp(local_void_ro, test, min(size, sizeof (test))) == 0);
    my_void_sp_var_rrelease(&local_void_ro, size);

    plat_shmem_var_free(my_void_sp, shared_void, size);
}

#include "platform/opts_c.h"
