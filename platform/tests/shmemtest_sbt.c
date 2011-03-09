/*
 * File:   sdf/platform/tests/shmemtest_sbt.c
 * Author: gshaw
 *
 * Created on June 27, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmemtest_sbt.c 5923 2009-02-19 11:20:40Z drew $
 */

/*
 * Test of stack backtraces on shmem_alloc and shmem_free.
 *
 * This is just like shmemtest_one.c, except that it arranges for
 * stack backtraces to be enabled and traces them, and at the end,
 * information about the set of backtraces is dumped out.
 *
 * Also, the test is repeated in a loop, in order to demonstrate that
 * stack backtraces get properly converted into indices of backtraces
 * that we have already seen.
 */


#include <limits.h>
#include <inttypes.h>           // Import PRI... format strings

#include "misc/misc.h"

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _shmemtest_sbt
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/shmem_debug.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/unistd.h"

#undef min
#define min(a, b) ((a) <= (b) ? (a) : (b))

#define eprintf(fmt, ...) \
    fprintf(stderr, fmt, ## __VA_ARGS__)

#define dprintf(fmt, ...) \
    ({ if (opt_debug) { fprintf (stderr, fmt, ## __VA_ARGS__); }; })

/* Command line arguments */
#define PLAT_OPTS_ITEMS_shmemtest_sbt()                                        \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    item("debug", "enable debugging of stack backtraces", DEBUG,               \
            ({ opt_debug = 1; 0; }), PLAT_OPTS_ARG_NO)

struct plat_opts_config_shmemtest_sbt {
    struct plat_shmem_config shmem;
    int opt_debug;
};

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_TEST, "shmemtest_sbt");

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

static void test_list(void);
static void test_array(void);
static void test_void(void);

/* Enable debugging of stack backtraces */
static int opt_debug = 0;

int
main(int argc, char **argv) {
    struct plat_opts_config_shmemtest_sbt config;
    struct plat_shmem_alloc_stats init_stats;
    struct plat_shmem_alloc_stats end_stats;
    int stats_balance;

    memset(&config, 0, sizeof(config));
    plat_shmem_config_init(&config.shmem);

    int status;
    int test_nr;

    if (plat_opts_parse_shmemtest_sbt(&config, argc, argv)) {
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

    plat_assert_always(node_sp_is_null(node_sp_null));

    status = plat_shmem_alloc_get_stats(&init_stats);
    plat_assert_always(!status);

    test_list();

    /* Detach and re-attach */
    status = plat_shmem_detach();
    plat_assert_always(!status);

    status = plat_shmem_attach(plat_shmem_config_get_path(&config.shmem));
    plat_assert_always(!status);

    test_array();

    shmem_backtrace_init();
    shmem_backtrace_enable();
    if (opt_debug) {
        shmem_backtrace_debug();
    }

    for (test_nr = 1; test_nr <= 10; ++test_nr) {
        if (opt_debug) {
            eprintf("======================================== ");
            eprintf("Test #%d\n", test_nr);
        }
        test_void();
    }

    shmem_backtrace_fini();

    /*
     * Allocations have the side effect of allocating thread local arenas.
     * Free these up before getting usage.
     */
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
test_list(void) {
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
test_array(void) {
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
test_void(void) {
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

    if (opt_debug) {
        extern unsigned int shmem_bt_state;
        extern unsigned int shmem_bt_debug;

        eprintf("shmem_bt_state = %u\n", shmem_bt_state);
        eprintf("shmem_bt_debug = %u\n", shmem_bt_debug);
        eprintf("Backtrace dump\n");
        eprintf("--------------\n");
        shmem_backtrace_dump();
    }
}

#include "platform/opts_c.h"
