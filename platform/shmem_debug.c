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
 * File:   sdf/platform/shmem_debug.c
 * Author: gshaw
 *
 * Created on June 12, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmem_debug.c,v 1.1 2008/06/26 04:26:18 gshaw Exp gshaw $
 */

/**
 * Debugging services for SDF shared memory.
 *
 */

#include <stdio.h>
#include <inttypes.h>           // Import PRI... format string constsnts
#include <stdlib.h>             // Import free()
#include <stdint.h>             // Import uintptr_t
#include <sys/types.h>          // Import pid_t
#include <unistd.h>             // Import getpid()
#include <execinfo.h>           // Import backtrace()

#include "shmem_global.h"
#include "shmem_debug.h"

#include "platform/aoset.h"

/**
 * @brief Common C language "extensions".
 *
 * This stuff should be in a "well-known" include file.
 */

#define ELEMENTS(var) (sizeof (var) / sizeof (*var))

#define eprintf(fmt, ...) \
    fprintf(stderr, fmt, ## __VA_ARGS__)

#define dprintf(fmt, ...) \
    ({ if (shmem_bt_debug) { fprintf(stderr, fmt, ## __VA_ARGS__); }; })

/**
 * @brief Current state of collection of stack backtraces.
 *
 * State can be controlled explicitly, using shmem_backtrace_enable()
 * and shmem_backtrace_disable().  The collection of stack backtraces
 * can be disabled when the resource allocation for collecting the
 * data fails.  That way, we don't just bail out, but we don't keep
 * trying to allocate resources, either.
 */

uint_t shmem_bt_state = 0;
uint_t shmem_bt_debug = 0;

static aoset_hdl_t shmem_backtrace_set;

/*
 * Initialize the global set of stack backtraces.
 *
 * For purposes of recording stack backtraces, there should be just the one
 * global set.  All processes should ensure that that set is initialized,
 * but only one process should win the race and actually create the
 * bookkeeping data structure that manages the set of stack backtraces.
 * All others should just make note of the fact that it has been done,
 * or, if it under construction, wait for initialization to complete.
 *
 * Values of global SHMEM_GLOBAL_BACKTRACE_SET:
 *   0:     Uninitialized
 *   1:     Under construction (wait)
 *   2:     Error occurred attempting to construct
 *   other: shmem pointer to set of stack backtraces
 */

void
shmem_backtrace_init(void)
{
    uint64_t global_set;
    aoset_hdl_t new_set;

    if (!GVPTR_IS_NULL(shmem_backtrace_set)) {
        /*
         * This process already has a copy of the pointer to the global set.
         */
        return;
    }

    for (;;) {
        global_set = shmem_global_get(SHMEM_GLOBAL_BACKTRACE_SET);
        if (global_set == 0) {
            /*
             * global-set is completely uninitialized.
             * Mark it as "under construction" and initialize it now.
             */
            uint64_t old_val;

            old_val = shmem_global_set(SHMEM_GLOBAL_BACKTRACE_SET, 1);
            if (old_val == 0) {
                new_set = aoset_create();
                dprintf("aoset_create() -> %" PRIx64 "\n", new_set.int_base);
                if (GVPTR_IS_NULL(new_set)) {
                    new_set.int_base = 2;   /* Mark as error */
                }
                if (shmem_global_reset(SHMEM_GLOBAL_BACKTRACE_SET,
                                   new_set.int_base)) {}
                shmem_backtrace_set = new_set;
                break;
            }
        } else if (global_set == 1) {
            /*
             * global_set is under construction.
             * Some other process is has won the race and is creating the
             * global stack backtrace set.
             *
             * Back off and try again, later.
             */
            sleep(1);
        } else if (global_set == 2) {
            /*
             * An error occurred while attempting to construct the global set.
             * Disable stack backtraces.
             */
            shmem_bt_state = 0;
            break;
        } else {
            /*
             * Initialization of global_set is complete.
             * Save the per-process copy.
             */
            shmem_backtrace_set.int_base = global_set;
            break;
        }
        shmem_backtrace_set = aoset_create();
    }
}

void
shmem_backtrace_fini(void)
{
    aoset_destroy(shmem_backtrace_set);
}

/**
 * @brief Enable collection of stack backtraces on every
 * shmem alloc and free.
 *
 * XXX Should have much more refined control, along the lines
 * XXX of the category/subcategory that the logging facility
 * XXX implements.  But for now, it is just on or off.
 */

void
shmem_backtrace_enable(void)
{
    shmem_bt_state = 1;
}

void
shmem_backtrace_disable(void)
{
    shmem_bt_state = 0;
}

void
shmem_backtrace_debug(void)
{
#ifdef AOSET_DEBUG
    aoset_debug(1);
#endif /* def AOSET_DEBUG */
    shmem_bt_debug = 1;
}

/**
 * @brief Print a stack backtrace on stderr.
 *
 * It might be nice to use the GNU libc function, backtrace_symbols()
 * to show a stack backtraces in a human-readable form.  But, meaningful
 * function names are not avaiable -- at least not the way we build our
 * binaries, now.  If and when binaries are built using -rdynamic,
 * then we can enable USE_GLIBC_BACKTRACE_SYMBOLS.
 *
 * But then we may not need to bother, if gdb can process the raw hexadecimal
 * stack backtraces conveniently enough.
 */

#ifdef USE_GLIBC_BACKTRACE_SYMBOLS

void
eprint_backtrace(void **bt_buf, int bt_size)
{
    char **symv;
    uintptr_t pc;
    uint_t i;
    int pid;

    pid = (int)(uintptr_t)bt_buf[0];
    eprintf("backtrace pid = %d\n", pid);
    ++bt_buf;
    --bt_size;
    eprintf("backtrace size = %d\n", bt_size);
    symv = backtrace_symbols(bt_buf, bt_size);
    for (i = 0; i < bt_size; ++i) {
        pc = (uintptr_t)bt_buf[i];
        eprintf("  %3u %16" PRIxPTR " %s\n", i, pc, symv[i]);
    }
    free(symv);
}

#else

void
eprint_backtrace(void **bt_buf, int bt_size)
{
    uintptr_t pc;
    uint_t i;
    int pid;

    pid = (int)(uintptr_t)bt_buf[0];
    eprintf("backtrace pid = %d\n", pid);
    ++bt_buf;
    --bt_size;

    eprintf("backtrace size = %d\n", bt_size);
    for (i = 0; i < bt_size; ++i) {
        pc = (uintptr_t)bt_buf[i];
        eprintf("  %3u %16" PRIxPTR "\n", i, pc);
    }
}

#endif /* def USE_GLIBC_BACKTRACE_SYMBOLS */

/**
 * @brief Save a stack backtrace.
 *
 */

int
shmem_save_backtrace(void)
{
    /**
     * @brief Use a "reasonable" value for the backtrace buffer.
     *
     * Stacks can get pretty deep.  Each saved backtrace consumes
     * only the amount of space it needs.
     *
     * NOTE: If this were inside the kernel, declaring the buffer
     * with storage class automatic would be a "bad thing" because
     * kernel stacks are fixed size.
     */
    void *bt_buf[150];
    int bt_count;       /* Number of entries (program counters) */
    int bt_bsize;       /* Size in bytes of bt_buf[] actually used */
    int bt_index;
    int my_pid;

    dprintf("> shmem_save_backtrace: shmem_bt_state = %u\n", shmem_bt_state);

    if (shmem_bt_state == 0) {
        return (SHMEM_BACKTRACE_NONE);
    }

    bt_count = backtrace(bt_buf, ELEMENTS(bt_buf));
    bt_bsize = bt_count * sizeof (*bt_buf);
    my_pid = getpid();
    bt_buf[0] = (void *)(uintptr_t)my_pid;

    if (GVPTR_IS_NULL(shmem_backtrace_set)) {
        shmem_backtrace_init();
    }

    if (shmem_bt_debug) {
        eprint_backtrace(bt_buf, bt_count);
    }

    /*
     * XXX aoset_add() should have options to work any of three ways:
     * XXX   1. Objects are stable, and therefore do not need to be saved;
     * XXX   2. Objects are saved using plat_alloc();
     * XXX   3. Objects are saved in shared memory.
     */
    bt_index = aoset_add(shmem_backtrace_set, bt_buf, bt_bsize);

    /*
     * XXX Perhaps should disable backtraces on first occurrance
     * XXX of memory exhaustion.  Or, abort().
     */
    if (bt_index < 0) {
        shmem_bt_state = 0;
    }

    dprintf("%s: backtrace index=%d\n", __func__, bt_index);

    return (bt_index);
}

/*
 * Dump out useful information about the accumulated stack backtraces
 */

void
shmem_backtrace_dump(void)
{
    eprintf("shmem_backtrace_dump: shmem_backtrace_set = %" PRIx64 "\n",
            shmem_backtrace_set.int_base);

    if (GVPTR_IS_NULL(shmem_backtrace_set)) {
        return;
    }

    aoset_diag(shmem_backtrace_set);
}
