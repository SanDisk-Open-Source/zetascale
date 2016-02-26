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
 * File:   alloc_stacktest.c
 * Author: drew
 *
 * Created on March 27, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: alloc_stacktest.c 4673 2008-11-26 02:57:28Z drew $
 */

/**
 * Test plat_alloc_stack.
 */
#include <setjmp.h>

#include <valgrind/valgrind.h>

#include "platform/alloc_stack.h"
#include "platform/opts.h"
#include "platform/signal.h"
#include "platform/stdlib.h"
#include "platform/string.h"

static sig_atomic_t got_signal;

static jmp_buf buf;

static struct sigaction action;

static void
sighandler(int signo) {
    got_signal = signo;
    longjmp(buf, 1);
}

int
main(int argc, char **argv) {
    int *stack;
    int status;

    if (plat_opts_parse(argc, argv)) {
        plat_exit(2);
    }

    /* Test normal operation */
    stack = plat_alloc_stack(100 * sizeof(int));
    plat_assert_always(stack);
    stack[0] = 1;
    stack[99] = 99;

    /* Test overrun */
    memset(&action, 0, sizeof(action));
    action.sa_handler = &sighandler;
    status = plat_sigaction(SIGSEGV, &action, NULL);
    plat_assert_always(!status);
    /*
     * Allow return from signal handler which doesn't redo the failed
     * operation and unblocks SIGSEGV.
     */
    if (!sigsetjmp(buf, 1)) {
        stack[-1] = 1;
    }
    plat_assert_always(got_signal == SIGSEGV);

    /* Test free */
    action.sa_handler = SIG_DFL;
    status = plat_sigaction(SIGSEGV, &action, NULL);
    plat_assert_always(!status);
    plat_free_stack(stack);
   
    if (!RUNNING_ON_VALGRIND) {
        /* Test refree */
        got_signal = 0;
        action.sa_handler = &sighandler;
        status = plat_sigaction(SIGABRT, &action, NULL);
        plat_assert_always(!status);
        if (!sigsetjmp(buf, 1)) {
            plat_free_stack(stack);
        }
        plat_assert_always(got_signal == SIGABRT);

        action.sa_handler = SIG_DFL;
        status = plat_sigaction(SIGSEGV, &action, NULL);
        plat_assert_always(!status);
    }

    return (0);
}
