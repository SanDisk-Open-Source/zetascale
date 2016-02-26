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
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/process.c $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: process.c 427 2008-03-01 03:28:13Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#define PLATFORM_INTERNAL 1
#define PLATFORM_PROCESS_C 1

#include "platform/types.h"
#include "platform/signal.h"
#include "platform/stdlib.h"
#include "platform/unistd.h"
#include "platform/wait.h"

#define item(ret, sym, declare, call, cppthrow, attributes) \
    PLAT_WRAP_IMPL(ret, sym, declare, call, cppthrow, attributes)
PLAT_UNISTD_WRAP_PROCESS_ITEMS()
#undef item

void
plat_abort() {
    sys_abort();
}

void
plat__exit(int status) {
    sys__exit(status);
}

void
plat_exit(int status) {
    sys_exit(status);
}

int
plat_kill(pid_t pid, int signal) {
    return (kill(pid, signal));
}

int
plat_sigaction(int signum, const struct sigaction *act, struct sigaction
    *oldact) {
    return (sigaction(signum, act, oldact));
}

pid_t
plat_wait(int *status) {
    return (plat_waitpid(-1, status, 0));
}

pid_t
plat_waitpid(pid_t pid, int *status, int flags) {
    return (sys_waitpid(pid, status, flags));
}
