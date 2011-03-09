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
