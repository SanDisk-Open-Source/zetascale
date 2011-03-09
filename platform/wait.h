#ifndef PLATFORM_WAIT_H
#define PLATFORM_WAIT_H 1

/*
 * File:   platform/wait.h
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: wait.h 208 2008-02-08 01:12:24Z drew $
 */

/*
 * Thin wrappers for unix wait functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include <sys/cdefs.h>

#include <sys/wait.h>

#include "platform/wrap.h"

#ifdef PLATFORM_INTERNAL
#define sys_wait wait
#define sys_wait3 wait3
#define sys_wait4 wait4
#define sys_waitpid waitpid
#endif

#ifndef PLATFORM_INTERNAL
PLAT_WRAP_CPP_POISON(wait wait3 wait4 waitpid)
#endif

#include "platform/types.h"

__BEGIN_DECLS

pid_t plat_wait(int *status);
pid_t plat_waitpid(pid_t pid, int *status, int flags);

__END_DECLS

#endif /* ndef PLATFORM_WAIT_H */
