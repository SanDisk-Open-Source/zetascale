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
