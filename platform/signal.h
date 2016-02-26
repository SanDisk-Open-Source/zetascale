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

#ifndef PLATFORM_SIGNAL_H
#define PLATFORM_SIGNAL_H 1
/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/signal.h $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: signal.h 284 2008-02-17 02:16:45Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 *
 * Sigaction currently can't be hidden because it's a structure
 * name and a function.
 */

#ifndef PLATFORM_PROCESS_C
#define kill kill_use_plat_kill
#define signal signal_use_plat_sigaction
#endif

#include <signal.h>

#ifndef PLATFORM_PROCESS_C
#undef kill
#undef signal
#endif

#include "platform/types.h"

__BEGIN_DECLS

int plat_kill(pid_t pid, int sig);
int plat_sigaction(int signum, const struct sigaction *act, struct sigaction
      *oldact);


__END_DECLS

#endif /* ndef PLATFORM_SIGNAL_H */
