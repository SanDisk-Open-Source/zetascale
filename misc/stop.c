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
 * File:   sdf/misc/stop.c
 * Author: drew
 *
 * Created on February 27, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: stop.c 590 2008-03-14 01:05:03Z drew $
 */

#include "platform/signal.h"
#include "platform/platform.h"
#include "platform/stdio.h"
#include "platform/unistd.h"

/*
 * Allow --stop to stop the process on startup and allow a debugger
 * to attach.
 */
int
stop_arg() {
    pid_t pid = plat_getpid();

    fprintf(stderr, "%s %d stopping\n", plat_get_exe_name(), (int) pid);
    plat_kill(pid, SIGSTOP);

    return (0);
}
