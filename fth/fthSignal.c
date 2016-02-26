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
 * File:   fthSignal.h
 * Author: drew
 *
 * Created on March 25, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthSignal.c 6472 2009-03-28 03:22:37Z drew $
 */

/**
 * Posix 1003 signal handling capabilities are insufficient to 
 * do anything significant because even system libraries
 * (such as stdio in libc) aren't re-entrant to say nothing
 * of user code.
 *
 * Standard practice is to run the actual signal handlers out 
 * of a thread which is merely awoken by an allowed mechanism.
 */

#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#include <semaphore.h>

#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/signal.h"

#include "fth.h"
#include "fthMbox.h"

enum {
    /** @brief Internal simulated signal for shutdown */
    FTH_SIGNAL_SHUTDOWN = 63,

    /** @brief Stack size for user handlers */
    FTH_SIGNAL_STACK_SIZE = 40960
};


void fthSignalInit() 
{
    // xxxzzz
}

void fthSignalShutdown() 
{
    // xxxzzz
}

sighandler_t fthSignal(int signum, sighandler_t handler) 
{
    sighandler_t old;

    memset(&old, 0, sizeof(sighandler_t));
    // xxxzzz
    return (old);
}
