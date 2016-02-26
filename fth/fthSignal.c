/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
