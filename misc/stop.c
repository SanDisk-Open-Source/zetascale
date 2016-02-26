/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
