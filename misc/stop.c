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
