/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
