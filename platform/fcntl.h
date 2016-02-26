/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_FCNTL_H
#define PLATFORM_FCNTL_H 1

/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/fcntl.h $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fcntl.h 1611 2008-06-13 01:27:35Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include <sys/cdefs.h>

#include <fcntl.h>

#include "platform/wrap.h"

#ifdef PLATFORM_INTERNAL
#define sys_creat creat
#define sys_fcntl fcntl
#define sys_open open
#endif

/*
 * FIXME: Should use sys_ within platform code so we have to make the choice
 * between plat_ and sys_ flavor instead of just getting something.  With
 * that in place poisoning can become unconditional.
 */
#ifndef PLATFORM_INTERNAL
/* Hide all wrapped header symbols here */
PLAT_WRAP_CPP_POISON(creat fcntl open)
#endif

__BEGIN_DECLS

int plat_creat(const char *pathname, int mode);
int plat_fcntl(int fd, int cmd, ...);
int plat_open(const char *pathname, int flags, ...);

__END_DECLS

#endif /* ndef PLATFORM_FCNTL_H */
