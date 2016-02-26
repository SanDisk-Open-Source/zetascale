/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_SHM_H
#define PLATFORM_SHM_H 1

/*
 * File:   platform/mman.h
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shm.h 168 2008-02-04 19:17:14Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include <sys/cdefs.h>
#include <sys/shm.h>

__BEGIN_DECLS
void *plat_shmat(int shmid, const void *shmaddr, int shmflg);
int plat_shmdt(const void *shmaddr);
__END_DECLS

#endif /* ndef PLATFORM_SHM_H */
