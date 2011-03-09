/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/sysvipc.c $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sysvipc.c 631 2008-03-17 22:58:28Z tomr $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include "platform/shm.h"

void *
plat_shmat(int shmid, const void *shmaddr, int shmflg) {
    return (shmat(shmid, shmaddr, shmflg));
}

int
plat_shmdt(const void *shmaddr) {
    return (shmdt(shmaddr));
}
