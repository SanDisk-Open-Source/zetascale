/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/thread.c $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: thread.c 208 2008-02-08 01:12:24Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#define PLATFORM_THREAD_C

#include "platform/thread.h"

int
plat_kthread_create(plat_kthread_t *thread, void * (*start_routine)(void *),
    void * arg) {
    /*
     * FIXME: Create thread structure in shared memory which is attached
     * to current process structure.
     */
    return (pthread_create(thread, NULL /* attr */, start_routine, arg));
}

int
plat_kthread_join(plat_kthread_t thread, void **ret) {
    return (pthread_join(thread, ret));
}

void
plat_kthread_exit(void *ret) {
    /*
     * FIXME: Remove thread structure from shared memory which is atttached
     * to curent process structure.
     */
    pthread_exit(ret);
}
