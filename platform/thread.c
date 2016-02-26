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
