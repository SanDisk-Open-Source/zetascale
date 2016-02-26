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
 * File:   $HeadURL
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mutex.c 342 2008-02-23 05:58:08Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include "platform/mutex.h"

int
plat_mutex_lock(plat_mutex_t *mutex) {
    return (pthread_mutex_lock(mutex));
}

int
plat_mutex_init(plat_mutex_t *mutex) {
    int ret;

    pthread_mutexattr_t attr;
    ret = pthread_mutexattr_init(&attr);
    if (!ret) {
        ret = pthread_mutexattr_setpshared(&attr, 1);
    }
    if (!ret) {
        ret = pthread_mutex_init(mutex, &attr);
    }

    return (ret);
}

int
plat_mutex_unlock(plat_mutex_t *mutex) {
    return (pthread_mutex_unlock(mutex));
}
