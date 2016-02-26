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

#ifndef PLATFORM_RWLOCK_H
#define PLATFORM_RWLOCK_H  1

/*
 * File:   rwlock.h
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: rwlock.h 342 2008-02-23 05:58:08Z drew $
 */

/*
 * Read-write lock wrapper 
 */
#ifndef notyet
#include <pthread.h>

#define PLAT_RWLOCK_INITIALIZER PTHREAD_RWLOCK_INITIALIZER

typedef pthread_rwlock_t plat_rwlock_t;

#endif

int plat_rwlock_init(plat_rwlock_t *rwlock);
int plat_rwlock_destroy(plat_rwlock_t *rwlock);
int plat_rwlock_rdlock(plat_rwlock_t *rwlock);
int plat_rwlock_tryrdlock(plat_rwlock_t *rwlock);
int plat_rwlock_wrlock(plat_rwlock_t *rwlock);
int plat_rwlock_trywrlock(plat_rwlock_t *rwlock);
int plat_rwlock_unlock(plat_rwlock_t *rwlock);

#endif /* ndef PLATFORM_RWLOCK_H */
