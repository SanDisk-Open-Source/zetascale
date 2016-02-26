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

#ifndef PLATFORM_MUTEX_H
#define PLATFORM_MUTEX_H  1

/*
 * File:   mutex.h
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mutex.h 1164 2008-05-07 10:23:12Z drew $
 */

/*
 * Mutex  for crash-consistent shm + IPC between user scheduled threads.
 *
 * FIXME: We need a more complex locking model (at least hierchical)
 */

#include "platform/defs.h"

#ifndef notyet
#include <pthread.h>

#define PLAT_RECURSIVE_MUTEX_INITIALIZER PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP
#define PLAT_MUTEX_INITIALIZER PTHREAD_MUTEX_INITIALIZER

typedef pthread_mutex_t plat_mutex_t;
#else

#include "platform/spin.h"
#include "platform/wait_list.h"

struct plat_mutext_t {
    plat_spin_t value;
    plat_wait_list_t waiting;
};
#endif /* ndef notyet */

__BEGIN_DECLS

/* Return 0 on success, -errno on failure */
int plat_mutex_lock(plat_mutex_t *mutex);
int plat_mutex_init(plat_mutex_t *mutex);
int plat_mutex_unlock(plat_mutex_t *unlock);

__END_DECLS

#endif /* ndef PLATFORM_MUTEX_H */
