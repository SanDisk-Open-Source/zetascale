/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
