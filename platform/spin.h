/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_SPIN_H
#define PLATFORM_SPIN_H 1

#include <sys/types.h>

/*
 * Spin lock
 *
 * 64 bits are used instead of 32 because this makes it inexpensive to
 * associate a unique value with each lightweight process to facilitate
 * lock release on abnormal termination.
 *
 * This should normally map to the PLAT_SP(struct plat_user_thread_t)
 * referring to the thread.
 */
typedef uint64_t plat_spin_t;

/* Tries count times.  Returns 0 on success, -1 on failure */
int plat_spin_lock_count(spin_t *lock, int count);

/*
 * Tries a reasonable number of times and periodically calls plat_yield.
 */
void plat_spin_lock(plat_spin_t *lock);

void plat_spin_ulock(plat_spin_t *lock);

#endif /* ndef PLATFORM_SPIN_H */
