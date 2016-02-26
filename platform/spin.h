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
