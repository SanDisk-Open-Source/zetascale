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
 * File:   sdf/platform/rwlock.c[
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: rwlock.c 342 2008-02-23 05:58:08Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) The simulated cluster environment
 */

#include "platform/rwlock.h"
#include "stdio.h"

__thread long long locked = 0;
#if 0
void print_hex(const char* title, char* buf, int len)
{
	static char hex_tab[] = "0123456789ABCDEF";
	int i;
	fprintf(stderr, "%x %s %p [", (int)pthread_self(), buf, title);
	for(i = 0; i < len; i++)
		fprintf(stderr, "%c%c", hex_tab[(buf[i] >> 4)&0xf], hex_tab[buf[i]&0xf]);
	fprintf(stderr, "]\n");
}
#else
#define print_hex(t, b, l) do {} while(0);
#endif

//#define dbg_print(msg, ...) do { fprintf(stderr, "%x %s:%d " msg, (int)pthread_self(), __FUNCTION__, __LINE__, ##__VA_ARGS__); } while(0)
#define dbg_print(msg, ...)
int
plat_rwlock_init(plat_rwlock_t *rwlock) {
    int ret;

    pthread_rwlockattr_t attr;
    ret = pthread_rwlockattr_init(&attr);

    if (!ret) {
        ret = pthread_rwlockattr_setpshared(&attr, 1);
    }

    if (!ret) {
        ret = pthread_rwlock_init(rwlock, &attr);
    }

    return (ret);
}

int
plat_rwlock_destroy(plat_rwlock_t *rwlock) {
    return (pthread_rwlock_destroy(rwlock));
}

int
plat_rwlock_rdlock(plat_rwlock_t *rwlock) {
    dbg_print("rwlock=%p\n", rwlock);
    locked++;
	print_hex(__FUNCTION__, (char*)rwlock, sizeof(plat_rwlock_t));
    return (pthread_rwlock_rdlock(rwlock));
}

int
plat_rwlock_tryrdlock(plat_rwlock_t *rwlock) {
    dbg_print("rwlock=%p\n", rwlock);
    return (pthread_rwlock_tryrdlock(rwlock));
}

int
plat_rwlock_wrlock(plat_rwlock_t *rwlock) {
    dbg_print("rwlock=%p\n", rwlock);
    locked++;
	print_hex(__FUNCTION__, (char*)rwlock, sizeof(plat_rwlock_t));
    return (pthread_rwlock_wrlock(rwlock));
}

int
plat_rwlock_trywrlock(plat_rwlock_t *rwlock) {
    return (pthread_rwlock_trywrlock(rwlock));
}

int
plat_rwlock_unlock(plat_rwlock_t *rwlock) {
    dbg_print("rwlock=%p\n", rwlock);
    locked--;
	print_hex(__FUNCTION__, (char*)rwlock, sizeof(plat_rwlock_t));
    return (pthread_rwlock_unlock(rwlock));
}
