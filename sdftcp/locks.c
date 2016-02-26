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
 * File: locks.c
 * Author: Johann George
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Locking primitives.
 */
#define _XOPEN_SOURCE 600
#include <stdlib.h>
#include "locks.h"
#include "trace.h"
#include "msg_cat.h"


/*
 * For aligning to a cache line boundary.
 */
#define CLSIZE        64
#define roundup(n, s) ((((n)+(s)-1)/(s))*(s))
#define align(n)      roundup(n, CLSIZE)


/*
 * Initialize a write lock.
 */
wlock_t *
wl_init(void)
{
    wlock_t *l;
    size_t size = align(sizeof(*l));

    if (posix_memalign((void **)&l, CLSIZE, size) != 0)
        fatal_sys("out of memory");
    memset(l, 0, size);
    return l;
}


/*
 * Free a write lock.
 */
void
wl_free(wlock_t *l)
{
    plat_free(l);
}


/*
 * Initialize a read/write lock.
 */
rwlock_t *
rwl_init(void)
{
    rwlock_t *l;
    size_t size = align(sizeof(*l));

    if (posix_memalign((void **)&l, CLSIZE, size) != 0)
        fatal_sys("out of memory");
    memset(l, 0, size);
    return l;
}


/*
 * Free a read/write lock.
 */
void
rwl_free(rwlock_t *l)
{
    plat_free(l);
}
