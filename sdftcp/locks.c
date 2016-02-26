/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
