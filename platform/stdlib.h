/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_STDLIB_H
#define PLATFORM_STDLIB_H 1

/*
 * File:   platform/alloc.h
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: stdlib.h 4056 2008-10-28 11:41:07Z drew $
 */

/*
 * Thin wrappers for unix functions to accomodate interception for
 * 1) Blocking behavior in user-scheduled threads
 * 2) Test and debugging instrumentation and error injection
 * 3) Process and cluster simulation with repeatable timing
 *
 * Non-standard allocation functions are provided so that the simulated
 * environment has an easier time separating target program and simulator
 * memory.  This is important for simulating process crashes where all
 * resources held by the target must be released.
 *
 * They are also provided so that the schooner code has separate allocation
 * arenas which can have known physical mappings for zero copy-IO and be used
 * to make it more difficult for a misbehaving third party application client
 * with our code co-resident (like MySQL, with 2500 reported segmentation
 * faults) to interfere with our software.
 *
 * A system compromising decreased performance for increased reliability could
 * even enable writes to our memory arena when our threads are scheduled, and
 * disable at all other times.
 */

#include <sys/cdefs.h>
#include <sys/types.h>

#include <stdlib.h>

#include "platform/assert.h"
#include "platform/logging.h"
#ifndef PLAT_SHMEM_FAKE
#include "platform/shmem.h"
#endif
#include "platform/wrap.h"

#include "platform/alloc.h"

#define PLAT_STDLIB_WRAP_RANDOM_ITEMS()                                        \
    item(void, srandom, (unsigned seed), (seed), __THROW, /* */)               \
    item(long, random, (void), (), __THROW, /* */)

#define PLAT_STDLIB_WRAP_ITEMS()                                               \
    PLAT_STDLIB_WRAP_RANDOM_ITEMS()

#ifdef PLATFORM_INTERNAL
#define sys_abort abort
#define sys_exit exit
#endif

PLAT_WRAP_CPP_POISON(abort exit)

__BEGIN_DECLS

#define __leaf__

#define item(ret, sym, declare, call, cppthrow, attributes) \
    PLAT_WRAP(ret, sym, declare, call, cppthrow, attributes)
PLAT_STDLIB_WRAP_ITEMS()
#undef item

void plat_abort() __attribute__((noreturn));
void plat_exit(int status) __attribute__((noreturn));

__END_DECLS

#endif /* def PLATFORM_STDLIB_H */
