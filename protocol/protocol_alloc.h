/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   protocol_alloc.h
 * Author: Brian O'Krafka
 *
 * Created on December 10, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: protocol_alloc.h 802 2008-03-29 00:44:48Z briano$
 */

#ifndef _PROTOCOL_ALLOC_H
#define _PROTOCOL_ALLOC_H

#include "platform/shmem_arena.h"

#ifdef __cplusplus
extern "C" {
#endif

    /* Define this to enable memory allocation using arenas. */
// #define PROTOCOL_USE_ARENAS
#define PROTOCOL_USE_ARENAS

#ifdef PROTOCOL_USE_ARENAS
    #define proto_plat_alloc_arena(x, y) plat_alloc_arena(x, y)
    #define proto_plat_shmem_arena_alloc(t, a, f) plat_shmem_arena_alloc(t, a, f)
    #define proto_plat_shmem_var_arena_alloc(t, s, a, f) plat_shmem_var_arena_alloc(t, s, a, f)
#else
    #define proto_plat_alloc_arena(x, y) plat_alloc(x)
    #define proto_plat_shmem_arena_alloc(t, a, f) plat_shmem_alloc(t)
    #define proto_plat_shmem_var_arena_alloc(t, s, a, f) plat_shmem_var_alloc(t, s)
#endif // PROTOCOL_USE_ARENAS

  // Arena settings for protocol code memory allocations
extern enum plat_shmem_arena NonCacheObjectArena;
extern enum plat_shmem_arena CacheObjectArena;

#ifdef	__cplusplus
}
#endif

#endif	/* _PROTOCOL_ALLOC_H */

