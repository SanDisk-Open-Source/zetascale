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

