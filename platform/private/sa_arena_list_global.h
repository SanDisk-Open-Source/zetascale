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
 * Defines to make make linked list in shmem of all arenas attached to the
 * shared space
 *
 * Included before fth/fthlll.h and fthlll_c.h
 */

#include "fth/fthlllUndef.h"

#define LLL_NAME(suffix) sa_arena_global ## suffix
#define LLL_SP_NAME(suffix) sa_arena_sp ## suffix
#define LLL_EL_TYPE struct sa_arena
#define LLL_EL_FIELD global_list_entry

#define LLL_SHMEM 1
#define LLL_INLINE static __attribute__((unused))
