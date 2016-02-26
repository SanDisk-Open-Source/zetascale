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

#ifndef GVPTR_H
#define GVPTR_H 1

/*
 * File:   sdf/platform/gvptr.h
 * Author: Guy Shaw
 *
 * Created on August 9, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: $
 */

/*
 * Generic Void Pointer.
 *
 * Supply a set of function and macro definitions to handle allocation
 * and freeing of objects of unknown type and size.  The macros should
 * work, without code source code modification, for either shared memory
 * allocation or ordinary memory allocation using malloc() and free().
 *
 * Most applications that do anything with shared memory allocation
 * should use the type-safe macros for generating alloc, free, and
 * pointer conversion.  GVPTRs are only for code that interacts closely
 * with the undrlying shared memory implementation, or that must be
 * general-purpose and handle objects of unknown type and unknown size.
 *
 */

#include <string.h>             /* Import size_t */

#include "platform/stdlib.h"

#ifdef GVPTR_SHMEM

#include "platform/shmem.h"

typedef plat_shmem_ptr_base_t gvptr_t;

#define GVPTR_IS_NULL(hdl) plat_shmem_ptr_base_is_null(hdl)

#define GVPTR_ALLOC_HELPER(objsize) \
    plat_shmem_alloc_helper("", 0, "", 0, objsize, PLAT_SHMEM_ARENA_DEFAULT,   \
                            PLAT_SHMEM_ALLOC_INTERNAL)

#define GVPTR_FREE_HELPER(objhdl, objsize) \
    plat_shmem_free_helper("", 0, "", 0, objhdl, objsize, 1)

/*
 * Allocate memory for an object.
 * Set both a handle and a regular pointer.
 * In the case of SDF shared memory allocation,
 * a handle is a shared memory "pointer", and the
 * regular pointer is set to the local reference to the same object.
 *
 * When SDF shared memory is not used, the handle and the regular pointer
 * are both void * and get set to the same value.
 */

#define GVPTR_ALLOC(hdl, objsize)					\
    hdl = GVPTR_ALLOC_HELPER(objsize);

#define GVPTR_ALLOC_REF(hdl, ptr, objsize)				\
    hdl = GVPTR_ALLOC_HELPER(objsize);					\
    if (plat_shmem_ptr_base_is_null(hdl)) {				\
        ptr = NULL;							\
    } else {								\
        ptr = (void *)plat_shmem_ptr_base_to_ptr(hdl);			\
    }

#define GVPTR_FREE(hdl, objsize)					\
    GVPTR_FREE_HELPER(hdl, objsize)

#define GVPTR_REF(hdl)							\
    ((void *)plat_shmem_ptr_base_to_ptr(hdl))

#define GVPTR_TOXIFY(hdl)						\
    (hdl).int_base = -1;

#define GVPTR_NULLIFY(hdl)						\
    (hdl).int_base = 0;

#else /* def GVPTR_SHMEM */

typedef void *gvptr_t;

#define GVPTR_IS_NULL(hdl) ((hdl) == NULL)

#define GVPTR_ALLOC(hdl, objsize)					\
    hdl = plat_alloc(objsize);

#define GVPTR_ALLOC_REF(hdl, ptr, objsize)				\
    hdl = plat_alloc(objsize);						\
    ptr = hdl;

#define GVPTR_FREE(hdl, objsize)					\
    plat_free(hdl)

#define GVPTR_TOXIFY(hdl)						\
    hdl = (void *)(-1);

#define GVPTR_NULLIFY(hdl)						\
    hdl = NULL;

#endif /* def GVPTR_SHMEM */

#endif /* ndef GVPTR_H */
