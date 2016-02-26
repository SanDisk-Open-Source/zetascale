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

#ifndef PLATFORM_SHMEM_PTRS_H
#define PLATFORM_SHMEM_PTRS_H 1

/*
 * File:   sdf/platform/shmem_ptrs.h
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmem_ptrs.h 264 2008-02-13 18:00:58Z drew $
 */

/**
 * Insure that platform shared-memory pointers are only declared once by
 * including shmem_ptrs.h wherever one is needed.
 */

#include "platform/shmem.h"

/**
 * Enumerates all forward-declarable public shared pointers so that
 * declarations and helper functions can be automatically generated.  Add
 * necessary includes in SHMEM_PTRS_C ifdef below
 */
#define PLAT_SHMEM_FORWARD_PUBLIC_PTR_ITEMS()                                  \
    item(plat_process_sp, struct plat_process)                                 \
    item(shmem_admin_sp, struct shmem_admin)                                   \
    item(shmem_header_sp, struct shmem_header)

/** 
 * Enumerates all forward-declarable shared pointers to opaque structures
 */  
#define PLAT_SHMEM_FORWARD_OPAQUE_PTR_ITEMS()                                  \
    item(shmem_alloc_sp, struct shmem_alloc)

#define item(name, type_name)                                                  \
    type_name;                                                                 \
    PLAT_SP(name, type_name)
PLAT_SHMEM_FORWARD_PUBLIC_PTR_ITEMS()
PLAT_SHMEM_FORWARD_OPAQUE_PTR_ITEMS()
#undef item

#ifdef SHMEM_PTRS_C
#include "private/process.h"
#include "private/shmem_internal.h"

#define item(name, type_name) PLAT_SP_IMPL(name, type_name)
PLAT_SHMEM_FORWARD_PUBLIC_PTR_ITEMS()
#undef item
#endif /* def SHMEM_PTRS_C */

#endif /* ndef PLATFORM_PROCESS_PTR_H */
