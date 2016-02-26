/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
