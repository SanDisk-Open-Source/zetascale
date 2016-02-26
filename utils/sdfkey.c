/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   utils/sdfkey.c
 * Author: Darryl Ouye
 * 
 * Created on August 22, 2008, 1:07 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdfkey.c $
 */

// All functions are inline and defined in sdfkey.h

#include "sdfkey.h"

PLAT_SP_VAR_OPAQUE_IMPL(char_sp, char);

#ifndef SDFCC_SHMEM_FAKE
// PLAT_SP_IMPL(SDF_object_key_sp, struct _SDF_object_key);
PLAT_SP_IMPL(SDF_key_sp, struct _SDF_key);
#else
// typedef struct _SDF_key SDF_key_sp;
// typedef struct _SDF_key * SDF_key_sp_t;
#endif
