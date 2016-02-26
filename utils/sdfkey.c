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
