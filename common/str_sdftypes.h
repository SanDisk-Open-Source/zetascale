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
 * File:   common/str_sdftypes.h
 * Author: Darpan Dinker
 *
 * Created on February 4, 2008, 4:20 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: str_sdftypes.h 1894 2008-07-02 09:00:22Z drew $
 */

#ifndef _STR_SDFTYPES_H
#define _STR_SDFTYPES_H

#ifdef __cplusplus
extern "C" {
#endif

const char *str_SDF_storage_type_t[] = {"SDF_DRAM", "SDF_FLASH", "SDF_DISK"};

const char *str_SDF_boolean_t[] = {"SDF_FALSE", "SDF_TRUE"};

const char *str_SDF_lock_type_t[] = {"UNLOCKED", "READ", "UPDATE", "WRITE"};

const char *str_SDF_object_get_mode_t[] = {"SDF_GET_FOR_READ", "SDF_GET_FOR_UPDATE"};

const char *str_SDF_pin_type_t[] = {"UNPINNED", "PINNED"};

const char *str_SDF_hierarchy_level_t[] = {"ALL_PERSISTENT", "PERSISTENT", "DEFAULT", "ONE_LEVEL", "TWO_LEVEL",
                                           "THREE_LEVEL", "FOUR_LEVEL", "FIVE_LEVEL", "SIX_LEVEL"};

#ifdef __cplusplus
}
#endif

#endif /* _STR_SDFTYPES_H */
