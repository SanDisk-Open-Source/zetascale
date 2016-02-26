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

#ifndef PLATFORM_TYPES_H
#define PLATFORM_TYPES_H  1

/*
 * File:   types.h
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: types.h 676 2008-03-20 05:00:53Z drew $
 */

#include <sys/types.h>
/* For uint<size>_t which are our standard integral types */
#include <stdint.h>

/*
 * Operation label.  To facilitate debugging this must be unique within a
 * cluster across a reasonable failure + recovery time frame.
 *
 * FIXME: Should have helper functions to create.
 */
typedef struct {
    int64_t node_id;
    int64_t op_id;
} plat_op_label_t;

typedef union {
    uint32_t integer;
    char text[4];
} plat_magic_t;

#endif /* ndef PLATFORM_TYPES_H */
