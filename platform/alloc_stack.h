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

#ifndef PLATFORM_ALLOC_STACK_H
#define PLATFORM_ALLOC_STACK_H 1

/*
 * File:   sdf/platform/alloc_stack.h
 * Author: drew
 *
 * Created on March 27, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: alloc_stack.h 761 2008-03-28 02:31:04Z drew $
 */

#include "platform/defs.h"

__BEGIN_DECLS

/**
 * @brief Allocate stack with red-zone
 *
 * Malloc is used to obtain memory so that leak checkers like dmalloc do 
 * there thing.
 *
 * @param len <IN> length in bytes
 * @return bottom of stack (user probably initializes sp to ret + len)
 */
void *plat_alloc_stack(int len);

/**
 * @brief Free stack allocated with plat_alloc_stack
 *
 * @param stack <IN> stack created with plat_alloc_stack
 */
void plat_free_stack(void *stack);

__END_DECLS

#endif /* ndef PLATFORM_ALLOC_STACK */
