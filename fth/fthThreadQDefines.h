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
 * File:   fthThreadQDefines.h
 * Author: drew
 *
 * Created on August 11, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthThreadQDefines.h 2791 2008-08-11 23:30:33Z drew $
 */

/*
 * The way the intrusive lists work, we need to 
 * - Define the list types
 * - Define the structure which inturn has list entry elements
 * - Define the inlines 
 *
 * This mess is easiest when an idempotent #define part of the lll
 * goo exists.
 */

// Linked list definitions for threads
#undef LLL_NAME
#undef LLL_EL_TYPE
#undef LLL_EL_FIELD
#undef LLL_INLINE

#define LLL_NAME(suffix) fthThreadQ ## suffix
#define LLL_EL_TYPE struct fthThread
#define LLL_EL_FIELD threadQ


