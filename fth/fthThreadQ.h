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
 * File:   fthThreadQ.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthThreadQ.h,v 1.1 2008/08/11 20:59:44 drew Exp drew $
 */


#ifndef _FTH_THREADQ_H
#define _FTH_THREADQ_H


#include "platform/defs.h"

#include  "fthThreadQDefines.h"
#include "fthlll.h"

#include "fthThread.h"

#if defined(FTH_THREADQ_C) || !defined(FTH_THREADQ_NO_INLINE)

// Make thread Q manipulations in-line when compiled with optomization,
// out-of-line without.

#include "fthThreadQDefines.h"


#ifdef FTH_THREADQ_C
#define LLL_INLINE
#else
#define LLL_INLINE PLAT_EXTERN_INLINE
#endif

#include "fthlll_c.h"

#undef LLL_INLINE

#endif // defined(FTH_THREADQ_C) || !defined(FTH_THREADQ_NO_INLINE)

#endif
