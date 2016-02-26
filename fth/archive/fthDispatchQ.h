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
 * File:   fthDispatchQ.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthDispatchQ.h,v 1.1 2008/08/11 20:59:44 drew Exp drew $
 */


#ifndef _FTH_DISPATCHQ_H
#define _FTH_DISPATCHQ_H

#include "platform/types.h"
#include "platform/defs.h"


#ifdef VECTOR_SCHED

struct fthDispatchQ;

// Linked list definitions for threads
#undef LLL_NAME
#undef LLL_EL_TYPE
#undef LLL_EL_FIELD
#undef LLL_INLINE

#define LLL_NAME(suffix) fthDispatchQ ## suffix
#define LLL_EL_TYPE struct fthDispatchQ
#define LLL_EL_FIELD dispatchQ
//#define LLL_INLINE PLAT_EXTERN_INLINE

#include "fthlll.h"

// Dispatch map is used in the scheduler to find the correct thread to dispatch
typedef struct fthDispatchQ {
    struct fthThread *thread[8];
    uint64_t schedPrefMask;                  // Mask of preferred schedulers

    fthDispatchQ_lll_el_t dispatchQ;
    
} fthDispatchQ_t;

#ifdef FTH_DISPATCHQ_C
#include "fthlll_c.h"
#endif

#undef LLL_INLINE

#endif // defined(FTH_THREADQ_C) || !defined(FTH_THREADQ_NO_INLINE)

#endif
