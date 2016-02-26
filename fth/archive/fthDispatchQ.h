/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
