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
