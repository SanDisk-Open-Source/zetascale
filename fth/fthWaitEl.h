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
 * File:   fthWaitEl.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthWaitEl.h 396 2008-02-29 22:55:43Z jim $
 */

/**
 * @brief  Featherweight threading structure for wait elements
 */

#include "fthSpinLock.h"

#ifndef _FTH_WAIT_EL_H
#define _FTH_WAIT_EL_H

#include "platform/types.h"

// Linked list definitions for wait elements
//
#undef LLL_NAME
#undef LLL_EL_TYPE
#undef LLL_EL_FIELD
#undef LLL_INLINE

#define LLL_NAME(suffix) fthWaitQ ## suffix
#define LLL_EL_TYPE struct fthWaitEl
#define LLL_EL_FIELD waitQEl

#include "fthlll.h"

typedef struct fthWaitEl {
    fthWaitQ_lll_el_t waitQEl;
    struct fthThread *thread;                // Requesting thread
    int pool;                                // Set if wait el is a pool (shared) el
    union {
        struct {                             // FTH lock
            int write;
            union {
                struct fthLock *lock;
                struct fthSparseLock *sparseLock;
            };
                
        };
        uint16_t *mem;                       // Mem wait
        struct fthCrossLock *crossLock;
        uint64_t mailData;
    };

#ifndef TEMP_WAIT_EL        
    struct fthWaitEl *list;
    void *caller;
#endif    
    
} fthWaitEl_t;

#define WAIT_EL_INIT(wa)                                        \
    (wa)->pool = 0;                                             \
    fthWaitQ_el_init(wa);                                       \
    (wa)->thread = (void *) fthLockID();

#undef LLL_INLINE

#ifdef FTH_WAITQ_C
#define LLL_INLINE
#else
#define LLL_INLINE PLAT_EXTERN_INLINE
#endif

#include "fthlll_c.h"

#undef LLL_INLINE

#endif
