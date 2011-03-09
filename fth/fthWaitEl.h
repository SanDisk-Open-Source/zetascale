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
