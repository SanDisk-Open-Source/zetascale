/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthlll.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthlll.h 396 2008-02-29 22:55:43Z jim $
 */

/**
 * @brief Linked list with locks
 *
 * This file is normally included by a user locking linked list header file.  The header file
 * can be stand-alone but typically it also contains the typedef for the element struct. The
 * following macros need to be set up before this file is included.  Here is some sample
 * code for the structure foo:
 *
 * @code
 *
 *  // Undef the macros (might be more than one linked list being defined)
 *  #include "fth/fthlllUndef.h"
 *
 *  #define LLL_SHMEM 1                      // All pointers are SHMEM pointers
 *  #define LLL_NAME(suffix) foo ## suffix   // Should be the same as the SHMEM name
 *  #define LLL_EL_TYPE struct foo           // Name of element data structure
 *  #define LLL_EL_FIELD fooQ                // Name of field in element data structure
 *  #include "fth/lll.h"
 *
 *  typedef struct foo {                     // Linked-list element (LLL_EL_TYPE)
 *     int bar;
 *     foo_ll_el_t fooQ;                     // Linked-list element pointers (LLL_EL_FIELD)
 *     float baz;
 *  } foo_t;
 *
 *  @endcode
 *
 * Static types for internal structures may be instantiated by including 
 * @code
 * #define LLL_INLINE static __attribute__((unused))
 * @code
 * in the definitions.
 *
 * Where multiple static types are needed, it may be worth putting the LLL_* definitions into separate files since the 
 * fth/fthlll.h include must preceed the LLL_EL_TYPE definition while the fth/fthlll_c.h must follow them
 */

#include "fthSpinLock.h"

#ifndef LLL_INLINE
#define LLL_INLINE
#endif

#undef LLL_EL_REF_TYPE
#undef LLL_EL_PTR_TYPE
#undef LLL_NULL
#undef LLL_REF

#ifdef LLL_SHMEM
#ifndef LLL_SP_NAME
#define LLL_SP_NAME(suffix) LLL_NAME(_sp ## suffix)
#endif

#define LLL_EL_PTR_TYPE LLL_SP_NAME(_t)
#define LLL_EL_REF_TYPE LLL_EL_TYPE *

/* Avoid complaints about foo_sp_null being static while the functions aren't */
#if 0
#define LLL_NULL LLL_SP_NAME(_null)
#else
/* XXX: There's probably a better way to get a literal struct */
#define LLL_NULL ({ LLL_EL_PTR_TYPE lll_null = PLAT_SP_INITIALIZER; lll_null; })
#endif

/*
 * XXX: Shmem performs an implicit release on whatever the previous reference
 * was, so in a debug environment the previous must start out at NULL.  It's
 * easier to set that here than in fthlll_c.h.
 */
#define LLL_REF(ref, arg) ((ref) = NULL, LLL_SP_NAME(_rwref)(&(ref), (arg)))
#define LLL_REF_RELEASE(ref) LLL_SP_NAME(_rwrelease)(&(ref))

#undef LLL_IS_NULL
#define LLL_IS_NULL(arg) LLL_SP_NAME(_is_null)(arg)

#else /* ifdef LLL_SHMEM */
#define LLL_EL_PTR_TYPE LLL_EL_TYPE *
#define LLL_EL_REF_TYPE LLL_EL_TYPE *
#define LLL_NULL (void *) 0
#define LLL_REF(ref, arg) ((ref) = (arg))
#define LLL_REF_RELEASE(ref)

#undef LLL_IS_NULL
#define LLL_IS_NULL(arg) arg == LLL_NULL

#endif /* else LLL_SHMEM */

//
// Basic data structure for elements of locked list
typedef struct LLL_NAME(_lll_el) {
    fthSpinLock_t spin;                      // Element spin lock
    LLL_EL_PTR_TYPE next;
    LLL_EL_PTR_TYPE prev;
} LLL_NAME(_lll_el_t);

//
// Basic data structure for base of locked list
typedef struct LLL_NAME(_lll) {
    fthSpinLock_t spin;
    LLL_EL_PTR_TYPE head;                    // Elements on list
    LLL_EL_PTR_TYPE tail;
} LLL_NAME(_lll_t);

// Function definition for manual list walking
LLL_INLINE void LLL_NAME(_spinLock)(LLL_NAME(_lll_t) *ll);
LLL_INLINE void LLL_NAME(_spinUnlock)(LLL_NAME(_lll_t) *ll);
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_head)(LLL_NAME(_lll_t) *ll);
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_tail)(LLL_NAME(_lll_t) *ll);
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_next)(LLL_EL_PTR_TYPE obj);
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_prev)(LLL_EL_PTR_TYPE obj);


// Function definitions for list manipulation
LLL_INLINE void LLL_NAME(_lll_init)(LLL_NAME(_lll_t) *ll);
LLL_INLINE void LLL_NAME(_el_init)(LLL_EL_PTR_TYPE obj);
LLL_INLINE int LLL_NAME(_is_empty)(LLL_NAME(_lll_t) *ll);
LLL_INLINE void LLL_NAME(_push)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj);
LLL_INLINE void LLL_NAME(_push)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj);
LLL_INLINE void LLL_NAME(_push_nospin)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj);
LLL_INLINE void LLL_NAME(_insert_nospin)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE prevObj, LLL_EL_PTR_TYPE obj);
LLL_INLINE void LLL_NAME(_unshift)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj);
LLL_INLINE void LLL_NAME(_unshift_nospin)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj);
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_pop)(LLL_NAME(_lll_t) *ll);
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_pop_precheck)(LLL_NAME(_lll_t) *ll);
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_pop_nospin)(LLL_NAME(_lll_t) *ll);
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_shift)(LLL_NAME(_lll_t) *ll);
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_shift_precheck)(LLL_NAME(_lll_t) *ll);
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_shift_nospin)(LLL_NAME(_lll_t) *ll);
LLL_INLINE void LLL_NAME(_remove)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj);
LLL_INLINE void LLL_NAME(_remove_nospin)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj);
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_pop_lock)(LLL_NAME(_lll_t) *ll);
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_shift_lock)(LLL_NAME(_lll_t) *ll);
LLL_INLINE void LLL_NAME(_remove_lock)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj);
