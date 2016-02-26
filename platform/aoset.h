/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef AOSET_H
#define AOSET_H 1

/*
 * File:   sdf/platform/aoset.h
 * Author: Guy Shaw
 *
 * Created on June 3, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: $
 */

/*
 * An Append-only Set (aoset) is a set of objects that can be referred to
 * later using a simple index, as well as being looked up by key.
 * Elements can be appended to the set, but are never removed.
 *
 * The fact that elements do not go away means that indices are stable;
 * that is, indices returned from aoset_add() operations remain valid
 * and can safely be used to refer to elements of the set.  However,
 * references to objects stored in the set are not necessarily stable.
 * Objects can move, if the set grows.  The aoset_* family of functions
 * never return pointers to the objects in the set, anyway, only indices.
 *
 * This is different from the way sets are typically implemented in
 * higher level languages, such as perl (and others) where references
 * to objects contained in associative arrays are stable.
 *
 * The aoset system handles the set-add operation for blobs of arbitrary size
 * and content, and knows nothing about the meaning of the elements.
 * Something to bear in mind is that the elements are not zstrings,
 * and may not be printable as strings.
 *
 * In the absence of tuning, aoset_create() and aoset_add() do something
 * reasonable.  Sets can grow, as the need arises.  But, for those who
 * know, aoset_create_tune() can take capacity planning hints, such as
 * how many objects to budget for, initially, and how fast should it be
 * allowed to grow.
 */

#include <string.h>             /* Import size_t */

#include "platform/stdlib.h"

#define AOSET_DEBUG 1

#define AOSET_SHMEM 1

typedef unsigned int uint_t;    /* Solaris only ? */

/*
 * An aoset_t (append-only set) is a completely opaque data type.
 * Users get a pointer returned by aoset_create() and pass that handle
 * to other aoset_*() functions.  Other than that, the structure
 * of an aoset_t is nobody's business.
 *
 * "struct aoset" is always an incomplete data type.
 */
struct aoset;
typedef struct aoset aoset_t;

/*
 * Ensure that generic void pointers are the same flavor as the
 * aoset code -- either shared memory references or ordinary pointers.
 */
#ifdef AOSET_SHMEM
#define GVPTR_SHMEM
#else
#undef GVPTR_SHMEM
#endif

#include "platform/gvptr.h"

typedef gvptr_t aoset_hdl_t;

/*
 * aoset_create():
 *   Create new aoset, return handle to it.
 *   It could return NULL if memory is exhausted.
 *
 * aoset_destroy():
 *   Release all resources that have been allocated to the given aoset_t.
 *   Return values:
 *     0:      Success
 *     < 0:    Negative failure code, where -n is an error code a la errno.h.
 *             Possible errors are: EINVAL
 *
 *   Note: There is no function to remove an individual element.
 *   The entire set can be destoyed, after the set is no longer needed.
 *
 * aoset_add():
 *   Add the given blob to the aoset.
 *   Return values:
 *     >= 0   The index of the element in the set.
 *     < 0    Negative failure code, where -n is an error code a la errno.h.
 *            Possible erros are: EINVAL, ENOMEM
 *
 *   If the given object is not an element of the set,
 *   then the new element is appended, and the function succeeds,
 *   returning the index of the new element.
 *
 * aoset_find():
 *   Return the index of a given object in the set.
 *   Return values:
 *     >= 0   The index of the element in the set.
 *     < 0    Negative failure code, where -n is an error code a la errno.h.
 *
 * aoset_get():
 *   Set the address and size of the blob stored in an aoset, given an index.
 *   Return values:
 *     0      Success.  The address and size have been set.
 *     < 0    Negative failure code, where -n is an error code a la errno.h.
 *
 * Failure codes:
 *     EINVAL: The given aoset_t pointer is not valid.
 *         Either we were given a pointer that was never an initialized
 *         aoset_t, or its contents has been written.
 *     ENOMEM: Could not append to set, because memory allocation failed.
 *     E2BIG:  A given index is larger than the number of elements in the set.
 *
 */


extern aoset_hdl_t aoset_create(void);
extern int aoset_destroy(aoset_hdl_t);
extern int aoset_add(aoset_hdl_t, void *obj, size_t objsize);
extern int aoset_find(aoset_hdl_t, void *obj, size_t objsize);
extern int aoset_get(aoset_hdl_t, int index, gvptr_t *objr, size_t *objsizer);

#ifdef AOSET_DEBUG

extern void aoset_debug(uint_t);
extern void aoset_diag(aoset_hdl_t);

#else

static __inline__ void aoset_debug(uint_t lvl) { }
static __inline__ void aoset_diag(aoset_hdl_t sethdl) { }

#endif

#endif /* ndef AOSET_H */
