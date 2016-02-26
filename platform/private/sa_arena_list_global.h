/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * Defines to make make linked list in shmem of all arenas attached to the
 * shared space
 *
 * Included before fth/fthlll.h and fthlll_c.h
 */

#include "fth/fthlllUndef.h"

#define LLL_NAME(suffix) sa_arena_global ## suffix
#define LLL_SP_NAME(suffix) sa_arena_sp ## suffix
#define LLL_EL_TYPE struct sa_arena
#define LLL_EL_FIELD global_list_entry

#define LLL_SHMEM 1
#define LLL_INLINE static __attribute__((unused))
