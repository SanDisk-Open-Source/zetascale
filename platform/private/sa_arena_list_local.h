/**
 * Defines to make make linked list in shmem of all arenas associated
 * with this process.
 *
 * Included before fth/fthlll.h and fthlll_c.h
 */

#include "fth/fthlllUndef.h"

#define LLL_NAME(suffix) sa_arena_local ## suffix
#define LLL_SP_NAME(suffix) sa_arena_sp ## suffix
#define LLL_EL_TYPE struct sa_arena
#define LLL_EL_FIELD local_list_entry

#define LLL_SHMEM 1
#define LLL_INLINE static __attribute__((unused))
