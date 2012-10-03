#ifndef PLATFORM_ALLOC_H
#define PLATFORM_ALLOC_H 1

/*
 * File:   platform/alloc.h
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: alloc.h 13118 2010-04-22 19:08:35Z drew $
 */

/**
 * Schooner memory allocation wrappers
 */

#include <sys/types.h>
#include <stdlib.h>

#include "platform/assert.h"
#include "platform/logging.h"
/* Even for plat_shmem_fake since users may be using #plat_shmem_arena  */
#include "platform/shmem_arena.h"
/*
 * XXX: drew 2008-12-02 As a compile time option this is making substantially
 * less sense because the current shared memory implementation has cost
 * below the measurement noise floor and the allocator can be replaced at
 * run time.
 *
 * With an increasing entanglement between real and test behaviors and
 * shared memory properties (for example the client->agent interface
 * used by all the SDF tests; the integration between memory allocator
 * and caching code for reasonable evictions; etc.) we should probably
 * do away with this since it's making maintenance more time consuming.
 */
#ifndef PLAT_SHMEM_FAKE
#include "platform/shmem.h"
#endif
#include "platform/wrap.h"

#define sys_malloc malloc
#define sys_calloc calloc
#define sys_realloc realloc
#define sys_free free

extern void *bwo_calloc(size_t nmemb, size_t size);
extern void *bwo_malloc(size_t size);
extern void bwo_free(void *ptr);
extern void *bwo_realloc(void *ptr, size_t size);

// #define sys_malloc bwo_malloc
// #define sys_calloc bwo_calloc
// #define sys_realloc bwo_realloc
// #define sys_free bwo_free

/* XXX: flash has fields called free; the protocol lex code #defines malloc */
#ifdef notyet
PLAT_WRAP_CPP_POISON(malloc calloc realloc free)
#endif

/*
 * XXX: drew 2008-09-03
 *
 * This is gross. The #plat_alloc macros are thin wrappers adding
 * logging to implementation macros which are defined differently depending
 * on whether shmem has been compiled out.
 *
 * Macros are used instead of inline functions so that we have
 * __FILE__, __LINE__, etc. information available to logging.
 */

#if defined(PLAT_SHMEM_FAKE)
#define plat_alloc_impl(arg_size) \
    sys_malloc(arg_size)

#define plat_alloc_arena_impl(arg_size, arena) \
    sys_malloc(arg_size)

#define plat_calloc_impl(arg_nmemb, arg_size) \
    sys_calloc((arg_nmemb), (arg_size))

#define plat_realloc_impl(arg_ptr, arg_size) \
    sys_realloc((arg_ptr), (arg_size))

#define plat_free_impl(arg_ptr) \
    sys_free(arg_ptr)

#define plat_alloc_steal_from_heap_impl(arg_size) \
    sys_malloc(arg_size)

#elif defined(PLAT_UNIFIED_ALLOC)

#define plat_alloc_impl(arg_size) \
    ({                                                                         \
     plat_shmem_alloc_helper(__FILE__,  __LINE__, __FUNCTION__,                \
                             PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC, (arg_size),   \
                             PLAT_SHMEM_ARENA_DEFAULT,                         \
                             PLAT_SHMEM_ALLOC_LOCAL).ptr;                      \
     })

#define plat_alloc_arena_impl(arg_size, arg_arena) \
    ({                                                                         \
     plat_shmem_alloc_helper(__FILE__,  __LINE__, __FUNCTION__,                \
                             PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC, (arg_size),   \
                             (arg_arena),                                      \
                             PLAT_SHMEM_ALLOC_LOCAL).ptr;                      \
     })

#define plat_calloc_impl(arg_nmemb, arg_size) \
    ({                                                                         \
     void *ret;                                                                \
                                                                               \
     ret = plat_shmem_alloc_helper(__FILE__,  __LINE__, __FUNCTION__,          \
                                   PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC,         \
                                   size, PLAT_SHMEM_ARENA_DEFAULT,             \
                                   PLAT_SHMEM_ALLOC_LOCAL).ptr;                \
     if (ret) {                                                                \
         memset(ret, 0, size);                                                 \
     }                                                                         \
                                                                               \
     ret;                                                                      \
    })

#define plat_realloc_impl(arg_ptr, arg_size) \
    ({                                                                         \
     void *ret;                                                                \
                                                                               \
     size_t size = (arg_size);                                                 \
     plat_shmem_ptr_base_t ptr;                                                \
     ptr.ptr = (arg_ptr);                                                      \
                                                                               \
     ret = plat_shmem_alloc_helper(__FILE__,  __LINE__, __FUNCTION__,          \
                                   PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC,         \
                                   size, PLAT_SHMEM_ARENA_DEFAULT,             \
                                   PLAT_SHMEM_ALLOC_LOCAL).ptr;                \
                                                                               \
     if (ret) {                                                                \
         memcpy(ret, ptr.ptr, size);                                           \
         plat_shmem_free_helper(__FILE__, __LINE__, __FUNCTION__,              \
                                PLAT_LOG_CAT_PLATFORM_ALLOC_FREE,              \
                                ptr, 0 /* unknown */, 1 /* free count */);     \
     }                                                                         \
                                                                               \
     ret;                                                                      \
    })

#define plat_free_impl(arg_ptr) \
    do {                                                                       \
        plat_shmem_ptr_base_t ptr;                                             \
        ptr.ptr = arg_ptr;                                                     \
                                                                               \
        plat_shmem_free_helper(__FILE__,  __LINE__, __FUNCTION__,              \
                               PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC, ptr,        \
                               0 /* size */, 1 /* free count */);              \
    } while (0)

#define plat_alloc_steal_from_heap_impl(arg_size) \
    plat_shmem_steal_from_heap(arg_size).ptr

#else /* #elif defined(PLAT_UNIFIED_ALLOC) */

enum plat_alloc_method {
    /*
     * FIXME: Should be paranoid and have a third state which causes asserts
     * for allocation before intitialization.
     */

    /** @brief Plat alloc uses system malloc always */
    PLAT_ALLOC_MALLOC,
    /** @brief Arena specification determines - non default isn't malloc */
    PLAT_ALLOC_BY_ARENA,
    /** @brief Allocate from physmem/shmem */
    PLAT_ALLOC_PHYSMEM,
    /** @brief Fail, because subsystem is no (or no longer) initialized */
    PLAT_ALLOC_UNINITIALIZED
};

extern enum plat_alloc_method plat_alloc_method;

#define plat_alloc_impl(arg_size) \
    ({                                                                         \
     plat_assert(plat_alloc_method != PLAT_ALLOC_UNINITIALIZED);               \
                                                                               \
     ((plat_alloc_method == PLAT_ALLOC_BY_ARENA ||                             \
       plat_alloc_method == PLAT_ALLOC_MALLOC) ? sys_malloc(arg_size) :        \
      plat_shmem_alloc_helper(__FILE__,  __LINE__, __FUNCTION__,               \
                              PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC, (arg_size),  \
                              PLAT_SHMEM_ARENA_DEFAULT,                        \
                              PLAT_SHMEM_ALLOC_LOCAL).ptr);                    \
     })

#define plat_alloc_arena_impl(arg_size, arg_arena) \
    ({                                                                         \
     plat_assert(plat_alloc_method != PLAT_ALLOC_UNINITIALIZED);               \
                                                                               \
     ((plat_alloc_method == PLAT_ALLOC_MALLOC ||                               \
       (plat_alloc_method == PLAT_ALLOC_BY_ARENA &&                            \
        (arg_arena) == PLAT_SHMEM_ARENA_DEFAULT)) ? sys_malloc(arg_size) :     \
      plat_shmem_alloc_helper(__FILE__,  __LINE__, __FUNCTION__,               \
                              PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC, (arg_size),  \
                              (arg_arena),                                     \
                              PLAT_SHMEM_ALLOC_LOCAL).ptr);                    \
     })

#define plat_calloc_impl(arg_nmemb, arg_size) \
    ({                                                                         \
     void *ret;                                                                \
                                                                               \
     plat_assert(plat_alloc_method != PLAT_ALLOC_UNINITIALIZED);               \
                                                                               \
     if (plat_alloc_method == PLAT_ALLOC_BY_ARENA ||                           \
         plat_alloc_method == PLAT_ALLOC_MALLOC) {                             \
         ret = sys_calloc((arg_nmemb), (arg_size));                            \
     } else {                                                                  \
         size_t size = (arg_nmemb) * (arg_size);                               \
                                                                               \
         ret = plat_shmem_alloc_helper(__FILE__,  __LINE__, __FUNCTION__,      \
                                       PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC,     \
                                       size,                                   \
                                       PLAT_SHMEM_ARENA_DEFAULT,               \
                                       PLAT_SHMEM_ALLOC_LOCAL).ptr;            \
         if (ret) {                                                            \
             memset(ret, 0, size);                                             \
         }                                                                     \
    }                                                                          \
                                                                               \
    ret;                                                                       \
    })

#define plat_realloc_impl(arg_ptr, arg_size) \
    ({                                                                         \
     void *ret;                                                                \
                                                                               \
     plat_assert(plat_alloc_method != PLAT_ALLOC_UNINITIALIZED);               \
                                                                               \
     if (plat_alloc_method == PLAT_ALLOC_BY_ARENA ||                           \
         plat_alloc_method == PLAT_ALLOC_MALLOC) {                             \
        ret = sys_realloc((arg_ptr), (arg_size));                              \
     } else {                                                                  \
        size_t size = (arg_size);                                              \
        plat_shmem_ptr_base_t ptr;                                             \
        ptr.ptr = (arg_ptr);                                                   \
                                                                               \
        ret = plat_shmem_alloc_helper(__FILE__,  __LINE__, __FUNCTION__,       \
                                      PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC,      \
                                      size,                                    \
                                      PLAT_SHMEM_ARENA_DEFAULT,                \
                                      PLAT_SHMEM_ALLOC_LOCAL).ptr;             \
                                                                               \
        if (ret) {                                                             \
            memcpy(ret, ptr.ptr, size);                                        \
            plat_shmem_free_helper(__FILE__, __LINE__, __FUNCTION__,           \
                                   PLAT_LOG_CAT_PLATFORM_ALLOC_FREE,           \
                                   ptr, 0 /* unknown */, 1 /* free count */);  \
        }                                                                      \
    }                                                                          \
                                                                               \
    ret;                                                                       \
    })

#define plat_free_impl(arg_ptr) \
    do {                                                                       \
        plat_assert(plat_alloc_method != PLAT_ALLOC_UNINITIALIZED);            \
                                                                               \
        if (plat_alloc_method == PLAT_ALLOC_MALLOC) {                          \
            sys_free(arg_ptr);                                                 \
        } else {                                                               \
            plat_shmem_ptr_base_t ptr;                                         \
            ptr.ptr = arg_ptr;                                                 \
                                                                               \
            plat_shmem_free_helper(__FILE__,  __LINE__, __FUNCTION__,          \
                                   PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC, ptr,    \
                                   0 /* size */, 1 /* free count */);          \
        }                                                                      \
    } while (0)

#define plat_alloc_steal_from_heap_impl(arg_size) \
    ({                                                                         \
        void *ret;                                                             \
        plat_assert(plat_alloc_method != PLAT_ALLOC_UNINITIALIZED);            \
                                                                               \
        if (plat_alloc_method == PLAT_ALLOC_MALLOC) {                          \
            ret = sys_malloc(arg_size);                                        \
        } else {                                                               \
            ret = plat_shmem_steal_from_heap(arg_size).ptr;                    \
        }                                                                      \
        ret;                                                                   \
    })

#endif /* else def PLAT_SHMEM_FAKE */

/* Reference arguments exactly once to avoid side effects */

#define plat_alloc(arg_size) \
({                                                                             \
 size_t alloc_size = (arg_size);                                               \
 void *alloc_ret = plat_alloc_impl(alloc_size);                                \
 plat_log_msg(21851, PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC,         \
              PLAT_LOG_LEVEL_TRACE_LOW,                                        \
              "plat_alloc(%llu) returned %p from %p",                          \
              (unsigned long long)alloc_size, alloc_ret,                       \
              __builtin_return_address(0));                                    \
 alloc_ret;                                                                    \
 })

#define plat_alloc_arena(arg_size, arg_arena) \
({                                                                             \
 size_t alloc_size = (arg_size);                                               \
 enum plat_shmem_arena alloc_arena = (arg_arena);                              \
 void *alloc_ret = plat_alloc_arena_impl(alloc_size, alloc_arena);             \
 plat_log_msg(20897, PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC,         \
              PLAT_LOG_LEVEL_TRACE_LOW,                                        \
              "plat_alloc_arena(%llu, %s) returned %p",                        \
              (unsigned long long)alloc_size,                                  \
              plat_shmem_arena_to_str(alloc_arena), alloc_ret);                \
 alloc_ret;                                                                    \
 })



#define plat_malloc(arg_size) plat_alloc(arg_size)

#define plat_calloc(arg_nmemb, arg_size) \
({                                                                             \
 size_t alloc_nmemb = (arg_nmemb);                                             \
 size_t alloc_size = (arg_size);                                               \
 void *alloc_ret = plat_calloc_impl(alloc_nmemb, alloc_size);                  \
 plat_log_msg(20898, PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC,         \
              PLAT_LOG_LEVEL_TRACE,                                            \
              "plat_calloc(%llu, %llu) returned %p",                           \
              (unsigned long long)alloc_nmemb, (unsigned long long)alloc_size, \
              alloc_ret);                                                      \
 alloc_ret;                                                                    \
 })

#define plat_realloc(arg_ptr, arg_size) \
({                                                                             \
 size_t alloc_size = (arg_size);                                               \
 void *alloc_ptr = (arg_ptr);                                                  \
 void *alloc_ret = plat_realloc_impl(alloc_ptr, alloc_size);                   \
 plat_log_msg(20899, PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC,         \
              PLAT_LOG_LEVEL_TRACE,                                            \
              "plat_realloc(%p, %llu) returned %p", alloc_ptr,                 \
              (unsigned long long)alloc_size, alloc_ret);                      \
 alloc_ret;                                                                    \
 })

#define plat_free(arg_ptr) \
 ({                                                                            \
  void *alloc_ptr = (arg_ptr);                                                 \
  plat_log_msg(21850, PLAT_LOG_CAT_PLATFORM_ALLOC_FREE,          \
               PLAT_LOG_LEVEL_TRACE_LOW,                                       \
               "plat_free(%p) from %p", alloc_ptr,                             \
               __builtin_return_address(0));                                   \
  plat_free_impl(alloc_ptr);                                                   \
  })

/**
 * @brief Steal memory from heap.
 *
 * This is desireable for large tables allocated at startup since it allows
 * using huge pages which result in a lower TLB miss rate with improved (we
 * observed 20% in SDF API testing) performance.
 *
 * It comes at the expense of not being able to free stolen memory.
 */
#define plat_alloc_steal_from_heap(arg_size) \
 ({                                                                            \
  size_t alloc_size = (arg_size);                                              \
  void *alloc_ret = plat_alloc_steal_from_heap_impl(alloc_size);               \
                                                                               \
  plat_log_msg(20901, PLAT_LOG_CAT_PLATFORM_ALLOC_MALLOC,        \
               PLAT_LOG_LEVEL_TRACE,                                           \
               "plat_alloc_steal_from_heap(%llu) returned %p",                 \
               (unsigned long long)alloc_size, alloc_ret);                     \
  alloc_ret;                                                                   \
 })

#define plat_alloc_struct(out) (*(out) = plat_alloc(sizeof (**(out))))
#define plat_malloc_struct(out) (*(out) = plat_alloc(sizeof (**(out))))
#define plat_calloc_struct(out) (*(out) = plat_calloc(1, sizeof (**(out))))

#define plat_sys_malloc_struct(out) (*(out) = sys_malloc(sizeof (**(out))))
#define plat_sys_calloc_struct(out) (*(out) = sys_calloc(1, sizeof (**(out))))

#endif /* def PLATFORM_ALLOC_H */
