#ifndef PLATFORM_SHMEM_H
#define PLATFORM_SHMEM_H 1

/*
 * File:   sdf/platform/shmem.h
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmem.h 13588 2010-05-12 01:50:47Z drew $
 */

/**
 * The shmem subsystem provides crash-consistent (fail-stop in initial
 * releases) access to shared memory for caching and IPC between user
 * processes, kernel scheduled user threads, user scheduled (non-premptive)
 * threads, and perhaps kernel subsystems.
 *
 * Users can request memory which is guaranteed to have fixed physical
 * mappings until freed.  Other memory may have valid physical mappings
 * but this is not guaranteed.
 *
 * Logging is to platform/shmem/alloc/name (2008-03-18) and
 * platform/shmem/reference/name (when a use case requires)
 *
 * There are four basic types of shared memory pointers defined by different
 * macros:
 *    PLAT_SP: To fixed-size types (2008-02-12)
 *
 *    PLAT_SP_VAR_OPAQUE: To variable sized types with externally specified
 *        sizes. PLAT_SP_VAR_OPAQUE(your_name, void) provides a pointer to
 *        raw memory (say for cached objects) that can't be accidentally
 *        confused with other pointers to raw memory. (2008-02-20)
 *
 *    PLAT_SP_VAR_DATA: To variable-sized types with data-specified size, as in
 *        a fixed sized header followed by variable data like string_t.
 *        (when a use case requires)
 *
 *    PLAT_SP_ARRAY: To arrays of fixed sized objects. (when a use case
 *        requires)
 *
 * The subsystem is layered, with the layers including
 * - Shared memory (segment management, attach, detach, well known
 *     pointers, failure handling, translation to local pointers) (2008-02-12)
 *
 * - Allocation (simple allocate, free) (2008-02-12)
 *
 * Crash consistent operation will require tendrils extending into the
 * threading code (user scheduled thread ready queues must be in shared memory
 * for wakeups across process boundaries, kernel scheduled threads have an
 * action state field in shared memory for crash recovery, lock state is also
 * in shared memory...).
 *
 * Shared pointers are an opaque typedef since we want to force the use
 * of _ref and _release functions so that debugging runs can operate with
 * memory protection on pointers that should be unreferenced.
 *
 * XXX: drew 2008-10-03 I punted on shmemd since we're not going to be
 * failstop for a while
 *
 * Communication between shmem client and shmemd via a socket with
 * messages only exchanged during startup and abnormal situations such as
 * pool growth and suicide messages for the initial fail-stop implementation.
 *
 * Connection is established via a connection oriented unix domain socket since
 *
 * 1.  The file system entry for the reverse socket (used on shmemd restart)
 *     can be flock(2)'d so that process death can be detected across a
 *     shmemd restart regardless of pid reuse (Linux defaults to 16 bit
 *     pids) or whether shmemd has permissions to kill(pid, 0).
 *
 * 2.  This makes simulation simpler because "node" namespace can
 *     start anywhere in the filesystem and still be well-known.
 *
 * 3.  Unix domain sockets can be used to pass file descriptors so shmemd
 *     for access control without client processes sharing uid/gid.
 *
 * See also docs/memory.txt for the original strawman.
 *
 * With a 64 bit machine, the physical address space can be mapped
 * (sparsley) into a fixed range which gets used for allocations
 * shared with hardware with simple arithmetic to yield physical
 * or kernel space addresses.
 *
 * Large (> 4M) virtual allocations can share the same memory pool
 * by mapping the same memory into a contiguous address spaces.
 *
 * User defines:
 *
 * PLAT_SHMEM_DEBUG - enable more expensive debugging primitives
 * PLAT_SHMEM_FAKE - replace shmem with local allocation
 * PLAT_SHMEM_MAP - fixed address at which shmem is mapped.  Implies that
 *                  PLAT_SHMEM_ADDRESS_SPACE is also defined
 * PLAT_SHMEM_ADDRESS_SPACE - finite size for shared memory address space
 */

#include <limits.h>
#include <stddef.h>

#include "platform/assert.h"

#include "platform/defs.h"
/*  XXX: just for inline #plat_shmem_config_parse_file */
#include "platform/errno.h"
#include "platform/logging.h"
/*  XXX: just for inline #plat_shmem_config_parse_file */
#include "misc/misc.h"
#include "platform/once.h"
#include "platform/shmem_arena.h"
/*  XXX: just for inline #plat_shmem_config_parse_file */
#include "platform/string.h"
#include "platform/types.h"

#define PLAT_OPTS_SHMEM(config_field)                                          \
    item("plat/shmem/file", "shmem file", PLAT_SHMEM_FILE,                     \
         plat_shmem_config_parse_file(&config->config_field, optarg),          \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("plat/shmem/physmem", "shmem physmem device", PLAT_SHMEM_PHYSMEM,     \
         plat_shmem_config_add_backing_physmem(&config->config_field, optarg), \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    /* XXX: drew  2008-09-02 temporary */                                      \
    item("plat/shmem/physmem_virt", "shmem physmem device",                    \
         PLAT_SHMEM_PHYSMEM_VIRT,                                              \
         plat_shmem_config_add_backing_physmem_virt(&config->config_field,     \
                                                    optarg),                   \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("plat/shmem/arena", "arena configuration arena_name.param=value",     \
         PLAT_SHMEM_ARENA,                                                     \
         plat_shmem_config_parse_arena(&config->config_field, optarg),         \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("plat/shmem/sim_phys", "simulate physical memory",                    \
         PLAT_SHMEM_SIM_PHYS,                                                  \
         parse_size(&config->config_field.sim_phys, optarg, NULL),             \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("plat/shmem/debug/alloc", "debug shmem alloc", PLAT_SHMEM_DEBUG_ALLOC,\
         ({                                                                    \
          config->config_field.flags |= PLAT_SHMEM_CONFIG_DEBUG_ALLOC; 0;      \
          }), PLAT_OPTS_ARG_NO)                                                \
    item("plat/shmem/debug/ref", "debug shmem reference", PLAT_SHMEM_DEBUG_REF,\
         ({                                                                    \
          config->config_field.flags |= PLAT_SHMEM_CONFIG_DEBUG_REFERENCE; 0;  \
          }), PLAT_OPTS_ARG_NO)                                                \
    item("plat/shmem/debug/poison", "poison freed memory",                     \
         PLAT_SHMEM_DEBUG_POISON,                                              \
         ({                                                                    \
          plat_shmem_config_set_flags(&config->config_field,                   \
                                      PLAT_SHMEM_CONFIG_DEBUG_POISON); 0;      \
          }), PLAT_OPTS_ARG_NO)                                                \
    /* NOTE: This is only for test programs, since it makes shmem not shared */\
    item("plat/shmem/debug/local_alloc", "use malloc instead of shmem",        \
         PLAT_SHMEM_DEBUG_LOCAL_ALLOC,                                         \
         ({                                                                    \
          plat_shmem_config_set_flags(&config->config_field,                   \
                                      PLAT_SHMEM_CONFIG_DEBUG_LOCAL_ALLOC); 0; \
          }), PLAT_OPTS_ARG_NO)                                                \
    item("plat/shmem/debug/replace_malloc", "replace malloc with shmem",       \
         PLAT_SHMEM_DEBUG_REPLACE_MALLOC,                                      \
         ({                                                                    \
          plat_shmem_config_set_flags(&config->config_field,                   \
                                      PLAT_SHMEM_CONFIG_DEBUG_REPLACE_MALLOC); \
          0;                                                                   \
          }), PLAT_OPTS_ARG_NO)                                                \
    item("plat/shmem/debug/no_replace_malloc", "do not replace malloc",        \
         PLAT_SHMEM_DEBUG_NO_REPLACE_MALLOC,                                   \
         ({                                                                    \
          plat_shmem_config_clear_flags(&config->config_field,                 \
                                        PLAT_SHMEM_CONFIG_DEBUG_REPLACE_MALLOC); \
          0;                                                                   \
          }), PLAT_OPTS_ARG_NO)                                                \
    item("plat/shmem/pthread_locks", "use pthread locks",                      \
         PLAT_SHMEM_PTHREAD_LOCKS,                                             \
         ({                                                                    \
          plat_shmem_config_set_flags(&config->config_field,                   \
                                      PLAT_SHMEM_CONFIG_PTHREAD_LOCKS); 0;     \
          }), PLAT_OPTS_ARG_NO)                                                \
    item("plat/shmem/address_space", "address space",                          \
         PLAT_SHMEM_ADDRESS_SPACE,                                             \
         parse_size(&config->config_field.address_space, optarg, NULL),        \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("plat/shmem/base", "base address",                                    \
         PLAT_SHMEM_BASE_ADDRESS,                                              \
         parse_uint64(&config->config_field.base_address, optarg, NULL),       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("plat/shmem/alloc_large",                                             \
         "use guma for all allocations (large allocations leak)",              \
         PLAT_SHMEM_ALLOC_LARGE,                                               \
         ({                                                                    \
          plat_shmem_config_set_flags(&config->config_field,                   \
                                      PLAT_SHMEM_CONFIG_ALLOC_LARGE); 0;       \
          }), PLAT_OPTS_ARG_NO)                                                \
    item("plat/shmem/prefault", "prefault pages", PLAT_SHMEM_PREFAULT,         \
         ({                                                                    \
          plat_shmem_config_set_flags(&config->config_field,                   \
                                      PLAT_SHMEM_CONFIG_PREFAULT); 0;          \
          }), PLAT_OPTS_ARG_NO)                                                \
    item("plat/shmem/retain_address_space", "retain address space",            \
         PLAT_SHMEM_RETAIN_ADDRESS_SPACE,                                      \
         ({                                                                    \
          plat_shmem_config_set_flags(&config->config_field,                   \
                                      PLAT_SHMEM_CONFIG_RETAIN_ADDRESS_SPACE); \
          0; }), PLAT_OPTS_ARG_NO)


__BEGIN_DECLS

#define PLAT_SHMEM_CONFIG_FLAGS_ITEMS() \
    /** @brief Debug allocations */                                            \
    item(DEBUG_ALLOC, 1 << 0)                                                  \
    /** @brief Debug references */                                             \
    item(DEBUG_REFERENCE, 1 << 1)                                              \
    /** @brief poison free memory */                                           \
    item(DEBUG_POISON, 1 << 3)                                                 \
    /** @brief Use local malloc, free (doesn't work across processes) */       \
    item(DEBUG_LOCAL_ALLOC, 1 << 4)                                            \
    /** @brief Use pthread locking */                                          \
    item(PTHREAD_LOCKS, 1 << 5)                                                \
    /** @brief Use local pointers (doesn't work across processes yet) */       \
    item(DEBUG_REPLACE_MALLOC, 1 << 6)                                         \
    /** @brief Use only a single memory allocation arena for everything */     \
    item(DEBUG_ONE_ARENA, 1 << 7)                                              \
    /** @brief Allocate large items in unified memory (can leak) */            \
    item(ALLOC_LARGE, 1 << 8)                                                  \
    /** @brief Prefault pages.  Use for normal operation. */                   \
    item(PREFAULT, 1 << 9)                                                     \
    /** @brief Retain entire address space for growth */                       \
    item(RETAIN_ADDRESS_SPACE, 1 << 10)


enum plat_shmem_config_flags {
#define item(caps, val) PLAT_SHMEM_CONFIG_ ## caps = (val),
    PLAT_SHMEM_CONFIG_FLAGS_ITEMS()
#undef item
};

/**
 * @brief Misc. shmem constants
 */
enum {
    PLAT_SHMEM_DEFAULT_SIZE = 16 * 1024 * 1024ULL,

    /**
     * @brief Current poisoning byte.  Like dmalloc
     */
    PLAT_SHMEM_POISON_BYTE = 0xda,

    /**
     * @brief address space size
     *
     * This is somewhat arbitarary, although twice this plus text, data,
     * bss, and heap all need to fit into the lower half of the x86_64
     * address space.  Try 256G.
     */
#ifdef notyet
    PLAT_SHMEM_DEFAULT_ADDRESS_SPACE = 256ULL << 30,
#else
    /*
     * XXX: drew 2008-10-06
     * 2-year old Linux kernels take tens of seconds to allocate
     * large address spaces.
     *
     * Use something smaller until that's fixed
     */
    PLAT_SHMEM_DEFAULT_ADDRESS_SPACE = 16ULL << 30,
#endif

    /*
     * VALGRIND fails on the large mmaps of /dev/zero we use to get address
     * space without memory.  Kludge around that until we mate valgrind
     * with real physmem or large installations.
     */
    PLAT_SHMEM_DEFAULT_VALGRIND_ADDRESS_SPACE = 2ULL << 30

};

/**
 * @brief Shared memory configuration
 *
 * Direct access to this is deprecated; treat this as an opaque structure
 * and use setter functions like #plat_shmem_config_add_backing_file.
 */
struct plat_shmem_config {
#ifndef DEPRECATED
    const char *mmap;
    size_t size;
#endif

    /** @brief plat_shmem_config_flags */
    uint32_t flags;

    /** @brief Requested address space size */
    int64_t address_space;

    /** @brief Requested base address for mapping */
    uint64_t base_address;

    /** @brief Physical memory base for simulation purpopses */
    int64_t sim_phys;

    /** @brief Guts of the configuration code */
    struct shmem_config_opaque *opaque;
};

/*
 * Most statistics are signed so that aggregating local usage numbers
 * which may be negative directly in a stats structure produces
 * results that are readable when debugging.
 */

/*
 * item(type, name)
 */
#define PLAT_SHMEM_STAT_BUCKET_ITEMS()                                         \
    /** @brief Current number of allocated objects */                          \
    item(int64_t, allocated_count)                                             \
    /** @brief Current total allocated object size (user requested) in bytes */\
    item(int64_t, allocated_bytes)                                             \
    /**                                                                        \
     * @brief Total number of bytes used, including allocation headers         \
     *                                                                         \
     * This includes internal overhead such as headers and rounding.           \
     */                                                                        \
    item(int64_t, used_bytes)                                                  \
    /** @brief Total number of calls to alloc */                               \
    item(uint64_t, total_alloc)                                                \
    /** @brief Total number of calls to free */                                \
    item(uint64_t, total_free)

#define PLAT_SHMEM_STAT_ARENA_ITEMS()                                          \
    item(int64_t, tree_size)

#define PLAT_SHMEM_STAT_ROOT_ITEMS()                                           \
    /**                                                                        \
     * @brief Total number of bytes which are unusable                         \
     *                                                                         \
     * This includes bytes that will never be freed because the simple         \
     * sbrk-like allocation scheme skipped to another segment.                 \
     *                                                                         \
     * XXX: drew 2008-11-20 This increases when processes terminate,           \
     * because the slab allocators don't return unused slab to the root        \
     * arena free list on process termination.                                 \
     */                                                                        \
    item(uint64_t, unusable_bytes)                                             \
    /** @brief Total number of bytes in memory pool. */                        \
    item(uint64_t, total_bytes)                                                \
    /** @brief Number of bytes returned by #plat_shmem_steal_from_heap */      \
    item(uint64_t, stolen_bytes)


#define PLAT_SHMEM_STAT_ITEMS()                                                \
    PLAT_SHMEM_STAT_BUCKET_ITEMS()                                             \
    PLAT_SHMEM_STAT_ARENA_ITEMS()                                              \
    PLAT_SHMEM_STAT_ROOT_ITEMS()

/**
 * Shared memory statistics.
 *
 * Name based stats interface should probably be used instead.
 */
struct plat_shmem_alloc_stats {
#define item(type, name) type name;
    PLAT_SHMEM_STAT_ITEMS()
#undef item
};

/**
 * @brief Flags passed in to the type specific name_ ##  alloc_helper
 * function and ultimately #plat_shmem_alloc_helper.
 */
enum plat_shmem_alloc_helper_flags {
    /** @brief Physical mapping requested for DMA */
    PLAT_SHMEM_ALLOC_PHYS = 1 << 0,

    /**
     * @brief Internal allocation (as for debug back trace).
     *
     * Avoid a spiral of death.
     */
    PLAT_SHMEM_ALLOC_INTERNAL = 1 << 1,

    /**
     * @brief Allocation from non-shared plat_alloc
     *
     * Implies return must be a normal pointer
     */
    PLAT_SHMEM_ALLOC_LOCAL = 1 << 2
};

/**
 * Actual shared memory pointer definition.  Wrappers are used for type
 * safety.
 *
 * Making this 64 bits allows a thread's control structure shared memory
 * address to uniquely identify the thread and serve as a spinlock value
 * set via cmpxchg8b so that things are more easily debugged.
 *
 * To match expected behavior, the null pointer can be represented with
 * all zero bits.
 *
 * static shmem_ptr_base foo_ptr;
 *
 * is null as expected.
 *
 * NOTE: users should treat this as opaque.  Field size and meaning will
 * change
 */
typedef struct plat_shmem_ptr_base {
    union {
        /** @brief For compare and swap use */
        uint64_t int_base;

        /** @brief Local representation with clear PLAT_SHMEM_FLAG_NOT_LOCAL */
        void *ptr;
    };
} plat_shmem_ptr_base_t;

/** @brief Physical address typedef */
typedef size_t plat_shmem_paddr_t;

#define PLAT_SHMEM_CONFIG_OPT_ITEMS(config_field)

/**
 * @brief Format string for printf.
 *
 * @code
 * PLAT_SP(proc_ptr, struct plat_process);
 *
 * proc_ptr proc;
 * printf("Process " PLAT_SP_FMT " has died\n", PLAT_SP_FMT_ARG(proc));
 * @endcode
 */
#define PLAT_SP_FMT "%p"

/**
 * @brief Printf arg string
 *
 * @code
 * PLAT_SP(proc_ptr, struct plat_process);
 *
 * proc_ptr proc;
 * printf("Process " PLAT_SP_FMT " has died\n", PLAT_SP_FMT_ARG(proc));
 * @endcode
 */
#define PLAT_SPB_FMT_ARG(ptr_arg) \
    (ptr_arg).ptr

#define PLAT_SP_FMT_ARG(ptr) \
    PLAT_SPB_FMT_ARG((ptr).base)

#ifdef PLAT_SHMEM_DEPRECATE
#define PLAT_SP_DEPRECATED __attribute__((deprecated))
#else
#define PLAT_SP_DEPRECATED
#endif

/*
 * Hooks for creating debugger friendly (out-of-line functions so break points
 * can be set, don't hint to compiler about what function calls can be
 * eliminated) and performance friendly versions of the code.
 *
 * GCC provides an extension
 * extern inline __attribute__((gnu_inline))
 * which emits an inline function definition when optimization is turned
 * on, and acts as a simple out-of-line extern declaration otherwise.
 *
 * This allows for source which is both easily debugged and performant.
 *
 * Problem 1:
 *
 * GCC sometimes limits the depth of what could be in-lined with
 * extern inline versus staic inline.  Switching to static inline
 * avoids this problem.
 *
 * Problem 2:
 *
 * GCC 4.3 complains when an extern inline function references a static
 * inline function which is bogus.
 *
 * Example:
 * error: ‘plat_shmem_get_attached’ is static but used in inline function
 * ‘plat_shmem_ptr_base_to_paddr’ which is not static
 *
 * The work-around is that all in-line functions must have the same form
 * which needs to be static inline.  Since this affects consumers of
 * shmem.h it has to be handled at a more common level like plat.
 */

#ifdef PLAT_SHMEM_DEBUG
#define PLAT_SHMEM_CONST
#define PLAT_SHMEM_PURE
#define PLAT_SHMEM_RELEASE(localptr) *(localptr) = NULL
#define plat_shmem_debug_assert(x) plat_assert(x)
#define plat_shmem_debug_assert_iff(a, b) plat_assert_iff(a, b)
#else
#define PLAT_SHMEM_CONST __attribute__((const))
#define PLAT_SHMEM_PURE __attribute__((pure))
#define plat_shmem_debug_assert(x)
#define plat_shmem_debug_assert_iff(a, b)
#define PLAT_SHMEM_RELEASE(localptr)
#endif

/* This is more readable with the spaces */
/* BEGIN CSTYLED */
#ifdef PLAT_SHMEM_MAP
#   ifndef PLAT_SHMEM_ADDRESS_SPACE
#       error "PLAT_SHMEM_ADDRESS_SPACE must be set with PLAT_SHMEM_MAP"
#   else /* ndef PLAT_SHMEM_ADDRESS_SPACE */
#       define PLAT_SHMEM_PHYS_MAP (PLAT_SHMEM_MAP)
#       define PLAT_SHMEM_VIRT_MAP ((PLAT_SHMEM_MAP) + (PLAT_SHMEM_ADDRESS_SPACE))
#   endif /* else def PLAT_SHMEM_MAP */
#endif /* def PLAT_SHMEM_MAP */
/* END CSTYLED */


/**
 * Declare shared pointer name_t to fixed length structure.
 *
 * Declares
 *     name_t name_alloc();
 *         DEPRECATED because this usage precludes including CPP determined
 *         file, line tuples in diagnostics.  Use #plat_shmem_alloc.
 *
 *         Allocates a single type_name.  Returns name_null on failure setting
 *         plat_errno.  Failures include ENOMEM.
 *
 *     name_t name_array_alloc(size_t nelm);
 *         DEPRECATED because this usage precludes including CPP determined
 *         file, line tuples in diagnostics.  Use #plat_shmem_array_alloc
 *
 *         Allocate an nelm array of typename.  Returns name_null on failure
 *         setting plat_errno.  Failures include ENOMEM.
 *
 *     void name_free(name_t)
 *         Free a name_t.  name_free(name_null) is legal like free(3).
 *         name_free(name_t) is not legal when name_t points to an array
 *         of name_t; name_array_free() must be used in that case as delete[]
 *         is used for C++ arrays.  Some failures (free pointer to sub-object)
 *         trap.
 *
 *     void name_array_free(name_t, size_t nelm)
 *         Free an array.  Some failures trap.  Some do not.
 *
 *     const type_name *name_rref(const type_name **local, name_t shared)
 *         Reference the shared memory for read access storing the local
 *         pointer in *local.  The user should not assign *local to another
 *         pointer.  *local must be null on the first call since an implicit
 *         release is performed on its contents.  References shall not cross
 *         function boundaries.
 *
 *         The reference/release model allows the shared pointer code to be
 *         smart about protecting "unaccessable" objects in debug mode.  The
 *         pointer to pointer interface allows specific references to be
 *         tracked.
 *
 *     void name_rrelease(const type_name **local);
 *         Release the local read-only reference *local.  The corresponding
 *         shared pointer is not required because that would not
 *         be convienent and make for a messy re-assign semantic.
 *
 *     void name_rwrelease(type_name **local)
 *         Release read-write reference.
 *
 *     const name_t name_null;
 *         Type-safe null pointer
 *
 *     int name_is_null(name_t ptr);
 *         Return true if ptr is null.
 *
 *     int name_eq(name_t lhs, name_t rhs);
 *         Return true if lhs == rhs
 *
 *     int name_cmp(name_t lhs, name_t rhs);
 *         Return -1 if lhs < rhs, 0 if lhs == rhs, > 1 if lhs > rhs
 *
 * type_name does not need to be complete at the time of declaration.
 *
 * The user must separately instantiate the pointer methods by invoking
 *    PLAT_SP_IMPL(name, type_name)
 * where type_name is complete.
 *
 * @param name <IN> name of shared memory pointer.
 * @param type <IN>_name type pointed to
 *
 */

/*
 * FIXME: Users should be able to specify "static" for implementation
 * definition so that we don't have name space pollution from users
 * (example: ad-hoc linked list implementations with list_node)
 *
 * FIXME: We need to capture how we got to the allocate and free.  For now
 * we statically capture __FUNCTION__/__FILE__/__LINE__ but long term we
 * need stack back tracing with no -fomit-frame-pointer micro-optimization.
 *
 * FIXME: Arrays should be handled differently to accomodate the
 * ref/release on sub-object granularity.
 */
#define PLAT_SP(name, type_name)                                               \
    __BEGIN_DECLS                                                              \
                                                                               \
    PLAT_SP_COMMON(name, type_name)                                            \
                                                                               \
    name ## _t name ## _alloc_helper(const char *file, unsigned line,          \
                                     const char *fn,                           \
                                     enum plat_shmem_arena arena,              \
                                     int flags);                               \
    name ## _t name ## _alloc() PLAT_SP_DEPRECATED;                            \
                                                                               \
    name ## _t name ## _array_alloc_helper(const char *file, unsigned line,    \
                                           const char *fn, size_t nelm,        \
                                           enum plat_shmem_arena arena,        \
                                           int flags);                         \
    name ## _t name ## _array_alloc(size_t nelm) PLAT_SP_DEPRECATED;           \
                                                                               \
    void name ## _free_helper(const char *file, unsigned line, const char *fn, \
                              name ## _t ptr, int free_count);                 \
    void name ## _free(name ## _t ptr) PLAT_SP_DEPRECATED;                     \
                                                                               \
    void name ## _array_free(name ##_t ptr, size_t nelm) PLAT_SP_DEPRECATED;   \
    void name ## _array_free_helper(const char *file, unsigned line,           \
                                    const char *fn, name ## _t ptr,            \
                                    size_t nelm, int free_count);              \
                                                                               \
    PLAT_SP_EXTERN_INLINE_IMPL(name, type_name, PLAT_INLINE)                   \
                                                                               \
    __END_DECLS

/**
 * Implement out-of-line functions for shared pointer name_t
 *
 * @param name <IN> name of shared memory pointer.
 * @param type <IN>_name type pointed to
 */
#define PLAT_SP_IMPL(name, type_name)                                          \
    PLAT_SP_COMMON_IMPL(name, type_name)                                       \
                                                                               \
    name ## _t                                                                 \
    name ## _alloc_helper(const char *file, unsigned line, const char *fn,     \
                          enum plat_shmem_arena arena, int flags) {            \
        name ## _t ret;                                                        \
        extern plat_shmem_ptr_base_t                                           \
        plat_shmem_alloc_helper(const char *file, unsigned line,               \
                                const char *fn, int log_cat, size_t size,      \
                                enum plat_shmem_arena arena, int flags);       \
        ret.base = plat_shmem_alloc_helper(file, line, fn,                     \
                                           name ## _get_log_cat(),             \
                                           sizeof (type_name), arena, flags);  \
        return (ret);                                                          \
    }                                                                          \
                                                                               \
    name ## _t                                                                 \
    name ## _alloc() {                                                         \
        return (name ## _alloc_helper(__FILE__, __LINE__,                      \
                                      __PRETTY_FUNCTION__,                     \
                                      PLAT_SHMEM_ARENA_DEFAULT,                \
                                      0 /* flags */));                         \
    }                                                                          \
                                                                               \
    name ## _t                                                                 \
    name ## _array_alloc_helper(const char *file, unsigned line,               \
                                const char *fn, size_t nelm,                   \
                                enum plat_shmem_arena arena, int flags) {      \
        name ## _t ret;                                                        \
        extern plat_shmem_ptr_base_t                                           \
        plat_shmem_alloc_helper(const char *file, unsigned line,               \
                                const char *fn, int log_cat, size_t size,      \
                                enum plat_shmem_arena arena, int flags);       \
        ret.base = plat_shmem_alloc_helper(file, line, fn,                     \
                                           name ## _get_log_cat(),             \
                                           nelm * sizeof (type_name), arena,   \
                                           flags);                             \
        return (ret);                                                          \
    }                                                                          \
                                                                               \
    name ## _t                                                                 \
    name ## _array_alloc(size_t nelm) {                                        \
        return (name ## _array_alloc_helper(__FILE__, __LINE__,                \
                                            __PRETTY_FUNCTION__, nelm,         \
                                            PLAT_SHMEM_ARENA_DEFAULT,          \
                                            0 /* flags */));                   \
    }                                                                          \
                                                                               \
    void                                                                       \
    name ## _free_helper(const char *file, unsigned line, const char *fn,      \
                         name ## _t ptr, int free_count) {                     \
        extern void plat_shmem_free_helper(const char *file, unsigned line,    \
                                           const char *fn,                     \
                                           int log_cat,                        \
                                           plat_shmem_ptr_base_t ptr,          \
                                           size_t size,                        \
                                           int free_count);                    \
        plat_shmem_free_helper(file, line, fn, name ## _get_log_cat(),         \
                               ptr.base, sizeof (type_name), free_count);      \
    }                                                                          \
                                                                               \
    void                                                                       \
    name ## _free(name ## _t ptr) {                                            \
        name ## _free_helper(__FILE__, __LINE__, __FUNCTION__, ptr, 1);        \
    }                                                                          \
                                                                               \
    void                                                                       \
    name ## _array_free_helper(const char *file, unsigned line, const char *fn,\
                               name ## _t ptr, size_t nelm, int free_count) {  \
        extern void plat_shmem_free_helper(const char *file, unsigned line,    \
                                           const char *fn,                     \
                                           int log_cat,                        \
                                           plat_shmem_ptr_base_t ptr,          \
                                           size_t size,                        \
                                           int free_count);                    \
        plat_shmem_free_helper(file, line, fn,                                 \
                               name ## _get_log_cat(),                         \
                               ptr.base, nelm * sizeof (type_name),            \
                               free_count);                                    \
    }                                                                          \
                                                                               \
    void                                                                       \
    name ## _array_free(name ## _t ptr, size_t nelm) {                         \
        name ## _array_free_helper(__FILE__, __LINE__, __FUNCTION__, ptr,      \
                                   nelm, 1);                                   \
    }                                                                          \
                                                                               \
    PLAT_OUT_OF_LINE(PLAT_SP_EXTERN_INLINE_IMPL(name, type_name,               \
                                                /* no inline */))


/*
 * Provide inline and out-of-line definitions for extern __inline__ parts
 * of PLAT_SP_IMPL.  Not for user use.
 *
 * @param name <IN> name of shared memory pointer.
 * @param type <IN>_name type pointed to
 * @param inline <IN>_decl prefix which is usually 'extern __inline__' or
 * ''
 */
#define PLAT_SP_EXTERN_INLINE_IMPL(name, type_name, inline_decl)               \
    inline_decl void                                                           \
    name ## _rwrelease(type_name **local) {                                    \
        PLAT_SHMEM_RELEASE(local);                                             \
    }                                                                          \
                                                                               \
    inline_decl void                                                           \
    name ## _rrelease(const type_name **local) {                               \
        PLAT_SHMEM_RELEASE(local);                                             \
    }                                                                          \
                                                                               \
    inline_decl type_name *                                                    \
    name ## _rwref(type_name **local, name ## _t shared) {                     \
        name ## _rwrelease(local);                                             \
        type_name *ret =                                                       \
            ((type_name *) plat_shmem_ptr_base_to_ptr(shared.base));           \
        *local = ret;                                                          \
        return (ret);                                                          \
    }                                                                          \
                                                                               \
    inline_decl const type_name *                                              \
    name ## _rref(const type_name **local, name ## _t shared) {                \
        name ## _rrelease(local);                                              \
        const type_name *ret =                                                 \
            ((type_name *) plat_shmem_ptr_base_to_ptr(shared.base));           \
        *local = ret;                                                          \
        return (ret);                                                          \
    }                                                                          \
                                                                               \
    inline_decl type_name *                                                    \
    name ## _rwref_nofail(type_name **local, name ## _t shared) {              \
        name ## _rwrelease(local);                                             \
        type_name *ret =                                                       \
            ((type_name *) plat_shmem_ptr_base_to_ptr_nofail(shared.base));    \
        *local = ret;                                                          \
        return (ret);                                                          \
    }                                                                          \
                                                                               \
    inline_decl const type_name *                                              \
    name ## _rref_nofail(const type_name **local, name ## _t shared) {         \
        name ## _rrelease(local);                                              \
        const type_name *ret =                                                 \
            ((type_name *) plat_shmem_ptr_base_to_ptr_nofail(shared.base));    \
        *local = ret;                                                          \
        return (ret);                                                          \
    }

/*
 * Implement parts common to all shared pointer types.  Not for user use.
 *
 * @param name <IN> name of shared memory pointer.
 * @param type <IN>_name type pointed to
 */
#define PLAT_SP_COMMON(name, typename)                                         \
    typedef struct name {                                                      \
        plat_shmem_ptr_base_t base;                                            \
    } name ## _t;                                                              \
                                                                               \
    extern int name ## _log_cat;                                               \
                                                                               \
    PLAT_ONCE(/* extern */, name)                                              \
                                                                               \
    static __inline__ int                                                      \
    name ## _get_log_cat(void) {                                               \
        name ## _once();                                                       \
        return (name ## _log_cat);                                             \
    }                                                                          \
                                                                               \
    static const name ## _t name ## _null __attribute__((unused)) =            \
        PLAT_SP_INITIALIZER;                                                   \
                                                                               \
    static __inline__ int                                                      \
    name ## _is_null(name ## _t ptr) {                                         \
        return (plat_shmem_ptr_base_is_null(ptr.base));                        \
    }                                                                          \
                                                                               \
    static __inline__ int                                                      \
    name ## _eq(name ## _t lhs, name ## _t rhs) {                              \
        return (plat_shmem_ptr_base_eq(lhs.base, rhs.base));                   \
    }                                                                          \
                                                                               \
    static __inline__ int                                                      \
    name ## _cmp(name ##_t lhs, name ## _t rhs) {                              \
        return (plat_shmem_ptr_base_cmp(lhs.base, rhs.base));                  \
    }                                                                          \
                                                                               \
    static __inline__ int                                                      \
    name ## _parse(name ##_t *outptr, const char *string,                      \
                   const char **endptr) {                                      \
        return (plat_shmem_ptr_base_parse(&outptr->base, string, endptr));     \
    }

/*
 * Execute once logic in constructor because we need it to happen before arg
 * processing to get into the usage message.  Of course, this eliminates the
 * need for the once logic since shmem can't be used before its attached.
 */
#define PLAT_SP_COMMON_IMPL(name, typename)                                    \
    int name ## _log_cat;                                                      \
                                                                               \
    PLAT_ONCE_IMPL(/* extern */, name, name ## _log_cat =                      \
                   plat_log_add_subcategory(PLAT_LOG_CAT_PLATFORM_SHMEM_ALLOC, \
                                            /* comment for cstyle */ #name))   \
                                                                               \
    static void name ## _log_cat_init() __attribute__((constructor));          \
    static void name ## _log_cat_init() {                                      \
        name ##_once();                                                        \
    }

/**
 * Declare shared pointer name_t to variable length structure of
 * user specified size.  Since the allocate/free/reference/etc. have different
 * semantics (requiring a specific size) they have a var for variable length
 * in their name.
 *
 * Declares
 *     name_t name_var_alloc(size_t size);
 *
 *         DEPRECATED because this usage precludes including CPP determined
 *         file, line tuples in diagnostics.  Use #plat_shmem_var_alloc
 *
 *         Allocates a single type_name of length size.  Size may not be
 *         less than sizeof (type_name) where sizeof (void) == 1.
 *         Returns name_null on failure setting plat_errno.
 *         Failures include ENOMEM.
 *
 *     void name_var_free(name_t, size_t size)
 *         Free a name_t of length size.  Size may not be less than
 *         sizeof (type_name).  name_free(name_null) is legal like free(3).
 *         of name_t;
 *
 *     const type_name *name_var_rref(const type_name **local, name_t shared,
 *         size_t size)
 *
 *         Reference the shared memory for read access storing the local
 *         pointer in *local.  The user should not assign *local to another
 *         pointer.  *local must be null on the call since implicit release
 *         is not possible with the size varying between calls and size being
 *         unknowable with zero space overhead.
 *
 *         1.  If *local previously mapped a shared pointer,
 *             name_release(previous_shared, *local) must have
 *             been called.
 *
 *         2.  References shall not cross function boundaries.
 *
 *         The reference/release model allows the shared pointer code to be
 *         smart about protecting "unaccessable" objects in debug mode.  The
 *         pointer to pointer interface allows specific references to be
 *         tracked.
 *
 *     void name_var_rrelease(const type_name **local, size_t size);
 *         Release the local read-only reference *local.  The corresponding
 *         shared pointer is not required because that would not
 *         be convienent and make for a messy re-assign semantic.
 *
 *     type_name *name_var_rwref(type_name **local, name_t shared, size_t size)
 *         Read write reference.
 *
 *         See name_var_rref above for preconditions; only the constness
 *         of local varies.
 *
 *     void name_var_rwrelease(type_name **local, size_t size)
 *         Release read-write reference.
 *
 *     const name_t name_null;
 *         Type-safe null pointer
 *
 *     int name_is_null(name_t ptr);
 *         Return true if ptr is null.
 *
 *     int name_eq(name_t lhs, name_t rhs);
 *         Return true if lhs == rhs
 *
 *     int name_cmp(name_t lhs, name_t rhs);
 *         Return -1 if lhs < rhs, 0 if lhs == rhs, > 1 if lhs > rhs
 *
 * type_name does not need to be complete at the time of declaration.
 *
 * The user must separately instantiate the pointer methods by invoking
 *    PLAT_SP_VAR_OPAQUE_IMPL(name, type_name)
 * where type_name is complete.
 *
 * @param name <IN> name of shared memory pointer.
 * @param type <IN>_name type pointed to
 */

#define PLAT_SP_VAR_OPAQUE(name, type_name)                                    \
    __BEGIN_DECLS                                                              \
                                                                               \
    PLAT_SP_COMMON(name, type_name)                                            \
                                                                               \
    name ## _t name ## _var_alloc_helper(const char *file, unsigned line,      \
                                         const char *fn, size_t size,          \
                                         enum plat_shmem_arena arena,          \
                                         int flags);                           \
    name ## _t name ## _var_alloc(size_t size) PLAT_SP_DEPRECATED;             \
    void name ## _var_free_helper(const char *file, unsigned line,             \
                                  const char *fn, name ## _t ptr, size_t size, \
                                  int free_count);                             \
    void name ## _var_free(name ## _t ptr, size_t size) PLAT_SP_DEPRECATED;    \
                                                                               \
    PLAT_SP_VAR_OPAQUE_EXTERN_INLINE_IMPL(name, type_name, PLAT_INLINE)        \
                                                                               \
    __END_DECLS

/**
 * Implement out-of-line functions for shared pointer name_t to variable
 * length structure of user specified size.
 *
 * @param name <IN> name of shared memory pointer.
 * @param type <IN>_name type pointed to
 */
#define PLAT_SP_VAR_OPAQUE_IMPL(name, type_name)	\
    PLAT_SP_COMMON_IMPL(name, type_name)                                       \
                                                                               \
    name ## _t                                                                 \
    name ## _var_alloc_helper(const char *file, unsigned line, const char *fn, \
                              size_t size, enum plat_shmem_arena arena,        \
                              int flags) {                                     \
        name ## _t ret;                                                        \
        plat_shmem_debug_assert(size >= sizeof (type_name));                   \
        ret.base = plat_shmem_alloc_helper(file, line, fn,                     \
                                           name ## _get_log_cat(),             \
                                           size, arena, flags);                \
        return (ret);                                                          \
    }                                                                          \
                                                                               \
    name ## _t                                                                 \
    name ## _var_alloc(size_t size) {                                          \
        return (name ## _var_alloc_helper(__FILE__, __LINE__,                  \
                                          __PRETTY_FUNCTION__, size,           \
                                          PLAT_SHMEM_ARENA_DEFAULT,            \
                                          0 /* flags */));                     \
    }                                                                          \
                                                                               \
    void                                                                       \
    name ## _var_free_helper(const char *file, unsigned line, const char *fn,  \
                             name ## _t ptr, size_t size, int free_count) {    \
        plat_shmem_free_helper(file, line, fn, name ## _get_log_cat(),         \
                               ptr.base, size, free_count);                    \
    }                                                                          \
                                                                               \
    void                                                                       \
    name ## _var_free(name ## _t ptr, size_t size) {                           \
        name ## _var_free_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, ptr, \
                                 size, 1);                                     \
    }                                                                          \
                                                                               \
    PLAT_OUT_OF_LINE(PLAT_SP_VAR_OPAQUE_EXTERN_INLINE_IMPL(name, type_name,    \
                                                           /* no inline */))


/*
 * Provide inline and out-of-line definitions for extern __inline__ parts
 * of PLAT_SP_VAR_OPAQUE_IMPL.  Not for user use.
 *
 * @param name <IN> name of shared memory pointer.
 * @param type <IN>_name type pointed to
 * @param inline <IN>_decl prefix which is usually 'extern __inline__' or
 * ''
 */
#define PLAT_SP_VAR_OPAQUE_EXTERN_INLINE_IMPL(name, type_name, inline_decl)    \
    inline_decl void                                                           \
    name ## _var_rwrelease(type_name **local, size_t size) {                   \
        PLAT_SHMEM_RELEASE(local);                                             \
    }                                                                          \
                                                                               \
    inline_decl void                                                           \
    name ## _var_rrelease(const type_name **local, size_t size) {\
        PLAT_SHMEM_RELEASE(local);                                             \
    }                                                                          \
                                                                               \
    inline_decl type_name *                                                    \
    name ## _var_rwref(type_name **local, name ## _t shared, size_t size) {    \
        plat_shmem_debug_assert(!*local);                                      \
        type_name *ret =                                                       \
            (type_name *)plat_shmem_ptr_base_to_ptr(shared.base);              \
        *local = ret;                                                          \
        return (ret);                                                          \
    }                                                                          \
                                                                               \
    inline_decl const type_name *                                              \
    name ## _var_rref(const type_name **local, name ## _t shared,              \
                      size_t size) {                                           \
        plat_shmem_debug_assert(!*local);                                      \
        const type_name *ret =                                                 \
            (type_name *) plat_shmem_ptr_base_to_ptr(shared.base);             \
        *local = ret;                                                          \
        return (ret);                                                          \
    }                                                                          \
                                                                               \
    inline_decl type_name *                                                    \
    name ## _var_rwref_nofail(type_name **local, name ## _t shared,            \
                              size_t size) {                                   \
        plat_shmem_debug_assert(!*local);                                      \
        type_name *ret =                                                       \
            (type_name *)plat_shmem_ptr_base_to_ptr_nofail(shared.base);       \
        *local = ret;                                                          \
        return (ret);                                                          \
    }                                                                          \
                                                                               \
    inline_decl const type_name *                                              \
    name ## _var_rref_nofail(const type_name **local, name ## _t shared,       \
                             size_t size) {                                    \
        plat_shmem_debug_assert(!*local);                                      \
        const type_name *ret =                                                 \
            (type_name *) plat_shmem_ptr_base_to_ptr_nofail(shared.base);      \
        *local = ret;                                                          \
        return (ret);                                                          \
    }

/** @brief plat_shmem_ptr_base_t NULL initializer */
#define PLAT_SPB_INITIALIZER { }

static const plat_shmem_ptr_base_t plat_shmem_ptr_base_null
    __attribute__((unused)) = PLAT_SPB_INITIALIZER;

/**
 * Initialize any shared memory pointer to NULL.
 */
#define PLAT_SP_INITIALIZER { PLAT_SPB_INITIALIZER }

typedef struct shmem_void {
    plat_shmem_ptr_base_t base;
} shmem_void_t;
static const shmem_void_t shmem_void_null __attribute__((unused)) =
    PLAT_SP_INITIALIZER;

/**
 * Classes of shared memory user
 *
 * - Shared state. The clients change shared state, using
 *   locks in shared memory.  Initial implementations are fail-stop
 *   when these clients fail.
 *
 */
enum plat_shmem_user_class {
    /**
     * Read only users never modify memory and may only be able
     * to get read references.
     */
    PLAT_SHMEM_USER_READ_ONLY,
    /**
     * Isolated users have read-write access but don't do anything which
     * requires global recovery like implementing a mutex as a lock in shared
     * memory.  Things like locking and non-atomic data structure operations
     * can be handled by messaging to a (hopefully more relaible) client
     * with higher class.
     */
    PLAT_SHMEM_USER_ISOLATED,
    /**
     * Local recovery users do non-atomic things which they can undo on
     * the next restart.
     */
    PLAT_SHMEM_USER_LOCAL_RECOVERY,
    /**
     * Shared state users manipulate structures in non-atomic ways which
     * require recovery.
     */
    PLAT_SHMEM_USER_GLOBAL_RECOVERY,
    /**
     * Shmemd has complete control over shared memory.
     */
    PLAT_SHMEM_USER_SHMEMD,

    PLAT_SHMEM_USER_INVALID = -1
};

static __inline__ int
plat_shmem_ptr_base_is_null(plat_shmem_ptr_base_t base) {
    return (!base.int_base);
}

static __inline__ int
plat_shmem_ptr_base_eq(plat_shmem_ptr_base_t lhs, plat_shmem_ptr_base_t rhs) {
    return (lhs.int_base == rhs.int_base);
}

static __inline__ int
plat_shmem_ptr_base_cmp(plat_shmem_ptr_base_t lhs, plat_shmem_ptr_base_t rhs) {
    int ret;

    if (lhs.ptr > rhs.ptr) {
        ret = 1;
    } else if (lhs.ptr < rhs.ptr) {
        ret = -1;
    } else {
        ret = 0;
    }

    return (ret);
}

/* Definition in shmem_c.h */
#if 0
/**
 * @brief Convert shared memory base pointer to physical representation.
 *
 * XXX: Should be uint64_t on 32 bit with big physmem
 *
 * @return Physical address, 0 when no mapping exists
 */
size_t plat_shmem_ptr_base_to_paddr(plat_shmem_ptr_base_t shmem_ptr_base);
#endif

#include "platform/shmem_c.h"

#ifndef NOTPROTOTYPE
/**
 * @brief Initialize shared memory for prototype (pre-shmemd) code
 *
 * @param config <IN> configuration
 * @return 0 on success, -plat_errno on failure.
 */
int plat_shmem_prototype_init(const struct plat_shmem_config *config);
#endif

/**
 * @brief Trivial unit test initialization code.
 *
 * Put all trivial unit test initialization in one function so its
 * contents can be changed in exactly one file instead of 50+.
 *
 * This initializes and attaches.  It terminates the entire program with
 * some sort of non-zero status on failure.
 */
void plat_shmem_trivial_test_start(int argc, char **argv);

/**
 * @brief Trivial unit test shutdown code
 *
 * Put all trivial unit test initialization in one function so its
 * contents can be changed in exactly one file instead of 50+.
 *
 * At some point in the future, this may cause programs to terminate with
 * non zero status if they've done bad things with shared memory.
 */
void plat_shmem_trivial_test_end();

/**
 * Attach to the shared memory space, where the name space is shared across
 * the node.  Due to the tight integration needed with the user thread
 * scheduler for blocking IPC objects, an individual process may attach to
 * at most one shared space but the mapped subset of the space can vary
 * between processes.
 *
 * Spaces may be composed of multiple non-contiguous segments and may not
 * be fully mapped.
 *
 * The named space must already exist
 *
 * plat_shmem_attach must be called before the first user thread is
 * created so that the scheduler can allocate shared memory.
 *
 * plat_shmem_detach should be called on termination for diagnosic
 * purposes but for correctness both abnormal and normal termination
 * imply detach.
 *
 * Returns:
 *      -ENOENT where space does not have exist
 *      -PLAT_SHMEM_EALREADY_ATTACHED
 */
int plat_shmem_attach(const char *space);

/**
 * @Detach from the shared memory object.
 *
 * All plat_shmem_ref_t references associated with plat_shmem_t are released.
 *
 * Note that with --plat/shmem/debug/replace_malloc, any attempts
 * to use pointers allocated while shared memory was attached will fail.
 *
 * Using #plat_shmem_prototype_init and then #plat_shmem_attach will
 * cause referencing those pointers to have undefined results.
 *
 * Returns:
 *      -EBUSY when shmem is still being referenced, whether via
 *      a user allocated pointer or lightweight process.
 */
int plat_shmem_detach();

/**
 * @brief Get statistics asssociated with shared memory.
 *
 * The statistics may not be self-consistent when the system is not queisced
 * due to locking issues.
 *
 * @param stats <IN> Store stats in user-provided buffer
 * @return -errno on failure.
 */
int plat_shmem_alloc_get_stats(struct plat_shmem_alloc_stats *stats);

/**
 * @brief Initialize shared memory configuration.
 */
void plat_shmem_config_init(struct plat_shmem_config *config);

/**
 * @brief parse --plat/shmem/file arg
 *
 * @return 0 on success, -plat_errno on failure
 */
int plat_shmem_config_parse_file(struct plat_shmem_config *config,
                                 const char *arg);

/**
 * @brief parse --plat/shmem/arena arg
 *
 * @return 0 on success, -plat_errno on failure
 */
int plat_shmem_config_parse_arena(struct plat_shmem_config *config,
                                  const char *arg);

/** @brief Destroy dynamically allocated pieces of the config */
void plat_shmem_config_destroy(struct plat_shmem_config *config);

/** @brief Add backing store file.  */
int plat_shmem_config_add_backing_file(struct plat_shmem_config *config,
                                       const char *file, off_t size);

/** @brief Add physmem device. */
int
plat_shmem_config_add_backing_physmem(struct plat_shmem_config *config,
                                      const char *device);

/**
 * @brief Add physmem device used as virtual memory only
 *
 * XXX: drew 2008-09-02 This is temporary to support the virtual
 * memory experiment.
 */
int
plat_shmem_config_add_backing_physmem_virt(struct plat_shmem_config *config,
                                           const char *device);

/** @brief Get path for #plat_shmem_attach */
const char *plat_shmem_config_get_path(const struct plat_shmem_config *config);


/** @brief Get path for #plat_shmem_attach */
int plat_shmem_config_path_is_default(const struct plat_shmem_config *config);

/** @brief set configuration flags */
void plat_shmem_config_set_flags(struct plat_shmem_config *config, int flags);

/** @brief clear configuration flags */
void plat_shmem_config_clear_flags(struct plat_shmem_config *config, int flags);

/** @brief Set limit for given arena */
void plat_shmem_config_set_arena_used_limit(struct plat_shmem_config *config,
                                            enum plat_shmem_arena arena,
                                            int64_t limit);

/**
 * @brief Allocate fixed-sized shared memory object
 *
 * @code
 * PLAT_SP(foo_sp, struct foo);
 *
 * foo_sp_t
 * fn() {
 *     return (plat_shmem_alloc(foo_sp));
 * }
 * @endcode
 *
 * @param type <IN> shared pointer type without trailing _t
 * @return shared memory pointer to type
 *
 */
#define plat_shmem_alloc(type)                                                 \
    type ## _alloc_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__,             \
                          PLAT_SHMEM_ARENA_DEFAULT, 0 /* flags */)

/**
 * @brief Allocate fixed-sized shared memory object with non-default arena
 *
 * @code
 * PLAT_SP(foo_sp, struct foo);
 *
 * foo_sp_t
 * fn() {
 *     return (plat_shmem_arena_alloc(foo_sp,
 *             PLAT_SHMEM_ARENA_ROOT_THREAD, 0));
 * }
 * @endcode
 *
 * @param type <IN> shared pointer type without trailing _t
 * @param  arena <IN> enum plat_shmem_arena specifying arena class (
 * see platform/shmem_arena.h)
 * @param flags <IN>  Bitwise or of #plat_shmem_alloc_helper_flags. 0 for
 * default behavior.
 * @return shared memory pointer to type
 *
 */
#define plat_shmem_arena_alloc(type, arena, flags)                             \
    type ## _alloc_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__,             \
                          (arena), (flags))


/**
 * @brief Allocate fixed-sized shared memory object in memory with known
 * physical address.
 *
 * @code
 * PLAT_SP(foo_sp, struct foo);
 *
 * foo_sp_t
 * fn() {
 *     return (plat_shmem_phys_alloc(foo_sp));
 * }
 * @endcode
 *
 * @param type <IN> shared pointer type without trailing _t
 * @return shared memory pointer to type
 */
#define plat_shmem_phys_alloc(type)                                            \
    type ## _alloc_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__,             \
                          PLAT_SHMEM_ARENA_DEFAULT, PLAT_SHMEM_ALLOC_PHYS)

/**
 * @brief Allocate shared memory array
 *
 * @param type <IN> shared pointer type without trailing _t
 * @param n <IN> array size in elements
 * @return shared memory pointer to type
 */
#define plat_shmem_array_alloc(type, n)                                        \
    type ## _array_alloc_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, (n),  \
                                PLAT_SHMEM_ARENA_DEFAULT, 0 /* flags */)


/**
 * @brief Allocate shared memory array with non-default arena
 *
 * @param type <IN> shared pointer type without trailing _t
 * @param n <IN> array size in elements
 * @param  arena <IN> enum plat_shmem_arena specifying arena class (
 * see platform/shmem_arena.h)
 * @param flags <IN>  Bitwise or of #plat_shmem_alloc_helper_flags. 0 for
 * default behavior.
 * @return shared memory pointer to type
 */
#define plat_shmem_array_arena_alloc(type, n, arena, flags) \
    type ## _array_alloc_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, (n),  \
                                (arena), (flags))


/**
 * @brief Allocate shared memory array with known physical address
 *
 * @param type <IN> shared pointer type without trailing _t
 * @param n <IN> array size in elements
 * @return shared memory pointer to type
 */
#define plat_shmem_array_phys_alloc(type, n)                                   \
    type ## _array_alloc_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, (n),  \
                                PLAT_SHMEM_ARENA_DEFAULT, PLAT_SHMEM_ALLOC_PHYS)


/**
 * @brief Allocate variable sized shared memory object
 *
 * @code
 * PLAT_SP(foo_sp, struct foo);
 *
 * struct foo {
 *   int nint;
 *   int ints[];
 * }
 *
 * foo_sp_t
 * fn(int n) {
 *     return (plat_shmem_var_alloc(foo_sp, sizeof(struct foo) +
 *                                  sizeof(foo.ints[0]) * n));
 * }
 * @endcode
 *
 * @param type <IN> shared pointer type without trailing _t
 * @param size <IN> total size of object, must be >= size of raw type
 * @return shared memory pointer to type
 */
#define plat_shmem_var_alloc(type, size)                                       \
    type ## _var_alloc_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, (size), \
                              PLAT_SHMEM_ARENA_DEFAULT, 0 /* flags */)

/**
 * @brief Allocate variable sized shared memory object from non-default arena
 *
 * @code
 * PLAT_SP(foo_sp, struct foo);
 *
 * struct foo {
 *   int nint;
 *   int ints[];
 * }
 *
 * foo_sp_t
 * fn(int n) {
 *     return (plat_shmem_var_alloc(foo_sp, sizeof(struct foo) +
 *                                  sizeof(foo.ints[0]) * n,
 *                                  PLAT_SHMEM_ARENA_ROOT_THREAD,
 *                                  0));
 * }
 * @endcode
 *
 * @param type <IN> shared pointer type without trailing _t
 * @param size <IN> total size of object, must be >= size of raw type
 * @param  arena <IN> enum plat_shmem_arena specifying arena class (
 * see platform/shmem_arena.h)
 * @param flags <IN>  Bitwise or of #plat_shmem_alloc_helper_flags. 0 for
 * default behavior.
 * @return shared memory pointer to type
 */
#define plat_shmem_var_arena_alloc(type, size, arena, flags)                   \
    type ## _var_alloc_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, (size), \
                              (arena), (flags))


/**
 * @brief Allocate variable sized shared memory object with known physical
 * address.
 *
 * @code
 * struct foo {
 *   int nint;
 *   int ints[];
 * }
 *
 * foo_sp_t
 * fn(int n) {
 *     return (plat_shmem_var_phys_alloc(foo_sp, sizeof(struct foo) +
 *                                       sizeof(foo.ints[0]) * n));
 * }
 *
 * @param type <IN> shared pointer type without trailing _t
 * @param size <IN> total size of object, must be >= size of raw type
 * @return shared memory pointer to type
 */
#define plat_shmem_var_phys_alloc(type, size)                                  \
    type ## _var_alloc_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, (size), \
                              PLAT_SHMEM_ARENA_DEFAULT, PLAT_SHMEM_ALLOC_PHYS)

/**
 * @brief Free fixed-size shared memory object
 *
 *  co-free version is like _free but does not actually free the space until
 *  it is called a second time (presumably from each of 2 cooperating threads).
 *
 * @param type <IN> shared pointer type without trailing _t
 * @param ptr <IN> type ## _t pointer
 */
#define plat_shmem_free(type, ptr)                                             \
    type ## _free_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, ptr, 1)

#define plat_shmem_co_free(type, ptr)                                          \
    type ## _free_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, ptr, 2)

/**
 * @brief Free variable sized shared memory object
 *
 * @param type <IN> shared pointer type without trailing _t
 * @param ptr <IN> type ## _t pointer
 */
#define plat_shmem_var_free(type, ptr, size)                                   \
    type ## _var_free_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, (ptr),   \
                             (size), 1)

#define plat_shmem_var_co_free(type, ptr, size)                                \
    type ## _var_free_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, (ptr),   \
                             (size), 2)

#define plat_shmem_array_free(type, ptr, n)                                    \
    type ## _array_free_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, (ptr), \
                               (n), 1)

#define plat_shmem_array_co_free(type, ptr, n)                                 \
    type ## _array_free_helper(__FILE__, __LINE__, __PRETTY_FUNCTION__, (ptr), \
                               (n), 2)

/**
 * @brief Return true if the shared memory pointer is null.
 *
 * @param ptr <IN> pointer to check (any shared memory type)
 * @return non-zero when ptr is NULL
 */
#define plat_shmem_ptr_is_null(ptr)                                            \
        plat_shmem_ptr_base_is_null((ptr).base)

/**
 * @brief Convert shared memory pointer to physical address
 *
 * For interfacing with user space hardware.  The address is guaranteed to not
 * change until after the pointer is freed.
 *
 * #plat_shmem_phys_can_fail where this is not acceptable
 *
 * @param ptr <IN> shared memory pointer of any type
 * @return physical address, 0 for no physical mapping
 */
#define plat_shmem_ptr_to_paddr(ptr)                                           \
        plat_shmem_ptr_base_to_paddr((ptr).base)

/**
 * @brief Convert offset from shared memory pointer to physical address
 *
 * The results are undefed when offset_arg is not within a region returned
 * by one of the plat_shmem_alloc functions.  Versions of the linux kernel
 * limit contiguous dma addressable memory allocations by modules to 4M at
 * a time so in practice areas will be discontigous.  Naieve implementations
 * of the attach code+kernel may result in virtual memory mappings which are
 * contiguous where physical addresses are not.
 *
 * @param ptr_arg <IN> shared memory pointer arg
 * @param offset_arg <IN> offset
 */
#define plat_shmem_ptr_offset_paddr(ptr_arg, offset_arg)                       \
        (plat_shmem_ptr_base_to_paddr((ptr_arg).base, (offset_arg)))

/**
 * @brief Stop gap to translate local address to physical address
 *
 * It's currently more efficient to convert a shared memory pointer to
 * its physical offset than trying to figure out the mapping.  This may
 * or may not remain the case.
 */
PLAT_SP_DEPRECATED plat_shmem_paddr_t
plat_shmem_local_to_paddr(const void *ptr);

/**
 * @brief Cast shmemptr.
 *
 * @param type <IN> Shmem pointer type name
 * @param ptr <IN> Pointer
 */
#define shmem_cast(type, shmem) ((type ## _t) { shmem.base })

/**
 * @brief Duplicate memory
 *
 * This is a kludge around how ptrace(2) interacts with device drivers
 * that remap physical pages.  Remapped physical pages don't have page
 * structures associated with them, so get_user_pages() doesn't return
 * page table entries, access_process_vm() returns -EFAULT, and
 * ptrace PTRACE_PEEKTEXT fails with errno EIO.
 *
 * @param type <IN> ptr which may refer to physical memory
 * @param type <IN> len length
 * @return copy of *ptr in normal memory.  Call #plat_memdup_free to
 * release.
 */
void *plat_memdup_alloc(const void *src, size_t len);

/**
 * @brief Release the results of #plat_memdup_alloc
 *
 * @param <IN> ptr pointer to free.
 */
void plat_memdup_free(void *ptr);

/**
 * @brief Explicitly signal pthread start.
 *
 * While some things can be done implicitly (allocate thread-local structures),
 * this may skew test statistics.
 */
void plat_shmem_pthread_started();

/**
 * @brief Explicitly signal pthread termination
 *
 * Pthread termination can't be handled well in libraries because the cleanup
 * interface is stack-based and we can't guarantee our cleanup  push and
 * pops will match.
 *
 * Usually we can wait to free things until detaching from shmem, but this
 * skews test results.
 */
void plat_shmem_pthread_done();

/**
 * @brief Get bucket sizes
 *
 * Use for determining eviction candidates in caches.  Buckets cover
 * the half open interval from the previous (0 bytes for the first)
 *
 * @param arena <IN> arena
 * @param bucket_ends <OUT> buckets.  Caller must plat_free *bucket_ends.
 * @param bucket_start <OUT> nbucket.  Number of buckets
 * @return 0 on success, -errno on failure (-ENOMEM, -PLAT_ENOTATTACHED,
 * -ENOSYS when compiled with PLAT_SHMEM_FAKE)
 */
int plat_shmem_get_buckets_alloc(enum plat_shmem_arena arena,
                                 size_t **bucket_ends, size_t *nbucket);


/* In the fake case, these are declared in-line in shmem_c.h */
#if !defined(PLAT_SHMEM_FAKE)
/**
 * @brief Internal function to allocate shared memory objects
 */
plat_shmem_ptr_base_t plat_shmem_alloc_helper(const char *file, unsigned line,
                                              const char *fn, int log_cat,
                                              size_t size,
                                              enum plat_shmem_arena arena,
                                              int flags);
/**
 * @brief Internal function to free shared memory objects.
 */
void plat_shmem_free_helper(const char *file, unsigned line, const char *fn,
                            int log_cat, plat_shmem_ptr_base_t ptr,
                            size_t size, int free_count);
#endif

/**
 * @brief Steal memory from shared memory heap
 *
 * There is no corresponding free function.
 */
plat_shmem_ptr_base_t
plat_shmem_steal_from_heap(size_t len);

#ifdef PLATFORM_INTERNAL
int shmem_alloc_attach(int create);
void shmem_alloc_detach();

void shmem_alloc_pthread_started();
void shmem_alloc_pthread_done();

/* These are all actually declared in shmem_c.h */
#if 0

/*
 * During normal operation, these functions are only called between
 * the time shmem has started and when it has finished.  Since the
 * shmem subsystem segment map is append only the function is
 * pure.
 *
 * Compiling with PLAT_SHMEM_DEBUG disables the optimization to
 * catch dangling references during shutdown.
 */
/** @brief Internal function which translates shmem pointer to local */
PLAT_SHMEM_CONST void *plat_shmem_ptr_base_to_ptr(plat_shmem_ptr_base_t
                                                  shmem_ptr_base);

/** @brief Internal function which translates shmem pointer (no assert) */
PLAT_SHMEM_CONST void *plat_shmem_ptr_base_to_ptr_nofail(plat_shmem_ptr_base_t
                                                         shmem_ptr_base);

/** @brief Internal function which translates shmem pointer to phys */
PLAT_SHMEM_CONST size_t plat_shmem_ptr_base_to_paddr(plat_shmem_ptr_base_t
                                                     shmem_ptr_base);

#endif /* 0 */
#endif /* def PLATFORM_INTERNAL */

int plat_shmem_ptr_base_parse(plat_shmem_ptr_base_t *outptr, const char *string,
                              const char **endptr);



__END_DECLS

#endif /* ndef PLATFORM_SHMEM_H */
