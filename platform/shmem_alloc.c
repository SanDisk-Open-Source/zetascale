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
 * File:   sdf/platform/shmem_alloc.c
 * Author: drew
 *
 * Created on February 7, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmem_alloc.c 13588 2010-05-12 01:50:47Z drew $
 */

/**
 * Shared memory allocator.  The shared memory allocator assumes complete
 * ownership of the shared memory pool.  It's implemented as a power of
 * two allocator with stacks for each bucket's free elements.
 *
 * Support for multiple arenas is provided to address lock contention,
 * cache+TLB locality, and allow subsystems to run out of memory
 * independantly (like the cache).  Arenas are hierchical and
 * can pull or push from their parent as the free list grows.  Scope
 * can be system (across processes) or pthread wide.
 *
 * Each arena has its own set of bucket lists.  A slab is associated with
 * each bucket.  When the free list is exahusted more memory is grabbed from
 * the slab.  Where the slab is empty, more objects are grabbed from the parent
 * arena free list (if not empty) or additional slab space is allocated once
 * the root is hit.
 *
 * Frees are returned to the appropriate arena, bucket stack with
 * excessive list growth triggering a return to a shared arena.
 *
 * Thread scope arenas are tracked but not persisted across restarts.
 *
 * When pthread termination is signaled to the allocator the corresponding
 * thread scope arenas have their free objects returned to the global free
 * list.
 *
 * XXX: drew 2008-11-04 Currently the slabs are leaked instead of being
 * returned to the global free list, there is no cleanup at program
 * exit, and there is no cleanup for abnormal shutdowns.
 *
 * TODO: We to integrate the debugging code and add a dump of all allocated
 * memory.
 *
 * TODO: For objects with large number of instances (OMT and cache meta-data
 * entries?) we want fixed bucket sizes.  An allocation flag and hash size to
 * bucket should be sufficient although if the worst happened we could
 * switch to type-specific allocators that draw pages from a common
 * allocation pool.
 */

#define PLATFORM_INTERNAL 1

#include "sys/queue.h"

#include "fth/fthSpinLock.h"

#include "platform/assert.h"
#include "platform/attr.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/mutex.h"
#include "platform/shmem.h"
#include "platform/shmem_debug.h"
#include "platform/stats.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/spin_rw.h"

#include "misc/misc.h"

#include "private/shmem_internal.h"

#define LOG_CAT PLAT_LOG_CAT_PLATFORM_SHMEM_ALLOC

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_RESIZE, LOG_CAT, "resize");

#undef max
#define max(a, b) ((a) >= (b) ? (a) : (b))

/* XXX: Compiler chokes on enum */
#define DEFAULT_ALIGN_SHIFT 4
#define DEFAULT_ALIGN_BYTES (1 << DEFAULT_ALIGN_SHIFT)

enum {
    DEFAULT_MIN_BUCKET_BITS = 4,
    DEFAULT_MAX_BUCKET_BITS = 26
};

enum {
    /* shm0 */
    SHMEM_ALLOC_MAGIC = 0x306d6873,

    /* shm1 */
    SHMEM_ARENA_MAGIC = 0x316d6873
};

/** @brief struct #shmem_alloc flags */
enum sa_flags {
    SA_FLAG_PHYSMEM = 1 << 0
};

/**  @brief Locking type to use within a given domain */
enum sa_lock_type {
    SA_LOCK_NONE,
    SA_LOCK_SPIN,
    SA_LOCK_PTHREAD
};

struct shmem_free_entry;
PLAT_SP(shmem_free_entry_sp, struct shmem_free_entry);

struct shmem_free_list;
PLAT_SP(shmem_free_list_sp, struct shmem_free_list);

/* XXX: This needs to be a PLAT_SP_VAR for the debugging tool to work */
struct sa_arena;
PLAT_SP(sa_arena_sp, struct sa_arena);

/**
 * @brief Lock abstraction
 *
 * Go both ways on locking for performance characterization, etc.
 *
 * For memcached we default to spin locks.  For Maria I think
 * most allocations happen in the agent side so that's OK too especially
 * since we have the multi-layer heaps.
 */
typedef union shmem_alloc_lock {
    plat_mutex_t pthread;
#if 1
    fthSpinLock_t spin;
#else
    void *spin;
#endif
} shmem_alloc_lock_t;

/**
 * @brief Bucket structure
 *
 * Buckets combine a slab with a free list
 */
struct sa_bucket {
#ifdef notyet
    /** @brief Grow/return object count */
    size_t grow_shrink_object_count;
#else
    /** @brief Threshold for this bucket */
    int shrink_object_threshold;
#endif

    /** @brief Lock order is shmem bucket then shmem_alloc */
    shmem_alloc_lock_t lock;

    /**
     * @brief Number of items in use associated with this bucket.
     *
     * May be negative, since the threads allocating and freeing can be
     * different
     */
    int64_t allocated_count;

    /**
     * @brief Total number of user-requested bytes.
     *
     * May be negative, since the threads allocating and freeing can be
     * different
     */
    int64_t allocated_bytes;

    /**
     * @brief Total number of used bytes including allocation overhead
     *
     * May be negative, since the threads allocating and freeing can be
     * different
     */
    int64_t used_bytes;

#ifdef notyet
    /**
     * @brief Total number of bytes used by children arenas.
     */
    int64_t child_used_bytes;
#endif

    /** @brief Total number of calls to alloc */
    uint64_t total_alloc;

    /** @brief Total number of calls to free */
    uint64_t total_free;

    /** @brief Current start of slab (may be null) */
    plat_shmem_ptr_base_t end_of_data;

    /** @brief Current end of slab */
    plat_shmem_ptr_base_t end_of_segment;

    /** @brief Length of free lists */
    uint64_t free_count;

    /** @brief Head of freelist used in FIFO order */
    shmem_free_entry_sp_t list_head;

#ifndef notyet
    /** @brief Tail of free list used in return to heap */
    shmem_free_entry_sp_t list_tail;
#else
    /**
     * @brief Excess lists each about grow_shrink_object_count long
     *
     * Thread local arenas have a single entry here which gets
     * pushed back to the parent's excess_lists; the root arena
     * has all of the left overs for the bucket.
     */
    shmem_free_entry_list_sp_t excess_lists;
#endif
};

#include "private/sa_arena_list_local.h"
#include "fth/fthlll.h"

#include "private/sa_arena_list_global.h"
#include "fth/fthlll.h"

/**
 * @brief Arena structure.
 *
 * One arena is used per pthread with over-flow and under-flow resulting in
 * free object movement to/from their #parent arena which is the root in
 * struct #shmem_alloc.  Global free list exhaustion results in slab
 * allocation from the struct shmem_alloc heap.
 */
struct sa_arena {
    /** @brief Always SHMEM_ARENA_MAGIC */
    plat_magic_t magic;

    /* Configuration fields */

    /** @brief Type of arena */
    enum plat_shmem_arena arena_class;

    /** @brief Configuration of this arena */
    struct plat_shmem_arena_config config;

    /** @brief Lock-type used on individual buckets */
    enum sa_lock_type lock_type;

    /** @brief Where to get more memory from, sa_arena_sp_null for heap only */
    sa_arena_sp_t parent;

    /* Linked list fields (updated outside state lock) */

    /** @brief Linked list of all arenas for statistics aggregation */
    sa_arena_global_lll_el_t global_list_entry;

    /** @brief Linked list of arenas attached to this process for shutdown */
    sa_arena_local_lll_el_t local_list_entry;

    /* State fields  */

    /**
     * @brief State lock
     *
     * Used when changing size.  Buckets have their own locks.
     */
    shmem_alloc_lock_t lock;

    /**
     * @brief size of self and all children
     *
     * Includes
     */
    int64_t tree_size;

    /**
     * @brief Array of bucket
     *
     * Sized to max_bucket_bits + 1.
     * Indexed by ceil(log2(user allocation size)).
     * Entries from 0 to min_bucket_bits inclusive are unused.
     */
    struct sa_bucket buckets[0];
};

PLAT_SP_IMPL(sa_arena_sp, struct sa_arena);

#include "private/sa_arena_list_local.h"
#include "fth/fthlll_c.h"

#include "private/sa_arena_list_global.h"
#include "fth/fthlll_c.h"

/**
 * Entire allocator state
 */
struct shmem_alloc {
    /** @brief Always SHMEM_ALLOC_MAGIC */
    int32_t magic;

    /** @brief Type of lock for shread structures including this */
    enum sa_lock_type lock_type;

    /** @brief Flags from #sa_flags */
    int flags;

    /**
     * @brief State lock
     *
     * Used when updating end_of_data, end_of_segment.  Buckets have their own
     * lock for better concurrency control.  Lock order is shmem bucket then
     * shmem_alloc.
     *
     * Statistics are updated lock-free
     */
    shmem_alloc_lock_t lock;

    /*
     * Global configuration fields
     *
     * Since data flows between arenas, we require them to all use the same
     * bucket configuration.
     */

    /** @brief The bulk of the configuration information */
    struct shmem_alloc_config config;

    /** @brief Minimum bucket size (in bits) */
    int min_bucket_bits;

    /**
     * @brief Maximum bucket size (in bits).
     *
     * XXX: Larger memory allocations currently aren't supported, although
     * we may switch to a scheme where larger allocations get their own
     * segment.
     */
    int max_bucket_bits;

    /** @brief 1 << max_bucket_bits. */
    int max_object_size;

    /* Heap fields */

    /** @brief Current segment allocations are coming out of */
    int current_segment;

    /** @brief Current end of data */
    plat_shmem_ptr_base_t end_of_data;

    /** @brief End of the current segment. */
    plat_shmem_ptr_base_t end_of_segment;

    /**
     * @brief Number of bytes which can't be used
     *
     * XXX: Currently this is a simple sbrk style allocator.  With
     * small physical memory ranges there's going to be waste.
     */
    uint64_t unusable_bytes;

    /** @brief Number of bytes returned by #plat_shmem_steal_from_heap */
    uint64_t stolen_bytes;

    /* Arena state */
    /**
     * @brief Lock on #local_arena_list and #shared_arena_list
     *
     * A read lock is held for statistics traversals, a write lock is held
     * for list additions.
     */
    plat_spin_rwlock_t all_arena_list_lock;

    /**
     * @brief Linked list of all arenas across processes
     *
     * This includes both #PLAT_SHMEM_ARENA_SCOPE_GLOBAL and
     * #PLAT_SHMEM_ARENA_SCOPE_THREAD arenas.
     */
    sa_arena_global_lll_t all_arena_list;

    /**
     * @brief Pointers to global arenas
     *
     * This is sparsely populated with null entries for arenas with
     * thread scope.
     *
     * global_arenas[PLAT_SHMEM_ARENA_NORMAL] points to root_arena.
     *
     * Global arenas are not lazily instantiated
     */
    sa_arena_sp_t global_arenas[PLAT_SHMEM_ARENA_COUNT];

    /**
     * @brief Root arena from which above arenas underflow/overflow to/from.
     *
     * Must be last since the arena structure is variable length.
     */
    struct sa_arena root_arena;
};

/*
 * XXX: drew 09-11-2008 Ultimately we want to have attached state which
 * is separate from the total shared alloc state so we can keep track
 * of our thread-local arenas to return to the global free list on
 * shutdown.
 */
struct shmem_alloc_attached {
    /** @brief Local pointer to shared memory struct shmem_alloc */
    struct shmem_alloc *local_alloc;

    /** @brief List of local per-thread arenas */
    sa_arena_local_lll_t local_arena_list;

    /**
     * @brief Local pointers to all global arena structures
     *
     * This is sparsely populated with null entries for arenas with
     * thread scope.
     */
    struct sa_arena *global_arenas[PLAT_SHMEM_ARENA_COUNT];

    /** @brief statistics gathering handles */
    struct sa_stat_handles *stat_handles;
};

PLAT_SP_IMPL(shmem_alloc_sp, struct shmem_alloc);
struct sa_stat_handles {
    /**
     * @brief Top level statistics.
     *
     * Explicit top-level statistics (for example, total size of shared memory)
     * plus a copy of things which are usually maintained on a per-thread
     * basis.
     */

    struct {
#define item(type, name) struct plat_stat *name;
        PLAT_SHMEM_STAT_ITEMS()
#undef item
    } totals;

    /**
     * @brief Per-arena statistics.
     *
     * This is sparse only containing entries for arenas which have the
     * #SHMEM_ARENA_FLAG_STATS stat.
     */
    struct sa_stat_arena_handles *per_arena[PLAT_SHMEM_ARENA_COUNT];

    /** @brief Called to generate stats and apply the handle set functions */
    struct plat_stat_setter *setter;
};

struct sa_stat_arena_handles {
#define item(type, name) struct plat_stat *name;
    PLAT_SHMEM_STAT_BUCKET_ITEMS()
    PLAT_SHMEM_STAT_ARENA_ITEMS()
#undef item
};

/** @brief Local and shared pointers for an arena */
struct sa_thread_local {
    /**
     * @brief Shared arena pointers need for administrative purposes
     *
     * All arenas.
     */
    sa_arena_sp_t shared_arena_ptrs[PLAT_SHMEM_ARENA_COUNT];

    /**
     * @brief Local arena pointers for memory allocation
     *
     * All arenas.
     */
    struct sa_arena *local_arena_ptrs[PLAT_SHMEM_ARENA_COUNT];
};

enum {
    /* frel */
    SHMEM_FREE_LIST_MAGIC = 0x6c657266
};

struct shmem_free_list {
    plat_magic_t magic;

    shmem_free_entry_sp_t list_head;

    int count;

    shmem_free_list_sp_t next;
};

PLAT_SP_IMPL(shmem_free_list_sp, struct shmem_free_list);

enum {
    /* free (bird) */
    SHMEM_FREE_MAGIC = 0x65657266
};

/**
 * @brief Free list entry.
 */
struct shmem_free_entry {
    /*
     * XXX: drew 2008-09-22 should track each set which will be moved between
     * arenas if that cost shows up in practice.
     */
    shmem_free_entry_sp_t next;

    /* XXX: Is there a good reason this is second? */
    plat_magic_t magic;

#ifdef PLAT_SHMEM_NEW_HEADERS$

    shmem_free_entry_sp_t next_batch;

    /** @brief Debugging stack backtrace index from allocation call */
    int alloc_backtrace;

    /** @brief Debugging stack backtrace index from free call */
    int free_backtrace;
#endif /* def PLAT_SHMEM_NEW_HEADERS */
};

PLAT_SP_IMPL(shmem_free_entry_sp, struct shmem_free_entry);

/*
 * FIXME: There should be different versions of the used header depending on
 * object size (fixed for most single objects, variable for structs ending
 * with foo array[0]) and/or debugging options.
 *
 * The production default (fixed size, no debugging) should be zero length to
 * optimize for small objects where any header would comprise a significant
 * fraction of the space.
 */

enum shmem_used_flags {
    SHMEM_USED_DEBUG = 1 << 0
};

/**
 * @brief Used entry header.
 */
struct shmem_used_entry {
    /**
     * @brief Allocated data length.
     *
     * Actual memory used is the larger of the free list entry and used
     * header + bucket size.  If this is an overflow allocation which doesn't
     * fit into our largest bucket it's actual size plus the used entry.
     */
    size_t len;

    /*
     * Count of number of calls to "free" before actually releasing.
     */
    short int free_count;

    /** @brief Arena class from which this came */
    enum plat_shmem_arena arena_class;

#ifdef PLAT_SHMEM_NEW_HEADERS
    int flags;

    /** @brief  Index into shmem debugging stack backtrace structure */
    int backtrace;
#endif /* def PLAT_SHMEM_NEW_HEADERS */

    /* Force sizeof struct shmem_used_entry to include alignment padding */

    int64_t data[0] __attribute__((aligned(DEFAULT_ALIGN_BYTES)));
};

#ifndef PLAT_SHMEM_FAKE

static __thread struct sa_thread_local sa_thread_local;

static struct shmem_alloc_attached shmem_alloc_attached;

static plat_shmem_ptr_base_t sa_steal_from_heap(struct shmem_alloc *local_alloc,
                                                size_t len, int backtrace);

static void sa_get_stats_internal(const struct plat_shmem_attached *attached,
                                  struct shmem_alloc *shmem_alloc,
                                  struct plat_shmem_alloc_stats *total_stats,
                                  struct plat_shmem_alloc_stats *
                                  arena_stat_array);
static struct sa_stat_handles *sa_stat_handles_alloc(struct shmem_alloc *shmem_alloc);

static void sa_stat_handles_free(struct sa_stat_handles *handles);
static void sa_stat_setter(void *extra);
static struct sa_stat_arena_handles *sa_stat_arena_handles_alloc(enum plat_shmem_arena index);
static void sa_stat_arena_handles_free(struct sa_stat_arena_handles *handles);

static sa_arena_sp_t sa_create_arena(struct shmem_alloc *local_alloc,
                                     enum plat_shmem_arena arena_class);

static void sa_arena_init_common(struct shmem_alloc *local_alloc,
                                 struct sa_arena *arena,
                                 enum plat_shmem_arena arena_class);
static void sa_destroy_thread_arena(struct shmem_alloc *shmem_alloc, sa_arena_sp_t arena);
static plat_shmem_ptr_base_t sa_arena_alloc(struct shmem_alloc *shmem_alloc,
                                            int config_flags,
                                            struct sa_arena *arena, size_t len,
                                            int flags, int backtrace);
static void sa_internal_free(struct shmem_alloc *shmem_alloc, int config_flags,
                             const char *file, unsigned line,
                             const char *function,
                             int log_cat, plat_shmem_ptr_base_t base,
                             size_t size, int free_count);
static size_t sa_arena_len(struct shmem_alloc *shmem_alloc);

static void sa_arena_add_stats(struct shmem_alloc *shmem_alloc,
                               struct sa_arena *arena,
                               struct plat_shmem_alloc_stats *arena_stats,
                               struct plat_shmem_alloc_stats *stats);

static int sa_bucket_grow_locked(struct shmem_alloc *local_alloc,
                                 struct sa_arena *arena,
                                 struct sa_bucket *bucket, int bucket_index);
static void sa_bucket_shrink_locked(struct shmem_alloc *shmem_alloc,
                                    struct sa_arena *arena,
                                    struct sa_bucket *bucket,
                                    int bucket_index,
                                    size_t retain_object_count);
static __inline__ void sa_bucket_lock(struct sa_arena *arena,
                                      struct sa_bucket *bucket);
static __inline__ void sa_bucket_unlock(struct sa_arena *arena,
                                        struct sa_bucket *bucket);

/* Align pointer to pow2 bytes */
static __inline__ void
shmem_alloc_align(plat_shmem_ptr_base_t *base, int pow2) {
        base->int_base += (1 << pow2) - 1;
        base->int_base &= ~((1 << pow2) - 1);
}

/* Get shmem alloc, returning NULL on failure */
static __inline__ PLAT_SHMEM_PURE struct shmem_alloc *
sa_get_shmem_alloc() {
    struct shmem_alloc *ret;

    ret = plat_attr_shmem_alloc_get();
    plat_shmem_debug_assert(!ret || ret->magic == SHMEM_ALLOC_MAGIC);

    return (ret);
}

/**
 * @brief Return per-thread arena, creating if necessary
 *
 * @param arena_class <IN> an actual arena class not including out-of-band
 * values like PLAT_SHMEM_ARENA_DEFAULT
 * @param shared_out <OUT> shared pointer to arena stored here when non-null
 */
static __inline__ struct sa_arena *
sa_get_create_arena(struct shmem_alloc *local_alloc,
                    enum plat_shmem_arena arena_class,
                    sa_arena_sp_t *shared_out) {
    sa_arena_sp_t new_arena;
    struct plat_shmem_arena_config *config;

    plat_shmem_debug_assert(arena_class >= 0);
    plat_shmem_debug_assert(arena_class < PLAT_SHMEM_ARENA_COUNT);

    if (PLAT_UNLIKELY(!sa_thread_local.local_arena_ptrs[arena_class])) {
        config = &local_alloc->config.arena_config[arena_class];

        switch (config->scope) {
        case PLAT_SHMEM_ARENA_SCOPE_GLOBAL:
            sa_thread_local.shared_arena_ptrs[arena_class] =
                    local_alloc->global_arenas[arena_class];
            sa_arena_sp_rwref(&sa_thread_local.local_arena_ptrs[arena_class],
                              sa_thread_local.shared_arena_ptrs[arena_class]);
            plat_assert(sa_thread_local.local_arena_ptrs[arena_class]);
            break;
        case PLAT_SHMEM_ARENA_SCOPE_THREAD:
            new_arena = sa_create_arena(local_alloc, arena_class);
            if (!sa_arena_sp_is_null(new_arena)) {
                sa_thread_local.shared_arena_ptrs[arena_class] = new_arena;
                sa_arena_sp_rwref(&sa_thread_local.local_arena_ptrs[arena_class],
                                  new_arena);
                sa_arena_local_push(&shmem_alloc_attached.local_arena_list,
                                    sa_thread_local.shared_arena_ptrs[arena_class]);
            }
            break;
        case PLAT_SHMEM_ARENA_SCOPE_INVALID:
            plat_assert_always(0);
        }
    }

    if (shared_out) {
        *shared_out = sa_thread_local.shared_arena_ptrs[arena_class];
    }

    return (sa_thread_local.local_arena_ptrs[arena_class]);
}

/* Assert if bucket index is bogus */
static __inline__ PLAT_SHMEM_CONST void
sa_check_bucket_index(struct shmem_alloc *shmem_alloc, int index) {
    plat_shmem_debug_assert(index >= shmem_alloc->min_bucket_bits);
    plat_shmem_debug_assert(index <= shmem_alloc->max_bucket_bits);
}

static __attribute__((unused)) void
sa_check_bucket_locked(struct shmem_alloc *shmem_alloc, struct sa_arena *arena,
                       struct sa_bucket *bucket, int bucket_index) {
    int list_len;
    int list_tail_index = -1;
    shmem_free_entry_sp_t shared_free;
    struct shmem_free_entry *local_free;

    if (bucket->free_count > 0) {
        shared_free = bucket->list_head;
        local_free = NULL;
        shared_free = bucket->list_head;
        list_len = 0;
        do {
            ++list_len;
            shmem_free_entry_sp_rwref(&local_free, shared_free);
            if (shmem_free_entry_sp_eq(shared_free, bucket->list_tail)) {
                list_tail_index = list_len;
            }
            shared_free = local_free->next;
        } while (!shmem_free_entry_sp_is_null(shared_free));

        plat_assert_always(bucket->free_count == list_len);
        plat_assert_always(bucket->free_count == list_tail_index);
    } else {
        plat_assert_always(shmem_free_entry_sp_is_null(bucket->list_head));
        plat_assert_always(shmem_free_entry_sp_is_null(bucket->list_tail));
    }
}

/* Calculate bucket */
static __inline__ PLAT_SHMEM_CONST int
sa_get_bucket_index(struct shmem_alloc *shmem_alloc, size_t len) {
    int limit = shmem_alloc->max_bucket_bits;
    size_t current_len;
    int ret;

    /* XXX: Could use bsr to calculate log2 */
    for (ret = shmem_alloc->min_bucket_bits, current_len = 1 << ret;
         ret <= limit && len > current_len;  ++ret, current_len <<= 1) {
    }

    return (ret);
}

static __inline__ void
sa_mutex_init(shmem_alloc_lock_t *lock, enum sa_lock_type lock_type) {
    switch (lock_type) {
    case SA_LOCK_NONE:
        break;
    case SA_LOCK_PTHREAD:
        plat_mutex_init(&lock->pthread);
        break;
    case SA_LOCK_SPIN:
#if 1
        FTH_SPIN_INIT(&lock->spin);
#else
        lock->spin = 0;
#endif
        break;
    }
}

static __inline__ void
sa_mutex_lock(shmem_alloc_lock_t *lock, enum sa_lock_type lock_type) {
    switch (lock_type) {
    case SA_LOCK_NONE:
        break;
    case SA_LOCK_PTHREAD:
        plat_mutex_lock(&lock->pthread);
        break;
    case SA_LOCK_SPIN:
#if 1
        FTH_SPIN_LOCK(&lock->spin);
#else
        while (PLAT_UNLIKELY(__sync_val_compare_and_swap(&lock->spin, 0,
                                                         __builtin_return_address(0)) != 0)) {
            __asm__ __volatile__("rep;nop" ::: "memory");
        }
#endif
    }
}

static __inline__ void
sa_mutex_unlock(shmem_alloc_lock_t *lock, enum sa_lock_type lock_type) {
    switch (lock_type) {
    case SA_LOCK_NONE:
        break;
    case SA_LOCK_PTHREAD:
        plat_mutex_unlock(&lock->pthread);
        break;
    case SA_LOCK_SPIN:
#if 1
        FTH_SPIN_UNLOCK(&lock->spin);
#else
        asm __volatile__("sfence":::"memory");
        lock->spin = 0;
#endif
        break;
    }
}

/**
 * @brief Return required bucket size for given payload
 *
 * FIXME: Needs to be based on both payload size and flags.  May want to
 * have separate allocators instead of fixed-sized buckets.
 */
static __inline__ PLAT_SHMEM_CONST size_t
sa_get_alloc_size(size_t payload) {
    return (max(sizeof (struct shmem_free_entry),
                sizeof (struct shmem_used_entry) + payload));
}
#endif /* ndef PLAT_SHMEM_FAKE */

int
plat_shmem_alloc_get_stats(struct plat_shmem_alloc_stats *stats) {
#ifdef PLAT_SHMEM_FAKE
    memset(stats, 0, sizeof (*stats));
    return (0);
#else
    int ret;
    struct shmem_alloc *shmem_alloc = sa_get_shmem_alloc();
    const struct plat_shmem_attached *attached = plat_shmem_get_attached();

    if (!shmem_alloc) {
        ret = -PLAT_ENOTATTACHED;
    } else {
        sa_get_stats_internal(attached, shmem_alloc, stats, NULL);
        ret = 0;
    }

    return (ret);
#endif
}

plat_shmem_ptr_base_t
plat_shmem_steal_from_heap(size_t len) {
    plat_shmem_ptr_base_t ret;

#ifdef PLAT_SHMEM_FAKE
    ret.ptr = sys_malloc(len);
#else
    ret = sa_steal_from_heap(sa_get_shmem_alloc(), len, SHMEM_BACKTRACE_NONE);
#endif
    return (ret);
}

#ifndef PLAT_SHMEM_FAKE

int
shmem_alloc_attach(int create) {
    int ret = 0;
    shmem_header_sp_t shared_header = {};
    struct shmem_header *local_header = NULL;
    shmem_admin_sp_t shared_admin = shmem_admin_sp_null;
    struct shmem_admin *local_admin = NULL;
    shmem_alloc_sp_t shared_alloc = shmem_alloc_sp_null;
    struct shmem_alloc *local_alloc = NULL;
    const struct plat_shmem_attached *attached = plat_shmem_get_attached();
    int config_flags = attached->config_flags;
    int i;
    int physmem_count;
    size_t len;
    plat_shmem_ptr_base_t end_of_data;
    plat_shmem_ptr_base_t end_of_segment;
    enum plat_shmem_arena arena_class;
    sa_arena_sp_t new_arena;

    /* First real segment */
    shared_header.base = plat_shmem_first_segment();

    /* Check if already attached */
    if (!sa_get_shmem_alloc()) {
        shmem_header_sp_rwref(&local_header, shared_header);
        if (!local_header) {
            plat_log_msg(20963, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "shmem header " PLAT_SP_FMT " maps to NULL",
                         PLAT_SP_FMT_ARG(shared_header));
            ret = EINVAL;
        }
    } else {
        ret = EBUSY;
    }

    /*
     * Help coverity since it doesn't grasp that !ret implies local_header
     * and therefore thinks the following local_header->magic may be a NULL
     * dereference.
     */
    plat_assert(ret || local_header);

    if (!ret && local_header->magic != SHMEM_HEADER_MAGIC) {
        plat_log_msg(20964, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "shmem header magic %x not %x",
                     local_header->magic, SHMEM_HEADER_MAGIC);
        ret = PLAT_EBADMAGIC;
    }

    if (!ret) {
        shared_admin = local_header->admin;
        shmem_admin_sp_rwref(&local_admin, shared_admin);
        plat_assert_imply(!shmem_admin_sp_is_null(shared_admin), local_admin);
    }
    if (!ret && local_admin->magic != SHMEM_ADMIN_MAGIC) {
        plat_log_msg(20965, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "shmem admin magic %x not %x",
                     local_header->magic, SHMEM_ADMIN_MAGIC);
        ret = PLAT_EBADMAGIC;
    }

    if (!ret) {
        shared_alloc = local_admin->alloc_state;
        shmem_alloc_sp_rwref(&local_alloc, shared_alloc);
        plat_assert_imply(!shmem_alloc_sp_is_null(shared_alloc), local_alloc);
        if (local_alloc) {
            plat_log_msg(20966, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "Reattached shmem_alloc at " PLAT_SP_FMT,
                         PLAT_SP_FMT_ARG(shared_alloc));
        }
    }

    if (!ret && !local_alloc && !create) {
        ret = -ENOENT;
    }

    if (!ret && !local_alloc && create) {
        shared_alloc.base =
            plat_shmem_ptr_base_add(shared_admin.base,
                                    sizeof (struct shmem_admin));
        shmem_alloc_align(&shared_alloc.base, DEFAULT_ALIGN_SHIFT);

        len = sizeof (struct shmem_alloc) + (DEFAULT_MAX_BUCKET_BITS + 1) *
            sizeof (struct sa_bucket);

        end_of_data = plat_shmem_ptr_base_add(shared_alloc.base, len);

        end_of_segment = plat_shmem_ptr_base_add(shared_header.base,
                                                 local_header->self.len);

        if (plat_shmem_ptr_base_cmp(end_of_data, end_of_segment) >= 0) {
            plat_log_msg(20967, LOG_CAT, PLAT_LOG_LEVEL_WARN,
                         "end_of_data >= end_of_segment");
            ret = -ENOMEM;
        }
        if (!ret) {
            shmem_alloc_sp_rwref(&local_alloc, shared_alloc);

            memset(local_alloc, 0, sizeof (*local_alloc));

            local_alloc->lock_type =
                (config_flags & PLAT_SHMEM_CONFIG_PTHREAD_LOCKS) ?
                SA_LOCK_PTHREAD : SA_LOCK_SPIN;
            sa_mutex_init(&local_alloc->lock, local_alloc->lock_type);

            local_alloc->flags = 0;

            /*
             * XXX: We need split arenas or free lists to support dual
             * allocations so we force all or nothing for now.
             */
            for (i = 1, physmem_count = 0; i < attached->nsegments; ++i) {
                if (attached->segments[i].paddr) {
                    ++physmem_count;
                }
            }

            /* Skip NULL */
            if (physmem_count != attached->nsegments - 1) {
                plat_log_msg(20968, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                             "Physmem disabled - not all segments have mapping");
            } else if (physmem_count) {
                plat_log_msg(20969, LOG_CAT,
                             PLAT_LOG_LEVEL_DIAGNOSTIC,
                             "Physmem allocation enabled");

                local_alloc->flags |= SA_FLAG_PHYSMEM;
            }

            local_alloc->config = local_admin->alloc_config;

            local_alloc->min_bucket_bits = DEFAULT_MIN_BUCKET_BITS;
            local_alloc->max_bucket_bits = DEFAULT_MAX_BUCKET_BITS;
            local_alloc->max_object_size = 1 << local_alloc->max_bucket_bits;


            /* With segment 0 used for the shmem null pointer */
            local_alloc->current_segment = 1;

            local_alloc->end_of_data = end_of_data;
            shmem_alloc_align(&local_alloc->end_of_data, DEFAULT_ALIGN_SHIFT);

            local_alloc->end_of_segment = end_of_segment;

            local_alloc->unusable_bytes = 0;
            local_alloc->stolen_bytes = 0;

            sa_arena_init_common(local_alloc, &local_alloc->root_arena,
                                 PLAT_SHMEM_ARENA_ROOT);

            plat_spin_rw_init(local_alloc->all_arena_list_lock);

            sa_arena_global_lll_init(&local_alloc->all_arena_list);

            local_alloc->global_arenas[PLAT_SHMEM_ARENA_ROOT].base =
                plat_shmem_ptr_base_add(shared_alloc.base,
                                        PLAT_OFFSET_OF(struct shmem_alloc,
                                                       root_arena));

            sa_arena_global_push_nospin(&local_alloc->all_arena_list,
                                        local_alloc->global_arenas[PLAT_SHMEM_ARENA_ROOT]);
        }

        for (arena_class = (enum plat_shmem_arena)0;
             !ret && arena_class < PLAT_SHMEM_ARENA_COUNT;
             ++arena_class) {
            if (arena_class != PLAT_SHMEM_ARENA_ROOT &&
                local_alloc->config.arena_config[arena_class].scope ==
                PLAT_SHMEM_ARENA_SCOPE_GLOBAL) {
                new_arena = sa_create_arena(local_alloc, arena_class);
                if (sa_arena_sp_is_null(new_arena)) {
                    ret = -ENOMEM;
                } else {
                    local_alloc->global_arenas[arena_class] = new_arena;
                }
            }
        }

        if (!ret) {
            /* Mark valid as last action */
            local_alloc->magic = SHMEM_ALLOC_MAGIC;
            local_admin->alloc_state = shared_alloc;

            plat_log_msg(20970, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "Created shmem_alloc at " PLAT_SP_FMT,
                         PLAT_SP_FMT_ARG(shared_alloc));
        }
    }

    if (!ret) {
        shmem_alloc_attached.stat_handles = sa_stat_handles_alloc(local_alloc);
        if (!shmem_alloc_attached.stat_handles) {
            plat_log_msg(20971, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "Unable to initialize shared memory statistics");
            ret = -EIO;
        }
    }

    if (!ret) {
        shmem_alloc_attached.local_alloc = local_alloc;
        sa_arena_local_lll_init(&shmem_alloc_attached.local_arena_list);
    }

    if (!ret) {
        plat_attr_shmem_alloc_set(local_alloc);
#if 0
        shmem_alloc_pthread_started();
#endif
    } else {
        shmem_alloc_sp_rwrelease(&local_alloc);
        shmem_alloc_attached.local_alloc = NULL;
        shmem_alloc_attached.stat_handles = NULL;
    }

    /* Release all dangling references */
    shmem_header_sp_rwrelease(&local_header);
    shmem_admin_sp_rwrelease(&local_admin);

    return (ret);
}

void
shmem_alloc_detach() {
    struct shmem_alloc *local_alloc = sa_get_shmem_alloc();

    if (local_alloc) {
        if (shmem_alloc_attached.stat_handles) {
            sa_stat_handles_free(shmem_alloc_attached.stat_handles);
            shmem_alloc_attached.stat_handles = NULL;
        }

        shmem_alloc_pthread_done();

        if (!sa_arena_local_is_empty(&shmem_alloc_attached.local_arena_list)) {
            plat_log_msg(20972, LOG_CAT, PLAT_LOG_LEVEL_WARN,
                         "Detaching shared memory with existing local arenas"
                         " threads must call #plat_shmem_pthread_done()");
        }

#ifdef notyet
        /*
         * XXX: drew 2009-02-19 Removing this kludges around
         * sdfclien/tests/sdfclient_flush_object and similar tests which
         * may do a detach and re-attach without shutting down pthreads.
         *
         * This can't easily be integrated into
         */

        /*
         * Thread arenas are destroyed in reverse order, so thread scope
         * arenas can have other thread scope arenas as their parent.
         */
        while (!sa_arena_local_is_empty(&shmem_alloc_attached.local_arena_list)) {
            sa_destroy_thread_arena(local_alloc,
                                    sa_arena_local_tail(&shmem_alloc_attached.local_arena_list));

        }
#endif /* def not yet */

        shmem_alloc_attached.local_alloc = NULL;
        plat_attr_shmem_alloc_set(NULL);
        shmem_alloc_sp_rwrelease(&local_alloc);
    }
}

void
shmem_alloc_pthread_started() {
    const struct plat_shmem_attached *attached = plat_shmem_get_attached();
    enum plat_shmem_arena i;

    if (attached &&
        !(attached->config_flags & PLAT_SHMEM_CONFIG_DEBUG_ONE_ARENA)) {
        struct shmem_alloc *local_alloc = sa_get_shmem_alloc();

        if (local_alloc) {
            for (i = (enum plat_shmem_arena)0; i < PLAT_SHMEM_ARENA_COUNT;
                 ++i) {
                sa_get_create_arena(local_alloc, i, NULL);
            }
        }
    }
}

void
shmem_alloc_pthread_done() {
    int i;
    struct shmem_alloc *local_alloc = sa_get_shmem_alloc();
    struct sa_arena *local_arena;

    if (local_alloc) {
        /* In reverse order for if we add more interesting hierchies */
        for (i = PLAT_SHMEM_ARENA_COUNT - 1; i >= 0; --i) {
            local_arena = sa_thread_local.local_arena_ptrs[i];
            if (local_arena && local_arena->config.scope ==
                PLAT_SHMEM_ARENA_SCOPE_THREAD) {
                sa_destroy_thread_arena(local_alloc,
                                        sa_thread_local.shared_arena_ptrs[i]);
            }
            sa_thread_local.shared_arena_ptrs[i] = sa_arena_sp_null;
            sa_thread_local.local_arena_ptrs[i] = NULL;
        }
    }
}

/**
 * @brief Advance to next shmem segment if the current one is full
 *
 * Precondition: shmem_alloc lock is held
 *
 * @return errno on failure, 0 on success
 */
static __inline__ int
sa_next_segment_locked(struct shmem_alloc *shmem_alloc) {
    const struct plat_shmem_attached *attached = plat_shmem_get_attached();
    struct plat_shmem_attached_segment *segment;
    uint64_t unusable_bytes;
    int ret;

    plat_shmem_ptr_base_t start_of_segment;

    /*
     * XXX: Should probably verify that the new segment has a valid header and
     * not just assume that it's there.
     */
    if (shmem_alloc->current_segment + 1 < attached->nsegments) {
        unusable_bytes = plat_shmem_ptr_base_diff(shmem_alloc->end_of_segment,
                                                  shmem_alloc->end_of_data);

        ++shmem_alloc->current_segment;

        segment = &attached->segments[shmem_alloc->current_segment];
        start_of_segment = plat_shmem_get_segment(shmem_alloc->current_segment);
        shmem_alloc->end_of_data = plat_shmem_ptr_base_add(start_of_segment,
                                                           segment->header_len);
        shmem_alloc->end_of_segment = plat_shmem_ptr_base_add(start_of_segment,
                                                              segment->len);

        (void) __sync_add_and_fetch(&shmem_alloc->unusable_bytes,
                                    unusable_bytes);

        plat_log_msg(20973, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "switched to segment %d of %d skipping %llu"
                     " bytes in last segment", shmem_alloc->current_segment,
                     attached->nsegments, (unsigned long long)unusable_bytes);
        ret = 0;
    } else {
        ret = ENOMEM;
    }

    return (ret);
}

/*
 * The PLAT_SHMEM_ALLOC_INTERNAL flag can be used to allocate instrumentation
 * data without recursing into the instrumentation.
 *
 * FIXME: We should push the log out to a higher level allocation helper
 * so that we don't have the extra stack traffic when logging is enabled.
 */
plat_shmem_ptr_base_t
plat_shmem_alloc_helper(const char *file, unsigned line, const char *function,
                        int log_cat, size_t len,
                        enum plat_shmem_arena arena_arg, int flags) {
    plat_shmem_ptr_base_t ret;
    struct shmem_alloc *shmem_alloc = sa_get_shmem_alloc();
    struct sa_arena *arena;
    int backtrace;
    int config_flags = plat_shmem_get_attached()->config_flags;
    enum plat_shmem_arena arena_index = arena_arg != PLAT_SHMEM_ARENA_DEFAULT ?
        arena_arg :  shmem_alloc->config.default_arena;

    plat_assert_imply(flags & PLAT_SHMEM_ALLOC_LOCAL,
                      (config_flags & PLAT_SHMEM_CONFIG_DEBUG_REPLACE_MALLOC) ||
                      arena_arg != PLAT_SHMEM_ARENA_DEFAULT);

    plat_shmem_debug_assert(shmem_alloc);

    /* XXX: maybe do this based on log-cat instead of all or nothing? */
    if (PLAT_UNLIKELY(!(flags & PLAT_SHMEM_ALLOC_INTERNAL)) &&
        (config_flags & PLAT_SHMEM_CONFIG_DEBUG_ALLOC)) {
        backtrace = shmem_save_backtrace();
    } else {
        backtrace = -1;
    }

    /*
     * Pass requests to alloc if requested for testing (shmem doesn't
     * work accross proesses) and for local requests where no bounds checking
     * is needed on the arena.
     */
    if (PLAT_UNLIKELY(!(flags & PLAT_SHMEM_ALLOC_PHYS) &&
                      ((config_flags & PLAT_SHMEM_CONFIG_DEBUG_LOCAL_ALLOC) ||
                       (flags & PLAT_SHMEM_ALLOC_LOCAL)) &&
                      ((!(config_flags & PLAT_SHMEM_CONFIG_DEBUG_REPLACE_MALLOC) &&
                        shmem_alloc->config.arena_config[arena_index].used_limit == PLAT_SHMEM_ARENA_USED_UNLIMITED) ||
                       len >= shmem_alloc->max_object_size))) {
        ret.ptr = sys_malloc(len);
    } else if (PLAT_UNLIKELY(flags & PLAT_SHMEM_ALLOC_PHYS) &&
               !(shmem_alloc->flags & SA_FLAG_PHYSMEM)) {
        ret.ptr = NULL;
        plat_errno = PLAT_ENOPHYSMEM;
    } else if (PLAT_UNLIKELY(!len >= shmem_alloc->max_object_size &&
                             !(flags & PLAT_SHMEM_ALLOC_PHYS) &&
                             (config_flags & PLAT_SHMEM_CONFIG_ALLOC_LARGE))) {
        ret = sa_steal_from_heap(shmem_alloc, len, backtrace);
    } else {
        if (PLAT_UNLIKELY(config_flags & PLAT_SHMEM_CONFIG_DEBUG_ONE_ARENA)) {
            arena = &shmem_alloc->root_arena;
        } else {
            arena = sa_get_create_arena(shmem_alloc,
                                        arena_arg == PLAT_SHMEM_ARENA_DEFAULT ?
                                        shmem_alloc->config.default_arena :
                                        arena_arg, NULL);
        }

        plat_shmem_debug_assert(arena);

        ret = sa_arena_alloc(shmem_alloc, config_flags, arena, len, flags,
                             backtrace);
    }

    plat_log_msg_forward(file, line, function, PLAT_LOG_ID_INITIAL, log_cat,
                         (!plat_shmem_ptr_base_is_null(ret) ||
                          (shmem_alloc->config.arena_config[arena_index].
                           flags & PLAT_SHMEM_ARENA_FLAG_OVERFLOW_EXPECTED)) ?
                         PLAT_LOG_LEVEL_TRACE : PLAT_LOG_LEVEL_ERROR,
                         "plat_shmem_alloc(%lu) arena %s = " PLAT_SP_FMT
                         " backtrace %d", (unsigned long) len,
                         plat_shmem_arena_to_str(arena_index),
                         PLAT_SPB_FMT_ARG(ret), backtrace);

    return (ret);
}

/*
 * Do the actual free of shared memory
 *
 * FIXME: Implement using size instead of shmem_used_entry except when
 * shmem flags specify otherwise.
 */
void
plat_shmem_free_helper(const char *file, unsigned line, const char *function,
                       int log_cat, plat_shmem_ptr_base_t base, size_t size,
                       int free_count) {
    int config_flags = plat_shmem_get_attached()->config_flags;
    struct shmem_alloc *shmem_alloc;

    /*
     * If we've replaced shmem with local memory or have a local
     * allocation in the unified allocator case (because it was
     * too big for the shmem allocator) free with the system
     * code.
     */
    if ((config_flags & PLAT_SHMEM_CONFIG_DEBUG_LOCAL_ALLOC) ||
        !plat_shmem_ptr_is_shared(base)) {
        sys_free(base.ptr);
    } else if (plat_shmem_ptr_base_is_null(base)) {
    } else {
        shmem_alloc = sa_get_shmem_alloc();
        plat_shmem_debug_assert(shmem_alloc);

        sa_internal_free(shmem_alloc, config_flags,
                         file, line, function, log_cat, base, size, free_count);
    }
}

static plat_shmem_ptr_base_t
sa_steal_from_heap(struct shmem_alloc *shmem_alloc, size_t len, int backtrace) {
    int done;
    plat_shmem_ptr_base_t ret;
    size_t alloc_len;
    struct shmem_used_entry *local_used;

    alloc_len = sa_get_alloc_size(len);

    sa_mutex_lock(&shmem_alloc->lock, shmem_alloc->lock_type);

    for (done = 0; !done; ) {
        if (plat_shmem_ptr_base_diff(shmem_alloc->end_of_segment,
                                     shmem_alloc->end_of_data) >= alloc_len) {
            local_used =
                (struct shmem_used_entry *)plat_shmem_ptr_base_to_ptr(shmem_alloc->end_of_data);
            local_used->len = len;
            local_used->free_count = 0;
            local_used->arena_class = PLAT_SHMEM_ARENA_STOLEN;
#ifdef PLAT_SHMEM_NEW_HEADERS
            local_used->backtrace = backtrace;
#endif /* def PLAT_SHMEM_NEW_HEADERS */

            ret = plat_shmem_ptr_base_add(shmem_alloc->end_of_data,
                                          sizeof (struct shmem_used_entry));

            shmem_alloc->end_of_data =
                plat_shmem_ptr_base_add(shmem_alloc->end_of_data, alloc_len);
            (void) __sync_add_and_fetch(&shmem_alloc->stolen_bytes, alloc_len);
            done = 1;
        } else {
            done = sa_next_segment_locked(shmem_alloc);
            ret = plat_shmem_ptr_base_null;
        }
    }

    sa_mutex_unlock(&shmem_alloc->lock, shmem_alloc->lock_type);

    return (ret);
}

/**
 * @brief Aggregate statistics
 *
 * @param attached <IN> Return of plat_shmem_get_attached();
 * @parma shmem_alloc <IN> Allocator
 * @param total_stats <IN> Aggregate statistics accross all arenas are
 * accumulated here.
 *
 * @param arena_stat_array <IN> Optional array of stats structures
 * sized to PLAT_SHMEM_ARENA_COUNT.  Sub-arenas have their stats
 * stored in the first ancestor's area which has te
 * #PLAT_SHMEM_ARENA_FLAG_STATS
 */

static void
sa_get_stats_internal(const struct plat_shmem_attached *attached,
                      struct shmem_alloc *shmem_alloc,
                      struct plat_shmem_alloc_stats *total_stats,
                      struct plat_shmem_alloc_stats *arena_stat_array) {
    sa_arena_sp_t shared_arena;
    struct sa_arena *local_arena;
    struct plat_shmem_alloc_stats *arena_stats;
    struct plat_shmem_arena_config *arena_config;

    memset(total_stats, 0, sizeof (*total_stats));

    if (arena_stat_array) {
        memset(arena_stat_array, 0, PLAT_SHMEM_ARENA_COUNT *
               sizeof (arena_stat_array[0]));
    }

    plat_spin_rw_rdlock(shmem_alloc->all_arena_list_lock);

    for (shared_arena =
         sa_arena_global_head(&shmem_alloc->all_arena_list),
         local_arena = NULL;
         !sa_arena_sp_is_null(shared_arena);
         shared_arena = local_arena->global_list_entry.next) {
        sa_arena_sp_rwref(&local_arena, shared_arena);
        arena_stats = NULL;
        if (arena_stat_array) {
            enum plat_shmem_arena index;

            /* XXX: drew 2008-12-15 Could maintain this statically */
            index = local_arena->arena_class;
            while (!arena_stats && index != PLAT_SHMEM_ARENA_HEAP &&
                   index >= 0) {
                arena_config = &shmem_alloc->config.arena_config[index];
                if (arena_config->flags & PLAT_SHMEM_ARENA_FLAG_STATS) {
                    arena_stats = &arena_stat_array[index];
                } else if (arena_config->parent != PLAT_SHMEM_ARENA_HEAP) {
                    index = arena_config->parent;
                }
            }
        }

        sa_arena_add_stats(shmem_alloc, local_arena, arena_stats, total_stats);
    }

    sa_arena_sp_rwrelease(&local_arena);

    plat_spin_rw_rdunlock(shmem_alloc->all_arena_list_lock);

    /*
     * XXX: drew 2008-11-24 The segment headers should be added here
     * although they're small.
     */
    total_stats->unusable_bytes = shmem_alloc->unusable_bytes;
    total_stats->total_bytes = (char *)attached->virt_end -
        (char *)attached->virt_map;
    total_stats->stolen_bytes = shmem_alloc->stolen_bytes;
}

/**
 * @brief Allocate sa_stat_handles
 */
static struct sa_stat_handles *
sa_stat_handles_alloc(struct shmem_alloc *shmem_alloc) {
    int failed;
    struct sa_stat_handles *stat_handles;
    enum plat_shmem_arena index;

    stat_handles = sys_calloc(1, sizeof (*stat_handles));
    failed = !stat_handles;
#define item(type, name) \
    if (!failed &&                                                             \
        !(stat_handles->totals.name =                                          \
          plat_stat_register("platform/shmem/alloc/total", #name, NULL,        \
                             PLAT_LOG_LEVEL_DEBUG, NULL /* fn */,               \
                             NULL /* extra */))) {                             \
        failed = 1;                                                            \
    }
    PLAT_SHMEM_STAT_ITEMS()
#undef item

    for (index = (enum plat_shmem_arena)0;
         !failed && index < PLAT_SHMEM_ARENA_COUNT; ++index) {
        if (shmem_alloc->config.arena_config[index].flags &
            PLAT_SHMEM_ARENA_FLAG_STATS) {
            stat_handles->per_arena[index] = sa_stat_arena_handles_alloc(index);
            failed = !stat_handles->per_arena[index];
        }
    }

    if (!failed) {
        stat_handles->setter =
            plat_stat_setter_register(&sa_stat_setter, stat_handles);
        failed = !stat_handles->setter;
    }

    if (failed && stat_handles) {
        sa_stat_handles_free(stat_handles);
        stat_handles = NULL;
    }

    return (stat_handles);
}

/**
 * @brief Free sa_stat_handles
 *
 * @param handles <IN> handles which may be incompletely intialized
 */
static void
sa_stat_handles_free(struct sa_stat_handles *handles) {
    enum plat_shmem_arena index;

    if (handles->setter) {
        plat_stat_setter_remove(handles->setter);
        handles->setter = NULL;
    }

    for (index = (enum plat_shmem_arena)0; index < PLAT_SHMEM_ARENA_COUNT;
         ++index) {
        if (handles->per_arena[index]) {
            sa_stat_arena_handles_free(handles->per_arena[index]);
        }
    }

#define item(type, name) \
    if (handles->totals.name) {                                                \
        plat_stat_remove(handles->totals.name);                                \
        handles->totals.name = NULL;                                           \
    }
    PLAT_SHMEM_STAT_ITEMS()
#undef item

    sys_free(handles);
}

/**
 * @brief Callback for collecting platform/stats.h shmem stats
 *
 * To avoid initialization ordering issues, the right magic
 * still happens whether or not the arena has been attached.
 */
static void
sa_stat_setter(void *extra) {
    struct sa_stat_handles *handles = (struct sa_stat_handles *)extra;
    const struct plat_shmem_attached *attached = plat_shmem_get_attached();
    struct shmem_alloc *shmem_alloc = sa_get_shmem_alloc();

    struct plat_shmem_alloc_stats total_stats;
    struct plat_shmem_alloc_stats arena_stats[PLAT_SHMEM_ARENA_COUNT];

    enum plat_shmem_arena index;
    struct sa_stat_arena_handles *arena_handles;

    if (attached && shmem_alloc) {
        sa_get_stats_internal(attached, shmem_alloc, &total_stats, arena_stats);

#define item(type, name) \
        plat_stat_set(handles->totals.name, total_stats.name, PLAT_STAT_SET);
        PLAT_SHMEM_STAT_ITEMS()
#undef item

        for (index = (enum plat_shmem_arena)0; index < PLAT_SHMEM_ARENA_COUNT;
             ++index) {
            arena_handles = handles->per_arena[index];

            if (arena_handles) {
#define item(type, name) \
                plat_stat_set(arena_handles->name, arena_stats[index].name,    \
                              PLAT_STAT_SET);
                PLAT_SHMEM_STAT_BUCKET_ITEMS()
                PLAT_SHMEM_STAT_ARENA_ITEMS()
#undef item
            }
        }
    }
}

static struct sa_stat_arena_handles *
sa_stat_arena_handles_alloc(enum plat_shmem_arena index) {
    int failed;
    struct sa_stat_arena_handles *ret;

    const char *arena_name = plat_shmem_arena_to_str(index);

    ret = sys_calloc(1, sizeof (*ret));
    failed = !ret;

#define item(type, name) \
    if (!failed &&                                                             \
        !(ret->name =                                                          \
          plat_stat_register("platform/shmem/alloc", arena_name, #name,        \
                             PLAT_LOG_LEVEL_DEBUG, NULL /* fn */,               \
                             NULL /* extra */))) {                             \
        failed = 1;                                                            \
    }

    PLAT_SHMEM_STAT_BUCKET_ITEMS()
    PLAT_SHMEM_STAT_ARENA_ITEMS()

#undef item

    if (failed && ret) {
        sa_stat_arena_handles_free(ret);
        ret = NULL;
    }

    return (ret);
}

static void
sa_stat_arena_handles_free(struct sa_stat_arena_handles *handles) {

#define item(type, name) \
    if (handles->name) {                                                       \
        plat_stat_remove(handles->name);                                       \
        handles->name = NULL;                                                  \
    }

    PLAT_SHMEM_STAT_BUCKET_ITEMS()
    PLAT_SHMEM_STAT_ARENA_ITEMS()

#undef item

    sys_free(handles);
}

/**
 * @brief Create new thread-local arena
 *
 * The arena has the root arena as a parent and is included in the global
 * and local lists used for cleanup and statistics gathering.
 *
 * @param local_alloc <IN> shmem_alloc structure which may be incompletely
 * initialized because it's a global arena class
 * @param arena_class <IN> arena class
 */

static sa_arena_sp_t
sa_create_arena(struct shmem_alloc *local_alloc,
                enum plat_shmem_arena arena_class) {

    sa_arena_sp_t shared_arena;
    size_t len;
    int failed = 0;
    struct sa_arena *local_arena = NULL;

    len = sa_arena_len(local_alloc);

    shared_arena.base = sa_arena_alloc(local_alloc, 0 /* config flags */,
                                       &local_alloc->root_arena, len,
                                       PLAT_SHMEM_ALLOC_INTERNAL /* flags */,
                                       SHMEM_BACKTRACE_NONE);

    failed = plat_shmem_ptr_base_is_null(shared_arena.base);

    if (!failed) {
        sa_arena_sp_rwref(&local_arena, shared_arena);

        sa_arena_init_common(local_alloc, local_arena, arena_class);
        sa_arena_global_el_init(shared_arena);
        sa_arena_local_el_init(shared_arena);

        plat_spin_rw_wrlock(local_alloc->all_arena_list_lock);
        sa_arena_global_push_nospin(&local_alloc->all_arena_list,
                                    shared_arena);
        plat_spin_rw_wrunlock(local_alloc->all_arena_list_lock);

    }

    sa_arena_sp_rwrelease(&local_arena);

    if (failed) {
        /* No previous log */
        plat_log_msg(20974, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "Unable to create arena %s",
                     plat_shmem_arena_to_str(arena_class));
    }

    return (shared_arena);
}

/**
 * @brief Initialize allocation arena (common code)
 *
 * Allocation and initialization are separate so that the root arena can be
 * stored in the allocation header.  This marginally simplifies startup
 * and debugging.
 *
 * The shared linked list entries are not initialized.
 *
 * Per-thread arenas must be initialized within the thread they
 * are being created for so that the lookup for parent arenas
 * is correct.
 *
 * @param local_alloc <IN> Local pointer the #shmem_alloc structure.
 * Initialization may not be finalized (the magic numbers may still
 * be unset).
 * @param arena <OUT> Local pointer to arena being initialized.
 * @param arena_class <IN> Class of arena being initialized
 * @param lock_type <IN> Type of locking
 */

/*
 * FIXME: Should take a shared pointer to the arena code, which will in
 * turn let us add it to the global list
 */

static void
sa_arena_init_common(struct shmem_alloc *local_alloc, struct sa_arena *arena,
                     enum plat_shmem_arena arena_class) {
    int i;
    struct sa_bucket *bucket;
    size_t alloc_size;

    plat_assert_always(arena_class >= 0);
    plat_assert_always(arena_class < PLAT_SHMEM_ARENA_COUNT);

    memset(arena, 0, sizeof (*arena));

    arena->magic.integer = SHMEM_ARENA_MAGIC;
    arena->arena_class = arena_class;
    arena->config = local_alloc->config.arena_config[arena_class];

    if (arena->config.parent == PLAT_SHMEM_ARENA_INVALID) {
        arena->parent = sa_arena_sp_null;
    } else {
        /*
         * This will cause the parent arenas to be created in order
         * so they can destroy in reverse order.
         */
        sa_get_create_arena(local_alloc, arena->config.parent, &arena->parent);
    }

    if (arena->config.scope == PLAT_SHMEM_ARENA_SCOPE_THREAD) {
        arena->lock_type = SA_LOCK_NONE;
    } else {
        arena->lock_type = local_alloc->lock_type;
    }

    sa_mutex_init(&arena->lock, arena->lock_type);
    arena->tree_size = 0;

    for (i = 0; i <= local_alloc->max_bucket_bits; ++i) {
        bucket = &arena->buckets[i];

        if (!sa_arena_sp_is_null(arena->parent)) {
            alloc_size = sa_get_alloc_size(1 << i);

            bucket->shrink_object_threshold =
                PLAT_MIN(arena->config.shrink_object_threshold,
                         arena->config.shrink_byte_threshold / alloc_size);
        } else {
            bucket->shrink_object_threshold = INT_MAX;
        }

        sa_mutex_init(&bucket->lock, arena->lock_type);
        bucket->allocated_count = 0;
        bucket->allocated_bytes = 0;
        bucket->used_bytes = 0;
#ifdef notyet
        bucket->child_used_bytes = 0;
#endif
        bucket->total_alloc = 0;
        bucket->total_free = 0;

        bucket->end_of_data = plat_shmem_ptr_base_null;
        bucket->end_of_segment = plat_shmem_ptr_base_null;

        bucket->free_count = 0;

#ifndef notyet
        bucket->list_head = shmem_free_entry_sp_null;
        bucket->list_tail = shmem_free_entry_sp_null;
#else
        bucket->working_list = shmem_free_list_entry_sp_null;
        bucket->local_working_list = NULL;
        bucket->excess_lists = shmem_free_list_entry_sp_null;
#endif
    }
}

/**
 * @brief Destroy thread local arena
 *
 * Since the Posix thread callback scheme is not composable and
 * we have shared memory access in non-schooner programs (innodb) withouot
 * complete control over the threading environment the thread local
 * arenas may not be destroyed from within the thread itself.
 */
static void
sa_destroy_thread_arena(struct shmem_alloc *local_alloc,
                        sa_arena_sp_t shared_arena) {
    int config_flags = plat_shmem_get_attached()->config_flags;
    struct sa_arena *local_arena;
    struct sa_arena *parent_arena;
    struct sa_bucket *parent_bucket;
    struct sa_bucket *bucket;
    int i;
    size_t len;
    size_t lost_space = 0;
    size_t lost_in_bucket;

    local_arena = NULL;
    len = sa_arena_len(local_alloc);

    /* XXX: Reference by size */
    sa_arena_sp_rwref(&local_arena, shared_arena);

    plat_spin_rw_wrlock(local_alloc->all_arena_list_lock);
    sa_arena_global_remove(&local_alloc->all_arena_list, shared_arena);
    plat_spin_rw_wrunlock(local_alloc->all_arena_list_lock);

    /*
     * XXX: drew 2009-02-19 Should probably split the remove from the local
     * allocate list out and track whether we're on it to accomodate orphaned
     * arenas.
     */
    sa_arena_local_remove(&shmem_alloc_attached.local_arena_list, shared_arena);

    parent_arena = NULL;
    sa_arena_sp_rwref(&parent_arena, local_arena->parent);
    plat_assert(parent_arena);

    for (i = 0; i < local_alloc->max_bucket_bits; ++i) {
        bucket = &local_arena->buckets[i];
        parent_bucket = &parent_arena->buckets[i];

        sa_bucket_lock(local_arena, bucket);

        /* FIXME: Convert slab into free objects */

        /*
         * Move free list to parent. Instantiation in parent to child order
         * and reverse order destruction guarantees it still works.
         */
        sa_bucket_shrink_locked(local_alloc, local_arena, bucket,
                                i /* bucket index */,
                                0 /* retain object count */);

        sa_bucket_lock(parent_arena, parent_bucket);
        parent_bucket->allocated_count += bucket->allocated_count;
        parent_bucket->allocated_bytes += bucket->allocated_bytes;
        parent_bucket->used_bytes += bucket->used_bytes;
        parent_bucket->total_alloc += bucket->total_alloc;
        parent_bucket->total_free += bucket->total_free;

#ifdef notyet
        parent_bucket->child_used_bytes -= bucket->used_bytes;
        parent_bucket->child_used_bytes -= bucket->free_count *
            sa_get_alloc_size(1 << i);
#endif

        if (!plat_shmem_ptr_base_is_null(bucket->end_of_data)) {
            lost_in_bucket = plat_shmem_ptr_base_diff(bucket->end_of_segment,
                                                      bucket->end_of_data);
#ifdef notyet
            parent_bucket->child_used_bytes -= lost_in_bucket;
#endif
            lost_space += lost_in_bucket;
        }
        sa_bucket_unlock(parent_arena, parent_bucket);
        sa_bucket_unlock(local_arena, bucket);
    }

    sa_arena_sp_rwrelease(&parent_arena);

    if (lost_space) {
        plat_log_msg(20975, LOG_CAT, PLAT_LOG_LEVEL_DIAGNOSTIC,
                     "%ld bytes becoming unusable", (long)lost_space);

        (void) __sync_add_and_fetch(&local_alloc->unusable_bytes, lost_space);
    }

    /* FIXME: Call plat_shmem_free_helper directly? */

    sa_internal_free(local_alloc,
                     config_flags, __FILE__, __LINE__, __FUNCTION__, LOG_CAT,
                     shared_arena.base, len, 1 /* free count */);

    /* XXX: Release by size */
    sa_arena_sp_rwrelease(&local_arena);
}

/* XXX: Sanity check that slabs are multiples of alloc_size */

/**
 * @brief Allocate from within a specific arena
 *
 * @param local_alloc <IN> Local pointer the #shmem_alloc structure.
 * @param config_flags <IN> Configuration flags.  May be zero
 * for internal allocations which aren't following the normal config
 * rules.
 * @param arena arena <IN> Specific arena structure for this thread,
 * arena class
 * @param len <IN> allocation size request
 * @param flags <IN> flags for this request
 * @param backtrace <IN> backtrace (SHMEM_BACKTRACE_NONE) grabbed previously
 *
 * @return allocated shared pointer, plat_shmem_ptr_base_null on failure
 */
static plat_shmem_ptr_base_t
sa_arena_alloc(struct shmem_alloc *shmem_alloc,
               int config_flags, struct sa_arena *arena, size_t len, int flags,
               int backtrace) {
    struct sa_bucket *bucket;
    plat_shmem_ptr_base_t ret = PLAT_SPB_INITIALIZER;
    shmem_free_entry_sp_t shared_free = shmem_free_entry_sp_null;
    struct shmem_free_entry *local_free = NULL;
    struct shmem_used_entry *local_used = NULL;
    int bucket_index = -1;
    size_t alloc_size;
    int failed = 0;
    int retry = 0;
    plat_shmem_ptr_base_t new_end_of_data;

    /*
     * Pick a power of two bucket.  Future versions of the code will
     * have exact size buckets for common objects.
     */
    bucket_index = sa_get_bucket_index(shmem_alloc, len);
    if (PLAT_UNLIKELY(bucket_index > shmem_alloc->max_bucket_bits)) {
        failed = ENOMEM;
    } else {
        bucket = &arena->buckets[bucket_index];
        alloc_size = sa_get_alloc_size(1 << bucket_index);

        sa_bucket_lock(arena, bucket);

        do {
            retry = 0;
            /* Try to allocate from free list */
            shared_free = *(volatile shmem_free_entry_sp_t *)&bucket->list_head;

            /* We want to have an all or nothing thing for physmem */
            if (PLAT_LIKELY(!shmem_free_entry_sp_is_null(shared_free))) {
                shmem_free_entry_sp_rwref(&local_free, shared_free);

                plat_assert(bucket->free_count  > 0);

                if (local_free->magic.integer != SHMEM_FREE_MAGIC) {
                    plat_log_msg(20976, LOG_CAT,
                                 PLAT_LOG_LEVEL_FATAL, "Arena corruption "
                                 " (probable buffer overrun) at "
                                 PLAT_SP_FMT " magic number is %x",
                                 PLAT_SP_FMT_ARG(shared_free),
                                 local_free->magic.integer);
                    plat_abort();
                }

                --bucket->free_count;
                bucket->list_head = local_free->next;
                if (shmem_free_entry_sp_is_null(bucket->list_head)) {
                    bucket->list_tail = shmem_free_entry_sp_null;
                }

                plat_shmem_debug_assert_iff(shmem_free_entry_sp_is_null(bucket->list_tail),
                                            bucket->free_count == 0);

                plat_shmem_debug_assert_iff(shmem_free_entry_sp_is_null(bucket->list_head),
                                            bucket->free_count == 0);

            /* Try to allocate from bucket slab. */
            } else {
                new_end_of_data = plat_shmem_ptr_base_add(bucket->end_of_data,
                                                          alloc_size);

                if (PLAT_LIKELY(!plat_shmem_ptr_base_is_null(bucket->end_of_data) &&
                                plat_shmem_ptr_base_cmp(bucket->end_of_data,
                                                        bucket->end_of_segment) < 0 &&
                                plat_shmem_ptr_base_cmp(new_end_of_data,
                                                        bucket->end_of_segment) <= 0)) {
                    shared_free.base = bucket->end_of_data;
                    shmem_free_entry_sp_rwref(&local_free, shared_free);
                    bucket->end_of_data = new_end_of_data;
                } else if (!sa_bucket_grow_locked(shmem_alloc, arena, bucket,
                                                  bucket_index)) {
                    retry = 1;
                } else {
                    failed = ENOMEM;
                }
            }
        } while (PLAT_UNLIKELY(retry));

        if (PLAT_LIKELY(!failed)) {
#ifndef PLAT_SHMEM_NO_STATS
            ++bucket->allocated_count;
            bucket->allocated_bytes += len;
            bucket->used_bytes += alloc_size;
            ++bucket->total_alloc;
#endif
        }

        sa_bucket_unlock(arena, bucket);

        /*
         * XXX: If we get the debugging code back in here, we'll want to
         * use the reference/release code.
         */
        if (PLAT_LIKELY(!failed)) {
            plat_assert(local_free);
            local_free->magic.integer = 0;
            local_used = (struct shmem_used_entry *)local_free;
            local_used->arena_class = arena->arena_class;
            local_used->len = len;
            local_used->free_count = 0;
#ifdef PLAT_SHMEM_NEW_HEADERS
            local_used->backtrace = backtrace;
#endif /* def PLAT_SHMEM_NEW_HEADERS */

            ret = plat_shmem_ptr_base_add(shared_free.base,
                                          sizeof (struct shmem_used_entry));
        }
    }

    if (failed)  {
        plat_errno = failed;
    }

    shmem_free_entry_sp_rwrelease(&local_free);

    return (ret);
}

/**
 * @brief Actually free memory
 */
static void
sa_internal_free(struct shmem_alloc *shmem_alloc,
                 int config_flags,
                 const char *file, unsigned line, const char *function,
                 int log_cat, plat_shmem_ptr_base_t base, size_t size,
                 int free_count) {
    struct sa_arena *arena;
    struct sa_bucket *bucket;
    struct shmem_used_entry *local_used;
    struct shmem_free_entry *local_free;
    plat_shmem_ptr_base_t entry_base;
    int bucket_index;
    size_t alloc_size;
    int alloc_backtrace = SHMEM_BACKTRACE_NONE;
    int free_backtrace = SHMEM_BACKTRACE_NONE;

#ifdef notyet
    if (base.flags & PLAT_SHMEM_FLAG_VARIABLE) {
#endif
        plat_shmem_debug_assert(base.offset >=
                                sizeof (struct shmem_used_entry));
        entry_base = plat_shmem_ptr_base_sub(base,
                                             sizeof (struct shmem_used_entry));
        local_used =
            (struct shmem_used_entry *)plat_shmem_ptr_base_to_ptr(entry_base);
        plat_shmem_debug_assert(local_used);
#ifdef PLAT_SHMEM_NEW_HEADERS
        alloc_backtrace = local_used->backtrace;
#endif /* def PLAT_SHMEM_NEW_HEADERS */

        local_free = (struct shmem_free_entry *)local_used;
        bucket_index = sa_get_bucket_index(shmem_alloc, local_used->len);
#ifdef notyet
    } else {
        plat_shmem_debug_assert(size > 0);
        bucket_index = sa_get_bucket_index(shmem_alloc, size);
        local_free.base = base;

        alloc_backtrace = -1;
    }
#endif

#ifdef PLAT_SHMEM_NEW_HEADERS
    if (alloc_backtrace != SHMEM_BACKTRACE_NONE) {
        free_backtrace = shmem_save_backtrace();
    }
#endif

    arena = sa_get_create_arena(shmem_alloc, local_used->arena_class !=
                                PLAT_SHMEM_ARENA_STOLEN ?
                                local_used->arena_class :
                                PLAT_SHMEM_ARENA_ROOT, NULL);
    plat_assert(arena);

    plat_log_msg_forward(file, line, function, PLAT_LOG_ID_INITIAL,
                         log_cat, PLAT_LOG_LEVEL_TRACE,
                         "plat_shmem_free(" PLAT_SP_FMT ")"
                         " arena %s alloc backrace %d free backtrace %d",
                         PLAT_SPB_FMT_ARG(base),
                         plat_shmem_arena_to_str(arena->arena_class),
                         alloc_backtrace, free_backtrace);

    local_used->free_count++;            // Increment the number of times frees
    if (PLAT_LIKELY(local_used->free_count >= free_count)) { // If time to free
        if (PLAT_UNLIKELY(local_free->magic.integer == SHMEM_FREE_MAGIC)) {
            /*
             * XXX: For non-variable sized objects walk the appropriate
             * free list and confirm that the object is not on it.  For
             * variable sized objects search all lists.  That will let
             * us change the "probably" to "definitely."
             */
            plat_log_msg_forward(file, line, function,
                                 PLAT_LOG_ID_INITIAL, log_cat,
                                 PLAT_LOG_LEVEL_WARN,
                                 "shmem pointer " PLAT_SP_FMT
                                 " probably already free",
                                 PLAT_SPB_FMT_ARG(base));
        }

        if (PLAT_UNLIKELY(config_flags & PLAT_SHMEM_CONFIG_DEBUG_POISON)) {
            memset(local_used->data, PLAT_SHMEM_POISON_BYTE,
                   local_used->len);
        }

        /* XXX: This is a little ugly. */

        if (PLAT_UNLIKELY(bucket_index > shmem_alloc->max_bucket_bits)) {
            /* We used to check (config_flags & PLAT_SHMEM_CONFIG_ALLOC_LARGE) */
            alloc_size = sa_get_alloc_size(local_used->len);
            bucket = NULL;
        } else {
            sa_check_bucket_index(shmem_alloc, bucket_index);
            alloc_size = sa_get_alloc_size(1 << bucket_index);
            bucket = &arena->buckets[bucket_index];
        }

        local_free->magic.integer = SHMEM_FREE_MAGIC;
#ifdef PLAT_SHMEM_NEW_HEADERS
        local_free->alloc_backtrace = alloc_backtrace;
        local_free->free_backtrace = free_backtrace;
#endif /* def PLAT_SHMEM_NEW_HEADERS */

        if (PLAT_UNLIKELY(!bucket)) {
            plat_log_msg_forward(file, line, function, PLAT_LOG_ID_INITIAL,
                                 LOG_CAT, PLAT_LOG_LEVEL_WARN,
                                 "Discarding %lu bytes stolen from heap",
                                 alloc_size);

            (void) __sync_add_and_fetch(&shmem_alloc->unusable_bytes,
                                        alloc_size);
        } else {
            sa_bucket_lock(arena, bucket);

            /*
             * We don't sanity check for positive values because allocation
             * and free in separate segments result in negative values
             * in some arenas.
             *
             * If checking, we'd have to add up all the values; or use
             * a mode where we maintained a central statistics structure.
             */
#ifndef PLAT_SHMEM_NO_STATS
#if 0
            plat_assert(bucket->allocated_count > 0);
#endif
            --bucket->allocated_count;
#ifdef notyet
            /* XXX: plat_free has no size */
#if 0
            plat_assert(bucket->allocated_bytes >= size);
#endif
            bucket->allocated_bytes -= size;
#else
#if 0
            plat_assert(bucket->allocated_bytes >= local_used->len);
#endif
            bucket->allocated_bytes -= local_used->len;
#endif
#if 0
            plat_assert(bucket->used_bytes >= alloc_size);
#endif
            bucket->used_bytes -= alloc_size;
            ++bucket->total_free;
#endif /* ndef PLAT_SHMEM_NO_STATS */

            plat_shmem_debug_assert_iff(shmem_free_entry_sp_is_null(bucket->list_tail),
                                        bucket->free_count == 0);

            plat_shmem_debug_assert_iff(shmem_free_entry_sp_is_null(bucket->list_head),
                                        bucket->free_count == 0);

            ++bucket->free_count;


            local_free->next = bucket->list_head;
            bucket->list_head.base = entry_base;

            if (shmem_free_entry_sp_is_null(bucket->list_tail)) {
                bucket->list_tail.base = entry_base;
            }

            if (bucket->free_count >= bucket->shrink_object_threshold) {
                sa_bucket_shrink_locked(shmem_alloc, arena, bucket,
                                        bucket_index,
                                        (1 + bucket->shrink_object_threshold) /
                                        2);
            }

            sa_bucket_unlock(arena, bucket);
        }
    }
}

static size_t
sa_arena_len(struct shmem_alloc *shmem_alloc) {
    struct sa_arena *arena = NULL;

    return (sizeof (*arena) + (shmem_alloc->max_bucket_bits + 1) *
            sizeof (arena->buckets[0]));
}

static void
sa_arena_add_stats(struct shmem_alloc *shmem_alloc, struct sa_arena *arena,
                   struct plat_shmem_alloc_stats *arena_stats,
                   struct plat_shmem_alloc_stats *total_stats) {
#ifndef PLAT_SHMEM_NO_STATS
    int i;
    struct sa_bucket *bucket;

    for (i = shmem_alloc->min_bucket_bits; i < shmem_alloc->max_bucket_bits;
         ++i) {
        bucket = &arena->buckets[i];
#define item(type, name) \
        if (arena_stats) {                                                     \
            arena_stats->name += bucket->name;                                 \
        }                                                                      \
        if (total_stats) {                                                     \
            total_stats->name += bucket->name;                                 \
        }
        PLAT_SHMEM_STAT_BUCKET_ITEMS()
#undef item
    }

#define item(type, name) \
    if (arena_stats) {                                                         \
        arena_stats->name += arena->name;                                      \
    }                                                                          \
    if (total_stats) {                                                         \
        total_stats->name += arena->name;                                      \
    }
    PLAT_SHMEM_STAT_ARENA_ITEMS()
#undef item

#endif /* ndef PLAT_SHMEM_NO_STATS */
}

/**
 * @brief Attempt to grow a bucket which is already locked
 *
 * Precondition: bucket->lock is held (if arena is locking), there is no
 * free space left on its free list or slab, and the arena has a parent.
 *
 * @return 0 on success, -errno on error
 */
static int
sa_bucket_grow_locked(struct shmem_alloc *shmem_alloc, struct sa_arena *arena,
                      struct sa_bucket *bucket, int bucket_index) {
    int size_locks_held = 0;
    int done;
    /* Number of objects we are at tempting to grow */
    int grow_objects_desired;
    /* Number of objects we actually got */
    int got_objects;
    struct sa_arena *tree_arena = NULL;
    struct sa_arena *parent_arena;
    struct sa_bucket *parent_bucket;
    size_t alloc_size;
    size_t heap_segment_objects_remain;
    size_t heap_objects_desired;
    shmem_free_entry_sp_t new_list_head;

    shmem_free_entry_sp_t shared_free;
    struct shmem_free_entry *local_free;

    plat_shmem_debug_assert(!plat_shmem_ptr_base_is_null(bucket->end_of_data) ||
                            !plat_shmem_ptr_base_cmp(bucket->end_of_data,
                                                     bucket->end_of_segment));
    plat_shmem_debug_assert(shmem_free_entry_sp_is_null(bucket->list_head));
    plat_shmem_debug_assert(!bucket->free_count);

    alloc_size = sa_get_alloc_size(1 << bucket_index);
    grow_objects_desired = arena->config.grow_objects_desired;
    if (grow_objects_desired * alloc_size > arena->config.grow_byte_limit) {
        grow_objects_desired = arena->config.grow_byte_limit / alloc_size;
    }
    if (!grow_objects_desired) {
        grow_objects_desired = 1;
    }

#ifdef PLAT_SHMEM_DEBUG
    if (PLAT_UNLIKELY(plat_log_enabled(LOG_CAT_RESIZE, PLAT_LOG_LEVEL_DEVEL))) {
        sa_check_bucket_locked(shmem_alloc, arena, bucket, bucket_index);
    }
#endif

    parent_arena = NULL;
    sa_arena_sp_rwref(&parent_arena, arena->parent);

    if (grow_objects_desired > 0 && parent_arena) {
        parent_bucket = &parent_arena->buckets[bucket_index];

        sa_bucket_lock(parent_arena, parent_bucket);

#ifdef PLAT_SHMEM_DEBUG
        if (PLAT_UNLIKELY(plat_log_enabled(LOG_CAT_RESIZE,
                                           PLAT_LOG_LEVEL_DEVEL))) {
            sa_check_bucket_locked(shmem_alloc, parent_arena, parent_bucket,
                                   bucket_index);
        }
#endif

        /* XXX: Should we grow from the parent slab because that's cheapest? */

        got_objects = 0;

        if (parent_bucket->free_count > 0) {
            local_free = NULL;
            shared_free = parent_bucket->list_head;

            /*
             * Leave shared_free at last object which will move from
             * parent.
             */
            while (1) {
                shmem_free_entry_sp_rwref(&local_free, shared_free);
                ++got_objects;
                if (got_objects >= grow_objects_desired ||
                    shmem_free_entry_sp_is_null(local_free->next)) {
                    break;
                } else {
                    shared_free = local_free->next;
                }
            }

            new_list_head = parent_bucket->list_head;
            parent_bucket->list_head = local_free->next;
            if (shmem_free_entry_sp_is_null(parent_bucket->list_head)) {
                parent_bucket->list_tail = shmem_free_entry_sp_null;
            }
            parent_bucket->free_count -= got_objects;

            bucket->free_count += got_objects;
            bucket->list_head = new_list_head;
            bucket->list_tail = shared_free;

            local_free->next = shmem_free_entry_sp_null;

            shmem_free_entry_sp_rwrelease(&local_free);

#ifdef PLAT_SHMEM_DEBUG
            if (PLAT_UNLIKELY(plat_log_enabled(LOG_CAT_RESIZE,
                                               PLAT_LOG_LEVEL_DEVEL))) {
                sa_check_bucket_locked(shmem_alloc, arena, bucket,
                                       bucket_index);
                sa_check_bucket_locked(shmem_alloc, parent_arena,
                                       parent_bucket, bucket_index);
            }
#endif
        }

        plat_log_msg(20977, LOG_CAT_RESIZE, PLAT_LOG_LEVEL_TRACE,
                     "arena %p bucket %d grow %d objects parent remaining %llu",
                     arena, bucket_index, got_objects,
                     (unsigned long long)parent_bucket->free_count);

        sa_bucket_unlock(parent_arena, parent_bucket);

        sa_arena_sp_rwrelease(&parent_arena);

    } else {
        got_objects = 0;
    }

    grow_objects_desired -= got_objects;

    done = (!grow_objects_desired);

    /* XXX: drew 2008-12-04 This just handles the two cases we need now */
    if (!done) {
        if (parent_arena) {
            tree_arena = parent_arena;
        } else {
            tree_arena = arena;
        }

        plat_assert(tree_arena->config.parent == PLAT_SHMEM_ARENA_INVALID);

        sa_mutex_lock(&tree_arena->lock, tree_arena->lock_type);
        size_locks_held = 1;

        /*
         * Enforce arena size limits when more memory is imported from
         * the heap into an arena tree since this plays nice with lock-free
         * distributed statistics and there isn't a good reason to support
         * separate limits on arena subtrees with a root node other than the
         * heap.
         */
        if (tree_arena->tree_size + alloc_size * grow_objects_desired >
            tree_arena->config.used_limit) {
            grow_objects_desired = (tree_arena->config.used_limit -
                                    tree_arena->tree_size) / alloc_size;
            if (!grow_objects_desired) {
                done = 1;
            }
        }
    }

    /* Get more free memory from the heap */
    while (!done) {
        sa_mutex_lock(&shmem_alloc->lock, shmem_alloc->lock_type);

        heap_segment_objects_remain =
            plat_shmem_ptr_base_diff(shmem_alloc->end_of_segment,
                                     shmem_alloc->end_of_data) /
            alloc_size;

        /* Take whatever number we can get instead of adding fragmentation */
        if (heap_segment_objects_remain > 0) {
            heap_objects_desired = PLAT_MIN(heap_segment_objects_remain,
                                            grow_objects_desired);
            bucket->end_of_data = shmem_alloc->end_of_data;
            shmem_alloc->end_of_data = bucket->end_of_segment =
                plat_shmem_ptr_base_add(shmem_alloc->end_of_data,
                                        heap_objects_desired * alloc_size);

            plat_log_msg(20978, LOG_CAT_RESIZE,
                         PLAT_LOG_LEVEL_TRACE,
                         "arena %p bucket %d grow %lu objects from heap ",
                         arena, bucket_index, heap_objects_desired);

            got_objects += heap_objects_desired;
            tree_arena->tree_size += heap_objects_desired * alloc_size;
            done = 1;
        } else {
            done = sa_next_segment_locked(shmem_alloc);
        }

        sa_mutex_unlock(&shmem_alloc->lock, shmem_alloc->lock_type);
    }

    if (size_locks_held) {
        sa_mutex_unlock(&tree_arena->lock, tree_arena->lock_type);
    }

    if (parent_arena) {
        sa_arena_sp_rwrelease(&parent_arena);
    }

    return ((got_objects > 0) ? 0 : -ENOMEM);
}

/**
 * @brief Shrink a bucket which is already locked
 *
 * Precondition: bucket->lock is held (if arena is locking), there is free
 * space left, and bucket has a parent.
 *
 * @return 0 on success, -errno on error
 */
static void
sa_bucket_shrink_locked(struct shmem_alloc *shmem_alloc,
                        struct sa_arena *arena, struct sa_bucket *bucket,
                        int bucket_index, size_t retain_object_count) {

    struct sa_arena *parent_arena;
    struct sa_bucket *parent_bucket;
    shmem_free_entry_sp_t shared_free;
    shmem_free_entry_sp_t parent_new_tail;
    struct shmem_free_entry *local_free;
    size_t object_count;

    parent_arena = NULL;
    sa_arena_sp_rwref(&parent_arena, arena->parent);

    plat_shmem_debug_assert(parent_arena);

    if (bucket->free_count <= retain_object_count) {
        return;
    }

#ifdef PLAT_SHMEM_DEBUG
    if (PLAT_UNLIKELY(plat_log_enabled(LOG_CAT_RESIZE, PLAT_LOG_LEVEL_DEVEL))) {
        sa_check_bucket_locked(shmem_alloc, arena, bucket, bucket_index);
    }
#endif

    parent_bucket = &parent_arena->buckets[bucket_index];

    /* Bucket free list tail becomes parent free list tail */
    parent_new_tail = bucket->list_tail;

    /*
     * Truncate bucket free list except free_count and leave shared_free
     * pointing after end.
     */
    if (retain_object_count > 0) {
        /* Traverse bucket free list stopping at last retained object */
        local_free = NULL;
        shared_free = bucket->list_head;
        object_count = 0;
        do {
            ++object_count;
            shmem_free_entry_sp_rwref(&local_free, shared_free);
            if (object_count < retain_object_count) {
                shared_free = local_free->next;
            }
        } while (object_count < retain_object_count);

        bucket->list_tail = shared_free;
        shared_free = local_free->next;
        local_free->next = shmem_free_entry_sp_null;
    } else {
        shared_free = bucket->list_head;
        bucket->list_head = bucket->list_tail = shmem_free_entry_sp_null;
    }

    object_count = bucket->free_count - retain_object_count;
    bucket->free_count = retain_object_count;

#ifdef PLAT_SHMEM_DEBUG
    if (PLAT_UNLIKELY(plat_log_enabled(LOG_CAT_RESIZE, PLAT_LOG_LEVEL_DEVEL))) {
        sa_check_bucket_locked(shmem_alloc, arena, bucket, bucket_index);
    }
#endif

    /* Append removed free list entries to parent free list */
    sa_bucket_lock(parent_arena, parent_bucket);

    if (parent_bucket->free_count > 0) {
        plat_shmem_debug_assert(!shmem_free_entry_sp_is_null(parent_bucket->list_tail));
        shmem_free_entry_sp_rwref(&local_free, parent_bucket->list_tail);
        local_free->next = shared_free;
    } else {
        parent_bucket->list_head = shared_free;
    }
    parent_bucket->list_tail = parent_new_tail;
    parent_bucket->free_count += object_count;

    shmem_free_entry_sp_rwrelease(&local_free);

    plat_log_msg(20979, LOG_CAT_RESIZE, PLAT_LOG_LEVEL_TRACE,
                 "arena %p bucket %d shrink %lu objects remain %lu parent %lu",
                 arena, bucket_index, object_count, retain_object_count,
                 parent_bucket->free_count);

#ifdef PLAT_SHMEM_DEBUG
    if (PLAT_UNLIKELY(plat_log_enabled(LOG_CAT_RESIZE, PLAT_LOG_LEVEL_DEVEL))) {
        sa_check_bucket_locked(shmem_alloc, parent_arena, parent_bucket,
                               bucket_index);
    }
#endif

    sa_bucket_unlock(parent_arena, parent_bucket);

    sa_arena_sp_rwrelease(&parent_arena);
}

static __inline__ void
sa_bucket_lock(struct sa_arena *arena, struct sa_bucket *bucket) {
    sa_mutex_lock(&bucket->lock, arena->lock_type);
}

static __inline__ void
sa_bucket_unlock(struct sa_arena *arena, struct sa_bucket *bucket) {
    sa_mutex_unlock(&bucket->lock, arena->lock_type);
}




#endif /* ndef PLAT_SHMEM_FAKE */

void
shmem_alloc_config_init(struct shmem_alloc_config *config) {
/* BEGIN CSTYLED */
    *config = ((struct shmem_alloc_config) {
               .default_arena = PLAT_SHMEM_ARENA_DEFAULT_VALUE,
               .arena_config = {
#define item(enum_val, short_text, config_initializer) config_initializer,
        PLAT_SHMEM_ARENA_ITEMS(PLAT_SHMEM_ARENA_CONFIG_DEFAULTS)
#undef item
        },
    });
/* END CSTYLED */
}

void
shmem_alloc_config_destroy(struct shmem_alloc_config *config) {
    /* NOP - nothing is dynamic here */
}

enum plat_shmem_arena
plat_shmem_str_to_arena(const char *string, size_t len) {
#define item(enum_val, short_text, config_initializer) \
    if (0 == (len > 0 ? strncmp(string, #short_text, len) :                    \
              strcmp(string, #short_text))) {                                  \
        return (enum_val);                                                     \
    } else
    PLAT_SHMEM_ARENA_ITEMS()
#undef item
    {
        return (PLAT_SHMEM_ARENA_INVALID);
    }
}

const char *
plat_shmem_arena_to_str(enum plat_shmem_arena arena) {
    switch (arena) {
#define item(enum_val, short_text, config_initializer) \
    case enum_val: return (#short_text);
    PLAT_SHMEM_ARENA_ITEMS()
#undef item
    default:
        return ("invalid");
    }
}

static void sa_arena_usage();

int
shmem_alloc_config_parse_arena(struct shmem_alloc_config *config,
                               const char *arg) {
    int ret = 0;
    const char *field_name;
    const char *dot;
    const char *equals = NULL;
    const char *value;
    enum plat_shmem_arena arena = PLAT_SHMEM_ARENA_INVALID;
    size_t field_len;


    dot = strchr(arg, '.');
    if (!dot) {
        fprintf(stderr, "no . separator found in arena arg\n");
        ret = -EINVAL;
    }

    if (!ret) {
        arena = plat_shmem_str_to_arena(arg, dot - arg);
        if (arena == PLAT_SHMEM_ARENA_INVALID) {
            fprintf(stderr, "arena not recognized in arena arg\n");
            ret = -EINVAL;
        }
    }

    if (!ret) {
        equals = strchr(dot, '=');
        if (!equals) {
            fprintf(stderr, "no = separator found in arena arg\n");
            ret = -EINVAL;
        }
    }

    if (!ret) {
        field_name = dot + 1;
        field_len = equals - field_name;

        value = equals + 1;

        if (strncmp(field_name, "used_limit", field_len) == 0) {
            ret = parse_size(&config->arena_config[arena].used_limit,
                             value, NULL);
            if (ret) {
                fprintf(stderr, "can't parse grow_byte_limit size %s: %s\n",
                        value, plat_strerror(-ret));
            }
        } else {
            fprintf(stderr, "unrecognized field name %*s\n",
                    (int)field_len, field_name);
            ret = -EINVAL;
        }
    }

    if (ret) {
        sa_arena_usage();
    }

    return (ret);
}

void
shmem_alloc_config_set_arena_used_limit(struct shmem_alloc_config *config,
                                        enum plat_shmem_arena arena,
                                        int64_t limit) {
    plat_assert(arena >= 0 && arena < PLAT_SHMEM_ARENA_COUNT);
    config->arena_config[arena].used_limit = limit;
}

static void
sa_arena_usage() {
    fprintf(stderr, "--plat/shmem/arena arena.field=value\n");

    fprintf(stderr, "arena is one of:\n%s",
#define item(enum_val, short_text, config_initializer) \
            "\t" #short_text "\n"
            PLAT_SHMEM_ARENA_ITEMS()
#undef item
            /* for cstyle */);

    fprintf(stderr, "field is one of:\n%s",
            "\tused_limit\n");

    fprintf(stderr, "\n");
}

/* Currently arena isn't significant */
int
plat_shmem_get_buckets_alloc(enum plat_shmem_arena arena, size_t **buckets_out,
                             size_t *nbucket_out) {
    int ret = 0;

#ifdef PLAT_SHMEM_FAKE
    ret = -ENOSYS;
#else
    size_t *buckets = NULL;
    size_t nbucket = 0;
    struct shmem_alloc *local_alloc = sa_get_shmem_alloc();
    int i;
    size_t len;

    if (!local_alloc) {
        ret = -PLAT_ENOTATTACHED;
    }

    if (!ret) {
        nbucket = local_alloc->max_bucket_bits -
            local_alloc->min_bucket_bits + 1;
        buckets = plat_malloc(nbucket * sizeof (*buckets));
        if (!buckets) {
            ret = -ENOMEM;
        } else {
            for (i = 0, len = ((size_t)1) << local_alloc->min_bucket_bits;
                 i < nbucket; ++i, len <<= 1) {
                buckets[i] = len;
            }

            *buckets_out = buckets;
            *nbucket_out = nbucket;
        }
    }
#endif /* else def PLAT_SHMEM_FAKE */

    return (ret);
}
