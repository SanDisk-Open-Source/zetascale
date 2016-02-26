/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_SHMEM_ARENA_H
#define PLATFORM_SHMEM_ARENA_H 1

/*
 * File:   sdf/platform/shmem_arena.h
 * Author: drew
 *
 * Created on December 02, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmem_arena.h 9201 2009-09-12 19:47:17Z drew $
 */

/*
 * The memory allocation code supports a hierchical set of arenas for
 * performance (per-pthread arenas are used to minimize cache and TLB
 * evictions) and partitioning (the current use cases are cache,
 * flash structures, and "system" memory).
 *
 * In its current incarnation, there are global and thread local arenas.
 * Thread local arenas pull from a specific global arena which is isolated
 * from the rest except for heap contention.  The current use case is to
 * partition cache and other memory requests.
 *
 * The current implementation has separate per-thread free lists
 * per arena class (ROOT, FLASH, TEST).  Cache and TLB effects make
 * this less performant than shared lists; but shared lists require
 * some movement away from our distributed statistics.
 */

#include "platform/types.h"

/**
 * @brief Defaults for allocation arenas
 *
 * See below for macro specifics
 */
#define PLAT_SHMEM_ARENA_CONFIG_DEFAULTS \
    .scope = PLAT_SHMEM_ARENA_SCOPE_INVALID,                                   \
    .parent = PLAT_SHMEM_ARENA_HEAP,                                           \
    .grow_objects_desired = PLAT_SHMEM_DEFAULT_GROW_OBJECTS_DESIRED,           \
    .grow_byte_limit = PLAT_SHMEM_DEFAULT_GROW_BYTE_LIMIT,                     \
    .shrink_object_threshold = PLAT_SHMEM_DEFAULT_SHRINK_OBJECT_THRESHOLD,     \
    .shrink_byte_threshold = PLAT_SHMEM_DEFAULT_SHRINK_BYTE_THRESHOLD,         \
    .used_limit = PLAT_SHMEM_ARENA_USED_UNLIMITED

/* BEGIN CSTYLED */

/**
 * @brief Defining macro for allocation arenas
 *
 * item(enum_val, short_text, config_initializer)
 *
 * @param enum_val <IN> Complete enum tag for this arena
 * @param short_text <IN> Short unquoted text to use for internal
 * symbols, log messages, and command line overrides.
 * config_expression <IN> Initializer for a struct plat_shmem_arena_config
 * plat_shmem_arena_config. Unspecified/0 fields are interpreted as the
 * defaults.  The scope and parent must be specified
 *
 * Aranged in depth first order; global arenas must preceed thread
 * local.
 *
 * XXX: drew 2009-04-16 arena size limits can only be enforced on 
 * trees rooted at the heap.
 */
#define PLAT_SHMEM_ARENA_ITEMS(defaults...) \
    item(PLAT_SHMEM_ARENA_ROOT, root,                                          \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_GLOBAL,                              \
          .parent = PLAT_SHMEM_ARENA_HEAP,                                     \
          .flags = PLAT_SHMEM_ARENA_FLAG_STATS,                                \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_ROOT_THREAD, root_thread,                            \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_THREAD,                              \
          .parent = PLAT_SHMEM_ARENA_ROOT, }))                                 \
    item(PLAT_SHMEM_ARENA_CACHE, cache,                                        \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_GLOBAL,                              \
          .parent = PLAT_SHMEM_ARENA_HEAP,                                     \
          .flags = PLAT_SHMEM_ARENA_FLAG_STATS |                               \
              PLAT_SHMEM_ARENA_FLAG_OVERFLOW_EXPECTED,                         \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_CACHE_THREAD, cache_thread,                          \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_THREAD,                              \
          .parent = PLAT_SHMEM_ARENA_CACHE,                                    \
          .flags = PLAT_SHMEM_ARENA_FLAG_OVERFLOW_EXPECTED                     \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_CACHE_READ_BUF, cache_read_buf,                      \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_GLOBAL,                              \
          .parent = PLAT_SHMEM_ARENA_HEAP,                                     \
          .flags = PLAT_SHMEM_ARENA_FLAG_STATS                                 \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_CACHE_READ_BUF_THREAD, cache_read_buf_thread,        \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_THREAD,                              \
          .parent = PLAT_SHMEM_ARENA_CACHE_READ_BUF                            \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_CACHE_DIRENTRY, cache_direntry,                      \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_GLOBAL,                              \
          .parent = PLAT_SHMEM_ARENA_HEAP,                                     \
          .flags = PLAT_SHMEM_ARENA_FLAG_STATS                                 \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_CACHE_DIRENTRY_THREAD, cache_direntry_thread,        \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_THREAD,                              \
          .parent = PLAT_SHMEM_ARENA_CACHE_DIRENTRY                            \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_FTH, fth,                                            \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_GLOBAL,                              \
          .parent = PLAT_SHMEM_ARENA_HEAP,                                     \
          .flags = PLAT_SHMEM_ARENA_FLAG_STATS,                                \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_FTH_THREAD, fth_thread,                              \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_THREAD,                              \
          .parent = PLAT_SHMEM_ARENA_FTH,                                      \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_FLASH_MISC, flash_misc,                              \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_GLOBAL,                              \
          .parent = PLAT_SHMEM_ARENA_HEAP,                                     \
          .flags = PLAT_SHMEM_ARENA_FLAG_STATS,                                \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_FLASH_MISC_THREAD, flash_misc_thread,                \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_THREAD,                              \
          .parent = PLAT_SHMEM_ARENA_FLASH_MISC,                               \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_FLASH_OBJ, flash_obj,                                \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_GLOBAL,                              \
          .parent = PLAT_SHMEM_ARENA_HEAP,                                     \
          .flags = PLAT_SHMEM_ARENA_FLAG_STATS,                                \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_FLASH_OBJ_THREAD, flash_obj_thread,                  \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_THREAD,                              \
          .parent = PLAT_SHMEM_ARENA_FLASH_OBJ,                                \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_FLASH_CHUNK, flash_chunk,                            \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_GLOBAL,                              \
          .parent = PLAT_SHMEM_ARENA_HEAP,                                     \
          .flags = PLAT_SHMEM_ARENA_FLAG_STATS,                                \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_FLASH_CHUNK_THREAD, flash_chunk_thread,              \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_THREAD,                              \
          .parent = PLAT_SHMEM_ARENA_FLASH_CHUNK,                              \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_FLASH_USER, flash_user,                              \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_GLOBAL,                              \
          .parent = PLAT_SHMEM_ARENA_HEAP,                                     \
          .flags = PLAT_SHMEM_ARENA_FLAG_STATS,                                \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_FLASH_USER_THREAD, flash_user_thread,                \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_THREAD,                              \
          .parent = PLAT_SHMEM_ARENA_FLASH_USER,                               \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_FLASH_TEMP, flash_temp,                              \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_GLOBAL,                              \
          .parent = PLAT_SHMEM_ARENA_HEAP,                                     \
          .flags = PLAT_SHMEM_ARENA_FLAG_STATS,                                \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_FLASH_TEMP_THREAD, flash_temp_thread,                \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_THREAD,                              \
          .parent = PLAT_SHMEM_ARENA_FLASH_TEMP,                               \
          }))                                                                  \
    item(PLAT_SHMEM_ARENA_TEST, test,                                          \
         ((struct plat_shmem_arena_config) {                                   \
          defaults,                                                            \
          .scope = PLAT_SHMEM_ARENA_SCOPE_GLOBAL,                              \
          .parent = PLAT_SHMEM_ARENA_HEAP,                                     \
          .flags = PLAT_SHMEM_ARENA_FLAG_STATS,                                \
          }))
/* BEGIN CSTYLED */

#ifndef PLAT_SHMEM_ARENA_DEFAULT_VALUE
/** @brief Value to use for default arena */
#define PLAT_SHMEM_ARENA_DEFAULT_VALUE PLAT_SHMEM_ARENA_ROOT_THREAD
#endif

enum plat_shmem_arena_scope {
    PLAT_SHMEM_ARENA_SCOPE_INVALID = 0,
    PLAT_SHMEM_ARENA_SCOPE_GLOBAL,
    PLAT_SHMEM_ARENA_SCOPE_THREAD
};

enum plat_shmem_arena {
    /** @brief Stolen from root */
    PLAT_SHMEM_ARENA_STOLEN = -3,

    /** @brief Illegal out-of-band value  */
    PLAT_SHMEM_ARENA_INVALID = -2,

    /** @brief Parent of this arena is the heap */
    PLAT_SHMEM_ARENA_HEAP = PLAT_SHMEM_ARENA_INVALID,

    /** @brief Changed to current default in alloc contexts */
    PLAT_SHMEM_ARENA_DEFAULT = -1,

#define item(enum_val, short_text, config_initializer) enum_val,
    PLAT_SHMEM_ARENA_ITEMS()
#undef item

    /** @brief Total number of arena classes */
    PLAT_SHMEM_ARENA_COUNT
};

enum plat_shmem_arena_flags {
    /** @brief This arena has a separate stats in plat_stats */
    PLAT_SHMEM_ARENA_FLAG_STATS = 1 << 0,

    /** @brief Running into arena limits isn't an error */
    PLAT_SHMEM_ARENA_FLAG_OVERFLOW_EXPECTED = 1 << 1
};

struct plat_shmem_arena_config {
    /** @brief Scope of arena */
    enum plat_shmem_arena_scope scope;

    /** @brief Parent of this arena */
    enum plat_shmem_arena parent;

    /** @brief Bitwise or of enum plat_shmem_arena_flags */
    int flags;

    /** @brief Desired object count to grab from parent or heap */
    int grow_objects_desired;

    /**
     * @brief No more than this many bytes will be grabbed on grow
     *
     * Except where the object size is > grow_byte_limit but less than
     * 1 << max_bucket_bits.
     */
    int64_t grow_byte_limit;

    /**
     * @brief Objects are returned to the parent arena when this many are free
     *
     * Half the objects are returned rounding up.  So if the threshold is "1"
     * (because the object is big) a single object is returned instead of zero.
     */
    int shrink_object_threshold;

    /**
     * @brief Objects are returned to the parent arena at this free byte count
     *
     * Half the objects are returned rounding up on an object boundary.
     */
    int64_t shrink_byte_threshold;

    /**
     * @brief Limit on total used space for arena.
     *
     * This includes allocator overhead.  Attempts to allocate which
     * would increase total arena size beyond this cause allocations
     * to fail with ENOMEM.
     *
     * Use #PLAT_SHMEM_ARENA_USED_UNLIMITED for no limit.
     */
    int64_t used_limit;
};

/** @brief Default values for arena configuration */

enum plat_shmem_default_values {
/* BEGIN CSTYLED */
#ifndef notyet
#   ifdef PLAT_SHMEM_DEBUG
    PLAT_SHMEM_DEFAULT_GROW_OBJECTS_DESIRED = 3,
#   else
    PLAT_SHMEM_DEFAULT_GROW_OBJECTS_DESIRED = 128,
#   endif
    PLAT_SHMEM_DEFAULT_GROW_BYTE_LIMIT = 512 * 1024,

    PLAT_SHMEM_DEFAULT_SHRINK_OBJECT_THRESHOLD =
        2 * PLAT_SHMEM_DEFAULT_GROW_OBJECTS_DESIRED,
    PLAT_SHMEM_DEFAULT_SHRINK_BYTE_THRESHOLD =
        2 * PLAT_SHMEM_DEFAULT_GROW_BYTE_LIMIT,
#else
#   ifdef PLAT_SHMEM_DEBUG
    PLAT_SHMEM_DEFAULT_GROW_SHRINK_OBJECTS_DESIRED = 3,
#   else
    PLAT_SHMEM_DEFAULT_GROW_OBJECTS_DESIRED = 128,
#   endif
    PLAT_SHMEM_DEFAULT_GROW_SHRINK_BYTE_THRESHOLD = 512 * 1024,
#endif /* ndef notyet */
};

enum {
    /** @brief Unlimited out-of-band value for used_limit configuration */
    PLAT_SHMEM_ARENA_USED_UNLIMITED = INT64_MAX
};

/* END CSTYLED */

__BEGIN_DECLS

/** @brief Convert arena to string */
const char *plat_shmem_arena_to_str(enum plat_shmem_arena arena);

/**
 * @brief Convert string to arena
 *
 * @return arena on success, PLAT_SHMEM_ARENA_INVALID on failure$.
 */
enum plat_shmem_arena plat_shmem_str_to_arena(const char *string, size_t n);

__END_DECLS

#endif /* ndef PLATFORM_SHMEM_ARENA_H */
