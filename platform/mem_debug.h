#ifndef PLATFORM_MEM_DEBUG_H
#define PLATFORM_MEM_DEBUG_H 1

/*
 * File:   sdf/platform/mem_debug.h
 * Author: drew
 *
 * Created on March 12, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mem_debug.h 1046 2008-04-23 23:34:19Z drew $
 */

/**
 * mem_debug provides infrastructure to implement reference tracking
 * (with potentially dangling references logged at detach time) and
 * memory protection.
 *
 * Since the primary intent is to debug shared memory applications where
 * mapping (in different processes) and allocation are orthogonal, a separate
 * memory debugger is provided instead of the conventional debugging allocator
 * approach.
 */

#include "platform/defs.h"
#include <sys/cdefs.h>

#include "platform/logging.h"

struct plat_mem_debug;

/**
 * @brief Memory debugger configuration
 *
 * This should be initialized with a call to #plat_mem_debug_config_init()
 * prior to setting user-specified fields so that the user is picking up
 * appropriate defaults for the current code version
 */

enum plat_mem_subobject {
    PLAT_MEM_SUBOBJECT_ALLOW,
    PLAT_MEM_SUBOBJECT_DENY
};

struct plat_mem_debug_config {
    /** @brief Backtrace depth to snapshot on reference creation. */
    int backtrace_depth;

    /** @brief Allow or deny sub-objects */
    enum plat_mem_subobject subobject;

    /** @brief Log category to use for error messages */
    int log_category;
};

struct plat_mem_debug_stats {
    /** Number of distinct object regions */
    int object_count;

    /** Total number of references */
    int reference_count;
};

__BEGIN_DECLS

/**
 * @brief Allocate memory debugger
 *
 * XXX The initial implementation does not allow sub-object mappings
 *
 * @param config  <IN>Configuration which remains owned by the caller,
 * should have been initilialized with #plat_mem_debug_config_init().
 *
 * @return memory debugger on success, NULL on failure
 */
struct plat_mem_debug *plat_mem_debug_alloc(const struct plat_mem_debug_config
                                            *config);

/**
 * @brief Free memory debugger.
 *
 * On free, all memory is made unprotected.
 */
void plat_mem_debug_free(struct plat_mem_debug *debug);

/**
 * @brief Add memory pool to debugger
 *
 * Since the debugger cannot inexpensively and easily protect memory on
 * non-page boundaries and must act pessimistically on protect + optomistically
 * on unprotect, start and start + len should be aligned on page boundaries.
 *
 * To detect overruns onto the first object or past the last object the most
 * pesimistic user would probide their own red-zones before and after.
 *
 *
 * @param debug memory <IN> debugger
 * @param start starting <IN> memory address.
 * @parma len length <IN>.
 * @return 0 on success, -errno on failure.
 */
int plat_mem_debug_add_unused(struct plat_mem_debug *debug, void *start,
                              size_t len);

/**
 * @brief Add memory reference to the given object.
 *
 * @param debug  <IN>Memory debugger
 * @param reference  <IN>Address of the reference.
 * @param start  <IN>Value of the reference
 * @param len  <IN>Length of the reference
 * @param ignore_frame_count  <IN>Number of stack frames to skip not
 * including this call.
 *
 * @return 0 on success, -errno on failure.
 */
int plat_mem_debug_reference(struct plat_mem_debug *debug, void **reference,
                             void *start, size_t len, int writeable,
                             int ignore_frame_count);

/**
 * @brief Release a memory reference to the given object.
 *
 * @param reference  <IN>Address of the reference.  If the user is careful
 * to acquire a reference each time a different local pointer is used error
 * reports at shut-down involving leaked references will be more meaningful.
 *
 * @param start  <IN>Value of the reference
 * @param len  <IN>Length of the reference
 * @param writeable  <IN>Boolean, whether the reference is writeable
 * @param ignore_frame_count  <IN>Number of stack frames to skip not
 * including this call.
 *
 * @return 0 on success, -errno on failure.  Possible failures include
 * the reference not matching one granted.
 */

int plat_mem_debug_release(struct plat_mem_debug *debug,
                           void **reference, void *start, size_t len,
                           int writeable);

/**
 * @brief Get statistics
 *
 * @param debug <IN> the memory debugger
 * @param stats <OUT> stats
 */
void plat_mem_debug_get_stats(struct plat_mem_debug *debug,
                              struct plat_mem_debug_stats *stats);

/**
 * @brief Log references.
 *
 * @param debug <IN> the memory debugger
 * @param logging <IN> category
 * @param logging <IN> level
 */
void plat_mem_debug_log_references(struct plat_mem_debug *debug,
                                   int category, enum plat_log_level level,
                                   int backtrace_limit);


/**
 * @brief Initialize memory debugger configuration
 */
void plat_mem_debug_config_init(struct plat_mem_debug_config *config);

/**
 * @brief Check whether or not the range is entirely referenced.
 *
 * @param debug  <IN>Memory debugger
 * @param start  <IN>Value of the reference
 * @param len  <IN>Length of the reference
 *
 * @return 1 on success, 0 on failure.
 */
int plat_mem_debug_check_referenced(struct plat_mem_debug *debug,
                                    void *start, size_t len);

/**
 * @brief Check whether or not the range is entirely free.
 *
 * @param debug  <IN>Memory debugger
 * @param start  <IN>Value of the reference
 * @param len  <IN>Length of the reference
 *
 * @return 1 on success, false 0 failure.
 */
int plat_mem_debug_check_free(struct plat_mem_debug *debug,
                              void *start, size_t len);

__END_DECLS

#endif /* def PLATFORM_MEM_DEBUG */
