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

#ifndef PLATFORM_SHMEM_C_H
#define PLATFORM_SHMEM_C_H 1

/*
 * File:   sdf/platform/shmem_to_ptr_c.h
 * Author: drew
 *
 * Created on July 18, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: shmem_c.h 12973 2010-04-17 02:58:50Z drew $
 */

/**
 * Implement public shmem to local pointer/physical conversion functions such
 * that we can get debuggable out-of-line version which allows for conditional
 * breakpoints to be set on specific shared memory pointers being derferenced
 * and an optimized version which expands inline with enough hints to
 * allow for reasonable global common subexpression elimination.
 *
 * This header is not for user consumption - other pieces of shmem.h
 * may need the inline expansions.
 *
 * It's split into two parts, the first half which gets processed once
 * the necessary extern, etc. declarations and a second which can be expanded
 * to produce both in-line and out-of-line versions.
 */

#include "platform/assert.h"
#ifdef PLAT_SIMULATION
#include "platform/attr.h"
#endif

#include "platform/defs.h"
#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/stdlib.h"
#include "platform/types.h"

/** @brief Shared memory segment type.  */
enum shmem_type {
    /**
     * @brief Segment part of NULL shmem pointer
     *
     * The first attached shared memory segment is just there so that 0 can
     * be used for NULL (required for an uninitialized shared pointer in BSS
     * to have correct semantics) and we don't have to special case a
     * 1-based array.
     */
    SHMEM_TYPE_NULL = 0,

    /**
     * @brief Memory mapped file
     *
     * mmap segments should not be used on disk-based file system files outside
     * debugging environments since the kernel will lazily write to backing
     * store, but mmap can allocate store from memory mapped file systems
     * (like tmpfs) without this problem.
     */
    SHMEM_TYPE_MMAP_FILE,

    /** @brief Memory map device other than Schooner physmem.  */
    SHMEM_TYPE_MMAP_DEV,

    /** @brief Memory map Schooner physmem device */
    SHMEM_TYPE_MMAP_DEV_PHYSMEM,

    /**
     * @brief Memory mapped Schooner physmem device, only for virtual memory
     * access.
     *
     * XXX: drew 2008-09-01 this should go away.  This is just a place holder
     * for the physmem allocation experiment where we want to get our own
     * memory and allocator but don't want to live with the contiguous
     * limits.
     */
    SHMEM_TYPE_MMAP_DEV_PHYSMEM_VIRT,

    /**
     * @brief Attach SYSV segment
     *
     * SYSV shared memory segments are the easiest way to avoid the backing
     * store paging problem and can be locked without locking every process
     * mapping
     *
     * FIXME: do minor page faults still happen on locked pages?
     */
    SHMEM_TYPE_SYSV
};

/**
 * @brief Descriptors, one per attached segment
 *
 * Currently we map all segments separately and have discontinuous
 * physical addresses imply separate virtual addresses.  If we can't
 * get one big memory pool from the kernel we'll want to separate the
 * two so that the normal operational case remains a single addition
 * for the translation between shared and local memory.
 *
 * Mechanically, this would mean mmap() on a chunk of /dev/zero
 * with segments wired at specific locations (we could still use
 * red-zones between segments to detect running off the end of
 * physical pages.
 */
struct plat_shmem_attached_segment {
    enum shmem_type type;

    /*
     * The current implementation completely maps each segment.  Debugging
     * hooks optionally mprotect pages within segments that should be
     * unreferenced and/or interface with valgrind.
     */

    /** @brief Local mapping for physical address translations */
    void *phys_ptr;

    /** @brief Local mapping for virtual address translations */
    void *virt_ptr;

    /**
     * @brief Length of the header.
     *
     * XXX: Generally we want the standard schooner header (it will make
     * post mortem analysis simpler) but it will hurt efficiency with
     * really small memory chunks.
     */
    size_t header_len;

    /** @brief Total segment length (including header) */
    size_t len;

    /**
     * @brief Physical address of offset 0
     *
     * Within a segment, addresses are physically contiguous.  0 is used
     * for no known physical mapping.
     */
    size_t paddr;
};

/**
 * The set of all attached segments. This may be maintained as a thread-local
 * structure so that it can be accessed without locking if we ever revisit
 * growth after attachment.
 *
 * Size overhead is negligible because the thread-local copy of the
 * descriptors only includes the mapping information needed to access
 * (address) and unmap (length, SYSV ipc id) it.
 *
 * plat_shmem_attached is externally visible as a typedef
 */
struct plat_shmem_attached {
/* XXX: The initial implementation initializes once only */
#ifdef notyet
    /* FIXME: read-write lock */
    plat_mutex_t lock;
#endif

    /**
     * @brief Configuration flags
     *
     * XXX: Currently these are set at initialzation time
     */
    int config_flags;

    /** @brief Address space length */
    uint64_t address_space;

    /** @brief Super-map, encompassing physical and virtual pieces */
    void *map;

    /** @brief Length of map */
    uint64_t map_len;

    /** @brief Sparse mapping for physical allocations */
    void *phys_map;

    /** @brief After end of highest mapped physical memory */
    void *phys_end;

    /** @brief Non-sparse mapping for virtual allocations */
    void *virt_map;

    /** @brief Virtual address space length (mapped contiguosly)  */
    uint64_t virt_address_space;

    /** @brief Current end of virtual memory */
    void *virt_end;

    /** @brief Size of *segments */
    int nsegments;

    /**
     * @brief Array of segments indexed by #plat_shmem_ptr_base segment
     *
     * XXX: Segments are indexed by initialization order, with
     * logical addresses at whatever the current (2008-07-29) code
     * thinks is appropriate.  We could change that to get things
     * attached in virtual address order to binary search for
     * local->physical pointer conversions but it's better just
     * use the shmem pointer that already contains that information
     * as-is.
     */
    struct plat_shmem_attached_segment *segments;

    /** @brief Shem pointer to first segment */
    plat_shmem_ptr_base_t first_segment;

    /*
     * Add refresh, grow closures; where refresh is a NOP within
     * the shmemd process itself. grow is local or IPC.
     */
};

extern const struct plat_shmem_attached  *plat_shmem_attached;

#if defined(PLATFORM_SHMEM_C) && defined(PLAT_NEED_OUT_OF_LINE)
#define PLAT_SHMEM_C_INLINE
#else
#define PLAT_SHMEM_C_INLINE PLAT_INLINE
#endif

__BEGIN_DECLS

/*
 * Once something is calling plat_shmem_get_attached, the shmem system
 * has been initialized so the return is constant and therefore subject
 * to optimization.
 */
PLAT_SHMEM_C_INLINE PLAT_SHMEM_CONST const struct plat_shmem_attached *
plat_shmem_get_attached() {
    /*
     * The micro-optimization breaks the unit tests which attach and detach
     * If this becomes important in the interposer case, implement a cache
     * directory scheme so we can use the local value.
     */
#if 1 || defined(PLAT_SHMEM_DEBUG)
    return (plat_shmem_attached);
#elif defined(PLAT_SIMULATION)
    return (plat_attr_shmem_attached_get());
#else
    /*
     * Grab a local copy to eliminate the global offset table indirect with
     * -fpic.
     */
    static const struct plat_shmem_attached *attached = NULL;

    if (!attached) {
        attached = plat_shmem_attached;
    }

    plat_shmem_debug_assert(attached);

    return (attached);
#endif
}

/*
 * Return pointer to start of Nth segment; preferably physical because
 * we'll use the pointer for allocations.
 */
PLAT_SHMEM_C_INLINE PLAT_SHMEM_CONST plat_shmem_ptr_base_t
plat_shmem_get_segment(int n) {
    struct plat_shmem_ptr_base ret;
    const struct plat_shmem_attached *attached;

    attached = plat_shmem_get_attached();
    plat_shmem_debug_assert(attached);
    plat_shmem_debug_assert(n < attached->nsegments);

    ret.ptr = attached->segments[n].phys_ptr ? : attached->segments[n].virt_ptr;

    return (ret);
}

/** @brief Return shmem ptr to first segment */
PLAT_SHMEM_C_INLINE PLAT_SHMEM_CONST plat_shmem_ptr_base_t
plat_shmem_first_segment() {
    return (plat_shmem_get_segment(1));
}

PLAT_SHMEM_C_INLINE PLAT_SHMEM_CONST void *
plat_shmem_ptr_base_to_ptr(plat_shmem_ptr_base_t shmem_ptr_base) {
    return (shmem_ptr_base.ptr);
}

/*
 * Nofail version of the above.  Can be called with a deallocated shmem pointer
 * (or other errors) and it just returns NULL.  Used by the XList
 * compare-and-swap routines.  This covers a race condition where the
 * storage associated with a shmem pointer is released kist before it is
 * dereferenced and then used in a CS operation.  In this case, the CS
 * operation will fail so we don't care what the pointer reference is - as
 * long as it doesn't cause an exception or some other failure.
 */
PLAT_SHMEM_C_INLINE PLAT_SHMEM_CONST void *
plat_shmem_ptr_base_to_ptr_nofail(plat_shmem_ptr_base_t shmem_ptr_base) {
    return (shmem_ptr_base.ptr);
}

/**
 * @brief Add offset to pointer
 *
 * No checking is perfored for overruns.
 */
PLAT_SHMEM_C_INLINE PLAT_SHMEM_CONST plat_shmem_ptr_base_t
plat_shmem_ptr_base_add(plat_shmem_ptr_base_t ptr, size_t offset) {
    plat_shmem_ptr_base_t ret;

    ret.ptr = (void *)((char *)ptr.ptr + offset);

    return (ret);
}

PLAT_SHMEM_C_INLINE PLAT_SHMEM_CONST plat_shmem_ptr_base_t
plat_shmem_ptr_base_sub(plat_shmem_ptr_base_t ptr, size_t offset) {
    plat_shmem_ptr_base_t ret;

    ret.ptr = (void *)((char *)ptr.ptr - offset);

    return (ret);
}

/**
 * @brief Calculate difference between two pointers in same segment
 */
PLAT_SHMEM_C_INLINE PLAT_SHMEM_CONST ssize_t
plat_shmem_ptr_base_diff(plat_shmem_ptr_base_t lhs, plat_shmem_ptr_base_t rhs) {
    ssize_t ret;

    ret = ((char *)lhs.ptr) - ((char *)rhs.ptr);

    return (ret);
}

/*
 * XXX: The fastest (but not necessarily safest) way to do this is to sort
 * the physical addresses, assign virtual addresses in order, and have the
 * the two address ranges share the same sparseness so shmem to virtual or
 * local operations are a pointer addition.  Where attach addresses are
 * constant across processes the non-debug shmempointer can be a flat pointer
 * making the physical conversion a register subtraction.  Hooks exist to do
 * this in user space and make sure the segments are all mapped in the right
 * order.
 *
 * The approach fails for 32 bit applications where physmem is larger than
 * the per process address space but we're 64 bit only.
 */
PLAT_SHMEM_C_INLINE PLAT_SHMEM_CONST plat_shmem_paddr_t
plat_shmem_local_to_paddr(const void *ptr) {

#ifndef SHMEM_DEBUG
    plat_shmem_paddr_t ptr_val = (plat_shmem_paddr_t)ptr;
    plat_shmem_paddr_t map_start;
    plat_shmem_paddr_t map_end;

#ifdef PLAT_SHMEM_PHYS_MAP
#ifdef PLAT_SHMEM_ADDRESS_SPACE
    map_start = (plat_shmem_paddr_t)PLAT_SHMEM_PHYS_MAP;
    map_end = (plat_shmem_paddr_t)PLAT_SHMEM_PHYS_MAP +
        PLAT_SHMEM_PHYS_ADDRESS_SPACE;

#else /* def PLAT_SHMEM_PHYS_ADDRESS_SPACE */
#error "PLAT_SHMEM_PHYS_MAP compile time define implies PLAT_SHMEM_ADDRESS_SPACE"
#endif /* else def PLAT_SHMEM_PHYS_ADDRESS_SPACE */
#else /* def PLAT_SHMEM_PHYS_MAP */
    const struct plat_shmem_attached *attached = plat_shmem_get_attached();

    map_start = (plat_shmem_paddr_t)attached->phys_map;
    map_end = map_start + attached->address_space;

#endif /* else def PLAT_SHMEM_PHYS_MAP */

    return (ptr_val >= map_start && ptr_val <= map_end ?  ptr_val - map_start :
            0);
#else /* ndef SHMEM_DEBUG */
    const struct plat_shmem_attached *attached = plat_shmem_get_attached();
    const struct plat_shmem_attached_segment *segments_end;
    const struct plat_shmem_attached_segment *segment;

    for (segments_end = attached->segments + attached->nsegments,
         /* Skip NULL */
         segment = attached->segments + 1; segment <= segments_end; ++segment) {
        if (segment->ptr <= ptr &&
            ptr <= (void *)((char *)segment->ptr + segment->len)) {
            if (!segment->paddr) {
                return (0);
            } else {
                return (((char *)ptr - (char *)segment->ptr) + segment->paddr);
            }
        }
    }

    return (0);
#endif /* else ndef SHMEM_DEBUG */
}

PLAT_SHMEM_C_INLINE PLAT_SHMEM_CONST plat_shmem_paddr_t
plat_shmem_ptr_base_to_paddr(plat_shmem_ptr_base_t shmem_ptr_base) {
    return (plat_shmem_local_to_paddr(shmem_ptr_base.ptr));
}

/** @brief Return non-zero if this is a shared pointer, zero if it's local */
PLAT_SHMEM_C_INLINE PLAT_SHMEM_CONST int
plat_shmem_ptr_is_shared(plat_shmem_ptr_base_t shmem_ptr_base) {
    const struct plat_shmem_attached *attached = plat_shmem_get_attached();

    return (attached ? attached->map <= shmem_ptr_base.ptr &&
            shmem_ptr_base.ptr <
            (void *)(((char *)attached->map) + attached->map_len) : 0);
}

PLAT_SHMEM_C_INLINE PLAT_SHMEM_CONST plat_shmem_ptr_base_t
plat_shmem_local_to_shared(void *ptr) {
    const struct plat_shmem_attached *attached = plat_shmem_get_attached();
    plat_shmem_ptr_base_t ret;

    if (attached && attached->map <= ptr &&
        ptr < (void *)(((char *)attached->map) + attached->map_len)) {
        ret.ptr = ptr;
    } else {
        ret = plat_shmem_ptr_base_null;
    }

    return (ret);
}


#ifdef PLAT_SHMEM_FAKE
PLAT_SHMEM_C_INLINE plat_shmem_ptr_base_t
plat_shmem_alloc_helper(const char *file, unsigned line, const char *fn,
                        int log_cat, size_t size, enum plat_shmem_arena arena,
                        int flags) {
    plat_shmem_ptr_base_t ret;

    ret.ptr = sys_malloc(size);

    plat_log_msg_forward(file, line, fn, PLAT_LOG_ID_INITIAL, log_cat,
                         PLAT_LOG_LEVEL_TRACE,
                         "plat_shmem_alloc(%lu) fake = %p",
                         (unsigned long)size, ret.ptr);

    return (ret);
}

PLAT_SHMEM_C_INLINE void
plat_shmem_free_helper(const char *file, unsigned line, const char *fn,
                       int log_cat, plat_shmem_ptr_base_t ptr, size_t size,
                       int free_count) {

    plat_log_msg_forward(file, line, fn, PLAT_LOG_ID_INITIAL, log_cat,
                         PLAT_LOG_LEVEL_TRACE, "plat_shmem_free(%p) fake ",
                         ptr.ptr);

    sys_free(ptr.ptr);
}
#endif /* def PLAT_SHMEM_FAKE */

__END_DECLS

#endif /* def PLATFORM_SHMEM_C_H 1 */
