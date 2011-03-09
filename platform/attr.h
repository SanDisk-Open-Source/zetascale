#ifndef PLATFORM_ATTR_H

/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/attr.h $
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: attr.h 3440 2008-09-17 10:11:17Z drew $
 */

/**
 * Define attributes at process, kernel scheduled thread, and user scheduled
 * thread scopes with transparent scope for user.
 *
 * These are used to provide operational state for things which can't
 * take extra parameters (shared memory pointers) or where values must
 * be scope local (action-consistent recovery structures unique
 * to each kernel thread).
 *
 * pthread_getspecific()/setspecific() are the analog.
 */

#include "platform/defs.h"


/* Extern struct defintions */
struct plat_shmem;
struct plat_shmem_attached;
struct shmem_alloc;
struct sdf_replicator_adapter_thread_state;
struct plat_attr_uthread_specific;

/**
 * Define process-level attributes
 *
 * item(description, level, name, type, attrs)
 */
#define PLAT_ATTR_PROC_ITEMS()                                                 \
    item("Client shared memory struct", proc, shmem, struct plat_shmem,        \
         /* none */)                                                           \
    item("Common shared memory struct", proc, shmem_attached,                  \
         const struct plat_shmem_attached, __attribute__((pure)))              \
    item("Shared memory allocator state", proc, shmem_alloc,                   \
         struct shmem_alloc, __attribute__((pure)))
        
/**
 * Define kernel-secheduled thread level attributes
 *
 * item(description, level, name, type, attrs)
 */
#define PLAT_ATTR_KTHREAD_ITEMS()                                              \
    item("Closure scheduler", kthread, closure_scheduler,                      \
         struct plat_closure_scheduler, /* none */)

/**
 * Define user-thread-scheduled thread level attributes
 *
 * item(description, level, name, type, attrs)
 */

#define PLAT_ATTR_UTHREAD_ITEMS()                                              \
    item("Replication adapter thread state", uthread,                          \
         sdf_replicator_adapter_thread_state,                                  \
         struct sdf_replicator_adapter_thread_state, /* none */)

#define PLAT_ATTR_ITEMS()                                                      \
    PLAT_ATTR_PROC_ITEMS()                                                     \
    PLAT_ATTR_KTHREAD_ITEMS()                                                  \
    PLAT_ATTR_UTHREAD_ITEMS()

__BEGIN_DECLS

/*
 * Declare getter and setters.
 */
#define item(description, level, name, type, attrs)                            \
    type * plat_attr_ ## name ## _get() attrs;                                 \
    type * plat_attr_ ## name ## _set(type *next);                             \
    type * plat_attr_ ## name ## _set_if_equals(type *next, type *prev);
PLAT_ATTR_ITEMS()
#undef item


/**
 * @brief Set getter function for user threads package.
 *
 * At most one user thread package may be used.  The user thread code
 * shall call #plat_attr_uthread_alloc to allocate the per-thread
 * structure on thread creation and #plat_attr_uthread_free on thread
 * termination.
 *
 * For simplicity, this is idempotent.
 */
void plat_attr_uthread_getter_set(struct plat_attr_uthread_specific
                                  *(*getter)());

/** @brief Allocate uthread structure */
struct plat_attr_uthread_specific *plat_attr_uthread_alloc();

/** @brief Free uthread structure */
void plat_attr_uthread_free(struct plat_attr_uthread_specific *);

__END_DECLS

#endif /* ndef PLATFORM_ATTR_H */
