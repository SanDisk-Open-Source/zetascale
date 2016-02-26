/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: locks.h
 * Author: Johann George
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * Write and read/write spinlocks.
 */

#ifndef LOCKS_H
#define LOCKS_H

#include <stdint.h>


/*
 * Type definitions.
 */
typedef uint64_t atom_t;


/*
 * A write lock.
 */
typedef struct {
    int64_t lock;
} wlock_t;


/*
 * A read write lock.
 *
 *  nr - If a read lock is being held, this contains the number of readers
 *       using the lock.  If a write lock is being held, the high bit will be
 *       set.
 *  nw - Number of writers waiting to acquire the lock.  This is advisory and
 *       helps keep away readers when a writer is attempting to acquire the
 *       lock.
 */
typedef struct {
    int64_t nr;
    int64_t nw;
} rwlock_t;


/* 
 * Function prototypes.
 */
int       atom_u64_max(uint64_t *val, uint64_t new);
int       atom_u64_min(uint64_t *val, uint64_t new);
void      wl_free(wlock_t *l);
void      rwl_free(rwlock_t *l);
wlock_t  *wl_init(void);
rwlock_t *rwl_init(void);


/*
 * Generally useful macros.
 */
#define likely(x)                   __builtin_expect((x),1)
#define unlikely(x)                 __builtin_expect((x),0)
#define nop()                       __asm__ __volatile__("rep;nop":::"memory")
#define barrier()                   __asm__ __volatile__("sfence":::"memory")
#define atomic_inc(v)               (void) __sync_add_and_fetch(&v, 1)
#define atomic_inc_get(v)                  __sync_add_and_fetch(&v, 1)
#define atomic_get_inc(v)                  __sync_fetch_and_add(&v, 1)
#define atomic_dec(v)               (void) __sync_sub_and_fetch(&v, 1)
#define atomic_dec_get(v)                  __sync_sub_and_fetch(&v, 1)
#define atomic_get_dec(v)                  __sync_fetch_and_sub(&v, 1)
#define atomic_add(v, a)            (void) __sync_add_and_fetch(&v, a)
#define atomic_add_get(v, a)               __sync_add_and_fetch(&v, a)
#define atomic_get_add(v, a)               __sync_fetch_and_add(&v, a)
#define atomic_sub(v, a)            (void) __sync_sub_and_fetch(&v, a)
#define atomic_and(v, a)            (void) __sync_and_and_fetch(&v, a)
#define atomic_or(v, a)             (void) __sync_or_and_fetch(&v, a)
#define atomic_get_or(v, a)                __sync_fetch_and_or(&v, a)
#define atomic_cmp_swap(v, a, b)           __sync_val_compare_and_swap(&v, a, b)
#define atomic_cmp_swap_bool(v, a, b)           __sync_bool_compare_and_swap(&v, a, b)


/*
 * Macros specific to locks.
 */
#define LOCK_HI                     (1LL<<63)
#define lock_wait(v)                while (unlikely(v)) nop()


/* 
 * Lock a write lock.
 */
#define wl_lock(l)                                                      \
    do {                                                                \
        lock_wait((l)->lock);                                           \
    } while (unlikely(atomic_cmp_swap((l)->lock, 0, 1)))


/* 
 * Try a write lock.  Return 1 if successful and 0 if not.
 */
#define wl_try(l) (                                                     \
    !atomic_cmp_swap((l)->lock, 0, 1)                                   \
)


/* 
 * Unlock a write lock.
 */
#define wl_unlock(l)                                                    \
    do {                                                                \
        barrier();                                                      \
        (l)->lock = 0;                                                  \
    } while (0)


/* 
 * Get a read/write lock in read mode.  We can be starved by writers.
 */
#define rwl_lockr(l)                                                    \
    for (;;) {                                                          \
        lock_wait((l)->nw);                                             \
        if (likely(atomic_inc_get((l)->nr) > 0))                        \
            break;                                                      \
        atomic_dec((l)->nr);                                            \
    }


/* 
 * Get a read/write lock in read mode quickly.  This will starve any writers.
 */
#define rwl_lockrq(l)                                                   \
    do {                                                                \
        atomic_inc((l)->nr);                                            \
        while (unlikely((l)->nr < 0))                                   \
            nop();                                                      \
    } while (0)


/* 
 * Unlock a read/write lock that was acquired in read mode.
 */
#define rwl_unlockr(l)                                                  \
    do {                                                                \
        barrier();                                                      \
        atomic_dec((l)->nr);                                            \
    } while (0)


/* 
 * Unlock a read/write lock that was acquired in read mode without a barrier.
 */
#define rwl_unlockrb(l)                                                 \
        atomic_dec((l)->nr)


/* 
 * Get a read/write lock in write mode.
 */
#define rwl_lockw(l)                                                    \
    do {                                                                \
        rwl_reqw(l);                                                    \
        do {                                                            \
            lock_wait((l)->nr);                                         \
        } while (unlikely(atomic_cmp_swap((l)->nr, 0, LOCK_HI)));       \
    } while (0)


/* 
 * Unlock a read/write lock that was acquired in read mode.
 */
#define rwl_unlockw(l)                                                  \
    do {                                                                \
        barrier();                                                      \
        atomic_dec((l)->nw);                                            \
        atomic_and((l)->nr, ~LOCK_HI);                                  \
    } while (0)


/*
 * Attempt to acquire a read/write lock in read mode.  Return 1 if successful
 * and 0 if not.
 */
#define rwl_tryr(l) (                                                   \
    (l)->nw                                                             \
        ? 0                                                             \
        : (atomic_inc_get((l)->nr) > 0)                                 \
            ? 1                                                         \
            : (atomic_dec((l)->nr), 0)                                  \
)


/*
 * Attempt to acquire a read/write lock in write mode. Return 1 if successful
 * and 0 if not.
 */
#define rwl_tryw(l) (                                                   \
    unlikely(atomic_cmp_swap((l)->nr, 0, LOCK_HI))                      \
        ? 0                                                             \
        : (rwl_reqw(l), 1)                                              \
)


/*
 * Request that we would like to acquire this lock for write.
 */
#define rwl_reqw(l)                                                     \
    atomic_inc((l)->nw)


/*
 * Attempt to acquire a read/write lock in write mode after we have requested
 * the lock. Return 1 if successful and 0 if not.
 */
#define rwl_reqtryw(l)                                                  \
    likely(!atomic_cmp_swap((l)->nr, 0, LOCK_HI))


/*
 * Request that we would like to acquire this lock for write.
 */
#define rwl_isreqw(l)                                                   \
    (l)->nw

#endif /* LOCKS_H */
