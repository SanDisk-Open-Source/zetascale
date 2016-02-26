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
 * File:   sdf/protocol/replicator_key_lock.c
 *
 * Author: drew
 *
 * Forked from sdf/protocol/replication/copy_replicator.c on February 16, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: key_lock.c 15015 2010-11-10 23:09:06Z briano $
 */

/**
 * Replicator locking mechanism used to close the delete+recovery
 * race condition in both simple replication and full featured
 * replication.
 */
#include "sys/queue.h"

#include "platform/assert.h"
#include "platform/closure.h"
#include "platform/logging.h"

#include "utils/hashmap.h"

#include "key_lock.h"

#include "fth/fth.h"
#include "fth/fthMbox.h"

/** @brief Locking */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_LOCKING, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "locking");

struct replicator_key_lock;
struct replicator_key_lock_wait;

enum {
    /**
     * @brief Number of hash buckets
     *
     * XXX: drew 2010-04-28 This is just a guess - we recover 32M worth
     * of objects at once and were noticing severe slow downs where 
     * we had 101 hash buckets and up to 600 objects at once.  It
     * should probably be dynamic since a smaller value is more
     * pleasant for debugging.
     *
     * 65537 is prime
     */
    CR_SHARD_KEY_LOCK_HASH_BUCKETS = 65537,
};

enum {
    /** @brief Out-of-band value for ltime entries */
    CR_SHARD_KEY_LOCK_LTIME_INVALID = -1
};

typedef int64_t rklc_ltime_t;

struct replicator_key_lock_container {
    /* Info for log messages */

    vnode_t my_node;

    /** @brief sguid */
    SDF_shardid_t sguid;

    /**
     * @brief Intra node vip group ID associated with this #cr_shard
     *
     * #VIP_GROUP_ID_INVALID for no gid.
     */
    int vip_group_id;

    /**
     * @brief replication_type
     *
     * The replication type controlls errors and assertions surrounding
     * legal states.
     */
    SDF_replication_t replication_type;

    /** @brief Lock */
    fthLock_t lock;

    /**
     * @brief Hash of key to replicator_key_lock structures
     *
     * FIXME: drew 2009-06-18 Calculate the syndrome on the client
     * side and use it for all our operations so that we don't run into
     * problems on caching containers where we only allow one object of the
     * same syndrome to exist.  This may be noticeable for performance with
     * large keys.
     *
     * FIXME: drew 2009-06-18 Use the same locking mechanism on read-only
     * replicas so we don't return values that are not yet persisted accross
     * all replicas.
     */
    HashMap hash;

    /**
     * @brief Current ltime
     *
     * Ltimes are assigned at lock grant and release time so that total
     * ordering can be determined in order to solve the delete (or put
     * for incremental recovery)/recovery operation race.
     */
    rklc_ltime_t current_ltime;

    /** @brief List of all key locks to make debugging easier */
    TAILQ_HEAD(, replicator_key_lock) all_list;

    /** @brief Count of all key lock to verify correct garbage collection */
    int all_list_count;

    /** @brief List of reserved keys in ltime order */
    TAILQ_HEAD(, replicator_key_lock) modify_list;

    /** @brief List of running modify operations in ltime order */
    TAILQ_HEAD(, replicator_key_lock) running_modify_list;

    /** @brief Number of entries on running modify list */
    int running_modify_list_count;

    /**
     * @brief Total reference count
     *
     * One reference count is held on the container until the user calls
     * #rklc_free, one reference count is held for
     * each #replicator_key_lock call which was successful and has not had
     * #rkl_unlock called on the returned object, and one reference
     * count is held for each #rklc_start_get return for which
     * #rklc_get_complete has not been called.
     */
    int ref_count;

    /** @brief Number of recoveries pending */
    int recovery_count;

    /** @brief List of pending gets in ltime order */
    TAILQ_HEAD(, rklc_get) get_list;

    /** @brief Number of entries on get list */
    int get_count;

};

/** @brief Structure for each pending recovery get */
struct rklc_get {
    /** @brief Parent container */
    struct replicator_key_lock_container *container;

    /** @brief Starting ltime */
    rklc_ltime_t start_ltime;

    /** @brief Oldest modify lock existing at time of get start */
    struct replicator_key_lock *oldest_overlapping_modify;

    /** @brief Entry in container->get_list */
    TAILQ_ENTRY(rklc_get) get_list_entry;
};

/** @brief Lock on a key within the shard */
struct replicator_key_lock {
    /** @brief Parent container */
    struct replicator_key_lock_container *container;

    /**
     * @brief Key
     *
     * FIXME: drew 2009-06-18 Replace with syndrome to work with cache mode
     * that treats all keys with the same syndrome as equivalent.
     *
     * XXX: drew 2010-04-21 Replace SDF_simple_key_t with separate
     * key and length to get rid of temporaries used outside of
     * sdf/protocol.
     */
    SDF_simple_key_t key;
#ifndef notyet
    uint64_t syndrome;
#endif

    /** @brief Current lock mode */
    enum rkl_mode lock_mode;

    /** @brief Number of lock holders */
    int lock_count;

    /**
     * @brief Number of overlapping get operations
     *
     * Lock table entries are retained until all simultaneous get operations
     * have completed.  One reserve count is included for modification
     * (RKL_MODE_EXCLUSIVE or RKLC_LOCK_MODE_RECOVERY)
     */
    int reserve_count;

    /*
     * A modifying lock is mode RKL_MODE_EXCLUSIVE or
     * RKL_MODE_RECOVERY.
     */
    /** @brief ltime of current modification */
    rklc_ltime_t current_modifying_grant_ltime;

    /** @brief Ltime of first modifying grant */
    rklc_ltime_t first_modifying_grant_ltime;

    /** @brief Ltime of last modifying release */
    rklc_ltime_t last_modifying_release_ltime;

    /** @brief Waiters */
    TAILQ_HEAD(, replicator_key_lock_wait) wait_list;

    /** @brief Entry in container->all_list */
    TAILQ_ENTRY(replicator_key_lock) all_list_entry;

    /** @brief Entry in container->modify_list */
    TAILQ_ENTRY(replicator_key_lock) modify_list_entry;

    /** @brief Entry in container->running_modify_list */
    TAILQ_ENTRY(replicator_key_lock) running_modify_list_entry;
};

/** @brief Wait list entry for #replicator_key_lock */
struct replicator_key_lock_wait {
    /** @brief Lock mode */
    enum rkl_mode lock_mode;

    /** @brief Applied when lock is granted. */
    rkl_cb_t granted_cb;

    /** @brief Entry on replicator_key_lock */
    TAILQ_ENTRY(replicator_key_lock_wait) wait_list_entry;
};

static void rklc_ref_count_dec(struct replicator_key_lock_container *container);
static void rkl_granted_locked(struct replicator_key_lock *lock);
static rklc_ltime_t rklc_get_ltime(struct replicator_key_lock_container *container);
static void rklc_recovery_cb(plat_closure_scheduler_t *context, void *env);
static void rkl_cb(struct plat_closure_scheduler *context, void *env,
                   SDF_status_t status_arg,
                   struct replicator_key_lock *key_lock);
static void rkl_reserve_dec_locked(struct replicator_key_lock *key_lock);
static void rkl_check_free_locked(struct replicator_key_lock *key_lock);

struct replicator_key_lock_container *
replicator_key_lock_container_alloc(vnode_t my_node, SDF_shardid_t sguid,
                                    int vip_group_id,
                                    SDF_replication_t replication_type) {
    struct replicator_key_lock_container *ret;
    int failed;

    failed = !plat_calloc_struct(&ret);

    if (!failed) {
        ret->my_node = my_node;
        ret->sguid = sguid;
        ret->vip_group_id = vip_group_id;
        ret->replication_type = replication_type;
        fthLockInit(&ret->lock);
        ret->hash = HashMap_create(CR_SHARD_KEY_LOCK_HASH_BUCKETS,
                                   NONE /* lock type */);
        failed = (!ret->hash);
        TAILQ_INIT(&ret->all_list);
        TAILQ_INIT(&ret->modify_list);
        TAILQ_INIT(&ret->modify_list);
        TAILQ_INIT(&ret->running_modify_list);
        ret->running_modify_list_count = 0;
        ret->ref_count = 1;
        ret->recovery_count = 0;
        TAILQ_INIT(&ret->get_list);
        ret->get_count = 0;

#ifdef ALWAYS_RECOVERING
        rklc_start_recovery_sync(ret);
#endif /* def ALWAYS_RECOVERING */
    }

    if (failed && ret) {
        rklc_free(ret);
        ret = NULL;
    }

    return (ret);
}

void
rklc_free(struct replicator_key_lock_container *container)
{
    if (container) {
#ifdef ALWAYS_RECOVERING
        rklc_recovery_complete(container);
#endif
        rklc_ref_count_dec(container);
    }
}

int
rklc_get_lock_count(struct replicator_key_lock_container *container) {
    fthWaitEl_t *container_lock;
    int ret;

    container_lock = fthLock(&container->lock, 0 /* read lock */, NULL);

    ret = container->all_list_count;

    fthUnlock(container_lock);

    return (ret);
}

static void
rklc_ref_count_dec(struct replicator_key_lock_container *container) {
    int after;

    after = __sync_sub_and_fetch(&container->ref_count, 1);
    plat_assert(after >= 0);
    if (!after) {
        if (container->hash) {
            HashMap_destroy(container->hash);
        }
        plat_free(container);
    }
}

/**
 * @brief set grant time for lock
 *
 * Preconditions:
 * key_lock->container->lock is held for writing
 * key_lock->lock_mode is set
 * key_lock->lock_count has been incremented
 *
 * The rest of the processing is currently handled separately
 * in #replicator_key_lock (for uncontended locks) and #rkl_unlock
 * (for contended locks).
 *
 * XXX: drew 2010-02-23 The code would be simpler and just a few instructions
 * slower if we just always forced the contended path.
 */
static void
rkl_granted_locked(struct replicator_key_lock *key_lock) {
    struct rklc_get *get;

    switch (key_lock->lock_mode) {
    case RKL_MODE_EXCLUSIVE:
    case RKL_MODE_RECOVERY:
        key_lock->current_modifying_grant_ltime =
            rklc_get_ltime(key_lock->container);

        if (key_lock->first_modifying_grant_ltime ==
            CR_SHARD_KEY_LOCK_LTIME_INVALID) {
            key_lock->first_modifying_grant_ltime =
                key_lock->current_modifying_grant_ltime;
            TAILQ_INSERT_TAIL(&key_lock->container->modify_list, key_lock,
                              modify_list_entry);
        }

        TAILQ_INSERT_TAIL(&key_lock->container->running_modify_list, key_lock,
                          running_modify_list_entry);
        ++key_lock->container->running_modify_list_count;

        /* Preserve the reserve count from a previous acquisition */
        if (!key_lock->reserve_count) {
            key_lock->reserve_count = key_lock->container->get_count;
        }

        ++key_lock->reserve_count;

        /*
         * XXX: drew 2010-02-25 If this becomes problematic due to having
         * a large number of gets, it is easily optimized by maintaining
         * a separate list of gets which have no overlapping modifies.
         *
         * It would also be correct but less efficient to eliminate the
         * oldest_overlapping_modify field and iterate over the full
         * key_lock->container->modify_list for garbage collection on
         * #rklc_get_complete.
         */
        TAILQ_FOREACH(get, &key_lock->container->get_list, get_list_entry) {
            if (!get->oldest_overlapping_modify) {
                get->oldest_overlapping_modify = key_lock;
            }
        }
        break;

    case RKL_MODE_SHARED:
        break;

    case RKL_MODE_NONE:
        plat_fatal("impossible situation");
        break;
    }
}

static void
rklc_recovery_cb(plat_closure_scheduler_t *context, void *env) {
    struct fthMbox *mbox = (struct fthMbox *)env;
    fthMboxPost(mbox, 0 /* doesn't matter */);
}

void
rklc_start_recovery(struct replicator_key_lock_container *container,
                    rklc_recovery_cb_t cb) {
    /* FIXME: drew 2010-02-17 can't be NOP */
    (void) __sync_add_and_fetch(&container->recovery_count, 1);
    plat_closure_apply(rklc_recovery_cb, &cb);
}

/*
 * @param container <IN> Container, may be locked or unlocked.  Since
 * lists are maintained in ltime order #rklc_get_ltime is
 * probably called with container->lock held.
 */
static rklc_ltime_t
rklc_get_ltime(struct replicator_key_lock_container *container) {
    return (__sync_fetch_and_add(&container->current_ltime, 1));
}

void
rklc_start_recovery_sync(struct replicator_key_lock_container *container) {
    rklc_recovery_cb_t cb;
    struct fthMbox mbox;

    fthMboxInit(&mbox);

    cb = rklc_recovery_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                 &rklc_recovery_cb, &mbox);
    rklc_start_recovery(container, cb);
    fthMboxWait(&mbox);
}


void
rklc_recovery_complete(struct replicator_key_lock_container *container) {
    int after;

    /* FIXME: drew 2010-02-17 can't be NOP */
    after = __sync_sub_and_fetch(&container->recovery_count, 1);
    plat_assert(after >= 0);
}

void
rklc_lock(struct replicator_key_lock_container *container,
          const SDF_simple_key_t *key, enum rkl_mode lock_mode,
          rkl_cb_t cb) {
    int complete;
    SDF_status_t callback_status;
    fthWaitEl_t *container_lock;
    struct replicator_key_lock *key_lock;
    char *insert_key;
    struct replicator_key_lock_wait *waiter;

    plat_assert(key->len <= sizeof (key->key));
    plat_assert(lock_mode != RKL_MODE_NONE);
    plat_assert(!rkl_cb_is_null(&cb));

    /* Held until lock is released */
    (void) __sync_add_and_fetch(&container->ref_count, 1);

    container_lock = fthLock(&container->lock, 1 /* write lock */, NULL);

    key_lock = HashMap_get1(container->hash, key->key, key->len);
    plat_assert_imply(key_lock, key_lock->lock_mode != RKL_MODE_NONE ||
                      key_lock->reserve_count);

    if (!key_lock) {
        /* NUL terminate so debugging works better */
        insert_key = plat_alloc(key->len + 1);
        plat_assert(insert_key);
        memcpy(insert_key, key->key, key->len);
        insert_key[key->len] = 0;

        plat_calloc_struct(&key_lock);
        plat_assert(key_lock);
        key_lock->container = container;

        /*
         * Simple assignment key_lock->key = *key can reference uninitialized
         * memory which is a valgrind error.
         */
        key_lock->key.len = key->len;
        memcpy(key_lock->key.key, key->key, key->len);
        /* Zero-padding makes debugging more aesthetic */
        if (key->len < sizeof (key->key)) {
            memset(key_lock->key.key + key->len, 0,
                   sizeof (key->key) - key->len);
        }

        key_lock->lock_mode = RKL_MODE_NONE;
        key_lock->lock_count = 0;
        /*
         * rkl_granted_locked(key_lock) makes all list
         * additions and advances reserve_count for modifying operations
         */
        key_lock->reserve_count = 0;
        key_lock->current_modifying_grant_ltime =
        key_lock->first_modifying_grant_ltime =
        key_lock->last_modifying_release_ltime =
            CR_SHARD_KEY_LOCK_LTIME_INVALID;
        TAILQ_INIT(&key_lock->wait_list);

        /* Assumes ownership of insert_key */
        HashMap_put1(container->hash, insert_key, key_lock, key->len);
        TAILQ_INSERT_TAIL(&container->all_list, key_lock,
                          all_list_entry);
        ++container->all_list_count;
    }

    plat_assert_iff(key_lock->lock_mode != RKL_MODE_NONE,
                    key_lock->lock_count != 0);
    plat_assert_imply(key_lock->lock_mode == RKL_MODE_EXCLUSIVE,
                      key_lock->lock_count == 1);
    plat_assert_imply(key_lock->lock_mode == RKL_MODE_EXCLUSIVE,
                      key_lock->reserve_count > 0);

    /*
     * Something's wrong with the underlying storage or recovery
     * implementation if we try to recovery the same object
     * simultaneously except with SDF_REPLICATION_V1_2_way
     * acquires #RKL_MODE_EXCLUSIVE locks for deletes but not puts and
     * can therefore exhibit the following legal sequence of events
     *
     * put foo=1
     * recovery get returns foo = 1
     * lock foo for recovery
     * put foo = 2
     * recovery get returns foo = 2
     * try lock foo for recovery
     */
    plat_assert_imply(container->replication_type != SDF_REPLICATION_V1_2_WAY &&
                      key_lock->lock_mode == RKL_MODE_RECOVERY,
                      lock_mode != RKL_MODE_RECOVERY);

    /* XXX: drew 2010-02-18 Enable once implemented */
#ifdef notyet
    plat_assert_imply(lock_mode == RKL_MODE_RECOVERY &&
                      key_lock->lock_mode == RKL_MODE_EXCLUSIVE,
                      key_lock->reserve_count > 0);

#endif

    /*
     * XXX: drew 2010-04-22 RKL_MODE_RECOVERY locks are treated as
     * exclusive with respect to RKL_MODE_SHARED locks which would 
     * produce sub-optimal recovery and read performance during recovery.
     *
     * Since we don't do this for trac #4080 and implementing this behavior
     * is non-trivial I've punted until later.
     *
     * Specifically, we can get optimal behavior on shared
     * read locks if we allow a transition from RKL_MODE_SHARED->
     * RKL_MODE_RECOVERY and then back if we have a replicator_key_lock
     * object for the RECOVERY case which refers to the normal shared object,
     * in order to differentiate between the RECOVERY holder and SHARED
     * holders without requiring lock holders to specifically track their
     * lock mode which woould be error probe.
     */

    /*
     * XXX: drew 2009-06-18 Should refactor into lock granting code
     * on unlock if practical.
     */
    if (lock_mode == RKL_MODE_RECOVERY && key_lock->reserve_count > 0) {
        complete = 1;
        callback_status = SDF_LOCK_RESERVED;
    } else if (!key_lock->lock_count) {
        key_lock->lock_mode = lock_mode;
        key_lock->lock_count = 1;
        complete = 1;
        callback_status = SDF_SUCCESS;
    } else if (key_lock->lock_mode == RKL_MODE_SHARED &&
               lock_mode == RKL_MODE_SHARED) {
        ++key_lock->lock_count;
        complete = 1;
        callback_status = SDF_SUCCESS;
    } else if (!plat_calloc_struct(&waiter)) {
        complete = 1;
        callback_status = SDF_OUT_OF_MEM;
    } else {
        complete = 0;
        /* Placate Coverity although !complete implies this won't be used */
        callback_status = SDF_FAILURE;
        waiter->lock_mode = lock_mode;
        waiter->granted_cb = cb;
        TAILQ_INSERT_TAIL(&key_lock->wait_list, waiter, wait_list_entry);
    }

    /* Common code for all grant cases which requires a locked container */
    if (complete && callback_status == SDF_SUCCESS) {
        rkl_granted_locked(key_lock);
    }

    fthUnlock(container_lock);

    plat_log_msg(21849, LOG_CAT_LOCKING, PLAT_LOG_LEVEL_TRACE,
                 "replicator_key_lock %p node %u shard 0x%lx vip group %d"
                 " %s lock key %*.*s %s",
                 key_lock, container->my_node, container->sguid,
                 container->vip_group_id, rkl_mode_to_string(lock_mode),
                 key->len, key->len, key->key,
                 complete ? "requested" : "granted");

    /*
     * Common code for all grant cases which can or must run without a
     * locked container
     */
    if (complete) {
        /* Code above uses specific failures */
        plat_assert(callback_status != SDF_FAILURE);
        plat_closure_apply(rkl_cb, &cb, callback_status,
                           key_lock);
        if (callback_status != SDF_SUCCESS) {
            rklc_ref_count_dec(container);
        }
    }
}

struct replicator_key_lock_state {
    struct fthMbox mbox;
    SDF_status_t status;
    struct replicator_key_lock *key_lock;
};

SDF_status_t
rklc_lock_sync(struct replicator_key_lock_container *container,
               const SDF_simple_key_t *key, enum rkl_mode lock_mode,
               struct replicator_key_lock **lock_out) {
    struct replicator_key_lock_state state;
    rkl_cb_t cb;

    fthMboxInit(&state.mbox);
    state.status = SDF_FAILURE;
    state.key_lock = NULL;

    cb = rkl_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS, &rkl_cb,
                       &state);
    rklc_lock(container, key, lock_mode, cb);
    fthMboxWait(&state.mbox);

    *lock_out = state.key_lock;

    return (state.status);
}

static void
rkl_cb(struct plat_closure_scheduler *context, void *env,
       SDF_status_t status_arg, struct replicator_key_lock *key_lock) {
    struct replicator_key_lock_state *state =
        (struct replicator_key_lock_state *)env;
    state->status = status_arg;
    state->key_lock = key_lock;

    fthMboxPost(&state->mbox, 0 /* doesn't matter */);
}

void
rkl_unlock(struct replicator_key_lock *key_lock) {
    struct replicator_key_lock_container *container;
    fthWaitEl_t *container_lock;
    int done;
    TAILQ_HEAD(, replicator_key_lock_wait) grant_list;
    struct replicator_key_lock_wait *waiter;
    struct replicator_key_lock_wait *next_waiter;
    rkl_cb_t cb;
    enum rkl_mode lock_mode;

    container = key_lock->container;

    plat_log_msg(21796, LOG_CAT_LOCKING,
                 PLAT_LOG_LEVEL_TRACE,
                 "replicator_key_lock %p node %u shard 0x%lx vip group %d"
                 " %s unlock key %*.*s",
                 key_lock, container->my_node, container->sguid,
                 container->vip_group_id,
                 rkl_mode_to_string(key_lock->lock_mode),
                 key_lock->key.len, key_lock->key.len, key_lock->key.key);

    plat_assert(key_lock->lock_mode != RKL_MODE_NONE);
    plat_assert(key_lock->lock_count > 0);
    /*
     * XXX: drew 2010-02-23 This changes when we add support for shared +
     * recovering.
     */
    plat_assert_imply(key_lock->lock_count > 1,
                      key_lock->lock_mode == RKL_MODE_SHARED);

    TAILQ_INIT(&grant_list);

    /*
     * To avoid deadlock issues in the lock granted callbacks, a two-pass
     * scheme is used where the waiters being granted locks are queued
     * on grant_list with the key_lock lock held and then executed
     * with it released.
     */
    container_lock = fthLock(&container->lock, 1 /* write lock */, NULL);

    --key_lock->lock_count;
    switch (key_lock->lock_mode) {
    case RKL_MODE_EXCLUSIVE:
    case RKL_MODE_RECOVERY:
        plat_assert(!TAILQ_EMPTY(&container->running_modify_list));
        plat_assert(container->running_modify_list_count > 0);

        TAILQ_REMOVE(&container->running_modify_list, key_lock,
                     running_modify_list_entry);
        --container->running_modify_list_count;

        plat_assert_iff(TAILQ_EMPTY(&container->running_modify_list),
                        !container->running_modify_list_count);

        key_lock->last_modifying_release_ltime =
            rklc_get_ltime(container);

        rkl_reserve_dec_locked(key_lock);
        break;
    case RKL_MODE_SHARED:
        break;
    case RKL_MODE_NONE:
        plat_fatal("impossible situation");
        break;
    }

    if (!key_lock->lock_count) {
        key_lock->lock_mode = RKL_MODE_NONE;
    }

    if (!key_lock->lock_count && TAILQ_EMPTY(&key_lock->wait_list)) {
        rkl_check_free_locked(key_lock);
        lock_mode = RKL_MODE_NONE;
    } else {
        do {
            done = 1;
            if (!TAILQ_EMPTY(&key_lock->wait_list)) {
                waiter = TAILQ_FIRST(&key_lock->wait_list);
                if (!key_lock->lock_count) {
                    key_lock->lock_mode = waiter->lock_mode;
                    key_lock->lock_count = 1;
                    done = 0;
                } else if (key_lock->lock_mode == RKL_MODE_SHARED &&
                           waiter->lock_mode == RKL_MODE_SHARED) {
                    ++key_lock->lock_count;
                    done = 0;
                }

                if (!done) {
                    TAILQ_REMOVE(&key_lock->wait_list, waiter, wait_list_entry);
                    TAILQ_INSERT_TAIL(&grant_list, waiter, wait_list_entry);
                    rkl_granted_locked(key_lock);
                }
            }
        } while (!done);
        lock_mode = key_lock->lock_mode;
    }

    fthUnlock(container_lock);

    plat_assert_imply(!TAILQ_EMPTY(&grant_list),
                      lock_mode != RKL_MODE_NONE);

    TAILQ_FOREACH_SAFE(waiter, &grant_list, wait_list_entry, next_waiter) {
        TAILQ_REMOVE(&grant_list, waiter, wait_list_entry);
        plat_log_msg(21797, LOG_CAT_LOCKING,
                     PLAT_LOG_LEVEL_TRACE,
                     "replicator_key_lock %p node %u shard 0x%lx vip group %d"
                     " %s lock key %*.*s granted",
                     key_lock, container->my_node, container->sguid,
                     container->vip_group_id,
                     rkl_mode_to_string(lock_mode),
                     key_lock->key.len, key_lock->key.len,
                     key_lock->key.key);
        cb = waiter->granted_cb;
        plat_free(waiter);
        plat_closure_apply(rkl_cb, &cb, SDF_SUCCESS,
                           key_lock);
    }

    /* Held for lock duration */
    rklc_ref_count_dec(container);
}

/**
 * @brief Decrement reserve count
 *
 * Side effects do not include currently include
 * a call to rkl_check_free_locked because of how
 * #rkl_unlock is written.
 *
 * @param key_lock <IN> key_lock->container->lock must be held for writing.
 */
static void
rkl_reserve_dec_locked(struct replicator_key_lock *key_lock) {
    plat_assert(key_lock->reserve_count > 0);

    --key_lock->reserve_count;
    if (!key_lock->reserve_count && key_lock->first_modifying_grant_ltime !=
        CR_SHARD_KEY_LOCK_LTIME_INVALID) {
        plat_assert(key_lock->last_modifying_release_ltime !=
                    CR_SHARD_KEY_LOCK_LTIME_INVALID);
        TAILQ_REMOVE(&key_lock->container->modify_list, key_lock,
                     modify_list_entry);

        key_lock->current_modifying_grant_ltime =
        key_lock->first_modifying_grant_ltime =
        key_lock->last_modifying_release_ltime =
            CR_SHARD_KEY_LOCK_LTIME_INVALID;
    }
}

/**
 * @brief Free if counts have reached zero
 *
 * @param key_lock <IN> key_lock->container->lock must be held for writing.
 */
static void
rkl_check_free_locked(struct replicator_key_lock *key_lock) {
    struct replicator_key_lock_container *container;
    const struct replicator_key_lock *removed;

    /*
     * XXX: drew 2010-02-23 Should move all the consistency checks except
     * function preconditions into a separate replicator_key_lock_check
     * function which is called before and after adjustments.
     */
    plat_assert_imply(key_lock->lock_mode == RKL_MODE_EXCLUSIVE ||
                      key_lock->lock_mode == RKL_MODE_RECOVERY,
                      key_lock->reserve_count > 0);

    container = key_lock->container;

    if (!key_lock->lock_count && !key_lock->reserve_count &&
        TAILQ_EMPTY(&key_lock->wait_list)) {
        plat_log_msg(21798, LOG_CAT_LOCKING,
                     PLAT_LOG_LEVEL_TRACE,
                     "replicator_key_lock %p node %u shard 0x%lx vip group %d"
                     " free", key_lock, container->my_node,
                     container->sguid, container->vip_group_id);

        /*
         * XXX: drew 2010-02-23 We want to reset this information when
         * key_lock->reserve_count first hits zero so that we can
         * successfully transition between RKL_MODE_SHARED and
         * the recovery states.
         */
        if (key_lock->first_modifying_grant_ltime !=
            CR_SHARD_KEY_LOCK_LTIME_INVALID) {
            plat_assert(key_lock->last_modifying_release_ltime !=
                        CR_SHARD_KEY_LOCK_LTIME_INVALID);
            TAILQ_REMOVE(&container->modify_list, key_lock, modify_list_entry);
        }

        removed = HashMap_remove1(container->hash, key_lock->key.key,
                                  key_lock->key.len);
        plat_assert(removed == key_lock);
        TAILQ_REMOVE(&container->all_list, key_lock, all_list_entry);
        --container->all_list_count;

        plat_free(key_lock);
    }
}

struct rklc_get *
rklc_start_get(struct replicator_key_lock_container *container) {
    struct rklc_get *ret;
    fthWaitEl_t *container_lock;
    struct replicator_key_lock *lock;

    plat_calloc_struct(&ret);
    if (ret) {
        ret->container = container;

        container_lock = fthLock(&container->lock, 1 /* write lock */, NULL);
        ret->start_ltime = rklc_get_ltime(container);

        ret->oldest_overlapping_modify =
            TAILQ_FIRST(&container->running_modify_list);

        TAILQ_FOREACH(lock, &container->running_modify_list,
                      running_modify_list_entry) {
            /* XXX: drew 2010-02-23 Trace logging could be useful */
            ++lock->reserve_count;
        }

        TAILQ_INSERT_TAIL(&container->get_list, ret, get_list_entry);
        ++container->get_count;
        ++container->ref_count;

        fthUnlock(container_lock);
    }

    return (ret);
}

void
rklc_get_complete(struct rklc_get *get) {
    struct replicator_key_lock_container *container = get->container;
    fthWaitEl_t *container_lock;
    struct replicator_key_lock *key_lock;
    struct replicator_key_lock *next_key_lock;
    rklc_ltime_t end_ltime;

    container_lock = fthLock(&container->lock, 1 /* write lock */, NULL);

    plat_assert(!TAILQ_EMPTY(&container->get_list));
    plat_assert(container->get_count > 0);

    end_ltime = rklc_get_ltime(container);

    for (key_lock = get->oldest_overlapping_modify; key_lock;
         key_lock = next_key_lock) {
        next_key_lock = TAILQ_NEXT(key_lock, modify_list_entry);

        if ((key_lock->last_modifying_release_ltime ==
             CR_SHARD_KEY_LOCK_LTIME_INVALID ||
             get->start_ltime < key_lock->last_modifying_release_ltime) &&
            end_ltime > key_lock->first_modifying_grant_ltime) {
            rkl_reserve_dec_locked(key_lock);
            /* May free lock */
            rkl_check_free_locked(key_lock);
        }
    }

    TAILQ_REMOVE(&container->get_list, get, get_list_entry);
    --container->get_count;

    fthUnlock(container_lock);

    plat_free(get);

    rklc_ref_count_dec(container);
}

const char *
rkl_mode_to_string(enum rkl_mode mode) {
    switch (mode) {
#define item(caps, lower) case caps: return (#lower);
    RKL_MODE_ITEMS()
#undef item
    }

    plat_assert(0);

    return (NULL);
}
