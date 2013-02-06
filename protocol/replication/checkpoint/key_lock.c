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
 * $Id: key_lock.c 11835 2010-02-25 01:29:20Z drew $
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

struct cr_shard_key_lock;
struct cr_shard_key_lock_wait;

enum {
    /** @brief Number of hash buckets */
    CR_SHARD_KEY_LOCK_HASH_BUCKETS = 101,
};

enum {
    /** @brief Out-of-band value for ltime entries */
    CR_SHARD_KEY_LOCK_LTIME_INVALID = -1
};

typedef int64_t cr_shard_key_lock_ltime_t;

struct cr_shard_key_lock_container {
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

    /** @brief Lock */
    fthLock_t lock;

    /**
     * @brief Hash of key to cr_shard_key_lock structures
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
    cr_shard_key_lock_ltime_t current_ltime;

    /** @brief List of all key locks to make debugging easier */
    TAILQ_HEAD(, cr_shard_key_lock) all_list;

    /** @brief List of reserved keys in ltime order */
    TAILQ_HEAD(, cr_shard_key_lock) modify_list;

    /** @brief List of running modify operations in ltime order */
    TAILQ_HEAD(, cr_shard_key_lock) running_modify_list;

    /** @brief Number of entries on running modify list */
    int running_modify_list_count;

    /**
     * @brief Total reference count
     *
     * One reference count is held on the container until the user calls
     * #cr_shard_key_lock_container_free, one reference count is held for
     * each #cr_shard_key_lock call which was successful and has not had
     * #cr_shard_key_unlock called on the returned object, and one reference
     * count is held for each #cr_shard_key_start_get return for which
     * #cr_shard_key_get_complete has not been called.
     */
    int ref_count;

    /** @brief Number of recoveries pending */
    int recovery_count;

    /** @brief List of pending gets in ltime order */
    TAILQ_HEAD(, cr_shard_key_get) get_list;

    /** @brief Number of entries on get list */
    int get_count;

};

/** @brief Structure for each pending recovery get */
struct cr_shard_key_get {
    /** @brief Parent container */
    struct cr_shard_key_lock_container *container;

    /** @brief Starting ltime */
    cr_shard_key_lock_ltime_t start_ltime;

    /** @brief Oldest modify lock existing at time of get start */
    struct cr_shard_key_lock *oldest_existing_modify_at_start;

    /** @brief Entry in container->get_list */
    TAILQ_ENTRY(cr_shard_key_get) get_list_entry;
};

/** @brief Lock on a key within the shard */
struct cr_shard_key_lock {
    /** @brief Parent container */
    struct cr_shard_key_lock_container *container;

    /**
     * @brief Key
     *
     * FIXME: drew 2009-06-18 Replace with syndrome to work with cache mode
     * that treats all keys with the same syndrome as equivalent.
     */
#ifndef notyet
    SDF_simple_key_t key;
#else
    uint64_t syndrome;
#endif

    /** @brief Current lock mode */
    enum cr_lock_mode lock_mode;

    /** @brief Number of lock holders */
    int lock_count;

    /**
     * @brief Number of overlapping get operations
     *
     * Lock table entries are retained until all simultaneous get operations
     * have completed.  One reserve count is included for modification
     * (CR_LOCK_MODE_EXCLUSIVE or CR_LOCK_MODE_RECOVERY)
     */
    int reserve_count;

    /*
     * A modifying lock is mode CR_LOCK_MODE_EXCLUSIVE or
     * CR_LOCK_MODE_RECOVERY.
     */
    /** @brief ltime of current modification */
    cr_shard_key_lock_ltime_t current_modifying_grant_ltime;

    /** @brief Ltime of first modifying grant */
    cr_shard_key_lock_ltime_t first_modifying_grant_ltime;

    /** @brief Ltime of last modifying release */
    cr_shard_key_lock_ltime_t last_modifying_release_ltime;

    /** @brief Waiters */
    TAILQ_HEAD(, cr_shard_key_lock_wait) wait_list;

    /** @brief Entry in container->all_list */
    TAILQ_ENTRY(cr_shard_key_lock) all_list_entry;

    /** @brief Entry in container->modify_list */
    TAILQ_ENTRY(cr_shard_key_lock) modify_list_entry;

    /** @brief Entry in container->running_modify_list */
    TAILQ_ENTRY(cr_shard_key_lock) running_modify_list_entry;
};

/** @brief Wait list entry for #cr_shard_key_lock */
struct cr_shard_key_lock_wait {
    /** @brief Lock mode */
    enum cr_lock_mode lock_mode;

    /** @brief Applied when lock is granted. */
    cr_shard_key_lock_cb_t granted_cb;

    /** @brief Entry on cr_shard_key_lock */
    TAILQ_ENTRY(cr_shard_key_lock_wait) wait_list_entry;
};

static void cr_shard_key_lock_container_ref_count_dec(struct cr_shard_key_lock_container *container);
static void cr_shard_key_lock_granted_locked(struct cr_shard_key_lock *lock);
static cr_shard_key_lock_ltime_t cr_shard_key_lock_container_get_ltime(struct cr_shard_key_lock_container *container);
static void cr_shard_key_lock_container_recovery_cb(plat_closure_scheduler_t *context,
                                                    void *env);
static void cr_shard_key_lock_reserve_dec_locked(struct cr_shard_key_lock *key_lock);
static void cr_shard_key_lock_check_free_locked(struct cr_shard_key_lock *key_lock);
static const char *cr_lock_mode_to_string(enum cr_lock_mode mode)
    __attribute__((const));

struct cr_shard_key_lock_container *
cr_shard_key_lock_container_alloc(vnode_t my_node, SDF_shardid_t sguid,
                                  int vip_group_id) {
    struct cr_shard_key_lock_container *ret;
    int failed;

    failed = !plat_calloc_struct(&ret);

    if (!failed) {
        ret->my_node = my_node;
        ret->sguid = sguid;
        ret->vip_group_id = vip_group_id;
        fthLockInit(&ret->lock);
        ret->hash = HashMap_create(CR_SHARD_KEY_LOCK_HASH_BUCKETS,
                                   FTH_MAP_RW /* lock */);
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
        cr_shard_key_lock_container_start_recovery_sync(ret);
#endif /* def ALWAYS_RECOVERING */
    }

    if (failed && ret) {
        cr_shard_key_lock_container_free(ret);
        ret = NULL;
    }

    return (ret);
}

void
cr_shard_key_lock_container_free(struct cr_shard_key_lock_container *container)
{
    if (container) {
#ifdef ALWAYS_RECOVERING
        cr_shard_key_lock_container_recovery_complete(container);
#endif
        cr_shard_key_lock_container_ref_count_dec(container);
    }
}

static void
cr_shard_key_lock_container_ref_count_dec(struct cr_shard_key_lock_container *container) {
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
 * in #cr_shard_key_lock (for uncontended locks) and #cr_shard_key_unlock
 * (for contended locks).
 *
 * XXX: drew 2010-02-23 The code would be simpler and just a few instructions
 * slower if we just always forced the contended path.
 */
static void
cr_shard_key_lock_granted_locked(struct cr_shard_key_lock *key_lock) {
    switch (key_lock->lock_mode) {
    case CR_LOCK_MODE_EXCLUSIVE:
    case CR_LOCK_MODE_RECOVERY:
        key_lock->current_modifying_grant_ltime =
            cr_shard_key_lock_container_get_ltime(key_lock->container);

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
        break;

    case CR_LOCK_MODE_SHARED:
        break;

    case CR_LOCK_MODE_NONE:
        plat_fatal("impossible situation");
        break;
    }
}

/*
 * @param container <IN> Container, may be locked or unlocked.  Since
 * lists are maintained in ltime order #cr_shard_key_lock_container_get_ltime is
 * probably called with container->lock held.
 */
static cr_shard_key_lock_ltime_t
cr_shard_key_lock_container_get_ltime(struct cr_shard_key_lock_container *container) {
    return (__sync_fetch_and_add(&container->current_ltime, 1));
}

void
cr_shard_key_lock_container_start_recovery_sync(struct cr_shard_key_lock_container *container) {
    cr_shard_key_lock_recovery_cb_t cb;
    struct fthMbox mbox;

    fthMboxInit(&mbox);

    cb = cr_shard_key_lock_recovery_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                              &cr_shard_key_lock_container_recovery_cb,
                                              &mbox);
    cr_shard_key_lock_container_start_recovery(container, cb);
}

static void
cr_shard_key_lock_container_recovery_cb(plat_closure_scheduler_t *context,
                                        void *env) {
    struct fthMbox *mbox = (struct fthMbox *)env;
    fthMboxPost(mbox, 0 /* doesn't matter */);
}

void
cr_shard_key_lock_container_start_recovery(struct cr_shard_key_lock_container *container,
                                           cr_shard_key_lock_recovery_cb_t cb) {
    /* FIXME: drew 2010-02-17 can't be NOP */
    __sync_add_and_fetch(&container->recovery_count, 1);
    plat_closure_apply(cr_shard_key_lock_recovery_cb, &cb);
}

void
cr_shard_key_lock_container_recovery_complete(struct cr_shard_key_lock_container *container) {
    int after;

    /* FIXME: drew 2010-02-17 can't be NOP */
    after = __sync_sub_and_fetch(&container->recovery_count, 1);
    plat_assert(after >= 0);
}

void
cr_shard_key_lock(struct cr_shard_key_lock_container *container,
                  const SDF_simple_key_t *key, enum cr_lock_mode lock_mode,
                  cr_shard_key_lock_cb_t cb) {
    int complete;
    SDF_status_t callback_status;
    fthWaitEl_t *container_lock;
    struct cr_shard_key_lock *key_lock;
    char *insert_key;
    struct cr_shard_key_lock_wait *waiter;

    plat_assert(lock_mode != CR_LOCK_MODE_NONE);
    plat_assert(!cr_shard_key_lock_cb_is_null(&cb));

    /* Held until lock is released */
    __sync_add_and_fetch(&container->ref_count, 1);

    container_lock = fthLock(&container->lock, 1 /* write lock */, NULL);

    key_lock = HashMap_get1(container->hash, key->key, key->len);
    if (!key_lock) {
        /* NUL terminate so debugging works better */
        insert_key = plat_alloc(key->len + 1);
        plat_assert(insert_key);
        memcpy(insert_key, key->key, key->len);
        insert_key[key->len] = 0;

        plat_calloc_struct(&key_lock);
        plat_assert(key_lock);
        key_lock->container = container;
        key_lock->key = *key;
        key_lock->lock_mode = CR_LOCK_MODE_NONE;
        key_lock->lock_count = 0;
        /*
         * cr_shard_key_lock_granted_locked(key_lock) makes all list
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
    }

    plat_assert_iff(key_lock->lock_mode != CR_LOCK_MODE_NONE,
                    key_lock->lock_count != 0);
    plat_assert_imply(key_lock->lock_mode == CR_LOCK_MODE_EXCLUSIVE,
                      key_lock->lock_count == 1);
    plat_assert_imply(key_lock->lock_mode == CR_LOCK_MODE_EXCLUSIVE,
                      key_lock->reserve_count > 0);

    /*
     * Something's wrong with the underlying storage or recovery
     * implementation if we try to recovery the same object
     * simultaneously.
     */
    plat_assert_imply(key_lock->lock_mode == CR_LOCK_MODE_RECOVERY,
                      lock_mode != CR_LOCK_MODE_RECOVERY);

    /*
     * FIXME: drew 2010-02-18 Making CR_LOCK_MODE_RECOVERY locks
     * co-exist with CR_LOCK_MODE_SHARED is a non-trivial exercise
     * not required for trac #4080.
     */
    plat_assert_imply(key_lock->lock_mode == CR_LOCK_MODE_RECOVERY,
                      lock_mode == CR_LOCK_MODE_EXCLUSIVE);
    plat_assert_imply(lock_mode == CR_LOCK_MODE_RECOVERY,
                      key_lock->lock_mode == CR_LOCK_MODE_NONE ||
                      key_lock->lock_mode == CR_LOCK_MODE_EXCLUSIVE);

    /* XXX: drew 2010-02-18 Enable once implemented */
#ifdef notyet
    plat_assert_imply(lock_mode == CR_LOCK_MODE_RECOVERY &&
                      key_lock->lock_mode == CR_LOCK_MODE_EXCLUSIVE,
                      key_lock->reserve_count > 0);

#endif

    /*
     * FIXME: drew 2010-02-23 We can get correct behavior on shared
     * read locks if we allow a transition from CR_LOCK_MODE_SHARED->
     * CR_LOCK_MODE_RECOVERY and then back if we have a cr_shard_key_lock
     * object for the RECOVERY case which refers to the normal shared object,
     * in order to differentiate between the RECOVERY holder and SHARED
     * holders without requiring lock holders to specifically track their
     * lock mode which woould be error probe.
     */

    /*
     * XXX: drew 2009-06-18 Should refactor into lock granting code
     * on unlock if practical.
     */
    if (lock_mode == CR_LOCK_MODE_RECOVERY && key_lock->reserve_count > 0) {
        complete = 1;
        callback_status = SDF_LOCK_RESERVED;
    } else if (!key_lock->lock_count) {
        key_lock->lock_mode = lock_mode;
        key_lock->lock_count = 1;
        complete = 1;
        callback_status = SDF_SUCCESS;
    } else if (key_lock->lock_mode == CR_LOCK_MODE_SHARED &&
               lock_mode == CR_LOCK_MODE_SHARED) {
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
        cr_shard_key_lock_granted_locked(key_lock);
    }

    fthUnlock(container_lock);

    plat_log_msg(21384, LOG_CAT_LOCKING, PLAT_LOG_LEVEL_TRACE,
                 "cr_key_lock %p node %u shard 0x%lx vip group %d"
                 " %s lock key %*.*s %s",
                 key_lock, container->my_node, container->sguid,
                 container->vip_group_id, cr_lock_mode_to_string(lock_mode),
                 key->len, key->len, key->key,
                 complete ? "requested" : "granted");

    /*
     * Common code for all grant cases which can or must run without a
     * locked container
     */
    if (complete) {
        /* Code above uses specific failures */
        plat_assert(callback_status != SDF_FAILURE);
        plat_closure_apply(cr_shard_key_lock_cb, &cb, callback_status,
                           key_lock);
        if (callback_status != SDF_SUCCESS) {
            cr_shard_key_lock_container_ref_count_dec(container);
        }
    }
}

void
cr_shard_key_unlock(struct cr_shard_key_lock *key_lock) {
    struct cr_shard_key_lock_container *container;
    fthWaitEl_t *container_lock;
    int done;
    TAILQ_HEAD(, cr_shard_key_lock_wait) grant_list;
    struct cr_shard_key_lock_wait *waiter;
    struct cr_shard_key_lock_wait *next_waiter;
    cr_shard_key_lock_cb_t cb;
    enum cr_lock_mode lock_mode;

    container = key_lock->container;

    plat_log_msg(21385, LOG_CAT_LOCKING,
                 PLAT_LOG_LEVEL_TRACE,
                 "cr_shard_key_lock %p node %u shard 0x%lx vip group %d"
                 " %s unlock key %*.*s",
                 key_lock, container->my_node, container->sguid,
                 container->vip_group_id,
                 cr_lock_mode_to_string(key_lock->lock_mode),
                 key_lock->key.len, key_lock->key.len, key_lock->key.key);

    plat_assert(key_lock->lock_mode != CR_LOCK_MODE_NONE);
    plat_assert(key_lock->lock_count > 0);
    /*
     * XXX: drew 2010-02-23 This changes when we add support for shared +
     * recovering.
     */
    plat_assert_imply(key_lock->lock_count > 1,
                      key_lock->lock_mode == CR_LOCK_MODE_SHARED);

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
    case CR_LOCK_MODE_EXCLUSIVE:
    case CR_LOCK_MODE_RECOVERY:
        plat_assert(!TAILQ_EMPTY(&container->running_modify_list));
        plat_assert(container->running_modify_list_count > 0);

        TAILQ_REMOVE(&container->running_modify_list, key_lock,
                     running_modify_list_entry);
        --container->running_modify_list_count;

        plat_assert_iff(TAILQ_EMPTY(&container->running_modify_list),
                        !container->running_modify_list_count);

        key_lock->last_modifying_release_ltime =
            cr_shard_key_lock_container_get_ltime(container);

        cr_shard_key_lock_reserve_dec_locked(key_lock);
        break;
    case CR_LOCK_MODE_SHARED:
        break;
    case CR_LOCK_MODE_NONE:
        plat_fatal("impossible situation");
        break;
    }

    if (!key_lock->lock_count) {
        key_lock->lock_mode = CR_LOCK_MODE_NONE;
    }

    if (!key_lock->lock_count && TAILQ_EMPTY(&key_lock->wait_list)) {
        cr_shard_key_lock_check_free_locked(key_lock);
        lock_mode = CR_LOCK_MODE_NONE;
    } else {
        do {
            done = 1;
            if (!TAILQ_EMPTY(&key_lock->wait_list)) {
                waiter = TAILQ_FIRST(&key_lock->wait_list);
                if (!key_lock->lock_count) {
                    key_lock->lock_mode = waiter->lock_mode;
                    key_lock->lock_count = 1;
                    done = 0;
                } else if (key_lock->lock_mode == CR_LOCK_MODE_SHARED &&
                           waiter->lock_mode == CR_LOCK_MODE_SHARED) {
                    ++key_lock->lock_count;
                    done = 0;
                }

                if (!done) {
                    TAILQ_REMOVE(&key_lock->wait_list, waiter, wait_list_entry);
                    TAILQ_INSERT_TAIL(&grant_list, waiter, wait_list_entry);
                    cr_shard_key_lock_granted_locked(key_lock);
                }
            }
        } while (!done);
        lock_mode = key_lock->lock_mode;
    }

    fthUnlock(container_lock);

    plat_assert_imply(!TAILQ_EMPTY(&grant_list),
                      lock_mode != CR_LOCK_MODE_NONE);

    TAILQ_FOREACH_SAFE(waiter, &grant_list, wait_list_entry, next_waiter) {
        TAILQ_REMOVE(&grant_list, waiter, wait_list_entry);
        plat_log_msg(21387, LOG_CAT_LOCKING,
                     PLAT_LOG_LEVEL_TRACE,
                     "cr_shard_key_lock %p node %u shard 0x%lx vip group %d"
                     " %s lock key %*.*s granted",
                     key_lock, container->my_node, container->sguid,
                     container->vip_group_id,
                     cr_lock_mode_to_string(lock_mode),
                     key_lock->key.len, key_lock->key.len,
                     key_lock->key.key);
        cb = waiter->granted_cb;
        plat_free(waiter);
        plat_closure_apply(cr_shard_key_lock_cb, &cb, SDF_SUCCESS,
                           key_lock);
    }

    /* Held for lock duration */
    cr_shard_key_lock_container_ref_count_dec(container);
}

/**
 * @brief Decrement reserve count
 *
 * Side effects do not include currently include
 * a call to cr_shard_key_lock_check_free_locked because of how
 * #cr_shard_key_unlock is written.
 *
 * @param key_lock <IN> key_lock->container->lock must be held for writing.
 */
static void
cr_shard_key_lock_reserve_dec_locked(struct cr_shard_key_lock *key_lock) {
    plat_assert(key_lock->reserve_count > 0);
    plat_assert(key_lock->lock_mode == CR_LOCK_MODE_EXCLUSIVE ||
                key_lock->lock_mode == CR_LOCK_MODE_RECOVERY);

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
cr_shard_key_lock_check_free_locked(struct cr_shard_key_lock *key_lock) {
    struct cr_shard_key_lock_container *container;

    /*
     * XXX: drew 2010-02-23 Should move all the consistency checks except
     * function preconditions into a separate cr_shard_key_lock_check
     * function which is called before and after adjustments.
     */
    plat_assert_imply(key_lock->lock_mode == CR_LOCK_MODE_EXCLUSIVE ||
                      key_lock->lock_mode == CR_LOCK_MODE_RECOVERY,
                      key_lock->reserve_count > 0);

    container = key_lock->container;

    if (!key_lock->lock_count && !key_lock->reserve_count &&
        TAILQ_EMPTY(&key_lock->wait_list)) {
        plat_log_msg(21386, LOG_CAT_LOCKING,
                     PLAT_LOG_LEVEL_TRACE,
                     "cr_shard_key_lock %p node %u shard 0x%lx vip group %d"
                     " free", key_lock, container->my_node,
                     container->sguid, container->vip_group_id);

        /*
         * XXX: drew 2010-02-23 We want to reset this information when
         * key_lock->reserve_count first hits zero so that we can
         * successfully transition between CR_LOCK_MODE_SHARED and
         * the recovery states.
         */
        if (key_lock->first_modifying_grant_ltime !=
            CR_SHARD_KEY_LOCK_LTIME_INVALID) {
            plat_assert(key_lock->last_modifying_release_ltime !=
                        CR_SHARD_KEY_LOCK_LTIME_INVALID);
            TAILQ_REMOVE(&container->modify_list, key_lock, modify_list_entry);
        }

        HashMap_remove1(container->hash, key_lock->key.key,
                        key_lock->key.len);
        TAILQ_REMOVE(&container->all_list, key_lock, all_list_entry);

        plat_free(key_lock);
    }
}

struct cr_shard_key_get *
cr_shard_key_start_get(struct cr_shard_key_lock_container *container) {
    struct cr_shard_key_get *ret;
    fthWaitEl_t *container_lock;
    struct cr_shard_key_lock *lock;

    plat_calloc_struct(&ret);
    if (ret) {
        ret->container = container;

        container_lock = fthLock(&container->lock, 1 /* write lock */, NULL);
        ret->start_ltime = cr_shard_key_lock_container_get_ltime(container);

        ret->oldest_existing_modify_at_start =
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
cr_shard_key_get_complete(struct cr_shard_key_get *get) {
    struct cr_shard_key_lock_container *container = get->container;
    fthWaitEl_t *container_lock;
    struct cr_shard_key_lock *key_lock;
    struct cr_shard_key_lock *next_key_lock;
    cr_shard_key_lock_ltime_t end_ltime;

    container_lock = fthLock(&container->lock, 1 /* write lock */, NULL);

    plat_assert(!TAILQ_EMPTY(&container->get_list));
    plat_assert(container->get_count > 0);

    end_ltime = cr_shard_key_lock_container_get_ltime(container);

    for (key_lock = get->oldest_existing_modify_at_start; key_lock;
         key_lock = next_key_lock) {
        next_key_lock = TAILQ_NEXT(key_lock, modify_list_entry);

        if ((key_lock->last_modifying_release_ltime ==
             CR_SHARD_KEY_LOCK_LTIME_INVALID ||
             get->start_ltime < key_lock->last_modifying_release_ltime) &&
            end_ltime > key_lock->first_modifying_grant_ltime) {
            cr_shard_key_lock_reserve_dec_locked(key_lock);
            /* May free lock */
            cr_shard_key_lock_check_free_locked(key_lock);
        }
    }

    TAILQ_REMOVE(&container->get_list, get, get_list_entry);
    --container->get_count;

    fthUnlock(container_lock);

    plat_free(get);

    cr_shard_key_lock_container_ref_count_dec(container);
}

static const char *
cr_lock_mode_to_string(enum cr_lock_mode mode) {
    switch (mode) {
#define item(caps, lower) case caps: return ("lower");
    CR_LOCK_MODE_ITEMS()
#undef item
    }

    plat_assert(0);

    return (NULL);
}
