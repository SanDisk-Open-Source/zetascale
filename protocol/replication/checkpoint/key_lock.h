#ifndef REPLICATION_REPLICATOR_KEY_LOCK_H
#define REPLICATION_REPLICATOR_KEY_LOCK_H 1

/*
 * File:   sdf/protocol/replicator_key_lock.h
 *
 * Author: drew
 *
 * Forked from sdf/protocol/replication/copy_replicator.c on February 16, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: key_lock.h 11826 2010-02-24 19:49:40Z drew $
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

#include "fth/fth.h"

#include "common/sdftypes.h"

struct cr_shard_key_lock_container;
struct cr_shard_key_lock;
struct cr_shard_key_get;

#define CR_LOCK_MODE_ITEMS() \
    item(CR_LOCK_MODE_NONE, none)                                              \
    item(CR_LOCK_MODE_SHARED, shared)                                          \
    item(CR_LOCK_MODE_EXCLUSIVE, exclusive)                                    \
    /**                                                                        \
     * @brief Recovery lock                                                    \
     *                                                                         \
     * Recovery lock acquisition fails immediately if the lock has non-zero    \
     * reserve count.                                                          \
     *                                                                         \
     * Recovery locks may eventually co-exist with CR_LOCK_MODE_SHARED locks.  \
     *                                                                         \
     * XXX: drew 2010-02-17 As a simplification, the initial code version      \
     * treats this the same as CR_LOCK_MODE_EXCLUSIVE which causes a block     \
     * until all pending CR_LOCK_MODE_SHARED locks are released.               \
     */                                                                        \
    item(CR_LOCK_MODE_RECOVERY, recovery)

enum cr_lock_mode {
#define item(caps, lower) caps,
    CR_LOCK_MODE_ITEMS()
#undef item
};

/**
 * @brief Callback for #cr_shard_key_lock
 *
 * @param status <IN> SDF_SUCCESS or reason for failure
 * @param key_lock <IN> Structure passed to #cr_shard_key_unlock
 * @param lock_mode <IN> Lock mode which was granted
 */
PLAT_CLOSURE2(cr_shard_key_lock_cb,
              SDF_status_t, status,
              struct cr_shard_key_lock *, key_lock);

PLAT_CLOSURE(cr_shard_key_lock_recovery_cb);
__BEGIN_DECLS

/**
 * @brief Allocate lock container
 *
 * One container has a common key space.  Typically, one container
 * per shard or vip group is used.
 *
 * @param my_node <IN> Local node.  Used only for log mesages.
 * @param sguid <IN> Shard.  Used only for log messages.
 * @param vip_group_id <IN> Vip group.  Use VIP_GROUP_ID_INVALID for
 * none.  Used only for log messages.
 * @return New container, NULL on allocation failure.  Free with
 * #cr_shard_key_lock_container_free.
 */
struct cr_shard_key_lock_container *
cr_shard_key_lock_container_alloc(vnode_t my_node, SDF_shardid_t sguid,
                                  int vip_group_id);

/**
 * @brief Free lock container
 *
 * #cr_shard_key_lock_container_free may be called from within lock grant
 * callbacks.
 *
 * @param container <IN> Container to free
 */
void cr_shard_key_lock_container_free(struct cr_shard_key_lock_container *container);


/**
 * @brief Asynchronously acquire lock on the given key with shard not locked
 *
 * #cr_shard_key_lock can only be called from an fthThread
 *
 * @param shard <IN> shard.  As a precondition
 * @param key <IN> key, caller retains ownership
 * @param cb <IN> applied on completion without locks held
 */
void cr_shard_key_lock(struct cr_shard_key_lock_container *container,
                       const SDF_simple_key_t *key, enum cr_lock_mode lock_mode,
                       cr_shard_key_lock_cb_t cb);

/**
 * @brief Unlock key lock
 *
 * #cr_shard_key_unlock can only be called from an fthThread
 *
 * As a side effect, one or more blocked locks may be executed.  The
 * callback will not be applied with any locks held.
 */
void cr_shard_key_unlock(struct cr_shard_key_lock *key_lock);

/**
 * @brief Start recovery process
 *
 * Recovery blocks on all existing IOs (which may have been in flight
 * to only a single replica) completing.
 *
 * More than one recovery can be in progress at a time.
 *
 * XXX: drew 2010-02-20 For America we can get away with always running
 * in recovery mode so this becomes a NOP.
 *
 * @param cb <IN> Callback applied when all outstanding operations to
 * previous nodes have completed.
 */
void cr_shard_key_lock_container_start_recovery(struct cr_shard_key_lock_container *container,
                                                cr_shard_key_lock_recovery_cb_t cb);

/**
 * @brief Recovery complete
 *
 * This may switch the code to a more efficient mode of operation.
 */
void cr_shard_key_lock_container_recovery_complete(struct cr_shard_key_lock_container *container);

/**
 * @brief Start recovery get
 *
 * Gets cause all keys with simultaenously held exclusive or recovery locks
 * to remain until the reserved state until they complete.  This is to
 * close the race condition between new deletes (and puts in the case of
 * incremental recovery) and recovery puts which exists because the
 * keys being recovered are unknown at the time the get by cursor
 * operation is performed.
 *
 * @return #cr_shard_key_get which must be released via
 * #cr_shard_key_get_complete after attempting to acquire
 * #CR_LOCK_MODE_RECOVERY locks on all included keys.
 */
struct cr_shard_key_get *cr_shard_key_start_get(struct cr_shard_key_lock_container *container);

/**
 * @brief Recovery get complete and corresponding locks acquired
 *
 * This must be called after the recovery get operation completes and
 * CR_LOCK_MODE_RECOVERY lock attempts have been made on all included keys.
 *
 * @param get <IN> From #cr_shard_key_start_get
 */
void cr_shard_key_get_complete(struct cr_shard_key_get *get);

#endif /* ndef REPLICATION_KEY_LOCK_H */
