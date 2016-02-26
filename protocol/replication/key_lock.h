/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef REPLICATION_KEY_LOCK_H
#define REPLICATION_KEY_LOCK_H 1

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
 * $Id: key_lock.h 13074 2010-04-21 23:19:39Z drew $
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
#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif /* SDFAPI */

struct replicator_key_lock_container;
struct replicator_key_lock;
struct rklc_get;

#define RKL_MODE_ITEMS() \
    item(RKL_MODE_NONE, none)                                                  \
    item(RKL_MODE_SHARED, shared)                                              \
    item(RKL_MODE_EXCLUSIVE, exclusive)                                        \
    /**                                                                        \
     * @brief Recovery lock                                                    \
     *                                                                         \
     * Recovery lock acquisition fails immediately if the lock has non-zero    \
     * reserve count.                                                          \
     *                                                                         \
     * Recovery locks may eventually co-exist with RKL_MODE_SHARED locks.      \
     *                                                                         \
     * XXX: drew 2010-02-17 As a simplification, the initial code version      \
     * treats this the same as RKL_MODE_EXCLUSIVE which causes a block         \
     * until all pending RKL_MODE_SHARED locks are released.                   \
     */                                                                        \
    item(RKL_MODE_RECOVERY, recovery)

enum rkl_mode {
#define item(caps, lower) caps,
    RKL_MODE_ITEMS()
#undef item
};

/**
 * @brief Callback for #replicator_key_lock
 *
 * @param status <IN> SDF_SUCCESS or reason for failure
 * @param key_lock <IN> Structure passed to #rkl_unlock
 * @param lock_mode <IN> Lock mode which was granted
 */
PLAT_CLOSURE2(rkl_cb,
              SDF_status_t, status,
              struct replicator_key_lock *, key_lock);

PLAT_CLOSURE(rklc_recovery_cb);
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
 * @param replication_type <IN> Replication type, affecting
 * errors and assertions surrounding legal states.
 *
 * Since SDF_REPLICATION_V1_2_WAY acquires #RKL_MODE_EXCLUSIVE locks
 * for deletes but not puts, it is not an error to attempt a
 * #RKL_MODE_RECOVERY lock on a key with an existing #RKL_MODE_RECOVERY
 * lock on that key.
 *
 * @return New container, NULL on allocation failure.  Free with
 * #rklc_free.
 */
#ifdef KEY_LOCK_CONTAINER
struct replicator_key_lock_container *
replicator_key_lock_container_alloc(vnode_t my_node, SDF_shardid_t sguid,
                                    int vip_group_id,
                                    SDF_replication_t replication_type);
#endif /* KEY_LOCK_CONTAINER */


/**
 * @brief Free lock container
 *
 * #rklc_free may be called from within lock grant
 * callbacks.
 *
 * @param container <IN> Container to free
 */
void rklc_free(struct replicator_key_lock_container *container);

/**
 * @brief Return number of locks
 *
 * This also includes 'reserved' locks which had grant lifetime overlapping
 * one or more currently running get operations started with
 * #rklc_start_get.
 */
int rklc_get_lock_count(struct replicator_key_lock_container *container);


/**
 * @brief Asynchronously acquire lock on the given key
 *
 * #rklc_lock can only be called from an fthThread
 *
 * @param key <IN> key, caller retains ownership
 *
 * @param lock_mode <IN> RKL_MODE_SHARED for shared read lock,
 * RKL_MODE_EXCLUSIVE for exclusive modifying lock for new operations,
 * RKL_MODE_RECOVERY for non-blocking exclusive lock attempts
 * that return SDF_LOCK_RESERVED if there were any overlapping
 * RKL_MODE_EXCLUSIVE or RKL_MODE_RECOVERY locks overlapping
 * a surrounding get.
 *
 * RKL_MODE_RECOVERY has undefined effects when used without pending
 * gets started via #rklc_start_get.
 *
 * @param cb <IN> applied on completion without locks held
 */
void rklc_lock(struct replicator_key_lock_container *container,
               const SDF_simple_key_t *key, enum rkl_mode lock_mode,
               rkl_cb_t cb);

/**
 * @brief Synchronously acquire lock on the given key
 *
 * #rklc_lock_sync can only be called from an fthThread
 *
 * @param key <IN> key, caller retains ownership
 *
 * @param lock_mode <IN> RKL_MODE_SHARED for shared read lock,
 * RKL_MODE_EXCLUSIVE for exclusive modifying lock for new operations,
 * RKL_MODE_RECOVERY for non-blocking exclusive lock attempts
 * that return SDF_LOCK_RESERVED if there were any overlapping
 * RKL_MODE_EXCLUSIVE or RKL_MODE_RECOVERY locks overlapping
 * a surrounding get.
 *
 * RKL_MODE_RECOVERY has undefined effects when used without pending
 * gets started via #rklc_start_get.
 *
 * @param lock_out <OUT> Set to a lock handle which must be passed to
 * #rkl_unlock to release.
 *
 * @return SF_SUCCESS on success, otherwise on failure with
 * SDF_LOCK_RESERVED being a normal case.
 */
SDF_status_t rklc_lock_sync(struct replicator_key_lock_container *container,
                            const SDF_simple_key_t *key,
                            enum rkl_mode lock_mode,
                            struct replicator_key_lock **lock_out);

/**
 * @brief Unlock key lock
 *
 * #rkl_unlock can only be called from an fthThread
 *
 * As a side effect, one or more blocked locks may be executed.  The
 * callback will not be applied with any locks held.
 *
 * @param key_lock <IN> Lock handle to release which was returned
 * by #rklc_lock_sync or specified as a closer argument in the
 * asynchronous #rlkc_lock
 */
void rkl_unlock(struct replicator_key_lock *key_lock);

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
void rklc_start_recovery(struct replicator_key_lock_container *container,
                         rklc_recovery_cb_t cb);

/**
 * @brief Recovery complete
 *
 * This may switch the code to a more efficient mode of operation.
 */
void rklc_recovery_complete(struct replicator_key_lock_container *container);

/**
 * @brief Start recovery get
 *
 * Gets cause all keys with simultaenously held exclusive or recovery locks
 * to remain in the reserved state until they complete.  This is to
 * close the race condition between new deletes (and puts in the case of
 * incremental recovery) and recovery puts which exists because the
 * keys being recovered are unknown at the time the get by cursor
 * operation is performed.
 *
 * @return #rklc_get which must be released via
 * #rklc_get_complete after attempting to acquire
 * #RKL_MODE_RECOVERY locks on all included keys.
 */
struct rklc_get *rklc_start_get(struct replicator_key_lock_container *container);

/**
 * @brief Recovery get complete and corresponding locks acquired
 *
 * This must be called after the recovery get operation completes and
 * RKL_MODE_RECOVERY lock attempts have been made on all included keys.
 *
 * @param get <IN> From #rklc_start_get
 */
void rklc_get_complete(struct rklc_get *get);

const char *rkl_mode_to_string(enum rkl_mode mode) __attribute__((const));

#endif /* ndef REPLICATION_KEY_LOCK_H */
