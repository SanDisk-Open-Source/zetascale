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

#ifndef REPLICATION_TEST_MODEL_H
#define REPLICATION_TEST_MODEL_H 1

/*
 * File:   sdf/protocol/replication/tests/replication_test_model.h
 *
 * Author: drew
 *
 * Created on October 30, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_model.h 7551 2009-05-22 18:42:34Z briano $
 */

/**
 * Model of expected replication test results, noting that multiple correct
 * states can exist until observed.
 *
 * See
 *     $(TOP)/docs/arch_manual/replication_test.txt
 * for details.
 */
#include "utils/hashmap.h"
#include "platform/defs.h"
#include "fth/fth.h"
#include "common/sdftypes.h"
#include "protocol/replication/meta_types.h"
#include "common/sdf_properties.h"

#include "test_meta.h"
#include "tlmap3.h"

#define SIMULATOR_SHARD_BUCKET 1000


enum shard_status {
    /** @brief flash_crash */
    SHARD_CRASH = 1 << 0,
    /** @brief flash_start */
    SHARD_START = 1 << 1,
};

struct SDF_shard_meta;
struct replication_test_model_meta {

};

struct replication_test_config *config;
struct replication_test_meta *meta;

struct rtm_hash_map_t;
struct rtm_ctn_sid;

struct replication_test_model {
    /** @brief assosicated model_id */
    uint64_t model_id; // not used currently

    /** @brief Simulated storage */
    TLMap3_t model_map; // not used currently

    /** @brief model status */
    int flags; // 0 for not exist, 1 for probably exist, 2 for exist

    /** @brief model locks */
    fthLock_t model_lock;

    /** @brief hash map for objects */
    struct rtm_hash_map_t *obj_status;

    /** @brief shard meta */
    struct rtm_ctn_sid *shard_status;

    /** @brief ltime */
    sdf_replication_ltime_t ltime;
};

typedef enum {
    RTM_GO_CR_SHARD, RTM_GO_DEL_SHARD,
    RTM_GO_WR, RTM_GO_RD, RTM_GO_DEL,
    RTM_GO_LAST_SEQNO, RTM_GO_GET_CURSORS,
    RTM_GO_GET_BY_CURSOR,
} rtm_go_type_t;


struct rtm_op_entry_t *entry;

struct rtm_general_op {
    rtm_go_type_t go_type;
    void * op_data;
    struct rtm_op_entry_t *entry;
};

typedef struct rtm_general_op rtm_general_op_t;

struct replication_state_read_op *op;

__BEGIN_DECLS

/**
 * @brief Create replication test model
 */
struct replication_test_model *
replication_test_model_alloc(const struct replication_test_config *config);

/**
 * @brief Free replication test model
 */
void
rtm_free(struct replication_test_model *);

/**
 * @brief Return non-zero to indicate the model's constraints have
 * been violated.
 */
int
rtm_failed(const struct replication_test_model *);

/**
 * @brief Start shard create operation
 *
 * @param test_model <IN> Test model
 * @param now <IN> Simulated time at start of the request
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard_meta <IN> shard to create, caller owns buffer
 */
rtm_general_op_t *
rtm_start_create_shard(struct replication_test_model *test_model,
                       struct timeval now, vnode_t node,
                       struct SDF_shard_meta *shard_meta);

/**
 * Shard create operation completed
 *
 * Logs a message via #plat_log_msg when the status is unexpected and
 * indicates failure in the return.  Notably a shard create will fail if
 * a shard create is started later before this message is processed.
 *
 * @param op <IN> operation returned from #rtm_shard_create
 * @param now <IN> Simulated time at end of request
 * @param status <IN> returned status
 * @return 0 when status was accepted, non-zero when it is
 * inappropriate.
 */
int
rtm_create_shard_complete(rtm_general_op_t *op,
                          struct timeval now, SDF_status_t status);

rtm_general_op_t *
rtm_start_last_seqno(struct replication_test_model *test_model,
                       struct timeval now, vnode_t node,
                       SDF_shardid_t shard);

int
rtm_last_seqno_complete(rtm_general_op_t *op,
                          struct timeval now, SDF_status_t status, uint64_t seqno);

rtm_general_op_t *
rtm_start_get_cursors(struct replication_test_model *test_model,
                      SDF_shardid_t shard, struct timeval now, vnode_t node,
		      uint64_t seqno_start, uint64_t seqno_len, uint64_t seqno_max);

int
rtm_get_cursors_complete(rtm_general_op_t *op, struct timeval now, SDF_status_t status, 
			 const void *data, size_t data_len);

rtm_general_op_t *
rtm_start_get_by_cursor(struct replication_test_model *test_model,
                        SDF_shardid_t shard, struct timeval now, vnode_t node,
		        const void *cursor, size_t cursor_len);

int
rtm_get_by_cursor_complete(rtm_general_op_t *op, struct timeval now, 
                           char *key, int key_len, SDF_time_t exptime, 
			   SDF_time_t createtime, uint64_t seqno,
			   SDF_status_t status, const void *data, size_t data_len);

/**
 * @brief Start shard delete operation
 *
 * @param test_model <IN> Test model
 * @param ltime <IN> Shard ltime at which this was started.  ltime
 * advances on configuration changes (For instance, when the home
 * node changes) and not on each operation.
 * See $(TOP)/sdf/protocol/replication/meta_types.h for more detail.
 * @param now <IN> Simulated time at start of the request
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard <IN> Shard id to delete
 */
rtm_general_op_t *
rtm_start_delete_shard(struct replication_test_model *test_model,
                       sdf_replication_ltime_t ltime,
                       struct timeval now, vnode_t node,
                       SDF_shardid_t shard);

/**
 * Shard create operation completed
 *
 * Logs a message via #plat_log_msg when the status is unexpected and
 * indicates failure in the return.  Notably a shard delete will fail if
 * ltime has advanced.
 *
 * @param op <IN> operation returned from #rtm_delete_shard
 * @param now <IN> Simulated time at end of request
 * @param status <IN> returned status
 * @return 0 when status was accepted, non-zero when it is
 * inappropriate.
 */
int
rtm_delete_shard_complete(rtm_general_op_t *op,
                          struct timeval now, SDF_status_t status);


/**
 * @brief Start write operation
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started.  ltime
 * advances on configuration changes (For instance, when the home
 * node changes) and not on each operation.
 * See $(TOP)/sdf/protocol/replication/meta_types.h for more detail.
 * @param now <IN> Simulated time at start of the request
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param meta <IN> Meta-data associated with write including sie of key, data
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @param data <IN> Data
 * @param data_len <IN> Data length
 * @param meta <IN> Meta-data
 * @return  a handle on the write operation which is passed to
 * #rtm_write_complete on completion.
 */
rtm_general_op_t *
rtm_start_write(struct replication_test_model *test_model,
                SDF_shardid_t shard,
                sdf_replication_ltime_t ltime, struct timeval now,
                vnode_t node,
                struct replication_test_meta *meta, const void *key,
                size_t key_len, const void *data, size_t data_len,
                const struct replication_test_model_meta *model_meta);

/**
 * @brief Write operation complete
 *
 * Logs a message via #plat_log_msg when the status is unexpected and
 * failure indicated in the return.  Notably a write started in ltime n
 * which was observed by a read in ltime n+1 to be incomplete
 * cannot complete successfully.
 *
 * @param op <IN> operation returned from #rtm_start_write
 * @param now <IN> Simulated time at end of request
 * @param status <IN> returned status
 * @return 0 when status was accepted, non-zero when it is
 * inappropriate.
 */
int
rtm_write_complete(rtm_general_op_t *op,
                   struct timeval now, SDF_status_t status);

/**
 * @brief Start read operation
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started.  ltime
 * advances on configuration changes (For instance, when the home
 * node changes) and not on each operation.
 * See $(TOP)/sdf/protocol/replication/meta_types.h for more detail.
 * @param now <IN> Simulated time at start of the request
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @return  a handle on the read operation which is passed to
 * #rtm_read_complete on completion.
 */
rtm_general_op_t *
rtm_start_read(struct replication_test_model *test_model,
               SDF_shardid_t shard, sdf_replication_ltime_t ltime,
               const struct timeval now, vnode_t node,
               const void *key, size_t key_len);

/**
 * @brief Read operation complete
 *
 * Logs a message when the status or result is incorrect and indicates
 * the faiure in the return.  Notably read cannot observe a value older
 * than one which has been observed by another read.
 *
 * @param op <IN> operation returned from #rtm_start_read
 * @param now <IN> Simulated time at end of request
 * @param status <IN> returned status
 * @param data <IN> returned data
 * @param data_len <IN> data length
 * @return 0 when status was accepted, non-zero when it is
 * inappropriate.  Notably a write started in ltime n
 * which was observed by a read in ltime n+1 to be incomplete
 * cannot complete successfully.
 */

int
rtm_read_complete(rtm_general_op_t *op,
                  struct timeval now, SDF_status_t status,
                  const void *data, size_t data_len);

/**
 * @brief Start delete operation
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started.  ltime
 * advances on configuration changes (For instance, when the home
 * node changes) and not on each operation.
 * See $(TOP)/sdf/protocol/replication/meta_types.h for more detail.
 * @param now <IN> Simulated time at start of the request
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @return  a handle on the read operation which is passed to
 * #rtm_read_complete on completion.
 */

rtm_general_op_t *
rtm_start_delete(struct replication_test_model *test_model,
                 SDF_shardid_t shard, sdf_replication_ltime_t ltime,
                 struct timeval now, vnode_t node,
                 const void *key, size_t key_len);

/**
 * @brief Delete operation complete
 *
 * Logs a message via #plat_log_msg when the status is unexpected and
 * indeicates failure in the return.  Notably a delete that starts in
 * ltime n will fail if a read in ltime n+1 successfully returns data.
 *
 * @param op <IN> operation returned from #rtm_start_write
 * @param now <IN> Simulated time at end of request
 * @param status <IN> returned status
 * @return 0 when status was accepted, non-zero when it is
 * inappropriate.
 */
int
rtm_delete_complete(rtm_general_op_t *op,
                    struct timeval now, SDF_status_t status);

__END_DECLS

#endif /* ndef REPLICATION_TEST_MODEL_H */
