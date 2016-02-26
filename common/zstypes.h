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
 * File:   zstypes.h
 * Author: Darpan Dinker
 *
 * Created on February 4, 2008, 4:20 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdftypes.h 13379 2010-05-02 07:02:09Z drew $
 */

#ifndef _ZSTYPES_H
#define _ZSTYPES_H

#ifdef __cplusplus
extern "C" {
#endif

#include <inttypes.h>
#include <stdint.h>
#include <limits.h>

#define MAX_HASHSYN 65535

/* ZS */
typedef enum {
    ZS_FALSE = 0,
    ZS_TRUE = 1
} ZS_boolean_t;

typedef enum {
    ZS_PHYSICAL_CNTR 	= 0x0,
    ZS_VIRTUAL_CNTR 	= 0x1,
} ZS_container_mode_t;


typedef uint64_t baddr_t;
typedef uint16_t cntr_id_t;
typedef uint16_t hashsyn_t;
typedef uint32_t ZS_time_t; 
typedef uint64_t ZS_cguid_t;

/**
 * @brief Statuses and their value
 * 
 * item(caps, value)
 */
 
/*  Be sure to add new codes at end so that rolling upgrades
 *  don't get screwed up because of inconsistent status codes!!!!!
 */ 
 
#define ZS_STATUS_ITEMS() \
    item(ZS_SUCCESS, = 1) \
    item(ZS_FAILURE, /* default */) \
    item(ZS_FAILURE_GENERIC, /* default */) \
    item(ZS_FAILURE_CONTAINER_GENERIC, /* default */) \
    item(ZS_FAILURE_CONTAINER_NOT_OPEN, /* default */) \
    item(ZS_FAILURE_INVALID_CONTAINER_TYPE, /* default */) \
    item(ZS_INVALID_PARAMETER, /* default */) \
    item(ZS_CONTAINER_UNKNOWN, /* default */) \
    item(ZS_UNPRELOAD_CONTAINER_FAILED, /* default */) \
    item(ZS_CONTAINER_EXISTS, /* default */) \
    item(ZS_SHARD_NOT_FOUND, /* default */) \
    item(ZS_OBJECT_UNKNOWN, /* default */) \
    item(ZS_OBJECT_EXISTS, /* default */) \
    item(ZS_OBJECT_TOO_BIG, /* default */) \
    item(ZS_FAILURE_STORAGE_READ, /* default */) \
    item(ZS_FAILURE_STORAGE_WRITE, /* default */) \
    item(ZS_FAILURE_MEMORY_ALLOC, /* default */) \
    item(ZS_LOCK_INVALID_OP, /* default */) \
    item(ZS_ALREADY_UNLOCKED, /* default */) \
    item(ZS_ALREADY_READ_LOCKED, /* default */) \
    item(ZS_ALREADY_WRITE_LOCKED, /* default */) \
    item(ZS_OBJECT_NOT_CACHED, /* default */) \
    item(ZS_SM_WAITING, /* default */) \
    item(ZS_TOO_MANY_OPIDS, /* default */) \
    item(ZS_TRANS_CONFLICT, /* default */) \
    item(ZS_PIN_CONFLICT, /* default */) \
    item(ZS_OBJECT_DELETED, /* default */) \
    item(ZS_TRANS_NONTRANS_CONFLICT, /* default */) \
    item(ZS_ALREADY_READ_PINNED, /* default */) \
    item(ZS_ALREADY_WRITE_PINNED, /* default */) \
    item(ZS_TRANS_PIN_CONFLICT, /* default */) \
    item(ZS_PIN_NONPINNED_CONFLICT, /* default */) \
    item(ZS_TRANS_FLUSH, /* default */) \
    item(ZS_TRANS_LOCK, /* default */) \
    item(ZS_TRANS_UNLOCK, /* default */) \
    item(ZS_UNSUPPORTED_REQUEST, /* default */) \
    item(ZS_UNKNOWN_REQUEST, /* default */) \
    item(ZS_BAD_PBUF_POINTER, /* default */) \
    item(ZS_BAD_PDATA_POINTER, /* default */) \
    item(ZS_BAD_SUCCESS_POINTER, /* default */) \
    item(ZS_NOT_PINNED, /* default */) \
    item(ZS_NOT_READ_LOCKED, /* default */) \
    item(ZS_NOT_WRITE_LOCKED, /* default */) \
    item(ZS_PIN_FLUSH, /* default */) \
    item(ZS_BAD_CONTEXT, /* default */) \
    item(ZS_IN_TRANS, /* default */) \
    item(ZS_NONCACHEABLE_CONTAINER, /* default */) \
    item(ZS_OUT_OF_CONTEXTS, /* default */) \
    item(ZS_INVALID_RANGE, /* default */) \
    item(ZS_OUT_OF_MEM, /* default */) \
    item(ZS_NOT_IN_TRANS, /* default */) \
    item(ZS_TRANS_ABORTED, /* default */) \
    item(ZS_FAILURE_MBOX, /* default */) \
    item(ZS_FAILURE_MSG_ALLOC, /* default */) \
    item(ZS_FAILURE_MSG_SEND, /* default */) \
    item(ZS_FAILURE_MSG_RECEIVE, /* default */) \
    item(ZS_ENUMERATION_END, /* default */) \
    item(ZS_BAD_KEY, /* default */) \
    item(ZS_FAILURE_CONTAINER_OPEN, /* default */) \
    item(ZS_BAD_PEXPTIME_POINTER, /* default */) \
    item(ZS_BAD_PINVTIME_POINTER, /* default */) \
    item(ZS_BAD_PSTAT_POINTER, /* default */) \
    item(ZS_BAD_PPCBUF_POINTER, /* default */) \
    item(ZS_BAD_SIZE_POINTER, /* default */) \
    item(ZS_EXPIRED, /* default */) \
    item(ZS_EXPIRED_FAIL, /* default */) \
    item(ZS_PROTOCOL_ERROR, /* default */)\
    item(ZS_TOO_MANY_CONTAINERS, /* default */)\
    item(ZS_STOPPED_CONTAINER, /* default */)\
    item(ZS_GET_METADATA_FAILED, /* default */)\
    item(ZS_PUT_METADATA_FAILED, /* default */)\
    item(ZS_GET_DIRENTRY_FAILED, /* default */)\
    item(ZS_EXPIRY_GET_FAILED, /* default */)\
    item(ZS_EXPIRY_DELETE_FAILED, /* default */)\
    item(ZS_EXIST_FAILED, /* default */)\
    item(ZS_NO_PSHARD, /* default */)\
    item(ZS_SHARD_DELETE_SERVICE_FAILED, /* default */) \
    item(ZS_START_SHARD_MAP_ENTRY_FAILED, /* default */) \
    item(ZS_STOP_SHARD_MAP_ENTRY_FAILED, /* default */) \
    item(ZS_DELETE_SHARD_MAP_ENTRY_FAILED, /* default */) \
    item(ZS_CREATE_SHARD_MAP_ENTRY_FAILED, /* default */) \
    item(ZS_FLASH_DELETE_FAILED, /* default */) \
    item(ZS_FLASH_EPERM, /* default */) \
    item(ZS_FLASH_ENOENT, /* default */) \
    item(ZS_FLASH_EIO, /* default */) \
    item(ZS_FLASH_EAGAIN, /* default */) \
    item(ZS_FLASH_ENOMEM, /* default */) \
    item(ZS_FLASH_EDATASIZE, /* default */) \
    item(ZS_FLASH_EBUSY, /* default */) \
    item(ZS_FLASH_EEXIST, /* default */) \
    item(ZS_FLASH_EACCES, /* default */) \
    item(ZS_FLASH_EINVAL, /* default */) \
    item(ZS_FLASH_EMFILE, /* default */) \
    item(ZS_FLASH_ENOSPC, /* default */) \
    item(ZS_FLASH_ENOBUFS, /* default */) \
    item(ZS_FLASH_EDQUOT, /* default */) \
    item(ZS_FLASH_STALE_CURSOR, /* default */) \
    item(ZS_FLASH_EDELFAIL, /* default */) \
    item(ZS_FLASH_EINCONS, /* default */) \
    item(ZS_STALE_LTIME, /* default */) \
    item(ZS_WRONG_NODE, /* default */) \
    item(ZS_UNAVAILABLE, /* default */) \
    item(ZS_TEST_FAIL, /* default */) \
    item(ZS_TEST_CRASH, /* default */) \
    item(ZS_VERSION_CHECK_NO_PEER, /* default */) \
    item(ZS_VERSION_CHECK_BAD_VERSION, /* default */) \
    item(ZS_VERSION_CHECK_FAILED, /* default */) \
    item(ZS_META_DATA_VERSION_TOO_NEW, /* default */) \
    item(ZS_META_DATA_INVALID, /* default */) \
    item(ZS_BAD_META_SEQNO, /* default */) \
    item(ZS_BAD_LTIME, /* default */) \
    item(ZS_LEASE_EXISTS, /* default */) \
    /** @brief Subsystem has conflicting requests in  progress */ \
    item(ZS_BUSY, /* default */) \
    /* @brief Subsystem already shutdown */ \
    item(ZS_SHUTDOWN, /* default */) \
    item(ZS_TIMEOUT, /* default */) \
    item(ZS_NODE_DEAD, /* default */) \
    item(ZS_SHARD_DOES_NOT_EXIST, /* default */) \
    item(ZS_STATE_CHANGED, /* default */) \
    item(ZS_NO_META, /* default */) \
    item(ZS_TEST_MODEL_VIOLATION, /* default */) \
    item(ZS_REPLICATION_NOT_READY, /* default */) \
    item(ZS_REPLICATION_BAD_TYPE, /* default */) \
    item(ZS_REPLICATION_BAD_STATE, /* default */) \
    item(ZS_NODE_INVALID, /* default */) \
    item(ZS_CORRUPT_MSG, /* default */) \
    item(ZS_QUEUE_FULL, /* default */) \
    item(ZS_RMT_CONTAINER_UNKNOWN, /* default */) \
    item(ZS_FLASH_RMT_EDELFAIL, /* default */)  \
    /* @brief Lock request conflicts with a reserved lock */ \
    item(ZS_LOCK_RESERVED, /* default */) \
    item(ZS_KEY_TOO_LONG, /* default */) \
    item(ZS_NO_WRITEBACK_IN_STORE_MODE, /* default */) \
    item(ZS_WRITEBACK_CACHING_DISABLED, /* default */) \
    /* @brief Update ignored due to duplicate info */ \
    item(ZS_UPDATE_DUPLICATE, /* default */) \
    item(ZS_FAILURE_CONTAINER_TOO_SMALL, /* default */) \
    item(ZS_CONTAINER_FULL, /* default */) \
    item(ZS_CANNOT_REDUCE_CONTAINER_SIZE, /* default */) \
    item(ZS_CANNOT_CHANGE_CONTAINER_SIZE, /* default */) \
    item(ZS_OUT_OF_STORAGE_SPACE, /* default */) \
    item(ZS_TRANS_LEVEL_EXCEEDED, /* default */) \
    item(ZS_FAILURE_NO_TRANS, /* default */) \
    item(ZS_CANNOT_DELETE_OPEN_CONTAINER, /* default */) \
    item(ZS_FAILURE_INVALID_KEY_SIZE, /* default */) \
    item(ZS_FAILURE_OPERATION_DISALLOWED, /* default */) \
    item(ZS_FAILURE_ILLEGAL_CONTAINER_ID, /* default */) \
    item(ZS_FAILURE_CONTAINER_NOT_FOUND, /* default */) \
    item(ZS_UNLIMITED_CONTAINER_MUST_BE_NON_EVICTING, /* default */) \
    item(ZS_THREAD_CONTEXT_BUSY, /*Multiple parallel fdf.calls using same thread context */) \
    item(ZS_LICENSE_CHK_FAILED, /* default */)\
    item(ZS_CONTAINER_OPEN,/* default */)\
    item(ZS_FAILURE_INVALID_CONTAINER_SIZE, /* default */) \
    item(ZS_FAILURE_INVALID_CONTAINER_STATE, /* default */) \
    item(ZS_FAILURE_CONTAINER_DELETED, /* default */) \
    item(ZS_QUERY_DONE, /* Completion of the ZS Range Query */) \
    item(ZS_FAILURE_CANNOT_CREATE_METADATA_CACHE, /* default */) \
    item(ZS_WARNING, /* default */) \
    item(ZS_QUERY_PAUSED, /* Query is paused by callback */) \
    item(ZS_SNAPSHOT_NOT_FOUND, /* Snapshot not found */) \
    item(ZS_TOO_MANY_SNAPSHOTS, /* No room for additional snapshots */) \
    item(ZS_SCAN_DONE, /*Scavenger scan done flag */) \
    item(ZS_RESCUE_INVALID_REQUEST, /* Invalid context to rescue */) \
    item(ZS_RESCUE_NOT_NEEDED, /* This error is already rescued */) \
    item(ZS_RESCUE_IO_ERROR, /* Rescue hit IO error */) \

typedef enum {
#define item(caps, value) \
    caps value,
    ZS_STATUS_ITEMS()
#undef item
    N_ZS_STATUS_STRINGS
} ZS_status_t;

#define ZS_NULL_CGUID 0

// For zsck
typedef enum {
    ZSCHECK_LABEL = 0,
    ZSCHECK_SUPERBLOCK,
    ZSCHECK_SHARD_DESCRIPTOR,
    ZSCHECK_SHARD_PROPERTIES,
    ZSCHECK_SEGMENT_LIST,
    ZSCHECK_CLASS_DESCRIPTOR,
    ZSCHECK_CKPT_DESCRIPTOR,
    ZSCHECK_FLOG_RECORD,
    ZSCHECK_LOG_PAGE_HEADER,
    ZSCHECK_SLAB_METADATA,
    ZSCHECK_SLAB_DATA,
    ZSCHECK_POT,
    ZSCHECK_POT_BITMAP,
    ZSCHECK_SLAB_BITMAP,
    ZSCHECK_BTREE_NODE,
    ZSCHECK_CONTAINER_META,
    ZSCHECK_SHARD_SPACE_MAP,
    ZSCHECK_OBJECT_TABLE,
    ZSCHECK_STORM_LOG,
} ZS_check_entity_t;

typedef enum {
    ZSCHECK_SUCCESS = 0,
    ZSCHECK_FAILURE,
    ZSCHECK_INFO,
    ZSCHECK_READ_ERROR,
    ZSCHECK_WRITE_ERROR,
    ZSCHECK_LABEL_ERROR,
    ZSCHECK_MAGIC_ERROR,
    ZSCHECK_CHECKSUM_ERROR,
    ZSCHECK_BTREE_ERROR,
    ZSCHECK_LSN_ERROR,
    ZSCHECK_CONTAINER_META_ERROR,
    ZSCHECK_SHARD_SPACE_MAP_ERROR,
} ZS_check_error_t;

typedef struct {
    ZS_check_entity_t entity;
    uint64_t id;
    ZS_check_error_t error;
    char *msg;
} ZS_check_log_entry_t;
    
int zscheck_init_log(char *file);
int zscheck_close_log();
    
void zscheck_log_msg(
    ZS_check_entity_t entity,
    uint64_t id,
    ZS_check_error_t error,
    char *msg
    );

typedef enum {
    ZSCHECK_NO_CHECK = 0,
    ZSCHECK_NO_INIT,
    ZSCHECK_BTREE_CHECK,
} ZS_check_mode_t;

// zscheck logfile default
#define  ZSCHECK_LOG_DEFAULT "/tmp/zsck.log"

// zs stats logging levels
#define ZS_STATS_LEVEL_NO_STATS          0
#define ZS_STATS_LEVEL_SUMMARY           1
#define ZS_STATS_LEVEL_CNTR_LIST         2
#define ZS_STATS_LEVEL_CNTR_STATS        3
#define ZS_STATS_LEVEL_FLASH_STATS       4
#define ZS_STATS_LEVEL_ALL_STATS         5

#ifdef __cplusplus
}
#endif

#endif /* _ZSTYPES_H */
