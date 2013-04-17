/*
 * File:   fdftypes.h
 * Author: Darpan Dinker
 *
 * Created on February 4, 2008, 4:20 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdftypes.h 13379 2010-05-02 07:02:09Z drew $
 */

#ifndef _FDFTYPES_H
#define _FDFTYPES_H

#ifdef __cplusplus
extern "C" {
#endif

#include <inttypes.h>
#include <stdint.h>
#include <limits.h>

#define MAX_HASHSYN 65535

/* FDF */
typedef enum {
    FDF_FALSE = 0,
    FDF_TRUE = 1
} FDF_boolean_t;

typedef enum {
    FDF_PHYSICAL_CNTR 	= 0x0,
    FDF_VIRTUAL_CNTR 	= 0x1,
} FDF_container_mode_t;


typedef uint32_t baddr_t;
typedef uint16_t cntr_id_t;
typedef uint16_t hashsyn_t;
typedef uint32_t FDF_time_t; 
typedef uint64_t FDF_cguid_t;

/**
 * @brief Statuses and their value
 * 
 * item(caps, value)
 */
 
/*  Be sure to add new codes at end so that rolling upgrades
 *  don't get screwed up because of inconsistent status codes!!!!!
 */ 
 
#define FDF_STATUS_ITEMS() \
    item(FDF_SUCCESS, = 1) \
    item(FDF_FAILURE, /* default */) \
    item(FDF_FAILURE_GENERIC, /* default */) \
    item(FDF_FAILURE_CONTAINER_GENERIC, /* default */) \
    item(FDF_FAILURE_CONTAINER_NOT_OPEN, /* default */) \
    item(FDF_FAILURE_INVALID_CONTAINER_TYPE, /* default */) \
    item(FDF_INVALID_PARAMETER, /* default */) \
    item(FDF_CONTAINER_UNKNOWN, /* default */) \
    item(FDF_UNPRELOAD_CONTAINER_FAILED, /* default */) \
    item(FDF_CONTAINER_EXISTS, /* default */) \
    item(FDF_SHARD_NOT_FOUND, /* default */) \
    item(FDF_OBJECT_UNKNOWN, /* default */) \
    item(FDF_OBJECT_EXISTS, /* default */) \
    item(FDF_OBJECT_TOO_BIG, /* default */) \
    item(FDF_FAILURE_STORAGE_READ, /* default */) \
    item(FDF_FAILURE_STORAGE_WRITE, /* default */) \
    item(FDF_FAILURE_MEMORY_ALLOC, /* default */) \
    item(FDF_LOCK_INVALID_OP, /* default */) \
    item(FDF_ALREADY_UNLOCKED, /* default */) \
    item(FDF_ALREADY_READ_LOCKED, /* default */) \
    item(FDF_ALREADY_WRITE_LOCKED, /* default */) \
    item(FDF_OBJECT_NOT_CACHED, /* default */) \
    item(FDF_SM_WAITING, /* default */) \
    item(FDF_TOO_MANY_OPIDS, /* default */) \
    item(FDF_TRANS_CONFLICT, /* default */) \
    item(FDF_PIN_CONFLICT, /* default */) \
    item(FDF_OBJECT_DELETED, /* default */) \
    item(FDF_TRANS_NONTRANS_CONFLICT, /* default */) \
    item(FDF_ALREADY_READ_PINNED, /* default */) \
    item(FDF_ALREADY_WRITE_PINNED, /* default */) \
    item(FDF_TRANS_PIN_CONFLICT, /* default */) \
    item(FDF_PIN_NONPINNED_CONFLICT, /* default */) \
    item(FDF_TRANS_FLUSH, /* default */) \
    item(FDF_TRANS_LOCK, /* default */) \
    item(FDF_TRANS_UNLOCK, /* default */) \
    item(FDF_UNSUPPORTED_REQUEST, /* default */) \
    item(FDF_UNKNOWN_REQUEST, /* default */) \
    item(FDF_BAD_PBUF_POINTER, /* default */) \
    item(FDF_BAD_PDATA_POINTER, /* default */) \
   item(FDF_BAD_SUCCESS_POINTER, /* default */) \
    item(FDF_NOT_PINNED, /* default */) \
    item(FDF_NOT_READ_LOCKED, /* default */) \
    item(FDF_NOT_WRITE_LOCKED, /* default */) \
    item(FDF_PIN_FLUSH, /* default */) \
    item(FDF_BAD_CONTEXT, /* default */) \
    item(FDF_IN_TRANS, /* default */) \
    item(FDF_NONCACHEABLE_CONTAINER, /* default */) \
    item(FDF_OUT_OF_CONTEXTS, /* default */) \
    item(FDF_INVALID_RANGE, /* default */) \
    item(FDF_OUT_OF_MEM, /* default */) \
    item(FDF_NOT_IN_TRANS, /* default */) \
    item(FDF_TRANS_ABORTED, /* default */) \
    item(FDF_FAILURE_MBOX, /* default */) \
    item(FDF_FAILURE_MSG_ALLOC, /* default */) \
    item(FDF_FAILURE_MSG_SEND, /* default */) \
    item(FDF_FAILURE_MSG_RECEIVE, /* default */) \
    item(FDF_ENUMERATION_END, /* default */) \
    item(FDF_BAD_KEY, /* default */) \
    item(FDF_FAILURE_CONTAINER_OPEN, /* default */) \
    item(FDF_BAD_PEXPTIME_POINTER, /* default */) \
    item(FDF_BAD_PINVTIME_POINTER, /* default */) \
    item(FDF_BAD_PSTAT_POINTER, /* default */) \
    item(FDF_BAD_PPCBUF_POINTER, /* default */) \
    item(FDF_BAD_SIZE_POINTER, /* default */) \
    item(FDF_EXPIRED, /* default */) \
    item(FDF_EXPIRED_FAIL, /* default */) \
    item(FDF_PROTOCOL_ERROR, /* default */)\
    item(FDF_TOO_MANY_CONTAINERS, /* default */)\
    item(FDF_STOPPED_CONTAINER, /* default */)\
    item(FDF_GET_METADATA_FAILED, /* default */)\
    item(FDF_PUT_METADATA_FAILED, /* default */)\
    item(FDF_GET_DIRENTRY_FAILED, /* default */)\
    item(FDF_EXPIRY_GET_FAILED, /* default */)\
    item(FDF_EXPIRY_DELETE_FAILED, /* default */)\
    item(FDF_EXIST_FAILED, /* default */)\
    item(FDF_NO_PSHARD, /* default */)\
    item(FDF_SHARD_DELETE_SERVICE_FAILED, /* default */) \
    item(FDF_START_SHARD_MAP_ENTRY_FAILED, /* default */) \
    item(FDF_STOP_SHARD_MAP_ENTRY_FAILED, /* default */) \
    item(FDF_DELETE_SHARD_MAP_ENTRY_FAILED, /* default */) \
    item(FDF_CREATE_SHARD_MAP_ENTRY_FAILED, /* default */) \
    item(FDF_FLASH_DELETE_FAILED, /* default */) \
    item(FDF_FLASH_EPERM, /* default */) \
    item(FDF_FLASH_ENOENT, /* default */) \
    item(FDF_FLASH_EAGAIN, /* default */) \
    item(FDF_FLASH_ENOMEM, /* default */) \
    item(FDF_FLASH_EDATASIZE, /* default */) \
    item(FDF_FLASH_EBUSY, /* default */) \
    item(FDF_FLASH_EEXIST, /* default */) \
    item(FDF_FLASH_EACCES, /* default */) \
    item(FDF_FLASH_EINVAL, /* default */) \
    item(FDF_FLASH_EMFILE, /* default */) \
    item(FDF_FLASH_ENOSPC, /* default */) \
    item(FDF_FLASH_ENOBUFS, /* default */) \
    item(FDF_FLASH_EDQUOT, /* default */) \
    item(FDF_FLASH_STALE_CURSOR, /* default */) \
    item(FDF_FLASH_EDELFAIL, /* default */) \
    item(FDF_FLASH_EINCONS, /* default */) \
    item(FDF_STALE_LTIME, /* default */) \
    item(FDF_WRONG_NODE, /* default */) \
    item(FDF_UNAVAILABLE, /* default */) \
    item(FDF_TEST_FAIL, /* default */) \
    item(FDF_TEST_CRASH, /* default */) \
    item(FDF_VERSION_CHECK_NO_PEER, /* default */) \
    item(FDF_VERSION_CHECK_BAD_VERSION, /* default */) \
    item(FDF_VERSION_CHECK_FAILED, /* default */) \
    item(FDF_META_DATA_VERSION_TOO_NEW, /* default */) \
    item(FDF_META_DATA_INVALID, /* default */) \
    item(FDF_BAD_META_SEQNO, /* default */) \
    item(FDF_BAD_LTIME, /* default */) \
    item(FDF_LEASE_EXISTS, /* default */) \
    /** @brief Subsystem has conflicting requests in  progress */ \
    item(FDF_BUSY, /* default */) \
    /* @brief Subsystem already shutdown */ \
    item(FDF_SHUTDOWN, /* default */) \
    item(FDF_TIMEOUT, /* default */) \
    item(FDF_NODE_DEAD, /* default */) \
    item(FDF_SHARD_DOES_NOT_EXIST, /* default */) \
    item(FDF_STATE_CHANGED, /* default */) \
    item(FDF_NO_META, /* default */) \
    item(FDF_TEST_MODEL_VIOLATION, /* default */) \
    item(FDF_REPLICATION_NOT_READY, /* default */) \
    item(FDF_REPLICATION_BAD_TYPE, /* default */) \
    item(FDF_REPLICATION_BAD_STATE, /* default */) \
    item(FDF_NODE_INVALID, /* default */) \
    item(FDF_CORRUPT_MSG, /* default */) \
    item(FDF_QUEUE_FULL, /* default */) \
    item(FDF_RMT_CONTAINER_UNKNOWN, /* default */) \
    item(FDF_FLASH_RMT_EDELFAIL, /* default */)  \
    /* @brief Lock request conflicts with a reserved lock */ \
    item(FDF_LOCK_RESERVED, /* default */) \
    item(FDF_KEY_TOO_LONG, /* default */) \
    item(FDF_NO_WRITEBACK_IN_STORE_MODE, /* default */) \
    item(FDF_WRITEBACK_CACHING_DISABLED, /* default */) \
    /* @brief Update ignored due to duplicate info */ \
    item(FDF_UPDATE_DUPLICATE, /* default */) \
    item(FDF_FAILURE_CONTAINER_TOO_SMALL, /* default */) \
    item(FDF_CONTAINER_FULL, /* default */) \
    item(FDF_CANNOT_REDUCE_CONTAINER_SIZE, /* default */) \
    item(FDF_CANNOT_CHANGE_CONTAINER_SIZE, /* default */) \
    item(FDF_OUT_OF_STORAGE_SPACE, /* default */) \
    item(FDF_FAILURE_ALREADY_IN_TRANS, /* default */) \
    item(FDF_FAILURE_NO_TRANS, /* default */) \
    item(FDF_CANNOT_DELETE_OPEN_CONTAINER, /* default */) \
    item(FDF_FAILURE_INVALID_KEY_SIZE, /* default */) \
    item(FDF_FAILURE_OPERATION_DISALLOWED, /* default */) \
    item(FDF_FAILURE_ILLEGAL_CONTAINER_ID, /* default */) \
    item(FDF_FAILURE_CONTAINER_NOT_FOUND, /* default */) \
    item(FDF_UNLIMITED_CONTAINER_MUST_BE_NON_EVICTING, /* default */) \
    item(FDF_THREAD_CONTEXT_BUSY, /*Multiple parallel fdf calls using same thread context */) \
    item(FDF_LICENSE_CHK_FAILED, /* default */)\
    item(FDF_CONTAINER_OPEN,/* default */)\
    item(FDF_FAILURE_INVALID_CONTAINER_SIZE, /* default */) 

typedef enum {
#define item(caps, value) \
    caps value,
    FDF_STATUS_ITEMS()
#undef item
    N_FDF_STATUS_STRINGS
} FDF_status_t;

#define FDF_NULL_CGUID 0




#ifdef __cplusplus
}
#endif

#endif /* _FDFTYPES_H */
