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

#ifndef SDFAPI

#define ISEMPTY(s) (s == NULL || *s == '\0')
#define HAVE_STDBOOL_H 1

/* Get a consistent bool type */
#if HAVE_STDBOOL_H
# include <stdbool.h>
#else
  typedef enum {false = 0, true = 1} bool;
#endif


enum {
    SDF_ILLEGAL_VNODE = UINT32_MAX,
    /* The code currently assumes that node ids are assigned from 0 */
    SDF_ILLEGAL_PNODE = UINT32_MAX
};

/* FIXME: do the right thing portable across architectures */
#define uint64_to_ptr_cast(arg64)               \
    ((void *)arg64)
#define ptr_to_uint64_cast(voidstar)            \
    ((uint64_t)voidstar)
#define uint64_to_shmem_ptr_cast(shmem, arg64)   \
    (shmem.base.int_base = arg64);

/* XXX: drew 2010-02-18 Used in over-the-wire representations */
#define SDF_SIMPLE_KEY_SIZE   256 // POSIX_PATH_MAX

/* XXX: drew 2010-02-18 Used in over-the-wire representations */
typedef struct {
    char        key[SDF_SIMPLE_KEY_SIZE];
    uint32_t    len;
} SDF_simple_key_t;

#define CMC_PATH "/sdf/CMC"

/*
 * maximum number of containers supported by one instance of SDF
 */
#define MCD_MAX_NUM_CNTRS       128

// Not 0 so that we can differentiate uninitialized
#define CMC_CGUID 1
#define CMC_HOME 0
#define CMC_CGUID_INITIAL_VALUE 1

#define MAX_OBJECT_ID_SIZE 256
#define MAX_CGUID_STR_LEN  21         // Enough for 20 digits + 1 for null (2**64)

typedef unsigned long SDF_operation_t;
typedef unsigned long SDF_size_t;

#define SDF_NULL_CGUID UINT64_MAX

#define SDF_MAX_OPIDS          16
#define SDF_RESERVED_CONTEXTS  1024

enum {
    SYS_FLASH_REFORMAT = 0,
    SYS_FLASH_RECOVERY = 1,
};

// Defines the type of meta object maintained in the 
enum {
    SDF_META_TYPE_UNDEFINED          = 0,
    SDF_META_TYPE_CONTAINER          = 1,
    SDF_META_TYPE_CGUID_MAP          = 2,
    SDF_META_TYPE_CGUID_STATE        = 3,
};

typedef uint64_t SDF_context_t;
typedef void SDF_internal_ctxt_t;
typedef int32_t SDF_vnode_t;
typedef uint32_t SDF_cacheid_t;
typedef uint32_t SDF_meta_t;

/** @brief Out-of-band values for sequence numbers */
enum {
    SDF_SEQUENCE_NO_INVALID = UINT64_MAX,
    /** @brief Inclusive */
    SDF_SEQUENCE_NO_LIMIT = UINT64_MAX - 1
};

/** @brief Out-of-band values for #SDF_shardid_t */
enum {
    SDF_SHARDID_INVALID = UINT64_MAX,
    /** @brief Everything with this set is for non-shard use */
    SDF_SHARDID_RESERVED_OFFSET = 1ULL << (CHAR_BIT * sizeof (uint64_t) - 1),

    /**
     * @brief Prefix for VIP groups masquerading as shardids
     *
     * XXX: drew 2009-08-09 For V1 treating intra node vip groups as shards
     * for lease management and meta-data storage  purposes was most expedient
     * given the short time frame available.
     *
     * Add to a group id and be happy.
     */
    SDF_SHARDID_VIP_GROUP_OFFSET = 3ULL << (CHAR_BIT * sizeof (uint64_t) - 2),

    /** @brief Inclusive */
    SDF_SHARDID_LIMIT = SDF_SHARDID_RESERVED_OFFSET - 1
};

typedef uint64_t SDF_shardid_t;
typedef uint64_t SDF_cguid_t;
typedef uint64_t SDF_thrdid_t;
typedef uint64_t SDF_transid_t;
typedef uint64_t SDF_objectid_t;
typedef uint32_t SDF_time_t;
typedef uint32_t SDF_tag_t;
typedef int32_t vnode_t;
#define SDF_Null_Context 0


#ifdef notdef
typedef struct {
    uint16_t     flags;
    uint16_t     segment;
    uint32_t     offset;
} SDFContainer;
#endif
typedef void *SDFContainer;

typedef enum {
    SDF_DRAM, SDF_FLASH, SDF_DISK
} SDF_storage_type_t;

typedef enum {
    SDF_FALSE = 0, SDF_TRUE = 1
} SDF_boolean_t;

typedef enum {
    SDF_UNLOCKED, SDF_READ_LOCK, SDF_WRITE_LOCK // , UPDATE
} SDF_lock_type_t;

typedef enum {
    UNPINNED, PINNED
} SDF_pin_type_t;

typedef enum sdf_cluster_grp_type {
    SDF_CLUSTER_GRP_TYPE_INDEPENDENT,
    SDF_CLUSTER_GRP_TYPE_SIMPLE_REPLICATION,
    SDF_CLUSTER_GRP_TYPE_MIRRORED,
    SDF_CLUSTER_GRP_TYPE_NPLUS1,
}SDF_cluster_grp_type_t;

typedef struct {
    unsigned long long id;
} SDF_tx_id;

// typedef unsigned SDF_command_status_t;
// typedef unsigned long long SDF_completion_t;

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
    item(FDF_UPDATE_DUPLICATE, /* default */)


typedef enum {
#define item(caps, value) \
    caps value,
    FDF_STATUS_ITEMS()
#undef item
    N_FDF_STATUS_STRINGS
} FDF_status_t;

    /* these MUST be kept in sync with above enums! */
#ifndef _INSTANTIATE_SDF_STATUS_STRINGS
    extern char *FDF_Status_Strings[];
#else
    char *FDF_Status_Strings[] = {
	"UNKNOWN_STATUS", /* since SDF_SUCCESS is 1! */
#define item(caps, value) \
        #caps,
        FDF_STATUS_ITEMS()
#undef item
    };
#endif

static inline int
fdf_status_valid(FDF_status_t status) {
    return (status < N_FDF_STATUS_STRINGS);
}

/* Avoid link order problem with strings */
static inline const char *
fdf_status_to_string(FDF_status_t status) {
    switch (status) {
#define item(caps, value) \
    case caps: return (#caps);
    FDF_STATUS_ITEMS()
#undef item
    default:
        return ("Invalid");
    }
}

/*  Settings for SDF initialization
 */

struct mcd_container;

typedef struct {
    uint32_t       opid;
    SDF_status_t   status;
} SDF_opid_t;

typedef enum {
    ALL_PERSISTENT = -2, PERSISTENT, DEFAULT,
    ONE_LEVEL, TWO_LEVEL, THREE_LEVEL, FOUR_LEVEL, FIVE_LEVEL, SIX_LEVEL
} SDF_hierarchy_level_t;

typedef struct {
    uint32_t size;
    char *name;
} SDF_object_info_t;

#ifdef SWIG
#else
typedef struct {
    SDF_meta_t type;
    uint32_t node;
    SDF_cguid_t cguid;
} SDF_cguid_map_t;
#endif

typedef struct {
    SDF_meta_t type;
    SDF_cguid_t cguid_counter;
} SDF_cguid_state_t;

typedef int stringLen_t;

typedef enum {
    SDF_UNSORTED, SDF_SORT_ASCENDING, SDF_SORT_DESCENDING,
} SDF_enumeration_order_t;

/*
 * Used to enumerate the cache for dirty objects.  The enumerator sets key_len,
 * data_len, create_time and expiry_time and then calls the fill function which
 * will return a pointer to a buffer with enough room for the key and data to
 * be stored concatenated.  If the fill function returns NULL, there is no more
 * room in the buffer and the enumerator returns.  The enumerator sets
 * enum_index1 and enum_index2 to specify how far it is along.  It should
 * appear to be stateless and if passed the same enum_index1 and enum_index2
 * that it had previously set, it should continue where it left off.
 *
 *  cguid       - Container guid that we are interested in.
 *  create_time - Create time for an object.
 *  data_len    - Length of the data.
 *  enum_index1 - This along with enum_index2 is state that may be used by the
 *                enumerator.
 *  enum_index2 - See enum_index1.
 *  expiry_time - Expiry time for an object.
 *  fill        - Fill function to fill the buffer.
 *  fill_state  - Used by the fill function to keep state.
 *  key_len     - Length of the key.
 */
typedef struct SDF_cache_enum {
    SDF_cguid_t cguid;
    int         key_len;
    int         data_len;
    SDF_time_t  create_time;
    SDF_time_t  expiry_time;
    char       *(*fill)(struct SDF_cache_enum *cenum);
    void       *fill_state;
    uint64_t    enum_index1;
    uint64_t    enum_index2;
} SDF_cache_enum_t;
#endif

/* FDF */
typedef enum {
    FDF_FALSE = 0,
    FDF_TRUE = 1
} FDF_boolean_t;

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
    item(FDF_FAILURE_CONTAINER_TOO_SMALL, /* default */)


typedef enum {
#define item(caps, value) \
    caps value,
    FDF_STATUS_ITEMS()
#undef item
    N_FDF_STATUS_STRINGS
} FDF_status_t;




#ifdef __cplusplus
}
#endif

#endif /* _FDFTYPES_H */
