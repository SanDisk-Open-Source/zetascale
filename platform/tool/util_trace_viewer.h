#ifndef UTIL_TRACE_VIEWER_H
#define UTIL_TRACE_VIEWER_H 1

/*
 * File:   $HeadURL:
 svn://s002.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/
 tool/util_trace_vie w. h $
 * Author: Wei,Li
 *
 * Created on July 31, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 */
/*
 * The trace viewer header for the defined macro, enum and struct. Some of the
 * define is duplicated for memcached.h and sdftypes.h, but in order to keep
 * this viewer seperated from all other commponent, I would like to keep all of
 * them to be redefined here. 
 */
#define HELP_STRING "Usage:\n"\
                    "   -h: show this help.\n"\
                    "   -m: show the current trace content in share memory.\n"\
                    "   -f: -f FILE_PATH_NAME. show the trace content in the "\
                            "file FILE_PATH_NAME.\n"\
                            "       *this option is required if "\
                            "we want to load the trace from file.\n"\
                            "       *if we want to load mutiple files, we "\
                            "could use FILE_PATH_NAME,FILES_NUMBER, such as "\
                            "\"-f end,10\" to load the \"end\" file and the"\
                            " last 10 files.\n"\
                            "       *also we could use -f for mutiple times\n"\
                    "   -a: show all the summary infomation\n"\
                    "   -s: show the status summary infomation\n"\
                    "   -c: show the command distribution infomation\n"\
                    "   -k: show the key size distribution infomation\n"\
                    "   -d: show the data size distribution infomation\n"\
                    "   -r: show the request distribution infomation\n"\
                    "   -u: show the create vs update infomation\n"\
                    "   -w: show the command with return code's distribution\n"\
                    "   -o: show the trace number per connection\n"\
                    "   -i: show the interval distribution\n"\
                    "   -x: dump the status summary to excel"\
                            " importable format\n"\

#define SAFE_FREE(a) if(a){\
                          plat_free(a);\
                          a=0;\
                     }
/*
 * Commands
 *
 * Identifies the memcache command operating on a key.
 */
#define MCD_TRACE_NOREPLY       (1 << 7)
#define MCD_TRACE_MULTIKEY      (1 << 6)
#define TOTAL_COMMAND_TPYES MCD_TRACE_CMD_ARITHMETIC
#define SHOW_CONTENT   (0)

#define SHOW_STATUS         (1<<0)
#define SHOW_COMAMND_DIS    (1<<1)
#define SHOW_KEYSIZE_DIS    (1<<2)
#define SHOW_DATASIZE_DIS   (1<<3)
#define SHOW_REQUEST_DIS    (1<<4)
#define SHOW_CVSU_DIS       (1<<5)
#define SHOW_CMDCODE_DIS    (1<<6)
#define SHOW_CONNECTION_DIS (1<<7)
#define SHOW_INTERVAL_DIS   (1<<8)
#define SHOW_DUMP_DIS       (1<<16)
#define SHOW_ALL            (0xffffffffffffffff>>55)

#define KEY_MAX_LENGTH 256
#define STATUS_MAX_NUMBER 256
#define MAX_INTERVAL 256
#define MAX_CONNECTION (1024*1024)
#define DATA_MAX_SIZE (1024*1024)
#define BUCKET_MAX_NUM (1024*1024)
#define MAX_FILE_OPT 512
typedef unsigned long long ulong64_t;
typedef enum
{
    MCD_TRACE_CMD_GET = 1,
    MCD_TRACE_CMD_SET,
    MCD_TRACE_CMD_ADD,
    MCD_TRACE_CMD_REPLACE,
    MCD_TRACE_CMD_APPEND,
    MCD_TRACE_CMD_PREPEND,
    MCD_TRACE_CMD_CAS,
    MCD_TRACE_CMD_SYNC,
    MCD_TRACE_CMD_DELETE,
    MCD_TRACE_CMD_ARITHMETIC,
} command_id;
static char *command_str[TOTAL_COMMAND_TPYES] = {
    "CMD_GET",
    "CMD_SET",
    "CMD_ADD",
    "CMD_REPLACE",
    "CMD_APPEND",
    "CMD_PREPEND",
    "CMD_CAS",
    "CMD_SYNC",
    "CMD_DELETE",
    "CMD_ARITHMETIC"
};
typedef enum
{
    SDF_SUCCESS = 1,
    SDF_FAILURE,
    SDF_FAILURE_GENERIC,
    SDF_FAILURE_CONTAINER_GENERIC,
    SDF_FAILURE_INVALID_CONTAINER_TYPE,
    SDF_INVALID_PARAMETER,
    SDF_CONTAINER_UNKNOWN,
    SDF_CONTAINER_EXISTS,
    SDF_SHARD_NOT_FOUND,
    SDF_OBJECT_UNKNOWN,
    SDF_OBJECT_EXISTS,
    SDF_FAILURE_STORAGE_READ,
    SDF_FAILURE_STORAGE_WRITE,
    SDF_FAILURE_MEMORY_ALLOC,
    SDF_LOCK_INVALID_OP,
    SDF_ALREADY_UNLOCKED,
    SDF_ALREADY_READ_LOCKED,
    SDF_ALREADY_WRITE_LOCKED,
    SDF_OBJECT_NOT_CACHED,
    SDF_SM_WAITING,
    SDF_TOO_MANY_OPIDS,
    SDF_TRANS_CONFLICT,
    SDF_PIN_CONFLICT,
    SDF_OBJECT_DELETED,
    SDF_TRANS_NONTRANS_CONFLICT,
    SDF_ALREADY_READ_PINNED,
    SDF_ALREADY_WRITE_PINNED,
    SDF_TRANS_PIN_CONFLICT,
    SDF_PIN_NONPINNED_CONFLICT,
    SDF_TRANS_FLUSH,
    SDF_TRANS_LOCK,
    SDF_TRANS_UNLOCK,
    SDF_UNSUPPORTED_REQUEST,
    SDF_UNKNOWN_REQUEST,
    SDF_BAD_PBUF_POINTER,
    SDF_BAD_PDATA_POINTER,
    SDF_BAD_SUCCESS_POINTER,
    SDF_NOT_PINNED,
    SDF_NOT_READ_LOCKED,
    SDF_NOT_WRITE_LOCKED,
    SDF_PIN_FLUSH,
    SDF_BAD_CONTEXT,
    SDF_IN_TRANS,
    SDF_NONCACHEABLE_CONTAINER,
    SDF_OUT_OF_CONTEXTS,
    SDF_INVALID_RANGE,
    SDF_OUT_OF_MEM,
    SDF_NOT_IN_TRANS,
    SDF_TRANS_ABORTED,
    SDF_FAILURE_MSG_SEND,
    SDF_ENUMERATION_END,
    SDF_BAD_KEY,
    SDF_FAILURE_CONTAINER_OPEN,
    SDF_BAD_PEXPTIME_POINTER,
    SDF_BAD_PSTAT_POINTER,
    SDF_BAD_PPCBUF_POINTER,
    SDF_BAD_SIZE_POINTER,
    SDF_EXPIRED,
    SDF_FLASH_EPERM,
    SDF_FLASH_ENOENT,
    SDF_FLASH_EIO,
    SDF_FLASH_EAGAIN,
    SDF_FLASH_ENOMEM,
    SDF_FLASH_EBUSY,
    SDF_FLASH_EEXIST,
    SDF_FLASH_EACCES,
    SDF_FLASH_EINVAL,
    SDF_FLASH_EMFILE,
    SDF_FLASH_ENOSPC,
    SDF_FLASH_ENOBUFS,
    SDF_FLASH_EDQUOT,
    SDF_STALE_LTIME,
    SDF_WRONG_NODE,
    SDF_UNAVAILABLE,
    SDF_TEST_FAIL,
    SDF_META_DATA_VERSION_TOO_NEW,
    SDF_META_DATA_INVALID
} status_id;
static char *status_str[SDF_META_DATA_INVALID] = {
    "SDF_SUCCESS",
    "SDF_FAILURE",
    "SDF_FAILURE_GENERIC",
    "SDF_FAILURE_CONTAINER_GENERIC",
    "SDF_FAILURE_INVALID_CONTAINER_TYPE",
    "SDF_INVALID_PARAMETER",
    "SDF_CONTAINER_UNKNOWN",
    "SDF_CONTAINER_EXISTS",
    "SDF_SHARD_NOT_FOUND",
    "SDF_OBJECT_UNKNOWN",
    "SDF_OBJECT_EXISTS",
    "SDF_FAILURE_STORAGE_READ",
    "SDF_FAILURE_STORAGE_WRITE",
    "SDF_FAILURE_MEMORY_ALLOC",
    "SDF_LOCK_INVALID_OP",
    "SDF_ALREADY_UNLOCKED",
    "SDF_ALREADY_READ_LOCKED",
    "SDF_ALREADY_WRITE_LOCKED",
    "SDF_OBJECT_NOT_CACHED",
    "SDF_SM_WAITING",
    "SDF_TOO_MANY_OPIDS",
    "SDF_TRANS_CONFLICT",
    "SDF_PIN_CONFLICT",
    "SDF_OBJECT_DELETED",
    "SDF_TRANS_NONTRANS_CONFLICT",
    "SDF_ALREADY_READ_PINNED",
    "SDF_ALREADY_WRITE_PINNED",
    "SDF_TRANS_PIN_CONFLICT",
    "SDF_PIN_NONPINNED_CONFLICT",
    "SDF_TRANS_FLUSH",
    "SDF_TRANS_LOCK",
    "SDF_TRANS_UNLOCK",
    "SDF_UNSUPPORTED_REQUEST",
    "SDF_UNKNOWN_REQUEST",
    "SDF_BAD_PBUF_POINTER",
    "SDF_BAD_PDATA_POINTER",
    "SDF_BAD_SUCCESS_POINTER",
    "SDF_NOT_PINNED",
    "SDF_NOT_READ_LOCKED",
    "SDF_NOT_WRITE_LOCKED",
    "SDF_PIN_FLUSH",
    "SDF_BAD_CONTEXT",
    "SDF_IN_TRANS",
    "SDF_NONCACHEABLE_CONTAINER",
    "SDF_OUT_OF_CONTEXTS",
    "SDF_INVALID_RANGE",
    "SDF_OUT_OF_MEM",
    "SDF_NOT_IN_TRANS",
    "SDF_TRANS_ABORTED",
    "SDF_FAILURE_MSG_SEND",
    "SDF_ENUMERATION_END",
    "SDF_BAD_KEY",
    "SDF_FAILURE_CONTAINER_OPEN",
    "SDF_BAD_PEXPTIME_POINTER",
    "SDF_BAD_PSTAT_POINTER",
    "SDF_BAD_PPCBUF_POINTER",
    "SDF_BAD_SIZE_POINTER",
    "SDF_EXPIRED",
    "SDF_FLASH_EPERM",
    "SDF_FLASH_ENOENT",
    "SDF_FLASH_EIO",
    "SDF_FLASH_EAGAIN",
    "SDF_FLASH_ENOMEM",
    "SDF_FLASH_EBUSY",
    "SDF_FLASH_EEXIST",
    "SDF_FLASH_EACCES",
    "SDF_FLASH_EINVAL",
    "SDF_FLASH_EMFILE",
    "SDF_FLASH_ENOSPC",
    "SDF_FLASH_ENOBUFS",
    "SDF_FLASH_EDQUOT",
    "SDF_STALE_LTIME",
    "SDF_WRONG_NODE",
    "SDF_UNAVAILABLE",
    "SDF_TEST_FAIL",
    "SDF_META_DATA_VERSION_TOO_NEW",
    "SDF_META_DATA_INVALID"
};
static char *store_str[5] = {
    "DO_STORE_FAILED",
    "DO_STORE_STORED",
    "DO_STORE_EXISTS",
    "DO_STORE_NOT_FOUND",
    "DO_STORE_NOT_STORED"
};
typedef struct cmd_item
{
    uint8_t cmd;
    uint8_t no_reply;
    uint8_t multi_key;
    uint8_t code;
    struct cmd_item *next;
} cmd_item_t;
typedef struct key_state
{
    unsigned long long key_syndrome;
    unsigned long long request_number;
    struct cmd_item *cmd_head;
    struct cmd_item *cmd_tail;
    struct key_state *next;
} key_state_t;

typedef struct interval_state
{
    unsigned long long interval;
    unsigned long long request_number;
    struct interval_state *next;
} interval_state_t;

typedef struct connection_state
{
    uint32_t connection;
    unsigned long long request_number;
    struct interval_state *interval_counter[MAX_INTERVAL];
    struct connection_state *next;
} connection_state_t;

#define Get_Hash_Value(type,name,k,ktype,max,counter) \
type *get_##name##_state(type *array[],ktype k)\
{\
    ktype index=k%max;\
    type *key=array[index];\
    type *end=NULL;\
    while(key)\
    {\
        if(key->k==k)\
                return key;\
        end=key;\
        key=key->next;\
    }\
    key=plat_alloc(sizeof(type));\
    memset(key,0,sizeof(type));\
    key->k=k;\
    key->next=NULL;\
    if(end==NULL)\
    {\
        array[index]=key;\
    }\
    else\
    {\
        end->next=key;\
    }\
    counter++;\
    return key;\
}
#endif /* ndef UTIL_TRACE_VIEWER_H */
