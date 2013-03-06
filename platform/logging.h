#ifndef PLATFORM_LOGGING_H
#define PLATFORM_LOGGING_H 1

#include <string.h>

/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/logging.h $
 * Author: drew
 *
 * Created on January 25, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: logging.h 15229 2010-12-09 22:53:51Z briano $
 */

/**
 * Logging subsystem intended to provide high-performance logging with
 * binary encoding for space and time efficiency with storage of fine-grained
 * logs in a circular buffer that gets flushed when an error is encountered.
 *
 * The design goals are
 * 1.  Run time cost below the measurement noise floor when nearly all
 *     development debugging log messages are included in the production
 *     binary.  Interesting problems often only manifest after substantial
 *     uptime examining the state of a customer's running system is
 *     often the only way to diagnose problems.
 *
 * 2.  Measurable but trivial cost to send most diagnostic messages to a
 *     circular buffer which is dumped to persistant storage when an error
 *     is encountered.  Most interesting failures are preceeded by a causal
 *     chain of events which cannot be derived from the current system
 *     state.  This is especially true in distributed systems where
 *     the failures result from what has already happened on other nodes.
 *     Dumping most trace messages to local circular buffers which
 *     are forwarded to a central server on any potentially related
 *     failure are a powerful debugging tool.
 *
 * Where implementation departs from theory, one can
 * add -DPLAT_LOG_COMPILE_LEVEL=caps_level to CPPFLAGS to limit output
 * generated at compile time to >= level.  The default is as if
 * -DPLAT_LOG_COMPILE_LEVEL=TRACE were specified, thus eliminating
 * the most expensive DEVEL category messages.
 *
 * Adding -DPLAT_LOG_RUNTIME_CATEGORIES to CPPFLAGS will enable
 * runtime specification of new log categories.  This is disabled for
 * production builds since static log category numbers make customer
 * reports consistent accross versions and therefore present a lesser
 * support load.
 */

#include "platform/assert.h"
#include "platform/defs.h"
#include "platform/types.h"
#include "platform/ffdc_log.h"

#if defined(PLATFORM_LOGGING_C) && defined(PLAT_NEED_OUT_OF_LINE)
#define PLAT_LOGGING_C_INLINE
#else
#define PLAT_LOGGING_C_INLINE PLAT_INLINE
#endif

__BEGIN_DECLS


#define PLAT_LOG_ID_INITIAL 0
#define LOGID PLAT_LOG_ID_INITIAL

/**
 * @brief Log levels
 *
 * item(caps, lower, syslog_prio)
 */
#define PLAT_LOG_LEVEL_ITEMS()                                                 \
    /* Fine-grained development logging.  Example: priq heap manipulation */   \
    /* Likely to be omitted at compile time */                                 \
    item(DEVEL, devel, 10, LOG_DEBUG)                                          \
    /* Fine-grained debugging activity with high cost. */                      \
    /* Likely to default to  disabled for binary logging but can be enabled */ \
    item(TRACE_LOW, trace_low, 20, LOG_DEBUG)                                  \
    /* Fine-grained debugging activity.  Example: logs for each RPC */         \
    item(TRACE, trace, 30, LOG_DEBUG)                                          \
    /* Summary messages for debugging. */                                      \
    item(DEBUG, debug, 40, LOG_DEBUG)                                          \
    /* More than DEBUG but may not be user visible.  Example: socket EOF  */   \
    item(DIAGNOSTIC, diagnostic, 50, LOG_INFO)                                 \
    /* Significant informational messages.  Example: recovery complete */      \
    item(INFO, info, 60, LOG_NOTICE)                                           \
    /* Abnormal conditions that bear monitoring.  Example: ECC correction */   \
    item(WARN, warning, 70, LOG_WARNING)                                       \
    /* Erors which are reported to the user */                                 \
    item(ERROR, error, 80, LOG_ERR)                                            \
    /* Errors which terminate a component */                                   \
    item(FATAL, fatal, 90, LOG_CRIT)

#define MAKE_PLAT_LOG_LEVEL_INTERNAL(caps) PLAT_LOG_LEVEL_ ## caps
#define MAKE_PLAT_LOG_LEVEL(caps) MAKE_PLAT_LOG_LEVEL_INTERNAL(caps)

#ifndef PLAT_LOG_COMPILE_LEVEL
#define PLAT_LOG_COMPILE_LEVEL TRACE_LOW
#endif

#ifndef PLAT_LOG_LAX
/**
 * @brief Configure log level at which we trade performance for lost messages
 *
 * At and beyond this level changes take effect at the next memory barrier
 * such as a global function call.
 *
 * Below this level determination is treated as a constant for optimization
 * purposes.  Thread main loops should probably set this to TRACE.
 *
 * This can be changed throughout the code
 * @code
 * #undef PLAT_LOG_PARANOID_LEVEL
 * #define PLAT_LOG_PARANOID_LEVEL TRACE
 *
 * // main loop function here
 *
 * #undef PLAT_LOG_PARANOID_LEVEL
 * #define PLAT_LOG_PARANOID_LEVEL PLAT_LOG_DEFAULT_PARANOID_LEVEL
 */
#define PLAT_LOG_PARANOID_LEVEL PLAT_LOG_DEFAULT_PARANOID_LEVEL

#define PLAT_LOG_DEFAULT_PARANOID_LEVEL TRACE_LOW
#endif

#define PLAT_LOG_PURE_LEVEL MAKE_PLAT_LOG_LEVEL(PLAT_LOG_PARANOID_LEVEL)

/*
 * Make it impossible to turn off trace level logging at compile time
 * because no trace level logging makes debugging impossible
 */
#define PLAT_LOG_CHECK_LEVEL (MAKE_PLAT_LOG_LEVEL(PLAT_LOG_COMPILE_LEVEL) >=   \
                              PLAT_LOG_LEVEL_TRACE_LOW ?                       \
                              PLAT_LOG_LEVEL_TRACE_LOW :                       \
                              MAKE_PLAT_LOG_LEVEL(PLAT_LOG_COMPILE_LEVEL))

enum plat_log_level {
#define item(caps, lower, value, syslog_prio) PLAT_LOG_LEVEL_ ## caps,
    PLAT_LOG_LEVEL_ITEMS()
#undef item

    /* Out-of-band values */
    PLAT_LOG_LEVEL_INVALID = -1,
    PLAT_LOG_LEVEL_DEFAULT = -2
};

/** @brief Other logging constants */
enum {
    /**
     * @brief maximum category
     *
     * Meeting design goal 1 means we want zero cost access to the
     * set of log levels, which in turn means no locking and no
     * stores to get access thus suggesting a shared array with
     * fixed maximum size.
     */
    PLAT_LOG_MAX_CATEGORY = 1024
};

/** @brief What to put in the log file along with plat_log messages */
enum plat_log_redirect {
    PLAT_LOG_REDIRECT_NONE = 0,
    PLAT_LOG_REDIRECT_STDOUT = 1 << 1,
    PLAT_LOG_REDIRECT_STDERR = 1 << 2,
    PLAT_LOG_REDIRECT_BOTH = PLAT_LOG_REDIRECT_STDOUT|PLAT_LOG_REDIRECT_STDERR
};

extern enum plat_log_level
plat_log_level_immediate_cache[PLAT_LOG_MAX_CATEGORY];

/*
 * FIXME: When each logging client specifies its local mapping between
 * text and numeric categories during logging initialization the
 * explicit mapping is not necessary and we can do interesting
 * programmatically generated things (ex: add platform/shmem/reference/type)
 */

/**
 * Complete enumeration of logging categories in the form
 * enum suffix, lower case name, and numeric value.  Hierchies may
 * be formed with slash deliminters in the lower case name ex.
 *
 * shmem
 * shmem/shmemd
 *
 * Categories have explicitly assigned numbers because we need to
 * filter on log-dump based on category and this means that the code generating
 * and processing messages acts the same.  We could also do this by associating
 * a category with a log message and just generating a new log message when
 * that gets updated.
 *
 * Next number: 300
 *
 * XXX: drew 2009-12-07 So that customer support phone sessions go more
 * smoothly, we'd like the dynamically assigned numeric categories from
 * PLAT_LOG_CAT_LOCAL/PLAT_LOG_SUBCAT_LOCAL and from super-categories
 * which were not explicitly specified to be consistent accross builds.
 *
 * It's a lower schedule risk for v1 to just deal with things here for the
 * first release than to change the ffdc script to maintain a static set.
 * Revisit that the issue after v1.
 *
 * For now PLAT_LOG_CATEGORY_AUTO_ITEMS() has everything which would be
 * added automatically
 */
#define PLAT_LOG_CATEGORY_ITEMS() \
    PLAT_LOG_CATEGORY_NORMAL_ITEMS() \
    PLAT_LOG_CATEGORY_AUTO_ITEMS() \
    PLAT_LOG_CATEGORY_DLL_ITEMS()

#define PLAT_LOG_CATEGORY_DLL_ITEMS() \
    item(PLATFORM_ALLOC_MPROBE, "platform/alloc/mprobe", 298) \
    item(PLATFORM_FD_DISPATCHER, "platform/fd_dispatcher", 299)

#define PLAT_LOG_CATEGORY_NORMAL_ITEMS() \
    /* For setting default=priority; users should log to something real */ \
    item(DEFAULT, "default", 7) \
    item(FLASH, "flash", 30) \
    item(FTH, "fth", 29) \
    item(FTH_IDLE, "fth/idle", 34) \
    item(FTH_TEST, "fth/test", 36) \
    item(PLATFORM, "platform", 0) \
    item(PLATFORM_AIO, "platform/aio", 287) \
    item(PLATFORM_AIO_ERROR_BDB, "platform/aio/error_bdb", 297) \
    item(PLATFORM_AIO_LIBAIO, "platform/aio/libaio", 288) \
    item(PLATFORM_AIO_WC, "platform/aio/wc", 289) \
    item(PLATFORM_ALLOC_COREDUMP_FILTER, "platform/alloc/coredump_filter", 46) \
    item(PLATFORM_ALLOC, "platform/alloc", 18) \
    item(PLATFORM_ALLOC_MALLOC, "platform/alloc/malloc", 31) \
    item(PLATFORM_ALLOC_FREE, "platform/alloc/free", 32) \
    item(PLATFORM_CLOSURE, "platform/closure", 25) \
    item(PLATFORM_EVENT, "platform/event", 17) \
    item(PLATFORM_FORK, "platform/fork", 15) \
    item(PLATFORM_MISC, "platform/misc", 33) \
    item(PLATFORM_MSG, "platform/msg", 2) \
    item(PLATFORM_SHMEM, "platform/shmem", 1) \
    item(PLATFORM_SHMEMD, "platform/shmem/shmemd", 5) \
    item(PLATFORM_SHMEM_ALLOC, "platform/shmem/alloc", 8) \
    item(PLATFORM_SHMEM_REFERENCE, "platform/shmem/reference", 9) \
    item(PLATFORM_TEST, "platform/test", 3) \
    item(PLATFORM_TEST_AIO, "platform/test/aio", 286) \
    item(PLATFORM_TEST_SHMEM, "platform/test/shmem", 10) \
    item(PLATFORM_MEM_DEBUG, "platform/mem_debug", 20) \
    item(PLATFORM_TEST_MEM_DEBUG, "platform/test/mem_debug", 6) \
    item(PRINT_ARGS, "print_args", 35) \
    item(SDF, "sdf", 39) \
    item(SDF_AGENT, "sdf/agent", 16) \
    item(SDF_APP_MEMCACHED, "apps/membrain/server", 26) \
    item(SDF_APP_MEMCACHED_TRACE, "apps/membrain/server/trace", 28) \
    item(SDF_APP_MEMCACHED_RECOVERY, "apps/membrain/server/recovery", 37) \
    item(SDF_APP_MEMCACHED_RECOVERY_LOG, \
         "apps/membrain/server/recovery_log", 293)  \
    item(SDF_APP_MEMCACHED_BACKUP, \
         "apps/membrain/server/backup", 295)  \
    item(SDF_APP_SDFUSE, "apps/sdfuse", 27) \
    item(SDF_CC, "sdf/cc", 11) \
    item(SDF_CMC, "sdf/cmc", 13) \
    item(SDF_CLIENT, "sdf/client", 4) \
    item(SDF_NAMING, "sdf/naming", 12) \
    item(SDF_PROT, "sdf/prot", 19) \
    item(SDF_SDFMSG, "sdf/sdfmsg", 24) \
    item(SDF_SHARED, "sdf/shared", 14) \
    item(SDF_SIMPLE_REPLICATION, "sdf/simple_replication", 40) \
    item(SDF_PROT_CONSISTENCY, "sdf/prot/consistency", 21) \
    item(SDF_PROT_FLASH, "sdf/prot/flash", 23) \
    item(SDF_PROT_REPLICATION, "sdf/prot/replication", 22) \
    item(PLATFORM_LOGGING, "platform/logging", 38) \
    item(SDF_PROT_REPLICATION_VIPS, "sdf/prot/replication/vips", 282)

#define PLAT_LOG_CATEGORY_AUTO_ITEMS() \
    item(APPS, \
         "apps", 41) \
    item(APPS_MEMCACHED, \
         "apps/membrain", 42) \
    item(APPS_MEMCACHED_SERVER_HOTKEY, \
         "apps/membrain/server/hotkey", 43) \
    item(FTH_FTHIDLETEST, \
         "fth/fthIdleTest", 44) \
    item(FTH_TEST_SIGNAL, \
         "fth/test/signal", 45) \
    item(PLATFORM_ALLOC_MEMORY_FAULT, \
         "platform/alloc/memory_fault", 47) \
    item(PLATFORM_ALLOC_MEMORY_SIZE, \
         "platform/alloc/memory_size", 48) \
    item(PLATFORM_ALLOC_STACK, \
         "platform/alloc/stack", 49) \
    item(PLATFORM_CLOSURE_ASYNC_PUTS_SHUTDOWN, \
         "platform/closure/async_puts_shutdown", 50) \
    item(PLATFORM_CLOSURE_CLOSURE_INT, \
         "platform/closure/closure_int", 51) \
    item(PLATFORM_CLOSURE_CLOSURE_VOID, \
         "platform/closure/closure_void", 52) \
    item(PLATFORM_CLOSURE_CR_CREATE_META_SHARD_CB, \
         "platform/closure/cr_create_meta_shard_cb", 53) \
    item(PLATFORM_CLOSURE_CR_DO_COMMAND_ASYNC, \
         "platform/closure/cr_do_command_async", 54) \
    item(PLATFORM_CLOSURE_CR_DO_GET_CONTAINER_STATS, \
         "platform/closure/cr_do_get_container_stats", 55) \
    item(PLATFORM_CLOSURE_CR_SHARD_NOTIFY_CB, \
         "platform/closure/cr_shard_notify_cb", 57) \
    item(PLATFORM_CLOSURE_CR_SHARD_PUT_META_CB, \
         "platform/closure/cr_shard_put_meta_cb", 58) \
    item(PLATFORM_CLOSURE_CR_SHARD_REPLICA_STATE, \
         "platform/closure/cr_shard_replica_state", 59) \
    item(PLATFORM_CLOSURE_FTH_SCHEDULER, \
         "platform/closure/fth_scheduler", 60) \
    item(PLATFORM_CLOSURE_HOME_FLASH_SHUTDOWN, \
         "platform/closure/home_flash_shutdown", 61) \
    item(PLATFORM_CLOSURE_PLAT_CLOSURE_SCHEDULER_SHUTDOWN, \
         "platform/closure/plat_closure_scheduler_shutdown", 62) \
    item(PLATFORM_CLOSURE_PLAT_EVENT_FIRED, \
         "platform/closure/plat_event_fired", 63) \
    item(PLATFORM_CLOSURE_PLAT_EVENT_FREE_DONE, \
         "platform/closure/plat_event_free_done", 64) \
    item(PLATFORM_CLOSURE_PLAT_EVENT_IMPL_CLOSURE, \
         "platform/closure/plat_event_impl_closure", 65) \
    item(PLATFORM_CLOSURE_PLAT_EVENT_VOID_CLOSURE, \
         "platform/closure/plat_event_void_closure", 66) \
    item(PLATFORM_CLOSURE_PLAT_MSG_FREE, \
         "platform/closure/plat_msg_free", 67) \
    item(PLATFORM_CLOSURE_PLAT_MSG_RECV, \
         "platform/closure/plat_msg_recv", 68) \
    item(PLATFORM_CLOSURE_PLAT_TIMER_DISPATCHER_GETTIME, \
         "platform/closure/plat_timer_dispatcher_gettime", 69) \
    item(PLATFORM_CLOSURE_RKL_CB, \
         "platform/closure/rkl_cb", 56) \
    item(PLATFORM_CLOSURE_RKLC_RECOVERY_CB, \
         "platform/closure/rklc_recovery_cb", 284) \
    item(PLATFORM_CLOSURE_REPLICATION_TEST_FLASH_CRASH_ASYNC_CB, \
         "platform/closure/replication_test_flash_crash_async_cb", 70) \
    item(PLATFORM_CLOSURE_REPLICATION_TEST_FLASH_SHUTDOWN_ASYNC_CB, \
         "platform/closure/replication_test_flash_shutdown_async_cb", 71) \
    item(PLATFORM_CLOSURE_REPLICATION_TEST_FRAMEWORK_CB, \
         "platform/closure/replication_test_framework_cb", 72) \
    item(PLATFORM_CLOSURE_REPLICATION_TEST_FRAMEWORK_READ_ASYNC_CB, \
         "platform/closure/replication_test_framework_read_async_cb", 73) \
    item(PLATFORM_CLOSURE_REPLICATION_TEST_FRAMEWORK_READ_DATA_FREE_CB, \
         "platform/closure/replication_test_framework_read_data_free_cb", 74) \
    item(PLATFORM_CLOSURE_REPLICATION_TEST_FRAMEWORK_SHUTDOWN_ASYNC_CB, \
         "platform/closure/replication_test_framework_shutdown_async_cb", 75) \
    item(PLATFORM_CLOSURE_REPLICATION_TEST_NODE_CRASH_ASYNC_CB, \
         "platform/closure/replication_test_node_crash_async_cb", 76) \
    item(PLATFORM_CLOSURE_REPLICATION_TEST_NODE_SHUTDOWN_ASYNC_CB, \
         "platform/closure/replication_test_node_shutdown_async_cb", 77) \
    item(PLATFORM_CLOSURE_RMS_DELETE_SHARD_META_CB, \
         "platform/closure/rms_delete_shard_meta_cb", 78) \
    item(PLATFORM_CLOSURE_RMS_SHARD_META_CB, \
         "platform/closure/rms_shard_meta_cb", 79) \
    item(PLATFORM_CLOSURE_RMS_SHUTDOWN_CB, \
         "platform/closure/rms_shutdown_cb", 80) \
    item(PLATFORM_CLOSURE_RR_GET_BY_CURSOR_CB, \
         "platform/closure/rr_get_by_cursor_cb", 81) \
    item(PLATFORM_CLOSURE_RR_GET_ITERATION_CURSORS_CB, \
         "platform/closure/rr_get_iteration_cursors_cb", 82) \
    item(PLATFORM_CLOSURE_RR_GET_MSG_BY_CURSOR_CB, \
         "platform/closure/rr_get_msg_by_cursor_cb", 83) \
    item(PLATFORM_CLOSURE_RR_GET_SEQNO_CB, \
         "platform/closure/rr_get_seqno_cb", 84) \
    item(PLATFORM_CLOSURE_RR_LAST_SEQNO_CB, \
         "platform/closure/rr_last_seqno_cb", 85) \
    item(PLATFORM_CLOSURE_RR_READ_DATA_FREE_CB, \
         "platform/closure/rr_read_data_free_cb", 86) \
    item(PLATFORM_CLOSURE_RTF_DO_CRASH, \
         "platform/closure/rtf_do_crash", 87) \
    item(PLATFORM_CLOSURE_RTF_DO_RECEIVE_MSG, \
         "platform/closure/rtf_do_receive_msg", 88) \
    item(PLATFORM_CLOSURE_RTF_DO_SEND_MSG, \
         "platform/closure/rtf_do_send_msg", 89) \
    item(PLATFORM_CLOSURE_RTF_DO_SHUTDOWN, \
         "platform/closure/rtf_do_shutdown", 90) \
    item(PLATFORM_CLOSURE_RTF_DO_START, \
         "platform/closure/rtf_do_start", 91) \
    item(PLATFORM_CLOSURE_RTFW_GET_BY_CURSOR_CB, \
         "platform/closure/rtfw_get_by_cursor_cb", 92) \
    item(PLATFORM_CLOSURE_RTFW_GET_CURSORS_CB, \
         "platform/closure/rtfw_get_cursors_cb", 93) \
    item(PLATFORM_CLOSURE_RTFW_LAST_SEQNO_CB, \
         "platform/closure/rtfw_last_seqno_cb", 94) \
    item(PLATFORM_CLOSURE_RTFW_REPLICATOR_NOTIFICATION_CB, \
         "platform/closure/rtfw_replicator_notification_cb", 95) \
    item(PLATFORM_CLOSURE_RTFW_SHARD_META_CB, \
         "platform/closure/rtfw_shard_meta_cb", 96) \
    item(PLATFORM_CLOSURE_RTFW_VOID_CB, \
         "platform/closure/rtfw_void_cb", 290) \
    item(PLATFORM_CLOSURE_RTN_DO_COMMAND, \
         "platform/closure/rtn_do_command", 97) \
    item(PLATFORM_CLOSURE_RTN_DO_CRASH, \
         "platform/closure/rtn_do_crash", 98) \
    item(PLATFORM_CLOSURE_RTN_DO_DELETE_SHARD_META, \
         "platform/closure/rtn_do_delete_shard_meta", 99) \
    item(PLATFORM_CLOSURE_RTN_DO_GET_SHARD_META, \
         "platform/closure/rtn_do_get_shard_meta", 100) \
    item(PLATFORM_CLOSURE_RTN_DO_LIVENESS, \
         "platform/closure/rtn_do_liveness", 101) \
    item(PLATFORM_CLOSURE_RTN_DO_NODE_LIVENESS, \
         "platform/closure/rtn_do_node_liveness", 292) \
    item(PLATFORM_CLOSURE_RTN_DO_PUT_SHARD_META, \
         "platform/closure/rtn_do_put_shard_meta", 102) \
    item(PLATFORM_CLOSURE_RTN_DO_RECEIVE_MSG, \
         "platform/closure/rtn_do_receive_msg", 103) \
    item(PLATFORM_CLOSURE_RTN_DO_SEND_MSG, \
         "platform/closure/rtn_do_send_msg", 104) \
    item(PLATFORM_CLOSURE_RTN_DO_SHUTDOWN, \
         "platform/closure/rtn_do_shutdown", 105) \
    item(PLATFORM_CLOSURE_RTN_DO_START, \
         "platform/closure/rtn_do_start", 106) \
    item(PLATFORM_CLOSURE_RTN_DO_VOID_CB, \
         "platform/closure/rtn_do_void_cb", 291) \
    item(PLATFORM_CLOSURE_SDF_MSG_RECV_WRAPPER, \
         "platform/closure/sdf_msg_recv_wrapper", 107) \
    item(PLATFORM_CLOSURE_SDF_MSG_WRAPPER_FREE_LOCAL, \
         "platform/closure/sdf_msg_wrapper_free_local", 108) \
    item(PLATFORM_CLOSURE_SDF_MSG_WRAPPER_FREE_SHARED, \
         "platform/closure/sdf_msg_wrapper_free_shared", 109) \
    item(PLATFORM_CLOSURE_SDF_REPLICATOR_ADAPTER_SHUTDOWN, \
         "platform/closure/sdf_replicator_adapter_shutdown", 110) \
    item(PLATFORM_CLOSURE_SDF_REPLICATOR_COMMAND_CB, \
         "platform/closure/sdf_replicator_command_cb", 111) \
    item(PLATFORM_CLOSURE_SDF_REPLICATOR_NODE_DEAD, \
         "platform/closure/sdf_replicator_node_dead", 112) \
    item(PLATFORM_CLOSURE_SDF_REPLICATOR_NODE_LIVE, \
         "platform/closure/sdf_replicator_node_live", 113) \
    item(PLATFORM_CLOSURE_SDF_REPLICATOR_NOTIFICATION_CB, \
         "platform/closure/sdf_replicator_notification_cb", 114) \
    item(PLATFORM_CLOSURE_SDF_REPLICATOR_NOTIFICATION_COMPLETE_CB, \
         "platform/closure/sdf_replicator_notification_complete_cb", 115) \
    item(PLATFORM_CLOSURE_SDF_REPLICATOR_REGISTER_RECV_CB, \
         "platform/closure/sdf_replicator_register_recv_cb", 116) \
    item(PLATFORM_CLOSURE_SDF_REPLICATOR_SEND_MSG_CB, \
         "platform/closure/sdf_replicator_send_msg_cb", 117) \
    item(PLATFORM_CLOSURE_SDF_REPLICATOR_SHUTDOWN, \
         "platform/closure/sdf_replicator_shutdown", 118) \
    item(PLATFORM_CLOSURE_SDF_REPLICATOR_SHUTDOWN_CB, \
         "platform/closure/sdf_replicator_shutdown_cb", 119) \
    item(PLATFORM_CLOSURE_TEST_CLOSURE, \
         "platform/closure/test_closure", 120) \
    item(PLATFORM_CPU_PEERS, \
         "platform/cpu_peers", 121) \
    item(PLATFORM_SHMEM_ALLOC_CACHE, \
         "platform/shmem/alloc/cache", 122) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_ALLOCATED_BYTES, \
         "platform/shmem/alloc/cache/allocated_bytes", 123) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_ALLOCATED_COUNT, \
         "platform/shmem/alloc/cache/allocated_count", 124) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_DIRENTRY, \
         "platform/shmem/alloc/cache_direntry", 125) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_DIRENTRY_ALLOCATED_BYTES, \
         "platform/shmem/alloc/cache_direntry/allocated_bytes", 126) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_DIRENTRY_ALLOCATED_COUNT, \
         "platform/shmem/alloc/cache_direntry/allocated_count", 127) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_DIRENTRY_TOTAL_ALLOC, \
         "platform/shmem/alloc/cache_direntry/total_alloc", 128) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_DIRENTRY_TOTAL_FREE, \
         "platform/shmem/alloc/cache_direntry/total_free", 129) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_DIRENTRY_TREE_SIZE, \
         "platform/shmem/alloc/cache_direntry/tree_size", 130) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_DIRENTRY_USED_BYTES, \
         "platform/shmem/alloc/cache_direntry/used_bytes", 131) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_READ_BUF, \
         "platform/shmem/alloc/cache_read_buf", 132) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_READ_BUF_ALLOCATED_BYTES, \
         "platform/shmem/alloc/cache_read_buf/allocated_bytes", 133) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_READ_BUF_ALLOCATED_COUNT, \
         "platform/shmem/alloc/cache_read_buf/allocated_count", 134) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_READ_BUF_TOTAL_ALLOC, \
         "platform/shmem/alloc/cache_read_buf/total_alloc", 135) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_READ_BUF_TOTAL_FREE, \
         "platform/shmem/alloc/cache_read_buf/total_free", 136) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_READ_BUF_TREE_SIZE, \
         "platform/shmem/alloc/cache_read_buf/tree_size", 137) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_READ_BUF_USED_BYTES, \
         "platform/shmem/alloc/cache_read_buf/used_bytes", 138) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_TOTAL_ALLOC, \
         "platform/shmem/alloc/cache/total_alloc", 139) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_TOTAL_FREE, \
         "platform/shmem/alloc/cache/total_free", 140) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_TREE_SIZE, \
         "platform/shmem/alloc/cache/tree_size", 141) \
    item(PLATFORM_SHMEM_ALLOC_CACHE_USED_BYTES, \
         "platform/shmem/alloc/cache/used_bytes", 142) \
    item(PLATFORM_SHMEM_ALLOC_CHAR_SP, \
         "platform/shmem/alloc/char_sp", 143) \
    item(PLATFORM_SHMEM_ALLOC_CONTAINERMAPBUCKET_SP, \
         "platform/shmem/alloc/ContainerMapBucket_sp", 144) \
    item(PLATFORM_SHMEM_ALLOC_CONTAINERMAP_SP, \
         "platform/shmem/alloc/ContainerMap_sp", 145) \
    item(PLATFORM_SHMEM_ALLOC_DESCRCHANGESPTR_SP, \
         "platform/shmem/alloc/DescrChangesPtr_sp", 146) \
    item(PLATFORM_SHMEM_ALLOC_ENTRY_SP, \
         "platform/shmem/alloc/entry_sp", 147) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_CHUNK, \
         "platform/shmem/alloc/flash_chunk", 148) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_CHUNK_ALLOCATED_BYTES, \
         "platform/shmem/alloc/flash_chunk/allocated_bytes", 149) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_CHUNK_ALLOCATED_COUNT, \
         "platform/shmem/alloc/flash_chunk/allocated_count", 150) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_CHUNK_TOTAL_ALLOC, \
         "platform/shmem/alloc/flash_chunk/total_alloc", 151) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_CHUNK_TOTAL_FREE, \
         "platform/shmem/alloc/flash_chunk/total_free", 152) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_CHUNK_TREE_SIZE, \
         "platform/shmem/alloc/flash_chunk/tree_size", 153) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_CHUNK_USED_BYTES, \
         "platform/shmem/alloc/flash_chunk/used_bytes", 154) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_MISC, \
         "platform/shmem/alloc/flash_misc", 155) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_MISC_ALLOCATED_BYTES, \
         "platform/shmem/alloc/flash_misc/allocated_bytes", 156) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_MISC_ALLOCATED_COUNT, \
         "platform/shmem/alloc/flash_misc/allocated_count", 157) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_MISC_TOTAL_ALLOC, \
         "platform/shmem/alloc/flash_misc/total_alloc", 158) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_MISC_TOTAL_FREE, \
         "platform/shmem/alloc/flash_misc/total_free", 159) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_MISC_TREE_SIZE, \
         "platform/shmem/alloc/flash_misc/tree_size", 160) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_MISC_USED_BYTES, \
         "platform/shmem/alloc/flash_misc/used_bytes", 161) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_OBJ, \
         "platform/shmem/alloc/flash_obj", 162) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_OBJ_ALLOCATED_BYTES, \
         "platform/shmem/alloc/flash_obj/allocated_bytes", 163) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_OBJ_ALLOCATED_COUNT, \
         "platform/shmem/alloc/flash_obj/allocated_count", 164) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_OBJ_TOTAL_ALLOC, \
         "platform/shmem/alloc/flash_obj/total_alloc", 165) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_OBJ_TOTAL_FREE, \
         "platform/shmem/alloc/flash_obj/total_free", 166) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_OBJ_TREE_SIZE, \
         "platform/shmem/alloc/flash_obj/tree_size", 167) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_OBJ_USED_BYTES, \
         "platform/shmem/alloc/flash_obj/used_bytes", 168) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_TEMP, \
         "platform/shmem/alloc/flash_temp", 169) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_TEMP_ALLOCATED_BYTES, \
         "platform/shmem/alloc/flash_temp/allocated_bytes", 170) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_TEMP_ALLOCATED_COUNT, \
         "platform/shmem/alloc/flash_temp/allocated_count", 171) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_TEMP_TOTAL_ALLOC, \
         "platform/shmem/alloc/flash_temp/total_alloc", 172) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_TEMP_TOTAL_FREE, \
         "platform/shmem/alloc/flash_temp/total_free", 173) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_TEMP_TREE_SIZE, \
         "platform/shmem/alloc/flash_temp/tree_size", 174) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_TEMP_USED_BYTES, \
         "platform/shmem/alloc/flash_temp/used_bytes", 175) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_USER, \
         "platform/shmem/alloc/flash_user", 176) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_USER_ALLOCATED_BYTES, \
         "platform/shmem/alloc/flash_user/allocated_bytes", 177) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_USER_ALLOCATED_COUNT, \
         "platform/shmem/alloc/flash_user/allocated_count", 178) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_USER_TOTAL_ALLOC, \
         "platform/shmem/alloc/flash_user/total_alloc", 179) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_USER_TOTAL_FREE, \
         "platform/shmem/alloc/flash_user/total_free", 180) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_USER_TREE_SIZE, \
         "platform/shmem/alloc/flash_user/tree_size", 181) \
    item(PLATFORM_SHMEM_ALLOC_FLASH_USER_USED_BYTES, \
         "platform/shmem/alloc/flash_user/used_bytes", 182) \
    item(PLATFORM_SHMEM_ALLOC_FTH, \
         "platform/shmem/alloc/fth", 183) \
    item(PLATFORM_SHMEM_ALLOC_FTH_ALLOCATED_BYTES, \
         "platform/shmem/alloc/fth/allocated_bytes", 184) \
    item(PLATFORM_SHMEM_ALLOC_FTH_ALLOCATED_COUNT, \
         "platform/shmem/alloc/fth/allocated_count", 185) \
    item(PLATFORM_SHMEM_ALLOC_FTHIDLECONTROL_SP, \
         "platform/shmem/alloc/fthIdleControl_sp", 186) \
    item(PLATFORM_SHMEM_ALLOC_FTH_TOTAL_ALLOC, \
         "platform/shmem/alloc/fth/total_alloc", 187) \
    item(PLATFORM_SHMEM_ALLOC_FTH_TOTAL_FREE, \
         "platform/shmem/alloc/fth/total_free", 188) \
    item(PLATFORM_SHMEM_ALLOC_FTH_TREE_SIZE, \
         "platform/shmem/alloc/fth/tree_size", 189) \
    item(PLATFORM_SHMEM_ALLOC_FTH_USED_BYTES, \
         "platform/shmem/alloc/fth/used_bytes", 190) \
    item(PLATFORM_SHMEM_ALLOC_FTOPMBOX_SP, \
         "platform/shmem/alloc/ftopMbox_sp", 191) \
    item(PLATFORM_SHMEM_ALLOC_HEAD_SP, \
         "platform/shmem/alloc/head_sp", 192) \
    item(PLATFORM_SHMEM_ALLOC_MAIL_SP, \
         "platform/shmem/alloc/mail_sp", 193) \
    item(PLATFORM_SHMEM_ALLOC_MCD_MAIL_SP, \
         "platform/shmem/alloc/mcd_mail_sp", 194) \
    item(PLATFORM_SHMEM_ALLOC_MY_VOID_SP, \
         "platform/shmem/alloc/my_void_sp", 195) \
    item(PLATFORM_SHMEM_ALLOC_NODE_SP, \
         "platform/shmem/alloc/node_sp", 196) \
    item(PLATFORM_SHMEM_ALLOC_PLAT_PROCESS_SP, \
         "platform/shmem/alloc/plat_process_sp", 197) \
    item(PLATFORM_SHMEM_ALLOC_PTOFMBOXPTRS_SP, \
         "platform/shmem/alloc/ptofMboxPtrs_sp", 198) \
    item(PLATFORM_SHMEM_ALLOC_PTOFMBOX_SP, \
         "platform/shmem/alloc/ptofMbox_sp", 199) \
    item(PLATFORM_SHMEM_ALLOC_RESIZE, \
         "platform/shmem/alloc/resize", 200) \
    item(PLATFORM_SHMEM_ALLOC_ROOT, \
         "platform/shmem/alloc/root", 201) \
    item(PLATFORM_SHMEM_ALLOC_ROOT_ALLOCATED_BYTES, \
         "platform/shmem/alloc/root/allocated_bytes", 202) \
    item(PLATFORM_SHMEM_ALLOC_ROOT_ALLOCATED_COUNT, \
         "platform/shmem/alloc/root/allocated_count", 203) \
    item(PLATFORM_SHMEM_ALLOC_ROOT_TOTAL_ALLOC, \
         "platform/shmem/alloc/root/total_alloc", 204) \
    item(PLATFORM_SHMEM_ALLOC_ROOT_TOTAL_FREE, \
         "platform/shmem/alloc/root/total_free", 205) \
    item(PLATFORM_SHMEM_ALLOC_ROOT_TREE_SIZE, \
         "platform/shmem/alloc/root/tree_size", 206) \
    item(PLATFORM_SHMEM_ALLOC_ROOT_USED_BYTES, \
         "platform/shmem/alloc/root/used_bytes", 207) \
    item(PLATFORM_SHMEM_ALLOC_SA_ARENA_SP, \
         "platform/shmem/alloc/sa_arena_sp", 208) \
    item(PLATFORM_SHMEM_ALLOC_SDFCACHEOBJ_SP, \
         "platform/shmem/alloc/SDFCacheObj_sp", 209) \
    item(PLATFORM_SHMEM_ALLOC__SDF_CONTAINER_PARENT_SP, \
         "platform/shmem/alloc/_SDF_container_parent_sp", 210) \
    item(PLATFORM_SHMEM_ALLOC__SDF_CONTAINER_SP, \
         "platform/shmem/alloc/_SDF_container_sp", 211) \
    item(PLATFORM_SHMEM_ALLOC_SDF_KEY_SP, \
         "platform/shmem/alloc/SDF_key_sp", 212) \
    item(PLATFORM_SHMEM_ALLOC_SDF_MSG_SP, \
         "platform/shmem/alloc/sdf_msg_sp", 213) \
    item(PLATFORM_SHMEM_ALLOC_SHMEM_ADMIN_SP, \
         "platform/shmem/alloc/shmem_admin_sp", 214) \
    item(PLATFORM_SHMEM_ALLOC_SHMEM_ALLOC_SP, \
         "platform/shmem/alloc/shmem_alloc_sp", 215) \
    item(PLATFORM_SHMEM_ALLOC_SHMEM_FREE_ENTRY_SP, \
         "platform/shmem/alloc/shmem_free_entry_sp", 216) \
    item(PLATFORM_SHMEM_ALLOC_SHMEM_FREE_LIST_SP, \
         "platform/shmem/alloc/shmem_free_list_sp", 217) \
    item(PLATFORM_SHMEM_ALLOC_SHMEM_HEADER_SP, \
         "platform/shmem/alloc/shmem_header_sp", 218) \
    item(PLATFORM_SHMEM_ALLOC_TEST, \
         "platform/shmem/alloc/test", 219) \
    item(PLATFORM_SHMEM_ALLOC_TEST_ALLOCATED_BYTES, \
         "platform/shmem/alloc/test/allocated_bytes", 220) \
    item(PLATFORM_SHMEM_ALLOC_TEST_ALLOCATED_COUNT, \
         "platform/shmem/alloc/test/allocated_count", 221) \
    item(PLATFORM_SHMEM_ALLOC_TEST_TOTAL_ALLOC, \
         "platform/shmem/alloc/test/total_alloc", 222) \
    item(PLATFORM_SHMEM_ALLOC_TEST_TOTAL_FREE, \
         "platform/shmem/alloc/test/total_free", 223) \
    item(PLATFORM_SHMEM_ALLOC_TEST_TREE_SIZE, \
         "platform/shmem/alloc/test/tree_size", 224) \
    item(PLATFORM_SHMEM_ALLOC_TEST_USED_BYTES, \
         "platform/shmem/alloc/test/used_bytes", 225) \
    item(PLATFORM_SHMEM_ALLOC_TOTAL, \
         "platform/shmem/alloc/total", 226) \
    item(PLATFORM_SHMEM_ALLOC_TOTAL_ALLOCATED_BYTES, \
         "platform/shmem/alloc/total/allocated_bytes", 227) \
    item(PLATFORM_SHMEM_ALLOC_TOTAL_ALLOCATED_COUNT, \
         "platform/shmem/alloc/total/allocated_count", 228) \
    item(PLATFORM_SHMEM_ALLOC_TOTAL_STOLEN_BYTES, \
         "platform/shmem/alloc/total/stolen_bytes", 296) \
    item(PLATFORM_SHMEM_ALLOC_TOTAL_TOTAL_ALLOC, \
         "platform/shmem/alloc/total/total_alloc", 229) \
    item(PLATFORM_SHMEM_ALLOC_TOTAL_TOTAL_BYTES, \
         "platform/shmem/alloc/total/total_bytes", 230) \
    item(PLATFORM_SHMEM_ALLOC_TOTAL_TOTAL_FREE, \
         "platform/shmem/alloc/total/total_free", 231) \
    item(PLATFORM_SHMEM_ALLOC_TOTAL_TREE_SIZE, \
         "platform/shmem/alloc/total/tree_size", 232) \
    item(PLATFORM_SHMEM_ALLOC_TOTAL_UNUSABLE_BYTES, \
         "platform/shmem/alloc/total/unusable_bytes", 233) \
    item(PLATFORM_SHMEM_ALLOC_TOTAL_USED_BYTES, \
         "platform/shmem/alloc/total/used_bytes", 234) \
    item(PLATFORM_SHMEM_ALLOC_XMBOXEL_SP, \
         "platform/shmem/alloc/XMboxEl_sp", 235) \
    item(PLATFORM_STRANGE, \
         "platform/strange", 236) \
    item(PLATFORM_STRANGE_BIZARRE, \
         "platform/strange/bizarre", 237) \
    item(PLATFORM_TEST_ALLOC_ARENATEST, \
         "platform/test/alloc_arenatest", 238) \
    item(PLATFORM_TEST_LOGTEST, \
         "platform/test/logtest", 239) \
    item(PLATFORM_TEST_MBOX_SCHEDULER, \
         "platform/test/mbox_scheduler", 240) \
    item(PLATFORM_TEST_SHMEMTEST_AOSET, \
         "platform/test/shmemtest_aoset", 241) \
    item(PLATFORM_TEST_SHMEMTEST_ONE, \
         "platform/test/shmemtest_one", 242) \
    item(PLATFORM_TEST_SHMEMTEST_SBT, \
         "platform/test/shmemtest_sbt", 243) \
    item(PLATFORM_TEST_TIMER_DISPATCHER, \
         "platform/test/timer_dispatcher", 244) \
    item(PLATFORM_TEST_TIMER_DISPATCHER_TIMER, \
         "platform/test/timer_dispatcher/timer", 245) \
    item(SDF_NAMING_SHARD, \
         "sdf/naming/shard", 246) \
    item(SDF_PROT_FLASH_SHARD, \
         "sdf/prot/flash/shard", 247) \
    item(SDF_PROT_REPLICATION_ADAPTER, \
         "sdf/prot/replication/adapter", 248) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR, \
         "sdf/prot/replication/copy_replicator", 249) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_EVENT, \
         "sdf/prot/replication/copy_replicator/event", 250) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_FAST_START, \
         "sdf/prot/replication/copy_replicator/fast_start", 251) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_LEASE, \
         "sdf/prot/replication/copy_replicator/lease", 252) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_LIVENESS, \
         "sdf/prot/replication/copy_replicator/liveness", 253) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_LOCKING, \
         "sdf/prot/replication/copy_replicator/locking", 254) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_META, \
         "sdf/prot/replication/copy_replicator/meta", 255) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_NOTIFY, \
         "sdf/prot/replication/copy_replicator/notify", 256) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_OP, \
         "sdf/prot/replication/copy_replicator/op", 257) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_RECOVERY, \
         "sdf/prot/replication/copy_replicator/recovery", 258) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_RECOVERY_REDO, \
         "sdf/prot/replication/copy_replicator/recovery/redo", 259) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_RECOVERY_UNDO, \
         "sdf/prot/replication/copy_replicator/recovery/undo", 260) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_SHUTDOWN, \
         "sdf/prot/replication/copy_replicator/shutdown", 261) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_STATE, \
         "sdf/prot/replication/copy_replicator/state", 262) \
    item(SDF_PROT_REPLICATION_COPY_REPLICATOR_VIP, \
         "sdf/prot/replication/copy_replicator/vip", 285) \
    item(SDF_PROT_REPLICATION_LOCKING, \
         "sdf/prot/replication/locking", 283) \
    item(SDF_PROT_REPLICATION_META_STORAGE, \
         "sdf/prot/replication/meta_storage", 263) \
    item(SDF_PROT_REPLICATION_META_STORAGE_BEACON, \
         "sdf/prot/replication/meta_storage/beacon", 264) \
    item(SDF_PROT_REPLICATION_META_STORAGE_DISTRIBUTED, \
         "sdf/prot/replication/meta_storage/distributed", 265) \
    item(SDF_PROT_REPLICATION_META_STORAGE_EVENT, \
         "sdf/prot/replication/meta_storage/event", 266) \
    item(SDF_PROT_REPLICATION_META_STORAGE_FLASH, \
         "sdf/prot/replication/meta_storage/flash", 267) \
    item(SDF_PROT_REPLICATION_META_STORAGE_LEASE, \
         "sdf/prot/replication/meta_storage/lease", 268) \
    item(SDF_PROT_REPLICATION_META_STORAGE_LEASE_LIVENESS, \
         "sdf/prot/replication/meta_storage/lease/liveness", 269) \
    item(SDF_PROT_REPLICATION_META_STORAGE_OP, \
         "sdf/prot/replication/meta_storage/op", 294) \
    item(SDF_PROT_REPLICATION_META_STORAGE_REMOTE, \
         "sdf/prot/replication/meta_storage/remote", 270) \
    item(SDF_PROT_REPLICATION_SEQNO, \
         "sdf/prot/replication/seqno", 271) \
    item(SDF_PROT_REPLICATION_TEST, \
         "sdf/prot/replication/test", 272) \
    item(SDF_PROT_REPLICATION_TEST_CASE, \
         "sdf/prot/replication/test/case", 273) \
    item(SDF_PROT_REPLICATION_TEST_EVENT, \
         "sdf/prot/replication/test/event", 274) \
    item(SDF_PROT_REPLICATION_TEST_FLASH, \
         "sdf/prot/replication/test/flash", 275) \
    item(SDF_PROT_REPLICATION_TEST_FRAMEWORK, \
         "sdf/prot/replication/test/framework", 276) \
    item(SDF_PROT_REPLICATION_TEST_GENERATOR, \
         "sdf/prot/replication/test/generator", 277) \
    item(SDF_PROT_REPLICATION_TEST_LIVENESS, \
         "sdf/prot/replication/test/liveness", 278) \
    item(SDF_PROT_REPLICATION_TEST_MKEY, \
         "sdf/prot/replication/test/mkey", 279) \
    item(SDF_PROT_REPLICATION_TEST_MSG, \
         "sdf/prot/replication/test/msg", 280) \
    item(SDF_PROT_REPLICATION_TEST_TIME, \
         "sdf/prot/replication/test/time", 281)
    

/**
 * Log category.
 */
enum plat_log_category {
#define item(caps, lower, value) \
    PLAT_LOG_CAT_ ## caps = value,
    PLAT_LOG_CATEGORY_NORMAL_ITEMS()

    /*
     * Figure out where the first automatically added category is while
     * still assigning values without holes.
     */
    PLAT_LOG_CAT_FIRST_AUTO,
    PLAT_LOG_CAT_LAST_NORMAL = PLAT_LOG_CAT_FIRST_AUTO - 1,

    PLAT_LOG_CATEGORY_AUTO_ITEMS()

    /** @brief statically assigned log categories are all before this */
    PLAT_LOG_CAT_STATIC_LIMIT,

#undef item
    PLAT_LOG_CAT_INVALID = -1
};

/**
 * @brief Define a local log category
 *
 * Example for shmemd.c:
 *
 * PLAT_LOG_CAT_LOCAL(LOG_CAT_STATE, "platform/shmemd/state")
 *
 * void enter_insane_state() {
 *     plat_log(PLAT_LOG_ID_INITIAL, LOG_CAT_STATE, LOG_LEVEL_DEBUG,
 *          "Entering insane state")
 *     // more magic here
 * }
 *
 * @param upper <IN> All caps and underscore
 * @param path <IN> Log path
 */
#define PLAT_LOG_CAT_LOCAL(upper, path)                                        \
    static int upper;                                                          \
                                                                               \
    static void upper ## _init() __attribute__((constructor));                 \
    static void upper ## _init() {                                             \
        upper = plat_log_add_category(path);                                   \
        plat_assert(upper > 0);                                                \
    }

#define PLAT_LOG_SUBCAT_LOCAL(upper, super, subpath)                           \
    static int upper;                                                          \
                                                                               \
    static void upper ## _init() __attribute__((constructor));                 \
    static void upper ## _init() {                                             \
        upper = plat_log_add_subcategory(super, subpath);                      \
        plat_assert(upper > 0);                                                \
    }

/**
 * @brief Set logging file
 *
 * The file is opened for appending and will be created if it does not
 * exist. In the current implementation
 *
 * #plat_log_reopen can be used to re-open the file to co-ordinate
 * with log rotation.
 *
 * @param file <IN> Log file
 * @param redirect <IN> What else to put into the log file.
 * @return 0 on success, -errno on failurec
 */
int plat_log_set_file(const char *filename, enum plat_log_redirect redirect);

/**
 * @brief Set logging to use syslog
 */
void plat_log_use_syslog();

/**
 * @brief Reopen log file.
 *
 * When a log file is in use instead of standard error it is
 * closed and re-opened for append so that log rotation can
 * be done.
 *
 * @return 0 on success, -errno on failure.
 */
int plat_log_reopen();

/**
 * @brief Enable logging time and set format
 *
 * @param format_arg <IN> strftime(3) format
 * @return 0 on success, -errno on failure.
 */
int plat_log_set_time_format(const char *format_arg);

/**
 * @brief Log seconds since epoch
 * @return 0 on success, -errno on failure.
 */
int plat_log_set_time_format_secs();

/**
 * @brief Log seconds since program start
 * @return 0 on success, -errno on failure.
 */
int plat_log_set_time_format_relative_secs();

/**
 * @brief Log fractional since program start
 * @return 0 on success, -errno on failure.
 */
int plat_log_set_time_format_relative_secs_float();

/**
 * @brief Log H:M:S since program start
 * @return 0 on success, -errno on failure.
 */
int plat_log_set_time_format_relative_hms();

/**
 * @brief Log H:M:S since program start with fractional seconds
 * @return 0 on success, -errno on failure.
 */
int plat_log_set_time_format_relative_hms_float();

/**
 * @brief Log time in default data(1) format
 * @return 0 on success, -errno on failure.
 */
int plat_log_set_time_format_bin_date();

/**
 * @brief Convert tv to current logging time format
 *
 * @return pthread-local buffer which will be used by subsequent calls
 */
char *plat_log_timeval_to_string(const struct timeval *tv);

/**
 * @brief Set the callback used to get time for prefixes
 *
 * A raw function pointer is used instead of the closure abstraction
 * because the closure code uses logging.
 *
 * @param fn <IN> function
 * @param extra <IN> first arg to function
 * @param old_fn <OUT> old function when not NULL
 * @param old_extra <OUT> old extra when not NULL
 */
void plat_log_set_gettime(void (*fn)(void *extra, struct timeval *tv),
                          void *extra,
                          void (**old_fn)(void *extra, struct timeval *tv),
                          void **old_extra);

/** @brief Return timeval using plat_log's gettime function */
void plat_log_gettimeofday(struct timeval *tv);

/**
 * @brief Reference logging subsystem
 *
 * Call from a constructor to guarantee logging will shutdown after your
 * destructor.
 *
 * @code
 * char *text = NULL;
 *
 * static __atttribute__((constructor)) your_constructor {
 *     plat_log_reference();
 *
 *     text = plat_strdup("The meaning of life");
 *
 *     plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
 *                  "Set to %s", text);
 * }
 *
 * static __atttribute__((constructor)) local_destructor {
 *     plat_free(text);
 *
 *     plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
 *                  "done");
 *
 *     plat_log_release();
 * }
 *
 * @endcode
 */
void plat_log_reference();

/**
 * @brief Release logging subsystem
 *
 * Call from a destructor if #plat_log_reference was called from a constructor
 */
void plat_log_release();

/**
 * Queue a log message associated with a specific operation opid,
 * emitting immediately when the current logging level for category
 * is no more severe than log_level.  format is a printf
 * string.
 *
 * The convenience function plat_log_msg is provided for system
 * activity not associated with an operation.
 *
 * Developers adding a plat_log message call to their code specify
 * an id of PLAT_LOG_ID_INITIAL and a permanant id is assigned by
 * the plat_log_update binary which is run as part of the
 * build process.
 *
 * Changing the log message/arguments requires a new id.
 */
void plat_log_op_msg(plat_op_label_t op, int logid,
                     enum plat_log_category category,
                     enum plat_log_level level, const char *format, ...)
    __attribute__((format(printf, 5, 6)));

/**
 * Internal helper function for plat_log_msg.  When file is NULL a file/line/
 * functionless log message will be emitted.  Log messages are unconditionally
 * output.
 */
void plat_log_msg_helper(const char *file, unsigned line, const char *function,
                         int logid, int category, enum plat_log_level level,
                         const char *format, ...)
    __attribute__((format(printf, 7, 8)));

/**
 * Identical to #plat_log_msg, but forwards line information from the caller
 * for use from other macros.
 */
#define plat_log_msg_forward(file, line, function, logid, category, level,     \
                             format, args...)                                  \
    do {                                                                       \
        if (plat_log_enabled((category), (level))) {                           \
            plat_log_msg_helper(basename(file), line, function, logid, category, level,  \
                                format, ##args);                               \
        }                                                                      \
    } while (0)

/**
 * Queue a log message, emitting immediately when the current logging
 * level for category is no more severe than log_level.  format is a printf
 * string.
 *
 * Developers adding a plat_log message call to their code specify
 * an id of PLAT_LOG_ID_INITIAL and a permanant id is assigned by
 * the plat_log_update binary which is run as part of the
 * build process.
 *
 * Changing the log message/arguments requires a new id.
 */
#define plat_log_msg(logid, category, level, format, args...)                  \
    ffdc_log(__LINE__, logid, category, level, format, ##args);                \
    plat_log_msg_forward(__FILE__, __LINE__, __PRETTY_FUNCTION__,              \
                         logid, category, level, format, ##args)

/**
 * @brief Internal helper function for plat_log_enabled.
 */
/*CSTYLED*/
PLAT_LOGGING_C_INLINE __attribute__((pure))
int
plat_log_enabled_pure(int category, enum plat_log_level level) {
    return (level >= plat_log_level_immediate_cache[category]);
}

/**
 * @brief Internal helper function for plat_log_enabled.
 */
/*CSTYLED*/
PLAT_LOGGING_C_INLINE __attribute__((const))
int
plat_log_enabled_const(int category, enum plat_log_level level) {
    return (plat_log_enabled_pure(category, level));
}

/**
 * @brief Determine whether logging is enabled for the category, level tuple
 *
 * "enabled" means that if called, plat_log_msg() or plat_log_op_msg() will
 * process its arguments.  Whether the log message will be output immediately is
 * orthogonal.
 *
 * XXX The current implementation always has immediate output for enabled
 * log messages but this will change.
 *
 * @param category <IN> A valid log category statically defined in
 * PLAT_LOG_CATEGORY_ITEMS() or added dynamically with plat_log_add_category(),
 * PLAT_LOG_CAT_LOCAL(), or PLAT_LOG_SUBCAT_LOCAL().  Other values will
 * produce undefined results that may include process termination.
 *
 * @param level <IN> Logging level
 *
 * @return Non-zero when logging is enabled, 0 disabled
 */
#define plat_log_enabled(category, level)                                      \
    ((level) >= PLAT_LOG_CHECK_LEVEL &&                                        \
     ((level) >= PLAT_LOG_PURE_LEVEL ?                                         \
      plat_log_enabled_pure(category, level) :                                 \
      plat_log_enabled_const(category, level)))

/**
 * @brief Convert integer log category to string.
 *
 * @return string on success, NULL on failure.  string is guaranteed valid
 * until static and global scope destructors run.
 */
const char *plat_log_cat_to_string(int category);

/**
 * Set level for immediate emission associated with category.
 */
void plat_log_set_level(int category, enum plat_log_level level);

/**
 * Parse log argument.  Returns -errno on failure.
 *
 * Currently, arguments of the form
 *  category=level
 * are supported with
 *  default=level
 * taking  effect
 *
 * Until we add reasonable argument parsing code where object files add
 * arguments to a parser via constructor attribute functions users should
 * pass the argument part of all "--log" arguments to plat_log_parse_arg.
 *
 * getopt_long() is recomended as the preferred mechanism.   See
 * sdf/platform/shmemd for example code.
 */
int plat_log_parse_arg(const char *text);

/**
 * Dump usage for the logging subsystem to stderr.
 */
void plat_log_usage();

/**
 * Dynamically add a log category.
 *
 * plat_log_add_category() is idempotent and may be called from static and
 * global scope constructors. It allows local logging primitives to be
 * defined without leaking implementation details and polluting logging.h.
 *
 * Applications where log category scope is no broader than a file should
 * use PLAT_LOG_CAT_LOCAL().
 *
 * @param category <IN> text category name
 * @return category index >= 0 on success, -errno on error.
 */
int plat_log_add_category(const char *category);

/**
 * Dynamically add a log subcategory.
 *
 * plat_log_add_subcategory() is idempotent and may be called from static and
 * global scope constructors. It allows local logging primitives to be
 * defined without leaking implementation details and polluting logging.h.
 *
 * @param category <IN> numeric category
 * @param subcategory <IN> string subcategory
 * @return category index >= 0 on success, -errno on error.
 */
int plat_log_add_subcategory(int category, const char *subcategory);

/*
 * Enabling this would guarantee that logging remains available until
 * all users have shutdown.
 */
#if 0
#ifndef PLATFORM_LOGGING_C
/* Guarantee the logging subsystem is initialized */
static void __attribute__((constructor))
plat_log_reference_local() {
    plat_log_reference();
}

static void __attribute__((destructor))
plat_log_release_local() {
    plat_log_release();
}
#endif
#endif

__END_DECLS
#endif /* ndef PLATFORM_LOGGING_H */
