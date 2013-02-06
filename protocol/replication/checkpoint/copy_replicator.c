/*
 * File:   sdf/protocol/replication/bulk_replicator.h
 *
 * Author: drew
 *
 * Created on April 18, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: copy_replicator.c 13386 2010-05-03 09:52:34Z drew $
 */

/**
 * Data path and recovery for replication.  Container meta-data such as
 * current home node, lease length, and replica authoritative or stale state
 * is handled by the replicator_meta_storage API an instance of which is owned
 * by each copy_replicator object.
 *
 * The copy replicator is historically named for the SDF_REPLICATION_SIMPLE
 * type that it originallly implemented, but other replication types shared
 * enough in common that putting them here worked better than refactoring.
 *
 * Most log messages start with a structure name and pointer argument to
 * aid in debugging a specific object's state.
 *
 * At least four classes of state machines run the replication data-path at
 * this layer:
 *
 * - One for shard level access and recovery.  A table enumerating the states,
 *   permitted operations, and enter/leave leave code is defined by
 *   #CR_SHARD_STATE_ITEMS.  State is stored in #cr_shard objects.
 *   For SDF_REPLICATION_V1_N_PLUS_1 and SDF_REPLICATION_V1_2_WAY types
 *   one cr_shard structure exists for each intra node VIP group.
 *
 * - One for replica level access and recovery.  A table enumerating the states,
 *   permitted operations, and enter/leave leavecode is defined by
 *   #CR_REPLICA_STATE_ITEMS.  State is stored in #cr_replica objects.
 *   For SDF_REPLICATION_V1_N_PLUS_1 and SDF_REPLICATION_V1_2_WAY types
 *   no cr_replica objects are used.
 *
 * - One for individual operations as an ad-hoc sequence of asynchronous
 *   functions connected by closures.  A table describing various
 *   messages entry points and how their state interacts with shard/
 *   replica state is enumerated in #CR_MSG_ITEMS. State is stored in
 *   #cr_op objects. For SDF_REPLICATION_V1_N_PLUS_1 and
 *   SDF_REPLICATION_V1_2_WAY data is not routed through #copy_replicator so
 *   no #cr_op objects are used.
 *
 *    The logic is split into operation level logic
 *        #cr_op_flash_forward      forward flash message to one node
 *        #cr_op_create_shard       create shard logic
 *    and lower level pieces with acronyms corresponding to their
 *    top-level function as prefixes
 *        #crofr_response           cr_op_flash_request_ response
 *    etc.
 *
 * - One for individual redo or undo recovery operations, implemented
 *   as an ad-hoc sequence of asynchronous functions conencted by
 *   closures.  #cr_replica_redo_op starts a single redo operation,
 *   #cr_replica_undo_op does the same for undo.
 *
 * Meta-data about shard/vip group (current home node; lease time remaining)
 * and replica state (authoritative or stale; recovery progress) is stored
 * in the #replicator_meta_storage subsystem.
 *
 * The has-a relationship tree looks like
 * copy_replicator
 *       replicator_meta_storage (1)
 *       cr_shard (>= 0)
 *               cr_replica (> 0 after receiving meta-data, 0 before)
 *                      cr_recovery_op (>= 0, source and destination)
 *                              replicator_key_lock (0 or 1)
 *       cr_op (>= 0)
 *               replicator_key_lock (0 or 1)
 *
 * The threading model is a single non-re-entrant thread for shard, replica,
 * and meta-data state with message based concurrency control using the closure
 * infrastructure and copy_replicator.callbacks.single_scheduler.  Trampoline
 * functions are used to run code on behalf of other threads within this
 * context.  There are no limits on the number of in-flight operations
 * beyond that used within each shard to place bounds on recovery time.
 *
 * As of 2009-06-05 all data traffic goes through the same mechanism.  This
 * may change to executing synchronous reads+writes in the context of their
 * calling thread since this simplifies pass-through to the underlying flash
 * code.  Empirical evidence will determine whether asynchronous/
 * remote calls execute in a thread per shard or shared thread pool.  Since
 * clusters must be sized to accomodate peak per load node we'll want more
 * containers than nodes so that load can shed uniformly - we want a failure
 * in a 5 node cluster to increase the load of the remaining 4 nodes by 25%
 * instead of 1 node by 100% which could mandate an 8 node cluster instead
 * of 5.  Due to locality effects such a configuration should perform better
 * with a thread per shard instead of a shared thread pool of the same size.
 *
 * Exceptions where data is not routed through #copy_replicator
 * have #CR_REPLICATION_TYPE_ITEMS() item flag arguments including
 * CRTF_MANAGE_META, notably SDF_REPLICATION_V1_2_WAY which is the only
 * supported replication type for customers through v2.5.
 */
#include "sys/queue.h"

#include "platform/assert.h"
#include "platform/closure.h"
#include "platform/defs.h"
#include "platform/logging.h"
#include "platform/mbox_scheduler.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"

#include "fth/fth.h"

#include "common/sdftypes.h"
#include "protocol/protocol_common.h"
/* XXX: Move to protocol/flash.h? to resolve link order */
#include "protocol/home/home_flash.h"
/* XXX: Move to protocol/util.h? to resolve link order */
#include "protocol/home/home_util.h"
#include "protocol/replication/replicator.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_msg_types.h"
#include "sdfmsg/sdf_msg_wrapper.h"

#include "shared/container_meta.h"
#include "shared/shard_meta.h"


#include "copy_replicator.h"
#include "copy_replicator_internal.h"
#include "key_lock.h"
#include "meta_types.h"
#include "meta_storage.h"
#include "rpc.h"

/* Forward declarations; definitions in top-down has-a order when possible */
struct cr_op;
struct cr_recovery_op;
struct cr_shard_notify_state;

/**
 * @brief Internal closure for implementing #sdf_replicator_get_container_stats
 *
 * Trampoline function which dispatches in single scheduler context
 */
PLAT_CLOSURE3(cr_do_get_container_stats,
              fthMbox_t *, status,
              struct sdf_replicator_shard_stats *, stats,
              SDF_cguid_t, cguid);

/**
 * @brief Internal closure for implementing #sdf_replicator_command_async
 *
 * Trampoline function which dispatches in single scheduler context
 */
PLAT_CLOSURE3(cr_do_command_async,
              SDF_shardid_t, shard,
              char *, command,
              sdf_replicator_command_cb_t, cb);

/** @brief Basic logging category */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator");

/** @brief Timer event reference counts and frees */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_EVENT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/event");

/** @brief Leases */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_LEASE, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/lease");

/** @brief Liveness events */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_LIVENESS, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/liveness");

/** @brief Immediate lease acquisition when the preferred node */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_FAST_START, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/fast_start");

/** @brief Consumer notification */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_NOTIFY, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/notify");

/** @brief Meta-data updates */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_META, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/meta");

/** @brief Operations */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_OP, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/op");

/**
 * @brief All things recovery
 *
 * Excludes simple state transitions which have #LOG_CAT_STATE
 */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_RECOVERY, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/recovery");

/** @brief Redo phase of recovery */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_RECOVERY_REDO, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/recovery/redo");

/** @brief Undo phase of recovery */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_RECOVERY_UNDO, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/recovery/undo");

/** @brief Shutdown code */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_SHUTDOWN, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/shutdown");

/** @brief Log state transitions at shard and replica scope */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_STATE, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/state");

/** @brief Log v1 vip group based information */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_VIP, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator/vip");

/**
 * @brief Callback for #cr_shard_put_meta
 *
 * We use a simplified version of #rms_put_shard_meta_cb because
 * only one copy of the current and proposed #cr_shard_meta
 * structures is needed due to serialization of recovery events.
 */
PLAT_CLOSURE1(cr_shard_put_meta_cb, SDF_status_t, status);

/** @brief Callback for #cr_shard_notify */
PLAT_CLOSURE(cr_shard_notify_cb);


/** @brief State of the replicator */
enum cr_state {
    /** @brief Allocated, but not started */
    CR_STATE_INITIAL,

    /** @brief Processing requests, #sdf_replicator_shutdown not called */
    CR_STATE_RUNNING,

    /** @brief #sdf_replicator_shutdown called but completing asynchronously */
    CR_STATE_STOPPING,

    /** @brief #sdf_replicator_shutdown completed */
    CR_STATE_STOPPED
};


/**
 * @brief Table of different message dispositions
 * item(caps, lower)
 */

/* BEGIN CSTYLED */

#define CR_FANOUT_ITEMS() \
    /* Send to local node (which contains one of the replicas) */              \
    item(LOCAL, local)                                                         \
    /* Send to all replicas. Fail if any sub-operation fails */                \
    item(ALL_OR_NOTHING, all_or_nothing)                                       \
    /* Send to all up replicas.  Update replica state if there is a problem */ \
    item(ALL_LIVE, all_live)

/* END CSTYLED */

enum cr_fanout {
#define item(caps, lower) CR_FANOUT_ ## caps,
    CR_FANOUT_ITEMS()
#undef item
};

/*
 * An unset value is provided for each option so we can programatically
 * sanity check that each table entry has a value assigned as opposed
 * to finding out the hard way that we missed one.
 */
enum cr_op_flags {
    /** @brief This op needs a sequence number */
    CROF_NEED_SEQNO = 1 << 0,
    /** @brief This op has no sequence number */
    CROF_NO_SEQNO = 1 << 1,

    /** @brief A write lock on the key/syndrome is required */
    CROF_LOCK_EXCLUSIVE = 1 << 2,
    /** @brief A read lock on the key/syndrome is required */
    CROF_LOCK_SHARED = 1 << 3,
    /** @brief No lock on the key/syndrome is required */
    CROF_LOCK_NONE = 1 << 4,

    /** @brief A lock exists on this key as used by the recovery code */
    CROF_LOCK_EXISTS = 1 << 5,

    /** @brief No #cr_shard is needed */
    CROF_CR_SHARD_NONE = 1 << 6,
    /** @brief cr_shard need is determined by replication type property */
    CROF_CR_SHARD_BY_TYPE = 1 << 7
};

/**
 * @brief Table describing handling of message types
 *
 * item(msgtype, fanout, start_fn)
 * @param msgtype <IN> type of message
 * @param fanout <IN> to which nodes it is sent
 * XXX: drew 2009-05-26 merge fanout with flags
 * @param flags <IN> flags for operation
 * @param start_fn <IN> lambda function called to start operation
 */

/* BEGIN CSTYLED */

#define CR_MSG_ITEMS(op_arg, context_arg) \
    item(HFXST, LOCAL, CROF_NO_SEQNO|CROF_LOCK_NONE|CROF_CR_SHARD_BY_TYPE,     \
         cr_op_flash_forward((op_arg), (context_arg),                          \
                             (op_arg)->cr->config.my_node,                     \
                             sdf_msg_recv_wrapper_create((op_arg)->cr->closure_scheduler, \
                                                         &cr_op_flash_response,\
                                                         (op_arg))))           \
    item(HFGFF, LOCAL, CROF_NO_SEQNO|CROF_LOCK_SHARED|CROF_CR_SHARD_BY_TYPE,   \
         cr_op_flash_forward((op_arg), (context_arg),                          \
                             (op_arg)->cr->config.my_node,                     \
                             sdf_msg_recv_wrapper_create((op_arg)->cr->closure_scheduler, \
                                                         &cr_op_flash_response,\
                                                         (op_arg))))           \
    item(HFPTF, ALL_LIVE,                                                      \
         CROF_NEED_SEQNO|CROF_LOCK_EXCLUSIVE|CROF_CR_SHARD_BY_TYPE,            \
         cr_op_flash_multi_node((op_arg), (context_arg),                       \
                                sdf_msg_recv_wrapper_create((op_arg)->cr->closure_scheduler, \
                                                            &cr_op_flash_response, \
                                                            (op_arg))))        \
    item(HFSSH, ALL_LIVE,                                                      \
         CROF_NEED_SEQNO|CROF_LOCK_EXCLUSIVE|CROF_CR_SHARD_BY_TYPE,            \
         cr_op_flash_multi_node((op_arg), (context_arg),                       \
                                sdf_msg_recv_wrapper_create((op_arg)->cr->closure_scheduler, \
                                                            &cr_op_flash_response, \
                                                            (op_arg))))        \
    item(HFCIF, ALL_LIVE,                                                      \
         CROF_NEED_SEQNO|CROF_LOCK_EXCLUSIVE|CROF_CR_SHARD_BY_TYPE,            \
         cr_op_flash_multi_node((op_arg), (context_arg),                       \
                                sdf_msg_recv_wrapper_create((op_arg)->cr->closure_scheduler, \
                                                            &cr_op_flash_response, \
                                                            (op_arg))))        \
    item(HFCZF, ALL_LIVE,                                                      \
         CROF_NEED_SEQNO|CROF_LOCK_EXCLUSIVE|CROF_CR_SHARD_BY_TYPE,            \
         cr_op_flash_multi_node((op_arg), (context_arg),                       \
                                sdf_msg_recv_wrapper_create((op_arg)->cr->closure_scheduler, \
                                                            &cr_op_flash_response, \
                                                            (op_arg))))        \
    item(HFDFF, ALL_LIVE,                                                      \
         CROF_NEED_SEQNO|CROF_LOCK_EXCLUSIVE|CROF_CR_SHARD_BY_TYPE,            \
         cr_op_flash_multi_node((op_arg), (context_arg),                       \
                                sdf_msg_recv_wrapper_create((op_arg)->cr->closure_scheduler, \
                                                            &cr_op_flash_response, \
                                                            (op_arg))))        \
    item(HFSET, ALL_LIVE,                                                      \
         CROF_NEED_SEQNO|CROF_LOCK_EXCLUSIVE|CROF_CR_SHARD_BY_TYPE,            \
         cr_op_flash_multi_node((op_arg), (context_arg),                       \
                                sdf_msg_recv_wrapper_create((op_arg)->cr->closure_scheduler, \
                                                            &cr_op_flash_response, \
                                                            (op_arg))))        \
    item(HFCSH, ALL_OR_NOTHING,                                                \
         CROF_NO_SEQNO|CROF_LOCK_NONE|CROF_CR_SHARD_NONE,                      \
         cr_op_create_shard((op_arg), (context_arg)))                          \
    /* XXX: This should be multi-phase */                                      \
    item(HFDSH,                                                                \
         ALL_LIVE, CROF_NO_SEQNO|CROF_LOCK_NONE|CROF_CR_SHARD_BY_TYPE,         \
         cr_op_flash_multi_node((op_arg), (context_arg),                       \
                                sdf_msg_recv_wrapper_create((op_arg)->cr->closure_scheduler, \
                                                            &cr_op_flash_response, \
                                                            (op_arg))))
/* END CSTYLED */

/**
 * @brief State
 *
 * The copy replicator uses a simple event driven implementation plumbed
 * together with closures that execute in a single fth thread.  Scalable
 * performant implementations will parallelize operations accross threads
 * while running the recovery state machines in the single scheduler.
 *
 * The interface to the outside world is entirely via asynchronous closures,
 * with opaque message payloads for the data path so that replication is
 * somewhat independant of SDF evolution.
 *
 * Service provider threading models are completely orthogonal to the
 * model used here; for example the #sdf_replicator_adapter implementation
 * is thread per request.
 *
 * One #cr_op is created for each user request.
 */
struct copy_replicator {
    /** @brief Entry points for wrappers.  Must be first */
    struct sdf_replicator base;

    /**
     * @brief Configuration
     *
     * XXX: drew 2009-08-11 Replace with pointer for deep copy.
     */
    struct sdf_replicator_config config;

    /** Interface to the outside world */
    struct sdf_replicator_api callbacks;

    /** @brief state */
    enum cr_state state;

    /** @brief plat_mbox_closure_scheduler run out of fthread */
    struct plat_closure_scheduler *closure_scheduler;

    /** @brief thread which runs closure scheduler until terminated */
    fthThread_t *closure_scheduler_thread;

    /* Meta-data storage subsystem */
    struct replicator_meta_storage *meta_storage;

    /** @brief list of all in-flight operations */
    LIST_HEAD(, cr_op) op_list;

    /**
     * @brief Reference count
     * 1 + one per op
     *
     * XXX: drew 2008-10-15 We need to differentiate between references which
     * count towards when the response gets sent and those that are just here
     * for memory purposes.  Probably significant for the Paxos implementation.
     */
    int ref_count;

    /** @brief Shard shutdown is complete when this count hits zero */
    int shard_shutdown_count;

    /** @brief Invoke on shutdown completion */
    sdf_replicator_shutdown_cb_t shutdown_closure;

    /** @brief Node state */
    struct cr_node *nodes;

    struct {
        /** @brief Lock on notification state */
        struct fthLock lock;

        /**
         * @brief Consumers which must be notified of state changes
         *
         * One reference count is used for each one.
         */
        LIST_HEAD(, cr_notifier) list;
    } notifiers;

    /** @brief Stats on all shards */
    struct cr_shard_stat_counters *total_stat_counters;

    /**
     * @brief Open shards
     *
     * XXX: drew 2008-11-13 We need to add a separate hash table for quicker
     * lookups when we have use cases involving more than a few containers.
     */
    LIST_HEAD(, cr_shard) shard_list;

};

/** @brief User notification state from #cr_add_notifier */
struct cr_notifier {
    /** @brief Parent */
    struct copy_replicator *cr;

    /** @brief User's function applied on changes */
    sdf_replicator_notification_cb_t cb;

    /** @brief Last reported shard state */
    LIST_HEAD(, cr_notifier_shard) shard_list;

    /** @brief Entry on cr->notifier.list */
    LIST_ENTRY(cr_notifier) notifiers_list_entry;
};

/** @brief Last reported shard state so that we can set edge triggered events */
struct cr_notifier_shard {
    /**
     * @brief sguid
     *
     * May be #SDF_SHARDID_INVALID for #CRTF_META_DISTRIBUTED shards
     * which this node has not yet associated with a container.
     */
    SDF_shardid_t sguid;

    /**
     * @brief Intra node vip group ID associated with this #cr_shard
     *
     * #VIP_GROUP_ID_INVALID for no gid.
     */
    int vip_group_id;

    /** @brief Previous ltime */
    uint64_t ltime;

    /** @brief Previous meta-data */
    uint64_t shard_meta_seqno;

    /** @brief Previous access update if shard_meta is non-null */
    enum sdf_replicator_access access;

    /** @brief Previous expiration time if shard_meta is non-null */
    struct timeval expires;

    LIST_ENTRY(cr_notifier_shard) shard_list_entry;
};

/**
 * @brief Operational flags associated with given shard states
 *
 * An unset value is provided for each option so we can programatically
 * sanity check that each table entry has a value assigned as opposed
 * to finding out the hard way that we missed one.
 */
enum cr_shard_state_flags {
    /** @brief shard->shard_meta being changed by other nodes */
    CRSSF_META_SOURCE_OTHER = 1 << 0,
    /** @brief shard->shard_meta being changed by this node */
    CRSSF_META_SOURCE_SELF = 1 << 1,

    /** @brief no lease should be held for this state */
    CRSSF_NO_LEASE = 1 << 2,
    /** @brief request lease initially */
    CRSSF_REQUEST_LEASE = 1 << 3,
    /** @brief renew lease as needed */
    CRSSF_RENEW_LEASE = 1 << 4,

    /** @brief no access for any one */
    CRSSF_ACCESS_NONE = 1 << 5,
    /** @brief read access allowed */
    CRSSF_ACCESS_READ = 1 << 6,
    /** @brief write access allowed */
    CRSSF_ACCESS_WRITE = 1 << 7,
    /** @brief Read-write access */
    CRSSF_ACCESS_RW = CRSSF_ACCESS_READ|CRSSF_ACCESS_WRITE
};

#ifdef notyet
/*
 * @brief Table enumerating CR shard level events
 *
 * item(caps, lower, lambda)
 */
#define CR_SHARD_EVENT_ITEMS(shard_arg) \
    /** @brief shard->op_count became zero */                                  \
    item(CR_SHARD_EVENT_OP_COUNT_ZERO, op_count_zero, /* none */)              \
    /** @brief no replicas have mutual redo operations remaining */            \
    item(CR_SHARD_EVENT_MUTUAL_REDO_DONE, mutual_redo_done, /* none */)        \
    /** @brief serious error */                                                \
    item(CR_SHARD_EVENT_ERROR, error, /* none */)                              \
    /** @brief replica state change */                                         \
    item(CR_SHARD_REPLICA_STATE, replica_state, /* none */)                    \
    item(CR_SHARD_LEASE_EXPIRED, lease_expired, /* none */)                    \
    item(CR_SHARD_META_UPDATE, meta_update, /* none */)
#endif

/**
 * @brief Table for CR shard state
 *
 * @param shard_arg <IN> struct cr_shard *
 * @param state_from <IN> state transitioning from
 * @param state_to <IN> state transitioning to
 *
 * The enter function may advance to a subsequent state, the leave function
 * may not
 *
 * item(caps, lower, flags, enter, leave)
 * @param caps <IN> enumeration value
 * @param lower <IN> pretty-printed value
 * @param flags <IN> operational flags for state
 * @param enter <IN> lambda applied on state entry
 * @param leave <IN> lambda applied on state exit$
 * @param event_stuff <IN> space delimited list of actions
 *     handle(event, lambda)
 *     ignore(event)
 * The CR_SHARD_EVENT_ prefix must be omitted from event specifications in
 * event_stuff
 */

/*
 * FIXME: drew 2009-06-07
 *
 * 1.  We need to sync before all iterations: mutual redo, undo, and redo
 *     because Jake's code is only guaranteed to iterate on-disk structures.
 *
 * 2.  The sync for mutual redo and undo have to happen after all prior
 *     IOs initiated by previous home nodes have completed so that those
 *     operations are included in the iterations.  The original ltime
 *     IO fencing mechanism is enough for this.
 *
 * 3.  The sync for redo needs to initiate after the last consecutive
 *     completed sequence reaches the its high water mark.
 *
 * A generic blocking mechanism may be useful, with a merge of home_flash.c
 * and copy_replicator.c
 */

#define CR_SHARD_STATE_ITEMS(shard_arg, state_from, state_to) \
    /**                                                                        \
     * @brief Just constructed                                                 \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_TO_WAIT_META immediately                  \
     */                                                                        \
    item(CR_SHARD_STATE_INITIAL, initial,                                      \
         CRSSF_META_SOURCE_OTHER|CRSSF_NO_LEASE|CRSSF_ACCESS_NONE,             \
         cr_shard_set_state(shard_arg,                                         \
                            CR_SHARD_STATE_WAIT_META) /* enter */,             \
         /* no leave */)                                                       \
    /**                                                                        \
     * @brief Await transition to CR_SHARD_STATE_WAIT_META                     \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_WAIT_META when the pending op count       \
     * reaches zero unless shutdown which moves the state to                   \
     * CR_SHARD_STATE_TO_SHUTDOWN                                              \
     */                                                                        \
    item(CR_SHARD_STATE_TO_WAIT_META, to_wait_meta,                            \
         CRSSF_META_SOURCE_OTHER|CRSSF_NO_LEASE|CRSSF_ACCESS_NONE,             \
         /* no enter */, /* no leave */)                                       \
    /**                                                                        \
     * @brief Waiting for meta-data                                            \
     *                                                                         \
     * When not actively serving traffic in RO or RW mode we await meta-data   \
     * updates which would cause us to transition into another node; such as   \
     * the home node's lease expiring or our replica being set to read-only    \
     * access following recovery.                                              \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_WAIT_META to re-evaluate cached meta-data,\
     * CR_SHARD_STATE_DELAY_LEASE_ACQUISITION, CR_SHARD_STATE_CREATE_LEASE,    \
     * CR_SHARD_STATE_CREATE_NO_LEASE, CR_SHARD_STATE_RO,                      \
     * CR_SHARD_STATE_TO_SHUTDOWN, or CR_SHARD_STATE_YIELD                     \
     */                                                                        \
    item(CR_SHARD_STATE_WAIT_META, wait_meta,                                  \
         CRSSF_META_SOURCE_OTHER|CRSSF_NO_LEASE|CRSSF_ACCESS_NONE,             \
         /* no enter */, /* no leave */)                                       \
    /**                                                                        \
     * @brief Yield shard ownership                                            \
     *                                                                         \
     * Set the shard to an un-owned state.  As of 2010-04-30 this was only     \
     * used for CRTF_META_DISTRIBUTED shards.                                  \
     *                                                                         \
     * Meta-data updates to a no-lease condition are done here so              \
     * that                                                                    \
     * 1) The existing error path is preserved so that the America release     \
     * is at lower risk                                                        \
     *                                                                         \
     * 2) They can block on previous meta updates                              \
     * (CR_SHARD_STATE_TO_WAIT_META won't transition until all pending puts    \
     * complete) and include the correct                                       \
     * #cr_persistent_shard_meta.last_home_node                                \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_TO_WAIT_META on successful exit or        \
     * CR_SHARD_STATE_TO_SHUTDOWN on shutdown.                                 \
     */                                                                        \
    item(CR_SHARD_STATE_YIELD, yield,                                          \
         CRSSF_META_SOURCE_SELF|CRSSF_NO_LEASE|CRSSF_ACCESS_NONE,              \
         cr_shard_yield_enter(shard_arg), /* no leave */)                      \
    /**                                                                        \
     * @brief Create shard no lease                                            \
     *                                                                         \
     * Transition to CR_SHARD_STATE_DELAY_LEASE_ACQUISITION on success,        \
     * CR_SHARD_STATE_TO_WAIT_META on failure,                                 \
     * CR_SHARD_STATE_TO_SHUTDOWN on shutdown.                                 \
     */                                                                        \
    item(CR_SHARD_STATE_CREATE_NO_LEASE, create,                               \
         CRSSF_META_SOURCE_SELF|CRSSF_NO_LEASE|CRSSF_ACCESS_NONE,              \
         cr_shard_create_enter(shard_arg), /* no leave */)                     \
    /**                                                                        \
     * @brief Create shard                                                     \
     *                                                                         \
     * Transition to CR_SHARD_STATE_RW on success,                             \
     * CR_SHARD_STATE_TO_WAIT_META on failure,                                 \
     * CR_SHARD_STATE_TO_SHUTDOWN on shutdown.                                 \
     */                                                                        \
    item(CR_SHARD_STATE_CREATE_LEASE, create,                                  \
         CRSSF_META_SOURCE_SELF|CRSSF_REQUEST_LEASE|CRSSF_ACCESS_NONE,         \
         cr_shard_create_enter(shard_arg), /* no leave */)                     \
    /**                                                                        \
     * @brief Wait to attempt lease acquisition                                \
     *                                                                         \
     * On startup we give the preferred node a chance to attempt acquiring     \
     * a lease, and where another node becomes the master we await becoming    \
     * authoritative with an open range.                                       \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_REQUEST_LEASE or                          \
     * CR_SHARD_STATE_TO_SHUTDOWN                                              \
     */                                                                        \
    item(CR_SHARD_STATE_DELAY_LEASE_ACQUISITION, delay_lease_acquisition,      \
         CRSSF_META_SOURCE_OTHER|CRSSF_NO_LEASE|CRSSF_ACCESS_NONE,             \
         cr_shard_delay_lease_acquisition_enter(shard_arg),                    \
         cr_shard_delay_lease_acquisition_leave(shard_arg))                    \
    /**                                                                        \
     * @brief Request lease                                                    \
     *                                                                         \
     * A meta storage request for a lease has been issued.                     \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_GET_SEQNO on success when copy_replicator \
     * is in the data path, CR_SHARD_STATE_RW when copy replicator is only in  \
     * the meta-data path, and CR_SHARD_STATE_TO_WAIT_META on lease expiration/\
     * grant failure, or CR_SHARD_STATE_TO_SHUTDOWN                            \
     */                                                                        \
    item(CR_SHARD_STATE_REQUEST_LEASE, request_lease,                          \
         CRSSF_META_SOURCE_SELF|CRSSF_REQUEST_LEASE|CRSSF_ACCESS_NONE,         \
         cr_shard_request_lease_enter(shard_arg), /* no leave */)              \
    /**                                                                        \
     * @brief Waiting on sequence number                                       \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_UPDATE_1 on success,                      \
     * CR_SHARD_STATE_WAIT_META on lease expiration.                           \
     */                                                                        \
    item(CR_SHARD_STATE_GET_SEQNO, get_seqno,                                  \
         CRSSF_META_SOURCE_SELF|CRSSF_RENEW_LEASE|CRSSF_ACCESS_NONE,           \
         cr_shard_get_seqno_enter(shard_arg), /* no leave */)                  \
    /**                                                                        \
     * @brief Make first shard meta update                                     \
     * - Mark all replicas with open ranges synced up to last - open range     \
     * - Mark all replicas requiring mutual redo from last - open range to     \
     *   last + open range                                                     \
     *                                                                         \
     * XXX: drew 2009-05-24 At this point we also have to fence any stale      \
     * operations from previous ltimes.                                        \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_MUTUAL_REDO on success,                   \
     * CR_SHARD_STATE_TO_WAIT_META on lease expiration.                        \
     */                                                                        \
    item(CR_SHARD_STATE_UPDATE_1, update_1,                                    \
         CRSSF_META_SOURCE_SELF|CRSSF_RENEW_LEASE|CRSSF_ACCESS_NONE,           \
         cr_shard_update_1_enter(shard_arg), /* no leave */)                   \
    /**                                                                        \
     * @brief Mutual redo                                                      \
     *                                                                         \
     * Perform mutual redo on all live replicas, where we perform redo from    \
     * each replica to all others with the original sequence numbers.  We      \
     * don't do an undo because that would advance the sequence  numbers       \
     * and complicate things.                                                  \
     *                                                                         \
     * It would be possible to set all but the local replica to                \
     * CR_REPLICA_STALE and recover normally but a failure of the local node   \
     * before recovery completed would leave the system unavailable.           \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_UPDATE_2 when all replicas reach the      \
     * CR_REPLICA_STATE_MUTUAL_REDO_DONE state,                                \
     * CR_SHARD_STATE_WAIT_META on lease expiration.                           \
     */                                                                        \
    item(CR_SHARD_STATE_MUTUAL_REDO, mutual_redo,                              \
         CRSSF_META_SOURCE_SELF|CRSSF_RENEW_LEASE|CRSSF_ACCESS_NONE,           \
         cr_shard_mutual_redo_enter(shard_arg), /* no leave */)                \
    /*                                                                         \
     * XXXX: drew 2009-05-26 We should block here until                        \
     * attempts to read or write from the shard so we don't become             \
     * non-authoritative before a subsequent failure                           \
     * on various replicas.                                                    \
     */                                                                        \
    /**                                                                        \
     * @brief Second meta-data update                                          \
     *                                                                         \
     * - For dead replicas convert mutual redo ranges into undo ranges         \
     * - Mark dead authoritative replicas non-authoritative                    \
     * - Mark live replica mutual redo ranges as synchronized                  \
     * - Mark live replicas as open from last + range to infinity              \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_RW on success,                            \
     * CR_SHARD_STATE_WAIT_META on lease expiration.                           \
     */                                                                        \
    item(CR_SHARD_STATE_UPDATE_2, update_2,                                    \
         CRSSF_META_SOURCE_SELF|CRSSF_RENEW_LEASE|CRSSF_ACCESS_NONE,           \
         cr_shard_update_2_enter(shard_arg), /* no leave */)                   \
    /**                                                                        \
     * @brief Read-write access                                                \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_TO_WAIT_META on lease expiration,         \
     * CR_SHARD_STATE_SWITCH_BACK during start of switch back.                 \
     */                                                                        \
    item(CR_SHARD_STATE_RW, rw,                                                \
         CRSSF_META_SOURCE_SELF|CRSSF_RENEW_LEASE|CRSSF_ACCESS_RW,             \
         cr_shard_rw_enter(shard_arg), /* no leave */)                         \
    /**                                                                        \
     * @brief Switch-back                                                      \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_TO_WAIT_META on unsuccessful completion,  \
     * CR_SHARD_STATE_SWITCH_BACK_2 on success.                                \
     */                                                                        \
    item(CR_SHARD_STATE_SWITCH_BACK, switch_back,                              \
         CRSSF_META_SOURCE_SELF|CRSSF_NO_LEASE|CRSSF_ACCESS_NONE,              \
         /* no enter */, /* no leave */)                                       \
    /**                                                                        \
     * @brief Second phase of switch back                                      \
     *                                                                         \
     * Wait for the preferred node to assume ownership or die.  Johann         \
     * promises that the liveness subsystem will never miss a node death       \
     * event although we'll be paranoid and wait at most                       \
     * config->switch_back_timeout_secs out of paranoia.                       \
     * Transitions to CR_SHARD_STATE_TO_WAIT_META on exit.                     \
     */                                                                        \
    item(CR_SHARD_STATE_SWITCH_BACK_2, switch_back_2,                          \
         CRSSF_META_SOURCE_SELF|CRSSF_NO_LEASE|CRSSF_ACCESS_NONE,              \
         cr_shard_switch_back_2_enter(shard_arg),                              \
         cr_shard_switch_back_2_leave(shard_arg))                              \
    /**                                                                        \
     * @brief Read-only access.                                                \
     *                                                                         \
     * Transitions to CR_SHARD_STATE_WAIT_TO_META on lease expiration          \
     */                                                                        \
    item(CR_SHARD_STATE_RO, ro,                                                \
         CRSSF_META_SOURCE_OTHER|CRSSF_NO_LEASE|CRSSF_ACCESS_READ,             \
         /* no enter */, /* no leave */)                                       \
    /**                                                                        \
     * @brief Shutting down                                                    \
     *                                                                         \
     * Transition to CR_SHARD_STATE_SHUTDOWN when pending operations reach 0   \
     */                                                                        \
    item(CR_SHARD_STATE_TO_SHUTDOWN, to_shutdown,                              \
         CRSSF_META_SOURCE_OTHER|CRSSF_NO_LEASE|CRSSF_ACCESS_NONE,             \
         /* no enter */, /* no leave */)                                       \
    /**@brief Stopped */                                                       \
    item(CR_SHARD_STATE_SHUTDOWN, shutdown,                                    \
         CRSSF_META_SOURCE_OTHER|CRSSF_NO_LEASE|CRSSF_ACCESS_NONE,             \
         /* no enter */, /* no leave */)

enum cr_shard_state {
#define item(upper, lower, flags, enter, leave) upper,
    CR_SHARD_STATE_ITEMS(/* none */, /* none */, /* none */)
#undef item
};

enum {
    /** @brief Value for no known local replica in cr_shard.local_replica */
    CR_SHARD_INVALID_LOCAL_REPLICA = -1
};

/*
 * @brief in-core per shard state
 *
 * lock start/lock end comments bracket the pieces which become locked
 * when we switch threading models to accomodate a synchronous client
 * API which looks like the flash API.
 *
 * Is allocated on a cr->meta_storage call to cr_meta_update_cb.
 *
 * XXX: drew 2009-08-09 For SDF_REPLICATION_V1_2_WAY and
 * SDF_REPLICATION_V1_N_PLUS_1 we maintain a #cr_shard structure
 * for each intra-node vip group in the system with #cr_shard
 * objects that have different vip_group_id fields but the same
 * sguid field.  Some work may be necessary to route IO requests to a single
 * #copy_replicator instance if we move the data path for these
 * replication types.
 *
 * XXX: drew 2009-06-10 We want to add shard (and maybe replica?) level
 * statistics so we can monitor recovery progress and normal operation.
 */
struct cr_shard {
    /** @brief parent */
    struct copy_replicator *cr;

    /**
     * @brief Shard id
     *
     * #SDF_SHARDID_INVALID when unset, as when a remote update was received
     * for #vip_group_id before a local HFCSH message was received for
     * shard creation.
     */
    SDF_shardid_t sguid;

#ifdef notyet
    /*
     * FIXME: drew 2009-08-11 May need to handle cname which disagree with
     * the current (remote) data SDF_REPLICATION_V1_N_PLUS_1 and
     * SDF_REPLICATION_V1_2_WAY as with sguid.
     *
     * I think these are the same, but we need to confirm.
     */
    /* @brief Container guid.  */
    SDF_cguid_t cguid;

    /** @brief Container name */
    char cname[MAX_CNAME_SIZE + 1];
#endif

    /**
     * @brief Intra node vip group ID associated with this #cr_shard
     *
     * #VIP_GROUP_ID_INVALID for no gid.
     */
    int vip_group_id;

#ifdef notyet
    /* XXX: drew 2009-08-12 we derive this from vip_group_id */
    /** @brief Inter node vip group group id */
    int vip_group_group_id;
#endif

    /** @brief Replication type */
    SDF_replication_t replication_type;

#ifdef notyet
    /*
     * XXX: drew 2009-08-10 We only attempt to acquire a lease on the
     * allowed number of intra node VIP groups at once.  Where
     * a failure occurs on one group due to races with other nodes
     * we want to retry on the ones where there timers expired and
     * we didn't attempt acquisition as soon as allowed.  Maintain
     * state for that.
     */
    /** @brief Time at which we attempt to acquire the lease */
    struct timeval attempt_acquire;
#endif

/* lock start */
    /** @brief current state */
    enum cr_shard_state state;

    /**
     * @brief operational flags associated with current state
     * from #cr_shard_state_flags
     */
    int state_flags;

/* Also add current live replicas to locked state */

/* lock end */
    /** @brief Current shard meta data */
    struct cr_shard_meta *shard_meta;

    /**
     * @brief Next proposed shard meta
     *
     * During long running recovery processes, shard meta-data will be
     * updating on both recovery progress and lease renewals.  We guarantee
     * that the #replicator_meta_storage system receives causally related
     * updates by running serializing schedulers in the recovery +
     * meta-storage code and deriving the next meta-data from the previous
     * proposed value stored here.
     */
    struct cr_shard_meta *proposed_shard_meta;

    /**
     * @brief Local vip meta-data
     *
     * Used only for V1 VIP groups, notably to keep track of the V1
     * shard state (RECOVERED or not) in each one.
     *
     * For v1 we only support shard deletion, but not vip group
     * deletion.
     */
    struct cr_vip_meta *local_vip_meta;

    /** @brief Timer for initial lease acquisition */
    struct plat_event *lease_acquisition_delay_event;

    /** @brief Name for event to ease tracking */
    char *delay_lease_acquisition_event_name;

    /** @brief Name for event to ease tracking */
    char *lease_renewal_event_name;

    /** @brief Timer for periodic lease renewal - merge with above? */
    struct plat_event *lease_renewal_event;

    /** @brief When the lease expires in absolute system time */
    struct timeval lease_expires;

    /** @brief Timer for switch-back timeout */
    struct plat_event *switch_back_timeout_event;

    /**
     * @brief State for each currently pending recovery
     *
     * We treat each replica's recovery as an independant process.  For
     * MUTUAL_REDO the recovery process acts as a source which writes
     * to all other live replicas.
     */
    struct cr_replica *replicas[SDF_REPLICATION_MAX_REPLICAS];

    /** @brief Which replica is ours */
    int local_replica;

    /**
     * @brief Have not seen a home node yet
     *
     * On initial restart when not the preferred authoritative node
     * wait for it to come up first.  Not on initial restart take over
     * immediately when the first choice is CR_NODE_DEAD and this node
     * is the first altnernative.  Otherwise wait.
     */
    unsigned after_restart : 1;

    /**
     * @brief This node recovered the shard and dropped ownership
     *
     * The coordinator/home node must migrate following a failure
     * in order to preserve load balancing.
     */
    unsigned after_recovery : 1;


/* lock start */
    /** @brief Sequence number */
    uint64_t seqno;

    /**
     * @brief Last consecutive sequence number completed on all live replicas;
     */
    uint64_t last_consecutive_complete_live_seqno;

    /** @brief Last consecutive complete seqno at time of sync start */
    uint64_t last_sync_start_seqno;

    /** @brief Last consecutive complete seqno at time of sync complete */
    uint64_t last_consecutive_sync_complete_live_seqno;

    /** @brief Last consecutive synced sequence number on all replicas */
    uint64_t last_consecutive_synced_all_seqno;

    /**
     * @brief FIFO of all operations
     *
     * Insert at tail, remove at head.  Operations flow through a FIFO
     * list since tombstone garbage collection is simplest (especially
     * with off-line replicas) when we provide a single
     * last_consecutive_synced_seqno.
     *
     * FIXME: drew 2009-06-18 Send remote writes through op_list with a
     * lock which is released as last_consecutive_complete_live_seqno
     * advances past them so that we don't make data visible which
     * may not be visible in a failure.
     */
    TAILQ_HEAD(, cr_op) op_list;

    /**
     * @brief Next operation to issue
     *
     * It seems least painful to maintain a single op_list which
     * is partitioned into portions which are pending (due to the
     * outstanding request window) and which have already been
     * issued.
     */
    struct cr_op *op_issue;

    /** @brief Last consecutive complete operation */
    struct cr_op *op_consecutive_complete;

    /** @brief Operation of last sync start, all older are complete */
    struct cr_op *op_sync_start;

    /** @brief Operation of newest sync complete, all older are synced */
    struct cr_op *op_sync_complete;

    /**
     * @brief Key based locking
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
    struct replicator_key_lock_container *lock_container;

/* lock end */
    unsigned notification_running : 1;

    /** @brief Pending notifies for this shard */
    TAILQ_HEAD(, cr_shard_notify_state) pending_notification_list;

    /** @brief Reference count by everything, lock free */
    int ref_count;

/*
 * lock ? As long as we're keeping the operations on a locked queue
 * it doesn't help to be unlocked.
 */
    /**
     * @brief Number of operations which are pending, lock free
     *
     * Notably, these are operations which will prevent a transition out of
     * CR_SHARD_STATE_TO_DEAD to CR_SHARD_STATE_DEAD.
     */
    int op_count;

    /** @brief Count of pending meta-data put operations */
    int put_count;
/* lock ? end */

    /** @brief Stats */
    struct cr_shard_stat_counters *stat_counters;

    /** @brief Entry in copy_replicator shard_list */
    LIST_ENTRY(cr_shard) shard_list_entry;
};

struct cr_shard_stat_counters {
#define item(lower) int64_t lower;
    SDF_REPLICATOR_SHARD_STAT_COUNTER_ITEMS()
#undef item
};

struct cr_shard_stat_pull {
#define item(lower) int64_t lower;
    SDF_REPLICATOR_SHARD_STAT_PULL_ITEMS()
#undef item
};

/**
 * @brief Operational flags associated with given replica states
 *
 * An unset value is provided for each option so we can programatically
 * sanity check that each table entry has a value assigned as opposed
 * to finding out the hard way that we missed one.
 */
enum cr_replica_state_flags {
    CRSF_ALLOW_CLIENT_NONE = 1 << 0,
    CRSF_ALLOW_CLIENT_WO_BLOCK = 1 << 2,
    CRSF_ALLOW_CLIENT_WO = 1 << 3,
    CRSF_ALLOW_CLIENT_RW = 1 << 4,
    CRSF_ALLOW_CLIENT_RO = 1 << 5,

    CRSF_NOT_ITERATING = 1 << 6,
    CRSF_ITERATING = 1 << 7
};

/**
 * @brief Table for CR replica state
 *
 * @param shard_arg <IN> struct cr_shard *
 * @param state_from <IN> state transitioning from
 * @param state_to <IN> state transitioning to
 *
 * The enter function may advance to a subsequent state, the leave function
 * may not
 *
 * item(caps, lower, flags, enter, leave)
 * @param caps <IN> enumeration value
 * @param lower <IN> pretty-printed value
 * @param flags <IN> operational flags for state
 * @param enter <IN> lambda applied on state entry
 * @param leave <IN> lambda applied on state exit$
 * @param event_stuff <IN> space delimited list of actions
 *     handle(event, lambda)
 *     ignore(event)
 * The CR_REPLICA_EVENT_ prefix must be omitted from event specifications in
 * event_stuff.
 */

/*
 * XXX: drew 2009-06-16 The state machine is getting too distributed,
 */
#define CR_REPLICA_STATE_ITEMS(replica_arg, state_from, state_to) \
    /**                                                                        \
     * @brief Intial state                                                     \
     * Immediately transitions to CR_REPLICA_STATE_TO_DEAD or                  \
     * CR_REPLICA_STATE_LIVE_OFFLINE                                           \
     */                                                                        \
    item(CR_REPLICA_STATE_INITIAL, initial,                                    \
         CRSF_ALLOW_CLIENT_NONE|CRSF_NOT_ITERATING,                            \
         /* no enter */, /* no leave */)                                       \
    /**                                                                        \
     * @brief Waiting for pending operations to complete before becoming dead  \
     *                                                                         \
     * Transition to CR_REPLICA_STATE_DEAD when everything completes unless    \
     * receiving a shutdown event for an immediate transition to               \
     * CR_REPLICA_STATE_TO_SHUTDOWN.                                           \
     */                                                                        \
    item(CR_REPLICA_STATE_TO_DEAD, to_dead,                                    \
         CRSF_ALLOW_CLIENT_NONE|CRSF_NOT_ITERATING,                            \
         /* no enter */, /* no leave */)                                       \
    /**                                                                        \
     * @brief Dead                                                             \
     *                                                                         \
     * Transitions to CR_REPLICA_STATE_LIVE_OFFLINE when a liveness event is   \
     * detected,                                                               \
     * Shutdown events enter CR_REPLICA_STATE_TO_SHUTDOWN                      \
     */                                                                        \
    item(CR_REPLICA_STATE_DEAD, dead,                                          \
         CRSF_ALLOW_CLIENT_NONE|CRSF_NOT_ITERATING,                            \
         /* no enter */, /* no leave */)                                       \
    /**                                                                        \
     * @brief Live, but being ignored                                          \
     *                                                                         \
     * We wait in this state until the local node becomes the master and       \
     * causes us to transition to CR_REPLICA_STATE_MUTUAL_REDO or              \
     * CR_REPLICA_STATE_UNDO.                                                  \
     *                                                                         \
     * Node dead shutdown events enter CR_REPLICA_STATE_TO_DEAD                \
     * Shutdown events enter CR_REPLICA_STATE_TO_SHUTDOWN                      \
     */                                                                        \
    item(CR_REPLICA_STATE_LIVE_OFFLINE, live_offline,                          \
         CRSF_ALLOW_CLIENT_NONE|CRSF_NOT_ITERATING,                            \
         /* no enter */, /* no leave */)                                       \
    /**                                                                        \
     * @brief Mutual redo                                                      \
     *                                                                         \
     * Redo all IOs within +/- outstanding                                     \
     *                                                                         \
     * Transitions to CR_REPLICA_STATE_RECOVERED on success                    \
     * Node dead shutdown events enter CR_REPLICA_STATE_TO_DEAD                \
     * Shutdown events enter CR_REPLICA_STATE_TO_SHUTDOWN                      \
     */                                                                        \
    item(CR_REPLICA_STATE_MUTUAL_REDO, mutual_redo,                            \
         CRSF_ALLOW_CLIENT_NONE|CRSF_ITERATING,                                \
         cr_replica_mutual_redo_enter(replica_arg), /* no leave */)            \
    /**                                                                        \
     * @brief Mutual done sourcing data                                        \
     *                                                                         \
     * The replica remains in this state until all replicas have completed     \
     * their mutual redo operations and the outer state machine has made       \
     * the transition to CR_SHARD_STATE_RW. On success the transition          \
     * is to CR_REPLICA_STATE_RECOVERED.                                       \
     * Node dead shutdown events enter CR_REPLICA_STATE_TO_DEAD                \
     * Shutdown events enter CR_REPLICA_STATE_TO_SHUTDOWN                      \
     */                                                                        \
    item(CR_REPLICA_STATE_MUTUAL_REDO_SCAN_DONE, mutual_redo_scan_done,        \
         CRSF_ALLOW_CLIENT_NONE|CRSF_NOT_ITERATING,                            \
         /* no enter */, /* no leave */)                                       \
    /**                                                                        \
     * @brief Undo state                                                       \
     *                                                                         \
     * Iterate over undo range and perform compensating actions to all         \
     * writeable replicas.  On no remaining undo segments transition to        \
     * CR_REPLICA_STATE_UPDATE_AFTER_UNDO                                      \
     * Node dead shutdown events enter CR_REPLICA_STATE_TO_DEAD                \
     * Shutdown events enter CR_REPLICA_STATE_TO_SHUTDOWN                      \
     */                                                                        \
    item(CR_REPLICA_STATE_UNDO, undo,                                          \
         CRSF_ALLOW_CLIENT_NONE|CRSF_ITERATING,                                \
         cr_replica_undo_enter(replica_arg), /* no leave */)                   \
    /**                                                                        \
     * @brief Update that follows undo                                         \
     *                                                                         \
     * - Begin blocking new IOs on writes completing to this shard             \
     * - Make open range starting at current sequence number                   \
     * - Make redo range from last range end to open range                     \
     *                                                                         \
     * On success transition to CR_REPLICA_STATE_REDO                          \
     * Node dead shutdown events enter CR_REPLICA_STATE_MARK_FAILED            \
     * Shutdown events enter CR_REPLICA_STATE_TO_SHUTDOWN                      \
     */                                                                        \
    item(CR_REPLICA_STATE_UPDATE_AFTER_UNDO, update_after_undo,                \
         CRSF_ALLOW_CLIENT_WO_BLOCK|CRSF_NOT_ITERATING,                        \
         cr_replica_update_after_undo_enter(replica_arg), /* no leave */)      \
    /**                                                                        \
     * @brief Redo                                                             \
     *                                                                         \
     * Iterate over all potentially missing entries in the local authoritative \
     * replica for the first REDO range.  On completion transition to          \
     * CR_REPLICA_STATE_UPDATE_AFTER_REDO.                                     \
     * Node dead shutdown events enter CR_REPLICA_STATE_MARK_FAILED            \
     * Shutdown events enter CR_REPLICA_STATE_TO_SHUTDOWN                      \
     */                                                                        \
    item(CR_REPLICA_STATE_REDO, redo,                                          \
         CRSF_ALLOW_CLIENT_WO|CRSF_ITERATING,                                  \
         cr_replica_redo_enter(replica_arg), /* no leave */)                   \
    /**                                                                        \
     * @brief Update that follows Redo                                         \
     *                                                                         \
     * Where this was the last redo range we allow                             \
     * last_consecutive_synced_seqno to advance normally and transition to     \
     * CR_REPLICA_STATE_RECOVERED.  Otherwise success goes back to             \
     * CR_REPLICA_STATE_REDO for the next range.                               \
     * Node dead shutdown events enter CR_REPLICA_STATE_MARK_FAILED            \
     * Shutdown events enter CR_REPLICA_STATE_TO_SHUTDOWN                      \
     */                                                                        \
    item(CR_REPLICA_STATE_UPDATE_AFTER_REDO, update_after_redo,                \
         CRSF_ALLOW_CLIENT_WO|CRSF_NOT_ITERATING,                              \
         cr_replica_update_after_redo_enter(replica_arg), /* no leave */)      \
    /**                                                                        \
     * @brief Recovered                                                        \
     * Node dead shutdown events enter CR_REPLICA_STATE_MARK_FAILED            \
     * Shutdown events enter CR_REPLICA_STATE_TO_SHUTDOWN                      \
     */                                                                        \
    item(CR_REPLICA_STATE_RECOVERED, recovered,                                \
         CRSF_ALLOW_CLIENT_RW|CRSF_NOT_ITERATING,                              \
         cr_replica_recovered_enter(replica_arg), /* no leave */)              \
    /**                                                                        \
     * @brief Mark failed replica synchronization                              \
     *                                                                         \
     * Transition to CR_REPLICA_STATE_TO_DEAD unless receiving a shutdown      \
     * event for an immediate transition to CR_REPLICA_STATE_TO_SHUTDOWN       \
     */                                                                        \
    item(CR_REPLICA_STATE_MARK_FAILED, mark_failed,                            \
         CRSF_ALLOW_CLIENT_NONE|CRSF_NOT_ITERATING,                            \
         cr_replica_mark_failed_enter(replica_arg), /* no leave */)            \
    /**                                                                        \
     * @brief Shutting down                                                    \
     *                                                                         \
     * Transition to CR_SHARD_STATE_SHUTDOWN when pending operations reach 0   \
     */                                                                        \
    item(CR_REPLICA_STATE_TO_SHUTDOWN, to_shutdown,                            \
         CRSF_ALLOW_CLIENT_NONE|CRSF_NOT_ITERATING,                            \
         cr_replica_check_state(replica_arg), /* no leave */)                  \
    /**@brief Stopped */                                                       \
    item(CR_REPLICA_STATE_SHUTDOWN, shutdown,                                  \
         CRSF_ALLOW_CLIENT_NONE|CRSF_NOT_ITERATING,                            \
         /* no enter */, /* no leave */)

enum cr_replica_state {
#define item(caps, lower, flags, enter, leave) caps,
    CR_REPLICA_STATE_ITEMS(/* none */, /* none */, /* none */)
#undef item
};

struct cr_replica {
    /** @brief Parent */
    struct cr_shard *shard;

    /** @brief Index into shard->replicas */
    int index;

    /** @brief Node on which replica lives */
    vnode_t node;

    /** @brief State of this replica */
    enum cr_replica_state state;

    /** @brief Specific characteristics from #cr_replica_state_flags */
    int state_flags;

    /** @brief Replica over which iteration is performed */
    int iterate_replica;

    /**
     * @brief Node on which replica being iterated lives
     *
     * For CR_REPLICA_STATE_UNDO and CR_REPLICA_STATE_MUTUAL_REDO it's the same
     * as #node.  In CR_REPLICA_STATE_REDO it's the current home node which is
     * the same as shard->cr->config.my_node.
     */
    vnode_t iterate_node;

    /** @brief Range being operated on */
    struct cr_persistent_shard_range current_range;

    /** @brief Index of current range */
    int current_range_index;

    /** @brief Current iteration state */
    struct flashGetIterationOutput *get_iteration_cursors;

    /** @brief Offset into get_iteration_cursors */
    int cursor_offset;

    /** @brief Next set of cursors so pipeline doesn't stall */
    struct flashGetIterationOutput *next_iteration_cursors;

    /** @brief Resume data */
    void *get_iteration_cursors_resume_data;

    /** @brief Length of resume data */
    int get_iteration_cursors_resume_data_len;

    /** @brief Number of cursor fetches pending (0 or 1)  */
    int get_iteration_cursors_pending;

    /** @brief No cursors remain for this range */
    int get_iteration_cursors_eof;

    /** @brief State to transition to after current completes */
    enum cr_replica_state next_state;

    /** @brief Number of recovery operations other than get cursor operations */
    int recovery_op_pending_count;

    /** @brief Source recovery ops associated with this replica */
    TAILQ_HEAD(, cr_recovery_op) src_recovery_op_list;

    /** @brief Destination recovery ops associated with this replica */
    TAILQ_HEAD(, cr_recovery_op) dest_recovery_op_list;

    /** @brief Total reference count on replica */
    int ref_count;
};

enum cr_node_state {
    /** @brief Down! */
    CR_NODE_DEAD,

    /** @brief Up! */
    CR_NODE_LIVE
};

struct cr_node {
    /** @brief Current state */
    enum cr_node_state state;

    /** @brief When this node was last alive */
    struct timeval last_live;
};

/**
 * @brief Per protocol operation (from local home node code)
 *
 * #cr_op objects are created by the #copy_replicator on message receipt
 * with an initial reference count of 1.  The reference count is incremented
 * as additional operations are added; when it reaches 0 an accumulated
 * response will be sent.
 */
struct cr_op {
    /** @brief Parent replicator */
    struct copy_replicator *cr;

    /** @brief Shard */
    struct cr_shard *shard;

    /** @brief Flags from #cr_op_flags */
    int flags;

    /** @brief Original request wrapper */
    struct sdf_msg_wrapper *request_wrapper;

    /** @brief Original request message (SDF header) */
    struct sdf_msg *request_msg;

    /** @brief Original request protocol message (from home node) */
    SDF_protocol_msg_t *request_pm;

    /** @brief Original request protocol message information for logging */
    SDF_Protocol_Msg_Info_t *request_pmi;

    /**
     * @brief Lock owned by this #cr_op
     * NULL for messages with request_pm->flags & f_internal_lock
     */
    struct replicator_key_lock *lock;

    /**
     * @brief Response to original messsage
     *
     * NULL before assignment.  Since this may be a forward from another
     * node the current source and destination node, service tuples may
     * not be valid.
     */
    struct sdf_msg_wrapper *response_wrapper;

    /** @brief Status of request - whether good (SDF_SUCCESS) or bad */
    SDF_status_t response_status;

    /** @brief Where da ta needs to be sent */
    enum cr_fanout fanout;

    /** @brief Sequence number assigned */
    uint64_t seqno;

    /** @brief Accumulated status */
    int status;

    /** @brief Entry in parent->op_list */
    LIST_ENTRY(cr_op) op_list;

    /** @brief number of references which remain before responding to caller */
    int response_ref_count;

    /**
     * @brief Reference count
     *
     * When the reference count hits zero, the request is done and a response
     * is sent.
     *
     * XXX: drew 2009-05-31 This needs to be replaced with #response_ref_count
     */
    int ref_count;
};

enum cr_replication_type_flags {
    /* What to index #cr_shard off */

    /** @brief No #cr_shard structure */
    CRTF_INDEX_NONE = 1 << 0,
    /** @brief #cr_shard indexed by CR_SHARD_ID */
    CRTF_INDEX_SHARDID = 1 << 1,
    /** @brief #cr_shard indexed by VIP group */
    CRTF_INDEX_VIP_GROUP_ID = 1 << 2,

    /* How to store it */

    /** @brief This type is not stored by rms */
    CRTF_META_NONE = 1 << 3,
    /** @brief Stored in a single supernode */
    CRTF_META_SUPER = 1 << 4,
    /** @brief Replicated via Paxos */
    CRTF_META_PAXOS = 1 << 5,
    /** @brief Distributed (weakly consistent) storage */
    CRTF_META_DISTRIBUTED = 1 << 6,

    /* XXX: drew 2009-08-07 TRAFFIC is probably a bad name */

    /* What copy_replicator is managing */

    /** @brief copy_replicator is not being used for this type */
    CRTF_MANAGE_NONE = 1 << 7,
    /**
     * @brief copy_replicator is used for meta-data
     *
     * As of 2009-08-07, this meant that the cr_shard structure and related
     * state machines were being used.
     */
    CRTF_MANAGE_META = 1 << 8,
    /**
     * @brief copy_replicator is used for data
     *
     * When not set, only HFCSH and HFDSH messages should be received by
     * the copy_replicator for the shard in question.
     */
    CRTF_MANAGE_DATA = 1 << 9,
    /** @brief copy_replicator is used for data and meta-data */
    CRTF_MANAGE_ALL = CRTF_MANAGE_META | CRTF_MANAGE_DATA,

    /*  What lease scheme is used */

    /** @brief No lease on create */
    CRTF_CREATE_LEASE_NONE = 1 << 10,
    /** @brief Immediately request lease on create */
    CRTF_CREATE_LEASE_IMMEDIATE = 1 << 11,
    /** @brief Delay lease acquisition on create */
    CRTF_CREATE_LEASE_DELAY = 1 << 12
};

/**
 * @brief Handling of given replication type
 * item(type, flags)
 */
#define CR_REPLICATION_TYPE_ITEMS() \
    item(SDF_REPLICATION_NONE,                                                 \
         CRTF_INDEX_NONE|CRTF_META_NONE|CRTF_MANAGE_NONE|                      \
         CRTF_CREATE_LEASE_NONE)                                               \
    item(SDF_REPLICATION_SIMPLE,                                               \
         CRTF_INDEX_NONE|CRTF_META_NONE|CRTF_MANAGE_DATA|                      \
         CRTF_CREATE_LEASE_NONE)                                               \
    item(SDF_REPLICATION_META_SUPER_NODE,                                      \
         CRTF_INDEX_SHARDID|CRTF_META_SUPER|CRTF_MANAGE_ALL|                   \
         CRTF_CREATE_LEASE_IMMEDIATE)                                          \
    item(SDF_REPLICATION_META_EXTERNAL_AUTHORITY,                              \
         CRTF_INDEX_SHARDID|CRTF_META_NONE|CRTF_MANAGE_ALL|                    \
         CRTF_CREATE_LEASE_IMMEDIATE)                                          \
    item(SDF_REPLICATION_META_CONSENSUS,                                       \
         CRTF_INDEX_SHARDID|CRTF_META_PAXOS|CRTF_MANAGE_ALL|                   \
         CRTF_CREATE_LEASE_IMMEDIATE)                                          \
    item(SDF_REPLICATION_V1_2_WAY,                                             \
         CRTF_INDEX_VIP_GROUP_ID|CRTF_META_DISTRIBUTED|CRTF_MANAGE_META|       \
         CRTF_CREATE_LEASE_DELAY)                                              \
    item(SDF_REPLICATION_V1_N_PLUS_1,                                          \
         CRTF_INDEX_VIP_GROUP_ID|CRTF_META_DISTRIBUTED|CRTF_MANAGE_META|       \
         CRTF_CREATE_LEASE_DELAY)                                              \
    item(SDF_REPLICATION_CONSENSUS,                                            \
         CRTF_INDEX_SHARDID|CRTF_META_PAXOS|CRTF_MANAGE_ALL|                   \
         CRTF_CREATE_LEASE_IMMEDIATE)

/** @brief Argument to #cr_shard_get_preference */
enum cr_shard_get_preference_type {
    /** @brief Get preference for normal operation */
    CR_SHARD_GET_PREFERENCE_NORMAL,
    /**
     * @brief Get preference for startup delay
     *
     * Preference used in state CR_SHARD_STATE_DELAY_LEASE_ACQUISITION
     * for delay length.  See #sdf_replicator_config.initial_preference
     * in replicator.h for an explanation.
     */
    CR_SHARD_GET_PREFERENCE_START_DELAY
};

static int cr_start(struct sdf_replicator *self);
static SDF_status_t
cr_get_op_meta(struct sdf_replicator *self,
               const struct SDF_container_meta *container_meta,
               SDF_shardid_t shard, struct sdf_replication_op_meta *op_meta);

static struct cr_notifier *cr_add_notifier(struct sdf_replicator *self,
                                           sdf_replicator_notification_cb_t cb);
static void cr_remove_notifier(struct sdf_replicator *self,
                               struct cr_notifier *handle);
static SDF_status_t cr_get_container_stats(struct sdf_replicator *self,
                                           struct sdf_replicator_shard_stats *stats,
                                           SDF_cguid_t cguid);
static void cr_get_container_stats_impl(plat_closure_scheduler_t *context,
                                        void *env, fthMbox_t *status_mbx,
                                        struct sdf_replicator_shard_stats *stats,
                                        SDF_cguid_t cguid);
static void cr_command_async(struct sdf_replicator *self,
                             SDF_shardid_t sguid, const char *command_arg,
                             sdf_replicator_command_cb_t cb);
static void cr_command_async_impl(plat_closure_scheduler_t *context, void *env,
                                  SDF_shardid_t sguid, char *command,
                                  sdf_replicator_command_cb_t cb);
static void cr_command_recovered(struct copy_replicator *cr, SDF_shardid_t sguid,
                                 sdf_replicator_command_cb_t cb);

static void cr_recv_msg(plat_closure_scheduler_t *context, void *env,
                        struct sdf_msg_wrapper *wrapper);
static void cr_recv_msg_replication(plat_closure_scheduler_t *context, void *env,
                                    struct sdf_msg_wrapper *wrapper);
static void cr_node_live(plat_closure_scheduler_t *context, void *env,
                         vnode_t pnode, struct timeval last_live);
static void cr_node_dead(plat_closure_scheduler_t *context, void *env,
                         vnode_t pnode);
static void cr_shards_signal_liveness_change(struct copy_replicator *cr,
                                             vnode_t pnode,
                                             enum cr_node_state state);
static void cr_shutdown(plat_closure_scheduler_t *context, void *env,
                        sdf_replicator_shutdown_cb_t shutdown_closure);
static void cr_ref_count_dec(struct copy_replicator *cr);
static void cr_signal_shard_shutdown_complete(struct copy_replicator *cr);
static void cr_rms_stopped(struct plat_closure_scheduler *context, void *env);
static void cr_closure_scheduler_stopped(struct plat_closure_scheduler *context,
                                         void *env);
static void cr_meta_update_cb(struct plat_closure_scheduler *context, void *env,
                              SDF_status_t status,
                              struct cr_shard_meta *shard_meta,
                              struct timeval lease_expires);
static struct cr_shard *cr_get_or_alloc_shard(struct copy_replicator *cr,
                                              SDF_shardid_t sguid,
                                              vip_group_id_t group_id,
                                              SDF_replication_t rep_type);

/* Shard functions */
static void cr_shard_signal_liveness_change(struct cr_shard *shard,
                                            vnode_t node,
                                            enum cr_node_state state);
static void cr_shard_set_state(struct cr_shard *shard,
                               enum cr_shard_state next_state);
static void cr_shard_signal_replica_state(struct cr_shard *shard,
                                          struct cr_replica *replica_arg);
static void cr_shard_signal_replica_state_cb(plat_closure_scheduler_t *context,
                                             void *env,
                                             struct cr_replica *replica_arg);
static void cr_shard_check_state(struct cr_shard *shard,
                                 struct cr_replica *replica);
static void cr_shard_check_state_wait_meta(struct cr_shard *shard);
static void cr_shard_wait_meta_update_for_both(struct cr_shard *shard);
static void cr_shard_wait_meta_update_for_meta(struct cr_shard *shard);

static void cr_shard_check_state_delay_lease_acquisition(struct cr_shard *shard, struct cr_replica *replica_arg);
static void cr_shard_check_state_delay_lease_acquisition_for_both(struct cr_shard *shard,
                                                                  struct cr_replica *replica_arg);
static void cr_shard_check_state_switch_back_2(struct cr_shard *shard);
static void cr_shard_meta_update_external(struct cr_shard *shard,
                                          struct cr_shard_meta *shard_meta_arg,
                                          struct timeval lease_expires);
static void cr_shard_alloc_replica(struct cr_shard *shard, int index,
                                   struct cr_shard_replica_meta *replica_meta);
static int cr_shard_vip_group_lease_allowed(struct cr_shard *shard);
static void cr_shard_vip_group_check(struct cr_shard *shard);
static void cr_shard_create_enter(struct cr_shard *shard);
static void cr_shard_delay_lease_acquisition_enter(struct cr_shard *shard);
static void cr_shard_acquire_delay_done_cb(plat_closure_scheduler_t *context,
                                           void *env, struct plat_event *event);
static void cr_shard_free_event(struct cr_shard *shard,
                                struct plat_event *event);
static void cr_shard_event_free_cb(plat_closure_scheduler_t *context,
                                   void *env);
static void cr_shard_delay_lease_acquisition_leave(struct cr_shard *shard);
static void cr_shard_request_lease_enter(struct cr_shard *shard);
static void cr_shard_request_lease_cb(struct plat_closure_scheduler *context,
                                      void *env, SDF_status_t status);
static void cr_shard_mutual_redo_enter(struct cr_shard *shard);
static void cr_shard_get_seqno_enter(struct cr_shard *shard);
static void cr_shard_get_seqno_cb(struct plat_closure_scheduler *context,
                                  void *env, SDF_status_t status,
                                  uint64_t seqno);
static void cr_shard_update_1_enter(struct cr_shard *shard);
static void cr_shard_update_1_cb(struct plat_closure_scheduler *context,
                                 void *env, SDF_status_t status);
static void cr_shard_mutual_redo_enter(struct cr_shard *shard);
static void cr_shard_update_2_enter(struct cr_shard *shard);
static void cr_shard_update_2_cb(struct plat_closure_scheduler *context,
                                 void *env, SDF_status_t status);
static void cr_shard_rw_enter(struct cr_shard *shard);
static void cr_shard_rw_enter_for_v1_2way(struct cr_shard *shard);
static void cr_shard_rw_enter_for_data(struct cr_shard *shard);
static void cr_shard_switch_back_2_enter(struct cr_shard *shard);
static void cr_shard_switch_back_timeout_done_cb(plat_closure_scheduler_t *,
                                                 void *env,
                                                 struct plat_event *event);
static void cr_shard_switch_back_2_leave(struct cr_shard *shard);
static void cr_shard_yield_enter(struct cr_shard *shard);

static void cr_shard_put_meta(struct cr_shard *shard,
                              cr_shard_put_meta_cb_t cb);
static void cr_shard_put_meta_cb(struct plat_closure_scheduler *context,
                                 void *env, SDF_status_t status,
                                 struct cr_shard_meta *shard_meta,
                                 struct timeval lease_expires);
static void cr_shard_self_update_meta_common(struct cr_shard *shard,
                                             SDF_status_t status,
                                             struct cr_shard_meta *shard_meta,
                                             struct timeval lease_expires);
static void cr_shard_update_meta(struct cr_shard *shard, SDF_status_t status,
                                 struct cr_shard_meta *shard_meta,
                                 struct timeval lease_expires);
static void cr_shard_update_proposed_shard_meta(struct cr_shard *shard);
static void cr_shard_ref_count_dec(struct cr_shard *shard);
static void cr_shard_op_count_dec(struct cr_shard *shard);
static void cr_shard_shutdown(struct cr_shard *shard);
static void cr_shard_lease_renewal_reset(struct cr_shard *shard);
static void cr_shard_lease_renewal_cb(plat_closure_scheduler_t *context,
                                      void *env, struct plat_event *event);
static void cr_shard_lease_renew(struct cr_shard *shard);
static void cr_shard_lease_renewal_cancel(struct cr_shard *shard);
static void cr_shard_notify(struct cr_shard *shard, cr_shard_notify_cb_t cb);
static void cr_shard_notify_do(struct cr_shard *shard);
static void cr_shard_notify_complete_cb(plat_closure_scheduler_t *context,
                                        void *env);
static void cr_shard_notify_ref_count_dec(struct cr_shard_notify_state *state);

static enum sdf_replicator_access cr_shard_get_access(struct cr_shard *shard);
static SDF_status_t cr_shard_get_stats(struct cr_shard *shard,
                                       struct sdf_replicator_shard_stats *stats);
static SDF_status_t cr_shard_get_op_meta(struct cr_shard *shard,
                                         struct sdf_replication_op_meta *op_meta);
static SDF_status_t cr_shard_get_preference(struct cr_shard *shard,
                                            vnode_t node,
                                            enum cr_shard_get_preference_type,
                                            int *numerator_ptr,
                                            int *denominator_ptr);
static void cr_shard_command_recovered(struct cr_shard *shard,
                                       sdf_replicator_command_cb_t cb);

static int cr_shard_state_to_flags(enum cr_shard_state state)
    __attribute__((const));
static const char *cr_shard_state_to_string(enum cr_shard_state state)
    __attribute__((const));
static void cr_shard_state_validate(enum cr_shard_state state);
static void __attribute__((constructor)) cr_shard_states_validate();

/* Replica functions */
static void cr_replica_signal_liveness_change(struct cr_replica *replica,
                                              enum cr_node_state state);
static void cr_replica_set_state(struct cr_replica *replica,
                                 enum cr_replica_state next_state);
static void cr_replica_check_state(struct cr_replica *replica);
static void cr_replica_mutual_redo_enter(struct cr_replica *replica);
static void cr_replica_undo_enter(struct cr_replica *replica);
static void cr_replica_update_after_undo_enter(struct cr_replica *replica);
static void cr_replica_redo_enter(struct cr_replica *replica);
static void cr_replica_update_after_redo_enter(struct cr_replica *replica);
static void cr_replica_recovery_update_cb(struct plat_closure_scheduler *context,
                                          void *env, SDF_status_t status);
static void cr_replica_recovered_enter(struct cr_replica *replica);
static void cr_replica_mark_failed_enter(struct cr_replica *replica);
static void cr_replica_mark_failed_cb(struct plat_closure_scheduler *context,
                                      void *env, SDF_status_t status);
static void cr_replica_next_iteration(struct cr_replica *replica,
                                      int iterate_replica,
                                      enum cr_replica_range_type range_type,
                                      enum cr_replica_state after_iterate);
static void cr_replica_start_iteration(struct cr_replica *replica,
                                       int iterate_replica, int range_index,
                                       enum cr_replica_state after_iterate);
static void cr_replica_do_recovery(struct cr_replica *replica);
static void cr_replica_get_iteration_cursors_cb(struct plat_closure_scheduler *context,
                                                void *env, SDF_status_t status,
                                                struct flashGetIterationOutput *output);
static void cr_replica_get_by_cursor_cb(struct plat_closure_scheduler *context,
                                        void *env,
                                        struct sdf_msg_wrapper *wrapper);
static void cr_replica_redo(struct cr_replica *replica,
                            struct sdf_msg_wrapper *wrapper);
static void cr_replica_redo_op(struct cr_replica *dest_replica,
                               struct cr_replica *src_replica,
                               struct sdf_msg_wrapper *request);
static void cr_replica_redo_response(struct plat_closure_scheduler *context,
                                     void *env,
                                     struct sdf_msg_wrapper *response);
static void cr_replica_undo_op(struct cr_replica *replica,
                               struct sdf_msg_wrapper *wrapper);
static void cr_replica_undo_op_lock_cb(struct plat_closure_scheduler *context,
                                       void *env, SDF_status_t status_arg,
                                       struct replicator_key_lock *key_lock);
static void cr_replica_undo_op_authoritative_get_cb(struct plat_closure_scheduler *context,
                                                    void *env,
                                                    struct sdf_msg_wrapper *get_response);
static void cr_replica_undo_op_put(struct cr_recovery_op *op);
static void cr_replica_undo_op_put_cb(struct plat_closure_scheduler *context,
                                      void *env,
                                      struct sdf_msg_wrapper *put_response);
static void cr_replica_undo_op_complete(struct cr_recovery_op *op,
                                        SDF_status_t status);
static void cr_replica_ref_count_dec(struct cr_replica *replica);
static int cr_replica_is_writeable(const struct cr_replica *replica);
static const char *cr_replica_state_to_string(enum cr_replica_state state)
    __attribute__((const));
static int cr_replica_state_to_flags(enum cr_replica_state state)
    __attribute__((const));
static void cr_replica_state_validate(enum cr_replica_state state);
static void __attribute__((constructor)) cr_replica_states_validate();


/* Op functions */
static struct cr_op *cr_op_alloc(struct copy_replicator *cr,
                                 struct sdf_msg_wrapper *request_wrapper);
static void cr_op_start(struct cr_op *op,
                        struct plat_closure_scheduler *context);
static void cr_op_lock_cb(struct plat_closure_scheduler *context, void *env,
                          SDF_status_t status_arg,
                          struct replicator_key_lock *key_lock);
/*
 * XXX: drew 2008-11-20 This needs to be deleted along with the matching part
 * in the #sdf_replicator_adapter.
 */
static void __attribute__((unused))
cr_op_meta_response_flash(struct plat_closure_scheduler *context,
                          void *env, SDF_status_t status,
                          struct SDF_container_meta *meta);

static void cr_op_flash_response(struct plat_closure_scheduler *context,
                                 void *env, struct sdf_msg_wrapper *response);
static SDF_status_t cr_op_accum_msg(struct cr_op *op,
                                    struct sdf_msg_wrapper *response_arg);
static SDF_status_t cr_op_accum_status(struct cr_op *op,
                                       SDF_status_t msg_status);
static void cr_op_ref_count_dec(struct cr_op *op);
static __inline__ void cr_op_ref_count_zero(struct cr_op *op);
static void cr_op_synthesize_response(struct cr_op *op);

static SDF_status_t
cr_op_flash_multi_node(struct cr_op *op, struct plat_closure_scheduler *context,
                       sdf_msg_recv_wrapper_t response_closure);

static SDF_status_t
cr_op_flash_forward(struct cr_op *op, struct plat_closure_scheduler
                    *context, vnode_t dest_node,
                    sdf_msg_recv_wrapper_t response_closure);
static SDF_status_t
cr_op_flash_request(struct cr_op *op, struct plat_closure_scheduler *context,
                    struct sdf_msg_wrapper *send_msg_wrapper, vnode_t dest_node,
                    sdf_msg_recv_wrapper_t response_closure);

static SDF_status_t
cr_op_create_shard(struct cr_op *op, struct plat_closure_scheduler *context);

struct cr_node *cr_get_node(struct copy_replicator *cr, vnode_t pnode);

static void cr_msg_free(plat_closure_scheduler_t *contect, void *env,
                        struct sdf_msg *msg);
static const char *cr_fanout_to_string(enum cr_fanout fanout)
    __attribute__((const));
static int cr_replication_type_flags(SDF_replication_t replication_type)
    __attribute__((const));
static void cr_replication_type_validate(SDF_replication_t replication_type);
static void __attribute__((constructor)) cr_replication_types_validate();

struct sdf_replicator *
sdf_copy_replicator_alloc(const struct sdf_replicator_config *config,
                          struct sdf_replicator_api *callbacks) {
    struct copy_replicator *ret;
    int failed;
    int i;
    plat_closure_scheduler_t *scheduler;

    failed = !plat_calloc_struct(&ret);
    if (!failed) {
        ret->base.start_fn = &cr_start;
        ret->base.get_op_meta_fn = &cr_get_op_meta;
        ret->base.add_notifier = &cr_add_notifier;
        ret->base.remove_notifier = &cr_remove_notifier;
        ret->base.get_container_stats = &cr_get_container_stats;
        ret->base.command_async = &cr_command_async;

        ret->config = *config;
        /*
         * XXX: drew 2009-01-27 May want to use an externally provided single
         * scheduler, although separate schedulers probably make it easier
         * to track down problems because each subsystem's closure activations
         * are on separate lists.
         */
        ret->callbacks = *callbacks;
        ret->state = CR_STATE_INITIAL;
        ret->closure_scheduler = plat_mbox_scheduler_alloc();
        if (!ret->closure_scheduler) {
            failed = 1;
        }
        ret->callbacks.single_scheduler = ret->closure_scheduler;
        LIST_INIT(&ret->op_list);
        ret->ref_count = 1;
        ret->shutdown_closure = sdf_replicator_shutdown_cb_null;
        ret->nodes = plat_calloc(config->node_count, sizeof (ret->nodes[0]));
        if (!ret->nodes) {
            failed = 1;
        } else {
            for (i = 0; i < config->node_count; ++i) {
                ret->nodes[i].state = CR_NODE_DEAD;
            }
        }
        plat_calloc_struct(&ret->total_stat_counters);
        if (!ret->total_stat_counters) {
            failed = 1;
        }

        LIST_INIT(&ret->shard_list);
    } else {
        failed = 1;
    }

    if (!failed) {
        ret->base.recv_closure =
            sdf_msg_recv_wrapper_create(ret->closure_scheduler, &cr_recv_msg,
                                        ret);
        ret->base.shutdown_closure =
            sdf_replicator_shutdown_create(ret->closure_scheduler,
                                           &cr_shutdown, ret);

        ret->base.node_live_closure  =
            sdf_replicator_node_live_create(ret->closure_scheduler,
                                            &cr_node_live, ret);

        ret->base.node_dead_closure  =
            sdf_replicator_node_dead_create(ret->closure_scheduler,
                                            &cr_node_dead, ret);

        ret->nodes[ret->config.my_node].state = CR_NODE_LIVE;

        rms_shard_meta_cb_t meta_update_closure =
            rms_shard_meta_cb_create(ret->callbacks.single_scheduler,
                                     &cr_meta_update_cb, ret);

        ret->meta_storage =
            replicator_meta_storage_alloc(&ret->config, &ret->callbacks,
                                          meta_update_closure);
        failed = !ret->meta_storage;
        /* Failures past this point have more complex undo */
    }

    if (!failed) {
        ret->closure_scheduler_thread =
            fthSpawn(&plat_mbox_scheduler_main, 40960);

        failed = !ret->closure_scheduler_thread;
    }

    if (!failed || !ret) {
    } else if (ret->closure_scheduler_thread) {
        plat_fatal("fatal copy_replicator allocation failure");
    } else {
        scheduler = ret->closure_scheduler;
        cr_shutdown(NULL, ret, sdf_replicator_shutdown_cb_null);
        if (scheduler) {
            plat_mbox_scheduler_main((uint64_t)scheduler);
        }
        ret = NULL;
    }

    if (!failed) {
        plat_log_msg(21344, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "copy_replicator %p allocated", ret);
    } else {
        plat_log_msg(21345, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "copy_replicator alloc failed");
    }

    /* Coverity does not like this safe type-pun */
#if 0
    return (&ret->base);
#else
    /* But may accept this */
    return ((struct sdf_replicator *)ret);
#endif
}

/** @brief Start Implementation */
static int
cr_start(struct sdf_replicator *self) {
    struct copy_replicator *cr = (struct copy_replicator *)self;
    int tmp;

    plat_log_msg(21346, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "copy_replicator %p starting", cr);

    tmp = __sync_bool_compare_and_swap(&cr->state, CR_STATE_INITIAL,
                                       CR_STATE_RUNNING);
    plat_assert(tmp);

    rms_start(cr->meta_storage);

    fthResume(cr->closure_scheduler_thread, (uint64_t)cr->closure_scheduler);

    return (0);
}

/**
 * @brief Get meta-data for operation  (called from action node code)
 *
 * Currently, this is a simple place holder.  The copy replicator
 * assumes chained declustering for shard placement with no meta-data
 * and the home node is always the first one with a replica.
 *
 * More interesting implementations will embed volume ltime to
 * allow fencing IOs from previous eras.
 *
 * @param self <IN> replicator
 * @param container_meta <IN> meta-data for entire container
 * @param shard <IN> shard guid
 * @param op_meta <OUT> meta-data valid for a single operation
 * @return SDF_SUCCESS on success, otherwise on error
 */
static SDF_status_t
cr_get_op_meta(struct sdf_replicator *self,
               const struct SDF_container_meta *container_meta,
               SDF_shardid_t shard, struct sdf_replication_op_meta *op_meta) {
    struct copy_replicator *cr = (struct copy_replicator *)self;
    struct sdf_replication_shard_meta *shard_meta = &op_meta->shard_meta;
    SDF_replication_t replication_type =
        container_meta->properties.replication.type;
    int shard_offset;
    vnode_t pnode;
    struct cr_node *node;
    SDF_status_t ret;
    int i;

    plat_assert(cr->state == CR_STATE_RUNNING);
    plat_assert(container_meta->properties.replication.enabled &&
                container_meta->properties.replication.type !=
                SDF_REPLICATION_NONE);

    memset(op_meta, 0, sizeof (*op_meta));
    shard_meta->type = container_meta->properties.replication.type;

    if (!(cr_replication_type_flags(shard_meta->type) & CRTF_MANAGE_DATA)) {
        ret = SDF_SUCCESS;
    } else {
        shard_offset = shard - container_meta->shard;

        /* XXX: drew 2008-10-27 This assumes chained declustering */
        cr_get_pnodes(&cr->config, &container_meta->properties.replication,
                      (container_meta->node + shard_offset) /* first node */ %
                      cr->config.node_count,
                      shard_meta->pnodes, &shard_meta->nreplica,
                      &shard_meta->nmeta);

        /* shard_offset doesn't figure in to this */
        if (container_meta->properties.replication.type ==
            SDF_REPLICATION_META_SUPER_NODE) {
            shard_meta->meta_pnode = container_meta->meta_node;
        } else {
            shard_meta->meta_pnode = SDF_ILLEGAL_PNODE;
        }

        plat_assert(!(cr_replication_type_flags(replication_type) &
                      CRTF_MANAGE_NONE));

        for (ret = SDF_UNAVAILABLE, i = 0;
             ret == SDF_UNAVAILABLE && i < shard_meta->nreplica; ++i) {
            pnode = shard_meta->pnodes[i];
            node = cr_get_node(cr, pnode);
            plat_assert(node);
            if (node->state == CR_NODE_LIVE) {
                shard_meta->current_home_node = pnode;
                ret = SDF_SUCCESS;
            }
        }

        shard_meta->meta_shardid = container_meta->meta_shard + shard_offset;
    }

    return (ret);
}

/**
 * @brief Add a listener to replication state change
 *
 * @param self <IN> Replicator
 * @param cb <IN> Closure applied when the state changes
 * @return A handle which can be passed to #sdf_replicator_remove_notifier
 * on shutdown.
 */
static struct cr_notifier *
cr_add_notifier(struct sdf_replicator *self,
                sdf_replicator_notification_cb_t cb) {
    struct copy_replicator *cr = (struct copy_replicator *)self;
    fthWaitEl_t *unlock;
    struct cr_notifier *ret;
    int after;

    if (plat_calloc_struct(&ret)) {
        ret->cr = cr;
        ret->cb = cb;

        unlock = fthLock(&cr->notifiers.lock, 1 /* write lock */, NULL);
        LIST_INSERT_HEAD(&cr->notifiers.list, ret, notifiers_list_entry);
        fthUnlock(unlock);

        after = __sync_add_and_fetch(&cr->ref_count, 1);
        plat_assert(after > 1);
    }

    return (ret);
}

/**
 * @brief Remove a notification
 *
 * @param self <IN> Replicator
 * @param notifier <IN> Handle to release from #sdf_replicator_add_notifier
 */
static void
cr_remove_notifier(struct sdf_replicator *self, struct cr_notifier *notifier) {
    struct copy_replicator *cr = (struct copy_replicator *)self;
    struct cr_notifier_shard *shard;
    struct cr_notifier_shard *next;
    fthWaitEl_t *unlock;

    unlock = fthLock(&cr->notifiers.lock, 1 /* write lock */, NULL);
    LIST_REMOVE(notifier, notifiers_list_entry);
    fthUnlock(unlock);

    /*
     * XXX: drew 2009-09-30 Coverity isn't smart enough to figure
     * out that notifier->shard_list.lh_first is changing each time when
     * we do this instead
     * while ((shard = LIST_FIRST(&notifier->shard_list)))
     */
    LIST_FOREACH_SAFE(shard, &notifier->shard_list, shard_list_entry, next) {
        LIST_REMOVE(shard, shard_list_entry);
        plat_free(shard);
    }

    plat_free(notifier);

    cr_ref_count_dec(cr);
}

/** @brief Get container stats */
static SDF_status_t
cr_get_container_stats(struct sdf_replicator *self,
                       struct sdf_replicator_shard_stats *stats,
                       SDF_cguid_t cguid) {
    struct copy_replicator *cr = (struct copy_replicator *)self;
    cr_do_get_container_stats_t do_get_container_stats =
        cr_do_get_container_stats_create(cr->callbacks.single_scheduler,
                                         &cr_get_container_stats_impl, cr);
    fthMbox_t status_mbx;
    SDF_status_t ret;

    fthMboxInit(&status_mbx);
    plat_closure_apply(cr_do_get_container_stats, &do_get_container_stats,
                       &status_mbx, stats, cguid);

    ret = fthMboxWait(&status_mbx);
    return (ret);
}

/** @brief Actual implementation of #cr_get_container_stats */
static void
cr_get_container_stats_impl(plat_closure_scheduler_t *context, void *env,
                            fthMbox_t *status_mbx,
                            struct sdf_replicator_shard_stats *stats,
                            SDF_cguid_t cguid) {
    struct copy_replicator *cr = (struct copy_replicator *)env;
    struct cr_shard *shard = NULL;
    SDF_status_t status;


    plat_assert(status_mbx);
    plat_assert(stats);

    LIST_FOREACH(shard, &cr->shard_list, shard_list_entry) {
        if (shard->shard_meta && cguid == shard->shard_meta->persistent.cguid) {
            break;
        }
    }

    if (!shard) {
        status = SDF_CONTAINER_UNKNOWN;
    } else {
        status = cr_shard_get_stats(shard, stats);
    }

    fthMboxPost(status_mbx, status);
}

/**
 * @brief Process a command asynchronously
 *
 * Commands currently supported are
 * RECOVERED: for simple replication to indicate that the remote replica
 * has recovered.
 *
 * XXX: drew 2009-07-30 This is just an expedient hack. which should go
 * away.
 *
 * XXX: drew 2009-07-30 Add "standard" commands once we decide on syntax
 *
 * @param replicator <IN> replicator
 * @param shard <IN> shard being operated on
 * @param command_arg <IN> command being executed; caller retains
 * ownership and cn free after return which may be before #cb
 * application.
 * @param cb <IN> closure applied on completion
 */
static void
cr_command_async(struct sdf_replicator *self,
                 SDF_shardid_t sguid, const char *command_arg,
                 sdf_replicator_command_cb_t cb) {
    struct copy_replicator *cr = (struct copy_replicator *)self;
    char *command;
    int after;

    cr_do_command_async_t do_async_command =
        cr_do_command_async_create(cr->callbacks.single_scheduler,
                                   &cr_command_async_impl, cr);
    command = plat_strdup(command_arg);
    if (!command) {
        plat_closure_apply(sdf_replicator_command_cb, &cb,
                           SDF_FAILURE_MEMORY_ALLOC, NULL);
    } else {
        after = __sync_add_and_fetch(&cr->ref_count, 1);
        plat_assert(after > 1);

        plat_closure_apply(cr_do_command_async, &do_async_command, sguid,
                           command, cb);
    }
}

/**
 * @brief Actual implementation of #cr_command_async
 *
 * Consumes one reference count of #env
 *
 * @param env <IN> #copy_replicator
 * @param command <IN> Copy of command with ownership transfer to
 * #cr_command_async_impl
 */
static void
cr_command_async_impl(plat_closure_scheduler_t *context, void *env,
                      SDF_shardid_t sguid, char *command,
                      sdf_replicator_command_cb_t cb) {
    struct copy_replicator *cr = (struct copy_replicator *)env;
    char *tmp;

    tmp = strchr(command, '\r');
    if (tmp) {
        *tmp = 0;
    }
    tmp = strchr(command, '\n');
    if (tmp) {
        *tmp = 0;
    }

    if (strcmp(command, "RECOVERED") == 0) {
        cr_command_recovered(cr, sguid, cb);
    } else {
        plat_closure_apply(sdf_replicator_command_cb, &cb, SDF_UNKNOWN_REQUEST,
                           plat_strdup("ERROR\r\n"));
    }

    plat_free(command);
    cr_ref_count_dec(cr);
}

/**
 * @brief Implementation of cr_command_async_impl RECOVERED
 *
 * FIXME: drew 2009-08-25 The v1 use case calls for per-node vip groups.
 *
 * The simplest approach for copy_replicator.c is agnosticism to shard ids
 * during VIP group operation, although that adds more special cases to
 * the consumer's API.
 *
 * A reasonable compromise may be to maintain a small list of cr_shard_id
 * structures which indicate whether or not they've recovered.  With that
 * in place VIP migration back needs to block on recovery within all
 * data shards.
 *
 * This will also require modification to the meta-data creation code.
 *
 * XXX: drew 2009-08-25 Currently the fall-back logic merely lets
 * the lease expire which means there will be a VIP outage for X
 * seconds.  We want to change this so that there's a clean
 * switch-over, perhaps using a special notation in the meta-data
 * structure.
 *
 * With a minor modification to #cr_shard_state_delay_lease_acquisition_enter
 * we could get that behavior.
 *
 * FIXME: drew 2009-09-11 For V1 multi-shard support this needs to update
 * the state of shard->local_vip_meta and only release the vip once all
 * shards are successfully recovered.
 */
static void
cr_command_recovered(struct copy_replicator *cr, SDF_shardid_t sguid,
                     sdf_replicator_command_cb_t cb) {
    struct cr_shard *shard;
    int found;
    char *output;
    SDF_status_t status;
    int preference;

    found = 0;
    status = SDF_SUCCESS;
    output = NULL;

    plat_log_msg(21347, LOG_CAT_RECOVERY, PLAT_LOG_LEVEL_DEBUG,
                 "copy_replicator %p shard 0x%lx recovered", cr, sguid);

    LIST_FOREACH(shard, &cr->shard_list, shard_list_entry) {
        if (shard->sguid != sguid) {
        } else if (shard->state != CR_SHARD_STATE_RW) {
        } else if (!shard->shard_meta) {
            output = plat_strdup("SERVER_ERROR Shard does not yet"
                                 " have meta-data\r\n");
            status = SDF_REPLICATION_NOT_READY;
        } else if (shard->shard_meta->persistent.type !=
                   SDF_REPLICATION_V1_2_WAY) {
            plat_asprintf(&output, "SERVER_ERROR Bad replication type %s"
                          " not V1 2 way\r\n",
                          sdf_replication_to_string(shard->shard_meta->persistent.type));
            status = SDF_REPLICATION_BAD_TYPE;

        } else if ((status =
                    cr_shard_get_preference(shard,
                                            cr->config.my_node,
                                            CR_SHARD_GET_PREFERENCE_NORMAL,
                                            &preference,
                                            NULL /* denominator */)) !=
                   SDF_SUCCESS) {
            plat_asprintf(&output, "SERVER ERROR get preference failed %s",
                          sdf_status_to_string(status));
        /* This shard shouldn't be on this node */
        } else if (preference > 0) {
            plat_assert_always(!found);
            found = 1;
            cr_shard_command_recovered(shard, cb);
        }

        if (status != SDF_SUCCESS || found) {
            break;
        }
    }

    if (status == SDF_SUCCESS && !found) {
        output =
            plat_strdup("SERVER_ERROR Cannot locate non-primary vip group\r\n");
        status = SDF_REPLICATION_BAD_STATE;
    }

    /* Otherwise we've started an asynchronous process */

    plat_assert_imply(status == SDF_SUCCESS, found);
    plat_assert_imply(status == SDF_SUCCESS, !output);

    if (status != SDF_SUCCESS) {
        plat_log_msg(21348, LOG_CAT_RECOVERY,
                     status == SDF_SUCCESS ? PLAT_LOG_LEVEL_DEBUG :
                     PLAT_LOG_LEVEL_WARN,
                     "copy_replicator %p shard 0x%lx recovered status %s"
                     " output '%s'", cr,
                     sguid, sdf_status_to_string(status), output ? output : "");

        plat_closure_apply(sdf_replicator_command_cb, &cb, status, output);
    }
}

static void
cr_recv_msg(plat_closure_scheduler_t *context, void *env,
            struct sdf_msg_wrapper *wrapper) {
    struct copy_replicator *cr = (struct copy_replicator *)env;

    if (wrapper->dest_service == SDF_REPLICATION_PEER_META_SUPER ||
        wrapper->dest_service == SDF_REPLICATION_PEER_META_CONSENSUS) {
        rms_receive_msg(cr->meta_storage, wrapper);
    } else if (wrapper->dest_service == SDF_REPLICATION) {
        cr_recv_msg_replication(context, env, wrapper);
    } else {
        plat_assert(0);
    }
}

/**
 * @brief #sdf_replicator recv_closure implementation
 *
 * Applied when any replication service message is received
 */
static void
cr_recv_msg_replication(plat_closure_scheduler_t *context, void *env,
                        struct sdf_msg_wrapper *wrapper) {
    struct copy_replicator *cr = (struct copy_replicator *)env;
    struct cr_op *op;
    SDF_replication_t replication_type_flags;

    /*
     * XXX: No way to easily reject this if the allocation fails; we need a
     * standardized response mechanism.
     */
    op = cr_op_alloc(cr, wrapper);
    plat_assert_always(op);

    plat_assert_either(op->flags & CROF_CR_SHARD_NONE,
                       op->flags & CROF_CR_SHARD_BY_TYPE);

    if (op->flags & CROF_CR_SHARD_BY_TYPE) {
        replication_type_flags =
            cr_replication_type_flags(op->request_pm->op_meta.shard_meta.type);
        plat_assert_either(replication_type_flags & CRTF_INDEX_NONE,
                           replication_type_flags & CRTF_INDEX_SHARDID,
                           replication_type_flags & CRTF_INDEX_VIP_GROUP_ID);

        /*
         * FIXME: drew 2009-08-19 This is wrong and should be
         * CRTF_INDEX_SHARD_ID.  We also need to include the VIP
         * group in messages if that's how we're indexing.
         */
        if (replication_type_flags &
            (CRTF_INDEX_SHARDID|CRTF_INDEX_VIP_GROUP_ID)) {
            op->shard =
                cr_get_or_alloc_shard(cr, op->request_pm->shard,
                                      VIP_GROUP_ID_INVALID,
                                      op->request_pm->op_meta.shard_meta.type);
            plat_assert(op->shard);
        }
    }

#ifdef notyet
    if (op->shard && op->shard->state != CR_SHARD_STATE_RW) {
        /* FIXME: Should queue here */
    } else
#endif
    {
        /*
         * XXX: drew 2009-05-22 Need to queue ops when shard recovery is
         * pending.
         */
        cr_op_start(op, context);
    }

    /* Release original reference count */
    cr_op_ref_count_dec(op);
}

/**
 * @brief #sdf_replicator shutdown implementation
 *
 * Can free partially constructed copy_replicator. Notifiers are left
 * connected, with the shutdown unable to complete until they have been
 * disconnected.
 *
 * XXX: The notifier behavior is awkward and should change.
 */
static void
cr_shutdown(plat_closure_scheduler_t *context, void *env,
            sdf_replicator_shutdown_cb_t shutdown_closure) {
    struct copy_replicator *cr = (struct copy_replicator *)env;
    enum cr_state old_state = cr->state;
    int status;

    /* Already shutdown */
    plat_assert(cr->ref_count > 0);

    plat_log_msg(21349, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "copy_replicator %p shutdown", cr);
    plat_assert(old_state == CR_STATE_INITIAL || old_state == CR_STATE_RUNNING);
    status = __sync_bool_compare_and_swap(&cr->state, old_state,
                                          CR_STATE_STOPPING);
    plat_assert(status);

    if (cr->ref_count != 1) {
        plat_log_msg(21350, LOG_CAT, PLAT_LOG_LEVEL_WARN,
                     "copy_replicator %p node %u shutdown with %d ref count",
                     cr, cr->config.my_node, cr->ref_count);
    }

    cr->shutdown_closure = shutdown_closure;

    cr_ref_count_dec(cr);
}

/**
 * @brief #sdf_replicator node_live implementation
 *
 * This excecutes in the cr->callbacks.single_scheduler context so it is
 * completely serialized with all other livenes and recovery events.
 */
static void
cr_node_live(plat_closure_scheduler_t *context, void *env,
             vnode_t pnode, struct timeval last_live) {
    struct copy_replicator *cr = (struct copy_replicator *)env;
    struct cr_node *node;

    if (pnode != cr->config.my_node) {
        node = cr_get_node(cr, pnode);
        if (node) {
            plat_log_msg(21351, LOG_CAT_LIVENESS,
                         node->state == CR_NODE_LIVE ?
                         PLAT_LOG_LEVEL_DEBUG : PLAT_LOG_LEVEL_INFO,
                         "copy_replicator %p node %u node %u live",
                         cr, cr->config.my_node, pnode);

            node->state = CR_NODE_LIVE;
            node->last_live = last_live;

            if (cr->meta_storage) {
                rms_node_live(cr->meta_storage, pnode);
            }

            cr_shards_signal_liveness_change(cr, pnode, CR_NODE_LIVE);
        } else {
            plat_log_msg(21352, LOG_CAT_LIVENESS,
                         PLAT_LOG_LEVEL_WARN,
                         "copy_replicator %p node %u unknown node %u live"
                         " cluster may be misconfigured",
                         cr, cr->config.my_node, pnode);
        }
    }
}

/**
 * @brief #sdf_replicator node_live implementation
 *
 * This excecutes in the cr->callbacks.single_scheduler context so it is
 * completely serialized with all other livenes and recovery events.
 */
static void
cr_node_dead(plat_closure_scheduler_t *context, void *env,
             vnode_t pnode) {
    struct copy_replicator *cr = (struct copy_replicator *)env;
    struct cr_node *node;

    if (pnode != cr->config.my_node) {
        node = cr_get_node(cr, pnode);
        if (node) {
            plat_log_msg(21353, LOG_CAT_LIVENESS,
                         node->state == CR_NODE_DEAD ?
                         PLAT_LOG_LEVEL_DEBUG : PLAT_LOG_LEVEL_WARN,
                         "copy_replicator %p node %u node %u dead",
                         cr, cr->config.my_node, pnode);

            node->state = CR_NODE_DEAD;

            if (cr->meta_storage) {
                rms_node_dead(cr->meta_storage, pnode);
            }

            cr_shards_signal_liveness_change(cr, pnode, CR_NODE_DEAD);
        } else {
            plat_log_msg(21354, LOG_CAT_LIVENESS,
                         PLAT_LOG_LEVEL_WARN,
                         "copy_replicator %p node %u unknown node %u dead"
                         " cluster may be misconfigured",
                         cr, cr->config.my_node, pnode);
        }
    }
}

static void
cr_shards_signal_liveness_change(struct copy_replicator *cr, vnode_t pnode,
                                 enum cr_node_state state) {
    struct cr_shard *shard;

    LIST_FOREACH(shard, &cr->shard_list, shard_list_entry) {
        cr_shard_signal_liveness_change(shard, pnode, state);
    }
}

/** @brief Decrement reference count, next shutdown phase on 0 */
static void
cr_ref_count_dec(struct copy_replicator *cr) {
    int after;
    struct cr_shard *shard;

    after = __sync_sub_and_fetch(&cr->ref_count, 1);
    plat_assert(after >= 0);

    if (!after) {
        plat_assert(cr->state == CR_STATE_STOPPING);

        plat_log_msg(21355, LOG_CAT_SHUTDOWN, PLAT_LOG_LEVEL_DEBUG,
                     "copy_replicator %p node %u refcount 0", cr,
                     cr->config.my_node);
        /*
         * Have at least one reference so we don't free cr until leaving
         * the loop.
         */
        ++cr->shard_shutdown_count;
        while (!LIST_EMPTY(&cr->shard_list)) {
            shard = LIST_FIRST(&cr->shard_list);
            LIST_REMOVE(shard, shard_list_entry);
            ++cr->shard_shutdown_count;
            cr_shard_shutdown(shard);
        }
        /* Opposite of the above reference increment */
        cr_signal_shard_shutdown_complete(cr);
    }
}

/** @brief Called when a shard has shutdown */
static void
cr_signal_shard_shutdown_complete(struct copy_replicator *cr) {
    --cr->shard_shutdown_count;
    plat_assert(cr->shard_shutdown_count >= 0);


    if (!cr->shard_shutdown_count) {
        plat_log_msg(21356, LOG_CAT_SHUTDOWN, PLAT_LOG_LEVEL_DEBUG,
                     "copy_replicator %p node %u shard shutdown count 0", cr,
                     cr->config.my_node);

        if (cr->meta_storage) {
            rms_shutdown(cr->meta_storage,
                         rms_shutdown_cb_create(cr->callbacks.single_scheduler,
                                                &cr_rms_stopped, cr));
        } else {
            cr_rms_stopped(NULL, cr);
        }
    }
}

/** @brief Called when cr->meta_storage has completed shutdown. */
static void
cr_rms_stopped(struct plat_closure_scheduler *context, void *env) {
    struct copy_replicator *cr = (struct copy_replicator *)env;
    plat_closure_scheduler_shutdown_t shutdown_closure;

    plat_log_msg(21357, LOG_CAT_SHUTDOWN, PLAT_LOG_LEVEL_DEBUG,
                 "copy_replicator %p node %u rms stopped", cr,
                 cr->config.my_node);

    shutdown_closure =
        plat_closure_scheduler_shutdown_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                               &cr_closure_scheduler_stopped,
                                               cr);

    if (cr->closure_scheduler) {
        plat_closure_scheduler_shutdown(cr->closure_scheduler,
                                        shutdown_closure);
    } else {
        cr_closure_scheduler_stopped(NULL, cr);
    }
}

/** @brief Closure scheduler stopped or skipped, finish shutdown */
static void
cr_closure_scheduler_stopped(struct plat_closure_scheduler *context, void *env) {
    struct copy_replicator *cr = (struct copy_replicator *)env;

    plat_log_msg(21358, LOG_CAT_SHUTDOWN, PLAT_LOG_LEVEL_DEBUG,
                 "copy_replicator %p node %u scheduler stopped", cr,
                 cr->config.my_node);


    plat_closure_chain(sdf_replicator_shutdown_cb, context,
                       &cr->shutdown_closure);

    if (cr->nodes) {
        plat_free(cr->nodes);
    }
    if (cr->total_stat_counters) {
        plat_free(cr->total_stat_counters);
    }
    plat_free(cr);
}

/**
 * @brief Return pnodes based on replication meta-data
 *
 * XXX: drew 2008-11-14 Currently this does chained declustering, but this is
 * a policy thing which should be handled by a higher layer like we do with
 * sharding.
 *
 * @param cofnig <IN> Configuration
 * @param dest <OUT> All pnodes
 * @param num_ptr <OUT> N pointer for number of replicas
 * @param num_ptr_meta <OUT> N pointer for number of meta-data replicas
 */
void
cr_get_pnodes(const struct sdf_replicator_config *config,
              const SDF_replication_props_t *rep_props, vnode_t first_node,
              SDF_vnode_t dest[SDF_REPLICATION_MAX_REPLICAS],
              uint32_t *num_ptr, uint32_t *num_ptr_meta) {
    uint32_t i;
    uint32_t num;
    uint32_t num_meta;
    uint32_t max;

    plat_assert(num_ptr);
    plat_assert(num_ptr_meta);

    num = rep_props->enabled ? rep_props->num_replicas : 1;
    num_meta = rep_props->enabled ? rep_props->num_meta_replicas : 0;
    max = PLAT_MAX(num, num_meta);

    plat_assert(rep_props->num_replicas <= config->node_count);

    for (i = 0; i < max; ++i) {
        dest[i] = (first_node + i) % config->node_count;
    }

    if (num_ptr_meta) {
        *num_ptr_meta = num_meta;
    }

    if (num_ptr) {
        *num_ptr = num;
    }
}

/**
 * @brief Callback from meta_storage code on update detected
 *
 * XXX: drew 2009-06-23 Note that this means that all nodes which have received
 * notifications have shard state, even if they can't actually serve the shard
 * or switch over.
 *
 * The right solution is to change meta_storage.c so that only "interested"
 * nodes are notified, and to clean up any existing shards which no longer
 * have local storage.
 *
 * @param status <IN> May not be SDF_SUCCESS due to implementation artifacts
 * @param shard_meta <IN> Replicator meta storage provided meta data with
 * ownership transfer.
 */
static void
cr_meta_update_cb(struct plat_closure_scheduler *context, void *env,
                  SDF_status_t status, struct cr_shard_meta *shard_meta,
                  struct timeval lease_expires) {
    struct copy_replicator *cr = (struct copy_replicator *)env;
    struct cr_shard *shard;
    int flags;

    plat_assert_imply(status == SDF_SUCCESS, shard_meta);

    /* Ignore errors */
    if (!shard_meta) {
    /* Skip whilst shutting down */
    } if (status == SDF_SUCCESS && cr->state != CR_STATE_STOPPING &&
          cr->state != CR_STATE_STOPPED) {
        flags = cr_replication_type_flags(shard_meta->persistent.type);
        plat_assert_either(flags & CRTF_INDEX_SHARDID,
                           flags & CRTF_INDEX_VIP_GROUP_ID);

        if (flags & CRTF_INDEX_SHARDID) {
            shard = cr_get_or_alloc_shard(cr, shard_meta->persistent.sguid,
                                          VIP_GROUP_ID_INVALID,
                                          shard_meta->persistent.type);
        } else if (flags & CRTF_INDEX_VIP_GROUP_ID) {
            shard = cr_get_or_alloc_shard(cr, SDF_SHARDID_INVALID,
                                          shard_meta->persistent.intra_node_vip_group_id,
                                          shard_meta->persistent.type);
        } else {
            plat_fatal("Bad flags");
        }
        plat_assert(shard);
        cr_shard_meta_update_external(shard, shard_meta, lease_expires);
    /* Otherwise eat the input */
    } else {
        cr_shard_meta_free(shard_meta);
    }
}

/**
 * @brief Get or create in-core cr_shard structure
 *
 * Does not imply creation of corresponding persistent state.
 *
 * Instead of taking a reference on cr we make its shutdown process block
 * on asynchronous shutdown of each shard.
 *
 * @param cr <IN> parent copy_replicator
 * @param sguid <IN> data shard guid being accessed, or #SDF_SHARDID_INVALID
 * when a data shard has yet to be associated with the shard.
 * @param group_id <IN> intra node vip group id being monitored,
 * VIP_GROUP_ID_INVALID if this is not associated with a group.
 * @param replication_type <IN> Replication type
 *
 * FIXME: drew 2009-09-11 Deal with shard->local_vip_meta in an appropriate
 * way.
 */
static struct cr_shard *
cr_get_or_alloc_shard(struct copy_replicator *cr, SDF_shardid_t sguid,
                      vip_group_id_t group_id,
                      SDF_replication_t replication_type) {
    struct cr_shard *shard = NULL;

    plat_assert(sguid != SDF_SHARDID_INVALID ||
                group_id != VIP_GROUP_ID_INVALID);

    /*
     * Match on group id first, ignoring the replication type because
     * that's not available.
     */
    LIST_FOREACH(shard, &cr->shard_list, shard_list_entry) {
        if (group_id != VIP_GROUP_ID_INVALID &&
            group_id == shard->vip_group_id) {
            plat_assert(sguid == SDF_SHARDID_INVALID ||
                        shard->sguid == SDF_SHARDID_INVALID ||
                        sguid == shard->sguid);
            /* XXX: Maintain a set of shard ids */
            /* This is symetric with the allocate case */
            if (shard->sguid == SDF_SHARDID_INVALID) {
                shard->sguid = sguid;
            }
            break;
        } else if (group_id == VIP_GROUP_ID_INVALID &&
                   sguid == shard->sguid) {
            plat_assert(shard->vip_group_id == VIP_GROUP_ID_INVALID);
            break;
        }
    }

    if (!shard && plat_calloc_struct(&shard)) {
        shard->cr = cr;
        shard->sguid = sguid;
        shard->vip_group_id = group_id;
        shard->replication_type = replication_type;
        shard->state = CR_SHARD_STATE_INITIAL;
        plat_asprintf(&shard->delay_lease_acquisition_event_name,
                      "delay lease acquisition"
                      " cr_shard %p node %u shard 0x%lx vip group %d",
                      shard, cr->config.my_node, shard->sguid,
                      shard->vip_group_id);
        plat_assert(shard->delay_lease_acquisition_event_name);
        plat_asprintf(&shard->lease_renewal_event_name, "lease renewal"
                      " cr_shard %p node %u shard 0x%lx vip group %d",
                      shard, cr->config.my_node, shard->sguid,
                      shard->vip_group_id);
        plat_assert(shard->lease_renewal_event_name);
        shard->local_replica = CR_SHARD_INVALID_LOCAL_REPLICA;
        shard->after_restart = 1;
        TAILQ_INIT(&shard->op_list);
        shard->lock_container =
            replicator_key_lock_container_alloc(cr->config.my_node,
                                                shard->sguid,
                                                shard->vip_group_id,
                                                shard->replication_type);
        plat_assert_always(shard->lock_container);
        TAILQ_INIT(&shard->pending_notification_list);

        shard->ref_count = 1;
        plat_calloc_struct(&shard->stat_counters);
        plat_assert(shard->stat_counters);

        LIST_INSERT_HEAD(&cr->shard_list, shard, shard_list_entry);

        /* Get other state related variables set */
        cr_shard_set_state(shard, CR_SHARD_STATE_INITIAL);
    }

    return (shard);
}

static void
cr_shard_signal_liveness_change(struct cr_shard *shard, vnode_t node,
                                enum cr_node_state state) {
    int flags;
    int i;
    struct cr_replica *replica;

    if (shard->shard_meta) {
        flags = cr_replication_type_flags(shard->shard_meta->persistent.type);
        if (flags & CRTF_MANAGE_DATA) {
            for (i = 0; i < shard->shard_meta->persistent.num_replicas; ++i) {
                replica = shard->replicas[i];
                if (replica && replica->node == node) {
                    cr_replica_signal_liveness_change(replica, state);
                }
            }
        } else if (flags & CRTF_MANAGE_META) {
            cr_shard_check_state(shard, NULL /* replica */);
        } else {
            plat_fatal("Bad flags");
        }
    }
}

/*
 * XXX: drew 2009-06-26 We should handle changes in the state flags here,
 * like from having no lease to requesting one.
 */
static void
cr_shard_set_state(struct cr_shard *shard, enum cr_shard_state next_state) {
    enum cr_shard_state from = shard->state;

    plat_log_msg(21359, LOG_CAT_STATE, PLAT_LOG_LEVEL_DEBUG,
                 "cr_shard %p node %u shard 0x%lx vip group %d state"
                 " from %s to %s",
                 shard, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id,
                 cr_shard_state_to_string(shard->state),
                 cr_shard_state_to_string(next_state));

    switch (from) {
#define item(caps, lower, flags, enter, leave) case caps: leave; break;
    CR_SHARD_STATE_ITEMS(shard, shard->state, next_state)
#undef item
    }

    plat_assert(shard->state == from);

    shard->state = next_state;

    switch (next_state) {
#define item(caps, lower, flags, enter, leave) \
    case caps:                                                                 \
        shard->state_flags = (flags);                                          \
        enter;                                                                 \
        break;
    CR_SHARD_STATE_ITEMS(shard, from, next_state)
#undef item
    }

    if ((shard->state_flags & CRSSF_NO_LEASE) && shard->lease_renewal_event) {
        cr_shard_lease_renewal_cancel(shard);
    }

    cr_shard_vip_group_check(shard);

    /* Maybe things have changed already */

    cr_shard_check_state(shard, NULL);
}

/** @brief Trampoline code for #cr_shard_signal_replica_state */
PLAT_CLOSURE1(cr_shard_replica_state, struct cr_replica *, replica_arg);

/**
 * @brief Signal shard level code that the replica may have changed state
 *
 * Shard level state changes trigger changes/activity in individual
 * replicas and their changing state in turn triggers shard state state
 * changes.
 *
 * Recursion is avoided by serializing all state changes by signaling
 * through a closure in the cr->callbacks.single_scheduler scheduler.
 */
static void
cr_shard_signal_replica_state(struct cr_shard *shard,
                              struct cr_replica *replica_arg) {
    cr_shard_replica_state_t cb =
        cr_shard_replica_state_create(shard->cr->callbacks.single_scheduler,
                                      &cr_shard_signal_replica_state_cb,
                                      shard);
    __sync_add_and_fetch(&shard->ref_count, 1);
    plat_closure_apply(cr_shard_replica_state, &cb, replica_arg);
}

/*
 * @brief Thin wrapper around #cr_shard_check_state for scheduled execution
 *
 * Thinking about all the potential recursion paths when shard level code
 * changes replica state resulting in shard state changes is difficult.
 *
 * This function is called from cr_shard interfaces used by cr_replica
 * code to avoid recursion from those changes (they're already serialized
 * due to message-based concurrency control).
 */
static void
cr_shard_signal_replica_state_cb(plat_closure_scheduler_t *context, void *env,
                                 struct cr_replica *replica_arg) {
    struct cr_shard *shard = (struct cr_shard *)env;

    cr_shard_check_state(shard, replica_arg);
    cr_shard_ref_count_dec(shard);
}

/**
 * @brief Poll state for shard
 *
 * @param replica <IN> When non-NULL this is the replica which has
 * potentially changed.
 *
 * XXX: drew 2009-12-16 It would keep everything in the same place if we
 * finished moving these into per-state functions like enter/leave.  This
 * has been done for CR_SHARD_STATE_DELAY_LEASE_ACQUISITION with
 * cr_shard_check_state_delay_lease_acquisition and
 * CR_SHARD_STATE_SWITCH_BACK_2 using cr_shard_check_state_switch_back_2(shard)
 */
static void
cr_shard_check_state(struct cr_shard *shard, struct cr_replica *replica_arg) {
    struct cr_replica *replica;
    int change;
    int i;


    switch (shard->state) {
    case CR_SHARD_STATE_TO_WAIT_META:
        if (!shard->op_count) {
            cr_shard_set_state(shard, CR_SHARD_STATE_WAIT_META);
        }
        break;

    case CR_SHARD_STATE_WAIT_META:
        cr_shard_check_state_wait_meta(shard);
        break;

    /*
     * Immediately acquire the lease when this is the preferred authoritative
     * replica and either this is not the first node to hold the lease after
     * reboot or all other authoritative replicas are up.
     */
    case CR_SHARD_STATE_DELAY_LEASE_ACQUISITION:
        cr_shard_check_state_delay_lease_acquisition(shard, replica_arg);
        break;

    /*
     * Transition to update following redo once all replicas have reached
     * their quiesced state.
     */
    case CR_SHARD_STATE_MUTUAL_REDO:
        plat_assert(shard->shard_meta);

        /*
         * FIXME: We need to correctly handle crashes here.  If any
         * of the nodes have failed their recovery process (state TO_DEAD,
         * state DEAD)  we need to bail out on the recovery process.
         */

        for (change = 1, i = 0;
             change && i < shard->shard_meta->persistent.num_replicas; ++i) {
            replica = shard->replicas[i];

            if (replica &&
                (replica->state == CR_REPLICA_STATE_MUTUAL_REDO ||
                 (replica->state == CR_REPLICA_STATE_MUTUAL_REDO_SCAN_DONE &&
                  replica->recovery_op_pending_count > 0))) {
                change = 0;
            }
        }

        if (change) {
            cr_shard_set_state(shard, CR_SHARD_STATE_UPDATE_2);
        }
        break;

    case CR_SHARD_STATE_RW:
        plat_assert(shard->shard_meta);

        for (i = 0; i < shard->shard_meta->persistent.num_replicas; ++i) {
            replica = shard->replicas[i];
            if (replica && replica->state == CR_REPLICA_STATE_LIVE_OFFLINE) {
                cr_replica_set_state(replica, CR_REPLICA_STATE_UNDO);
            }
        }
        break;

    case CR_SHARD_STATE_TO_SHUTDOWN:
        if (!shard->op_count) {
            cr_shard_set_state(shard, CR_SHARD_STATE_SHUTDOWN);
        }
        break;

    case CR_SHARD_STATE_SWITCH_BACK_2:
        cr_shard_check_state_switch_back_2(shard);
        break;

    default:
        break;
    }
}

/**
 * @brief Poll state for shard in CR_SHARD_STATE_WAIT_META
 *
 * Immediately leave if a lease should be attempted.
 */
static void
cr_shard_check_state_wait_meta(struct cr_shard *shard) {
    int flags;

    plat_assert(shard->state == CR_SHARD_STATE_WAIT_META);

    if (shard->sguid != SDF_SHARDID_INVALID && shard->shard_meta) {
        flags = cr_replication_type_flags(shard->shard_meta->persistent.type);
        if (flags & CRTF_MANAGE_META) {
            if (flags & CRTF_MANAGE_DATA) {
                cr_shard_wait_meta_update_for_both(shard);
            } else {
                cr_shard_wait_meta_update_for_meta(shard);
            }
        } else {
            plat_fatal("Flags say state machines should not be used");
        }
    }
}

/**
 * @brief Update state for CR_SHARD_STATE_WAIT_META data+meta path
 *
 * Immediately leave if a lease should be attempted.
 *
 * @param shard <IN> shard with valid shard->shard_meta in
 * CR_SHARD_STATE_WAIT_META
 */
static void
cr_shard_wait_meta_update_for_both(struct cr_shard *shard) {
    int flags;
    struct cr_node *node;
    struct cr_replica *replica;
    struct cr_shard_replica_meta *replica_meta;
    int i;

    plat_assert(shard->sguid != SDF_SHARDID_INVALID);
    plat_assert(shard->shard_meta);
    plat_assert(shard->state == CR_SHARD_STATE_WAIT_META);
    flags = cr_replication_type_flags(shard->shard_meta->persistent.type);
    plat_assert((flags & CRTF_MANAGE_META) && (flags & CRTF_MANAGE_DATA));

    for (i = 0; i < shard->shard_meta->persistent.num_replicas; ++i) {
        replica = shard->replicas[i];
        replica_meta = shard->shard_meta->replicas[shard->local_replica];
        node = cr_get_node(shard->cr, replica->node);
        plat_assert(node);
        switch (node->state) {
        case CR_NODE_LIVE:
            if (replica->state != CR_REPLICA_STATE_LIVE_OFFLINE) {
                cr_replica_set_state(replica, CR_REPLICA_STATE_LIVE_OFFLINE);
            }
            break;

        case CR_NODE_DEAD:
            if (replica->state != CR_REPLICA_STATE_DEAD &&
                replica->state != CR_REPLICA_STATE_TO_DEAD) {
                cr_replica_set_state(replica, CR_REPLICA_STATE_TO_DEAD);
            }
            break;
        }
    }

    if (shard->local_replica >= 0) {
        replica_meta = shard->shard_meta->replicas[shard->local_replica];
    } else {
        replica_meta = NULL;
    }

    /*
     * XXX: drew 2009-06-05 We may also be able to special case for an
     * existing lease from a previous restart but I'm not entirely sure
     * how that interacts with read-only replicas.
     */
    if (shard->shard_meta->persistent.lease_usecs == 0 && replica_meta &&
        (replica_meta->persistent.state == CR_REPLICA_STATE_AUTHORITATIVE ||
         replica_meta->persistent.state == CR_REPLICA_STATE_SYNCHRONIZED)) {
        cr_shard_set_state(shard, CR_SHARD_STATE_DELAY_LEASE_ACQUISITION);
    }
}

/**
 * @brief Update state for CR_SHARD_STATE_WAIT_META in meta only path
 *
 * Immediately leave if a lease should be attempted.
 *
 * @param shard <IN> shard with valid shard->shard_meta in
 * CR_SHARD_STATE_WAIT_META
 */
static void
cr_shard_wait_meta_update_for_meta(struct cr_shard *shard) {
    plat_assert(shard->state == CR_SHARD_STATE_WAIT_META);

    if (shard->shard_meta &&
        (cr_replication_type_flags(shard->shard_meta->persistent.type) &
         CRTF_META_DISTRIBUTED) &&
        shard->shard_meta->persistent.current_home_node ==
        shard->cr->config.my_node) {
        cr_shard_set_state(shard, CR_SHARD_STATE_YIELD);
    } else if (cr_shard_vip_group_lease_allowed(shard)) {
        cr_shard_set_state(shard, CR_SHARD_STATE_DELAY_LEASE_ACQUISITION);
    }
}

/**
 * @brief Poll state for shard in CR_SHARD_STATE_DELAY_LEASE_ACQUISITION
 *
 * @param shard <IN> Shard which is in state
 * CR_SHARD_STATE_DELAY_LEASE_ACQUISITION.
 *
 * @param replica_arg <IN> When non-NULL this is the replica which has
 * potentially changed.
 */
static void
cr_shard_check_state_delay_lease_acquisition(struct cr_shard *shard,
                                             struct cr_replica *replica_arg) {
    int flags;

    plat_assert(shard->state == CR_SHARD_STATE_DELAY_LEASE_ACQUISITION);
    plat_assert(shard->shard_meta);

    flags = cr_replication_type_flags(shard->shard_meta->persistent.type);

    if (flags & CRTF_MANAGE_DATA) {
        cr_shard_check_state_delay_lease_acquisition_for_both(shard,
                                                              replica_arg);
    }
}

/**
 * @brief Poll state for shard in CR_SHARD_STATE_DELAY_LEASE_ACQUISITION
 *
 * @param shard <IN> Shard which is handling both data and meta-data which
 * is in state CR_SHARD_STATE_DELAY_LEASE_ACQUISITION.
 * @param replica_arg <IN> When non-NULL this is the replica which has
 * potentially changed.
 */
static void
cr_shard_check_state_delay_lease_acquisition_for_both(struct cr_shard *shard,
                                                      struct cr_replica *replica_arg) {
    int change;
    int i;
    struct cr_shard_replica_meta *replica_meta;
    struct cr_node *node;
    int preferred_replica;

    plat_assert(shard->state == CR_SHARD_STATE_DELAY_LEASE_ACQUISITION);
    plat_assert(shard->shard_meta);

    replica_meta = NULL;

    for (preferred_replica = CR_SHARD_INVALID_LOCAL_REPLICA, i = 0;
         preferred_replica == CR_SHARD_INVALID_LOCAL_REPLICA &&
         i < shard->shard_meta->persistent.num_replicas; ++i) {
        replica_meta = shard->shard_meta->replicas[i];
        node = cr_get_node(shard->cr, replica_meta->persistent.pnode);
        if (replica_meta->persistent.state ==
            CR_REPLICA_STATE_AUTHORITATIVE &&
            node && (node->state == CR_NODE_LIVE ||
                     (shard->after_restart &&
                      replica_meta->persistent.pnode ==
                      shard->shard_meta->persistent.last_home_node))) {
            preferred_replica = i;
        }
    }
    plat_log_msg(21360, LOG_CAT_FAST_START,
                 PLAT_LOG_LEVEL_TRACE,
                 "cr_shard %p node %u shard 0x%lx vip group %d fast start"
                 " preferred replica %d on node %d",
                 shard, shard->cr->config.my_node,
                 shard->sguid, shard->vip_group_id, preferred_replica,
                 replica_meta ? replica_meta->persistent.pnode : -1);

    if (shard->local_replica != CR_SHARD_INVALID_LOCAL_REPLICA &&
        preferred_replica == shard->local_replica) {
        if (shard->after_restart) {
            change = 1;
        } else {
            for (change = 1, i = 0; change &&
                 i < shard->shard_meta->persistent.num_replicas; ++i) {
                replica_meta = shard->shard_meta->replicas[i];
                node = cr_get_node(shard->cr,
                                   replica_meta->persistent.pnode);
                if (replica_meta->persistent.state ==
                    CR_REPLICA_STATE_AUTHORITATIVE &&
                    (!node || node->state != CR_NODE_LIVE)) {
                    change = 0;
                }
            }
        }

        if (change) {
            plat_log_msg(21361, LOG_CAT_FAST_START,
                         PLAT_LOG_LEVEL_DEBUG,
                         "cr_shard %p node %u shard 0x%lx vip group %d"
                         " fast start after_restart %d",
                         shard, shard->cr->config.my_node,
                         shard->sguid, shard->vip_group_id,
                         shard->after_restart);
            cr_shard_set_state(shard, CR_SHARD_STATE_REQUEST_LEASE);
        }
    }
}

/**
 * @brief Poll state for shard in CR_SHARD_STATE_SWITCH_BACK_2
 *
 * @param shard <INOUT> which is in CR_SHARD_STATE_SWITCH_BACK_2
 */
static void
cr_shard_check_state_switch_back_2(struct cr_shard *shard) {
    int done;
    int replication_type_flags;
    struct sdf_vip_group_group *inter_group_group;
    struct sdf_vip_group *intra_group;
    vnode_t node_id;
    struct cr_node *node;

    plat_assert(shard->state == CR_SHARD_STATE_SWITCH_BACK_2);
    plat_assert(shard->shard_meta);

    replication_type_flags =
        cr_replication_type_flags(shard->shard_meta->persistent.type);

    if (shard->shard_meta->persistent.current_home_node != CR_HOME_NODE_NONE &&
        shard->shard_meta->persistent.current_home_node !=
        shard->cr->config.my_node) {
        plat_log_msg(21759, LOG_CAT_STATE, PLAT_LOG_LEVEL_DEBUG,
                     "cr_shard %p node %u shard 0x%lx vip group %d"
                     " home node %u found",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id,
                     shard->shard_meta->persistent.current_home_node);
        done = 1;
    } else if (replication_type_flags & CRTF_INDEX_VIP_GROUP_ID) {
        plat_assert(shard->vip_group_id != VIP_GROUP_ID_INVALID);
        inter_group_group =
            sdf_vip_config_get_group_group_by_gid(shard->cr->config.vip_config,
                                                  shard->vip_group_id);
        plat_assert(inter_group_group);
        intra_group =
            sdf_vip_config_get_vip_group(shard->cr->config.vip_config,
                                         shard->vip_group_id);
        plat_assert(intra_group);

        node_id = sdf_vip_group_get_node_by_preference(intra_group, 0);
        plat_assert(node_id != SDF_ILLEGAL_PNODE);

        node = cr_get_node(shard->cr, node_id);
        if (!node) {
            plat_log_msg(21760, LOG_CAT_STATE,
                         PLAT_LOG_LEVEL_ERROR,
                         "cr_shard %p node %u shard 0x%lx vip group %d"
                         " preferred node %u not found",
                         shard, shard->cr->config.my_node, shard->sguid,
                         shard->vip_group_id, node_id);
            done = 1;
        } else if (node->state == CR_NODE_DEAD) {
            plat_log_msg(21761, LOG_CAT_STATE,
                         PLAT_LOG_LEVEL_DEBUG,
                         "cr_shard %p node %u shard 0x%lx vip group %d"
                         " preferred node %u dead",
                         shard, shard->cr->config.my_node, shard->sguid,
                         shard->vip_group_id, node_id);
            done = 1;
        } else {
            plat_log_msg(21762, LOG_CAT_STATE,
                         PLAT_LOG_LEVEL_TRACE,
                         "cr_shard %p node %u shard 0x%lx vip group %d"
                         " preferred node %u live but home still %u",
                         shard, shard->cr->config.my_node, shard->sguid,
                         shard->vip_group_id, node_id,
                         shard->shard_meta->persistent.current_home_node);
            done = 0;
        }
    } else {
        plat_fatal("Illegal situation");
        done = 1;
    }

    if (done) {
        cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
    }
}

/**
 * @brief Callback from meta_storage code on update detected.
 *
 * This is also invoked as a side effect of pilot beacon operation even
 * when there have been no changes.
 *
 * @param shard_meta <IN> Replicator meta storage provided meta data with
 * ownership transfer.   Must be non-NULL.
 */
static void
cr_shard_meta_update_external(struct cr_shard *shard,
                              struct cr_shard_meta *shard_meta_arg,
                              struct timeval lease_expires) {
    struct copy_replicator *cr = shard->cr;
    struct cr_shard_meta *shard_meta = shard_meta_arg;
    int do_update;
    int to_wait_meta;
    struct cr_shard_replica_meta *replica_meta;
    enum cr_replica_state replica_state;
    int i;

    plat_assert_either(shard->state_flags & CRSSF_META_SOURCE_OTHER,
                       shard->state_flags & CRSSF_META_SOURCE_SELF);
    plat_assert(shard_meta_arg);

    plat_log_msg(21362, LOG_CAT_META,
                 shard_meta->persistent.lease_usecs == 0 ?
                 PLAT_LOG_LEVEL_DEBUG : PLAT_LOG_LEVEL_TRACE,
                 "cr_shard %p node %u shard 0x%lx vip group %d home %d"
                 " lease len %3.1f expires %s",
                 shard, cr->config.my_node, shard->sguid,
                 shard->vip_group_id, shard_meta->persistent.current_home_node,
                 shard_meta->persistent.lease_usecs / (float)PLAT_MILLION,
                 plat_log_timeval_to_string(&lease_expires));

    /*
     * FIXME: drew 2009-06-03 We need to handle the situation where the
     * meta-storage system goes off-line and then comes back. In that
     * case we'd want to pick up the lease renewal.
     *
     * The other problem we may want to deal with is out-of-order update
     * receipt.
     */

    /*
     * XXX: drew 2009-08-21
     *
     * This means that we'll see our own meta-data when we perform a put
     * and then transition to a state with external meta-data; as in
     * shard create with no lease which is followed by a delay request
     * lease.
     *
     * FIXME: drew 2010-04-27 This is the root cause of trac #4509
     * where we have a put pending, we receive meta-data kicking us
     * back to CR_SHARD_STATE_TO_WAIT_META, the put completes causing
     * an advance to CR_SHARD_STATE_WAIT_META but is ignored, the
     * external update for it comes in, and causes us to advance to
     * CR_SHARD_STATE_DELAY_LEASE_ACQUISITION which fails because
     * a lease exists.
     */
    if (shard->state_flags & CRSSF_META_SOURCE_OTHER) {
        do_update = 1;
        to_wait_meta = 0;
    /*
     * Defer handling the potentially conflciting update until the
     * pending put completes.
     */
    } else if (shard->shard_meta &&
               (cr_replication_type_flags(shard_meta->persistent.type) &
                CRTF_META_DISTRIBUTED) &&
               shard->put_count) {
        plat_log_msg(21853, LOG_CAT_META,
                     PLAT_LOG_LEVEL_DIAGNOSTIC,
                     "cr_shard %p node %u shard 0x%lx vip group %d home %d"
                     " lease len %3.1f expires %s ignored from %d pending puts",
                     shard, cr->config.my_node, shard->sguid,
                     shard->vip_group_id,
                     shard_meta->persistent.current_home_node,
                     shard_meta->persistent.lease_usecs / (float)PLAT_MILLION,
                     plat_log_timeval_to_string(&lease_expires),
                     shard->put_count);
        do_update = 0;
        to_wait_meta = 0;
    /* Try for last-write wins with eventual consistency */
    } else if (shard->shard_meta &&
               (cr_replication_type_flags(shard_meta->persistent.type) &
                CRTF_META_DISTRIBUTED) &&
               shard_meta->persistent.current_home_node !=
               shard->cr->config.my_node) {
        /* Force initial startup delay to resolve conflict */
        shard->after_restart = 1;
        shard->after_recovery = 0;

        /*
         * FIXME: drew 2010-04-23 This is incorrect because the other
         * end is doing exactly the same thing which will result in
         * neither node hosting the VIPs.
         *
         * Best "correct" behavior is probably going through a no-lease
         * period
         */
        do_update = 1;
        to_wait_meta = 1;
    /*
     * XXX: drew 2009-08-19  I think we assume that the right thing will happen
     * with CRSSF_REQUEST_LEASE.
     */
    } else if ((shard->state_flags & CRSSF_RENEW_LEASE) &&
               shard_meta->persistent.current_home_node !=
               cr->config.my_node &&
               /* Could be a stale message. */
               shard_meta->persistent.ltime >=
               shard->shard_meta->persistent.ltime) {
        /*
         * This is a programming failure since we should have forgotten
         * about being the home node before getting here.  With ltime
         * fencing it's probably safe.  Provided that no reads were
         * started after the expiration time returned by the last
         * successful #rms_put_shard_meta it should be correct too.
         *
         * XXX: drew 2009-06-03 We can only be fatal if we have a
         * properly functioning timeout that will transition us to
         * other state when we are expired.  We don't yet.
         *
         */
        plat_log_msg(21363, LOG_CAT_META,
#ifdef notyet
                     PLAT_LOG_LEVEL_FATAL,
#else
                     PLAT_LOG_LEVEL_WARN,
#endif
                     "cr_shard %p node %u shard 0x%lx vip group %d home %d"
                     " lease len %3.1f expires %s"
                     " with local shard state %s",
                     shard, cr->config.my_node, shard->sguid,
                     shard->vip_group_id,
                     shard_meta->persistent.current_home_node,
                     shard_meta->persistent.lease_usecs / (float)PLAT_MILLION,
                     plat_log_timeval_to_string(&lease_expires),
                     cr_shard_state_to_string(shard->state));
#ifdef notyet
        plat_abort();
        distributed_update = 0;
        to_wait_meta = 0;
#else
        plat_assert(shard->state != CR_SHARD_STATE_TO_SHUTDOWN);
        plat_assert(shard->state != CR_SHARD_STATE_SHUTDOWN);
        do_update = 1;
        to_wait_meta = 1;
#endif
    } else {
        do_update = 0;
        to_wait_meta = 0;
    }

    if (do_update) {
        /* Consumes shard_meta */
        cr_shard_update_meta(shard, SDF_SUCCESS, shard_meta, lease_expires);
        shard_meta = NULL;

        /*
         * Make sure whatever had been proposed but may or may not
         * have completed has been replaced by the newer meta-data.
         */
        cr_shard_update_proposed_shard_meta(shard);

        /* FIXME: drew 2009-08-19 Merge with cr_shard_update_meta? */
        if (shard->shard_meta &&
            cr_replication_type_flags(shard->shard_meta->persistent.type) &
            CRTF_MANAGE_DATA) {
            /*
             * XXX: drew 2009-05-31 It would be cleaner to have a
             * separate CR_SHARD_WAIT_META_FIRST state in which we know
             * we do not yet have replicas.
             */
            for (i = 0; i < shard->shard_meta->persistent.num_replicas; ++i) {
                replica_meta = shard->shard_meta->replicas[i];
                if (!shard->replicas[i]) {
                    cr_shard_alloc_replica(shard, i, replica_meta);
                }
            }
        }
    }

    if (to_wait_meta) {
        plat_assert(shard->state != CR_SHARD_STATE_TO_SHUTDOWN);
        plat_assert(shard->state != CR_SHARD_STATE_SHUTDOWN);
        cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
    }

    /*
     * This needs to be separate from the above code because it must
     * follow the update.
     *
     * XXX: drew 2009-12-16 This should move to cr_shard_check_state
     * so every state change which isn't caused explicitly by an
     * event (like the above to_wait_meta case) is handled in the same
     * place.
     *
     * This has been done singly for case CR_SHARD_STATE_SWITCH_BACK_2/
     * cr_shard_check_state_switch_back_2(shard) as a safer fix for
     * trac #3029 in the days before GA1.
     */
    switch (shard->state) {
    case CR_SHARD_STATE_INITIAL:
        cr_shard_set_state(shard, CR_SHARD_STATE_WAIT_META);
        break;

    case CR_SHARD_STATE_WAIT_META:
        cr_shard_set_state(shard, CR_SHARD_STATE_WAIT_META);
        break;

    case CR_SHARD_STATE_DELAY_LEASE_ACQUISITION:
        plat_assert(shard->shard_meta);

        /* Lease granted elsewhere */
        /*
         * XXX: drew 2009-12-25 Does this include lease_liveness? Do we
         * want to check against CR_HOME_NODE_NONE instead?  It would be
         * less confusing to have a #cr_shard_owned function.
         */
        if (shard->shard_meta->persistent.lease_usecs > 0) {
            cr_shard_set_state(shard, CR_SHARD_STATE_WAIT_META);
        /* Replica may have become non-authoritative */
        } else if (shard->local_replica >= 0) {
            replica_state =
                shard->shard_meta->replicas[shard->local_replica]->persistent.state;
            /* can no longer become home node */
            if (shard->shard_meta->persistent.lease_usecs > 0 ||
                (replica_state != CR_REPLICA_STATE_AUTHORITATIVE &&
                 replica_state != CR_REPLICA_STATE_SYNCHRONIZED)) {
                cr_shard_set_state(shard, CR_SHARD_STATE_WAIT_META);
            }
        }
        break;

    case CR_SHARD_STATE_SWITCH_BACK_2:
        cr_shard_check_state_switch_back_2(shard);
        break;

    default:
        break;
    }

    /*
     * Potentially foreign updates are ignored when this node is the
     * source of meta-data because we don't want them
     */
    if (shard_meta) {
        cr_shard_meta_free(shard_meta);
    }
}

/** @brief Allocate replica state with reference count of one */
static void
cr_shard_alloc_replica(struct cr_shard *shard, int index,
                       struct cr_shard_replica_meta *replica_meta) {
    int failed;
    struct cr_replica *replica;

    plat_assert(index < SDF_REPLICATION_MAX_REPLICAS);
    plat_assert(!shard->replicas[index]);

    failed = !plat_calloc_struct(&replica);
    plat_assert_always(!failed);

    replica->shard = shard;
    replica->index = index;
    replica->node = replica_meta->persistent.pnode;
    replica->state = CR_REPLICA_STATE_INITIAL;
    TAILQ_INIT(&replica->src_recovery_op_list);
    TAILQ_INIT(&replica->dest_recovery_op_list);
    replica->ref_count = 1;

    shard->replicas[index] = replica;

    if (replica->node == shard->cr->config.my_node) {
        shard->local_replica = index;
    }

    cr_replica_set_state(replica, CR_REPLICA_STATE_INITIAL);
}

/**
 * @brief Return non-zero if this shard could acquire or retain a vip lease
 *
 * XXX: drew 2010-03-03 This is here for SDF_REPLICATION_V1_2_WAY, although
 * it was easier to just make the call non-conditional and we've picked up
 * other checks too.
 *
 * In some cases, the checks are incomplete.
 *
 * @return boolean, non-zero for allowed
 */
static int
cr_shard_vip_group_lease_allowed(struct cr_shard *shard) {
    int flags;
    int hosted;
    struct cr_shard *other_shard;
    struct sdf_vip_group_group *inter_group_group;
    struct sdf_vip_group *intra_group;
    struct sdf_vip_group *other_group;
    int ret;
    int preference;
    int preferred_hosted;

    if (shard->shard_meta) {
        flags = cr_replication_type_flags(shard->shard_meta->persistent.type);
    } else {
        flags = 0;
    }

    if (shard->sguid == SDF_SHARDID_INVALID)  {
        plat_log_msg(21839, LOG_CAT_VIP, PLAT_LOG_LEVEL_TRACE,
                     "cr_shard %p node %u shard 0x%lx vip group %d"
                     " lease not allowed since sguid not assigned",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id);
        ret = 0;
    } else if (!shard->shard_meta) {
        plat_log_msg(21840, LOG_CAT_VIP, PLAT_LOG_LEVEL_TRACE,
                     "cr_shard %p node %u shard 0x%lx vip group %d"
                     " lease not allowed since shard_meta is unset",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id);
        ret = 0;
    } else if (shard->shard_meta->persistent.current_home_node !=
               CR_HOME_NODE_NONE &&
               shard->shard_meta->persistent.current_home_node !=
               shard->cr->config.my_node) {
        plat_log_msg(21841, LOG_CAT_VIP, PLAT_LOG_LEVEL_TRACE,
                     "cr_shard %p node %u shard 0x%lx vip group %d"
                     " lease not allowed since current home is %u",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id,
                     shard->shard_meta->persistent.current_home_node);
        ret = 0;
    /*
     * Intra node vip groups can be hosted when
     *
     * 1.  The maximum per group will not be exceeded
     *     (2 for SDF_REPLICATION_V1_2_WAY)
     *
     * 2.  The maximum per group is < 2, this is a preferred intra node
     *     vip group, or the preferred intra node vip group is already
     *     attached to the same shard.
     */
    } else if (flags & CRTF_INDEX_VIP_GROUP_ID) {
        plat_assert((flags & CRTF_MANAGE_META) && !(flags & CRTF_MANAGE_DATA));

        inter_group_group =
            sdf_vip_config_get_group_group_by_gid(shard->cr->config.vip_config,
                                                  shard->vip_group_id);
        plat_assert(inter_group_group);

        intra_group =
            sdf_vip_config_get_vip_group(shard->cr->config.vip_config,
                                         shard->vip_group_id);
        plat_assert(intra_group);

        preference =
            sdf_vip_group_get_node_preference(intra_group,
                                              shard->cr->config.my_node);
        /* This would imply that my node is not in vip group set */
        plat_assert(preference != -1);

        hosted = 0;
        preferred_hosted = 0;
        /*
         * XXX: drew 2010-03-26  This is incorrect where there are multiple
         * inter-node vip groups.  We should verify that matches on the
         * other nodes.
         *
         * This would work anyways if shards are not associated with more
         * than one inter-node vip group group.
         */
        LIST_FOREACH(other_shard, &shard->cr->shard_list, shard_list_entry) {
            if (other_shard != shard && other_shard->sguid == shard->sguid &&
                other_shard->vip_group_id != VIP_GROUP_ID_INVALID) {
                if (other_shard->state_flags &
                    (CRSSF_REQUEST_LEASE|CRSSF_RENEW_LEASE)) {
                    ++hosted;
                }
                other_group =
                    sdf_vip_config_get_vip_group(shard->cr->config.vip_config,
                                                 other_shard->vip_group_id);
                plat_assert(other_group);

                if (other_shard->state_flags & CRSSF_RENEW_LEASE &&
                    !sdf_vip_group_get_node_preference(other_group,
                                                       other_shard->cr->
                                                       config.my_node)) {
                    ++preferred_hosted;
                }
            }
        }

        if (hosted >= inter_group_group->max_group_per_node) {
            plat_log_msg(21799, LOG_CAT_VIP, PLAT_LOG_LEVEL_TRACE,
                         "cr_shard %p node %u shard 0x%lx vip group %d"
                         " lease not allowed due to %d others hosted",
                         shard, shard->cr->config.my_node, shard->sguid,
                         shard->vip_group_id, hosted);
            ret = 0;
        } else if (inter_group_group->max_group_per_node > 1 &&
                   preference && !preferred_hosted) {
            plat_log_msg(21800, LOG_CAT_VIP, PLAT_LOG_LEVEL_TRACE,
                         "cr_shard %p node %u shard 0x%lx vip group %d"
                         " lease not allowed since preferred group not hosted",
                         shard, shard->cr->config.my_node, shard->sguid,
                         shard->vip_group_id);
            ret = 0;
        } else {
            plat_log_msg(21842, LOG_CAT_VIP, PLAT_LOG_LEVEL_TRACE,
                         "cr_shard %p node %u shard 0x%lx vip group %d"
                         " lease allowed with preference %d"
                         " preffered hosted %d",
                         shard, shard->cr->config.my_node, shard->sguid,
                         shard->vip_group_id, preference, preferred_hosted);
            ret = 1;
        }
    } else {
        ret = 1;
    }

    return (ret);
}

/**
 * @brief Check state on intra-node vip groups
 *
 * Check state on all intra-node vip groups associated with
 * the shard because state may have changed.  Fail shards back to
 * #CR_SHARD_STATE_TO_WAIT_META when they should no longer hold vips.
 *
 * XXX: drew 2010-03-03 Note that this will run recursively until
 * all shards in the inter-node vip group bound to the same local
 * container have achieved their goal state.  While sub-optimal,
 * with the current release having only two vips in a group this is not
 * a big deal.
 *
 * @param shard <IN> Shard which is using a vip-group based replication
 * scheme as implied by
 * cr_replication_type_flags(shard->shard_meta->persistent.type) &
 * CRTF_INDEX_VIP_GROUP_ID)
 */
static void
cr_shard_vip_group_check(struct cr_shard *shard) {
    int flags;
    struct cr_shard *other_shard;
    struct cr_shard *next_shard;
    int allowed;
    int lease_started;

    if (shard->sguid != SDF_SHARDID_INVALID &&
        shard->vip_group_id != VIP_GROUP_ID_INVALID && shard->shard_meta) {
        flags = cr_replication_type_flags(shard->shard_meta->persistent.type);

        LIST_FOREACH_SAFE(other_shard, &shard->cr->shard_list, shard_list_entry,
                          next_shard) {
            if (other_shard != shard && other_shard->sguid == shard->sguid) {
                allowed = cr_shard_vip_group_lease_allowed(other_shard);
                lease_started = other_shard->state_flags &
                    (CRSSF_REQUEST_LEASE|CRSSF_RENEW_LEASE);
                if (!allowed && lease_started) {
                    plat_assert(shard->proposed_shard_meta);
                    cr_shard_set_state(other_shard,
                                       CR_SHARD_STATE_TO_WAIT_META);
                } else {
                    cr_shard_check_state(other_shard, NULL);
                }
            }
        }
    }
}

/**
 * @brief Enter CR_SHARD_STATE_CREATE_LEASE or CR_SHARD_STATE_CREATE_NO_LEASE
 */
static void
cr_shard_create_enter(struct cr_shard *shard) {
    plat_assert(shard->state == CR_SHARD_STATE_CREATE_LEASE ||
                shard->state == CR_SHARD_STATE_CREATE_NO_LEASE);

    shard->seqno = CR_FIRST_VALID_SEQNO;
}

/**
 * @brief Enter CR_SHARD_STATE_DELAY_LEASE_ACQUISITION
 *
 * Immediately leave when this node is the preferred home and has an
 * authoritative local replica.
 */
static void
cr_shard_delay_lease_acquisition_enter(struct cr_shard *shard) {
    int flags;
    struct cr_shard_replica_meta *replica_meta;
    plat_event_fired_t fired;
    int tmp;
    int preference_numerator;
    int preference_denominator;
    int64_t delay_usecs;
    struct timeval delay;

    plat_assert(shard->shard_meta);
    flags = cr_replication_type_flags(shard->shard_meta->persistent.type);
    plat_assert_either(flags & CRTF_INDEX_VIP_GROUP_ID,
                       flags & CRTF_INDEX_SHARDID);
    plat_assert_imply(flags & CRTF_INDEX_SHARDID,
                      shard->local_replica >= 0);
    plat_assert(!shard->shard_meta->persistent.lease_usecs);

    tmp = cr_shard_get_preference(shard, shard->cr->config.my_node,
                                  CR_SHARD_GET_PREFERENCE_START_DELAY,
                                  &preference_numerator,
                                  &preference_denominator);
    plat_assert(tmp == SDF_SUCCESS);

    if (flags & CRTF_INDEX_SHARDID) {
        replica_meta = shard->shard_meta->replicas[shard->local_replica];
        plat_assert(replica_meta);

        plat_assert(replica_meta->persistent.state ==
                    CR_REPLICA_STATE_AUTHORITATIVE ||
                    replica_meta->persistent.state ==
                    CR_REPLICA_STATE_SYNCHRONIZED);
    }

    /*
     * XXX: drew 2009-06-10 where this is initial startup and not a crash
     * recovery we may want a longer delay until all the other nodes to come
     * up or at least restart mutual redo when it happens.
     */

    /*
     * The two cases where we avoid the time delay are
     *
     * 1.  (!(flags & CRTF_META_DISTRIBUTED) && !preference_numerator)
     *
     *     Safe because we have authoritative meta-data and know we are
     *     preferred
     *
     * 2.  (shard->shard_meta->persistent.type == SDF_REPLICATION_V1_2_WAY &&
     *      shard->shard_meta->lease_liveness && !shard->after_restart)
     *
     *     As safe as the liveness system or our time out because we've waited
     *     during initial startup and have gotten out of
     *     CR_SHARD_STATE_WAIT_META because the replicator meta-storage
     *     system created a no-lease entry either due to its timeout or
     *     the liveness system.
     *
     *     Handling the V1_NPLUS_1 case would require
     *     !preference_numerator,
     *     !shard->after_restart, and preference_numerator to select
     *     only from the set of live nodes to handle the simultaneous
     *     restart of other nodes situation.
     *
     * XXX: drew 2009-12-15 Will this cause an incorrect switch back if
     * we don't immediately receive the message claiming the lease?
     */
    if ((!(flags & CRTF_META_DISTRIBUTED) && !preference_numerator) ||
        (shard->shard_meta->persistent.type == SDF_REPLICATION_V1_2_WAY &&
         !shard->after_restart && !shard->after_recovery)) {
        cr_shard_set_state(shard, CR_SHARD_STATE_REQUEST_LEASE);
    } else {
        plat_assert(!shard->lease_acquisition_delay_event);

        /*
         * Delay at least half the switch-over interval so that the preferred
         * node has a chance and uniformly distribute the other attempts
         * accross the remainder.
         */
        delay_usecs = shard->cr->config.lease_usecs / 2;
        delay_usecs = delay_usecs +
            (preference_numerator * delay_usecs) / preference_denominator;
        delay.tv_usec = delay_usecs % PLAT_MILLION;
        delay.tv_sec = delay_usecs / PLAT_MILLION;

        plat_log_msg(21843, LOG_CAT_LEASE, PLAT_LOG_LEVEL_DEBUG,
                     "cr_shard %p node %u shard 0x%lx vip group %d"
                     " delay %ld secs %lu usecs before lease request "
                     " with preference %d of %d",
                     shard, shard->cr->config.my_node,
                     shard->sguid, shard->vip_group_id,
                     delay.tv_sec, delay.tv_usec, preference_numerator,
                     preference_denominator);

        /* Hold refcount until timeout event is freed */
        ++shard->ref_count;

        fired = plat_event_fired_create(shard->cr->callbacks.single_scheduler,
                                        &cr_shard_acquire_delay_done_cb,
                                        shard);
        /*
         * FIXME: drew 2010-03-11 Have a name specifically associating
         * the event with this shard so that if we have problems we can
         * figure out what's going on.
         */
        shard->lease_acquisition_delay_event =
            plat_timer_dispatcher_timer_alloc(shard->cr->callbacks.timer_dispatcher,
                                              "cr_shard delay lease acquisition",
                                              LOG_CAT_EVENT,
                                              fired, 1 /* free_count */,
                                              &delay,
                                              PLAT_TIMER_RELATIVE,
                                              NULL /* rank_ptr */);
    }

    shard->after_recovery = 0;
}

/**
 * @brief Delay in CR_SHARD_STATE_DELAY_LEASE_ACQUISITION complete
 *
 * Continue on to CR_SHARD_STATE_REQUEST_LEASE when the timeout was
 * delivered with the state machine still in the same state of the
 * iteration which scheduled the timeout.
 */
static void
cr_shard_acquire_delay_done_cb(plat_closure_scheduler_t *context, void *env,
                               struct plat_event *event) {
    struct cr_shard *shard = (struct cr_shard *)env;
    int flags;

    plat_assert(shard);
    plat_assert(shard->shard_meta);

    flags = cr_replication_type_flags(shard->shard_meta->persistent.type);

    if (shard->state == CR_SHARD_STATE_DELAY_LEASE_ACQUISITION &&
        shard->lease_acquisition_delay_event == event) {
        shard->lease_acquisition_delay_event = NULL;
        cr_shard_free_event(shard, event);

        if (!(flags & CRTF_INDEX_VIP_GROUP_ID) ||
            cr_shard_vip_group_lease_allowed(shard)) {
            cr_shard_set_state(shard, CR_SHARD_STATE_REQUEST_LEASE);
        } else {
            cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
        }
    }
}

/**
 * @brief Free event associated with shard
 *
 * To handle the race conditions between asynchronous execution of
 * a timer event and its cancellation, the free funciton is asynchronous
 * with completion guaranteeing the timer function will never be called.
 *
 * We hold a reference count on the surround state until the free
 * completes.  This starts the asynchronous free and decrements the
 * reference count on completion.
 */
static void
cr_shard_free_event(struct cr_shard *shard, struct plat_event *event) {
    plat_event_free_done_t free_done_cb;

    free_done_cb =
        plat_event_free_done_create(shard->cr->callbacks.single_scheduler,
                                    &cr_shard_event_free_cb, shard);

    plat_event_free(event, free_done_cb);
}


/** @brief Callback on async cancellation of event */
static void
cr_shard_event_free_cb(plat_closure_scheduler_t *context, void *env) {
    struct cr_shard *shard = (struct cr_shard *)env;
    cr_shard_ref_count_dec(shard);
}

/**
 * @brief Leave CR_SHARD_STATE_DELAY_LEASE_ACQUISITION
 *
 * The timer event triggering the transition to CR_SHARD_SATE_REQUEST_LEASE
 * is cancelled.
 */
static void
cr_shard_delay_lease_acquisition_leave(struct cr_shard *shard) {
    struct plat_event *event = shard->lease_acquisition_delay_event;
    if (event) {
        shard->lease_acquisition_delay_event = NULL;
        cr_shard_free_event(shard, event);
    }
}

/*
 * XXX: drew 2009-05-22 We have interesting situations which arise from
 * the request timeouts (from the liveness subsystem) and state machine
 * changes being orthogonal.
 *
 * A request serial number and indirection through a thin wrapping
 * structure would allow us to detet this without blocking the entire
 * state machine on having a positive or negative resolution before
 * the whole thing resets.
 *
 * struct request_state {
 *    int serial;
 *    // rest
 * }
 */

/**
 * @brief Enter CR_SHARD_STATE_REQUEST_LEASE
 *
 * An initial lease is requested with the appropriate ltime and
 * shard_meta_seqno advance with completion leading to the startup and
 * recovery process.
 */
static void
cr_shard_request_lease_enter(struct cr_shard *shard) {
    struct cr_shard_replica_meta *replica_meta;
    cr_shard_put_meta_cb_t put_meta_cb;
    int flags;

    plat_assert(shard->shard_meta);
    plat_assert(!shard->shard_meta->persistent.lease_usecs);
    flags = cr_replication_type_flags(shard->shard_meta->persistent.type);
    plat_assert_imply(flags & CRTF_MANAGE_DATA, shard->local_replica >= 0);

    if (shard->local_replica >= 0) {
        replica_meta = shard->shard_meta->replicas[shard->local_replica];

        /* XXX: drew 2009-05-22 Move into a common lease renewal function */
        plat_assert(replica_meta->persistent.state ==
                    CR_REPLICA_STATE_AUTHORITATIVE ||
                    replica_meta->persistent.state ==
                    CR_REPLICA_STATE_SYNCHRONIZED);
    }

    cr_shard_update_proposed_shard_meta(shard);

    shard->proposed_shard_meta->persistent.lease_usecs =
        shard->cr->config.lease_usecs;
    shard->proposed_shard_meta->persistent.lease_liveness =
        shard->cr->config.lease_liveness;

    /*
     * Although this node may have been the previous home node, ltime
     * must advance for fencing because we've lost our state.
     */
    ++shard->proposed_shard_meta->persistent.ltime;
    shard->proposed_shard_meta->persistent.current_home_node =
        shard->cr->config.my_node;

    ++shard->proposed_shard_meta->persistent.shard_meta_seqno;

    put_meta_cb =
        cr_shard_put_meta_cb_create(shard->cr->callbacks.single_scheduler,
                                    &cr_shard_request_lease_cb, shard);
    ++shard->ref_count;
    cr_shard_put_meta(shard, put_meta_cb);
}

/**
 * @brief Callback invoked for CR_SHARD_STATE_REQUEST_LEASE #rms_put_shard_meta
 */
static void
cr_shard_request_lease_cb(struct plat_closure_scheduler *context, void *env,
                          SDF_status_t status) {
    struct cr_shard *shard = (struct cr_shard *)env;

    plat_assert_imply(shard->state == CR_SHARD_STATE_REQUEST_LEASE &&
                      status == SDF_SUCCESS, shard->shard_meta);

    /*
     * XXX: We need to make this one of the events which doesn't allow
     * us to advance out of CR_SHARD_STATE_TO_WAIT_META until it completes.
     *
     */
    if (shard->state != CR_SHARD_STATE_REQUEST_LEASE) {
        /* Ignore */
    } else if (status != SDF_SUCCESS) {
        cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
    } else if (shard->shard_meta &&
               (cr_replication_type_flags(shard->shard_meta->persistent.type)
                & CRTF_MANAGE_DATA)) {
        cr_shard_set_state(shard, CR_SHARD_STATE_GET_SEQNO);
    } else {
        cr_shard_set_state(shard, CR_SHARD_STATE_RW);
    }

    cr_shard_ref_count_dec(shard);
}

/**
 * @brief Enter CR_SHARD_STATE_GET_SEQNO
 *
 * Get sequence number at start of recovery process
 */
static void
cr_shard_get_seqno_enter(struct cr_shard *shard) {
    rr_last_seqno_cb_t cb;

    ++shard->stat_counters->master_recovery_started;
    ++shard->cr->total_stat_counters->master_recovery_started;

    cb = rr_last_seqno_cb_create(shard->cr->callbacks.single_scheduler,
                                 &cr_shard_get_seqno_cb, shard);

    ++shard->ref_count;
    rr_get_last_seqno_async(shard->cr->callbacks.send_msg,
                            shard->cr->config.my_node, shard->cr->config.my_node,
                            shard->sguid, cb);
}

/**
 * @brief Callback on sequence number get for  CR_SHARD_STATE_GET_SEQNO
 *
 * Advances to the next state or failure.  The current sequence number is
 * set to an outstanding window after what was in persistent storage.
 */
static void
cr_shard_get_seqno_cb(struct plat_closure_scheduler *context, void *env,
                      SDF_status_t status, uint64_t seqno) {
    struct cr_shard *shard = (struct cr_shard *)env;

    if (status == SDF_SUCCESS) {
        /*
         * XXX: drew 2009-06-02 This can be incorrect in multiple
         * failure scenarios.  We need to start with the maximum of the
         * sequence number and end of the highest SYNCED range and then
         * add an outstanding window.
         *
         * It would be most reasonable to pass the sequence number
         * unchanged to CR_SHARD_STATE_UPDATE_1 which can then make
         * the determination when it iterates the replica ranges.
         */
        shard->seqno = seqno +
            shard->proposed_shard_meta->persistent.outstanding_window;
        cr_shard_set_state(shard, CR_SHARD_STATE_UPDATE_1);
    } else {
        cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
    }

    cr_shard_ref_count_dec(shard);
}

/**
 * @brief Enter CR_SHARD_STATE_UPDATE_1
 *
 * Convert the open end of all #CR_REPLICA_RANGE_ACTIVE and
 * #CR_REPLICA_RANGE_SYNCED ranges to CR_REPLICA_RANGE_MUTUAL_REDO
 * so they serve as data sources during #CR_SHARD_STATE_MUTUAL_REDO
 *
 * XXX: drew 2009-06-02 This is sub-optimal for SYNCED ranges in the failure
 * case since MUTUAL_REDO ranges can become UNDO ranges, although it should be
 * safe because anything which can become authoritative will have seen the same
 * updates in the range.
 */
static void
cr_shard_update_1_enter(struct cr_shard *shard) {
    int i;
    struct cr_shard_replica_meta *replica_meta;
    struct cr_persistent_shard_range *range;
    /** @brief Start of the CR_REPLICA_RANGE_MUTUAL_REDO range being added */
    uint64_t range_start;
    cr_shard_put_meta_cb_t put_meta_cb;

    plat_assert(shard->shard_meta);
    plat_assert(shard->shard_meta->persistent.lease_usecs > 0);
    plat_assert(shard->local_replica >= 0);
    replica_meta = shard->shard_meta->replicas[shard->local_replica];
    plat_assert(replica_meta->persistent.state ==
                CR_REPLICA_STATE_AUTHORITATIVE ||
                replica_meta->persistent.state ==
                CR_REPLICA_STATE_SYNCHRONIZED);
    plat_assert(shard->seqno >=
                shard->shard_meta->persistent.outstanding_window);

    cr_shard_update_proposed_shard_meta(shard);
    plat_assert(shard->proposed_shard_meta);

    if (shard->seqno > 2 * shard->shard_meta->persistent.outstanding_window +
        CR_FIRST_VALID_SEQNO) {
        range_start = shard->seqno -
            2 * shard->shard_meta->persistent.outstanding_window;
    } else {
        range_start = CR_FIRST_VALID_SEQNO;
    }

    for (i = 0; i < shard->proposed_shard_meta->persistent.num_replicas; ++i) {
        replica_meta = shard->proposed_shard_meta->replicas[i];
        plat_assert(replica_meta->persistent.nrange > 0);
        range = &replica_meta->ranges[replica_meta->persistent.nrange - 1];
        if (range->range_type == CR_REPLICA_RANGE_ACTIVE ||
            range->range_type == CR_REPLICA_RANGE_SYNCED) {

            /*
             * The mutual redo range is passed the end of data which has
             * been seen by this replica so it will not participate.
             */
            if (range->len != CR_SHARD_RANGE_OPEN &&
                range_start > range->start + range->len) {
                /*
                 * Must be because it's been off-line since it was cleanly
                 * shutdown.
                 */
                plat_assert(range->range_type == CR_REPLICA_RANGE_SYNCED);
            /*
             * The mutual redo range covers a subset of the existing range,
             * in which case it's truncated with the mutual redo appended
             * at the end.
             */
            } else {
                /* Since ranges are in-order this will apply to all of them */
                plat_assert(range->len == CR_SHARD_RANGE_OPEN ||
                            shard->seqno >= range->start +
                            range->len);

                /*
                 * Subsume all ranges this starts before
                 *
                 * XXX: Should add cr_shard_meta_remove_last_range and
                 * cr_shard_meta__get_range.
                 */
                while (replica_meta->persistent.nrange > 0 &&
                       range_start <= range->start) {
                    --replica_meta->persistent.nrange;
                    if (replica_meta->persistent.nrange > 0) {
                        range = &replica_meta->ranges[
                            replica_meta->persistent.nrange - 1];
                    } else {
                        range = NULL;
                    }
                }

                /* Truncate the last range if the redo begins within it */
                if (replica_meta->persistent.nrange > 0 &&
                    (range->len == CR_SHARD_RANGE_OPEN ||
                     range_start < range->start + range->len)) {
                    range->range_type = CR_REPLICA_RANGE_SYNCED;
                    range->len = range_start - range->start;
                }

                /* Add the redo as a fresh range */
                cr_shard_replica_meta_add_range(replica_meta,
                                                CR_REPLICA_RANGE_MUTUAL_REDO,
                                                range_start,
                                                shard->seqno - range_start);
            }
            /* XXX: drew 2009-06-02 Should assert that meta is still canonical */
        }
    }

    /* ltime already advanced when we got the lease */
    ++shard->proposed_shard_meta->persistent.shard_meta_seqno;

    put_meta_cb =
        cr_shard_put_meta_cb_create(shard->cr->callbacks.single_scheduler,
                                    &cr_shard_update_1_cb, shard);
    ++shard->ref_count;
    cr_shard_put_meta(shard, put_meta_cb);
}

/**
 * @brief Callback invoked for CR_SHARD_STATE_REQUEST_LEASE #rms_put_shard_meta
 *
 * Consumes one shard reference count
 */
static void
cr_shard_update_1_cb(struct plat_closure_scheduler *context, void *env,
                     SDF_status_t status) {
    struct cr_shard *shard = (struct cr_shard *)env;

    /*
     * XXX: We need to make this one of the events which doesn't allow
     * us to advance out of CR_SHARD_STATE_TO_WAIT_META until it completes.
     */
    if (shard->state == CR_SHARD_STATE_UPDATE_1) {
        if (status == SDF_SUCCESS) {
            cr_shard_set_state(shard, CR_SHARD_STATE_MUTUAL_REDO);
        } else {
            cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
        }
    }

    cr_shard_ref_count_dec(shard);
}

/** @brief Enter CR_SHARD_STATE_MUTUAL_REDO */
static void
cr_shard_mutual_redo_enter(struct cr_shard *shard) {
    int i;
    struct cr_replica *replica;
    struct cr_shard_replica_meta *replica_meta;
    struct cr_persistent_shard_range *range;
    int started;
    const char *how;

    plat_assert(shard->shard_meta);
    plat_assert(shard->shard_meta->persistent.lease_usecs > 0);
    plat_assert(shard->local_replica >= 0);
    replica_meta = shard->shard_meta->replicas[shard->local_replica];
    plat_assert(replica_meta->persistent.state ==
                CR_REPLICA_STATE_AUTHORITATIVE ||
                replica_meta->persistent.state == CR_REPLICA_STATE_SYNCHRONIZED);
    plat_assert(shard->seqno >=
                shard->shard_meta->persistent.outstanding_window);

    started = 0;
    for (i = 0; i < shard->shard_meta->persistent.num_replicas; ++i) {
        replica = shard->replicas[i];
        replica_meta = shard->shard_meta->replicas[i];
        plat_assert(replica_meta->persistent.nrange > 0);
        range = &replica_meta->ranges[replica_meta->persistent.nrange - 1];

        if (replica->state == CR_REPLICA_STATE_LIVE_OFFLINE &&
            range->range_type == CR_REPLICA_RANGE_MUTUAL_REDO)  {
            cr_replica_set_state(replica, CR_REPLICA_STATE_MUTUAL_REDO);
            ++started;
            how = "repair";
        } else {
            how = "skip";
        }

        plat_log_msg(21364, LOG_CAT_RECOVERY_REDO,
                     PLAT_LOG_LEVEL_DEBUG,
                     "cr_shard %p node %u shard 0x%lx vip group %d mutual redo"
                     " %s replica %d local to node %u in-core state %s"
                     " last range type %s start %lld len %lld",
                     shard, shard->cr->config.my_node,
                     shard->sguid, shard->vip_group_id, how, i, replica->node,
                     cr_replica_state_to_string(replica->state),
                     cr_replica_range_type_to_string(range->range_type),
                     (long long)range->start,
                     (long long)range->len);
    }

    /* The local replica better be working */
    plat_assert_always(started);
#if 0
    /*
     * Change this so that if we don't have more than one replica (the local
     * one) we immediately jump to the next real state .
     */
    if (!started) {
        cr_shard_set_state(shard, CR_SHARD_STATE_UPDATE_2);
    }
#endif
}

/** @brief Enter CR_SHARD_STATE_UPDATE_2 */
static void
cr_shard_update_2_enter(struct cr_shard *shard) {
    int i;
    struct cr_replica *replica;
    struct cr_shard_replica_meta *replica_meta;
    int range_index;
    struct cr_persistent_shard_range *range;
    struct cr_persistent_shard_range *range_before;
    cr_shard_put_meta_cb_t put_meta_cb;

    plat_assert(shard->proposed_shard_meta);

    for (i = 0; i < shard->proposed_shard_meta->persistent.num_replicas; ++i) {
        replica = shard->replicas[i];
        plat_assert(replica);

        replica_meta = shard->proposed_shard_meta->replicas[i];
        plat_assert(replica_meta);
        plat_assert(replica_meta->persistent.nrange > 0);

        range_index = replica_meta->persistent.nrange - 1;
        range = &replica_meta->ranges[range_index];

        /* We have no active ranges which are the only sort that can be open */
        plat_assert(range->len != CR_SHARD_RANGE_OPEN);

        if (replica_meta->persistent.state == CR_REPLICA_STATE_AUTHORITATIVE) {
            /* XXX: drew 2009-05-26 need to accomodate clean shutdown */
            plat_assert(range->range_type == CR_REPLICA_RANGE_MUTUAL_REDO);

            if (replica->state == CR_REPLICA_STATE_MUTUAL_REDO_SCAN_DONE) {
                if (range_index > 0) {
                    range_before = &replica_meta->ranges[range_index - 1];
                    plat_assert(range_before->range_type ==
                                CR_REPLICA_RANGE_SYNCED);
                    plat_assert(range_before->len !=
                                CR_SHARD_RANGE_OPEN);
                    plat_assert(range_before->start +
                                range_before->len == range->start);
                    range_before->len += range->len;
                    --replica_meta->persistent.nrange;
                    range = range_before;
                } else {
                    range->range_type = CR_REPLICA_RANGE_SYNCED;
                }
                cr_shard_replica_meta_add_range(replica_meta,
                                                CR_REPLICA_RANGE_ACTIVE,
                                                range->start +
                                                range->len,
                                                CR_SHARD_RANGE_OPEN);
            } else {
                range->range_type = CR_REPLICA_RANGE_UNDO;
                replica_meta->persistent.state = CR_REPLICA_STALE;
            }
        } else {
            plat_assert(range->range_type != CR_REPLICA_RANGE_MUTUAL_REDO);
        }
    }

    ++shard->proposed_shard_meta->persistent.shard_meta_seqno;
    put_meta_cb =
        cr_shard_put_meta_cb_create(shard->cr->callbacks.single_scheduler,
                                    &cr_shard_update_2_cb, shard);
    ++shard->ref_count;
    cr_shard_put_meta(shard, put_meta_cb);
}

/**
 * @brief Callback on meta-data put from CR_SHARD_STATE_UPDATE_2
 *
 * Consumes one shard reference count.
 */
static void
cr_shard_update_2_cb(struct plat_closure_scheduler *context, void *env,
                     SDF_status_t status) {
    struct cr_shard *shard = (struct cr_shard *)env;
    /*
     * XXX: We need to make this one of the events which doesn't allow
     * us to advance out of CR_SHARD_STATE_TO_WAIT_META until it completes.
     */
    if (shard->state == CR_SHARD_STATE_UPDATE_2) {
        if (status == SDF_SUCCESS) {
            cr_shard_set_state(shard, CR_SHARD_STATE_RW);
        } else {
            cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
        }
    }

    cr_shard_ref_count_dec(shard);
}

/** @brief Enter CR_SHARD_STATE_RW */
static void
cr_shard_rw_enter(struct cr_shard *shard) {
    plat_assert(shard->shard_meta);

    ++shard->stat_counters->master_recovery_complete;
    ++shard->cr->total_stat_counters->master_recovery_complete;

    if (shard->shard_meta->persistent.type == SDF_REPLICATION_V1_2_WAY) {
        cr_shard_rw_enter_for_v1_2way(shard);
    } else if (cr_replication_type_flags(shard->shard_meta->persistent.type) &
               CRTF_MANAGE_DATA) {
        cr_shard_rw_enter_for_data(shard);
    }

    /*
     * XXX: drew 2009-09-13 Rather than being done on an ad-hoc basis,
     * notifications should be triggered in #cr_shard_set_state when
     * there is a change in access flags.
     */
    cr_shard_notify(shard, cr_shard_notify_cb_null);
}

/** @brief CR_SHARD_STATE_RW enter code specific to SDF_REPLICATION_V1_2_WAY */
static void
cr_shard_rw_enter_for_v1_2way(struct cr_shard *shard) {
    struct sdf_vip_group_group *inter_group_group;
    struct sdf_vip_group *intra_group;
    struct cr_shard *other_shard;
    struct sdf_vip_group_group *other_inter_group_group;

    plat_assert(shard->shard_meta);
    plat_assert(shard->shard_meta->persistent.type == SDF_REPLICATION_V1_2_WAY);

    inter_group_group =
        sdf_vip_config_get_group_group_by_gid(shard->cr->config.vip_config,
                                              shard->vip_group_id);
    plat_assert(inter_group_group);
    intra_group = sdf_vip_config_get_vip_group(shard->cr->config.vip_config,
                                               shard->vip_group_id);
    plat_assert(intra_group);

    /* Allow non-preferred vips to immediately join the preferred vip */
    if (!shard->cr->config.initial_preference &&
        !sdf_vip_group_get_node_preference(intra_group,
                                           shard->cr->config.my_node)) {
        LIST_FOREACH(other_shard, &shard->cr->shard_list, shard_list_entry) {
            other_inter_group_group =
                sdf_vip_config_get_group_group_by_gid(shard->cr->config.vip_config,
                                                      other_shard->vip_group_id);
            if (other_inter_group_group == inter_group_group) {
                other_shard->after_restart = 0;
            }
        }
    }
}

/** @brief CR_SHARD_STATE_RW enter code where copy_replicator handles data */
static void
cr_shard_rw_enter_for_data(struct cr_shard *shard) {
    int i;
    struct cr_replica *replica;
    struct cr_shard_replica_meta *replica_meta;
    int range_index;
    struct cr_persistent_shard_range *range;

    plat_assert(shard->shard_meta);
    plat_assert(cr_replication_type_flags(shard->shard_meta->persistent.type) &
                CRTF_MANAGE_DATA);

    /*
     * XXX: drew 2009-06-28 We should be doing this before we
     * get to the rw state
     */

    for (i = 0; i < shard->shard_meta->persistent.num_replicas; ++i) {
        replica = shard->replicas[i];
        plat_assert(replica);

        replica_meta = shard->proposed_shard_meta->replicas[i];
        plat_assert(replica_meta);

        plat_assert(replica_meta->persistent.nrange > 0);
        range_index = replica_meta->persistent.nrange - 1;
        range = &replica_meta->ranges[range_index];

        if (replica->state == CR_REPLICA_STATE_MUTUAL_REDO_SCAN_DONE) {
            plat_assert(replica_meta->persistent.state ==
                        CR_REPLICA_STATE_AUTHORITATIVE);
            plat_assert(range->range_type == CR_REPLICA_RANGE_ACTIVE);
            cr_replica_set_state(replica, CR_REPLICA_STATE_RECOVERED);
        }

        /* #cr_shard_check_state will handle all others */
    }

}

/**
 * @brief Enter CR_SHARD_STATE_SWITCH_BACK_2
 *
 * schedule the timeout event
 */
static void
cr_shard_switch_back_2_enter(struct cr_shard *shard) {
    struct timeval delay;
    plat_event_fired_t fired;

    plat_assert(shard->state == CR_SHARD_STATE_SWITCH_BACK_2);
    plat_assert(!shard->switch_back_timeout_event);

    if (shard->cr->config.switch_back_timeout_usecs > 0) {
        delay.tv_sec = shard->cr->config.switch_back_timeout_usecs /
            PLAT_MILLION;
        delay.tv_usec = shard->cr->config.switch_back_timeout_usecs %
            PLAT_MILLION;

        /* Hold refcount until timeout event is freed */
        ++shard->ref_count;

        fired = plat_event_fired_create(shard->cr->callbacks.single_scheduler,
                                        &cr_shard_switch_back_timeout_done_cb,
                                        shard);
        shard->switch_back_timeout_event =
            plat_timer_dispatcher_timer_alloc(shard->cr->callbacks.timer_dispatcher,

                                              "cr_shard switch back timeout",
                                              LOG_CAT,
                                              fired, 1 /* free_count */,
                                              &delay,
                                              PLAT_TIMER_RELATIVE,
                                              NULL /* rank_ptr */);
    }

    cr_shard_check_state_switch_back_2(shard);
}

/**
 * @brief Timeout in CR_SHARD_STATE_SWITCH_BACK_2 complete
 *
 * Continue on to CR_SHARD_STATE_TO_WAIT_META when the timeout was
 * delivered with the state machine still in the same state of the
 * iteration which scheduled the timeout.
 */
static void
cr_shard_switch_back_timeout_done_cb(plat_closure_scheduler_t *context,
                                     void *env, struct plat_event *event) {
    struct cr_shard *shard = (struct cr_shard *)env;

    plat_assert(shard);

    if (shard->state == CR_SHARD_STATE_SWITCH_BACK_2 &&
        shard->switch_back_timeout_event == event) {
        shard->switch_back_timeout_event = NULL;
        cr_shard_free_event(shard, event);
        cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
    }
}

/**
 * @brief Leave CR_SHARD_STATE_SWITCH_BACK_2
 *
 * shard->switch_back_timeout is cancelled
 */
static void
cr_shard_switch_back_2_leave(struct cr_shard *shard) {
    struct plat_event *event = shard->switch_back_timeout_event;
    if (event) {
        shard->switch_back_timeout_event = NULL;
        cr_shard_free_event(shard, event);
    }
}

static void cr_shard_yield_put_meta_cb(struct plat_closure_scheduler *context,
                                       void *env, SDF_status_t status_arg);

/**
 * @brief Enter CR_SHARD_STATE_YIELD
 *
 * Put meta-data indicating that no lease exists and proceed to
 * CR_SHARD_STATE_TO_WAIT_META on success
 */
static void
cr_shard_yield_enter(struct cr_shard *shard) {
    cr_shard_put_meta_cb_t put_meta_cb;

    plat_assert(shard->shard_meta);
    plat_assert(cr_replication_type_flags(shard->proposed_shard_meta->
                                          persistent.type) &
                CRTF_META_DISTRIBUTED);
    plat_assert(shard->shard_meta->persistent.current_home_node ==
                shard->cr->config.my_node);
    plat_assert(!shard->put_count);

    put_meta_cb =
        cr_shard_put_meta_cb_create(shard->cr->callbacks.single_scheduler,
                                    &cr_shard_yield_put_meta_cb, shard);

    cr_shard_update_proposed_shard_meta(shard);
    plat_assert(shard->proposed_shard_meta);

    shard->proposed_shard_meta->persistent.last_home_node =
        shard->proposed_shard_meta->persistent.current_home_node;
    shard->proposed_shard_meta->persistent.current_home_node =
        CR_HOME_NODE_NONE;
    shard->proposed_shard_meta->persistent.lease_usecs = 0;
    shard->proposed_shard_meta->persistent.lease_liveness = 0;
    ++shard->proposed_shard_meta->persistent.shard_meta_seqno;

    __sync_add_and_fetch(&shard->ref_count, 1);
    __sync_add_and_fetch(&shard->op_count, 1);

    cr_shard_put_meta(shard, put_meta_cb);
}

static void
cr_shard_yield_put_meta_cb(struct plat_closure_scheduler *context,
                           void *env, SDF_status_t status_arg) {
    struct cr_shard *shard = (struct cr_shard *)env;


    plat_log_msg(21382, LOG_CAT_META,
                 status_arg == SDF_SUCCESS ? PLAT_LOG_LEVEL_TRACE :
                 PLAT_LOG_LEVEL_WARN,
                 "cr_shard %p node %u shard 0x%lx vip group %d"
                 " put_meta complete: %s",
                 shard, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id, sdf_status_to_string(status_arg));

    plat_assert(shard->shard_meta);

    if (shard->state != CR_SHARD_STATE_YIELD) {
    } else if (shard->shard_meta->persistent.current_home_node !=
               shard->cr->config.my_node) {
        /* Use initial back-off to resolve conflict */
        shard->after_restart = 1;
        shard->after_recovery = 0;
        cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
    } else {
        plat_assert(status_arg != SDF_SUCCESS);

        /*
         * XXX: drew 2010-04-30 The only correct solutions are to
         * retry or fail; although knowing how #replicator_meta_storage
         * works this should never happen so failing is safe.
         *
         * replicator_meta_storage could change so that's no longer the case,
         * although instead of the direct approach here we'd want to have a
         * timed back off.
         *
         * For shutdown to work while in the back-off state we'd
         * need to keep a reference to the timer in the #cr_shard
         * structure and cancel it in a leave function from
         * CR_SHARD_STATE_YIELD.
         */

#if 1
        plat_fatal("Impossible situation");
#else
        cr_shard_set_state(shard, CR_SHARD_STATE_YIELD);
#endif
    }

    cr_shard_op_count_dec(shard);
    cr_shard_ref_count_dec(shard);
}


/** @brief State structure for #cr_shard_put_meta */
struct cr_shard_put_meta_state {
    /** @brief Associated shard */
    struct cr_shard *shard;

    /** @brief Completion closure */
    cr_shard_put_meta_cb_t cb;
};

/**
 * @brief Create shard.
 *
 * @brief shard <IN> Existing cr_shard structure.  Must be in
 * CR_SHARD_STATE_WAIT_META to succeed.  Implemented as a thin
 * wrapper around #cr_shard_put_meta.
 *
 * @brief shard_meta <IN> Proposed shard meta data which is consumed
 * by #cr_shard_create
 */
static void
cr_shard_create(struct cr_shard *shard,
                struct cr_shard_meta *shard_meta,
                cr_shard_put_meta_cb_t cb) {
    int flags;

    flags = cr_replication_type_flags(shard_meta->persistent.type);

    /*
     * FIXME: drew 2009-08-09 This is not quite correct for the V1
     * replication features, where the shards on both ends are
     * created independantly.  We need to add a WAIT_LOCAL_CREATE
     * state where the code blocks on local creation.
     */

    /*
     * FIXME: drew 2009-08-11 We need to skip over the meta-data
     * operation and return a synthetic response if meta-data already
     * exists.
     *
     * Or perform proper merging.
     *
     * Or most intuitively, keep the remote meta-data in the store
     * and provide sguid and container name which are filled in on
     * every update to proposed meta data, which means sanitizing
     * sguid use and making sure it always comes out of the #cr_shard
     * field.
     */
    if ((shard->state != CR_SHARD_STATE_WAIT_META &&
         shard->state != CR_SHARD_STATE_TO_WAIT_META) ||
        ((shard->shard_meta || shard->proposed_shard_meta) &&
         !(flags & CRTF_META_DISTRIBUTED))) {
        plat_log_msg(21365, LOG_CAT, PLAT_LOG_LEVEL_WARN,
                     "cr_shard %p node %u shard 0x%lx vip group %d create"
                     " failed state %s", shard, shard->cr->config.my_node,
                     shard->sguid, shard->vip_group_id,
                     cr_shard_state_to_string(shard->state));
        /* XXX: drew 2009-05-29 Should be more specific on the failure */
        plat_closure_apply(cr_shard_put_meta_cb, &cb, SDF_CONTAINER_EXISTS);
    } else {
        /*
         * This is a legitimate case where the remote shard creation happens
         * before local.  Where a home node already exists this will fail
         * due to the existing lease which is handled by the caller.
         *
         * XXX: drew 2009-09-18 We should validate that the right magic
         * is happening here.  Ideally, shard_meta would derive from
         * shard->proposed_shard_meta.
         */
        if (shard->proposed_shard_meta) {
            cr_shard_meta_free(shard->proposed_shard_meta);
        }
        shard->proposed_shard_meta = shard_meta;
        if (flags & CRTF_CREATE_LEASE_IMMEDIATE) {
            cr_shard_set_state(shard, CR_SHARD_STATE_CREATE_LEASE);
        } else if (flags & CRTF_CREATE_LEASE_DELAY) {
            cr_shard_set_state(shard, CR_SHARD_STATE_CREATE_NO_LEASE);
        } else {
            plat_fatal("Bad flags");
        }
        cr_shard_put_meta(shard, cb);
    }
}

/**
 * @brief Puts shard->proposed_shard_meta
 *
 * A #rms_put_shard_meta or #rms_create_shard_meta is executed depending
 * on state.  Successful create mode triggers a transition to
 * CR_SHARD_STATE_RW, failed create CR_SHARD_STATE_TO_WAIT_META.
 *
 * The lease expiration time may be adjusted upwards but never backwards
 * except on CRTF_META_DISTRIBUTED replication types.
 *
 * Callers must account for state changes between their
 * #cr_shard_put_meta invocation and cb application.
 *
 * @param shard <IN> the shard
 * @param cb <IN> applied on completion
 */
static void
cr_shard_put_meta(struct cr_shard *shard, cr_shard_put_meta_cb_t cb) {
    struct cr_shard_put_meta_state *state;
    int failed;
    rms_shard_meta_cb_t put_meta_cb;
    SDF_status_t status;

    plat_assert_imply(shard->state != CR_SHARD_STATE_CREATE_LEASE &&
                      shard->state != CR_SHARD_STATE_CREATE_NO_LEASE,
                      shard->shard_meta);
    plat_assert(shard->proposed_shard_meta);

    plat_log_msg(21791, LOG_CAT_META, PLAT_LOG_LEVEL_TRACE,
                 "cr_shard %p node %u shard 0x%lx vip group %d"
                 " current seqno %lld proposed %"FFDC_LONG_STRING(512)"",
                 shard, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id,
                 shard->shard_meta ?
                 (long long)shard->shard_meta->persistent.shard_meta_seqno :
                 -1LL, cr_shard_meta_to_string(shard->proposed_shard_meta));

    failed = !plat_calloc_struct(&state);
    plat_assert_always(!failed);
    state->shard = shard;
    state->cb = cb;

    /* XXX: drew 2009-08-109 Move to a lease allowed function */
    if (!(shard->state_flags & CRSSF_NO_LEASE)) {
        /*
         * Avoid having two in-flight lease renewals at a time.
         * #cr_shard_self_update_meta_common will resart the timer after the
         * lease has been granted.
         */
        cr_shard_lease_renewal_cancel(shard);

        shard->proposed_shard_meta->persistent.current_home_node =
            shard->cr->config.my_node;

        /* XXX: drew 2009-05-26 This needs to change for controlled shutdown */
        shard->proposed_shard_meta->persistent.lease_usecs =
            shard->cr->config.lease_usecs;
        shard->proposed_shard_meta->persistent.lease_liveness =
            shard->cr->config.lease_liveness;

        plat_log_msg(21367, LOG_CAT_LEASE, PLAT_LOG_LEVEL_TRACE,
                     "cr_shard %p node %u shard 0x%lx vip group %d"
                     " state %s putting meta lease len %3.1f",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id,
                     cr_shard_state_to_string(shard->state),
                     shard->proposed_shard_meta->persistent.lease_usecs /
                     (float)PLAT_MILLION);
    }

    shard->proposed_shard_meta->persistent.write_node =
        shard->cr->config.my_node;

    /*
     * Handle situations where the current meta-data does not match that
     * of the local shard. As of 2009-08-11 this included
     * SDF_REPLICATION_V1_N_PLUS_1 and SDF_REPLICATION_V1_2_WAY where
     * the shard and container attached to the VIP are that of the
     * current home node.
     */
    shard->proposed_shard_meta->persistent.sguid = shard->sguid;

    /*
     * XXX: drew 2009-08-11 Need to handle cname and cguid the same way if
     * these are not unique accross the cluster.
     */
#ifdef notyet
    shard->proposed_shard_meta->persistent.cguid = shard->cguid;
    if (shard->cname_valid) {
        plat_assert(sizeof (shard->proposed_shard_meta->persistent.cname) ==
                    sizeof (shard->cname));
        memcpy(&shard->proposed_shard_meta->persistent.cname,
               shard->cname, sizeof (shard->cname));
    }
#endif

    if (shard->local_vip_meta) {
        status = cr_shard_meta_replace_vip_meta(shard->proposed_shard_meta,
                                                shard->local_vip_meta);
    }

    put_meta_cb =
        rms_shard_meta_cb_create(shard->cr->callbacks.single_scheduler,
                                 &cr_shard_put_meta_cb, state);
    ++shard->ref_count;
    ++shard->op_count;
    ++shard->put_count;

    if (shard->state == CR_SHARD_STATE_CREATE_LEASE ||
        shard->state == CR_SHARD_STATE_CREATE_NO_LEASE) {
        rms_create_shard_meta(shard->cr->meta_storage,
                              shard->proposed_shard_meta, put_meta_cb);
    } else {
        rms_put_shard_meta(shard->cr->meta_storage, shard->proposed_shard_meta,
                           put_meta_cb);
    }
}

/**
 * @brief rms_put_shard_meta callback handler for #cr_shard_put_meta
 *
 * Consumes #shard_meta, one shard ref count and op count.
 */
static void
cr_shard_put_meta_cb(struct plat_closure_scheduler *context, void *env,
                     SDF_status_t status_arg, struct cr_shard_meta *shard_meta,
                     struct timeval lease_expires) {
    struct cr_shard_put_meta_state *state =
        (struct cr_shard_put_meta_state *)env;
    struct cr_shard *shard = state->shard;
    cr_shard_put_meta_cb_t cb = state->cb;
    int i;
    struct cr_replica *replica;
    struct cr_node *node;
    vnode_t pnode;
    int flags;
    SDF_status_t status_ret;


    if (!(shard->state_flags & CRSSF_META_SOURCE_SELF)) {
        plat_log_msg(21368, LOG_CAT_META, PLAT_LOG_LEVEL_WARN,
                     "cr_shard %p node %u shard 0x%lx vip group %d put meta"
                     " complete: %s state %s flags 0x%x do not include"
                     " CRSSF_META_SOURCE_SELF",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id, sdf_status_to_string(status_arg),
                     cr_shard_state_to_string(shard->state),
                     shard->state_flags);
    } else {
        plat_log_msg(21369, LOG_CAT_META,
                     status_arg == SDF_SUCCESS ? PLAT_LOG_LEVEL_DEBUG :
                     PLAT_LOG_LEVEL_WARN,
                     "cr_shard %p node %u shard 0x%lx vip group %d put meta"
                     " complete: %s",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id, sdf_status_to_string(status_arg));
    }

    if (shard_meta) {
        flags = cr_replication_type_flags(shard_meta->persistent.type);
    } else {
        flags = 0;
    }

    plat_assert_imply(status_arg == SDF_SUCCESS && shard_meta,
                      shard_meta->persistent.sguid == shard->sguid);
    plat_assert_imply(shard_meta && (flags & CRTF_INDEX_VIP_GROUP_ID),
                      shard_meta->persistent.intra_node_vip_group_id ==
                      shard->vip_group_id);

    cr_shard_self_update_meta_common(state->shard, status_arg, shard_meta,
                                     lease_expires);
    plat_assert_imply(status_arg == SDF_SUCCESS ||
                      status_arg == SDF_CONTAINER_EXISTS, shard->shard_meta);


    /* Before possible state change; ref count is still held until return */
    --shard->op_count;
    --shard->put_count;

    /*
     * Do shard level activity before replica level activity which may
     * be handled in the client's closure.
     *
     * Create is also considered successful when the container exists
     * and we're operating in distributed meta-data mode because the shards
     * on each node are created independently and there's a race between
     * local creation and a remote meta-data update.
     */
    if ((shard->state == CR_SHARD_STATE_CREATE_LEASE ||
         shard->state == CR_SHARD_STATE_CREATE_NO_LEASE) &&
        (status_arg == SDF_SUCCESS ||
         (((status_arg == SDF_CONTAINER_EXISTS ||
            status_arg == SDF_LEASE_EXISTS)) &&
          (flags & CRTF_META_DISTRIBUTED)))) {
        /* Replica state is only maintained in the data path */
        if (status_arg == SDF_SUCCESS && (flags & CRTF_MANAGE_DATA)) {
            for (i = 0; i < shard->shard_meta->persistent.num_replicas; ++i) {
                cr_shard_alloc_replica(shard, i,
                                       shard->shard_meta->replicas[i]);
                replica = shard->replicas[i];
                plat_assert(replica);
                /*
                 * FIXME: drew 2009-05-29 Need to handle case where nodes
                 * are no longer live.  Need to do right magic for I/O
                 * fencing.
                 *
                 * Shouldn't have to special case local node
                 */
                pnode = shard->shard_meta->replicas[i]->persistent.pnode;
                if (pnode != shard->cr->config.my_node) {
                    node = cr_get_node(shard->cr, pnode);
                    plat_assert(node);
                    plat_assert(node->state == CR_NODE_LIVE);
                }
                cr_replica_set_state(replica, CR_REPLICA_STATE_RECOVERED);
                /* Otherwise CR_REPLICA_STATE_MARK_FAILED */
            }
        }

        /* Don't become active if some one else owns the lease */
        if (status_arg != SDF_SUCCESS &&
            shard->shard_meta->persistent.current_home_node !=
            CR_HOME_NODE_NONE &&
            shard->shard_meta->persistent.current_home_node !=
            shard->cr->config.my_node) {
            plat_assert(status_arg == SDF_CONTAINER_EXISTS ||
                        status_arg == SDF_LEASE_EXISTS);
            cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
        } else if (flags &
                   (CRTF_CREATE_LEASE_IMMEDIATE|CRTF_CREATE_LEASE_NONE)) {
            cr_shard_set_state(shard, CR_SHARD_STATE_RW);
        } else if (flags & CRTF_CREATE_LEASE_DELAY) {
            cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
        } else {
            plat_fatal("Illegal flags");
        }
        status_ret = SDF_SUCCESS;
    } else {
        status_ret = status_arg;
        if (status_ret != SDF_SUCCESS) {
            if (shard->state != CR_SHARD_STATE_TO_SHUTDOWN) {
                cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
            }
        }
    }

    cr_shard_check_state(shard, NULL /* replica */);

    if (!cr_shard_put_meta_cb_is_null(&cb)) {
        plat_closure_apply(cr_shard_put_meta_cb, &cb, status_ret);
    }

    plat_free(state);

    /*
     * FIXME: drew 2009-06-17 This is getting messy - when an update fails the
     * proposed meta-data is no longer causally related to the current meta-data
     * so no future meta-data updates are going to succeed.
     *
     * In theory we could add an additional state before each meta-data
     * update which blocked on currently running meta-data updates to
     * complete but that wouldn't solve the problem.  When failures have
     * occurred forward progress cannot be made and recovery can't happen
     * because it's predicated on a certain state existing.
     *
     * The best thing to do may be to become read only until the lease
     * expires at which point we transition into CR_SHARD_STATE_TO_WAIT_META
     * and attempt whole-shard recovery.
     *
     * The current symptom of this is the assert during undo recovery
     * following a failure to update the meta-data.
     *
     * I think a transition to CR_SHARD_STATE_TO_WAIT_META unless we're
     * shutting down would be appropriate, although things are getting
     * so spread out with the cr_shard_set_state() calls that we want to
     * take the opportunity to explicitly enumerate the events.
     */

    cr_shard_ref_count_dec(shard);
}

/**
 * @brief Common code on a meta-data update initiated by this node
 *
 * Consumes #shard_meta
 */
static void
cr_shard_self_update_meta_common(struct cr_shard *shard,
                                 SDF_status_t status,
                                 struct cr_shard_meta *shard_meta,
                                 struct timeval lease_expires) {
    /* Ignore if we're now in a mode with external updates only */
    if (shard->state_flags & CRSSF_META_SOURCE_SELF) {
        cr_shard_update_meta(shard, status, shard_meta, lease_expires);
    } else {
        cr_shard_meta_free(shard_meta);
    }

    /* If a lease exists, continue to update it on timeouts */
    if (status == SDF_SUCCESS && !(shard->state_flags & CRSSF_NO_LEASE)) {
        cr_shard_lease_renewal_reset(shard);
    }
}

/**
 * @brief Common code to all meta-data updates
 *
 * @param shard_meta <IN> consumed
 *
 * XXX: drew 2009-09-13 The split between this and
 * #cr_shard_meta_udate_external is getting to be too big.
 * 1.  Make this unconditional
 *
 * 2.  Move the trivial checks for the internal update case into
 *     #cr_shard_put_meta_cb function.
 *
 * 3.  Merge the remainder into #cr_shard_meta_upate_external
 */
static void
cr_shard_update_meta(struct cr_shard *shard, SDF_status_t status,
                     struct cr_shard_meta *shard_meta_arg,
                     struct timeval lease_expires) {
    int flags;
    enum plat_log_level update_log_level;
    struct cr_shard_meta *cmp_meta;
    /* NULL when ownership has transfered, non-NULL when free needed */
    struct cr_shard_meta *shard_meta;

    shard_meta = shard_meta_arg;

    /* XXX: drew 2009-06-20 consolidate log messages */

    if (!shard_meta) {
        /* Ignore for now */
    /*
     * Process updates when we have nothing local, they're newer, or we're
     * in the distributed eventually consistent case and accepting all foreign
     * nodes leases is the simplest path to correctness.
     *
     * XXX: drew 2009-08-20 Re-evaluate the distributed case if we ever
     * make it faster instead of just using Paxos to get away from it.
     */
    } else if (!shard->shard_meta || shard_meta->persistent.shard_meta_seqno >
               shard->shard_meta->persistent.shard_meta_seqno ||
               ((cr_replication_type_flags(shard_meta->persistent.type) &
                 CRTF_META_DISTRIBUTED) &&
                !(shard->shard_meta->persistent.current_home_node ==
                  shard->cr->config.my_node &&
                  shard->shard_meta->persistent.lease_usecs &&
                  shard_meta->persistent.current_home_node ==
                  CR_HOME_NODE_NONE))) {
        if (!shard->shard_meta) {
            update_log_level = PLAT_LOG_LEVEL_DEBUG;
        } else if (shard->shard_meta) {
            /* Home node changes are most interesting */
            if (shard_meta->persistent.current_home_node !=
                shard->shard_meta->persistent.current_home_node) {
                update_log_level = PLAT_LOG_LEVEL_DIAGNOSTIC;
            /* Places that aren't lease renewals are interest ing */
            } else if (plat_log_enabled(LOG_CAT_META, PLAT_LOG_LEVEL_DEBUG)) {
                cmp_meta = cr_shard_meta_dup(shard->shard_meta);
                cmp_meta->persistent.ltime = shard_meta->persistent.ltime;
                cmp_meta->persistent.lease_usecs =
                    shard_meta->persistent.lease_usecs;
                if (!cr_shard_meta_cmp(shard_meta, cmp_meta)) {
                    update_log_level = PLAT_LOG_LEVEL_DEBUG;
                } else {
                    update_log_level = PLAT_LOG_LEVEL_TRACE;
                }
                cr_shard_meta_free(cmp_meta);
            /* And mere lease renewals are usually noise */
            } else {
                update_log_level = PLAT_LOG_LEVEL_TRACE;
            }
            flags =
                cr_replication_type_flags(shard->shard_meta->persistent.type);
            /*
             * XXX: drew 2009-08-21 It seems that we get duplicates; is
             * that because of seeing our own updates or what?
             */
            if (shard_meta && shard_meta->persistent.shard_meta_seqno >
                shard->shard_meta->persistent.shard_meta_seqno + 1) {
                plat_log_msg(21370, LOG_CAT_META,
                             (flags & CRTF_META_DISTRIBUTED) ?
                             PLAT_LOG_LEVEL_DIAGNOSTIC : PLAT_LOG_LEVEL_WARN,
                             "cr_shard %p node %u shard 0x%lx vip group %d"
                             " missed meta"
                             " update last seqno %lld received %lld",
                             shard, shard->cr->config.my_node, shard->sguid,
                             shard->vip_group_id,
                             (long long)shard->shard_meta->persistent.shard_meta_seqno,
                             (long long)shard_meta->persistent.shard_meta_seqno);
            }
            cr_shard_meta_free(shard->shard_meta);
        }
        shard->shard_meta = shard_meta;
        shard_meta = NULL;
        shard->lease_expires = lease_expires;
        if (shard->shard_meta->persistent.current_home_node !=
            CR_HOME_NODE_NONE) {
            shard->after_restart = 0;
            shard->after_recovery = 0;
        }

        plat_log_msg(21792, LOG_CAT_META, update_log_level,
                     "cr_shard %p node %u shard 0x%lx vip group %d"
                     " lease expires %s meta now %"FFDC_LONG_STRING(512)"",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id,
                     plat_log_timeval_to_string(&shard->lease_expires),
                     cr_shard_meta_to_string(shard->shard_meta));
        cr_shard_notify(shard, cr_shard_notify_cb_null);
    /*
     * Ignore equal sequence numbers which may be expected due to
     * pilot beacon operation.
     *
     * FIXME: drew 2009-08-19 This is incorrect for the distributed
     * mode of operation where sequence numbers are not causal.
     */
    } else if (shard_meta->persistent.shard_meta_seqno <
               shard->shard_meta->persistent.shard_meta_seqno) {
        plat_log_msg(21372, LOG_CAT_META,
                     shard_meta->persistent.current_home_node !=
                     shard->cr->config.my_node ? PLAT_LOG_LEVEL_WARN :
                     PLAT_LOG_LEVEL_TRACE,
                     "cr_shard %p node %u shard 0x%lx vip group %d"
                     " stale meta received "
                     " current seqno %lld received %lld",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id,
                     shard->shard_meta ?
                     (long long)shard->shard_meta->persistent.shard_meta_seqno :
                     -1LL,
                     shard_meta ?
                     (long long)shard_meta->persistent.shard_meta_seqno :
                     -1LL);
    }

    if (shard_meta) {
        cr_shard_meta_free(shard_meta);
    }
}

/**
 * @brief Duplicate current shard meta into proposed shard-meta
 *
 * This is only used where externally provided meta-data is being used
 * because otherwise proposed_shard_meta could become inconsistent
 * with updates that have been queued.
 */
static void
cr_shard_update_proposed_shard_meta(struct cr_shard *shard) {
    plat_assert(shard->shard_meta);

    if (shard->proposed_shard_meta) {
        cr_shard_meta_free(shard->proposed_shard_meta);
    }

    shard->proposed_shard_meta = cr_shard_meta_dup(shard->shard_meta);
    plat_assert(shard->proposed_shard_meta);
}

struct cr_shard_notify_state {
    /** @brief Shard for notification */
    struct cr_shard *shard;

    /** @brief When this notification started */
    struct timeval start_tv;

    /** @brief Reference count must hit 0 for closure application */
    int ref_count;

    /** @brief Callback applied on completion */
    cr_shard_notify_cb_t cb;

    /** @brief Entry in shard->pending_notification_list */
    TAILQ_ENTRY(cr_shard_notify_state) pending_notification_list_entry;
};

/**
 * @brief Notify clients registered with #sdf_replicator_add_notifier
 *
 * We serialize the user's receipt of notifications by using the
 * #cr_shard_notify_do co-routine which keeps at most one in-flight
 * until the user's hand shake has completed.
 *
 * Note that where no information has been changed the user will not
 * be notified although the cb argument will execute as described.
 *
 * One reference count is held on the state which is released
 * when all necessary notifications have been started.
 *
 * One reference count is held on the shard object until the async
 * notification process completes.
 *
 * @param shard <IN> Shard on which notification is performed.
 * @param cb <IN> Closure applied when this notification and all
 * preceding notifications have been delivered
 */
static void
cr_shard_notify(struct cr_shard *shard, cr_shard_notify_cb_t cb) {
    struct cr_shard_notify_state *state;

    ++shard->ref_count;

    plat_calloc_struct(&state);
    plat_assert(state);

    state->shard = shard;
    plat_closure_apply(plat_timer_dispatcher_gettime,
                       &shard->cr->callbacks.gettime,
                       &state->start_tv);

    state->ref_count = 1;
    state->cb = cb;

    plat_log_msg(21373, LOG_CAT_NOTIFY, PLAT_LOG_LEVEL_TRACE,
                 "cr_shard %p node %u shard 0x%lx vip group %d seqno %lld"
                 " notify start",
                 shard, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id,
                 (long long)shard->shard_meta->persistent.shard_meta_seqno);

    TAILQ_INSERT_TAIL(&shard->pending_notification_list, state,
                      pending_notification_list_entry);
    cr_shard_notify_do(shard);
}

/**
 * @brief Co-routine which delivers the next notification
 *
 * Consumes the extra reference count on the state from creation
 */
static void
cr_shard_notify_do(struct cr_shard *shard) {
    struct copy_replicator *cr = shard->cr;
    struct cr_shard_notify_state *state =
        TAILQ_FIRST(&shard->pending_notification_list);
    sdf_replicator_notification_complete_cb_t complete_cb;
    enum sdf_replicator_access access;
    fthWaitEl_t *unlock;
    struct cr_notifier *notifier;
    struct cr_notifier_shard *notifier_shard;
    int events;
    struct cr_shard_meta *shard_meta;

    complete_cb =
        sdf_replicator_notification_complete_cb_create(cr->callbacks.single_scheduler,
                                                       &cr_shard_notify_complete_cb,
                                                       state);

    if (!shard->notification_running && state) {
        shard->notification_running = 1;

        access = cr_shard_get_access(shard);

        unlock = fthLock(&cr->notifiers.lock, 0 /* read lock */, NULL);
        LIST_FOREACH(notifier, &cr->notifiers.list, notifiers_list_entry) {
            LIST_FOREACH(notifier_shard, &notifier->shard_list,
                         shard_list_entry) {
                if (notifier_shard->vip_group_id == shard->vip_group_id &&
                    (shard->vip_group_id != VIP_GROUP_ID_INVALID ||
                     notifier_shard->sguid == shard->sguid)) {
                        break;
                }
            }

            events = 0;
            if (!notifier_shard) {
                events |= SDF_REPLICATOR_EVENT_FIRST;
            }
            if (!notifier_shard || notifier_shard->ltime !=
                shard->shard_meta->persistent.ltime) {
                events |= SDF_REPLICATOR_EVENT_LTIME;
            }
            if (!notifier_shard || notifier_shard->access != access) {
                events |= SDF_REPLICATOR_EVENT_ACCESS;
            }
            if (!notifier_shard ||
                notifier_shard->expires.tv_sec != shard->lease_expires.tv_sec ||
                notifier_shard->expires.tv_usec != shard->lease_expires.tv_usec) {
                events |= SDF_REPLICATOR_EVENT_LEASE;
            }
            if (!events &&
                (!notifier_shard || notifier_shard->shard_meta_seqno !=
                 shard->shard_meta->persistent.shard_meta_seqno)) {
                events |= SDF_REPLICATOR_EVENT_OTHER;
            }

            if (events) {
                shard_meta = cr_shard_meta_dup(shard->shard_meta);
                plat_assert(shard_meta);

                if (!notifier_shard) {
                    plat_calloc_struct(&notifier_shard);
                    plat_assert(notifier_shard);
                    notifier_shard->sguid = shard->sguid;
                    notifier_shard->vip_group_id = shard->vip_group_id;
                    LIST_INSERT_HEAD(&notifier->shard_list, notifier_shard,
                                     shard_list_entry);
                }

                notifier_shard->ltime = shard_meta->persistent.ltime;
                notifier_shard->shard_meta_seqno =
                    shard_meta->persistent.shard_meta_seqno;
                notifier_shard->access = access;
                notifier_shard->expires = shard->lease_expires;

                /* Reference until the notification callback is delivered */
                ++state->ref_count;

                plat_log_msg(21374, LOG_CAT_NOTIFY,
                             PLAT_LOG_LEVEL_DEBUG,
                             "cr_shard %p node %u shard 0x%lx vip group %d"
                             " seqno %lld notify"
                             " events {%s} access %s expires %s",
                             shard, cr->config.my_node, shard->sguid,
                             shard->vip_group_id,
                             (long long)notifier_shard->shard_meta_seqno,
                             sdf_replicator_events_to_string(events),
                             sdf_replicator_access_to_string(access),
                             plat_log_timeval_to_string(&notifier_shard->expires));

                /*
                 * XXX: drew 2009-07-05 We should be applying the users
                 * closures once we have released the lock.
                 */
                plat_closure_apply(sdf_replicator_notification_cb,
                                   &notifier->cb, events, shard_meta,
                                   access, notifier_shard->expires,
                                   complete_cb);
            }
        }
        fthUnlock(unlock);

        /* Initial extra reference count */
        cr_shard_notify_ref_count_dec(state);
    }
}

/**
 * @brief Closure applied when the client is done with notification
 *
 * This is called by the replicator client code when it has done all high
 * level state changes required by the notification such as cache flushes or
 * connection closes, thus allowing the transition to a writeable state to
 * stop until all previous IOs have been successfully fenced.
 */
static void
cr_shard_notify_complete_cb(plat_closure_scheduler_t *context, void *env) {
    struct cr_shard_notify_state *state = (struct cr_shard_notify_state *)env;
    cr_shard_notify_ref_count_dec(state);
}

/**
 * @brief Decrement shard notify state reference count
 *
 * On zero, cleanup is performed, the user callback applied,
 */
static void
cr_shard_notify_ref_count_dec(struct cr_shard_notify_state *state) {
    struct cr_shard *shard = state->shard;
    int after;
    cr_shard_notify_cb_t cb;
    struct timeval now_tv;
    int64_t elapsed_usec;

    after = __sync_sub_and_fetch(&state->ref_count, 1);
    plat_assert(after >= 0);
    if (!after) {
        plat_closure_apply(plat_timer_dispatcher_gettime,
                           &shard->cr->callbacks.gettime, &now_tv);

        elapsed_usec =
            ((int64_t)now_tv.tv_sec * PLAT_MILLION + now_tv.tv_usec) -
            ((int64_t)state->start_tv.tv_sec * PLAT_MILLION +
             state->start_tv.tv_usec);

        plat_log_msg(21375, LOG_CAT_NOTIFY,
                     PLAT_LOG_LEVEL_DEBUG,
                     "cr_shard %p node %u shard 0x%lx vip group %d"
                     " notification ref count 0 after %ld usec",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id, (long)elapsed_usec);

        cb = state->cb;
        shard->notification_running = 0;
        TAILQ_REMOVE(&shard->pending_notification_list, state,
                     pending_notification_list_entry);
        plat_free(state);
        if (!cr_shard_notify_cb_is_null(&cb)) {
            plat_closure_apply(cr_shard_notify_cb, &cb);
        }
        cr_shard_notify_do(shard);
        cr_shard_ref_count_dec(shard);
    }
}

static void
cr_shard_shutdown(struct cr_shard *shard) {
    plat_assert(shard->state != CR_SHARD_STATE_TO_SHUTDOWN);
    plat_assert(shard->state != CR_SHARD_STATE_SHUTDOWN);

    cr_shard_set_state(shard, CR_SHARD_STATE_TO_SHUTDOWN);

    cr_shard_ref_count_dec(shard);
}

static void
cr_shard_ref_count_dec(struct cr_shard *shard) {
    struct copy_replicator *cr = shard->cr;
    int after;
    int i;
    struct cr_replica *replica;

    after = __sync_sub_and_fetch(&shard->ref_count, 1);
    plat_assert(after >= 0);
    if (!after) {
        plat_log_msg(21376, LOG_CAT_SHUTDOWN,
                     PLAT_LOG_LEVEL_DEBUG,
                     "cr_shard %p node %u shard 0x%lx vip group %d refcount 0",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id);

        plat_assert(shard->state == CR_SHARD_STATE_SHUTDOWN);
        /* And make sure nothing which should have a ref count is out there */
        plat_assert(!shard->op_count);
        plat_assert(!shard->lease_acquisition_delay_event);
        plat_assert(!shard->lease_renewal_event);
        plat_assert(!shard->switch_back_timeout_event);

        if (shard->shard_meta) {
            for (i = 0; i < shard->shard_meta->persistent.num_replicas; ++i) {
                replica = shard->replicas[i];
                if (replica) {
                    cr_replica_ref_count_dec(replica);
                }
            }

            cr_shard_meta_free(shard->shard_meta);
        }

        if (shard->proposed_shard_meta) {
            cr_shard_meta_free(shard->proposed_shard_meta);
        }

        if (shard->local_vip_meta) {
            cr_vip_meta_free(shard->local_vip_meta);
        }

        if (shard->lock_container) {
            rklc_free(shard->lock_container);
        }

        plat_free(shard->delay_lease_acquisition_event_name);
        plat_free(shard->lease_renewal_event_name);
        plat_free(shard->stat_counters);

        plat_free(shard);

        cr_signal_shard_shutdown_complete(cr);
    }
}

/** @brief Decrement pending op-count in shard */
static void
cr_shard_op_count_dec(struct cr_shard *shard) {
    int after;

    after = __sync_sub_and_fetch(&shard->op_count, 1);
    if (!after) {
        /*
         * XXX: drew 2009-07-01 If we change the threading model for the
         * data-path to be thread-pool based this needs to become a
         * trampoline like #cr_shard_signal_replica_state.
         */
        cr_shard_check_state(shard, NULL /* replica */);
    }
}

/**
 * @brief Reset lease renewal timer
 *
 * We schedule a lease renewal for half a lease interval ahead of the expiration
 * time which seems like a good enough idea.  It may be better to figure out
 * what normal messaging, etc. delay are use some safety factor ahead of that.
 *
 * Where that time has already passed we immediately attempt a lease renewal.
 */
static void
cr_shard_lease_renewal_reset(struct cr_shard *shard) {
    struct copy_replicator *cr = shard->cr;
    plat_event_fired_t fired;
    struct timeval when_tv;

    plat_assert_imply(!(shard->state_flags & CRSSF_NO_LEASE),
                      shard->shard_meta);

    cr_shard_lease_renewal_cancel(shard);
    plat_assert(!shard->lease_renewal_event);

    if (!(shard->state_flags & CRSSF_NO_LEASE) &&
        !shard->shard_meta->persistent.lease_liveness) {
        plat_assert(shard->shard_meta->persistent.current_home_node ==
                    cr->config.my_node);

        when_tv.tv_usec = (shard->lease_expires.tv_usec -
                           cr->config.lease_usecs / 2) % PLAT_MILLION;
        when_tv.tv_sec = shard->lease_expires.tv_sec -
            cr->config.lease_usecs / 2 / PLAT_MILLION;
        if (when_tv.tv_usec < 0) {
            when_tv.tv_usec += PLAT_MILLION;
            ++when_tv.tv_sec;
        }

        plat_log_msg(21377, LOG_CAT_LEASE, PLAT_LOG_LEVEL_TRACE,
                     "cr_shard %p node %u shard 0x%lx vip group %d"
                     " state %s scheduling renewal callback at %s",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id,
                     cr_shard_state_to_string(shard->state),
                     plat_log_timeval_to_string(&when_tv));

        /* Hold refcount until timeout event is freed */
        ++shard->ref_count;

        fired = plat_event_fired_create(cr->callbacks.single_scheduler,
                                        &cr_shard_lease_renewal_cb, shard);
        shard->lease_renewal_event =
            plat_timer_dispatcher_timer_alloc(cr->callbacks.timer_dispatcher,
                                              shard->lease_renewal_event_name,
                                              LOG_CAT_EVENT, fired,
                                              1 /* free_count */, &when_tv,
                                              PLAT_TIMER_ABSOLUTE,
                                              NULL /* rank_ptr */);
    }
}

/**
 * @brief Called on lease renewal timer expiration
 *
 * Performs lease renewal iff appropriate for current shard state and
 * the renewal event is still valid thus implying that there isn't another
 * lease renewal scheduled since this event firing was set in motion.
 */
static void
cr_shard_lease_renewal_cb(plat_closure_scheduler_t *context, void *env,
                          struct plat_event *event) {
    struct cr_shard *shard = (struct cr_shard *)env;
    /*
     * Only act when we haven't hit the race condition where the event
     * delivery occurs after cancellation.
     */
    if (shard->lease_renewal_event == event) {
        plat_log_msg(21378, LOG_CAT_LEASE, PLAT_LOG_LEVEL_TRACE,
                     "cr_shard %p node %u shard 0x%lx vip group %d"
                     " state %s renewal callback",
                     shard, shard->cr->config.my_node, shard->sguid,
                     shard->vip_group_id,
                     cr_shard_state_to_string(shard->state));

        cr_shard_lease_renewal_cancel(shard);
        if (shard->state_flags & (CRSSF_RENEW_LEASE|CRSSF_REQUEST_LEASE)) {
            cr_shard_lease_renew(shard);
        }
    }
}

/*
 * @brief Renew lease and schedule next timeout on completion
 */
static void
cr_shard_lease_renew(struct cr_shard *shard) {
    plat_assert(shard->shard_meta);
    plat_assert(shard->shard_meta->persistent.current_home_node ==
                shard->cr->config.my_node);
    plat_assert(shard->state_flags & (CRSSF_RENEW_LEASE|CRSSF_REQUEST_LEASE));

    cr_shard_update_proposed_shard_meta(shard);
    plat_assert(shard->proposed_shard_meta);

    /* XXX: drew 2009-05-26 This needs to change for controlled shutdown */
    shard->proposed_shard_meta->persistent.lease_usecs =
        shard->cr->config.lease_usecs;
    ++shard->proposed_shard_meta->persistent.shard_meta_seqno;

    plat_log_msg(21379, LOG_CAT_LEASE, PLAT_LOG_LEVEL_TRACE,
                 "cr_shard %p node %u shard 0x%lx vip group %d"
                 " state %s lease renew"
                 " meta_seqno %u lease len %lu seconds",
                 shard, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id, cr_shard_state_to_string(shard->state),
                 shard->proposed_shard_meta->persistent.shard_meta_seqno,
                 shard->proposed_shard_meta->persistent.lease_usecs /
                 PLAT_MILLION);

    /*
     * XXX: drew 2009-06-03 We don't need a callback, do we want one for
     * logging?
     */
    cr_shard_put_meta(shard, cr_shard_put_meta_cb_null);
}


static void
cr_shard_lease_renewal_cancel(struct cr_shard *shard) {
    struct plat_event *event;

    /* Temporary allows reference count to hit 0 on shard */
    event = shard->lease_renewal_event;
    if (event) {
        shard->lease_renewal_event = NULL;
        cr_shard_free_event(shard, event);
    }
}

/** @brief Return access state for client */
static enum sdf_replicator_access
cr_shard_get_access(struct cr_shard *shard) {
    enum sdf_replicator_access ret;

    if ((shard->state_flags & CRSSF_ACCESS_NONE) ||
        (shard->shard_meta &&
         shard->shard_meta->persistent.current_home_node ==
         CR_HOME_NODE_NONE)) {
        ret = SDF_REPLICATOR_ACCESS_NONE;
    } else if (shard->state_flags & CRSSF_ACCESS_READ) {
        if (shard->state_flags & CRSSF_ACCESS_WRITE) {
            ret = SDF_REPLICATOR_ACCESS_RW;
        } else {
            ret = SDF_REPLICATOR_ACCESS_RO;
        }
    } else {
        ret = SDF_REPLICATOR_ACCESS_NONE;
        plat_fatal("Bad state flags");
    }

    return (ret);
}

/**
 * @brief Get shard statistics
 *
 * @param shard <IN> shard
 * @param stats <OUT> statistics
 * @return SDF_SUCCESS on success, otherwise on failure
 */
static SDF_status_t
cr_shard_get_stats(struct cr_shard *shard,
                   struct sdf_replicator_shard_stats *stats) {
    int i;
    struct cr_shard_replica_meta *replica_meta;
    struct cr_replica *replica;

#define item(lower) stats->lower = shard->stat_counters->lower;
    SDF_REPLICATOR_SHARD_STAT_COUNTER_ITEMS()
#undef item

#define item(lower) stats->lower = 0;
    SDF_REPLICATOR_SHARD_STAT_PULL_ITEMS()
#undef item

    if (shard->shard_meta) {
        if (shard->state_flags & CRSSF_ACCESS_WRITE) {
            stats->writeable = 1;
        }
        if (shard->state_flags & CRSSF_ACCESS_READ) {
            stats->readable = 1;
        }

        for (i = 0; i < shard->shard_meta->persistent.num_replicas; ++i) {
            replica_meta = shard->shard_meta->replicas[i];
            replica = shard->replicas[i];
            if (replica_meta->persistent.state ==
                CR_REPLICA_STATE_AUTHORITATIVE ||
                replica_meta->persistent.state ==
                CR_REPLICA_STATE_SYNCHRONIZED) {
                ++stats->num_authoritative_replicas;
            }

            if (replica && cr_replica_is_writeable(replica)) {
                ++stats->num_writeable_replicas;
            }
        }
    }

    return (SDF_SUCCESS);
}

/**
 * @brief Return op_meta from cached replicator meta storage state
 *
 * XXX: drew 2009-07-01 #cr_get_op_meta which provides the operation
 * meta-data needed for action node request routing and stale IO fencing
 * is currently implemented programatically from the CMC
 * #SDF_container_meta structure.
 *
 * Use #cr_shard_get_op_meta to implement that before we move away
 * from the redundant properties file, CMC, and replicator_meta_storage
 * code.
 *
 * Before that happens this also has to fit into the threading model.  We
 * could also move away from the separate action node code since all IOs
 * are local.
 *
 * @param shard <IN> Shard which has received meta-data from rms
 * @param op_meta <OUT> Operation meta data.  May be a pointer into
 * #SDF_protocol_msg op_meta field.
 * @return SDF_SUCCESS on success, otherwise on failure
 */
static SDF_status_t
cr_shard_get_op_meta(struct cr_shard *shard,
                     struct sdf_replication_op_meta *op_meta) {
    SDF_status_t ret;
    struct sdf_replication_shard_meta *op_shard_meta = &op_meta->shard_meta;
    struct cr_shard_meta *shard_meta = shard->shard_meta;
    int i;

    if (!shard_meta) {
        ret = SDF_NO_META;
    } else if (shard_meta->persistent.current_home_node == CR_HOME_NODE_NONE ||
               !shard_meta->persistent.lease_usecs) {
        ret = SDF_UNAVAILABLE;
    } else {
        op_meta->shard_ltime = shard_meta->persistent.ltime;
        op_meta->guid = 0;

        /* These aren't filled in until at least cr_op_start */
        op_meta->seqno = 0;
        op_meta->last_consecutive_decided_seqno = 0;

        op_shard_meta->nreplica = shard_meta->persistent.num_replicas;
        op_shard_meta->nmeta = shard_meta->persistent.num_meta_replicas;

        for (i = 0; i < shard_meta->persistent.num_replicas; ++i) {
            op_shard_meta->pnodes[i] =
                shard_meta->replicas[i]->persistent.pnode;
        }

        /*
         * XXX: drew 2009-07-01 We don't need the meta nodes since we find the
         * right replicator_meta_storage service by its pilot beacon.
         */
#ifdef notyet
        for (i = 0; i < shard_meta->persistent.num_replicas; ++i) {
            op_shard_meta->meta_pnodes[i] =
                shard_meta->persistent.meta_pnodes[i];
        }
#else
        op_shard_meta->meta_pnode = shard_meta->persistent.meta_pnodes[0];
#endif
        op_shard_meta->type = shard_meta->persistent.type;
        op_shard_meta->current_home_node =
            shard_meta->persistent.current_home_node;
        op_shard_meta->meta_shardid = shard_meta->persistent.sguid_meta;

        ret = SDF_SUCCESS;
    }

    return (ret);
}

/**
 * @brief Get local node's preference for a given shard
 *
 * @param shard <IN> a #cr_shard object which must have properly
 * populated meta-data for a successful return.
 *
 * @param node <IN> node for which preference is being obtained;
 * callers should use shard->cr->config.my_node for this node.
 *
 * @param pref_type <IN> The purpose of this call, with
 * #CR_SHARD_GET_PREFERENCE_NORMAL for most purposes and
 * #CR_SHARD_GET_PREFERENCE_START_DELAY for the initial
 * lease acquisition delay.
 *
 * @param numerator_ptr <OUT> numerator/absolute rank; 0 when
 * this is the preferred node.  *numerator_ptr is untouched
 * on error.
 *
 * @param denominator_ptr <OUT> denominator.  May be NULL where
 * the caller is only testing whether this is the preferred node
 * for the given shard.  *denominator_ptr is untouched on error.
 *
 * @return SDF_SUCCESS on success, something else on failure
 */
static SDF_status_t
cr_shard_get_preference(struct cr_shard *shard,
                        vnode_t node,
                        enum cr_shard_get_preference_type pref_type,
                        int *numerator_ptr,
                        int *denominator_ptr) {
    SDF_status_t ret;
    int replication_type_flags;
    struct sdf_vip_group_group *inter_group_group;
    struct sdf_vip_group *intra_group;
    unsigned i;
    struct cr_shard_replica_meta *replica;

    plat_assert(numerator_ptr);

    if (!shard->shard_meta) {
        ret = SDF_FAILURE;
    } else {
        replication_type_flags =
            cr_replication_type_flags(shard->shard_meta->persistent.type);

        plat_assert_either(replication_type_flags & CRTF_INDEX_NONE,
                           replication_type_flags & CRTF_INDEX_SHARDID,
                           replication_type_flags & CRTF_INDEX_VIP_GROUP_ID);

        if (replication_type_flags & CRTF_INDEX_VIP_GROUP_ID) {
            plat_assert(shard->vip_group_id != VIP_GROUP_ID_INVALID);
            inter_group_group =
                sdf_vip_config_get_group_group_by_gid(shard->cr->config.vip_config,
                                                      shard->vip_group_id);
            plat_assert(inter_group_group);
            intra_group =
                sdf_vip_config_get_vip_group(shard->cr->config.vip_config,
                                             shard->vip_group_id);
            plat_assert(intra_group);

            /*
             * See #sdf_replicator_config.initial_preference
             * in replicator.h for an explanation.
             *
             * For V1 simple 2-way replication
             * 1.  On restart, nodes attempt to claim all VIPs
             * 2.  The back-off is based on node number.
             *
             * This makes situations where the two nodes start close
             * together likely (separate messages are used, so there
             * is a brief exposure) to result in both VIPs landing on
             * the same node after which Jake's code will recover
             * and cause migration.
             */
            if (pref_type ==
                CR_SHARD_GET_PREFERENCE_START_DELAY &&
                shard->after_restart &&
                shard->shard_meta->persistent.type ==
                SDF_REPLICATION_V1_2_WAY &&
                !(shard->cr->config.initial_preference)) {
                *numerator_ptr =
                    sdf_vip_group_get_node_rank(intra_group, node);
                plat_log_msg(21844, LOG_CAT_LEASE, PLAT_LOG_LEVEL_DEBUG,
                             "cr_shard %p node %u shard 0x%lx vip group %d"
                             " initial preference %d of %d",
                             shard, shard->cr->config.my_node,
                             shard->sguid, shard->vip_group_id,
                             *numerator_ptr, inter_group_group->num_groups);
            } else {
                *numerator_ptr =
                    sdf_vip_group_get_node_preference(intra_group, node);
                plat_log_msg(21845, LOG_CAT_LEASE, PLAT_LOG_LEVEL_DEBUG,
                             "cr_shard %p node %u shard 0x%lx vip group %d"
                             " normal preference %d of %d",
                             shard, shard->cr->config.my_node,
                             shard->sguid, shard->vip_group_id,
                             *numerator_ptr, inter_group_group->num_groups);
            }

            if (denominator_ptr) {
                *denominator_ptr = inter_group_group->num_groups;
            }
            ret = SDF_SUCCESS;

        } else if (replication_type_flags & CRTF_INDEX_SHARDID) {
            ret = SDF_FAILURE;
            for (i = 0; ret == SDF_FAILURE &&
                 i < shard->shard_meta->persistent.num_replicas; ++i) {
                replica = shard->shard_meta->replicas[i];
                if (replica && replica->persistent.pnode == node) {
                    *numerator_ptr = i;
                    ret = SDF_SUCCESS;
                }
            }

            if (ret == SDF_SUCCESS) {
                plat_log_msg(21846, LOG_CAT_LEASE, PLAT_LOG_LEVEL_DEBUG,
                             "cr_shard %p node %u shard 0x%lx non-vip group"
                             " preference %d of %d", shard,
                             shard->cr->config.my_node, shard->sguid,
                             *numerator_ptr,
                             shard->shard_meta->persistent.num_replicas);
            }

            if (ret == SDF_SUCCESS && denominator_ptr) {
                *denominator_ptr = shard->shard_meta->persistent.num_replicas;
            }
        } else {
            ret = SDF_FAILURE;
        }
    }

    return (ret);
}

/** @brief State for #cr_shard_command_recovered */
struct cr_shard_command_recovered_state {
    /** @brief Attached shard */
    struct cr_shard *shard;
    /** @brief User callback applied on completion */
    sdf_replicator_command_cb_t cb;
};

static void cr_shard_command_recovered_notify_cb(struct plat_closure_scheduler *context,
                                                 void *env);
static void cr_shard_command_recovered_put_meta_cb(struct plat_closure_scheduler *context,
                                                   void *env, SDF_status_t status_arg);
static void cr_shard_command_recovered_complete(struct cr_shard_command_recovered_state *state,
                                                SDF_status_t status,
                                                char *output);
/**
 * @brief Shard-level handling of v1 recovered logic
 *
 * Holds shard ref_count and op_count until complete.
 *
 * XXX: drew 2009-12-16 This currently works by creating a 1uS lease which
 * then times out.  State changes from CR_SHARD_STATE_RW to
 * CR_SHARD_STATE_SWITCH_BACK prior to the lease update,
 * CR_SHARD_STATE_SWITCH_BACK_2 after, and remains in
 * CR_SHARD_STATE_SWITCH_BACK_2 until the preferred node assumes ownership
 * of the shardof the or the preferred node enters a dead state.
 *
 * For v.next the lease would just be allowed to expire so that read-only
 * replicas are guaranteed to see every write.
 *
 * @param shard <IN> Shard must be #CR_SHARD_STATE_RW.
 * @param cb <IN> closure applied on completion.
 */
static void
cr_shard_command_recovered(struct cr_shard *shard,
                           sdf_replicator_command_cb_t cb) {
    struct cr_shard_command_recovered_state *state;
    cr_shard_notify_cb_t notify_cb;

    plat_assert(shard->state == CR_SHARD_STATE_RW);

    plat_log_msg(21380, LOG_CAT_RECOVERY, PLAT_LOG_LEVEL_DEBUG,
                 "cr_shard %p node %u shard 0x%lx vip group %d recovered",
                 shard, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id);

    if (!plat_calloc_struct(&state)) {
        plat_closure_apply(sdf_replicator_command_cb, &cb,
                           SDF_FAILURE_MEMORY_ALLOC, NULL);
    } else {
        state->shard = shard;
        state->cb = cb;

        __sync_add_and_fetch(&shard->ref_count, 1);
        __sync_add_and_fetch(&shard->op_count, 1);

        cr_shard_set_state(shard, CR_SHARD_STATE_SWITCH_BACK);

        /*
         * Notify first so that this node drops the VIPs before the other
         * node assumes them.
         */
        notify_cb =
            cr_shard_notify_cb_create(shard->cr->callbacks.single_scheduler,
                                      &cr_shard_command_recovered_notify_cb,
                                      state);
        cr_shard_notify(shard, notify_cb);
    }
}

/** @brief #cr_shard_command_recovered #cr_shard_notify complete */
static void
cr_shard_command_recovered_notify_cb(struct plat_closure_scheduler *context,
                                     void *env) {
    struct cr_shard_command_recovered_state *state =
        (struct cr_shard_command_recovered_state *)env;
    struct cr_shard *shard = state->shard;
    char *out;
    cr_shard_put_meta_cb_t put_meta_cb;

    put_meta_cb =
        cr_shard_put_meta_cb_create(shard->cr->callbacks.single_scheduler,
                                    &cr_shard_command_recovered_put_meta_cb,
                                    state);

    plat_log_msg(21381, LOG_CAT_RECOVERY, PLAT_LOG_LEVEL_TRACE,
                 "cr_shard %p node %u shard 0x%lx vip group %d notify complete",
                 shard, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id);

    if (shard->state != CR_SHARD_STATE_SWITCH_BACK) {
        out = NULL;
        plat_asprintf(&out,
                      "SERVER ERROR: after notify state %s"
                      " not CR_SHARD_STATE_SWITCH_BACK\r\n",
                      cr_shard_state_to_string(shard->state));
        cr_shard_command_recovered_complete(state, SDF_FAILURE, out);
    } else {
        plat_assert(shard->proposed_shard_meta);
        plat_assert(shard->proposed_shard_meta->persistent.current_home_node ==
                    shard->cr->config.my_node);

        /*
         * XXX: drew 2009-09-13 Switch to an explicit switch-back
         * indication so that the other node doesn't go through it's
         * dealy to see if some one else will be acquiring the lease.
         */
        shard->proposed_shard_meta->persistent.lease_liveness = 0;
        shard->proposed_shard_meta->persistent.lease_usecs = 1;
        ++shard->proposed_shard_meta->persistent.shard_meta_seqno;

        /*
         * XXX: drew 2009-09-13 With single copy semantics and reads on
         * replicas other than the coordinator we can't shorten our lease
         * without getting all other nodes to agree to it (because the
         * new co-ordinator might not think that they were live thus
         * causing them to miss writes and return data.
         *
         * A transfer_to_node would be most reasonable, allowing the node
         * transfered over to assume ownership of the lease with its existing
         * expiration time.
         */
        cr_shard_put_meta(shard, put_meta_cb);
    }
}

/** @brief #cr_shard_command_recovered #cr_shard_put_meta complete */
static void
cr_shard_command_recovered_put_meta_cb(struct plat_closure_scheduler *context,
                                       void *env, SDF_status_t status_arg) {
    struct cr_shard_command_recovered_state *state =
        (struct cr_shard_command_recovered_state *)env;
    struct cr_shard *shard = state->shard;
    char *out;
    SDF_status_t status;

    plat_log_msg(21382, LOG_CAT_RECOVERY,
                 status_arg == SDF_SUCCESS ? PLAT_LOG_LEVEL_TRACE :
                 PLAT_LOG_LEVEL_WARN,
                 "cr_shard %p node %u shard 0x%lx vip group %d"
                 " put_meta complete: %s",
                 shard, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id, sdf_status_to_string(status_arg));

    out = NULL;
    if (status_arg != SDF_SUCCESS) {
        plat_asprintf(&out, "SERVER ERROR: put meta failed: %s\r\n",
                      sdf_status_to_string(status_arg));
        status = status_arg;
    } else {
        out = plat_strdup("END\r\n");
        status = SDF_SUCCESS;
    }

    cr_shard_command_recovered_complete(state, status, out);
}

/**
 * @brief #cr_shard_command_recovered complete
 *
 * Consumes one ref_count and one op_count.
 *
 * @param state <IN> state which will be free on return
 * @param output <IN> plat_alloc alloced string with ownership transfer returned
 * to user's completion closure.
 */
static void
cr_shard_command_recovered_complete(struct cr_shard_command_recovered_state *state,
                                    SDF_status_t status,
                                    char *output) {
    struct cr_shard *shard = state->shard;

    plat_log_msg(21383, LOG_CAT_RECOVERY,
                 status == SDF_SUCCESS ? PLAT_LOG_LEVEL_DEBUG :
                 PLAT_LOG_LEVEL_WARN,
                 "cr_shard %p node %u shard 0x%lx vip group %d"
                 " recovered status %s output '%s'",
                 shard, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id, sdf_status_to_string(status),
                 output ? output : "");

    if (shard->state == CR_SHARD_STATE_SWITCH_BACK) {
        /* Force delay before reclamation of VIPs */
        shard->after_recovery = 1;

        if (status == SDF_SUCCESS) {
            cr_shard_set_state(shard, CR_SHARD_STATE_SWITCH_BACK_2);
        } else {
            cr_shard_set_state(shard, CR_SHARD_STATE_TO_WAIT_META);
        }
    }

    plat_closure_apply(sdf_replicator_command_cb, &state->cb, status, output);
    plat_free(state);

    cr_shard_op_count_dec(shard);
    cr_shard_ref_count_dec(shard);
}

static const char *
cr_shard_state_to_string(enum cr_shard_state state) {
    switch (state) {
#define item(caps, lower, flags, enter, leave) case caps: return (#lower);
    CR_SHARD_STATE_ITEMS(/* none */, /* none */, /* none */)
#undef item
    }

    plat_assert(0);

    return ("invalid");
}

static int
cr_shard_state_to_flags(enum cr_shard_state state) {
    switch (state) {
#define item(caps, lower, flags, enter, leave) case caps: return (flags);
    CR_SHARD_STATE_ITEMS(/* none */, /* none */, /* none */)
#undef item
    }

    plat_fatal("Unknown state");
    return (0);
}

/** @brief Assert when shard state is obviously incorrect */
static void
cr_shard_state_validate(enum cr_shard_state state) {
    int flags;

    flags = cr_shard_state_to_flags(state);
    plat_assert_either(flags & CRSSF_META_SOURCE_OTHER,
                       flags & CRSSF_META_SOURCE_SELF);

    plat_assert_either(flags & CRSSF_NO_LEASE,
                       flags & CRSSF_REQUEST_LEASE,
                       flags & CRSSF_RENEW_LEASE);

    plat_assert_either(flags & CRSSF_ACCESS_NONE,
                       (flags & CRSSF_ACCESS_READ)|
                       (flags & CRSSF_ACCESS_WRITE));
}

/** @brief Assert when shard states are obviously incorrect */
static void
cr_shard_states_validate() {
#define item(caps, lower, flags, enter, leave) cr_shard_state_validate(caps);
    CR_SHARD_STATE_ITEMS(/* none */, /* none */, /* none */)
#undef item
}

/**
 * @brief Signal that a liveness change has occurred
 *
 * @brief state <IN> New node liveness state
 */
static void
cr_replica_signal_liveness_change(struct cr_replica *replica,
                                  enum cr_node_state state) {
    cr_replica_check_state(replica);
}

/**
 * @brief Set current state for a replica
 *
 * As a side effect, we test whether we should remain in this state.
 */
static void
cr_replica_set_state(struct cr_replica *replica,
                     enum cr_replica_state next_state) {
    struct cr_shard *shard = replica->shard;
    enum cr_replica_state from = replica->state;

    if (replica->state != next_state) {
        plat_log_msg(21388, LOG_CAT_STATE, PLAT_LOG_LEVEL_DEBUG,
                     "cr_replica %p node %u shard 0x%lx vip group %d replica %d"
                     " local to node %u state from %s to %s",
                     replica, shard->cr->config.my_node,
                     shard->sguid, shard->vip_group_id,
                     replica->index, replica->node,
                     cr_replica_state_to_string(replica->state),
                     cr_replica_state_to_string(next_state));
    }

    switch (from) {
#define item(caps, lower, flags, enter, leave) case caps: leave; break;
    CR_REPLICA_STATE_ITEMS(replica, from, next_state)
#undef item
    }

    plat_assert(replica->state == from);

    replica->state = next_state;

    switch (next_state) {
#define item(caps, lower, flags, enter, leave) \
    case caps:                                                                 \
        replica->state_flags = (flags);                                        \
        enter;                                                                 \
        break;
    CR_REPLICA_STATE_ITEMS(replica, from, next_state)
#undef item
    }

    /* Maybe things have changed already */

    cr_replica_check_state(replica);
}

/** @brief Trigger state change on events */
static void
cr_replica_check_state(struct cr_replica *replica) {
    struct cr_shard *shard = replica->shard;
    struct copy_replicator *cr = shard->cr;
    struct cr_node *node = cr_get_node(cr, replica->node);

    plat_assert(node);

    if (replica->state != CR_REPLICA_STATE_MARK_FAILED &&
        replica->state != CR_REPLICA_STATE_TO_DEAD &&
        replica->state != CR_REPLICA_STATE_DEAD &&
        node->state != CR_NODE_LIVE) {
        /*
         * All replicas which were writeable when their node became
         * dead must be marked failed.
         */
        if (replica->state_flags &
            (CRSF_ALLOW_CLIENT_WO_BLOCK|CRSF_ALLOW_CLIENT_WO|
             CRSF_ALLOW_CLIENT_RW)) {
            cr_replica_set_state(replica, CR_REPLICA_STATE_MARK_FAILED);
        } else {
            cr_replica_set_state(replica, CR_REPLICA_STATE_TO_DEAD);
        }
    } else {
        switch (replica->state) {
        case CR_REPLICA_STATE_INITIAL:
        case CR_REPLICA_STATE_DEAD:
            node = cr_get_node(cr, replica->node);
            plat_assert(node);
            if (node->state == CR_NODE_LIVE) {
                cr_replica_set_state(replica, CR_REPLICA_STATE_LIVE_OFFLINE);
            }
            break;

        case CR_REPLICA_STATE_TO_DEAD:
            if (!replica->get_iteration_cursors_pending &&
                !replica->recovery_op_pending_count) {
                cr_replica_set_state(replica, CR_REPLICA_STATE_DEAD);
            }
            break;

        case CR_REPLICA_STATE_MUTUAL_REDO:
        case CR_REPLICA_STATE_UNDO:
        case CR_REPLICA_STATE_REDO:
            if (replica->get_iteration_cursors_eof &&
                !replica->get_iteration_cursors &&
                !replica->next_iteration_cursors &&
                !replica->recovery_op_pending_count) {
                cr_replica_set_state(replica, replica->next_state);
            }
            break;

        /*
         * Explicitly enumerate states for which we do nothing so that we
         * catch new states which get added without an appropriate entry.
         */
        case CR_REPLICA_STATE_LIVE_OFFLINE:
        case CR_REPLICA_STATE_MUTUAL_REDO_SCAN_DONE:
        case CR_REPLICA_STATE_UPDATE_AFTER_UNDO:
        case CR_REPLICA_STATE_UPDATE_AFTER_REDO:
        case CR_REPLICA_STATE_RECOVERED:
        case CR_REPLICA_STATE_MARK_FAILED:
        case CR_REPLICA_STATE_TO_SHUTDOWN:
        case CR_REPLICA_STATE_SHUTDOWN:
            break;

        }
    }

    /*
     * XXX: drew 2009-05-26 We could optimize the situations in which we
     * look for transitions?
     */
    cr_shard_signal_replica_state(shard, replica);
}

/** @brief Enter CR_REPLICA_STATE_MUTUAL_REDO */
static void
cr_replica_mutual_redo_enter(struct cr_replica *replica) {
    struct cr_shard *shard = replica->shard;
    int range_index;
    struct cr_shard_replica_meta *replica_meta;
    struct cr_persistent_shard_range *range;

    plat_assert(shard);
    plat_assert(shard->shard_meta);
    replica_meta = shard->shard_meta->replicas[replica->index];
    plat_assert(replica_meta->persistent.nrange > 0);
    range_index = replica_meta->persistent.nrange - 1;
    range = &replica_meta->ranges[replica_meta->persistent.nrange - 1];
    plat_assert(range->range_type == CR_REPLICA_RANGE_MUTUAL_REDO);

    ++shard->stat_counters->replica_recovery_started;
    ++shard->cr->total_stat_counters->replica_recovery_started;

    cr_replica_start_iteration(replica, replica->index, range_index,
                               CR_REPLICA_STATE_MUTUAL_REDO_SCAN_DONE);
}

/** @brief Enter CR_REPLICA_STATE_UNDO */
static void
cr_replica_undo_enter(struct cr_replica *replica) {
    struct cr_shard *shard = replica->shard;

    /*
     * XXX: drew 2009-06-28 Should have a separate start recovery state
     * which is divorced from the mechanics.
     */

    ++shard->stat_counters->stale_slave_recovery_started;
    ++shard->cr->total_stat_counters->stale_slave_recovery_started;

    ++shard->stat_counters->replica_recovery_started;
    ++shard->cr->total_stat_counters->replica_recovery_started;

    cr_replica_next_iteration(replica, replica->index /* iterate from */,
                              CR_REPLICA_RANGE_UNDO,
                              CR_REPLICA_STATE_UPDATE_AFTER_UNDO);
}

/** @brief State associated with a recovery  meta-data update */
struct cr_replica_recovery_update_meta {
    /** @brief Replica on behalf of which update was performed  */
    struct cr_replica *replica;

    /** @brief State at time of operation start */
    enum cr_replica_state start_state;

    /** @brief Next state on successful completion */
    enum cr_replica_state success_state;
};

/**
 * @brief Enter CR_REPLICA_STATE_UPDATE_AFTER_UNDO
 *
 * The current itertion's CR_REPLICA_RANGE_UNDO range is converted into
 * CR_REPLICA_RANGE_REDO. When no CR_REPLICA_RANGE_UNDO ranges remain
 * a redo range is created to the current sequence number, active range
 * from then on, the replica becomes writeable, and the in-core replica
 * state advances to CR_REPLICA_STATE_REDO.
 */
static void
cr_replica_update_after_undo_enter(struct cr_replica *replica) {
    struct cr_shard *shard = replica->shard;
    struct cr_shard_replica_meta *replica_meta;
    struct cr_persistent_shard_range *range;
    cr_shard_put_meta_cb_t put_cb;
    int range_index;
    int done;
    uint64_t seqno;
    uint64_t range_start;

    plat_assert(replica->state == CR_REPLICA_STATE_UPDATE_AFTER_UNDO);
    plat_assert(replica->get_iteration_cursors_eof);
    plat_assert(!replica->get_iteration_cursors);
    plat_assert(!replica->next_iteration_cursors);
    replica_meta = shard->proposed_shard_meta->replicas[replica->index];
    plat_assert(replica->current_range_index < replica_meta->persistent.nrange);
    range = &replica_meta->ranges[replica->current_range_index];
    plat_assert(range->range_type == CR_REPLICA_RANGE_UNDO);
    /*
     * XXX: drew 2009-05-27 this needs to be  atomic for when we change
     * the threading model for performance to a pool for operations while
     * retaining the serialized closure scheduler for recovery.
     */
    seqno = shard->seqno;

    range->range_type = CR_REPLICA_RANGE_REDO;

    for (done = 1, range_index = 0;
         done && range_index < replica_meta->persistent.nrange; ++range_index) {
        range = &replica_meta->ranges[range_index];

        plat_assert(range->range_type != CR_REPLICA_RANGE_MUTUAL_REDO);
        plat_assert(range->range_type != CR_REPLICA_RANGE_ACTIVE);
        plat_assert(range->len != CR_SHARD_RANGE_OPEN);
        plat_assert(range->start + range->len <= seqno);

        if (range->range_type == CR_REPLICA_RANGE_UNDO) {
            done = 0;
        }
    }

    if (done) {
        range_start = range->start + range->len;

        cr_shard_replica_meta_add_range(replica_meta,
                                        CR_REPLICA_RANGE_REDO,
                                        range_start,
                                        seqno - range_start);
        cr_shard_replica_meta_add_range(replica_meta,
                                        CR_REPLICA_RANGE_ACTIVE,
                                        seqno,
                                        CR_SHARD_RANGE_OPEN);
        replica->next_state = CR_REPLICA_STATE_REDO;
    } else {
        replica->next_state = CR_REPLICA_STATE_UNDO;
    }


    cr_shard_replica_meta_make_canonical(replica_meta);

    ++shard->proposed_shard_meta->persistent.shard_meta_seqno;

    put_cb = cr_shard_put_meta_cb_create(shard->cr->callbacks.single_scheduler,
                                         &cr_replica_recovery_update_cb, replica);

    ++replica->recovery_op_pending_count;
    ++replica->ref_count;

    cr_shard_put_meta(replica->shard, put_cb);
}

/**
 * @brief Enter CR_REPLICA_STATE_REDO
 *
 * Begin iteration from the local replica which is guaranteed to be
 * CR_REPLICA_STATE_AUTHORITATIVE.
 */
static void
cr_replica_redo_enter(struct cr_replica *replica) {
    cr_replica_next_iteration(replica,
                              replica->shard->local_replica /* iterate from */,
                              CR_REPLICA_RANGE_REDO,
                              CR_REPLICA_STATE_UPDATE_AFTER_REDO);
}

/**
 * @brief Enter CR_REPLICA_STATE_UPDATE_AFTER_UNDO
 *
 * The current itertion's CR_REPLICA_RANGE_REDO range is converted into
 * CR_REPLICA_RANGE_SYNCED. When no CR_REPLICA_RANGE_REDO ranges remain
 * persistent replica state is set to  CR_REPLICA_STATE_AUTHORITATIVE and the
 * incore state becomes CR_REPLICA_STATE_RECOVERED.
 */
static void
cr_replica_update_after_redo_enter(struct cr_replica *replica) {
    struct cr_shard *shard = replica->shard;
    struct cr_shard_replica_meta *replica_meta;
    struct cr_persistent_shard_range *range;
    cr_shard_put_meta_cb_t put_cb;
    int range_index;
    int done;

    plat_assert(replica->state == CR_REPLICA_STATE_UPDATE_AFTER_REDO);
    plat_assert(replica->get_iteration_cursors_eof);
    plat_assert(!replica->get_iteration_cursors);
    plat_assert(!replica->next_iteration_cursors);
    replica_meta = shard->proposed_shard_meta->replicas[replica->index];
    plat_assert(replica->current_range_index < replica_meta->persistent.nrange);
    range = &replica_meta->ranges[replica->current_range_index];
    plat_assert(range->range_type == CR_REPLICA_RANGE_REDO);

    range->range_type = CR_REPLICA_RANGE_SYNCED;

    for (done = 1, range_index = 0; done &&
         range_index < replica_meta->persistent.nrange; ++range_index) {
        range = &replica_meta->ranges[range_index];

        plat_assert(range->range_type != CR_REPLICA_RANGE_MUTUAL_REDO);
        plat_assert(range->range_type != CR_REPLICA_RANGE_ACTIVE ||
                    range_index == replica_meta->persistent.nrange - 1);

        plat_assert(range->len != CR_SHARD_RANGE_OPEN ||
                    range->range_type == CR_REPLICA_RANGE_ACTIVE);

        if (range->range_type == CR_REPLICA_RANGE_REDO) {
            done = 0;
        }
    }

    if (done) {
        replica_meta->persistent.state = CR_REPLICA_STATE_AUTHORITATIVE;
        replica->next_state = CR_REPLICA_STATE_RECOVERED;
    } else {
        replica->next_state = CR_REPLICA_STATE_REDO;
    }


    cr_shard_replica_meta_make_canonical(replica_meta);

    ++shard->proposed_shard_meta->persistent.shard_meta_seqno;

    put_cb = cr_shard_put_meta_cb_create(shard->cr->callbacks.single_scheduler,
                                         &cr_replica_recovery_update_cb, replica);

    ++replica->recovery_op_pending_count;
    ++replica->ref_count;

    cr_shard_put_meta(replica->shard, put_cb);
}

/** @brief Called after meta-data is stored following undo */
static void
cr_replica_recovery_update_cb(struct plat_closure_scheduler *context,
                              void *env, SDF_status_t status) {
    struct cr_replica *replica = (struct cr_replica *)env;

    --replica->recovery_op_pending_count;

    /*
     * XXX: drew 2009-06-17 Only correct if we haven't already transitioned
     * to a dead state.  Should switch to events.
     */
    if (status == SDF_SUCCESS) {
        cr_replica_set_state(replica, replica->next_state);
    } else {
        cr_replica_set_state(replica, CR_REPLICA_STATE_TO_DEAD);
    }

    cr_replica_ref_count_dec(replica);
}

/** @brief Enter CR_REPLICA_STATE_RECOVERED */
static void
cr_replica_recovered_enter(struct cr_replica *replica) {
    struct cr_shard *shard = replica->shard;

    ++shard->stat_counters->replica_recovery_complete;
    ++shard->cr->total_stat_counters->replica_recovery_complete;

    plat_log_msg(21389, LOG_CAT_RECOVERY, PLAT_LOG_LEVEL_INFO,
                 "node %u shard 0x%lx vip group %d replica %d local to node %u"
                 " set to RECOVERED state", shard->cr->config.my_node,
                 shard->sguid, shard->vip_group_id, replica->index,
                 replica->node);
}

/**
 * @brief Enter CR_REPLICA_STATE_MARK_FAILED
 *
 * Mark the replica as CR_REPLICA_STALE and convert the CR_REPLICA_RANGE_ACTIVE
 * to CR_REPLICA_RANGE_SYNCED up to a point before where the replica
 * diverges.
 */
static void
cr_replica_mark_failed_enter(struct cr_replica *replica) {
    struct cr_shard *shard = replica->shard;
    struct cr_shard_replica_meta *replica_meta;
    struct cr_persistent_shard_range *range;
    uint64_t seqno;
    uint64_t window;
    cr_shard_put_meta_cb_t put_cb;

    plat_assert(shard->proposed_shard_meta);
    replica_meta = shard->proposed_shard_meta->replicas[replica->index];
    plat_assert(replica_meta->persistent.nrange > 0);
    range = &replica_meta->ranges[replica_meta->persistent.nrange - 1];
    plat_assert(range->range_type == CR_REPLICA_RANGE_ACTIVE);

    /*
     * FIXME: drew 2009-05-28 We need to track the last consecutive
     * synced sequence number for the update to be correct so we just
     * assert where that's not possible.  As a stop gap it should be
     * easy enough to take the shard off-line and iniate recovery.
     *
     * This is completely wrong but quicker than initiating recovery
     * with the shard missing.
     *
     * Currently, the recovery code expects to have at least one undo
     * range which becomes a redo range.  Accomodate that as a kludge.
     */
    seqno = shard->seqno;

#ifdef notyet
    range->len = shard->seqno - range->start;
    range->range_type = CR_REPLICA_RANGE_SYNCED;
#else
    window = shard->proposed_shard_meta->persistent.outstanding_window;

    /*
     * Convert the CR_REPLICA_RANGE_ACTIVE range into a SYNCED range up to
     * window before the current sequence number.
     *
     * FIXME: drew 2009-06-16 seqno is replaced with the last consecutive live
     * synced sequence number.
     */
    if (range->start +
        shard->proposed_shard_meta->persistent.outstanding_window < seqno) {
        range->len = seqno - range->start - window;
        range->range_type = CR_REPLICA_RANGE_SYNCED;

        cr_shard_replica_meta_add_range(replica_meta,
                                        CR_REPLICA_RANGE_UNDO,
                                        range->start + range->len,
                                        seqno - (range->len - window));
    /*
     * XXX: drew 2009-06-16 Where the node crashed before any new data was
     * written this will be a zero length range which is non-sensical.
     *
     * Change the recovery process to accomodate situations where there
     * are no UNDO ranges.
     *
     * In the case where the shard was freshly created and no data exists,
     * we'll end up with zero ranges.  This makes the most sense for DEAD
     * replicas so we'll do that to.
     */
    } else {
        plat_assert(seqno >= range->start);
        range->range_type = CR_REPLICA_RANGE_UNDO;
        range->len = seqno - range->start;
    }
#endif

    cr_shard_replica_meta_make_canonical(replica_meta);
    replica_meta->persistent.state = CR_REPLICA_STALE;

    ++shard->proposed_shard_meta->persistent.shard_meta_seqno;

    put_cb = cr_shard_put_meta_cb_create(shard->cr->callbacks.single_scheduler,
                                         &cr_replica_mark_failed_cb, replica);

    ++replica->recovery_op_pending_count;
    ++replica->ref_count;

    cr_shard_put_meta(replica->shard, put_cb);
}

/**
 * @brief Callback on #cr_shard_put_meta for CR_REPLICA_STATE_MARK_FAILED
 *
 * Transition out to CR_REPLICA_STATE_TO_DEAD
 */
static void
cr_replica_mark_failed_cb(struct plat_closure_scheduler *context,
                          void *env, SDF_status_t status) {
    struct cr_replica *replica = (struct cr_replica *)env;
    struct cr_shard *shard = replica->shard;

    /* These states aren't possible with operations pending */
    plat_assert(replica->state != CR_REPLICA_STATE_SHUTDOWN);
    plat_assert(replica->state != CR_REPLICA_STATE_DEAD);

    /* shard level code will set shard state on failure */
    if (status != SDF_SUCCESS &&
        replica->state == CR_REPLICA_STATE_MARK_FAILED &&
        status != SDF_NODE_DEAD && status != SDF_TIMEOUT &&
        status != SDF_TEST_CRASH) {
        plat_log_msg(21390, LOG_CAT_RECOVERY,
                     PLAT_LOG_LEVEL_WARN,
                     "cr_replica %p node %u shard 0x%lx vip group %d"
                     " replica %d local to node %u mark_failed failed: %s",
                     replica, shard->cr->config.my_node,
                     shard->sguid, shard->vip_group_id, replica->index,
                     replica->node, sdf_status_to_string(status));
    }

    /* Let shard level code do what it does */

    --replica->recovery_op_pending_count;
    if (replica->state == CR_REPLICA_STATE_MARK_FAILED) {
        cr_replica_set_state(replica, CR_REPLICA_STATE_TO_DEAD);
    } else {
        cr_replica_check_state(replica);
    }

    cr_replica_ref_count_dec(replica);
}

/**
 * @brief Perform next iteration in current phase
 *
 * @param replica <IN> Replica being recovered
 * @param iterate_replica <IN> Replica which is being iterated.  Refers to
 * replica for #CR_REPLICA_STATE_UNDO, local authoritative replica for
 * #CR_REPLICA_STATE_REDO.
 * @param range_type <IN> Type of range we're looking to fix, either
 * CR_REPLICA_RANGE_UNDO or CR_REPLICA_RANGE_REDO
 * @param after_iterate <IN> Staet to transition to after successful
 * iteration process.
 */
static void
cr_replica_next_iteration(struct cr_replica *replica,
                          int iterate_replica,
                          enum cr_replica_range_type range_type,
                          enum cr_replica_state after_iterate) {
    struct cr_shard *shard = replica->shard;
    int range_index;
    struct cr_shard_replica_meta *replica_meta;
    struct cr_persistent_shard_range *range;

    plat_assert(shard);
    plat_assert(shard->shard_meta);
    replica_meta = shard->shard_meta->replicas[replica->index];
    plat_assert(replica_meta->persistent.nrange > 0);

    range = NULL;
    range_index = -1;
    do {
        ++range_index;
        range = &replica_meta->ranges[range_index];
    } while (range_index < replica_meta->persistent.nrange &&
             range->range_type != range_type);

    if (range_index < replica_meta->persistent.nrange) {
        cr_replica_start_iteration(replica, iterate_replica /* from */,
                                   range_index, after_iterate);
    } else {
        /*
         * Until we have clean shutdown there should be no way to get
         * here since we have at least one undo range which then becomes
         * at least one redo range.
         */
        plat_assert(0);
    }
}

/**
 * @brief Start iteration found by #cr_replica_next_iteration
 *
 * @param replica <IN> Replica being recovered
 * @param iterate_replica <IN> Replica which is being iterated.  Refers to
 * replica for #CR_REPLICA_STATE_UNDO, local authoritative replica for
 * #CR_REPLICA_STATE_REDO.
 * @param range_index <IN> Range to iterate on
 * @param after_iterate <IN> Staet to transition to after successful
 * iteration process.
 */
static void
cr_replica_start_iteration(struct cr_replica *replica,
                           int iterate_replica, int range_index,
                           enum cr_replica_state after_iterate) {
    struct cr_shard *shard = replica->shard;
    struct cr_shard_replica_meta *replica_meta;
    struct cr_persistent_shard_range *range;

    plat_assert(shard);
    plat_assert(shard->shard_meta);
    replica_meta = shard->shard_meta->replicas[replica->index];
    plat_assert(range_index < replica_meta->persistent.nrange);
    range = &replica_meta->ranges[range_index];

    replica->current_range_index = range_index;
    replica->iterate_replica = iterate_replica;
    replica->iterate_node = shard->replicas[iterate_replica]->node;
    replica->current_range = *range;
    replica->current_range_index = range_index;
    plat_assert(!replica->get_iteration_cursors);
    replica->cursor_offset = 0;
    plat_assert(!replica->next_iteration_cursors);
    replica->get_iteration_cursors_resume_data = NULL;
    replica->get_iteration_cursors_resume_data_len = 0;
    replica->get_iteration_cursors_pending = 0;
    replica->get_iteration_cursors_eof = 0;
    replica->next_state = after_iterate;

    cr_replica_do_recovery(replica);
}

/**
 * @brief Do recovery
 *
 * Essentially this is a coroutine that runs an iteration from the appropriate
 * source for the phase and operation sequences to perform the data
 * modification.
 *
 * In all cases, the operation sequence begins with a get by cursor.
 * Redo operations are a simple put (or delete) with the f_write_if_newer
 * flag set.  Undo operations follow the get with a get in the local
 * authoritative replica which then leads to a compensating action when
 * the sequence numbers conflict.
 *
 * @param replica <IN> replica which is being operated on.  For
 * CR_REPLICA_STATE_MUTUAL_REDO it's the source for operations to
 * other replicas.  Otherwise it's the destination.
 */

/*
 * FIXME: drew 2009-06-18 for efficient tombstones (target syndrome and
 * sequence number for superseded objects in store shards, target syndrome
 * for cache shards) which are only stored in the log we need to
 * 1. Iterate the persistent structures first
 *
 * 2. Issue the log tombstone requests in order, meaning that on get we putu
 *    the operation into a queue and don't consider the get complete until
 *    all previously issued gets complete.
 *
 * 3. Lock on tombstone or sequence number as appropriate so that we don't
 *    have multiple requests in flight at a given point in time.
 */
static void
cr_replica_do_recovery(struct cr_replica *replica) {
    struct cr_shard *shard = replica->shard;
    struct copy_replicator *cr = shard->cr;
    char *cursor;
    int cursor_len;
    int done;
    void *resume_data;
    int resume_data_len;

    rr_get_iteration_cursors_cb_t get_cursors_cb =
        rr_get_iteration_cursors_cb_create(cr->callbacks.single_scheduler,
                                           &cr_replica_get_iteration_cursors_cb,
                                           replica);

    rr_get_msg_by_cursor_cb_t get_by_cursor_cb =
        rr_get_msg_by_cursor_cb_create(cr->callbacks.single_scheduler,
                                       &cr_replica_get_by_cursor_cb, replica);

    /*
     * FIXME: drew 2009-05-25 The initial deliverable skips over the undo
     * phase since time is constrained and it is not needed for the
     * first incremental deliverable.
     */
    if (replica->state == CR_REPLICA_STATE_UNDO) {
        replica->get_iteration_cursors_eof = 1;
    }

    /*
     * XXX: drew 2009-05-25 Jakes code seems to go into an infinite loop when
     * it gets a zero length range.
     */
    if (replica->current_range.len == 0) {
        replica->get_iteration_cursors_eof = 1;
    }

    /* Stop recovery when state has changed to CR_REPLICA_STATE_TO_DEAD, etc. */
    done = !(replica->state_flags & CRSF_ITERATING);

    /* Loop until unable to initiate any additional asynchronous ops */
    while (!done) {
        done = 1;

        if (replica->get_iteration_cursors && replica->cursor_offset ==
            replica->get_iteration_cursors->cursor_count) {
            rr_get_iteration_cursors_free(replica->get_iteration_cursors);
            replica->get_iteration_cursors = NULL;
        }

        /*
         * FIXME: drew 2009-06-30 With the space efficient tombstone
         * implementation we'll want to enqueue all get-by-cursor requests
         * and process their results in order so that we can replay requests
         * for a single key in order and don't have to keep tombstones
         * indexed by key in the flash subsystem to make write-if-newer work.
         */
        if (replica->recovery_op_pending_count <
            cr->config.recovery_ops && replica->get_iteration_cursors) {

            plat_log_msg(21391, LOG_CAT_RECOVERY,
                         PLAT_LOG_LEVEL_TRACE,
                         "c_replicar %p node %u shard 0x%lx vip group %d"
                         " replica %d local to node %u get by cursor"
                         " from node %u",
                         replica, shard->cr->config.my_node,
                         shard->sguid, shard->vip_group_id, replica->index,
                         replica->node, replica->iterate_node);

            plat_assert(replica->cursor_offset <
                        replica->get_iteration_cursors->cursor_count);

            ++shard->stat_counters->recovery_get_by_cursor_started;
            ++shard->cr->total_stat_counters->recovery_get_by_cursor_started;

            cursor_len = replica->get_iteration_cursors->cursor_len;
            cursor = replica->get_iteration_cursors->cursors +
                replica->cursor_offset * cursor_len;

            ++replica->cursor_offset;
            ++replica->recovery_op_pending_count;
            ++replica->ref_count;

            rr_get_msg_by_cursor_async(cr->callbacks.send_msg,
                                       shard->cr->config.my_node,
                                       replica->iterate_node, shard->sguid,
                                       cursor, cursor_len, get_by_cursor_cb);
            done = 0;
        }

        if (!replica->get_iteration_cursors &&
            replica->next_iteration_cursors) {
            replica->get_iteration_cursors = replica->next_iteration_cursors;
            replica->next_iteration_cursors = NULL;

            replica->get_iteration_cursors_resume_data_len =
                sizeof (replica->get_iteration_cursors->resume_cursor);
            replica->get_iteration_cursors_resume_data =
                malloc(replica->get_iteration_cursors_resume_data_len);
            plat_assert_always(replica->get_iteration_cursors_resume_data);
            memcpy(replica->get_iteration_cursors_resume_data,
                   &replica->get_iteration_cursors->resume_cursor,
                   replica->get_iteration_cursors_resume_data_len);
            done = 0;
        }

        if (!replica->get_iteration_cursors_pending &&
            !replica->get_iteration_cursors_eof &&
            !replica->next_iteration_cursors) {

            plat_log_msg(21392, LOG_CAT_RECOVERY,
                         PLAT_LOG_LEVEL_TRACE,
                         "cr_replica %p node %u shard 0x%lx vip group %d"
                         " replica %d local to node %u get iteration cursors"
                         " from node %u",
                         replica, shard->cr->config.my_node, shard->sguid,
                         shard->vip_group_id, replica->index, replica->node,
                         replica->iterate_node);

            ++shard->stat_counters->recovery_get_cursors_started;
            ++shard->cr->total_stat_counters->recovery_get_cursors_started;

            ++replica->get_iteration_cursors_pending;
            ++replica->ref_count;

            resume_data = replica->get_iteration_cursors_resume_data;
            resume_data_len = replica->get_iteration_cursors_resume_data_len;

            if (replica->get_iteration_cursors_resume_data) {
                plat_free(replica->get_iteration_cursors_resume_data);
                replica->get_iteration_cursors_resume_data = NULL;
            }
            replica->get_iteration_cursors_resume_data_len = 0;

            rr_get_iteration_cursors_async(cr->callbacks.send_msg,
                                           shard->cr->config.my_node,
                                           replica->iterate_node, shard->sguid,
                                           replica->current_range.start,
                                           cr->config.recovery_ops,
                                           replica->current_range.start +
                                           replica->current_range.len,
                                           resume_data, resume_data_len,
                                           get_cursors_cb);
            done = 0;
        }
    }

    cr_replica_check_state(replica);
}

/**
 * @brief Callback invoked on #rr_get_iteration_cursors_async completion
 *
 * This just stashes the results and wakes up the
 * #cr_replica_do_recovery coroutine.
 *
 * @param env <IN> replica which is being operated on.  For
 * CR_REPLICA_STATE_MUTUAL_REDO it's the source for operations to
 * other replicas.  Otherwise it's the destination.
 */
static void
cr_replica_get_iteration_cursors_cb(struct plat_closure_scheduler *context,
                                    void *env, SDF_status_t status,
                                    struct flashGetIterationOutput *output) {
    struct cr_replica *replica = (struct cr_replica *)env;
    struct cr_shard *shard = replica->shard;

    plat_assert(replica->get_iteration_cursors_pending);
    plat_assert(!replica->next_iteration_cursors);

    ++shard->stat_counters->recovery_get_cursors_complete;
    ++shard->cr->total_stat_counters->recovery_get_cursors_complete;

    --replica->get_iteration_cursors_pending;

    if (replica->state_flags & CRSF_ITERATING) {
        if (status == SDF_SUCCESS) {
            if (output && !output->cursor_count) {
                rr_get_iteration_cursors_free(output);
                output = NULL;
            }
            replica->next_iteration_cursors = output;
            if (!output) {
                plat_log_msg(21393, LOG_CAT_RECOVERY,
                             PLAT_LOG_LEVEL_DEBUG,
                             "cr_replica %p node %u shard 0x%lx vip group %d"
                             " replica %d local to node %u eof",
                             replica, shard->cr->config.my_node,
                             shard->sguid, shard->vip_group_id,
                             replica->index, replica->node);
                replica->get_iteration_cursors_eof = 1;
            } else {
                plat_log_msg(21394, LOG_CAT_RECOVERY,
                             PLAT_LOG_LEVEL_DEBUG,
                             "cr_replica %p node %u shard 0x%lx vip group %d"
                             " replica %d local to node %u %d cursors",
                             replica, shard->cr->config.my_node,
                             shard->sguid, shard->vip_group_id, replica->index,
                             replica->node, output->cursor_count);
            }
        } else {
            ++shard->stat_counters->recovery_get_cursors_failed;
            ++shard->cr->total_stat_counters->recovery_get_cursors_failed;

            plat_log_msg(21395, LOG_CAT_RECOVERY,
                         PLAT_LOG_LEVEL_WARN,
                         "cr_replica %p node %u shard 0x%lx vip group %d"
                         " replica %d local to node %u status %s",
                         replica, shard->cr->config.my_node,
                         shard->sguid, shard->vip_group_id, replica->index,
                         replica->node, sdf_status_to_string(status));

            cr_replica_set_state(replica, CR_REPLICA_STATE_TO_DEAD);
            /*
             * XXX: drew 2009-05-25 For the normal redo mode of operation,
             * the get was performed in the local aurhoritative replica.  We
             * need to initiate recovery.
             */
        }
    } else if (output) {
        rr_get_iteration_cursors_free(output);
    }

    cr_replica_do_recovery(replica);
    cr_replica_ref_count_dec(replica);
}

/**
 * @brief Callback invoked on #rr_get_msg_by_cursor_async completion
 *
 * For CR_REPLICA_STATE_MUTUAL_REDO and CR_REPLICA_STATE_REDO this
 * generates a put or delete into the target replica.  It consumes
 * a reference count and recovery_op_pending count in the replica
 * referred to by env.
 *
 * XXX: drew 2009-05-25 We should consume a reference in both the recovery
 * replica and the replica in which the get was being performed so that
 * the other replica remains in an appropriate state.
 *
 * @param env <IN> replica which is being operated on.  For
 * CR_REPLICA_STATE_MUTUAL_REDO it's the source for operations to
 * other replicas.  Otherwise it's the destination.
 */
static void
cr_replica_get_by_cursor_cb(struct plat_closure_scheduler *context,
                            void *env, struct sdf_msg_wrapper *wrapper) {
    struct cr_replica *replica = (struct cr_replica *)env;
    struct cr_shard *shard = replica->shard;
    struct sdf_msg *msg;
    SDF_protocol_msg_t *pm;
    struct sdf_msg_error_payload *error_payload;
    SDF_status_t status;

    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, wrapper);
    pm = (SDF_protocol_msg_t *)msg->msg_payload;

    ++shard->stat_counters->recovery_get_by_cursor_complete;
    ++shard->cr->total_stat_counters->recovery_get_by_cursor_complete;

    if (msg->msg_type == SDF_MSG_ERROR) {
        error_payload = (struct sdf_msg_error_payload *)msg->msg_payload;
        plat_assert(msg->msg_len > sizeof (*msg) + sizeof (*error_payload));
        status = error_payload->error;
    } else {
        status = pm->status;
    }

    if (replica->state == CR_REPLICA_STATE_TO_DEAD ||
        replica->state == CR_REPLICA_STATE_TO_SHUTDOWN) {
            --replica->recovery_op_pending_count;
            cr_replica_check_state(replica);
            cr_replica_ref_count_dec(replica);
    } else if (!(replica->state_flags & CRSF_ITERATING)) {
        plat_log_msg(21396, LOG_CAT_RECOVERY,
                     PLAT_LOG_LEVEL_FATAL,
                     "cr_replica %p node %u shard 0x%lx vip group %d replica %d"
                     " local to node %u get by cursor returned in"
                     " non-iterating, non-shutdown state %s",
                     replica, shard->cr->config.my_node,
                     shard->sguid, shard->vip_group_id, replica->index,
                     replica->node, cr_replica_state_to_string(shard->state));
        plat_fatal("");
    } else if (status != SDF_SUCCESS &&
               !(status == SDF_OBJECT_UNKNOWN && (pm->flags & f_tombstone))) {
        plat_log_msg(21397, LOG_CAT_RECOVERY,
                     (status != SDF_SUCCESS &&
                      status != SDF_FLASH_STALE_CURSOR &&
                      status != SDF_OBJECT_UNKNOWN &&
                      shard->state != CR_REPLICA_STATE_TO_DEAD) ?
                     PLAT_LOG_LEVEL_WARN : PLAT_LOG_LEVEL_TRACE,
                     "cr_replica %p node %u shard 0x%lx vip group %d replica %d"
                     " local to node %u shard state %s replica state %s"
                     " get by cursor failed status %s",
                     replica, shard->cr->config.my_node,
                     shard->sguid, shard->vip_group_id, replica->index,
                     replica->node, cr_shard_state_to_string(shard->state),
                     cr_replica_state_to_string(shard->state),
                     sdf_status_to_string(status));
        sdf_msg_wrapper_rwrelease(&msg, wrapper);
        sdf_msg_wrapper_ref_count_dec(wrapper);

        --replica->recovery_op_pending_count;
        if (status == SDF_FLASH_STALE_CURSOR || status == SDF_OBJECT_UNKNOWN) {
            cr_replica_do_recovery(replica);
        } else {
            ++shard->stat_counters->recovery_get_by_cursor_failed;
            ++shard->cr->total_stat_counters->recovery_get_by_cursor_failed;
            cr_replica_set_state(replica, CR_REPLICA_STATE_TO_DEAD);
        }
        cr_replica_ref_count_dec(replica);
    } else {
        plat_log_msg(21398, LOG_CAT_RECOVERY,
                     PLAT_LOG_LEVEL_TRACE,
                     "cr_replica %p node %u shard 0x%lx vip group %d replica %d"
                     " local to node %u"
                     " get by cursor returned seqno %llu key %*.*s"
                     " %sdata len %u status %s",
                     replica, shard->cr->config.my_node,
                     shard->sguid, shard->vip_group_id, replica->index,
                     replica->node, (long long)pm->seqno,
                     pm->key.len, pm->key.len, pm->key.key,
                     pm->flags & f_tombstone ?  "tombstone " : "",
                     (unsigned)pm->data_size,
                     sdf_status_to_string(status));
        sdf_msg_wrapper_rwrelease(&msg, wrapper);

        switch (replica->state) {
        case CR_REPLICA_STATE_MUTUAL_REDO:
        case CR_REPLICA_STATE_REDO:
            cr_replica_redo(replica, wrapper);
            break;
        case CR_REPLICA_STATE_UNDO:
            cr_replica_undo_op(replica, wrapper);
            break;
        /*
         * It's impossible to get to any other state with pending recovery
         * operations.
         */
        default:
            plat_log_msg(21399, LOG_CAT_RECOVERY,
                         PLAT_LOG_LEVEL_FATAL,
                         "cr_replica %p node %u shard 0x%lx vip group %d"
                         " replica %d local to node %u get by cursor returned"
                         " in unhandled state %s",
                         replica, shard->cr->config.my_node,
                         shard->sguid, shard->vip_group_id, replica->index,
                         replica->node,
                         cr_replica_state_to_string(shard->state));
            plat_fatal("");
        }
    }
}

/** @brief Op types for debugging */
enum cr_recovery_op_type {
    CRO_TYPE_REDO,
    CRO_TYPE_UNDO
};

/** @brief All state associated with a redo/undo op */
struct cr_recovery_op {
    /** @brief Op type for debugging */
    enum cr_recovery_op_type op_type;

    /** @brief replica which is being copied from */
    struct cr_replica *src_replica;

    /** @brief replica being copied to */
    struct cr_replica *dest_replica;

    /** @brief Source get by cursor response, perhaps modified for redo */
    struct {
        /** @brief data from source request */
        struct sdf_msg_wrapper *wrapper;

        /** @brief sdf_msg header of data from initial iteration */
        struct sdf_msg *msg;

        /** @brief protocol msg of data.wrapper from initial iteration */
        SDF_protocol_msg_t *pm;

        /** @brief pmi of data.wrapper */
        SDF_Protocol_Msg_Info_t *pmi;
    } data;

    /** @brief Authoritative get response, perhaps modified for undo */
    struct {
        struct sdf_msg_wrapper *wrapper;

        /** @brief sdf_msg header of undo.wrapper from authoritative get */
        struct sdf_msg *msg;

        /** @brief protocol msg of undo.wrapper from authoritative get */
        SDF_protocol_msg_t *pm;

        /** @brief pmi of undo.wrapper from authoritative get */
        SDF_Protocol_Msg_Info_t *pmi;

        /** @brief handle on lock */
        struct replicator_key_lock *lock;
    } undo;

    /** @brief Type of message being sent */
    SDF_protocol_msg_type_t put_msgtype;

    /** @brief response goes here */
    struct sdf_fth_mbx *mbx;

    /** @brief Entry in src_replica->src_recovery_op_list */
    TAILQ_ENTRY(cr_recovery_op) src_recovery_op_list_entry;

    /** @brief Entry in dest_replica->dest_recovery_op_list */
    TAILQ_ENTRY(cr_recovery_op) dest_recovery_op_list_entry;
};

/**
 * @brief Perform put part of redo sequence
 *
 * Preconditions: a reference count and op_pending_count is held in
 * replica on behalf of the current operation.
 *
 * For CR_REPLICA_STATE_MUTUAL_REDO this is a fan out operation to all
 * replicas in the same state from replica, and for CR_REPLICA_STATE_REDO
 * it's a single operation to replica.
 *
 * @param replica <IN> replica which is being operated on.  For
 * CR_REPLICA_STATE_MUTUAL_REDO it's the source for operations to
 * other replicas.  Otherwise it's the destination.
 *
 * @param wrapper <IN> response from a successful get operation.  Unsuccessful
 * operations are processed elsewhere.  One reference count is consumed.
 */
static void
cr_replica_redo(struct cr_replica *replica, struct sdf_msg_wrapper *wrapper) {
    struct cr_shard *shard = replica->shard;
    struct sdf_msg *msg;
    SDF_protocol_msg_t *pm;
    struct cr_replica *other_replica;
    int i;

    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, wrapper);
    pm = (SDF_protocol_msg_t *)msg->msg_payload;
    msg->msg_flags &=
        ~(SDF_MSG_FLAG_MBX_RESP_INCLUDED|SDF_MSG_FLAG_MBX_RESP_EXPECTED);

    plat_assert(replica->state == CR_REPLICA_STATE_MUTUAL_REDO ||
                replica->state == CR_REPLICA_STATE_REDO);

    plat_assert(shard->shard_meta);

    if (pm->flags & f_tombstone) {
        pm->msgtype = HFDFF;
    } else {
        pm->msgtype = HFSET;
    }

    pm->op_meta.shard_ltime = shard->shard_meta->persistent.ltime;
    pm->flags &= ~(f_tombstone);
    pm->flags |= f_write_if_newer;

    switch (replica->state) {
    case CR_REPLICA_STATE_MUTUAL_REDO:
        for (i = 0; i < shard->shard_meta->persistent.num_replicas; ++i) {
            other_replica = shard->replicas[i];
            if (other_replica && other_replica != replica &&
                (other_replica->state == CR_REPLICA_STATE_MUTUAL_REDO ||
                 other_replica->state ==
                 CR_REPLICA_STATE_MUTUAL_REDO_SCAN_DONE)) {
                cr_replica_redo_op(other_replica, replica, wrapper);
            }
        }
        break;

    case CR_REPLICA_STATE_REDO:
        other_replica = shard->replicas[shard->local_replica];
        /* Copy other replica to iterating replica */
        cr_replica_redo_op(replica, other_replica, wrapper);
        break;

    default:
        plat_assert(0);
    }

    sdf_msg_wrapper_rwrelease(&msg, wrapper);
    sdf_msg_wrapper_ref_count_dec(wrapper);

    --replica->recovery_op_pending_count;
    cr_replica_do_recovery(replica);
    cr_replica_ref_count_dec(replica);
}

/**
 * @brief Start a single redo modification (put or delete)
 *
 * A recovery_op_pending_count and reference count are held for both
 * dest_replica and src_replic
 *
 * @param dest_replica <IN> replica which is modified
 * @param src_replica <IN> source of operation
 * @param request <IN> request which will be made to the other
 * replicas.  No reference counts are consumed although a reference
 * count is held for the duration of the operation.  Multiple
 * calls to #cr_replica_redo_op with the same wrapper are made in
 * parallel for the CR_REPLICA_STATE_MUTUAL_REDO.
 */
static void
cr_replica_redo_op(struct cr_replica *dest_replica,
                   struct cr_replica *src_replica,
                   struct sdf_msg_wrapper *request) {
    struct cr_shard *shard = dest_replica->shard;
    struct copy_replicator *cr = shard->cr;
    int failed;
    struct cr_recovery_op *op;
    sdf_msg_recv_wrapper_t response_cb;
    struct sdf_msg_wrapper *send_wrapper;

    failed = !plat_calloc_struct(&op);
    plat_assert(!failed);
    op->op_type = CRO_TYPE_REDO;
    op->src_replica = src_replica;
    op->dest_replica = dest_replica;
    op->data.wrapper = request;
    sdf_msg_wrapper_ref_count_inc(op->data.wrapper);
    sdf_msg_wrapper_rwref(&op->data.msg, op->data.wrapper);
    op->data.pm = (SDF_protocol_msg_t *)op->data.msg->msg_payload;
    op->data.pmi = &(SDF_Protocol_Msg_Info[op->data.pm->msgtype]);

    op->put_msgtype = op->data.pm->msgtype;

    /* FIXME: Initialize op->data.msg, op->data.pm, op->data.pmi.  */

    response_cb =
        sdf_msg_recv_wrapper_create(cr->callbacks.single_scheduler,
                                    &cr_replica_redo_response, op);

    op->mbx =
        sdf_fth_mbx_resp_closure_alloc(response_cb,
                                       /* XXX: release arg goes away */
                                       SACK_REL_YES,
                                       cr->config.timeout_usecs);

    send_wrapper =
        sdf_msg_wrapper_copy(request,
                             SMW_TYPE_REQUEST,
                             cr->config.my_node /* src */,
#if 1
                             cr->config.response_service /* src */,
#else
                             cr->config.replication_service,
#endif
                             dest_replica->node, /* dest */
                             cr->config.flash_service /* dest */,
                             FLSH_REQUEST, NULL);

    ++src_replica->recovery_op_pending_count;
    ++src_replica->ref_count;

    /*
     * Ops are charged to both source (to apply back pressure, so it will
     * stop stuffing IOs into the destination replicas and allow them to make
     * progress on their own gets) and destination which will keep the
     * destination from transitioning with stale operations still pending.
     *
     * XXX: drew 2009-05-25 We could skip the reference count in
     * the replica being modified if we had correct ltime fencing.
     */
    ++dest_replica->recovery_op_pending_count;
    ++dest_replica->ref_count;

    plat_log_msg(21400, LOG_CAT_RECOVERY_REDO,
                 PLAT_LOG_LEVEL_TRACE,
                 "cr_replica %p node %u shard 0x%lx vip group %d redo"
                 " replica at node %u to node %u"
                 " op %s seqno %llu key %*.*s data len %u",
                 src_replica, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id, src_replica->node, dest_replica->node,
                 op->data.pmi->shortname,
                 (long long)op->data.pm->seqno, op->data.pm->key.len,
                 op->data.pm->key.len, op->data.pm->key.key,
                 (unsigned)op->data.pm->data_size);

    if (op->put_msgtype == HFSET) {
        ++shard->stat_counters->recovery_set_started;
        ++shard->cr->total_stat_counters->recovery_set_started;
    } else if (op->put_msgtype == HFDFF) {
        ++shard->stat_counters->recovery_delete_started;
        ++shard->cr->total_stat_counters->recovery_delete_started;
    } else {
        plat_assert(0);
    }

    plat_closure_apply(sdf_replicator_send_msg_cb,
                       &cr->callbacks.send_msg, send_wrapper,
                       op->mbx, NULL);
}

/**
 * @brief Callback invoked on redo modification operation completion
 *
 * @param env <IN> #cr_redo_op structure for this operation
 * @param response <IN>  SDF_protocol_msg_t from messsaging system
 */
static void
cr_replica_redo_response(struct plat_closure_scheduler *context,
                         void *env, struct sdf_msg_wrapper *response) {
    struct cr_recovery_op *op = (struct cr_recovery_op *)env;
    struct cr_shard *shard = op->src_replica->shard;
    struct cr_replica *src_replica = op->src_replica;
    struct cr_replica *dest_replica = op->dest_replica;
    struct sdf_msg *response_msg;
    SDF_protocol_msg_t *response_pm;
    SDF_status_t status;
    struct sdf_msg_error_payload *error_payload;

    response_msg = NULL;
    sdf_msg_wrapper_rwref(&response_msg, response);
    response_pm = (SDF_protocol_msg_t *)response_msg->msg_payload;

    if (response_msg->msg_type == SDF_MSG_ERROR) {
        error_payload =
            (struct sdf_msg_error_payload *)response_msg->msg_payload;
        status = error_payload->error;
    } else {
        status = response_pm->status;
    }

    plat_log_msg(21401, LOG_CAT_RECOVERY_REDO,
                 (status != SDF_SUCCESS &&
                  shard->state != CR_REPLICA_STATE_TO_DEAD) ?
                 PLAT_LOG_LEVEL_WARN : PLAT_LOG_LEVEL_TRACE,
                 "cr_replica %p node %u shard 0x%lx vip group %d redo replica"
                 " at node %u to cr_replica %p node %u"
                 " op %s seqno %llu key %*.*s data len %u status %s",
                 src_replica, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id, src_replica->node, dest_replica,
                 dest_replica->node,
                 op->data.pmi->shortname, (long long)op->data.pm->seqno,
                 op->data.pm->key.len, op->data.pm->key.len,
                 op->data.pm->key.key, (unsigned)op->data.pm->data_size,
                 sdf_status_to_string(status));

    if (op->put_msgtype == HFSET) {
        ++shard->stat_counters->recovery_set_complete;
        ++shard->cr->total_stat_counters->recovery_set_complete;
        if (status != SDF_SUCCESS && shard->state != CR_REPLICA_STATE_TO_DEAD) {
            ++shard->stat_counters->recovery_set_failed;
            ++shard->cr->total_stat_counters->recovery_set_failed;
        }
    } else if (op->put_msgtype == HFDFF) {
        ++shard->stat_counters->recovery_delete_complete;
        ++shard->cr->total_stat_counters->recovery_delete_complete;
        if (status != SDF_SUCCESS && shard->state != CR_REPLICA_STATE_TO_DEAD) {
            ++shard->stat_counters->recovery_delete_failed;
            ++shard->cr->total_stat_counters->recovery_delete_failed;
        }
    } else {
        plat_fatal("Unhandled op->put_msgtype");
    }

    if (status != SDF_SUCCESS) {
        cr_replica_set_state(dest_replica, CR_REPLICA_STATE_TO_DEAD);
    }


    plat_assert(src_replica->recovery_op_pending_count > 0);
    --src_replica->recovery_op_pending_count;
    plat_assert(dest_replica->recovery_op_pending_count > 0);
    --dest_replica->recovery_op_pending_count;

    sdf_msg_wrapper_rwrelease(&response_msg, response);
    sdf_msg_wrapper_ref_count_dec(response);

    cr_replica_do_recovery(src_replica);
    cr_replica_do_recovery(dest_replica);

    sdf_msg_wrapper_rwrelease(&op->data.msg, op->data.wrapper);
    sdf_msg_wrapper_ref_count_dec(op->data.wrapper);

    sdf_fth_mbx_free(op->mbx);

    plat_free(op);

    cr_replica_ref_count_dec(src_replica);
    cr_replica_ref_count_dec(dest_replica);
}

/**
 * @brief Perform undo in replica where wrapper is from the failed node get
 *
 * 1.  Starts with get by cursor from the stale replica.
 *
 * 2.  Write lock key so that we don't conflict with new writes in live writable
 *     replicas.
 *
 * 3.  Get from the local authoritative replica
 *
 * 4.  Perform compensating action when the stale replica does not match
 *     the authoritative  copy.
 *
 * 5.  Release lock on key, allow cr_replica_do_recovery to continue
 *
 * Performs step:
 * 2.  Write lock key so that we don't conflict with new writes in live writable
 *     replicas.
 *
 * Completes operation on failure
 *
 * @param replica <IN> Replica which is being undone
 * @param get_response <IN> Result from the iteration get by cursor
 */

/*
 * XXX: drew 2009-06-18 There may be some reasonable level of refactoring
 * which can be done between this and cr_replica_redo_op.
 */
static void
cr_replica_undo_op(struct cr_replica *replica,
                   struct sdf_msg_wrapper *get_response) {
    struct cr_shard *shard = replica->shard;
    struct copy_replicator *cr = shard->cr;
    int failed;
    struct cr_recovery_op *op;
    rkl_cb_t lock_cb;

    plat_assert(replica->state == CR_REPLICA_STATE_UNDO);

    failed = !plat_calloc_struct(&op);
    plat_assert(!failed);
    op->op_type = CRO_TYPE_UNDO;
    op->src_replica = replica->shard->replicas[shard->local_replica];
    op->dest_replica = replica;
    op->data.wrapper = get_response;
    sdf_msg_wrapper_ref_count_inc(op->data.wrapper);
    sdf_msg_wrapper_rwref(&op->data.msg, get_response);
    op->data.pm = (SDF_protocol_msg_t *)op->data.msg->msg_payload;
    op->data.pmi = &(SDF_Protocol_Msg_Info[op->data.pm->msgtype]);
    op->put_msgtype = SDF_PROTOCOL_MSG_INVALID;

    TAILQ_INSERT_TAIL(&op->src_replica->src_recovery_op_list,
                      op, src_recovery_op_list_entry);
    TAILQ_INSERT_TAIL(&op->dest_replica->dest_recovery_op_list,
                      op, dest_recovery_op_list_entry);

    /*
     * Ops are charged to both source (to apply back pressure, so it will
     * stop stuffing IOs into the destination replicas and allow them to make
     * progress on their own gets) and destination which will keep the
     * destination from transitioning with stale operations still pending.
     *
     * XXX: drew 2009-07-01 For undo we could skip the src_replica ref count
     * if we had the ltime fencing working correctly because undo works by
     * writing compensating ops into all live replicas, the local replica
     * is the source replica, it's always live when the local node is the
     * master, and a non-zero recovery op pending count prevents a transition
     * out of CR_SHARD_STATE_TO_DEAD.
     */
    ++op->src_replica->recovery_op_pending_count;
    ++op->src_replica->ref_count;

    ++op->dest_replica->recovery_op_pending_count;
    ++op->dest_replica->ref_count;

    plat_log_msg(21402, LOG_CAT_RECOVERY, PLAT_LOG_LEVEL_TRACE,
                 "cr_recovery_op %p node %u shard 0x%lx vip group %d undo"
                 " replica at node %u get by cursor results "
                 " msg %s seqno %llu key %*.*s data len %u allocated",
                 op, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id, op->dest_replica->node,
                 op->data.pmi->shortname, (long long)op->data.pm->seqno,
                 op->data.pm->key.len, op->data.pm->key.len,
                 op->data.pm->key.key, (unsigned)op->data.pm->data_size);

    lock_cb =
        rkl_cb_create(cr->callbacks.single_scheduler,
                      &cr_replica_undo_op_lock_cb, op);
    rklc_lock(shard->lock_container, &op->data.pm->key, RKL_MODE_EXCLUSIVE,
              lock_cb);
}

/**
 * @brief #cr_replica_undo_op lock acquired callback
 *
 * In response to step:
 * 2.  Write lock key so that we don't conflict with new writes in live writable
 *     replicas.
 *
 * Performs step:
 * 3.  Get from the local authoritative replica
 *
 * Completes operation when no compensatory action is required and on
 * error.
 *
 * @param env <IN> operation
 */
static void
cr_replica_undo_op_lock_cb(struct plat_closure_scheduler *context, void *env,
                           SDF_status_t status_arg,
                           struct replicator_key_lock *key_lock) {
    struct cr_recovery_op *op = (struct cr_recovery_op *)env;
    struct cr_shard *shard = op->src_replica->shard;
    struct copy_replicator *cr = shard->cr;
    sdf_msg_recv_wrapper_t response_cb;
    struct sdf_msg *send_msg = NULL;
    struct sdf_msg_wrapper *send_wrapper;
    SDF_protocol_msg_t *send_pm;

    SDF_status_t status = status_arg;

    plat_log_msg(21403, LOG_CAT_RECOVERY, PLAT_LOG_LEVEL_TRACE,
                 "cr_recovery_op %p node %u shard 0x%lx vip group %d undo"
                 " replica at node %u seqno %llu tombstone %d"
                 " key %*.*s lock %s",
                 op, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id, op->dest_replica->node,
                 (long long)op->data.pm->seqno,
                 op->undo.pm->flags & f_tombstone ? 1 : 0, op->data.pm->key.len,
                 op->data.pm->key.len, op->data.pm->key.key,
                 status == SDF_SUCCESS ? "granted" : "failed");

    if (status == SDF_SUCCESS) {
        op->undo.lock = key_lock;
    }

    if (op->dest_replica->state != CR_REPLICA_STATE_UNDO) {
        status = SDF_STATE_CHANGED;
    }

    if (status == SDF_SUCCESS) {
        response_cb =
            sdf_msg_recv_wrapper_create(cr->callbacks.single_scheduler,
                                        &cr_replica_undo_op_authoritative_get_cb,
                                        op);
        op->mbx =
            sdf_fth_mbx_resp_closure_alloc(response_cb,
                                           /* XXX: release arg goes away */
                                           SACK_REL_YES,
                                           cr->config.timeout_usecs);

        send_msg = sdf_msg_alloc(sizeof (*send_pm));
        plat_assert(send_msg);

        send_wrapper =
            sdf_msg_wrapper_local_alloc(send_msg,
                                        sdf_msg_wrapper_free_local_null,
                                        SMW_MUTABLE_FIRST, SMW_TYPE_REQUEST,
                                        cr->config.my_node /* src */,
#if 1
                                        cr->config.response_service /* src */,
#else /* FIXME do we still need this below */
                                        cr->config.replication_service,
#endif
                                        cr->config.my_node /* dest */,
                                        cr->config.replication_service /* dest */,
                                        /* XXX the message type is silly */
                                        REPL_REQUEST,
                                        NULL /* not response */);

        plat_assert(send_wrapper);

        send_pm = (SDF_protocol_msg_t *)send_msg->msg_payload;
        send_pm->msgtype = HFGFF;
        send_pm->shard = shard->sguid;
        send_pm->key = op->data.pm->key;
        send_pm->node_from = cr->config.my_node;
        send_pm->node_to = cr->config.my_node;
        send_pm->status = SDF_SUCCESS;

        /* This is safe because we have a lock on the object */
        plat_closure_apply(sdf_replicator_send_msg_cb,
                           &cr->callbacks.send_msg, send_wrapper,
                           op->mbx, NULL);
    }

    if (status != SDF_SUCCESS) {
        cr_replica_undo_op_complete(op, status);
    }
}

/**
 * @brief #cr_replica_undo_op authoritative get callback
 *
 * In response to step:
 * 3.  Get from the local authoritative replica
 *
 * Starts step:
 * 4.  Perform compensating action when the stale replica does not match
 *     the authoritative copy.
 *
 * @param env <IN> operation
 */
static void
cr_replica_undo_op_authoritative_get_cb(struct plat_closure_scheduler *context,
                                        void *env,
                                        struct sdf_msg_wrapper *get_response) {
    struct cr_recovery_op *op = (struct cr_recovery_op *)env;
    struct cr_shard *shard = op->src_replica->shard;

    plat_assert(op->mbx);
    plat_assert_imply(op->data.pm->status == SDF_OBJECT_UNKNOWN,
                      op->data.pm->flags & f_tombstone);
    plat_assert(op->data.pm->seqno != SDF_SEQUENCE_NO_INVALID);

    op->undo.wrapper = get_response;
    sdf_msg_wrapper_rwref(&op->undo.msg, op->undo.wrapper);
    op->undo.pm =
        (SDF_protocol_msg_t *)op->undo.msg->msg_payload;
    op->undo.pmi =
        &(SDF_Protocol_Msg_Info[op->undo.pm->msgtype]);

    sdf_fth_mbx_free(op->mbx);
    op->mbx = NULL;

    /* Log */
    plat_log_msg(21404, LOG_CAT_RECOVERY_UNDO,
                 op->undo.pm->status == SDF_SUCCESS ||
                 op->undo.pm->status == SDF_OBJECT_UNKNOWN ?
                 PLAT_LOG_LEVEL_TRACE : PLAT_LOG_LEVEL_WARN,
                 "cr_recovery_op %p node %u shard 0x%lx vip group %d undo"
                 " replica at node %u authoritative get results "
                 " msg %s seqno %llu tombstone %d key %*.*s data len %u"
                 " status %s",
                 op, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id, op->dest_replica->node,
                 op->undo.pmi->shortname,
                 (long long)op->undo.pm->seqno,
                 op->undo.pm->flags & f_tombstone ? 1 : 0,
                 op->undo.pm->key.len,
                 op->undo.pm->key.len, op->undo.pm->key.key,
                 (unsigned)op->undo.pm->data_size,
                 sdf_status_to_string(op->undo.pm->status));

    /*
     * Decide what to do
     *
     * XXX: drew 2009-06-18 Add statistics for each of the cases
     * so we can validate that we're getting reasonable test coverage.
     */
    if (op->dest_replica->state != CR_REPLICA_STATE_UNDO) {
        cr_replica_undo_op_complete(op, SDF_STATE_CHANGED);
    } else if (op->undo.pm->status != SDF_OBJECT_UNKNOWN &&
               op->undo.pm->status != SDF_SUCCESS) {
        cr_replica_undo_op_complete(op, op->undo.pm->status);
    /* Ignore when stale replica sequence number matches current */
    } else if (op->data.pm->seqno != SDF_SEQUENCE_NO_INVALID &&
               op->data.pm->seqno == op->undo.pm->seqno) {
        cr_replica_undo_op_complete(op, SDF_SUCCESS);
    /*
     * Ignore when the stale replica version has been superseded and fix later
     * in the redo phase.
     */
    } else if (op->data.pm->seqno != SDF_SEQUENCE_NO_INVALID &&
               op->undo.pm->seqno != SDF_SEQUENCE_NO_INVALID &&
               op->undo.pm->seqno > op->data.pm->seqno) {
        cr_replica_undo_op_complete(op, SDF_SUCCESS);
    /* Ignore when stale replica is a tombstone which was garbage collected */
    } else if ((op->data.pm->flags & f_tombstone) &&
               op->undo.pm->status == SDF_OBJECT_UNKNOWN &&
               !(op->undo.pm->flags & f_tombstone)) {
        cr_replica_undo_op_complete(op, SDF_SUCCESS);
    /* Otherwise it must be a mismatch */

    /*
     * An object exists in the stale replica but there is no tombstone
     * in the new replica
     *
     * As of 2009-06-19 we didn't return tombstones on reads, so this will be
     * an unconditional delete of objects not received by the authoritative
     * replicas.
     */
    } else if (!(op->data.pm->flags & f_tombstone) &&
               op->undo.pm->status == SDF_OBJECT_UNKNOWN &&
               !(op->undo.pm->flags & f_tombstone)) {
        op->undo.pm->msgtype = HFDFF;
        cr_replica_undo_op_put(op);
    /*
     * The object or tombstone in the authoritative replicas is older
     * than the object in the stale replica.  Even tombstones in the
     * stale replica must be replaced so that when the stale replica
     * becomes authoritative and is used to recover another replica it has a
     * tombstone which is guaranteed to supersede all newer objects.
     *
     * As of 2009-06-19 we didn't return tombstones on reads, so this will
     * unconditionally duplicate deletes
     */
    } else if (op->undo.pm->seqno != SDF_SEQUENCE_NO_INVALID &&
               op->data.pm->seqno > op->undo.pm->seqno) {
        op->undo.pm->msgtype = HFSET;
        cr_replica_undo_op_put(op);
    /*
     * FIXME: drew 2009-06-19 We need to move away from plat_fatal
     * since it will take all shards off line when there's a
     * failure with one.
     *
     * For now we just fail so that we get a core file and can figure out
     * what's happening
     */
    } else {
        plat_fatal("Recovery code is defective");
    }
}

/*
 * @brief Perform compensatory operation for undo
 *
 * Starts step:
 * 4.  Perform compensating action when the stale replica does not match
 *     the authoritative copy.
 *
 * @param op <IN> operation
 * @param put_msgtype <IN> compensating action (most likely HFDFF
 * or HFSET)
 * @param get_response_msg <IN> response authoritative replica
 */
static void
cr_replica_undo_op_put(struct cr_recovery_op *op) {
    struct cr_replica *replica = op->dest_replica;
    struct cr_shard *shard = replica->shard;
    SDF_status_t status;
    struct sdf_msg_wrapper *send_wrapper;
    sdf_msg_recv_wrapper_t response_cb;

    plat_assert(replica->state == CR_REPLICA_STATE_UNDO);

    op->undo.pm->flags |= f_internal_lock;
    op->undo.pm->flags &= ~f_tombstone;

    status = cr_shard_get_op_meta(shard, &op->undo.pm->op_meta);

    plat_log_msg(21405, LOG_CAT_RECOVERY_UNDO,
                 PLAT_LOG_LEVEL_TRACE,
                 "cr_recovery_op %p node %u shard 0x%lx vip group %d undo"
                 " replica at node %d put "
                 " msg %s key %*.*s data len %u",
                 op, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id, op->dest_replica->node,
                 op->undo.pmi->shortname,
                 op->undo.pm->key.len, op->undo.pm->key.len,
                 op->undo.pm->key.key, (unsigned)op->undo.pm->data_size);

    if (status != SDF_SUCCESS) {
        cr_replica_undo_op_complete(op, status);
    } else {
        op->put_msgtype = op->undo.pm->msgtype;

        if (op->put_msgtype == HFSET) {
            ++shard->stat_counters->recovery_set_started;
            ++shard->cr->total_stat_counters->recovery_set_started;
        } else if (op->put_msgtype == HFDFF) {
            ++shard->stat_counters->recovery_delete_started;
            ++shard->cr->total_stat_counters->recovery_delete_started;
        }

        send_wrapper = op->undo.wrapper;
        sdf_msg_wrapper_rwrelease(&op->undo.msg, send_wrapper);
        op->undo.wrapper = NULL;
        op->undo.pm = NULL;
        op->undo.pmi = NULL;

        response_cb =
            sdf_msg_recv_wrapper_create(shard->cr->callbacks.single_scheduler,
                                        &cr_replica_undo_op_put_cb, op);
        plat_assert(!op->mbx);
        op->mbx =
            sdf_fth_mbx_resp_closure_alloc(response_cb,
                                           /* XXX: release arg goes away */
                                           SACK_REL_YES,
                                           shard->cr->config.timeout_usecs);
        plat_closure_apply(sdf_replicator_send_msg_cb,
                           &shard->cr->callbacks.send_msg, send_wrapper,
                           op->mbx, NULL);
    }
}

/**
 * @brief #cr_replica_undo_op put callback
 *
 * In response to step:
 * 4.  Perform compensating action when the stale replica does not match
 *     the authoritative copy.
 *
 * Completes operation
 *
 * @param env <IN> operation
 */
static void
cr_replica_undo_op_put_cb(struct plat_closure_scheduler *context, void *env,
                          struct sdf_msg_wrapper *put_response) {
    struct cr_recovery_op *op = (struct cr_recovery_op *)env;
    struct cr_shard *shard = op->dest_replica->shard;
    struct sdf_msg *msg;
    SDF_protocol_msg_t *pm;
    SDF_Protocol_Msg_Info_t *pmi;
    SDF_status_t status;

    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, put_response);
    pm = (SDF_protocol_msg_t *)msg->msg_payload;
    pmi = &(SDF_Protocol_Msg_Info[pm->msgtype]);
    status = pm->status;

    /* Log */
    plat_log_msg(21406, LOG_CAT_RECOVERY_UNDO,
                 pm->status == SDF_SUCCESS ? PLAT_LOG_LEVEL_TRACE :
                 PLAT_LOG_LEVEL_WARN,
                 "cr_recovery_op %p node %u shard 0x%lx vip group %d undo"
                 " replica at node %u put results "
                 " msg %s seqno %llu key %*.*s data len %u status %s",
                 op, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id, op->dest_replica->node,
                 pmi->shortname, (long long)pm->seqno,
                 pm->key.len, pm->key.len, pm->key.key,
                 (unsigned)pm->data_size, sdf_status_to_string(status));

    plat_assert(op->put_msgtype != SDF_PROTOCOL_MSG_INVALID);

    if (op->put_msgtype == HFSET) {
        ++shard->stat_counters->recovery_set_complete;
        ++shard->cr->total_stat_counters->recovery_set_complete;
        if (status != SDF_SUCCESS) {
            ++shard->stat_counters->recovery_set_failed;
            ++shard->cr->total_stat_counters->recovery_set_failed;
        }
    } else if (op->put_msgtype == HFDFF) {
        ++shard->stat_counters->recovery_delete_complete;
        ++shard->cr->total_stat_counters->recovery_delete_complete;
        if (status != SDF_SUCCESS) {
            ++shard->stat_counters->recovery_delete_failed;
            ++shard->cr->total_stat_counters->recovery_delete_failed;
        }
    } else {
        plat_fatal("Unhandled op->put_msgtype");
    }

    sdf_fth_mbx_free(op->mbx);
    op->mbx = NULL;

    sdf_msg_wrapper_rwrelease(&msg, put_response);
    sdf_msg_wrapper_ref_count_dec(put_response);

    cr_replica_undo_op_complete(op, status);
}

/**
 * @brief Completes undo operation
 *
 * Performs step:
 *
 * 5.  Release lock on key, allow cr_replica_do_recovery to continue
 *
 * @param op <IN> operation
 * @param status <IN> status of entire operation
 */
static void
cr_replica_undo_op_complete(struct cr_recovery_op *op, SDF_status_t status) {
    struct cr_replica *dest_replica = op->dest_replica;
    struct cr_replica *src_replica = op->src_replica;
    struct cr_shard *shard = op->dest_replica->shard;

    plat_log_msg(21407, LOG_CAT_RECOVERY_UNDO,
                 status == SDF_SUCCESS ? PLAT_LOG_LEVEL_TRACE :
                 PLAT_LOG_LEVEL_WARN,
                 "cr_recovery_op %p node %u shard 0x%lx vip group %d undo "
                 " replica at node %d complete  status %s",
                 op, shard->cr->config.my_node, shard->sguid,
                 shard->vip_group_id, op->dest_replica->node,
                 sdf_status_to_string(status));

    /* Can't have anything in flight */
    plat_assert(!op->mbx);
    plat_assert_iff(op->data.wrapper, op->data.msg);
    plat_assert_iff(op->undo.wrapper, op->undo.msg);

    if (op->undo.lock) {
        rkl_unlock(op->undo.lock);
        op->undo.lock = NULL;
    }

    if (status != SDF_SUCCESS &&
        op->dest_replica->state == CR_REPLICA_STATE_UNDO) {
        cr_replica_set_state(op->dest_replica, CR_REPLICA_STATE_MARK_FAILED);
    }

    TAILQ_REMOVE(&src_replica->dest_recovery_op_list, op,
                 src_recovery_op_list_entry);
    TAILQ_REMOVE(&dest_replica->dest_recovery_op_list, op,
                 dest_recovery_op_list_entry);

    --dest_replica->recovery_op_pending_count;
    --src_replica->recovery_op_pending_count;

    cr_replica_do_recovery(dest_replica);

    /*
     * Keep these pieces around until after we return from
     * #cr_replica_do_recovery incase we get a core dump and want
     * to look at the state
     */

    if (op->data.wrapper) {
        sdf_msg_wrapper_rwrelease(&op->data.msg, op->data.wrapper);
        sdf_msg_wrapper_ref_count_dec(op->data.wrapper);
        op->data.wrapper = NULL;
    }

    if (op->undo.wrapper) {
        sdf_msg_wrapper_rwrelease(&op->undo.msg, op->undo.wrapper);
        sdf_msg_wrapper_ref_count_dec(op->undo.wrapper);
        op->undo.wrapper = NULL;
    }

    plat_free(op);

    cr_replica_ref_count_dec(src_replica);
    cr_replica_ref_count_dec(dest_replica);
}

/** @brief Decrement #cr_replica reference count, free on zero */
static void
cr_replica_ref_count_dec(struct cr_replica *replica) {
    int after;

    after = __sync_sub_and_fetch(&replica->ref_count, 1);
    plat_assert(after >= 0);
    if (!after) {
        plat_log_msg(21408, LOG_CAT_SHUTDOWN,
                     PLAT_LOG_LEVEL_DEBUG,
                     "cr_replica %p node %u shard 0x%lx vip group %d"
                     " refcount 0",
                     replica, replica->shard->cr->config.my_node,
                     replica->shard->sguid, replica->shard->vip_group_id);

        plat_assert(!replica->get_iteration_cursors_pending);
        plat_assert(!replica->recovery_op_pending_count);
        plat_assert(TAILQ_EMPTY(&replica->src_recovery_op_list));
        plat_assert(TAILQ_EMPTY(&replica->dest_recovery_op_list));

        if (replica->get_iteration_cursors) {
            rr_get_iteration_cursors_free(replica->get_iteration_cursors);
        }
        if (replica->next_iteration_cursors) {
            rr_get_iteration_cursors_free(replica->next_iteration_cursors);
        }
        if (replica->get_iteration_cursors_resume_data) {
            plat_free(replica->get_iteration_cursors_resume_data);
        }
    }
}

static const char *
cr_replica_state_to_string(enum cr_replica_state state) {
    switch (state) {
#define item(caps, lower, flags, enter, leave) case caps: return (#lower);
    CR_REPLICA_STATE_ITEMS(/* none */, /* none */, /* none */)
#undef item
    }

    plat_assert(0);

    return ("invalid");
}

static int
cr_replica_is_writeable(const struct cr_replica *replica) {
    return (replica->state_flags &
            (CRSF_ALLOW_CLIENT_WO_BLOCK|CRSF_ALLOW_CLIENT_WO|
             CRSF_ALLOW_CLIENT_RW));
}

static int
cr_replica_state_to_flags(enum cr_replica_state state) {
    switch (state) {
#define item(caps, lower, flags, enter, leave) case caps: return (flags);
    CR_REPLICA_STATE_ITEMS(/* none */, /* none */, /* none */)
#undef item
    }

    plat_fatal("Unknown state");
    return (0);
}

/** @brief Assert when replica state is obviously incorrect */
static void
cr_replica_state_validate(enum cr_replica_state state) {
    int flags;

    flags = cr_replica_state_to_flags(state);
    plat_assert_either(flags & CRSF_ALLOW_CLIENT_NONE,
                       flags & CRSF_ALLOW_CLIENT_WO_BLOCK,
                       flags & CRSF_ALLOW_CLIENT_WO,
                       flags & CRSF_ALLOW_CLIENT_RW,
                       flags & CRSF_ALLOW_CLIENT_RO);

    plat_assert_either(flags & CRSF_ITERATING,
                       flags & CRSF_NOT_ITERATING);
}

/** @brief Assert when replica states are obviously incorrect */
static void
cr_replica_states_validate() {
#define item(caps, lower, flags, enter, leave) cr_replica_state_validate(caps);
    CR_REPLICA_STATE_ITEMS(/* none */, /* none */, /* none */)
#undef item
}


/**
 * @brief Allocate an operation structure with reference count 1
 *
 * The operation is created in an un-issued state and must be
 * started with #cr_op_start.
 */
static struct cr_op *
cr_op_alloc(struct copy_replicator *cr,
            struct sdf_msg_wrapper *request_wrapper) {
    struct cr_op *ret;

    if (plat_calloc_struct(&ret)) {
        ret->cr = cr;

        ret->request_wrapper = request_wrapper;
        ret->request_msg = NULL;
        sdf_msg_wrapper_rwref(&ret->request_msg, request_wrapper);
        ret->request_pm = (SDF_protocol_msg_t *)ret->request_msg->msg_payload;
        ret->request_pmi = &(SDF_Protocol_Msg_Info[ret->request_pm->msgtype]);

        ret->response_wrapper = NULL;

        switch (ret->request_pm->msgtype) {
#define item(msgtype, fanout_arg, flags_arg, start_fn) \
        case msgtype:                                                          \
            ret->flags = flags_arg;                                            \
            ret->fanout = CR_FANOUT_ ## fanout_arg;                            \
            break;
        CR_MSG_ITEMS(/* none */, /* none */)
#undef item
        default:
            plat_log_msg(21409, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "Unsupported message type (msgtype = %s)",
                         SDF_Protocol_Msg_Info[ret->request_pm->msgtype].
                         shortname);
            plat_fatal("");
        }
        ret->status = SDF_SUCCESS;
        ret->ref_count = 1;

        /* FIXME: A threading model change means more locking here */
        (void) __sync_add_and_fetch(&cr->ref_count, 1);
        LIST_INSERT_HEAD(&cr->op_list, ret, op_list);
    }

    return (ret);
}

/** @brief Start the given operation */
static void
cr_op_start(struct cr_op *op, struct plat_closure_scheduler *context) {
    struct copy_replicator *cr = op->cr;
    rkl_cb_t lock_cb;
    enum rkl_mode lock_mode;

    /* Hold a reference count until a lock is acquired */
    (void) __sync_add_and_fetch(&op->ref_count, 1);

    plat_log_msg(21410, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "cr_op %p node %u shard 0x%lx start"
                 " Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d sending to %s",
                 op, op->cr->config.my_node, op->request_pm->shard,
                 op->request_pmi->name, op->request_pmi->shortname,
                 op->request_pm->node_from,
                 SDF_Protocol_Nodes_Info[op->request_pmi->src].name,
                 op->request_pm->node_to,
                 SDF_Protocol_Nodes_Info[op->request_pmi->dest].name,
                 op->request_pm->tag, cr_fanout_to_string(op->fanout));
    if (op->request_pm->flags & f_internal_lock) {
        op->flags |= CROF_LOCK_EXISTS;
    }

    if ((op->request_pm->op_meta.shard_meta.type ==
         SDF_REPLICATION_META_SUPER_NODE ||
         op->request_pm->op_meta.shard_meta.type ==
         SDF_REPLICATION_META_CONSENSUS)) {
        if (!op->shard || op->shard->state != CR_SHARD_STATE_RW) {
            cr_op_accum_status(op, SDF_WRONG_NODE);
            lock_mode = RKL_MODE_NONE;
        } else if ((op->flags & (CROF_LOCK_NONE|CROF_LOCK_EXISTS))) {
            lock_mode = RKL_MODE_NONE;
        } else if (op->flags & CROF_LOCK_EXCLUSIVE) {
            lock_mode = RKL_MODE_EXCLUSIVE;
        } else if (op->flags & CROF_LOCK_SHARED) {
            lock_mode = RKL_MODE_SHARED;
        } else {
            plat_fatal("Impossible lock mode");
            lock_mode = RKL_MODE_NONE;
        }
    } else {
        lock_mode = RKL_MODE_NONE;
    }

    lock_cb = rkl_cb_create(cr->callbacks.single_scheduler, &cr_op_lock_cb, op);
    if (lock_mode != RKL_MODE_NONE) {
        plat_assert(op->shard);
        rklc_lock(op->shard->lock_container, &op->request_pm->key, lock_mode,
                  lock_cb);
    } else {
        cr_op_lock_cb(context, op, SDF_SUCCESS, NULL /* lock */);
    }
}

/*
 * @brief Called when lock is acquired or bypassed
 *
 * @param env <IN> op
 */
static void
cr_op_lock_cb(struct plat_closure_scheduler *context,
              void *env, SDF_status_t status_arg,
              struct replicator_key_lock *key_lock) {
    struct cr_op *op = (struct cr_op *)env;
    SDF_status_t status = status_arg;

    op->lock = key_lock;

    if (status == SDF_SUCCESS && op->shard && op->flags & CROF_NEED_SEQNO) {
        op->seqno = __sync_fetch_and_add(&op->shard->seqno, 1);
        /*
         * XXX: drew 2009-05-26 Move this logic down into #cr_op_flash_forward
         * so we can retain a read-only version of the original message.
         */
        op->request_pm->seqno = op->seqno;
        op->request_pm->op_meta.seqno = op->seqno;
    }

    if (status == SDF_SUCCESS) {
        switch (op->request_pm->msgtype) {
#define item(msgtype, fanout, flags_arg, start_fn) \
        case msgtype:                                                          \
            status = start_fn;                                                 \
            break;
        CR_MSG_ITEMS(op, context)
#undef item
        default:
            plat_fatal("Unhandled message type");
        }
    }

    /* Defer completion until end of this fn incase ref count would hit zero */
    cr_op_ref_count_dec(op);
}

/**
 * @brief Called by top-level op code on meta get completion to start flash op
 *
 * Consumes no reference counts.
 *
 * @param context <IN> scheduler context.  Used for closure chaining
 * optimization.
 * @param env <IN> op from closure
 * @param meta <IN>
 *
 * XXX: We need to embed the meta-data in the messages starting
 * at the client side so we don't make a round trip to the replication
 * code.
 */
static void
cr_op_meta_response_flash(struct plat_closure_scheduler *context, void *env,
                          SDF_status_t status,
                          struct SDF_container_meta *meta) {
    struct cr_op *op = (struct cr_op *)env;
    uint32_t i;
    uint32_t nreplica = 0;
    uint32_t nmeta_replica = 0;
    vnode_t pnodes[SDF_REPLICATION_MAX_REPLICAS];
    struct cr_node *node;
    sdf_msg_recv_wrapper_t response_closure;

    plat_assert(op->fanout == CR_FANOUT_ALL_LIVE ||
                op->fanout == CR_FANOUT_ALL_OR_NOTHING);

    plat_log_msg(21411, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "copy_replicator %p op %p got meta", op->cr, op);

    cr_op_accum_status(op, status);

    if (status == SDF_SUCCESS) {
        cr_get_pnodes(&op->cr->config, &meta->properties.replication,
                      /* XXX: drew 2008-10-27 This is wrong with > 1 shard */
                      meta->node,
                      pnodes, &nreplica,
                      &nmeta_replica);

        plat_log_msg(21412, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "copy_replicator %p op %p %d replicas",
                     op->cr, op, nreplica);
    }

    response_closure =
        sdf_msg_recv_wrapper_create(op->cr->closure_scheduler,
                                    &cr_op_flash_response, op);

    for (i = 0; status == SDF_SUCCESS && i < nreplica; ++i) {
        node = cr_get_node(op->cr, pnodes[i]);
        plat_assert(node);

        /*
         * FIXME: drew 2009-05-26 We need to base this on replica status not
         * node status.  This can wait until after the 2009-05-27 demo because
         * we won't demonstrate on-line recovery.
         */
        if (op->fanout == CR_FANOUT_ALL_OR_NOTHING ||
            node->state == CR_NODE_LIVE) {
            status = cr_op_accum_status(op,
                                        cr_op_flash_forward(op, context, pnodes[i],
                                                            response_closure));
        }
    }

    /*
     * FIXME: This is ultimately a bug which needs to go away, because it
     * creates a race condition between create and destroy.   It's a silly
     * micro-optimization.
     */
#ifndef CMC_NO_COPY
    if (meta) {
        container_meta_destroy(meta);
    }
#endif
}

/**
 * @brief Top-level handling of flash operation completion
 *
 * The the operation response/status are updated with a call to
 * #cr_op_accum.
 *
 * @param context <IN> scheduler context.  Used for closure chaining
 * optimization.
 * @param env <IN> op from user request
 * @param response <IN> inbound flash response.  One reference count
 * is consumed
 */
static void
cr_op_flash_response(struct plat_closure_scheduler *context, void *env,
                     struct sdf_msg_wrapper *response) {
    struct cr_op *op = (struct cr_op *)env;

#if 0
    struct sdf_msg *msg;
    SDF_protocol_msg_t *pm;
    SDF_status_t msg_status;

    /*
     * FIXME: message is not necessarily a protocol message,
     * since them messaging layer doesn't have the original message
     * and doesn't know about message types.
     *
     * Should probably split cr_op_accum into cr_op_accum_status
     * and cr_op_accum_msg, where cr_op_accum_msg translates error
     * messages into statuses and the things passing in resposnes
     * don't need to be hung up on the specifics of message type.
     */

    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, response);
    pm = (SDF_protocol_msg_t *)msg->msg_payload;
    msg_status = pm->status;

    /* This now duplicates logging being done in crofr_response */
    SDF_Protocol_Msg_Info_t *pmi = &(SDF_Protocol_Msg_Info[pm->msgtype]);
    plat_log_msg(21413,
                 LOG_CAT, msg_status == SDF_SUCCESS ? PLAT_LOG_LEVEL_TRACE :
                 (msg_status == SDF_OBJECT_UNKNOWN ? PLAT_LOG_LEVEL_DIAGNOSTIC :
                  PLAT_LOG_LEVEL_INFO),
                 "copy_replicator %p op %p flash response received"
                 " Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d",
                 op->cr, op, pmi->name, pmi->shortname,
                 pm->node_from, SDF_Protocol_Nodes_Info[pmi->src].name,
                 pm->node_to, SDF_Protocol_Nodes_Info[pmi->dest].name,
                 pm->tag);

    sdf_msg_wrapper_rwrelease(&msg, response);
#endif

    cr_op_accum_msg(op, response);

    sdf_msg_wrapper_ref_count_dec(response);
}

/**
 * @brief Accumulate operation status/response for a message input
 *
 * XXX: drew 2009-01-15 The home node code does not know about
 * the messaging layer generated SDF_MSG_ERROR type and it's unclear
 * how hard it will be to retrofit.  For now we convert the error
 * message to an SDF_protocol_msg_t;
 *
 * @param op <IN> Operation
 *
 * @param response <IN>  Most recent response.  The pending response is
 * replaced when the status is SDF_SUCCESS and no response message is
 * set yet or the operation is transitioning to an error state.
 *
 * @return Cumulative status.
 */
static SDF_status_t
cr_op_accum_msg(struct cr_op *op, struct sdf_msg_wrapper *response_arg) {
    struct sdf_msg_wrapper *replacement_response;
    struct sdf_msg *msg;
    struct sdf_msg_error_payload *error_payload;
    SDF_protocol_msg_t *pm;
    SDF_status_t msg_status;

    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, response_arg);
    if (msg->msg_type == SDF_MSG_ERROR) {
        error_payload = (struct sdf_msg_error_payload *)msg->msg_payload;
        msg_status = error_payload->error;
        replacement_response = NULL;
    } else {
        pm = (SDF_protocol_msg_t *)msg->msg_payload;
        msg_status = pm->status;
        replacement_response = response_arg;
    }
    sdf_msg_wrapper_rwrelease(&msg, response_arg);

    if (op->status == SDF_SUCCESS &&
        (msg_status != SDF_SUCCESS || !(op->response_wrapper))) {
        if (op->response_wrapper) {
            sdf_msg_wrapper_ref_count_dec(op->response_wrapper);
        }
        if (replacement_response) {
            sdf_msg_wrapper_ref_count_inc(replacement_response);
        }
        op->response_wrapper = replacement_response;
        op->status = msg_status;
    }

    return (op->status);
}

/**
 * @brief Accumulate operation status/response for a status input
 *
 * @param op <IN> Operation
 *
 * @param msg_status <IN>  The pending response is replaced when the status is
 * SDF_SUCCESS and the new msg_status is not successful.
 *
 * @return Cumulative status.
 */
static SDF_status_t
cr_op_accum_status(struct cr_op *op, SDF_status_t msg_status) {
    if (op->status == SDF_SUCCESS && msg_status != SDF_SUCCESS) {
        if (op->response_wrapper) {
            sdf_msg_wrapper_ref_count_dec(op->response_wrapper);
        }
        op->response_wrapper = NULL;
        op->status = msg_status;
    }

    return (op->status);
}

/** @brief Decrement operation reference count, send response at 0 */
static void
cr_op_ref_count_dec(struct cr_op *op) {
    int after;

    after = __sync_sub_and_fetch(&op->ref_count, 1);
    plat_assert(after >= 0);
    if (PLAT_UNLIKELY(!after)) {
        cr_op_ref_count_zero(op);
    }
}

/**
 * @brief Call when op ref count hits zero; send response and cleanup
 */
static __inline__ void
cr_op_ref_count_zero(struct cr_op *op) {
    SDF_status_t status;

    plat_assert(!op->ref_count);

    /*
     * XXX: drew 2009-06-23
     *
     * Unfortunately, some of our engineers never released
     * a product in which they learned the importane of propagating
     * low level error reports to top level code.
     *
     * Until that changes log ALL potential problems at WARN
     * priority so we know what's going on
     */

    /* XXX: Do we want to print the original message type out here? */
    plat_log_msg(21414, LOG_CAT_OP,
                 op->status == SDF_SUCCESS && op->status != SDF_OBJECT_UNKNOWN
                 ? PLAT_LOG_LEVEL_TRACE : PLAT_LOG_LEVEL_WARN,
                 "copy_replicator %p node %u op %p seqno %llu"
                 " request done status %s",
                 op->cr, op->cr->config.my_node,
                 op, (long long)op->seqno,
                 sdf_status_to_string(op->status));

    plat_assert_imply(op->status == SDF_SUCCESS, op->response_wrapper);

    if (!op->response_wrapper) {
        cr_op_synthesize_response(op);
    } else {
        op->response_wrapper =
            sdf_msg_wrapper_forward_reply_alloc(op->request_wrapper,
                                                op->response_wrapper);
    }
    plat_assert(op->response_wrapper);

    plat_closure_apply(sdf_replicator_send_msg_cb, &op->cr->callbacks.send_msg,
                       op->response_wrapper,
                       NULL /* no response expected */, &status);
    plat_assert(status == SDF_SUCCESS);

    /* sdf_msg_wrapper send decrements the reference count */
    op->response_wrapper = NULL;

    sdf_msg_wrapper_rwrelease(&op->request_msg, op->request_wrapper);
    sdf_msg_wrapper_ref_count_dec(op->request_wrapper);

    LIST_REMOVE(op, op_list);

    /*
     * Unlock last so that responses for the same key are always sent in
     * order and we can sanity check for consistency between all internal
     * structures; but before ref count decrement so we don't dereference
     * a stale pointer.
     */
    if (op->lock) {
        rkl_unlock(op->lock);
    }

    cr_ref_count_dec(op->cr);
    plat_free(op);
}

/**
 * @brief Allocate a synthetic response
 *
 * When errors occur at the replication layer, synthesize a suitable response
 * suitable for the home node code as op->response_wrapper.  It is an error to
 * call #cr_op_syntheisze when the op has a response assigned or is not in
 * an error state.
 *
 * XXX: This should go away when we have generic response types
 */
static void
cr_op_synthesize_response(struct cr_op *op) {
    struct sdf_msg *resp_msg;
    SDF_protocol_msg_type_t msg_type;
    sdf_msg_wrapper_free_local_t local_free;
    SDF_size_t msize;

    local_free =
        sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                          &cr_msg_free, NULL);

    plat_assert(!op->response_wrapper);
    plat_assert(op->response_status != SDF_SUCCESS);

    msg_type = home_flash_response_type(op->request_pm->msgtype, op->status);

    resp_msg =
        home_load_msg(op->request_pm->node_to /* node from */,
                      op->request_pm->node_from /* node to */,
                      op->request_pm /* original message */, msg_type,
                      NULL /* data */, 0 /* data size */,
                      0 /* exptime */, 0 /* createtime */,
                      SDF_SEQUENCE_NO_INVALID,
                      op->status, &msize,
                      NULL /* key */, 0 /* key len */, 0 /* flags */);
    plat_assert(resp_msg);
    resp_msg->msg_len = sizeof (*resp_msg) + msize;

    op->response_wrapper =
        sdf_msg_wrapper_local_alloc(resp_msg, sdf_msg_wrapper_free_local_null,
                                    SMW_MUTABLE_FIRST, SMW_TYPE_RESPONSE,
                                    op->request_wrapper->dest_vnode /* src */,
                                    op->request_wrapper->dest_service /* src */,
                                    op->request_wrapper->src_vnode /* dest */,
                                    op->request_wrapper->src_service /* dest */,
                                    /* XXX the message type is silly */
                                    FLSH_RESPOND,
                                    &op->request_wrapper->response_mbx);

    plat_assert(op->response_wrapper);

}

/**
 * @brief Send flash request to multiple flash nodes
 *
 * All of the nodes meeting the operation's requiremens (all for
 * CR_FANOUT_ALL_OR_NOTHING; just the live nodes for CR_NODE_LIVE)
 *
 * @param op <IN> operation.  Reference count shall be greater than zero
 * @param context <IN> scheduler context.  Used for closure chaining
 * optimization.
 * @param response_closure <IN> closure invoked on response
 * @return SDF_SUCCESS when asynchronous request was started,
 */
static SDF_status_t
cr_op_flash_multi_node(struct cr_op *op, struct plat_closure_scheduler *context,
                       sdf_msg_recv_wrapper_t response_closure) {
    struct sdf_replication_op_meta *op_meta =  &op->request_pm->op_meta;
    uint32_t nreplica = op_meta->shard_meta.nreplica;
    SDF_status_t status = SDF_SUCCESS;
    uint32_t i;
    vnode_t pnode;
    struct cr_node *node;

    plat_assert(op->fanout == CR_FANOUT_ALL_LIVE ||
                op->fanout == CR_FANOUT_ALL_OR_NOTHING);

    plat_log_msg(21415, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "copy_replicator %p op %p flash_multi", op->cr, op);

    (void) __sync_add_and_fetch(&op->ref_count, 1);

    for (i = 0; status == SDF_SUCCESS && i < nreplica; ++i) {
        pnode = op_meta->shard_meta.pnodes[i];
        node = cr_get_node(op->cr, pnode);
        plat_assert(node);

        if (op->fanout == CR_FANOUT_ALL_OR_NOTHING ||
            node->state == CR_NODE_LIVE) {
            status = cr_op_flash_forward(op, context, pnode, response_closure);
        }
    }

    if (status != SDF_SUCCESS) {
        cr_op_accum_status(op, status);
    }

    cr_op_ref_count_dec(op);

    return (status);
}

/**
 * @brief State for a single flash request (> 1 per replicated write)
 *
 * XXX: drew 2008-06-11 This is a kludge around the current sdf_fth_mbx
 * which needs to exist until the message response has been received
 */
struct cr_op_flash_request_state {
    /** @brief op for reference counting */
    struct cr_op *op;

    /** @brief Outbound message type (may be different than in op) */
    SDF_protocol_msg_type_t msgtype;

    /** @brief Destination node for this request */
    vnode_t dest_node;

    /** @brief closure appleid on completion */
    sdf_msg_recv_wrapper_t response_closure;

    /** @brief dynamically allocated mbx structure */
    struct sdf_fth_mbx *mbx;
};

static void crofr_response(struct plat_closure_scheduler *context, void *env,
                           struct sdf_msg_wrapper *response);
static void crofr_free(struct cr_op_flash_request_state *request_state);

/**
 * @brief Forward flash operation
 *
 * The inbound messages sent to the replicator are home-flash messages
 * meaning the normal operational case can be a simple pass-through which
 * is accomplised by this function.
 *
 * The caller must accumulate status since it may be appropriate
 * for an intermediate request to fail.
 *
 * @param op <IN> operation.  Reference count shall be greater than zero
 * @param context <IN> scheduler context.  Used for closure chaining
 * optimization.
 * @param dest_node <IN> node to which the operation's message shall
 * be forwarded
 * @param response_closure <IN> closure invoked on response
 * @return SDF_SUCCESS when asynchronous request was started,
 */
static SDF_status_t
cr_op_flash_forward(struct cr_op *op, struct plat_closure_scheduler
                    *context, vnode_t dest_node,
                    sdf_msg_recv_wrapper_t response_closure) {
    struct copy_replicator *cr = op->cr;
    struct sdf_msg_wrapper *send_msg_wrapper;
    SDF_status_t status;

    send_msg_wrapper =
            sdf_msg_wrapper_copy(op->request_wrapper,
                                 SMW_TYPE_REQUEST,
                                 cr->config.my_node /* src */,
#if 1
                                 cr->config.response_service /* src */,
#else /* FIXME do we still need this below */
                                 cr->config.replication_service,
#endif
                                 dest_node /* dest */,
                                 cr->config.flash_service /* dest */,
                                 op->request_pm->msgtype,
                                 NULL /* mresp_mbx */);

    if (!send_msg_wrapper) {
        status = SDF_FAILURE_MEMORY_ALLOC;
        plat_log_msg(21416, LOG_CAT, PLAT_LOG_LEVEL_WARN,
                     "copy_replicator %p op %p flash request to node %lu"
                     " failed: %s",
                     op->cr, op, (unsigned long)dest_node,
                     sdf_status_to_string(status));
    } else {
        status = cr_op_flash_request(op, context, send_msg_wrapper, dest_node,
                                     response_closure);
    }

    return (status);
}

/**
 * @brief Make flash request.
 *
 * Make a flash request which may or may not be associated with the
 * original operation.
 *
 * The caller must accumulate status since it may be appropriate
 * for an intermediate request to fail.
 *
 * @param op <IN> operation.  Reference count shall be greater than zero
 * @param context <IN> scheduler context.  Used for closure chaining
 * optimization.
 * @param send_msg_wrapper <IN> Message being sent.  One reference count
 * is consumed regardless of success.  Must be non-null.
 * @param dest_node <IN> node to which the operation's message shall
 * be forwarded
 * @param response_closure <IN> closure invoked on response
 * @return SDF_SUCCESS when asynchronous request was started,
 */
static SDF_status_t
cr_op_flash_request(struct cr_op *op, struct plat_closure_scheduler *context,
                    struct sdf_msg_wrapper *send_msg_wrapper, vnode_t dest_node,
                    sdf_msg_recv_wrapper_t response_closure) {
    struct copy_replicator *cr = op->cr;
    struct cr_op_flash_request_state *request_state;
    SDF_status_t status = SDF_SUCCESS;
    struct SDF_protocol_msg *pm;
    struct sdf_msg *msg;

    plat_assert(send_msg_wrapper);

    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, send_msg_wrapper);
    pm = (struct SDF_protocol_msg *)(msg + 1);

    plat_log_msg(21417, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "copy_replicator %p op %p flash request %s to node %lu start",
                 op->cr, op, SDF_Protocol_Msg_Info[pm->msgtype].shortname,
                 (unsigned long)dest_node);

    (void) __sync_add_and_fetch(&op->ref_count, 1);

    if (plat_calloc_struct(&request_state)) {

        request_state->op = op;
        request_state->msgtype = pm->msgtype;
        request_state->dest_node = dest_node;
        request_state->response_closure = response_closure;
        request_state->mbx =
            sdf_fth_mbx_resp_closure_alloc(sdf_msg_recv_wrapper_create(cr->closure_scheduler,
                                                                       &crofr_response,
                                                                       request_state),
                                           /* XXX: release arg goes away */
                                           SACK_REL_YES,
                                           cr->config.timeout_usecs);
        if (!request_state->mbx) {
            status = SDF_FAILURE_MEMORY_ALLOC;
        }

    } else {
        status = SDF_FAILURE_MEMORY_ALLOC;
    }

    sdf_msg_wrapper_rwrelease(&msg, send_msg_wrapper);

    if (status != SDF_SUCCESS) {
        sdf_msg_wrapper_ref_count_dec(send_msg_wrapper);
        send_msg_wrapper = NULL;
    }

    if (status == SDF_SUCCESS) {
        plat_closure_apply(sdf_replicator_send_msg_cb, &cr->callbacks.send_msg,
                           send_msg_wrapper, request_state->mbx, &status);
    }

    if (status != SDF_SUCCESS) {
        plat_log_msg(21416, LOG_CAT, PLAT_LOG_LEVEL_WARN,
                     "copy_replicator %p op %p flash request to node %lu"
                     " failed: %s",
                     op->cr, op, (unsigned long)dest_node,
                     sdf_status_to_string(status));

        if (request_state) {
            crofr_free(request_state);
        }

        cr_op_ref_count_dec(op);
    }

    return (status);
}

/** @brief Process flash request response */
static void
crofr_response(struct plat_closure_scheduler *context, void *env,
               struct sdf_msg_wrapper *response) {
    struct cr_op_flash_request_state *request_state =
        (struct cr_op_flash_request_state *)env;
    struct cr_op *op = request_state->op;

    struct sdf_msg *msg;
    SDF_protocol_msg_t *pm;
    SDF_Protocol_Msg_Info_t *pmi;
    SDF_status_t msg_status;
    struct sdf_msg_error_payload *error_payload;


    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, response);

    if (msg->msg_type == SDF_MSG_ERROR) {
        error_payload = (struct sdf_msg_error_payload *)msg->msg_payload;
        plat_log_msg(21418, LOG_CAT, PLAT_LOG_LEVEL_WARN,
                     "copy_replicator %p op %p flash request %s node %d"
                     " SDF_MSG_ERROR received status %s",
                     op->cr, op,
                     SDF_Protocol_Msg_Info[request_state->msgtype].shortname,
                     (int)request_state->dest_node,
                     sdf_status_to_string(error_payload->error));
    } else {
        pm = (SDF_protocol_msg_t *)msg->msg_payload;
        pmi = &(SDF_Protocol_Msg_Info[pm->msgtype]);
        msg_status = pm->status;

        plat_log_msg(21419,
                     LOG_CAT, msg_status == SDF_SUCCESS ? PLAT_LOG_LEVEL_TRACE :
                     (msg_status == SDF_OBJECT_UNKNOWN ?
                      PLAT_LOG_LEVEL_DIAGNOSTIC : PLAT_LOG_LEVEL_INFO),
                     "copy_replicator %p op %p flash request %s node %d"
                     " response received"
                     " Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d status %s",
                     op->cr, op,
                     SDF_Protocol_Msg_Info[request_state->msgtype].shortname,
                     (int)request_state->dest_node,
                     pmi->name, pmi->shortname,
                     pm->node_from, SDF_Protocol_Nodes_Info[pmi->src].name,
                     pm->node_to, SDF_Protocol_Nodes_Info[pmi->dest].name,
                     pm->tag, sdf_status_to_string(msg_status));

// xxxzzz remove this!
#if 0
            plat_log_msg(21420, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                         "===============> RESPONSE node_id %"PRIu32" response=%s, mkey=%s",
                         pm->node_from,
                         SDF_Protocol_Msg_Info[pm->msgtype].shortname,
                         msg->mkey);
#endif
    }


    plat_closure_chain(sdf_msg_recv_wrapper, context,
                       &request_state->response_closure, response);

    cr_op_ref_count_dec(op);

    crofr_free(request_state);
}

/** @brief Free flash request state */
static void
crofr_free(struct cr_op_flash_request_state *request_state) {
    if (request_state) {
        if (request_state->mbx) {
            sdf_fth_mbx_free(request_state->mbx);
        }
        plat_free(request_state);
    }
}

/*
 * XXX: Drew 2008-11-18 Container creation needs to become a two-phase
 * operation with rollback on failure so we don't leak space in test
 * environments with failure injection or if we get into some sort of
 * spiral of death on customer systems.
 */

#define CROCS_STAGE_ITEMS() \
    /** @brief Creating data and optional meta-data shards */ \
    item(CREATE_SHARDS, "create_shards") \
    /** @brief Writing initial meta-data */ \
    item(WRITE_META, "write_meta") \
    /** @brief Operation done */ \
    item(DONE, "done")

enum cr_op_create_shard_stage {
#define item(caps, text) CR_CREATE_SHARD_ ## caps,
    CROCS_STAGE_ITEMS()
#undef item
};

/*
 * Shard creation itself is multi-stage, comprised of the actual shard
 * creation and writing the initial meta-data.  A reference count is
 * held from when the operation starts to when it completes.
 */
struct cr_op_create_shard_request_state {
    /** @brief  op for reference counting, logging */
    struct cr_op *op;

    /** @brief Meta-data for shard being created */
    struct SDF_shard_meta  shard_meta;

    /** @brief stage we're currently in */
    enum cr_op_create_shard_stage stage;

    /** @brief number of sub-operations which remain in this stage */
    int stage_op_remain;

    /** @brief Data pnodes */
    vnode_t data_pnodes[SDF_REPLICATION_MAX_REPLICAS];

    /** @brief Number of replicas */
    uint32_t nreplica;

    /** @brief Number of meta-data replicas */
    uint32_t nmeta_replica;

    /** @brief Handler for all flash operation completion */
    sdf_msg_recv_wrapper_t response_closure;
};


static SDF_status_t
crocs_create_shards(struct cr_op_create_shard_request_state *request_state,
                    struct plat_closure_scheduler *context);
static SDF_status_t
crocs_write_meta(struct cr_op_create_shard_request_state *request_state,
                 struct plat_closure_scheduler *context);
static SDF_status_t
crocs_write_meta_one(struct cr_op_create_shard_request_state *request_state,
                     struct plat_closure_scheduler *context, int group_id);
static void crocs_write_meta_cb(struct plat_closure_scheduler *context,
                                void *env, SDF_status_t status);

static void
crocs_flash_response(struct plat_closure_scheduler *context, void *env,
                     struct sdf_msg_wrapper *response);
static void
crocs_ref_count_dec(struct cr_op_create_shard_request_state *request_state,
                    struct plat_closure_scheduler *context);
static void
crocs_ref_count_zero(struct cr_op_create_shard_request_state *request_state,
                     struct plat_closure_scheduler *context);

static const char *crocs_stage_to_string(enum cr_op_create_shard_stage stage);

/** @brief Create all replicas of a shard include meta-data */

/*
 * FIXME: drew 2009-09-11 Detect the initial create for V1 multi-shard support,
 * and immediately have all additional shard creations return success after
 * the shard has been added to shard->local_vip_meta.
 */
static SDF_status_t
cr_op_create_shard(struct cr_op *op, struct plat_closure_scheduler *context) {
    SDF_status_t status = SDF_SUCCESS;
    struct SDF_shard_meta  *shard_meta;
    struct cr_op_create_shard_request_state *request_state;
    SDF_replication_t replication_type;

    plat_assert(op->request_pm->msgtype == HFCSH);

    /* XXX should spew instead of asserting */
    plat_assert(op->request_pm->data_size == sizeof(struct SDF_shard_meta));
    shard_meta = (struct SDF_shard_meta *)((char *)(op->request_pm + 1) +
                                           op->request_pm->data_offset);
    plat_assert(op->request_pm->shard == shard_meta->sguid);
    replication_type = shard_meta->replication_props.type;
    plat_assert(!(cr_replication_type_flags(replication_type) &
                  CRTF_MANAGE_NONE));

    if (!plat_calloc_struct(&request_state)) {
        status = SDF_FAILURE_MEMORY_ALLOC;
    } else {
        request_state->op = op;
        request_state->shard_meta = *shard_meta;
        request_state->stage = CR_CREATE_SHARD_CREATE_SHARDS;

        /* Start at one reference count */
        request_state->stage_op_remain = 1;

        cr_get_pnodes(&op->cr->config,
                      &request_state->shard_meta.replication_props,
                      request_state->shard_meta.first_node,
                      request_state->data_pnodes,
                      &request_state->nreplica,
                      &request_state->nmeta_replica);

        request_state->response_closure =
            sdf_msg_recv_wrapper_create(op->cr->closure_scheduler,
                                        &crocs_flash_response, request_state);

        /* Keep a reference count on the op until we're done at this level */
        (void) __sync_add_and_fetch(&op->ref_count, 1);

        plat_log_msg(21421, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "copy_replicator %p op %p create shard nreplica = %d",
                     op->cr, op, request_state->nreplica);
    }

    cr_op_accum_status(op, status);

    /*
     * XXX: drew 2008-11-12 To be more resilient to errors we should be doing
     * container creation as a multi-phase operation which starts with the
     * meta-data update.
     *
     * This may also need to change so that we can get reasonable test
     * coverage with a reasonable pseudo-random workload without running out
     * of space in our simulated flash, because a failure between the shard
     * create and meta create will leak the shard space.
     */
    if (status == SDF_SUCCESS) {
        status = crocs_create_shards(request_state, context);
        cr_op_accum_status(op, status);
    }

    if (request_state) {
        crocs_ref_count_dec(request_state, context);
    }

    return (status);
}

/** @brief Start shard creation */
static SDF_status_t
crocs_create_shards(struct cr_op_create_shard_request_state *request_state,
                    struct plat_closure_scheduler *context) {
    uint32_t i;
    SDF_status_t status = SDF_SUCCESS;
    SDF_replication_t replication_type =
        request_state->shard_meta.replication_props.type;

    plat_assert(request_state);
    plat_assert(request_state->op->status == SDF_SUCCESS);

    if (cr_replication_type_flags(replication_type) & CRTF_MANAGE_DATA) {
        for (i = 0; i < request_state->nreplica; ++i) {
            (void) __sync_add_and_fetch(&request_state->stage_op_remain, 1);
            status = cr_op_flash_forward(request_state->op, context,
                                         request_state->data_pnodes[i],
                                         request_state->response_closure);
            /* XXX: Eliminate when sub-functions always apply closures */
            if (status != SDF_SUCCESS) {
                crocs_ref_count_dec(request_state, context);
            }
        }
    }

    cr_op_accum_status(request_state->op, status);

    return (status);
}

/** @brief #cr_op_create_shard meta-data writing step */
static SDF_status_t
crocs_write_meta(struct cr_op_create_shard_request_state *request_state,
                 struct plat_closure_scheduler *context) {
    int flags;
    struct sdf_vip_group_group *inter_group_group;
    unsigned i;
    struct sdf_vip_group *intra_group;
    SDF_status_t status;

    /* So reference count can edge trigger on zero even on failure */
    (void) __sync_add_and_fetch(&request_state->stage_op_remain, 1);

    /*
     * As a side effect, this ignores an uninitialized
     * shard_meta->inter_node_vip_group_group_id
     */
    flags = cr_replication_type_flags(request_state->shard_meta.replication_props.type);
    if (flags & CRTF_INDEX_SHARDID) {
        status = crocs_write_meta_one(request_state, context,
                                      VIP_GROUP_ID_INVALID);
    } else {
        inter_group_group =
            sdf_vip_config_get_group_group_by_ggid(request_state->op->cr->config.vip_config,
                                                   request_state->shard_meta.inter_node_vip_group_group_id);
        plat_assert(inter_group_group);
        for (status = SDF_SUCCESS, i = 0;
             status == SDF_SUCCESS && i < inter_group_group->num_groups;
             ++i) {
            intra_group = &inter_group_group->groups[i];
            status = crocs_write_meta_one(request_state, context,
                                          intra_group->vip_group_id);
        }
    }

    /** To compensate for the first one */
    crocs_ref_count_dec(request_state, context);

    return (status);
}

/**
 * @brief Write the meta-data for a single shard, intra-node vip group tuple
 *
 * @param group_id <IN> #VIP_GROUP_ID_INVALID where the #copy_replicator
 * is being used for replication not V1 VIP migration, valid VIP group
 * id otherwise.  A shard sguid may be used for either application but
 * not both.  In the VIP case multiple groups are associated with
 * the same shard sguid.
 */
static SDF_status_t
crocs_write_meta_one(struct cr_op_create_shard_request_state *request_state,
                     struct plat_closure_scheduler *context, int group_id) {
    int flags;
    struct cr_op *op = request_state->op;
    struct cr_shard *shard;
    struct cr_shard_meta *shard_meta;
    struct copy_replicator *cr = op->cr;
    SDF_status_t status;
    cr_shard_put_meta_cb_t cb;
    struct sdf_vip_group_group *inter_group_group;

    /* So reference count can edge trigger on zero even on failure */
    (void) __sync_add_and_fetch(&request_state->stage_op_remain, 1);

    flags = cr_replication_type_flags(request_state->shard_meta.replication_props.type);

    shard = cr_get_or_alloc_shard(cr, op->request_pm->shard, group_id,
                                  request_state->shard_meta.replication_props.type);

    /*
     * FIXME: drew 2009-05-28 We need locking once we get away from the
     * single scheduler handling data and meta-data paths so that the
     * shard meta-data doesn't change underneath us.
     */
    plat_assert(shard);

    plat_assert_imply(shard->shard_meta, flags & CRTF_INDEX_VIP_GROUP_ID);
    plat_assert_imply(shard->proposed_shard_meta,
                      flags & CRTF_INDEX_VIP_GROUP_ID);

    /*
     * We always go through the creation process so the state machine
     * gets manipulated as necessary.
     */
    status = cr_shard_meta_create(&shard_meta,
                                  &cr->config, &request_state->shard_meta);
    if (status == SDF_SUCCESS) {
        shard_meta->persistent.cguid = op->request_pm->cguid;
        shard_meta->persistent.intra_node_vip_group_id = group_id;
        if (group_id == VIP_GROUP_ID_INVALID) {
            shard_meta->persistent.inter_node_vip_group_group_id =
                VIP_GROUP_GROUP_ID_INVALID;
        } else {
            inter_group_group =
                sdf_vip_config_get_group_group_by_gid(shard->cr->config.vip_config,
                                                      group_id);
            plat_assert(inter_group_group);
            shard_meta->persistent.inter_node_vip_group_group_id =
                inter_group_group->group_group_id;
        }
        plat_assert(sizeof (shard_meta->persistent.cname) ==
                    sizeof (op->request_pm->cname));
        memcpy(&shard_meta->persistent.cname, op->request_pm->cname,
               sizeof (op->request_pm->cname));

        (void) __sync_add_and_fetch(&request_state->stage_op_remain, 1);
        /*
         * XXX: drew 2009-05-31 With the crocs operation maintaining a
         * reference do we need to do this?
         */
        (void) __sync_add_and_fetch(&op->ref_count, 1);

        cb = cr_shard_put_meta_cb_create(cr->callbacks.single_scheduler,
                                         &crocs_write_meta_cb,
                                         request_state);

        cr_shard_create(shard, shard_meta, cb);
    }

    /** To compensate for the first one */
    crocs_ref_count_dec(request_state, context);

    return (status);
}

/** @brief #cr_op_create_shard meta-data write complete */
static void
crocs_write_meta_cb(struct plat_closure_scheduler *context, void *env,
                    SDF_status_t status) {
    struct cr_op_create_shard_request_state *request_state =
        (struct cr_op_create_shard_request_state *)env;

    plat_assert(request_state);

    cr_op_accum_status(request_state->op, status);

    cr_op_ref_count_dec(request_state->op);
    /* request state may be free */
    crocs_ref_count_dec(request_state, context);
}

/** @brief #cr_op_create_shard flash response received */
static void
crocs_flash_response(struct plat_closure_scheduler *context, void *env,
                     struct sdf_msg_wrapper *response) {
    struct cr_op_create_shard_request_state *request_state =
        (struct cr_op_create_shard_request_state *)env;
    struct cr_op *op = request_state->op;

    /*
     * This works until we have flash operations that do not affect
     * the accumulated status.
     */
    cr_op_flash_response(context, op, response);

    crocs_ref_count_dec(request_state, context);
}

/** @brief #cr_op_create_shard decrement request state ref count */
static void
crocs_ref_count_dec(struct cr_op_create_shard_request_state *request_state,
                    struct plat_closure_scheduler *context) {
    int after;

    after = __sync_sub_and_fetch(&request_state->stage_op_remain, 1);
    plat_assert(after >= 0);
    if (!after) {
        crocs_ref_count_zero(request_state, context);
    }
}

/** @brief #cr_op_create_shard meta-data reference count 0, to next stage */
static void
crocs_ref_count_zero(struct cr_op_create_shard_request_state *request_state,
                     struct plat_closure_scheduler *context) {
    struct cr_op *op = request_state->op;
    struct copy_replicator *cr = op->cr;
    SDF_replication_t replication_type =
        request_state->shard_meta.replication_props.type;

    enum cr_op_create_shard_stage before;

    plat_assert(!request_state->stage_op_remain);
    plat_assert(request_state->stage < CR_CREATE_SHARD_DONE);

    before = request_state->stage;

    if (op->status != SDF_SUCCESS) {
        request_state->stage = CR_CREATE_SHARD_DONE;
    } else {
        ++request_state->stage;
    }

    if (request_state->stage == CR_CREATE_SHARD_WRITE_META &&
        !(cr_replication_type_flags(replication_type) & CRTF_MANAGE_META)) {
            ++request_state->stage;
    }

    plat_log_msg(21422, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "copy_replicator %p op %p create shard stage %s -> %s",
                 cr, op, crocs_stage_to_string(before),
                 crocs_stage_to_string(request_state->stage));

    switch (request_state->stage) {
    case CR_CREATE_SHARD_CREATE_SHARDS:
        plat_assert_always(0);
        break;
    case CR_CREATE_SHARD_WRITE_META:
        /* XXX: Might lead to tail recursion but that's OK */
        crocs_write_meta(request_state, context);
        break;
    case CR_CREATE_SHARD_DONE:
        /*
         * XXX: drew 2009-08-18 This should only happen where we're only
         * in the meta-data path and therefore not making any message
         * based requests.
         */
        if (op->status == SDF_SUCCESS && !op->response_wrapper) {
            cr_op_synthesize_response(op);
        }

        cr_op_ref_count_dec(request_state->op);
        plat_free(request_state);
        break;
    }
}

static const char *
crocs_stage_to_string(enum cr_op_create_shard_stage stage) {
    switch (stage) {
#define item(caps, text) \
    case CR_CREATE_SHARD_ ## caps: return (text);
    CROCS_STAGE_ITEMS()
    }
#undef item
    /* No default so compiler can warn & -Werror  */
    plat_assert(0);
    return (NULL);
}

#ifdef notyet
/*
 * FIXME: drew 2009-09-11 Need to remove shard id from containers associated
 * with vips and write meta-data as the set shrinks.
 */
static SDF_status_t
cr_op_delete_shard(struct cr_op *op, struct plat_closure_scheduler *context) {
    SDF_status_t status = SDF_SUCCESS;
    struct SDF_shard_meta  *shard_meta;
    struct cr_op_create_shard_request_state *request_state;
    SDF_replication_t replication_type;

    plat_assert(op->request_pm->msgtype == HFDSH);

    /*
     * XXX: drew 2009-08-10 Should use a multi-stage delete where
     * we mark the meta-data as being deleted.
     */
}
#endif

/**
 * @brief Return node structure for given pnode
 *
 * Currently, the SDF code assumes that node identifiers are consecutively
 * assigned.  This function allows for a little indirection so that can
 * change.
 *
 * @return pointer to node, NULL if the node is invalid
 */
struct cr_node *
cr_get_node(struct copy_replicator *cr, vnode_t pnode) {
    struct cr_node *ret;

    plat_assert(pnode >= 0);

    if (pnode < cr->config.node_count) {
        ret = &cr->nodes[pnode];
    } else {
        ret = NULL;
    }

    return (ret);
}

static void
cr_msg_free(plat_closure_scheduler_t *context, void *env, struct sdf_msg *msg) {
    sdf_msg_free_buff(msg);
}

static const char *
cr_fanout_to_string(enum cr_fanout fanout) {
    switch (fanout) {
#define item(caps, lower) \
    case CR_FANOUT_ ## caps: return (#lower);
    CR_FANOUT_ITEMS()
    }
#undef item

    plat_assert(0);

    return (NULL);
}

/** @brief Return whether only meta-data replication is performed */
static int
cr_replication_type_flags(SDF_replication_t replication_type) {
    switch (replication_type) {
#define item(caps, flags) case caps: return (flags);
    CR_REPLICATION_TYPE_ITEMS()
#undef item
    case SDF_REPLICATION_INVALID:
        plat_fatal("bad replication_type");
    }
    plat_fatal("bad replication_type");
    return (0);
}

/** @brief Assert when shard state is obviously incorrect */
static void
cr_replication_type_validate(SDF_replication_t replication_type) {
    int flags;

    flags = cr_replication_type_flags(replication_type);
    plat_assert_either(flags & CRTF_INDEX_NONE,
                       flags & CRTF_INDEX_SHARDID,
                       flags & CRTF_INDEX_VIP_GROUP_ID);

    plat_assert_either(flags & CRTF_META_NONE,
                       flags & CRTF_META_SUPER,
                       flags & CRTF_META_PAXOS,
                       flags & CRTF_META_DISTRIBUTED);

    plat_assert_either(flags & CRTF_MANAGE_NONE,
                       (flags & (CRTF_MANAGE_META|CRTF_MANAGE_DATA)));

    plat_assert_either(flags & CRTF_CREATE_LEASE_NONE,
                       flags & CRTF_CREATE_LEASE_IMMEDIATE,
                       flags & CRTF_CREATE_LEASE_DELAY);

}

/** @brief Assert when shard states are obviously incorrect */
static void
cr_replication_types_validate() {
#define item(caps, flags) cr_replication_type_validate(caps);
    CR_REPLICATION_TYPE_ITEMS()
#undef item
}
