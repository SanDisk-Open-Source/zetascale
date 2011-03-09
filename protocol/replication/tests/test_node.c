/*
 * File:   test_node.c
 * Author: Drew Eckhardt
 *
 * Created on Dec 31, 2008, 4:00 PM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_node.c 12844 2010-04-10 03:38:36Z drew $
 */

#include "sys/queue.h"

#include "platform/stdlib.h"
#include "platform/stdio.h"

#include "fth/fth.h"
#include "fth/fthMbox.h"

#include "sdfmsg/sdf_msg_action.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_msg_types.h"
#include "sdfmsg/sdf_msg_wrapper.h"

#include "utils/hashmap.h"

#include "protocol/home/home_util.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_common.h"
#include "protocol/home/home_flash.h"

#include "protocol/replication/copy_replicator.h"
#include "protocol/replication/copy_replicator_internal.h"
#include "protocol/replication/meta_storage.h"
#include "protocol/replication/meta_types.h"

#include "test_framework.h"
#include "test_flash.h"
#include "test_node.h"
#include "test_common.h"

#define MSG_KEYSZE 17

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test");

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_EVENT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/event");

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_MSG, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/msg");

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_LIVENESS, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/liveness");

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_MKEY, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "test/mkey");

enum {
    /** @brief Number of hash buckets */
    RTN_HASH_BUCKETS = 101,
};

#define RTN_LIVENESS_ITEMS() \
    item(RTNL_LIVE, live)                                                      \
    item(RTNL_DEAD, dead)

enum rtn_liveness {
#define item(caps, lower) caps,
    RTN_LIVENESS_ITEMS()
#undef item
};

/*
 * Concurrency control becomes especially problematic where threads,
 * synchronous callbacks, and non-reentrant (pthread recursive mutexes
 * reference count when called multiple times in the same thread;
 * fthLocks do not) locking are all combined.
 *
 * We avoid the problem by making all functions exposed to the
 * consumer (#replication_test_framework) and providers (#sdf_replicator
 * as implemeted by #copy_replicator and the various
 * #test_meta implementations) proxy through thin wrappers which
 * schedule the actual work for dispatching in a single #fthThread
 * executing the plat_mbox_scheduler test_node->closure_scheduler
 *
 * The net effect is lock-free operation with a few lines of glue per API
 *
 * void
 * rtn_crash_async(struct replication_test_node *test_node,
 *                 replication_test_node_crash_async_cb_t cb)  {
 *     rtn_do_crash_t do = rtn_do_crash_create(test_node->closure_scheduler,
 *                                             rtn_crash_impl, cb);
 *     plat_closure_apply(rtn_do_start, &do);
 *     rtn_guarantee_bootstrapped(test_node);
 * }
 *
 * followed by the actual implementation function
 *
 * These are the wrapper type definitions
 */

/**
 * @brief Internal closure type implementing #rtn_node_live, #rtn_node_dead
 *
 * @param src_node <IN> Originating "live" node
 *
 * @param epoch <IN> Monotonically increasing with each node
 * start.
 *
 * #rtn_node_live uses the src_node epoch
 * #rtn_node_live uses the local node's epoch
 */
PLAT_CLOSURE2(rtn_do_node_liveness,
              vnode_t, src_node,
              int64_t, epoch);

/** @brief Internal closure type implementing #rtn_shutdown_async */
PLAT_CLOSURE1(rtn_do_shutdown, replication_test_node_shutdown_async_cb_t,
              user_cb);

/** @brief Internal closure type implementing #rtn_start */
PLAT_CLOSURE1(rtn_do_start, replication_test_framework_cb_t, user_cb);

/** @brief Internal closure type implementing #rtn_crash_async */
PLAT_CLOSURE1(rtn_do_crash, replication_test_node_crash_async_cb_t, user_cb);

/** @brief Internal closure for implementing #rtn_command_async */
PLAT_CLOSURE3(rtn_do_command,
              SDF_shardid_t, shard,
              char *, command,
              sdf_replicator_command_cb_t, cb);



/** @brief Internal closure type implementing #rtn_receive_msg */
PLAT_CLOSURE1(rtn_do_receive_msg, struct sdf_msg_wrapper *, msg_wrapper);

/**
 * @brief Internal closure type implementing #rtn_send_msg
 *
 * The #rtn_send_msg function always returns SDF_SUCCESS because
 * the asynchrony precludes setting it to anything else, and we always
 * want to handle failures local or remote the same way using messaging
 * layer generated message types.
 */
PLAT_CLOSURE2(rtn_do_send_msg, struct sdf_msg_wrapper *, msg_wrapper,
              struct sdf_fth_mbx *, ar_mbx);

/**
 * @brief Internal closure type implementing #rtn_get_shard_meta
 *
 * @param sguid <IN> data shard GUID
 *
 * @param shard_meta_arg <IN> Callee assumes ownership because
 * objects are copied to preserve the wrapped API's ownership
 * semantics inspite of the asynchronous start.
 */
PLAT_CLOSURE3(rtn_do_get_shard_meta,
              SDF_shardid_t, sguid,
              const struct sdf_replication_shard_meta *, shard_meta_arg,
              rtfw_shard_meta_cb_t, user_cb);

/**
 * @brief Internal closure type implementing functions which take rtfw_void_cb_t
 *
 * @param user_cb <IN> applied with operation status on completion
 */
PLAT_CLOSURE1(rtn_do_void_cb,
              rtfw_void_cb_t, user_cb);

/** @brief For mode argument to #rtn_put_shard_meta_impl */
enum rtn_put_shard_meta_mode {
    /* @brief Create */
    RTN_PSM_CREATE,
    /* @brief Put */
    RTN_PSM_PUT
};

/*
 * @brief Internal closure type implementing #rtn_put_shard_meta
 *
 * @param cr_shard_meta <IN> Callee assumes ownership because
 * objects are copied to preserve the wrapped API's ownership
 * semantics inspite of the asynchronous start.
 */
PLAT_CLOSURE3(rtn_do_put_shard_meta,
              const struct cr_shard_meta *, cr_shard_meta,
              rtfw_shard_meta_cb_t, user_cb,
              enum rtn_put_shard_meta_mode, mode);

/**
 * @brief Internal closure type implementing #rtn_delete_shard_meta
 *
 * @param cr_shard_meta <IN> Callee assumes ownership because
 * objects are copied to preserve the wrapped API's ownership
 * semantics inspite of the asynchronous start.
 */
PLAT_CLOSURE2(rtn_do_delete_shard_meta,
              const struct cr_shard_meta *, cr_shard_meta,
              rtfw_shard_meta_cb_t, user_cb);

/** @brief Mode for #rtn_liveness_ping_reset mode argument */
enum rtn_liveness_ping_reset_mode {
    /** @brief Initial reset, implementing start delay */
    RTN_LPRM_INITIAL,
    /** @Brief Normal operation reset */
    RTN_LPRM_RECURRING
};

/** @brief Current state */

#define RTN_NODE_STATE_ITEMS() \
    /**                                                                        \
     * @brief Initial state (only on construction)                             \
     *                                                                         \
     * Prior to the replicator's initial start, the mbox dispatcher            \
     * is not running.                                                         \
     */                                                                        \
    item(RTN_STATE_INITIAL, initial)                                           \
    /** @brief Up (legal from RTN_STATE_DEAD) */                               \
    item(RTN_STATE_LIVE, live)                                                 \
    /** @brief Transitioning to dead (legal from RTN_STATE_LIVE) */            \
    item(RTN_STATE_TO_DEAD, to_dead)                                           \
    /** @brief Down (legal from RTN_STATE_TO_DEAD and RTN_STATE_INITIAL) */    \
    item(RTN_STATE_DEAD, dead)                                                 \
    /**                                                                        \
     * @brief Transitioning to RTN_STATE_SHUTDOWN                              \
     * legal from RTN_STATE_LIVE, RTN_STATE_TO_DEAD, and RTN_STATE_DEAD        \
     */                                                                        \
    item(RTN_STATE_TO_SHUTDOWN, to_shutdown)                                   \
    /** @brief Shut down (legal from RTN_STATE_TO_SHUTDOWN) */                 \
    item(RTN_STATE_SHUTDOWN, shutdown)

enum rtn_state {
#define item(caps, lower) caps,
    RTN_NODE_STATE_ITEMS()
#undef item
};

enum rtn_network_state {
    /* @brief Up */
    RTN_NET_UP,
    /* @brief Down */
    RTN_NET_DOWN
};

TAILQ_HEAD(rtn_msg_queue, rtn_msg_entry);

struct replication_test_node {
    /* Static configuration */

    /** @brief replication test config */
    struct replication_test_config test_config;

    /** @brief associated test framework */
    struct replication_test_framework *framework;

    /** @brief node_id */
    vnode_t node_id;

    /** @brief Interface for subsystems */
    replication_test_api_t component_api;

    /* Dynamic state */

    /* Liveness */

    /** @brief Current node state */
    enum rtn_state state;

    /** @brief Liveness epoch, monotonically increases with restarts */
    int64_t epoch;

    /**
     * @brief closure_scheduler
     *
     * In order to serialize message queue time delay expiration,
     * timeout, sending, and delivery functions everything is run
     * through a single plat_mbox_scheduler.
     */
    struct plat_closure_scheduler *closure_scheduler;

    /** @brief #plat_closure_scheduler_sthudown already called */
    unsigned closure_scheduler_shutdown_started : 1;

    /**
     * @brief number of events in flight.
     *
     * When this reaches 0 in RTN_STATE_TO_SHUTDOWN or RTN_STATE_TO_DEAD
     * the state becomes RTN_STATE_SHUTDOWN or RTN_STATE_DEAD respectively.
     */
    int ref_count;

    /**
     * @brief Completion for the executing crash
     *
     * From the call to #rtn_crash_async.
     *
     * All subsequently queued crashes apply (schedule) their
     * callback with an appropriate failure status.
     */
    replication_test_node_crash_async_cb_t crash_cb;

    /**
     * @brief Completion for the executing shutdown
     *
     * From the call to #rtn_shutdown_async
     */
    replication_test_node_shutdown_async_cb_t shutdown_cb;

    /* @brief Per-node state (liveness, outbound queues) indexed by node */
    struct rtn_per_node **per_node;


    /* Network simulation */

    /** @brief Current network state */
    enum rtn_network_state net_state;

    /**
     * @brief Event which delivers next liveness ping
     *
     * The time delay includes both the ping interval and
     * messaging delay.
     *
     * XXX: drew 2010-04-06 We want to split that so that the
     * individual nodes receive messages at different times to
     * cover interesting race conditions once we have more than
     * two nodes (ex, ship N+M service availability).
     *
     * Simplest to prevent drift is probably this event
     * scheduling the actual message sends in the per_node
     * basis.
     */
    struct plat_event *liveness_ping_event;

    /** @brief Name of replication_test_node.liveness_ping_event */
    char *liveness_ping_event_name;

    /** @brief Monotonically increasing outbound request number */
    uint64_t msg_seqno;

    /** @brief Outbound messages */

    /*
     * FIXME: Make this an array indexed by destination node so we're
     * more likely to have out-of-order starts and completions on
     * simultaenouos crashes, and so it's easier to special case
     * the loop back interface (this node just never uses the
     * next_delivery_event.
     *
     * This isn't half a screen of work.
     */
    struct {
        /** @brief Total size of per_node.msg_outbound.queue_size */
        uint32_t total_queue_size;

        /**
         * @brief Next destination node to deliver to
         *
         * Nodes are iterated in round-robin fashion.  Nodes with no output
         * pending are skipped.
         *
         * XXX: drew 2010-04-02 Doing some sort of more random ordering
         * which still preserves fairness would work better.  Simplest would
         * be to put N copies of each node id in an array and shuffle.
         */
        int next_delivery_node;

        /** @brief Name of next_delivery_event */
        char *next_delivery_event_name;

        /** @brief Event delivering the next message from queue */
        struct plat_event *next_delivery_event;
    } msg_outbound;

    struct {
        /** @brief List of all pending requests. */
        struct rtn_msg_queue queue;

        /** @brief Map of response mkeys to mailboxes. */
        HashMap map;
    } msg_requests;

    /*  Liveness simulation  */

    /**
     * @brief Array indexed by node offset indicating which are live
     *
     * XXX: drew 2010-03-30 This needs to change to allow delayed
     * liveness events.
     *
     * The simplest way to achieve this is to add
     * 1.  An ltime to guarantee messages are only delivered in the
     *     correct epoch.
     *
     * 2.  An event for when the next liveness change will be registered
     *
     * 3.  The next liveness change
     *
     * Where multiple liveness changes are queued, the previous
     * one is immediately delivered and the timer reset.
     *
     * This merges into a per_node structure, which also includes
     * the per-node queues necessary to implement network partitions
     * for testing.
     */
    enum rtn_liveness liveness;

    /* Parts under test */

    /* For test type  RT_TYPE_REPLICATOR but not RT_TYPE_META_STORAGE */

    /**@brief Replicator, owned by this */
    struct sdf_replicator *replicator;

    /**
     * @brief Handle on notifier
     *
     * A notification exists between when the replicator is created and
     * when when it is shutdown which in turn is blocked on all notifications
     * being free.
     */
    struct cr_notifier *replicator_notifier;

    /** @brief user-provided replica notification cb */
    rtfw_replicator_notification_cb_t replicator_notification_user_cb;

    /* Used for test_type RT_TYPE_META_STORAGE but not RT_TYPE_REPLICATOR */

    /** @brief replicator_meta_storage implementation, owned by this */
    struct replicator_meta_storage *meta_storage;

    /** @brief user-provided update callback */
    rtfw_shard_meta_cb_t meta_storage_user_cb;

    /* Simulated subsystems */

    /**
     * @brief Lock that guarantees the replicator is still here
     *
     * This only locks #state thus preventing transitions.  It allows
     * a multi-scheduler test environment to safely call
     * functions like #rtn_get_op_meta without the replicator being deleted
     * during the call
     */
    fthLock_t replicator_op_lock;

    /** @brief Flash header */
    struct replication_test_flash *flash;

    /** @brief rtf_shutdown_async has been called */
    unsigned flash_shutdown_started : 1;
};

struct rtn_per_node {
    /** @brief This node */
    struct replication_test_node *local_node;

    /** @brief Remote node id */
    vnode_t remote_node;

    /** @brief Network state for outgoing traffic to remote_node */
    enum rtn_network_state net_state;

    /** @brief rtn_per_node_connection_close needed on network up */
    unsigned connection_close_needed : 1;

    struct {
        /** @brief Current liveness state */
        enum rtn_liveness state;

        /** @brief Event which delivers next liveness transition */
        struct plat_event *event;

        /** @brief Name of rtn_remote_node.state.liveness.event */
        char *event_name;

        /**
         * @brief nth time the local node believed the remote node was up
         *
         * Messaging guarantees that messages will be delivered in their
         * epoch.
         */
        int64_t epoch;
    } liveness;

    struct {
        /** @brief Queue messages */
        struct rtn_msg_queue queue;

        /** @brief queue size */
        uint32_t queue_size;
    } msg_outbound;
};

/** @brief How the rtn_msg_entry is currently referenced */
enum rtn_msg_entry_ref {
    /**
     * @brief Referenced by #replication_test_node msg_outbound
     * (.queue and .count)
     */
    RTNME_REF_MSG_OUTBOUND = 1 << 0,

    /**
     * @brief Referenced by #replication_test_node msg_requests
     * (.map and .queue)
     */
    RTNME_REF_MSG_REQUESTS = 1 << 1,

    /** @brief Reference by #rtn_msg_entry timeout_event  */
    RTNME_REF_MSG_TIMEOUT_EVENT = 1 << 2,

    /**
     * @brief Reference by plat_event_free of #rtn_msg_entry timeout_event
     *
     * The platform library avoids locking to be useful accross threading
     * environments (fth and pthread) and behave well regardless of lock
     * contention.  A side effect is that event free is asynchronous;
     * and that at the time of the free the event closures' environment
     * may still be referenced until the caller provided free closure
     * is applied.
     */
    RTNME_REF_MSG_TIMEOUT_EVENT_FREE = 1 << 3,

    /** @brief Local code has a reference */
    RTNME_REF_MSG_LOCAL = 1 << 4
};

/** @brief Entry in replication_test_node message structures. */
struct rtn_msg_entry {
    /* @brief Parent of this structure */
    struct replication_test_node *test_node;

    /* Fields for all messages */

    /** @brief Bit fielded with rtn_msg_entry_ref */
    int references;

    /* Original request data */

    /** @brief Source service; source must be this node */
    service_t src_service;
    /** @brief Destination node */
    service_t dest_node;
    /** @brief Destination service */
    service_t dest_service;

    /** @brief Whether there's a response expected */
    unsigned response_expected : 1;

    /**
     * @brief Message wrapper.
     *
     * May be NULL when the message has already been sent so that this
     * code no longer holds a reference on the message so there are no
     * test infrastructure references on the data, zero copy optimizations
     * are happening, and we can detect some dangling pointer problems.
     */
    struct sdf_msg_wrapper *msg_wrapper;

    /** @brief #rtn_per_node.msg_outbound.queue entry (all messages) */
    TAILQ_ENTRY(rtn_msg_entry) msg_outbound_queue_entry;

    /** @brief Incremental delay from previous message */
    uint32_t us_latency;

    /* Fields for requests */

    /*
     * This is needed for the cleaner interface to the new message
     * wrapper APIs.
     */

    /**
     * @brief resp_mbx for artificial reply to sender
     *
     * We don't need this because we have the ar_mbx and mkey to find
     * it as separate fields.
     */
    struct sdf_resp_mbx resp_mbx_for_synthetic_reply;

    /**
     * @brief Response structure for request
     *
     * May be NULL for messages that are responses.  The calling
     * code guarantees this will exist until the message is delivered.
     */
    struct sdf_fth_mbx *ar_mbx;

    /**
     * @brief Key used to map response to ar_mbx.
     *
     * Not valid for requests not soliciting a response.
     */
    char mkey[MSG_KEYSZE];

    /** @brief Request list (only with ar_mbx) */
    TAILQ_ENTRY(rtn_msg_entry) msg_requests_queue_entry;

    /** @brief Timeout event; NULL for none */
    struct plat_event *timeout_event;

    /** @brief Response was delivered */
    unsigned response_delivered : 1;
};

/**
 * @brief Callback funciton of shutdown and free simulated node
 */
static void rtn_msg_free(plat_closure_scheduler_t *context, void *env,
                         struct sdf_msg *msg);
#ifdef NETWORK_DELAY
/** @brief Timer fired (all common code) */
static void rtn_next_delivery_cb(plat_closure_scheduler_t *context, void *env,
                                 struct plat_event *event);
#endif

static void rtn_guarantee_bootstrapped(struct replication_test_node *test_node);

static void rtn_start_impl(plat_closure_scheduler_t *context, void *env,
                           replication_test_framework_cb_t user_cb);

static SDF_status_t rtn_start_replicator(struct replication_test_node *test_node);
static SDF_status_t rtn_start_meta_storage(struct replication_test_node *test_node);
static void rtn_liveness_ping_reset(struct replication_test_node *test_node,
                                    enum rtn_liveness_ping_reset_mode mode);
static void rtn_liveness_ping_cancel(struct replication_test_node *test_node);
static void rtn_liveness_ping_cb(plat_closure_scheduler_t *context, void *env,
                                 struct plat_event *event);


static void rtn_crash_impl(plat_closure_scheduler_t *context, void *env,
                           replication_test_node_crash_async_cb_t cb);

static void rtn_crash_or_shutdown_common(struct replication_test_node *test_node);

static void rtn_crash_or_shutdown_messaging_and_liveness(struct replication_test_node *test_node);

static void rtn_crash_or_shutdown_event_cb(plat_closure_scheduler_t *context,
                                           void *env);

static void rtn_replicator_shutdown_cb(plat_closure_scheduler_t *context,
                                       void *env);
static void rtn_meta_storage_shutdown_cb(plat_closure_scheduler_t *context,
                                         void *env);
static int rtn_alloc_per_node(struct replication_test_node *test_node,
                              vnode_t node_id);
static void rtn_node_live_impl(plat_closure_scheduler_t *context, void *env,
                               vnode_t node_id, int64_t epoch);
static void rtn_per_node_liveness_reset_timeout(struct rtn_per_node *per_node);
static void rtn_per_node_liveness_cancel_timeout(struct rtn_per_node *per_node);
static void rtn_per_node_liveness_timeout(plat_closure_scheduler_t *context,
                                          void *env, struct plat_event *event);
static void rtn_node_dead_impl(plat_closure_scheduler_t *context, void *env,
                               vnode_t node_id, int64_t epoch);
static void rtn_per_node_connection_close(struct rtn_per_node *per_node);

static void rtn_shutdown_impl(plat_closure_scheduler_t *context, void *env,
                              replication_test_node_shutdown_async_cb_t cb);

static void rtn_shutdown_flash(struct replication_test_node *test_node);

static void rtn_shutdown_flash_cb(plat_closure_scheduler_t *context, void *env);

static void rtn_crash_do_cb(struct replication_test_node *test_node,
                            replication_test_node_crash_async_cb_t cb,
                            SDF_status_t status);

static void rtn_crash_or_shutdown_complete(struct replication_test_node *test_node);

static void rtn_shutdown_closure_scheduler(struct replication_test_node *test_node);

static void rtn_closure_scheduler_shutdown_cb(plat_closure_scheduler_t *context,
                                              void *env);

static void rtn_ref_count_zero(struct replication_test_node *test_node);

static void rtn_free(struct replication_test_node *test_node);

static void rtn_per_node_free(struct rtn_per_node *per_node);

static void rtn_msg_entry_response(struct rtn_msg_entry *entry,
                                   struct sdf_msg_wrapper *response_wrapper);

static void rtn_msg_entry_release(struct rtn_msg_entry *entry,
                                  enum rtn_msg_entry_ref ref);

static void rtn_msg_entry_error(struct replication_test_node *test_node,
                                struct rtn_msg_entry *entry,
                                SDF_status_t error_type);

static void rtn_deliver_msg(struct replication_test_node *test_node);

static SDF_status_t rtn_next_delivery_node(struct replication_test_node *test_node);
static void rtn_reset_outbound_timer(struct replication_test_node *test_node);

static void rtn_msg_entry_add_timeout(struct rtn_msg_entry *entry);
static void rtn_msg_entry_timeout_cb(plat_closure_scheduler_t *context,
                                     void *env, struct plat_event *event);

static void rtn_msg_entry_add_outbound(struct rtn_msg_entry *entry);


static void rtn_command_impl(plat_closure_scheduler_t *context, void *env,
                             SDF_shardid_t shard, char *command,
                             sdf_replicator_command_cb_t cb);

static void rtn_replicator_notification_cb(plat_closure_scheduler_t *context,
                                           void *env, int events,
                                           struct cr_shard_meta *shard_meta,
                                           enum sdf_replicator_access access,
                                           struct timeval expires,
                                           sdf_replicator_notification_complete_cb_t complete_cb);

static void rtn_send_msg_api_cb(plat_closure_scheduler_t *context, void *env,
                                struct sdf_msg_wrapper *msg_wrapper,
                                struct sdf_fth_mbx *ar_mbx,
                                SDF_status_t *status);

static void rtn_send_msg_impl(plat_closure_scheduler_t *context, void *env,
                              struct sdf_msg_wrapper *msg_wrapper,
                              struct sdf_fth_mbx *ar_mbx);

static void rtn_receive_msg_impl(plat_closure_scheduler_t *context, void *env,
                                 struct sdf_msg_wrapper *msg_wrapper);

static void rtn_ref_count_dec(struct replication_test_node *test_node);

static void rtn_msg_entry_free(struct rtn_msg_entry *entry);

static void rtn_msg_entry_remove_timeout(struct rtn_msg_entry *entry);

static void rtn_msg_entry_remove_timeout_cb(plat_closure_scheduler_t *context,
                                            void *env);

static void rtn_msg_entry_remove_outbound(struct rtn_msg_entry *entry);

static void rtn_event_free_cb(plat_closure_scheduler_t *context, void *env);

static void rtn_msg_entry_remove_requests(struct rtn_msg_entry *entry);

static void rtn_msg_entry_add_requests(struct rtn_msg_entry *entry);
static void
rtn_meta_storage_update_cb(plat_closure_scheduler_t *context, void *env,
                           SDF_status_t status,
                           struct cr_shard_meta *shard_meta,
                           struct timeval lease_expires);
static void rtn_get_shard_meta_impl(plat_closure_scheduler_t *context,
                                    void *env, SDF_shardid_t sguid,
                                    const struct sdf_replication_shard_meta *shard_meta,
                                    rtfw_shard_meta_cb_t user_cb);
static void rtn_put_shard_meta_impl(plat_closure_scheduler_t *context,
                                    void *env,
                                    const struct cr_shard_meta *cr_shard_meta,
                                    rtfw_shard_meta_cb_t cb,
                                    enum rtn_put_shard_meta_mode mode);
static void rtn_delete_shard_meta_impl(plat_closure_scheduler_t *context,
                                       void *env,
                                       const struct cr_shard_meta *cr_shard_meta,
                                       rtfw_shard_meta_cb_t cb);

static struct rtn_meta_storage_request *
rtn_meta_storage_request_start(struct replication_test_node *test_node,
                               rtfw_shard_meta_cb_t user_cb);
static void rtn_meta_storage_request_cb(plat_closure_scheduler_t *context,
                                        void *env, SDF_status_t status,
                                        struct cr_shard_meta *shard_meta,
                                        struct timeval lease_expires);

static void rtn_log_msg_wrapper(struct replication_test_node *test_node,
                                struct sdf_msg_wrapper *msg_wrapper,
                                const char *where, int log_cat,
                                enum plat_log_level log_level);

static void rtn_start_network_impl(plat_closure_scheduler_t *context, void *env,
                                   rtfw_void_cb_t user_cb);
static void rtn_start_network_internal(struct replication_test_node *test_node);
static void rtn_resume_network_internal(struct replication_test_node *test_node);
static void rtn_shutdown_network_impl(plat_closure_scheduler_t *context,
                                      void *env, rtfw_void_cb_t user_cb);
static void rtn_suspend_network_internal(struct replication_test_node *test_node,
                                         plat_event_free_done_t free_done_cb);
static const char *rtn_state_to_string(enum rtn_state state) __attribute__((unused));
static const char *rtn_liveness_to_string(enum rtn_liveness liveness);


struct replication_test_node *
replication_test_node_alloc(const struct replication_test_config *test_config,
                            struct replication_test_framework *test_framework,
                            const replication_test_api_t *api_arg,
                            vnode_t node) {
    struct replication_test_node *ret = NULL;
    int failed;
    vnode_t other_node;

    failed = !plat_calloc_struct(&ret);
    if (!failed) {
        /* Static configuration */
        ret->test_config = *test_config;
        ret->test_config.replicator_config.my_node = node;
        ret->framework = test_framework;
        ret->node_id = node;

        /* Node dynamic state */
        fthLockInit(&ret->replicator_op_lock);
        ret->state = RTN_STATE_INITIAL;
        ret->epoch = 0;

        ret->closure_scheduler = plat_mbox_scheduler_alloc();
        if (!ret->closure_scheduler) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "node_id %lld failed to allocate closure_scheduler",
                         (long long)node);
            failed = 1;
        }

        ret->ref_count = 0;
        ret->crash_cb = replication_test_node_crash_async_cb_null;
        ret->shutdown_cb = replication_test_node_shutdown_async_cb_null;

        /* Messaging state */
        ret->net_state = RTN_NET_UP;

        ret->liveness_ping_event = NULL;
        if (-1 == plat_asprintf(&ret->liveness_ping_event_name,
                                "node_id %lld liveness_ping",
                                (long long)ret->node_id)) {
            failed = 1;
        }

        ret->msg_seqno = 0;

        ret->msg_outbound.total_queue_size = 0;
        if (-1 == plat_asprintf(&ret->msg_outbound.next_delivery_event_name,
                                "node_id %lld next_delivery",
                                (long long)ret->node_id)) {
            failed = 1;
        }
        ret->msg_outbound.next_delivery_event = NULL;
        ret->msg_outbound.next_delivery_node = 0;


        TAILQ_INIT(&ret->msg_requests.queue);
        ret->msg_requests.map = HashMap_create(RTN_HASH_BUCKETS,
                                               FTH_MAP_RW /* lock */);
        if (!ret->msg_requests.map) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "node_id %lld failed to allocate msg_requests.mmap",
                         (long long)node);
            failed = 1;
        }

        ret->per_node = plat_calloc(ret->test_config.nnode,
                                    sizeof (ret->per_node[0]));
        if (!ret->per_node) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "node_id %lld failed to allocate per_node array",
                         (long long)node);
            failed = 1;
        }

        ret->replicator_notification_user_cb =
            rtfw_replicator_notification_cb_null;
        ret->meta_storage_user_cb = rtfw_shard_meta_cb_null;
    }

    for (other_node = 0; !failed && other_node < ret->test_config.nnode;
         ++other_node) {
        failed = rtn_alloc_per_node(ret, other_node);
    }

    if (!failed) {
        ret->component_api = *api_arg;
        ret->component_api.send_msg =
            sdf_replicator_send_msg_cb_create(PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS,
                                              &rtn_send_msg_api_cb, ret);
        ret->component_api.single_scheduler = ret->closure_scheduler;

        ret->flash = replication_test_flash_alloc(test_config, &ret->component_api, node);
        if (!ret->flash) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "node_id %lld failed to allocate flash",
                         (long long)node);
            failed = 1;
        }
    }

    if (!failed) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "node_id %lld allocated at %p",
                     (long long)ret->node_id, ret);
    } else if (!ret) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR, "node_id %"PRIu32" alloc failed",
                     node);
    } else if (ret->closure_scheduler) {
        /*
         * XXX: This needs to happen to actually destroy the closure scheduler,
         * although an asynchronous flash implementation won't shutdown cleanly.
         *
         * Things are more composable if we have separate shutdown and free;
         * so we probably want those APIs split.
         */
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "test_node alloc failed, temporary scheduler to shutdown");
        ret->state = RTN_STATE_DEAD;
        rtn_shutdown_async(ret, replication_test_node_shutdown_async_cb_null);
        plat_mbox_scheduler_main((uint64_t)ret->closure_scheduler);
        ret = NULL;
    } else {
        rtn_free(ret);
        ret = NULL;
    }
    return (ret);
}

static int
rtn_alloc_per_node(struct replication_test_node *test_node, vnode_t node_id) {
    int failed;
    struct rtn_per_node *per_node;

    failed = !plat_calloc_struct(&per_node);
    if (!failed) {
        per_node->local_node = test_node;
        per_node->remote_node = node_id;

        /* XXX: drew 2010-04-02 Probably bring up all nets at the same time */
        per_node->net_state = RTN_NET_UP;

        per_node->liveness.state = RTNL_DEAD;
        per_node->liveness.event = NULL;
        if (-1 == plat_asprintf(&per_node->liveness.event_name,
                                "node_id %lld remote node %lld liveness",
                                (long long)test_node->node_id,
                                (long long)node_id)) {
            failed = 1;
        }
        per_node->liveness.epoch = 0;

        TAILQ_INIT(&per_node->msg_outbound.queue);
        per_node->msg_outbound.queue_size = 0;
    }

    if (!failed) {
        test_node->per_node[node_id] = per_node;
    } else if (per_node) {
        rtn_per_node_free(per_node);
    }

    return (failed);
}

void
rtn_node_live(struct replication_test_node *test_node, vnode_t node_id,
              int64_t epoch) {
    rtn_do_node_liveness_t do_cb =
        rtn_do_node_liveness_create(test_node->closure_scheduler,
                                    &rtn_node_live_impl, test_node);
    plat_closure_apply(rtn_do_node_liveness, &do_cb, node_id, epoch);
    rtn_guarantee_bootstrapped(test_node);
}

/* XXX: drew 2010-04-05 Should add messaging delay to liveness indication */
static void
rtn_node_live_impl(plat_closure_scheduler_t *context, void *env,
                   vnode_t src_node, int64_t src_epoch) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    struct rtn_per_node *per_node;
    struct timeval now;

    plat_assert(src_node < test_node->test_config.nnode);
    per_node = test_node->per_node[src_node];
    plat_assert(per_node);
    plat_assert(test_node->state == RTN_STATE_DEAD ||
                test_node->state == RTN_STATE_LIVE);

    /* XXX: drew 2010-04-06 Should signal that this node is alive */
    if (src_node != test_node->node_id) {
        plat_log_msg(LOG_ID, LOG_CAT_LIVENESS, LOG_DBG,
                     "node %lld node %lld state %s -> live",
                     (long long)test_node->node_id, (long long)src_node,
                     rtn_liveness_to_string(per_node->liveness.state));

        if (per_node->liveness.state == RTN_STATE_LIVE &&
            src_epoch != per_node->liveness.epoch) {
            rtn_node_dead_impl(context, env, src_node, test_node->epoch);
        }

        switch (per_node->liveness.state) {
        case RTNL_DEAD:
            per_node->liveness.state = RTNL_LIVE;
            per_node->liveness.epoch = src_epoch;

            if (test_node->state == RTN_STATE_LIVE) {
                rtn_per_node_liveness_reset_timeout(per_node);

                /*
                 * XXX: drew 2010-04-05 Do we forward to sub-components always,
                 * or only on edges?
                 */
                plat_closure_apply(plat_timer_dispatcher_gettime,
                                   &test_node->component_api.gettime, &now);
                if (test_node->replicator) {
                    sdf_replicator_node_live(test_node->replicator, src_node,
                                             now);
                }
                if (test_node->meta_storage) {
                    rms_node_live(test_node->meta_storage, src_node);
                }

                if (test_node->test_config.new_liveness) {
                    rtfw_receive_liveness_ping(test_node->framework,
                                               test_node->node_id, src_node,
                                               test_node->epoch);
                }
            }
            break;

        case RTNL_LIVE:
            /* NOP */
            break;
        }
    }
}

static void
rtn_per_node_liveness_reset_timeout(struct rtn_per_node *per_node) {
    plat_event_fired_t fired;
    struct timeval when;

    fired = plat_event_fired_create(per_node->local_node->closure_scheduler,
                                    &rtn_per_node_liveness_timeout, per_node);

    if (per_node->local_node->test_config.new_liveness) {
        rtn_per_node_liveness_cancel_timeout(per_node);

        when.tv_sec = per_node->local_node->test_config.msg_live_secs;
        when.tv_usec = 0;

        per_node->liveness.event =
            plat_timer_dispatcher_timer_alloc(per_node->local_node->component_api.timer_dispatcher,
                                              per_node->liveness.event_name,
                                              LOG_CAT_EVENT,
                                              fired, 1 /* free_count */,
                                              &when,
                                              PLAT_TIMER_RELATIVE,
                                              NULL /* rank_ptr */);
        plat_assert(per_node->liveness.event);
    }
}

static void
rtn_per_node_liveness_cancel_timeout(struct rtn_per_node *per_node) {
    plat_event_free_done_t free_done_cb;
    free_done_cb =
        plat_event_free_done_create(per_node->local_node->closure_scheduler,
                                    &rtn_event_free_cb, per_node);

    if (per_node->liveness.event) {
        ++per_node->local_node->ref_count;
        plat_event_free(per_node->liveness.event, free_done_cb);
        per_node->liveness.event = NULL;
    }
}

static void
rtn_per_node_liveness_timeout(plat_closure_scheduler_t *context, void *env,
                              struct plat_event *event) {
    struct rtn_per_node *per_node = (struct rtn_per_node *)env;

    if (per_node->liveness.event == event) {
        rtn_node_dead_impl(context, per_node->local_node,
                           per_node->remote_node,
                           per_node->liveness.epoch);
    }
}

void
rtn_node_dead(struct replication_test_node *test_node, vnode_t src_node,
              int64_t dest_epoch) {
    rtn_do_node_liveness_t do_cb =
        rtn_do_node_liveness_create(test_node->closure_scheduler,
                                    &rtn_node_dead_impl, test_node);
    plat_closure_apply(rtn_do_node_liveness, &do_cb, src_node, dest_epoch);
    rtn_guarantee_bootstrapped(test_node);
}

static void
rtn_node_dead_impl(plat_closure_scheduler_t *context, void *env,
                   vnode_t src_node, int64_t dest_epoch) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    struct rtn_per_node *per_node;
    struct rtn_msg_entry *entry;
    struct rtn_msg_entry *next_entry;

    plat_assert(src_node < test_node->test_config.nnode);
    per_node = test_node->per_node[src_node];
    plat_assert(per_node);

    if (src_node != test_node->node_id && dest_epoch == test_node->epoch) {
        plat_log_msg(LOG_ID, LOG_CAT_LIVENESS, LOG_DBG,
                     "node %lld node %lld state %s -> dead",
                     (long long)test_node->node_id, (long long)src_node,
                     rtn_liveness_to_string(per_node->liveness.state));

        per_node->liveness.state = RTNL_DEAD;
        rtn_per_node_liveness_cancel_timeout(per_node);

        if (test_node->state == RTN_STATE_LIVE) {
            /*
             * XXX: drew 2009-05-20 It would be interesting to simulate the
             * indeterminant ordering of whether the message closures
             * get applied first or the liveness event is delivered.
             */
            if (test_node->replicator) {
                sdf_replicator_node_dead(test_node->replicator, src_node);
            }
            if (test_node->meta_storage) {
                rms_node_dead(test_node->meta_storage, src_node);
            }
            TAILQ_FOREACH_SAFE(entry, &test_node->msg_requests.queue,
                               msg_requests_queue_entry, next_entry) {
                rtn_msg_entry_error(test_node, entry, SDF_NODE_DEAD);
            }

            rtn_per_node_connection_close(per_node);
        }
    }
}

/**
 * @brief Simulated close of connection to per_node
 *
 * Delivery is deferred if network is currently down.
 */
static void
rtn_per_node_connection_close(struct rtn_per_node *per_node) {

    if (!per_node->local_node->test_config.new_liveness) {
    } else if (per_node->local_node->net_state == RTN_NET_UP &&
               per_node->net_state == RTN_NET_UP) {
        per_node->connection_close_needed = 0;
        rtfw_receive_connection_close(per_node->local_node->framework,
                                      per_node->local_node->node_id,
                                      per_node->remote_node,
                                      per_node->liveness.epoch);
    } else {
        per_node->connection_close_needed = 1;
    }
}

int
rtn_node_is_live(struct replication_test_node *test_node) {
    return (test_node->state == RTN_STATE_LIVE);
}

void
rtn_shutdown_async(struct replication_test_node *test_node,
                   replication_test_node_shutdown_async_cb_t cb) {
    rtn_do_shutdown_t do_shutdown =
        rtn_do_shutdown_create(test_node->closure_scheduler,
                               &rtn_shutdown_impl, test_node);
    plat_closure_apply(rtn_do_shutdown, &do_shutdown, cb);
    rtn_guarantee_bootstrapped(test_node);
}

static void
rtn_shutdown_impl(plat_closure_scheduler_t *context, void *env,
                  replication_test_node_shutdown_async_cb_t cb) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;

    plat_assert(test_node->state != RTN_STATE_INITIAL &&
                test_node->state != RTN_STATE_SHUTDOWN &&
                test_node->state != RTN_STATE_TO_SHUTDOWN);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "node_id %lld rtn_shutdown_impl",
                 (long long)test_node->node_id);

    switch (test_node->state) {
    case RTN_STATE_INITIAL:
        plat_assert_always(0);
        break;
    case RTN_STATE_LIVE:
    case RTN_STATE_DEAD:
    case RTN_STATE_TO_DEAD:
        test_node->state = RTN_STATE_TO_SHUTDOWN;

        /* Delay shutting down flash until crash completes so its simplest */
        if (test_node->state != RTN_STATE_TO_DEAD) {
            rtn_shutdown_flash(test_node);
        }

        test_node->shutdown_cb = cb;
        rtn_crash_or_shutdown_common(test_node);
        break;

    /* Shutdown converts this into a NULL pointer and isn't legal */
    case RTN_STATE_TO_SHUTDOWN:
    case RTN_STATE_SHUTDOWN:
        plat_assert_always(0);
    }
}

/**
 * @brief Shutdown flash subsystem
 *
 * Called from within the shutdown code implementation, either initially
 * or after a pending crash completes when state was #RTN_STATE_TO_DEAD.
 */
static void
rtn_shutdown_flash(struct replication_test_node *test_node) {
    replication_test_flash_shutdown_async_cb_t flash_cb;

    plat_assert(test_node->state == RTN_STATE_TO_SHUTDOWN);
    plat_assert(!test_node->flash_shutdown_started);

    flash_cb =
        replication_test_flash_shutdown_async_cb_create(test_node->closure_scheduler,
                                                        &rtn_shutdown_flash_cb,
                                                        test_node);
    test_node->flash_shutdown_started = 1;
    ++test_node->ref_count;
    rtf_shutdown_async(test_node->flash, flash_cb);
}

/** @brief Called when #rtf_shutdown completes */
static void
rtn_shutdown_flash_cb(plat_closure_scheduler_t *context, void *env) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;

    /* This became invalid at some point before this call */
    test_node->flash = NULL;

    /* Share common event reference counting code */
    rtn_crash_or_shutdown_event_cb(context, env);
}

void
rtn_start(struct replication_test_node *test_node,
          replication_test_framework_cb_t user_cb) {
    rtn_do_start_t do_start;
    do_start = rtn_do_start_create(test_node->closure_scheduler,
                                   &rtn_start_impl, test_node);
    plat_closure_apply(rtn_do_start, &do_start, user_cb);
    /* After, so do_start is most likely to execute first */
    rtn_guarantee_bootstrapped(test_node);
}

/**
 * @brief Guarantee the closure scheduler is running
 *
 * This means that a first call from consumer or producter other than
 * #rtn_start can indirect via its four line proxy and get correct
 * results.
 *
 * The caller should probably enqueue its worker closure before
 * calling this so that it executes "first" but that's not required.
 */
static void
rtn_guarantee_bootstrapped(struct replication_test_node *test_node) {
    /* Special case the transition between initial and starting state */
    if (test_node->state == RTN_STATE_INITIAL &&
        __sync_bool_compare_and_swap(&test_node->state,
                                     RTN_STATE_INITIAL /* from */,
                                     RTN_STATE_DEAD /* to */)) {

        /* start event scheduler */
        fthResume(fthSpawn(&plat_mbox_scheduler_main, 40960),
                  (uint64_t)test_node->closure_scheduler);
    }
}

static void
rtn_start_impl(plat_closure_scheduler_t *context, void *env,
               replication_test_framework_cb_t user_cb) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    SDF_status_t status = SDF_SUCCESS;
    int i;

    /* State must be at least RTN_STATE_DEAD before getting here */
    plat_assert(test_node->state != RTN_STATE_INITIAL);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "node_id %lld rtn_start_impl",
                 (long long)test_node->node_id);

    if (test_node->state != RTN_STATE_DEAD) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_WARN,
                     "node_id %lld skipping start: state %s not RTN_STATE_DEAD",
                     (long long)test_node->node_id,
                     rtn_state_to_string(test_node->state));
        status = SDF_FAILURE;
    }

    if (status == SDF_SUCCESS) {
        status = rtf_start(test_node->flash);
        if (status != SDF_SUCCESS) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "node_id %lld failed to start flash: %s",
                         (long long)test_node->node_id,
                         sdf_status_to_string(status));
        }
    }

    if (status == SDF_SUCCESS) {
        switch (test_node->test_config.test_type) {
        case RT_TYPE_REPLICATOR:
            status = rtn_start_replicator(test_node);
            break;
        case RT_TYPE_META_STORAGE:
            status = rtn_start_meta_storage(test_node);
            break;
        }
    }

    if (status == SDF_SUCCESS) {
        /*
         * XXX: drew 2010-04-15 It would be cleaner to consider
         * the old liveness instantiation where we're dealing
         * with it; but better still just to deprecate it
         */
        if (test_node->test_config.new_liveness) {
            ++test_node->epoch;
        }

        plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                     "node_id %lld started epoch %lld",
                     (long long)test_node->node_id,
                     (long long)test_node->epoch);
        test_node->state = RTN_STATE_LIVE;


        /* Notify self other nodes which are up */
        for (i = 0; i < test_node->test_config.nnode; ++i) {
            plat_assert(test_node->per_node[i]);
            plat_assert(!test_node->per_node[i]->connection_close_needed);
#ifdef notyet
            /*
             * XXX: drew 2010-04-15 Liveness for my node if Johann
             * does this.
             */
            if (i == test_node->node_id) {
                /* Create edge for current implementation */
                test_node->per_node[i]->liveness.state = RTNL_DEAD;
                rtn_node_live_impl(context, test_node, i,
                                   test_node->epoch);
            } else
#endif
            if (test_node->test_config.new_liveness) {
                test_node->per_node[i]->liveness.state = RTNL_DEAD;
            } else if (test_node->per_node[i]->liveness.state == RTNL_LIVE) {
                /* Create edge for current implementation */
                test_node->per_node[i]->liveness.state = RTNL_DEAD;
                rtn_node_live_impl(context, test_node, i, 0 /* epoch */);
            }
            /* Ignore RTNL_DEAD state */
        }

        rtn_liveness_ping_reset(test_node, RTN_LPRM_INITIAL);

        rtn_resume_network_internal(test_node);
    }

    plat_closure_apply(replication_test_framework_cb, &user_cb, status);
}

/** @brief Create and start replicator for RT_TYPE_REPLICATOR */
static SDF_status_t
rtn_start_replicator(struct replication_test_node *test_node) {
    SDF_status_t status = SDF_SUCCESS;
    sdf_replicator_notification_cb_t notification_cb =
        sdf_replicator_notification_cb_create(test_node->component_api.
                                              single_scheduler,
                                              &rtn_replicator_notification_cb,
                                              test_node);

    plat_assert(test_node->test_config.test_type == RT_TYPE_REPLICATOR);

    if (status == SDF_SUCCESS && !test_node->replicator) {
        test_node->replicator =
            sdf_copy_replicator_alloc(&test_node->test_config.replicator_config,
                                      &test_node->component_api);
        if (!test_node->replicator) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "node_id %lld failed to allocate replicator",
                         (long long)test_node->node_id);
            status = SDF_FAILURE_MEMORY_ALLOC;
        }
        /*
         * Because handling undo is unecessarily complicated for something
         * which should never happen.
         */
        plat_assert_always(status == SDF_SUCCESS);
    }

    if (status == SDF_SUCCESS && !test_node->replicator_notifier) {
        test_node->replicator_notifier =
            sdf_replicator_add_notifier(test_node->replicator, notification_cb);
        if (!test_node->replicator_notifier) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "node_id %lld failed to allocate replicator notifier",
                         (long long)test_node->node_id);
            status = SDF_FAILURE_MEMORY_ALLOC;
        }
        plat_assert_always(status == SDF_SUCCESS);
    }

    if (status == SDF_SUCCESS) {
        if (sdf_replicator_start(test_node->replicator)) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "node_id %lld failed to start replicator",
                         (long long)test_node->node_id);
            status = SDF_FAILURE;
        }
        /*
         * Because handling undo is unecessarily complicated for something
         * which should never happen.
         */
        plat_assert_always(status == SDF_SUCCESS);
    }

    return (status);
}

/** @brief Create and start replicator for RT_TYPE_META_STORAGE */
static SDF_status_t
rtn_start_meta_storage(struct replication_test_node *test_node) {
    rms_shard_meta_cb_t update_cb;
    SDF_status_t status = SDF_SUCCESS;

    update_cb = rms_shard_meta_cb_create(test_node->closure_scheduler,
                                         rtn_meta_storage_update_cb,
                                         test_node);

    plat_assert(test_node->test_config.test_type == RT_TYPE_META_STORAGE);

    if (status == SDF_SUCCESS && !test_node->meta_storage) {
        test_node->meta_storage =
            replicator_meta_storage_alloc(&test_node->test_config.replicator_config,
                                          &test_node->component_api,
                                          update_cb);
        if (!test_node->meta_storage) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "node_id %lld failed to allocate meta storage",
                         (long long)test_node->node_id);
            status = SDF_FAILURE_MEMORY_ALLOC;
        }
        /*
         * Because handling undo is unecessarily complicated for something
         * which should never happen.
         */
        plat_assert_always(status == SDF_SUCCESS);
    }

    if (status == SDF_SUCCESS) {
        rms_start(test_node->meta_storage);
    }

    return (status);
}

static void
rtn_liveness_ping_reset(struct replication_test_node *test_node,
                        enum rtn_liveness_ping_reset_mode mode) {
    plat_event_fired_t fired;
    struct timeval when;
    struct replication_test_timing timing;

    fired = plat_event_fired_create(test_node->closure_scheduler,
                                    &rtn_liveness_ping_cb, test_node);

    if (test_node->test_config.new_liveness) {
        rtn_liveness_ping_cancel(test_node);

        timing = test_node->test_config.network_timing;

        switch (mode) {
        case RTN_LPRM_INITIAL:
            /* Only messaging delay */
            break;
        case RTN_LPRM_RECURRING:
            timing.min_delay_us +=
                test_node->test_config.msg_ping_secs * PLAT_MILLION;
            timing.max_delay_us +=
                test_node->test_config.msg_ping_secs * PLAT_MILLION;
            break;
        }

        (void) rtfw_get_delay_us(test_node->framework, &timing, &when);

        test_node->liveness_ping_event =
            plat_timer_dispatcher_timer_alloc(test_node->component_api.timer_dispatcher,
                                              test_node->liveness_ping_event_name,
                                              LOG_CAT_EVENT,
                                              fired, 1 /* free_count */,
                                              &when,
                                              PLAT_TIMER_RELATIVE,
                                              NULL /* rank_ptr */);
        plat_assert(test_node->liveness_ping_event);
    }
}

static void
rtn_liveness_ping_cancel(struct replication_test_node *test_node) {
    plat_event_free_done_t free_done_cb;

    free_done_cb = plat_event_free_done_create(test_node->closure_scheduler,
                                               &rtn_event_free_cb, test_node);

    if (test_node->liveness_ping_event) {
        ++test_node->ref_count;
        plat_event_free(test_node->liveness_ping_event, free_done_cb);
        test_node->liveness_ping_event = NULL;
    }
}

static void
rtn_liveness_ping_cb(plat_closure_scheduler_t *context, void *env,
                     struct plat_event *event) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    int i;

    plat_assert(test_node->test_config.new_liveness);

    /* Otherwise this has been cancelled */
    if (test_node->liveness_ping_event == event) {
        rtn_liveness_ping_reset(test_node, RTN_LPRM_RECURRING);

        if (test_node->net_state == RTN_NET_UP) {
            for (i = 0; i < test_node->test_config.nnode; ++i) {
                if (test_node->per_node[i]->remote_node != test_node->node_id &&
                    test_node->per_node[i]->net_state == RTN_NET_UP) {
                    rtfw_receive_liveness_ping(test_node->framework,
                                               test_node->node_id,
                                               test_node->per_node[i]->remote_node,
                                               test_node->epoch);
                }
            }
        }
    }
}

void
rtn_crash_async(struct replication_test_node *test_node,
                replication_test_node_crash_async_cb_t cb)  {
    rtn_do_crash_t do_crash = rtn_do_crash_create(test_node->closure_scheduler,
                                                  rtn_crash_impl, test_node);
    plat_closure_apply(rtn_do_crash, &do_crash, cb);
    rtn_guarantee_bootstrapped(test_node);
}

/** @brief Actual implementation of #rtn_crash_async */
static void
rtn_crash_impl(plat_closure_scheduler_t *context, void *env,
               replication_test_node_crash_async_cb_t cb) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    replication_test_flash_crash_async_cb_t flash_cb;

    /* State must be at least RTN_STATE_DEAD before getting here */
    plat_assert(test_node->state != RTN_STATE_INITIAL);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "node_id %lld rtn_crash_impl",
                 (long long)test_node->node_id);

    switch (test_node->state) {
    case RTN_STATE_INITIAL:
        plat_assert_always(0);
        break;
    case RTN_STATE_LIVE:
        test_node->state = RTN_STATE_TO_DEAD;
        test_node->crash_cb = cb;

        ++test_node->ref_count;
        flash_cb =
            replication_test_flash_crash_async_cb_create(test_node->closure_scheduler,
                                                         &rtn_crash_or_shutdown_event_cb,
                                                         test_node);
        rtf_crash_async(test_node->flash, flash_cb);

        rtn_crash_or_shutdown_common(test_node);
        break;
    case RTN_STATE_TO_DEAD:
        rtn_crash_do_cb(test_node, cb, SDF_BUSY);
        break;
    case RTN_STATE_DEAD:
        rtn_crash_do_cb(test_node, cb, SDF_NODE_DEAD);
        break;
    case RTN_STATE_TO_SHUTDOWN:
    case RTN_STATE_SHUTDOWN:
        rtn_crash_do_cb(test_node, cb, SDF_SHUTDOWN);
        break;
   }
}

/**
 * @brief Common code to crash or shutdown
 *
 * When a crash is requested and then a shutdown before the crash completes,
 * this is only executed by the first piece of code.  New operations cause
 * ref_count to be generated and completions decrement
 *
 * The common pieces of crash and shutdown are replicator shutdown
 * and messaging shutdown.
 */
static void
rtn_crash_or_shutdown_common(struct replication_test_node *test_node) {
    sdf_replicator_shutdown_cb_t replicator_cb;
    rms_shutdown_cb_t meta_storage_cb;
    plat_event_free_done_t event_cb;

    plat_assert(test_node->state == RTN_STATE_TO_DEAD ||
                test_node->state == RTN_STATE_TO_SHUTDOWN);

    replicator_cb =
        sdf_replicator_shutdown_cb_create(test_node->closure_scheduler,
                                          &rtn_replicator_shutdown_cb,
                                          test_node);
    if (test_node->replicator) {
        /*
         * Gurantee that test_node->replicator does not become invalid
         * until after #rtn_get_op_meta terminates.  Once we're here
         * the state is no longer RTN_STATE_LIVE so a fresh call won't touch
         * the replicator.
         */
        fthUnlock(fthLock(&test_node->replicator_op_lock,
                          1 /* write lock */, NULL));
        ++test_node->ref_count;
        sdf_replicator_shutdown(test_node->replicator, replicator_cb);
    }

    if (test_node->replicator_notifier) {
        plat_assert(test_node->replicator);
        sdf_replicator_remove_notifier(test_node->replicator,
                                       test_node->replicator_notifier);
        test_node->replicator_notifier = NULL;
    }

    meta_storage_cb =
        rms_shutdown_cb_create(test_node->closure_scheduler,
                               &rtn_meta_storage_shutdown_cb, test_node);
    if (test_node->meta_storage) {
        ++test_node->ref_count;
        rms_shutdown(test_node->meta_storage, meta_storage_cb);
    }

    /* XXX: drew 2010-04-07 See #rtn_suspend_network_internal comment */
    event_cb = plat_event_free_done_create(test_node->closure_scheduler,
                                           &rtn_crash_or_shutdown_event_cb,
                                           test_node);
    rtn_suspend_network_internal(test_node, event_cb);

    rtn_crash_or_shutdown_messaging_and_liveness(test_node);

    /*
     * We don't have an extra increment at the start of this function because
     * ref_count is only used for event frees which are alway asynchronous.
     */
    if (!test_node->ref_count) {
        rtn_ref_count_zero(test_node);
    }
}

/**
 * @brief #sdf_replicator shutdown complete callback
 */
static void
rtn_replicator_shutdown_cb(plat_closure_scheduler_t *context, void *env) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;

    test_node->replicator = NULL;
    rtn_crash_or_shutdown_event_cb(context, env);
}

/**
 * @brief #rms_shutdown complete callback
 */
static void
rtn_meta_storage_shutdown_cb(plat_closure_scheduler_t *context, void *env) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;

    test_node->meta_storage = NULL;
    rtn_crash_or_shutdown_event_cb(context, env);
}

/**
 * @brief Shutdown the node's messaging layer and livenss
 */
static void
rtn_crash_or_shutdown_messaging_and_liveness(struct replication_test_node *test_node) {
    SDF_status_t error_type;
    plat_event_free_done_t free_done_cb;
    struct rtn_msg_entry *entry;
    int i;
    struct rtn_msg_queue *queue;

    plat_assert(test_node->state == RTN_STATE_TO_DEAD ||
                test_node->state == RTN_STATE_TO_SHUTDOWN);

    if (test_node->state == RTN_STATE_TO_SHUTDOWN) {
        error_type = SDF_SHUTDOWN;
    } else {
        error_type = SDF_TEST_CRASH;
    }

    free_done_cb =
        plat_event_free_done_create(test_node->closure_scheduler,
                                    &rtn_crash_or_shutdown_event_cb,
                                    test_node);

    if (test_node->liveness_ping_event) {
        plat_event_free(test_node->liveness_ping_event, free_done_cb);
        test_node->liveness_ping_event = NULL;
    }

    for (i = 0; i < test_node->test_config.nnode; ++i) {
        if (test_node->per_node[i]) {
            plat_assert_imply(test_node->per_node[i]->liveness.event,
                              test_node->test_config.new_liveness);

            /*
             * FIXME: drew 2010-04-06 Add a clean shutdown mode where
             * this is delivered if it exists.
             */
            test_node->per_node[i]->connection_close_needed = 0;

            queue = &test_node->per_node[i]->msg_outbound.queue;
            while (!TAILQ_EMPTY(queue)) {
                entry = TAILQ_FIRST(queue);
                rtn_msg_entry_error(test_node, entry, error_type);
            }

            /* XXX: drew 2010-04-07 do we need to exclude ourself? */
            if (test_node->test_config.new_liveness &&
                i != test_node->node_id) {
                test_node->per_node[i]->liveness.state = RTNL_DEAD;
            }

            if (test_node->per_node[i]->liveness.event) {
                ++test_node->ref_count;
                plat_event_free(test_node->per_node[i]->liveness.event,
                                free_done_cb);
                test_node->per_node[i]->liveness.event = NULL;
            }

        }
    }

    /* Remove all remaining messages */
    while (!TAILQ_EMPTY(&test_node->msg_requests.queue)) {
        entry = TAILQ_FIRST(&test_node->msg_requests.queue);
        rtn_msg_entry_error(test_node, entry, error_type);
    }
}

static void
rtn_crash_do_cb(struct replication_test_node *test_node,
                replication_test_node_crash_async_cb_t cb,
                SDF_status_t status) {

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, LOG_DBG,
                 "node_id %"PRIu32" crash completed with status %s",
                 test_node->node_id, sdf_status_to_string(status));

    plat_closure_apply(replication_test_node_crash_async_cb, &cb, status);
}

static
void rtn_per_node_free(struct rtn_per_node *per_node) {
    if (per_node) {
        if (per_node->local_node) {
            per_node->local_node->per_node[per_node->remote_node] = NULL;
        }

        /** per_node->liveness.event would reference per_node */
        plat_assert(!per_node->liveness.event);
        plat_free(per_node->liveness.event_name);

        plat_assert(TAILQ_EMPTY(&per_node->msg_outbound.queue));
        plat_assert(!per_node->msg_outbound.queue_size);

        plat_free(per_node);
    }
}

/**
 * @brief Signal error detection for #rtn_msg_entry
 *
 * A SDF_MSG_ERROR response is generated for entry and it's removed from
 * all internal structures just as if a regular response had been received
 * for it.
 *
 * @param entry <IN> message entry
 * @param error_type <IN> type of error
 * @param entry_done_cb <IN> closure applied when the system is finally
 * done with the entry.
 */
static void
rtn_msg_entry_error(struct replication_test_node *test_node,
                    struct rtn_msg_entry *entry,
                    SDF_status_t error_type) {
    struct sdf_msg_wrapper *response_wrapper;
    struct sdf_msg *response_msg;
    struct sdf_msg_error_payload *response_payload;
    sdf_msg_wrapper_free_local_t local_free;

    test_node = entry->test_node;

    if (!entry->response_expected || entry->response_delivered) {
        response_wrapper = NULL;
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT,
                     (error_type != SDF_TEST_CRASH &&
                      error_type != SDF_SHUTDOWN) ? LOG_WARN : LOG_DIAG,
                     "node_id %lld mkey %s error %s",
                     (long long)test_node->node_id, entry->mkey,
                     sdf_status_to_string(error_type));

        response_msg = sdf_msg_alloc(sizeof (*response_payload));
        plat_assert(response_msg);

        response_msg->msg_type = SDF_MSG_ERROR;
        response_msg->msg_len = sizeof(SDF_protocol_msg_t) +
            sizeof(*response_msg);
        response_msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_INCLUDED;
        /*
         * XXX: drew 2009-05-31 This is incorrect, although we probably
         * do want to have one class of messages.
         */
        response_payload =
            (struct sdf_msg_error_payload *)&response_msg->msg_payload;
        response_payload->error = error_type;

        /* Wrapper as a sdf_msg_wrapper */
        local_free =
            sdf_msg_wrapper_free_local_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                              &rtn_msg_free, NULL);

        response_wrapper =
            sdf_msg_wrapper_local_alloc(response_msg, local_free,
                                        SMW_MUTABLE_FIRST,
                                        SMW_TYPE_RESPONSE,
                                        test_node->node_id /* src */,
                                        SDF_SDFMSG /* src service */,
                                        test_node->node_id /* dest */,
                                        entry->src_service, SDF_MSG_ERROR,
                                        &entry->resp_mbx_for_synthetic_reply);
        plat_assert(response_wrapper);
    }

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "send a error response");
    rtn_msg_entry_response(entry, response_wrapper);
}

/** @brief Deliver response for given entry which is a request */
static void
rtn_msg_entry_response(struct rtn_msg_entry *entry,
                       struct sdf_msg_wrapper *response_wrapper) {
    plat_assert_imply(response_wrapper, entry->response_expected &&
                      !entry->response_delivered);

    if (response_wrapper) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                     "send msg response to %p", entry->ar_mbx);
        sdf_fth_mbx_deliver_resp_wrapper(entry->ar_mbx, response_wrapper);
        entry->response_delivered = 1;
    }

    rtn_msg_entry_free(entry);
}

/** @brief Free msg entry, removing from all containing structures */
static void
rtn_msg_entry_free(struct rtn_msg_entry *entry) {
    /* grab because entry may go away when ref count hits zero */
    int references = entry->references;

    if (references & RTNME_REF_MSG_OUTBOUND) {
        rtn_msg_entry_remove_outbound(entry);
    }
    if (references & RTNME_REF_MSG_REQUESTS) {
        rtn_msg_entry_remove_requests(entry);
    }
    if (references & RTNME_REF_MSG_TIMEOUT_EVENT) {
        rtn_msg_entry_remove_timeout(entry);
    }
}

/** Remove entry from test_node->msg_outbound */
static void
rtn_msg_entry_remove_outbound(struct rtn_msg_entry *entry) {
    struct replication_test_node *test_node = entry->test_node;
    struct rtn_per_node *per_node = test_node->per_node[entry->dest_node];
    plat_event_free_done_t free_done_cb;

    plat_assert(entry->references & RTNME_REF_MSG_OUTBOUND);
    plat_assert(test_node->msg_outbound.total_queue_size);
    plat_assert(!TAILQ_EMPTY(&per_node->msg_outbound.queue));
    plat_assert(per_node->msg_outbound.queue_size > 0);

    free_done_cb =
        plat_event_free_done_create(test_node->closure_scheduler,
                                    &rtn_event_free_cb,
                                    test_node);

    if (entry->dest_node == test_node->msg_outbound.next_delivery_node &&
        entry == TAILQ_FIRST(&per_node->msg_outbound.queue) &&
        test_node->msg_outbound.next_delivery_event) {
        ++test_node->ref_count;
        plat_event_free(test_node->msg_outbound.next_delivery_event,
                        free_done_cb);
        test_node->msg_outbound.next_delivery_event = NULL;
        rtn_next_delivery_node(test_node);
    }

    TAILQ_REMOVE(&per_node->msg_outbound.queue, entry,
                 msg_outbound_queue_entry);
    --per_node->msg_outbound.queue_size;
    --test_node->msg_outbound.total_queue_size;

    if (test_node->msg_outbound.total_queue_size &&
        !test_node->msg_outbound.next_delivery_event &&
        test_node->state == RTN_STATE_LIVE) {
        rtn_reset_outbound_timer(test_node);
    }

    plat_assert_iff(TAILQ_EMPTY(&per_node->msg_outbound.queue),
                    !per_node->msg_outbound.queue_size);
    plat_assert_imply(!test_node->msg_outbound.total_queue_size,
                      !test_node->msg_outbound.next_delivery_event);
    plat_assert_imply(!test_node->msg_outbound.total_queue_size,
                      TAILQ_EMPTY(&per_node->msg_outbound.queue));

    rtn_msg_entry_release(entry, RTNME_REF_MSG_OUTBOUND);
}

static void
rtn_event_free_cb(plat_closure_scheduler_t *context, void *env) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;

    rtn_ref_count_dec(test_node);
}

/** @brief Remove entry from test_node->msg_requests */
static void
rtn_msg_entry_remove_requests(struct rtn_msg_entry *entry) {
    struct rtn_msg_entry *removed_entry;
    struct replication_test_node *test_node = entry->test_node;

    plat_assert(entry->references & RTNME_REF_MSG_REQUESTS);
    plat_assert(!TAILQ_EMPTY(&test_node->msg_requests.queue));

    TAILQ_REMOVE(&test_node->msg_requests.queue, entry,
                 msg_requests_queue_entry);
    plat_log_msg(LOG_ID, LOG_CAT_MKEY, LOG_DBG, "remove mkey:%s from node:%d",
                 entry->mkey, (int)test_node->node_id);
    removed_entry = HashMap_remove(test_node->msg_requests.map, entry->mkey);
    plat_assert(removed_entry == entry);

    rtn_msg_entry_release(entry, RTNME_REF_MSG_REQUESTS);
}

/** @brief Cancel entry->timeout_event */
static void
rtn_msg_entry_remove_timeout(struct rtn_msg_entry *entry) {
    struct replication_test_node *test_node = entry->test_node;
    plat_event_free_done_t free_done_cb;
    struct plat_event *event;

    plat_assert(entry->references & RTNME_REF_MSG_TIMEOUT_EVENT);
    plat_assert(entry->timeout_event);

    event = entry->timeout_event;
    entry->timeout_event = NULL;

    entry->references |= RTNME_REF_MSG_TIMEOUT_EVENT_FREE;
    ++test_node->ref_count;

    free_done_cb =
        plat_event_free_done_create(test_node->closure_scheduler,
                                    &rtn_msg_entry_remove_timeout_cb,
                                    entry);
    struct timeval now;
    plat_closure_apply(plat_timer_dispatcher_gettime,
                       &entry->test_node->component_api.gettime, &now);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE, "free a timeout event, now:%d, %d",
                 (int)now.tv_sec, (int)now.tv_usec);

    plat_event_free(event, free_done_cb);

    rtn_msg_entry_release(entry, RTNME_REF_MSG_TIMEOUT_EVENT);
}

static void
rtn_msg_entry_remove_timeout_cb(plat_closure_scheduler_t *context, void *env) {
    struct rtn_msg_entry *entry = (struct rtn_msg_entry *)env;
    struct replication_test_node *test_node = entry->test_node;

    plat_assert(!(entry->references & RTNME_REF_MSG_TIMEOUT_EVENT));
    plat_assert(entry->references & RTNME_REF_MSG_TIMEOUT_EVENT_FREE);

    rtn_msg_entry_release(entry, RTNME_REF_MSG_TIMEOUT_EVENT_FREE);

    rtn_ref_count_dec(test_node);
}

/** @brief Release a reference for the entry and free on no references */
static void
rtn_msg_entry_release(struct rtn_msg_entry *entry, enum rtn_msg_entry_ref ref) {
    plat_assert(entry->references & ref);

    entry->references &= ~ref;
    if (!entry->references) {
        if (entry->msg_wrapper) {
            sdf_msg_wrapper_ref_count_dec(entry->msg_wrapper);
            entry->msg_wrapper = NULL;
        }
        plat_free(entry);
    }
}

/** @brief Called when any asynchronous crash or shutdown event completes */
static void
rtn_crash_or_shutdown_event_cb(plat_closure_scheduler_t *context, void *env) {
    struct replication_test_node *test_node =
            (struct replication_test_node *)env;

    plat_assert(test_node->state == RTN_STATE_TO_DEAD ||
                test_node->state == RTN_STATE_TO_SHUTDOWN);
    rtn_ref_count_dec(test_node);
}

static void
rtn_ref_count_dec(struct replication_test_node *test_node) {
    plat_assert(test_node->ref_count > 0);
    --test_node->ref_count;

    if (!test_node->ref_count) {
        rtn_ref_count_zero(test_node);
    }
}

/** @brief Called when any asynchronous crash or shutdown event count is 0 */
static void
rtn_ref_count_zero(struct replication_test_node *test_node) {
    plat_assert(!test_node->ref_count);

    if (test_node->state == RTN_STATE_TO_DEAD) {
        test_node->state = RTN_STATE_DEAD;
        rtn_crash_or_shutdown_complete(test_node);
    } else if (test_node->state == RTN_STATE_TO_SHUTDOWN) {
        if (!test_node->flash_shutdown_started) {
            /* Flash shutdown was deferred because it was crashing */
            rtn_shutdown_flash(test_node);
        /* Closure scheduler is last thing shutdown */
        } else if (!test_node->closure_scheduler_shutdown_started) {
            rtn_shutdown_closure_scheduler(test_node);
        } else {
            test_node->state = RTN_STATE_SHUTDOWN;
            rtn_crash_or_shutdown_complete(test_node);
        }
    }
}

static void
rtn_crash_or_shutdown_complete(struct replication_test_node *test_node) {
    replication_test_node_crash_async_cb_t crash_cb;
    replication_test_node_shutdown_async_cb_t shutdown_cb;
    vnode_t node_id;

    crash_cb = test_node->crash_cb;
    test_node->crash_cb = replication_test_node_crash_async_cb_null;
    shutdown_cb = test_node->shutdown_cb;
    test_node->shutdown_cb = replication_test_node_shutdown_async_cb_null;

    test_node->replicator = NULL;

    if (!replication_test_node_crash_async_cb_is_null(&crash_cb) ||
        test_node->state == RTN_STATE_DEAD) {
        rtn_crash_do_cb(test_node, crash_cb, SDF_SUCCESS);
    }

    if (test_node->state == RTN_STATE_SHUTDOWN) {
        node_id = test_node->node_id;

        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "node_id %"PRIu32" shutdown",
                     test_node->node_id);

        rtn_free(test_node);

        plat_closure_apply(replication_test_node_shutdown_async_cb,
                           &shutdown_cb);
    }
}

static void
rtn_shutdown_closure_scheduler(struct replication_test_node *test_node) {
    plat_closure_scheduler_shutdown_t cb;

    plat_assert(test_node->state == RTN_STATE_TO_SHUTDOWN);

    cb = plat_closure_scheduler_shutdown_create(test_node->closure_scheduler,
                                                &rtn_closure_scheduler_shutdown_cb,
                                                test_node);

    test_node->closure_scheduler_shutdown_started = 1;
    ++test_node->ref_count;
    plat_closure_scheduler_shutdown(test_node->closure_scheduler, cb);
}


static void
rtn_closure_scheduler_shutdown_cb(plat_closure_scheduler_t *context,
                                  void *env) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;

    /* This became invalid at some point before this call */
    test_node->closure_scheduler = NULL;

    /* Share common event reference counting code */
    rtn_crash_or_shutdown_event_cb(context, env);
}

/**
 * @brief Called to actually free the replication_test_node
 *
 * The two ways this can be freed are via shutdown (where all asynchronous
 * shutdown/free combinations have run to completion and their fields set
 * to NULL) or free before the closure scheduler was started.
 */
static void
rtn_free(struct replication_test_node *test_node) {
    int i;
    plat_assert(test_node->state == RTN_STATE_SHUTDOWN ||
                (test_node->state == RTN_STATE_INITIAL &&
                 !test_node->closure_scheduler));

    /* Shutdown was asynchronous */
    plat_assert(!test_node->closure_scheduler);
    plat_assert(!test_node->ref_count);
    plat_assert(!test_node->msg_outbound.next_delivery_event);

    plat_free(test_node->liveness_ping_event_name);
    plat_free(test_node->msg_outbound.next_delivery_event_name);

    if (test_node->msg_requests.map) {
        HashMap_destroy(test_node->msg_requests.map);
    }
    if (test_node->per_node) {
        for (i = 0; i < test_node->test_config.nnode; ++i) {
            if (test_node->per_node[i]) {
                rtn_per_node_free(test_node->per_node[i]);
            }
        }
        plat_free(test_node->per_node);
    }
    plat_assert(!test_node->replicator);
    plat_assert(!test_node->flash);
    plat_free(test_node);
}

void
rtn_receive_msg(struct replication_test_node *test_node,
                struct sdf_msg_wrapper *msg_wrapper) {
    rtn_do_receive_msg_t do_recv_msg =
        rtn_do_receive_msg_create(test_node->closure_scheduler,
                                  &rtn_receive_msg_impl, test_node);
    plat_closure_apply(rtn_do_receive_msg, &do_recv_msg, msg_wrapper);
    /* After, so do_start is most likely to execute first */
    rtn_guarantee_bootstrapped(test_node);
}

static void
rtn_receive_msg_impl(plat_closure_scheduler_t *context, void *env,
                     struct sdf_msg_wrapper *msg_wrapper) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    struct sdf_msg *request_msg;
    struct rtn_msg_entry *msg_entry;
    char *mkey;

    /* State must be at least RTN_STATE_DEAD before getting here */
    plat_assert(test_node->state != RTN_STATE_INITIAL);

    rtn_log_msg_wrapper(test_node, msg_wrapper, "rtn_receive_msg_impl",
                        LOG_CAT_MSG, LOG_TRACE);
    request_msg = NULL;
    sdf_msg_wrapper_rwref(&request_msg, msg_wrapper);

    /* Eat messages; sending side must have timeouts */
    /* if (test_node->state != RTN_STATE_LIVE || test_node->net_state != RTN_NET_UP) { */
    if (test_node->state != RTN_STATE_LIVE) {
        sdf_msg_wrapper_rwrelease(&request_msg, msg_wrapper);
        sdf_msg_wrapper_ref_count_dec(msg_wrapper);
    } else if (request_msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_INCLUDED) {
        plat_log_msg(LOG_ID, LOG_CAT_MKEY, LOG_DBG, "get mkey:%lx from node:%d",
                     request_msg->sent_id, (int)test_node->node_id);
        plat_asprintf(&mkey, "%lx", request_msg->sent_id);
        plat_assert(mkey);
        msg_entry = HashMap_get(test_node->msg_requests.map, mkey);
        plat_free(mkey);
        sdf_msg_wrapper_rwrelease(&request_msg, msg_wrapper);
        if (msg_entry) {
            rtn_msg_entry_response(msg_entry, msg_wrapper);
        } else {
            /*
             * XXX: drew 2009-05-20 There are probably situations in which
             * a less severe message is appropriate (race condition with
             * a timeout) but they may be hard to detect programatically.
             *
             * Maybe we have a substantially longer timeout for garbage
             * collecting entries?
             */
            rtn_log_msg_wrapper(test_node, msg_wrapper,
                                "rtn_receive_msg_impl no response handler",
                                LOG_CAT_MSG, LOG_INFO);
            sdf_msg_wrapper_ref_count_dec(msg_wrapper);
        }
    } else if (request_msg->msg_dest_service == SDF_FLSH) {
        sdf_msg_wrapper_rwrelease(&request_msg, msg_wrapper);
        rtf_receive_msg(test_node->flash, msg_wrapper);
    } else if (request_msg->msg_dest_service == SDF_REPLICATION &&
               test_node->test_config.test_type == RT_TYPE_REPLICATOR) {
        plat_assert(test_node->replicator);
        sdf_msg_wrapper_rwrelease(&request_msg, msg_wrapper);
        sdf_replicator_receive_msg(test_node->replicator, msg_wrapper);
    } else if (request_msg->msg_dest_service == SDF_REPLICATION_PEER ||
               request_msg->msg_dest_service == SDF_REPLICATION_PEER_META_SUPER ||
               request_msg->msg_dest_service == SDF_REPLICATION_PEER_META_CONSENSUS) {
        sdf_msg_wrapper_rwrelease(&request_msg, msg_wrapper);
        switch (test_node->test_config.test_type) {
        case RT_TYPE_REPLICATOR:
            sdf_replicator_receive_msg(test_node->replicator, msg_wrapper);
            break;
        case RT_TYPE_META_STORAGE:
            rms_receive_msg(test_node->meta_storage, msg_wrapper);
            break;
        }
    } else {
        rtn_log_msg_wrapper(test_node, msg_wrapper,
                            "rtn_receive_msg_impl msg not handled",
                            LOG_CAT_MSG, LOG_WARN);
        sdf_msg_wrapper_rwrelease(&request_msg, msg_wrapper);
        sdf_msg_wrapper_ref_count_dec(msg_wrapper);
    }
}

SDF_status_t
rtn_get_op_meta(struct replication_test_node *test_node,
                const struct SDF_container_meta *container_meta,
                SDF_shardid_t shard, struct sdf_replication_op_meta *op_meta) {
    fthWaitEl_t *unlock;
    SDF_status_t ret = SDF_FAILURE /* silence gcc */;

    /* freeze state (and replicator validity) for duration of call */
    unlock = fthLock(&test_node->replicator_op_lock,
                     0 /* read lock */, NULL);

    switch (test_node->state) {
    case RTN_STATE_LIVE:
        ret = sdf_replicator_get_op_meta(test_node->replicator,
                                         container_meta, shard, op_meta);
        break;
    case RTN_STATE_INITIAL:
    case RTN_STATE_DEAD:
    case RTN_STATE_TO_DEAD:
        ret = SDF_NODE_DEAD;
        break;
    case RTN_STATE_SHUTDOWN:
    case RTN_STATE_TO_SHUTDOWN:
        ret = SDF_SHUTDOWN;
        break;
    }
    fthUnlock(unlock);

    return (ret);
}

void
rtn_set_replicator_notification_cb(struct replication_test_node *test_node,
                                   rtfw_replicator_notification_cb_t cb) {
    test_node->replicator_notification_user_cb = cb;
}

void
rtn_command_async(struct replication_test_node *test_node, SDF_shardid_t shard,
                  const char *command_arg, sdf_replicator_command_cb_t cb) {
    rtn_do_command_t do_command =
        rtn_do_command_create(test_node->closure_scheduler,
                              &rtn_command_impl, test_node);
    /* Copy so caller can free command_arg */
    char *command = plat_strdup(command_arg);

    plat_assert(command);

    plat_closure_apply(rtn_do_command, &do_command,
                       shard, command, cb);

    /* After, so do_start is most likely to execute first */
    rtn_guarantee_bootstrapped(test_node);
}

static void
rtn_command_impl(plat_closure_scheduler_t *context, void *env,
                 SDF_shardid_t shard, char *command,
                 sdf_replicator_command_cb_t cb) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    SDF_status_t status = SDF_FAILURE /* silence gcc */;
    char *output;

    /* State must be at least RTN_STATE_DEAD before getting here */
    plat_assert(test_node->state != RTN_STATE_INITIAL);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "node_id %lld rtn_command_impl shard 0x%lx command '%s'",
                 (long long)test_node->node_id, (long)shard, command);

    switch (test_node->state) {
    case RTN_STATE_LIVE:
        sdf_replicator_command_async(test_node->replicator, shard, command, cb);
        status = SDF_SUCCESS;
        break;
    case RTN_STATE_INITIAL:
    case RTN_STATE_DEAD:
    case RTN_STATE_TO_DEAD:
        status = SDF_NODE_DEAD;
        break;
    case RTN_STATE_SHUTDOWN:
    case RTN_STATE_TO_SHUTDOWN:
        status = SDF_SHUTDOWN;
        break;
    }

    plat_free(command);

    if (status != SDF_SUCCESS) {
        plat_asprintf(&output, "SERVER_ERROR %s\r\n",
                      sdf_status_to_string(status));
        plat_closure_apply(sdf_replicator_command_cb, &cb, status, output);
    }
}



static void
rtn_replicator_notification_cb(plat_closure_scheduler_t *context, void *env,
                               int events, struct cr_shard_meta *shard_meta,
                               enum sdf_replicator_access access,
                               struct timeval expires,
                               sdf_replicator_notification_complete_cb_t
                               complete_cb) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;

    if (rtfw_replicator_notification_cb_is_null(&test_node->replicator_notification_user_cb) ||
        test_node->state != RTN_STATE_LIVE) {
        cr_shard_meta_free(shard_meta);
        plat_closure_apply(sdf_replicator_notification_complete_cb,
                           &complete_cb);
    } else {
        plat_closure_apply(rtfw_replicator_notification_cb,
                           &test_node->replicator_notification_user_cb,
                           events, shard_meta, access, expires,
                           complete_cb, test_node->node_id);
    }
}

static void
rtn_send_msg_api_cb(plat_closure_scheduler_t *context, void *env,
                    struct sdf_msg_wrapper *msg_wrapper,
                    struct sdf_fth_mbx *ar_mbx, SDF_status_t *status) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;

    rtn_send_msg(test_node, msg_wrapper, ar_mbx);

    /*
     * XXX: drew 2009-05-10 All failures should now generate messages so
     * status is deprecated.
     */
    if (status) {
        *status = SDF_SUCCESS;
    }
}


void
rtn_send_msg(struct replication_test_node *test_node,
             struct sdf_msg_wrapper *msg_wrapper,
             struct sdf_fth_mbx *ar_mbx) {
    rtn_do_send_msg_t do_send =
        rtn_do_send_msg_create(test_node->closure_scheduler,
                               &rtn_send_msg_impl, test_node);

    plat_closure_apply(rtn_do_send_msg, &do_send, msg_wrapper, ar_mbx);
    /* After, so do_start is most likely to execute first */
    rtn_guarantee_bootstrapped(test_node);
}

/* @brief Actual implementation of #rtn_send_msg and #rtn_send_msg_api_cb */
static void
rtn_send_msg_impl(plat_closure_scheduler_t *context, void *env,
                  struct sdf_msg_wrapper *msg_wrapper,
                  struct sdf_fth_mbx *ar_mbx) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    struct rtn_msg_entry *entry;
    struct sdf_msg_wrapper *send_wrapper __attribute__((unused));
    struct sdf_msg *send_msg;
    long long seqno;

    /* FIXME: zhenwei, 2009-07-08, disable message flag checker now */
#if 0
    if ((msg_wrapper->msg_wrapper_type & SMW_TYPE_RESPONSE) &&
        !(msg_wrapper->ptr.local.ptr->msg_flags & SDF_MSG_FLAG_MBX_RESP_INCLUDED)) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "src_service: %d, dest_service: %d, type: %d",
                     msg_wrapper->src_service,
                     msg_wrapper->dest_service, msg_wrapper->msg_wrapper_type);
        plat_abort();
    }

    if ((msg_wrapper->ptr.local.ptr->msg_flags & SDF_MSG_FLAG_MBX_RESP_INCLUDED) &&
        ar_mbx) {
        rtn_log_msg_wrapper(test_node, msg_wrapper,
                            "rtn_send_msg_impl illegal message flags ",
                            LOG_CAT_MSG, LOG_FATAL);

        plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                     "src_service: %d, dest_service: %d, type: %d",
                     msg_wrapper->src_service,
                     msg_wrapper->dest_service, msg_wrapper->msg_wrapper_type);
        plat_abort();
    }
    plat_assert_imply(msg_wrapper->msg_wrapper_type & SMW_TYPE_RESPONSE, !ar_mbx);
#endif

    send_msg = sdf_msg_wrapper_to_send_msg_alloc(msg_wrapper);
    plat_assert(send_msg);

    seqno = __sync_fetch_and_add(&test_node->msg_seqno, 1);

    if (ar_mbx) {
        send_msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;
        if (send_msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_INCLUDED) {
            plat_log_msg(LOG_ID, LOG_CAT, LOG_ERR,
                         "\n\n WTF  msg_flags 0x%x\n", send_msg->msg_flags);
            send_msg->msg_flags &= ~SDF_MSG_FLAG_MBX_RESP_INCLUDED;
        }

        send_msg->sent_id = seqno;
    }

    /**
     * FIXME: zhenwei, 2009-07-08, move assert_imply here since
     * replicator does not prepare well for message flags
     */
    /*
     * Replies and responses can't be mixed because there's only one mkey
     * field.  There are other uses of ar_mbx (message disposition on send)
     * but they're unused.
     */
    plat_assert_imply(send_msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_INCLUDED,
                      !ar_mbx);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                 "\nNode %d: dn %d ss %d ds %d msg_flags 0x%x mkey %lx\n",
                 send_msg->msg_src_vnode, send_msg->msg_dest_vnode,
                 send_msg->msg_src_service, send_msg->msg_dest_service,
                 send_msg->msg_flags, send_msg->sent_id);

    plat_calloc_struct(&entry);
    plat_assert(entry);

    entry->test_node = test_node;
    entry->references = RTNME_REF_MSG_LOCAL;

    entry->src_service = send_msg->msg_src_service;
    entry->dest_node = send_msg->msg_dest_vnode;
    entry->dest_service = send_msg->msg_dest_service;

    entry->response_expected = ar_mbx ? 1 : 0;

    if (entry->response_expected) {
        sdf_msg_get_response(send_msg, &entry->resp_mbx_for_synthetic_reply);
    }

    /*
     * XXX: drew 2009-01-12 this currently causes leaks.
     *
     * The problem is that the current message layer allows one to
     * call sdf_msg_buff_free immediately, except according to Tom
     * Riddle it defers the actual free by sending the buffer on a
     * queue to the messaging thread which may defer freeing it until
     * after MPI is done with the message.
     */

    entry->msg_wrapper = sdf_msg_wrapper_recv(send_msg);

    rtn_log_msg_wrapper(test_node, entry->msg_wrapper,
                        "rtn_send_msg_impl", LOG_CAT_MSG, LOG_TRACE);

#ifdef NETWORK_DELAY
    entry->us_latency = plat_prng_next_int(test_node->component_api.prng,
                                           test_node->test_config.network_timing.max_delay_us);
#endif

    entry->ar_mbx = ar_mbx;
    snprintf(entry->mkey, MSG_KEYSZE, "%lx", send_msg->sent_id);

    switch (test_node->state) {
    case RTN_STATE_LIVE:
            if (entry->dest_node != test_node->node_id &&
                test_node->per_node[entry->dest_node]->liveness.state !=
                RTNL_LIVE) {
                rtn_msg_entry_error(test_node, entry, SDF_NODE_DEAD);
            } else {
                /*
                 * XXX: drew 2009-01-12 We should special case local messages so
                 * they can be delivered quicker.
                 */
                rtn_msg_entry_add_outbound(entry);
                rtn_msg_entry_add_requests(entry);
                /*
                 * XXX: drew 2009-05-22 NEVER EVER COMMENT OUT CODE YOU DO NOT
                 * UNDERSTAND AND LEAVE NO INDICATION AS TO THE REASON.  THE
                 * TIMEOUT IS CONFIGURABLE.  CHANGE IT FOR TESTS THAT BREAK.
                 * DO NOT SILENTLY DISABLE THINGS!
                 */
                rtn_msg_entry_add_timeout(entry);
            }
#ifndef NETWORK_DELAY
            rtn_deliver_msg(test_node);
#endif /* def NETWORK_DELAY */
        break;
    case RTN_STATE_DEAD:
    case RTN_STATE_TO_DEAD:
        rtn_msg_entry_error(test_node, entry, SDF_TEST_CRASH);
        break;

    case RTN_STATE_SHUTDOWN:
    case RTN_STATE_TO_SHUTDOWN:
        rtn_msg_entry_error(test_node, entry, SDF_SHUTDOWN);
        break;

    case RTN_STATE_INITIAL:
        plat_assert_always(0);
    }

    /* Ref count goes to zero if it wasn't handled */
    rtn_msg_entry_release(entry, RTNME_REF_MSG_LOCAL);
}

static void
rtn_msg_entry_add_outbound(struct rtn_msg_entry *entry) {
    struct replication_test_node *test_node = entry->test_node;
    struct rtn_per_node *per_node = test_node->per_node[entry->dest_node];
    int was_empty;

    plat_assert(test_node->state == RTN_STATE_LIVE);
    plat_assert_imply(entry->response_expected, entry->ar_mbx);
    plat_assert(entry->dest_node < test_node->test_config.nnode);

    plat_assert(per_node);

    was_empty = !test_node->msg_outbound.total_queue_size;

    plat_assert_iff(was_empty, !test_node->msg_outbound.next_delivery_event);
    plat_assert_imply(was_empty, TAILQ_EMPTY(&per_node->msg_outbound.queue));

    TAILQ_INSERT_TAIL(&per_node->msg_outbound.queue, entry,
                      msg_outbound_queue_entry);
    ++per_node->msg_outbound.queue_size;
    ++test_node->msg_outbound.total_queue_size;

    entry->references |= RTNME_REF_MSG_OUTBOUND;

#ifdef NETWORK_DELAY
    if (was_empty) {
        test_node->msg_outbound.next_delivery_node = entry->dest_node;
        /* FIXME: We should dequeue immediately if we aren't doing the delay */
        rtn_reset_outbound_timer(test_node);
    }
#else /* def NETWORK_DELAY */
    /* The previous message should have been immediately dequeued */
    plat_assert(was_empty);
#endif /* else def NETWORK_DELAY */
}

static void
rtn_msg_entry_add_requests(struct rtn_msg_entry *entry) {
    struct replication_test_node *test_node = entry->test_node;
    char *mkey;
    SDF_status_t status;

    plat_assert(test_node->state == RTN_STATE_LIVE);

    if (entry->response_expected) {
        TAILQ_INSERT_TAIL(&test_node->msg_requests.queue, entry,
                          msg_requests_queue_entry);
        plat_asprintf(&mkey, "%s", entry->mkey);
        plat_assert(mkey);
        plat_log_msg(LOG_ID, LOG_CAT_MKEY, LOG_DBG, "put mkey:%s into node:%d",
                     mkey, (int)test_node->node_id);
        status = HashMap_put(test_node->msg_requests.map, mkey, entry);
        plat_assert(status == SDF_TRUE);

        entry->references |= RTNME_REF_MSG_REQUESTS;
    }
}

static void
rtn_msg_entry_add_timeout(struct rtn_msg_entry *entry) {
    struct replication_test_node *test_node = entry->test_node;
    struct timeval when;
    plat_event_fired_t fired;

    plat_assert(test_node->state == RTN_STATE_LIVE);

    if (entry->response_expected && entry->ar_mbx->timeout_usec > 0) {
        when.tv_sec = entry->ar_mbx->timeout_usec / MILLION;
        when.tv_usec = entry->ar_mbx->timeout_usec % MILLION;

        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "timeout_u:%lu, sec:%lu, usec:%lu",
                     entry->ar_mbx->timeout_usec, when.tv_sec, when.tv_usec);
        fired = plat_event_fired_create(test_node->closure_scheduler,
                                        &rtn_msg_entry_timeout_cb, entry);

        entry->timeout_event =
            plat_timer_dispatcher_timer_alloc(test_node->component_api.timer_dispatcher,
                                              "rtn timeout", LOG_CAT_EVENT,
                                              fired, 1 /* free_count */,
                                              &when, PLAT_TIMER_RELATIVE,
                                              NULL /* rank_ptr */);

        entry->references |= RTNME_REF_MSG_TIMEOUT_EVENT;
    }
}

static void
rtn_msg_entry_timeout_cb(plat_closure_scheduler_t *context, void *env,
                         struct plat_event *event) {
    struct rtn_msg_entry *entry = (struct rtn_msg_entry *)env;
    struct replication_test_node *test_node = entry->test_node;


    plat_log_msg(LOG_ID, LOG_CAT, LOG_INFO,
                 "timeout cb trigger");
    rtn_msg_entry_error(test_node, entry, SDF_TIMEOUT);
}

/**
 * @brief Advance to next node with message traffic to deliver
 *
 * @return SDF_SUCCESS on success, otherwise on failure (notably
 * no messages available)
 */
static SDF_status_t
rtn_next_delivery_node(struct replication_test_node *test_node) {
    int i;

    for (i = (test_node->msg_outbound.next_delivery_node + 1) %
         test_node->test_config.nnode;
         i != test_node->msg_outbound.next_delivery_node &&
         TAILQ_EMPTY(&test_node->per_node[i]->msg_outbound.queue);
         i = (i + 1) % test_node->test_config.nnode) {
    }

    test_node->msg_outbound.next_delivery_node = i;

    return (!TAILQ_EMPTY(&test_node->per_node[i]->msg_outbound.queue) ?
            SDF_SUCCESS : SDF_FAILURE);
}

/**
 * @brief Schedule the next outbound message
 *
 * @param test_node <IN> test_node with
 * test_node->msg_outbound.next_delivery_node referring to
 * a test_node->per_node entry with non-empty
 * #rtn_per_node.msg_outbound.queue.
 */
static void
rtn_reset_outbound_timer(struct replication_test_node *test_node) {
    struct rtn_per_node *per_node;

    plat_assert(test_node->state == RTN_STATE_LIVE);
    plat_assert(!test_node->msg_outbound.next_delivery_event);

    per_node = test_node->per_node[test_node->msg_outbound.next_delivery_node];
    plat_assert(per_node);
    plat_assert(!TAILQ_EMPTY(&per_node->msg_outbound.queue));

#ifdef NETWORK_DELAY
    struct timeval now;
    plat_event_fired_t fired;
    struct rtn_msg_entry *entry;
    struct timeval when;

    entry = TAILQ_FIRST(&per_node->msg_outbound.queue);

    plat_closure_apply(plat_timer_dispatcher_gettime,
                       &test_node->component_api.gettime, &now);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                 "node_id %lld NOW:usec:%lu, sec:%lu, nw_ltcy:%"PRIu32,
                 (long long)test_node->node_id,
                 now.tv_usec, now.tv_sec, entry->us_latency);

    when.tv_sec = (entry->us_latency)/ MILLION;
    when.tv_usec = (entry->us_latency) % MILLION;

    fired = plat_event_fired_create(test_node->closure_scheduler,
                                    &rtn_next_delivery_cb, test_node);
    test_node->msg_outbound.next_delivery_event =
            plat_timer_dispatcher_timer_alloc(test_node->component_api.timer_dispatcher,
                                              test_node->msg_outbound.next_delivery_event_name,
                                              LOG_CAT_EVENT, fired,
                                              1 /* free_count */, &when,
                                              PLAT_TIMER_RELATIVE,
                                              NULL /* rank_ptr */);
    plat_assert(test_node->msg_outbound.next_delivery_event);
#endif /* def NETWORK_DELAY */
}

#ifdef NETWORK_DELAY
/** @brief Timer fired (all common code) */
static void
rtn_next_delivery_cb(plat_closure_scheduler_t *context, void *env,
                     struct plat_event *event) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    rtn_deliver_msg(test_node);
}
#endif

/**
 * @brief Deliver first message on msg_outbound.queue
 *
 * @param test_node <IN> test_node with
 * test_node->msg_outbound.next_delivery_node referring to
 * a test_node->per_node entry with non-empty
 * #rtn_per_node.msg_outbound.queue.
 */
static void
rtn_deliver_msg(struct replication_test_node *test_node) {
    struct replication_test_framework *framework = test_node->framework;
    struct rtn_per_node *per_node;
    SDF_status_t status;
    struct rtn_msg_entry *entry;
    struct sdf_msg_wrapper *msg_wrapper;

    per_node = test_node->per_node[test_node->msg_outbound.next_delivery_node];
    plat_assert(per_node);
    /*
     * XXX: drew 2010-04-02 We previous allowed an empty queue
     * but don't seem to have used the feature.  Find out whether
     * or not that's the case.  If not, we have to iterate over
     * the queues.
     *
     * Only working in the non-empty case is consistent with
     * #rtn_reset_outbound_timer
     */
    plat_assert(!TAILQ_EMPTY(&per_node->msg_outbound.queue));

    if (!TAILQ_EMPTY(&per_node->msg_outbound.queue)) {
        entry = TAILQ_FIRST(&per_node->msg_outbound.queue);
        msg_wrapper = entry->msg_wrapper;
        entry->msg_wrapper = NULL;
        /* XXX: drew 2009-01-13 should uniquely identify message sent */
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "node_id %"PRIu32" send msg, entry ptr:%p",
                     test_node->node_id, entry);
        status = rtfw_receive_message(framework, msg_wrapper);
        if (SDF_SUCCESS != status) {
            /* XXX: drew 2010-04-09 should uniquely identify message sent */
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DIAG,
                         "node_id %lld rtfw_receive_message failed: %s",
                         (long long)test_node->node_id,
                         sdf_status_to_string(status));
            rtn_msg_entry_error(test_node, entry, status);
        } else {
            rtn_msg_entry_remove_outbound(entry);
        }
    }
}

void
rtn_set_meta_storage_cb(struct replication_test_node *test_node,
                        rtfw_shard_meta_cb_t update_cb) {
    test_node->meta_storage_user_cb = update_cb;
}

static void
rtn_meta_storage_update_cb(plat_closure_scheduler_t *context, void *env,
                           SDF_status_t status,
                           struct cr_shard_meta *shard_meta,
                           struct timeval lease_expires) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;

    if (rtfw_shard_meta_cb_is_null(&test_node->meta_storage_user_cb) ||
        test_node->state != RTN_STATE_LIVE) {
        cr_shard_meta_free(shard_meta);
    } else {
        plat_closure_apply(rtfw_shard_meta_cb,
                           &test_node->meta_storage_user_cb,
                           status, shard_meta, lease_expires,
                           test_node->node_id);
    }
}

void
rtn_create_shard_meta(struct replication_test_node *test_node,
                      const struct cr_shard_meta *cr_shard_meta_arg,
                      rtfw_shard_meta_cb_t user_cb) {
    struct cr_shard_meta *cr_shard_meta;
    rtn_do_put_shard_meta_t do_put_shard_meta =
        rtn_do_put_shard_meta_create(test_node->closure_scheduler,
                                     &rtn_put_shard_meta_impl, test_node);

    cr_shard_meta = cr_shard_meta_dup(cr_shard_meta_arg);
    plat_assert(cr_shard_meta);

    plat_closure_apply(rtn_do_put_shard_meta, &do_put_shard_meta, cr_shard_meta,
                       user_cb, RTN_PSM_CREATE);
    /* After, so do_start is most likely to execute first */
    rtn_guarantee_bootstrapped(test_node);
}

void
rtn_get_shard_meta(struct replication_test_node *test_node,
                   SDF_shardid_t sguid,
                   const struct sdf_replication_shard_meta *shard_meta_arg,
                   rtfw_shard_meta_cb_t user_cb) {
    struct sdf_replication_shard_meta *shard_meta;
    rtn_do_get_shard_meta_t do_get_shard_meta =
        rtn_do_get_shard_meta_create(test_node->closure_scheduler,
                                     &rtn_get_shard_meta_impl, test_node);

    plat_calloc_struct(&shard_meta);
    plat_assert(shard_meta);
    *shard_meta = *shard_meta_arg;

    plat_closure_apply(rtn_do_get_shard_meta, &do_get_shard_meta, sguid,
                       shard_meta, user_cb);
    /* After, so do_start is most likely to execute first */
    rtn_guarantee_bootstrapped(test_node);
}

static void
rtn_get_shard_meta_impl(plat_closure_scheduler_t *context, void *env,
                        SDF_shardid_t sguid,
                        const struct sdf_replication_shard_meta *shard_meta,
                        rtfw_shard_meta_cb_t user_cb) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    struct rtn_meta_storage_request *request;
    rms_shard_meta_cb_t meta_cb;

    request = rtn_meta_storage_request_start(test_node, user_cb);
    meta_cb = rms_shard_meta_cb_create(test_node->closure_scheduler,
                                       &rtn_meta_storage_request_cb, request);
    if (request) {
        rms_get_shard_meta(test_node->meta_storage, sguid, shard_meta, meta_cb);
    }

    plat_free((void *)shard_meta);
}

void
rtn_put_shard_meta(struct replication_test_node *test_node,
                   const struct cr_shard_meta *cr_shard_meta_arg,
                   rtfw_shard_meta_cb_t user_cb) {
    struct cr_shard_meta *cr_shard_meta;
    rtn_do_put_shard_meta_t do_put_shard_meta =
        rtn_do_put_shard_meta_create(test_node->closure_scheduler,
                                     &rtn_put_shard_meta_impl, test_node);

    cr_shard_meta = cr_shard_meta_dup(cr_shard_meta_arg);
    plat_assert(cr_shard_meta);

    plat_closure_apply(rtn_do_put_shard_meta, &do_put_shard_meta, cr_shard_meta,
                       user_cb, RTN_PSM_PUT);
    /* After, so do_start is most likely to execute first */
    rtn_guarantee_bootstrapped(test_node);
}

static void
rtn_put_shard_meta_impl(plat_closure_scheduler_t *context, void *env,
                        const struct cr_shard_meta *cr_shard_meta,
                        rtfw_shard_meta_cb_t cb,
                        enum rtn_put_shard_meta_mode mode) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    struct rtn_meta_storage_request *request;
    rms_shard_meta_cb_t meta_cb;

    request = rtn_meta_storage_request_start(test_node, cb);
    meta_cb = rms_shard_meta_cb_create(test_node->closure_scheduler,
                                       &rtn_meta_storage_request_cb, request);
    if (request) {
        switch (mode) {
        case RTN_PSM_CREATE:
            rms_create_shard_meta(test_node->meta_storage, cr_shard_meta,
                                  meta_cb);
            break;
        case RTN_PSM_PUT:
            rms_put_shard_meta(test_node->meta_storage, cr_shard_meta, meta_cb);
            break;
        }
    }

    cr_shard_meta_free((struct cr_shard_meta *)cr_shard_meta);
}

void
rtn_delete_shard_meta(struct replication_test_node *test_node,
                      const struct cr_shard_meta *cr_shard_meta_arg,
                      rtfw_shard_meta_cb_t user_cb) {
    struct cr_shard_meta *cr_shard_meta;
    rtn_do_delete_shard_meta_t do_delete_shard_meta =
        rtn_do_delete_shard_meta_create(test_node->closure_scheduler,
                                        &rtn_delete_shard_meta_impl, test_node);

    cr_shard_meta = cr_shard_meta_dup(cr_shard_meta_arg);
    plat_assert(cr_shard_meta);

    plat_closure_apply(rtn_do_delete_shard_meta, &do_delete_shard_meta,
                       cr_shard_meta, user_cb);
    /* After, so do_start is most likely to execute first */
    rtn_guarantee_bootstrapped(test_node);
}

static void
rtn_delete_shard_meta_impl(plat_closure_scheduler_t *context, void *env,
                           const struct cr_shard_meta *cr_shard_meta,
                           rtfw_shard_meta_cb_t cb) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    struct rtn_meta_storage_request *request;
    rms_shard_meta_cb_t meta_cb;

    request = rtn_meta_storage_request_start(test_node, cb);
    meta_cb = rms_shard_meta_cb_create(test_node->closure_scheduler,
                                       &rtn_meta_storage_request_cb, request);
    if (request) {
        rms_delete_shard_meta(test_node->meta_storage, cr_shard_meta, meta_cb);
    }

    cr_shard_meta_free((struct cr_shard_meta *)cr_shard_meta);
}

struct rtn_meta_storage_request {
    /** @brief Test node associated with this request */
    struct replication_test_node *test_node;

    /** @brief User closure */
    rtfw_shard_meta_cb_t user_cb;
};

static struct rtn_meta_storage_request *
rtn_meta_storage_request_start(struct replication_test_node *test_node,
                               rtfw_shard_meta_cb_t user_cb) {
    struct rtn_meta_storage_request *request;
    struct timeval now;

    plat_assert(test_node->test_config.test_type == RT_TYPE_META_STORAGE);

    if (plat_calloc_struct(&request)) {
        request->test_node = test_node;
        request->user_cb = user_cb;
        ++test_node->ref_count;

        if (test_node->state != RTN_STATE_LIVE) {
            plat_closure_apply(plat_timer_dispatcher_gettime,
                               &test_node->component_api.gettime, &now);
            /**
             * Fixme: zhenwei
             * that's a bug here, env of
             * #rtn_meta_storage_request_cb is request
             */

            rtn_meta_storage_request_cb(NULL /* context */,
                                        /* test_node, */
                                        request,
                                        SDF_TEST_CRASH, NULL /* shard_meta */,
                                        now);
            request = NULL;
        }
    } else {
        plat_closure_apply(plat_timer_dispatcher_gettime,
                           &test_node->component_api.gettime, &now);
        plat_closure_apply(rtfw_shard_meta_cb, &user_cb,
                           SDF_OUT_OF_MEM, NULL /* shard meta */, now,
                           test_node->node_id);
    }

    return (request);
}

static void
rtn_meta_storage_request_cb(plat_closure_scheduler_t *context, void *env,
                            SDF_status_t status,
                            struct cr_shard_meta *shard_meta,
                            struct timeval lease_expires) {
    struct rtn_meta_storage_request *request =
        (struct rtn_meta_storage_request *)env;
    struct replication_test_node *test_node = request->test_node;
    struct timeval now;

    plat_assert(test_node->test_config.test_type == RT_TYPE_META_STORAGE);

    if (rtfw_shard_meta_cb_is_null(&request->user_cb)) {
        cr_shard_meta_free(shard_meta);
    } else {
        plat_closure_apply(plat_timer_dispatcher_gettime,
                           &test_node->component_api.gettime, &now);

        switch (test_node->state) {
        case RTN_STATE_INITIAL:
            plat_assert_always(0);
            break;
        case RTN_STATE_LIVE:
            plat_closure_apply(rtfw_shard_meta_cb, &request->user_cb,
                               status, shard_meta, lease_expires,
                               test_node->node_id);
            break;
        case RTN_STATE_DEAD:
        case RTN_STATE_TO_DEAD:
            plat_closure_apply(rtfw_shard_meta_cb, &request->user_cb,
                               SDF_TEST_CRASH, NULL /* shard meta */, now,
                               test_node->node_id);
            cr_shard_meta_free(shard_meta);
            break;
        case RTN_STATE_TO_SHUTDOWN:
        case RTN_STATE_SHUTDOWN:
            plat_closure_apply(rtfw_shard_meta_cb, &request->user_cb,
                               SDF_SHUTDOWN, NULL /* shard meta */, now,
                               test_node->node_id);
            cr_shard_meta_free(shard_meta);
            break;
        }
    }

    plat_free(request);
    rtn_ref_count_dec(test_node);
}

/**
 * @brief Log message for diagnostic purposes
 *
 * @param test_node <IN> Test node.  The node_id is included in all log
 * activity.
 * @param where <IN> Text logged about individual message state/location;
 * such as send, receive, receive unexpected, etc.
 * @param log_cat <IN> Log category.  User provided categories allow filtering
 * based on things like message direction (send or receive).
 * @param log_level <IN> Log level.  More severe levels are used for
 * things which might be errors than normal
 */
static void
rtn_log_msg_wrapper(struct replication_test_node *test_node,
                    struct sdf_msg_wrapper *msg_wrapper,
                    const char *where, int log_cat,
                    enum plat_log_level log_level) {
    struct sdf_msg *msg;
    SDF_protocol_msg_t *protocol_msg;
    SDF_Protocol_Msg_Info_t *protocol_info;
    struct sdf_msg_error_payload *error_msg;
    struct cr_shard_meta *shard_meta;
    char stmp[1024];

    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, msg_wrapper);

    protocol_msg = (SDF_protocol_msg_t *)msg->msg_payload;

    plat_log_msg(LOG_ID, log_cat, log_level,
                 "wrapper:%p, node_id %lld %s type %s src %lld:%s dest %lld:%s mkey %lx msg: %s",
                 msg_wrapper, (long long)test_node->node_id, where,
                 SDF_msg_type_to_string((enum SDF_msg_type)msg->msg_type),
                 (long long)msg_wrapper->src_vnode,
                 SDF_msg_protocol_to_string((enum SDF_msg_protocol)msg_wrapper->
                                            src_service),
                 (long long)msg_wrapper->dest_vnode,
                 SDF_msg_protocol_to_string((enum SDF_msg_protocol)msg_wrapper->
                                            dest_service),
                 msg->sent_id, SDFsPrintProtocolMsg(stmp, protocol_msg, 1023));

    switch (msg->msg_type) {
    case SDF_MSG_ERROR:
        error_msg = (struct sdf_msg_error_payload *)msg->msg_payload;

        plat_log_msg(LOG_ID, log_cat, log_level, "node_id %lld error %s",
                     (long long)test_node->node_id,
                     sdf_status_to_string(error_msg->error));
        break;

    /*
     * XXX: Enumerate all new message types which imply an SDF_protocol_msg
     * payload here.
     */
    case FLSH_REQUEST:
    case FLSH_RESPOND:
    case META_REQUEST:
    case META_RESPOND:
    case REPL_REQUEST:
    case RESP_ONE:
    case REQ_MISS:
        protocol_msg = (SDF_protocol_msg_t *)msg->msg_payload;
        if (protocol_msg->msgtype >= 0 &&
            protocol_msg->msgtype < N_SDF_PROTOCOL_MSGS) {
            protocol_info = &(SDF_Protocol_Msg_Info[protocol_msg->msgtype]);
            plat_log_msg(LOG_ID, log_cat, LOG_TRACE,
                         "node_id %lld protocol msgtype %s",
                         (long long)test_node->node_id,
                         protocol_info->shortname);

#if 0
            /* FIXME: Should fix protocol pretty printer and use that */
            /* FIXME: zhenwei, 2009-07-08, some key is not set in replicator */
            if (protocol_info->format & m_key_) {
                plat_log_msg(LOG_ID, log_cat, log_level,
                             "node_id %lld key %*.*s",
                             (long long)test_node->node_id,
                             protocol_msg->key.len, protocol_msg->key.len,
                             protocol_msg->key.key);
            }
#endif

            switch (protocol_msg->msgtype) {
            case MMCSM:
            case MMPSM:
            case MMDSM:
            case MMRSM:
                if (cr_shard_meta_unmarshal(&shard_meta,
                                            protocol_msg + 1,
                                            protocol_msg->data_size) ==
                    SDF_SUCCESS) {
                    plat_log_msg(LOG_ID, log_cat, log_level,
                                 "node_id %lld home %lld lease %3.1f"
                                 " ltime %lld meta seqno  %lld",
                                 (long long)test_node->node_id,
                                 (long long)shard_meta->persistent.current_home_node,
                                 shard_meta->persistent.lease_usecs /
                                 (float)MILLION,
                                 (long long)shard_meta->persistent.ltime,
                                 (long long)shard_meta->persistent.shard_meta_seqno);
                    cr_shard_meta_free(shard_meta);
                }
                break;
            default:
                break;
            }
        } else {
            plat_log_msg(LOG_ID, log_cat, log_level,
                         "node_id %lld invalid protocol message type %d",
                         (long long)test_node->node_id,
                         protocol_msg->msgtype);
        }
        break;

    default:
        break;
    }

    sdf_msg_wrapper_rwrelease(&msg, msg_wrapper);
}

static const char *
rtn_state_to_string(enum rtn_state state) {
    switch (state) {
#define item(caps, lower)             \
    case caps:                        \
        return (#lower);
    RTN_NODE_STATE_ITEMS()
#undef item
    default:
        plat_assert(0);
    }
}

static const char *
rtn_liveness_to_string(enum rtn_liveness liveness) {
    switch (liveness) {
#define item(caps, lower)             \
    case caps:                        \
        return (#lower);
    RTN_LIVENESS_ITEMS()
#undef item
    default:
        plat_assert(0);
    }
}

static void
rtn_msg_free(plat_closure_scheduler_t *context, void *env,
             struct sdf_msg *msg) {
    plat_assert(msg);
    sdf_msg_free(msg);
}

void
rtn_start_network(struct replication_test_node *test_node,
                  rtfw_void_cb_t user_cb) {
    rtn_do_void_cb_t
        do_cb = rtn_do_void_cb_create(test_node->closure_scheduler,
                                      rtn_start_network_impl, test_node);
    plat_closure_apply(rtn_do_void_cb, &do_cb, user_cb);
    rtn_guarantee_bootstrapped(test_node);
}

static void
rtn_start_network_impl(plat_closure_scheduler_t *context, void *env,
                       rtfw_void_cb_t user_cb) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;

    rtn_start_network_internal(test_node);
    plat_closure_apply(rtfw_void_cb, &user_cb);
}

/**
 * @brief Internal network start
 *
 * This is only called from within the test_node->closure_scheduler
 * thread.
 *
 * @param test_node <IN> May be in RTN_STATE_DEAD where this is called
 * as the final part of node start.
 */
static void
rtn_start_network_internal(struct replication_test_node *test_node) {
    if (test_node->net_state != RTN_NET_UP) {
        test_node->net_state = RTN_NET_UP;

        if (test_node->state == RTN_STATE_LIVE) {
            rtn_resume_network_internal(test_node);
        }
    }
}

/**
 * @brief Internal restoration of previous network state for start
 */
static void
rtn_resume_network_internal(struct replication_test_node *test_node) {
    plat_assert(test_node->state == RTN_STATE_LIVE);

    if (rtn_next_delivery_node(test_node) == SDF_SUCCESS) {
        rtn_reset_outbound_timer(test_node);
    }
}

void
rtn_shutdown_network(struct replication_test_node *test_node,
                     rtfw_void_cb_t user_cb) {
    rtn_do_void_cb_t
        do_cb = rtn_do_void_cb_create(test_node->closure_scheduler,
                                      rtn_shutdown_network_impl, test_node);
    plat_closure_apply(rtn_do_void_cb, &do_cb, user_cb);
    rtn_guarantee_bootstrapped(test_node);
}

static void
rtn_shutdown_network_impl(plat_closure_scheduler_t *context, void *env,
                          rtfw_void_cb_t user_cb) {
    struct replication_test_node *test_node =
        (struct replication_test_node *)env;
    plat_event_free_done_t free_done_cb;

    free_done_cb =
        plat_event_free_done_create(test_node->closure_scheduler,
                                    &rtn_event_free_cb,
                                    test_node);

    if (test_node->net_state != RTN_NET_DOWN) {
        test_node->net_state = RTN_NET_DOWN;

        if (test_node->state == RTN_STATE_LIVE) {
            /*
             * Although free is async, the event is blocked so the
             * caller does not need to wait on it.
             */
            rtn_suspend_network_internal(test_node, free_done_cb);
        }
    }
    plat_closure_apply(rtfw_void_cb, &user_cb);
}

/**
 * @brief Suspend network operation without affecting liveness
 *
 * @param free_done_cb <IN> Closure applied at event free which
 * does a rtn_ref_count_dec(test_node).
 *
 * XXX: drew 2010-04-07 For historical reasons, the normal operation
 * (#rtn_event_free_cb) and shutdown (#rtn_crash_or_shutdown_event_cb)
 * event free clean-up are spearate with the only difference being that
 * the shutdown code sanity checks that the event frees happened
 * in a shut-down state.
 *
 * This is not an interesting distinction and should go away
 */
static void
rtn_suspend_network_internal(struct replication_test_node *test_node,
                             plat_event_free_done_t free_done_cb) {
    plat_assert(test_node->state != RTN_STATE_DEAD);

    if (test_node->msg_outbound.next_delivery_event) {
        ++test_node->ref_count;
        plat_event_free(test_node->msg_outbound.next_delivery_event,
                        free_done_cb);
        test_node->msg_outbound.next_delivery_event = NULL;
    }
}

int
rtn_node_is_network_up(struct replication_test_node *test_node) {
    plat_assert(test_node);
    return (test_node->net_state == RTN_NET_UP);
}
