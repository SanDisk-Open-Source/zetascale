/*
 * File:   sdf/protocol/replication/meta_storage.c
 *
 * Author: drew
 *
 * Created on February 3, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: meta_storage.c 13651 2010-05-14 22:09:43Z drew $
 */

/*
 * shard meta-data storage
 *
 * As of 2009-07-16 the super-node mode of operation is supported where
 * meta-data is stored on a single node which cannot fail.  The loosly
 * consistent distributed mode is supported.  It will evolve to include a
 * consensus mode using Paxos which will continue to function as long as
 * a simple majority of nodes are available.
 *
 * The implementation consists of a #replication_meta_storage object  which
 * owns a number of stateful #rms_shard abstractions which have at most one
 * operation in-flight to flash (since the flash code may not guarantee
 * ordering between operations to the same object).   Shards can be
 * indexed either by shard-id or intra-node vip group id (which requires their
 * ids to be globaly unique) or shard id.  The #rms_shard sguid, #rms_op sguid,
 * and #SDF_protocol_msg_t shard fields are are overloaded by adding
 * adding SDF_SHARDID_VIP_GROUP_OFFSET to the shard id.  An #rms_op object
 * exists for each queued operation, parented of the #replication_meta_storage
 * object for remote shard access and #rms_shard access for local
 * objects.
 *
 * Mapping from replication types to mode of operation and indexing
 * scheme is controlled by the #RMS_REPLICATION_TYPE_ITEMS macro.
 *
 * For simplicity, all closures bound to
 * #replicator_meta_Storage.component_api.single_scheduler are executed out of
 * the * same thread making calls to the various replicator_meta_storage
 * functions without re-entrancy.
 *
 * #replicator_meta_storage objects are reference counted with the user
 * holding one reference count which is released by a call to
 * #rms_shutdown and by each of the #rms_shard objects.
 *
 * #rms_shard objects are reference counted with one count held by
 * the owning #replicator_meta_storage_object that is released on
 * the call to #rms_shutdown.  One count is held for each #rms_op
 * that is queued. One is held by the lease timeout code.
 *
 * For remote access a message based API is provided with most functions
 * corresponding to local functions
 *
 * MMCSM, MM_createshard_meta -> #rms_create_shard_meta
 *    Payload: marhsalled #cr_shard_meta object
 * MMGSM, MM_get_shard_meta -> #rms_get_shard_meta
 *    Payload: none
 * MMPSM, MM_put_shard_meta -> #rms_put_shard_meta
 *    Payload: marhsalled #cr_shard_meta object
 * MMDSM, MM_delete_shard_meta -> #rms_delete_shard_meta
 *    Payload: marhsalled #cr_shard_meta object
 *
 * where all requests return a
 *
 * MMRSM, MM_return_shard_meta
 *   Payload: marshalled #cr_shard_meta object when available.
 *
 * For leases to work correctly on home node and read only nodes, the
 * consumers must guarantee that they will use less than the available lease
 * time.  Where modified data is pulled this is easy to accomplish since the
 * consumer can measure the lease time from the interval where they sent
 * the mesage since this preceeds the lease being sent over the wire.
 *
 * For all lease updates to be pulls
 * MMSMC, MM_shard_meta_changed
 *   Payload: none
 * messages are sent whenever a change happens.
 *
 * For a multicast of the actual data to work, we'd want to periodically
 * ping the meta-data system with the local times on those nodes and perhaps
 * broadcast marshaled #cr_shard_meta objects with a vector clock from the
 * nodes and meta-system offset.
 *
 * Messages only need to be sent to the nodes which have local storage for
 * the given shard.
 *
 * Short term, we hit remote nodes directly at the functional API layer,
 * for instance with #rms_put_shard_meta directly generating the
 * MMPSM message.
 *
 * XXX: drew 2009-08-21 Currently we overload the #rms_shard sguid
 * field for vip group based indexing.  That's not very intuitive
 * and should probably be split internally.
 */

#include "sys/queue.h"

#include "platform/assert.h"
#include "platform/closure.h"
#include "platform/event.h"
#include "platform/stdio.h"

#include "protocol/protocol_common.h"

#include "protocol/replication/copy_replicator_internal.h"
#include "protocol/replication/meta_storage.h"
#include "protocol/replication/meta_types.h"
#include "protocol/replication/replicator.h"

#include "shared/shard_meta.h"

#undef MILLION
#define MILLION 1000000

static const struct timeval rms_tv_zero = { 0, 0 };

static const struct timeval rms_tv_infinite = {
    /* tv_sec is a long, but this may avoid overflow elsewhere */
    .tv_sec = INT_MAX,
    .tv_usec = MILLION - 1
};

/** @brief Base logging */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "meta_storage");
/** @brief Pilot beacon, used when no node has a lease */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_BEACON, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "meta_storage/beacon");
/** @brief Log for RRTF_META_DISTRIBUTED specific activity in V1 */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_DISTRIBUTED, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "meta_storage/distributed");
/** @brief Event handling, tracking timer events */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_EVENT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "meta_storage/event");
/** @brief Flash access */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_FLASH, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "meta_storage/flash");
/** @brief Lease management */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_LEASE, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "meta_storage/lease");
/** @brief Lease changes with liveness events */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_LEASE_LIVENESS, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "meta_storage/lease/liveness");
/** @brief Individual operations */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_OP, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "meta_storage/op");
/** @brief RPC calls */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_REMOTE, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "meta_storage/remote");

#define RMS_STATE_ITEMS() \
    /** @brief Initial state, only on construction */                          \
    item(RMS_STATE_INITIAL)                                                    \
    /** @brief Iterate over container entries */                               \
    item(RMS_STATE_ITERATE)                                                    \
    /** @brief Format, probably only in test environment */                    \
    item(RMS_STATE_FORMAT)                                                     \
    /** @brief Normal operation */                                             \
    item(RMS_STATE_NORMAL)                                                     \
    /** @brief Shutting down asynchronously */                                 \
    item(RMS_STATE_TO_SHUTDOWN)                                                \
    /** @brief Stopped (on the way to destruction) */                          \
    item(RMS_STATE_SHUTDOWN)

enum rms_state {
#define item(caps) caps,
    RMS_STATE_ITEMS()
#undef item
};

/** @brief Flags controlling rms operation */
enum rms_replication_type_flags {
    /* What to index #rms_shard off */

    /** @brief No #rms_shard structure */
    RRTF_INDEX_NONE = 1 << 0,
    /** @brief #rms_shard indexed by CR_SHARD_ID */
    RRTF_INDEX_SHARDID = 1 << 1,
    /** @brief #rms_shard indexed by VIP group */
    RRTF_INDEX_VIP_GROUP_ID = 1 << 2,

    /* How to store it */

    /** @brief This type is not stored by rms */
    RRTF_META_NONE = 1 << 3,
    /** @brief Stored in a single supernode */
    RRTF_META_SUPER = 1 << 4,
    /** @brief Replicated via Paxos */
    RRTF_META_PAXOS = 1 << 5,
    /** @brief Distributed (weakly consistent) storage */
    RRTF_META_DISTRIBUTED = 1 << 6
};

/**
 * @brief Handling of given replication type
 * item(type, flags)
 */
#define RMS_REPLICATION_TYPE_ITEMS() \
    item(SDF_REPLICATION_NONE, RRTF_INDEX_NONE|RRTF_META_NONE)                \
    item(SDF_REPLICATION_SIMPLE, RRTF_INDEX_NONE|RRTF_META_NONE)              \
    item(SDF_REPLICATION_META_SUPER_NODE,                                      \
         RRTF_INDEX_SHARDID|RRTF_META_SUPER)                                  \
    item(SDF_REPLICATION_META_EXTERNAL_AUTHORITY,                              \
         RRTF_INDEX_SHARDID|RRTF_META_NONE)                                   \
    item(SDF_REPLICATION_META_CONSENSUS,                                       \
         RRTF_INDEX_SHARDID|RRTF_META_PAXOS)                                  \
    item(SDF_REPLICATION_V1_2_WAY,                                             \
         RRTF_INDEX_VIP_GROUP_ID|RRTF_META_DISTRIBUTED)                       \
    item(SDF_REPLICATION_V1_N_PLUS_1,                                          \
         RRTF_INDEX_VIP_GROUP_ID|RRTF_META_DISTRIBUTED)                       \
    item(SDF_REPLICATION_CONSENSUS,                                            \
         RRTF_INDEX_SHARDID|RRTF_META_PAXOS)

/**
 * @brief State for the entire replicator_meta_storage supernode implementation
 */
struct replicator_meta_storage {
    /* Static configuration */

    /** @brief Replicator configuration for node count, my node */
    struct sdf_replicator_config config;

    /** @brief Interface for sending messages, scheduling events */
    struct sdf_replicator_api component_api;

    /** @brief single threaded closure scheduler */
    struct plat_closure_scheduler *closure_scheduler;

    /** @brief Constructor specified closure for updating clients */
    rms_shard_meta_cb_t update_cb;

    /* Static state */

    /** @brief Message received from any source. */
    sdf_msg_recv_wrapper_t recv_closure;

    /* Dynamic state */

    /** @brief Current state */
    enum rms_state state;

    /** @brief Cached liveness state for notifications */
    struct {
        /** @brief Nodes (unsorted) */
        vnode_t *nodes;

        /** @brief Number of live nodes */
        int nnodes;
    } live;

    /**
     * @brief number of events in flight.
     *
     * One ref count exists for the user, one for each loaded
     * shard, and one for each remote operation.
     *
     * Shutdown is an asynchronous action which does not complete until
     * after all pending events complete.
     */
    int ref_count;

    /**
     * @brief Completion for the executing shutdown
     *
     * From the call to #rms_shutdown
     */
    rms_shutdown_cb_t shutdown_cb;

    /** @brief Container of rms_shard indexed by shard id */
    TAILQ_HEAD(, rms_shard) shards;

    /**
     * @brief List of  all pending remote operations.
     *
     * We handle remote operations as a special case completely separate
     * from local because that makes our code simplest for reaching short-term
     * milestones.
     *
     * Long term we can cache until the lease expires.
     */
    TAILQ_HEAD(, rms_op) remote_ops;
};

/*
 * Meta-data shards have at most one pending operation since there's a
 * small amount of data that's updated as a blob, it's cached during
 * normal operation, and udpates are infrequent (on the order of
 * the switch-over interval)
 */

/* XXX: Add BLOCK_FORMAT between STATE_INITIAL and STATE_NO_META, etc. */

#define RMS_SHARD_STATE_ITEMS() \
    /** @brief State unknown, get required */                                  \
    item(RMS_SHARD_STATE_INITIAL, initial)                                     \
    /** @brief Internal format does not exist yet */                           \
    item(RMS_SHARD_STATE_NO_FORMAT, no_shard)                                  \
    /** @brief Meta-data object does not exist yet */                          \
    item(RMS_SHARD_STATE_NO_META, no_meta)                                     \
    /** @brief Waiting for input */                                            \
    item(RMS_SHARD_STATE_NORMAL, normal)                                       \
    /** @brief Shutdown. Delete when reference count hits zero */              \
    item(RMS_SHARD_STATE_TO_SHUTDOWN, to_shutdown)                             \
    /** @brief Deleted.  Delete when reference count hits zero */              \
    item(RMS_SHARD_STATE_DELETED, deleted)

enum rms_shard_state {
#define item(caps, lower) caps,
    RMS_SHARD_STATE_ITEMS()
#undef item
};

/** @brief All state for a given shard */
struct rms_shard {
    /** @brief Pointer to #rms_supernode */
    struct replicator_meta_storage *rms;

    /**
     * @brief Data shard id
     *
     * Shards with replication type flag RRTF_INDEX_SHARDID are indexed
     * by shardid.  This should be SDF_SHARDID_INVALID when a vip group
     * is being indexed.
     *
     * XXX: drew 2009-08-16 For v1, this is overloaded for vip group
     * indexing by adding SDF_SHARDID_VIP_GROUP_OFFSET to the vip
     * group.
     */
    SDF_shardid_t sguid;

#ifdef notyet
    /**
     * @brief Group ID being mapped in
     *
     * Shards with replication type flag RRTF_INDEX_VIP_GROUP_ID are
     * indexed by #vip_group_id.  When not VIP_GROUP_ID_INVALID
     * this takes precidence over sguid.
     */
    int vip_group_id;
#endif

    /**
     * @brief Node on which meta-data for this shard lives
     *
     * XXX: drew 2009-05-18 This becomes an array for the Paxos case
     */
    vnode_t meta_pnode;

    /**
     * @brief Meta shard id
     * XXX: drew 2009-05-12 switch to one shard for all user containers
     */
    SDF_shardid_t sguid_meta;

    /** @brief State of this shard */
    enum rms_shard_state state;

    /** @brief An operation is running */
    int flash_updating;

    /**
     * @brief #rms_shard_do_work is running so don't re-enter
     *
     * This allows operation implementation to have fan-out side effects
     * without re-entrancy.
     */
    int do_work_running;

    /**
     * @brief Last known persistent unmarshalled shard meta_data
     *
     * This is updated when a get or put operation completes.  The
     * lease field can be updated as needed prior to marshalling since
     * it's not being used for timeouts.
     */
    struct cr_shard_meta *shard_meta;

    /** @brief Set when the shard meta data was added as part of a reput */
    unsigned shard_meta_reput : 1;

    /** @brief Delivery disposition for per-shard pending flash request state */
    struct sdf_fth_mbx *mbx;

    /**
     * @brief All information about this shard's lease
     *
     * XXX: drew 2009-02-03 There may be a special case on startup
     * where we don't allow the previous lease holder to access
     * things and don't allow new leases.  SDF_ILLEGAL_PNODE would be
     * a fine current home node value in #shard_meta if that is the case.
     */
    struct {
        /**
         * @brief Lease expiration time in real time
         *
         * Real time is defined using rms_supernode->component_api.gettime
         * for compatability with the simulated environment;
         */
        struct timeval expires;

        /** @brief Name for timeout_event to ease tracking */
        char *timeout_event_name;

        /**
         * @brief Event which fires when the lease times out.
         *
         * One ref_count shall be held for a pending timer until
         * #plat_event_free has applied its asynchronous free complete
         * closure.
         */
        struct plat_event *timeout_event;

        /**
         * @brief A valid lease exists
         *
         * This implies that #shard_meta and #lease_expiration are both
         * valid.
         */
        unsigned exists : 1;
    } lease;

    /** @brief Name for beacon_event to ease tracking */
    char *beacon_event_name;

    /**
     * @brief When no home node exists heartbeat all live replica nodes
     *
     * We do this at the #rms_shard level instead of #replicator_meta_storage
     * because the timeouts (and therefore switch over intervals) may be
     * different for various replicas.
     */
    struct plat_event *beacon_event;

    /**
     * @brief number of events in flight.
     *
     * Ref counts exist for the #replicator_meta_storage object, each
     * pending #rms_op object, and this->lease.timeout_event until its
     * async free action has completed.
     *
     * Shutdown is an asynchronous action which does not complete until
     * after all pending events complete.
     */
    int ref_count;

    /**
     * @brief Message type of pending operation
     *
     *  SDF_PROTOCOL_MSG_INVALID for none
     */
    SDF_protocol_msg_type_t op_msgtype;

    /** @brief Operations blocked on flash activity completion */
    TAILQ_HEAD(, rms_op) blocked_ops;

    /** @brief Entry in rms_supernode->shards */
    TAILQ_ENTRY(rms_shard) shards_entry;
};

#define RMS_OP_TYPE_ITEMS() \
    /** @brief Shard create operation */                                       \
    item(RMS_OP_CREATE, create, MMCSM, 1)                                      \
    /** @brief Get operation  */                                               \
    item(RMS_OP_GET, get, MMGSM, 0)                                            \
    /** @brief Put operation */                                                \
    item(RMS_OP_PUT, put, MMPSM, 1)                                            \
    /**                                                                        \
     * @brief Put for distributed shard meta.                                  \
     *                                                                         \
     * This is the put following the MMSMC shard meta changed,                 \
     * MMGSM get shard meta, MMRSM return shard meta sequence which does not   \
     * result in a remote notification on update.                              \
     */                                                                        \
    item(RMS_OP_REPUT, reput, SDF_PROTOCOL_MSG_INVALID, 1)                     \
    /** @brief Delete operation */                                             \
    item(RMS_OP_DELETE, delete, MMDSM, 1)

enum rms_op_type {
#define item(caps, lower, msg, meta) caps,
    RMS_OP_TYPE_ITEMS()
#undef item
    RMS_OP_INVALID = -1
};

/**
 * @brief Structure for each user-initiated operation
 *
 * From #rms_get_shard_meta, #rms_put_shard_meta, #rms_delete_shard_meta
 */
struct rms_op {
    /** @brief Replication meta storage structure */
    struct replicator_meta_storage *rms;

    /** @brief Associated shard */
    struct rms_shard *shard;

    /**
     * @brief Data shard GUID
     *
     * XXX: drew 2009-08-16 For v1, this is overloaded for vip group
     * indexing by adding SDF_SHARDID_VIP_GROUP_OFFSET to the vip
     * group.
     */
    SDF_shardid_t sguid;

#ifdef notyet
    /**
     * @brief Group ID being mapped in
     *
     * Shards with replication type flag RRTF_INDEX_VIP_GROUP_ID are
     * indexed by #vip_group_id.  When not VIP_GROUP_ID_INVALID
     * this takes precidence over sguid.
     */
    int vip_group_id;
#endif

    /**
     * @brief Node on which meta-data for this shard lives
     *
     * XXX: drew 2009-05-18 This becomes an array for the Paxos case
     */
    vnode_t meta_pnode;

    /**
     * @brief Meta shard id
     * XXX: drew 2009-05-12 switch to one shard for all user containers
     */
    SDF_shardid_t sguid_meta;

    /** @brief Type of this operation */
    enum rms_op_type op_type;

    /** @brief Thin shard meta valid for #op_type RMS_OP_GET */
    struct sdf_replication_shard_meta get_shard_meta;

    /**
     * @brief Start time
     *
     * This allows calculation of lease times from the request start so that
     * we can bound lease times regardless of what message delays were
     * incurred.
     */
    struct timeval start_time;

    /**
     * @brief cr_shard_meta input from user or unmarshaled
     *
     * #shard_meta copied in from #rms_put_shard_meta or
     * #rms_delete_shard_meta for #op_type RMS_OP_PUT and RMS_OP_DELETE
     * respectively.  #shard_meta is NULL for #op_type RMS_OP_GET
     *
     * The #rms_op structure retains ownership until completion at which
     * point it may become rms_shard->shard_meta on successful
     * operations.
     */
    struct cr_shard_meta *shard_meta;

    /** @brief Original request for remote requests */
    struct sdf_msg_wrapper *request_wrapper;

    /** @brief Delivery disposition for rpc op (one at a time for shard)  */
    struct sdf_fth_mbx *mbx;

    /** @brief User provided callback for function completion */
    rms_shard_meta_cb_t cb;

    /** @brief Entry in rms_shard->blocked_ops */
    TAILQ_ENTRY(rms_op) blocked_ops_entry;

    /** @brief Entry in rms->remote_ops */
    TAILQ_ENTRY(rms_op) remote_ops_entry;
};

enum {
#if 1
    /*
     * XXX: drew 2009-06-17 According to xioanan we need to have at least
     * one segment (32M) per size of object we'll be creating.  Speculate
     * on how many we'll need.
     *
     * If ENOSPC results on HFSET, RMS_META_QUOTA needs to be bigger.
     */
    RMS_META_QUOTA = 1024 * 1024 * 32 * 10,
#else
    /* Get ENOSPC to test error path */
    RMS_META_QUOTE = 1024 * 1024
#endif
    /* Also arbitrary.  We need # of containers * whatever we need for Paxos */
    RMS_META_OBJS = 1024
};

static void rms_modify_shard_meta(struct replicator_meta_storage *rms,
                                  enum rms_op_type op_type,
                                  const struct cr_shard_meta *shard_meta_arg,
                                  rms_shard_meta_cb_t cb);
static void rms_queue_op(struct replicator_meta_storage *rms,
                         struct rms_op *op);
static void rms_queue_local_op(struct replicator_meta_storage *rms,
                               struct rms_op *op);
static void rms_queue_remote_op(struct replicator_meta_storage *rms,
                                struct rms_op *op);
static int rms_node_is_live(struct replicator_meta_storage *rms,
                            vnode_t pnode) __attribute__((unused));
static void rms_recv_msg(plat_closure_scheduler_t *context, void *env,
                         struct sdf_msg_wrapper *wrapper);
static void rms_mmsmc_get_cb(plat_closure_scheduler_t *context, void *env,
                             SDF_status_t status,
                             struct cr_shard_meta *shard_meta,
                             struct timeval lease_expires);
static void rms_mmsmc_reput_cb(plat_closure_scheduler_t *context, void *env,
                               SDF_status_t status,
                               struct cr_shard_meta *shard_meta,
                               struct timeval lease_expires);
static void rms_ref_count_dec(struct replicator_meta_storage *rms);

static void rms_shard_shutdown(struct rms_shard *shard);
static void rms_shard_ref_count_dec(struct rms_shard *shard);
static void rms_shard_do_work(struct rms_shard *shard, struct rms_op *op_arg);
static void rms_shard_flash_response(struct plat_closure_scheduler *context,
                                     void *env,
                                     struct sdf_msg_wrapper *response);
static void rms_shard_put_complete(struct rms_shard *shard, struct rms_op *op,
                                   SDF_status_t status);
static void rms_shard_notify(struct rms_shard *shard);
static void rms_shard_notify_node_by_msg(struct rms_shard *shard, vnode_t node);
static void rms_shard_do_notify_node_by_msg(struct rms_shard *shard, vnode_t node);
static void rms_shard_schedule_lease_timeout(struct rms_shard *shard);
static void rms_shard_lease_expired_cb(plat_closure_scheduler_t *context,
                                       void *env, struct plat_event *event);
static void rms_shard_lease_cancel_timeout(struct rms_shard *shard);
static void rms_shard_node_dead(struct rms_shard *shard, vnode_t pnode);
static void rms_shard_set_lease_none(struct rms_shard *shard);
static void rms_shard_beacon_reset(struct rms_shard *shard);
static void rms_shard_beacon_cb(plat_closure_scheduler_t *context,
                                void *env, struct plat_event *event);
static void rms_shard_beacon_cancel(struct rms_shard *shard);
static void rms_shard_free_event(struct rms_shard *shard,
                                 struct plat_event *event);
static void rms_shard_event_free_cb(plat_closure_scheduler_t *context,
                                    void *env);
static void rms_shard_lease_update(struct rms_shard *shard);
static void rms_shard_to_meta_key(struct rms_shard *shard,
                                  SDF_simple_key_t *key);
const char *rms_shard_state_to_string(enum rms_shard_state state);

static SDF_status_t rms_op_allowed(struct rms_op *op);
static void rms_op_end(struct rms_op *op, SDF_status_t status,
                       struct cr_shard_meta *shard_meta_arg);
static void rms_op_send_response(struct rms_op *op, SDF_status_t status_arg);
static void rms_op_recv_response(struct plat_closure_scheduler *context,
                                 void *env, struct sdf_msg_wrapper *response);
static const char *rms_op_type_to_string(enum rms_op_type op_type);
static int rms_op_type_is_put(enum rms_op_type op_type);
static int rms_replication_type_flags(SDF_replication_t replication_type);
static void rms_replication_type_validate(SDF_replication_t replication_type);
static void rms_replication_types_validate() __attribute__((constructor));

struct replicator_meta_storage *
replicator_meta_storage_alloc(const struct sdf_replicator_config *config,
                              struct sdf_replicator_api *api,
                              rms_shard_meta_cb_t update_cb) {
    struct replicator_meta_storage *ret;
    int failed;

    failed = !plat_calloc_struct(&ret);
    if (!failed) {
        /* Static configuration */
        ret->config = *config;
        ret->component_api = *api;
        ret->update_cb = update_cb;

        /* Static state */
        plat_assert(ret->component_api.single_scheduler);
        ret->closure_scheduler = ret->component_api.single_scheduler;

        ret->recv_closure =
            sdf_msg_recv_wrapper_create(ret->closure_scheduler, rms_recv_msg,
                                        ret);

        /* Dynamic state */
        ret->state = RMS_STATE_INITIAL;

        /* One reference count going away at shutdown */
        ret->ref_count = 1;
        ret->shutdown_cb  = rms_shutdown_cb_null;
        TAILQ_INIT(&ret->shards);
        TAILQ_INIT(&ret->remote_ops);
    }

#ifndef notyet
    plat_assert_imply(failed, !ret);
    /*
     * XXX: drew 2009-09-25 Coverity correctly flags this as dead code.
     * Leave it as a reminder in case #replicator_meta_storage_alloc becomes
     * more complex and needs the free.
     */
#else
    if (failed && ret) {
        rms_shutdown(ret, rms_shutdown_cb_null);
        ret = NULL;
    }
#endif

    if (!failed) {
        plat_log_msg(21425, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "rms %p allocated", ret);
    } else {
        plat_log_msg(21426, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "rms alloc failed");
    }

    return (ret);
}

void
rms_start(struct replicator_meta_storage *rms) {
    int success;

    plat_log_msg(21427, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "rms %p node %u start", rms, rms->config.my_node);

    success = __sync_bool_compare_and_swap(&rms->state, RMS_STATE_INITIAL,
                                           RMS_STATE_NORMAL);
    plat_assert(success);

    /*
     * XXX: drew 2009-05-18 We need to address startup.  Right now
     * nothing will happen until we're poked.  At least with unified
     * meta-data we want to iterate all the containers and notify
     * the consumers; where this can trigger recovery or whatever.
     */
}

void
rms_shutdown(struct replicator_meta_storage *rms,
             rms_shutdown_cb_t shutdown_complete) {
    struct rms_shard *shard;
    struct rms_shard *next;
    int success;

    plat_log_msg(21428, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "rms %p node %u shutdown", rms, rms->config.my_node);

    success = __sync_bool_compare_and_swap(&rms->state, RMS_STATE_NORMAL,
                                           RMS_STATE_TO_SHUTDOWN);
    plat_assert(success);

    plat_assert(rms_shutdown_cb_is_null(&rms->shutdown_cb));

    rms->shutdown_cb = shutdown_complete;
    TAILQ_FOREACH_SAFE(shard, &rms->shards, shards_entry, next) {
        rms_shard_shutdown(shard);
    }

    rms_ref_count_dec(rms);
}

void
rms_create_shard_meta(struct replicator_meta_storage *rms,
                      const struct cr_shard_meta *cr_shard_meta,
                      rms_shard_meta_cb_t cb) {
    plat_log_msg(21429, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "rms %p node %u create shard 0x%lx vip group %d", rms,
                 rms->config.my_node, (long)cr_shard_meta->persistent.sguid,
                 cr_shard_meta->persistent.intra_node_vip_group_id);

    rms_modify_shard_meta(rms, RMS_OP_CREATE, cr_shard_meta, cb);
}

void
rms_get_shard_meta(struct replicator_meta_storage *rms,
                   SDF_shardid_t sguid,
                   const struct sdf_replication_shard_meta *shard_meta,
                   rms_shard_meta_cb_t cb) {
    struct rms_op *op;

    plat_log_msg(21430, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "rms %p node %u get shard 0x%lx", rms,
                 rms->config.my_node, (long)sguid);

    if (plat_calloc_struct(&op)) {
        op->rms = rms;
        op->sguid = sguid;
        op->meta_pnode = shard_meta->meta_pnode;
        op->sguid_meta = shard_meta->meta_shardid;
        op->op_type = RMS_OP_GET;
        op->get_shard_meta = *shard_meta;
        op->cb = cb;
        rms_queue_op(rms, op);
    } else if (!rms_shard_meta_cb_is_null(&cb)) {
        plat_closure_apply(rms_shard_meta_cb, &cb, SDF_OUT_OF_MEM, NULL,
                           rms_tv_zero);
    }
}

void
rms_put_shard_meta(struct replicator_meta_storage *rms,
                   const struct cr_shard_meta *cr_shard_meta,
                   rms_shard_meta_cb_t cb) {
    plat_log_msg(21431, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "rms %p node %u put shard 0x%lx seqno %ld", rms,
                 rms->config.my_node,
                 (long)cr_shard_meta->persistent.sguid,
                 (long)cr_shard_meta->persistent.shard_meta_seqno);

    rms_modify_shard_meta(rms, RMS_OP_PUT, cr_shard_meta, cb);
}

void
rms_delete_shard_meta(struct replicator_meta_storage *rms,
                      const struct cr_shard_meta *cr_shard_meta,
                      rms_shard_meta_cb_t cb) {
    plat_log_msg(21432, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "rms %p node %u delete shard 0x%lx seqno %ld", rms,
                 rms->config.my_node,
                 (long)cr_shard_meta->persistent.sguid,
                 (long)cr_shard_meta->persistent.shard_meta_seqno);

    rms_modify_shard_meta(rms, RMS_OP_DELETE, cr_shard_meta, cb);
}

/**
 * @brief modify shard meta
 *
 * Used to implement #rms_create_shard_meta, #rms_delete_shard_meta,
 * and #rms_put_shard_meta.
 *
 * @param rms <IN> replicator meta storage
 *
 * @param op_type <IN> operation type
 *
 * @param cr_shard_meta <IN> shard meta-data.  The requested lease
 * time may be ignored.  The caller retains ownership of the cr_shard_meta
 * structure.  The structure will not be referenced by the replicator
 * meta storage code after #rms_put_shard_meta returns.
 *
 * @param cb <IN> applied on completion.
 */
static void
rms_modify_shard_meta(struct replicator_meta_storage *rms,
                      enum rms_op_type op_type,
                      const struct cr_shard_meta *shard_meta_arg,
                      rms_shard_meta_cb_t cb) {
    int failed;
    struct rms_op *op;
    struct cr_shard_meta *shard_meta;
    int flags;

    flags = rms_replication_type_flags(shard_meta_arg->persistent.type);
    plat_assert_imply(flags & RRTF_INDEX_VIP_GROUP_ID,
                      shard_meta_arg->persistent.intra_node_vip_group_id !=
                      VIP_GROUP_ID_INVALID);

    shard_meta = cr_shard_meta_dup(shard_meta_arg);
    failed = !shard_meta;

    if (!failed) {
        failed = !plat_calloc_struct(&op);
    }

    if (!failed) {
        op->rms = rms;
        /*
         * Always write distributed shard-meta to the local node, but
         * preserve where it came from because that's interesting for
         * diagnostics.
         */
        if (flags & RRTF_META_DISTRIBUTED) {
            op->meta_pnode = rms->config.my_node;
        } else {
            op->meta_pnode = shard_meta->persistent.meta_pnodes[0];
        }

        /*
         * XXX: drew 2009-08-16 Overload op->sguid and shard->sguid
         * for expedience.
         */
        if (flags & RRTF_INDEX_VIP_GROUP_ID) {
            op->sguid = SDF_SHARDID_VIP_GROUP_OFFSET +
                shard_meta->persistent.intra_node_vip_group_id;
        } else if (flags & RRTF_INDEX_SHARDID) {
            op->sguid = shard_meta->persistent.sguid;
        }

        op->sguid_meta = shard_meta->persistent.sguid_meta;
        op->op_type = op_type;
        op->shard_meta = shard_meta;
        op->cb = cb;
        rms_queue_op(rms, op);
    } else if (!rms_shard_meta_cb_is_null(&cb)) {
        if (shard_meta) {
            cr_shard_meta_free(shard_meta);
        }
        plat_closure_apply(rms_shard_meta_cb, &cb, SDF_OUT_OF_MEM, NULL,
                           rms_tv_zero);
    }
}

/**
 * @brief Start local initialized meta-data operation
 *
 * @param rms <IN> replicator meta storage
 * @param op <IN> An op which has been initialized but not yet added to
 * the #rms list of pending operations.
 */
static void
rms_queue_op(struct replicator_meta_storage *rms, struct rms_op *op) {
    plat_closure_apply(plat_timer_dispatcher_gettime,
                       &rms->component_api.gettime, &op->start_time);

    if (op->meta_pnode != rms->config.my_node || rms->config.meta_by_message) {
        rms_queue_remote_op(rms, op);
    } else {
        rms_queue_local_op(rms, op);
    }
}

/**
 * @brief Start initialized meta-data operation
 *
 * @param rms <IN> replicator meta storage
 * @param op <IN> An op which has been initialized but not yet added to
 * the #rms list of pending operations.
 */
static void
rms_queue_local_op(struct replicator_meta_storage *rms, struct rms_op *op) {
    struct rms_shard *shard;
    sdf_msg_recv_wrapper_t recv_closure;

    plat_assert(!op->shard);
    plat_assert(op->meta_pnode == rms->config.my_node);

    TAILQ_FOREACH(shard, &rms->shards, shards_entry) {
        if (shard->sguid == op->sguid) {
            break;
        }
    }

    if (!shard && plat_calloc_struct(&shard)) {
        shard->rms = rms;
        shard->meta_pnode = op->meta_pnode;
        shard->sguid = op->sguid;
        shard->sguid_meta = op->sguid_meta;

        shard->state = RMS_SHARD_STATE_INITIAL;
        shard->op_msgtype = SDF_PROTOCOL_MSG_INVALID;

        recv_closure = sdf_msg_recv_wrapper_create(rms->closure_scheduler,
                                                   &rms_shard_flash_response,
                                                   shard);
        shard->mbx =
            sdf_fth_mbx_resp_closure_alloc(recv_closure, SACK_REL_YES,
                                           SDF_FTH_MBX_TIMEOUT_ONLY_ERROR);

        plat_asprintf(&shard->lease.timeout_event_name,
                      "rms_shard %p node %u shard 0x%lx lease renewal",
                      shard, rms->config.my_node, shard->sguid);
        plat_assert(shard->lease.timeout_event_name);

        plat_asprintf(&shard->beacon_event_name,
                      "rms_shard %p node %u shard 0x%lx beacon",
                      shard, rms->config.my_node, shard->sguid);
        plat_assert(shard->beacon_event_name);

        if (shard->mbx) {
            shard->ref_count = 1;
            ++rms->ref_count;

            TAILQ_INIT(&shard->blocked_ops);
            TAILQ_INSERT_TAIL(&rms->shards, shard, shards_entry);
        } else {
            plat_free(shard);
            shard = NULL;
        }
    }

    if (!shard) {
        rms_op_end(op, SDF_OUT_OF_MEM, NULL);
    } else {
        op->shard = shard;
        ++shard->ref_count;

        TAILQ_INSERT_TAIL(&shard->blocked_ops, op, blocked_ops_entry);

        rms_shard_do_work(shard, op);
    }
}

static void
rms_queue_remote_op(struct replicator_meta_storage *rms, struct rms_op *op) {
    SDF_status_t status;
    void *data;
    size_t total_len = 0;
    struct sdf_msg *msg;
    SDF_protocol_msg_type_t msgtype;
    SDF_protocol_msg_t *pm;
    SDF_Protocol_Msg_Info_t *pmi;
    size_t header_len = sizeof (*msg) + sizeof (*pm);
    sdf_msg_recv_wrapper_t recv_closure;
    struct sdf_msg_wrapper *msg_wrapper;

    plat_assert(!op->shard);

    switch (op->op_type) {
#define item(caps, lower, msgtype_arg, unmarshal) \
    case caps:                                                                 \
        if (unmarshal) {                                                       \
            plat_assert(op->shard_meta);                                       \
            status = cr_shard_meta_marshal(&data, &total_len, header_len,      \
                                           op->shard_meta);                    \
            msg = (struct sdf_msg *)data;                                      \
        } else {                                                               \
            msg = sdf_msg_alloc(header_len - sizeof(*msg));                    \
            total_len = header_len;                                            \
        }                                                                      \
        msgtype = msgtype_arg;                                                 \
        break;
    RMS_OP_TYPE_ITEMS();
#undef item

    case RMS_OP_INVALID:
    default:
        msg = NULL;
        msgtype = SDF_PROTOCOL_MSG_INVALID;
        plat_assert(0);
        break;
    }

    plat_assert(msgtype != SDF_PROTOCOL_MSG_INVALID);
    plat_assert_always(msg);

    pm = (struct SDF_protocol_msg *)msg->msg_payload;
    memset(msg, 0, header_len);
    msg->msg_flags = 0;
    msg->msg_len = total_len;

    pm->op_meta.shard_meta.meta_pnode = op->meta_pnode;
    pm->op_meta.shard_meta.meta_shardid = op->sguid_meta;

    pm->shard = op->sguid;
    pm->data_offset = 0;
    pm->data_size = total_len - header_len;
    pm->msgtype = msgtype;
    pm->node_from = rms->config.my_node;
    pm->node_to = op->meta_pnode;

    pm->status = SDF_SUCCESS;

    recv_closure = sdf_msg_recv_wrapper_create(rms->closure_scheduler,
                                               &rms_op_recv_response, op);
    op->mbx = sdf_fth_mbx_resp_closure_alloc(recv_closure, SACK_REL_YES,
                                             SDF_FTH_MBX_TIMEOUT_ONLY_ERROR);

    msg_wrapper =
        sdf_msg_wrapper_local_alloc(msg, sdf_msg_wrapper_free_local_null,
                                    SMW_MUTABLE_FIRST, SMW_TYPE_REQUEST,
                                    /* XXX: drew 2009-05-09 from responses? */
                                    pm->node_from /* src */,
                                    SDF_REPLICATION_PEER_META_SUPER /* src */,
                                    pm->node_to /* dest */,
                                    SDF_REPLICATION_PEER_META_SUPER /* dest */,
                                    META_REQUEST,
                                    NULL /* new request not response */);
    plat_assert(msg_wrapper);
    pmi = &(SDF_Protocol_Msg_Info[pm->msgtype]);
    plat_log_msg(21433, LOG_CAT_REMOTE, PLAT_LOG_LEVEL_TRACE,
                 "rms %p node %u shard 0x%lx op %p request to node %u"
                 " Msg: %s (%s)", rms, rms->config.my_node,
                 op->sguid, op, pm->node_to, pmi->name, pmi->shortname);

    TAILQ_INSERT_TAIL(&rms->remote_ops, op, remote_ops_entry);
    ++rms->ref_count;

    plat_closure_apply(sdf_replicator_send_msg_cb,
                       &rms->component_api.send_msg, msg_wrapper,
                       op->mbx, NULL /* status */);
}

void
rms_receive_msg(struct replicator_meta_storage *rms,
                struct sdf_msg_wrapper *msg_wrapper) {
    plat_closure_apply(sdf_msg_recv_wrapper, &rms->recv_closure, msg_wrapper);
}

void
rms_node_live(struct replicator_meta_storage *rms, vnode_t pnode) {
    struct rms_shard *shard;
    int i;

    for (i = 0; i < rms->live.nnodes && rms->live.nodes[i] != pnode; ++i) {
    }

    /* Only notify iff pnode was not already live */
    if (i == rms->live.nnodes) {
        ++rms->live.nnodes;
        if (!rms->live.nodes) {
            rms->live.nodes = plat_malloc(rms->live.nnodes *
                                          sizeof (rms->live.nodes[0]));
        } else {
            rms->live.nodes = plat_realloc(rms->live.nodes,
                                           rms->live.nnodes *
                                           sizeof (rms->live.nodes[0]));
        }
        plat_assert(rms->live.nodes);

        rms->live.nodes[i] = pnode;

        TAILQ_FOREACH(shard, &rms->shards, shards_entry) {
            rms_shard_notify_node_by_msg(shard, pnode);
        }
    }

}

void
rms_node_dead(struct replicator_meta_storage *rms, vnode_t pnode) {
    int i;
    struct rms_shard *shard;

    for (i = 0; i < rms->live.nnodes && rms->live.nodes[i] != pnode; ++i) {
    }

    /* Only notify iff pnode was not already dead */
    if (i < rms->live.nnodes) {
        for (; i < rms->live.nnodes - 1; ++i) {
            rms->live.nodes[i] = rms->live.nodes[i + 1];
        }
        --rms->live.nnodes;

        TAILQ_FOREACH(shard, &rms->shards, shards_entry) {
            rms_shard_node_dead(shard, pnode);
        }
    }
}

/** @brief Return whether pnode is live */
static int
rms_node_is_live(struct replicator_meta_storage *rms, vnode_t pnode) {
    int i;

    for (i = 0; i < rms->live.nnodes && rms->live.nodes[i] != pnode; ++i) {
    }

    return (i != rms->live.nnodes);
}

/* @brief State associated with get in response to MMSMC message */
struct rms_mmsmc_get {
    struct replicator_meta_storage *rms;

    /** @brief Node from which data was received */
    vnode_t remote_node;
};

/**
 * @brief Receive message closure for inbound requests
 */
static void
rms_recv_msg(plat_closure_scheduler_t *context, void *env,
             struct sdf_msg_wrapper *wrapper) {
    struct replicator_meta_storage *rms = (struct replicator_meta_storage *)env;
    struct sdf_msg *msg;
    SDF_protocol_msg_t *pm;
    struct rms_op *op;
    SDF_status_t status;
    /* Preserve for debugger, even if message is unmapped or free */
    SDF_protocol_msg_type_t pm_msgtype;
    struct rms_mmsmc_get *mmsmc_get;

    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, wrapper);
    pm = (SDF_protocol_msg_t *)msg->msg_payload;
    pm_msgtype = pm->msgtype;

    plat_calloc_struct(&op);
    plat_assert(op);

    op->rms = rms;

    /* XXX: drew 2009-05-18 Sanity check fields */

    if (msg->msg_type != META_REQUEST) {
        status = SDF_UNKNOWN_REQUEST;
    } else {
        op->sguid = pm->shard;
        op->meta_pnode = pm->op_meta.shard_meta.meta_pnode;
        op->sguid_meta = pm->op_meta.shard_meta.meta_shardid;
        op->get_shard_meta = pm->op_meta.shard_meta;

        switch (pm_msgtype) {
#define item(caps, lower, msgtype, unmarshal) \
        case msgtype:                                                          \
            op->op_type = caps;                                                \
            op->request_wrapper = wrapper;                                     \
            if (unmarshal) {                                                   \
                /* put has a shard meta as input and output */                 \
                if (op->shard_meta) {                                          \
                    cr_shard_meta_free(op->shard_meta);                        \
                    op->shard_meta = NULL;                                     \
                }                                                              \
                status = cr_shard_meta_unmarshal(&op->shard_meta,              \
                                                 (void *)(pm + 1),             \
                                                 pm->data_size);               \
            } else {                                                           \
                status = SDF_SUCCESS;                                          \
            }                                                                  \
            /* Ref count held by op */                                         \
            sdf_msg_wrapper_rwrelease(&msg, wrapper);                          \
            break;
        RMS_OP_TYPE_ITEMS();
#undef item
        /* Convert notification into a get request for the same shard info */
        case MMSMC:
            op->op_type = RMS_OP_GET;
            /* Consumed in #rms_mmsmc_get_cb */
            ++rms->ref_count;
            /*
             * XXX: drew 2009-08-17 This is a little ugly.  Skip the common
             * get callback where we were running in the allways-message
             * test mode so we don't have to add additional state.
             */
            if (wrapper->src_vnode != rms->config.my_node) {
                plat_calloc_struct(&mmsmc_get);
                plat_assert(mmsmc_get);
                mmsmc_get->rms = rms;
                mmsmc_get->remote_node = wrapper->src_vnode;
                op->cb = rms_shard_meta_cb_create(rms->closure_scheduler,
                                                  &rms_mmsmc_get_cb, mmsmc_get);
            } else {
                op->cb = rms->update_cb;
            }
            status = SDF_SUCCESS;
            sdf_msg_wrapper_rwrelease(&msg, wrapper);
            sdf_msg_wrapper_ref_count_dec(wrapper);
            break;
        default:
            op->op_type = RMS_OP_INVALID;
            op->request_wrapper = wrapper;
            status = SDF_UNKNOWN_REQUEST;
            /* Ref count held by op */
            sdf_msg_wrapper_rwrelease(&msg, wrapper);
        }
    }

    if (status == SDF_SUCCESS) {
        rms_queue_op(rms, op);
    } else {
        rms_op_end(op, status, NULL);
    }
}

/**
 * @brief Called on get completion triggered by MMSMC update message
 *
 * Triggers local reput + lease operations for RRTF_META_DISTRIBUTED
 * eventually consistent meta-data as used in SDF_REPLICATION_V1_N_PLUS_1 and
 * SDF_REPLICATION_V1_2_WAY replication.
 */
static void
rms_mmsmc_get_cb(plat_closure_scheduler_t *context, void *env,
                 SDF_status_t status, struct cr_shard_meta *shard_meta_arg,
                 struct timeval lease_expires) {
    struct rms_mmsmc_get *mmsmc_get = (struct rms_mmsmc_get *)env;
    struct replicator_meta_storage *rms;
    int flags;
    rms_shard_meta_cb_t reput_cb;
    struct cr_shard_meta *shard_meta;

    plat_assert(mmsmc_get);
    rms = mmsmc_get->rms;

    /* NULL at end indicates ownership transfer, otherwise free */
    shard_meta = shard_meta_arg;

    if (status == SDF_SUCCESS) {
        flags = rms_replication_type_flags(shard_meta->persistent.type);

        /*
         * XXX: drew 2009-08-17 We filter out situations where this
         * would be inappropriate since the notification was from the local
         * node in #rms_recv_msg.
         */
        if (flags & RRTF_META_DISTRIBUTED) {
            /*
             * Ignore remote updates from nodes other than the ones which
             * wrote their meta-data.  For instance, the right interleving
             * of messages on node restart will result in us getting our
             * own meta-data back (trac #4950 first reopen).
             */
            if (shard_meta->persistent.write_node != mmsmc_get->remote_node) {
                plat_log_msg(21836, LOG_CAT_DISTRIBUTED,
                             PLAT_LOG_LEVEL_DIAGNOSTIC,
                             "rms %p node %u shard 0x%lx vip group %d"
                             " mmsmc get from node %u not meta write node %u",
                             rms, rms->config.my_node,
                             shard_meta->persistent.sguid,
                             shard_meta->persistent.intra_node_vip_group_id,
                             mmsmc_get->remote_node,
                             shard_meta->persistent.write_node);
            } else {
                /* FIXME: drew 2010-03-23 If the writing meta-data node was */
                plat_log_msg(21434, LOG_CAT_DISTRIBUTED,
                             PLAT_LOG_LEVEL_TRACE,
                             "rms %p node %u reput shard 0x%lx vip group %d",
                             rms, rms->config.my_node,
                             shard_meta->persistent.sguid,
                             shard_meta->persistent.intra_node_vip_group_id);

                /* Consumed in #rms_mmsmc_reput_cb */
                ++rms->ref_count;
                reput_cb = rms_shard_meta_cb_create(rms->closure_scheduler,
                                                    &rms_mmsmc_reput_cb, rms);

                rms_modify_shard_meta(rms, RMS_OP_REPUT, shard_meta, reput_cb);
            }
        /*
         * In the non-distributed case meta-data is strongly consistent
         * so we can immediately notify the user.
         */
        } else if (!rms_shard_meta_cb_is_null(&rms->update_cb)) {
            plat_closure_apply(rms_shard_meta_cb, &rms->update_cb, status,
                               shard_meta, lease_expires);
            /* update_cb consumes shard_meta */
            shard_meta = NULL;
        }
    }

    if (shard_meta) {
        cr_shard_meta_free(shard_meta);
    }

    plat_free(mmsmc_get);

    rms_ref_count_dec(rms);
}

/**
 * @brief Called on reput completion triggered by get following  MMSMC
 *
 * XXX: drew 2010-05-02 This is probably unecessary, since the notification
 * side is handled as part of the normal put path.  We should make sure that
 * a null closer works, transfer the log messages which are not being
 * duplicated, and get rid of this vestigal appendage.
 */
static void
rms_mmsmc_reput_cb(plat_closure_scheduler_t *context, void *env,
                   SDF_status_t status, struct cr_shard_meta *shard_meta,
                   struct timeval lease_expires) {
    struct replicator_meta_storage *rms = (struct replicator_meta_storage *)env;
    enum plat_log_level log_level;

    if (status == SDF_SUCCESS) {
        log_level = PLAT_LOG_LEVEL_TRACE;
    /*
     * This can be a normal consequence of startup in the ddistributed state,
     * where the remote node attempts a create with no lease held and the
     * local node holds a lease.  Or it can be an actual conflict from split
     * brain which is more interesting.  #rms_op_allowed or whatever is
     * detecting the error should log appropriately.
     */
    } else if (status == SDF_LEASE_EXISTS) {
        log_level = PLAT_LOG_LEVEL_DIAGNOSTIC;
    } else {
        log_level = PLAT_LOG_LEVEL_WARN;
    }

    plat_log_msg(21435, LOG_CAT_DISTRIBUTED, log_level,
                 "rms %p node %u reput shard 0x%lx vip group %d: %s",
                 rms, rms->config.my_node,
                 shard_meta ? (long)shard_meta->persistent.sguid : -1L,
                 shard_meta ?
                 (int)shard_meta->persistent.intra_node_vip_group_id : -1,
                 sdf_status_to_string(status));

#if 0
    if (status == SDF_SUCCESS) {
        /*
         * The update is called twice with the second time reflecting the
         * time in which updates should be rejected
         */
        plat_closure_apply(rms_shard_meta_cb, &rms->update_cb, status,
                           shard_meta, lease_expires);
    } else if (shard_meta)
#endif

    {
        cr_shard_meta_free(shard_meta);
    }

    rms_ref_count_dec(rms);
}

/** @brief Release one ref count */
static void
rms_ref_count_dec(struct replicator_meta_storage *rms) {
    int after;
    rms_shutdown_cb_t shutdown_cb;

    after = __sync_sub_and_fetch(&rms->ref_count, 1);

    plat_assert(after >= 0);

    if (!after) {
        /* Shards were removed as they shutdown */
        plat_assert(TAILQ_EMPTY(&rms->shards));

        shutdown_cb = rms->shutdown_cb;
        if (rms->live.nodes) {
            plat_free(rms->live.nodes);
        }
        plat_free(rms);
        rms_shutdown_cb_apply(&shutdown_cb);
    }
}

/**
 * @brief Shutdown shard asynchronously
 *
 * Shutdown is simply waiting for the operation to quiesce.  On completion,
 * one reference count of the parent replicator_meta_storage object will
 * be consumed.
 */

static void
rms_shard_shutdown(struct rms_shard *shard) {
    struct replicator_meta_storage *rms;

    rms = shard->rms;

    plat_log_msg(21436, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "rms %p node %u shutdown shard 0x%lx",
                 rms, rms->config.my_node, shard->sguid);

    plat_assert(shard->state != RMS_SHARD_STATE_TO_SHUTDOWN);
    shard->state = RMS_SHARD_STATE_TO_SHUTDOWN;

    rms_shard_lease_cancel_timeout(shard);
    rms_shard_beacon_cancel(shard);

    rms_shard_ref_count_dec(shard);
}

/** @brief Release one shard ref count */
static void
rms_shard_ref_count_dec(struct rms_shard *shard) {
    struct replicator_meta_storage *rms;
    int after;

    rms = shard->rms;
    plat_assert(rms);

    after = __sync_sub_and_fetch(&shard->ref_count, 1);
    plat_assert(after >= 0);

    if (!after || (shard->state == RMS_SHARD_STATE_DELETED &&
                   rms->state != RMS_STATE_TO_SHUTDOWN && after == 1)) {
        plat_log_msg(21437, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "rms %p node %u shard 0x%lx ref count zero state=%s",
                     rms, rms->config.my_node, shard->sguid,
                     rms_shard_state_to_string(shard->state));

        plat_assert(shard->state == RMS_SHARD_STATE_TO_SHUTDOWN ||
                    shard->state == RMS_SHARD_STATE_DELETED);

        /* All operations must have completed */
        plat_assert(shard->op_msgtype == SDF_PROTOCOL_MSG_INVALID);
        plat_assert(TAILQ_EMPTY(&shard->blocked_ops));

        if (shard->mbx) {
            sdf_fth_mbx_free(shard->mbx);
        }

        if (shard->shard_meta) {
            cr_shard_meta_free(shard->shard_meta);
        }

        if (shard->lease.timeout_event_name) {
            plat_free(shard->lease.timeout_event_name);
        }

        if (shard->beacon_event_name) {
            plat_free(shard->beacon_event_name);
        }

        TAILQ_REMOVE(&rms->shards, shard, shards_entry);

        plat_free(shard);

        rms_ref_count_dec(rms);
    }
}

/**
 * @brief Co-routine handling work
 *
 * Starts whatever operation needs to be started, deliver results, and
 * return for system to sleep on returns or new requests.
 *
 * @param shard <IN> Shard to operate on
 * @param op_arg <IN> New operation
 */
static void
rms_shard_do_work(struct rms_shard *shard, struct rms_op *op_arg) {
    struct replicator_meta_storage *rms = shard->rms;
    struct rms_op *op = NULL;
    SDF_status_t status;
    void *data = NULL;
    struct sdf_msg *msg = NULL;
    struct SDF_protocol_msg *pm = NULL;
    SDF_Protocol_Msg_Info_t *pmi;
    struct SDF_shard_meta *meta_shard_meta;
    size_t header_len = sizeof (*msg) + sizeof (*pm);
    size_t total_len = 0;
    struct sdf_msg_wrapper *msg_wrapper;

    /*
     * Tweak reference count so that the shard cannot become invalid
     * because of deletion or shutdown in the loop.
     */
    ++shard->ref_count;

    ++shard->do_work_running;

    if (op_arg && op_arg->op_type == RMS_OP_GET && shard->shard_meta) {
        rms_shard_lease_update(shard);
        rms_op_end(op_arg, SDF_SUCCESS, NULL);
    }

    while (shard->do_work_running == 1 &&
           !TAILQ_EMPTY(&shard->blocked_ops) && !msg &&
           shard->op_msgtype == SDF_PROTOCOL_MSG_INVALID) {

        op = TAILQ_FIRST(&shard->blocked_ops);
        plat_assert_imply(rms_op_type_is_put(op->op_type), op->shard_meta);

        if (shard->state == RMS_SHARD_STATE_TO_SHUTDOWN) {
            rms_op_end(op, SDF_SHUTDOWN, NULL);
        } else if (shard->state == RMS_SHARD_STATE_DELETED) {
            rms_op_end(op, SDF_SHARD_DOES_NOT_EXIST, NULL);
        } else if (shard->state == RMS_SHARD_STATE_INITIAL) {
            /*
             * XXX: drew 2009-08-26 Currently we assume that distributed
             * meta-data is non-persistent; although long term that information
             * should propagate down from the shard creation code in
             * #cr_shard_meta.
             */
            if (rms_op_type_is_put(op->op_type) &&
                (rms_replication_type_flags(op->shard_meta->persistent.type) &
                 RRTF_META_DISTRIBUTED)) {
                shard->state = RMS_SHARD_STATE_NO_META;
            } else {
                total_len = header_len;
                msg = sdf_msg_calloc(total_len);
                plat_assert_always(msg);
                pm = (SDF_protocol_msg_t *)msg->msg_payload;
                pm->msgtype = HFGFF;
                rms_shard_to_meta_key(shard, &pm->key);
            }
        } else if ((shard->state == RMS_SHARD_STATE_NO_FORMAT ||
                    shard->state == RMS_SHARD_STATE_NO_META) &&
                   op->op_type != RMS_OP_CREATE &&
                   op->op_type != RMS_OP_REPUT) {
            rms_op_end(op, SDF_SHARD_DOES_NOT_EXIST, NULL);
        /*
         * XXX: drew 2009-06-09 Block on the format completing if
         * the #rms_start has yet to complete.
         */
        } else if (shard->state == RMS_SHARD_STATE_NO_FORMAT &&
                   (op->op_type == RMS_OP_CREATE ||
                    op->op_type == RMS_OP_REPUT)) {
            total_len = header_len + sizeof (*meta_shard_meta);
            msg = sdf_msg_calloc(total_len);
            plat_assert_always(msg);
            pm = (SDF_protocol_msg_t *)msg->msg_payload;

            pm->msgtype = HFCSH;
            pm->flags   = f_open_ctnr;

            meta_shard_meta = (struct SDF_shard_meta *)((char *)(pm + 1));

            meta_shard_meta->sguid = shard->sguid_meta;
            meta_shard_meta->type = SDF_SHARD_TYPE_OBJECT;
            meta_shard_meta->persistence = SDF_SHARD_PERSISTENCE_YES;
            meta_shard_meta->eviction = SDF_SHARD_EVICTION_STORE;
            meta_shard_meta->quota = RMS_META_QUOTA;

            meta_shard_meta->num_objs = RMS_META_OBJS;
        } else if (op->op_type == RMS_OP_CREATE || op->op_type == RMS_OP_PUT ||
                   op->op_type == RMS_OP_REPUT) {
            plat_assert(op->shard_meta);
            rms_shard_lease_update(shard);
            status = rms_op_allowed(op);
            if (status == SDF_SUCCESS) {
                /*
                 * XXX: drew 2009-08-26 Currently we assume that distributed
                 * meta-data is non-persistent; although long term that
                 * information should propagate down from the shard creation
                 * code in #cr_shard_meta.
                 */
                if (rms_replication_type_flags(op->shard_meta->persistent.type) &
                    RRTF_META_DISTRIBUTED) {
                    rms_shard_put_complete(shard, op, SDF_SUCCESS);
                } else {
                    status = cr_shard_meta_marshal(&data, &total_len,
                                                   header_len,
                                                   op->shard_meta);
                    msg = (struct sdf_msg *)data;
                    memset(msg, 0, header_len);
                    pm = (SDF_protocol_msg_t *)msg->msg_payload;
                    pm->flags = f_sync;
                    pm->msgtype = HFSET;
                    rms_shard_to_meta_key(shard, &pm->key);
                }
            }
            if (status != SDF_SUCCESS) {
                rms_op_end(op, status, NULL);
            }
        } else if (op->op_type == RMS_OP_GET) {
            rms_shard_lease_update(shard);
            rms_op_end(op, SDF_SUCCESS, NULL);
        } else if (op->op_type == RMS_OP_DELETE) {
            rms_shard_lease_update(shard);
            status = rms_op_allowed(op);
            if (status == SDF_SUCCESS) {
                /*
                 * FIXME: drew 2009-05-08 Implement delete.  We didn't
                 * have support for this in Jim's code
                 */
                plat_assert_always(0);
            } else {
                rms_op_end(op, status, NULL);
            }
        } else {
            plat_assert(0);
        }
    }
    --shard->do_work_running;

    if (msg) {
        plat_assert(op);
        plat_assert(pm);

        msg->msg_flags = 0;
        msg->msg_len = total_len;

        pm->shard = shard->sguid_meta;
        pm->data_offset = 0;
        pm->data_size = total_len - header_len;
        pm->node_from = rms->config.my_node;
        pm->node_to = rms->config.my_node;

        pm->status = SDF_SUCCESS;

        msg_wrapper =
            sdf_msg_wrapper_local_alloc(msg, sdf_msg_wrapper_free_local_null,
                                        SMW_MUTABLE_FIRST, SMW_TYPE_REQUEST,
                                        pm->node_from /* src */,
                                        rms->config.response_service /* src */,
                                        pm->node_to /* dest */,
                                        rms->config.flash_service /* dest */,
                                        FLSH_REQUEST,
                                        NULL /* new request not response */);
        plat_assert(msg_wrapper);
        pmi = &(SDF_Protocol_Msg_Info[pm->msgtype]);
        plat_log_msg(21438, LOG_CAT_FLASH, PLAT_LOG_LEVEL_TRACE,
                     "rms %p node %u shard 0x%lx op %p flash request"
                     " Msg: %s (%s) shard 0x%lx key %*.*s data_size %lu", rms,
                     rms->config.my_node, shard->sguid, op, pmi->name,
                     pmi->shortname, pm->shard, pm->key.len, pm->key.len,
                     pm->key.key, (unsigned long)pm->data_size);

        shard->op_msgtype = pm->msgtype;

        plat_closure_apply(sdf_replicator_send_msg_cb,
                           &rms->component_api.send_msg, msg_wrapper,
                           shard->mbx, NULL /* status */);
    }

    /* Release the ref count aquired at the start of this call */
    rms_shard_ref_count_dec(shard);
}

/**
 * @brief Handle flash response for shard operation
 *
 * @param context <IN> scheuler context.  Used for closure chaining
 * optimization.
 * @param env <IN> op from user request
 * @param response <IN> inbound flash response.  One reference count
 * is consumed.
 */
static void
rms_shard_flash_response(struct plat_closure_scheduler *context, void *env,
                         struct sdf_msg_wrapper *response) {
    struct rms_shard *shard = (struct rms_shard *)env;
    struct replicator_meta_storage *rms = shard->rms;
    struct rms_op *op;
    struct sdf_msg *msg;
    SDF_protocol_msg_t *pm;
    struct sdf_msg_error_payload *error_payload;
    SDF_status_t msg_status;
    enum plat_log_level log_level;
    SDF_protocol_msg_type_t op_msgtype;

    op_msgtype = shard->op_msgtype;
    plat_assert(op_msgtype != SDF_PROTOCOL_MSG_INVALID);

    op = TAILQ_FIRST(&shard->blocked_ops);
    /* Otherwise there wouldn't have been an operation */
    plat_assert(op);

    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, response);

    /* FIXME: Look for shutdown here */
    if (msg->msg_type == SDF_MSG_ERROR) {
        error_payload = (struct sdf_msg_error_payload *)msg->msg_payload;
        pm = NULL;
        msg_status = error_payload->error;

        plat_log_msg(21439, LOG_CAT, PLAT_LOG_LEVEL_WARN,
                     "rms %p node %u shard 0x%lx op %p flash response received"
                     " error status %s",
                     rms, rms->config.my_node, shard->sguid, op,
                     sdf_status_to_string(msg_status));
    } else {
        pm = (SDF_protocol_msg_t *)msg->msg_payload;
        msg_status = pm->status;

        if (msg_status == SDF_SUCCESS ||
            /* Non-existing gets on behalf of put operations are OK */
            ((msg_status == SDF_CONTAINER_UNKNOWN ||
              msg_status == SDF_SHARD_NOT_FOUND ||
              msg_status == SDF_OBJECT_UNKNOWN) &&
             op_msgtype == HFGFF &&
             (op->op_type == RMS_OP_CREATE || op->op_type == RMS_OP_REPUT))) {
            log_level = PLAT_LOG_LEVEL_TRACE;
        } else {
            log_level = PLAT_LOG_LEVEL_WARN;
        }

        SDF_Protocol_Msg_Info_t *pmi = &(SDF_Protocol_Msg_Info[pm->msgtype]);
        plat_log_msg(21440, LOG_CAT_FLASH, log_level,
                     "rms %p node %u shard 0x%lx op %p flash response received"
                     " Msg: %s (%s) status %s",
                     rms, rms->config.my_node, shard->sguid, op, pmi->name,
                     pmi->shortname, sdf_status_to_string(msg_status));
    }

    /* Avoid free of structures in callback */
    ++shard->ref_count;

    if (shard->state == RMS_SHARD_STATE_TO_SHUTDOWN) {
        rms_op_end(op, SDF_SHUTDOWN, NULL);
    } else if (op_msgtype == HFGFF) {
        plat_assert(shard->state == RMS_SHARD_STATE_INITIAL);
        plat_assert(!shard->shard_meta);

        /*
         * drew 2009-06-03 New flash code apparantly does object unknown
         * and then asserts.  Would be nice to have documented APIs people
         * follow.
         */
        if (msg_status == SDF_CONTAINER_UNKNOWN ||
            msg_status == SDF_SHARD_NOT_FOUND ||
#ifndef notyet
            (msg_status == SDF_OBJECT_UNKNOWN &&
             (op->op_type == RMS_OP_CREATE || op->op_type == RMS_OP_REPUT)) ||
#endif
            0) {
            shard->state = RMS_SHARD_STATE_NO_FORMAT;
        } else if (msg_status == SDF_OBJECT_UNKNOWN) {
            shard->state = RMS_SHARD_STATE_NO_META;
        } else if (msg_status != SDF_SUCCESS) {
            shard->state = RMS_SHARD_STATE_INITIAL;
            rms_op_end(op, msg_status, NULL);
        } else {
            plat_assert(msg_status == SDF_SUCCESS);
            msg_status = cr_shard_meta_unmarshal(&shard->shard_meta,
                                                 (void *)(pm + 1),
                                                 pm->data_size);
            if (msg_status != SDF_SUCCESS) {
                plat_log_msg(21441, LOG_CAT,
                             PLAT_LOG_LEVEL_ERROR,
                             "rms  %p shard 0x%lx get failed to unmarshal"
                             " status %s",
                             rms, shard->sguid,
                             sdf_status_to_string(msg_status));
                rms_op_end(op, msg_status, NULL);
            /* Update timeout */
            } else {
                shard->state = RMS_SHARD_STATE_NORMAL;
                rms_shard_schedule_lease_timeout(shard);
                rms_shard_notify(shard);
            }
        }
    } else if (op_msgtype == HFCSH) {
        plat_assert(op->op_type == RMS_OP_CREATE ||
                    op->op_type == RMS_OP_REPUT);
        plat_assert(shard->state == RMS_SHARD_STATE_NO_FORMAT);
        plat_assert(!shard->shard_meta);

        /*
         * XXX: drew 2009-08-21 the current incarnation of the
         * shardCreate interface only returns NULL on failure
         * without specifying what the error is.
         *
         * sdf/protocol/home/home_flash.c translates that
         * into SDF_FAILURE_STORAGE_WRITE.  So for now we treat
         * SDF_FAILURE_STORAGE_WRITE as if the shard already
         * exists.
         */
        if (msg_status == SDF_SUCCESS ||
#ifndef notyet
            msg_status == SDF_FAILURE_STORAGE_WRITE
#else /* def notyet */
            0
#endif /* else def notyet */
            /* cstyle */) {
            shard->state = RMS_SHARD_STATE_NO_META;
        } else {
            rms_op_end(op, msg_status, NULL);
        }
    } else if (op_msgtype == HFSET) {
        rms_shard_put_complete(shard, op, msg_status);
    } else {
        plat_assert(0);
    }

    sdf_msg_wrapper_rwrelease(&msg, response);
    sdf_msg_wrapper_ref_count_dec(response);

    shard->op_msgtype = SDF_PROTOCOL_MSG_INVALID;

    rms_shard_do_work(shard, NULL);

    rms_shard_ref_count_dec(shard);
}

/**
 * @brief Handle completion of create/put/reput operation at shard level
 *
 * @param op <IN> create, put, or reput operation which has yet to
 * complete fully.
 * @param status <IN> status of corresponding flash or synthetic
 * operation
 */
static void
rms_shard_put_complete(struct rms_shard *shard, struct rms_op *op,
                       SDF_status_t status) {
    int flags;

    plat_assert(shard == op->shard);
    plat_assert(op->op_type == RMS_OP_CREATE || op->op_type == RMS_OP_PUT ||
                op->op_type == RMS_OP_REPUT);
    plat_assert_iff(!shard->shard_meta,
                    shard->state == RMS_SHARD_STATE_NO_META);
    plat_assert_iff(shard->shard_meta,
                    shard->state == RMS_SHARD_STATE_NORMAL);

    plat_assert(op->shard_meta);
    flags = rms_replication_type_flags(op->shard_meta->persistent.type);

    /*
     * XXX: drew 2009-08-16 Should use common sanity check code,
     * like an op-allowed in assert mode.
     */

    /*
     * Note that the net effect for RRTF_META_DISTRIBUTED is approximate
     * last write wins, with different nodes varying back-offs resulting
     * in eventual consistency.
     *
     * While sub-optimal this is not important since we are implementing
     * the Paxos based scheme for meta-data with single copy semantics,
     * and all customers big enough to be interesting will be buying at
     * least the three nodes needed to have a subset of machines as quorum.
     */
    plat_assert(!shard->shard_meta || !shard->lease.exists ||
                shard->shard_meta->persistent.current_home_node ==
                op->shard_meta->persistent.current_home_node ||
                (shard->shard_meta->persistent.lease_liveness &&
                 op->shard_meta->persistent.current_home_node ==
                 CR_HOME_NODE_NONE) ||
                (flags & RRTF_META_DISTRIBUTED));
    plat_assert(!shard->shard_meta ||
                op->shard_meta->persistent.shard_meta_seqno ==
                shard->shard_meta->persistent.shard_meta_seqno + 1 ||
                (flags & RRTF_META_DISTRIBUTED));

    if (status == SDF_SUCCESS) {
        shard->state = RMS_SHARD_STATE_NORMAL;
        if (shard->shard_meta) {
            cr_shard_meta_free(shard->shard_meta);
        }
        /* op->shard_meta becomes owned by shard->shard_meta */
        shard->shard_meta = op->shard_meta;
        op->shard_meta = NULL;
        if (op->op_type == RMS_OP_REPUT) {
            shard->shard_meta_reput = 1;
        } else {
            shard->shard_meta_reput = 0;
        }
        rms_shard_schedule_lease_timeout(shard);
        rms_shard_notify(shard);
    /*
     * XXX: drew 2010-05-03 This should be able to go away once we have the
     * seqno based comparison in place.
     */
    } else if (status != SDF_UPDATE_DUPLICATE && op->op_type == RMS_OP_REPUT) {
        rms_shard_notify(shard);
    }

    rms_op_end(op, status, NULL);
}

/**
 * @brief Notify consumers of the shard's state
 *
 * The local node is notified of all state changes, including those originated
 * as #RMS_OP_REPUT.  Remote nodes are only notified by local state changes from
 * RMS_OP_CREATE and RMS_OP_PUT.
 *
 * Called on changes+lease renewals when a home node exists and on periodic
 * pilot beacon timer events otherwise.
 */
static void
rms_shard_notify(struct rms_shard *shard) {
    struct replicator_meta_storage *rms;
    struct cr_shard_meta *shard_meta;
    int i;
    vnode_t node;

    rms = shard->rms;

    /* Avoid deletions if the notification callback is synchronous */
    ++shard->ref_count;

    if (shard->shard_meta) {
        if (!rms_shard_meta_cb_is_null(&rms->update_cb) &&
            !rms->config.meta_by_message) {
            shard_meta = cr_shard_meta_dup(shard->shard_meta);
            plat_assert(shard_meta);
            plat_closure_apply(rms_shard_meta_cb, &rms->update_cb, SDF_SUCCESS,
                               shard_meta, shard->lease.expires);
        }

        /*
         * XXX: drew 2009-05-21 This should change so that we only notify
         * listeners about shards that they're interested in.  It would
         * be trivial to iterate over the shards replicas and do this.
         *
         * It would also be more efficient than searching for each node
         * on the replica list in rms_shard_notify_node_by_msg, but doing it there
         * would also catch the node startup case.
         */
        for (i = 0; i < rms->live.nnodes; ++i) {
            node = rms->live.nodes[i];
            if (node != rms->config.my_node || rms->config.meta_by_message) {
                rms_shard_notify_node_by_msg(shard, node);
            }
        }
    }

    rms_shard_ref_count_dec(shard);
}

/**
 * @brief Notify node that this shard has changed via a message
 *
 * XXX: drew 2009-05-21 We should not notify nodes which don't care about this
 * shard
 *
 * The local node is notified of all state changes, including those originated
 * as #RMS_OP_REPUT.  Remote nodes are only notified by local state changes from
 * RMS_OP_CREATE and RMS_OP_PUT.
 *
 * Also called when the node first comes online
 */
static void
rms_shard_notify_node_by_msg(struct rms_shard *shard, vnode_t node) {
    /* Skip if no meta-data */
    if (!shard->shard_meta) {
        plat_log_msg(21837, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "rms %p node %u shard 0x%lx skip notify node %u"
                     " (no meta)", shard->rms, shard->rms->config.my_node,
                     shard->sguid, node);
    /* Skip if cached version and trying to notify remote node */
    } else if (shard->shard_meta_reput && node != shard->rms->config.my_node) {
        plat_log_msg(21838, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "rms %p node %u shard 0x%lx skip notify node %u"
                     " (local version is reput)", shard->rms,
                     shard->rms->config.my_node, shard->sguid, node);
    /* Otherwise do message based update */
    } else {
        rms_shard_do_notify_node_by_msg(shard, node);
    }
}

/**
 * @brief Perform actual notification of shard state
 *
 * @param shard <IN> shard with meta-data being transmitted
 * @param node <IN> node to notify
 */
static void
rms_shard_do_notify_node_by_msg(struct rms_shard *shard, vnode_t node) {
    struct replicator_meta_storage *rms;
    struct sdf_msg *msg;
    struct SDF_protocol_msg *pm = NULL;
    SDF_Protocol_Msg_Info_t *pmi;
    struct sdf_msg_wrapper *msg_wrapper;
    size_t header_len = sizeof (*msg) + sizeof (*pm);

    rms = shard->rms;
    msg = sdf_msg_calloc(header_len);
    msg->msg_flags = 0;
    msg->msg_len = header_len;
    plat_assert_always(msg);

    pm = (struct SDF_protocol_msg *)msg->msg_payload;

    pm->msgtype = MMSMC;
    pm->status = SDF_SUCCESS;
    pm->node_from = rms->config.my_node;
    pm->node_to = node;
    pm->shard = shard->sguid;

    /*
     * XXX: drew 2009-08-16 This may not be correct where we're
     * acting as a proxy; although in that case we shouldn't be
     * sending the update.
     */
    pm->op_meta.shard_meta.meta_pnode = rms->config.my_node;
    pm->op_meta.shard_meta.meta_shardid = shard->sguid_meta;

    msg_wrapper =
        sdf_msg_wrapper_local_alloc(msg, sdf_msg_wrapper_free_local_null,
                                    SMW_MUTABLE_FIRST,
                                    SMW_TYPE_ONE_WAY,
                                    /* XXX: drew 2009-05-09 from responses? */
                                    pm->node_from /* src */,
                                    SDF_REPLICATION_PEER_META_SUPER /* src */,
                                    pm->node_to /* dest */,
                                    SDF_REPLICATION_PEER_META_SUPER /* dest */,
                                    META_REQUEST,
                                    NULL /* new request not response */);
    plat_assert(msg_wrapper);
    pmi = &(SDF_Protocol_Msg_Info[pm->msgtype]);
    plat_log_msg(21442, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "rms %p node %u shard 0x%lx notify node %u"
                 " Msg: %s (%s)", rms, rms->config.my_node,
                 shard->sguid, node, pmi->name, pmi->shortname);

    plat_closure_apply(sdf_replicator_send_msg_cb,
                       &rms->component_api.send_msg, msg_wrapper,
                       NULL /* no response expected */, NULL /* status */);
}

/**
 * @brief Indicate that a node should be treated as dead
 *
 * @param pnode <IN> Node which is dead.
 */

/**
 * @brief Start or renew lease per contents of shard->shard_meta
 *
 * The timeout mechanics are setup for the lease per the contents of
 * shard->shard_meta.  Lease state must be persisted before this function
 * is called and update shard meta-data propagated to consumers or acknowledged
 * to the #rms_put_shard_meta caller
 */
static void
rms_shard_schedule_lease_timeout(struct rms_shard *shard) {
    struct replicator_meta_storage *rms;
    struct timeval now;
    plat_event_fired_t fired;

    plat_assert(shard->shard_meta);
    rms = shard->rms;

    /* Hold refcount on until timeout event is freed */
    ++shard->ref_count;

    rms_shard_lease_cancel_timeout(shard);
    plat_assert(!shard->lease.timeout_event);

    if (!shard->shard_meta->persistent.lease_usecs) {
        shard->lease.exists = 0;
        if (!shard->beacon_event) {
            rms_shard_beacon_reset(shard);
        }
        /* undo */
        rms_shard_ref_count_dec(shard);
    } else {
#ifndef RMS_BEACON_ALWAYS_ON
        rms_shard_beacon_cancel(shard);
#endif /* ndef RMS_BEACON_ALWAYS_ON */
        shard->lease.exists = 1;
        plat_closure_apply(plat_timer_dispatcher_gettime,
                           &rms->component_api.gettime, &now);

        if (shard->shard_meta->persistent.lease_liveness) {
            shard->lease.expires = rms_tv_infinite;
            shard->shard_meta->persistent.lease_usecs =
                (shard->lease.expires.tv_sec - now.tv_sec) * MILLION +
                (shard->lease.expires.tv_usec - now.tv_usec);
        } else {
            shard->lease.expires.tv_sec = now.tv_sec +
                (now.tv_usec + shard->shard_meta->persistent.lease_usecs) /
                MILLION;
            shard->lease.expires.tv_usec =
                (now.tv_usec + shard->shard_meta->persistent.lease_usecs) %
                MILLION;
        }

        plat_log_msg(21444, LOG_CAT_LEASE, PLAT_LOG_LEVEL_DEBUG,
                     "rms %p node %u shard 0x%lx vip group %d lease expires"
                     " in %3.1f seconds at %s",
                     rms, rms->config.my_node,
                     shard->shard_meta->persistent.sguid,
                     shard->shard_meta->persistent.intra_node_vip_group_id,
                     shard->shard_meta->persistent.lease_usecs / (float)MILLION,
                     plat_log_timeval_to_string(&shard->lease.expires));

        fired = plat_event_fired_create(rms->closure_scheduler,
                                        &rms_shard_lease_expired_cb, shard);
        shard->lease.timeout_event =
            plat_timer_dispatcher_timer_alloc(rms->component_api.timer_dispatcher,
                                              "rms lease timeout",
                                              LOG_CAT_EVENT, fired,
                                              1 /* free_count */,
                                              &shard->lease.expires,
                                              PLAT_TIMER_ABSOLUTE,
                                              NULL /* rank_ptr */);
    }
}

/**
 * @brief Called on lease expiration
 */
static void
rms_shard_lease_expired_cb(plat_closure_scheduler_t *context, void *env,
                           struct plat_event *event) {
    struct rms_shard *shard = (struct rms_shard *)env;
    struct replicator_meta_storage *rms;

    rms = shard->rms;

    plat_assert(shard->shard_meta || shard->state == RMS_SHARD_STATE_DELETED);

    /*
     * Only act when we haven't hit the race condition where the timeout event
     * delivery occurs after a pending put has completed and cancelled the
     * timeout.  In that case shard->lease.timeout_event would be non-null.
     *
     * With distributed meta-data storage we'll have lease timeouts when
     * remote nodes do not renew their leases at their local node.
     */
    if (shard->lease.timeout_event == event) {
        plat_log_msg(21445, LOG_CAT_LEASE,
                     shard->shard_meta->persistent.current_home_node ==
                     rms->config.my_node ? PLAT_LOG_LEVEL_WARN :
                     PLAT_LOG_LEVEL_DIAGNOSTIC,
                     "rms %p node %u shard 0x%lx vip group %d lease expired"
                     " home was %d",
                     rms, rms->config.my_node,
                     shard->shard_meta ? shard->shard_meta->persistent.sguid :
                     SDF_SHARDID_INVALID,
                     shard->shard_meta ?
                     shard->shard_meta->persistent.intra_node_vip_group_id :
                     VIP_GROUP_ID_INVALID,
                     shard->shard_meta ?
                     shard->shard_meta->persistent.current_home_node :
                     CR_HOME_NODE_NONE);


        rms_shard_lease_cancel_timeout(shard);
        /* This is only needed as a sanity check */
        rms_shard_lease_update(shard);

        /*
         * There was at least one place where non-monotonic time
         * caused this not to update to no time remaining.  Log
         * instead of terminating.
         */
#if 0
        plat_assert(!shard->lease.exists);
#else /* if 0 */
        if (shard->lease.exists) {
            plat_log_msg(21446, LOG_CAT_LEASE,
                         PLAT_LOG_LEVEL_WARN,
                         "rms %p node %u shard 0x%lx vip group %d lease expired"
                         " home was %d but exists with %ld usecs remain",
                         rms, rms->config.my_node,
                         shard->shard_meta ? shard->shard_meta->persistent.sguid :
                         SDF_SHARDID_INVALID,
                         shard->shard_meta ?
                         shard->shard_meta->persistent.intra_node_vip_group_id :
                         VIP_GROUP_ID_INVALID,
                         shard->shard_meta ?
                         shard->shard_meta->persistent.current_home_node :
                         CR_HOME_NODE_NONE,
                         shard->shard_meta ?
                         shard->shard_meta->persistent.lease_usecs : -1);
            shard->lease.exists = 0;
            if (shard->shard_meta) {
                shard->shard_meta->persistent.lease_usecs = 0;
            }
        }
#endif /* else 0 */

        rms_shard_set_lease_none(shard);
    }
}

/** @brief Cancel existing lease timeout. */
static void
rms_shard_lease_cancel_timeout(struct rms_shard *shard) {
    if (shard->lease.timeout_event) {
        rms_shard_free_event(shard, shard->lease.timeout_event);
        shard->lease.timeout_event = NULL;
    }
}

/** @brief Signal to shard that the given node has died */
static void
rms_shard_node_dead(struct rms_shard *shard, vnode_t pnode) {
    if (shard->shard_meta &&
        shard->shard_meta->persistent.lease_liveness &&
        shard->shard_meta->persistent.current_home_node == pnode) {

        plat_log_msg(21447, LOG_CAT_LEASE_LIVENESS,
                     PLAT_LOG_LEVEL_DEBUG,
                     "rms_shard %p node %u node %u dead", shard,
                     shard->rms->config.my_node, pnode);

        rms_shard_set_lease_none(shard);
    }
}

/*
 * @brief Reset the home node to CR_HOME_NODE_NONE
 *
 * Operates unconditionally.
 */
static void
rms_shard_set_lease_none(struct rms_shard *shard) {
    struct cr_shard_meta *shard_meta;
    vnode_t old_home;

    /* Unless being deleted, persist the new meta-data */
    if (shard->state != RMS_SHARD_STATE_DELETED && shard->shard_meta) {
        /*
         * FIXME: drew 2010-03-12 In RRTF_META_DISTRIBUTED mode with
         * persistent meta-data in-core information not updating until
         * after write completion combined with no enforcement of causality
         * means that a queued and executing meta-data update may be
         * subsumed by a #rms_shard_set_lease_none call generated
         * due to an earlier lease timing out.
         *
         * Enforcing causality among local writes (keeping their
         * sequence numbers monotonic) may be the simplest fix.
         *
         * Short term we should get away with doing nothing since
         * all writes should complete synchronously where we have no
         * persistence and the flash system delays that go with that.
         */
        rms_shard_lease_cancel_timeout(shard);
        shard->lease.exists = 0;

        old_home = shard->shard_meta->persistent.current_home_node;
        shard->shard_meta->persistent.current_home_node = CR_HOME_NODE_NONE;
        shard->shard_meta->persistent.lease_usecs = 0;

        shard_meta = cr_shard_meta_dup(shard->shard_meta);
        plat_assert(shard_meta);

        shard_meta->persistent.last_home_node = old_home;

        shard_meta->persistent.current_home_node = CR_HOME_NODE_NONE;
        shard_meta->persistent.write_node = shard->rms->config.my_node;
        shard_meta->persistent.lease_usecs = 0;
        /*
         * XXX: drew 2009-05-20 Technically speaking only seqno
         * needs to advance, with ltime remaining unchanged until
         * a new node becomes the home node.  To keep things sane in
         * that optimization we might just leave the current home
         * node where it was.
         */
        ++shard_meta->persistent.ltime;
        ++shard_meta->persistent.shard_meta_seqno;
        rms_put_shard_meta(shard->rms, shard_meta, rms_shard_meta_cb_null);
        /* rms_put_shard_meta makes a copy */
        cr_shard_meta_free(shard_meta);
    }
}

/** @brief Reset pilot beacon timer */
static void
rms_shard_beacon_reset(struct rms_shard *shard) {
    struct replicator_meta_storage *rms;
    struct timeval interval;
    plat_event_fired_t fired;

    plat_assert(shard->shard_meta);

    rms = shard->rms;

    /* Hold refcount on until timeout event is freed */
    ++shard->ref_count;

    rms_shard_beacon_cancel(shard);
    plat_assert(!shard->beacon_event);

    if (
#ifndef RMS_BEACON_ALWAYS_ON
        shard->shard_meta->persistent.lease_usecs ||
#endif /* ndef RMS_BEACON_ALWAYS_ON */
        shard->state == RMS_SHARD_STATE_TO_SHUTDOWN ||
        shard->state == RMS_SHARD_STATE_DELETED) {
        /* undo */
        rms_shard_ref_count_dec(shard);
    } else {
        fired = plat_event_fired_create(rms->closure_scheduler,
                                        &rms_shard_beacon_cb, shard);
        interval.tv_sec = rms->config.lease_usecs / 2 / MILLION;
        interval.tv_usec = rms->config.lease_usecs / 2 % MILLION;
        shard->beacon_event =
            plat_timer_dispatcher_timer_alloc(rms->component_api.timer_dispatcher,
                                              shard->beacon_event_name,
                                              LOG_CAT_EVENT, fired,
                                              1 /* free_count */, &interval,
                                              PLAT_TIMER_RELATIVE,
                                              NULL /* rank_ptr */);
    }
}

/** @brief Called on pilot beacon timer expiration */
static void
rms_shard_beacon_cb(plat_closure_scheduler_t *context, void *env,
                    struct plat_event *event) {
    struct rms_shard *shard = (struct rms_shard *)env;
    struct replicator_meta_storage *rms;

    rms = shard->rms;

    plat_assert(shard->shard_meta || shard->state == RMS_SHARD_STATE_DELETED);

    /*
     * Only act when we haven't hit the race condition where the event
     * delivery occurs after cancellation.
     */
    if (shard->beacon_event == event) {
        plat_log_msg(21448, LOG_CAT_BEACON, PLAT_LOG_LEVEL_TRACE,
                     "rms %p node %u shard 0x%lx pilot beacon",
                     rms, rms->config.my_node, shard->sguid);

        rms_shard_beacon_reset(shard);

        if (
#ifndef RMS_BEACON_ALWAYS_ON
            !shard->lease.exists &&
#endif /* ndef RMS_BEACON_ALWAYS_ON */
            shard->state != RMS_SHARD_STATE_DELETED &&
            shard->state != RMS_SHARD_STATE_TO_SHUTDOWN &&
            shard->state != RMS_SHARD_STATE_DELETED) {
            /*
             * XXX: Drew 2009-08-28 If we really wanted to we could
             * split rms_shard_notify into rms_shard_notify_self and
             * rms_shard_notify_others and just call rms_shard_notify_others
             * here.
             */
            rms_shard_notify(shard);
        }
    }
}

static void
rms_shard_beacon_cancel(struct rms_shard *shard) {
    if (shard->beacon_event) {
        rms_shard_free_event(shard, shard->beacon_event);
        shard->beacon_event = NULL;
    }
}

/**
 * @brief Free a single event.
 *
 * This will ultimately cause the reference count on the shard to decrement
 * and shutdown to progress when in progress.
 */
static void
rms_shard_free_event(struct rms_shard *shard, struct plat_event *event) {
    struct replicator_meta_storage *rms;
    plat_event_free_done_t free_done_cb;

    rms = shard->rms;
    free_done_cb =
        plat_event_free_done_create(rms->closure_scheduler,
                                    &rms_shard_event_free_cb, shard);

    plat_event_free(event, free_done_cb);
}


/** @brief Callback on async cancellation of lease timeout */
static void
rms_shard_event_free_cb(plat_closure_scheduler_t *context, void *env) {
    struct rms_shard *shard = (struct rms_shard *)env;

    rms_shard_ref_count_dec(shard);
}

/** @brief make shard->shard_meta match time remaining on lease expiration */
static void
rms_shard_lease_update(struct rms_shard *shard) {
    struct replicator_meta_storage *rms;
    struct timeval now;

    rms = shard->rms;

    if (shard->shard_meta && shard->lease.exists) {
        plat_closure_apply(plat_timer_dispatcher_gettime,
                           &rms->component_api.gettime, &now);
        if (now.tv_sec > shard->lease.expires.tv_sec ||
            (now.tv_sec == shard->lease.expires.tv_sec &&
             now.tv_usec  >= shard->lease.expires.tv_usec)) {

#ifdef notyet
            /*
             * XXX: drew 2010-03-12 This is more elegant, but has the
             * side effect of starting a #RMS_OP_PUT on non #RRTF_META_PAXOS
             * meta-data which might have unforseen side effects (although
             * we do deal correctly with #rms_shard_do_work re-entrancy.
             */
            rms_shard_set_lease_none(shard);
#else /* def notyet */
            /*
             * Note that the in-core sequence number is not updated, thus
             * avoiding problems where a new write was already in-flight
             * with that sequence number thus resulting in two different
             * pieces of data with same sequence number.
             */
            shard->shard_meta->persistent.current_home_node = CR_HOME_NODE_NONE;
            shard->shard_meta->persistent.lease_usecs = 0;
            shard->lease.exists = 0;
#endif /* else def notyet */
        } else {
            shard->shard_meta->persistent.lease_usecs =
                (shard->lease.expires.tv_sec - now.tv_sec) * MILLION +
                (shard->lease.expires.tv_usec - now.tv_usec);
        }
    }
}

/**
 * @brief Get key used for shard's meta-data
 *
 * @param shard <IN> shard
 * @param key <OUT> key returned
 */
static void
rms_shard_to_meta_key(struct rms_shard *shard, SDF_simple_key_t *key) {
    key->len = snprintf(key->key, sizeof (key->key), "shard_meta_0x%lx",
                        (long)shard->sguid) + 1;
}

const char *
rms_shard_state_to_string(enum rms_shard_state state) {
    switch (state) {
#define item(caps, lower) case caps: return (#lower);
    RMS_SHARD_STATE_ITEMS()
#undef item
    }

    plat_assert(0);
    return ("invalid");
}

/**
 * @brief Validate that the op is allowed
 *
 * This enforces the modification rules
 *
 * XXX: drew 2010-05-03 The current scheme of ignoring sequence numbers for
 * RRTF_META_DISTRIBUTED is too complex.  We should switch to using them with
 * the write_node as a tie breaker with the only issue being backwards
 * compatability.
 *
 * As long as we're stuck with the distributed meta-data system
 * backwards compatability could be addressed by negotiating at
 * node live time with a new message with notifications of that
 * node deferred until
 * 1) Another command is received from it (indicating that the node is
 * v0)
 *
 * 2) The node negotiates to a newer version
 *
 * @param op <IN> Operation to check.
 * @return SDF_SUCCESS when the operation can proceed, the reason it
 * can't otherwise.
 */
static SDF_status_t
rms_op_allowed(struct rms_op *op) {
    struct replicator_meta_storage *rms;
    struct rms_shard *shard;
    SDF_status_t ret;
    const char *pass_reason = "unknown";

    rms = op->rms;
    shard = op->shard;

    plat_assert_imply(op->op_type == RMS_OP_CREATE ||
                      op->op_type == RMS_OP_PUT || op->op_type == RMS_OP_REPUT,
                      op->shard_meta);
    plat_assert_imply(op->op_type == RMS_OP_REPUT,
                      rms_replication_type_flags(op->shard_meta->persistent.type) &
                      RRTF_META_DISTRIBUTED);

    plat_assert(shard);
    plat_assert_imply(shard->state == RMS_SHARD_STATE_NORMAL,
                      shard->shard_meta);
    plat_assert_iff(shard->lease.exists, shard->shard_meta &&
                    shard->shard_meta->persistent.current_home_node !=
                    CR_HOME_NODE_NONE);

    /* Non-modifying operations allways succeed */
    if (op->op_type != RMS_OP_PUT && op->op_type != RMS_OP_REPUT &&
        op->op_type != RMS_OP_CREATE) {
        pass_reason = "not put";
        ret = SDF_SUCCESS;
    /*
     * Create operations with an existing shard are disallowed execpt
     * in the distributed mode where all the other constraints apply.
     */
    } else if (shard->state == RMS_SHARD_STATE_NORMAL &&
               op->op_type == RMS_OP_CREATE &&
               !(rms_replication_type_flags(op->shard_meta->persistent.type) &
                 RRTF_META_DISTRIBUTED)) {
        ret = SDF_CONTAINER_EXISTS;
    /*
     * Never over-write an existing lease with no lease unless the request is
     * from the node which formerly held the lease. This is an expected
     * situation for distributed meta-data in the create and reput cases
     * resulting in lower logging severity.
     */
    } else if (shard->state == RMS_SHARD_STATE_NORMAL &&
               op->shard_meta->persistent.current_home_node ==
               CR_HOME_NODE_NONE && shard->lease.exists &&
               op->shard_meta->persistent.write_node !=
               shard->shard_meta->persistent.current_home_node) {
        plat_log_msg(21449, LOG_CAT_OP,
                     (op->op_type == RMS_OP_REPUT ||
                      op->op_type == RMS_OP_CREATE) &&
                     (rms_replication_type_flags(op->shard_meta->persistent.type) &
                      RRTF_META_DISTRIBUTED) ? PLAT_LOG_LEVEL_DEBUG :
                     PLAT_LOG_LEVEL_WARN,
                     "rms %p node %u shard 0x%lx put with no lease but lease"
                     " exists for node %u", rms, rms->config.my_node,
                     shard->sguid,
                     shard->shard_meta->persistent.current_home_node);
        /*
         * XXX: 2009-08-21 Differentiate between a lease already existing
         * in this case (SDF_LEASE_EXISTS, this is a normal side effect of
         * startup) and a lease conflicting (SDF_LEASE_CONFLICTS, this is
         * a software error or split brain problem) so that we
         * can be more particular about log priority elsewhere.
         */
        ret = SDF_LEASE_EXISTS;
    /*
     * Correct split-brain problem by setting both sides to a lease which
     * will time-out.
     *
     * XXX: drew 2010-05-03 The better solution is eventual consistency
     * on seqno + write_node
     *
     * XXX: drew 2010-05-03 The right solution is to use strongly consistent
     * Paxos based meta-data instead of making this work better.
     */
    } else if (shard->state == RMS_SHARD_STATE_NORMAL &&
               (rms_replication_type_flags(op->shard_meta->persistent.type) &
                RRTF_META_DISTRIBUTED) &&
               op->shard_meta->persistent.write_node !=
               rms->config.my_node &&
               shard->shard_meta->persistent.current_home_node ==
               rms->config.my_node &&
               op->shard_meta->persistent.current_home_node !=
               rms->config.my_node) {
        /* Should have been filtered by above rule */
        plat_assert(op->shard_meta->persistent.current_home_node !=
                    CR_HOME_NODE_NONE);

        plat_log_msg(21852, LOG_CAT_OP, PLAT_LOG_LEVEL_WARN,
                     "rms %p node %u shard 0x%lx op %s lease exists request"
                     " home %d"
                     " current %d converting to put with no lease",
                     rms, rms->config.my_node, shard->sguid,
                     rms_op_type_to_string(op->op_type),
                     op->shard_meta->persistent.current_home_node,
                     shard->shard_meta->persistent.current_home_node);

        /*
         * XXX: drew 2010-05-14 Note that a log groveller is looking
         * for this message.  Changes must be co-ordinated with that
         * software.
         */
        plat_log_msg(21862, LOG_CAT_OP, PLAT_LOG_LEVEL_WARN,
                     "split brain detected - this node thinks home is %d"
                     " remote node %d thinks home is %d",
                     shard->shard_meta->persistent.current_home_node,
                     op->shard_meta->persistent.write_node,
                     op->shard_meta->persistent.current_home_node);

        op->op_type = RMS_OP_PUT;
        /*
         * XXX: drew 2010-05-03 Arguably cleaner to clone shard->shard_meta
         * and just replace lease fields.
         */
        op->shard_meta->persistent.current_home_node =
            shard->shard_meta->persistent.current_home_node;
        op->shard_meta->persistent.last_home_node =
            shard->shard_meta->persistent.last_home_node;
        op->shard_meta->persistent.write_node = rms->config.my_node;

        op->shard_meta->persistent.lease_usecs = 1;
        op->shard_meta->persistent.lease_liveness = 0;

        pass_reason = "distributed meta-data split brain";
        ret = SDF_SUCCESS;
    } else if (shard->state == RMS_SHARD_STATE_NORMAL &&
               (rms_replication_type_flags(op->shard_meta->persistent.type) &
                RRTF_META_DISTRIBUTED) &&
               op->op_type == RMS_OP_REPUT &&
               op->shard_meta->persistent.current_home_node ==
               CR_HOME_NODE_NONE &&
               shard->shard_meta->persistent.current_home_node ==
               CR_HOME_NODE_NONE) {
        plat_log_msg(21860, LOG_CAT_OP, PLAT_LOG_LEVEL_DEBUG,
                     "rms %p node %u shard 0x%lx put with no home when"
                     " home exists", rms, rms->config.my_node,
                     shard->sguid);
        ret = SDF_UPDATE_DUPLICATE;
    /*
     * Implement the eventually consistent scheme by eliminating the
     * causality rules for distributed updates since this will mean
     * fewer retries that would be a mess in a distributed system.
     *
     * XXX: drew 2009-08-28 We may want to maintain a local ltime
     * for such containers so that concurrent client side code gets
     * updates in-order.
     */
    } else if (shard->state == RMS_SHARD_STATE_NORMAL &&
               (rms_replication_type_flags(op->shard_meta->persistent.type) &
                RRTF_META_DISTRIBUTED)) {
        pass_reason = "distributed meta-data";
        ret = SDF_SUCCESS;
    /*
     * XXX: drew 2010-05-01 This is redundant for RRTF_META_DISTRIBUTED,
     * but may be needed for the Paxos case
     */
    } else if (shard->state == RMS_SHARD_STATE_NORMAL &&
               op->op_type == RMS_OP_REPUT) {
        pass_reason = "reput";
        ret = SDF_SUCCESS;
    } else if (shard->state == RMS_SHARD_STATE_NORMAL &&
               shard->lease.exists &&
               op->shard_meta->persistent.current_home_node !=
               shard->shard_meta->persistent.current_home_node &&
               (!shard->shard_meta->persistent.lease_liveness ||
                op->shard_meta->persistent.current_home_node !=
                CR_HOME_NODE_NONE)) {
        plat_log_msg(21450, LOG_CAT_OP, PLAT_LOG_LEVEL_WARN,
                     "rms %p node %u shard 0x%lx put lease exists request"
                     " home %d"
                     " current %d",
                     rms, rms->config.my_node, shard->sguid,
                     op->shard_meta->persistent.current_home_node,
                     shard->shard_meta->persistent.current_home_node);
        ret = SDF_LEASE_EXISTS;
    } else if (shard->state == RMS_SHARD_STATE_NORMAL &&
               op->shard_meta->persistent.shard_meta_seqno !=
               shard->shard_meta->persistent.shard_meta_seqno + 1) {
        plat_log_msg(21451, LOG_CAT_OP, PLAT_LOG_LEVEL_WARN,
                     "rms %p node %u shard 0x%lx put non-causal seqno %lld"
                     " current %lld",
                     rms, rms->config.my_node, shard->sguid,
                     (long long)op->shard_meta->persistent.shard_meta_seqno,
                     (long long)shard->shard_meta->persistent.shard_meta_seqno);
        ret = SDF_BAD_META_SEQNO;
    } else if (shard->state == RMS_SHARD_STATE_NORMAL &&
               !shard->lease.exists &&
               op->shard_meta->persistent.current_home_node !=
               shard->shard_meta->persistent.current_home_node &&
               op->shard_meta->persistent.ltime !=
               shard->shard_meta->persistent.ltime + 1) {
        plat_log_msg(21452, LOG_CAT_OP, PLAT_LOG_LEVEL_WARN,
                     "rms %p node %u shard 0x%lx put new home bad ltime "
                     " request %lu"
                     " current %lu",
                     rms, rms->config.my_node, shard->sguid,
                     (long)op->shard_meta->persistent.ltime,
                     (long)shard->shard_meta->persistent.ltime);
        ret = SDF_BAD_LTIME;
    /*
     * XXX: drew 2009-05-18 Also disallow shortening an existing lease
     * since read-only nodes depend on no other node getting the lease.
     *
     * All other modifications succeed; notably those that are outside
     * the normal mode which implies shard creation.
     */
    } else {
        plat_assert(op->shard_meta);
        pass_reason = "default";
        ret = SDF_SUCCESS;
    }

    if (ret == SDF_SUCCESS && rms_op_type_is_put(op->op_type)) {
        plat_log_msg(21861, LOG_CAT_OP, PLAT_LOG_LEVEL_TRACE,
                     "rms %p node %u shard 0x%lx op %s allowed"
                     " reason '%s' shard meta current home %d write node %d"
                     " last home %d"
                     " new home %d lease sec %3.1f meta seqno %lld ltime %lld",
                     rms, rms->config.my_node, shard->sguid,
                     rms_op_type_to_string(op->op_type),
                     pass_reason,
                     shard->shard_meta ?
                     shard->shard_meta->persistent.current_home_node : -2,
                     op->shard_meta->persistent.write_node,
                     op->shard_meta->persistent.last_home_node,
                     op->shard_meta->persistent.current_home_node,
                     (op->shard_meta->persistent.lease_usecs / (float)MILLION),
                     (long long)op->shard_meta->persistent.shard_meta_seqno,
                     (long long)op->shard_meta->persistent.ltime);
    }

    return (ret);
}

/**
 * @brief Complete operation
 *
 * preconditions:
 * - op is at the head of the blocked ops list or not attached to
 * the shard
 * - op still references rms so this is symetrical with rms_queue_op
 * - where state is present, it's up-to-date in the shard meta-data
 * @param status_arg <IN> status
 * @param shard_meta_arg <IN> shard meta to pass to user with ownership
 * transfer.  Where NULL and op->shard is non-null, op->shard->shard_meta
 * will be used instead.
 */
static void
rms_op_end(struct rms_op *op, SDF_status_t status_arg,
           struct cr_shard_meta *shard_meta_arg) {
    rms_shard_meta_cb_t cb;
    struct replicator_meta_storage *rms;
    struct rms_shard *shard;
    struct cr_shard_meta *shard_meta;
    enum plat_log_level log_level;
    SDF_status_t status;
    struct timeval now;
    long elapsed;
    struct timeval expires;

    status = status_arg;
    rms = op->rms;
    shard = op->shard;
    if (shard_meta_arg) {
        shard_meta = shard_meta_arg;
    } else if (shard && shard->shard_meta) {
        shard_meta = shard->shard_meta;
    } else {
        shard_meta = NULL;
    }
    cb = op->cb;

    /*
     * Internally generated puts on lease expiration don't have a message
     * or callback response.  Where there is a response, it must be either/or.
     */
    plat_assert(!(op->request_wrapper && !rms_shard_meta_cb_is_null(&op->cb)));

    plat_closure_apply(plat_timer_dispatcher_gettime,
                       &rms->component_api.gettime, &now);
    elapsed = (now.tv_sec - op->start_time.tv_sec) * MILLION +
        (now.tv_usec - op->start_time.tv_usec);

    if (shard) {
        TAILQ_REMOVE(&shard->blocked_ops, op, blocked_ops_entry);
    }

    if (status == SDF_SUCCESS || status == SDF_SHUTDOWN ||
        status == SDF_TEST_CRASH) {
        log_level = PLAT_LOG_LEVEL_TRACE;
    } else if (op->op_type == RMS_OP_REPUT) {
        log_level = PLAT_LOG_LEVEL_DIAGNOSTIC;
    } else {
        log_level = PLAT_LOG_LEVEL_WARN;
    }

    if (shard_meta) {
        /* XXX: drew 2009-08-20 Log at DIAG priority for REPUT failures */
        plat_log_msg(21453, LOG_CAT, log_level,
                     "rms %p node %u op %p %s complete status %s"
                     " shard 0x%lx vip group %d"
                     " home %d lease sec %3.1f elapsed %ld usec"
                     " meta seqno %lld ltime %lld",
                     rms, rms->config.my_node, op,
                     rms_op_type_to_string(op->op_type),
                     sdf_status_to_string(status),
                     shard_meta->persistent.sguid,
                     shard_meta->persistent.intra_node_vip_group_id,
                     shard_meta->persistent.current_home_node,
                     (shard_meta->persistent.lease_usecs / (float)MILLION),
                     elapsed,
                     (long long)shard_meta->persistent.shard_meta_seqno,
                     (long long)shard_meta->persistent.ltime);

    } else {
        plat_log_msg(21454, LOG_CAT, log_level,
                     "rms %p node %u op %p %s complete status %s",
                     rms, rms->config.my_node, op,
                     rms_op_type_to_string(op->op_type),
                     sdf_status_to_string(status));
    }

    if (op->request_wrapper) {
        /*
         * Since the time delay of the network plus the time delay of the
         * local operation is greater than just the local operation, the
         * adjustment only needs to be performed once we're converting
         * back to a callback.  Skip the adjustment for elapsed time
         * here.
         */
        rms_op_send_response(op, status);
        sdf_msg_wrapper_ref_count_dec(op->request_wrapper);
    } else if (!rms_shard_meta_cb_is_null(&cb)) {
        shard_meta = shard_meta_arg;
        if (!shard_meta && shard && shard->shard_meta) {
            shard_meta = cr_shard_meta_dup(shard->shard_meta);
            if (!shard_meta) {
                status = SDF_OUT_OF_MEM;
            }
        }

        /* Local shard has actual absolute expiration time */
        if (shard) {
            plat_closure_apply(rms_shard_meta_cb, &cb, status, shard_meta,
                               shard->lease.expires);
        /*
         * For a remote shard the lease cannot expire before the time
         * the remote end thought was remaining plus the local time
         * on the get (assuming a monotonic time base).
         */
        } else if (shard_meta) {
            if (shard_meta->persistent.lease_liveness) {
                expires = rms_tv_infinite;
            } else {
                expires.tv_sec = op->start_time.tv_sec +
                    (op->start_time.tv_usec +
                     shard_meta->persistent.lease_usecs) / MILLION;
                expires.tv_usec =
                    (op->start_time.tv_usec +
                     shard_meta->persistent.lease_usecs) % MILLION;
            }
            plat_closure_apply(rms_shard_meta_cb, &cb, status, shard_meta,
                               expires);
        } else {
            expires = op->start_time;
            plat_closure_apply(rms_shard_meta_cb, &cb, status, NULL, expires);
        }
    } else if (shard_meta_arg) {
        cr_shard_meta_free(shard_meta_arg);
    }

    if (shard) {
        rms_shard_ref_count_dec(shard);
    }

    if (op->shard_meta) {
        cr_shard_meta_free(op->shard_meta);
    }

    if (op->mbx) {
        sdf_fth_mbx_free(op->mbx);
    }

    plat_free(op);
}

/**
 * @brief Send response to given message.
 *
 * Called handles all book keeping on op (free, shard ref count decrement,
 * etc.
 */
static void
rms_op_send_response(struct rms_op *op, SDF_status_t status_arg) {
    SDF_status_t status;
    SDF_status_t tmp_status;
    void *data;
    size_t header_len;
    size_t total_len;
    struct sdf_msg *request_msg;
    struct SDF_protocol_msg *request_pm;
    SDF_Protocol_Msg_Info_t *request_pmi;
    struct sdf_msg *response_msg;
    struct SDF_protocol_msg *response_pm;
    SDF_Protocol_Msg_Info_t *response_pmi;
    struct sdf_msg_wrapper *response_wrapper;

    status = status_arg;

    header_len = sizeof (*response_msg) + sizeof (*response_pm);

    request_msg = NULL;
    sdf_msg_wrapper_rwref(&request_msg, op->request_wrapper);
    request_pm = (struct SDF_protocol_msg *)request_msg->msg_payload;

    plat_assert(op->rms);
    plat_assert(op->request_wrapper);

    response_msg = NULL;
    if (op->shard && op->shard->shard_meta) {
        tmp_status = cr_shard_meta_marshal(&data, &total_len,
                                           header_len,
                                           op->shard->shard_meta);
        if (tmp_status == SDF_SUCCESS) {
            response_msg = (struct sdf_msg *)data;
        } else if (status == SDF_SUCCESS) {
            status = tmp_status;
        }
    }

    if (!response_msg) {
        total_len = header_len;
        response_msg = plat_malloc(header_len);
        plat_assert_always(response_msg);
    }

    memset(response_msg, 0, header_len);
    response_msg->msg_flags = 0;
    response_msg->msg_len = total_len;

    response_pm = (struct SDF_protocol_msg *)response_msg->msg_payload;
    response_pm->shard = op->sguid;
    response_pm->data_size = total_len - header_len;
    response_pm->msgtype = MMRSM;
    response_pm->node_from = op->rms->config.my_node;
    response_pm->node_to = request_pm->node_from;
    response_pm->status = status;

    /*
     * XXX: drew 2009-05-17 need to validate msgyype is in range.  Add
     * a _to_string function and use that.  Especially since we're
     * generating responses to whatever random garbage we get.
     */
    request_pmi = &(SDF_Protocol_Msg_Info[request_pm->msgtype]);
    response_pmi = &(SDF_Protocol_Msg_Info[response_pm->msgtype]);

    plat_log_msg(21455, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "rms %p node %u shard 0x%lx respond to node %u"
                 " Msg: %s (%s) response %s (%s) status %s", op->rms,
                 op->rms->config.my_node, op->sguid, response_pm->node_to,
                 request_pmi->name, request_pmi->shortname,
                 response_pmi->name, response_pmi->shortname,
                 sdf_status_to_string(status));

    response_wrapper =
        sdf_msg_wrapper_local_alloc(response_msg,
                                    sdf_msg_wrapper_free_local_null,
                                    SMW_MUTABLE_FIRST, SMW_TYPE_RESPONSE,
                                    op->request_wrapper->dest_vnode /* src */,
                                    op->request_wrapper->dest_service /* src */,
                                    op->request_wrapper->src_vnode /* dest */,
                                    op->request_wrapper->src_service /* dest */,
                                    META_RESPOND,
                                    sdf_msg_wrapper_get_response_mbx(op->request_wrapper));
    sdf_msg_wrapper_rwrelease(&request_msg, op->request_wrapper);

    plat_closure_apply(sdf_replicator_send_msg_cb,
                       &op->rms->component_api.send_msg, response_wrapper,
                       NULL, NULL /* status */);
}

/** @brief Process response to rpc request sent from this node */
static void
rms_op_recv_response(struct plat_closure_scheduler *context, void *env,
                     struct sdf_msg_wrapper *response) {
    struct rms_op *op = (struct rms_op *)env;
    struct replicator_meta_storage *rms;
    struct sdf_msg *msg;
    SDF_protocol_msg_t *pm;
    SDF_Protocol_Msg_Info_t *pmi;
    struct sdf_msg_error_payload *error_payload;
    SDF_status_t status;
    SDF_status_t tmp_status;
    struct cr_shard_meta *shard_meta = NULL;

    rms = op->rms;
    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, response);

    if (msg->msg_type == SDF_MSG_ERROR) {
        error_payload = (struct sdf_msg_error_payload *)msg->msg_payload;
        status = error_payload->error;
        plat_log_msg(21456, LOG_CAT_REMOTE,
                     (status != SDF_SUCCESS && status != SDF_SHUTDOWN &&
                      status != SDF_TEST_CRASH) ?  PLAT_LOG_LEVEL_WARN :
                     PLAT_LOG_LEVEL_DEBUG,
                     "rms %p node %u shard 0x%lx op %p meta response from"
                     " node %u SDF_MSG_ERROR status %s",
                     rms, rms->config.my_node, op->sguid,
                     op, msg->msg_src_vnode, sdf_status_to_string(status));
    } else {
        pm = (SDF_protocol_msg_t *)msg->msg_payload;
        pmi = &(SDF_Protocol_Msg_Info[pm->msgtype]);

        status = pm->status;

        if (pm->data_size) {
            tmp_status = cr_shard_meta_unmarshal(&shard_meta, (void *)(pm + 1),
                                                 pm->data_size);
            if (status == SDF_SUCCESS) {
                status = tmp_status;
            }
        }

        if (shard_meta) {
            plat_log_msg(21457, LOG_CAT_REMOTE,
                         status != SDF_SUCCESS ?
                         PLAT_LOG_LEVEL_WARN : PLAT_LOG_LEVEL_DEBUG,
                         "rms %p node %u shard 0x%lx op %p meta response from"
                         " node %u Msg: %s (%s) status %s current home node %d"
                         " meta seqno %lld ltime %lld",
                         rms, rms->config.my_node, op->sguid,
                         op, msg->msg_src_vnode, pmi->name, pmi->shortname,
                         sdf_status_to_string(status),
                         shard_meta->persistent.current_home_node,
                         (long long)shard_meta->persistent.shard_meta_seqno,
                         (long long)shard_meta->persistent.ltime);
        } else {
            plat_log_msg(21458, LOG_CAT_REMOTE,
                         status != SDF_SUCCESS ?  PLAT_LOG_LEVEL_WARN :
                         PLAT_LOG_LEVEL_DEBUG,
                         "rms %p node %u shard 0x%lx op %p meta response from"
                         " node %u Msg: %s (%s) status %s",
                         rms, rms->config.my_node, op->sguid,
                         op, msg->msg_src_vnode, pmi->name, pmi->shortname,
                         sdf_status_to_string(status));
        }
    }

    sdf_msg_wrapper_ref_count_dec(response);

    plat_assert(!TAILQ_EMPTY(&rms->remote_ops));
    TAILQ_REMOVE(&rms->remote_ops, op, remote_ops_entry);

    rms_op_end(op, status, shard_meta);

    rms_ref_count_dec(rms);
}

static const char *
rms_op_type_to_string(enum rms_op_type op_type) {
    switch (op_type) {
#define item(caps, lower, msg, meta) case caps: return (#lower);
    RMS_OP_TYPE_ITEMS()
#undef item
    case RMS_OP_INVALID: return ("invalid");
    }

    plat_assert(0);
    return ("invalid");
}

static int
rms_op_type_is_put(enum rms_op_type op_type) {
    return (op_type == RMS_OP_CREATE || op_type == RMS_OP_PUT ||
            op_type == RMS_OP_REPUT);
}

static int
rms_replication_type_flags(SDF_replication_t replication_type) {
    switch (replication_type) {
#define item(caps, flags) case caps: return (flags);
    RMS_REPLICATION_TYPE_ITEMS()
#undef item
    case SDF_REPLICATION_INVALID:
        plat_fatal("bad replication_type");
    }
    plat_fatal("bad replication_type");
    return (0);
}

/** @brief Assert when shard state is obviously incorrect */
static void
rms_replication_type_validate(SDF_replication_t replication_type) {
    int flags;

    flags = rms_replication_type_flags(replication_type);
    plat_assert_either(flags & RRTF_INDEX_NONE,
                       flags & RRTF_INDEX_SHARDID,
                       flags & RRTF_INDEX_VIP_GROUP_ID);

    plat_assert_either(flags & RRTF_META_NONE,
                       flags & RRTF_META_SUPER,
                       flags & RRTF_META_PAXOS,
                       flags & RRTF_META_DISTRIBUTED);
}

/** @brief Assert when shard states are obviously incorrect */
static void
rms_replication_types_validate() {
#define item(caps, flags) rms_replication_type_validate(caps);
    RMS_REPLICATION_TYPE_ITEMS()
#undef item
}
