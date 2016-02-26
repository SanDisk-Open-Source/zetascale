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

#ifndef REPLICATION_REPLICATOR_H
#define REPLICATION_REPLICATOR_H 1

/*
 * File:   sdf/protocol/replication/replicator.h
 *
 * Author: drew
 *
 * Created on April 18, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: replicator.h 1480 2008-06-05 09:23:13Z drew $
 */

/**
 * The sdf replicator interface abstracts a message-driven replication
 * scheme where the other end points are
 *
 * 1.  The client, receiving responses on the replication_response_service
 *
 * 2.  The persistence system, receiving requests on the persistence service
 *     and sending responses to the persistence_response service.
 *
 * 3.  The replication service peers.
 *
 * In a production environment, "client" implies the home node code although
 * direct access is allowed to other programs for diagnostic and white box
 * test purposes.
 */

#include <stdint.h>

#include "platform/closure.h"
#include "platform/defs.h"
#include "platform/timer_dispatcher.h"

#include "common/sdftypes.h"
#include "protocol/replication/sdf_vips.h"

#include "sdfmsg/sdf_msg_action.h"
#include "sdfmsg/sdf_msg_wrapper.h"

/* XXX: Only for cmc.h which defines CMC_NO_COPY */
#ifndef notyet
#include "shared/cmc.h"
#endif

struct cr_shard_meta;
struct SDF_container_meta;
struct sdf_replication_op_meta;
struct timeval;

struct sdf_replicator_config {
    /**
     * @brief Some place to stick an instance
     *
     * For congruence with flash, coherence code. Unused in the test
     * environment and by the replicator itself.
     */
    struct sdf_replicator_adapter *adapter;

    /**
     * Number of threads to start
     *
     * As of 2008-06-09 this meant the number of threads which are allocated
     * in the #sdf_replicator_adapter in order to convert synchronous internal
     * APIs to asynchronous.
     */
    int nthreads;

    /**
     * @brief My address.
     *
     * FIXME: A messaging API built arround pnodes makes more sense since
     * the number of buffers to maintain aggregate throughput is independant
     * of how many vnodes are mapped onto the cluster.
     */
    vnode_t my_node;

    /**
     * @brief Cluster size.
     *
     * FIXME: This is here because
     * 1) the current sdf_msg API requires explicit construction of the end
     *    points.
     *
     * 2) currently the replication code is calculating placement
     *    based on this.
     */
    size_t node_count;

    /*
     * XXX: drew 2009-01-23 The services need to go away since they're
     * always set to the same service.
     */

    /** @brief In bound replication requests */
    service_t replication_service;

    /**
     * @brief Requests from other replication services
     *
     * Currently used by the meta_storage subsystem of replication
     */

    service_t replication_peer_service;

    /** @brief Requests for persistence */
    service_t flash_service;

    /**
     * @brief Responses for all services except persistence_service
     *
     * FIXME: This is only here because of how things currently work in
     * sdf msg, which requires a sending queue between a given service
     * and port to work.
     *
     * If we're ever actually connection oriented we should probably
     * be starting at an ephemeral port.
     */
    service_t response_service;

    /**
     * @brief Lease on home node
     *
     * This trades off switch-over time (and therefore unavailability
     * while a failure exists) for background flash traffic executing
     * Paxos NOPs.
     *
     * If it becomes impossible to issue a request this frequently
     * the system stalls.
     */
    uint64_t lease_usecs;

    /**
     * @brief Leases remain until liveness changes for test
     *
     * When debugging recovery state problems lease timeouts are inconvienient
     * because they change the system state. When sdf_replicator_config,
     * lease_usecs is set to this value leases will never time out and
     * all attempts to acquire a lease will succeed as long as the old
     * home node is dead according to the liveness subsystem and the new
     * meta-data is causally related to the previous version with a correct
     * ltime.
     */
    unsigned lease_liveness : 1;

    /**
     * @brief Switch-back timeout
     *
     * The replication code will hold-off on regaining ownership of vips
     * it gave back until it sees a node dead event or the other node 
     * assume ownership.
     *
     * This timeout should be unecessary (we will never fail to generate
     * a node dead event) although as of 2009-12-16 drew feels anxiety 
     * about the coming release and decided to include this as a fail
     * safe.
     */
    int64_t switch_back_timeout_usecs;

    /**
     * @brief Attempt to land VIPs on preferred node at startup
     *
     * XXX: drew 2009-11-04 In the situation
     *
     * node 1 start, gets preferred vip 10.0.0.1
     * client writes to node 1
     * node 2 start, gets preferred vip 10.0.0.2
     *
     * sdf/action/simple_replication.c would never copy the client writes
     * to node 2.
     *
     * VIPs are handled with a preference list
     *
     * vip 10.0.0.1 prefered nodes { 1, 2 }
     * vip 10.0.0.2 preferred nodes { 2, 1 }
     *
     * So that a simultaneous restart puts VIPs on their preferred nodes
     * and to minimize chances of two nodes grabbing the same VIP and
     * colliding on retry, the acquisition delay is a constant
     * (to receive updates on what nodes currently host what VIPs which
     * were triggered by the liveness system) plus an offset according
     * to the nodes instance.
     *
     * For instance, node 1 may wait the fixed 1 second before acquiring
     * 10.0.0.1 and 2 seconds before getting 10.0.0.2 while node 2
     * does the opposite.
     *
     * The simplest work around for the simple_replication.c bug
     * is to make the initial back-off entirely dependent on
     * node number, which leads to the situation
     *
     * node 1 start, gets both vips 10.0.0.1 and 10.0.0.2
     * client writes to node 1
     * node 2 start, gets no vips
     * recovery
     * noed 1 drops 10.0.0.2
     * node 2 gets 10.0.0.2
     *
     * This is now the default behavior.
     *
     * #replication_2_way_initial_preference causes the historic
     * behavior.
     */
    unsigned initial_preference : 1;

    /**
     * @brief Number of outstanding writes for for default replication
     *
     * Putting a cap on the number of outstanding commands which can be
     * executed bounds the search space in which replicas can disagree
     * following a crash.
     *
     * XXX: drew 2008-11-14 This can be changed so that we adjust it at
     * run-time, but the old window needs to close before it can update
     * so we'll keep.
     */
    int outstanding_window;

    /**
     * @brief Number of recovery operations to run in parallel
     *
     * Initially we just keep a fixed number in flight.  This should
     * be an empirically determined numbr which provides a reasonable
     * trade-off between MTTR and performance impact.
     *
     * Long term we need to schedule more intelligently perhaps
     * via a leaky bucket mechanism.
     */
    int recovery_ops;

    /**
     * @brief Timeout on each operation
     *
     * The replication code requires a guarantee from the messaging
     * layer that a (potentially synthetic) response will be received
     * for a each outbound request.
     *
     * The two mechanisms by which this can be implemented are via
     * liveness integration with the messaging layer (the node being
     * marked dead causes all new requests to terminate with an error
     * and all current requests to fail with an error) and via timeouts.
     *
     * The liveness option is preferable because it can leverage
     * structures which are already in place for no additional cost
     * during normal operation; while a perfect timeout mechanism
     * is O(n log n) and an imperfect one requires periodic O(n)
     * scans with n the number of simultaneous in-flight messages.
     *
     * XXX 2009-01-19 Since the test infrastructure has per-message timeouts
     * and Tom Riddle has timeout logic implemented we use that for initial
     * development purposes; and in environments where that option is used set
     * the replicator to have timeouts that are on the order of the lease
     * length and therefore the tolerable peak delay.
     *
     * As a foot note, this has to be higher than the maximum outstanding
     * I/O window times the per-request latency.  Mis-configuration will
     * lead to undesirable effects.
     */
    int64_t timeout_usecs;

    /** @brief Always get meta by message for testing purposes */
    int meta_by_message;

    /**
     * @brief How frequently to look at the timed event priority queue
     *
     * The plat_timer_dispatcher and fth/memcached idle code are not fully
     * integrated.  The simplest work around is an idle thread that
     * periodically polls and_ executes the newest timer events.
     *
     * This is that interval.
     */
    int64_t timer_poll_usecs;

    /**
     * @brief SDF VIP group, group group config
     * This Data structure is built at the startup by simple_replication_init
     */
    sdf_vip_config_t *vip_config;

};

#define SDF_REPLICATOR_SHARD_STAT_COUNTER_ITEMS() \
    /** @brief Number of times this node became the home node */               \
    item(master_recovery_started)                                              \
    /** @brief Number of times we made it all the way to completion */         \
    item(master_recovery_complete)                                             \
    /** @brief Number of times stale slave replica started */                  \
    item(stale_slave_recovery_started)                                         \
    /** @brief Number of times replica recovery started */                     \
    item(replica_recovery_started)                                             \
    /** @brief Number of times replica recovery completed */                   \
    item(replica_recovery_complete)                                            \
    /** @brief Number of get iteration cursors calls started */                \
    item(recovery_get_cursors_started)                                         \
    /** @brief Number of get iteration cursors calls completed */              \
    item(recovery_get_cursors_complete)                                        \
    /** @brief Number of get iteration cursors calls failed */                 \
    item(recovery_get_cursors_failed)                                          \
    /** @brief Number of get by cursor calls started */                        \
    item(recovery_get_by_cursor_started)                                       \
    /** @brief Number of get by cursor calls completed */                      \
    item(recovery_get_by_cursor_complete)                                      \
    /** @brief Number of get by cursor calls failed */                         \
    item(recovery_get_by_cursor_failed)                                        \
    /** @brief Number of set operations started during recovery */             \
    item(recovery_set_started)                                                 \
    /** @brief Number of set operations completed during recovery */           \
    item(recovery_set_complete)                                                \
    /** @brief Number of set operations failed during recovery */              \
    item(recovery_set_failed)                                                  \
    /** @brief Number of delete operations started during recovery */          \
    item(recovery_delete_started)                                              \
    /** @brief Number of delete operations complete during recovery */         \
    item(recovery_delete_complete)                                             \
    /** @brief Number of delete operations complete during recovery */         \
    item(recovery_delete_failed)

#define SDF_REPLICATOR_SHARD_STAT_PULL_ITEMS() \
    /** @brief whether local node is writable */                               \
    item(writeable)                                                            \
    /** @brief whether local node is writable */                               \
    item(readable)                                                             \
    /** @brief Number of replicas that are authoritative */                    \
    item(num_authoritative_replicas)                                           \
    /** @brief Number of replicas active for writing */                        \
    item(num_writeable_replicas)

#define SDF_REPLICATOR_STAT_ITEMS() \
    SDF_REPLICATOR_SHARD_STAT_COUNTER_ITEMS() \
    SDF_REPLICATOR_SHARD_STAT_PULL_ITEMS()

struct sdf_replicator_shard_stats {
#define item(lower) int64_t lower;
    SDF_REPLICATOR_STAT_ITEMS()
#undef item
};

/**
 * @brief Local access to given replicated shard
 * - The current home node has read-write access
 * - Asynchronous replicas have read-only access
 * - Authoritative synchronous replicas with an existing lease on the
 *   home node will have read-only access.
 *
 * Non-authoritative replicas have no access
 */
#define SDF_REPLICATOR_ACCESS_ITEMS() \
    /** @brief No access is allowed to the given shard */                      \
    item(SDF_REPLICATOR_ACCESS_NONE, none)                                     \
    /** @brief The given shard is read-only */                                 \
    item(SDF_REPLICATOR_ACCESS_RO, ro)                                         \
    /** @brief The shard has read-write access */                              \
    item(SDF_REPLICATOR_ACCESS_RW, rw)

enum sdf_replicator_access {
#define item(caps, lower) caps,
    SDF_REPLICATOR_ACCESS_ITEMS()
#undef item
};

/**
 * @brief Set of edge triggered events of interest to replicator clients
 *
 * Certain state transitions require handling at the high level
 *
 * item(caps, lower, value)
 */
#define SDF_REPLICATOR_EVENT_ITEMS() \
    /** @brief First call for this shard this run */                           \
    item(SDF_REPLICATOR_EVENT_FIRST, first, 1 << 0)                            \
    /**                                                                        \
     * @brief ltime advanced                                                   \
     *                                                                         \
     * The shard ltime advances on home node changes.  Where this node has     \
     * read only #SDF_REPLICATOR_ACCESS_RO access the new home node may have   \
     * seen a different set of IOs and roll-back on this node.                 \
     *                                                                         \
     * Where this node was the home node, got partitioned off from the         \
     * meta-data service, and re-established the connection the cache must     \
     * be flushed because it may maintain stale data.                          \
     *                                                                         \
     * This is always necessary for write-through caches where the contents    \
     * may not have made it to persistent storage where it will be seen        \
     * by the roll-back code.                                                  \
     *                                                                         \
     * Finally, where acceess is read-write #SDF_REPLICATOR_ACCESS_RW          \
     * active TCP connections must be closed because they may have queued      \
     * writes that are no longer causally related to the current data.         \
     */                                                                        \
    item(SDF_REPLICATOR_EVENT_LTIME, ltime, 1 << 1)                            \
    /** @brief Access changed */                                               \
    item(SDF_REPLICATOR_EVENT_ACCESS, access, 1 << 2)                          \
    /** @brief Lease was extended */                                           \
    item(SDF_REPLICATOR_EVENT_LEASE, lease, 1 << 3)                            \
    /** @brief Miscellaneous state change */                                   \
    item(SDF_REPLICATOR_EVENT_OTHER, other, 1 << 4)

enum sdf_replicator_event {
#define item(caps, lower, value) caps = (value),
    SDF_REPLICATOR_EVENT_ITEMS()
#undef item
};

/* User closures */

/**
 * @brief Closure applied by the user after their notification
 * handling has completed.
 */
PLAT_CLOSURE(sdf_replicator_notification_complete_cb)

 /**
  * @brief Closure applied by replication code on state changes
  *
  * @param events <IN> What has changed since the last call as a bit-fielded
  * or of #sdf_replicator_event
  *
  * @param shard_meta <IN> Deep shard meta-data structure.  The
  * memcached container name and other relevant attributes can
  * be extracted from this.  The code called by the closure assumes
  * ownership of hard meta and shall free it with a call to
  * #cr_shard_meta_free.
  *
  * @param access <IN> The new access mode.
  *
  * @param expires <IN> The access level is guaranteed not to change before
  * the expiration time.
  *
  * @param complete <IN> Shall be applied by the user when the transition
  * completes.  To avoid issues with stale data, on a transition to write
  * the replication code will refuse writes until the handshake completes.
  */
PLAT_CLOSURE5(sdf_replicator_notification_cb,
              int, events,
              struct cr_shard_meta *, shard_meta,
              enum sdf_replicator_access, access,
              struct timeval, expires,
              sdf_replicator_notification_complete_cb_t, complete);

/* Closures used by various replicators provided by adapter */

/**
 * @brief Register receiver callback.
 *
 * Binding a non-null closure to an existing service shall
 * fail.  Binding to sdf_msg_recv_null will cancel an existing
 * mapping.
 *
 * @param service <IN> service
 * @param recv_closure <IN>  closure, sdf_msg_recv_wrapper_null to
 * undo an existing mapping.
 * @param status <OUT> SDF_SUCCESS is stored here on success, something
 * else on error.
 */

PLAT_CLOSURE3(sdf_replicator_register_recv_cb,
              service_t, service,
              sdf_msg_recv_wrapper_t, recv_closure,
              SDF_status_t *, status)

/**
 * @brief Send callback
 *
 * @param sdf_msg_wrapper <IN> message being sent.   One reference count on the
 * #sdf_msgf_wrapper is consumed.
 * @param ar_mbx <IN> When non-NULL, responses to the message will
 * be delivered to the fthMbox, closure, or other abstraction it
 * wraps.  The contract requires that the sdf_fth_mbx exist until a
 * response (real or synthetic for an error condition such as a timeout
 * or cancelled request) is received)
 * @param status <OUT> SDF_SUCCESS is stored here on success, something
 * else on error.
 */
PLAT_CLOSURE3(sdf_replicator_send_msg_cb,
              struct sdf_msg_wrapper *, sdf_msg_wrapper,
              struct sdf_fth_mbx *, ar_mbx,
              SDF_status_t *, status);


/**
 * @brief API gluing replicator into the rest of the system or
 * test environment.
 */
struct sdf_replicator_api {
    /**
     * @brief Register a service
     *
     * This API will replace the single receive closure.
     *
     * This encapsulates the services used (Paxos meta-data,
     * super-node meta-data, etc.) so we can grow without  wading
     * through the test environment.
     */
    sdf_replicator_register_recv_cb_t register_recv;

    /**
     * @brief Send a message
     *
     * Replicators use this interface to send responses to their clients,
     * make flash requests (potentially locally and remotely), and communicate
     * with their peers.
     *
     * To get concurrency this must be a non-blocking implementation which
     * may just queue the work for a thread pool.
     */
    sdf_replicator_send_msg_cb_t send_msg;

    /**
     * @brief Single-threaded scheduler for subsystem use
     *
     * XXX: drew 2009-01-26 Until performance measurements show otherwise,
     * we'll guarantee that re-entrant calls won't be made into replication
     * subsystems provided that its message and timer events are being
     * scheduled in the single scheduler.
     */
    struct plat_closure_scheduler *single_scheduler;

    /**
     * @brief Trigger timeouts.
     *
     * The simulated environment shall instantiate a timer dispatcher
     * which it calls as an idle task.  Using timer events to schedule
     * message delivery makes it simple to have the simulated time base
     * advance to the next scheduled event.
     */
    struct plat_timer_dispatcher *timer_dispatcher;

    /**
     * @brief Return time elapsed since the epoch
     *
     * In the simulated environment time only advances when the replicator
     * instances for each simulated node are blocked on incoming message
     * traffic.
     *
     * gettime shall return synchronouosly.
     */
    plat_timer_dispatcher_gettime_t gettime;

    /**
     * @brief Pseudo-random number generator
     *
     * Statistical approaches are appropriate for a lot of things in
     * distributed computing, like gossip/epidemic propagation
     * protocols, back-off delays to avoid contention, cache invalidation, etc.
     *
     * XXX: drew 2009-01-20 We're currently abusing #sdf_replicator_api
     * for the interface between replication_test_framework and
     * replication_test_node; and replication_test_node and
     * replication_test_flash.  If that changes we may want to move
     * this out into those new APIs.
     *
     * We aren't setting prng in the replicator adapter until we
     * need it for something in the replicator itself and have decided to
     * keep this instead of splitting out the
     */
    struct plat_prng *prng;

    /*
     * XXX: drew 2009-06-10 Add crash function here which does the right thing
     * in real environment and replication unit test framework.
     */
};

/* Closures for consumer interaction with replicator */

/** @brief Shutdown callback */
PLAT_CLOSURE(sdf_replicator_shutdown_cb);

/** @brief Shutdown */
PLAT_CLOSURE1(sdf_replicator_shutdown,
              sdf_replicator_shutdown_cb_t, complete);

/**
 * @brief Asynchronous command execution  completion
 *
 * @param status <IN> status; SDF_SUCCESS on success, other
 * on failure
 * @param out <IN> cr lf delimited output
 */
PLAT_CLOSURE2(sdf_replicator_command_cb,
              SDF_status_t, status,
              char *, output);

/**
 * @brief Indicate when a given node is  live
 */
PLAT_CLOSURE2(sdf_replicator_node_live,
              vnode_t, node,
              struct timeval, last_live);

/**
 * @brief Indicate that the remote node should be treated as dead as soon
 * as practical.
 */
PLAT_CLOSURE1(sdf_replicator_node_dead, vnode_t, node);

/**
 * @brief Replicator consumer interface
 */
typedef struct sdf_replicator {
    /** @brief Start replicator.  Synchronously returns success or failure  */
    int (*start_fn)(struct sdf_replicator *self);

    /**
     * @brief Return meta-data for a client operation
     *
     * The replicator must embed meta-data in each action node
     * request in order to fence stale IOs languishing in queues and
     * efficiently recover.  Provide that meta-data.
     */
    SDF_status_t
    (*get_op_meta_fn)(struct sdf_replicator *self,
                      const struct SDF_container_meta *container_meta,
                      SDF_shardid_t shard,
                      struct sdf_replication_op_meta *op_meta);

    /** @brief Add a listener to replication state change */
    struct cr_notifier *
    (*add_notifier)(struct sdf_replicator *replicator,
                    sdf_replicator_notification_cb_t cb);

    /** @brief Remove a state change listener */
    void
    (*remove_notifier)(struct sdf_replicator *replicator,
                       struct cr_notifier *handle);


    /** @brief Get statistics */
    SDF_status_t
    (*get_container_stats)(struct sdf_replicator *replicator,
                           struct sdf_replicator_shard_stats *stats,
                           SDF_cguid_t cguid);

    /** @brief Pass a generic command through to the replicator */
    void
    (*command_async)(struct sdf_replicator *replicator, SDF_shardid_t shard,
                     const char *command,
                     sdf_replicator_command_cb_t cb);

    /**
     * @brief Message received from any source.
     *
     * Since msg has a reference count of one added for the replicator,
     * the replicator shall invoke #sdf_msg_wrapper_ref_count_dec when
     * it is through with the wrapper.
     */
    sdf_msg_recv_wrapper_t recv_closure;

    /** @brief Shutdown asynchronously after all pending closures have fired */
    sdf_replicator_shutdown_t shutdown_closure;

    /** @brief Indicate node was alive */
    sdf_replicator_node_live_t node_live_closure;

    /** @brief Indicate that the node is dead */
    sdf_replicator_node_dead_t node_dead_closure;

} sdf_replicator_t;

__BEGIN_DECLS

/**
 * @brief Provide default initialization
 *
 * @param config <OUT> buffer for configuration owned by caller
 * @param addr <IN> local node address
 * @param node_count <IN> number of nodes in cluster
 */
void sdf_replicator_config_init(struct sdf_replicator_config *config,
                                vnode_t addr, size_t node_count);

/**
 * @brief Start replicator
 *
 * @return 0 on success, non-zero on failure.
 */
static __inline__ int
sdf_replicator_start(struct sdf_replicator *replicator) {
    return ((*replicator->start_fn)(replicator));
}

/**
 * @brief Pass message to the scheduler.
 *
 * All in-bound messages are passed through the sdf_replicator_receive_msg
 * function.
 *
 * When msg_wrapper->mutability is #SMW_MUTABLE_FIRST the replicator
 * can modify the message prior to fan-out.
 */

static __inline__ void
sdf_replicator_receive_msg(struct sdf_replicator *replicator,
                           struct sdf_msg_wrapper *msg_wrapper) {
    plat_closure_apply(sdf_msg_recv_wrapper, &replicator->recv_closure,
                       msg_wrapper);
}

/**
 * @brief Shutdown and free the replicator
 *
 * @param replicator <IN> replicator to shutdown
 * @param shutdown_complete <IN> invoked when there is no activity left
 */
static __inline__ void
sdf_replicator_shutdown(struct sdf_replicator *replicator,
                        sdf_replicator_shutdown_cb_t shutdown_complete) {
    plat_closure_apply(sdf_replicator_shutdown,
                       &replicator->shutdown_closure,
                       shutdown_complete);
}

/**
 * @brief Get #sdf_replication_shard_meta for given container meta, shard tuple
 *
 * @param container_meta <IN> Container meta-data
 * @param shard <IN> Data shard id.
 * @param op_meta <OUT> Output
 * @return SDF_SUCCESS on success, otherwise on failure
 */
static __inline__ SDF_status_t
sdf_replicator_get_op_meta(struct sdf_replicator *replicator,
                           const struct SDF_container_meta *container_meta,
                           SDF_shardid_t shard,
                           struct sdf_replication_op_meta *op_meta) {
    return ((*replicator->get_op_meta_fn)(replicator, container_meta, shard,
                                          op_meta));
}

/**
 * @brief Indicate node is live
 *
 * @param pnode <IN> Node which was alive
 * @param last_live <IN> Time since the epoch when the node was last alive
 * @param expires <IN> Time node should be considered alive until
 */

static __inline__ void
sdf_replicator_node_live(struct sdf_replicator *replicator, vnode_t pnode,
                         struct timeval last_live) {
    plat_closure_apply(sdf_replicator_node_live, &replicator->node_live_closure,
                       pnode, last_live);
}

/**
 * @brief Indicate that a node should be treated as dead
 *
 * @param pnode <IN> Node which is dead.
 */
static __inline__ void
sdf_replicator_node_dead(struct sdf_replicator *replicator, vnode_t pnode) {
    plat_closure_apply(sdf_replicator_node_dead, &replicator->node_dead_closure,
                       pnode);
}

/**
 * @brief Add a listener to replication state change
 *
 * Notifications are delivered for all changes whether or not the
 * local node has read-write access, since this is useful for
 * administrative purposes like tracking recovery progress.
 *
 * Consumers should expect repeated notifications as leases are
 * renewed.  Where access is RW or RO this translates into a keep-alive
 * for local VIP groups.  The events cb closure argument indicates what
 * changes are new.  For example, SDF_REPLICATOR_EVENT_ACCESS will not
 * accompany a lease renewal.
 *
 * Notifiers which were added before * #sdf_replicator_start was called
 * will be applied as shard information becomes available.
 *
 * @param replicator <IN> Replicator
 * @param cb <IN> Closure applied when the state changes.
 *
 * See the PLAT_CLOSURE sdf_replicator_notification_cb definition for
 * details.
 *
 * Note that notifications block parts of the replication code until
 * plat_closure_apply(cb) returns.  The implications of this are that
 * when cb is bound to PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS,
 * PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS, or PLAT_CLOSURE_SCHEDULER_ANY
 * it cannot do anything significant.
 *
 * Also note that since cb is dispatched from an fthread, blocking
 * system calls or library functions like system(3) will stop a fthread
 * scheduler; which in the MULTIQ environment will stop 1/n schedulers
 * of the fthreads due to strong fthread affinity.
 *
 * @return A handle which can be passed to #sdf_replicator_remove_notifier
 * on shutdown.
 */
static __inline__ struct cr_notifier *
sdf_replicator_add_notifier(struct sdf_replicator *replicator,
                            sdf_replicator_notification_cb_t cb) {
    return ((*replicator->add_notifier)(replicator,  cb));
}

/**
 * @brief Remove a notification
 *
 * @param replicator <IN> Replicator
 * @param handle <IN> Handle to release from #sdf_replicator_add_notifier
 */
static __inline__ void
sdf_replicator_remove_notifier(struct sdf_replicator *replicator,
                               struct cr_notifier *handle) {
    (*replicator->remove_notifier)(replicator, handle);
}

/**
 * @brief Get statistics
 * @param replicator <IN> Replicator
 * @param stats <OUT> Stats stored here
 * @return SDF_SUCCESS on success, otherwise on failure
 */
static __inline__ SDF_status_t
sdf_replicator_get_container_stats(struct sdf_replicator *replicator,
                                   struct sdf_replicator_shard_stats *stats,
                                   SDF_cguid_t cguid) {
    return ((*replicator->get_container_stats)(replicator, stats, cguid));
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
 * @param command <IN> command being executed; caller can free after
 * return.
 * @param cb <IN> closure applied on completion
 */
static __inline__ void
sdf_replicator_command_async(struct sdf_replicator *replicator,
                             SDF_shardid_t shard, const char *command,
                             sdf_replicator_command_cb_t cb) {
    (*replicator->command_async)(replicator, shard, command, cb);
}

/**
 * @brief Process a command synchronously from an fthThread
 *
 * Commands currently supported are
 * RECOVERED: for simple replication to indicate that the remote replica
 * has recovered.
 *
 * XXX: drew 2009-07-30 Add "standard" commands once we decide on syntax
 *
 * @param replicator <IN> replicator
 * @param shard <IN> shard being operated on
 * @param command <IN> command being executed; caller can free after
 * return.
 * @param output <OUT> cr lf delimited output
 * @return SDF_SUCCESS on success, other on failure
 */
SDF_status_t sdf_replicator_command_sync(struct sdf_replicator *replicator,
                                         SDF_shardid_t shard,
                                         const char *command, char **output);


/** @brief Convert #sdf_replicator_access to string */
const char *sdf_replicator_access_to_string(enum sdf_replicator_access access);

/**
 * @brief Convert flags of #sdf_replicator_events to string
 *
 * @param events <IN> Bit-fielded or of  #sdf_replicator_events
 * @return pthread local static buffer of space delimited events
 */
char *sdf_replicator_events_to_string(int events);

/**
 * @brief Copy replicator_config
 *
 * Free with #sdf_replicator_config_free
 */
struct sdf_replicator_config *sdf_replicator_config_copy(const struct sdf_replicator_config *config);

/**
 * @brief Copy replicator_config
 *
 * Free with #sdf_replicator_config_free
 */
struct sdf_replicator_config *sdf_replicator_config_copy(const struct sdf_replicator_config *config);

/** @brief Free config allocated by #sdf_replicator_config_copy */
void sdf_replicator_config_free(struct sdf_replicator_config *config);

__END_DECLS

#endif /* ndef REPLICATION_REPLICATOR_H */
