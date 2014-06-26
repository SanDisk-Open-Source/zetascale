#ifndef REPLICATION_TEST_FRAMEWORK_H
#define REPLICATION_TEST_FRAMEWORK_H 1

/*
 * File:   $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/protocol/replication/tests/test_framework.h $
 *
 * Author: drew
 *
 * Created on October 31, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: test_framework.h 12947 2010-04-16 02:05:47Z drew $
 */

/**
 * Test framework for replication tests.
 *
 * The test framework provides the infrastructure to run a simulated cluster
 * with a synthetic time base, with hooks for tests to make synchronous calls
 * from fthreads (for simple test cases like canned sequence of operations
 * executed sequentially, or N worker threads which collectively maintain a
 * set of * N requests in parallel) or asynchronous calls modeled as a set
 * of closures (for more complex  call graphs, like starting 3 writes in
 * parallel and issuing a read when all three complete) options.  Test
 * operations are directed to a specific node which may reject them (for
 * example with status SDF_WRONG_NODE) when the current replication server
 * does not match.
 */


#define MILLION 1000000

#include "common/sdftypes.h"
#include "platform/timer_dispatcher.h"
#include "platform/closure.h"
#include "platform/defs.h"
#include "platform/prng.h"
#include "shared/shard_meta.h"

#include "test_model.h"
#include "test_config.h"
#include "test_meta.h"

#define RELATIVE_TIME

struct cr_shard_meta;
struct plat_timer_dipatcher;

struct sdf_shard_meta;

#define NETWORK_DELAY
#define CONTAINER_META_MAX 0x1000

// #define RTFW_PROT


#if 0
#ifndef DEBUG
#define DEBUG

#endif
#endif

typedef struct sdf_replicator_api replication_test_api_t;

/** @brief Replication test generic async op completion callback */
PLAT_CLOSURE1(replication_test_framework_cb, SDF_status_t, status);

/** @brief Replication test generic void closure */
PLAT_CLOSURE(rtfw_void_cb);

/** @brief Replication test framework shutdown callback */
PLAT_CLOSURE(replication_test_framework_shutdown_async_cb);

/** @brief Apply to free buffer. */
PLAT_CLOSURE2(replication_test_framework_read_data_free_cb,
              const void *, data, size_t, data_len);

/**
 * @brief Replication test read op completion callback
 *
 * data should be freed by the free callback argument.
 */
PLAT_CLOSURE4(replication_test_framework_read_async_cb,
              SDF_status_t, status, const void *, data, size_t, data_len,
              replication_test_framework_read_data_free_cb_t, free_cb);

/** @brief Callback for "get last sequence number". */
PLAT_CLOSURE2(rtfw_last_seqno_cb,
              uint64_t, seqno, SDF_status_t, status);

/**
 * @brief Replication test get cursors op completion callback
 *
 * data should be freed by the free callback argument.
 */
PLAT_CLOSURE4(rtfw_get_cursors_cb,
              SDF_status_t, status, const void *, data, size_t, data_len,
              replication_test_framework_read_data_free_cb_t, free_cb);

/**
 * @brief Replication test get-by-cursor op completion callback
 *
 * data should be freed by the free callback argument.
 */
PLAT_CLOSURE9(rtfw_get_by_cursor_cb,
              SDF_status_t, status, const void *, data, size_t, data_len,
              char *, key, int,  key_len, SDF_time_t, exptime,
              SDF_time_t, createtime, uint64_t, seqno,
              replication_test_framework_read_data_free_cb_t, free_cb);

/**
 * @brief Callback for meta-data updates.
 *
 * @param <IN> status SDF_SUCCESS on success; otherwise implies a failure.
 * The conflicting shard meta is returned in shard meta, NULL if none is
 * available.
 *
 * @param <IN> shard_meta Receiver owns the result and shall free with
 * #cr_shard_meta_free.  Where an operation was requested, if known the
 * current meta-data can be returned.
 *
 * @param lease_expires <IN> A lower bound on when the lease expires in
 * absolute local time according to the #sdf_replicator_api gettime
 * function.  Network propagation delays have been accounted for.
 *
 * @param <IN> node_id Node.  Allows test code to run without added levels of
 * indirection to pickup which node saw the changes.
 */
PLAT_CLOSURE4(rtfw_shard_meta_cb,
              SDF_status_t, status,
              struct cr_shard_meta *, shard_meta,
              struct timeval, lease_expires,
              vnode_t, node)

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
  *
  * @param node <IN> node from which this event originated
  */
PLAT_CLOSURE6(rtfw_replicator_notification_cb,
              int, events,
              struct cr_shard_meta *, shard_meta,
              enum sdf_replicator_access, access,
              struct timeval, expires,
              sdf_replicator_notification_complete_cb_t, complete,
              vnode_t, node);

struct replication_test_framework {
#ifdef DEBUG
    /** @brief Wrapper count */
    uint32_t n_wrapper;
#endif
    /** @brief Rtfw shutdown flag */
    int final_shutdown_phase;

    /** @brief Test configuration */
    struct replication_test_config config;

    /** @brief Replicator api */
    replication_test_api_t *api;

#ifdef notyet
    /** @brief Test itself */
    struct replication_test *test;
#endif

    /** @brief Associated mailbox */
    fthMbox_t mbox;

    /** @brief Current biggest shard_id */
    SDF_shardid_t max_shard_id;

    /** @brief A hanging off SDF_container_meta * list */
    struct SDF_container_meta *cmeta[CONTAINER_META_MAX];

    /** @brief scheduler fthread */
    fthThread_t *closure_scheduler_thread;

    /**
     * @brief Map of response mkeys to mailboxes.
     * map mbox address to a mbox, others null yet
     */
    struct SDFMSGMap *rtfw_msg_response_map;

    /** @brief Closure_scheduler */
    struct plat_closure_scheduler *closure_scheduler;

    /** @brief Test_framework lock, for append node */
    fthLock_t rtfw_lock;

    /** @brief Model sanity checking test */
    struct replication_test_model *model;

    /** @brief Nodes numbering config->nnodes */
    struct replication_test_node **nodes;

    /** @brief Synthetic time base */
    struct plat_timer_dispatcher *timer_dispatcher;

    /** @brief A ugly logic time */
    sdf_replication_ltime_t ltime;

    /** @brief Not shutdown nodes number */
    int active_nodes;

    /** @brief Common time base for event trigger */
    struct timeval now;

    /** @brief Original plat_log time function */
    void (*old_log_time_fn)(void *extra, struct timeval *tv);

    /** @brief Original plat_log time function extra arg */
    void *old_log_time_extra;

    /** @brief Timeout; 0 for none */
    int64_t timeout_usecs;
};
__BEGIN_DECLS

/* test API */

/**
 * @brief Create replication test framework
 *
 * Use #rtfw_shutdown_async or #rtfw_shutdown_sync to stop (if
 * started with #rtfw_start) and free.
 *
 * @param replicator_alloc <IN> Allocate the replicator callback
 * @param replicator_alloc_extra <IN> Extra parameter.
 */
struct replication_test_framework *
replication_test_framework_alloc(const struct replication_test_config  *config);

/**
 * @brief Start test framework
 *
 * XXX: drew 2009-08-21 Currently, all nodes are started by default which
 * is probably not what we want for some adversarial tests.  We should
 * force users to call rtfw_start_all_nodes which most of the tests seem
 * to be doing.
 */
void
rtfw_start(struct replication_test_framework *test_framework);

/**
 * @brief Shutdown and free asynchronously
 * @param cb <IN> closure invoked on completion
 */
void
rtfw_shutdown_async(struct replication_test_framework *test_framework,
                    replication_test_framework_shutdown_async_cb_t cb);

/**
 * @brief Synchronous shutdown and free
 *
 * This is a thin wrapper arround #rtfw_shutdown_async which uses
 * #fthMbox_t to allow synchronous use.
 */
void
rtfw_shutdown_sync(struct replication_test_framework *test_framework);

/* Generic test hooks */

/**
 * @brief Set timeout in usecs
 *
 * @return Old value.
 */
unsigned
rtfw_set_timeout_usecs(struct replication_test_framework *test_framework,
                       int64_t timeout_usecs);

/**
 * @brief Return whether the test has failed
 *
 * This function aggregates the model (#rtm_get_failed), test result,
 * and other failures.
 */
int
rtfw_get_failed(struct replication_test_framework *test_framework);

/**
 * @brief Set failure
 */
void
rtfw_set_failed(struct replication_test_framework *test_framework,
                int status);

/**
 * @brief Get time from simulated timebase.
 */
void
rtfw_get_time(struct replication_test_framework *test_framework,
              struct timeval *tv);

/**
 * @brief Apply given closure at scheduled simulated time
 *
 * @param when <IN> Time to block until
 * @param cb <IN> Closure to apply at given time
 */
void
rtfw_at_async(struct replication_test_framework *test_framework,
              const struct timeval *when, replication_test_framework_cb_t cb);

/**
 * @brief Block calling #fthThread until we reach when
 */
void
rtfw_block_until(struct replication_test_framework *test_framework,
                 const struct timeval when);

/**
 * @brief Sleep for given number of simulated microseconds.
 * @param usec <IN> Relative time from now
 */
void
rtfw_sleep_usec(struct replication_test_framework *test_framework,
                unsigned usec);

/* Node operations */

/**
 * @brief Crash node asynchronously
 *
 * Node crashes are simulated by discarding everything in the inbound
 * and outbound message queues, returning timeout errors for all existing
 * message requests from the node and new requests, and shutting the node
 * down.
 *
 * @param test_framework <IN> A running test framework.
 * @param node <IN> Vnode to crash
 * @param cb <IN> Callback applied when the simulated crash is complete
 * and the node can be restarted.  The callback may return other than
 * SDF_SUCCESS when the node is in the process of crashing.
 */
void
rtfw_crash_node_async(struct replication_test_framework *test_framework,
                      vnode_t node, replication_test_framework_cb_t cb);


/**
 * @brief Crash node synchronously from fthThread.
 *
 * This is a thin wrapper around #rtfw_crash_node_async which uses a
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_framework <IN> A running test framework.
 * @param node <IN> Vnode to crash
 * @return SDF_SUCCESS on success, otherwise on failure such as when the
 * node was already in the process of crashing.
 */
SDF_status_t
rtfw_crash_node_sync(struct replication_test_framework *test_framework,
                     vnode_t node);

/**
 * @brief Start node
 *
 * Start the given simulated node. By default, all of the nodes in the
 * simulated cluster are in the unstarted state.
 *
 * XXX: drew 2010-04-06 Currently, the node starts asynchronously and
 * is not "up" when the function returns.  #rtfw_start_node should split
 * into #rtfw_start_node_async and #rtfw_start_node_sync so that the
 * caller can correctly block.
 */
SDF_status_t
rtfw_start_node(struct replication_test_framework *test_framework,
                vnode_t node);

/**
 * @brief Start all nodes
 */
SDF_status_t
rtfw_start_all_nodes(struct replication_test_framework *test_framework);

#ifdef notyet
/** @brief Start network of given node asynchronously */
void
rtfw_start_node_network_async(struct replication_test_framework *test_framework,
                              vnode_t node, rtfw_void_cb_t cb);
#endif

/** @brief Start network of given node synchronously */
SDF_status_t
rtfw_start_node_network_sync(struct replication_test_framework *test_framework,
                             vnode_t node);

#ifdef notyet
/** @brief Shutdown network of given node asynchronously */
SDF_status_t
rtfw_shutdown_node_network_async(struct replication_test_framework *test_framework,
                                 vnode_t node, rtfw_void_cb_t cb);
#endif

/** @brief Shutdown network of given node synchronously */
SDF_status_t
rtfw_shutdown_node_network_sync(struct replication_test_framework *test_framework,
                                vnode_t node);


/* Functions for RT_TYPE_REPLICATOR */

/**
 * @brief Set closure applied on notification callback events
 *
 * @param test_framework <IN> An initialized test framework with test type
 * RT_TEST_TYPE_REPLICATOR
 *
 * @param cb <IN> attached via the #sdf_replicator_add_notifier.  Note
 * that the user must hand-shake the callback before receiving an additional
 * notification and that shutdown/crashes block on pending notifications.
 * This replaces all previously specified closures.
 * rtfw_replicator_notification_cb_null may be specified to
 * route messages into the aether.
 */
void
rtfw_set_replicator_notification_cb(struct replication_test_framework *framework,
                                    rtfw_replicator_notification_cb_t cb);

/**
 * @brief Process a command asynchronously
 *
 * @param test_framework <IN> An initialized test framework with test type
 * RT_TEST_TYPE_REPLICATOR
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard <IN> shard being operated on
 * @param command <IN> command being executed; caller can free after
 * return.
 * @param cb <IN> closure applied on completion
 */
void
rtfw_command_async(struct replication_test_framework *framework, vnode_t node,
                   SDF_shardid_t shard, const char *command_arg,
                   sdf_replicator_command_cb_t cb);

/**
 * @brief Synchronous shard create operation from fthThread
 *
 * This is a thin wrapper arround #rtfw_create_shard_async which uses a
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_framework <IN> An initialized test framework with test type
 * RT_TEST_TYPE_REPLICATOR
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard <IN> shard being operated on
 * @param command <IN> command being executed; caller can free after
 * return.
 * @param output <OUT> cr lf delimited output
 * @return SDF_SUCCESS on success, other on failure
 */
SDF_status_t
rtfw_command_sync(struct replication_test_framework *framework, vnode_t node,
                  SDF_shardid_t shard, const char *command_arg,
                  char **output);

/**
 * @brief Asynchronous shard create operation
 *
 * This is a thin wrapper which invokes the model's #rtm_create_shard function,
 * sends sends a HFCSH message to the replication service on the given node,
 * and applies the closure on completion.
 *
 * @param test_framework <IN> Test framework
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard_meta <IN> shard to create, caller owns buffer
 * @param cb <IN> closure invoked on completion
 */
void
rtfw_create_shard_async(struct replication_test_framework *test_framework,
                        vnode_t node, struct SDF_shard_meta *shard_meta,
                        replication_test_framework_cb_t cb);

/**
 * @brief Synchronous shard create operation from fthThread
 *
 * This is a thin wrapper arround #rtfw_create_shard_async which uses a
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_framework <IN> Test framework
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard_meta <IN> shard to create, caller owns buffer
 * @return SDF status
 */
SDF_status_t
rtfw_create_shard_sync(struct replication_test_framework *test_framework,
                       vnode_t node, struct SDF_shard_meta *shard_meta);

/**
 * @brief Asynchronous shard delete operation
 *
 * This is a thin wrapper which invokes the model's #rtm_delete_shard function,
 * sends sends a HFDSH message to the replication service on the given node,
 * and applies the closure on completion.
 *
 * @param test_framework <IN> Test framework
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard <IN> Shard id to delete
 * @param cb <IN> closure invoked on completion
 */
void
rtfw_delete_shard_async(struct replication_test_framework *test_framework,
                        vnode_t node, SDF_shardid_t shard,
                        replication_test_framework_cb_t cb);

/**
 * @brief Synchronous shard delete operation from fthThread
 *
 * This is a thin wrapper arround #rtfw_delete_shard_sync which uses n
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_framework <IN> Test framework
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param shard <IN> Shard id to delete
 * @param cb <IN> closure invoked on completion
 */
SDF_status_t
rtfw_delete_shard_sync(struct replication_test_framework *test_framework,
                       vnode_t node, SDF_shardid_t shard);

/**
 * @brief Asynchronous write operation
 *
 * This is a thin wrapper which invokes the node's replicator
 * #sdf_replicator_get_op_meta function, starts the operation in the
 * model with #rtm_start_write, and sends an HFPTF message to the
 * replicator.
 *
 * On completion, the FHPTC (success) or FHPTF (failure) message will
 * be received thus triggering logic which invokes #rtm_write_complete
 * and applying the user's closure function.
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param meta <IN> Meta-data.  Should be initialized with
 * #replication_test_meta_init before non-defaults are specified.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @param data <IN> Data
 * @param data_len <IN> Data length
 * @param cb <IN> closure invoked on completion.  The buffer should be treated
 * as read-only until released by rtfw_read_free.
 */
void
rtfw_write_async(struct replication_test_framework *framework,
                 SDF_shardid_t shard, vnode_t node,
                 const struct replication_test_meta *meta,
                 const void *key, size_t key_len,
                 const void *data, size_t data_len,
                 replication_test_framework_cb_t cb);

/**
 * @brief Synchronous write operation from fthThread
 *
 * This is a thin wrapper arround #rtfw_write_async  which uses n
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param meta <IN> Meta-data.  Should be initialized with
 * #replication_test_meta_init before non-defaults are specified.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @param data <IN> Data
 * @param data_len <IN> Data length
 * @return SDF status
 */
SDF_status_t
rtfw_write_sync(struct replication_test_framework *framework,
                SDF_shardid_t shard, vnode_t node,
                const struct replication_test_meta *meta,
                const void *key, size_t key_len,
                const void *data, size_t data_len);

/**
 * @brief Asynchronous read operation
 *
 * This is a thin wrapper which invokes the node's replicator
 * #sdf_replicator_get_op_meta function, starts the operation in the
 * model with #rtm_start_read, and sends an HFGFF message to the
 * replicator.
 *
 * On completion, the FHDAT (success) or FHGTF (failure) message will
 * be received thus triggering logic which invokes #rtm_read_complete
 * and applyies the user's closure function.
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @param cb <IN> closure invoked on completion.  The buffer should be treated
 * as read-only until released by rtfw_read_free.
 */
void
rtfw_read_async(struct replication_test_framework *framework,
                SDF_shardid_t shard, vnode_t node,
                const void *key, size_t key_len,
                replication_test_framework_read_async_cb_t cb);

/**
 * @brief Synchronous read operation from fthThread
 *
 * This is a thin wrapper arround #rtfw_read_async which uses n
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @param data <OUT> Data, must free with free_cb.
 * @param data_len <OUT> Data length
 * @param free_cb <OUT> Free callback for data.  Call
 * replication_test_framework_read_data_free_cb_apply(free_cb, data, data_len)
 * @return SDF status
 */
SDF_status_t
rtfw_read_sync(struct replication_test_framework *framework,
               SDF_shardid_t shard, vnode_t node,
               const void *key, size_t key_len, void **data, size_t *data_len,
               replication_test_framework_read_data_free_cb_t *free_cb);

/**
 * @brief Asynchronous delete operation
 *
 * This is a thin wrapper which invokes the node's replicator
 * #sdf_replicator_get_op_meta function, starts the operation in the
 * model with #rtm_start_write, and sends an HZSF message to the
 * replicator.
 *
 * On completion, the FHDEC (success) or FHDEF (failure) message will
 * be received thus triggering logic which invokes #rtm_read_complete
 * and applies the user's closure function.
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @param cb <IN> closure invoked on completion.  The buffer should be treated
 * as read-only until released by rtfw_read_free.
 */
void
rtfw_delete_async(struct replication_test_framework *framework,
                  SDF_shardid_t shard, vnode_t node,
                  const void *key, size_t key_len,
                  replication_test_framework_cb_t cb);

/**
 * @brief Synchronous delete operation from fthThread
 *
 * This is a thin wrapper arround #rtfw_delete_async  which uses n
 * #fthMbox_t to allow synchronous use.
 *
 * @param test_model <IN> Test model
 * @param shard <IN> Unique shard id
 * @param ltime <IN> Shard ltime at which this was started
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param key <IN> Key
 * @param key_len <IN> Key length
 * @return SDF status
 */
SDF_status_t
rtfw_delete_sync(struct replication_test_framework *framework,
                 SDF_shardid_t shard, vnode_t node,
                 const void *key, size_t key_len);

/* RT_TYPE_META_STORAGE functions */

/**
 * @brief Set function called when replication shard meta data propagates
 * to a node.
 *
 * @param test_framework <IN> An initialized test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param update_cb <IN> applied when an update is detected for
 * a shard whether remote or local.  This replaces all previously
 * specified closures.  rtfw_shard_meta_cb_null may be specified to
 * route messages into the aether.
 */
void
rtfw_set_meta_storage_cb(struct replication_test_framework *framework,
                         rtfw_shard_meta_cb_t update_cb);

/**
 * @brief Create shard meta-data asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param cr_shard_meta <IN> shard meta-data to create.  The requested lease
 * time is ignored.  The caller may immediately free cr_shard_meta.
 *
 * @param cb <IN> applied on completion. See #rtfw_shard_meta_cb
 * for ownership of closure shard_meta argument.
 */
void rtfw_create_shard_meta_async(struct replication_test_framework *framework,
                                  vnode_t node,
                                  const struct cr_shard_meta *cr_shard_meta,
                                  rtfw_shard_meta_cb_t cb);

/**
 * @brief Create shard meta-data asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param cr_shard_meta <IN> shard meta-data to put.  The requested lease
 * time is ignored.
 *
 * @param meta_out <OUT> The meta-data with a valid (relative to the
 * remote time) lease time is stored here, with the caller owning.
 * On failure, conflicting meta-data may be stored here.
 *
 * @param lease_expires_out <OUT> A lower bound on when the lease expires in
 * absolute local time according to the #sdf_replicator_api gettime
 * function.  Network propagation delays have been accounted for.
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t rtfw_create_shard_meta_sync(struct replication_test_framework *framework,
                                         vnode_t node,
                                         const struct cr_shard_meta *cr_shard_meta,
                                         struct cr_shard_meta **meta_out,
                                         struct timeval *lease_expires_out);

/**
 * @brief Get shard meta-data asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param sguid <IN> data shard GUID
 *
 * @param shard_meta <IN> subset of shard meta-data to get. This is used instead
 * of the sguid by itself so that the "client" side doesn't have to maintain
 * any sort of routing hash.  The caller may free to free shard_meta
 * immediately.
 *
 * @param cb <IN> applied on completion. See #rtfw_shard_meta_cb
 * for ownership of closure shard_meta argument.
 */
void rtfw_get_shard_meta_async(struct replication_test_framework *framework,
                               vnode_t node, SDF_shardid_t sguid,
                               const struct sdf_replication_shard_meta *shard_meta,
                               rtfw_shard_meta_cb_t cb);

/**
 * @brief Get shard meta-data synchronously
 *
 * This is a thin wrapper around #rtfw_get_shard_meta_async
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param sguid <IN> data shard GUID
 *
 * @param shard_meta <IN> subset of shard meta-data to get. This is used instead
 * of the sguid by itself so that the "client" side doesn't have to maintain
 * any sort of routing hash.
 *
 * @param meta_out <OUT> The meta-data retrieved is stored here, with the
 * caller owning.
 *
 * @param lease_expires_out <OUT> A lower bound on when the lease expires in
 * absolute local time according to the #sdf_replicator_api gettime
 * function.  Network propagation delays have been accounted for.
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t rtfw_get_shard_meta_sync(struct replication_test_framework *framework,
                                      vnode_t node, SDF_shardid_t sguid,
                                      const struct sdf_replication_shard_meta *shard_meta,
                                      struct cr_shard_meta **meta_out,
                                      struct timeval *lease_expires_out);

/**
 * @brief Put shard meta-data asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param cr_shard_meta <IN> shard meta-data to put.  The requested lease
 * time is ignored.  The shard meta-data being put must be causaly related
 * to the current meta data, meaning that its shard_meta_seqno must be exactly
 * one higher than the last version (otherwise the callback status argument
 * will be SDF_STALE_META_SEQNO).  The caller may immediately free
 * cr_shard_meta.
 *
 * @param cb <IN> applied on completion. See #rtfw_shard_meta_cb
 * for ownership of closure shard_meta argument.
 */
void rtfw_put_shard_meta_async(struct replication_test_framework *framework,
                               vnode_t node,
                               const struct cr_shard_meta *cr_shard_meta,
                               rtfw_shard_meta_cb_t cb);

/**
 * @brief Put shard meta-data asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param cr_shard_meta <IN> shard meta-data to put.  The requested lease
 * time is ignored.  The shard meta-data being put must be causaly related
 * to the current meta data, meaning that its shard_meta_seqno must be exactly
 * one higher than the last version (otherwise the return will be
 * SDF_STALE_META_SEQNO).
 *
 * @param meta_out <OUT> The meta-data with a valid (relative to the
 * start of the call) lease time is stored here, with the caller owning.
 * On failure,  conflicting meta-data may be stored here.
 *
 * @param lease_expires_out <OUT> A lower bound on when the lease expires in
 * absolute local time according to the #sdf_replicator_api gettime
 * function.  Network propagation delays have been accounted for.
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t rtfw_put_shard_meta_sync(struct replication_test_framework *framework,
                                      vnode_t node,
                                      const struct cr_shard_meta *cr_shard_meta,
                                      struct cr_shard_meta **meta_out,
                                      struct timeval *lease_expires_out);


/**
 * @brief Delete shard meta asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param cr_shard_meta <IN> Current shard meta-data.  The caller may
 * immediately free #cr_shard_meta.
 *
 * @param cb <IN> applied on completion.   On success a NULL pointer
 * will be returned for the current meta-data.
 */
void rtfw_delete_shard_meta_async(struct replication_test_framework *framework,
                                  vnode_t node,
                                  const struct cr_shard_meta *cr_shard_meta,
                                  rtfw_shard_meta_cb_t cb);

/**
 * @brief Delete shard meta asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param cr_shard_meta <IN> Current shard meta-data
 *
 * @param meta_out <OUT> On success, NULL is stored here.  On failure,
 * conflicting meta-data is stored here with the caller owning.
 *
 * @param lease_expires_out <OUT> A lower bound on when the lease expires in
 * absolute local time according to the #sdf_replicator_api gettime
 * function.  Network propagation delays have been accounted for.
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t rtfw_delete_shard_meta_sync(struct replication_test_framework *framework,
                                         vnode_t node,
                                         const struct cr_shard_meta *cr_shard_meta,
                                         struct cr_shard_meta **meta_out,
                                         struct timeval *lease_expires_out);

/**
 * @brief Get last sequence number asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param sguid <IN> data shard GUID
 *
 * @param cb <IN> applied on completion.
 */
void rtfw_get_last_seqno_async(struct replication_test_framework *framework,
                               vnode_t node, SDF_shardid_t sguid,
                               rtfw_last_seqno_cb_t cb);

/**
 * @brief Get last sequence number synchronously
 *
 * This is a thin wrapper around #rtfw_get_last_seqno_async
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param sguid <IN> data shard GUID
 *
 * @param pseqno <OUT> The sequence number retrieved is stored here.
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t rtfw_get_last_seqno_sync(struct replication_test_framework *framework,
                                      vnode_t node, SDF_shardid_t sguid,
                                      uint64_t *pseqno);

/**
 * @brief Get iteration cursors asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param sguid <IN> data shard GUID
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param seqno_start <IN> start of sequence number range, inclusive
 *
 * @param seqno_len   <IN> max number of cursors to return at a time
 *
 * @param seqno_max   <IN> end of sequence number range, inclusive
 *
 * @param cb <IN> applied on completion.
 */
void rtfw_get_cursors_async(struct replication_test_framework *framework,
                            SDF_shardid_t shard, vnode_t node,
                            uint64_t seqno_start, uint64_t seqno_len, uint64_t seqno_max,
                            void *cursor, int cursor_size,
                            rtfw_get_cursors_cb_t cb);

/**
 * @brief Get iteration cursors synchronously
 *
 * This is a thin wrapper around #rtfw_get_cursors_async
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param sguid <IN> data shard GUID
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param seqno_start <IN> start of sequence number range, inclusive
 *
 * @param seqno_len   <IN> max number of cursors to return at a time
 *
 * @param seqno_max   <IN> end of sequence number range, inclusive
 *
 * @param data        <OUT> Data, must free with free_cb.
 * @param data_len    <OUT> Data length
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t rtfw_get_cursors_sync(struct replication_test_framework *framework,
                                   SDF_shardid_t shard, vnode_t node,
                                   uint64_t seqno_start, uint64_t seqno_len,
                                   uint64_t seqno_max,
                                   void *cursor, int cursor_size,
                                   void **data, size_t *data_len,
                                   replication_test_framework_read_data_free_cb_t *free_cb);

/**
 * @brief Get by cursor asynchronously
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param sguid <IN> data shard GUID
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param cursor     <IN> Opaque cursor data
 * @param cursor_len <IN> Cursor length
 *
 * @param cb <IN> applied on completion.
 */
void rtfw_get_by_cursor_async(struct replication_test_framework *framework,
                              SDF_shardid_t shard, vnode_t node,
                              const void *cursor, size_t cursor_len,
                              rtfw_get_by_cursor_cb_t cb);

/**
 * @brief Get by cursor synchronously
 *
 * This is a thin wrapper around #rtfw_get_cursors_async
 *
 * @param test_framework <IN> A running test framework with test type
 * RT_TEST_TYPE_META_STORAGE
 *
 * @param sguid <IN> data shard GUID
 *
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 *
 * @param cursor      <IN> Opaque cursor data
 * @param cursor_len  <IN> Cursor length
 * @param max_key_len <OUT> Maximum key length
 *
 * @param key         <OUT> Key (points to buffer of length max_key_len provided by caller)
 * @param key_len     <OUT> Key length
 * @param exptime     <OUT> Expiry time
 * @param createtime  <OUT> Create time
 * @param seqno       <OUT> Sequence number
 * @param data        <OUT> Data, must free with free_cb.
 * @param data_len    <OUT> Data length
 *
 * @return SDF_SUCCESS on success, otherwise on failure.
 */
SDF_status_t rtfw_get_by_cursor_sync(struct replication_test_framework *framework,
                                     SDF_shardid_t shard, vnode_t node,
                                     void *cursor, size_t cursor_len,
                                     char *key, int max_key_len, int *key_len,
                                     SDF_time_t *exptime, SDF_time_t *createtime,
                                     uint64_t *seqno,
                                     void **data, size_t *data_len,
                                     replication_test_framework_read_data_free_cb_t *free_cb);

/* Internal API for nodes, etc. */

/**
 * @brief Send mesage
 *
 * For internal rtfw use and rtfw unit tests only.
 *
 * @param test_framework <IN> A running test framework.
 * @param node <IN> Node from which this request was "sent".  Node ids
 * are consecutive integers starting at 0.
 * @param wrapper <IN> sdf_msg wrapper.  One reference count is consumed
 * by the function.
 * @param ar_mbx <IN> ar_mbx as in #sdf_msg_send
 */
void
rtfw_send_msg(struct replication_test_framework *framework,
              vnode_t node, struct sdf_msg_wrapper *msg_wrapper,
              struct sdf_fth_mbx *ar_mbx);

/**
 * @brief Synchronously deliver message
 *
 * The function shall plat_assert_always that the source and destination
 * nodes are valid.
 *
 * One reference count of msg_wrapper is consumed
 *
 * XXX: Should be a part of the API provided to the node
 *
 * @param test_framework <IN> Test framework
 * @param msg_wrapper <IN> Message being sent
 */
SDF_status_t
rtfw_receive_message(struct replication_test_framework *framework,
                     struct sdf_msg_wrapper *msg_wrapper);

/**
 * @brief Synchronously queue liveness ping
 *
 * The function shall plat_assert_always that the source and destination
 * nodes are valid.
 *
 * Liveness pings are out-of-band from the normal messaging system since
 * they drive its node view.
 *
 * Liveness pings are not broadcast because the network connectivity
 * view is handled by the sending node and split-brain scenarios
 * require fine-grained control over which nodes can reach which
 * other nodes.
 *
 * XXX: Should be a part of the API provided to the node
 *
 * @param test_framework <IN> Test framework
 * @param src_node <IN> Originating "live" node
 * @param dest_node <IN> Receiving node
 * @param src_epoch <IN> Monotonically increasing each time src_node
 * restarts.  Note that this is different from #rtfw_receive_connection_close's
 * dest_epoch.
 */
void
rtfw_receive_liveness_ping(struct replication_test_framework *framework,
                           vnode_t src_node, vnode_t dest_node,
                           int64_t src_epoch);

/**
 * @brief Synchronously queue socket close (ECONNRESETBYPEER)
 *
 * The function shall plat_assert_always that the source and destination
 * nodes are valid.
 *
 * The real messaging/liveness system closes all TCP connections when it
 * decides that the remote end is 'dead', with this behavior causing the
 * (eventually) receiving end to correctly generate synthetic errors for
 * messages which will never be delivered.
 *
 * @param test_framework <IN> Test framework
 * @param src_node <IN> Node closing the conenction because it thought
 * #dest_node was dead.
 * @param dest_node <IN> Node receiving socket close
 * @param dest_epoch <IN> Monotonically increasing each time #dest_node
 * restarts.  Note this is different from #rtfw_receive_liveness_ping since
 * it is being used to fence socket closes which happened before a #dest_node
 * restart
 */
void
rtfw_receive_connection_close(struct replication_test_framework *framework,
                              vnode_t src_node, vnode_t dest_node,
                              int64_t dest_epoch);

/**
 * @brief init a shard_meta
 *
 * XXX: drew 2010-04-01 inconsistent naming
 */
struct SDF_shard_meta *
rtfw_init_shard_meta(struct replication_test_config *config,
                     vnode_t first_node,
                     SDF_shardid_t shard_id /* in real system generated by generate_shard_ids() */,
                     SDF_replication_props_t *replication_props);
/**
 * @brief Convert test configuration into replication properties
 *
 * @param config <IN> configuration
 * @param props <OUT> replication properties
 *
 * XXX: drew 2010-04-01 inconsistent naming
 */
void
rtfw_set_default_replication_props(struct replication_test_config *config,
                                   SDF_replication_props_t *props);

/**
 * @brief Allocate shard id for creation
 *
 * XXX: drew 2010-04-01 I have no clue what this is for
 */
void
rtfw_read_free(plat_closure_scheduler_t *context, void *env,
               const void *data, size_t data_len);

/**
 * @brief Return pseudo-random delay in microseconds
 *
 * @param framework <IN> this framework's PRNG is used so that
 * values on subsequent test runs are consistent
 * @param timing <IN> range on values to return
 * @param tv_out <OUT> when non-NULL, the delay is stored here
 * in timval form.
 */
int
rtfw_get_delay_us(struct replication_test_framework *framework,
                  const struct replication_test_timing *timing,
                  struct timeval *tv_out);


__END_DECLS

#endif /* ndef REPLICATION_TEST_CONFIG_H */
