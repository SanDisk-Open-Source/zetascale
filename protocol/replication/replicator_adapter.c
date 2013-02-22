/*
 * File:   sdf/protocol/replication/replicator_adapter.c
 *
 * Author: drew
 *
 * Created on April 18, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: replicator_adapter.c 1500 2008-06-06 02:06:33Z drew $
 */

#include <sys/queue.h>

#include "platform/assert.h"
#include "platform/fth_scheduler.h"
#include "platform/logging.h"
#include "platform/stdlib.h"
#include "platform/timer_dispatcher.h"

#include "fth/fth.h"

#include "common/sdftypes.h"

#include "sdfmsg/sdf_msg_action.h"
#include "sdfmsg/sdf_msg_types.h"
#include "sdfmsg/sdf_msg_wrapper.h"

#include "shared/name_service.h"

#include "protocol/protocol_common.h"

#include "replicator_adapter.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION, "adapter");

/**
 * XXX: With the wildcard send/receive queues we should ditch the ra_peer
 * plumbing.
 */
struct ra_peer_state;

struct sdf_replicator_adapter {
    struct sdf_replicator_config config;

    /** @brief Scheduler for internal sdf client (nameservice, etc.) calls */
    struct plat_closure_scheduler *internal_sdf_client_scheduler;

    /** @brief Timer Timer dispatcher */
    struct plat_timer_dispatcher *timer_dispatcher;

    /** @brief Thread polling plat_timer_dispatcher */
    fthThread_t *timer_thread;

    /** @brief Plumbing from replicator into adapter */
    struct sdf_replicator_api replicator_cbs;

    /** @brief The wrapped replicator, owned by this */
    struct sdf_replicator *replicator;

    /** @brief Used for all incoming traffic */
    struct sdf_msg_action *recv_msg_action;

    /** @brief Inbound requests from home code */
    struct sdf_msg_binding *replication_msg_binding;

    /** @brief Inbound requests from peer */
    struct sdf_msg_binding *peer_msg_binding;

    /** Queue pair from replication_server_service to response_service */
    struct sdf_queue_pair *send_queue_pair;

    /** Queue pair from response_service to replication_server_service */
    struct sdf_queue_pair *recv_queue_pair;

    /** Array of peer states sized config.node_count */
    struct ra_peer_state **peers;

    /** @brief Shutdown called */
    int shutdown_count;

    /** @brief Invoke on shutdown completion */
    sdf_replicator_shutdown_cb_t shutdown_closure;
};

struct sdf_replicator_adapter_thread_state {
    /** @brief Replicator adapter for log messages */
    struct sdf_replicator_adapter *ra;
};

struct ra_peer_state {
    struct sdf_replicator_adapter *parent;

    /** @brief local, replication_server <-> remote, response */
    struct sdf_queue_pair *inbound;

    /** @brief local, response <-> remote, replication_server */
    struct sdf_queue_pair *outbound;
};

static void ra_replicator_stopped(plat_closure_scheduler_t *context,
                                  void *env);
static void ra_scheduler_stopped(plat_closure_scheduler_t *context, void *env);
static void ra_gettime(plat_closure_scheduler_t *context, void *env,
                       struct timeval *ret);
static void ra_send_msg(plat_closure_scheduler_t *context, void *env,
                        struct sdf_msg_wrapper *msg_wrapper,
                        struct sdf_fth_mbx *ar_mbx,
                        SDF_status_t *out);
static void ra_timer_thread(uint64_t arg);
static void *ra_sched_pts_alloc(void *extra);
static void ra_sched_pts_start(void *pts);
static void ra_sched_pts_free(void *pts);

static struct ra_peer_state *
ra_peer_alloc(struct sdf_replicator_adapter *parent, vnode_t node);
static void ra_peer_free(struct ra_peer_state *peer);
static struct sdf_queue_pair *
ra_queue_pair_alloc(vnode_t src_vnode, service_t src_service,
                    vnode_t dest_vnode, service_t dest_service);

struct sdf_replicator_adapter *
sdf_replicator_adapter_alloc(const struct sdf_replicator_config *config,
                             struct sdf_replicator *(*replicator_alloc)
                             (const struct sdf_replicator_config *config,
                              struct sdf_replicator_api *callbacks,
                              void *extra),
                             void *replicator_alloc_extra) {
    struct sdf_replicator_adapter *ret;
    int failed;
    int i;

    failed = !plat_calloc_struct(&ret);
    if (!failed) {
        ret->config = *config;

        ret->peers = plat_calloc(config->node_count, sizeof (*ret->peers));
        if (!ret->peers) {
            failed = 1;
        }

        ret->shutdown_count = 0;
        ret->shutdown_closure = sdf_replicator_shutdown_cb_null;
    }

    if (!failed) {
        ret->send_queue_pair =
            ra_queue_pair_alloc(ret->config.my_node /* src */,
                                ret->config.replication_service /* src */,
                                VNODE_ANY /* dest */,
                                SERVICE_ANY /* dest */);
        failed = !ret->send_queue_pair;
    }

    if (!failed) {
        ret->recv_queue_pair =
            ra_queue_pair_alloc(VNODE_ANY /* src */,
                                SERVICE_ANY /* src */,
                                ret->config.my_node /* dest */,
                                ret->config.replication_service /* dest */);
        failed = !ret->recv_queue_pair;
    }

    // Hack around to setup queue pairs for PEER_META_SUPER
    if (!failed) {
        failed = !ra_queue_pair_alloc(ret->config.my_node /* src */,
                                      SDF_REPLICATION_PEER_META_SUPER /* src */,
                                      VNODE_ANY /* dest */,
                                      SERVICE_ANY /* dest */);
    }

    if (!failed) {
        failed =
            !ra_queue_pair_alloc(VNODE_ANY /* src */,
                                 SERVICE_ANY /* src */,
                                 ret->config.my_node /* dest */,
                                 SDF_REPLICATION_PEER_META_SUPER /* dest */);
    }

    if (!failed) {
        failed = !ra_queue_pair_alloc(ret->config.my_node /* src */,
                                      GOODBYE /* src */,
                                      VNODE_ANY /* dest */,
                                      SERVICE_ANY /* dest */);
    }

    if (!failed) {
        failed =
            !ra_queue_pair_alloc(VNODE_ANY /* src */,
                                 SERVICE_ANY /* src */,
                                 ret->config.my_node /* dest */,
                                 GOODBYE /* dest */);
    }



    /*
     * FIXME tomr 2008-11-20
     * have revised ra_peer_alloc to create queues needed by "flash to responses" service to
     * allow for internode transport. Currently wildcards work well when on the same node but since the remote flash
     * needs to get remote responses the message needs to be picked up and delivered to the node first
     * and so not to conflict we bypass the queue creation if were the same node. This is not a permenant fix.
     * In order to get the send to post and allow the transport this was the least obstrusive way to implement
     */

    for (i = 0; !failed && i < ret->config.node_count; ++i) {
        if (i != ret->config.my_node) {
            ret->peers[i] = ra_peer_alloc(ret, i);
            if (!ret->peers[i]) {
                failed = 1;
            }
        }
    }

    /*
     * XXX: drew 2008-11-24 Since I eliminated the name service
     * calls this hasn't been used for anything; although I've left it
     * because that's easier than re-adding it if any synchronous
     * calls need adapting.
     */
    if (!failed) {
        ret->internal_sdf_client_scheduler =
            plat_fth_scheduler_alloc(ret->config.nthreads,
                                     &ra_sched_pts_alloc, ret /* alloc extra */,
                                     &ra_sched_pts_start, &ra_sched_pts_free);
        if (!ret->internal_sdf_client_scheduler) {
            failed = 1;
        }
    }

    if (!failed) {
        ret->replicator_cbs.gettime =
            plat_timer_dispatcher_gettime_create(PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS,
                                                 &ra_gettime, ret);
        ret->timer_dispatcher =
            plat_timer_dispatcher_alloc(ret->replicator_cbs.gettime);
        failed = !ret->timer_dispatcher;

        ret->replicator_cbs.timer_dispatcher = ret->timer_dispatcher;
    }


    /* Create replicator */
    if (!failed) {
        ret->timer_thread = fthSpawn(&ra_timer_thread, 40960);
        plat_assert(ret->timer_thread);

        ret->replicator_cbs.send_msg =
            sdf_replicator_send_msg_cb_create(PLAT_CLOSURE_SCHEDULER_SYNCHRONOUS,
                                              &ra_send_msg, ret);

#if 0
        /*
         * This is old, but shows how synchronous functions can be interfaced
         * to event driven replicators.
         */
        ret->replicator_cbs.get_meta_from_cguid =
            sdf_replicator_get_meta_from_cguid_cb_create(ret->internal_sdf_client_scheduler,
                                                         &ra_get_meta_from_cguid,
                                                         ret);
#endif

        ret->replicator = (*replicator_alloc)(&ret->config, &ret->replicator_cbs,
                                              replicator_alloc_extra);
        if (!ret->replicator) {
            failed = 1;
        }
    }

    if (!failed) {
        ret->recv_msg_action =
            sdf_msg_action_closure_alloc(ret->replicator->recv_closure);
        failed = !ret->recv_msg_action;
    }

    if (!failed) {
        ret->replication_msg_binding =
            sdf_msg_binding_create(ret->recv_msg_action,
                                   ret->config.my_node,
                                   ret->config.replication_service);
        if (!ret->replication_msg_binding) {
            plat_log_msg(21459, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "Can't bind bind node %u service %u",
                         ret->config.my_node,
                         ret->config.replication_service);
            failed = 1;
        }
    }

    if (!failed) {
        ret->peer_msg_binding =
            sdf_msg_binding_create(ret->recv_msg_action,
                                   ret->config.my_node,
                                   SDF_REPLICATION_PEER_META_SUPER);
        if (!ret->peer_msg_binding) {
            plat_log_msg(21459, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "Can't bind bind node %u service %u",
                         ret->config.my_node,
                         ret->config.replication_peer_service);
            failed = 1;
        }
    }

    if (failed && ret) {
        sdf_replicator_adapter_shutdown(ret, sdf_replicator_shutdown_cb_null);
        ret = NULL;
    }

    if (!failed) {
        plat_log_msg(21460, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "replicator_adapter %p allocated", ret);
    } else {
        plat_log_msg(21461, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "sdf_replicator_adapter_alloc failed");
    }

    return (ret);
}

struct sdf_replicator *
sdf_replicator_adapter_get_replicator(struct sdf_replicator_adapter *adapter) {
    return (adapter->replicator);
}

int
sdf_replicator_adapter_start(struct sdf_replicator_adapter *ra) {
    int ret = 0;

    plat_log_msg(21462, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "replicator adapter %p starting", ra);

    if (!ret) {
        fthResume(ra->timer_thread, (uint64_t)ra);
    }

    ret = plat_fth_scheduler_start(ra->internal_sdf_client_scheduler);
    if (ret) {
        plat_log_msg(21463, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "replicator adapter %p scheduler failed to start", ra);
    }

    if (!ret) {
        ret = sdf_replicator_start(ra->replicator);
        if (ret) {
            plat_log_msg(21464, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                         "replicator adapter %p replicator failed to start", ra);
        }
    }

    if (!ret) {
        plat_log_msg(21465, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "replicator adapter %p started", ra);
    } else {
        plat_log_msg(21466, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "replicator adapter %p failed to start", ra);
    }

    return (ret);
}

void
sdf_replicator_adapter_shutdown(struct sdf_replicator_adapter *ra,
                                sdf_replicator_shutdown_cb_t shutdown_closure) {

    int before;
    sdf_replicator_shutdown_cb_t shutdown;
    struct sdf_replicator *replicator;

    shutdown =
        sdf_replicator_shutdown_cb_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                          &ra_replicator_stopped, ra);

    before = __sync_fetch_and_add(&ra->shutdown_count, 1);

    plat_assert(!before);
    if (!before) {
        plat_log_msg(21467, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "replicator_adapter %p shutdown called", ra);

        ra->shutdown_closure = shutdown_closure;

        replicator = ra->replicator;
        ra->replicator = NULL;
        if (replicator) {
            sdf_replicator_shutdown(replicator, shutdown);
        } else {
            ra_replicator_stopped(NULL, ra);
        }
    }
}

/** @brief Called when wrapped replicator has shutdown */
static void
ra_replicator_stopped(plat_closure_scheduler_t *context, void *env) {
    struct sdf_replicator_adapter *ra = (struct sdf_replicator_adapter *)env;
    plat_closure_scheduler_shutdown_t shutdown;

    plat_log_msg(21468, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "replicator_adapter %p replicator stopped", ra);
    ra->replicator = NULL;


    shutdown =
        plat_closure_scheduler_shutdown_create(PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                               &ra_scheduler_stopped, ra);
    if (ra->internal_sdf_client_scheduler) {
        plat_closure_scheduler_shutdown(ra->internal_sdf_client_scheduler,
                                        shutdown);
    } else {
        ra_scheduler_stopped(NULL, ra);
    }
}

/** @brief Decrement reference count */
static void
ra_scheduler_stopped(plat_closure_scheduler_t *context, void *env) {
    int i;
    struct sdf_replicator_adapter *ra = (struct sdf_replicator_adapter *)env;

    plat_log_msg(21469, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "replicator_adapter %p scheduler stopped", ra);
    ra->internal_sdf_client_scheduler = NULL;

    plat_assert(!ra->replicator);
    plat_assert(!ra->internal_sdf_client_scheduler);

    if (ra->replication_msg_binding) {
        sdf_msg_binding_free(ra->replication_msg_binding);
    }

    if (ra->peer_msg_binding) {
        sdf_msg_binding_free(ra->peer_msg_binding);
    }

    if (ra->recv_msg_action) {
        sdf_msg_action_free(ra->recv_msg_action);
    }

    /** XXX use destructors here */
    if (ra->send_queue_pair) {
        sdf_delete_queue_pair(ra->send_queue_pair);
    }

    if (ra->recv_queue_pair) {
        sdf_delete_queue_pair(ra->recv_queue_pair);
    }

    for (i = 0; i < ra->config.node_count; ++i) {
        if (ra->peers[i]) {
            ra_peer_free(ra->peers[i]);
        }
    }

    plat_free(ra->peers);

    plat_free(ra);
}

/** @brief Get time of day */
static void
ra_gettime(plat_closure_scheduler_t *context, void *env,
           struct timeval *ret) {
#ifdef RA_MONOTONIC_TIME
    fthGetTimeMonotonic(ret)
#else /* def RA_MONOTONIC_TIME */
    fthGetTimeOfDay(ret);
#endif /* else def RA_MONOTONIC_TIME */
}

/*
 * @brief Send message
 *
 * 1.  We don't do anything about the reference counts and assume
 *     that the caller will deal with that.
 *
 * 2.  The current sdf_msg interface modifies the header of the
 *     sdf_msg which was passed in.  We don't do anything to
 *     control this.
 */
static void
ra_send_msg(plat_closure_scheduler_t *context, void *env,
            struct sdf_msg_wrapper *msg_wrapper, struct sdf_fth_mbx *ar_mbx,
            SDF_status_t *status) {
    struct sdf_replicator_adapter *adapter =
        (struct sdf_replicator_adapter *)env;
    struct sdf_msg *msg;
    SDF_protocol_msg_t *pm;
    SDF_Protocol_Msg_Info_t *pmi;
    int result;
    int is_request;

    msg = NULL;
    sdf_msg_wrapper_rwref(&msg, msg_wrapper);
    pm = (SDF_protocol_msg_t *)msg->msg_payload;
    pmi = &(SDF_Protocol_Msg_Info[pm->msgtype]);

    /* XXX: add access functions which sanity check msg */
    plat_log_msg(21470, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "replication_adapter %p send"
                 " Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d",
                 adapter, pmi->name, pmi->shortname,
                 pm->node_from, SDF_Protocol_Nodes_Info[pmi->src].name,
                 pm->node_to, SDF_Protocol_Nodes_Info[pmi->dest].name,
                 pm->tag);

    is_request = (msg_wrapper->msg_wrapper_type == SMW_TYPE_REQUEST);

    sdf_msg_wrapper_rwrelease(&msg, msg_wrapper);

    result = sdf_msg_wrapper_send(msg_wrapper, ar_mbx);

    /*
     * XXX: drew 2009-06-03 We're moving to a world were all errors are handled
     * with a synthetic error message since that means the normal and error
     * cases have the same path for less code.  Allow a NULL status pointer
     * since that's how we're headed, and just sanity check if something
     * bad happened.
     */
    if (status) {
        if (!result) {
            *status = SDF_SUCCESS;
        } else {
            *status = SDF_FAILURE_MSG_SEND;
        }
    } else if (result) {
        /* Log all we can on failure in case lower level code doesn't */

        /*
         * Only requests are fatal because an error response means that the messaging
         * layer has violated its contract to deliver all errors as synthetic responses.
         */
        plat_log_msg(21471, LOG_CAT,
                     is_request ?  PLAT_LOG_LEVEL_FATAL : PLAT_LOG_LEVEL_WARN,
                     "replication_adapter %p node %u send failed", env,
                     adapter->config.my_node);
        if (is_request) {
            plat_abort();
        }
    }
}

static void
ra_timer_thread(uint64_t arg) {
    struct sdf_replicator_adapter *ra = (struct sdf_replicator_adapter *)arg;
    // int fired;
    uint64_t delay_nsecs = ra->config.timer_poll_usecs * 1000;

    /*
     * FIXME: drew 2009-06-02 Need to play nice with
     * #sdf_replicator_adapter_shutdown detecting shutdown (off a
     * field?) and resuming the shutdown process.
     */
    while (1) {
        // fired = plat_timer_dispatcher_fire(ra->timer_dispatcher);
        (void) plat_timer_dispatcher_fire(ra->timer_dispatcher);
        fthNanoSleep(delay_nsecs);
    }
}

static void  *
ra_sched_pts_alloc(void *extra) {
    struct sdf_replicator_adapter *ra = (struct sdf_replicator_adapter *)extra;
    struct sdf_replicator_adapter_thread_state *rats;
    int failed;

    failed = !plat_calloc_struct(&rats);
    if (!failed) {
        rats->ra = ra;
    }

    /* XXX: frew 2009-09-25 Coverity flags this as dead code too */
#if 0
    if (failed && rats) {
        /* XXX: drew 2008-11-24 This is dead code until we have more state */
        ra_sched_pts_free(rats);
        rats = NULL;
    }
#endif

    return (rats);
}

static void
ra_sched_pts_start(void *pts) {
    struct sdf_replicator_adapter_thread_state *rats =
        (struct sdf_replicator_adapter_thread_state *)pts;
    struct sdf_replicator_adapter_thread_state *old;

    old = plat_attr_sdf_replicator_adapter_thread_state_set(rats);
    plat_assert(!old);
}

static void
ra_sched_pts_free(void *pts) {
    struct sdf_replicator_adapter_thread_state *rats =
        (struct sdf_replicator_adapter_thread_state *)pts;

    if (rats) {
        plat_free(rats);
    }
}

/** @brief Allocate one peer's structures */
static struct ra_peer_state *
ra_peer_alloc(struct sdf_replicator_adapter *parent, vnode_t node) {
    struct ra_peer_state *ret;
    int failed;

    /*
     * XXX: drew 2008-11-12
     *
     * We may want individual queues to prevent deadlock when
     * one remote peer is down; but it would be better to fix
     * messaging.
     *
     * tomr 2008-11-20
     * Create the remote flash service from a response service. When the message
     * is sent, the receiver sees it as a response and will post it as such.
     */
    failed = !plat_calloc_struct(&ret);
    if (!failed) {
        ret->parent = parent;
        ret->inbound =
            ra_queue_pair_alloc(parent->config.my_node /* src */,
                                parent->config.response_service /* src */,
                                node  /* dest */,
                                SDF_FLSH /* parent->config.replication_service */ /* dest */);
        failed = !ret->inbound;
    }
#ifdef notyet
    if (!failed) {
        ret->outbound =
            ra_queue_pair_alloc(parent->config.my_node /* src */,
                                parent->config.replication_service /* src */,
                                node /* dest */,
                                parent->config.response_service /* dest */);
        failed = !ret->outbound;
    }
#endif
    if (failed && ret) {
        ra_peer_free(ret);
        ret = NULL;
    }

    return (ret);
}

/** @brief Free one peer's structures */
static void
ra_peer_free(struct ra_peer_state *peer) {
    if (peer) {
        if (peer->inbound) {
            sdf_delete_queue_pair(peer->inbound);
        }
        if (peer->outbound) {
            sdf_delete_queue_pair(peer->outbound);
        }
        plat_free(peer);
    }
}

/** @brief Allocate queue pair and spew on failure */
static struct sdf_queue_pair *
ra_queue_pair_alloc(vnode_t src_vnode, service_t src_service,
                    vnode_t dest_vnode, service_t dest_service) {
    struct sdf_queue_pair *ret;

    ret = sdf_create_queue_pair(src_vnode, dest_vnode, src_service,
                                dest_service, SDF_WAIT_FTH);
    if (!ret) {
        plat_log_msg(21472, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "failed to allocate queue pair local %x:%x remote %x:%x",
                     src_vnode, src_service, dest_vnode, dest_service);
    }

    return (ret);
}
