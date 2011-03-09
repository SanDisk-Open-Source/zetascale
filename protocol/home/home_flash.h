#ifndef HOME_FLASH_H
#define HOME_FLASH_H 1

/*
 * File:   $HeadURL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/protocol/home/home_flash.h $
 * Author: drew
 *
 * Created on May 19, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: home_flash.h 9244 2009-09-15 11:24:15Z jmoilanen $
 */

/**
 * Home node flash protocol code, processing messages of external and
 * internal origins.
 *
 * FIXME: The flash hardware is event driven, so providing a native
 * message based interface for the flash would eliminate unecessary
 * abstraction layers that need to be maintained.
 *
 * This will also mean moving the (blocking) cmc calls out and relying on
 * meta-data populated elsewhere; for instance shard numbers should be
 * passed down instead of this relying on cmc lookups.  That in turn
 * will limit the potential for deadlock situations where a home
 * node thread blocks on a flash call which in turn makes a CMC call
 * that translates into a home node thread call...
 */

#include "platform/closure.h"
#include "platform/defs.h"

#include "sdfmsg/sdf_msg.h"
#include "common/sdftypes.h"
#include "protocol/protocol_common.h"

struct SDF_action_state;
struct flashDev;

PLAT_CLOSURE(home_flash_shutdown);

/**
 * @brief Home protocol flash protocol initialization
 *
 * User fills in all fields; with the #pfs field provided for symetry
 * with the action and home protocol configuration.
 */
typedef struct SDF_flash_init {
    /** Protocol state  */
    struct SDF_flash_state *pfs;

    /**
     * @brief  Number of threads to start
     *
     * XXX: This probably goes away.  The replication code is closure based
     * without a concept of threads, but currently has some wrapper threads
     * around name_service calls which are blocking since we do not have
     * a composable asynchronous interface there.
     */
    int nthreads;

    /** @brief my node number */
    uint32_t my_node;

    /** @brief Total node count */
    uint32_t nnodes;

    /** @brief Port to use for flash service (flash end) */
    service_t flash_server_service;

    /** @brief Port to use for flash service (client end) */
    service_t flash_client_service;

#ifdef MULTIPLE_FLASH_DEV_ENABLED
    struct flashDev **flash_dev;
#else
    struct flashDev *flash_dev;
#endif
    uint32_t flash_dev_count;
} SDF_flash_init_t;

__BEGIN_DECLS

/**
 * @brief Initialize flash protocol state
 * @param pfs <OUT> flash state
 * @param pfi <IN> flash initialization parameters.
 * @param pai <IN> action initialization structure referencing
 * constructed
 */
struct SDF_flash_state *
home_flash_alloc(struct SDF_flash_init *pfi, struct SDF_action_state *pas);

/**
 * @brief Shutdown flash protocol asynchronously
 *
 * @param pfs <IN> flash protocol common state
 * @param shutdown_closure <IN> invoked when shutdown is complete
 */
void home_flash_shutdown(struct SDF_flash_state *pfs,
                         home_flash_shutdown_t shutdown_closure);

/**
 * @brief Start processing.
 *
 * @param pfs <IN> flash protocol common state
 * @return 0 on success, non-zero on failure
 */
int home_flash_start(struct SDF_flash_state *pfs);

/**
 * @brief Convert between sdf/shard/shard.h and protocol messages
 *
 * This provides a common (one hopes correct) path to flash for
 * sdf/protocol/home/home_thread.c and the replication code.
 *
 * @param flash_dev <IN> flash device state
 *
 * @param pai <IN> context, which is really a SDF_action_init_t
 *
 * @param recv_msg <IN> the original (AH) message which must be of
 * a flash payload type. ZDUMY to use the original message value.
 *
 * @param type_replacement <IN> the renamed type of this message to encapsulate
 * flash and action->home protocols which allows the original message to be
 * unchanged.
 *
 * @param dest_node <IN> the destination node of the output message
 *
 * @param pmsize <OUT> when non-null *pmsize = protocol message size
 *
 * @return A response message which shall be released with sdf_msg_free
 * and does not yet have envelope information initialized.  The
 * SDF_protocol_msg_t payload has the original message's payload
 * from vnode and specified destination vnode.
 */
struct sdf_msg *
home_flash_wrapper(
#ifdef MULTIPLE_FLASH_DEV_ENABLED
                   struct flashDev **in_flash_dev,
#else
                   struct flashDev *in_flash_dev,
#endif
    uint32_t         flash_dev_count,
    SDF_internal_ctxt_t *pai,
    struct sdf_msg *recv_msg,
    SDF_protocol_msg_type_t type_replacement,
    vnode_t dest_node,
    SDF_size_t *pmsize);

/**
 * @brief Convert between sdf/shard/shard.h and protocol shard messages
 *
 * This provides a common (one hopes correct) path to flash for
 * sdf/protocol/home/home_thread.c and the replication code.
 *
 * XXX: drew 2008-10-30 This is only separate from the home_flash_wrapper
 * for historical reasons.  The two functions should probably merge.
 *
 * @param flash_dev <IN> flash device state
 *
 * @param recv_msg <IN> the original (AH) message which must be of
 * a flash payload type. ZDUMY to use the original message value.
 *
 * @param pmsize <OUT> when non-null *pmsize = protocol message size
 *
 * @return A response message which shall be released with sdf_msg_free
 * and does not yet have envelope information initialized.  The
 * SDF_protocol_msg_t payload has the original message's payload
 * from vnode and specified destination vnode.
 */
struct sdf_msg *
home_flash_shard_wrapper(
#ifdef MULTIPLE_FLASH_DEV_ENABLED
    struct flashDev **flash_dev,
#else
    struct flashDev *flash_dev,
#endif
    uint32_t flash_dev_count,
    SDF_internal_ctxt_t *pai_in,
    struct sdf_msg *recv_msg,
    SDF_size_t *pmsize);

/**
 * @brief Return the expected response type
 *
 * FIXME: Currently, each request demands a unique response type.  This
 * entangles generic routing layer (timeout, replication) implementation
 * with the coherency protocol implementation.
 *
 * @param type <IN> protocol message type.  Passing a non-HF message
 * in shall have undefined results.
 * @param status <IN> return status.
 *
 */
SDF_protocol_msg_type_t
home_flash_response_type(SDF_protocol_msg_type_t type, SDF_status_t status);

SDF_time_t (*memcached_get_current_time) (void);
SDF_time_t sdf_get_current_time(void);

__END_DECLS

#endif /* ndef HOME_FLASH_H */
