/*
 * File:   init_sdf.h
 * Author: DO
 *
 * Created on January 15, 2008, 2:46 PM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: init_sdf.h 8330 2009-07-09 00:38:37Z johann $
 *
 */

#ifndef _INIT_SDF_H
#define _INIT_SDF_H

/* XXX: service_t should come from sdftypes.h instead */
#include "sdfmsg/sdf_msg.h"
#include "protocol/replication/replicator.h"
#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif /* SDFAPI */

struct SDF_action_init;

/*
 * XXX: A lot of this needs to become part of a common agent configuration
 * structure since it's replicated across subsystems.
 *
 * This is probably also mis-named
 */
struct SDF_config {
    /** @brief Context for initialization code */
    struct SDF_action_init *pai;

    /**
     * @brief Object container size
     *
     * XXX: This was a global in the code when I found it but needs to become
     * a container property.
     */
    int num_objs;

    /**
     * @brief Container cguid counter.
     *
     * Used to generate unique container ids. Persistent (updated to last used value on recovery).
     *
     */
    uint16_t cguid_counter;

    /**
     * @brief Recovery flag
     *
     * Tells SDF how to initialize (from scratch, or recover flash in some form).
     * 
     */
    int system_recovery;
    
    /**
     * @brief authoritative flag
     *
     * Tells SDF to start the instance as authoritative instance for persistent mode containers
     * 
     */
    int auth_mode;


    /** @brief my node number (rank) */
    uint32_t my_node;

    /** @brief number of nodes */
    int nnodes;

    /** @brief node used for the meta-data supernode on container create */
    uint32_t super_node;

    /**
     * @brief in the multiple device world, it's 
     * a pointer to array of  device structs
     */
#ifdef MULTIPLE_FLASH_DEV_ENABLED    
    struct flashDev **flash_dev;
#else
    struct flashDev *flash_dev;
#endif
    uint32_t flash_dev_count;
    
     /**
      * @brief number of shards per container. This 
      * can be overriden by setting the number in the
      * container properties before calling the SDFCreateContainer
      * call
      */
    uint32_t shard_count;

    /** @brief Use message interface to flash in non-replicated case */
    int flash_msg;

    /** @brief number of ways to replicate containers with default properties */
    int always_replicate;

    /** @brief Default replication type */
    SDF_replication_t replication_type;

    /** @brief Requests from other replication services */
    service_t replication_service;

    /** @brief Requests for persistence */
    service_t flash_service;

    /**
     * @brief Responses for all services.  With wildcard messaging this 
     * is now more for differentiating where messages come from.
     */
    service_t response_service;

    /** @brief Replicator instance */
    struct sdf_replicator *replicator;
};

#ifdef  __cplusplus
extern "C" {
#endif

#include "fth/fthMbox.h"
#include "common/sdftypes.h"
#include "container_meta.h"

__BEGIN_DECLS

/**
 * Provide required configuration with defaults
 * @param num_objs <IN> Flash capacity.
 * @param rank <IN> Node id.
 * @param flash_dev <IN> Flash device struct.
 * @param repllicator <IN> Replicator handle.
 */
void
init_sdf_initialize_config(struct SDF_config *config,
                           SDF_internal_ctxt_t *pai,
                           int num_objs,
                           int system_recovery,
                           unsigned rank,
#ifdef MULTIPLE_FLASH_DEV_ENABLED
                           struct flashDev *flash_dev[],
#else
                           struct flashDev *flash_dev,
#endif                           
                           uint32_t flash_dev_count,
                           uint32_t shard_count,
			   struct sdf_replicator *replicator);

/**
 * @brief Create and initialize resource needed by SDF, e.g. CMC
 * @param config <IN> Pointer to intization structure which remains
 * owned by the caller.
 * @return SDF_SUCCESS on success
 */
SDF_status_t
init_sdf_initialize(const struct SDF_config *config, int restart);

/**
 * @brief Free up resources held by SDF, e.g. CMC
 */
SDF_status_t
init_sdf_reset(SDF_internal_ctxt_t *pai);

/**
 * @brief Initialize the container metadata container (CMC).
 *
 * @param meta <IN> CMC metadata.
 * @return Pointer to the CMC object.
 */
void
init_cmc(SDF_container_meta_t *meta);

/**
 * @brief Return the node identifier.
 *
 * @return This node's identifier.
 */
uint32_t
init_get_my_node_id();

/**
 * @brief Initialize the node's cguid counter.
 *
 */
void
init_set_cguid_counter(SDF_cguid_t cguid_counter);

extern int 
(*init_container_meta_blob_put)( uint64_t shard_id, char * data, int len );

extern int 
(*init_container_meta_blob_get)( char * blobs[], int num_slots );

__END_DECLS

#ifdef  __cplusplus
}
#endif

#endif  /* _INIT_SDF_H */
