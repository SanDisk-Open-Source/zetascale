/*
 * File:   name_service.h
 * Author: DO
 *
 * Created on January 15, 2008, 2:46 PM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: name_service.h 12335 2010-03-18 20:56:46Z briano $
 *
 */

#ifndef _NAME_SERVICE_H
#define _NAME_SERVICE_H 1

#include "platform/string.h"
#include "container_props.h"
#include "common/sdftypes.h"
#include "container_meta.h"
#include "container.h"

__BEGIN_DECLS


// ==========================================================

/**
 * @brief Create the container metadata object for the named container.
 *
 * @param cname <IN> Container name.
 * @param cguid <IN> Container guid.
 * @param meta <IN> Pointer to the metadata object.
 * @return SDF status.
 */
SDF_status_t
name_service_create_meta(SDF_internal_ctxt_t *pai, const char *cname, SDF_cguid_t cguid,
			 SDF_container_meta_t *meta);

/**
 * @brief Get the container metadata object for the named container.
 *
 * @param cguid <IN> Container guid.
 * @param meta <OUT> Pointer to a user-supplied meta data buffer.
 * @return SDF status.
 */
SDF_status_t
name_service_get_meta(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, SDF_container_meta_t *meta);

/**
 * @brief Get the metadata for the named container.
 *
 * @param cname <IN> Container name.
 * @param meta <OUT> Pointer to a user-supplied meta data buffer.
 * @return SDF status.
 */
SDF_status_t
name_service_get_meta_from_cname(SDF_internal_ctxt_t *pai, const char *cname,
				 SDF_container_meta_t *meta);

/**
 * @brief Set the container metadata object for the named container.
 *
 * @param cguid <IN> Container guid.
 * @param meta <IN> Pointer to the metadata object.
 * @return SDF status.
 */
SDF_status_t
name_service_put_meta(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, SDF_container_meta_t *meta);

/**
 * @brief Remove the container metadata object for the named container.
 *
 * @param cname <IN> Container name.
 * @return SDF status.
 */
SDF_status_t
name_service_remove_meta(SDF_internal_ctxt_t *pai, const char *cname);

/**
 * @brief Lock the container metadata.
 *
 * @param cname <IN> Container name.
 * @return SDF status.
 */
SDF_status_t
name_service_lock_meta(SDF_internal_ctxt_t *pai, const char *cname);

/**
 * @brief Unlock the container metadata.
 *
 * @param cname <IN> Container name.
 * @return SDF status.
 */
SDF_status_t
name_service_unlock_meta(SDF_internal_ctxt_t *pai, const char *cname);

/**
 * @brief Lock the container metadata.
 *
 * @param cguid <IN> Container cguid.
 * @return SDF status.
 */
SDF_status_t
name_service_lock_meta_by_cguid(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid);

/**
 * @brief Unlock the container metadata.
 *
 * @param cguid <IN> Container cguid.
 * @return SDF status.
 */
SDF_status_t
name_service_unlock_meta_by_cguid(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid);

/**
 * @brief Test for container metadata existence (status == SDF_OBJECT_EXISTS).
 *
 * @param cname <IN> Container name.
 * @return SDF status.
 */
SDF_status_t
name_service_meta_exists(SDF_internal_ctxt_t *pai, const char *cname);

/**
 * @brief Test for cguid existence (status == SDF_OBJECT_EXISTS).
 *
 * @param cguid <IN> Container cguid.
 * @return SDF status.
 */
SDF_status_t
name_service_cguid_exists(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid);

/**
 * @brief Get the container properties object for the named container.
 *
 * @param cname <IN> Container name.
 * @param props <OUT> Pointer to the container properties object.
 * @return SDF status.
 */
SDF_status_t
name_service_get_props(SDF_internal_ctxt_t *pai, const char *cname, SDF_container_props_t *props);

/**
 * @brief Set the container properties object for the named container.
 *
 * @param cname <IN> Container name.
 * @param props <IN> Pointer to the container properties object.
 * @return SDF status.
 */
SDF_status_t
name_service_put_props(SDF_internal_ctxt_t *pai, const char *cname, SDF_container_props_t props);


/**
 * @brief Get the container shard mapping table object for the named container.
 *
 * @param cguid <IN> Container guid.
 * @param objkey <IN> Object key,
 * @return Shard, #SDF_SHARDID_INVALID on failure
 */
SDF_shardid_t
name_service_get_shard(SDF_cguid_t cguid, const char *objkey,
                       SDF_shardid_t first_shard, uint32_t shard_count);
                       
/**
 * @brief Set the container shard mapping table object for the named container.
 *
 * @param cguid <IN> Container guid.
 * @param shard <IN> Shard id
 * @return SDF status.
 */
SDF_status_t
name_service_put_shard(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, SDF_shardid_t shard);

/**
 * @brief Delete all of a container's shards.
 *
 * @param cname <IN> Container name.
 * @return SDF status.
 */
SDF_status_t
name_service_delete_shards(SDF_internal_ctxt_t *pai, const char *cname);

/**
 * @brief Get the cguid associated with a container name.
 *
 * @param cname <IN> Container name.
 * @return SDF status.
 */
SDF_status_t
name_service_get_cguid(SDF_internal_ctxt_t *pai, const char *cname, SDF_cguid_t *cguid);

/**
 * @brief Get the home node for the container guid + object tuple.
 *
 * @param cguid <IN> Container guid.
 * @param objkey <IN> Object key.
 * @param node <OUT> Returned value of the node.
 * @return SDF status.
 */
SDF_status_t
name_service_get_home_node(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, SDF_simple_key_t *pkey, uint32_t *node);

/**
 * @brief Flush and invalidate a container.
 *
 * @param cguid <IN> Container name.
 * @return SDF status.
 */
SDF_status_t
name_service_flush_inval_object_container(SDF_internal_ctxt_t *pai, const char *cname);

/**
 * @brief Invalidate a container.
 *
 * @param cguid <IN> Container name.
 * @return SDF status.
 */
SDF_status_t
name_service_inval_object_container(SDF_internal_ctxt_t *pai, const char *cname);

__END_DECLS


#endif  /* _NAME_SERVICE_H */
