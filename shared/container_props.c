/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   container_props.c
 * Author: DO
 *
 * Created on January 15, 2008, 10:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: container_props.c 9460 2009-09-27 18:27:51Z briano $
 */
#include <stdio.h>
#include "platform/stdlib.h"
#include "platform/string.h"
#include "container_props.h"


// ========================================================
#if 0
SDF_container_props_t
*container_props_create() {

    SDF_container_props_t *props = NULL;

    props = (SDF_container_props_t *)
        plat_alloc(sizeof (SDF_container_props_t));
    return (props);
}

SDF_status_t
container_props_destroy(SDF_container_props_t *props) {

    SDF_status_t status = SDF_SUCCESS;

    if (props != NULL) {
        plat_free(props);
    }
    return (status);
}

SDF_container_props_value_t
container_props_get_value(SDF_container_props_t *props, uint16_t property) {

    SDF_container_props_value_t value;

    if (props != NULL) {

        switch (property) {

            // case SDF_CONTAINER_NAME:
            // strcpy(value->container_id.name, props->container_id.name);
            // break;

        case SDF_CONTAINER_ID:
            value.container_id.id = props->container_id.id;
            break;

        case SDF_CONTAINER_OWNER:
            value.container_id.owner = props->container_id.owner;
            break;

        case SDF_CONTAINER_SIZE:
            value.container_id.size = props->container_id.size;
            break;

        case SDF_CONTAINER_TYPE:
            value.container_type.type = props->container_type.type;
            break;

        case SDF_CONTAINER_PERSISTENT:
            value.container_type.persistent = props->container_type.persistent;
            break;

        case SDF_CONTAINER_STORAGE_LEVEL:
            value.hierarchy.level = props->hierarchy.level;
            break;

        case SDF_CONTAINER_STORAGE_DISTRIBUTION:
            value.hierarchy.distribution = props->hierarchy.distribution;
            break;

        case SDF_CONTAINER_REPLICATION_ENABLED:
            value.replication.enabled = props->replication.enabled;
            break;

        case SDF_CONTAINER_REPLICATION_SIZE:
            value.replication.size = props->replication.size;
            break;

        case SDF_CONTAINER_NUM_REPLICAS:
            value.replication.num_replicas = props->replication.num_replicas;
            break;

        case SDF_CONTAINER_SHARD_PLACEMENT:
            value.replication.shard_placement =
                props->replication.shard_placement;
            break;

        case SDF_CONTAINER_SYNCHRONOUS_REPLICATION:
            value.replication.synchronous = props->replication.synchronous;
            break;

        case SDF_CONTAINER_QUOTA_ENABLED:
            value.quota.enabled = props->quota.enabled;
            break;

        case SDF_CONTAINER_QUOTA_SIZE:
            value.quota.size = props->quota.size;
            break;

        case SDF_CONTAINER_COMPRESSION_ENABLED:
            value.compression.enabled = props->compression.enabled;
            break;

        case SDF_CONTAINER_COMPRESSION_TYPE:
            value.compression.type = props->compression.type;
            break;

        case SDF_CONTAINER_COMPRESSION_SIZE:
            value.compression.size = props->compression.size;
            break;

        case SDF_CONTAINER_ENCRYPTION_ENABLED:
            value.encryption.enabled = props->encryption.enabled;
            break;

        case SDF_CONTAINER_ENCRYPTION_TYPE:
            value.encryption.type = props->encryption.type;
            break;

        case SDF_CONTAINER_MIGRATION_ENABLED:
            value.migration.enabled = props->migration.enabled;
            break;

        case SDF_CONTAINER_MIGRATION_POLICY:
            value.migration.policy = props->migration.policy;
            break;

        case SDF_CONTAINER_FLUSH_TYPE:
            value.flush.type = props->flush.type;
            break;

        case SDF_CONTAINER_CACHING_ENABLED:
            value.cache.enabled = props->cache.enabled;
            break;

        case SDF_CONTAINER_CACHE_SIZE:
            value.cache.size = props->cache.size;
            break;

        case SDF_CONTAINER_MAX_CACHE_SIZE:
            value.cache.max_size = props->cache.max_size;
            break;

        case SDF_CONTAINER_TRANSACTIONS_ENABLED:
            value.transaction.enabled = props->transaction.enabled;
            break;

        case SDF_CONTAINER_TRANSACTION_TYPE:
            value.transaction.type = props->transaction.type;
            break;

        case SDF_CONTAINER_TRANSACTION_LOG:
            value.transaction.log = props->transaction.log;
            break;

        case SDF_CONTAINER_ACCESS_CONTROL_ENABLED:
            value.access_control.enabled = props->access_control.enabled;
            break;

        case SDF_CONTAINER_ACCESS_TYPE:
            value.access_control.type = props->access_control.type;
            break;

        case SDF_CONTAINER_ACCESS_PERMISSIONS:
            value.access_control.permissions =
                props->access_control.permissions;
            break;

        case SDF_CONTAINER_ACCESS_PATTERN:
            value.access_hints.pattern = props->access_hints.pattern;
            break;

        case SDF_CONTAINER_ACCESS_SIZE:
            value.access_hints.size = props->access_hints.size;
            break;

        case SDF_CONTAINER_CONFLICT_DETECTION_ENABLED:
            value.conflict.enabled = props->conflict.enabled;
            break;

        case SDF_CONTAINER_CONFLICT_BOUNDARY:
            value.conflict.boundary = props->conflict.boundary;
            break;

        case SDF_CONTAINER_LAST_UPDATE:
            value.attributes.last_update = props->attributes.last_update;
            break;

        case SDF_CONTAINER_EXTENDED_ATTRIBUTES:
            value.attributes.extended_attributes =
                props->attributes.extended_attributes;
            break;

        case SDF_CONTAINER_INHERIT_PROPERTIES:
            value.attributes.inherit_properties =
                props->attributes.inherit_properties;
            break;

        case SDF_CONTAINER_DEBUG_ENABLED:
            value.debug.enabled = props->debug.enabled;
            break;

        case SDF_CONTAINER_DEBUG_LEVEL:
            value.debug.level = props->debug.level;
            break;

        case SDF_CONTAINER_DEBUG_LOG_ENABLED:
            value.debug.log_enabled = props->debug.log_enabled;
            break;

        case SDF_BLOCK_CONTAINER_BLOCKSIZE:
            value.specific.block_props.blockSize = props->specific.block_props.blockSize;
            break;

        default:
            break;
        }
    }
    return (value);
}

SDF_status_t
container_props_set_value(SDF_container_props_t *props, uint16_t property,
                          SDF_container_props_value_t value) {

    SDF_status_t status = SDF_SUCCESS;

    if (props != NULL) {
        switch (property) {

            // case SDF_CONTAINER_NAME:
            // strcpy(props->container_id.name, value.container_id.name);
            // break;

        case SDF_CONTAINER_ID:
            props->container_id.id = value.container_id.id;
            break;

        case SDF_CONTAINER_OWNER:
            props->container_id.owner = value.container_id.owner;
            break;

        case SDF_CONTAINER_SIZE:
            props->container_id.size = value.container_id.size;
            break;

        case SDF_CONTAINER_TYPE:
            props->container_type.type = value.container_type.type;
            break;

        case SDF_CONTAINER_PERSISTENT:
            props->container_type.persistent = value.container_type.persistent;
            break;

        case SDF_CONTAINER_STORAGE_LEVEL:
            props->hierarchy.level = value.hierarchy.level;
            break;

        case SDF_CONTAINER_STORAGE_DISTRIBUTION:
            props->hierarchy.distribution = value.hierarchy.distribution;
            break;

        case SDF_CONTAINER_QUOTA_ENABLED:
            props->quota.enabled = value.quota.enabled;
            break;

        case SDF_CONTAINER_QUOTA_SIZE:
            props->quota.size = value.quota.size;
            break;

        case SDF_CONTAINER_REPLICATION_ENABLED:
            props->replication.enabled = value.replication.enabled;
            break;

        case SDF_CONTAINER_REPLICATION_SIZE:
            props->replication.size = value.replication.size;
            break;

        case SDF_CONTAINER_NUM_REPLICAS:
            props->replication.num_replicas = value.replication.num_replicas;
            break;

        case SDF_CONTAINER_SHARD_PLACEMENT:
            props->replication.shard_placement =
                value.replication.shard_placement;
            break;

        case SDF_CONTAINER_SYNCHRONOUS_REPLICATION:
            props->replication.synchronous = value.replication.synchronous;
            break;

        case SDF_CONTAINER_COMPRESSION_ENABLED:
            props->compression.enabled = value.compression.enabled;
            break;

        case SDF_CONTAINER_COMPRESSION_TYPE:
            props->compression.type = value.compression.type;
            break;

        case SDF_CONTAINER_COMPRESSION_SIZE:
            props->compression.size = value.compression.size;
            break;

        case SDF_CONTAINER_ENCRYPTION_ENABLED:
            props->encryption.enabled = value.encryption.enabled;
            break;

        case SDF_CONTAINER_ENCRYPTION_TYPE:
            props->encryption.type = value.encryption.type;
            break;

        case SDF_CONTAINER_MIGRATION_ENABLED:
            props->migration.enabled = value.migration.enabled;
            break;

        case SDF_CONTAINER_MIGRATION_POLICY:
            props->migration.policy = value.migration.policy;
            break;

        case SDF_CONTAINER_FLUSH_TYPE:
            props->flush.type = value.flush.type;
            break;

        case SDF_CONTAINER_CACHING_ENABLED:
            props->cache.enabled = value.cache.enabled;
            break;

        case SDF_CONTAINER_CACHE_SIZE:
            props->cache.size = value.cache.size;
            break;

        case SDF_CONTAINER_MAX_CACHE_SIZE:
            props->cache.max_size = value.cache.max_size;
            break;

        case SDF_CONTAINER_TRANSACTIONS_ENABLED:
            props->transaction.enabled = value.transaction.enabled;
            break;

        case SDF_CONTAINER_TRANSACTION_TYPE:
            props->transaction.type = value.transaction.type;
            break;

        case SDF_CONTAINER_TRANSACTION_LOG:
            props->transaction.log = value.transaction.log;
            break;

        case SDF_CONTAINER_ACCESS_CONTROL_ENABLED:
            props->access_control.enabled = value.access_control.enabled;
            break;

        case SDF_CONTAINER_ACCESS_TYPE:
            props->access_control.type = value.access_control.type;
            break;

        case SDF_CONTAINER_ACCESS_PERMISSIONS:
            props->access_control.permissions =
                value.access_control.permissions;
            break;

        case SDF_CONTAINER_ACCESS_PATTERN:
            props->access_hints.pattern = value.access_hints.pattern;
            break;

        case SDF_CONTAINER_ACCESS_SIZE:
            props->access_hints.size = value.access_hints.size;
            break;

        case SDF_CONTAINER_CONFLICT_DETECTION_ENABLED:
            props->conflict.enabled = value.conflict.enabled;
            break;

        case SDF_CONTAINER_CONFLICT_BOUNDARY:
            props->conflict.boundary = value.conflict.boundary;
            break;

        case SDF_CONTAINER_LAST_UPDATE:
            props->attributes.last_update = value.attributes.last_update;
            break;

        case SDF_CONTAINER_EXTENDED_ATTRIBUTES:
            props->attributes.extended_attributes =
                value.attributes.extended_attributes;
            break;

        case SDF_CONTAINER_INHERIT_PROPERTIES:
            props->attributes.inherit_properties =
                value.attributes.inherit_properties;
            break;

        case SDF_CONTAINER_DEBUG_ENABLED:
            props->debug.enabled = value.debug.enabled;
            break;

        case SDF_CONTAINER_DEBUG_LEVEL:
            props->debug.level = value.debug.level;
            break;

        case SDF_CONTAINER_DEBUG_LOG_ENABLED:
            props->debug.log_enabled = value.debug.log_enabled;
            break;

        case SDF_BLOCK_CONTAINER_BLOCKSIZE:
            props->specific.block_props.blockSize = value.specific.block_props.blockSize;
            break;

        default:
            status = SDF_FAILURE;
        }
    }
    return (status);
}
#endif

SDF_container_props_t *
container_props_copy(SDF_container_props_t props) {

    SDF_container_props_t *p;

    p = NULL;

#if 0
    if ((p = container_props_create()) != NULL) {

        p->container_id.id = props.container_id.id;

        p->container_id.owner = props.container_id.owner;

        p->container_id.size = props.container_id.size;

        p->container_type.type = props.container_type.type;

        p->container_type.persistent = props.container_type.persistent;

        p->hierarchy.level = props.hierarchy.level;

        p->hierarchy.distribution = props.hierarchy.distribution;

        p->quota.enabled = props.quota.enabled;

        p->quota.size = props.quota.size;

        p->replication.enabled = props.replication.enabled;

        p->replication.size = props.replication.size;

        p->replication.num_replicas = props.replication.num_replicas;

        p->replication.shard_placement = props.replication.shard_placement;

        p->replication.synchronous = props.replication.synchronous;

        p->compression.enabled = props.compression.enabled;

        p->compression.type = props.compression.type;

        p->compression.size = props.compression.size;

        p->encryption.enabled = props.encryption.enabled;

        p->encryption.type = props.encryption.type;

        p->migration.enabled = props.migration.enabled;

        p->migration.policy = props.migration.policy;

        p->flush.type = props.flush.type;

        p->cache.enabled = props.cache.enabled;

        p->cache.size = props.cache.size;

        p->cache.max_size = props.cache.max_size;

        p->transaction.enabled = props.transaction.enabled;

        p->transaction.type = props.transaction.type;

        p->transaction.log = props.transaction.log;

        p->access_control.enabled = props.access_control.enabled;

        p->access_control.type = props.access_control.type;

        p->access_control.permissions = props.access_control.permissions;

        p->access_hints.pattern = props.access_hints.pattern;

        p->access_hints.size = props.access_hints.size;

        p->conflict.enabled = props.conflict.enabled;

        p->conflict.boundary = props.conflict.boundary;

        p->attributes.last_update = props.attributes.last_update;

        p->attributes.extended_attributes =
            props.attributes.extended_attributes;

        p->attributes.inherit_properties =
            props.attributes.inherit_properties;

        p->debug.enabled = props.debug.enabled;

        p->debug.level = props.debug.level;

        p->debug.log_enabled = props.debug.log_enabled;

        if (props.container_type.type == SDF_BLOCK_CONTAINER) {
            p->specific.block_props.blockSize = props.specific.block_props.blockSize;
        }
    }
#endif
    return (p);
}
