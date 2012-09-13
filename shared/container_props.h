/*
 * File:   container_props.h
 * Author: DO
 *
 * Created on January 15, 2008, 2:46 PM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: container_props.h 9460 2009-09-27 18:27:51Z briano $
 */
#ifndef _CONTAINER_PROPS_H
#define _CONTAINER_PROPS_H

#ifdef __cplusplus
extern "C" {
#endif


#include "common/sdftypes.h"
#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif /* SDFAPI */

__BEGIN_DECLS

/*
 * Defines for accessing container properties fields
 */

// Container ID
#define SDF_CONTAINER_NAME                              0
#define SDF_CONTAINER_ID                                1
#define SDF_CONTAINER_OWNER                             2
#define SDF_CONTAINER_SIZE                              3
// Container Type
#define SDF_CONTAINER_TYPE                              4
#define SDF_CONTAINER_PERSISTENT                        5
// Storage Hierarchy
#define SDF_CONTAINER_STORAGE_LEVEL                     6
#define SDF_CONTAINER_STORAGE_DISTRIBUTION              7
// Storage Replication
#define SDF_CONTAINER_REPLICATION_ENABLED               8
#define SDF_CONTAINER_NUM_REPLICAS                      9
#define SDF_CONTAINER_SHARD_PLACEMENT                   10
#define SDF_CONTAINER_REPLICATION_SIZE                  11
#define SDF_CONTAINER_SYNCHRONOUS_REPLICATION           12
// Quota
#define SDF_CONTAINER_QUOTA_ENABLED                     13
#define SDF_CONTAINER_QUOTA_SIZE                        14
// Compression
#define SDF_CONTAINER_COMPRESSION_ENABLED               15
#define SDF_CONTAINER_COMPRESSION_TYPE                  16
#define SDF_CONTAINER_COMPRESSION_SIZE                  17
// Encryption
#define SDF_CONTAINER_ENCRYPTION_ENABLED                18
#define SDF_CONTAINER_ENCRYPTION_TYPE                   19
// Migration
#define SDF_CONTAINER_MIGRATION_ENABLED                 20
#define SDF_CONTAINER_MIGRATION_POLICY                  21
// Flush
#define SDF_CONTAINER_FLUSH_TYPE                        22
// Cache
#define SDF_CONTAINER_CACHING_ENABLED                   23
#define SDF_CONTAINER_CACHE_SIZE                        24
#define SDF_CONTAINER_MAX_CACHE_SIZE                    25
// Transaction
#define SDF_CONTAINER_TRANSACTIONS_ENABLED              26
#define SDF_CONTAINER_TRANSACTION_TYPE                  27
#define SDF_CONTAINER_TRANSACTION_LOG                   28
// Access Control
#define SDF_CONTAINER_ACCESS_CONTROL_ENABLED            29
#define SDF_CONTAINER_ACCESS_TYPE                       30
#define SDF_CONTAINER_ACCESS_PERMISSIONS                31
// Access Hints
#define SDF_CONTAINER_ACCESS_PATTERN                    32
#define SDF_CONTAINER_ACCESS_SIZE                       33
// Conflict Detection
#define SDF_CONTAINER_CONFLICT_DETECTION_ENABLED        34
#define SDF_CONTAINER_CONFLICT_BOUNDARY                 35
// Attributes
#define SDF_CONTAINER_LAST_UPDATE                       36
#define SDF_CONTAINER_EXTENDED_ATTRIBUTES               37
#define SDF_CONTAINER_INHERIT_PROPERTIES                38
// Debug
#define SDF_CONTAINER_DEBUG_ENABLED                     39
#define SDF_CONTAINER_DEBUG_LEVEL                       40
#define SDF_CONTAINER_DEBUG_LOG_ENABLED                 41
// Specific container type attributes
#define SDF_BLOCK_CONTAINER_BLOCKSIZE                   42

// ==============================================================
#if 0
/*
 * Create a container properties object.
 *
 * @return Pointer to the properties object.
 */
SDF_container_props_t *
container_props_create();

/*
 * Destroy a container properties object.
 *
 * @param Pointer to the container properties object.
 * @return SDF_status_t
 */
SDF_status_t
container_props_destroy(SDF_container_props_t *props);

/*
 * Get a value object from a container properties object.
 *
 * The returned value structure is a union type that holds the requested
 * property value. The value is generally a multi-part structure,
 * each part containing one property value.
 *
 * @param Container properties object.
 * @param Container properties field id.
 * @return The requested container properties value object.
 */
SDF_container_props_value_t
container_props_get_value(SDF_container_props_t *props, uint16_t property);

/*
 * Set a value within a container properties object.
 *
 * The value object is a union type that holds the property value to be set.
 * The property value is generally a multi-part structure,
 * each part containing one property value.
 *
 * @param Container properties object.
 * @param Container properties field id.
 * @param Property values object which contains the value to be set.
 * @return SDF_status_t
 */
SDF_status_t
container_props_set_value(SDF_container_props_t *props, uint16_t property, SDF_container_props_value_t value);


#endif
/*
 * Copy container properties.
 *
 * @param Container properties object.
 * @return Copy of the input properties object.
 */
SDF_container_props_t *
container_props_copy(SDF_container_props_t props);

__END_DECLS


#ifdef __cplusplus
}
#endif

#endif /* _CONTAINER_PROPS_H */
