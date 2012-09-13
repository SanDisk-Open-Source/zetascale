/*
 * File:   sdfclient/object_container.h
 * Author: Darryl Ouye
 *
 * Created on March 2, 2008, 5:34 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: object_container.h 1805 2008-06-25 23:35:30Z darryl $
 */

#ifndef _OBJECT_CONTAINER_H
#define _OBJECT_CONTAINER_H

#ifdef __cplusplus
extern "C" {
#endif

// #define _SDF_PROCSESING_INLINE
#include "common/sdftypes.h"
#include "common/sdfstats.h"
#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif /* SDFAPI */

/**
 * @brief Create an object container.
 *
 * @param path <IN> The directory path of the container. Similar to a file directory path.
 * @param properties <IN> Container properties.
 * @return SDF status.
 */
SDF_status_t
SDFCreateObjectContainer(const char *path, SDF_container_props_t properties);

/**
 * @brief Delete an object container.
 *
 * Delete an object container. This call may require the right permissions to delete the container.
 * If the container has dirty data, the results of ongoing requests is undefined.
 *
 * @param path <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @return SDF_SUCCSES on success
 */
SDF_status_t
SDFDeleteObjectContainer(const char *path);

/**
 * @brief Open an object container.
 *
 * @param path <IN> The directory path of the container. Similar to a file directory path.
 * @param mode <IN> The mode to open the container in (e.g. read/write/append/read-write).
 * @param container <OUT> pointer to the container handle
 * @see SDF_container_mode_t
 * @return SDF_SUCCSES on success
 */
SDF_status_t
SDFOpenObjectContainer(const char *path, SDF_container_mode_t mode, SDFContainer *container);

/**
 * @brief Close an object container.
 *
 * @param container <IN> The container handle;
 * @return SDF status.
 */
SDF_status_t
SDFCloseObjectContainer(SDFContainer container);

/**
 * @brief Get the list of container objects.
 *
 * @param ctxt <IN> <NULLABLE> context.
 * @param container <IN> Pointer to the container handle.
 * @param info <OUT> Returned object information list.
 * @param sort <IN> Sorting order
 * @return SDF status.
 */
SDF_status_t
SDFEnumerateObjectContainer(SDF_context_t ctxt, SDFContainer container, SDF_object_info_t ***info, SDF_enumeration_order_t sort);

/**
 * @brief Initiate a container enumeration session.
 *
 * @param ctxt <IN> <NULLABLE> context.
 * @param container <IN> Pointer to the container handle.
 * @param sort <IN> Sorting order
 * @return SDF status.
 */
SDF_status_t
SDFStartObjectIteration(SDF_context_t ctxt, SDFContainer container, SDF_enumeration_order_t sort);

/**
 * @brief Get the results of a container enumeration.
 *
 * @param ctxt <IN> <NULLABLE> context.
 * @param container <IN> Pointer to the container handle.
 * @param key <Out> Object key.
 * @return SDF status.
 */
SDF_status_t
SDFNextObject(SDF_context_t ctxt, SDFContainer container, char **key);

/**
 * @brief End a container enumeration session.
 *
 * @param ctxt <IN> <NULLABLE> context.
 * @param container <IN> Pointer to the container handle.
 * @return SDF status.
 */
SDF_status_t
SDFCompleteObjectIteration(SDF_context_t ctxt, SDFContainer container);

/**
 * @brief Test the existence of a container.
 *
 * @param path <IN> The directory path of the container. Similar to a file directory path.
 * @return SDF status. 
 */
SDF_status_t
SDFContainerExists(const char *path);

#ifndef SDFAPI
/**
 * @brief Get container statistics.
 *
 * @param container <IN> Pointer to the container handle.
 * @param code <IN> The container statistic to retrieve.
 * @param stat <OUT> The returned container statistic.
 * @return SDF status. 
 */
SDF_status_t
SDFGetContainerStats(SDFContainer container, int key, uint64_t *stat);
#endif /* SDFAPI */

#ifdef __cplusplus
}
#endif

#endif /* _OBJECT_CONTAINER_H */
