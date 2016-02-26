/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdfclient/block_container.h
 * Author: Darpan Dinker
 *
 * Created on February 2, 2008, 5:34 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: block_container.h 3981 2008-10-23 01:18:50Z darpan $
 */

#ifndef _BLOCK_CONTAINER_H
#define _BLOCK_CONTAINER_H

#ifdef __cplusplus
extern "C" {
#endif

// #define _SDF_PROCESSING_INLINE

/**
 * @brief Create a block container.
 *
 * Create a block container. Block containers have the storage entity as a
 * block. Each block has a fixed size that is determined at the creation of the
 * container. Block size is a multiple of 2.
 *
 * @param path <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @param blockSize <IN> fixed size of each block in the container.
 * @param properties <IN> container properties
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFCreateBlockContainer(const char *path, unsigned blockSize, SDF_container_props_t properties);

/**
 * @brief Delete a block container.
 *
 * Delete a block container. This call may require the right permissions to delete the container.
 * If the container has dirty data, the results of ongoing requests is undefined.
 *
 * @param path <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @return SDF_SUCCSES on success
 */
SDF_status_t SDFDeleteBlockContainer(const char *path);

/**
 * @brief Open a block container.
 *
 * Checks are made to ensure that the container is a block container and that
 * the caller has the right permissions to open it in the provided mode.
 *
 * @param path <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @param mode <IN> mode to open the container in (e.g. read/write/append/read-write)
 * @param container <OUT> pointer to the container handle
 * @see SDF_container_mode_t
 * @return SDF_SUCCSES on success
 */
 SDF_status_t SDFOpenBlockContainer(const char *path, SDF_container_mode_t mode, SDFContainer *container);

/**
 * @brief Close a block container.
 *
 * Close a block container.
 *
 * @param container <IN> handle to the container
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFCloseBlockContainer(SDFContainer container);

/**
 * @brief Lock a container.
 *
 * Lock an entire container in specified lock mode. This acts as a multi-
 * granularity operation and does NOT lock all the blocks in the container.
 *
 * @param txid <IN> <NULLABLE> associated transactionID if in an SDF transaction
 * @param container <IN> pointer to the container handle
 * @param lockRequested <IN> desired state of the container's lock
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFLockContainer(SDF_tx_id txid, SDFContainer container, SDF_lock_type_t lockRequested);

/**
 * @brief Lock a container in non-blocking fashion.
 *
 * Lock an entire container in specified lock mode. This acts as a multi-
 * granularity operation and does NOT lock all the blocks in the container.
 * <BR><B>Non-blocking operation</B>
 *
 * @param txid <IN> <NULLABLE> associated transactionID if in an SDF transaction
 * @param container <IN> pointer to the container handle
 * @param lockRequested <IN> desired state of the container's lock
 * @param opid <OUT> pointer to the completion structure for this operation
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFLockContainer_NB(SDF_tx_id txid, SDFContainer container, SDF_lock_type_t lockRequested,
                                 SDF_opid_t *opid);

/**
 * @brief Unlock a container.
 *
 * Unlock an entire container. This is a multi-granularity lock operation and
 * does NOT unlock all the blocks within the container.
 *
 * @param txid <IN> <NULLABLE> associated transactionID if in an SDF transaction
 * @param container <IN> pointer to the container handle
 * @param lock <IN> desired state of the container's lock
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFUnlockContainer(SDF_tx_id txid, SDFContainer container);

/**
 * @brief Unlock a container in non-blocking fashion.
 *
 * Unlock an entire container. This is a multi-granularity lock operation and
 * does NOT unlock all the blocks within the container.
 * <BR><B>Non-blocking operation</B>
 *
 * @param txid <IN> <NULLABLE> associated transactionID if in an SDF transaction
 * @param container <IN> pointer to the container handle
 * @param lock <IN> desired state of the container's lock
 * @param opid <OUT> pointer to the completion structure for this operation
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFUnlockContainer_NB(SDF_tx_id txid, SDFContainer container, SDF_opid_t *opid);

/**
 * @brief Get the results of a block container enumeration.
 *
 * @param ctxt <IN> <NULLABLE> context.
 * @param container <IN> Pointer to the container handle.
 * @param id <Out> Block id.
 * @return SDF status.
 */
SDF_status_t
SDFNextBlock(SDF_context_t ctxt, SDFContainer container, uint64_t *id);

SDF_status_t
SDFPerfMessage();

SDF_status_t
SDFStatsMessage();

#ifdef __cplusplus
}
#endif

#endif /* _BLOCK_CONTAINER_H */
