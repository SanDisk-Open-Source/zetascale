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

/**************************************************************************
 *   
 *   General Function Declarations (Non-blocking)
 *   
 **************************************************************************/
#ifndef INTERNAL_BLK_OBJ_API_H
#define INTERNAL_BLK_OBJ_API_H

extern int SDFSimpleReplicationEnabled();
extern SDF_status_t SDFAutoDelete(SDF_internal_ctxt_t *pai);
#ifndef SDFAPI
extern SDF_status_t SDFGetContainers(SDF_internal_ctxt_t *pai, struct mcd_container **pcontainers, int *pn_containers);
#else
extern SDF_status_t SDFGetContainersPtrs(SDF_internal_ctxt_t *pai, struct mcd_container **pcontainers, int *pn_containers);
#endif /* SDFAPI */

/**
 *  @brief Get a container statistic.
 *
 *  Get a container statistic.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param stat_Key <IN> Key of a container statistic.
 *  @param pstat <OUT> Pointer to variable in which to return the value of a container
 *  statistic.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePinnedBlockWithExpiry, SDFSetPinnedBlockWithExpiry, SDFGetForReadPinnedBlockWithExpiry,
 *  SDFGetForWritePinnedBlockWithExpiry, SDFUnpinBlock, SDFInvalidateContainerBlocks
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetContainerStats2_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint32_t stat_Key, uint64_t * pstat, SDF_opid_t * p_opid);



/**************************************************************************
 *   
 *   Object Function Declarations (Non-blocking)
 *   
 **************************************************************************/

/**
 *  @brief Create an object.
 *
 *  Creates an object of the specified size.  The new object has undefined
 *  contents.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFRemoveObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the object already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreateObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, SDF_opid_t * p_opid);

/**
 *  @brief Create an object with an initial value.
 *
 *  Creates an object of the specified size and initializes it from
 *  an application buffer.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFRemoveObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the object already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePutBufferedObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void * pbuf_out, SDF_opid_t * p_opid);

/**
 *  @brief Create an object with an initial value.
 *
 *  Creates an object of the specified size and initializes it from
 *  an application shmem buffer.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  @param pshbuf_out <IN> Shmem pointer to a data buffer from which to get write data (managed
 *  by the application).
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFRemoveObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the object already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePutShmemObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, SDFCacheObj_sp_t pshbuf_out, SDF_opid_t * p_opid);

/**
 *  @brief Create a pinned object of the specified size.
 *
 *  Creates a pinned object of the specified size and returns a pointer
 *  to the object. The initial contents of the object are undefined.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param sze <IN> Size of object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFSetPinnedObject, SDFRemoveObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the object already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePinnedObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t sze, SDF_opid_t * p_opid);

/**
 *  @brief Delete an object.
 *
 *  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreateObject, SDFCreatePutBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_RemoveObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of an object for read-only  access.
 *
 *  Get an object and copy it into an application-provided buffer.
 *  The application only intends to read the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param max_size <IN> Maximum number of bytes that can be put in the user buffer.
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see 
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadBufferedObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_in, SDF_size_t max_size, SDF_size_t * pactual_size, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of an object for read-only  access.
 *
 *  Get an object and copy it into an application-provided shmem buffer.
 *  The application only intends to read the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pshbf_in <OUT> Shmem pointer to a data buffer into which to copy read data (managed
 *  by the application).
 *  @param max_size <IN> Maximum number of bytes that can be put in the user buffer.
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see 
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadShmemObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDFCacheObj_sp_t pshbf_in, SDF_size_t max_size, SDF_size_t * pactual_size, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of part of an object for read-only access.
 *
 *  Get an object and copy it (or a subrange of it) into an application
 *  provided buffer. The application only intends to read the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param offset <IN> Offset of range within an object.
 *  @param nbytes <IN> Length of range within an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see 
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadRangeBufferedObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_in, SDF_size_t offset, SDF_size_t nbytes, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of an object for read-write access.
 *
 *  Get an object, and copy it into an application-provided buffer.
 *  The application  intends to read and write  the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param max_size <IN> Maximum number of bytes that can be put in the user buffer.
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFPutRangeBufferedObject, SDFPutBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWriteBufferedObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_in, SDF_size_t max_size, SDF_size_t * pactual_size, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of part of an object for read-write access.
 *
 *  Get an object and copy it (or a subrange of it) into an application
 *  provided buffer. The application intends to read and write the
 *  object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param offset <IN> Offset of range within an object.
 *  @param nbytes <IN> Length of range within an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFPutRangeBufferedObject, SDFPutBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWriteRangeBufferedObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_in, SDF_size_t offset, SDF_size_t nbytes, SDF_opid_t * p_opid);

/**
 *  @brief Get a pointer to a pinned copy of an object for read-only access.
 *
 *  Get an object and give the application a pointer to a copy of
 *  the object in SDF storage. The application only intends to read
 *  the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFUnpinObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadPinnedObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t * pactual_size, SDF_opid_t * p_opid);

/**
 *  @brief Get a pointer to a pinned copy of an object for  read-write access.
 *
 *  Get an object and give the application a pointer to a copy of
 *  the object in SDF storage. The application will read and write
 *  the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFUnpinObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWritePinnedObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t * pactual_size, SDF_opid_t * p_opid);

/**
 *  @brief Get a pointer to a pinned copy of an object for read/write access; create the object if necessary.  
 *
 *  Get an object and give the application a pointer to a copy of
 *  the object in SDF storage. The application needs write access
 *  to the object. Create or resize the object if necessary. If resizing
 *  is necessary, the contents of the object are undefined.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param sze <IN> Size of object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFUnpinObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_ALREADY_READ_PINNED: object is already read pinned by this context. 
 *          SDF_ALREADY_WRITE_PINNED: object is already write pinned by this context. 
 *          SDF_TRANS_CONFLICT: object is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetPinnedObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t sze, SDF_opid_t * p_opid);

/**
 *  @brief Copy back part of an object that was modified.
 *
 *  Update a subrange of an object from an application-provided buffer.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param offset <IN> Offset of range within an object.
 *  @param nbytes <IN> Length of range within an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFGetForWriteBufferedObject,  SDFGetForWriteRangeBufferedObject,
 *  SDFSetBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_INVALID_RANGE: the range lies outside the object. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutRangeBufferedObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_out, SDF_size_t offset, SDF_size_t nbytes, SDF_opid_t * p_opid);

/**
 *  @brief Copy back an entire object that was modified.
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer.  put_all may change the size of the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param sze <IN> Size of object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFGetForWriteBufferedObject,  SDFGetForWriteRangeBufferedObject,
 *  SDFSetBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutBufferedObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_out, SDF_size_t sze, SDF_opid_t * p_opid);

/**
 *  @brief Copy back an entire object that was modified.
 *
 *  Put an entire object, with contents copied from an application-provided
 *  shmem buffer. put_all may change the size of the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pshbuf_out <IN> Shmem pointer to a data buffer from which to get write data (managed
 *  by the application).
 *  @param sze <IN> Size of object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFGetForWriteBufferedObject,  SDFGetForWriteRangeBufferedObject,
 *  SDFSetBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutShmemObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDFCacheObj_sp_t pshbuf_out, SDF_size_t sze, SDF_opid_t * p_opid);

/**
 *  @brief Unpin an object.
 *
 *  Unpin an object that was previously pinned in SDF storage by 
 *  get_to_read_pin or get_to_write_pin.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFGetForReadPinnedObject,  SDFGetForWritePinnedObject, SDFSetPinnedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_NOT_PINNED: object is not pinned. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_UnpinObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_opid_t * p_opid);

/**
 *  @brief Copy back an entire object that was modified;  create the object if necessary.  
 *
 *  Put an entire object, with contents copied from an  application-provided
 *  buffer. Create or resize the object if necessary.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param sze <IN> Size of object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFGetForWriteBufferedObject,  SDFGetForWriteRangeBufferedObject,
 *  SDFPutBufferedObject, SDFPutRangeBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: object is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetBufferedObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_out, SDF_size_t sze, SDF_opid_t * p_opid);

/**
 *  @brief Copy back an entire object that was modified;  create the object if necessary.  
 *
 *  Put an entire object, with contents copied from an  application-provided
 *  shmem buffer. Create or resize the object if necessary.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pshbuf_out <IN> Shmem pointer to a data buffer from which to get write data (managed
 *  by the application).
 *  @param sze <IN> Size of object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFGetForWriteBufferedObject,  SDFGetForWriteRangeBufferedObject,
 *  SDFPutBufferedObject, SDFPutRangeBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: object is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetShmemObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDFCacheObj_sp_t pshbuf_out, SDF_size_t sze, SDF_opid_t * p_opid);

/**
 *  @brief Force modifications of an object to primary storage.
 *
 *  Flush any modified contents of an object to its backing store
 *  (as determined by its container type). For coherent containers,
 *  this is a global operation that applies to any cache or buffer
 *  in the SDF cluster. For non-coherent containers, this only applies
 *  to the local cache.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFFlushInvalObject, SDFFlushContainer, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_FlushObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_opid_t * p_opid);

/**
 *  @brief Force modifications of an object to primary storage, and invalidate any cached copies.
 *
 *  Flush any modified contents of an object to its backing store
 *  (as determined by its container type). Invalidate all cached copies.
 *  For coherent containers, this is a global operation that applies
 *  to any cache or buffer in the SDF cluster. For non-coherent containers,
 *  this only applies to the local cache.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFFlushObject, SDFFlushContainer, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_FlushInvalObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_opid_t * p_opid);

/**
 *  @brief Invalidate any cached copies of an object.
 *
 *  Invalidate all cached copies of an object. For coherent containers,
 *  this is a global operation that applies to any cache or buffer
 *  in the SDF cluster. For non-coherent containers, this only applies
 *  to the local cache. No dirty data is flushed to primary storage!
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFFlushObject, SDFFlushContainer, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_InvalObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_opid_t * p_opid);

/**
 *  @brief Lock an object for read access.
 *
 *  Place an object in a locked state, blocking if necessary. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFLockWriteObject, SDFTryLockReadObject, SDFTryLockWriteObject,
 *  SDFUnlockReadObject, SDFUnlockWriteObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_LockReadObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_opid_t * p_opid);

/**
 *  @brief Lock an object for write access.
 *
 *  Place an object in a locked state, blocking if necessary. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFLockReadObject, SDFTryLockReadObject, SDFTryLockWriteObject,
 *  SDFUnlockReadObject, SDFUnlockWriteObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_LockWriteObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_opid_t * p_opid);

/**
 *  @brief Lock an object for read access if it is currently unlocked.
 *
 *  Place an object in a locked state only if the lock is immediately
 *  available. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  @param p_success <OUT> Poinert to the variable in which to return the success or failure
 *  of the operation.
 *  
 *  @see SDFLockReadObject, SDFLockWriteObject, SDFTryLockWriteObject,
 *  SDFUnlockReadObject, SDFUnlockWriteObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_TryLockReadObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_opid_t * p_opid, SDF_boolean_t * p_success);

/**
 *  @brief Lock an object for write access if it is currently unlocked.
 *
 *  Place an object in a locked state only if the lock is immediately
 *  available. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  @param p_success <OUT> Poinert to the variable in which to return the success or failure
 *  of the operation.
 *  
 *  @see SDFLockReadObject, SDFLockWriteObject, SDFTryLockReadObject, SDFUnlockReadObject,
 *  SDFUnlockWriteObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_TryLockWriteObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_opid_t * p_opid, SDF_boolean_t * p_success);

/**
 *  @brief Unlock an object previously locked for read access.
 *
 *  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFLockReadObject, SDFLockWriteObject, SDFTryLockReadObject, SDFTryLockWriteObject,
 *  SDFUnlockWriteObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_NOT_READ_LOCKED: this object was not read locked. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_UnlockReadObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_opid_t * p_opid);

/**
 *  @brief Unlock an object previously locked for write access.
 *
 *  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFLockReadObject, SDFLockWriteObject, SDFTryLockReadObject, SDFTryLockWriteObject,
 *  SDFUnlockReadObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_NOT_WRITE_LOCKED: this object was not write locked. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_UnlockWriteObject_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_opid_t * p_opid);

/**
 *  @brief Create a pinned object of the specified size. Set the expiry time.
 *
 *  Creates a pinned object of the specified size and returns a pointer
 *  to the object. The initial contents of the object are undefined.
 *  The expiry time is set.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param sze <IN> Size of object.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFSetPinnedObjectWithExpiry, SDFGetForReadPinnedObjectWithExpiry,
 *  SDFGetForWritePinnedObjectWithExpiry, SDFUnpinObject, SDFInvalidateContainerObjects,
 *  SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the object already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePinnedObjectWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t sze, SDF_time_t curtime, SDF_time_t exptime, SDF_opid_t * p_opid);

/**
 *  @brief Get a pointer to a pinned copy of an object for read/write access; create the object if necessary.   Set its expiry time.
 *
 *  Get an object and give the application a pointer to a copy of
 *  the object in SDF storage. The application needs write access
 *  to the object. Create or resize the object if necessary. If resizing
 *  is necessary, the contents of the object are undefined. The expiry
 *  time for the object is set.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param sze <IN> Size of object.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePinnedObjectWithExpiry, SDFGetForReadPinnedObjectWithExpiry,
 *  SDFGetForWritePinnedObjectWithExpiry, SDFUnpinObject, SDFInvalidateContainerObjects,
 *  SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_ALREADY_READ_PINNED: object is already read pinned by this context. 
 *          SDF_ALREADY_WRITE_PINNED: object is already write pinned by this context. 
 *          SDF_TRANS_CONFLICT: object is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetPinnedObjectWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t sze, SDF_time_t curtime, SDF_time_t exptime, SDF_opid_t * p_opid);

/**
 *  @brief Get a pointer to a pinned copy of an object for read-only access; return its current expiry time.
 *
 *  Get an object and give the application a pointer to a copy of
 *  the object in SDF storage. The application only intends to read
 *  the object. The current expiry time is returned.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  @param curtime <IN> Current time.
 *  @param pexptme <OUT> Current expiry time for an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePinnedObjectWithExpiry, SDFSetPinnedObjectWithExpiry,
 *  SDFGetForWritePinnedObjectWithExpiry, SDFUnpinObject, SDFInvalidateContainerObjects,
 *  SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_ALREADY_READ_PINNED: object is already read pinned by this context. 
 *          SDF_ALREADY_WRITE_PINNED: object is already write pinned by this context. 
 *          SDF_TRANS_CONFLICT: object is already being used in a transaction. 
 *          SDF_EXPIRED: object has expired or been flushed . 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadPinnedObjectWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t * pactual_size, SDF_time_t curtime, SDF_time_t * pexptme, SDF_opid_t * p_opid);

/**
 *  @brief Create an object with an initial value, and set an expiry time.
 *
 *  Creates an object of the specified size and initializes it from
 *  an application buffer.  The expiry time is set.  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFSetBufferedObjectWithExpiry, SDFGetForReadBufferedObjectWithExpiry,
 *  SDFGetForWriteBufferedObjectWithExpiry, SDFPutBufferedObjectWithExpiry,
 *  SDFInvalidateContainerObjects, SDFRemoveObjectWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the object already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePutBufferedObjectWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime, SDF_opid_t * p_opid);

/**
 *  @brief Copy back an entire object that was modified,  and set an expiry time.
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer. put_all may change the size of the object. The expiry
 *  time is set.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePutBufferedObjectWithExpiry, SDFSetBufferedObjectWithExpiry,
 *  SDFGetForReadBufferedObjectWithExpiry, SDFGetForWriteBufferedObjectWithExpiry,
 *  SDFInvalidateContainerObjects, SDFRemoveObjectWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutBufferedObjectWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime, SDF_opid_t * p_opid);

/**
 *  @brief Copy back an entire object, creating it if necessary.  Set an expiry time.
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer. This may change the size of the object. The expiry time
 *  is set. If the object does not exist, create it and assign its
 *  value.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePutBufferedObjectWithExpiry, SDFGetForReadBufferedObjectWithExpiry,
 *  SDFGetForWriteBufferedObjectWithExpiry, SDFPutBufferedObjectWithExpiry,
 *  SDFInvalidateContainerObjects, SDFRemoveObjectWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetBufferedObjectWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of an object for read-only  access. Return its current expiry time.
 *
 *  Get an object and copy it into an SDF-allocated buffer. The application
 *  only intends to read the object. The current expiry time is returned.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppbf_in <IN> Variable in which to return a pointer to the object (in a buffer
 *  allocated by SDF; it must be freed by the application with a call
 *  to SDFFreeObjectBuffer).
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  @param curtime <IN> Current time.
 *  @param pexptme <OUT> Current expiry time for an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePutBufferedObjectWithExpiry, SDFSetBufferedObjectWithExpiry,
 *  SDFGetForWriteBufferedObjectWithExpiry, SDFPutBufferedObjectWithExpiry,
 *  SDFInvalidateContainerObjects, SDFRemoveObjectWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadBufferedObjectWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppbf_in, SDF_size_t * pactual_size, SDF_time_t curtime, SDF_time_t * pexptme, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of an object for read-write access. Return its current expiry time.
 *
 *  Get an object, and copy it into an SDF-allocated buffer. The application
 *  intends to read and write the object. The current expiry time
 *  is returned.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppbf_in <IN> Variable in which to return a pointer to the object (in a buffer
 *  allocated by SDF; it must be freed by the application with a call
 *  to SDFFreeObjectBuffer).
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  @param curtime <IN> Current time.
 *  @param pexptme <OUT> Current expiry time for an object.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePutBufferedObjectWithExpiry, SDFSetBufferedObjectWithExpiry,
 *  SDFGetForReadBufferedObjectWithExpiry, SDFPutBufferedObjectWithExpiry,
 *  SDFInvalidateContainerObjects, SDFRemoveObjectWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWriteBufferedObjectWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppbf_in, SDF_size_t * pactual_size, SDF_time_t curtime, SDF_time_t * pexptme, SDF_opid_t * p_opid);

/**
 *  @brief Delete an object, but check for expiry first.
 *
 *  Delete an object, but check for expiry first.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param curtime <IN> Current time.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePutBufferedObjectWithExpiry, SDFSetBufferedObjectWithExpiry,
 *  SDFGetForReadBufferedObjectWithExpiry, SDFGetForWriteBufferedObjectWithExpiry,
 *  SDFPutBufferedObjectWithExpiry, SDFInvalidateContainerObjects
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_RemoveObjectWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_time_t curtime, SDF_opid_t * p_opid);



/**************************************************************************
 *   
 *   Block Function Declarations (Non-blocking)
 *   
 **************************************************************************/

/**
 *  @brief Create a block.
 *
 *  Creates a block of the specified size.  The new block has undefined
 *  contents.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFRemoveBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the block already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreateBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_opid_t * p_opid);

/**
 *  @brief Create a block with an initial value.
 *
 *  Creates a block of the specified size and initializes it from
 *  an application buffer.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFRemoveBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the block already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePutBufferedBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out, SDF_opid_t * p_opid);

/**
 *  @brief Create a block with an initial value.
 *
 *  Creates a block of the specified size and initializes it from
 *  an application shmem buffer.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pshbuf_out <IN> Shmem pointer to a data buffer from which to get write data (managed
 *  by the application).
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFRemoveBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the block already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePutShmemBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDFCacheObj_sp_t pshbuf_out, SDF_opid_t * p_opid);

/**
 *  @brief Create a pinned block of the specified size.
 *
 *  Creates a pinned block of the specified size and returns a pointer
 *  to the block. The initial contents of the block are undefined.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFSetPinnedBlock, SDFRemoveBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the block already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePinnedBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata, SDF_opid_t * p_opid);

/**
 *  @brief Delete a block.
 *
 *  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreateBlock, SDFCreatePutBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_RemoveBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of a block for read-only  access.
 *
 *  Get a block and copy it into an application-provided buffer. The
 *  application only intends to read the block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param max_size <IN> Maximum number of bytes that can be put in the user buffer.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see 
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadBufferedBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_in, SDF_size_t max_size, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of a block for read-only  access.
 *
 *  Get a block and copy it into an application-provided shmem buffer.
 *  The application only intends to read the block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pshbf_in <OUT> Shmem pointer to a data buffer into which to copy read data (managed
 *  by the application).
 *  @param max_size <IN> Maximum number of bytes that can be put in the user buffer.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see 
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadShmemBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDFCacheObj_sp_t pshbf_in, SDF_size_t max_size, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of part of a block for read-only access.
 *
 *  Get a block and copy it (or a subrange of it) into an application
 *  provided buffer. The application only intends to read the block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param offset <IN> Offset of range within a block.
 *  @param nbytes <IN> Length of range within a block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see 
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadRangeBufferedBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_in, SDF_size_t offset, SDF_size_t nbytes, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of a block for read-write access.
 *
 *  Get a block, and copy it into an application-provided buffer.
 *  The application  intends to read and write  the block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param max_size <IN> Maximum number of bytes that can be put in the user buffer.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFPutRangeBufferedBlock, SDFPutBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWriteBufferedBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_in, SDF_size_t max_size, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of part of a block for read-write access.
 *
 *  Get a block and copy it (or a subrange of it) into an application
 *  provided buffer. The application intends to read and write the
 *  block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param offset <IN> Offset of range within a block.
 *  @param nbytes <IN> Length of range within a block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFPutRangeBufferedBlock, SDFPutBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWriteRangeBufferedBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_in, SDF_size_t offset, SDF_size_t nbytes, SDF_opid_t * p_opid);

/**
 *  @brief Get a pointer to a pinned copy of a block for read-only access.
 *
 *  Get a block and give the application a pointer to a copy of the
 *  block in SDF storage. The application only intends to read the
 *  block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFUnpinBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadPinnedBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata, SDF_opid_t * p_opid);

/**
 *  @brief Get a pointer to a pinned copy of a block for  read-write access.
 *
 *  Get a block and give the application a pointer to a copy of the
 *  block in SDF storage. The application will read and write the
 *  block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFUnpinBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWritePinnedBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata, SDF_opid_t * p_opid);

/**
 *  @brief Get a pointer to a pinned copy of a block for read/write access; create the block if necessary.  
 *
 *  Get a block and give the application a pointer to a copy of the
 *  block in SDF storage. The application needs write access to the
 *  block. Create or resize the block if necessary. If resizing is
 *  necessary, the contents of the block are undefined.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFUnpinBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_ALREADY_READ_PINNED: block is already read pinned by this context. 
 *          SDF_ALREADY_WRITE_PINNED: block is already write pinned by this context. 
 *          SDF_TRANS_CONFLICT: block is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetPinnedBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata, SDF_opid_t * p_opid);

/**
 *  @brief Copy back part of a block that was modified.
 *
 *  Update a subrange of a block from an application-provided buffer.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param offset <IN> Offset of range within a block.
 *  @param nbytes <IN> Length of range within a block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFGetForWriteBufferedBlock,  SDFGetForWriteRangeBufferedBlock,
 *  SDFSetBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_INVALID_RANGE: the range lies outside the block. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutRangeBufferedBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out, SDF_size_t offset, SDF_size_t nbytes, SDF_opid_t * p_opid);

/**
 *  @brief Copy back an entire block that was modified.
 *
 *  Put an entire block, with contents copied from an application-provided
 *  buffer.  put_all may change the size of the block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFGetForWriteBufferedBlock,  SDFGetForWriteRangeBufferedBlock,
 *  SDFSetBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutBufferedBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out, SDF_opid_t * p_opid);

/**
 *  @brief Copy back an entire block that was modified.
 *
 *  Put an entire block, with contents copied from an application-provided
 *  shmem buffer. put_all may change the size of the block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pshbuf_out <IN> Shmem pointer to a data buffer from which to get write data (managed
 *  by the application).
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFGetForWriteBufferedBlock,  SDFGetForWriteRangeBufferedBlock,
 *  SDFSetBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutShmemBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDFCacheObj_sp_t pshbuf_out, SDF_opid_t * p_opid);

/**
 *  @brief Unpin a block.
 *
 *  Unpin a block that was previously pinned in SDF storage by  get_to_read_pin
 *  or get_to_write_pin.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFGetForReadPinnedBlock,  SDFGetForWritePinnedBlock, SDFSetPinnedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_NOT_PINNED: block is not pinned. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_UnpinBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_opid_t * p_opid);

/**
 *  @brief Copy back an entire block that was modified;  create the block if necessary.  
 *
 *  Put an entire block, with contents copied from an  application-provided
 *  buffer. Create or resize the block if necessary.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFGetForWriteBufferedBlock,  SDFGetForWriteRangeBufferedBlock,
 *  SDFPutBufferedBlock, SDFPutRangeBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: block is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetBufferedBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out, SDF_opid_t * p_opid);

/**
 *  @brief Copy back an entire block that was modified;  create the block if necessary.  
 *
 *  Put an entire block, with contents copied from an  application-provided
 *  shmem buffer. Create or resize the block if necessary.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pshbuf_out <IN> Shmem pointer to a data buffer from which to get write data (managed
 *  by the application).
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFGetForWriteBufferedBlock,  SDFGetForWriteRangeBufferedBlock,
 *  SDFPutBufferedBlock, SDFPutRangeBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: block is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetShmemBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDFCacheObj_sp_t pshbuf_out, SDF_opid_t * p_opid);

/**
 *  @brief Force modifications of a block to primary storage.
 *
 *  Flush any modified contents of a block to its backing store (as
 *  determined by its container type). For coherent containers, this
 *  is a global operation that applies to any cache or buffer in the
 *  SDF cluster. For non-coherent containers, this only applies to
 *  the local cache.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFFlushInvalBlock, SDFFlushContainer, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_FlushBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_opid_t * p_opid);

/**
 *  @brief Force modifications of a block to primary storage, and invalidate any cached copies.
 *
 *  Flush any modified contents of a block to its backing store (as
 *  determined by its container type). Invalidate all cached copies.
 *  For coherent containers, this is a global operation that applies
 *  to any cache or buffer in the SDF cluster. For non-coherent containers,
 *  this only applies to the local cache.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFFlushBlock, SDFFlushContainer, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_FlushInvalBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_opid_t * p_opid);

/**
 *  @brief Invalidate any cached copies of a block.
 *
 *  Invalidate all cached copies of a block. For coherent containers,
 *  this is a global operation that applies to any cache or buffer
 *  in the SDF cluster. For non-coherent containers, this only applies
 *  to the local cache. No dirty data is flushed to primary storage!
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFFlushBlock, SDFFlushContainer, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_InvalBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_opid_t * p_opid);

/**
 *  @brief Lock a block for read access.
 *
 *  Place a block in a locked state, blocking if necessary. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFLockWriteBlock, SDFTryLockReadBlock, SDFTryLockWriteBlock,
 *  SDFUnlockReadBlock, SDFUnlockWriteBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_LockReadBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_opid_t * p_opid);

/**
 *  @brief Lock a block for write access.
 *
 *  Place a block in a locked state, blocking if necessary. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFLockReadBlock, SDFTryLockReadBlock, SDFTryLockWriteBlock, SDFUnlockReadBlock,
 *  SDFUnlockWriteBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_LockWriteBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_opid_t * p_opid);

/**
 *  @brief Lock a block for read access if it is currently unlocked.
 *
 *  Place a block in a locked state only if the lock is immediately
 *  available. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  @param p_success <OUT> Poinert to the variable in which to return the success or failure
 *  of the operation.
 *  
 *  @see SDFLockReadBlock, SDFLockWriteBlock, SDFTryLockWriteBlock, SDFUnlockReadBlock,
 *  SDFUnlockWriteBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_TryLockReadBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_opid_t * p_opid, SDF_boolean_t * p_success);

/**
 *  @brief Lock a block for write access if it is currently unlocked.
 *
 *  Place a block in a locked state only if the lock is immediately
 *  available. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  @param p_success <OUT> Poinert to the variable in which to return the success or failure
 *  of the operation.
 *  
 *  @see SDFLockReadBlock, SDFLockWriteBlock, SDFTryLockReadBlock, SDFUnlockReadBlock,
 *  SDFUnlockWriteBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_TryLockWriteBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_opid_t * p_opid, SDF_boolean_t * p_success);

/**
 *  @brief Unlock a block previously locked for read access.
 *
 *  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFLockReadBlock, SDFLockWriteBlock, SDFTryLockReadBlock, SDFTryLockWriteBlock,
 *  SDFUnlockWriteBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_NOT_READ_LOCKED: this block was not read locked. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_UnlockReadBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_opid_t * p_opid);

/**
 *  @brief Unlock a block previously locked for write access.
 *
 *  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFLockReadBlock, SDFLockWriteBlock, SDFTryLockReadBlock, SDFTryLockWriteBlock,
 *  SDFUnlockReadBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_NOT_WRITE_LOCKED: this block was not write locked. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_UnlockWriteBlock_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_opid_t * p_opid);

/**
 *  @brief Create a pinned block of the specified size. Set the expiry time.
 *
 *  Creates a pinned block of the specified size and returns a pointer
 *  to the block. The initial contents of the block are undefined.
 *  The expiry time is set.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for a block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFSetPinnedBlockWithExpiry, SDFGetForReadPinnedBlockWithExpiry,
 *  SDFGetForWritePinnedBlockWithExpiry, SDFUnpinBlock, SDFInvalidateContainerBlocks,
 *  SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the block already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePinnedBlockWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata, SDF_time_t curtime, SDF_time_t exptime, SDF_opid_t * p_opid);

/**
 *  @brief Get a pointer to a pinned copy of a block for read/write access; create the block if necessary.   Set its expiry time.
 *
 *  Get a block and give the application a pointer to a copy of the
 *  block in SDF storage. The application needs write access to the
 *  block. Create or resize the block if necessary. If resizing is
 *  necessary, the contents of the block are undefined. The expiry
 *  time for the block is set.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for a block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePinnedBlockWithExpiry, SDFGetForReadPinnedBlockWithExpiry,
 *  SDFGetForWritePinnedBlockWithExpiry, SDFUnpinBlock, SDFInvalidateContainerBlocks,
 *  SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_ALREADY_READ_PINNED: block is already read pinned by this context. 
 *          SDF_ALREADY_WRITE_PINNED: block is already write pinned by this context. 
 *          SDF_TRANS_CONFLICT: block is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetPinnedBlockWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata, SDF_time_t curtime, SDF_time_t exptime, SDF_opid_t * p_opid);

/**
 *  @brief Get a pointer to a pinned copy of a block for read-only access; return its current expiry time.
 *
 *  Get a block and give the application a pointer to a copy of the
 *  block in SDF storage. The application only intends to read the
 *  block. The current expiry time is returned.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  @param curtime <IN> Current time.
 *  @param pexptme <OUT> Current expiry time for a block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePinnedBlockWithExpiry, SDFSetPinnedBlockWithExpiry, SDFGetForWritePinnedBlockWithExpiry,
 *  SDFUnpinBlock, SDFInvalidateContainerBlocks, SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_ALREADY_READ_PINNED: block is already read pinned by this context. 
 *          SDF_ALREADY_WRITE_PINNED: block is already write pinned by this context. 
 *          SDF_TRANS_CONFLICT: block is already being used in a transaction. 
 *          SDF_EXPIRED: block has expired or been flushed . 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadPinnedBlockWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata, SDF_time_t curtime, SDF_time_t * pexptme, SDF_opid_t * p_opid);

/**
 *  @brief Create a block with an initial value, and set an expiry time.
 *
 *  Creates a block of the specified size and initializes it from
 *  an application buffer.  The expiry time is set.  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for a block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFSetBufferedBlockWithExpiry, SDFGetForReadBufferedBlockWithExpiry,
 *  SDFGetForWriteBufferedBlockWithExpiry, SDFPutBufferedBlockWithExpiry,
 *  SDFInvalidateContainerBlocks, SDFRemoveBlockWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the block already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePutBufferedBlockWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime, SDF_opid_t * p_opid);

/**
 *  @brief Copy back an entire block that was modified,  and set an expiry time.
 *
 *  Put an entire block, with contents copied from an application-provided
 *  buffer. put_all may change the size of the block. The expiry time
 *  is set.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for a block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePutBufferedBlockWithExpiry, SDFSetBufferedBlockWithExpiry,
 *  SDFGetForReadBufferedBlockWithExpiry, SDFGetForWriteBufferedBlockWithExpiry,
 *  SDFInvalidateContainerBlocks, SDFRemoveBlockWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutBufferedBlockWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime, SDF_opid_t * p_opid);

/**
 *  @brief Copy back an entire block, creating it if necessary.  Set an expiry time.
 *
 *  Put an entire block, with contents copied from an application-provided
 *  buffer. This may change the size of the block. The expiry time
 *  is set. If the block does not exist, create it and assign its
 *  value.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for a block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePutBufferedBlockWithExpiry, SDFGetForReadBufferedBlockWithExpiry,
 *  SDFGetForWriteBufferedBlockWithExpiry, SDFPutBufferedBlockWithExpiry,
 *  SDFInvalidateContainerBlocks, SDFRemoveBlockWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetBufferedBlockWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of a block for read-only  access. Return its current expiry time.
 *
 *  Get a block and copy it into an SDF-allocated buffer. The application
 *  only intends to read the block. The current expiry time is returned.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppbf_in <IN> Variable in which to return a pointer to the block (in a buffer
 *  allocated by SDF; it must be freed by the application with a call
 *  to SDFFreeBlockBuffer).
 *  @param curtime <IN> Current time.
 *  @param pexptme <OUT> Current expiry time for a block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePutBufferedBlockWithExpiry, SDFSetBufferedBlockWithExpiry,
 *  SDFGetForWriteBufferedBlockWithExpiry, SDFPutBufferedBlockWithExpiry,
 *  SDFInvalidateContainerBlocks, SDFRemoveBlockWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadBufferedBlockWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppbf_in, SDF_time_t curtime, SDF_time_t * pexptme, SDF_opid_t * p_opid);

/**
 *  @brief Get a copy of a block for read-write access. Return its current expiry time.
 *
 *  Get a block, and copy it into an SDF-allocated buffer. The application
 *  intends to read and write the block. The current expiry time is
 *  returned.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppbf_in <IN> Variable in which to return a pointer to the block (in a buffer
 *  allocated by SDF; it must be freed by the application with a call
 *  to SDFFreeBlockBuffer).
 *  @param curtime <IN> Current time.
 *  @param pexptme <OUT> Current expiry time for a block.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePutBufferedBlockWithExpiry, SDFSetBufferedBlockWithExpiry,
 *  SDFGetForReadBufferedBlockWithExpiry, SDFPutBufferedBlockWithExpiry,
 *  SDFInvalidateContainerBlocks, SDFRemoveBlockWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWriteBufferedBlockWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppbf_in, SDF_time_t curtime, SDF_time_t * pexptme, SDF_opid_t * p_opid);

/**
 *  @brief Delete a block, but check for expiry first.
 *
 *  Delete a block, but check for expiry first.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param curtime <IN> Current time.
 *  @param p_opid <OUT> Pointer to the variable in which to return the opid for an asynchronous
 *  operation.
 *  
 *  @see SDFCreatePutBufferedBlockWithExpiry, SDFSetBufferedBlockWithExpiry,
 *  SDFGetForReadBufferedBlockWithExpiry, SDFGetForWriteBufferedBlockWithExpiry,
 *  SDFPutBufferedBlockWithExpiry, SDFInvalidateContainerBlocks
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_RemoveBlockWithExpiry_NB(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_time_t curtime, SDF_opid_t * p_opid);



/**************************************************************************
 *   
 *   General Function Declarations (Blocking)
 *   
 **************************************************************************/

/**
 *  @brief Create a new SDF context.
 *
 *  Get a new, unique SDF context. SDF contexts are used to distinguish
 *  between different entities in lock, pin and transactional requests.
 *  SDF enforces serial dependencies of operations issued with the
 *  same context.
 *
 *  @param pctxt <OUT> Pointer to the variable in which to return the SDF context.
 *  
 *  @see SDFDeleteContext
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_OUT_OF_CONTEXTS: all SDF contexts have been exhausted. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_NewContext(SDF_internal_ctxt_t *pai, SDF_context_t * pctxt);

/**
 *  @brief Delete an existing SDF context.
 *
 *  Retire a context previously generated with SDFNewContext.
 *
 *  @param contxt <IN> The SDF context for which this operation applies.
 *  
 *  @see SDFNewContext
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_Delete_Context(SDF_internal_ctxt_t *pai, SDF_context_t contxt);

/**
 *  @brief Flush all objects in a particular container from the local cache.
 *
 *  Flush all modified contents of a container from the local cache.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  
 *  @see SDFFlushBlock, SDFFlushInvalBlock, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_NONCACHEABLE_CONTAINER: this container is not cacheable. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_FlushContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr);

/**
 *  @brief Flush all objects in a particular container from the local cache, and invalidate them.
 *
 *  Flush all modified contents of a container from the local cache;
 *  invalidate all objects from this container that are in the local
 *  cache..
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  
 *  @see SDFFlushBlock, SDFFlushInvalBlock, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_NONCACHEABLE_CONTAINER: this container is not cacheable. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_FlushInvalContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr);

/**
 *  @brief Invalidate all objects in a particular container from the local cache.
 *
 *  Invalidate all objects from this container that are in the local
 *  cache. Any dirty data is lost (it is not flushed to primary storage!).
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  
 *  @see SDFFlushBlock, SDFFlushInvalBlock, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_NONCACHEABLE_CONTAINER: this container is not cacheable. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_InvalContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr);

/**
 *  @brief Wait until all specified operations have completed.
 *
 *  Wait for all of the specified operations to “complete”. Put
 *  operations are “complete” when their effects are visible to
 *  all other threads. Get operations are “complete” when their
 *  results can no longer be affected by any “put” in the system.
 *  Flush operations are “complete” when modified data has been
 *  committed to primary storage. In either case, data has not necessarily
 *  been committed to any replicas.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param opid[] <IN> Array of operation id's to synchronize with.
 *  @param n_opids <IN> Length of opid[] array.
 *  
 *  @see SDFSync_P, SDFSyncAll, SDFSyncAll_P, SDFSyncReplicas, SDFSyncReplicas_P,
 *  SDFSyncReplicasAll, SDFSyncReplicasAll_P
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_Sync(SDF_internal_ctxt_t *pai, SDF_opid_t opid[], uint32_t n_opids);

/**
 *  @brief Query if all of the specified operations have completed.
 *
 *  See if all of the specified operations are “complete”. Put
 *  operations are “complete” when their effects are visible to
 *  all other threads. Get operations are “complete” when their
 *  results can no longer be affected by any “put” in the system.
 *  Flush operations are “complete” when modified data has been
 *  committed to primary storage. In either case, data has not necessarily
 *  been committed to any replicas.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param opid[] <IN> Array of operation id's to synchronize with.
 *  @param n_opids <IN> Length of opid[] array.
 *  @param p_success <OUT> Poinert to the variable in which to return the success or failure
 *  of the operation.
 *  
 *  @see SDFSync, SDFSyncAll, SDFSyncAll_P, SDFSyncReplicas, SDFSyncReplicas_P,
 *  SDFSyncReplicasAll, SDFSyncReplicasAll_P
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_Sync_P(SDF_internal_ctxt_t *pai, SDF_opid_t opid[], uint32_t n_opids, SDF_boolean_t * p_success);

/**
 *  @brief Wait until all prior operations have completed.
 *
 *  Wait for all prior storage operations (issued by this thread)
 *  to complete.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  
 *  @see SDFSync, SDFSync_P, SDFSyncAll_P, SDFSyncReplicas, SDFSyncReplicas_P,
 *  SDFSyncReplicasAll, SDFSyncReplicasAll_P
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SyncAll(SDF_internal_ctxt_t *pai);

/**
 *  @brief Query if all prior operations have completed.
 *
 *  See if all prior storage operations (issued by this thread) to
 *  complete.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param p_success <OUT> Poinert to the variable in which to return the success or failure
 *  of the operation.
 *  
 *  @see SDFSync, SDFSync_P, SDFSyncAll, SDFSyncReplicas, SDFSyncReplicas_P,
 *  SDFSyncReplicasAll, SDFSyncReplicasAll_P
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SyncAll_P(SDF_internal_ctxt_t *pai, SDF_boolean_t * p_success);

/**
 *  @brief Flush all dirty objects from the local cache.
 *
 *  Flush all dirty data from the cache with which the current thread
 *  is associated.  No objects in the cache are invalidated.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  
 *  @see SDFFlushBlock, SDFFlushInvalBlock, SDFFlushContainer, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_FlushCache(SDF_internal_ctxt_t *pai);

/**
 *  @brief Flush all dirty objects from the local cache, and invalidate all objects.
 *
 *  Flush all dirty data from the cache with which the current thread
 *  is associated.  All objects in the cache are invalidated.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  
 *  @see SDFFlushBlock, SDFFlushInvalBlock, SDFFlushContainer, SDFFlushCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_FlushInvalCache(SDF_internal_ctxt_t *pai);

/**
 *  @brief Invalidate all objects in the local cache.
 *
 *  Invalidate all data from the cache with which the current thread
 *  is associated.  No dirty data is pushed to primary storage!
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  
 *  @see SDFFlushBlock, SDFFlushInvalBlock, SDFFlushContainer, SDFFlushCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_InvalCache(SDF_internal_ctxt_t *pai);

/**
 *  @brief Count the number of entries in the cache.
 *
 *  Count the number of entries in the cache.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param pstat <OUT> Pointer to variable in which to return the value of a container
 *  statistic.
 *  
 *  @see SDFCountContainerCacheEntries
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CountCacheEntries(SDF_internal_ctxt_t *pai, uint64_t * pstat);

/**
 *  @brief Count the number of entries in the cache that are in a particular container.
 *
 *  Count the number of entries in the cache that are in a particular
 *  container.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param pstat <OUT> Pointer to variable in which to return the value of a container
 *  statistic.
 *  
 *  @see SDFCountCacheEntries
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CountContainerCacheEntries(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t * pstat);

/**
 *  @brief Invalidate all objects in the local cache that are homed at the specified vnode.
 *
 *  Invalidate all data homed at vnode from the cache with which the
 *  current thread is associated. No dirty data is pushed to primary
 *  storage!
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param vnode <IN> Virtual node ID of a home node.
 *  
 *  @see SDFFlushBlock, SDFFlushInvalBlock, SDFFlushContainer, SDFFlushCache,
 *  SDFInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_InvalCacheHome(SDF_internal_ctxt_t *pai, SDF_vnode_t vnode);

/**
 *  @brief Get a container statistic.
 *
 *  Get a container statistic.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param stat_Key <IN> Key of a container statistic.
 *  @param pstat <OUT> Pointer to variable in which to return the value of a container
 *  statistic.
 *  
 *  @see SDFCreatePinnedBlockWithExpiry, SDFSetPinnedBlockWithExpiry, SDFGetForReadPinnedBlockWithExpiry,
 *  SDFGetForWritePinnedBlockWithExpiry, SDFUnpinBlock, SDFInvalidateContainerBlocks
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetContainerStats2(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint32_t stat_Key, uint64_t * pstat);


/**
 *  @brief Sync primary storage (ensure that any buffered data is put to storage).
 *
 *  Sync any buffered changes to container to primary storage.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  
 *  @see SDFFlushBlock, SDFFlushInvalBlock, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_NONCACHEABLE_CONTAINER: this container is not cacheable. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SyncContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr);

/**************************************************************************
 *   
 *   Object Function Declarations (Blocking)
 *   
 **************************************************************************/

/**
 *  @brief Create an object.
 *
 *  Creates an object of the specified size.  The new object has undefined
 *  contents.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  
 *  @see SDFRemoveObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the object already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreateObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze);

/**
 *  @brief Create an object with an initial value.
 *
 *  Creates an object of the specified size and initializes it from
 *  an application buffer.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  
 *  @see SDFRemoveObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the object already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePutBufferedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void * pbuf_out);

/**
 *  @brief Create an object with an initial value.
 *
 *  Creates an object of the specified size and initializes it from
 *  an application shmem buffer.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  @param pshbuf_out <IN> Shmem pointer to a data buffer from which to get write data (managed
 *  by the application).
 *  
 *  @see SDFRemoveObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the object already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePutShmemObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, SDFCacheObj_sp_t pshbuf_out);

/**
 *  @brief Create a pinned object of the specified size.
 *
 *  Creates a pinned object of the specified size and returns a pointer
 *  to the object. The initial contents of the object are undefined.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param sze <IN> Size of object.
 *  
 *  @see SDFSetPinnedObject, SDFRemoveObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the object already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePinnedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t sze);

/**
 *  @brief Delete an object.
 *
 *  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  
 *  @see SDFCreateObject, SDFCreatePutBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_RemoveObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen);

/**
 *  @brief Get a copy of an object for read-only  access.
 *
 *  Get an object and copy it into an application-provided buffer.
 *  The application only intends to read the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param max_size <IN> Maximum number of bytes that can be put in the user buffer.
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  
 *  @see 
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadBufferedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_in, SDF_size_t max_size, SDF_size_t * pactual_size);

/**
 *  @brief Get a copy of an object for read-only  access.
 *
 *  Get an object and copy it into an application-provided shmem buffer.
 *  The application only intends to read the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pshbf_in <OUT> Shmem pointer to a data buffer into which to copy read data (managed
 *  by the application).
 *  @param max_size <IN> Maximum number of bytes that can be put in the user buffer.
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  
 *  @see 
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadShmemObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDFCacheObj_sp_t pshbf_in, SDF_size_t max_size, SDF_size_t * pactual_size);

/**
 *  @brief Get a copy of part of an object for read-only access.
 *
 *  Get an object and copy it (or a subrange of it) into an application
 *  provided buffer. The application only intends to read the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param offset <IN> Offset of range within an object.
 *  @param nbytes <IN> Length of range within an object.
 *  
 *  @see 
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadRangeBufferedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_in, SDF_size_t offset, SDF_size_t nbytes);

/**
 *  @brief Get a copy of an object for read-write access.
 *
 *  Get an object, and copy it into an application-provided buffer.
 *  The application  intends to read and write  the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param max_size <IN> Maximum number of bytes that can be put in the user buffer.
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  
 *  @see SDFPutRangeBufferedObject, SDFPutBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWriteBufferedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_in, SDF_size_t max_size, SDF_size_t * pactual_size);

/**
 *  @brief Get a copy of part of an object for read-write access.
 *
 *  Get an object and copy it (or a subrange of it) into an application
 *  provided buffer. The application intends to read and write the
 *  object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param offset <IN> Offset of range within an object.
 *  @param nbytes <IN> Length of range within an object.
 *  
 *  @see SDFPutRangeBufferedObject, SDFPutBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWriteRangeBufferedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_in, SDF_size_t offset, SDF_size_t nbytes);

/**
 *  @brief Get a pointer to a pinned copy of an object for read-only access.
 *
 *  Get an object and give the application a pointer to a copy of
 *  the object in SDF storage. The application only intends to read
 *  the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  
 *  @see SDFUnpinObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadPinnedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t * pactual_size);

/**
 *  @brief Get a pointer to a pinned copy of an object for  read-write access.
 *
 *  Get an object and give the application a pointer to a copy of
 *  the object in SDF storage. The application will read and write
 *  the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  
 *  @see SDFUnpinObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWritePinnedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t * pactual_size);

/**
 *  @brief Get a pointer to a pinned copy of an object for read/write access; create the object if necessary.  
 *
 *  Get an object and give the application a pointer to a copy of
 *  the object in SDF storage. The application needs write access
 *  to the object. Create or resize the object if necessary. If resizing
 *  is necessary, the contents of the object are undefined.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param sze <IN> Size of object.
 *  
 *  @see SDFUnpinObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_ALREADY_READ_PINNED: object is already read pinned by this context. 
 *          SDF_ALREADY_WRITE_PINNED: object is already write pinned by this context. 
 *          SDF_TRANS_CONFLICT: object is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetPinnedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t sze);

/**
 *  @brief Copy back part of an object that was modified.
 *
 *  Update a subrange of an object from an application-provided buffer.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param offset <IN> Offset of range within an object.
 *  @param nbytes <IN> Length of range within an object.
 *  
 *  @see SDFGetForWriteBufferedObject,  SDFGetForWriteRangeBufferedObject,
 *  SDFSetBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_INVALID_RANGE: the range lies outside the object. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutRangeBufferedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_out, SDF_size_t offset, SDF_size_t nbytes);

/**
 *  @brief Copy back an entire object that was modified.
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer.  put_all may change the size of the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param sze <IN> Size of object.
 *  
 *  @see SDFGetForWriteBufferedObject,  SDFGetForWriteRangeBufferedObject,
 *  SDFSetBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutBufferedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_out, SDF_size_t sze);

/**
 *  @brief Copy back an entire object that was modified.
 *
 *  Put an entire object, with contents copied from an application-provided
 *  shmem buffer. put_all may change the size of the object.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pshbuf_out <IN> Shmem pointer to a data buffer from which to get write data (managed
 *  by the application).
 *  @param sze <IN> Size of object.
 *  
 *  @see SDFGetForWriteBufferedObject,  SDFGetForWriteRangeBufferedObject,
 *  SDFSetBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutShmemObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDFCacheObj_sp_t pshbuf_out, SDF_size_t sze);

/**
 *  @brief Unpin an object.
 *
 *  Unpin an object that was previously pinned in SDF storage by 
 *  get_to_read_pin or get_to_write_pin.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  
 *  @see SDFGetForReadPinnedObject,  SDFGetForWritePinnedObject, SDFSetPinnedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_NOT_PINNED: object is not pinned. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_UnpinObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen);

/**
 *  @brief Copy back an entire object that was modified;  create the object if necessary.  
 *
 *  Put an entire object, with contents copied from an  application-provided
 *  buffer. Create or resize the object if necessary.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param sze <IN> Size of object.
 *  
 *  @see SDFGetForWriteBufferedObject,  SDFGetForWriteRangeBufferedObject,
 *  SDFPutBufferedObject, SDFPutRangeBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: object is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetBufferedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_out, SDF_size_t sze);

/**
 *  @brief Copy back an entire object that was modified;  create the object if necessary.  
 *
 *  Put an entire object, with contents copied from an  application-provided
 *  shmem buffer. Create or resize the object if necessary.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param pshbuf_out <IN> Shmem pointer to a data buffer from which to get write data (managed
 *  by the application).
 *  @param sze <IN> Size of object.
 *  
 *  @see SDFGetForWriteBufferedObject,  SDFGetForWriteRangeBufferedObject,
 *  SDFPutBufferedObject, SDFPutRangeBufferedObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: object is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetShmemObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDFCacheObj_sp_t pshbuf_out, SDF_size_t sze);

/**
 *  @brief Force modifications of an object to primary storage.
 *
 *  Flush any modified contents of an object to its backing store
 *  (as determined by its container type). For coherent containers,
 *  this is a global operation that applies to any cache or buffer
 *  in the SDF cluster. For non-coherent containers, this only applies
 *  to the local cache.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  
 *  @see SDFFlushInvalObject, SDFFlushContainer, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_FlushObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen);

/**
 *  @brief Force modifications of an object to primary storage, and invalidate any cached copies.
 *
 *  Flush any modified contents of an object to its backing store
 *  (as determined by its container type). Invalidate all cached copies.
 *  For coherent containers, this is a global operation that applies
 *  to any cache or buffer in the SDF cluster. For non-coherent containers,
 *  this only applies to the local cache.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  
 *  @see SDFFlushObject, SDFFlushContainer, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_FlushInvalObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen);

/**
 *  @brief Invalidate any cached copies of an object.
 *
 *  Invalidate all cached copies of an object. For coherent containers,
 *  this is a global operation that applies to any cache or buffer
 *  in the SDF cluster. For non-coherent containers, this only applies
 *  to the local cache. No dirty data is flushed to primary storage!
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  
 *  @see SDFFlushObject, SDFFlushContainer, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_InvalObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen);

/**
 *  @brief Remote invalidate cached copies of an object.
 *
 *  Invalidate all cached copies of an object. 
 *  This version tries to get the cache slab lock, and if it fails, it
 *  enqueues an invalidation to be applied later.  This invalidation
 *  command is for use by remote replication requests to prevent deadlock.
 *  For coherent containers,
 *  this is a global operation that applies to any cache or buffer
 *  in the SDF cluster. For non-coherent containers, this only applies
 *  to the local cache. No dirty data is flushed to primary storage!
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  
 *  @see SDFRemoteUpdateObject, SDFRemoteFlushObject, SDFRemoteFlushInvalObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_RemoteInvalObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen);

/**
 *  @brief Remote flush a cached copy of an object.
 *
 *  Flush the cached copy of an object. 
 *  This version tries to get the cache slab lock, and if it fails, it
 *  enqueues a flush to be applied later.  This flush 
 *  command is for use by remote replication requests to prevent deadlock.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  
 *  @see SDFRemoteInvalObject, SDFRemoteFlushInvalObject, SDFRemoteUpdateObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_RemoteFlushObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen);

/**
 *  @brief Remote flush and invalidate a cached copy of an object.
 *
 *  Flush and invalidate the cached copy of an object. 
 *  This version tries to get the cache slab lock, and if it fails, it
 *  enqueues a flush-invalidation to be applied later.  This
 *  command is for use by remote replication requests to prevent deadlock.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  
 *  @see SDFRemoteInvalObject, SDFRemoteFlushObject, SDFRemoteUpdateObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_RemoteFlushInvalObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen);

/**
 *  @brief On behalf of a remote mirror, Update an entire object, creating it if necessary.  Set an expiry time.
 *
 *  Set an entire object, with contents copied from a message buffer
 *  buffer. This may change the size of the object. The expiry time
 *  is set. If the object does not exist, create it and assign its
 *  value.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for an object.
 *  
 *  @see SDFRemoteInvalObject, SDFRemoteFlushObject, SDFRemoteFlushInvalObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_RemoteUpdateObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime);

/**
 *  @brief Lock an object for read access.
 *
 *  Place an object in a locked state, blocking if necessary. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  
 *  @see SDFLockWriteObject, SDFTryLockReadObject, SDFTryLockWriteObject,
 *  SDFUnlockReadObject, SDFUnlockWriteObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_LockReadObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen);

/**
 *  @brief Lock an object for write access.
 *
 *  Place an object in a locked state, blocking if necessary. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  
 *  @see SDFLockReadObject, SDFTryLockReadObject, SDFTryLockWriteObject,
 *  SDFUnlockReadObject, SDFUnlockWriteObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_LockWriteObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen);

/**
 *  @brief Lock an object for read access if it is currently unlocked.
 *
 *  Place an object in a locked state only if the lock is immediately
 *  available. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_success <OUT> Poinert to the variable in which to return the success or failure
 *  of the operation.
 *  
 *  @see SDFLockReadObject, SDFLockWriteObject, SDFTryLockWriteObject,
 *  SDFUnlockReadObject, SDFUnlockWriteObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_TryLockReadObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_boolean_t * p_success);

/**
 *  @brief Lock an object for write access if it is currently unlocked.
 *
 *  Place an object in a locked state only if the lock is immediately
 *  available. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param p_success <OUT> Poinert to the variable in which to return the success or failure
 *  of the operation.
 *  
 *  @see SDFLockReadObject, SDFLockWriteObject, SDFTryLockReadObject, SDFUnlockReadObject,
 *  SDFUnlockWriteObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_TryLockWriteObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_boolean_t * p_success);

/**
 *  @brief Unlock an object previously locked for read access.
 *
 *  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  
 *  @see SDFLockReadObject, SDFLockWriteObject, SDFTryLockReadObject, SDFTryLockWriteObject,
 *  SDFUnlockWriteObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_NOT_READ_LOCKED: this object was not read locked. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_UnlockReadObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen);

/**
 *  @brief Unlock an object previously locked for write access.
 *
 *  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  
 *  @see SDFLockReadObject, SDFLockWriteObject, SDFTryLockReadObject, SDFTryLockWriteObject,
 *  SDFUnlockReadObject
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_NOT_WRITE_LOCKED: this object was not write locked. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_UnlockWriteObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen);

/**
 *  @brief Create a pinned object of the specified size. Set the expiry time.
 *
 *  Creates a pinned object of the specified size and returns a pointer
 *  to the object. The initial contents of the object are undefined.
 *  The expiry time is set.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param sze <IN> Size of object.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for an object.
 *  
 *  @see SDFSetPinnedObjectWithExpiry, SDFGetForReadPinnedObjectWithExpiry,
 *  SDFGetForWritePinnedObjectWithExpiry, SDFUnpinObject, SDFInvalidateContainerObjects,
 *  SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the object already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePinnedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t sze, SDF_time_t curtime, SDF_time_t exptime);

/**
 *  @brief Get a pointer to a pinned copy of an object for read/write access; create the object if necessary.   Set its expiry time.
 *
 *  Get an object and give the application a pointer to a copy of
 *  the object in SDF storage. The application needs write access
 *  to the object. Create or resize the object if necessary. If resizing
 *  is necessary, the contents of the object are undefined. The expiry
 *  time for the object is set.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param sze <IN> Size of object.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for an object.
 *  
 *  @see SDFCreatePinnedObjectWithExpiry, SDFGetForReadPinnedObjectWithExpiry,
 *  SDFGetForWritePinnedObjectWithExpiry, SDFUnpinObject, SDFInvalidateContainerObjects,
 *  SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_ALREADY_READ_PINNED: object is already read pinned by this context. 
 *          SDF_ALREADY_WRITE_PINNED: object is already write pinned by this context. 
 *          SDF_TRANS_CONFLICT: object is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetPinnedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t sze, SDF_time_t curtime, SDF_time_t exptime);

/**
 *  @brief Get a pointer to a pinned copy of an object for read-only access; return its current expiry time.
 *
 *  Get an object and give the application a pointer to a copy of
 *  the object in SDF storage. The application only intends to read
 *  the object. The current expiry time is returned.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to an
 *  object.
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  @param curtime <IN> Current time.
 *  @param pexptme <OUT> Current expiry time for an object.
 *  
 *  @see SDFCreatePinnedObjectWithExpiry, SDFSetPinnedObjectWithExpiry,
 *  SDFGetForWritePinnedObjectWithExpiry, SDFUnpinObject, SDFInvalidateContainerObjects,
 *  SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_ALREADY_READ_PINNED: object is already read pinned by this context. 
 *          SDF_ALREADY_WRITE_PINNED: object is already write pinned by this context. 
 *          SDF_TRANS_CONFLICT: object is already being used in a transaction. 
 *          SDF_EXPIRED: object has expired or been flushed . 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadPinnedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t * pactual_size, SDF_time_t curtime, SDF_time_t * pexptme);

/**
 *  @brief At the specified time, delete all objects in a  particular container.
 *
 *  At the specified time, delete all objects from the  specified
 *  container.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param curtime <IN> Current time.
 *  @param invtime <IN> Time at which a container invalidation takes effect.
 *  
 *  @see SDFCreatePinnedObjectWithExpiry, SDFSetPinnedObjectWithExpiry,
 *  SDFGetForReadPinnedObjectWithExpiry, SDFGetForWritePinnedObjectWithExpiry,
 *  SDFUnpinObject, SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_InvalidateContainerObjects(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, SDF_time_t curtime, SDF_time_t invtime);

/**
 *  @brief Get the most recent invalidation time
 *
 *  Get the invalidation time set by the most recent call to
 *  SDFInvalidateContainerObjects.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param invtime <OUT> Pointer to the variable into which to return the invalidation time.
 *  
 *  @see SDFInvalidateContainerObjects
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetContainerInvalidationTime(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, SDF_time_t *invtime);

/**
 *  @brief Create an object with an initial value, and set an expiry time.
 *
 *  Creates an object of the specified size and initializes it from
 *  an application buffer.  The expiry time is set.  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for an object.
 *  
 *  @see SDFSetBufferedObjectWithExpiry, SDFGetForReadBufferedObjectWithExpiry,
 *  SDFGetForWriteBufferedObjectWithExpiry, SDFPutBufferedObjectWithExpiry,
 *  SDFInvalidateContainerObjects, SDFRemoveObjectWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the object already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePutBufferedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime);

/**
 *  @brief Copy back an entire object that was modified,  and set an expiry time.
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer. put_all may change the size of the object. The expiry
 *  time is set.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for an object.
 *  
 *  @see SDFCreatePutBufferedObjectWithExpiry, SDFSetBufferedObjectWithExpiry,
 *  SDFGetForReadBufferedObjectWithExpiry, SDFGetForWriteBufferedObjectWithExpiry,
 *  SDFInvalidateContainerObjects, SDFRemoveObjectWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutBufferedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime);

/**
 *  @brief Copy back an entire object, creating it if necessary.  Set an expiry time.
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer. This may change the size of the object. The expiry time
 *  is set. If the object does not exist, create it and assign its
 *  value.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param sze <IN> Size of object.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for an object.
 *  
 *  @see SDFCreatePutBufferedObjectWithExpiry, SDFGetForReadBufferedObjectWithExpiry,
 *  SDFGetForWriteBufferedObjectWithExpiry, SDFPutBufferedObjectWithExpiry,
 *  SDFInvalidateContainerObjects, SDFRemoveObjectWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetBufferedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime);

/**
 *  @brief Get a copy of an object for read-only  access. Return its current expiry time.
 *
 *  Get an object and copy it into an SDF-allocated buffer. The application
 *  only intends to read the object. The current expiry time is returned.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppbf_in <IN> Variable in which to return a pointer to the object (in a buffer
 *  allocated by SDF; it must be freed by the application with a call
 *  to SDFFreeObjectBuffer).
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  @param curtime <IN> Current time.
 *  @param pexptme <OUT> Current expiry time for an object.
 *  
 *  @see SDFCreatePutBufferedObjectWithExpiry, SDFSetBufferedObjectWithExpiry,
 *  SDFGetForWriteBufferedObjectWithExpiry, SDFPutBufferedObjectWithExpiry,
 *  SDFInvalidateContainerObjects, SDFRemoveObjectWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadBufferedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppbf_in, SDF_size_t * pactual_size, SDF_time_t curtime, SDF_time_t * pexptme);

/**
 *  @brief Get a copy of an object for read-write access. Return its current expiry time.
 *
 *  Get an object, and copy it into an SDF-allocated buffer. The application
 *  intends to read and write the object. The current expiry time
 *  is returned.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param ppbf_in <IN> Variable in which to return a pointer to the object (in a buffer
 *  allocated by SDF; it must be freed by the application with a call
 *  to SDFFreeObjectBuffer).
 *  @param pactual_size <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *  @param curtime <IN> Current time.
 *  @param pexptme <OUT> Current expiry time for an object.
 *  
 *  @see SDFCreatePutBufferedObjectWithExpiry, SDFSetBufferedObjectWithExpiry,
 *  SDFGetForReadBufferedObjectWithExpiry, SDFPutBufferedObjectWithExpiry,
 *  SDFInvalidateContainerObjects, SDFRemoveObjectWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWriteBufferedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppbf_in, SDF_size_t * pactual_size, SDF_time_t curtime, SDF_time_t * pexptme);

/**
 *  @brief Delete an object, but check for expiry first.
 *
 *  Delete an object, but check for expiry first.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param objkey <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param curtime <IN> Current time.
 *  @param pref_del <IN> If 1, prefix-based delete is enabled.
 *  
 *  @see SDFCreatePutBufferedObjectWithExpiry, SDFSetBufferedObjectWithExpiry,
 *  SDFGetForReadBufferedObjectWithExpiry, SDFGetForWriteBufferedObjectWithExpiry,
 *  SDFPutBufferedObjectWithExpiry, SDFInvalidateContainerObjects
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the object does not exist. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_RemoveObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_time_t curtime, SDF_boolean_t pref_del);



/**************************************************************************
 *   
 *   Block Function Declarations (Blocking)
 *   
 **************************************************************************/

/**
 *  @brief Create a block.
 *
 *  Creates a block of the specified size.  The new block has undefined
 *  contents.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  
 *  @see SDFRemoveBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the block already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreateBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum);

/**
 *  @brief Create a block with an initial value.
 *
 *  Creates a block of the specified size and initializes it from
 *  an application buffer.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  
 *  @see SDFRemoveBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the block already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePutBufferedBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out);

/**
 *  @brief Create a block with an initial value.
 *
 *  Creates a block of the specified size and initializes it from
 *  an application shmem buffer.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pshbuf_out <IN> Shmem pointer to a data buffer from which to get write data (managed
 *  by the application).
 *  
 *  @see SDFRemoveBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the block already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePutShmemBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDFCacheObj_sp_t pshbuf_out);

/**
 *  @brief Create a pinned block of the specified size.
 *
 *  Creates a pinned block of the specified size and returns a pointer
 *  to the block. The initial contents of the block are undefined.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  
 *  @see SDFSetPinnedBlock, SDFRemoveBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the block already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePinnedBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata);

/**
 *  @brief Delete a block.
 *
 *  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  
 *  @see SDFCreateBlock, SDFCreatePutBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_RemoveBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum);

/**
 *  @brief Get a copy of a block for read-only  access.
 *
 *  Get a block and copy it into an application-provided buffer. The
 *  application only intends to read the block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param max_size <IN> Maximum number of bytes that can be put in the user buffer.
 *  
 *  @see 
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadBufferedBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_in, SDF_size_t max_size);

/**
 *  @brief Get a copy of a block for read-only  access.
 *
 *  Get a block and copy it into an application-provided shmem buffer.
 *  The application only intends to read the block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pshbf_in <OUT> Shmem pointer to a data buffer into which to copy read data (managed
 *  by the application).
 *  @param max_size <IN> Maximum number of bytes that can be put in the user buffer.
 *  
 *  @see 
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadShmemBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDFCacheObj_sp_t pshbf_in, SDF_size_t max_size);

/**
 *  @brief Get a copy of part of a block for read-only access.
 *
 *  Get a block and copy it (or a subrange of it) into an application
 *  provided buffer. The application only intends to read the block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param offset <IN> Offset of range within a block.
 *  @param nbytes <IN> Length of range within a block.
 *  
 *  @see 
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadRangeBufferedBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_in, SDF_size_t offset, SDF_size_t nbytes);

/**
 *  @brief Get a copy of a block for read-write access.
 *
 *  Get a block, and copy it into an application-provided buffer.
 *  The application  intends to read and write  the block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param max_size <IN> Maximum number of bytes that can be put in the user buffer.
 *  
 *  @see SDFPutRangeBufferedBlock, SDFPutBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWriteBufferedBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_in, SDF_size_t max_size);

/**
 *  @brief Get a copy of part of a block for read-write access.
 *
 *  Get a block and copy it (or a subrange of it) into an application
 *  provided buffer. The application intends to read and write the
 *  block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_in <IN> Pointer to application buffer into which to copy data.
 *  @param offset <IN> Offset of range within a block.
 *  @param nbytes <IN> Length of range within a block.
 *  
 *  @see SDFPutRangeBufferedBlock, SDFPutBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWriteRangeBufferedBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_in, SDF_size_t offset, SDF_size_t nbytes);

/**
 *  @brief Get a pointer to a pinned copy of a block for read-only access.
 *
 *  Get a block and give the application a pointer to a copy of the
 *  block in SDF storage. The application only intends to read the
 *  block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  
 *  @see SDFUnpinBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadPinnedBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata);

/**
 *  @brief Get a pointer to a pinned copy of a block for  read-write access.
 *
 *  Get a block and give the application a pointer to a copy of the
 *  block in SDF storage. The application will read and write the
 *  block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  
 *  @see SDFUnpinBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWritePinnedBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata);

/**
 *  @brief Get a pointer to a pinned copy of a block for read/write access; create the block if necessary.  
 *
 *  Get a block and give the application a pointer to a copy of the
 *  block in SDF storage. The application needs write access to the
 *  block. Create or resize the block if necessary. If resizing is
 *  necessary, the contents of the block are undefined.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  
 *  @see SDFUnpinBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_ALREADY_READ_PINNED: block is already read pinned by this context. 
 *          SDF_ALREADY_WRITE_PINNED: block is already write pinned by this context. 
 *          SDF_TRANS_CONFLICT: block is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetPinnedBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata);

/**
 *  @brief Copy back part of a block that was modified.
 *
 *  Update a subrange of a block from an application-provided buffer.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param offset <IN> Offset of range within a block.
 *  @param nbytes <IN> Length of range within a block.
 *  
 *  @see SDFGetForWriteBufferedBlock,  SDFGetForWriteRangeBufferedBlock,
 *  SDFSetBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_INVALID_RANGE: the range lies outside the block. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutRangeBufferedBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out, SDF_size_t offset, SDF_size_t nbytes);

/**
 *  @brief Copy back an entire block that was modified.
 *
 *  Put an entire block, with contents copied from an application-provided
 *  buffer.  put_all may change the size of the block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  
 *  @see SDFGetForWriteBufferedBlock,  SDFGetForWriteRangeBufferedBlock,
 *  SDFSetBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutBufferedBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out);

/**
 *  @brief Copy back an entire block that was modified.
 *
 *  Put an entire block, with contents copied from an application-provided
 *  shmem buffer. put_all may change the size of the block.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pshbuf_out <IN> Shmem pointer to a data buffer from which to get write data (managed
 *  by the application).
 *  
 *  @see SDFGetForWriteBufferedBlock,  SDFGetForWriteRangeBufferedBlock,
 *  SDFSetBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutShmemBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDFCacheObj_sp_t pshbuf_out);

/**
 *  @brief Unpin a block.
 *
 *  Unpin a block that was previously pinned in SDF storage by  get_to_read_pin
 *  or get_to_write_pin.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  
 *  @see SDFGetForReadPinnedBlock,  SDFGetForWritePinnedBlock, SDFSetPinnedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_NOT_PINNED: block is not pinned. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_UnpinBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum);

/**
 *  @brief Copy back an entire block that was modified;  create the block if necessary.  
 *
 *  Put an entire block, with contents copied from an  application-provided
 *  buffer. Create or resize the block if necessary.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  
 *  @see SDFGetForWriteBufferedBlock,  SDFGetForWriteRangeBufferedBlock,
 *  SDFPutBufferedBlock, SDFPutRangeBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: block is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetBufferedBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out);

/**
 *  @brief Copy back an entire block that was modified;  create the block if necessary.  
 *
 *  Put an entire block, with contents copied from an  application-provided
 *  shmem buffer. Create or resize the block if necessary.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pshbuf_out <IN> Shmem pointer to a data buffer from which to get write data (managed
 *  by the application).
 *  
 *  @see SDFGetForWriteBufferedBlock,  SDFGetForWriteRangeBufferedBlock,
 *  SDFPutBufferedBlock, SDFPutRangeBufferedBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: block is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetShmemBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDFCacheObj_sp_t pshbuf_out);

/**
 *  @brief Force modifications of a block to primary storage.
 *
 *  Flush any modified contents of a block to its backing store (as
 *  determined by its container type). For coherent containers, this
 *  is a global operation that applies to any cache or buffer in the
 *  SDF cluster. For non-coherent containers, this only applies to
 *  the local cache.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  
 *  @see SDFFlushInvalBlock, SDFFlushContainer, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_FlushBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum);

/**
 *  @brief Force modifications of a block to primary storage, and invalidate any cached copies.
 *
 *  Flush any modified contents of a block to its backing store (as
 *  determined by its container type). Invalidate all cached copies.
 *  For coherent containers, this is a global operation that applies
 *  to any cache or buffer in the SDF cluster. For non-coherent containers,
 *  this only applies to the local cache.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  
 *  @see SDFFlushBlock, SDFFlushContainer, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_FlushInvalBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum);

/**
 *  @brief Invalidate any cached copies of a block.
 *
 *  Invalidate all cached copies of a block. For coherent containers,
 *  this is a global operation that applies to any cache or buffer
 *  in the SDF cluster. For non-coherent containers, this only applies
 *  to the local cache. No dirty data is flushed to primary storage!
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  
 *  @see SDFFlushBlock, SDFFlushContainer, SDFFlushCache, SDFFlushInvalCache
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_InvalBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum);

/**
 *  @brief Lock a block for read access.
 *
 *  Place a block in a locked state, blocking if necessary. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  
 *  @see SDFLockWriteBlock, SDFTryLockReadBlock, SDFTryLockWriteBlock,
 *  SDFUnlockReadBlock, SDFUnlockWriteBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_LockReadBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum);

/**
 *  @brief Lock a block for write access.
 *
 *  Place a block in a locked state, blocking if necessary. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  
 *  @see SDFLockReadBlock, SDFTryLockReadBlock, SDFTryLockWriteBlock, SDFUnlockReadBlock,
 *  SDFUnlockWriteBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_LockWriteBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum);

/**
 *  @brief Lock a block for read access if it is currently unlocked.
 *
 *  Place a block in a locked state only if the lock is immediately
 *  available. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_success <OUT> Poinert to the variable in which to return the success or failure
 *  of the operation.
 *  
 *  @see SDFLockReadBlock, SDFLockWriteBlock, SDFTryLockWriteBlock, SDFUnlockReadBlock,
 *  SDFUnlockWriteBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_TryLockReadBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_boolean_t * p_success);

/**
 *  @brief Lock a block for write access if it is currently unlocked.
 *
 *  Place a block in a locked state only if the lock is immediately
 *  available. 
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param p_success <OUT> Poinert to the variable in which to return the success or failure
 *  of the operation.
 *  
 *  @see SDFLockReadBlock, SDFLockWriteBlock, SDFTryLockReadBlock, SDFUnlockReadBlock,
 *  SDFUnlockWriteBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_TryLockWriteBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_boolean_t * p_success);

/**
 *  @brief Unlock a block previously locked for read access.
 *
 *  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  
 *  @see SDFLockReadBlock, SDFLockWriteBlock, SDFTryLockReadBlock, SDFTryLockWriteBlock,
 *  SDFUnlockWriteBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_NOT_READ_LOCKED: this block was not read locked. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_UnlockReadBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum);

/**
 *  @brief Unlock a block previously locked for write access.
 *
 *  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  
 *  @see SDFLockReadBlock, SDFLockWriteBlock, SDFTryLockReadBlock, SDFTryLockWriteBlock,
 *  SDFUnlockReadBlock
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_NOT_WRITE_LOCKED: this block was not write locked. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_UnlockWriteBlock(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum);

/**
 *  @brief Create a pinned block of the specified size. Set the expiry time.
 *
 *  Creates a pinned block of the specified size and returns a pointer
 *  to the block. The initial contents of the block are undefined.
 *  The expiry time is set.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for a block.
 *  
 *  @see SDFSetPinnedBlockWithExpiry, SDFGetForReadPinnedBlockWithExpiry,
 *  SDFGetForWritePinnedBlockWithExpiry, SDFUnpinBlock, SDFInvalidateContainerBlocks,
 *  SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the block already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_TRANS_CONFLICT: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePinnedBlockWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata, SDF_time_t curtime, SDF_time_t exptime);

/**
 *  @brief Get a pointer to a pinned copy of a block for read/write access; create the block if necessary.   Set its expiry time.
 *
 *  Get a block and give the application a pointer to a copy of the
 *  block in SDF storage. The application needs write access to the
 *  block. Create or resize the block if necessary. If resizing is
 *  necessary, the contents of the block are undefined. The expiry
 *  time for the block is set.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for a block.
 *  
 *  @see SDFCreatePinnedBlockWithExpiry, SDFGetForReadPinnedBlockWithExpiry,
 *  SDFGetForWritePinnedBlockWithExpiry, SDFUnpinBlock, SDFInvalidateContainerBlocks,
 *  SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_ALREADY_READ_PINNED: block is already read pinned by this context. 
 *          SDF_ALREADY_WRITE_PINNED: block is already write pinned by this context. 
 *          SDF_TRANS_CONFLICT: block is already being used in a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetPinnedBlockWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata, SDF_time_t curtime, SDF_time_t exptime);

/**
 *  @brief Get a pointer to a pinned copy of a block for read-only access; return its current expiry time.
 *
 *  Get a block and give the application a pointer to a copy of the
 *  block in SDF storage. The application only intends to read the
 *  block. The current expiry time is returned.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppdata <OUT> Pointer to a user pointer in which to return the pointer to a
 *  block.
 *  @param curtime <IN> Current time.
 *  @param pexptme <OUT> Current expiry time for a block.
 *  
 *  @see SDFCreatePinnedBlockWithExpiry, SDFSetPinnedBlockWithExpiry, SDFGetForWritePinnedBlockWithExpiry,
 *  SDFUnpinBlock, SDFInvalidateContainerBlocks, SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_ALREADY_READ_PINNED: block is already read pinned by this context. 
 *          SDF_ALREADY_WRITE_PINNED: block is already write pinned by this context. 
 *          SDF_TRANS_CONFLICT: block is already being used in a transaction. 
 *          SDF_EXPIRED: block has expired or been flushed . 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadPinnedBlockWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppdata, SDF_time_t curtime, SDF_time_t * pexptme);

/**
 *  @brief At the specified time, delete all blocks in a  particular container.
 *
 *  At the specified time, delete all blocks from the  specified container.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param curtime <IN> Current time.
 *  @param invtime <IN> Time at which a container invalidation takes effect.
 *  
 *  @see SDFCreatePinnedBlockWithExpiry, SDFSetPinnedBlockWithExpiry, SDFGetForReadPinnedBlockWithExpiry,
 *  SDFGetForWritePinnedBlockWithExpiry, SDFUnpinBlock, SDFGetContainerStats2
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_InvalidateContainerBlocks(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, SDF_time_t curtime, SDF_time_t invtime);

/**
 *  @brief Create a block with an initial value, and set an expiry time.
 *
 *  Creates a block of the specified size and initializes it from
 *  an application buffer.  The expiry time is set.  
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for a block.
 *  
 *  @see SDFSetBufferedBlockWithExpiry, SDFGetForReadBufferedBlockWithExpiry,
 *  SDFGetForWriteBufferedBlockWithExpiry, SDFPutBufferedBlockWithExpiry,
 *  SDFInvalidateContainerBlocks, SDFRemoveBlockWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_EXISTS: the block already exists. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_CreatePutBufferedBlockWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime);

/**
 *  @brief Copy back an entire block that was modified,  and set an expiry time.
 *
 *  Put an entire block, with contents copied from an application-provided
 *  buffer. put_all may change the size of the block. The expiry time
 *  is set.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for a block.
 *  
 *  @see SDFCreatePutBufferedBlockWithExpiry, SDFSetBufferedBlockWithExpiry,
 *  SDFGetForReadBufferedBlockWithExpiry, SDFGetForWriteBufferedBlockWithExpiry,
 *  SDFInvalidateContainerBlocks, SDFRemoveBlockWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_PutBufferedBlockWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime);

/**
 *  @brief Copy back an entire block, creating it if necessary.  Set an expiry time.
 *
 *  Put an entire block, with contents copied from an application-provided
 *  buffer. This may change the size of the block. The expiry time
 *  is set. If the block does not exist, create it and assign its
 *  value.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param pbuf_out <IN> Pointer to application buffer from which to copy data.
 *  @param curtime <IN> Current time.
 *  @param exptime <IN> New expiry time for a block.
 *  
 *  @see SDFCreatePutBufferedBlockWithExpiry, SDFGetForReadBufferedBlockWithExpiry,
 *  SDFGetForWriteBufferedBlockWithExpiry, SDFPutBufferedBlockWithExpiry,
 *  SDFInvalidateContainerBlocks, SDFRemoveBlockWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OUT_OF_MEM: there is insufficient memory/flash. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_SetBufferedBlockWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime);

/**
 *  @brief Get a copy of a block for read-only  access. Return its current expiry time.
 *
 *  Get a block and copy it into an SDF-allocated buffer. The application
 *  only intends to read the block. The current expiry time is returned.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppbf_in <IN> Variable in which to return a pointer to the block (in a buffer
 *  allocated by SDF; it must be freed by the application with a call
 *  to SDFFreeBlockBuffer).
 *  @param curtime <IN> Current time.
 *  @param pexptme <OUT> Current expiry time for a block.
 *  
 *  @see SDFCreatePutBufferedBlockWithExpiry, SDFSetBufferedBlockWithExpiry,
 *  SDFGetForWriteBufferedBlockWithExpiry, SDFPutBufferedBlockWithExpiry,
 *  SDFInvalidateContainerBlocks, SDFRemoveBlockWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForReadBufferedBlockWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppbf_in, SDF_time_t curtime, SDF_time_t * pexptme);

/**
 *  @brief Get a copy of a block for read-write access. Return its current expiry time.
 *
 *  Get a block, and copy it into an SDF-allocated buffer. The application
 *  intends to read and write the block. The current expiry time is
 *  returned.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param ppbf_in <IN> Variable in which to return a pointer to the block (in a buffer
 *  allocated by SDF; it must be freed by the application with a call
 *  to SDFFreeBlockBuffer).
 *  @param curtime <IN> Current time.
 *  @param pexptme <OUT> Current expiry time for a block.
 *  
 *  @see SDFCreatePutBufferedBlockWithExpiry, SDFSetBufferedBlockWithExpiry,
 *  SDFGetForReadBufferedBlockWithExpiry, SDFPutBufferedBlockWithExpiry,
 *  SDFInvalidateContainerBlocks, SDFRemoveBlockWithExpiry
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_IN_TRANS: this operation cannot be done inside a transaction. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_GetForWriteBufferedBlockWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, void ** ppbf_in, SDF_time_t curtime, SDF_time_t * pexptme);

/**
 *  @brief Delete a block, but check for expiry first.
 *
 *  Delete a block, but check for expiry first.
 *
 *  @param pai <IN> The SDF context for which this operation applies.
 *  @param ctnr <IN> Identity of an open container with appropriate permissions.
 *  @param blknum <IN> Key of block to be operated on.
 *  @param curtime <IN> Current time.
 *  
 *  @see SDFCreatePutBufferedBlockWithExpiry, SDFSetBufferedBlockWithExpiry,
 *  SDFGetForReadBufferedBlockWithExpiry, SDFGetForWriteBufferedBlockWithExpiry,
 *  SDFPutBufferedBlockWithExpiry, SDFInvalidateContainerBlocks
 *  
 *  @return SDF_SUCCESS: operation completed successfully. 
 *          SDF_BAD_CONTEXT: the provided context is invalid. 
 *          SDF_CONTAINER_UNKNOWN: the container ID is invalid. 
 *          SDF_OBJECT_UNKNOWN: the block does not exist. 
 *          SDF_FAILURE: operation failed. 
 */
extern SDF_status_t SDF_I_RemoveBlockWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, uint64_t blknum, SDF_time_t curtime);


#endif
