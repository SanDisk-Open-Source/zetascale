/*
 * File:   container_meta_blob.h
 * Author: Wayne Hineman
 *
 * Created on Apr 23, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_osd.h 6901 2009-04-22 02:32:36Z hiney $
 */

#ifndef __CONTAINER_META_BLOB_H__
#define __CONTAINER_META_BLOB_H__

/** @brief Maximum length of blob */
#define MCD_METABLOB_MAX_LEN  496      // 512 bytes - 8 (header) - 8 (checksum)

/** @brief Maximum number of array slots */
#define MCD_METABLOB_MAX_SLOTS  130    // includes singleton "global" blob
                                       // and the cmc itself


/** @brief Store a blob of data
 *
 * Store an opaque blob of data.
 *
 * @param shard_id <IN> ID of shard that owns this blob
 * @param data <IN> pointer to blob
 * @param len <IN> length of blob
 *
 * @return SDF_SUCCESS on successful put, SDF_FAILURE otherwise
 */
int container_meta_blob_put( uint64_t shard_id, char * data, int len );


/** @brief Get all stored blobs
 *
 * Retrieve all previously stored blobs
 *
 * @param blobs <IN> array of pointers to blobs
 * @param num_slots <IN> number of slots in array
 *
 * @return number of slots filled in
 */
int container_meta_blob_get( char * blobs[], int num_slots );

#endif  /* __CONTAINER_META_BLOB_H__ */
