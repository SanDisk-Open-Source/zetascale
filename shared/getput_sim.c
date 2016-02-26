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

/*
 * File:   getput_sim.c
 * Author: Darpan Dinker
 *
 * Created on March 4, 2008, 1:13 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: getput_sim.c 10527 2009-12-12 01:55:08Z drew $
 */

#include <unistd.h> // for pread/pwrite
#include "platform/logging.h"
#include "name_service.h"
#include "getput_sim.h"

#include "flash/flash.h"

// container.h currently does a bunch of includes for the flash stuff
// in future it shouldnt, so may need to add the includes here.


string_t *
build_string_type_from_key_type(SDF_key_t key) {

    string_t *fkey;
    const char *name;

    // The string_t key should NOT include the NULL termination char!
    fkey = (string_t *) plat_alloc(4+ObjectKey_getLen(key->object_id));
    fkey->len = (int) ObjectKey_getLen(key->object_id) - 1; // Note the reduction in LEN by 1
    name = ObjectKey_getName(key->object_id);
    memcpy(&fkey->chars, name, fkey->len);

    return (fkey);
}

struct shard *
get_shard(SDF_internal_ctxt_t *pai, local_SDF_CONTAINER lc) {

    struct shard *shard = NULL;
    SDF_container_meta_t *meta;
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, lc->parent);

    if (!isContainerParentNull(lc->parent) && !ISEMPTY(lparent->name)) {
        if (name_service_get_meta(pai, lparent->name, &meta) == SDF_SUCCESS) {
            shard = meta->shard;
        }
    }
    releaseLocalContainerParent(&lparent);
    return (shard);
}

// Temporary for block containers
SDF_size_t
flashBlockRead(struct flashDev *dev, void *buff, SDF_blocknum_t blockNum, SDF_size_t blockSize) {

    SDF_size_t size = pread(dev->fd, buff, blockSize, blockNum * blockSize);

    return (size);
}

int
flashBlockWrite(struct flashDev *dev, char *buf, SDF_blocknum_t blockNum, SDF_size_t blockSize) {

    ssize_t size = pwrite(dev->fd, buf, blockSize, blockNum * blockSize);

    return (size);
}

SDF_status_t
get_object(struct shard *shard, SDF_key_t key, SDF_CACHE_OBJ *dest, SDF_size_t *size, SDF_operation_t *opid) {

    int code = 0;
    SDF_status_t status = SDF_FAILURE;
    char *data = NULL;
    local_SDF_CACHE_OBJ lo = NULL;
    char *fkey = plat_alloc(256);
    char *name = NULL;
    objMetaData_t *metaData = plat_alloc(sizeof(objMetaData_t));

    name = (char *)ObjectKey_getName(key->object_id);

    metaData->objFlags = 0;
    metaData->expTime = 0;
    metaData->createTime = 0;
    metaData->keyLen = strlen(name);
    metaData->dataLen = 0;

    memcpy(fkey, name, strlen(name));

    if ((code = flashGet(shard, metaData, (char *)fkey, &data)) == 0) {

        plat_log_msg(21588, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_TRACE,
                     "FAILURE: get_object - flashget");

    } else {
        uint32_t dataLen = *((uint32_t *) data);
        *dest = createCacheObject(dataLen);
        plat_assert(!isCacheObjectNull(*dest));
        getLocalCacheObject(&lo, *dest, dataLen);
        memcpy(lo, data, dataLen);
        releaseLocalCacheObject(&lo, dataLen);
        *size = dataLen;
        status = SDF_SUCCESS;
        plat_log_msg(21589, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_TRACE,
                     "SUCCESS: get_object - flashget");
    }

    if (fkey != NULL) {
        plat_free(fkey);
    }

    return (status);
}

SDF_status_t
put_object(struct shard *shard, SDF_key_t key, void *pbuf, SDF_size_t size, SDF_operation_t *opid) {

    SDF_status_t status = SDF_FAILURE;
    char *data = NULL;
    objMetaData_t *metaData = plat_alloc(sizeof(objMetaData_t));
    char *fkey = plat_alloc(256);
    char *name = NULL;

    name = (char *)ObjectKey_getName(key->object_id);

    metaData->objFlags = 0;
    metaData->expTime = 0;
    metaData->createTime = 0;
    metaData->keyLen = strlen(name);
    metaData->dataLen = size;

    memcpy(fkey, name, strlen(name));

    if (pbuf != NULL && (data = plat_alloc(4+size)) == NULL) {

        status = SDF_FAILURE_MEMORY_ALLOC;
        plat_log_msg(21590, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_TRACE,
                     "FAILURE: put_object - memory allocation");

    } else {

        if (pbuf != NULL) {
	    memcpy(data, pbuf, size);
	}

        if (!flashPut(shard, metaData, (char *)fkey, data)) {
            status = SDF_FAILURE_STORAGE_WRITE;
            plat_log_msg(21591, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_TRACE,
                         "FAILURE: put_object - flashPut");
        } else {
            status = SDF_SUCCESS;
            plat_log_msg(21592, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_TRACE,
                         "SUCCESS: put_object - flashPut");
        }

        if (fkey != NULL) {
            plat_free(fkey);    
        }
    }

    return (status);
}

SDF_status_t
get_block(SDF_CONTAINER c, struct shard *shard, SDF_key_t key, SDF_CACHE_OBJ *dest, SDF_size_t *size,
          SDF_operation_t *opid) {

    SDF_status_t status = SDF_FAILURE;
    local_SDF_CONTAINER lc = getLocalContainer(&lc, c);
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, lc->parent);
    struct flashDev *dev = shard->dev;
    SDF_blocknum_t blockNum = key->block_id;
    SDF_size_t blockSize = lparent->blockSize;

    releaseLocalContainer(&lc);
    releaseLocalContainerParent(&lparent);

    local_SDF_CACHE_OBJ lo = NULL;
    *dest = createCacheObject(blockSize);
    plat_assert(!isCacheObjectNull(*dest));
    getLocalCacheObject(&lo, *dest, blockSize);

    if (blockSize != flashBlockRead(dev, (void *) lo, blockNum, blockSize)) {

        status = SDF_FAILURE_STORAGE_READ;
        plat_log_msg(21593, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_TRACE,
                     "FAILURE: get_block - flashBlockRead");

    } else {
        *size = blockSize;
        status = SDF_SUCCESS;
        plat_log_msg(21594, PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE, "SUCCESS: get_block - flashBlockRead");
    }
    releaseLocalCacheObject(&lo, blockSize);
    return (status);
}

SDF_status_t
put_block(SDF_CONTAINER c, struct shard *shard, SDF_key_t key, void *pbuf, SDF_size_t size,
          SDF_operation_t *opid) {

    SDF_status_t status = SDF_FAILURE;
    local_SDF_CONTAINER lc = getLocalContainer(&lc, c);
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, lc->parent);
    struct flashDev *dev = shard->dev;
    SDF_blocknum_t blockNum = key->block_id;
    SDF_size_t blockSize = lparent->blockSize;
    char *buf = NULL;

    releaseLocalContainer(&lc);
    releaseLocalContainerParent(&lparent);

    if (pbuf == NULL) {
	buf = (char *) plat_alloc(blockSize);
	memset(buf, ' ', blockSize);
	pbuf = buf;
    }

    if (flashBlockWrite(dev, pbuf, blockNum, blockSize) != blockSize) {

        status = SDF_FAILURE_STORAGE_WRITE;
        plat_log_msg(21595, PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE, "FAILURE: put_block - flashBlockWrite");

    } else {

        status = SDF_SUCCESS;
        plat_log_msg(21596, PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE, "SUCCESS: put_block - flashBlockWrite");

    }

    return (status);
}

SDF_boolean_t
object_exists(SDF_internal_ctxt_t *pai, SDF_CONTAINER c, SDF_key_t key) {

    SDF_boolean_t exists = SDF_FALSE;
    objMetaData_t *metaData = plat_alloc(sizeof(objMetaData_t));
    char *fkey = plat_alloc(256);
    char *name = NULL;

    name = (char *)ObjectKey_getName(key->object_id);

    metaData->objFlags = 0;
    metaData->expTime = 0;
    metaData->createTime = 0;
    metaData->keyLen = strlen(name);
    metaData->dataLen = 0;

    memcpy(fkey, name, strlen(name));

    if (!isContainerNull(c) && key != NULL) {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, c);
        local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, lc->parent);

        // We only need to do the check for object containers
        if (lparent->container_type == SDF_OBJECT_CONTAINER) {
            // char buf[size];
            struct shard *shard = NULL;

            if ((shard = get_shard(pai, lc)) == NULL) {

                exists = SDF_FALSE;
                plat_log_msg(21597, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_TRACE,
                             "FAILURE: object_exists - failed to get shard");

            } else {

                if (flashGet(shard, metaData, (char *) fkey, NULL)) {

                        exists = SDF_TRUE;
                }

                if (fkey != NULL) {
                    plat_free(fkey);
                }
            }
        }
        releaseLocalContainer(&lc);
        releaseLocalContainerParent(&lparent);
    }

    return (exists);
}

SDF_status_t
put(SDF_internal_ctxt_t *pai, SDF_CONTAINER c, SDF_key_t key, void *pbuf, SDF_size_t size, SDF_operation_t *opid) {

    SDF_status_t status = SDF_FAILURE;
    struct shard *shard = NULL;
    local_SDF_CONTAINER lc = getLocalContainer(&lc, c);
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, lc->parent);

    if (isContainerNull(c) || key == NULL || pbuf == NULL || size < 0) {

        status = SDF_INVALID_PARAMETER;
        plat_log_msg(21598, PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE, "FAILURE: put - invalid parm");

    } else if ((shard = get_shard(pai, lc)) == NULL) {

        status = SDF_SHARD_NOT_FOUND;
        plat_log_msg(21599, PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE, "FAILURE: put - could not find shard");

    } else {

        switch (lparent->container_type) {

        case SDF_OBJECT_CONTAINER:

            status = put_object(shard, key, pbuf, size, opid);
            break;

        case SDF_BLOCK_CONTAINER:

            status = put_block(c, shard, key, pbuf, size, opid);
            break;

        default:
            status = SDF_FAILURE_INVALID_CONTAINER_TYPE;
            plat_log_msg(21600, PLAT_LOG_CAT_SDF_SHARED,
                         PLAT_LOG_LEVEL_ERROR, "FAILURE: put - unknown container type");
            break;
        }
    }

    releaseLocalContainer(&lc);
    releaseLocalContainerParent(&lparent);

    return (status);
}


SDF_status_t
get(SDF_internal_ctxt_t *pai, SDF_CONTAINER c, SDF_key_t key, SDF_CACHE_OBJ *dest, SDF_size_t *size, SDF_operation_t *opid) {

    SDF_status_t status = SDF_FAILURE;
    struct shard *shard = NULL;
    local_SDF_CONTAINER lc = getLocalContainer(&lc, c);
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, lc->parent);

    if (isContainerNull(c) || key == NULL) {

        status = SDF_INVALID_PARAMETER;
        plat_log_msg(21601, PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE, "FAILURE: SDFGet - invalid parm");

    } else if ((shard = get_shard(pai, lc)) == NULL) {

        status = SDF_SHARD_NOT_FOUND;
        plat_log_msg(21602, PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE, "FAILURE: SDFGet - could not find shard");

    } else {

        switch (lparent->container_type) {

        case SDF_OBJECT_CONTAINER:

            status = get_object(shard, key, dest, size, opid);
            break;

        case SDF_BLOCK_CONTAINER:

            status = get_block(c, shard, key, dest, size, opid);
            break;

        default:
            status = SDF_FAILURE_INVALID_CONTAINER_TYPE;
            plat_log_msg(21603, PLAT_LOG_CAT_SDF_SHARED,
                         PLAT_LOG_LEVEL_ERROR, "FAILURE: SDFGet - unknown container type");
            break;
        }
    }

    releaseLocalContainer(&lc);
    releaseLocalContainerParent(&lparent);

    return (status);
}


SDF_boolean_t
is_block_container(SDF_CONTAINER c) {

    SDF_boolean_t ret = SDF_FALSE;
    local_SDF_CONTAINER lc = getLocalContainer(&lc, c);
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, lc->parent);

    if (SDF_BLOCK_CONTAINER == lparent->container_type) {
        ret = SDF_TRUE;
    }
    releaseLocalContainer(&lc);
    releaseLocalContainerParent(&lparent);

    return (ret);
}

SDF_status_t
delete(SDF_internal_ctxt_t *pai, SDF_CONTAINER c, SDF_key_t key, SDF_size_t size, SDF_operation_t *opid) {

    SDF_status_t status = SDF_FAILURE;
#if 0
    struct shard *shard = NULL;
    local_SDF_CONTAINER lc = getLocalContainer(&lc, c);
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, lc->parent);

    if (isContainerNull(c) || key == NULL || size < 0) {

        status = SDF_INVALID_PARAMETER;
        plat_log_msg(21598, PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE, "FAILURE: put - invalid parm");

    } else if ((shard = get_shard(pai, lc)) == NULL) {

        status = SDF_SHARD_NOT_FOUND;
        plat_log_msg(21599, PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE, "FAILURE: put - could not find shard");

    } else {

        switch (lparent->container_type) {

        case SDF_OBJECT_CONTAINER:

            status = put_object(shard, key, pbuf, size, opid);
            break;

        case SDF_BLOCK_CONTAINER:

            status = put_block(c, shard, key, pbuf, size, opid);
            break;

        default:
            status = SDF_FAILURE_INVALID_CONTAINER_TYPE;
            plat_log_msg(21600, PLAT_LOG_CAT_SDF_SHARED,
                         PLAT_LOG_LEVEL_ERROR, "FAILURE: put - unknown container type");
            break;
        }
    }

    releaseLocalContainer(&lc);
    releaseLocalContainerParent(&lparent);
#endif
    return (status);
}

struct objDesc *
enumerate_container(struct shard *shard, struct objDesc *obj, int *hashIndex, char **key, uint32_t *len) {

    return (flashEnumerate(shard, obj, hashIndex, (char **)key));
}
