/*
 * File:   shared/container.h
 * Author: Brian O'Krafka
 *
 * Created on February 2, 2008, 1:08 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: container.h 12335 2010-03-18 20:56:46Z briano $
 */

#ifndef _CONTAINER_H
#define _CONTAINER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h> // for FILE in import and export methods
#include <stdint.h>
#include "platform/shmem.h"
#include "common/sdftypes.h"
#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif /* SDFAPI */
#include "common/sdfstats.h"
#include "flash/flash.h"
#include "utils/sdfkey.h"

/** Max container name length. Container name is a char * */
#define MAX_CNAME_SIZE                                  64

extern void *cmc_settings;

// These have been moved to utils/sdfkey.h
#if 0
typedef uint64_t SDF_blocknum_t;

PLAT_SP_VAR_OPAQUE(char_sp, char);

struct _SDF_object_key {
    uint32_t len;
    char_sp_t name;
};

PLAT_SP(SDF_object_key_sp, struct _SDF_object_key);

struct _SDF_key {
    union {
        SDF_blocknum_t block_id;    // For block containers
        SDF_object_key_sp_t object_id; // For object containers
    };
};

PLAT_SP(SDF_key_sp, struct _SDF_key);

typedef struct _SDF_key * SDF_key_t;

__inline__ SDF_key_t Key_getLocalPtr(SDF_key_t *localKey, SDF_key_sp_t key);
__inline__ void Key_releaseLocalPtr(SDF_key_t *localKey);
__inline__ void Key_setNull(SDF_key_sp_t key);
__inline__ SDF_key_sp_t Key_createObjectKey(const char *src, uint16_t len);
__inline__ void Key_freeObjectKey(SDF_key_sp_t key);
__inline__ const char * Key_getObjectName(SDF_key_sp_t key); //;
__inline__ void Key_setObjectName(SDF_key_sp_t object_id, const char *src, uint16_t len);
__inline__ uint32_t Key_getObjectLen(SDF_key_sp_t key); //;
__inline__ uint64_t Key_getBlockId(SDF_key_sp_t key);//;
__inline__ void Key_setBlockId(SDF_key_sp_t key, uint64_t block_id);

__inline__ const char *ObjectKey_getName(SDF_object_key_sp_t object_id);
__inline__ uint32_t ObjectKey_getLen(SDF_object_key_sp_t object_id);

__inline__ SDF_key_sp_t Key_createBlockKey(uint64_t block_id);
__inline__ void Key_freeBlockKey(SDF_key_sp_t key);

#endif

/**
 * Example, used in SDFGetForRead() and SDFGetForUpdate
 */
typedef enum {
    SDF_GET_FOR_READ, SDF_GET_FOR_UPDATE
} SDF_object_get_mode_t;

/** @brief Argument to #build_shard */
enum build_shard_type {
    BUILD_SHARD_CMC,
    BUILD_SHARD_OTHER
};

enum {
    SDF_SHARD_QUOTA_MAX = 0xffffffffffffffff,
};

enum {
    NODE_MULTIPLIER = (1<<20) //easier to read logged values in hex
};

#ifndef SDFAPI
struct _SDF_container_parent;
PLAT_SP(_SDF_container_parent_sp, struct _SDF_container_parent);

struct _SDF_container;
PLAT_SP(_SDF_container_sp, struct _SDF_container);

PLAT_SP_VAR_OPAQUE(DescrChangesPtr_sp, void);

#ifndef SDFCACHEOBJ_SP
#define SDFCACHEOBJ_SP
PLAT_SP_VAR_OPAQUE(SDFCacheObj_sp, void);
#endif

/**
 * Each unique open container has _SDF_container_parent structures. Each open
 * container creates a new _SDF_container, that has a pointer to the parent.
 *
 * Need to be cautious about space, so using bit fields.
 */
struct _SDF_container_parent {
    SDF_vnode_t node;
    SDF_cguid_t cguid;
    uint64_t container_id;
    uint32_t num_shards;
    unsigned container_type: 4; // allows 16 container types
    unsigned blockSize;
    char *dir;
    char *name;
    unsigned num_open_descriptors;
    SDF_boolean_t delete_pending;
    _SDF_container_parent_sp_t bucket_next;
    _SDF_container_sp_t open_containers;
};

/**
 * Container handle provided with each SDFOpenContainer() call.
 */
struct _SDF_container {
    _SDF_container_parent_sp_t parent;
    _SDF_container_sp_t next;
    DescrChangesPtr_sp_t ptr; // TODO pointer to list of block/object updates, etc.
    SDF_container_mode_t mode;
    SDF_vnode_t node; // Copied from parent for convenience
    SDF_cguid_t cguid; // Copied from parent for convenience
    uint64_t container_id;
    uint32_t num_shards; // Copied from parent for convenience
    unsigned blockSize;
    SDF_object_info_t **info;
    uint32_t info_index;

    // TODO add "access permission"
};

typedef _SDF_container_parent_sp_t SDF_CONTAINER_PARENT;
typedef _SDF_container_sp_t SDF_CONTAINER;
typedef SDFCacheObj_sp_t SDF_CACHE_OBJ;

typedef struct _SDF_container_parent *local_SDF_CONTAINER_PARENT;
typedef struct _SDF_container *local_SDF_CONTAINER;
typedef void * local_SDF_CACHE_OBJ;

SDF_CONTAINER createContainer();
void freeContainer(SDF_CONTAINER c);
__inline__ local_SDF_CONTAINER getLocalContainer(local_SDF_CONTAINER *lc, SDF_CONTAINER c);
__inline__ void releaseLocalContainer(local_SDF_CONTAINER *lc);
__inline__ int isContainerNull(SDF_CONTAINER c);
__inline__ int containerPtrEqual(SDF_CONTAINER c1, SDF_CONTAINER c2);
#define containerNull _SDF_container_sp_null

SDF_CONTAINER_PARENT createContainerParent();
void freeContainerParent(SDF_CONTAINER_PARENT p);
__inline__ local_SDF_CONTAINER_PARENT getLocalContainerParent(local_SDF_CONTAINER_PARENT *lp, SDF_CONTAINER_PARENT p);
__inline__ void releaseLocalContainerParent(local_SDF_CONTAINER_PARENT *lp);
__inline__ int isContainerParentNull(SDF_CONTAINER_PARENT p);
#define containerParentNull _SDF_container_parent_sp_null

__inline__ SDF_CACHE_OBJ createCacheObject(size_t size);
__inline__ void freeCacheObject(SDF_CACHE_OBJ o, size_t size);
__inline__ local_SDF_CACHE_OBJ getLocalCacheObject(local_SDF_CACHE_OBJ *lo, SDF_CACHE_OBJ o, size_t size);
__inline__ void releaseLocalCacheObject(local_SDF_CACHE_OBJ *lo, size_t size);
__inline__ int isCacheObjectNull(SDF_CACHE_OBJ o);
__inline__ int cacheObjectPtrEqual(SDF_CACHE_OBJ o1, SDF_CACHE_OBJ o2);
#define cacheObjectNull SDFCacheObj_sp_null

#define descrChangesNull DescrChangesPtr_sp_null
#endif /* SDFAPI */
// ===================================================================

SDF_CONTAINER internal_clientToServerContainer(SDFContainer clientContainer);

SDFContainer internal_serverToClientContainer(SDF_CONTAINER serverContainer);
// ===================================================================
void print_sm_stats(struct plat_shmem_alloc_stats init, struct plat_shmem_alloc_stats end);

#ifndef SDFAPI
/**
 * @brief Create a container.
 *
 * @param path <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @param properties <IN> container properties
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFCreateContainer(SDF_internal_ctxt_t *pai, const char *path, SDF_container_props_t properties, int64_t cntr_id);

/**
 * @brief Delete a container (by name).
 *
 * @param path <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFDeleteContainer(SDF_internal_ctxt_t *pai, const char *path);
#endif /* SDFAPI */

/**
 * @brief Delete a container (by cguid).
 *
 * @param cguid <IN> container global identifier
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFDeleteContainerByCguid(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid);

/**
 * @brief Change caching mode for a container
 *
 * @param cguid <IN> container global identifier
 * @param enable_writeback <IN> if SDF_TRUE, use writeback caching, otherwise use writethru caching
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFChangeContainerWritebackMode(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, SDF_boolean_t enable_writeback);

#ifndef SDFAPI
/**
 * @brief Start accepting accesses to a container (by cguid).
 *
 * @param cguid <IN> container global identifier
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFStartContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid);

/**
 * @brief Stop accepting accesses to a container (by cguid).
 *
 * @param cguid <IN> container global identifier
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFStopContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid);

/**
 * @brief Get container properties by cguid.
 *
 * @param cguid  <IN> container global identifier
 * @param pprops <IN> pointer to structure into which to copy properties
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFGetContainerProps(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, SDF_container_props_t *pprops) ;

/**
 * @brief Set container properties by cguid.
 *
 * @param cguid  <IN> container global identifier
 * @param pprops <IN> pointer to structure into which to copy properties
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFSetContainerProps(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, SDF_container_props_t *pprops) ;
#endif /* SDFAPI */

/**
 * @brief Copy a container from source to destination.
 *
 * @param srcPath <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @param destPath <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFCopyContainer(SDF_internal_ctxt_t *pai, const char *srcPath, const char *destPath);

/**
 * @brief Move a container from source to destination.
 *
 * @param srcPath <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @param destPath <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFMoveContainer(SDF_internal_ctxt_t *pai, const char *srcPath, const char *destPath);

/**
 * @brief Rename a container.
 *
 * @param path <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @param newName <IN> new name of the container
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFRenameContainer(SDF_internal_ctxt_t *pai, const char *path, const char *newName);

/**
 * @brief List containers in a directory.
 * <BR>TODO where to list the containers - STDOUT?
 * @param dirPath <IN> directory path specified in similar ways as a File system
 * directory.
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFListContainers(SDF_internal_ctxt_t *pai, const char *dirPath);

// ===================================================================

/*
 * @brief Export the contents of the container to a file.
 *
 * @param sdfPath <IN> directory path and name of the container to be exported.
 * @param destFile <IN> file to export data to.
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFExportContainer(SDF_internal_ctxt_t *pai, const char *sdfPath, FILE *destFile);

/*
 * @brief Import the contents of a file to the container.
 *
 * @param sdfPath <IN> directory path and name of the container to import into.
 * @param srcFile <IN> file to import data from.
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFImportContainer(SDF_internal_ctxt_t *pai, const char *sdfPath, FILE *srcFile);

// ===================================================================

#ifndef SDFAPI
/**
 * @brief Open a container.
 *
 * @param path <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @param mode <IN> mode to open the container in (e.g. read/write/append/read-write)
 * @param container <OUT> pointer to the container handle
 * @see SDF_container_mode_t
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFOpenContainer(SDF_internal_ctxt_t *pai, const char *path, SDF_container_mode_t mode, SDF_CONTAINER *container);

/**
 * @brief Close a container.
 *
 * @param container <IN> handle to the container
 * @return SDF_SUCCESS on success
 */
SDF_status_t SDFCloseContainer(SDF_internal_ctxt_t *pai, SDF_CONTAINER container);
#endif /* SDFAPI */

int
validateContainerType(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, SDF_container_type_t type);

/**
 * @brief Enumerate a container's objects.
 * <BR>Blocking operation
 *
 * @param container <IN> handle to the container
 * @param blob <OUT> returned list of object identifiers in serialized format
 * @param len <OUT> length of blob
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFEnumerateContainer(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, char_sp_t *blob, uint32_t *len);

/**
 * @brief Check the existence of a container.
 *
 * @param path <IN> directory path and name of the container specified in
 * similar ways as a file.
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFDoesContainerExist(SDF_internal_ctxt_t *pai, const char *path);

/**
 * @brief Get container statistics (and serialize using the container lock).
 *
 * @param container <IN> handle to the container
 * similar ways as a file.
 * @param stat <IN> The statistic to retrieve.
 * @param data <IN> The return statistic.
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFContainerStat(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, int key, uint64_t *stat);

/**
 * @brief Get container statistics (without serializing on the container lock).
 *
 * @param container <IN> handle to the container
 * similar ways as a file.
 * @param stat <IN> The statistic to retrieve.
 * @param data <IN> The return statistic.
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFContainerStatInternal(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, int key, uint64_t *stat);

/**
 * @brief Get container shard pointers.
 *
 * @param container <IN> handle to the container
 * @param max_shards <IN> max size of the array
 * @param sahrds <OUT> the return shards pointers
 * @param shard_count <OUT> number of shard pointers returned (<= max_shards)
 * @return SDF_SUCCESS on success
 */
SDF_status_t
SDFContainerShards(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, uint32_t max_shards, struct shard * shards[], uint32_t * shard_count);

/**
 * @brief Serialize the following container operation
 *
 * @param pai <IN> pointer to SDF context
 */
extern void SDFStartSerializeContainerOp(SDF_internal_ctxt_t *pai);

/**
 * @brief End serialization of a container operation
 *
 * @param pai <IN> pointer to SDF context
 */
extern void SDFEndSerializeContainerOp(SDF_internal_ctxt_t *pai);

/**
 * @brief Recovery code interface to enumerate cached, modified objects.
 *
 * @param pai              <IN> pointer to SDF context
 * @param SDF_cache_enum_t <IN> pointer to enumeration structure
 */
extern SDF_status_t SDFGetModifiedObjects(SDF_internal_ctxt_t *pai, SDF_cache_enum_t *pes);

/**
 * Print the container's parent structure.
 */
void
_sdf_print_parent_container_structure(SDF_CONTAINER_PARENT parent);

/**
 * Print the container's descriptor and it's parent descriptor.
 */
void
_sdf_print_container_descriptor(SDF_CONTAINER container);

/** @brief Return non-zero if cguid may be valid */
int _sdf_cguid_valid(SDF_cguid_t cguid);

/** @brief Return non-zero if cguid may be valid */
int _sdf_shardid_valid(SDF_shardid_t shard);

int cguid_to_id(SDF_cguid_t cguid);

#ifdef __cplusplus
}
#endif

#endif /* _CONTAINER_H */
