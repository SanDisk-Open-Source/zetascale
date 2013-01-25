/*
 * More FDF code.
 *
 * This defines:
 *   FDFEnumerateContainerObjects()
 *   FDFNextEnumeratedObject()
 *   FDFFinishEnumeration()
 *
 * Author: Johann George
 *
 * Copyright (c) 2013 SanDisk Corporation.  All rights reserved.
 */
#include "fdf.h"
#include "shared/private.h"
#include "shared/name_service.h"
#include "protocol/protocol_common.h"
#include "protocol/action/action_thread.h"


/*
 * Functions defined in fdf.c
 */
extern int fdf_is_ctnr_open(FDF_cguid_t cguid);


/*
 * Functions defined in recovery.c
 */
extern FDF_status_t
enumerate_init(SDF_action_init_t *pai, struct shard *shard,
               FDF_cguid_t cguid, void **iter);

extern FDF_status_t
enumerate_done(SDF_action_init_t *pai, void *iter);

extern FDF_status_t
enumerate_next(SDF_action_init_t *pai, void *iter, char **key,
	       uint32_t *keylen, char **data, uint64_t *datalen);


/*
 * Convert a cguid to a shard.
 */
static FDF_status_t
cguid_to_shard(SDF_action_init_t *pai, FDF_cguid_t cguid, shard_t **shard_ptr)
{
    SDF_container_meta_t meta;

    if (!fdf_is_ctnr_open(cguid))
        return FDF_FAILURE_CONTAINER_NOT_OPEN;
    
    FDF_status_t s = name_service_get_meta(pai, cguid, &meta);
    if (s != FDF_SUCCESS)
        return s;

    flashDev_t *dev = 
    #ifdef MULTIPLE_FLASH_DEV_ENABLED
	get_flashdev_from_shardid(sdf_shared_state.config.flash_dev,
                                  meta.shard,
                                  sdf_shared_state.config.flash_dev_count);
    #else
	sdf_shared_state.config.flash_dev;
    #endif

    shard_t *shard = shardFind(dev, meta.shard);
    if (!shard)
        return FDF_SHARD_NOT_FOUND;

    *shard_ptr = shard;
    return 0;
}


/*
 * Prepare for the enumeration of all objects in a container.
 */
FDF_status_t
FDFEnumerateContainerObjects(struct FDF_thread_state *ts,
	                     FDF_cguid_t   cguid,
	                     struct FDF_iterator  **iter)
{
    FDF_status_t s;
    struct shard *shard = NULL;
    SDF_action_init_t *pai = (SDF_action_init_t *) ts;

    s = cguid_to_shard(pai, cguid, &shard);
    if (s)
        return s;

    s = enumerate_init(pai, shard, cguid, (void **)iter);
    if (s)
        return s;
    return FDF_SUCCESS;
}


/*
 * Return the next enumerated object in a container.
 */
FDF_status_t
FDFNextEnumeratedObject(struct FDF_thread_state *ts,
	                struct FDF_iterator *iter,
	                char      **key,
	                uint32_t   *keylen,
	                char      **data,
	                uint64_t   *datalen)
{
    FDF_status_t s;
    SDF_action_init_t *pai = (SDF_action_init_t *) ts;
    
    s = enumerate_next(pai, iter, key, keylen, data, datalen);
    if (s)
        return s;
    return FDF_SUCCESS;
}


/*
 * Finish enumeration of a container.
 */
FDF_status_t
FDFFinishEnumeration(struct FDF_thread_state *ts, struct FDF_iterator *iter)
{
    FDF_status_t s;
    
    s = enumerate_done((SDF_action_init_t *)ts, iter);
    if (s)
        return s;
    return FDF_SUCCESS;
}
