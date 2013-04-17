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
#include "protocol/action/recovery.h"
#include "protocol/protocol_common.h"
#include "protocol/action/action_thread.h"


/*
 * Functions defined in fdf.c
 */
extern FDF_status_t fdf_get_ctnr_status(FDF_cguid_t cguid, int delete_ok);
extern int fdf_get_ctnr_from_cguid(FDF_cguid_t cguid);
extern inline void fdf_incr_io_count( FDF_cguid_t cguid );
extern inline void fdf_decr_io_count( FDF_cguid_t cguid );

/*
 * Convert a cguid to a shard.
 */
FDF_status_t
cguid_to_shard(SDF_action_init_t *pai, FDF_cguid_t cguid, shard_t **shard_ptr, int delete_ok)
{
    FDF_status_t s;
    SDF_container_meta_t meta;

    if ( (s = fdf_get_ctnr_status(cguid, delete_ok)) != FDF_CONTAINER_OPEN )
        return s;
    
    s = name_service_get_meta(pai, cguid, &meta);
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
    return FDF_SUCCESS;
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

    if ( !ts || !cguid || !iter ) {
        if ( !ts ) {
            plat_log_msg(80049,PLAT_LOG_CAT_SDF_NAMING,
                         PLAT_LOG_LEVEL_DEBUG,"FDF Thread state is NULL");
        }
        if ( !cguid ) {
            plat_log_msg(80050,PLAT_LOG_CAT_SDF_NAMING,
                  PLAT_LOG_LEVEL_DEBUG,"Invalid container cguid:%lu",cguid);
        }
        if ( !iter ) {
            plat_log_msg(80051,PLAT_LOG_CAT_SDF_NAMING,
              PLAT_LOG_LEVEL_DEBUG, "The argument FDF_iterator is NULL");
        }
        return FDF_INVALID_PARAMETER;
    }
    SDF_action_init_t *pai = (SDF_action_init_t *) ts;

    s = cguid_to_shard(pai, cguid, &shard, 0);
    if (s != FDF_SUCCESS)
        return s;

    s = enumerate_init(pai, shard, cguid, iter);
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
    uint64_t keylen64;

    if ( !ts || !iter ) {
        if ( !ts ) {
            plat_log_msg(80049,PLAT_LOG_CAT_SDF_NAMING,
                         PLAT_LOG_LEVEL_DEBUG,"FDF Thread state is NULL");
        }
        if ( !iter ) {
            plat_log_msg(80051,PLAT_LOG_CAT_SDF_NAMING,
               PLAT_LOG_LEVEL_DEBUG, "The argument FDF_iterator is NULL");
        }
        return FDF_INVALID_PARAMETER;
    }

    if ( (s = fdf_get_ctnr_status(get_e_cguid(iter), 0)) != FDF_CONTAINER_OPEN ) {
        return s;
    }
#ifdef CMAP
   	fdf_incr_io_count( get_e_cguid(iter) );
#endif /* CMAP */
    SDF_action_init_t *pai = (SDF_action_init_t *) ts;
    
    s = enumerate_next(pai, iter, key, &keylen64, data, datalen);

#ifdef CMAP
   	fdf_decr_io_count(get_e_cguid(iter));
#endif /* CMAP */

    if (s != FDF_SUCCESS)
        return s;

    *keylen = keylen64;
    return FDF_SUCCESS;
}


/*
 * Finish enumeration of a container.
 */
FDF_status_t
FDFFinishEnumeration(struct FDF_thread_state *ts, struct FDF_iterator *iter)
{
    FDF_status_t s;

    if ( !ts || !iter ) {
        if ( !ts ) {
            plat_log_msg(80049,PLAT_LOG_CAT_SDF_NAMING,
                         PLAT_LOG_LEVEL_DEBUG,"FDF Thread state is NULL");
        }
        if ( !iter ) {
            plat_log_msg(80051,PLAT_LOG_CAT_SDF_NAMING,
               PLAT_LOG_LEVEL_DEBUG, "The argument FDF_iterator is NULL");
        }
        return FDF_INVALID_PARAMETER;
    }
    s = enumerate_done((SDF_action_init_t *)ts, iter);
    if (s)
        return s;
    return FDF_SUCCESS;
}
