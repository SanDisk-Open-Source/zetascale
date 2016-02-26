/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   cmc.c
 * Author: DO
 *
 * Created on January 15, 2008, 10:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: cmc.c 12635 2010-03-30 14:52:30Z briano $
 *
 */
#include <stdio.h>
#include <sys/stat.h>
#include <dirent.h>
#include "platform/errno.h"
#include "platform/string.h"
#include "platform/stdlib.h"
#include "platform/logging.h"
#include "common/sdftypes.h"
#include "fth/fth.h"
#include "init_sdf.h"
#include "object.h"
#include "container.h"
#include "name_service.h"
#include "shard_compute.h"
#include "container_props.h"
#include "internal_blk_obj_api.h"
#include "ssd/fifo/mcd_osd.h"
#include "private.h"
#include "api/sdf_internal.h"
#include "api/fdf_internal.h"
#include "cmc.h"

#include <netinet/in.h>
#include "ssd/fifo/mcd_osd_internal.h"
#include "api/sdf_internal.h"
#include "api/fdf_internal.h"

extern SDF_cmc_t *theCMC; // Global CMC object
extern struct SDF_shared_state sdf_shared_state;
extern SDF_shardid_t generate_shard_ids(uint32_t node, SDF_cguid_t cguid, uint32_t count);

static fthLock_t cguid_lock;        // For the cguid hashmap
static fthLock_t meta_lock;         // For the metadata hashmap
static fthLock_t metamap_lock;      // For the TL metadata hashmap list

static int cmc_type = CMC_HASHMAP; 

#include "utils/properties.h"
#include "utils/hashmap.h"


#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_CMC
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_TRACE PLAT_LOG_LEVEL_TRACE
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_WARN PLAT_LOG_LEVEL_WARN
#define LOG_FATAL PLAT_LOG_LEVEL_FATAL


extern
SDF_container_meta_t * build_meta(
    const char              *path,
    SDF_container_props_t    props,
    SDF_cguid_t              cguid,
    SDF_shardid_t            shard
    );

static SDF_container_props_t *cmc_create_sdf_props(ZS_container_props_t *fdf_properties);

// Process local meta map interface - maintains version
SDF_boolean_t
cmc_metamap_update(HashMap map, const char *key, void *value);

// Process local meta map interface - no version maintenance
SDF_boolean_t
cmc_cguidmap_update(HashMap map, const char *key, void *value);

// Thread local meta map interface - no version maintenance
SDF_boolean_t
cmc_tlmap_update(HashMap map, const char *key, void *value);


// Send a delete shard message to the replication subsystem
// From container.c
extern int
delete_shard_from_service(struct SDF_shared_state *state, service_t service,
                   const char *cname, SDF_cguid_t cguid, SDF_shardid_t sguid);


// Process local maps
HashMap meta_map;			// cguid->meta

// Thread local meta map
static __thread HashMap TL_meta_map = NULL;

// List of thread local meta maps so we can keep them in sync during updates
typedef struct {
    HashMap this;
    void *next;
} cmc_map_item_t;

static cmc_map_item_t *meta_map_list = NULL;

/** Thread-local meta map list utility */
__inline__ SDF_boolean_t 
TL_meta_map_list_add(HashMap map) {

    SDF_boolean_t ret = SDF_FALSE;
    cmc_map_item_t *item = NULL;
    fthWaitEl_t *wait = NULL;

    wait = fthLock(&metamap_lock, 1, NULL);

    item = plat_alloc(sizeof(cmc_map_item_t));

    if (item != NULL) {
	item->this = map;
	item->next = NULL;

	if (meta_map_list == NULL) {
	    meta_map_list = item;
	} else {
	    cmc_map_item_t *cur = meta_map_list;
	    while(cur->next != NULL) {
		cur = cur->next;
	    }
	    cur->next = (void *)item;
	}

	ret = SDF_TRUE;
    }

    fthUnlock(wait);
    
    return (ret);
}

/** Thread-local HashMap_get() */
__inline__ void* 
TL_HashMap_get(HashMap globalMap, const char *key) {
    void *ret = NULL;
    fthWaitEl_t *wait = NULL;

    plat_log_msg(21491, LOG_CAT, LOG_TRACE,"%p - %s", fthSelf(), key);

    if (PLAT_UNLIKELY (NULL==TL_meta_map)) {

        int buckets = getProperty_Int("SDF_CMC_BUCKETS", 10000);

        plat_log_msg(21492, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, 
		     "PROP: SDF_CMC_BUCKETS=%d", buckets);

        TL_meta_map = HashMap_create(buckets, NONE);

	if (TL_meta_map_list_add(TL_meta_map) == SDF_FALSE) {

	    plat_log_msg(21493, LOG_CAT, LOG_FATAL,"Failed to update meta data map list");

	    return (NULL);
	}
    }

    if (PLAT_UNLIKELY(NULL==(ret=HashMap_get(TL_meta_map, key)))) {

        if (NULL != (ret = HashMap_get(globalMap, key))) {

	    	plat_log_msg(21494, LOG_CAT, LOG_TRACE,"%p - TL Update: %s", fthSelf(), key);

	    	cmc_tlmap_update(TL_meta_map, key, ret);

	    	// Now update all other TL meta hashmaps
	    	cmc_map_item_t *cur = meta_map_list;

	    	wait = fthLock(&metamap_lock, 1, NULL);

	    	while(cur != NULL) {
				if (cur->this != TL_meta_map) {
		    		cmc_tlmap_update(cur->this, key, ret);	    
				}
				cur = cur->next;
	    	}

	    	fthUnlock(wait);
        }
    } else {
		plat_log_msg(21495, LOG_CAT, LOG_TRACE,"%p - map %p - key %s - CACHED: %p", 
		     		fthSelf(), TL_meta_map, key, ret);
    }

    return (ret);
}


// Internal interfaces forward declarations
SDF_status_t
#ifdef SDFAPI
cmc_close_object_container(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid);
#else
cmc_close_object_container(SDF_internal_ctxt_t *pai, SDFContainer container);
#endif

SDF_status_t
cmc_get_for_read_pinned_object(SDF_internal_ctxt_t *pai,  SDFContainer container, const char *objkey,
			       SDF_size_t *destLen, void **pptr);

SDF_status_t
cmc_get_for_write_pinned_object(SDF_internal_ctxt_t *pai, SDFContainer container, const char *objkey,
				SDF_size_t *destLen, void **pptr);

SDF_status_t
cmc_unpin_object(SDF_internal_ctxt_t *pai, SDFContainer container, const char *objkey);

SDF_status_t
cmc_flush_object(SDF_internal_ctxt_t *pai, SDFContainer c, const char *objkey);

int
cmc_object_container_arg_check1(SDF_CONTAINER container, SDF_status_t *status);

int
cmc_object_container_arg_check2(SDF_CONTAINER container, const char *key, SDF_status_t *status);

SDF_cmc_t *
cmc_initialize(SDF_internal_ctxt_t *pai, const char *cmc_path);

SDF_cmc_t *
cmc_recover(SDF_internal_ctxt_t *pai, const char *cmc_path);


// CMC Init and Destroy ============================================================

SDF_cmc_t *
cmc_create(
	SDF_internal_ctxt_t *pai, 
	const char 			*cmc_path 
	) 
{
    SDF_cmc_t *cmc = NULL;

    // Init the first TL meta map
    int buckets = getProperty_Int("SDF_CMC_BUCKETS", 10000);
    plat_log_msg(21492, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, 
		 "PROP: SDF_CMC_BUCKETS=%d", buckets);

    TL_meta_map = HashMap_create(buckets, NONE);

    if (TL_meta_map_list_add(TL_meta_map) == SDF_TRUE) {

		struct SDF_shared_state *state = &sdf_shared_state;

		if (state->config.system_recovery == SYS_FLASH_RECOVERY) {
	    	plat_log_msg(21496, LOG_CAT, LOG_DBG, "********CMC RECOVER********");
	    	cmc = cmc_recover(pai, cmc_path);
		} else {
	    	plat_log_msg(21497, LOG_CAT, LOG_DBG, "********CMC INITIALIZE********");
	    	cmc = cmc_initialize(pai, cmc_path);
		}
    }

    return (cmc);
}

SDF_status_t
cmc_destroy(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc) {

    SDF_status_t status = SDF_FAILURE;

    if (cmc != NULL) {
        // plat_free(cmc->cmc_path);
#ifdef SDFAPI
	SDF_cguid_t cguid = CMC_CGUID;
        cmc_close_object_container(pai, cguid);
#else
        cmc_close_object_container(pai, cmc->c);
#endif /* SDFAPI */
        plat_free(cmc);

	if (cmc_type == CMC_HASHMAP) {
	    HashMap_destroy(meta_map); // TODO Remove all entries from HashMap
        meta_map = NULL;
	}

        status = SDF_SUCCESS;
    }

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s",  SDF_Status_Strings[status]);

    return (status);
}


/*
 * @brief Rebuild the CMC from scratch (ie, no recovery).
 *
 * @param cmc_path <IN> CMC path
 * @return Pointer to CMC structure
 */
SDF_cmc_t *
cmc_initialize(SDF_internal_ctxt_t *pai, const char *cmc_path) {

    SDF_status_t status = SDF_FAILURE;
#ifndef SDFAPI
    SDF_CONTAINER c;
    SDFContainer container = 0;
#endif /* SDFAPI */
    int log_level = LOG_ERR;

    plat_log_msg(21498, LOG_CAT, LOG_TRACE, "Node: %d", init_get_my_node_id());

    if (ISEMPTY(cmc_path)) {
		status = SDF_INVALID_PARAMETER;
    } else if ((theCMC = (SDF_cmc_t *)plat_alloc(sizeof (SDF_cmc_t))) == NULL) {
		status = SDF_FAILURE_MEMORY_ALLOC;
    } else {

		theCMC->initialized = SDF_FALSE;
		theCMC->c = 0;
		theCMC->node = CMC_HOME;

		cmc_type = getProperty_Int("SDF_META_TYPE", CMC_HASHMAP); 
		plat_log_msg(21499, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_META_TYPE=%d", cmc_type);
		if (cmc_type == CMC_HASHMAP) {
            int buckets = getProperty_Int("SDF_CMC_BUCKETS", 10000);
            plat_log_msg(21492, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_CMC_BUCKETS=%d",
                         buckets);
	    	meta_map = HashMap_create(buckets, FTH_BUCKET_RW);
		}

#ifdef SDFAPI
    	ZS_container_props_t   zs_p;
    	ZS_cguid_t             cguid       = ZS_NULL_CGUID;
		int						flags		= ZS_CTNR_CREATE;

#if 1//Rico - lc
    	memset( &zs_p, 0, sizeof zs_p);
#endif
    	// Create the CMC
    	//zs_p.fifo_mode             = ZS_FALSE;
    	zs_p.persistent            = ZS_TRUE;
    	zs_p.evicting              = ZS_FALSE;
    	zs_p.writethru             = ZS_TRUE;
    	//zs_p.num_shards            = 1;
    	zs_p.durability_level      = ZS_DURABILITY_HW_CRASH_SAFE;
    	zs_p.size_kb               = CMC_SIZE_KB; // Fixed to 1G right now
        plat_log_msg(80039,LOG_CAT, LOG_DBG, "Creating Container Metadata Container"
                          " (name = %s,size = %lu kbytes,"
                          "persistence = %s,eviction = %s,writethrough = %s,fifo = %s,"
                          "async_writes = %s,durability = %s)",
                          cmc_path,zs_p.size_kb, get_bool_str(zs_p.persistent),
                          get_bool_str(zs_p.evicting),get_bool_str(zs_p.writethru),
                          get_bool_str(zs_p.fifo_mode),get_bool_str(zs_p.async_writes),
                          get_durability_str(zs_p.durability_level));

    	if ((status = ZSOpenPhysicalContainer(pai, (char *) cmc_path, &zs_p, flags, &cguid)) != SDF_SUCCESS) {
        	plat_log_msg(150059, LOG_CAT, LOG_ERR, "Failed to create CMC container - %s\n", SDF_Status_Strings[status]);
			status = SDF_FAILURE_CONTAINER_OPEN;
    	} else { 
			// Fill in the rest of the CMC (meta has been filled in create container) 
			memcpy(&theCMC->cmc_path, cmc_path, strlen(cmc_path)); 
			log_level = LOG_TRACE; 
			status = SDF_SUCCESS;
		}
#else
	plat_assert(0);
#endif /* SDFAPI */
    }

    fthLockInit(&cguid_lock);
    fthLockInit(&meta_lock);
    fthLockInit(&metamap_lock);

    plat_log_msg(21501, LOG_CAT, log_level, "Node: %d - %s\n", 
		 init_get_my_node_id(), SDF_Status_Strings[status]);

    if (status != SDF_SUCCESS) {
        plat_free(theCMC);
        theCMC = NULL;
    }

    return (theCMC);
}

// FIXME: get rid of this
#define CMC_SHARD_ID 1
extern uint64_t generate_shard_ids(uint32_t node, SDF_cguid_t cguid, uint32_t count);

#define CGUID_COUNTER_TEMPLATE "cguid-counter-%u"

/*
 * @brief Recover the CMC from flash.
 *
 * @param cmc_path <IN> CMC path
 * @return Pointer to CMC structure
 */
SDF_cmc_t *
cmc_recover(SDF_internal_ctxt_t *pai, const char *cmc_path) {
    SDF_status_t			 status		= SDF_FAILURE;
#ifndef SDFAPI
    SDF_CONTAINER			 c;
    SDFContainer			 container	= 0;
#endif /* SDFAPI */
    SDF_container_props_t	 cmc_sdf_props;
    SDF_container_props_t	*vmc_sdf_props = NULL;
    SDF_container_props_t	*vdc_sdf_props = NULL;
    int						 log_level	= LOG_ERR;
    SDF_container_meta_t	*cmc_meta   = NULL;
    SDF_container_meta_t	*vmc_meta	= NULL;
    SDF_container_meta_t	*vdc_meta	= NULL;
    ZS_container_props_t    cmc_fdf_props;
    ZS_container_props_t    vmc_fdf_props;
    ZS_container_props_t    vdc_fdf_props;
    ZS_cguid_t              cguid       = ZS_NULL_CGUID;
    int                     flags       = 0;
    SDF_shardid_t           vmc_shard   = SDF_SHARDID_INVALID;
    SDF_shardid_t           vdc_shard   = SDF_SHARDID_INVALID;

    plat_log_msg(21498, LOG_CAT, LOG_TRACE, "Node: %d", init_get_my_node_id());

#if 1//Rico - lc
    memset( &cmc_fdf_props, 0, sizeof cmc_fdf_props);
    memset( &vmc_fdf_props, 0, sizeof vmc_fdf_props);
    memset( &vdc_fdf_props, 0, sizeof vdc_fdf_props);
#endif
    if (ISEMPTY(cmc_path)) {
		status = SDF_INVALID_PARAMETER;
        goto out;
    } else if ((theCMC = (SDF_cmc_t *)plat_alloc(sizeof (SDF_cmc_t))) == NULL) {
		status = SDF_FAILURE_MEMORY_ALLOC;
        goto out;
    } else {

		theCMC->initialized = SDF_FALSE;
		theCMC->c = 0;
		theCMC->node = CMC_HOME;

		cmc_type = getProperty_Int("SDF_META_TYPE", CMC_HASHMAP);
		plat_log_msg(21499, 
					 PLAT_LOG_CAT_PRINT_ARGS, 
					 PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_META_TYPE=%d",
                     cmc_type);
		if (cmc_type == CMC_HASHMAP) {
			int buckets = getProperty_Int("SDF_CMC_BUCKETS", 100000);
			plat_log_msg(21492, 
						 PLAT_LOG_CAT_PRINT_ARGS, 
						 PLAT_LOG_LEVEL_DEBUG, 
						 "PROP: SDF_CMC_BUCKETS=%d",
                         buckets);
		    meta_map = HashMap_create(buckets, FTH_BUCKET_RW);
		}

		memset(&cmc_sdf_props, 0, sizeof(cmc_sdf_props));
		cmc_sdf_props.container_type.type = SDF_OBJECT_CONTAINER;
		cmc_sdf_props.container_type.persistence = SDF_TRUE;
		cmc_sdf_props.container_type.caching_container = SDF_FALSE;
		cmc_sdf_props.shard.num_shards = SDF_SHARD_DEFAULT_SHARD_COUNT;

		cmc_meta = container_meta_create(cmc_path, cmc_sdf_props, CMC_CGUID, generate_shard_ids(CMC_HOME, CMC_CGUID, 1));

		init_cmc(cmc_meta);
		theCMC->initialized = SDF_TRUE;

        // Create the CMC
        cmc_fdf_props.persistent            = ZS_TRUE;
        cmc_fdf_props.evicting              = ZS_FALSE;
        cmc_fdf_props.writethru             = ZS_TRUE;
        cmc_fdf_props.durability_level      = ZS_DURABILITY_HW_CRASH_SAFE;
        cmc_fdf_props.size_kb               = CMC_SIZE_KB;
        plat_log_msg(80040,LOG_CAT, LOG_INFO, "Opening Container Metadata Container"
                          " (name = %s,size = %lu kbytes,"
                          "persistence = %s,eviction = %s,writethrough = %s,fifo = %s,"
                          "async_writes = %s,durability = %s)",
                          cmc_path,cmc_fdf_props.size_kb, get_bool_str(cmc_fdf_props.persistent),
                          get_bool_str(cmc_fdf_props.evicting),get_bool_str(cmc_fdf_props.writethru),
                          get_bool_str(cmc_fdf_props.fifo_mode),get_bool_str(cmc_fdf_props.async_writes),
                          get_durability_str(cmc_fdf_props.durability_level));
        
        if ((status = ZSOpenPhysicalContainer(pai, (char *) cmc_path, &cmc_fdf_props, flags, &cguid)) != SDF_SUCCESS) {
            plat_log_msg(150059, LOG_CAT, LOG_ERR, "Failed to create CMC container - %s\n", SDF_Status_Strings[status]);
            status = SDF_FAILURE_CONTAINER_OPEN;
        } else { 
            // Fill in the rest of the CMC (meta has been filled in create container)
            memcpy(&theCMC->cmc_path, cmc_path, strlen(cmc_path));
            log_level = LOG_TRACE;
            status = SDF_SUCCESS;
        }
	}

    // Recreate the VMC metdata. We use the existing shard.
    vmc_fdf_props.persistent            = ZS_TRUE;
    vmc_fdf_props.evicting              = ZS_FALSE;
    vmc_fdf_props.writethru             = ZS_TRUE;
    vmc_fdf_props.durability_level      = ZS_DURABILITY_HW_CRASH_SAFE;
    vmc_fdf_props.size_kb               = VMC_SIZE_KB;

    vmc_sdf_props = cmc_create_sdf_props(&vmc_fdf_props);

    vmc_shard = generate_shard_ids(init_get_my_node_id(), VMC_CGUID, 1);

    vmc_meta = container_meta_create(VMC_PATH, *vmc_sdf_props, VMC_CGUID, vmc_shard);

    memcpy(&vmc_meta->zs_properties, &vmc_fdf_props, sizeof(ZS_container_props_t));

    status = cmc_create_meta(pai, theCMC->c, VMC_PATH, VMC_CGUID, vmc_meta);
    if ( SDF_SUCCESS != status )
        goto out;

    //  Initialize the container map
    status = zs_cmap_create( VMC_PATH,
                             VMC_CGUID,  
                             vmc_meta->properties.container_id.size,
                             ZS_CONTAINER_STATE_CLOSED,
                             vmc_meta->properties.container_type.caching_container
#if 1//Rico - lc
                             ,
                             ZS_FALSE
#endif
                             );
            
    if ( SDF_SUCCESS != status ) 
        goto out;
        
    for (int i=0; i<max_num_containers; i++) {
        if (Mcd_containers[i].cguid == ZS_NULL_CGUID) {
            // this is an unused map entry  
            Mcd_containers[i].cguid         = vmc_meta->cguid;
            Mcd_containers[i].container_id  = vmc_meta->properties.container_id.container_id;
            strcpy(Mcd_containers[i].cname, vmc_meta->cname);
            break;
        }
    }
    // Recreate the VDC metdata. We use the existing shard.
    vdc_fdf_props.persistent            = ZS_TRUE;
    vdc_fdf_props.evicting              = ZS_FALSE;
    vdc_fdf_props.writethru             = ZS_TRUE;
    vdc_fdf_props.durability_level      = ZS_DURABILITY_HW_CRASH_SAFE;
    vdc_fdf_props.size_kb               = (uint64_t)(((uint64_t)getProperty_uLongLong("ZS_FLASH_SIZE", ZS_MIN_FLASH_SIZE)) *
                                          1024 * 1024 - (2 * ZS_DEFAULT_CONTAINER_SIZE_KB) - (32 * 1024));

    vdc_sdf_props = cmc_create_sdf_props(&vdc_fdf_props);

    vdc_shard = generate_shard_ids(init_get_my_node_id(), VDC_CGUID, 1);

    vdc_meta = container_meta_create(VDC_PATH, *vdc_sdf_props, VDC_CGUID, vdc_shard);

    memcpy(&vdc_meta->zs_properties, &vdc_fdf_props, sizeof(ZS_container_props_t));

    status = cmc_create_meta(pai, theCMC->c, VDC_PATH, VDC_CGUID, vdc_meta);
    if ( SDF_SUCCESS != status )
        goto out;

    //  Initialize the container map
    status = zs_cmap_create( VDC_PATH,
                             VDC_CGUID,
                             vdc_meta->properties.container_id.size,
                             ZS_CONTAINER_STATE_CLOSED,
                             vdc_meta->properties.container_type.caching_container
#if 1//Rico - lc
                             ,
                             ZS_FALSE
#endif
                             );

    if ( SDF_SUCCESS != status )
        goto out;

    for (int i=0; i<max_num_containers; i++) {
        if (Mcd_containers[i].cguid == ZS_NULL_CGUID) {
            // this is an unused map entry
            Mcd_containers[i].cguid         = vdc_meta->cguid;
            Mcd_containers[i].container_id  = vdc_meta->properties.container_id.container_id;
            strcpy(Mcd_containers[i].cname, vdc_meta->cname);
            break;
        }
    }

    fthLockInit(&cguid_lock);
    fthLockInit(&meta_lock);
    fthLockInit(&metamap_lock);

out:
    plat_log_msg(21504, LOG_CAT, log_level, "Node: %d - %s",
		 init_get_my_node_id(), SDF_Status_Strings[status]);

    if (status != SDF_SUCCESS) {
        plat_free(theCMC);
        theCMC = NULL;
    }

    return (theCMC);
}

// Container metadata  ============================================================

SDF_status_t
cmc_create_meta(SDF_internal_ctxt_t *pai, SDFContainer c, const char *cname,
		SDF_cguid_t cguid, SDF_container_meta_t *meta) {

    SDF_status_t 			 status 		= SDF_FAILURE;
    int 					 log_level 		= LOG_ERR;
    SDF_size_t 				 len 			= sizeof(SDF_container_meta_t);
    char 					 cguid_str[MAX_CGUID_STR_LEN];
    SDF_container_meta_t 	*cached 		= NULL;

    plat_log_msg(21505, LOG_CAT, LOG_TRACE, "%s - cguid: %llu\n", cname, (unsigned long long) cguid);

    if (meta == NULL) {
		status = SDF_INVALID_PARAMETER;
    } else {
		sprintf(cguid_str, "%llu", (unsigned long long) cguid);

		switch (cmc_type) {

		case CMC_HASHMAP:

	    	cached = (SDF_container_meta_t *)plat_alloc(len);
	    	if (!cached) {
				status = SDF_FAILURE_MEMORY_ALLOC;
	    	} else {
				memcpy(cached, meta, len);

				if ((cmc_metamap_update(meta_map, cguid_str, (void *) cached)) == SDF_TRUE &&
		    		(status = cmc_create_put_buffered_object(pai, 
							     		c, 
							     		cguid_str,
							     		len, 
							     		(void *)meta)) == SDF_SUCCESS) {
		    		log_level = LOG_TRACE;
				}
	    	}
	    	break;

		default:
	    	if ((status = 
		 	cmc_create_put_buffered_object(pai, c, cguid_str,
							len, (void *)meta)) == SDF_SUCCESS) {
				status = SDF_SUCCESS;
				log_level = LOG_TRACE;
	    	}
	    	break;
		}
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s",
		 (unsigned long long) cguid, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_get_meta(SDF_internal_ctxt_t *pai, SDFContainer c, SDF_cguid_t cguid, 
	     SDF_container_meta_t *meta) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_TRACE;
    fthWaitEl_t *wait = NULL;
    SDF_size_t destLen = 0;
    SDF_size_t size = sizeof(SDF_container_meta_t);
    char cguid_str[MAX_CGUID_STR_LEN];
    SDF_container_meta_t *lmeta = NULL;
    SDF_container_meta_t *cached = NULL;

    plat_log_msg(21507, LOG_CAT, LOG_TRACE, "%llu", (unsigned long long) cguid);

    // Check parameters
    if (c == NULL) {

		status = SDF_INVALID_PARAMETER;
		log_level = LOG_ERR;

    } else if (cguid == CMC_CGUID) {

        // Accessing the CMC
		memcpy(meta, &theCMC->meta, size);
		sprintf(cguid_str, "%llu", (unsigned long long) CMC_CGUID);
		status = SDF_SUCCESS;

    } else {

		sprintf(cguid_str, "%llu", (unsigned long long) cguid);

		switch (cmc_type) {

		case CMC_HASHMAP:

	    	if ((cached = TL_HashMap_get(meta_map, cguid_str)) == NULL) {

				/* Miss - fetch from flash */
				if ((status = cmc_get_for_read_buffered_object(pai, 
							       		c, 
							       		cguid_str,
							       		(void *) meta, 
							       		size, 
							       		&destLen)) == SDF_SUCCESS) {

		    		cached = plat_alloc(size);
		    		if (!cached) {
						status = SDF_FAILURE_MEMORY_ALLOC;
						bzero(meta, size);
		    		} else {
						memcpy(cached, meta, size);
						cmc_metamap_update(meta_map, cguid_str, (void *) cached);
		    		}

				} else {
		    		// Status has been set by the get buffered object call above
		    		plat_log_msg(21508, LOG_CAT, LOG_ERR, "Failed to get %s", cguid_str);
				}

	    	} else {

				memcpy(meta, cached, size);
				status = SDF_SUCCESS;
				plat_log_msg(21509, LOG_CAT, log_level, "CACHED - %llu - %s", 
			     		(unsigned long long) cguid, SDF_Status_Strings[status]);
	    	}
	    	break;

		case CMC_PIN:

	    	wait = fthLock(&meta_lock, 1, NULL);

	    	if ((status = 
		 		cmc_get_for_read_pinned_object(pai, c, cguid_str,
							&destLen, (void **) &lmeta)) == SDF_SUCCESS) {

				memcpy(meta, lmeta, destLen);
				status = cmc_unpin_object(pai, c, cguid_str);

	    	}

	    	fthUnlock(wait);
	    	break;

		case CMC_BUFFERED:

	    	status = cmc_get_for_read_buffered_object(pai, c, cguid_str,
							  (void *)meta, size, &destLen);
	    	break;

		default:

	    	plat_log_msg(21510, LOG_CAT, LOG_FATAL, "Invalid CMC_TYPE!");
	    	plat_assert(0 == 1);
	    	break;
		}
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
     		 cguid_str, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_get_meta_from_cname(SDF_internal_ctxt_t *pai, SDFContainer c, const char *cname, 
			SDF_container_meta_t *meta) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_TRACE;
    cntr_map_t *cmap = NULL;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", cname);

    if (/*!c || */ ISEMPTY(cname)) {

		status = SDF_INVALID_PARAMETER;
		log_level = LOG_ERR;

    } else if (strcmp(cname, CMC_PATH) == 0) {

		// Handle CMC
        memcpy(meta, &theCMC->meta, sizeof(SDF_container_meta_t));
		status = SDF_SUCCESS;

    } else if ((cmap = zs_cmap_get_by_cname((char *)cname)) != NULL) { 

		status = cmc_get_meta(pai, c, cmap->cguid, meta);
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s",
		 cname, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_put_meta(SDF_internal_ctxt_t *pai, SDFContainer c, SDF_cguid_t cguid,
	     SDF_container_meta_t *meta) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_ERR;
    SDF_size_t destLen = 0;
    SDF_size_t size = sizeof(SDF_container_meta_t);
    char cguid_str[MAX_CGUID_STR_LEN];
    SDF_container_meta_t *cached = NULL;
    SDF_container_meta_t *data = NULL;

    plat_log_msg(21507, LOG_CAT, LOG_TRACE, "%llu", (unsigned long long)cguid);

    if (c == NULL || meta == NULL) {
		status = SDF_INVALID_PARAMETER;
    } else {

		sprintf(cguid_str, "%llu", (unsigned long long) cguid);

		switch (cmc_type) {

			case CMC_HASHMAP:
	    		cached = (SDF_container_meta_t *)plat_alloc(size);
	    		if (!cached) {
					status = SDF_FAILURE_MEMORY_ALLOC;
	    		} else {

					memcpy(cached, meta, size);
					if ((cmc_metamap_update(meta_map, cguid_str, (void *) cached)) == SDF_TRUE &&
		    			(status = cmc_put_buffered_object(pai, c, cguid_str,
						      size, (void *)meta)) == SDF_SUCCESS) {
		    			log_level = LOG_TRACE;
					}
	    		}
	    	break;

			case CMC_PIN:
	    		if ((status = cmc_lock_meta(pai, c, cguid)) == SDF_SUCCESS &&
					(status = cmc_get_for_write_pinned_object(pai, c, cguid_str, &destLen,
						 (void **) &data)) == SDF_SUCCESS) {
					memcpy(data, meta, destLen);
					if ((status = cmc_unpin_object(pai, c, cguid_str)) == SDF_SUCCESS &&
		    			(status = cmc_unlock_meta(pai, c, cguid)) == SDF_SUCCESS) {
		    			log_level = LOG_TRACE;
					}
	    		}
	    	break;

			case CMC_BUFFERED:
	    		if ((status = cmc_put_buffered_object(pai, c, cguid_str,
					 		size, (void *)meta)) == SDF_SUCCESS) {
					log_level = LOG_TRACE;
	    		}
	    	break;

			default:
	    		plat_log_msg(21510, LOG_CAT, LOG_FATAL, "Invalid CMC_TYPE!");
	    		plat_assert(0 == 1);
	    	break;
		}
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long) cguid, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_remove_meta(SDF_internal_ctxt_t *pai, SDFContainer c, SDF_cguid_t cguid) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_ERR;
    char cguid_str[MAX_CGUID_STR_LEN];
    SDF_container_meta_t *cached = NULL;
    cmc_map_item_t *cur = meta_map_list;
    fthWaitEl_t *wait = NULL;

    plat_log_msg(21507, LOG_CAT, LOG_DBG, "%llu", (unsigned long long)cguid);

    sprintf(cguid_str, "%llu", (unsigned long long)cguid);

    switch(cmc_type) {

    case CMC_HASHMAP:

	// Remove meta from TL maps
	wait = fthLock(&metamap_lock, 1, NULL);
	while(cur != NULL && cur->this != NULL) {
	    plat_log_msg(21512, LOG_CAT, LOG_TRACE, "remove from TL %llu", (unsigned long long)cguid);
	    // We don't free the metadata until we remove it from the global map below
	    HashMap_remove(cur->this, cguid_str);
	    cur = cur->next;
	}
	fthUnlock(wait);

	// Remove meta from global map
	plat_log_msg(21513, LOG_CAT, LOG_TRACE, "remove from global map %llu", (unsigned long long)cguid);
	if ((cached = HashMap_remove(meta_map, cguid_str)) != NULL) {
	    container_meta_destroy(cached);
	}
	// Fall through to remove from flash

    default:
	plat_log_msg(21514, LOG_CAT, LOG_TRACE, "remove from flash %llu", (unsigned long long)cguid);
	if (c == NULL) {
	    status = SDF_INVALID_PARAMETER;
	} else if ((status = cmc_remove_object(pai, c,
					       cguid_str)) == SDF_SUCCESS) {
	    log_level = LOG_TRACE;
	}
	break;
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long) cguid, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_get_props(SDF_internal_ctxt_t *pai, SDFContainer c, SDF_cguid_t cguid,
	      SDF_container_props_t *props) {

    SDF_status_t status = SDF_FAILURE;
    SDF_container_meta_t meta;
    int log_level = LOG_TRACE;

    plat_log_msg(21507, LOG_CAT, LOG_TRACE, "%llu", (unsigned long long)cguid);

    if ((status = cmc_get_meta(pai, c, cguid, &meta)) == SDF_SUCCESS) {
        memcpy((void *)props, (void *)&meta.properties, sizeof(SDF_container_props_t));
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long)cguid, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_put_props(SDF_internal_ctxt_t *pai, SDFContainer c, SDF_cguid_t cguid,
	      SDF_container_props_t props) {

    SDF_status_t status = SDF_FAILURE;
    SDF_container_meta_t meta;
    int log_level = LOG_ERR;

    plat_log_msg(21507, LOG_CAT, LOG_TRACE, "%llu", (unsigned long long)cguid);
    if ((status = cmc_lock_meta(pai, c, cguid)) == SDF_SUCCESS &&
	(status = cmc_get_meta(pai, c, cguid, &meta)) == SDF_SUCCESS) {

        meta.properties = props;

	if ((status = cmc_put_meta(pai, c, cguid, &meta)) &&
	    (status = cmc_unlock_meta(pai, c, cguid)) == SDF_SUCCESS) {	    
	    log_level = LOG_TRACE;
	}
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long)cguid,SDF_Status_Strings[status]);		 

    return (status);
}

SDF_shardid_t
cmc_get_shard(SDF_internal_ctxt_t *pai, SDFContainer c, SDF_cguid_t cguid) {

    SDF_shardid_t shard = SDF_SHARDID_INVALID;
    SDF_status_t status = SDF_FAILURE;
    SDF_container_meta_t meta;
    int log_level = LOG_TRACE;

    plat_log_msg(21507, LOG_CAT, LOG_TRACE, "%llu", (unsigned long long)cguid);

    if ((status = cmc_get_meta(pai, c, cguid, &meta)) == SDF_SUCCESS) {
        shard = meta.shard;
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long)cguid, SDF_Status_Strings[status]);		 

    return (shard);
}

SDF_status_t
cmc_put_shard(SDF_internal_ctxt_t *pai, SDFContainer c, SDF_cguid_t cguid, SDF_shardid_t shard) {

    SDF_status_t status = SDF_FAILURE;
    SDF_container_meta_t meta;
    int log_level = LOG_ERR;

    plat_log_msg(21507, LOG_CAT, LOG_TRACE, "%llu", (unsigned long long)cguid);

    if ((status = cmc_lock_meta(pai, c, cguid)) == SDF_SUCCESS &&
	(status = cmc_get_meta(pai, c, cguid, &meta)) == SDF_SUCCESS) {

        meta.shard = shard;

	if ((status = cmc_put_meta(pai, c, cguid, &meta)) == SDF_SUCCESS &&
	    (status = cmc_unlock_meta(pai, c, cguid)) == SDF_SUCCESS) {	    
	    log_level = LOG_TRACE;
	}
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long)cguid, SDF_Status_Strings[status]);		 

    return (status);
}

// FIXME - this needs to be executed at the home node for each shard
SDF_status_t
cmc_delete_shards(SDF_internal_ctxt_t *pai, SDFContainer c, const char *cname) {

    struct SDF_shared_state   *state = &sdf_shared_state;
    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_ERR;
    SDF_shardid_t shard_id = SDF_SHARDID_INVALID;
    struct shard *shard = NULL;
    SDF_container_meta_t meta;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", cname);

    if ((status = cmc_get_meta_from_cname(pai, c, cname, &meta)) == SDF_SUCCESS) {

	// Find all shards and delete them
	for (int i = 0; i < state->config.shard_count; i++) {

	    shard_id = meta.shard + i;  // 1st shard id + index

            #ifdef notdef
            switch (state->config.replication_type) {
	        case SDF_REPLICATION_NONE:
		    // purposefully empty
		    break;
		default:
		    if (delete_shard_from_service(state, 
			  state->config.replication_service, 
			  cname, meta.cguid, shard_id) != 0) 
		    {
			status = SDF_SHARD_DELETE_SERVICE_FAILED;
		    }
		    break;
	    }
	    #endif

            switch (state->config.replication_type) {
	        case SDF_REPLICATION_NONE:
	        case SDF_REPLICATION_SIMPLE:
	        case SDF_REPLICATION_V1_2_WAY:
	        case SDF_REPLICATION_V1_N_PLUS_1:

		    #ifdef MULTIPLE_FLASH_DEV_ENABLED
			flashDev_t *flash_dev = NULL;

			flash_dev = get_flashdev_from_shardid(state->config.flash_dev,
							      shard_id, state->config.flash_dev_count);

			shard = shardFind(flash_dev, shard_id);
		    #else
			shard = shardFind(state->config.flash_dev, shard_id);
		    #endif
#ifdef SDFAPIONLY
/* EF: the shard has never been opened. Open and Close here to initialize shard structures which required on later shardDelete */
			if(!shard)
			{
				shardOpen(state->config.flash_dev, shard_id);
				shard = shardFind(state->config.flash_dev, shard_id);
				shardClose(shard);
			}
#endif
		    shardDelete(shard);
		    break;
		default:
		    // purposefully empty
		    break;
	    }
	}

	log_level = LOG_TRACE;
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", cname, SDF_Status_Strings[status]);		 

    return (status);
}


SDF_status_t
cmc_lock_meta(SDF_internal_ctxt_t *pai, SDFContainer c, SDF_cguid_t cguid) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_TRACE;

    plat_log_msg(21507, LOG_CAT, LOG_TRACE, "%llu", (unsigned long long)cguid);

    if (cmc_type == CMC_HASHMAP) {
		status = SDF_SUCCESS;
    } else {
		plat_log_msg(21515, LOG_CAT, LOG_ERR, "Invalid cmc_type!");
		plat_assert(0);
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long) cguid, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_unlock_meta(SDF_internal_ctxt_t *pai, SDFContainer c, SDF_cguid_t cguid) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_TRACE;

    plat_log_msg(21507, LOG_CAT, LOG_TRACE, "%llu", (unsigned long long)cguid);

    if (cmc_type == CMC_HASHMAP) {
		status = SDF_SUCCESS;
    } else {
		plat_log_msg(21515, LOG_CAT, LOG_ERR, "Invalid cmc_type!");
		plat_assert(0);
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long) cguid, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_meta_exists(SDF_internal_ctxt_t *pai, SDFContainer c, const char *cname) {

    SDF_status_t 			status 		= SDF_OBJECT_UNKNOWN;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", cname);

    if (ISEMPTY(cname)) {

		status = SDF_INVALID_PARAMETER;

    } else if (strcmp(cname, CMC_PATH) == 0) {

		if (theCMC != NULL && theCMC->initialized == SDF_TRUE) {

	    	status = SDF_SUCCESS;
		} 

    } else if ((zs_cmap_get_by_cname((char *)cname)) != NULL) {

		status = SDF_SUCCESS;

    }

    plat_log_msg(21511, LOG_CAT, LOG_DBG, "%s - %s", cname, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_cguid_exists(SDF_internal_ctxt_t *pai, SDFContainer c, SDF_cguid_t cguid) {

    SDF_status_t            status      = SDF_OBJECT_UNKNOWN;
    char 					cguid_str[MAX_CGUID_STR_LEN];
    SDF_container_meta_t    meta;

    plat_log_msg(21630, LOG_CAT, LOG_TRACE, "%lu", cguid);

    if (SDF_NULL_CGUID == cguid) {

        status = SDF_INVALID_PARAMETER;

    } else if ( CMC_CGUID == cguid) {

        if (theCMC != NULL && theCMC->initialized == SDF_TRUE) {

            status = SDF_SUCCESS;
        }
	}

	sprintf( cguid_str, "%lu", cguid );

    if ((cmc_type == CMC_HASHMAP) && (HashMap_get(meta_map, cguid_str)) != NULL) {

        status = SDF_SUCCESS;

    } else if (1 || (status = cmc_get_meta(pai, c, cguid, &meta)) != SDF_SUCCESS) {
    }

    plat_log_msg(150021, LOG_CAT, LOG_DBG, "%lu - %s", cguid, SDF_Status_Strings[status]);

    return (status);
}

SDF_cguid_t
cmc_get_cguid_from_cname(SDF_internal_ctxt_t *pai, SDFContainer c, const char *cname) {

    cntr_map_t  *cmap  = NULL;
    SDF_cguid_t  cguid = SDF_NULL_CGUID;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", cname);

    cmap = zs_cmap_get_by_cname((char *)cname);
    if (NULL != cmap)
       cguid = cmap->cguid;

    return cguid;
}

#if 0
// Internal interfaces

SDF_status_t
cmc_create_object_container(SDF_internal_ctxt_t *pai, const char *cname, 
			    SDF_container_props_t *properties) {

    SDF_status_t status = SDF_FAILURE;
#ifdef SDFAPI
    int log_level = LOG_ERR;
    SDF_cguid_t cguid;
#endif /* SDFAPI */

    plat_log_msg(20819, PLAT_LOG_CAT_SDF_CLIENT, LOG_TRACE, "%s", cname);

    if (NULL == cname) {
        status = SDF_INVALID_PARAMETER;
	plat_log_msg(30543, LOG_CAT, log_level, "NULL container name - %s", 
		     SDF_Status_Strings[status]);		 
    } else {
        properties->container_type.type = SDF_OBJECT_CONTAINER;
#ifdef SDFAPI
        if ((status = SDFCreateContainer(pai, (char *) cname, properties, &cguid)) == SDF_SUCCESS) {
#else
        if ((status = SDFCreateContainer(pai, cname, properties, 0)) == SDF_SUCCESS) {
#endif /* SDFAPI */
	    log_level = LOG_TRACE;
	}
	plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		     cname, SDF_Status_Strings[status]);		 
    }

    return (status);
}

SDF_status_t
cmc_delete_object_container(SDF_internal_ctxt_t *pai, const char *cname) {

    SDF_status_t status = SDF_FAILURE;
#ifdef notdef
    int log_level = LOG_ERR;

    plat_log_msg(20819, PLAT_LOG_CAT_SDF_CLIENT, LOG_TRACE, "%s", cname);

    if (NULL == cname) {
        status = SDF_INVALID_PARAMETER;
	plat_log_msg(30543, LOG_CAT, log_level, "NULL container name - %s", 
		     SDF_Status_Strings[status]);		 
    } else {
#ifdef SDFAPI
        if ((status = SDFDeleteContainerPath(pai, cname)) == SDF_SUCCESS) {
#else
        if ((status = SDFDeleteContainer(pai, cname)) == SDF_SUCCESS) {
#endif
	    log_level = LOG_TRACE;
	}
	plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		     cname, SDF_Status_Strings[status]);		 
    }
#endif /* notdef */
    return (status);
}
#endif

#ifdef SDFAPI
SDF_status_t 
cmc_open_object_container(
	SDF_internal_ctxt_t 	*pai, 
	SDF_cguid_t	 	 		 cguid,
	SDF_container_mode_t 	 mode
	) 
{
    SDF_status_t 	 status			= SDF_FAILURE;
    int 		 	log_level 		= LOG_ERR;

    plat_log_msg(21630, PLAT_LOG_CAT_SDF_CLIENT, LOG_TRACE, "%lu", cguid);

    if (SDF_NULL_CGUID <= cguid) {
        status = SDF_INVALID_PARAMETER;
        plat_log_msg(150020, LOG_CAT, log_level, "Invalid cguid - %s",
                     SDF_Status_Strings[status]);
    } else {
        if ((status = SDFOpenContainer(pai, cguid, mode)) == SDF_SUCCESS) {
            log_level = LOG_TRACE;
        }
        plat_log_msg(150021, LOG_CAT, log_level, "%lu - %s",
                     cguid, SDF_Status_Strings[status]);
    }

    return (status);
}
#else
SDF_CONTAINER
cmc_open_object_container(SDF_internal_ctxt_t *pai, const char *path, 
			  SDF_container_mode_t mode, SDFContainer *c) {

    SDF_CONTAINER container = containerNull;
    SDF_status_t status;
    int log_level = LOG_ERR;

    plat_log_msg(20819, PLAT_LOG_CAT_SDF_CLIENT, LOG_TRACE, "%s", path);

    if (NULL == path) {
        status = SDF_FAILURE_GENERIC;
	plat_log_msg(30544, LOG_CAT, log_level, "NULL path - %s", 
		     SDF_Status_Strings[status]);		 
    } else {
        if ((status = SDFOpenContainer(pai, path, mode, &container)) == SDF_SUCCESS) {
	    *c = internal_serverToClientContainer(container);
	    log_level = LOG_TRACE;
	}
	plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		     path, SDF_Status_Strings[status]);		 
    }

    return (container);
}
#endif /* SDFAPI */

SDF_status_t
cmc_flush_inval_object_container(SDF_internal_ctxt_t *pai, const char *path) {

    SDF_status_t status = SDF_SUCCESS;
    SDF_cguid_t cguid = SDF_NULL_CGUID;
    int log_level = LOG_ERR;


    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", path);

    if ((cguid = cmc_get_cguid_from_cname(pai, theCMC->c, (char *)path)) != SDF_NULL_CGUID &&
        (status = SDF_I_FlushInvalContainer(pai, cguid)) == SDF_SUCCESS &&
        (status = SDF_I_SyncContainer(pai, cguid)) == SDF_SUCCESS) {
        log_level = LOG_TRACE;
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s",
		 path, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_inval_object_container(SDF_internal_ctxt_t *pai, const char *path) {

    SDF_status_t status = SDF_SUCCESS;
    SDF_cguid_t cguid = SDF_NULL_CGUID;
    int log_level = LOG_ERR;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", path);

    if ((cguid = cmc_get_cguid_from_cname(pai, theCMC->c, (char *)path)) == SDF_NULL_CGUID &&
	    (status = SDF_I_InvalContainer(pai, cguid)) == SDF_SUCCESS)
    {
	    log_level = LOG_TRACE;	
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s",
		 path, SDF_Status_Strings[status]);

    return (status);
}

#ifdef SDFAPI
SDF_status_t
cmc_close_object_container(
	SDF_internal_ctxt_t *pai,
	SDF_cguid_t	    cguid
	)
{
    SDF_status_t 	 status 	= SDF_FAILURE;
    int 		 log_level 	= LOG_ERR;

    plat_log_msg(21517, PLAT_LOG_CAT_SDF_CLIENT, LOG_TRACE, "entry");

    if (SDF_NULL_CGUID <= cguid) {
	status = SDF_INVALID_PARAMETER;
    	plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);		 
    } else if ((status = SDFCloseContainer(pai, cguid)) == SDF_SUCCESS) {
	log_level = LOG_TRACE;
    }

    plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);		 

    return (status);
}
#ifdef notdef
SDF_status_t
cmc_close_object_container_path(SDF_internal_ctxt_t *pai, SDFContainer c) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_ERR;

    plat_log_msg(21517, PLAT_LOG_CAT_SDF_CLIENT, LOG_TRACE, "entry");

    if ((!cmc_object_container_arg_check1(container, &status)) &&
        ((status = SDFCloseContainerPath(pai, container)) == SDF_SUCCESS)) {
        log_level = LOG_TRACE;
    }

    plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);

    return (status);
}
#endif
#else
SDF_status_t
cmc_close_object_container(SDF_internal_ctxt_t *pai, SDFContainer c) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_ERR;

    plat_log_msg(21517, PLAT_LOG_CAT_SDF_CLIENT, LOG_TRACE, "entry");

    if ((!cmc_object_container_arg_check1(container, &status)) &&
	((status = SDFCloseContainer(pai, container)) == SDF_SUCCESS)) {
        log_level = LOG_TRACE;
    }

    plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);

    return (status);
}
#endif /* SDFAPI */

SDF_status_t
cmc_create_put_buffered_object(SDF_internal_ctxt_t *pai, SDFContainer c,
			       const char *objkey,
			       SDF_size_t size, void *pbuf) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_ERR;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", objkey);

    if (!cmc_object_container_arg_check2(container, objkey, &status)) {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
        status = SDF_I_SetBufferedObjectWithExpiry( pai, lc->cguid, objkey, strlen(objkey), size, pbuf, 0, 0);
        if (status == SDF_SUCCESS) {
	    cmc_flush_object(pai, c, objkey);
	    log_level = LOG_TRACE;
	}
        releaseLocalContainer(&lc);
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		 objkey, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_get_for_read_pinned_object(SDF_internal_ctxt_t *pai, SDFContainer c, const char *objkey,
			       SDF_size_t *destLen, void **pptr) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_TRACE;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", objkey);

    if (!cmc_object_container_arg_check2(container, objkey, &status)) {
	plat_log_msg(21515, LOG_CAT, LOG_ERR, "Invalid cmc_type!");
	plat_assert(0);
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s",
		 objkey, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_get_for_write_pinned_object(SDF_internal_ctxt_t *pai, SDFContainer c, const char *objkey,
				SDF_size_t *destLen, void **pptr) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_TRACE;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", objkey);

    if (!cmc_object_container_arg_check2(container, objkey, &status)) {
	plat_log_msg(21518, LOG_CAT, LOG_ERR, "Unsupported operation!");
	plat_assert(0);
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		 objkey, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_unpin_object(SDF_internal_ctxt_t *pai, SDFContainer c, const char *objkey) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_ERR;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", objkey);

    if (!cmc_object_container_arg_check2(container, objkey, &status)) {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
        if (SDF_SUCCESS == (status = SDF_I_UnpinObject(pai, lc->cguid, objkey, strlen(objkey)))) {
	    cmc_flush_object(pai, c, objkey);
	    log_level = LOG_TRACE;
        }
        releaseLocalContainer(&lc);
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		 objkey, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_get_for_read_buffered_object(SDF_internal_ctxt_t *pai, SDFContainer c,
				 const char *objkey, void *pbuf, SDF_size_t size,
				 SDF_size_t *destLen) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_TRACE;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", objkey);

    if (!cmc_object_container_arg_check2(container, objkey, &status)) {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
	status = SDF_I_GetForReadBufferedObject(pai, lc->cguid, objkey, 
	                                        strlen(objkey), pbuf,
						size, destLen);
        releaseLocalContainer(&lc);
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		 objkey, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_get_for_write_buffered_object(SDF_internal_ctxt_t *pai, SDFContainer c,
				  const char *objkey, void *pbuf, SDF_size_t size,
				  SDF_size_t *destLen) {
    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_TRACE;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", objkey);

    if (!cmc_object_container_arg_check2(container, objkey, &status)) {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
	// status = SDF_I_GetForWriteBufferedObject(pai, lc->cguid, objkey, pbuf,
        //  					    size, destLen);
	status = SDF_I_GetForReadBufferedObject(pai, lc->cguid, objkey, 
	                                         strlen(objkey), pbuf,
						 size, destLen);
        releaseLocalContainer(&lc);
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		 objkey, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_put_buffered_object(SDF_internal_ctxt_t *pai, SDFContainer c, const char *objkey, 
			SDF_size_t size, void *pbuf) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_ERR;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", objkey);

    if (!cmc_object_container_arg_check2(container, objkey, &status)) {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
        if (SDF_SUCCESS == (status = SDF_I_PutBufferedObject(pai, lc->cguid, 
						     objkey, strlen(objkey), 
						     pbuf, size))) {
	    	cmc_flush_object(pai, c, objkey);
	    	log_level = LOG_TRACE;
        }
        releaseLocalContainer(&lc);
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		 objkey, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_remove_object(SDF_internal_ctxt_t *pai, SDFContainer c, const char *objkey) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_ERR;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", objkey);

    if (cmc_object_container_arg_check2(container, objkey, &status)) {
	status = SDF_INVALID_PARAMETER;
    } else {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
	if (SDF_SUCCESS == (status = SDF_I_RemoveObject(pai, lc->cguid, objkey, strlen(objkey)))) {
	    log_level = LOG_TRACE;
        }
        releaseLocalContainer(&lc);
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		 objkey, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_flush_object(SDF_internal_ctxt_t *pai, SDFContainer c, const char *objkey) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_ERR;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", objkey);

    if (!cmc_object_container_arg_check2(container, objkey, &status)) {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
        if ((status = 
	     SDF_I_FlushObject(pai, lc->cguid, (char *)objkey, strlen(objkey))) == SDF_SUCCESS &&
            (status = SDF_I_SyncContainer(pai, lc->cguid)) == SDF_SUCCESS) {
	    log_level = LOG_TRACE;
	}
        releaseLocalContainer(&lc);
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		 objkey, SDF_Status_Strings[status]);

    return (status);
}


/*
 * Helper function...update a cmc hashmap entry. 
 * Handles creation and update of an entry.
 * Updates the process local map and thread local maps.
 */
SDF_boolean_t
cmc_metamap_update(HashMap map, const char *key, void *value) {

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", key);

    SDF_boolean_t status = SDF_TRUE;

    // Need to make a copy of the key as HashMap just stuffs a pointer
    char *lkey = plat_strdup(key);
    if (!lkey) {
	plat_log_msg(21519, LOG_CAT, LOG_ERR, "Failed to allocate memory for hash key");
	status = SDF_FALSE;
    }

    if (status == SDF_TRUE) {

	// We don't expect many updates so assume the entry does not exist
	if ((HashMap_put(map, lkey, value)) == SDF_FALSE) {

	    SDF_container_meta_t *old = NULL;

	    // Failed to create the entry, so try replace
	    if ((old = (SDF_container_meta_t *)HashMap_replace(map, lkey, value)) == NULL) {
		// There was no entry in the process global map...something is wrong
		plat_log_msg(21520, LOG_CAT, LOG_ERR, "Failed to replace meta hash entry");
		status = SDF_FALSE;
	    } else {

		// Update the TL copy
		plat_log_msg(21521, LOG_CAT, LOG_TRACE, "%p - REPLACE: %s - %p", 
			     fthSelf(), lkey, value);

		cmc_map_item_t *cur = meta_map_list;
		fthWaitEl_t *wait = NULL;

		wait = fthLock(&metamap_lock, 1, NULL);

		while(cur != NULL && cur->this != NULL) {
		    // We ignore the return here because the map may not have been cached in the TL_meta_map
		    HashMap_replace(cur->this, lkey, value);
		    cur = cur->next;
		}

		fthUnlock(wait);

		// Replace was successful, free the old entry
		container_meta_destroy(old);
	    }

	    // Safe to free the local copy of the key since we have not created a new hashmap entry
	    plat_free(lkey);
	} else {
	    plat_log_msg(21522, LOG_CAT, LOG_TRACE, "%p - INSERT: %s - %p", 
			 fthSelf(), lkey, value);
	}
    }

    plat_log_msg(21523, LOG_CAT, status == SDF_TRUE ? LOG_TRACE:LOG_ERR, 
		 "Status: %s", status == SDF_TRUE ? "success":"failure");

    return (status);
}

/*
 * Update the process local cguid hashmap.
 */
SDF_boolean_t
cmc_cguidmap_update(HashMap map, const char *key, void *value) {

    SDF_boolean_t status = SDF_TRUE;

    // Need to make a copy of the key as HashMap just stuffs a pointer
    char *lkey = plat_strdup(key);
    if (!lkey) {
		plat_log_msg(21519, LOG_CAT, LOG_ERR, "Failed to allocate memory for hash key");
		status = SDF_FALSE;
    }

    if (status == SDF_TRUE) {
		// We don't expect many updates so assume the entry does not exist
		if ((HashMap_put(map, lkey, value)) == SDF_FALSE) {

	    	SDF_container_meta_t *old = NULL;

	    	// Failed to create the entry, so try replace
	    	if ((old = (SDF_container_meta_t *)HashMap_replace(map, lkey, value)) == NULL) {
				// There was no entry...something is wrong
				plat_log_msg(21520, LOG_CAT, LOG_ERR, "Failed to replace meta hash entry");
				status = SDF_FALSE;
	    	} else {
				// Replace was successful, free the old entry
				plat_free(old);
	    	}

	    	// Safe to free the local copy of the key since we have not created a new hashmap entry
	    	plat_free(lkey);
		}
    }

    plat_log_msg(21523, LOG_CAT, status == SDF_TRUE ? LOG_TRACE:LOG_ERR, 
		 "Status: %s", status == SDF_TRUE ? "success":"failure");

    return (status);
}

/*
 * Update the thread local meta hashmaps.
 */
SDF_boolean_t
cmc_tlmap_update(HashMap map, const char *key, void *value) {

    SDF_boolean_t status = SDF_TRUE;

    // Need to make a copy of the key as HashMap just stuffs a pointer
    char *lkey = plat_strdup(key);
    if (!lkey) {
		plat_log_msg(21519, LOG_CAT, LOG_ERR, "Failed to allocate memory for hash key");
		status = SDF_FALSE;
    }

    if (status == SDF_TRUE) {
		// We don't expect many updates so assume the entry does not exist
		if ((HashMap_put(map, lkey, value)) == SDF_FALSE) {

	    	SDF_container_meta_t *old = NULL;

	    	// Failed to create the entry, so try replace
	    	// No need to free the old entry as it would have been freed in the global hashmap update
	    	// DANGER...fix this...
	    	if ((old = (SDF_container_meta_t *)HashMap_replace(map, lkey, value)) == NULL) {
				// There was no entry...something is wrong
				plat_log_msg(21520, LOG_CAT, LOG_ERR, "Failed to replace meta hash entry");
				status = SDF_FALSE;
	    	}

	    	// Safe to free the local copy of the key since we have not created a new hashmap entry
	    	plat_free(lkey);
		}
    }

    plat_log_msg(21523, LOG_CAT, status == SDF_TRUE ? LOG_TRACE:LOG_ERR, 
		 "Status: %s", status == SDF_TRUE ? "success":"failure");
    plat_assert(status == SDF_TRUE);

    return (status);
}

// Internal functions
int
cmc_object_container_arg_check1(SDF_CONTAINER container, SDF_status_t *status)
{
    int ret = 0; // validated

    if (isContainerNull(container)) {
        *status = SDF_FAILURE_CONTAINER_GENERIC;
        ret = 1;
    } else {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
        local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, lc->parent);
        if (SDF_OBJECT_CONTAINER != lparent->container_type) {
            *status = SDF_FAILURE_CONTAINER_GENERIC;
            ret = 1;
        }
        releaseLocalContainer(&lc);
        releaseLocalContainerParent(&lparent);
    }

    return (ret);
}

int
cmc_object_container_arg_check2(SDF_CONTAINER container, const char *key, SDF_status_t *status)
{
    int ret = 0; // validated

    if (isContainerNull(container)) {
        *status = SDF_FAILURE_CONTAINER_GENERIC;
        ret = 1;
    } else if (NULL == key) {
        *status = SDF_FAILURE_GENERIC;
        ret = 1;
    } else {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
        local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, lc->parent);
        if (SDF_OBJECT_CONTAINER != lparent->container_type) {
            *status = SDF_FAILURE_CONTAINER_GENERIC;
            ret = 1;
        }
        releaseLocalContainer(&lc);
        releaseLocalContainerParent(&lparent);
    }
    return (ret);
}

static SDF_container_props_t *cmc_create_sdf_props(
    ZS_container_props_t *fdf_properties
    )
{
    SDF_container_props_t   *sdf_properties = (SDF_container_props_t *) plat_alloc ( sizeof ( SDF_container_props_t ) );

    if ( NULL != sdf_properties ) {
        sdf_properties->container_id.owner                      = 0;
        sdf_properties->container_id.size                       = fdf_properties->size_kb;
        sdf_properties->container_id.container_id               = 0;
        //sdf_properties->container_id.sc_num_objs                 = (fdf_properties->size_kb * 1024 / Mcd_osd_blk_size);

        sdf_properties->cguid                                   = fdf_properties->cguid;

        sdf_properties->container_type.type                     = SDF_OBJECT_CONTAINER;
        sdf_properties->container_type.persistence              = SDF_TRUE /* fdf_properties->persistent */;
        sdf_properties->container_type.caching_container        = fdf_properties->evicting;
        sdf_properties->container_type.async_writes             = fdf_properties->async_writes;

        sdf_properties->replication.enabled                     = 0;
        sdf_properties->replication.type                        = SDF_REPLICATION_NONE;
        sdf_properties->replication.num_replicas                = 1;
        sdf_properties->replication.num_meta_replicas           = 0;
        sdf_properties->replication.synchronous                 = 1;

        sdf_properties->cache.not_cacheable                     = SDF_FALSE;
        sdf_properties->cache.shared                            = SDF_FALSE;
        sdf_properties->cache.coherent                          = SDF_FALSE;
        sdf_properties->cache.enabled                           = SDF_TRUE;
        sdf_properties->cache.writethru                         = fdf_properties->writethru;
        sdf_properties->cache.size                              = 0;
        sdf_properties->cache.max_size                          = 0;

        sdf_properties->shard.enabled                           = SDF_TRUE;
        sdf_properties->shard.num_shards                        = 1 /* fdf_properties->num_shards */;
        sdf_properties->fifo_mode                               = SDF_FALSE /* fdf_properties->fifo_mode */;

        sdf_properties->durability_level                        = SDF_NO_DURABILITY;

        if ( fdf_properties->durability_level == ZS_DURABILITY_HW_CRASH_SAFE )
            sdf_properties->durability_level = SDF_FULL_DURABILITY;
        else if ( fdf_properties->durability_level == ZS_DURABILITY_SW_CRASH_SAFE )
            sdf_properties->durability_level = SDF_RELAXED_DURABILITY;

        sdf_properties->flash_only                              = fdf_properties->flash_only;
        sdf_properties->cache_only                              = fdf_properties->cache_only;
        sdf_properties->compression                             = fdf_properties->compression;
        sdf_properties->flags                                   = fdf_properties->flags;
    }

    return sdf_properties;
}

