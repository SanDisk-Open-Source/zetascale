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
#include "fth/fthLock.h"
#include "init_sdf.h"
#include "object.h"
#include "container.h"
#include "name_service.h"
#include "shard_compute.h"
#include "container_props.h"
#include "internal_blk_obj_api.h"
#include "private.h"
#include "cmc.h"

extern SDF_cmc_t *theCMC; // Global CMC object
extern struct SDF_shared_state sdf_shared_state;

static fthLock_t cguid_lock;        // For the cguid hashmap
static fthLock_t meta_lock;         // For the metadata hashmap
static fthLock_t metamap_lock;      // For the TL metadata hashmap list

static int cmc_type = 0; 

#include "utils/properties.h"
#include "utils/hashmap.h"


#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_CMC
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_WARN PLAT_LOG_LEVEL_WARN
#define LOG_FATAL PLAT_LOG_LEVEL_FATAL


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
HashMap meta_map;
HashMap cguid_map;

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

    plat_log_msg(21491, LOG_CAT, LOG_DBG,"%p - %s", fthSelf(), key);

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

	    plat_log_msg(21494, LOG_CAT, LOG_DBG,"%p - TL Update: %s", fthSelf(), key);

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
	plat_log_msg(21495, LOG_CAT, LOG_DBG,"%p - map %p - key %s - CACHED: %p", 
		     fthSelf(), TL_meta_map, key, ret);
    }

    return (ret);
}


// Internal interfaces forward declarations
SDF_status_t
cmc_close_object_container(SDF_internal_ctxt_t *pai, SDFContainer container);

SDF_status_t
cmc_get_for_read_pinned_object(SDF_internal_ctxt_t *pai,  SDFContainer container, const char *objkey,
			       SDF_size_t *destLen, void **pptr);

SDF_status_t
cmc_get_for_write_pinned_object(SDF_internal_ctxt_t *pai, SDFContainer container, const char *objkey,
				SDF_size_t *destLen, void **pptr);

SDF_status_t
cmc_unpin_object(SDF_internal_ctxt_t *pai, SDFContainer container, const char *objkey);

SDF_status_t
cmc_put_cguid_map(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, SDF_cguid_t cguid, const char *cname);

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
cmc_create(SDF_internal_ctxt_t *pai, const char *cmc_path) {

    SDF_cmc_t *cmc = NULL;

    // Init the first TL meta map
    int buckets = getProperty_Int("SDF_CMC_BUCKETS", 100000);
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
        cmc_close_object_container(pai, cmc->c);
        plat_free(cmc);

	if (cmc_type == CMC_HASHMAP) {
	    HashMap_destroy(meta_map); // TODO Remove all entries from HashMap
	    HashMap_destroy(cguid_map);
            cguid_map = meta_map = NULL;
	}

        status = SDF_SUCCESS;
    }

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s",  SDF_Status_Strings[status]);

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
    SDF_CONTAINER c;
    SDFContainer container = 0;
    SDF_container_props_t p;
    int log_level = LOG_ERR;

    plat_log_msg(21498, LOG_CAT, LOG_DBG, "Node: %d", init_get_my_node_id());

    if (ISEMPTY(cmc_path)) {
	status = SDF_INVALID_PARAMETER;
    } else if ((theCMC = (SDF_cmc_t *)plat_alloc(sizeof (SDF_cmc_t))) == NULL) {
	status = SDF_FAILURE_MEMORY_ALLOC;
    } else {

	theCMC->initialized = SDF_FALSE;
	theCMC->c = 0;
	theCMC->node = CMC_HOME;

	cmc_type = getProperty_Int("SDF_META_TYPE", CMC_HASHMAP);
        plat_log_msg(21499, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_META_TYPE=%d",
                     cmc_type);
	if (cmc_type == CMC_HASHMAP) {
            int buckets = getProperty_Int("SDF_CMC_BUCKETS", 100000);
            plat_log_msg(21492, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_CMC_BUCKETS=%d",
                         buckets);
	    meta_map = HashMap_create(buckets, FTH_BUCKET_RW);

	    cguid_map = HashMap_create(buckets, FTH_BUCKET_RW);
	}

        /*
         * XXX: drew 2008-07-01 This is probably incorrect.  We probably want
         * some form of default initializer.
         */
	memset(&p, 0, sizeof(p));
	p.container_type.type = SDF_OBJECT_CONTAINER;
        p.container_type.persistence = SDF_TRUE;
	p.container_type.caching_container = SDF_FALSE;
	p.shard.num_shards = SDF_SHARD_DEFAULT_SHARD_COUNT;
	p.cache.writethru = SDF_TRUE;
	#ifdef ENABLE_MULTIPLE_FLASH_SUBSYSTEMS
	    p.container_id.size = 1024; // kB
	#endif
	if ((status = cmc_create_object_container(pai, cmc_path, p)) == SDF_SUCCESS) {
	    c = cmc_open_object_container(pai, cmc_path, SDF_READ_WRITE_MODE, &container);
	    if (isContainerNull(c)) {
		status = SDF_FAILURE_CONTAINER_OPEN;
	    } else {
		// Fill in the rest of the CMC (meta has been filled in create container)
		memcpy(&theCMC->cmc_path, cmc_path, strlen(cmc_path));
		theCMC->c = container;
		log_level = LOG_DBG;
	    }
        } else {
            plat_log_msg(21500, LOG_CAT, LOG_DBG, 
                         "\nNode : Failed status = %d...\n", status);
        }

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

static void blob_version_check(uint32_t version)
{
    if (version != SDF_BLOB_CONTAINER_META_VERSION) {
	plat_log_msg(21502, LOG_CAT, LOG_ERR, 
	    "Incompatible version of SDF_container_meta_blob_t\n");
	plat_assert(0);
    }
}

static void ctnr_meta_version_check(uint32_t version)
{
    if (version != SDF_CONTAINER_META_VERSION) {
	plat_log_msg(21503, LOG_CAT, LOG_ERR, 
	    "Incompatible version of SDF_container_meta_t\n");
	plat_assert(0);
    }
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
    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER c;
    SDFContainer container = 0;
    SDF_container_props_t p;
    int log_level = LOG_ERR;
    SDF_container_meta_t *meta = NULL;
    struct SDF_shared_state *state = &sdf_shared_state;

    plat_log_msg(21498, LOG_CAT, LOG_DBG, "Node: %d", init_get_my_node_id());

    if (ISEMPTY(cmc_path)) {
	status = SDF_INVALID_PARAMETER;
    } else if ((theCMC = (SDF_cmc_t *)plat_alloc(sizeof (SDF_cmc_t))) == NULL) {
	status = SDF_FAILURE_MEMORY_ALLOC;
    } else {

	theCMC->initialized = SDF_FALSE;
	theCMC->c = 0;
	theCMC->node = CMC_HOME;

	cmc_type = getProperty_Int("SDF_META_TYPE", CMC_HASHMAP);
        plat_log_msg(21499, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_META_TYPE=%d",
                     cmc_type);
	if (cmc_type == CMC_HASHMAP) {
            int buckets = getProperty_Int("SDF_CMC_BUCKETS", 100000);
            plat_log_msg(21492, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_CMC_BUCKETS=%d",
                         buckets);
	    meta_map = HashMap_create(buckets, FTH_BUCKET_RW);
	    cguid_map = HashMap_create(buckets, FTH_BUCKET_RW);
	}

	memset(&p, 0, sizeof(p));
	p.container_type.type = SDF_OBJECT_CONTAINER;
        p.container_type.persistence = SDF_TRUE;
	p.container_type.caching_container = SDF_FALSE;
	p.shard.num_shards = SDF_SHARD_DEFAULT_SHARD_COUNT;

	// FIXME...enable this when CMC is replicating
	if (0 && state->config.always_replicate) {
	    p.replication.enabled = 1;
	    p.replication.type = SDF_REPLICATION_SIMPLE;
	    p.replication.num_replicas = state->config.always_replicate;
	    p.replication.num_meta_replicas = state->config.always_replicate;
	    p.replication.synchronous = 1;
	}

	meta = container_meta_create(cmc_path, p, CMC_CGUID, generate_shard_ids(CMC_HOME, CMC_CGUID, 1));
	init_cmc(meta);
        theCMC->initialized = SDF_TRUE;
	c = cmc_open_object_container(pai, cmc_path, SDF_READ_WRITE_MODE, &container);
	if (isContainerNull(c)) {
	    status = SDF_FAILURE_CONTAINER_OPEN;
	} else {
	    memcpy(&theCMC->cmc_path, cmc_path, strlen(cmc_path));
	    theCMC->c = container;
	    log_level = LOG_DBG;
	    status = SDF_SUCCESS;
	}
    }

    // Recovery hack - we must rebuild the CMC from metadata kept in shard...
    // This value must match the one in container_meta_blob.h
#define MCD_METABLOB_MAX_SLOTS  130
    char *blobs[MCD_METABLOB_MAX_SLOTS];
    int num_blobs = 0;

    if (status == SDF_SUCCESS &&

 	init_container_meta_blob_get != NULL) {

	num_blobs = init_container_meta_blob_get(blobs, MCD_METABLOB_MAX_SLOTS);

	for (int i = 0; i < num_blobs && status == SDF_SUCCESS; i++) {

	    if (i == 0) {

		// First entry is the global cguid counter
		SDF_cguid_state_t state;
		char name[MAX_OBJECT_ID_SIZE];
		SDF_container_meta_blob_t *blob = (SDF_container_meta_blob_t *) blobs[0];
		blob_version_check(blob->version);
	    
		state.type = SDF_META_TYPE_CGUID_STATE;
		memcpy( (char *)&state.cguid_counter, &(blob->meta), sizeof(state.cguid_counter) );
 
		snprintf(name, sizeof(name), CGUID_COUNTER_TEMPLATE, init_get_my_node_id());
	    
		status = cmc_create_put_buffered_object(pai, 
							theCMC->c, 
							name, 
							sizeof(SDF_cguid_state_t), 
							&state);
	    } else {

		SDF_container_meta_blob_t *blob = (SDF_container_meta_blob_t *) blobs[i];
		blob_version_check(blob->version);

		// Bypass the CMC blob...we re-create it in the SDF globals...
		if (blob->meta.cguid != CMC_CGUID) {
		    ctnr_meta_version_check(blob->meta.version);

		    status = cmc_create_meta(pai,
					     theCMC,
					     blob->meta.cname,
					     blob->meta.cguid,
					     &blob->meta);

		    if (status == SDF_SUCCESS) {
			status = cmc_create_cguid_map(pai,
						      theCMC,
						      blob->meta.cguid,
						      blob->meta.cname);
		    }
		}
	    }
	}
    }

    fthLockInit(&cguid_lock);
    fthLockInit(&meta_lock);
    fthLockInit(&metamap_lock);

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
cmc_create_meta(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, const char *cname,
		SDF_cguid_t cguid, SDF_container_meta_t *meta) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_ERR;
    SDF_size_t len = sizeof(SDF_container_meta_t);
    char cguid_str[MAX_CGUID_STR_LEN];
    SDF_container_meta_t *cached = NULL;

    plat_log_msg(21505, LOG_CAT, LOG_DBG, "%s - cguid: %llu\n", cname, (unsigned long long) cguid);

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
							     cmc->c, 
							     cguid_str,
							     len, 
							     (void *)meta)) == SDF_SUCCESS) {
		    log_level = LOG_DBG;
		}
	    }
	    break;

	default:
	    if ((status = 
		 cmc_create_put_buffered_object(pai, cmc->c, cguid_str,
						len, (void *)meta)) == SDF_SUCCESS) {
		status = SDF_SUCCESS;
		log_level = LOG_DBG;
	    }
	    break;
	}
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s",
		 (unsigned long long) cguid, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_get_meta(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, SDF_cguid_t cguid, 
	     SDF_container_meta_t *meta) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_DBG;
    fthWaitEl_t *wait = NULL;
    SDF_size_t destLen = 0;
    SDF_size_t size = sizeof(SDF_container_meta_t);
    char cguid_str[MAX_CGUID_STR_LEN];
    SDF_container_meta_t *lmeta = NULL;
    SDF_container_meta_t *cached = NULL;

    plat_log_msg(21507, LOG_CAT, LOG_DBG, "%llu", (unsigned long long) cguid);

    // Check parameters
    if (cmc == NULL) {

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
							       cmc->c, 
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
		 cmc_get_for_read_pinned_object(pai, cmc->c, cguid_str,
						&destLen, (void **) &lmeta)) == SDF_SUCCESS) {

		memcpy(meta, lmeta, destLen);
		status = cmc_unpin_object(pai, cmc->c, cguid_str);

	    }

	    fthUnlock(wait);
	    break;

	case CMC_BUFFERED:

	    status = cmc_get_for_read_buffered_object(pai, cmc->c, cguid_str,
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
cmc_get_meta_from_cname(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, const char *cname, 
			SDF_container_meta_t *meta) {

    SDF_status_t status = SDF_FAILURE;
    SDF_cguid_map_t map;
    int log_level = LOG_DBG;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", cname);

    if (ISEMPTY(cname)) {

	status = SDF_INVALID_PARAMETER;
	log_level = LOG_ERR;
    } else if (strcmp(cname, CMC_PATH) == 0) {

	// Handle CMC
        memcpy(meta, &theCMC->meta, sizeof(SDF_container_meta_t));
	status = SDF_SUCCESS;
    } else if ((status = cmc_get_cguid_map(pai, theCMC, cname, &map)) == SDF_SUCCESS) {

	status = cmc_get_meta(pai, theCMC, map.cguid, meta);
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s",
		 cname, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_put_meta(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, SDF_cguid_t cguid,
	     SDF_container_meta_t *meta) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_ERR;
    SDF_size_t destLen = 0;
    SDF_size_t size = sizeof(SDF_container_meta_t);
    char cguid_str[MAX_CGUID_STR_LEN];
    SDF_container_meta_t *cached = NULL;
    SDF_container_meta_t *data = NULL;

    plat_log_msg(21507, LOG_CAT, LOG_DBG, "%llu", (unsigned long long)cguid);

    if (cmc == NULL || meta == NULL) {
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
		    (status = cmc_put_buffered_object(pai, cmc->c, cguid_str,
						      size, (void *)meta)) == SDF_SUCCESS) {
		    log_level = LOG_DBG;
		}
	    }
	    break;

	case CMC_PIN:
	    if ((status = cmc_lock_meta(pai, cmc->c, cguid)) == SDF_SUCCESS &&
		(status =
		 cmc_get_for_write_pinned_object(pai, cmc->c, cguid_str, &destLen,
						 (void **) &data)) == SDF_SUCCESS) {
		memcpy(data, meta, destLen);
		if ((status = cmc_unpin_object(pai, cmc->c, cguid_str)) == SDF_SUCCESS &&
		    (status = cmc_unlock_meta(pai, cmc->c, cguid)) == SDF_SUCCESS) {
		    log_level = LOG_DBG;
		}
	    }
	    break;

	case CMC_BUFFERED:
	    if ((status = 
		 cmc_put_buffered_object(pai, cmc->c, cguid_str,
					 size, (void *)meta)) == SDF_SUCCESS) {
		log_level = LOG_DBG;
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
cmc_remove_meta(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, SDF_cguid_t cguid) {

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
	    plat_log_msg(21512, LOG_CAT, LOG_DBG, "remove from TL %llu", (unsigned long long)cguid);
	    // We don't free the metadata until we remove it from the global map below
	    HashMap_remove(cur->this, cguid_str);
	    cur = cur->next;
	}
	fthUnlock(wait);

	// Remove meta from global map
	plat_log_msg(21513, LOG_CAT, LOG_DBG, "remove from global map %llu", (unsigned long long)cguid);
	if ((cached = HashMap_remove(meta_map, cguid_str)) != NULL) {
	    container_meta_destroy(cached);
	}
	// Fall through to remove from flash

    default:
	plat_log_msg(21514, LOG_CAT, LOG_DBG, "remove from flash %llu", (unsigned long long)cguid);
	if (cmc == NULL) {
	    status = SDF_INVALID_PARAMETER;
	} else if ((status = cmc_remove_object(pai, cmc->c,
					       cguid_str)) == SDF_SUCCESS) {
	    log_level = LOG_DBG;
	}
	break;
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long) cguid, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_get_props(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, SDF_cguid_t cguid,
	      SDF_container_props_t *props) {

    SDF_status_t status = SDF_FAILURE;
    SDF_container_meta_t meta;
    int log_level = LOG_DBG;

    plat_log_msg(21507, LOG_CAT, LOG_DBG, "%llu", (unsigned long long)cguid);

    if ((status = cmc_get_meta(pai, cmc, cguid, &meta)) == SDF_SUCCESS) {
        memcpy((void *)props, (void *)&meta.properties, sizeof(SDF_container_props_t));
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long)cguid, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_put_props(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, SDF_cguid_t cguid,
	      SDF_container_props_t props) {

    SDF_status_t status = SDF_FAILURE;
    SDF_container_meta_t meta;
    int log_level = LOG_ERR;

    plat_log_msg(21507, LOG_CAT, LOG_DBG, "%llu", (unsigned long long)cguid);
    if ((status = cmc_lock_meta(pai, cmc->c, cguid)) == SDF_SUCCESS &&
	(status = cmc_get_meta(pai, cmc, cguid, &meta)) == SDF_SUCCESS) {

        meta.properties = props;

	if ((status = cmc_put_meta(pai, cmc, cguid, &meta)) &&
	    (status = cmc_unlock_meta(pai, cmc->c, cguid)) == SDF_SUCCESS) {	    
	    log_level = LOG_DBG;
	}
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long)cguid,SDF_Status_Strings[status]);		 

    return (status);
}

SDF_shardid_t
cmc_get_shard(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, SDF_cguid_t cguid) {

    SDF_shardid_t shard = SDF_SHARDID_INVALID;
    SDF_status_t status = SDF_FAILURE;
    SDF_container_meta_t meta;
    int log_level = LOG_DBG;

    plat_log_msg(21507, LOG_CAT, LOG_DBG, "%llu", (unsigned long long)cguid);

    if ((status = cmc_get_meta(pai, cmc, cguid, &meta)) == SDF_SUCCESS) {
        shard = meta.shard;
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long)cguid, SDF_Status_Strings[status]);		 

    return (shard);
}

SDF_status_t
cmc_put_shard(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, SDF_cguid_t cguid, SDF_shardid_t shard) {

    SDF_status_t status = SDF_FAILURE;
    SDF_container_meta_t meta;
    int log_level = LOG_ERR;

    plat_log_msg(21507, LOG_CAT, LOG_DBG, "%llu", (unsigned long long)cguid);

    if ((status = cmc_lock_meta(pai, cmc->c, cguid)) == SDF_SUCCESS &&
	(status = cmc_get_meta(pai, cmc, cguid, &meta)) == SDF_SUCCESS) {

        meta.shard = shard;

	if ((status = cmc_put_meta(pai, cmc, cguid, &meta)) == SDF_SUCCESS &&
	    (status = cmc_unlock_meta(pai, cmc->c, cguid)) == SDF_SUCCESS) {	    
	    log_level = LOG_DBG;
	}
    }

    plat_log_msg(21506, LOG_CAT, log_level, "%llu - %s", 
		 (unsigned long long)cguid, SDF_Status_Strings[status]);		 

    return (status);
}

// FIXME - this needs to be executed at the home node for each shard
SDF_status_t
cmc_delete_shards(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, const char *cname) {

    struct SDF_shared_state   *state = &sdf_shared_state;
    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_ERR;
    SDF_shardid_t shard_id = SDF_SHARDID_INVALID;
    struct shard *shard = NULL;
    SDF_container_meta_t meta;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", cname);

    if ((status = cmc_get_meta_from_cname(pai, cmc, cname, &meta)) == SDF_SUCCESS) {

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
		    shardDelete(shard);
		    break;
		default:
		    // purposefully empty
		    break;
	    }
	}

	log_level = LOG_DBG;
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", cname, SDF_Status_Strings[status]);		 

    return (status);
}


SDF_status_t
cmc_lock_meta(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, SDF_cguid_t cguid) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_DBG;

    plat_log_msg(21507, LOG_CAT, LOG_DBG, "%llu", (unsigned long long)cguid);

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
cmc_unlock_meta(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, SDF_cguid_t cguid) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_DBG;

    plat_log_msg(21507, LOG_CAT, LOG_DBG, "%llu", (unsigned long long)cguid);

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

// If the cguid map object exists, then the metadata exists
SDF_status_t
cmc_meta_exists(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, const char *cname) {

    SDF_status_t status = SDF_OBJECT_UNKNOWN;
    SDF_cguid_map_t map;
    int log_level = LOG_DBG;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", cname);

    if (cmc == NULL || ISEMPTY(cname)) {

	status = SDF_INVALID_PARAMETER;
	log_level = LOG_ERR;

    } else if (strcmp(cname, CMC_PATH) == 0) {

	if (theCMC != NULL && theCMC->initialized == SDF_TRUE) {

	    status = SDF_SUCCESS;
	} 

    } else if ((cmc_type == CMC_HASHMAP) && (HashMap_get(cguid_map, cname)) != NULL) {

	status = SDF_SUCCESS;

    } else if ((status = cmc_get_cguid_map(pai, cmc, cname, &map)) != SDF_SUCCESS) {

	log_level = LOG_WARN;
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", cname, SDF_Status_Strings[status]);

    return (status);
}

// Container name to cguid map  ============================================================
SDF_status_t
cmc_create_cguid_map(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, SDF_cguid_t cguid, const char *cname) {
    SDF_status_t status = SDF_FAILURE;
    SDF_cguid_map_t *map = NULL;
    SDF_size_t datalen = sizeof(SDF_cguid_map_t);
    int log_level = LOG_ERR;

    plat_log_msg(21506, LOG_CAT, LOG_DBG, "%llu - %s", 
		 (unsigned long long)cguid, cname);

    if (ISEMPTY(cname)) {
	status = SDF_INVALID_PARAMETER;
    } else {

	// Create a map structure
	map = (SDF_cguid_map_t *) plat_alloc(datalen);
	if (!map) {
	    status = SDF_FAILURE_MEMORY_ALLOC;
	} else {

	    map->type = SDF_META_TYPE_CGUID_MAP;
	    map->node = init_get_my_node_id();
	    map->cguid = cguid;

	    switch(cmc_type) {

	    case CMC_HASHMAP:
		if ((cmc_cguidmap_update(cguid_map, cname, (void *)map)) == SDF_TRUE &&
		    (status = cmc_create_put_buffered_object(pai, cmc->c, 
							     cname, datalen,
							     (void *)map)) == SDF_SUCCESS) {
		    log_level = LOG_DBG;
		}
		break;


	    default:

		if ((status = 
		     cmc_create_put_buffered_object(pai, cmc->c, 
						    cname, datalen,
						    (void *)map)) == SDF_SUCCESS) {
		    log_level = LOG_DBG;
		    plat_free(map);
		}
		break;
	    }
	}
    }

    plat_log_msg(21516, LOG_CAT, log_level, "%llu - %s - %s", 
		 (unsigned long long)cguid, cname, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_put_cguid_map(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, SDF_cguid_t cguid, const char *cname) {
    SDF_status_t status = SDF_FAILURE;
    SDF_cguid_map_t *map = NULL;
    SDF_size_t datalen = sizeof(SDF_cguid_map_t);
    int log_level = LOG_ERR;

    plat_log_msg(21506, LOG_CAT, LOG_DBG, "%llu - %s", 
		 (unsigned long long)cguid, cname);

    if (ISEMPTY(cname)) {
	status = SDF_INVALID_PARAMETER;
    } else {

	// Create a map structure
	map = (SDF_cguid_map_t *) plat_alloc(datalen);
	if (!map) {
	    status = SDF_FAILURE_MEMORY_ALLOC;
	} else {

	    map->node = init_get_my_node_id();
	    map->cguid = cguid;

	    switch(cmc_type) {

	    case CMC_HASHMAP:
		if ((cmc_cguidmap_update(cguid_map, cname, (void *)map)) == SDF_TRUE &&
		    (status = cmc_put_buffered_object(pai, cmc->c, 
						      cname, datalen,
						      (void *)map)) == SDF_SUCCESS) {
		    log_level = LOG_DBG;
		}
		break;


	    default:

		if ((status = 
		     cmc_put_buffered_object(pai, cmc->c, 
					     cname, datalen,
					     (void *)map)) == SDF_SUCCESS) {
		    log_level = LOG_DBG;
		    plat_free(map);
		}
		break;
	    }
	}
    }

    plat_log_msg(21516, LOG_CAT, log_level, "%llu - %s - %s", 
		 (unsigned long long)cguid, cname, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_get_cguid_map(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, const char *cname, SDF_cguid_map_t *map) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_DBG;
    SDF_size_t destLen = 0;
    SDF_cguid_map_t *cached = NULL;
    SDF_size_t size = sizeof(SDF_cguid_map_t);
    SDF_cguid_map_t *lmap = NULL;
    fthWaitEl_t *wait = NULL;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", cname);

    switch (cmc_type) {

    case CMC_HASHMAP:
	if ((cached = (SDF_cguid_map_t *) HashMap_get(cguid_map, cname)) == NULL) {
	    // Miss - must get from flash
	    if ((status = cmc_get_for_read_buffered_object(pai, cmc->c,
							   cname, (void *) map,
							   size, &destLen)) != SDF_SUCCESS) {
		log_level = LOG_WARN;
	    }
	} else {

	    memcpy(map, cached, size);
	    status = SDF_SUCCESS;
	}
	break;

    case CMC_PIN:
	wait = fthLock(&cguid_lock, 1, NULL);

	if ((status =
	     cmc_get_for_read_pinned_object(pai, cmc->c, cname, &destLen,
					    (void **) &lmap)) == SDF_SUCCESS) {
	    memcpy(map, lmap, destLen);
	    status = cmc_unpin_object(pai, cmc->c, cname);
	}

	fthUnlock(wait);
	break;

    case CMC_BUFFERED:

	if ((status = cmc_get_for_read_buffered_object(pai, cmc->c,
						       cname, (void *) map,
						       size, &destLen)) != SDF_SUCCESS) {
	    log_level = LOG_WARN;
	}
	break;

    default:
	plat_log_msg(21510, LOG_CAT, LOG_FATAL, "Invalid CMC_TYPE!");
	plat_assert(0 == 1);
	break;
    }

    if (status == SDF_OBJECT_UNKNOWN) {
	status = SDF_CONTAINER_UNKNOWN;
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		 cname, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_remove_cguid_map(SDF_internal_ctxt_t *pai, SDF_cmc_t *cmc, const char *cname) {

    SDF_status_t status = SDF_FAILURE;
    fthWaitEl_t *wait = NULL;
    int log_level = LOG_ERR;
    SDF_cguid_map_t *map = NULL;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", cname);

    switch(cmc_type) {

    case CMC_HASHMAP:
	if ((map = (SDF_cguid_map_t *) HashMap_remove(cguid_map, cname)) != NULL) {
	    plat_free(map);
	    status = SDF_SUCCESS;
	    log_level = LOG_DBG;
	}
	// Fall through to remove from flash

    default:
	wait = fthLock(&cguid_lock, 1, NULL);
	if ((status = cmc_remove_object(pai, cmc->c, cname)) == SDF_SUCCESS) {
	    log_level = LOG_DBG;
	}
	fthUnlock(wait);
    }

    if (status == SDF_OBJECT_UNKNOWN) {
	status = SDF_CONTAINER_UNKNOWN;
    }

    plat_log_msg(21511, LOG_CAT, LOG_DBG, "%s - %s",  cname, SDF_Status_Strings[status]);

    return (status);
}


// Internal interfaces

SDF_status_t
cmc_create_object_container(SDF_internal_ctxt_t *pai, const char *cname, 
			    SDF_container_props_t properties) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_ERR;

    plat_log_msg(20819, PLAT_LOG_CAT_SDF_CLIENT, LOG_DBG, "%s", cname);

    if (NULL == cname) {
        status = SDF_INVALID_PARAMETER;
	plat_log_msg(30543, LOG_CAT, log_level, "NULL container name - %s", 
		     SDF_Status_Strings[status]);		 
    } else {
        properties.container_type.type = SDF_OBJECT_CONTAINER;
        if ((status = SDFCreateContainer(pai, cname, properties, 0)) == SDF_SUCCESS) {
	    log_level = LOG_DBG;
	}
	plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		     cname, SDF_Status_Strings[status]);		 
    }

    return (status);
}

SDF_status_t
cmc_delete_object_container(SDF_internal_ctxt_t *pai, const char *cname) {

    SDF_status_t status = SDF_FAILURE;
    int log_level = LOG_ERR;

    plat_log_msg(20819, PLAT_LOG_CAT_SDF_CLIENT, LOG_DBG, "%s", cname);

    if (NULL == cname) {
        status = SDF_INVALID_PARAMETER;
	plat_log_msg(30543, LOG_CAT, log_level, "NULL container name - %s", 
		     SDF_Status_Strings[status]);		 
    } else {
        if ((status = SDFDeleteContainer(pai, cname)) == SDF_SUCCESS) {
	    log_level = LOG_DBG;
	}
	plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		     cname, SDF_Status_Strings[status]);		 
    }

    return (status);
}

SDF_CONTAINER
cmc_open_object_container(SDF_internal_ctxt_t *pai, const char *path, 
			  SDF_container_mode_t mode, SDFContainer *c) {

    SDF_CONTAINER container = containerNull;
    SDF_status_t status;
    int log_level = LOG_ERR;

    plat_log_msg(20819, PLAT_LOG_CAT_SDF_CLIENT, LOG_DBG, "%s", path);

    if (NULL == path) {
        status = SDF_FAILURE_GENERIC;
	plat_log_msg(30544, LOG_CAT, log_level, "NULL path - %s", 
		     SDF_Status_Strings[status]);		 
    } else {
        if ((status = SDFOpenContainer(pai, path, mode, &container)) == SDF_SUCCESS) {
	    *c = internal_serverToClientContainer(container);
	    log_level = LOG_DBG;
	}
	plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", 
		     path, SDF_Status_Strings[status]);		 
    }

    return (container);
}

SDF_status_t
cmc_flush_inval_object_container(SDF_internal_ctxt_t *pai, const char *path) {

    SDF_status_t status = SDF_SUCCESS;
    SDF_cguid_map_t map;
    int log_level = LOG_ERR;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", path);

    if ((status = cmc_get_cguid_map(pai, theCMC, path, &map)) == SDF_SUCCESS &&
	(status = SDF_I_FlushInvalContainer(pai, map.cguid)) == SDF_SUCCESS &&
        (status = SDF_I_SyncContainer(pai, map.cguid)) == SDF_SUCCESS) {
	log_level = LOG_DBG;	
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s",
		 path, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_inval_object_container(SDF_internal_ctxt_t *pai, const char *path) {

    SDF_status_t status = SDF_SUCCESS;
    SDF_cguid_map_t map;
    int log_level = LOG_ERR;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", path);

    if ((status = cmc_get_cguid_map(pai, theCMC, path, &map)) == SDF_SUCCESS &&
	(status = SDF_I_InvalContainer(pai, map.cguid)) == SDF_SUCCESS)
    {
	log_level = LOG_DBG;	
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s",
		 path, SDF_Status_Strings[status]);

    return (status);
}

SDF_status_t
cmc_close_object_container(SDF_internal_ctxt_t *pai, SDFContainer c) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_ERR;

    plat_log_msg(21517, PLAT_LOG_CAT_SDF_CLIENT, LOG_DBG, "entry");

    if (!cmc_object_container_arg_check1(container, &status)) {
    } else if ((status = SDFCloseContainer(pai, container)) == SDF_SUCCESS) {
	log_level = LOG_DBG;
    }

    plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);		 

    return (status);
}

SDF_status_t
cmc_create_put_buffered_object(SDF_internal_ctxt_t *pai, SDFContainer c,
			       const char *objkey,
			       SDF_size_t size, void *pbuf) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER container = internal_clientToServerContainer(c);
    int log_level = LOG_ERR;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", objkey);

    if (!cmc_object_container_arg_check2(container, objkey, &status)) {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
        if ((status = 
	     SDF_I_CreatePutBufferedObject(pai, lc->cguid, (char *)objkey, strlen(objkey), size, pbuf)) == SDF_SUCCESS) 
	{
	    cmc_flush_object(pai, c, objkey);
	    log_level = LOG_DBG;
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
    int log_level = LOG_DBG;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", objkey);

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
    int log_level = LOG_DBG;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", objkey);

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

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", objkey);

    if (!cmc_object_container_arg_check2(container, objkey, &status)) {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
        if (SDF_SUCCESS == (status = SDF_I_UnpinObject(pai, lc->cguid, objkey, strlen(objkey)))) {
	    cmc_flush_object(pai, c, objkey);
	    log_level = LOG_DBG;
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
    int log_level = LOG_DBG;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", objkey);

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
    int log_level = LOG_DBG;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", objkey);

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

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", objkey);

    if (!cmc_object_container_arg_check2(container, objkey, &status)) {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
        if (SDF_SUCCESS == (status = SDF_I_PutBufferedObject(pai, lc->cguid, 
						     objkey, strlen(objkey), 
						     pbuf, size))) {
	    cmc_flush_object(pai, c, objkey);
	    log_level = LOG_DBG;
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

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", objkey);

    if (cmc_object_container_arg_check2(container, objkey, &status)) {
	status = SDF_INVALID_PARAMETER;
    } else {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
	if (SDF_SUCCESS == (status = SDF_I_RemoveObject(pai, lc->cguid, objkey, strlen(objkey)))) {
	    log_level = LOG_DBG;
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

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", objkey);

    if (!cmc_object_container_arg_check2(container, objkey, &status)) {
        local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
        if ((status = 
	     SDF_I_FlushObject(pai, lc->cguid, (char *)objkey, strlen(objkey))) == SDF_SUCCESS &&
            (status = SDF_I_SyncContainer(pai, lc->cguid)) == SDF_SUCCESS) {
	    log_level = LOG_DBG;
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

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", key);

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
		plat_log_msg(21521, LOG_CAT, LOG_DBG, "%p - REPLACE: %s - %p", 
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
	    plat_log_msg(21522, LOG_CAT, LOG_DBG, "%p - INSERT: %s - %p", 
			 fthSelf(), lkey, value);
	}
    }

    plat_log_msg(21523, LOG_CAT, status == SDF_TRUE ? LOG_DBG:LOG_ERR, 
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

    plat_log_msg(21523, LOG_CAT, status == SDF_TRUE ? LOG_DBG:LOG_ERR, 
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

    plat_log_msg(21523, LOG_CAT, status == SDF_TRUE ? LOG_DBG:LOG_ERR, 
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
