/*
 * File:   container_meta.c
 * Author: DO
 *
 * Created on January 15, 2008, 10:04 AM
 *
 * Copyright Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 *
 * $Id: container_meta.c 15229 2010-12-09 22:53:51Z briano $
 *
 */
#include <stdio.h>
#include "platform/stdlib.h"
#include "platform/stdio.h"
#include "platform/string.h"
#include "platform/logging.h"
#include "init_sdf.h"
#include "container_meta.h"
#include "shard_compute.h"


int container_meta_report_version( char **bufp, int *lenp) 
{
    plat_snprintfcat(bufp, lenp, "%s %d.%d.%d\r\n", 
	"sdf/container_meta", SDF_CONTAINER_META_VERSION, 0, 0);
    return(plat_snprintfcat(bufp, lenp, "%s %d.%d.%d\r\n", 
	"sdf/blob_container_meta", SDF_BLOB_CONTAINER_META_VERSION, 0, 0));
}

// =====================================================
SDF_container_meta_t *
container_meta_create(const char *name, SDF_container_props_t props, SDF_cguid_t cguid, SDF_shardid_t shard) {

    SDF_container_meta_t *meta = NULL;

    if (shard <= SDF_SHARDID_LIMIT && !ISEMPTY(name)) {
        if ((meta = (SDF_container_meta_t *)
             plat_alloc(sizeof (SDF_container_meta_t))) != NULL) {
	    	bzero((void *) meta, sizeof(SDF_container_meta_t));	    
	    	meta->type = SDF_META_TYPE_CONTAINER;
	    	meta->version = SDF_CONTAINER_META_VERSION;
	    	meta->cguid = cguid;
            meta->properties = props;
            meta->flush_time = 0;
            meta->delete_in_progress = SDF_FALSE;
            meta->stopflag = SDF_TRUE; // default is now stopped
            meta->flush_set_time = 0;
            meta->shard = shard;
	    	if (cguid == CMC_CGUID) 
				meta->node = CMC_HOME;
	    	else
				meta->node = init_get_my_node_id();
	    		meta->counters.sguid = 0;
	    		meta->counters.oguid = 0;
	    		if (meta->properties.shard.num_shards <= 0) {
					meta->properties.shard.num_shards = SDF_SHARD_DEFAULT_SHARD_COUNT;
	    		}
	    		if (strlen(name) > MAX_CNAME_SIZE) {
					plat_log_msg(21582, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_ERROR, "FAILED: container name exceeds max");
	    		} else {
					memcpy(&meta->cname, name, strlen(name));

					plat_log_msg(21583, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_TRACE, "metadata created for %s", name);
	    		}
       	} else {
            plat_log_msg(21584, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_TRACE, "could not allocate memory");
       	}
   	} else {
       	plat_log_msg(21585, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_TRACE, "invalid parameter");
   	}

    return (meta);
}

SDF_status_t
container_meta_destroy(SDF_container_meta_t *meta) {

    SDF_status_t status = SDF_FAILURE;

    if (meta != NULL) {
        plat_free(meta);
        status = SDF_SUCCESS;
        plat_log_msg(21586, PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE, "SUCCESS: container_meta_destroy");
    } else {
        plat_log_msg(21587, PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE, "FAILED: container_meta_destroy");
    }

    return (status);
}
#if 0
char *
container_meta_get_name(SDF_container_meta_t *meta) {

    char *name = NULL;

    if (meta != NULL) {
	//        name = meta->name;
    }
    return (name);
}

SDF_status_t
container_meta_set_name(SDF_container_meta_t *meta, const char *name) {

    SDF_status_t status = SDF_FAILURE;

    if (meta != NULL && name != NULL) {
	//        plat_free(meta->name);
	//        meta->name = (char *)name;
        status = SDF_SUCCESS;
    }

    return (status);
}
#endif
SDF_status_t
container_meta_get_props(SDF_container_meta_t *meta, SDF_container_props_t *props) {

    SDF_status_t status = SDF_FAILURE;

    if (meta != NULL && props != NULL) {
        *props = meta->properties;
        status = SDF_SUCCESS;
    }
    return (status);
}

SDF_status_t
container_meta_set_props(SDF_container_meta_t *meta, SDF_container_props_t props) {

    SDF_status_t status = SDF_FAILURE;

    if (meta != NULL) {
        meta->properties = props;
        status = SDF_SUCCESS;
    }

    return (status);
}

SDF_shardid_t
container_meta_get_shard(SDF_container_meta_t *meta) {

    SDF_shardid_t shard = SDF_SHARDID_INVALID;

    if (meta != NULL) {
        shard = meta->shard;
    }

    return (shard);
}

SDF_status_t
container_meta_set_shard(SDF_container_meta_t *meta, SDF_shardid_t shard) {

    SDF_status_t status = SDF_FAILURE;

    if (meta != NULL && shard <= SDF_SHARDID_LIMIT) {
        meta->shard = shard;
        status = SDF_SUCCESS;
    }

    return (status);
}

uint32_t
container_meta_get_vnode(SDF_container_meta_t *meta) {

    uint32_t node = VNODE_UNKNOWN;

    if (meta != NULL) {
        node = meta->node;
    }

    return (node);
}

