/*
 * File:   fdf_static_test.c
 * Author: Darryl Ouye
 *
 * Created on September 12
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 */

#include <stdio.h> 
#include <inttypes.h>
#include <string.h> 
#include "common/sdftypes.h"
#include "fth/fth.h"
#include "utils/properties.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_common.h"
#include "protocol/action/action_thread.h"
#include "protocol/action/async_puts.h"
#include "protocol/home/home_flash.h"
#include "protocol/action/async_puts.h"
#include "protocol/replication/copy_replicator.h"
#include "protocol/replication/replicator.h"
#include "protocol/replication/replicator_adapter.h"
#include "shared/init_sdf.h"
#include "agent/agent_common.h"
#include "ssd/fifo/mcd_aio.h"
#include "ssd/fifo/mcd_osd.h"
#include "ssd/fifo/mcd_ipf.h"
#include "api/sdf.h"

enum { NO_CPU_AFFINITY = -1 };

volatile time_t 		 current_time;
struct settings 		 settings;
static struct SDF_state 	*sdf_state;


// ====================================================================================================================
static int container_test(struct SDF_thread_state *_sdf_thread_state)
{
    char				*cname = "/cache_container";
    SDF_container_props_t		 props;
    SDF_cguid_t				 cguid = SDF_NULL_CGUID;
    char				*key = "object1";
    int					 keylen = strlen(key);
    char				*data = "this-is-the-data";
    int					 datalen = strlen(data);
    char				*buf = NULL;
    uint64_t 				 buflen = 0;
    SDF_time_t    			 expiry;

    props.container_id.owner                    = 0;
    props.container_id.size                     = 1024 * 1024;
    props.container_id.container_id             = 1;
    props.container_id.owner                    = 0;
    props.container_id.num_objs                 = 1000000;

    props.cguid                                 = SDF_NULL_CGUID;

    props.container_type.type                   = SDF_OBJECT_CONTAINER;
    props.container_type.persistence            = SDF_TRUE;
    props.container_type.caching_container      = SDF_TRUE;
    props.container_type.async_writes           = SDF_FALSE;

    props.replication.enabled                   = 0;
    props.replication.type                      = SDF_REPLICATION_NONE;
    props.replication.num_replicas              = 1;
    props.replication.num_meta_replicas         = 0;
    props.replication.synchronous               = 1;

    props.cache.not_cacheable                   = SDF_FALSE;
    props.cache.shared                          = SDF_FALSE;
    props.cache.coherent                        = SDF_FALSE;
    props.cache.enabled                         = SDF_TRUE;
    props.cache.writethru                       = SDF_TRUE;
    props.cache.size                            = 0;
    props.cache.max_size                        = 0;

    props.shard.enabled                         = SDF_TRUE;
    props.shard.num_shards                      = 1;

    props.fifo_mode                             = SDF_FALSE;

    props.durability_level                      = SDF_FULL_DURABILITY;

    if (SDF_SUCCESS == SDFCreateContainer(_sdf_thread_state, cname, &props, &cguid)) {
	fprintf(stderr, "Created container %s\n", cname);
    } else {
	fprintf(stderr, "Error creating container %s\n", cname);
	return (-1);
    }

    if (SDF_SUCCESS == (SDFOpenContainer(_sdf_thread_state, cguid, SDF_READ_WRITE_MODE))) {
		fprintf(stderr, "Opened container %s.\n", cname);
	} else {
		fprintf(stderr, "Error opening container %s.\n", cname);
        return (-1);
    }

    if (SDF_SUCCESS == (SDFStartContainer(_sdf_thread_state, cguid))) {
	fprintf(stderr, "Started container %s.\n", cname);
    } else {
	fprintf(stderr, "Error starting container %s.\n", cname);
        return (-1);
    }

    if (SDF_SUCCESS == (SDFCreateBufferedObject(_sdf_thread_state, cguid, key, keylen, data, datalen, 0, 0))) {
	fprintf(stderr, "Create object %s.\n", key);
    } else {
	fprintf(stderr, "Error putting object %s.\n", key);
        return (-1);
    }

    if (SDF_SUCCESS == (SDFGetForReadBufferedObject(_sdf_thread_state, cguid, key, keylen, &buf, &buflen, 0, &expiry))) {
	fprintf(stderr, "Get object %s, value = %s\n", key, buf);
    } else {
	fprintf(stderr, "Error getting object %s.\n", key);
        return (-1);
    }
#if 0
    if (SDF_SUCCESS == (SDFStopContainer(_sdf_thread_state, cguid))) {
	fprintf(stderr, "Stopped container %s.\n", cname);
    } else {
	fprintf(stderr, "Error stoping container %s.\n", cname);
        return (-1);
    }
#endif
    if (SDF_SUCCESS == (SDFCloseContainer(_sdf_thread_state, cguid))) {
	fprintf(stderr, "Closed container %s.\n", cname);
    } else {
	fprintf(stderr, "Error closing container %s.\n", cname);
        return (-1);
    }

    if (SDF_SUCCESS == SDFDeleteContainer(_sdf_thread_state, cguid)) {
	fprintf(stderr, "Deleted container %s\n", cname);
    } else {
	fprintf(stderr, "Error Deleting container %s\n", cname);
	return (-1);
    }

    return (0);
}

// ====================================================================================================================
int
main(int argc, char *argv[])
{
    struct SDF_thread_state *_sdf_thread_state;

    if (SDFInit(&sdf_state, 0, NULL) != SDF_SUCCESS) {
        fprintf(stderr, "SDF initialization failed!\n");
        plat_assert(0);
    }

    fprintf(stderr, "SDF was initialized successfully!\n");

    _sdf_thread_state    = SDFInitPerThreadState(sdf_state);

    container_test(_sdf_thread_state);
}

