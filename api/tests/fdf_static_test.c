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
//static struct SDF_thread_state 	*_sdf_thrd_state;


// ====================================================================================================================
static int container_test()
{
    char				*cname = "/cache_container";
    SDF_container_props_t		 p;
    SDF_thread_state_t			*sdf_thread_state;
    uint64_t 				 cid = 1;
    SDF_cguid_t				 cguid = SDF_NULL_CGUID;
    char				*key = "object1";
    int					 keylen = strlen(key);
    char				*data = "this-is-the-data";
    int					 datalen = strlen(data);
    char				*buf = NULL;
    uint64_t 				 buflen = 0;

    p.container_type.type = SDF_OBJECT_CONTAINER;
    p.container_id.size = 1000; /* KB */
    cguid = SDFGenerateCguid(sdf_thread_state, cid);

    if (SDF_SUCCESS == SDFCreateContainer(sdf_thread_state, cname, &p, &cguid)) {
	fprintf(stderr, "Created container %s\n", cname);
    } else {
	fprintf(stderr, "Error creating container %s\n", cname);
	return (-1);
    }

    if (SDF_SUCCESS == (SDFOpenContainer(sdf_thread_state, cguid, SDF_READ_WRITE_MODE))) {
		fprintf(stderr, "Opened container %s.\n", cname);
	} else {
		fprintf(stderr, "Error opening container %s.\n", cname);
        return (-1);
    }

    if (SDF_SUCCESS == (SDFStartContainer(sdf_thread_state, cguid))) {
	fprintf(stderr, "Started container %s.\n", cname);
    } else {
	fprintf(stderr, "Error starting container %s.\n", cname);
        return (-1);
    }

    if (SDF_SUCCESS == (SDFPutBufferedObject(sdf_thread_state, cguid, key, keylen, data, datalen, 0, 0))) {
	fprintf(stderr, "Put object %s.\n", key);
    } else {
	fprintf(stderr, "Error putting object %s.\n", key);
        return (-1);
    }

    if (SDF_SUCCESS == (SDFGetForReadBufferedObject(sdf_thread_state, cguid, key, keylen, &buf, &buflen, 0, 0))) {
	fprintf(stderr, "Get object %s, value = %s\n", key, buf);
    } else {
	fprintf(stderr, "Error getting object %s.\n", key);
        return (-1);
    }

    if (SDF_SUCCESS == (SDFStopContainer(sdf_thread_state, cguid))) {
	fprintf(stderr, "Stopped container %s.\n", cname);
    } else {
	fprintf(stderr, "Error stoping container %s.\n", cname);
        return (-1);
    }

    if (SDF_SUCCESS == (SDFCloseContainer(sdf_thread_state, cguid))) {
	fprintf(stderr, "Closed container %s.\n", cname);
    } else {
	fprintf(stderr, "Error closing container %s.\n", cname);
        return (-1);
    }

    if (SDF_SUCCESS == SDFDeleteContainer(sdf_thread_state, cguid)) {
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
#if 0
    int affinity = NO_CPU_AFFINITY;
    char *cname = "foo";
    api_settings_init();

    api_init_sdf( settings.sdf_log_level,
                  settings.num_sdf_threads,  // # of fthreads
                  settings.num_cores,        // # of cores
                  affinity,                  // CPU affinity or -1
                  cname,
                  argc,     
                  argv    );
#endif

    if (SDF_SUCCESS == SDFInit(&sdf_state, argc, argv)) {
	return (container_test());
    } else {
	fprintf(stderr, "Failed to initialize SDF\n");
	return (-1);
    }
}

