/*
 * File:   agent_helper.c
 * Author: Darpan Dinker
 *
 * Created on March 11, 2008, 5:32 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: agent_helper.c 15229 2010-12-09 22:53:51Z briano $
 */
#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <semaphore.h>

#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_msg_types.h"
#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/fcntl.h"
#include "platform/logging.h"
#include "platform/mutex.h"
#include "platform/shmem.h"
#include "platform/socket.h"
#include "platform/stdlib.h"
#include "platform/stdio.h"
#include "platform/string.h"
#include "platform/time.h"
#include "platform/types.h"
#include "platform/unistd.h"

#include "protocol/init_protocol.h"
#include "common/sdftypes.h"
#include "shared/init_sdf.h"
#include "shared/sdf_sm_msg.h"
#include "shared/name_service.h"
#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdftcp/msg_map.h"
#include "flash/flash.h"

#include "protocol/protocol_utils.h"
#include "protocol/protocol_common.h"
#include "protocol/action/action_thread.h"
#include "protocol/action/async_puts.h"
#include "protocol/home/home_flash.h"
#include "protocol/action/async_puts.h"

#include "protocol/replication/copy_replicator.h"
#include "protocol/replication/replicator.h"
#include "protocol/replication/replicator_adapter.h"
#include "utils/properties.h"
#include "agent_helper.h"
//#define PLAT_OPTS_NAME(name) name ## _fdf_agent
//#include "platform/opts.h"

#include "agent_common.h"
#include <inttypes.h>

#ifdef SIMPLE_REPLICATION
extern int SDFGetNumNodesInMyGroupFromConfig();
extern int SDFGetNumNodesInClusterFromConfig();
extern int SDFMyGroupGroupTypeFromConfig();
#endif

extern void agent_config_set_defaults();
extern int	agent_config_set_properties();
extern void set_debug_flags();

    /* are we running in new "streamlined" SDF mode? */
static SDF_boolean_t SDFNew_Mode = SDF_FALSE;
    /* are we running with replication? */
static SDF_boolean_t SDFEnable_Replication = SDF_FALSE;

/************************************************************************
 *                                                                      *
 *              Migrated over from agent_engine.c                       *
 *                                                                      *
 ************************************************************************/

#undef LOG_CAT
#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_AGENT
#define LOG_LEV PLAT_LOG_LEVEL_TRACE

/*
 * @brief Pthread-safe pre-initialization of SDF agent engine.
 *
 * Order of initialization (change accordingly): <br>
 * 1. sdf_msg_init_mpi (find out who we are in the cluster?) <br>
 * 2. shmem initialization <br>
 * 3. fthInit <br>
 * 4. sdf_msg_init <br>
 * 5. sdf_msg_startmsg <br>   // FIXME: merge sdf_msg_xxx calls?
 * 
 * @return status, SDF_TRUE on success
 */
static SDF_boolean_t
fdf_agent_engine_pre_init_internal(struct sdf_agent_state *state) {
    int numScheds;
    uint32_t sdf_msg_numprocs;

    sdf_msg_init(0, NULL);
    state->rank = sdf_msg_myrank();
    sdf_msg_numprocs = sdf_msg_numranks();

    /*
     * Allow number of nodes to not match MPI so that node failures 
     * can be simulated for multi-node operation as with replication 
     * prior to the adoption of non-MPI based messaging.
     */
    if (!state->config.nnodes)
        state->config.nnodes = sdf_msg_numprocs;

    if (!init_agent_sm_config(&state->config.shmem, state->rank))
        return SDF_FALSE;

    if (state->config.ffdc_disable == 0) {
        if (ffdc_initialize(0, BLD_VERSION, 
                            state->config.ffdc_buffer_len) == 0) {
            plat_log_msg(21788, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                         "Initialized FFDC logging "
                         "(max buffers=%d, thread bufsize=0x%lx)", 
                         FFDC_MAX_BUFFERS, 
                         (long)state->config.ffdc_buffer_len);
        } else {
            plat_log_msg(21789, LOG_CAT, PLAT_LOG_LEVEL_WARN,
                         "Unable to initialize FFDC logging "
                         "(max buffers=%d, thread bufsize=0x%lx)", 
                         FFDC_MAX_BUFFERS, 
                         (long)state->config.ffdc_buffer_len);
        }
    } else {
        plat_log_msg(10005, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                     "FFDC logging disabled");
    }

    numScheds = getProperty_Int("SDF_FTHREAD_SCHEDULERS", 1);
    fthInitMultiQ(1, numScheds);
    return SDF_TRUE;
}

/**
 * @brief Pre-initialization of SDF agent engine (pthread safe)
 *
 * 0. argument parsing, <br>
 * 1. read and initialize properties <br>
 * 2. calls agent_engine_pre_init_internal <br>
 * 
 * @return status, SDF_TRUE on success
 */
SDF_boolean_t fdf_agent_engine_pre_init(struct sdf_agent_state *state)
{
    SDF_boolean_t  success = SDF_TRUE;
    const char    *sdf_mode_string;
    const char    *sdf_replication_string;
    char log_file[128]="";

    agent_config_set_defaults(&state->config);
    /* Set the log file if configured*/
    getPropertyFromFile(state->config.propertyFileName, "FDF_LOG_FILE",log_file);
    if( strcmp(log_file,"") ) {
        plat_log_set_file(log_file, PLAT_LOG_REDIRECT_STDERR|PLAT_LOG_REDIRECT_STDOUT);
    }

    if (success) {
#if 0
        success = plat_opts_parse_sdf_agent(&state->config, argc, argv) ?
            SDF_FALSE : SDF_TRUE;
#endif
        plat_log_msg(20849, LOG_CAT, 
                     success ?  LOG_LEV : PLAT_LOG_LEVEL_DEBUG,
                     "plat_opts_parse_sdf_agent SUCCESS = %u", success);
    }

    if (success) {
        success = agent_config_set_properties(&state->config);
        plat_log_msg(20850, LOG_CAT, 
                     success ? LOG_LEV : PLAT_LOG_LEVEL_DEBUG,
                     "set properties SUCCESS = %u", success);
    }

#ifdef SDFAPIONLY
    (void ) strcpy(state->flash_settings.aio_base, getProperty_String("AIO_BASE_FILENAME", "/schooner/backup/schooner%d"));
#endif /* SDFAPIONLY */

    if (success && !state->config.log_less) {
        set_debug_flags();
    }

    /*  Determine if we are in "new" mode, so that we don't
     *  start up unnecessary subsystems.
     */

    sdf_mode_string = getProperty_String("SDF_MODE", "new");

    if (strcmp(sdf_mode_string, "new") == 0) {
        SDFNew_Mode = SDF_TRUE;
    } else {
	SDFNew_Mode = SDF_FALSE;
    }

    /*  Determine if replication is turned on.
     */

    sdf_replication_string = getProperty_String("SDF_REPLICATION", "Off");

    if (strcmp(sdf_replication_string, "On") == 0) {
        SDFEnable_Replication = SDF_TRUE;
    } else {
	sdf_replication_string = getProperty_String("SDF_SIMPLE_REPLICATION", "Off");
	if (strcmp(sdf_replication_string, "On") == 0) {
	    SDFEnable_Replication = SDF_TRUE;
	} else {
	    SDFEnable_Replication = SDF_FALSE;
	}
    }

    if (success) {
        success = fdf_agent_engine_pre_init_internal(state);
    }

    return (success);
}


//#include "platform/opts_c.h"
