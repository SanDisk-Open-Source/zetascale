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
#define PLAT_OPTS_NAME(name) name ## _sdf_agent
#include "platform/opts.h"
// #include "platform/opts_c.h"

#include "agent_common.h"
#include <inttypes.h>

#ifdef SIMPLE_REPLICATION
extern int SDFGetNumNodesInMyGroupFromConfig();
extern int SDFGetNumNodesInClusterFromConfig();
extern int SDFMyGroupGroupTypeFromConfig();
#endif

// #define SOCKET_PATHSRV "/tmp/shmemq-socket"

/*  Global that holds version of the property file that is supported.
 */
uint64_t SDFPropertyFileVersionSupported = SDF_PROPERTY_FILE_VERSION;

/*  Global that holds version of the property file that was loaded.
 */
uint64_t SDFPropertyFileVersionFound = 0;

int msgCount = 5;

int (*sdf_agent_start_cb)(struct sdf_replicator *) = NULL;

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
#define LOG_LEV PLAT_LOG_LEVEL_DEBUG

void
set_debug_flags()
{
    // This is just some place to affirmitively add log messages for people
    // who'd rather recompile than run agent with a shell script.
    // plat_log_parse_arg("sdf/prot=debug");
    // plat_log_parse_arg("sdf/cmc=debug");
    // plat_log_parse_arg("sdf/naming=debug");
}


static int
init_flash(struct sdf_agent_state *state)
{
    char *device_name;
    int flash_flags = 0;

    /*  This initializes the code that redirects
     *  flash API calls to one of several alternative
     *  flash subsystems (see sdf/ssd).
     */
    #ifdef ENABLE_MULTIPLE_FLASH_SUBSYSTEMS
        ssd_Init();
    #endif

#ifdef FLASH_RECOVERY
    flash_flags = FLASH_OPEN_PERSISTENCE_AVAILABLE;
#endif

    switch(state->config.system_recovery) {
    case SYS_FLASH_RECOVERY:
    default:
        flash_flags |= FLASH_OPEN_NORMAL_RECOVERY;
        break;
    case SYS_FLASH_REFORMAT:
        flash_flags |= FLASH_OPEN_REFORMAT_DEVICE;
        break;
    }

#ifdef MULTIPLE_FLASH_DEV_ENABLED
    int ii;

    for(ii=0; ii < state->config.numFlashDevs; ii++) {
        if (plat_asprintf(&device_name, "%s%d", state->config.flashDevName, ii) > 0) {            
            state->flash_dev[ii] = (flashDev_t *)NULL;
            state->flash_dev[ii] = flashOpen(device_name, &state->flash_settings, flash_flags);
            plat_assert(state->flash_dev[ii]);
            plat_free(device_name);
        }
    }
    
/* All the /dev/flash names have been converted to /dev/flash0 
   even in signle flashcard machines. So no need to handle single
   card case separately */
#else 
    state->flash_dev = NULL;
    if (strstr(state->config.flashDevName, "%")) {
        if (plat_asprintf(&device_name, state->config.flashDevName,
                          (int)state->rank) > 0) {
            state->flash_dev = flashOpen(device_name, &state->flash_settings, flash_flags);
            plat_assert(state->flash_dev);
            plat_free(device_name);
        }
    } else {
        if (plat_asprintf(&device_name, "%s%d", state->config.flashDevName, 0) > 0) {
                state->flash_dev = flashOpen(device_name, &state->flash_settings, flash_flags);
                plat_assert(state->flash_dev);
                plat_free(device_name);
            }
    }
#endif //  MULTI_FLASH_DEV_ENABLED

    state->flash_dev_count = state->config.numFlashDevs;

    return (state->flash_dev != NULL);
}


int
init_containers(struct sdf_agent_state *state)
{
    SDF_status_t status = SDF_SUCCESS;
    struct sdf_replicator *replicator = NULL;

    if (state->config.always_replicate) {
        replicator = sdf_replicator_adapter_get_replicator(state->ReplicationInitState.adapter);
        if (replicator == NULL) {
            status = SDF_FAILURE;
            plat_assert(replicator != NULL);
        } 
    }

    SDF_action_init_t *pai = (SDF_action_init_t *)&state->ActionInitState;
    SDF_action_thrd_state_t *pts = (SDF_action_thrd_state_t *) plat_alloc( sizeof(SDF_action_thrd_state_t) );
    plat_assert( NULL != pts );
    pai->pts = (void *) pts;
    pts->phs = state->ActionInitState.pcs;
    InitActionAgentPerThreadState(pai->pcs, pts, pai);

    pai->ctxt = ActionGetContext(pts);

    if (status == SDF_SUCCESS) {
        init_sdf_initialize_config(&state->ContainerInitState,
				   (SDF_internal_ctxt_t *)pai,
        			   state->config.numObjs,
        			   state->config.system_recovery,
        			   state->rank,
        			   state->flash_dev,
                                   state->flash_dev_count,
                                   state->config.defaultShardCount,
        			   replicator); 

        state->ContainerInitState.flash_msg = state->config.flash_msg;
        state->ContainerInitState.always_replicate = state->config.always_replicate;
        state->ContainerInitState.replication_type =
            state->config.replication_type;
        state->ContainerInitState.nnodes = state->config.nnodes;
        state->ContainerInitState.flash_dev = state->flash_dev;
        state->ContainerInitState.flash_dev_count = state->config.numFlashDevs;
        status = init_sdf_initialize(&state->ContainerInitState,
                                     state->config.system_restart);
    }

    return (status == SDF_SUCCESS);
}

static struct sdf_replicator *
alloc_replicator(const struct sdf_replicator_config *replicator_config,
                 struct sdf_replicator_api *api, void *extra) 
{
   struct sdf_replicator *replicator;

    /* For use from GDB */
    __attribute__((unused))
    struct plat_opts_config_sdf_agent *agent_config =
        (struct plat_opts_config_sdf_agent *)extra;

    /* XXX - generate appropriate replicator here */
    replicator = sdf_copy_replicator_alloc(replicator_config, api);

    if (sdf_agent_start_cb != NULL) {
        sdf_agent_start_cb(replicator);
    }

    return  replicator;
}

int
init_action_home(struct sdf_agent_state *state)
{
    struct plat_opts_config_sdf_agent *config = &state->config;
    SDF_action_init_t *pai = &state->ActionInitState;

    SDF_flash_init_t *pfi = &state->FlashInitState;
    SDF_async_puts_init_t *papi = &state->AsyncPutsInitState;
    struct sdf_replicator_config *pri = &state->ReplicationInitState;

    initCommonProtocolStuff();

    pai->pcs      = (SDF_action_state_t *) plat_alloc(sizeof(SDF_action_state_t));
    plat_assert(pai->pcs);
    pai->pcs->failback = config->system_restart;
    pai->nthreads = config->numActionThreads;
    pai->nnode    = state->rank;
    pai->nnodes   = config->nnodes;
    pai->disable_fast_path = config->disable_fast_path ||
                             config->always_replicate;
    pai->flash_dev = state->flash_dev;
    pai->flash_dev_count = config->numFlashDevs;
    InitActionProtocolCommonState(pai->pcs, pai);

    if ((!SDFNew_Mode) || SDFEnable_Replication) {

	pfi->nthreads = config->numFlashProtocolThreads;
	pfi->my_node  = state->rank;
	pfi->nnodes   = config->nnodes;
	pfi->flash_server_service = SDF_FLSH;
	pfi->flash_client_service = SDF_RESPONSES;
	pfi->flash_dev = state->flash_dev;
	pfi->flash_dev_count = config->numFlashDevs;
	pfi->pfs      = home_flash_alloc(pfi, pai->pcs);
	plat_assert(pfi->pfs);

	sdf_replicator_config_init(pri, state->rank, config->nnodes);
	pri->nthreads = config->numReplicationThreads;
	pri->replication_service = SDF_REPLICATION;
	pri->replication_peer_service = SDF_REPLICATION_PEER;
	pri->flash_service = SDF_FLSH;
	pri->response_service = SDF_RESPONSES;
	pri->lease_usecs = config->replication_lease_secs * 1000000L;
        pri->lease_liveness = config->replication_lease_liveness;
        pri->switch_back_timeout_usecs =
            config->replication_switch_back_timeout_secs * PLAT_MILLION;
        pri->initial_preference = config->replication_initial_preference;
	pri->outstanding_window = config->replication_outstanding_window;
	pri->timeout_usecs = sdf_msg_getp_int("msg_timeout") * 1000L;
#ifdef SIMPLE_REPLICATION
        pri->node_count = SDFGetNumNodesInClusterFromConfig();
#endif
        pri->vip_config = pai->pcs->qrep_state.vip_config;
	pri->adapter = sdf_replicator_adapter_alloc(pri, &alloc_replicator, config);
    }

    /* initialization state for async puts thread pool */
    papi->nthreads        = config->numAsyncPutThreads;
    papi->my_node         = state->rank;
    papi->nnodes          = config->nnodes;
    papi->flash_dev       = state->flash_dev;
    papi->flash_dev_count = config->numFlashDevs;
    papi->max_flushes_in_progress = getProperty_uLongInt("SDF_MAX_OUTSTANDING_FLUSHES", 8);
    if (papi->max_flushes_in_progress == 0) {
        papi->max_flushes_in_progress = 8;
	plat_log_msg(30603, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, 
		     "SDF_MAX_OUTSTANDING_FLUSHES must be non-zero; using default of %d", papi->max_flushes_in_progress);
    } else if (papi->max_flushes_in_progress > papi->nthreads) {
        papi->max_flushes_in_progress = papi->nthreads;
	plat_log_msg(30604, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, 
		     "SDF_MAX_OUTSTANDING_FLUSHES must be less than or equal to the number of async put threads; setting to %d", papi->max_flushes_in_progress);
    } else {
	plat_log_msg(30605, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, 
		     "SDF_MAX_OUTSTANDING_FLUSHES = %d", papi->max_flushes_in_progress);
    }

    papi->max_background_flushes_in_progress = getProperty_uLongInt("SDF_MAX_OUTSTANDING_BACKGROUND_FLUSHES", 8);
    if (papi->max_background_flushes_in_progress > papi->nthreads) {
        papi->max_background_flushes_in_progress = papi->nthreads;
	plat_log_msg(30608, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, 
		     "SDF_MAX_OUTSTANDING_BACKGROUND_FLUSHES must be less than or equal to the number of async put threads; setting to %d", papi->max_background_flushes_in_progress);
    } else {
	plat_log_msg(30609, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, 
		     "SDF_MAX_OUTSTANDING_BACKGROUND_FLUSHES = %d", papi->max_background_flushes_in_progress);
    }

    papi->background_flush_sleep_msec = getProperty_uLongInt("SDF_BACKGROUND_FLUSH_SLEEP_MSEC", 1000);
    if (papi->background_flush_sleep_msec < MIN_BACKGROUND_FLUSH_SLEEP_MSEC) {
        papi->background_flush_sleep_msec = MIN_BACKGROUND_FLUSH_SLEEP_MSEC;
	plat_log_msg(30610, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, 
		     "SDF_BACKGROUND_FLUSH_SLEEP_MSEC must be >= %d; defaulting to minimum value", MIN_BACKGROUND_FLUSH_SLEEP_MSEC);
    } else {
	plat_log_msg(30611, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_INFO, 
		     "SDF_BACKGROUND_FLUSH_SLEEP_MSEC = %d", papi->background_flush_sleep_msec);
    }

    papi->paps            = async_puts_alloc(papi, pai->pcs);
    plat_assert(papi->paps);

    return (SDF_TRUE);
}


/** @brief Set defaults before command line is parsed */
void
agent_config_set_defaults(struct plat_opts_config_sdf_agent *config)
{
    char *s;
    memset(config, 0, sizeof (config));
    plat_shmem_config_init(&config->shmem);
#ifdef SIMPLE_REPLICATION
    config->replication_type = SDFMyGroupGroupTypeFromConfig();
#else
    config->replication_type = SDF_REPLICATION_SIMPLE;
#endif

   /* /dev/flash is the "base" name for the flash devices. 
      a 0 based index will be applied in init_flash function to the 
      basename to create names like /dev/flash0 /dev/flash1 etc.
    */
    strncpy(config->flashDevName, "/dev/flash", sizeof(config->flashDevName));

    #ifdef SDFAPI
	if ((s = getenv("FDF_PROPERTY_FILE"))) {
	    strncpy(config->propertyFileName, 
		    s, 
		    sizeof(config->propertyFileName));
	} else {
	    strncpy(config->propertyFileName, 
		    "/home/briano/config/really_simple.prop",
		    sizeof(config->propertyFileName));
	}
    #else // SDFAPI
	strncpy(config->propertyFileName, 
		"/opt/schooner/config/schooner-med.properties",
		sizeof(config->propertyFileName));
    #endif // SDFAPI

    config->system_recovery = SYS_FLASH_RECOVERY;

    config->ffdc_buffer_len = FFDC_THREAD_BUFSIZE;
    config->ffdc_disable = 0;
}

int property_file_report_version( char **bufp, int *lenp) 
{
    return(plat_snprintfcat(bufp, lenp, "%s %lld.%d.%d\r\n", 
	"sdf/prop_file", SDF_PROPERTY_FILE_VERSION, 0, 0));
}

/** @brief Fetch config from properties file which may be set on command line */
int
agent_config_set_properties(struct plat_opts_config_sdf_agent *config)
{
    int success;

    success = !loadProperties(config->propertyFileName);

    SDFPropertyFileVersionFound = getProperty_uLongInt("SDF_PROP_FILE_VERSION", 0);

    /* xxxzzz
     *  Temporarily accept property files without a version, but
     *  print a nasty message.
     */
    if (SDFPropertyFileVersionFound == 0) {
	plat_log_msg(20838, PLAT_LOG_CAT_PRINT_ARGS, 
		     PLAT_LOG_LEVEL_ERROR, 
		     "Missing version in property file  (Add 'SDF_PROP_FILE_VERSION = %"PRIu64"')", SDFPropertyFileVersionSupported);
	success = 0;
    } else {
	if (SDFPropertyFileVersionFound != SDFPropertyFileVersionSupported) {
	    plat_log_msg(20839, PLAT_LOG_CAT_PRINT_ARGS, 
			 PLAT_LOG_LEVEL_ERROR, 
			 "Inconsistent version of property file: %"PRIu64" (only version %"PRIu64" is supported)", SDFPropertyFileVersionFound, SDFPropertyFileVersionSupported);
	    success = 0;
	} else {
	    plat_log_msg(20840, PLAT_LOG_CAT_PRINT_ARGS, 
			 PLAT_LOG_LEVEL_INFO, 
			 "Version of property file: %"PRIu64"", SDFPropertyFileVersionFound);
	}
    }

    config->numFlashProtocolThreads = getProperty_uLongInt("SDF_FLASH_PROTOCOL_THREADS", 128);
    config->numAsyncPutThreads = getProperty_uLongInt("SDF_ASYNC_PUT_THREADS", 128);
    config->numReplicationThreads = getProperty_uLongInt("SDF_REPLICATION_THREADS", 8);
    config->numAgentMboxes =  getProperty_uLongInt("SDF_NUM_AGENT_MBOXES", 10);
    config->defaultShardCount = getProperty_uLongInt("SDF_DEFAULT_SHARD_COUNT", 1);
    config->numFlashDevs =    getProperty_uLongInt("SDF_NUM_FLASH_DEVS", 1);

    /*
     * XXX: This is the only one which currently exists as an overidable 
     * command line option.
     */
    if (!config->numObjs) {
        config->numObjs = getProperty_uLongInt("SDF_SHARD_MAX_OBJECTS",
                                               1000000);
    }

    if (!config->nnodes) {
        config->nnodes = getProperty_Int("SDF_NNODES", 0);
    }

    if (!config->replication_outstanding_window) {
        config->replication_outstanding_window =
            getProperty_Int("SDF_REPLICATION_OUTSTANDING_WINDOW", 1000);
    }

    if (!config->replication_lease_secs) {
        config->replication_lease_secs = 
            getProperty_Int("SDF_REPLICATION_LEASE_SECS", 5);
    }

    #ifdef SDFAPI
	config->system_recovery = getProperty_Int("SDF_REFORMAT", 0);
	if (config->system_recovery) {
	    config->system_recovery = 0;
	}
    #endif // SDFAPI

    plat_log_msg(20841, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_CLUSTER_NUMBER_NODES=%u",
                 config->nnodes);
    plat_log_msg(20842, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_ACTION_NODE_THREADS=%u",
                 config->numActionThreads);
    plat_log_msg(20843, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_FLASH_PROTOCOL_THREADS=%u",
                 config->numFlashProtocolThreads);
    plat_log_msg(20844, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_REPLICATION_THREADS=%u",
                 config->numReplicationThreads);
    plat_log_msg(20845, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_SHARD_MAX_OBJECTS=%u",
                 config->numObjs);
    plat_log_msg(20846, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_MSG_ENGINE_START=%u",
                 getProperty_Int("SDF_MSG_ENGINE_START", 1));
    plat_log_msg(20847, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_NUM_FLASH_DEVS=%u",
        config->numFlashDevs);
    plat_log_msg(20848, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_DEBUG, "PROP: SDF_DEFAULT_SHARD_COUNT=%u",
        config->defaultShardCount);

    return (success);
}


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
agent_engine_pre_init_internal(struct sdf_agent_state *state,
                               int argc, char *argv[]) {
    int numScheds;
    uint32_t sdf_msg_numprocs;

    sdf_msg_init(argc, argv);
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
            plat_log_msg(21788, LOG_CAT, PLAT_LOG_LEVEL_INFO,
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
        plat_log_msg(10005, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                     "FFDC logging disabled");
    }

    numScheds = getProperty_Int("SDF_FTHREAD_SCHEDULERS", 1);
    fthInitMultiQ(1, numScheds);
    return SDF_TRUE;
}

/**
 * @brief  Main initialization routine for SDF
 *
 * @return status, SDF_TRUE on success
 */
SDF_boolean_t 
sdf_init(struct sdf_agent_state *state, int argc, char *argv[])
{
    if (agent_engine_pre_init(state, argc, argv)) {
        return(agent_engine_post_init(state));
    } else {
        return(SDF_FALSE); // failure
    }
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
SDF_boolean_t agent_engine_pre_init(struct sdf_agent_state *state, int argc, char *argv[])
{
    SDF_boolean_t  success = SDF_TRUE;
    const char    *sdf_mode_string;
    const char    *sdf_replication_string;

    agent_config_set_defaults(&state->config);

    if (success) {
        success = plat_opts_parse_sdf_agent(&state->config, argc, argv) ?
            SDF_FALSE : SDF_TRUE;
        plat_log_msg(20849, LOG_CAT, 
                     success ?  LOG_LEV : PLAT_LOG_LEVEL_INFO,
                     "plat_opts_parse_sdf_agent SUCCESS = %u", success);
    }

    if (success) {
        success = agent_config_set_properties(&state->config);
        plat_log_msg(20850, LOG_CAT, 
                     success ? LOG_LEV : PLAT_LOG_LEVEL_INFO,
                     "set properties SUCCESS = %u", success);
    }

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
        success = agent_engine_pre_init_internal(state, argc, argv);
    }

    return (success);
}


/*
 * Liveness callback function.
 */
static void
live_back(int live, int rank, void *arg)
{
    if (!live)
	sdf_replicator_node_dead(arg, rank);
    else {
        struct timeval now;

        plat_gettimeofday(&now, NULL);
	sdf_replicator_node_live(arg, rank, now);
    }
}


/**
 * @brief Post-initialization of SDF agent engine (fthread safe)
 *
 * Order of initialization (change accordingly): <br>
 *
 * 1. init_flash <br>
 * 2. init_action_home <br>
 * 3. home_flash_start <br>
 * 3. async_puts_start <br>
 * 4. spawn home node fthreads <br>
 * 5. init_containers <br>
 *  
 * @return status, SDF_TRUE on success
 */
SDF_boolean_t agent_engine_post_init(struct sdf_agent_state * state )
{
    SDF_boolean_t success = SDF_TRUE;
    

    /*
     * Enable the Replication only if my node is part of
     * N+1, 2way, simple replication */
#ifdef SIMPLE_REPLICATION  
    int grp_type;
    grp_type = SDFMyGroupGroupTypeFromConfig();
    if( (grp_type == SDF_REPLICATION_V1_2_WAY) || (grp_type == SDF_REPLICATION_V1_N_PLUS_1)) {
        plat_log_msg(20851, LOG_CAT,PLAT_LOG_LEVEL_TRACE," Replication StateMachine Turned ON\n");
        state->config.always_replicate = 1;
    }
#endif

    /*
     * Initialize the flash subsystem.
     */
    if (success) {
        success = init_flash(state );
        plat_log_msg(20852, LOG_CAT, 
                     success ?  LOG_LEV : PLAT_LOG_LEVEL_INFO,
                     "init_flash  = %u", success);
    }

    /*
     * Initialize the action and home protocol threads.
     */
    if (success) {
        success = init_action_home(state);
        plat_log_msg(20853, LOG_CAT, 
                     success ?  LOG_LEV : PLAT_LOG_LEVEL_INFO,
                     "init_action_home = %u", success);
    }

    if ((!SDFNew_Mode) || SDFEnable_Replication) {

        /* we don't use any of this stuff in streamlined SDF mode */

	/*
	 * Initialize the home flash threads.
	 */
	if (success) {
	    success = home_flash_start( state->FlashInitState.pfs );
	    /*
	     * FIXME: home_flash_start returns 0 as success
	     */
	    success = ( 0 == success ) ? SDF_TRUE : SDF_FALSE;
	    plat_log_msg(20854, LOG_CAT, 
			 success ?  LOG_LEV : PLAT_LOG_LEVEL_INFO,
			 "home_flash_start = %u", success);
	}

	/*
	 * Initialize the replication subsystem.
	 */
	// XXX: drew 2008-08-29 conditional until replication is re-fixed
	if (success && state->config.always_replicate) {
	    success = sdf_replicator_adapter_start(state->ReplicationInitState.adapter);
	    success = ( 0 == success ) ? SDF_TRUE : SDF_FALSE;
	    plat_log_msg(20855, LOG_CAT, 
			 success ?  LOG_LEV : PLAT_LOG_LEVEL_INFO,
			 "sdf_replicator_adapter_start = %u", success);

	    if (success) {
		struct sdf_replicator *replicator = NULL;
		replicator = sdf_replicator_adapter_get_replicator(state->ReplicationInitState.adapter);

                /* Request liveness callbacks from the messaging system */
                msg_livecall(1, 1, live_back, replicator);
	    }
	}
    }

    /*
     * Initialize the async put threads.
     */
    if (success) {
	success = async_puts_start( state->AsyncPutsInitState.paps );
	/*
	 * FIXME: async_puts_start returns 0 as success
	 */
	success = ( 0 == success ) ? SDF_TRUE : SDF_FALSE;
	plat_log_msg(20856, LOG_CAT, 
		     success ?  LOG_LEV : PLAT_LOG_LEVEL_INFO,
		     "async_puts_start = %u", success);
    }

    /*
     * Initialize the CMC.
     */
    if (success) {
	//	schedule_container_thread(state);

	success = init_containers(state);
	
	plat_log_msg(20857, LOG_CAT, 
		     success ?  LOG_LEV : PLAT_LOG_LEVEL_INFO,
		     "init_containers  = %u", success);
    }

    /* Declare ourselves alive */
    //msg_map_alive();

    return (success);
}

#include "platform/opts_c.h"
