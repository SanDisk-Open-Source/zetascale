//  sdf.cc

#include <stdio.h>
#include <stdint.h>

#include "sdf.h"
#include "sdf_internal.h"
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
#include "ssd/fifo/mcd_ipf.h"
#include "ssd/fifo/mcd_osd.h"
#include "shared/init_sdf.h"
#include "shared/private.h"
#include "shared/open_container_mgr.h"
#include "shared/container_meta.h"
#include "shared/name_service.h"
#include "shared/shard_compute.h"
#include "agent/agent_common.h"
#include "agent/agent_helper.h"

#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_NAMING
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_WARN PLAT_LOG_LEVEL_WARN
#define LOG_FATAL PLAT_LOG_LEVEL_FATAL

/*
** Externals
*/
extern void *cmc_settings;

extern 
SDF_cguid_t generate_cguid(
	SDF_internal_ctxt_t	*pai, 
	const char		*path, 
	uint32_t 		 node, 
	int64_t 		 cntr_id
	);

extern 
SDF_container_meta_t * build_meta(
	const char 		*path, 
	SDF_container_props_t 	 props, 
	SDF_cguid_t 		 cguid, 
	SDF_shardid_t 		 shard
	);

extern 
SDF_shardid_t build_shard(
	struct SDF_shared_state *state, 
	SDF_internal_ctxt_t 	*pai,
        const char 		*path, 
	int 			 num_objs,  
	uint32_t 		 in_shard_count,
        SDF_container_props_t 	 props,  
	SDF_cguid_t 		 cguid,
        enum build_shard_type 	 build_shard_type, 
	const char 		*cname
	);

extern 
SDF_status_t create_put_meta(
	SDF_internal_ctxt_t 	*pai, 
	const char 		*path, 
	SDF_container_meta_t 	*meta,
        SDF_cguid_t 		 cguid
	);

extern 
SDF_boolean_t agent_engine_pre_init(
	struct sdf_agent_state 	*state, 
	int 			 argc, 
	char 			*argv[]
	);

extern 
SDF_boolean_t agent_engine_post_init(
	struct sdf_agent_state 	*state
	);

extern 
SDF_status_t delete_container_internal(
	SDF_internal_ctxt_t 	*pai, 
	const char 		*path, 
	SDF_boolean_t 		 serialize
	);

static void set_log_level( unsigned int log_level )
{
    char                buf[80];
    char              * levels[] = { "devel", "trace", "debug", "diagnostic",
                                     "info", "warn", "error", "fatal" };

    sprintf( buf, "apps/memcached/server=%s", levels[log_level] );
    plat_log_parse_arg( buf );
}

static int sdf_check_delete_in_future(void *data)
{
    return(0);
}

static void load_settings(flash_settings_t *osd_settings)
{
    (void) strcpy(osd_settings->aio_base, getProperty_String("AIO_BASE_FILENAME", "/mnt/ssd/schooner%d")); // base filename of flash files
    osd_settings->aio_create          = 1;// use O_CREAT
    osd_settings->aio_total_size      = getProperty_Int("AIO_FLASH_SIZE_TOTAL", 0); // this flash size counts!
    osd_settings->aio_sync_enabled    = getProperty_Int("AIO_SYNC_ENABLED", 0); // AIO_SYNC_ENABLED
    osd_settings->rec_log_verify      = 0;
    osd_settings->enable_fifo         = 1;
    osd_settings->bypass_aio_check    = 0;
    osd_settings->chksum_data         = 1;
    osd_settings->chksum_metadata     = 1;
    osd_settings->sb_data_copies      = 0; // use default
    osd_settings->multi_fifo_writers  = getProperty_Int("SDF_MULTI_FIFO_WRITERS", 1);
    osd_settings->aio_wc              = false;
    osd_settings->aio_error_injection = false;
    osd_settings->aio_queue_len       = MCD_MAX_NUM_FTHREADS;

    // num_threads // legacy--not used

    osd_settings->num_cores        = getProperty_Int("SDF_FTHREAD_SCHEDULERS", 1); // "-N" 
    osd_settings->num_sdf_threads  = getProperty_Int("SDF_THREADS_PER_SCHEDULER", 1); // "-T"

    osd_settings->sdf_log_level    = getProperty_Int("SDF_LOG_LEVEL", 4); 
    osd_settings->aio_num_files    = getProperty_Int("AIO_NUM_FILES", 1); // "-Z"
    osd_settings->aio_sub_files    = 0; // what are these? ignore?
    osd_settings->aio_first_file   = 0; // "-z" index of first file!
    osd_settings->mq_ssd_balance   = 0;  // what does this do?
    osd_settings->no_direct_io     = 0; // do NOT use O_DIRECT
    osd_settings->sdf_persistence  = 0; // "-V" force containers to be persistent!
    osd_settings->max_aio_errors   = getProperty_Int("MEMCACHED_MAX_AIO_ERRORS", 1000 );
    osd_settings->check_delete_in_future = sdf_check_delete_in_future;
}

/*
** Globals
*/
static struct sdf_agent_state agent_state;
static sem_t Mcd_fsched_sem;
static sem_t Mcd_initer_sem;

typedef struct ctnr_map {
    char           *cname;
    SDF_cguid_t     cguid;
    SDF_CONTAINER   sdf_container;
} ctnr_map_t;

static ctnr_map_t CtnrMap[MCD_MAX_NUM_CNTRS];

static int get_ctnr_from_cguid(SDF_cguid_t cguid)
{
    int i;
    int i_ctnr = -1;

    for (i=0; i<MCD_MAX_NUM_CNTRS; i++) {
        if (CtnrMap[i].cguid == cguid) {
	    i_ctnr = i;
	    break;
	}
    }
    return(i_ctnr);
}


#define MCD_FTH_STACKSIZE       81920

static void mcd_fth_initer(uint64_t arg)
{
    /*
     *  Final SDF Initialization
     */
    struct sdf_agent_state    *state = (struct sdf_agent_state *) arg;
    int                        i;

    //  Initialize the container map
    for (i=0; i<MCD_MAX_NUM_CNTRS; i++) {
        CtnrMap[i].cname         = NULL;
        CtnrMap[i].cguid         = 0;
	CtnrMap[i].sdf_container = containerNull;
    }

    if ( SDF_TRUE != agent_engine_post_init( state ) ) {
        mcd_log_msg( 20147, PLAT_LOG_LEVEL_FATAL,
                     "agent_engine_post_init() failed" );
        plat_assert_always( 0 == 1 );
    }

    /*
     * signal the parent thread
     */
    sem_post( &Mcd_initer_sem );
}

static void *scheduler_thread(void *arg)
{
    // mcd_osd_assign_pthread_id();

    /*
     * signal the parent thread
     */
    sem_post( &Mcd_fsched_sem );

    fthSchedulerPthread( 0 );

    return NULL;
}


static int mcd_fth_cleanup( void )
{
    return 0;   /* SUCCESS */
}


static void *run_schedulers(void *arg)
{
    int                 rc;
    int                 i;
    pthread_t           fth_pthreads[MCD_MAX_NUM_SCHED];
    pthread_attr_t      attr;
    int                 num_sched;

    num_sched = (uint64_t) arg;

    /*
     * create the fthread schedulers
     */
    sem_init( &Mcd_fsched_sem, 0, 0 );

    pthread_attr_init( &attr );
    pthread_attr_setdetachstate( &attr, PTHREAD_CREATE_JOINABLE );

    for ( i = 1; i < num_sched; i++ ) {
        rc = pthread_create( &fth_pthreads[i], &attr, scheduler_thread,
                             NULL);
        if ( 0 != rc ) {
            mcd_log_msg( 20163, PLAT_LOG_LEVEL_FATAL,
                         "pthread_create() failed, rc=%d", rc );
	    plat_assert(0);
        }
        mcd_log_msg( 20164, PLAT_LOG_LEVEL_DEBUG, "scheduler %d created",
                     (int)fth_pthreads[i] );
    }

    /*
     * wait for fthreads initialization
     */
    for ( i = 1; i < num_sched; i++ ) {
        do {
            rc = sem_wait( &Mcd_fsched_sem );
        } while (rc == -1 && errno == EINTR);
        plat_assert( 0 == rc );
    }

    /*
     * Turn the main thread into a fthread scheduler directly as a
     * workaround for ensuring all the fthread stacks are registered
     * from the correct pthread in the N1 case.
     */

    fthSchedulerPthread( 0 );

    for ( i = 1; i < num_sched; i++ ) {
        pthread_join( fth_pthreads[i], NULL );
    }

    mcd_fth_cleanup();

    return(NULL);
}

/*
** API
*/
SDF_status_t SDFInit(
	struct SDF_state 	**sdf_state, 
	int 			  argc, 
	char 			**argv
	)
{
    int                 rc;
    pthread_t           run_sched_pthread;
    pthread_attr_t      attr;
    uint64_t            num_sched;

    sem_init( &Mcd_initer_sem, 0, 0 );

    *sdf_state = (struct SDF_state *) &agent_state;

    mcd_aio_register_ops();
    mcd_osd_register_ops();

    //  Initialize a crap-load of settings
    load_settings(&(agent_state.flash_settings));

    //  Set the logging level
    set_log_level(agent_state.flash_settings.sdf_log_level);

    if (!agent_engine_pre_init(&agent_state, argc, argv)) {
        return(SDF_FAILURE); 
    }

    // spawn initer thread (like mcd_fth_initer)
    fthResume( fthSpawn( &mcd_fth_initer, MCD_FTH_STACKSIZE ),
               (uint64_t) &agent_state);

    ipf_set_active(1);

    // spawn scheduler startup process
    pthread_attr_init( &attr );
    pthread_attr_setdetachstate( &attr, PTHREAD_CREATE_JOINABLE );

    num_sched = agent_state.flash_settings.num_cores;

    rc = pthread_create( &run_sched_pthread, &attr, run_schedulers,
			 (void *) num_sched);
    if ( 0 != rc ) {
	mcd_log_msg( 20163, PLAT_LOG_LEVEL_FATAL,
		     "pthread_create() failed, rc=%d", rc );
	return rc;
    }
    mcd_log_msg( 150022, PLAT_LOG_LEVEL_DEBUG, "scheduler startup process created");

    // Wait until mcd_fth_initer is done
    do {
	rc = sem_wait( &Mcd_initer_sem );
    } while (rc == -1 && errno == EINTR);
    plat_assert( 0 == rc );

    return(SDF_SUCCESS);
}

struct SDF_thread_state * SDFInitPerThreadState(
	struct SDF_state *sdf_state
	)
{
    struct SDF_thread_state *pts_out;
    SDF_action_init_t       *pai;
    SDF_action_init_t       *pai_new;
    SDF_action_thrd_state_t *pts;

    struct sdf_agent_state    *state = &agent_state;

    pai = &state->ActionInitState;

    pai_new = (SDF_action_init_t *) plat_alloc( sizeof(SDF_action_init_t) );
    plat_assert_always( NULL != pai_new );
    memcpy( pai_new, pai, (size_t) sizeof(SDF_action_init_t));

    //==================================================


    pts = (SDF_action_thrd_state_t *)
        plat_alloc( sizeof(SDF_action_thrd_state_t) );
    plat_assert_always( NULL != pts );
    pts->phs = state->ActionInitState.pcs;

    pai_new->pts = (void *) pts;
    InitActionAgentPerThreadState(pai_new->pcs, pts, pai_new);
    pai_new->paio_ctxt = pts->pai->paio_ctxt;

    pai_new->ctxt = ActionGetContext(pts);

    pts_out = (struct SDF_thread_state *) pai_new;

    return(pts_out);
}

#ifdef SDFAPI      
SDF_status_t SDFGetContainerProps(
	SDF_thread_state_t 	*sdf_thread_state, 
	SDF_cguid_t 		 cguid, 
	SDF_container_props_t 	*pprops
	)
{   
    SDF_status_t             status = SDF_SUCCESS;
    SDF_container_meta_t     meta;
    SDF_internal_ctxt_t     *pai = (SDF_internal_ctxt_t *) sdf_thread_state;

    SDFStartSerializeContainerOp(pai);  
    if ((status = name_service_get_meta(pai, cguid, &meta)) == SDF_SUCCESS) {
        *pprops = meta.properties;      
    }              
    SDFEndSerializeContainerOp(pai);   
                   
    return(status);
}
    
SDF_status_t SDFSetContainerProps(
	SDF_thread_state_t 	*sdf_thread_state, 
	SDF_cguid_t 	 	 cguid,
	SDF_container_props_t 	*pprops
	)
{
    SDF_status_t             status = SDF_SUCCESS;
    SDF_container_meta_t     meta;
    SDF_internal_ctxt_t     *pai = (SDF_internal_ctxt_t *) sdf_thread_state;

    SDFStartSerializeContainerOp(pai);
    if ((status = name_service_get_meta(pai, cguid, &meta)) == SDF_SUCCESS) {
        meta.properties = *pprops;
        status = name_service_put_meta(pai, cguid, &meta);
    }
    SDFEndSerializeContainerOp(pai);

    return(status);
}

#define CONTAINER_PENDING

static uint64_t cid_counter = 0;

SDF_status_t SDFCreateContainer(
	SDF_thread_state_t 	*sdf_thread_state, 
	const char 		*cname,
	SDF_container_props_t 	*properties, 
	SDF_cguid_t 		*cguid 
	)
{
    uint64_t  cid;
    struct SDF_shared_state *state = &sdf_shared_state;
    SDF_status_t status = SDF_FAILURE;
    SDF_shardid_t shardid = SDF_SHARDID_INVALID;
    SDF_container_meta_t *meta = NULL;
    SDF_CONTAINER_PARENT parent = containerParentNull;
    local_SDF_CONTAINER_PARENT lparent = NULL;
    SDF_boolean_t isCMC;
    uint32_t in_shard_count=0;
    SDF_vnode_t home_node;
    uint32_t num_objs = 0;
    const char *writeback_enabled_string;
    SDF_internal_ctxt_t     *pai = (SDF_internal_ctxt_t *) sdf_thread_state;

    plat_log_msg(20819, LOG_CAT, LOG_DBG, "%s", cname);

    if ((!properties->container_type.caching_container) && (!properties->cache.writethru)) {
        plat_log_msg(30572, LOG_CAT, LOG_ERR,
                     "Writeback caching can only be enabled for eviction mode containers");
        return(SDF_FAILURE_INVALID_CONTAINER_TYPE);
    }
    if (!properties->cache.writethru) {
        if (!properties->container_type.caching_container) {
            plat_log_msg(30572, LOG_CAT, LOG_ERR,
                         "Writeback caching can only be enabled for eviction mode containers");
            return(SDF_FAILURE_INVALID_CONTAINER_TYPE);
        } else {
            writeback_enabled_string = getProperty_String("SDF_WRITEBACK_CACHE_SUPPORT", "On");
            if (strcmp(writeback_enabled_string, "On") != 0) {
                plat_log_msg(30575, LOG_CAT, LOG_ERR,
                             "Cannot enable writeback caching for container '%s' because writeback caching is disabled.",
                             cname);

                properties->cache.writethru = SDF_TRUE;
            }
        }
    }

    SDFStartSerializeContainerOp(pai);

    cid = cid_counter++;

    if (strcmp(cname, CMC_PATH) == 0) {
        *cguid = CMC_CGUID;
        isCMC = SDF_TRUE;
        home_node = CMC_HOME;
    } else {
        int i;
        *cguid = generate_cguid(pai, cname, init_get_my_node_id(), cid); // Generate the cguid
	for (i=0; i<MCD_MAX_NUM_CNTRS; i++) {
	    if (CtnrMap[i].cguid == 0) {
	        // this is an unused map entry
		CtnrMap[i].cname = plat_alloc(strlen(cname)+1);
		if (CtnrMap[i].cname == NULL) {
		    status = SDF_FAILURE_MEMORY_ALLOC;
		    SDFEndSerializeContainerOp(pai);
		    return(status);
		}
		strcpy(CtnrMap[i].cname, cname);
		CtnrMap[i].cguid         = *cguid;
		CtnrMap[i].sdf_container = containerNull;
		break;
	    }
	}

	if (i == MCD_MAX_NUM_CNTRS) {
	    plat_log_msg(150023, LOG_CAT,LOG_ERR, "SDFCreateContainer failed for container %s because 128 containers have already been created.", cname);
	    status = SDF_TOO_MANY_CONTAINERS;
	    SDFEndSerializeContainerOp(pai);
	    return(status);
	}
        isCMC = SDF_FALSE;
        home_node = init_get_my_node_id();
    }

    /*
     *  Save the cguid in a useful place so that the replication code in
     *  mcd_ipf.c can find it.
     */

    if (cmc_settings != NULL) {
        struct settings *settings = cmc_settings;
        int  i;

        for (i = 0; i < sizeof(settings->vips) / sizeof(settings->vips[0]); i++) {
            if (properties->container_id.container_id ==
                   settings->vips[i].container_id) {
                   settings->vips[i].cguid = *cguid;
            }
        }
    }

    num_objs = properties->container_id.num_objs;

    /*
     * XXX: Provide default replication parameters for non-CMC containers.
     * It would be better for this to be a test program runtime option for
     * default container properties.
     *
     * 1/6/09: Enabled CMC replication.
     *
     * XXX: Disabling CMC replication doesn't seem to work in the current
     * code, perhaps because it's getting to the replication code with
     * a type of SDF_REPLICATION_NONE?  Try to make the CMC available
     * everywhere since it should be write once.
     */
    if (/*!isCMC &&*/ state->config.always_replicate) {
        if (isCMC && state->config.replication_type != SDF_REPLICATION_SIMPLE) {
            properties->replication.num_replicas = state->config.nnodes;
            properties->replication.num_meta_replicas = 0;
            properties->replication.type = SDF_REPLICATION_SIMPLE;
        } else {
            properties->replication.num_replicas = state->config.always_replicate;
            properties->replication.num_meta_replicas = 1;
            properties->replication.type = state->config.replication_type;
        }

        properties->replication.enabled = 1;
        properties->replication.synchronous = 1;
        if( properties->replication.type == SDF_REPLICATION_V1_2_WAY ) {
            properties->replication.num_replicas = 2;
        }
    }

    /*
       How do we set shard_count :
     1) check if shard_count in the incoming Container properties is non-zero
     2) else use the shard_count from the properties file (by incredibly
        complicated maze of initialization ending up in state->config)
     3) else  use the hard-coded SDF_SHARD_DEFAULT_SHARD_COUNT macro
    */

    in_shard_count = properties->shard.num_shards?properties->shard.num_shards:
        (state->config.shard_count?
         state->config.shard_count:SDF_SHARD_DEFAULT_SHARD_COUNT);

     /* XXX: If we reached here without having set the shard_count in
        container properties, set the property here. In the future we
        might want to assert on this condition.
     */
    if (properties->shard.num_shards == 0) {
        properties->shard.num_shards = in_shard_count;
    }

#ifdef MULTIPLE_FLASH_DEV_ENABLED
    plat_log_msg(21527, LOG_CAT, LOG_INFO, "Container: %s - Multi Devs: %d",
                 path, state->config.flash_dev_count);
#else
    plat_log_msg(21528, LOG_CAT, LOG_INFO, "Container: %s - Single Dev",
                 cname);
#endif
    plat_log_msg(21529, LOG_CAT, LOG_INFO, "Container: %s - Num Shards: %d",
                 cname, properties->shard.num_shards);

    plat_log_msg(21530, LOG_CAT, LOG_INFO, "Container: %s - Num Objs: %d",
                 cname, state->config.num_objs);

    plat_log_msg(21531, LOG_CAT, LOG_INFO, "Container: %s - DEBUG_MULTI_SHARD_INDEX: %d",
                 cname, getProperty_Int("DEBUG_MULTISHARD_INDEX", -1));

    if (ISEMPTY(cname)) {
        status = SDF_INVALID_PARAMETER;
    } else if (doesContainerExistInBackend(pai, cname)) {
        #ifdef CONTAINER_PENDING
        // Unset parent delete flag if with deleted flag
        if (!isContainerParentNull(parent = isParentContainerOpened(cname))) {
                local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, parent);

             if (lparent->delete_pending == SDF_TRUE) {
                    if (!isCMC && (status = name_service_lock_meta(pai, cname)) != SDF_SUCCESS) {
                            plat_log_msg(21532, LOG_CAT, LOG_ERR, "failed to lock %s", cname);
                }

                lparent->delete_pending = SDF_FALSE;

                if (!isCMC && (status = name_service_unlock_meta(pai, cname)) != SDF_SUCCESS) {
                            plat_log_msg(21533, LOG_CAT, LOG_ERR, "failed to unlock %s", cname);
                }

             }

            releaseLocalContainerParent(&lparent); // TODO C++ please!
        }
        #endif
       status = SDF_CONTAINER_EXISTS;
    } else {
        
	SDF_container_props_t 	 properties2 = *properties;
        if ((shardid = build_shard(state, pai, cname, num_objs,
                                   in_shard_count, properties2, *cguid,
                                   isCMC ? BUILD_SHARD_CMC : BUILD_SHARD_OTHER, cname)) <= SDF_SHARDID_LIMIT) {
            if ((meta = build_meta(cname, properties2, *cguid, shardid)) != NULL) {
#ifdef STATE_MACHINE_SUPPORT
                SDFUpdateMetaClusterGroupInfo(pai,meta,properties->container_id.container_id);
#endif
                if (create_put_meta(pai, cname, meta, *cguid) == SDF_SUCCESS) {

                    // For non-CMC, map the cguid and cname
                    if (!isCMC && (name_service_create_cguid_map(pai, cname, *cguid)) != SDF_SUCCESS) {
                        plat_log_msg(21534, LOG_CAT, LOG_ERR,
                                     "failed to map cguid: %s", cname);
                    }
                    if (!isCMC && (status = name_service_lock_meta(pai, cname)) != SDF_SUCCESS) {
                        plat_log_msg(21532, LOG_CAT, LOG_ERR, "failed to lock %s", cname);
                    } else if (!isContainerParentNull(parent = createParentContainer(pai, cname, meta))) {
                        lparent = getLocalContainerParent(&lparent, parent); // TODO C++ please!
                        lparent->container_type = properties->container_type.type;
                        if (lparent->container_type == SDF_BLOCK_CONTAINER) {
                            lparent->blockSize = properties->specific.block_props.blockSize;
                        }
                        releaseLocalContainerParent(&lparent); // TODO C++ please!

                        status = SDF_SUCCESS;

                        if (!isCMC && (status = name_service_unlock_meta(pai, cname)) != SDF_SUCCESS) {
                            plat_log_msg(21533, LOG_CAT, LOG_ERR, "failed to unlock %s", cname);
                        }
                    } else {
                        plat_log_msg(21535, LOG_CAT, LOG_ERR, "cname=%s, build_shard failed", cname);
                    }
                } else {
                    plat_log_msg(21536, LOG_CAT, LOG_ERR, "cname=%s, createParentContainer() failed", cname);
                }

                container_meta_destroy(meta);

            } else {
                plat_log_msg(21537, LOG_CAT, LOG_ERR, "cname=%s, build_meta failed", cname);
            }
        } else {
            plat_log_msg(21535, LOG_CAT, LOG_ERR, "cname=%s, build_shard failed", cname);
        }
    }

    plat_log_msg(21511, LOG_CAT, LOG_DBG, "%s - %s", cname, SDF_Status_Strings[status]);

    if (status != SDF_SUCCESS && status != SDF_CONTAINER_EXISTS) {
        plat_log_msg(21538, LOG_CAT, LOG_ERR, "cname=%s, function returned status = %u", cname, status);
        name_service_remove_meta(pai, cname);
#if 0
        /*
         * XXX We're leaking the rest of the shard anyways, and this could
         * cause dangling pointer problems to manifest from the coalesce, etc.
         * internal flash threads so skip it.
         */
        shardDelete(shard);
        xxxzzz continue from here
#endif
    }
#ifdef SIMPLE_REPLICATION
/*
    else if (status == SDF_SUCCESS) {
        SDFRepDataStructAddContainer(pai, properties, *cguid);
    }
*/
#endif

    SDFEndSerializeContainerOp(pai);
    return (status);
}

SDF_status_t SDFOpenContainer(
	SDF_thread_state_t 	*sdf_thread_state, 
	SDF_cguid_t 	 	 cguid,
	SDF_container_mode_t 	 mode
	) 
{               
                    
    SDF_status_t status = SDF_SUCCESS;
    local_SDF_CONTAINER lc = NULL;
    SDF_CONTAINER_PARENT parent;
    local_SDF_CONTAINER_PARENT lparent = NULL;
    int log_level = LOG_ERR;
    char *path = NULL;
    int  i_ctnr;
    SDF_CONTAINER container = containerNull;
    SDF_internal_ctxt_t     *pai = (SDF_internal_ctxt_t *) sdf_thread_state;
                        
    plat_log_msg(21630, LOG_CAT, LOG_INFO, "%lu", cguid);

    SDFStartSerializeContainerOp(pai);

    i_ctnr = get_ctnr_from_cguid(cguid);

    if (i_ctnr == -1) {
	fprintf(stderr, "SDFOpenContainer: NULL mcd_container\n");
        status = SDF_INVALID_PARAMETER;
    } else {
	path = CtnrMap[i_ctnr].cname;
    }
             
    if (ISEMPTY(path)) { 
        status = SDF_INVALID_PARAMETER;
    } else if (!isContainerParentNull(parent = isParentContainerOpened(path))) {
                
        plat_log_msg(20819, LOG_CAT, LOG_INFO, "%s", path);

        // Test for pending delete
        lparent = getLocalContainerParent(&lparent, parent);
        if (lparent->delete_pending == SDF_TRUE) {
            // Need a different error?
            status = SDF_CONTAINER_UNKNOWN;
            plat_log_msg(21552, LOG_CAT,LOG_ERR, "Delete pending for %s", path);

        } 
        releaseLocalContainerParent(&lparent);
    }

    if (status == SDF_SUCCESS) {

        // Ok to open
        container = openParentContainer(pai, path);

	CtnrMap[i_ctnr].sdf_container = container;

        if (!isContainerNull(container)) {
            lc = getLocalContainer(&lc, container);
            lc->mode = mode; // (container)->mode = mode;
            _sdf_print_container_descriptor(container);
            log_level = LOG_DBG;

            if (cmc_settings != NULL) {
                 struct settings *settings = cmc_settings;
                 int  i;

                 for (i = 0;
                      i < sizeof(settings->vips) / sizeof(settings->vips[0]);
                      i++)
                 {
                     if (lc->container_id == settings->vips[i].container_id) {
                         settings->vips[i].cguid = lc->cguid;

                         plat_log_msg(21553, LOG_CAT, LOG_DBG, "set cid %d (%d) to cguid %d\n",
                         (int) settings->vips[i].container_id,
                         (int) i,
                         (int) lc->cguid);
                     }
                 }
            }

            // FIXME: This is where the call to shardOpen goes.
            #define MAX_SHARDIDS 32 // Not sure what max is today
            SDF_shardid_t shardids[MAX_SHARDIDS];
            uint32_t shard_count;
            get_container_shards(pai, lc->cguid, shardids, MAX_SHARDIDS, &shard_count);
            for (int i = 0; i < shard_count; i++) {
                struct SDF_shared_state *state = &sdf_shared_state;
                shardOpen(state->config.flash_dev, shardids[i]);
            }

            status = SDFActionOpenContainer(pai, lc->cguid);
            if (status != SDF_SUCCESS) {
                plat_log_msg(21554, LOG_CAT,LOG_ERR, "SDFActionOpenContainer failed for container %s", path);
            }

            releaseLocalContainer(&lc);
            plat_log_msg(21555, LOG_CAT, LOG_DBG, "Opened %s", path);
        } else {
            status = SDF_CONTAINER_UNKNOWN;
            plat_log_msg(21556, LOG_CAT,LOG_ERR, "Failed to find %s", path);
        }
    }

    if (path) {
	plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", path, SDF_Status_Strings[status]);
    } else {
	plat_log_msg(150024, LOG_CAT, log_level, "NULL - %s", SDF_Status_Strings[status]);
    }

    SDFEndSerializeContainerOp(pai);
    return (status);
}

/*
** Temporary for internal compatibility
*/
SDF_status_t SDFOpenContainerPath(
	SDF_thread_state_t 	*sdf_thread_state, 
	const char 		*path, 
	SDF_container_mode_t 	 mode,
        SDF_CONTAINER 		*container
	) 
{
    SDF_status_t status = SDF_SUCCESS;
    local_SDF_CONTAINER lc = NULL;
    SDF_CONTAINER_PARENT parent;
    local_SDF_CONTAINER_PARENT lparent = NULL;
    int log_level = LOG_ERR;
    SDF_internal_ctxt_t     *pai = (SDF_internal_ctxt_t *) sdf_thread_state;

    SDFStartSerializeContainerOp(pai);

    if (ISEMPTY(path)) {
        status = SDF_INVALID_PARAMETER;

    } else if (NULL == container) {
        status = SDF_FAILURE_CONTAINER_GENERIC;

    } else if (!isContainerParentNull(parent = isParentContainerOpened(path))) {

        // Test for pending delete
        lparent = getLocalContainerParent(&lparent, parent);
        if (lparent->delete_pending == SDF_TRUE) {
            // Need a different error?
            status = SDF_CONTAINER_UNKNOWN;
        }
        releaseLocalContainerParent(&lparent);
    }

    if (status == SDF_SUCCESS) {

        // Ok to open
        *container = openParentContainer(pai, path);

        if (!isContainerNull(*container)) {
            lc = getLocalContainer(&lc, *container);
            lc->mode = mode; // (*container)->mode = mode;
            _sdf_print_container_descriptor(*container);
            log_level = LOG_DBG;
            if (cmc_settings != NULL) {
                 struct settings *settings = cmc_settings;
                 int  i;

                 for (i = 0;
                      i < sizeof(settings->vips) / sizeof(settings->vips[0]);
                      i++)
                 {
                     if (lc->container_id == settings->vips[i].container_id) {
                         settings->vips[i].cguid = lc->cguid;

                         plat_log_msg(21553, LOG_CAT, LOG_DBG, "set cid %d (%d) to cguid %d\n",
                         (int) settings->vips[i].container_id,
                         (int) i,
                         (int) lc->cguid);
                     }
                 }
            }

            // FIXME: This is where the call to shardOpen goes.
            #define MAX_SHARDIDS 32 // Not sure what max is today
            SDF_shardid_t shardids[MAX_SHARDIDS];
            uint32_t shard_count;
            get_container_shards(pai, lc->cguid, shardids, MAX_SHARDIDS, &shard_count);
            for (int i = 0; i < shard_count; i++) {
                struct SDF_shared_state *state = &sdf_shared_state;
                shardOpen(state->config.flash_dev, shardids[i]);
            }

            status = SDFActionOpenContainer(pai, lc->cguid);
            if (status != SDF_SUCCESS) {
            }

            releaseLocalContainer(&lc);
        } else {
            status = SDF_CONTAINER_UNKNOWN;
        }
    }

    SDFEndSerializeContainerOp(pai);
    return (status);
}

SDF_status_t SDFCloseContainer(
        SDF_thread_state_t      *sdf_thread_state,
	SDF_cguid_t  		 cguid
	)
{
    SDF_status_t status = SDF_FAILURE;
    unsigned     n_descriptors;
    int  i_ctnr;
    SDF_CONTAINER container = containerNull;
    SDF_internal_ctxt_t     *pai = (SDF_internal_ctxt_t *) sdf_thread_state;
    SDF_status_t lock_status = SDF_SUCCESS;
    int log_level = LOG_ERR;
    int ok_to_delete = 0;
    SDF_cguid_t parent_cguid = SDF_NULL_CGUID;

    plat_log_msg(21630, LOG_CAT, LOG_INFO, "%lu", cguid);

    SDFStartSerializeContainerOp(pai);

    i_ctnr = get_ctnr_from_cguid(cguid);

    if (i_ctnr == -1) {
        status = SDF_INVALID_PARAMETER;
    } else {
	container = CtnrMap[i_ctnr].sdf_container;
    }

    if (isContainerNull(container)) {
        status = SDF_FAILURE_CONTAINER_GENERIC;
    } else {

        // Delete the container if there are no outstanding opens and a delete is pending
        local_SDF_CONTAINER lcontainer = getLocalContainer(&lcontainer, container);
        SDF_CONTAINER_PARENT parent = lcontainer->parent;
        local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, parent);
        char path[MAX_OBJECT_ID_SIZE] = "";

        if (lparent->name) {
            memcpy(&path, lparent->name, strlen(lparent->name));
        }

        if (lparent->num_open_descriptors == 1 && lparent->delete_pending == SDF_TRUE) {
            ok_to_delete = 1;
        }
        // copy these from lparent before I nuke it!
        n_descriptors = lparent->num_open_descriptors;
        parent_cguid  = lparent->cguid;
        releaseLocalContainerParent(&lparent);

        if (closeParentContainer(container)) {
            status = SDF_SUCCESS;
            log_level = LOG_DBG;
        }

        // FIXME: This is where shardClose call goes.
        if (n_descriptors == 1) {
            #define MAX_SHARDIDS 32 // Not sure what max is today
            SDF_shardid_t shardids[MAX_SHARDIDS];
            uint32_t shard_count;
            get_container_shards(pai, parent_cguid, shardids, MAX_SHARDIDS, &shard_count);
            for (int i = 0; i < shard_count; i++) {
                //shardClose(shardids[i]);
            }
        }

        if (status == SDF_SUCCESS && ok_to_delete) {

            if ((status = name_service_lock_meta(pai, path)) == SDF_SUCCESS) {

                // Flush and invalidate all of the container's cached objects
                if ((status = name_service_inval_object_container(pai, path)) != SDF_SUCCESS) {
                    plat_log_msg(21540, LOG_CAT, LOG_ERR,
                                 "%s - failed to flush and invalidate container", path);
                    log_level = LOG_ERR;
                } else {
                    plat_log_msg(21541, LOG_CAT, LOG_DBG,
                                 "%s - flush and invalidate container succeed", path);
                }

                // Remove the container shards
                if (status == SDF_SUCCESS &&
                    (status = name_service_delete_shards(pai, path)) != SDF_SUCCESS) {
                    plat_log_msg(21544, LOG_CAT, LOG_ERR,
                                 "%s - failed to delete container shards", path);
                    log_level = LOG_ERR;
                } else {
                    plat_log_msg(21545, LOG_CAT, LOG_DBG,
                                 "%s - delete container shards succeeded", path);
                }

                // Remove the container metadata
                if (status == SDF_SUCCESS &&
                    (status = name_service_remove_meta(pai, path)) != SDF_SUCCESS) {
                    plat_log_msg(21546, LOG_CAT, LOG_ERR,
                                 "%s - failed to remove metadata", path);
                    log_level = LOG_ERR;
                } else {
                    plat_log_msg(21547, LOG_CAT, LOG_DBG,
                                 "%s - remove metadata succeeded", path);
                }

                // Unlock the metadata - only return lock error status if no other errors
                if ((lock_status == name_service_unlock_meta(pai, path)) != SDF_SUCCESS) {
                    plat_log_msg(21548, LOG_CAT, LOG_ERR,
                                 "%s - failed to unlock metadata", path);
                    if (status == SDF_SUCCESS) {
                        status = lock_status;
                        log_level = LOG_ERR;
                    }
                } else {
                    plat_log_msg(21549, LOG_CAT, LOG_DBG,
                                 "%s - unlock metadata succeeded", path);
                }

                // Remove the container guid map
                if (status == SDF_SUCCESS &&
                    name_service_remove_cguid_map(pai, path) != SDF_SUCCESS) {
                    plat_log_msg(21550, LOG_CAT, LOG_ERR,
                                 "%s - failed to remove cguid map", path);
                    log_level = LOG_ERR;
                } else {
                    plat_log_msg(21551, LOG_CAT, LOG_DBG,
                                 "%s - remove cguid map succeeded", path);
                }
            }
        }
    }

    plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);

    SDFEndSerializeContainerOp(pai);
    return (status);
}

/*
** Temporary
*/
SDF_status_t SDFCloseContainerPath(
        SDF_thread_state_t      *sdf_thread_state,
	SDF_CONTAINER 		container
	)
{
    SDF_status_t status = SDF_FAILURE;
    SDF_cguid_t  cguid;
    unsigned     n_descriptors;
    SDF_internal_ctxt_t     *pai = (SDF_internal_ctxt_t *) sdf_thread_state;

    SDF_status_t lock_status = SDF_SUCCESS;
    int log_level = LOG_ERR;
    int ok_to_delete = 0;

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, " ");

    SDFStartSerializeContainerOp(pai);

    if (isContainerNull(container)) {
        status = SDF_FAILURE_CONTAINER_GENERIC;
    } else {

        // Delete the container if there are no outstanding opens and a delete is pending
        local_SDF_CONTAINER lcontainer = getLocalContainer(&lcontainer, container);
        SDF_CONTAINER_PARENT parent = lcontainer->parent;
        local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, parent);
        char path[MAX_OBJECT_ID_SIZE] = "";

        if (lparent->name) {
            memcpy(&path, lparent->name, strlen(lparent->name));
        }

        if (lparent->num_open_descriptors == 1 && lparent->delete_pending == SDF_TRUE) {
            ok_to_delete = 1;
        }
        // copy these from lparent before I nuke it!
        n_descriptors = lparent->num_open_descriptors;
        cguid         = lparent->cguid;
        releaseLocalContainerParent(&lparent);

        if (closeParentContainer(container)) {
            status = SDF_SUCCESS;
            log_level = LOG_DBG;
        }

        // FIXME: This is where shardClose call goes.
        if (n_descriptors == 1) {
            #define MAX_SHARDIDS 32 // Not sure what max is today
            SDF_shardid_t shardids[MAX_SHARDIDS];
            uint32_t shard_count;
            get_container_shards(pai, cguid, shardids, MAX_SHARDIDS, &shard_count);
            for (int i = 0; i < shard_count; i++) {
                //shardClose(shardids[i]);
            }
        }

        if (status == SDF_SUCCESS && ok_to_delete) {

            if ((status = name_service_lock_meta(pai, path)) == SDF_SUCCESS) {

                // Flush and invalidate all of the container's cached objects
                if ((status = name_service_inval_object_container(pai, path)) != SDF_SUCCESS) {
                    plat_log_msg(21540, LOG_CAT, LOG_ERR,
                                 "%s - failed to flush and invalidate container", path);
                    log_level = LOG_ERR;
                } else {
                    plat_log_msg(21541, LOG_CAT, LOG_DBG,
                                 "%s - flush and invalidate container succeed", path);
                }

                // Remove the container shards
                if (status == SDF_SUCCESS &&
                    (status = name_service_delete_shards(pai, path)) != SDF_SUCCESS) {
                    plat_log_msg(21544, LOG_CAT, LOG_ERR,
                                 "%s - failed to delete container shards", path);
                    log_level = LOG_ERR;
                } else {
                    plat_log_msg(21545, LOG_CAT, LOG_DBG,
                                 "%s - delete container shards succeeded", path);
                }
                // Remove the container metadata
                if (status == SDF_SUCCESS &&
                    (status = name_service_remove_meta(pai, path)) != SDF_SUCCESS) {
                    plat_log_msg(21546, LOG_CAT, LOG_ERR,
                                 "%s - failed to remove metadata", path);
                    log_level = LOG_ERR;
                } else {
                    plat_log_msg(21547, LOG_CAT, LOG_DBG,
                                 "%s - remove metadata succeeded", path);
                }

                // Unlock the metadata - only return lock error status if no other errors
                if ((lock_status == name_service_unlock_meta(pai, path)) != SDF_SUCCESS) {
                    plat_log_msg(21548, LOG_CAT, LOG_ERR,
                                 "%s - failed to unlock metadata", path);
                    if (status == SDF_SUCCESS) {
                        status = lock_status;
                        log_level = LOG_ERR;
                    }
                } else {
                    plat_log_msg(21549, LOG_CAT, LOG_DBG,
                                 "%s - unlock metadata succeeded", path);
                }

                // Remove the container guid map
                if (status == SDF_SUCCESS &&
                    name_service_remove_cguid_map(pai, path) != SDF_SUCCESS) {
                    plat_log_msg(21550, LOG_CAT, LOG_ERR,
                                 "%s - failed to remove cguid map", path);
                    log_level = LOG_ERR;
                } else {
                    plat_log_msg(21551, LOG_CAT, LOG_DBG,
                                 "%s - remove cguid map succeeded", path);
                }
            }
        }
    }

    plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);
    SDFEndSerializeContainerOp(pai);
    return (status);
}

SDF_status_t SDFDeleteContainer(
	SDF_thread_state_t	*sdf_thread_state,
	SDF_cguid_t		 cguid
	)
{  
    SDF_status_t status = SDF_FAILURE;
    char *path = NULL;
    int  i_ctnr;
    SDF_internal_ctxt_t *pai = (SDF_internal_ctxt_t *) sdf_thread_state;

    i_ctnr = get_ctnr_from_cguid(cguid);

    if (i_ctnr == -1) {
        status = SDF_INVALID_PARAMETER;
    } else {
	path = CtnrMap[i_ctnr].cname;
    	if (ISEMPTY(path)) {
       	    status = SDF_INVALID_PARAMETER;
	} else {
	    status = delete_container_internal(pai, path, SDF_TRUE /* serialize */);
	}
    }
    plat_free(CtnrMap[i_ctnr].cname);
    CtnrMap[i_ctnr].cguid         = 0;
    CtnrMap[i_ctnr].sdf_container = containerNull;

    return status;
}

/*
** Temporary
*/
SDF_status_t SDFDeleteContainerPath(
	SDF_thread_state_t	*sdf_thread_state,
	const char 		*path
	)
{  
    SDF_internal_ctxt_t *pai = (SDF_internal_ctxt_t *) sdf_thread_state;

    return(delete_container_internal(pai, path, SDF_TRUE /* serialize */));
}

SDF_status_t SDFStartContainer(
	SDF_thread_state_t *sdf_thread_state, 
	SDF_cguid_t cguid
	)
{
    SDF_status_t             status = SDF_SUCCESS;
    SDF_container_meta_t     meta;
    struct shard            *shard = NULL;
    struct SDF_shared_state *state = &sdf_shared_state;
    flashDev_t              *flash_dev;
    SDF_internal_ctxt_t     *pai = (SDF_internal_ctxt_t *) sdf_thread_state;

    SDFStartSerializeContainerOp(pai);

    if ((status = name_service_get_meta(pai, cguid, &meta)) == SDF_SUCCESS) {

        meta.stopflag = SDF_FALSE;
        if ((status = name_service_put_meta(pai, cguid, &meta)) == SDF_SUCCESS) {

            #ifdef MULTIPLE_FLASH_DEV_ENABLED
                flash_dev = get_flashdev_from_shardid(state->config.flash_dev,
                                                      meta.shard, state->config.flash_dev_count);
            #else
                flash_dev = state->config.flash_dev;
            #endif
            shard = shardFind(flash_dev, meta.shard);

            shardStart(shard);

            /* Clean up additional action node state for the container.
             */
            status = SDFActionStartContainer(pai, &meta);
        } else {
            plat_log_msg(21539, LOG_CAT, LOG_ERR,
                         "name_service_put_meta failed for cguid %"PRIu64"", cguid);
        }
    }


    SDFEndSerializeContainerOp(pai);
    return(status);
}

SDF_status_t SDFStopContainer(
	SDF_thread_state_t *sdf_thread_state, 
	SDF_cguid_t cguid
	)
{
    SDF_status_t             status = SDF_SUCCESS;
    SDF_container_meta_t     meta;
    struct shard            *shard = NULL;
    struct SDF_shared_state *state = &sdf_shared_state;
    flashDev_t              *flash_dev;
    SDF_internal_ctxt_t     *pai = (SDF_internal_ctxt_t *) sdf_thread_state;

    SDFStartSerializeContainerOp(pai);
    if ((status = name_service_get_meta(pai, cguid, &meta)) == SDF_SUCCESS) {

        meta.stopflag = SDF_TRUE;
        if ((status = name_service_put_meta(pai, cguid, &meta)) == SDF_SUCCESS) {

            #ifdef MULTIPLE_FLASH_DEV_ENABLED
                flash_dev = get_flashdev_from_shardid(state->config.flash_dev,
                                                      meta.shard, state->config.flash_dev_count);
            #else
                flash_dev = state->config.flash_dev;
            #endif
            shard = shardFind(flash_dev, meta.shard);

            shardStop(shard);

            /* Clean up additional action node state for the container.
             */
            status = SDFActionStopContainer(pai, &meta);
        } else {
            plat_log_msg(21539, LOG_CAT, LOG_ERR,
                         "name_service_put_meta failed for cguid %"PRIu64"", cguid);
        }
    }

    SDFEndSerializeContainerOp(pai);
    return(status);
}

SDF_status_t SDFGetContainers(
	SDF_thread_state_t	*sdf_thread_state,
	SDF_cguid_t             *cguids,
	uint32_t                *n_cguids
	)
{
    SDF_action_init_t  *pac;

    pac = (SDF_action_init_t *) sdf_thread_state;

    mcd_osd_get_containers_cguids(((SDF_action_init_t *) pac)->paio_ctxt, cguids, n_cguids);

    return(SDF_SUCCESS);
}

SDF_status_t SDFFlushContainer(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_cguid_t               cguid,
	SDF_time_t                current_time
	)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;

    pac = (SDF_action_init_t *) sdf_thread_state;

    ar.reqtype = APFCO;
    ar.curtime = current_time;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDFGetForReadBufferedObject(
	struct SDF_thread_state   *sdf_thread_state,
	SDF_cguid_t                cguid,
	char                      *key,
	uint32_t                   keylen,
	char                     **data,
	uint64_t                  *datalen,
	SDF_time_t                 current_time,
	SDF_time_t                *expiry_time
	)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) sdf_thread_state;
   
    ar.reqtype = APGRX;
    ar.curtime = current_time;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    if (data == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }
    ar.ppbuf_in = (void **)data;

    ActionProtocolAgentNew(pac, &ar);

    if (datalen == NULL) {
        return(SDF_BAD_SIZE_POINTER);
    }
    *datalen = ar.destLen;

    if (expiry_time == NULL) {
        return(SDF_BAD_PEXPTIME_POINTER);
    }
    *expiry_time     = ar.exptime;

    return(ar.respStatus);
}

SDF_status_t SDFGetBuffer(
                   struct SDF_thread_state  *sdf_thread_state,
		   char                     **data,
		   uint64_t                   datalen
               )
{
    char *p;
    p = (char *) plat_alloc(datalen);
    if (p) {
        *data = p;
	return(SDF_SUCCESS);
    } else {
	return(SDF_FAILURE);
    }
}

SDF_status_t SDFCreateBufferedObject(
                   struct SDF_thread_state  *sdf_thread_state,
		   SDF_cguid_t          cguid,
		   char                *key,
		   uint32_t             keylen,
		   char                *data,
		   uint64_t             datalen,
		   SDF_time_t           current_time,
		   SDF_time_t           expiry_time
	       )
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) sdf_thread_state;

    ar.reqtype = APCOE;
    ar.curtime = current_time;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    ar.sze = datalen;
    ar.pbuf_out = (void *) data;
    ar.exptime = expiry_time;
    if (data == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDFSetBufferedObject(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_cguid_t          	  cguid,
	char                	 *key,
	uint32_t             	  keylen,
	char                	 *data,
	uint64_t             	  datalen,
	SDF_time_t           	  current_time,
	SDF_time_t           	  expiry_time
	)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) sdf_thread_state;

    ar.reqtype = APSOE;
    ar.curtime = current_time;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    ar.sze = datalen;
    ar.pbuf_out = (void *) data;
    ar.exptime = expiry_time;
    if (data == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDFPutBufferedObject(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_cguid_t          	  cguid,
	char                	 *key,
	uint32_t             	  keylen,
	char                	 *data,
	uint64_t             	  datalen,
	SDF_time_t           	  current_time,
	SDF_time_t           	  expiry_time
	)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) sdf_thread_state;
   
    ar.reqtype = APPAE;
    ar.curtime = current_time;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    ar.sze = datalen;
    ar.pbuf_out = (void *) data;
    ar.exptime = expiry_time;
    if (data == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDFRemoveObjectWithExpiry(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_cguid_t          	  cguid,
	char                	 *key,
	uint32_t             	  keylen,
	SDF_time_t           	  current_time
	)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;
    
    pac = (SDF_action_init_t *) sdf_thread_state;
    
    ar.reqtype = APDBE;
    //ar.prefix_delete = pref_del;
    ar.curtime = current_time;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != SDF_SUCCESS) {
        return(status); 
    }
    
    ActionProtocolAgentNew(pac, &ar);
    
    return(ar.respStatus);
}

SDF_status_t SDFFlushObject(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_cguid_t          	  cguid,
	char                	 *key,
	uint32_t             	  keylen,
	SDF_time_t           	  current_time
	)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) sdf_thread_state;
   
    ar.reqtype = APFLS;
    ar.curtime = current_time;
    ar.ctxt = pac->ctxt;
    ar.ctnr = cguid;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) key, keylen)) != SDF_SUCCESS) {
        return(status);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_cguid_t SDFGenerateCguid(
	SDF_thread_state_t *sdf_thread_state, 
	int64_t 	    cntr_id64
	)
{
    SDF_cguid_t cguid;

    /*   xxxzzz briano
     *   cguid's must be global across all nodes.
     *   Eg: a given container must have the same cguid at any node
     *   on which it has a replica.  We also ensure that all of its
     *   shard id's are the same at any node on which it has a
     *   replica.
     *   cguid = <port_number> | <container_id>
     *
     *   This MUST be kept in sync with the code in memcached that
     *   generates the path as "%s%5d%.8x" for <name>|<port>|<ctnr_id>.
     */

    /*  xxxzzz briano
     *  For now I am NOT including the port number in the cguid.
     *  This is so that we can run 2 or more instances of memcached on
     *  the same physical node without port conflicts.  It decouples
     *  the port number from the global cguid.  Consequently, a container
     *  can have different port numbers on different nodes for the same
     *  container (but this will require different property files per
     *  node).  Alas, this is necessary to support multiple memcached
     *  processes per system.
     */
    cguid = cntr_id64 + NODE_MULTIPLIER; // so they don't collide with CMC_CGUID=1

    plat_log_msg(150019, LOG_CAT, LOG_DBG,
                 "SDFGenerateCguid: %llu", (unsigned long long) cguid);

    return (cguid);
}

SDF_status_t SDFFlushCache(
	struct SDF_thread_state  *sdf_thread_state,
	SDF_time_t           	  current_time
	)
{
    return(SDF_SUCCESS);
}

SDF_status_t SDFEnumerateContainerObjects(
	struct SDF_thread_state *sdf_thread_state,
	SDF_cguid_t              cguid
	)
{
    // xxxzzz finish this!
    return(SDF_SUCCESS);
}

SDF_status_t SDFNextEnumeratedObject(
	struct SDF_thread_state *sdf_thread_state,
	SDF_cguid_t              cguid,
	char                    **key,
	uint32_t                *keylen,
	char                    **data,
	uint64_t                *datalen
	)
{
    // xxxzzz finish this!
    return(SDF_SUCCESS);
}

SDF_status_t SDFFinishEnumeration(
                   struct SDF_thread_state *sdf_thread_state,
		   SDF_cguid_t              cguid
	       )
{
    // xxxzzz finish this!
    return(SDF_SUCCESS);
}

SDF_status_t SDFGetStats(
	struct SDF_thread_state *sdf_thread_state,
	SDF_stats_t             *stats
	)
{
    //  no-op in this simple implementation
    return(SDF_SUCCESS);
}

SDF_status_t SDFGetContainerStats(
	struct SDF_thread_state   *sdf_thread_state,
	SDF_cguid_t                cguid,
	SDF_container_stats_t      stats
	)
{
    //  no-op in this simple implementation
    return(SDF_SUCCESS);
}

SDF_status_t SDFBackupContainer(
	struct SDF_thread_state   *sdf_thread_state,
	SDF_cguid_t                cguid,
	char                      *backup_directory
	)
{
    //  no-op in this simple implementation
    return(SDF_SUCCESS);
}

SDF_status_t SDFRestoreContainer(
	struct SDF_thread_state   *sdf_thread_state,
	SDF_cguid_t                cguid,
	char                      *backup_directory
	)
{
    //  no-op in this simple implementation
    return(SDF_SUCCESS);
}

SDF_status_t SDFFreeBuffer(
	struct SDF_thread_state  *sdf_thread_state,
	char                     *data
	)
{
    free(data);
    return(SDF_SUCCESS);
}

    /******************************************************
     *
     *  Callback functions for simple_replication.
     *
     ******************************************************/

/*
 * For simple replication: if simple replication is enabled,
 * persistent container will start in stopped mode and requires to
 * be explicitly turned on. If only one node failed, then the
 * recovery module should call this function at the beginning of
 * recovery to wipe the old content out. If both nodes failed, then
 * users need to select a node and manually start it.
 *
 * Return codes:
 *   -EBUSY:    the container is busy and needs to be stopped first
 *   -ENOENT:   container not found
 */
int mcd_format_container_byname_internal( void * pai, char * cname )
{
    #ifdef notdef
    mcd_log_msg(50006, PLAT_LOG_LEVEL_INFO,
                 "formatting container, name=%s", cname );

    for ( int i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
	if (pMcd_containers[i] == NULL) { continue; }
        if ( 0 == strncmp( cname, mcd_osd_container_cname(pMcd_containers[i]),
                           MCD_CNTR_NAME_MAXLEN ) ) {
            if ( cntr_stopped != mcd_osd_container_state(pMcd_containers[i]) ) {
                return -EBUSY;
            }
            return mcd_do_format_container( pai, pMcd_containers[i], true );
        }
    }
    #endif

    return -ENOENT;
}


/*
 * Return codes:
 *   1: container is active and running
 *   0: container is not running
 *   -ENOENT: container not found
 */
int mcd_is_container_running_byname( char * cname )
{
    #ifdef notdef
    for ( int i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
	if (pMcd_containers[i] == NULL) { continue; }
        if ( 0 == strncmp( cname, mcd_osd_container_cname(pMcd_containers[i]),
                           MCD_CNTR_NAME_MAXLEN ) ) {
            return ( cntr_running == mcd_osd_container_state(pMcd_containers[i]) );
        }
    }
    #endif

    return -ENOENT;
}


/*
 * returns the TCP port number for the container with the specified cguid
 *
 * Return codes:
 *   0: success
 *   -ENOENT: specified container not found
 */
int mcd_get_tcp_port_by_cguid( SDF_cguid_t cguid, int * tcp_port )
{
    #ifdef notdef
    for ( int i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
	if (pMcd_containers[i] == NULL) { continue; }
        if ( cguid == mcd_osd_container_cguid(pMcd_containers[i]) ) {
            *tcp_port = mcd_osd_container_tcp_port(pMcd_containers[i]);
            return 0;
        }
    }
    #endif

    return -ENOENT;
}


/*
 * returns the UDP port number for the container with the specified cguid
 *
 * Return codes:
 *   0: success
 *   -ENOENT: specified container not found
 */
int mcd_get_udp_port_by_cguid( SDF_cguid_t cguid, int * udp_port )
{
    #ifdef notdef
    for ( int i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
	if (pMcd_containers[i] == NULL) { continue; }
        if ( cguid == mcd_osd_container_cguid(pMcd_containers[i]) ) {
            *udp_port = mcd_osd_container_udp_port(pMcd_containers[i]);
            return 0;
        }
    }
    #endif

    return -ENOENT;
}

/*
 * return the container name for the container with the specified cguid
 *
 * @param cname <OUT> buffer for storing container name (must >= 64 bytes)
 *
 * Return codes:
 *   0: success
 *   -ENOENT: specified container not found
 */
int mcd_get_cname_by_cguid( SDF_cguid_t cguid, char * cname )
{
    #ifdef notdef
    for ( int i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
	if (pMcd_containers[i] == NULL) { continue; }
        if ( cguid == mcd_osd_container_cguid(pMcd_containers[i]) ) {
            snprintf( cname, 64, "%s", mcd_osd_container_cname(pMcd_containers[i]) );
            return 0;
        }
    }
    #endif

    return -ENOENT;
}


/*
 * check whether there is any container update activities going on
 * Return codes:
 *   1: there are ongoing container update operations
 *   0: no ongoing container update operations
 */
int mcd_processing_container_cmds( void )
{
    #ifdef notdef
    return __sync_fetch_and_or( &Mcd_cntr_cmd_count, 0 );
    #endif

    return -ENOENT;
}

int mcd_vip_server_socket(struct vip *vip, Network_Port_Type port_type)
{
    /*
     * FIXME_BINARY: time to remove this function
     */
    plat_abort();
}


/*
 * FIXME: obsolete now and to be removed
 */
int mcd_format_container_internal( void * pai, int tcp_port )
{
    #ifdef notdef
    mcd_log_msg(20728, PLAT_LOG_LEVEL_INFO, "formatting container, port=%d",
                 tcp_port );

    if ( settings.ips_per_cntr ) {
        plat_abort();
    }

    for ( int i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
	if (pMcd_containers[i] == NULL) { continue; }
        if ( tcp_port == mcd_osd_container_tcp_port(pMcd_containers[i]) ) {
            if ( cntr_stopped != mcd_osd_container_state(pMcd_containers[i]) ) {
                return -EBUSY;
            }
            return mcd_do_format_container( pai, pMcd_containers[i], true );
        }
    }
    #endif

    return -ENOENT;
}


/*
 * FIXME: obsolete now and to be removed
 */
int mcd_is_container_running( int tcp_port )
{
    #ifdef notdef
    if ( settings.ips_per_cntr ) {
        plat_abort();
    }

    for ( int i = 0; i < MCD_MAX_NUM_CNTRS; i++ ) {
	if (pMcd_containers[i] == NULL) { continue; }
        if ( tcp_port == mcd_osd_container_tcp_port(pMcd_containers[i]) ) {
            return ( cntr_running == mcd_osd_container_state(pMcd_containers[i]) );
        }
    }
    #endif

    return -ENOENT;
}


    /******************************************************
     *
     *  End of callback functions for simple_replication.
     *
     ******************************************************/


#endif /* SDFAPI */

