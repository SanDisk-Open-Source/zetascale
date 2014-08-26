/*
 * File:   sdfclient/container.c
 * Author: Darryl Ouye
 *
 * Created on February 2, 2008, 1:07 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: container.c 12335 2010-03-18 20:56:46Z briano $
 */
#include <stdio.h>
#include <inttypes.h>

#include "platform/string.h"
#include "platform/stdlib.h"
#include "platform/logging.h"
#include "common/sdftypes.h"
#include "common/sdfstats.h"
#include "utils/hash.h"
#include "utils/properties.h"
#include "cmc.h"
#include "name_service.h"
#include "container_props.h"
#include "open_container_mgr.h"
#include "open_container_map.h"
#include "fth/fthSpinLock.h"
#include "init_sdf.h"
#include "object.h"
#include "container.h"
#include "private.h"
#include "shard_meta.h"
#include "shard_compute.h"

// from protocol/action/simple_replication.c
extern int SDFSimpleReplication; // non-zero if simple replication is on

// from protocol/protocol_utils.c
#ifndef SDFAPI
extern char *UTStartDebugger(char *s);
#endif /* SDFAPI */

#ifdef STATE_MACHINE_SUPPORT
// from protocol/action/simple_replication.c
extern SDF_status_t SDFUpdateMetaClusterGroupInfo( SDF_internal_ctxt_t *pai, SDF_container_meta_t *meta, int64_t cid );
extern int SDFGetClusterGroupType( SDF_internal_ctxt_t *pai, int64_t cid );
extern int SDFGetClusterGroupGroupId( SDF_internal_ctxt_t *pai, int64_t cid );
extern int msg_sdf_myrank();
#endif

#define STM_INT
#ifdef STM_INT
#ifdef SIMPLE_REPLICATION
extern int SDFNodeGroupGroupTypeFromConfig( int node_id );
extern int SDFNodeGroupGroupIdFromConfig( int node_id );
extern int SDFGetNumNodesInMyGroupFromConfig();
//void SDFRepDataStructAddContainer( SDF_internal_ctxt_t *, SDF_container_props_t, SDF_cguid_t cguid);
void SDFRepDataStructDeleteContainer( SDF_internal_ctxt_t *,SDF_cguid_t );
extern int msg_sdf_myrank();
#endif
#endif

// from protocol/action/action_new.c
extern SDF_status_t SDFGetCacheStat(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, int stat_name, uint64_t *pstat);
extern SDF_status_t SDFActionCreateContainer(SDF_internal_ctxt_t *pai, SDF_container_meta_t *pmeta);
extern SDF_status_t SDFActionOpenContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid);
extern SDF_status_t SDFActionStartContainer(SDF_internal_ctxt_t *pai, SDF_container_meta_t *pmeta);
extern SDF_status_t SDFActionStopContainer(SDF_internal_ctxt_t *pai, SDF_container_meta_t *pmeta);
extern SDF_status_t SDFActionDeleteContainer(SDF_internal_ctxt_t *pai, SDF_container_meta_t *pmeta);
extern SDF_status_t SDFActionChangeContainerWritebackMode(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, SDF_boolean_t enable_writeback);

/*
 * XXX: These should go away once the agent is handling all IPC and
 * flash access is happening in home_flash.c by way of home_thread or
 * the replication subsystem.
 */
#include "flash/flash.h"
#include "sdfmsg/sdf_msg.h"
#include "sdfmsg/sdf_msg_types.h"
#include "protocol/protocol_common.h"
#include "protocol/replication/meta.h"
#include "ssd/fifo/mcd_ipf.h"

#include "internal_blk_obj_api.h"

#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_NAMING
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_TRACE PLAT_LOG_LEVEL_TRACE
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_WARN PLAT_LOG_LEVEL_WARN
#define LOG_FATAL PLAT_LOG_LEVEL_FATAL

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_SHARD, LOG_CAT, "shard");


extern struct SDF_shared_state sdf_shared_state;
void *cmc_settings;

/*
 * Forward declarations
 */

//SDF_status_t delete_container_internal(SDF_internal_ctxt_t *pai, const char *path, SDF_boolean_t serialize);

SDF_container_meta_t *
build_meta(const char *path, SDF_container_props_t props, SDF_cguid_t cguid, SDF_shardid_t shard);

SDF_shardid_t
build_shard(struct SDF_shared_state *state, SDF_internal_ctxt_t *pai,
            const char *path, int num_objs,  uint32_t in_shard_count, 
            SDF_container_props_t props,  SDF_cguid_t cguid,
            enum build_shard_type build_shard_type, const char *cname);

SDF_status_t
create_put_meta(SDF_internal_ctxt_t *pai, const char *path, SDF_container_meta_t *meta, SDF_cguid_t cguid);

SDF_cguid_t
generate_cguid(SDF_internal_ctxt_t *pai, const char *path, uint32_t node, int64_t cntr_id);

uint64_t
generate_shard_ids(uint32_t node, SDF_cguid_t cguid, uint32_t count);

static struct shard *
get_shard_from_container(struct SDF_shared_state *state,
                         SDF_internal_ctxt_t *pai, local_SDF_CONTAINER lc,
			 uint32_t shard_index);
#if 0

static void
append_to_blob(char **blob, char *name, uint32_t len, int is_block);
#endif
struct plat_shmem_alloc_stats g_init_sm_stats, g_end_sm_stats;


// =====================================================================================================================
PLAT_SP_IMPL(_SDF_container_parent_sp, struct _SDF_container_parent);
PLAT_SP_IMPL(_SDF_container_sp, struct _SDF_container);
PLAT_SP_VAR_OPAQUE_IMPL(SDFCacheObj_sp, void);
PLAT_SP_VAR_OPAQUE_IMPL(DescrChangesPtr_sp, void);

SDF_CONTAINER
createContainer()
{
    _SDF_container_sp_t c = plat_shmem_alloc(_SDF_container_sp);
    if (_SDF_container_sp_is_null(c)) {
        plat_log_msg(20866, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_FATAL, "Not enough shared memory, plat_shmem_alloc() failed.");
        plat_assert_always(0);
    }
    return c;
}

void
freeContainer(SDF_CONTAINER c)
{
    plat_shmem_free(_SDF_container_sp, c);
}

local_SDF_CONTAINER
getLocalContainer(local_SDF_CONTAINER *lc, SDF_CONTAINER c)
{
    return (_SDF_container_sp_rwref(lc, c));
}

void
releaseLocalContainer(local_SDF_CONTAINER *lc)
{
    _SDF_container_sp_rwrelease(lc);
}

int
isContainerNull(SDF_CONTAINER c)
{
    return (_SDF_container_sp_is_null(c));
}

int
containerPtrEqual(SDF_CONTAINER c1, SDF_CONTAINER c2)
{
    return (_SDF_container_sp_eq(c1, c2));
}

SDF_CONTAINER_PARENT
createContainerParent()
{
    _SDF_container_parent_sp_t p = plat_shmem_alloc(_SDF_container_parent_sp);
    if (_SDF_container_parent_sp_is_null(p)) {
        plat_log_msg(20866, PLAT_LOG_CAT_SDF_SHARED, PLAT_LOG_LEVEL_FATAL, "Not enough shared memory, plat_shmem_alloc() failed.");
        plat_assert_always(0);
    }
    return p;
}

void
freeContainerParent(SDF_CONTAINER_PARENT p)
{
    plat_shmem_free(_SDF_container_parent_sp, p);
}

local_SDF_CONTAINER_PARENT
getLocalContainerParent(local_SDF_CONTAINER_PARENT *lp, SDF_CONTAINER_PARENT p)
{
    return (_SDF_container_parent_sp_rwref(lp, p));
}

void
releaseLocalContainerParent(local_SDF_CONTAINER_PARENT *lp)
{
    _SDF_container_parent_sp_rwrelease(lp);
}

int
isContainerParentNull(SDF_CONTAINER_PARENT p)
{
    return (_SDF_container_parent_sp_is_null(p));
}

SDF_CACHE_OBJ
createCacheObject(size_t size)
{
    return (plat_shmem_var_alloc(SDFCacheObj_sp, size));
}

void
freeCacheObject(SDF_CACHE_OBJ o, size_t size)
{
    plat_shmem_var_free(SDFCacheObj_sp, o, size);
}

local_SDF_CACHE_OBJ
getLocalCacheObject(local_SDF_CACHE_OBJ *lo, SDF_CACHE_OBJ o, size_t size)
{
    return (SDFCacheObj_sp_var_rwref(lo, o, size));
}

void
releaseLocalCacheObject(local_SDF_CACHE_OBJ *lo, size_t size)
{
    SDFCacheObj_sp_var_rwrelease(lo, size);
}

int
isCacheObjectNull(SDF_CACHE_OBJ o)
{
    return (SDFCacheObj_sp_is_null(o));
}

int
cacheObjectPtrEqual(SDF_CACHE_OBJ o1, SDF_CACHE_OBJ o2)
{
    return (SDFCacheObj_sp_eq(o1, o2));
}
// =====================================================================================================================

/*  xxxzzz yuck! */
typedef union {
    SDF_CONTAINER    server;
    SDFContainer     client;
} container_conversion_union_t;

SDF_CONTAINER
internal_clientToServerContainer(SDFContainer clientContainer)
{
    container_conversion_union_t  mix;

    mix = (container_conversion_union_t) clientContainer;
    return (mix.server);
}

SDFContainer
internal_serverToClientContainer(SDF_CONTAINER serverContainer)
{
    container_conversion_union_t  mix;

    mix = (container_conversion_union_t) serverContainer;
    return (mix.client);
}
// =====================================================================================================================
size_t
plat_strlen(const unsigned char *p)
{
    size_t i = 0;
    for(; i < INT32_MAX && '\0' != *p; i++, p++);
    return i;
}

/**
 * @brief Like strcat in <string.h>, only this works on unsigned character strings.
 *
 * The plat_safe_strcat() function appends the src string to the dest string, overwriting the null byte (’\0’) at the
 * end of dest,and then adds a terminating null byte. The strings may not overlap, and the dest string must have enough
 * space for the result. If destBufLen is non zero, checks are made if the plat_safe_strcat will succeed incorporating
 * the size of both the strings.
 *
 * @param dest <IN>
 * @param src <IN>
 * @param n <IN> copy n characters from src, maybe zero (in which case strlen will be used on src)
 * @param destBufLen <IN> if 0 no length checks are made, otherwise checks are made according to the length of the
 * destination buffer (preventing overwrites and memory corruption)
 * @return return a pointer to the resulting string dest
 */
unsigned char *
plat_safe_strcat(unsigned char *dest, const unsigned char *src, size_t n, int destBufLen)
{
    int i;
    size_t dlen = plat_strlen(dest);

    if (!n) {
        n = plat_strlen(src);
    }
    if (destBufLen) {
        plat_assert(destBufLen > (dlen+n)); // needed atleast (dlen+n+1), prevents memory corruption
    }

    for (i = 0 ; i < n && src[i] != '\0' ; i++) {
        dest[dlen + i] = src[i];
    }
    dest[dlen + i] = '\0';

    return dest;
}

void
print_sm_stats(struct plat_shmem_alloc_stats init, struct plat_shmem_alloc_stats end)
{
    plat_log_msg(21524, LOG_CAT, LOG_TRACE, "Shared Memory Stats:\n Initial: allocated_bytes=%"PRIu64", allocated_count="
                 "%"PRIu64", used_bytes=%"PRIu64, init.allocated_bytes, init.allocated_count, init.used_bytes);
    plat_log_msg(21525, LOG_CAT, LOG_TRACE, "\n Ending : allocated_bytes=%"PRIu64", allocated_count=%"PRIu64
                 ", used_bytes=%"PRIu64, end.allocated_bytes, end.allocated_count, end.used_bytes);
    plat_log_msg(21526, LOG_CAT, LOG_TRACE, "\n Diff   : allocated_bytes=%"PRIu64", allocated_count=%"PRIu64
                 ", used_bytes=%"PRIu64"\n", end.allocated_bytes - init.allocated_bytes,
                 end.allocated_count - init.allocated_count, end.used_bytes - init.used_bytes);
}

#ifndef SDFAPI
/*
 * Create a container:
 *
 * - Find out what nodes are available for a new container from the
 *   load balancer.
 * - Allocate a set of shards for each container replica (results in an SMT).
 * - Create a container metadata object from the name, properties and SMT.
 * - Register the container metadata object with the name service.
 */
#define CONTAINER_PENDING

SDF_status_t
SDFCreateContainer(SDF_internal_ctxt_t *pai, const char *path, SDF_container_props_t properties, int64_t cntr_id)
{
    struct SDF_shared_state *state = &sdf_shared_state;
    SDF_status_t status = SDF_FAILURE;
    SDF_shardid_t shardid = SDF_SHARDID_INVALID;
    SDF_container_meta_t *meta = NULL;
    SDF_CONTAINER_PARENT parent = containerParentNull;
    local_SDF_CONTAINER_PARENT lparent = NULL;
    SDF_cguid_t cguid;
    SDF_boolean_t isCMC;
    uint32_t in_shard_count=0;
    SDF_vnode_t home_node;
    uint32_t num_objs = 0;
    const char *writeback_enabled_string;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", path);

    if ((!properties.container_type.caching_container) && (!properties.cache.writethru)) {
	plat_log_msg(30572, LOG_CAT, LOG_ERR,
		     "Writeback caching can only be enabled for eviction mode containers");
	return(SDF_FAILURE_INVALID_CONTAINER_TYPE);
    }
    if (!properties.cache.writethru) {
	if (!properties.container_type.caching_container) {
	    plat_log_msg(30572, LOG_CAT, LOG_ERR,
			 "Writeback caching can only be enabled for eviction mode containers");
	    return(SDF_FAILURE_INVALID_CONTAINER_TYPE);
	} else {
	    writeback_enabled_string = getProperty_String("SDF_WRITEBACK_CACHE_SUPPORT", "On");
	    if (strcmp(writeback_enabled_string, "On") != 0) {
		plat_log_msg(30575, LOG_CAT, LOG_ERR,
			     "Cannot enable writeback caching for container '%s' because writeback caching is disabled.",
			     path);
		properties.cache.writethru = SDF_TRUE;
	    }
	}
    }

    SDFStartSerializeContainerOp(pai);

    if (strcmp(path, CMC_PATH) == 0) {
        cguid = CMC_CGUID;
        isCMC = SDF_TRUE;
	home_node = CMC_HOME;
    } else {
        cguid = generate_cguid(pai, path, init_get_my_node_id(), cntr_id); // Generate the cguid
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
            if (properties.container_id.container_id ==
                   settings->vips[i].container_id) {
                   settings->vips[i].cguid = cguid;
            }
        }
    }

    num_objs = properties.container_id.num_objs;

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
            properties.replication.num_replicas = state->config.nnodes;
            properties.replication.num_meta_replicas = 0;
            properties.replication.type = SDF_REPLICATION_SIMPLE;
        } else {
            properties.replication.num_replicas = state->config.always_replicate;
            properties.replication.num_meta_replicas = 1;
            properties.replication.type = state->config.replication_type;
        }

        properties.replication.enabled = 1;
        properties.replication.synchronous = 1;
        if( properties.replication.type == SDF_REPLICATION_V1_2_WAY ) {
            properties.replication.num_replicas = 2;
        }
    }

    /* 
       How do we set shard_count :
     1) check if shard_count in the incoming Container properties is non-zero 
     2) else use the shard_count from the properties file (by incredibly
        complicated maze of initialization ending up in state->config)
     3) else  use the hard-coded SDF_SHARD_DEFAULT_SHARD_COUNT macro
    */
    
    in_shard_count = properties.shard.num_shards?properties.shard.num_shards:
        (state->config.shard_count?
         state->config.shard_count:SDF_SHARD_DEFAULT_SHARD_COUNT);
    
     /* XXX: If we reached here without having set the shard_count in
        container properties, set the property here. In the future we 
        might want to assert on this condition.
     */
    if (properties.shard.num_shards == 0) {
	properties.shard.num_shards = in_shard_count;
    }

#ifdef MULTIPLE_FLASH_DEV_ENABLED
    plat_log_msg(21527, LOG_CAT, LOG_DBG, "Container: %s - Multi Devs: %d",
		 path, state->config.flash_dev_count);
#else
    plat_log_msg(21528, LOG_CAT, LOG_DBG, "Container: %s - Single Dev",
		 path);
#endif
    plat_log_msg(21529, LOG_CAT, LOG_DBG, "Container: %s - Num Shards: %d",
		 path, properties.shard.num_shards);

    plat_log_msg(21530, LOG_CAT, LOG_DBG, "Container: %s - Num Objs: %d",
		 path, state->config.num_objs);

    plat_log_msg(21531, LOG_CAT, LOG_DBG, "Container: %s - DEBUG_MULTI_SHARD_INDEX: %d",
		 path, getProperty_Int("DEBUG_MULTISHARD_INDEX", -1));

    if (ISEMPTY(path)) {
        status = SDF_INVALID_PARAMETER;
    } else if (doesContainerExistInBackend(pai, path)) {
        #ifdef CONTAINER_PENDING
        // Unset parent delete flag if with deleted flag
    	if (!isContainerParentNull(parent = isParentContainerOpened(path))) {
	        local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, parent);

             if (lparent->delete_pending == SDF_TRUE) {
    		    if (!isCMC && (status = name_service_lock_meta(pai, path)) != SDF_SUCCESS) {
       			    plat_log_msg(21532, LOG_CAT, LOG_ERR, "failed to lock %s", path);
                }
                 
                lparent->delete_pending = SDF_FALSE;
                
                if (!isCMC && (status = name_service_unlock_meta(pai, path)) != SDF_SUCCESS) {
       			    plat_log_msg(21533, LOG_CAT, LOG_ERR, "failed to unlock %s", path);
                }

             }

            releaseLocalContainerParent(&lparent); // TODO C++ please!
        }
        #endif
       status = SDF_CONTAINER_EXISTS;
    } else {
        if ((shardid = build_shard(state, pai, path, num_objs,
				   in_shard_count, properties, cguid,
				   isCMC ? BUILD_SHARD_CMC : BUILD_SHARD_OTHER, path)) <= SDF_SHARDID_LIMIT) {
            if ((meta = build_meta(path, properties, cguid, shardid)) != NULL) {
#ifdef STATE_MACHINE_SUPPORT
                SDFUpdateMetaClusterGroupInfo(pai,meta,properties.container_id.container_id);
#endif
                if (create_put_meta(pai, path, meta, cguid) == SDF_SUCCESS) {

		    if (!isCMC && (status = name_service_lock_meta(pai, path)) != SDF_SUCCESS) {
			plat_log_msg(21532, LOG_CAT, LOG_ERR, "failed to lock %s", path);
		    } else if (!isContainerParentNull(parent = createParentContainer(pai, path, meta))) {
			lparent = getLocalContainerParent(&lparent, parent); // TODO C++ please!
			lparent->container_type = properties.container_type.type;
			if (lparent->container_type == SDF_BLOCK_CONTAINER) {
			    lparent->blockSize = properties.specific.block_props.blockSize;
			}
			releaseLocalContainerParent(&lparent); // TODO C++ please!

			status = SDF_SUCCESS;

			if (!isCMC && (status = name_service_unlock_meta(pai, path)) != SDF_SUCCESS) {
			    plat_log_msg(21533, LOG_CAT, LOG_ERR, "failed to unlock %s", path);
			}
		    } else {
			plat_log_msg(21535, LOG_CAT, LOG_ERR, "cname=%s, build_shard failed", path);
		    }
		} else {
		    plat_log_msg(21536, LOG_CAT, LOG_ERR, "cname=%s, createParentContainer() failed", path);
		}

		container_meta_destroy(meta);

	    } else {
		plat_log_msg(21537, LOG_CAT, LOG_ERR, "cname=%s, build_meta failed", path);
	    }
	} else {
	    plat_log_msg(21535, LOG_CAT, LOG_ERR, "cname=%s, build_shard failed", path);
	}
    }

    plat_log_msg(21511, LOG_CAT, LOG_TRACE, "%s - %s", path, SDF_Status_Strings[status]);

    if (status != SDF_SUCCESS && status != SDF_CONTAINER_EXISTS) {
        plat_log_msg(21538, LOG_CAT, LOG_ERR, "cname=%s, function returned status = %u", path, status);
        name_service_remove_meta(pai, path);
#if 0
        /*
         * XXX We're leaking the rest of the shard anyways, and this could
         * cause dangling pointer problems to manifest from the coalesce, etc.
         * internal flahs threads so skip it.
         */
        shardDelete(shard);
	xxxzzz continue from here
#endif
    }
#ifdef SIMPLE_REPLICATION
/*
    else if (status == SDF_SUCCESS) {
        SDFRepDataStructAddContainer(pai,properties,cguid);    
    } 
*/
#endif

    SDFEndSerializeContainerOp(pai);
    return (status);
}
#endif /* SDFAPI */

#if 0
SDF_status_t
SDFDeleteContainerByCguid(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid) 
{
    SDF_status_t         status = SDF_SUCCESS;
    SDF_container_meta_t meta;
    char                 cname[MAX_CNAME_SIZE + 1];

    SDFStartSerializeContainerOp(pai);

    if ((status = name_service_get_meta(pai, cguid, &meta)) == SDF_SUCCESS) {
	/* I must copy cname because SDFDeleteContainer is going to nuke
	 * the meta structure, so meta.cname may get corrupted.
	 */
        (void) strcpy(cname, meta.cname);
	status = delete_container_internal(pai, cname, SDF_FALSE /* serialize */);
    }

#ifdef SIMPLE_REPLICATION
    if( status == SDF_SUCCESS ) {
        SDFRepDataStructDeleteContainer(pai,cguid);
    }
#endif
  
    SDFEndSerializeContainerOp(pai);
    return(status);
}
#endif

#ifndef SDFAPI
SDF_status_t
SDFStartContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid) 
{
    SDF_status_t             status = SDF_SUCCESS;
    SDF_container_meta_t     meta;
    struct shard            *shard = NULL;
    struct SDF_shared_state *state = &sdf_shared_state;
    flashDev_t              *flash_dev;

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
#endif /* SDFAPI */

  /*  Note: a container must be stopped (and all accesses to it must be quiesced)
   *  before this function is called!
   */
SDF_status_t
SDFChangeContainerWritebackMode(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, SDF_boolean_t enable_writeback) 
{
    SDF_status_t             status = SDF_SUCCESS;
    SDF_container_meta_t     meta;

    SDFStartSerializeContainerOp(pai);
    if ((status = name_service_get_meta(pai, cguid, &meta)) == SDF_SUCCESS) {
        if (enable_writeback && (!meta.properties.container_type.caching_container)) {
	    plat_log_msg(30572, LOG_CAT, LOG_ERR,
			 "Writeback caching can only be enabled for eviction mode containers");
	    status = SDF_FAILURE_INVALID_CONTAINER_TYPE;
	} else {
	    meta.properties.cache.writethru = (enable_writeback ? SDF_FALSE : SDF_TRUE);
	    if ((status = name_service_put_meta(pai, cguid, &meta)) == SDF_SUCCESS) {

		/* Clean up additional action node state for the container.
		 */
		status = SDFActionChangeContainerWritebackMode(pai, cguid, enable_writeback);
	    } else {
		plat_log_msg(21539, LOG_CAT, LOG_ERR,
			     "name_service_put_meta failed for cguid %"PRIu64"", cguid);
	    }
	}
    }

    SDFEndSerializeContainerOp(pai);
    return(status);
}
#if 0
#ifndef SDFAPI
SDF_status_t
SDFGetContainerProps(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, SDF_container_props_t *pprops) 
{
    SDF_status_t             status = SDF_SUCCESS;
    SDF_container_meta_t     meta;

    plat_log_msg(150007, LOG_CAT, LOG_DBG, "SDFGetContainerProps: %p - %lu - %p", pai, cguid, pprops);
    SDFStartSerializeContainerOp(pai);
    if ((status = name_service_get_meta(pai, cguid, &meta)) == SDF_SUCCESS) {
        *pprops = meta.properties;
    }
    SDFEndSerializeContainerOp(pai);
    plat_log_msg(150011, LOG_CAT, LOG_DBG, "SDFGetContainerProps: status=%s", (status==SDF_SUCCESS) ? "SUCCESS":"FAIL");

    return(status);
}

SDF_status_t
SDFSetContainerProps(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid, SDF_container_props_t *pprops)
{
    SDF_status_t             status = SDF_SUCCESS;
    SDF_container_meta_t     meta;

    SDFStartSerializeContainerOp(pai);
    if ((status = name_service_get_meta(pai, cguid, &meta)) == SDF_SUCCESS) {
        meta.properties = *pprops;
    	status = name_service_put_meta(pai, cguid, &meta);
    }
    SDFEndSerializeContainerOp(pai);

    return(status);
}

SDF_status_t
SDFStopContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t cguid) 
{
    SDF_status_t             status = SDF_SUCCESS;
    SDF_container_meta_t     meta;
    struct shard            *shard = NULL;
    struct SDF_shared_state *state = &sdf_shared_state;
    flashDev_t              *flash_dev;

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

SDF_status_t
SDFDeleteContainer(SDF_internal_ctxt_t *pai, const char *path)
{
    return(delete_container_internal(pai, path, SDF_TRUE /* serialize */));
}
#endif /* SDFAPI */
#endif
SDF_status_t delete_container_internal_low(
	SDF_internal_ctxt_t *pai, 
	const char *path, 
	SDF_boolean_t serialize, 
	SDF_boolean_t	delete_shards,
	int *deleted) 
{

    SDF_status_t         status = SDF_SUCCESS;
#ifndef SDFAPIONLY
    SDF_status_t         tmp_status = SDF_SUCCESS;
#endif
    SDF_status_t         lock_status = SDF_SUCCESS;
    SDF_CONTAINER_PARENT p;
    int                  log_level = LOG_TRACE;
    int                  ok_to_delete = 1;
    SDF_container_meta_t meta;

    plat_log_msg(160096, LOG_CAT, LOG_TRACE, "%s serialize:%d", path,serialize);

    if (serialize) {
		SDFStartSerializeContainerOp(pai);
    }

    if (ISEMPTY(path)) {
		status = SDF_INVALID_PARAMETER;
    } else if (!doesContainerExistInBackend(pai, path)) {
		status = SDF_CONTAINER_UNKNOWN;
    }

    if (status == SDF_SUCCESS) {
        if (!isContainerParentNull(p = isParentContainerOpened(path))) {
            // Container is open - just set the tombstone
            local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, p);
	    
	    	/*  Get a copy of the metadata here for use later in call to
	     	*  SDFActionDeleteContainer, before things start to disappear.
	     	*/
	    	if ((status = name_service_get_meta(pai, lparent->cguid, &meta)) != SDF_SUCCESS) {
				ok_to_delete = 0;
	    	}
	
			if (lparent->num_open_descriptors > 0) {
				lparent->delete_pending = SDF_TRUE;
				ok_to_delete = 0;
			}
            releaseLocalContainerParent(&lparent);
        }
    }

    if (ok_to_delete) {

        if (!isContainerParentNull(p = containerMap_removeParent(path))) {
            freeContainerParent(p);
        }

        if ((status = name_service_lock_meta(pai, path)) == SDF_SUCCESS) {

#ifndef SDFAPIONLY //Moved to SDFCloseContainer
	    // Invalidate all of the container's cached objects
	    if ((status = name_service_inval_object_container(pai, path)) != SDF_SUCCESS) {
		plat_log_msg(21540, LOG_CAT, LOG_ERR,
			     "%s - failed to flush and invalidate container", path);
		log_level = LOG_ERR;
	    } else {
		plat_log_msg(21541, LOG_CAT, LOG_TRACE,
			     "%s - flush and invalidate container succeed", path);
	    }

            // Clean up the action thread container state.

            if (status == SDF_SUCCESS) {
	        /*  If SDFActionDeleteContainer fails, still proceed with
		 *  other deletion activities!
		 */

		if ((tmp_status = name_service_get_meta_from_cname(pai, path, &meta)) == SDF_SUCCESS) {
		    tmp_status = SDFActionDeleteContainer(pai, &meta);
		    if (tmp_status != SDF_SUCCESS) {
			// xxxzzz container will be left in a weird state!
			plat_log_msg(21542, LOG_CAT, LOG_ERR,
				"%s - failed to delete action thread container state", path);
			log_level = LOG_ERR;
		    } else {
			plat_log_msg(21543, LOG_CAT, LOG_TRACE,
				"%s - action thread delete container state succeeded", path);
		    }
		}
	    }
#endif
            // Remove the container shards
            if (delete_shards && 
				status == SDF_SUCCESS && 
				(status = name_service_delete_shards(pai, path)) != SDF_SUCCESS) {
                plat_log_msg(21544, LOG_CAT, LOG_ERR,
                        "%s - failed to delete container shards", path);
                log_level = LOG_ERR;
            } else {
                plat_log_msg(21545, LOG_CAT, LOG_TRACE,
                        "%s - delete container shards succeeded", path);
	    }

            // Remove the container metadata
            if (status == SDF_SUCCESS &&
		(status = name_service_remove_meta(pai, path)) != SDF_SUCCESS) {
                plat_log_msg(21546, LOG_CAT, LOG_ERR,
                        "%s - failed to remove metadata", path);
                log_level = LOG_ERR;
		plat_assert(0);
            } else {
                plat_log_msg(21547, LOG_CAT, LOG_TRACE,
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
		plat_assert(0);
            } else {
                plat_log_msg(21549, LOG_CAT, LOG_TRACE,
                        "%s - unlock metadata succeeded", path);
	    }
	}
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", path, SDF_Status_Strings[status]);

    if (serialize) {
	SDFEndSerializeContainerOp(pai);
    }

	if(deleted) *deleted = ok_to_delete;

    return (status);
}
#if 0
SDF_status_t delete_container_internal(SDF_internal_ctxt_t *pai, const char *path, SDF_boolean_t serialize) 
{
	return delete_container_internal_low(pai, path, serialize, SDF_TRUE, NULL);	
}
#endif


#ifndef SDFAPI
SDF_status_t
SDFOpenContainer(SDF_internal_ctxt_t *pai, const char *path, SDF_container_mode_t mode,
		 SDF_CONTAINER *container) 
{

    SDF_status_t status = SDF_SUCCESS;
    local_SDF_CONTAINER lc = NULL;
    SDF_CONTAINER_PARENT parent;
    local_SDF_CONTAINER_PARENT lparent = NULL;
    int log_level = LOG_ERR;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", path);

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
	    plat_log_msg(21552, LOG_CAT,LOG_ERR, "Delete pending for %s", path);

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
	    log_level = LOG_TRACE;

            if (cmc_settings != NULL) {
                 struct settings *settings = cmc_settings;
                 int  i;

                 for (i = 0;
                      i < sizeof(settings->vips) / sizeof(settings->vips[0]);
                      i++) 
		 {
                     if (lc->container_id == settings->vips[i].container_id) {
                         settings->vips[i].cguid = lc->cguid;
                            
			 plat_log_msg(21553, LOG_CAT, LOG_TRACE, "set cid %d (%d) to cguid %d\n",
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
	    plat_log_msg(21555, LOG_CAT, LOG_TRACE, "Opened %s", path);
	} else {
	    status = SDF_CONTAINER_UNKNOWN;
	    plat_log_msg(21556, LOG_CAT,LOG_ERR, "Failed to find %s", path);
        }
    }

    plat_log_msg(21511, LOG_CAT, log_level, "%s - %s", path, SDF_Status_Strings[status]);

    SDFEndSerializeContainerOp(pai);
    return (status);
}

SDF_status_t
SDFCloseContainer(SDF_internal_ctxt_t *pai, SDF_CONTAINER container) 
{
    SDF_status_t status = SDF_FAILURE;
    SDF_cguid_t  cguid;
    unsigned     n_descriptors;

    SDF_status_t lock_status = SDF_SUCCESS;
    int log_level = LOG_ERR;
    int ok_to_delete = 0;

    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE, " ");

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
	        log_level = LOG_TRACE;
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
		            plat_log_msg(21541, LOG_CAT, LOG_TRACE,
				     "%s - flush and invalidate container succeed", path);
		        }

		        // Remove the container shards
		        if (status == SDF_SUCCESS && 
		            (status = name_service_delete_shards(pai, path)) != SDF_SUCCESS) {
		            plat_log_msg(21544, LOG_CAT, LOG_ERR,
				         "%s - failed to delete container shards", path);
		            log_level = LOG_ERR;
		        } else {
		            plat_log_msg(21545, LOG_CAT, LOG_TRACE,
				         "%s - delete container shards succeeded", path);
		        }

		        // Remove the container metadata
		        if (status == SDF_SUCCESS &&
		            (status = name_service_remove_meta(pai, path)) != SDF_SUCCESS) {
		            plat_log_msg(21546, LOG_CAT, LOG_ERR,
				                 "%s - failed to remove metadata", path);
		            log_level = LOG_ERR;
		        } else {
		            plat_log_msg(21547, LOG_CAT, LOG_TRACE,
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
		            plat_log_msg(21549, LOG_CAT, LOG_TRACE,
				                 "%s - unlock metadata succeeded", path);
		        }
	        }
	    }
    }

    plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);

    SDFEndSerializeContainerOp(pai);
    return (status);
}
#endif /* SDFAPI */

SDF_status_t
SDFCopyContainer(SDF_internal_ctxt_t *pai, const char *srcPath, const char *destPath) {
    SDF_status_t status = SDF_FAILURE;
    status = SDF_FAILURE;
    return (status);
}

SDF_status_t
SDFMoveContainer(SDF_internal_ctxt_t *pai, const char *srcPath, const char *destPath) {
    SDF_status_t status = SDF_FAILURE;
    status = SDF_FAILURE;
    return (status);
}

SDF_status_t
SDFRenameContainer(SDF_internal_ctxt_t *pai, const char *path, const char *newName) {
    SDF_status_t status = SDF_FAILURE;
    status = SDF_FAILURE;
    return (status);
}

SDF_status_t
SDFListContainers(SDF_internal_ctxt_t *pai, const char *dirPath) {
    SDF_status_t status = SDF_FAILURE;
    status = SDF_FAILURE;
    return (status);
}

SDF_status_t
SDFExportContainer(SDF_internal_ctxt_t *pai, const char *sdfPath, FILE *destFile) {
    SDF_status_t status = SDF_FAILURE;
    status = SDF_FAILURE;
    return (status);
}

SDF_status_t
SDFImportContainer(SDF_internal_ctxt_t *pai, const char *sdfPath, FILE *srcFile) {
    SDF_status_t status = SDF_FAILURE;
    status = SDF_FAILURE;
    return (status);
}
#if 0
SDF_status_t
SDFEnumerateContainer(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, char_sp_t *blob, uint32_t *len) 
{

    struct SDF_shared_state *state = &sdf_shared_state;
    SDF_status_t status = SDF_SUCCESS;
    int count = 0;
    char *key = NULL;
    int hashIndex = 0;
    local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, lc->parent);
    struct shard *shard = NULL;
    char *myblob = NULL;
    char *buf = plat_alloc(4);
    char *str = NULL;
    objDesc_t *obj = NULL;
    objMetaData_t *metaData = NULL;
    int is_block = 0;
    int log_level = LOG_TRACE;

    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE, " ");

    SDFStartSerializeContainerOp(pai);

    is_block = lparent->container_type == SDF_BLOCK_CONTAINER;

    /*
     * Fixme: This only works when the flash is local. We need to go through
     * the agent then home node code.
     */
    for (int i = 0; i < lc->num_shards; i++) {
	shard = get_shard_from_container(state, pai, lc, i);

	while ((obj = flashEnumerate(shard, obj, &hashIndex, (char **)&key)) != NULL) {
	    metaData = getMetaData(obj);
	    ++count;
	    plat_log_msg(21557, LOG_CAT, LOG_TRACE, "Enumerate: %s - %u", key, metaData->dataLen);
	    append_to_blob(&myblob, key, metaData->dataLen, is_block);
	    plat_free(key);
	    key = NULL;
	}
    }

    if (myblob != NULL) {
	sprintf(buf, "%u\t", count);
	buf = plat_realloc(buf, strlen(buf) + strlen(myblob) + 100);
	buf = strcat(buf, myblob);
	*len = strlen(buf);
#ifndef SDFCC_SHMEM_FAKE
	*blob = plat_shmem_var_alloc(char_sp, sizeof (char) * *len);
#else
	*blob = plat_alloc(*len * sizeof (char));
#endif
	char_sp_var_rwref(&str, *blob, *len);
	memcpy(str, buf, *len);
	plat_free(myblob);
	char_sp_var_rwrelease(&str, *len);
    }
    plat_free(buf);

    plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);

    SDFEndSerializeContainerOp(pai);
    return (status);
}
#endif

SDF_status_t
SDFDoesContainerExist(SDF_internal_ctxt_t *pai, const char *path) {
    SDF_status_t status = SDF_FAILURE;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", path);

    SDFStartSerializeContainerOp(pai);

    if ((name_service_meta_exists(pai, path)) == SDF_SUCCESS) {
	status = SDF_CONTAINER_EXISTS;
    } else {
	status = SDF_CONTAINER_UNKNOWN;
    }

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", SDF_Status_Strings[status]);

    SDFEndSerializeContainerOp(pai);
    return (status);
}
#if 0
SDF_status_t SDFContainerStat(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, int key, uint64_t *stat) 
{
    SDF_status_t   status;

    SDFStartSerializeContainerOp(pai);
    status = SDFContainerStatInternal(pai, container, key, stat);
    SDFEndSerializeContainerOp(pai);
    return(status);
}
#endif

SDF_status_t SDFContainerStatInternal(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, int key, uint64_t *stat) 
{
    SDF_status_t status = SDF_FAILURE;
    struct SDF_shared_state *state = &sdf_shared_state;
    local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
    struct shard *shard = NULL;
#ifndef SDFAPI
    int log_level = LOG_ERR;
#endif /* SDFAPI */

    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE, " ");

    if (isContainerNull(container) || stat == NULL) {
        status = SDF_INVALID_PARAMETER;
    } else {
	*stat = 0;

	if ((key >= FIRST_STAT_TYPE_PER_CONTAINER) &&
	    (key < FIRST_STAT_TYPE_PER_DUMMY))
	{
	    status = SDFGetCacheStat(pai, container, key, stat);

	} else {

	    // TEMP DEBUGGING
	    int debug_shard_index = -1;
	    if ((debug_shard_index = getProperty_Int("DEBUG_MULTISHARD_INDEX", -1)) >= 0) {
		shard = get_shard_from_container(state, pai, lc, debug_shard_index);
		if (shard != NULL) {
		    // Assumes container and flash stats keys map 1:1
		    *stat = flashStats(shard, key);
		}
	    } else {
		for (int i = 0; i < lc->num_shards; i++) {
		    // FIXME...we need another interface for non-shard counters...
		    if (i == 0 || key >= FIRST_STAT_TYPE_PER_DEV) {
			shard = get_shard_from_container(state, pai, lc, i);
			if (shard != NULL) {
			    // Assumes container and flash stats keys map 1:1
			    *stat += flashStats(shard, key);
			}
		    }
		}
	    }
	    status = SDF_SUCCESS;
	}

#ifndef SDFAPI
	log_level = LOG_TRACE;
#endif /* SDFAPI */
    }
    #ifndef SDFAPIONLY
    plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);
    #endif

    return (status);
}

#if 0
SDF_status_t
SDFContainerShards(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, uint32_t max_shards, struct shard * shards[], uint32_t * shard_count) {

    struct SDF_shared_state *state = &sdf_shared_state;
    SDF_status_t status = SDF_SUCCESS;
    local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
    int log_level = LOG_TRACE;

    SDFStartSerializeContainerOp(pai);

    *shard_count = 0;

    /*
     * Fixme: This only works when the flash is local. We need to go through
     * the agent then home node code.
     */
    for (int i = 0; i < lc->num_shards && i < max_shards; i++) {
	shards[i] = get_shard_from_container(state, pai, lc, i);
        *shard_count += 1;
    }

    plat_log_msg(20819, LOG_CAT, log_level, "%s", SDF_Status_Strings[status]);

    SDFEndSerializeContainerOp(pai);
    return (status);
}
#endif

// =====================================================================
// Utilities
// =====================================================================
SDF_container_meta_t *
build_meta(const char *path, SDF_container_props_t props, SDF_cguid_t cguid, SDF_shardid_t shard) {

    SDF_container_meta_t *cm = NULL;

    if ((cm = container_meta_create(path, props, cguid, shard)) == NULL) {
        plat_log_msg(21558, LOG_CAT, LOG_ERR, "FAILURE: SDFCreateContainer - build meta");
    }
    return (cm);
}

SDF_status_t
create_put_meta(SDF_internal_ctxt_t *pai, const char *path, SDF_container_meta_t *meta, SDF_cguid_t cguid) {

    SDF_status_t status = SDF_FAILURE;

    plat_log_msg(21559, LOG_CAT, LOG_TRACE, "create_put_meta: %s - %lu", path, cguid);
    if (strcmp(path, CMC_PATH) != 0) {

        if (name_service_create_meta(pai, path, cguid, meta) != SDF_SUCCESS) {
            plat_log_msg(21560, LOG_CAT, LOG_ERR, "FAILURE: create_put_meta - create %s", path);
        } else {
            status = SDF_SUCCESS;
            plat_log_msg(21561, LOG_CAT, LOG_TRACE, "SUCCESS: create_put_meta - put %s", path);
        }
    } else {
        // For the CMC, simply initialize the CMC structure.
        init_cmc(meta);

        status = SDF_SUCCESS;
    }

    plat_log_msg(21562, LOG_CAT, LOG_TRACE, "create_put_meta: %d\n", status);

    return (status);
}

/*
 * Print the parent container structure.
 * @param parent
 */
void
_sdf_print_parent_container_structure(SDF_CONTAINER_PARENT parent)
{
    plat_assert(!isContainerParentNull(parent));
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, parent);

    plat_log_msg(21563, LOG_CAT, LOG_TRACE, "Container parent descriptor [blockSize=%u, container_type=%u, dir="
                 "%s, cguid=%lu, name=%s, num_open_descr=%u, ptr_open_descr=TODO]\n",
                 lparent->blockSize, lparent->container_type, lparent->dir,
                 lparent->cguid, lparent->name, lparent->num_open_descriptors);
    // ,(int)lparent->open_containers);

    releaseLocalContainerParent(&lparent);
}

/*
 * Print the container descriptor, and the parent pointed to.
 * @param container container descriptor
 */
void
_sdf_print_container_descriptor(SDF_CONTAINER container)
{

    plat_assert(!isContainerNull(container));
    local_SDF_CONTAINER lc = getLocalContainer(&lc, container);

    plat_log_msg(21564, LOG_CAT, LOG_TRACE, "Container descriptor [mode=%u, ptr_next=TODO, ptr_parent=TODO, "
                 "ptr_void=TODO]", lc->mode);
    // , (int)lc->next, (int)lc->parent, (int)lc->ptr);

    _sdf_print_parent_container_structure(lc->parent);

    releaseLocalContainer(&lc);
}


int
validateContainerType(SDF_internal_ctxt_t *pai, SDF_CONTAINER container, SDF_container_type_t type) {
    return (0);
}
#if 0
/*
 * Blob format:
 *   member count
 *   "|" separator
 *   object name
 *   "|" separator
 *   object len
 *   "|" separator
 *   ...
 */
static void
append_to_blob(char **blob, char *name, uint32_t len, int is_block) {
    char *separator = "\t";
    char buf[32];
    char idc[8];
    uint64_t id;

    sprintf(buf, "%u", len);

    // Blob is: count|name|len|name|len...
    if (*blob == NULL) {
	if (is_block) {
	    memcpy((char *)&id, name, 8);
	    sprintf(idc, "%lu", id);
	    *blob = plat_strdup(idc);
	    *blob = plat_realloc(*blob, strlen(*blob) + 1 + strlen(buf) + 1);
	} else {
	    *blob = plat_strdup(name);
	    *blob = plat_realloc(*blob, strlen(*blob) + 1 + strlen(buf) + 1);
	}
    } else {
	if (is_block) {
	    memcpy((char *)&id, name, 8);
	    sprintf(idc, "%lu", id);
	    *blob = plat_realloc(*blob, strlen(*blob) + 1 + strlen(idc) + 1 + strlen(buf) + 1);
	    strcat(*blob, separator);
	    strcat(*blob, idc);
	} else {
	    *blob = plat_realloc(*blob, strlen(*blob) + 1 + strlen(name) + 1 + strlen(buf) + 1);
	    strcat(*blob, separator);
	    strcat(*blob, name);
	}
    }
    strcat(*blob, separator);
    strcat(*blob, buf);
}
#endif
static struct shard *
get_shard_from_container(struct SDF_shared_state *state,
                         SDF_internal_ctxt_t *pai, local_SDF_CONTAINER lc, 
			 uint32_t shard_index) {

    SDF_shardid_t shard_id;
    struct shard *shard = NULL;
    SDF_container_meta_t meta;
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, lc->parent);
    flashDev_t * flash_dev;

    if (!isContainerParentNull(lc->parent) && !ISEMPTY(lparent->name)) {
        if (name_service_get_meta(pai, lparent->cguid, &meta) == SDF_SUCCESS) {
            shard_id = meta.shard + shard_index;
            plat_assert(shard_id <= SDF_SHARDID_LIMIT);
#ifdef MULTIPLE_FLASH_DEV_ENABLED
            flash_dev = get_flashdev_from_shardid(state->config.flash_dev,
                                                  shard_id, state->config.flash_dev_count);
#else
            flash_dev = state->config.flash_dev;
#endif                           
            shard = shardFind(flash_dev, shard_id);
        }
    }

    releaseLocalContainerParent(&lparent);

    return (shard);
}


struct shard * 
container_to_shard( SDF_internal_ctxt_t * pai, local_SDF_CONTAINER lc )
{

    SDF_shardid_t               shard_id;
    struct shard              * shard = NULL;
    SDF_container_meta_t        meta;
    flashDev_t                * flash_dev;
    local_SDF_CONTAINER_PARENT  lparent = 
        getLocalContainerParent(&lparent, lc->parent);
    struct SDF_shared_state   * state = &sdf_shared_state;
    uint32_t                    shard_index = 0;

    if (!isContainerParentNull(lc->parent) && !ISEMPTY(lparent->name)) {
        if (name_service_get_meta(pai, lparent->cguid, &meta) == SDF_SUCCESS) {
            shard_id = meta.shard + shard_index;
            plat_assert(shard_id <= SDF_SHARDID_LIMIT);
#ifdef MULTIPLE_FLASH_DEV_ENABLED
            flash_dev = get_flashdev_from_shardid(state->config.flash_dev,
                                                  shard_id, state->config.flash_dev_count);
#else
            flash_dev = state->config.flash_dev;
#endif                           
            shard = shardFind(flash_dev, shard_id);
        }
    }

    releaseLocalContainerParent(&lparent);

    return (shard);
}



/**
 * @brief Create single shard w/replicas using service (flash or replication) 
 *
 * XXX: Should return an SDF status
 *
 * @return 0 on success, non-zero on failure
 */
static int
shard_from_service(struct SDF_shared_state *state, service_t service,
                   const struct SDF_shard_meta *shard_meta, const char *cname, SDF_cguid_t cguid) {
    struct sdf_msg *send_msg = NULL;
    struct SDF_protocol_msg *send_pm;
    uint32_t send_len;
    struct sdf_msg *recv_msg = NULL;
    struct SDF_protocol_msg *recv_pm;
    int failed;
    SDF_status_t status;

    /*
     * XXX: Ideally this would use the action thread code which assigns
     * tag, etc. This changes once container operations are handled in
     * the data path so we won't have to worry.
     *
     * send_len does not include struct sdf_msg envelope length
     *
     * XXX: Need to include replication properties so this works in cases
     * other than the always replicate case.
     */
    send_len = sizeof (struct SDF_protocol_msg) + sizeof (*shard_meta);
    send_msg = sdf_msg_alloc(send_len);
    failed = !send_msg ? -ENOMEM : 0;

    if (!failed) {
        memset(send_msg, 0, sizeof (*send_msg));

        send_pm = (struct SDF_protocol_msg *)send_msg->msg_payload;
        memset(send_pm, 0, sizeof (*send_pm));
        send_pm->msgtype = HFCSH;
        send_pm->flags = 0;
        send_pm->status = SDF_SUCCESS;
        send_pm->node_from = state->config.my_node;
        send_pm->node_to = state->config.my_node;
        send_pm->cguid = cguid;
        send_pm->data_offset = 0;
        send_pm->data_size = sizeof (*shard_meta);
        send_pm->shard = shard_meta->sguid;


        /*
         * FIXME: drew 2009-06-24 Do we need send_pm->op_meta filled in 
         * some how? I don't think the replication code special cases
         * shard creation.
         */

        if (strlen(cname) >= sizeof(send_pm->cname)) {
	    failed = 1;
	    sdf_msg_free_buff(send_msg);
	} else {
	    (void) strcpy(send_pm->cname, cname);

	    memcpy(send_pm + 1, shard_meta, sizeof (*shard_meta));
	    plat_log_msg(21565, LOG_CAT_SHARD, PLAT_LOG_LEVEL_TRACE,
			 "create shard %lu on node %u", shard_meta->sguid,
			 state->config.my_node);

	    recv_msg = sdf_msg_send_receive(send_msg, send_len,
					    state->config.my_node /* dest */,
					    service /* dest */,
					    state->config.my_node /* src */,
					    state->config.response_service,
					    FLSH_REQUEST /* type */, SACK_REL_YES);
	    if (!recv_msg) {
		sdf_msg_free_buff(send_msg);
		plat_log_msg(21566, LOG_CAT_SHARD, LOG_ERR,
			     "create shard %lu on node %u failed to send message",
			     shard_meta->sguid, state->config.my_node);
		failed = -EIO;
	    }
        }
    }

    plat_assert_imply(!failed, recv_msg);
    if (!failed) {
        status = sdf_msg_get_error_status(recv_msg);
        if (status == SDF_SUCCESS) {
            recv_pm = (struct SDF_protocol_msg *)(recv_msg + 1);
            plat_assert(recv_msg->msg_len >= sizeof (*recv_msg) +
                        sizeof (*recv_pm));

            status = recv_pm->status;
        }

        if (status != SDF_SUCCESS) {
            plat_log_msg(21567, LOG_CAT_SHARD, LOG_ERR,
                         "create shard %lu on node %u received error %s",
                         shard_meta->sguid, state->config.my_node,
                         sdf_status_to_string(status));
            failed = -EIO;
        }
    }

    if (!failed) {
        plat_log_msg(21568, LOG_CAT_SHARD, PLAT_LOG_LEVEL_TRACE,
                     "create shard %lu on node %u succeeded",
                     shard_meta->sguid, state->config.my_node);
    } else {
        plat_log_msg(21569, LOG_CAT_SHARD, PLAT_LOG_LEVEL_ERROR,
                     "create shard %lu on node %u failed",
                     shard_meta->sguid, state->config.my_node);
    }

    if (recv_msg) {

        sdf_msg_free_buff(recv_msg);
    }

    return (failed ? -1 : 0);
}

/**
 * @brief Delete single shard w/replicas using service (flash or replication) 
 *
 * XXX: Should return an SDF status
 *
 * @return 0 on success, non-zero on failure
 */
int
delete_shard_from_service(struct SDF_shared_state *state, service_t service,
                   const char *cname, SDF_cguid_t cguid, SDF_shardid_t sguid) 
{
    struct sdf_msg *send_msg = NULL;
    struct SDF_protocol_msg *send_pm;
    uint32_t send_len;
    struct sdf_msg *recv_msg = NULL;
    struct SDF_protocol_msg *recv_pm;
    int failed;
    SDF_status_t status;

    // send_len does not include struct sdf_msg envelope length

    send_len = sizeof (struct SDF_protocol_msg);
    send_msg = sdf_msg_alloc(send_len);
    failed = !send_msg ? -ENOMEM : 0;

    if (!failed) {
        memset(send_msg, 0, sizeof (*send_msg));

        send_pm = (struct SDF_protocol_msg *)send_msg->msg_payload;
        memset(send_pm, 0, sizeof (*send_pm));
        send_pm->msgtype = HFDSH;
        send_pm->flags = 0;
        send_pm->status = SDF_SUCCESS;
        send_pm->node_from = state->config.my_node;
        send_pm->node_to = state->config.my_node;
        send_pm->cguid = cguid;
        send_pm->data_offset = 0;
        send_pm->data_size = 0;
        send_pm->shard = sguid;

        /*
         * FIXME: drew 2009-06-24 Do we need send_pm->op_meta filled in 
         * some how? I don't think the replication code special cases
         * shard creation.
         */

        if (strlen(cname) >= sizeof(send_pm->cname)) {
	    failed = 1;
	    sdf_msg_free_buff(send_msg);
	} else {
	    (void) strcpy(send_pm->cname, cname);

	    plat_log_msg(21570, LOG_CAT_SHARD, PLAT_LOG_LEVEL_TRACE,
			 "delete shard %lu on node %u", sguid,
			 state->config.my_node);

	    recv_msg = sdf_msg_send_receive(send_msg, send_len,
					    state->config.my_node /* dest */,
					    service /* dest */,
					    state->config.my_node /* src */,
					    state->config.response_service,
					    FLSH_REQUEST /* type */, SACK_REL_YES);
	    if (!recv_msg) {
		sdf_msg_free_buff(send_msg);
		plat_log_msg(21571, LOG_CAT_SHARD, LOG_ERR,
			     "delete shard %lu on node %u failed to send message",
			     sguid, state->config.my_node);
		failed = -EIO;
	    }
        }
    }

    plat_assert_imply(!failed, recv_msg);
    if (!failed) {
        status = sdf_msg_get_error_status(recv_msg);
        if (status == SDF_SUCCESS) {
            recv_pm = (struct SDF_protocol_msg *)(recv_msg + 1);
            plat_assert(recv_msg->msg_len >= sizeof (*recv_msg) +
                        sizeof (*recv_pm));

            status = recv_pm->status;
        }

        if (status != SDF_SUCCESS) {
            plat_log_msg(21572, LOG_CAT_SHARD, LOG_ERR,
                         "delete shard %lu on node %u received error %s",
                         sguid, state->config.my_node,
                         sdf_status_to_string(status));
            failed = -EIO;
        }
    }

    if (!failed) {
        plat_log_msg(21573, LOG_CAT_SHARD, PLAT_LOG_LEVEL_TRACE,
                     "delete shard %lu on node %u succeeded",
                     sguid, state->config.my_node);
    } else {
        plat_log_msg(21574, LOG_CAT_SHARD, PLAT_LOG_LEVEL_ERROR,
                     "delete shard %lu on node %u failed",
                     sguid, state->config.my_node);
    }

    if (recv_msg) {
        sdf_msg_free_buff(recv_msg);
    }

    return (failed ? -1 : 0);
}

#ifdef STM_INT
#ifdef SIMPLE_REPLICATION
void create_vip_group_meta_shard(SDF_internal_ctxt_t *pai, SDF_container_props_t props,
                                                       const char *cname, SDF_cguid_t cguid, SDF_shardid_t sh_id ) {
    struct SDF_shared_state *state = &sdf_shared_state;
    struct SDF_shard_meta shard_meta;
    SDF_shardid_t ret = SDF_SHARDID_INVALID;

    switch (props.container_type.type) {
        case SDF_BLOCK_CONTAINER:
#ifdef notyet
            shard_meta.type = SDF_SHARD_TYPE_BLOCK;
#else
            shard_meta.type = SDF_SHARD_TYPE_OBJECT;
#endif
            shard_meta.num_objs = 1;
            break;
        case SDF_OBJECT_CONTAINER:
        case SDF_LOCK_CONTAINER:
            shard_meta.type = SDF_SHARD_TYPE_OBJECT;
            break;
        case SDF_LOG_CONTAINER:
            shard_meta.type = SDF_SHARD_TYPE_LOG;
            break;
        case SDF_ANY_CONTAINER:
        case SDF_UNKNOWN_CONTAINER:
        case SDF_INVALID_CONTAINER:
            plat_assert(0);
    }

    if (SDF_TRUE == props.container_type.persistence) {
        shard_meta.persistence = SDF_SHARD_PERSISTENCE_YES;
    } else {
        shard_meta.persistence = SDF_SHARD_PERSISTENCE_NO;
    }

    if (SDF_TRUE == props.container_type.caching_container) {
        shard_meta.eviction = SDF_SHARD_EVICTION_CACHE;
    } else {
        shard_meta.eviction = SDF_SHARD_EVICTION_STORE;
    }

    shard_meta.first_node = msg_sdf_myrank();
    /* Fill Shard Meta Data */
    shard_meta.replication_props = props.replication;
    shard_meta.sguid = shard_meta.sguid_meta  = sh_id;
    shard_meta.replication_props.type         = SDFNodeGroupGroupTypeFromConfig(msg_sdf_myrank());
    shard_meta.inter_node_vip_group_group_id  = SDFNodeGroupGroupIdFromConfig(msg_sdf_myrank());
    if( (shard_meta.replication_props.type == SDF_REPLICATION_V1_2_WAY) ||
        (shard_meta.replication_props.type == SDF_REPLICATION_V1_N_PLUS_1) ) {

        shard_meta.replication_props.enabled      = SDF_TRUE;
        shard_meta.replication_props.num_replicas = SDFGetNumNodesInMyGroupFromConfig();

        plat_log_msg(21575, LOG_CAT_SHARD, LOG_DBG, "STMDBG: Create META shard id:%lx  metasguid:%lx CGUID:%lu\n", 
                                                                       shard_meta.sguid,shard_meta.sguid_meta,cguid);
        ret = shard_from_service(state, shard_meta.replication_props.enabled ? state->config.replication_service :
                                              state->config.flash_service, &shard_meta, cname, cguid);
        if( ret == SDF_SHARDID_INVALID ) {
            plat_log_msg(21576, LOG_CAT_SHARD, LOG_DBG, "STMDBG: Sh. Create id:%lx metasguid:%lx CGUID:%lu FAILED\n",
                                                   shard_meta.sguid,shard_meta.sguid_meta,cguid);
        }
    }
}
#endif
#endif


/**
 * @brief Create shards and all of their replicas.
 *
 * XXX: All shard-level meta-data operations should route to the
 * CMC home node for the given path instead of having the side-band interface
 * to either flash or the repliaction service.
 *
 * @param build_shard_type <IN> The CMC is currently special cased
 *
 * @return First of new shard IDs on success, SDF_SHARDID_INVALID on failure
 */
SDF_shardid_t
build_shard(struct SDF_shared_state *state, SDF_internal_ctxt_t *pai,
            const char *path, int num_objs, uint32_t shard_count,
            SDF_container_props_t props, SDF_cguid_t cguid,
            enum build_shard_type build_shard_type, const char *cname) 
{

    SDF_shardid_t ret = SDF_SHARDID_INVALID;
    SDF_shardid_t first_shard = SDF_SHARDID_INVALID;
    struct SDF_shard_meta shard_meta;
    flashDev_t * flash_dev;
    int need_meta_shard;
    int master_vnode;

    /** @brief flags are used for the direct path to flash */
    int flags;

    plat_log_msg(20819, LOG_CAT, LOG_TRACE, "%s", path);

    plat_assert(state->config.flash_dev);

    memset(&shard_meta, 0, sizeof (shard_meta));

    /*
     * XXX: All traffic should go through messaging API to home node so the
     * direct path disappears.
     */
    flags = 0;

    need_meta_shard = replication_meta_need_meta_shard(&props);

    ret = first_shard = generate_shard_ids(state->config.my_node, cguid,
                                           need_meta_shard ? shard_count * 2 :
                                           shard_count);

    switch (props.container_type.type) {
    case SDF_BLOCK_CONTAINER:
#ifdef notyet
        shard_meta.type = SDF_SHARD_TYPE_BLOCK;
        flags |= FLASH_SHARD_INIT_TYPE_BLOCK;
#else
        /*
         * FIXME: drew 2008-07-01 This is obviously incorrect, but is required
         * by the current flash code which doesn't create a sparse lock table
         * for "BLOCK" containers.
         */

        shard_meta.type = SDF_SHARD_TYPE_OBJECT;
        flags |= FLASH_SHARD_INIT_TYPE_OBJECT;
#endif

        /* FIXME: This is just what the code was using */
        shard_meta.num_objs = 1;
        break;
    case SDF_OBJECT_CONTAINER:
    case SDF_LOCK_CONTAINER:
        shard_meta.type = SDF_SHARD_TYPE_OBJECT;
        flags |= FLASH_SHARD_INIT_TYPE_OBJECT;
        shard_meta.num_objs = num_objs;
        break;
    case SDF_LOG_CONTAINER:
        shard_meta.type = SDF_SHARD_TYPE_LOG;
        flags |= FLASH_SHARD_INIT_TYPE_LOG;
        shard_meta.num_objs = num_objs;
        break;
    case SDF_ANY_CONTAINER:
    case SDF_UNKNOWN_CONTAINER:
    case SDF_INVALID_CONTAINER:
        plat_assert(0);
    }

#ifdef SDFAPIONLY
    flags |= (props.durability_level << 20);
#endif

    if (SDF_TRUE == props.container_type.persistence) {
        shard_meta.persistence = SDF_SHARD_PERSISTENCE_YES;
        flags |= FLASH_SHARD_INIT_PERSISTENCE_YES;
    } else {
        shard_meta.persistence = SDF_SHARD_PERSISTENCE_NO;
        flags |= FLASH_SHARD_INIT_PERSISTENCE_NO;
    }
   
    if (SDF_TRUE == props.container_type.caching_container) {
        shard_meta.eviction = SDF_SHARD_EVICTION_CACHE;
        flags |= FLASH_SHARD_INIT_EVICTION_CACHE;
    } else {
        shard_meta.eviction = SDF_SHARD_EVICTION_STORE;
        flags |= FLASH_SHARD_INIT_EVICTION_STORE;
    }

    /*
     * Calculate the per shard quota (container size in KB / shard count)
     */
    shard_meta.quota = props.container_id.size * 1024 / shard_count;	
    shard_meta.replication_props = props.replication;

#ifdef STATE_MACHINE_SUPPORT
    shard_meta.first_node = msg_sdf_myrank();
    shard_meta.inter_node_vip_group_group_id = SDFGetClusterGroupGroupId(pai,cguid);
#endif

    /*
     * XXX: It would be real nice to have an enum pretty-printer 
     * definition.
     */
    plat_log_msg(21577, PLAT_LOG_CAT_SDF_SHARED,
                 PLAT_LOG_LEVEL_TRACE,
                 "build_shard container_type.type %d num_objs %d num_replicas"
                 " %d",
                 props.container_type.type, num_objs,
                 shard_meta.replication_props.enabled ?
                 shard_meta.replication_props.num_replicas : 1);

    /*
     * XXX: drew 2008-10-14 Since runtime init + container creation are
     * merged, the flash messaging is not yet available and the container
     * must be locally created.
     *
     * This implies the CMC will always be unreplicated and needs to change.
     */

    int ii;
    for (ii = 0; ii < shard_count; ++ii) {
        /*ss
         * XXX: drew 2008-10-14 Correct node for shard goes here from sharding
         * API if not programatically determined;
         */
        shard_meta.first_node = state->config.my_node;
        shard_meta.sguid = first_shard + ii;

        if (need_meta_shard) {
            shard_meta.sguid_meta = first_shard + shard_count + ii;
        } else {
            shard_meta.sguid_meta = SDF_SHARDID_INVALID;
        }

        /* XXX:CA This short-circtuit needs to go away. We should always
           be calling the home_node (via shard_from_service) for all
           shard/flash operations. Otherwise things will break 
           in the multi-node world. */
#ifdef MULTIPLE_FLASH_DEV_ENABLED
        flash_dev =  get_flashdev_from_shardid(state->config.flash_dev, shard_meta.sguid,
                                               state->config.flash_dev_count);
#else
        flash_dev = state->config.flash_dev;
#endif

	master_vnode = props.master_vnode;

#ifdef SIMPLE_REPLICATION
	    if (SDFSimpleReplication) {
		if (!shardCreate(flash_dev, shard_meta.sguid,
				flags, shard_meta.quota  /* quota */,
				// Default in properties file
				shard_meta.num_objs)) {
		    ret = SDF_SHARDID_INVALID;
		}
                #ifdef STATE_MACHINE_SUPPORT
                {
                    int grp_type;
                    grp_type = SDFGetClusterGroupType(pai,props.container_id.container_id);
                    if( (grp_type == SDF_REPLICATION_V1_2_WAY) || (grp_type ==SDF_REPLICATION_V1_N_PLUS_1)) {
                        if (shard_from_service(state,
                                              shard_meta.replication_props.enabled ?
                                              state->config.replication_service :
                                              state->config.flash_service,
                                              &shard_meta, cname, cguid)) {
                            ret = SDF_SHARDID_INVALID;
                        }
                    }
                }
                #endif
	    } else {
		if (build_shard_type == BUILD_SHARD_CMC ||
		    (master_vnode != state->config.my_node) ||
		    !(state->config.flash_msg ||
		    shard_meta.replication_props.enabled)) 
		{
		    if (!shardCreate(flash_dev, shard_meta.sguid,
				    flags, shard_meta.quota  /* quota */,
				    // Default in properties file
				    shard_meta.num_objs)) {
			ret = SDF_SHARDID_INVALID;
		    }
		} else if (shard_from_service(state,
					      shard_meta.replication_props.enabled ?
					      state->config.replication_service :
					      state->config.flash_service,
					      &shard_meta, cname, cguid)) {
			ret = SDF_SHARDID_INVALID;
		}
	    }
#else // def SIMPLE_REPLICATION
	    if (build_shard_type == BUILD_SHARD_CMC ||
		(master_vnode != state->config.my_node) ||
		!(state->config.flash_msg ||
		shard_meta.replication_props.enabled)) 
	    {
		if (!shardCreate(flash_dev, shard_meta.sguid,
				flags, shard_meta.quota  /* quota */,
				// Default in properties file
				shard_meta.num_objs)) {
		    ret = SDF_SHARDID_INVALID;
		}
	    } else if (shard_from_service(state,
					  shard_meta.replication_props.enabled ?
					  state->config.replication_service :
					  state->config.flash_service,
					  &shard_meta, cname, cguid)) {
		    ret = SDF_SHARDID_INVALID;
	    }
#endif // else SIMPLE_REPLICATION
    }

    if (ret > SDF_SHARDID_LIMIT) {
        plat_log_msg(21578, LOG_CAT_SHARD, LOG_TRACE,
                     "FAILURE: SDFCreateContainer - %d shard",
                     shard_meta.type);
    }

    plat_log_msg(21579, LOG_CAT, LOG_TRACE, "%s - %lu", path, (unsigned long) ret);

    return (ret);
}


SDF_cguid_t
generate_cguid(SDF_internal_ctxt_t *pai, const char *path, uint32_t node, int64_t cntr_id64) 
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
    //cguid = cntr_id64 + NODE_MULTIPLIER; // so they don't collide with CMC_CGUID=1
    cguid = cntr_id64; 

    plat_log_msg(21580, LOG_CAT_SHARD, LOG_TRACE,
		 "generate_cguid: %llu", (unsigned long long) cguid);

    return (cguid);
}

int
_sdf_cguid_valid(SDF_cguid_t cguid) {
    return (cguid >= NODE_MULTIPLIER || cguid == CMC_CGUID);
}

/** @brief return contiguous range of count shardids */
/* XXX: This needs to persist (in the local container) */
SDF_shardid_t
generate_shard_ids(uint32_t node, SDF_cguid_t cguid, uint32_t count) {
    uint64_t shard_part = 0;
    uint64_t shard_id = 0;
    shard_part = 1;

/* can't allow shard_id and container_id (cguid) to exceed their
   respective bit budgets.  XXXCA : Improve error handling here (don't
   just assert) */
    plat_assert_always(shard_part < (SDF_SHARD_ID_MAX - count));

    plat_assert_always(cguid < SDF_CONTAINER_ID_MAX);

    shard_id =  (cguid << (64 - SDF_SHARD_ID_BITS) | shard_part);
    plat_log_msg(21581, LOG_CAT_SHARD, LOG_TRACE,
                 "generated shardids 0x%lx (+ %u reserved) for  cguid %lu", 
                 shard_id, count, cguid);

    return (shard_id);
}

int 
_sdf_shardid_valid(SDF_shardid_t shard) {
    return (shard >= NODE_MULTIPLIER);
}
