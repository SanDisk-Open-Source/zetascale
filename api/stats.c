//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/*
 * ZS Admin Interface.
 *
 * Author: Manavalan Krishnan
 *
 * Copyright (c) 2013 SanDisk Corporation.  All rights reserved.
 */
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <time.h> 
#include "zs.h"
#include "nvr_svc.h"
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include "sdf.h"
#include "sdf_internal.h"
#include "zs.h"
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
#include "ssd/fifo/mcd_bak.h"
#include "shared/init_sdf.h"
#include "shared/private.h"
#include "shared/open_container_mgr.h"
#include "shared/container_meta.h"
#include "shared/name_service.h"
#include "shared/shard_compute.h"
#include "shared/internal_blk_obj_api.h"
#include "agent/agent_common.h"
#include "agent/agent_helper.h"
#include "fdf_internal.h"
#ifdef FLIP_ENABLED
#include "flip/flip.h"
#endif
#include <ssd/fifo/scavenger.h>
#include "ssd/fifo/slab_gc.h"

/*
 * Functions defined in fdf.c
 */

static admin_config_t admin_config;


typedef struct {
    STATS_PRINT_TYPE type;
    char cname[128];
    int state; /* 0 - not running, 1 - running */
}stats_dump_cfg_t;
static stats_dump_cfg_t dump_thd_cfg;

#define MAX_CMD_TOKENS						20
#define STATS_BUFFER_SIZE					1024 

#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_NAMING
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_WARN PLAT_LOG_LEVEL_WARN
#define LOG_FATAL PLAT_LOG_LEVEL_FATAL

ZS_status_t print_container_stats_by_name(struct ZS_thread_state *thd_state,
                              FILE *fp, char *cname, int stats_type);
ZS_status_t print_container_stats_by_cguid( struct ZS_thread_state *thd_state,
                                   FILE *fp, ZS_cguid_t cguid, int stats_type);
void *ZSStatsDumpThread(void *arg);



/* Admin Commands 
 * container stats <detailed|mini> <cname>
 * container stats_dump <detailed|mini> <cname|all>
 * container list
 */

/*
 * Tokenize the command string by replacing whitespace with '\0' and update
 * the token array tokens with pointer to start of each token and length.
 * Returns total number of tokens.  The last valid token is the terminal
 * token (value points to the first unprocessed character of the string and
 * length zero).
 *
 * Usage example:
 *  while(tokenize_command(command, ncommand, tokens, max_tokens) > 0) {
 *      for(int ix = 0; tokens[ix].length != 0; ix++) {
 *          ...
 *      }
 *      ncommand = tokens[ix].value - command;
 *      command  = tokens[ix].value;
 *   }
 */
static size_t tokenize_adm_cmd(char *command, cmd_token_t *tokens, 
                                          const size_t max_tokens) {
    char *s, *e;
    size_t ntokens = 0;

    for (s = e = command; ntokens < max_tokens - 1; ++e) {
        if (*e == ' ') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
                *e = '\0';
            }
            s = e + 1;
        }
        else if (*e == '\0') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
            }
            
            break; /* string end */
        }
    }
    
    /*
     * If we scanned the whole string, the terminal value pointer is null,
     * otherwise it is the first unprocessed character.
     */
    tokens[ntokens].value =  *e == '\0' ? NULL : e;
    tokens[ntokens].length = 0;
    ntokens++;
    return ntokens;
}

ZS_status_t write_socket(int conn_fd, char *data, int len ) {
    int bytes_written, rc;
    int err = 0;

    bytes_written = 0;
    while( bytes_written != len ) {
        rc = write(conn_fd,data+bytes_written, (len - bytes_written));
        if ( rc < 0 ) {
            if ( err > 3 ) {
                plat_log_msg(160053, LOG_CAT, LOG_ERR,
                     "Unable to write string %s",data+bytes_written);
                return ZS_FAILURE;
            }
            err++;
            continue;
        }
        bytes_written = bytes_written + rc;
        err = 0;
    }
    return ZS_SUCCESS;
}

char *get_stats_catogory_desc_str(ZS_STATS_TYPE type) {
    if( type == ZS_STATS_TYPE_APP_REQ ) {
        return "Application requests";
    }
    else if( type == ZS_STATS_TYPE_FLASH ) {
        return "Flash statistics";
    }
    else if( type == ZS_STATS_TYPE_OVERWRITES )  {
        return "Overwrite and write-through statistics";
    }
    else if( type == ZS_STATS_TYPE_CACHE_TO_FLASH )  {
        return "Cache to Flash Manager requests";
    }
    else if( type == ZS_STATS_TYPE_FLASH_TO_CACHE )  {
        return "Flash Manager responses to cache";
    }
    else if( type == ZS_STATS_TYPE_FLASH_RC )  {
        return "Flash layer return codes";
    }
    else if( type == ZS_STATS_TYPE_PER_CACHE )  {
        return "Cache statistics";
    }
    else if( type == ZS_STATS_TYPE_FLASH_MANAGER )  {
        return "Flash Manager requests/responses";;
    }
    else if( type == ZS_STATS_TYPE_CONTAINER_FLASH )  {
        return "Container evictions";;
    }
    else if(type == ZS_STATS_TYPE_BTREE ) {
        return "Btree/Log layer statistics";
    }
    return "Unknown type:";
}


void print_stats_btree(FILE *fp, ZS_stats_t *stats) {
    int i;

    fprintf(fp,"  %s:\n",get_stats_catogory_desc_str(ZS_STATS_TYPE_APP_REQ));
    for (i = 0; i < ZS_N_ACCESS_TYPES; i++ ) { 
        if( (stats->n_accesses[i] == 0) || ( i < ZS_ACCESS_TYPES_READ ) ){ 
            continue;
        }   
        fprintf(fp,"    %s = %lu\n",get_access_type_stats_desc(i), 
                                                          stats->n_accesses[i]);
    }   
    fprintf(fp,"  %s:\n",get_stats_catogory_desc_str(ZS_STATS_TYPE_BTREE));
    for (i = 0; i < ZS_N_BTREE_STATS; i++ ) {
        if( stats->btree_stats[i] == 0 ) {
            continue;
        }
        fprintf(fp,"    %s = %lu\n",get_btree_stats_desc(i),
                                                      stats->btree_stats[i]);
    }
}

void print_nvram_stats(FILE *fp, ZS_stats_t *stats) {
    fprintf(fp,"NVRAM Statistics\n");
    fprintf(fp,"  Write requests = %"PRId64"\n", stats->nvr_stats[ZS_NVR_WRITE_REQS]);
    fprintf(fp,"  Write saved = %"PRId64"\n", stats->nvr_stats[ZS_NVR_WRITE_SAVED]);
    fprintf(fp,"  Sync requests = %"PRId64"\n", stats->nvr_stats[ZS_NVR_SYNC_REQS]);
    fprintf(fp,"  Sync restarts = %"PRId64"\n", stats->nvr_stats[ZS_NVR_SYNC_RESTARTS]);
    fprintf(fp,"  Sync splits = %"PRId64"\n", stats->nvr_stats[ZS_NVR_SYNC_SPLITS]);
    fprintf(fp,"  Total data received = %"PRId64"\n", stats->nvr_stats[ZS_NVR_DATA_IN]);
    fprintf(fp,"  Total data written = %"PRId64"\n", stats->nvr_stats[ZS_NVR_DATA_OUT]);
    fprintf(fp,"  Returned insufficient space = %"PRId64"\n", stats->nvr_stats[ZS_NVR_NOSPC]);

    fflush(fp);
}

void print_stats(FILE *fp, ZS_stats_t *stats, int cntr_stat) {
    int i, stats_type, category;
    char space[8] = "  ";

    if( is_btree_loaded() && (cntr_stat == 1 ) ) { 
        /* Btree enabled and function is called to print per cont. stats */
        print_stats_btree(fp,stats);
        fprintf(fp,"  %s:\n","ZS Layer statistics");
        snprintf(space,5,"%s","    ");
    }
    fprintf(fp,"%s%s:\n",space,get_stats_catogory_desc_str(ZS_STATS_TYPE_APP_REQ));
    for (i = 0; i < ZS_N_ACCESS_TYPES; i++ ) {
        if( (stats->n_accesses[i] == 0 )/* || ( i >= ZS_ACCESS_TYPES_READ )*/ ) {
            continue;
        }
        fprintf(fp,"%s  %s = %lu\n",space,get_access_type_stats_desc(i), 
                                                          stats->n_accesses[i]);
    }
    stats_type = ZS_STATS_TYPE_OVERWRITES; 
    fprintf(fp,"%s%s:\n",space,
                        get_stats_catogory_desc_str(ZS_STATS_TYPE_OVERWRITES));
    for (i = 0; i < ZS_N_CACHE_STATS; i++ ) {
        if( (i != ZS_CACHE_STAT_CACHE_MISSES) &&
            (i != ZS_CACHE_STAT_CACHE_HITS) &&
            (stats->cache_stats[i] == 0 )  ) {
            continue;
        }
        /* Do not print 0 stats for cache miss and hit in the container section */
        if( (stats->cache_stats[i] == 0 ) && (cntr_stat == 1) && 
            ((i == ZS_CACHE_STAT_CACHE_MISSES) || (i == ZS_CACHE_STAT_CACHE_HITS)))
        {
            continue;
        }
        category = get_cache_type_stats_category(i); 
        if ( category != stats_type )  {
            fprintf(fp,"%s%s:\n",space, get_stats_catogory_desc_str(category));
            stats_type = category;
        }
        fprintf(fp,"%s  %s = %lu\n",space,get_cache_type_stats_desc(i),
                                                        stats->cache_stats[i]);
    }
}

FILE *open_stats_dump_file() {
    const char *file_name;
    FILE *fp;
    /* Open the file */
    file_name = getProperty_String("ZS_STATS_FILE","/tmp/zsstats.log");
    fp = fopen(file_name,"a+");
    if( fp == NULL ) {
        plat_log_msg(160054, LOG_CAT, LOG_ERR,
                     "Unable to open stats file %s",file_name);
    }
    return fp;
}

ZS_status_t dump_container_stats_by_name(struct ZS_thread_state *thd_state,
                                                 char *cname, int stats_type) {
    int rc;
    FILE *fp;

    /* Open the file */
    fp = open_stats_dump_file();
    if( fp == NULL) {
        return ZS_FAILURE;
    }
    rc = print_container_stats_by_name(thd_state,fp,cname,stats_type);
    fflush(fp);
    fclose(fp);
    return rc;
}

ZS_status_t dump_container_stats_by_cguid(struct ZS_thread_state *thd_state,
                                           ZS_cguid_t cguid, int stats_type) {
    int rc;
    FILE *fp;
        
    /* Open the file */
    fp = open_stats_dump_file();
    if( fp == NULL ) {
        return ZS_FAILURE;
    }
    rc = print_container_stats_by_cguid(thd_state,fp,cguid,stats_type);
    fflush(fp);
    fclose(fp);
    return rc;
}       

ZS_status_t dump_all_container_stats(struct ZS_thread_state *thd_state,
                                                               int stats_type) {
    int i;
    FILE *fp;
    uint32_t n_cguids;
    ZS_cguid_t *cguids = NULL;
    ZS_status_t ret = ZS_SUCCESS;


    cguids = (ZS_cguid_t *) plat_alloc(sizeof(*cguids) * max_num_containers);
    if (cguids == NULL) {
	    return ZS_FAILURE;	
    }

    ret = ZSGetContainers(thd_state,cguids,&n_cguids);
	if ( ZS_SUCCESS != ret )
	    goto out;

    if( n_cguids <= 0 ) {
        plat_log_msg(160055, LOG_CAT, LOG_DBG,
                           "No container exists");
        ret = ZS_FAILURE;
	    goto out;
    }

    fp = open_stats_dump_file();
    if( fp == NULL ) {
        ret = ZS_FAILURE;
	    goto out;
    }

    for ( i = 0; i < n_cguids; i++ ) {
        print_container_stats_by_cguid(thd_state,fp,cguids[i],stats_type);
        fflush(fp);
    }
    fprintf(fp,"--------\n");
    fflush(fp);
    fclose(fp);

out:
    plat_free(cguids);
    return ret;
}

ZS_status_t log_summary_stats(struct ZS_thread_state *thd_state, FILE *fp) {
    int i = 0;
    time_t st;
    uint32_t n_cguids = 0;
    ZS_cguid_t *cguids = NULL;
    ZS_status_t ret = ZS_SUCCESS;
    ZS_container_props_t props;
    uint64_t total_hash_num_objs = 0;
    uint64_t total_btree_num_objs = 0;
    uint64_t total_num_objs = 0;
    uint64_t total_hash_used_space = 0;
    uint64_t total_btree_used_space = 0;
    uint64_t total_used_space = 0;
    int total_num_btree_containers = 0;
    int total_num_log_containers = 0;
    int total_num_hash_containers = 0;
	ZS_stats_t stat;
        
    cguids = (ZS_cguid_t *) plat_alloc(sizeof(*cguids) * max_num_containers);
    if (cguids == NULL) { 
        return ZS_FAILURE;
    }

    ret = ZSGetContainers(thd_state,cguids,&n_cguids); 
    if ( ZS_SUCCESS != ret ) 
        goto out;

    if( n_cguids <= 0 ) {
        plat_log_msg(160055, LOG_CAT, LOG_DBG, "No container exists");
        ret = ZS_FAILURE; 
        goto out;
    }

    for ( i = 0; i < n_cguids; i++ ) {
        ZS_cguid_t cguid = cguids[i];
        uint64_t num_objs = 0;
        uint64_t used_space = 0;

        /* Get container properties and print */
        ret = ZSGetContainerProps(thd_state, cguid, &props);
        if ( ret != ZS_SUCCESS ) { 
            if ( ZS_FAILURE_OPERATION_DISALLOWED != ret && ZS_FAILURE_CONTAINER_NOT_FOUND != ret ) 
                fprintf(fp,"Unable to get container properties for cguid %lu (error: %s)\n",cguid,ZSStrError(ret)); 
            goto out;
        }

        get_cntr_info(cguid, NULL, 0, &num_objs, &used_space, NULL, NULL);

	if (IS_ZS_HASH_CONTAINER(props.flags)) {
		++total_num_hash_containers;
		total_hash_num_objs += num_objs;
		total_hash_used_space += used_space;
	} else if (IS_ZS_LOG_CONTAINER(props.flags)) {
		++total_num_log_containers;
	} else {
		++total_num_btree_containers;
		total_btree_num_objs += num_objs;
		total_btree_used_space += used_space;
        }

        total_num_objs += num_objs;
        total_used_space += used_space;
    }

    time(&st);
    fprintf(fp,"Timestamp:%sSummary Statistics\n", ctime(&st));
    fprintf(fp,"  num_log_containers = %d\n", total_num_log_containers);
    fprintf(fp,"  num_btree_containers = %d\n", total_num_btree_containers);
    fprintf(fp,"  num_btree_container_objs = %lu\n", total_btree_num_objs);
    fprintf(fp,"  total_btree_container_used_space = %lu\n", total_btree_used_space);
    fprintf(fp,"  num_hash_containers = %d\n", total_num_hash_containers);
    fprintf(fp,"  num_hash_container_objs = %lu\n", total_hash_num_objs);
    fprintf(fp,"  total_hash_container_used_space = %lu\n", total_hash_used_space);
    fprintf(fp,"  total_num_objs = %lu\n", total_num_objs);
    fprintf(fp,"  total_used_space = %lu\n", total_used_space);


	get_nvram_stats(&stat);
	print_nvram_stats(fp, &stat);

    fflush(fp);

out:
    plat_free(cguids);
    return ret;
}

ZS_status_t log_container_props(struct ZS_thread_state *thd_state, FILE *fp) {
    int i = 0;
    time_t st;
    uint32_t n_cguids = 0;
    ZS_cguid_t *cguids = NULL;
    ZS_status_t ret = ZS_SUCCESS;
    ZS_container_props_t props;
    char *cname = NULL;
    char *type = NULL;
    uint64_t num_objs = 0;
    uint64_t used_space = 0;

        
    cguids = (ZS_cguid_t *) plat_alloc(sizeof(*cguids) * max_num_containers);
    if (cguids == NULL) { 
        return ZS_FAILURE;
    }

    ret = ZSGetContainers(thd_state,cguids,&n_cguids);
        if ( ZS_SUCCESS != ret )
            goto out;

    if( n_cguids <= 0 ) {
        plat_log_msg(160055, LOG_CAT, LOG_DBG, "No container exists");
        ret = ZS_FAILURE;
        goto out;
    }

    for ( i = 0; i < n_cguids; i++ ) {
        ZS_cguid_t cguid = cguids[i];

        /* Get container name */
        cname = ZSGetContainerName(cguid);
        if( cname == NULL ) {
             fprintf(fp,"Unable to container name for cguid %lu", cguid);
             cname = "unknown";
        }

        /* Get container properties and print */
        ret = ZSGetContainerProps(thd_state, cguid, &props);
        if ( ret != ZS_SUCCESS ) {
            if ( ZS_FAILURE_OPERATION_DISALLOWED != ret && ZS_FAILURE_CONTAINER_NOT_FOUND != ret )
                fprintf(fp,"Unable to get container properties for cguid %lu (error: %s)\n",cguid,ZSStrError(ret));
            goto out;
        }
    
        get_cntr_info(cguid, NULL, 0, &num_objs, &used_space, NULL, NULL);
    
        if (IS_ZS_HASH_CONTAINER(props.flags)) {
            type = "hash";
        } else if (IS_ZS_LOG_CONTAINER(props.flags)) {
            type = "log";
		} else {
            type = "btree";
        }

        time(&st);
        fprintf(fp,"Container Properties:\n"
                   "    name             = %s\n"
                   "    cguid            = %lu\n"
                   "    Size             = %lu kbytes\n"
                   "    persistence      = %s\n"
                   "    eviction         = %s\n"
                   "    writethrough     = %s\n"
                   "    fifo             = %s\n"
                   "    async_writes     = %s\n"
                   "    durability       = %s\n"
                   "    compression      = %s\n"
                   "    bypass_zs_cache = %s\n"
                   "    num_objs         = %lu\n"
                   "    used_space       = %lu\n"
                   "    type             = %s\n",
            cname, cguid, props.size_kb, get_bool_str(props.persistent),
            get_bool_str(props.evicting),get_bool_str(props.writethru),
            get_bool_str(props.fifo_mode),get_bool_str(props.async_writes),
            get_durability_str(props.durability_level),
            get_bool_str(props.compression), get_bool_str(props.flash_only),
            num_objs, used_space, type);
            fflush(fp);
    }

out:
    plat_free(cguids);
    return ret;
}

ZS_status_t log_container_stats(struct ZS_thread_state *thd_state, FILE *fp) {
    int i = 0;
    uint32_t n_cguids = 0;
    ZS_cguid_t *cguids = NULL;
    ZS_status_t ret = ZS_SUCCESS;
    char *cname = NULL;
    ZS_stats_t stats;
    cntr_map_t *cmap = NULL;
	int log_nvram = 0;
                   
                   
    cguids = (ZS_cguid_t *) plat_alloc(sizeof(*cguids) * max_num_containers);
    if (cguids == NULL) { 
        return ZS_FAILURE;
    }              
                   
    ret = ZSGetContainers(thd_state,cguids,&n_cguids); 
    if ( ZS_SUCCESS != ret ) 
        goto out;   
                   
    if( n_cguids <= 0 ) {
        plat_log_msg(160055, LOG_CAT, LOG_DBG, "No container exists");
        ret = ZS_FAILURE; 
        goto out;
    }       
            
    for ( i = 0; i < n_cguids; i++ ) {
        ZS_cguid_t cguid = cguids[i];
    
        /* Get container name */
        cname = ZSGetContainerName(cguid);
        if( cname == NULL ) {
             fprintf(fp,"Unable to container name for cguid %lu", cguid);
             cname = "unknown";
        }

        /* Get Per container stats */
        memset(&stats, 0, sizeof(ZS_stats_t));
        ret = ZSGetContainerStats(thd_state, cguid, &stats);
        if ( ret != ZS_SUCCESS ) { 
            goto out;
        }

        fprintf(fp,"Container Stats:\n"
                   "    name             = %s\n"
                   "    cguid            = %lu\n",
                   cname, cguid);

		if (log_nvram) {
			get_nvram_stats(&stats);
		}
		print_stats(fp, &stats, 1);

        // Output per container evictions
        cmap = zs_cmap_get_by_cguid( cguid );
        if ( cmap && cmap->container_stats.num_evictions > 0 ) { 
            fprintf(fp,"  %s:\n", get_stats_catogory_desc_str(ZS_STATS_TYPE_CONTAINER_FLASH)); 
            fprintf(fp,"    %s = %lu\n", "num_evictions", cmap->container_stats.num_evictions);
        }
		if (cmap->lc) {
			log_nvram = 1;
		}
        fflush(fp);
    }


out:
    plat_free(cguids);
    return ret;
}

ZS_status_t log_flash_stats(struct ZS_thread_state *thd_state, FILE *fp) {
    int i = 0;
    int len = 0;
    uint32_t n_cguids = 0;
    ZS_cguid_t *cguids = NULL;
    ZS_status_t ret = ZS_SUCCESS;
    ZS_stats_t stats;
    char stats_buffer[STATS_BUFFER_SIZE];
                   

    cguids = (ZS_cguid_t *) plat_alloc(sizeof(*cguids) * max_num_containers);
    if (cguids == NULL) { 
        return ZS_FAILURE;
    }              
     
    ret = ZSGetContainers(thd_state,cguids,&n_cguids);
    if ( ZS_SUCCESS != ret )
        goto out;

    if( n_cguids <= 0 ) {
        plat_log_msg(160055, LOG_CAT, LOG_DBG, "No container exists");
        ret = ZS_FAILURE;
        goto out;
    }

    for ( i = 0; i < n_cguids; i++ ) {
        int j;
        ZS_cguid_t cguid = cguids[i];

        /* Print Flash layer statistics */
        fprintf(fp, "Overall ZS Statistics\n");
        fprintf(fp,"  %s:\n", get_stats_catogory_desc_str(ZS_STATS_TYPE_FLASH));
        for (j = 0; j < ZS_N_FLASH_STATS; j++ ) {
            if ( stats.flash_stats[j] == 0 ) {
                continue;
            }
            fprintf(fp,"    %s = %lu\n",get_flash_type_stats_desc(j), stats.flash_stats[j]);
        }
        zs_get_flash_map(thd_state, cguid, stats_buffer, &len);
        fprintf(fp,"  Flash layout:\n%s", stats_buffer);

        /* Get the Total Flash stats */
        memset(&stats, 0, sizeof(ZS_stats_t));
        ret = ZSGetStats(thd_state,&stats);
        if ( ret != ZS_SUCCESS ) {
            goto out;
        }
        print_stats(fp, &stats, 0);
        fflush(fp);
        break; // Only need 1 set of stats
    }

out:
    plat_free(cguids);
    return ret;
}

ZS_status_t log_all_container_stats(struct ZS_thread_state *thd_state, FILE *fp, int stats_type) {
    int i;
    uint32_t n_cguids;
    ZS_cguid_t *cguids = NULL;
    ZS_status_t ret = ZS_SUCCESS;


    cguids = (ZS_cguid_t *) plat_alloc(sizeof(*cguids) * max_num_containers);
    if (cguids == NULL) {
        return ZS_FAILURE;
    }

    ret = ZSGetContainers(thd_state,cguids,&n_cguids);
    if ( ZS_SUCCESS != ret )
        goto out;

    if( n_cguids <= 0 ) {
        plat_log_msg(160055, LOG_CAT, LOG_DBG,
                           "No container exists");
        ret = ZS_FAILURE;
        goto out;
    }

    for ( i = 0; i < n_cguids; i++ ) {
        print_container_stats_by_cguid(thd_state,fp,cguids[i],stats_type);
        fflush(fp);
    }
    fflush(fp);

out:
    plat_free(cguids);
    return ret;
}

ZS_status_t print_container_stats_by_name(struct ZS_thread_state *thd_state,
                              FILE *fp, char *cname, int stats_type) {
    ZS_cguid_t cguid;

    /* Find cgiud for given container name */
    cguid = ZSGetCguid(cname);
    if ( cguid == SDF_NULL_CGUID ) {
        fprintf(fp,"Container %s not found", cname);
        return ZS_FAILURE;
    }
    return print_container_stats_by_cguid(thd_state,fp,cguid,stats_type);
}

char *get_bool_str( int val) {
    if( val == 1 ) {
        return "enabled";
    }
    else {
        return "disabled";
    }
}

char *get_durability_str(ZS_durability_level_t dura) {
    if ( dura == ZS_DURABILITY_PERIODIC ) {
        return "Periodic sync";
    }
    else if ( dura == ZS_DURABILITY_SW_CRASH_SAFE ) {
        return "Software crash safe";
    }
    else if ( dura == ZS_DURABILITY_HW_CRASH_SAFE ) {
        return "Hardware crash safe";
    }
    else {
         return "Unknown";
    }
}

ZS_status_t print_container_stats_by_cguid( struct ZS_thread_state *thd_state,
                                   FILE *fp, ZS_cguid_t cguid, int stats_type) {
    int len,i;
    time_t t;
    ZS_status_t rc;
    ZS_stats_t stats;
    char stats_buffer[STATS_BUFFER_SIZE], *cname;
    ZS_container_props_t props;
    uint64_t num_objs = 0;
    uint64_t used_space = 0;   
    cntr_map_t *cmap = NULL;
    char *type = NULL;

    /* Get container name */
    cname = ZSGetContainerName(cguid);
    if( cname == NULL ) {
         fprintf(fp,"Unable to container name for cguid %lu", cguid);
         return ZS_FAILURE;
    }

    /* Get container properties and print */
    rc = ZSGetContainerProps(thd_state,cguid,&props);
    if ( rc != ZS_SUCCESS ) {
		if ( ZS_FAILURE_OPERATION_DISALLOWED != rc && ZS_FAILURE_CONTAINER_NOT_FOUND != rc ) 
			fprintf(fp,"Unable to get container properties for %s(error:%s)\n",cname,ZSStrError(rc)); 
		return ZS_FAILURE;
    }

    if (props.flags & (1 << 0)) {
        type = "hash";
    } else if (props.flags & (1 << 1)) {
	type = "log";
    } else {
        type = "btree";
    }

    time(&t);
    /* Print the container properties */
    get_cntr_info(cguid,NULL, 0, &num_objs, &used_space, NULL, NULL);
    fprintf(fp,"Timestamp:%sPer Container Statistics\n"
                          "  Container Properties:\n"
                          "    name             = %s\n"
                          "    cguid            = %lu\n"
                          "    Size             = %lu kbytes\n"
                          "    persistence      = %s\n"
                          "    eviction         = %s\n"
                          "    writethrough     = %s\n"
                          "    fifo             = %s\n"
                          "    async_writes     = %s\n"
                          "    durability       = %s\n"
                          "    compression      = %s\n"
                          "    bypass_zs_cache = %s\n"
                          "    num_objs         = %lu\n"
                          "    used_space       = %lu\n"
                          "    type             = %s\n",
            ctime(&t),
            cname, cguid, props.size_kb, get_bool_str(props.persistent),
            get_bool_str(props.evicting),get_bool_str(props.writethru),
            get_bool_str(props.fifo_mode),get_bool_str(props.async_writes),
            get_durability_str(props.durability_level),
            get_bool_str(props.compression), get_bool_str(props.flash_only), 
            num_objs, used_space, type);

    /* Get Per container stats */
    memset(&stats,0,sizeof(ZS_stats_t));
    rc = ZSGetContainerStats(thd_state,cguid,&stats); 
    if ( rc != ZS_SUCCESS ) {
		if ( 0 && ZS_FAILURE_CONTAINER_DELETED != rc )
        	fprintf(fp,"ZSGetContainerStats failed for %s(error:%s)",cname,ZSStrError(rc));
        return ZS_FAILURE;
    }
    print_stats(fp,&stats, 1);

	// Output per container evictions 
	cmap = zs_cmap_get_by_cguid( cguid ); 
	if ( cmap && cmap->container_stats.num_evictions > 0 ) {
		fprintf(fp,"  %s:\n", get_stats_catogory_desc_str(ZS_STATS_TYPE_CONTAINER_FLASH));
		fprintf(fp,"    %s = %lu\n", "num_evictions", cmap->container_stats.num_evictions);
	}

    if( stats_type != STATS_PRINT_TYPE_DETAILED ) {   
        return ZS_SUCCESS;
    }
    /* Print Flash layer statistics */
    fprintf(fp,"Overall ZS Statistics\n");
    fprintf(fp,"  %s:\n",get_stats_catogory_desc_str(ZS_STATS_TYPE_FLASH));
    for (i = 0; i < ZS_N_FLASH_STATS; i++ ) {
        if( stats.flash_stats[i] == 0 ) {
            continue;
        }
        fprintf(fp,"    %s = %lu\n",get_flash_type_stats_desc(i),
                                                          stats.flash_stats[i]);
    }
    zs_get_flash_map(thd_state,cguid,stats_buffer,&len);        
    fprintf(fp,"  Flash layout:\n%s",stats_buffer);

    /* Get the Total Flash stats */
    memset(&stats,0,sizeof(ZS_stats_t));
    rc = ZSGetStats(thd_state,&stats);
    if ( rc != ZS_SUCCESS ) {
		if ( 0 && ZS_FAILURE_CONTAINER_DELETED != rc )
        	fprintf(fp,"ZSGetStats failed for %s(error:%s)", cname,ZSStrError(rc));
        return ZS_FAILURE;
    }
    print_stats(fp,&stats, 0);
    fflush(fp);
    return ZS_SUCCESS;
}
static void process_log_level_cmd(struct ZS_thread_state *thd_state,
                       FILE *fp, cmd_token_t *tokens, size_t ntokens){
    if ( ntokens < 2 ) {
        fprintf(fp,"Invalid argument! Type help for more info\n");
        return;
    }
    if ( strcmp(tokens[1].value,"get" ) == 0 ){
        fprintf(fp,"%s\n",get_log_level());
    }
    else if ( strcmp(tokens[1].value,"set" ) == 0 ){
        if( ntokens < 3 ) {
            fprintf(fp,"Invalid arguments! Type help for more info\n");
            return;
        }
        if( change_log_level(tokens[2].value) != ZS_SUCCESS ) {
            fprintf(fp,"Invalid log level %s! Type help for more info\n",
                           tokens[2].value);
        }
    }
    else { 
        fprintf(fp,"Invalid argument(%s)! Type help for more info\n",
                             tokens[1].value);
    }
}

static void process_scavenger_cmd(struct ZS_thread_state *thd_state,
                            FILE *fp, cmd_token_t *tokens, size_t ntokens)
{
    if ( ntokens < 2 ) {
        fprintf(fp,"Invalid argument! Type help for more info\n");
        return;
    }

    if ( 0 == getProperty_uLongInt("ZS_EXPIRY_SCAVENGER_ENABLE", 1)) {
        fprintf(fp,"scavenger not enabled, use ZS_EXPIRY_SCAVENGER_ENABLE\n");
        return;
    }

    if ( strcmp(tokens[1].value,"start" ) == 0 ){
        if(ZS_SUCCESS == zs_start_scavenger_thread(
                    (struct ZS_state *)0xdeadbeef)){
            fprintf(fp,"scavenger started\n");
        } else {
            fprintf(fp,"scavenger start failed\n");   
        }
    } else if (strcmp(tokens[1].value,"stop" ) == 0 ){
        if(ZS_SUCCESS == zs_stop_scavenger_thread()){
            fprintf(fp,"scavenger stopped\n");
        }
    }
}

ZS_status_t zs_start_defrag_thread(struct ZS_state *zs_state );
ZS_status_t zs_stop_defrag_thread();

static void process_defrag_cmd(struct ZS_thread_state *thd_state,
                        FILE *fp, cmd_token_t *tokens, size_t ntokens)
{
    if ( ntokens < 2 ) {
        fprintf(fp,"Invalid argument! Type help for more info\n");
        return;
    }

    if ( 0 == getProperty_uLongInt("ZS_DEFRAG_ENABLE", 0)) {
        fprintf(fp,"defragmenter not enabled, use ZS_DEFRAG_ENABLE\n");
        return;
    }

    if ( strcmp(tokens[1].value,"start" ) == 0 ){
        if(ZS_SUCCESS == zs_start_defrag_thread(
                    (struct ZS_state *)0xdeadbeef)){
            fprintf(fp,"defragmenter started\n");
        } else {
            fprintf(fp,"defragmenter start failed\n");
        }
    } else if (strcmp(tokens[1].value,"stop" ) == 0 ){
        if(ZS_SUCCESS == zs_stop_defrag_thread()){
            fprintf(fp,"defragmenter stopped\n");
        }
    }
}

static void process_container_cmd(struct ZS_thread_state *thd_state, 
                       FILE *fp, cmd_token_t *tokens, size_t ntokens){
    int rc,i;
    int stats_type = 0;
    char *cname;
    pthread_attr_t attr;
    pthread_t thread;
    //fprintf(stderr,"Number of tokens:%d\n",(int)ntokens);
    if ( ntokens < 2 ) {
        fprintf(fp,"Invalid argument! Type help for more info\n");
        return;
    }
    if( strcmp(tokens[1].value,"stats" ) == 0 ){
        if( ntokens < 3 ) {
            fprintf(fp,"Invalid arguments! Type help for more info\n");
            return;
        }
        if( ntokens >= 4 ) { 
            if( strcmp(tokens[3].value,"v" ) == 0) {
                stats_type = STATS_PRINT_TYPE_DETAILED;
            }
        }
        print_container_stats_by_name(thd_state, fp, 
                                                  tokens[2].value, stats_type);
        return;
    }
    else if( strcmp(tokens[1].value,"stats_dump" ) == 0 ){
        if( is_auto_dump_enabled() ) {
            fprintf(fp,"Periodic stats dump has been enabled."
                                             " Can not dump container stats\n");
            return;
        }
        if( ntokens < 3 ) {
            fprintf(fp,"Invalid arguments for stats_dump:" 
                                                 " Type help for more info\n");
            return;
        }
        /* Check if dump thread is already running */
        if( dump_thd_cfg.state == 1 ) {
             fprintf(fp,"A dump is already in progress. Pls try later\n");
            return;
        }
        if( dump_thd_cfg.state == 1 ) {
             fprintf(fp,"A dump is already in progress. Pls try later\n");
            return;
        }
        if( ntokens >= 4 ) { 
            if( strcmp(tokens[3].value,"v" ) == 0 ) {
                stats_type = STATS_PRINT_TYPE_DETAILED;
            }
        }
        dump_thd_cfg.state = 1;
        strcpy(dump_thd_cfg.cname,tokens[2].value);
        dump_thd_cfg.type = stats_type;

        /* Create dump thread */
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        rc = pthread_create(&thread, &attr,ZSStatsDumpThread, NULL);
        if( rc ) {
            fprintf(fp,"Unable to create dump thread\n");
            dump_thd_cfg.state = 0;
        }
    }
    else if( strcmp(tokens[1].value,"autodump" ) == 0 ) {
        if( ntokens < 3 ) {
            fprintf(fp,"Invalid arguments for autodump"
                                                 " Type help for more info\n");
            return;
        }
        if( strcmp(tokens[2].value,"enable") == 0 ) {
            enable_stats_auto_dump();
        }
        else if( strcmp(tokens[2].value,"disable") == 0 ) {
            disable_stats_auto_dump();
        }
        else if( strcmp(tokens[2].value,"interval") == 0 ) {
            if ( ntokens < 4 ) {
                 fprintf(fp,"Invalid arguments for autodump interval"
                                             "Type help for more info\n");
                 return;
            }
            if ( atoi(tokens[3].value) < 0 ) {
                fprintf(fp,"Invalid auto dump interval:%d\n",
                                                  atoi(tokens[3].value));
                return;
            } 
            set_stats_autodump_interval(atoi(tokens[3].value));
        }
        else if( strcmp(tokens[2].value,"printcfg") == 0 ) {
            fprintf(fp,"Auto dump:%s Interval:%d\n",
                   is_auto_dump_enabled()?"enabled":"disabled", 
                   get_autodump_interval());
        }
        else {
            fprintf(fp,"Invalid subcommand %s\n", tokens[2].value);
            return;
        }
    }
    else if( strcmp(tokens[1].value,"list" ) == 0 ) {
	    struct ZSCMapIterator *iterator = NULL; 
            ZS_cguid_t cguid;
            bool verbose = ((ntokens > 2) && 
                            (strcmp(tokens[2].value, "-v") == 0));

	    for ( i = 0; i < max_num_containers; i++ ) {
	        cname = ZSGetNextContainerName(thd_state, &iterator, &cguid); 
	        if ( cname ) {     
                    if (verbose) fprintf(fp, "%"PRIu64": ", cguid);
	            fprintf(fp,"%s\n",cname); 
	            continue; 
	        }
	        break;
	    } 
    }
    else {
        //fprintf(stderr,"Command:(%s)\n",tokens[1].value);
        fprintf(fp,"Invalid command:(%s). Type help for list"
                         " of commands\n",tokens[1].value);
    }
}

/* From enumerate.c */
ZS_status_t cguid_to_shard(
        SDF_action_init_t *pai, 
        ZS_cguid_t cguid, 
        shard_t **shard_ptr, 
        int delete_ok);

static void process_slab_gc_cmd(struct ZS_thread_state *thd_state, 
                       FILE *fp, cmd_token_t *tokens, size_t ntokens){
    if ( ntokens < 2 ) {
        fprintf(fp,"Invalid argument! Type help for more info\n");
        return;
    }

    shard_t *shard; 
    SDF_action_init_t *pai = (SDF_action_init_t *) thd_state; 
    int s = cguid_to_shard(pai, VDC_CGUID, &shard, 0); 
    if (s != ZS_SUCCESS) {
        fprintf(fp, "Error getting shard from cguid.\n");
        return;
    }
    mcd_osd_shard_t *mcd_shard = (mcd_osd_shard_t *)shard;

    if( strcmp(tokens[1].value,"enable") == 0 )
        slab_gc_init(mcd_shard, getProperty_Int("ZS_SLAB_GC_THRESHOLD", 70 ));
    else if( strcmp(tokens[1].value,"disable") == 0 )
        slab_gc_end(mcd_shard);
    else if( strcmp(tokens[1].value,"threshold") == 0 ) {
        if ( ntokens < 3 ) {
            fprintf(fp,"Invalid arguments. Slab GC threshold. "
                    "Type help for more info\n");
            return;
        }
        int t = atoi(tokens[2].value);
        if (t < 0 || t > 100 || (!t && tokens[2].value[0] !='0') || strlen(tokens[2].value) > 5) {
            fprintf(fp,"Invalid slab GC threshold: %.5s\n", tokens[2].value);
            return;
        }
        slab_gc_update_threshold(mcd_shard, t);

        char* val = strndup(tokens[2].value, 6);
        if(val)
            setProperty("ZS_SLAB_GC_THRESHOLD", val);
    }
    else {
        fprintf(fp,"Invalid arguments. Type help for more info\n");
        return;
    }
    fprintf(fp, "OK\n");
}

static void print_admin_command_usage(FILE *fp) {
    fprintf(fp,"\nSupported commands:\n" 
                   "container stats <container name> [v]\n"
                   "container stats_dump <container name|all> [v]\n"
                   "container autodump <enable/disable/interval/printcfg>"
                   " [interval in secs]\n"
                   "container list [-v]\n"
                   "log_level <set/get> [fatal/error/warning/info/diagnostic/"
                   "debug/trace/trace_low/devel]\n"
                   "help\nquit\n");
}

static ZS_status_t process_admin_cmd( struct ZS_thread_state *thd_state, 
                                                      FILE *fp, char *cmd ) {
    size_t ntokens;
    cmd_token_t tokens[MAX_CMD_TOKENS];

    /* Parse the command */
    ntokens = tokenize_adm_cmd(cmd, tokens, MAX_CMD_TOKENS);
    ntokens--;
    //fprintf(stderr,"Input command:(%s) ntokens:%u\n",cmd,(int)ntokens);
    if ( ntokens <= 0 ) {
        fprintf(fp,"Please specify a command." 
                                           "Type help for list of commands\n");
        return ZS_SUCCESS;
    }  
    /*
    for(len = 0; len < ntokens; len++ ) {
       fprintf(stderr,"token[%d]:%s\n",len,tokens[len].value);
    } */
    if( strcmp(tokens[0].value,"container") == 0 ) {
        process_container_cmd(thd_state, fp,tokens,ntokens);
    }
    else if( strcmp(tokens[0].value,"gc") == 0 ) {
        process_slab_gc_cmd(thd_state, fp, tokens, ntokens);
    }
    else if( strcmp(tokens[0].value,"help") == 0 ) {
        print_admin_command_usage(fp);
        ext_cbs->admin_cb(thd_state, fp, tokens, ntokens);
    }
    else if( strcmp(tokens[0].value,"log_level") == 0 ) {
        process_log_level_cmd(thd_state, fp,tokens,ntokens);
    }
#ifdef FLIP_ENABLED
    else if (strcmp(tokens[0].value, "flip") == 0) {
        process_flip_cmd(fp, tokens, ntokens);
    }
#endif
    else if (strcmp(tokens[0].value, "snapshot") == 0) {
        ext_cbs->admin_cb(thd_state, fp, tokens, ntokens);
    }
    else if( strcmp(tokens[0].value,"quit") == 0 ) {
         return ZS_FAILURE;
    }
    else if( strcmp(tokens[0].value,"scavenger") == 0 ) {
        process_scavenger_cmd(thd_state, fp, tokens, ntokens);
    } 
    else if( strcmp(tokens[0].value,"defrag") == 0 ) {
        process_defrag_cmd(thd_state, fp, tokens, ntokens);
    }
    else {
        fprintf(fp,"Invalid command:(%s)." 
                          "Type help for list of commands\n",cmd);
    }
    return ZS_SUCCESS;
}

#define CMD_BUFFER_SIZE 256

void *ZSStatsDumpThread(void *arg) {
    int ret;
    struct ZS_thread_state *thd_state;
    //fprintf(stderr,"Starting Dump thread...\n");

    ret = ZSInitPerThreadState(admin_config.zs_state,
                                 (struct ZS_thread_state **) &thd_state );
    if (ret != ZS_SUCCESS) {
        plat_log_msg(160056,LOG_CAT, LOG_ERR,
                    "Unable to create thread state(%d)\n", ret);
        dump_thd_cfg.state = 0;
        return NULL;
    }
    if( strcmp(dump_thd_cfg.cname,"all") == 0 ) {
        dump_all_container_stats(thd_state,dump_thd_cfg.type);
    }
    else {
        dump_container_stats_by_name(thd_state,dump_thd_cfg.cname,
                                                    dump_thd_cfg.type);
    }
    dump_thd_cfg.state = 0;
    return NULL;
}

#define CMD_PROMPT   "zs> "

void *ZSAdminThread(void *arg) {
    int server_fd, conn_fd, ret,i;
    FILE *fp;
    struct sockaddr_in srv_addr;
    admin_config_t *adm_cfg;
    char buffer[CMD_BUFFER_SIZE];
    struct ZS_thread_state *thd_state;
    

    /* Create server socket and listen on it for commands */
    adm_cfg = (admin_config_t *)arg;

    ret = ZSInitPerThreadState( adm_cfg->zs_state, 
                                 (struct ZS_thread_state **) &thd_state );
    if (ret != ZS_SUCCESS) {
        plat_log_msg(160056,LOG_CAT, LOG_ERR,
                            "Unable to create thread state(%d)\n", ret);
        return NULL;
    }

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if( server_fd < 0 ) {
        plat_log_msg(160057, LOG_CAT, LOG_ERR, 
                                   "Unable to open socket for admin port");
        return 0;    
    }
    memset(&srv_addr, '0', sizeof(srv_addr));

    ret = 1;
    ret = setsockopt( server_fd, SOL_SOCKET, SO_REUSEADDR, &ret, sizeof(ret) );

    srv_addr.sin_family      = AF_INET;
    srv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    srv_addr.sin_port        = htons(adm_cfg->admin_port); 

    ret = bind(server_fd, (struct sockaddr*)&srv_addr, sizeof(srv_addr));
    if ( ret < 0 ) {
         plat_log_msg(160058, LOG_CAT, LOG_ERR, 
                     "Unable to bind admin port %u",adm_cfg->admin_port);
         return 0;
    }
    listen(server_fd, 5); 
    while(1)
    {
        conn_fd = accept(server_fd, (struct sockaddr*)NULL, NULL); 
        if( conn_fd < 0 ) {
            plat_log_msg(160059, LOG_CAT, LOG_ERR,
                         "Unable to accept new connections");
            continue;
        }
        fp = fdopen(conn_fd,"a+");
        if( fp == NULL ) {
            plat_log_msg(70113, LOG_CAT, LOG_ERR,
                         "Unable to open file descriptor ");
            continue;
        }
        /* Process the connection and respond*/
        while (1) {
            fprintf(fp, CMD_PROMPT);
            fflush(fp);

            ret = read(conn_fd,buffer,CMD_BUFFER_SIZE); 
            if( ret < 0 ) {
                break;
            }
            if( ret == 0 ) {
                /*Client closed the connectio. Break */
                break;
            }
            /* Remove the \r\n char. at the end */
            for( i = 0; i < ret; i++ ) {
                if( (buffer[i] == '\r') || (buffer[i] == '\n')) {
                    buffer[i] = 0; 
                }
            } 
            if(process_admin_cmd(thd_state, fp,buffer) == ZS_FAILURE) {
                break;
            }
            fflush(fp);
        }
        /*close the connection*/ 
        fclose(fp);
    }
    plat_log_msg(160060, LOG_CAT, LOG_DBG,"Admin thread exiting...");
    return 0;
}
ZS_status_t zs_start_admin_thread( struct ZS_state *zs_state ) {
    pthread_t thd;
    int rc;

    admin_config.admin_port  = getProperty_Int( "ZS_ADMIN_PORT", 51350 );
    admin_config.num_threads = 1;
    admin_config.zs_state   = zs_state;

    if(!getProperty_Int( "ZS_TEST_MODE", 0) || getProperty_Int("ZS_ADMIN_PORT", 0))
	{
        plat_log_msg(80027,LOG_CAT, LOG_INFO,
              "Starting ZS admin on TCP Port:%u", admin_config.admin_port);
		/* Create Admin thread */    
		rc = pthread_create(&thd,NULL,ZSAdminThread,(void *)&admin_config);
		if( rc != 0 ) {
			plat_log_msg(80028,LOG_CAT, LOG_ERR,
					"Unable to start admin on TCP Port:%u",admin_config.admin_port);
		}
	}

    return ZS_SUCCESS; 
}

ZS_status_t zs_stop_admin_thread(uint16_t admin_port, uint16_t num_thds ) {
    /* Create Admin thread */  
    return ZS_SUCCESS;
}

