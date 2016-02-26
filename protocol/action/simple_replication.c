/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   simple_replication.c
 * Author: Brian O'Krafka
 *
 * Created on July 04, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_qrep.c 8284 2009-07-03 01:39:16Z briano $
 */

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include "sdfmsg/sdf_msg_types.h"

#include "common/sdftypes.h"
#include "platform/stdlib.h"
#include "platform/shmem.h"
#include "platform/assert.h"
#include "platform/stats.h"
#include "platform/time.h"
#include "shared/sdf_sm_msg.h"
#include "shared/name_service.h"
#include "shared/shard_compute.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_common.h"
#include "shared/init_sdf.h"
#include "shared/private.h"
#include "shared/object.h"
#include "shared/container.h"
#include "ssd/ssd.h"
#include "ssd/ssd_aio.h"
#include "ssd/ssd_local.h"
#include "protocol/action/fastcc_new.h"
#include "protocol/action/action_thread.h"
#include "protocol/action/action_new.h"
#include "protocol/home/home_flash.h"
#include "protocol/replication/key_lock.h"
#include "protocol/replication/replicator.h"
#include "protocol/replication/rpc.h"
#include "protocol/replication/sdf_vips.h"
#include "protocol/protocol_alloc.h"
#include "protocol/action/simple_replication.h"
#include "utils/properties.h"
#include "sdftcp/msg_map.h"
#include <inttypes.h>
#include "shared/container.h"
#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif /* SDFAPI */
#include "recovery.h"

/*
 * XXX: drew 2009-12-07 HAVE_CONFIG_H is needed to avoid memcached.h
 * redefinitions of things from <stdint>; although the right fix is to
 * eliminate this back-door mess.
 */
//#ifndef HAVE_CONFIG_H
//#define HAVE_CONFIG_H 1
//#endif
#include "ssd/fifo/mcd_rep.h"
#include "ssd/fifo/mcd_osd.h"
// #include "../../apps/memcached/server/memcached-1.2.5-schooner/memcached.h"

int SDFSimpleReplication = 0; // non-zero if simple replication is on
struct sdf_vip_config *vip_grp_config = NULL;
fthMbox_t rec_thread_avail_mbox;

struct cursor_data * cursor_datas;

int num_sync_container_threads = 0;

fthLock_t sync_remote_container_lock;

/********************************************
 *
 *   New recovery data copy functions
 *
 ********************************************/

// set to 0 to disable new recovery, non-zero otherwise
#define NEW_RECOVERY_CODE     0

static SDF_status_t simple_replicator_sync_remote_container_new(
			SDF_action_init_t * pai,
			SDF_action_state_t *pas,
			vnode_t dest_node, SDF_cguid_t cguid);
static SDF_status_t rpc_get_by_cursor_new(
          SDF_action_state_t *pas,
          SDF_context_t ctxt, SDF_shardid_t shard, vnode_t src_node,
	  vnode_t dest_node, void *cursor,
	  size_t cursor_len, SDF_cguid_t cguid,
	  char **key, int *key_len,
	  SDF_time_t *exptime, SDF_time_t *createtime,
	  uint64_t *seqno, void **data, size_t *data_len, 
	  uint32_t *flags, fthMbox_t *req_resp_fthmbx);
static SDF_status_t rpc_get_iteration_cursors_new(
          SDF_action_state_t *pas,
          SDF_context_t ctxt, vnode_t src_node, vnode_t dest_node,
	  SDF_shardid_t shard, SDF_cguid_t cguid, uint64_t seqno_start,
	  uint64_t seqno_len, uint64_t seqno_max,
	  void *cursor, int cursor_size,
	  struct flashGetIterationOutput **out);
static void sync_container_thread_new(uint64_t arg);

/********************************************/

static sdf_msg_t * rpc_load_msg(SDF_vnode_t node_from, 
                                SDF_vnode_t node_to, 
                                SDF_protocol_msg_type_t msg_type,
                                SDF_size_t data_size, void *pdata,
                                SDF_context_t ctxt, SDF_cguid_t cguid,
                                int seqno_start, int seqno_max,
                                int seqno_len, SDF_size_t * message_size,
                                SDF_shardid_t shard);

int  (*sdf_start_simple)  (int) = NULL;
void (*sdf_notify_simple) (uint64_t, int) = NULL;
SDF_boolean_t (*sdf_is_node_started_first_time)() = NULL;
SDF_boolean_t (*sdf_is_node_started_in_auth_mode)() = NULL;
void (*sdf_simple_dead)   (int, int) = NULL;
int (*sdf_remove_vip)   (sdf_vip_config_t *, int) = NULL;
SDF_action_init_t *(*sdf_action_init_ptr)() = NULL;
int  (*sdf_iterate_cursors_progress)(struct shard *, struct flashGetIterationResumeOutput *) = NULL;

int  (*sdf_mcd_format_container_internal)( void *, int );
int (*sdf_mcd_start_container_internal)( void *, int );
int (*sdf_mcd_stop_container_internal)( void *, int );

int  (*sdf_mcd_format_container_byname_internal)( void *, char * );
int (*sdf_mcd_start_container_byname_internal)( void *, char * );
int (*sdf_mcd_stop_container_byname_internal)( void *, char * );

int (*sdf_mcd_get_tcp_port_by_cguid)( SDF_cguid_t, int *);
int (*sdf_mcd_get_cname_by_cguid)( SDF_cguid_t, char *);
int (*sdf_mcd_is_container_running)(int);
int (*sdf_mcd_is_container_running_byname)(char *);
int (*sdf_mcd_processing_container_commands)(void);

int (*simple_replication_flash_put) (struct ssdaio_ctxt *, struct shard *, struct objMetaData *, char *, char *, int) = NULL;
ssdaio_ctxt_t * (*simple_replication_init_ctxt) (int) = NULL;
struct shard *(*simple_replication_shard_find) (struct flashDev *, uint64_t);

extern int sdf_msg_myrank();
extern struct sdf_vip_group_group *
sdf_vip_config_get_group_group_by_gid(const struct sdf_vip_config *, int );
extern struct SDF_shared_state sdf_shared_state;
extern SDF_status_t get_status(int retcode);
struct sdf_vip_config *sdf_get_vip_config_instance();
#define SDF_VIP_TEST 1

void print_sdf_vip_config( sdf_vip_config_t *vipcfg );

/*
 * Static functions declarations
 */
static SDF_status_t simple_replicator_sync_remote_container(SDF_action_init_t * pai, SDF_action_state_t *pas, vnode_t dest_node, SDF_cguid_t cguid);
static SDF_status_t simple_replicator_request_replicating(SDF_action_state_t *pas, vnode_t master, vnode_t slave, qrep_ctnr_state_t * pcs);
static SDF_status_t simple_replicator_sync_flush_times(SDF_action_init_t * pai, SDF_cguid_t cguid);
static SDF_status_t rpc_start_replicating(SDF_context_t ctxt, SDF_cguid_t cguid, vnode_t src_node, vnode_t dest_node, SDF_shardid_t shard, void * data, size_t * data_len);
static SDF_status_t rpc_get_iteration_cursors(SDF_action_state_t *pas,
                                              SDF_context_t ctxt, vnode_t src_node, vnode_t dest_node, SDF_shardid_t shard,
                                              SDF_cguid_t cguid, uint64_t seqno_start, uint64_t seqno_len, uint64_t seqno_max,
                                              void *cursor, int cursor_size, struct flashGetIterationOutput **out);
static SDF_status_t rpc_get_by_cursor(SDF_context_t ctxt, SDF_shardid_t shard, vnode_t src_node, vnode_t dest_node, void *cursor,
                                      size_t cursor_len, SDF_cguid_t cguid, fthMbox_t *req_resp_fthmbx, char **key, int *key_len,
                                      SDF_time_t *exptime, SDF_time_t *createtime, uint64_t *seqno, void **data, size_t *data_len, uint32_t *flags);
SDF_status_t rpc_release_vip_group(SDF_context_t ctxt, SDF_cguid_t cguid,
                                   vnode_t src_node,
                                   vnode_t dest_node,
                                   SDF_shardid_t shard);
static int64_t rpc_get_clock_skew(SDF_context_t ctxt, vnode_t src_node,
                                  vnode_t dest_node, SDF_shardid_t shard,
                                  SDF_cguid_t cguid, int *rec_ver_ptr);

static void sync_container_thread(uint64_t arg);

static void live_back(int live, int rank, void *arg);

static void simple_replicator_node_live(struct SDF_action_state *pas, vnode_t pnode);
static void simple_replicator_node_dead(struct SDF_action_state *pas, vnode_t pnode);
static int fast_copy_ctr(SDF_action_init_t * pai, SDF_action_state_t *pas,
                         vnode_t master, SDF_cguid_t cguid);

int SDFNodeGroupGroupTypeFromConfig( int node_id );
int SDFNodeGroupGroupIdFromConfig( int node_id );
int SDFMyGroupGroupTypeFromConfig();
int SDFGetNumNodesInMyGroupFromConfig();
int SDFGetNumNodesInClusterFromConfig();
void send_recovery_start_event(int32_t pnode, QREP_RECOVERY_TYPE rtype);
    
SDF_cguid_t get_container_guid(qrep_ctnr_state_t *cntnr);
int cguid_to_index(SDF_action_state_t * pas, SDF_cguid_t cguid);
void data_recovery_thread(uint64_t arg);
void simple_replicator_send_data_copy_completed();
#ifdef SDF_VIP_TEST
/*The following print_* functions are used for testing VIP and new property file
 *functions. This section will be removed from this file before Aug 15: MANA
 */
void print_container_info( qrep_ctnr_state_t *pcs ) {
    int i, num=0;
    char temp_str[1024];
    sprintf(temp_str,"No VIPS");
    if( pcs->num_vgrps >= 1 ) {
        for(  i = 0; i < pcs->num_vips[0]; i++ ) {
            num = num + sprintf( &(temp_str[num]),"%s %s %s %s;",pcs->vip_info[0][i].ip,
                    pcs->vip_info[0][i].mask, pcs->vip_info[0][i].gw, pcs->vip_info[0][i].name);
        }
    }
    plat_log_msg(21161, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
            "ID:%d name:%s cguid:%d flags:%d nreplicas:%d num_vgrps:%d numvips:%d vips:%s\n",
             pcs->id, pcs->name,(int)pcs->cguid, pcs->flags, 
             pcs->nreplicas, pcs->num_vgrps, pcs->num_vips[0], temp_str);
    

}
void print_node_info( qrep_node_state_t * pns ) {
    int i, num=0;
    char temp_str[2048];
    for(  i = 0; i < pns->num_ifs; i++ ) {
        num = num + sprintf( &(temp_str[num]),"%s ",pns->ifInfo[i].ip);
    }
    num = num + sprintf( &(temp_str[num]),"\nNum IPs in Vgrp: %d\n",pns->num_vips);
    for(  i = 0; i < pns->num_vips; i++ ) {
        num = num + sprintf( &(temp_str[num]),"ip:%s,Mask:%s if:%s gw:%s ",pns->vipgroup[i].ip,
                                           pns->vipgroup[i].mask,pns->vipgroup[i].name,pns->vipgroup[i].gw);
    }
    plat_log_msg(21162, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                "GroupId:%d\nhName:%s\nNumCnts:%d\nNumIfs:%d\nIfs:%s",
                 pns->group_id, pns->host_name, pns->nctnrs_node, pns->num_ifs,temp_str);
}

void print_group_info( qrep_cluster_grp_t * pgs ) {
    int i, num=0;
    char temp_str[128];

    for( i = 0; i < pgs->num_nodes; i++ ) {
        num = num + sprintf( &(temp_str[num]),"%d ",pgs->nodes[i]);
    }
    plat_log_msg(21163, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                "GroupId:%d\nType:%d\nNumNodes:%d\nNodes:%s",pgs->grp_id, pgs->type, pgs->num_nodes, temp_str);

}
void print_property_file( SDF_action_state_t *pas ) {
    int i;
/*
    qrep_node_state_t *pns;
    qrep_ctnr_state_t *pcs; */
    qrep_state_t      *ps;
    ps = &(pas->qrep_state);
    plat_log_msg(20823, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                     "Number Of Nodes:%d\nStatus Port:%d\nClusterId:%d\nClusterName:%s\nNumGroups:%d\n",
                     ps->nnodes, ps->status_port, ps->cluster_id, ps->cluster_name, ps->num_groups);
    for( i = 0; i <  ps->num_groups; i++ ) {
        print_group_info(&(ps->groups[i]));
    }
    for( i = 0; i < ps->nnodes; i++ ) {
        print_node_info( &(ps->node_state[i]));
    } 
}

void print_sdf_vip_group( sdf_vip_group_t *vgrp ) {
   int  k, index=0;
   char print_str[2048];

   index = index + sprintf(&(print_str[index]),"VipGroupId:%d\n",vgrp->vip_group_id);
   index = index + sprintf(&(print_str[index]),"VipGroupGroupId:%d\n",vgrp->group_group_id);
   index = index + sprintf(&(print_str[index]),"NumNodes:%d\n",vgrp->num_nodes);
   index = index + sprintf(&(print_str[index]),"vgid_start_index:%d\n",vgrp->vgid_start_index);
   for( k = 0; k < vgrp->num_nodes; k++ ) {
       index = index + sprintf(&(print_str[index]),"prefOrderforNode:%d:%d\n",k+vgrp->vgid_start_index, vgrp->nodes_pref_list[k]);
   }
   plat_log_msg(21164, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR, "VIPGROUP:%s",print_str);
}


void print_sdf_vip_group_group( sdf_vip_group_group_t *vggrp ) {
   int j, k, index=0;
   char print_str[4096];

   index = index + sprintf(&(print_str[index]),"TYPE:%d\n",vggrp->type);
   index = index + sprintf(&(print_str[index]),"grpgrpid:%d\n",vggrp->group_group_id);
   index = index + sprintf(&(print_str[index]),"MaxGrpPerNode:%d\n",vggrp->max_group_per_node);
   index = index + sprintf(&(print_str[index]),"NumVipGroups:%d\n",vggrp->num_groups);
   index = index + sprintf(&(print_str[index]),"vgid_start_index:%d\n",vggrp->vgid_start_index);
   for( j = 0; j < vggrp->num_groups; j++) {
       index = index + sprintf(&(print_str[index]),"VipGroup:%d\n",j);
       index = index + sprintf(&(print_str[index]),"VipGroupId:%d\n",vggrp->groups[j].vip_group_id);
       index = index + sprintf(&(print_str[index]),"VipGroupGroupId:%d\n",vggrp->groups[j].group_group_id);
       index = index + sprintf(&(print_str[index]),"NumNodes:%d\n",vggrp->groups[j].num_nodes);
       index = index + sprintf(&(print_str[index]),"vgid_start_index:%d\n",vggrp->groups[j].vgid_start_index);
       for( k = 0; k < vggrp->groups[j].num_nodes; k++ ) {
           index = index + sprintf(&(print_str[index]),"prefOrderforNode:%d:%d\n",k+vggrp->groups[j].vgid_start_index, vggrp->groups[j].nodes_pref_list[k]);

       }
   }
   plat_log_msg(21165, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR, "VIPGROUPGROUP:%s",print_str);
}

void print_sdf_vip_config( sdf_vip_config_t *vipcfg ) {
    int i,j,k, index=0;; 
    char print_str[4098];

    strcpy(print_str,"NO VIP CONFIG\n");
    if( vipcfg == NULL ) {
        plat_log_msg(21166, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR, "VIP CONFIG NULL:%s",print_str);
        return;
    }
    index = index + sprintf(&(print_str[index]),"\nnum_vip_group_groups:%d\n",vipcfg->num_vip_group_groups);
    for( i = 0; i < vipcfg->num_vip_group_groups; i++ ) {
        index = index + sprintf(&(print_str[index]),"vipgrpgrp:%d\n",i);
        index = index + sprintf(&(print_str[index]),"TYPE:%d\n",vipcfg->ggroups[i].type);
        index = index + sprintf(&(print_str[index]),"grpgrpid:%d\n",vipcfg->ggroups[i].group_group_id);
        //index = index + sprintf(&(print_str[index]),"MaxGrpPerNode:%d\n",vipcfg->ggroups[i].max_group_per_node);
        index = index + sprintf(&(print_str[index]),"NumVipGroups:%d\n",vipcfg->ggroups[i].num_groups);
        //index = index + sprintf(&(print_str[index]),"vgid_start_index:%d\n",vipcfg->ggroups[i].vgid_start_index);
        for( j = 0; j < vipcfg->ggroups[i].num_groups; j++) {
            //index = index + sprintf(&(print_str[index]),"VipGroup:%d\n",j);
            index = index + sprintf(&(print_str[index]),"VipGroupId:%d\n",vipcfg->ggroups[i].groups[j].vip_group_id);
            index = index + sprintf(&(print_str[index]),"VipGroupGroupId:%d\n",vipcfg->ggroups[i].groups[j].group_group_id);
            index = index + sprintf(&(print_str[index]),"NumNodes:%d\n",vipcfg->ggroups[i].groups[j].num_nodes);
            //index = index + sprintf(&(print_str[index]),"vgid_start_index:%d\n",vipcfg->ggroups[i].groups[j].vgid_start_index);
            index = index + sprintf(&(print_str[index]),"prefOrderforGroup:%d = ",vipcfg->ggroups[i].groups[j].vip_group_id);
            for( k = 0; k < vipcfg->ggroups[i].groups[j].num_nodes; k++ ) {
                index = index + sprintf(&(print_str[index]),"%d  ",vipcfg->ggroups[i].groups[j].nodes_pref_list[k]);
            }
            index = index + sprintf(&(print_str[index]),"\n");
        }
    }
    plat_log_msg(21167, PLAT_LOG_CAT_SDF_PROT_REPLICATION_VIPS, PLAT_LOG_LEVEL_TRACE, "VIP CONFIG:%s",print_str);
}
#endif


void build_sdf_vip_config(SDF_action_state_t *pas) { 
    int i,j,k,start_index;
    qrep_state_t      *ps;
    sdf_vip_config_t  *vipcfg;

    ps = &(pas->qrep_state);

    ps->vip_config = plat_alloc(sizeof(struct sdf_vip_config));
    if( ps->vip_config == NULL ) {
	plat_log_msg(20813, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "Unable to allocate mem for sdf_vip_config\n");
        plat_abort();
    }
    vip_grp_config = ps->vip_config;
    vipcfg = ps->vip_config;
    vipcfg->num_vip_groups = 0;
    vipcfg->num_vip_group_groups = ps->num_groups;
    vipcfg->ggroups = plat_alloc( ps->num_groups * sizeof(sdf_vip_group_group_t));
    if( vipcfg->ggroups == NULL ) {
	plat_log_msg(20814, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "Unable to allocate mem for vip_group groups\n");
        plat_abort();
    }
    for( i = 0; i < ps->num_groups; i++ ) {
        vipcfg->ggroups[i].group_group_id = ps->groups[i].grp_id;
        vipcfg->ggroups[i].type = ps->groups[i].type;
        if( vipcfg->ggroups[i].type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
          vipcfg->ggroups[i].max_group_per_node = 2;
          vipcfg->ggroups[i].num_groups =  ps->groups[i].num_nodes;
        }
        else {
          vipcfg->ggroups[i].max_group_per_node = 1;
          vipcfg->ggroups[i].num_groups =  ps->groups[i].num_nodes - 1;
        }
        vipcfg->ggroups[i].groups= plat_alloc( vipcfg->ggroups[i].num_groups * sizeof(sdf_vip_group_t));
        vipcfg->ggroups[i].vgid_start_index = ps->groups[i].nodes[0];
        
        if( vipcfg->ggroups[i].groups == NULL ) {
            plat_log_msg(20815, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "Unable to allocate mem for vip_groups for group:%d\n",i);
            plat_abort();
        }
        /*For each vip group in the group group, create a sdf_vip_group and order of preference*/
        for( j = 0; j < vipcfg->ggroups[i].num_groups; j++ ) {
            vipcfg->num_vip_groups++;
            vipcfg->ggroups[i].groups[j].vgid_start_index = vipcfg->ggroups[i].vgid_start_index;
            vipcfg->ggroups[i].groups[j].vip_group_id = ps->groups[i].nodes[j];
            vipcfg->ggroups[i].groups[j].group_group_id = i;
            vipcfg->ggroups[i].groups[j].num_nodes =  ps->groups[i].num_nodes;
            vipcfg->ggroups[i].groups[j].nodes_pref_list = plat_alloc( ps->groups[i].num_nodes * sizeof(vnode_t));
            if( vipcfg->ggroups[i].groups[j].nodes_pref_list == NULL ) {
                plat_log_msg(20816, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "Unable to allocate mem for pref List for vip group :%d\n",ps->groups[i].nodes[j]);
                plat_abort();
            }
            if( vipcfg->ggroups[i].type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
                vipcfg->ggroups[i].groups[j].nodes_pref_list[0] = ps->groups[i].nodes[j];
                if( j == 0 ) {
                    vipcfg->ggroups[i].groups[j].nodes_pref_list[1] = ps->groups[i].nodes[1];
                }
                else {
                    vipcfg->ggroups[i].groups[j].nodes_pref_list[1] = ps->groups[i].nodes[0];
                }
            }
            else {
               /*N+1: in N+1, 0th pref goes to the node which is statically configured to 
                *     host the ViP group, 1st pref goes to standby node.*/
               start_index = j;
               vipcfg->ggroups[i].groups[j].nodes_pref_list[0]=ps->groups[i].nodes[j];
               vipcfg->ggroups[i].groups[j].nodes_pref_list[1]=ps->groups[i].num_nodes-1;
               for( k = 2; k <  ps->groups[i].num_nodes; k++ ) {
                   start_index++;
                   start_index = (start_index)%(ps->groups[i].num_nodes-1);
                   vipcfg->ggroups[i].groups[j].nodes_pref_list[k] = ps->groups[i].nodes[start_index];
               }
            }
            
        }
    }
}

void simple_replication_init(SDF_action_state_t *pas)
{
    qrep_state_t      *ps;
    qrep_node_state_t *pns;


    SDFTLMap4Entry_t  *pe;
    SDF_boolean_t      bad = SDF_FALSE;
    /*
    const char        *s;
    int                dq1, dq2, dq3, dq4;
    int                count;
    char              *vip_mask; */
    char               prop_name[128];
    int                mynode;
    int                ndistinct;
    int                flag;
    int                nnode;
    int                i;

    extern int sdf_msg_numranks();

    ps = &(pas->qrep_state);
    mynode = sdf_msg_myrank();

    // global used to indicate if simple replication is on
    SDFSimpleReplication = 1; 

    ps->failback = pas->failback;

    /* load cluster state */
    sprintf( prop_name, "SDF_CLUSTER_NUMBER_NODES");
    ps->nnodes = getProperty_uLongInt(prop_name, 0);
    if( ps->nnodes == 0 ) {
          plat_log_msg(21168, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                    "SDF_CLUSTER_NUMBER_NODES > 0\n");
	  plat_abort();
    }

    /* check that ps->nnodes matches what the messaging system reports */
    if (0 && ps->nnodes != sdf_msg_numranks()) {
	plat_log_msg(21169, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                     "Number of nodes in property file (%d) is not the same as the number of nodes reported by the messaging system (%d)!", ps->nnodes, sdf_msg_numranks());
	/*plat_abort(); */
    }
    ps->node_state = plat_alloc(ps->nnodes*sizeof(qrep_node_state_t));
    if (ps->node_state == NULL) {
	plat_log_msg(21170, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "Could not allocate per-node replication state");
	plat_abort();
    }
    memset(ps->node_state, 0, ps->nnodes*sizeof(qrep_node_state_t));

    SDFTLMap4Init(&ps->ctnr_id_map, 1000, NULL);


    sprintf( prop_name, "SDF_CLUSTER_STATUS_PORT");
    ps->status_port = getProperty_uLongInt(prop_name, 14000);
   
    sprintf( prop_name, "SDF_CLUSTER_ID");
    ps->cluster_id = getProperty_uLongInt(prop_name, -1);
    if( ps->cluster_id  == -1 ) {
        plat_log_msg(21171, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "SDF_CLUSTER_ID should  be >= 0 ");
        bad = SDF_TRUE;    
    }

    sprintf( prop_name, "SDF_CLUSTER_NAME");
    memset(ps->cluster_name,0,sizeof(ps->cluster_name));
    strncpy(ps->cluster_name, (char *)getProperty_String(prop_name, ""), sizeof(ps->cluster_name)-1);
    if( strcmp(ps->cluster_name,"") == 0 ) {
        plat_log_msg(21172, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "SDF_CLUSTER_NAME should not be empty");
        bad = SDF_TRUE;    
    }
    sprintf( prop_name, "SDF_CLUSTER_NUMBER_OF_GROUPS");
    ps->num_groups = getProperty_uLongInt(prop_name, 0);
    if( ps->num_groups == 0 ) {
        plat_log_msg(21173, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "SDF_CLUSTER_NUMBER_OF_GROUPS must be > 0");
        bad = SDF_TRUE;    
    }

    ps->groups = plat_alloc(ps->num_groups*sizeof(qrep_cluster_grp_t));
    if (ps->groups == NULL) {
        plat_log_msg(21174, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "Could not allocate per group structure");
        plat_abort();
    }
    memset(ps->groups, 0, ps->num_groups*sizeof(qrep_cluster_grp_t));

    for( i = 0; i < ps->num_groups; i++ ) {
       char *grp_type;
       int j;
       sprintf( prop_name, "SDF_CLUSTER_GROUP[%d].TYPE",i);
       grp_type = (char *)getProperty_String(prop_name, "");
       if( ! strcmp(grp_type,"MIRRORED") ) {
            ps->groups[i].type = SDF_CLUSTER_GRP_TYPE_MIRRORED;
            ps->enable_ipf = 1;
       }
       else if( ! strcmp(grp_type,"N+1") ) {
            ps->groups[i].type = SDF_CLUSTER_GRP_TYPE_NPLUS1;
            ps->enable_ipf = 1;
       }
       else {
            ps->groups[i].type = SDF_CLUSTER_GRP_TYPE_INDEPENDENT;
       }

       sprintf( prop_name, "SDF_CLUSTER_GROUP[%d].NUM_NODES",i);
       ps->groups[i].num_nodes = getProperty_uLongInt(prop_name, 0);

       if( ((ps->groups[i].type == SDF_CLUSTER_GRP_TYPE_MIRRORED) || (ps->groups[i].type == SDF_CLUSTER_GRP_TYPE_NPLUS1) ) &&
            (ps->groups[i].num_nodes < 2  ) )  {
           plat_log_msg(21175, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "SDF_CLUSTER_GROUP[%d].NUM_NODES must be >= 2 for 2way or nplus1\n", i);
           bad = SDF_TRUE;    
       }

       sprintf( prop_name, "SDF_CLUSTER_GROUP[%d].GROUP_ID",i);
       ps->groups[i].grp_id = getProperty_uLongInt(prop_name, -1);
       if( ps->groups[i].grp_id == -1 ) {
          plat_log_msg(21176, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                    "SDF_CLUSTER_GROUP[%d].GROUP_ID must be >= 0\n", i);
          bad = SDF_TRUE;   
       }

       ps->groups[i].nodes = plat_alloc(ps->groups[i].num_nodes*sizeof(uint16_t));
       if (ps->groups == NULL) {
            plat_log_msg(21177, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "Could not allocate per group per node stcructure\n");
            plat_abort();
       }
       for( j = 0; j < ps->groups[i].num_nodes; j++ ) {
           sprintf( prop_name, "SDF_CLUSTER_GROUP[%d].NODE[%d]",i,j);
           ps->groups[i].nodes[j] = (uint16_t)getProperty_uLongInt(prop_name, -1);
           if( ps->groups[i].nodes[j] == -1 ) {
               plat_log_msg(21178, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                    "SDF_CLUSTER_GROUP[%d].NODE[%d] must be configured \n", i,j);
               bad = SDF_TRUE;   
           }
       }
    }

    ps->ndistinct_ctnrs = 0;
    plat_log_msg(160023, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION,PLAT_LOG_LEVEL_DEBUG,
                            "Instance iteration local tag %d:%u", msg_sdf_myrank(), (unsigned int)time(NULL));
    if( ( sdf_is_node_started_first_time() == SDF_TRUE ) || (sdf_is_node_started_in_auth_mode() == SDF_TRUE) ) {
        plat_log_msg(160020, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                         "Node becomes authoritative for persistent containers"); 
    }
    for (nnode = 0; nnode < ps->nnodes; nnode++) {
        int j;
        pns = &(ps->node_state[nnode]);
        pns->node_id = nnode;
        pns->num_vips=0;

        fthLockInit(&pns->lock);

        /*for each node, get the group ID, node name, Node IFs*/
	sprintf( prop_name, "NODE[%d].GROUP_ID", nnode);
	pns->group_id = getProperty_uLongInt(prop_name, -1);

#ifdef VIPGROUP_SUPPORT
       if( pns->group_id == -1 ) {
          plat_log_msg(21179, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                    "NODE[%d].GROUP_ID must be >= 0 for 2way or nplus1\n", i);
          bad = SDF_TRUE;   
        }
#endif
        
        /*Get Node Name*/
        sprintf( prop_name, "NODE[%d].NAME", nnode);
        memset(pns->host_name,0,sizeof(pns->host_name));
        strncpy(pns->host_name, getProperty_String(prop_name, ""), sizeof(pns->host_name)-1);
        if( strcmp(pns->host_name,"") == 0 ) {
           plat_log_msg(21180, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "NODE[%d].NAME must not be empty\n", nnode);
           bad = SDF_TRUE;    
        }

        /*Get the Number of IPs*/
        sprintf( prop_name, "NODE[%d].NUM_IFS", nnode);
        pns->num_vips = getProperty_uLongInt(prop_name, 0);
        for( i = 0; i < pns->num_vips ; i++ ) {
            sprintf( prop_name, "NODE[%d].IF[%d].VIP", nnode,i);
            memset( pns->vipgroup[i].ip,0,sizeof(pns->vipgroup[i].ip));
            strncpy(pns->vipgroup[i].ip,getProperty_String(prop_name, ""),sizeof(pns->vipgroup[i].ip)-1);

            sprintf( prop_name, "NODE[%d].IF[%d].MASK", nnode,i);
            memset( pns->vipgroup[i].mask,0,sizeof(pns->vipgroup[i].mask));
            strncpy(pns->vipgroup[i].mask,getProperty_String(prop_name, ""),sizeof(pns->vipgroup[i].mask)-1);

            sprintf( prop_name, "NODE[%d].IF[%d].GW", nnode,i);
            memset( pns->vipgroup[i].gw,0,sizeof(pns->vipgroup[i].gw));
            strncpy(pns->vipgroup[i].gw,getProperty_String(prop_name, ""),sizeof(pns->vipgroup[i].gw)-1);

            sprintf( prop_name, "NODE[%d].IF[%d].NAME", nnode,i);
            memset( pns->vipgroup[i].name,0,sizeof(pns->vipgroup[i].name));
            strncpy(pns->vipgroup[i].name,getProperty_String(prop_name, ""),sizeof(pns->vipgroup[i].name)-1);
        }

        /* If started first time, set the persistent auth flag = 1*/
        plat_log_msg(160021, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
              "IS NODE STARTED FIRST TIME: %d, Is node started in authoritative mode:%d\n",
                                      sdf_is_node_started_first_time(),sdf_is_node_started_in_auth_mode() );
        if(sdf_is_node_started_first_time() == SDF_TRUE) {
            pns->persistent_auth = 1;
        }
        if(sdf_is_node_started_in_auth_mode() == SDF_TRUE) {
            pns->persistent_auth = 1;
        }

        /* if Dynamic container is enabled, ignore the container section */
        sprintf( prop_name, "MEMCACHED_STATIC_CONTAINERS");
        if( getProperty_uLongInt(prop_name, 0) != 1 ) { 
            continue;
        }

	sprintf( prop_name, "MEMCACHED_NCONTAINERS[%d]", nnode);
	pns->nctnrs_node = getProperty_uLongInt(prop_name, 0);
        if( pns->nctnrs_node <= 0 ) {
           plat_log_msg(21182, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "MEMCACHED_NCONTAINERS[%d] must be > 0\n", nnode);
           bad = SDF_TRUE;    
        }

	pns->cntrs = plat_alloc(pns->nctnrs_node*sizeof(qrep_ctnr_state_t));
	if (pns->cntrs == NULL) {
	    plat_log_msg(21183, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
			 "Could not allocate per-node container state array");
	    plat_abort();
	}

	memset(pns->cntrs, 0, pns->nctnrs_node*sizeof(qrep_ctnr_state_t));

	for (i=0; i<pns->nctnrs_node; i++) {
	    sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].CONTAINER_ID", nnode, i);
	    pns->cntrs[i].id = getProperty_uLongInt(prop_name, -1);
	    pe = SDFTLMap4Get(&ps->ctnr_id_map, pns->cntrs[i].id);
	    if (pe == NULL) {
		pe = SDFTLMap4Create(&ps->ctnr_id_map, pns->cntrs[i].id);
		pe->contents = ps->ndistinct_ctnrs;
		(ps->ndistinct_ctnrs)++;
	    }
	    ndistinct = pe->contents;
	    pns->cntrs[i].global_index = ndistinct;

	    sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].CAPACITY_GB", nnode, i);
	    pns->cntrs[i].gb = getProperty_uLongInt(prop_name, -1);
	    sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].MAX_OBJECTS", nnode, i);
	    pns->cntrs[i].max_objs = getProperty_uLongLong(prop_name, -1);
	    sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].EVICTION", nnode, i);
	    flag = getProperty_uLongInt(prop_name, -1);
	    if (flag == 1) {
		pns->cntrs[i].flags |= qr_evict;
	    }
	    sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].PERSISTENT", nnode, i);
	    flag = getProperty_uLongInt(prop_name, -1);
	    if (flag == 1) {
		pns->cntrs[i].flags |= qr_persist;
	    }
	    sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].NREPLICAS", nnode, i);
	    pns->cntrs[i].nreplicas = getProperty_uLongInt(prop_name, -1);
            if( ps->groups[pns->group_id].type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
            if (pns->cntrs[i].nreplicas == 1) {
		sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].REPLICA[0].NODE", nnode, i);
		pns->cntrs[i].replica_node[0] = getProperty_Int(prop_name, -1);
		if ((pns->cntrs[i].replica_node[0] < 0) || 
		    (pns->cntrs[i].replica_node[0] >= ps->nnodes))
		{
		    plat_log_msg(21184, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: Invalid replica node (%d) for MEMCACHED_CONTAINER[%d][%d]", pns->cntrs[i].replica_node[0], nnode, i);
		    bad = SDF_TRUE;
		}
		pns->cntrs[i].replica_node[1] = -1;
	    } else if (pns->cntrs[i].nreplicas == 2) {
		sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].REPLICA[0].NODE", nnode, i);
		pns->cntrs[i].replica_node[0] = getProperty_Int(prop_name, -1);
		if ((pns->cntrs[i].replica_node[0] < 0) || 
		    (pns->cntrs[i].replica_node[0] >= ps->nnodes))
		{
		    plat_log_msg(21184, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: Invalid replica node (%d) for MEMCACHED_CONTAINER[%d][%d]", pns->cntrs[i].replica_node[0], nnode, i);
		    bad = SDF_TRUE;
		}
		sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].REPLICA[1].NODE", nnode, i);
		pns->cntrs[i].replica_node[1] = getProperty_Int(prop_name, -1);
		if ((pns->cntrs[i].replica_node[1] < 0) || 
		    (pns->cntrs[i].replica_node[1] >= ps->nnodes))
		{
		    plat_log_msg(21184, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: Invalid replica node (%d) for MEMCACHED_CONTAINER[%d][%d]", pns->cntrs[i].replica_node[1], nnode, i);
		    bad = SDF_TRUE;
		}
	    } else {
		plat_log_msg(21185, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
			     "Inconsistency in cluster configuration: NREPLICAS must be 1 or 2 for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		bad = SDF_TRUE;
	    }
            }
	    sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].STANDBY_NODE", nnode, i);
	    pns->cntrs[i].standby_node = getProperty_Int(prop_name, -1);
	    if (pns->cntrs[i].standby_node != -1) {
	        if ((pns->cntrs[i].standby_node < 0) || 
		    (pns->cntrs[i].standby_node >= ps->nnodes))
		{
		    plat_log_msg(21186, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: Invalid standby node (%d) for MEMCACHED_CONTAINER[%d][%d]", pns->cntrs[i].standby_node, nnode, i);
		    bad = SDF_TRUE;
		}
		if ((!(pns->cntrs[i].flags & qr_evict)) &&
		    (pns->cntrs[i].nreplicas != 2))
		{
		    plat_log_msg(21187, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: Unreplicated containers with a standby must allow eviction (MEMCACHED_CONTAINER[%d][%d])", nnode, i);
		    bad = SDF_TRUE;
		}
	    }

            /* MEMCACHED_CONTAINER[X][X].STANDBY_CONTAINER */
	    sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].STANDBY_CONTAINER", nnode, i);
	    pns->cntrs[i].standby_container = getProperty_Int(prop_name, -1);
	    if (pns->cntrs[i].standby_container != -1) {
	        if ((pns->cntrs[i].standby_container < 0) || 
		    (pns->cntrs[i].standby_container >= ps->nnodes))
		{
		    plat_log_msg(21188, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: Invalid standby container (%d) for MEMCACHED_CONTAINER[%d][%d]", pns->cntrs[i].standby_container, nnode, i);
		    bad = SDF_TRUE;
		}
		if ((!(pns->cntrs[i].flags & qr_evict)) &&
		    (pns->cntrs[i].nreplicas != 2))
		{
		    plat_log_msg(21189, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: Unreplicated containers with a standby must allow eviction (MEMCACHED_CONTAINER[%d][%d].STANDBY_CONTAINER)", nnode, i);
		    bad = SDF_TRUE;
		}
	        if ((pns->cntrs[i].standby_node < 0) || 
		    (pns->cntrs[i].standby_node >= ps->nnodes))
		{
		    plat_log_msg(21190, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: Standby containers must have a standby node (ie need to know what node the container is on) Invalid standby node (%d) for MEMCACHED_CONTAINER[%d][%d]", pns->cntrs[i].standby_node, nnode, i);
		    bad = SDF_TRUE;
		}
	    }
	    sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].IS_STANDBY", nnode, i);
	    flag = getProperty_uLongInt(prop_name, 0);
	    if (flag == 1) {
		pns->cntrs[i].flags |= qr_standby;
	    }

	   sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].CONTAINER_NAME", nnode, i);
           memset(pns->cntrs[i].name,0,sizeof(pns->cntrs[i].name));
           strncpy(pns->cntrs[i].name,getProperty_String(prop_name, ""),sizeof(pns->cntrs[i].name)-1);
           pns->cntrs[i].num_vgrps = 1;
	   sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].NUM_IFACES", nnode, i);
           pns->cntrs[i].num_vips[0] = getProperty_uLongInt(prop_name, 0);
           /*Check whether the node which is in part of 2way mirrored or nplus 1 group has NUM_IFACES > 0*/ 
           if( ((ps->groups[pns->group_id].type == SDF_CLUSTER_GRP_TYPE_MIRRORED) || 
                (ps->groups[pns->group_id].type == SDF_CLUSTER_GRP_TYPE_NPLUS1)) && 
                                    (!(pns->cntrs[i].flags & qr_standby)) && (pns->cntrs[i].num_vips[0] == 0 ) ){
               plat_log_msg(21191, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
               "Inconsistency in cluster configuration: MEMCACHED_CONTAINER[%d][%d].NUM_IFACES must be > 0 for 2way or n+1 group\n", nnode, i);
               bad = SDF_TRUE;
           }

   	   sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].TCP_PORT", nnode, i );
	   pns->cntrs[i].vip_tcp_port = getProperty_Int( prop_name, 11211 );
 	   sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].UDP_PORT", nnode, i );
	   pns->cntrs[i].vip_udp_port = getProperty_Int( prop_name, pns->cntrs[i].vip_tcp_port );
           /* If VIPs are specified at container level, then ignore the node level VIPs */
           if( pns->cntrs[i].num_vips[0] > 0 ) {
               pns->num_vips=0;
           }
           for( j = 0; j < pns->cntrs[i].num_vips[0] ; j++ ) {

               sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].IF[%d].VIP", nnode,i,j);    
               memset(pns->cntrs[i].vip_info[0][j].ip,0,sizeof(pns->cntrs[i].vip_info[0][j].ip));
               strncpy(pns->cntrs[i].vip_info[0][j].ip,getProperty_String(prop_name, ""), sizeof(pns->cntrs[i].vip_info[0][j].ip)-1);

               sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].IF[%d].MASK", nnode,i,j);    
               memset(pns->cntrs[i].vip_info[0][j].mask,0,sizeof(pns->cntrs[i].vip_info[0][j].mask));
               strncpy(pns->cntrs[i].vip_info[0][j].mask,getProperty_String(prop_name, ""), sizeof(pns->cntrs[i].vip_info[0][j].mask)-1);

               sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].IF[%d].GW", nnode,i,j);    
               memset(pns->cntrs[i].vip_info[0][j].gw,0,sizeof(pns->cntrs[i].vip_info[0][j].gw));
               strncpy(pns->cntrs[i].vip_info[0][j].gw,getProperty_String(prop_name, ""), sizeof(pns->cntrs[i].vip_info[0][j].gw)-1);

               sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].IF[%d].NAME", nnode,i,j);    
               memset(pns->cntrs[i].vip_info[0][j].name,0,sizeof(pns->cntrs[i].vip_info[0][j].name));
               strncpy(pns->cntrs[i].vip_info[0][j].name,getProperty_String(prop_name, ""), sizeof(pns->cntrs[i].vip_info[0][j].name)-1);
               if( (strcmp(pns->cntrs[i].vip_info[0][j].ip,"") == 0 ) ||
                   (strcmp(pns->cntrs[i].vip_info[0][j].mask,"") == 0 ) ||
                   (strcmp(pns->cntrs[i].vip_info[0][j].name,"") == 0 ) ) {

                   plat_log_msg(21192, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                           "Inconsistency in cluster configuration: for the node:%d cont:%d, ip(%s),mask(%s),ifname(:%s) should not be null\n", 
                              nnode, i, pns->cntrs[i].vip_info[0][j].ip, pns->cntrs[i].vip_info[0][j].mask,pns->cntrs[i].vip_info[0][j].name);
                   bad = SDF_TRUE;
               }

               if( ! (pns->cntrs[i].flags & qr_standby) ) {
                 /*Container is not standby. collect the vips of this container and 
                   add it to the node's vip group */ 
                   int vipindex;
                   int found = 0;
                   for( vipindex = 0; vipindex < pns->num_vips; vipindex++ ) {
                       if( ! strcmp(pns->vipgroup[vipindex].ip,pns->cntrs[i].vip_info[0][j].ip) ) {
                           found = 1;
                       }
                   }
                   if( found == 0 ) {
                       strcpy(pns->vipgroup[pns->num_vips].ip,pns->cntrs[i].vip_info[0][j].ip);
                       strcpy(pns->vipgroup[pns->num_vips].gw,pns->cntrs[i].vip_info[0][j].gw);
                       strcpy(pns->vipgroup[pns->num_vips].mask,pns->cntrs[i].vip_info[0][j].mask);
                       strcpy(pns->vipgroup[pns->num_vips].name,pns->cntrs[i].vip_info[0][j].name);
                       pns->num_vips++;
                   }
               }
           }
#if 0
            /* determine if VIP switchover is required */
	    sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].VIP", nnode, i);
	    s = getProperty_String(prop_name, "");
	    if (s[0] != '\0') {
		pns->cntrs[i].flags |= qr_vip;

		sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].VIP", nnode, i );
		pns->cntrs[i].vip_addr = (void *) getProperty_String( prop_name, 0 );

		sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].VIP_MASK", nnode, i );
		vip_mask = (void *) getProperty_String( prop_name, 0 );

		if (vip_mask == NULL || strlen(vip_mask) == 0) {
		    plat_log_msg(21193, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: No mask was given for vip %s (MEMCACHED_CONTAINER[%d][%d])", pns->cntrs[i].vip_addr, nnode, i);
		    bad = SDF_TRUE;
		}
		count = sscanf(vip_mask, "%d.%d.%d.%d", &dq1, &dq2, &dq3, &dq4);
		if (count != 4) {
		    plat_log_msg(21194, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "The vip mask \"%s\" is not valid for MEMCACHED_CONTAINER[%d][%d]", vip_mask, nnode, i);
		    bad = SDF_TRUE;
		}
		pns->cntrs[i].vip_mask = (dq1 << 24) | (dq2 << 16) | (dq3 << 8) | dq4;

		sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].VIP_TCP_PORT", nnode, i );
		pns->cntrs[i].vip_tcp_port = getProperty_Int( prop_name, 11211 );

		sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].VIP_UDP_PORT", nnode, i );
		pns->cntrs[i].vip_udp_port = getProperty_Int( prop_name, pns->cntrs[i].vip_tcp_port );

		sprintf( prop_name, "MEMCACHED_CONTAINER[%d][%d].VIP_IF", nnode, i );
		strncpy(pns->cntrs[i].vip_if_id, getProperty_String( prop_name, "eth2" ),
                        sizeof(pns->cntrs[i].vip_if_id));
	    }
#endif
            pns->cntrs[i].node_id = pns->node_id;
            pns->cntrs[i].group_id = pns->group_id;
            pns->cntrs[i].cguid = get_container_guid(&(pns->cntrs[i]));
            pns->cntrs[i].replicating_to_node = SDF_ILLEGAL_VNODE;
	    plat_log_msg(30555, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_TRACE,
		     "replicating_to_node = SDF_ILLEGAL_VNODE");
	}
    }
    /* if Dynamic container is enabled, ignore the container section */
    sprintf( prop_name, "MEMCACHED_STATIC_CONTAINERS");
    if( getProperty_uLongInt(prop_name, 0) == 1 ) {
        ps->ctnr_state = plat_alloc(ps->ndistinct_ctnrs*sizeof(qrep_ctnr_state_t));
        if (ps->ctnr_state == NULL) {
    	    plat_log_msg(21195, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "Could not allocate per-container replication state");
	    plat_abort();
        }
        memset(ps->ctnr_state, 0, ps->ndistinct_ctnrs*sizeof(qrep_ctnr_state_t));

        /*  Check consistency of containers and their spares. 
         */

        for (nnode = 0; nnode < ps->nnodes; nnode++) {
            pns = &(ps->node_state[nnode]);
            pns->live = 0;
	    for (i=0; i<pns->nctnrs_node; i++) {
	        pe = SDFTLMap4Get(&ps->ctnr_id_map, pns->cntrs[i].id);
	        if (pe == NULL) {
	    	    plat_log_msg(21196, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
	    		     "Fatal inconsistency in cluster configuration");
		    plat_abort();
	        }
	        ndistinct = pe->contents;
	        if (ps->ctnr_state[ndistinct].flags & qr_loaded) {
	            /* check consistency */

                    if (ps->ctnr_state[ndistinct].id != pns->cntrs[i].id) {
		        plat_log_msg(21197, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
		    		 "Inconsistency in cluster configuration: mismatched container ID's for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		        bad = SDF_TRUE;
		    }
                    if (ps->ctnr_state[ndistinct].global_index != pns->cntrs[i].global_index) {
    		        plat_log_msg(21198, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: mismatched global indices for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		        bad = SDF_TRUE;
		    }
                    if (ps->ctnr_state[ndistinct].gb != pns->cntrs[i].gb) {
		        plat_log_msg(21199, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: mismatched GB's for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		        bad = SDF_TRUE;
		    }
                    if (ps->ctnr_state[ndistinct].max_objs != pns->cntrs[i].max_objs) {
		        plat_log_msg(21200, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: mismatched MAX_OBJS's for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		        bad = SDF_TRUE;
	    	    }
                    /*
                    if (ps->ctnr_state[ndistinct].standby_node != pns->cntrs[i].standby_node) {
		        plat_log_msg(21201, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: mismatched STANDBY_NODE's for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
    		        bad = SDF_TRUE;
		    }
                    */
                    if( ps->groups[pns->group_id].type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
                        if (ps->ctnr_state[ndistinct].nreplicas != pns->cntrs[i].nreplicas) {
		            plat_log_msg(21202, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: mismatched NREPLICAS for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		            bad = SDF_TRUE;
		        }

		        if (ps->ctnr_state[ndistinct].nreplicas == 1) {
		            if (ps->ctnr_state[ndistinct].nreplicas != pns->cntrs[i].nreplicas) {
			        plat_log_msg(21203, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				     "Inconsistency in cluster configuration: unreplicated container on more than one node for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
			        bad = SDF_TRUE;
		            }
		        } else if (ps->ctnr_state[ndistinct].nreplicas == 2) {
		            if (ps->ctnr_state[ndistinct].replica_node[0] == pns->cntrs[i].replica_node[0]) {
			        if (ps->ctnr_state[ndistinct].replica_node[1] != pns->cntrs[i].replica_node[1]) {
			            plat_log_msg(21204, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
					 "Inconsistency in cluster configuration: mismached REPLICA_NODE's for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
			            bad = SDF_TRUE;
			        }
		            } else if (ps->ctnr_state[ndistinct].replica_node[0] == pns->cntrs[i].replica_node[1]) {
			        if (ps->ctnr_state[ndistinct].replica_node[1] != pns->cntrs[i].replica_node[0]) {
			            plat_log_msg(21204, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
					 "Inconsistency in cluster configuration: mismached REPLICA_NODE's for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
			            bad = SDF_TRUE;
			        }
		            } else {
			        plat_log_msg(21204, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				     "Inconsistency in cluster configuration: mismached REPLICA_NODE's for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
			        bad = SDF_TRUE;
		            }
		        } else {
		            plat_log_msg(21205, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: invalid NREPLICAS for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		            bad = SDF_TRUE;
		        }
                    }

                    if ((ps->ctnr_state[ndistinct].flags & qr_evict)!= (pns->cntrs[i].flags & qr_evict)) {
		        plat_log_msg(21206, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: EVICTION mode conflict for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		        bad = SDF_TRUE;
		    }
                    if ((ps->ctnr_state[ndistinct].flags & qr_persist)!= (pns->cntrs[i].flags & qr_persist)) {
		        plat_log_msg(21207, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: PERSISTENT mode conflict for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		        bad = SDF_TRUE;
		    }
                    /*
                    if ((ps->ctnr_state[ndistinct].flags & qr_standby) != (pns->cntrs[i].flags & qr_standby)) {
		        plat_log_msg(21208, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: an IS_STANDBY container can only reside on a single node (MEMCACHED_CONTAINER[%d][%d])", nnode, i);
		        bad = SDF_TRUE;
	    	    }
                    */
                    if ((ps->ctnr_state[ndistinct].flags & qr_vip)!= (pns->cntrs[i].flags & qr_vip)) {
		        plat_log_msg(21209, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: VIP mode conflict for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		        bad = SDF_TRUE;
		    }

                #ifdef notdef
		    // are these checks necessary?  xxxzzz
                    if (strcmp(ps->ctnr_state[ndistinct].vip_addr, pns->cntrs[i].vip_addr) != 0) {
		        plat_log_msg(21210, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: mismatched VIP's for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		        bad = SDF_TRUE;
	    	    }
                    if (ps->ctnr_state[ndistinct].vip_mask != pns->cntrs[i].vip_mask) {
		        plat_log_msg(21211, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: mismatched container VIP_MASK's for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		        bad = SDF_TRUE;
		    }
                    if (ps->ctnr_state[ndistinct].vip_tcp_port != pns->cntrs[i].vip_tcp_port) {
		        plat_log_msg(21212, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: mismatched container VIP_TCP_PORT's for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		        bad = SDF_TRUE;
		    }
                    if (ps->ctnr_state[ndistinct].vip_udp_port != pns->cntrs[i].vip_udp_port) {
		        plat_log_msg(21213, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: mismatched container VIP_UDP_PORT's for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		        bad = SDF_TRUE;
		    }
                    if (ps->ctnr_state[ndistinct].vip_if_id != pns->cntrs[i].vip_if_id) {
		        plat_log_msg(21214, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
				 "Inconsistency in cluster configuration: mismatched container VIP_IF's for MEMCACHED_CONTAINER[%d][%d]", nnode, i);
		        bad = SDF_TRUE;
	    	    }
		    #endif

	        } else {
	            memcpy(&(ps->ctnr_state[ndistinct]), &(pns->cntrs[i]), sizeof(qrep_ctnr_state_t));
		    ps->ctnr_state[ndistinct].flags |= qr_loaded;
	        }
	    }
        }
    }
    if (bad) {
	plat_abort();
    }

    /* If MULTI_INSTANCE_MCD support is enabled, then mark the 
       second node of each mirrored group to Virtual */
    sprintf( prop_name, "MULTI_INSTANCE_MCD");
    if( getProperty_uLongInt(prop_name, 0) == 1 ) {
        int j;
        for( i = 0; i <  ps->num_groups; i++ ) {
            if( ps->groups[i].type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
                int second_node = ps->groups[i].nodes[1];
                ps->node_state[second_node].is_virtual = 1;
                plat_log_msg(21215, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                                               "Setting Node: %d as Virtual",second_node);
                /* Change the ports of all containers to something else */
               for( j = 0; j < ps->node_state[second_node].nctnrs_node; j++ ) {
                   ps->node_state[second_node].cntrs[j].vip_tcp_port += MULTI_INST_MCD_VPORT_OFFSET;
                   ps->node_state[second_node].cntrs[j].vip_udp_port += MULTI_INST_MCD_VPORT_OFFSET;
               }
            }
        }
    }

    /* setup VIP */
    build_sdf_vip_config(pas);
    print_sdf_vip_config(ps->vip_config);
    

    /* prepare to receive liveness events */
    msg_livecall(1, 1, live_back, pas);

    // Number of threads used to sync a remote container
    ps->nctnr_sync_threads = getProperty_uLongInt("SDF_N_SYNC_CONTAINER_THREADS", DEFAULT_NUM_SYNC_CONTAINER_THREADS);

    cursor_datas = (struct cursor_data *) plat_alloc(sizeof(struct cursor_data) * ps->nctnr_sync_threads);
    plat_assert(cursor_datas);

    /* Enable replication only if the node is in 2way group */
    mynode = sdf_msg_myrank();
    pns = &(ps->node_state[mynode]);
    if( ps->groups[pns->group_id].type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
        /* in 2Way Mirrored cluster group, the Node goes to Recovering Mode when it starts*/
        pns->type = QREP_NODE_TYPE_RECOVERING;    

        /*  Determine if I am replacing a dead node.
         *  For all replicating containers without a standby, 
         *  do a handshake with the survivor to resume replication and
         *  to transfer the current contents.
         *  For replicating containers with a standby, 
         *  and for non-replicating containers, I now become a standby.
         */
        sdf_create_queue_pair(mynode, VNODE_ANY, SDF_RESPONSES, SDF_FLSH, SDF_WAIT_FTH);
        fthMboxInit(&ps->sync_thread_avail_mbox);
        fthMboxInit(&rec_thread_avail_mbox);
        fthLockInit(&sync_remote_container_lock);

        for (i = 0; i < ps->nctnr_sync_threads; i++) {
            if (NEW_RECOVERY_CODE) {
	        fthResume(fthSpawn(&sync_container_thread_new, 40960), (uint64_t) pas);
	    } else {
	        fthResume(fthSpawn(&sync_container_thread, 40960), (uint64_t) pas);
	    }
        }
        fthResume(fthSpawn(&data_recovery_thread, 40960), (uint64_t) 0);
    }
    else if( ps->groups[pns->group_id].type == SDF_CLUSTER_GRP_TYPE_NPLUS1 ) {
        /* in N+1, The Node goes to Standby by default when itstarts*/
        pns->type = QREP_NODE_TYPE_STANDBY;    
    }
    else {
        pns->type = QREP_NODE_TYPE_ACTIVE;
        sprintf( prop_name, "MEMCACHED_INDEP_CLONE");
        /* In All other cases the Node goes to Active by default when itstarts*/
        /* Independent node */
        if( (sdf_is_node_started_first_time() == SDF_TRUE) && (getProperty_Int( prop_name, 0 ) == 1)) {
            plat_log_msg(21216, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                                       "Starting in CLONE mode \n");
            sdf_create_queue_pair(mynode, VNODE_ANY, SDF_RESPONSES, SDF_FLSH, SDF_WAIT_FTH);
            fthMboxInit(&ps->sync_thread_avail_mbox);
            fthMboxInit(&rec_thread_avail_mbox);
            fthLockInit(&sync_remote_container_lock);

            for (i = 0; i < ps->nctnr_sync_threads; i++) {
                if (NEW_RECOVERY_CODE) {
                    fthResume(fthSpawn(&sync_container_thread_new, 40960), (uint64_t) pas);
                } else {
                    fthResume(fthSpawn(&sync_container_thread, 40960), (uint64_t) pas);
                }
            }
            fthResume(fthSpawn(&data_recovery_thread, 40960), (uint64_t) 0);
            pns->type = QREP_NODE_TYPE_CLONE;
            pns->rec_count++; /* This flag avoids creation of these threads later when this node clondes to other nodes*/
        }
    }
}

int SDFIsNodeInCloneMode() {
    char prop_name[256];
    sprintf( prop_name, "MEMCACHED_INDEP_CLONE");    
    if( (sdf_is_node_started_first_time() == SDF_TRUE) && (getProperty_Int( prop_name, 0 ) == 1)) {
        return 1;
    }
    return 0;
}

void SDFRepDataStructAddContainer( SDF_internal_ctxt_t *pai , SDF_container_props_t properties, SDF_cguid_t cguid) {
    char prop_name[256];
    int mynode, peer_node_id = -1, num_cntrs=0, rc;
    struct SDF_action_state *pas = ((SDF_action_init_t *)pai)->pcs;
    qrep_state_t *rep_state = &(pas->qrep_state);
    qrep_node_state_t *lnode; /* Local Node */
    qrep_node_state_t *pnode; /* peer Node */
    qrep_ctnr_state_t  *new_cntrs_list, *peer_cntrs_list;
    fthWaitEl_t *wait;

    if( properties.container_id.container_id  == 0 ) {
        /* CMC container. Do not need to add */
        return;
    }
    /* Ignore if dynamic container is not enabled */
    sprintf( prop_name, "MEMCACHED_STATIC_CONTAINERS");
    if( getProperty_uLongInt(prop_name, 0) == 1 ) {
        return;
    }
    /* Ignore if simple replication is turned off */
    sprintf( prop_name, "SDF_SIMPLE_REPLICATION");
    if( strcmp( getProperty_String( "SDF_SIMPLE_REPLICATION", "Off" ),"Off") == 0 ) {
        return;
    }
    mynode = msg_sdf_myrank();
    /*Update the container in the local node*/
    lnode = &(rep_state->node_state[mynode]);
    /* Lock the node here */
    wait = fthLock( &lnode->lock, 1, NULL);
    new_cntrs_list = plat_alloc( (lnode->nctnrs_node + 1)*sizeof(qrep_ctnr_state_t));
    if (new_cntrs_list == NULL) {
        plat_log_msg(21183, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                       "Could not allocate per-node container state array");
        plat_abort();
    }

    memset(new_cntrs_list, 0, (lnode->nctnrs_node + 1)*sizeof(qrep_ctnr_state_t));

    if( lnode->nctnrs_node > 0 ) {
        memcpy(new_cntrs_list,lnode->cntrs,lnode->nctnrs_node*sizeof(qrep_ctnr_state_t));
        plat_free(lnode->cntrs);
    }
    new_cntrs_list[lnode->nctnrs_node].cguid = cguid;
    /* Fill the new container information */
    new_cntrs_list[lnode->nctnrs_node].id =  properties.container_id.container_id;
    new_cntrs_list[lnode->nctnrs_node].gb = properties.container_id.size;
    new_cntrs_list[lnode->nctnrs_node].max_objs = properties.container_id.sc_num_objs;
    strcpy(new_cntrs_list[lnode->nctnrs_node].name,"");
    new_cntrs_list[lnode->nctnrs_node].num_vgrps = 0;
    new_cntrs_list[lnode->nctnrs_node].nreplicas = 0;
    new_cntrs_list[lnode->nctnrs_node].replicating_to_node = SDF_ILLEGAL_VNODE;
    /* new_cntrs_list[lnode->nctnrs_node].flags |= qr_evict; */
    if( properties.container_type.persistence == SDF_TRUE ) {
        new_cntrs_list[lnode->nctnrs_node].flags |= qr_persist;
    }
    lnode->nctnrs_node++;
    num_cntrs = lnode->nctnrs_node;
    lnode->cntrs = new_cntrs_list;
    /* Create Replica Node locally. This is required for laptop configuration */
    if ( rep_state->groups[lnode->group_id].type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
        peer_node_id=rep_state->groups[lnode->group_id].nodes[0];
        if( rep_state->groups[lnode->group_id].nodes[0] == mynode ){
            peer_node_id=rep_state->groups[lnode->group_id].nodes[1];
        }
        new_cntrs_list[lnode->nctnrs_node - 1].nreplicas = 2;
        new_cntrs_list[lnode->nctnrs_node - 1].replica_node[0] = mynode;
        new_cntrs_list[lnode->nctnrs_node - 1].replica_node[1] = peer_node_id;
        peer_cntrs_list = plat_alloc(num_cntrs * sizeof(qrep_ctnr_state_t));
        if (peer_cntrs_list == NULL) {
            plat_log_msg(21183, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                       "Could not allocate per-node container state array");
            plat_abort();
        }
        memcpy(peer_cntrs_list,new_cntrs_list, num_cntrs*sizeof(qrep_ctnr_state_t));
        /* Release the local node lock */
        fthUnlock(wait);
        pnode = &(rep_state->node_state[peer_node_id]);
        /* Lock the peer Node */
        wait = fthLock( &pnode->lock, 1, NULL);
        if( pnode->nctnrs_node > 0 ) {
            plat_free(pnode->cntrs);
        }
        pnode->cntrs = peer_cntrs_list;
        pnode->nctnrs_node = num_cntrs;
        /*Release the peer node lock */
        fthUnlock(wait);
    }
    else {
        /* release the node lock */
        fthUnlock(wait);
    }
    plat_log_msg(21217, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                        "Added a container with container id: %d (cguid:%d)to RepStruture peer:%d\n", 
                         (int)properties.container_id.container_id, (int)cguid, peer_node_id);

    if( peer_node_id >= 0 ) {
        /*Enable the replication only if we have one VIP group*/
        wait = fthLock( &lnode->lock, 1, NULL);
        if( lnode->num_vgrps_being_serviced == 1 ) {
            plat_log_msg(21218, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                         "Enable replication to node %d\n",peer_node_id);
            rc = simple_replicator_enable_node(pas, cguid, peer_node_id);
            if (rc != SDF_SUCCESS) {
                plat_log_msg(21219, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                         "Could not start replication of container %d to node %d\n",
                                                (int)properties.container_id.container_id,peer_node_id);
            }
        }
        fthUnlock(wait);
    }
}

void check_for_authoritativeness(qrep_state_t *rep_state) {
    int mynode,i=0;
    qrep_node_state_t *lnode;
    fthWaitEl_t *wait;

    mynode = msg_sdf_myrank();
    /*Update the container in the local node*/
    lnode = &(rep_state->node_state[mynode]);
    wait = fthLock( &lnode->lock, 1, NULL);
    for( i = 0; i < lnode->nctnrs_node; i++ ) {
        if( lnode->cntrs[i].flags & qr_persist ) {
            break;
        }
    }
    if( i >= lnode->nctnrs_node ) {
        fthUnlock(wait);
        plat_log_msg(160025, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                                                     "Node %d does not have any persistent container\n",mynode);
        plat_log_msg(160026, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                                                     "Node becomes authoritative for persistent containers\n");
    }
    else {
        fthUnlock(wait);
    }
}

void SDFRepDataStructDeleteContainer( SDF_internal_ctxt_t *pai , SDF_cguid_t cguid) {
    char prop_name[256];
    int mynode, peer_node_id=-1, num_cntrs=0, i,j;
    struct SDF_action_state *pas = ((SDF_action_init_t *)pai)->pcs;
    qrep_state_t *rep_state = &(pas->qrep_state);
    qrep_node_state_t *lnode; /* Local Node */
    qrep_node_state_t *pnode; /* peer Node */
    qrep_ctnr_state_t  *new_cntrs_list, *peer_cntrs_list;
    fthWaitEl_t *wait;

    /* Ignore if dynamic container is not enabled */
    sprintf( prop_name, "MEMCACHED_STATIC_CONTAINERS");
    if( getProperty_uLongInt(prop_name, 0) == 1 ) {
        return;
    }
    /* Ignore if simple replication is turned off */
    sprintf( prop_name, "SDF_SIMPLE_REPLICATION");
    if( strcmp( getProperty_String( "SDF_SIMPLE_REPLICATION", "Off" ),"Off") == 0 ) {
        return;
    }

    mynode = msg_sdf_myrank();
    /*Update the container in the local node*/
    lnode = &(rep_state->node_state[mynode]);
    wait = fthLock( &lnode->lock, 1, NULL);
    if( lnode->nctnrs_node <= 0 ) {
        plat_log_msg(21220, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                                                     "Node %d does not have any container\n",mynode);
        fthUnlock(wait);
        return;
    }
    new_cntrs_list = NULL; 
    if( lnode->nctnrs_node > 1 ) { 
        new_cntrs_list = plat_alloc( (lnode->nctnrs_node - 1)*sizeof(qrep_ctnr_state_t));
        if (new_cntrs_list == NULL) {
            plat_log_msg(21183, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                       "Could not allocate per-node container state array");
            plat_abort();
        }
    }
   
    j = 0;
    for( i = 0; i < lnode->nctnrs_node; i++ ) {
        if( lnode->cntrs[i].cguid != cguid ) {
            if( j >= (lnode->nctnrs_node -1) ) {
                /* Trying to delete a container which is not in list */
                plat_log_msg(21221, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                       "container %d not found in list\n",(int)cguid);
                /* free lock here */
                plat_free(new_cntrs_list);
                fthUnlock(wait);
                return;
            }
            memcpy( &(new_cntrs_list[j]), &(lnode->cntrs[i]), sizeof(qrep_ctnr_state_t));
            j++;
        }
    }
    if ( rep_state->groups[lnode->group_id].type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
        peer_node_id=rep_state->groups[lnode->group_id].nodes[0];
        if( rep_state->groups[lnode->group_id].nodes[0] == mynode ){
            peer_node_id=rep_state->groups[lnode->group_id].nodes[1];
        }
    }
    /* Unlock first before disabling the replicatoon */
    fthUnlock(wait);
    if( peer_node_id >= 0 ) { 
        simple_replicator_disable_replication(pas,cguid,peer_node_id);
    }
    /* lock again to clear things...*/
    wait = fthLock( &lnode->lock, 1, NULL);
    plat_free(lnode->cntrs);
    lnode->cntrs=new_cntrs_list;
    lnode->nctnrs_node--;
    num_cntrs = lnode->nctnrs_node;
    
    /* Delete this in the peer node. This is required for laptop configuration */
    if ( rep_state->groups[lnode->group_id].type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
        peer_node_id=rep_state->groups[lnode->group_id].nodes[0];
        if( rep_state->groups[lnode->group_id].nodes[0] == mynode ){
            peer_node_id=rep_state->groups[lnode->group_id].nodes[1];
        }
        peer_cntrs_list=NULL;
        if( num_cntrs > 0 ) {
            peer_cntrs_list = plat_alloc(num_cntrs * sizeof(qrep_ctnr_state_t));
            if (peer_cntrs_list == NULL) {
                plat_log_msg(21183, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                       "Could not allocate per-node container state array");
                plat_abort();
            }
            memcpy(peer_cntrs_list,new_cntrs_list, num_cntrs*sizeof(qrep_ctnr_state_t));
        }
        /* Release the local node lock */
        fthUnlock(wait);
        pnode = &(rep_state->node_state[peer_node_id]);
        /* Lock the peer Node */
        wait = fthLock( &pnode->lock, 1, NULL);
        if( pnode->nctnrs_node > 0 ) {
            plat_free(pnode->cntrs);
        }
        pnode->cntrs = peer_cntrs_list;
        pnode->nctnrs_node = num_cntrs;
        /*Release the peer node lock */
        fthUnlock(wait);
    }
    else {
        /* release the node lock */
        fthUnlock(wait);
    }
    check_for_authoritativeness(rep_state);
}

int SDFNodeGroupGroupType( SDF_internal_ctxt_t *pai, int node_id ) {
    struct SDF_action_state *pas = ((SDF_action_init_t *)pai)->pcs;
    qrep_state_t *rep_state = &(pas->qrep_state);
    qrep_node_state_t *node;
    qrep_cluster_grp_t *grp;
    int grp_type = SDF_REPLICATION_NONE;

    if( node_id >= rep_state->nnodes ) {
        plat_log_msg(21222, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "Invalid Node Id:%d NumNodes:%d\n",node_id,rep_state->nnodes);
        plat_assert( node_id < rep_state->nnodes );
    }
    node = &(rep_state->node_state[node_id]);
    grp = &(rep_state->groups[node->group_id]);

    if( grp->type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
        grp_type = SDF_REPLICATION_V1_2_WAY;
    }
    else if( grp->type == SDF_CLUSTER_GRP_TYPE_NPLUS1 ) {
        grp_type = SDF_REPLICATION_V1_N_PLUS_1;
    }
    else if( grp->type == SDF_CLUSTER_GRP_TYPE_SIMPLE_REPLICATION ) {
        grp_type = SDF_REPLICATION_SIMPLE;
    }
    return grp_type;
}

int SDFMyGroupGroupType( SDF_internal_ctxt_t *pai) {
    return SDFNodeGroupGroupType(pai,msg_sdf_myrank());
}

int SDFGetPeerNodeId(int mynode) {
     char prop_name[128];
     int grpgrpid, node0, node1;

     if(SDFNodeGroupGroupTypeFromConfig(mynode) != SDF_REPLICATION_V1_2_WAY){
         return -1;
     }
     grpgrpid = SDFNodeGroupGroupIdFromConfig(mynode);
     sprintf( prop_name, "SDF_CLUSTER_GROUP[%d].NODE[0]",grpgrpid);
     node0 = getProperty_uLongInt(prop_name, -1);
     if( node0 == -1 ) {
         return -1;
     }

     sprintf( prop_name, "SDF_CLUSTER_GROUP[%d].NODE[1]",grpgrpid);
     node1 = getProperty_uLongInt(prop_name, -1);
     if( node1 == -1 ) {
         return -1;
     }

     if ( mynode == node0 ) {
         return node1;
     }
     else {
         return node0;
     }
}

SDF_boolean_t SDFIsMyNodeHasHigherNodeId() {
    int my_node, peer_node;
    my_node  = msg_sdf_myrank();
    peer_node = SDFGetPeerNodeId(my_node);
    if( peer_node < 0 ) {
        return SDF_FALSE;
    }
    if( my_node > peer_node ) {
        return SDF_TRUE;
    }
    return SDF_FALSE;
}

int SDFNodeGroupGroupTypeFromConfig(int node_id) {
    int  group_id;
    char *grp_type, prop_name[128];

    if( SDFSimpleReplication != 1 ) {
        return  SDF_REPLICATION_NONE;    
    }

    sprintf( prop_name, "NODE[%d].GROUP_ID", node_id);
    group_id = getProperty_uLongInt(prop_name, -1);
    plat_assert(group_id >= 0 );

    sprintf( prop_name, "SDF_CLUSTER_GROUP[%d].TYPE",group_id);
    grp_type = (char *)getProperty_String(prop_name, "");
    plat_log_msg(21223, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION,PLAT_LOG_LEVEL_DEBUG,
                       "Group Type:%s for node:%d\n",grp_type,node_id);
    plat_assert( strcmp(grp_type,"") != 0 );

    if( strcmp(grp_type,"MIRRORED") == 0) {
        return SDF_REPLICATION_V1_2_WAY;
    }
    else if( strcmp(grp_type,"N+1") == 0) {
        return SDF_REPLICATION_V1_N_PLUS_1;
    }
    if( strcmp(grp_type,"SIMPLE") == 0) {
        return SDF_REPLICATION_SIMPLE;
    }
    else {
       return SDF_REPLICATION_NONE;
    }
}

int SDFMyGroupGroupTypeFromConfig() {
    return SDFNodeGroupGroupTypeFromConfig(msg_sdf_myrank());
}

int SDFNodeGroupGroupId( SDF_internal_ctxt_t *pai, int node_id ) {
    struct SDF_action_state *pas = ((SDF_action_init_t *)pai)->pcs;
    qrep_state_t *rep_state = &(pas->qrep_state);
    qrep_node_state_t *node;
    if( node_id >= rep_state->nnodes ) {
        plat_log_msg(21222, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "Invalid Node Id:%d NumNodes:%d\n",node_id,rep_state->nnodes);
        plat_assert( node_id < rep_state->nnodes );
    }
    node = &(rep_state->node_state[node_id]);
    return node->group_id;
}

int SDFNodeGroupGroupIdFromConfig(int node_id) {
    int  group_id;
    char prop_name[128];

    sprintf( prop_name, "NODE[%d].GROUP_ID", node_id);
    group_id = getProperty_uLongInt(prop_name, -1);
    plat_assert(group_id >= 0 );
    plat_log_msg(21224, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION,PLAT_LOG_LEVEL_DEBUG,
                       "Group ID:%d for node:%d\n",group_id,node_id);
    return group_id;
}

int SDFMyGroupGroupIdFromConfig() {
    return SDFNodeGroupGroupIdFromConfig(msg_sdf_myrank());
}

void SDFSetContainerStatus(SDF_internal_ctxt_t *pai, int cid, int status ) {
    struct SDF_action_state *pas = ((SDF_action_init_t *)pai)->pcs;
    qrep_state_t *rep_state = &(pas->qrep_state);
    qrep_node_state_t *pns;
    qrep_ctnr_state_t * pcs;
    int node_id,grp_id, peer_node_id;
    fthWaitEl_t * wait;

    node_id = msg_sdf_myrank();
    pns = &(rep_state->node_state[node_id]);
    wait = fthLock(&pns->lock, 1, NULL);
    grp_id = pns->group_id;
    for( int i = 0; i < pns->nctnrs_node; i++ ) {
        pcs = &(pns->cntrs[i]);
        if( pcs->id != cid ) {
            continue;
        }
        plat_log_msg(21225, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION,
                                   PLAT_LOG_LEVEL_DEBUG,
                       "Setting container status %d to cid :%d\n",status,cid);
        pcs->ctnr_state = QREP_CTNR_STATE_STOPPED;
        if( status == 1 ) {
            pcs->ctnr_state = QREP_CTNR_STATE_ACTIVE;
        }
    }
    fthUnlock(wait);
    if (rep_state->groups[grp_id].type != SDF_CLUSTER_GRP_TYPE_MIRRORED) {
        return;
    }
    /* Update the status in the replica also */
    peer_node_id=rep_state->groups[grp_id].nodes[0];
    if( rep_state->groups[grp_id].nodes[0] == node_id ){
        peer_node_id=rep_state->groups[grp_id].nodes[1];
    }
    pns = &(rep_state->node_state[peer_node_id]);
    wait = fthLock(&pns->lock, 1, NULL);
    for( int i = 0; i < pns->nctnrs_node; i++ ) {
        pcs = &(pns->cntrs[i]);
        if( pcs->id != cid ) {
            continue;
        }
        plat_log_msg(21226, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION,
              PLAT_LOG_LEVEL_DEBUG,
             "Setting container status %d to cid :%d on replica\n",status,cid);
        pcs->ctnr_state = QREP_CTNR_STATE_STOPPED;
        if( status == 1 ) {
            pcs->ctnr_state = QREP_CTNR_STATE_ACTIVE;
        }
    }
    fthUnlock(wait);
}


int SDFGetNumNodesInMyGroup( SDF_internal_ctxt_t *pai) {
    struct SDF_action_state *pas = ((SDF_action_init_t *)pai)->pcs;
    qrep_state_t *rep_state = &(pas->qrep_state);
    qrep_node_state_t *node;
    int node_id;

    node_id = msg_sdf_myrank(); 
    if( node_id >= rep_state->nnodes ) {
        plat_log_msg(21222, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "Invalid Node Id:%d NumNodes:%d\n",node_id,rep_state->nnodes);
        plat_assert( node_id < rep_state->nnodes );
    }
    node = &(rep_state->node_state[node_id]);
    return rep_state->groups[node->group_id].num_nodes;
}
int SDFGetNumNodesInMyGroupFromConfig() {
    int  node_id,group_id, num_nodes;
    char prop_name[128];

    /* Ignore if simple replication is turned off */
    sprintf( prop_name, "SDF_SIMPLE_REPLICATION");
    if( strcmp( getProperty_String( "SDF_SIMPLE_REPLICATION", "Off" ),"Off") == 0 ) {
        return 0;
    }
    node_id = msg_sdf_myrank();
    sprintf( prop_name, "NODE[%d].GROUP_ID", node_id);
    group_id = getProperty_uLongInt(prop_name, -1);
    plat_assert(group_id >= 0 );

    sprintf( prop_name, "SDF_CLUSTER_GROUP[%d].NUM_NODES",group_id);
    num_nodes = getProperty_uLongInt(prop_name, -1);
    plat_assert(num_nodes >= 0 );
    plat_log_msg(21227, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION,PLAT_LOG_LEVEL_DEBUG,
                       "Number of Nodes:%d in node:%d\n",num_nodes,node_id);
    return num_nodes;
}

int SDFGetNumNodesInClusterFromConfig() {
    int num_nodes;
    char prop_name[128];
    sprintf( prop_name, "SDF_CLUSTER_NUMBER_NODES");
    num_nodes  = getProperty_uLongInt(prop_name, 0);
    if( num_nodes <= 0 ) {
         plat_log_msg(80003, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                    "SDF_CLUSTER_NUMBER_NODES should be > 0\n");
         plat_abort();
    }
    plat_log_msg(80004, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                    "SDFGetNumNodesInClusterFromConfig: Number of Nodes  %d \n",num_nodes);
    return num_nodes;
}

/*
 * Gets the cluster group type
 */
int SDFGetClusterGroupType( SDF_internal_ctxt_t *pai, int64_t cid ) {
    int i, j , k, grp_type=SDF_REPLICATION_NONE;
    struct SDF_action_state *pas = ((SDF_action_init_t *)pai)->pcs;
    qrep_state_t *rep_state = &(pas->qrep_state);
    qrep_cluster_grp_t *grp;
    qrep_node_state_t *node;

    for( i = 0; i < rep_state->num_groups; i++ ) {
       grp = &(rep_state->groups[i]);
       for( j = 0; j < grp->num_nodes; j++ ) {
          node = &(rep_state->node_state[grp->nodes[j]]);
          for( k = 0; k < node->nctnrs_node; k++ ) {
              if( node->cntrs[k].id == cid ) {
                   if( grp->type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
                       grp_type = SDF_REPLICATION_V1_2_WAY;
                   }
                   else if( grp->type == SDF_CLUSTER_GRP_TYPE_NPLUS1 ) {
                       grp_type = SDF_REPLICATION_V1_N_PLUS_1;
                   }
                   else if( grp->type == SDF_CLUSTER_GRP_TYPE_SIMPLE_REPLICATION ) {
                       grp_type = SDF_REPLICATION_SIMPLE;
                   }
              }
          }
       }
    }
    plat_log_msg(21228, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                 "SDFGetClusterGroupType CID:%d GroupType:%d \n",(int)cid, grp_type);
    return grp_type;
}

/*
 * Returns VIP Group Group ID associated with the cid
 */
int SDFGetClusterGroupGroupId( SDF_internal_ctxt_t *pai, int64_t cid ) {
    int i, j , k;
    struct SDF_action_state *pas = ((SDF_action_init_t *)pai)->pcs;
    qrep_state_t *rep_state = &(pas->qrep_state);
    qrep_cluster_grp_t *grp;
    qrep_node_state_t *node;

    if( cid == 1 ) {
        /* Special case. This is CMC. Just return 0 */   
        return 0;
    }
    for( i = 0; i < rep_state->num_groups; i++ ) {
       grp = &(rep_state->groups[i]);
       for( j = 0; j < grp->num_nodes; j++ ) {
          node = &(rep_state->node_state[grp->nodes[j]]);
          for( k = 0; k < node->nctnrs_node; k++ ) {
              if( node->cntrs[k].cguid == cid ) {
                  return grp->grp_id;
              }
          }
       }
    }
    plat_log_msg(21229, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                     "Unable to get Group Group ID for container id:%d\n",(int)cid);
    plat_abort();
    return -1;
}

/*
 * Updates the meta data struture with container's vgrpgrp ID and Group Type
 */
SDF_status_t SDFUpdateMetaClusterGroupInfo( SDF_internal_ctxt_t *pai, SDF_container_meta_t *meta, int64_t cid ) {
    int i, j , k;
    struct SDF_action_state *pas = ((SDF_action_init_t *)pai)->pcs;
    qrep_state_t *rep_state = &(pas->qrep_state);
    qrep_cluster_grp_t *grp;
    qrep_node_state_t *node;

    for( i = 0; i < rep_state->num_groups; i++ ) {
       grp = &(rep_state->groups[i]);
       for( j = 0; j < grp->num_nodes; j++ ) {
          node = &(rep_state->node_state[grp->nodes[j]]);
          for( k = 0; k < node->nctnrs_node; k++ ) {
              if( node->cntrs[k].id == cid ) {
                   if( grp->type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
                       meta->type = SDF_REPLICATION_V1_2_WAY;
                   }
                   else if( grp->type == SDF_CLUSTER_GRP_TYPE_NPLUS1 ) {
                       meta->type = SDF_REPLICATION_V1_N_PLUS_1;
                   }
                   else if( grp->type == SDF_CLUSTER_GRP_TYPE_SIMPLE_REPLICATION ) {
                       meta->type = SDF_REPLICATION_SIMPLE;
                   }
                   else {
                       meta->type = SDF_REPLICATION_NONE;
                   }
                   meta->inter_node_vip_group_group_id = node->cntrs[k].group_id;
                   plat_log_msg(21230, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                              "UPDATE META: CID:%d GroupId:%d type:%d\n",(int)cid, meta->inter_node_vip_group_group_id,(int)(grp->type));
                   return SDF_SUCCESS;
              }
          }
       }
    }
    return SDF_FAILURE;
}

#define SB_HANDLER_TCP_PORT 51360
#define SB_HANDLER_BUF_SIZE 128
/* This function sends the given command to local split brain handler */
int send_command_to_sb_handler(char *command) {
    int sockfd,n;
    struct sockaddr_in serveraddr;
    struct hostent *server;
    char buf[SB_HANDLER_BUF_SIZE];

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)  {
        plat_log_msg(21231, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                 "Unable to open socket\n");
        return 1;
    }

    server = gethostbyname("127.0.0.1");
    if (server == NULL) {
        plat_log_msg(21232, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                 "Unable to resolve local host\n");
        close(sockfd);
        return 1;
    }

    bzero((char *) &serveraddr, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    bcopy((char *)server->h_addr,(char *)&serveraddr.sin_addr.s_addr, server->h_length);
    serveraddr.sin_port = htons(SB_HANDLER_TCP_PORT);
    /* connect: create a connection with the server */
    if (connect(sockfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) < 0) {
      plat_log_msg(21233, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                 "Connection to split brain handler failed\n");
      close(sockfd);
      return 1;
    }
     /* send the message line to the server */
    n = write(sockfd, command, strlen(command));
    if (n < 0) {
      plat_log_msg(21234, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                 "Write to split brain handler failed\n");
      close(sockfd);
      return 1;
    }
    /* print the server's reply */
    bzero(buf, SB_HANDLER_BUF_SIZE);
    n = read(sockfd, buf, SB_HANDLER_BUF_SIZE);
    if (n < 0) {
        plat_log_msg(21235, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                 "Read from split brain handler failed\n");
        close(sockfd);
        return 1 ;
    }
    close(sockfd);
    return 0;
}

int send_node_state_event_to_sb_handler( int node, int status ) {
    char buf[SB_HANDLER_BUF_SIZE];
    sprintf(buf,"node_state %d %s",node, (status == 1)?"up":"down");
    return send_command_to_sb_handler(buf);
}

/*
 * Liveness callback function.
 */
static void live_back(int live, int rank, void *arg)
{
    if (!live) {
	simple_replicator_node_dead(arg, rank);
    } else {
	simple_replicator_node_live(arg, rank);
    }
    send_node_state_event_to_sb_handler(rank,live);
}


/**
 * @brief Indicate node is live
 *
 * @param pnode <IN> Node which was alive
 * @param last_live <IN> Time since the epoch when the node was last alive
 * @param expires <IN> Time node should be considered alive until
 */

void simple_replicator_node_live(SDF_action_state_t *pas, vnode_t pnode)
{
    int my_node, i,rtype;
    qrep_state_t      *ps;
    qrep_node_state_t *pns;
    fthWaitEl_t * wait_list;
    ps = &(pas->qrep_state);
    pns = &(ps->node_state[pnode]);

    /*   If this is replacing a dead node, configure it as a standby
     *   for all containers that died with it.
     */
    pns->live = 1;

    plat_log_msg(21236, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
		 "Node %d is live!", pnode);
    my_node = msg_sdf_myrank();
    /* If my node is not part of NPLUS1 or 2way. Just skip from here */
    if( (ps->groups[ps->node_state[my_node].group_id].type != SDF_CLUSTER_GRP_TYPE_MIRRORED) &&
        (ps->groups[ps->node_state[my_node].group_id].type != SDF_CLUSTER_GRP_TYPE_NPLUS1) ) {
        return;
    }

    /* Check whether the node is my self */
    if( my_node == pnode ) {
        plat_log_msg(21237, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
		 "Live Node is my node. Ignore the event\n");
        return;
    }

    /* Check whether the new node is in the same group */
    if( ps->node_state[my_node].group_id != ps->node_state[pnode].group_id ) {
        plat_log_msg(21238, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                 "The Live node is not part of the the local group(id:%d). So Ignore the event\n",
                                                     ps->node_state[my_node].group_id);
        return;
    }
    /* Both are in the same group */
    plat_log_msg(21239, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
          "Both the local node %d  and new live node %d are in same group %d\n",
                                              my_node, pnode, ps->node_state[my_node].group_id);
    /* Check whether local node is owning the virtual IP group of the new live node */
    for( i = 0; i < ps->node_state[my_node].num_vgrps_being_serviced; i++ ) {
       if( ps->node_state[my_node].serviced_vgrp_ids[i] == pnode ) {
           break;
       }
    }
    if( (ps->node_state[my_node].type == QREP_NODE_TYPE_ACTIVE) && 
                                   (i >= ps->node_state[my_node].num_vgrps_being_serviced) ) {
        plat_log_msg(21240, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
        "The Live node %d is not serviced by the local node %d !. Ignore the event for now\n",
                                                                                      pnode, my_node);
        return;
    }
    /* Local node is serving the new node */
    if( ps->groups[ps->node_state[my_node].group_id].type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
        /* The group is N plus 1 group. Just ignore */
        /* Check whether my current node is active. If not active. Ignore it */
        if( ps->node_state[my_node].type != QREP_NODE_TYPE_ACTIVE ) {
            plat_log_msg(21241, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                         "My local node %d is not yet active just ignore\n", my_node);
            return;
        }
        //fthResume(fthSpawn(&data_recovery_thread, 4096), (uint64_t) pnode);
        //fthMboxPost(&rec_thread_avail_mbox, (uint64_t)pnode);
        wait_list = fthLock(&(ps->node_state[my_node].lock), 1, NULL);
        if( ps->node_state[my_node].persistent_auth == 1 ) {
            rtype = QREP_RECOVERY_TYPE_ALL;
        }
        else {
            rtype = QREP_RECOVERY_TYPE_NON_PERSISTENT_ONLY;
        }
        fthUnlock(wait_list);
        send_recovery_start_event(pnode,rtype);
        //simple_replicator_send_data_copy_completed();
        return;
    }
    if( ps->groups[ps->node_state[my_node].group_id].type == SDF_CLUSTER_GRP_TYPE_NPLUS1 ) {
        /* The group is N plus 1 group. Just ignore */
        plat_log_msg(21242, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                         "The Recovering node is part of nplus 1. Just ignore \n");
        return;
    }
    else {
        /* The group is N plus 1 group. Just ignore */
        plat_log_msg(21243, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                         "The Recovering node is in group %d Just ignore \n", 
                                                             ps->groups[ps->node_state[my_node].group_id].type);
    }
    return;
}

void send_recovery_start_event(int32_t pnode, QREP_RECOVERY_TYPE rtype) {
    fthWaitEl_t * wait_list;
    SDF_action_init_t *pai;
    qrep_state_t * ps;
    int my_node = sdf_msg_myrank();

    if( sdf_action_init_ptr == NULL ) {
        plat_log_msg(21244, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                             "Unable to find Action Init state. Skipping recovery...\n");
        return;
    }
    pai = sdf_action_init_ptr();
    ps = &(pai->pcs->qrep_state);
    wait_list = fthLock(&(ps->node_state[my_node].lock), 1, NULL);
    ps->node_state[my_node].rec_count++;
    /* pnode variable holds   <rec_count(bits 31-16)>:<rec_type(bits 15-8)>:<nodeid(bits 7-0)) */
    pnode = (ps->node_state[my_node].rec_count << 16) | ((rtype & 0xFF) << 8) | (pnode & 0xFF);
    fthMboxPost(&rec_thread_avail_mbox, (uint64_t)pnode);
    fthUnlock(wait_list);
}


/*
 * Release the VIP groups from a node
 */
SDF_status_t simple_replicator_remove_vip_group(SDF_action_init_t * pai, vnode_t node)
{
    qrep_state_t * ps;
    
    ps = &(pai->pcs->qrep_state); 

    if (sdf_remove_vip) {
        sdf_remove_vip(ps->vip_config, node);
    }

    return SDF_SUCCESS;
}

int simple_replicator_replicate_to_node(SDF_action_init_t * pai, vnode_t node)
{
    SDF_action_state_t * pas;
    qrep_node_state_t *pns;
    qrep_ctnr_state_t *pcs;
    qrep_state_t * ps;
    int rc;
    int i;

    pas = pai->pcs;
    ps = &(pai->pcs->qrep_state);

    pns = &(ps->node_state[node]);
    
    for (i = 0; i < pns->nctnrs_node; i++) {
        pcs = &(pns->cntrs[i]);

        rc = simple_replicator_enable_node(pas, pcs->cguid, node);
        if (rc != SDF_SUCCESS) {
            return 1;
        }
    }

    return 0;
}

SDF_status_t simple_replicator_enable_node(SDF_action_state_t * pas, SDF_cguid_t cguid, vnode_t node)
{
    qrep_state_t * ps;
    qrep_ctnr_state_t * pcs;
    qrep_node_state_t * pns;
    int mynode;
    int i;
    
    mynode = sdf_msg_myrank();

    ps = &(pas->qrep_state);
    pns = &ps->node_state[mynode];
    
    // First get the container
    for (i = 0; i < pns->nctnrs_node; i++) {
        pcs = &pns->cntrs[i];
        if (pcs->cguid == cguid) {
            pcs->replicating_to_node = node;
	    plat_log_msg(30556, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_TRACE,
		     "replicating_to_node = %d", node);

            return SDF_SUCCESS;
        }
    }

    return SDF_OBJECT_UNKNOWN;

}

static SDF_status_t simple_replicator_sync_flush_times(SDF_action_init_t * pai, SDF_cguid_t cguid)
{
    struct SDF_trans_state * ptrans;
    SDF_action_state_t * pas;
    SDF_container_meta_t * meta;
    SDF_appreq_t par;
    
    pas = pai->pcs;

    ptrans = get_trans_state((SDF_action_thrd_state_t *) pai->pts);
    meta = sdf_get_preloaded_ctnr_meta(pas, cguid);
    if (!meta) {
        return SDF_CONTAINER_UNKNOWN;
    }
    ptrans->pas = pas;
    ptrans->pai = pai;
    ptrans->pts = pai->pts;
    par.ctnr = cguid;
    par.ctxt = pai->ctxt;
    par.reqtype = APICD;
    par.invtime = meta->flush_time;
    par.curtime = sdf_get_current_time();
    ptrans->par = &par;

    if (meta->flush_time) {
        // This does both a local and remote flush_all
        // Besides a quick blip, shouldn't hurt to do it twice locally
        flush_all(ptrans);
    }

    return SDF_SUCCESS;
}

SDF_status_t simple_replicator_enable_replication(SDF_action_init_t * pai, SDF_cguid_t cguid, vnode_t node)
{
    qrep_state_t * ps;
    SDF_action_state_t * pas;
    qrep_node_state_t * pns;
    
    pas = pai->pcs;
    ps = &(pas->qrep_state);
    pns = &ps->node_state[node];

    simple_replicator_enable_node(pas, cguid, node);
    
    // Set the node to being live so we don't have a race
    // condition with the liveness event coming in and the
    // data forwarding starting.
    pns->live = 1;

    simple_replicator_sync_flush_times(pai, cguid);

    return SDF_SUCCESS;
}

SDF_status_t simple_replicator_disable_replication(SDF_action_state_t * pas, SDF_cguid_t cguid, vnode_t node)
{
    qrep_state_t * ps;
    qrep_ctnr_state_t * pcs;
    qrep_node_state_t * pns;
    int mynode;
    int i;
    
    mynode = sdf_msg_myrank();

    ps = &(pas->qrep_state);
    pns = &ps->node_state[mynode];
    
    // First get the container
    for (i = 0; i < pns->nctnrs_node; i++) {
        pcs = &pns->cntrs[i];
        if (pcs->cguid == cguid) {
            pcs->replicating_to_node = SDF_ILLEGAL_VNODE;
	    plat_log_msg(30555, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_TRACE,
		     "replicating_to_node = SDF_ILLEGAL_VNODE");
            return SDF_SUCCESS;
        }
    }

    return SDF_OBJECT_UNKNOWN;
}


#if 0
/*
 * @brief Find out what node survived
 */
static int surviving_replica(qrep_ctnr_state_t * pcs, vnode_t dead_node)
{
    int i;
    
    plat_assert(QREP_MAX_REPLICAS == 2);

    for (i = 0; i < QREP_MAX_REPLICAS; i++) {
        if (pcs->replica_node[i] != dead_node &&
            pcs->replica_node[i] != -1) {
            return i;
        }
    }

    // If we get here, either the config is wrong, or else we are a
    // zombie (ie a running dead node)
    plat_assert(0);

    return -1;
}
#endif

int partner_replica_by_cguid(qrep_state_t * ps, SDF_cguid_t cguid)
{
    qrep_ctnr_state_t * pcs = 0;
    qrep_node_state_t * pns;
    fthWaitEl_t * wait_list;
    int mynode;
    int outnode = SDF_ILLEGAL_VNODE;
    int i;
    
    mynode = sdf_msg_myrank();
    pns = &ps->node_state[mynode];
    
    wait_list = fthLock(&pns->lock, 0, NULL);

    if ((ps->groups[pns->group_id].type == SDF_CLUSTER_GRP_TYPE_MIRRORED) ||
        (pns->type == QREP_NODE_TYPE_ACTIVE_CLONING)) {
        // First get the container
        for (i = 0; i < pns->nctnrs_node; i++) {
            pcs = &pns->cntrs[i];
            if (pcs->cguid == cguid) {
                break;
            }
        }
        if( i < pns->nctnrs_node ) {
            outnode = pcs ? pcs->replicating_to_node : SDF_ILLEGAL_VNODE;
        }
    }
    fthUnlock(wait_list);

    return outnode;
}

// XXX only support TCP!
int partner_replica_port_by_cguid(qrep_state_t * ps, SDF_cguid_t cguid)
{
    qrep_ctnr_state_t * pcs;
    qrep_node_state_t * pns;
    int mynode;
    int i, j;
    
    mynode = sdf_msg_myrank();
    
    for (i = 0; i < ps->nnodes; i++) {
        if (i == mynode) {
            continue;
        }

        pns = &ps->node_state[i];
        
        // First get the container
        for (j = 0; j < pns->nctnrs_node; j++) {
            pcs = &pns->cntrs[j];

            if (pcs->cguid == cguid &&
                !(pcs->flags & qr_standby)) {
                return pcs->vip_tcp_port;
            }
                
        }
    }
    
    return 0;
}

int partner_replica_by_container_id(qrep_state_t * ps, int container_id)
{
    qrep_ctnr_state_t * pcs = 0;
    qrep_node_state_t * pns;
    int mynode;
    int i;
    
    mynode = sdf_msg_myrank();
    pns = &ps->node_state[mynode];
    
    // First get the container
    for (i = 0; i < pns->nctnrs_node; i++) {
        pcs = &pns->cntrs[i];
        if (pcs->id == container_id) {
            break;
        }
    }
    
    return pcs ? pcs->replicating_to_node : -1;
}

int node_is_alive(qrep_state_t * ps, int node) {
    qrep_node_state_t *pns;
    
    pns = &(ps->node_state[node]);
    return pns->live;
}

/**
 * @brief Indicate that a node should be treated as dead
 *
 * @param pnode <IN> Node which is dead.
 */
static void simple_replicator_node_dead(SDF_action_state_t *pas, vnode_t pnode)
{
    
#if 1
    qrep_state_t      *ps;
    qrep_node_state_t *pns;
    qrep_ctnr_state_t *pcs;
    int mynode;
    int i;

    mynode = sdf_msg_myrank();

    ps = &(pas->qrep_state);
    pns = &(ps->node_state[pnode]);
    // Mark the node dead so no more messages get lost
    pns->live = 0;
    pns->rec_flag = 0;
    
    plat_log_msg(21245, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
		 "Node %d is dead!", pnode);

    pns = &(ps->node_state[mynode]);

    // Stop replicating to that node too
    for (i = 0; i < pns->nctnrs_node; i++) {
        pcs = &pns->cntrs[i];

        if (pcs->replicating_to_node == pnode) {
            pcs->replicating_to_node = SDF_ILLEGAL_VNODE;
	    plat_log_msg(30555, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_TRACE,
		     "replicating_to_node = SDF_ILLEGAL_VNODE");
        }
    }

    // Take over VIP if is a standby
    //sdf_simple_dead(mynode, pnode);

    return;
#else
    /* Old state machine */

    qrep_state_t      *ps;
    qrep_node_state_t *pns;
    qrep_ctnr_state_t *pcs;
    int mynode;
    int i;

    mynode = sdf_msg_myrank();

    ps = &(pas->qrep_state);
    pns = &(ps->node_state[pnode]);
    
    plat_log_msg(21245, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
		 "Node %d is dead!", pnode);

    // Mark the node dead so no more messages get lost
    pns->live = 0;

    for (i = 0; i < pns->nctnrs_node; i++) {
        // Get our container's state
        pcs = &(pns->cntrs[i]);
        
	if (pcs->nreplicas <= 1) { 
	    /*   If dead node did not replicate, and has a designated standby, 
	     *   do the VIP switchover
	     *   for all dead containers that require VIP switchover.
	     */

	    if (pcs->standby_node == mynode) {
		if (pcs->flags & qr_vip) {
                    // Turn on the VIP
		    sdf_notify_simple(pcs->id, 1);
		}
	    }
	} else {
	    /*   If dead node did replicate, failover to the replica,
	     *   doing a VIP switchover if specified.
	     */
            if (surviving_replica(pnode, pcs) == mynode) {
                if (pcs->flags & qr_vip) {
		    sdf_notify_simple(pcs->id, 1);
                }
            }
            
            if (pcs->standby_node == mynode) {
                /*   If dead node replicated and we are the standby,
                 *   start replicating from the survivor
                 */

                simple_replicator_start_new_replica(pas, surviving_replica(pnode, pcs), pcs,0,QREP_RECOVERY_TYPE_ALL);
            }
	}
    }
#endif

}

qrep_ctnr_state_t * cguid_to_pcs(qrep_state_t * ps, vnode_t node, SDF_cguid_t cguid)
{
    qrep_node_state_t *pns;
    qrep_ctnr_state_t *pcs;
    // int mynode;
    int i;

    // mynode = sdf_msg_myrank();
    
    pns = &(ps->node_state[node]);

    for (i = 0; i < pns->nctnrs_node; i++) {
        // Get our container's state
        pcs = &(pns->cntrs[i]);

        if (pcs->cguid == cguid) {
            return pcs;
        }
    }

    // Not found
    return 0;
}

void simple_replicator_send_data_copy_completed() {
#ifdef SIMPLE_REPLICATION
    #define MAX_SHARDIDS 32 // Not sure what max is today
    char * output;
    int i;
    uint32_t shard_count;
    SDF_shardid_t shardids[MAX_SHARDIDS];
    SDF_action_init_t *pai;
    struct shard *pshard = NULL;

    plat_assert( sdf_action_init_ptr != NULL );
    pai = sdf_action_init_ptr();
    plat_assert( pai != NULL );
    get_container_shards(pai, CMC_CGUID, shardids, MAX_SHARDIDS, &shard_count);
    for (i = 0; i < shard_count; i++) {

        pshard = shardFind(pai->flash_dev, shardids[i]);
        ssd_shardSync(pshard);    

        plat_log_msg(21246, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION,PLAT_LOG_LEVEL_TRACE, "CMC Shard: %lx cguid:%d\n",
                                              shardids[i],CMC_CGUID);
    }
    plat_log_msg(21247, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION,PLAT_LOG_LEVEL_DEBUG, "Sending RECOVERED command to STM\n");
    sdf_replicator_command_sync(sdf_shared_state.config.replicator,shardids[0], "RECOVERED", &output);
    send_command_to_sb_handler("db_fail_check");
#endif
}

int stop_clone_to_node(SDF_action_init_t * pai, int cnode ) {
    SDF_action_state_t *pas;
    qrep_state_t * ps;
    qrep_node_state_t *pns;
    qrep_ctnr_state_t * pcs;
    int mynode, i;
    fthWaitEl_t * wait_list;

    mynode = sdf_msg_myrank();
    pas = pai->pcs;
    ps = &(pai->pcs->qrep_state);
    pns = &(ps->node_state[mynode]);
    plat_log_msg(21248, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION,
                 PLAT_LOG_LEVEL_DEBUG,"Stoping cloning\n");
    pns->type = QREP_NODE_TYPE_ACTIVE;
    wait_list = fthLock(&(pns->lock), 1, NULL);
    for (i = 0; i < pns->nctnrs_node; i++) {
        pcs = &(pns->cntrs[i]);
        simple_replicator_disable_replication(pas, pcs->cguid, cnode);
    }
    fthUnlock(wait_list);
    return 0;
}

int start_persistent_recovery(SDF_action_init_t * pai) {
    int mynode;
    // int peer_node;
    qrep_state_t      *rep_state;
    qrep_node_state_t *pns;
    /*qrep_ctnr_state_t * pcs;
    SDF_cguid_t cguid = 0; */
    //fthWaitEl_t * wait;
    if( SDFSimpleReplication != 1 ) {
        return 1;
    }

    mynode = sdf_msg_myrank();
    rep_state = &(pai->pcs->qrep_state);
    pns = &(rep_state->node_state[mynode]);

    /* Set the persistent auth flag */
    mynode = sdf_msg_myrank();
    if ( rep_state->groups[pns->group_id].type != SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
        return 1;
    }
    // peer_node = rep_state->groups[pns->group_id].nodes[0];
    if( rep_state->groups[pns->group_id].nodes[0] == mynode ){
        // peer_node = rep_state->groups[pns->group_id].nodes[1];
    }
    pns->persistent_auth = 1;
#if 0
    /* Start the containers */
    //wait = fthLock(&pns->lock, 1, NULL);
    for (i = 0; i < pns->nctnrs_node; i++) {
        pcs = &(pns->cntrs[i]);
        cguid = pcs->cguid;
        if(  ! (pcs->flags & qr_persist)  ) {
            continue;
        }
        if( sdf_mcd_get_tcp_port_by_cguid(cguid, (int *)&(pcs->vip_tcp_port)) != 0 ) {
            plat_log_msg(21249, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                         "Failed to get the container port for :%d", (int)pcs->id);
            fail = 1;
            break;
        }
        if( sdf_mcd_start_container_internal(pai,pcs->vip_tcp_port) != 0 ) {
                     plat_log_msg(21250, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                        "Restarting the container %d failed before starting recovery", pcs->vip_tcp_port);
            fail = 1;
            continue;
        }
    }
    //fthUnlock(wait); 
    if( fail == 0 ) {
        send_recovery_start_event(peer_node,QREP_RECOVERY_TYPE_PERSISTENT_ONLY);
    }
#endif
    return 0;
}

int start_clone_to_node(SDF_action_init_t * pai, int cnode ) {
    SDF_action_state_t *pas;
    qrep_state_t * ps;
    qrep_node_state_t *pns;
    qrep_ctnr_state_t * pcs;
    int mynode, i;
    fthWaitEl_t * wait_list;

    mynode = sdf_msg_myrank();
    pas = pai->pcs;
    ps = &(pai->pcs->qrep_state);
    pns = &(ps->node_state[mynode]);
    pns->type = QREP_NODE_TYPE_ACTIVE_CLONING;
    wait_list = fthLock(&(pns->lock), 1, NULL);
    /* Start the threads if not runnung already*/
    if( pns->rec_count == 0 ) {
        plat_log_msg(21251, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION,
                 PLAT_LOG_LEVEL_DEBUG,"Starting cloning threads\n");
        sdf_create_queue_pair(mynode, VNODE_ANY, SDF_RESPONSES, SDF_FLSH, SDF_WAIT_FTH);
        fthMboxInit(&ps->sync_thread_avail_mbox);
        fthMboxInit(&rec_thread_avail_mbox);
        fthLockInit(&sync_remote_container_lock);

        for (i = 0; i < ps->nctnr_sync_threads; i++) {
            if (NEW_RECOVERY_CODE) {
                fthResume(fthSpawn(&sync_container_thread_new, 40960), (uint64_t) pas);
            } else {
                fthResume(fthSpawn(&sync_container_thread, 40960), (uint64_t) pas);
            }
        }
        fthResume(fthSpawn(&data_recovery_thread, 40960), (uint64_t) 0);
    }
    for (i = 0; i < pns->nctnrs_node; i++) {
        pcs = &(pns->cntrs[i]);
        simple_replicator_enable_node(pas, pcs->cguid, cnode);
    }
    fthUnlock(wait_list);
    /* send the recoovery message */
    send_recovery_start_event(cnode,QREP_RECOVERY_TYPE_ALL);
    return 0;
}

/*
 * simple_replicator_start_new_replica: Master starts sending replicate message to slave
 * for each container. the slave calls this function when it received the messages from master 
 * with the given container Id. Master does not use cntr_id
 * recovery type is decided at Master of the recovery. rtype is only used at the master side
 */
int simple_replicator_start_new_replica(SDF_action_init_t * pai, vnode_t master, vnode_t slave,int cntr_id, 
                                                                                  QREP_RECOVERY_TYPE rtype)
{
    int rc, i, fail=0,nctnrs_node;
    time_t cur_time;
    vnode_t mynode;
    SDF_action_state_t *pas;
    qrep_state_t * ps;
    qrep_node_state_t *pns;
    qrep_ctnr_state_t *pcs, *pcs_list;
    SDF_cguid_t cguid = 0;
    fthWaitEl_t * wait;

    pas = pai->pcs;
    ps = &(pai->pcs->qrep_state);
    pns = &(ps->node_state[master]);
    mynode = sdf_msg_myrank();

    /* pns->container list may be modified if we do formating. so take a local copy of the
       container list and work on that */
    wait = fthLock(&pns->lock, 1, NULL);
    pcs_list = plat_alloc(pns->nctnrs_node * sizeof(qrep_ctnr_state_t));
    if (pcs_list == NULL) {
        plat_log_msg(21252, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                       "Could not allocate memory for container list");
        plat_abort();
    }
    nctnrs_node = pns->nctnrs_node;
    memcpy(pcs_list, pns->cntrs, (pns->nctnrs_node * sizeof(qrep_ctnr_state_t)));
    fthUnlock(wait);

    if( mynode != master ) {
        if( ps->node_state[mynode].num_vgrps_being_serviced > 0) {
            plat_log_msg(21253, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_FATAL,
                        "Recovery without memcached restart due to possible splitbrian condition. Exiting...");
             plat_abort();
        }
    }
    if( SDFIsNodeInCloneMode() == 1 ) {
         qrep_node_state_t *mns;
         /* Containers are not modified dynamically. So no need to locl*/
         plat_log_msg(10000, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION,PLAT_LOG_LEVEL_DEBUG,
                             "The Node is in CLONE Mode.\n");
         mns = &(ps->node_state[mynode]);
         pcs_list = plat_alloc(mns->nctnrs_node * sizeof(qrep_ctnr_state_t));
         if (pcs_list == NULL) {
             plat_log_msg(21252, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                       "Could not allocate memory for container list");
             return fail;
         }
         nctnrs_node = mns->nctnrs_node;
         memcpy(pcs_list, mns->cntrs, (mns->nctnrs_node * sizeof(qrep_ctnr_state_t)));
    }

    for (i = 0; i < nctnrs_node; i++) {
        pcs = &(pcs_list[i]);
        cguid = pcs->cguid;
        if( sdf_mcd_get_cname_by_cguid(cguid, pcs->name) != 0 ) {
            plat_log_msg(80005, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                         "Failed to get the container name for :%d", (int)pcs->id);
            continue;
        }
        if( mynode != master ) {
            if( pcs->id != cntr_id ) {
                 plat_log_msg(21254, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION,
                              PLAT_LOG_LEVEL_DEBUG,"SLAVE Skipping container %d\n",cntr_id);
                continue;
            }
        }
        else {
            /* Master */
            if( pcs->flags & qr_persist  ) {
                /* Persistent Container */
                /* ignore replicating if recovery type is only Non Persistent */
                if( rtype == QREP_RECOVERY_TYPE_NON_PERSISTENT_ONLY ) {
                    plat_log_msg(21255, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                         "Recovery type is only Non Persitent: Ignore recoverying container %d",pcs->id); 
                    continue;
                }
            }
            else {
                /* Non Persistent Container */
                /* Ignore, replicating if recovery type is only PERSISTENT container */
                if( rtype == QREP_RECOVERY_TYPE_PERSISTENT_ONLY ) {
                    plat_log_msg(21256, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                         "Recovery type is only Persitent: Ignore recoverying container %d",pcs->id); 
                    continue;
                }
            }
        }

        if (mynode != master) {
            time(&cur_time);
            sprintf(ps->node_state[mynode].datarecstat,"Data recovery from Node :%d Started on %s",master,ctime(&cur_time));
            plat_log_msg(20819, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG, "%s", 
                                                                      ps->node_state[mynode].datarecstat);
            if( pcs->flags & qr_persist  ) {
                /* Format the local persistant container before recovery*/
                plat_log_msg(80006, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                          "Formating the container %s", pcs->name);
                /* Stop the container before formating*/
                if( sdf_mcd_stop_container_byname_internal(pai,pcs->name) != 0 ) {
                     plat_log_msg(80007, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                          "Stopping the container %s failed", pcs->name);
                }
                /* mcd_agent_state.config.system_recovery = SYS_FLASH_REFORMAT;*/
                if( sdf_mcd_format_container_byname_internal(pai,pcs->name) != 0 ) {
                     plat_log_msg(80008, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                      "Failed to format the container port:%s", pcs->name);
                     time(&cur_time);
                     sprintf(ps->node_state[mynode].datarecstat,"Data recovery from Node :%d Failed on %s",
                                                                           master,ctime(&cur_time));
                     fail = 1;
                     break;
                }
            }
            int cntr_stat = sdf_mcd_is_container_running_byname(pcs->name);
            if( cntr_stat == -ENOENT ) {
                plat_log_msg(80009, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                          "Container %s Not Found", pcs->name);
                fail = 1;
                break;  
            }
            else if( cntr_stat == 0 ) {
                if( sdf_mcd_start_container_byname_internal(pai,pcs->name) != 0 ) {
                    plat_log_msg(80010, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                          "Starting the container %s Failed", pcs->name);
                    fail = 1;
                    break;  
                }
            }
            /* Send a replication message */
            rc = simple_replicator_request_replicating(pas, master, slave, pcs);
            if (rc != SDF_SUCCESS) {
                plat_log_msg(21262, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                             "Failed to start replicating from node: %d rc: %d", mynode, rc);
                time(&cur_time);
                sprintf(ps->node_state[mynode].datarecstat,"Data recovery to Node :%d Failed on %s",
                        master,ctime(&cur_time));
                fail = 1;
                break;
            }

            /*
             * Copy over the old data.  If both sides can handle fast recovery,
             * we use that; otherwise we use the default recovery.  The call to
             * rpc_get_clock_skew is only to get the current recovery version.
             */
	    if (NEW_RECOVERY_CODE) {
		rc = simple_replicator_sync_remote_container_new(pai, pas,
                                                                 master, cguid);
	    } else {
                int s = fast_copy_ctr(pai, pas, master, cguid);

                if (s >= 0)
                    rc = s ? SDF_SUCCESS : SDF_FAILURE;
                else {
		    rc = simple_replicator_sync_remote_container(pai, pas,
                                                                 master, cguid);
                }
	    }
            if (rc != SDF_SUCCESS) {
                time(&cur_time);
                sprintf(ps->node_state[mynode].datarecstat,"Data recovery from Node :%d Failed on %s",
                                                                                     master,ctime(&cur_time));
                plat_log_msg(21263, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR, 
                                        "%s: Failed to start replication", ps->node_state[mynode].datarecstat);
                fail = 1;
                break;
            }
            time(&cur_time);
            sprintf(ps->node_state[mynode].datarecstat,"Data recovery from Node :%d Completed on %s",master,ctime(&cur_time));
            plat_log_msg(20819, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG, "%s", 
                                                                      ps->node_state[mynode].datarecstat);
            /* Get the peer container status and set the status  */
            if (pcs->ctnr_state !=  QREP_CTNR_STATE_ACTIVE) {
                if( sdf_mcd_stop_container_byname_internal(pai,pcs->name) != 0 ) {
                     plat_log_msg(80011, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                          "Stoping the container %s after recovery failed", pcs->name);
                }
            }
            plat_log_msg(21265, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                         "Data recovery done for the container %d",pcs->id);
            if( pcs->flags & qr_persist  ) {
                if( fail == 0 ) {
                    /* success. set my node */
                    plat_log_msg(160024, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                         "Node becomes authoritative for persistent containers %d",pcs->id);
                    ps->node_state[mynode].persistent_auth = 1; 
                }
                else {
                    ps->node_state[mynode].persistent_auth = 0; 
                }
            }
            /* Data recovery from master completed. Enable Replication to Master Node */
            rc = simple_replicator_enable_node(pas, cguid, master);
            if (rc != SDF_SUCCESS) {
                plat_log_msg(21219, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                         "Could not start replication of container %d to node %d\n",
                                                (int)pcs->id, master);
            }
        }

        if (mynode == master) {
            /* Send a replication message */
            rc = simple_replicator_request_replicating(pas, master, slave, pcs);
            if (rc != SDF_SUCCESS) {
                plat_log_msg(21262, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                             "Failed to start replicating from node: %d rc: %d", mynode, rc);
                time(&cur_time);
                sprintf(ps->node_state[mynode].datarecstat,"Data recovery to Node :%d Failed on %s",
                        master,ctime(&cur_time));
                fail = 1;
                break;
            }

        }

        plat_assert(cguid);
    }
    /* The container stats back to the pns list on the slave*/
    if( mynode != master ) {
        for (i = 0; i < nctnrs_node; i++) {
            int status = 0;
            if( pcs_list[i].ctnr_state == QREP_CTNR_STATE_ACTIVE ) {
                status = 1;
            }
            SDFSetContainerStatus(pai,pcs_list[i].id, status);
        }
    }
    if( nctnrs_node > 0 ) {
        plat_free(pcs_list);
    }
    return fail;
}


/*
 * Starting a new replica is done in the following order:
 *
 * 1.) Start sending any new incoming data write operation
 * 2.) Do data copy the existing data
 * 3.) Mark the replica as "authoratative"
 */
int simple_replicator_start_new_replica_old(SDF_action_init_t * pai, vnode_t master, vnode_t slave)
{
    SDF_action_state_t *pas;
    qrep_state_t * ps;
    qrep_node_state_t *pns;
    qrep_ctnr_state_t * pcs;
    SDF_cguid_t cguid = 0;
    //int container_index;
    //SDF_shardid_t shard = -1;
    vnode_t mynode;
    time_t cur_time;
#if noyet
    char * output;
#endif
    int rc;
    int i;

    pas = pai->pcs;
    ps = &(pai->pcs->qrep_state);
    pns = &(ps->node_state[master]);

    mynode = sdf_msg_myrank();

    for (i = 0; i < pns->nctnrs_node; i++) {
        pcs = &(pns->cntrs[i]);
        cguid = pcs->cguid;
        if( sdf_mcd_get_tcp_port_by_cguid(cguid, (int *)&(pcs->vip_tcp_port)) != 0 ) {
            plat_log_msg(21249, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                         "Failed to get the container port for :%d", (int)pcs->id);
            continue;
        }
  
        if (mynode != master) {
            time(&cur_time);
            sprintf(ps->node_state[mynode].datarecstat,"Data recovery from Node :%d Started on %s",master,ctime(&cur_time));
            plat_log_msg(20819, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG, "%s", 
                                                                      ps->node_state[mynode].datarecstat);
            /* I am slave If the container is in persistant mode. Nuke all the containers contents*/
            if( pcs->flags & qr_persist  ) {
                /* get the TCP port for the given cguid */
                plat_log_msg(21257, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                          "Formating the container %d", pcs->vip_tcp_port);
                /* Stop the container before formating*/
                sdf_mcd_stop_container_internal(pai,pcs->vip_tcp_port);
                /* mcd_agent_state.config.system_recovery = SYS_FLASH_REFORMAT;*/
                
                if( sdf_mcd_format_container_internal(pai,pcs->vip_tcp_port) != 0 ) {
                     plat_log_msg(21259, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                      "Failed to format the container port:%d", pcs->vip_tcp_port);
                     time(&cur_time);
                     sprintf(ps->node_state[mynode].datarecstat,"Data recovery from Node :%d Failed on %s",
                                                                           master,ctime(&cur_time));
                     return 1;
                }
                plat_log_msg(21266, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                          "Restarting the container %d", pcs->vip_tcp_port);
                sdf_mcd_start_container_internal(pai,pcs->vip_tcp_port);
            }

            /* Send a replication message */
            rc = simple_replicator_request_replicating(pas, master, slave, pcs);
            if (rc != SDF_SUCCESS) {
                plat_log_msg(21262, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                             "Failed to start replicating from node: %d rc: %d", mynode, rc);
                time(&cur_time);
                sprintf(ps->node_state[mynode].datarecstat,"Data recovery from Node :%d Failed on %s",
                        master,ctime(&cur_time));
                return 1;
            }

            /* Do remote calls to data copy over the "old" data */
	    if (NEW_RECOVERY_CODE) {
		rc = simple_replicator_sync_remote_container_new(pai, pas, master, cguid);
	    } else {
		rc = simple_replicator_sync_remote_container(pai, pas, master, cguid);
	    }
            if (rc != SDF_SUCCESS) {
                plat_log_msg(21267, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                             "Failed to start replication: sync old node %d failed: %d", mynode, rc);
                time(&cur_time);
                sprintf(ps->node_state[mynode].datarecstat,"Data recovery from Node :%d Failed on %s",
                                                                                                 master,ctime(&cur_time));
                plat_log_msg(20819, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG, "%s", 
                                                                      ps->node_state[mynode].datarecstat);
                return 1;
            }
            else {
                /* Data recovery for the container is SUCCESS. Find peer its status in peer node and set it accordingly here
                   Call xianon's function here */
            }
            time(&cur_time);
            sprintf(ps->node_state[mynode].datarecstat,"Data recovery from Node :%d Completed on %s",master,ctime(&cur_time));
            plat_log_msg(20819, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG, "%s", 
                                                                      ps->node_state[mynode].datarecstat);
            /* Get the peer container status and set the status  */
            for( int j = 0; j < ps->node_state[mynode].nctnrs_node; j++ ) {
                qrep_ctnr_state_t *slave_cntr = &(ps->node_state[mynode].cntrs[j]);  
                plat_log_msg(21268, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                           "SLAVE CNTRID: %d status %d\n",slave_cntr->id,slave_cntr->ctnr_state); 
                if ((slave_cntr->id == pcs->id) && (slave_cntr->ctnr_state !=  QREP_CTNR_STATE_ACTIVE)) {
                    sdf_mcd_stop_container_internal(pai,pcs->vip_tcp_port);
                }
            }
            plat_log_msg(21269, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                         "Data recovery done");
        } else {
            /* Send a replication message */
            rc = simple_replicator_request_replicating(pas, master, slave, pcs);
            if (rc != SDF_SUCCESS) {
                plat_log_msg(21262, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                             "Failed to start replicating from node: %d rc: %d", mynode, rc);
                time(&cur_time);
                sprintf(ps->node_state[mynode].datarecstat,"Data recovery from Node :%d Failed on %s",
                        master,ctime(&cur_time));
                return 1;
            }
        }            

        plat_assert(cguid);
#if 0
        container_index = cguid_to_index(pas, cguid);
        shard = pas->ctnr_meta[container_index].meta.shard;
        /* Notify that we are authoratative on this container */
        sdf_replicator_command_sync(sdf_shared_state.config.replicator, shard, "RECOVERED", &output);
#endif
    }
    /* Set the state of the container */
    /*Recover is success, just set the containers state to its peers*/
    
#if 0
    if (ps->failback && mynode == slave) {
        /*
         * xxxzzz does this need to be in the for loop?
         */
        // use the last shard ID
        rpc_release_vip_group(pas->next_ctxt,
                              cguid, mynode,
                              master, shard);
        sdf_start_simple(mynode);
    }
#endif
    return 0;
}

static struct shard * shardid_to_shard(SDF_action_state_t * pas, SDF_shardid_t shard)
{
    flashDev_t *flash_dev;

#ifdef MULTIPLE_FLASH_DEV_ENABLED
    flash_dev = get_flashdev_from_shardid(pas->flash_dev, shard, pas->flash_dev_count);
#else
    flash_dev = pas->flash_dev;
#endif

    return simple_replication_shard_find(flash_dev, shard);
}

static SDF_status_t simple_replicator_request_replicating(SDF_action_state_t *pas,
                                                          vnode_t master, vnode_t slave,
                                                          qrep_ctnr_state_t * pcs)
{
    SDF_shardid_t shard;
    vnode_t dest_node;
    vnode_t mynode;
    size_t direction_size;
    int container_index;
    int rc;
    rep_cntl_msg_t cntl_msg;
    
    mynode = sdf_msg_myrank();
    dest_node = mynode == master ? slave : master;
    
    container_index = cguid_to_index(pas, pcs->cguid);
    shard = pas->ctnr_meta[container_index].meta.shard;

    cntl_msg.direction[0] = master;
    cntl_msg.direction[1] = slave;
    cntl_msg.cntr_id = pcs->id;
    cntl_msg.cntr_status = sdf_mcd_is_container_running_byname(pcs->name);
    direction_size = sizeof(rep_cntl_msg_t);
    
    rc = rpc_start_replicating(pas->next_ctxt, pcs->cguid, mynode, dest_node, shard, (void *)&cntl_msg, &direction_size);
    if (rc != SDF_SUCCESS) {
        plat_log_msg(21270, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                     "rpc_start_replicating failed: %d", rc);
    }

    return rc;
}

/*
 * Thread to start the data recovery
 */
void data_recovery_thread(uint64_t arg) {
    //int rnode,ret=0;
    SDF_action_init_t *pai;
    int mynode, rnode, ret=0,rtype;
    SDF_action_state_t * pas;
    qrep_state_t * ps;
    qrep_node_state_t *pns; 
    time_t cur_time;

    while (1) {

        /* Wait at the mail box */
        rnode = (int)fthMboxWait(&rec_thread_avail_mbox);
        plat_assert( sdf_action_init_ptr != NULL );
        pai = sdf_action_init_ptr();
        plat_assert( pai != NULL );

        pas = pai->pcs;
        plat_assert( pas != NULL );

        ps = &(pai->pcs->qrep_state);
        plat_assert( ps != NULL );

        mynode = msg_sdf_myrank();
        pns = &(ps->node_state[mynode]);

        /* At least wait for a lease Interval to make sure that current node is master
         * when Both the nodes of memcached is started at the same time, there is a possibility that
         * both the nodes in some transiant state for some time until state machine figures it out 
         * who owns the Virtual IPS */

        int rec_id =  rnode >> 16;
        rtype = (rnode >> 8) & 0xFF;
        rnode = rnode & 0xFF;
        if ( (pns->type != QREP_NODE_TYPE_ACTIVE_CLONING) && (rtype != QREP_RECOVERY_TYPE_PERSISTENT_ONLY) ) {
            plat_log_msg(21271, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                         "Waiting to start recovery to %d\n",rnode);
            time(&cur_time);
            unsigned long int timeinsecs = (unsigned long int)cur_time;
            while( 1 ) {
                 fthYield(2);
                 time(&cur_time);
                 if( ((unsigned long int)cur_time - timeinsecs ) >= 6 ) {
                     break;
                 }
            }
            if( pns->num_vgrps_being_serviced != 2 ) {
                plat_log_msg(21272, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                                                            "Number of VGRP serviced %d. Ignore Recovery to node %d\n",
                                                             pns->num_vgrps_being_serviced, rnode);
                continue;
            }
            if( ps->node_state[rnode].rec_flag == 1 ) {
                plat_log_msg(21273, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                                                            "Recovery to node %d already completed. Ignore\n", rnode);
                continue;
            }
        }
        /* Check whether processing any container commands like formatiing etc
           Recovery should not start if any container manipulation is pending*/
        while( 1 ) {
            if(sdf_mcd_processing_container_commands() == 0) {
                /* Not processing Any Container commands. Continue wuth the recovery*/
                break;
            }
            else {
                 plat_log_msg(21274, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                              "Waiting for completion of container manipulation command to start recovery");
            }
            fthYield(5);
        }
        fthWaitEl_t * wait_list;
        plat_log_msg(21275, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                         "recovery request ID  %d remote node %d rec_type %d\n",rec_id, rnode,rtype);
        int cur_recid=0;
        wait_list = fthLock(&(pns->lock), 1, NULL);
        cur_recid = pns->rec_count;
        fthUnlock(wait_list);

        if( rec_id != cur_recid ) {
            plat_log_msg(21276, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                                                            "There is new recovery request waiting. Ignore the currrent one %d\n",rec_id);
            continue;
        }
        if( ps->node_state[rnode].live != 1 )  {
            plat_log_msg(21277, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                                                            "Remote Node %d died. Ignore the recovery\n",rnode );
            continue;
        }
        plat_log_msg(21278, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,
                         "My Node Hosts 2 VIP Groups.  Start the recovery to node %d\n",rnode);
        ps->node_state[rnode].rec_flag = 1;
         
        /* Update the local node data recovery  recover starts 
         * Data recovery to Node :%d  Started on $date
         */
        time(&cur_time);
        sprintf(pns->datarecstat,"Data recovery to Node :%d Started on %s",rnode,ctime(&cur_time));
        plat_log_msg(20819, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG, "%s",pns->datarecstat);
        ret = simple_replicator_start_new_replica(pai, mynode, rnode,0,rtype);
        /* Update the local node data recovery  recover starts */
        if( ret == 0 ) {
            /* Update the local node data recovery  recover starts 
             * Data recovery to Node :%d Completed on $date
             */
            time(&cur_time);
            sprintf(pns->datarecstat,"Data recovery to Node :%d Completed on %s",rnode,ctime(&cur_time));
            plat_log_msg(20819, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,"%s",pns->datarecstat);
            if( (pns->type != QREP_NODE_TYPE_ACTIVE_CLONING) && (rtype != QREP_RECOVERY_TYPE_PERSISTENT_ONLY) &&
                (ps->groups[pns->group_id].type == SDF_CLUSTER_GRP_TYPE_MIRRORED )) {
                simple_replicator_send_data_copy_completed();
            }
        }
        else {
            /* Update the local node data recovery  recover starts 
             * Data recovery Start to Node :%d Failed on $date
             */
            time(&cur_time);
            sprintf(pns->datarecstat,"Data recovery to Node :%d Failed on %s",rnode,ctime(&cur_time));
            plat_log_msg(20819, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DEBUG,"%s",pns->datarecstat);
        }
    }
}

/*
 * Worker thread to sync a container
 */
static void sync_container_thread(uint64_t arg) {
    struct cursor_data * cursor_data;
    SDF_action_state_t * pas;
    qrep_state_t * ps;
    struct ssdaio_ctxt * paio_ctxt;   

    struct objMetaData meta;
    struct shard *pshard;
    SDF_time_t createtime = 0;
    SDF_time_t exptime = 0;
    size_t data_len = 0;
    void * data = 0;
    char * key = 0;
    uint32_t flags = 0;
    int tmp_index;
    int index;
    int key_len = 0;
    int mynode;
#ifdef KEY_LOCK_CONTAINER
    struct replicator_key_lock *key_lock;
#endif /* KEY_LOCK_CONTAINER */
    SDF_simple_key_t lock_key;

    int rc = 0;

    pas = (SDF_action_state_t *) arg;
    ps = &pas->qrep_state;

    mynode = sdf_msg_myrank();

    index = __sync_fetch_and_add(&num_sync_container_threads, 1);

    cursor_data = &cursor_datas[index];

    fthMboxInit(&cursor_datas[index].mbox);

    paio_ctxt = simple_replication_init_ctxt( SSD_AIO_CTXT_SYNC_CNTR );
    plat_assert(paio_ctxt);

    while (1) {
        cursor_data->rc = rc;

        // Let it be known we are available
        fthMboxPost(&ps->sync_thread_avail_mbox, (uint64_t)index);

        tmp_index = fthMboxWait(&cursor_datas[index].mbox);
        plat_assert(tmp_index == index);

        cursor_data = &cursor_datas[index];

        // Get cursor
        rc = rpc_get_by_cursor(cursor_data->ctxt, cursor_data->shard, mynode,
                               cursor_data->dest_node, cursor_data->cursor,
                               cursor_data->cursor_len, cursor_data->cguid, &(pas->gbc_mbx[index]),
                               &key, &key_len, &exptime, &createtime,
                               &meta.sequence, &data, &data_len, &flags);
        
        if (rc != SDF_SUCCESS) {
            if (rc == SDF_OBJECT_UNKNOWN || rc == SDF_FLASH_STALE_CURSOR) {
                if (rc == SDF_OBJECT_UNKNOWN) {
                    plat_free(key);
                }

                // these are tolerable & expected 
                rc = SDF_SUCCESS;
            }
            continue;
        }

        pshard = shardid_to_shard(pas, cursor_data->shard);
        plat_assert(pshard);
        
        /* Adjust for clock skew */
        if (exptime > 1) {
            meta.expTime = exptime - cursor_data->clock_skew;
        } else {
            meta.expTime = exptime;
        }
        meta.createTime = createtime - cursor_data->clock_skew;

        meta.keyLen = key_len;
        meta.dataLen = data_len;
       
        /*
         * XXX: drew 2010-03-05 Both the function pointer and the function
         * it points to are completely undocumented as to what a cas ID
         * is and when it needs to be replicated.  Maybe this stays here,
         * maybe it only happens when the lock is granted.
         */

        /* Update the cas ID */
	osd_set_shard_cas_id_if_higher(pshard, data);

        plat_assert(key_len <= SDF_SIMPLE_KEY_SIZE);
        memcpy(&lock_key.key, key, key_len);
        lock_key.len = key_len;

#ifdef KEY_LOCK_CONTAINER
        key_lock = NULL;
        rc = rklc_lock_sync(cursor_data->lock_container,
                            &lock_key, RKL_MODE_RECOVERY, &key_lock);
#endif /* KEY_LOCK_CONTAINER */
        if (rc == SDF_LOCK_RESERVED) {
            rc = SDF_SUCCESS;
        } else if (rc == SDF_SUCCESS) {
            rc = simple_replication_flash_put(paio_ctxt, pshard, &meta, key, data, FLASH_PUT_TEST_EXIST);

            if (rc != FLASH_EOK && rc != FLASH_EEXIST) {
                plat_log_msg(21279, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                             "flash_put failed: key: %s len: %d data: 0x%lx", key, key_len, (uint64_t)data);
                rc = get_status(rc);
            } else {
                plat_log_msg(21280, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_TRACE,
                             "data copy recv: key: %s len: %d data: 0x%lx rc: %d", key, key_len, (uint64_t)data, rc);
                rc = SDF_SUCCESS;
            }
        }
        
#ifdef KEY_LOCK_CONTAINER
        if (key_lock) {
            rkl_unlock(key_lock);
        }
#endif /* KEY_LOCK_CONTAINER */

        plat_free(key);
        plat_free(data);
    }
}

void update_cntr_data_copy_progress(qrep_node_state_t *node, SDF_cguid_t cguid, int percentage) {
    int i;

    for (i =0; i < node->nctnrs_node; i++) {
        if (node->cntrs[i].cguid == cguid) {
            node->cntrs[i].rec_prog = percentage;
        }
    }
}

void update_data_copy_progress(struct shard * pshard, int percentage)
{
    static int last_percentage = 0;

    SDF_action_init_t * pai;
    SDF_shardid_t shardid;
    qrep_node_state_t *pns;
    qrep_ctnr_state_t * pcs;
    SDF_action_state_t * pas;
    qrep_state_t * ps;
    vnode_t mynode;
    int container_index;
    int i;

    pai = sdf_shared_state.config.pai;
    ps = &(pai->pcs->qrep_state);
    mynode = sdf_msg_myrank();
    pns = &(ps->node_state[mynode]);

    pas = pai->pcs;

    if (last_percentage != percentage) {
#if SHOW_REPLICATION_STATUS
        if (!last_percentage) {
            printf("Container Data Sync\n");
        }
        printf("%d%%\n", percentage);
#endif
        last_percentage = percentage;
    }

    // XXX ugly, but since this is throw away code, leave it for now
    for (i = 0; i < pns->nctnrs_node; i++) {
        pcs = &(pns->cntrs[i]);
        container_index = cguid_to_index(pas, pcs->cguid);
        shardid = pas->ctnr_meta[container_index].meta.shard;

        if (shardid == pshard->shardID) {
            update_cntr_data_copy_progress(pns, pcs->cguid, percentage);
        }
    }
    
}

static SDF_status_t rpc_get_iteration_cursors_new(
          SDF_action_state_t *pas,
          SDF_context_t ctxt, vnode_t src_node, vnode_t dest_node,
	  SDF_shardid_t shard, SDF_cguid_t cguid, uint64_t seqno_start,
	  uint64_t seqno_len, uint64_t seqno_max,
	  void *cursor, int cursor_size,
	  struct flashGetIterationOutput **out)
{
    struct sdf_msg      *recv_msg;
    SDF_protocol_msg_t  *recv_pm;
    struct sdf_msg      *sdf_msg;
    SDF_size_t           message_size;
    SDF_status_t         error;
    int                  rc;
    sdf_fth_mbx_t        resp_mbx;
    fthMbox_t           *req_resp_fthmbx;
    it_cursor_t         *it_cursor;        

    *out = NULL;  // default return value

    req_resp_fthmbx = &(pas->git_mbx);
    
    resp_mbx.rbox = req_resp_fthmbx;
    resp_mbx.abox = NULL;
    resp_mbx.release_on_send = 1;
    resp_mbx.actlvl = SACK_RESP_ONLY_FTH;

    sdf_msg = rpc_load_msg(src_node,
                           dest_node, // to
                           HFGIC,
                           cursor_size,
                           (void *)cursor,
                           ctxt, 
                           cguid,
                           seqno_start,
                           seqno_max,
                           seqno_len,
                           &message_size, 
                           shard);

    if (sdf_msg_send(sdf_msg,
                     message_size,
                     /* to */
                     dest_node,
                     SDF_FLSH,
                     /* from */
                     src_node,
                     SDF_RESPONSES,
                     FLSH_REQUEST,
                     &resp_mbx, NULL)) {
        printf("send failure");
    }

    recv_msg = (struct sdf_msg *)fthMboxWait(req_resp_fthmbx);
    if (recv_msg == NULL) {
        return(SDF_FAILURE_MSG_RECEIVE);
    }
    
    // If used a cursor, make sure to free it
    if (cursor) {
        plat_free(cursor);
    }

    /* Check message response */
    error = sdf_msg_get_error_status(recv_msg);
    if (error != SDF_SUCCESS) {
        sdf_dump_msg_error(error, recv_msg);
        sdf_msg_free(recv_msg);
        return(SDF_FAILURE_MSG_RECEIVE);
    }

    recv_pm = (SDF_protocol_msg_t *)recv_msg->msg_payload;
    if (recv_pm == NULL) {
        sdf_msg_free(recv_msg);
        return(SDF_FAILURE_MSG_RECEIVE);
    }

    rc   = recv_pm->status;
    
    // Set the data
    if (rc == SDF_SUCCESS) {

        it_cursor = plat_alloc(recv_pm->data_size); // This gets freed on
                                               // the subsequent
                                               // iterate cursors call
        plat_assert(it_cursor);
        memcpy(it_cursor, (void *) recv_pm + sizeof(SDF_protocol_msg_t), recv_pm->data_size);

	*out = it_cursor;

        plat_assert(((it_cursor->cursor_count * it_cursor->cursor_len) + sizeof(it_cursor_t)) == recv_pm->data_size);
    } else {
        rc = SDF_FAILURE_MSG_RECEIVE;
    }

    sdf_msg_free(recv_msg);

    return rc;
}

static SDF_status_t rpc_get_by_cursor_new(
          SDF_action_state_t *pas,
          SDF_context_t ctxt, SDF_shardid_t shard, vnode_t src_node,
	  vnode_t dest_node, void *cursor,
	  size_t cursor_len, SDF_cguid_t cguid,
	  char **key, int *key_len,
	  SDF_time_t *exptime, SDF_time_t *createtime,
	  uint64_t *seqno, void **data, size_t *data_len, 
	  uint32_t *flags, fthMbox_t *req_resp_fthmbx)
{
    struct sdf_msg * recv_msg;
    SDF_protocol_msg_t * recv_pm;
    struct sdf_msg * sdf_msg;
    SDF_size_t message_size;
    SDF_status_t error;
    int rc;

    sdf_fth_mbx_t            resp_mbx;
    
    resp_mbx.rbox = req_resp_fthmbx;
    resp_mbx.release_on_send = 1;
    resp_mbx.actlvl = SACK_RESP_ONLY_FTH;

    sdf_msg = rpc_load_msg(src_node,
                           dest_node, // to
                           HFGBC,
                           cursor_len,
                           cursor,
                           ctxt, 
                           cguid,
                           0,
                           0,
                           0,
                           &message_size, 
                           shard);

    if (sdf_msg_send(sdf_msg,
                     message_size,
                     /* to */
                     dest_node,
                     SDF_FLSH,
                     /* from */
                     src_node,
                     SDF_RESPONSES,
                     FLSH_REQUEST,
                     &resp_mbx, NULL)) {
        printf("send failure");
    }

    recv_msg = (struct sdf_msg *)fthMboxWait(req_resp_fthmbx);
    if (recv_msg == NULL) {
        return(SDF_FAILURE_MSG_RECEIVE);
    }
    
    /* Check message response */
    error = sdf_msg_get_error_status(recv_msg);
    if (error != SDF_SUCCESS) {
        sdf_dump_msg_error(error, recv_msg);
        sdf_msg_free(recv_msg);
        return SDF_FAILURE;
    }

    recv_pm  = (SDF_protocol_msg_t *)recv_msg->msg_payload;
    if (recv_pm == NULL) {
        sdf_msg_free(recv_msg);
        return(SDF_FAILURE_MSG_RECEIVE);
    }

    rc = recv_pm->status;
    
    // Set the data
    if (recv_pm->status == SDF_SUCCESS) {
        // key has already been loaded via recv_pm->key
        *key         = plat_alloc(recv_pm->key.len + 1);
        plat_assert(*key);
        memcpy(*key, recv_pm->key.key, recv_pm->key.len);
        *key_len    = recv_pm->key.len;
        *exptime    = recv_pm->exptime;
        *createtime = recv_pm->createtime;
        *seqno      = recv_pm->seqno;
        *data_len   = recv_pm->data_size;
        *flags      = recv_pm->flags;
        *data       = plat_alloc(recv_pm->data_size);
        plat_assert(*data);
        memcpy(*data, (void *) recv_pm + sizeof(SDF_protocol_msg_t), recv_pm->data_size);
        /* free data */
        //plat_free(recv_pm->data);
    } else if (recv_pm->status == SDF_OBJECT_UNKNOWN) {
        // key has already been loaded via recv_pm->key
        *key         = plat_alloc(recv_pm->key.len + 1);
        plat_assert(*key);
        memcpy(*key, recv_pm->key.key, recv_pm->key.len);
        *key_len    = recv_pm->key.len;
        *exptime    = recv_pm->exptime;
        *createtime = recv_pm->createtime;
        *seqno      = recv_pm->seqno;
        *flags      = recv_pm->flags;
        *data_len   = 0;
	*data = NULL;
    } else {
        *key_len    = 0;
        *key        = NULL;
        *data_len   = 0;
	*data       = NULL;
    }

    sdf_msg_free(recv_msg);
    
    return rc;
}

static SDF_status_t simple_replicator_sync_remote_container_new(
			SDF_action_init_t * pai,
			SDF_action_state_t *pas,
			vnode_t dest_node, SDF_cguid_t cguid)
{
    qrep_state_t        *ps;
    qrep_node_state_t   *pns;
    it_cursor_t         *it_cursor = 0; 
    SDF_shardid_t        shard;
    void                *resume_cursor;
    struct cursor_data  *cursor_data;
    int                  resume_cursor_size;
    int                  container_index;
    int64_t              clock_skew;
    fthWaitEl_t         *wait;
    int                  mynode;
    int                  i;
    int                  rc;
    int                  rc_done;
    int                  rc_worst;
    uint64_t             done;
    uint32_t             max_iter_cursors;
    uint32_t             n_recovery_mboxes;
    
    /* Only one caller at a time */
    wait = fthLock(&pas->sync_remote_ctnr_lock, 1, NULL);

    ps = &(pai->pcs->qrep_state);
    pns = &(ps->node_state[dest_node]);

    container_index = cguid_to_index(pas, cguid);
    plat_assert(container_index >= 0);

    mynode = sdf_msg_myrank();
    shard = pas->ctnr_meta[container_index].meta.shard;

    /* Use clock skew if we have it, otherwise figure it out */
    if (pns->clock_skew) {
        clock_skew = pns->clock_skew;
    } else {
        clock_skew = rpc_get_clock_skew(pas->next_ctxt,
                                        mynode, dest_node,
                                        shard, cguid, NULL);
        pns->clock_skew = clock_skew;
    }
    
    // Get interation cursors
    // This is single threaded for now

    n_recovery_mboxes = getProperty_uLongInt("SDF_N_SYNC_CONTAINER_THREADS", DEFAULT_NUM_SYNC_CONTAINER_THREADS);
    max_iter_cursors = getProperty_uLongInt("SDF_N_SYNC_CONTAINER_CURSORS", DEFAULT_NUM_SYNC_CONTAINER_CURSORS);

    plat_assert(max_iter_cursors >= MIN_ITERATION_CURSORS);

    resume_cursor_size = 0;
    resume_cursor      = NULL;

    while (SDF_TRUE) {
        rc = rpc_get_iteration_cursors_new(pas, pas->next_ctxt,
                                           mynode, dest_node,
                                           shard, cguid,
                                           0, max_iter_cursors, 0,
                                           resume_cursor, 
					   resume_cursor_size,
                                           &it_cursor);

        if (rc != SDF_SUCCESS) {
	    plat_log_msg(21281, 
	                 PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, 
			 PLAT_LOG_LEVEL_ERROR,
			 "Failed to get iteration cursors node:"
			 "%d rc: %d\n", dest_node, rc);
	    if (it_cursor) {
		plat_free(it_cursor);
	    }
	    break;
	} else if (it_cursor->cursor_count == 0) {
	    plat_log_msg(21282, 
	                 PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, 
			 PLAT_LOG_LEVEL_TRACE,
			 "get iteration complete");
	    if (it_cursor) {
		plat_free(it_cursor);
	    }
	    break;
	} else if (it_cursor->cursor_count < 0) {
	    plat_log_msg(21283, 
	                 PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, 
			 PLAT_LOG_LEVEL_ERROR,
			 "get iteration cursors returned a negative"
			 "cursor count: %d", it_cursor->cursor_count);
	    plat_abort();
	    if (it_cursor) {
		plat_free(it_cursor);
	    }
	    break;
	} else  {
	    plat_assert(it_cursor->cursor_count <= max_iter_cursors);
	    for (i = 0; i < it_cursor->cursor_count; i++) {
                cursor_data             = &(pas->cursor_datas[i]);
		cursor_data->ctxt       = 0;
		cursor_data->shard      = shard;
		cursor_data->dest_node  = dest_node;
		cursor_data->cursor     = (void *)&it_cursor->cursors[i*it_cursor->cursor_len];
		cursor_data->cursor_len = it_cursor->cursor_len;
		cursor_data->cguid      = cguid;
		cursor_data->clock_skew = clock_skew;

		fthMboxPost(&(pas->cursor_mbx_todo[i % n_recovery_mboxes]), (uint64_t) cursor_data);
	    }
	    rc_worst = SDF_SUCCESS;
	    for (i = 0; i < it_cursor->cursor_count; i++) {
		done = fthMboxWait(&(pas->cursor_mbx_done[i % n_recovery_mboxes]));
		if (done == 0) {
		    plat_log_msg(21284, 
			 PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, 
			 PLAT_LOG_LEVEL_ERROR,
			 "cursor_mbx_done returned a NULL value");
		    rc_done = SDF_FAILURE_MBOX;
		} else {
		    rc_done = (int) done;
		}
		if (rc_done != SDF_SUCCESS) {
		    rc_worst = rc_done;
		}
	    }
	}
	if (rc_worst != SDF_SUCCESS) {
	    if (it_cursor) {
		plat_free(it_cursor);
	    }
	    rc = rc_worst;
	    break;
	}

        resume_cursor      = it_cursor;
        resume_cursor_size = sizeof(resume_cursor_t);
    }

    fthUnlock(wait);

    return rc;
}

/*
 * Worker thread to sync a container
 */
static void sync_container_thread_new(uint64_t arg) 
{
    struct cursor_data   *cursor_data;
    SDF_action_state_t   *pas;
    // qrep_state_t         *ps;
    struct ssdaio_ctxt   *paio_ctxt;   

    struct objMetaData    meta;
    struct shard         *pshard;
    SDF_time_t            createtime = 0;
    SDF_time_t            exptime = 0;
    size_t                data_len = 0;
    void                 *data = 0;
    char                 *key = 0;
    uint32_t              flags = 0;
    int                   index;
    int                   key_len = 0;
    int                   mynode;
    int                   rc = 0;
    uint32_t              n_recovery_mboxes;

    pas = (SDF_action_state_t *) arg;
    // ps = &pas->qrep_state;

    mynode = sdf_msg_myrank();

    n_recovery_mboxes = getProperty_uLongInt("SDF_N_SYNC_CONTAINER_THREADS", DEFAULT_NUM_SYNC_CONTAINER_THREADS);
    index = __sync_fetch_and_add(&num_sync_container_threads, 1);

    paio_ctxt = simple_replication_init_ctxt( SSD_AIO_CTXT_SYNC_CNTR_NEW );
    plat_assert(paio_ctxt);

    while (1) {
        cursor_data = (struct cursor_data *) fthMboxWait(&(pas->cursor_mbx_todo[index % n_recovery_mboxes]));
        plat_assert(cursor_data);

        // Get cursor
        rc = rpc_get_by_cursor_new(
		   pas, cursor_data->ctxt, cursor_data->shard, mynode,
		   cursor_data->dest_node, cursor_data->cursor,
		   cursor_data->cursor_len, cursor_data->cguid, &key,
		   &key_len, &exptime, &createtime,
		   &meta.sequence, &data, &data_len, &flags, &(pas->gbc_mbx[index]));
        
        if (rc != SDF_SUCCESS) {
            if (rc == SDF_OBJECT_UNKNOWN || rc == SDF_FLASH_STALE_CURSOR) {
                if (rc == SDF_OBJECT_UNKNOWN) {
                    plat_free(key);
                }

                // these are tolerable & expected 
                rc = SDF_SUCCESS;
            }
        } else {

	    pshard = shardid_to_shard(pas, cursor_data->shard);
	    plat_assert(pshard);
	    
	    /* Adjust for clock skew */
	    if (exptime > 1) {
		meta.expTime = exptime - cursor_data->clock_skew;
	    } else {
		meta.expTime = exptime;
	    }
	    meta.createTime = createtime - cursor_data->clock_skew;

	    meta.keyLen  = key_len;
	    meta.dataLen = data_len;
	    
	    /* Update the cas ID */
	    osd_set_shard_cas_id_if_higher(pshard, data);

	    rc = simple_replication_flash_put(paio_ctxt, pshard, &meta, key, data, FLASH_PUT_TEST_EXIST);

	    if (rc != FLASH_EOK && rc != FLASH_EEXIST) {
		plat_log_msg(21279, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
			     "flash_put failed: key: %s len: %d data: 0x%lx", key, key_len, (uint64_t)data);
		rc = get_status(rc);
	    } else {
		plat_log_msg(21280, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_TRACE,
			     "data copy recv: key: %s len: %d data: 0x%lx rc: %d", key, key_len, (uint64_t)data, rc);
		rc = SDF_SUCCESS;
	    }

	    plat_free(key);
	    plat_free(data);
	}

        /* acknowledge completion */
	fthMboxPost(&(pas->cursor_mbx_done[index % n_recovery_mboxes]), (uint64_t) rc);
    }
}


/*
 * Download all the remote data from a remote container and integrate
 * the needed items
 */
static SDF_status_t simple_replicator_sync_remote_container(SDF_action_init_t * pai,
                                                            SDF_action_state_t *pas,
                                                            vnode_t dest_node, SDF_cguid_t cguid)
{
    qrep_state_t * ps;
    qrep_node_state_t *pns;
    it_cursor_t *it_cursor = 0; 
    SDF_shardid_t shard;
    struct shard *pshard;
    void * resume_cursor = 0;
    volatile struct cursor_data * cursor_data;
    static int cursor_data_count = 0;
    int resume_cursor_size = 0;
    int container_index;
    int64_t clock_skew;
    fthWaitEl_t *wait;
    int max_iter_cursors;
    int complete = 0;
    int index;
    int percentage = 0;
    int mynode;
    int i;
    int rc;
#ifdef KEY_LOCK_CONTAINER
    struct replicator_key_lock_container *lock_container;
#endif /* KEY_LOCK_CONTAINER */
    struct rklc_get *get_lock;
    
    /* Only one caller at a time */
    wait = fthLock(&sync_remote_container_lock, 1, NULL);

    ps = &(pai->pcs->qrep_state);
    pns = &(ps->node_state[dest_node]);

    container_index = cguid_to_index(pas, cguid);
    plat_assert(container_index >= 0);

    mynode = sdf_msg_myrank();
    shard = pas->ctnr_meta[container_index].meta.shard;
#ifdef KEY_LOCK_CONTAINER
    lock_container = pas->ctnr_meta[container_index].lock_container;
    plat_assert(lock_container);
#endif /* KEY_LOCK_CONTAINER */

    /* Use clock skew if we have it, otherwise figure it out */
    if (pns->clock_skew) {
        clock_skew = pns->clock_skew;
    } else {
        clock_skew = rpc_get_clock_skew(pas->next_ctxt,
                                        mynode, dest_node,
                                        shard, cguid, NULL);
        pns->clock_skew = clock_skew;
    }
    
    max_iter_cursors = getProperty_uLongInt("SDF_N_SYNC_CONTAINER_CURSORS", DEFAULT_NUM_SYNC_CONTAINER_CURSORS);

    /* This is the minimum that replication code needs in a transfer */
    plat_assert(max_iter_cursors >= MIN_ITERATION_CURSORS);

    /* Get all of the cursor data */
    for (i = cursor_data_count; i < ps->nctnr_sync_threads; i++) {
        // Wait for a worker thread to indicate it's available
        index = (int)fthMboxWait(&ps->sync_thread_avail_mbox);

        cursor_data_count++;
    }

    // Get interation cursors
    // This is single threaded for now
    do {
        plat_assert(cursor_data_count == ps->nctnr_sync_threads);

#ifdef KEY_LOCK_CONTAINER 
        get_lock = rklc_start_get(lock_container);
        plat_assert(get_lock);
#endif /* KEY_LOCK_CONTAINER */

        rc = rpc_get_iteration_cursors(pas, pas->next_ctxt,
                                       mynode, dest_node,
                                       shard, cguid,
                                       0, max_iter_cursors, 0,
                                       resume_cursor, resume_cursor_size,
                                       &it_cursor);

        // Stop once we have all the cursors or failure
        if (rc != SDF_SUCCESS || !it_cursor->cursor_count) {
            if (it_cursor && !it_cursor->cursor_count) {
                plat_log_msg(21282, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_TRACE,
                             "get iteration complete");
                rc = SDF_SUCCESS;

                complete = 1;

                plat_free(it_cursor);
            } else {
                plat_log_msg(21285, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                             "Failed to get iteration cursors node: %d rc: %d\n", dest_node, rc);
            }

            goto cleanup;
        }

        /* 
         * XXX: drew 2010-03-05 This is a lot more complicated than it 
         * needs to be - the worker threads can all block on the
         * same mailbox and the main loop can avoid blocking on the
         * sync_thread_avail_mbox until it reaches the thread pool 
         * count.
         */

        for (i = 0; i < it_cursor->cursor_count; i++) {

            /* See if we have any available */
            if (cursor_data_count) {
                cursor_data = &cursor_datas[cursor_data_count-1];

                cursor_data_count--;

                index = cursor_data_count;
            } else {
                /* Must go and wait for a worker to come back */
                index = (int)fthMboxWait(&ps->sync_thread_avail_mbox);
                plat_assert(index < ps->nctnr_sync_threads);

                cursor_data = &cursor_datas[index];

                // Check it's previous 
                if (cursor_data->rc == SDF_FAILURE) {
                    plat_log_msg(21286, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                                 "Failed to get cursors\n");
                    rc = -cursor_data->rc;

                    goto cleanup;
                }
            }

            // Setup the data for the worker thread
            cursor_data->ctxt = 0;
            cursor_data->shard = shard;
            cursor_data->dest_node = dest_node;
            cursor_data->cursor = (void *)&it_cursor->cursors[i*it_cursor->cursor_len];
            cursor_data->cursor_len = it_cursor->cursor_len;
            cursor_data->cguid = cguid;
            cursor_data->clock_skew = clock_skew;
#ifdef KEY_LOCK_CONTAINER 
            cursor_data->lock_container = lock_container;
#endif /* KEY_LOCK_CONTAINER */

            fthMboxPost(&cursor_datas[index].mbox, (uint64_t) index);
            
        }

    cleanup:
        /* Must get all cursor data back before do another iteration */
        for (i = cursor_data_count; i < ps->nctnr_sync_threads; i++) {
            index = (int)fthMboxWait(&ps->sync_thread_avail_mbox);
            plat_assert(index < ps->nctnr_sync_threads);

            cursor_data = &cursor_datas[index];

            cursor_data_count++;

            // Check it's previous 
            if (cursor_data->rc == SDF_FAILURE) {
                plat_log_msg(21286, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_ERROR,
                             "Failed to get cursors\n");
                rc = -cursor_data->rc;
            }
        }

        if (get_lock) {
            rklc_get_complete(get_lock);
            get_lock = NULL;
        }

        resume_cursor = it_cursor;
        resume_cursor_size = sizeof(resume_cursor_t);

        pshard = shardid_to_shard(pas, shard);

        // Update data copy progress
        if (complete) {
            percentage = 100;
        } else {
            if (resume_cursor) {
                percentage = sdf_iterate_cursors_progress(pshard, (resume_cursor_t *)resume_cursor);
            }
        }
        update_cntr_data_copy_progress(&(ps->node_state[mynode]), cguid, percentage);
    } while (rc == SDF_SUCCESS && !complete);

    fthUnlock(wait);

    return rc;
}

// Move this to container.c and resolve linking error
int cguid_to_index(SDF_action_state_t * pas, SDF_cguid_t cguid) {    
    int i;
    
    {
        i = cguid;
        if (pas->ctnr_meta[i].cguid == cguid) {
            return i;
        }
    }
    return -1;
}

SDF_cguid_t get_container_guid(qrep_ctnr_state_t *cntnr)
{
#if 0
    /* This functions make sure that, cguid is unique on similar containers 
      on multiple nodes. For example, cguid of container 1(port 10000) of node 1 and cguid of 
      container id 2( port 10000) of node 2 of 2way mirrored group or N+1 will be same*/
    return cntnr->group_id + cntnr->vip_tcp_port;
#else
    return cntnr->id + NODE_MULTIPLIER;
#endif    
}


/**************************************************
 *
 * Remote Procedure Calls for Simple Replication
 *
 ***************************************************/

static sdf_msg_t * rpc_load_msg(SDF_vnode_t node_from, 
                                SDF_vnode_t node_to, 
                                SDF_protocol_msg_type_t msg_type,
                                SDF_size_t data_size, void *pdata,
                                SDF_context_t ctxt, SDF_cguid_t cguid,
                                int seqno_start, int seqno_max,
                                int seqno_len, SDF_size_t * message_size,
                                SDF_shardid_t shard)
{
    SDF_protocol_msg_t * pm_new;
    unsigned char * pdata_msg;
    sdf_msg_t * msg;
    SDF_size_t msize;

    msize = sizeof(SDF_protocol_msg_t);
    if (data_size) {
        msize += data_size;
	msg = (struct sdf_msg *) sdf_msg_alloc(msize);
	if (!msg) {
            plat_log_msg(21133, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "Not enough memory, sdf_msg_alloc() failed.");
	    return(NULL);
	}
        msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;

	pm_new = (SDF_protocol_msg_t *) msg->msg_payload;
	pdata_msg = (unsigned char *) pm_new + sizeof(SDF_protocol_msg_t);
	plat_assert(pdata != NULL);
	memcpy((void *) pdata_msg, pdata, data_size);
	pm_new->data_offset = 0;
    } else {
	msg = (struct sdf_msg *) sdf_msg_alloc(msize);
	if (!msg) {
            plat_log_msg(21133, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_DIAGNOSTIC,
                         "Not enough memory, sdf_msg_alloc() failed.");
	    return(NULL);
	}
        msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;
	pm_new = (SDF_protocol_msg_t *) msg->msg_payload;
	pm_new->data_offset = 0;
    }
    memset(pm_new, 0, sizeof(SDF_protocol_msg_t));

    pm_new->current_version = PROTOCOL_MSG_VERSION;
    pm_new->supported_version = PROTOCOL_MSG_VERSION;

    pm_new->data_size   = data_size;
    pm_new->msgtype     = msg_type;
    pm_new->node_from   = node_from;
    pm_new->node_to     = node_to;
    pm_new->action_node = node_from;
    pm_new->cache       = node_from;
    pm_new->thrd        = ctxt;
    pm_new->cguid       = cguid;
    pm_new->seqno       = seqno_start;
    pm_new->seqno_max   = seqno_max;
    pm_new->seqno_len   = seqno_len;
    pm_new->shard       = shard;
    pm_new->status      = SDF_SUCCESS;

    *message_size = msize;

    return msg;
}

static SDF_status_t rpc_start_replicating(SDF_context_t ctxt, SDF_cguid_t cguid,
                                          vnode_t src_node,
                                          vnode_t dest_node,
                                          SDF_shardid_t shard,
                                          void * data,
                                          size_t * data_len)
{
    struct sdf_msg * recv_msg;
    struct sdf_msg * sdf_msg;
    SDF_protocol_msg_t * recv_pm;
    SDF_size_t message_size;
    sdf_fth_mbx_t resp_mbx;
    fthMbox_t * req_resp_fthmbx;
    SDF_status_t error;
    int rc;
    
    req_resp_fthmbx = plat_alloc(sizeof(fthMbox_t));
    
    fthMboxInit((req_resp_fthmbx));
    resp_mbx.rbox = req_resp_fthmbx;
    resp_mbx.release_on_send = 1;
    resp_mbx.actlvl = SACK_RESP_ONLY_FTH;

    sdf_msg = rpc_load_msg(src_node,
                           dest_node, // to
                           HFSRR,
                           *data_len,
                           data,
                           ctxt, 
                           cguid,
                           0,
                           0,
                           0,
                           &message_size, 
                           shard);

    if (sdf_msg_send(sdf_msg,
                     message_size,
                     /* to */
                     dest_node,
                     SDF_FLSH,
                     /* from */
                     src_node,
                     SDF_RESPONSES,
                     FLSH_REQUEST,
                     &resp_mbx, NULL)) {
        printf("send failure");
    }


    recv_msg = (struct sdf_msg *)fthMboxWait(req_resp_fthmbx);
    if (recv_msg == NULL) {
        return(SDF_FAILURE_MSG_RECEIVE);
    }
    
    plat_free(req_resp_fthmbx);

    /* Check message response */
    error = sdf_msg_get_error_status(recv_msg);
    if (error != SDF_SUCCESS) {
        sdf_dump_msg_error(error, recv_msg);
        sdf_msg_free(recv_msg);
        return SDF_FAILURE;
    }

    recv_pm = (SDF_protocol_msg_t *) (recv_msg->msg_payload);

    rc = recv_pm->status;

    sdf_msg_free(recv_msg);

    return rc;
}

static int64_t rpc_get_clock_skew(SDF_context_t ctxt, vnode_t src_node,
                                  vnode_t dest_node, SDF_shardid_t shard,
                                  SDF_cguid_t cguid, int *rec_ver_ptr)
{
    struct sdf_msg * recv_msg;
    SDF_protocol_msg_t * recv_pm;
    struct sdf_msg * sdf_msg;
    SDF_size_t message_size;
    SDF_status_t error;
    int64_t clock_skew = 0;
    time_t remote_time;

    sdf_fth_mbx_t resp_mbx;
    fthMbox_t *  req_resp_fthmbx;

    req_resp_fthmbx = plat_alloc(sizeof(fthMbox_t));
    
    fthMboxInit(req_resp_fthmbx);
    resp_mbx.rbox = req_resp_fthmbx;
    resp_mbx.release_on_send = 0;
    resp_mbx.actlvl = SACK_RESP_ONLY_FTH;

    sdf_msg = rpc_load_msg(src_node,
                           dest_node, // to
                           HFNOP,
                           0,
                           0,
                           ctxt, 
                           cguid,
                           0,
                           0,
                           0,
                           &message_size, 
                           shard);

    if (sdf_msg_send(sdf_msg,
                     message_size,
                     /* to */
                     dest_node,
                     SDF_FLSH,
                     /* from */
                     src_node,
                     SDF_RESPONSES,
                     FLSH_REQUEST,
                     &resp_mbx, NULL)) {
        printf("send failure");
    }

    recv_msg = (struct sdf_msg *)fthMboxWait(req_resp_fthmbx);
    if (recv_msg == NULL) {
        return(SDF_FAILURE_MSG_RECEIVE);
    }
    
    plat_free(req_resp_fthmbx);

    /* Check message response */
    error = sdf_msg_get_error_status(recv_msg);
    if (error != SDF_SUCCESS) {
        sdf_dump_msg_error(error, recv_msg);
        sdf_msg_free(recv_msg);
        return SDF_FAILURE;
    }

    recv_pm = (SDF_protocol_msg_t *)recv_msg->msg_payload;

    // Set the data
    if (recv_pm->status == SDF_SUCCESS) {
        remote_time = recv_pm->createtime;
        clock_skew = remote_time - sdf_get_current_time();
    }

    /* Return recovery version */
    if (rec_ver_ptr) {
        int     ver = 0;
        int pit_len = sizeof(SDF_protocol_msg_t);
        int end_len = recv_msg->msg_len - sizeof(sdf_msg_t) - pit_len;

        if (end_len >= sizeof(sdf_hfnop_t))
            ver = ((sdf_hfnop_t *)(recv_msg->msg_payload + pit_len))->rec_ver;
        *rec_ver_ptr = ver;
    }

    sdf_msg_free(recv_msg);

    return clock_skew;
}

static SDF_status_t rpc_get_iteration_cursors(SDF_action_state_t *pas, SDF_context_t ctxt, vnode_t src_node, vnode_t dest_node,
                                              SDF_shardid_t shard, SDF_cguid_t cguid, uint64_t seqno_start,
                                              uint64_t seqno_len, uint64_t seqno_max,
                                              void *cursor, int cursor_size,
                                              struct flashGetIterationOutput **out)
{
    struct sdf_msg * recv_msg;
    SDF_protocol_msg_t * recv_pm;
    struct sdf_msg * sdf_msg;
    SDF_size_t message_size = 0;

    SDF_status_t error;
    int rc;

    sdf_fth_mbx_t resp_mbx;
    fthMbox_t *  req_resp_fthmbx;

    req_resp_fthmbx = &(pas->git_mbx);
    
    resp_mbx.rbox = req_resp_fthmbx;
    resp_mbx.abox = NULL;
    resp_mbx.release_on_send = 1;
    resp_mbx.actlvl = SACK_RESP_ONLY_FTH;

    sdf_msg = rpc_load_msg(src_node,
                           dest_node, // to
                           HFGIC,
                           cursor_size,
                           (void *)cursor,
                           ctxt, 
                           cguid,
                           seqno_start,
                           seqno_max,
                           seqno_len,
                           &message_size, 
                           shard);

    if (sdf_msg_send(sdf_msg,
                     message_size,
                     /* to */
                     dest_node,
                     SDF_FLSH,
                     /* from */
                     src_node,
                     SDF_RESPONSES,
                     FLSH_REQUEST,
                     &resp_mbx, NULL)) {
        printf("send failure");
    }

    recv_msg = (struct sdf_msg *)fthMboxWait(req_resp_fthmbx);
    if (recv_msg == NULL) {
        return(SDF_FAILURE_MSG_RECEIVE);
    }
    
    // If used a cursor, make sure to free it
    if (cursor) {
        plat_free(cursor);
    }

    /* Check message response */
    error = sdf_msg_get_error_status(recv_msg);
    if (error != SDF_SUCCESS) {
        sdf_dump_msg_error(error, recv_msg);
        sdf_msg_free(recv_msg);
        return SDF_FAILURE;
    }

    recv_pm = (SDF_protocol_msg_t *)recv_msg->msg_payload;

    rc = recv_pm->status;
    
    // Set the data
    if (recv_pm->status == SDF_SUCCESS) {
        it_cursor_t * it_cursor;        

        *out = plat_alloc(recv_pm->data_size); // This gets freed on
                                               // the subsequent
                                               // iterate cursors call
        plat_assert(*out);
        memcpy(*out, (void *) recv_pm + sizeof(SDF_protocol_msg_t), recv_pm->data_size);

        it_cursor = *out;

        plat_assert(((it_cursor->cursor_count * it_cursor->cursor_len) + sizeof(it_cursor_t)) == recv_pm->data_size);
    } else {
        rc = SDF_FAILURE;
    }

    sdf_msg_free(recv_msg);

    return rc;
}

static SDF_status_t rpc_get_by_cursor(SDF_context_t ctxt, SDF_shardid_t shard, vnode_t src_node,
                                      vnode_t dest_node, void *cursor,
                                      size_t cursor_len, SDF_cguid_t cguid, fthMbox_t *req_resp_fthmbx,
                                      char **key, int *key_len,
                                      SDF_time_t *exptime, SDF_time_t *createtime,
                                      uint64_t *seqno, void **data, size_t *data_len, uint32_t *flags)
{
    struct sdf_msg * recv_msg;
    SDF_protocol_msg_t * recv_pm;
    struct sdf_msg * sdf_msg;
    SDF_size_t message_size;
    SDF_status_t error;
    int rc;

    sdf_fth_mbx_t resp_mbx;
    
    resp_mbx.rbox = req_resp_fthmbx;
    resp_mbx.release_on_send = 1;
    resp_mbx.actlvl = SACK_RESP_ONLY_FTH;

    sdf_msg = rpc_load_msg(src_node,
                           dest_node, // to
                           HFGBC,
                           cursor_len,
                           cursor,
                           ctxt, 
                           cguid,
                           0,
                           0,
                           0,
                           &message_size, 
                           shard);

    if (sdf_msg_send(sdf_msg,
                     message_size,
                     /* to */
                     dest_node,
                     SDF_FLSH,
                     /* from */
                     src_node,
                     SDF_RESPONSES,
                     FLSH_REQUEST,
                     &resp_mbx, NULL)) {
        printf("send failure");
    }

    recv_msg = (struct sdf_msg *)fthMboxWait(req_resp_fthmbx);
    if (recv_msg == NULL) {
        return(SDF_FAILURE_MSG_RECEIVE);
    }
    
    /* Check message response */
    error = sdf_msg_get_error_status(recv_msg);
    if (error != SDF_SUCCESS) {
        sdf_dump_msg_error(error, recv_msg);
        sdf_msg_free(recv_msg);
        return SDF_FAILURE;
    }

    recv_pm  = (SDF_protocol_msg_t *)recv_msg->msg_payload;

    rc = recv_pm->status;
    
    // Set the data
    if (recv_pm->status == SDF_SUCCESS) {
        // key has already been loaded via recv_pm->key
        *key         = plat_alloc(recv_pm->key.len + 1);
        plat_assert(*key);
        memcpy(*key, recv_pm->key.key, recv_pm->key.len);
        *key_len    = recv_pm->key.len;
        *exptime    = recv_pm->exptime;
        *createtime = recv_pm->createtime;
        *seqno      = recv_pm->seqno;
        *data_len   = recv_pm->data_size;
        *flags      = recv_pm->flags;
        *data       = plat_alloc(recv_pm->data_size);
        plat_assert(*data);
        memcpy(*data, (void *) recv_pm + sizeof(SDF_protocol_msg_t), recv_pm->data_size);
        /* free data */
        //plat_free(recv_pm->data);
    } else if (recv_pm->status == SDF_OBJECT_UNKNOWN) {
        // key has already been loaded via recv_pm->key
        *key         = plat_alloc(recv_pm->key.len + 1);
        plat_assert(*key);
        memcpy(*key, recv_pm->key.key, recv_pm->key.len);
        *key_len    = recv_pm->key.len;
        *exptime    = recv_pm->exptime;
        *createtime = recv_pm->createtime;
        *seqno      = recv_pm->seqno;
        *flags      = recv_pm->flags;
    } else {
	*key = 0;
    }

    plat_log_msg(21287, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_TRACE,
                 "rpc_get_by_cursor recv: key: %s status: %d", *key ? *key : "null", rc);


    sdf_msg_free(recv_msg);
    
    return rc;
}

SDF_status_t rpc_release_vip_group(SDF_context_t ctxt, SDF_cguid_t cguid,
                                          vnode_t src_node,
                                          vnode_t dest_node,
                                          SDF_shardid_t shard)
{
    struct sdf_msg * recv_msg;
    struct sdf_msg * sdf_msg;
    SDF_protocol_msg_t * recv_pm;
    SDF_size_t message_size;
    sdf_fth_mbx_t resp_mbx;
    fthMbox_t req_resp_fthmbx;
    SDF_status_t error;
    int rc;
    
    fthMboxInit(&(req_resp_fthmbx));
    resp_mbx.rbox = &req_resp_fthmbx;
    resp_mbx.release_on_send = 0;
    resp_mbx.actlvl = SACK_RESP_ONLY_FTH;

    sdf_msg = rpc_load_msg(src_node,
                           dest_node, // to
                           HFRVG,
                           0, // data len
                           0, // data
                           ctxt, 
                           cguid,
                           0,
                           0,
                           0,
                           &message_size, 
                           shard);

    if (sdf_msg_send(sdf_msg,
                     message_size,
                     /* to */
                     dest_node,
                     SDF_FLSH,
                     /* from */
                     src_node,
                     SDF_RESPONSES,
                     FLSH_REQUEST,
                     &resp_mbx, NULL)) {
        printf("send failure");
    }


    recv_msg = (struct sdf_msg *)fthMboxWait(&req_resp_fthmbx);
    if (recv_msg == NULL) {
        return(SDF_FAILURE_MSG_RECEIVE);
    }
    
    /* Check message response */
    error = sdf_msg_get_error_status(recv_msg);
    if (error != SDF_SUCCESS) {
        sdf_dump_msg_error(error, recv_msg);
        sdf_msg_free(recv_msg);
        return SDF_FAILURE;
    }

    recv_pm = (SDF_protocol_msg_t *) (recv_msg->msg_payload);

    rc = recv_pm->status;

    sdf_msg_free(recv_msg);

    return rc;
}


/*
 * Attempt to copy a container using fast recovery.  If fast recovery is not
 * available, we return -1.  If we succeed, we return 1 and if we fail, we
 * return 0.
 */
static int
fast_copy_ctr(SDF_action_init_t * pai, SDF_action_state_t *pas,
              vnode_t master, SDF_cguid_t cguid)
{
    int rank;
    int index;
    int rem_rec_ver;
    SDF_shardid_t id;
    struct shard *shard;

    if (!sdf_rec_funcs)
        return -1;

    index = cguid_to_index(pas, cguid);
    if (index < 0)
        return -1;

    id = pas->ctnr_meta[index].meta.shard;
    rank = sdf_msg_myrank();
    rpc_get_clock_skew(pas->next_ctxt, rank, master, id, cguid, &rem_rec_ver);
    if (!rem_rec_ver)
        return -1;

    shard = shardid_to_shard(pas, id);
    return sdf_rec_funcs->ctr_copy(master, shard, pai);
}
