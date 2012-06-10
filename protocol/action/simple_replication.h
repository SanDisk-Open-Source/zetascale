/*
 * File:   simple_replication.h
 * Author: Brian O'Krafka
 *
 * Created on June 5, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: simple_replication.h 802 2008-03-29 00:44:48Z briano $
 */

#ifndef _SIMPLE_REPLICATION_H
#define _SIMPLE_REPLICATION_H

#include "common/sdftypes.h"
#include "fth/fthMbox.h"
#include  "protocol/action/tlmap4.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int SDFSimpleReplication; // non-zero if simple replication is on

typedef enum {
    qr_persist = 1,
    qr_evict   = 2,
    qr_standby = 4,
    qr_vip     = 8,
    qr_loaded  = 16,
} qrep_ctnr_flags;

typedef enum {
    qr_spare = 1,
    qr_live  = 2,
} qrep_node_flags;

#define QREP_MAX_NODES                1000
#define QREP_MAX_CONTAINERS_PER_NODE  SDF_MAX_CONTAINERS
#define QREP_MAX_REPLICAS             2
#define QREP_MAX_IFS_PER_NODE         100 
#define MAX_VIP_GROUPS_PER_NODE 2 /*For mirrored case only. For N+1 only one VIP grps per node*/
#define MULTI_INST_MCD_VPORT_OFFSET 1000 /* Used for running multiple memcacheds in same machine */

#define MIN_ITERATION_CURSORS 48 /* FIFO bucket size + overflow table
                                  * gives us this restriction */
typedef enum qrep_recovery_type {
    QREP_RECOVERY_TYPE_ALL,
    QREP_RECOVERY_TYPE_NON_PERSISTENT_ONLY,
    QREP_RECOVERY_TYPE_PERSISTENT_ONLY,
}QREP_RECOVERY_TYPE;

typedef enum qrep_container_state {
    QREP_CTNR_STATE_STANDBY,
    QREP_CTNR_STATE_ACTIVE,
    QREP_CTNR_STATE_ACTIVE_REPLICATING,
    QREP_CTNR_STATE_RECOVERING,
    QREP_CTNR_STATE_STOPPED
}QREP_CTNR_STATE;

typedef enum qrep_node_type {
    QREP_NODE_TYPE_STANDBY,
    QREP_NODE_TYPE_ACTIVE,
    QREP_NODE_TYPE_RECOVERING,
    QREP_NODE_TYPE_CLONE,
    QREP_NODE_TYPE_ACTIVE_CLONING,
}QREP_NODE_TYPE;

typedef struct rep_cntl_msg {
    int version;
    int direction[2];
    int cntr_id;
    int cntr_status;
}rep_cntl_msg_t;


typedef enum qrep_vip_type {
    //QREP_VIP_TYPE_UNKNOWN,
    //QREP_VIP_TYPE_PRIMARY,
    //QREP_VIP_TYPE_SECONDARY,
    QREP_VIP_SUBNET_TYPE_UNKNOWN,
    QREP_VIP_SUBNET_TYPE_IFACE,
    QREP_VIP_SUBNET_TYPE_OWN
}QREP_VIP_SUBNET_TYPE;

typedef struct qrep_ctnr_iface {
    char name[32];
    char ip[32];
    char mask[32];
    char gw[32];
    int  rule_table_id; /*Not a config Item*/
    QREP_VIP_SUBNET_TYPE vip_subnet_type;
    int temp;  /*Temp varaible to pass args among threads*/
} qrep_ctnr_iface_t;

typedef struct qrep_cluster_grp {
    int grp_id;
    SDF_cluster_grp_type_t type;
    int num_nodes;
    int16_t *nodes;
} qrep_cluster_grp_t;

typedef struct qrep_ctnr_state {
    int32_t           id;
    SDF_cguid_t       cguid;
    int32_t           global_index;
    uint32_t          gb;
    uint32_t          max_objs;
    int32_t           standby_node;
    int32_t           standby_container;
    uint32_t          nreplicas;
    int32_t           replica_node[QREP_MAX_REPLICAS];
    uint32_t          flags;
    SDF_vnode_t       replicating_to_node;
    char             *vip_addr;
    uint32_t          vip_mask;
    uint32_t          vip_tcp_port;
    uint32_t          vip_udp_port;
    char              vip_if_id[64];

  
    uint8_t           num_vgrps;   /*In N+1, container always has one vip group
                                     In Mirroed group, a container can have two vip groups when a node fails*/
    uint8_t           num_vips[2];  /*Number of virtual Interfaces the container boundTo*/
    qrep_ctnr_iface_t vip_info[2][QREP_MAX_IFS_PER_NODE];
    char              name[128];
    QREP_CTNR_STATE   ctnr_state;
    int  node_id;
    int  group_id;
    uint16_t   rec_prog;
} qrep_ctnr_state_t;

typedef struct qrep_node_state {
    uint32_t            flags;
    uint32_t            nctnrs_node;
    uint32_t            live;
    qrep_ctnr_state_t  *cntrs;
   
    fthLock_t           lock;

    int  group_id;
    int  node_id;
    char host_name[128];
    uint8_t num_ifs;  /*Number IPs that the node is configured. The SDF Messaging
                         Will use this to assign node id*/
    qrep_ctnr_iface_t ifInfo[QREP_MAX_IFS_PER_NODE];
    int num_vips;
    qrep_ctnr_iface_t vipgroup[QREP_MAX_IFS_PER_NODE];
    QREP_NODE_TYPE type; /*Dynamic type info*/
    int num_vgrps_being_serviced;
    int serviced_vgrp_ids[MAX_VIP_GROUPS_PER_NODE];/*For Now Maximum 2*/
    int is_virtual; /* This field is boolen and set to 1 if single node,
                          multi instance memcached support is required*/
    char datarecstat[128]; /* Status info on data recovery */
    int rec_flag;          /* used to remember whether recovery completed */
    int rec_count;
    int persistent_auth;
    time_t clock_skew;
} qrep_node_state_t;

typedef struct qrep_state {
    int                 nnodes;
    int                 ndistinct_ctnrs;
    qrep_node_state_t  *node_state;
    qrep_ctnr_state_t  *ctnr_state;
    SDFTLMap4_t         ctnr_id_map;

    int                 failback;
    int                 nctnr_sync_threads;
    uint32_t            status_port; 
    uint32_t            cluster_id;
    uint32_t            num_groups; 
    uint32_t            enable_ipf; /*This is set 1 if the cluster has atleat a 
                                     N+1 cluster or Mirrored cluster*/
    qrep_cluster_grp_t *groups;
    char                cluster_name[128];
    struct sdf_vip_config *vip_config;
    fthMbox_t           sync_thread_avail_mbox;
} qrep_state_t;

struct SDF_action_state;
struct SDF_action_init;
struct request;
struct conn;
struct ssdaio_ctxt;
struct resume_cursor;
struct shard;
struct flashDev;
struct flashGetIterationResumeOutput;
struct objMetaData;

extern void simple_replication_init(struct SDF_action_state *pas);
extern int simple_replicator_start_new_replica(struct SDF_action_init * pai, vnode_t master, vnode_t slave, int cntr_id, QREP_RECOVERY_TYPE rtype);
extern int start_clone_to_node(struct SDF_action_init  * pai, int cnode);
extern int start_persistent_recovery(struct SDF_action_init * pai);
extern int stop_clone_to_node(struct SDF_action_init  * pai, int cnode);
extern SDF_status_t simple_replicator_enable_replication(struct SDF_action_init * pai, SDF_cguid_t cguid, vnode_t node);
extern SDF_status_t simple_replicator_enable_node(struct SDF_action_state * pas, SDF_cguid_t cguid, vnode_t node);
extern SDF_status_t simple_replicator_disable_replication(struct SDF_action_state * pas, SDF_cguid_t cguid, vnode_t node);
extern SDF_status_t simple_replicator_remove_vip_group(struct SDF_action_init * pai, vnode_t node);
extern int simple_replicator_replicate_to_node(struct SDF_action_init * pai, vnode_t node);
extern void SDFSetContainerStatus(SDF_internal_ctxt_t *pai, int cid, int status );
extern int SDFGetPeerNodeId(int mynode);

extern int partner_replica_by_cguid(qrep_state_t * ps, SDF_cguid_t cguid);
extern int partner_replica_by_container_id(qrep_state_t * ps, int container_id);
extern int partner_replica_port_by_cguid(qrep_state_t * ps, SDF_cguid_t cguid);
extern SDF_cguid_t port_to_cguid(struct SDF_action_init * pai, int port);
extern int node_is_alive(qrep_state_t * ps, int node);
extern void update_data_copy_progress(struct shard * pshard, int percentage);
extern SDF_boolean_t (*sdf_is_node_started_in_auth_mode)(void);
extern SDF_boolean_t (*sdf_is_node_started_first_time)(void);
extern int (*sdf_start_simple)(int);
extern void (*sdf_notify_simple)(uint64_t, int);
extern void (*sdf_simple_dead)(int, int);
extern int (*simple_replication_flash_put) (struct ssdaio_ctxt *, struct shard *, struct objMetaData *, char *, char *, int);
extern struct SDF_action_init *(*sdf_action_init_ptr)(void);
extern struct ssdaio_ctxt * (*simple_replication_init_ctxt) ( int category );
extern struct shard *(*simple_replication_shard_find) (struct flashDev *, uint64_t);
extern int (*sdf_iterate_cursors_progress)(struct shard *, struct flashGetIterationResumeOutput *);
extern void (*simple_replication_set_cas_id_high_watermark)(void *, void *);
extern int  (*sdf_mcd_format_container_internal)( void *, int );
extern int (*sdf_mcd_start_container_internal)( void *, int );
extern int (*sdf_mcd_stop_container_internal)( void *, int );
extern int  (*sdf_mcd_format_container_byname_internal)( void *, char * );
extern int (*sdf_mcd_start_container_byname_internal)( void *, char * );
extern int (*sdf_mcd_stop_container_byname_internal)( void *, char * );
extern int (*sdf_mcd_get_tcp_port_by_cguid)( SDF_cguid_t, int *);
extern int (*sdf_mcd_get_cname_by_cguid)( SDF_cguid_t, char *);
extern int (*sdf_mcd_is_container_running)(int);
extern int (*sdf_mcd_is_container_running_byname)(char *);
extern int (*sdf_mcd_processing_container_commands)(void);
#ifdef	__cplusplus
}
#endif

#endif /* _SIMPLE_REPLICATION_H */
