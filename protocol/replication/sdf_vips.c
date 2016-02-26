/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/protocol/replication/sdf_vips.c
 *
 * Author: Mana
 *
 * Created on July 24, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 */
#include "platform/alloc.h"
#include "platform/logging.h"
#include "platform/stdlib.h"
#include "common/sdftypes.h"
#include "sdf_vips.h"

#define LOG_CAT PLAT_LOG_CAT_SDF_PROT_REPLICATION_VIPS

/**
 *@brief Perform deep copy of sdf_config
 *@return sdf_vip_group, NULL on failure
 */
struct sdf_vip_config *sdf_vip_config_copy(const struct sdf_vip_config *rhs) {
    struct sdf_vip_config *lhs;
    int i,j;

    lhs = plat_alloc(sizeof(struct sdf_vip_config));
    if( lhs == NULL ) {
        plat_log_msg(20813, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                     "Unable to allocate mem for sdf_vip_config\n");
        return NULL;
    }
    memcpy((void *)lhs,(void *)rhs,sizeof(struct sdf_vip_config));
    lhs->ggroups = plat_alloc( lhs->num_vip_group_groups * sizeof(sdf_vip_group_group_t));
    if( lhs->ggroups == NULL ) {
        plat_log_msg(20814, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                     "Unable to allocate mem for vip_group groups\n");
        plat_abort();
    }
    for( i = 0;  i < lhs->num_vip_group_groups; i++ ) {
         memcpy( &(lhs->ggroups[i]), &(rhs->ggroups[i]),sizeof(sdf_vip_group_group_t));
         lhs->ggroups[i].groups = plat_alloc( lhs->ggroups[i].num_groups * sizeof(sdf_vip_group_t));
         if( lhs->ggroups[i].groups == NULL ) {
             plat_log_msg(21482, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                     "Unable to allocate mem for vip_group \n");
             plat_abort();
         }
         for( j = 0; j < lhs->ggroups[i].num_groups; j++ ) {
             memcpy( &(lhs->ggroups[i].groups[j]), &(rhs->ggroups[i].groups[j]), sizeof(sdf_vip_group_t));
             lhs->ggroups[i].groups[j].nodes_pref_list = plat_alloc( lhs->ggroups[i].groups[j].num_nodes * sizeof(vnode_t));
             if( lhs->ggroups[i].groups[j].nodes_pref_list == NULL ) {
                 plat_log_msg(21483, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                     "Unable to allocate mem pref nodes\n");
             }
             memcpy(lhs->ggroups[i].groups[j].nodes_pref_list, rhs->ggroups[i].groups[j].nodes_pref_list,
                                            lhs->ggroups[i].groups[j].num_nodes * sizeof(vnode_t));
             
         }
    }
    return lhs;
}

/**
 * @brief Free sdf_config structure
 */
void sdf_vip_config_free_vip_group(struct sdf_vip_group *vgroup ) {
    if( vgroup && vgroup->nodes_pref_list ) {
        plat_free(vgroup->nodes_pref_list);    
    }
    return;
}

/**
 * @brief Free sdf_config structure
 */
void sdf_vip_config_free_vip_group_group(struct sdf_vip_group_group *ggroup ) {
    int i;
    if( ggroup && (ggroup->num_groups > 0 ) ) {
        for( i = 0;  i < ggroup->num_groups; i++) {
            sdf_vip_config_free_vip_group( &(ggroup->groups[i]));
        }
        plat_free(ggroup->groups);    
    }
    return;
}

/**
 * @brief Free sdf_config structure
 */
void sdf_vip_config_free(struct sdf_vip_config * config) {
    int i;
    if( config && (config->num_vip_group_groups > 0 )) {
       for( i = 0 ; i < config->num_vip_group_groups; i++ ) {
           sdf_vip_config_free_vip_group_group( &(config->ggroups[i]));
       }
       plat_free(config->ggroups);
    }
    plat_free(config);
    return;
}

/** 
 * @brief Get vip_group_group by group_group_id from config structure
 * @return sdf_vip_group_group owned by config on success, NULL on failure
 */
struct sdf_vip_group_group *
sdf_vip_config_get_group_group_by_ggid(const struct sdf_vip_config *config, int group_group_id) {
    if( group_group_id >= config->num_vip_group_groups ) {
        return NULL;
    }
    return  &(config->ggroups[group_group_id]);
}

/**
 * @brief Get vip_group_group by vip_group_id from config structure
 * @return sdf_vip_group_group owned by config on success, NULL on failure
 */ 
struct sdf_vip_group_group *
sdf_vip_config_get_group_group_by_gid(const struct sdf_vip_config *config, int vip_group_id) {
    struct sdf_vip_group_group *ggrp;
    int i, j;

    for( i = 0; i < config->num_vip_group_groups; i++ ) {
        ggrp = &(config->ggroups[i]);
        for( j = 0 ; j < ggrp->num_groups; j++ ) {
            if( ggrp->groups[j].vip_group_id  == vip_group_id ) {
                return ggrp;
            }
        }
    }
    return NULL;
}

/**
 * @brief Get vip_group by vip_group_id from config structure
 * @return sdf_vip_group owned by config on success, NULL on failure
 */
struct sdf_vip_group *
sdf_vip_config_get_vip_group(const struct sdf_vip_config *config, int vip_group_id) {
    struct sdf_vip_group_group *ggrp;
    struct sdf_vip_group *vgrp=NULL;
    int i;

    ggrp = sdf_vip_config_get_group_group_by_gid(config,vip_group_id);
    if( ggrp == NULL) {
        plat_log_msg(20812, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
          "No Vip group group for vid:%d\n",vip_group_id);
        return NULL;
    }
    for ( i = 0; i < ggrp->num_groups; i++ ) {
        if( ggrp->groups[i].vip_group_id == vip_group_id) {
            vgrp = &(ggrp->groups[i]);
        }
    }
    return vgrp;
}

/**
 * @brief Get all vip_group Ids in the cluster
 * @return number of VIP groups owned by config on success, NULL on failure
 */
int 
sdf_vip_config_get_num_vip_groups(const struct sdf_vip_config *config, int **vip_group_id_array) {
    struct sdf_vip_group_group *ggrp;
    int i, j, vgroup_index=0, *vidarr;

    vidarr =  plat_alloc(config->num_vip_groups * sizeof(int));
    if( vidarr == NULL ) {
        plat_log_msg(21484, LOG_CAT, PLAT_LOG_LEVEL_FATAL, "Unable to alloccate memory for vip_group_id_array\n");
         return 0; 
    }
    for( i = 0; i < config->num_vip_group_groups; i++ ) {
        ggrp = &(config->ggroups[i]);
        for( j = 0 ; j < ggrp->num_groups; j++ ) {
            vidarr[vgroup_index] = ggrp->groups[j].vip_group_id;
            vgroup_index++;
        }
    }
    *vip_group_id_array = vidarr;
    return config->num_vip_groups;
}

/**
 * @brief Get preference for given pnode
 * @return Preference for given node with 0 as highest, -1 on error (node
 * not in list).
 */
int
sdf_vip_group_get_node_preference(const struct sdf_vip_group *group,
                                  vnode_t pnode) {
    int i;
    for( i = 0; i < group->num_nodes ; i++ ) {
        if( group->nodes_pref_list[i] == pnode ) {
            plat_log_msg(80016, LOG_CAT, PLAT_LOG_LEVEL_TRACE, 
                    "sdf_vip_group_get_node_preference for %d(grpid:%d) = %d\n",
                                                   pnode,group->vip_group_id,i);
            return i;
        }
    }
    plat_log_msg(80017, LOG_CAT, PLAT_LOG_LEVEL_WARN, 
     "sdf_vip_group_get_node_preference FOR %d(grpid:%d) = %d\n",pnode,
                                                  group->vip_group_id,-1);
    return -1;
}

int
sdf_vip_group_get_node_rank(const struct sdf_vip_group *group, vnode_t pnode) {
    int i;
    int less_than;
    for (i = 0, less_than = 0; i < group->num_nodes; ++i) {
        if (group->nodes_pref_list[i] < pnode) {
            ++less_than;
        }
    }
    plat_log_msg(80014, LOG_CAT, PLAT_LOG_LEVEL_TRACE, 
             "sdf_vip_group_get_node_rank for %d = %d\n",pnode,less_than);
    return (less_than < group->num_nodes ? less_than : -1);
}

vnode_t 
sdf_vip_group_get_node_by_preference(const struct sdf_vip_group *group, int n) {
    if ( (n < 0)  ||  (n >= group->num_nodes) ) {
        plat_log_msg(80015, LOG_CAT, PLAT_LOG_LEVEL_WARN, 
           "sdf_vip_group_get_node_by_preference for %d(grp:%d) = %d\n",n, 
                                    group->vip_group_id,SDF_ILLEGAL_PNODE);
        return SDF_ILLEGAL_PNODE;
    } else {
        plat_log_msg(80015, LOG_CAT, PLAT_LOG_LEVEL_TRACE, 
           "sdf_vip_group_get_node_by_preference for %d(grp:%d) = %d\n",n,
                            group->vip_group_id,group->nodes_pref_list[n]);
        return group->nodes_pref_list[n];
    }
}
