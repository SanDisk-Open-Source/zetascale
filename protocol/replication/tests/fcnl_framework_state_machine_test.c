/*
 * File: $URL: svn://svn.schoonerinfotech.net/schooner-trunk/trunk/sdf/protocol/replication/tests/fcnl_framework_state_machine_test.c $
 * Author: Manavalan Krishnan
 *
 * Created on Aug 11, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http:// www.schoonerinfotech.com/
 *
 * $Id: fcnl_framework_state_machine_test.c 13389 2010-05-03 12:40:01Z drew $
 */

/* Test switch-over, recovery, switch-back */

#include "fth/fthOpt.h"
#include "platform/stdio.h"
#include "test_framework.h"

#undef MILLION
#define MILLION 1000000

#define RT_USE_COMMON 1
#include "test_common.h"



/*
 * We use a sub-category under test because test implies a huge number
 * of log messages out of simulated networking, flash, etc.
 *
 */
PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION, "test/case");

#define PLAT_OPTS_NAME(name) name ## _state_machine_test
#include "platform/opts.h"
#include "platform/opts.h"
#include "protocol/replication/sdf_vips.h"
#include "misc/misc.h"

#define PLAT_OPTS_ITEMS_state_machine_test() \
    PLAT_OPTS_COMMON_TEST(common_config)                                       \
    item("2way", "2way mode", MODE_2WAY,                                       \
         ({ config->common_config.test_config.replication_type =               \
          SDF_REPLICATION_V1_2_WAY; 0; }), PLAT_OPTS_ARG_NO)                   \
    item("nplus1", "nplus1 mode", MODE_NPLUS1,                                 \
         ({ config->common_config.test_config.replication_type =               \
          SDF_REPLICATION_V1_N_PLUS_1; 0; }), PLAT_OPTS_ARG_NO)                \
    item("new_tests", "run new tests", NEW_TESTS,                              \
         ({ new_tests = 1; 0; }), PLAT_OPTS_ARG_NO)

#define SLEEP_US 1000000

SDF_shardid_t initial_shard_id = 100;
sdf_vip_config_t *vip_cfg;
void print_node_list(int num_nodes);
void create_shard(struct replication_test_framework *test_framework, int node, int vipgrpgrp);
struct replication_test_framework *test_framework_global = NULL;

struct plat_opts_config_state_machine_test {
    struct rt_common_test_config common_config;
};

typedef enum {
    NODE_TYPE_STANDBY,
    NODE_TYPE_RECOVERING,
    NODE_TYPE_ACTIVE,
}NODE_TYPE;

typedef enum {
    NODE_STATUS_DOWN,
    NODE_STATUS_UP,
}NODE_STATUS;

typedef enum {
    NODE_ADM_STATE_DISABLED,
    NODE_ADM_STATE_ENABLED,
}NODE_ADM_STATE;

typedef struct cluster_node {
    NODE_ADM_STATE adm_state; /* Enabled or disabled */
    NODE_TYPE node_type; /* Active or stand by */
    int grp_type;
    NODE_STATUS status;  /* UP or DOWN */
    int maxgrps;
    int num_vip_grps;    /* Number of vip groupd being serviced */
    int vipgrp_srvcd[2]; /* Vip group currently being service by this node */
    struct timeval expires;
} cluster_node_t;

typedef struct vipgroup_stat {
    int usage_count;
} vipgroup_stat_t;

enum {
    MAX_NODES = 10,
    MAX_GROUP_GROUPS = 10
};

static cluster_node_t my_nodes[MAX_NODES];
/* Indexed by node, vip group group */
static SDF_shardid_t shardids[MAX_NODES][MAX_GROUP_GROUPS];

/* Run new tests */
static int new_tests;

sdf_vip_config_t *build_test_sdf_vip_config_2way() {
    sdf_vip_config_t  *vipcfg;

    vipcfg = plat_alloc(sizeof(struct sdf_vip_config));
    if (vipcfg == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for sdf_vip_config");
        plat_abort();
    }
    vipcfg->num_vip_groups = 0;
    vipcfg->num_vip_group_groups = 1;
    vipcfg->ggroups = plat_alloc(vipcfg->num_vip_group_groups * sizeof(sdf_vip_group_group_t));
    if (vipcfg->ggroups == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for vip_group groups");
        plat_abort();
    }
    /* Build 2 way configuration */
    /* Group Group Config */
    vipcfg->ggroups[0].group_group_id = 0;
    vipcfg->ggroups[0].type = SDF_CLUSTER_GRP_TYPE_MIRRORED;
    vipcfg->ggroups[0].max_group_per_node = 2;
    vipcfg->ggroups[0].num_groups = 2;
    vipcfg->ggroups[0].vgid_start_index = 0;
    /* Group Config Group 0(node0) */
    vipcfg->ggroups[0].groups = plat_alloc(vipcfg->ggroups[0].num_groups *
                                           sizeof(sdf_vip_group_t));
    if (vipcfg->ggroups[0].groups == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for vip_groups for group:%d", 0);
        plat_abort();
    }
    vipcfg->ggroups[0].groups[0].vgid_start_index = 0;
    vipcfg->ggroups[0].groups[0].vip_group_id = 0;
    vipcfg->ggroups[0].groups[0].group_group_id = 0;
    vipcfg->ggroups[0].groups[0].num_nodes = 2;
    vipcfg->ggroups[0].groups[0].nodes_pref_list = plat_alloc(2 * sizeof(vnode_t));
    if (vipcfg->ggroups[0].groups[0].nodes_pref_list == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for pref List for vip group :%d",
                     0);
        plat_abort();
    }
    vipcfg->ggroups[0].groups[0].nodes_pref_list[0] = 0;
    vipcfg->ggroups[0].groups[0].nodes_pref_list[1] = 1;
    vipcfg->num_vip_groups++;
    /* Group Config Group 1(node1) */
    vipcfg->ggroups[0].groups[1].vgid_start_index = 0;
    vipcfg->ggroups[0].groups[1].vip_group_id = 1;
    vipcfg->ggroups[0].groups[1].group_group_id = 0;
    vipcfg->ggroups[0].groups[1].num_nodes = 2;
    vipcfg->ggroups[0].groups[1].nodes_pref_list = plat_alloc(2 * sizeof(vnode_t));
    if (vipcfg->ggroups[0].groups[1].nodes_pref_list == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for pref List for vip group :%d",
                     0);
        plat_abort();
    }
    vipcfg->ggroups[0].groups[1].nodes_pref_list[0] = 1;
    vipcfg->ggroups[0].groups[1].nodes_pref_list[1] = 0;
    vipcfg->num_vip_groups++;
    return (vipcfg);
}


sdf_vip_config_t *build_test_sdf_vip_config_nplus1() {
    sdf_vip_config_t  *vipcfg;

    vipcfg = plat_alloc(sizeof(struct sdf_vip_config));
    if (vipcfg == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for sdf_vip_config");
        plat_abort();
    }
    vipcfg->num_vip_groups = 0;
    vipcfg->num_vip_group_groups = 1;
    vipcfg->ggroups = plat_alloc(vipcfg->num_vip_group_groups *
                                 sizeof(sdf_vip_group_group_t));
    if (vipcfg->ggroups == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for vip_group groups");
        plat_abort();
    }
    /* Build N+1 configuration */
    /* Build 2 way configuration */
    /* Group Group Config */
    vipcfg->ggroups[0].group_group_id = 0;
    vipcfg->ggroups[0].vgid_start_index = 0;
    vipcfg->ggroups[0].type = SDF_CLUSTER_GRP_TYPE_NPLUS1;
    vipcfg->ggroups[0].max_group_per_node = 1;
    vipcfg->ggroups[0].num_groups = 2;
    /* Group Config Group 0(node0) */
    vipcfg->ggroups[0].groups =
        plat_alloc(vipcfg->ggroups[0].num_groups * sizeof(sdf_vip_group_t));
    if (vipcfg->ggroups[0].groups == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for vip_groups for group:%d", 0);
        plat_abort();
    }
    /* Group 2(node 2) */
    vipcfg->ggroups[0].groups[0].vgid_start_index = 0;
    vipcfg->ggroups[0].groups[0].vip_group_id = 0;
    vipcfg->ggroups[0].groups[0].group_group_id = 0;
    vipcfg->ggroups[0].groups[0].num_nodes = 3;
    vipcfg->ggroups[0].groups[0].nodes_pref_list =
        plat_alloc(vipcfg->ggroups[0].groups[0].num_nodes * sizeof(vnode_t));
    if (vipcfg->ggroups[0].groups[0].nodes_pref_list == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for pref List for vip group :%d",
                     0);
        plat_abort();
    }
    vipcfg->ggroups[0].groups[0].nodes_pref_list[0] = 0;
    vipcfg->ggroups[0].groups[0].nodes_pref_list[1] = 2;
    vipcfg->ggroups[0].groups[0].nodes_pref_list[2] = 1;
    vipcfg->num_vip_groups++;

    /* Group 3(node 3) */

    vipcfg->ggroups[0].groups[1].vgid_start_index = 0;
    vipcfg->ggroups[0].groups[1].vip_group_id = 1;
    vipcfg->ggroups[0].groups[1].group_group_id = 0;
    vipcfg->ggroups[0].groups[1].num_nodes = 3;
    vipcfg->ggroups[0].groups[1].nodes_pref_list = plat_alloc(vipcfg->ggroups[0].groups[0].num_nodes * sizeof(vnode_t));
    if (vipcfg->ggroups[0].groups[1].nodes_pref_list == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for pref List for vip group :%d",
                     0);
        plat_abort();
    }
    vipcfg->ggroups[0].groups[1].nodes_pref_list[2] = 0;
    vipcfg->ggroups[0].groups[1].nodes_pref_list[0] = 2;
    vipcfg->ggroups[0].groups[1].nodes_pref_list[1] = 1;
    vipcfg->num_vip_groups++;
    return (vipcfg);
}

sdf_vip_config_t *build_test_sdf_vip_config_2way_nplus1() {
    sdf_vip_config_t  *vipcfg;

    vipcfg = plat_alloc(sizeof(struct sdf_vip_config));
    if (vipcfg == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for sdf_vip_config");
        plat_abort();
    }
    vipcfg->num_vip_groups = 0;
    vipcfg->num_vip_group_groups = 2;
    vipcfg->ggroups = plat_alloc(vipcfg->num_vip_group_groups * sizeof(sdf_vip_group_group_t));
    if (vipcfg->ggroups == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for vip_group groups");
        plat_abort();
    }

    /* Build 2 way configuration */
    /* Group Group Config */
    vipcfg->ggroups[0].group_group_id = 0;
    vipcfg->ggroups[0].type = SDF_CLUSTER_GRP_TYPE_MIRRORED;
    vipcfg->ggroups[0].max_group_per_node = 2;
    vipcfg->ggroups[0].num_groups = 2;
    vipcfg->ggroups[0].vgid_start_index  = 0;
    /* Group Config Group 0(node0) */
    vipcfg->ggroups[0].groups =
        plat_alloc(vipcfg->ggroups[0].num_groups * sizeof(sdf_vip_group_t));
    if (vipcfg->ggroups[0].groups == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for vip_groups for group:%d", 0);
        plat_abort();
    }
    vipcfg->ggroups[0].groups[0].vgid_start_index = 0;
    vipcfg->ggroups[0].groups[0].vip_group_id = 0;
    vipcfg->ggroups[0].groups[0].group_group_id = 0;
    vipcfg->ggroups[0].groups[0].num_nodes = 2;
    vipcfg->ggroups[0].groups[0].nodes_pref_list =
        plat_alloc(2 * sizeof(vnode_t));
    if (vipcfg->ggroups[0].groups[0].nodes_pref_list == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for pref List for vip group :%d",
                     0);
        plat_abort();
    }
    vipcfg->ggroups[0].groups[0].nodes_pref_list[0] = 0;
    vipcfg->ggroups[0].groups[0].nodes_pref_list[1] = 1;
    vipcfg->num_vip_groups++;

    /* Group Config Group 1(node1) */
    vipcfg->ggroups[0].groups[1].vgid_start_index = 0;
    vipcfg->ggroups[0].groups[1].vip_group_id = 1;
    vipcfg->ggroups[0].groups[1].group_group_id = 0;
    vipcfg->ggroups[0].groups[1].num_nodes = 2;
    vipcfg->ggroups[0].groups[1].nodes_pref_list = plat_alloc(2 * sizeof(vnode_t));
    if (vipcfg->ggroups[0].groups[1].nodes_pref_list == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for pref List for vip group :%d",
                     0);
        plat_abort();
    }
    vipcfg->ggroups[0].groups[1].nodes_pref_list[0] = 1;
    vipcfg->ggroups[0].groups[1].nodes_pref_list[1] = 0;
    vipcfg->num_vip_groups++;
    /* Build N+1 configuration */
    /* Build 2 way configuration */
    /* Group Group Config */
    vipcfg->ggroups[1].group_group_id = 1;
    vipcfg->ggroups[1].vgid_start_index  = 2;
    vipcfg->ggroups[1].type = SDF_CLUSTER_GRP_TYPE_NPLUS1;
    vipcfg->ggroups[1].max_group_per_node = 1;
    vipcfg->ggroups[1].num_groups = 2;
    /* Group Config Group 0(node0) */
    vipcfg->ggroups[1].groups =
        plat_alloc(vipcfg->ggroups[1].num_groups * sizeof(sdf_vip_group_t));
    if (vipcfg->ggroups[1].groups == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for vip_groups for group:%d", 0);
        plat_abort();
    }
    /* Group 2(node 2) */
    vipcfg->ggroups[1].groups[0].vgid_start_index = 2;
    vipcfg->ggroups[1].groups[0].vip_group_id = 2;
    vipcfg->ggroups[1].groups[0].group_group_id = 1;
    vipcfg->ggroups[1].groups[0].num_nodes = 3;
    vipcfg->ggroups[1].groups[0].nodes_pref_list = plat_alloc(vipcfg->ggroups[1].groups[0].num_nodes * sizeof(vnode_t));
    if (vipcfg->ggroups[1].groups[0].nodes_pref_list == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for pref List for vip group :%d",
                     0);
        plat_abort();
    }
    vipcfg->ggroups[1].groups[0].nodes_pref_list[0] = 0;
    vipcfg->ggroups[1].groups[0].nodes_pref_list[1] = 2;
    vipcfg->ggroups[1].groups[0].nodes_pref_list[2] = 1;
    vipcfg->num_vip_groups++;

    /* Group 3(node 3) */

    vipcfg->ggroups[1].groups[1].vgid_start_index = 2;
    vipcfg->ggroups[1].groups[1].vip_group_id = 3;
    vipcfg->ggroups[1].groups[1].group_group_id = 1;
    vipcfg->ggroups[1].groups[1].num_nodes = 3;
    vipcfg->ggroups[1].groups[1].nodes_pref_list =
        plat_alloc(vipcfg->ggroups[1].groups[0].num_nodes * sizeof(vnode_t));
    if (vipcfg->ggroups[1].groups[1].nodes_pref_list == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                     PLAT_LOG_LEVEL_ERROR,
                     "Unable to allocate mem for pref List for vip group :%d",
                     0);
        plat_abort();
    }
    vipcfg->ggroups[1].groups[1].nodes_pref_list[2] = 0;
    vipcfg->ggroups[1].groups[1].nodes_pref_list[0] = 2;
    vipcfg->ggroups[1].groups[1].nodes_pref_list[1] = 1;
    vipcfg->num_vip_groups++;
    return (vipcfg);
}
int
sdf_get_ggrpid_by_gid(const struct sdf_vip_config *config, int vip_group_id) {
    struct sdf_vip_group_group *ggrp;
    int i, j;

    for (i = 0; i < config->num_vip_group_groups; i++) {
        ggrp = &(config->ggroups[i]);
        for (j = 0; j < ggrp->num_groups; j++) {
            if (ggrp->groups[j].vip_group_id  == vip_group_id) {
                return (ggrp->group_group_id);
            }
        }
    }
    return (-1);
}

void
print_test_sdf_vip_config(sdf_vip_config_t *vipcfg) {
    int i, j, k, index = 0;
    char print_str[4098];

    strcpy(print_str, "NO VIP CONFIG");
    if (vipcfg == NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR, "VIP CONFIG NULL:%s", print_str);
        return;
    }
    index = index + sprintf(&(print_str[index]), "num_vip_group_groups:%d",
                            vipcfg->num_vip_group_groups);
    for (i = 0; i < vipcfg->num_vip_group_groups; i++) {
        index = index + sprintf(&(print_str[index]), "vipgrpgrp:%d", i);
        index = index + sprintf(&(print_str[index]), "TYPE:%d",
                                vipcfg->ggroups[i].type);
        index = index + sprintf(&(print_str[index]), "grpgrpid:%d",
                                vipcfg->ggroups[i].group_group_id);
        index = index + sprintf(&(print_str[index]), "MaxGrpPerNode:%d",
                                vipcfg->ggroups[i].max_group_per_node);
        index = index + sprintf(&(print_str[index]), "NumVipGroups:%d",
                                vipcfg->ggroups[i].num_groups);
        index = index + sprintf(&(print_str[index]), "vgid_start_index:%d",
                                vipcfg->ggroups[i].vgid_start_index);
        for (j = 0; j < vipcfg->ggroups[i].num_groups; j++) {
            index = index + sprintf(&(print_str[index]), "VipGroup:%d", j);
            index = index + sprintf(&(print_str[index]), "VipGroupId:%d",
                                    vipcfg->ggroups[i].groups[j].vip_group_id);
            index = index + sprintf(&(print_str[index]), "VipGroupGroupId:%d",
                                    vipcfg->ggroups[i].groups[j].group_group_id);
            index = index + sprintf(&(print_str[index]), "NumNodes:%d",
                                    vipcfg->ggroups[i].groups[j].num_nodes);
            index = index +
                sprintf(&(print_str[index]), "vgid_start_index:%d",
                        vipcfg->ggroups[i].groups[j].vgid_start_index);
            for (k = 0; k < vipcfg->ggroups[i].groups[j].num_nodes; k++) {
                index = index +
                    sprintf(&(print_str[index]), "prefOrderforNode:%d:%d",
                            k+vipcfg->ggroups[i].groups[j].vgid_start_index,
                            vipcfg->ggroups[i].groups[j].nodes_pref_list[k]);

            }
        }
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                 PLAT_LOG_LEVEL_INFO, "VIP CONFIG:%s", print_str);
}

/*
 * XXX: drew 2009-11-04 It appears that user_notification_cb only
 * updates the current observed state and validates that consecutive
 * calls have been conssitent with each other; with all validatation
 * against user specified state performed in the test code itself.
 */
void
user_notification_cb(plat_closure_scheduler_t *context, void *env,
                     int events, struct cr_shard_meta *shard_meta,
                     enum sdf_replicator_access access, struct timeval expires,
                     sdf_replicator_notification_complete_cb_t complete,
                     vnode_t node) {
    int i;

    plat_assert(shard_meta !=  NULL);
    switch (access) {
        case SDF_REPLICATOR_ACCESS_RO:
            plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "ACCESS_RO");
            break;
        case SDF_REPLICATOR_ACCESS_RW:
            if (my_nodes[node].adm_state !=  NODE_ADM_STATE_ENABLED) {
                plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "ACCESS_RW:Ivalid NodeID(%d).Not enabled", node);
                break;
            }

            /*
             * XXX: drew 2009-08-21 Should validate that this already
             * exists if it isn't an edge.
             */
            if (!(events & SDF_REPLICATOR_EVENT_ACCESS)) {
                break;
            }

            if (my_nodes[node].num_vip_grps >=  my_nodes[node].maxgrps) {
                plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "ACCESS_RW:Node %d(nvgrps:%d)alrdy owns %d vgrps",
                             node, my_nodes[node].num_vip_grps,
                             my_nodes[node].maxgrps);
                break;
            }

            my_nodes[node].vipgrp_srvcd[my_nodes[node].num_vip_grps] =
                shard_meta->persistent.intra_node_vip_group_id;
            my_nodes[node].num_vip_grps++;
            my_nodes[node].node_type = NODE_TYPE_ACTIVE;
            plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                         "ACCESS_RW:Node:%d vgrp:%d ggrp:%d ngrps:%d exp:%d secs, %d usecs",
                         node, shard_meta->persistent.intra_node_vip_group_id,
                         shard_meta->persistent.inter_node_vip_group_group_id,
                         my_nodes[node].num_vip_grps,
                         (unsigned int)expires.tv_sec,
                         (unsigned int)expires.tv_usec);
            break;
        case SDF_REPLICATOR_ACCESS_NONE:
            if (my_nodes[node].adm_state !=  NODE_ADM_STATE_ENABLED) {
                plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "ACCESS_NONE: Ivalid Node ID(%d). Not enabled!",
                             node);
                break;
            }

            if (!(events & SDF_REPLICATOR_EVENT_ACCESS)) {
                break;
            }

            /* No vip groups */
            if (my_nodes[node].num_vip_grps < 1) {
                if (!(events & SDF_REPLICATOR_EVENT_FIRST)) {
                    plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                                 "ACCESS_NONE: Node %d doesnt service vgroup:%d",
                                 node,
                                 shard_meta->persistent.intra_node_vip_group_id);
                }
                break;
            }

            for (i = 0; i < my_nodes[node].num_vip_grps; i++) {
                if (my_nodes[node].vipgrp_srvcd[i] ==
                    shard_meta->persistent.intra_node_vip_group_id) {
                    break;
                }
            }
            if (i == my_nodes[node].maxgrps) {
                plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "ACCESS_NONE: Node %d does not service vgroup:%d",
                             node,
                             shard_meta->persistent.intra_node_vip_group_id);
                break;
            }
            my_nodes[node].num_vip_grps--;

#ifdef notyet
            /* Works with > 2 vip groups */
            for (; i < my_nodes[node].num_vip_grps; ++i) {
                my_nodes[node].viprp_srvcd[i] =
                my_nodes[node].viprp_srvcd[i + 1];
            }
#else
            if (my_nodes[node].num_vip_grps > 0) {
                if (my_nodes[node].vipgrp_srvcd[0] ==
                    shard_meta->persistent.intra_node_vip_group_id) {
                    my_nodes[node].vipgrp_srvcd[0] =
                        my_nodes[node].vipgrp_srvcd[1];
                }
            }
#endif
            plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                         "ACCESS_NONE: Node:%d removing vipgrp:%d numgrps:%d",
                         node, shard_meta->persistent.intra_node_vip_group_id,
                         my_nodes[node].num_vip_grps);
        break;
    }
    cr_shard_meta_free(shard_meta);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE, "Calling callback complete");
    plat_closure_apply(sdf_replicator_notification_complete_cb, &complete);
}

/*
 * @brief this function returns 0 if validation succeeds or it ruturns 1
 *
 * XXX: drew 2009-11-04 It appears that this only sanity checks the
 * vip groups against the nodes which are claimed to be recovering.  Nothing
 * validates that nodes are actually in their expected state.
 */
int
validate_2way() {
    int i, n_act_nodes = 0;
    struct replication_test_config *test_config;
    test_config = &(test_framework_global->config);

    if (test_config->nnode !=  2) {
        plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "Error:Number of Nodes is less than 2 in 2way");
        return (SDF_FAILURE);
    }

    for (i = 0; i < test_config->nnode; i++) {
        if (my_nodes[i].node_type == NODE_TYPE_ACTIVE) {
            n_act_nodes++;
        }
    }

    if (n_act_nodes == 2) {
        /* Both Nodes should service its own vgroup */
        if ((my_nodes[0].num_vip_grps !=  1) ||
            (my_nodes[1].num_vip_grps !=  1)) {
            plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "Both UP but nvgrps in node0(%d) and node1(%d)!=1",
                         my_nodes[0].num_vip_grps, my_nodes[1].num_vip_grps);
            return (SDF_FAILURE);
        } else {
            if ((my_nodes[0].vipgrp_srvcd[0] !=  0) ||
                (my_nodes[1].vipgrp_srvcd[0] !=  1)) {
                plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "Both UP but N0's vgrp(%d)!=0 N1's vgrp(%d)!=1",
                             my_nodes[0].vipgrp_srvcd[0],
                             my_nodes[1].vipgrp_srvcd[0]);
                return (SDF_FAILURE);
            }
        }
        plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                     "Validate 2way: Both the nodes own 1 group");
        return (SDF_SUCCESS);
    } else if (n_act_nodes == 1) {
        int active_node = 0;
        if (my_nodes[1].node_type == NODE_TYPE_ACTIVE) {
            active_node = 1;
        }
        /* This node should service two vips */
        if (my_nodes[active_node].num_vip_grps !=  2) {
            plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "One Node down but active node %d doesn't own 2 vgrs",
                         active_node);
            return (SDF_FAILURE);
        }
        /* Validate that the serviced vip groups are unique */
        if (my_nodes[active_node].vipgrp_srvcd[0] ==
            my_nodes[active_node].vipgrp_srvcd[1]) {
            plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "VGrps srvced by active node %d not unique(%d)",
                         active_node, my_nodes[active_node].vipgrp_srvcd[0]);
            return (SDF_FAILURE);
        }

        if ((my_nodes[active_node].vipgrp_srvcd[0] >= 2) ||
            (my_nodes[active_node].vipgrp_srvcd[1] >= 2)) {
            plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "Invalid vgrps in active node %d:(%d, %d)",
                         active_node, my_nodes[active_node].vipgrp_srvcd[0],
                         my_nodes[active_node].vipgrp_srvcd[1]);
            return (SDF_FAILURE);
        }
        plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE,
                     "Validate 2way: Active node:%d owns %d grps\n",
                     active_node, my_nodes[active_node].num_vip_grps);
        return (SDF_SUCCESS);
    } else {
        /* both the nodes are down */
        plat_log_msg(LOG_ID, LOG_CAT, LOG_TRACE, "Both the nodes are down");
    }
    return (SDF_SUCCESS);
}

int
validate_nplus1() {
    int i, n_act_nodes = 0;
    struct replication_test_config *test_config;
    int vipgrp_list[MAX_NODES];
    test_config = &(test_framework_global->config);

    for (i = 0; i < test_config->nnode; i++) {
        if (my_nodes[i].node_type == NODE_TYPE_ACTIVE) {
            n_act_nodes++;
        }
    }

    if (n_act_nodes > (test_config->nnode -1)) {
        plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "Invalid state:Num of active nodes > %d correct is <=%d",
                     n_act_nodes, (test_config->nnode -1));
        return (SDF_FAILURE);
    }
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Number of Active nodes %d",
                 n_act_nodes);
    if (n_act_nodes == 0) {
        plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                     "Number of Active nodes %d", n_act_nodes);
        return (SDF_SUCCESS);
    }

    /* Check whether any node is handling more than 1 group or not handling vgroup */
    for (i = 0; i < test_config->nnode; i++) {
        if ((my_nodes[i].node_type == NODE_TYPE_ACTIVE) &&
            (my_nodes[i].num_vip_grps !=  1)) {
            plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "Invalid state:Num of active nodes > %d correct <=%d",
                         n_act_nodes, (test_config->nnode -1));
            return (SDF_FAILURE);
        }
    }
    memset(vipgrp_list, 0, sizeof(int) * MAX_NODES);

    /* Check whether any vgroup is handled by morethan one node */
    for (i = 0; i < test_config->nnode; i++) {
        if (my_nodes[i].node_type == NODE_TYPE_ACTIVE) {
            vipgrp_list[my_nodes[i].vipgrp_srvcd[0]]++;
        }
    }
    for (i = 0; i < test_config->nnode; i++) {
        if (vipgrp_list[i] > 1) {
            plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "ERROR:VIP GROUP %d is servoced by %d nodes", i,
                         vipgrp_list[i]);
            return (SDF_FAILURE);
        }
    }
    return (SDF_SUCCESS);
}
int
test_nplus1(struct replication_test_framework *test_framework) {
    int ret, status, ggrp_id;

    /* Wait here until both the groups are owned */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Wait here until both the groups are owned");
    rtfw_sleep_usec(test_framework,
                    test_framework->config.replicator_config.lease_usecs * 5);
    /* Here is assumed that, all the nodes are UP and running */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Validate: Two nodes should be active and own 1 vgrp each");
    ret = validate_nplus1();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    /* Stop the Node 0 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Stop Node 0");
    status = rtfw_crash_node_sync(test_framework, 0);
    sdf_status_assert_eq(status, SDF_SUCCESS);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Node 0 down");
    my_nodes[0].status    = NODE_STATUS_DOWN;
    my_nodes[0].node_type = NODE_TYPE_STANDBY;
    my_nodes[0].num_vip_grps = 0;
    /* Wait for 2 lease intervals */
    rtfw_sleep_usec(test_framework,
                    test_framework->config.replicator_config.lease_usecs * 2);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Validate: Node 1 should pickup vgrp 0. Node 0 & 1 should own 1 vgrp each");
    ret = validate_nplus1();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    /* restart Node 0 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Restart Node 0");
    status = rtfw_start_node(test_framework, 0);
    sdf_status_assert_eq(status, SDF_SUCCESS);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Node 0 Restarted");
    my_nodes[0].status = NODE_STATUS_UP;
    my_nodes[0].node_type = NODE_TYPE_STANDBY; /* Set to Non Active state */
    ggrp_id =  sdf_get_ggrpid_by_gid(vip_cfg, 0);
    if (ggrp_id == -1) {
        plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_INFO, "Invalid grpgrp id for node 0");
        plat_abort();
    }
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Restart Node 0 Create Shard");
    create_shard(test_framework, 0, ggrp_id);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Restart Node 0 Shard Created");
    /* Wait for 2 lease intervals */
    rtfw_sleep_usec(test_framework, test_framework->config.replicator_config.lease_usecs * 2);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Validate, Node 0 should go to standby");
    ret = validate_nplus1();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    /* stop node 1 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Stop Node 1");
    status = rtfw_crash_node_sync(test_framework, 1);
    sdf_status_assert_eq(status, SDF_SUCCESS);
    /* Wait for 2 lease intervals */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Node 1 down");
    my_nodes[1].status = NODE_STATUS_DOWN;
    my_nodes[1].node_type = NODE_TYPE_STANDBY; /* Set to Non Active state */
    my_nodes[1].num_vip_grps = 0;

    /* Wait for 2 lease intervals */
    rtfw_sleep_usec(test_framework, test_framework->config.replicator_config.lease_usecs * 2);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Validate: Node  0  should own VGRP 1");
    ret = validate_nplus1();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    /* restart Node 1 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Restart Node 1");
    status = rtfw_start_node(test_framework, 1);
    sdf_status_assert_eq(status, SDF_SUCCESS);
    my_nodes[1].status = NODE_STATUS_UP;
    my_nodes[1].node_type = NODE_TYPE_STANDBY; /* Set to Non Active state */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Node 1 Restarted");
    ggrp_id =  sdf_get_ggrpid_by_gid(vip_cfg, 1);
    if (ggrp_id == -1) {
        plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_INFO, "Invalid grpgrp id for node 0");
        plat_abort();
    }
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Restart Node 1 Create Shard");
    create_shard(test_framework, 1, ggrp_id);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Restart Node 1 Shard Created");
    rtfw_sleep_usec(test_framework, test_framework->config.replicator_config.lease_usecs * 2);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Validating, Node 1  should go to standby");
    ret = validate_nplus1();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    return (SDF_SUCCESS);
}

int
test_2way(struct replication_test_framework *test_framework) {
    int ret, status, ggrp_id;
    vnode_t node;
    vnode_t found;
    vnode_t other;
    int groups;
    int group;
    int i;

    /* Wait here until both the groups are owned */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Wait here until both the groups are owned");

    /* Delay is arbitary, but should be longer than two lease intervals */
    rtfw_sleep_usec(test_framework, test_framework->config.replicator_config.lease_usecs * 5);

    if (!test_framework->config.replicator_config.initial_preference) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "Validate: Node 0 should be active with VGRP 0 & 1,"
                     " node 1 recovering");

        /* Don't count on the correct assignments having been made */
        my_nodes[0].node_type = NODE_TYPE_ACTIVE;
        my_nodes[1].node_type = NODE_TYPE_RECOVERING;

        ret = validate_2way();
        sdf_status_assert_eq(ret, SDF_SUCCESS);

        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Send RECOVERED");
        ret = rtfw_command_sync(test_framework, 0 /* node */,
                                shardids[0 /* node */][0 /* group grpup */],
                                "RECOVERED\r\n",  NULL);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Send RECOVERED complete");
        sdf_status_assert_eq(ret, SDF_SUCCESS);

        rtfw_sleep_usec(test_framework,
                        test_framework->config.network_timing.max_delay_us * 5);
    }

    plat_assert_always(my_nodes[0].node_type == NODE_TYPE_ACTIVE);
    plat_assert_always(my_nodes[1].node_type == NODE_TYPE_ACTIVE);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Validate: Node 0 should own VGRP 0 &"
                 " Node1 should own VGRP 1");

    ret = validate_2way();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    /* Stop the Node 0 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Stop the Node 0");
    status = rtfw_crash_node_sync(test_framework, 0);
    sdf_status_assert_eq(status, SDF_SUCCESS);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Stop the Node 0 complete");
    my_nodes[0].status = NODE_STATUS_DOWN;
    my_nodes[0].node_type = NODE_TYPE_RECOVERING; /* Set the state to non active */
    my_nodes[0].num_vip_grps = 0;

    /*
     * XXX: drew 2009-12-15 This should be on the order of network delays when
     * lease_liveness is used.
     */
    /* Wait for 2 lease intervals */
    rtfw_sleep_usec(test_framework,
                    test_framework->config.replicator_config.lease_liveness ?
                    (test_framework->config.network_timing.max_delay_us * 5) :
                    (test_framework->config.replicator_config.lease_usecs * 2));
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Validate: Node 1 should own VGRP 0 & 1");
    ret = validate_2way();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    /* Stop Node 1 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Stop the Node 1");
    status = rtfw_crash_node_sync(test_framework, 1);
    sdf_status_assert_eq(status, SDF_SUCCESS);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Stop the Node 1 complete");
    my_nodes[1].status = NODE_STATUS_DOWN;
    my_nodes[1].node_type = NODE_TYPE_RECOVERING; /* Set the state to non active */
    my_nodes[1].num_vip_grps = 0;
    /* Wait for 2 lease intervals */
    rtfw_sleep_usec(test_framework, test_framework->config.replicator_config.lease_usecs * 2);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Validate:Both vgrps not owned");
    ret = validate_2way();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    /* Start the Node 0 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Restart the Node 0");
    status = rtfw_start_node(test_framework, 0);
    sdf_status_assert_eq(status, SDF_SUCCESS);
    my_nodes[0].status = NODE_STATUS_UP;
    my_nodes[0].node_type = NODE_TYPE_RECOVERING;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Restart the Node 0 complete");

    ggrp_id =  sdf_get_ggrpid_by_gid(vip_cfg, 0);
    if (ggrp_id == -1) {
        plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR, "Invalid grpgrp id for node 0");
        plat_abort();
    }
    /* Create shard */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Recreate Node 0 shards");
    create_shard(test_framework, 0, ggrp_id);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Recreate Node 0 shards complete");

    rtfw_sleep_usec(test_framework, test_framework->config.replicator_config.lease_usecs * 5);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Validate:Node 0 should own Vgrp 0 & 1");
    ret = validate_2way();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    /* Start the Node 1 */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Restart the Node 1");
    status = rtfw_start_node(test_framework, 1);
    sdf_status_assert_eq(status, SDF_SUCCESS);
    my_nodes[1].status = NODE_STATUS_UP;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Retart the Node 1 complete");

    ggrp_id =  sdf_get_ggrpid_by_gid(vip_cfg, 1);
    if (ggrp_id == -1) {
        plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "Invalid grpgrp id for node 0");
        plat_abort();
    }
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Recreate Node 1 shards");
    create_shard(test_framework, 1, ggrp_id);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Recreate Node 1 shards complete");

    my_nodes[1].node_type = NODE_TYPE_RECOVERING;
    rtfw_sleep_usec(test_framework,
                    test_framework->config.replicator_config.lease_usecs * 2);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Validate: Node 0 still should own Vgrp 0 & 1");
    ret = validate_2way();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Send RECOVERED");
    ret = rtfw_command_sync(test_framework, 0 /* node */,
                            shardids[0 /* node */][0 /* group grpup */],
                            "RECOVERED\r\n",  NULL);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Send RECOVERED complete");
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    my_nodes[1].node_type = NODE_TYPE_ACTIVE;
    /* XXX: drew 2009-12-15 This should be on the order of network delays */
    rtfw_sleep_usec(test_framework,
                    test_framework->config.network_timing.max_delay_us * 5);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Validate: Node 0 should own Vgrp 0 & node1 should own 1");
    ret = validate_2way();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    if (!new_tests) {
        goto new_tests_exit;
    }

    /* Test split-brain recovery */

    for (node = 0; node < 2; ++node) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "shutdown node %ld network",
                     (long)node);
        ret = rtfw_shutdown_node_network_sync(test_framework, node);
        sdf_status_assert_eq(ret, SDF_SUCCESS);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "shutdown node %ld network complete", (long)node);
    }

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "split brain initiated, sleeping");

    /* XXX: drew 2009-12-15 This should be on the order of network delays */
    rtfw_sleep_usec(test_framework,
                    test_framework->config.network_timing.max_delay_us * 5);

    plat_assert(my_nodes[0].num_vip_grps == 2);
    plat_assert(my_nodes[1].num_vip_grps == 2);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "split brain complete, both nodes own both VIPs");

    for (node = 0; node < 2; ++node) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start node %ld network",
                     (long)node);
        ret = rtfw_start_node_network_sync(test_framework, node);
        sdf_status_assert_eq(ret, SDF_SUCCESS);
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "start node %ld network complete", (long)node);
    }

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "networks started, sleeping");

    /* XXX: drew 2010-04-19 This should validate timing constraints */
    rtfw_sleep_usec(test_framework,
                    test_framework->config.replicator_config.lease_usecs * 20);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "sleep done, verifying split brain recovery");

    for (other = found = SDF_ILLEGAL_VNODE, groups = node = 0;
         node < 2; ++node) {
        for (i = 0; i < my_nodes[node].num_vip_grps; ++i) {
            group = 1 << my_nodes[node].vipgrp_srvcd[i];
            plat_assert((groups & group) == 0);
            groups |= group;
        }

        if (my_nodes[node].num_vip_grps == 2) {
            plat_assert(found == SDF_ILLEGAL_VNODE);
            found = node;
        } else if (my_nodes[node].num_vip_grps == 0) {
            plat_assert(other == SDF_ILLEGAL_VNODE);
            other = node;
        }
    }

    /* Both must be serviced */
    plat_assert(groups == 3);

    /* And both should be on one node with none on the other to recover */
    plat_assert(found != SDF_ILLEGAL_VNODE);
    plat_assert(other != SDF_ILLEGAL_VNODE);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "split brain resolved");

    my_nodes[other].node_type = NODE_TYPE_RECOVERING;
    ret = validate_2way();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Send RECOVERED");
    ret = rtfw_command_sync(test_framework, found /* node */,
                            shardids[0 /* node */][0 /* group grpup */],
                            "RECOVERED\r\n",  NULL);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Send RECOVERED complete");
    sdf_status_assert_eq(ret, SDF_SUCCESS);

    /* XXX: drew 2009-12-15 This should be on the order of network delays */
    rtfw_sleep_usec(test_framework,
                    test_framework->config.network_timing.max_delay_us * 5);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Validate: Node 0 should own Vgrp 0 & node1 should own 1");
    my_nodes[other].node_type = NODE_TYPE_ACTIVE;
    ret = validate_2way();
    sdf_status_assert_eq(ret, SDF_SUCCESS);

new_tests_exit:
    return (SDF_SUCCESS);
}

void
create_shard(struct replication_test_framework *test_framework, int node,
             int vipgrpgrp) {
    int failed;
    struct SDF_shard_meta *shard_meta = NULL;
    SDF_shardid_t shard_id;
    SDF_boolean_t op_ret;
    SDF_replication_props_t *replication_props = NULL;

    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Create shard for node:%d vgrpgrp:%d", node, vipgrpgrp);

    plat_assert(node < MAX_NODES);
    plat_assert(vipgrpgrp < MAX_GROUP_GROUPS);

    /* XXX: drew 2009-08-27 This probably leaks replication_props */
    failed = !plat_calloc_struct(&replication_props);
    plat_assert(!failed);
    rtfw_set_default_replication_props(&test_framework->config, replication_props);

    shard_id = __sync_fetch_and_add(&initial_shard_id, 1);
    shard_meta = rtfw_init_shard_meta(&test_framework->config,
                                      node, shard_id, replication_props);
    shard_meta->inter_node_vip_group_group_id = vipgrpgrp;
    op_ret = rtfw_create_shard_sync(test_framework, node, shard_meta);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                 "Create shard for node:%d vgrpgrp:%d complete", node,
                 vipgrpgrp);
    sdf_status_assert_eq(op_ret, SDF_SUCCESS);

    shardids[node][vipgrpgrp] = shard_id;
    plat_free(replication_props);
}


void
test_state_machine(uint64_t args) {
    int i, j, failed;
    rtfw_replicator_notification_cb_t notify_cb;
    SDF_replication_props_t *replication_props = NULL;
    struct replication_test_framework *test_framework;
    struct replication_test_config *test_config;

    /* Start the test framework */
    test_framework = (struct replication_test_framework *)args;
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start replication test framework");
    rtfw_start(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "replication test framework started");
    test_config = &(test_framework->config);

    /* Start all the nodes and Mark all nodes admin enabled */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "start nodes");
    for (i = 0; i < test_config->nnode; i++) {
        my_nodes[i].adm_state  = NODE_ADM_STATE_ENABLED;
        if (test_config->replication_type == SDF_REPLICATION_V1_2_WAY) {
            my_nodes[i].node_type = NODE_TYPE_RECOVERING;
        } else {
            my_nodes[i].node_type = NODE_TYPE_STANDBY;
        }
    }
    rtfw_start_all_nodes(test_framework);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "nodes started");

    /* Create Default replication properties */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Create Default replication properties");
    failed = !plat_calloc_struct(&replication_props);
    plat_assert(!failed);
    rtfw_set_default_replication_props(&test_framework->config, replication_props);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Default replication properties Created");

    /* Create Notification Callback */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Create Notification Callback");
    notify_cb =
        rtfw_replicator_notification_cb_create(test_framework->closure_scheduler,
                                               &user_notification_cb,
                                               test_framework);
    rtfw_set_replicator_notification_cb(test_framework, notify_cb);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Notification Callback Created");

    /* Create Shards */
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Create shards");
    for (i = 0; i < vip_cfg->num_vip_group_groups; i++) {
        for (j = 0; j < vip_cfg->ggroups[i].num_groups; j++) {
            create_shard(test_framework,
                         vip_cfg->ggroups[i].groups[j].vip_group_id,
                         vip_cfg->ggroups[i].groups[j].group_group_id);
        }
        if (vip_cfg->ggroups[i].type == SDF_CLUSTER_GRP_TYPE_NPLUS1) {
            int standby_nodeid;
            /*
             * Create Shard for Standby Node. The standby node is the lat node in the
             * group. so vip_cfg->ggroups[i].num_groups is the standby node id
             */
            standby_nodeid = vip_cfg->ggroups[i].num_groups;
            create_shard(test_framework, standby_nodeid,
                         vip_cfg->ggroups[i].group_group_id);
        }
    }
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "Create shards complete");
    /* Start the test */
    if (test_config->replication_type == SDF_REPLICATION_V1_2_WAY) {
        test_2way(test_framework);
    } else {
        test_nplus1(test_framework);
    }
    plat_free(replication_props);
    rtfw_shutdown_sync(test_framework);
    fthKill(1);
}


void
initialize_nodes(int rep_type, int num_nodes) {
    int i;
    for (i = 0; i < num_nodes; i++) {
        my_nodes[i].num_vip_grps = 0;
        my_nodes[i].adm_state = NODE_ADM_STATE_ENABLED;
        my_nodes[i].maxgrps = 1;
        if (rep_type == SDF_REPLICATION_V1_2_WAY) {
            my_nodes[i].maxgrps = 2;
        }
    }
}

void
print_node_list(int num_nodes) {
    int i, index = 0;
    char print_str[2048];

    for (i = 0; i < num_nodes; i++) {
        index = index +
            sprintf(&(print_str[index]),
                    "Node:%d: numvgrps:%d maxgrps:%d admstate:%d", i,
                    my_nodes[i].num_vip_grps, my_nodes[i].maxgrps,
                    my_nodes[i].adm_state);
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_PROT,
                 PLAT_LOG_LEVEL_INFO, "NODE_INFO:%s", print_str);
}

int
main(int argc, char **argv) {
    SDF_status_t status;
    struct plat_opts_config_state_machine_test opts_config;
    int opts_status;

    memset(&opts_config, 0, sizeof (opts_config));
    rt_common_test_config_init(&opts_config.common_config);

    opts_status = plat_opts_parse_state_machine_test(&opts_config, argc, argv);
    if (opts_status) {
        return (1);
    }

    status = rt_common_init(&opts_config.common_config);
    if (status) {
        return (1);
    }

    if (opts_config.common_config.test_config.replication_type ==
        SDF_REPLICATION_V1_2_WAY) {
        vip_cfg = build_test_sdf_vip_config_2way();
    } else if (opts_config.common_config.test_config.replication_type ==
               SDF_REPLICATION_V1_N_PLUS_1) {
        vip_cfg = build_test_sdf_vip_config_nplus1();
#ifdef notyet
        // For both mode
        vip_cfg = build_test_sdf_vip_config_2way_nplus1();
        plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                     "\n2way and n+1 together not supported yet");
        plat_abort();
#endif
    } else {
        plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_FATAL,
                     "\nUsage:%s --2way|--nplus1 [optional args]",
                     argv[0]);
        plat_abort();
    }

    if (vip_cfg == NULL) {
        plat_log_msg(LOG_ID, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "Building Test VIP CFG failed");
        plat_abort();
    }

    if (opts_config.common_config.test_config.replication_type ==
        SDF_REPLICATION_V1_2_WAY) {
        opts_config.common_config.test_config.nnode = vip_cfg->num_vip_groups;
    } else {
        opts_config.common_config.test_config.nnode = vip_cfg->num_vip_groups + 1;
    }
    opts_config.common_config.test_config.replicator_config.lease_usecs =
        100 * MILLION;
    opts_config.common_config.test_config.replicator_config.vip_config =
        vip_cfg;

    initialize_nodes(opts_config.common_config.test_config.replication_type,
                     opts_config.common_config.test_config.nnode);

    /* start fthread library */
    fthInit();

    test_framework_global =
        replication_test_framework_alloc(&opts_config.common_config.test_config);
    if (test_framework_global) {
        plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG,
                     "test_framework %p allocated", test_framework_global);
    }
    XResume(fthSpawn(&test_state_machine, 40960),
            (uint64_t)test_framework_global);
    fthSchedulerPthread(0);
    plat_log_msg(LOG_ID, LOG_CAT, LOG_DBG, "JOIN");
    rt_common_detach(&opts_config.common_config);
    rt_common_test_config_destroy(&opts_config.common_config);
    return (0);
}

#include "platform/opts_c.h"
