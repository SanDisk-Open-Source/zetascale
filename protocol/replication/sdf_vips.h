#ifndef SDF_VIPS_H
#define SDF_VIPS_H 1

typedef int vip_group_id_t;

enum {
    /** @brief Out-of-band value for invalid vip_group_id_t */
    VIP_GROUP_ID_INVALID = -1
};

typedef int vip_group_group_id_t;

enum {
    /** @brief Out-of-band value for invalid vip_group_group_id_t */
    VIP_GROUP_GROUP_ID_INVALID = -1
};

/**
 * @brief Intra-node vip group
 */
typedef struct sdf_vip_group {
    /** @brief Id of this group from properties file */
    int vip_group_id;
    /** @brief Id of this group from properties file */
    int group_group_id;
    /** @brief Number of nodes in this group */
    int num_nodes;
    /** @brief start index of nodes in the group group */
    int vgid_start_index;
    /** @brief Nodes sorted in order of decreasing preference */
    vnode_t *nodes_pref_list;
} sdf_vip_group_t;

/** @brief Inter-node vip group group */
typedef struct sdf_vip_group_group {
    /** @brief Type (simple replication, N+1, etc.) */
    SDF_cluster_grp_type_t type;
    /** @brief Id of this group from properties file */
    int group_group_id;

    /**
     * @brief Maximum number of intra-node VIP groups on a single physical node
     *
     * XXX: drew 2009-08-11 We shouldn't need this limit
     */
    int max_group_per_node;

    /** @brief Number of intra-node VIP groups in this inter-node group group */
    int num_groups;

    /**
     * @brief VIP group ID are unique per cluster & identified by an int ID
     *        but stored in an array starting index 0
     *        in order to have vipgroup search O(1), start VIP group ID is
     *        stored here. for any virtual group ID, the index in the array
     *        is vip_group_id - vgroup_start_index`
     *
     * XXX: drew 2009-08-11 These shouldn't be contiguous in a global
     * name space because that precludes dynamic additions to the group.
     *
     * Since
     * 1) Groups are never going to get too big (one spare out of tens
     *    of machines ceases to be enough
     * and
     * 2) This is only done on liveness events which occur several per
     * second
     * the O(1) restriction is unecessary.
     */
    int vgid_start_index;

    /** @brief Intra-node groups */
    struct sdf_vip_group *groups;
}sdf_vip_group_group_t;



typedef struct sdf_vip_config {
    /** @brief Number of VIP groups */
    int num_vip_groups;

    /** @brief Number of VIP group groups */
    int num_vip_group_groups;
    /** @brief VIP group groups */
    struct sdf_vip_group_group *ggroups;
} sdf_vip_config_t;

/**
 * @brief Perform deep copy of sdf_config
 * @return sdf_vip_group, NULL on failure
 */
struct sdf_vip_config *sdf_vip_config_copy(const struct sdf_vip_config *);

/** @brief Free sdf_config structure */
void sdf_vip_config_free(struct sdf_vip_config *);

/**
 * @brief Get vip_group_group by vip_group_id from config structure
 * @return sdf_vip_group_group owned by config on success, NULL on failure
 */
struct sdf_vip_group_group *
sdf_vip_config_get_group_group_by_gid(const struct sdf_vip_config *,
                                      int gid);

/**
 * @brief Get vip_group_group by group_group_id from config structure
 * @return sdf_vip_group_group owned by config on success, NULL on failure
 */
struct sdf_vip_group_group *
sdf_vip_config_get_group_group_by_ggid(const struct sdf_vip_config *,
                                       int ggid);

/**
 * @brief Get vip_group by vip_group_id from config structure
 * @return sdf_vip_group owned by config on success, NULL on failure
 */
struct sdf_vip_group *
sdf_vip_config_get_vip_group(const struct sdf_vip_config *, int gid);

/**
 * @brief Get vip_group by vip_group_id from config structure
 * @return number of VIP groups owned by config on success, NULL on failure
 */
int
sdf_vip_config_get_num_vip_groups(const struct sdf_vip_config *, int **);

/**
 * @brief Get preference for given pnode
 * @return Preference for given node with 0 as highest, -1 on error (node
 * not in list).
 */
int
sdf_vip_group_get_node_preference(const struct sdf_vip_group *,
                                  vnode_t node);

/**
 * @brief Get rank for given pnode
 *
 * Where rank is where it lies in a sorted sequence.  Given a preference
 * list of { 10, 5, 7, 11 }
 *
 *      node rank    preference
 *      5    0       1
 *      7    1       2
 *      10   2       0
 *      11   3       3
 *
 * @return Rank for given node with 0 as highest, -1 on error (node
 * not in list).
 */
int
sdf_vip_group_get_node_rank(const struct sdf_vip_group *, vnode_t node);

/**
 * @brief Get the nth most preferred node in the group, 0 for the first
 *
 * @return node, SDF_ILLEGAL_PNODE on failure
 */
vnode_t
sdf_vip_group_get_node_by_preference(const struct sdf_vip_group *, int n);

#endif /* ndef SDF_VIPS_H */
