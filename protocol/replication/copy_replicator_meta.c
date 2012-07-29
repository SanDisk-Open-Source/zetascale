/*
 * File:   sdf/protocol/replication/copy_replicator_internal.h
 *
 * Author: drew
 *
 * Created on November 13, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: copy_replicator_meta.c 10527 2009-12-12 01:55:08Z drew $
 */

/**
 * Copy replicator meta-data manipulation
 */

#include "platform/alloc.h"
#include "platform/logging.h"
#include "platform/stdio.h"

#include "shared/shard_meta.h"

#include "protocol/replication/replicator.h"

#include "copy_replicator_internal.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_SDF_PROT_REPLICATION,
                      "copy_replicator");

/**
 * @brief Append structure at src to out and advance out
 *
 * @param out <INOUT> char * buffer which is advanced
 * @param src <IN> Structure which is appended with cr_persistent_header
 * at its front
 */
#define cr_meta_append(out, src) \
    do {                                                                       \
        memcpy(out, (src), (src)->header.len);                                 \
        (out) += (src)->header.len;                                            \
    } while (0)

/**
 * @brief Extract dest structure and advance in pointer
 *
 * @param dest <OUT> Destination structure.
 * @param in <INOUT> char * buffer pointer
 * @param expect_magic <IN> expected magic number on structure
 * @param expect_version <IN> expected version on structure
 * @return SDF_SUCCESS on success
 */

/* BEGIN CSTYLED */

enum {
    CR_META_EXTRACT_FIXED,
    CR_META_EXTRACT_VAR
};

/*
 * XXX: drew 2009-05-20 This needs to split into a helper function which
 * takes the function/file/line for error forwarding and a stringified dest.
 */
#define cr_meta_extract(dest, in, expect_magic, expect_version) \
    ({                                                                         \
        struct cr_persistent_header header;                                    \
        SDF_status_t status;                                                   \
        const char *reason = "";                                               \
                                                                               \
        memcpy(&header, in, sizeof (header));                                  \
        if (header.magic.integer != (expect_magic)) {                          \
            status = SDF_META_DATA_INVALID;                                    \
            reason = "magic";                                                  \
        } else if (header.version > (expect_version)) {                        \
            status = SDF_META_DATA_VERSION_TOO_NEW;                            \
            reason = "version too new";                                        \
        } else if (header.version != (expect_version)) {                       \
            status = SDF_META_DATA_INVALID;                                    \
            reason = "version mismatch";                                       \
        } else if (header.len != sizeof (*dest)) {                             \
            status = SDF_META_DATA_INVALID;                                    \
            reason = "len incorrect";                                          \
        } else {                                                               \
            memcpy((dest), in, header.len);                                    \
            in += header.len;                                                  \
            status = SDF_SUCCESS;                                              \
        }                                                                      \
                                                                               \
        if (status != SDF_SUCCESS) {                                           \
            plat_log_msg(21423, LOG_CAT,                         \
                         PLAT_LOG_LEVEL_INFO,                                  \
                         "error reading meta_data %s: %s %s", #dest,           \
                         sdf_status_to_string(status), reason);                \
        }                                                                      \
                                                                               \
        status;                                                                \
     })
/* END CSTYLED */

static int cr_vip_meta_find_shard(struct cr_vip_meta *meta,
                                  SDF_shardid_t sguid);
static size_t cr_persistent_vip_meta_len(int count);
static int cr_persistent_vip_meta_shard_cmp(const void *lhs,
                                            const void *rhs) __attribute__((unused));


SDF_status_t
cr_shard_meta_create(struct cr_shard_meta **out,
                     const struct sdf_replicator_config *config,
                     const struct SDF_shard_meta *shard_meta) {
    vnode_t pnodes[SDF_REPLICATION_MAX_REPLICAS];
    int i;
    SDF_status_t ret = SDF_SUCCESS;

    struct cr_shard_meta *cr_shard_meta;
    struct cr_persistent_shard_meta *persistent;
    struct cr_shard_replica_meta *cr_shard_replica_meta;
    struct cr_persistent_shard_replica_meta *persistent_replica;


    if (!plat_calloc_struct(&cr_shard_meta)) {
        ret = SDF_FAILURE_MEMORY_ALLOC;
    }

    if (ret == SDF_SUCCESS) {
        persistent = &cr_shard_meta->persistent;
        persistent->header.magic.integer = CR_SHARD_META_MAGIC;
        persistent->header.len = sizeof (*persistent);
        persistent->header.version = CR_SHARD_META_VERSION;

        persistent->sguid = shard_meta->sguid;
        persistent->sguid_meta = shard_meta->sguid_meta;
        persistent->type = shard_meta->replication_props.type;

        cr_get_pnodes(config, &shard_meta->replication_props,
                      shard_meta->first_node, pnodes,
                      &persistent->num_replicas,
                      &persistent->num_meta_replicas);

        /* XXX: drew 2008-11-17 this goes away */
        plat_assert(persistent->num_meta_replicas <= 1);
        persistent->meta_pnodes[0] = shard_meta->first_meta_node;

        persistent->outstanding_window = config->outstanding_window;

        persistent->current_home_node = CR_HOME_NODE_NONE;
        persistent->last_home_node = CR_HOME_NODE_NONE;
        persistent->lease_usecs = 0;
        persistent->ltime = 0;

        for (i = 0; ret == SDF_SUCCESS && i < persistent->num_replicas; ++i) {
            if (plat_calloc_struct(&cr_shard_replica_meta)) {
                persistent_replica = &cr_shard_replica_meta->persistent;

                persistent_replica->header.magic.integer =
                    CR_SHARD_REPLICA_META_MAGIC;
                persistent_replica->header.len = sizeof (*persistent_replica);
                persistent_replica->header.version =
                    CR_SHARD_REPLICA_META_VERSION;

                persistent_replica->pnode = pnodes[i];
                persistent_replica->state = CR_REPLICA_STATE_AUTHORITATIVE;
                persistent_replica->nrange = 0;

                cr_shard_replica_meta->ranges = NULL;

                /*
                 * XXX: drew 2009-05-28 Do we always want to create the
                 * range "normally" and then just enter recovery?
                 */
                cr_shard_replica_meta_add_range(cr_shard_replica_meta,
                                                CR_REPLICA_RANGE_ACTIVE,
                                                CR_FIRST_VALID_SEQNO,
                                                CR_SHARD_RANGE_OPEN);


                cr_shard_meta->replicas[i] = cr_shard_replica_meta;
            } else {
                ret = SDF_FAILURE_MEMORY_ALLOC;
            }
        }

        cr_shard_meta->vip_meta = cr_vip_meta_alloc();
        if (!cr_shard_meta->vip_meta) {
            ret = SDF_FAILURE_MEMORY_ALLOC;
        }
    }

    if (ret == SDF_SUCCESS) {
        *out = cr_shard_meta;
    } else {
        cr_shard_meta_free(cr_shard_meta);
    }

    return (ret);
}

void
cr_shard_meta_free(struct cr_shard_meta *meta) {
    struct cr_shard_replica_meta *cr_shard_replica_meta;
    int i;

    if (meta) {
        for (i = 0; i < meta->persistent.num_replicas; ++i) {
            cr_shard_replica_meta = meta->replicas[i];
            if (cr_shard_replica_meta) {
                if (cr_shard_replica_meta->ranges) {
                    plat_free(cr_shard_replica_meta->ranges);
                }
                plat_free(cr_shard_replica_meta);
            }
        }
        if (meta->vip_meta) {
            cr_vip_meta_free(meta->vip_meta);
        }
        plat_free(meta);
    }
}

struct cr_shard_meta *
cr_shard_meta_dup(const struct cr_shard_meta *rhs) {
    struct cr_shard_meta *lhs;
    struct cr_shard_replica_meta *lhs_replica_meta;
    struct cr_shard_replica_meta *rhs_replica_meta;
    int failed;
    int i;

    plat_malloc_struct(&lhs);
    failed = !lhs;

    if (!failed) {
        *lhs = *rhs;
        /*
         * Continue to initialize after a failure so that all of the pointers
         * are either NULL or reference pieces of the left hand side so that
         * cleanup can be stupid.
         */
        for (i = 0; i < lhs->persistent.num_replicas; ++i) {
            rhs_replica_meta = rhs->replicas[i];
            if (rhs_replica_meta) {
                plat_malloc_struct(&lhs->replicas[i]);
                if (!lhs->replicas[i]) {
                    failed = 1;
                }
            }
            lhs_replica_meta = lhs->replicas[i];
            plat_assert_imply(lhs_replica_meta, rhs_replica_meta);
            if (rhs_replica_meta && lhs_replica_meta) {
                *lhs_replica_meta = *rhs_replica_meta;
                if (rhs_replica_meta->ranges) {
                    lhs_replica_meta->ranges =
                        plat_malloc(lhs_replica_meta->persistent.nrange *
                                    sizeof (*lhs_replica_meta->ranges));
                    if (!lhs_replica_meta->ranges) {
                        failed = 1;
                    }
                }
            }
            if (rhs_replica_meta && rhs_replica_meta->ranges &&
                lhs_replica_meta && lhs_replica_meta->ranges) {
                memcpy(lhs_replica_meta->ranges, rhs_replica_meta->ranges,
                       lhs_replica_meta->persistent.nrange *
                       sizeof (*lhs_replica_meta->ranges));
            }
        }

        /* XXX: drew 2009-09-02 Always use cr_vip_meta structure */
        if (lhs->vip_meta) {
            lhs->vip_meta = cr_vip_meta_dup(lhs->vip_meta);
        }
    }

    if (failed && lhs) {
        for (i = 0; i < lhs->persistent.num_replicas; ++i) {
            lhs_replica_meta = lhs->replicas[i];
            if (lhs_replica_meta) {
                plat_free(lhs_replica_meta->ranges);
                plat_free(lhs_replica_meta);
            }
        }
        if (lhs->vip_meta) {
            cr_vip_meta_free(lhs->vip_meta);
        }
        plat_free(lhs);
        lhs  = NULL;
    }

    return (lhs);
}

int
cr_shard_meta_cmp(const struct cr_shard_meta *lhs,
                  const struct cr_shard_meta *rhs) {
    int ret;
    int i;
    int j;

    ret = memcmp(&lhs->persistent, &rhs->persistent, sizeof(lhs->persistent));

    for (i = 0; !ret && i < lhs->persistent.num_replicas; ++i) {
        ret = memcmp(&lhs->replicas[i]->persistent,
                     &rhs->replicas[i]->persistent,
                     sizeof (lhs->replicas[i]->persistent));
        for (j = 0; !ret && j < lhs->replicas[i]->persistent.nrange; ++j) {
            ret = memcmp(&lhs->replicas[i]->ranges[j],
                         &rhs->replicas[i]->ranges[j],
                         sizeof (lhs->replicas[i]->ranges[j]));
        }
    }

    if (!ret) {
        ret = cr_vip_meta_cmp(lhs->vip_meta, rhs->vip_meta);
    }

    return (ret);
}

SDF_status_t
cr_shard_meta_marshal(void **out, size_t *out_len, size_t buffer_offset,
                      const struct cr_shard_meta *shard) {
    const struct cr_shard_replica_meta *replica;
    int i;
    int j;
    size_t len;
    SDF_status_t ret;
    void *buf;
    char *ptr;

    plat_assert(shard);

    len = buffer_offset + shard->persistent.header.len;
    for (i = 0; i < shard->persistent.num_replicas; ++i) {
        replica = shard->replicas[i];
        len += replica->persistent.header.len;
        if (replica->persistent.nrange > 0) {
            len += replica->persistent.nrange * replica->ranges[0].header.len;
        }
    }

    buf = NULL;
    /*
     * XXX: drew 2009-10-11 Revert to plat_alloc when we move the
     * vip and lease code up a level.
     */
    ret = cr_vip_meta_marshal(&buf, &len, len, shard->vip_meta);

    if (ret == SDF_SUCCESS) {
        ptr = (char *)buf + buffer_offset;
        cr_meta_append(ptr, &shard->persistent);
        for (i = 0; i < shard->persistent.num_replicas; ++i) {
            replica = shard->replicas[i];
            cr_meta_append(ptr, &replica->persistent);
            for (j = 0; j < replica->persistent.nrange; ++j) {
                cr_meta_append(ptr, &replica->ranges[j]);
            }
        }
        *out = buf;
        *out_len = len;
    }

    return (ret);
}

SDF_status_t
cr_shard_meta_unmarshal(struct cr_shard_meta **out, const void *in,
                        size_t in_len) {
    const char *ptr;
    struct cr_shard_meta *shard;
    struct cr_shard_replica_meta *replica;
    SDF_status_t ret;
    int i;
    int j;
    ptr = (const char *)in;

    if (!plat_calloc_struct(&shard)) {
        ret = SDF_FAILURE_MEMORY_ALLOC;
    } else {
        ret = cr_meta_extract(&shard->persistent, ptr, CR_SHARD_META_MAGIC,
                              CR_SHARD_META_VERSION);
    }

    for (i = 0; ret == SDF_SUCCESS && i < shard->persistent.num_replicas; ++i) {
        if (!plat_calloc_struct(&replica)) {
            ret = SDF_FAILURE_MEMORY_ALLOC;
        } else {
            shard->replicas[i] = replica;
            ret = cr_meta_extract(&replica->persistent, ptr,
                                  CR_SHARD_REPLICA_META_MAGIC,
                                  CR_SHARD_REPLICA_META_VERSION);
            if (ret == SDF_SUCCESS) {
                replica->ranges = plat_calloc(replica->persistent.nrange,
                                              sizeof (replica->ranges[0]));
                if (!replica->ranges) {
                    ret = SDF_FAILURE_MEMORY_ALLOC;
                }
            }
            for (j = 0; ret == SDF_SUCCESS && j < replica->persistent.nrange;
                 ++j) {
                ret = cr_meta_extract(&replica->ranges[j], ptr,
                                      CR_SHARD_RANGE_MAGIC,
                                      CR_SHARD_RANGE_VERSION);

            }
        }
    }

    if (ret == SDF_SUCCESS) {
        ret = cr_vip_meta_unmarshal(&shard->vip_meta, (const void **)&ptr,
                                    in_len - (ptr - (char *)in));
    }

    if (ret == SDF_SUCCESS) {
        *out = shard;
    } else if (shard) {
        cr_shard_meta_free(shard);
    }

    return (ret);
}

char *
cr_shard_meta_to_string(const struct cr_shard_meta *shard_meta) {
    static __thread char buf[512];
    char *ptr = buf;
    int len = sizeof (buf);
    int replica_index;
    struct cr_shard_replica_meta *replica_meta;
    int range_index;
    struct cr_persistent_shard_range *range;

    plat_snprintfcat(&ptr, &len,
                     "%s sguid 0x%lx sguid_meta 0x%lx"
                     " vigg %d vig %d type %s"
                     " home %d last %d lease %3.1f lease liveness %d ltime %lld"
                     " meta_seqno %lld",
                     shard_meta->persistent.cname,
                     shard_meta->persistent.sguid,
                     shard_meta->persistent.sguid_meta,
                     shard_meta->persistent.inter_node_vip_group_group_id,
                     shard_meta->persistent.intra_node_vip_group_id,
                     sdf_replication_to_string(shard_meta->persistent.type),
                     shard_meta->persistent.current_home_node,
                     shard_meta->persistent.last_home_node,
                     shard_meta->persistent.lease_usecs/1000000.0,
                     shard_meta->persistent.lease_liveness,
                     (long long)shard_meta->persistent.ltime,
                     (long long)shard_meta->persistent.shard_meta_seqno);

    if (shard_meta->persistent.type == SDF_REPLICATION_V1_2_WAY ||
        shard_meta->persistent.type == SDF_REPLICATION_V1_N_PLUS_1) {
        plat_snprintfcat(&ptr, &len, " vip meta %s",
                         cr_vip_meta_to_string(shard_meta->vip_meta));
    } else {
        plat_snprintfcat(&ptr, &len, "replicas {");

        for (replica_index = 0;
             replica_index < shard_meta->persistent.num_replicas;
             ++replica_index) {
            replica_meta = shard_meta->replicas[replica_index];
            plat_snprintfcat(&ptr, &len,
                             " { replica %d node %u state %s nrange %d"
                             " ranges {",
                             replica_index, replica_meta->persistent.pnode,
                             cr_persistent_replica_state_to_string(replica_meta->persistent.state),
                             replica_meta->persistent.nrange);

            for (range_index = 0; range_index < replica_meta->persistent.nrange;
                 ++range_index) {
                range = &replica_meta->ranges[range_index];
                plat_snprintfcat(&ptr, &len,
                                 " { type %s start %llu len %llu }",
                                 cr_replica_range_type_to_string(range->range_type),
                                 (long long)range->start, (long long)range->len);
            }

            plat_snprintfcat(&ptr, &len, " } }");
        }

        plat_snprintfcat(&ptr, &len, " }");
    }

    if (!ptr) {
        buf[sizeof (buf) - 1] = 0;
    }

    return (buf);
}

SDF_status_t
cr_shard_meta_replace_vip_meta(struct cr_shard_meta *shard_meta,
                               const struct cr_vip_meta *in) {
    struct cr_vip_meta *replace;
    SDF_status_t ret;

    replace = cr_vip_meta_dup(in);
    if (!replace) {
        ret = SDF_FAILURE_MEMORY_ALLOC;
    } else {
        if (shard_meta->vip_meta) {
            cr_vip_meta_free(shard_meta->vip_meta);
        }
        shard_meta->vip_meta = replace;
        ret = SDF_SUCCESS;
    }

    return (ret);
}

void
cr_shard_replica_meta_add_range(struct cr_shard_replica_meta *meta,
                                enum cr_replica_range_type range_type,
                                uint64_t start, uint64_t len) {
    struct cr_persistent_shard_range *range;
    ++meta->persistent.nrange;

    if (meta->ranges) {
        meta->ranges = plat_realloc(meta->ranges, meta->persistent.nrange *
                                    sizeof (meta->ranges[0]));
    } else {
        meta->ranges = plat_malloc(meta->persistent.nrange *
                                   sizeof (meta->ranges[0]));
    }
    plat_assert_always(meta->ranges);
    range = &meta->ranges[meta->persistent.nrange - 1];
    memset(range, 0, sizeof (*range));
    range->header.magic.integer = CR_SHARD_RANGE_MAGIC;
    range->header.len = sizeof (*range);
    range->header.version = CR_SHARD_RANGE_VERSION;
    range->start = start;
    range->len = len;
    range->range_type = range_type;
}

enum {
    START_INVALID = -1
};

/*
 * The canonical representation does not have two adjacent ranges of the
 * same type.
 */
void
cr_shard_replica_meta_make_canonical(struct cr_shard_replica_meta *meta) {
    struct cr_persistent_shard_range *range;
    /** @brief Address of last range entry */
    struct cr_persistent_shard_range *last_range;
    struct cr_persistent_shard_range *move_range;

    plat_assert(meta->persistent.nrange >= 0);

    range = &meta->ranges[0];
    last_range = range + meta->persistent.nrange - 1;
    while (range + 1 <= last_range) {
        /* There must be no gaps between ranges */
        plat_assert(range[0].start + range[0].len == range[1].start);
        plat_assert(range[0].range_type != CR_REPLICA_RANGE_ACTIVE);
        plat_assert(range[0].len != CR_SHARD_RANGE_OPEN);

        plat_assert_imply(range[1].len == CR_SHARD_RANGE_OPEN,
                          range[1].range_type == CR_REPLICA_RANGE_ACTIVE);
        plat_assert_imply(range[1].range_type == CR_REPLICA_RANGE_ACTIVE,
                          range + 1 == last_range);

        /* So two adjacent ranges of the same type can be combined */
        if (range[0].range_type == range[1].range_type) {
            if (range[1].len == CR_SHARD_RANGE_OPEN) {
                plat_assert(range + 1 == last_range);
                plat_assert(range[1].range_type == CR_REPLICA_RANGE_ACTIVE);
                range[0].len = CR_SHARD_RANGE_OPEN;
            } else {
                range[0].len += range[1].len;
            }
            /* Move all following ranges up one */
            for (move_range = range + 1; move_range + 1 <= last_range;
                 ++move_range) {
                move_range[0] = move_range[1];
            }
            --last_range;
            --meta->persistent.nrange;
        } else {
            ++range;
        }
    }
}

const char *
cr_replica_range_type_to_string(enum cr_replica_range_type range_type) {
    switch (range_type) {
#define item(caps) case caps: return (#caps);
    CR_REPLICA_RANGE_TYPE_ITEMS()
#undef item
    }
    plat_assert(0);
}

const char *
cr_persistent_replica_state_to_string(enum cr_persistent_replica_state
                                      replica_state) {
    switch (replica_state) {
#define item(caps, lower) case caps: return (#lower);
    CR_PERSISTENT_REPLICA_STATE_ITEMS()
#undef item
    }
    plat_assert(0);
}

struct cr_vip_meta *
cr_vip_meta_alloc() {
    struct cr_vip_meta *ret;
    int failed;
    int len = 0; /* silence gcc */

    failed = !plat_calloc_struct(&ret);
    if (!failed) {
        len = cr_persistent_vip_meta_len(0);
        ret->persistent = (struct cr_persistent_vip_meta *)plat_calloc(1,  len);
        failed = !ret->persistent;
    }

    if (!failed) {
        ret->persistent->header.magic.integer = CR_VIP_META_MAGIC;
        ret->persistent->header.len = len;
        ret->persistent->header.version = CR_VIP_META_VERSION;
        ret->persistent->num_shards = 0;
    }

    if (failed && ret) {
        cr_vip_meta_free(ret);
        ret = NULL;
    }

    return (ret);
}

struct cr_vip_meta *
cr_vip_meta_dup(const struct cr_vip_meta *meta) {
    struct cr_vip_meta *ret;
    int failed;
    int len;

    len = cr_persistent_vip_meta_len(meta->persistent->num_shards);
    failed = !plat_calloc_struct(&ret);
    if (!failed) {
        memcpy(ret, meta, sizeof (*ret));
        ret->persistent = plat_calloc(1, len);
        failed = !ret->persistent;
    }

    if (!failed) {
        memcpy(ret->persistent, meta->persistent, len);
    }

    if (failed && ret) {
        cr_vip_meta_free(ret);
        ret = NULL;
    }

    return (ret);
}

void
cr_vip_meta_free(struct cr_vip_meta *meta) {
    if (meta) {
        if (meta->persistent) {
            plat_free(meta->persistent);
        }
        plat_free(meta);
    }
}

/** @brief Add sguid to #cr_vip_meta */
SDF_status_t
cr_vip_meta_set_shard_state(struct cr_vip_meta *meta, SDF_shardid_t sguid,
                            enum cr_vip_meta_shard_state state) {
    SDF_status_t ret;
    struct cr_persistent_vip_meta *tmp;
    int len;
    int i;
  
    len = cr_persistent_vip_meta_len(meta->persistent->num_shards + 1);

    i = cr_vip_meta_find_shard(meta, sguid);

    if (0 <= i && i < meta->persistent->num_shards) {
        meta->persistent->shards[i].state = state;
        ret = SDF_SUCCESS;
    } else {
        tmp = plat_realloc(meta->persistent, len);
        if (!tmp) {
            ret = SDF_FAILURE_MEMORY_ALLOC;
        } else {
            tmp->header.len = len;
            tmp->shards[tmp->num_shards].sguid = sguid;
            tmp->shards[tmp->num_shards].state = state;
            ++tmp->num_shards;
            meta->persistent = tmp;
            ret = SDF_SUCCESS;
        }
    }

    return (ret);
}

void
cr_vip_meta_set_all_shard_state(struct cr_vip_meta *meta,
                                enum cr_vip_meta_shard_state state) {
    int i;

    for (i = 0; i < meta->persistent->num_shards; ++i) {
        meta->persistent->shards[i].state = state;
    }
}

/** @brief Remove sguid from #cr_vip_meta */
SDF_status_t
cr_vip_meta_remove(struct cr_vip_meta *meta, SDF_shardid_t sguid) {
    int i;
    SDF_status_t ret;

    i = cr_vip_meta_find_shard(meta, sguid);
    if (i < 0 || i >= meta->persistent->num_shards) {
        ret = SDF_SHARD_NOT_FOUND;
    } else {
        --meta->persistent->num_shards;
        for (; i < meta->persistent->num_shards; ++i) {
            meta->persistent->shards[i] = meta->persistent->shards[i + 1];
        }
        meta->persistent->header.len =
            cr_persistent_vip_meta_len(meta->persistent->num_shards);
        ret = SDF_SUCCESS;
    }

    return (ret);
}

int
cr_vip_meta_contains(struct cr_vip_meta *meta, SDF_shardid_t sguid) {
    int i;

    i = cr_vip_meta_find_shard(meta, sguid);

    return (0 <= i && i <= meta->persistent->num_shards);
}

/** @brief Return index of sguid in meta, current sguid count on failure */
static int
cr_vip_meta_find_shard(struct cr_vip_meta *meta, SDF_shardid_t sguid) {
    int i;

    for (i = 0; i < meta->persistent->num_shards &&
         meta->persistent->shards[i].sguid != sguid; ++i) {
    }

    return (i);
}

int
cr_vip_meta_is_recovered(struct cr_vip_meta *meta) {
    int i;
    int ret;
    struct cr_vip_meta_shard *shard;

    for (ret = 1, i = 0; ret && i < meta->persistent->num_shards; ++i) {
        shard = &meta->persistent->shards[i];
        if (shard->state == CR_VMSS_UNRECOVERED) {
            ret = 0;
        }
    }

    return (ret);
}

/** @brief Return length of persistent #cr_persistent_vip_meta */
static size_t
cr_persistent_vip_meta_len(int count) {
    struct cr_persistent_vip_meta *meta = NULL;

    return (sizeof (*meta) + count * sizeof (meta->shards[0]));
}

int
cr_vip_meta_cmp(const struct cr_vip_meta *lhs,
                const struct cr_vip_meta *rhs) {
    int ret;
    size_t cmp_len;

    /* XXX: drew 2009-09-02 Fix it so that we always have lhs and rhs */
    if (lhs && rhs) {
        ret = 0;
    } else if (lhs) {
        ret = 1;
    } else {
        plat_assert(rhs);
        ret = -1;
    }

    if (!ret) {
        cmp_len =
            cr_persistent_vip_meta_len(PLAT_MIN(lhs->persistent->num_shards,
                                                rhs->persistent->num_shards));
        ret = memcmp(lhs->persistent, rhs->persistent, cmp_len);
    }

    if (!ret) {
        if (lhs->persistent->num_shards == rhs->persistent->num_shards) {
            ret = 0;
        } else if (lhs->persistent->num_shards >
                   rhs->persistent->num_shards) {
            ret = 1;
        } else {
            plat_assert(lhs->persistent->num_shards <
                        rhs->persistent->num_shards);
            ret = -1;
        }
    }

    return (ret);
}

SDF_status_t
cr_vip_meta_marshal(void **out, size_t *out_len, size_t buffer_offset,
                    const struct cr_vip_meta *in) {
    size_t len;
    SDF_status_t ret;
    char *buf;

    len = buffer_offset + in->persistent->header.len;

    buf = plat_calloc(1, len);
    if (!buf) {
        ret = SDF_FAILURE_MEMORY_ALLOC;
    } else {
        memcpy(buf + buffer_offset, in->persistent,
               in->persistent->header.len);
        *out = buf;
        *out_len = len;
        ret = SDF_SUCCESS;
    }

    return (ret);
}

SDF_status_t
cr_vip_meta_unmarshal(struct cr_vip_meta **out, const void **inout,
                      size_t inout_len) {
    const char *ptr;
    struct cr_vip_meta *meta;
    SDF_status_t ret;
    struct cr_persistent_header header = {};
    const char *reason;

    ptr = (const char *)*inout;
    reason = "";

    if (sizeof (header) > inout_len) {
        ret = SDF_META_DATA_INVALID;
        reason = "too few bytes";
    } else {
        ret = SDF_SUCCESS;
    }

    meta = NULL;

    if (ret == SDF_SUCCESS) {
        memcpy(&header, ptr, sizeof (header));
        if (header.magic.integer != CR_VIP_META_MAGIC) {
            ret = SDF_META_DATA_INVALID;
            reason = "magic";
        } else if (header.version > CR_VIP_META_VERSION) {
            ret = SDF_META_DATA_VERSION_TOO_NEW;
            reason = "version too new";
        } else if (header.version != CR_VIP_META_VERSION) {
            ret = SDF_META_DATA_INVALID;
            reason = "version mismatch";
        } else if (header.len < sizeof (*meta->persistent)) {
            ret = SDF_META_DATA_INVALID;
            reason = "len incorrect";
        } else if (header.len > inout_len) {
            ret = SDF_META_DATA_INVALID;
            reason = "short read";
        }
    }

    if (ret == SDF_SUCCESS && !plat_calloc_struct(&meta)) {
        ret = SDF_FAILURE_MEMORY_ALLOC;
    }

    if (ret == SDF_SUCCESS) {
        meta->persistent = plat_alloc(header.len);
        if (!meta->persistent) {
            ret = SDF_FAILURE_MEMORY_ALLOC;
        } else {
            memcpy(meta->persistent, ptr,  header.len);
            ptr += header.len;
        }
    }

    if (ret == SDF_SUCCESS && header.len !=
        cr_persistent_vip_meta_len(meta->persistent->num_shards)) {
        ret = SDF_META_DATA_INVALID;
        reason = "len incorrect";
    }

    if (ret == SDF_SUCCESS) {
        *out = meta;
        *inout = ptr;
    } else {
        plat_log_msg(21424, LOG_CAT, PLAT_LOG_LEVEL_INFO,
                     "error reading vip meta: %s %s",
                     sdf_status_to_string(ret), reason);

        cr_vip_meta_free(meta);
    }

    return (ret);
}

char *
cr_vip_meta_to_string(const struct cr_vip_meta *meta) {
    static __thread char buf[512];
    char *ptr;
    int len;
    int i;
    struct cr_vip_meta_shard *shard;

    ptr = buf;
    len = sizeof (buf);

    plat_snprintfcat(&ptr, &len, "{ ");
    for (i = 0; i < meta->persistent->num_shards; ++i) {
        shard = &meta->persistent->shards[i];
        plat_snprintfcat(&ptr, &len, "{ sguid 0x%lx state %s } ", shard->sguid,
                         cr_vip_meta_shard_state_to_string(shard->state));
    }
    plat_snprintfcat(&ptr, &len, "}");

    return (buf);
}

static int
cr_persistent_vip_meta_shard_cmp(const void *lhs, const void *rhs) {
    const struct cr_persistent_vip_meta *tmp = NULL;

    return (memcmp(lhs, rhs, sizeof (tmp->shards[0])));
}

const char *
cr_vip_meta_shard_state_to_string(enum cr_vip_meta_shard_state state) {
    switch (state) {
#define item(caps, lower) case caps: return (#lower);
    CR_VIP_META_SHARD_STATE_ITEMS()
#undef item
    }
    plat_assert(0);
}
