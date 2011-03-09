#ifndef REPLICATION_META_H
#define REPLICATION_META_H 1

#include "meta_types.h"

/*
 * File:   sdf/protocol/replication/meta.h
 *
 * Author: drew
 *
 * Created on October 23, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: meta.h 4001 2008-10-23 23:12:09Z drew $
 */

/**
 * Encapsulate the replication layer meta-data
 */
#include "common/sdf_properties.h"

/**
 * @brief Return whether shard guids must be allocated for meta data
 * 
 * A non-zero return indicates that one meta-data shard should be allocated for 
 * each data shard.
 */
int replication_meta_need_meta_shard(const SDF_container_props_t *props);

#endif /* ndef REPLICATION_META_H */
