/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif

/**
 * @brief Return whether shard guids must be allocated for meta data
 * 
 * A non-zero return indicates that one meta-data shard should be allocated for 
 * each data shard.
 */
int replication_meta_need_meta_shard(const SDF_container_props_t *props);

#endif /* ndef REPLICATION_META_H */
