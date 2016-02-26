/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/protocol/replication/meta.c
 *
 * Author: drew
 *
 * Created on October 23, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: meta.c 4001 2008-10-23 23:12:09Z drew $
 */

/**
 * Encapsulate the replication layer meta-data
 */

#include "meta.h"

int
replication_meta_need_meta_shard(const SDF_container_props_t *props) {
    return ((props->replication.enabled &&
            props->replication.type != SDF_REPLICATION_SIMPLE &&
            props->replication.type != SDF_REPLICATION_NONE) ? 1 : 0);
}
