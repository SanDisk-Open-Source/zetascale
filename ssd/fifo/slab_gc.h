/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef __ZS_SLAB_GC_H__
#define __ZS_SLAB_GC_H__

#include "mcd_osd.h"

bool slab_gc_init(mcd_osd_shard_t* shard, int threshold);

void slab_gc_signal(mcd_osd_shard_t* shard, mcd_osd_slab_class_t* class);

void slab_gc_end(mcd_osd_shard_t* shard);

bool slab_gc_update_threshold(mcd_osd_shard_t *shard, int threshold);

void slab_gc_get_stats(mcd_osd_shard_t* shard, ZS_stats_t* stats, FILE* log);

#define STAT(A) \
    ZS_FLASH_STATS_SLAB_GC_ ## A

#endif /* __ZS_SLAB_GC_H__ */
