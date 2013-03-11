#ifndef __FDF_SLAB_GC_H__
#define __FDF_SLAB_GC_H__

#include "mcd_osd.h"

bool slab_gc_init(mcd_osd_shard_t* shard, int threshold);

void slab_gc_signal(mcd_osd_shard_t* shard, mcd_osd_slab_class_t* class);

void slab_gc_end(mcd_osd_shard_t* shard);

void slab_gc_update_threshold(mcd_osd_shard_t *shard, int threshold);

void slab_gc_get_stats(mcd_osd_shard_t* shard, FDF_stats_t* stats, FILE* log);

#define STAT(A) \
    FDF_FLASH_STATS_SLAB_GC_ ## A

#endif /* __FDF_SLAB_GC_H__ */
