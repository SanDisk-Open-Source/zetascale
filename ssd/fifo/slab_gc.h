//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

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
