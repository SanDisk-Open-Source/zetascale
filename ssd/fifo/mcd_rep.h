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

/*
 * File:   apps/memcached/server/memcached-1.2.5-schooner/mcd_rep.h
 * Author: Jake Moilanen
 *
 * Created on May 13, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: 
 */

#ifndef __MCD_REP_H__
#define __MCD_REP_H__

typedef struct rep_cursor {
    uint64_t seqno;
    uint16_t syndrome;
    uint16_t tombstone:1;
    uint16_t reserved:3;    
    uint16_t blocks:12;
    uint32_t blk_offset;
    uint32_t bucket;
} rep_cursor_t;

// The index will be the logbuf index into the log buff
typedef struct seqno_logbuf_cache {
    uint64_t seqno_start;
    uint64_t seqno_end;
} seqno_logbuf_cache_t;

typedef enum {
    LOG_TABLE = 1,
    OBJECT_TABLE = 2,
    VOLATILE_OBJECT_TABLE = 3
} cursor_state_t;

int rep_get_iteration_cursors(struct shard *shard, uint64_t seqno_start,
                              uint64_t seqno_len, 
                              uint64_t seqno_max,
                              const resume_cursor_t * resume_cursor_in,
                              it_cursor_t ** out);

int rep_get_by_cursor(struct shard *shard, int cursor_len,
		      const void *cursor, struct objMetaData *metaData,
                      char **key, void **data, int flags, time_t flush_time);

int rep_logbuf_seqno_update(struct shard * shard, uint64_t logbuf_num, uint64_t seq_num);
int rep_logbuf_seqno_invalidate(struct shard * shard, uint64_t logbuf_num);
int replication_init(void);
int seqno_cache_init(struct shard *shard);
int rep_iterate_cursors_progress(struct shard * shard, resume_cursor_t * cursor);

extern struct SDF_shared_state sdf_shared_state;
uint64_t rep_seqno_get(struct shard * shard);

#endif
