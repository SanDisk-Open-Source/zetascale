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

/**********************************************************************
 *
 *  ws_internal.h   8/29/16   Brian O'Krafka   
 *
 *  Write serializer module for ZetaScale b-tree.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 **********************************************************************/

#ifndef _WS_INTERNAL_H
#define _WS_INTERNAL_H

#include "ws.h"

#define MAX_WRITE_BATCH 1024

typedef struct ws_stripe_entry {
    int16_t        cnt;
    int16_t        n_group;
    uint32_t       next;
    uint32_t       prev;
} ws_stripe_entry_t;

typedef struct ws_fill_groups {
    pthread_mutex_t       free_lock;
    uint32_t              free_list;
    int                   n_fill_groups;
    pthread_mutex_t       group_lock[WS_MAX_FILL_GROUPS];
    uint32_t              fill_group_percents[WS_MAX_FILL_GROUPS];
    uint32_t              fill_groups[WS_MAX_FILL_GROUPS];
} ws_fill_groups_t;

typedef struct ws_stripe_tbl {
    uint64_t              n_entries;
    uint64_t              entries_per_dump;
    ws_stripe_entry_t    *entries;
} ws_stripe_tbl_t;

#define WS_SBUF_STATE_ITEMS() \
    item(Unused) \
    item(Open) \
    item(Full_pending_io) \
    item(Available_for_gc) \
    item(In_gc) \
    item(Compacted_and_ready) \
    item(Next_Open)

typedef enum ws_sbuf_state_names {
#define item(v) v,
    WS_SBUF_STATE_ITEMS()
#undef item
} ws_sbuf_state_t;

#ifdef WS_C
const char *WSSbufStateString(ws_sbuf_state_t n) {
#define item(v) case v: return (#v);
    switch (n) {
    WS_SBUF_STATE_ITEMS()
    default:
        return ("INVALID");
    }
#undef item
}
#endif

typedef struct ws_stripe_buf {
    uint32_t               n;
    uint64_t               n_stripe;
    char                  *buf;
    uint64_t               p;
    uint64_t               bytes_when_full;
    uint64_t               bytes_written_so_far;
    ws_sbuf_state_t        state;
    struct ws_stripe_buf  *next;
} ws_stripe_buf_t;

#define N_LAT_BUCKETS     1024
#define N_LAT_FREE_LISTS  16

typedef struct lat_data {
    ws_stripe_buf_t   *sbuf;
} lat_data_t;

typedef struct write_mail {
    mbox_t          *mbox_return;
    ws_stripe_buf_t *sbuf;
    uint64_t         p;
    uint64_t         size;
} write_mail_t;

typedef struct ws_state {

    struct ZS_state         *zs_state;

    FILE                    *f_stats;
    uint64_t                 next_check_cnt;

    struct alat             *lat; // data lookaside table
    ws_fill_groups_t         fill_groups;
    ws_stripe_tbl_t          stripe_tbl;
    ws_config_t              config;
    int                      trace_on;
    uint64_t                 stripe_bytes;
    uint64_t                 stripe_sectors;
    uint64_t                 sector_bytes;
    uint64_t                 n_stripes;
    uint32_t                 user_hz;

    ws_stats_t               stats;

    mbox_t                   mbox_free;
    mbox_t                   mbox_write;
    mbox_t                   mbox_gc;
    mbox_t                   mbox_batch;
    ws_stripe_buf_t         *sbufs;
    ws_stripe_buf_t         *curbuf;
    uint64_t                *curptr;
    threadpool_t            *stripe_tbl_pool;
    threadpool_t            *gc_threads;
    threadpool_t            *scrub_threads;
    threadpool_t            *batch_threads;
    threadpool_t            *io_threads;
    threadpool_t            *stats_thread;

    pthread_mutex_t          mutex;
    pthread_cond_t           write_cv;
    uint32_t                 nwaiters;
    int                      is_terminating;
    int                      failed_to_start;

    pthread_mutex_t          quiesce_mutex;
    pthread_cond_t           quiesce_cv;
    uint32_t                 quiesce;
    uint32_t                 writes_in_progress;
    uint32_t                 deletes_in_progress;
    uint32_t                 stripe_writes_in_progress;
    uint32_t                 batch_writes_in_progress;
    uint32_t                 gcs_in_progress;
    uint32_t                 scrubs_in_progress;

} ws_state_t;

#endif
