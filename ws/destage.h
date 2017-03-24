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
 *  destage.h   11/29/16   Brian O'Krafka   
 *
 *  Code for destaging writes.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 **********************************************************************/

#ifndef _DESTAGE_H
#define _DESTAGE_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>
#include <time.h>
#include <string.h>
#include "ws/wsstats.h"


struct destager;
struct ZS_state;
struct ZS_thread_state;
struct btree;

extern struct destager *pDestager; // global state pointer

#define MAX_FILENAME_LEN  1024

  /*  Callback for allocating per-thread ZS state.
   *  Returns 0 for success, non-zero for error.
   */
typedef int (*ds_per_thread_state_cb_t)(struct ZS_state *pzs, struct ZS_thread_state **pzst);

  /*  Callback for getting number of ZS ops for stats calculations.
   *  Returns 0 for success, non-zero for error.
   */

typedef int (*ds_stats_ops_cb_t)(struct ZS_thread_state *pzs, void *state, uint64_t *n_rd, uint64_t *n_wr, uint64_t *n_del);

typedef struct destager_config {
    struct ZS_state          *pzs;
    uint32_t                  pool_size;
    uint32_t                  batch_commit;
    uint32_t                  trace_on;
    uint32_t                  batch_usecs;
    uint32_t                  n_batch_bufs;
    uint32_t                  batch_buf_kb;
    char                      batch_file[MAX_FILENAME_LEN];
    char                      stats_file[MAX_FILENAME_LEN];
    uint32_t                  stats_steady_state_secs;
    uint32_t                  stats_secs;
    ds_per_thread_state_cb_t  per_thread_cb;
    ds_stats_ops_cb_t         client_ops_cb;
    void                     *cb_state;
} destager_config_t;

    /*  Stats counter names and string functions.
     */

    // Write Destaging Stats

#define DS_RAW_STAT_ITEMS() \
    item(N_CLIENT_RD) \
    item(N_CLIENT_WR) \
    item(N_CLIENT_DEL) \
    item(N_LATD_MISSES) \
    item(N_LATD_HITS) \
    item(N_POINT_READ) \
    item(N_RANGE_READ) \
    item(N_WRITE) \
    item(N_DELETE) \
    item(POINT_READ_BYTES) \
    item(RANGE_READ_BYTES) \
    item(WRITE_BYTES) \
    item(N_TXN_START) \
    item(N_TXN_COMMIT) \
    item(N_TXN_ABORT) \
    item(N_DS_RAW_STATS)

typedef enum ds_raw_stat_names {
#define item(v) v,
    DS_RAW_STAT_ITEMS()
#undef item
} ds_raw_stat_names_t;

#ifdef DESTAGE_C
const char *DSRawStatString(int n) {
#define item(v) case v: return (#v);
    switch (n) {
    DS_RAW_STAT_ITEMS()
    default:
        return ("Invalid");
    }
#undef item
}
#endif


#define DS_STAT_ITEMS() \
    item(K_CLIENT_RD_RATE) \
    item(K_CLIENT_WR_RATE) \
    item(K_CLIENT_DEL_RATE) \
    item(K_READ_PT_RATE) \
    item(K_READ_RANGE_RATE) \
    item(K_WRITE_RATE) \
    item(K_DELETE_RATE) \
    item(KBYTES_PER_READ_PT) \
    item(KBYTES_PER_READ_RANGE) \
    item(KBYTES_PER_WRITE) \
    item(K_TXN_START_RATE) \
    item(K_TXN_COMMIT_RATE) \
    item(K_TXN_ABORT_RATE) \
    item(PCT_LAT_MISS_RATE) \
    item(N_DS_STATS)

typedef enum ds_stat_names {
#define item(v) v,
    DS_STAT_ITEMS()
#undef item
} ds_stat_names_t;

#ifdef DESTAGE_C
const char *DSStatString(int n) {
#define item(v) case v: return (#v);
    switch (n) {
    DS_STAT_ITEMS()
    default:
        return ("Invalid");
    }
#undef item
}
#endif

typedef struct ds_substats {
    struct timeval     tspec;
    uint64_t          *ds_raw_stat;
    double            *ds_stat;
} ds_substats_t;

typedef struct ds_stats {
    int                      in_steady_state;

    ds_substats_t            stats;         //  current sample
    ds_substats_t            stats_t0;      //  initial sample (at t=0)
    ds_substats_t            stats_ss0;     //  at beginning of steady-state
    ds_substats_t            stats_last;    //  previous sample
    ds_substats_t            stats_overall; //  since start of run
    ds_substats_t            stats_ss;      //  since start of steady-state
    ds_substats_t            stats_window;  //  since last report

    wsrt_statistics_t        read_pt_bytes;
    wsrt_statistics_t        read_range_counts;
    wsrt_statistics_t        read_range_bytes;

    wsrt_statistics_t        write_bytes;
    wsrt_statistics_t        batch_counts;
    wsrt_statistics_t        batch_bytes;

} ds_stats_t;

void ds_load_default_config(destager_config_t *cfg);
struct destager *destage_init(struct ZS_state *pzs, destager_config_t *cfg);
void destage_dump_config(FILE *f, struct destager *pds);
void destage_destroy(struct destager *pds);
void destage_dump(FILE *f, struct destager *pds);
int destage_check(FILE *f, struct destager *pds);
void destage_stats(struct ZS_thread_state *pzs, struct destager *pds, ds_stats_t *stats);
void destage_dump_stats(FILE *f, struct ZS_thread_state *pzs, struct destager *pds);
int destage_read(struct destager *pds, struct ZS_thread_state *pzst, uint64_t cguid, struct btree *bt, char *key, uint32_t key_len, void *pdata, uint64_t *size, uint32_t flags);
int destage_write(struct destager *pds, struct ZS_thread_state *pzst, uint64_t cguid, struct btree *bt, char *key, uint32_t key_len, void *pdata, uint64_t size, uint32_t flags);
int destage_delete(struct destager *pds, struct ZS_thread_state *pzst, uint64_t cguid, struct btree *bt, char *key, uint32_t key_len, uint32_t flags);
int destage_txn_start(struct destager *pds, struct ZS_thread_state *pzst, uint32_t flags);
int destage_txn_commit(struct destager *pds, struct ZS_thread_state *pzst, uint32_t flags);
int destage_txn_abort(struct destager *pds, struct ZS_thread_state *pzst, uint32_t flags);

#endif
