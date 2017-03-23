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
 *  ws.h   8/29/16   Brian O'Krafka   
 *
 *  Write serializer module for ZetaScale b-tree.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 **********************************************************************/

#ifndef _WS_H
#define _WS_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>
#include <time.h>
#include <string.h>
#include "wsstats.h"

#define WS_MAX_DEVICES       128
#define WS_MAX_FILL_GROUPS   100
// #define WS_MAX_STRIPE_SIZE   (16*1024*1024)
#define WS_MAX_STRIPE_SIZE   (1*1024*1024)
#define WS_NULL_STRIPE_ENTRY UINT32_MAX

//  xxxzzz create strings for these
#define WS_OK                 0
#define WS_ERROR              1
#define WS_READ_ONLY          2
#define WS_DELETE_ERROR       3
#define WS_EARLY_TERMINATION  4

struct ZS_state;
struct ZS_thread_state;

//  Callback for GC
  /*  returns:
   *    WS_NOT_COLLECTED
   *    WS_COLLECTED
   *    WS_ERROR
   */
typedef int (*ws_gc_cb_t)(void *state, void *pbuf, uint64_t *p_out, uint32_t stripe_bytes, uint32_t sector_bytes, uint64_t stripe_addr, uint64_t *compacted_bytes);

/*  Callback used by btree_traversal_cb to check a single node
 */
typedef void (node_check_cb_t)(void *pdata, uint64_t addr);

/*  Callback for traversing B-tree for comprehensive checks during debug.
 */
typedef int (*ws_btree_traversal_cb_t)(void *pdata, void *pzst, node_check_cb_t *check_cb);

//  Callback for allocating per-thread ZS state
  /*  returns:
   *    WS_OK
   *    WS_ERROR
   */
typedef int (*ws_per_thread_state_cb_t)(struct ZS_state *pzs, struct ZS_thread_state **pzst);

//  Callback for getting number of ZS ops for stats calculations
  /*  returns:
   *    WS_OK
   *    WS_ERROR
   */
typedef int (*ws_stats_ops_cb_t)(struct ZS_thread_state *pzs, void *state, uint64_t *n_ops);

//  Callback for setting metadata (transactionally!)
  /*  returns:
   *    WS_OK
   *    WS_ERROR
   */
typedef int (*ws_set_metadata_cb_t)(struct ZS_thread_state *pzs, void *state, void *key, uint32_t key_size, void *data, uint32_t data_size);

//  Callback for getting metadata
  /*  returns:
   *    WS_OK
   *    WS_ERROR
   */
typedef int (*ws_get_metadata_cb_t)(struct ZS_thread_state *pzs, void *state, void *key, uint32_t key_size, void *data, uint32_t max_size, uint32_t *md_size);

#define WS_LINE_SIZE   1024

typedef struct ws_config {
    int                       n_devices;
    int                       fd_devices[WS_MAX_DEVICES];
    int                       batch_fd;
    char                      stat_device_string[WS_LINE_SIZE];
    uint32_t                  chunk_kbytes;
    uint64_t                  device_mbytes;
    uint64_t                  device_offset_mb;
    ws_gc_cb_t                gc_cb;
    ws_btree_traversal_cb_t   btree_traversal_cb;
    ws_per_thread_state_cb_t  per_thread_cb;
    ws_get_metadata_cb_t      get_md_cb;
    ws_set_metadata_cb_t      set_md_cb;
    ws_stats_ops_cb_t         get_client_ops_cb;
    void                     *cb_state;

    uint32_t                  percent_op;
    uint64_t                  stripe_tbl_bytes_per_dump;
    uint32_t                  stripe_tbl_dump_usecs;
    uint32_t                  scrub_usecs;
    uint32_t                  quiesce_usecs;
    uint32_t                  n_stripe_bufs;
    uint64_t                  gc_per_read_bytes;
    int                       n_gc_threads;
    int                       n_scrub_threads;
    int                       n_io_threads;
    int                       n_stripe_tbl_threads;
    uint32_t                  batch_size;

    uint32_t                  sector_size;
    uint32_t                  sectors_per_node;

    int                       trace_on;
    char                      stats_file[WS_LINE_SIZE];
    uint32_t                  stats_steady_state_secs;
    uint32_t                  stats_secs;
    uint32_t                  check_interval;

    int                       n_fill_groups;
    uint32_t                  fill_group_percents[WS_MAX_FILL_GROUPS];

} ws_config_t;

    /*  Stats counter names and string functions.
     */

    // Write Serialization Stats

#define WS_RAW_STAT_ITEMS() \
    item(N_CLIENT_OPS) \
    item(N_WRITES) \
    item(N_WRITE_BYTES) \
    item(N_UPDATES) \
    item(N_UPDATE_BYTES) \
    item(N_READS) \
    item(N_READ_BYTES) \
    item(N_DELETES) \
    item(N_DELETE_BYTES) \
    item(N_GCS) \
    item(N_GC_BYTES) \
    item(N_EXTRA_COND_WAITS) \
    item(N_MD_GET) \
    item(N_MD_GET_BYTES) \
    item(N_MD_SET) \
    item(N_MD_SET_BYTES) \
    item(N_IO_STRIPE_READ) \
    item(N_IO_STRIPE_READ_BYTES) \
    item(N_IO_POINT_READ) \
    item(N_IO_POINT_READ_BYTES) \
    item(N_IO_STRIPE_WRITE) \
    item(N_IO_STRIPE_WRITE_BYTES) \
    item(N_STRIPE_TBL_DUMPS) \
    item(N_LAT_HITS) \
    item(N_LAT_MISSES) \
    item(N_WS_RAW_STATS)

typedef enum ws_raw_stat_names {
#define item(v) v,
    WS_RAW_STAT_ITEMS()
#undef item
} ws_raw_stat_names_t;

#ifdef WS_C
const char *WSRawStatString(int n) {
#define item(v) case v: return (#v);
    switch (n) {
    WS_RAW_STAT_ITEMS()
    default:
        return ("Invalid");
    }
#undef item
}
#endif


#define WS_STAT_ITEMS() \
    item(WRITE_AMP) \
    item(K_CLIENT_RATE) \
    item(K_READ_RATE) \
    item(K_WRITE_RATE) \
    item(K_UPDATE_RATE) \
    item(K_DELETE_RATE) \
    item(STRIPE_TBL_RATE) \
    item(N_WS_STATS)

typedef enum ws_stat_names {
#define item(v) v,
    WS_STAT_ITEMS()
#undef item
} ws_stat_names_t;

#ifdef WS_C
const char *WSStatString(int n) {
#define item(v) case v: return (#v);
    switch (n) {
    WS_STAT_ITEMS()
    default:
        return ("Invalid");
    }
#undef item
}
#endif

    // CPU Utilization Stats

#define WS_CPU_RAW_STAT_ITEMS() \
    item(CPU_USR_CNT) \
    item(CPU_NICE_CNT) \
    item(CPU_SYS_CNT) \
    item(CPU_IDLE_CNT) \
    item(CPU_IOWAIT_CNT) \
    item(N_CPU_RAW_STATS)

typedef enum ws_cpu_raw_stat_names {
#define item(v) v,
    WS_CPU_RAW_STAT_ITEMS()
#undef item
} ws_cpu_raw_stat_names_t;

#ifdef WS_C
const char *WSCPURawStatString(int n) {
#define item(v) case v: return (#v);
    switch (n) {
    WS_CPU_RAW_STAT_ITEMS()
    default:
        return ("Invalid");
    }
#undef item
}
#endif

#define WS_CPU_STAT_ITEMS() \
    item(CPU_UTIL_TOT) \
    item(CPU_UTIL_USR) \
    item(CPU_UTIL_SYS) \
    item(CPU_UTIL_IOWAIT) \
    item(CPU_USECS_TOT_PER_OP) \
    item(CPU_USECS_USR_PER_OP) \
    item(CPU_USECS_SYS_PER_OP) \
    item(CPU_USECS_IOWAIT_PER_OP) \
    item(CPU_USECS_TOT_PER_CLIENT_OP) \
    item(CPU_USECS_USR_PER_CLIENT_OP) \
    item(CPU_USECS_SYS_PER_CLIENT_OP) \
    item(CPU_USECS_IOWAIT_PER_CLIENT_OP) \
    item(N_CPU_STATS)

typedef enum ws_cpu_stat_names {
#define item(v) v,
    WS_CPU_STAT_ITEMS()
#undef item
} ws_cpu_stat_names_t;

#ifdef WS_C
const char *WSCPUStatString(int n) {
#define item(v) case v: return (#v);
    switch (n) {
    WS_CPU_STAT_ITEMS()
    default:
        return ("Invalid");
    }
#undef item
}
#endif


    // I/O Utilization Stats (Per I/O Device)

#define WS_IO_RAW_STAT_ITEMS() \
    item(IOP_RD_CNT) \
    item(IOP_WR_CNT) \
    item(IOP_RD_MERGE_CNT) \
    item(IOP_WR_MERGE_CNT) \
    item(BYTES_RD_CNT) \
    item(BYTES_WR_CNT) \
    item(MSEC_RD_CNT) \
    item(MSEC_WR_CNT) \
    item(MSEC_IO_CNT) \
    item(MSEC_IO_WEIGHTED_CNT) \
    item(IOPS_IN_PROGRESS) \
    item(N_IO_RAW_STATS)

typedef enum ws_io_raw_stat_names {
#define item(v) v,
    WS_IO_RAW_STAT_ITEMS()
#undef item
} ws_io_raw_stat_names_t;

#ifdef WS_C
const char *WSIORawStatString(int n) {
#define item(v) case v: return (#v);
    switch (n) {
    WS_IO_RAW_STAT_ITEMS()
    default:
        return ("Invalid");
    }
#undef item
}
#endif

#define WS_IO_STAT_ITEMS() \
    item(IOP_RD_PER_CLIENT_OP) \
    item(IOP_WR_PER_CLIENT_OP) \
    item(K_IOBYTES_RD_PER_CLIENT_OP) \
    item(K_IOBYTES_WR_PER_CLIENT_OP) \
    item(IOP_RD_PER_OP) \
    item(IOP_WR_PER_OP) \
    item(K_IOBYTES_RD_PER_OP) \
    item(K_IOBYTES_WR_PER_OP) \
    item(K_BYTES_PER_RD_IOP) \
    item(K_BYTES_PER_WR_IOP) \
    item(K_IOPS_RD_PER_SEC) \
    item(K_IOPS_WR_PER_SEC) \
    item(IOPS_RD_MERGE_PER_SEC) \
    item(IOPS_WR_MERGE_PER_SEC) \
    item(MBYTES_RD_PER_SEC) \
    item(MBYTES_WR_PER_SEC) \
    item(N_IO_STATS)

typedef enum ws_io_stat_names {
#define item(v) v,
    WS_IO_STAT_ITEMS()
#undef item
} ws_io_stat_names_t;

#ifdef WS_C
const char *WSIOStatString(int n) {
#define item(v) case v: return (#v);
    switch (n) {
    WS_IO_STAT_ITEMS()
    default:
        return ("Invalid");
    }
#undef item
}
#endif

#define WS_MAX_STAT_DEVICES  20

typedef struct ws_substats {
    struct timeval     tspec;
    uint64_t          *ws_raw_stat;
    double            *ws_stat;
    uint64_t          *cpu_raw_stat;
    double            *cpu_stat;
    uint64_t          *io_raw_stat[WS_MAX_STAT_DEVICES];
    double            *io_stat[WS_MAX_STAT_DEVICES];
} ws_substats_t;

typedef struct ws_stats {
    int                      in_steady_state;
    int                      n_stat_devices;
    char                     stat_device_names[WS_MAX_STAT_DEVICES][WS_LINE_SIZE];
    int                      stat_device_sector_bytes[WS_MAX_STAT_DEVICES];

    ws_substats_t            stats;         //  current sample
    ws_substats_t            stats_t0;      //  initial sample (at t=0)
    ws_substats_t            stats_ss0;     //  at beginning of steady-state
    ws_substats_t            stats_last;    //  previous sample
    ws_substats_t            stats_overall; //  since start of run
    ws_substats_t            stats_ss;      //  since start of steady-state
    ws_substats_t            stats_window;  //  since last report

    wsrt_statistics_t        gc_compaction;
    wsrt_statistics_t        batch_writes;
} ws_stats_t;


struct ws_state;

void WSLoadDefaultConfig(ws_config_t *cfg);

  /*  Returns pointer to write serializer state if successful,
   *  or NULL if fails.
   */
struct ws_state *WSStart(struct ZS_state *pzs, struct ZS_thread_state *pzst, ws_config_t *config, int format);

  /*  returns:
   *    WS_OK
   *    WS_ERROR
   */
int WSQuit(struct ws_state *ps);

  /*  returns:
   *    WS_OK
   *    WS_ERROR
   */
int WSTraceControl(struct ws_state *ps, int on_off);

void WSStats(struct ZS_thread_state *pzs, struct ws_state *ps, ws_stats_t *stats);
void WSDumpStats(FILE *f, struct ZS_thread_state *pzs, struct ws_state *ps);

  /*  returns:
   *    WS_OK
   *    WS_ERROR
   */
int WSChangeConfig(struct ws_state *ps);

void WSDumpConfig(FILE *f, struct ws_state *ps);

  /*  Read data into a caller-provided buffer.
   *
   *  "pdata" points to the buffer to hold data (not necessarily sector
   *          aligned)
   *  "addr" is byte-granularity at the pool level, but sector-aligned.
   *  "size" is in bytes and must be a sector-multiple
   *
   *  Returns:
   *    WS_OK
   *    WS_BAD_ADDR
   *    WS_IO_ERROR
   *    WS_ERROR
   */
int WSRead(struct ws_state *ps, void *pzst, char *key, uint32_t key_len, uint64_t addr, void *pdata, uint64_t size);

  /*  Allocate space for writing data into a serialized stripe, 
   *  and return the location.  The actual writing will be done
   *  later via WSWriteComplete.  
   *  
   *  The allocation is done separately
   *  to avoid a deadlock with the hash table locks.
   *  WSWriteAllocate should be called before mcd_osd_flash_put()
   *  takes the hash table lock.  This ensures that the writer
   *  thread cannot deadlock on a hash table lock required by
   *  the garbage collector thread (to free up space required
   *  by the writer thread).  This function ensures that any
   *  garbage collection required to make space for this write
   *  is done before the hash table lock is acquired by the
   *  writer thread.
   *
   *  "pdata" points to the data to be written (not necessarily sector
   *          aligned)
   *  "size" is in bytes and must be a sector-multiple
   *  "addr" is returned, and is byte-granularity at the pool level.
   *         It is a sector-multiple and sector-aligned.
   *
   *  Returns:
   *    WS_OK
   *    WS_BAD_ADDR
   *    WS_READ_ONLY
   *    WS_DELETE_ERROR
   *    WS_ERROR
   *    WS_EARLY_TERMINATION
   */
int WSWriteAllocate(struct ws_state *ps, void *pzst, char *key, uint32_t key_len, uint64_t size);

  /*  Write data into a serialized stripe, at the preallocated
   *  location.  If I filled the stripe, send it on for writing.
   *
   *  "pdata" points to the data to be written (not necessarily sector
   *          aligned)
   *  "size" is in bytes and must be a sector-multiple
   *  "addr" is returned, and is byte-granularity at the pool level.
   *         It is a sector-multiple and sector-aligned.
   *
   *  Returns:
   *    WS_OK
   *    WS_BAD_ADDR
   *    WS_READ_ONLY
   *    WS_DELETE_ERROR
   *    WS_ERROR
   *    WS_EARLY_TERMINATION
   */
int WSWriteComplete(struct ws_state *ps, void *pzst, char *key, uint32_t key_len, void *pdata, uint64_t size, uint64_t *addr, int is_update, uint64_t old_addr, uint64_t old_size);

  /*  Delete data from a serialized stripe.
   *
   *  "addr" is byte-granularity at the pool level, but sector-aligned.
   *  "size" is in bytes and must be a sector-multiple
   *
   *  Returns:
   *    WS_OK
   *    WS_BAD_ADDR
   *    WS_READ_ONLY
   *    WS_ERROR
   */
int WSDelete(struct ws_state *ps, void *pzst, char *key, uint32_t key_len, uint64_t addr, uint64_t size);

    /*  Run comprehensive consistency checks.
     * 
     *  Returns: WS_OK if success, WS_ERROR otherwise.
     */
int WSCheck(FILE *f, struct ws_state *ps, void *pzst, uint32_t sectors_per_node);

    /*  Dump contents of data structures.
     * 
     *  "level" can be 0, 1 or 2: higher levels dump more stuff.
     * 
     *  Returns: WS_OK if success, WS_ERROR otherwise.
     */
int WSDump(FILE *f, struct ws_state *ps, struct ZS_thread_state *pzs, int level);

#endif
