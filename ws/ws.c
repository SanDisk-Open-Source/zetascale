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
 *  ws.c   8/29/16   Brian O'Krafka   
 *
 *  Write serializer module for ZetaScale b-tree.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 *  Notes:
 *  - Make striping more sophisticated.
 *  - Does GC of a stripe have to be done transactionally? NO!
 *  - Should you use dedicated stripes for GC?  (this solves the
 *    problem of avoiding device-to-device xcopy's)
 *  - xxxzzz where is quota enforced? (to maintain desired OP)
 *  - xxxzzz what about versioning stuff?
 *  - do GC buffers have to be in nvram and replicated?
 *  - xxxzzz make sure that pse->cnt is only updated using
 *    __sync_fetch_and_add!
 *  - xxxzzz zero out any unused bytes at end of stripe?
 *
 *
 *  How deref_l1cache() uses the write serializer API:
 *
 *    - deref_... is called with a list of modified nodes
 *      and a list of deleted nodes.
 *    - It needs to be extended to include a pointer to the
 *      data pointer within a leaf node corresponding to
 *      the data item whose mapping is stored in the b-tree.
 *      (this is only needed when ZS is used as a metadata
 *      store for a block storage system).
 *    - deref_... writes out the modified nodes using
 *      write_node_cb.  This currently calls ZSWriteObjects().
 *    - write_node_cb must be changed to: 
 *          - call WSWrite(), which will return the LBA at 
 *            which the data is written.
 *          - call ZSHashMap(), which will create/update the
 *            key-to-LBA mapping for the data key.  ZSHashMap()
 *            will return the old addr for the mapping if this
 *            is an update.
 *          - If this is an update, write_node_cb must call
 *            WSDelete() for the old data address (so that
 *            free space is properly accounted for in the
 *            write serializer!).
 *
 *   Assume a separate instance of write serializer per
 *   storage pool.
 *
 *   Storage addressing scheme:
 *     - 512B sector resolution
 *     - "client" 64-bit addresses are byte-granularity pool addresses
 *
 *  Stripe buffer states (in order of transitions):
 *  These must be persisted at transitions (via nvram) 
 *  and are used for recovery
 *  xxxzzz checksums to detect torn nodes/data during recovery?
 *
 *   Unused
 *   Open
 *   Full_pending_io
 *   Available_for_gc
 *   Compacted_but_transient
 *   Compacted_and_ready
 *   Next_Open
 *
 **********************************************************************/

#define WS_C

#include <time.h>
#include <stdarg.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include "threadpool_internal.h"
#include "wsstats.h"
#include "mbox.h"
#include "lat_internal.h"
#include "ws_internal.h"

static int   WSReadOnly  = 0;
// static int   WSDoChecks  = 1; // xxxzzz disable this or make configurable
static int   WSTraceOn   = 0;
static FILE *WSStatsFile = NULL;

static void goto_readonly()
{
    int   x = 0;
    WSReadOnly = 1;
    WSReadOnly = 1/x; //  xxxzzz remove this
}

/*  For remembering my thread-id to minimize 
 *  calls to syscall(SYS_gettid).
 */
__thread long              WSThreadId = 0;

/*  To remember the allocation ptr between calls to
 *  WSWriteAllocate and WSWriteComplete.
 */
static ws_stripe_buf_t    *WSNewBuf   = NULL; // NOTE: this is NOT per-thread!
static uint64_t            WSFinalPtr = 0;    // NOTE: this is NOT per-thread!
static uint64_t            WSGlobalPtr = 0;   // NOTE: this is NOT per-thread!
__thread uint64_t          WSCurPtr   = 0;
__thread uint64_t          WSCurSize  = 0;

__thread int               WSBatchReturnMboxInited = 0;
__thread mbox_t            WSBatchReturnMbox;

static long get_tid()
{
    if (WSThreadId == 0) {
        WSThreadId = syscall(SYS_gettid);
    }
    return(WSThreadId);
}

#define STRIPE_KEY_SIZE  200

#define TRACE(ps, fmt, args...) \
    if ((ps)->trace_on) { \
        tracemsg(fmt, ##args); \
    }

#define DOSTAT(ps, pzst, nstat, val) \
{\
    (void) __sync_fetch_and_add(&((ps)->stats.stats.ws_raw_stat[nstat]), val);\
}

void load_env_variables(ws_state_t *ps, ws_config_t *cfg);

static void init_stats(ws_state_t *ps);
static void compute_derived_stats(ws_state_t *ps, ws_substats_t *pstats_ref, ws_substats_t *pstats_now, ws_substats_t *pstats_out, double secs, double user_hz);
static void tracemsg(char *fmt, ...);
static void infomsg(char *fmt, ...);
static void errmsg(char *fmt, ...);
static void panicmsg(char *fmt, ...);
static void check_info(FILE *f, char *fmt, ...);
static void check_err(FILE *f, char *fmt, ...);
static int stripe_tbl_adjust_count(ws_state_t *ps, uint64_t addr, int32_t size);
static uint64_t stripe_from_addr(ws_state_t *pst, uint64_t addr);
static uint64_t addr_from_stripe(ws_state_t *pst, uint64_t n_stripe);
static void copy_substats(ws_substats_t *pto, ws_substats_t *pfrom);

static int check_config(ws_config_t *cfg);
static int format_stripe_tbl(struct ZS_thread_state *pzst, ws_state_t *ps);
static int recover_stripe_tbl(struct ZS_thread_state *pzst, ws_state_t *ps);
static int check_fill_group_percents(ws_fill_groups_t *pfg);
static int persist_stripe_tbl(struct ZS_thread_state *pzst, ws_state_t *ps);
static int do_point_read(ws_state_t *ps, void *pdata, uint64_t addr, uint32_t size);
static uint32_t get_group(ws_state_t *ps, int16_t cnt);
static int do_stripe_write(ws_state_t *ps, ws_stripe_buf_t *sbuf, int syncflag);
static int do_gc(ws_state_t *ps, ws_stripe_buf_t *sbuf);

static void *scrub_thread(threadpool_argstuff_t *as);
static void *stripe_tbl_dumper_thread(threadpool_argstuff_t *as);
static void *batch_thread(threadpool_argstuff_t *as);
static void *writer_thread(threadpool_argstuff_t *as);
static void *gc_thread(threadpool_argstuff_t *as);
static void *stats_thread(threadpool_argstuff_t *as);

static void quiesce(ws_state_t *ps);
static void unquiesce(ws_state_t *ps);

static int lat_check_cb(FILE *f, void *pdata, uint64_t addr, void *ae_pdata, int locked, int is_write_lock);


  /*    Load default configuration parameters.
   */
void WSLoadDefaultConfig(ws_config_t *cfg)
{
    cfg->n_devices                 = 0;
    cfg->fd_devices[0]             = 0;
    cfg->batch_fd                  = 0;
    strcpy(cfg->stat_device_string, "");

    cfg->sector_size               = 512;
    cfg->sectors_per_node          = 32;

    cfg->chunk_kbytes              = 32;
    cfg->device_mbytes             = 0;
    cfg->device_offset_mb          = 0;
    cfg->gc_phase1_cb              = NULL;
    cfg->gc_phase2_cb              = NULL;
    cfg->per_thread_cb             = NULL;
    cfg->get_md_cb                 = NULL;
    cfg->set_md_cb                 = NULL;

    cfg->cb_state                  = NULL;

    cfg->percent_op                = 28;
    cfg->stripe_tbl_bytes_per_dump = 128*1024;
    cfg->stripe_tbl_dump_usecs     = 1000;
    cfg->scrub_usecs               = 1000000000;
    cfg->quiesce_usecs             = 1000;
    // cfg->n_stripe_bufs             = 100;
    cfg->n_stripe_bufs             = 2;
    cfg->gc_per_read_bytes         = 128*1024;

    // cfg->n_gc_threads              = 32;
    cfg->n_gc_threads              = 1;
    cfg->n_scrub_threads           = 1;
    // cfg->n_io_threads              = 32;
    cfg->n_io_threads              = 1;
    cfg->n_stripe_tbl_threads      = 1;

    cfg->n_fill_groups             = 14;

    cfg->fill_group_percents[0]    = 90;
    cfg->fill_group_percents[1]    = 80;
    cfg->fill_group_percents[2]    = 70;
    cfg->fill_group_percents[3]    = 60;
    cfg->fill_group_percents[4]    = 50;
    cfg->fill_group_percents[5]    = 40;
    cfg->fill_group_percents[6]    = 30;
    cfg->fill_group_percents[7]    = 25;
    cfg->fill_group_percents[8]    = 20;
    cfg->fill_group_percents[9]    = 15;
    cfg->fill_group_percents[10]   = 10;
    cfg->fill_group_percents[11]   = 5;
    cfg->fill_group_percents[12]   = 3;
    cfg->fill_group_percents[13]   = 0;

    cfg->trace_on                  = 0;
    strcpy(cfg->stats_file, "ws_stats.txt");
    cfg->stats_secs                = 10;
    cfg->stats_steady_state_secs   = 30; 
    cfg->check_interval            = 0;
}

static int parse_stat_devices(ws_state_t *ps, char *s)
{
    int      i;
    char    *p, *token;
    int      n;
    char     stmp[1024];

    strcpy(stmp, s);

    n = 0;
    p = stmp;
    while ((token=strsep(&p, ","))) {
	strcpy(ps->stats.stat_device_names[n], token);
	n++;
	if (n >= WS_MAX_STAT_DEVICES) {
	    errmsg("WS_STATS_DEVICES='%s' has too many devices listed; can only track first %d devices\n", s, n);
	    ps->stats.n_stat_devices = n;
	    return(0);
	}
    }
    ps->stats.n_stat_devices = n;

    infomsg("%d Stats Devices: ");
    for (i=0; i<n; i++) {
	infomsg("%s, ", ps->stats.stat_device_names[i]);
    }
    infomsg("\n");

    return(0);
}

void load_env_variables(ws_state_t *ps, ws_config_t *cfg)
{
    uint32_t     i32;
    char        *s;

    /*  See if these hardcoded defaults have been overridden
     *  by environment variables.
     */

    s = getenv("WS_GC_THREADS");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 <= 0) || (i32 > 512)) {
	    errmsg("WS_GC_THREADS=%d is out of range, reverting to default of %d \n", i32, cfg->n_gc_threads);
	} else {
	    cfg->n_gc_threads = i32;
	}
    }

    s = getenv("WS_IO_THREADS");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 <= 0) || (i32 > 512)) {
	    errmsg("WS_IO_THREADS=%d is out of range, reverting to default of %d \n", i32, cfg->n_io_threads);
	} else {
	    cfg->n_io_threads = i32;
	}
    }

    s = getenv("WS_STRIPE_BUFS");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 <= 0) || (i32 > 1000)) {
	    errmsg("WS_STRIPE_BUFS=%d is out of range, reverting to default of %d \n", i32, cfg->n_stripe_bufs);
	} else {
	    cfg->n_stripe_bufs = i32;
	}
    }

    s = getenv("WS_PERCENT_OP");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 <= 0) || (i32 > 1000)) {
	    errmsg("WS_PERCENT_OP=%d is out of range, reverting to default of %d \n", i32, cfg->percent_op);
	} else {
	    cfg->percent_op = i32;
	}
    }

    s = getenv("WS_DEVICE_MBYTES");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 <= 0) || (i32 > 1000000)) {
	    errmsg("WS_DEVICE_MBYTES=%d is out of range, reverting to default of %d \n", i32, cfg->device_mbytes);
	} else {
	    cfg->device_mbytes = i32;
	}
    }

    s = getenv("WS_DEVICE_OFFSET_MB");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 <= 0) || (i32 > 1000000)) {
	    errmsg("WS_DEVICE_OFFSET_MB=%d is out of range, reverting to default of %d \n", i32, cfg->device_offset_mb);
	} else {
	    cfg->device_offset_mb = i32;
	}
    }

    s = getenv("WS_CHUNK_KBYTES");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 <= 0) || (i32 > 16*1024)) {
	    errmsg("WS_CHUNK_KBYTES=%d is out of range, reverting to default of %d \n", i32, cfg->chunk_kbytes);
	} else {
	    cfg->chunk_kbytes = i32;
	}
    }

    s = getenv("WS_BATCH_SIZE");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 < 0) || (i32 > 16*1024)) {
	    errmsg("WS_BATCH_SIZE=%d is out of range, reverting to default of %d \n", i32, cfg->batch_size);
	} else {
	    cfg->batch_size = i32;
	}
    }

    s = getenv("WS_TRACE_ON");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 < 0) || (i32 > 1)) {
	    errmsg("WS_TRACE_ON=%d is out of range, reverting to default of %d \n", i32, cfg->trace_on);
	} else {
	    cfg->trace_on = i32;
	}
    }

    s = getenv("WS_STATS_INTERVAL");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 < 0) || (i32 > 1000000000)) {
	    errmsg("WS_STATS_INTERVAL=%d is out of range, reverting to default of %d \n", i32, cfg->stats_secs);
	} else {
	    if (i32 == 0) {
	        i32 = 10;
	    }
	    cfg->stats_secs = i32;
	}
    }

    s = getenv("WS_CHECK_INTERVAL");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 < 0) || (i32 > 1000000000)) {
	    errmsg("WS_CHECK_INTERVAL=%d is out of range, reverting to default of %d \n", i32, cfg->check_interval);
	} else {
	    cfg->check_interval = i32;
	}
    }

    s = getenv("WS_STEADY_STATE_SECS");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 < 0) || (i32 > 1000000000)) {
	    errmsg("WS_STEADY_STATE_SECS=%d is out of range, reverting to default of %d \n", i32, cfg->stats_steady_state_secs);
	} else {
	    cfg->stats_steady_state_secs = i32;
	}
    }

    s = getenv("WS_STATS_FILE");
    if (s != NULL) {
        (void) strcpy(cfg->stats_file, s);
    }

    s = getenv("WS_STATS_DEVICES");
    if (s != NULL) {
        if (parse_stat_devices(ps, s)) {
	    errmsg("Could not parse WS_STATS_DEVICES='%s', reverting to default of '%s'\n", s, cfg->stat_device_string);
	    if (parse_stat_devices(ps, cfg->stat_device_string)) {
	        errmsg("Internal error: bad default stat_device_string='%s', defaulting to 0 stat devices", cfg->stat_device_string);
		ps->stats.n_stat_devices = 0;
	    }
	} else {
	    strcpy(cfg->stat_device_string, s);
	}
    }

    s = getenv("WS_STRIPE_DUMP_USECS");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 < 0) || (i32 > 1000000000)) {
	    errmsg("WS_STRIPE_DUMP_USECS=%d is out of range, reverting to default of %d \n", i32, cfg->stripe_tbl_dump_usecs);
	} else {
	    cfg->stripe_tbl_dump_usecs = i32;
	}
    }

    s = getenv("WS_QUIESCE_USECS");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 < 0) || (i32 > 1000000000)) {
	    errmsg("WS_QUIESCE_USECS=%d is out of range, reverting to default of %d \n", i32, cfg->quiesce_usecs);
	} else {
	    cfg->quiesce_usecs = i32;
	}
    }
}

  /*    Dump configuration parameters.
   */
void WSDumpConfig(FILE *f, struct ws_state *ps)
{
    int             i;
    ws_config_t    *cfg = &(ps->config);

    fprintf(f, "                n_devices = %d\n", cfg->n_devices);
    fprintf(f, "             chunk_kbytes = %d\n", cfg->chunk_kbytes);
    fprintf(f, "            device_mbytes = %"PRIu64"\n", cfg->device_mbytes);
    fprintf(f, "         device_offset_mb = %"PRIu64"\n", cfg->device_offset_mb);
    fprintf(f, "              sector_size = %d\n", cfg->sector_size);
    fprintf(f, "         sectors_per_node = %d\n", cfg->sectors_per_node);
    fprintf(f, "               percent_op = %d\n", cfg->percent_op);
    fprintf(f, "stripe_tbl_bytes_per_dump = %"PRIu64"\n", cfg->stripe_tbl_bytes_per_dump);
    fprintf(f, "    stripe_tbl_dump_usecs = %d\n", cfg->stripe_tbl_dump_usecs);
    fprintf(f, "               batch_size = %d\n", cfg->batch_size);
    fprintf(f, "              scrub_usecs = %d\n", cfg->scrub_usecs);
    fprintf(f, "            quiesce_usecs = %d\n", cfg->quiesce_usecs);
    fprintf(f, "            n_stripe_bufs = %d\n", cfg->n_stripe_bufs);
    fprintf(f, "        gc_per_read_bytes = %"PRIu64"\n", cfg->gc_per_read_bytes);
    fprintf(f, "             n_gc_threads = %d\n", cfg->n_gc_threads);
    fprintf(f, "          n_scrub_threads = %d\n", cfg->n_scrub_threads);
    fprintf(f, "             n_io_threads = %d\n", cfg->n_io_threads);
    fprintf(f, "     n_stripe_tbl_threads = %d\n", cfg->n_stripe_tbl_threads);

    fprintf(f, "                 trace_on = %d\n", cfg->trace_on);
    fprintf(f, "               stats_file = %s\n", cfg->stats_file);
    fprintf(f, "               stats_secs = %d\n", cfg->stats_secs);
    fprintf(f, "            stats_devices = %s\n", cfg->stat_device_string);
    fprintf(f, "  stats_steady_state_secs = %d\n", cfg->stats_steady_state_secs);
    fprintf(f, "           check_interval = %d\n", cfg->check_interval);

    fprintf(f, "            n_fill_groups = %d\n", cfg->n_fill_groups);
    
    for (i=0; i<cfg->n_fill_groups; i++) {
	fprintf(f, "  fill_group_percents[%d] = %d\n", i, cfg->fill_group_percents[i]);
    }
}

  /*  returns:
   *    - pointer to serializer state is successful, NULL otherwise.
   */
struct ws_state *WSStart(struct ZS_state *pzs, struct ZS_thread_state *pzst, ws_config_t *config, int format)
{
    int               i;
    ws_state_t       *ps;
    ws_stripe_buf_t  *sbufs;
    ws_stripe_buf_t  *psb;
    char             *bufmem;
    char             *pbuf;

    //  If an unrecoverable error occurs, this is set to 1.
    WSReadOnly = 0;

    ps = (ws_state_t *) malloc(sizeof(ws_state_t));
    if (ps == NULL) {
        return(NULL);
    }

    /*   See if any configuration values are overridden by 
     *   environment variables.
     */
    load_env_variables(ps, config);
    
    if (check_config(config) != 0) {
        return(NULL);
    }

    //  Open stats file, if required.
    if (config->stats_file[0] != '\0') {
	ps->f_stats = fopen(config->stats_file, "w");
	if (ps->f_stats == NULL) {
	    errmsg("Could not open stats file '%s'", config->stats_file);
	}
    } else {
	ps->f_stats = NULL;
    }
    WSStatsFile = ps->f_stats;

    memcpy((void *) &(ps->config), (void *) config, sizeof(ws_config_t));
    if (WSStatsFile != NULL) {
        WSDumpConfig(WSStatsFile, ps);
    }

    pthread_mutex_init(&(ps->mutex), NULL);
    pthread_cond_init(&(ps->write_cv), NULL);
    ps->nwaiters                  = 0;
    ps->is_terminating            = 0;
    ps->failed_to_start           = 0;
    ps->quiesce                   = 0;

    pthread_mutex_init(&(ps->quiesce_mutex), NULL);
    pthread_cond_init(&(ps->quiesce_cv), NULL);
    ps->writes_in_progress        = 0;
    ps->deletes_in_progress       = 0;
    ps->stripe_writes_in_progress = 0;
    ps->batch_writes_in_progress  = 0;
    ps->gcs_in_progress           = 0;
    ps->scrubs_in_progress        = 0;
    ps->next_check_cnt            = config->check_interval;

    ps->zs_state                  = pzs;

    init_stats(ps);

    ps->lat                    = alat_init(N_LAT_BUCKETS, sizeof(lat_data_t), N_LAT_FREE_LISTS);
    if (ps->lat == NULL) {
        errmsg("Could not initialize the write serialization look-aside table");
        return(NULL);
    }

    ps->trace_on               = config->trace_on;
    WSTraceOn                  = ps->trace_on;
    TRACE(ps, "WSStart...");
    ps->stripe_bytes           = ps->config.chunk_kbytes*1024 * ps->config.n_devices;
    ps->sector_bytes           = config->sector_size;
    ps->stripe_sectors         = ps->stripe_bytes / ps->sector_bytes;
    if (ps->stripe_bytes % ps->sector_bytes != 0) {
        errmsg("stripe_bytes (%lld) must be a multiple of the sector size (%lld)", ps->stripe_bytes, ps->sector_bytes);
        return(NULL);
    }
    if (ps->config.device_mbytes <= 0) {
        errmsg("device_mbytes (%lld) must be non-zero", ps->config.device_mbytes);
        return(NULL);
    }
    ps->n_stripes    = ps->config.device_mbytes*1024ULL/ps->config.chunk_kbytes;

    /* initialize the mailboxes before starting threadpools */

    mboxInit(&(ps->mbox_free));
    mboxInit(&(ps->mbox_write));
    mboxInit(&(ps->mbox_gc));
    mboxInit(&(ps->mbox_batch));

    /* start garbage collector threadpool */
    /*
     *  A garbage collector thread wakes up whenever a 
     *  new stripe is queued for writing to flash.
     *  The maximum number of stripes undergoing garbage
     *  collection in parallel is determined by the size
     *  of the GC threadpool.
     */
    ps->gc_threads = tp_init(config->n_gc_threads, gc_thread, ps);
    
    /* start scrubber threadpool */
    /*
     *   Scrubber verifies checksums, updates stripe sector counts, 
     *   and restructures data if device layout changes.
     *   Scrub rate can change if entire storage must be processed
     *   more quickly than the default background rate.
     */
    ps->scrub_threads = tp_init(config->n_scrub_threads, scrub_thread, ps);
    
    /* start batch persist threadpool */
    /*
     *  The batch threadpool combines multiple concurrent small 
     *  writes to the volatile serialization buffer and persists
     *  them for durability.
     */
    ps->batch_threads = tp_init(1, batch_thread, ps);
    
    /* start I/O threadpool */
    /*
     *  The I/O threadpool does parallel async I/O for writing
     *  stripes to flash.
     */
    ps->io_threads = tp_init(config->n_io_threads, writer_thread, ps);
    
    /* start stats dumper threadpool */
    /*
     *  The stats dumper threadpool has a single thread that
     *  periodically dumps statistics to stderr and optionally
     *  a stats file.
     */
    ps->stats_thread = tp_init(1, stats_thread, ps);
    
    /* Initialize the stripe table and fill groups */
    if (format) {
        /* reformat storage */
	if (format_stripe_tbl(pzst, ps) != 0) {
	    // xxxzzz free up malloc'd stuff!
	    return(NULL);
	}
    } else {
        /* load the stripe table from persistent storage */
	if (recover_stripe_tbl(pzst, ps) != 0) {
	    // xxxzzz free up malloc'd stuff!
	    return(NULL);
	}
    }

    /* start stripe table dumper thread (threadpool of 1) 
     *
     *!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
     *  MUST DO THIS AFTER pzst IS USED TO FORMAT/RECOVER
     *  STRIPE TABLE ABOVE!!!!!!!!
     *!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
     */
    /*
     *  The stripe table dumper "trickles-out" the stripe
     *  table at a configurable rate.  Since the stripe table
     *  just tracks the emptiness of stripes, it does not have
     *  to be precise in the event of a crash.  The scrubber
     *  ensures that the stripe table eventually corrects itself
     *  after a crash recovery.
     * 
     *  Sample math:
     *    - Stripe table entry size: 12B (2B count, 2B group, 4B next, 4B prev)
     *    - With 512kB stripes: 
     *        - 1B entries per 512TB of flash
     *        - 12GB of stripe table
     *    - At 100MB/s dump rate: 100s per full dump
     *    - At 5GB/s data write rate: 500GB per 100s
     *    - Max error of last persisted stripe table: 500GB out of 512TB
     *      (.1%).
     */
    ps->stripe_tbl_pool = tp_init(config->n_stripe_tbl_threads, stripe_tbl_dumper_thread, ps);
    
    /*  Create pool of free stripe buffers.
     *  Put them on the GC queue to get stripe numbers assigned to them.
     */

    sbufs = (ws_stripe_buf_t *) malloc(ps->config.n_stripe_bufs*sizeof(ws_stripe_buf_t));
    if (sbufs == NULL) {
        errmsg("Could not allocate stripe buffer structures");
	return(NULL);
    }

    //  xxxzzz the factor of 8 here is to deal with 4kB sectors
    //  in the IceChip drive
    bufmem = (char *) malloc(ps->config.n_stripe_bufs*ps->stripe_bytes + 8*ps->sector_bytes);
    if (bufmem == NULL) {
        errmsg("Could not allocate stripe buffer bufs");
	return(NULL);
    }
    // bufs must be sector-aligned
    pbuf = bufmem + 8*ps->sector_bytes - (((uint64_t) bufmem) % (8*ps->sector_bytes)); 
    for (i=0; i<ps->config.n_stripe_bufs; i++) {
        psb = &(sbufs[i]);
	psb->n        = i;
	psb->n_stripe = WS_NULL_STRIPE_ENTRY;
	psb->state    = Unused;
	psb->p        = 0;
	psb->buf      = pbuf;
	pbuf         += ps->stripe_bytes;
	if (i < (ps->config.n_stripe_bufs-1)) {
	    psb->next = &(sbufs[i+1]);
	} else {
	    psb->next = NULL;
	}
	// kick off a garbage collection to allocate one of the emptiest stripes
	mboxPost(&(ps->mbox_gc), (uint64_t) psb);
    }
    ps->sbufs = &(sbufs[0]);

    /* assign the current "open buffer" */
    ps->curbuf = (ws_stripe_buf_t *) mboxWait(&(ps->mbox_free));
    ps->curbuf->state = Open;
    ps->curptr = &(ps->curbuf->p);
    WSGlobalPtr = ps->curbuf->p;

    /* make sure that everything started properly */
    if (ps->failed_to_start) {
        WSQuit(ps);
	return(NULL);
    }

    TRACE(ps, "...WSStart");
    return(ps);
}

static int allocate_stripe_tbl(ws_state_t *ps)
{
    ws_stripe_tbl_t   *pst = &(ps->stripe_tbl);

    pst->n_entries = ps->n_stripes;
    pst->entries_per_dump = ps->config.stripe_tbl_bytes_per_dump/sizeof(ws_stripe_entry_t);
    pst->entries = (ws_stripe_entry_t *) malloc(pst->n_entries*sizeof(ws_stripe_entry_t));
    if (pst->entries == NULL) {
        return(1);
    }

    return(0);
}

static int format_stripe_tbl(struct ZS_thread_state *pzst, ws_state_t *ps)
{
    uint64_t                  i;
    ws_stripe_tbl_t          *pst = &(ps->stripe_tbl);
    ws_fill_groups_t         *pfg = &(ps->fill_groups);
    ws_config_t              *cfg = &(ps->config);
    ws_stripe_entry_t        *pse;

    allocate_stripe_tbl(ps);
    for (i=0; i<pst->n_entries; i++) {
        pse          = &(pst->entries[i]);
	pse->cnt     = 0;
	pse->n_group = -1;
	pse->next    = i+1;
	pse->prev    = i-1;
    }
    pst->entries[0].prev                = WS_NULL_STRIPE_ENTRY;
    pst->entries[pst->n_entries-1].next = WS_NULL_STRIPE_ENTRY;

    pfg->free_list = 0;
    pthread_mutex_init(&(pfg->free_lock), NULL);

    pfg->n_fill_groups = cfg->n_fill_groups;
    if (pfg->n_fill_groups > WS_MAX_FILL_GROUPS) {
        panicmsg("Too many fill groups (%d); %d is max allowed", pfg->n_fill_groups, WS_MAX_FILL_GROUPS);
	return(1);
    }

    for (i=0; i<pfg->n_fill_groups; i++) {
        pfg->fill_group_percents[i] = cfg->fill_group_percents[i];
	pthread_mutex_init(&(pfg->group_lock[i]), NULL);
	pfg->fill_groups[i] = WS_NULL_STRIPE_ENTRY;
    }

    if (check_fill_group_percents(pfg) != 0) {
        return(1);
    }

    // persist freshly initialized stripe table
    if (persist_stripe_tbl(pzst, ps) != 0) {
       return(1);
    }

    return(0);
}

static int check_fill_group_percents(ws_fill_groups_t *pfg)
{
    uint32_t   i;
    uint32_t   percent, last_percent;

    last_percent = 100;
    for (i=0; i<pfg->n_fill_groups; i++) {
        percent = pfg->fill_group_percents[i];
	if (percent >= last_percent) {
	    if (i == 0) {
		errmsg("0'th fill group percentage [%d found] must be less than 100", i, percent);
		return(1);
	    } else {
		errmsg("%d'th fill group percentage [%d found] must be less than the %d'th fill group percentage [%d found]", i, percent, i-1, last_percent);
		return(1);
	    }
	}
	if (percent > 99) {
	    errmsg("%d'th fill group percentage [%d found] must be less than 100", i, percent);
	    return(1);
	}

	last_percent = percent;
    }
    return(0);
}

static int build_stripe_key(char *key, uint64_t n)
{
    sprintf(key, "_stripe_tbl_%"PRIu64"", n);
    return(strlen(key));
}

static int persist_stripe_tbl(struct ZS_thread_state *pzst, ws_state_t *ps)
{
    uint64_t                  i, total_dumps, n_dumps;
    ws_stripe_tbl_t          *pst = &(ps->stripe_tbl);
    ws_config_t              *cfg = &(ps->config);
    ws_stripe_entry_t        *pse_from;
    uint32_t                  keylen;
    char                      key[STRIPE_KEY_SIZE];
    uint32_t                  data_size;
 
    TRACE(ps, "Persisting stripe table ...");

    total_dumps = (ps->n_stripes + pst->entries_per_dump - 1)/pst->entries_per_dump;

    pse_from = pst->entries;
    n_dumps = 0;
    for (i=0; i<ps->n_stripes; i+= pst->entries_per_dump) {
	keylen = build_stripe_key(key, i);
	if (n_dumps < (total_dumps - 1)) {
	    data_size = pst->entries_per_dump*sizeof(ws_stripe_entry_t);
	} else {
	    data_size = (ps->n_stripes % pst->entries_per_dump)*sizeof(ws_stripe_entry_t);
	}
	TRACE(ps, "persist_stripe_tbl: set_md_cb(key=%s, pse_from=%p, data_size=%d", key, pse_from, data_size);
	DOSTAT(ps, pzst, N_MD_SET, 1);
	DOSTAT(ps, pzst, N_MD_SET_BYTES, keylen+data_size);
	if (cfg->set_md_cb(pzst, ps->config.cb_state, key, keylen, pse_from, data_size) != 0) {
	    panicmsg("Failure writing dump %d for persisted stripe table", n_dumps);
	    return(1);
	}
	n_dumps++;
	pse_from += pst->entries_per_dump;
    }
    TRACE(ps, "... Persisting stripe table");
    return(0);
}

static int load_stripe_tbl(struct ZS_thread_state *pzst, ws_state_t *ps)
{
    uint64_t                  i, total_dumps, n_dumps;
    ws_stripe_tbl_t          *pst = &(ps->stripe_tbl);
    ws_config_t              *cfg = &(ps->config);
    ws_stripe_entry_t        *pse_to;
    uint32_t                  keylen;
    char                      key[STRIPE_KEY_SIZE];
    uint32_t                  expected_size;
    uint32_t                  actual_size;

    TRACE(ps, "Loading stripe table ...");
    total_dumps = (ps->n_stripes + pst->entries_per_dump - 1)/pst->entries_per_dump;

    pse_to = pst->entries;
    n_dumps = 0;
    for (i=0; i<ps->n_stripes; i+= pst->entries_per_dump) {
	keylen = build_stripe_key(key, i);
	if (n_dumps < (total_dumps - 1)) {
	    expected_size = pst->entries_per_dump*sizeof(ws_stripe_entry_t);
	} else {
	    expected_size = (ps->n_stripes % pst->entries_per_dump)*sizeof(ws_stripe_entry_t);
	}
	DOSTAT(ps, pzst, N_MD_GET, 1);
	DOSTAT(ps, pzst, N_MD_GET_BYTES, keylen+expected_size);
	if (cfg->get_md_cb(pzst, ps->config.cb_state, key, keylen, pse_to, expected_size, &actual_size) != 0) {
	    panicmsg("Failure retrieving dump %d for persisted stripe table", n_dumps);
	    return(1);
	}
	n_dumps++;
	pse_to += pst->entries_per_dump;
	if (actual_size != expected_size) {
	    panicmsg("Inconsistency in stripe table dump sizes (%d found, %d expected)", actual_size, expected_size);
	    return(1);
	}
    }
    TRACE(ps, "... Loading stripe table");
    return(0);
}

static int recover_stripe_tbl(struct ZS_thread_state *pzst, ws_state_t *ps)
{
    uint64_t                  i, j;
    ws_stripe_tbl_t          *pst = &(ps->stripe_tbl);
    ws_fill_groups_t         *pfg = &(ps->fill_groups);
    ws_config_t              *cfg = &(ps->config);
    ws_stripe_entry_t        *pse, *pse2;
    double                    percent;
    uint64_t                  n_top;

    TRACE(ps, "Recovering stripe table ...");
    allocate_stripe_tbl(ps);

    // restore stripe table 
    if (load_stripe_tbl(pzst, ps) != 0) {
       return(1);
    }

    // rebuild fill groups

    pfg->free_list     = WS_NULL_STRIPE_ENTRY;

    pfg->n_fill_groups = cfg->n_fill_groups;
    if (pfg->n_fill_groups > WS_MAX_FILL_GROUPS) {
        errmsg("Too many fill groups (%d); %d is max allowed", pfg->n_fill_groups, WS_MAX_FILL_GROUPS);
	return(1);
    }
    for (i=0; i<pfg->n_fill_groups; i++) {
        pfg->fill_group_percents[i] = cfg->fill_group_percents[i];
	pthread_mutex_init(&(pfg->group_lock[i]), NULL);
	pfg->fill_groups[i] = WS_NULL_STRIPE_ENTRY;
    }

    if (check_fill_group_percents(pfg) != 0) {
        return(1);
    }

    for (i=0; i<pst->n_entries; i++) {
        pse = &(pst->entries[i]);
	percent = 0.5 + (100.0 - 100.0*(((double) pse->cnt)/ps->stripe_sectors));

	for (j=0; j<pfg->n_fill_groups; j++) {
	    if (percent < pfg->fill_group_percents[j]) {
	        n_top = pfg->fill_groups[j];
		pse->next = n_top;
		if (n_top != WS_NULL_STRIPE_ENTRY) {
		    pse2 = &(pst->entries[n_top]);
		    pse2->prev = i;
		}
		pse->prev = WS_NULL_STRIPE_ENTRY;
		pse->n_group = j;
		break;
	    }
	}
    }

    TRACE(ps, "... Recovering stripe table");
    return(0);
}

static int check_config(ws_config_t *config)
{
    if (config->batch_size > MAX_WRITE_BATCH) {
	errmsg("batch_size=%d is out of range (%d max)\n", config->batch_size, MAX_WRITE_BATCH);
        return(WS_ERROR);
    }

    return(WS_OK);
}

  /*  returns:
   *    WS_OK
   *    WS_ERROR
   */
int WSQuit(struct ws_state *ps)
{
    TRACE(ps, "WSQuit ...");
    //  Signal termination to any clients stalled on writes
    pthread_mutex_lock(&(ps->mutex));
    ps->is_terminating = 1;
    pthread_cond_broadcast(&(ps->write_cv));
    pthread_mutex_unlock(&(ps->mutex));

    pthread_mutex_lock(&(ps->quiesce_mutex));
    pthread_cond_broadcast(&(ps->quiesce_cv));
    pthread_mutex_unlock(&(ps->quiesce_mutex));

    tp_shutdown(ps->stripe_tbl_pool);
    tp_shutdown(ps->gc_threads);
    tp_shutdown(ps->scrub_threads);
    tp_shutdown(ps->batch_threads);
    tp_shutdown(ps->io_threads);
    tp_shutdown(ps->stats_thread);

    TRACE(ps, "... WSQuit");
    return(WS_OK);
}

  /*  returns:
   *    WS_OK
   *    WS_ERROR
   */
int WSTraceControl(struct ws_state *ps, int on_off)
{
    if (on_off) {
        ps->trace_on = 1;
	WSTraceOn    = 1;
	infomsg("Trace turned on");
    } else {
        ps->trace_on = 0;
	WSTraceOn    = 0;
	infomsg("Trace turned off");
    }
    return(WS_OK);
}

static int get_raw_system_stats(struct ZS_thread_state *pzs, ws_state_t *ps, ws_substats_t *pstats)
{
    FILE      *fcpu, *fio;
    char       line[WS_LINE_SIZE+1];
    char       sdev[WS_LINE_SIZE+1];
    uint64_t   tusr, tnice, tsys, tidle, tiowait, tirq;
    uint64_t   tsoftirq, tsteal, tguest, tguest_nice;
    uint64_t   nrd,  nrdm, srd, msrd, nwr, nwrm, swr;
    uint64_t   mswr, ioip, msio, wmsio;
    uint64_t   client_ops;
    int        i, nline;
    int        major, minor;

    if (pzs == NULL) {
	client_ops = 0;
    } else if (ps->config.get_client_ops_cb(pzs, ps->config.cb_state, &client_ops)) {
        errmsg("get_raw_system_stats() could not get client ops.");
	client_ops = 0;
    }
    pstats->ws_raw_stat[N_CLIENT_OPS] = client_ops;

    fcpu = fopen("/proc/stat", "r");
    if (fcpu == NULL) {
        errmsg("Could not open /proc/stat to get cpu utilization");
	return(1);
    }

    fio = fopen("/proc/diskstats", "r");
    if (fio == NULL) {
        errmsg("Could not open /proc/diskstats to get disk statistics");
	return(1);
    }

    /*   CPU stats
     */

    if (fgets(line, WS_LINE_SIZE, fcpu) == NULL) {
        errmsg("Could not read /proc/stat");
	return(1);
    } else if (sscanf(line, "cpu %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64"", &tusr, &tnice, &tsys, &tidle, &tiowait, &tirq, &tsoftirq, &tsteal, &tguest, &tguest_nice) != 10) {
        errmsg("Could not parse /proc/stat");
	return(1);
    } else {
          pstats->cpu_raw_stat[CPU_USR_CNT]    = tusr;
          pstats->cpu_raw_stat[CPU_NICE_CNT]   = tnice;
          pstats->cpu_raw_stat[CPU_SYS_CNT]    = tsys;
          pstats->cpu_raw_stat[CPU_IDLE_CNT]   = tidle;
          pstats->cpu_raw_stat[CPU_IOWAIT_CNT] = tiowait;
    }

    nline = 0;
    while (fgets(line, WS_LINE_SIZE, fio) != NULL) {
        nline++;
        if (sscanf(line, "%d %d %s %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64"", &major, &minor, sdev, &nrd,  &nrdm, &srd, &msrd, &nwr, &nwrm, &swr, &mswr, &ioip, &msio, &wmsio) != 14) {
	    errmsg("Could not parse line %d of /proc/diskstats", nline);
	}
	if (ioip > 1000000) {
	    fprintf(stderr, "===========   Aha!!!   ============\n");
	}
	for (i=0; i<ps->stats.n_stat_devices; i++) {
	    if (strcmp(ps->stats.stat_device_names[i], sdev) == 0) {

		pstats->io_raw_stat[i][IOP_RD_CNT]           = nrd;
		pstats->io_raw_stat[i][IOP_WR_CNT]           = nwr;
		pstats->io_raw_stat[i][IOP_RD_MERGE_CNT]     = nrdm;
		pstats->io_raw_stat[i][IOP_WR_MERGE_CNT]     = nwrm;
		pstats->io_raw_stat[i][BYTES_RD_CNT]         = srd*ps->stats.stat_device_sector_bytes[i];
		pstats->io_raw_stat[i][BYTES_WR_CNT]         = swr*ps->stats.stat_device_sector_bytes[i];
		pstats->io_raw_stat[i][MSEC_RD_CNT]          = msrd;
		pstats->io_raw_stat[i][MSEC_WR_CNT]          = mswr;
		pstats->io_raw_stat[i][MSEC_IO_CNT]          = msio;
		pstats->io_raw_stat[i][MSEC_IO_WEIGHTED_CNT] = wmsio;
		pstats->io_raw_stat[i][IOPS_IN_PROGRESS]     = ioip;

		break;
	    }
	}
    }

    if (fclose(fcpu) != 0) {
        errmsg("Could not close /proc/stat");
    }
    if (fclose(fio) != 0) {
        errmsg("Could not close /proc/diskstats");
    }

    return(0);
}

static void diff_raw(uint64_t xto[], uint64_t xa[], uint64_t xb[], uint32_t n)
{
    int   i;

    for (i=0; i<n; i++) {
        xto[i] = xa[i] - xb[i];
    }
}

static double get_secs(struct timeval *ts)
{
    double    secs;

    secs = ts->tv_sec + ((double) ts->tv_usec)/1000000.0;
    return(secs);
}

static void calc_stats(struct ZS_thread_state *pzs, struct ws_state *ps)
{
    double      secs_t0, secs_last, secs_overall;
    double      secs_window, secs_ss;
    double      user_hz;

    user_hz = ps->user_hz;

    if (gettimeofday(&(ps->stats.stats.tspec), NULL) != 0) {
        errmsg("gettimeofday failed in calc_stats with error '%s'", strerror(errno));
    }

    if (get_raw_system_stats(pzs, ps, &(ps->stats.stats)) != 0) {
        errmsg("calc_stats(): could not get raw system stats");
    }

    secs_t0      = get_secs(&(ps->stats.stats_t0.tspec));
    secs_last    = get_secs(&(ps->stats.stats_last.tspec));
    secs_overall = get_secs(&(ps->stats.stats.tspec));;
    secs_window  = secs_overall - secs_last;

    //  Overall
    compute_derived_stats(ps, &(ps->stats.stats_t0), &(ps->stats.stats), &(ps->stats.stats_overall), secs_overall - secs_t0, user_hz); 

    if (ps->stats.in_steady_state) {
	//  Steady-state
	secs_ss = secs_overall - get_secs(&(ps->stats.stats_ss0.tspec));
	compute_derived_stats(ps, &(ps->stats.stats_ss0), &(ps->stats.stats), &(ps->stats.stats_ss), secs_ss, user_hz);
    }

    //  This Reporting Interval
    compute_derived_stats(ps, &(ps->stats.stats_last), &(ps->stats.stats), &(ps->stats.stats_window), secs_window, user_hz); 

    copy_substats(&(ps->stats.stats_last), &(ps->stats.stats));

    if (!ps->stats.in_steady_state) {
	if ((secs_overall - secs_t0) >= ps->config.stats_steady_state_secs) {
	    ps->stats.in_steady_state = 1;
	    copy_substats(&(ps->stats.stats_ss0), &(ps->stats.stats));
	}
    }

}

static double do_div(double x, double y)
{
    if (y == 0) {
        return(0);
    } else {
        return(x/y);
    }
}

static void compute_derived_stats(ws_state_t *ps, ws_substats_t *pstats_ref, ws_substats_t *pstats_now, ws_substats_t *pstats_out, double secs, double user_hz)
{
    int         i;
    uint64_t   *x;
    double     *xd;
    double      ops, client_ops;
    double      client_bytes, gc_bytes;

    diff_raw(pstats_out->ws_raw_stat, pstats_now->ws_raw_stat, pstats_ref->ws_raw_stat, N_WS_RAW_STATS);
    diff_raw(pstats_out->cpu_raw_stat, pstats_now->cpu_raw_stat, pstats_ref->cpu_raw_stat, N_CPU_RAW_STATS);

    for (i=0; i<ps->stats.n_stat_devices; i++) {
	diff_raw(pstats_out->io_raw_stat[i], pstats_now->io_raw_stat[i], pstats_ref->io_raw_stat[i], N_IO_RAW_STATS);
	pstats_out->io_raw_stat[i][IOPS_IN_PROGRESS] = pstats_now->io_raw_stat[i][IOPS_IN_PROGRESS];
    }

    x  = pstats_out->ws_raw_stat;
    xd = pstats_out->ws_stat;

    client_bytes = x[N_WRITE_BYTES];
    gc_bytes     = x[N_GC_BYTES];

    xd[WRITE_AMP] = do_div((client_bytes + gc_bytes), client_bytes);

    xd[K_CLIENT_RATE]     = do_div((double) x[N_CLIENT_OPS], secs)/1000;
    xd[K_READ_RATE]       = do_div((double) x[N_READS], secs)/1000;
    xd[K_WRITE_RATE]      = do_div((double) x[N_WRITES], secs)/1000;
    xd[K_UPDATE_RATE]     = do_div((double) x[N_UPDATES], secs)/1000;
    xd[K_DELETE_RATE]     = do_div((double) x[N_DELETES], secs)/1000;
    xd[STRIPE_TBL_RATE]   = do_div((double) x[N_STRIPE_TBL_DUMPS], secs);

    ops        = x[N_READS] + x[N_WRITES] + (x[N_DELETES] - x[N_UPDATES]);
    client_ops = x[N_CLIENT_OPS];

    x  = pstats_out->cpu_raw_stat;
    xd = pstats_out->cpu_stat;

    xd[CPU_UTIL_TOT]      = do_div(100.0/user_hz*(x[CPU_USR_CNT] + x[CPU_SYS_CNT]), secs);
    xd[CPU_UTIL_USR]      = do_div(100.0/user_hz*x[CPU_USR_CNT], secs);
    xd[CPU_UTIL_SYS]      = do_div(100.0/user_hz*x[CPU_SYS_CNT], secs);
    xd[CPU_UTIL_IOWAIT]   = do_div(100.0/user_hz*x[CPU_SYS_CNT], secs);
    xd[CPU_USECS_TOT_PER_OP]    = do_div(1.0/user_hz*(x[CPU_USR_CNT] + x[CPU_SYS_CNT]), ops)*1000000;
    xd[CPU_USECS_USR_PER_OP]    = do_div(1.0/user_hz*x[CPU_USR_CNT], ops)*1000000;
    xd[CPU_USECS_SYS_PER_OP]    = do_div(1.0/user_hz*x[CPU_SYS_CNT], ops)*1000000;
    xd[CPU_USECS_IOWAIT_PER_OP] = do_div(1.0/user_hz*x[CPU_IOWAIT_CNT], ops)*1000000;

    xd[CPU_USECS_TOT_PER_CLIENT_OP]    = do_div(1.0/user_hz*(x[CPU_USR_CNT] + x[CPU_SYS_CNT]), client_ops)*1000000;
    xd[CPU_USECS_USR_PER_CLIENT_OP]    = do_div(1.0/user_hz*x[CPU_USR_CNT], client_ops)*1000000;
    xd[CPU_USECS_SYS_PER_CLIENT_OP]    = do_div(1.0/user_hz*x[CPU_SYS_CNT], client_ops)*1000000;
    xd[CPU_USECS_IOWAIT_PER_CLIENT_OP] = do_div(1.0/user_hz*x[CPU_IOWAIT_CNT], client_ops)*1000000;

    for (i=0; i<ps->stats.n_stat_devices; i++) {

	x  = pstats_out->io_raw_stat[i];
	xd = pstats_out->io_stat[i];

	xd[IOP_RD_PER_OP]         = do_div(x[IOP_RD_CNT], ops);
	xd[IOP_WR_PER_OP]         = do_div(x[IOP_WR_CNT], ops);
	xd[K_IOBYTES_RD_PER_OP]   = do_div(x[BYTES_RD_CNT], ops)/1000;
	xd[K_IOBYTES_WR_PER_OP]   = do_div(x[BYTES_WR_CNT], ops)/1000;

	xd[IOP_RD_PER_CLIENT_OP]         = do_div(x[IOP_RD_CNT], client_ops);
	xd[IOP_WR_PER_CLIENT_OP]         = do_div(x[IOP_WR_CNT], client_ops);
	xd[K_IOBYTES_RD_PER_CLIENT_OP]   = do_div(x[BYTES_RD_CNT], client_ops)/1000;
	xd[K_IOBYTES_WR_PER_CLIENT_OP]   = do_div(x[BYTES_WR_CNT], client_ops)/1000;

	xd[K_BYTES_PER_RD_IOP]    = do_div(x[BYTES_RD_CNT], x[IOP_RD_CNT])/1000;
	xd[K_BYTES_PER_WR_IOP]    = do_div(x[BYTES_WR_CNT], x[IOP_WR_CNT])/1000;
	xd[K_IOPS_RD_PER_SEC]     = do_div(x[IOP_RD_CNT], secs)/1000;
	xd[K_IOPS_WR_PER_SEC]     = do_div(x[IOP_WR_CNT], secs)/1000;
	xd[IOPS_RD_MERGE_PER_SEC] = do_div(x[IOP_RD_MERGE_CNT], secs);
	xd[IOPS_WR_MERGE_PER_SEC] = do_div(x[IOP_WR_MERGE_CNT], secs);
	xd[MBYTES_RD_PER_SEC]     = do_div(x[BYTES_RD_CNT], secs)/1000000;
	xd[MBYTES_WR_PER_SEC]     = do_div(x[BYTES_WR_CNT], secs)/1000000;
    }
}

static void init_substats(ws_state_t *ps, ws_substats_t *pss)
{
    int   i, j;

    pss->ws_raw_stat = (uint64_t *) malloc(N_WS_RAW_STATS*sizeof(uint64_t));
    assert(pss->ws_raw_stat != NULL);
    for (i=0; i<N_WS_RAW_STATS; i++) {
	pss->ws_raw_stat[i] = 0;
    }

    pss->ws_stat = (double *) malloc(N_WS_STATS*sizeof(double));
    assert(pss->ws_stat != NULL);
    for (i=0; i<N_WS_STATS; i++) {
	pss->ws_stat[i] = 0;
    }

    pss->cpu_raw_stat = (uint64_t *) malloc(N_CPU_RAW_STATS*sizeof(uint64_t));
    assert(pss->cpu_raw_stat != NULL);
    for (i=0; i<N_CPU_RAW_STATS; i++) {
	pss->cpu_raw_stat[i] = 0;
    }

    pss->cpu_stat = (double *) malloc(N_CPU_STATS*sizeof(double));
    assert(pss->cpu_stat != NULL);
    for (i=0; i<N_CPU_STATS; i++) {
	pss->cpu_stat[i] = 0;
    }

    for (i=0; i<WS_MAX_STAT_DEVICES; i++) {
	pss->io_raw_stat[i] = (uint64_t *) malloc(N_IO_RAW_STATS*sizeof(uint64_t));
	assert(pss->io_raw_stat[i] != NULL);
	for (j=0; j<N_IO_RAW_STATS; j++) {
	    pss->io_raw_stat[i][j] = 0;
	}

	pss->io_stat[i] = (double *) malloc(N_IO_STATS*sizeof(double));
	assert(pss->io_stat[i] != NULL);
	for (j=0; j<N_IO_STATS; j++) {
	    pss->io_stat[i][j] = 0;
	}
    }
    
}

static void init_stats(ws_state_t *ps)
{
    int     i;
    char    line[WS_LINE_SIZE];
    char    sector_filename[WS_LINE_SIZE];
    FILE   *f;

    ps->stats.in_steady_state = 0;

    init_substats(ps, &(ps->stats.stats));
    init_substats(ps, &(ps->stats.stats_t0));
    init_substats(ps, &(ps->stats.stats_ss0));
    init_substats(ps, &(ps->stats.stats_last));
    init_substats(ps, &(ps->stats.stats_overall));
    init_substats(ps, &(ps->stats.stats_ss));
    init_substats(ps, &(ps->stats.stats_window));

    if (gettimeofday(&(ps->stats.stats_t0.tspec), NULL) != 0) {
        errmsg("gettimeofday failed in init_stats with error '%s'", strerror(errno));
    }

    if (get_raw_system_stats(NULL, ps, &(ps->stats.stats_t0)) != 0) {
        errmsg("init_stats(): could not get raw system stats");
    }
    copy_substats(&(ps->stats.stats_last), &(ps->stats.stats_t0));

    wsrt_init_stats(&ps->stats.gc_compaction, "GC_Compaction");
    wsrt_init_stats(&ps->stats.batch_writes,  "Batch_Writes");

    ps->user_hz = sysconf(_SC_CLK_TCK); // clock ticks per second (usually 100?)
    infomsg("User Hz = %d\n", ps->user_hz);

    infomsg("%d Stat Devices: \n", ps->stats.n_stat_devices);
    for (i=0; i<ps->stats.n_stat_devices; i++) {
        infomsg("%s: ", ps->stats.stat_device_names[i]);
        sprintf(sector_filename, "/sys/block/%s/queue/hw_sector_size", ps->stats.stat_device_names[i]);
	f = fopen(sector_filename, "r");
	if (f == NULL) {
	    ps->stats.stat_device_sector_bytes[i] = 512;
	    infomsg("%d sector bytes\n", ps->stats.stat_device_sector_bytes[i]);
	    errmsg("init_stats() could not open hw_sector_size file for device '%s'; setting sector size for device to default of 512", ps->stats.stat_device_names[i]);
	    continue;
	}
	if (fgets(line, WS_LINE_SIZE, f) == NULL) {
	    ps->stats.stat_device_sector_bytes[i] = 512;
	    infomsg("%d sector bytes\n", ps->stats.stat_device_sector_bytes[i]);
	    errmsg("init_stats() could not parse hw_sector_size file for device '%s'; setting sector size for device to default of 512", ps->stats.stat_device_names[i]);
	} else if (sscanf(line, "%d", &(ps->stats.stat_device_sector_bytes[i])) != 1) {
	    ps->stats.stat_device_sector_bytes[i] = 512;
	    infomsg("%d sector bytes\n", ps->stats.stat_device_sector_bytes[i]);
	    errmsg("init_stats() could not parse hw_sector_size file for device '%s'; setting sector size for device to default of 512", ps->stats.stat_device_names[i]);
	} else {
            //  force this since the hw_sector_size file is wrong sometimes!
	    ps->stats.stat_device_sector_bytes[i] = 512;
	    infomsg("%d sector bytes\n", ps->stats.stat_device_sector_bytes[i]);
	}
	if (fclose(f) != 0) {
	    errmsg("init_stats() could not close hw_sector_size file for device '%s'", ps->stats.stat_device_names[i]);
	}
    }
}

void WSStats(struct ZS_thread_state *pzs, struct ws_state *ps, ws_stats_t *stats)
{
    calc_stats(pzs, ps);
    memcpy((void *) stats, (void *) &(ps->stats), sizeof(ws_stats_t));
    copy_substats(&(stats->stats),         &(ps->stats.stats));
    copy_substats(&(stats->stats_t0),      &(ps->stats.stats_t0));
    copy_substats(&(stats->stats_ss0),     &(ps->stats.stats_ss0));
    copy_substats(&(stats->stats_last),    &(ps->stats.stats_last));
    copy_substats(&(stats->stats_overall), &(ps->stats.stats_overall));
    copy_substats(&(stats->stats_ss),      &(ps->stats.stats_ss));
    copy_substats(&(stats->stats_window),  &(ps->stats.stats_window));
}

static void copy_substats(ws_substats_t *pto, ws_substats_t *pfrom)
{
    int   i, j;

    pto->tspec = pfrom->tspec;

    for (i=0; i<N_WS_RAW_STATS; i++) {
	pto->ws_raw_stat[i] = pfrom->ws_raw_stat[i];
    }

    for (i=0; i<N_WS_STATS; i++) {
	pto->ws_stat[i] = pfrom->ws_stat[i];
    }

    for (i=0; i<N_CPU_RAW_STATS; i++) {
	pto->cpu_raw_stat[i] = pfrom->cpu_raw_stat[i];
    }

    for (i=0; i<N_CPU_STATS; i++) {
	pto->cpu_stat[i] = pfrom->cpu_stat[i];
    }

    for (i=0; i<WS_MAX_STAT_DEVICES; i++) {
	for (j=0; j<N_IO_RAW_STATS; j++) {
	    pto->io_raw_stat[i][j] = pfrom->io_raw_stat[i][j];
	}

	for (j=0; j<N_IO_STATS; j++) {
	    pto->io_stat[i][j] = pfrom->io_stat[i][j];
	}
    }
}

static void dump_raw_stats(FILE *f, ws_state_t *ps, uint32_t nstats, uint64_t *stat_window, uint64_t *stat_ss, uint64_t *stat_overall, const char *(string_fn)(int n))
{
    int    i;

    for (i=0; i<nstats; i++) {
        fprintf(f, "%30s = %12"PRIu64", %12"PRIu64", %12"PRIu64"\n", (string_fn)(i), stat_window[i], stat_ss[i], stat_overall[i]);
        
    }
}

static void dump_nonraw_stats(FILE *f, ws_state_t *ps, uint32_t nstats, double *stat_window, double *stat_ss, double *stat_overall, const char *(string_fn)(int n))
{
    int    i;

    for (i=0; i<nstats; i++) {
        fprintf(f, "%30s = %12g, %12g, %12g\n", (string_fn)(i), stat_window[i], stat_ss[i], stat_overall[i]);
        
    }
}

static void dump_stats(FILE *f, struct ws_state *ps)
{
    int           i;
    uint64_t      t;

    t = get_secs(&(ps->stats.stats.tspec)) - get_secs(&(ps->stats.stats_t0.tspec));

    fprintf(f, "\n");
    fprintf(f, "========  Stats at t=%"PRIu64" secs (window, steady-state, overall)  =======\n", t);
    fprintf(f, "\n");
    dump_nonraw_stats(f, ps, N_WS_STATS, ps->stats.stats_window.ws_stat, ps->stats.stats_ss.ws_stat, ps->stats.stats_overall.ws_stat, WSStatString);
    fprintf(f, "\n");
    dump_nonraw_stats(f, ps, N_CPU_STATS, ps->stats.stats_window.cpu_stat, ps->stats.stats_ss.cpu_stat, ps->stats.stats_overall.cpu_stat, WSCPUStatString);
    fprintf(f, "\n");
    for (i=0; i<ps->stats.n_stat_devices; i++) {
        if (i>0) {
	    fprintf(f, "\n");
	}
        fprintf(f, "Device '%s':\n", ps->stats.stat_device_names[i]);
	dump_nonraw_stats(f, ps, N_IO_STATS, ps->stats.stats_window.io_stat[i], ps->stats.stats_ss.io_stat[i], ps->stats.stats_overall.io_stat[i], WSIOStatString);
    }
    fprintf(f, "\n");

    dump_raw_stats(f, ps, N_WS_RAW_STATS, ps->stats.stats_window.ws_raw_stat, ps->stats.stats_ss.ws_raw_stat, ps->stats.stats_overall.ws_raw_stat, WSRawStatString);
    fprintf(f, "\n");
    dump_raw_stats(f, ps, N_CPU_RAW_STATS, ps->stats.stats_window.cpu_raw_stat, ps->stats.stats_ss.cpu_raw_stat, ps->stats.stats_overall.cpu_raw_stat, WSCPURawStatString);
    fprintf(f, "\n");
    for (i=0; i<ps->stats.n_stat_devices; i++) {
        if (i>0) {
	    fprintf(f, "\n");
	}
        fprintf(f, "Device '%s':\n", ps->stats.stat_device_names[i]);
	dump_raw_stats(f, ps, N_IO_RAW_STATS, ps->stats.stats_window.io_raw_stat[i], ps->stats.stats_ss.io_raw_stat[i], ps->stats.stats_overall.io_raw_stat[i], WSIORawStatString);
    }
    
    wsrt_dump_stats(f, &(ps->stats.gc_compaction), 1 /* dumpflag */);
    wsrt_dump_stats(f, &(ps->stats.batch_writes), 1 /* dumpflag */);
    fprintf(f, "\n");
}

void WSDumpStats(FILE *f, struct ZS_thread_state *pzs, struct ws_state *ps)
{
    calc_stats(pzs, ps);

    dump_stats(f, ps);
    if (WSStatsFile != NULL) {
	dump_stats(WSStatsFile, ps);
    }

    if (fflush(f) != 0) {
        errmsg("fflush failed for 'f' in WSDumpStats with error '%s'", strerror(errno));
    }
    if (WSStatsFile != NULL) {
	if (fflush(WSStatsFile) != 0) {
	    errmsg("fflush failed for 'WSStatsFile' in WSDumpStats with error '%s'", strerror(errno));
	}
    }
}

  /*  returns:
   *    WS_OK
   *    WS_ERROR
   */
int WSChangeConfig(struct ws_state *ps)
{
    TRACE(ps, "WSChangeConfig");
    // purposefully empty
    return(WS_OK);
}

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
int WSRead(struct ws_state *ps, void *pzst, char *key, uint32_t key_len, uint64_t addr, void *pdata, uint64_t size)
{
    uint64_t         n_stripe;
    alat_entry_t     lat_entry;
    lat_data_t      *pld;
    void            *pfrom;
    int              do_read_io;
    ws_stripe_buf_t *sbuf;
    uint64_t         keynum;

    /*   The "Look Aside Table" (lat) structure is used
     *   to ensure that reads concurrent with write serialization and
     *   GC return current data.
     */

    keynum = *((uint64_t *) key);
    DOSTAT(ps, pzst, N_READS, 1);
    DOSTAT(ps, pzst, N_READ_BYTES, size);
    n_stripe = stripe_from_addr(ps, addr);
    TRACE(ps, "WSRead(key=%"PRIu64", addr=0x%llx, n_stripe=%lld, pdata=%p, size=%lld) ...", keynum, addr, n_stripe, pdata, size);
    lat_entry = alat_read_start(ps->lat, n_stripe);
    if (lat_entry.lat_handle == NULL) {
	do_read_io = 1;
        DOSTAT(ps, pzst, N_LAT_MISSES, 1);
    } else {
        DOSTAT(ps, pzst, N_LAT_HITS, 1);
	pld   = (lat_data_t *) lat_entry.pdata;
	sbuf    = pld->sbuf;
	TRACE(ps, "WSRead: hit in Look-Aside Table: key=%"PRIu64", sbuf[%d].n_stripe=%"PRIu64", addr=%"PRIu64"", keynum, sbuf->n, sbuf->n_stripe, addr);
	// copy data from sbuf
	pfrom = sbuf->buf + (addr % ps->stripe_bytes);
	(void) memcpy(pdata, pfrom, size);
	do_read_io = 0;
    }
    if (do_read_io) {
	TRACE(ps, "WSRead: miss in Look-Aside Table: I/O read for addr=%"PRIu64", key=%"PRIu64", stripe=%"PRIu64"", addr, keynum, n_stripe);
	do_point_read(ps, pdata, addr, size);
    } else {
	alat_read_end(lat_entry.lat_handle);
    }

    TRACE(ps, "... WSRead: (key=%"PRIu64", stripe=%"PRIu64")", keynum, n_stripe);
    return(WS_OK);
}

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

int WSWriteAllocate(struct ws_state *ps, void *pzst, char *key, uint32_t key_len, uint64_t size)
{
    int                overflow;
    uint64_t           p_old;
    int32_t            n_extra_cond_waits;
    uint64_t           keynum;

    keynum = *((uint64_t *) key);

    DOSTAT(ps, pzst, N_WRITES, 1);
    DOSTAT(ps, pzst, N_WRITE_BYTES, size);
    TRACE(ps, "WSWriteAllocate(key=%lld, size=%lld ) ...", *((uint64_t *) key), size);
    if (WSReadOnly) {
	TRACE(ps, "... WSWriteAllocate in read-only mode!");
        return(WS_READ_ONLY);
    }
    if (ps->quiesce) {
	pthread_mutex_lock(&(ps->quiesce_mutex));
	pthread_cond_wait(&(ps->quiesce_cv), &(ps->quiesce_mutex));
	pthread_mutex_unlock(&(ps->quiesce_mutex));
    }

    (void) __sync_fetch_and_add(&(ps->writes_in_progress), 1);

    overflow           = 0;
    n_extra_cond_waits = -1;
    while (1) {

	p_old = __sync_fetch_and_add(&WSGlobalPtr, size);
	WSCurPtr  = p_old;
	WSCurSize = size;

	if ((p_old + size) <= ps->stripe_bytes) {
	    TRACE(ps, "WSWriteAllocate: key=%"PRIu64", non-overflowed buffer; n_stripe=%lld, curbuf=%p, WSCurPtr=%lld, WSCurSize=%lld", keynum, ps->curbuf->n_stripe, ps->curbuf, WSCurPtr, WSCurSize);
	    break;
	} else {
	    // overflow
	    if (p_old <= ps->stripe_bytes) {

	        // only one consumer will meet this criteria

                /*  Get a new serialization buffer.
		 *  This is done here (presumably) before the caller grabs
		 *  the ZS hashtable lock.  This avoids deadlock.
		 */

		WSNewBuf = (ws_stripe_buf_t *) mboxWait(&(ps->mbox_free));
		WSNewBuf->state = Next_Open;

		TRACE(ps, "WSWriteAllocate: key=%"PRIu64", overflowed buffer--allocator of WSNewBuf: n_stripe=%lld, curbuf=%p, WSCurPtr=%lld, WSCurSize=%lld, WSNewBuf=%p (n_stripe=%lld)", keynum, ps->curbuf->n_stripe, ps->curbuf, WSCurPtr, WSCurSize, WSNewBuf, WSNewBuf->n_stripe);

		assert(p_old == ps->stripe_bytes); // xxxzzz remove me and other asserts
		overflow = 1;
		break;

	    } else {

		TRACE(ps, "WSWriteAllocate: key=%"PRIu64", overflowed buffer--waiter; n_stripe=%lld, curbuf=%p, WSCurPtr=%lld, WSCurSize=%lld", keynum, ps->curbuf->n_stripe, ps->curbuf, WSCurPtr, WSCurSize);

                // yield and try again
		pthread_yield();

		if (ps->is_terminating) {
		    TRACE(ps, "WSWriteAllocate: terminating!");
		    (void) __sync_fetch_and_add(&(ps->writes_in_progress), -1);
		    return(WS_EARLY_TERMINATION);
		}
	    }
	}
    }

    if (n_extra_cond_waits > 0) {
        DOSTAT(ps, pzst, N_EXTRA_COND_WAITS, n_extra_cond_waits);
    }

    if (overflow) {
	TRACE(ps, "... WSWriteAllocate key=%"PRIu64" OVERFLOW; newbuf=%p, p=%lld, size=%lld, new_stripe=%"PRIu64"", keynum, WSNewBuf, WSCurPtr, WSCurSize, WSNewBuf->n_stripe);
    } else {
	TRACE(ps, "... WSWriteAllocate key=%"PRIu64"; curbuf=%p, p=%lld, size=%lld, stripe=%"PRIu64"", keynum, ps->curbuf, WSCurPtr, WSCurSize, ps->curbuf->n_stripe);
    }
    return(WS_OK);
}

static void do_batch_write(ws_state_t *ps, ws_stripe_buf_t *sbuf, uint64_t p, uint64_t size)
{
    write_mail_t       ml;

    if (!WSBatchReturnMboxInited) {
        mboxInit(&WSBatchReturnMbox);
        WSBatchReturnMboxInited = 1;
    }

    ml.mbox_return = &WSBatchReturnMbox;
    ml.sbuf        = sbuf;
    ml.p           = p;
    ml.size        = size;
    mboxPost(&(ps->mbox_batch), (uint64_t) &ml);
    (void) mboxWait(&WSBatchReturnMbox);
}

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

int WSWriteComplete(struct ws_state *ps, void *pzst, char *key, uint32_t key_len, void *pdata, uint64_t size, uint64_t *addr, int is_update, uint64_t old_addr, uint64_t old_size)
{
    uint64_t           bytes_wrote;
    uint64_t           old_n_stripe;
    uint64_t           final_ptr;
    ws_stripe_buf_t   *oldbuf;
    uint64_t           keynum;
    uint64_t           write_stripe;

    keynum = *((uint64_t *) key);

    old_n_stripe = stripe_from_addr(ps, old_addr);

    TRACE(ps, "WSWriteComplete(key=%lld, pdata=%p, size=%lld, is_update=%d, old_addr=0x%llx (n_stripe=%lld), old_size=%"PRIu64", curbuf=%p, curptr=%"PRIu64", bytes_written_so_far=%lld, bytes_when_full=%lld) ...", *((uint64_t *) key), pdata, size, is_update, old_addr, old_n_stripe, old_size, ps->curbuf, WSCurPtr, ps->curbuf->bytes_written_so_far, ps->curbuf->bytes_when_full);

    if (WSCurSize != size) {
	panicmsg("WSWriteComplete: WSCurSize (=%lld) is not equal to size (=%lld)", WSCurSize, size);
	panicmsg("Going into read-only mode");
	// go to read-only mode
	goto_readonly();
	(void) __sync_fetch_and_add(&(ps->writes_in_progress), -1);
	return(WS_ERROR);
    }
    assert(size == 8192); //  xxxzzz remove me

    if (is_update) {
	DOSTAT(ps, pzst, N_UPDATES, 1);
	DOSTAT(ps, pzst, N_UPDATE_BYTES, size);
    }

    if (WSReadOnly) {
	TRACE(ps, "... WSWriteComplete in read-only mode!");
        return(WS_READ_ONLY);
    }
    if (ps->quiesce) {
	pthread_mutex_lock(&(ps->quiesce_mutex));
	pthread_cond_wait(&(ps->quiesce_cv), &(ps->quiesce_mutex));
	pthread_mutex_unlock(&(ps->quiesce_mutex));
    }

    if (is_update) {
	if (WSDelete(ps, pzst, key, key_len, old_addr, old_size)) {
	    panicmsg("WSWriteComplete: WSDelete failed for addr=%lld (n_stripe=%lld), size=%lld", old_addr, old_n_stripe, old_size);
	    panicmsg("Going into read-only mode");
	    // go to read-only mode
	    goto_readonly();
	    (void) __sync_fetch_and_add(&(ps->writes_in_progress), -1);
	    return(WS_DELETE_ERROR);
	}
    }

    if ((WSCurPtr + size) <= ps->stripe_bytes) {

        //  This is one of possibly many non-overflow writes
	TRACE(ps, "WSWriteComplete: key=%"PRIu64", non-overflow write; buf=%p (n_stripe=%lld), WSCurPtr=%lld", keynum, ps->curbuf, ps->curbuf->n_stripe, WSCurPtr);

	memcpy(ps->curbuf->buf + WSCurPtr, pdata, size);
	*addr = addr_from_stripe(ps, ps->curbuf->n_stripe) + WSCurPtr;
	write_stripe = ps->curbuf->n_stripe;

	if (ps->config.batch_size > 0) {
	    do_batch_write(ps, ps->curbuf, WSCurPtr, size);
	}

    } else {

        assert(WSCurPtr == ps->stripe_bytes);

        //  This is the sole overflow write: put data in new stripe buffer
	TRACE(ps, "WSWriteComplete: key=%"PRIu64", overflow write; buf=%p (n_stripe=%lld), p=%lld", keynum, WSNewBuf, WSNewBuf->n_stripe, WSNewBuf->p);

	memcpy(WSNewBuf->buf + WSNewBuf->p, pdata, size);
	*addr = addr_from_stripe(ps, WSNewBuf->n_stripe) + WSNewBuf->p;
	write_stripe = WSNewBuf->n_stripe;

	if (ps->config.batch_size > 0) {
	    do_batch_write(ps, WSNewBuf, WSNewBuf->p, size);
	}

	/*  I can do this here because WSNewBuf has not yet been made available
	 *  for concurrent access.
	 */
	WSNewBuf->p += size;
	WSNewBuf->bytes_written_so_far += size;
	WSFinalPtr = WSCurPtr;
    }

    bytes_wrote = __sync_add_and_fetch(&(ps->curbuf->bytes_written_so_far), size);
    if (bytes_wrote > ps->stripe_bytes) {

	/*  Overflow!
	 *
	 *  WSWriteCompletion() has been called for all writes for 
	 *  this stripe buffer, including the single final write that
	 *  overflows.  My job is to:
         *    - assign the new current serialization buffer
         *    - pass old stripe buffer to IO writer
	 */

        int64_t     delta;

	// assign the new serialization buffer
	TRACE(ps, "WSWriteComplete: key=%"PRIu64", overflowed buffer--fixer; old_buf=%p (old_stripe=%lld): final_ptr=%lld, new_buf=%p (new_stripe=%lld)", keynum, ps->curbuf, ps->curbuf->n_stripe, WSFinalPtr, WSNewBuf, WSNewBuf->n_stripe);
	final_ptr     = WSFinalPtr;  // remember value before curbuf is changed!
	assert(ps->curbuf != WSNewBuf);
 
        oldbuf            = ps->curbuf;
        ps->curbuf        = WSNewBuf;
	ps->curbuf->state = Open;

        delta = WSNewBuf->p;
	(void) __atomic_exchange_n(&WSGlobalPtr, delta, __ATOMIC_ACQ_REL);

	//  Must do these after taking oldbuf out of circulation:
	oldbuf->p     = final_ptr;
	oldbuf->state = Full_pending_io;

	// submit the filled buffer for writing
	mboxPost(&(ps->mbox_write), (uint64_t) oldbuf);
    }

    (void) __sync_fetch_and_add(&(ps->writes_in_progress), -1);
    TRACE(ps, "... WSWriteComplete key=%"PRIu64"; addr=0x%llx, stripe=%"PRIu64"", keynum, *addr, write_stripe);
    return(WS_OK);
}

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
int WSDelete(struct ws_state *ps, void *pzst, char *key, uint32_t key_len, uint64_t addr, uint64_t size)
{
    int       ret;
    uint64_t  n_stripe;

    /* update valid sector count for the corresponding stripe */

    DOSTAT(ps, pzst, N_DELETES, 1);
    DOSTAT(ps, pzst, N_DELETE_BYTES, size);
    n_stripe = stripe_from_addr(ps, addr);
    TRACE(ps, "WSDelete(key=%lld, addr=0x%llx, size=%lld, n_stripe=%lld) ...", *((uint64_t *) key), addr, size, n_stripe);
    if (WSReadOnly) {
	TRACE(ps, "... WSDelete; Read-Only Mode!");
        return(WS_READ_ONLY);
    }
    if (ps->quiesce) {
	pthread_mutex_lock(&(ps->quiesce_mutex));
	pthread_cond_wait(&(ps->quiesce_cv), &(ps->quiesce_mutex));
	pthread_mutex_unlock(&(ps->quiesce_mutex));
    }
    (void) __sync_fetch_and_add(&(ps->deletes_in_progress), 1);

    ret = stripe_tbl_adjust_count(ps, addr, -size);

    (void) __sync_fetch_and_add(&(ps->deletes_in_progress), -1);
    TRACE(ps, "... WSDelete; ret=%d)", ret);
    return(ret);
}

    /* 
     *  Returns: 0 if success, 1 otherwise.
     */
static int stripe_tbl_adjust_count(ws_state_t *ps, uint64_t addr, int32_t size)
{
    uint64_t            n_stripe;
    int16_t             old_cnt, new_cnt;
    int16_t             old_group, new_group;
    ws_fill_groups_t   *pfg;
    ws_stripe_entry_t  *pse, *pse2;
    ws_stripe_tbl_t    *pst;

    pfg = &(ps->fill_groups);
    pst = &(ps->stripe_tbl);

    assert(size%ps->sector_bytes == 0);

    n_stripe = stripe_from_addr(ps, addr);
    pse = &(ps->stripe_tbl.entries[n_stripe]);
    old_cnt = __sync_fetch_and_add(&(pse->cnt), size/ps->sector_bytes);
    new_cnt = old_cnt + size/ps->sector_bytes;
    if (new_cnt < 0) {
        new_cnt = 0;
    }

    old_group = pse->n_group;
    new_group = get_group(ps, new_cnt);

    TRACE(ps, "stripe_tbl_adjust_count(addr=0x%llx, size=%d, n_stripe=%lld, n_group=%d, old_cnt=%d, new_cnt=%d, old_group=%d, new_group=%d) ...", addr, size, n_stripe, pse->n_group, old_cnt, new_cnt, old_group, new_group);

    if (pse->n_group != -1) { // make sure stripe is not open
	if (new_group != old_group) {

	    //  detach from old group
            while (1) {
		pthread_mutex_lock(&pfg->group_lock[old_group]);
		if (pse->n_group == old_group) {
		    break;
		}
		pthread_mutex_unlock(&pfg->group_lock[old_group]);
		old_group = pse->n_group;
		if (old_group == -1) {
		    goto done;
		}
	    }
	    pse->n_group = -1;
	    if (pse->next != WS_NULL_STRIPE_ENTRY) {
		pse2 = &(pst->entries[pse->next]);
		pse2->prev = pse->prev;
	    }
	    if (pse->prev != WS_NULL_STRIPE_ENTRY) {
		pse2 = &(pst->entries[pse->prev]);
		pse2->next = pse->next;
	    } else {
		pfg->fill_groups[old_group] = pse->next;
	    }
	    pthread_mutex_unlock(&pfg->group_lock[old_group]);

	    //  insert into new group
	    pthread_mutex_lock(&pfg->group_lock[new_group]);
	    pse->n_group = new_group;
	    pse->prev = WS_NULL_STRIPE_ENTRY;
	    pse->next = pfg->fill_groups[new_group];
	    pfg->fill_groups[new_group] = n_stripe;
	    if (pse->next != WS_NULL_STRIPE_ENTRY) {
		pse2 = &(pst->entries[pse->next]);
		pse2->prev = n_stripe;
	    }
	    pthread_mutex_unlock(&pfg->group_lock[new_group]);
	}
    }
done:
    return(0);
}

static uint32_t get_group(ws_state_t *ps, int16_t cnt)
{
    uint32_t             percent;
    int                  i;
    uint32_t             n_group;
    ws_fill_groups_t    *pfg = &(ps->fill_groups);

    if (cnt < 0) {
        cnt = 0;
    }
    // this is percent empty!
    percent = 0.5 + 100.0 - (100.0*cnt)/ps->stripe_sectors;

    /*  NOTE: fill group percentage i is for stripes with 
     *  emptiness <= to that percentage.
     */

    for (i=0; i<pfg->n_fill_groups; i++) {
        if (percent >= pfg->fill_group_percents[i]) {
	    break;
	}
    }
    n_group = i;
    assert(n_group < pfg->n_fill_groups); // xxxzzz remove me

    return(n_group);
}

static uint64_t stripe_from_addr(ws_state_t *pst, uint64_t addr)
{
    uint64_t   n_stripe;

    n_stripe = addr / pst->stripe_bytes;
    return(n_stripe);
}

static uint64_t addr_from_stripe(ws_state_t *pst, uint64_t n_stripe)
{
    uint64_t   addr;

    addr = n_stripe*pst->stripe_bytes;
    return(addr);
}

/**********************************************************************
 *
 *  simple bit vector data structure
 *
 **********************************************************************/

typedef struct bv {
    uint32_t    n_ints;
    uint32_t   *ints;
} bv_t;

struct bv *bv_init(uint32_t n)
{
    int     i;
    bv_t   *bv;

    bv = (bv_t *) malloc(sizeof(bv_t));
    assert(bv);
    bv->n_ints = (n+8*sizeof(uint32_t)-1)/(8*sizeof(uint32_t));
    bv->ints = (uint32_t *) malloc(bv->n_ints*sizeof(uint32_t));
    assert(bv->ints);
    for (i=0; i<bv->n_ints; i++) {
        bv->ints[i] = 0;
    }
    return(bv);
}

void bv_clear(struct bv *bv)
{
    int     i;
    for (i=0; i<bv->n_ints; i++) {
        bv->ints[i] = 0;
    }
}

void bv_set(struct bv *bv, uint32_t n)
{
    uint32_t  ni, no;
    
    ni = n/(8*sizeof(uint32_t));
    no = n % (8*sizeof(uint32_t));

    bv->ints[ni] |= (1<<no);
}

void bv_unset(struct bv *bv, uint32_t n)
{
    uint32_t  ni, no;
    
    ni = n/(8*sizeof(uint32_t));
    no = n % (8*sizeof(uint32_t));

    bv->ints[ni] &= (~(1<<no));
}

int bv_test(struct bv *bv, uint32_t n)
{
    uint32_t  ni, no;
    
    ni = n/(8*sizeof(uint32_t));
    no = n % (8*sizeof(uint32_t));

    if (bv->ints[ni] & (1<<no)) {
        return(1);
    } else {
        return(0);
    }
}


/**********************************************************************
 *
 *  end of code for simple bit vector data structure
 *
 **********************************************************************/

    /*  Used by WSCheck */
static struct bv *WSStripeBitvec     = NULL;
static struct bv *WSFreeStripeBitvec = NULL;
static uint32_t  *WSStripeCnts       = NULL;

static void quiesce(ws_state_t *ps)
{
    struct timespec           ts, ts_rem;

    /*
     *   To quiesce:
     *       - Set to ReadOnly mode.
     *       - Wait for pending write requests to finish
     *           - global client I/O count
     *       - Wait for pending GC's to finish
     *           - global GC count
     *       - Wait for pending stripe I/O's to finish
     *           - global stripe I/O count
     */

    ps->quiesce = 1;

    ts.tv_sec  = ps->config.quiesce_usecs/1000000;
    ts.tv_nsec = (ps->config.quiesce_usecs % 1000000)*1000;

    while ((ps->writes_in_progress != 0) ||
           (ps->deletes_in_progress != 0) ||
           (ps->stripe_writes_in_progress != 0) ||
           (ps->batch_writes_in_progress != 0) ||
           (ps->scrubs_in_progress != 0) ||
           (ps->gcs_in_progress != 0))
    {
	//  xxxzzz should I care about the return value here?
	(void) nanosleep(&ts, &ts_rem);
    }
}

static void unquiesce(ws_state_t *ps)
{

    /*
     *   To unquiesce:
     *       - clear quiesce flag
     *       - wake-up threads waiting on quiesce conditional variable
     */

    ps->quiesce = 0;
    pthread_mutex_lock(&(ps->quiesce_mutex));
    pthread_cond_broadcast(&(ps->quiesce_cv));
    pthread_mutex_unlock(&(ps->quiesce_mutex));
}

static void per_node_check_cb(void *pdata, uint64_t addr)
{
    uint64_t      n_stripe;
    ws_state_t   *ps;

    ps = (ws_state_t *) pdata;

    n_stripe = stripe_from_addr(ps, addr);
    (WSStripeCnts[n_stripe])++;
    bv_set(WSStripeBitvec, n_stripe);
}


    /*  Run comprehensive consistency checks.
     *  Should only be called by a single thread at a time.
     *  (Concurrent calls to WSCheck have undefined results!)
     * 
     *  Returns: WS_OK if success, WS_ERROR otherwise.
     */
int WSCheck(FILE *f, struct ws_state *ps, void *pzst, uint32_t sectors_per_node)
{
    uint64_t             i;
    uint64_t            *st;
    uint32_t             cnt;
    uint32_t             pct_actual;
    uint64_t             vcnt;
    uint64_t             n_stripe;
    uint64_t             old_n_stripe;
    uint64_t             n_stripes;
    uint64_t             n_free_stripes;
    uint64_t             n_free;
    uint64_t             n_empty;
    uint64_t             n_nonempty;
    ws_stripe_entry_t   *pse;
    ws_fill_groups_t    *pfg;

    TRACE(ps, "WSCheck()");

    pfg = &(ps->fill_groups);

    quiesce(ps); // things must be quiet for the check!

    /*  Allocate static check structures only once
     */

    if (WSStripeBitvec == NULL) {
	WSStripeBitvec = bv_init(ps->n_stripes);
	assert(WSStripeBitvec);
	WSFreeStripeBitvec = bv_init(ps->n_stripes);
	assert(WSFreeStripeBitvec);
	WSStripeCnts   = (uint32_t *) malloc(ps->n_stripes*sizeof(uint32_t));
	assert(WSStripeCnts);
    }
    bv_clear(WSStripeBitvec);
    bv_clear(WSFreeStripeBitvec);

    for (i=0; i<ps->n_stripes; i++) {
        WSStripeCnts[i] = 0;
    }

    /*    - Enumerate all B-tree nodes.
     *    - Mark all touched stripes and accumulate counts peer stripe.
     *    - Check that accumulated counts match stripe table counts.
     *    - Check that counts don't exceed max per stripe!
     *    - Check that untouched stripes are:
     *        - in free list (with cnt==0)
     *        - OR: not in free list, but cnt==0
     */

    ps->config.btree_traversal_cb((void *) ps, pzst, per_node_check_cb);

    for (n_stripe=0; n_stripe<ps->n_stripes; n_stripe++) {
	pse = &(ps->stripe_tbl.entries[n_stripe]);
	if (pse->cnt != WSStripeCnts[i]) {
	    check_err(f, "Stripe '%"PRIu64"' has an inconsistent count (%d found, %d expected)!\n", n_stripe, pse->cnt, WSStripeCnts[i]);
	}
	if (pse->cnt > ps->stripe_sectors/sectors_per_node) {
	    check_err(f, "Stripe '%"PRIu64"' has a count (%d) that exceeds the maximum possible (%d)", n_stripe, pse->cnt, ps->stripe_sectors/sectors_per_node);
	}
    }

    /*  Check that all stripes are accounted for between:
     *     - free list
     *     - holding 1 or more B-tree blocks or data
     *     - empty but not on free list
     *
     *  Differentiating "empty" from "free":
     *     - pse->cnt == 0 but pse not on free list
     */

    for (n_stripe = pfg->free_list;
	 n_stripe != WS_NULL_STRIPE_ENTRY;
	 n_stripe = pse->next)
    {
	bv_set(WSFreeStripeBitvec, n_stripe);
	pse = &(ps->stripe_tbl.entries[n_stripe]);
	if (pse->cnt != 0) {
	    check_err(f, "Stripe "PRIu64" is in free list with non-zero count (%d)", pse->cnt);
	}
    }

    n_free     = 0;
    n_empty    = 0;
    n_nonempty = 0;
    for (n_stripe=0; n_stripe<ps->n_stripes; n_stripe++) {
	pse = &(ps->stripe_tbl.entries[n_stripe]);
	if (pse->cnt == 0) {
	    if (bv_test(WSFreeStripeBitvec, n_stripe)) {
	        n_empty++;
	    } else {
	        n_free++;
	    }
	} else {
	    n_nonempty++;
	    if (!bv_test(WSStripeBitvec, n_stripe)) {
	        check_err(f, "Stripe %"PRIu64" is non-empty (cnt=%d) but not was not touched during b-tree traversal", n_stripe, pse->cnt);
	    }
	}
    }

    assert((n_free + n_empty + n_nonempty) == ps->n_stripes);

    check_info(f, "Stripe breakdown: %"PRIu64" free, %"PRIu64" empty, %"PRIu64" non-empty [%"PRIu64" total]", n_free, n_empty, n_nonempty, ps->n_stripes);

    /*  Check that fill groups are correct.
     */

    bv_clear(WSStripeBitvec);

    (void) check_fill_group_percents(&(ps->fill_groups));

    old_n_stripe   = WS_NULL_STRIPE_ENTRY;
    n_free_stripes = 0;
    for (n_stripe = pfg->free_list;
	 n_stripe != WS_NULL_STRIPE_ENTRY;
	 n_stripe = pse->next)
    {
	bv_set(WSStripeBitvec, n_stripe);
	n_free_stripes++;
	pse = &(ps->stripe_tbl.entries[n_stripe]);
	if (pse->cnt != 0) {
	    check_err(f, "Free stripe '%"PRIu64"' has a non-zero count (%d)!\n", n_stripe, pse->cnt);
	}

        /*  linked list checks  */
	if (n_stripe == pfg->free_list) {
	     if (pse->prev != WS_NULL_STRIPE_ENTRY) {
	         check_err(f, "First free list entry has non-NULL back pointer (%"PRIu64")", pse->prev);
	     }
	} else {
	     if (pse->prev != old_n_stripe) {
	         check_err(f, "Free list entry has inconsistent back pointer (%"PRIu64" found, %"PRIu64" expected)", pse->prev, old_n_stripe);
	     }
	}
	old_n_stripe = n_stripe;
    }

    old_n_stripe = WS_NULL_STRIPE_ENTRY;
    n_stripes    = 0;
    vcnt         = 0;
    for (i=0; i<pfg->n_fill_groups; i++) {
	for (n_stripe = pfg->fill_groups[i];
	     n_stripe != WS_NULL_STRIPE_ENTRY;
	     n_stripe = pse->next)
	{
	    bv_set(WSStripeBitvec, n_stripe);
	    n_stripes++;
	    pse = &(ps->stripe_tbl.entries[n_stripe]);
	    vcnt += pse->cnt;
	    pct_actual = 0.5 + (100.0 - 100.0*(((double) pse->cnt)/ps->stripe_sectors));
	    if (i==0) {
		if (pct_actual >= pfg->fill_group_percents[i]) {
		    check_err(f, "Stripe '%"PRIu64"' has fill of %d%% (cnt=%d), but is in fill group [0%%-%d%%]!\n", n_stripe, pct_actual, pse->cnt, pfg->fill_group_percents[i]);
		}
	    } else {
		if ((pct_actual < pfg->fill_group_percents[i-1]) ||
		   (pct_actual >= pfg->fill_group_percents[i]))
		{
		    check_err(f, "Stripe '%"PRIu64"' has fill of %d%% (cnt=%d), but is in fill group [%d%%-%d%%]!\n", n_stripe, pct_actual, pse->cnt, pfg->fill_group_percents[i-1], pfg->fill_group_percents[i]);
		}
	    }

	    /*  linked list checks  */
	    if (n_stripe == pfg->fill_groups[i]) {
		 if (pse->prev != WS_NULL_STRIPE_ENTRY) {
		     check_err(f, "First fill_group[%d] list entry has non-NULL back pointer (%"PRIu64")", i, pse->prev);
		 }
	    } else {
		 if (pse->prev != old_n_stripe) {
		     check_err(f, "fill_group[%d] list entry has inconsistent back pointer (%"PRIu64" found, %"PRIu64" expected)", i, pse->prev, old_n_stripe);
		 }
	    }
	    old_n_stripe = n_stripe;
	}
    }

    if (n_stripes + n_free_stripes != ps->n_stripes) {
	check_err(f, "Total number of free (%"PRIu64") and fill group stripes (%"PRIu64") DOES NOT MATCH total number of stripes (%"PRIu64")!!!!\n", n_free_stripes, n_stripes, ps->n_stripes);
    }

    /*  Check that all stripes are accounted for.
     */

    cnt = 0;
    for (n_stripe=0; n_stripe<ps->n_stripes; n_stripe++) {
        if (!bv_test(WSStripeBitvec, n_stripe)) {
	    cnt++;
	    check_err(f, "Stripe %"PRIu64" is not in a free or fill group list\n", n_stripe);
	}
    }
    if (cnt == 0) {
	check_err(f, "All stripes accounted for in a free or fill group list.\n");
    } else {
	check_err(f, "%d stripes are unaccounted for in a free or fill group list.\n", cnt);
    }

    /*  Check that byte counts are consistent and reasonable.
     */

    st = ps->stats.stats.ws_raw_stat;
    if ((vcnt*ps->sector_bytes) != (st[N_WRITE_BYTES] - st[N_UPDATE_BYTES])) {
        check_err(f, "Stripe byte count (%"PRIu64") does not match stats write byte count adjusted for updates (%"PRIu64" = %"PRIu64" - %"PRIu64")", vcnt*ps->sector_bytes, vcnt*ps->sector_bytes, st[N_WRITE_BYTES], st[N_UPDATE_BYTES]);
    }

    /*  Check that alat table is in a reasonable state.
     */

    if (alat_check(f, ps->lat, lat_check_cb, (void *) ps) != 0) {
        alat_dump(f, ps->lat);
    }

    unquiesce(ps); // let processing of writes proceed

    return(WS_OK);
}

static int lat_check_cb(FILE *f, void *pdata, uint64_t addr, void *ae_pdata, int locked, int is_write_lock)
{
    ws_state_t        *ps   = (ws_state_t *) pdata;
    lat_data_t        *pld  = (lat_data_t *) ae_pdata;
    ws_stripe_buf_t   *sbuf = pld->sbuf;
    int                ret  = 0;

    if (addr != sbuf->n_stripe) {
        check_err(f, "Lookaside Table: addr=%"PRIu64" does not match sbuf n_stripe=%"PRIu64"\n", addr, sbuf->n_stripe);
	ret = 1;
    }
    if (locked) {
        /*  Shouldn't be locked during quiesced state */
        check_err(f, "Lookaside Table: Entry for n_stripe=%"PRIu64" is locked while in the quiesced state\n", addr);
	ret = 1;
    }
    if (sbuf->state == Open) {
        /*  There should be exactly one Open sbuf */
        if (ps->curbuf != sbuf) {
	    check_err(f, "Lookaside Table: Open sbuf for n_stripe=%"PRIu64" does not match ps->curbuf (n_stripe=%"PRIu64")\n", addr, ps->curbuf->n_stripe);
	    ret = 1;
	}
    } else {
        if (sbuf->state != Compacted_and_ready) {
	    check_err(f, "Lookaside Table: sbuf for n_stripe=%"PRIu64" is in state '%s' in quiesced state; expected 'Open' or 'Compacted_and_ready' state\n", addr, WSSbufStateString(sbuf->state));
	    ret = 1;
	}
    }
    return(ret);
}

    /*  Dump contents of data structures.
     * 
     *  "level" can be 0, 1 or 2: higher levels dump more stuff.
     * 
     *  Returns: WS_OK if success, WS_ERROR otherwise.
     */
int WSDump(FILE *f, struct ws_state *ps, struct ZS_thread_state *pzs, int level)
{
    int                  j;
    uint64_t             i;
    ws_fill_groups_t    *pfg;
    uint32_t             cnt;
    uint64_t             tcnt;
    uint64_t             n_stripe;
    uint64_t             n_stripes;
    uint64_t             n_free_stripes;
    ws_stripe_entry_t   *pse;
    char                 sprev[100], snext[100];
    ws_stripe_buf_t     *sbuf;

    TRACE(ps, "WSDump()");

    pfg = &(ps->fill_groups);

    /*   Configuration */
    fprintf(f, "\nConfiguration:\n\n");
    WSDumpConfig(f, ps);

    /*   Stats */
    fprintf(f, "\nStatistics:\n\n");
    WSDumpStats(f, pzs, ps);

    /*   Stripe Table */

    fprintf(f, "\nStripe Table:\n");
    cnt = 0;
    fprintf(f, "          ");
    for (n_stripe=0; n_stripe<ps->n_stripes; n_stripe++) {
	pse = &(ps->stripe_tbl.entries[n_stripe]);
	if (pse->prev == WS_NULL_STRIPE_ENTRY) {
	    strcpy(sprev, "-");
	} else {
	    sprintf(sprev, "%d", pse->prev);
	}
	if (pse->next == WS_NULL_STRIPE_ENTRY) {
	    strcpy(snext, "-");
	} else {
	    sprintf(snext, "%d", pse->next);
	}
	fprintf(f, "[%"PRIu64":%d,%d,%s,%s] ", n_stripe, pse->cnt, pse->n_group, sprev, snext);
	cnt++;
	if (cnt >= 4) {
	    cnt = 0;
	    if (ps->n_stripes > 32) {
		if (n_stripe == 15) {
		    fprintf(f, "\n          ...");
		    n_stripe = ps->n_stripes - 16;
		}
	    }
	    fprintf(f, "\n          ");
	}
    }
    fprintf(f, "\n\n");

    /*   Free Stripes */

    fprintf(f, "Free Stripes:");
    fprintf(f, "\n          ");

    n_free_stripes = 0;
    for (n_stripe = pfg->free_list;
	 n_stripe != WS_NULL_STRIPE_ENTRY;
	 n_stripe = pse->next)
    {
	pse = &(ps->stripe_tbl.entries[n_stripe]);
        n_free_stripes++;
    }

    cnt            = 0;
    tcnt           = 0;
    for (n_stripe = pfg->free_list;
	 n_stripe != WS_NULL_STRIPE_ENTRY;
	 n_stripe = pse->next)
    {
	pse = &(ps->stripe_tbl.entries[n_stripe]);
	tcnt++;

	if (n_free_stripes > 40) {
	    if ((tcnt > 20) && (tcnt < (n_free_stripes - 19))) {
	        continue;
	    }
	}

	cnt++;
	fprintf(f, "%"PRIu64",", n_stripe);

	if (cnt >= 10) {
	    cnt = 0;
	    if (n_free_stripes > 40) {
		if (tcnt == 20) {
		    fprintf(f, "\n          ...");
		}
	    }
	    fprintf(f, "\n          ");
	}
    }
    fprintf(f, "\n\n");

    /*   Fill Groups */

    n_stripes = 0;
    fprintf(f, "Fill Groups:\n\n");
    for (i=0; i<pfg->n_fill_groups; i++) {
        fprintf(f, "   [%"PRIu64"] ", i);
        fprintf(f, "%d%%: ", pfg->fill_group_percents[i]);
	cnt = 0;
	for (n_stripe = pfg->fill_groups[i];
	     n_stripe != WS_NULL_STRIPE_ENTRY;
	     n_stripe = pse->next)
	{
	    n_stripes++;
	    pse = &(ps->stripe_tbl.entries[n_stripe]);
	    fprintf(f, "%"PRIu64",", n_stripe);
	    cnt++;
	    if (cnt >= 10) {
	        cnt = 0;
                fprintf(f, "\n          ");
	    }
	}

        fprintf(f, "\n");
    }
    fprintf(f, "   %"PRIu64" stripes in fill group structure\n", n_stripes);

    if (n_stripes + n_free_stripes == ps->n_stripes) {
	fprintf(f, "   total number of free and fill group stripes matches total number of stripes (%"PRIu64")\n", ps->n_stripes);
    } else {
	fprintf(f, "   total number of free (%"PRIu64") and fill group stripes (%"PRIu64") DOES NOT MATCH total number of stripes (%"PRIu64")!!!!\n", n_free_stripes, n_stripes, ps->n_stripes);
    }

    /*   Address Look-aside Table */

    fprintf(f, "\nAddress Look-aside Table:\n\n");
    alat_dump(f, ps->lat);
    fprintf(f, "\n");

    /*   State of current stripe buffer */

    sbuf = ps->curbuf;
    fprintf(f, "Current stripe buffer: ");
    fprintf(f, "n_stripe=%"PRIu64", p=%"PRIu64", state='%s', bytes_written_so_far=%"PRIu64", bytes_when_full=%"PRIu64"\n", sbuf->n_stripe, sbuf->p, WSSbufStateString(sbuf->state), sbuf->bytes_written_so_far, sbuf->bytes_when_full);
    fprintf(f, "\n");

    /*   Dump all state buffers  */

    j = 0;
    for (sbuf = ps->sbufs; sbuf != NULL; sbuf = sbuf->next) {
	fprintf(f, "sbuf[%d]: n_stripe=%"PRIu64", p=%"PRIu64", state='%s', bytes_written_so_far=%"PRIu64", bytes_when_full=%"PRIu64"\n", j, sbuf->n_stripe, sbuf->p, WSSbufStateString(sbuf->state), sbuf->bytes_written_so_far, sbuf->bytes_when_full);
	j++;
    }
    fprintf(f, "\n");

    return(WS_OK);
}

/****************************************************************
 *
 *   Batch Writer Threadpool
 *
 ****************************************************************/

static void *batch_thread(threadpool_argstuff_t *as)
{
    uint32_t            i;
    uint32_t            n;
    uint32_t            n_sbuf;
    ws_state_t         *ps;
    write_mail_t       *todo[MAX_WRITE_BATCH];
    struct iovec        iov[MAX_WRITE_BATCH];
    ssize_t             ssize;
    int                 fd;
    ws_stripe_buf_t    *sbuf;
    off_t               offset;
    off_t               offset_to_use;
    uint64_t            total_size;
    struct threadpool  *ptp;

    ps  = (ws_state_t *) as->pdata;
    ptp = as->ptp;

    TRACE(ps, "batch_thread started (id=%ld)", get_tid());

    fd     = ps->config.batch_fd;

    offset = 0;

    while (1) {

        TRACE(ps, "batch_thread writing waiting for a new batch...");

	if (ptp->quit) {
	    break;
	}

        if (WSReadOnly) {
	    TRACE(ps, "batch_thread in Read-Only mode!");
	    todo[0] = (write_mail_t *) mboxWait(&(ps->mbox_batch));
	    if (todo[0] != NULL) {
		mboxPost(todo[0]->mbox_return, 1);
	    }
	    continue;
	}

	(void) __sync_fetch_and_add(&(ps->batch_writes_in_progress), 1);

        n = 0;
	total_size = 0;

	todo[n] = (write_mail_t *) mboxWait(&(ps->mbox_batch));
	TRACE(ps, "batch_thread received write[%d]: n_sbuf=%d, p=%lld, size=%lld", n, todo[n]->sbuf->n, todo[n]->p, todo[n]->size);
	sbuf             = todo[n]->sbuf;
	total_size      += todo[n]->size;
	iov[n].iov_base  = (void *) (sbuf->buf + todo[n]->p);
	iov[n].iov_len   = (size_t) todo[n]->size;
	n++;


        if (ps->config.batch_size > 1) {
	    while ((todo[n] = (write_mail_t *) mboxTry(&(ps->mbox_batch))) != NULL) {
		TRACE(ps, "batch_thread received write[%d]: n_sbuf=%d, p=%lld, size=%lld", n,todo[n]->sbuf->n, todo[n]->p, todo[n]->size);
		total_size += todo[n]->size;
		iov[n].iov_base = (void *) (sbuf->buf + todo[n]->p);
		iov[n].iov_len  = (size_t) todo[n]->size;

		n++;
		if (n >= ps->config.batch_size) {
		    break;
		}
	    }
	}
	if (WSReadOnly) {
	    for (i=0; i<n; i++) {
		mboxPost(todo[i]->mbox_return, 1);
	    }
	    continue;
	}

	n_sbuf = todo[0]->sbuf->n;
	offset_to_use = offset;

	offset += total_size;
	if (offset >= (2*ps->config.n_stripe_bufs*ps->stripe_bytes)) {
	    offset = 0;
	}

	TRACE(ps, "batch_thread writing to sbuf[%d], %d iovec's, %lld bytes total, new stripe file offset=%lld", n_sbuf, n, total_size, offset_to_use);
	wsrt_record_value(n, &(ps->stats.batch_writes));
        ssize = pwritev(fd, iov, n, offset_to_use);
	if (ssize != total_size) {
	    panicmsg("batch_thread: pwritev wrote %d bytes, but %lld expected", ssize, total_size);
	    goto_readonly();
	}
	for (i=0; i<n; i++) {
	    mboxPost(todo[i]->mbox_return, 1);
	}
	(void) __sync_fetch_and_add(&(ps->batch_writes_in_progress), -1);
    }
    return(NULL);
}

/****************************************************************
 *
 *   I/O Writer Threadpool
 *
 ****************************************************************/

static void *writer_thread(threadpool_argstuff_t *as)
{
    ws_state_t             *ps;
    struct threadpool      *ptp;
    ws_stripe_buf_t        *sbf;
    uint64_t                cnt;
    uint64_t                x;
    struct ZS_thread_state *pzst;
    int                     rc;

    ps   = (ws_state_t *) as->pdata;
    ptp  = as->ptp;
    pzst = NULL;

    if (as->myid == 0) {
	rc = ps->config.per_thread_cb(ps->zs_state, &pzst);
	if (rc != WS_OK) {
	    panicmsg("Failure in writer_thread: could not allocate per-thread state ");
	    panicmsg("Going into read-only mode");
	    // go to read-only mode
	    goto_readonly();
	}
    }

    TRACE(ps, "writer_thread started (id=%ld)", get_tid());

    cnt = 0;
    while ((sbf = (ws_stripe_buf_t *) mboxWait(&(ps->mbox_write)))) {

	TRACE(ps, "writer_thread writing sbf=%p, n_stripe = %lld, p=%lld", sbf, sbf->n_stripe, sbf->p);

	if ((as->myid == 0) && (pzst != NULL)) {
            x = ps->stats.stats.ws_raw_stat[N_IO_STRIPE_WRITE];
	    if ((ps->config.check_interval != 0) && (x >= ps->next_check_cnt)) {
		(void) WSCheck(stderr, ps, pzst, ps->config.sectors_per_node);
		ps->next_check_cnt += ps->config.check_interval;
	    }
	}
	cnt++;

	if (ptp->quit) {
	    break;
	}

        if (WSReadOnly) {
	    TRACE(ps, "writer_thread in Read-Only mode!");
	    continue;
	}

	(void) __sync_fetch_and_add(&(ps->stripe_writes_in_progress), 1);

        if (do_stripe_write(ps, sbf, 1) != 0) {
	    /*  Error in do_stripe_write().
	     *  There should already be an error message, so make
	     *  sure we go into ReadOnly mode.
	     */
	    goto_readonly();
	    (void) __sync_fetch_and_add(&(ps->stripe_writes_in_progress), -1);
	    continue;
	}
	// kick off a garbage collection to backfill the empty buffer
	TRACE(ps, "writer_thread passing sbf=%p (stripe=%"PRIu64") to mbox_gc", sbf, sbf->n_stripe);
	sbf->state = Available_for_gc;
	mboxPost(&(ps->mbox_gc), (uint64_t) sbf);
	(void) __sync_fetch_and_add(&(ps->stripe_writes_in_progress), -1);
    }
    TRACE(ps, "writer_thread quitting");
    return(NULL);
}

static int do_stripe_write(ws_state_t *ps, ws_stripe_buf_t *sbuf, int syncflag)
{
    alat_entry_t        le;
    ws_fill_groups_t   *pfg = &(ps->fill_groups);
    ws_stripe_entry_t  *pse, *pse2;
    uint32_t            n_group;
    uint32_t            nfree;
    ssize_t             sze;
    uint64_t            addr;
    ws_stripe_tbl_t    *pst = &(ps->stripe_tbl);

    DOSTAT(ps, NULL, N_IO_STRIPE_WRITE, 1);
    DOSTAT(ps, NULL, N_IO_STRIPE_WRITE_BYTES, ps->stripe_bytes);

    // do RAID stuff
    // purposefully empty for now

    le = alat_write_start(ps->lat, sbuf->n_stripe);
    if (le.lat_handle == NULL) {
	panicmsg("Failure in do_stripe_write: inconsistency in address lookaside table for stripe=%lld; Going into read-only mode", sbuf->n_stripe);
	goto_readonly();
	return(1);
    }

    //  write out sbuf synchronously

    //  xxxzzz just do pwrite for now; change to io_submit later
    #ifdef notdef
    io_submit(xxxzzz);
    if (syncflag) {
	io_getevents(xxxzzz);
    }
    #endif 

    addr = addr_from_stripe(ps, sbuf->n_stripe);

    TRACE(ps, "do_stripe_write: n_stripe=%lld, addr=0x%llx", sbuf->n_stripe, addr);

    //  xxxzzz hardcoded to 1 device for now!
    sze = pwrite(ps->config.fd_devices[0], sbuf->buf, ps->stripe_bytes, addr + ps->config.device_offset_mb*1024*1024);
    // xxxzzz improve error handling
    // xxxzzz what if fewer bytes were written?
    if (sze != ps->stripe_bytes) {
        if (sze == -1) {
	    panicmsg("pwrite for stripe=%lld failed with errno=%d [%s]; Going into read-only mode", sbuf->n_stripe, errno, strerror(errno));
	} else {
	    panicmsg("pwrite for stripe=%lld only wrote %lld of %lld bytes; Going into read-only mode", sbuf->n_stripe, sze, ps->stripe_bytes);
	}
	goto_readonly();
        return(1);
    }

    alat_write_end_and_delete(le.lat_handle);

    //  Put written stripe in a fill group

    pse = &(pst->entries[sbuf->n_stripe]);
    //  adjust cnt to allow for free space at end
    assert(sbuf->p <= ps->stripe_bytes); // xxxzzz remove me
    nfree = ps->stripe_sectors - sbuf->p/ps->sector_bytes;
    __sync_fetch_and_add(&(pse->cnt), -nfree);

    n_group = get_group(ps, pse->cnt);

    TRACE(ps, "do_stripe_write: cnt=%d, n_group=%d, stripe=%"PRIu64"", pse->cnt, n_group, sbuf->n_stripe);

    pthread_mutex_lock(&pfg->group_lock[n_group]);
    pse->prev = WS_NULL_STRIPE_ENTRY;
    pse->next = pfg->fill_groups[n_group];
    pfg->fill_groups[n_group] = sbuf->n_stripe;
    if (pse->next != WS_NULL_STRIPE_ENTRY) {
	pse2 = &(pst->entries[pse->next]);
	pse2->prev = sbuf->n_stripe;
    }
    pse->n_group = n_group;
    pthread_mutex_unlock(&pfg->group_lock[n_group]);

    return(0);
}

static int do_point_read(ws_state_t *ps, void *pdata, uint64_t addr, uint32_t size)
{
    ssize_t    sze;
    uint64_t   n_stripe;

    n_stripe = stripe_from_addr(ps, addr);
    DOSTAT(ps, NULL, N_IO_POINT_READ, 1);
    DOSTAT(ps, NULL, N_IO_POINT_READ_BYTES, size);
    TRACE(ps, "do_point_read: addr=0x%llx, pdata=%p, size=%d, stripe=%"PRIu64"", addr, pdata, size, n_stripe);
    // xxxzzz hardwired to 1 device for now!
    sze = pread(ps->config.fd_devices[0], pdata, size, addr + ps->config.device_offset_mb*1024*1024);
    // xxxzzz improve error handling
    // xxxzzz what if fewer bytes were read?
    if (sze != size) {
        return(1);
    }
    return(0);
}

/****************************************************************
 *
 *   Stripe Table Dumper Threadpool
 *
 ****************************************************************/

static void *stripe_tbl_dumper_thread(threadpool_argstuff_t *as)
{
    struct timespec           ts, ts_rem;
    uint64_t                  i;
    ws_stripe_entry_t        *pse_from;
    uint64_t                  n_dumps, total_dumps;
    struct threadpool        *ptp;
    uint32_t                  keylen;
    uint32_t                  data_size;
    char                      key[STRIPE_KEY_SIZE];
    ws_state_t               *ps;
    ws_stripe_tbl_t          *pst;
    ws_config_t              *cfg;
    struct ZS_thread_state   *pzst;
    int                       rc;

    ps   = (ws_state_t *) as->pdata;
    pst  = &(ps->stripe_tbl);
    cfg  = &(ps->config);
    ptp  = as->ptp;

    rc = cfg->per_thread_cb(ps->zs_state, &pzst);
    if (rc != WS_OK) {
	panicmsg("Failure in stripe table dumper: could not allocate per-thread state ");
	panicmsg("Going into read-only mode");
	// go to read-only mode
	goto_readonly();
        return(NULL);
    }

    total_dumps = (ps->n_stripes + pst->entries_per_dump - 1)/pst->entries_per_dump;

    ts.tv_sec  = cfg->stripe_tbl_dump_usecs/1000000;
    ts.tv_nsec = (cfg->stripe_tbl_dump_usecs % 1000000)*1000;

    TRACE(ps, "stripe_tbl_dumper_thread started (id=%ld), total_dumps = %lld", get_tid(), total_dumps);

    while (1) {

	if (ptp->quit) {
	    break;
	}

	pse_from = pst->entries;
	n_dumps = 0;
	for (i=0; i<ps->n_stripes; i+= pst->entries_per_dump) {

	    if (ptp->quit) {
		break;
	    }

            //  xxxzzz should I care about the return value here?
	    (void) nanosleep(&ts, &ts_rem);

	    if (WSReadOnly) {
		TRACE(ps, "stripe_tbl_dumper_thread in Read-Only mode!");
		continue;
	    }

	    keylen = build_stripe_key(key, i);
	    if (n_dumps < (total_dumps - 1)) {
		data_size = pst->entries_per_dump*sizeof(ws_stripe_entry_t);
	    } else {
		data_size = (ps->n_stripes % pst->entries_per_dump)*sizeof(ws_stripe_entry_t);
	    }
	    TRACE(ps, "stripe_tbl_dumper_thread: set_md_cb(key=%s, pse_from=%p, data_size=%d", key, pse_from, data_size);
	    DOSTAT(ps, pzst, N_MD_SET, 1);
	    DOSTAT(ps, pzst, N_MD_SET_BYTES, keylen+data_size);
	    if (cfg->set_md_cb(pzst, ps->config.cb_state, key, keylen, pse_from, data_size) != 0) {
		panicmsg("Failure in stripe table dumper writing dump %d for persisted stripe table", n_dumps);
		panicmsg("Going into read-only mode");
		// go to read-only mode
		goto_readonly();
	    }
	    n_dumps++;
	    pse_from += pst->entries_per_dump;
	}
	DOSTAT(ps, pzst, N_STRIPE_TBL_DUMPS, 1);
    }

    TRACE(ps, "stripe_tbl_dumper_thread quitting");
    return(0);
}

/****************************************************************
 *
 *   Garbage Collection Threadpool
 *
 ****************************************************************/

static void *gc_thread(threadpool_argstuff_t *as)
{
    ws_state_t               *ps;
    ws_stripe_buf_t          *sbuf;
    struct threadpool        *ptp;

    // xxxzzz zs thread state is allocated in callback routine in fdf_ws.c!

    ps  = (ws_state_t *) as->pdata;
    ptp = as->ptp;

    TRACE(ps, "gc_thread started (id=%ld)", get_tid());

    while ((sbuf = (ws_stripe_buf_t *) mboxWait(&(ps->mbox_gc)))) {
	TRACE(ps, "gc_thread received sbuf=%p, n_stripe = %lld", sbuf, sbuf->n_stripe);

	if (ptp->quit) {
	    break;
	}

        if (WSReadOnly) {
	    TRACE(ps, "gc_thread in Read-Only mode");
	    continue;
	}
        if (do_gc(ps, sbuf)) {
	    //  purposefully empty
	}
    }
    TRACE(ps, "gc_thread quitting");
    return(NULL);
}

static uint64_t get_free_stripe(ws_state_t *ps)
{
    ws_stripe_entry_t  *pse;
    ws_fill_groups_t   *pfg;
    uint64_t            n_stripe;

    pfg = &(ps->fill_groups);

    pthread_mutex_lock(&(pfg->free_lock));
    if (pfg->free_list == WS_NULL_STRIPE_ENTRY) {
	pthread_mutex_unlock(&(pfg->free_lock));
        return(WS_NULL_STRIPE_ENTRY);
    } else {
        n_stripe       = pfg->free_list;
	pse            = &(ps->stripe_tbl.entries[n_stripe]);
        pfg->free_list = pse->next;
	pthread_mutex_unlock(&(pfg->free_lock));
    }
    return(n_stripe);
}

static int do_gc(ws_state_t *ps, ws_stripe_buf_t *sbuf)
{
    int                       i;
    ws_stripe_tbl_t          *pst;
    ws_fill_groups_t         *pfg;
    ws_stripe_entry_t        *pse, *pse2;
    ws_config_t              *cfg;
    uint64_t                  n_stripe;
    uint64_t                  cleaned_bytes;
    alat_entry_t              ae;
    lat_data_t               *pld;

    sbuf->state = In_gc;
    DOSTAT(ps, NULL, N_GCS, 1);

    pst = &(ps->stripe_tbl);
    cfg = &(ps->config);
    pfg = &(ps->fill_groups);

    (void) __sync_fetch_and_add(&(ps->gcs_in_progress), 1);

    /*************************************************
     *
     *  Crash-safeness:
     *      - xxxzzz
     *
     *  Stripe buffer states (in order of transitions):
     *  These must be persisted at transitions (via nvram) 
     *  and are used for recovery
     *  xxxzzz checksums to detect torn nodes/data during recovery?
     *
     *   Unused
     *   Open
     *   Full_pending_io
     *   Available_for_gc
     *   Compacted_but_transient
     *   Compacted_and_ready
     *   Next_Open
     *
     *   - stripes are removed from a fill group when they are
     *     selected for gc
     *   - they are returned to a fill group only after they
     *     have been written out
     *
     *
     *  Thread-safeness:
     *      - xxxzzz
     *
     *************************************************/

    // use up unused stripes until they are all in play
    if ((n_stripe = get_free_stripe(ps)) != WS_NULL_STRIPE_ENTRY) {

        TRACE(ps, "gc_thread: found free stripe=%lld", n_stripe);

        /*   Since free stripes are the result of a
	 *   format, no garbage collection is needed.
	 */

	sbuf->n_stripe             = n_stripe;
	sbuf->p                    = 0;
	sbuf->bytes_written_so_far = 0;
	sbuf->bytes_when_full      = ps->stripe_bytes;
	pse            = &(pst->entries[n_stripe]);
	/*  Initialize as full, to be ready for do_stripe_write(). */
	pse->cnt       = ps->stripe_sectors;
	pse->n_group   = -1;

	/*  allocate a data lookaside table entry so reads are handled
	 *  correctly.
	 */

	ae = alat_create_start(ps->lat, n_stripe, 1 /* must_not_exist */);
	if (ae.pdata == NULL) {
	    panicmsg("Problem creating an entry in the data lookaside table; going into read-only mode");
	    goto_readonly();
	    (void) __sync_fetch_and_add(&(ps->gcs_in_progress), -1);
	    return(1);
	}

	pld = (lat_data_t *) ae.pdata;
	pld->sbuf = sbuf;

	alat_create_end(ae.lat_handle);

    } else {

	for (i=0; i<pfg->n_fill_groups; i++) {
	    pthread_mutex_lock(&pfg->group_lock[i]);
	    if ((n_stripe = pfg->fill_groups[i]) != WS_NULL_STRIPE_ENTRY) {
		pse = &(pst->entries[n_stripe]);
		// found the most empty stripe
		// remove from linked list
		pfg->fill_groups[i] = pse->next;
		if (pse->next != WS_NULL_STRIPE_ENTRY) {
		    pse2 = &(pst->entries[pse->next]);
		    pse2->prev = WS_NULL_STRIPE_ENTRY;
		}
		pse->n_group = -1;
		pthread_mutex_unlock(&pfg->group_lock[i]);
		break;
	    }
	    pthread_mutex_unlock(&pfg->group_lock[i]);
	}
	assert(n_stripe != WS_NULL_STRIPE_ENTRY);

        TRACE(ps, "gc_thread: no free stripes, so selected victim stripe=%lld, cnt=%d, from fill_group[%d]", n_stripe, pse->cnt, i);
	if ((pse->cnt > 0) && (pse->cnt >= ps->stripe_sectors)) {
	    //   no free sectors!
	    //  go into read-only mode!
	    panicmsg("Failure in gc_cb: no free space, going into read-only mode");
	    goto_readonly();
	    (void) __sync_fetch_and_add(&(ps->gcs_in_progress), -1);
	    return(1);
	}

	sbuf->n_stripe = n_stripe;
	sbuf->p        = 0;

	/*  Initialize as full.
	 *  Adjust for unused sectors in do_stripe_write.
	 *  Any deletions within this stripe will be handled
	 *  properly by WSDelete().
	 */
	pse->cnt       = ps->stripe_sectors; 

	/*   Read in entire stripe
	 *   Must do this BEFORE creating alat entry, otherwise
	 *   alat read hits may return junk data!
	 */

        TRACE(ps, "gc_thread: gc_phase1_cb(buf=%p, n_stripe=%lld)", sbuf->buf, sbuf->n_stripe);
	if (cfg->gc_phase1_cb(ps->config.cb_state, sbuf->buf, &(sbuf->p), ps->stripe_bytes, ps->sector_bytes, addr_from_stripe(ps, sbuf->n_stripe), &cleaned_bytes, ps->config.device_offset_mb*1024*1024)) {
	    //  go into read-only mode!
	    panicmsg("Failure in gc_phase1_cb: going into read-only mode");
	    goto_readonly();
	    (void) __sync_fetch_and_add(&(ps->gcs_in_progress), -1);
	    return(1);
	}
	DOSTAT(ps, NULL, N_IO_STRIPE_READ, 1);
	DOSTAT(ps, NULL, N_IO_STRIPE_READ_BYTES, cleaned_bytes);

	/*  allocate a data lookaside table entry so reads are handled
	 *  correctly.
	 */

	ae = alat_create_start(ps->lat, n_stripe, 1 /* must_not_exist */);
	if (ae.pdata == NULL) {
	    panicmsg("Problem creating an entry in the data lookaside table; going into read-only mode");
	    goto_readonly();
	    (void) __sync_fetch_and_add(&(ps->gcs_in_progress), -1);
	    return(1);
	}

	pld = (lat_data_t *) ae.pdata;
	pld->sbuf = sbuf;

	alat_create_end(ae.lat_handle);

	/* Compact the stripe
	 *
	 *  NOTE: Compaction will never cause an overflow of the stripe
	 *        (eg: because of b-tree actions).
	 *	      This isn't possible because gc only makes space-neutral updates
	 *	      to leaf nodes.
	 */

	//  call gc callback to do the heavy lifting

        TRACE(ps, "gc_thread: gc_phase2_cb(buf=%p, n_stripe=%lld)", sbuf->buf, sbuf->n_stripe);
	if (cfg->gc_phase2_cb(ps->config.cb_state, sbuf->buf, &(sbuf->p), ps->stripe_bytes, ps->sector_bytes, addr_from_stripe(ps, sbuf->n_stripe), &cleaned_bytes, ps->config.device_offset_mb*1024*1024)) {
	    //  go into read-only mode!
	    panicmsg("Failure in gc_phase2_cb: going into read-only mode");
	    goto_readonly();
	    (void) __sync_fetch_and_add(&(ps->gcs_in_progress), -1);
	    return(1);
	}
	sbuf->bytes_written_so_far = cleaned_bytes;
	sbuf->bytes_when_full      = ps->stripe_bytes;
        TRACE(ps, "gc_thread: gc_cb successfully returned, %lld cleaned bytes for stripe=%"PRIu64"", cleaned_bytes, sbuf->n_stripe);
	DOSTAT(ps, NULL, N_GC_BYTES, cleaned_bytes);
	wsrt_record_value(cleaned_bytes, &(ps->stats.gc_compaction));
    }

    TRACE(ps, "gc_thread passing sbuf=%p to mbox_free, sbuf->p=%lld, n_stripe=%lld", sbuf, sbuf->p, sbuf->n_stripe);
    sbuf->state = Compacted_and_ready;
// xxxzzz
if (sbuf->p > ps->stripe_bytes) {
    fprintf(stderr, "Oh-oh! (do_gc)\n");
}
    mboxPost(&(ps->mbox_free), (uint64_t) sbuf);
    (void) __sync_fetch_and_add(&(ps->gcs_in_progress), -1);
    return(0);
}

/****************************************************************
 *
 *   Scrubber Threadpool
 *
 ****************************************************************/

static void *scrub_thread(threadpool_argstuff_t *as)
{
    struct timespec           ts, ts_rem;
    ws_state_t               *ps;
    ws_config_t              *cfg;
    struct threadpool        *ptp;

    ps  = (ws_state_t *) as->pdata;
    cfg = &(ps->config);
    ptp = as->ptp;

    ts.tv_sec  = cfg->scrub_usecs/1000000;
    ts.tv_nsec = (cfg->scrub_usecs % 1000000)*1000;

    TRACE(ps, "scrub_thread started (id=%ld)", get_tid());

    while (1) {
	TRACE(ps, "scrub_thread starting an iteration");

	if (ptp->quit) {
	    break;
	}

	//  xxxzzz should I care about the return value here?
	(void) nanosleep(&ts, &ts_rem);

        // xxxzzz check this quiesce stuff:
	if (ps->quiesce) {
	    pthread_mutex_lock(&(ps->quiesce_mutex));
	    pthread_cond_wait(&(ps->quiesce_cv), &(ps->quiesce_mutex));
	    pthread_mutex_unlock(&(ps->quiesce_mutex));
	}

	// (void) __sync_fetch_and_add(&(ps->deletes_in_progress), 1);
        // xxxzzz purposefully empty for now!
	// (void) __sync_fetch_and_add(&(ps->deletes_in_progress), -1);

    }

    TRACE(ps, "scrub_thread quitting");
    return(0);
}

/****************************************************************
 *
 *   Stats Threadpool
 *
 ****************************************************************/

static void *stats_thread(threadpool_argstuff_t *as)
{
    struct timespec           ts, ts_rem;
    ws_state_t               *ps;
    ws_config_t              *cfg;
    struct threadpool        *ptp;
    struct ZS_thread_state   *pzs;
    int                       rc;

    ps  = (ws_state_t *) as->pdata;
    cfg = &(ps->config);
    ptp = as->ptp;

    rc = ps->config.per_thread_cb(ps->zs_state, &pzs);
    if (rc != WS_OK) {
	panicmsg("Failure in stats_thread: could not allocate per-thread state ");
	pzs = NULL;
    }

    ts.tv_sec  = cfg->stats_secs;
    ts.tv_nsec = 0;

    TRACE(ps, "stats_thread started (id=%ld)", get_tid());

    while (1) {
	TRACE(ps, "stats_thread starting an iteration");

	if (ptp->quit) {
	    break;
	}

	//  xxxzzz should I care about the return value here?
	(void) nanosleep(&ts, &ts_rem);

	WSDumpStats(stderr, pzs, ps);
    }

    TRACE(ps, "stats_thread quitting");
    return(0);
}


/****************************************************************
 *
 *   Miscellaneous
 *
 ****************************************************************/

static void infomsg(char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);
   vfprintf(stderr, fmt, args);
   va_end(args);

   if (WSStatsFile != NULL) {
       va_start(args, fmt);
       vfprintf(WSStatsFile, fmt, args);
       va_end(args);
   }
}

static void tracemsg(char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);
   fprintf(stderr, "WS_TRACE[%ld]> ", get_tid());
   vfprintf(stderr, fmt, args);
   fprintf(stderr, "\n");
   va_end(args);

   if (WSStatsFile != NULL) {
       va_start(args, fmt);
       fprintf(WSStatsFile, "WS_TRACE[%ld]> ", get_tid());
       vfprintf(WSStatsFile, fmt, args);
       fprintf(WSStatsFile, "\n");
       va_end(args);
   }
}

static void errmsg(char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);
   vfprintf(stderr, fmt, args);
   va_end(args);

   if (WSStatsFile != NULL) {
       va_start(args, fmt);
       vfprintf(WSStatsFile, fmt, args);
       va_end(args);
   }
}

static void check_err(FILE *f, char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);
   fprintf(f, "CHECK ERROR: ");
   vfprintf(f, fmt, args);
   fprintf(f, "\n");
   va_end(args);

   if (WSStatsFile != NULL) {
       va_start(args, fmt);
       fprintf(WSStatsFile, "CHECK ERROR: ");
       vfprintf(WSStatsFile, fmt, args);
       fprintf(WSStatsFile, "\n");
       va_end(args);
   }
}

static void check_info(FILE *f, char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);
   fprintf(f, "CHECK INFO: ");
   vfprintf(f, fmt, args);
   fprintf(f, "\n");
   va_end(args);

   if (WSStatsFile != NULL) {
       va_start(args, fmt);
       fprintf(WSStatsFile, "CHECK INFO: ");
       vfprintf(WSStatsFile, fmt, args);
       fprintf(WSStatsFile, "\n");
       va_end(args);
   }
}

static void panicmsg(char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);
   fprintf(stderr, "[%ld]>PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC\n", get_tid());
   vfprintf(stderr, fmt, args);
   fprintf(stderr, "\n");
   fprintf(stderr, "PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC\n");
   va_end(args);

   if (WSStatsFile != NULL) {
       va_start(args, fmt);
       fprintf(WSStatsFile, "[%ld]>PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC\n", get_tid());
       vfprintf(WSStatsFile, fmt, args);
       fprintf(WSStatsFile, "\n");
       fprintf(WSStatsFile, "PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC\n");
       va_end(args);
   }

}

