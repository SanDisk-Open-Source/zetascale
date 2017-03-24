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
 *  destage.c   11/29/16   Brian O'Krafka   
 *
 *  Code to initialize Write serializer module for ZetaScale b-tree.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 *  Notes:
 *
 **********************************************************************/

// xxxzzz where is latd_write/create_start?

#define DESTAGE_C

#include <time.h>
#include <stdarg.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include "threadpool_internal.h"
#include "wsstats.h"
#include "mbox.h"
#include "latd_internal.h"
#include "destage_internal.h"
#include "btree/btree.h"

//   Disable platform printf stuff!
#ifdef fprintf
#undef fprintf
#endif
#ifdef vfprintf
#undef vfprintf
#endif

#define N_LATD_BUCKETS     1024
#define N_LATD_FREE_LISTS  16

//  imported from btree/fdf_wrapper.c
extern __thread struct ZS_thread_state *my_thd_state;
extern ZS_status_t BtreeErr_to_ZSErr(btree_status_t b_status);

//  Allocated and used by threads in writer pool
__thread destage_buf_t  *DestageBuf = NULL;

static int SectorSize = 512;

struct destager *pDestager = NULL;

static int   DSReadOnly  = 0;
static int   DSTraceOn   = 0;
//  File to capture stats, trace and debug messages
static FILE *DSStatsFile = NULL;

static void go_to_readonly()
{
    int   x = 0;
    DSReadOnly = 1;
    DSReadOnly = 1/x; //  xxxzzz remove this
}

#define DOSTAT(pds, nstat, val) \
{\
    (void) __sync_fetch_and_add(&((pds)->stats.stats.ds_raw_stat[nstat]), val);\
}

//  Print various kinds of messages
static void tracemsg(char *fmt, ...);
static void infomsg(char *fmt, ...);
static void errmsg(char *fmt, ...);
static void panicmsg(char *fmt, ...);
static void check_info(FILE *f, char *fmt, ...);
static void check_err(FILE *f, char *fmt, ...);

static uint64_t do_checksum64(destager_t *pds, char *d1, uint32_t l1, char *d2, uint32_t l2, char *d3, uint32_t l3);
static void load_env_variables(destager_t *ps, destager_config_t *cfg);
static void *writer_thread(threadpool_argstuff_t *as);
static void *batch_thread(threadpool_argstuff_t *as);
static void *stats_thread(threadpool_argstuff_t *as);
static int txn_op(struct destager *pds, struct ZS_thread_state *pzst, ds_ops_t op, uint32_t flags);
static void compute_derived_stats(struct destager *ps, ds_substats_t *pstats_ref, ds_substats_t *pstats_now, ds_substats_t *pstats_out, double secs, double user_hz);
static void copy_substats(ds_substats_t *pto, ds_substats_t *pfrom);
static void init_stats(struct destager *ps);

/*  For remembering my thread-id to minimize 
 *  calls to syscall(SYS_gettid).
 */
__thread long              DSThreadId = 0;

static long ds_get_tid()
{
    if (DSThreadId == 0) {
        DSThreadId = syscall(SYS_gettid);
    }
    return(DSThreadId);
}

#define TRACE(pds, fmt, args...) \
    if ((pds)->config.trace_on) { \
        tracemsg(fmt, ##args); \
    }

/***********************************************************************
 *
 *    ========================
 *    ordering issues:
 *    ========================
 *    
 *    - on writes:
 *        - at enqueue: create_start (must-not-exist), create_end 
 *            - write lock is held until create_end
 *        - at dequeue (write is complete and durable):
 *            - write_start, release buffer, write_end_and_delete
 *            - write lock is held until end_and_delete 
 *        - should not have to hold locks because app must enforce isolation
 *        - "must-not-exist" check will detect isolation violation
 *    
 *    - transactions:
 *        - requests from a particular therad must all go to the same
 *          write handler thread (ZS assumes this for transaction processing)
 *            - this also enforces proper write ordering per client thread
 *    
 *    - on reads:
 *        - read_start
 *        - if hit:
 *            - get data from buffer
 *    	- read_end
 *    	- read lock is held until read_end
 *        - if miss:
 *            - get data from ZS
 *    	- no read lock held after call to read_start with a miss
 *    
 *    - with write destaging, can lower-level ZS logging be eliminated?
 *        - requires txn start/commit entries in destaging buffer
 *        - how about collecting entire txn in destage buffer before
 *          proceeding?
 *            - still need lower level txn's for btree
 *    
 *    - multi-instance for performance:
 *        - problem: txn's across instances!
 *    
 *    - range query:
 *        - xxxzzz TBD
 *
 ***********************************************************************/

static int check_config(destager_config_t *cfg)
{
    if ((cfg->batch_buf_kb*1024) % SectorSize) {
        errmsg("check_config: batch_buf_kb=%d must be a multiple of sector size (%d)\n", cfg->batch_buf_kb*1024, SectorSize);
	return(1);
    }
    return(0);
}

struct destager *destage_init(struct ZS_state *pzs, destager_config_t *config)
{
    int              i;
    struct destager *pds;

    pds = (struct destager *) malloc(sizeof(struct destager));
    if (pds == NULL) {
        errmsg("destage_init() could not allocate state");
        return(NULL);
    }
    pDestager = pds;
    pds->pzs  = pzs;
    init_stats(pds);

    /*   See if any configuration values are overridden by 
     *   environment variables.
     */
    load_env_variables(pds, config);
    
    if (check_config(config) != 0) {
        return(NULL);
    }
    memcpy((void *) &(pds->config), (void *) config, sizeof(destager_config_t));
    DSTraceOn = pds->config.trace_on;

    //  Open stats file, if required.
    if (config->stats_file[0] != '\0') {
	pds->f_stats = fopen(config->stats_file, "w");
	if (pds->f_stats == NULL) {
	    errmsg("Could not open stats file '%s'", config->stats_file);
	}
    } else {
	pds->f_stats = NULL;
    }
    DSStatsFile = pds->f_stats;

    destage_dump_config(stderr, pds);

    pds->master_seqno = 1;

    mboxInit(&(pds->batch_mbox));
    mboxInit(&(pds->batch_bufs_mbox));

    pds->async_mbox = (mbox_t *) malloc(config->pool_size*sizeof(mbox_t));
    if (pds->async_mbox == NULL) {
        errmsg("destage_init() could not allocate async mbox's");
        return(NULL);
    }
    for (i=0; i<config->pool_size; i++) {
	mboxInit(&(pds->async_mbox[i]));
    }

    /*  Set up array of batch states */
    pds->batch_states = (batch_state_t *) malloc(pds->config.n_batch_bufs*sizeof(batch_state_t));
    if (pds->batch_states == NULL) {
        errmsg("destage_init() could not allocate batch_states array!");
        return(NULL);
    }

    pds->writer_pool = tp_init(config->pool_size, writer_thread, pds);
    if (pds->writer_pool == NULL) {
        free(pds);
        errmsg("destage_init() could not create writer threadpool");
        return(NULL);
    }

    pds->batch_pool = tp_init(1, batch_thread, pds);
    if (pds->batch_pool == NULL) {
        free(pds);
        errmsg("destage_init() could not create batch threadpool");
        return(NULL);
    }

    pds->stats_pool = tp_init(1, stats_thread, pds);
    if (pds->stats_pool == NULL) {
        free(pds);
        errmsg("destage_init() could not create stats threadpool");
        return(NULL);
    }

    pds->lat = latd_init(N_LATD_BUCKETS, sizeof(latd_data_t), N_LATD_FREE_LISTS);
    if (pds->lat == NULL) {
        errmsg("destage_init(): Could not initialize the write serialization look-aside table");
        return(NULL);
    }

    if (pds->config.batch_commit) {
        if (pds->config.batch_file == NULL) {
	    free(pds);
	    errmsg("destage_init() batch file must be specified if batch commit is enabled");
	    return(NULL);
	} else {
	    pds->fd_batch = open(pds->config.batch_file, O_WRONLY);
	    if (pds->fd_batch == 0) {
		free(pds);
		errmsg("destage_init() could not open batch file '%s'", pds->config.batch_file);
		return(NULL);
	    }
	}
    }

    return(pds);
}

void destage_destroy(struct destager *pds)
{
    tp_shutdown(pds->writer_pool);
    tp_shutdown(pds->batch_pool);
    tp_shutdown(pds->stats_pool);
    free(pds);
}

int destage_read(struct destager *pds, struct ZS_thread_state *pzst, uint64_t cguid, struct btree *bt, char *key, uint32_t key_len, void *pdata, uint64_t *psize, uint32_t flags)
{
    latd_data_t   *pld;
    latd_entry_t   lat_entry;
    int            do_zs_read;
    destage_buf_t *dsbuf;
    uint64_t       size;
    void          *pfrom;

    DOSTAT(pds, N_POINT_READ, 1);
    lat_entry = latd_read_start(pds->lat, cguid, key, key_len);
    if (lat_entry.latd_handle == NULL) {
	do_zs_read = 1;
	TRACE(pds, "destage_read: miss in Look-Aside Table: key=%"PRIu64"", key);
        DOSTAT(pds, N_LATD_MISSES, 1);
    } else {
        DOSTAT(pds, N_LATD_HITS, 1);
	pld   = (latd_data_t *) lat_entry.pdata;
	dsbuf    = pld->dsbuf;
	TRACE(pds, "destage_read: hit in Look-Aside Table: key=%"PRIu64", size=%d", key, dsbuf->size);
	// copy data from sbuf
	pfrom = (void *) dsbuf->buf;
	(void) memcpy(pdata, pfrom, dsbuf->size);
	size = dsbuf->size;
	do_zs_read = 0;
    }

    if (do_zs_read) {
	*psize = 0;
	return(1);
    } else {
	latd_read_end(lat_entry.latd_handle);
	*psize = size;
	wsrt_record_value(size, &(pds->stats.read_pt_bytes));
	DOSTAT(pds, POINT_READ_BYTES, size);
	return(0);
    }
}

static __thread int       do_mbox_init = 1;
static __thread mbox_t    MboxAck;

int destage_write(struct destager *pds, struct ZS_thread_state *pzst, uint64_t cguid, struct btree *bt, char *key, uint32_t key_len, void *pdata, uint64_t size, uint32_t flags)
{
    uint64_t       rc = 0;
    mbx_entry_t    mbe;
    long           tid;
    int            n_dest;

    DOSTAT(pds, N_WRITE, 1);
    wsrt_record_value(size, &(pds->stats.write_bytes));
    DOSTAT(pds, WRITE_BYTES, size);

    if (do_mbox_init) {
        do_mbox_init = 0;
        mboxInit(&MboxAck);
    }

    mbe.op              = Write;
    mbe.btree           = bt;
    mbe.flags           = flags;
    mbe.key             = key;
    mbe.key_len         = key_len;
    mbe.pdata           = pdata;
    mbe.size            = size;
    mbe.client_ack_mbox = &MboxAck;
    mbe.n_batch_state   = -1;

    if (pds->config.batch_commit) {
        
	//  Put request in batch request queue

	mboxPost(&(pds->batch_mbox), (uint64_t) &mbe);
	rc = mboxWait(&MboxAck);
    } else {
        
	//  Put request in non-batch request queue

        tid    = ds_get_tid();
	n_dest = wshash((unsigned char *) &tid, sizeof(long), 0) % pds->config.pool_size;
	mboxPost(&(pds->async_mbox[n_dest]), (uint64_t) &mbe);
	rc = mboxWait(&MboxAck);
    }
    return(rc);
}

int destage_delete(struct destager *pds, struct ZS_thread_state *pzst, uint64_t cguid, struct btree *bt, char *key, uint32_t key_len, uint32_t flags)
{
    uint64_t       rc;
    mbx_entry_t    mbe;
    long           tid;
    int            n_dest;

    DOSTAT(pds, N_DELETE, 1);

    if (do_mbox_init) {
        do_mbox_init = 0;
        mboxInit(&MboxAck);
    }

    mbe.op              = Delete;
    mbe.btree           = bt;
    mbe.flags           = flags;
    mbe.key             = key;
    mbe.key_len         = key_len;
    mbe.pdata           = NULL;
    mbe.size            = 0;
    mbe.client_ack_mbox = &MboxAck;
    mbe.n_batch_state   = -1;

    if (pds->config.batch_commit) {
        
	//  Put request in batch request queue

	mboxPost(&(pds->batch_mbox), (uint64_t) &mbe);
	rc = mboxWait(&MboxAck);
    } else {
        
	//  Put request in non-batch request queue

        tid    = ds_get_tid();
	n_dest = wshash((unsigned char *) &tid, sizeof(long), 0) % pds->config.pool_size;
	mboxPost(&(pds->async_mbox[n_dest]), (uint64_t) &mbe);
	rc = mboxWait(&MboxAck);
    }
    return(rc);
}

int destage_txn_start(struct destager *pds, struct ZS_thread_state *pzst, uint32_t flags)
{
   DOSTAT(pds, N_TXN_START, 1);
   return(txn_op(pds, pzst, TxnStart, flags));
}

int destage_txn_commit(struct destager *pds, struct ZS_thread_state *pzst, uint32_t flags)
{
   DOSTAT(pds, N_TXN_COMMIT, 1);
   return(txn_op(pds, pzst, TxnCommit, flags));
}

int destage_txn_abort(struct destager *pds, struct ZS_thread_state *pzst, uint32_t flags)
{
   DOSTAT(pds, N_TXN_ABORT, 1);
   return(txn_op(pds, pzst, TxnAbort, flags));
}

static int txn_op(struct destager *pds, struct ZS_thread_state *pzst, ds_ops_t op, uint32_t flags)
{
    uint64_t       rc;
    mbx_entry_t    mbe;
    long           tid;
    int            n_dest;

    DOSTAT(pds, N_WRITE, 1);

    if (do_mbox_init) {
        do_mbox_init = 0;
        mboxInit(&MboxAck);
    }

    mbe.op              = op;
    mbe.flags           = flags;
    mbe.key             = NULL;
    mbe.key_len         = 0;
    mbe.pdata           = NULL;
    mbe.size            = 0;
    mbe.client_ack_mbox = &MboxAck;
    mbe.n_batch_state   = -1;

    if (pds->config.batch_commit) {
        
	//  Put request in batch request queue

	mboxPost(&(pds->batch_mbox), (uint64_t) &mbe);
	rc = mboxWait(&MboxAck);
    } else {
        
	//  Put request in non-batch request queue

        tid    = ds_get_tid();
	n_dest = wshash((unsigned char *) &tid, sizeof(long), 0) % pds->config.pool_size;
	mboxPost(&(pds->async_mbox[n_dest]), (uint64_t) &mbe);
	rc = mboxWait(&MboxAck);
    }
    return(rc);
}

static void dump_mail_mbx_entry(FILE *f, int n, uint64_t mail, void *pstuff)
{
    mbx_entry_t *mbe = (mbx_entry_t *) mail;

    fprintf(f, "   %d) OP=%s, key='%s', n_batch_state=%d, seqno=%"PRIu64", key_len=%d, pdata=%p, size=%"PRIu64", client_ack=%p, batch_ack=%p\n", n, DSOpString(mbe->op), mbe->key, mbe->n_batch_state, mbe->seqno, mbe->key_len, mbe->pdata, mbe->size, mbe->client_ack_mbox, mbe->batch_ack_mbox);
}

static void dump_mail_batch_buf(FILE *f, int n, uint64_t mail, void *pstuff)
{
    fprintf(f, "   %d) n_batch_state=%"PRIu64"\n", n, mail);
}

static void dump_writer_tp(FILE *f, int n, void *pstuff)
{
    destage_buf_t   *pdb = DestageBuf;

    if (pdb != NULL) {
        if (pdb->state == Free) {
	    fprintf(f, "%d) FREE\n", n);
	} else {
	    fprintf(f, "%d) %s: op=%s, cguid=%"PRIu64", flags=%d, checksum=%"PRIu64", seqno=%"PRIu64", key='%s', key_len=%d, size=%"PRIu64", n_batch_state=%d\n", n, DSBufStateString(pdb->state), DSOpString(pdb->op), pdb->cguid, pdb->flags, pdb->checksum, pdb->seqno, pdb->key, pdb->key_len, pdb->size, pdb->n_batch_state);
	}
    }
}

void destage_dump(FILE *f, struct destager *pds)
{
    int             i, j;
    void           *pbuf_malloc;
    void           *pbuf;
    void           *p;
    md_entry_t     *md0, *md;
    uint64_t        checksum, checksum2, total_size;
    batch_state_t  *pbs;
    ssize_t         ssize;

    pbuf_malloc = malloc((pds->config.batch_buf_kb*1024) + SectorSize);
    pbuf = pbuf_malloc + (SectorSize - (((uint64_t) pbuf_malloc) % SectorSize)); // sector align

    destage_dump_stats(f, NULL, pds);
    destage_dump_config(f, pds);

    fprintf(f, "master_seqno = %"PRIu64"\n", pds->master_seqno);
    fprintf(f, "user_hz = %d\n", pds->user_hz);
    fprintf(f, "fd_batch = %d\n", pds->fd_batch);

    fprintf(f, "Batch mbox:\n");
    mboxDump(&(pds->batch_mbox), f, dump_mail_mbx_entry, NULL);
    fprintf(f, "\n");

    for (j=0; j<pds->config.pool_size; j++) {
	fprintf(f, "Async mbox[%d]:\n", j);
	mboxDump(&(pds->async_mbox[j]), f, dump_mail_mbx_entry, NULL);
    }
    fprintf(f, "\n");

    fprintf(f, "Batch Bufs mbox:\n");
    mboxDump(&(pds->batch_mbox), f, dump_mail_batch_buf, NULL);
    fprintf(f, "\n");

    fprintf(f, "Batch State Array:\n");
    for (i=0; i<pds->config.n_batch_bufs; i++) {
        pbs = &(pds->batch_states[i]);
        if (pbs->batch_size != 0) {
	    fprintf(f, "   %d) i=%d, done_cnt=%"PRIu64", batch_size=%d, offset=%"PRIu64"\n", i, pbs->i, pbs->done_cnt, pbs->batch_size, pbs->offset);

            ssize = pread(pds->fd_batch, pbuf, pds->config.batch_buf_kb*1024, pbs->offset);
	    if (ssize != (pds->config.batch_buf_kb*1024)) {
	        errmsg("pread failed in destage_dump: %lld bytes read, but %lld expected\n", ssize, pds->config.batch_buf_kb*1024);
	    } else {
	        md0 = (md_entry_t *) pbuf;
		p   = pbuf;
	        for (j=0; j<md0->batch_size; j++) {
		    md = (md_entry_t *) p;
		    fprintf(f, "      %d) op=%s, key='%s', key_len=%d, seqno=%"PRIu64", size=%"PRIu64", checksum=%"PRIu64"\n", j, DSOpString(md->op), md->key, md->key_len, md->seqno, md->size, md->checksum);
		    p += sizeof(md_entry_t);
		    p += md->size;
		    p += (4 - (((uint64_t) p) % 4)); // align p
		}

		// check checksum
		checksum      = md0->checksum;
		md0->checksum = 0;
		total_size    = (p - pbuf);
		total_size += (SectorSize- (((uint64_t) p) % SectorSize)); // make total_size a multiple of sector size
		checksum2 = do_checksum64(pds, ((char *) pbuf), total_size, NULL, 0, NULL, 0);
		if (checksum2 != checksum) {
		    fprintf(f, "=====================================================================================================\n");
		    fprintf(f, "============>   CHECKSUM MISMATCH! (%"PRIu64" found vs %"PRIu64" computed)   <============\n", checksum, checksum2);
		    fprintf(f, "=====================================================================================================\n");
		}
	    }
	}
    }
    fprintf(f, "\n");

    // dump destage buffers
    tp_dump(f, pds->writer_pool, dump_writer_tp, NULL);
    fprintf(f, "\n");

    latd_dump(f, pds->lat);

    free(pbuf_malloc);
}

int destage_check(FILE *f, struct destager *pds)
{
    check_info(f, "destage_check() is not yet implemented\n");
    check_err(f, "I repeat, destage_check() is not yet implemented\n");
    return(1);
}

// there should just be a pool of 1 or more of these:
static void *writer_thread(threadpool_argstuff_t *as)
{
    mbx_entry_t              *mbe;
    int                       my_id;
    uint64_t                  old_done_cnt;
    btree_status_t            btree_ret;
    btree_metadata_t          meta;
    destager_t               *pds;
    destage_buf_t            *pdb;
    ZS_status_t               status;
    struct ZS_thread_state   *pzst;
    batch_state_t            *pbs;
    int                       rc;

    pds = (destager_t *) as->pdata;
    my_id = as->myid;

    rc = pds->config.per_thread_cb(pds->pzs, &pzst);
    if (rc != 0) {
	panicmsg("Failure in writer_thread(%d): could not allocate per-thread state; going to read-only!", my_id);
	go_to_readonly();
	pzst = NULL;
    }

    TRACE(pds, "writer_thread %d started (tid=%ld)", my_id, ds_get_tid());

    if (DestageBuf == NULL) {
        DestageBuf = (destage_buf_t *) malloc(sizeof(destage_buf_t));
	assert(DestageBuf);
    }

    while (1) {

	mbe = (mbx_entry_t *) mboxWait(&(pds->async_mbox[my_id]));

        // Make copy of data (in persistent memory if required)
	DestageBuf->seqno         = __sync_fetch_and_add(&(pds->master_seqno), 1);
	DestageBuf->state         = WriteQueued;
	DestageBuf->op            = mbe->op;
	DestageBuf->btree         = mbe->btree;
	DestageBuf->key_len       = mbe->key_len;
	DestageBuf->size          = mbe->size;
	DestageBuf->cguid         = mbe->cguid;
	DestageBuf->flags         = mbe->flags;
	DestageBuf->checksum      = 0;
	DestageBuf->n_batch_state = mbe->n_batch_state;
	memcpy(DestageBuf->key, mbe->key, mbe->key_len);
	memcpy(DestageBuf->buf, mbe->pdata, mbe->size);
	DestageBuf->checksum  = do_checksum64(pds, ((char *) DestageBuf), sizeof(DestageBuf), NULL, 0, NULL, 0);

	// Once data is copied, let client proceed
	mboxPost(mbe->client_ack_mbox, 0);

	pdb = DestageBuf;

        if (!DSReadOnly) {
	    // Do ZetaScale operation
	    my_thd_state = pzst;
	    switch (DestageBuf->op) {
	        case TxnStart: 
		    status = ZSTransactionService(pzst, 0, 0);
		    TRACE(pds, "writer_thread (%ld): TxnStart(%d) returned status %d ('%s')", ds_get_tid(), DestageBuf->op, status, ZSStrError(status));
		    break;
	        case Write: 
		    meta.flags = 0;
		    if (pdb->flags & ZS_WRITE_MUST_NOT_EXIST) {
			btree_ret = btree_insert(pdb->btree, pdb->key, pdb->key_len, pdb->buf, pdb->size, &meta);
			status = BtreeErr_to_ZSErr(btree_ret);
			TRACE(pds, "writer_thread (%ld): Write MUST_NOT_EXIST (%d) returned status %d ('%s'), btree_ret=%d", ds_get_tid(), DestageBuf->op, status, ZSStrError(status), btree_ret);
		    } else if (pdb->flags & ZS_WRITE_MUST_EXIST) {
			btree_ret = btree_update(pdb->btree, pdb->key, pdb->key_len, pdb->buf, pdb->size, &meta);
			status = BtreeErr_to_ZSErr(btree_ret);
			TRACE(pds, "writer_thread (%ld): Write MUST_EXIST (%d) returned status %d ('%s'), btree_ret=%d", ds_get_tid(), DestageBuf->op, status, ZSStrError(status), btree_ret);
		    } else if (pdb->flags & ZS_WRITE_TRIM) {
			status = ZS_FAILURE_OPERATION_DISALLOWED;
			TRACE(pds, "writer_thread (%ld): Write ZS_WRITE_TRIM (%d) returned status %d ('%s')", ds_get_tid(), DestageBuf->op, status, ZSStrError(status));
		    } else {
			btree_ret = btree_set(pdb->btree, pdb->key, pdb->key_len, pdb->buf, pdb->size, &meta);
			status = BtreeErr_to_ZSErr(btree_ret);
			TRACE(pds, "writer_thread (%ld): Write set (%d) returned status %d ('%s'), btree_ret=%d", ds_get_tid(), DestageBuf->op, status, ZSStrError(status), btree_ret);
		    }
		    break;
	        case Delete: 
		    meta.flags = 0;
		    btree_ret = btree_delete(pdb->btree, pdb->key, pdb->key_len, &meta);
		    status = BtreeErr_to_ZSErr(btree_ret);
		    TRACE(pds, "writer_thread (%ld): Delete (%d) returned status %d ('%s'), btree_ret=%d", ds_get_tid(), DestageBuf->op, status, ZSStrError(status), btree_ret);
		    break;
	        case TxnCommit: 
		    status = ZSTransactionService(pzst, 1, 0);
		    TRACE(pds, "writer_thread (%ld): TxnCommit (%d) returned status %d ('%s')", ds_get_tid(), DestageBuf->op, status, ZSStrError(status));
		    break;
	        case TxnAbort: 
		    status = ZS_FAILURE_OPERATION_DISALLOWED;
		    TRACE(pds, "writer_thread (%ld): TxnAbort (%d) returned status %d ('%s')", ds_get_tid(), DestageBuf->op, status, ZSStrError(status));
		    break;
		default : 
		    status = ZS_FAILURE;
		    TRACE(pds, "writer_thread (%ld): INVALID OP (%d) returned status %d ('%s')", ds_get_tid(), DestageBuf->op, status, ZSStrError(status));
		    break;
	    }
	    if (status != ZS_SUCCESS) {
		panicmsg("writer_thread: op=%d (%s) failed with status %d (%s)", DestageBuf->op, DSOpString(DestageBuf->op), status, ZSStrError(status));
		go_to_readonly();
	    }
	}

	if (DestageBuf->n_batch_state != -1) {
	    // Once operation is durable, let batch thread proceed
	    pbs = &(pds->batch_states[DestageBuf->n_batch_state]);

	    old_done_cnt = __sync_fetch_and_add(&(pbs->done_cnt), 1);
	    if (old_done_cnt == (pbs->batch_size-1)) {
	        pbs->batch_size = 0;
	        //   This batch is finished, so the batch state and buffer can be reused.
		mboxPost(&(pds->batch_bufs_mbox), (uint64_t) DestageBuf->n_batch_state);
	    }
	}


	// Note that buffer is now free
	DestageBuf->state = Free;
    }
}

#define MAX_MAILS  1024

// there should just be one of these:
static void *batch_thread(threadpool_argstuff_t *as)
{
    int                  i;
    mbx_entry_t         *mbe;
    uint64_t             mails[MAX_MAILS];
    struct timespec      ts, ts_rem;
    struct iovec         iov[MAX_MAILS];
    ssize_t              ssize;
    md_entry_t          *metadata;
    uint64_t             offset;
    batch_state_t       *pbs;
    uint32_t             total_size;
    uint32_t             n_pbs;
    void                *pbuf;
    void                *p;
    void                *p_old;
    destager_t          *pds;
    uint64_t             batch_size;
    int                  n_iov;
    long                 tid;
    uint32_t             n_dest;

    pds = (destager_t *) as->pdata;

    pbuf = malloc(pds->config.batch_buf_kb*1024 + SectorSize);
    pbuf += (SectorSize - (((uint64_t) pbuf) % SectorSize)); // sector align

    TRACE(pds, "batch_thread started (id=%ld)", ds_get_tid());

    ts.tv_sec  = pds->config.batch_usecs/1000000;
    ts.tv_nsec = (pds->config.batch_usecs % 1000000)*1000;

    /*  Set up pool of buffers on batch storage device */

    offset = 0;
    for (i=0; i<pds->config.n_batch_bufs; i++) {
	pds->batch_states[i].offset     = offset;
	pds->batch_states[i].i          = i;
	pds->batch_states[i].batch_size = 0;
	pds->batch_states[i].done_cnt   = 0;
	mboxPost(&(pds->batch_bufs_mbox), (uint64_t) i);
        offset += (pds->config.batch_buf_kb*1024);
    }

    n_pbs = mboxWait(&(pds->batch_bufs_mbox));
    pbs = &(pds->batch_states[n_pbs]);
    while (1) {

        batch_size = mboxWaitBatch(&(pds->batch_mbox), mails, MAX_MAILS);

        total_size = 0;
	n_iov      = 0;
	p          = pbuf;

	for (i=0; i<batch_size; i++) {
	    p_old = p;
	    mbe = (mbx_entry_t *) mails[i];
	    TRACE(pds, "batch_thread received write[%d]: key=%s, key_len=%d, pdata=%lld, size=%lld", i, mbe->key, mbe->key_len, mbe->pdata, mbe->size);

            metadata              = (md_entry_t *) p;
	    metadata->batch_size  = batch_size;
	    metadata->key_len     = mbe->key_len;
	    metadata->size        = mbe->size;
	    metadata->op          = mbe->op;
	    metadata->seqno       = mbe->seqno;
	    metadata->checksum    = 0;
	    memcpy(metadata->key, mbe->key, mbe->key_len);
	    memcpy(p, mbe->pdata, mbe->size);

	    p += sizeof(md_entry_t);
	    p += mbe->size;
	    p += (4 - (((uint64_t) p) % 4)); // align p

	    total_size += (p - p_old);

            if (!DSReadOnly) {
		tid    = ds_get_tid();
		n_dest = wshash((unsigned char *) &tid, sizeof(long), 0) % pds->config.pool_size;
		mbe->n_batch_state = pbs->i;
		mboxPost(&(pds->async_mbox[n_dest]), (uint64_t) mbe);
	    }
	}

        if (DSReadOnly) {
	    continue;
	}

	if (total_size > (pds->config.batch_buf_kb*1024)) {
	    panicmsg("batch data size (%d) exceeded batch buffer size (%d)!", total_size, pds->config.batch_buf_kb*1024);
	    go_to_readonly();
	    continue;
	}

	TRACE(pds, "batch_thread writing %d iovec's, %lld bytes total", batch_size, total_size);

	wsrt_record_value(batch_size, &(pds->stats.batch_counts));
	wsrt_record_value(total_size, &(pds->stats.batch_bytes));

	metadata           = (md_entry_t *) pbuf;

        // do checksum for entire buffer and stash in first metadata entry
	metadata->checksum = do_checksum64(pds, ((char *) pbuf), total_size, NULL, 0, NULL, 0);

	total_size += (SectorSize - (((uint64_t) p) % SectorSize)); // make total_size a multiple of sector size
	iov[n_iov].iov_base = pbuf;
	iov[n_iov].iov_len  = (size_t) total_size;
	n_iov++;

        pbs->batch_size = batch_size;
	pbs->done_cnt   = 0;
        ssize = pwritev(pds->fd_batch, iov, n_iov, pbs->offset);

	if (ssize != total_size) {
	    panicmsg("batch_thread: pwritev wrote %d bytes, but %lld expected", ssize, total_size);
	    go_to_readonly();
	}

	//  get next free buffer
	n_pbs = mboxWait(&(pds->batch_bufs_mbox));
	pbs   = &(pds->batch_states[n_pbs]);

	if (batch_size < MAX_MAILS) {
	    // allow a batch of writes to accumulate
	    //  xxxzzz should I care about the return value here?
	    (void) nanosleep(&ts, &ts_rem);
	}
    }

    return(NULL);
}

static uint64_t do_checksum64(destager_t *pds, char *d1, uint32_t l1, char *d2, uint32_t l2, char *d3, uint32_t l3)
{
    uint64_t    checksum;

    //  xxxzzz is there something better than this?
    checksum = wshash((unsigned char *) d1, l1, 0);
    if (d2 != NULL) {
	checksum = wshash((unsigned char *) d2, l2, checksum);
	if (d3 != NULL) {
	    checksum = wshash((unsigned char *) d3, l3, checksum);
	}
    }

    return(checksum);
}

  /*    Load default configuration parameters.
   */
void ds_load_default_config(destager_config_t *cfg)
{
    cfg->pool_size               = 32;
    cfg->batch_commit            = 0;
    cfg->trace_on                = 0;
    cfg->batch_usecs             = 1000;
    cfg->n_batch_bufs            = 100;
    cfg->batch_buf_kb            = 256;
    strcpy(cfg->batch_file, "");
    strcpy(cfg->stats_file, "ds_stats.out");
    cfg->stats_steady_state_secs = 30;
    cfg->stats_secs              = 10;
    cfg->per_thread_cb           = NULL;
    cfg->client_ops_cb           = NULL;
    cfg->cb_state                = NULL;
}

static void load_env_variables(destager_t *ps, destager_config_t *cfg)
{
    uint32_t     i32;
    char        *s;

    /*  See if these hardcoded defaults have been overridden
     *  by environment variables.
     */

    s = getenv("DS_POOL_SIZE");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 <= 0) || (i32 > 512)) {
	    errmsg("DS_POOL_SIZE=%d is out of range, reverting to default of %d \n", i32, cfg->pool_size);
	} else {
	    cfg->pool_size = i32;
	}
    }

    s = getenv("DS_BATCH_COMMIT");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 <= 0) || (i32 > 1)) {
	    errmsg("DS_BATCH_COMMIT=%d must be 0 or 1, reverting to default of %d \n", i32, cfg->batch_commit);
	} else {
	    cfg->batch_commit = i32;
	}
    }

    s = getenv("DS_TRACE_ON");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 < 0) || (i32 > 1)) {
	    errmsg("DS_TRACE_ON=%d must be 0 or 1, reverting to default of %d \n", i32, cfg->trace_on);
	} else {
	    cfg->trace_on = i32;
	}
    }

    s = getenv("DS_BATCH_USECS");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 <= 0) || (i32 > 1000000000)) {
	    errmsg("DS_BATCH_USECS=%d is out of range, reverting to default of %d \n", i32, cfg->batch_usecs);
	} else {
	    cfg->batch_usecs = i32;
	}
    }

    s = getenv("DS_N_BATCH_BUFS");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 <= 0) || (i32 > 1000)) {
	    errmsg("DS_N_BATCH_BUFS=%d is out of range, reverting to default of %d \n", i32, cfg->n_batch_bufs);
	} else {
	    cfg->n_batch_bufs = i32;
	}
    }

    s = getenv("DS_BATCH_BUF_KB");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 <= 0) || (i32 > 1000000)) {
	    errmsg("DS_BATCH_BUF_KB=%d is out of range, reverting to default of %d \n", i32, cfg->batch_buf_kb);
	} else {
	    cfg->batch_buf_kb = i32;
	}
    }

    s = getenv("DS_BATCH_FILE");
    if (s != NULL) {
        (void) strncpy(cfg->batch_file, s, MAX_FILENAME_LEN-1);
	cfg->batch_file[MAX_FILENAME_LEN-1] = '\0';
    }

    s = getenv("DS_STATS_FILE");
    if (s != NULL) {
        (void) strncpy(cfg->stats_file, s, MAX_FILENAME_LEN-1);
	cfg->stats_file[MAX_FILENAME_LEN-1] = '\0';
    }

    s = getenv("DS_STEADY_STATE_SECS");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 < 0) || (i32 > 1000000000)) {
	    errmsg("DS_STEADY_STATE_SECS=%d is out of range, reverting to default of %d \n", i32, cfg->stats_steady_state_secs);
	} else {
	    cfg->stats_steady_state_secs = i32;
	}
    }

    s = getenv("DS_STATS_SECS");
    if (s != NULL) {
	i32 = atoi(s);
	if ((i32 < 0) || (i32 > 1000000000)) {
	    errmsg("DS_STATS_SECS=%d is out of range, reverting to default of %d \n", i32, cfg->stats_secs);
	} else {
	    cfg->stats_secs = i32;
	}
    }
}

  /*    Dump configuration parameters.
   */
void destage_dump_config(FILE *f, struct destager *ps)
{
    destager_config_t    *cfg = &(ps->config);

    fprintf(f, "              pool_size = %d\n",   cfg->pool_size);
    fprintf(f, "           batch_commit = %d\n",   cfg->batch_commit);
    fprintf(f, "               trace_on = %d\n",   cfg->trace_on);
    fprintf(f, "            batch_usecs = %d\n",   cfg->batch_usecs);
    fprintf(f, "           n_batch_bufs = %d\n",   cfg->n_batch_bufs);
    fprintf(f, "           batch_buf_kb = %d\n",   cfg->batch_buf_kb);
    fprintf(f, "             batch_file = '%s'\n", cfg->batch_file);
    fprintf(f, "             stats_file = '%s'\n", cfg->stats_file);
    fprintf(f, "stats_steady_state_secs = %d\n",   cfg->stats_steady_state_secs);
    fprintf(f, "             stats_secs = %d\n",   cfg->stats_secs);
}

/*******************************************************
 * 
 *         Statistics Stuff
 * 
 *******************************************************/

/*   Stats Threadpool
 */

static void *stats_thread(threadpool_argstuff_t *as)
{
    struct timespec           ts, ts_rem;
    destager_config_t        *cfg;
    struct ZS_thread_state   *pzst;
    int                       rc;
    destager_t               *pds;

    pds = (destager_t *) as->pdata;

    cfg = &(pds->config);

    rc = pds->config.per_thread_cb(pds->pzs, &pzst);
    if (rc != 0) {
	panicmsg("Failure in stats_thread: could not allocate per-thread state ");
	pzst = NULL;
    }

    ts.tv_sec  = cfg->stats_secs;
    ts.tv_nsec = 0;

    TRACE(pds, "stats_thread started (id=%ld)", ds_get_tid());

    while (1) {
	TRACE(pds, "stats_thread starting an iteration");

	if (as->ptp->quit) {
	    break;
	}

	//  xxxzzz should I care about the return value here?
	(void) nanosleep(&ts, &ts_rem);

	destage_dump_stats(stderr, pzst, pds);
    }

    TRACE(pds, "stats_thread quitting");
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

static void calc_stats(struct ZS_thread_state *pzs, struct destager *ps)
{
    double      secs_t0, secs_last, secs_overall;
    double      secs_window, secs_ss;
    double      user_hz;
    uint64_t    client_rd, client_wr, client_del;

    user_hz = ps->user_hz;

    if (gettimeofday(&(ps->stats.stats.tspec), NULL) != 0) {
        errmsg("gettimeofday failed in calc_stats with error '%s'", strerror(errno));
    }

    secs_t0      = get_secs(&(ps->stats.stats_t0.tspec));
    secs_last    = get_secs(&(ps->stats.stats_last.tspec));
    secs_overall = get_secs(&(ps->stats.stats.tspec));;
    secs_window  = secs_overall - secs_last;

    client_rd  = 0;
    client_wr  = 0;
    client_del = 0;

    if (pzs != NULL) {
        if (ps->config.client_ops_cb(pzs, ps->config.cb_state, &client_rd, &client_wr, &client_del)) {
	    errmsg("get_raw_system_stats() could not get client ops.");
	}
    }
    ps->stats.stats.ds_raw_stat[N_CLIENT_RD]  = client_rd;
    ps->stats.stats.ds_raw_stat[N_CLIENT_WR]  = client_wr;
    ps->stats.stats.ds_raw_stat[N_CLIENT_DEL] = client_del;

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

static void compute_derived_stats(struct destager *ps, ds_substats_t *pstats_ref, ds_substats_t *pstats_now, ds_substats_t *pstats_out, double secs, double user_hz)
{
    uint64_t   *x;
    double     *xd;

    diff_raw(pstats_out->ds_raw_stat, pstats_now->ds_raw_stat, pstats_ref->ds_raw_stat, N_DS_RAW_STATS);

    x  = pstats_out->ds_raw_stat;
    xd = pstats_out->ds_stat;

    xd[K_CLIENT_RD_RATE]        = do_div((double) x[N_CLIENT_RD], secs)/1000;
    xd[K_CLIENT_WR_RATE]        = do_div((double) x[N_CLIENT_WR], secs)/1000;
    xd[K_CLIENT_DEL_RATE]       = do_div((double) x[N_CLIENT_DEL], secs)/1000;

    xd[K_READ_PT_RATE]          = do_div((double) x[N_POINT_READ], secs)/1000;
    xd[K_READ_RANGE_RATE]       = do_div((double) x[N_RANGE_READ], secs)/1000;
    xd[K_WRITE_RATE]            = do_div((double) x[N_WRITE], secs)/1000;
    xd[K_DELETE_RATE]           = do_div((double) x[N_DELETE], secs)/1000;

    xd[KBYTES_PER_READ_PT]      = do_div((double) x[POINT_READ_BYTES], (double) x[N_POINT_READ])/1024;
    xd[KBYTES_PER_READ_RANGE]   = do_div((double) x[RANGE_READ_BYTES], (double) x[N_RANGE_READ])/1024;
    xd[KBYTES_PER_WRITE]        = do_div((double) x[WRITE_BYTES], (double) x[N_WRITE])/1024;

    xd[K_TXN_START_RATE]        = do_div((double) x[N_TXN_START], secs)/1000;
    xd[K_TXN_COMMIT_RATE]       = do_div((double) x[N_TXN_COMMIT], secs)/1000;
    xd[K_TXN_ABORT_RATE]        = do_div((double) x[N_TXN_ABORT], secs)/1000;

    xd[PCT_LAT_MISS_RATE]        = do_div((double) x[N_LATD_MISSES], (double) x[N_LATD_MISSES] + x[N_LATD_HITS])*100.0;
}

static void init_substats(struct destager *ps, ds_substats_t *pss)
{
    int   i;

    pss->ds_raw_stat = (uint64_t *) malloc(N_DS_RAW_STATS*sizeof(uint64_t));
    assert(pss->ds_raw_stat != NULL);
    for (i=0; i<N_DS_RAW_STATS; i++) {
	pss->ds_raw_stat[i] = 0;
    }

    pss->ds_stat = (double *) malloc(N_DS_STATS*sizeof(double));
    assert(pss->ds_stat != NULL);
    for (i=0; i<N_DS_STATS; i++) {
	pss->ds_stat[i] = 0;
    }
}

static void init_stats(struct destager *ps)
{
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

    copy_substats(&(ps->stats.stats_last), &(ps->stats.stats_t0));

    wsrt_init_stats(&ps->stats.read_pt_bytes,     "Read_Pt_Bytes");
    wsrt_init_stats(&ps->stats.read_range_counts, "Read_Range_Counts");
    wsrt_init_stats(&ps->stats.read_range_bytes,  "Read_Range_Bytes");

    wsrt_init_stats(&ps->stats.write_bytes,       "Write_Bytes");

    wsrt_init_stats(&ps->stats.batch_counts,      "Batch_Counts");
    wsrt_init_stats(&ps->stats.batch_bytes,       "Batch_Bytes");

    ps->user_hz = sysconf(_SC_CLK_TCK); // clock ticks per second (usually 100?)
    infomsg("User Hz = %d\n", ps->user_hz);
}

void destage_stats(struct ZS_thread_state *pzs, struct destager *ps, ds_stats_t *stats)
{
    calc_stats(pzs, ps);
    memcpy((void *) stats, (void *) &(ps->stats), sizeof(ds_stats_t));
    copy_substats(&(stats->stats),         &(ps->stats.stats));
    copy_substats(&(stats->stats_t0),      &(ps->stats.stats_t0));
    copy_substats(&(stats->stats_ss0),     &(ps->stats.stats_ss0));
    copy_substats(&(stats->stats_last),    &(ps->stats.stats_last));
    copy_substats(&(stats->stats_overall), &(ps->stats.stats_overall));
    copy_substats(&(stats->stats_ss),      &(ps->stats.stats_ss));
    copy_substats(&(stats->stats_window),  &(ps->stats.stats_window));
}

static void copy_substats(ds_substats_t *pto, ds_substats_t *pfrom)
{
    int   i;

    pto->tspec = pfrom->tspec;

    for (i=0; i<N_DS_RAW_STATS; i++) {
	pto->ds_raw_stat[i] = pfrom->ds_raw_stat[i];
    }

    for (i=0; i<N_DS_STATS; i++) {
	pto->ds_stat[i] = pfrom->ds_stat[i];
    }
}

static void dump_raw_stats(FILE *f, struct destager *ps, uint32_t nstats, uint64_t *stat_window, uint64_t *stat_ss, uint64_t *stat_overall, const char *(string_fn)(int n))
{
    int    i;

    for (i=0; i<nstats; i++) {
        fprintf(f, "%30s = %12"PRIu64", %12"PRIu64", %12"PRIu64"\n", (string_fn)(i), stat_window[i], stat_ss[i], stat_overall[i]);
        
    }
}

static void dump_nonraw_stats(FILE *f, struct destager *ps, uint32_t nstats, double *stat_window, double *stat_ss, double *stat_overall, const char *(string_fn)(int n))
{
    int    i;

    for (i=0; i<nstats; i++) {
        fprintf(f, "%30s = %12g, %12g, %12g\n", (string_fn)(i), stat_window[i], stat_ss[i], stat_overall[i]);
        
    }
}

static void dump_stats(FILE *f, struct destager *ps)
{
    uint64_t      t;

    t = get_secs(&(ps->stats.stats.tspec)) - get_secs(&(ps->stats.stats_t0.tspec));

    fprintf(f, "\n");
    fprintf(f, "========  Stats at t=%"PRIu64" secs (window, steady-state, overall)  =======\n", t);
    fprintf(f, "\n");
    dump_nonraw_stats(f, ps, N_DS_STATS, ps->stats.stats_window.ds_stat, ps->stats.stats_ss.ds_stat, ps->stats.stats_overall.ds_stat, DSStatString);
    fprintf(f, "\n");

    dump_raw_stats(f, ps, N_DS_RAW_STATS, ps->stats.stats_window.ds_raw_stat, ps->stats.stats_ss.ds_raw_stat, ps->stats.stats_overall.ds_raw_stat, DSRawStatString);
    fprintf(f, "\n");
    
    wsrt_dump_stats(f, &(ps->stats.read_pt_bytes), 1 /* dumpflag */);
    wsrt_dump_stats(f, &(ps->stats.read_range_counts), 1 /* dumpflag */);
    wsrt_dump_stats(f, &(ps->stats.read_range_bytes), 1 /* dumpflag */);
    wsrt_dump_stats(f, &(ps->stats.write_bytes), 1 /* dumpflag */);
    wsrt_dump_stats(f, &(ps->stats.batch_counts), 1 /* dumpflag */);
    wsrt_dump_stats(f, &(ps->stats.batch_bytes), 1 /* dumpflag */);
    fprintf(f, "\n");
}

void destage_dump_stats(FILE *f, struct ZS_thread_state *pzs, struct destager *ps)
{
    calc_stats(pzs, ps);

    dump_stats(f, ps);
    if (DSStatsFile != NULL) {
	dump_stats(DSStatsFile, ps);
    }

    if (fflush(f) != 0) {
        errmsg("fflush failed for 'f' in DSDumpStats with error '%s'", strerror(errno));
    }
    if (DSStatsFile != NULL) {
	if (fflush(DSStatsFile) != 0) {
	    errmsg("fflush failed for 'DSStatsFile' in DSDumpStats with error '%s'", strerror(errno));
	}
    }
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

   if (DSStatsFile != NULL) {
       va_start(args, fmt);
       vfprintf(DSStatsFile, fmt, args);
       va_end(args);
   }
}

static void tracemsg(char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);
   fprintf(stderr, "DS_TRACE[%ld]> ", ds_get_tid());
   vfprintf(stderr, fmt, args);
   fprintf(stderr, "\n");
   va_end(args);

   if (DSStatsFile != NULL) {
       va_start(args, fmt);
       fprintf(DSStatsFile, "DS_TRACE[%ld]> ", ds_get_tid());
       vfprintf(DSStatsFile, fmt, args);
       fprintf(DSStatsFile, "\n");
       va_end(args);
   }
}

static void errmsg(char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);
   vfprintf(stderr, fmt, args);
   va_end(args);

   if (DSStatsFile != NULL) {
       va_start(args, fmt);
       vfprintf(DSStatsFile, fmt, args);
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

   if (DSStatsFile != NULL) {
       va_start(args, fmt);
       fprintf(DSStatsFile, "CHECK ERROR: ");
       vfprintf(DSStatsFile, fmt, args);
       fprintf(DSStatsFile, "\n");
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

   if (DSStatsFile != NULL) {
       va_start(args, fmt);
       fprintf(DSStatsFile, "CHECK INFO: ");
       vfprintf(DSStatsFile, fmt, args);
       fprintf(DSStatsFile, "\n");
       va_end(args);
   }
}

static void panicmsg(char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);
   fprintf(stderr, "[%ld]>PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC\n", ds_get_tid());
   vfprintf(stderr, fmt, args);
   fprintf(stderr, "\n");
   fprintf(stderr, "PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC\n");
   va_end(args);

   if (DSStatsFile != NULL) {
       va_start(args, fmt);
       fprintf(DSStatsFile, "[%ld]>PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC\n", ds_get_tid());
       vfprintf(DSStatsFile, fmt, args);
       fprintf(DSStatsFile, "\n");
       fprintf(DSStatsFile, "PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC  PANIC\n");
       va_end(args);
   }

}

