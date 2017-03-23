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

===============================================================
xxxzzz  move callbacks outside of write serializer package
===============================================================

typedef struct gc_ws_state {
    struct ZS_thread_state    *pzs;
    btree_raw_t               *bt;
    int                        data_in_leaves;
    struct ws_state           *ps;
} gc_ws_state_t;

typedef struct dataptrs {
    uint32_t      n;
    uint32_t      n_in_stripe;
    char        **keybufs;
    uint64_t     *keyptrs;
    uint32_t     *keylens;
    uint32_t     *keydatasizes;
    int          *copyflags;
} dataptrs_t;
            
static __pthread_thread_local dataptrs_t  dataptrs = {0};
static __pthread_thread_local dataptrs_t  dataptrs2 = {0};

static char **alloc_keybufs(gc_ws_state_t *pgc, int n, dataptrs_t *dp)
{
    int    i;
    char **keybufs;
    char  *s;

    dp->n           = n;
    dp->n_in_stripe = 0;

    dp->copyflags = (int *) malloc(n*sizeof(int));
    if (dp->copyflags == NULL) {
        return(NULL);
    }
    dp->keyptrs = (char **) malloc(n*sizeof(uint64_t));
    if (dp->keyptrs == NULL) {
        free(dp->copyflags);
        return(NULL);
    }
    dp->keylens = (char **) malloc(n*sizeof(uint32_t));
    if (dp->keylens == NULL) {
        free(dp->copyflags);
        free(dp->keyptrs);
        return(NULL);
    }
    dp->keydatasizes = (char **) malloc(n*sizeof(uint32_t));
    if (dp->keydatasizes == NULL) {
        free(dp->copyflags);
        free(dp->keyptrs);
        free(dp->keylens);
        return(NULL);
    }
    dp->keybufs = (char **) malloc(n*sizeof(char *));
    if (dp->keybufs == NULL) {
        free(dp->copyflags);
        free(dp->keyptrs);
        free(dp->keylens);
        free(dp->keydatasizes);
        return(NULL);
    }
    s = (char *) malloc(n*pgc->bt->max_key_size*sizeof(char));
    if (s == NULL) {
        free(dp->copyflags);
        free(dp->keyptrs);
        free(dp->keylens);
        free(dp->keydatasizes);
        free(dp->keybufs);
        return(NULL);
    }

    for (i=0; i<n; i++) {
        dp->keybufs[i] = s;
	s += pgc->bt->max_key_size;
    }
    return(dp->keybufs);
}

static int ws_gc_cb(void *state, void *pbuf, uint64_t stripe_bytes, uint32_t sector_bytes, uint64_t stripe_addr)
{
    gc_ws_state_t             *pgc = (gc_ws_state_t *) state;
    uint32_t                   sectors_per_node;
    uint32_t                   sectors_per_stripe;
    uint32_t                   nodesize;
    void                      *p_from, *pma_from, *p_to, *p_from_new;
    int                        rc;

    if (dataptrs.n == 0) {
        // xxxzzz check if number of keybufs is accurate enough
        if (alloc_keybufs(pgc, pgc->bt->nodesize/16, &dataptrs)) {
	    errmsg("Could not allocate dataptrs in ws_gc_cb");
	    return(1); // error!
	}
        if (alloc_keybufs(pgc, pgc->bt->nodesize/16, &dataptrs2)) {
	    errmsg("Could not allocate dataptrs2 in ws_gc_cb");
	    return(1); // error!
	}
    }

    /*******************************************************
     * Algorithm: xxxzzz check that this is correct!
     *
     *    - First node-sized region must be a b-tree node.
     *    - Lock hashtable for node id.
     *    - Check hashtable to see if node is current.  If not, fetch current.
     *	  - Find location of all data in stripe pointed to by this node.
     *	      - Note that there may be multiple writes in the same
     *          leaf node!
     *        - Note that some nodes may be non-leaf due to b-tree splits/joins.
     *    - Copy node and data from read buffer to new compacted sbuf.
     *    - Update physical pointers in hashtable and (copied) leaf-node.
     *	  - Mark bit vector for accounted blocks.
     *    - Unlock hashtable for node id.
     *	  - Go to next unaccounted region and decode the
     *	    next b-tree node and repeat.
     *
     *   Must identify non-leaf b-tree nodes caused by b-tree restructuring.
     *   These also require changes logical-to-physical map in ZS hashtable.
     *
     *******************************************************/

    bv_clear(accounted);  //  bit vector is at sector granularity
    nodesize           = pgc->bt->nodesize;
    sectors_per_node   = nodesize/sector_bytes;
    sectors_per_stripe = stripe_size/sector_bytes;
    nodesize           = sector_bytes*sectors_per_node;
    p_from             = pbuf;
    pmax_from          = p_from + stripe_bytes;
    p_to               = pbuf_to;

    while (1) {
        if (data_in_leaves) {
	    rc = decode_btree_node_data_in_leaves(pgc->pzs, pgc->bt, accounted, sector_bytes, sectors_per_node, pbuf, stripe_addr, p_from, &p_to, pmax_from)
	} else {
	    rc = decode_btree_node_data_not_in_leaves(pgc->pzs, pgc->bt, accounted, sector_bytes, sectors_per_node, stripe_addr, pbuf, p_from, &p_to, pmax_from)
	}
	if (rc != 0) {
	    //  oh-oh!
	    return(1); // panic!
	}
	p_from_new = advance_p(accounted, pbuf, p_from, pmax_from, sector_bytes, sectors_per_stripe);
	if (p_from_new == p_from) {
	    break;
	}
	p_from = p_from_new;
    }
    return(0);
}

    /*  Cases:
     *
     *  - If data in b-tree leaves:
     *
     *      - node id in use and still points to this node:
     *          - copy node
     *      - otherwise:
     *          - do not copy node
     *
     *  - If data outside of b-tree:
     *
     *      - node id in use and still points to this node:
     *          - find data pointers landing in stripe
     *          - copy data that is still valid
     *          - copy node
     *
     *      - node id in use and not current
     *          - find data pointers landing in stripe
     *          - read in current contents
     *          - for stripe data that is still in range of current contents,
     *            copy data that is still valid
     *          - for stripe data that is out of range:
     *              - fetch next leaf to check data for missing, larger keys
     *                  - must check that next node is valid!
     *              - use keys to query b-tree to see which smaller keys 
     *                point to data that is still valid in this stripe
     *              - copy data that is still valid
     *          - do not copy node
     *
     *      - otherwise:
     *          - find data pointers landing in stripe
     *          - use keys to query b-tree to see which pointers are
     *            still valid 
     *              - do a range query, since keys should be clustered 
     *                in a small number of leaf nodes)
     *              - in most cases, there should only be one key to look up!
     *          - copy data that is still valid
     *          - do not copy node
     *
     *      - xxxzzz: what about extent handling?
     */

typedef struct ht_handle {
    xxxzzzz                 lk_wait;
    xxxzzzz                 hash_entry;
    xxxzzzz                 cguid;
    xxxzzz                  shard;
    xxxzzz                  syndrome;
    struct ZS_thread_state *pzs;
} ht_handle_t;

static int ht_lock_and_fetch_data_ptr(struct ZS_thread_state *pzs, btree_raw_t *bt, uint64_t logical_id, uint64_t old_addr, uint64_t new_addr, uint64_t *ht_addr, ht_handle_t *ph)
{
    fthLock_t           *lk;
    fthWaitEl_t         *lk_wait;
    xxxzzz               shard;
    xxxzzz               hash_entry;
    SDF_action_init_t   *pac = (SDF_action_init_t *) pzs;

    syndrome = hashck((unsigned char *) &logical_id, 8, 0, bt->cguid);

    shard    = xxxzzz(bt->cguid);;

    lk       = hash_table_find_lock(shard->hash_handle, syndrome, SYN);
    lk_wait  = fthLock(lk, 1, NULL );

    hash_entry = hash_table_get( pac->paio_ctxt, shard->hash_handle, (xxxzzz *) &logical_id, 8, bt->cguid);
    if (hash_entry == NULL) {
	fthLock(lk_wait);
        return(1);
    }

    *ht_addr = hash_entry->;

    ph->lk_wait    = lk_wait;
    ph->hash_entry = hash_entry;
    ph->syndrome   = syndrome;
    ph->cguid      = bt->cguid;
    ph->shard      = shard;
    ph->nodesize   = bt->nodesize;
    ph->logical_id = logical_id;
    ph->pzs        = pzs;

    return(0);
}

static int ht_unlock(ht_handle_t *ph)
{
    fthUnlock(ph->lk_wait);
}

static int ht_change_ptr_and_unlock(ht_handle_t *ph, uint64_t new_addr)
{

    //  point to the new node 
    if (xxxzzz == old_addr) {
        xxxzzz = new_addr;

	xxxzzz don't forget logging!
    }

    xxxzzz continue from here

    hdl = xxxzzz;

    syndrome
    blk_offset
    cntr_id = ph->cguid;
    key
    key_len
ok  meta_data->sequence
    durlevel = SDF_FULL_DURABILITY  or  SDF_RELAXED_DURABILITY
?   target_seqno = xxxzzz   // seqno of node before rewrite here
ok  shard = ph->shard

    SDF_cache_ctnr_metadata_t    *meta;
    SDF_action_init_t            *pac;

    pac = (SDF_action_init_t *) my_thd_state;

    meta = get_container_metadata(pac, cguid);
    if (meta == NULL) {
	 xxxzzz
    }

    seqno = atomic_inc_get(shard->sequence);

    /*
     * update the hash table entry
     */
    xxxzzz ? new_entry.used       = 1;
    xxxzzz ? new_entry.referenced = 1;

    new_blkaddr = xxxzzz;

    //  we should always use a key_cache when using the write serializer
    if(key_len == 8 && hdl->key_cache) {
	keycache_set(shard->hash_handle, new_blkaddr, ph->logical_id);
    }


    /*
     * overwrite case in store mode
     *
     * Here we won't write a log record or remove the hash entry;
     * instead we write a special log record at create time
     */
    mcd_fth_osd_remove_entry(shard, hash_entry, shard->persistent, false);




    log_rec.syndrome   = new_entry.hesyndrome;
    log_rec.deleted    = new_entry.deleted;
    log_rec.reserved   = 0;
    log_rec.blocks     = new_entry.blocks;
    if (!hdl->addr_table) {
	log_rec.rbucket = (syndrome % hdl->hash_size) / Mcd_osd_bucket_size;
    } else {
	log_rec.rbucket = (syndrome % hdl->hash_size);
    }
    log_rec.mlo_blk_offset = new_entry.blkaddress;
    log_rec.cntr_id    = cntr_id;
    log_rec.seqno      = seqno;
    log_rec.raw = FALSE;
    log_rec.mlo_dl = durlevel;

    // overwrite case in store mode
    log_rec.mlo_old_offset   = ~(hash_entry->blkaddress) & 0x0000ffffffffffffull;
    log_rec.target_seqno = target_seqno;
    log_write_trx( shard, &log_rec, syndrome, hash_entry);

    //  update hash table entry
    hash_entry->blkaddress = new_blkaddr;

    fthUnlock(ph->lk_wait);
}

xxxzzz can gc'd data actually grow because of changes to nodes no originally
       in the stripe?

static int decode_btree_node_data_in_leaves(struct ZS_thread_state *pzs, btree_raw_t *bt, bv_t *accounted, uint32_t sector_bytes, uint32_t sectors_per_node, uint64_t stripe_addr, void *pbuf, void *p_from, void **pp_to, void *pmax_from)
{
    uint64_t           node_addr, ht_addr, new_addr;
    btree_raw_node_t  *n;
    void              *p_to;
    ht_handle_t        handle;

    n = (btree_raw_node_t *) p_from;
    p_to = *pp_to;

    node_addr = stripe_addr + (p_from - pbuf);

    new_addr = xxxzzz;

    if ((ht_lock_and_fetch_data_ptr(pzs, bt, n->logical_id, node_addr, new_addr, &ht_addr, &handle) == 0) &&
        (node_addr == ht_addr))
    {
	memcpy(p_from, p_to, sector_bytes*sectors_per_node);
        
	ht_unlock(&handle);
    }
    // account for the space for the node we just decoded
    bv_set_bits(accounted, (p_from - pbuf)/sector_bytes, sectors_per_node);

    p_to += sector_bytes*sectors_per_node;
    *pp_to   = p_to;
    return(0);
}

static int data_in_buf(uint64_t start, uint64_t end, uint64_t ptr)
{
    if ((ptr >= start) &&
        (ptr < end))
    {
	return(1);
    }
    return(0);
}

static int find_stripe_data(btree_raw_t *bt, btree_raw_node_t *n, dataptrs_t *dp)
{
    int                i;
    key_stuff_info_t   ks = {0};

    dp->n_in_stripe = 0;
    ks.key = dp->keybufs[dp->n_in_stripe];
    for (i = 0; i < n->nkeys; i++) {
	get_key_stuff_info2(bt, n, i, &ks);

	if (ks.leaf) {
	    if (data_in_buf(stripe_addr, addr_end, ks.ptr)) {
	        dp->keyptrs[n_in_stripe]      = ks.ptr;
	        dp->keylens[n_in_stripe]      = ks.keylen;
		dp->keydatasizes[n_in_stripe] = ks.datalen;
	        dp->n_in_stripe++;
		if (dp->n_in_stripe >= MAX_STRIPE_KEYS) {
		    errmsg("Exceeded max number of stripe keys per node");
		    //  panic!
		    return(1);
		}
		ks.key = dp->keybufs[dp->n_in_stripe];
	    }
	}
    }
    return(0);
}

static void do_copy(bv_t *accounted, void **pp_to, void *p_from, void *pbuf, uint32_t datasize, uint32_t sector_bytes)
{
    void *p_to = *pp_to;

    memcpy(p_to, p_from, datasize);
    bv_set_bits(accounted, (p_from - pbuf)/sector_bytes, datasize/sector_bytes);
    p_to += datasize;

    *pp_to = p_to;
}

static void copy_datas(bv_t *accounted, void *pbuf, void **pp_to, dataptrs_t *dp, uint32_t sector_bytes, uint32_t sectors_per_node)
{
    int         i;
    void       *p_to = *pp_to;

    for (i=0; i<dp->n_in_stripe; i++) {
        do_copy(accounted, &p_to, dp->keyptrs[i], pbuf, dp->keydatasizes[i], sector_bytes);
    }

    *pp_to = p_to;
}

//  returns 1 if dp2 is the same or a subset of dp
static int check_subset(dataptrs_t *dp, dataptrs_t *dp2)
{
    int   i, j;

    if (dp->n_in_stripe < dp2->n_in_stripe) {
        return(0);
    }

    for (i=0; i<dp2->n_in_stripe; i++) {
        is_match = 0;
	for (j=0; j<dp->n_in_stripe; j++) {
	    if (dp2->keyptrs[i] == dp->keyptrs[j]) {

	        // check that other stuff matches

		if (dp2->keylens[i] != dp->keylens[j]) {
		    continue;
		}
		if (dp2->datasizes[i] != dp->datasizes[j]) {
		    continue;
		}
		if (memcmp(dp2->keybufs[i], dp->keybufs[j], dp->keylens[j] != 0) {
		    continue;
		}
		is_match = 1;
		break;
	    }
	}
	if (!is_match) {
	    return(0);
	}
    }
    return(1);
}

typedef struct gc_read_cb_data {
    void         **pp_to;
    dataptrs_t    *dp;
    dataptrs_t    *dp2;
    bv_t          *accounted;
    void          *pbuf;
    uint32_t       sector_bytes;
    uint32_t       sectors_per_node;
    uint64_t       old_logical_id;
} gc_read_cb_data_t;

static int read_object_gc_cb(void *cb_data, bt_raw_node_t *n)
{
    gc_read_cb_data_t   *rd = (gc_read_cb_data_t *) cb_data;
    void                *p_to = *rd->pp_to;

    if (find_stripe_data(bt, n, dp2)) { 
	return(1); // panic!
    }

xxxzzz where is node pointer updated?

    for (i=0; i<dp2->n_in_stripe; i++) {
        // see if we already copied this
	for (j=0; j<dp->n_in_stripe; j++) {
	    if (dp2->keyptrs[i] == dp->keyptrs[j]) {
	        if (!dp->copyflags[j]) {
	            dp->copyflags[j] = 1;
		    do_copy(rd->accounted, &p_to, dp2->keyptrs[i], rd->pbuf, dp2->keydatasizes[i], rd->sector_bytes);
		}
		break;
	    }
	    if (j >= dp->n_in_stripe) {
	        //  oh-oh!  internal inconsistency!   panic!
		panicmsg("Internal inconsistency in read_object_gc_cb: in-stripe data found in latest b-tree node %lld, but not in older node %lld", n->logical_id, old_logical_id);
		*(rd->pp_to) = p_to;
		return(1);
	    }
	}
    }

    *(rd->pp_to) = p_to;
    return(0);
}

static int  decode_btree_node_data_not_in_leaves(struct ZS_thread_state *pzs, btree_raw_t *bt, bv_t *accounted, uint32_t sector_bytes, uint32_t sectors_per_node, uint64_t stripe_addr, void *pbuf, void *p_from, void **pp_to, void *pmax_from)
{
    int                do_full_check;
    int                i, j;
    uint64_t           addr_end;
    uint64_t           new_addr;
    btree_raw_node_t  *n, *n2;
    void              *p_to;
    char               tmp_node[MAX_NODE_SIZE];
    uint32_t           n_in_stripe, n2_in_stripe;
    dataptrs_t        *dp  = &dataptrs;
    dataptrs_t        *dp2 = &dataptrs2;
    gc_read_cb_data_t  rd;
    ht_handle_t        handle;

    addr_end = stripe_addr + (pmax_from - pbuf);

    p_to = *pp_to;

    //  find keys that point to data within this stripe

    n = (btree_raw_node_t *) p_from;

    if (find_stripe_data(bt, n, dp)) {
        return(1); // panic!
    }

    do_full_check = 0;
    node_addr     = stripe_addr + (p_from - pbuf);
    new_addr      = xxxzzz;
    if (ht_lock_and_fetch_data_ptr(pzs, bt, n->logical_id, node_addr, new_addr, &ht_addr, &handle) == 0) {
        if (node_addr == ht_addr) {

	    /* Case 1: node id in use and still points to this node:
	     *          - copy node
	     *          - find data pointers landing in stripe
	     *          - copy data that is still valid
	     */

	    do_copy(accounted, &p_to, p_from, pbuf, node_bytes, sector_bytes);

            copy_datas(accounted, pbuf, &p_to, dp, sector_bytes, sectors_per_node);
	    ht_change_ptr_and_unlock(&handle, new_addr);

	} else {

	    /* Case 2: node id in use and not current
	     *          - find data pointers landing in stripe
	     *          - read in current contents
	     *          - copy stripe data that is still valid
	     *          - if any of the old stripe data pointers fall
	     *            outside of latest node keys, resort to a full search
	     *          - do not copy node
	     */

            if (do_point_read(ps, (void *) node_buf, ht_addr, node_bytes) {
		ht_unlock(&handle);
		return(1); // panic!
	    }
	    n2 = (btree_raw_node_t *) node_buf;

	    if (find_stripe_data(bt, n2, dp2)) { 
	        ht_unlock(&handle);
		return(1); // panic!
	    }

xxxzzz where is node pointer updated?  do it above in ht_lock_and_fetch_data_ptr
	    //  update node pointers write out node
	    xxxzzz

            if (check_subset(dp, dp2)) {
		copy_datas(accounted, pbuf, &p_to, dp2, sector_bytes, sectors_per_node);
		ht_change_ptr_and_unlock(&handle, new_addr);
	    } else {
		do_full_check = 1;
	    }
	}

    } else {
        do_full_check = 1;
    }

    if (do_full_check) {

	/* Case 3: otherwise:
	 *          - find data pointers landing in stripe
	 *          - use keys to query b-tree to see which pointers are
	 *            still valid 
	 *              - do a range query, since keys should be clustered 
	 *                in a small number of leaf nodes)
	 *              - in most cases, there will only be one key to look up!
	 *          - copy data that is still valid
	 *          - do not copy node
	 */

        //  clear copy flags
	for (i=0; i<dp->n_in_stripe; i++) {
	    dp->copyflag[i] = 0;
	}

	rd.pp_to            = pp_to;
	rd.dp               = dp;
	rd.dp2              = dp2;
	rd.accounted        = accounted;
	rd.pbuf             = pbuf;
	rd.sector_bytes     = sector_bytes;
	rd.sectors_per_node = sectors_per_node;
	rd.old_logical_id   = n->logical_id;

	for (i=0; i<dp->n_in_stripe; i++) {
	    if (dp->copyflag[i]) {
	        continue;
	    }

            ret = ZSReadObjectCB(pgc->pzs, read_object_gc_cb, (void *) &rd, cguid, dp->keybufs[i], dp->keylens[i]);
	}

	ht_unlock(handle);
    }

    *pp_to   = p_to;
    return(0);
}

static void *advance_p(bv_t *accounted, void *pbuf, void *p, void *pmax, uint32_t sector_bytes, uint32_t max_sectors)
{
    for (i=(p-pbuf)/sector_bytes; i<max_sectors; i++) {
        if (!bv_bit(accounted, i)) {
	    break;
	}
	p += sector_bytes;
    }
    return(p);
}


====================================
xxxzzz move this to fdf_wrapper.c:
====================================

xxxzzz set callback function pointer to NULL for other calls to btree_get, etc!

xxxzzz add BSTAT_READ_CB_CNT

xxxzzz add rw_cb and rw_cb_data to metadata structure

xxxzzz modify btree_raw_get to call callback

typedef int (*read_object_gc_cb_t)(void *cb_data, bt_raw_node_t *n);

/**
 *  @brief Read the leaf node that contains (or would contain) a specified key.
 *
 *  Once the leaf is found, invoke a callback function to process the node.
 *
 *  @param zs_thread_state <IN> The ZS context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OBJECT_UNKNOWN: the object does not exist.
 *          ZS_IN_TRANS: this operation cannot be done inside a transaction.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t _ZSReadObjectCB(
	struct ZS_thread_state  *zs_thread_state,
	read_object_gc_cb_t      read_cb,
	void                    *cb_data,
	ZS_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen
	)
{
    ZS_status_t      ret = ZS_SUCCESS;
    btree_status_t    btree_ret = BTREE_SUCCESS;
    btree_metadata_t  meta;
    struct btree     *bt;
    int index;
    btree_range_meta_t   rmeta;
    btree_range_cursor_t *cursor;
    btree_range_data_t   values[1];
    int n_out;


	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}
	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}

    {
        // xxxzzz this is temporary!
        __sync_fetch_and_add(&(n_reads), 1);
        if (0 && (n_reads % 200000) == 0) {
            dump_btree_stats(stderr, cguid);
        }
    }

    my_thd_state = zs_thread_state;

    if (ZS_SUCCESS != (ret = ZSOperationAllowed())) {
        msg("Shutdown in Progress. Read object not allowed\n");
        return (ret);
    }

    if ((index = bt_get_ctnr_from_cguid(cguid)) == -1) {
        return ZS_FAILURE_CONTAINER_NOT_FOUND;
    }

    cm_lock(cguid, READ);
    if (!IS_ZS_BTREE_CONTAINER(Container_Map[cguid].flags) ) {
        ret = ZS_FAILURE;  //  not allowed for hash containers
        cm_unlock(cguid);
#ifdef UNIFIED_CNTR_DEBUG
        fprintf(stderr, "Reading object with callback from a HASH container\n");
#endif
        return ret;
    }
    cm_unlock(cguid);

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
    if (bt == NULL) {
        return (ret);
    }

    if (keylen > bt->max_key_size) {
        msg("btree_insert/update keylen(%d) more than max_key_size(%d)\n",
                 keylen, bt->max_key_size);
        bt_rel_entry(index, false);
        return (ZS_KEY_TOO_LONG);
    }

    meta.flags = 0;
    __sync_add_and_fetch(&(bt->partitions[0]->stats.stat[BTSTAT_READ_CB_CNT]),1);
    if (Container_Map[index].read_by_rquery) {
        btree_ret = BTREE_FAILURE;  //  not allowed for read_by_rquery
    } else {
        rmeta.rw_cb      = read_cb;
        rmeta.rw_cb_data = cb_data;
	trxenter( cguid);
        btree_ret = btree_get(bt, key, keylen, data, datalen, &meta);
    }

done:
    trxleave( cguid);
    ret = BtreeErr_to_ZSErr(btree_ret);
    bt_rel_entry(index, false);
    return(ret);
}

============================================
xxxzzz end of move this to fdf_wrapper.c:
============================================


//  Callback for setting metadata (transactionally!)
  /*  returns:
   *    WS_OK
   *    WS_ERROR
   */
static int ws_set_metadata_cb(void *state, void *key, uint32_t key_size, void *data, uint32_t data_size)
{
    gc_ws_state_t             *pgc = (gc_ws_state_t *) state;

    pzs, bt
    xxxzzz
}

//  Callback for getting metadata
  /*  returns:
   *    WS_OK
   *    WS_ERROR
   */
static int ws_get_metadata_cb(void *state, void *key, uint32_t key_size, void *data, uint32_t max_size, uint32_t *md_size)
{
    gc_ws_state_t             *pgc = (gc_ws_state_t *) state;

    pzs, bt
    xxxzzz
}

===============================================================
xxxzzz  end of move callbacks outside of write serializer package
===============================================================

