/************************************************************************
 * 
 *  btree_range.c  May 17, 2013   Harihara Kadayam
 * 
 ************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <inttypes.h>
#include <assert.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include "btree_hash.h"
#include "btree_raw.h"
#include "btree_map.h"
#include "btree_raw_internal.h"
#include "btree_range.h"
#include "btree/btree_var_leaf.h"

#define N_CURSOR_MAX 3
static __thread btree_range_cursor_t __thread_cursor[N_CURSOR_MAX];
static __thread char tmp_key_buf[BTREE_MAX_NODE_SIZE] = {0};

#define IS_ASCENDING_QUERY(meta) \
             (((meta)->key_start && ((meta)->flags & (RANGE_START_GT | RANGE_START_GE))) || \
              ((meta)->key_end && ((meta)->flags & (RANGE_END_LT | RANGE_END_LE))) || \
              (((meta)->key_start == NULL) && ((meta)->key_end == NULL)))

#define is_snapshot_query(meta) (meta->flags & (RANGE_SEQNO_EQ | RANGE_SEQNO_LE | RANGE_SEQNO_GT_LE))

static inline int
fatal(btree_status_t s) { return s != BTREE_SUCCESS && s != BTREE_WARNING; }

static int 
is_multiple_flags_set(btree_range_flags_t flags, uint32_t f)
{
	uint32_t n = flags & f;
	return ((n & (n-1)) ? 1: 0);
}

static btree_status_t
validate_range_query(btree_range_meta_t *rmeta)
{
	int all_start_flags; 
	int all_end_flags; 

	all_start_flags = 
	         RANGE_START_GT | RANGE_START_GE | RANGE_START_LE | RANGE_START_LT;

	all_end_flags = RANGE_END_GT | RANGE_END_GE | RANGE_END_LE | RANGE_END_LT;

	if (is_multiple_flags_set(rmeta->flags, RANGE_SEQNO_LE | RANGE_SEQNO_GT_LE)) {
		return BTREE_INVALID_QUERY;
	}

	/* There can be only one start flag and one end flag */
	if (is_multiple_flags_set(rmeta->flags, all_start_flags)) {
		return BTREE_INVALID_QUERY;
	}

	if (is_multiple_flags_set(rmeta->flags, all_end_flags)) {
		return BTREE_INVALID_QUERY;
	}

	/* If any start is given, key_start should be non-NULL and converse as well */
	if ((rmeta->flags & all_start_flags)) {
		if (rmeta->key_start == NULL) {
			return BTREE_INVALID_QUERY;
		}
	} else {
		if (rmeta->key_start != NULL) {
			return BTREE_INVALID_QUERY;
		}
	}

	if ((rmeta->flags & all_end_flags)) {
		if (rmeta->key_end == NULL) {
			return BTREE_INVALID_QUERY;
		}
	} else {
		if (rmeta->key_end != NULL) {
			return BTREE_INVALID_QUERY;
		}
	}

	/* Incompatible flags. GT or GE start should have LE or LT end and vice versa */
	if (is_multiple_flags_set(rmeta->flags, 
	       RANGE_START_GT | RANGE_START_GE | RANGE_END_GT | RANGE_END_GE)) {
		return BTREE_INVALID_QUERY;
	}
	if (is_multiple_flags_set(rmeta->flags, 
	       RANGE_START_LT | RANGE_START_LE | RANGE_END_LT | RANGE_END_LE)) {
		return BTREE_INVALID_QUERY;
	}

	return (BTREE_SUCCESS);
}

static btree_status_t
fill_key_range(btree_range_cursor_t *c,
               btree_raw_mem_node_t *n,
	       int index,
               btree_range_meta_t   *rmeta,
               btree_range_data_t   *value,
               btree_range_data_t   *prev_value)
{
	node_vlkey_t      *pvlk;
	btree_status_t status;
	btree_status_t ret = BTREE_SUCCESS;
	uint32_t keybuf_size;
	uint64_t databuf_size;
	uint32_t meta_flags;
	key_meta_t key_meta;
	btree_metadata_t smeta;
	bool query_match, range_match;
	key_stuff_info_t ks = {0};
	key_stuff_info_t *pks = NULL;

	/* Validate if this is in valid seqno range */
	btree_leaf_get_meta(n->pnode, index, &key_meta);
	smeta.flags = rmeta->flags;
	smeta.start_seqno = rmeta->start_seq;
	smeta.end_seqno = rmeta->end_seq;
	(void)seqno_cmp_range(&smeta, key_meta.seqno, &query_match, &range_match);
	if (!query_match && !range_match) {
		return (BTREE_SKIPPED);
	}

	ks.key = tmp_key_buf;
	get_key_stuff_info2(c->btree, n->pnode, index, &ks);
	pks = &ks;

	if (c->prior_version_tombstoned) {
		/* There is a previous tombstoned key present and this key
		 * is same as the one tombstoned, so skip it */
		if (c->btree->cmp_cb(c->btree->cmp_cb_data, c->ts_key,
		               c->ts_keylen, ks.key, ks.keylen) == 0) {
			return BTREE_SKIPPED;
		}
	} else {
		/* Previous key is same as current one, previous is the latest, so skip
		 * this one */
		if (prev_value && 
		     (c->btree->cmp_cb(c->btree->cmp_cb_data, prev_value->key,
		                       prev_value->keylen, ks.key, ks.keylen) == 0)) {
			return (BTREE_SKIPPED);
		}
	}

	/* If key is tombstoned, apart from skipping, store this key to
	 * avoid subsequent versions to skip as well */
	if (key_meta.tombstone) {
		memcpy(c->ts_key, ks.key, ks.keylen);
		c->ts_keylen = ks.keylen;
		c->prior_version_tombstoned = true;
		return BTREE_SKIPPED;
	} else {
		c->prior_version_tombstoned = false;
	}

	/* TODO: Later on try to combine range_meta and btree_meta
	 * For now, set the meta_flags for get_leaf_data usage */
	meta_flags = rmeta->flags & (RANGE_BUFFER_PROVIDED | RANGE_ALLOC_IF_TOO_SMALL | RANGE_INPLACE_POINTERS);
	if (rmeta->flags & RANGE_BUFFER_PROVIDED) {
		keybuf_size  = rmeta->keybuf_size;
		databuf_size = rmeta->databuf_size;
	}
 
	value->status = BTREE_RANGE_STATUS_NONE; 

	status = get_leaf_key_index(c->btree, n->pnode, index,
				      &value->key,
				      &keybuf_size,
				      meta_flags, pks);

	dbg_print_key(value->key, keybuf_size, "key");

	switch (status) {
	case BTREE_SUCCESS:
		break;
	case BTREE_BUFFER_TOO_SMALL:
		ret = BTREE_WARNING;
		value->status |= BTREE_KEY_BUFFER_TOO_SMALL;
		break;
	case BTREE_FAILURE:
	default:
		value->status |= BTREE_FAILURE;
		return (BTREE_FAILURE);
	}
	value->keylen = keybuf_size;

	/* Check if we need to pause at this key */
	if (rmeta->allowed_fn) {
		if (!rmeta->allowed_fn(rmeta->cb_data, value->key,
		                       value->keylen)) {
			value->status |= BTREE_RANGE_PAUSED;
			return (BTREE_QUERY_PAUSED);
		}
	}

	if (!(rmeta->flags & RANGE_KEYS_ONLY)) {
		status = get_leaf_data_index(c->btree, n->pnode, index,
					       &value->data, &databuf_size,
					       meta_flags, 0,
		                               is_snapshot_query(rmeta));

		switch (status) {
		case BTREE_SUCCESS:
			break;
		case BTREE_BUFFER_TOO_SMALL:
			ret = BTREE_WARNING;
			value->status |= BTREE_DATA_BUFFER_TOO_SMALL;
			break;
		case BTREE_FAILURE:
		default:
			value->status |= BTREE_FAILURE;
			return (status);
		}
		value->datalen = databuf_size;
	}

	//value->seqno    = pvlk->seqno; 
	//value->syndrome = 0; // Is this required 

	if (ret == BTREE_SUCCESS) {
		value->status = BTREE_RANGE_SUCCESS;
	}

	return ret;
}

static inline
int bsearch_end(btree_range_cursor_t *c, btree_raw_mem_node_t *node) 
{
	btree_range_meta_t* meta = &c->query_meta;
	btree_metadata_t smeta;
	bool found;

	if(!meta->key_end) {
		return c->dir > 0 ? node->pnode->nkeys : 0;
	} else {
		smeta.flags       = meta->flags;
		smeta.start_seqno = meta->start_seq;
		smeta.end_seqno   = meta->end_seq;

		return bsearch_key_low(c->btree, node->pnode, meta->key_end,
		                       meta->keylen_end, &smeta, 0, -1, node->pnode->nkeys,
		                       (meta->flags & (RANGE_END_GE | RANGE_END_LT)) ? 
		                          BSF_LATEST: BSF_NEXT,
		                       &found);
	}
}

static inline
int bsearch_start(btree_range_cursor_t *c, btree_raw_mem_node_t *node) 
{
	btree_range_meta_t* meta = &c->query_meta;
	btree_metadata_t smeta;
	bool found;

	if(!meta->key_start) {
		return c->dir > 0 ? 0 : node->pnode->nkeys;
	} else {
		smeta.flags       = meta->flags;
		smeta.start_seqno = meta->start_seq;
		smeta.end_seqno   = meta->end_seq;

		return bsearch_key_low(c->btree, node->pnode, meta->key_start,
				meta->keylen_start, &smeta, 0, -1, node->pnode->nkeys,
				(meta->flags & (RANGE_START_GE | RANGE_START_LT)) ?
		                         BSF_LATEST : BSF_NEXT,
		                &found);
	}
}

static btree_status_t
btree_range_find_diversion(btree_range_cursor_t* c)
{
	int x;
	key_stuff_info_t ks;
	btree_range_meta_t *meta = &c->query_meta;
	btree_raw_mem_node_t *parent, *node;
	btree_raw_node_t* pnode;
	btree_status_t ret = BTREE_SUCCESS;
	uint64_t child_id;

	node = root_get_and_lock(c->btree, 0, &ret);
	if (node == NULL) {
		if (storage_error(ret) && btree_in_rescue_mode(c->btree)) {
			add_to_rescue(c->btree, NULL, c->btree->rootid, 0);
		}
		return ret;
	}

	ks.key = tmp_key_buf;

	while(!is_leaf(c->btree, node->pnode)) {
		pnode = node->pnode;

		x = bsearch_start(c, node);

		child_id = pnode->rightmost;
		if(x < pnode->nkeys) {
			get_key_stuff_info2(c->btree, pnode, x, &ks); //get key_stuff_info
			child_id = ks.ptr;
		}

		c->start_idx = x;

		if((c->dir > 0 && x < pnode->nkeys) || (c->dir < 0 && x > 0))
		{
			if(meta->key_end) {
				if(c->dir < 0) {
					get_key_stuff_info2(c->btree, pnode, x - 1, &ks);
				}

				x = c->btree->cmp_cb(c->btree->cmp_cb_data, meta->key_end, meta->keylen_end,
					ks.key, ks.keylen);
			} else {
				x = c->dir;
			}


			if(c->dir * x > 0 || (!x && (meta->flags & RANGE_END_LE)))
			{
				c->end_idx = bsearch_end(c, node);
				c->end_idx += c->dir;
				c->node = node;
				return ret;
			}
		}

		parent = node;

		getnode_flags_t nflags = NODE_CACHE_VALIDATE;
		if (is_snapshot_query(meta)) {
			nflags |= NODE_CACHE_DEREF_DELETE;
		}
		node = get_existing_node(&ret, c->btree, child_id, nflags, LOCKTYPE_READ);
		if (node == NULL) {
			if (storage_error(ret) && btree_in_rescue_mode(c->btree)) {
				add_to_rescue(c->btree, parent->pnode, child_id, c->start_idx);
			}
			plat_rwlock_unlock(&parent->lock);
			deref_l1cache_node(c->btree, parent);
			return ret;
		}

		plat_rwlock_unlock(&parent->lock);
		deref_l1cache_node(c->btree, parent);
	}

	c->start_idx = bsearch_start(c, node);
	c->end_idx = bsearch_end(c, node);

	if(c->dir < 0) {
		c->end_idx += c->dir;
		c->start_idx += c->dir;
	}

	c->node = node;

	return ret;
}

static void
populate_output_array(btree_range_cursor_t *c,
		btree_raw_mem_node_t* node,
		int16_t *cur_idx, int16_t end_idx, int n_in, int *n_out,
		btree_range_data_t* values,
		btree_status_t* status)
{
	btree_status_t ret;

	dbg_print("cur_idx=%d end_idx=%d n_in=%d n_out=%d\n", (int)*cur_idx, (int)end_idx, n_in, *n_out);

	while (*cur_idx != end_idx && *n_out < n_in && !fatal(*status))
	{
//		ret = fill_key_range(c->btree, node, key_offset(c->btree,
//						node->pnode, *cur_idx), c->query_meta, values + *n_out);
		ret = fill_key_range(c, node, (int) *cur_idx, &c->query_meta,
		                     values + *n_out,
		                     (*n_out) ? values + (*n_out) - 1: NULL);

		if (ret == BTREE_SKIPPED) {
			(*cur_idx) += c->dir;
			continue;
		}

		if(ret != BTREE_SUCCESS)
			*status = ret;

		if ((ret != BTREE_FAILURE) && !storage_error(ret)) {
			(*n_out)++;
			if(ret != BTREE_QUERY_PAUSED)
				(*cur_idx) += c->dir;
		}
	}

	dbg_print("return cur_idx=%d end_idx=%d n_in=%d n_out=%d status=%d\n", (int)*cur_idx, (int)end_idx, n_in, *n_out, *status);
}

int store_key(char **to, uint32_t *tolen, char* from, uint32_t fromlen) {
	if(*to)
		free(*to);
	*to = NULL;
	if (from) {
		*to = (char *)malloc(fromlen);
		if (*to == NULL)
			return 0;
		memcpy(*to, from, fromlen);
		*tolen = fromlen;
	}
	return 1;
}

static
btree_status_t
btree_range_get_next_fast(btree_range_cursor_t *c,
                     int                   n_in,
                     int                  *n_out,
                     btree_range_data_t   *values)
{
#define START_EDGE 1
#define END_EDGE 2
#define TREE_DEPTH_MAX 32
	struct {
		btree_raw_mem_node_t* node;
		int16_t start_idx, end_idx, cur_idx;
		uint8_t edge;
	} stack[TREE_DEPTH_MAX], *cur, *child;

	key_stuff_info_t ks;
	btree_range_meta_t *meta = &c->query_meta;
	btree_status_t ret = BTREE_SUCCESS, r = BTREE_SUCCESS;
	int sp = 0;

	*n_out = 0;

	/* Right now it is not required to keep track of tombstone
	 * across two chunks of range queries */
	c->prior_version_tombstoned = false;

	if (!btree_in_rescue_mode(c->btree)) {
		plat_rwlock_rdlock(&c->btree->lock);
	}

	ret = btree_range_find_diversion(c);
	if (ret != BTREE_SUCCESS) {
		sp = -1;
		goto cleanup;
	}

	stack[0].node = c->node;
	stack[0].cur_idx = c->start_idx;
	stack[0].start_idx = c->start_idx;
	stack[0].end_idx = c->end_idx;
	stack[0].edge = START_EDGE | END_EDGE;

	while(sp >= 0 && !fatal(ret)) {
		cur = stack + sp;
		child = cur + 1;

		int isleaf = is_leaf(c->btree, cur->node->pnode);

		dbg_print("sp=%d cur_idx %d start_idx %d end_idx %d is_leaf %d logical_id %ld\n", sp, cur->cur_idx, cur->start_idx, cur->end_idx, isleaf, cur->node->pnode->logical_id);

		if(isleaf) {
			if(*n_out >= n_in)
				break;

			if(cur->cur_idx * c->dir > cur->end_idx * c->dir)
				ret = BTREE_FAILURE;
			else
				populate_output_array(c, cur->node, &cur->cur_idx, cur->end_idx, n_in, n_out, values, &ret);
		} else if(cur->cur_idx != cur->end_idx) {
			uint64_t logical_id = cur->node->pnode->rightmost;
			if(cur->cur_idx < cur->node->pnode->nkeys) {
				ks.key = tmp_key_buf;
				get_key_stuff_info2(c->btree, cur->node->pnode, cur->cur_idx, &ks);
				logical_id = ks.ptr;
			}

			getnode_flags_t nflags = NODE_CACHE_VALIDATE;
			if (is_snapshot_query(meta)) nflags |= NODE_CACHE_DEREF_DELETE;

			child->node = get_existing_node(&r, c->btree, logical_id, nflags, LOCKTYPE_READ);
			if (child->node == NULL) {
				if (storage_error(r) && btree_in_rescue_mode(c->btree)) {
					add_to_rescue(c->btree, cur->node->pnode, logical_id, cur->cur_idx);
				}
				ret = r;
				goto cleanup;
			}

			ks.key = NULL;
			ks.keylen = 0;

			child->cur_idx = child->start_idx = c->dir > 0 ? 0 : child->node->pnode->nkeys;
			if(cur->cur_idx == cur->start_idx && (cur->edge & START_EDGE)) {
				child->edge |= START_EDGE;
				child->cur_idx = child->start_idx = bsearch_start(c, child->node);
			}

			child->end_idx = c->dir > 0 ? child->node->pnode->nkeys : 0;
			if(cur->cur_idx == cur->end_idx - c->dir && (cur->edge & END_EDGE)) {
				child->edge |= END_EDGE;
				child->end_idx = bsearch_end(c, child->node);
			}

			if(!is_leaf(c->btree, child->node->pnode))
				child->end_idx += c->dir;
			else if(c->dir < 0) {
				child->end_idx += c->dir;
				child->start_idx += c->dir;
				child->cur_idx += c->dir;
			}
			sp++; //call
		}
		if(cur->cur_idx == cur->end_idx) {
			plat_rwlock_unlock(&cur->node->lock);
			deref_l1cache_node(c->btree, cur->node);
			sp--; //return
		}
		if(!isleaf)
			cur->cur_idx += c->dir;
	}

cleanup:
	if (!btree_in_rescue_mode(c->btree)) {
		plat_rwlock_unlock(&c->btree->lock);

		if (storage_error(ret)) {
			set_lasterror_rquery(c->btree, meta);
		}
	}

	if(!fatal(ret) && !*n_out)
		ret = BTREE_QUERY_DONE;

	if(ret != BTREE_FAILURE && ret != BTREE_QUERY_DONE && !storage_error(ret)) {
		assert(*n_out);
		store_key(&meta->key_start, &meta->keylen_start, values[*n_out - 1].key, values[*n_out - 1].keylen);
		dbg_print_key(values[*n_out - 1].key, values[*n_out - 1].keylen, "last_key");
		meta->flags &= ~(RANGE_START_LE | RANGE_START_LT | RANGE_START_GE | RANGE_START_GT);
		if(ret == BTREE_QUERY_PAUSED)
			meta->flags |= c->dir > 0 ? RANGE_START_GE : RANGE_START_LE;
		else
			meta->flags |= c->dir > 0 ? RANGE_START_GT : RANGE_START_LT;
	}

	while(sp >= 0) {
		plat_rwlock_unlock(&stack[sp].node->lock);
		deref_l1cache_node(c->btree, stack[sp].node);
		sp--; //return
	}

	dbg_print("ret=%d sp=%d n_in=%d n_out=%d\n", ret, sp, n_in, *n_out);

	return ret;
}

btree_status_t
btree_range_query_start_fast(btree_raw_t            *btree,
                             btree_indexid_t         indexid,
                             btree_range_cursor_t    *c,
                             btree_range_meta_t      *rmeta)
{
	int pathcnt;
	btree_raw_mem_node_t node;

	c->dir = IS_ASCENDING_QUERY(rmeta) ? 1 : -1;

	c->ts_key = malloc(btree->max_key_size);
	if (c->ts_key == NULL) {
		assert(0);
		c->ts_key = NULL;
		return BTREE_FAILURE;
	}
	c->ts_keylen = 0;

	if(!store_key(&c->query_meta.key_start, &c->query_meta.keylen_start,
			rmeta->key_start, rmeta->keylen_start))
		return BTREE_FAILURE;

	return BTREE_SUCCESS;
}

static
btree_status_t
btree_range_query_end_fast(btree_range_cursor_t *c)
{
	assert(c != NULL && c->btree != NULL);

	if (c->query_meta.key_start) {
		free(c->query_meta.key_start);
		c->query_meta.key_start = NULL;
	}

	if (c->query_meta.key_end) {
		free(c->query_meta.key_end);
		c->query_meta.key_end = NULL;
	}

	if (c->ts_key) {
		free(c->ts_key);
		c->ts_key = NULL;
	}
	assert(!dbg_referenced);

	return (BTREE_SUCCESS);
}

/* Inplace functions (uses next pointer in the node. ascending only. */

static
int find_end_idx(btree_raw_t* bt, btree_range_cursor_t *c, btree_range_meta_t* rmeta)
{
	btree_raw_node_t *node = c->node->pnode;
	char* key;
	int keylen;
	bool found;
	btree_metadata_t smeta;

	/* Handle open-right-end of a query */
	if(!rmeta->key_end)
		return node->nkeys;

	get_key_val(bt, node, node->nkeys - 1, &key, &keylen);

	int x = bt->cmp_cb(bt->cmp_cb_data, rmeta->key_end, rmeta->keylen_end,
		           key, keylen);

	if(x > 0 || (x == 0 && (rmeta->flags & RANGE_END_LE)))
		return node->nkeys;

	smeta.flags       = rmeta->flags;
	smeta.start_seqno = rmeta->start_seq;
	smeta.end_seqno   = rmeta->end_seq;

	x = bsearch_key_low(bt, node, rmeta->key_end, rmeta->keylen_end, &smeta, 0,
			c->cur_idx - 1, node->nkeys,
			(rmeta->flags & RANGE_END_LT) ? BSF_LATEST: BSF_NEXT,
	                &found);

	return x;
}

btree_status_t
btree_range_query_start_inplace(btree_raw_t                 *btree, 
                                btree_indexid_t         indexid,
                                btree_range_cursor_t    *c,
                                btree_range_meta_t      *rmeta)
{
#if 0
	/*
	 * In place range query us broken with space opt.
	 */
	btree_metadata_t meta;
	int pathcnt = 0;
	node_key_t *ptr = NULL;

	meta.flags = 0;

	plat_rwlock_rdlock(&c->btree->lock);
	ptr = btree_raw_find(c->btree, rmeta->key_start,
			     rmeta->keylen_start, 0, &meta, &c->node, 0 /* shared */,
				 0);

	plat_rwlock_unlock(&c->btree->lock);

	ref_l1cache(c->btree, c->node);

	c->cur_idx = key_idx(c->btree, c->node->pnode, ptr);
	c->end_idx = find_end_idx(c->btree, c, rmeta);

#endif
	assert(0);
	return (BTREE_SUCCESS);
}

static
int
btree_range_get_next_inplace_low(btree_range_cursor_t *c,
                     int                   n_in,
                     int                  *n_out,
                     btree_range_data_t   *values,
                     btree_status_t       *status)
{
	btree_raw_mem_node_t *node = c->node;
	btree_range_meta_t* rmeta = &c->query_meta;
	btree_status_t ret;

	populate_output_array(c, node, &c->cur_idx, c->end_idx, n_in, n_out,
			values, status);

	if(fatal(*status))
		return 0;

	if(c->cur_idx >= c->end_idx && (c->end_idx < c->node->pnode->nkeys - 1 ||
			!c->node->pnode->next))
		*status = BTREE_QUERY_DONE;

	if(*n_out >= n_in || *status == BTREE_QUERY_DONE)
		return 0;

	node = get_existing_node(&ret, c->btree, c->node->pnode->next,
	                         NODE_CACHE_VALIDATE, LOCKTYPE_READ);
	if(!node) {
		*status = ret;
		return 0;
	}

	c->node = node;
	c->cur_idx = 0;
	c->end_idx = find_end_idx(c->btree, c, rmeta);

	return 1;
}

static
btree_status_t
btree_range_get_next_inplace(btree_range_cursor_t *c,
                     int                   n_in,
                     int                  *n_out,
                     btree_range_data_t   *values)
{
	btree_status_t status = BTREE_SUCCESS;

	assert(c != NULL && c->btree != NULL);

	unlock_and_unreference_all_but_last(c->btree);

	*n_out = 0;

	while(btree_range_get_next_inplace_low(c, n_in, n_out, values, &status));

	return status;
}

static
btree_status_t
btree_range_query_end_inplace(btree_range_cursor_t *c)
{
	assert(c != NULL && c->btree != NULL);

	unlock_and_unreference_all(c->btree);

	if (c->query_meta.key_end)
		free(c->query_meta.key_end);

	assert(!dbg_referenced);

	return (BTREE_SUCCESS);
}

/* Start an index query.
 * 
 * Returns: BTREE_SUCCESS if successful
 *          BTREE_FAILURE if unsuccessful
 */
btree_status_t
btree_raw_range_query_start(btree_raw_t             *btree, 
                            btree_indexid_t         indexid,
                            btree_range_cursor_t    **cursor,
                            btree_range_meta_t      *rmeta)
{
	btree_metadata_t  meta;
	btree_status_t       status;
	btree_range_cursor_t *c;

#ifdef DEBUG_STUFF
	if (indexid != BTREE_RANGE_PRIMARY_INDEX) {
		fprintf(stderr, "Index other than primary index is not "
		                "supported yet\n");
		return (BTREE_FAILURE);
	}

	if (rmeta->flags & RANGE_PRIMARY_KEY) {
		fprintf(stderr, "Primary key retrival for secondary index not "
		                "supported yet\n");
		return (BTREE_FAILURE);
	}
#endif

	status = validate_range_query(rmeta);
	if (status != BTREE_SUCCESS) {
		return status;
	}

	int i = 0;
	while(i < N_CURSOR_MAX && __thread_cursor[i].btree)
		i++;

	if(i >= N_CURSOR_MAX) {
		fprintf(stderr, "Limit(%d) on number of concurrent range queries exceeded\n", N_CURSOR_MAX);
		return BTREE_FAILURE;
	}

	c = &__thread_cursor[i];

	/* Initialize the cursor, to accomplish further queries */
	c->btree = btree;
	c->cguid = c->btree->cguid;

	memcpy(&c->query_meta, rmeta, sizeof(btree_range_meta_t));

	c->query_meta.key_end = NULL;
	c->query_meta.key_start = NULL;
	c->dir = 1;

	store_key(&c->query_meta.key_end, &c->query_meta.keylen_end,
			rmeta->key_end, rmeta->keylen_end);

	*cursor = c;

	dbg_print_key(rmeta->key_start, rmeta->keylen_start, "start");
	dbg_print_key(rmeta->key_end, rmeta->keylen_end, "end");

	if(rmeta->flags & RANGE_INPLACE_POINTERS)
	{
		assert(IS_ASCENDING_QUERY(rmeta));
		return btree_range_query_start_inplace(btree, indexid, c, rmeta);
	}

	return  btree_range_query_start_fast(btree, indexid, c, rmeta);
}

/* Get the next set of range from cursor.
 *
 * Input:
 * cursor: Cursor where it is last range query left off. 
 *         Initialized by btree_start_range_query
 * n_in:   Number of entries needed  
 *
 * Output:
 * n_out:  Number of entries actually returned
 * values: Array of Key/Value pair and other details for the given range
 *
 * Returns:
 * Either one of the following status
 * BTREE_SUCCESS      = All successfully done. Output is in values.
 * BTREE_FAILURE      = Error while getting the data.
 * BTREE_QUERY_DONE   = Query is done.
 * BTREE_QUERY_PAUSED = Range Query is paused by callback
 * BTREE_WARNING      = Successfully read, but there are some errors on the way
 *                      that the caller needs to pay attention to.
 *
 * For every return status, the caller needs to check the status is each
 * values[] entry and free up the memory if its status is not 
 * BTREE_RANGE_STATUS_NONE for all n_in values.
 */
btree_status_t
btree_range_get_next(btree_range_cursor_t *c,
                     int                   n_in,
                     int                  *n_out,
                     btree_range_data_t   *values)
{
	if(c->query_meta.flags & RANGE_INPLACE_POINTERS)
		return btree_range_get_next_inplace(c, n_in, n_out, values);

	return btree_range_get_next_fast(c, n_in, n_out, values);
}

btree_status_t
btree_range_query_end(btree_range_cursor_t *c)
{
	btree_status_t status;

	if(c->query_meta.flags & RANGE_INPLACE_POINTERS)
		status = btree_range_query_end_inplace(c);

	status = btree_range_query_end_fast(c);

	c->btree = NULL; // free the cursor

	return status;
}

