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

static __thread btree_range_cursor_t __thread_cursor;
static __thread btree_range_meta_t __thread_meta;

#define IS_ASCENDING_QUERY(meta) \
             (((meta)->key_start && ((meta)->flags & (RANGE_START_GT | RANGE_START_GE))) || \
              ((meta)->key_end && ((meta)->flags & (RANGE_END_LT | RANGE_END_LE))) || \
              (((meta)->key_start == NULL) && ((meta)->key_end == NULL)))

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

/* Populate the values with key/value details for given rec */
static btree_status_t
fill_key_range(btree_raw_t          *bt,
               btree_raw_mem_node_t *n,
               void                 *keyrec,
               btree_range_meta_t   *rmeta,
               btree_range_data_t   *value)
{
	node_vlkey_t      *pvlk;
	btree_status_t status;
	btree_status_t ret = BTREE_SUCCESS;
	uint32_t keybuf_size;
	uint64_t databuf_size;
	uint32_t meta_flags;

	/* TODO: Later on try to combine range_meta and btree_meta
	 * For now, set the meta_flags for get_leaf_data usage */
	meta_flags = rmeta->flags & (RANGE_BUFFER_PROVIDED | RANGE_ALLOC_IF_TOO_SMALL | RANGE_INPLACE_POINTERS);
	if (rmeta->flags & RANGE_BUFFER_PROVIDED) {
		keybuf_size  = rmeta->keybuf_size;
		databuf_size = rmeta->databuf_size;
	}
 
	value->status = BTREE_RANGE_STATUS_NONE; 
	status = get_leaf_key(bt, n->pnode, keyrec,
	                      &value->key,
	                      &keybuf_size,
	                      meta_flags);

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
		status = get_leaf_data(bt, n->pnode, keyrec,
		                       &value->data,
		                       &databuf_size,
		                       meta_flags, 0);

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
			return (BTREE_FAILURE);
		}
		value->datalen = databuf_size;
	}

	pvlk = (node_vlkey_t *) keyrec;
	value->seqno    = pvlk->seqno; 
	value->syndrome = 0;

	if (ret == BTREE_SUCCESS) {
		value->status = BTREE_RANGE_SUCCESS;
	}

	return ret;
}

static inline
int bsearch_end(btree_range_cursor_t *c, btree_raw_mem_node_t *node) {
	btree_range_meta_t* meta = c->query_meta;
	int found;
	if(!meta->key_end)
		return c->dir > 0 ? node->pnode->nkeys : 0;
	else
		return bsearch_key_low(c->btree, node->pnode, meta->key_end,
				meta->keylen_end, 0, -1, node->pnode->nkeys, &found,
				(meta->flags & (RANGE_END_GE | RANGE_END_LT)) ?
				BSF_LEFT : BSF_RIGHT);
}

static inline
int bsearch_start(btree_range_cursor_t *c, btree_raw_mem_node_t *node) {
	btree_range_meta_t* meta = c->query_meta;
	int found;
	if(!meta->key_start)
		return c->dir > 0 ? 0 : node->pnode->nkeys;
	else
		return bsearch_key_low(c->btree, node->pnode, meta->key_start,
				meta->keylen_start, 0, -1, node->pnode->nkeys, &found,
				(meta->flags & (RANGE_START_GE | RANGE_START_LT)) ?
				BSF_LEFT : BSF_RIGHT);
}

static
void
btree_range_find_diversion(btree_range_cursor_t* c)
{
	int x;
	key_stuff_t ks;
	btree_range_meta_t *meta = c->query_meta;
	btree_raw_mem_node_t *parent, *node;
	btree_raw_node_t* pnode;
	btree_status_t ret = BTREE_SUCCESS;
	uint64_t child_id;

	node = root_get_and_lock(c->btree, 0);
	assert(node);

	while(!is_leaf(c->btree, node->pnode)) {
		pnode = node->pnode;

		x = bsearch_start(c, node);

		child_id = pnode->rightmost;
		if(x < pnode->nkeys) {
			get_key_stuff(c->btree, pnode, x, &ks);
			child_id = ks.ptr;
		}

		c->start_idx = x;

		if((c->dir > 0 && x < pnode->nkeys) || (c->dir < 0 && x > 0))
		{
			if(meta->key_end) {
				if(c->dir < 0)
					get_key_stuff(c->btree, pnode, x - 1, &ks);

				x = c->btree->cmp_cb(c->btree->cmp_cb_data, meta->key_end, meta->keylen_end,
					ks.pkey_val, ks.keylen);
			}
			else
				x = c->dir;

			if(c->dir * x > 0 || (!x && (meta->flags & RANGE_END_LE)))
			{
				c->end_idx = bsearch_end(c, node);
				c->end_idx += c->dir;
				c->node = node;
				return;
			}
		}

		parent = node;

		node = get_existing_node_low(&ret, c->btree, child_id, 0);
		assert(BTREE_SUCCESS == ret && node); //FIXME add correct error checking here

		plat_rwlock_rdlock(&node->lock);

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
}

static
btree_status_t
populate_output_array(btree_range_cursor_t *c,
		btree_raw_mem_node_t* node,
		int16_t *cur_idx, int16_t end_idx, int n_in, int *n_out,
		btree_range_data_t* values)
{
	btree_status_t ret = BTREE_SUCCESS, status = BTREE_SUCCESS;

	dbg_print("cur_idx=%d end_idx=%d n_in=%d n_out=%d\n", (int)*cur_idx, (int)end_idx, n_in, *n_out);

	while (*cur_idx != end_idx && *n_out < n_in && !fatal(status))
	{
		ret = fill_key_range(c->btree, node, key_offset(c->btree,
						node->pnode, *cur_idx), c->query_meta, values + *n_out);

		if(ret != BTREE_SUCCESS)
			status = ret;

		if(ret != BTREE_FAILURE) {
			(*n_out)++;
			if(ret != BTREE_QUERY_PAUSED)
				(*cur_idx) += c->dir;
		}
	}

	dbg_print("return cur_idx=%d end_idx=%d n_in=%d n_out=%d status=%d\n", (int)*cur_idx, (int)end_idx, n_in, *n_out, status);

	return status;
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

	key_stuff_t ks;
	btree_range_meta_t *meta = c->query_meta;
	btree_status_t ret = BTREE_SUCCESS, r = BTREE_SUCCESS;
	int sp = 0;

	*n_out = 0;

	plat_rwlock_rdlock(&c->btree->lock);

	btree_range_find_diversion(c);

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

			ret = populate_output_array(c, cur->node, &cur->cur_idx, cur->end_idx, n_in, n_out, values);
		} else if(cur->cur_idx != cur->end_idx) {
			uint64_t logical_id = cur->node->pnode->rightmost;
			if(cur->cur_idx < cur->node->pnode->nkeys) {
				get_key_stuff(c->btree, cur->node->pnode, cur->cur_idx, &ks);
				logical_id = ks.ptr;
			}

			child->node = get_existing_node_low(&r, c->btree, logical_id, 0);
			assert(child->node);

			plat_rwlock_rdlock(&child->node->lock);

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

	plat_rwlock_unlock(&c->btree->lock);

	if(!fatal(ret) && !*n_out)
		ret = BTREE_QUERY_DONE;

	if(ret != BTREE_FAILURE && ret != BTREE_QUERY_DONE) {
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
btree_range_query_start_fast(btree_t            *btree,
                        btree_indexid_t         indexid,
                        btree_range_cursor_t    *c,
                        btree_range_meta_t      *rmeta)
{
	int pathcnt;
	btree_raw_mem_node_t node;

	c->dir = IS_ASCENDING_QUERY(rmeta) ? 1 : -1;

	if(!store_key(&c->query_meta->key_start, &c->query_meta->keylen_start,
			rmeta->key_start, rmeta->keylen_start))
		return BTREE_FAILURE;

	return BTREE_SUCCESS;
}

static
btree_status_t
btree_range_query_end_fast(btree_range_cursor_t *c)
{
	assert(c != NULL && c->btree != NULL);

	if (c->query_meta->key_start) {
		free(c->query_meta->key_start);
		c->query_meta->key_start = NULL;
	}

	if (c->query_meta->key_end) {
		free(c->query_meta->key_end);
		c->query_meta->key_end = NULL;
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
	int keylen, found;

	/* Handle open-right-end of a query */
	if(!rmeta->key_end)
		return node->nkeys;

	get_key_val(bt, node, node->nkeys - 1, &key, &keylen);

	int x = bt->cmp_cb(bt->cmp_cb_data, rmeta->key_end, rmeta->keylen_end,
		           key, keylen);

	if(x > 0 || (x == 0 && (rmeta->flags & RANGE_END_LE)))
		return node->nkeys;

	x = bsearch_key_low(bt, node, rmeta->key_end, rmeta->keylen_end, 0,
			c->cur_idx - 1, node->nkeys, &found,
			(rmeta->flags & RANGE_END_LT) ? BSF_LEFT : BSF_RIGHT);

	return x;
}

btree_status_t
btree_range_query_start_inplace(btree_t                 *btree, 
                        btree_indexid_t         indexid,
                        btree_range_cursor_t    *c,
                        btree_range_meta_t      *rmeta)
{
    btree_metadata_t meta;

	meta.flags = 0;
	int pathcnt;

	plat_rwlock_rdlock(&c->btree->lock);

	node_key_t* ptr = btree_raw_find(c->btree, rmeta->key_start,
			rmeta->keylen_start, 0, &meta, &c->node, 0 /* shared */,
			&pathcnt, (rmeta->flags & RANGE_START_GT) ? BSF_RIGHT : BSF_LEFT);

	plat_rwlock_unlock(&c->btree->lock);

	ref_l1cache(c->btree, c->node);

	c->cur_idx = key_idx(c->btree, c->node->pnode, ptr);
	c->end_idx = find_end_idx(c->btree, c, rmeta);

	return (BTREE_SUCCESS);
}

static
btree_status_t
btree_range_get_next_inplace_low(btree_range_cursor_t *c,
                     int                   n_in,
                     int                  *n_out,
                     btree_range_data_t   *values,
                     btree_status_t       *status)
{
	btree_raw_mem_node_t *node = c->node;
	btree_range_meta_t* rmeta = c->query_meta;
	btree_status_t ret = BTREE_SUCCESS;

	ret = populate_output_array(c, node, &c->cur_idx, c->end_idx, n_in, n_out,
			values);

	if(fatal(ret)) {
		*status = ret;
		return 0;
	}

	if(c->cur_idx >= c->end_idx && (c->end_idx < c->node->pnode->nkeys - 1 ||
			!c->node->pnode->next))
		*status = BTREE_QUERY_DONE;

	if(*n_out >= n_in || *status == BTREE_QUERY_DONE)
		return 0;

	node = get_existing_node(&ret, c->btree, c->node->pnode->next);
	if(!node) {
		*status = BTREE_FAILURE;
		return 0;
	}

	plat_rwlock_rdlock(&node->lock);

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

	if (c->query_meta->key_end)
		free(c->query_meta->key_end);

	assert(!dbg_referenced);

	return (BTREE_SUCCESS);
}

/* Start an index query.
 * 
 * Returns: BTREE_SUCCESS if successful
 *          BTREE_FAILURE if unsuccessful
 */
btree_status_t
btree_range_query_start(btree_t                 *btree, 
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

	c = &__thread_cursor;

	/* Initialize the cursor, to accomplish further queries */
	int n_partition = 0;
	c->btree = btree->partitions[n_partition];

	c->query_meta = &__thread_meta;
	memcpy(c->query_meta, rmeta, sizeof(btree_range_meta_t));

	c->query_meta->key_end = NULL;
	c->query_meta->key_start = NULL;
	c->dir = 1;

	store_key(&c->query_meta->key_end, &c->query_meta->keylen_end,
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
	if(c->query_meta->flags & RANGE_INPLACE_POINTERS)
		return btree_range_get_next_inplace(c, n_in, n_out, values);

	return btree_range_get_next_fast(c, n_in, n_out, values);
}

btree_status_t
btree_range_query_end(btree_range_cursor_t *c)
{
	if(c->query_meta->flags & RANGE_INPLACE_POINTERS)
		return btree_range_query_end_inplace(c);

	return btree_range_query_end_fast(c);
}

