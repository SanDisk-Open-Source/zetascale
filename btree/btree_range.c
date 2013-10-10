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

static int
find_first_greater_key_index(btree_raw_t *bt, 
                             btree_raw_node_t *n,
                             int i_start, int i_end,
                             char *key, uint32_t keylen)
{
	int i, x = 0, i_centre = 0, tkeylen;
	char    *tkey = NULL;

#ifdef DEBUG_STUFF
	for(i = 0; i <= i_end;i++)
	{
		(void) get_key_val(bt, n, i, &tkey, &tkeylen);
		dbg_print_key(tkey, tkeylen, "i=%d", i);
	}
#endif

	while (i_start <= i_end) {
		i_centre = (i_start + i_end)/2;

		(void) get_key_val(bt, n, i_centre, &tkey, &tkeylen);

		dbg_print_key(tkey, tkeylen, "compare i_center=%d i_start=%d i_end=%d", i_centre, i_start, i_end);

		x = bt->cmp_cb(bt->cmp_cb_data, key, keylen,
		                    tkey, tkeylen);
		if (x < 0)
			i_end = i_centre - 1;
		else {
			i_start = i_centre + 1;
			i_centre += 1;
		}
	}

#ifdef DEBUG_STUFF
	(void) get_key_val(bt, n, i_centre, &tkey, &tkeylen);
	dbg_print_key(tkey, tkeylen, "i_center=%d i_start=%d i_end=%d x=%d", i_centre, i_start, i_end, x);
#endif
	return i_centre;
}

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
			RANGE_END_LT ? BSF_LEFT : BSF_RIGHT);

	return x;
}

/* Start an index query.
 * 
 * Returns: BTREE_SUCCESS if successful
 *          BTREE_FAILURE if unsuccessful
 */
btree_status_t
btree_start_range_query(btree_t                 *btree, 
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

	if (rmeta->key_end) {
		c->query_meta->key_end  = (char *)malloc(rmeta->keylen_end);
		if (c->query_meta->key_end == NULL) {
			assert(0);
			return BTREE_FAILURE;
		}
		memcpy(c->query_meta->key_end, rmeta->key_end, 
		       rmeta->keylen_end);
	}

	plat_rwlock_rdlock(&c->btree->lock);

	meta.flags = 0;
	int pathcnt;

	node_key_t* ptr = btree_raw_find(c->btree, rmeta->key_start,
			rmeta->keylen_start, 0, &meta, &c->node, 0 /* shared */,
			&pathcnt, RANGE_START_GT ? BSF_RIGHT : BSF_LEFT);

	ref_l1cache(c->btree, c->node);

    dbg_print_key(rmeta->key_start, rmeta->keylen_start, "start ptr=%p", ptr);
    dbg_print_key(rmeta->key_end, rmeta->keylen_end, "end");

	c->cur_idx = key_idx(c->btree, c->node->pnode, ptr);
	c->end_idx = find_end_idx(c->btree, c, rmeta);

	dbg_print("cur_idx=%d end_idx=%d\n", c->cur_idx, c->end_idx);

	*cursor = c;

	dbg_print("leave\n");

	return (BTREE_SUCCESS);
}

/* Populate the values with key/value details for given rec */
static btree_status_t
fill_key_range(btree_raw_t          *bt,
               btree_raw_mem_node_t *n,
               void                 *keyrec,
               btree_range_meta_t   *rmeta,
               btree_range_data_t   *values,
               int                  *n_out)
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
 
	values[*n_out].status = BTREE_RANGE_STATUS_NONE; 
	status = get_leaf_key(bt, n->pnode, keyrec,
	                      &values[*n_out].key,
	                      &keybuf_size,
	                      meta_flags);

	dbg_print_key(values[*n_out].key, keybuf_size, "key");

	switch (status) {
	case BTREE_SUCCESS:
		break;
	case BTREE_BUFFER_TOO_SMALL:
		ret = BTREE_WARNING;
		values[*n_out].status |= BTREE_KEY_BUFFER_TOO_SMALL;
		break;
	case BTREE_FAILURE:
	default:
		values[*n_out].status |= BTREE_FAILURE;
		return (BTREE_FAILURE);
	}
	values[*n_out].keylen = keybuf_size;

	/* Check if we need to pause at this key */
	if (rmeta->allowed_fn) {
		if (!rmeta->allowed_fn(rmeta->cb_data, values[*n_out].key,
		                       values[*n_out].keylen)) {
			values[*n_out].status |= BTREE_RANGE_PAUSED;
			return (BTREE_QUERY_PAUSED);
		}
	}

	if (!(rmeta->flags & RANGE_KEYS_ONLY)) {
		status = get_leaf_data(bt, n->pnode, keyrec,
		                       &values[*n_out].data,
		                       &databuf_size,
		                       meta_flags, 0);

		switch (status) {
		case BTREE_SUCCESS:
			break;
		case BTREE_BUFFER_TOO_SMALL:
			ret = BTREE_WARNING;
			values[*n_out].status |= BTREE_DATA_BUFFER_TOO_SMALL;
			break;
		case BTREE_FAILURE:
		default:
			values[*n_out].status |= BTREE_FAILURE;
			return (BTREE_FAILURE);
		}
		values[*n_out].datalen = databuf_size;
	}

	pvlk = (node_vlkey_t *) keyrec;
	values[*n_out].seqno    = pvlk->seqno; 
	values[*n_out].syndrome = 0;

	if (ret == BTREE_SUCCESS) {
		values[*n_out].status = BTREE_RANGE_SUCCESS;
	}

	return ret;
}

static inline int fatal(btree_status_t s)
{
	return s != BTREE_SUCCESS && s != BTREE_WARNING && s != BTREE_QUERY_PAUSED;
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
static
btree_status_t
btree_get_next_range_low(btree_range_cursor_t *c,
                     int                   n_in,
                     int                  *n_out,
                     btree_range_data_t   *values,
                     btree_status_t       *status)
{
	btree_raw_mem_node_t *node = c->node;
	btree_range_meta_t* rmeta = c->query_meta;
	btree_status_t ret = BTREE_SUCCESS;

	dbg_print("enter %d %d %d %d\n", c->cur_idx, c->end_idx, *n_out, n_in);

	while (c->cur_idx < c->end_idx && *n_out < n_in &&
			!fatal(ret = fill_key_range(c->btree, node, key_offset(c->btree,
						node->pnode, c->cur_idx), rmeta, values, n_out)))
	{
		if(ret != BTREE_SUCCESS)
			*status = ret;
		(*n_out)++;
		c->cur_idx++;
	}

	if(fatal(ret))
	{
		*status = ret;
		return 0;
	}

	if(c->cur_idx >= c->end_idx && c->end_idx < c->node->pnode->nkeys - 1
			|| !c->node->pnode->next)
		*status = BTREE_QUERY_DONE;

	if(*n_out >= n_in || *status == BTREE_QUERY_DONE)
		return 0;

	dbg_print("next_node=%ld\n", c->node->pnode->next);

	node = get_existing_node(&ret, c->btree, c->node->pnode->next);
	if(!node)
	{
		*status = BTREE_FAILURE;
		return 0;
	}

	plat_rwlock_rdlock(&node->lock);

	c->node = node;
	c->cur_idx = 0;
	c->end_idx = find_end_idx(c->btree, c, rmeta);

	dbg_print("leave cont\n");

	return 1;
}

btree_status_t
btree_get_next_range(btree_range_cursor_t *c,
                     int                   n_in,
                     int                  *n_out,
                     btree_range_data_t   *values)
{
	btree_status_t status = BTREE_SUCCESS;

	dbg_print("enter\n");
	assert(c != NULL && c->btree != NULL);

	unlock_and_unreference_all_but_last(c->btree);

	*n_out = 0;

	while(btree_get_next_range_low(c, n_in, n_out, values, &status));

	dbg_print("leave %d\n", status);

	return status;
}

btree_status_t
btree_end_range_query(btree_range_cursor_t *c)
{
	dbg_print("enter\n");
	assert(c != NULL && c->btree != NULL);

	unlock_and_unreference_all(c->btree);

	plat_rwlock_unlock(&c->btree->lock);

	if (c->query_meta->key_end)
		free(c->query_meta->key_end);

	assert(!dbg_referenced);

	return (BTREE_SUCCESS);
}
