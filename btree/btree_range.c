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
#include "btree_list.h"

#ifdef _OPTIMIZE
#undef assert
#define assert(a)
#endif

//  Define this to include detailed debugging code
#define DEBUG_STUFF

#define IS_ASCENDING_QUERY(meta) \
             (((meta)->key_start && ((meta)->flags & (RANGE_START_GT | RANGE_START_GE))) || \
              ((meta)->key_end && ((meta)->flags & (RANGE_END_LT | RANGE_END_LE))) || \
              (((meta)->key_start == NULL) && ((meta)->key_end == NULL)))

#define LEFT_EDGE     1
#define RIGHT_EDGE    2

typedef struct range_key_list {
	btree_raw_mem_node_t *n;
	plat_rwlock_t    *node_lock;
	int              key_count;
	int              next_read_ind;         
	int              pos_flag;
	node_key_t       *keyrecs[0];
} range_key_list_t;

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

#if 0
static int range_cmp(btree_raw_t *bt, char *cmp, uint32_t cmplen, 
                     char *start, uint32_t startlen, int start_incl, 
                     char *end, uint32_t endlen, int end_incl)
{
	int x;
	int y;

	if (start != NULL) {
		x = bt->cmp_cb(bt->cmp_cb_data, cmp, cmplen, start, startlen);
	} else {
		x = 1;  /* If no lower, then given number is above that */
	}

	if (end != NULL) {
		y = bt->cmp_cb(bt->cmp_cb_data, cmp, cmplen, end, endlen);
	} else {
		y = -1; /* If no upper bound, number is less than that */
	}

	if ((x > 0) && (y < 0)) {
		return 0;   /* Value in range */
	} else if((x == 0) && (y < 0)) {
		return (start_incl ? 0 : -1); 
	} else if((x <  0) && (y < 0)) {
		return -1; /* number is lesser to start and end */
	} else if((x >  0) && (y == 0)) {
		return (end_incl ? 0 : 1);
	} else if((x == 0) && (y == 0)) {
		/* start and end matches. range only if both has
		 * inclusive, else it is exclusive */
		return ((start_incl && end_incl) ? 0: 1);
	} else if((x <  0) && (y == 0)) {
		/* Error condition, start is lesser, but matches end */
		return 1;
	} else if((x >  0) && (y > 0)) {
		return 1; /* genuine case where number is right of range */
	} else if((x == 0) && (y > 0)) {
		/* Error condition, start is lesser, but matches end */
		return 1;
	} else { /* ((x <  0) && (y > 0)) */
		/* somehow lower is bigger then upper,
		 * return 1, so that further walk stops */
		return 1;
	}
}
#endif

static int is_seqno_in_range(btree_range_meta_t *rmeta, key_stuff_t *ks)
{
	if (rmeta->flags & RANGE_SEQNO_LE) {
		return (ks->seqno <= rmeta->end_seq);
	} else if (rmeta->flags & RANGE_SEQNO_GT_LE) {
		return ((ks->seqno > rmeta->start_seq)) &&
		        ((ks->seqno <= rmeta->end_seq));
	} else {
		return 1; /* No range specified, so its accepted */
	}
}

static int
find_first_greater_key_index(btree_raw_t *bt, 
                             btree_raw_node_t *n,
                             char *key, uint32_t keylen, 
                             int match, node_key_t **match_pk)
{
	int i_start, i_end, i_centre;
	int x;
	int first_index;
	node_key_t    *pk = NULL;
	key_stuff_t    ks;

	i_start = 0;
	i_end   = n->nkeys - 1;
	first_index = n->nkeys;

	if (match_pk) *match_pk = NULL;

	while (i_start <= i_end) {
		i_centre = (i_start + i_end)/2;

		(void) get_key_stuff(bt, n, i_centre, &ks);
		pk  = ks.pkey_struct;

		x = bt->cmp_cb(bt->cmp_cb_data, key, keylen,
		                    ks.pkey_val, ks.keylen);
		if (x == 0) {
			if (match) {
				if (match_pk) *match_pk = pk;
				return (i_centre);
			}

			/* non-match, key should be considered
			 * right of given centre */
			x = 1;
		}

		if (x < 0) {
			first_index = i_centre;
			i_end = i_centre - 1;
		} else { /* x > 0 */
			i_start = i_centre + 1;
		}
	}

	return (first_index);
}

#define GET_PTR_FROM_KEY(flags, pk) \
	(flags & SYNDROME_INDEX) ? \
	    (node_key_t *)(uint64_t)(((node_fkey_t *)pk)->ptr): \
	    (node_key_t *)(uint64_t)(((node_vkey_t *)pk)->ptr)

static void
add_to_key_list(btree_raw_t *bt,
                btree_raw_node_t *n,
                btree_range_meta_t *rmeta,
                key_stuff_t *pks,
                range_key_list_t *klist,
                int index)
{
	node_key_t *pk = NULL;

	if (pks != NULL) {
		pk  = pks->pkey_struct;
	}

	if (!is_leaf(bt, n)) {
		if (index == n->nkeys) {
			klist->keyrecs[klist->key_count++] = 
			                    (node_key_t *)n->rightmost;
		} else {
			assert(pk);
			klist->keyrecs[klist->key_count++] = 
		                  GET_PTR_FROM_KEY(bt->flags, pk);
		}
	} else if (is_seqno_in_range(rmeta, pks)) {
		assert(pk);
		klist->keyrecs[klist->key_count++] = pk;
	}
}

static void 
add_all_keys_to_list(btree_raw_t *bt,
                     btree_raw_node_t *n,
                     btree_range_meta_t *rmeta,
                     range_key_list_t *klist,
                     int max_keys,
                     int lo_to_hi)
{
	int i_cur;
	key_stuff_t ks;

	if (lo_to_hi) {
		for (i_cur = 0; i_cur < n->nkeys; i_cur++) {
			if (klist->key_count == max_keys) return;
			(void) get_key_stuff(bt, n, i_cur, &ks);
			add_to_key_list(bt, n, rmeta, &ks, klist, i_cur);
		}

		if (!is_leaf(bt, n)) {
			if (klist->key_count == max_keys) return;
			add_to_key_list(bt, n, rmeta, NULL, klist, n->nkeys);
		}
	} else {
		if (!is_leaf(bt, n)) {
			if (klist->key_count == max_keys) return;
			add_to_key_list(bt, n, rmeta, NULL, klist, n->nkeys);
		}

		for (i_cur = n->nkeys-1; i_cur >=0; i_cur--) {
			if (klist->key_count == max_keys) return;
			(void) get_key_stuff(bt, n, i_cur, &ks);
			add_to_key_list(bt, n, rmeta, &ks, klist, i_cur);
		}
	}
}

static void
find_key_range_asc(btree_raw_t *bt, 
                   btree_raw_node_t *n, 
                   btree_range_meta_t *rmeta,
                   range_key_list_t *klist,
                   int max_keys)
{
	int            x = 0;
	int            i_cur;
	key_stuff_t    ks;
	int           is_leaf_node;
	int           push_last_node;
	int           first_index;

	/* Already reached the max keys needed, return */
	if (klist->key_count >= max_keys) {
		return;
	} 

	is_leaf_node = is_leaf(bt, n);

	/* Find the start of the range */
	if (rmeta->key_start) {
		first_index = find_first_greater_key_index(bt, n, 
		                                rmeta->key_start, 
		                                rmeta->keylen_start,
		                                rmeta->flags & RANGE_START_GE,
		                                NULL);
	} else {
		first_index = 0;
	}

	/* lower key is beyond this node's range, it could be in the rightmost for
	 * non-leaf nodes */
	if (first_index == n->nkeys) {
		if (!is_leaf_node) {
			add_to_key_list(bt, n, rmeta, &ks, klist, first_index);
		}
		return;
	}

	/* Start searching for end condition from start index onwards */
	i_cur = first_index;
	push_last_node = 1;

	while (i_cur < n->nkeys) {
		if (klist->key_count >= max_keys) {
			break;
		} 

		(void) get_key_stuff(bt, n, i_cur, &ks);

		if (rmeta->key_end) {
			x = bt->cmp_cb(bt->cmp_cb_data, 
			               rmeta->key_end, rmeta->keylen_end,
			               ks.pkey_val, ks.keylen);
		} else {
			x = 1; /* end is null - all keys are left to it */
		}

		/* Either key less than range end or equal with incl set */
		if ((x > 0) || ((x == 0) && (rmeta->flags & RANGE_END_LE))) {
			add_to_key_list(bt, n, rmeta, &ks, klist, i_cur);

			/* If exact matches, non-leaf nodes need not write last node*/
			if (x == 0) {
				push_last_node = 0;
				break;
			}
		} else { /* End condition: key greater or equal nonincl reached */
			break;
		}
		i_cur++;
	}

	if ((!is_leaf_node) && push_last_node &&
	     (klist->key_count < max_keys)) {
		add_to_key_list(bt, n, rmeta, &ks, klist, i_cur);
	}
}

static void
find_key_range_des(btree_raw_t *bt, 
                   btree_raw_node_t *n, 
                   btree_range_meta_t *rmeta,
                   range_key_list_t *klist,
                   int max_keys)
{
	int            x = 0;
	int            i_cur;
	node_key_t    *pk = NULL;
	key_stuff_t    ks;
	int           is_leaf_node;
	int           first_index;

	is_leaf_node = is_leaf(bt, n);

	/* Already reached the max keys needed, return */
	if (klist->key_count >= max_keys) {
		return;
	} 

	/* Find the start of the range */
	if (rmeta->key_start) {
		first_index = find_first_greater_key_index(bt, n, 
		                                       rmeta->key_start, 
		                                       rmeta->keylen_start,
		                                       1, &pk);
	} else {
		first_index = n->nkeys;
	}

	if (is_leaf_node) {
		/* Unless there is an exact match and start condition
		 * says we need less than or equal to, leaf node should
		 * skip the bigger node, which is returned in
		 * find_first_greater call. */
		if ((pk == NULL) || !(rmeta->flags & RANGE_START_LE)) {
			first_index--;
		}
	}

	/* If this is beyond last key, then this key is valid (rightmost)
	 * only for non-leaf nodes */
	if (first_index == n->nkeys) {
		/* If there is an exact match, only include the rightmost
		 * if explicitly asked for */
		if ((pk && (rmeta->flags & RANGE_START_LE)) || (!pk)) {
			add_to_key_list(bt, n, rmeta, NULL, klist, first_index);
		}
		first_index--; /* Start with the last key */
	}

	i_cur = first_index;
	while (i_cur >= 0) {
#if 0
		if (klist->key_count >= max_keys) {
			break;
		} 
#endif

		(void) get_key_stuff(bt, n, i_cur, &ks);
		pk  = ks.pkey_struct;

		if (rmeta->key_end) {
			x = bt->cmp_cb(bt->cmp_cb_data, 
			               rmeta->key_end, rmeta->keylen_end,
			               ks.pkey_val, ks.keylen);
		} else {
			x = -1;
		}

		if ((x < 0) || ((x == 0) && (rmeta->flags & RANGE_END_GE))) {
			add_to_key_list(bt, n, rmeta, &ks, klist, i_cur);
		} else { /* End condition: key lesser or equal nonincl reached */
			break;
		}
		i_cur--;
	}
}

/*
 * Find the keys in the node for the range provided. Returns total keys found between the
 * the range. The resulted key structures are put in the key_list structure. It will be
 * put in the same order as it is requested for.
 */
static range_key_list_t *
find_key_range(btree_raw_t *bt,
               btree_raw_mem_node_t *node,
               btree_range_meta_t *rmeta,
               int max_keys,
               int all_keys)
{
	btree_raw_node_t *n;
	range_key_list_t *klist;

	n = node->pnode;
	if (!IS_ASCENDING_QUERY(rmeta) && !all_keys) {
		max_keys = -1;
	}
		
	if ((max_keys == -1) || (max_keys > (n->nkeys + 1))) {
		max_keys = n->nkeys + 1;
	}

	/* Allocate for header portion and all nodes which will comprise
	 * the keys */
	klist = (range_key_list_t *)malloc(sizeof(range_key_list_t) + 
	                                   sizeof(node_key_t *) * max_keys);
	if (klist == NULL) {
		assert(0);
		return NULL;
	}

	klist->n = node;
	klist->node_lock = &node->lock;
	klist->key_count = 0;
	klist->next_read_ind = 0;
	klist->pos_flag = 0;

	if (n->nkeys == 0) {
		return klist;
	}

	if (IS_ASCENDING_QUERY(rmeta)) {
		if (all_keys) {
			add_all_keys_to_list(bt, n, rmeta, klist, max_keys, 1);
		} else {
			find_key_range_asc(bt, n, rmeta, klist, max_keys);
		}
	} else {
		if (all_keys) {
			add_all_keys_to_list(bt, n, rmeta, klist, max_keys, 0);
		} else {
			find_key_range_des(bt, n, rmeta, klist, max_keys);
		}
	}
	return klist;
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
	btree_status_t       status;
	btree_range_cursor_t *cr;

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

	cr = (btree_range_cursor_t *)malloc(sizeof(btree_range_cursor_t));
	if (cr == NULL) {
		/* TODO: Log error message */
		return BTREE_FAILURE;
	}

	/* Initialize the cursor, to accomplish further queries */
	cr->btree        = btree;
	cr->query_meta   = malloc(sizeof(btree_range_meta_t));
	if (cr->query_meta == NULL) {
		assert(0);
		free(cr);
		return BTREE_FAILURE;
	}
	memcpy(cr->query_meta, rmeta, sizeof(btree_range_meta_t));

	if (rmeta->key_start) {
		cr->query_meta->key_start = (char *)malloc(rmeta->keylen_start);
		if (cr->query_meta->key_start == NULL) {
			assert(0);
			free(cr->query_meta);
			free(cr);
			return BTREE_FAILURE;
		}
		memcpy(cr->query_meta->key_start, rmeta->key_start, 
		       rmeta->keylen_start);
	}

	if (rmeta->key_end) {
		cr->query_meta->key_end  = (char *)malloc(rmeta->keylen_end);
		if (cr->query_meta->key_end == NULL) {
			assert(0);
			free(cr->query_meta->key_start);
			free(cr->query_meta);
			free(cr);
			return BTREE_FAILURE;
		}
		memcpy(cr->query_meta->key_end, rmeta->key_end, 
		       rmeta->keylen_end);
	}

	cr->indexid      = indexid;
	cr->last_key     = NULL;
	cr->last_keylen  = 0;
	cr->last_status  = BTREE_SUCCESS;

	*cursor = cr;
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
	meta_flags = rmeta->flags & (RANGE_BUFFER_PROVIDED | RANGE_ALLOC_IF_TOO_SMALL);
	if (rmeta->flags & RANGE_BUFFER_PROVIDED) {
		keybuf_size  = rmeta->keybuf_size;
		databuf_size = rmeta->databuf_size;
	}

	values[*n_out].status = BTREE_RANGE_STATUS_NONE; 
	status = get_leaf_key(bt, n->pnode, keyrec,
	                      &values[*n_out].key,
	                      &keybuf_size,
	                      meta_flags);
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

extern __thread uint64_t dbg_referenced;
//#define dbg_print(msg, ...) do { fprintf(stderr, "%x %s:%d " msg, (int)pthread_self(), __FUNCTION__, __LINE__, ##__VA_ARGS__); } while(0)
#define dbg_print(msg, ...)

static btree_raw_mem_node_t *
get_root_node_locked_low(btree_raw_t *bt, int ref)
{
	uint64_t child_id;
	btree_raw_mem_node_t *n;
	btree_status_t ret = BTREE_SUCCESS;

restart:
	child_id = bt->rootid;
	n = get_existing_node_low(&ret, bt, child_id, ref);
	if (n == NULL) {
		assert(0);
		return NULL;
	}

	plat_rwlock_rdlock(&n->lock);
	if (child_id != bt->rootid) {
		plat_rwlock_unlock(&n->lock);
		deref_l1cache_node(bt, n);
		goto restart;
	}

	return n;
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
btree_get_next_range(btree_range_cursor_t *cursor,
                     int                   n_in,
                     int                  *n_out,
                     btree_range_data_t   *values)
{

	uint64_t          ptr;
	btree_raw_t      *bt;
	btree_raw_mem_node_t *n;
	btree_raw_mem_node_t *child_n;
	blist_t          *master_list;
	range_key_list_t *klist;
	range_key_list_t *child_klist;
	int               pathcnt = 1;
	btree_range_meta_t rmeta;
	btree_status_t    overall_status;
	btree_status_t    status;
	btree_status_t    ret = BTREE_SUCCESS;
	node_key_t        *keyrec = NULL;
	int               key_index;
	int               cur_key_ind;
	int               all_keys;
	int               left_edge, right_edge;

	/* TODO: Handle non-debug failures */
	assert(cursor != NULL);
	assert(cursor->btree != NULL);

	/* TODO: Take the lock and also clean this up */
	int n_partition = 0;
	bt = cursor->btree->partitions[n_partition];
	*n_out = 0;

	rmeta = *(cursor->query_meta);

	/* If last key is there, we need to search for next onwards.
	 * TODO: This will not work for non-unique keys, since it will 
	 * skip the matched keys, but n_in does not hold all records.
	 * Need a more smarter way to  remember the key pointer.
	 */
	if (cursor->last_key) {
		rmeta.key_start = cursor->last_key;
		rmeta.keylen_start = cursor->last_keylen;

		rmeta.flags &= ~(RANGE_START_GT | RANGE_START_GE | 
		                 RANGE_START_LE | RANGE_START_LT);
		if (IS_ASCENDING_QUERY(cursor->query_meta)) { 
			rmeta.flags |= RANGE_START_GT;
		} else {
			rmeta.flags |= RANGE_START_LT;
		}
	}

	key_index = -1;

	master_list = blist_init();
	if (master_list == NULL) {
		overall_status = BTREE_FAILURE;
		goto done;
	}

	plat_rwlock_rdlock(&bt->lock);

	n = get_root_node_locked_low(bt, 0);
	if (n == NULL) {
		overall_status = BTREE_FAILURE;
		goto done;
	}

	klist = find_key_range(bt, n, &rmeta, n_in, 0);
	if (klist == NULL) {
		plat_rwlock_unlock(&n->lock);
		deref_l1cache_node(bt, n);
		overall_status = BTREE_FAILURE;
		goto done;
	}
		
	blist_push_node_from_head(master_list, (void *)klist);
	if ((klist->key_count == 0) || (*n_out == n_in)) {
		overall_status = BTREE_QUERY_DONE;
		goto done;
	}

	left_edge = right_edge = 1;
	klist->pos_flag = LEFT_EDGE | RIGHT_EDGE;
	overall_status = BTREE_SUCCESS;

	/* Need to remove elements from head, to maintain order */
	while (blist_get_head_node_data(master_list, (void *)&klist)) {
		assert(klist);

		/* Initialize loop variants */
		n = klist->n;
		child_klist = NULL;

		/* By default put all keys in range query (middle node) */
		all_keys = 1;

		if (klist->key_count == 0) {
			/* Nothing to process, clean it up */
			goto key_cleanup;
		}

		cur_key_ind = klist->next_read_ind++;
		if (is_leaf(bt, n->pnode)) {
			keyrec = klist->keyrecs[cur_key_ind];

			status = fill_key_range(bt, n, 
			                        (void *)keyrec,
			                        &rmeta,
			                        values, 
			                        n_out);
			switch (status) {
			case BTREE_WARNING:
				overall_status = status; /* Fallthrough */
			case BTREE_SUCCESS:
				key_index = *n_out;
				(*n_out)++;
				break;
			case BTREE_FAILURE:
			case BTREE_QUERY_PAUSED:
			default:
				overall_status = status;
				(*n_out)++;
				goto done;
			}
		} else {
			left_edge = right_edge = 0;

			ptr = (uint64_t)klist->keyrecs[cur_key_ind];
			ret = 0;

			/* Get the node corresponding to this pointer and leaf
			 * lock for this node */
			child_n = get_existing_node_low(&ret, bt, ptr, 0);
			if (ret) {
				assert(0);
				goto key_cleanup;
			}

			/* If we are at the edge, should do range check */
			if ((cur_key_ind == 0) && 
			    (klist->pos_flag & LEFT_EDGE)) {
				left_edge = 1;
				all_keys = 0;
			}

			if ((cur_key_ind == klist->key_count - 1) && 
			    (klist->pos_flag & RIGHT_EDGE)) {
				right_edge = 1;
				all_keys = 0;
			}

			plat_rwlock_rdlock(&child_n->lock);
			child_klist = find_key_range(bt, child_n,
			                             &rmeta,
			                             (n_in - (*n_out)), 
			                             all_keys);
			if (child_klist == NULL) {
				plat_rwlock_unlock(&child_n->lock);
			}
		}

key_cleanup:
		if (n_in == *n_out) {
			break;
		}

		/* If we have read all data from this node, we no longer 
		 * need it and hence can be unlocked and derefed */
		if (klist->next_read_ind == klist->key_count) {
			assert(n == klist->n);

			/* Unlock nodelock which was locked by find_key_range */
			plat_rwlock_unlock(&klist->n->lock);

			/* We are done using the node */
			deref_l1cache_node(bt, n);

			(void)blist_pop_node_from_head(master_list, NULL);
			free(klist);
		}

		/* Time to add the child node to the list */
		if (child_klist != NULL) {
			/* Propogate the left/right edge property to the
			 * child */
			if (left_edge) child_klist->pos_flag |= LEFT_EDGE;
			if (right_edge) child_klist->pos_flag |= RIGHT_EDGE;

			blist_push_node_from_head(master_list, 
			                          (void *)child_klist);
		}
	}

done:
	if (master_list) {
		/* deref all touched nodes, in case we broke the earlier loop */
		while (blist_pop_node_from_head(master_list, (void *)&klist)) {
			assert(klist);

			/* Initialize loop variants */
			n = klist->n;
			plat_rwlock_unlock(&n->lock);

			deref_l1cache_node(bt, n);
			free(klist);
		}
		blist_end(master_list, 1);
		plat_rwlock_unlock(&bt->lock);

		pathcnt++;
		__sync_add_and_fetch(&(bt->stats.stat[BTSTAT_GET_CNT]),1);
		__sync_add_and_fetch(&(bt->stats.stat[BTSTAT_GET_PATH]),pathcnt);
	}

	if (key_index != -1) {
		if (cursor->last_key) {
			free(cursor->last_key);
		}
		cursor->last_key  = (char *)malloc(values[key_index].keylen);
		assert(cursor->last_key);

		memcpy(cursor->last_key, values[key_index].key, 
		       values[key_index].keylen);
		cursor->last_keylen = values[key_index].keylen; 
	}

	assert(!dbg_referenced);
	if ((overall_status == BTREE_SUCCESS) && ((*n_out) == 0)) {
		overall_status = BTREE_QUERY_DONE;
	}
	return(overall_status);
}

btree_status_t
btree_end_range_query(btree_range_cursor_t *cursor)
{
	assert(cursor != NULL);
	assert(cursor->btree != NULL);

	if (cursor->last_key) {
		free(cursor->last_key);
	}

	if (cursor->query_meta) {
		if (cursor->query_meta->key_start) {
			free(cursor->query_meta->key_start);
		}

		if (cursor->query_meta->key_end) {
			free(cursor->query_meta->key_end);
		}

		free(cursor->query_meta);
	}
	free(cursor);

	return (BTREE_SUCCESS);
}
