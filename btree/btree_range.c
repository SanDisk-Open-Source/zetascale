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

//  Define this to include detailed debugging code
#define DEBUG_STUFF

#define IS_ASCENDING_QUERY(meta) ((meta)->flags & (RANGE_START_GT | RANGE_START_GE))

static int is_multiple_flags_set(btree_range_flags_t flags, uint32_t f);

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

	if (x == 0) {
		return (start_incl ? 0 : -1);
	} else if (y == 0) {
		return (end_incl ? 0 : 1);
	} else if (x < 0) {
		return -1;
	} else if (y > 0) {
		return 1;
	} else {
		return 0;
	}
}

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

#define PUSH_DATA_TO_LIST(l, rmeta, d1, d2, d3) \
	if (IS_ASCENDING_QUERY(rmeta)) { \
		blist_push_node_from_tail(l, (void *)d1, (void *)d2, (void *)d3, (void *)NULL); \
	} else { \
		blist_push_node_from_head(l, (void *)d1, (void *)d2, (void *)d3, (void *)NULL); \
	} \

/* Marker should always be pushed to the end of the list, irrespective of whatever
 * type the query is 
 */
#define PUSH_MARKER_TO_LIST(l, n, m) \
	blist_push_node_from_tail(l, (void *)n, (void *)NULL, (void *)NULL, (void *)m); \

/*
 * Find the keys in the node for the range provided. Returns total keys found between the
 * the range. The resulted key structures are put in the key_list structure. It will be
 * put in the same order as it is requested for.
 *
 * The leaf_lock is the lock corresponding to the node, in case it is a leaf node.
 */
static int find_key_range(btree_raw_t *bt,
                          btree_raw_node_t *n,
                          btree_range_meta_t *rmeta,
                          blist_t *key_list,
                          plat_rwlock_t *leaf_lock)
{
	int            x;
	int            i_cur;
	node_key_t    *pk = NULL;
	key_stuff_t    ks;
	char          *lower;
	char          *upper;
	int           lower_incl, upper_incl;
	uint32_t      lower_len, upper_len;
	int           is_leaf_node;

	/* Easy denotions to avoid confusions and checks inside the logic */
	if (IS_ASCENDING_QUERY(rmeta)) {
		lower      = rmeta->key_start;
		lower_len  = rmeta->keylen_start;
		lower_incl = (rmeta->flags & RANGE_START_GE);
		upper      = rmeta->key_end;
		upper_len  = rmeta->keylen_end;
		upper_incl = (rmeta->flags & RANGE_END_LE);
	} else {
		lower      = rmeta->key_end;
		lower_len  = rmeta->keylen_end;
		lower_incl = (rmeta->flags & RANGE_END_GE);
		upper      = rmeta->key_start;
		upper_len  = rmeta->keylen_start;
		upper_incl = (rmeta->flags & RANGE_START_LE);
	}

	is_leaf_node = is_leaf(bt, n);

	/* Leaf node could get modified without global btree lock. */
 	 /* TODO: Leaf lock could possibly be combined with node structure 
	  * itself if possible or use the mem_node everywhere */
	if (is_leaf_node) plat_rwlock_rdlock(leaf_lock);

	if (n->nkeys == 0) {
		if (is_leaf_node) plat_rwlock_unlock(leaf_lock);
		return 0;
	}

	i_cur = 0;
	while (i_cur < n->nkeys) {
		(void) get_key_stuff(bt, n, i_cur, &ks);
		pk  = ks.pkey_struct;

		x = range_cmp(bt, ks.pkey_val, ks.keylen, 
		              lower, lower_len, lower_incl,
		              upper, upper_len, upper_incl);

		if (x == 0) { /* Value in range, push it inside */
			if (!is_leaf_node || is_seqno_in_range(rmeta, &ks)) {
				PUSH_DATA_TO_LIST(key_list, rmeta, n, pk, NULL);
			}
		} else if (x > 0) { /* Value beyond upper bound */
			break;
		}
		i_cur++;
	}

	if (!is_leaf_node) {
		int push_last_node = 1;

		/* For non-leaf nodes, the non-matched one need to be pushed 
		 * as well except the cases where, it matches the end condition
		 * of previous check exactly */
		if ((x == 0) &&
		    (bt->cmp_cb(bt->cmp_cb_data, upper, upper_len,
		                ks.pkey_val, ks.keylen) == 0)) {
			push_last_node = 0;
		}
			
		if (push_last_node) {
			if (i_cur == n->nkeys) {
				PUSH_DATA_TO_LIST(key_list, rmeta, n, NULL, 
				                  n->rightmost);
			} else {
				PUSH_DATA_TO_LIST(key_list, rmeta, n, pk, NULL);
			}
		}
		/* Use node as a dummy marker */
		PUSH_MARKER_TO_LIST(key_list, n, n); 
	} else {
		/* Leaf nodes at the end of it, will not unlock. It will
		 * be instead added as a marker to the list, which caller will
		 * use to unlock. This is to provide a contigous locking
		 * mechanism across the query */
		PUSH_MARKER_TO_LIST(key_list, n, leaf_lock);
	}

	return (key_list->cnt);
}

/* Start an index query.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_FAILURE if unsuccessful
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
	cr->query_meta   = rmeta;
	cr->indexid      = indexid;
	cr->last_key     = NULL;
	cr->last_keylen = 0;
	cr->last_status  = BTREE_SUCCESS;

	*cursor = cr;
	return (BTREE_SUCCESS);
}

btree_status_t
btree_get_next_range(btree_range_cursor_t *cursor,
                     int                   n_in,
                     int                  *n_out,
                     btree_range_data_t   *values)
{

	uint64_t          ptr;
	btree_raw_t      *bt;
	btree_raw_node_t *n;
	blist_t          *master_list;
	blist_t          *key_list;
	int               pathcnt = 1;
	btree_range_meta_t rmeta;
	uint32_t          meta_flags;
	btree_status_t    overall_status;
	btree_status_t    status;
	int               ret = 0;
	int               key_count;
	node_key_t        *keyrec = NULL;
	node_vlkey_t      *pvlk;
	uint64_t          right;
	int               key_index;
	plat_rwlock_t     *leaf_lock;
	void              *marker;

	/* TODO: Handle non-debug failures */
	assert(cursor != NULL);
	assert(cursor->btree != NULL);

	/* TODO: Take the lock and also clean this up */
	int n_partition = 0;
	bt = cursor->btree->partitions[n_partition];
	*n_out = 0;

	plat_rwlock_rdlock(&bt->lock);

	n = get_existing_node_low(&ret, bt, bt->rootid, &leaf_lock, 0);
	if (n == NULL) {
		plat_rwlock_unlock(&bt->lock);
		return BTREE_FAILURE;
	}

	/* Stack to hold all the node keys */
	master_list = blist_init();
	if (master_list == NULL) {
		plat_rwlock_unlock(&bt->lock);
		return BTREE_FAILURE;
	}

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

	/* Find all the key range provided in the meta into the stack.
	 * Order should be same order requested (ascending/descending) */
	key_list = blist_init();
	if (key_list == NULL) {
		blist_end(master_list, 1);
		plat_rwlock_unlock(&bt->lock);
		return BTREE_FAILURE;
	}

	key_count = find_key_range(bt, n, &rmeta, key_list, leaf_lock);
	if ((key_count == 0) || (*n_out == n_in)) {
		blist_end(key_list, 1);
		blist_end(master_list, 1);
		plat_rwlock_unlock(&bt->lock);
		return BTREE_QUERY_DONE;
	}

	/* Add the key_list into master_list */
	blist_push_list_from_head(master_list, key_list);
	blist_end(key_list, 0);

	/* TODO: Later on try to combine range_meta and btree_meta
	 * For now, set the meta_flags for get_leaf_data usage */
	meta_flags = rmeta.flags & (RANGE_BUFFER_PROVIDED | RANGE_ALLOC_IF_TOO_SMALL);
	overall_status = BTREE_SUCCESS;

	key_index = -1;

	/* Need to remove elements from head, to maintain order */
	while (blist_pop_node_from_head(master_list, 
	                               (void **)&n,
	                               (void **)&keyrec,
	                               (void **)&right,
	                               (void **)&marker)) {

		if (marker) {
			if (is_leaf(bt, n)) {
				leaf_lock = (plat_rwlock_t *)marker;

				/* This is the marker entry from the find_key_range. At
				 * present use this marker to unlock the leaf lock */
				if (leaf_lock) {
					/* Unlock leaf node lock which was locked by find_key_range */
					plat_rwlock_unlock(leaf_lock);
				}
			}

			/* We are done using the node */
			deref_l1cache_node(bt, n);
			continue;
		}
				
		/* If we have reached the end of the list, need to continue process
		 * the markers to unlock the leaf */
		if (n_in == *n_out) {
			continue;
		}

		if (is_leaf(bt, n)) {

			/* Populate the output value with key, value, status 
			 * syndrome information */
			values[*n_out].status = BTREE_RANGE_SUCCESS;

			status = get_leaf_key(bt, n, (void *)keyrec, 
			                      &values[*n_out].key,
			                      &values[*n_out].keylen,
			                      meta_flags);
			if (status != BTREE_SUCCESS) {
				/* TODO: Consider introducing BTREE_WARNING */
				overall_status = BTREE_FAILURE;

				values[*n_out].status |= 
				   (status == BTREE_BUFFER_TOO_SMALL) ?
				      BTREE_KEY_BUFFER_TOO_SMALL: BTREE_FAILURE;
			} else {
				key_index = *n_out;
			}

			if (!(rmeta.flags & RANGE_KEYS_ONLY)) {
				status = get_leaf_data(bt, n, (void *)keyrec, 
				                    &values[*n_out].data,
				                    &values[*n_out].datalen,
				                    meta_flags, 0);

				if (status != BTREE_SUCCESS) {
					overall_status = BTREE_FAILURE;

					values[*n_out].status |= 
					   (status == BTREE_BUFFER_TOO_SMALL) ?
					      BTREE_DATA_BUFFER_TOO_SMALL: BTREE_FAILURE;
				}
			}

			pvlk = (node_vlkey_t *) keyrec;
			values[*n_out].seqno    = pvlk->seqno; 
			values[*n_out].syndrome = pvlk->syndrome; 

			(*n_out)++;
		} else {
			if (keyrec == NULL) {
				ptr = right;
			} else {
				if (bt->flags & SYNDROME_INDEX) {
					ptr = ((node_fkey_t *) keyrec)->ptr;
				} else {
					ptr = ((node_vkey_t *) keyrec)->ptr;
				}
			}

			/* Get the node corresponding to this pointer and leaf
			 * lock for this node */
			n = get_existing_node_low(&ret, bt, ptr, &leaf_lock, 0);
			if (ret) {
				assert(0);
				break;
			}

			key_list = blist_init();
			if (key_list == NULL) {
				blist_end(master_list, 1);
				plat_rwlock_unlock(&bt->lock);
				return BTREE_FAILURE;
			}
			key_count = find_key_range(bt, n, &rmeta, key_list, leaf_lock);
			blist_push_list_from_head(master_list, key_list);
			blist_end(key_list, 0);
		}
		pathcnt++;
	}

	plat_rwlock_unlock(&bt->lock);

	blist_end(master_list, 1);

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

	bt->stats.stat[BTSTAT_GET_CNT]++;
	bt->stats.stat[BTSTAT_GET_PATH] += pathcnt;

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

	free(cursor);

	return (BTREE_SUCCESS);
}
