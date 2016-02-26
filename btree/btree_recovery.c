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

/************************************************************************
 * 
 *  btree_recovery.c  Sept 16, 2013   Harihara Kadayam
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
#include "btree_raw.h"
#include "btree_raw_internal.h"
#include "btree/btree_var_leaf.h"

#ifdef _OPTIMIZE
#undef assert
#define assert(a)
#endif

typedef enum {
	RCVR_OP_SET  = 1,
	RCVR_OP_DELETE
} rcvry_op_type_t;

typedef struct btree_robj_info {
	struct btree_robj_info *next;       /* Next in key list */
	struct btree_robj_info *op_next;    /* Next in op list */
	struct btree_robj_info *buf_next;   /* Next in buf pool */

	char                   *key;        /* Key/Data pairs */
	uint32_t               keylen;
	char                   *data;
	uint64_t               datalen;
	uint64_t		ptr;
	bool                   tombstone;
	btree_raw_node_t       **data_node_start;
	uint32_t               data_node_cnt;

	node_vlkey_t           *pvlk;       /* Key details inside node */
	uint64_t               seqno;       /* Seq no of operation */

	rcvry_op_type_t        rcvry_op;    /* What undo/redo op to perform */
	uint64_t               rcvry_flags; /* Flags if any to logical op */
} btree_robj_info_t;

typedef struct {
	btree_robj_info_t *head;
	btree_robj_info_t *cur;
	btree_robj_info_t *buf_head;
	uint32_t cnt;
} btree_robj_list_t;

#define AVG_KEY_COUNT  100

static inline int
cmp_robjs_with_seq(btree_raw_t *bt, btree_robj_info_t *o1, btree_robj_info_t *o2)
{
	int x;

	x = bt->cmp_cb(bt->cmp_cb_data, o1->key, o1->keylen, o2->key, o2->keylen);
	if (x == 0) {
		if (o1->seqno > o2->seqno) {
			x = 1;
		} else if (o1->seqno < o2->seqno) {
			x = -1;
		}
	}

	return (x);
}

static inline void
add_overflow_robj(btree_raw_t *bt, btree_robj_info_t *r, 
                  btree_raw_node_t **nodes, uint32_t node_cnt)
{
	uint32_t ind = 0;

	/* Get to the first overflow node */
	while (ind < node_cnt) {
		if (nodes[ind]->logical_id == (uint64_t)r->ptr) {
			assert(is_overflow(bt, nodes[ind]));
			break;
		}
		ind++;
	}

	if (ind == node_cnt) {
		/* No overflow node found */
		return;
	}

	/* TODO: Need to handle cases where all overflow nodes are
	 * not in succession */
	r->data_node_start = &nodes[ind++];
	r->data_node_cnt = 1;
	while ((ind < node_cnt) && is_overflow(bt, nodes[ind++])) {
		r->data_node_cnt++;
	}
}

/* Generate a list of objects on a given set of nodes, sorted in 
 * btree order. */
static void
gen_robj_list(btree_raw_t *bt, btree_raw_node_t **nodes, 
              uint32_t node_cnt, btree_robj_list_t *list)
{
	int i, j;
	uint32_t alloc_cnt = 0;
	uint32_t used_cnt = 0;
	btree_robj_info_t *robj_buf = NULL;
	btree_robj_info_t *r;
	btree_robj_info_t *e;
	btree_robj_info_t *prev_e;
	int x;

	list->head = NULL;
	list->cnt = 0;

	robj_buf = malloc(node_cnt * AVG_KEY_COUNT * sizeof(btree_robj_info_t));
	assert(robj_buf);
	alloc_cnt += node_cnt * AVG_KEY_COUNT;
	list->buf_head = robj_buf;

	/* TODO: Instead of using naive insert in sorted order, implement
	 * jumping from one node to other (like skip list) to insert keys
	 * to the list in sorted order. */
	for (i = 0; i < node_cnt; i++) {

		/* Overflow nodes are already handled */
		if (is_overflow(bt, nodes[i])) {
			/* TODO: Validate the assumption that overflow
			 * node will always follow the leaf node and not
			 * vice versa */
			continue;
		}

		if (!is_leaf(bt, nodes[i])) {
			continue;
		}

		for (j = nodes[i]->nkeys - 1; j >= 0; j--) {
			if (used_cnt == alloc_cnt) {
				btree_robj_info_t *tmp_buf = robj_buf;

				/* Estimate is not correct. Incrementally add
				 * more buffer to the pool list */
				/* TODO: Instead of adding whole new buffer, add
				 * incrementally some buffer */
				robj_buf = malloc(node_cnt * AVG_KEY_COUNT 
				                  * sizeof(btree_robj_info_t));
				assert(robj_buf);
				//alloc_cnt += node_cnt * AVG_KEY_COUNT;
				tmp_buf->buf_next = robj_buf;

				used_cnt = 0;
			}
			key_info_t ki;
			bool b = btree_leaf_get_nth_key_info( bt, nodes[i], j, &ki);
			char *data;
			uint64_t datalen;
			b = btree_leaf_get_data_nth_key( bt, nodes[i], j, &data, &datalen);

			r = &robj_buf[used_cnt++];
			r->key     = ki.key;
			r->keylen  = ki.keylen;
			r->datalen = ki.datalen;
			r->seqno   = ki.seqno;
			r->pvlk    = 0;
			r->tombstone = btree_leaf_is_key_tombstoned(bt, nodes[i], j);
			assert(!r->tombstone || !r->datalen);

			r->data_node_cnt   = 0;
			if ((r->keylen + r->datalen) < bt->big_object_size) {
				r->data = memcpy( malloc( r->datalen), data, r->datalen);
				r->data_node_start = NULL;
			} else {
				r->ptr = ki.ptr;
				uint64_t rem = btree_get_bigobj_inleaf(bt, r->keylen, r->datalen);
				if (rem) {
					r->data = memcpy( malloc( rem), data, rem);
				}
				add_overflow_robj(bt, r, nodes, node_cnt);
			}
			r->buf_next = NULL;

			prev_e = NULL;
			e = list->head;
			while (e) {
				/* Most of the time, key should
				 * go to the top */
				x = cmp_robjs_with_seq(bt, r, e);

				assert(x != 0);
				if (x < 0) {
					/* we found the slot */
					break;
				}
				prev_e = e;
				e = e->next;
			}

			if (prev_e == NULL) {
				list->head = r;
			} else {
				prev_e->next = r;
			}
			r->next = e;
			list->cnt++;
		}
	}
}

static void
get_robj_data(btree_raw_t *bt, btree_robj_info_t *obj)
{
	uint32_t ncnt = 0;
	btree_raw_node_t **nodes;
	char     *p;
	uint64_t nbytes;
	uint64_t copybytes;
	uint64_t ovdatasize = get_data_in_overflownode(bt);

	if (obj->data_node_cnt == 0) {
		if ((obj->keylen + obj->datalen) > bt->big_object_size) {
			fprintf(stderr, "ERROR: Recovery expecting big object (keylen=%u datalen=%lu),"
			                "but packet missing overflow node\n", obj->keylen, obj->datalen);
			abort();
		}

		/* Data resides in node */
		char *tmp_data = obj->data;
		obj->data = malloc(obj->datalen);
		assert(obj->data);

		memcpy(obj->data, tmp_data, obj->datalen);
	} else {
		/* Overflow node */
		uint64_t next_lid = (uint64_t)obj->ptr;
		char *tmp_data = obj->data;
		obj->data = malloc(obj->datalen);
		assert(obj->data);

		uint64_t rem = btree_get_bigobj_inleaf(bt, obj->keylen, obj->datalen);
		if (rem) {
			assert(tmp_data);
			memcpy(obj->data, tmp_data, rem);
		}

		nodes = obj->data_node_start;
		nbytes = obj->datalen - rem;
		p = obj->data + rem;

		while((nbytes > 0) && (ncnt < obj->data_node_cnt)) {
			assert(next_lid == nodes[ncnt]->logical_id);

			copybytes = (nbytes >= ovdatasize) ? 
			                ovdatasize : nbytes;
			memcpy(p, ((char *) nodes[ncnt] + sizeof(btree_raw_node_t)), copybytes);

			nbytes -= copybytes;
			p      += copybytes;
			next_lid = nodes[ncnt]->next;
			ncnt++;
		}

		assert(nbytes == 0);
	}
}

static void
add_robj_op(btree_raw_t *bt, btree_robj_list_t *op_list, btree_robj_info_t *obj,
            rcvry_op_type_t op, uint32_t flags)
{
	char *tmp_key  = obj->key;

	/* Need to copy the one in obj as it is a direct pointer in the node */
	obj->key  = malloc(obj->keylen);
	assert(obj->key);
	memcpy(obj->key, tmp_key, obj->keylen);
	if (op == RCVR_OP_SET) {
		get_robj_data(bt, obj);
	} else {
		obj->data = NULL;
	}

	/* Set the recovery op */
	obj->rcvry_op = op;
	obj->rcvry_flags = flags;

	/* Add to op_list */
	if (op_list->head == NULL) {
		assert(op_list->cur == NULL);
		op_list->head = op_list->cur =  obj;
		obj->op_next = NULL;
	} else {
		op_list->cur->op_next = obj;
		obj->op_next = NULL;
		op_list->cur = obj;
	}
	op_list->cnt++;
}

static btree_status_t
do_recovery_op(btree_raw_t *bt, btree_robj_list_t *op_list)
{
	btree_robj_info_t *cur_obj;
	btree_robj_info_t *next_obj;
	btree_metadata_t meta;
	btree_status_t status = BTREE_SUCCESS;
	int x;
	uint32_t stats_delete_cnt = 0;
	uint32_t stats_set_cnt = 0;
	uint32_t stats_warning_cnt = 0;

	cur_obj = op_list->head;
	next_obj = NULL;

	while (cur_obj) {
		next_obj = cur_obj->op_next;

		switch (cur_obj->rcvry_op) {
		case RCVR_OP_DELETE:
#if 0
#ifndef _OPTIMIZE
			if (next_obj) {
	        		x = bt->cmp_cb(bt->cmp_cb_data, 
				               cur_obj->key, cur_obj->keylen,
				               next_obj->key, next_obj->keylen);

				/* TODO: This assert will be removed
				 * when snapshots are supported. Until then
				 * there can be only one set undo operation */
				assert(x != 0);
			}
#endif
#endif
			/* Delete the cur obj.
			 * TODO: Handle its return status */
			bzero(&meta, sizeof(btree_metadata_t));
			meta.flags = 0;
			if (cur_obj->tombstone) {
				meta.flags = FORCE_DELETE | READ_SEQNO_EQ;
			}
			status = btree_raw_delete(bt, cur_obj->key, 
			                     cur_obj->keylen, &meta);
			if (status == BTREE_SUCCESS) {
				stats_delete_cnt++;
			} else if (status == BTREE_KEY_NOT_FOUND) {
				stats_warning_cnt++;
			} else {
				fprintf(stderr, "Recovery failed because undo "
				        "write operation failed. "
				        "Reason code %u\n", status);
				exit(0);
			}

#if 0//Rico - buggy?
			free(cur_obj->key);
#endif
			break;

		case RCVR_OP_SET:
			while (next_obj) {
				/* Check if next op is for same key */
	        		x = bt->cmp_cb(bt->cmp_cb_data, 
				               cur_obj->key, cur_obj->keylen,
				               next_obj->key, next_obj->keylen);
				if (x != 0) 
					break;

				/* TODO: This assert will be removed when 
				 * snapshots are supported. Until then there
				 * cannot be 2 updates for the same key */
				assert (next_obj->rcvry_op != RCVR_OP_SET);
				next_obj = next_obj->op_next;
			}
			
			bzero(&meta, sizeof(btree_metadata_t));
			meta.flags = UPDATE_USE_SEQNO;
			meta.seqno = cur_obj->seqno;
			if (cur_obj->tombstone) {
				meta.flags |= INSERT_TOMBSTONE;
			}
			status = btree_raw_write(bt, cur_obj->key, cur_obj->keylen, 
			                 cur_obj->data, cur_obj->datalen, &meta, 0);
#if 0//Rico - buggy?
			free(cur_obj->key);
			free(cur_obj->data);
#endif

			if (status == BTREE_SUCCESS) {
				stats_set_cnt++;
			} else if (status == BTREE_KEY_NOT_FOUND) {
				stats_warning_cnt++;
			} else {
				fprintf(stderr, "Recovery failed because undo "
				        "write operation failed. "
				        "Reason code %u\n", status);
				exit(0);
			}
			break;

		default:
			assert(0);
			break;
		}
		cur_obj = next_obj;
	}

#if 0//Rico
	if (stats_warning_cnt) {
		printf("Warning: Undo operation reports not found "
		       "error for %u objects. Perhaps these objects are "
		       "already recovered\n", stats_warning_cnt);
	}
#else
	static bool once;
	if ((stats_warning_cnt)
	&& (once == 0)) {
		once = 1;
		printf("Warning: Undo operation reports not found "
		       "error for %u objects. Perhaps these objects are "
		       "already recovered (reported once only)\n", stats_warning_cnt);
	}
#endif

#if 0//Rico
	if (stats_set_cnt || stats_delete_cnt) {
		printf("Recovered %u objs by deleting %u objs and resetting %u objs\n",
		        stats_set_cnt + stats_delete_cnt, stats_delete_cnt, stats_set_cnt);
	}
#endif

	return (status);
}

btree_status_t
btree_recovery_process_minipkt(btree_raw_t *bt,
                               btree_raw_node_t **onodes, uint32_t on_cnt, 
                               btree_raw_node_t **nnodes, uint32_t nn_cnt)
{
	btree_robj_info_t *o_obj;
	btree_robj_info_t *n_obj;
	btree_robj_info_t *r;
	btree_robj_list_t or_list;
	btree_robj_list_t nr_list;
	btree_robj_list_t op_list;
	btree_status_t st;
	uint32_t alloc_cnt;
	int i, j;
	int x;

	/* Add all old and new nodes objs to the list */
	gen_robj_list(bt, onodes, on_cnt, &or_list);
	gen_robj_list(bt, nnodes, nn_cnt, &nr_list);

	/* Initialize op list, which will hold all the operations */
	op_list.head = NULL;
	op_list.cnt = 0;
	op_list.cur = NULL;

	o_obj = or_list.head;
	n_obj = nr_list.head;

	/* Walk individual objects and compare them and
	 * generate an undo op for them */
	while (o_obj && n_obj) {
		x = cmp_robjs_with_seq(bt, o_obj, n_obj);
		if (x == 0) { /* No change */
			o_obj = o_obj->next;
			n_obj = n_obj->next;
		} else if (x < 0) { 
			/* Old is present, set old obj */
			add_robj_op(bt, &op_list, o_obj, RCVR_OP_SET, 0/*flags*/);
			o_obj = o_obj->next;
		} else { 
			/* New is present, delete new obj */
			add_robj_op(bt, &op_list, n_obj, RCVR_OP_DELETE, 0/*flags*/);
			n_obj = n_obj->next;
		}
	}

	while (o_obj) {
		/* All remaining old entries to be re set */
		add_robj_op(bt, &op_list, o_obj, RCVR_OP_SET, 0 /* flags */);
		o_obj = o_obj->next;
	}

	while (n_obj) {
		/* All remaining new entries to be deleted */
		add_robj_op(bt, &op_list, n_obj, RCVR_OP_DELETE, 0/*flags*/);
		n_obj = n_obj->next;
	}

	/* Process all these undo ops */
	st = do_recovery_op(bt, &op_list);

#if 0//Rico - buggy
	btree_robj_info_t *buf = or_list.buf_head;
	while (buf) {
		btree_robj_info_t *tmpbuf = buf;
		buf = buf->buf_next;
		free(tmpbuf);
	}

	buf = nr_list.buf_head;
	while (buf) {
		btree_robj_info_t *tmpbuf = buf;
		buf = buf->buf_next;
		free(tmpbuf);
	}
#endif

	return (st);
}

#ifdef BTREE_UNDO_TEST

#define MAX_RTEST_NODES   100

void *test_rnode_buf_1 = NULL;
btree_raw_node_t *test_rnode_list_1[MAX_RTEST_NODES];
uint32_t test_rnode_cnt_1 = 0;

void *test_rnode_buf_2 = NULL;
btree_raw_node_t *test_rnode_list_2[MAX_RTEST_NODES];
uint32_t test_rnode_cnt_2 = 0;
uint64_t rtest_deleted_nodes_2[MAX_RTEST_NODES];
uint32_t rtest_deleted_cnt_2 = 0;

uint8_t collect_phase = 0;
uint8_t recovery_phase = 0;

static inline 
void add_rtest_rnode(btree_raw_t *bt, btree_raw_node_t *node, btree_raw_node_t **list, uint32_t *pcnt)
{
	bool found = false;
	int i;

	if (node == NULL) {
		return;
	}

	for (i = 0; i < *pcnt; i++) {
		if (list[i]->logical_id == node->logical_id) {
			memcpy(list[i], node, bt->nodesize);
			found = true;
			break;
		}
	}

	if (!found) {
		memcpy(list[(*pcnt)++], node, bt->nodesize);
	}
}

static inline
void delete_rtest_rnode(btree_raw_t *bt, btree_raw_node_t *node, btree_raw_node_t **list, uint32_t *pcnt)
{
	bool found = false;
	int i;

	for (i = 0; i < *pcnt; i++) {
		if (list[i]->logical_id == node->logical_id) {
			found = true;
			break;
		}
	}

	if (!found)
		return;

	memmove(list[i], list[i+1], (((*pcnt) - i - 1) * bt->nodesize));
	(*pcnt)--;
}

static void 
btree_rtest_dump_node(btree_raw_t *bt, btree_raw_node_t *n)
{
	key_stuff_t ks;
	int i;
	int last_key = -1;
	int cur_key;

	printf("-----------------------------------\n");
	printf("Node: %"PRIu64"\n", n->logical_id);
	printf("-----------------------------------\n");
	for (i = 0; i < n->nkeys; i++) {
		(void) get_key_stuff(bt, n, i, &ks);

		sscanf(ks.pkey_val, "Key_%d", &cur_key);
		if (cur_key != (last_key + 1)) {
			if (last_key == -1) {
				printf("%d - ", cur_key);
			} else {
				printf("%d, %d - ", last_key, cur_key);
			}
		}
		last_key = cur_key;
	}
	printf("\n");
}

void 
btree_rtest_dump_nodes(btree_raw_t *bt, btree_raw_node_t *nlist, uint32_t ncnt)
{
	int i;

	for (i = 0; i < ncnt; i++) {
		btree_rtest_dump_node(bt, nlist++);
	}
}

btree_status_t 
btree_recovery_ioctl(struct btree_raw *bt, uint32_t ioctl_type, void *data)
{
	uint64_t type = (uint64_t)data;
	int i;

	switch (type) {
	case BTREE_IOCTL_RECOVERY_COLLECT_1:
		if (test_rnode_buf_1 == NULL) {
			test_rnode_buf_1  = (btree_raw_node_t *)malloc(
			                           bt->nodesize * MAX_RTEST_NODES);
		}

		for (i = 0; i < MAX_RTEST_NODES; i++) {
			test_rnode_list_1[i] = 
			     (btree_raw_node_t *)((char *)test_rnode_buf_1 + (i * bt->nodesize));
		}

		/* Start collecting first set */
		test_rnode_cnt_1 = 0;
		collect_phase = 1;
		break;

	case BTREE_IOCTL_RECOVERY_COLLECT_2:
		if (test_rnode_buf_2 == NULL) {
			test_rnode_buf_2  = (btree_raw_node_t *)malloc(
			                           bt->nodesize * MAX_RTEST_NODES);
		}

		for (i = 0; i < MAX_RTEST_NODES; i++) {
			test_rnode_list_2[i] = 
			     (btree_raw_node_t *)((char *)test_rnode_buf_2 + (i * bt->nodesize));
		}

		/* Start collecting second set */
		test_rnode_cnt_2 = 0;
		collect_phase = 2;
		rtest_deleted_cnt_2 = 0;
		break;

	case BTREE_IOCTL_RECOVERY_START:
		collect_phase = 0;
		btree_rcvry_test_recover(bt);
		break;

	default:
		assert(0);
		break;
	}

	return BTREE_SUCCESS;
}

void btree_rcvry_test_collect(btree_raw_t *bt, btree_raw_node_t *node)
{
	btree_raw_node_t *n = NULL;
	int i, j;

	if (!is_leaf(bt, node) && !is_overflow(bt, node)) {
		return;
	}

	if (collect_phase == 1) {
		if (test_rnode_cnt_1 > MAX_RTEST_NODES) {
			fprintf(stderr, "Reached max collection for phase 1 of recovery test\n");
			return;
		}
		add_rtest_rnode(bt, node, test_rnode_list_1, &test_rnode_cnt_1);
	} else if (collect_phase == 2) {
		if (test_rnode_cnt_2 > MAX_RTEST_NODES) {
			fprintf(stderr, "Reached max collection for phase 2 of recovery test\n");
			return;
		}
		add_rtest_rnode(bt, node, test_rnode_list_2, &test_rnode_cnt_2);
	}
}

void btree_rcvry_test_delete(btree_raw_t *bt, btree_raw_node_t *node)
{
	if (!is_leaf(bt, node) && !is_overflow(bt, node)) {
		return;
	}

	if (collect_phase == 1) {
		delete_rtest_rnode(bt, node, test_rnode_list_1, &test_rnode_cnt_1);
	} else if (collect_phase == 2) {
		delete_rtest_rnode(bt, node, test_rnode_list_2, &test_rnode_cnt_2);
		/* Keep track of deleted node logical id */
		rtest_deleted_nodes_2[rtest_deleted_cnt_2++] = node->logical_id;
	}
}

void btree_rcvry_test_recover(btree_raw_t *bt)
{
	static int recovery_phase = 0;
	int i, j, d;

	if (recovery_phase == 1) {
		printf("Recovery is already in progress\n");
		return;
	}

	recovery_phase = 1;
	printf("test_rnode_cnt_1 = %d, test_rnode_cnt_2 = %d\n", test_rnode_cnt_1, test_rnode_cnt_2);

	/* Have only the clashing nodes in the list */
	uint32_t newcnt = 0;
	for (i = 0; i < test_rnode_cnt_1; i++) {
		/* Add nodes that were deleted later into the list */
		for (d = 0; d < rtest_deleted_cnt_2; d++) {
			if (test_rnode_list_1[i]->logical_id == 
			                        rtest_deleted_nodes_2[d]) {
				test_rnode_list_1[newcnt++] = test_rnode_list_1[i];
				break;
			}
		}	

		if (d < rtest_deleted_cnt_2) 
			continue;

		for (j = 0; j < test_rnode_cnt_2; j++) {
			if (test_rnode_list_1[i]->logical_id ==
			      test_rnode_list_2[j]->logical_id) {
				test_rnode_list_1[newcnt++] = test_rnode_list_1[i];
				break;
			}
		}
	}

	printf("Attempting to recover %d modified nodes and %d new nodes\n",
	          newcnt, test_rnode_cnt_2 - newcnt);

	(void) btree_recovery_process_minipkt(bt, 
	                             test_rnode_list_1, newcnt,
	                             test_rnode_list_2, test_rnode_cnt_2);

	/* Restore it back to original */
	for (i = 0; i < MAX_RTEST_NODES; i++) {
		test_rnode_list_1[i] = 
		     (btree_raw_node_t *)((char *)test_rnode_buf_1 + (i * bt->nodesize));
		test_rnode_list_2[i] = 
		     (btree_raw_node_t *)((char *)test_rnode_buf_2 + (i * bt->nodesize));
	}

	recovery_phase = 0;

//	fprintf(stderr, "BTree Leaf Nodes after recovery\n");
//	btree_raw_dump(stderr, bt, 0);
//	btree_rtest_dump_nodes(bt, test_rnode_list_1, test_rnode_cnt_1);
}
#endif
