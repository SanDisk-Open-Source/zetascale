/************************************************************************
 * 
 *  btree_list.c  May 17, 2013   Harihara Kadayam
 * 
 ************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/types.h>
#include "btree_list.h"

blist_t *
blist_init(void)
{
	blist_t *l = malloc(sizeof(blist_t));
	if (l == NULL) {
		return NULL;
	}

	l->head = NULL;
	l->tail = NULL;
	l->cnt = 0;
	return l;
}

btree_status_t
blist_push_node_from_head(blist_t *l, void *data1, void *data2, void *data3)
{
	blist_node_t *lnode;

	if (l == NULL) {
		return BTREE_FAILURE;
	}

	/* TODO: Do a prealloc as a seperate function and alloc only
	 * if an entry is not available */
	lnode = (blist_node_t *)malloc(sizeof(blist_node_t));
	if (lnode == NULL) {
		return BTREE_FAILURE;
	}

	lnode->data1 = data1;
	lnode->data2 = data2;
	lnode->data3 = data3;
	lnode->next  = l->head;

	l->head = lnode;
	if (l->tail == NULL) {
		l->tail = lnode;
	}
	l->cnt++;
	return BTREE_SUCCESS;
}

btree_status_t 
blist_push_list_from_head(blist_t *l, blist_t *src_list)
{
	/* Nothing to merge */
	if (src_list->cnt == 0) {
		return BTREE_SUCCESS;
	}

	/* Nothing in my list. Your list is mine now */
	if (l->head == NULL) {
		l->head = src_list->head;
		l->tail = src_list->tail;
		l->cnt  = src_list->cnt;

	} else {
		assert(src_list->tail->next == NULL);
		src_list->tail->next = l->head;
		l->head = src_list->head;
		l->cnt += src_list->cnt;
	}

	src_list->head = src_list->tail = NULL;
	src_list->cnt = 0;
	return BTREE_SUCCESS;
}

btree_status_t
blist_push_node_from_tail(blist_t *l, void *data1, void *data2, void *data3)
{
	blist_node_t *lnode;

	if (l == NULL) {
		return BTREE_FAILURE;
	}

	/* TODO: Do a prealloc as a seperate function and alloc only
	 * if an entry is not available */
	lnode = (blist_node_t *)malloc(sizeof(blist_node_t));
	if (lnode == NULL) {
		return BTREE_FAILURE;
	}

	lnode->data1 = data1;
	lnode->data2 = data2;
	lnode->data3 = data3;
	lnode->next = NULL;

	if (l->tail == NULL) {
		assert(l->head == NULL);
		assert(l->cnt == 0);
		l->tail = l->head = lnode;
	} else {
		l->tail->next = lnode;
		l->tail = lnode;

		if (l->head == NULL) {
			l->head = lnode;
		}
	}

	l->cnt++;
	return BTREE_SUCCESS;
}

void *
blist_pop_node_from_head(blist_t *l, void **data1, void **data2, void **data3)
{
	blist_node_t *lnode;
	if ((l == NULL) || (l->head == NULL)) {
		return NULL;
	}

	lnode = l->head;
	*data1 = lnode->data1;
	*data2 = lnode->data2;
	*data3 = lnode->data3;

	l->head = lnode->next;
	if (l->tail == lnode) {
		l->tail = NULL;
	}
	free(lnode);

	l->cnt--;
	return (*data1);
}

void 
blist_end(blist_t *l)
{
	blist_node_t *lnode;

	if (l == NULL) {
		return;
	}

	while (l->head) {
		lnode = l->head;
		l->head = lnode->next;
		free(lnode);
	}

	free(l);
}
