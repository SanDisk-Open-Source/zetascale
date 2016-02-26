/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
#include "btree_raw_internal.h"

extern btree_node_list_t *free_node_list;
extern btree_node_list_t *free_raw_node_list;
/******************* Node related functions *********************/
btree_node_list_t *
btree_node_list_init(uint64_t n_entries, uint64_t size)
{
	btree_node_list_t *l = malloc(sizeof(btree_node_list_t));
	if (l == NULL) {
		return NULL;
	}

	l->head = NULL;
	l->n_free_entries = 0;
	l->size = size;

	pthread_mutex_init(&l->mem_mgmt_lock, NULL);
	if (n_entries != 0) {
		btree_node_list_alloc(l, n_entries, size);
	}

	l->n_entries = n_entries;
#ifdef MEM_SIZE_DEBUG
	l->n_threshold_entries = n_entries - (0.01 * n_entries);
	l->min_free_entries = l->n_free_entries;
#endif

	return l;
}

btree_status_t
btree_node_list_alloc(btree_node_list_t *l, uint64_t n_entries, uint64_t size)
{
	uint64_t i;
	char *buf = (char *)malloc((uint64_t)(n_entries * size));
	btree_raw_mem_node_t *mnode;

	if (buf == NULL) {
		assert(0); /* Unavailability of memory */
		return BTREE_FAILURE;
	}

	/* TODO: Look to optimize this push into freelist instead
	 * of doing it one-by-one. At present, it is getting called
	 * only during btree_init, so its fine for now. */
	for (i = 0; i < n_entries; i++) {
		mnode = (btree_raw_mem_node_t *)buf;
		mnode->malloced = false;
		btree_node_free2(l, mnode);
		buf += size;
	}

	return BTREE_SUCCESS;
}

btree_raw_mem_node_t *
btree_node_alloc(btree_node_list_t *l)
{
	btree_raw_mem_node_t *mnode;

	if (l == NULL) {
		assert(0);
		return NULL;
	}

	/* If we no longer have free entries, get it from malloc.
	 * Should not happen */
	pthread_mutex_lock(&l->mem_mgmt_lock);
	if (l->n_free_entries == 0) {
		pthread_mutex_unlock(&l->mem_mgmt_lock);
		
		mnode = (btree_raw_mem_node_t *)malloc(l->size);
		assert(mnode != NULL);

		mnode->malloced = true;
		return (mnode);
	}

	mnode = l->head;
	l->head = mnode->free_next;
	l->n_free_entries--;
	pthread_mutex_unlock(&l->mem_mgmt_lock);

#ifdef MEM_SIZE_DEBUG
	if (l->n_free_entries < l->min_free_entries) {
		l->min_free_entries = l->n_free_entries;
	}

	if ((l->n_entries - l->n_free_entries) > l->n_threshold_entries) {
		fprintf(stdout, "Total used entries: %"PRIu64" exceeded threshold %"PRIu64"\n",
		        (l->n_entries - l->n_free_entries), l->n_threshold_entries);
	}
//	fprintf(stdout, "btree_node_alloc: Allocated node=%p. Total free entries: %"PRIu64"\n", mnode, l->n_free_entries);
#endif

	assert(mnode != NULL);

	mnode->malloced = false;
	return mnode;
}

void
btree_node_free(btree_raw_mem_node_t *mnode)
{
	btree_node_free2(BT_USE_RAWOBJ(mnode->pnode->flags) ? free_raw_node_list : free_node_list, mnode);
}

void
btree_node_free2(btree_node_list_t *l, btree_raw_mem_node_t *mnode)
{
	if (l == NULL) {
		assert(0);
		return;
	}

	/* Not from pool */
	if (mnode->malloced) {
		free(mnode);
		return;
	}

	pthread_mutex_lock(&l->mem_mgmt_lock);
	mnode->free_next = l->head;
	l->head = mnode;
	l->n_free_entries++;
	pthread_mutex_unlock(&l->mem_mgmt_lock);

//	fprintf(stdout, "Freed a node. Total free entries: %"PRIu64"\n", l->n_free_entries);
}
