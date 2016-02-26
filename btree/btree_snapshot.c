/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/************************************************************************
 * 
 *  btree_snapshot.c  Nov 7, 2013   Harihara Kadayam
 * 
 ************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/time.h>
#include <inttypes.h>
#include <assert.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include "btree_hash.h"
#include "btree_raw.h"
#include "btree_map.h"
#include "btree_raw_internal.h"
#include "zs.h"

static int btree_snap_find_meta_index_low(btree_raw_t *bt, uint64_t seqno);
extern __thread struct ZS_thread_state *my_thd_state;
extern btree_node_list_t *free_node_list;

#define offsetof(st, m) ((size_t)(&((st *)0)->m))

uint32_t
btree_snap_get_max_snapshot(btree_snap_meta_t *snap_meta, size_t size)
{
	switch (snap_meta->snap_version) {
		case SNAP_VERSION1 :
					return (size / sizeof(btree_snap_info_v1_t));
		default :	assert(0);
	}
        return 0;
}

btree_status_t
btree_snap_rw_metanode(btree_raw_t *bt, bool write)
{
	btree_status_t ret;

	if (write) {
		uint64_t *plogical_id = &(bt->snap_meta->n_hdr.logical_id);
		bt->write_node_cb(my_thd_state, &ret, bt->write_node_cb_data,
		                  &plogical_id, (char**)&(bt->snap_meta),
		                  bt->nodesize, 1, 0);
	} else {
		uint64_t logical_id = META_SNAPSHOT_LOGICAL_ID;
		bt->read_node_cb(&ret, bt->read_node_cb_data, (void *)bt->snap_meta,
		                 logical_id, 0 /* its not a raw obj */);
		if (ret == BTREE_SUCCESS) {
			assert(logical_id == bt->snap_meta->n_hdr.logical_id);
			bt->snap_mnode->cache_valid = true;
		}
	}

	if ((ret != BTREE_SUCCESS) && (storage_error(ret))) {
		abort(); /* We cannot handle storage error for meta nodes yet */
	}

	return (ret);
}

btree_status_t
btree_snap_init(btree_raw_t *bt, bool create)
{
	btree_raw_mem_node_t *mnode;
	uint32_t snap_data_size;
	btree_status_t ret;

	bt->snap_mnode = btree_node_alloc(free_node_list);
	if (bt->snap_mnode == NULL) {
		fprintf(stderr, "Error: Unable to allocate a snapshot metadata node\n");
		return BTREE_FAILURE;
	}

	bt->snap_mnode->pnode = (btree_raw_node_t*)(((void *)bt->snap_mnode) + 
	                                              sizeof(btree_raw_mem_node_t));
	bt->snap_meta = (btree_snap_meta_t *)bt->snap_mnode->pnode;
	bzero(bt->snap_meta, bt->nodesize);

	pthread_rwlock_init(&bt->snap_lock, NULL);

	if (create) {
		snap_data_size = bt->nodesize - offsetof(btree_snap_meta_t, meta);

		bt->snap_meta->n_hdr.logical_id = META_SNAPSHOT_LOGICAL_ID;
		bt->snap_meta->snap_version = SNAP_VERSION;
		bt->snap_meta->total_snapshots = 0;
		bt->snap_meta->max_snapshots = btree_snap_get_max_snapshot(bt->snap_meta, snap_data_size);
		bt->snap_meta->sc_status = 0;

		ret = btree_snap_rw_metanode(bt, true /* write */);
	} else {
		ret = btree_snap_rw_metanode(bt, false /* its a read */);
	}

	return ret;
}

static btree_status_t
btree_snap_create_meta_v1(btree_raw_t *bt, uint64_t seqno)
{
	btree_snap_info_v1_t *info;
	struct timeval     now;

	if (bt->snap_meta->total_snapshots >= bt->snap_meta->max_snapshots) {
		return (BTREE_TOO_MANY_SNAPSHOTS);
	}

	info = &bt->snap_meta->meta.v1_meta.snapshots[bt->snap_meta->total_snapshots];
	info->seqno = seqno;

	// get current time in GMT
	gettimeofday(&now, NULL);
	info->timestamp = now.tv_sec;

	return (BTREE_SUCCESS);
}

btree_status_t
btree_snap_create_meta(btree_raw_t *bt, uint64_t seqno)
{
	btree_snap_meta_t  *snap_meta = bt->snap_meta;
	btree_status_t     ret;

	pthread_rwlock_wrlock(&bt->snap_lock);
	switch (snap_meta->snap_version) {
	case SNAP_VERSION1: 
		ret = btree_snap_create_meta_v1(bt, seqno);
		if (ret != BTREE_SUCCESS) {
			pthread_rwlock_unlock(&bt->snap_lock);
			return (ret);
		}
		break;

	default:
		assert(0);
	}
	bt->snap_meta->total_snapshots++;
	__sync_add_and_fetch(&(bt->stats.stat[BTSTAT_NUM_SNAPS]), 1);

	ret = btree_snap_rw_metanode(bt, true /* write */);
	pthread_rwlock_unlock(&bt->snap_lock);

	return (ret);
}

static btree_status_t
btree_snap_delete_meta_v1(btree_raw_t *bt, uint64_t seqno)
{
	btree_snap_info_v1_t *info;
	int index;

	index = btree_snap_find_meta_index_low(bt, seqno);
	if (index == -1) {
		return BTREE_FAILURE;
	}

	info = &bt->snap_meta->meta.v1_meta.snapshots[index];
	if (info->seqno != seqno) {
		/* while seqno is within range of snapshots,
		 * for deletes, exact seqnos have to be given */
		return BTREE_FAILURE;
	}

#if 0
	info->flag = SNAP_DELETED;
#endif
	//Need to wake up scavenger which should clean up this entry too...
	memmove(&bt->snap_meta->meta.v1_meta.snapshots[index],
	        &bt->snap_meta->meta.v1_meta.snapshots[index+1], 
	        ((bt->snap_meta->total_snapshots - 1 - index) * 
	                    sizeof(btree_snap_info_v1_t)));

	return (BTREE_SUCCESS);
}

btree_status_t
btree_snap_delete_meta(btree_raw_t *bt, uint64_t seqno)
{
	btree_snap_meta_t *smeta = bt->snap_meta;
	btree_status_t ret;

	pthread_rwlock_wrlock(&bt->snap_lock);
	switch (smeta->snap_version) {
	case SNAP_VERSION1:
		ret = btree_snap_delete_meta_v1(bt, seqno);
		if (ret != BTREE_SUCCESS) {
			pthread_rwlock_unlock(&bt->snap_lock);
			return (ret);
		}
		break;

	default:
		assert(0);
	}

	bt->snap_meta->total_snapshots--;
	__sync_sub_and_fetch(&(bt->stats.stat[BTSTAT_NUM_SNAPS]), 1);
	ret = btree_snap_rw_metanode(bt, true /* write */);

	pthread_rwlock_unlock(&bt->snap_lock);
	return (ret);
}

/* 
 * ASSUMPTION: Caller will take the lock */
static int
btree_snap_find_meta_index_low(btree_raw_t *bt, uint64_t seqno)
{
	btree_snap_meta_t *smeta = bt->snap_meta;
	int i_start, i_end, i_check;

	switch (smeta->snap_version) {
	case SNAP_VERSION1 :
		/* If seqno in active container, return right away */
		if ((smeta->total_snapshots == 0) || 
			(seqno > smeta->meta.v1_meta.snapshots[smeta->total_snapshots-1].seqno)) {
			return -1;
		}

		/* Do binary search for snapshot meta */
		i_start = 0;
		i_end = smeta->total_snapshots - 1;

		while (i_end >= i_start) {
			i_check = (i_start + i_end)/2;

			if (seqno <= smeta->meta.v1_meta.snapshots[i_check].seqno) {
				/* Seqno in range between 2 snapshots */
				if ((i_check == 0) ||
					(seqno > smeta->meta.v1_meta.snapshots[i_check-1].seqno)) {
					/* seqno greater than previous and lesser 
					 * than present, we found it */
					return (i_check);
				}
				
				i_end = i_check - 1;
			} else {
				i_start = i_check + 1;
			}
		}
		break;

	default:	
		assert(0);
	}

	return -1;
}

int
btree_snap_find_meta_index(btree_raw_t *bt, uint64_t seqno)
{
	btree_snap_meta_t *smeta = bt->snap_meta;
	int index;

	pthread_rwlock_rdlock(&bt->snap_lock);
	index = btree_snap_find_meta_index_low(bt, seqno);
	pthread_rwlock_unlock(&bt->snap_lock);

	return (index);
}

/* If seqno in active container, return false */
bool
btree_snap_seqno_in_snap(btree_raw_t *bt, uint64_t seqno)
{
	btree_snap_meta_t *smeta = bt->snap_meta;
	int i_start, i_end, i_check;

	switch (smeta->snap_version) {
	case SNAP_VERSION1 :
		if ((smeta->total_snapshots == 0) || 
			(seqno > smeta->meta.v1_meta.snapshots[smeta->total_snapshots-1].seqno)) {
			return false;
		}
		break;

	default:	
		assert(0);
	}

	return true;
}

btree_status_t
btree_snap_get_meta_list(btree_raw_t *bt, uint32_t *n_snapshots,
							 ZS_container_snapshots_t **snap_seqs)
{
	int					i;
	btree_snap_meta_t	*smeta = bt->snap_meta;

	*n_snapshots = smeta->total_snapshots;
	*snap_seqs = (ZS_container_snapshots_t *)malloc(*n_snapshots * sizeof(ZS_container_snapshots_t));
	pthread_rwlock_rdlock(&bt->snap_lock);
	switch (smeta->snap_version) {
		case SNAP_VERSION1 :
			for (i = 0; i < *n_snapshots; i++) {
				if (i == smeta->total_snapshots) {
					break;
				}
				(*snap_seqs)[i].timestamp = smeta->meta.v1_meta.snapshots[i].timestamp;
				(*snap_seqs)[i].seqno = smeta->meta.v1_meta.snapshots[i].seqno;
			}
			break;
		default : assert(0);
	}
	pthread_rwlock_unlock(&bt->snap_lock);

	return (BTREE_SUCCESS);

}
