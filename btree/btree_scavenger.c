#include<stdio.h>
#include "btree_scavenger.h"
#include "btree_sync_mbox.h"
#include "btree_sync_th.h"
#include <string.h>
#include <assert.h>
#include "btree_raw_internal.h"
#include "btree_var_leaf.h"
#include "btree_hash.h"

#define NUM_IN_CHUNK 100
#define MAX_KEYLEN 2000
#define MAX_OPEN_CONTAINERS   (UINT16_MAX - 1 - 9)

extern  __thread struct FDF_thread_state *my_thd_state;
__thread int key_loc = 0;
__thread int start_key_in_node = 0;
static __thread char tmp_key_buf[8100] = {0};

extern void open_container(struct FDF_thread_state *fdf_thread_state, FDF_cguid_t cguid);
extern void close_container(struct FDF_thread_state *fdf_thread_state, Scavenge_Arg_t *S);
extern void get_key_stuff_leaf(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_info_t *key_sinfo);
extern void get_key_stuff_leaf2(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_info_t *key_sinfo);
extern btree_status_t btree_delete(struct btree *btree, char *key, uint32_t keylen, btree_metadata_t *meta);

void
scavenger_worker(uint64_t arg)
{
	Scavenge_Arg_t *s;
	struct FDF_thread_state  *fdf_thread_state;
	int i;

	while(1) {
		uint64_t mail;
		uint64_t          child_id,snap_n1,snap_n2,syndrome = 0;
		int write_lock = 0, snap_no,j,total_keys = 0,deleting_keys = -1,nkey_prev = 0;
		struct btree_raw_mem_node *node;
		int deletedobjects = 0;
		btree_raw_mem_node_t *parent;
		btree_status_t    ret = BTREE_SUCCESS;
		key_stuff_t    ks_current_1;
		key_stuff_info_t ks_prev,ks_current,*key,*key1;
		key_info_t key_info;
		btree_metadata_t  meta;
		btree_status_t    btree_ret = BTREE_SUCCESS;

		mail = btSyncMboxWait(&mbox_scavenger);
		s = (Scavenge_Arg_t *)mail;

		FDFInitPerThreadState(s->fdf_state,&fdf_thread_state);
		my_thd_state = fdf_thread_state;
	 
		open_container(fdf_thread_state, s->cguid);
		node = root_get_and_lock(s->btree, write_lock);
		assert(node);

		while(!is_leaf(s->btree, (node)->pnode)) {
     
			(void) get_key_stuff(s->btree, node->pnode, 0, &ks_current_1);
			child_id = ks_current_1.ptr;
			parent = node;
			node = get_existing_node_low(&ret, s->btree, child_id, 0, false, true);
			plat_rwlock_rdlock(&node->lock);
			plat_rwlock_unlock(&parent->lock);
			deref_l1cache_node(s->btree, parent);

		}
		i = 0;
		bool found = 0,last_node=0;
		start_key_in_node = 0;
		bzero(&ks_prev,sizeof(key_stuff_t));
		while (1)
		{
			key = (key_stuff_info_t *)malloc(16*sizeof(key_stuff_info_t));
			int temp = 0;
			i = scavenge_node(s->btree, node, &ks_prev, &key);
			//if((i == 0) || (key_loc == 0)) {
			if(i == 0) {
				parent = node;
				child_id = node->pnode->next;
				node =  get_existing_node_low(&ret, s->btree, child_id, 0, false, true);
				plat_rwlock_rdlock(&node->lock);
				plat_rwlock_unlock(&parent->lock);
				deref_l1cache_node(s->btree, parent);
				start_key_in_node = 0;
         		continue;
			}
			if (node->pnode->next == 0) {
				last_node = 1;
			}
			plat_rwlock_unlock(&node->lock);
			deref_l1cache_node(s->btree, node);
	
			j=0;
			meta.flags = 0;
			for (j = 0;j < key_loc;j++) {
				meta.seqno = key[j].seqno;
				meta.flags = FORCE_DELETE | READ_SEQNO_EQ;
				btree_ret = btree_delete(s->bt, key[j].key, key[j].keylen, &meta);
				usleep(1000*(s->throttle_value));
				if (btree_ret != BTREE_SUCCESS) {
					fprintf(stderr,"btree delete failed for the key %s\n", key[j].key);
					//goto end need to add after failure in btree logic is fixed
				}
				deletedobjects++;
			}
			memcpy(&ks_current, &(key[key_loc-1]), sizeof(key_stuff_t));
			free(key);
			if (last_node) {
				break;
			}

			uint64_t syndrome = btree_hash((const unsigned char *) ks_current.key, ks_current.keylen, 0); 
			int                   pathcnt = 0;
			bool key_exists = false;
			
			meta.flags = READ_SEQNO_LE;
			meta.end_seqno = ks_current.seqno - 1;
			start_key_in_node = btree_raw_find(s->btree, ks_current.key, ks_current.keylen, syndrome, &meta, &node, write_lock, &pathcnt, &key_exists);
		}
		fprintf(stderr,"Total number of objects scavenged %d \n\n\n",deletedobjects);
		close_container(fdf_thread_state,s);
		FDFReleasePerThreadState(&fdf_thread_state);
	}
}

FDF_status_t btree_scavenge(struct FDF_state  *fdf_state, Scavenge_Arg_t S) 
{
	Scavenge_Arg_t *s;
	int index = -1;

	s = (Scavenge_Arg_t *)malloc(sizeof(Scavenge_Arg_t));
	s->type = S.type;
	s->cguid = S.cguid;
	s->snap_seq = S.snap_seq;
	s->fdf_state = fdf_state;
	s->btree_index = S.btree_index;
	s->btree = S.btree;
	s->bt = S.bt;
	s->throttle_value = S.throttle_value;
	btSyncMboxPost(&mbox_scavenger, (uint64_t)s);
	return FDF_SUCCESS;
}

static inline int
non_minimal_delete(btree_raw_t *btree, btree_raw_mem_node_t *mnode, int index)
{
	btree_status_t ret = BTREE_SUCCESS;

	if (is_leaf_minimal_after_delete(btree, mnode->pnode, index)) {
		return -1;
	}

	delete_key_by_index(&ret, btree, mnode, index);
	if (ret != BTREE_SUCCESS) {
		return -1;
	}

	return 0;
}

static int scavenge_node_all_keys(btree_raw_t *btree, btree_raw_mem_node_t *node)
{
	key_stuff_info_t ks_prev, ks_current;
	int i = 0;
	int x;
	int duplicate_keys = 0;
	int deleting_keys = 0;
	int set_start_index = 0;
	int snap_n1, snap_n2;
	key_meta_t key_meta;
	char *tmp_ptr;
	int scavenged_keys = 0;
	uint64_t scavenged_bytes = 0;
	
	bzero(&ks_prev,sizeof(key_stuff_t));
	bzero(&ks_current,sizeof(key_stuff_t));

	ks_prev.key    = (char *) &tmp_key_buf;
	ks_current.key = (char *) &tmp_key_buf + btree->max_key_size;
	assert((btree->max_key_size * 2) < 8100);

	while (i < node->pnode->nkeys) {
		(void) get_key_stuff_leaf2(btree, node->pnode, i, &ks_current);
		x = (i == 0) ? -1: btree->cmp_cb(btree->cmp_cb_data,
		                                 ks_current.key,
		                                 ks_current.keylen,
		                                 ks_prev.key,
		                                 ks_prev.keylen);

		if (x == 0) {
			snap_n1 = btree_snap_find_meta_index(btree, ks_prev.seqno);
			snap_n2 = btree_snap_find_meta_index(btree, ks_current.seqno);
			duplicate_keys++;

			if (snap_n1 == snap_n2) {
				deleting_keys++;

				uint64_t bytes = ks_current.datalen;
				if (non_minimal_delete(btree, node, i) != 0) {
					goto done;
				}
				scavenged_keys++;
				scavenged_bytes += bytes;
				continue; /* Do not incr index if we delete a key */
			}
		} else {
			/* If there were some duplicate keys and all of them are
			 * deleted, then look if the leader of the set is a 
			 * tombstone, in that case, delete the tombstone as well */
			if (duplicate_keys && (duplicate_keys == deleting_keys)) {
				btree_leaf_get_meta(node->pnode, set_start_index,
				                    &key_meta);
				if (key_meta.tombstone) {
					uint64_t bytes = ks_current.datalen;
					if (non_minimal_delete(btree, node, 
					                       set_start_index) != 0) {
						goto done;
					}
					scavenged_keys++;
					scavenged_bytes += bytes;
					duplicate_keys = deleting_keys = 0;
					continue;
				}
			}
			duplicate_keys = deleting_keys = 0;
			set_start_index = i; // New set if starting
		}
		tmp_ptr = ks_prev.key;
		ks_prev = ks_current;
		ks_current.key = tmp_ptr;
		i++;
	}

done:
	if (scavenged_keys) {
		 __sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_NUM_SNAP_OBJS]), scavenged_keys);
		 __sync_sub_and_fetch(&(btree->stats.stat[BTSTAT_SNAP_DATA_SIZE]), scavenged_bytes);
#if 0
		fprintf(stderr, "In-place scavenger deleted %d keys\n", scavenged_keys);
#endif
	}
	return (scavenged_keys);
}

int scavenge_node(struct btree_raw *btree, btree_raw_mem_node_t* node, key_stuff_info_t *ks_prev_key, key_stuff_info_t **key)
{
	int i = 0, temp;
	key_stuff_info_t ks_prev,ks_current;
	int  total_keys = 0,deleting_keys = -1,nkey_prev = 0;
	uint64_t         snap_n1,snap_n2;
	btree_status_t	ret;
	key_info_t key_info;
	key_stuff_info_t *key1;
	
	if (key == NULL) {
		return (scavenge_node_all_keys(btree, node));
	}

	key_loc = 0;
	i = start_key_in_node;

	key1 = *key;

	memcpy(&ks_prev,ks_prev_key,sizeof(key_stuff_t));
	for(;i<node->pnode->nkeys;i++)
	{
		(void) get_key_stuff_leaf(btree, node->pnode, i, &ks_current);
		temp = btree->cmp_cb(btree->cmp_cb_data, ks_current.key, ks_current.keylen, ks_prev.key, ks_prev.keylen);
		if (!temp)
		{
			snap_n1 = btree_snap_find_meta_index(btree, ks_prev.seqno);
			snap_n2 = btree_snap_find_meta_index(btree, ks_current.seqno);
			total_keys++;
			if (snap_n1 == snap_n2)
			{
				deleting_keys++;
				if (key_loc >= 16 && ((key_loc & key_loc-1)==0)) {
					key1 = (key_stuff_info_t *)realloc(key1,2*key_loc*sizeof(key_stuff_info_t));
					*key = key1;
				}
				memcpy(&(key1[key_loc++]),&ks_current,sizeof(key_stuff_t));
			}
			continue;
		} else {
			if (total_keys == deleting_keys) {
				(void) btree_leaf_get_nth_key_info(btree, node->pnode, nkey_prev, &key_info);
				if(key_info.tombstone == 1)
				{
					if (key_loc >= 16 && ((key_loc & key_loc-1)==0)) {
						key1 = (key_stuff_info_t *)realloc(key1,2*key_loc*sizeof(key_stuff_info_t));
						*key = key1;
					}
					memcpy(&(key1[key_loc++]),&ks_prev,sizeof(key_stuff_t));
				}
			}
			total_keys = deleting_keys = 0;
		}
		nkey_prev = i;
		ks_prev = ks_current;
	}
	memcpy(ks_prev_key,&ks_prev,sizeof(key_stuff_t));
	return i;
}
