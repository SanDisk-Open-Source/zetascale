#include<stdio.h>
#include "btree_scavenger.h"
#include "btree_sync_mbox.h"
#include "btree_sync_th.h"
#include <string.h>
#include <assert.h>
#include "btree.h"
#include "btree_raw.h"
#include "btree_raw_internal.h"
#include "btree_internal.h"
#include "btree_var_leaf.h"
#include "btree_hash.h"
#include "btree_malloc.h"
#include "trx.h"

#define NUM_IN_CHUNK 100
#define MAX_KEYLEN 2000
#define MAX_OPEN_CONTAINERS   (UINT16_MAX - 1 - 9)

int astats_done = 1;
int N_sc_wrk;

extern  __thread struct ZS_thread_state *my_thd_state;
extern __thread ZS_cguid_t my_thrd_cguid;
//extern int bt_storm_mode;
extern bool bt_shutdown;
extern int N_Open_Containers;
extern ctrmap_t Container_Map[];

extern pthread_rwlock_t ctnrmap_rwlock;
extern struct ZS_state *ZSState;

__thread int key_loc = 0;
__thread int nkey_prev_tombstone = 0;
__thread int start_key_in_node = 0;
static __thread char tmp_key_buf[8100] = {0};

extern ZS_status_t astats_open_container(struct ZS_thread_state *zs_thread_state, ZS_cguid_t cguid, astats_arg_t *s);
extern ZS_status_t open_container(struct ZS_thread_state *zs_thread_state, ZS_cguid_t cguid);
extern void close_container(struct ZS_thread_state *zs_thread_state, Scavenge_Arg_t *S);
extern void bt_cntr_unlock_scavenger(Scavenge_Arg_t *s, bool write);
extern ZS_status_t bt_cntr_lock_scavenger(Scavenge_Arg_t *s);
extern btree_t *bt_get_btree_from_cguid(ZS_cguid_t cguid, int *index, ZS_status_t *error, bool write);
extern ZS_status_t _ZSInitPerThreadState(struct ZS_state  *zs_state, struct ZS_thread_state **thd_state);
extern ZS_status_t _ZSReleasePerThreadState(struct ZS_thread_state **thd_state);

extern void astats_deref_container(astats_arg_t *S);

extern void get_key_stuff_leaf(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_info_t *key_sinfo);
extern void get_key_stuff_leaf2(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_info_t *key_sinfo);
extern btree_status_t btree_delete(struct btree *btree, char *key, uint32_t keylen, btree_metadata_t *meta);

extern  ZS_status_t BtreeErr_to_ZSErr(btree_status_t b_status);

static void update_leaf_bytes_count(btree_raw_t *btree, btree_raw_mem_node_t *node);
static void scavenger_del_stale_ent(Scavenge_Arg_t *s);
static void scavenger_del_overflw_n_cont(Scavenge_Arg_t *s);


static btSyncMbox_t mbox_scavenger;
static btSyncMbox_t mbox_scavenger_stopped;

static btSyncMbox_t mbox_astats;
static btSyncMbox_t mbox_astats_stopped;

static int num_scavenger_threads;
static int num_astats_threads;

static int scavenger_killed = false;
static int astats_killed = false;

void
scavenger_worker(uint64_t arg)
{
    Scavenge_Arg_t *s;
    struct ZS_thread_state  *zs_thread_state;
    int i;

	if (ZS_SUCCESS != _ZSInitPerThreadState(ZSState, &zs_thread_state)) {
		return;
	}

	(void) __sync_add_and_fetch(&N_sc_wrk, 1);

	my_thd_state = zs_thread_state;

	while(!scavenger_killed) {
        uint64_t mail;

		mail = btSyncMboxWait(&mbox_scavenger);

		if (!mail) {
			break;
		}

		s = (Scavenge_Arg_t *)mail;
		my_thrd_cguid = 0; /* To avoid recurrence triggering of scavenger from btree_delete rouinte after ZS_SCAVENGE_PER_OBJECTS deletes*/

		switch (s->type) {
			case SC_OVERFLW_DELCONT:
				scavenger_del_overflw_n_cont(s);
				break;

			case SC_STALE_ENT:
				scavenger_del_stale_ent(s);
				break;
		}
		free(s);
    }

	btSyncMboxPost(&mbox_scavenger_stopped, 0);
}

/*
 * This routine deletes stale entries i.e. two consecutive entries belonging to same
 * snapshot
 */
static void
scavenger_del_stale_ent(Scavenge_Arg_t *s)
{
	uint64_t					child_id,snap_n1,snap_n2,syndrome = 0;
	int							write_lock = 0, snap_no,j,total_keys = 0,deleting_keys = -1,nkey_prev = 0;
	struct btree_raw_mem_node	*node;
	int 						deletedobjects = 0;
	btree_raw_mem_node_t 		*parent;
	btree_status_t				ret = BTREE_SUCCESS;
	key_stuff_t					ks_current_1;
	key_stuff_info_t			ks_prev,ks_current,*key,*key1;
	key_info_t					key_info;
	btree_metadata_t			meta;
	btree_status_t				btree_ret = BTREE_SUCCESS;
	ZS_status_t					zs_ret;
	int							i;

	assert(my_thd_state);
	open_container(my_thd_state, s->cguid);

	plat_rwlock_rdlock(&s->btree->lock);
	node = root_get_and_lock(s->btree, write_lock, &ret);
	if (node == NULL) {
		goto cleanup;
	}

	while(!is_leaf(s->btree, (node)->pnode)) {
 
		(void) get_key_stuff(s->btree, node->pnode, 0, &ks_current_1);
		child_id = ks_current_1.ptr;
		parent = node;

		node = get_existing_node(&ret, s->btree, child_id, NODE_CACHE_DEREF_DELETE,
		                         LOCKTYPE_READ);
		if (node == NULL) {
			plat_rwlock_unlock(&parent->lock);
			deref_l1cache_node(s->btree, parent);
			goto cleanup;
		}

		plat_rwlock_unlock(&parent->lock);
		deref_l1cache_node(s->btree, parent);

	}
	i = 0;
	bool found = 0,last_node=0;
	start_key_in_node = 0;
	nkey_prev_tombstone = 0;
	bzero(&ks_prev,sizeof(key_stuff_t));
	while (!bt_shutdown) {
		key = (key_stuff_info_t *)malloc(16*sizeof(key_stuff_info_t));
		int temp = 0;

		if (start_key_in_node < node->pnode->nkeys) {
			i = scavenge_node(s->btree, node, &ks_prev, &key);
		} else {
			i = 0;
		}
		if((i == 0) || (key_loc == 0)) {
			parent = node;
			child_id = node->pnode->next;
			if (child_id == 0) {
				plat_rwlock_unlock(&parent->lock);
				deref_l1cache_node(s->btree, parent);
				break;
			}

			node = get_existing_node(&ret, s->btree, child_id,
			                        NODE_CACHE_DEREF_DELETE, LOCKTYPE_READ);
			if (node == NULL) {
				// Need to replace this with assert(0) once the broken leaf node chain in btree is fixed
				plat_rwlock_unlock(&parent->lock);
				deref_l1cache_node(s->btree, parent);
				break;
			}

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

		/* Unlock for delete to proceed */
		plat_rwlock_unlock(&s->btree->lock);

		j=0;
		meta.flags = 0;
		for (j = 0;j < key_loc;j++) {
			meta.seqno = key[j].seqno;
			meta.flags = FORCE_DELETE | READ_SEQNO_EQ;
			btree_ret = btree_delete(s->bt, key[j].key, key[j].keylen, &meta);
			bt_cntr_unlock_scavenger(s, false);
			if (!bt_shutdown) {
				usleep(1000*(s->throttle_value));
			}
			zs_ret = bt_cntr_lock_scavenger(s);
			if (s->btree == NULL) {
				fprintf(stderr,"bt_get_btree_from_cguid failed with error: %s\n", ZSStrError(zs_ret));
				free(key);
				return;
			}
			if (btree_ret != BTREE_SUCCESS) {
				//fprintf(stderr,"btree delete failed for the key %s and error is %s\n", key[j].key,ZSStrError(BtreeErr_to_ZSErr(btree_ret)));
				//goto end need to add after failure in btree logic is fixed
			}
			deletedobjects++;
		}
		memcpy(&ks_current, &ks_prev, sizeof(key_stuff_t));
		free(key);

		/* Relock before proceeding to walk down the tree */
		plat_rwlock_rdlock(&s->btree->lock);
		if (last_node) {
			break;
		}

		uint64_t syndrome = btree_hash_int((const unsigned char *) ks_current.key, ks_current.keylen, 0); 
		int                   pathcnt = 0;
		bool key_exists = false;
		
		meta.flags = READ_SEQNO_LE;
		meta.end_seqno = ks_current.seqno - 1;
		start_key_in_node = btree_raw_find(&ret, s->btree, ks_current.key, ks_current.keylen, syndrome, &meta, &node, write_lock, &pathcnt, &key_exists);

		if (node == NULL) {
			break;
		}
	}

cleanup:
	plat_rwlock_unlock(&s->btree->lock);

	fprintf(stderr,"Total number of objects scavenged %d \n\n\n",deletedobjects);
	close_container(my_thd_state, s);
}


/*
 * This routine deletes all overflow nodes first and then deletes container.
 * IMPORTANT: This routine to be used in strom mode where overflow nodes are RAW objects
 */
static void
scavenger_del_overflw_n_cont(Scavenge_Arg_t *s)
{
	uint64_t					child_id,snap_n1,snap_n2,syndrome = 0;
	int							write_lock = 0, snap_no,j,total_keys = 0,deleting_keys = -1,nkey_prev = 0;
	struct btree_raw_mem_node	*node;
	int 						deletedobjects = 0;
	btree_raw_mem_node_t 		*parent;
	btree_status_t				ret = BTREE_SUCCESS;
	key_stuff_t					ks_current_1;
	key_stuff_info_t			ks_prev,ks_current,*key,*key1;
	key_info_t					key_info;
	btree_metadata_t			meta;
	btree_status_t				btree_ret = BTREE_SUCCESS;
	ZS_status_t					zs_ret;
	int							i, flag;
	key_meta_t					key_meta;

	/*
	 * Acquire the lock of the container map. The sender had taken the lock and unlocked it.
	 * We have to take the lock again here fom scavenger context.
	 * We can be sure of consistent entry since we had incremented bt_wr_count from the sender.
	 */
	pthread_rwlock_wrlock(&Container_Map[s->btree_index].bt_cm_rwlock);

	if (bt_shutdown == true) {
		bt_cntr_unlock_scavenger(s, true);
		return;
	}

	assert(bt_storm_mode == 1);
	assert(s->btree->snap_meta->sc_status & SC_OVERFLW_DELCONT); //Only called by DelCont
	assert(my_thd_state);

	open_container(my_thd_state, s->cguid);
	node = root_get_and_lock(s->btree, write_lock, &ret);
	if (node == NULL) {
		goto cleanup;
	}

	while(!is_leaf(s->btree, (node)->pnode)) {
 
		(void) get_key_stuff(s->btree, node->pnode, 0, &ks_current_1);
		child_id = ks_current_1.ptr;

		parent = node;

		node = get_existing_node(&ret, s->btree, child_id, NODE_CACHE_DEREF_DELETE,
		                         LOCKTYPE_LEAF_WRITE_REST_READ);
		if (node == NULL) {
			node_unlock(parent);
			deref_l1cache_node(s->btree, parent);
			goto cleanup;
		}

		node_unlock(parent);
		deref_l1cache_node(s->btree, parent);

	}

	while (1) {
		flag = 0;
		for (i = 0; i < node->pnode->nkeys; i++) {
			btree_leaf_get_meta(node->pnode, i, &key_meta);
			if ((key_meta.keylen + key_meta.datalen) >=
								s->btree->big_object_size) {
				flag = 1;
				delete_overflow_data(&ret, s->btree, node->pnode, key_meta.ptr, key_meta.datalen);
				btree_leaf_unset_dataptr(node->pnode, i);
			}
		}
		if (flag == 1) {
			uint64_t *logical_id = &node->pnode->logical_id;
			s->bt->write_node_cb(my_thd_state, &ret, s->bt->write_node_cb_data,
								&logical_id, (char**)&node->pnode,
								s->bt->nodesize, 1, 0);
			deref_l1cache(s->btree);
		}

		child_id = node->pnode->next;
		parent = node;

		if (child_id == 0) {
			node_unlock(parent);
			deref_l1cache_node(s->btree, parent);

			zs_ret = ZSCloseContainer(my_thd_state, s->cguid);
			assert((zs_ret == ZS_SUCCESS) || (bt_shutdown==true && zs_ret==ZS_FAILURE_OPERATION_DISALLOWED));
			trxdeletecontainer( my_thd_state, s->cguid);

			Container_Map[s->btree_index].cguid = ZS_NULL_CGUID;
			Container_Map[s->btree_index].btree = NULL;
			Container_Map[s->btree_index].bt_state = BT_CNTR_UNUSED;
			Container_Map[s->btree_index].scavenger_state = false;
			(void) __sync_sub_and_fetch(&N_Open_Containers, 1);

			bt_cntr_unlock_scavenger(s, true);

			btree_destroy(s->bt, true);

			zs_ret = ZSDeleteContainer(my_thd_state, s->cguid);
			assert((zs_ret == ZS_SUCCESS) || (bt_shutdown==true && zs_ret==ZS_FAILURE_OPERATION_DISALLOWED));

			return;
		} else if (bt_shutdown == true) {
			node_unlock(parent);
			deref_l1cache_node(s->btree, parent);
			break;
		} else {
			node = get_existing_node(&ret, s->btree, child_id,
			                         NODE_CACHE_DEREF_DELETE, LOCKTYPE_WRITE);
			if (node == NULL) {
				node_unlock(parent);
				deref_l1cache_node(s->btree, parent);
				break;
			}
			node_unlock(parent);
			deref_l1cache_node(s->btree, parent);
		}
	}

cleanup:
	bt_cntr_unlock_scavenger(s, true);

	return;
}



void
scavenger_init(int nthreads) {
	int i;

	num_scavenger_threads = nthreads;

    btSyncMboxInit(&mbox_scavenger);
    btSyncMboxInit(&mbox_scavenger_stopped);

    for (i = 0;i < num_scavenger_threads;i++) {
	btSyncResume(btSyncSpawn(NULL,i,&scavenger_worker),(uint64_t)NULL);
    }
}

void
scavenger_stop() {
	int i;

	scavenger_killed = true;

	for(i = 0; i < num_scavenger_threads; i++)
		btSyncMboxPost(&mbox_scavenger, 0);

	while(num_scavenger_threads--)
		(void)btSyncMboxWait(&mbox_scavenger_stopped);
}


ZS_status_t btree_scavenge(struct ZS_state  *zs_state, Scavenge_Arg_t S) 
{
	Scavenge_Arg_t *s;
	int index = -1;

	s = (Scavenge_Arg_t *)malloc(sizeof(Scavenge_Arg_t));
	s->type = S.type;
	s->cguid = S.cguid;
	s->snap_seq = S.snap_seq;
	s->zs_state = zs_state;
	s->btree_index = S.btree_index;
	s->btree = S.btree;
	s->bt = S.bt;
	s->throttle_value = S.throttle_value;
	btSyncMboxPost(&mbox_scavenger, (uint64_t)s);
	return ZS_SUCCESS;
}


ZS_status_t
btree_start_astats(struct ZS_state  *zs_state, astats_arg_t S) 
{
	astats_arg_t *s;

	s = (astats_arg_t*)malloc(sizeof(astats_arg_t));
	s->cguid = S.cguid;
	s->zs_state = zs_state;
	s->btree = S.btree;
	s->bt = S.bt;
	s->btree_index = S.btree_index;
    s->suspend_duration = S.suspend_duration;
    s->suspend_after_node_count = S.suspend_after_node_count;
	btSyncMboxPost(&mbox_astats, (uint64_t)s);
	return ZS_SUCCESS;
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

static int 
scavenge_node_all_keys(btree_raw_t *btree, btree_raw_mem_node_t *node)
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

#ifdef DEBUG_BUILD 
					key_info_t key_info1 = {0};
					key_info_t key_info2 = {0};
					btree_leaf_get_nth_key_info(btree, node->pnode, set_start_index, &key_info1);
#endif 
					if (non_minimal_delete(btree, node, 
					                       set_start_index) != 0) {
						goto done;
					}
#ifdef DEBUG_BUILD 
					btree_leaf_get_nth_key_info(btree, node->pnode, set_start_index, &key_info2);

					assert(btree->cmp_cb(btree->cmp_cb_data, key_info1.key, key_info1.keylen,
							     key_info2.key, key_info2.keylen) < 0);	
					btree_free(key_info1.key);
					btree_free(key_info2.key);
#endif 

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

int 
scavenge_node(struct btree_raw *btree, btree_raw_mem_node_t* node, key_stuff_info_t *ks_prev_key, key_stuff_info_t **key)
{
	if (0 == astats_done) {
		return 0;
	}

	int i = 0, temp;
	key_stuff_info_t ks_prev,ks_current,temp1;
	int  total_keys = 0,deleting_keys = 0,nkey_prev = 0;
	uint64_t         snap_n1,snap_n2;
	btree_status_t	ret;
	key_info_t key_info;
	key_stuff_info_t *key1;

		
	if (key == NULL) {
		return (scavenge_node_all_keys(btree, node));
	}

	key_loc = 0;
	i = start_key_in_node;
	//meta.flags = 0;
	key1 = *key;
	
	bzero(&temp1,sizeof(key_stuff_info_t));
	memcpy(&ks_prev,ks_prev_key,sizeof(key_stuff_t));
	(void) get_key_stuff_leaf(btree, node->pnode, i, &ks_current);
	for(;i<node->pnode->nkeys;i++)
	{
		(void) get_key_stuff_leaf(btree, node->pnode, i, &ks_current);
		if (ks_current.key == NULL || ks_current.keylen == 0) {
			temp = -1;
		} else if (ks_prev.key == NULL || ks_prev.keylen == 0) {
			temp = -1;
		} else {
			temp = btree->cmp_cb(btree->cmp_cb_data, ks_current.key, ks_current.keylen, ks_prev.key, ks_prev.keylen);
		}
		if (!temp)
		{
			if (ks_prev.seqno != ks_current.seqno) {
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
					temp1 = ks_current;
				}
			}
			continue;
		} else {
			if (total_keys == deleting_keys) {
				if(temp1.keylen != 0) {
					temp = btree->cmp_cb(btree->cmp_cb_data, temp1.key, temp1.keylen, ks_prev.key, ks_prev.keylen);
				} else { 
					temp = 1;
				}

				if (!temp && nkey_prev_tombstone == 1)
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
		(void) btree_leaf_get_nth_key_info(btree, node->pnode,i, &key_info);
		nkey_prev_tombstone = key_info.tombstone;
		nkey_prev = i;
		ks_prev = ks_current;
	}
	memcpy(ks_prev_key,&ks_prev,sizeof(key_stuff_t));
	return i;
}


#define big_object1(bt, x) (((x)->keylen + (x)->datalen) >= (bt)->big_object_size)

static void
update_leaf_bytes_count(btree_raw_t *btree, btree_raw_mem_node_t *node)
{
    assert(btree);
    assert(node);

    uint32_t used_space = 0;
    int i = 0;
    key_stuff_info_t ks_current;
    btree_status_t      ret=BTREE_SUCCESS, ret2 = BTREE_SUCCESS;

    used_space = sizeof(btree_raw_node_t) + btree_leaf_used_space(btree, node->pnode);
    __sync_add_and_fetch(&(btree->stats.stat[BTSTAT_LEAF_BYTES]), used_space);

    char buffer[8192];
    uint64_t            nbytes;
    uint64_t            copybytes;
    bzero(&ks_current, sizeof(key_stuff_t));
    ks_current.key = (char *) buffer; 
    bool ref = 0;
    uint64_t            z_next;
    btree_raw_node_t   *z;
    uint64_t onodes = 0, obytes = 0;
    key_info_t 	key_info = {0};
    key_info.key = buffer;

    while (i < node->pnode->nkeys) {
        key_info.key = buffer;
        assert(btree_leaf_get_nth_key_info2(btree, node->pnode, i, &key_info));

        obytes += key_info.datalen;

        if (big_object1(btree, (&key_info))) {
            z_next = key_info.ptr;

            uint32_t nflags = NODE_CACHE_DEREF_DELETE | (ref ? NODE_REF: 0);
            while(z_next) {
                btree_raw_mem_node_t *node1 = get_existing_overflow_node(&ret2, btree, z_next, nflags);

                if(!node1) {
                    break;
                }

                ++onodes;

                z = node1->pnode;
                obytes += sizeof(btree_raw_node_t);
                z_next  = z->next;

                if (!ref) {
                    deref_l1cache_node(btree, node1);
                }
            }
        }
        ++i;
    }
    //fprintf(stderr, "Overflow nodes=%ld bytes=%ld\n", onodes, obytes);
    __sync_add_and_fetch(&(btree->stats.stat[BTSTAT_OVERFLOW_NODES]), onodes);
    __sync_add_and_fetch(&(btree->stats.stat[BTSTAT_OVERFLOW_BYTES]), obytes); 
}


void
astats_worker(uint64_t arg)
{
    astats_arg_t *s;
    struct ZS_thread_state  *zs_thread_state;
    int i;

    while (astats_killed) {
        uint64_t mail;
        uint64_t child_id;

        int write_lock = 0, j;
        struct btree_raw_mem_node *node;
        btree_raw_mem_node_t *parent = NULL;

        btree_status_t    ret = BTREE_SUCCESS;
        key_stuff_t    ks_current_1;
        btree_metadata_t  meta;

        mail = btSyncMboxWait(&mbox_astats);
        if(!mail)
            break;
        s = (astats_arg_t*)mail;

        _ZSInitPerThreadState(s->zs_state, &zs_thread_state);
        my_thd_state = zs_thread_state;

        /*
         * Open the container
         */
        if (ZS_SUCCESS != astats_open_container(zs_thread_state, s->cguid, s)) {
            goto astats_worker_exit;
        }

        /*
         * Acquire read lock on root
         */
        node = root_get_and_lock(s->btree, write_lock, &ret);
        if (node == NULL) {
            goto out;
        }

        while(!is_leaf(s->btree, (node)->pnode)) {
            (void) get_key_stuff(s->btree, node->pnode, 0, &ks_current_1);
            child_id = ks_current_1.ptr;
            parent = node;

            node = get_existing_node(&ret, s->btree, child_id, NODE_CACHE_DEREF_DELETE, LOCKTYPE_READ);
            if (node == NULL) {
                plat_rwlock_unlock(&parent->lock);
                deref_l1cache_node(s->btree, parent);
                goto out;
            }

            plat_rwlock_unlock(&parent->lock);
            deref_l1cache_node(s->btree, parent);
        }

        int count = 0;
        int duplicate = 0;

        while (astats_killed) {
            parent = node;
            child_id = node->pnode->next;

#ifdef ASTATS_DEBUG
            //fprintf(stderr, "Async stats: leaf child_id=%ld uid=%ld\n", child_id, node->pnode->pstats.seq);
#endif

            /*
             * Only if the node is part of old session
             */
            if (parent->pnode->logical_id < s->btree->logical_id_counter) {
                if (duplicate) {
                    duplicate = 0;
                } else {
                    __sync_add_and_fetch(&(s->btree->stats.stat[BTSTAT_LEAF_NODES]), 1);
                    update_leaf_bytes_count(s->btree, parent);
                }
            }

            if (0 == child_id) {
                //fprintf(stderr, "Async stats: null child total leaves= %ld\n", s->btree->stats.stat[BTSTAT_LEAF_NODES]);
                break;
            }

            node = get_existing_node(&ret, s->btree, child_id, NODE_CACHE_DEREF_DELETE, LOCKTYPE_READ);
            if (NULL == node) { 
                fprintf(stderr, "Async stats: error null node total leaves= %ld\n", s->btree->stats.stat[BTSTAT_LEAF_NODES]);
                break;
            }

            if (count++ == s->suspend_after_node_count) {
                key_stuff_info_t ks_info;
                struct timespec tim, tim2;

                tim.tv_sec = 0;
                tim.tv_nsec = s->suspend_duration*1000000;

                bzero(&ks_info, sizeof(key_stuff_info_t));

                ks_info.key = (char *) &tmp_key_buf;

                (void) get_key_stuff_leaf2(s->btree, parent->pnode, parent->pnode->nkeys - 1, &ks_info);

                uint64_t pre_sleep_child_id = parent->pnode->logical_id;

                plat_rwlock_unlock(&parent->lock);
                deref_l1cache_node(s->btree, parent);

                plat_rwlock_unlock(&node->lock);
                deref_l1cache_node(s->btree, node);

                parent = node = NULL;

                astats_deref_container(s);

                if (nanosleep(&tim , &tim2) < 0 )   
                {
                    fprintf(stderr, "Nano sleep system call failed \n");
                    break;
                }

                count = 0;

                /*
                 * Re-open the container
                 */
                if (ZS_SUCCESS != astats_open_container(zs_thread_state, s->cguid, s)) {
                    goto astats_worker_exit;
                }

                /*
                 * Get the node that has next key
                 */
                uint64_t syndrome = btree_hash_int((const unsigned char *) ks_info.key, ks_info.keylen, 0); 
                int pathcnt = 0;
                bool key_exists = false;
                meta.flags = 0;

                int start_key_in_node = btree_raw_find(&ret, s->btree, ks_info.key, ks_info.keylen,
                        syndrome, &meta, &node, write_lock, &pathcnt, &key_exists);
                if (NULL == node) {
                    fprintf(stderr, "astats_worker: btree_raw_find failed \n");
                    break;
                }

                uint64_t post_sleep_child_id = node->pnode->logical_id;
#ifdef ASTATS_DEBUG
                fprintf(stderr, "astats_worker: pre = %ld post = %ld\n", pre_sleep_child_id, post_sleep_child_id);
#endif
                if (pre_sleep_child_id == post_sleep_child_id) {
                    duplicate = 1; 
                }
                continue;
            }

            plat_rwlock_unlock(&parent->lock);
            deref_l1cache_node(s->btree, parent);
        }

        if (parent) {
            plat_rwlock_unlock(&parent->lock);
            deref_l1cache_node(s->btree, parent);
        }

out:
        astats_deref_container(s);

astats_worker_exit:
        _ZSReleasePerThreadState(&zs_thread_state);
        fprintf(stderr, "astats worker is done\n");
        astats_done = 1;
    }

    btSyncMboxPost(&mbox_astats_stopped, 0);
}

void astats_init(int nthreads) {
	int i;

	num_astats_threads = nthreads;

    btSyncMboxInit(&mbox_astats);
    btSyncMboxInit(&mbox_astats_stopped);

    for (i = 0; i < num_astats_threads; i++) {
        btSyncResume(btSyncSpawn(NULL, i, &astats_worker),(uint64_t)NULL);
    }
}

void
astats_stop() {
	int i;

	astats_killed = true;

	for(i = 0; i < num_astats_threads; i++)
		btSyncMboxPost(&mbox_astats, 0);

	while(num_astats_threads--)
		(void)btSyncMboxWait(&mbox_astats_stopped);
}

