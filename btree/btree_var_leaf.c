/*
 * Compact format of btree leaf node.
 *
 * Author: Ramesh Chander.
 * Created on Nov, 2013.
 * (c) Sandisk Inc.
 */


/*
 * === BTREE LEAF NODE FORMAT ===
 *
 * The btree leaf node has a format that stores keys and it's metadata in a compacted format.
 * At start of node are the headers for all keys in sorted order. These headers grow from left to right.
 * At end of the node is key's metadata, actual key and data. This part grows from right to left.
 * | hdr1 | hdr2 | hdr3 | ......Growing Area...... | meta1 (type1) | key1| data1| meta2 (type3)| key2| data2|......|meta n (type2) |key n|data n|
* This format allows us to insert the entries in node with minimal movement of the data in node.
*
* === Variable Metadata Structure ===
* Since there could be multiple size of keys and data for those keys, the metadata structure of each key
* can be of different size. For example, if key is less than 256 bytes, we can store the key length just in 1 * byte.  This node format makes this possible by having some predefined types of metadata structure and chosei
* one of them dynamically as keys are inserted.  
* 
*  === Prefix of the keys ===
* Keys could have common duplicate part at start of the keys, for e.g. the rowname in FDF-cassandra.
* In order to avoid storing duplicate parts multiple time, we user prefix concept in node.
* The kyes are split in to two parts, the first part if the prefix and second part is the short key.
* The prefix is the part of key that is actually pointing to another key to left of this key.
* In order to read full key, we need to collect both part of the key, combine them and then 
* return it to user.
* The metadata part of the keys tells if key has a prefix and if it has where it is. 
* The prefix part of key could be NULL it does not have common part with any of the key previous to it.
* If prefix of the key is NULL, the short key is the full key. 
*/


#include <assert.h>
#include <string.h>
#include <stdio.h>

#include "btree_malloc.h"
#include "btree_var_leaf.h"

//#define COLLECT_TIME_STATS
#undef COLLECT_TIME_STATS


#define MAX_UINT8_T  0xff 
#define MAX_UINT16_T 0xffff

uint64_t bt_leaf_insert_kbi_total_time = 0;
uint64_t bt_leaf_insert_kbi_count = 0;
uint64_t bt_leaf_split_count = 0;
uint64_t bt_leaf_split_time = 0;
uint64_t bt_leaf_split_memcpy_time = 0;
uint64_t bt_leaf_memcpy_time = 0;
uint64_t bt_leaf_is_full = 0;

char global_tmp_data[4096] = {0};
static __thread char tmp_leaf_node[8200] = {0};
static __thread char tmp_key_buf[8100] = {0};

#ifdef DEBUG_BUILD		
#define dbg_assert(x) assert(x)
#else
#define dbg_assert(x) ;
#endif


bool
btree_leaf_get_nth_key_info2(btree_raw_t *btree, btree_raw_node_t *n, int index,
		       	    key_info_t *key_info);

/*
 * Get common part of two keys.
 *
 * returns length that is common.
 */
#define MIN_COMMON_LENGTH 10
static int
get_common_part(char *key1, uint32_t keylen1, char *key2, uint32_t keylen2)
{
        int i = 0;

        while (i < keylen1 && i < keylen2 && key1[i] == key2[i]) {
                i++;
        }

	if (i < MIN_COMMON_LENGTH) {
		return 0;
	}
        return i;
}

void
btree_leaf_init(btree_raw_t *bt, btree_raw_node_t *n)
{
	memset((char *) ((uint64_t) n + sizeof(btree_raw_node_t)), 0 , bt->nodesize_less_hdr);
	n->flags = LEAF_NODE;
	n->insert_ptr = bt->nodesize;
	n->rightmost  = BAD_CHILD;
	n->nkeys = 0;

}

static inline int32_t  
get_meta_type_to_size(key_meta_type_t meta_type)
{
	switch(meta_type) {
	
		case BTREE_KEY_META_TYPE1:
			return sizeof(key_meta_type1_t);
		case BTREE_KEY_META_TYPE2:
			return sizeof(key_meta_type2_t);
		case BTREE_KEY_META_TYPE3:
			return sizeof(key_meta_type3_t);
		case BTREE_KEY_META_TYPE4:
			return sizeof(key_meta_type4_t);
		default:
			dbg_assert(0);
			return 0;
	}
}

static int32_t
get_key_meta_space_used(key_meta_t *key_meta)
{
	int32_t space_used = 0;

	space_used += get_meta_type_to_size(key_meta->meta_type);
	space_used += key_meta->keylen - key_meta->prefix_len;
	if (key_meta->ptr == 0) {
		space_used += key_meta->datalen;
	}
	return space_used;
}

static inline void
key_meta1_to_key_meta(btree_raw_node_t *n, key_meta_type1_t *key_meta1,
		      key_meta_t *key_meta, int index)
{
	/*
	 * get previous entry from index, the metadata is same for 
	 * these entries.
	 */
	dbg_assert(index >= 0);
	btree_leaf_get_meta(n, index, key_meta);

	key_meta->seqno = key_meta1->seqno;
	key_meta->meta_type = BTREE_KEY_META_TYPE1;
	key_meta->compact_meta = true;
}

static inline void
key_meta2_to_key_meta(btree_raw_node_t *n, key_meta_type2_t *key_meta2, key_meta_t *key_meta)
{
	key_meta->keylen = key_meta2->keylen;
	key_meta->datalen = key_meta2->datalen;
	key_meta->prefix_idx = key_meta2->prefix_idx;
	key_meta->prefix_len = key_meta2->prefix_len;

	key_meta->seqno = key_meta2->seqno;
	key_meta->ptr = 0;

}

static void
key_meta3_to_key_meta(btree_raw_node_t *n, key_meta_type3_t *key_meta3, key_meta_t *key_meta)
{
	key_meta->keylen = key_meta3->keylen;
	key_meta->datalen = key_meta3->datalen;
	key_meta->prefix_idx = key_meta3->prefix_idx;
	key_meta->prefix_len = key_meta3->prefix_len;

	key_meta->seqno = key_meta3->seqno;
	key_meta->ptr = key_meta3->ptr;
}

static void
key_meta4_to_key_meta(btree_raw_node_t *n, key_meta_type4_t *key_meta4, key_meta_t *key_meta)
{
	key_meta->keylen = key_meta4->keylen;
	key_meta->datalen = key_meta4->datalen;
	key_meta->prefix_idx = key_meta4->prefix_idx;
	key_meta->prefix_len = key_meta4->prefix_len;

	key_meta->seqno = key_meta4->seqno;
	key_meta->ptr = key_meta4->ptr;
}

/*
 * Giuen a node and index, seraches the key_meta recursively.
 */
void inline 
btree_leaf_get_meta(btree_raw_node_t *n, int index, key_meta_t *key_meta)
{
	entry_header_t *headers = NULL;
	char *data_ptr = NULL;
	entry_header_t *ent_hdr = NULL;

	headers = (entry_header_t *) ((char *) n->keys);
	data_ptr = (char *) ((uint64_t )n + n->insert_ptr);
	ent_hdr = &headers[index];

	switch (ent_hdr->meta_type) {

	case BTREE_KEY_META_TYPE1: {
		key_meta_type1_t *key_meta1 = 
			(key_meta_type1_t *) (ent_hdr->header_offset + (uint64_t) data_ptr);

		key_meta1_to_key_meta(n, key_meta1, key_meta, index - 1);

		break;
	}	

	case BTREE_KEY_META_TYPE2: {
		key_meta_type2_t *key_meta2 = 
			(key_meta_type2_t *) (ent_hdr->header_offset + (uint64_t) data_ptr);

		key_meta2_to_key_meta(n, key_meta2, key_meta);

		break;
	}

	case BTREE_KEY_META_TYPE3: {
		key_meta_type3_t *key_meta3 = 
			(key_meta_type3_t *) (ent_hdr->header_offset + (uint64_t) data_ptr);

		key_meta3_to_key_meta(n, key_meta3, key_meta);

		break;
	}

	case BTREE_KEY_META_TYPE4: {
		key_meta_type4_t *key_meta4 = 
			(key_meta_type4_t *) (ent_hdr->header_offset + (uint64_t) data_ptr);

		key_meta4_to_key_meta(n, key_meta4, key_meta);

		break;
	}

	default:
		dbg_assert(0);
	}

	key_meta->meta_type = ent_hdr->meta_type;

	dbg_assert(key_meta->keylen != 0);
	dbg_assert(key_meta->prefix_len <= key_meta->keylen);
}

/*
 * Adjust the prefix index of entries that has index > base index.
 *
 * Used in insertion and remove of key.
 */
void
btree_leaf_adjust_prefix_idx(btree_raw_node_t *n, int index, int base, int shift)
{
	entry_header_t *headers = NULL;
	char *data_ptr = NULL;
	entry_header_t *ent_hdr = NULL;

	headers = (entry_header_t *) n->keys;
	data_ptr = (char *) ((uint64_t )n + n->insert_ptr);
	ent_hdr = &headers[index];

	switch (ent_hdr->meta_type) {

	case BTREE_KEY_META_TYPE1: {
		key_meta_type1_t *key_meta1 = 
			(key_meta_type1_t *) (ent_hdr->header_offset + (uint64_t) data_ptr);
		/*
		 * No meta in this entry.
		 */
		break;
	}	

	case BTREE_KEY_META_TYPE2: {
		key_meta_type2_t *key_meta2 = 
			(key_meta_type2_t *) (ent_hdr->header_offset + (uint64_t) data_ptr);

		if (key_meta2->prefix_len > 0 && 
		    key_meta2->prefix_idx > base) {
			key_meta2->prefix_idx += shift;
			dbg_assert(key_meta2->prefix_idx < index);
		}
		dbg_assert(key_meta2->keylen != 0);
		break;
	}

	case BTREE_KEY_META_TYPE3: {
		key_meta_type3_t *key_meta3 = 
			(key_meta_type3_t *) (ent_hdr->header_offset + (uint64_t) data_ptr);

		if (key_meta3->prefix_len > 0 &&
		    key_meta3->prefix_idx > base) {
			key_meta3->prefix_idx += shift;
			dbg_assert(key_meta3->prefix_idx < index);
		}
		dbg_assert(key_meta3->keylen != 0);
		break;
	}

	case BTREE_KEY_META_TYPE4: {
		key_meta_type4_t *key_meta4 = 
			(key_meta_type4_t *) (ent_hdr->header_offset + (uint64_t) data_ptr);

		if (key_meta4->prefix_len > 0 &&
		    key_meta4->prefix_idx > base) {
			key_meta4->prefix_idx += shift;
			dbg_assert(key_meta4->prefix_idx < index);
		}
		dbg_assert(key_meta4->keylen != 0);
		break;
	}

	default:
		dbg_assert(0);
	}
}

/*
 * Adjust prefix idx of all entries by value of shift 
 * starting from from to to that have prefix_idx > base.
 */
void
btree_leaf_adjust_prefix_idxes(btree_raw_node_t *n, int from, int to, int base, int shift)
{
	int i = 0;
	for (i = from; i <= to; i++) {
		btree_leaf_adjust_prefix_idx(n, i, base, shift);
	}
}

/*
 * Given a prefix index, returns pointer to key where our prefix index points to.
 */
char *
get_key_prefix(btree_raw_node_t *n, int index)
{
	key_meta_t key_meta = {0};
	char *key = NULL;
	char *data_ptr = NULL;
	entry_header_t *headers = NULL;
	entry_header_t *ent_hdr = NULL;

	headers = (entry_header_t *) n->keys;
	ent_hdr = &headers[index];
	data_ptr = (char *) ((uint64_t )n + n->insert_ptr);

	/*
	 * get key metadata
	 */
	btree_leaf_get_meta(n, index, &key_meta);

	if (key_meta.prefix_len != 0) {
		dbg_assert(0); //We dont support chains right now
		key = get_key_prefix(n, key_meta.prefix_idx);
	} else {
		/*
		 * This is end of the prefix chain.
		 */
		key = (char *) (data_ptr + get_meta_type_to_size(key_meta.meta_type) +
				ent_hdr->header_offset);
	}

	dbg_assert(key);	
	return key;
}

static inline key_meta_type_t
get_key_meta_type(btree_raw_t *bt, key_meta_t *key_meta)
{

	if (key_meta->compact_meta) {
		/*
		 * Meta type 1.
		 */
		return BTREE_KEY_META_TYPE1;
	}

	if (key_meta->keylen < MAX_UINT8_T &&
	    key_meta->datalen < MAX_UINT8_T) {
		/*
		 * Meta type 2.
		 */
		return BTREE_KEY_META_TYPE2;
	}

	if (key_meta->keylen < MAX_UINT16_T &&
	    key_meta->datalen < MAX_UINT16_T) {
		/*
		 * Meta type 3.
		 */
		return BTREE_KEY_META_TYPE3;
	}

	return BTREE_KEY_META_TYPE4;
}

/*
 * Converts fixed size key meta to variable key_meta_typeX_t structure.
 *
 * The buffer p must have enough space to hold max sized meta_type.
 */
static key_meta_type_t 
key_meta_to_key_metax(btree_raw_t *bt, btree_raw_node_t *n,
		      key_meta_t *key_meta, void *p, uint32_t *meta_len)
{
	key_meta_type_t meta_type  = get_key_meta_type(bt, key_meta);

	switch (meta_type) {

	case BTREE_KEY_META_TYPE1: {
		key_meta_type1_t *key_meta1 = 
			(key_meta_type1_t *) p;

		key_meta1->seqno = key_meta->seqno;
		*meta_len = sizeof(*key_meta1);
	}
	break;
	case BTREE_KEY_META_TYPE2: {
		key_meta_type2_t *key_meta2 = 
			(key_meta_type2_t *) p;

		dbg_assert(key_meta->keylen < MAX_UINT8_T);
		dbg_assert(key_meta->datalen < MAX_UINT8_T);

		key_meta2->seqno = key_meta->seqno;
		key_meta2->keylen = key_meta->keylen;
		key_meta2->datalen = key_meta->datalen;
		key_meta2->prefix_idx = key_meta->prefix_idx;
		key_meta2->prefix_len = key_meta->prefix_len;
		*meta_len = sizeof(*key_meta2);
		dbg_assert(key_meta->ptr == 0);
	}
	break;
	case BTREE_KEY_META_TYPE3: {
		key_meta_type3_t *key_meta3 = 
			(key_meta_type3_t *) p;

		dbg_assert(key_meta->keylen < MAX_UINT16_T);
		dbg_assert(key_meta->datalen < MAX_UINT16_T);

		key_meta3->seqno = key_meta->seqno;
		key_meta3->keylen = key_meta->keylen;
		key_meta3->datalen = key_meta->datalen;
		key_meta3->prefix_idx = key_meta->prefix_idx;
		key_meta3->prefix_len = key_meta->prefix_len;
	
		key_meta3->ptr = key_meta->ptr;

		*meta_len = sizeof(*key_meta3);
	}
	break;
	case BTREE_KEY_META_TYPE4: {
		key_meta_type4_t *key_meta4 = 
			(key_meta_type4_t *) p;
		key_meta4->seqno = key_meta->seqno;
		key_meta4->keylen = key_meta->keylen;
		key_meta4->datalen = key_meta->datalen;
		key_meta4->ptr = key_meta->ptr;
		key_meta4->prefix_idx = key_meta->prefix_idx;
		key_meta4->prefix_len = key_meta->prefix_len;

		*meta_len = sizeof(*key_meta4);
	}
	break;

	default:
		dbg_assert(0);
	}

	return meta_type;
}

/*
 * Converts from meta_typeX to fixed size
 */
static void
key_metax_to_key_meta(btree_raw_node_t *n, void *key_metax,
		      key_meta_type_t meta_type, key_meta_t *key_meta,
		      int index)
{

	switch (meta_type) {

	case BTREE_KEY_META_TYPE1: {
		key_meta_type1_t *key_meta1 = (key_meta_type1_t *) key_metax;
		key_meta1_to_key_meta(n, key_meta1, key_meta, index);
	}

	break;
	case BTREE_KEY_META_TYPE2: {
		key_meta_type2_t *key_meta2 = (key_meta_type2_t *) key_metax;
		key_meta2_to_key_meta(n, key_meta2, key_meta);
	}
	break;

	case BTREE_KEY_META_TYPE3: {
		key_meta_type3_t *key_meta3 = (key_meta_type3_t *) key_metax;
		key_meta3_to_key_meta(n, key_meta3, key_meta);
	}

	break;
	case BTREE_KEY_META_TYPE4: {
		key_meta_type4_t *key_meta4 = (key_meta_type4_t *) key_metax;
		key_meta4_to_key_meta(n, key_meta4, key_meta);
	}
	break;

	default: dbg_assert(0);
	}

	key_meta->meta_type = meta_type;
}

/*
 * Given a prev key, prev_key_meta and prev_key_index and
 * and a key and key_meta, build the possible prefix idx and length for key.
 */	
static void
build_key_prefix_int(char *prev_key, uint32_t prev_keylen,
		     char *key, uint32_t keylen, int prev_key_index,
		     key_meta_t *prev_key_meta, key_meta_t *key_meta)
{
	/*
	 * Find the common part in keys 
	 */
	key_meta->prefix_len = get_common_part(prev_key, prev_keylen,
					       key, keylen);

	if (key_meta->prefix_len == 0) {
		/*
		 * There is no common part. set prefix to NULL.
		 */
		key_meta->prefix_idx = 0;

	} else if (prev_key_meta->prefix_len > 0) {
		/*
		 * There is common part and last key has a prefix.
		 * Let us make our prefix from last key's prefix.
		 */
		key_meta->prefix_idx = prev_key_meta->prefix_idx;
		if (key_meta->prefix_len > prev_key_meta->prefix_len) {
			/*
			 * We cannot have prefix length > previous keys prefix len.
			 */
			key_meta->prefix_len = prev_key_meta->prefix_len;	
		}
	} else {
		/*
		 * Last key did not have prefix, so we point to it
		 * directly.
		 */
		key_meta->prefix_idx = prev_key_index;
	}

	key_meta->compact_meta = false;

	if (keylen == prev_key_meta->keylen &&
	    key_meta->datalen == prev_key_meta->datalen &&
	    key_meta->prefix_len == prev_key_meta->prefix_len &&
	    key_meta->prefix_idx == prev_key_meta->prefix_idx &&
	    key_meta->ptr == 0 &&
	    prev_key_meta->prefix_len != 0) {
		/*
		 * Same keylen, datalen and prefix idx and len.
		 * This is compact metadata. The type 1
		 *
		 * If there is no prefix idx for prev_key, we choose
		 * to not make it compact to allow prefix saving for this
		 * node.
		 */
		key_meta->compact_meta = true;
	}	

}

/*
 * build prefix index and prefix length for a key to be inserted at 
 * index location.
 *
 * Caller must have set fields keylen and datalen in key_meta.
 *
 */
void
build_key_prefix(btree_raw_t *bt, btree_raw_node_t *n,
		 int index, char *key,
	   	 uint32_t keylen, key_meta_t *key_meta)
{
	key_meta_t prev_key_meta = {0};	
	key_info_t key_info = {0};
	bool res1 = false;

	dbg_assert(key_meta->keylen == keylen);

	key_info.key = tmp_key_buf;

	key_meta->prefix_len = 0;
	key_meta->prefix_idx = 0;
	
	if ((index - 1) < 0) {
		return;
	}

	/*
	 * Get index - 1 th keys meta and key info.
	 */
	btree_leaf_get_meta(n, index - 1, &prev_key_meta);

	res1 = btree_leaf_get_nth_key_info2(bt, n, index - 1, &key_info);
	dbg_assert(res1 == true);
	
	build_key_prefix_int(key_info.key, key_info.keylen, key, keylen, 
			     index - 1, &prev_key_meta, key_meta);
						
}

int32_t
btree_leaf_num_entries(btree_raw_t *bt, btree_raw_node_t *n)
{
	return n->nkeys;
}

int32_t
btree_leaf_used_space(btree_raw_t *bt, btree_raw_node_t *n)
{
	int32_t used_space = 0;
	used_space = n->nkeys * sizeof(entry_header_t);
	
	used_space += bt->nodesize - n->insert_ptr;

	return used_space;
}

static entry_header_t *
btree_leaf_get_nth_entry(btree_raw_node_t *n, int num)
{
	entry_header_t *headers = NULL;

	dbg_assert(num < n->nkeys);
	headers = (entry_header_t *) n->keys;

	return &headers[num];
}

/*
 * Get nth key and all information about it.
 *
 * Caller must free the key_info->key after use.
 */
bool
btree_leaf_get_nth_key_info(btree_raw_t *btree, btree_raw_node_t *n, int index,
		       	    key_info_t *key_info)
{
	entry_header_t *headers = NULL;
	char *data_ptr = NULL;
	key_meta_t key_meta = {0};
	entry_header_t *ent_hdr = NULL;
	char *prefix = NULL;
	char *short_key = NULL;

	headers = (entry_header_t *) ((char *) n->keys);
	data_ptr = (char *) ((uint64_t )n + n->insert_ptr);

	ent_hdr = &headers[index];

	/*
	 * get key metadata
	 */
	btree_leaf_get_meta(n, index, &key_meta);

	/*
	 * get key and its prefix
	 */
	if (key_meta.prefix_len != 0) {
		prefix = get_key_prefix(n, key_meta.prefix_idx);
	}

	short_key = (char *) (data_ptr + ent_hdr->header_offset +
			      get_meta_type_to_size(key_meta.meta_type));

	/*
	 * Alloc memory and fill full key
	 * XX: caller can pass memory to us.
	 */
	key_info->key = get_buffer(btree, key_meta.keylen);
	if (key_info->key == NULL) {
		return false;
	}

	btree_memcpy(key_info->key, prefix, key_meta.prefix_len, false);
	btree_memcpy(key_info->key + key_meta.prefix_len,
		     short_key, 
		     key_meta.keylen - key_meta.prefix_len, false);
	/*
	 * fill other fields
	 */
	key_info->ptr = key_meta.ptr;
	key_info->seqno = key_meta.seqno;

	key_info->keylen = key_meta.keylen;
	key_info->datalen = key_meta.datalen;

	return true;
}

//Can be merged with top one
// this one expect buffer from caller

bool
btree_leaf_get_nth_key_info2(btree_raw_t *btree, btree_raw_node_t *n, int index,
		       	    key_info_t *key_info)
{
	entry_header_t *headers = NULL;
	char *data_ptr = NULL;
	key_meta_t key_meta = {0};
	entry_header_t *ent_hdr = NULL;
	char *prefix = NULL;
	char *short_key = NULL;

	headers = (entry_header_t *) ((char *) n->keys);
	data_ptr = (char *) ((uint64_t )n + n->insert_ptr);

	ent_hdr = &headers[index];

	/*
	 * get key metadata
	 */
	btree_leaf_get_meta(n, index, &key_meta);

	/*
	 * get key and its prefix
	 */
	if (key_meta.prefix_len != 0) {
		prefix = get_key_prefix(n, key_meta.prefix_idx);
	}

	short_key = (char *) (data_ptr + ent_hdr->header_offset +
			      get_meta_type_to_size(key_meta.meta_type));


	btree_memcpy(key_info->key, prefix, key_meta.prefix_len, false);
	btree_memcpy(key_info->key + key_meta.prefix_len, short_key, 
		     key_meta.keylen - key_meta.prefix_len, false);
	/*
	 * fill other fields
	 */
	key_info->ptr = key_meta.ptr;
	key_info->seqno = key_meta.seqno;

	key_info->keylen = key_meta.keylen;
	key_info->datalen = key_meta.datalen;

	return true;
}

bool
btree_leaf_get_data_nth_key(btree_raw_t *bt, btree_raw_node_t *n, int index,
			    char **data, uint64_t *datalen)
{
	entry_header_t *headers = NULL;
	char *data_ptr = NULL;
	key_meta_t key_meta = {0};
	entry_header_t *ent_hdr = NULL;

	assert(index >= 0 && index < n->nkeys);

	headers = (entry_header_t *) n->keys;
	data_ptr = (char *) ((uint64_t )n + n->insert_ptr);

	ent_hdr = &headers[index];

	/*
	 * get key metadata
	 */
	btree_leaf_get_meta(n, index, &key_meta);
	
	*data = data_ptr + ent_hdr->header_offset + 
			get_meta_type_to_size(key_meta.meta_type) + 
			key_meta.keylen - key_meta.prefix_len;	
			
	*datalen = key_meta.datalen;

	return true;
}


/*
 * Caller must free the key out.
 */
bool
btree_leaf_get_nth_key(btree_raw_t *btree, btree_raw_node_t *n, int index,
		       char **key_out, uint32_t *key_out_len)
{
	key_info_t key_info;
	bool res = false;

	res = btree_leaf_get_nth_key_info(btree, n, index, &key_info);	

	*key_out = key_info.key;	
	*key_out_len = key_info.keylen;

	return res;
}


// buffer provided by caller
bool
btree_leaf_get_nth_key2(btree_raw_t *btree, btree_raw_node_t *n, int index,
		        char **key_out, uint32_t *key_out_len)
{
	key_info_t key_info = {0};
	bool res = false;

	/*
	 * buffer to get key in
	 */
	key_info.key = *key_out;

	res = btree_leaf_get_nth_key_info2(btree, n, index, &key_info);	

	*key_out_len = key_info.keylen;
	return res;
}


bool
btree_leaf_find_key2(btree_raw_t *bt, btree_raw_node_t *n, char *key,
		     uint32_t keylen, int32_t *index)
{
	int start = 0;
	int end = 0;
	int center = 0;
	char *tmp_key = NULL;
	uint32_t tmp_key_len = 0;
	bool res = false;
	bool res1 = false;
	int x = 0;

	tmp_key = tmp_key_buf;

	end = n->nkeys - 1;
	*index = n->nkeys;

	while (start <= end) {
		center = (start + end) / 2;
		res1 = btree_leaf_get_nth_key2(bt, n, center, &tmp_key, &tmp_key_len);
		dbg_assert(res1 == true);
		x = (* (bt->cmp_cb)) (bt->cmp_cb_data, key, keylen,
				      tmp_key, tmp_key_len);
		if (x == 0) {
                        *index = center;
                        res = true;
                        break;
                } else if (x < 0) {
                        *index = center;
                        end = center - 1;
                } else {
                        start = center + 1;
                }
	}

	return res;
}

bool
btree_leaf_find_key(btree_raw_t *bt, btree_raw_node_t *n, char *key, uint32_t keylen,
		    btree_metadata_t *meta, uint64_t syndrome, int32_t *index)
{
	return btree_leaf_find_key2(bt, n, key, keylen,index);
}

//#define MAX_META_SIZE 100
#define MAX_META_SIZE (sizeof(key_meta_type4_t) + sizeof(entry_header_t))
#define BTREE_LEAF_ENTRY_MAX_SIZE(keylen, datalen) (keylen + datalen + MAX_META_SIZE)


static inline void
btree_leaf_adjust_offsets(btree_raw_t *bt, btree_raw_node_t *n)
{
	int i = 0;
	entry_header_t *ent_hdr = NULL;
	key_meta_t key_meta = {0};
	int end = n->nkeys - 1;
	uint32_t offset = 0;
	uint32_t meta_size = 0;
	uint64_t meta_offset = (uint64_t) n + n->insert_ptr;

	for (i = 0; i <= end; i++) {
		/*
		 * Get entry header.
		 */
		ent_hdr = btree_leaf_get_nth_entry(n, i);
		ent_hdr->header_offset = offset;
		 
		dbg_assert((meta_offset) < ((uint64_t)n + bt->nodesize));
		/*
		 * Get meta info for key.
		 */
		key_metax_to_key_meta(n, (void *) meta_offset, ent_hdr->meta_type,
				      &key_meta, i);
		dbg_assert(key_meta.keylen != 0);

		meta_size = get_meta_type_to_size(ent_hdr->meta_type);

		offset += meta_size + key_meta.keylen - key_meta.prefix_len;

		if (key_meta.ptr == 0) {
			offset += key_meta.datalen;
		}

		meta_offset = ((uint64_t) n + n->insert_ptr + offset); 
		dbg_assert(meta_offset <= ((uint64_t)n + bt->nodesize));

	}
}

/*
 * Append a key entry in temp mem buffer.
 * Returns number of bytes appended.
 */
static uint32_t
append_entry_to_tmp_buf(btree_raw_t *bt, btree_raw_node_t *n, key_meta_t *key_meta,
			key_info_t *key_info, char *data, uint64_t datalen,
			char *mem_buf, entry_header_t *tmp_hdr, bool dry_run)
{
	char meta_buf[MAX_META_SIZE];
	uint32_t metalen = 0;
	key_meta_type_t meta_type;
	char *buf_ptr = mem_buf;
	
	/*
	 * Build meta for this entry
	 */
	meta_type = key_meta_to_key_metax(bt, n, key_meta, meta_buf, &metalen);
	
	if (!dry_run) {
		tmp_hdr->meta_type = meta_type;
	}
	 
	btree_memcpy(buf_ptr, meta_buf, metalen, dry_run);
	buf_ptr += metalen;

	btree_memcpy(buf_ptr, (char *)(key_info->key + key_meta->prefix_len), 
	key_meta->keylen - key_meta->prefix_len, dry_run);

	buf_ptr += (key_meta->keylen - key_meta->prefix_len);

	if (key_meta->ptr == 0) {
		dbg_assert(datalen < bt->big_object_size);
		btree_memcpy(buf_ptr, data, datalen, dry_run);
		buf_ptr += datalen;
	 }
	
	 dbg_assert((buf_ptr - mem_buf) > 0); 
	 return (buf_ptr - mem_buf);
}


/*
 * Given a node with start index, copy the keys to temp buf in sorted order
 * with new prefix and meta according to the prev key.
 * Break if we find first unimpacted key.
 * Returns number of keys copied.
 * The headers is the pointer to temp header entries for the entries to be copied to
 * temp mem area. the meta_type field is set after evey insertion.
 */ 
static int
copy_impacted_keys_to_tmp_buf(btree_raw_t *bt, btree_raw_node_t *n, int from,
			      int impacted_index, int prev_key_index, char *mem_area,
			      entry_header_t *headers, char *prev_key,
			      uint32_t prev_keylen, key_meta_t *prev_key_meta,
			      uint32_t *total_bytes_copied, int32_t *bytes_increased, bool dry_run)
{
	int end = n->nkeys - 1;
	int copy_idx = 0;
	int buf_offset = 0;
	char *tmp_data = NULL;
	uint64_t tmp_datalen = 0;
	key_info_t key_info = {0};
	key_meta_t old_key_meta = {0};
	key_meta_t new_key_meta = {0};

	*total_bytes_copied = 0;

	if (prev_key == NULL && (from + copy_idx <= end)) {
		dbg_assert(prev_keylen == 0);

		btree_leaf_get_nth_key_info(bt, n, from + copy_idx, &key_info);
		btree_leaf_get_meta(n, from + copy_idx, prev_key_meta);

		btree_leaf_get_data_nth_key(bt, n, from + copy_idx, &tmp_data, &tmp_datalen);

		/*
		 * This is first key, this cannot have any prefix.
		 */
		prev_key_meta->prefix_len = 0;
		prev_key_meta->prefix_idx = 0;

		prev_key_meta->keylen = key_info.keylen;
		prev_key_meta->datalen = key_info.datalen;

		prev_key_meta->seqno = key_info.seqno;
		prev_key_meta->ptr = key_info.ptr;	

		prev_key_meta->compact_meta = false; /// In trial XXX

		buf_offset += append_entry_to_tmp_buf(bt, n, prev_key_meta, 
						      &key_info, tmp_data, tmp_datalen,
						      (mem_area + buf_offset), &headers[copy_idx], dry_run);

		prev_key = key_info.key;
		prev_keylen = key_info.keylen;

		prev_key_index = 0;

		copy_idx++;
	}

	while ((from + copy_idx) <= end) {
		/*
		 * Get current key_info for the key
		 */
		btree_leaf_get_nth_key_info(bt, n, from + copy_idx, &key_info);

		/*
		 * Get current key_meta for the the key.
		 */
		btree_leaf_get_meta(n, from + copy_idx, &old_key_meta);

		/*
		 * Build the new prefix and meta type for the key.
		 */
		new_key_meta.keylen = key_info.keylen;
		new_key_meta.datalen = key_info.datalen;
		new_key_meta.ptr = key_info.ptr;
		new_key_meta.seqno = key_info.seqno;


		build_key_prefix_int(prev_key, prev_keylen, key_info.key,
				key_info.keylen, prev_key_index, /// check this 
				prev_key_meta, &new_key_meta);

		dbg_assert(new_key_meta.prefix_idx < n->nkeys);

		if (old_key_meta.meta_type != BTREE_KEY_META_TYPE1 &&
		    ((new_key_meta.prefix_len == 0 &&
		      old_key_meta.prefix_len == 0) ||
		     (old_key_meta.prefix_len != 0 &&
		      old_key_meta.prefix_idx < impacted_index))) { /// Check this again ??

			/*
			 * Either this entry points to index before the impacted
			 * index or this does not have prefix even after
			 * insertion of new key. So this is unimpacted key
			 * All keys after this are also unimpacted.
			 *
			 * If key has META_TYPE1, then also it is impacted
			 * since previous keys meta might have changes.
			 */
			
			free_buffer(bt, key_info.key);
			break;
		}


		if (copy_idx != 0) {
			free_buffer(bt, prev_key);
		}

		btree_leaf_get_data_nth_key(bt, n, from + copy_idx, &tmp_data, &tmp_datalen);
		buf_offset += append_entry_to_tmp_buf(bt, n, &new_key_meta, 
						&key_info, tmp_data, tmp_datalen,
						(mem_area + buf_offset), &headers[copy_idx], dry_run);
		/*
		 * This entry is now previous for next.
		 */
		prev_key = key_info.key;
		prev_keylen = key_info.keylen;

		*prev_key_meta = new_key_meta;

		copy_idx++;
		prev_key_index++;
	}

	if (copy_idx != 0) {
		free_buffer(bt, prev_key);
	}

	if (*bytes_increased < 0) {
		*bytes_increased = 0;
	}

	*total_bytes_copied = buf_offset;
	return copy_idx;
}

bool
btree_leaf_insert_key_index(btree_raw_t *bt, btree_raw_node_t *n, char *key, uint32_t keylen,
		      	    char *data, uint64_t datalen, key_info_t *key_info, 
			    btree_metadata_t *meta, int index, int32_t *bytes_increased, bool dry_run)
{
	key_meta_t key_meta = {0};
	entry_header_t *ent_hdr = NULL;
	entry_header_t *tmp_hdr = NULL;
	char meta_buf[MAX_META_SIZE];
	uint32_t metalen = 0;
	uint64_t src_offset = 0;
	uint64_t dst_offset = 0;
	uint32_t length_to_copy = 0;
	char *tmp_node_mem = NULL;
	int items_copied = 0;
	int dst_index = 0;
	int dst_nkeys = 0;
	key_meta_type_t meta_type;
	int32_t bytes_adjusted = 0;
	int32_t old_used_space = 0;
	int32_t new_used_space = 0;

	key_meta.keylen = keylen;
	key_meta.datalen = datalen;
	key_meta.seqno = key_info->seqno;
	key_meta.ptr = key_info->ptr;

	dbg_assert(key_info->keylen == keylen);

#ifdef COLLECT_TIME_STATS 
	uint64_t start_time = 0;
	start_time = get_tod_usecs();

	if (!dry_run) {
		__sync_add_and_fetch(&bt_leaf_insert_kbi_count, 1);
	}
	
#endif 
	*bytes_increased = 0;

#ifdef DEBUG_BUILD
	if (!dry_run) {
		tmp_node_mem = (char *) get_buffer(bt, bt->nodesize_less_hdr); 
		if (tmp_node_mem == NULL) {
			return false;
		}
	}
#else
	tmp_node_mem = tmp_leaf_node;
#endif 
	old_used_space = btree_leaf_used_space(bt, n);

	/*
	 * Build the prefix.
	 */
	build_key_prefix(bt, n, index, key, keylen, &key_meta);

	/*
	 * Build the meta type for new key.
	 */
	meta_type = key_meta_to_key_metax(bt, n, &key_meta, meta_buf, &metalen);
	key_meta.meta_type = meta_type;
	
	/*
	 * Copy the headers before index.
	 */
	src_offset = (uint64_t) n->keys;
	dst_offset = (uint64_t) tmp_node_mem;
	length_to_copy = (index * sizeof(entry_header_t));
	btree_memcpy((char *) dst_offset, (char *) src_offset, length_to_copy, dry_run);

	/*
	 * Create new index for new entry.
	 */
	if (!dry_run) {
		ent_hdr = (entry_header_t *) 
			((uint64_t) tmp_node_mem + (index * sizeof(entry_header_t))); 
		ent_hdr->meta_type = meta_type;
		ent_hdr->header_offset = 0; // Will set it later.
	}

	/*
	 * Copy rest of the headers.
	 */
	src_offset = (uint64_t) n->keys + (index * sizeof(entry_header_t));
	dst_offset = (uint64_t) tmp_node_mem + ((index + 1) * sizeof(entry_header_t));
	length_to_copy = (n->nkeys - index) * sizeof(entry_header_t);

	btree_memcpy((char *) dst_offset, (char *) src_offset, length_to_copy, dry_run);
	
	/*
	 * Copy the keys before the index to temp node.
	 */	
	dst_offset = (uint64_t) tmp_node_mem + (n->nkeys + 1) // We added a key 
				    * sizeof(entry_header_t);

	src_offset = (uint64_t) n + n->insert_ptr;

	if (index == 0) {
		length_to_copy = 0;
	} else {
		key_meta_t tmp_key_meta;

		ent_hdr = (entry_header_t *) 
			((uint64_t) n->keys + ((index - 1) * sizeof(entry_header_t)));

		btree_leaf_get_meta(n, index - 1, &tmp_key_meta);
		length_to_copy = ent_hdr->header_offset +
				 get_key_meta_space_used(&tmp_key_meta); 
				  
	}

	btree_memcpy((char *) dst_offset, (char *) src_offset, length_to_copy, dry_run);

	dst_offset += length_to_copy;
	items_copied = index;

	/*
	 * Append the new entry to tmp_mem_node.
	 */
	tmp_hdr = (entry_header_t *) 
		((uint64_t) tmp_node_mem + (index * sizeof(entry_header_t)));

	length_to_copy = append_entry_to_tmp_buf(bt, n, &key_meta, key_info,
						 data, datalen, (char *) dst_offset, tmp_hdr, dry_run);
	dst_offset += length_to_copy;
	items_copied += 1;

	/*
	 * Copy the impacted entries one by one.
	 */
	tmp_hdr = (entry_header_t *) 
		((uint64_t) tmp_node_mem + ((index + 1) * sizeof(entry_header_t)));

	length_to_copy = 0;
	items_copied += copy_impacted_keys_to_tmp_buf(bt, n, index, index, index, (char *) dst_offset, tmp_hdr,
				      key, keylen, &key_meta, &length_to_copy, &bytes_adjusted, dry_run);
	dst_offset += length_to_copy;

	if (items_copied >= (n->nkeys + 1)) {
		/*
		 * No more objects left.
		 */
		goto done;
	}

	/*
	 * Found a key that is not impacted, copy rest in bulk.
	 */
	ent_hdr = btree_leaf_get_nth_entry(n, items_copied - 1);
	src_offset = (uint64_t) n + n->insert_ptr + ent_hdr->header_offset;

	length_to_copy = bt->nodesize - (n->insert_ptr + ent_hdr->header_offset);
	btree_memcpy((char *) dst_offset, (char *) src_offset, length_to_copy, dry_run);

	dst_offset += length_to_copy;

done:
	/*
	 * Next index till what we have already finished copying.
	 */
	dst_index = items_copied;
	dst_nkeys = n->nkeys + 1;	

	new_used_space = (n->nkeys + 1) * sizeof(entry_header_t) +
			 dst_offset - ((uint64_t) tmp_node_mem +
				       (n->nkeys + 1) * sizeof(entry_header_t));

	dbg_assert(new_used_space > old_used_space);
	*bytes_increased = new_used_space - old_used_space;

	if (dry_run) {
		goto exit;
	}
	
	/*
	 * Actual change to node happens after this.
	 */
	n->nkeys++;

	/*
	 * Copy the headers back to start of the node.
	 */
#ifdef COLLECT_TIME_STATS 
	uint64_t start_time1 = 0;
	start_time1 = get_tod_usecs();

	btree_memcpy(n->keys, tmp_node_mem, n->nkeys * sizeof(entry_header_t), dry_run);

	__sync_add_and_fetch(&bt_leaf_memcpy_time, get_tod_usecs() - start_time1);
#else
	btree_memcpy(n->keys, tmp_node_mem, n->nkeys * sizeof(entry_header_t), dry_run);
#endif

	/*
	 * Copy the rest of data to end of the node
	 */
	length_to_copy = dst_offset - ((uint64_t) tmp_node_mem +
				       n->nkeys * sizeof(entry_header_t));

	dbg_assert(((uint64_t) n + (bt->nodesize - length_to_copy)) >=
		 ((uint64_t) n->keys + n->nkeys * sizeof(entry_header_t)));

#ifdef COLLECT_TIME_STATS 
	start_time1 = get_tod_usecs();
	btree_memcpy((char *) ((uint64_t) n + bt->nodesize - length_to_copy), 
	       (char *) (tmp_node_mem + (n->nkeys * sizeof(entry_header_t))),
	       length_to_copy, dry_run);

	__sync_add_and_fetch(&bt_leaf_memcpy_time, get_tod_usecs() - start_time1);
#else
	btree_memcpy((char *) ((uint64_t) n + bt->nodesize - length_to_copy), 
	       (char *) (tmp_node_mem + (n->nkeys * sizeof(entry_header_t))),
	       length_to_copy, dry_run);

#endif
	     
	n->insert_ptr = bt->nodesize - length_to_copy;

	dbg_assert(n->insert_ptr + length_to_copy == bt->nodesize);
	dbg_assert(((uint64_t) n + n->insert_ptr) >=
		   ((uint64_t) n->keys + n->nkeys * sizeof(entry_header_t)));

	/*
	 * Update the offset of entries.
	 */
	btree_leaf_adjust_offsets(bt, n);

	/*
	 * Increment the prefix_idx of keys copied in bulk.
	 */
	btree_leaf_adjust_prefix_idxes(n, dst_index, dst_nkeys - 1, index - 1, 1);

exit:
#ifdef DEBUG_BUILD
	if (tmp_node_mem) {
		free_buffer(bt, tmp_node_mem);
	}
#endif 


#ifdef COLLECT_TIME_STATS 
	if (!dry_run) {
		__sync_add_and_fetch(&bt_leaf_insert_kbi_total_time, get_tod_usecs() - start_time);
	}
#endif 
	return true;
}

static bool
btree_leaf_remove_key_index_int(btree_raw_t *bt, btree_raw_node_t *n,
			    int index, key_info_t *key_info, int32_t *bytes_decreased, bool dry_run)
{
	entry_header_t *ent_hdr = NULL;
	entry_header_t *tmp_hdr = NULL;
	char meta_buf[MAX_META_SIZE];
	uint32_t metalen = 0;
	uint64_t src_offset = 0;
	uint64_t dst_offset = 0;
	uint32_t length_to_copy = 0;
	char *tmp_node_mem = NULL;
	int items_copied = 0;
	int dst_index = 0;
	int dst_nkeys = 0;
	key_meta_type_t meta_type;
	key_info_t prev_key_info = {0};
	key_meta_t prev_key_meta = {0};
	key_meta_t tmp_key_meta = {0};
	int32_t bytes_adjusted = 0;
	int32_t old_used_space = 0;
	int32_t new_used_space = 0;

	*bytes_decreased = 0;

#ifdef DEBUG_BUILD
	if (!dry_run) {
		tmp_node_mem = (char *) get_buffer(bt, bt->nodesize_less_hdr);
		if (tmp_node_mem == NULL) {
			return false;
		}
	}
#else
	tmp_node_mem = tmp_leaf_node;
#endif 

	old_used_space = btree_leaf_used_space(bt, n);

	/*
	 * Copy the headers before index.
	 */
	src_offset = (uint64_t) n->keys;
	dst_offset = (uint64_t) tmp_node_mem;
	length_to_copy = (index * sizeof(entry_header_t));
	btree_memcpy((char *) dst_offset, (char *) src_offset, length_to_copy, dry_run);

	/*
	 * Copy rest of the headers except at index.
	 */
	src_offset = (uint64_t) n->keys + ((index + 1) * sizeof(entry_header_t));
	dst_offset = (uint64_t) tmp_node_mem + (index * sizeof(entry_header_t));
	length_to_copy = (n->nkeys - index - 1) * sizeof(entry_header_t);

	btree_memcpy((char *) dst_offset, (char *) src_offset, length_to_copy, dry_run);
	
	/*
	 * Copy the keys before the index to temp node.
	 */	
	dst_offset = (uint64_t) tmp_node_mem + (n->nkeys - 1) // We removed a key 
				    * sizeof(entry_header_t);

	src_offset = (uint64_t) n + n->insert_ptr;

	if (index == 0) {
		length_to_copy = 0;
	} else {
		key_meta_t tmp_key_meta;

		ent_hdr = (entry_header_t *) 
			((uint64_t) n->keys + ((index - 1) * sizeof(entry_header_t)));

		btree_leaf_get_meta(n, index - 1, &tmp_key_meta);
		length_to_copy = ent_hdr->header_offset +
				 get_key_meta_space_used(&tmp_key_meta); 
				  
	}

	btree_memcpy((char *) dst_offset, (char *) src_offset, length_to_copy, dry_run);

	dst_offset += length_to_copy;
	items_copied = index;

	/*
	 * Account for space ocupied by the deleted entry.
	 */
	btree_leaf_get_meta(n, index, &tmp_key_meta);

	/*
	 * Get the key meta and key_info for index key and see if any of 
	 * keys are impacted due to this.
	 */
	if (index == 0) {
		prev_key_info.keylen = 0;
		prev_key_info.key = NULL;
	} else {
		btree_leaf_get_nth_key_info(bt, n, index - 1, &prev_key_info);
		btree_leaf_get_meta(n, index - 1, &prev_key_meta);
	}

	/*
	 * Copy the impacted entries one by one.
	 */
	tmp_hdr = (entry_header_t *) 
		((uint64_t) tmp_node_mem + ((index) * sizeof(entry_header_t)));

	items_copied += copy_impacted_keys_to_tmp_buf(bt, n, index + 1, index, index - 1,
						     (char *) dst_offset, tmp_hdr, prev_key_info.key, prev_key_info.keylen,
						     &prev_key_meta, &length_to_copy, &bytes_adjusted, dry_run);
	
	dst_offset += length_to_copy;

	if (prev_key_info.key) {
		free_buffer(bt, prev_key_info.key);
	}

	if (items_copied >= (n->nkeys - 1)) {
		/*
		 * No more objects left.
		 */
		goto done;
	}

	/*
	 * Found a key that is not impacted, copy rest in bulk.
	 */
	ent_hdr = btree_leaf_get_nth_entry(n, items_copied + 1);
	src_offset = (uint64_t) n + n->insert_ptr + ent_hdr->header_offset;

	length_to_copy = bt->nodesize - (n->insert_ptr + ent_hdr->header_offset);
	btree_memcpy((char *) dst_offset, (char *) src_offset, length_to_copy, dry_run);

	dst_offset += length_to_copy;

done:

	/*
	 * Next index till what we have already finished copying.
	 */
	dst_index = items_copied;
	dst_nkeys = n->nkeys - 1;	

	new_used_space =  (n->nkeys - 1) * sizeof(entry_header_t) +
			  dst_offset - ((uint64_t) tmp_node_mem +
				       (n->nkeys - 1) * sizeof(entry_header_t));
	*bytes_decreased = old_used_space - new_used_space;
	if (dry_run) {
		goto exit;
	}
		
	n->nkeys--;
	
	/*
	 * Copy the headers back to start of the node.
	 */
	btree_memcpy(n->keys, tmp_node_mem, n->nkeys * sizeof(entry_header_t), dry_run);

	/*
	 * Copy the rest of data to end of the node
	 */
	length_to_copy = dst_offset - ((uint64_t) tmp_node_mem +
				       n->nkeys * sizeof(entry_header_t));
						
	dbg_assert(((uint64_t) n + (bt->nodesize - length_to_copy)) >=
		 ((uint64_t) n->keys + n->nkeys * sizeof(entry_header_t)));

	btree_memcpy((char *) ((uint64_t) n + bt->nodesize - length_to_copy), 
	       (char *) (tmp_node_mem + (n->nkeys * sizeof(entry_header_t))),
	       length_to_copy, dry_run);

	n->insert_ptr = bt->nodesize - length_to_copy;

	dbg_assert(((uint64_t) n + n->insert_ptr) > 
		   ((uint64_t) n->keys + n->nkeys * sizeof(entry_header_t)));
	dbg_assert(n->insert_ptr + length_to_copy == bt->nodesize);
	     
	/*
	 * Update the offset of entries.
	 */
	btree_leaf_adjust_offsets(bt, n);

	/*
	 * Decrement the prefix_idx of keys copied in bulk.
	 */
	btree_leaf_adjust_prefix_idxes(n, dst_index, dst_nkeys - 1, index, -1);

exit:

#ifdef DEBUG_BUILD
	if (tmp_node_mem) {
		free_buffer(bt, tmp_node_mem);
	}
#endif 

	return true;
}

bool
btree_leaf_remove_key_index(btree_raw_t *bt, btree_raw_node_t *n,
			    int index, key_info_t *key_info, int32_t *bytes_decreased)
{
	*bytes_decreased = 0;
	return btree_leaf_remove_key_index_int(bt, n, index, key_info, bytes_decreased, false);
}

bool
btree_leaf_update_key(btree_raw_t *bt, btree_raw_node_t *n, char *key, uint32_t keylen,
	      char *data, uint64_t datalen, uint64_t seqno, uint64_t ptr,
	      btree_metadata_t *meta, uint64_t syndrome, int index, bool key_exists,
	      int32_t *bytes_saved, int32_t *size_increased)
{
	key_info_t key_info = {0};
	bool res = false;
	int32_t bytes_increased = 0;
	int32_t bytes_decreased = 0;
	int32_t size_expected = 0;
#ifdef DEBUG_BUILD
	bool res1 = false;
	int index1 = -1;
#endif

	dbg_assert(index >= 0);
	key_info.ptr = ptr;
	key_info.seqno = seqno;
	key_info.syndrome = syndrome;
	key_info.keylen = keylen;
	key_info.datalen = datalen;
	key_info.key = key;

	if (key_exists) {
		res = btree_leaf_remove_key_index_int(bt, n, index, 
						      &key_info, &bytes_decreased, false);	
		dbg_assert(res == true);
	}

	res = btree_leaf_insert_key_index(bt, n, key, keylen, data, datalen,
					  &key_info, meta, index, &bytes_increased, false);
#ifdef DEBUG_BUILD
	res1 = btree_leaf_find_key2(bt, n, key, keylen, &index1);
	if (res1 == false || index != index1) {
		(void) btree_leaf_find_key2(bt, n, key, keylen, &index1);
		btree_leaf_print(bt, n);
	}
	dbg_assert(res1 == true && index1 == index);
	dbg_assert(res1 == true);

#endif
	*bytes_saved = 0;
	size_expected = BTREE_LEAF_ENTRY_MAX_SIZE(keylen, datalen);
	*size_increased = bytes_increased - bytes_decreased;

	*bytes_saved = size_expected - *size_increased;

	return res;
}

#define MAX_KEY_LEN 1024 
void 
btree_print_key(char * string, uint32_t keylen)
{
	char tmp_key[MAX_KEY_LEN];

	dbg_assert(keylen < MAX_KEY_LEN);
	btree_memcpy(tmp_key, string, keylen, false);

	tmp_key[keylen] = 0;

	printf("[%5d] >>> %s <<<.\n ", keylen, tmp_key);
}

void 
btree_leaf_print(btree_raw_t *bt, btree_raw_node_t *n)
{
	int i = 0;
	int end = n->nkeys - 1;
	key_info_t key_info = {0};
	key_meta_t key_meta = {0};
	entry_header_t *ent_hdr = NULL;

	printf(" =====  Node ====== \n");
	printf(" Logical ID = %"PRIu64".\n", n->logical_id);	
	printf(" Flags = %d.\n", n->flags);
	printf(" Nkeys = %d. \n", n->nkeys);

	for (i = 0; i <= end; i++) {
		btree_leaf_get_nth_key_info(bt, n, i, &key_info);
		btree_leaf_get_meta(n, i, &key_meta);
		ent_hdr = btree_leaf_get_nth_entry(n, i);
		
		printf("Key index = %d, meta_type = %d, header offset = %d.\n", i, ent_hdr->meta_type, ent_hdr->header_offset);
		printf("	>>  meta_type = %d, prefix_index = %d, prefix_len = %d, keylen = %d,  datalen = %"PRIu64".\n", 
			key_meta.meta_type,  key_meta.prefix_idx, key_meta.prefix_len, key_meta.keylen, key_meta.datalen);

			
		btree_print_key(key_info.key, key_info.keylen);
		free_buffer(bt, key_info.key);
	}
}

// Caller must free the key_out
bool
btree_leaf_find_right_key(btree_raw_t *bt, btree_raw_node_t *n,
			  char *key, uint32_t keylen,
			  char **key_out, uint32_t *keyout_len,
			  int *index, bool inclusive)
{
	bool res = false; 
	int32_t num_entries = n->nkeys;

	res = btree_leaf_find_key2(bt, n, key, keylen, index);

        if ((res == true) && !inclusive) {
                (*index)++;
        }

	if (*index >= 0 && *index < num_entries) {
		btree_leaf_get_nth_key(bt, n, *index, key_out, keyout_len);
		res = true;
        } else {
                res = false;
        }

        return res;
}

static int 
btree_leaf_copy_keys(btree_raw_t *bt, btree_raw_node_t *from_node, int from_index,
		     btree_raw_node_t *to_node, int to_index, int num_keys, uint32_t size_limit)
{
	int i = 0;
	key_info_t key_info = {0};
	char *tmp_data = NULL;
	uint64_t tmp_datalen = 0;
	btree_metadata_t *meta = NULL; //unsed right now
	int32_t bytes_increased = 0; // unused
	int32_t used_space = 0;
	bool res = false;

	key_info.key = tmp_key_buf;

	for (i = 0; i < num_keys; i++) {
		res = btree_leaf_get_nth_key_info(bt, from_node, from_index + i, &key_info);
		dbg_assert(res == true);

		res = btree_leaf_get_data_nth_key(bt, from_node, from_index + i,
						 &tmp_data, &tmp_datalen);
		dbg_assert(res == true);
		res = btree_leaf_insert_key_index(bt, to_node, key_info.key, key_info.keylen,
					    tmp_data, tmp_datalen, &key_info, meta, to_index + i,
					    &bytes_increased, false);
		dbg_assert(res == true);
		free_buffer(bt, key_info.key);

		used_space = btree_leaf_used_space(bt, to_node);
		if (size_limit && used_space >= size_limit) {
			i++;
			break;
		}
	}
	
	return i;
}

bool
btree_leaf_split(btree_raw_t *bt, btree_raw_node_t *from_node,
		 btree_raw_node_t *to_node, char **key_out,
		 uint32_t *key_out_len, uint64_t *split_syndrome,
		 int32_t *bytes_increased)
{
	btree_raw_node_t *tmp_node_from = NULL;
	btree_raw_node_t *tmp_node_to = NULL;
	int i = 0;
	int j = 0;
	int middle_index = 0;
	int end_index = 0;
	key_info_t key_info = {0};
	char *data = NULL;
	uint64_t datalen = 0;
	bool res = false;
	btree_metadata_t *meta = NULL; //unsed right now
	int32_t bytes_increased1 = 0;
	char * src_offset = NULL;
	char * dst_offset = NULL;
	uint32_t length_to_copy = 0;
	entry_header_t *ent_hdr = NULL;
	entry_header_t *tmp_hdr = NULL;
	key_meta_t prev_key_meta = {0};
	int32_t used_space_old = 0;
	int32_t used_space_new = 0;

	*bytes_increased = 0;
	
#ifdef COLLECT_TIME_STATS
	uint64_t start_time = 0;
	__sync_add_and_fetch(&bt_leaf_split_count, 1);
	start_time = get_tod_usecs();
#endif 

	used_space_old = btree_leaf_used_space(bt, from_node);

	tmp_node_from = (btree_raw_node_t *) get_buffer(bt, bt->nodesize);
	if (tmp_node_from == NULL) {
		dbg_assert(0);
		return false;
	}

	btree_leaf_init(bt, tmp_node_from);

	tmp_node_to = (btree_raw_node_t *) get_buffer(bt, bt->nodesize);
	if (tmp_node_to == NULL) {
		dbg_assert(0);
		return false;
	}

	btree_leaf_init(bt, tmp_node_to);

	middle_index = from_node->nkeys / 2;
	end_index = from_node->nkeys - 1;

	src_offset = (char *) from_node->keys;
	dst_offset = (char *) tmp_node_from->keys;
	length_to_copy = (middle_index + 1) * sizeof(entry_header_t);
	btree_memcpy(dst_offset, src_offset, length_to_copy, false);

	ent_hdr = btree_leaf_get_nth_entry(from_node, middle_index + 1);

	src_offset = (char *) ((uint64_t) from_node + from_node->insert_ptr);
	length_to_copy = ent_hdr->header_offset;
	dst_offset = (char *) ((uint64_t) tmp_node_from + bt->nodesize - length_to_copy);
	btree_memcpy(dst_offset, src_offset, length_to_copy, false);

	tmp_node_from->nkeys = middle_index + 1;
	tmp_node_from->insert_ptr = bt->nodesize - length_to_copy;

	i = middle_index + 1;

	/*
	 * Copy headers first to to_node_temp 
	 */
	src_offset = (char  *) ((uint64_t) from_node->keys + 
				 (middle_index + 1) * sizeof(entry_header_t));
	dst_offset = (char *) (uint64_t) tmp_node_to->keys;
	length_to_copy = (from_node->nkeys - (middle_index + 1)) *
			 sizeof(entry_header_t);

	btree_memcpy(dst_offset, src_offset, length_to_copy, false);
	
	j = 0;
	tmp_hdr = (entry_header_t *)
		   ((uint64_t) tmp_node_to->keys + (0 * sizeof(entry_header_t)));

	dst_offset = (char *)
		   	((uint64_t) tmp_node_to->keys + ((from_node->nkeys - (middle_index + 1)) *
								sizeof(entry_header_t)));

	j = copy_impacted_keys_to_tmp_buf(bt, from_node, i, 0 /* all keys are impacted */,
					  0, (char *) dst_offset, tmp_hdr, NULL, 0,
					  &prev_key_meta, &length_to_copy, &bytes_increased1, false);

	dst_offset += length_to_copy;

	/*
	 * Found a key that is not impacted, copy rest in bulk.
	 */
	if ((j + middle_index + 1) > (from_node->nkeys - 1)) {
		/*
		 * No more keys.
		 */
		goto done;
	}

	ent_hdr = btree_leaf_get_nth_entry(from_node, j + middle_index + 1); // ??
	src_offset = (char *)
			((uint64_t) from_node + from_node->insert_ptr + ent_hdr->header_offset);

	length_to_copy = bt->nodesize - (from_node->insert_ptr + ent_hdr->header_offset);

	btree_memcpy((char *) dst_offset, (char *) src_offset, length_to_copy, false);

	dst_offset += length_to_copy;

done:
	j = from_node->nkeys - (middle_index + 1); // All keys copied

#ifdef COLLECT_TIME_STATS
	{
		// Copy back header to to_node
		uint64_t start_time1 = 0;
		start_time1 = get_tod_usecs();

		btree_memcpy(to_node->keys, tmp_node_to->keys, j * sizeof(entry_header_t), false);


		// Copy back actual key  + data to to_node
		length_to_copy = (uint64_t) dst_offset - ((uint64_t) tmp_node_to->keys +
					       j * sizeof(entry_header_t));

		btree_memcpy((char *) ((uint64_t) to_node + bt->nodesize - length_to_copy), 
		       (char *) ((uint64_t) tmp_node_to->keys + (j * sizeof(entry_header_t))),
		       length_to_copy, false);
		__sync_add_and_fetch(&bt_leaf_split_memcpy_time, get_tod_usecs() - start_time1);
	}
#else
	// Copy back header to to_node
	btree_memcpy(to_node->keys, tmp_node_to->keys, j * sizeof(entry_header_t), false);

	// Copy back actual key  + data to to_node
	length_to_copy = (uint64_t) dst_offset - ((uint64_t) tmp_node_to->keys +
				       j * sizeof(entry_header_t));
	btree_memcpy((char *) ((uint64_t) to_node + bt->nodesize - length_to_copy), 
	       (char *) ((uint64_t) tmp_node_to->keys + (j * sizeof(entry_header_t))),
	       length_to_copy, false);
#endif


	to_node->insert_ptr = bt->nodesize - length_to_copy;
	to_node->nkeys = j;

	/*
	 * Update the offset of entries.
	 */
	btree_leaf_adjust_offsets(bt, to_node);


	/* 
	 * Adjust prefixes of the to_node
	 */
	btree_leaf_adjust_prefix_idxes(to_node, 0,
				       to_node->nkeys - 1, 0,  -(middle_index + 1));

	/*
	 * fill key out.
	 */
	res = btree_leaf_get_nth_key(bt, from_node, middle_index, key_out, key_out_len);

	/*
	 * Copy the tmp node back to from_node, except the header fields.
	 */
#ifdef COLLECT_TIME_STATS
	{
		uint64_t start_time1 = 0;
		start_time1 = get_tod_usecs();
		btree_memcpy((char *) from_node->keys, (char *) tmp_node_from->keys,
			     bt->nodesize_less_hdr, false);
		__sync_add_and_fetch(&bt_leaf_split_memcpy_time, get_tod_usecs() - start_time1);
	}
#else
	btree_memcpy((char *) from_node->keys, (char *) tmp_node_from->keys,
		      bt->nodesize_less_hdr, false);
#endif

	from_node->nkeys = tmp_node_from->nkeys;
	from_node->insert_ptr = tmp_node_from->insert_ptr;

	dbg_assert(tmp_node_from->nkeys == middle_index + 1);

	used_space_new = btree_leaf_used_space(bt, from_node) +
			 btree_leaf_used_space(bt, to_node);
	*bytes_increased = used_space_new - used_space_old;

#ifdef COLLECT_TIME_STATS
	__sync_add_and_fetch(&bt_leaf_split_time, get_tod_usecs() - start_time);
#endif

	free_buffer(bt, tmp_node_from);
	free_buffer(bt, tmp_node_to);

	return true;
}

bool
btree_leaf_is_full_index(btree_raw_t *bt, btree_raw_node_t *n, char *key, uint32_t keylen,
			 uint64_t datalen, btree_metadata_t *meta, uint64_t syndrome,
			 bool key_exists, int index)
{
	key_info_t key_info = {0};
	int32_t bytes_needed = 0;
	int32_t max_bytes_needed = 0;
	int32_t bytes_free = 0;
	uint64_t old_data_len = 0;
	int index1 = -1;
	bool res = false;
	int32_t used_space = 0;

	used_space = btree_leaf_used_space(bt, n);
	bytes_free = bt->nodesize_less_hdr - used_space;

#if 1
	if (index >= n->nkeys) {
		max_bytes_needed = BTREE_LEAF_ENTRY_MAX_SIZE(keylen, datalen);
		if (max_bytes_needed < bytes_free) {
			return false;
		}
	}
#endif 

#ifdef COLLECT_TIME_STATS
	uint64_t start_time = 0;
	start_time = get_tod_usecs();
#endif


	if (key_exists) {
		int32_t bytes_increased = 0;
		int32_t bytes_decreased = 0;

#ifdef DEBUG_BUILD
		res = btree_leaf_find_key2(bt, n ,key, keylen, &index1);
		dbg_assert(res == true);
		dbg_assert(index1 == index);
#endif

		res = btree_leaf_remove_key_index_int(bt, n, index, &key_info,
						      &bytes_decreased, true);
		dbg_assert(res == true);

		key_info.key = key;
		key_info.keylen = keylen;
		key_info.datalen = datalen;
		if (datalen >= bt->big_object_size) {
			key_info.ptr=0xdeadc0de; //pure hack
		}

		res = btree_leaf_insert_key_index(bt, n, key, keylen, global_tmp_data,
						  datalen, &key_info, meta, index, &bytes_increased, true);
		dbg_assert(res == true);
		bytes_needed = bytes_increased - bytes_decreased;
		if (bytes_needed < 0) {
			bytes_needed = 0;
		}
	
	} else {

#ifdef DEBUG_BUILD
		(void) btree_leaf_find_key2(bt, n, key, keylen, &index1);	
		dbg_assert(index == index1);
#endif

		key_info.key = key;
		key_info.keylen = keylen;
		key_info.datalen = datalen;
		if (datalen >= bt->big_object_size) {
			key_info.ptr=0xdeadc0de; //pure hack
		}
		res = btree_leaf_insert_key_index(bt, n, key, keylen, global_tmp_data, datalen,
						  &key_info, meta, index, &bytes_needed, true);
		dbg_assert(res == true);
//		dbg_assert(bytes_needed >= bytes_needed1);
	}

#ifdef COLLECT_TIME_STATS
	__sync_add_and_fetch(&bt_leaf_is_full, get_tod_usecs() - start_time);
#endif

	if (bytes_free < bytes_needed) {
		return true;
	}
	return false;
}

bool
btree_leaf_is_full(btree_raw_t *bt, btree_raw_node_t *n, char *key, uint32_t keylen,
		   uint64_t datalen, btree_metadata_t *meta, uint64_t syndrome,
		   bool key_exists)
{
	int index = -1;
	bool res = false;

	(void) btree_leaf_find_key2(bt, n, key, keylen, &index);	

	res = btree_leaf_is_full_index(bt, n, key, keylen, datalen, meta,
				       syndrome, key_exists, index);

	return res;
}

bool
btree_leaf_shift_left(btree_raw_t *bt, btree_raw_node_t *from_node,
		      btree_raw_node_t *to_node, key_info_t *key_info_out)
{
	btree_raw_node_t *tmp_node = NULL;
	int nkeys_from = 0;
	int nkeys_to = 0;
	int keys_copied = 0;
	int32_t avg_size = 0;


	tmp_node = (btree_raw_node_t *) get_buffer(bt, bt->nodesize);
	if (tmp_node == NULL) {
		dbg_assert(0);
		return false;
	}

	btree_leaf_init(bt, tmp_node);

	avg_size = (btree_leaf_used_space(bt, from_node) + btree_leaf_used_space(bt, to_node)) / 2;

	nkeys_from = btree_leaf_num_entries(bt, from_node);
	nkeys_to = btree_leaf_num_entries(bt, to_node);

	keys_copied = btree_leaf_copy_keys(bt, from_node, 0, to_node, nkeys_to, nkeys_from, avg_size);

	(void) btree_leaf_copy_keys(bt, from_node, keys_copied, tmp_node, 0, nkeys_from - keys_copied, 0);


	btree_memcpy(from_node->keys, tmp_node->keys, bt->nodesize_less_hdr, false);
	from_node->nkeys = tmp_node->nkeys;
	from_node->insert_ptr = tmp_node->insert_ptr;

	btree_leaf_get_nth_key_info(bt, to_node, to_node->nkeys - 1, key_info_out);

	free_buffer(bt, tmp_node);

	return true;
}

bool
btree_leaf_shift_right(btree_raw_t *bt, btree_raw_node_t *from_node,
		       btree_raw_node_t *to_node, key_info_t *key_info_out)
{
	btree_raw_node_t *tmp_node_to = NULL;
	btree_raw_node_t *tmp_node_from = NULL;
	int nkeys_from = 0;
	int nkeys_to = 0;
	int keys_copied = 0;
	int32_t avg_size = 0;


	tmp_node_to = (btree_raw_node_t *) get_buffer(bt, bt->nodesize);
	if (tmp_node_to == NULL) {
		dbg_assert(0);
		return false;
	}

	btree_leaf_init(bt, tmp_node_to);

	tmp_node_from = (btree_raw_node_t *) get_buffer(bt, bt->nodesize);
	if (tmp_node_from == NULL) {
		dbg_assert(0);
		free_buffer(bt, tmp_node_to);
		return false;
	}

	btree_leaf_init(bt, tmp_node_from);


	avg_size = (btree_leaf_used_space(bt, from_node) + btree_leaf_used_space(bt, to_node)) / 2;

	nkeys_from = btree_leaf_num_entries(bt, from_node);
	nkeys_to = btree_leaf_num_entries(bt, to_node);

	keys_copied = btree_leaf_copy_keys(bt, from_node, 0, tmp_node_from, 0, nkeys_from, avg_size);

	keys_copied = btree_leaf_copy_keys(bt, from_node, keys_copied, tmp_node_to, 0, nkeys_from - keys_copied, 0);

	
	keys_copied = btree_leaf_copy_keys(bt, to_node, 0, tmp_node_to, keys_copied, nkeys_to, 0);
	dbg_assert(keys_copied == nkeys_to);


	btree_memcpy(from_node->keys, tmp_node_from->keys, bt->nodesize_less_hdr, false);
	from_node->nkeys = tmp_node_from->nkeys;
	from_node->insert_ptr = tmp_node_from->insert_ptr;

	btree_memcpy(to_node->keys, tmp_node_to->keys, bt->nodesize_less_hdr, false);
	to_node->nkeys = tmp_node_to->nkeys;
	to_node->insert_ptr = tmp_node_to->insert_ptr;

	btree_leaf_get_nth_key_info(bt, from_node, from_node->nkeys - 1, key_info_out);

	free_buffer(bt, tmp_node_from);
	free_buffer(bt, tmp_node_to);

	return true;
}

bool
btree_leaf_merge_left(btree_raw_t *bt, btree_raw_node_t *from_node, btree_raw_node_t *to_node)
{
	int nkeys_from = 0;
	int nkeys_to = 0;
	int keys_copied = 0;

	
	nkeys_from = btree_leaf_num_entries(bt, from_node);
	nkeys_to = btree_leaf_num_entries(bt, to_node);

	keys_copied = btree_leaf_copy_keys(bt, from_node, 0, to_node, nkeys_to, nkeys_from, 0);
	dbg_assert(keys_copied == nkeys_from);


	from_node->nkeys = 0;
	from_node->insert_ptr = bt->nodesize;

	return true;
}

bool
btree_leaf_merge_right(btree_raw_t *bt, btree_raw_node_t *from_node,
		       btree_raw_node_t *to_node)
{
	btree_raw_node_t *tmp_node = NULL;
	int nkeys_from = 0;
	int nkeys_to = 0;
	int keys_copied = 0;

	tmp_node = (btree_raw_node_t *) get_buffer(bt, bt->nodesize);
	if (tmp_node == NULL) {
		dbg_assert(0);
		return false;
	}

	btree_leaf_init(bt, tmp_node);

	nkeys_from = btree_leaf_num_entries(bt, from_node);
	nkeys_to = btree_leaf_num_entries(bt, to_node);

	keys_copied = btree_leaf_copy_keys(bt, from_node, 0, tmp_node, 0, nkeys_from, 0);
	dbg_assert(keys_copied == nkeys_from);

	keys_copied = btree_leaf_copy_keys(bt, to_node, 0, tmp_node, nkeys_from, nkeys_to, 0);
	dbg_assert(keys_copied == nkeys_to);

	from_node->nkeys = 0;
	from_node->insert_ptr = bt->nodesize;

	btree_memcpy(to_node->keys , tmp_node->keys, bt->nodesize_less_hdr, false);
	to_node->nkeys = tmp_node->nkeys;
	to_node->insert_ptr = tmp_node->insert_ptr;

	free_buffer(bt, tmp_node);
	return true;
}
