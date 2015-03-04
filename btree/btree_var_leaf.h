/*
 * Author: Ramesh Chander.
 * Created on Nov, 2013.
 * (c) Sandisk Inc.
 */


/*
 * Btree leaf node structure for variable metadata, key and data fields.
 *
 * The node structure is as follows:
 * | hdr1 | hdr2 | hdr3 | ......Growing Area...... | meta1 (type1) | key1| data1| meta2 (type3)| key2| data2|......|meta n (type2) |key n|data n|
 *
 *
 */

#ifndef __BTREE_VAR_LAEF_H_
#define __BTREE_VAR_LAEF_H_

#include "btree_raw_internal.h" 

typedef struct  __attribute__((__packed__)) entry_header {
//typedef struct entry_header {
	uint8_t tombstone:1;
	uint8_t meta_type:7; // Tells type of key_meta_typexx struct
#ifdef BIG_NODES
	uint32_t header_offset; // Offset from data ptr (insert_ptr) in node.
#else
	uint16_t header_offset;
#endif 
} entry_header_t;

typedef enum key_meta_type {
	BTREE_KEY_META_TYPE1 = 1,
	BTREE_KEY_META_TYPE2 = 1 << 1,
	BTREE_KEY_META_TYPE3 = 1 << 2,
	BTREE_KEY_META_TYPE4 = 1 << 3,
} key_meta_type_t;

#define big_object(bt, x) (((x)->keylen + (x)->datalen) >= (bt)->big_object_size)
#define big_object_kd(bt, k, d) ((k + d) >= (bt)->big_object_size)

/*
 * This is the fixed size metadata structure for temporaray processing.
 */
typedef struct key_meta {
	uint32_t keylen; // Real length of the key
	uint64_t datalen;
	uint16_t prefix_idx;
	uint32_t prefix_len;

	uint64_t ptr;
	uint64_t seqno;
	uint8_t tombstone;

//	uint64_t key_offset; // Offset to the key in the node relative to insert_ptr
	key_meta_type_t meta_type;
	bool compact_meta;
} key_meta_t;

typedef struct  __attribute__((__packed__)) key_meta_type1 {
//typedef struct key_meta_type1 {
	/*
	 * Same as the type2 but 
	 * Rest of the entries in prev entry.
	 */
	uint64_t seqno;
} key_meta_type1_t;

typedef struct  __attribute__((__packed__)) key_meta_type2 {
	uint8_t keylen;
	uint8_t datalen;
	uint16_t prefix_idx;
	uint8_t prefix_len;

	uint64_t seqno;
} key_meta_type2_t;

typedef struct  __attribute__((__packed__)) key_meta_type3 {
	uint16_t keylen;
	uint16_t datalen;
	uint16_t prefix_idx;
	uint16_t prefix_len;

	uint64_t ptr;
	uint64_t seqno;
} key_meta_type3_t;

typedef struct  __attribute__((__packed__)) key_meta_type4 {
	uint32_t keylen;
	uint64_t datalen;
	uint16_t prefix_idx;
	uint32_t prefix_len;

	uint64_t ptr;
	uint64_t seqno;
} key_meta_type4_t;

typedef struct key_info {
    int       fixed;
    uint64_t  ptr;
    char      *key;
    uint32_t  keylen;
    uint64_t  datalen;
    uint32_t  fkeys_per_node;
    uint64_t  seqno;
    uint64_t syndrome;
    bool      tombstone;
} key_info_t;


void
btree_leaf_init(btree_raw_t *bt, btree_raw_node_t *n);

int32_t
btree_leaf_num_entries(btree_raw_t *bt, btree_raw_node_t *n);

int32_t
btree_leaf_used_space(btree_raw_t *bt, btree_raw_node_t *n);

bool
btree_leaf_find_key2(btree_raw_t *bt, btree_raw_node_t *n, char *key,
		     uint32_t keylen, btree_metadata_t *meta, int32_t *index);
bool
btree_leaf_find_key(btree_raw_t *bt, btree_raw_node_t *n, char *key, uint32_t keylen,
		    btree_metadata_t *meta, uint64_t syndrome, int32_t *index);

bool
btree_leaf_find_right_key(btree_raw_t *bt, btree_raw_node_t *n,
			  char *key, uint32_t keylen,
			  char **key_out, uint32_t *keyout_len, uint64_t *seqno,
			  int *index, bool inclusive);

bool
btree_leaf_insert_key_index(btree_raw_t *bt, btree_raw_node_t *n, char *key, uint32_t keylen,
		      	    char *data, uint64_t datalen, key_info_t *key_info, 
			    int index, int32_t *bytes_increased, bool dry_run);

bool
btree_leaf_insert_key(btree_raw_t *bt, btree_raw_node_t *n, char *key, uint32_t keylen,
		      char *data, uint64_t datalen, uint64_t seqno, uint64_t ptr,
		      btree_metadata_t *meta, uint64_t syndrome, int index);

bool
btree_leaf_update_key(btree_raw_t *bt, btree_raw_node_t *n, char *key, uint32_t keylen,
		      char *data, uint64_t datalen, uint64_t seqno, uint64_t ptr,
		      btree_metadata_t *meta, uint64_t syndrome, int index, bool key_exists,
		      int32_t *bytes_saved, int32_t *size_increased);

bool
btree_leaf_split(btree_raw_t *btree, btree_raw_node_t *from_node,
		 btree_raw_node_t *to_node, char **key_out, uint32_t *keyout_len,
		 uint64_t *split_syndrome, uint64_t *split_seqno, int32_t *bytes_increased, uint32_t split_key);

bool
btree_leaf_is_full_index(btree_raw_t *bt, btree_raw_node_t *n, char *key, uint32_t keylen,
			 uint64_t datalen, btree_metadata_t *meta, uint64_t syndrome,
			 bool key_exists, int index);
bool
btree_leaf_is_full(btree_raw_t *btree, btree_raw_node_t *n, char *key, uint32_t keylen,
		   uint64_t datalen, btree_metadata_t *meta, uint64_t syndrome,
		   bool key_exists);

bool
btree_leaf_get_nth_key(btree_raw_t *btree, btree_raw_node_t *n, int index,
                       char **key_out, uint32_t *key_out_len,
                       uint64_t *key_seqno);

bool
btree_leaf_get_nth_key_info2(btree_raw_t *btree, btree_raw_node_t *n, int index,
				 key_info_t *key_info);
bool
btree_leaf_get_nth_key_info(btree_raw_t *btree, btree_raw_node_t *n, int index,
		            key_info_t *key_info);

bool
btree_leaf_is_key_tombstoned(btree_raw_t *bt, btree_raw_node_t *n, int index);

bool
btree_leaf_get_data_nth_key(btree_raw_t *btree, btree_raw_node_t *n, int index, 
			    char **data, uint64_t *datalen);
bool 
btree_leaf_get_data_key(btree_raw_t *btree, btree_raw_node_t *n,
			char *key, uint32_t keylen, 
			char **data, uint64_t *datalen);
bool
btree_leaf_remove_key_index(btree_raw_t *bt, btree_raw_node_t *n,
			    int index, key_info_t *key_info, int32_t *bytes_decreased);

bool
btree_leaf_shift_left(btree_raw_t *btree, btree_raw_node_t *from_node,
		      btree_raw_node_t *to_node, key_info_t *key_info_out, uint32_t max_keylen);

bool
btree_leaf_shift_right(btree_raw_t *btree, btree_raw_node_t *from_node,
		      btree_raw_node_t *to_node, key_info_t *key_info_out, uint32_t max_keylen);
bool
btree_leaf_merge_left(btree_raw_t *btree, btree_raw_node_t *from_node,
		      btree_raw_node_t *to_node);

bool
btree_leaf_merge_right(btree_raw_t *btree, btree_raw_node_t *from_node,
		       btree_raw_node_t *to_node);
void 
btree_leaf_print(btree_raw_t *btree, btree_raw_node_t *n);

/// New functions 
void inline
btree_leaf_get_meta(btree_raw_node_t *n, int index, key_meta_t *key_meta);

void
btree_leaf_adjust_prefix_idx(btree_raw_node_t *n, int index, int base, int shift);

void
btree_leaf_adjust_prefix_idxes(btree_raw_node_t *n, int from, int to, int base, int shift);

char *
get_key_prefix(btree_raw_node_t *n, int index);

void
build_key_prefix(btree_raw_t *bt, btree_raw_node_t *n,
		 int index, char *key,
	   	 uint32_t keylen, key_meta_t *key_meta);

int
btree_leaf_find_split_idx(btree_raw_t *bt, btree_raw_node_t *n);

uint64_t
btree_get_bigobj_inleaf(btree_raw_t *bt, uint64_t keylen, uint64_t datalen);

void inline
btree_leaf_unset_dataptr(btree_raw_node_t *n, int index, uint64_t datalen);

size_t
btree_leaf_get_max_meta(void);

#if 0
bool 
btree_leaf_node_check(btree_raw_t *btree, btree_raw_node_t *node,
		      char *left_anchor_key, uint32_t left_anchor_keylen, 
		      char *right_anchor_key, uint32_t right_anchor_keylen);
	
#endif 
#endif  // End if file
