# Btree GDB macros
#
# Author: Ramesh Chander , July, 213
# Sandisk Copyright 
# 

#====== Leaf node functions==============

# print the metadata types
define  bt_leaf_print_meta1
set $meta = (key_meta_type1_t *) $arg0
#printf "Called meta print1"
p *$meta
end

define  bt_leaf_print_meta2
set $meta = (key_meta_type2_t *) $arg0
#printf "Called meta print2"
p *$meta
end

define  bt_leaf_print_meta3
set $meta = (key_meta_type3_t *) $arg0
#printf "Called meta print3"
p *$meta
end

define  bt_leaf_print_meta4
set $meta = (key_meta_type3_t *) $arg0
#printf "Called meta print4"
p *$meta
end

#print metadata of a key 
# usage: btree_leaf_print_meta type metadata_ptr
define btree_leaf_print_meta
set $type = $arg0
set $meta = $arg1
#printf "meta type = %d", $type

if ($type == BTREE_KEY_META_TYPE1) 
	bt_leaf_print_meta1 $meta
end
if ($type == BTREE_KEY_META_TYPE2) 
	bt_leaf_print_meta2 $meta
end
if ($type == BTREE_KEY_META_TYPE3) 
	bt_leaf_print_meta3 $meta
end
if ($type == BTREE_KEY_META_TYPE4) 
	bt_leaf_print_meta4 $meta;
end
end

#print nthe key of a lead node
define bt_leaf_print_nth_key
set $node=$arg0
set $num=$arg1
set $ent_hdr = (entry_header_t *) ((uint64_t) $node->keys + sizeof(entry_header_t) * $num)
p *$ent_hdr
set $meta = ((uint64_t) $node + $node->insert_ptr + $ent_hdr->header_offset)
btree_leaf_print_meta $ent_hdr->meta_type $meta
end

document bt_leaf_print_nth_key 
Print the Nth key in a Btree leaf node.
Usage: btree_leaf_print_meta (btree_raw_node *) (key number)
end

#print a btree leaf node 
define bt_leaf_print 
set $node = $arg0
p *$node
set $num = 0
while ($num < $node->nkeys)
bt_leaf_print_nth_key $node $num
set $num++
end
end

document bt_leaf_print
Print a given btree leaf node keys
usage: bt_leaf_print  (btree_raw_node *)
end

#======== Non-Leaf node functions ==========

# Get nth key of a non-leaf node 
# args: entrynumber node (btree_raw_node *)
define get_nonleaf_nth_key
set $num=$arg0
set $node=$arg1
set $pvk = ((node_vkey_t *)$node->keys) + $num
set $key_len = $pvk->keylen
set $key = (char *)$node + $pvk->keypos
set $ptr = $pvk->ptr
printf "Key Len = %d, Key = %s, Child ID = %ld.\n", $key_len, $key, $ptr
end

# Get nth key of a btree node 
# args: entrynumber node(btree_raw_node *)
define btree_get_nth_key
set $num = $arg0
set $node = $arg1
 if ($node->flags & 1) 
   bt_leaf_print_nth_key $node $num
 else 
   get_nonleaf_nth_key $num $node
 end
end 

document btree_get_nth_key
Print the Nth key in a Btree node.
Usage: btree_print_node (key number) (btree_raw_node *)
end

# Get nth key of a btree node 
# args: node(btree_raw_node *)
define btree_print_node 
set $node = $arg0
printf " ======= Node Details =======\n"
if ($node->flags & 1) 
	printf "LEAF. \n"
else 
	printf "NON-LEAF.\n"
end
set $count = 0
while ($count < $node->nkeys)
	printf " === Key Index %d === ::\n", $count
	btree_get_nth_key  $count $node
	set $count++
end
printf "=============End =========="
end

document btree_print_node
Prints info about a Btree Node.
Usage: btree_print_node (btree_raw_node *)
end
