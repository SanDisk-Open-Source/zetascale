# Btree GDB macros
#
# Author: Ramesh Chander , July, 213
# Sandisk Copyright 
# 

# Get nth key of a leaf node 
# args: entrynumber node (btree_raw_node *)
define get_leaf_nth_key
set $num=$arg0
set $node=$arg1
set $pvlk = ((node_vlkey_t *)$node->keys) + $num 
set $key_len = $pvlk->keylen
set $key = (char *)$node + $pvlk->keypos
printf "Key Len = %d, Key = %s.\n", $key_len, $key
end

# Get nth key of a non-leaf node 
# args: entrynumber node (btree_raw_node *)
define get_nonleaf_nth_key
set $num=$arg0
set $node=$arg1
set $pvlk = ((node_vkey_t *)$node->keys) + $num 
set $key_len = $pvlk->keylen
set $key = (char *)$node + $pvlk->keypos

printf "Key Len = %d, Key = %s.\n", $key_len, $key
end

# Get nth key of a btree node 
# args: entrynumber node(btree_raw_node *)
define btree_get_nth_key
set $num = $arg0
set $node = $arg1
 if ($node->flags & 1) 
	get_leaf_nth_key $num $node
 else 
   get_nonleaf_nth_key $num $node
 end
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
	
	btree_get_nth_key  $count $node
	set $count++
end
printf "=============End =========="
end
