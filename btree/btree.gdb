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
set $ptr = $pvlk->ptr
printf "Key Len = %d, Key = %s, Overflow Node = %ld.\n", $key_len, $key, $ptr
end


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
	get_leaf_nth_key $num $node
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
	
	btree_get_nth_key  $count $node
	set $count++
end
printf "=============End =========="
end

document btree_print_node
Prints info about a Btree Node.
Usage: btree_print_node (btree_raw_node *)
end
