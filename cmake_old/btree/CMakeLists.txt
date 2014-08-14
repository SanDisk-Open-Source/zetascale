add_library(btree SHARED btree_hash.c btree_map.c btree_raw.c btree.c btree_scavenger.c
		btree_range.c btree_pmap.c btree_var_leaf.c btree_malloc.c btree_recovery.c packet.c btree_sync_th.c
		fdf_wrapper.c trx.c ../platform/rwlock.c ../flip/flip.c ../flip/flip_cmd.c
		btree_snapshot.c btree_sync_th.c btree_sync_mbox.c btree_mem.c)
target_link_libraries(btree fdfdll)
add_definitions(-Wno-parentheses -Wno-unused)
