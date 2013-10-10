add_library(btree SHARED btree_hash.c btree_map.c btree_raw.c btree.c
		btree_range.c btree_pmap.c
		fdf_wrapper.c trx.c selftest.c ../platform/rwlock.c)
target_link_libraries(btree fdfdll)
add_definitions(-Wno-parentheses -Wno-unused)
