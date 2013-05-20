add_library(btree SHARED btree_hash.c btree_map.c btree_raw.c btree.c fdf_wrapper.c selftest.c)
target_link_libraries(btree fdfdll)
