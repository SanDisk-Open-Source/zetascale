add_definitions(-std=gnu99 -fgnu89-inline)

add_executable(btest_life_cycle ${test_src_file}
		btest_life_cycle.c btest_common.c)
add_executable(btest_range ${test_src_file}
		btest_range.c btest_common.c)
target_link_libraries( btest_life_cycle btree fdfdll dl pthread aio event)
target_link_libraries( btest_range btree fdfdll dl pthread aio event)

