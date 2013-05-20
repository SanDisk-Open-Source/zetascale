add_definitions(-std=gnu99 -fgnu89-inline)

file( GLOB TEST_SOURCES *.c )
foreach( test_src_file ${TEST_SOURCES} )
	IF(NOT ${test_src_file} MATCHES "/test.c")
		get_filename_component(testname ${test_src_file} NAME_WE)
		add_executable(${testname} ${test_src_file} test.c)
		add_executable(${testname}_btree ${test_src_file} test.c)
		add_dependencies(${testname} fdf)
		add_dependencies(${testname}_btree fdf)
		add_test(${testname} ${CMAKE_CURRENT_BINARY_DIR}/${testname} 1 8 10000)
		add_test(${testname}_btree
				${CMAKE_CURRENT_BINARY_DIR}/${testname}_btree 1 8 10000)
		target_link_libraries( ${testname} fdfdll dl pthread aio event)
		target_link_libraries( ${testname}_btree btreedll dl pthread aio event)
	endif (NOT ${test_src_file} MATCHES "/test.c")
endforeach(test_src_file ${TEST_SOURCES})

