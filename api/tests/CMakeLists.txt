add_definitions(-std=gnu99 -fgnu89-inline)
#add_executable(fdf_mini_trx_api_test fdf_mini_trx_api.c)
#add_dependencies(fdf_mini_trx_api_test fdf)
#target_link_libraries(fdf_mini_trx_api_test fdfdll dl pthread) 

file( GLOB TEST_SOURCES *.c )
foreach( test_src_file ${TEST_SOURCES} )
	IF(NOT ${test_src_file} MATCHES "/test.c")
		get_filename_component(testname ${test_src_file} NAME_WE)
		add_executable( ${testname} ${test_src_file} test.c)
		add_dependencies(${testname} fdf)
		add_definitions(-g)
		target_link_libraries( ${testname} fdf dl pthread aio event)
	endif (NOT ${test_src_file} MATCHES "/test.c")
endforeach(test_src_file ${TEST_SOURCES})

add_test(mini_trx_api ${CMAKE_CURRENT_BINARY_DIR}/fdf_mini_trx_api 1 8 10000)
add_test(multi_test ${CMAKE_CURRENT_BINARY_DIR}/fdf_multi_test 1 8 10000)

