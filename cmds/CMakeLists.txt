file(GLOB_RECURSE LIC_FILE RELATIVE  ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../license/*.c ${CMAKE_CURRENT_SOURCE_DIR}/../license/*.cpp)
file( GLOB TEST_SOURCES *.c )
foreach( test_src_file ${TEST_SOURCES} )
	get_filename_component(testname ${test_src_file} NAME_WE)
	add_executable(${testname} ${test_src_file} ${LIC_FILE})
endforeach(test_src_file ${TEST_SOURCES})

