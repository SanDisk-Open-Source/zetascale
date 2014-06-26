add_definitions(-std=gnu99 -fgnu89-inline)

SET(test_src_file FDF_Listen.c)
get_filename_component(testname ${test_src_file} NAME_WE)
add_executable(${testname} ${test_src_file})
add_dependencies(${testname} zs)
add_test(gc_admin /bin/bash gc_admin.sh)
target_link_libraries( ${testname} zsdll dl pthread aio event snappy)
