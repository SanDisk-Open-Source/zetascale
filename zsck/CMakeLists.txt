add_definitions(-std=gnu99 -fgnu89-inline)

add_executable(zsck zsck.c) 
add_dependencies(zsck zs) 
target_link_libraries( zsck zsdll dl pthread aio event snappy)

add_executable(zsck_btree zsck.c) 
add_dependencies(zsck_btree zs)
target_link_libraries( zsck_btree zsdll dl pthread aio event snappy)

