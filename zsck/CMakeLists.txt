add_definitions(-std=gnu99 -fgnu89-inline)

add_executable(zsck zsck.c) 
add_dependencies(zsck zs) 
target_link_libraries( zsck zsdll dl pthread aio event snappy lz4)

add_executable(zsformat zsformat.c) 
add_dependencies(zsformat zs) 
target_link_libraries( zsformat zsdll dl pthread aio event snappy lz4)

add_executable(zsmetafault zsmetafault.c)
add_dependencies(zsmetafault zs)
target_link_libraries( zsmetafault zsdll dl pthread aio event snappy lz4)
