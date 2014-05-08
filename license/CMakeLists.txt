file(GLOB_RECURSE LIC2_FILE RELATIVE  ${CMAKE_CURRENT_SOURCE_DIR}
		${CMAKE_CURRENT_SOURCE_DIR}/../license/*/*.c
		${CMAKE_CURRENT_SOURCE_DIR}/../license/*/*.cpp)
add_library(license OBJECT interface.c versions.c utils.cpp ${LIC2_FILE})

