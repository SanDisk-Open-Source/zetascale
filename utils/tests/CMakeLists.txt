set(COMPILE_DEFINITIONS ${COMPILE_DEFINITIONS} -fno-builtin-printf)
add_executable(propertiestest propertiestest.c)
add_executable(hashmaptest hashmaptest.c)
#target_link_libraries()