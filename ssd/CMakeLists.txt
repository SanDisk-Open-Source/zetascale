#ADD_DEFINITIONS(-UENABLE_MULTIPLE_FLASH_SUBSYSTEMS)
add_library(sdfssd ssd.c ssd_aio.c ssd_defaults.c)
add_subdirectory(clipper)
add_subdirectory(fifo)
