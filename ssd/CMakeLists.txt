ADD_DEFINITIONS(-std=gnu99 -fgnu89-inline)
add_library(sdfssd ssd.c ssd_aio.c ssd_defaults.c)
add_subdirectory(clipper)
add_subdirectory(fifo)
