cmake_minimum_required(VERSION 2.6)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../3rdparty_inc)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/output/lib)
add_definitions(-O3 -pipe -g -Wall -fstack-protector-all -fno-strict-aliasing
-fno-omit-frame-pointer )
add_definitions( -D_GNU_SOURCE -DMPI_BUILD -DFLASH_SIM_TYPE=5
-DPLAT_LOG_COMPILE_LEVEL=DEVEL -DENABLE_MULTIPLE_FLASH_SUBSYSTEMS
-DBLD_VERSION=857 -DSDFAPI -DSDFAPIREFORMAT -DSIMPLE_REPLICATION
-DVIPGROUP_SUPPORT -DENABLE_MULTIPLE_FLASH_SUBSYSTEMS)
#add_definitions(-Werror)
add_subdirectory(platform)
add_subdirectory(fth)
add_subdirectory(misc)
add_subdirectory(sdftcp)
add_subdirectory(sdfmsg) 
add_subdirectory(utils)
add_subdirectory(ecc)
add_subdirectory(ssd)
add_subdirectory(protocol)
add_subdirectory(shared)
add_subdirectory(agent)
add_subdirectory(api)
