cmake_minimum_required(VERSION 2.6)
#
IF(NOT $ENV{SCHOONER_VERSION})
	SET(VERSION "4.2")
ELSE()
	SET(VERSION $ENV{SCHOONER_VERSION})
ENDIF()
#
OPTION(OPTIMIZE "Generate optimized build" ON)
OPTION(DEBUG "Generate debug build" OFF)
OPTION(BUILD_STATIC "Generate static libs" ON)
OPTION(BUILD_SHARED "Generate shared libs" OFF)
#
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../3rdparty_inc)
#
SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/output/lib)
#
IF(DEBUG)
	ADD_DEFINITIONS(-g3 -ggdb3)
	SET(OPTIMIZE OFF)
	SET(LIBRARY fdfdbg)
ENDIF(DEBUG)
#
IF(OPTIMIZE)
	ADD_DEFINITIONS(-D_OPTIMIZE)
	SET(LIBRARY fdf)
ENDIF(OPTIMIZE)
#
IF(BUILD_SHARED)
	ADD_DEFINITIONS(-fPIC)
	SET(BUILD_STATIC OFF)
ENDIF(BUILD_SHARED)
#
add_definitions(-O3 -pipe -Wall -fstack-protector-all -fno-strict-aliasing
-fno-omit-frame-pointer)
add_definitions( -D_GNU_SOURCE -DMPI_BUILD -DFLASH_SIM_TYPE=5
-DPLAT_LOG_COMPILE_LEVEL=DEVEL -DENABLE_MULTIPLE_FLASH_SUBSYSTEMS
-DBLD_VERSION=857 -DSDFAPI -DSDFAPIREFORMAT -DSIMPLE_REPLICATION
-DVIPGROUP_SUPPORT -DENABLE_MULTIPLE_FLASH_SUBSYSTEMS)
add_definitions(-Werror)
#
add_subdirectory(agent)
add_subdirectory(api)
add_subdirectory(ecc)
add_subdirectory(fth)
add_subdirectory(misc)
add_subdirectory(platform)
add_subdirectory(protocol)
add_subdirectory(sdftcp)
add_subdirectory(sdfmsg) 
add_subdirectory(utils)
add_subdirectory(ssd)
add_subdirectory(shared)
#
IF(BUILD_STATIC)
ADD_LIBRARY(${LIBRARY} STATIC 
$<TARGET_OBJECTS:agent>
$<TARGET_OBJECTS:api>
$<TARGET_OBJECTS:ecc> 
$<TARGET_OBJECTS:fth> 
$<TARGET_OBJECTS:hotkey> 
$<TARGET_OBJECTS:misc> 
$<TARGET_OBJECTS:platform> 
$<TARGET_OBJECTS:protocol> 
$<TARGET_OBJECTS:action> 
$<TARGET_OBJECTS:home> 
$<TARGET_OBJECTS:replication> 
$<TARGET_OBJECTS:sdftcp> 
$<TARGET_OBJECTS:sdfmsg> 
$<TARGET_OBJECTS:utils> 
$<TARGET_OBJECTS:ssd> 
$<TARGET_OBJECTS:shared> 
$<TARGET_OBJECTS:clipper> 
$<TARGET_OBJECTS:fifo> 
)
ENDIF(BUILD_STATIC)
#
IF(BUILD_SHARED)
ADD_LIBRARY(${LIBRARY} SHARED
$<TARGET_OBJECTS:agent>
$<TARGET_OBJECTS:api>
$<TARGET_OBJECTS:ecc>
$<TARGET_OBJECTS:fth>
$<TARGET_OBJECTS:hotkey>
$<TARGET_OBJECTS:misc>
$<TARGET_OBJECTS:platform>
$<TARGET_OBJECTS:protocol>
$<TARGET_OBJECTS:action>
$<TARGET_OBJECTS:home>
$<TARGET_OBJECTS:replication>
$<TARGET_OBJECTS:sdftcp>
$<TARGET_OBJECTS:sdfmsg>
$<TARGET_OBJECTS:utils>
$<TARGET_OBJECTS:ssd>
$<TARGET_OBJECTS:shared>
$<TARGET_OBJECTS:clipper>
$<TARGET_OBJECTS:fifo>
)
ENDIF(BUILD_SHARED)























