cmake_minimum_required(VERSION 2.8 FATAL_ERROR)
#
IF($ENV{FDF_SDK_VERSION})
	SET(VERSION $ENV{FDF_SDK_VERSION})
ELSE()
	SET(VERSION 1.0)
ENDIF()
#
OPTION(DEBUG "Generate debug build" ON)
OPTION(BUILD_SHARED "Generate shared libs" ON)
OPTION(PERMISSIVE "Omit -Werror for build" OFF)
#
ENABLE_TESTING()

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/api)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../3rdparty_inc)
#
SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/output/lib)

IF(NOT NCPU)
	SET(NCPU 16)
ENDIF(NOT NCPU)
#
IF(TRACE)
	ADD_DEFINITIONS(-DDBG_PRINT -DDEBUG_STUFF)
ENDIF(TRACE)
#
#
IF(DEBUG)
	ADD_DEFINITIONS(-g3 -ggdb3)
ELSE(DEBUG)
	ADD_DEFINITIONS(-g -D_OPTIMIZE)
ENDIF(DEBUG)
#
SET(LIBRARY fdf)
#
IF(BUILD_SHARED)
	ADD_DEFINITIONS(-fPIC)
	SET(LIB_TYPE SHARED)
ENDIF(BUILD_SHARED)
#
IF(FDF_REVISION)
	ADD_DEFINITIONS(-DFDF_REVISION="${FDF_REVISION}")
ENDIF(FDF_REVISION)

add_definitions(-O2 -pipe -Wall -fstack-protector-all -fno-strict-aliasing -fno-omit-frame-pointer)
#add_definitions(-pipe -Wall -fstack-protector-all -fno-strict-aliasing -fno-omit-frame-pointer)
add_definitions( -D_GNU_SOURCE -DMPI_BUILD -DFLASH_SIM_TYPE=5 -DPLAT_LOG_COMPILE_LEVEL=DEVEL -DENABLE_MULTIPLE_FLASH_SUBSYSTEMS
-DBLD_VERSION=857 -DSDFAPI -DSDFAPIONLY -DSDFAPIREFORMAT -DENABLE_MULTIPLE_FLASH_SUBSYSTEMS)
#-DBLD_VERSION=857 -DSDFAPI -DSDFAPIONLY -DSDFAPIREFORMAT
#-DENABLE_MULTIPLE_FLASH_SUBSYSTEMS -DDEBUG_BUILD )
IF(NOT PERMISSIVE)
	add_definitions(-Werror)
ENDIF(NOT PERMISSIVE)
#
add_subdirectory(agent)
add_subdirectory(api)
add_subdirectory(api/tests)
add_subdirectory(api/tests/btree)
add_subdirectory(btree)
add_subdirectory(btree/tests)
add_subdirectory(cmds)
add_subdirectory(ecc)
add_subdirectory(fth)
add_subdirectory(license)
add_subdirectory(misc)
add_subdirectory(platform)
add_subdirectory(protocol)
add_subdirectory(sdftcp)
add_subdirectory(sdfmsg) 
add_subdirectory(utils)
add_subdirectory(ssd)
add_subdirectory(shared)
add_subdirectory(dll)
IF(EXISTS test_suite/)
add_subdirectory(test_suite)
ENDIF(EXISTS test_suite/)
IF(EXISTS fdf_slap/)
add_subdirectory(fdf_slap)
ENDIF(EXISTS fdf_slap/)
#
ADD_LIBRARY(${LIBRARY} ${LIB_TYPE} 
$<TARGET_OBJECTS:agent>
$<TARGET_OBJECTS:api>
$<TARGET_OBJECTS:ecc> 
$<TARGET_OBJECTS:fth> 
$<TARGET_OBJECTS:hotkey> 
$<TARGET_OBJECTS:license>
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
add_dependencies(${LIBRARY} ffdc)
#
IF(BUILD_SHARED)
	SET_TARGET_PROPERTIES(${LIBRARY} PROPERTIES 
	VERSION ${VERSION} 
	SOVERSION ${VERSION}
	)
ENDIF(BUILD_SHARED)

