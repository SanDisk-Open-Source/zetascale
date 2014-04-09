#IF(NOT DEBUG) 
add_library(fdfdll STATIC fdf_dll.c)
add_library(btreedll STATIC btree_dll.c)
#ENDIF(NOT DEBUG)
