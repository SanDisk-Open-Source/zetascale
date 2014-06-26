#IF(NOT DEBUG) 
add_library(zsdll STATIC fdf_dll.c)
add_library(btreedll STATIC btree_dll.c)
#ENDIF(NOT DEBUG)
