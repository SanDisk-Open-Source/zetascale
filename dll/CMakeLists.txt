#IF(NOT DEBUG) 
add_library(fdfdll STATIC fdf_dll.c)
#ENDIF(NOT DEBUG)
