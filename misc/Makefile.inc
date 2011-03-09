# $Id: Makefile.inc 9059 2009-09-04 00:50:42Z johann $

# While the plethora of subdirectories have been useful for keeping things
# grouped, link order (especially circular dependencies amongst  
# platform, misc, etc) have become a headache.

# The miscellaneous files are listed here; the grand unified library
# just includes this, tacks it on the appropriate LIB SRCS, and vpaths

MISC_SRCS = \
	parse_int.c \
	parse_reseed.c \
	parse_string.c \
	parse_string_alloc.c \
	parse_size.c \
	parse_time.c \
	parse_uint.c \
	parse_int32.c \
	parse_uint64.c \
	seed.c \
	stop.c \
	timeout.c
