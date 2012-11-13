# $Id: Makefile 9970 2009-10-30 23:51:23Z briano $

TOP = ../../..
LIB = libsdfaction.a
LIB_SRCS = \
	tlmap.c\
	fastcc_new.c\
	action_new.c\
	tlmap4.c\
	simple_replication.c\
	async_puts.c\
	appbuf_pool.c\
	response_tbl.c

# SUBDIR = tests

include $(TOP)/Makefile.inc

BROKEN = action_thread.c
$(call MAKE_OPTOBJS_FROM_SRCS,$(BROKEN)): OPTFLAGS += -fno-strict-aliasing

vpath %.c $(SDF_PROTOCOL_TABLES_GENERATED)
CPP_INCLUDES += -I../Tables
CPPFLAGS += -DYY_NO_INPUT
