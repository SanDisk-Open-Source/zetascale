# $Id: Makefile 11746 2010-02-19 23:22:21Z drew $

TOP = ../..
LIB = libsdfprotocol.a
LIB_SRCS = \
	init_protocol.c \
	protocol_common.c \
	protocol_utils.c

SUBDIRS = \
	action \
	home \
	replication 

include $(TOP)/Makefile.inc

BROKEN = protocol_common.c
$(call MAKE_OPTOBJS_FROM_SRCS,$(BROKEN)): OPTFLAGS += -fno-builtin
