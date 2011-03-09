# $Id: Makefile 9970 2009-10-30 23:51:23Z briano $

TOP = ../../..
LIB = libsdfhome.a
LIB_SRCS = \
	home_flash.c\
	home_util.c\
	tlmap2.c

SUBDIRS = 

include $(TOP)/Makefile.inc

vpath %.c $(SDF_PROTOCOL_TABLES_GENERATED)
CPP_INCLUDES += -I../Tables
CPPFLAGS += -DYY_NO_INPUT
