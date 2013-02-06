# $Id$

TOP = ../..
PROG =
LIB_SRCS =

TESTS =

TESTS_BROKEN =

LIB =

LIBS =

include $(TOP)/Makefile.pre

#$(BUILD)threadXTest.o : WERROR =

include $(TOP)/Makefile.post

BROKEN = ftopMboxTest.c ptofMboxTest.c simpleWaitTest.c
$(call MAKE_OPTOBJS_FROM_SRCS,$(BROKEN)): OPTFLAGS += -fno-builtin-printf
