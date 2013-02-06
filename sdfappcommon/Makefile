# $Id$

TOP = ../..
PROG =
LIB_SRCS =

#TESTS = XListTest XListShmemTest

LIB =

LIBS =

USE_MPI = 1

include $(TOP)/Makefile.pre

# $(BUILD)threadXTest.o : WERROR =

include $(TOP)/Makefile.post

BROKEN = threadPoolTest.c
$(call MAKE_OPTOBJS_FROM_SRCS,$(BROKEN)): OPTFLAGS += -fno-builtin-printf

BROKEN2 = XMbox.c
$(call MAKE_OPTOBJS_FROM_SRCS,$(BROKEN2)): OPTFLAGS += -fno-strict-aliasing

