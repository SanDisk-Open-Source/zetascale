# $Id$

TOP = ../..
PROG = threadPoolTest
LIB_SRCS = \
	XMbox.c \
	XResume.c \
	threadPool.c

#TESTS = XListTest XListShmemTest

LIB = libsdfappcommon.a

LIBS =  libsdfappcommon.a \
	../sdfmsg/libsdfmsgqueue.a \
	../sdftcp/libsdfmtp.a \
	../fth/libfth.a \
	../ssd/libsdfssd.a \
	../protocol/action/libsdfaction.a \
	../ssd/fifo/libfifo.a \
	../ssd/libsdfssd.a \
	../applib/libapplib.a \
	libsdfappcommon.a \
	../platform/libplatform.a \
	../utils/libutils.a -lpthread -lrt

USE_MPI = 1

include $(TOP)/Makefile.pre

# $(BUILD)threadXTest.o : WERROR =

include $(TOP)/Makefile.post

BROKEN = threadPoolTest.c
$(call MAKE_OPTOBJS_FROM_SRCS,$(BROKEN)): OPTFLAGS += -fno-builtin-printf

BROKEN2 = XMbox.c
$(call MAKE_OPTOBJS_FROM_SRCS,$(BROKEN2)): OPTFLAGS += -fno-strict-aliasing

