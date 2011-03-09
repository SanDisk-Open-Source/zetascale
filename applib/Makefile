# $Id$

TOP = ../..
PROG = $(TESTS) $(TESTS_BROKEN)
LIB_SRCS = \
	XLock.c \
	XMbox.c \
	simpleWait.c \
	fthFlashAccess.c \

TESTS = XLockTest ftopMboxTest ptofMboxTest 

TESTS_BROKEN = simpleWaitTest

LIB = libapplib.a

LIBS = libapplib.a ../fth/libfth.a ../fth/libfth.a ../sdfappcommon/libsdfappcommon.a  ../platform/libplatform.a ../utils/libutils.a -lpthread -lrt

include $(TOP)/Makefile.pre

#$(BUILD)threadXTest.o : WERROR =

include $(TOP)/Makefile.post

BROKEN = ftopMboxTest.c ptofMboxTest.c simpleWaitTest.c
$(call MAKE_OPTOBJS_FROM_SRCS,$(BROKEN)): OPTFLAGS += -fno-builtin-printf
