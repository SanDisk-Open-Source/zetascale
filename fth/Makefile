LIBS = ../misc/libmisc.a ../platform/libplatform.a libfth.a  ../applib/libapplib.a ../platform/libplatform.a libfth.a ../utils/libutils.a -lpthread -lrt

TOP   = ../..
TESTS =
TESTS_BROKEN = fthMPTest
PROG  = 

LIB_SRCS = \
	fth.c \
	fthLock.c \
	fthMbox.c \
        fthSem.c \
        fthOpt.c \
	fthSignal.c

LIB = libfth.a

include $(TOP)/Makefile.inc

OPTFLAGS += -fno-builtin-printf

# These definitely have format string issues
$(call MAKE_OPTOBJS_FROM_SRCS,$(BROKEN)): OPTFLAGS = -fno-builtin
