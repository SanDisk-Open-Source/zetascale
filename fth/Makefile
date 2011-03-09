LIBS = ../misc/libmisc.a ../platform/libplatform.a libfth.a  ../sdfappcommon/libsdfappcommon.a ../applib/libapplib.a ../platform/libplatform.a libfth.a ../utils/libutils.a -lpthread -lrt

TOP   = ../..
TESTS = fthIdleTest fthTest fthSparseTest fthSignalTest fthCvTest \
	fthMonotonicTest
TESTS_BROKEN = fthMPTest
PROG  = $(TESTS) $(TESTS_BROKEN) fthSpeed fthGetTimeSpeed

LIB_SRCS = \
	fth.c \
	fthIdleControl.c \
	fthLock.c \
	fthMem.c \
	fthMutex.c \
	fthCv.c \
	fthMbox.c \
	fthOpt.c \
        fthSem.c \
	fthSignal.c \
	fthSocket.c \
	fthSparseLock.c \
	fthStructQ.c \
	fthThreadQ.c \
	fthWaitQ.c \
	fthXLock.c \
	fthXMbox.c \
	fthTrace.c \
	fthRcu.c \
	fthThread.S

LIB = libfth.a

include $(TOP)/Makefile.inc

OPTFLAGS += -fno-builtin-printf

# These definitely have format string issues
$(call MAKE_OPTOBJS_FROM_SRCS,$(BROKEN)): OPTFLAGS = -fno-builtin
