TOP = ../../..

PROG = $(TESTS)

TESTS = tm_orig sdf_bounce sdf_bw sdf_lat sdf_live sdf_lock

LIB_SRCS = agent.c conf.c

LIB = libsdfmsgnewtest.a

LIBS =  \
	libsdfmsgnewtest.a \
	.././libsdfmsgqueue.a \
	../../sdftcp/libsdfmtp.a \
	../../fth/libfth.a \
	../../utils/libutils.a \
	../../fth/libfth.a \
	../../misc/libmisc.a \
	../../platform/libplatform.a \
        ../../sdfappcommon/libsdfappcommon.a \
	../../sdftcp/libsdfmtp.a \
	-lpthread -lrt

USE_MPI = 1

TARGETRUN := $(foreach testitem, $(TESTS), $(testitem).run)
$(TARGETRUN): TESTRUNNER = $(SHELL) testrun

sdf_bounce.run : TESTARGS = --timeout 5

include $(TOP)/Makefile.inc
