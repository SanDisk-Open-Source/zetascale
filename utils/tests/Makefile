#$Id$

TOP = ../../..

TESTS_BROKEN = \
        propertiestest

TESTS = \
		hashmaptest

PROG = $(TESTS) $(TESTS_BROKEN)

LIBS =  \
        ../../agent/libsdfagent.a \
        ../../shared/libsdfshared.a \
        ../../protocol/home/libsdfhome.a \
        ../../protocol/action/libsdfaction.a \
        ../../protocol/replication/libsdfreplication.a \
        ../../protocol/libsdfprotocol.a \
        ../../sdfmsg/libsdfmsgqueue.a \
        ../../utils/libutils.a \
        ../../fth/libfth.a \
        ../../misc/libmisc.a \
        ../../platform/libplatform.a \
        -lpthread -lrt

hashmaptest.run: TESTARGS = 10240 0 1 7919 12 10
propertiestest.run TESTARGS = . ./schooner.properties

USE_MPI = 0

include $(TOP)/Makefile.inc

BROKEN = propertiestest.c
$(call MAKE_OPTOBJS_FROM_SRCS,$(BROKEN)): OPTFLAGS += -fno-builtin-printf
