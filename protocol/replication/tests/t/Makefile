# $Id: Makefile 10059 2009-06-16 00:10:47Z syncsvn $

TOP = ../../../../..

TEST_PROG = \
	fcnl_rtg_test \

TESTS = \
	$(TEST_PROG) \
	static_test_rtm

TESTS_BROKEN = \
	fcnl_rtm_test \
	fcnl_rtm_test_lzwei \


PROG = \
	$(TEST_PROG) \
	$(TESTS_BROKEN) \
	fcnl_rtm_case

#	fcnl_rtm_test \
#	fcnl_rtm_case \
#	fcnl_rtm_test_lzwei \
#	fcnl_rtg_test

LIBS = \
	../libreplica.a \
        ../../../../agent/libsdfagent.a \
        ../../../../shared/libsdfshared.a \
        ../../../../api/libsdf.a \
        ../../../../protocol/action/libsdfaction.a \
        ../../../../ssd/fifo/libfifo.a \
        ../../../../protocol/replication/libsdfreplication.a \
        ../../../../protocol/home/libsdfhome.a \
        ../../../../protocol/libsdfprotocol.a \
        ../../../../ssd/libsdfssd.a \
        ../../../../ssd/clipper/libclipper.a \
        ../../../../protocol/action/libsdfaction.a \
        ../../../../utils/libutils.a \
        ../../../../fth/libfth.a \
        ../../../../sdfmsg/libsdfmsgqueue.a \
	../../../../sdftcp/libsdfmtp.a \
        ../../../../ecc/libecc.a \
        ../../../../misc/libmisc.a \
        ../../../../platform/libplatform.a \
        ../../../../fth/libfth.a \
	$(TOP)/sdf/platform/libplatform.a \
        -lpthread -lm -lrt -lcunit -levent -laio

TESTARGS = --timeout 5
fcnl_rtg_test.run: TESTARGS  = --log sdf/prot/replication/test=info --timeout 5

USE_MPI = 1

include $(TOP)/Makefile.inc
