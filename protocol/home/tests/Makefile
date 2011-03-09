# $Id: Makefile 9983 2009-11-02 18:25:23Z gxu $

TOP = ../../../..

PROG = testhomedir \
	homedir_perf_test1 \
	homedir_perf_test2 

TESTS = 

LIBS = \
        ../../../../sdfclient/libsdfclient.a \
        ../../../agent/libsdfagent.a \
        ../../../shared/libsdfshared.a \
        ../../../protocol/action/libsdfaction.a \
        ../../../protocol/replication/libsdfreplication.a \
        ../../../protocol/home/libsdfhome.a \
        ../../../sdfcc/libsdfcc.a \
        ../../../protocol/libsdfprotocol.a \
        ../../../ssd/libsdfssd.a \
        ../../../ssd/clipper/libclipper.a \
        ../../../protocol/action/libsdfaction.a \
        ../../../ssd/fifo/libfifo.a \
        ../../../ssd/htf/libhtf.a \
	../../../ecc/libecc.a \
        ../../../utils/libutils.a \
        ../../../sdfappcommon/libsdfappcommon.a \
        ../../../sdfmsg/libsdfmsgqueue.a \
        ../../../sdftcp/libsdfmtp.a \
        ../../../applib/libapplib.a \
        ../../../fth/libfth.a \
        ../../../platform/libplatform.a \
        -lpthread -lrt -lcunit -levent

USE_MPI = 1

include $(TOP)/Makefile.pre
include $(TOP)/Makefile.post

testhomedir.run: TESTARGS = .
