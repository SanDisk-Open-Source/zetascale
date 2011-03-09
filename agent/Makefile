# $Id: Makefile 9983 2009-11-02 18:25:23Z gxu $

TOP = ../..

LIBS = libsdfagent.a \
	../shared/libsdfshared.a \
	../protocol/home/libsdfhome.a \
	../protocol/action/libsdfaction.a \
	../protocol/replication/libsdfreplication.a \
	../ssd/libsdfssd.a \
	../ssd/clipper/libclipper.a \
	../protocol/action/libsdfaction.a \
	../ssd/fifo/libfifo.a \
	../ssd/htf/libhtf.a \
	../ecc/libecc.a \
	../sdfcc/libsdfcc.a \
	../protocol/libsdfprotocol.a \
	../sdfmsg/libsdfmsgqueue.a \
        ../sdftcp/libsdfmtp.a \
	../utils/libutils.a \
	../applib/libapplib.a  \
	../fth/libfth.a \
        ../sdfappcommon/libsdfappcommon.a \
	../misc/libmisc.a \
	../platform/libplatform.a \
	-lpthread -lrt -levent

# libsdfagent is linked against by client code and PROG sdfagent 
LIB = libsdfagent.a

LIB_SRCS = \
	agent_helper.c \
	agent_sm.c

USE_MPI = 1

include $(TOP)/Makefile.inc
