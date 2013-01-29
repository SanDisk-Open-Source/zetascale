# $Id: Makefile 9983 2009-11-02 18:25:23Z gxu $

TOP = ../..
LIB = libsdf.a
LIB_SRCS = \
	sdf.c \
	fdf.c \
        enumerate.c

SUBDIRS = tests

PROG = sdftest

SRCS = \
	main.c

LIBS = \
	$(TOP)/sdf/api/libsdf.a \
	$(TOP)/sdf/agent/libsdfagent.a \
	$(TOP)/sdf/shared/libsdfshared.a \
	$(TOP)/sdf/api/libsdf.a \
    $(TOP)/sdf/protocol/action/libsdfaction.a \
    $(TOP)/sdf/protocol/home/libsdfhome.a \
	$(TOP)/sdf/protocol/libsdfprotocol.a \
	$(TOP)/sdf/ssd/libsdfssd.a \
	$(TOP)/sdf/ssd/clipper/libclipper.a \
	$(TOP)/sdf/ssd/fifo/libfifo.a \
	$(TOP)/sdf/ssd/libsdfssd.a \
	$(TOP)/sdf/protocol/replication/libsdfreplication.a \
	$(TOP)/sdf/ecc/libecc.a \
	$(TOP)/sdf/utils/libutils.a \
	$(TOP)/sdf/sdfmsg/libsdfmsgqueue.a \
	$(TOP)/sdf/sdftcp/libsdfmtp.a \
	$(TOP)/sdf/platform/tool/hotkey/libhotkey.a \
	$(TOP)/sdf/misc/libmisc.a \
	$(TOP)/sdf/fth/libfth.a \
	$(TOP)/sdf/platform/libplatform.a \
        -lpthread -lrt -lnsl -lutil -lm -laio -lsasl2 -levent


include $(TOP)/Makefile.inc
