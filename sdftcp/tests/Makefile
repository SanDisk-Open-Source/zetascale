# $Id: Makefile,v 1.1 2008/05/22 09:48:07 drew Exp drew $
#
# msgtest is the SDF Message Transport library and tests
#
# The default build is a standalone program for the unit testing of the 
# msgtest messaging transport functions
# ------------------------------------------------------------------
#

TOP = ../../..

LIB = libmsgtestcommon.a

SRCS = msgtest_common.c msgtest_common.h

PROG = \
	$(TESTS)

TESTS = msgtest_im msgtest_hello msgtest_pplat msgtest_remote
#TESTS = msgtest_im msgtest_hello msgtest_pplat msgtest_auto_heavy.c \
		msgtest_auto_int.c msgtest_point_receive.c \
		msgtest_point_send.c

LIBS =  \
	libmsgtestcommon.a \
	.././libsdfmtp.a \
	../../sdfmsg/libsdfmsgqueue.a \
	../../fth/libfth.a \
	../../utils/libutils.a \
	../../fth/libfth.a \
	../../misc/libmisc.a \
	../../platform/libplatform.a \
        ../../sdfappcommon/libsdfappcommon.a \
	-lpthread -lrt

USE_MPI = 1

include $(TOP)/Makefile.inc
#WERROR=
